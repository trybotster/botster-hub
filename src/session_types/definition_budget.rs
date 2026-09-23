//! Successful-path storage and schema-position error candidates.
//! The production decoder retains authority over errors and validation.

use std::collections::BTreeMap;
use std::fmt::{self, Write};

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

use super::{PackageSessionType, PackageSessionTypeExecution, PackageSessionTypeWorkingDirectory};
use super::content_budget::{ContentContainer, ContentStorage};
use super::materialization_error::ErrorTrack;
use super::materialization_timeline::Track;
use super::tagged_budget::{TaggedField, TaggedStorage};

#[derive(Clone, Copy)]
pub(super) enum Shape {
    File,
    Definitions,
    Definition,
    String,
    OptionalString,
    Strings,
    Environment,
    Execution,
    WorkingDirectory,
}

trait CountedFieldType {
    const SHAPE: Shape;
}

impl CountedFieldType for String {
    const SHAPE: Shape = Shape::String;
}

impl CountedFieldType for Option<String> {
    const SHAPE: Shape = Shape::OptionalString;
}

impl CountedFieldType for Vec<String> {
    const SHAPE: Shape = Shape::Strings;
}

impl CountedFieldType for BTreeMap<String, String> {
    const SHAPE: Shape = Shape::Environment;
}

impl CountedFieldType for PackageSessionTypeExecution {
    const SHAPE: Shape = Shape::Execution;
}

impl CountedFieldType for PackageSessionTypeWorkingDirectory {
    const SHAPE: Shape = Shape::WorkingDirectory;
}

#[derive(Default)]
pub(super) struct ErrorCandidates {
    pub(super) execution_tag: Option<usize>,
    pub(super) directory_tag: Option<usize>,
    // File, definitions, definition, string array, environment, execution, directory.
    pub(super) wrong_shape_debug: [Option<usize>; 7],
}

#[derive(Default)]
pub(super) struct DefinitionStorage {
    pub(super) output: ContentStorage,
    pub(super) errors: ErrorCandidates,
}

fn max_candidate(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, None) => a,
        (None, b) => b,
    }
}

impl ErrorCandidates {
    fn merge(&mut self, other: Self) {
        self.execution_tag = max_candidate(self.execution_tag, other.execution_tag);
        self.directory_tag = max_candidate(self.directory_tag, other.directory_tag);
        for (old, new) in self
            .wrong_shape_debug
            .iter_mut()
            .zip(other.wrong_shape_debug)
        {
            *old = max_candidate(*old, new);
        }
    }
}

impl DefinitionStorage {
    fn append<E: de::Error>(&mut self, next: Self, track: Track<'_>) -> Result<(), E> {
        track
            .transfer(next.output.retained)
            .ok_or_else(|| E::custom("session type output storage overflow"))?;
        self.output = ContentStorage::map_entry(self.output, next.output)
            .ok_or_else(|| E::custom("session type output storage overflow"))?;
        self.errors.merge(next.errors);
        Ok(())
    }
}

// One declaration supplies the count dispatch and its exhaustive type guard.
// Adding a PackageSessionType field now requires updating the parser mirror.
macro_rules! counted_definition_fields {
    ($( $field:ident : $field_type:ty ),+ $(,)?) => {
        const FIELDS: [(&str, Shape); counted_definition_fields!(@count $( $field )+)] = [
            $( (stringify!($field), <$field_type as CountedFieldType>::SHAPE), )+
        ];

        #[allow(dead_code)]
        fn assert_counted_definition_fields(definition: &PackageSessionType) {
            let PackageSessionType { $( $field, )+ } = definition;
            $( let _: &$field_type = $field; )+
        }
    };
    (@count $( $field:ident )+) => {
        <[()]>::len(&[$( counted_definition_fields!(@unit $field) ),+])
    };
    (@unit $field:ident) => { () };
}

// The order is the declaration order in PackageSessionType, including defaults.
counted_definition_fields! {
    id: String,
    label: String,
    description: Option<String>,
    icon: Option<String>,
    role: String,
    interaction: String,
    traits: Vec<String>,
    lifecycle: String,
    execution: PackageSessionTypeExecution,
    command: String,
    args: Vec<String>,
    working_directory: PackageSessionTypeWorkingDirectory,
    environment: BTreeMap<String, String>,
    allowed_environment_overrides: Vec<String>,
    context: Vec<String>,
    target_id: Option<String>,
}

struct KeySeed;

enum Key {
    SessionTypes,
    Definition(usize),
    Other,
}

impl<'de> DeserializeSeed<'de> for KeySeed {
    type Value = Key;

    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Key, D::Error> {
        decoder.deserialize_identifier(self)
    }
}

impl<'de> Visitor<'de> for KeySeed {
    type Value = Key;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a session type field name")
    }

    fn visit_str<E: de::Error>(self, name: &str) -> Result<Key, E> {
        Ok(if name == "session_types" {
            Key::SessionTypes
        } else if let Some(index) = FIELDS.iter().position(|(field, _)| *field == name) {
            Key::Definition(index)
        } else {
            Key::Other
        })
    }

    fn visit_borrowed_str<E: de::Error>(self, name: &'de str) -> Result<Key, E> {
        self.visit_str(name)
    }
}

struct CountBytes(usize);

impl Write for CountBytes {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}

impl Shape {
    fn error_index(self) -> Option<usize> {
        match self {
            Self::File => Some(0),
            Self::Definitions => Some(1),
            Self::Definition => Some(2),
            Self::Strings => Some(3),
            Self::Environment => Some(4),
            Self::Execution => Some(5),
            Self::WorkingDirectory => Some(6),
            Self::String | Self::OptionalString => None,
        }
    }

    fn tagged(self, value: TaggedStorage) -> DefinitionStorage {
        let mut errors = ErrorCandidates::default();
        match self {
            Self::Execution => errors.execution_tag = value.unknown_tag_bytes,
            Self::WorkingDirectory => errors.directory_tag = value.unknown_tag_bytes,
            _ => unreachable!("only tagged shapes collect tagged storage"),
        }
        if value.unexpected_string_debug_bytes != 0 {
            errors.wrong_shape_debug[self.error_index().unwrap()] =
                Some(value.unexpected_string_debug_bytes);
        }
        DefinitionStorage {
            output: ContentStorage {
                retained: value.output_string,
                peak: value.peak_with_output,
            },
            errors,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ObservedShape<'a> {
    shape: Shape,
    track: Track<'a>,
    errors: ErrorTrack<'a>,
}
impl Shape {
    pub(super) fn with_track<'a>(
        self,
        track: Track<'a>,
        errors: ErrorTrack<'a>,
    ) -> ObservedShape<'a> {
        ObservedShape {
            shape: self,
            track,
            errors,
        }
    }
}
impl<'de> DeserializeSeed<'de> for Shape {
    type Value = DefinitionStorage;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        self.with_track(Track::default(), ErrorTrack::default())
            .deserialize(decoder)
    }
}
impl<'de> DeserializeSeed<'de> for ObservedShape<'_> {
    type Value = DefinitionStorage;

    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        match self.shape {
            Shape::Execution => TaggedField::Execution
                .with_track(self.track, self.errors)
                .deserialize(decoder)
                .map(|value| self.shape.tagged(value)),
            Shape::WorkingDirectory => TaggedField::WorkingDirectory
                .with_track(self.track, self.errors)
                .deserialize(decoder)
                .map(|value| self.shape.tagged(value)),
            _ => decoder.deserialize_any(self),
        }
    }
}

impl<'de> Visitor<'de> for ObservedShape<'_> {
    type Value = DefinitionStorage;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a session type value")
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<Self::Value, E> {
        let mut counted = DefinitionStorage::default();
        if let Some(index) = self.shape.error_index() {
            let mut bytes = CountBytes(0);
            write!(bytes, "{text:?}").map_err(|_| E::custom("session type error size overflow"))?;
            counted.errors.wrong_shape_debug[index] = Some(bytes.0);
            self.errors.wrong_string(bytes.0);
        } else {
            self.track
                .allocate(text.len())
                .ok_or_else(|| E::custom("session type output storage overflow"))?;
            counted.output = ContentStorage::copied_string(text.len())
                .ok_or_else(|| E::custom("session type output storage overflow"))?;
        }
        Ok(counted)
    }

    fn visit_borrowed_str<E: de::Error>(self, text: &'de str) -> Result<Self::Value, E> {
        self.visit_str(text)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(DefinitionStorage::default())
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
        self.visit_unit()
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
        self.visit_unit()
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
        self.visit_unit()
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
        self.visit_unit()
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut counted = DefinitionStorage::default();
        match self.shape {
            Shape::File => {
                if let Some(value) = sequence
                    .next_element_seed(Shape::Definitions.with_track(self.track, self.errors))?
                {
                    counted.append::<A::Error>(value, self.track)?;
                }
            }
            Shape::Definition => {
                for (_, shape) in FIELDS {
                    let Some(value) =
                        sequence.next_element_seed(shape.with_track(self.track, self.errors))?
                    else {
                        return Ok(counted);
                    };
                    counted.append::<A::Error>(value, self.track)?;
                }
            }
            Shape::Definitions | Shape::Strings => {
                let (shape, mut vector) = if matches!(self.shape, Shape::Definitions) {
                    (
                        Shape::Definition,
                        ContentContainer::output_vector::<PackageSessionType>(),
                    )
                } else {
                    (Shape::String, ContentContainer::output_vector::<String>())
                };
                loop {
                    // Only this schema seed selects a completed-definition scope.
                    let before = if matches!(self.shape, Shape::Definitions) {
                        Some(self.track.begin_scope().ok_or_else(|| {
                            <A::Error as de::Error>::custom("session type output storage overflow")
                        })?)
                    } else {
                        None
                    };
                    let Some(value) =
                        sequence.next_element_seed(shape.with_track(self.track, self.errors))?
                    else {
                        break;
                    };
                    // Err has propagated and None has left the loop. Sample and
                    // release before parent growth enters the shared byte count.
                    if let Some(before) = before {
                        self.track
                            .finish_scope(before, value.output.retained)
                            .ok_or_else(|| {
                                <A::Error as de::Error>::custom(
                                    "session type output storage overflow",
                                )
                            })?;
                    }
                    let old = vector.buffer_charge().ok_or_else(|| {
                        <A::Error as de::Error>::custom("session type output storage overflow")
                    })?;
                    vector.push(value.output).ok_or_else(|| {
                        <A::Error as de::Error>::custom("session type output storage overflow")
                    })?;
                    let new = vector.buffer_charge().ok_or_else(|| {
                        <A::Error as de::Error>::custom("session type output storage overflow")
                    })?;
                    if old != new {
                        self.track.replace(old, new).ok_or_else(|| {
                            <A::Error as de::Error>::custom("session type output storage overflow")
                        })?;
                    }
                    self.track.transfer(value.output.retained).ok_or_else(|| {
                        <A::Error as de::Error>::custom("session type output storage overflow")
                    })?;
                    counted.errors.merge(value.errors);
                }
                counted.output = vector.storage().ok_or_else(|| {
                    <A::Error as de::Error>::custom("session type output storage overflow")
                })?;
                return Ok(counted);
            }
            _ => {}
        }
        // Production rejects extra File elements with TrailingCharacters.
        // This loop only counts additional decoder scratch for those elements.
        // The production decoder determines the error.
        while sequence.next_element::<IgnoredAny>()?.is_some() {}
        Ok(counted)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut counted = DefinitionStorage::default();
        if matches!(self.shape, Shape::Environment) {
            let mut entries = 0usize;
            let mut previous_nodes = 0usize;
            let mut strings = ContentStorage::default();
            while let Some(key) =
                map.next_key_seed(Shape::String.with_track(self.track, self.errors))?
            {
                let value =
                    map.next_value_seed(Shape::String.with_track(self.track, self.errors))?;
                let entry =
                    ContentStorage::map_entry(key.output, value.output).ok_or_else(|| {
                        <A::Error as de::Error>::custom("session type output storage overflow")
                    })?;
                strings = ContentStorage::map_entry(strings, entry).ok_or_else(|| {
                    <A::Error as de::Error>::custom("session type output storage overflow")
                })?;
                entries = entries.checked_add(1).ok_or_else(|| {
                    <A::Error as de::Error>::custom("session type output storage overflow")
                })?;
                let nodes =
                    crate::lua_memory::layout::btree_nodes_checked::<String, String>(entries)
                        .ok_or_else(|| {
                            <A::Error as de::Error>::custom("session type output storage overflow")
                        })?;
                self.track
                    .allocate(nodes.checked_sub(previous_nodes).ok_or_else(|| {
                        <A::Error as de::Error>::custom("session type output storage overflow")
                    })?)
                    .ok_or_else(|| {
                        <A::Error as de::Error>::custom("session type output storage overflow")
                    })?;
                previous_nodes = nodes;
                counted.output.retained = nodes.checked_add(strings.retained).ok_or_else(|| {
                    <A::Error as de::Error>::custom("session type output storage overflow")
                })?;
                counted.output.peak =
                    counted
                        .output
                        .peak
                        .max(nodes.checked_add(strings.peak).ok_or_else(|| {
                            <A::Error as de::Error>::custom("session type output storage overflow")
                        })?);
            }
            // Repeated keys conservatively retain all key/value terms here.
            // Production still replaces duplicate values in its actual BTreeMap.
            return Ok(counted);
        }
        // Production rejects duplicate known keys with duplicate_field.
        // This count retains their storage as a conservative term.
        while let Some(key) = map.next_key_seed(KeySeed)? {
            let shape = match (self.shape, key) {
                (Shape::File, Key::SessionTypes) => Some(Shape::Definitions),
                (Shape::Definition, Key::Definition(index)) => Some(FIELDS[index].1),
                _ => None,
            };
            if let Some(shape) = shape {
                counted.append::<A::Error>(
                    map.next_value_seed(shape.with_track(self.track, self.errors))?,
                    self.track,
                )?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(counted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(input: &str) -> DefinitionStorage {
        let mut decoder = serde_json::Deserializer::from_str(input);
        let result = Shape::File.deserialize(&mut decoder).unwrap();
        decoder.end().unwrap();
        result
    }

    #[test]
    fn outer_and_definition_sequences_use_production_field_order() {
        let map = r#"{"session_types":[{"id":"agent","label":"Agent","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent","execution":{"mode":"shell_command"},"working_directory":{"policy":"relative","path":"sub"},"environment":{"KEY":"VALUE"}}]}"#;
        let sequence = r#"[[["agent","Agent",null,null,"agent","interactive",[],"persistent",["shell_command"],"agent",[],["relative","sub"],{"KEY":"VALUE"},[],[],null]]]"#;
        let mapped: super::super::RepoSessionTypesFile = serde_json::from_str(map).unwrap();
        let positional: super::super::RepoSessionTypesFile =
            serde_json::from_str(sequence).unwrap();
        assert_eq!(mapped.session_types, positional.session_types);
        assert_eq!(count(map).output.retained, count(sequence).output.retained);
        assert_eq!(count(sequence).errors.execution_tag, None);
        assert_eq!(count(sequence).errors.directory_tag, None);
    }

    #[test]
    fn valid_string_fields_are_not_error_candidates() {
        let label = "x".repeat(1024 * 1024);
        let input = format!(
            r#"{{"session_types":[{{"id":"agent","label":"{label}","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent"}}]}}"#
        );
        let _: super::super::RepoSessionTypesFile = serde_json::from_str(&input).unwrap();
        let result = count(&input);
        assert!(result.output.retained >= label.len());
        assert_eq!(result.errors.execution_tag, None);
        assert_eq!(result.errors.directory_tag, None);
        assert_eq!(result.errors.wrong_shape_debug, [None; 7]);
    }

    #[test]
    fn wrong_shape_string_is_counted_at_its_schema_position() {
        let result = count(r#"{"session_types":[{"traits":"\n","label":"accepted string"}]}"#);
        assert_eq!(result.errors.wrong_shape_debug[3], Some(4));
        assert_eq!(result.errors.wrong_shape_debug.iter().flatten().count(), 1);
    }

    #[test]
    fn duplicate_environment_storage_is_a_conservative_term() {
        let once = count(r#"{"session_types":[{"environment":{"K":"V"}}]}"#);
        let twice = count(r#"{"session_types":[{"environment":{"K":"V","K":"V"}}]}"#);
        assert!(twice.output.retained > once.output.retained);
        assert_eq!(twice.errors.wrong_shape_debug, [None; 7]);
    }
}
