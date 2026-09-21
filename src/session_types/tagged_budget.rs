//! Count the two tagged fields without choosing a production error.

use std::fmt::{self, Write};

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use super::content_budget::{ContentContainer, ContentStorage, ContentVisitor};

#[derive(Clone, Copy)]
pub(super) enum TaggedField {
    Execution,
    WorkingDirectory,
}

#[derive(Default)]
pub(super) struct TaggedStorage {
    pub(super) content: ContentStorage,
    pub(super) peak_with_output: usize,
    pub(super) output_string: usize,
    // These are token lengths, not complete formatted error reservations.
    pub(super) unknown_tag_bytes: Option<usize>,
    pub(super) unexpected_string_debug_bytes: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Symbol {
    Mode,
    Policy,
    Path,
    RelativeExecutable,
    ShellCommand,
    PackageRoot,
    Relative,
    Other,
}

struct StringInfo {
    symbol: Symbol,
    bytes: usize,
    debug_bytes: usize,
    borrowed: bool,
}

#[derive(Default)]
struct ValueStorage {
    storage: ContentStorage,
    string: Option<StringInfo>,
}

struct ValueSeed;
struct ValueVisitor;

impl<'de> DeserializeSeed<'de> for ValueSeed {
    type Value = ValueStorage;

    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        decoder.deserialize_any(ValueVisitor)
    }
}

struct DebugBytes(usize);

impl Write for DebugBytes {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0 = self.0.checked_add(value.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}

fn string_value<E: de::Error>(value: &str, borrowed: bool) -> Result<ValueStorage, E> {
    let symbol = match value {
        "mode" => Symbol::Mode,
        "policy" => Symbol::Policy,
        "path" => Symbol::Path,
        "relative_executable" => Symbol::RelativeExecutable,
        "shell_command" => Symbol::ShellCommand,
        "package_root" => Symbol::PackageRoot,
        "relative" => Symbol::Relative,
        _ => Symbol::Other,
    };
    let mut debug = DebugBytes(0);
    write!(debug, "{value:?}").map_err(|_| E::custom("session type Content storage overflow"))?;
    let storage = if borrowed {
        ContentStorage::default()
    } else {
        ContentStorage::copied_string(value.len())
            .ok_or_else(|| E::custom("session type Content storage overflow"))?
    };
    Ok(ValueStorage {
        storage,
        string: Some(StringInfo {
            symbol,
            bytes: value.len(),
            debug_bytes: debug.0,
            borrowed,
        }),
    })
}

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = ValueStorage;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(ValueStorage::default())
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

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        string_value(value, false)
    }

    fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
        string_value(value, true)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, sequence: A) -> Result<Self::Value, A::Error> {
        Ok(ValueStorage {
            storage: ContentVisitor.visit_seq(sequence)?,
            string: None,
        })
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        Ok(ValueStorage {
            storage: ContentVisitor.visit_map(map)?,
            string: None,
        })
    }
}

impl TaggedField {
    fn tag_key(self) -> Symbol {
        match self {
            Self::Execution => Symbol::Mode,
            Self::WorkingDirectory => Symbol::Policy,
        }
    }

    fn classify_tag(self, value: &ValueStorage, counted: &mut TaggedStorage) -> bool {
        let Some(string) = value.string.as_ref() else {
            return false;
        };
        let known = match self {
            Self::Execution => matches!(
                string.symbol,
                Symbol::RelativeExecutable | Symbol::ShellCommand
            ),
            Self::WorkingDirectory => {
                matches!(string.symbol, Symbol::PackageRoot | Symbol::Relative)
            }
        };
        if !known {
            counted.unknown_tag_bytes =
                Some(counted.unknown_tag_bytes.unwrap_or(0).max(string.bytes));
        }
        matches!(self, Self::WorkingDirectory) && string.symbol == Symbol::Relative
    }
}

impl<'de> DeserializeSeed<'de> for TaggedField {
    type Value = TaggedStorage;

    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        decoder.deserialize_any(self)
    }
}

fn finish<E: de::Error>(
    mut counted: TaggedStorage,
    container: ContentContainer,
    relative: bool,
    path: Option<StringInfo>,
) -> Result<TaggedStorage, E> {
    counted.content = container
        .storage()
        .ok_or_else(|| E::custom("session type Content storage overflow"))?;
    let path = path.filter(|_| relative);
    counted.output_string = path.as_ref().map_or(0, |value| value.bytes);
    let copied = path
        .filter(|value| value.borrowed)
        .map_or(0, |value| value.bytes);
    counted.peak_with_output = counted
        .content
        .with_borrowed_output(copied)
        .ok_or_else(|| E::custom("session type Content storage overflow"))?;
    Ok(counted)
}

impl<'de> Visitor<'de> for TaggedField {
    type Value = TaggedStorage;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a tagged session type field")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut container = ContentContainer::map();
        let mut counted = TaggedStorage::default();
        let mut tag_seen = false;
        let mut relative = false;
        let mut path = None;
        while let Some(key) = map.next_key_seed(ValueSeed)? {
            let symbol = key.string.as_ref().map(|key| key.symbol);
            let value = map.next_value_seed(ValueSeed)?;
            if symbol == Some(self.tag_key()) {
                if !tag_seen {
                    relative = self.classify_tag(&value, &mut counted);
                    tag_seen = true;
                }
                continue;
            }
            let entry = ContentStorage::map_entry(key.storage, value.storage).ok_or_else(|| {
                <A::Error as de::Error>::custom("session type Content storage overflow")
            })?;
            container.push(entry).ok_or_else(|| {
                <A::Error as de::Error>::custom("session type Content storage overflow")
            })?;
            // A repeated path makes production decoding fail after buffering.
            // Counting its first string path remains conservative for this term.
            if symbol == Some(Symbol::Path) && path.is_none() {
                path = value.string;
            }
        }
        finish(counted, container, relative, path)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut counted = TaggedStorage::default();
        let relative = sequence
            .next_element_seed(ValueSeed)?
            .is_some_and(|tag| self.classify_tag(&tag, &mut counted));
        let mut container = ContentContainer::sequence();
        let mut first = true;
        let mut path = None;
        while let Some(value) = sequence.next_element_seed(ValueSeed)? {
            container.push(value.storage).ok_or_else(|| {
                <A::Error as de::Error>::custom("session type Content storage overflow")
            })?;
            if first {
                path = value.string;
                first = false;
            }
        }
        finish(counted, container, relative, path)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let counted = string_value::<E>(value, false)?;
        Ok(TaggedStorage {
            unexpected_string_debug_bytes: counted.string.expect("string metadata").debug_bytes,
            ..TaggedStorage::default()
        })
    }

    fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
        self.visit_str(value)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(TaggedStorage::default())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(field: TaggedField, input: &str) -> TaggedStorage {
        let mut decoder = serde_json::Deserializer::from_str(input);
        let result = field.deserialize(&mut decoder).unwrap();
        decoder.end().unwrap();
        result
    }

    #[test]
    fn relative_path_copy_coexists_with_nested_duplicate_entries() {
        let input =
            r#"{"path":"subdir","y":["\n",{"mode":"\u0080"}],"y":["\n"],"policy":"relative"}"#;
        let parsed: super::super::PackageSessionTypeWorkingDirectory =
            serde_json::from_str(input).unwrap();
        assert!(matches!(
            parsed,
            super::super::PackageSessionTypeWorkingDirectory::Relative { .. }
        ));
        let counted = count(TaggedField::WorkingDirectory, input);
        assert_eq!(counted.output_string, 6);
        assert_eq!(
            counted.peak_with_output,
            counted.content.peak.max(counted.content.retained + 6)
        );
        assert!(counted.content.retained > 6);
        assert_eq!(counted.unknown_tag_bytes, None);
    }

    #[test]
    fn escaped_relative_path_transfers_without_a_second_copy() {
        let counted = count(
            TaggedField::WorkingDirectory,
            r#"["relative","s\u0075bdir"]"#,
        );
        assert_eq!(counted.output_string, 6);
        assert_eq!(counted.peak_with_output, counted.content.peak);
        let parsed: super::super::PackageSessionTypeWorkingDirectory =
            serde_json::from_str(r#"["relative","s\u0075bdir"]"#).unwrap();
        assert!(matches!(
            parsed,
            super::super::PackageSessionTypeWorkingDirectory::Relative { .. }
        ));
    }

    #[test]
    fn unit_sequence_has_no_buffered_remainder() {
        let counted = count(TaggedField::Execution, r#"["shell_command"]"#);
        assert_eq!(counted.content, ContentStorage::default());
        assert_eq!(counted.output_string, 0);
    }

    #[test]
    fn only_tag_and_wrong_shape_strings_supply_error_candidates() {
        let input = r#"{"mode":"other","label":"a long unknown field"}"#;
        let counted = count(TaggedField::Execution, input);
        assert_eq!(counted.unknown_tag_bytes, Some(5));
        assert_eq!(counted.unexpected_string_debug_bytes, 0);
        let counted = count(TaggedField::Execution, r#""\n""#);
        assert_eq!(counted.unexpected_string_debug_bytes, 4);
    }
}
