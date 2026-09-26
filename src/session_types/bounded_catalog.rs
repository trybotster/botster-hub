use std::fmt;
use std::fs::File;
use std::io::Read;
use std::mem::{align_of, size_of};
use std::path::Path;

use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::*;

macro_rules! field_deserialize {
    ($name:ty, {$($text:literal => $variant:expr),* $(,)?}) => {
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where D: Deserializer<'de> {
                struct FieldVisitor;
                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = $name;
                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("a catalog field name")
                    }
                    fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> { self.visit_str(value) }
                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                        Ok(match value { $($text => $variant,)* _ => <$name>::Other })
                    }
                }
                deserializer.deserialize_identifier(FieldVisitor)
            }
        }
    };
}

macro_rules! reject_scalars_fixed {
    () => {
        fn visit_borrowed_str<E>(self, _value: &'de str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_string<E>(self, _value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("catalog value has the wrong shape"))
        }
    };
}

macro_rules! reject_non_string_fixed {
    () => {
        fn visit_bool<E: de::Error>(self, _value: bool) -> Result<Self::Value, E> {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_i64<E: de::Error>(self, _value: i64) -> Result<Self::Value, E> {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_u64<E: de::Error>(self, _value: u64) -> Result<Self::Value, E> {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_f64<E: de::Error>(self, _value: f64) -> Result<Self::Value, E> {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Err(E::custom("catalog value has the wrong shape"))
        }
        fn visit_seq<A: SeqAccess<'de>>(self, _sequence: A) -> Result<Self::Value, A::Error> {
            Err(de::Error::custom("catalog value has the wrong shape"))
        }
        fn visit_map<A: MapAccess<'de>>(self, _map: A) -> Result<Self::Value, A::Error> {
            Err(de::Error::custom("catalog value has the wrong shape"))
        }
    };
}

#[derive(Debug, Clone, Deserialize)]
struct RepoCatalogFile {
    #[serde(default)]
    session_types: Vec<RepoCatalogDefinition>,
}

#[derive(Debug, Clone, Deserialize)]
struct RepoCatalogDefinition {
    id: String,
    label: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    icon: Option<String>,
    role: String,
    interaction: String,
    #[serde(default)]
    traits: Vec<String>,
    lifecycle: String,
    #[serde(default, deserialize_with = "deserialize_execution")]
    execution: PackageSessionTypeExecution,
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_working_directory")]
    working_directory: CatalogWorkingDirectory,
    #[serde(
        default,
        rename = "environment",
        deserialize_with = "discard_environment"
    )]
    _environment: (),
    #[serde(default)]
    allowed_environment_overrides: Vec<String>,
    #[serde(default)]
    context: Vec<String>,
    #[serde(default)]
    target_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
enum CatalogWorkingDirectory {
    #[default]
    PackageRoot,
    Relative,
}

fn deserialize_execution<'de, D>(deserializer: D) -> Result<PackageSessionTypeExecution, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(ExecutionOwned::deserialize(deserializer)?.0)
}

fn deserialize_working_directory<'de, D>(
    deserializer: D,
) -> Result<CatalogWorkingDirectory, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(WorkingDirectoryOwned::deserialize(deserializer)?.0)
}

struct RelativePathVisitor;
impl<'de> Visitor<'de> for RelativePathVisitor {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a relative working-directory string")
    }
    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<(), E>
    where
        E: de::Error,
    {
        validate_relative_manifest_path(value, "working directory")
            .map_err(|_| E::custom("invalid relative working directory"))
    }
    fn visit_str<E>(self, value: &str) -> Result<(), E>
    where
        E: de::Error,
    {
        validate_relative_manifest_path(value, "working directory")
            .map_err(|_| E::custom("invalid relative working directory"))
    }
    reject_non_string_fixed!();
}

fn discard_environment<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    struct EnvironmentVisitor;

    impl<'de> Visitor<'de> for EnvironmentVisitor {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an environment string map")
        }

        fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
        where
            A: MapAccess<'de>,
        {
            while map.next_key_seed(EnvironmentKeySeed)?.is_some() {
                map.next_value::<DiscardString>()?;
            }
            Ok(())
        }
        reject_scalars_fixed!();
    }

    deserializer.deserialize_any(EnvironmentVisitor)
}

struct EnvironmentCheck;
impl<'de> Deserialize<'de> for EnvironmentCheck {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        discard_environment(deserializer)?;
        Ok(EnvironmentCheck)
    }
}

struct EnvironmentKeySeed;
impl<'de> DeserializeSeed<'de> for EnvironmentKeySeed {
    type Value = ();
    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(EnvironmentKeyVisitor)
    }
}

struct EnvironmentKeyVisitor;
impl<'de> Visitor<'de> for EnvironmentKeyVisitor {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an environment variable name")
    }
    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<(), E>
    where
        E: de::Error,
    {
        validate_environment_name(value).map_err(|_| E::custom("invalid environment variable name"))
    }
    fn visit_str<E>(self, value: &str) -> Result<(), E>
    where
        E: de::Error,
    {
        validate_environment_name(value).map_err(|_| E::custom("invalid environment variable name"))
    }
}

struct DiscardString;
impl<'de> Deserialize<'de> for DiscardString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DiscardStringVisitor;
        impl<'de> Visitor<'de> for DiscardStringVisitor {
            type Value = DiscardString;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string")
            }
            fn visit_borrowed_str<E>(self, _value: &'de str) -> Result<Self::Value, E> {
                Ok(DiscardString)
            }
            fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
                Ok(DiscardString)
            }
            reject_non_string_fixed!();
        }
        deserializer.deserialize_any(DiscardStringVisitor)
    }
}

#[derive(Clone, Copy)]
struct ExecutionCheck;
impl<'de> Deserialize<'de> for ExecutionCheck {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ExecutionOwned::deserialize(deserializer)?;
        Ok(ExecutionCheck)
    }
}

struct ExecutionOwned(PackageSessionTypeExecution);
impl<'de> Deserialize<'de> for ExecutionOwned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ExecutionVisitor;
        impl<'de> Visitor<'de> for ExecutionVisitor {
            type Value = ExecutionOwned;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an execution object")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut mode = None;
                while let Some(field) = map.next_key::<ExecutionField>()? {
                    match field {
                        ExecutionField::Mode if mode.is_none() => {
                            mode = Some(map.next_value::<ExecutionMode>()?)
                        }
                        ExecutionField::Mode => {
                            return Err(de::Error::custom("duplicate execution mode"));
                        }
                        ExecutionField::Other => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                let mode = mode.ok_or_else(|| de::Error::custom("execution mode is required"))?;
                Ok(ExecutionOwned(mode.0))
            }
            reject_scalars_fixed!();
        }
        deserializer.deserialize_any(ExecutionVisitor)
    }
}

#[derive(Clone, Copy)]
enum ExecutionField {
    Mode,
    Other,
}
field_deserialize!(ExecutionField, { "mode" => ExecutionField::Mode });

struct ExecutionMode(PackageSessionTypeExecution);
impl<'de> Deserialize<'de> for ExecutionMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ModeVisitor;
        impl<'de> Visitor<'de> for ModeVisitor {
            type Value = ExecutionMode;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a supported execution mode")
            }
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(value)
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "relative_executable" => Ok(ExecutionMode(
                        PackageSessionTypeExecution::RelativeExecutable,
                    )),
                    "shell_command" => Ok(ExecutionMode(PackageSessionTypeExecution::ShellCommand)),
                    _ => Err(E::custom("unsupported execution mode")),
                }
            }
            reject_non_string_fixed!();
        }
        deserializer.deserialize_any(ModeVisitor)
    }
}

#[derive(Clone, Copy)]
struct WorkingDirectoryCheck;
impl<'de> Deserialize<'de> for WorkingDirectoryCheck {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        WorkingDirectoryOwned::deserialize(deserializer)?;
        Ok(WorkingDirectoryCheck)
    }
}

struct WorkingDirectoryOwned(CatalogWorkingDirectory);
impl<'de> Deserialize<'de> for WorkingDirectoryOwned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WorkingVisitor;
        impl<'de> Visitor<'de> for WorkingVisitor {
            type Value = WorkingDirectoryOwned;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a working-directory object")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut policy = None;
                let mut path = false;
                while let Some(field) = map.next_key::<WorkingField>()? {
                    match field {
                        WorkingField::Policy if policy.is_none() => {
                            policy = Some(map.next_value::<WorkingPolicy>()?)
                        }
                        WorkingField::Policy => {
                            return Err(de::Error::custom("duplicate working-directory policy"));
                        }
                        WorkingField::Path if !path => {
                            path = true;
                            map.next_value_seed(RelativePathSeed)?;
                        }
                        WorkingField::Path => {
                            return Err(de::Error::custom("duplicate working-directory path"));
                        }
                        WorkingField::Other => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                match policy {
                    Some(WorkingPolicy(true)) if !path => Err(de::Error::custom(
                        "relative working directory requires path",
                    )),
                    Some(WorkingPolicy(false)) if path => Err(de::Error::custom(
                        "package-root working directory rejects path",
                    )),
                    Some(WorkingPolicy(true)) => {
                        Ok(WorkingDirectoryOwned(CatalogWorkingDirectory::Relative))
                    }
                    Some(WorkingPolicy(false)) => {
                        Ok(WorkingDirectoryOwned(CatalogWorkingDirectory::PackageRoot))
                    }
                    None => Err(de::Error::custom("working-directory policy is required")),
                }
            }
            reject_scalars_fixed!();
        }
        deserializer.deserialize_any(WorkingVisitor)
    }
}

struct RelativePathSeed;
impl<'de> DeserializeSeed<'de> for RelativePathSeed {
    type Value = ();
    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(RelativePathVisitor)
    }
}

#[derive(Clone, Copy)]
enum WorkingField {
    Policy,
    Path,
    Other,
}
field_deserialize!(WorkingField, { "policy" => WorkingField::Policy, "path" => WorkingField::Path });

#[derive(Clone, Copy)]
struct WorkingPolicy(bool);
impl<'de> Deserialize<'de> for WorkingPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PolicyVisitor;
        impl<'de> Visitor<'de> for PolicyVisitor {
            type Value = WorkingPolicy;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a supported working-directory policy")
            }
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(value)
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "package_root" => Ok(WorkingPolicy(false)),
                    "relative" => Ok(WorkingPolicy(true)),
                    _ => Err(E::custom("unsupported working-directory policy")),
                }
            }
            reject_non_string_fixed!();
        }
        deserializer.deserialize_any(PolicyVisitor)
    }
}

#[derive(Debug, Default)]
struct Admission {
    definitions: usize,
    retained_string_bytes: usize,
    retained_vec_bytes: usize,
}

impl Admission {
    fn add_string(&mut self, value: MeasuredString) -> Result<(), &'static str> {
        self.retained_string_bytes = self
            .retained_string_bytes
            .checked_add(value.len)
            .ok_or("catalog admission size overflow")?;
        Ok(())
    }

    fn add_list(&mut self, value: MeasuredStrings) -> Result<(), &'static str> {
        self.retained_string_bytes = self
            .retained_string_bytes
            .checked_add(value.bytes)
            .ok_or("catalog admission size overflow")?;
        self.retained_vec_bytes = self
            .retained_vec_bytes
            .checked_add(
                vec_capacity_for_pushes(value.count, size_of::<String>())
                    .checked_mul(size_of::<String>())
                    .ok_or("catalog admission size overflow")?,
            )
            .ok_or("catalog admission size overflow")?;
        Ok(())
    }

    fn owned_layout_bytes(&self) -> Option<usize> {
        vec_capacity_for_pushes(self.definitions, size_of::<RepoCatalogDefinition>())
            .checked_mul(size_of::<RepoCatalogDefinition>())?
            .checked_add(self.retained_vec_bytes)?
            .checked_add(self.retained_string_bytes)
    }
}

fn repo_catalog_owned_layout(definitions: &Vec<RepoCatalogDefinition>) -> Option<usize> {
    let mut bytes = definitions
        .capacity()
        .checked_mul(size_of::<RepoCatalogDefinition>())?;
    for definition in definitions {
        for value in [
            &definition.id,
            &definition.label,
            &definition.role,
            &definition.interaction,
            &definition.lifecycle,
            &definition.command,
        ] {
            bytes = bytes.checked_add(value.capacity())?;
        }
        for value in [
            definition.description.as_ref(),
            definition.icon.as_ref(),
            definition.target_id.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            bytes = bytes.checked_add(value.capacity())?;
        }
        for values in [
            &definition.traits,
            &definition.args,
            &definition.allowed_environment_overrides,
            &definition.context,
        ] {
            bytes = bytes.checked_add(values.capacity().checked_mul(size_of::<String>())?)?;
            for value in values {
                bytes = bytes.checked_add(value.capacity())?;
            }
        }
    }
    Some(bytes)
}

fn vec_capacity_for_pushes(len: usize, element_size: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let minimum = if element_size == 1 {
        8
    } else if element_size <= 1024 {
        4
    } else {
        1
    };
    let mut capacity = minimum;
    while capacity < len {
        capacity = capacity.saturating_mul(2);
    }
    capacity
}

#[derive(Clone, Copy)]
struct MeasuredString {
    len: usize,
}

impl<'de> Deserialize<'de> for MeasuredString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StringVisitor;
        impl<'de> Visitor<'de> for StringVisitor {
            type Value = MeasuredString;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string")
            }
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
                Ok(MeasuredString { len: value.len() })
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(MeasuredString { len: value.len() })
            }
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(MeasuredString { len: value.len() })
            }
            reject_non_string_fixed!();
        }
        deserializer.deserialize_any(StringVisitor)
    }
}

#[derive(Clone, Copy)]
struct MeasuredStrings {
    count: usize,
    bytes: usize,
}

impl<'de> Deserialize<'de> for MeasuredStrings {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StringsVisitor;
        impl<'de> Visitor<'de> for StringsVisitor {
            type Value = MeasuredStrings;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string array")
            }
            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut count = 0usize;
                let mut bytes = 0usize;
                while let Some(value) = sequence.next_element::<MeasuredString>()? {
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| de::Error::custom("catalog admission size overflow"))?;
                    bytes = bytes
                        .checked_add(value.len)
                        .ok_or_else(|| de::Error::custom("catalog admission size overflow"))?;
                }
                Ok(MeasuredStrings { count, bytes })
            }
            reject_scalars_fixed!();
        }
        deserializer.deserialize_any(StringsVisitor)
    }
}

#[derive(Clone, Copy)]
enum FileField {
    SessionTypes,
    Other,
}

#[derive(Clone, Copy)]
enum DefinitionField {
    Id,
    Label,
    Description,
    Icon,
    Role,
    Interaction,
    Traits,
    Lifecycle,
    Execution,
    Command,
    Args,
    WorkingDirectory,
    Environment,
    AllowedEnvironmentOverrides,
    Context,
    TargetId,
    Other,
}

field_deserialize!(FileField, { "session_types" => FileField::SessionTypes });
field_deserialize!(DefinitionField, {
    "id" => DefinitionField::Id,
    "label" => DefinitionField::Label,
    "description" => DefinitionField::Description,
    "icon" => DefinitionField::Icon,
    "role" => DefinitionField::Role,
    "interaction" => DefinitionField::Interaction,
    "traits" => DefinitionField::Traits,
    "lifecycle" => DefinitionField::Lifecycle,
    "execution" => DefinitionField::Execution,
    "command" => DefinitionField::Command,
    "args" => DefinitionField::Args,
    "working_directory" => DefinitionField::WorkingDirectory,
    "environment" => DefinitionField::Environment,
    "allowed_environment_overrides" => DefinitionField::AllowedEnvironmentOverrides,
    "context" => DefinitionField::Context,
    "target_id" => DefinitionField::TargetId
});

struct AdmissionSeed;

impl<'de> DeserializeSeed<'de> for AdmissionSeed {
    type Value = Admission;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FileVisitor;
        impl<'de> Visitor<'de> for FileVisitor {
            type Value = Admission;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a session-types file object")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut admission = Admission::default();
                let mut seen = false;
                while let Some(field) = map.next_key::<FileField>()? {
                    match field {
                        FileField::SessionTypes if !seen => {
                            seen = true;
                            admission = map.next_value_seed(DefinitionsSeed)?;
                        }
                        FileField::SessionTypes => {
                            return Err(de::Error::custom("duplicate session_types field"));
                        }
                        FileField::Other => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(admission)
            }
            reject_scalars_fixed!();
        }
        deserializer.deserialize_any(FileVisitor)
    }
}

struct DefinitionsSeed;
impl<'de> DeserializeSeed<'de> for DefinitionsSeed {
    type Value = Admission;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DefinitionsVisitor;
        impl<'de> Visitor<'de> for DefinitionsVisitor {
            type Value = Admission;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a session type array")
            }
            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut total = Admission::default();
                while let Some(definition) = sequence.next_element_seed(DefinitionSeed)? {
                    total.definitions = total
                        .definitions
                        .checked_add(1)
                        .ok_or_else(|| de::Error::custom("catalog admission size overflow"))?;
                    total.retained_string_bytes = total
                        .retained_string_bytes
                        .checked_add(definition.retained_string_bytes)
                        .ok_or_else(|| de::Error::custom("catalog admission size overflow"))?;
                    total.retained_vec_bytes = total
                        .retained_vec_bytes
                        .checked_add(definition.retained_vec_bytes)
                        .ok_or_else(|| de::Error::custom("catalog admission size overflow"))?;
                }
                Ok(total)
            }
            reject_scalars_fixed!();
        }
        deserializer.deserialize_any(DefinitionsVisitor)
    }
}

struct DefinitionSeed;
impl<'de> DeserializeSeed<'de> for DefinitionSeed {
    type Value = Admission;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DefinitionVisitor;
        impl<'de> Visitor<'de> for DefinitionVisitor {
            type Value = Admission;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a session type object")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut admission = Admission::default();
                let mut required = [false; 6];
                while let Some(field) = map.next_key::<DefinitionField>()? {
                    match field {
                        DefinitionField::Id => {
                            required[0] = true;
                            admission
                                .add_string(map.next_value()?)
                                .map_err(de::Error::custom)?;
                        }
                        DefinitionField::Label => {
                            required[1] = true;
                            admission
                                .add_string(map.next_value()?)
                                .map_err(de::Error::custom)?;
                        }
                        DefinitionField::Description
                        | DefinitionField::Icon
                        | DefinitionField::TargetId => {
                            if let Some(value) = map.next_value::<Option<MeasuredString>>()? {
                                admission.add_string(value).map_err(de::Error::custom)?;
                            }
                        }
                        DefinitionField::Role => {
                            required[2] = true;
                            admission
                                .add_string(map.next_value()?)
                                .map_err(de::Error::custom)?;
                        }
                        DefinitionField::Interaction => {
                            required[3] = true;
                            admission
                                .add_string(map.next_value()?)
                                .map_err(de::Error::custom)?;
                        }
                        DefinitionField::Lifecycle => {
                            required[4] = true;
                            admission
                                .add_string(map.next_value()?)
                                .map_err(de::Error::custom)?;
                        }
                        DefinitionField::Command => {
                            required[5] = true;
                            admission
                                .add_string(map.next_value()?)
                                .map_err(de::Error::custom)?;
                        }
                        DefinitionField::Traits
                        | DefinitionField::Args
                        | DefinitionField::AllowedEnvironmentOverrides
                        | DefinitionField::Context => {
                            admission
                                .add_list(map.next_value()?)
                                .map_err(de::Error::custom)?;
                        }
                        DefinitionField::Execution => {
                            map.next_value::<ExecutionCheck>()?;
                        }
                        DefinitionField::WorkingDirectory => {
                            map.next_value::<WorkingDirectoryCheck>()?;
                        }
                        DefinitionField::Environment => {
                            map.next_value::<EnvironmentCheck>()?;
                        }
                        DefinitionField::Other => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                if required.iter().any(|seen| !seen) {
                    return Err(de::Error::custom(
                        "session type is missing a required field",
                    ));
                }
                Ok(admission)
            }
            reject_scalars_fixed!();
        }
        deserializer.deserialize_any(DefinitionVisitor)
    }
}

#[derive(Clone, Copy)]
enum DefinitionRef<'a> {
    Full(&'a PackageSessionType),
    Repo(&'a RepoCatalogDefinition),
}

macro_rules! definition_accessors {
    ($($name:ident: $ty:ty),* $(,)?) => {
        $(fn $name(self) -> &'a $ty {
            match self {
                Self::Full(value) => &value.$name,
                Self::Repo(value) => &value.$name,
            }
        })*
    };
}

impl<'a> DefinitionRef<'a> {
    definition_accessors! {
        id: String, label: String, description: Option<String>, icon: Option<String>,
        role: String, interaction: String, traits: Vec<String>, lifecycle: String,
        execution: PackageSessionTypeExecution, command: String, args: Vec<String>,
        allowed_environment_overrides: Vec<String>, context: Vec<String>, target_id: Option<String>,
    }

    fn working_directory_policy(self) -> &'static str {
        match self {
            Self::Full(value) => match value.working_directory {
                PackageSessionTypeWorkingDirectory::PackageRoot => "package_root",
                PackageSessionTypeWorkingDirectory::Relative { .. } => "relative",
            },
            Self::Repo(value) => match value.working_directory {
                CatalogWorkingDirectory::PackageRoot => "package_root",
                CatalogWorkingDirectory::Relative => "relative",
            },
        }
    }
}

#[derive(Clone, Copy)]
struct SourceRef<'a> {
    rank: SessionTypeSourceRank,
    source: &'static str,
    source_name: &'a str,
    definition: DefinitionRef<'a>,
    available: bool,
}

impl SourceRef<'_> {
    fn qualified_matches(&self, value: &str) -> bool {
        value.len() == self.source_name.len() + 1 + self.definition.id().len()
            && value.starts_with(self.source_name)
            && value.as_bytes().get(self.source_name.len()) == Some(&b'/')
            && value.ends_with(self.definition.id())
    }

    fn eligible(&self, target_id: &str) -> bool {
        if !self.available {
            return false;
        }
        match self.rank {
            SessionTypeSourceRank::Device => self
                .definition
                .target_id()
                .as_deref()
                .is_none_or(|pin| pin == target_id),
            SessionTypeSourceRank::Repo => self.source_name == target_id,
            SessionTypeSourceRank::Package => {
                self.definition.target_id().as_deref().unwrap_or("") == target_id
                    || (self.definition.target_id().is_none()
                        && target_id.strip_prefix("package:") == Some(self.source_name))
            }
        }
    }
}

struct Budget {
    limit: usize,
    used: usize,
}

impl Budget {
    fn with_error_reserve(limit: usize) -> Option<Self> {
        let mut budget = Self { limit, used: 0 };
        budget.retain(error_reserve_bytes()).then_some(budget)
    }

    fn retain(&mut self, bytes: usize) -> bool {
        let Some(next) = self.used.checked_add(bytes) else {
            return false;
        };
        if next > self.limit {
            return false;
        }
        self.used = next;
        true
    }

    fn release(&mut self, bytes: usize) {
        self.used = self
            .used
            .checked_sub(bytes)
            .expect("release retained catalog bytes");
    }
}

// These bounds depend on the versions checked by check-lua-memory-build-contract.
// Recheck the messages when this schema or its validation helpers change.
fn longest_text(values: &[&str]) -> usize {
    values.iter().map(|value| value.len()).max().unwrap_or(0)
}

fn schema_error_text_bytes() -> usize {
    let fixed = longest_text(&[
        "catalog value has the wrong shape",
        "invalid relative working directory",
        "invalid environment variable name",
        "duplicate execution mode",
        "execution mode is required",
        "unsupported execution mode",
        "duplicate working-directory policy",
        "duplicate working-directory path",
        "relative working directory requires path",
        "package-root working directory rejects path",
        "working-directory policy is required",
        "unsupported working-directory policy",
        "catalog admission size overflow",
        "duplicate session_types field",
        "session type is missing a required field",
        // This is the longest syntax message in serde_json 1.0.150 ErrorCode.
        "control character (\\u0000-\\u001F) found while parsing a string",
    ]);
    let expecting = longest_text(&[
        "a catalog field name",
        "a relative working-directory string",
        "an environment string map",
        "an environment variable name",
        "a string",
        "an execution object",
        "a supported execution mode",
        "a working-directory object",
        "a supported working-directory policy",
        "a string array",
        "a session-types file object",
        "a session type array",
        "a session type object",
        "struct RepoCatalogFile",
        "struct RepoCatalogDefinition",
    ]);
    // Wrong numeric and string values use fixed errors in the admission pass.
    // Only these payload-free Unexpected values can use a default visitor.
    let unexpected = longest_text(&["sequence", "map", "null"]);
    let wrong_shape = "invalid type: , expected ".len() + unexpected + expecting;
    // The owned derive pass can reject duplicate fields accepted by preflight.
    // This is the longest field name in either derived structure.
    let field = "allowed_environment_overrides".len();
    let derived_field = "duplicate field ``".len().max("missing field ``".len()) + field;
    fixed.max(wrong_shape).max(derived_field)
}

pub(super) fn validation_error_text_bytes() -> usize {
    // These maxima include the borrowed target and package/device validators.
    longest_text(&[
        "session type id must be a non-empty token of at most 128 characters",
        "session type id matches more than one source at the same precedence",
        "session type interaction and lifecycle must be bounded tokens",
        "session type presentation metadata exceeds its size limit",
        "session type traits must be unique bounded tokens",
        "session type label must be between 1 and 120 characters",
        "session type is not eligible for the requested target",
        "repo-local session type metadata could not be read",
        "session type working directory is unsafe",
    ])
}

pub(super) fn json_error_impl_bytes() -> usize {
    // ErrorCode has one tagged payload: Box<str> or io::Error, plus unit variants.
    // The pinned compiler needs at most one aligned word for its discriminant.
    let alignment = align_of::<Box<str>>()
        .max(align_of::<std::io::Error>())
        .max(align_of::<usize>());
    let payload = size_of::<Box<str>>()
        .max(size_of::<std::io::Error>())
        .next_multiple_of(alignment);
    // ErrorImpl adds the line and column. Round its final size for alignment.
    (alignment + payload + 2 * size_of::<usize>()).next_multiple_of(alignment)
}

const PARSE_ERROR_PREFIX: &str = "repo-local session type file is invalid: ";

fn parse_error_text_bytes() -> usize {
    // from_slice positions cannot exceed the input length plus one.
    let digits = decimal_digits(REPO_SESSION_TYPES_FILE_BYTE_CAPACITY + 1);
    PARSE_ERROR_PREFIX.len() + schema_error_text_bytes() + " at line  column ".len() + 2 * digits
}

fn error_reserve_bytes() -> usize {
    let message = schema_error_text_bytes();
    let helper = validation_error_text_bytes();
    // String formatting starts empty or reserves at most twice its literal size.
    // RawVec growth then requests at most twice the final text size, or 8 bytes.
    let message_capacity = 2 * message.max(8);
    let helper_capacity = 2 * helper.max(8);
    // Reserve both buffers during growth. The helper error can remain live while
    // a visitor creates its Serde error. into_boxed_str can overlap one more copy.
    // fix_position overlaps two ErrorImpl boxes but moves their message box.
    // The outer parser message uses exact capacity and cannot grow below.
    2 * helper_capacity
        + 2 * message_capacity
        + message
        + 2 * json_error_impl_bytes()
        + parse_error_text_bytes()
}

struct ParseErrorText {
    text: String,
    limit: usize,
}

impl fmt::Write for ParseErrorText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if value.len() > self.limit - self.text.len() {
            return Err(fmt::Error);
        }
        self.text.push_str(value);
        Ok(())
    }
}

fn repo_parse_error(error: serde_json::Error) -> SessionTypeError {
    use std::fmt::Write as _;
    let limit = parse_error_text_bytes();
    let mut message = ParseErrorText {
        text: String::with_capacity(limit),
        limit,
    };
    if write!(&mut message, "{PARSE_ERROR_PREFIX}{error}").is_err() {
        // Preserve a bounded diagnostic if a future schema changes the bound.
        message.text.clear();
        message
            .text
            .push_str("repo-local session type file is invalid");
    }
    SessionTypeError::new("invalid_repo_session_types", message.text)
}

fn read_repo_catalog(
    root: &Path,
    budget: &mut Budget,
) -> SessionTypeResult<Option<Vec<RepoCatalogDefinition>>> {
    let path_capacity = root
        .as_os_str()
        .as_encoded_bytes()
        .len()
        .checked_add(1)
        .and_then(|bytes| bytes.checked_add(REPO_SESSION_TYPES_FILE.len()))
        .unwrap_or(usize::MAX);
    // Unix File::open uses run_path_with_cstr. Long paths allocate a CString.
    // Rust 1.97 specializes CString::new(&[u8]) to one len+1 allocation, including
    // the NUL-error path. It drops that allocation before File::open returns.
    let Some(path_peak) = path_capacity
        .checked_add(1)
        .and_then(|ffi_capacity| path_capacity.checked_add(ffi_capacity))
    else {
        return Ok(None);
    };
    if !budget.retain(path_peak) {
        return Ok(None);
    }
    let mut path = PathBuf::with_capacity(path_capacity);
    path.push(root);
    path.push(REPO_SESSION_TYPES_FILE);
    let opened = File::open(&path);
    drop(path);
    budget.release(path_peak);
    let file = match opened {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Some(Vec::new())),
        Err(_error) => {
            return Err(SessionTypeError::new(
                "invalid_repo_session_types",
                "repo-local session type file could not be read",
            ));
        }
    };
    let capacity = usize::try_from(
        file.metadata()
            .map_err(|_error| {
                SessionTypeError::new(
                    "invalid_repo_session_types",
                    "repo-local session type metadata could not be read",
                )
            })?
            .len(),
    )
    .unwrap_or(usize::MAX);
    if capacity > REPO_SESSION_TYPES_FILE_BYTE_CAPACITY || !budget.retain(capacity) {
        return Ok(None);
    }
    let mut bytes = vec![0_u8; capacity];
    let mut filled = 0;
    while filled < capacity {
        let read = (&file).read(&mut bytes[filled..]).map_err(|_error| {
            SessionTypeError::new(
                "invalid_repo_session_types",
                "repo-local session type file could not be read",
            )
        })?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    let mut extra = [0_u8; 1];
    if (&file).read(&mut extra).map_err(|_error| {
        SessionTypeError::new(
            "invalid_repo_session_types",
            "repo-local session type file could not be read",
        )
    })? != 0
    {
        return Ok(None);
    }
    bytes.truncate(filled);
    let scratch = 3usize.saturating_mul(filled.max(8));
    if !budget.retain(scratch) {
        return Ok(None);
    }

    let admission = {
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        AdmissionSeed
            .deserialize(&mut deserializer)
            .and_then(|value| {
                deserializer.end()?;
                Ok(value)
            })
    }
    .map_err(repo_parse_error)?;
    let Some(retained) = admission.owned_layout_bytes() else {
        return Ok(None);
    };
    // With the pinned serde 1.0.228 / serde_json 1.0.150 pair, from_slice
    // dispatches copied JSON strings through StringVisitor::visit_str, whose
    // str::to_owned allocation has exactly the decoded length measured above.
    // `owned_layout_bytes` also models the pinned Vec push-growth capacities,
    // so this second reservation bounds the allocating pass before it begins.
    if !budget.retain(retained) {
        return Ok(None);
    }
    if !budget.retain(retained) {
        return Ok(None);
    }
    let mut parsed = {
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        RepoCatalogFile::deserialize(&mut deserializer).and_then(|value| {
            deserializer.end()?;
            Ok(value)
        })
    }
    .map_err(repo_parse_error)?;
    let Some(actual_retained) = repo_catalog_owned_layout(&parsed.session_types) else {
        return Ok(None);
    };
    assert!(
        actual_retained <= retained,
        "admission must bound parsed ownership"
    );
    budget.release(retained - actual_retained);
    budget.release(retained);
    budget.release(scratch);
    drop(bytes);
    budget.release(capacity);
    parsed
        .session_types
        .sort_unstable_by(|left, right| left.id.cmp(&right.id));
    validate_repo_catalog(&parsed.session_types)?;
    Ok(Some(parsed.session_types))
}

fn validate_repo_catalog(definitions: &[RepoCatalogDefinition]) -> SessionTypeResult<()> {
    for definition in definitions {
        if !bounded_token(&definition.id, 128, false) {
            return Err(SessionTypeError::new(
                "invalid_repo_session_types",
                "session type id must be a non-empty token of at most 128 characters",
            ));
        }
        if definition.label.trim().is_empty() || definition.label.len() > 120 {
            return Err(SessionTypeError::new(
                "invalid_repo_session_types",
                "session type label must be between 1 and 120 characters",
            ));
        }
        if definition
            .description
            .as_ref()
            .is_some_and(|v| v.len() > 1024)
            || definition.icon.as_ref().is_some_and(|v| v.len() > 256)
            || !bounded_token(&definition.role, 128, true)
            || !bounded_token(&definition.interaction, 64, false)
            || !bounded_token(&definition.lifecycle, 64, false)
            || definition.traits.len() > 32
            || definition
                .traits
                .iter()
                .any(|v| !bounded_token(v, 128, false))
            || definition
                .traits
                .iter()
                .enumerate()
                .any(|(i, v)| definition.traits[..i].contains(v))
        {
            return Err(SessionTypeError::new(
                "invalid_repo_session_types",
                "repo session type semantics are invalid",
            ));
        }
        match definition.execution {
            PackageSessionTypeExecution::RelativeExecutable => {
                validate_relative_manifest_path(&definition.command, "command").map_err(
                    |error| SessionTypeError::new("invalid_repo_session_types", error.message),
                )?
            }
            PackageSessionTypeExecution::ShellCommand if definition.command.trim().is_empty() => {
                return Err(SessionTypeError::new(
                    "invalid_repo_session_types",
                    "session type shell command must not be empty",
                ));
            }
            PackageSessionTypeExecution::ShellCommand => {}
        }
        for name in &definition.allowed_environment_overrides {
            validate_environment_name(name).map_err(|error| {
                SessionTypeError::new("invalid_repo_session_types", error.message)
            })?;
        }
    }
    if definitions.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(SessionTypeError::new(
            "invalid_repo_session_types",
            "duplicate session type id",
        ));
    }
    Ok(())
}

fn row_owned_bytes(
    winner: &SourceRef<'_>,
    peers: &[SourceRef<'_>],
    target_id_len: usize,
) -> Option<usize> {
    let definition = winner.definition;
    let mut bytes = 0usize;
    let mut add = |value: usize| {
        bytes = bytes.checked_add(value)?;
        Some(())
    };
    add(winner.source_name.len() + 1 + definition.id().len())?;
    add(winner.source_name.len())?;
    for value in [
        definition.id(),
        definition.label(),
        definition.role(),
        definition.interaction(),
        definition.lifecycle(),
        definition.command(),
    ] {
        add(value.len())?;
    }
    for value in [
        definition.description().as_ref(),
        definition.icon().as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        add(value.len())?;
    }
    for values in [
        definition.traits(),
        definition.args(),
        definition.allowed_environment_overrides(),
        definition.context(),
    ] {
        add(values.len().checked_mul(size_of::<String>())?)?;
        for value in values {
            add(value.len())?;
        }
    }
    add(winner.source.len() + winner.definition.working_directory_policy().len() + target_id_len)?;
    let overridden = peers
        .iter()
        .filter(|source| source.rank < winner.rank)
        .count();
    add(
        vec_capacity_for_pushes(overridden, size_of::<HubSessionTypeSource>())
            .checked_mul(size_of::<HubSessionTypeSource>())?,
    )?;
    for source in peers.iter().filter(|source| source.rank < winner.rank) {
        add(source.source.len() + source.source_name.len())?;
    }
    if overridden > 0 {
        add("overrides  lower-precedence definition(s)".len() + decimal_digits(overridden))?;
        add(vec_capacity_for_pushes(1, size_of::<String>()).checked_mul(size_of::<String>())?)?;
    }
    Some(bytes)
}

fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn build_row(winner: &SourceRef<'_>, peers: &[SourceRef<'_>], target_id: &str) -> HubSessionType {
    let definition = winner.definition;
    let mut qualified = String::with_capacity(winner.source_name.len() + 1 + definition.id().len());
    qualified.push_str(winner.source_name);
    qualified.push('/');
    qualified.push_str(definition.id());
    let overridden_sources = peers
        .iter()
        .filter(|source| source.rank < winner.rank)
        .map(|source| HubSessionTypeSource {
            kind: source.source.to_string(),
            name: source.source_name.to_string(),
        })
        .collect::<Vec<_>>();
    let diagnostics = if overridden_sources.is_empty() {
        Vec::new()
    } else {
        let count = overridden_sources.len();
        let mut message = String::with_capacity(
            "overrides  lower-precedence definition(s)".len() + decimal_digits(count),
        );
        use std::fmt::Write as _;
        write!(message, "overrides {count} lower-precedence definition(s)").expect("write string");
        vec![message]
    };
    HubSessionType {
        session_type_id: qualified,
        source_name: winner.source_name.to_string(),
        id: definition.id().clone(),
        source: winner.source.to_string(),
        editable: winner.rank != SessionTypeSourceRank::Package,
        overridden_sources,
        diagnostics,
        label: definition.label().clone(),
        description: definition.description().clone(),
        icon: definition.icon().clone(),
        role: definition.role().clone(),
        interaction: definition.interaction().clone(),
        traits: definition.traits().clone(),
        lifecycle: definition.lifecycle().clone(),
        execution: definition.execution().clone(),
        command: definition.command().clone(),
        args: definition.args().clone(),
        working_directory_policy: definition.working_directory_policy().to_string(),
        allowed_environment_overrides: definition.allowed_environment_overrides().clone(),
        context_keys: definition.context().clone(),
        target_id: target_id.to_string(),
        available: winner.available,
    }
}

fn collect_repo_files<'a>(
    state: &'a HubState,
    budget: &mut Budget,
) -> SessionTypeResult<Option<Vec<(&'a str, Vec<RepoCatalogDefinition>)>>> {
    let count = state
        .spawn_targets
        .iter()
        .filter(|target| target.enabled)
        .count();
    let layout = count.saturating_mul(size_of::<(&str, Vec<RepoCatalogDefinition>)>());
    if !budget.retain(layout) {
        return Ok(None);
    }
    let mut files = Vec::with_capacity(count);
    for target in state.spawn_targets.iter().filter(|target| target.enabled) {
        let Some(definitions) = read_repo_catalog(&target.root, budget)? else {
            return Ok(None);
        };
        files.push((target.target_id.as_str(), definitions));
    }
    Ok(Some(files))
}

fn collect_sources<'a, I>(
    records: I,
    state: &'a HubState,
    repo_files: &'a [(&'a str, Vec<RepoCatalogDefinition>)],
    budget: &mut Budget,
) -> SessionTypeResult<Option<Vec<SourceRef<'a>>>>
where
    I: Iterator<Item = &'a PackageRecord> + Clone,
{
    let package_count = records
        .clone()
        .filter(|record| matches!(record.manifest.source, Some(PackageSource::Path { .. })))
        .map(|record| record.session_types.len())
        .sum::<usize>();
    let device_count = state
        .device_session_type_sources
        .iter()
        .map(|source| source.session_types.len())
        .sum::<usize>();
    let repo_count = repo_files
        .iter()
        .map(|(_, values)| values.len())
        .sum::<usize>();
    let Some(count) = package_count
        .checked_add(device_count)
        .and_then(|v| v.checked_add(repo_count))
    else {
        return Ok(None);
    };
    let Some(layout) = count.checked_mul(size_of::<SourceRef<'_>>()) else {
        return Ok(None);
    };
    if !budget.retain(layout) {
        return Ok(None);
    }
    let mut sources = Vec::with_capacity(count);
    for record in records {
        if !matches!(record.manifest.source, Some(PackageSource::Path { .. })) {
            continue;
        }
        for definition in &record.session_types {
            validate_session_type(definition)?;
            sources.push(SourceRef {
                rank: SessionTypeSourceRank::Package,
                source: PACKAGE_SESSION_TYPE_SOURCE,
                source_name: &record.manifest.name,
                definition: DefinitionRef::Full(definition),
                available: record.state == PackageState::Enabled,
            });
        }
    }
    for source in &state.device_session_type_sources {
        validate_session_types(&source.session_types)
            .map_err(|message| SessionTypeError::new("invalid_device_session_types", message))?;
        for definition in &source.session_types {
            sources.push(SourceRef {
                rank: SessionTypeSourceRank::Device,
                source: DEVICE_SESSION_TYPE_SOURCE,
                source_name: DEVICE_SESSION_TYPE_SOURCE,
                definition: DefinitionRef::Full(definition),
                available: true,
            });
        }
    }
    for (target_id, definitions) in repo_files {
        for definition in definitions {
            sources.push(SourceRef {
                rank: SessionTypeSourceRank::Repo,
                source: REPO_SESSION_TYPE_SOURCE,
                source_name: target_id,
                definition: DefinitionRef::Repo(definition),
                available: true,
            });
        }
    }
    sources.sort_unstable_by(|left, right| {
        left.definition
            .id()
            .cmp(right.definition.id())
            .then(left.rank.cmp(&right.rank))
            .then(left.source_name.cmp(right.source_name))
    });
    Ok(Some(sources))
}

fn winner<'a>(peers: &'a [SourceRef<'a>]) -> SessionTypeResult<&'a SourceRef<'a>> {
    let best_rank = peers
        .iter()
        .map(|source| source.rank)
        .max()
        .expect("nonempty peers");
    let mut best = peers.iter().filter(|source| source.rank == best_rank);
    let winner = best.next().expect("best rank exists");
    if best.next().is_some() {
        return Err(SessionTypeError::new(
            "ambiguous_session_type",
            "session type id matches more than one source at the same precedence",
        ));
    }
    Ok(winner)
}

pub(super) fn list(
    records: &crate::packages::PackageRegistry,
    state: &HubState,
    target_id: &str,
    limit: usize,
) -> SessionTypeResult<Option<Vec<HubSessionType>>> {
    let Some(mut budget) = Budget::with_error_reserve(limit) else {
        return Ok(None);
    };
    ensure_enabled_admitted_target_borrowed(state, target_id)?;
    let Some(repo_files) = collect_repo_files(state, &mut budget)? else {
        return Ok(None);
    };
    let Some(mut sources) =
        collect_sources(records.package_records(), state, &repo_files, &mut budget)?
    else {
        return Ok(None);
    };
    sources.retain(|source| source.eligible(target_id));
    let mut row_count = 0usize;
    let mut row_bytes = 0usize;
    let mut start = 0;
    while start < sources.len() {
        let mut end = start + 1;
        while end < sources.len() && sources[end].definition.id() == sources[start].definition.id()
        {
            end += 1;
        }
        let selected = winner(&sources[start..end])?;
        if selected.available {
            row_count += 1;
            row_bytes = row_bytes
                .checked_add(
                    row_owned_bytes(selected, &sources[start..end], target_id.len()).ok_or_else(
                        || SessionTypeError::new("callback_memory_limit", "catalog size overflow"),
                    )?,
                )
                .ok_or_else(|| {
                    SessionTypeError::new("callback_memory_limit", "catalog size overflow")
                })?;
        }
        start = end;
    }
    let Some(total_rows) =
        row_bytes.checked_add(row_count.saturating_mul(size_of::<HubSessionType>()))
    else {
        return Ok(None);
    };
    let Some(rows_with_overlap) = total_rows.checked_mul(2) else {
        return Ok(None);
    };
    if !budget.retain(rows_with_overlap) {
        return Ok(None);
    }
    let mut rows = Vec::with_capacity(row_count);
    let mut start = 0;
    while start < sources.len() {
        let mut end = start + 1;
        while end < sources.len() && sources[end].definition.id() == sources[start].definition.id()
        {
            end += 1;
        }
        let selected = winner(&sources[start..end])?;
        if selected.available {
            rows.push(build_row(selected, &sources[start..end], target_id));
        }
        start = end;
    }
    budget.release(total_rows);
    rows.sort_unstable_by(|left, right| left.session_type_id.cmp(&right.session_type_id));
    Ok(Some(rows))
}

pub(super) fn show(
    records: &crate::packages::PackageRegistry,
    state: &HubState,
    target_id: &str,
    session_type_id: &str,
    limit: usize,
) -> SessionTypeResult<Option<HubSessionType>> {
    let Some(mut budget) = Budget::with_error_reserve(limit) else {
        return Ok(None);
    };
    ensure_enabled_admitted_target_borrowed(state, target_id)?;
    let Some(repo_files) = collect_repo_files(state, &mut budget)? else {
        return Ok(None);
    };
    let Some(mut sources) =
        collect_sources(records.package_records(), state, &repo_files, &mut budget)?
    else {
        return Ok(None);
    };
    let exists = sources.iter().any(|source| {
        source.definition.id() == session_type_id || source.qualified_matches(session_type_id)
    });
    sources.retain(|source| source.eligible(target_id));
    let mut matched = None;
    let mut start = 0;
    while start < sources.len() {
        let mut end = start + 1;
        while end < sources.len() && sources[end].definition.id() == sources[start].definition.id()
        {
            end += 1;
        }
        let eligible = &sources[start..end];
        if !eligible.is_empty() {
            let selected = winner(eligible)?;
            if selected.definition.id() == session_type_id
                || selected.qualified_matches(session_type_id)
            {
                if matched.is_some() {
                    return Err(SessionTypeError::new(
                        "ambiguous_session_type",
                        "session type id matches more than one source at the same precedence",
                    ));
                }
                let bytes = row_owned_bytes(selected, eligible, target_id.len());
                let Some(bytes) = bytes else {
                    return Ok(None);
                };
                let Some(with_overlap) = bytes.checked_mul(2) else {
                    return Ok(None);
                };
                if !budget.retain(with_overlap) {
                    return Ok(None);
                }
                matched = Some(build_row(selected, eligible, target_id));
                budget.release(bytes);
            }
        }
        start = end;
    }
    matched.map_or_else(
        || {
            Err(SessionTypeError::new(
                if exists {
                    "session_type_not_eligible"
                } else {
                    "unknown_session_type"
                },
                if exists {
                    "session type is not eligible for the requested target"
                } else {
                    "session type was not found"
                },
            ))
        },
        |row| Ok(Some(row)),
    )
}

pub(super) fn list_all(
    records: &[&PackageRecord],
    state: &HubState,
    limit: usize,
) -> SessionTypeResult<Option<(Vec<HubSessionType>, usize)>> {
    let Some(mut budget) = Budget::with_error_reserve(limit) else {
        return Ok(None);
    };
    let Some(repo_files) = collect_repo_files(state, &mut budget)? else {
        return Ok(None);
    };
    let Some(sources) = collect_sources(records.iter().copied(), state, &repo_files, &mut budget)?
    else {
        return Ok(None);
    };
    let mut row_count = 0usize;
    let mut row_bytes = 0usize;
    let mut start = 0;
    while start < sources.len() {
        let mut end = start + 1;
        while end < sources.len() && sources[end].definition.id() == sources[start].definition.id()
        {
            end += 1;
        }
        let selected = winner(&sources[start..end])?;
        let target_len = selected.definition.target_id().as_ref().map_or_else(
            || match selected.rank {
                SessionTypeSourceRank::Package => "package:".len() + selected.source_name.len(),
                SessionTypeSourceRank::Device => DEFAULT_DEVICE_TARGET_ID.len(),
                SessionTypeSourceRank::Repo => selected.source_name.len(),
            },
            String::len,
        );
        row_count = row_count.checked_add(1).ok_or_else(|| {
            SessionTypeError::new("callback_memory_limit", "catalog size overflow")
        })?;
        row_bytes = row_bytes
            .checked_add(
                row_owned_bytes(selected, &sources[start..end], target_len).ok_or_else(|| {
                    SessionTypeError::new("callback_memory_limit", "catalog size overflow")
                })?,
            )
            // `build_row` clones the target while this temporary target String is
            // still live. Account for that overlap for explicit and derived
            // targets from every source rank.
            .and_then(|value| value.checked_add(target_len))
            .ok_or_else(|| {
                SessionTypeError::new("callback_memory_limit", "catalog size overflow")
            })?;
        start = end;
    }
    let Some(total_rows) =
        row_bytes.checked_add(row_count.saturating_mul(size_of::<HubSessionType>()))
    else {
        return Ok(None);
    };
    let Some(with_overlap) = total_rows.checked_mul(2) else {
        return Ok(None);
    };
    if !budget.retain(with_overlap) {
        return Ok(None);
    }
    let mut rows = Vec::with_capacity(row_count);
    let mut start = 0;
    while start < sources.len() {
        let mut end = start + 1;
        while end < sources.len() && sources[end].definition.id() == sources[start].definition.id()
        {
            end += 1;
        }
        let selected = winner(&sources[start..end])?;
        let target =
            selected
                .definition
                .target_id()
                .clone()
                .unwrap_or_else(|| match selected.rank {
                    SessionTypeSourceRank::Package => {
                        let mut value =
                            String::with_capacity("package:".len() + selected.source_name.len());
                        value.push_str("package:");
                        value.push_str(selected.source_name);
                        value
                    }
                    SessionTypeSourceRank::Device => DEFAULT_DEVICE_TARGET_ID.to_string(),
                    SessionTypeSourceRank::Repo => selected.source_name.to_string(),
                });
        rows.push(build_row(selected, &sources[start..end], &target));
        start = end;
    }
    budget.release(total_rows);
    rows.sort_unstable_by(|left, right| left.session_type_id.cmp(&right.session_type_id));
    Ok(Some((rows, budget.used)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temporary_root(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("botster-bounded-catalog-{label}-{unique}"))
    }

    fn admission(bytes: &[u8]) -> Result<Admission, serde_json::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let admitted = AdmissionSeed.deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(admitted)
    }

    #[test]
    fn error_reserve_is_admitted_before_error_construction() {
        let reserve = error_reserve_bytes();
        assert!(Budget::with_error_reserve(reserve - 1).is_none());
        assert_eq!(Budget::with_error_reserve(reserve).unwrap().used, reserve);
        for bytes in [b"0".as_slice(), b"[]", b"{", b"null"] {
            let error = admission(bytes).expect_err("invalid file shape");
            let error = repo_parse_error(error);
            assert_eq!(error.kind, "invalid_repo_session_types");
            assert!(error.message.len() <= parse_error_text_bytes());
            assert_eq!(error.message.capacity(), parse_error_text_bytes());
        }
    }

    #[test]
    fn parser_error_conversion_cannot_grow_its_buffer() {
        let oversized =
            <serde_json::Error as de::Error>::custom("x".repeat(parse_error_text_bytes() + 1));
        let error = repo_parse_error(oversized);
        assert_eq!(error.message, "repo-local session type file is invalid");
        assert_eq!(error.message.capacity(), parse_error_text_bytes());
    }

    #[test]
    fn pinned_json_error_layout_fits_the_reserved_envelope() {
        // Mirror the payload shapes and variant count in serde_json 1.0.150.
        // This test checks the pinned compiler, not a public Serde layout API.
        #[allow(dead_code)]
        enum ErrorCode {
            Message(Box<str>),
            Io(std::io::Error),
            EofWhileParsingList,
            EofWhileParsingObject,
            EofWhileParsingString,
            EofWhileParsingValue,
            ExpectedColon,
            ExpectedListCommaOrEnd,
            ExpectedObjectCommaOrEnd,
            ExpectedSomeIdent,
            ExpectedSomeValue,
            ExpectedDoubleQuote,
            InvalidEscape,
            InvalidNumber,
            NumberOutOfRange,
            InvalidUnicodeCodePoint,
            ControlCharacterWhileParsingString,
            KeyMustBeAString,
            ExpectedNumericKey,
            FloatKeyMustBeFinite,
            LoneLeadingSurrogateInHexEscape,
            TrailingComma,
            TrailingCharacters,
            UnexpectedEndOfHexEscape,
            RecursionLimitExceeded,
        }
        #[allow(dead_code)]
        struct ErrorImpl {
            code: ErrorCode,
            line: usize,
            column: usize,
        }
        assert!(size_of::<ErrorImpl>() <= json_error_impl_bytes());
    }

    #[test]
    fn long_file_paths_reserve_the_unix_c_string_copy() {
        let temporary = temporary_root("long-path");
        let root = temporary.join("a".repeat(200)).join("b".repeat(200));
        fs::create_dir_all(&root).unwrap();
        let path_capacity =
            root.as_os_str().as_encoded_bytes().len() + 1 + REPO_SESSION_TYPES_FILE.len();
        // Rust 1.97 uses CString above its 384-byte stack threshold.
        assert!(path_capacity > 384);
        let peak = error_reserve_bytes() + 2 * path_capacity + 1;
        let mut short = Budget::with_error_reserve(peak - 1).unwrap();
        assert!(read_repo_catalog(&root, &mut short).unwrap().is_none());
        let mut exact = Budget::with_error_reserve(peak).unwrap();
        let rows = read_repo_catalog(&root, &mut exact).unwrap().unwrap();
        assert!(rows.is_empty());
        assert_eq!(exact.used, error_reserve_bytes());
        fs::remove_dir_all(temporary).unwrap();
    }

    fn valid_document(environment_value: &str, unknown_value: &str) -> Vec<u8> {
        format!(
            r#"{{"session_types":[{{"id":"agent","label":"Ag\u0065nt","role":"botster.agent","interaction":"interactive","lifecycle":"task","command":"bin/agent","execution":{{"mode":"relative_executable","future":"{unknown_value}"}},"working_directory":{{"policy":"package_root","future":{{"nested":["{unknown_value}"]}}}},"environment":{{"GOOD":"{environment_value}"}}}}]}}"#
        ).into_bytes()
    }

    #[test]
    fn schema_wrong_string_errors_do_not_retain_attacker_text() {
        let large = "x".repeat(64 * 1024);
        let cases = [
            format!(r#""{large}""#),
            format!(r#"{{"session_types":"{large}"}}"#),
            format!(r#"{{"session_types":["{large}"]}}"#),
            format!(
                r#"{{"session_types":[{{"id":"agent","label":"Agent","role":"botster.agent","interaction":"interactive","lifecycle":"task","command":"bin/agent","args":"{large}"}}]}}"#
            ),
            r#"{"session_types":[{"id":1.23456789012345678901234567890123456789,"label":"Agent","role":"botster.agent","interaction":"interactive","lifecycle":"task","command":"bin/agent"}]}"#.to_string(),
            r#"{"session_types":[{"id":"agent","label":"Agent","role":"botster.agent","interaction":"interactive","lifecycle":"task","command":9.99999999999999999999999999999999999999}]}"#.to_string(),
        ];
        for bytes in cases {
            let error = admission(bytes.as_bytes()).expect_err("wrong schema shape");
            assert!(
                error.to_string().len() < 256,
                "error retained attacker text"
            );
        }
    }

    #[test]
    fn escaped_unknown_enum_fields_and_environment_values_are_discarded() {
        let escaped = "\\u0061".repeat(4096);
        let bytes = valid_document(&escaped, &escaped);
        let root = temporary_root("escaped");
        fs::create_dir_all(root.join(".botster")).unwrap();
        fs::write(root.join(REPO_SESSION_TYPES_FILE), &bytes).unwrap();
        let admitted = admission(&bytes).unwrap();
        let retained = admitted.owned_layout_bytes().unwrap();
        let mut budget = Budget::with_error_reserve(usize::MAX).unwrap();
        let definitions = read_repo_catalog(&root, &mut budget).unwrap().unwrap();
        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0].label, "Agent");
        assert_eq!(repo_catalog_owned_layout(&definitions).unwrap(), retained);
        assert_eq!(
            budget.used,
            error_reserve_bytes() + retained,
            "input and scratch must be released"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repo_parse_admits_exact_peak_and_rejects_one_byte_less() {
        let bytes = valid_document("value", "ignored");
        let root = temporary_root("boundary");
        fs::create_dir_all(root.join(".botster")).unwrap();
        fs::write(root.join(REPO_SESSION_TYPES_FILE), &bytes).unwrap();
        let retained = admission(&bytes).unwrap().owned_layout_bytes().unwrap();
        let peak = error_reserve_bytes() + bytes.len() + 3 * bytes.len().max(8) + 2 * retained;
        let mut exact = Budget::with_error_reserve(peak).unwrap();
        assert!(read_repo_catalog(&root, &mut exact).unwrap().is_some());
        let mut short = Budget::with_error_reserve(peak - 1).unwrap();
        assert!(read_repo_catalog(&root, &mut short).unwrap().is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn discarded_environment_size_does_not_survive_repo_parse() {
        let small = valid_document("x", "ignored");
        let large = valid_document(&"y".repeat(32 * 1024), "ignored");
        let root = temporary_root("retention");
        fs::create_dir_all(root.join(".botster")).unwrap();
        let path = root.join(REPO_SESSION_TYPES_FILE);
        fs::write(&path, &small).unwrap();
        let mut small_budget = Budget::with_error_reserve(usize::MAX).unwrap();
        read_repo_catalog(&root, &mut small_budget)
            .unwrap()
            .unwrap();
        fs::write(&path, &large).unwrap();
        let mut large_budget = Budget::with_error_reserve(usize::MAX).unwrap();
        read_repo_catalog(&root, &mut large_budget)
            .unwrap()
            .unwrap();
        assert_eq!(small_budget.used, large_budget.used);
        fs::remove_dir_all(root).unwrap();
    }
}
