//! Isolated allocation fixtures for the complete repository definition parser.

use super::{
    PackageSessionType, REPO_SESSION_TYPES_FILE, REPO_SESSION_TYPES_FILE_BYTE_CAPACITY,
    RepoSessionTypesFile, SessionTypeError, SessionTypeResult, repo_session_types,
    validate_session_types,
};
use std::path::Path;

const COUNTEREXAMPLE: &str = r#"{"session_types":[{"description":"\naaaaaaaa","label":"aaaaaaaaaaaaa\u0080","id":"agent","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent"}]}"#;

#[derive(Clone, Copy, Debug)]
pub enum Case {
    Counterexample,
    MalformedEscape,
    TruncatedUnicode,
    DuplicateTaggedValues,
    UnknownVariant,
    DuplicateEnvironment,
    TaggedSequence,
    Whitespace,
}

impl Case {
    pub const ALL: [Self; 8] = [
        Self::Counterexample,
        Self::MalformedEscape,
        Self::TruncatedUnicode,
        Self::DuplicateTaggedValues,
        Self::UnknownVariant,
        Self::DuplicateEnvironment,
        Self::TaggedSequence,
        Self::Whitespace,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Counterexample => "counterexample",
            Self::MalformedEscape => "malformed-escape",
            Self::TruncatedUnicode => "truncated-unicode",
            Self::DuplicateTaggedValues => "duplicate-tagged-values",
            Self::UnknownVariant => "unknown-variant",
            Self::DuplicateEnvironment => "duplicate-environment",
            Self::TaggedSequence => "tagged-sequence",
            Self::Whitespace => "whitespace",
        }
    }

    fn input(self) -> Vec<u8> {
        let definition = |extra: &str| {
            format!(
                r#"{{"session_types":[{{"id":"agent","label":"Agent","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent",{extra}}}]}}"#
            )
            .into_bytes()
        };
        match self {
            Self::Counterexample => COUNTEREXAMPLE.as_bytes().to_vec(),
            Self::MalformedEscape => format!(
                r#"{{"session_types":[{{"description":"\naaaaaaaa","label":"{}\q","id":"agent","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent"}}]}}"#,
                "a".repeat(4096)
            )
            .into_bytes(),
            Self::TruncatedUnicode => format!(
                r#"{{"session_types":[{{"description":"\naaaaaaaa","label":"{}\u00"#,
                "a".repeat(4096)
            )
            .into_bytes(),
            Self::DuplicateTaggedValues => {
                let strings = (0..64)
                    .map(|index| format!(r#""value-{index}\n""#))
                    .collect::<Vec<_>>()
                    .join(",");
                definition(&format!(
                    r#""execution":{{"mode":"shell_command","x":[{strings}],"x":[{strings}]}},"working_directory":{{"policy":"relative","path":"sub","y":{{"values":[{strings}]}},"y":{{"values":[{strings}]}}}},"environment":{{"MODE":"test"}}"#
                ))
            }
            Self::UnknownVariant => definition(&format!(
                r#""execution":{{"mode":"{}"}}"#,
                "x".repeat(1024 * 1024)
            )),
            Self::DuplicateEnvironment => definition(
                r#""environment":{"SAME":"\naaaaaaaa","SAME":"aaaaaaaaaaaaa\u0080","KEEP":"present"}"#,
            ),
            Self::TaggedSequence => definition(
                r#""execution":["shell_command"],"working_directory":["relative","sub"],"environment":{"MODE":"test"}"#,
            ),
            Self::Whitespace => {
                let mut bytes = COUNTEREXAMPLE.as_bytes().to_vec();
                bytes.resize(REPO_SESSION_TYPES_FILE_BYTE_CAPACITY, b' ');
                bytes
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum ProbePhase {
    Parse,
    Parsed,
    ErrorFormat,
    ParserErrorDrop,
    Validate,
    ResultRetained,
    ResultDrop,
    End,
}

impl ProbePhase {
    pub const ALL: [Self; 8] = [
        Self::Parse,
        Self::Parsed,
        Self::ErrorFormat,
        Self::ParserErrorDrop,
        Self::Validate,
        Self::ResultRetained,
        Self::ResultDrop,
        Self::End,
    ];
}

pub struct PreparedProbe {
    input: Vec<u8>,
    expected: SessionTypeResult<Vec<PackageSessionType>>,
}

impl PreparedProbe {
    /// Create fixture files and run the production control before recording.
    pub fn prepare(case: Case, directory: &Path) -> Result<Self, String> {
        let input = case.input();
        let path = directory.join(REPO_SESSION_TYPES_FILE);
        std::fs::create_dir_all(path.parent().expect("fixture parent"))
            .map_err(|error| error.to_string())?;
        std::fs::write(&path, &input).map_err(|error| error.to_string())?;
        let expected = repo_session_types(directory).and_then(validate);
        if matches!(case, Case::Whitespace) {
            let unpadded =
                serde_json::from_slice::<RepoSessionTypesFile>(COUNTEREXAMPLE.as_bytes())
                    .map(|file| file.session_types)
                    .map_err(|error| {
                        SessionTypeError::new("invalid_repo_session_types", error.to_string())
                    })
                    .and_then(validate);
            if expected != unpadded {
                return Err("whitespace changed the production control result".into());
            }
        }
        Ok(Self { input, expected })
    }

    pub fn expected_json(&self) -> serde_json::Value {
        match &self.expected {
            Ok(definitions) => serde_json::json!({"definitions": definitions}),
            Err(error) => serde_json::json!({
                "error": {"kind": error.kind, "message": error.message}
            }),
        }
    }

    /// Compare all definition fields and exact error bytes without cloning them.
    pub fn run(&self, mark: fn(ProbePhase)) -> bool {
        mark(ProbePhase::Parse);
        let parsed = serde_json::from_slice::<RepoSessionTypesFile>(&self.input);
        mark(ProbePhase::Parsed);
        let decoded = match parsed {
            Ok(file) => Ok(file.session_types),
            Err(error) => {
                mark(ProbePhase::ErrorFormat);
                let message = format!("repo-local session type file is invalid: {error}");
                mark(ProbePhase::ParserErrorDrop);
                drop(error);
                Err(SessionTypeError::new("invalid_repo_session_types", message))
            }
        };
        mark(ProbePhase::Validate);
        let result = decoded.and_then(validate);
        mark(ProbePhase::ResultRetained);
        let equal = result == self.expected;
        mark(ProbePhase::ResultDrop);
        drop(result);
        mark(ProbePhase::End);
        equal
    }
}

fn validate(definitions: Vec<PackageSessionType>) -> SessionTypeResult<Vec<PackageSessionType>> {
    validate_session_types(&definitions)
        .map_err(|message| SessionTypeError::new("invalid_repo_session_types", message))?;
    Ok(definitions)
}

pub fn definition_layout() -> (usize, usize) {
    (
        std::mem::size_of::<PackageSessionType>(),
        std::mem::align_of::<PackageSessionType>(),
    )
}

pub fn environment_node_bound(entries: usize) -> Option<usize> {
    crate::lua_memory::layout::btree_nodes_checked::<String, String>(entries)
}
