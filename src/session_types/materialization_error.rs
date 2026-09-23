//! Count and fund the Hub error text while the Serde error remains live.

use std::cell::Cell;
use std::fmt::{self, Write};

use super::SessionTypeError;
use crate::lua_memory::LuaCallbackCharge;

/// Candidate text survives a later counting failure without owning heap data.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct TypedErrorCandidates {
    pub(super) tag_bytes: usize,
    pub(super) wrong_string_debug: usize,
}

#[derive(Clone, Copy, Default)]
pub(super) struct ErrorTrack<'a>(Option<&'a Cell<TypedErrorCandidates>>);

impl<'a> ErrorTrack<'a> {
    pub(super) fn new(state: &'a Cell<TypedErrorCandidates>) -> Self {
        Self(Some(state))
    }

    pub(super) fn tag(self, bytes: usize) {
        if let Some(state) = self.0 {
            let mut next = state.get();
            next.tag_bytes = next.tag_bytes.max(bytes);
            state.set(next);
        }
    }

    pub(super) fn wrong_string(self, debug_bytes: usize) {
        if let Some(state) = self.0 {
            let mut next = state.get();
            next.wrong_string_debug = next.wrong_string_debug.max(debug_bytes);
            state.set(next);
        }
    }
}

impl TypedErrorCandidates {
    /// Bound simultaneous typed error storage, not counting-decoder errors.
    pub(super) fn storage_bytes(self, input_bytes: usize) -> Option<usize> {
        let expected = [
            "a string",
            "a sequence",
            "a map",
            "a string or map",
            "struct RepoSessionTypesFile with 1 element",
            "struct PackageSessionType with 16 elements",
            "internally tagged enum PackageSessionTypeExecution",
            "internally tagged enum PackageSessionTypeWorkingDirectory",
            "struct variant PackageSessionTypeWorkingDirectory::Relative with 1 element",
            "unit variant PackageSessionTypeExecution::RelativeExecutable",
            "unit variant PackageSessionTypeExecution::ShellCommand",
            "unit variant PackageSessionTypeWorkingDirectory::PackageRoot",
        ]
        .iter()
        .map(|text| text.len())
        .max()?;
        // Pinned serde_json formats floats through zmij's 24-byte buffer.
        // Integer and boolean Unexpected text is shorter than this term.
        let unexpected = ("floating point ``".len() + 24)
            .max("string ".len().checked_add(self.wrong_string_debug)?);
        let wrong_shape = "invalid value: , expected "
            .len()
            .checked_add(unexpected)?
            .checked_add(expected)?;
        let tag = "unknown variant ``, expected one of `relative_executable`, `shell_command`"
            .len()
            .checked_add(self.tag_bytes)?;
        let field = "duplicate field ``"
            .len()
            .max("missing field ``".len())
            .checked_add("allowed_environment_overrides".len())?;
        let length = "invalid length , expected "
            .len()
            .checked_add(decimal_digits(usize::MAX))?
            .checked_add(expected)?;
        let message = wrong_shape
            .max(tag)
            .max(field)
            .max(length)
            .max("control character (\\u0000-\\u001F) found while parsing a string".len());
        let position_digits = decimal_digits(input_bytes.checked_add(1)?);
        let final_message = "repo-local session type file is invalid: "
            .len()
            .checked_add(message)?
            .checked_add(" at line  column ".len())?
            .checked_add(position_digits.checked_mul(2)?)?;
        // Formatting growth overlaps old and new buffers. Shrinking to Box<str>
        // can overlap its final copy. Position correction overlaps two boxes.
        formatting_buffer_bytes(message)?
            .checked_add(message)?
            .checked_add(super::bounded_catalog::json_error_impl_bytes().checked_mul(2)?)?
            // Cover both the existing format! caller and the charged wrapper.
            // The charged wrapper reserves exactly and uses less than this term.
            .checked_add(formatting_buffer_bytes(final_message)?)
    }
}

/// Bound old and new formatting buffers under the pinned RawVec growth rule.
/// This excludes the formatted arguments and any surrounding error objects.
fn formatting_buffer_bytes(message_bytes: usize) -> Option<usize> {
    message_bytes.max(8).checked_mul(4)
}

// These are ordinary source lookup and materialization errors, not I/O errors.
const FIXED_CONSTRUCTION_ERRORS: &[(&str, &str)] = &[
    ("target_not_admitted", "spawn target is not enabled"),
    ("target_not_found", "spawn target was not found"),
    (
        "target_not_admitted",
        "requested spawn target is not admitted for this session_type",
    ),
    ("unknown_session_type", "session type was not found"),
    (
        "ambiguous_session_type",
        "session type id matches more than one source at the same precedence",
    ),
    (
        "session_type_unavailable",
        "session type package has no local package root",
    ),
    (
        "session_type_unavailable",
        "session type source is not enabled",
    ),
    (
        "cwd_not_admitted",
        "requested cwd is outside the admitted spawn target",
    ),
    (
        "cwd_not_admitted",
        "session_type working directory escapes the admitted spawn target",
    ),
];

// Validation can move its message into either source-family error unchanged.
const VALIDATION_ERROR_KINDS: &[&str] = &[
    "invalid_session_type",
    "invalid_session_type_role",
    "invalid_session_type_semantics",
    "invalid_session_type_traits",
    "invalid_session_type_command",
    "invalid_session_type_path",
    "invalid_environment",
    "invalid_device_session_types",
    "invalid_repo_session_types",
];

/// Include message construction and the runtime's `kind: message` wrapper.
/// The original message remains live while the wrapper grows.
fn construction_candidate_bytes(
    kind_bytes: usize,
    message_bytes: usize,
    formatted: bool,
) -> Option<usize> {
    let (construction, retained) = if formatted {
        (
            formatting_buffer_bytes(message_bytes)?,
            message_bytes.max(8).checked_mul(2)?,
        )
    } else {
        (message_bytes, message_bytes)
    };
    let wrapper = kind_bytes.checked_add(2)?.checked_add(message_bytes)?;
    let wrapped = retained.checked_add(formatting_buffer_bytes(wrapper)?)?;
    Some(construction.max(wrapped))
}

pub(super) fn formatted_error_storage_bytes(
    kind_bytes: usize,
    message_bytes: usize,
) -> Option<usize> {
    construction_candidate_bytes(kind_bytes, message_bytes, true)
}

/// Bound ordinary semantic errors using borrowed input lengths only.
/// This excludes I/O, parser, managed-spawn, Core, and allocation-refusal errors.
/// Successful products and other live temporary storage need separate charges.
#[allow(dead_code)] // A reviewed construction caller must supply its live terms.
pub(super) fn construction_error_storage_bytes(
    longest_override_name: Option<usize>,
) -> Option<usize> {
    let validation_kind = VALIDATION_ERROR_KINDS.iter().map(|kind| kind.len()).max()?;
    // This existing maximum covers all ordinary validation messages, including
    // duplicate IDs and both fixed labels used by relative-path validation.
    // Treat the family as formatted to include the two path-error constructors.
    let mut maximum = construction_candidate_bytes(
        validation_kind,
        super::bounded_catalog::validation_error_text_bytes(),
        true,
    )?;
    for (kind, message) in FIXED_CONSTRUCTION_ERRORS {
        maximum = maximum.max(construction_candidate_bytes(
            kind.len(),
            message.len(),
            false,
        )?);
    }
    if let Some(name_bytes) = longest_override_name {
        let message_bytes = "environment override is not admitted: "
            .len()
            .checked_add(name_bytes)?;
        maximum = maximum.max(construction_candidate_bytes(
            "environment_not_admitted".len(),
            message_bytes,
            true,
        )?);
    }
    Some(maximum)
}

fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

pub(super) struct ChargedMaterializationError {
    error: SessionTypeError,
    // The owned message must be destroyed before its charge.
    _storage: LuaCallbackCharge,
}

impl ChargedMaterializationError {
    pub(super) fn error(&self) -> &SessionTypeError {
        &self.error
    }

    pub(super) fn into_parts(self) -> (SessionTypeError, LuaCallbackCharge) {
        (self.error, self._storage)
    }
}

struct ByteCount(usize);

impl Write for ByteCount {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}

/// The caller retains the separate Serde allocation charge during this call.
/// The supplied storage is part of the admitted materialization permit.
/// Refusal preserves that storage and leaves the borrowed Serde error intact.
pub(super) fn wrap_repo_error(
    error: &serde_json::Error,
    storage: &mut LuaCallbackCharge,
) -> Option<ChargedMaterializationError> {
    let mut count = ByteCount(0);
    write!(count, "repo-local session type file is invalid: {error}").ok()?;
    let charge = storage.split(count.0)?;
    let mut message = String::with_capacity(count.0);
    write!(message, "repo-local session type file is invalid: {error}")
        .expect("writing the immutable Serde error into a String cannot fail");
    debug_assert_eq!(message.len(), count.0);
    Some(ChargedMaterializationError {
        error: SessionTypeError::new("invalid_repo_session_types", message),
        _storage: charge,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};

    #[test]
    fn parser_bounds_match_the_previous_arithmetic() {
        let boxes = 2 * super::super::bounded_catalog::json_error_impl_bytes();
        for (tag_bytes, wrong_string_debug, input_bytes, buffers) in [
            (0, 0, 0, 1509),
            (4096, 0, 10000, 37802),
            (0, 4096, 10000, 38099),
            (23, 17, 99, 1525),
        ] {
            let candidates = TypedErrorCandidates {
                tag_bytes,
                wrong_string_debug,
            };
            assert_eq!(candidates.storage_bytes(input_bytes), Some(buffers + boxes));
        }
        assert_eq!(
            TypedErrorCandidates::default().storage_bytes(usize::MAX),
            None
        );
    }

    #[test]
    fn construction_sizing_refuses_each_arithmetic_overflow() {
        assert_eq!(
            formatting_buffer_bytes(usize::MAX / 4),
            Some((usize::MAX / 4) * 4)
        );
        assert_eq!(formatting_buffer_bytes(usize::MAX / 4 + 1), None);
        assert_eq!(construction_candidate_bytes(usize::MAX, 1, false), None);
        assert_eq!(construction_candidate_bytes(0, usize::MAX, false), None);
        assert_eq!(construction_candidate_bytes(0, usize::MAX, true), None);
        // Both buffer terms fit individually, but their overlap does not.
        assert_eq!(construction_candidate_bytes(0, usize::MAX / 5, true), None);
        assert_eq!(construction_error_storage_bytes(Some(usize::MAX)), None);
        assert!(construction_error_storage_bytes(None).is_some());
    }

    #[test]
    fn runtime_wrapper_keeps_the_original_message_live() {
        let kind = "environment_not_admitted";
        let name = "x".repeat(1024);
        let message = format!("environment override is not admitted: {name}");
        let error = SessionTypeError::new(kind, message);
        let wrapper = format!("{}: {}", error.kind, error.message);
        let retained_bound = 2 * error.message.len().max(8);
        let wrapper_peak = formatting_buffer_bytes(wrapper.len()).unwrap();
        let bytes = construction_candidate_bytes(kind.len(), error.message.len(), true).unwrap();
        assert_eq!(bytes, retained_bound + wrapper_peak);
        assert!(bytes > wrapper_peak);
        assert!(bytes >= error.message.capacity() + wrapper.capacity());
        // The bound contains no Serde error box or position-correction term.
        assert_eq!(
            construction_candidate_bytes(3, 10, false),
            Some(10 + 4 * 15)
        );
    }

    #[test]
    fn construction_candidates_take_the_maximum_not_the_sum() {
        let fixed = construction_error_storage_bytes(None).unwrap();
        let name_bytes = 4096;
        let dynamic = construction_candidate_bytes(
            "environment_not_admitted".len(),
            "environment override is not admitted: ".len() + name_bytes,
            true,
        )
        .unwrap();
        assert!(dynamic > fixed);
        assert_eq!(
            construction_error_storage_bytes(Some(name_bytes)),
            Some(dynamic)
        );
        assert_ne!(
            construction_error_storage_bytes(Some(name_bytes)),
            Some(fixed + dynamic)
        );
        assert_eq!(construction_error_storage_bytes(Some(0)), Some(fixed));
    }

    fn valid_definition() -> super::super::PackageSessionType {
        serde_json::from_value(serde_json::json!({
            "id": "worker", "label": "Worker", "role": "agent.worker",
            "interaction": "terminal", "lifecycle": "persistent",
            "command": "worker", "execution": {"mode": "shell_command"}
        }))
        .unwrap()
    }

    #[test]
    fn existing_validation_maximum_covers_the_actual_construction_family() {
        use super::super::{validate_session_type, validate_session_types};
        let original = valid_definition();
        assert!(validate_session_type(&original).is_ok());
        let changes: &[fn(&mut super::super::PackageSessionType)] = &[
            |value| value.id.clear(),
            |value| value.label.clear(),
            |value| value.description = Some("x".repeat(1025)),
            |value| value.icon = Some("x".repeat(257)),
            |value| value.role = "unnamespaced".into(),
            |value| value.interaction.clear(),
            |value| value.lifecycle.clear(),
            |value| value.traits = vec!["duplicate".into(), "duplicate".into()],
            |value| value.traits = vec!["bad trait".into()],
            |value| value.traits = (0..33).map(|index| format!("trait{index}")).collect(),
            |value| value.command.clear(),
            |value| {
                value.execution = super::super::PackageSessionTypeExecution::RelativeExecutable;
                value.command = "../escape".into();
            },
            |value| {
                value.working_directory =
                    super::super::PackageSessionTypeWorkingDirectory::Relative {
                        path: "../escape".into(),
                    }
            },
            |value| {
                value.environment.insert("bad name".into(), "value".into());
            },
            |value| value.allowed_environment_overrides = vec!["bad name".into()],
        ];
        let bound = super::super::bounded_catalog::validation_error_text_bytes();
        let total = construction_error_storage_bytes(None).unwrap();
        for change in changes {
            let mut value = original.clone();
            change(&mut value);
            let error = validate_session_type(&value).unwrap_err();
            assert!(
                VALIDATION_ERROR_KINDS.contains(&error.kind),
                "{}",
                error.kind
            );
            assert!(error.message.len() <= bound, "{}", error.message);
            let actual =
                construction_candidate_bytes(error.kind.len(), error.message.len(), true).unwrap();
            assert!(actual <= total);
            for kind in ["invalid_device_session_types", "invalid_repo_session_types"] {
                assert!(
                    construction_candidate_bytes(kind.len(), error.message.len(), true).unwrap()
                        <= total
                );
            }
        }
        let duplicate = validate_session_types(&[original.clone(), original]).unwrap_err();
        assert_eq!(duplicate, "duplicate session type id");
        assert!(duplicate.len() <= bound);
    }

    #[test]
    fn fixed_lookup_and_materialization_messages_fit_the_family_bound() {
        let total = construction_error_storage_bytes(None).unwrap();
        assert_eq!(FIXED_CONSTRUCTION_ERRORS.len(), 9);
        for (kind, message) in FIXED_CONSTRUCTION_ERRORS {
            let error = SessionTypeError::new(kind, *message);
            let wrapper = format!("{}: {}", error.kind, error.message);
            let bound = construction_candidate_bytes(kind.len(), message.len(), false).unwrap();
            assert!(bound <= total);
            assert!(bound >= error.message.capacity() + wrapper.capacity());
        }
    }

    #[test]
    fn refusal_preserves_storage_and_exact_admission_preserves_error_text() {
        let error = serde_json::from_str::<serde_json::Value>("[\n").unwrap_err();
        let expected = format!("repo-local session type file is invalid: {error}");
        let bytes = expected.len();
        let account = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: bytes,
            total_callback_bytes: bytes,
        })
        .unwrap();
        let mut insufficient = account.reserve_callback_total(bytes - 1).unwrap();
        assert!(wrap_repo_error(&error, &mut insufficient).is_none());
        assert_eq!(insufficient.bytes(), bytes - 1);
        drop(insufficient);
        let mut exact = account.reserve_callback_total(bytes).unwrap();
        let wrapped = wrap_repo_error(&error, &mut exact).unwrap();
        drop(error);
        assert_eq!(exact.bytes(), 0);
        assert_eq!(wrapped.error().kind, "invalid_repo_session_types");
        assert_eq!(wrapped.error().message, expected);
        assert!(account.reserve_callback_total(1).is_err());
        drop(wrapped);
        assert!(account.reserve_callback_total(bytes).is_ok());
    }
}
