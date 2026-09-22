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
        message
            .max(8)
            .checked_mul(4)?
            .checked_add(message)?
            .checked_add(super::bounded_catalog::json_error_impl_bytes().checked_mul(2)?)?
            // Cover both the existing format! caller and the charged wrapper.
            // The charged wrapper reserves exactly and uses less than this term.
            .checked_add(final_message.max(8).checked_mul(4)?)
    }
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
