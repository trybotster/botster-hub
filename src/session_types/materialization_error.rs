//! Count and fund the Hub error text while the Serde error remains live.

use std::fmt::{self, Write};

use super::SessionTypeError;
use crate::lua_memory::LuaCallbackCharge;

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
