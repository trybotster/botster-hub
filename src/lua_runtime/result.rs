//! The one result shape every `botster.*` helper returns.
//!
//! `{ ok = true, value = ... }` or
//! `{ ok = false, error = { kind = ..., message = ..., retryable = ... } }`.
//! See docs/plans/plugin-platform.md section 6.

use mlua::{Lua, Table, Value};

/// The closed set of error kinds shared by every helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    InvalidRequest,
    CapabilityDenied,
    NotFound,
    Conflict,
    QuotaExceeded,
    Backpressured,
    TimedOut,
    Cancelled,
    Unavailable,
    Failed,
}

impl ErrorKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::CapabilityDenied => "capability_denied",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::QuotaExceeded => "quota_exceeded",
            Self::Backpressured => "backpressured",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Unavailable => "unavailable",
            Self::Failed => "failed",
        }
    }

    pub(crate) const fn retryable(self) -> bool {
        matches!(
            self,
            Self::Backpressured | Self::TimedOut | Self::Unavailable
        )
    }
}

/// `{ ok = true, value = value }`.
pub(crate) fn ok(lua: &Lua, value: Value) -> mlua::Result<Table> {
    let result = lua.create_table()?;
    result.raw_set("ok", true)?;
    result.raw_set("value", value)?;
    Ok(result)
}

/// `{ ok = false, error = { kind, message, retryable } }`.
pub(crate) fn err(lua: &Lua, kind: ErrorKind, message: &str) -> mlua::Result<Table> {
    let error = lua.create_table()?;
    error.raw_set("kind", kind.as_str())?;
    error.raw_set("message", message)?;
    error.raw_set("retryable", kind.retryable())?;
    let result = lua.create_table()?;
    result.raw_set("ok", false)?;
    result.raw_set("error", error)?;
    Ok(result)
}
