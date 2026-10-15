//! The typed start failure of `botster-hub up` (HC HS-0, steward R-118).

use std::fmt;

/// The `code` of a `failed <code> <message>` ready line (HC HS-0; the codes of steward R-118).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailCode {
    LockHeld,
    ConfigInvalid,
    ConfigUnknownKey,
    ConfigUnsafe,
    SocketPathTooLong,
    SocketUnavailable,
    AdoptionFailed,
    DataRootUnusable,
    /// Core lacks a feature that the Hub requires (R-118 makes the set of codes open); the message names it.
    CoreFeatureMissing,
    Internal,
}

impl FailCode {
    /// The code as the ready line writes it.
    pub fn as_str(self) -> &'static str {
        match self {
            FailCode::LockHeld => "lock_held",
            FailCode::ConfigInvalid => "config_invalid",
            FailCode::ConfigUnknownKey => "config_unknown_key",
            FailCode::ConfigUnsafe => "config_unsafe",
            FailCode::SocketPathTooLong => "socket_path_too_long",
            FailCode::SocketUnavailable => "socket_unavailable",
            FailCode::AdoptionFailed => "adoption_failed",
            FailCode::DataRootUnusable => "data_root_unusable",
            FailCode::CoreFeatureMissing => "core_feature_missing",
            FailCode::Internal => "internal",
        }
    }
}

/// A start failure: the code and a one-line message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartFailure {
    pub code: FailCode,
    pub message: String,
}

impl StartFailure {
    pub fn new(code: FailCode, message: impl Into<String>) -> StartFailure {
        StartFailure {
            code,
            message: message.into(),
        }
    }

    /// The ready line, with no line feed inside it: a message line feed becomes a space (HC HS-0 writes one line).
    pub fn ready_line(&self) -> String {
        format!(
            "failed {} {}\n",
            self.code.as_str(),
            self.message.replace('\n', " ")
        )
    }
}

impl fmt::Display for StartFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.code.as_str(), self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// HC HS-0: one line, `failed <code> <message>`.
    #[test]
    fn hs_0_failed_line_is_one_line() {
        let failure = StartFailure::new(FailCode::LockHeld, "held by\nbotster-hub");
        assert_eq!(
            failure.ready_line(),
            "failed lock_held held by botster-hub\n"
        );
        assert_eq!(failure.to_string(), "lock_held held by\nbotster-hub");
    }

    /// Every code of R-118, spelled as the contract spells it.
    #[test]
    fn codes() {
        let all = [
            (FailCode::LockHeld, "lock_held"),
            (FailCode::ConfigInvalid, "config_invalid"),
            (FailCode::ConfigUnknownKey, "config_unknown_key"),
            (FailCode::ConfigUnsafe, "config_unsafe"),
            (FailCode::SocketPathTooLong, "socket_path_too_long"),
            (FailCode::SocketUnavailable, "socket_unavailable"),
            (FailCode::AdoptionFailed, "adoption_failed"),
            (FailCode::DataRootUnusable, "data_root_unusable"),
            (FailCode::CoreFeatureMissing, "core_feature_missing"),
            (FailCode::Internal, "internal"),
        ];
        for (code, text) in all {
            assert_eq!(code.as_str(), text);
        }
    }
}
