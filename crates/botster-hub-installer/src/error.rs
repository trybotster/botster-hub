//! Installer failure vocabulary.

use botster_hub_installation::InstallationProblem;

/// A recoverable installer failure.
///
/// "Recoverable" is load-bearing here: every error in this type is *returned*,
/// so rollback code runs and the installation reaches a defined final state.
/// Abrupt termination — `SIGKILL`, power loss — is a different thing entirely
/// and is bounded by the on-disk ordering, not by this type.
#[derive(Debug)]
pub struct InstallerError {
    kind: &'static str,
    message: String,
}

impl InstallerError {
    pub fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> &'static str {
        self.kind
    }
}

impl std::fmt::Display for InstallerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.kind, self.message)
    }
}

impl From<InstallationProblem> for InstallerError {
    fn from(problem: InstallationProblem) -> Self {
        Self::new(problem.kind(), problem.message().to_string())
    }
}

pub type InstallerResult<T> = Result<T, InstallerError>;
