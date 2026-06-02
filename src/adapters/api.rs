//! External API provider contract seam.
//!
//! The host profile owns admission, audit, timeout, and capability policy for
//! external APIs. Concrete API clients live in provider packages outside this
//! crate.

/// External API contract categories governed by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiContract {
    /// Provider performs requests under hub timeout and audit policy.
    RequestPolicy,
    /// Provider declares required secrets and scopes before enablement.
    SecretScopeDeclaration,
}
