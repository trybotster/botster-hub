//! Profile-owned authentication hook seam.
//!
//! The first-party host profile owns admission and auth policy hooks. Concrete
//! OAuth, device-code, cloud, or provider-specific flows are intentionally out
//! of scope.

/// Auth hook points exposed by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthHook {
    /// A client asks to attach to a hub.
    ClientAdmission,
    /// A provider asks to enable privileged capabilities.
    ProviderEnablement,
    /// A package asks to use a stored or delegated secret.
    SecretAccess,
}
