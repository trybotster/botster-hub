//! Privileged provider capability contracts.
//!
//! The hub owns the capability vocabulary, admission policy, lifecycle ordering,
//! timeout/failure policy, and audit hooks. Provider packages implement cloud,
//! signaling, browser shell, API, and other privileged behavior outside this
//! crate.

/// Capabilities a provider package may request from the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderCapability {
    /// Admit or reject clients before they attach.
    ClientAdmission,
    /// Create pairing invitations or similar admission handles.
    PairingInvites,
    /// Relay encrypted signaling messages.
    SignalingRelay,
    /// Publish hub presence to an external provider.
    HubPresence,
    /// Provide a browser shell or hosted UI surface.
    BrowserShell,
    /// Read or write provider-scoped secrets through hub policy.
    Secrets,
    /// Use reusable crypto envelope operations under hub policy.
    CryptoEnvelope,
    /// Integrate with an external API.
    ExternalApi,
}
