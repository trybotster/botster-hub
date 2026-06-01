//! Signaling relay contract seam.
//!
//! This module defines the hub-visible signaling contract only. Signaling relay,
//! ActionCable, WebRTC, and cloud transport implementations live in provider
//! packages outside the hub crate.

/// Signaling contract categories a provider may implement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalingContract {
    /// Relay opaque encrypted signaling envelopes.
    EncryptedRelay,
    /// Report relay readiness and failure state to hub policy.
    RelayHealth,
}
