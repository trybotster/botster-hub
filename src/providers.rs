//! Privileged provider capability contracts.
//!
//! The first-party host profile governs provider capability policy, admission,
//! lifecycle ordering, timeout/failure policy, and audit hooks. Provider
//! packages implement cloud, signaling, browser shell, API, and other
//! privileged behavior outside this crate.

/// Capabilities a provider package may request from the host profile.
pub type ProviderCapability = botster_core::CapabilitySurface;
