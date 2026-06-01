//! Host adapter contract namespace.
//!
//! Adapter modules live in this crate to define the contracts the hub governs.
//! They are not concrete cloud, signaling, API, WebRTC, browser, TUI, or socket
//! implementations.

pub mod api;
pub mod clients;
pub mod cloud;
pub mod signaling;

/// Adapter families governed by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterFamily {
    /// Browser, TUI, socket, or custom client transports.
    Clients,
    /// Cloud federation contracts implemented by providers.
    Cloud,
    /// Signaling relay contracts implemented by providers.
    Signaling,
    /// External API contracts implemented by providers.
    Api,
}
