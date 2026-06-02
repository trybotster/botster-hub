//! Client transport adapter seam.
//!
//! Browser, TUI, socket, and custom clients consume profile-owned admission
//! contracts. This module classifies clients at the host-profile boundary; it
//! does not replace core `TransportIngress`, `TransportEgress`, `SessionIo`, or
//! client-stream contracts.

/// Client admission categories recognized by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientTransport {
    /// Browser client admitted through browser-facing host policy.
    Browser,
    /// Terminal UI client admitted through local operator policy.
    Tui,
    /// Local socket client admitted through local IPC policy.
    Socket,
    /// Custom client admitted through the same hub policy vocabulary.
    Custom,
}
