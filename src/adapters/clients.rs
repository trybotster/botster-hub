//! Client transport adapter seam.
//!
//! Browser, TUI, socket, and custom clients consume profile-owned contracts.
//! This module names the transport categories without implementing a transport.

/// Client transport categories the hub can admit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientTransport {
    /// Browser client transport.
    Browser,
    /// Terminal UI client transport.
    Tui,
    /// Local socket client transport.
    Socket,
    /// Custom client transport using the same hub contracts.
    Custom,
}
