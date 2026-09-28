//! MCP over HTTP on loopback: the one agent-facing MCP transport.
//!
//! Sessions reach the daemon here with `Authorization: Bearer <token>`, the
//! token the Hub issued at spawn. There is no operator over HTTP; the Unix
//! socket stays the operator's path.

pub(crate) mod connection;
pub(crate) mod listener;
pub(crate) mod tools;
pub(crate) mod wire;
