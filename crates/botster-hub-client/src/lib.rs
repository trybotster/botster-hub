//! The connection code that the TUI and the CLI share (lead ruling Q2; `design-hub-client-protocol.md` section 6).
//!
//! - [`discovery`]: the one Hub socket (HC TB-L1, CLI-RT3).
//! - [`owner`]: the socket and its directory belong to this user (HC RT-5).
//! - [`framing`]: the control framing, `[u32 big-endian length][UTF-8 JSON]` (HC TR-1).
//! - [`control`]: `hello`, then `welcome` or `close` (HC HS-1, HS-3, HS-9).
//! - [`route`]: route connect, a token handshake on the Hub's listener (HC TB-L2).
//! - [`reconnect`]: the UI's reconnect attempts after a Hub restart (CLI-AT4).
//!
//! The wire types are `botster-hub-contract`'s. This crate defines none, except the tagged first frame of a route
//! connection, which the contract crate has no type for ([`route::RouteConnectFrame`]).

pub mod control;
pub mod discovery;
pub mod framing;
pub mod owner;
pub mod reconnect;
pub mod route;
