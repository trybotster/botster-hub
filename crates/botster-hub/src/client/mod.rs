//! The client protocol server over `local-stream` (design `design-hub-client-protocol.md` revision 8, sha256
//! `bed83e8f…`).
//!
//! These modules need neither Core nor the bridge:
//! - [`peer`]: the peer uid of an accepted connection (TB-L3);
//! - [`classify`]: the first frame of a connection, `hello` or `route_connect` (HS-1, TB-L2);
//! - [`route`]: the owner thread's reservation and route tables, the token check and the route-connect closes (design
//!   sections 2.1 to 2.5).
//!
//! The `io` loop and the Core calls use them after P5's daemon skeleton lands.

pub mod classify;
pub mod peer;
pub mod route;
