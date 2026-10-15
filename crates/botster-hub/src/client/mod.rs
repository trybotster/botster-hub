//! The client protocol server over `local-stream` (design `design-hub-client-protocol.md` revision 9, sha256
//! `4ae72c8f…`).
//!
//! `peer`, `classify` and `route` need neither Core nor the bridge:
//! - [`peer`]: the peer uid of an accepted connection (TB-L3);
//! - [`classify`]: the first frame of a connection, `hello` or `route_connect` (HS-1, TB-L2);
//! - [`route`]: the owner thread's reservation and route tables, the token check and the route-connect closes (design
//!   sections 2.1 to 2.5);
//! - [`handoff`]: the owner's reserve and connect steps over Core, and the Core route events;
//! - [`kinds`]: the `session` and `session_type` read kinds of the control envelope.

pub mod classify;
pub mod formats;
pub mod handoff;
pub mod kinds;
pub mod peer;
pub mod route;
pub mod rows;
pub mod server;
pub mod wire;
