//! The Botster Hub daemon (repo-plan section 3): the data root and the `flock` singleton, ONE owner of the embedded Core
//! behind a small bounded bridge, start and stop, and restore through the live admission path. No product logic.
//!
//! Design: `shared/stage2/design-hub-daemon.md`, sha256 0d39345f (Sol CLEAN, round 4).

pub mod bridge;
pub mod daemon;
pub mod failure;
pub mod fsutil;
pub mod owner;
pub mod phase;
pub mod roots;
pub mod start;
pub mod store;
pub mod watch;
