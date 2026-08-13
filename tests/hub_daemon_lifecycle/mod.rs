//! Shared helpers for lifecycle integration proofs, split by owner.
//!
//! Test functions are `include!`d from `tests/hub_daemon_lifecycle_test.rs`
//! so `./test.sh --test hub_daemon_lifecycle_test <fn> -- --exact` keeps working.

pub(crate) mod cli;
pub(crate) mod common;
pub(crate) mod operator_console_fixtures;
pub(crate) mod package_fixtures;
pub(crate) mod process;
pub(crate) mod session_fixtures;
pub(crate) mod webrtc_fixtures;

pub(crate) use cli::*;
pub(crate) use common::*;
pub(crate) use operator_console_fixtures::*;
pub(crate) use package_fixtures::*;
pub(crate) use process::*;
pub(crate) use session_fixtures::*;
pub(crate) use webrtc_fixtures::*;
