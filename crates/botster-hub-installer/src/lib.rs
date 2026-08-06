//! The managed installer for the revision-coupled `botster-hub` distribution.
//!
//! This crate is separate from `botster-hub-installation` because the installer
//! needs signature verification and the Hub must not carry a crypto trust root
//! even architecturally. Folding both into one crate behind optional
//! dependencies and a `required-features` bin was rejected: it would make the
//! installer's tests opt-in under `./test.sh`, which silently drops coverage.
//! That is a hack, not a boundary.

pub mod error;
pub mod fetch;
pub mod inject;
pub mod install;
pub mod run;
pub mod verify;
