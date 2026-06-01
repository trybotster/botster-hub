//! Cloud provider contract seam.
//!
//! This module defines hub-visible cloud federation contracts only. Cloud
//! federation implementations live in installable provider packages outside the
//! hub crate.

/// Cloud-facing provider contract categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudContract {
    /// Provider publishes hub presence externally.
    Presence,
    /// Provider brokers cross-device or hosted federation metadata.
    Federation,
}
