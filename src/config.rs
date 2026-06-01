//! Hub-owned configuration policy seam.
//!
//! The hub decides where product host configuration is discovered and how
//! policy is resolved. This module intentionally does not choose filesystem
//! paths or load concrete config files yet.

/// Configuration areas owned by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigArea {
    /// Device or host-level hub identity and local policy.
    Host,
    /// Provider package enablement, pinning, and capability grants.
    Providers,
    /// Client admission and transport policy.
    Clients,
}
