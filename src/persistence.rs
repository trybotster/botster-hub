//! Hub-owned persistence policy seam.
//!
//! Persistence choices for host state, package state, and provider state are
//! product policy. This scaffold defines the buckets without selecting a
//! database or storage implementation.

/// Persistence buckets the hub must govern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceBucket {
    /// Durable host and admission state.
    HostState,
    /// Installed package metadata, pins, provenance, and enabled state.
    PackageState,
    /// Provider-owned runtime metadata admitted by hub policy.
    ProviderState,
}
