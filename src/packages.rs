//! Plugin and provider package policy seam.
//!
//! The hub owns install, enable, disable, pin, update, capability grant, and
//! provenance policy. Package managers and provider packages implement behavior
//! behind those contracts; this module does not fetch or install anything.

use crate::providers::ProviderCapability;

/// Hub policy action over an installable package.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackagePolicy {
    /// Install a package after provenance and compatibility checks pass.
    Install,
    /// Enable a package with explicit capability grants.
    Enable,
    /// Disable a package while preserving durable package metadata.
    Disable,
    /// Pin a package version or source revision.
    Pin,
    /// Update a package after compatibility and provenance checks pass.
    Update,
}

/// Declared capability grant for a package.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityGrant {
    /// Capability requested by a provider or plugin package.
    pub capability: ProviderCapability,
    /// Human-readable reason stored with the grant.
    pub reason: &'static str,
}
