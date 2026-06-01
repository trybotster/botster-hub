//! Public architecture facade for the `botster-hub` product host.
//!
//! `botster-hub` owns product host policy around reusable `botster-core`
//! mechanics. This crate currently defines boundary contracts only; provider,
//! cloud, Rails, WebRTC, and client transport implementations intentionally
//! live outside this scaffold.
//!
//! ```
//! let summary = botster_hub::architecture_summary();
//! assert!(summary.role_labels().contains(&"botster-core"));
//! assert!(summary.provider_capabilities().contains(
//!     &botster_hub::providers::ProviderCapability::SignalingRelay,
//! ));
//! ```

pub mod adapters;
pub mod auth;
pub mod config;
pub mod core;
pub mod packages;
pub mod persistence;
pub mod providers;

use providers::ProviderCapability;

pub use config::{
    CoreEngineOptions, CoreQueueCapacity, DataDirectoryOption, DirectoryList, HostIdentity,
    HostIdentityOptions, HubConfig, HubConfigError, HubStartupOptions, LocalSocketBinding,
    RuntimeEnvironment, SessionDefaults, SessionIoCoalescingOptions, TcpBinding, TransportBindings,
    build_default_config_for_runtime,
};

/// Compile-checked description of the crate boundaries exposed by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchitectureSummary {
    roles: &'static [Responsibility],
    provider_capabilities: &'static [ProviderCapability],
}

impl ArchitectureSummary {
    /// Stable role labels used by docs, tests, and the binary smoke path.
    pub fn role_labels(&self) -> Vec<&'static str> {
        self.roles.iter().map(Responsibility::label).collect()
    }

    /// Hub-owned provider capability vocabulary.
    pub fn provider_capabilities(&self) -> &'static [ProviderCapability] {
        self.provider_capabilities
    }

    /// Responsibility rows for README-aligned callers.
    pub fn responsibilities(&self) -> &'static [Responsibility] {
        self.roles
    }
}

/// Named ownership boundary in the Botster package layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Responsibility {
    /// Stable short label.
    pub label: &'static str,
    /// One-sentence ownership boundary.
    pub owns: &'static str,
}

impl Responsibility {
    const fn new(label: &'static str, owns: &'static str) -> Self {
        Self { label, owns }
    }

    /// Return the stable short label.
    pub fn label(&self) -> &'static str {
        self.label
    }
}

const RESPONSIBILITIES: &[Responsibility] = &[
    Responsibility::new(
        "botster-core",
        "reusable local engine mechanics and transport-neutral primitives",
    ),
    Responsibility::new(
        "botster-hub",
        "product host policy, capability contracts, admission, lifecycle, and audit hooks",
    ),
    Responsibility::new(
        "CLI",
        "thin operator entrypoints that start or attach to a hub",
    ),
    Responsibility::new(
        "clients",
        "browser, TUI, socket, and custom renderers consuming hub contracts",
    ),
    Responsibility::new(
        "plugins/providers",
        "installable behavior packages that declare capabilities and provenance",
    ),
    Responsibility::new(
        "external providers",
        "cloud federation, signaling relay, browser shell, and API implementations",
    ),
];

const PROVIDER_CAPABILITIES: &[ProviderCapability] = &[
    ProviderCapability::ClientAdmission,
    ProviderCapability::PairingInvites,
    ProviderCapability::SignalingRelay,
    ProviderCapability::HubPresence,
    ProviderCapability::BrowserShell,
    ProviderCapability::Secrets,
    ProviderCapability::CryptoEnvelope,
    ProviderCapability::ExternalApi,
];

/// Return the public architecture summary used by the binary smoke path.
pub fn architecture_summary() -> ArchitectureSummary {
    ArchitectureSummary {
        roles: RESPONSIBILITIES,
        provider_capabilities: PROVIDER_CAPABILITIES,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn architecture_summary_names_required_roles_and_capabilities() {
        let summary = architecture_summary();

        assert_eq!(
            summary.role_labels(),
            vec![
                "botster-core",
                "botster-hub",
                "CLI",
                "clients",
                "plugins/providers",
                "external providers"
            ]
        );
        assert!(
            summary
                .provider_capabilities()
                .contains(&ProviderCapability::SignalingRelay)
        );
        assert!(
            summary
                .provider_capabilities()
                .contains(&ProviderCapability::BrowserShell)
        );
    }
}
