//! Public architecture facade for the `botster-hub` product host.
//!
//! `botster-hub` owns product host policy around reusable `botster-core`
//! mechanics. This crate defines hub boundary contracts and a minimal runtime
//! facade over `botster-core`; provider, cloud, Rails, WebRTC, and client
//! transport implementations intentionally live outside this scaffold.
//!
//! ```
//! let summary = botster_hub::architecture_summary();
//! assert!(summary.role_labels().contains(&"botster-core"));
//! assert!(summary.facade_decisions().iter().any(|decision| {
//!     decision.core_operation() == "execute_command(DefaultEngineCommand)"
//!         && decision.exposure() == botster_hub::HubFacadeExposure::Hidden
//! }));
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
pub mod runtime;

use providers::ProviderCapability;

pub use config::{
    CoreEngineOptions, CoreQueueCapacity, DataDirectoryOption, DirectoryList, HostIdentity,
    HostIdentityOptions, HubConfig, HubConfigError, HubStartupOptions, LocalSocketBinding,
    RuntimeEnvironment, SessionDefaults, SessionIoCoalescingOptions, TcpBinding, TransportBindings,
    build_default_config_for_runtime,
};
pub use runtime::{
    HubRuntime, HubRuntimeError, HubRuntimeObservation, HubRuntimeOutput, HubRuntimeSpawnOutcome,
};

/// Compile-checked description of the crate boundaries exposed by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchitectureSummary {
    roles: &'static [Responsibility],
    facade_decisions: &'static [HubFacadeDecision],
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

    /// README-aligned audit of current core operations exposed or hidden by `HubRuntime`.
    pub fn facade_decisions(&self) -> &'static [HubFacadeDecision] {
        self.facade_decisions
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

/// Whether a current core operation is part of the public hub runtime facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubFacadeExposure {
    /// Exposed as an explicit hub method because it is policy or visibility adjacent.
    Exposed,
    /// Intentionally hidden because exposing it would collapse hub policy into core mechanics.
    Hidden,
}

/// One audited core operation and the hub facade decision for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubFacadeDecision {
    core_operation: &'static str,
    exposure: HubFacadeExposure,
    reason: &'static str,
}

impl HubFacadeDecision {
    const fn new(
        core_operation: &'static str,
        exposure: HubFacadeExposure,
        reason: &'static str,
    ) -> Self {
        Self {
            core_operation,
            exposure,
            reason,
        }
    }

    /// Core operation audited for the hub facade.
    pub fn core_operation(&self) -> &'static str {
        self.core_operation
    }

    /// Hub facade exposure decision.
    pub const fn exposure(&self) -> HubFacadeExposure {
        self.exposure
    }

    /// Short reason for the hub facade decision.
    pub fn reason(&self) -> &'static str {
        self.reason
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

const HUB_FACADE_DECISIONS: &[HubFacadeDecision] = &[
    HubFacadeDecision::new(
        "execute_command(DefaultEngineCommand)",
        HubFacadeExposure::Hidden,
        "generic core router would obscure hub admission and policy boundaries",
    ),
    HubFacadeDecision::new(
        "list_sessions",
        HubFacadeExposure::Exposed,
        "host visibility over core-recorded sessions",
    ),
    HubFacadeDecision::new(
        "inspect_session",
        HubFacadeExposure::Exposed,
        "host visibility over lifecycle and activity",
    ),
    HubFacadeDecision::new(
        "read_screen",
        HubFacadeExposure::Exposed,
        "explicit host request for core-owned session screen state",
    ),
    HubFacadeDecision::new(
        "capture_snapshot",
        HubFacadeExposure::Exposed,
        "explicit host request for core-owned snapshot mechanics",
    ),
    HubFacadeDecision::new(
        "replay_snapshot",
        HubFacadeExposure::Exposed,
        "explicit host request for core-owned snapshot replay mechanics",
    ),
    HubFacadeDecision::new(
        "drain_runtime_all_once",
        HubFacadeExposure::Exposed,
        "host scheduler drain hook over live core sessions",
    ),
    HubFacadeDecision::new(
        "report_backpressure",
        HubFacadeExposure::Exposed,
        "typed pressure evidence without hub-owned retry policy",
    ),
    HubFacadeDecision::new(
        "report_delivery_lag",
        HubFacadeExposure::Exposed,
        "typed slow-delivery evidence without hub-owned retry policy",
    ),
    HubFacadeDecision::new(
        "report_delivery_failure",
        HubFacadeExposure::Exposed,
        "typed failed-delivery evidence without hub-owned retry policy",
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
        facade_decisions: HUB_FACADE_DECISIONS,
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

    #[test]
    fn architecture_summary_records_hub_runtime_facade_audit() {
        let summary = architecture_summary();

        let exposed: Vec<_> = summary
            .facade_decisions()
            .iter()
            .filter(|decision| decision.exposure() == HubFacadeExposure::Exposed)
            .map(HubFacadeDecision::core_operation)
            .collect();

        assert!(exposed.contains(&"list_sessions"));
        assert!(exposed.contains(&"inspect_session"));
        assert!(exposed.contains(&"read_screen"));
        assert!(exposed.contains(&"capture_snapshot"));
        assert!(exposed.contains(&"replay_snapshot"));
        assert!(exposed.contains(&"drain_runtime_all_once"));
        assert!(exposed.contains(&"report_backpressure"));
        assert!(exposed.contains(&"report_delivery_lag"));
        assert!(exposed.contains(&"report_delivery_failure"));
        assert!(summary.facade_decisions().iter().any(|decision| {
            decision.core_operation() == "execute_command(DefaultEngineCommand)"
                && decision.exposure() == HubFacadeExposure::Hidden
                && decision.reason().contains("generic core router")
        }));
    }
}
