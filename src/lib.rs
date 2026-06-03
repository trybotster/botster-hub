//! Public architecture facade for the `botster-hub` first-party host profile.
//!
//! `botster-hub` is a trusted profile over reusable `botster-core` mechanics.
//! This crate defines profile-owned policy seams and a minimal runtime facade
//! over `botster-core`; provider, cloud, Rails, WebRTC, and client transport
//! implementations intentionally live outside this scaffold.
//!
//! ```
//! let profile = botster_hub::host_profile();
//! assert_eq!(profile.id, "botster-hub");
//! assert!(profile.capability_surfaces().contains(
//!     &botster_core::CapabilitySurface::SignalingRelay,
//! ));
//!
//! let summary = botster_hub::architecture_summary();
//! assert!(summary.facade_decisions().iter().any(|decision| {
//!     decision.core_operation() == "execute_command(DefaultEngineCommand)"
//!         && decision.exposure() == botster_hub::HubFacadeExposure::Hidden
//! }));
//!
//! let registry = botster_hub::PackageRegistry::new(botster_core::CapabilitySet::new());
//! assert!(registry.packages().is_empty());
//!
//! let policy = botster_hub::default_package_policy();
//! assert_eq!(
//!     policy.registry().granted_capabilities().len(),
//!     profile.default_capability_grants().len()
//! );
//! ```

pub mod auth;
pub mod capabilities;
pub mod client_api;
pub mod config;
pub mod daemon;
pub mod lifecycle;
pub mod packages;
pub mod persistence;
pub mod profile;
pub mod runtime;

use botster_core::CapabilitySurface;

pub use capabilities::HubCapabilityRuntime;
pub use client_api::{
    HubClientAdmission, HubClientApi, HubClientCapability, HubClientError, HubClientEvent,
    HubClientIdentity, HubClientObservationKind, HubClientOperation, HubClientPackage,
    HubClientPackageClassification, HubClientPackageState, HubClientPluginLifecycle,
    HubClientRequest, HubClientResponse, HubClientResponseBody, HubClientResult, HubClientRole,
    HubClientSession, HubClientSpawned, HubClientStatus,
};
pub use config::{
    CoreEngineOptions, CoreQueueCapacity, DataDirectoryOption, DirectoryList, HostIdentity,
    HostIdentityOptions, HubConfig, HubConfigError, HubStartupOptions, LocalSocketBinding,
    RuntimeEnvironment, SessionDefaults, SessionIoCoalescingOptions, TcpBinding, TransportBindings,
    build_default_config_for_runtime,
};
pub use daemon::{
    HubDaemon, HubDaemonError, HubDaemonResult, HubDaemonState, HubDaemonStatus, HubStateLoadSource,
};
pub use lifecycle::{
    HubLifecycleError, HubLifecycleResult, HubPluginLifecycle, HubPluginLifecycleStatus,
    HubPluginRuntimeBundle,
};
pub use packages::{
    LOCAL_PACKAGE_MANIFEST_FILE, PackageAction, PackageAdmissionPolicy, PackageAdmissionReason,
    PackageClassification, PackageDecision, PackagePin, PackageProvenance, PackageRecord,
    PackageRegistry, PackageRegistryError, PackageRegistryResult, PackageRegistrySnapshot,
    PackageRegistrySnapshotError, PackageState, PackageUpdatePolicy, PreparedLocalPackage,
    default_package_policy,
};
pub use persistence::{
    CapabilityGrantRecord, FileHubStateStore, HubAuditEntry, HubState, HubStateError,
    HubStateResult, HubStateStore, HubStateStoreError, HubStateStoreResult, LocalRuntimeSettings,
    PackageAdmissionDecision, SchemaMetadata,
};
pub use profile::{
    CoreRuntimeRole, HostProfileManifest, HostProfileTrust, PolicyArea, Responsibility,
    host_profile,
};
pub use runtime::{
    HubRuntime, HubRuntimeError, HubRuntimeObservation, HubRuntimeOutput, HubRuntimeSpawnOutcome,
};

/// Compile-checked description of the profile plus the audited `HubRuntime` facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchitectureSummary {
    profile: &'static HostProfileManifest,
    facade_decisions: &'static [HubFacadeDecision],
}

impl ArchitectureSummary {
    /// Stable role labels used by docs, tests, and the binary smoke path.
    #[must_use]
    pub fn role_labels(&self) -> Vec<&'static str> {
        self.profile.role_labels()
    }

    /// Capability surfaces governed by the first-party host profile.
    #[must_use]
    pub const fn capability_surfaces(&self) -> &'static [CapabilitySurface] {
        self.profile.capability_surfaces()
    }

    /// Responsibility rows for README-aligned callers.
    #[must_use]
    pub const fn responsibilities(&self) -> &'static [Responsibility] {
        self.profile.responsibilities()
    }

    /// README-aligned audit of current core operations exposed or hidden by `HubRuntime`.
    #[must_use]
    pub const fn facade_decisions(&self) -> &'static [HubFacadeDecision] {
        self.facade_decisions
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
    #[must_use]
    pub const fn core_operation(&self) -> &'static str {
        self.core_operation
    }

    /// Hub facade exposure decision.
    #[must_use]
    pub const fn exposure(&self) -> HubFacadeExposure {
        self.exposure
    }

    /// Short reason for the hub facade decision.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        self.reason
    }
}

const HUB_FACADE_DECISIONS: &[HubFacadeDecision] = &[
    HubFacadeDecision::new(
        "PluginWorkerEngine::load_plugin",
        HubFacadeExposure::Exposed,
        "enabled hub package records are registered through core worker lifecycle",
    ),
    HubFacadeDecision::new(
        "PluginWorkerEngine::invoke",
        HubFacadeExposure::Exposed,
        "plugin handlers dispatch through core worker capability and timeout enforcement",
    ),
    HubFacadeDecision::new(
        "PluginWorkerEngine::reload_plugin",
        HubFacadeExposure::Exposed,
        "plugin reload cleanup and replacement stay in core worker mechanics",
    ),
    HubFacadeDecision::new(
        "PluginWorkerEngine::unload_plugin",
        HubFacadeExposure::Exposed,
        "plugin unload cleanup stays scoped by core worker ownership",
    ),
    HubFacadeDecision::new(
        "PluginCapabilityRuntime::submit",
        HubFacadeExposure::Exposed,
        "hub owns concrete local capability policy and submits through core request contracts",
    ),
    HubFacadeDecision::new(
        "PluginCapabilityRuntime::drain_events",
        HubFacadeExposure::Exposed,
        "plugin capability completions and timer events are drained through a hub-owned path",
    ),
    HubFacadeDecision::new(
        "PluginCapabilityRuntime::cleanup_plugin",
        HubFacadeExposure::Exposed,
        "capability resources are released during hub plugin reload and unload",
    ),
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
        "spawn_session",
        HubFacadeExposure::Exposed,
        "host-admitted local session creation through core mechanics",
    ),
    HubFacadeDecision::new(
        "attach_client",
        HubFacadeExposure::Exposed,
        "explicit client subscription handshake without global state hydration",
    ),
    HubFacadeDecision::new(
        "detach_client",
        HubFacadeExposure::Exposed,
        "explicit client subscription teardown through core mechanics",
    ),
    HubFacadeDecision::new(
        "write_bytes",
        HubFacadeExposure::Exposed,
        "explicit client terminal input path through core mechanics",
    ),
    HubFacadeDecision::new(
        "resize",
        HubFacadeExposure::Exposed,
        "explicit client terminal resize path through core mechanics",
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

/// Return the public architecture summary used by docs and tests.
#[must_use]
pub const fn architecture_summary() -> ArchitectureSummary {
    ArchitectureSummary {
        profile: host_profile(),
        facade_decisions: HUB_FACADE_DECISIONS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn architecture_summary_names_profile_roles_and_capabilities() {
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
                .capability_surfaces()
                .contains(&CapabilitySurface::SignalingRelay)
        );
        assert!(
            summary
                .capability_surfaces()
                .contains(&CapabilitySurface::BrowserShell)
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

        assert!(exposed.contains(&"PluginCapabilityRuntime::submit"));
        assert!(exposed.contains(&"PluginCapabilityRuntime::drain_events"));
        assert!(exposed.contains(&"PluginCapabilityRuntime::cleanup_plugin"));
        assert!(exposed.contains(&"list_sessions"));
        assert!(exposed.contains(&"spawn_session"));
        assert!(exposed.contains(&"attach_client"));
        assert!(exposed.contains(&"detach_client"));
        assert!(exposed.contains(&"write_bytes"));
        assert!(exposed.contains(&"resize"));
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
