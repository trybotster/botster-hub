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
pub mod daemon_transport;
pub mod entrypoint_supervisor;
pub mod lifecycle;
pub mod lua_runtime;
pub mod mcp;
pub mod packages;
pub mod persistence;
pub mod profile;
pub mod runtime;
pub mod tui;

use botster_core::CapabilitySurface;

pub use capabilities::HubCapabilityRuntime;
pub use client_api::{
    HubClientAdmission, HubClientApi, HubClientCapability, HubClientError, HubClientEvent,
    HubClientGuardedWrite, HubClientIdentity, HubClientObservationKind, HubClientOperation,
    HubClientPackage, HubClientPackageAvailability, HubClientPackageAvailabilityReason,
    HubClientPackageAvailabilityState, HubClientPackageClassification,
    HubClientPackageConfiguration, HubClientPackageDependencyAvailability,
    HubClientPackageDiagnostic, HubClientPackageEnvironmentRequirement,
    HubClientPackageFeatureAvailability, HubClientPackageProcess,
    HubClientPackageRunnableEntrypoint, HubClientPackageState, HubClientPackageWorkingDirectory,
    HubClientPluginLifecycle, HubClientRequest, HubClientResponse, HubClientResponseBody,
    HubClientResult, HubClientRole, HubClientRoutedEnvelopeAck, HubClientRoutedEnvelopeDrain,
    HubClientRoutedEnvelopePublish, HubClientRuntimeErrorKind, HubClientSession, HubClientSpawned,
    HubClientStatus,
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
pub use daemon_transport::{
    DaemonAvailablePackage, DaemonCapability, DaemonCompatibility, DaemonConnection,
    DaemonCoordination, DaemonEnvelope, DaemonEnvelopeAck, DaemonEnvelopeDelivery,
    DaemonEnvelopePublish, DaemonEvent, DaemonIdentity, DaemonNotify, DaemonOperatorError,
    DaemonPackage, DaemonPackageActionRequest, DaemonPackageActionRequiredReference,
    DaemonPackageActionState, DaemonPackageActionStatus, DaemonPackageAvailability,
    DaemonPackageAvailabilityReason, DaemonPackageAvailabilityState, DaemonPackageCompatibility,
    DaemonPackageConfiguration, DaemonPackageDecision, DaemonPackageDependencyAvailability,
    DaemonPackageDiagnostic, DaemonPackageEnvironmentRequirement, DaemonPackageFeatureAvailability,
    DaemonPackageInstallEffect, DaemonPackageInstallPlan, DaemonPackagePin, DaemonPackageProcess,
    DaemonPackageRunnableEntrypoint, DaemonPackageWorkingDirectory, DaemonPluginLifecycle,
    DaemonRequest, DaemonResponse, DaemonResponseKind, DaemonSession, DaemonSessionCleanup,
    DaemonStatus, DaemonTransportError, DaemonTransportResult, request as daemon_transport_request,
    serve_daemon, stream_attach,
};
pub use entrypoint_supervisor::{
    EntrypointDiagnostic, EntrypointProcessSnapshot, EntrypointSupervisor,
    EntrypointSupervisorError, EntrypointSupervisorResult,
};
pub use lifecycle::{
    HubLifecycleError, HubLifecycleResult, HubPluginLifecycle, HubPluginLifecycleStatus,
    HubPluginRuntimeBundle,
};
pub use lua_runtime::{LuaPluginRuntime, LuaPluginRuntimeError, SharedHubCapabilityRuntime};
pub use mcp::{
    McpCallRequest, McpServeError, McpToolDescriptor, McpToolError, McpToolProvider,
    McpToolRegistry, McpToolResult, NativeHubToolProvider, PluginHubToolProvider, serve_mcp_stdio,
};
pub use packages::{
    AvailablePackage, AvailablePackageState, LOCAL_PACKAGE_MANIFEST_FILE,
    LOCAL_PACKAGE_REGISTRY_FILE, PackageAction, PackageAdmissionPolicy, PackageAdmissionReason,
    PackageClassification, PackageCompatibility, PackageCompatibilityResult,
    PackageConfigurationDiagnostic, PackageConfigurationState, PackageConfigurationView,
    PackageDecision, PackageEnvironmentRequirement, PackageInstallDiagnostic, PackageInstallEffect,
    PackageInstallPlan, PackagePin, PackageProvenance, PackageRecord, PackageRegistry,
    PackageRegistryEntrySourceKind, PackageRegistryError, PackageRegistryResult,
    PackageRegistrySnapshot, PackageRegistrySnapshotError, PackageRegistrySource,
    PackageRegistrySourceKind, PackageRunnableDiagnostic, PackageRunnableEntrypoint,
    PackageRunnableEntrypointKind, PackageRunnableMode, PackageRunnableProcess,
    PackageRunnableProcessState, PackageRunnableWorkingDirectory, PackageSourceMetadata,
    PackageState, PackageTrust, PackageTrustClassification, PackageUpdatePolicy,
    PreparedLocalPackage, default_package_policy,
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
    HubLuaPluginLoadError, HubRuntime, HubRuntimeError, HubRuntimeObservation, HubRuntimeOutput,
    daemon_session_to_core_session,
};
pub use tui::{
    ScriptedTuiDriver, ScriptedTuiProof, TuiError, TuiResult, run as run_tui, run_scripted_probe,
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
    /// Public facade or client request shape exists, but the current daemon API does not support it yet.
    Deferred,
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
        "explicit client terminal input path through the core daemon",
    ),
    HubFacadeDecision::new(
        "resize",
        HubFacadeExposure::Exposed,
        "explicit client terminal resize path through the core daemon",
    ),
    HubFacadeDecision::new(
        "guarded_write",
        HubFacadeExposure::Exposed,
        "hub-admitted guarded notification write delegated to core daemon readiness and delivery states",
    ),
    HubFacadeDecision::new(
        "publish/drain/acknowledge_routed_envelope",
        HubFacadeExposure::Exposed,
        "native coordination reference tools delegate queue, cursor, and ack semantics to the core daemon routed-envelope primitive",
    ),
    HubFacadeDecision::new(
        "release_sessions_for_restart/adoption_scan/adopt_session",
        HubFacadeExposure::Exposed,
        "explicit daemon restart/adoption control over worker-backed core sessions",
    ),
    HubFacadeDecision::new(
        "read_screen/capture_snapshot/report_delivery_*",
        HubFacadeExposure::Deferred,
        "daemon-backed core API does not expose these embedded-engine-only helpers yet",
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
        assert!(exposed.contains(&"guarded_write"));
        assert!(exposed.contains(&"publish/drain/acknowledge_routed_envelope"));
        assert!(exposed.contains(&"release_sessions_for_restart/adoption_scan/adopt_session"));
        assert!(summary.facade_decisions().iter().any(|decision| {
            decision.core_operation() == "execute_command(DefaultEngineCommand)"
                && decision.exposure() == HubFacadeExposure::Hidden
                && decision.reason().contains("generic core router")
        }));
        assert!(summary.facade_decisions().iter().any(|decision| {
            decision.core_operation() == "read_screen/capture_snapshot/report_delivery_*"
                && decision.exposure() == HubFacadeExposure::Deferred
                && decision.reason().contains("daemon-backed core API")
        }));
    }
}
