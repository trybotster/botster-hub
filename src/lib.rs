//! Public architecture facade for the `botster-hub` first-party host profile.
//!
//! `botster-hub` is a trusted profile over reusable `botster-core` mechanics.
//! This crate defines profile-owned policy surfaces and a runtime facade over
//! `botster-core-daemon` (`CoreDaemon` + session worker). Provider, cloud,
//! Rails, public WebRTC, and non-local client transport implementations live
//! outside this host profile, not as parallel hub runtimes.
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
//!
//! let summary = botster_hub::architecture_summary();
//! assert!(summary.crate_exports().iter().any(|export| {
//!     export.name() == "daemon_transport Daemon* DTO re-exports"
//!         && export.class() == botster_hub::HubCrateExportClass::ClientContract
//!         && export.stability() == botster_hub::HubCrateExportStability::KeepPublic
//! }));
//!
//! let mut transport = botster_hub::LocalWebrtcTransport::default();
//! assert!(
//!     transport
//!         .issue_bootstrap("botster-web", "web-client", "http://127.0.0.1:1")
//!         .is_ok()
//! );
//! transport.stop_all();
//! ```
//!
//! [`architecture_summary`] classifies Hub policy exports, `botster-hub-client`
//! contract re-exports, and internal modules. `AlreadyInternal` marks
//! crate-private modules. A later dedicated API change may hide remaining
//! `FutureInternal` modules.

pub(crate) mod admission;
pub mod auth;
pub mod capabilities;
pub mod client_api;
pub(crate) mod client_api_dto;
pub mod config;
pub mod credentials;
pub mod daemon;
mod daemon_maintenance;
mod daemon_projection;
pub mod daemon_transport;
pub mod entrypoint_supervisor;
pub(crate) mod event_plane_counters;
mod host_control_fair_write;
pub mod lifecycle;
pub(crate) mod local_webrtc;
pub mod lua_runtime;
pub mod maintenance;
pub mod managed_git_worktrees;
pub mod mcp;
pub mod package_entity_fanout;
pub mod package_event_router;
pub(crate) mod package_event_schema;
pub mod packages;
pub mod persistence;
pub mod profile;
pub mod runtime;
mod session_projection;
pub mod session_types;
#[doc(hidden)]
pub mod source_update;
pub mod spawn_targets;
pub(crate) mod subscription;
mod unix_terminal_adapter;
mod webrtc_terminal_adapter;
pub mod worktrees;

use botster_core::CapabilitySurface;
pub use botster_core::{
    RunnableEntrypointKind, RunnableEntrypointLaunchMode, RunnableEntrypointLaunchResult,
    RunnableEntrypointProcessState, RunnableEntrypointReadiness, RunnableEntrypointResultField,
};

/// Product deadline for the local runtime daemon to publish typed readiness.
///
/// This is public so lifecycle harnesses can keep their liveness backstop
/// strictly outside the production policy they observe.
pub const LOCAL_RUNTIME_DAEMON_READINESS_BUDGET: std::time::Duration =
    std::time::Duration::from_secs(30);

pub use capabilities::HubCapabilityRuntime;
pub use client_api::{
    HubClientAdmission, HubClientApi, HubClientCapability, HubClientCaptureSnapshot,
    HubClientError, HubClientEvent, HubClientGuardedWrite, HubClientIdentity, HubClientModeFlags,
    HubClientModeGatedInputResult, HubClientObservationKind, HubClientOperation, HubClientPackage,
    HubClientPackageAvailability, HubClientPackageAvailabilityReason,
    HubClientPackageAvailabilityState, HubClientPackageClassification,
    HubClientPackageConfiguration, HubClientPackageDependencyAvailability,
    HubClientPackageDiagnostic, HubClientPackageEnvironmentRequirement,
    HubClientPackageFeatureAvailability, HubClientPackageNavigationEntry,
    HubClientPackageNavigationTarget, HubClientPackageProcess, HubClientPackageRunnableEntrypoint,
    HubClientPackageState, HubClientPackageWorkingDirectory, HubClientPluginLifecycle,
    HubClientPluginLifecycleReport, HubClientPluginResourceCounters, HubClientPluginSurface,
    HubClientPluginWorkerCounters, HubClientReadScreen, HubClientRequest, HubClientResponse,
    HubClientResponseBody, HubClientResult, HubClientRole, HubClientRoutedEnvelopeAck,
    HubClientRoutedEnvelopeDrain, HubClientRoutedEnvelopePublish, HubClientRuntimeErrorKind,
    HubClientSession, HubClientSpawned, HubClientStatus,
};
pub use config::{
    CoreEngineOptions, CoreQueueCapacity, DataDirectoryOption, DirectoryList, HostIdentity,
    HostIdentityOptions, HubConfig, HubConfigError, HubStartupOptions, LocalSocketBinding,
    PackageEventPlaneOptions, PackageEventPlanePolicy, RuntimeEnvironment, SessionDefaults,
    SessionIoCoalescingOptions, TcpBinding, TransportBindings, build_default_config_for_runtime,
};
pub use credentials::{
    CredentialKeyPurpose, CredentialPolicyError, CredentialProviderKind, OsKeychainCredentialStore,
    TestFileCredentialStore, credential_key_id, validate_hub_credentials,
};
pub use daemon::{
    HubDaemon, HubDaemonError, HubDaemonResult, HubDaemonState, HubDaemonStatus, HubStateLoadSource,
};
pub use daemon_maintenance::{MAX_OWNER_TURN_MS, MAX_READY_OPERATION_WAIT_MS};
pub use daemon_transport::{
    DaemonApp, DaemonAppLaunchTarget, DaemonAttachOccupancy, DaemonAvailablePackage,
    DaemonCapability, DaemonCompatibility, DaemonConnection, DaemonCoordination, DaemonEnvelope,
    DaemonEnvelopeAck, DaemonEnvelopeDelivery, DaemonEnvelopePublish, DaemonEvent, DaemonHubUpdate,
    DaemonHubUpdateExecution, DaemonHubUpdateExecutionState, DaemonHubUpdateScope,
    DaemonHubUpdateState, DaemonIdentity, DaemonInstallationDiagnostic, DaemonInstallationIdentity,
    DaemonInstallationMode, DaemonModeFlags, DaemonNotify, DaemonOperatorError, DaemonPackage,
    DaemonPackageActionRequest, DaemonPackageActionRequiredReference, DaemonPackageActionState,
    DaemonPackageActionStatus, DaemonPackageAvailability, DaemonPackageAvailabilityReason,
    DaemonPackageAvailabilityState, DaemonPackageCompatibility, DaemonPackageConfiguration,
    DaemonPackageDecision, DaemonPackageDependencyAvailability, DaemonPackageDiagnostic,
    DaemonPackageEnvironmentRequirement, DaemonPackageFeatureAvailability,
    DaemonPackageInstallEffect, DaemonPackageInstallPlan, DaemonPackageNavigationEntry,
    DaemonPackageNavigationSource, DaemonPackagePin, DaemonPackageProcess,
    DaemonPackageRouteDescriptor, DaemonPackageRouteTarget, DaemonPackageRunnableEntrypoint,
    DaemonPackageWorkingDirectory, DaemonPluginLifecycle, DaemonPluginWorkerCounters,
    DaemonRequest, DaemonResolvedAppLaunch, DaemonResolvedSessionType, DaemonResponse,
    DaemonResponseKind, DaemonSession, DaemonSessionCleanup, DaemonSessionContext,
    DaemonSessionType, DaemonSessionTypeContextInput, DaemonSessionTypeDefinition,
    DaemonSessionTypeExecution, DaemonSessionTypeMutationSource, DaemonSessionTypeRequest,
    DaemonSessionTypeWorkingDirectory, DaemonSoftwareIdentity, DaemonSpawnTarget,
    DaemonSpawnTargetValidation, DaemonStatus, DaemonTransportError, DaemonTransportResult,
    DaemonWorktree, DaemonWorktreeGitMetadata, request as daemon_transport_request, serve_daemon,
    stream_attach,
};
pub use entrypoint_supervisor::{
    EntrypointDiagnostic, EntrypointProcessSnapshot, EntrypointSupervisor,
    EntrypointSupervisorError, EntrypointSupervisorResult,
};
pub use lifecycle::{
    HubLifecycleError, HubLifecycleResult, HubPluginLifecycle, HubPluginLifecycleStatus,
    HubPluginRuntimeBundle,
};
pub use local_webrtc::{LocalWebrtcError, LocalWebrtcTransport};
pub use lua_runtime::{
    LuaPluginHostApi, LuaPluginRuntime, LuaPluginRuntimeError, SharedHubCapabilityRuntime,
};
pub use maintenance::{installation_identity, software_identity};
pub use mcp::{
    McpCallRequest, McpServeError, McpToolDescriptor, McpToolError, McpToolProvider,
    McpToolRegistry, McpToolResult, NativeHubToolProvider, PluginHubToolProvider, serve_mcp_stdio,
};
pub use package_event_router::{EventPlaneStatus, PackageEventRouter};
pub use packages::{
    AvailablePackage, AvailablePackageState, HubEmittedEvent, HubPackageEvents, HubPackageManifest,
    LOCAL_PACKAGE_MANIFEST_FILE, LOCAL_PACKAGE_REGISTRY_FILE, PackageAction,
    PackageAdmissionPolicy, PackageAdmissionReason, PackageClassification, PackageCompatibility,
    PackageCompatibilityResult, PackageConfigurationDiagnostic, PackageConfigurationState,
    PackageConfigurationView, PackageDecision, PackageEnvironmentRequirement,
    PackageInstallDiagnostic, PackageInstallEffect, PackageInstallPlan, PackagePin,
    PackageProvenance, PackageRecord, PackageRegistry, PackageRegistryEntrySourceKind,
    PackageRegistryError, PackageRegistryResult, PackageRegistrySnapshot,
    PackageRegistrySnapshotError, PackageRegistrySource, PackageRegistrySourceKind,
    PackageResolvedForegroundLaunch, PackageRunnableDiagnostic, PackageRunnableEntrypoint,
    PackageRunnableProcess, PackageRunnableProcessState, PackageRunnableWorkingDirectory,
    PackageSourceMetadata, PackageState, PackageTrust, PackageTrustClassification,
    PackageUpdatePolicy, PreparedLocalPackage, default_package_policy,
    resolve_foreground_launch_contract,
};
pub use persistence::{
    BootstrapGrantRecord, CapabilityGrantRecord, CredentialKeyReference, DeviceSessionTypeSource,
    FileHubStateStore, HubAuditEntry, HubState, HubStateError, HubStateResult, HubStateStore,
    HubStateStoreError, HubStateStoreResult, LocalRuntimeSettings, PackageAdmissionDecision,
    SchemaMetadata, TrustedBrowserIdentity,
};
pub use profile::{
    CoreRuntimeRole, HostProfileManifest, HostProfileTrust, PolicyArea, Responsibility,
    host_profile,
};
pub use runtime::{
    HubLuaPluginLoadError, HubRuntime, HubRuntimeError, HubRuntimeObservation, HubRuntimeOutput,
    daemon_session_to_core_session,
};
pub use session_types::{
    HubSessionContext, HubSessionType, HubSessionTypeDefinition, HubSessionTypeSource,
    MaterializedSessionType, PackageSessionType, PackageSessionTypeExecution,
    PackageSessionTypeWorkingDirectory, ResolvedSessionType, SessionTypeContextInput,
    SessionTypeError, SessionTypeMutation, SessionTypeMutationSource, SessionTypeRequest,
};
pub use spawn_targets::{
    SpawnTarget, SpawnTargetCreate, SpawnTargetError, SpawnTargetResult, SpawnTargetUpdate,
    SpawnTargetValidation, create_spawn_target, delete_spawn_target, list_spawn_targets,
    show_spawn_target, update_spawn_target, validate_spawn_target,
};
pub use worktrees::{
    Worktree, WorktreeCreate, WorktreeError, WorktreeGitMetadata, WorktreeResult, create_worktree,
    delete_worktree, list_worktrees, show_worktree,
};

/// Compile-checked description of the profile plus the audited `HubRuntime` facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchitectureSummary {
    profile: &'static HostProfileManifest,
    facade_decisions: &'static [HubFacadeDecision],
    crate_exports: &'static [HubCrateExport],
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

    /// Crate-root export classification. Visibility is unchanged by this audit.
    #[must_use]
    pub const fn crate_exports(&self) -> &'static [HubCrateExport] {
        self.crate_exports
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

/// Where a crate-root export belongs after a dedicated visibility change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubCrateExportClass {
    /// Host-profile policy. Stays public on `botster-hub`.
    HubPolicy,
    /// Wire or protocol surface already owned by `botster-hub-client`.
    ClientContract,
    /// Implementation module. Current visibility stays until a dedicated API change.
    Internal,
    /// Core type re-exported because host-profile surfaces name it.
    CoreReexport,
}

/// Current stability of a crate-root export. This audit does not change visibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubCrateExportStability {
    /// Keep the current public export.
    KeepPublic,
    /// Future dedicated visibility change may hide this. Not changed here.
    FutureInternal,
    /// Already crate-private. This audit does not change that.
    AlreadyInternal,
}

/// One audited crate-root module or re-export group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubCrateExport {
    name: &'static str,
    class: HubCrateExportClass,
    stability: HubCrateExportStability,
    reason: &'static str,
}

impl HubCrateExport {
    const fn new(
        name: &'static str,
        class: HubCrateExportClass,
        stability: HubCrateExportStability,
        reason: &'static str,
    ) -> Self {
        Self {
            name,
            class,
            stability,
            reason,
        }
    }

    /// Crate-root module or re-export group name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Ownership class for a later dedicated API change.
    #[must_use]
    pub const fn class(&self) -> HubCrateExportClass {
        self.class
    }

    /// Current stability. `FutureInternal` rows stay public; `AlreadyInternal` is private.
    #[must_use]
    pub const fn stability(&self) -> HubCrateExportStability {
        self.stability
    }

    /// Short reason for the classification.
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
        "read_screen/capture_snapshot",
        HubFacadeExposure::Exposed,
        "explicit daemon-backed terminal readback through HubRuntime and CoreDaemon",
    ),
    HubFacadeDecision::new(
        "report_delivery_*",
        HubFacadeExposure::Deferred,
        "core daemon does not expose delivery-pressure reporting through the production hub path yet",
    ),
];

const HUB_CRATE_EXPORTS: &[HubCrateExport] = &[
    HubCrateExport::new(
        "auth",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "host admission and identity policy",
    ),
    HubCrateExport::new(
        "capabilities",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "capability grant policy",
    ),
    HubCrateExport::new(
        "client_api",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "local HubRuntime client API; not the daemon wire crate",
    ),
    HubCrateExport::new(
        "config",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "startup composition and host configuration",
    ),
    HubCrateExport::new(
        "credentials",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "credential policy and store admission",
    ),
    HubCrateExport::new(
        "daemon",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "HubDaemon host lifecycle",
    ),
    HubCrateExport::new(
        "daemon_projection",
        HubCrateExportClass::Internal,
        HubCrateExportStability::AlreadyInternal,
        "pure DTO projection; already crate-private",
    ),
    HubCrateExport::new(
        "daemon_transport",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "same-device server adapter: accept, handshake, control, cleanup",
    ),
    HubCrateExport::new(
        "entrypoint_supervisor",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "package entrypoint supervision admission",
    ),
    HubCrateExport::new(
        "lifecycle",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "plugin lifecycle policy",
    ),
    HubCrateExport::new(
        "local_webrtc",
        HubCrateExportClass::Internal,
        HubCrateExportStability::AlreadyInternal,
        "crate-private adapter; LocalWebrtcError and LocalWebrtcTransport stay public at crate root",
    ),
    HubCrateExport::new(
        "lua_runtime",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "first-party Lua plugin host",
    ),
    HubCrateExport::new(
        "maintenance",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "software and installation identity",
    ),
    HubCrateExport::new(
        "managed_git_worktrees",
        HubCrateExportClass::Internal,
        HubCrateExportStability::FutureInternal,
        "git worktree implementation behind the worktrees facade",
    ),
    HubCrateExport::new(
        "mcp",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "MCP registration and host serving",
    ),
    HubCrateExport::new(
        "package_entity_fanout",
        HubCrateExportClass::Internal,
        HubCrateExportStability::FutureInternal,
        "package entity fanout implementation",
    ),
    HubCrateExport::new(
        "packages",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "package admission, install, pin, and enablement policy",
    ),
    HubCrateExport::new(
        "persistence",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "durable hub-state persistence",
    ),
    HubCrateExport::new(
        "profile",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "host-profile manifesto",
    ),
    HubCrateExport::new(
        "runtime",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "HubRuntime control-plane facade",
    ),
    HubCrateExport::new(
        "session_types",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "session type admission and projection policy",
    ),
    HubCrateExport::new(
        "source_update",
        HubCrateExportClass::Internal,
        HubCrateExportStability::FutureInternal,
        "already doc-hidden; used by the first-party update command",
    ),
    HubCrateExport::new(
        "spawn_targets",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "spawn target policy",
    ),
    HubCrateExport::new(
        "worktrees",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "worktree policy facade",
    ),
    HubCrateExport::new(
        "botster_core RunnableEntrypoint* re-exports",
        HubCrateExportClass::CoreReexport,
        HubCrateExportStability::KeepPublic,
        "host-profile package surfaces name these core types",
    ),
    HubCrateExport::new(
        "LOCAL_RUNTIME_DAEMON_READINESS_BUDGET",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "lifecycle harnesses observe this production readiness budget",
    ),
    HubCrateExport::new(
        "daemon_transport Daemon* DTO re-exports",
        HubCrateExportClass::ClientContract,
        HubCrateExportStability::KeepPublic,
        "wire DTOs already live in botster-hub-client; hub re-export stays for compatibility",
    ),
    HubCrateExport::new(
        "serve_daemon / daemon_transport_request",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "host server adapter and first-party control helper",
    ),
    HubCrateExport::new(
        "stream_attach",
        HubCrateExportClass::ClientContract,
        HubCrateExportStability::KeepPublic,
        "held-open attach helper already owned by botster-hub-client",
    ),
    HubCrateExport::new(
        "HubClient* re-exports",
        HubCrateExportClass::HubPolicy,
        HubCrateExportStability::KeepPublic,
        "local request API over HubRuntime, not the external daemon wire crate",
    ),
];

/// Return the public architecture summary used by docs and tests.
#[must_use]
pub const fn architecture_summary() -> ArchitectureSummary {
    ArchitectureSummary {
        profile: host_profile(),
        facade_decisions: HUB_FACADE_DECISIONS,
        crate_exports: HUB_CRATE_EXPORTS,
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
        assert!(exposed.contains(&"read_screen/capture_snapshot"));
        assert!(summary.facade_decisions().iter().any(|decision| {
            decision.core_operation() == "execute_command(DefaultEngineCommand)"
                && decision.exposure() == HubFacadeExposure::Hidden
                && decision.reason().contains("generic core router")
        }));
        assert!(summary.facade_decisions().iter().any(|decision| {
            decision.core_operation() == "report_delivery_*"
                && decision.exposure() == HubFacadeExposure::Deferred
                && decision.reason().contains("delivery-pressure")
        }));
    }

    #[test]
    fn architecture_summary_classifies_every_crate_root_module() {
        let names: Vec<_> = architecture_summary()
            .crate_exports()
            .iter()
            .map(HubCrateExport::name)
            .collect();

        for module in [
            "auth",
            "capabilities",
            "client_api",
            "config",
            "credentials",
            "daemon",
            "daemon_projection",
            "daemon_transport",
            "entrypoint_supervisor",
            "lifecycle",
            "local_webrtc",
            "lua_runtime",
            "maintenance",
            "managed_git_worktrees",
            "mcp",
            "package_entity_fanout",
            "packages",
            "persistence",
            "profile",
            "runtime",
            "session_types",
            "source_update",
            "spawn_targets",
            "worktrees",
        ] {
            assert!(
                names.contains(&module),
                "missing crate-root module {module}"
            );
        }
    }

    #[test]
    fn architecture_summary_keeps_current_public_modules_and_reexports() {
        // Compiling these imports is the stability proof. This audit must not hide them.
        #[allow(unused_imports)]
        use crate::{
            DaemonRequest, HubClientRequest, HubRuntime, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
            LocalWebrtcError, LocalWebrtcTransport, PackageRegistry, auth, capabilities,
            client_api, config, credentials, daemon, daemon_transport, entrypoint_supervisor,
            lifecycle, lua_runtime, maintenance, managed_git_worktrees, mcp, package_entity_fanout,
            packages, persistence, profile, runtime, session_types, source_update, spawn_targets,
            worktrees,
        };

        let _: Option<DaemonRequest> = None;
        let _: Option<HubClientRequest> = None;
        let _: Option<HubRuntime> = None;
        let _: Option<PackageRegistry> = None;
        let _: Option<LocalWebrtcError> = None;
        let _: Option<LocalWebrtcTransport> = None;
        let _ = LOCAL_RUNTIME_DAEMON_READINESS_BUDGET;
        let _ = source_update::mark_update_running;
        let _ = std::any::type_name::<auth::AuthHook>();
    }

    #[test]
    fn architecture_summary_defers_visibility_changes_for_internal_modules() {
        let summary = architecture_summary();
        for name in ["daemon_projection", "local_webrtc"] {
            let export = summary
                .crate_exports()
                .iter()
                .find(|export| export.name() == name)
                .unwrap_or_else(|| panic!("missing export {name}"));
            assert_eq!(export.class(), HubCrateExportClass::Internal, "{name}");
            assert_eq!(
                export.stability(),
                HubCrateExportStability::AlreadyInternal,
                "{name}"
            );
        }

        for name in [
            "managed_git_worktrees",
            "package_entity_fanout",
            "source_update",
        ] {
            let export = summary
                .crate_exports()
                .iter()
                .find(|export| export.name() == name)
                .unwrap_or_else(|| panic!("missing export {name}"));
            assert_eq!(export.class(), HubCrateExportClass::Internal, "{name}");
            assert_eq!(
                export.stability(),
                HubCrateExportStability::FutureInternal,
                "{name}"
            );
        }
    }

    #[test]
    fn architecture_summary_keeps_client_contract_reexports_public() {
        let summary = architecture_summary();
        for name in ["daemon_transport Daemon* DTO re-exports", "stream_attach"] {
            let export = summary
                .crate_exports()
                .iter()
                .find(|export| export.name() == name)
                .unwrap_or_else(|| panic!("missing export {name}"));
            assert_eq!(
                export.class(),
                HubCrateExportClass::ClientContract,
                "{name}"
            );
            assert_eq!(
                export.stability(),
                HubCrateExportStability::KeepPublic,
                "{name}"
            );
        }

        let client_api = summary
            .crate_exports()
            .iter()
            .find(|export| export.name() == "client_api")
            .expect("client_api");
        assert_eq!(client_api.class(), HubCrateExportClass::HubPolicy);
        assert!(client_api.reason().contains("not the daemon wire crate"));
    }

    fn production_source(source: &str) -> String {
        let mut out = String::new();
        let mut skip_depth = 0i32;
        let mut skipping_item = false;
        let mut seen_body = false;
        for line in source.lines() {
            let trimmed = line.trim();
            let opens = line.matches('{').count() as i32;
            let closes = line.matches('}').count() as i32;
            if trimmed.starts_with("#[cfg(test)]") {
                skipping_item = true;
                seen_body = false;
                skip_depth = 0;
                continue;
            }
            if skipping_item {
                skip_depth += opens - closes;
                if opens > 0 {
                    seen_body = true;
                }
                if (seen_body && skip_depth <= 0) || (!seen_body && trimmed.ends_with(';')) {
                    skipping_item = false;
                    skip_depth = 0;
                }
                continue;
            }
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    const FORBIDDEN_PRODUCTION_CONSTRUCTS: &[&str] = &[
        r#""READY""#,
        r#""PAGE""#,
        r#""FINISH""#,
        "GHOSTSNP",
        "drain_subscription(",
        "drain_runtime_once(",
        ".drain(session_id",
        "lifecycle_baseline()",
        "DaemonEvent::TerminalOutput",
        "DaemonEvent::Snapshot",
        "DaemonEvent::Scrollback",
        "DaemonEvent::ProcessExit",
        "DaemonEvent::AttachState",
    ];

    #[test]
    fn production_source_scan_covers_items_after_cfg_test_imports() {
        let source = concat!(
            "#[cfg(test)]\n",
            "use crate::test_only;\n",
            "#[cfg(test)]\n",
            "fn helper(\n",
            "    x: u8,\n",
            ") {\n",
            "    drain_subscription();\n",
            "}\n",
            "fn production() {}\n",
            "fn sneaky() { drain_subscription(); }\n",
        );
        let scanned = production_source(source);
        assert!(scanned.contains("fn production()"));
        assert!(scanned.contains("drain_subscription()"));
        assert!(!scanned.contains("test_only"));
    }

    #[test]
    fn production_source_scan_covers_items_after_one_line_cfg_test() {
        let source = concat!(
            "#[cfg(test)]\n",
            "fn helper() {}\n",
            "fn production() {}\n",
            "fn sneaky() { drain_subscription(); }\n",
        );
        let scanned = production_source(source);
        assert!(scanned.contains("fn production()"));
        assert!(scanned.contains("drain_subscription()"));
        assert!(!scanned.contains("fn helper()"));
    }

    #[test]
    fn production_source_known_positives_catch_every_forbidden_construct() {
        for forbidden in FORBIDDEN_PRODUCTION_CONSTRUCTS {
            let source =
                format!("#[cfg(test)]\nfn helper() {{}}\nfn sneaky() {{ {forbidden}; }}\n");
            let scanned = production_source(&source);
            assert!(
                scanned.contains(forbidden),
                "scanner must remain live for {forbidden}"
            );
        }
    }

    fn contains_two_argument_drain(source: &str) -> bool {
        let mut rest = source;
        while let Some(idx) = rest.find(".drain(") {
            rest = &rest[idx + ".drain(".len()..];
            let mut depth = 1i32;
            let mut saw_comma = false;
            for ch in rest.chars() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    ',' if depth == 1 => saw_comma = true,
                    _ => {}
                }
            }
            if saw_comma {
                return true;
            }
        }
        false
    }

    #[test]
    fn production_sources_reject_terminal_drain_and_snapshot_phase_decode() {
        let files = [
            ("src/runtime.rs", include_str!("runtime.rs")),
            ("src/client_api.rs", include_str!("client_api.rs")),
            (
                "src/daemon_transport.rs",
                include_str!("daemon_transport.rs"),
            ),
            (
                "src/subscription/entity.rs",
                include_str!("subscription/entity.rs"),
            ),
            (
                "src/subscription/attach_routes.rs",
                include_str!("subscription/attach_routes.rs"),
            ),
            ("src/main.rs", include_str!("main.rs")),
            ("src/local_webrtc.rs", include_str!("local_webrtc.rs")),
            ("src/client_api_dto.rs", include_str!("client_api_dto.rs")),
            (
                "src/client_api_dto/response.rs",
                include_str!("client_api_dto/response.rs"),
            ),
            (
                "src/client_api_dto/session.rs",
                include_str!("client_api_dto/session.rs"),
            ),
            (
                "src/client_api_dto/package.rs",
                include_str!("client_api_dto/package.rs"),
            ),
            (
                "src/client_api_dto/workspace.rs",
                include_str!("client_api_dto/workspace.rs"),
            ),
            (
                "src/client_api_dto/plugin.rs",
                include_str!("client_api_dto/plugin.rs"),
            ),
            ("src/daemon/error.rs", include_str!("daemon/error.rs")),
            ("src/daemon/shutdown.rs", include_str!("daemon/shutdown.rs")),
        ];
        for (path, source) in files {
            let production = production_source(source);
            for forbidden in FORBIDDEN_PRODUCTION_CONSTRUCTS {
                if *forbidden == "GHOSTSNP" && path == "src/runtime.rs" {
                    continue;
                }
                assert!(
                    !production.contains(forbidden),
                    "{path} production source must not contain {forbidden}"
                );
            }
            assert!(
                !contains_two_argument_drain(&production),
                "{path} production source must not call two-argument Core drain"
            );
        }
    }

    #[test]
    fn two_argument_drain_scan_is_independent_of_local_variable_names() {
        assert!(contains_two_argument_drain("core.drain(&id, now_seconds);"));
        assert!(contains_two_argument_drain(
            "core_daemon.drain(session_id, last_output_at)"
        ));
        assert!(!contains_two_argument_drain("operations.drain(..)"));
        assert!(!contains_two_argument_drain("tasks.drain(..)"));
        let sneaky = production_source(
            "#[cfg(test)]\nfn helper() { core.drain(&id, now); }\nfn production() { core.drain(&id, now); }\n",
        );
        assert!(contains_two_argument_drain(&sneaky));
    }
}
