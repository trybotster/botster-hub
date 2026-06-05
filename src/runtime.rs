//! Profile-owned runtime facade over the core daemon session supervisor.
//!
//! The first-party host profile owns explicit configuration and admission
//! policy. Session process mechanics, terminal byte routing, activity
//! accounting, guarded-write readiness, and shutdown stay in `botster-core`
//! through `botster-core-daemon`.

use botster_core::{
    BotsterEngineObservation, BotsterEngineOutput, ClientId, CoreSession, CoreSessionMetadata,
    ManagedSessionRuntimeError, PluginCapabilityRuntime, PluginCleanupResult,
    PluginInvocationOutcome, PluginInvocationRequest, PluginKey, RequestId, SessionId,
    SessionLifecycleState, SessionRuntimeErrorKind, SessionSpawnRequest, SubscriptionId,
};
use botster_core_daemon::{
    CoreDaemon, CoreDaemonConfig, CoreDaemonError, DaemonSession, DrainResult, GuardedWriteRequest,
    GuardedWriteResult, RegistrySessionState, SessionAdoptionReport, SessionAdoptionState,
    SpawnSessionRequest,
};
use std::env;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::capabilities::HubCapabilityRuntime;
use crate::config::HubConfig;
use crate::lifecycle::{
    HubLifecycleResult, HubPluginLifecycle, HubPluginLifecycleStatus, HubPluginRuntimeBundle,
};
use crate::packages::PackageRegistry;
use crate::persistence::{FileHubStateStore, HubState, HubStateStore, HubStateStoreError};

/// Hub-owned adapter and policy facade over the default local core engine.
///
/// This facade exposes host-adjacent admission, visibility, runtime-drain, and
/// typed pressure-reporting operations. It intentionally does not expose core's
/// generic `DefaultEngineCommand` router; hub callers use explicit methods so
/// admission and policy boundaries remain visible at the hub layer.
pub struct HubRuntime {
    config: HubConfig,
    state: HubState,
    core_daemon: CoreDaemon,
    reconciliation: HubSessionReconciliation,
    plugin_lifecycle: HubPluginLifecycle,
    capability_runtime: HubCapabilityRuntime,
    last_capability_cleanup: Option<PluginCleanupResult>,
}

/// Deterministic session reconciliation summary from hub startup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HubSessionReconciliation {
    /// Registry-backed sessions that were adopted into the restarted hub.
    pub recovered_sessions: Vec<SessionId>,
    /// Registry-backed sessions that were marked stale by hub startup policy.
    pub stale_sessions: Vec<SessionId>,
}

impl HubRuntime {
    /// Build a hub runtime from explicit, already-validated hub config.
    #[must_use]
    pub fn new(config: HubConfig) -> Self {
        let state = HubState::from_config(&config);
        let core_daemon = CoreDaemon::new(core_daemon_config(&config.data_directory));
        Self {
            capability_runtime: HubCapabilityRuntime::from_config(&config),
            config,
            state,
            core_daemon,
            reconciliation: HubSessionReconciliation::default(),
            plugin_lifecycle: HubPluginLifecycle::new(),
            last_capability_cleanup: None,
        }
    }

    /// Load durable hub state from the resolved data directory before building runtime.
    pub fn load(config: HubConfig) -> HubRuntimeResult<Self> {
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        Self::load_from_store(config, &store)
    }

    /// Load durable hub state through an explicit storage boundary.
    pub fn load_from_store(
        config: HubConfig,
        store: &impl HubStateStore,
    ) -> HubRuntimeResult<Self> {
        let state = store.load_or_initialize(&config)?;
        let core_daemon = CoreDaemon::new(core_daemon_config(&config.data_directory));
        let mut runtime = Self {
            capability_runtime: HubCapabilityRuntime::from_config(&config),
            config,
            state,
            core_daemon,
            reconciliation: HubSessionReconciliation::default(),
            plugin_lifecycle: HubPluginLifecycle::new(),
            last_capability_cleanup: None,
        };
        runtime.reconcile_sessions(0)?;
        Ok(runtime)
    }

    /// Return the policy-resolved hub config that created this runtime.
    #[must_use]
    pub const fn config(&self) -> &HubConfig {
        &self.config
    }

    /// Return the concrete local capability runtime owned by this hub.
    #[must_use]
    pub const fn capability_runtime(&self) -> &HubCapabilityRuntime {
        &self.capability_runtime
    }

    /// Return the durable hub state loaded for this runtime.
    #[must_use]
    pub const fn state(&self) -> &HubState {
        &self.state
    }

    /// Return the startup reconciliation decisions made against the core daemon registry.
    #[must_use]
    pub const fn reconciliation(&self) -> &HubSessionReconciliation {
        &self.reconciliation
    }

    /// Load an enabled package through core plugin worker mechanics.
    pub fn load_plugin_package(
        &mut self,
        registry: &PackageRegistry,
        package_name: &str,
        bundle: HubPluginRuntimeBundle,
    ) -> HubLifecycleResult<PluginKey> {
        self.plugin_lifecycle
            .load_package(registry, package_name, bundle)
    }

    /// Invoke a plugin handler through core plugin worker mechanics.
    #[must_use]
    pub fn invoke_plugin(&self, request: PluginInvocationRequest) -> PluginInvocationOutcome {
        self.plugin_lifecycle.invoke(request)
    }

    /// Reload an enabled package through core plugin worker cleanup and replacement.
    pub fn reload_plugin_package(
        &mut self,
        request_id: RequestId,
        registry: &PackageRegistry,
        package_name: &str,
        bundle: HubPluginRuntimeBundle,
    ) -> HubLifecycleResult<PluginCleanupResult> {
        let plugin_key = PluginKey(package_name.to_string());
        let capability_cleanup = self.cleanup_plugin_capabilities(&plugin_key).ok();
        let mut lifecycle_cleanup =
            self.plugin_lifecycle
                .reload_package(request_id, registry, package_name, bundle)?;
        if let Some(cleanup) = capability_cleanup {
            lifecycle_cleanup
                .removed_resources
                .extend(cleanup.removed_resources.clone());
            self.last_capability_cleanup = Some(cleanup);
        }
        Ok(lifecycle_cleanup)
    }

    /// Unload a plugin package through core plugin worker cleanup mechanics.
    #[must_use]
    pub fn unload_plugin_package(
        &mut self,
        request_id: RequestId,
        package_name: &str,
    ) -> PluginCleanupResult {
        let plugin_key = PluginKey(package_name.to_string());
        let capability_cleanup = self.cleanup_plugin_capabilities(&plugin_key).ok();
        let mut lifecycle_cleanup = self
            .plugin_lifecycle
            .unload_package(request_id, package_name);
        if let Some(cleanup) = capability_cleanup {
            lifecycle_cleanup
                .removed_resources
                .extend(cleanup.removed_resources.clone());
            self.last_capability_cleanup = Some(cleanup);
        }
        lifecycle_cleanup
    }

    /// Submit a plugin capability request through the hub-owned concrete runtime.
    pub fn submit_capability_request(
        &mut self,
        request: botster_core::CapabilityRuntimeRequest,
    ) -> Result<botster_core::CapabilityRuntimeHandle, botster_core::CapabilityRuntimeError> {
        self.capability_runtime.submit(request)
    }

    /// Cancel one plugin-owned capability operation.
    pub fn cancel_capability_operation(
        &mut self,
        plugin_key: &PluginKey,
        operation_id: &botster_core::CapabilityOperationId,
    ) -> Result<(), botster_core::CapabilityRuntimeError> {
        self.capability_runtime.cancel(plugin_key, operation_id)
    }

    /// Release one plugin-owned capability resource.
    pub fn release_capability_resource(
        &mut self,
        resource: botster_core::PluginResourceRef,
    ) -> Result<(), botster_core::CapabilityRuntimeError> {
        self.capability_runtime.release_resource(resource)
    }

    /// Drain currently available capability events for one plugin.
    pub fn drain_capability_events(
        &mut self,
        plugin_key: &PluginKey,
    ) -> Result<Vec<botster_core::CapabilityRuntimeEvent>, botster_core::CapabilityRuntimeError>
    {
        self.capability_runtime.drain_events(plugin_key)
    }

    /// Drain capability events after advancing the local logical timer clock.
    pub fn drain_capability_events_at(
        &mut self,
        plugin_key: &PluginKey,
        now_ms: u64,
    ) -> Result<Vec<botster_core::CapabilityRuntimeEvent>, botster_core::CapabilityRuntimeError>
    {
        self.capability_runtime.drain_events_at(plugin_key, now_ms)
    }

    /// Stop all capability runtime resources owned by one plugin.
    pub fn cleanup_plugin_capabilities(
        &mut self,
        plugin_key: &PluginKey,
    ) -> Result<PluginCleanupResult, botster_core::CapabilityRuntimeError> {
        let cleanup = self.capability_runtime.cleanup_plugin(plugin_key)?;
        self.last_capability_cleanup = Some(cleanup.clone());
        Ok(cleanup)
    }

    /// Last capability cleanup produced by reload, unload, or explicit cleanup.
    #[must_use]
    pub const fn last_capability_cleanup(&self) -> Option<&PluginCleanupResult> {
        self.last_capability_cleanup.as_ref()
    }

    /// Return read-only plugin lifecycle status derived from hub package records and load state.
    #[must_use]
    pub fn plugin_lifecycle_status(
        &self,
        registry: &PackageRegistry,
    ) -> Vec<HubPluginLifecycleStatus> {
        self.plugin_lifecycle.status(registry)
    }

    /// Return a daemon-recorded session summary.
    pub fn session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<DaemonSession>, CoreDaemonError> {
        Ok(self
            .core_daemon
            .list()?
            .into_iter()
            .find(|session| &session.session_id == session_id))
    }

    /// Return daemon-recorded sessions for host visibility without exposing core's command router.
    pub fn list_sessions(&self) -> Result<Vec<DaemonSession>, CoreDaemonError> {
        self.core_daemon.list()
    }

    /// Spawn a daemon-owned session through core from a host-owned request.
    pub fn spawn_session(
        &mut self,
        request: SessionSpawnRequest,
        metadata: CoreSessionMetadata,
        now_seconds: u64,
    ) -> Result<CoreSession, CoreDaemonError> {
        self.core_daemon
            .spawn(SpawnSessionRequest { request, metadata }, now_seconds)
    }

    /// Attach a client subscription to a session through the core daemon.
    pub fn attach_client(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.core_daemon
            .attach(client_id, session_id, subscription_id, now_seconds)
            .map(|_| ())
    }

    /// Detach a client subscription from a session through the core daemon.
    pub fn detach_client(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.core_daemon
            .detach(client_id, session_id, subscription_id, now_seconds)
    }

    /// Write terminal bytes into a session through the core daemon.
    pub fn write_bytes(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        data: impl Into<Vec<u8>>,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.core_daemon
            .input(client_id, session_id, data, now_seconds)
    }

    /// Resize a session terminal through the core daemon.
    pub fn resize(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        rows: u16,
        cols: u16,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.core_daemon
            .resize(client_id, session_id, rows, cols, now_seconds)
    }

    /// Drain available daemon output through core's subscription path.
    pub fn drain_runtime_once(
        &mut self,
        session_id: &SessionId,
        last_output_at: u64,
    ) -> Result<DrainResult, CoreDaemonError> {
        self.core_daemon.drain(session_id, last_output_at)
    }

    /// Evaluate guarded-write readiness and inject only through the core daemon.
    pub fn guarded_write(
        &mut self,
        request: GuardedWriteRequest,
    ) -> Result<GuardedWriteResult, CoreDaemonError> {
        self.core_daemon.guarded_write(request)
    }

    /// Release worker-backed sessions before an intentional daemon restart.
    pub fn release_sessions_for_restart(&mut self) {
        self.core_daemon.release_for_restart();
    }

    /// Release worker-backed sessions before an intentional hub restart.
    pub fn release_for_restart(&mut self) {
        self.release_sessions_for_restart();
    }

    /// Scan daemon registry records for worker-backed restart/adoption evidence.
    pub fn adoption_scan(&self) -> Result<Vec<SessionAdoptionReport>, CoreDaemonError> {
        self.core_daemon.adoption_scan()
    }

    /// Reattach one live worker-backed session after daemon restart.
    pub fn adopt_session(
        &mut self,
        session_id: &SessionId,
        now_seconds: u64,
    ) -> Result<CoreSession, CoreDaemonError> {
        self.core_daemon.adopt_session(session_id, now_seconds)
    }

    /// Shut down one daemon-owned session through core.
    pub fn shutdown_session(
        &mut self,
        session_id: SessionId,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.core_daemon.shutdown(Some(session_id), now_seconds)
    }

    fn reconcile_sessions(&mut self, now_seconds: u64) -> Result<(), CoreDaemonError> {
        self.reconciliation = HubSessionReconciliation::default();
        for report in self.core_daemon.adoption_scan()? {
            match report.state {
                SessionAdoptionState::Adoptable => {
                    match self
                        .core_daemon
                        .adopt_session(&report.record.session_id, now_seconds)
                    {
                        Ok(session) => {
                            self.reconciliation
                                .recovered_sessions
                                .push(session.session_id);
                        }
                        Err(error) if is_stale_worker_control_socket_adoption_error(&error) => {
                            self.core_daemon
                                .mark_stale(&report.record.session_id, now_seconds)?;
                            self.reconciliation
                                .stale_sessions
                                .push(report.record.session_id);
                        }
                        Err(error) => return Err(error),
                    }
                }
                SessionAdoptionState::MissingProtocolEvidence
                | SessionAdoptionState::StaleWorker { .. }
                | SessionAdoptionState::UnhealthyWorker { .. }
                | SessionAdoptionState::DuplicateWorker { .. } => {
                    self.core_daemon
                        .mark_stale(&report.record.session_id, now_seconds)?;
                    self.reconciliation
                        .stale_sessions
                        .push(report.record.session_id);
                }
                SessionAdoptionState::Terminal => {
                    if report.record.state == RegistrySessionState::Running {
                        self.core_daemon
                            .mark_stale(&report.record.session_id, now_seconds)?;
                        self.reconciliation
                            .stale_sessions
                            .push(report.record.session_id);
                    }
                }
            }
        }
        Ok(())
    }
}

fn is_stale_worker_control_socket_adoption_error(error: &CoreDaemonError) -> bool {
    match error {
        CoreDaemonError::Engine(ManagedSessionRuntimeError::Runtime(runtime_error)) => {
            runtime_error.kind == SessionRuntimeErrorKind::SpawnFailed
                && runtime_error
                    .message
                    .starts_with("connect worker control socket failed: ")
        }
        _ => false,
    }
}

/// Observation type emitted by the embedded core engine.
pub type HubRuntimeObservation = BotsterEngineObservation;

/// Output batch emitted by the embedded core engine.
pub type HubRuntimeOutput = BotsterEngineOutput;

/// Error emitted by the daemon-backed hub runtime.
#[derive(Debug)]
pub enum HubRuntimeError {
    /// Core daemon operation failed.
    CoreDaemon(CoreDaemonError),
    /// Durable hub state failed to load.
    State(HubStateStoreError),
}

impl fmt::Display for HubRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CoreDaemon(error) => write!(formatter, "{error}"),
            Self::State(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for HubRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CoreDaemon(error) => Some(error),
            Self::State(error) => Some(error),
        }
    }
}

impl From<CoreDaemonError> for HubRuntimeError {
    fn from(error: CoreDaemonError) -> Self {
        Self::CoreDaemon(error)
    }
}

impl From<HubStateStoreError> for HubRuntimeError {
    fn from(error: HubStateStoreError) -> Self {
        Self::State(error)
    }
}

/// Hub runtime result alias.
pub type HubRuntimeResult<T> = Result<T, HubRuntimeError>;

fn core_daemon_config(data_directory: &Path) -> CoreDaemonConfig {
    CoreDaemonConfig::new(data_directory).with_worker_path(session_worker_path())
}

fn session_worker_path() -> PathBuf {
    let current = env::current_exe().unwrap_or_else(|_| PathBuf::from("botster-hub"));
    let Some(dir) = current.parent() else {
        return PathBuf::from("botster-session-worker");
    };
    let sibling = dir.join("botster-session-worker");
    if sibling.exists() {
        return sibling;
    }
    if dir.file_name().and_then(|name| name.to_str()) == Some("deps")
        && let Some(debug_dir) = dir.parent()
    {
        let debug_sibling = debug_dir.join("botster-session-worker");
        if debug_sibling.exists() {
            return debug_sibling;
        }
    }
    sibling
}

/// Convert daemon registry state into the client-facing core lifecycle summary.
#[must_use]
pub fn daemon_session_to_core_session(session: DaemonSession) -> CoreSession {
    let lifecycle = match session.registry_state {
        RegistrySessionState::Running => SessionLifecycleState::Running,
        RegistrySessionState::Stopping => SessionLifecycleState::Stopping,
        RegistrySessionState::Exited => SessionLifecycleState::Exited { code: None },
        RegistrySessionState::Stale => SessionLifecycleState::Failed {
            reason: "stale daemon session".to_string(),
        },
    };
    CoreSession::new(session.session_id, lifecycle)
}
