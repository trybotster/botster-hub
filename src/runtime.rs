//! Profile-owned runtime facade over the daemon-backed `botster-core` engine.
//!
//! The first-party host profile owns explicit configuration and admission
//! policy. Session process mechanics, terminal byte routing, activity
//! accounting, and shutdown stay in `botster-core` through `CoreDaemon` and
//! the configured session-worker executable.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use botster_core::{
    BotsterEngineObservation, BotsterEngineOutput, BotsterSpawnOutcome, ClientId, CoreSession,
    CoreSessionMetadata, EngineSessionInspection, MailboxSendFailureReason,
    PluginCapabilityRuntime, PluginCleanupResult, PluginInvocationOutcome, PluginInvocationRequest,
    PluginKey, PreparedSnapshotRequest, ProcessIdentity, QueueSource, RequestId,
    SessionActivityStatus, SessionId, SessionLifecycleState, SessionRuntimeHandle,
    SessionSpawnRequest, SubscriptionId,
};
use botster_core_daemon::{
    CoreDaemon, CoreDaemonConfig, CoreDaemonError, RegistrySessionState, SessionAdoptionState,
    SpawnSessionRequest,
};

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
    sessions: HashMap<SessionId, CoreSession>,
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
        let mut runtime = Self::from_state(config, state);
        let _ = runtime.reconcile_sessions(0);
        runtime
    }

    fn from_state(config: HubConfig, state: HubState) -> Self {
        let core_daemon = CoreDaemon::new(core_daemon_config(&config));
        Self {
            capability_runtime: HubCapabilityRuntime::from_config(&config),
            config,
            state,
            core_daemon,
            sessions: HashMap::new(),
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
        let mut runtime = Self::from_state(config, state);
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

    /// Release worker processes for an intentional hub restart.
    pub fn release_for_restart(&mut self) {
        self.core_daemon.release_for_restart();
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

    /// Return a recorded core session.
    #[must_use]
    pub fn session(&self, session_id: &SessionId) -> Option<&botster_core::CoreSession> {
        self.sessions.get(session_id)
    }

    /// Return recorded sessions for host visibility without exposing core's command router.
    #[must_use]
    pub fn list_sessions(&self) -> Vec<CoreSession> {
        self.sessions.values().cloned().collect()
    }

    /// Spawn a local PTY-backed session through core from a host-owned request.
    pub fn spawn_session(
        &mut self,
        request: SessionSpawnRequest,
        metadata: CoreSessionMetadata,
    ) -> Result<BotsterSpawnOutcome, HubRuntimeError> {
        let request_id = request.request_id.clone();
        let session = self.core_daemon.spawn(
            SpawnSessionRequest {
                request,
                metadata: metadata.clone(),
            },
            0,
        )?;
        self.sessions
            .insert(session.session_id.clone(), session.clone());
        Ok(BotsterSpawnOutcome {
            handle: session_handle(&session.session_id, request_id),
            session: session.clone(),
            observations: vec![BotsterEngineObservation::SessionLifecycle {
                session_id: session.session_id,
                state: SessionLifecycleState::Running,
            }],
        })
    }

    /// Attach a client subscription to a session through core.
    pub fn attach_client(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        self.core_daemon
            .attach(client_id, session_id, subscription_id, now_seconds)?;
        Ok(BotsterEngineOutput::empty())
    }

    /// Detach a client subscription from a session through core.
    pub fn detach_client(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        self.core_daemon
            .detach(client_id, session_id, subscription_id, now_seconds)?;
        Ok(BotsterEngineOutput::empty())
    }

    /// Write terminal bytes into a session through core.
    pub fn write_bytes(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        data: impl Into<Vec<u8>>,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        self.core_daemon
            .input(client_id, session_id, data, now_seconds)?;
        Ok(BotsterEngineOutput::empty())
    }

    /// Resize a session terminal through the explicit hub facade.
    pub fn resize(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        rows: u16,
        cols: u16,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        self.core_daemon
            .resize(client_id, session_id, rows, cols, now_seconds)?;
        Ok(BotsterEngineOutput::empty())
    }

    /// Drain available local runtime output through core's subscription path.
    pub fn drain_runtime_once(
        &mut self,
        session_id: &SessionId,
        last_output_at: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        let drained = self.core_daemon.drain(session_id, last_output_at)?;
        for observation in &drained.observations {
            if let BotsterEngineObservation::SessionLifecycle { session_id, state } = observation
                && let Some(session) = self.sessions.get_mut(session_id)
            {
                session.lifecycle = state.clone();
            }
        }
        Ok(BotsterEngineOutput {
            client_egress: drained.client_egress,
            session_requests: Vec::new(),
            client_control_frames: Vec::new(),
            session_events: Vec::new(),
            observations: drained.observations,
        })
    }

    /// Drain available local runtime output once for every live session.
    pub fn drain_runtime_all_once(
        &mut self,
        last_output_at: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        let mut output = BotsterEngineOutput::empty();
        let session_ids: Vec<_> = self.sessions.keys().cloned().collect();
        for session_id in session_ids {
            let drained = self.drain_runtime_once(&session_id, last_output_at)?;
            output.client_egress.extend(drained.client_egress);
            output.observations.extend(drained.observations);
        }
        Ok(output)
    }

    /// Classify one session's activity through core.
    pub fn classify_activity(
        &self,
        session_id: &SessionId,
        now_seconds: u64,
        active_threshold_seconds: u64,
    ) -> Result<SessionActivityStatus, HubRuntimeError> {
        let inspection = self.inspect_session(session_id, now_seconds, active_threshold_seconds)?;
        Ok(inspection.activity_status)
    }

    /// Inspect one session's lifecycle and activity through core.
    pub fn inspect_session(
        &self,
        session_id: &SessionId,
        now_seconds: u64,
        active_threshold_seconds: u64,
    ) -> Result<EngineSessionInspection, HubRuntimeError> {
        let session = self
            .sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| HubRuntimeError::UnknownSession(session_id.clone()))?;
        let latest = session.activity.latest_activity_at().unwrap_or(now_seconds);
        let activity_status = if now_seconds.saturating_sub(latest) <= active_threshold_seconds {
            SessionActivityStatus::Active
        } else {
            SessionActivityStatus::Idle
        };
        Ok(EngineSessionInspection {
            session,
            activity_status,
        })
    }

    /// Ask core to read a session screen where the runtime supports it.
    pub fn read_screen(
        &mut self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        let _ = (request_id, session_id, now_seconds);
        Err(HubRuntimeError::UnsupportedDaemonOperation("read_screen"))
    }

    /// Ask core to capture a session snapshot where the runtime supports it.
    pub fn capture_snapshot(
        &mut self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        let _ = (request_id, session_id, now_seconds);
        Err(HubRuntimeError::UnsupportedDaemonOperation(
            "capture_snapshot",
        ))
    }

    /// Ask core to replay or prepare a session snapshot.
    pub fn replay_snapshot(
        &mut self,
        request: PreparedSnapshotRequest,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        let _ = (request, now_seconds);
        Err(HubRuntimeError::UnsupportedDaemonOperation(
            "replay_snapshot",
        ))
    }

    /// Report client-side backpressure as typed core observation data.
    pub fn report_backpressure(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        source: QueueSource,
        capacity: usize,
        depth: usize,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        let _ = (client_id, session_id, source, capacity, depth);
        Err(HubRuntimeError::UnsupportedDaemonOperation(
            "report_backpressure",
        ))
    }

    /// Report accepted-but-slow delivery as typed core observation data.
    pub fn report_delivery_lag(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        source: QueueSource,
        capacity: usize,
        depth: usize,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        let _ = (
            client_id,
            session_id,
            subscription_id,
            source,
            capacity,
            depth,
        );
        Err(HubRuntimeError::UnsupportedDaemonOperation(
            "report_delivery_lag",
        ))
    }

    /// Report a failed delivery attempt as typed core observation data.
    pub fn report_delivery_failure(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        source: QueueSource,
        reason: MailboxSendFailureReason,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        let _ = (client_id, session_id, subscription_id, source, reason);
        Err(HubRuntimeError::UnsupportedDaemonOperation(
            "report_delivery_failure",
        ))
    }

    /// Shut down one local PTY-backed session through core.
    pub fn shutdown_session(
        &mut self,
        session_id: SessionId,
        reason: impl Into<String>,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, HubRuntimeError> {
        let _ = reason.into();
        self.core_daemon
            .shutdown(Some(session_id.clone()), now_seconds)?;
        if let Some(session) = self.sessions.get_mut(&session_id) {
            session.lifecycle = SessionLifecycleState::Stopping;
        }
        Ok(BotsterEngineOutput {
            observations: vec![BotsterEngineObservation::SessionLifecycle {
                session_id,
                state: SessionLifecycleState::Stopping,
            }],
            ..BotsterEngineOutput::empty()
        })
    }

    fn reconcile_sessions(&mut self, now_seconds: u64) -> Result<(), HubRuntimeError> {
        self.reconciliation = HubSessionReconciliation::default();
        for report in self.core_daemon.adoption_scan()? {
            match report.state {
                SessionAdoptionState::Adoptable => {
                    let session = self
                        .core_daemon
                        .adopt_session(&report.record.session_id, now_seconds)?;
                    self.reconciliation
                        .recovered_sessions
                        .push(session.session_id.clone());
                    self.sessions.insert(session.session_id.clone(), session);
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

/// Observation type emitted by the embedded core engine.
pub type HubRuntimeObservation = BotsterEngineObservation;

/// Output batch emitted by one hub runtime operation.
pub type HubRuntimeOutput = BotsterEngineOutput;

/// Spawn result emitted by the embedded core engine.
pub type HubRuntimeSpawnOutcome = BotsterSpawnOutcome;

/// Error emitted by the daemon-backed hub runtime.
#[derive(Debug)]
pub enum HubRuntimeError {
    /// Core daemon operation failed.
    CoreDaemon(CoreDaemonError),
    /// Durable hub state failed to load.
    State(HubStateStoreError),
    /// Session was not present in the daemon-backed hub session index.
    UnknownSession(SessionId),
    /// The current core daemon API does not expose this facade operation yet.
    UnsupportedDaemonOperation(&'static str),
}

impl fmt::Display for HubRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CoreDaemon(error) => write!(formatter, "{error}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::UnknownSession(session_id) => {
                write!(formatter, "unknown session: {session_id:?}")
            }
            Self::UnsupportedDaemonOperation(operation) => {
                write!(formatter, "core daemon does not expose {operation}")
            }
        }
    }
}

impl Error for HubRuntimeError {}

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

fn core_daemon_config(config: &HubConfig) -> CoreDaemonConfig {
    CoreDaemonConfig::new(config.data_directory.join("core-daemon"))
        .with_worker_path(session_worker_path(config))
}

fn session_worker_path(config: &HubConfig) -> PathBuf {
    config
        .core_engine
        .session_worker_path
        .clone()
        .unwrap_or_else(|| {
            std::env::current_exe()
                .ok()
                .and_then(|path| {
                    path.parent().map(|parent| {
                        if parent.file_name().and_then(|name| name.to_str()) == Some("deps") {
                            parent
                                .parent()
                                .unwrap_or(parent)
                                .join("botster-session-worker")
                        } else {
                            parent.join("botster-session-worker")
                        }
                    })
                })
                .unwrap_or_else(|| PathBuf::from("botster-session-worker"))
        })
}

fn session_handle(session_id: &SessionId, request_id: RequestId) -> SessionRuntimeHandle {
    SessionRuntimeHandle {
        request_id,
        session_id: session_id.clone(),
        process: ProcessIdentity {
            pid: None,
            runtime_id: Some(format!("{}-worker", session_id.0)),
        },
    }
}
