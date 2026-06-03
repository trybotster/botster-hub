//! Profile-owned runtime facade over the default `botster-core` engine.
//!
//! The first-party host profile owns explicit configuration and admission
//! policy. Session process mechanics, terminal byte routing, activity
//! accounting, and shutdown stay in `botster-core` through
//! `DefaultBotsterEngine`.

use botster_core::{
    BotsterEngineObservation, BotsterEngineOutput, BotsterSpawnOutcome, ClientId, CoreSession,
    CoreSessionMetadata, DefaultBotsterEngine, DefaultBotsterEngineError, EngineSessionInspection,
    MailboxSendFailureReason, PluginCleanupResult, PluginInvocationOutcome,
    PluginInvocationRequest, PluginKey, PreparedSnapshotRequest, QueueSource, RequestId,
    SessionActivityStatus, SessionId, SessionSpawnRequest, SubscriptionId,
};

use crate::config::HubConfig;
use crate::lifecycle::{HubLifecycleResult, HubPluginLifecycle, HubPluginRuntimeBundle};
use crate::packages::PackageRegistry;

/// Hub-owned adapter and policy facade over the default local core engine.
///
/// This facade exposes host-adjacent admission, visibility, runtime-drain, and
/// typed pressure-reporting operations. It intentionally does not expose core's
/// generic `DefaultEngineCommand` router; hub callers use explicit methods so
/// admission and policy boundaries remain visible at the hub layer.
pub struct HubRuntime {
    config: HubConfig,
    engine: DefaultBotsterEngine,
    plugin_lifecycle: HubPluginLifecycle,
}

impl HubRuntime {
    /// Build a hub runtime from explicit, already-validated hub config.
    #[must_use]
    pub fn new(config: HubConfig) -> Self {
        Self {
            config,
            engine: DefaultBotsterEngine::new(),
            plugin_lifecycle: HubPluginLifecycle::new(),
        }
    }

    /// Return the policy-resolved hub config that created this runtime.
    #[must_use]
    pub const fn config(&self) -> &HubConfig {
        &self.config
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
        self.plugin_lifecycle
            .reload_package(request_id, registry, package_name, bundle)
    }

    /// Unload a plugin package through core plugin worker cleanup mechanics.
    #[must_use]
    pub fn unload_plugin_package(
        &mut self,
        request_id: RequestId,
        package_name: &str,
    ) -> PluginCleanupResult {
        self.plugin_lifecycle
            .unload_package(request_id, package_name)
    }

    /// Return a recorded core session.
    #[must_use]
    pub fn session(&self, session_id: &SessionId) -> Option<&botster_core::CoreSession> {
        self.engine.session(session_id)
    }

    /// Return recorded sessions for host visibility without exposing core's command router.
    #[must_use]
    pub fn list_sessions(&self) -> Vec<CoreSession> {
        self.engine.list_sessions()
    }

    /// Spawn a local PTY-backed session through core from a host-owned request.
    pub fn spawn_session(
        &mut self,
        request: SessionSpawnRequest,
        metadata: CoreSessionMetadata,
    ) -> Result<BotsterSpawnOutcome, DefaultBotsterEngineError> {
        self.engine.spawn_session(request, metadata)
    }

    /// Attach a client subscription to a session through core.
    pub fn attach_client(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine
            .attach_client(client_id, session_id, subscription_id, now_seconds)
    }

    /// Write terminal bytes into a session through core.
    pub fn write_bytes(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        data: impl Into<Vec<u8>>,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine
            .write_bytes(client_id, session_id, data, now_seconds)
    }

    /// Drain available local runtime output through core's subscription path.
    pub fn drain_runtime_once(
        &mut self,
        session_id: &SessionId,
        last_output_at: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine.drain_runtime_once(session_id, last_output_at)
    }

    /// Drain available local runtime output once for every live session.
    pub fn drain_runtime_all_once(
        &mut self,
        last_output_at: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine.drain_runtime_all_once(last_output_at)
    }

    /// Classify one session's activity through core.
    pub fn classify_activity(
        &self,
        session_id: &SessionId,
        now_seconds: u64,
        active_threshold_seconds: u64,
    ) -> Result<SessionActivityStatus, DefaultBotsterEngineError> {
        self.engine
            .classify_activity(session_id, now_seconds, active_threshold_seconds)
    }

    /// Inspect one session's lifecycle and activity through core.
    pub fn inspect_session(
        &self,
        session_id: &SessionId,
        now_seconds: u64,
        active_threshold_seconds: u64,
    ) -> Result<EngineSessionInspection, DefaultBotsterEngineError> {
        self.engine
            .inspect_session(session_id, now_seconds, active_threshold_seconds)
    }

    /// Ask core to read a session screen where the runtime supports it.
    pub fn read_screen(
        &mut self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine.read_screen(request_id, session_id, now_seconds)
    }

    /// Ask core to capture a session snapshot where the runtime supports it.
    pub fn capture_snapshot(
        &mut self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine
            .capture_snapshot(request_id, session_id, now_seconds)
    }

    /// Ask core to replay or prepare a session snapshot.
    pub fn replay_snapshot(
        &mut self,
        request: PreparedSnapshotRequest,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine.replay_snapshot(request, now_seconds)
    }

    /// Report client-side backpressure as typed core observation data.
    pub fn report_backpressure(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        source: QueueSource,
        capacity: usize,
        depth: usize,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine
            .report_backpressure(client_id, session_id, source, capacity, depth)
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
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine.report_delivery_lag(
            client_id,
            session_id,
            subscription_id,
            source,
            capacity,
            depth,
        )
    }

    /// Report a failed delivery attempt as typed core observation data.
    pub fn report_delivery_failure(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        source: QueueSource,
        reason: MailboxSendFailureReason,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine
            .report_delivery_failure(client_id, session_id, subscription_id, source, reason)
    }

    /// Shut down one local PTY-backed session through core.
    pub fn shutdown_session(
        &mut self,
        session_id: SessionId,
        reason: impl Into<String>,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine
            .shutdown_session(session_id, reason, now_seconds)
    }
}

/// Observation type emitted by the embedded core engine.
pub type HubRuntimeObservation = BotsterEngineObservation;

/// Output batch emitted by one hub runtime operation.
pub type HubRuntimeOutput = BotsterEngineOutput;

/// Spawn result emitted by the embedded core engine.
pub type HubRuntimeSpawnOutcome = BotsterSpawnOutcome;

/// Error emitted by the embedded default local core engine.
pub type HubRuntimeError = DefaultBotsterEngineError;
