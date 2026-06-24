//! Profile-owned runtime facade over the core daemon session supervisor.
//!
//! The first-party host profile owns explicit configuration and admission
//! policy. Session process mechanics, terminal byte routing, activity
//! accounting, guarded-write readiness, and shutdown stay in `botster-core`
//! through `botster-core-daemon`.

use botster_core::{
    BotsterEngineObservation, BotsterEngineOutput, BoundaryJson, ClientId, CoreSession,
    CoreSessionMetadata, EnvelopeId, EnvelopeTarget, ManagedSessionRuntimeError,
    PluginCapabilityRuntime, PluginCleanupResult, PluginHandlerKind, PluginInvocationOutcome,
    PluginInvocationRequest, PluginInvocationResult, PluginKey, RequestId, RoutedEnvelope,
    RoutedEnvelopeDrainOutcome, RoutedEnvelopePublishOutcome, RoutedEnvelopeRouter, SessionId,
    SessionLifecycleState, SessionRuntimeErrorKind, SessionSpawnRequest, SubscriptionId,
    UiActionResult, UiNode,
};
use botster_core_daemon::{
    CoreDaemon, CoreDaemonConfig, CoreDaemonError, DaemonSession, DrainResult, GuardedWriteRequest,
    GuardedWriteResult, RegistrySessionState, RoutedEnvelopeDeliveryStateResult,
    SessionAdoptionReport, SessionAdoptionState, SpawnSessionRequest,
};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::capabilities::HubCapabilityRuntime;
use crate::config::HubConfig;
use crate::lifecycle::{
    HubLifecycleResult, HubPluginLifecycle, HubPluginLifecycleStatus, HubPluginRuntimeBundle,
};
use crate::lua_runtime::{
    LuaPluginRuntime, LuaPluginRuntimeError, SharedHubCapabilityRuntime,
    SharedRoutedEnvelopeRuntime,
};
use crate::packages::{PackageRegistry, PackageRegistryError};
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
    capability_runtime: SharedHubCapabilityRuntime,
    // HubRuntime owns coordination routing so native MCP tools and Lua plugin
    // helpers share one route table from the plugin invocation path.
    routed_envelopes: SharedRoutedEnvelopeRuntime,
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
        let core_config = core_daemon_config(&config);
        let routed_envelopes = Arc::new(Mutex::new(RoutedEnvelopeRouter::with_config(
            core_config.routed_envelope_queue.clone(),
        )));
        let core_daemon = CoreDaemon::new(core_config);
        Self {
            capability_runtime: Arc::new(Mutex::new(HubCapabilityRuntime::from_config(&config))),
            routed_envelopes,
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
        let core_config = core_daemon_config(&config);
        let routed_envelopes = Arc::new(Mutex::new(RoutedEnvelopeRouter::with_config(
            core_config.routed_envelope_queue.clone(),
        )));
        let core_daemon = CoreDaemon::new(core_config);
        let mut runtime = Self {
            capability_runtime: Arc::new(Mutex::new(HubCapabilityRuntime::from_config(&config))),
            routed_envelopes,
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
    pub fn capability_runtime(&self) -> SharedHubCapabilityRuntime {
        self.capability_runtime.clone()
    }

    /// Return the shared routed-envelope primitive used by Lua coordination helpers.
    ///
    /// The hub owns this router for now because Lua plugin handlers are invoked
    /// through `&self`; native MCP coordination commands and Lua helpers must
    /// still observe the same route table.
    #[must_use]
    pub fn routed_envelope_runtime(&self) -> SharedRoutedEnvelopeRuntime {
        self.routed_envelopes.clone()
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

    /// Prepare and load an enabled local Lua package through the real Lua runtime.
    pub fn load_lua_plugin_package(
        &mut self,
        registry: &PackageRegistry,
        package_name: &str,
    ) -> Result<PluginKey, HubLuaPluginLoadError> {
        let prepared = registry
            .prepare_local_package(package_name, "load local lua plugin package")
            .map_err(HubLuaPluginLoadError::Package)?;
        let configuration = registry
            .package(package_name)
            .map(|record| record.configuration_view())
            .expect("prepared local package must have a registry record");
        let bundle = LuaPluginRuntime::load_prepared(
            &prepared,
            configuration,
            self.capability_runtime.clone(),
            self.routed_envelopes.clone(),
        )
        .map_err(HubLuaPluginLoadError::Lua)?;
        self.load_plugin_package(registry, package_name, bundle)
            .map_err(HubLuaPluginLoadError::Lifecycle)
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
        self.capability_runtime
            .lock()
            .expect("hub capability runtime lock")
            .submit(request)
    }

    /// Cancel one plugin-owned capability operation.
    pub fn cancel_capability_operation(
        &mut self,
        plugin_key: &PluginKey,
        operation_id: &botster_core::CapabilityOperationId,
    ) -> Result<(), botster_core::CapabilityRuntimeError> {
        self.capability_runtime
            .lock()
            .expect("hub capability runtime lock")
            .cancel(plugin_key, operation_id)
    }

    /// Release one plugin-owned capability resource.
    pub fn release_capability_resource(
        &mut self,
        resource: botster_core::PluginResourceRef,
    ) -> Result<(), botster_core::CapabilityRuntimeError> {
        self.capability_runtime
            .lock()
            .expect("hub capability runtime lock")
            .release_resource(resource)
    }

    /// Drain currently available capability events for one plugin.
    pub fn drain_capability_events(
        &mut self,
        plugin_key: &PluginKey,
    ) -> Result<Vec<botster_core::CapabilityRuntimeEvent>, botster_core::CapabilityRuntimeError>
    {
        self.capability_runtime
            .lock()
            .expect("hub capability runtime lock")
            .drain_events(plugin_key)
    }

    /// Drain capability events after advancing the local logical timer clock.
    pub fn drain_capability_events_at(
        &mut self,
        plugin_key: &PluginKey,
        now_ms: u64,
    ) -> Result<Vec<botster_core::CapabilityRuntimeEvent>, botster_core::CapabilityRuntimeError>
    {
        self.capability_runtime
            .lock()
            .expect("hub capability runtime lock")
            .drain_events_at(plugin_key, now_ms)
    }

    /// Stop all capability runtime resources owned by one plugin.
    pub fn cleanup_plugin_capabilities(
        &mut self,
        plugin_key: &PluginKey,
    ) -> Result<PluginCleanupResult, botster_core::CapabilityRuntimeError> {
        let cleanup = self
            .capability_runtime
            .lock()
            .expect("hub capability runtime lock")
            .cleanup_plugin(plugin_key)?;
        self.last_capability_cleanup = Some(cleanup.clone());
        Ok(cleanup)
    }

    /// Return loaded plugin MCP tool descriptors.
    #[must_use]
    pub fn list_plugin_mcp_tools(&self) -> Vec<crate::McpToolDescriptor> {
        self.plugin_lifecycle
            .mcp_tool_descriptors()
            .into_iter()
            .filter_map(crate::mcp::mcp_descriptor_from_plugin)
            .collect()
    }

    /// Invoke a loaded plugin MCP tool through the core worker path.
    pub fn call_plugin_mcp_tool(
        &self,
        call: crate::McpCallRequest,
    ) -> Result<serde_json::Value, crate::McpToolError> {
        let descriptor = self
            .plugin_lifecycle
            .mcp_tool_descriptors()
            .into_iter()
            .find(|descriptor| {
                descriptor
                    .body
                    .0
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    == Some(call.name.as_str())
            })
            .ok_or_else(|| {
                crate::McpToolError::new(
                    "unknown_tool",
                    format!("unknown plugin MCP tool: {}", call.name),
                )
            })?;
        let handler = descriptor.handler.ok_or_else(|| {
            crate::McpToolError::new("plugin_tool_unavailable", "plugin MCP tool has no handler")
        })?;
        let outcome = self.invoke_plugin(PluginInvocationRequest {
            request_id: RequestId(format!("mcp-tool-{}", call.name)),
            handler,
            timeout_ms: 1_000,
            context: botster_core::PluginInvocationContext {
                client_id: None,
                session_id: None,
                subscription_id: None,
                surface_id: None,
                origin: Some("mcp-serve".to_string()),
                metadata: None,
            },
            payload: botster_core::BoundaryJson(call.arguments),
        });
        match outcome.result {
            botster_core::PluginInvocationResult::Completed(success) => {
                Ok(success.payload.map_or_else(json_null, |payload| payload.0))
            }
            botster_core::PluginInvocationResult::Failed(failure) => Err(crate::McpToolError::new(
                "plugin_tool_failed",
                failure.reason,
            )),
        }
    }

    /// Render a plugin-owned surface route through the plugin worker path.
    pub fn render_plugin_surface(
        &self,
        package_name: &str,
        surface_id: &str,
        payload: serde_json::Value,
    ) -> Result<UiNode, crate::McpToolError> {
        let descriptor = self
            .plugin_lifecycle
            .surface_route_descriptors()
            .into_iter()
            .find(|descriptor| {
                descriptor.descriptor.plugin_key.0 == package_name
                    && descriptor.descriptor.descriptor_id == surface_id
            })
            .ok_or_else(|| {
                crate::McpToolError::new(
                    "unknown_surface",
                    format!("unknown plugin surface: {package_name}/{surface_id}"),
                )
            })?;
        let handler = descriptor.handler.ok_or_else(|| {
            crate::McpToolError::new("surface_unavailable", "plugin surface has no handler")
        })?;
        let request_id = RequestId(format!("plugin-surface-render-{package_name}-{surface_id}"));
        let outcome = self.invoke_plugin(PluginInvocationRequest {
            request_id,
            handler,
            timeout_ms: 1_000,
            context: botster_core::PluginInvocationContext {
                client_id: None,
                session_id: None,
                subscription_id: None,
                surface_id: Some(surface_id.to_string()),
                origin: Some("local-client-api".to_string()),
                metadata: None,
            },
            payload: BoundaryJson(payload),
        });
        let value = completed_plugin_payload(outcome.result, "plugin surface render")?;
        let node: UiNode = serde_json::from_value(value).map_err(|error| {
            crate::McpToolError::new("invalid_surface", format!("invalid plugin UiNode: {error}"))
        })?;
        node.validate().map_err(|error| {
            crate::McpToolError::new("invalid_surface", format!("invalid plugin UiNode: {error}"))
        })?;
        Ok(node)
    }

    /// Dispatch a plugin-owned semantic UI action through the plugin worker path.
    pub fn dispatch_plugin_surface_action(
        &self,
        package_name: &str,
        surface_id: &str,
        action_id: &str,
        payload: serde_json::Value,
    ) -> Result<UiActionResult, crate::McpToolError> {
        let descriptor = self
            .plugin_lifecycle
            .ui_action_descriptors()
            .into_iter()
            .find(|descriptor| {
                descriptor.descriptor.plugin_key.0 == package_name
                    && descriptor.descriptor.descriptor_id == action_id
            })
            .ok_or_else(|| {
                crate::McpToolError::new(
                    "unknown_action",
                    format!("unknown plugin UI action: {package_name}/{action_id}"),
                )
            })?;
        let handler = descriptor.handler.ok_or_else(|| {
            crate::McpToolError::new("action_unavailable", "plugin UI action has no handler")
        })?;
        let outcome = self.invoke_plugin(PluginInvocationRequest {
            request_id: RequestId(format!(
                "plugin-surface-action-{package_name}-{surface_id}-{action_id}"
            )),
            handler: botster_core::PluginHandlerRef {
                kind: PluginHandlerKind::UiAction,
                ..handler
            },
            timeout_ms: 1_000,
            context: botster_core::PluginInvocationContext {
                client_id: None,
                session_id: None,
                subscription_id: None,
                surface_id: Some(surface_id.to_string()),
                origin: Some("local-client-api".to_string()),
                metadata: None,
            },
            payload: BoundaryJson(payload),
        });
        let value = completed_plugin_payload(outcome.result, "plugin surface action")?;
        serde_json::from_value(value).map_err(|error| {
            crate::McpToolError::new(
                "invalid_action_result",
                format!("invalid plugin UiActionResult: {error}"),
            )
        })
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

    /// Publish one coordination envelope through the hub-owned routed-envelope router.
    pub fn publish_routed_envelope(
        &mut self,
        envelope: RoutedEnvelope,
    ) -> Result<RoutedEnvelopePublishOutcome, CoreDaemonError> {
        Ok(self
            .routed_envelopes
            .lock()
            .expect("routed envelope runtime lock")
            .publish(envelope))
    }

    /// Drain coordination envelopes for one routed target through hub cursor semantics.
    pub fn drain_routed_envelopes(
        &mut self,
        target: EnvelopeTarget,
        after: Option<botster_core::EnvelopeCursor>,
        limit: usize,
    ) -> Result<RoutedEnvelopeDrainOutcome, CoreDaemonError> {
        Ok(self
            .routed_envelopes
            .lock()
            .expect("routed envelope runtime lock")
            .drain(&target, after, limit))
    }

    /// Acknowledge one routed envelope delivery through the hub-owned router.
    pub fn acknowledge_routed_envelope(
        &mut self,
        target: EnvelopeTarget,
        envelope_id: EnvelopeId,
    ) -> Result<RoutedEnvelopeDeliveryStateResult, CoreDaemonError> {
        Ok(RoutedEnvelopeDeliveryStateResult {
            state: self
                .routed_envelopes
                .lock()
                .expect("routed envelope runtime lock")
                .acknowledge(&target, &envelope_id),
        })
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

fn completed_plugin_payload(
    result: PluginInvocationResult,
    operation: &str,
) -> Result<serde_json::Value, crate::McpToolError> {
    match result {
        PluginInvocationResult::Completed(success) => Ok(success
            .payload
            .map_or_else(|| serde_json::Value::Null, |payload| payload.0)),
        PluginInvocationResult::Failed(failure) => Err(crate::McpToolError::new(
            "plugin_invocation_failed",
            format!("{operation} failed: {}", failure.reason),
        )),
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

/// Error emitted while preparing and loading a real Lua plugin package.
#[derive(Debug)]
pub enum HubLuaPluginLoadError {
    Package(PackageRegistryError),
    Lua(LuaPluginRuntimeError),
    Lifecycle(crate::HubLifecycleError),
}

impl fmt::Display for HubLuaPluginLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Package(error) => write!(formatter, "{error:?}"),
            Self::Lua(error) => write!(formatter, "{error}"),
            Self::Lifecycle(error) => write!(formatter, "{error:?}"),
        }
    }
}

impl Error for HubLuaPluginLoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Package(_) => None,
            Self::Lua(error) => Some(error),
            Self::Lifecycle(_) => None,
        }
    }
}

fn json_null() -> serde_json::Value {
    serde_json::Value::Null
}

fn core_daemon_config(config: &HubConfig) -> CoreDaemonConfig {
    CoreDaemonConfig::new(&config.data_directory).with_worker_path(session_worker_path(config))
}

fn session_worker_path(config: &HubConfig) -> PathBuf {
    if let Some(path) = &config.core_engine.session_worker_path {
        return path.clone();
    }

    let current = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("botster-hub"));
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
