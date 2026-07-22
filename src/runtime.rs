//! Profile-owned runtime facade over the core daemon session supervisor.
//!
//! The first-party host profile owns explicit configuration and admission
//! policy. Session process mechanics, terminal byte routing, activity
//! accounting, guarded-write readiness, and shutdown stay in `botster-core`
//! through `botster-core-daemon`.

use botster_core::{
    BotsterEngineObservation, BotsterEngineOutput, BoundaryJson, ClientId, CoreSession,
    CoreSessionMetadata, EnvelopeId, EnvelopeTarget, ManagedSessionRuntimeError,
    PluginCapabilityRuntime, PluginCleanupResult, PluginHandlerKind, PluginInvocationFailure,
    PluginInvocationFailureKind, PluginInvocationOutcome, PluginInvocationRequest,
    PluginInvocationResult, PluginKey, RequestId, RoutedEnvelope, RoutedEnvelopeDrainOutcome,
    RoutedEnvelopePublishOutcome, SessionId, SessionLifecycleState, SessionRuntimeErrorKind,
    SessionSpawnRequest, SubscriptionId, UiActionResult, UiNode,
};
use botster_core_daemon::{
    AcknowledgeRoutedEnvelopeRequest, CaptureSnapshotRequest, CaptureSnapshotResult, CoreDaemon,
    CoreDaemonConfig, CoreDaemonError, DaemonSession, DrainResult, DrainRoutedEnvelopesRequest,
    GuardedWriteRequest, GuardedWriteResult, PublishRoutedEnvelopeRequest, ReadModeFlagsRequest,
    ReadModeFlagsResult, ReadScreenRequest, ReadScreenResult, RegistrySessionState,
    RoutedEnvelopeDeliveryStateResult, SessionAdoptionReport, SessionAdoptionState,
    SessionLifecycleBaseline, SessionLifecycleChanges, SessionLifecycleCursor, SpawnSessionRequest,
};
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::capabilities::HubCapabilityRuntime;
use crate::config::HubConfig;
use crate::credentials::{
    CredentialPolicyError, CredentialProviderKind, OsKeychainCredentialStore,
    validate_hub_credentials,
};
use crate::lifecycle::{
    HubLifecycleResult, HubPluginLifecycle, HubPluginLifecycleStatus, HubPluginRuntimeBundle,
};
use crate::lua_runtime::{
    HubCoordinationBridge, HubCoordinationResponse, LuaPluginHostApi, LuaPluginRuntime,
    LuaPluginRuntimeError, PendingCoordinationOperation, SharedHubCapabilityRuntime,
};
use crate::packages::{PackageRecord, PackageRegistry, PackageRegistryError, PackageState};
use crate::persistence::{FileHubStateStore, HubState, HubStateStore, HubStateStoreError};
use crate::session_templates::{
    HubSessionContext, SessionTemplateRequest, materialize_session_template,
};
use crate::spawn_targets::SpawnTarget;
use crate::worktrees::Worktree;

/// Hub-owned adapter and policy facade over the default local core engine.
///
/// This facade exposes host-adjacent admission, visibility, runtime-drain, and
/// typed pressure-reporting operations. It intentionally does not expose core's
/// generic `DefaultEngineCommand` router; hub callers use explicit methods so
/// admission and policy boundaries remain visible at the hub layer.
pub struct HubRuntime {
    config: HubConfig,
    state: HubState,
    core_daemon: SharedCoreDaemon,
    reconciliation: HubSessionReconciliation,
    plugin_lifecycle: HubPluginLifecycle,
    capability_runtime: SharedHubCapabilityRuntime,
    spawn_targets: SharedSpawnTargets,
    worktrees: SharedWorktrees,
    session_template_spawner: SharedSessionTemplateSpawner,
    coordination_bridge: HubCoordinationBridge,
    last_capability_cleanup: Option<PluginCleanupResult>,
    session_contexts: SharedSessionContexts,
}

type SharedCoreDaemon = Mutex<CoreDaemon>;
type SharedSessionContexts = Arc<Mutex<BTreeMap<String, HubSessionContext>>>;
const SESSION_TEMPLATE_SPAWN_TIMEOUT_MS: u64 = 30_000;
const PLUGIN_EVENT_TIMEOUT_MS: u64 = 1_000;

/// Shared hub-owned session-template spawn bridge exposed to Lua plugin workers.
pub type SharedSessionTemplateSpawner = Arc<HubSessionTemplateSpawner>;
/// Shared hub-owned spawn-target projection exposed to Lua plugin workers.
pub type SharedSpawnTargets = Arc<Mutex<Vec<SpawnTarget>>>;
/// Shared hub-owned worktree projection exposed to Lua plugin workers.
pub type SharedWorktrees = Arc<Mutex<Vec<Worktree>>>;

/// Hub-owned policy bridge for plugin-safe session-template spawns.
pub struct HubSessionTemplateSpawner {
    pending: Mutex<VecDeque<PendingSessionTemplateSpawn>>,
}

struct PendingSessionTemplateSpawn {
    plugin_key: PluginKey,
    template_id: String,
    request: SessionTemplateRequest,
    package_records: Vec<PackageRecord>,
    response: mpsc::Sender<Result<PluginSessionTemplateSpawned, String>>,
}

/// Structured Lua-facing session-template spawn response.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PluginSessionTemplateSpawned {
    pub session_id: String,
    pub lifecycle: String,
    pub template_id: String,
    pub context_id: String,
    pub context_keys: Vec<String>,
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
        let core_daemon = Mutex::new(CoreDaemon::new(core_config));
        Self {
            capability_runtime: Arc::new(Mutex::new(HubCapabilityRuntime::from_config(&config))),
            spawn_targets: Arc::new(Mutex::new(state.spawn_targets.clone())),
            worktrees: Arc::new(Mutex::new(state.worktrees.clone())),
            session_template_spawner: Arc::new(HubSessionTemplateSpawner::new()),
            coordination_bridge: HubCoordinationBridge::new(),
            config,
            state,
            core_daemon,
            reconciliation: HubSessionReconciliation::default(),
            plugin_lifecycle: HubPluginLifecycle::new(),
            last_capability_cleanup: None,
            session_contexts: Arc::new(Mutex::new(BTreeMap::new())),
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
        validate_hub_credentials(
            &state,
            CredentialProviderKind::OsKeychain,
            &OsKeychainCredentialStore::new(),
        )?;
        Self::from_validated_state(config, state)
    }

    /// Load durable hub state with an explicit credential store.
    ///
    /// Production callers should use [`Self::load_from_store`], which selects
    /// the OS keychain provider. This hook exists for deterministic tests and
    /// tightly controlled embedders that need to exercise provider failures.
    pub fn load_from_store_with_credentials(
        config: HubConfig,
        store: &impl HubStateStore,
        provider_kind: CredentialProviderKind,
        credential_store: &impl botster_core::CredentialStore,
    ) -> HubRuntimeResult<Self> {
        let state = store.load_or_initialize(&config)?;
        validate_hub_credentials(&state, provider_kind, credential_store)?;
        Self::from_validated_state(config, state)
    }

    fn from_validated_state(config: HubConfig, state: HubState) -> HubRuntimeResult<Self> {
        let core_config = core_daemon_config(&config);
        let core_daemon = Mutex::new(CoreDaemon::new(core_config));
        let mut runtime = Self {
            capability_runtime: Arc::new(Mutex::new(HubCapabilityRuntime::from_config(&config))),
            spawn_targets: Arc::new(Mutex::new(state.spawn_targets.clone())),
            worktrees: Arc::new(Mutex::new(state.worktrees.clone())),
            session_template_spawner: Arc::new(HubSessionTemplateSpawner::new()),
            coordination_bridge: HubCoordinationBridge::new(),
            config,
            state,
            core_daemon,
            reconciliation: HubSessionReconciliation::default(),
            plugin_lifecycle: HubPluginLifecycle::new(),
            last_capability_cleanup: None,
            session_contexts: Arc::new(Mutex::new(BTreeMap::new())),
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

    /// Return the CoreDaemon-backed coordination bridge used by Lua helpers.
    #[must_use]
    pub fn coordination_bridge(&self) -> HubCoordinationBridge {
        self.coordination_bridge.clone()
    }

    /// Return the shared session-template spawn bridge used by Lua helpers.
    #[must_use]
    pub fn session_template_spawner(&self) -> SharedSessionTemplateSpawner {
        self.session_template_spawner.clone()
    }

    /// Return the durable hub state loaded for this runtime.
    #[must_use]
    pub const fn state(&self) -> &HubState {
        &self.state
    }

    /// Replace durable hub state after an owner-thread mutation.
    pub fn replace_state(&mut self, state: HubState) {
        if let Ok(mut spawn_targets) = self.spawn_targets.lock() {
            *spawn_targets = state.spawn_targets.clone();
        }
        if let Ok(mut worktrees) = self.worktrees.lock() {
            *worktrees = state.worktrees.clone();
        }
        self.state = state;
    }

    /// Return the shared spawn-target projection used by Lua helpers.
    #[must_use]
    pub fn spawn_targets(&self) -> SharedSpawnTargets {
        self.spawn_targets.clone()
    }

    /// Return the shared worktree projection used by Lua helpers.
    #[must_use]
    pub fn worktrees(&self) -> SharedWorktrees {
        self.worktrees.clone()
    }

    fn lua_plugin_host_api(&self) -> LuaPluginHostApi {
        LuaPluginHostApi {
            capabilities: self.capability_runtime.clone(),
            coordination: self.coordination_bridge(),
            session_templates: self.session_template_spawner.clone(),
            spawn_targets: self.spawn_targets.clone(),
            worktrees: self.worktrees.clone(),
        }
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
            self.lua_plugin_host_api(),
            registry.packages().into_iter().cloned().collect(),
        )
        .map_err(HubLuaPluginLoadError::Lua)?;
        self.load_plugin_package(registry, package_name, bundle)
            .map_err(HubLuaPluginLoadError::Lifecycle)
    }

    /// Re-read and replace an enabled local Lua package through the real Lua runtime.
    pub fn reload_lua_plugin_package(
        &mut self,
        request_id: RequestId,
        registry: &PackageRegistry,
        package_name: &str,
    ) -> Result<PluginCleanupResult, HubLuaPluginLoadError> {
        let prepared = registry
            .prepare_local_package(package_name, "reload local lua plugin package")
            .map_err(HubLuaPluginLoadError::Package)?;
        let configuration = registry
            .package(package_name)
            .map(|record| record.configuration_view())
            .expect("prepared local package must have a registry record");
        let bundle = LuaPluginRuntime::load_prepared(
            &prepared,
            configuration,
            self.lua_plugin_host_api(),
            registry.packages().into_iter().cloned().collect(),
        )
        .map_err(HubLuaPluginLoadError::Lua)?;
        self.reload_plugin_package(request_id, registry, package_name, bundle)
            .map_err(HubLuaPluginLoadError::Lifecycle)
    }

    /// Invoke a plugin handler through core plugin worker mechanics.
    #[must_use]
    pub fn invoke_plugin(&self, request: PluginInvocationRequest) -> PluginInvocationOutcome {
        let request_id = request.request_id.clone();
        let handler = request.handler.clone();
        let timeout_ms = request.timeout_ms;
        let lifecycle = self.plugin_lifecycle.clone();
        let (outcome_sender, outcome_receiver) = mpsc::channel();
        let spawn_result = std::thread::Builder::new()
            .name("botster-plugin-invocation".to_string())
            .spawn(move || {
                let _ = outcome_sender.send(lifecycle.invoke(request));
            });
        let Ok(worker) = spawn_result else {
            return PluginInvocationOutcome {
                result: PluginInvocationResult::Failed(PluginInvocationFailure {
                    request_id,
                    handler,
                    kind: PluginInvocationFailureKind::WorkerStopped,
                    timeout_ms: Some(timeout_ms),
                    reason: "failed to start plugin invocation".to_string(),
                }),
                events: Vec::new(),
            };
        };
        drop(worker);

        loop {
            match outcome_receiver.recv_timeout(Duration::from_millis(1)) {
                Ok(outcome) => {
                    self.fulfill_pending_plugin_requests();
                    break outcome;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    self.fulfill_pending_plugin_requests();
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.fulfill_pending_plugin_requests();
                    break PluginInvocationOutcome {
                        result: PluginInvocationResult::Failed(PluginInvocationFailure {
                            request_id,
                            handler,
                            kind: PluginInvocationFailureKind::WorkerStopped,
                            timeout_ms: Some(timeout_ms),
                            reason: "plugin invocation worker stopped before returning a result"
                                .to_string(),
                        }),
                        events: Vec::new(),
                    };
                }
            }
        }
    }

    /// Emit a hub lifecycle event to matching plugin event handlers.
    #[must_use]
    pub fn emit_plugin_event(
        &self,
        event_name: &str,
        payload: serde_json::Value,
    ) -> Vec<PluginInvocationOutcome> {
        self.plugin_lifecycle
            .event_handlers_for(event_name)
            .into_iter()
            .map(|event_handler| {
                self.invoke_plugin(PluginInvocationRequest {
                    request_id: RequestId(format!(
                        "plugin-event-{}-{}",
                        event_name, event_handler.handler.plugin_key.0
                    )),
                    handler: event_handler.handler,
                    timeout_ms: PLUGIN_EVENT_TIMEOUT_MS,
                    context: botster_core::PluginInvocationContext {
                        client_id: None,
                        session_id: None,
                        subscription_id: None,
                        surface_id: None,
                        origin: Some("hub-worktree-lifecycle".to_string()),
                        metadata: None,
                    },
                    payload: BoundaryJson(payload.clone()),
                })
            })
            .collect()
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
        let request = PluginInvocationRequest {
            request_id: RequestId(format!("mcp-tool-{}", call.name)),
            handler,
            timeout_ms: SESSION_TEMPLATE_SPAWN_TIMEOUT_MS,
            context: botster_core::PluginInvocationContext {
                client_id: None,
                session_id: None,
                subscription_id: None,
                surface_id: None,
                origin: Some("mcp-serve".to_string()),
                metadata: None,
            },
            payload: botster_core::BoundaryJson(call.arguments),
        };
        let outcome = self.invoke_plugin(request);
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

    fn fulfill_pending_session_template_spawns(&self) {
        while let Some(pending) = self.session_template_spawner.take_pending() {
            let result = self.fulfill_session_template_spawn(&pending);
            if pending.response.send(result.clone()).is_err()
                && let Ok(spawned) = result
            {
                self.cleanup_undelivered_session_template_spawn(&spawned);
            }
        }
    }

    fn fulfill_pending_plugin_requests(&self) {
        self.fulfill_pending_coordination_requests();
        self.fulfill_pending_session_template_spawns();
    }

    fn fulfill_pending_coordination_requests(&self) {
        while let Some(pending) = self.coordination_bridge.take_pending() {
            let result = match pending.operation {
                PendingCoordinationOperation::Publish { envelope } => self
                    .core_daemon
                    .lock()
                    .map_err(|_| "core daemon lock poisoned".to_string())
                    .and_then(|mut daemon| {
                        daemon
                            .publish_routed_envelope(PublishRoutedEnvelopeRequest { envelope })
                            .map(HubCoordinationResponse::Publish)
                            .map_err(|error| error.to_string())
                    }),
                PendingCoordinationOperation::Drain {
                    target,
                    after,
                    limit,
                } => self
                    .core_daemon
                    .lock()
                    .map_err(|_| "core daemon lock poisoned".to_string())
                    .and_then(|mut daemon| {
                        daemon
                            .drain_routed_envelopes(DrainRoutedEnvelopesRequest {
                                target,
                                after,
                                limit,
                            })
                            .map(HubCoordinationResponse::Drain)
                            .map_err(|error| error.to_string())
                    }),
                PendingCoordinationOperation::Acknowledge {
                    target,
                    envelope_id,
                } => self
                    .core_daemon
                    .lock()
                    .map_err(|_| "core daemon lock poisoned".to_string())
                    .and_then(|mut daemon| {
                        daemon
                            .acknowledge_routed_envelope(AcknowledgeRoutedEnvelopeRequest {
                                target,
                                envelope_id,
                            })
                            .map(HubCoordinationResponse::Acknowledge)
                            .map_err(|error| error.to_string())
                    }),
            };
            let _ = pending.response.send(result);
        }
    }

    fn cleanup_undelivered_session_template_spawn(&self, spawned: &PluginSessionTemplateSpawned) {
        let session_id = SessionId(spawned.session_id.clone());
        if let Ok(mut daemon) = self.core_daemon.lock() {
            let _ = daemon.shutdown(Some(session_id.clone()), current_unix_seconds());
        }
        if let Ok(mut contexts) = self.session_contexts.lock() {
            contexts.remove(&spawned.context_id);
            contexts.remove(&session_id.0);
        }
    }

    fn fulfill_session_template_spawn(
        &self,
        pending: &PendingSessionTemplateSpawn,
    ) -> Result<PluginSessionTemplateSpawned, String> {
        if !package_allows_session_template_spawn(&pending.package_records, &pending.plugin_key) {
            return Err("plugin package lacks session_template_spawn capability".to_string());
        }

        let records = pending.package_records.iter().collect::<Vec<_>>();
        let materialized = materialize_session_template(
            &self.config,
            &records,
            &self.state,
            &pending.template_id,
            pending.request.clone(),
        )
        .map_err(|error| format!("{}: {}", error.kind, error.message))?;
        let context = materialized.context.clone();
        {
            let mut contexts = self
                .session_contexts
                .lock()
                .map_err(|_| "session context lock poisoned".to_string())?;
            contexts.insert(context.context_id.clone(), context.clone());
            contexts.insert(context.session_id.0.clone(), context.clone());
        }

        let outcome = self
            .core_daemon
            .lock()
            .map_err(|_| "core daemon lock poisoned".to_string())?
            .spawn(
                SpawnSessionRequest {
                    request: materialized.spawn_request,
                    metadata: plugin_session_metadata(&pending.plugin_key),
                },
                current_unix_seconds(),
            )
            .map_err(|error| {
                match self.session_contexts.lock() {
                    Ok(mut contexts) => {
                        contexts.remove(&context.context_id);
                        contexts.remove(&context.session_id.0);
                        format!("session template spawn failed: {error}")
                    }
                    Err(_) => {
                        format!(
                            "session template spawn failed: {error}; session context rollback lock poisoned"
                        )
                    }
                }
            })?;

        Ok(PluginSessionTemplateSpawned {
            session_id: outcome.session_id.0,
            lifecycle: session_lifecycle_label(outcome.lifecycle).to_string(),
            template_id: materialized.resolved.template.template_id,
            context_id: materialized.resolved.context_id,
            context_keys: materialized.resolved.context_keys,
        })
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
            timeout_ms: SESSION_TEMPLATE_SPAWN_TIMEOUT_MS,
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
            timeout_ms: SESSION_TEMPLATE_SPAWN_TIMEOUT_MS,
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
            .lock()
            .expect("core daemon mutex")
            .list()?
            .into_iter()
            .find(|session| &session.session_id == session_id))
    }

    /// Return daemon-recorded sessions for host visibility without exposing core's command router.
    pub fn list_sessions(&self) -> Result<Vec<DaemonSession>, CoreDaemonError> {
        self.core_daemon.lock().expect("core daemon mutex").list()
    }

    /// Return CoreDaemon's authoritative session lifecycle baseline.
    pub fn session_lifecycle_baseline(&self) -> Result<SessionLifecycleBaseline, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .lifecycle_baseline()
    }

    /// Return ordered lifecycle changes after one CoreDaemon cursor.
    #[must_use]
    pub fn session_lifecycle_changes(
        &self,
        after: &SessionLifecycleCursor,
    ) -> SessionLifecycleChanges {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .lifecycle_changes(after)
    }

    /// Forget one terminal session through CoreDaemon's lifecycle authority.
    pub fn remove_terminal_session(
        &mut self,
        session_id: &SessionId,
    ) -> Result<bool, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .remove_session(session_id)
    }

    /// Spawn a daemon-owned session through core from a host-owned request.
    pub fn spawn_session(
        &mut self,
        request: SessionSpawnRequest,
        metadata: CoreSessionMetadata,
        now_seconds: u64,
    ) -> Result<CoreSession, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .spawn(SpawnSessionRequest { request, metadata }, now_seconds)
    }

    /// Store hub-owned context for one spawned template session.
    pub fn record_session_context(&self, context: HubSessionContext) {
        let mut contexts = self
            .session_contexts
            .lock()
            .expect("session contexts mutex");
        contexts.insert(context.context_id.clone(), context.clone());
        contexts.insert(context.session_id.0.clone(), context);
    }

    /// Remove hub-owned context for a template session that did not start.
    pub fn remove_session_context(&self, context: &HubSessionContext) {
        let mut contexts = self
            .session_contexts
            .lock()
            .expect("session contexts mutex");
        contexts.remove(&context.context_id);
        contexts.remove(&context.session_id.0);
    }

    /// Read hub-owned context by context id or session id.
    #[must_use]
    pub fn session_context(&self, id: &str) -> Option<HubSessionContext> {
        self.session_contexts
            .lock()
            .expect("session contexts mutex")
            .get(id)
            .cloned()
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
            .lock()
            .expect("core daemon mutex")
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
        self.core_daemon.lock().expect("core daemon mutex").detach(
            client_id,
            session_id,
            subscription_id,
            now_seconds,
        )
    }

    /// Write terminal bytes into a session through the core daemon.
    pub fn write_bytes(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        data: impl Into<Vec<u8>>,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.core_daemon.lock().expect("core daemon mutex").input(
            client_id,
            session_id,
            data,
            now_seconds,
        )
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
        self.core_daemon.lock().expect("core daemon mutex").resize(
            client_id,
            session_id,
            rows,
            cols,
            now_seconds,
        )
    }

    /// Drain available daemon output through core's subscription path.
    pub fn drain_runtime_once(
        &mut self,
        session_id: &SessionId,
        last_output_at: u64,
    ) -> Result<DrainResult, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .drain(session_id, last_output_at)
    }

    /// Read the current daemon-owned terminal screen through the production core path.
    pub fn read_screen(
        &mut self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> Result<ReadScreenResult, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .read_screen(ReadScreenRequest {
                request_id,
                session_id,
                now_seconds,
            })
    }

    /// Read authoritative terminal mode flags through the production core path.
    pub fn read_mode_flags(
        &mut self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> Result<ReadModeFlagsResult, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .read_mode_flags(ReadModeFlagsRequest {
                request_id,
                session_id,
                now_seconds,
            })
    }

    /// Capture daemon-owned terminal snapshot metadata through the production core path.
    pub fn capture_snapshot(
        &mut self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> Result<CaptureSnapshotResult, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .capture_snapshot(CaptureSnapshotRequest {
                request_id,
                session_id,
                now_seconds,
            })
    }

    /// Evaluate guarded-write readiness and inject only through the core daemon.
    pub fn guarded_write(
        &mut self,
        request: GuardedWriteRequest,
    ) -> Result<GuardedWriteResult, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .guarded_write(request)
    }

    /// Publish one coordination envelope through the CoreDaemon routed-envelope router.
    pub fn publish_routed_envelope(
        &mut self,
        envelope: RoutedEnvelope,
    ) -> Result<RoutedEnvelopePublishOutcome, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .publish_routed_envelope(PublishRoutedEnvelopeRequest { envelope })
    }

    /// Drain coordination envelopes for one routed target through CoreDaemon cursor semantics.
    pub fn drain_routed_envelopes(
        &mut self,
        target: EnvelopeTarget,
        after: Option<botster_core::EnvelopeCursor>,
        limit: usize,
    ) -> Result<RoutedEnvelopeDrainOutcome, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .drain_routed_envelopes(DrainRoutedEnvelopesRequest {
                target,
                after,
                limit,
            })
    }

    /// Acknowledge one routed envelope delivery through CoreDaemon.
    pub fn acknowledge_routed_envelope(
        &mut self,
        target: EnvelopeTarget,
        envelope_id: EnvelopeId,
    ) -> Result<RoutedEnvelopeDeliveryStateResult, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .acknowledge_routed_envelope(AcknowledgeRoutedEnvelopeRequest {
                target,
                envelope_id,
            })
    }

    /// Return one CoreDaemon routed-envelope delivery state without mutation.
    pub fn routed_envelope_delivery_state(
        &self,
        target: &EnvelopeTarget,
        envelope_id: &EnvelopeId,
    ) -> RoutedEnvelopeDeliveryStateResult {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .routed_envelope_delivery_state(target, envelope_id)
    }

    /// Release worker-backed sessions before an intentional daemon restart.
    pub fn release_sessions_for_restart(&mut self) {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .release_for_restart();
    }

    /// Release worker-backed sessions before an intentional hub restart.
    pub fn release_for_restart(&mut self) {
        self.release_sessions_for_restart();
    }

    /// Scan daemon registry records for worker-backed restart/adoption evidence.
    pub fn adoption_scan(&self) -> Result<Vec<SessionAdoptionReport>, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .adoption_scan()
    }

    /// Reattach one live worker-backed session after daemon restart.
    pub fn adopt_session(
        &mut self,
        session_id: &SessionId,
        now_seconds: u64,
    ) -> Result<CoreSession, CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .adopt_session(session_id, now_seconds)
    }

    /// Shut down one daemon-owned session through core.
    pub fn shutdown_session(
        &mut self,
        session_id: SessionId,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .shutdown(Some(session_id), now_seconds)
    }

    fn reconcile_sessions(&mut self, now_seconds: u64) -> Result<(), CoreDaemonError> {
        self.reconciliation = HubSessionReconciliation::default();
        let reports = self
            .core_daemon
            .lock()
            .expect("core daemon mutex")
            .adoption_scan()?;
        for report in reports {
            match report.state {
                SessionAdoptionState::Adoptable => {
                    let adoption_result = {
                        let mut core_daemon = self.core_daemon.lock().expect("core daemon mutex");
                        core_daemon.adopt_session(&report.record.session_id, now_seconds)
                    };
                    match adoption_result {
                        Ok(session) => {
                            self.reconciliation
                                .recovered_sessions
                                .push(session.session_id);
                        }
                        Err(error) if is_stale_worker_control_socket_adoption_error(&error) => {
                            self.core_daemon
                                .lock()
                                .expect("core daemon mutex")
                                .mark_stale(&report.record.session_id, now_seconds)?;
                            self.reconciliation
                                .stale_sessions
                                .push(report.record.session_id);
                        }
                        Err(error) => return Err(error),
                    }
                }
                SessionAdoptionState::InProcessDaemonNotRestartDurable
                // Hub always builds CoreDaemonConfig with a worker path, so this is
                // only reachable for stale records written by an older or invalid embedder.
                | SessionAdoptionState::MissingProtocolEvidence
                | SessionAdoptionState::StaleWorker { .. }
                | SessionAdoptionState::UnhealthyWorker { .. }
                | SessionAdoptionState::DuplicateWorker { .. } => {
                    self.core_daemon
                        .lock()
                        .expect("core daemon mutex")
                        .mark_stale(&report.record.session_id, now_seconds)?;
                    self.reconciliation
                        .stale_sessions
                        .push(report.record.session_id);
                }
                SessionAdoptionState::Terminal => {
                    if report.record.state == RegistrySessionState::Running {
                        self.core_daemon
                            .lock()
                            .expect("core daemon mutex")
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

impl HubSessionTemplateSpawner {
    fn new() -> Self {
        Self {
            pending: Mutex::new(VecDeque::new()),
        }
    }

    /// Queue a session-template spawn for the hub owner and wait for its result.
    pub fn spawn(
        &self,
        plugin_key: &PluginKey,
        template_id: &str,
        request: SessionTemplateRequest,
        package_records: Vec<PackageRecord>,
    ) -> Result<PluginSessionTemplateSpawned, String> {
        let (response, receiver) = mpsc::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| "session-template spawn queue lock poisoned".to_string())?;
            pending.push_back(PendingSessionTemplateSpawn {
                plugin_key: plugin_key.clone(),
                template_id: template_id.to_string(),
                request,
                package_records,
                response,
            });
        }

        receiver
            .recv_timeout(Duration::from_millis(SESSION_TEMPLATE_SPAWN_TIMEOUT_MS))
            .map_err(|_| "session-template spawn did not complete before timeout".to_string())?
    }

    fn take_pending(&self) -> Option<PendingSessionTemplateSpawn> {
        self.pending
            .lock()
            .expect("session-template spawn queue lock")
            .pop_front()
    }
}

fn package_allows_session_template_spawn(
    package_records: &[PackageRecord],
    plugin_key: &PluginKey,
) -> bool {
    package_records.iter().any(|record| {
        record.manifest.name == plugin_key.0
            && matches!(record.state, PackageState::Enabled)
            && record.manifest.capabilities.iter().any(|capability| {
                capability.surface == botster_core::CapabilitySurface::SessionActions
                    && capability.scope.as_deref() == Some("session_template_spawn")
            })
    })
}

fn plugin_session_metadata(plugin_key: &PluginKey) -> CoreSessionMetadata {
    CoreSessionMetadata::from_entries(BTreeMap::from([(
        "client".to_string(),
        format!("plugin:{}", plugin_key.0),
    )]))
}

fn session_lifecycle_label(lifecycle: SessionLifecycleState) -> &'static str {
    match lifecycle {
        SessionLifecycleState::Starting => "starting",
        SessionLifecycleState::Running => "running",
        SessionLifecycleState::Stopping => "stopping",
        SessionLifecycleState::Exited { .. } => "exited",
        SessionLifecycleState::Failed { .. } => "failed",
    }
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
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
    /// Credential provider or persisted credential references failed validation.
    Credentials(CredentialPolicyError),
}

impl fmt::Display for HubRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CoreDaemon(error) => write!(formatter, "{error}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::Credentials(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for HubRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CoreDaemon(error) => Some(error),
            Self::State(error) => Some(error),
            Self::Credentials(error) => Some(error),
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

impl From<CredentialPolicyError> for HubRuntimeError {
    fn from(error: CredentialPolicyError) -> Self {
        Self::Credentials(error)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DataDirectoryOption, HostIdentityOptions, HubStartupOptions, RuntimeEnvironment,
        SessionDefaults, TransportBindings,
    };

    #[test]
    fn hub_core_daemon_config_always_supplies_worker_path() {
        let config = HubStartupOptions {
            host: HostIdentityOptions {
                id: "runtime-test".to_string(),
                display_name: "Runtime Test".to_string(),
                fingerprint: None,
            },
            data_directory: DataDirectoryOption::Explicit(
                "target/botster-hub-test-data/runtime/worker-path-invariant".into(),
            ),
            session_defaults: SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            transports: TransportBindings::default(),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
        .expect("runtime config should build");

        let core_config = core_daemon_config(&config);
        assert!(
            core_config.worker_path.is_some(),
            "hub CoreDaemonConfig must use worker-backed sessions so in-process durability adoption is unreachable"
        );
    }
}
