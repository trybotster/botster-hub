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
    SessionSpawnRequest, SubscriptionId,
};
use botster_core_daemon::{
    AcknowledgeRoutedEnvelopeRequest, CaptureSnapshotRequest, CaptureSnapshotResult, CoreDaemon,
    CoreDaemonConfig, CoreDaemonError, DaemonSession, DrainResult, DrainRoutedEnvelopesRequest,
    GuardedWriteRequest, GuardedWriteResult, PublishRoutedEnvelopeRequest, ReadModeFlagsRequest,
    ReadModeFlagsResult, ReadScreenRequest, ReadScreenResult, RegistrySessionState,
    RoutedEnvelopeDeliveryStateResult, SessionAdoptionReport, SessionAdoptionState,
    SessionLifecycleBaseline, SessionLifecycleChanges, SessionLifecycleCursor, SpawnSessionRequest,
};
use botster_ui_contract::{UiActionRequest, UiActionResult, UiNode};
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
use crate::managed_git_worktrees::{
    MANAGED_GIT_OPERATION_TIMEOUT, ManagedGitError, ManagedGitRequest, PreparedManagedWorktree,
    adopt_unrecorded_managed_worktrees, managed_worktree_id, prepare_managed_worktree,
    rollback_prepared_worktree,
};
use crate::packages::{PackageRecord, PackageRegistry, PackageRegistryError, PackageState};
use crate::persistence::{FileHubStateStore, HubState, HubStateStore, HubStateStoreError};
use crate::session_templates::{
    EnsuredManagedWorktree, HubSessionContext, HubSessionTemplate, ManagedSessionTemplateRequest,
    SessionTemplateRequest, list_session_templates_for_target,
    materialize_managed_session_template, materialize_session_template,
    show_session_template_for_target,
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
    // Managed-session fulfillment needs interior mutability while the plugin
    // owner loop holds `&self`. This state remains owner-thread policy: never
    // hold a read guard across `replace_state`, which takes the write guard.
    state: RwLock<HubState>,
    core_daemon: SharedCoreDaemon,
    reconciliation: HubSessionReconciliation,
    plugin_lifecycle: HubPluginLifecycle,
    capability_runtime: SharedHubCapabilityRuntime,
    spawn_targets: SharedSpawnTargets,
    worktrees: SharedWorktrees,
    session_template_spawner: SharedSessionTemplateSpawner,
    managed_git_coordinator: ManagedGitCoordinator,
    managed_git_operations: Mutex<Vec<PendingManagedGitOperation>>,
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
    reads: Mutex<VecDeque<PendingSessionTemplateRead>>,
    managed: Mutex<VecDeque<PendingManagedSessionSpawn>>,
}

struct PendingSessionTemplateSpawn {
    plugin_key: PluginKey,
    template_id: String,
    request: SessionTemplateRequest,
    package_records: Vec<PackageRecord>,
    response: mpsc::Sender<Result<PluginSessionTemplateSpawned, String>>,
}

enum SessionTemplateRead {
    List,
    Show { template_id: String },
}

struct PendingSessionTemplateRead {
    target_id: String,
    operation: SessionTemplateRead,
    package_records: Vec<PackageRecord>,
    response: mpsc::Sender<Result<Vec<HubSessionTemplate>, String>>,
}

struct PendingManagedSessionSpawn {
    plugin_key: PluginKey,
    target_id: String,
    branch: String,
    template_id: String,
    request: ManagedSessionTemplateRequest,
    package_records: Vec<PackageRecord>,
    accepted_at: Instant,
    response: mpsc::Sender<Result<PluginManagedSessionSpawned, ManagedGitError>>,
}

struct ManagedGitWorkerJob {
    request: ManagedGitRequest,
    prepared: mpsc::Sender<Result<PreparedManagedWorktree, ManagedGitError>>,
    decision: mpsc::Receiver<ManagedGitDecision>,
    finalized: mpsc::Sender<Result<(), ManagedGitError>>,
}

enum ManagedGitDecision {
    Commit,
    Rollback,
    Preserve,
}

struct ManagedGitCoordinator {
    sender: mpsc::SyncSender<ManagedGitWorkerJob>,
}

type ManagedGitSubmission = (
    mpsc::Receiver<Result<PreparedManagedWorktree, ManagedGitError>>,
    mpsc::Sender<ManagedGitDecision>,
    mpsc::Receiver<Result<(), ManagedGitError>>,
);

enum ManagedGitOwnerPhase {
    Preparing,
    Finalizing,
}

struct PendingManagedGitOperation {
    pending: PendingManagedSessionSpawn,
    prepared_receiver: mpsc::Receiver<Result<PreparedManagedWorktree, ManagedGitError>>,
    decision: mpsc::Sender<ManagedGitDecision>,
    finalized_receiver: mpsc::Receiver<Result<(), ManagedGitError>>,
    prepared: Option<PreparedManagedWorktree>,
    phase: ManagedGitOwnerPhase,
    deferred_error: Option<ManagedGitError>,
    response_delivered: bool,
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

/// Tagged Lua-facing result for the atomic managed-worktree/session operation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PluginManagedSessionSpawned {
    pub session_id: String,
    pub target_id: String,
    pub branch: String,
    pub worktree_id: String,
    pub worktree_path: String,
    pub base_ref: String,
    pub base_commit: String,
    pub created_worktree: bool,
    pub created_branch: bool,
    pub reused_worktree: bool,
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
            managed_git_coordinator: ManagedGitCoordinator::new(),
            managed_git_operations: Mutex::new(Vec::new()),
            coordination_bridge: HubCoordinationBridge::new(),
            config,
            state: RwLock::new(state),
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
        let mut state = store.load_or_initialize(&config)?;
        if adopt_unrecorded_managed_worktrees(
            &state.spawn_targets,
            &mut state.worktrees,
            &managed_worktree_root(&config),
        ) {
            store.save(&state)?;
        }
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
        let mut state = store.load_or_initialize(&config)?;
        if adopt_unrecorded_managed_worktrees(
            &state.spawn_targets,
            &mut state.worktrees,
            &managed_worktree_root(&config),
        ) {
            store.save(&state)?;
        }
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
            managed_git_coordinator: ManagedGitCoordinator::new(),
            managed_git_operations: Mutex::new(Vec::new()),
            coordination_bridge: HubCoordinationBridge::new(),
            config,
            state: RwLock::new(state),
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
    pub fn state(&self) -> std::sync::RwLockReadGuard<'_, HubState> {
        self.state.read().expect("hub state lock")
    }

    /// Replace durable hub state after an owner-thread mutation.
    pub fn replace_state(&self, state: HubState) {
        if let Ok(mut spawn_targets) = self.spawn_targets.lock() {
            *spawn_targets = state.spawn_targets.clone();
        }
        if let Ok(mut worktrees) = self.worktrees.lock() {
            *worktrees = state.worktrees.clone();
        }
        *self.state.write().expect("hub state lock") = state;
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

    fn fulfill_pending_session_template_reads(&self) {
        while let Some(pending) = self.session_template_spawner.take_read() {
            let records = pending.package_records.iter().collect::<Vec<_>>();
            let state = self.state();
            let result = match pending.operation {
                SessionTemplateRead::List => {
                    list_session_templates_for_target(&records, &state, &pending.target_id)
                }
                SessionTemplateRead::Show { template_id } => show_session_template_for_target(
                    &records,
                    &state,
                    &pending.target_id,
                    &template_id,
                )
                .map(|template| vec![template]),
            }
            .map_err(|error| format!("{}: {}", error.kind, error.message));
            let _ = pending.response.send(result);
        }
    }

    fn accept_pending_managed_git_operations(&self) {
        while let Some(pending) = self.session_template_spawner.take_managed() {
            let validation = self.validate_managed_git_request(&pending);
            let request = match validation {
                Ok(request) => request,
                Err(error) => {
                    let _ = pending.response.send(Err(error));
                    continue;
                }
            };
            match self.managed_git_coordinator.submit(request) {
                Ok((prepared_receiver, decision, finalized_receiver)) => {
                    if let Ok(mut operations) = self.managed_git_operations.lock() {
                        operations.push(PendingManagedGitOperation {
                            pending,
                            prepared_receiver,
                            decision,
                            finalized_receiver,
                            prepared: None,
                            phase: ManagedGitOwnerPhase::Preparing,
                            deferred_error: None,
                            response_delivered: false,
                        });
                    } else {
                        let _ = pending.response.send(Err(ManagedGitError::new(
                            "ensure_unavailable",
                            "managed Git owner state is unavailable",
                        )));
                    }
                }
                Err(error) => {
                    let _ = pending.response.send(Err(error));
                }
            }
        }
    }

    fn validate_managed_git_request(
        &self,
        pending: &PendingManagedSessionSpawn,
    ) -> Result<ManagedGitRequest, ManagedGitError> {
        if !package_allows_managed_git_spawn(&pending.package_records, &pending.plugin_key) {
            return Err(ManagedGitError::new(
                "capability_denied",
                "plugin package lacks managed session-template spawn capability",
            ));
        }
        let state = self.state();
        let target = state
            .spawn_targets
            .iter()
            .find(|target| target.target_id == pending.target_id)
            .cloned()
            .ok_or_else(|| {
                ManagedGitError::new("target_not_found", "spawn target was not found")
            })?;
        let records = pending.package_records.iter().collect::<Vec<_>>();
        show_session_template_for_target(
            &records,
            &state,
            &pending.target_id,
            &pending.template_id,
        )
        .map_err(|error| ManagedGitError::new(error.kind, error.message))?;
        let worktree_id = managed_worktree_id(&pending.target_id, &pending.branch);
        let persisted_worktree = state
            .worktrees
            .iter()
            .find(|worktree| worktree.worktree_id == worktree_id)
            .cloned();
        Ok(ManagedGitRequest {
            target,
            branch: pending.branch.clone(),
            managed_root: managed_worktree_root(&self.config),
            persisted_worktree,
            accepted_at: pending.accepted_at,
        })
    }

    fn advance_managed_git_operations(&self) {
        let mut operations = match self.managed_git_operations.lock() {
            Ok(operations) => operations,
            Err(_) => return,
        };
        let mut retained = Vec::with_capacity(operations.len());
        for mut operation in operations.drain(..) {
            let complete = match operation.phase {
                ManagedGitOwnerPhase::Preparing => {
                    self.advance_preparing_managed_operation(&mut operation)
                }
                ManagedGitOwnerPhase::Finalizing => {
                    self.advance_finalizing_managed_operation(&mut operation)
                }
            };
            if !complete {
                retained.push(operation);
            }
        }
        *operations = retained;
    }

    fn advance_preparing_managed_operation(
        &self,
        operation: &mut PendingManagedGitOperation,
    ) -> bool {
        let prepared = match operation.prepared_receiver.try_recv() {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(error)) => {
                let _ = operation.pending.response.send(Err(error));
                return true;
            }
            Err(mpsc::TryRecvError::Empty) => return false,
            Err(mpsc::TryRecvError::Disconnected) => {
                let _ = operation.pending.response.send(Err(ManagedGitError::new(
                    "ensure_unavailable",
                    "managed Git worker stopped before preparing the worktree",
                )));
                return true;
            }
        };
        operation.prepared = Some(prepared.clone());
        if Instant::now() >= operation.pending.accepted_at + MANAGED_GIT_OPERATION_TIMEOUT {
            operation.deferred_error = Some(ManagedGitError::new(
                "reconciliation_required",
                "managed Git owner deadline elapsed; prepared resources were preserved",
            ));
            let _ = operation.decision.send(ManagedGitDecision::Preserve);
            operation.phase = ManagedGitOwnerPhase::Finalizing;
            return false;
        }
        let result = self
            .persist_managed_worktree(&prepared)
            .and_then(|()| self.spawn_prepared_managed_session(&operation.pending, &prepared));
        match result {
            Ok(spawned) => {
                if Instant::now() >= operation.pending.accepted_at + MANAGED_GIT_OPERATION_TIMEOUT {
                    self.cleanup_managed_session(&spawned);
                    operation.deferred_error = Some(ManagedGitError::new(
                        "reconciliation_required",
                        "managed Git owner deadline elapsed; prepared resources were preserved",
                    ));
                    let _ = operation.decision.send(ManagedGitDecision::Preserve);
                    operation.phase = ManagedGitOwnerPhase::Finalizing;
                    return false;
                }
                operation.response_delivered =
                    operation.pending.response.send(Ok(spawned.clone())).is_ok();
                if operation.response_delivered {
                    let _ = operation.decision.send(ManagedGitDecision::Commit);
                    // The worker owns lane release. Once success is delivered
                    // and commit is decided, owner bookkeeping is complete.
                    return true;
                } else {
                    self.cleanup_managed_session(&spawned);
                    let _ = operation.decision.send(ManagedGitDecision::Rollback);
                }
            }
            Err(error) => {
                operation.deferred_error = Some(error);
                let _ = operation.decision.send(ManagedGitDecision::Rollback);
            }
        }
        operation.phase = ManagedGitOwnerPhase::Finalizing;
        false
    }

    fn advance_finalizing_managed_operation(
        &self,
        operation: &mut PendingManagedGitOperation,
    ) -> bool {
        let finalized = match operation.finalized_receiver.try_recv() {
            Ok(result) => result,
            Err(mpsc::TryRecvError::Empty) => return false,
            Err(mpsc::TryRecvError::Disconnected) => Err(ManagedGitError::new(
                "reconciliation_failed",
                "managed Git worker stopped before reconciliation completed",
            )),
        };
        let prepared = operation
            .prepared
            .as_ref()
            .expect("finalizing managed operation has prepared worktree");
        if !operation.response_delivered {
            match finalized {
                Ok(()) => {
                    if prepared.created_worktree {
                        let _ = self.remove_managed_worktree_record(&prepared.worktree_id);
                    }
                    if let Some(error) = operation.deferred_error.take() {
                        let _ = operation.pending.response.send(Err(error));
                    }
                }
                Err(error) => {
                    let _ = operation.pending.response.send(Err(error));
                }
            }
        }
        true
    }

    fn persist_managed_worktree(
        &self,
        prepared: &PreparedManagedWorktree,
    ) -> Result<(), ManagedGitError> {
        let config = self.config.clone();
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let row = prepared.worktree();
        let current_state = self.state().clone();
        let mut conflict = None;
        let state = store
            .update(&config, |state| {
                state.spawn_targets = current_state.spawn_targets.clone();
                state.device_session_template_sources =
                    current_state.device_session_template_sources.clone();
                if let Some(existing) = state
                    .worktrees
                    .iter_mut()
                    .find(|worktree| worktree.worktree_id == row.worktree_id)
                {
                    if existing.target_id != row.target_id
                        || existing.path != row.path
                        || existing.management != "hub_managed_git"
                    {
                        conflict = Some(ManagedGitError::new(
                            "worktree_record_mismatch",
                            "managed worktree record conflicts with the prepared worktree",
                        ));
                    } else {
                        *existing = row.clone();
                    }
                } else {
                    state.worktrees.push(row.clone());
                }
            })
            .map_err(|_| {
                ManagedGitError::new(
                    "persistence_failed",
                    "managed worktree state could not be persisted",
                )
            })?;
        if let Some(conflict) = conflict {
            return Err(conflict);
        }
        self.replace_state(state);
        Ok(())
    }

    fn remove_managed_worktree_record(&self, worktree_id: &str) -> Result<(), ManagedGitError> {
        let config = self.config.clone();
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let state = store
            .update(&config, |state| {
                state.worktrees.retain(|worktree| {
                    worktree.worktree_id != worktree_id || worktree.management != "hub_managed_git"
                });
            })
            .map_err(|_| {
                ManagedGitError::new(
                    "reconciliation_failed",
                    "managed worktree rollback state could not be persisted",
                )
            })?;
        self.replace_state(state);
        Ok(())
    }

    fn spawn_prepared_managed_session(
        &self,
        pending: &PendingManagedSessionSpawn,
        prepared: &PreparedManagedWorktree,
    ) -> Result<PluginManagedSessionSpawned, ManagedGitError> {
        let session_id = generated_session_uuid()?;
        let records = pending.package_records.iter().collect::<Vec<_>>();
        let state = self.state();
        let materialized = materialize_managed_session_template(
            &self.config,
            &records,
            &state,
            &pending.template_id,
            session_id,
            pending.request.clone(),
            &EnsuredManagedWorktree {
                target_id: prepared.target_id.clone(),
                repository_root: prepared.repository_root.clone(),
                worktree_path: prepared.path.clone(),
                branch: prepared.branch.clone(),
                base_ref: prepared.base_ref.clone(),
                base_commit: prepared.base_commit.clone(),
            },
        )
        .map_err(|error| ManagedGitError::new(error.kind, error.message))?;
        drop(state);
        let context = materialized.context.clone();
        {
            let mut contexts = self.session_contexts.lock().map_err(|_| {
                ManagedGitError::new("spawn_failed", "session context state is unavailable")
            })?;
            contexts.insert(context.context_id.clone(), context.clone());
            contexts.insert(context.session_id.0.clone(), context.clone());
        }
        let outcome = self
            .core_daemon
            .lock()
            .map_err(|_| ManagedGitError::new("spawn_failed", "core daemon is unavailable"))?
            .spawn(
                SpawnSessionRequest {
                    request: materialized.spawn_request,
                    metadata: plugin_session_metadata(&pending.plugin_key),
                },
                current_unix_seconds(),
            )
            .map_err(|_error| {
                if let Ok(mut contexts) = self.session_contexts.lock() {
                    contexts.remove(&context.context_id);
                    contexts.remove(&context.session_id.0);
                }
                ManagedGitError::new("spawn_failed", "configured session could not be spawned")
            })?;
        Ok(PluginManagedSessionSpawned {
            session_id: outcome.session_id.0,
            target_id: prepared.target_id.clone(),
            branch: prepared.branch.clone(),
            worktree_id: prepared.worktree_id.clone(),
            worktree_path: prepared.path.display().to_string(),
            base_ref: prepared.base_ref.clone(),
            base_commit: prepared.base_commit.clone(),
            created_worktree: prepared.created_worktree,
            created_branch: prepared.created_branch,
            reused_worktree: !prepared.created_worktree,
        })
    }

    fn cleanup_managed_session(&self, spawned: &PluginManagedSessionSpawned) {
        let session_id = SessionId(spawned.session_id.clone());
        if let Ok(mut daemon) = self.core_daemon.lock() {
            let _ = daemon.shutdown(Some(session_id.clone()), current_unix_seconds());
        }
        if let Ok(mut contexts) = self.session_contexts.lock() {
            contexts.remove(&session_id.0);
            contexts.remove(&format!("ctx-{}", session_id.0));
        }
    }

    fn fulfill_pending_plugin_requests(&self) {
        self.fulfill_pending_coordination_requests();
        self.fulfill_pending_session_template_reads();
        self.fulfill_pending_session_template_spawns();
        self.accept_pending_managed_git_operations();
        self.advance_managed_git_operations();
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
        let state = self.state();
        let materialized = materialize_session_template(
            &self.config,
            &records,
            &state,
            &pending.template_id,
            pending.request.clone(),
        )
        .map_err(|error| format!("{}: {}", error.kind, error.message))?;
        drop(state);
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
        validate_plugin_surface_binding_families(&node)?;
        Ok(node)
    }

    /// Dispatch a plugin-owned semantic UI action through the plugin worker path.
    pub fn dispatch_plugin_surface_action(
        &self,
        package_name: &str,
        request: &UiActionRequest,
    ) -> Result<UiActionResult, crate::McpToolError> {
        let surface_id = &request.surface_id.0;
        let action_id = &request.action_id.0;
        let descriptor = self
            .plugin_lifecycle
            .ui_action_descriptors()
            .into_iter()
            .find(|descriptor| {
                descriptor.descriptor.plugin_key.0 == package_name
                    && descriptor.descriptor.descriptor_id == action_id.as_str()
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
            payload: BoundaryJson(serde_json::to_value(request).map_err(|error| {
                crate::McpToolError::new(
                    "invalid_action_request",
                    format!("invalid plugin UiActionRequest: {error}"),
                )
            })?),
        });
        let value = completed_plugin_payload(outcome.result, "plugin surface action")?;
        let result: UiActionResult = serde_json::from_value(value).map_err(|error| {
            crate::McpToolError::new(
                "invalid_action_result",
                format!("invalid plugin UiActionResult: {error}"),
            )
        })?;
        validate_plugin_surface_action_result(&result, request)?;
        Ok(result)
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

    pub(crate) fn mark_session_stale(
        &self,
        session_id: &SessionId,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.core_daemon
            .lock()
            .expect("core daemon mutex")
            .mark_stale(session_id, now_seconds)
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
                            self.mark_session_stale(&report.record.session_id, now_seconds)?;
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
                    self.mark_session_stale(&report.record.session_id, now_seconds)?;
                    self.reconciliation
                        .stale_sessions
                        .push(report.record.session_id);
                }
                SessionAdoptionState::Terminal => {
                    if report.record.state == RegistrySessionState::Running {
                        self.mark_session_stale(&report.record.session_id, now_seconds)?;
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
            reads: Mutex::new(VecDeque::new()),
            managed: Mutex::new(VecDeque::new()),
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

    /// List enabled effective templates admitted for one target.
    pub fn list(
        &self,
        target_id: &str,
        package_records: Vec<PackageRecord>,
    ) -> Result<Vec<HubSessionTemplate>, String> {
        self.read(target_id, SessionTemplateRead::List, package_records)
    }

    /// Show one enabled effective template admitted for one target.
    pub fn show(
        &self,
        target_id: &str,
        template_id: &str,
        package_records: Vec<PackageRecord>,
    ) -> Result<HubSessionTemplate, String> {
        self.read(
            target_id,
            SessionTemplateRead::Show {
                template_id: template_id.to_string(),
            },
            package_records,
        )?
        .into_iter()
        .next()
        .ok_or_else(|| "session template was not found".to_string())
    }

    fn read(
        &self,
        target_id: &str,
        operation: SessionTemplateRead,
        package_records: Vec<PackageRecord>,
    ) -> Result<Vec<HubSessionTemplate>, String> {
        let (response, receiver) = mpsc::channel();
        self.reads
            .lock()
            .map_err(|_| "session-template read queue lock poisoned".to_string())?
            .push_back(PendingSessionTemplateRead {
                target_id: target_id.to_string(),
                operation,
                package_records,
                response,
            });
        receiver
            .recv_timeout(Duration::from_millis(SESSION_TEMPLATE_SPAWN_TIMEOUT_MS))
            .map_err(|_| "session-template read did not complete before timeout".to_string())?
    }

    /// Queue the one atomic managed-worktree/session spawn operation.
    pub fn ensure_worktree_and_spawn(
        &self,
        plugin_key: &PluginKey,
        target_id: &str,
        branch: &str,
        template_id: &str,
        request: ManagedSessionTemplateRequest,
        package_records: Vec<PackageRecord>,
    ) -> Result<PluginManagedSessionSpawned, ManagedGitError> {
        let (response, receiver) = mpsc::channel();
        let mut managed = self.managed.lock().map_err(|_| {
            ManagedGitError::new(
                "ensure_unavailable",
                "managed session spawn queue is unavailable",
            )
        })?;
        if managed.len() >= 2 {
            return Err(ManagedGitError::new(
                "ensure_backpressured",
                "managed session spawn queue is saturated",
            ));
        }
        managed.push_back(PendingManagedSessionSpawn {
            plugin_key: plugin_key.clone(),
            target_id: target_id.to_string(),
            branch: branch.to_string(),
            template_id: template_id.to_string(),
            request,
            package_records,
            accepted_at: Instant::now(),
            response,
        });
        drop(managed);
        receiver
            .recv_timeout(Duration::from_millis(SESSION_TEMPLATE_SPAWN_TIMEOUT_MS))
            .map_err(|_| {
                ManagedGitError::new(
                    "ensure_timed_out",
                    "managed session spawn did not complete before timeout",
                )
            })?
    }

    fn take_read(&self) -> Option<PendingSessionTemplateRead> {
        self.reads
            .lock()
            .expect("session-template read queue lock")
            .pop_front()
    }

    fn take_managed(&self) -> Option<PendingManagedSessionSpawn> {
        self.managed
            .lock()
            .expect("managed session spawn queue lock")
            .pop_front()
    }
}

impl ManagedGitCoordinator {
    fn new() -> Self {
        let (sender, receiver) = mpsc::sync_channel::<ManagedGitWorkerJob>(1);
        thread::Builder::new()
            .name("botster-managed-git".to_string())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let accepted_at = job.request.accepted_at;
                    match prepare_managed_worktree(&job.request) {
                        Ok(prepared) => {
                            if job.prepared.send(Ok(prepared.clone())).is_err() {
                                let _ = rollback_prepared_worktree(
                                    &prepared,
                                    accepted_at + MANAGED_GIT_OPERATION_TIMEOUT,
                                );
                                let _ = job.finalized.send(Ok(()));
                                continue;
                            }
                            let remaining = (accepted_at + MANAGED_GIT_OPERATION_TIMEOUT)
                                .saturating_duration_since(Instant::now());
                            let decision = job.decision.recv_timeout(remaining);
                            let finalized = finalize_prepared_managed_worktree(
                                &prepared,
                                decision,
                                accepted_at + MANAGED_GIT_OPERATION_TIMEOUT,
                            );
                            let _ = job.finalized.send(finalized);
                        }
                        Err(error) => {
                            let _ = job.prepared.send(Err(error));
                        }
                    }
                }
            })
            .expect("managed Git worker thread");
        Self { sender }
    }

    fn submit(&self, request: ManagedGitRequest) -> Result<ManagedGitSubmission, ManagedGitError> {
        let (prepared, prepared_receiver) = mpsc::channel();
        let (decision, decision_receiver) = mpsc::channel();
        let (finalized, finalized_receiver) = mpsc::channel();
        self.sender
            .try_send(ManagedGitWorkerJob {
                request,
                prepared,
                decision: decision_receiver,
                finalized,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => ManagedGitError::new(
                    "ensure_backpressured",
                    "managed Git worker already has an active and waiting operation",
                ),
                mpsc::TrySendError::Disconnected(_) => {
                    ManagedGitError::new("ensure_unavailable", "managed Git worker is unavailable")
                }
            })?;
        Ok((prepared_receiver, decision, finalized_receiver))
    }
}

fn finalize_prepared_managed_worktree(
    prepared: &PreparedManagedWorktree,
    decision: Result<ManagedGitDecision, mpsc::RecvTimeoutError>,
    deadline: Instant,
) -> Result<(), ManagedGitError> {
    match decision {
        Ok(ManagedGitDecision::Commit) => Ok(()),
        Ok(ManagedGitDecision::Rollback) | Err(mpsc::RecvTimeoutError::Disconnected) => {
            rollback_prepared_worktree(prepared, deadline)
        }
        Ok(ManagedGitDecision::Preserve) => Err(ManagedGitError::new(
            "reconciliation_required",
            "managed Git owner deadline elapsed; prepared resources were preserved",
        )),
        Err(mpsc::RecvTimeoutError::Timeout) => Err(ManagedGitError::new(
            "reconciliation_required",
            "managed Git owner decision timed out; prepared resources were preserved",
        )),
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

fn package_allows_managed_git_spawn(
    package_records: &[PackageRecord],
    plugin_key: &PluginKey,
) -> bool {
    package_records.iter().any(|record| {
        record.manifest.name == plugin_key.0
            && matches!(record.state, PackageState::Enabled)
            && record.manifest.capabilities.iter().any(|capability| {
                capability.surface == botster_core::CapabilitySurface::SessionActions
                    && capability.scope.as_deref() == Some("session_template_managed_git_spawn")
            })
    })
}

fn generated_session_uuid() -> Result<SessionId, ManagedGitError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| {
        ManagedGitError::new(
            "session_id_unavailable",
            "session id could not be generated",
        )
    })?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(SessionId(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )))
}

fn managed_worktree_root(config: &HubConfig) -> PathBuf {
    let data_directory = if config.data_directory.is_absolute() {
        config.data_directory.clone()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(&config.data_directory)
    };
    data_directory.join("managed-worktrees")
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

fn validate_plugin_surface_binding_families(node: &UiNode) -> Result<(), crate::McpToolError> {
    let value = serde_json::to_value(node).map_err(|error| {
        crate::McpToolError::new(
            "invalid_surface",
            format!("failed to inspect plugin UiNode bindings: {error}"),
        )
    })?;
    validate_plugin_surface_binding_value(&value)
}

fn validate_plugin_surface_binding_value(
    value: &serde_json::Value,
) -> Result<(), crate::McpToolError> {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                validate_plugin_surface_binding_value(value)?;
            }
        }
        serde_json::Value::Object(object) => {
            if let Some(path) = object.get("$bind").and_then(serde_json::Value::as_str) {
                validate_plugin_surface_binding_path(path)?;
            }
            match object.get("$kind").and_then(serde_json::Value::as_str) {
                Some("bind_list") => {
                    if let Some(path) = object.get("source").and_then(serde_json::Value::as_str) {
                        validate_plugin_surface_binding_path(path)?;
                    }
                }
                Some("bind_if") => {
                    if let Some(path) = object.get("path").and_then(serde_json::Value::as_str) {
                        validate_plugin_surface_binding_path(path)?;
                    }
                }
                _ => {}
            }
            for value in object.values() {
                validate_plugin_surface_binding_value(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_plugin_surface_binding_path(path: &str) -> Result<(), crate::McpToolError> {
    if !path.starts_with('/') || path == "/session" || path.starts_with("/session/") {
        return Ok(());
    }
    Err(crate::McpToolError::new(
        "invalid_surface",
        format!(
            "plugin UiNode binding family is not admitted by this Hub: {path}; only /session is available"
        ),
    ))
}

fn validate_plugin_surface_action_result(
    result: &UiActionResult,
    request: &UiActionRequest,
) -> Result<(), crate::McpToolError> {
    result.validate().map_err(|error| {
        crate::McpToolError::new(
            "invalid_action_result",
            format!("invalid plugin UiActionResult: {error}"),
        )
    })?;
    if result.request_id != request.request_id
        || result.surface_id != request.surface_id
        || result.action_id != request.action_id
        || result.node_id != request.node_id
    {
        return Err(crate::McpToolError::new(
            "invalid_action_result",
            "plugin UiActionResult identity does not match the request",
        ));
    }
    if let Some(replacement) = &result.replacement {
        validate_plugin_surface_binding_families(replacement).map_err(|error| {
            crate::McpToolError::new(
                "invalid_action_result",
                format!(
                    "invalid plugin UiActionResult replacement: {}",
                    error.message
                ),
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DataDirectoryOption, HostIdentityOptions, HubStartupOptions, RuntimeEnvironment,
        SessionDefaults, TransportBindings,
    };
    use std::fs;
    use std::process::Command;

    fn binding_test_node(child: serde_json::Value) -> UiNode {
        serde_json::from_value(serde_json::json!({
            "type": "panel",
            "id": "binding-test",
            "children": [child]
        }))
        .expect("binding test UiNode")
    }

    #[test]
    fn plugin_surface_binding_admission_accepts_only_session_absolute_family() {
        let node = binding_test_node(serde_json::json!({
            "$kind": "bind_list",
            "source": "/session",
            "where": { "session_uuid": "session-1" },
            "item_template": {
                "type": "text",
                "id": "session-row",
                "props": {
                    "text": { "$bind": "/session/session-1/lifecycle_class" }
                }
            },
            "empty_template": {
                "type": "text",
                "id": "session-unavailable",
                "props": { "text": "Session unavailable" }
            }
        }));

        node.validate().expect("generic UiNode validation");
        validate_plugin_surface_binding_families(&node)
            .expect("/session and item-relative bindings are admitted");
    }

    #[test]
    fn plugin_surface_binding_admission_rejects_foreign_and_dotted_absolute_families() {
        for source in ["/workspace", "/project-pipelines.ticket", "/sessionish"] {
            let node = binding_test_node(serde_json::json!({
                "$kind": "bind_list",
                "source": source,
                "item_template": {
                    "type": "text",
                    "id": "row",
                    "props": { "text": "row" }
                }
            }));
            node.validate().expect("generic UiNode validation");
            let error = validate_plugin_surface_binding_families(&node)
                .expect_err("foreign absolute binding family must be rejected");
            assert_eq!(error.code, "invalid_surface");
            assert!(error.message.contains(source), "{error:?}");
        }
    }

    fn binding_action_result(source: &str) -> (UiActionRequest, UiActionResult) {
        let request = serde_json::from_value(serde_json::json!({
            "request_id": "binding-action-request",
            "surface_id": "contract.sessions",
            "action_id": "replace",
            "node_id": "binding-action",
            "kind": "submit"
        }))
        .expect("binding action request");
        let result = serde_json::from_value(serde_json::json!({
            "request_id": "binding-action-request",
            "surface_id": "contract.sessions",
            "action_id": "replace",
            "node_id": "binding-action",
            "state": "accepted",
            "replacement": {
                "type": "panel",
                "id": "binding-action-replacement",
                "children": [{
                    "$kind": "bind_list",
                    "source": source,
                    "where": { "session_uuid": "session-1" },
                    "item_template": {
                        "type": "text",
                        "id": "binding-action-row",
                        "props": { "text": { "$bind": "@/lifecycle_class" } }
                    }
                }]
            }
        }))
        .expect("binding action result");
        (request, result)
    }

    #[test]
    fn plugin_surface_action_replacement_applies_binding_family_admission() {
        let (request, accepted) = binding_action_result("/session");
        validate_plugin_surface_action_result(&accepted, &request)
            .expect("/session replacement binding must be admitted");

        let (_, rejected) = binding_action_result("/workspace");
        let error = validate_plugin_surface_action_result(&rejected, &request)
            .expect_err("foreign replacement binding must be rejected");
        assert_eq!(error.code, "invalid_action_result");
        assert!(error.message.contains("/workspace"), "{error:?}");
    }

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
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("runtime config should build");

        let core_config = core_daemon_config(&config);
        assert!(
            core_config.worker_path.is_some(),
            "hub CoreDaemonConfig must use worker-backed sessions so in-process durability adoption is unreachable"
        );
    }

    #[test]
    fn managed_git_coordinator_serializes_one_active_and_one_waiting_job() {
        let root = std::env::temp_dir().join(format!(
            "botster-managed-coordinator-{}-{}",
            std::process::id(),
            current_unix_nanos()
        ));
        let repository = root.join("repository");
        let managed_root = root.join("managed");
        fs::create_dir_all(&repository).expect("create repository");
        run_git(None, &["init", "-b", "main", path_str(&repository)]);
        run_git(
            Some(&repository),
            &["config", "user.email", "botster@example.invalid"],
        );
        run_git(Some(&repository), &["config", "user.name", "Botster Test"]);
        fs::write(repository.join("README.md"), "fixture\n").expect("write fixture");
        run_git(Some(&repository), &["add", "README.md"]);
        run_git(Some(&repository), &["commit", "-m", "fixture"]);

        let request = || ManagedGitRequest {
            target: SpawnTarget {
                target_id: "tgt_coordinator".to_string(),
                label: "Coordinator".to_string(),
                root: repository.clone(),
                enabled: true,
                kind: "git".to_string(),
                base_ref: Some("main".to_string()),
                metadata: BTreeMap::new(),
            },
            branch: "feature/coordinated".to_string(),
            managed_root: managed_root.clone(),
            persisted_worktree: None,
            accepted_at: Instant::now(),
        };
        let coordinator = ManagedGitCoordinator::new();
        let (first_prepared, first_decision, first_finalized) =
            coordinator.submit(request()).expect("submit active job");
        let first = first_prepared
            .recv_timeout(Duration::from_secs(5))
            .expect("active preparation response")
            .expect("active preparation");
        assert!(first.created_branch);
        assert!(first.created_worktree);

        let (second_prepared, second_decision, second_finalized) = coordinator
            .submit(request())
            .expect("submit one waiting job");
        let third = coordinator
            .submit(request())
            .expect_err("third job must be backpressured");
        assert_eq!(third.kind, "ensure_backpressured");
        first_decision
            .send(ManagedGitDecision::Commit)
            .expect("commit active job");
        first_finalized
            .recv_timeout(Duration::from_secs(5))
            .expect("active finalization")
            .expect("active commit");

        let second = second_prepared
            .recv_timeout(Duration::from_secs(5))
            .expect("waiting preparation response")
            .expect("waiting preparation");
        assert!(!second.created_branch);
        assert!(!second.created_worktree);
        second_decision
            .send(ManagedGitDecision::Commit)
            .expect("commit waiting job");
        second_finalized
            .recv_timeout(Duration::from_secs(5))
            .expect("waiting finalization")
            .expect("waiting commit");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn managed_git_decision_timeout_preserves_prepared_worktree() {
        let root = std::env::temp_dir().join(format!(
            "botster-managed-decision-timeout-{}-{}",
            std::process::id(),
            current_unix_nanos()
        ));
        let repository = root.join("repository");
        let managed_root = root.join("managed");
        fs::create_dir_all(&repository).expect("create repository");
        run_git(None, &["init", "-b", "main", path_str(&repository)]);
        run_git(
            Some(&repository),
            &["config", "user.email", "botster@example.invalid"],
        );
        run_git(Some(&repository), &["config", "user.name", "Botster Test"]);
        fs::write(repository.join("README.md"), "fixture\n").expect("write fixture");
        run_git(Some(&repository), &["add", "README.md"]);
        run_git(Some(&repository), &["commit", "-m", "fixture"]);
        let request = ManagedGitRequest {
            target: SpawnTarget {
                target_id: "tgt_timeout".to_string(),
                label: "Timeout".to_string(),
                root: repository.clone(),
                enabled: true,
                kind: "git".to_string(),
                base_ref: Some("main".to_string()),
                metadata: BTreeMap::new(),
            },
            branch: "feature/preserve".to_string(),
            managed_root,
            persisted_worktree: None,
            accepted_at: Instant::now(),
        };
        let prepared = prepare_managed_worktree(&request).expect("prepare managed worktree");
        let error = finalize_prepared_managed_worktree(
            &prepared,
            Err(mpsc::RecvTimeoutError::Timeout),
            Instant::now(),
        )
        .expect_err("missing owner decision must preserve the prepared worktree");
        assert_eq!(error.kind, "reconciliation_required");
        assert!(
            prepared.path.exists(),
            "a decision timeout must not remove the worktree later reported by the owner"
        );
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&repository)
                .args([
                    "show-ref",
                    "--verify",
                    "--quiet",
                    "refs/heads/feature/preserve"
                ])
                .status()
                .expect("inspect preserved branch")
                .success()
        );
        let _ = fs::remove_dir_all(root);
    }

    fn current_unix_nanos() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    }

    fn path_str(path: &std::path::Path) -> &str {
        path.to_str().expect("test path is UTF-8")
    }

    fn run_git(root: Option<&std::path::Path>, args: &[&str]) {
        let mut command = Command::new("git");
        if let Some(root) = root {
            command.arg("-C").arg(root);
        }
        assert!(
            command.args(args).status().expect("run git").success(),
            "git command failed: {args:?}"
        );
    }
}
