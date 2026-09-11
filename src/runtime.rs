//! Profile-owned runtime facade over the core daemon session supervisor.
//!
//! The first-party host profile owns explicit configuration and admission
//! policy. Session process mechanics, terminal byte routing, activity
//! accounting, guarded-write readiness, and shutdown stay in `botster-core`
//! through `botster-core-daemon`.

use botster_core::{
    BindTerminalAdapterError, BotsterEngineObservation, BotsterEngineOutput, BoundaryJson,
    ClientId, CoreSession, CoreSessionMetadata, EntityContract, EntityFrame, EntityKind,
    EnvelopeId, EnvelopeTarget, ManagedSessionRuntimeError, MultiplexerEngineError,
    PluginAdmissionResult, PluginCapabilityRuntime, PluginCleanupResult, PluginCompletionDrain,
    PluginHandlerKind, PluginInvocationClass, PluginInvocationFailure, PluginInvocationFailureKind,
    PluginInvocationOutcome, PluginInvocationRequest, PluginInvocationResult, PluginKey,
    PluginWorkerDebugSnapshot, RequestId, Rgb, RoutedEnvelope, RoutedEnvelopeDrainOutcome,
    RoutedEnvelopePublishOutcome, ReservedSessionSpawnError, SessionId, SessionLifecycleState,
    SessionReservation, SessionReservationRefusal, SessionReservationRelease, SessionRuntimeErrorKind,
    SessionSpawnRequest,
    SubscriptionId, TerminalCapabilitySet, TerminalColorProfile, TerminalSubscriptionGeneration,
};
use botster_core_daemon::{
    AcknowledgeRoutedEnvelopeRequest, CaptureId, CaptureOwner, CaptureSnapshotRequest,
    CoreCompletion, CoreDaemonConfig, CoreDaemonError, CoreOperation, DaemonSession,
    DetachTerminalSubscriptionResult, DrainRoutedEnvelopesRequest, GuardedWriteRequest,
    GuardedWriteResult, LifecycleBaselineBudget, ObserveLifecycleBudget, ObserveLifecycleCursor,
    ObserveLifecycleSlice, PendingOperationId, PublishRoutedEnvelopeRequest, ReadModeFlagsRequest,
    ReadScreenRequest, RegistrySessionState, RetentionAccounting, RetentionPolicy,
    RoutedEnvelopeDeliveryStateResult, SessionAdoptionReport, SessionAdoptionState,
    SessionLifecycleBaselinePage, SessionLifecycleCursor, SessionLifecyclePage,
    SessionLifecyclePageError, SessionRegistryStateLookup, SnapshotPage, SpawnSessionRequest,
};
use botster_core_daemon::operation::ReservedSpawnResult;
use botster_ui_contract::{UiActionRequest, UiActionResult, UiNode};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::capabilities::HubCapabilityRuntime;
use crate::config::HubConfig;
use crate::credentials::{
    CredentialPolicyError, CredentialProviderKind, OsKeychainCredentialStore,
    validate_hub_credentials,
};
use crate::data_plane::driver::{CoreOperationTicket, CoreTicket, CoreTicketError, CoreTicketPoll};
use crate::lifecycle::{
    HubLifecycleResult, HubPluginLifecycle, HubPluginLifecycleStatus, HubPluginLoadFailure,
    HubPluginRuntimeBundle, package_entity_owner_token,
};
use crate::lua_runtime::{
    HubCoordinationBridge, HubEntityPublishBridge, LuaPluginHostApi, LuaPluginRuntimeError,
    PendingCoordinationOperation, SharedHubCapabilityRuntime,
};
use crate::managed_git_worktrees::{
    ManagedGitError, ManagedGitRequest, PreparedManagedWorktree,
    adopt_unrecorded_managed_worktrees, managed_worktree_id,
};
use crate::package_entity_fanout::{
    EntityMutationLease, LeasedFanoutMutation, PackageEntityFamilyProgress,
    PackageEntityFamilyState, PackageEntityFamilyStep, PackageEntityFanoutQueue,
    PackageEntityMutation, PackageEntityPublishResult, PackageEntityPublishStatus,
    coerce_entity_frame_empty_items, prepare_publish_mutation,
};
use crate::package_event_router::{
    CausalAdmitResult, CausalOp, EventPlaneStatus, EventSubscription, LeaseIdentity,
};
use crate::packages::{PackageRecord, PackageRegistry, PackageRegistryError, PackageState};
use crate::persistence::{FileHubStateStore, HubState, HubStateStore, HubStateStoreError};
use crate::session_types::{
    EnsuredManagedWorktree, HubSessionContext, ManagedSessionTypeRequest, SessionTypeRequest,
    materialize_managed_session_type, materialize_session_type, show_session_type_for_target,
};
use crate::shared_view::{SharedView, SharedViewBudget};

pub(crate) mod causal;
pub use causal::{CAUSAL_OWNER_CAPACITY, CAUSAL_OWNER_PAYLOAD_BYTES, CausalTransitionStatus};
use causal::{CausalOwnerQueue, CausalReservation};
mod entities;
use entities::PackageEntities;
pub(crate) mod entity_model;
pub(crate) mod provider;
pub(crate) mod publication;
pub(crate) mod resync;
use provider::{ProviderExpectation, ProviderRequestPlan};
pub(crate) mod family_cleanup;
pub(crate) mod package_effect;
use package_effect::{HostPackageCleanup, HostPackageRuntime};

/// Allocation-owned immutable durable state view.
pub type HubStateView = SharedView<HubState>;

/// Hub-owned adapter and policy facade over the default local core engine.
///
/// This facade exposes host-adjacent admission, visibility, runtime-drain, and
/// typed pressure-reporting operations. It intentionally does not expose core's
/// generic `DefaultEngineCommand` router; hub callers use explicit methods so
/// admission and policy boundaries remain visible at the hub layer.
pub struct HubRuntime {
    config: HubConfig,
    lua_memory: Arc<crate::lua_memory::LuaMemoryAccount>,
    // Readers clone the current Arc under this short lock. Publication swaps
    // one Arc, so the owner never clones a durable state collection.
    state: SharedHubState,
    core_daemon: SharedCoreDaemon,
    detached_operations: Mutex<Vec<CoreOperationTracker>>,
    inflight_plugin_core: Mutex<Vec<InflightPluginCore>>,
    retained_plugin_reservations: Mutex<Vec<SessionReservation>>,
    close_work: crate::data_plane::CloseWorkSource,
    data_plane: Option<crate::data_plane::DataPlaneDriver>,
    reconciliation: HubSessionReconciliation,
    plugin_lifecycle: Option<HubPluginLifecycle>,
    capability_runtime: SharedHubCapabilityRuntime,
    session_type_spawner: SharedSessionTypeSpawner,
    host_executor: crate::host_executor::HostExecutor,
    coordination_bridge: HubCoordinationBridge,
    entity_publish_bridge: HubEntityPublishBridge,
    entity_publish_wait: Cell<PublicationWait>,
    package_entities: Arc<Mutex<PackageEntities>>,
    entity_model_owner: entity_model::Owner,
    next_provider_token: Cell<u64>,
    package_entity_resync_changed: std::cell::Cell<bool>,
    last_capability_cleanup: Option<PluginCleanupResult>,
    session_contexts: SharedSessionContexts,
    package_event_router: Arc<crate::package_event_router::PackageEventRouter>,
    event_plane_counters: Arc<crate::event_plane_counters::EventPlaneCounters>,
    causal_scopes: Arc<crate::package_event_router::CausalScopeTable>,
    causal_queue: CausalOwnerQueue,
    direct_family_cleanup: std::cell::RefCell<Option<HostPackageCleanup>>,
    event_plane_owner_ops: std::cell::RefCell<crate::package_event_router::EventPlaneOwnerOps>,
    event_plane_owner_ops_changed: std::cell::Cell<bool>,
    event_plane_cleanup_faults: std::cell::RefCell<
        Vec<(
            Option<Result<u64, EventPlaneStatus>>,
            crate::package_event_router::EventOwnerWorkError,
        )>,
    >,
    acknowledged_spawn_ids: Mutex<BTreeSet<String>>,
    force_plugin_admit_backpressure: Arc<std::sync::atomic::AtomicBool>,
    pending_test_event_settlements: Mutex<Vec<PendingTestEvent>>,
    force_park_test_events: std::sync::atomic::AtomicBool,
}

enum PendingTestEvent {
    Requeue {
        delivery: crate::package_event_router::ReadyDelivery,
        scope: Option<(u64, crate::package_event_router::LeaseIdentity)>,
    },
    Complete {
        delivery: crate::package_event_router::ReadyDelivery,
        scope: Option<(u64, crate::package_event_router::LeaseIdentity)>,
    },
    Release {
        scope_id: u64,
        identity: crate::package_event_router::LeaseIdentity,
    },
}

type SharedCoreDaemon = crate::data_plane::driver::CoreDaemonHandle;
type SharedSessionContexts = Arc<Mutex<BTreeMap<String, HubSessionContext>>>;
const SESSION_TYPE_SPAWN_TIMEOUT_MS: u64 = 30_000;
const PLUGIN_EVENT_TIMEOUT_MS: u64 = 1_000;
/// Shared hub-owned session-type spawn bridge exposed to Lua plugin workers.
pub type SharedSessionTypeSpawner = Arc<HubSessionTypeSpawner>;
struct PublishedHubState {
    revision: u64,
    state: SharedView<HubState>,
}

/// One versioned durable state view shared by every runtime and daemon reader.
pub struct HubStatePublication(RwLock<PublishedHubState>);

impl HubStatePublication {
    pub(crate) fn new(state: HubState) -> Result<Self, HubStateStoreError> {
        let budget = SharedViewBudget::new();
        let logical_bytes = serde_json::to_vec_pretty(&state)
            .map_err(HubStateStoreError::Serialize)?
            .len();
        let state = SharedView::try_new(&budget, state, logical_bytes).map_err(|error| {
            HubStateStoreError::ViewCapacity {
                requested: error.requested,
                available: error.available,
            }
        })?;
        Ok(Self(RwLock::new(PublishedHubState { revision: 0, state })))
    }

    pub(crate) fn snapshot(&self) -> (u64, SharedView<HubState>) {
        let published = self.0.read().expect("hub state lock");
        (published.revision, published.state.clone())
    }

    pub(crate) fn try_snapshot(&self) -> Result<(u64, SharedView<HubState>), ()> {
        self.0
            .read()
            .map(|published| (published.revision, published.state.clone()))
            .map_err(|_| ())
    }

    pub(crate) fn publish(&self, state: SharedView<HubState>) {
        let mut published = self.0.write().expect("hub state lock");
        published.revision = published
            .revision
            .checked_add(1)
            .expect("hub state revision exhausted");
        published.state = state;
    }

    pub(crate) fn budget(&self) -> Arc<SharedViewBudget> {
        let published = self.0.read().expect("hub state lock");
        published.state.budget()
    }

    pub(crate) fn prepare(
        &self,
        state: HubState,
    ) -> Result<SharedView<HubState>, HubStateStoreError> {
        let logical_bytes = serde_json::to_vec_pretty(&state)
            .map_err(HubStateStoreError::Serialize)?
            .len();
        SharedView::try_new(&self.budget(), state, logical_bytes).map_err(|error| {
            HubStateStoreError::ViewCapacity {
                requested: error.requested,
                available: error.available,
            }
        })
    }
}

/// Shared immutable durable state exposed to runtime and Lua readers.
pub type SharedHubState = Arc<HubStatePublication>;
/// Shared hub-owned spawn-target view exposed to Lua plugin workers.
pub type SharedSpawnTargets = SharedHubState;
/// Shared hub-owned worktree view exposed to Lua plugin workers.
pub type SharedWorktrees = SharedHubState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageEntityCleanupError {
    GenerationExhausted,
    Busy,
}

/// Prepared package entity-provider work and its causal lease.
pub(crate) struct PluginEntitySnapshotInvocation {
    pub(crate) expected: SharedView<ProviderExpectation>,
    pub(crate) family_generation: u64,
    // The retained scope ID and invocation token identify this exact lease.
    pub(crate) causal_lease: Option<(u64, u64)>,
    pub(crate) lease_acquired: bool,
    pub(crate) admission: Option<crate::lua_runtime::EntityPublishPermit>,
}

impl PluginEntitySnapshotInvocation {
    #[must_use]
    pub(crate) fn expected_entity_kind(&self) -> &EntityKind {
        &self.expected.entity_kind
    }
}

/// Hub-owned policy bridge for plugin-safe session-type spawns.
pub struct HubSessionTypeSpawner {
    pending: Mutex<VecDeque<PendingSessionTypeSpawn>>,
    managed: Mutex<VecDeque<PendingManagedSessionSpawn>>,
    managed_pending: AtomicBool,
    managed_owner: Mutex<Option<crate::daemon::control::message::ControlSender>>,
    abandoned: Mutex<Vec<String>>,
}

/// These handles permit explicit queue cleanup after the engine disposal receipt.
pub(crate) struct TerminalPluginBridges {
    coordination: HubCoordinationBridge,
    entity_publish: HubEntityPublishBridge,
    spawner: SharedSessionTypeSpawner,
}

impl TerminalPluginBridges {
    /// Host clears actual queue contents after all scoped producers stop.
    pub(crate) fn dispose(&self) -> bool {
        self.coordination.dispose_terminal_pending()
            && self.entity_publish.dispose_terminal_pending()
            && self.spawner.dispose_terminal_pending()
    }
}

#[cfg(test)]
struct TerminalSpawnerProbe {
    spawner: std::sync::Weak<HubSessionTypeSpawner>,
    dropped: mpsc::Sender<(String, bool)>,
    gate: Option<mpsc::Receiver<()>>,
}

#[cfg(test)]
impl Drop for TerminalSpawnerProbe {
    fn drop(&mut self) {
        let spawner = self
            .spawner
            .upgrade()
            .expect("the test retains the spawner");
        let locks_released = !matches!(
            spawner.pending.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ) && !matches!(
            spawner.managed.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        );
        let _ = self.dropped.send((
            thread::current().name().unwrap_or("unnamed").to_string(),
            locks_released,
        ));
        if let Some(gate) = self.gate.take()
            && matches!(
                gate.recv_timeout(Duration::from_secs(5)),
                Err(mpsc::RecvTimeoutError::Timeout)
            )
        {
            panic!("the test must release the payload destructor");
        }
    }
}

struct PendingSessionTypeSpawn {
    #[cfg(test)]
    _dispose_probe: Option<TerminalSpawnerProbe>,
    plugin_key: PluginKey,
    session_type_id: String,
    request: SessionTypeRequest,
    package_records: Vec<PackageRecord>,
    response: mpsc::Sender<Result<PluginSessionTypeSpawned, String>>,
}

pub(crate) struct PendingManagedSessionSpawn {
    #[cfg(test)]
    _dispose_probe: Option<TerminalSpawnerProbe>,
    pub(crate) plugin_key: PluginKey,
    pub(crate) target_id: String,
    pub(crate) branch: String,
    pub(crate) session_type_id: String,
    pub(crate) request: ManagedSessionTypeRequest,
    pub(crate) package_records: Vec<PackageRecord>,
    pub(crate) accepted_at: Instant,
    pub(crate) response: mpsc::Sender<Result<PluginManagedSessionSpawned, ManagedGitError>>,
}

impl PendingManagedSessionSpawn {
    #[cfg(test)]
    pub(crate) fn test_new(
        plugin_key: PluginKey,
        target_id: String,
        branch: String,
        session_type_id: String,
        request: ManagedSessionTypeRequest,
        package_records: Vec<PackageRecord>,
        response: mpsc::Sender<Result<PluginManagedSessionSpawned, ManagedGitError>>,
    ) -> Self {
        Self {
            plugin_key,
            target_id,
            branch,
            session_type_id,
            request,
            package_records,
            accepted_at: Instant::now(),
            response,
            _dispose_probe: None,
        }
    }
}

/// One managed session spawn in flight on the Core owner thread.
pub(crate) struct ManagedSessionSpawnStart {
    pub(crate) tracker: CoreOperationTracker,
    pub(crate) context: HubSessionContext,
    waiter_id: crate::owner_identity::WaiterId,
    stage: PluginSpawnStage,
    reservation: Option<SessionReservation>,
    spawn: SpawnSessionRequest,
    spawn_error: Option<CoreDaemonError>,
    reserve_operation_id: Option<PendingOperationId>,
    context_published: bool,
}

/// Structured Lua-facing session-type spawn response.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PluginSessionTypeSpawned {
    pub session_id: String,
    pub lifecycle: String,
    pub session_type_id: String,
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
    /// Live workers whose protocol evidence did not match this Hub.
    pub incompatible_sessions: Vec<SessionId>,
}

impl HubRuntime {
    /// Build a hub runtime from explicit, already-validated hub config.
    ///
    /// # Errors
    /// Returns an error when memory policy is invalid or the plugin database cannot be opened.
    pub fn new(config: HubConfig) -> HubRuntimeResult<Self> {
        let lua_memory = crate::lua_memory::LuaMemoryAccount::new(
            crate::config::lua_memory_limits(),
        )
        .map_err(|_| {
            HubRuntimeError::Config(crate::config::HubConfigError::InvalidCapacity {
                field: "lua_memory",
            })
        })?;
        let state = HubState::from_config(&config);
        let state = Arc::new(HubStatePublication::new(state)?);
        let core_config = core_daemon_config(&config);
        let plugin_worker_config = config.plugin_worker_config();
        let plugin_lifecycle = HubPluginLifecycle::with_config(plugin_worker_config);
        let (close_work, data_plane, core_daemon) = start_data_plane(core_config);
        let package_event_router = Arc::new(crate::package_event_router::PackageEventRouter::new(
            config.package_event_plane,
        ));
        let event_plane_counters = Arc::clone(package_event_router.counters());
        Ok(Self {
            capability_runtime: Arc::new(Mutex::new(
                HubCapabilityRuntime::from_config(&config).map_err(HubRuntimeError::Capability)?,
            )),
            session_type_spawner: Arc::new(HubSessionTypeSpawner::new()),
            host_executor: crate::host_executor::HostExecutor::new(),
            coordination_bridge: HubCoordinationBridge::new(),
            entity_publish_bridge: HubEntityPublishBridge::new(
                plugin_lifecycle.entity_provider_registrations(),
            ),
            entity_publish_wait: Cell::new(PublicationWait::Ready),
            package_entities: Arc::new(Mutex::new(PackageEntities::default())),
            entity_model_owner: entity_model::Owner::default(),
            next_provider_token: Cell::new(1),
            package_entity_resync_changed: std::cell::Cell::new(false),
            config,
            lua_memory,
            state,
            core_daemon,
            detached_operations: Mutex::new(Vec::new()),
            inflight_plugin_core: Mutex::new(Vec::new()),
            retained_plugin_reservations: Mutex::new(Vec::new()),
            close_work,
            data_plane: Some(data_plane),
            reconciliation: HubSessionReconciliation::default(),
            plugin_lifecycle: Some(plugin_lifecycle),
            last_capability_cleanup: None,
            session_contexts: Arc::new(Mutex::new(BTreeMap::new())),
            package_event_router,
            event_plane_counters,
            causal_scopes: Arc::new(crate::package_event_router::CausalScopeTable::new()),
            causal_queue: CausalOwnerQueue::default(),
            direct_family_cleanup: std::cell::RefCell::new(None),
            event_plane_owner_ops: std::cell::RefCell::new(
                crate::package_event_router::EventPlaneOwnerOps::default(),
            ),
            event_plane_owner_ops_changed: std::cell::Cell::new(false),
            event_plane_cleanup_faults: std::cell::RefCell::new(Vec::new()),
            acknowledged_spawn_ids: Mutex::new(BTreeSet::new()),
            force_plugin_admit_backpressure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            pending_test_event_settlements: Mutex::new(Vec::new()),
            force_park_test_events: std::sync::atomic::AtomicBool::new(false),
        })
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
            store.save_exclusive_startup_state(&state)?;
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
            store.save_exclusive_startup_state(&state)?;
        }
        validate_hub_credentials(&state, provider_kind, credential_store)?;
        Self::from_validated_state(config, state)
    }

    fn from_validated_state(config: HubConfig, state: HubState) -> HubRuntimeResult<Self> {
        let lua_memory = crate::lua_memory::LuaMemoryAccount::new(
            crate::config::lua_memory_limits(),
        )
        .map_err(|_| {
            HubRuntimeError::Config(crate::config::HubConfigError::InvalidCapacity {
                field: "lua_memory",
            })
        })?;
        let state = Arc::new(HubStatePublication::new(state)?);
        let core_config = core_daemon_config(&config);
        let plugin_worker_config = config.plugin_worker_config();
        let plugin_lifecycle = HubPluginLifecycle::with_config(plugin_worker_config);
        let (close_work, data_plane, core_daemon) = start_data_plane(core_config);
        let package_event_router = Arc::new(crate::package_event_router::PackageEventRouter::new(
            config.package_event_plane,
        ));
        let event_plane_counters = Arc::clone(package_event_router.counters());
        let mut runtime = Self {
            capability_runtime: Arc::new(Mutex::new(
                HubCapabilityRuntime::from_config(&config).map_err(HubRuntimeError::Capability)?,
            )),
            session_type_spawner: Arc::new(HubSessionTypeSpawner::new()),
            host_executor: crate::host_executor::HostExecutor::new(),
            coordination_bridge: HubCoordinationBridge::new(),
            entity_publish_bridge: HubEntityPublishBridge::new(
                plugin_lifecycle.entity_provider_registrations(),
            ),
            entity_publish_wait: Cell::new(PublicationWait::Ready),
            package_entities: Arc::new(Mutex::new(PackageEntities::default())),
            entity_model_owner: entity_model::Owner::default(),
            next_provider_token: Cell::new(1),
            package_entity_resync_changed: std::cell::Cell::new(false),
            config,
            lua_memory,
            state,
            core_daemon,
            detached_operations: Mutex::new(Vec::new()),
            inflight_plugin_core: Mutex::new(Vec::new()),
            retained_plugin_reservations: Mutex::new(Vec::new()),
            close_work,
            data_plane: Some(data_plane),
            reconciliation: HubSessionReconciliation::default(),
            plugin_lifecycle: Some(plugin_lifecycle),
            last_capability_cleanup: None,
            session_contexts: Arc::new(Mutex::new(BTreeMap::new())),
            package_event_router,
            event_plane_counters,
            causal_scopes: Arc::new(crate::package_event_router::CausalScopeTable::new()),
            causal_queue: CausalOwnerQueue::default(),
            direct_family_cleanup: std::cell::RefCell::new(None),
            event_plane_owner_ops: std::cell::RefCell::new(
                crate::package_event_router::EventPlaneOwnerOps::default(),
            ),
            event_plane_owner_ops_changed: std::cell::Cell::new(false),
            event_plane_cleanup_faults: std::cell::RefCell::new(Vec::new()),
            acknowledged_spawn_ids: Mutex::new(BTreeSet::new()),
            force_plugin_admit_backpressure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            pending_test_event_settlements: Mutex::new(Vec::new()),
            force_park_test_events: std::sync::atomic::AtomicBool::new(false),
        };
        runtime.reconcile_sessions(0)?;
        if !runtime.reconciliation.incompatible_sessions.is_empty() {
            return Err(HubRuntimeError::IncompatibleWorkers {
                sessions: runtime
                    .reconciliation
                    .incompatible_sessions
                    .iter()
                    .map(|session_id| session_id.0.clone())
                    .collect(),
            });
        }
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

    /// Return the package entity publish bridge used by Lua helpers.
    #[must_use]
    pub fn entity_publish_bridge(&self) -> HubEntityPublishBridge {
        self.entity_publish_bridge.clone()
    }

    /// Return the shared session-type spawn bridge used by Lua helpers.
    #[must_use]
    pub fn session_type_spawner(&self) -> SharedSessionTypeSpawner {
        self.session_type_spawner.clone()
    }

    /// Return the durable hub state loaded for this runtime.
    pub fn state(&self) -> HubStateView {
        self.state.snapshot().1
    }

    /// Return the shared state publication used by daemon and plugin readers.
    pub(crate) fn state_publication(&self) -> SharedHubState {
        Arc::clone(&self.state)
    }

    /// Publish durable hub state after an owner-thread mutation.
    pub(crate) fn publish_state_view(&self, state: SharedView<HubState>) {
        self.state.publish(state);
    }

    /// Replace the in-memory state after reserving its shared-view charge.
    pub fn replace_state(&self, state: HubState) -> Result<(), HubStateStoreError> {
        let state = self.prepare_state(state)?;
        self.publish_state_view(state);
        Ok(())
    }

    pub(crate) fn shared_view_budget(&self) -> Arc<SharedViewBudget> {
        self.state.budget()
    }

    pub(crate) fn prepare_state(
        &self,
        state: HubState,
    ) -> Result<SharedView<HubState>, HubStateStoreError> {
        self.state.prepare(state)
    }

    /// Return the shared spawn-target projection used by Lua helpers.
    #[must_use]
    pub fn spawn_targets(&self) -> SharedSpawnTargets {
        Arc::clone(&self.state)
    }

    /// Return the shared worktree projection used by Lua helpers.
    #[must_use]
    pub fn worktrees(&self) -> SharedWorktrees {
        Arc::clone(&self.state)
    }

    /// Return Host primitives that retain this Hub's shared Lua memory account.
    #[must_use]
    pub fn lua_plugin_host_api(&self) -> LuaPluginHostApi {
        LuaPluginHostApi {
            memory: Arc::clone(&self.lua_memory),
            capabilities: self.capability_runtime.clone(),
            coordination: self.coordination_bridge(),
            entity_publish: self.entity_publish_bridge(),
            session_types: self.session_type_spawner.clone(),
            spawn_targets: Arc::clone(&self.state),
            worktrees: Arc::clone(&self.state),
            package_event_router: self.package_event_router.clone(),
            causal_scopes: self.causal_scopes.clone(),
        }
    }

    #[must_use]
    pub fn package_event_router(&self) -> &Arc<crate::package_event_router::PackageEventRouter> {
        &self.package_event_router
    }

    #[must_use]
    pub fn event_plane_counters(&self) -> &Arc<crate::event_plane_counters::EventPlaneCounters> {
        &self.event_plane_counters
    }

    #[must_use]
    pub fn event_plane_counters_snapshot(&self) -> botster_hub_client::DaemonObservabilityCounters {
        self.event_plane_counters.snapshot()
    }

    #[must_use]
    pub fn causal_scopes(&self) -> &Arc<crate::package_event_router::CausalScopeTable> {
        &self.causal_scopes
    }

    pub fn record_event_plane_owner_op(&self, op: crate::package_event_router::OwnerOp) {
        self.event_plane_owner_ops.borrow_mut().record(op);
        if !self.event_plane_owner_ops.borrow().is_empty() {
            self.event_plane_owner_ops_changed.set(true);
        }
    }

    pub(crate) fn take_event_plane_owner_ops_notification(&self) -> bool {
        self.event_plane_owner_ops_changed.replace(false)
    }

    #[must_use]
    pub fn event_plane_owner_ops_pending(&self) -> bool {
        !self.event_plane_owner_ops.borrow().is_empty()
    }

    #[doc(hidden)]
    pub fn causal_owner_ops_pending(&self) -> bool {
        !self.causal_queue.is_empty()
            || self.has_family_resync_releases()
            || self.direct_family_cleanup.borrow().is_some()
    }

    pub(crate) fn causal_faulted(&self) -> bool {
        self.causal_scopes.is_faulted() || self.causal_queue.is_exhausted()
    }

    pub(crate) fn causal_owner_ops_ready(&self) -> bool {
        !self.causal_queue.is_empty() && self.causal_scopes.apply_ready()
    }

    pub(crate) fn take_causal_capacity_notification(&self) -> bool {
        self.causal_queue.take_capacity_notification()
    }

    pub(crate) fn reserve_causal_transition(
        &self,
    ) -> Result<CausalReservation, CausalTransitionStatus> {
        if self.causal_scopes.is_faulted() {
            return Err(CausalTransitionStatus::Fault);
        }
        self.causal_queue.reserve()
    }

    /// Complete one operation for synchronous runtime callers outside the daemon owner loop.
    /// The daemon uses retained Host dispatch through `step_event_plane_owner_op`.
    pub fn apply_event_plane_owner_ops(&self) -> Vec<crate::package_event_router::OwnerOp> {
        self.apply_causal_owner_ops();
        self.retry_family_resync_release();
        self.step_direct_family_cleanup();
        match self.step_event_plane_owner_op() {
            crate::package_event_router::OwnerStep::Applied(op) => vec![op],
            crate::package_event_router::OwnerStep::Work(work) => {
                match work.run(&self.package_event_router) {
                    Ok(completion) => self
                        .complete_event_plane_owner_op(completion)
                        .into_iter()
                        .collect(),
                    Err(error) => {
                        self.event_plane_cleanup_faults
                            .borrow_mut()
                            .push((None, error));
                        Vec::new()
                    }
                }
            }
            _ => Vec::new(),
        }
    }

    pub(crate) fn event_plane_owner_op_ready(&self) -> bool {
        self.event_plane_owner_ops.borrow().has_ready()
    }

    pub(crate) fn step_event_plane_owner_op(&self) -> crate::package_event_router::OwnerStep {
        self.event_plane_owner_ops
            .borrow_mut()
            .apply_ready(&self.package_event_router)
    }

    pub(crate) fn complete_event_plane_owner_op(
        &self,
        completion: crate::package_event_router::EventOwnerCompletion,
    ) -> Option<crate::package_event_router::OwnerOp> {
        self.event_plane_owner_ops.borrow_mut().complete(completion)
    }

    pub(crate) fn restart_event_plane_owner_op(
        &self,
        identity: &crate::package_event_router::EventOwnerWorkId,
    ) -> Option<crate::package_event_router::EventOwnerWork> {
        self.event_plane_owner_ops.borrow_mut().restart(identity)
    }

    pub(crate) fn retire_terminal_event_plane_owner_op(
        &self,
        identity: &crate::package_event_router::EventOwnerWorkId,
    ) -> Option<crate::package_event_router::OwnerOp> {
        self.event_plane_owner_ops
            .borrow_mut()
            .retire_terminal(identity)
    }

    pub(crate) fn apply_causal_owner_ops(&self) {
        let Some(op) = self.causal_queue.take_head() else {
            return;
        };
        match self.causal_scopes.try_apply_or_wait(op) {
            crate::package_event_router::CausalWaitResult::Applied => {
                self.causal_queue.note_applied();
            }
            crate::package_event_router::CausalWaitResult::Waiting(op)
            | crate::package_event_router::CausalWaitResult::Fault(op) => {
                self.causal_queue.restore_head(op);
            }
        }
    }

    /// Enqueue one causal transition. A refused operation remains with its caller.
    pub fn admit_causal_op(&self, op: CausalOp) -> CausalAdmitResult {
        match self.reserve_causal_transition() {
            Ok(reservation) => {
                reservation.commit(op);
                CausalAdmitResult::Applied
            }
            Err(_) => CausalAdmitResult::Retry(op),
        }
    }

    fn has_family_resync_releases(&self) -> bool {
        self.entity_model_readiness().releases
    }

    pub(crate) fn causal_family_release_ready(&self) -> bool {
        self.has_family_resync_releases()
            && !self.causal_faulted()
            && self.causal_queue.has_capacity()
    }

    pub(crate) fn retry_family_resync_release(&self) {
        let Ok(reservation) = self.reserve_causal_transition() else {
            return;
        };
        let mut cursor = resync::Cursor::default();
        let mut operation = None;
        self.with_direct_entity_model(|model| {
            model.step_resync(resync::Action::Release, &mut cursor, &mut operation)
        });
        if let Some(operation) = operation {
            reservation.commit(operation);
        }
    }

    #[doc(hidden)]
    pub fn causal_operation_count(&self) -> usize {
        self.causal_queue.len()
    }

    #[doc(hidden)]
    pub fn test_family_causal_token(&self, name: &str) -> u64 {
        self.package_entities
            .lock()
            .unwrap()
            .ensure_family_token(name)
            .expect("test family token capacity")
    }

    #[doc(hidden)]
    pub fn test_store_pending_lease(&self, scope_id: u64, family: &str, seq: u64) {
        let mut model = self.package_entities.lock().unwrap();
        let family_token = model
            .ensure_family_token(family)
            .expect("test family token capacity");
        let state = model.family(family);
        state.store_pending_lease(EntityMutationLease {
            admission: None,
            scope_id,
            family_token,
            family: family.to_string(),
            generation: state.generation,
            seq,
        });
        self.entity_model_owner.update_readiness(&model);
    }

    #[cfg(test)]
    pub(crate) fn test_store_family_payload(&self, mutation: PackageEntityMutation) {
        self.package_entities
            .lock()
            .unwrap()
            .family(mutation.entity_type())
            .pending_by_seq
            .insert(mutation.snapshot_seq(), mutation);
    }

    fn next_package_entity_epoch(&self) -> Result<u64, PackageEntityCleanupError> {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .next_epoch()
    }

    fn advance_package_entity_epoch(&self) -> Result<u64, PackageEntityCleanupError> {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .advance_epoch()
    }

    #[cfg(test)]
    pub(crate) fn test_exhaust_package_entity_epochs(&self) {
        self.package_entities.lock().unwrap().epoch = u64::MAX;
    }

    pub(crate) fn package_entity_family_generation(&self, family: &str) -> Option<u64> {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .get(family)
            .map(|state| state.generation)
    }

    #[cfg(test)]
    pub(crate) fn test_stop_core_driver(&mut self) {
        self.stop_data_plane_with_release(false);
    }

    #[cfg(test)]
    pub(crate) fn test_refuse_next_owner_begins(&self, count: usize) {
        self.core_daemon.test_refuse_next_owner_begins(count);
    }

    #[cfg(test)]
    pub(crate) fn test_lose_next_owner_begins(&self, count: usize) {
        self.core_daemon.test_lose_next_owner_begins(count);
    }

    #[cfg(test)]
    pub(crate) fn test_release_session_reservation_begins(&self) -> usize {
        self.core_daemon.test_release_session_reservation_begins()
    }

    #[cfg(test)]
    pub(crate) fn test_stop_host_submissions(&mut self) {
        self.host_executor.test_stop_submissions();
    }

    #[doc(hidden)]
    pub fn test_fulfill_pending_publishes(&self) {
        self.fulfill_pending_entity_publish_requests();
    }

    #[doc(hidden)]
    pub fn test_admit_publish(
        &self,
        plugin_key: &str,
        frame: serde_json::Value,
        scope_id: Option<u64>,
    ) -> Result<PackageEntityPublishResult, String> {
        let reservation = self
            .reserve_causal_transition()
            .map_err(|_| "causal transition capacity unavailable".to_string())?;
        let prepared = prepare_publish_mutation(frame).and_then(|mutation| {
            self.entity_publish_bridge
                .prepare_registration(plugin_key, &mutation)
                .map(|registration| (mutation, registration))
        });
        let (mutation, registration) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                if let Some(scope_id) = scope_id {
                    reservation.commit(CausalOp::Release {
                        scope_id,
                        identity: LeaseIdentity::PendingEntityPublish {
                            publication_token: 0,
                        },
                    });
                }
                return Err(error);
            }
        };
        let (result, discarded, release, drain) =
            self.admit_package_entity_publish(registration, mutation, scope_id, 0, reservation);
        drop(discarded);
        let (response, receiver) = std::sync::mpsc::channel();
        assert!(
            self.package_entities
                .lock()
                .expect("package entity model lock")
                .publication
                .is_none()
        );
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .publication = Some(PublicationRetirement {
            disposed: false,
            drain,
            daemon_owned: false,
            response,
            result,
            release,
        });
        self.complete_entity_publish_disposal();
        while self.advance_entity_publish() == PublicationAdvance::Again {}
        self.finish_entity_publish_retirement();
        receiver
            .try_recv()
            .unwrap_or_else(|_| Err("publication retirement is pending".into()))
    }

    #[doc(hidden)]
    pub fn test_settle_publish(&self, family: &str, scope_id: u64, seq: u64, resync_needed: bool) {
        let result = PackageEntityPublishResult {
            ok: true,
            status: PackageEntityPublishStatus::Accepted,
            last_accepted_seq: seq,
            high_water_seq: seq,
            resync_needed,
            resync_degraded: false,
        };
        let reservation = self
            .reserve_causal_transition()
            .expect("test transition capacity");
        let mut model = self.package_entities.lock().unwrap();
        model
            .ensure_family_token(family)
            .expect("test family token capacity");
        self.settle_entity_publish_lease(
            model.family(family),
            scope_id,
            0,
            seq,
            &result,
            reservation,
        );
        model.index_resync_releases(family);
        self.entity_model_owner.update_readiness(&model);
    }

    #[doc(hidden)]
    pub fn test_store_resync_lease(&self, scope_id: u64, name: &str) {
        let mut model = self.package_entities.lock().unwrap();
        model
            .ensure_family_token(name)
            .expect("test family token capacity");
        model.family(name).remember_resync_lease(scope_id);
        model.index_resync_releases(name);
        self.entity_model_owner.update_readiness(&model);
    }

    #[must_use]
    #[doc(hidden)]
    pub fn test_resync_lease_count(&self, family: &str) -> usize {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .get(family)
            .map(|state| state.resync.leases.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn test_resync_scope_ids(&self, name: &str) -> Vec<u64> {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .families[name]
            .resync
            .leases
            .keys()
            .copied()
            .collect()
    }

    #[must_use]
    #[doc(hidden)]
    pub fn test_family_seq(&self, family: &str) -> u64 {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .get(family)
            .map(|state| state.last_accepted_seq)
            .unwrap_or(0)
    }

    #[doc(hidden)]
    pub fn test_set_family_seq(&self, family: &str, seq: u64) {
        let mut model = self.package_entities.lock().unwrap();
        let entry = model.family(family);
        entry.last_accepted_seq = seq;
        entry.high_water_seq = seq;
        self.entity_model_owner.update_readiness(&model);
    }

    #[must_use]
    #[doc(hidden)]
    pub fn test_family_exists(&self, family: &str) -> bool {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .contains_key(family)
    }

    /// Return the startup reconciliation decisions made against the core daemon registry.
    #[must_use]
    pub const fn reconciliation(&self) -> &HubSessionReconciliation {
        &self.reconciliation
    }

    /// Capture shared runtime handles for one admitted host operation.
    pub(crate) fn host_package_runtime(&self) -> HostPackageRuntime {
        HostPackageRuntime::new(self.plugin_lifecycle().clone(), self.lua_plugin_host_api())
    }

    /// Apply cleanup identities after host execution completes.
    pub(crate) fn apply_host_package_cleanup(&mut self, cleanup: HostPackageCleanup) {
        assert!(
            cleanup.unloaded_families.is_empty() || cleanup.family_epoch.is_some(),
            "family cleanup must reserve its generation boundary first"
        );
        self.event_plane_cleanup_faults.borrow_mut().extend(
            cleanup
                .event_plane_faults
                .into_iter()
                .map(|(result, error)| (Some(result), error)),
        );
        for operation in cleanup.event_plane_unloads {
            self.record_event_plane_owner_op(operation);
        }
        assert!(
            cleanup.unloaded_families.is_empty(),
            "finish family cleanup before applying the result"
        );
        if let Some(cleanup) = cleanup.last_capability_cleanup {
            self.last_capability_cleanup = Some(cleanup);
        }
    }

    pub(crate) fn begin_direct_package_entity_cleanup(
        &self,
        cleanup: &mut HostPackageCleanup,
    ) -> Result<(), PackageEntityCleanupError> {
        if !cleanup.unloaded_families.is_empty() && cleanup.family_epoch.is_none() {
            cleanup.family_epoch = Some(self.advance_package_entity_epoch()?);
        }
        Ok(())
    }

    fn direct_family_cleanup_available(&self) -> Result<(), PackageEntityCleanupError> {
        if self.direct_family_cleanup.borrow().is_some() {
            Err(PackageEntityCleanupError::Busy)
        } else {
            Ok(())
        }
    }

    /// Advance one retained cleanup for synchronous callers outside the daemon owner.
    fn step_direct_family_cleanup(&self) {
        let Some(mut cleanup) = self.direct_family_cleanup.borrow_mut().take() else {
            return;
        };
        match self.step_direct_package_entity_cleanup(&mut cleanup) {
            family_cleanup::FamilyCleanupStep::Payload(payload) => {
                drop(payload);
                self.complete_direct_package_entity_cleanup_item(&mut cleanup);
            }
            family_cleanup::FamilyCleanupStep::Complete => return,
            _ => {}
        }
        *self.direct_family_cleanup.borrow_mut() = Some(cleanup);
    }

    /// Apply a boundary checked before synchronous host execution.
    fn apply_direct_package_cleanup(&mut self, mut cleanup: HostPackageCleanup, next_epoch: u64) {
        if !cleanup.unloaded_families.is_empty() {
            self.package_entities
                .lock()
                .expect("package entity model lock")
                .epoch = next_epoch;
            cleanup.family_epoch = Some(next_epoch);
        }
        if !self.drain_direct_package_entity_cleanup(&mut cleanup) {
            let retained = HostPackageCleanup {
                family_epoch: cleanup.family_epoch,
                family_cursor: std::mem::take(&mut cleanup.family_cursor),
                unloaded_families: std::mem::take(&mut cleanup.unloaded_families),
                ..HostPackageCleanup::default()
            };
            assert!(self.direct_family_cleanup.borrow().is_none());
            *self.direct_family_cleanup.borrow_mut() = Some(retained);
        }
        self.apply_host_package_cleanup(cleanup);
    }

    /// Load an enabled package through core plugin worker mechanics.
    pub fn load_plugin_package(
        &mut self,
        registry: &PackageRegistry,
        package_name: &str,
        bundle: HubPluginRuntimeBundle,
    ) -> HubLifecycleResult<PluginKey> {
        let mut context = self.host_package_runtime();
        let result = context.load_plugin_package(registry, package_name, bundle);
        self.apply_host_package_cleanup(context.into_cleanup());
        result
    }

    /// Prepare and load an enabled local Lua package through the real Lua runtime.
    pub fn load_lua_plugin_package(
        &mut self,
        registry: &PackageRegistry,
        package_name: &str,
    ) -> Result<PluginKey, HubLuaPluginLoadError> {
        self.direct_family_cleanup_available()
            .map_err(HubLuaPluginLoadError::EntityFamilyCleanup)?;
        let next_epoch = self
            .next_package_entity_epoch()
            .map_err(HubLuaPluginLoadError::EntityFamilyCleanup)?;
        let mut context = self.host_package_runtime();
        let result = context.load_lua_plugin_package(registry, package_name);
        self.apply_direct_package_cleanup(context.into_cleanup(), next_epoch);
        result
    }

    /// Re-read and replace an enabled local Lua package through the real Lua runtime.
    pub fn reload_lua_plugin_package(
        &mut self,
        request_id: RequestId,
        registry: &PackageRegistry,
        package_name: &str,
    ) -> Result<PluginCleanupResult, HubLuaPluginLoadError> {
        let mut context = self.host_package_runtime();
        let result = context.reload_lua_plugin_package(request_id, registry, package_name);
        self.apply_host_package_cleanup(context.into_cleanup());
        result
    }

    /// Invoke a plugin handler through core plugin worker mechanics.
    #[must_use]
    pub fn invoke_plugin(&self, request: PluginInvocationRequest) -> PluginInvocationOutcome {
        let request_id = request.request_id.clone();
        let handler = request.handler.clone();
        let timeout_ms = request.timeout_ms;
        let lifecycle = self.plugin_lifecycle().clone();
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

    /// Reload an enabled package through core plugin worker cleanup and replacement.
    pub fn reload_plugin_package(
        &mut self,
        request_id: RequestId,
        registry: &PackageRegistry,
        package_name: &str,
        bundle: HubPluginRuntimeBundle,
    ) -> HubLifecycleResult<PluginCleanupResult> {
        let mut context = self.host_package_runtime();
        let result = context.reload_plugin_package(request_id, registry, package_name, bundle);
        self.apply_host_package_cleanup(context.into_cleanup());
        result
    }

    /// Unload a plugin package through core plugin worker cleanup mechanics.
    #[must_use]
    pub fn unload_plugin_package(
        &mut self,
        request_id: RequestId,
        package_name: &str,
    ) -> Result<PluginCleanupResult, PackageEntityCleanupError> {
        let next_epoch = self.next_package_entity_epoch()?;
        let mut context = self.host_package_runtime();
        let result = context.unload_plugin_package(request_id, package_name);
        self.apply_direct_package_cleanup(context.into_cleanup(), next_epoch);
        Ok(result)
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
        let mut context = self.host_package_runtime();
        let result = context.cleanup_plugin_capabilities(plugin_key);
        self.apply_host_package_cleanup(context.into_cleanup());
        result
    }

    /// Return loaded plugin MCP tool descriptors.
    #[must_use]
    pub fn list_plugin_mcp_tools(&self) -> Vec<crate::McpToolDescriptor> {
        self.plugin_lifecycle()
            .mcp_tool_descriptors()
            .into_iter()
            .filter_map(crate::mcp::mcp_descriptor_from_plugin)
            .collect()
    }

    /// Return the lifecycle before the daemon transfers its terminal ownership.
    fn plugin_lifecycle(&self) -> &HubPluginLifecycle {
        self.plugin_lifecycle
            .as_ref()
            .expect("plugin lifecycle was taken for terminal disposal")
    }

    /// Move the actual lifecycle owner to terminal disposal.
    ///
    /// The daemon must drain ordinary owners before this transfer.
    /// Normal lifecycle methods must not run after this transfer.
    #[must_use]
    pub(crate) fn take_plugin_lifecycle(&mut self) -> Option<HubPluginLifecycle> {
        self.plugin_lifecycle.take()
    }

    /// Capture queue handles without accessing the transferred lifecycle.
    pub(crate) fn terminal_plugin_bridges(&self) -> TerminalPluginBridges {
        TerminalPluginBridges {
            coordination: self.coordination_bridge.clone(),
            entity_publish: self.entity_publish_bridge.clone(),
            spawner: Arc::clone(&self.session_type_spawner),
        }
    }

    /// Return a cheap handle to the shared plugin lifecycle state.
    #[must_use]
    pub(crate) fn plugin_lifecycle_handle(&self) -> HubPluginLifecycle {
        self.plugin_lifecycle().clone()
    }

    /// Invoke a loaded plugin MCP tool through the core worker path.
    pub fn call_plugin_mcp_tool(
        &self,
        call: crate::McpCallRequest,
    ) -> Result<serde_json::Value, crate::McpToolError> {
        let request_id = RequestId(format!("mcp-tool-{}", call.name));
        let request = self.prepare_plugin_mcp_tool(call, request_id, None)?;
        Self::complete_plugin_mcp_tool(self.invoke_plugin(request).result)
    }

    /// Prepare one plugin MCP call for non-blocking worker admission.
    pub(crate) fn prepare_plugin_mcp_tool(
        &self,
        call: crate::McpCallRequest,
        request_id: RequestId,
        client_id: Option<ClientId>,
    ) -> Result<PluginInvocationRequest, crate::McpToolError> {
        let descriptor = self
            .plugin_lifecycle()
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
        Ok(PluginInvocationRequest {
            request_id,
            handler,
            timeout_ms: SESSION_TYPE_SPAWN_TIMEOUT_MS,
            context: botster_core::PluginInvocationContext {
                client_id,
                session_id: None,
                subscription_id: None,
                surface_id: None,
                origin: Some("mcp-serve".to_string()),
                metadata: None,
            },
            payload: botster_core::BoundaryJson(call.arguments),
        })
    }

    /// Convert one plugin MCP completion to the existing client result.
    pub(crate) fn complete_plugin_mcp_tool(
        result: PluginInvocationResult,
    ) -> Result<serde_json::Value, crate::McpToolError> {
        match result {
            botster_core::PluginInvocationResult::Completed(success) => {
                Ok(success.payload.map_or_else(json_null, |payload| payload.0))
            }
            botster_core::PluginInvocationResult::Failed(failure) => Err(crate::McpToolError::new(
                "plugin_tool_failed",
                failure.reason,
            )),
        }
    }

    fn fulfill_pending_session_type_spawns(&self) {
        for session_id in self.session_type_spawner.take_abandoned() {
            self.cleanup_undelivered_session_type_spawn(&PluginSessionTypeSpawned {
                session_id: session_id.clone(),
                lifecycle: String::new(),
                session_type_id: String::new(),
                context_id: format!("ctx-{session_id}"),
                context_keys: Vec::new(),
            });
        }
        while let Some(pending) = self.session_type_spawner.take_pending() {
            match self.fulfill_session_type_spawn(&pending) {
                Ok(start) => {
                    if let Ok(mut inflight) = self.inflight_plugin_core.lock() {
                        inflight.push(InflightPluginCore::SessionTypeSpawn {
                            start,
                            response: pending.response,
                        });
                    }
                }
                Err(error) => {
                    let _ = pending.response.send(Err(error));
                }
            }
        }
        self.advance_inflight_plugin_core();
    }

    /// Poll every plugin-facing Core operation and deliver finished results.
    fn advance_inflight_plugin_core(&self) {
        let Ok(mut inflight) = self.inflight_plugin_core.lock() else {
            return;
        };
        let mut retained = Vec::with_capacity(inflight.len());
        for mut entry in inflight.drain(..) {
            match &mut entry {
                InflightPluginCore::Coordination { ticket, rejected, .. } => match if rejected.is_some() {
                    CoreTicketPoll::Refused
                } else {
                    ticket.poll()
                } {
                    CoreTicketPoll::Pending => retained.push(entry),
                    CoreTicketPoll::Ready(result) => {
                        if let InflightPluginCore::Coordination { response, .. } = entry {
                            let _ = response.send(result);
                        }
                    }
                    CoreTicketPoll::Lost => {
                        if let InflightPluginCore::Coordination { response, .. } = entry {
                            let _ = response.send(crate::lua_runtime::CoordinationDelivery::Refused(
                                crate::lua_runtime::CoordinationRefusal::HelperStopped,
                            ));
                        }
                    }
                    CoreTicketPoll::Refused => {
                        if let InflightPluginCore::Coordination { response, rejected, .. } = entry {
                            let refusal = if rejected.as_ref().is_some_and(|rejected|
                                rejected.reason == crate::data_plane::driver::CoreRefusal::Stopped
                            ) {
                                crate::lua_runtime::CoordinationRefusal::HelperStopped
                            } else {
                                crate::lua_runtime::CoordinationRefusal::HelperFull
                            };
                            drop(rejected);
                            let _ = response.send(crate::lua_runtime::CoordinationDelivery::Refused(refusal));
                        }
                    }
                },
                InflightPluginCore::SessionTypeSpawn { start, .. } => {
                    match start.poll(self) {
                        PluginSpawnPoll::Pending => retained.push(entry),
                        PluginSpawnPoll::Ready(result) => {
                            let InflightPluginCore::SessionTypeSpawn { start, response } = entry
                            else {
                                continue;
                            };
                            let result = self.finish_session_type_spawn(&start, result);
                            if response.send(result.clone()).is_err()
                                && let Ok(spawned) = result
                            {
                                self.cleanup_undelivered_session_type_spawn(&spawned);
                            }
                        }
                    }
                }
            }
        }
        *inflight = retained;
    }

    pub(crate) fn validate_managed_git_request(
        &self,
        pending: &PendingManagedSessionSpawn,
    ) -> Result<ManagedGitRequest, ManagedGitError> {
        if !package_allows_managed_git_spawn(&pending.package_records, &pending.plugin_key) {
            return Err(ManagedGitError::new(
                "capability_denied",
                "plugin package lacks managed session-type spawn capability",
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
        show_session_type_for_target(
            &records,
            &state,
            &pending.target_id,
            &pending.session_type_id,
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

    pub(crate) fn spawn_prepared_managed_session(
        &self,
        pending: &PendingManagedSessionSpawn,
        prepared: &PreparedManagedWorktree,
        owner_waiter: crate::owner_identity::WaiterId,
    ) -> Result<ManagedSessionSpawnStart, ManagedGitError> {
        let session_id = generated_session_uuid()?;
        let records = pending.package_records.iter().collect::<Vec<_>>();
        let state = self.state();
        let materialized = materialize_managed_session_type(
            &self.config,
            &records,
            &state,
            &pending.session_type_id,
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
        let metadata = session_type_plugin_metadata(materialized.metadata, &pending.plugin_key);
        let spawn = SpawnSessionRequest {
            request: materialized.spawn_request,
            metadata,
        };
        let session_id = spawn.request.session_id.clone();
        let tracker = self.begin_reserve_session_for_owner(owner_waiter, session_id);
        Ok(ManagedSessionSpawnStart {
            tracker,
            context,
            waiter_id: owner_waiter,
            stage: PluginSpawnStage::Reserve,
            reservation: None,
            spawn,
            spawn_error: None,
            reserve_operation_id: None,
            context_published: false,
        })
    }

    /// Finish one managed session spawn from its Core completion.
    pub(crate) fn finish_managed_session_spawn(
        &self,
        start: &ManagedSessionSpawnStart,
        prepared: &PreparedManagedWorktree,
        result: Result<CoreSession, PluginSpawnFailure>,
    ) -> Result<PluginManagedSessionSpawned, ManagedGitError> {
        let context = &start.context;
        let outcome = result.map_err(|failure| {
            eprintln!(
                "managed_session_spawn_failed session_id={} core_error={}",
                context.session_id.0,
                managed_session_core_error_class(&failure.error)
            );
            if start.context_published {
                self.retract_spawn_context(context);
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

    pub(crate) fn cleanup_managed_session(&self, spawned: &PluginManagedSessionSpawned) {
        let session_id = SessionId(spawned.session_id.clone());
        self.shutdown_session_detached(session_id.clone());
        if let Ok(mut contexts) = self.session_contexts.lock() {
            contexts.remove(&session_id.0);
            contexts.remove(&format!("ctx-{}", session_id.0));
        }
    }

    fn fulfill_pending_plugin_requests(&self) {
        self.apply_causal_owner_ops();
        self.fulfill_pending_coordination_requests();
        self.fulfill_pending_entity_publish_requests();
        self.fulfill_pending_session_type_spawns();
    }

    fn fulfill_pending_entity_publish_requests(&self) {
        self.note_entity_publish_progress(
            self.causal_scopes.take_progress_notification(),
            self.take_causal_capacity_notification(),
        );
        self.step_entity_publish();
    }

    pub(crate) fn entity_publish_ready(&self) -> bool {
        self.entity_publish_wait.get() == PublicationWait::Ready
            && self.entity_model_readiness().publication == publication::Next::Idle
            && self.entity_publish_bridge.ready()
    }

    pub(crate) fn note_entity_publish_progress(
        &self,
        table_progress: bool,
        capacity_progress: bool,
    ) {
        let wait = self.entity_publish_wait.get();
        if table_progress && self.causal_faulted() && wait != PublicationWait::Fault {
            // The next bridge step latches the fault under the queue guard.
            self.entity_publish_wait.set(PublicationWait::Ready);
            return;
        }
        if (wait == PublicationWait::Table && table_progress)
            || (wait == PublicationWait::Capacity && capacity_progress)
        {
            self.entity_publish_wait.set(PublicationWait::Ready);
        }
    }

    /// Synchronous runtime pumping uses the same admission and retirement transitions.
    pub(crate) fn step_entity_publish(&self) {
        if self.entity_model_in_flight() {
            return;
        }
        if self
            .package_entities
            .lock()
            .expect("package entity model lock")
            .publication
            .as_ref()
            .is_some_and(|pending| pending.daemon_owned)
        {
            return;
        }
        if self
            .package_entities
            .lock()
            .expect("package entity model lock")
            .publication
            .is_none()
        {
            if let Some(payload) = self.begin_entity_publish() {
                drop(payload);
                self.complete_entity_publish_disposal();
            }
            self.finish_entity_publish_retirement();
            return;
        }
        if self.advance_entity_publish() == PublicationAdvance::Complete {
            self.finish_entity_publish_retirement();
        }
    }

    #[cfg(test)]
    pub(crate) fn test_fanout_sequence(&self, set: Option<u64>) -> u64 {
        let mut model = self
            .package_entities
            .lock()
            .expect("package entity model lock");
        let fanout = &mut model.fanout;
        if let Some(sequence) = set {
            fanout.set_next_sequence_for_test(sequence);
        }
        fanout.sequence_for_test()
    }

    /// Advance one mutation in the exact family generation retained by this publication.
    pub(crate) fn advance_entity_publish(&self) -> PublicationAdvance {
        let mut model = self
            .package_entities
            .lock()
            .expect("package entity model lock");
        let Some((name, generation)) = model
            .publication
            .as_ref()
            .and_then(|pending| pending.drain.clone())
        else {
            return PublicationAdvance::Complete;
        };
        let (status, result) = model.advance_publish(&name, generation);
        let pending = model
            .publication
            .as_mut()
            .expect("the publication remains retained");
        if let Some(result) = result {
            pending.result = Ok(result);
        }
        if status == PublicationAdvance::Complete {
            pending.drain = None;
        }
        if status != PublicationAdvance::Fault {
            self.note_package_entity_resync_changed();
        }
        self.entity_model_owner.update_readiness(&model);
        status
    }

    pub(crate) fn entity_publish_retirement_pending(&self) -> bool {
        self.entity_model_readiness().publication != publication::Next::Idle
    }

    pub(crate) fn mark_entity_publish_daemon_owned(&self) {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .publication
            .as_mut()
            .expect("daemon retains its publication")
            .daemon_owned = true;
    }

    pub(crate) fn complete_entity_publish_disposal(&self) {
        let mut model = self
            .package_entities
            .lock()
            .expect("package entity model lock");
        model
            .publication
            .as_mut()
            .expect("disposal retains its publication")
            .disposed = true;
        self.entity_model_owner.update_readiness(&model);
    }

    pub(crate) fn finish_entity_publish_retirement(&self) -> CausalTransitionStatus {
        let mut model = self
            .package_entities
            .lock()
            .expect("package entity model lock");
        let Some(pending) = model.publication.as_mut() else {
            return CausalTransitionStatus::Applied;
        };
        if !pending.disposed || pending.drain.is_some() {
            return CausalTransitionStatus::Waiting;
        }
        if pending.release.is_some() {
            let reservation = match self.reserve_causal_transition() {
                Ok(reservation) => reservation,
                Err(status) => return status,
            };
            reservation.commit(pending.release.take().unwrap());
        }
        let pending = model.publication.take().unwrap();
        let _ = pending.response.send(pending.result);
        self.entity_model_owner.update_readiness(&model);
        CausalTransitionStatus::Applied
    }

    /// The daemon must reserve Host and Owner capacity before this call.
    pub(crate) fn begin_entity_publish(&self) -> Option<PackageEntityMutation> {
        use crate::package_event_router::CausalAcquireResult;
        if !self.entity_publish_ready() {
            return None;
        }
        let selected = self.entity_publish_bridge.take_if(|pending| {
            let reservation = match self.reserve_causal_transition() {
                Ok(reservation) => reservation,
                Err(CausalTransitionStatus::Waiting) => {
                    self.entity_publish_wait.set(PublicationWait::Capacity);
                    return None;
                }
                Err(_) => {
                    self.entity_publish_bridge.retain_faulted();
                    self.entity_publish_wait.set(PublicationWait::Fault);
                    return None;
                }
            };
            let acquired = if let Some(scope_id) = pending.scope_id {
                match self.causal_scopes.try_acquire_with_admission_or_wait(
                    scope_id,
                    &pending.identity,
                    pending.mutation.admission(),
                ) {
                    CausalAcquireResult::Acquired => true,
                    CausalAcquireResult::MissingScope => false,
                    CausalAcquireResult::Waiting => {
                        self.entity_publish_wait.set(PublicationWait::Table);
                        return None;
                    }
                    CausalAcquireResult::Fault => {
                        self.entity_publish_bridge.retain_faulted();
                        self.entity_publish_wait.set(PublicationWait::Fault);
                        return None;
                    }
                }
            } else {
                true
            };
            Some((reservation, acquired))
        });
        let Some((pending, (reservation, acquired))) = selected else {
            return None;
        };
        let (result, discarded, release, drain) = if acquired {
            self.admit_package_entity_publish(
                pending.registration,
                pending.mutation,
                pending.scope_id,
                pending.token,
                reservation,
            )
        } else {
            (
                Err("causal scope no longer exists".into()),
                Some(pending.mutation),
                None,
                None,
            )
        };
        if discarded.is_some() || drain.is_some() {
            self.package_entities
                .lock()
                .expect("package entity model lock")
                .publication = Some(PublicationRetirement {
                disposed: discarded.is_none(),
                drain,
                daemon_owned: false,
                response: pending.response,
                result,
                release,
            });
        } else {
            assert!(release.is_none());
            let _ = pending.response.send(result);
        }
        self.entity_model_owner.update_readiness(
            &self
                .package_entities
                .lock()
                .expect("package entity model lock"),
        );
        discarded
    }

    fn admit_package_entity_publish(
        &self,
        registration: crate::lifecycle::EntityProviderRegistration,
        mutation: PackageEntityMutation,
        scope_id: Option<u64>,
        publication_token: u64,
        reservation: CausalReservation,
    ) -> (
        Result<PackageEntityPublishResult, String>,
        Option<PackageEntityMutation>,
        Option<CausalOp>,
        Option<(String, u64)>,
    ) {
        let pending_identity = LeaseIdentity::PendingEntityPublish { publication_token };
        let mut reservation = Some(reservation);
        let (result, discarded, drain) = match self.admit_package_entity_publish_inner(
            &registration,
            mutation,
            scope_id,
            publication_token,
            &mut reservation,
        ) {
            Ok((result, discarded, drain)) => (Ok(result), discarded, drain),
            Err((error, mutation)) => (Err(error), Some(mutation), None),
        };
        let release = scope_id
            .filter(|_| discarded.is_some())
            .map(|scope_id| CausalOp::Release {
                scope_id,
                identity: pending_identity,
            });
        (result, discarded, release, drain)
    }

    fn admit_package_entity_publish_inner(
        &self,
        registration: &crate::lifecycle::EntityProviderRegistration,
        mutation: PackageEntityMutation,
        scope_id: Option<u64>,
        publication_token: u64,
        reservation: &mut Option<CausalReservation>,
    ) -> Result<
        (
            PackageEntityPublishResult,
            Option<PackageEntityMutation>,
            Option<(String, u64)>,
        ),
        (String, PackageEntityMutation),
    > {
        let transition = self.with_direct_entity_model(|model| {
            model.admit(registration, mutation, scope_id, publication_token)
        })?;
        if let Some(op) = transition.causal {
            reservation
                .take()
                .expect("publication reserved its transition")
                .commit(op);
        }
        self.note_package_entity_resync_changed();
        Ok((transition.result, transition.discarded, transition.drain))
    }

    /// Take admitted mutations for callers outside the owner delivery path.
    #[must_use]
    pub fn take_package_entity_fanout(&self) -> Vec<PackageEntityMutation> {
        let mut mutations = Vec::new();
        while let Ok(reservation) = self.reserve_causal_transition() {
            let Some(item) = self.take_one_package_entity_fanout() else {
                break;
            };
            let (mutation, finish) = item.into_parts();
            if let Some(lease) = finish.lease.as_ref() {
                reservation.commit(self.prepare_finish_op(lease, finish.scheduled_resync));
            }
            mutations.push(mutation);
        }
        mutations
    }

    /// Take one mutation. The caller must reserve Host capacity first.
    pub(crate) fn has_package_entity_fanout(&self) -> bool {
        self.entity_model_readiness().fanout
    }

    /// Take one mutation. The caller must reserve Host capacity first.
    #[must_use]
    pub fn take_one_package_entity_fanout(&self) -> Option<TakenPackageEntityMutation> {
        self.with_direct_entity_model(|model| model.fanout.pop_first())
            .map(|item| TakenPackageEntityMutation {
                mutation: item.mutation,
                finish: PackageEntityFanoutFinish {
                    lease: item.lease,
                    scheduled_resync: false,
                },
                generation: item.generation,
            })
    }

    fn settle_entity_publish_lease(
        &self,
        family: &mut PackageEntityFamilyState,
        scope_id: u64,
        publication_token: u64,
        mutation_seq: u64,
        result: &PackageEntityPublishResult,
        reservation: CausalReservation,
    ) {
        let op =
            settle_entity_publish_op(family, scope_id, publication_token, mutation_seq, result);
        reservation.commit(op);
        if result.ok && result.resync_needed {
            family.remember_resync_lease(scope_id);
        }
    }

    /// The caller retains the finish until its transition is admitted.
    #[must_use]
    pub fn finish_package_entity_fanout(
        &self,
        finish: &PackageEntityFanoutFinish,
    ) -> CausalTransitionStatus {
        let Some(lease) = finish.lease.as_ref() else {
            return CausalTransitionStatus::Applied;
        };
        let reservation = match self.reserve_causal_transition() {
            Ok(reservation) => reservation,
            Err(status) => return status,
        };
        let op = self.prepare_finish_op(lease, finish.scheduled_resync);
        reservation.commit(op);
        CausalTransitionStatus::Applied
    }

    fn prepare_finish_op(&self, lease: &EntityMutationLease, scheduled_resync: bool) -> CausalOp {
        let (op, changed) =
            self.with_direct_entity_model(|model| model.finish(lease, scheduled_resync));
        if changed {
            self.note_package_entity_resync_changed();
        }
        op
    }

    /// Read scalar family progress without copying pending payloads.
    #[must_use]
    pub fn package_entity_family_progress(
        &self,
        entity_type: &str,
    ) -> Option<PackageEntityFamilyProgress> {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .get(entity_type)
            .map(PackageEntityFamilyState::provider_snapshot_progress)
    }

    /// Advance the provider floor without removing pending payloads.
    pub fn begin_package_entity_provider_snapshot(
        &self,
        entity_type: &str,
        snapshot_seq: u64,
    ) -> PackageEntityFamilyProgress {
        self.note_package_entity_resync_changed();
        self.with_direct_entity_model(|model| model.begin_snapshot(entity_type, snapshot_seq))
    }

    /// Take one family transition after the provider snapshot reaches its subscribers.
    /// The caller must reserve Host capacity before a payload can leave the family.
    pub fn step_package_entity_provider_snapshot(
        &self,
        entity_type: &str,
    ) -> PackageEntitySnapshotStep {
        let reservation = match self.reserve_causal_transition() {
            Ok(reservation) => reservation,
            Err(CausalTransitionStatus::Waiting) => return PackageEntitySnapshotStep::Waiting,
            Err(_) => return PackageEntitySnapshotStep::Fault,
        };
        let (generation, step) =
            self.with_direct_entity_model(|model| model.step_snapshot(entity_type));
        match step {
            PackageEntityFamilyStep::Discarded { mutation, lease } => {
                PackageEntitySnapshotStep::Discarded(TakenPackageEntityMutation {
                    mutation,
                    generation,
                    finish: PackageEntityFanoutFinish {
                        lease,
                        scheduled_resync: false,
                    },
                })
            }
            PackageEntityFamilyStep::Ready { mutation, lease } => {
                PackageEntitySnapshotStep::Ready(TakenPackageEntityMutation {
                    mutation,
                    generation,
                    finish: PackageEntityFanoutFinish {
                        lease,
                        scheduled_resync: false,
                    },
                })
            }
            PackageEntityFamilyStep::ReleaseResync {
                scope_id,
                family_token,
            } => {
                reservation.commit(CausalOp::Release {
                    scope_id,
                    identity: LeaseIdentity::ProviderResyncNeed { family_token },
                });
                PackageEntitySnapshotStep::Pending
            }
            PackageEntityFamilyStep::Complete(progress) => {
                PackageEntitySnapshotStep::Complete(progress)
            }
        }
    }

    /// Mark family resync needed (e.g. overflow or residual gap).
    ///
    /// No-ops while the family is `resync_degraded`; only [`Self::rearm_package_entity_resync`]
    /// or a new publish admission restarts a need cycle after degradation.
    pub fn mark_package_entity_resync_needed(&self, entity_type: &str) {
        self.note_package_entity_resync_changed();
        self.with_direct_entity_model(|model| model.mark_resync(entity_type));
    }

    /// Explicitly re-arm resync after a new catching-up subscription (or other
    /// progress event that must clear degradation).
    pub fn rearm_package_entity_resync(&self, entity_type: &str) {
        self.note_package_entity_resync_changed();
        self.with_direct_entity_model(|model| model.rearm_resync(entity_type));
    }

    /// Retain a resync change until the owner records the scheduling work.
    pub(crate) fn note_package_entity_resync_changed(&self) {
        self.package_entity_resync_changed.set(true);
    }

    pub(crate) fn take_package_entity_resync_notification(&self) -> bool {
        self.package_entity_resync_changed.replace(false)
    }

    pub(crate) fn package_entity_resync_next_attempt(&self, entity_type: &str) -> Option<Instant> {
        let model = self
            .package_entities
            .lock()
            .expect("package entity model lock");
        model.resync_observation(entity_type).deadline
    }

    /// Record a resync attempt; returns whether the family entered degraded.
    pub fn record_package_entity_resync_attempt(&self, entity_type: &str) -> bool {
        let degraded =
            self.with_direct_entity_model(|model| model.record_resync_attempt(entity_type));
        if degraded {
            self.release_one_degraded_package_entity_resync_lease(entity_type);
        }
        degraded
    }

    /// Release one degraded resync lease and retain any rejected causal operation.
    pub(crate) fn release_one_degraded_package_entity_resync_lease(
        &self,
        entity_type: &str,
    ) -> bool {
        let Ok(reservation) = self.reserve_causal_transition() else {
            return false;
        };
        let op =
            self.with_direct_entity_model(|model| model.take_resync_release(entity_type, true));
        let Some(op) = op else {
            return false;
        };
        reservation.commit(op);
        true
    }

    /// Drop all package entity admission state for families owned by a package.
    pub fn drop_package_entity_families_for(
        &self,
        package_name: &str,
    ) -> Result<(), PackageEntityCleanupError> {
        self.direct_family_cleanup_available()?;
        let families = self.plugin_entity_provider_families(package_name);
        self.advance_package_entity_epoch()?;
        self.drop_package_entity_families(package_name, families);
        Ok(())
    }

    fn drop_package_entity_families(&self, package_name: &str, families: BTreeSet<String>) {
        let mut cleanup = HostPackageCleanup {
            family_epoch: Some(
                self.package_entities
                    .lock()
                    .expect("package entity model lock")
                    .epoch,
            ),
            unloaded_families: vec![(package_name.to_string(), families)],
            ..HostPackageCleanup::default()
        };
        if !self.drain_direct_package_entity_cleanup(&mut cleanup) {
            assert!(self.direct_family_cleanup.borrow().is_none());
            *self.direct_family_cleanup.borrow_mut() = Some(cleanup);
        }
    }

    /// Resync attempt counter for observability (attempts field across families).
    #[must_use]
    pub fn package_entity_resync_attempt_total(&self, entity_type: &str) -> u32 {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .get(entity_type)
            .map(|family| family.resync.attempts)
            .unwrap_or(0)
    }

    /// True when admitted fanout or a non-degraded resync is waiting.
    #[must_use]
    pub fn package_entity_work_pending(&self) -> bool {
        self.package_entity_resync_still_needed()
            || self.causal_owner_ops_pending()
            || self.has_package_entity_fanout()
    }

    /// True when a family still needs resync and has not degraded.
    #[must_use]
    pub fn package_entity_resync_still_needed(&self) -> bool {
        self.entity_model_readiness().resync
    }

    /// Whether the family is currently marked resync_degraded.
    #[must_use]
    pub fn package_entity_resync_degraded(&self, entity_type: &str) -> bool {
        self.package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .get(entity_type)
            .is_some_and(|family| family.resync.degraded)
    }

    pub(crate) fn coordination_retirement(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
    ) -> crate::data_plane::driver::CoreWaiterRetirement {
        self.core_daemon.waiter_retirement(waiter_id)
    }

    #[cfg(test)]
    pub(crate) fn test_core_waiter_probe(
        &self,
    ) -> impl Fn(crate::owner_identity::WaiterId) -> bool + Send + 'static {
        let core = self.core_daemon.clone();
        move |waiter_id| core.test_retains_waiter(waiter_id)
    }

    #[cfg(test)]
    pub(crate) fn test_lua_callback_usage_probe(
        &self,
    ) -> impl Fn() -> usize + Send + 'static {
        let memory = Arc::clone(&self.lua_memory);
        move || memory.usage().1
    }

    #[cfg(test)]
    pub(crate) fn test_lua_memory(&self) -> Arc<crate::lua_memory::LuaMemoryAccount> {
        Arc::clone(&self.lua_memory)
    }

    pub(crate) fn bind_terminal_core_owner(&self) {
        self.core_daemon.bind_terminal_owner();
    }

    pub(crate) fn take_terminal_coordination_completion(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
    ) {
        self.core_daemon.take_terminal_completion(waiter_id);
    }

    pub(crate) fn submit_coordination_for_owner(
        &self,
        retirement: &crate::data_plane::driver::CoreWaiterRetirement,
        operation: PendingCoordinationOperation,
        caller: crate::lua_runtime::CoordinationCaller,
        storage: Option<crate::data_plane::driver::CoreSubmissionStorage>,
    ) -> crate::data_plane::driver::CoreSubmission<crate::lua_runtime::CoordinationReply> {
        self.core_daemon.submit_retained_for_owner(
            retirement,
            move |daemon| operation.execute(daemon),
            move || caller.claim(),
            storage,
        )
    }

    fn fulfill_pending_coordination_requests(&self) {
        while let Some(pending) = self.coordination_bridge.take_pending() {
            let (core_storage, storage) = match pending.storage {
                Some(storage) => (Some(storage.core), Some((storage.continuation, storage.disposal))),
                None => (None, None),
            };
            let caller = pending.caller;
            let submission = self.core_daemon.submit_retained(
                move |daemon| pending.operation.execute(daemon),
                move || caller.claim(),
                core_storage,
            );
            if let Ok(mut inflight) = self.inflight_plugin_core.lock() {
                inflight.push(InflightPluginCore::Coordination {
                    ticket: submission.ticket,
                    response: pending.response,
                    rejected: submission.rejected,
                    _storage: storage,
                });
            }
        }
        self.advance_inflight_plugin_core();
    }

    fn cleanup_undelivered_session_type_spawn(&self, spawned: &PluginSessionTypeSpawned) {
        let session_id = SessionId(spawned.session_id.clone());
        self.shutdown_session_detached(session_id.clone());
        if let Ok(mut contexts) = self.session_contexts.lock() {
            contexts.remove(&spawned.context_id);
            contexts.remove(&session_id.0);
        }
    }

    fn fulfill_session_type_spawn(
        &self,
        pending: &PendingSessionTypeSpawn,
    ) -> Result<SessionTypeSpawnStart, String> {
        if !package_allows_session_type_spawn(&pending.package_records, &pending.plugin_key) {
            return Err("plugin package lacks session_type_spawn capability".to_string());
        }

        let records = pending.package_records.iter().collect::<Vec<_>>();
        let state = self.state();
        let materialized = materialize_session_type(
            &self.config,
            &records,
            &state,
            &pending.session_type_id,
            pending.request.clone(),
        )
        .map_err(|error| format!("{}: {}", error.kind, error.message))?;
        drop(state);
        let context = materialized.context.clone();
        let metadata = session_type_plugin_metadata(materialized.metadata, &pending.plugin_key);
        let spawn = SpawnSessionRequest {
            request: materialized.spawn_request,
            metadata,
        };
        let session_id = spawn.request.session_id.clone();
        let retry_tokens = self.take_retained_reservations();
        let stage = if retry_tokens.is_empty() {
            PluginSpawnStage::Reserve
        } else {
            PluginSpawnStage::RetryRetained
        };
        let tracker = if matches!(stage, PluginSpawnStage::Reserve) {
            self.begin_reserve_session(session_id)
        } else {
            self.begin_release_session_reservation(retry_tokens[0].clone())
        };
        Ok(SessionTypeSpawnStart {
            tracker,
            stage,
            reservation: None,
            spawn,
            spawn_error: None,
            reserve_operation_id: None,
            retry_tokens,
            retry_keep: Vec::new(),
            context_published: false,
            context,
            session_type_id: materialized.resolved.session_type.session_type_id,
            context_id: materialized.resolved.context_id,
            context_keys: materialized.resolved.context_keys,
        })
    }

    fn finish_session_type_spawn(
        &self,
        start: &SessionTypeSpawnStart,
        result: Result<CoreSession, PluginSpawnFailure>,
    ) -> Result<PluginSessionTypeSpawned, String> {
        let context = &start.context;
        let outcome = result.map_err(|failure| {
            if start.context_published {
                self.retract_spawn_context(context);
            }
            match failure.disposition {
                Some(SessionReservationRelease::RetainedUnconfirmed) => {
                    format!("cleanup_unconfirmed: {}", failure.error)
                }
                _ => format!("session type spawn failed: {}", failure.error),
            }
        })?;
        Ok(PluginSessionTypeSpawned {
            session_id: outcome.session_id.0,
            lifecycle: session_lifecycle_label(outcome.lifecycle).to_string(),
            session_type_id: start.session_type_id.clone(),
            context_id: start.context_id.clone(),
            context_keys: start.context_keys.clone(),
        })
    }

    /// Render a plugin-owned surface route through the plugin worker path.
    pub fn render_plugin_surface(
        &self,
        package_name: &str,
        surface_id: &str,
        payload: serde_json::Value,
    ) -> Result<UiNode, crate::McpToolError> {
        let request = self.prepare_plugin_surface_render(
            package_name,
            surface_id,
            payload,
            RequestId(format!("plugin-surface-render-{package_name}-{surface_id}")),
            None,
        )?;
        self.complete_plugin_surface_render(package_name, self.invoke_plugin(request).result)
    }

    /// Prepare one plugin surface render for non-blocking worker admission.
    pub(crate) fn prepare_plugin_surface_render(
        &self,
        package_name: &str,
        surface_id: &str,
        payload: serde_json::Value,
        request_id: RequestId,
        client_id: Option<ClientId>,
    ) -> Result<PluginInvocationRequest, crate::McpToolError> {
        let descriptor = self
            .plugin_lifecycle()
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
        Ok(PluginInvocationRequest {
            request_id,
            handler,
            timeout_ms: SESSION_TYPE_SPAWN_TIMEOUT_MS,
            context: botster_core::PluginInvocationContext {
                client_id,
                session_id: None,
                subscription_id: None,
                surface_id: Some(surface_id.to_string()),
                origin: Some("local-client-api".to_string()),
                metadata: None,
            },
            payload: BoundaryJson(payload),
        })
    }

    /// Convert one plugin surface render completion to the existing client result.
    pub(crate) fn complete_plugin_surface_render(
        &self,
        package_name: &str,
        result: PluginInvocationResult,
    ) -> Result<UiNode, crate::McpToolError> {
        complete_plugin_surface_render_with_lifecycle(self.plugin_lifecycle(), package_name, result)
    }

    /// Dispatch a plugin-owned semantic UI action through the plugin worker path.
    pub fn dispatch_plugin_surface_action(
        &self,
        package_name: &str,
        request: &UiActionRequest,
    ) -> Result<UiActionResult, crate::McpToolError> {
        let invocation = self.prepare_plugin_surface_action(
            package_name,
            request,
            RequestId(format!(
                "plugin-surface-action-{package_name}-{}-{}",
                request.surface_id.0, request.action_id.0
            )),
            None,
        )?;
        self.complete_plugin_surface_action(
            package_name,
            request,
            self.invoke_plugin(invocation).result,
        )
    }

    /// Prepare one plugin surface action for non-blocking worker admission.
    pub(crate) fn prepare_plugin_surface_action(
        &self,
        package_name: &str,
        request: &UiActionRequest,
        request_id: RequestId,
        client_id: Option<ClientId>,
    ) -> Result<PluginInvocationRequest, crate::McpToolError> {
        let surface_id = &request.surface_id.0;
        let action_id = &request.action_id.0;
        let descriptor = self
            .plugin_lifecycle()
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
        Ok(PluginInvocationRequest {
            request_id,
            handler: botster_core::PluginHandlerRef {
                kind: PluginHandlerKind::UiAction,
                ..handler
            },
            timeout_ms: SESSION_TYPE_SPAWN_TIMEOUT_MS,
            context: botster_core::PluginInvocationContext {
                client_id,
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
        })
    }

    /// Convert one plugin surface action completion to the existing client result.
    pub(crate) fn complete_plugin_surface_action(
        &self,
        package_name: &str,
        request: &UiActionRequest,
        result: PluginInvocationResult,
    ) -> Result<UiActionResult, crate::McpToolError> {
        complete_plugin_surface_action_with_lifecycle(
            self.plugin_lifecycle(),
            package_name,
            request,
            result,
        )
    }

    /// Return exact entity families currently provided by one loaded package.
    #[must_use]
    pub fn plugin_entity_provider_families(&self, package_name: &str) -> BTreeSet<String> {
        self.plugin_lifecycle()
            .entity_provider_families_for(package_name)
    }

    /// Return whether an exact mapped family still has a loaded provider.
    #[must_use]
    pub fn has_plugin_entity_provider_family(&self, entity_type: &str) -> bool {
        self.plugin_lifecycle()
            .has_entity_provider_family(entity_type)
    }

    /// Query one loaded package-owned entity provider through its worker.
    pub fn plugin_entity_snapshot(
        &self,
        entity_type: &str,
        subscription_id: &str,
    ) -> Result<(u64, Vec<serde_json::Value>), crate::McpToolError> {
        let reservation = self.reserve_causal_transition().map_err(|_| {
            crate::McpToolError::new(
                "causal_scope_busy",
                "causal transition capacity unavailable",
            )
        })?;
        let (request, invocation) = self.prepare_plugin_entity_snapshot(
            entity_type,
            subscription_id,
            RequestId(format!("plugin-entity-provider-{subscription_id}")),
            None,
        )?;
        let result = self.invoke_plugin(request).result;
        if let Some((scope_id, invocation_token)) = invocation.causal_lease {
            reservation.commit(CausalOp::Release {
                scope_id,
                identity: LeaseIdentity::ProviderInFlight { invocation_token },
            });
        }
        self.complete_plugin_entity_snapshot(invocation, result)
    }

    /// Prepare a provider for synchronous callers outside the daemon owner.
    pub(crate) fn prepare_plugin_entity_snapshot(
        &self,
        entity_type: &str,
        subscription_id: &str,
        request_id: RequestId,
        client_id: Option<ClientId>,
    ) -> Result<(PluginInvocationRequest, PluginEntitySnapshotInvocation), crate::McpToolError>
    {
        let mut plan = ProviderRequestPlan::prepare(
            self.plugin_lifecycle(),
            &self.shared_view_budget(),
            entity_type,
            subscription_id,
            request_id,
            client_id,
        )?;
        let mut invocation = self.select_plugin_entity_snapshot(&plan.expected)?;
        if let Some((scope_id, invocation_token)) = invocation.causal_lease {
            if !self.causal_scopes.acquire_with_admission(
                scope_id,
                LeaseIdentity::ProviderInFlight { invocation_token },
                invocation.admission.clone(),
            ) {
                return Err(crate::McpToolError::new(
                    "causal_scope_busy",
                    "could not acquire provider causal lease",
                ));
            }
        }
        invocation.lease_acquired = true;
        plan.request.context.metadata = invocation
            .causal_lease
            .map(|(scope_id, _)| BoundaryJson(serde_json::json!({ "causal_scope_id": scope_id })));
        Ok((plan.request, invocation))
    }

    /// Retain the selected family obligation before causal acquisition.
    pub(crate) fn select_plugin_entity_snapshot(
        &self,
        expected: &SharedView<ProviderExpectation>,
    ) -> Result<PluginEntitySnapshotInvocation, crate::McpToolError> {
        let (obligation, family_generation) = {
            let mut model = self
                .package_entities
                .lock()
                .expect("package entity model lock");
            let family = model.family(expected.entity_kind.as_str());
            (family.provider_obligation(), family.generation)
        };
        self.selected_plugin_entity_snapshot(expected, family_generation, obligation.as_ref())
    }

    pub(crate) fn selected_plugin_entity_snapshot(
        &self,
        expected: &SharedView<ProviderExpectation>,
        family_generation: u64,
        obligation: Option<&(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,
    ) -> Result<PluginEntitySnapshotInvocation, crate::McpToolError> {
        let scope_id = obligation.map(|(scope_id, _)| *scope_id);
        if scope_id.is_some() && self.causal_faulted() {
            return Err(crate::McpToolError::new(
                "causal_scope_busy",
                "could not acquire provider causal lease",
            ));
        }
        let causal_lease = if let Some(scope_id) = scope_id {
            let invocation_token = self.next_provider_token.get();
            if invocation_token == 0 {
                return Err(crate::McpToolError::new(
                    "entity_provider_token_exhausted",
                    "provider invocation tokens are exhausted",
                ));
            }
            self.next_provider_token
                .set(invocation_token.checked_add(1).unwrap_or(0));
            Some((scope_id, invocation_token))
        } else {
            None
        };
        Ok(PluginEntitySnapshotInvocation {
            expected: expected.clone(),
            family_generation,
            causal_lease,
            lease_acquired: false,
            admission: obligation.and_then(|(_, admission)| admission.clone()),
        })
    }

    /// Release a prepared entity-provider lease after refused admission.
    pub(crate) fn retire_plugin_entity_snapshot(
        &self,
        invocation: &PluginEntitySnapshotInvocation,
    ) -> CausalTransitionStatus {
        if invocation.lease_acquired {
            self.release_plugin_entity_snapshot_lease(invocation.causal_lease)
        } else {
            CausalTransitionStatus::Applied
        }
    }

    /// Convert one entity-provider completion and release its causal lease.
    pub(crate) fn complete_plugin_entity_snapshot(
        &self,
        invocation: PluginEntitySnapshotInvocation,
        result: PluginInvocationResult,
    ) -> Result<(u64, Vec<serde_json::Value>), crate::McpToolError> {
        if self.package_entity_family_generation(invocation.expected_entity_kind().as_str())
            != Some(invocation.family_generation)
        {
            return Err(crate::McpToolError::new(
                "entity_provider_stale",
                "the entity family changed during the provider request",
            ));
        }
        Self::convert_plugin_entity_snapshot(invocation.expected_entity_kind(), result)
    }

    /// Convert and validate one entity-provider completion without runtime state access.
    pub(crate) fn convert_plugin_entity_snapshot(
        expected_entity_kind: &EntityKind,
        result: PluginInvocationResult,
    ) -> Result<(u64, Vec<serde_json::Value>), crate::McpToolError> {
        if let PluginInvocationResult::Failed(failure) = &result {
            if failure.kind == PluginInvocationFailureKind::CompletionTooLarge {
                return Err(crate::McpToolError::new(
                    "entity_provider_frame_too_large",
                    &failure.reason,
                ));
            }
        }
        let value = completed_plugin_payload(result, "plugin entity provider")?;
        let value = coerce_entity_frame_empty_items(value);
        let frame: EntityFrame = serde_json::from_value(value).map_err(|error| {
            crate::McpToolError::new(
                "invalid_entity_provider",
                format!("invalid entity provider frame: {error}"),
            )
        })?;
        if frame.entity_type() != expected_entity_kind {
            return Err(crate::McpToolError::new(
                "invalid_entity_provider",
                format!(
                    "entity provider returned wrong family: {}",
                    frame.entity_type().as_str()
                ),
            ));
        }
        let EntityFrame::Snapshot {
            snapshot_seq,
            items,
            ..
        } = frame
        else {
            return Err(crate::McpToolError::new(
                "invalid_entity_provider",
                "entity provider must return an authoritative whole-family snapshot",
            ));
        };
        let mut record_ids = BTreeSet::new();
        for item in &items {
            let record_id =
                EntityContract::extract_record_id(expected_entity_kind, item).map_err(|error| {
                    crate::McpToolError::new("invalid_entity_provider", error.to_string())
                })?;
            if !record_ids.insert(record_id.0.clone()) {
                return Err(crate::McpToolError::new(
                    "invalid_entity_provider",
                    format!(
                        "entity provider snapshot contains duplicate record id {}",
                        record_id.0
                    ),
                ));
            }
        }
        Ok((snapshot_seq, items))
    }

    fn release_plugin_entity_snapshot_lease(
        &self,
        causal_lease: Option<(u64, u64)>,
    ) -> CausalTransitionStatus {
        let Some((scope_id, invocation_token)) = causal_lease else {
            return CausalTransitionStatus::Applied;
        };
        let reservation = match self.reserve_causal_transition() {
            Ok(reservation) => reservation,
            Err(status) => return status,
        };
        reservation.commit(CausalOp::Release {
            scope_id,
            identity: LeaseIdentity::ProviderInFlight { invocation_token },
        });
        CausalTransitionStatus::Applied
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
        self.plugin_lifecycle().status(registry)
    }

    /// Record one package-scoped startup load failure without loading the package.
    pub(crate) fn record_startup_plugin_load_failure(
        &self,
        package_name: &str,
        error: &HubLuaPluginLoadError,
    ) {
        self.plugin_lifecycle().record_load_failure(
            package_name,
            HubPluginLoadFailure {
                code: error.code().to_string(),
                message: error.to_string(),
            },
        );
    }

    /// Return Core's authoritative read-only plugin worker snapshot.
    #[must_use]
    pub fn plugin_worker_debug_snapshot(&self) -> PluginWorkerDebugSnapshot {
        self.plugin_lifecycle().debug_snapshot()
    }

    /// Return the sanitized count of active Hub-owned timer resources.
    #[must_use]
    pub fn active_plugin_timer_resources(&self) -> usize {
        self.capability_runtime
            .lock()
            .expect("hub capability runtime lock")
            .active_timer_resource_count()
    }

    /// Return a daemon-recorded session summary.
    /// Durable daemon session records, read on the Core owner thread.
    ///
    /// The owner loop serves session listings from its projection; this
    /// ticket exists for threads outside the owner loop.
    pub fn list_sessions(&self) -> CoreTicket<Result<Vec<DaemonSession>, CoreDaemonError>> {
        self.core_daemon.submit(|daemon| daemon.list())
    }

    /// Return one bounded owner-loop observe slice.
    pub fn observe_lifecycle_slice(
        &self,
        now_seconds: u64,
        resume: Option<&ObserveLifecycleCursor>,
        budget: ObserveLifecycleBudget,
    ) -> CoreTicket<Result<ObserveLifecycleSlice, SessionLifecyclePageError>> {
        let resume = resume.cloned();
        self.core_daemon.submit(move |daemon| {
            daemon.observe_lifecycle_slice(now_seconds, resume.as_ref(), budget)
        })
    }

    /// Return one bounded observe slice and publish its owner identity.
    pub(crate) fn observe_lifecycle_slice_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        now_seconds: u64,
        resume: Option<&ObserveLifecycleCursor>,
        budget: ObserveLifecycleBudget,
    ) -> CoreTicket<Result<ObserveLifecycleSlice, SessionLifecyclePageError>> {
        let resume = resume.cloned();
        self.core_daemon.submit_for_owner(waiter_id, move |daemon| {
            daemon.observe_lifecycle_slice(now_seconds, resume.as_ref(), budget)
        })
    }

    /// Return one bounded lifecycle baseline page.
    pub fn lifecycle_baseline_page(
        &self,
        snapshot: Option<&SessionLifecycleCursor>,
        after: Option<&SessionId>,
        budget: LifecycleBaselineBudget,
    ) -> CoreTicket<Result<SessionLifecycleBaselinePage, SessionLifecyclePageError>> {
        let snapshot = snapshot.cloned();
        let after = after.cloned();
        self.core_daemon.submit(move |daemon| {
            daemon.lifecycle_baseline_page(snapshot.as_ref(), after.as_ref(), budget)
        })
    }

    /// Return one bounded baseline page and publish its owner identity.
    pub(crate) fn lifecycle_baseline_page_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        snapshot: Option<&SessionLifecycleCursor>,
        after: Option<&SessionId>,
        budget: LifecycleBaselineBudget,
    ) -> CoreTicket<Result<SessionLifecycleBaselinePage, SessionLifecyclePageError>> {
        let snapshot = snapshot.cloned();
        let after = after.cloned();
        self.core_daemon.submit_for_owner(waiter_id, move |daemon| {
            daemon.lifecycle_baseline_page(snapshot.as_ref(), after.as_ref(), budget)
        })
    }

    /// Return one bounded lifecycle journal page after a cursor.
    pub fn lifecycle_changes_page(
        &self,
        after: &SessionLifecycleCursor,
        max_changes: usize,
        max_bytes: usize,
    ) -> CoreTicket<Result<SessionLifecyclePage, SessionLifecyclePageError>> {
        let after = after.clone();
        self.core_daemon
            .submit(move |daemon| daemon.lifecycle_changes_page(&after, max_changes, max_bytes))
    }

    /// Return one bounded journal page and publish its owner identity.
    pub(crate) fn lifecycle_changes_page_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        after: &SessionLifecycleCursor,
        max_changes: usize,
        max_bytes: usize,
    ) -> CoreTicket<Result<SessionLifecyclePage, SessionLifecyclePageError>> {
        let after = after.clone();
        self.core_daemon.submit_for_owner(waiter_id, move |daemon| {
            daemon.lifecycle_changes_page(&after, max_changes, max_bytes)
        })
    }

    /// Admit ready package-event deliveries and wait for completions.
    ///
    /// Production delivery uses the owner-loop `PackageEventDelivery` slice.
    /// Tests use this helper when they do not own that loop. Each pulled copy
    /// keeps one causal scope. On every exit the copy is completed, requeued,
    /// or retired. Busy no-wait results stay in a durable retry store. The
    /// helper never drops that store when a settle-turn limit ends.
    pub fn drive_package_events_for_test(&self) -> Vec<botster_core::PluginCompletion> {
        use std::time::{Duration, Instant};

        use botster_core::{PluginCompletion, PluginInvocationClass, PluginInvocationContext};

        use crate::package_event_router::CausalAdmitResult;

        let mut pending = self
            .pending_test_event_settlements
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .drain(..)
            .collect::<Vec<_>>();
        let mut outcomes = Vec::new();
        if pending.is_empty() {
            let mut pulled = Vec::new();
            loop {
                let batch = self
                    .package_event_router
                    .pull_ready_batch(16, 64 * 1024, Instant::now(), Duration::from_secs(5))
                    .unwrap_or_default();
                if batch.is_empty() {
                    break;
                }
                pulled.extend(batch);
            }

            for delivery in pulled {
                let Some(handler) = self.package_event_handler(
                    &delivery.holder.plugin_key,
                    &delivery.owner,
                    &delivery.name,
                    &delivery.holder.handler_id,
                ) else {
                    pending.push(PendingTestEvent::Complete {
                        delivery,
                        scope: None,
                    });
                    continue;
                };
                let request_id = RequestId(format!(
                    "package-event-test-{}-{}-{}",
                    delivery.name, delivery.envelope_id, delivery.holder.handler_id
                ));
                let identity = crate::package_event_router::LeaseIdentity::EventInFlight;
                let Some(scope_id) = self.causal_scopes.mint_with_lease(Some(identity.clone()))
                else {
                    pending.push(PendingTestEvent::Requeue {
                        delivery,
                        scope: None,
                    });
                    continue;
                };

                if std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
                    && self
                        .force_plugin_admit_backpressure
                        .load(std::sync::atomic::Ordering::SeqCst)
                {
                    pending.push(PendingTestEvent::Requeue {
                        delivery,
                        scope: Some((scope_id, identity)),
                    });
                    continue;
                }

                let outcome = self.plugin_lifecycle().invoke(PluginInvocationRequest {
                    request_id,
                    handler: handler.handler,
                    timeout_ms: 1_000,
                    context: PluginInvocationContext {
                        client_id: None,
                        session_id: None,
                        subscription_id: None,
                        surface_id: None,
                        origin: Some("package-event-test".to_string()),
                        metadata: None,
                    },
                    payload: BoundaryJson(delivery.payload_json.clone()),
                });
                pending.push(PendingTestEvent::Complete {
                    delivery,
                    scope: Some((scope_id, identity)),
                });
                outcomes.push(PluginCompletion {
                    class: PluginInvocationClass::Background,
                    result: outcome.result,
                });
            }
        }

        let park = std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
            && self
                .force_park_test_events
                .load(std::sync::atomic::Ordering::SeqCst);
        if !park {
            const MAX_SETTLE_TURNS: usize = 64;
            let push_release =
                |leftover: &mut Vec<PendingTestEvent>,
                 scope: Option<(u64, crate::package_event_router::LeaseIdentity)>| {
                    let Some((scope_id, identity)) = scope else {
                        return;
                    };
                    match self.admit_causal_op(CausalOp::Release {
                        scope_id,
                        identity: identity.clone(),
                    }) {
                        CausalAdmitResult::Applied => {
                            self.apply_causal_owner_ops();
                        }
                        CausalAdmitResult::Retry(_) => {
                            leftover.push(PendingTestEvent::Release { scope_id, identity })
                        }
                    }
                };
            for _ in 0..MAX_SETTLE_TURNS {
                if pending.is_empty() {
                    break;
                }
                let mut leftover = Vec::new();
                for item in pending.drain(..) {
                    match item {
                        PendingTestEvent::Requeue { delivery, scope } => {
                            match self.package_event_router.requeue_delivery(delivery) {
                                Ok(()) => push_release(&mut leftover, scope),
                                Err((
                                    delivery,
                                    crate::package_event_router::EventPlaneStatus::ShedBusy,
                                )) => leftover.push(PendingTestEvent::Requeue {
                                    delivery: *delivery,
                                    scope,
                                }),
                                Err((delivery, _)) => leftover.push(PendingTestEvent::Complete {
                                    delivery: *delivery,
                                    scope,
                                }),
                            }
                        }
                        PendingTestEvent::Complete { delivery, scope } => {
                            match self.package_event_router.complete_pulled_delivery(delivery) {
                                Ok(()) => push_release(&mut leftover, scope),
                                Err((delivery, _)) => leftover.push(PendingTestEvent::Complete {
                                    delivery: *delivery,
                                    scope,
                                }),
                            }
                        }
                        PendingTestEvent::Release { scope_id, identity } => {
                            push_release(&mut leftover, Some((scope_id, identity)));
                        }
                    }
                }
                pending = leftover;
            }
        }
        self.pending_test_event_settlements
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(pending);
        outcomes
    }

    pub fn set_test_plugin_admit_backpressure(&self, on: bool) {
        self.force_plugin_admit_backpressure
            .store(on, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn set_test_park_package_events(&self, on: bool) {
        if std::env::var("BOTSTER_ENV").as_deref() == Ok("test") {
            self.force_park_test_events
                .store(on, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[must_use]
    pub fn test_pending_event_settlements(&self) -> usize {
        self.pending_test_event_settlements
            .lock()
            .map(|pending| pending.len())
            .unwrap_or(0)
    }

    /// Look up one exact package-event handler.
    #[must_use]
    pub fn package_event_handler(
        &self,
        plugin_key: &str,
        owner: &str,
        event_name: &str,
        handler_id: &str,
    ) -> Option<crate::lifecycle::HubPluginEventHandler> {
        self.plugin_lifecycle()
            .event_handler_for(plugin_key, owner, event_name, handler_id)
    }

    /// Admit one plugin invocation without waiting.
    #[must_use]
    pub fn try_admit_plugin(
        &self,
        class: PluginInvocationClass,
        request: PluginInvocationRequest,
    ) -> PluginAdmissionResult {
        if std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
            && self
                .force_plugin_admit_backpressure
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return PluginAdmissionResult::Backpressured {
                request_id: request.request_id,
                class,
                reason: "test-forced plugin admission backpressure".to_string(),
                backpressure: None,
            };
        }
        self.plugin_lifecycle().try_admit(class, request)
    }

    pub(crate) fn plugin_provider_admission(&self) -> (HubPluginLifecycle, Arc<AtomicBool>) {
        (
            self.plugin_lifecycle().clone(),
            Arc::clone(&self.force_plugin_admit_backpressure),
        )
    }

    pub(crate) fn try_acquire_plugin_entity_snapshot(
        &self,
        invocation: &mut PluginEntitySnapshotInvocation,
    ) -> crate::package_event_router::CausalAcquireResult {
        use crate::package_event_router::CausalAcquireResult;
        if invocation.lease_acquired {
            return CausalAcquireResult::Acquired;
        }
        if invocation.causal_lease.is_some() && self.causal_faulted() {
            return CausalAcquireResult::Fault;
        }
        if !self.causal_queue.is_empty() {
            return CausalAcquireResult::Waiting;
        }
        if invocation.causal_lease.is_none() {
            invocation.lease_acquired = true;
            return CausalAcquireResult::Acquired;
        }
        let (scope_id, invocation_token) = invocation.causal_lease.unwrap();
        let result = self.causal_scopes.try_acquire_with_admission_or_wait(
            scope_id,
            &LeaseIdentity::ProviderInFlight { invocation_token },
            invocation.admission.as_ref(),
        );
        if matches!(result, CausalAcquireResult::Acquired) {
            invocation.lease_acquired = true;
        }
        result
    }

    /// Drain previously published plugin completions without waiting.
    #[must_use]
    pub fn drain_plugin_completions(
        &self,
        max_items: usize,
        max_bytes: usize,
    ) -> PluginCompletionDrain {
        self.plugin_lifecycle()
            .drain_completions(max_items, max_bytes)
    }

    /// Install the owner-loop callback for newly published plugin completions.
    pub fn install_plugin_completion_notifier(
        &self,
        notifier: botster_core::PluginCompletionNotifier,
    ) {
        self.plugin_lifecycle()
            .install_completion_notifier(notifier);
    }

    /// Event handlers subscribed to the Hub-owned `/session` family.
    #[must_use]
    pub fn session_family_event_handlers(&self) -> Vec<crate::lifecycle::HubPluginEventHandler> {
        self.session_family_event_handlers_page(None, usize::MAX).0
    }

    /// One bounded page of `/session` family event handlers.
    #[must_use]
    pub fn session_family_event_handlers_page(
        &self,
        after_plugin_key: Option<&str>,
        max_items: usize,
    ) -> (
        Vec<crate::lifecycle::HubPluginEventHandler>,
        Option<String>,
        usize,
        bool,
    ) {
        self.plugin_lifecycle().event_handlers_for_page(
            "session_family",
            after_plugin_key,
            max_items,
        )
    }

    #[cfg(test)]
    pub fn insert_test_event_handler(&self, plugin_key: &str, event_name: &str) {
        self.plugin_lifecycle()
            .insert_test_event_handler(plugin_key, event_name);
    }

    /// Start forgetting one terminal session. Core answers with
    /// `CoreCompletion::RemoveSession`.
    pub fn begin_remove_session(&self, session_id: &SessionId) -> CoreOperationTracker {
        CoreOperationTracker::new(
            self.core_daemon
                .begin(CoreOperation::RemoveSession(session_id.clone())),
        )
    }

    pub(crate) fn begin_remove_session_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        session_id: &SessionId,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(
            self.core_daemon
                .begin_for_owner(waiter_id, CoreOperation::RemoveSession(session_id.clone())),
        )
    }

    /// Start one daemon-owned session spawn. Core answers with
    /// `CoreCompletion::Spawn`; the caller records the acknowledged spawn id
    /// once it returns the response.
    pub fn begin_spawn(
        &self,
        request: SessionSpawnRequest,
        metadata: CoreSessionMetadata,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin(CoreOperation::Spawn(
            SpawnSessionRequest { request, metadata },
        )))
    }

    pub(crate) fn begin_reserve_session(&self, session_id: SessionId) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin(CoreOperation::ReserveSession(session_id)))
    }

    pub(crate) fn begin_spawn_reserved(
        &self,
        reservation: SessionReservation,
        request: SpawnSessionRequest,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin(CoreOperation::SpawnReserved {
            reservation,
            request,
        }))
    }

    pub(crate) fn begin_release_session_reservation(
        &self,
        reservation: SessionReservation,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(
            self.core_daemon
                .begin(CoreOperation::ReleaseSessionReservation(reservation)),
        )
    }

    pub(crate) fn begin_lookup_session_reservation(
        &self,
        session_id: SessionId,
        reserve_operation_id: PendingOperationId,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin(CoreOperation::LookupSessionReservation {
            session_id,
            reserve_operation_id,
        }))
    }

    pub(crate) fn take_retained_reservations(&self) -> Vec<SessionReservation> {
        self.retained_plugin_reservations
            .lock()
            .map(|mut held| std::mem::take(&mut *held))
            .unwrap_or_default()
    }

    pub(crate) fn merge_retained_reservations(&self, tokens: Vec<SessionReservation>) {
        if let Ok(mut held) = self.retained_plugin_reservations.lock() {
            for token in tokens {
                if !held.iter().any(|existing| existing == &token) {
                    held.push(token);
                }
            }
        }
    }

    pub(crate) fn retain_reservation(&self, reservation: SessionReservation) {
        if let Ok(mut held) = self.retained_plugin_reservations.lock()
            && !held.iter().any(|existing| existing == &reservation)
        {
            held.push(reservation);
        }
    }

    #[cfg(test)]
    pub(crate) fn test_fulfill_plugin_spawns(&self) {
        self.fulfill_pending_session_type_spawns();
    }

    #[cfg(test)]
    pub(crate) fn test_plugin_spawn(
        &self,
        plugin_key: &str,
        session_type_id: &str,
        request: crate::session_types::SessionTypeRequest,
        package_records: Vec<crate::packages::PackageRecord>,
    ) -> Result<PluginSessionTypeSpawned, String> {
        let (response, receiver) = mpsc::channel();
        self.session_type_spawner
            .pending
            .lock()
            .map_err(|_| "session type spawn queue lock poisoned".to_string())?
            .push_back(PendingSessionTypeSpawn {
                plugin_key: PluginKey(plugin_key.into()),
                session_type_id: session_type_id.into(),
                request,
                package_records,
                response,
                _dispose_probe: None,
            });
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            self.fulfill_pending_session_type_spawns();
            match receiver.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Empty) => {
                    if Instant::now() >= deadline {
                        return Err("plugin session type spawn timed out".to_string());
                    }
                    thread::yield_now();
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("plugin session type spawn disconnected".to_string());
                }
            }
        }
    }

    pub(crate) fn retained_reservations(&self) -> Vec<SessionReservation> {
        self.retained_plugin_reservations
            .lock()
            .map(|held| held.clone())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn test_session_context(&self, key: &str) -> Option<crate::session_types::HubSessionContext> {
        self.session_contexts
            .lock()
            .ok()?
            .get(key)
            .cloned()
    }

    pub(crate) fn publish_spawn_context(&self, context: &HubSessionContext) -> Result<(), String> {
        let mut contexts = self
            .session_contexts
            .lock()
            .map_err(|_| "session context lock poisoned".to_string())?;
        contexts.insert(context.context_id.clone(), context.clone());
        contexts.insert(context.session_id.0.clone(), context.clone());
        Ok(())
    }

    pub(crate) fn retract_spawn_context(&self, context: &HubSessionContext) {
        let Ok(mut contexts) = self.session_contexts.lock() else {
            return;
        };
        if contexts
            .get(&context.context_id)
            .is_some_and(|stored| stored.context_id == context.context_id)
        {
            contexts.remove(&context.context_id);
        }
        if contexts
            .get(&context.session_id.0)
            .is_some_and(|stored| stored.context_id == context.context_id)
        {
            contexts.remove(&context.session_id.0);
        }
    }

    pub(crate) fn begin_spawn_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        request: SessionSpawnRequest,
        metadata: CoreSessionMetadata,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin_for_owner(
            waiter_id,
            CoreOperation::Spawn(SpawnSessionRequest { request, metadata }),
        ))
    }

    pub(crate) fn begin_reserve_session_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        session_id: SessionId,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(
            self.core_daemon
                .begin_for_owner(waiter_id, CoreOperation::ReserveSession(session_id)),
        )
    }

    pub(crate) fn begin_spawn_reserved_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        reservation: SessionReservation,
        request: SpawnSessionRequest,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin_for_owner(
            waiter_id,
            CoreOperation::SpawnReserved {
                reservation,
                request,
            },
        ))
    }

    pub(crate) fn begin_release_session_reservation_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        reservation: SessionReservation,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin_for_owner(
            waiter_id,
            CoreOperation::ReleaseSessionReservation(reservation),
        ))
    }

    pub(crate) fn begin_lookup_session_reservation_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        session_id: SessionId,
        reserve_operation_id: PendingOperationId,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin_for_owner(
            waiter_id,
            CoreOperation::LookupSessionReservation {
                session_id,
                reserve_operation_id,
            },
        ))
    }

    /// Record one session id this process already returned from a successful Spawn.
    pub fn record_acknowledged_spawn(&self, session_id: impl Into<String>) {
        self.acknowledged_spawn_ids
            .lock()
            .expect("acknowledged spawn ids mutex")
            .insert(session_id.into());
    }

    /// Drop one pending Spawn acknowledgement after the projection observed it.
    pub fn retire_acknowledged_spawn(&self, session_id: &str) {
        self.acknowledged_spawn_ids
            .lock()
            .expect("acknowledged spawn ids mutex")
            .remove(session_id);
    }

    /// Session ids this process already returned from a successful Spawn.
    #[must_use]
    pub fn acknowledged_spawn_ids(&self) -> BTreeSet<String> {
        self.acknowledged_spawn_ids
            .lock()
            .expect("acknowledged spawn ids mutex")
            .clone()
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
    /// Shared handle to the session context map for deferred completions.
    pub(crate) fn session_contexts_handle(&self) -> SharedSessionContexts {
        Arc::clone(&self.session_contexts)
    }

    pub fn session_context(&self, id: &str) -> Option<HubSessionContext> {
        self.session_contexts
            .lock()
            .expect("session contexts mutex")
            .get(id)
            .cloned()
    }

    /// Attach one route and bind its terminal adapter in one Core owner turn.
    ///
    /// The sequence is: detach the same client's previous generation when one
    /// exists, declare the adapter, attach, look up the new generation, bind
    /// the adapter. Any failure after attach detaches again so Core holds no
    /// route without an adapter. Nothing here waits on the owner thread.
    pub(crate) fn attach_and_bind_terminal(
        &self,
        plan: AttachBindPlan,
    ) -> CoreTicket<Result<TerminalSubscriptionGeneration, AttachBindFailure>> {
        self.core_daemon
            .submit(move |daemon| attach_and_bind_on_core(daemon, plan))
    }

    pub(crate) fn attach_and_bind_terminal_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        plan: AttachBindPlan,
    ) -> CoreTicket<Result<TerminalSubscriptionGeneration, AttachBindFailure>> {
        self.core_daemon.submit_for_owner(waiter_id, move |daemon| {
            attach_and_bind_on_core(daemon, plan)
        })
    }

    /// Attach one route without an adapter. Core holds the route's frames
    /// until [`Self::bind_route_adapter`] binds one (WebRTC reserved channel).
    pub(crate) fn attach_route(
        &self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    ) -> CoreTicket<Result<TerminalSubscriptionGeneration, AttachBindFailure>> {
        self.core_daemon.submit(move |daemon| {
            attach_route_on_core(daemon, client_id, session_id, subscription_id, now_seconds)
        })
    }

    pub(crate) fn attach_route_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    ) -> CoreTicket<Result<TerminalSubscriptionGeneration, AttachBindFailure>> {
        self.core_daemon.submit_for_owner(waiter_id, move |daemon| {
            attach_route_on_core(daemon, client_id, session_id, subscription_id, now_seconds)
        })
    }

    /// Bind an adapter to an attached generation. On failure Core detaches
    /// that generation so no route stays without an adapter.
    pub(crate) fn bind_route_adapter(
        &self,
        plan: BindRoutePlan,
    ) -> CoreTicket<Result<(), AttachBindFailure>> {
        self.core_daemon
            .submit(move |daemon| bind_route_on_core(daemon, plan))
    }

    /// Detach one subscription through Core's client detach path.
    pub fn detach_client(
        &self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    ) -> CoreTicket<Result<(), CoreDaemonError>> {
        self.core_daemon.submit(move |daemon| {
            daemon.detach(client_id, session_id, subscription_id, now_seconds)
        })
    }

    pub(crate) fn close_work_source(&self) -> crate::data_plane::CloseWorkSource {
        self.close_work.clone()
    }

    pub(crate) fn bind_data_plane_owner_wake(
        &self,
        sender: crate::daemon::control::message::ControlSender,
    ) {
        self.coordination_bridge.bind_owner_wake(sender.clone());
        if let Some(driver) = self.data_plane.as_ref() {
            driver.bind_owner_wake(sender);
        }
    }

    pub(crate) fn bind_host_owner_wake(
        &self,
        sender: crate::daemon::control::message::ControlSender,
    ) {
        self.causal_scopes.bind_owner_wake(sender.clone());
        self.entity_publish_bridge.bind_owner_wake(sender.clone());
        self.host_executor.bind_owner_wake(sender);
    }

    pub(crate) fn bind_managed_spawn_owner_wake(
        &self,
        sender: crate::daemon::control::message::ControlSender,
    ) {
        self.session_type_spawner.bind_managed_owner_wake(sender);
    }

    pub(crate) fn take_managed_spawn_notification(&self) -> bool {
        self.session_type_spawner
            .managed_pending
            .swap(false, Ordering::AcqRel)
    }

    pub(crate) fn take_pending_managed_spawn(&self) -> Option<PendingManagedSessionSpawn> {
        self.session_type_spawner.take_managed()
    }

    pub(crate) fn host_executor(&self) -> &crate::host_executor::HostExecutor {
        &self.host_executor
    }

    pub(crate) fn next_waiter_id(&self) -> Option<crate::owner_identity::WaiterId> {
        self.core_daemon.waiter_ids().next()
    }

    pub(crate) fn take_core_completion_notification(&self) -> bool {
        self.core_daemon.take_completion_notification()
    }

    pub(crate) fn take_owner_core_completions(
        &self,
        limit: usize,
    ) -> Vec<crate::owner_identity::OwnerWorkIdentity> {
        self.core_daemon.take_owner_completion_identities(limit)
    }

    pub(crate) fn restore_owner_core_completions(
        &self,
        identities: &[crate::owner_identity::OwnerWorkIdentity],
    ) {
        self.core_daemon
            .restore_owner_completion_identities(identities);
    }

    pub(crate) fn retire_owner_core_completion(
        &self,
        identity: crate::owner_identity::OwnerWorkIdentity,
    ) -> bool {
        self.core_daemon.retire_owner_completion(identity)
    }

    pub(crate) fn retire_owner_core_waiter(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
    ) -> usize {
        self.core_daemon.retire_owner_waiter(waiter_id)
    }

    pub(crate) fn take_data_plane_progress(&self) -> crate::data_plane::driver::DataPlaneProgress {
        self.data_plane
            .as_ref()
            .map(crate::data_plane::driver::DataPlaneDriver::take_progress)
            .unwrap_or_default()
    }

    pub(crate) fn data_plane_progress_pending(&self) -> bool {
        self.data_plane
            .as_ref()
            .is_some_and(crate::data_plane::driver::DataPlaneDriver::progress_pending)
    }

    /// Return complete inventory within the caller's retained logical byte allowance.
    /// The caller must retain the allowance until it drops the inventory.
    #[must_use]
    pub fn list_terminal_subscriptions(
        &self,
        max_logical_bytes: usize,
    ) -> CoreTicket<
        Result<
            botster_core::TerminalSubscriptionInventory,
            botster_core::TerminalSubscriptionInventoryError,
        >,
    > {
        self.core_daemon
            .submit(move |daemon| daemon.list_terminal_subscriptions(max_logical_bytes))
    }

    /// Return exact terminal generations and publish the owner identity.
    pub(crate) fn terminal_subscription_generations_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        routes: Vec<(String, String)>,
    ) -> CoreTicket<Vec<(String, String, Option<TerminalSubscriptionGeneration>)>> {
        self.core_daemon.submit_for_owner(waiter_id, move |daemon| {
            routes
                .into_iter()
                .map(|(session_id, subscription_id)| {
                    let generation = daemon.terminal_subscription_generation(
                        &SessionId(session_id.clone()),
                        &SubscriptionId(subscription_id.clone()),
                    );
                    (session_id, subscription_id, generation)
                })
                .collect()
        })
    }

    /// Detach one route for a client: the exact generation when the owner
    /// recorded one, otherwise whatever generation the client owns now.
    pub(crate) fn detach_route_exact_or_owned(
        &self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        generation: Option<TerminalSubscriptionGeneration>,
        now_seconds: u64,
    ) -> CoreTicket<Result<(), CoreDaemonError>> {
        self.core_daemon.submit(move |daemon| match generation {
            Some(generation) => daemon
                .detach_terminal_subscription(
                    client_id,
                    session_id,
                    subscription_id,
                    generation,
                    now_seconds,
                )
                .map(|_| ()),
            None => daemon.detach(client_id, session_id, subscription_id, now_seconds),
        })
    }

    pub(crate) fn detach_route_exact_or_owned_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        generation: Option<TerminalSubscriptionGeneration>,
        now_seconds: u64,
    ) -> CoreTicket<Result<(), CoreDaemonError>> {
        self.core_daemon
            .submit_for_owner(waiter_id, move |daemon| match generation {
                Some(generation) => daemon
                    .detach_terminal_subscription(
                        client_id,
                        session_id,
                        subscription_id,
                        generation,
                        now_seconds,
                    )
                    .map(|_| ()),
                None => daemon.detach(client_id, session_id, subscription_id, now_seconds),
            })
    }

    /// Detach one subscription generation without deleting a newer owner.
    pub fn detach_terminal_subscription(
        &self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        generation: TerminalSubscriptionGeneration,
        now_seconds: u64,
    ) -> CoreTicket<Result<DetachTerminalSubscriptionResult, CoreDaemonError>> {
        self.core_daemon.submit(move |daemon| {
            daemon.detach_terminal_subscription(
                client_id,
                session_id,
                subscription_id,
                generation,
                now_seconds,
            )
        })
    }
}

impl HubRuntime {
    /// Exact non-mutating registry state for one session.
    #[allow(dead_code)]
    pub(crate) fn session_registry_state(
        &self,
        session_id: &SessionId,
    ) -> CoreTicket<Result<SessionRegistryStateLookup, CoreDaemonError>> {
        let session_id = session_id.clone();
        self.core_daemon
            .submit(move |daemon| daemon.session_registry_state(&session_id))
    }

    /// Start a plain-text screen read. Core answers with
    /// `CoreCompletion::ReadScreen`.
    pub fn begin_read_screen(
        &self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin(CoreOperation::ReadScreen(
            ReadScreenRequest {
                request_id,
                session_id,
                now_seconds,
            },
        )))
    }

    pub(crate) fn begin_read_screen_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin_for_owner(
            waiter_id,
            CoreOperation::ReadScreen(ReadScreenRequest {
                request_id,
                session_id,
                now_seconds,
            }),
        ))
    }

    /// Start a mode-flags read. Core answers with `CoreCompletion::ReadModeFlags`.
    pub fn begin_read_mode_flags(
        &self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin(CoreOperation::ReadModeFlags(
            ReadModeFlagsRequest {
                request_id,
                session_id,
                now_seconds,
            },
        )))
    }

    pub(crate) fn begin_read_mode_flags_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin_for_owner(
            waiter_id,
            CoreOperation::ReadModeFlags(ReadModeFlagsRequest {
                request_id,
                session_id,
                now_seconds,
            }),
        ))
    }

    /// Start a GHOSTSNP capture for paging. Core answers with
    /// `CoreCompletion::CaptureSnapshot`; the capture counts against `owner`.
    pub fn begin_capture_snapshot(
        &self,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
        owner: CaptureOwner,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin(CoreOperation::CaptureSnapshot {
            request: CaptureSnapshotRequest {
                request_id,
                session_id,
                now_seconds,
            },
            owner,
        }))
    }

    pub(crate) fn begin_capture_snapshot_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
        owner: CaptureOwner,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin_for_owner(
            waiter_id,
            CoreOperation::CaptureSnapshot {
                request: CaptureSnapshotRequest {
                    request_id,
                    session_id,
                    now_seconds,
                },
                owner,
            },
        ))
    }

    /// Cancel one pending Core operation. `true` when it was still pending.
    pub(crate) fn cancel_core_operation(&self, id: PendingOperationId) -> CoreTicket<bool> {
        self.core_daemon.submit(move |daemon| daemon.cancel(id))
    }

    /// Release one open capture. `true` when it was open.
    pub(crate) fn release_capture(&self, capture: CaptureId) -> CoreTicket<bool> {
        self.core_daemon
            .submit(move |daemon| daemon.release_capture(&capture))
    }

    /// Read one page of an open capture. The page shares the capture buffer.
    pub fn read_snapshot_page(
        &self,
        capture: CaptureId,
        page: u32,
    ) -> CoreTicket<Result<SnapshotPage, CoreDaemonError>> {
        self.core_daemon
            .submit(move |daemon| daemon.read_snapshot_page(&capture, page))
    }

    pub(crate) fn read_snapshot_page_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        capture: CaptureId,
        page: u32,
    ) -> CoreTicket<Result<SnapshotPage, CoreDaemonError>> {
        self.core_daemon.submit_for_owner(waiter_id, move |daemon| {
            daemon.read_snapshot_page(&capture, page)
        })
    }

    /// Evaluate guarded-write readiness and inject only through the core daemon.
    pub fn guarded_write(
        &self,
        request: GuardedWriteRequest,
    ) -> CoreTicket<Result<GuardedWriteResult, CoreDaemonError>> {
        self.core_daemon
            .submit(move |daemon| daemon.guarded_write(request))
    }

    /// Publish one coordination envelope through the CoreDaemon routed-envelope router.
    pub fn publish_routed_envelope(
        &self,
        envelope: RoutedEnvelope,
    ) -> CoreTicket<Result<RoutedEnvelopePublishOutcome, CoreDaemonError>> {
        self.core_daemon.submit(move |daemon| {
            daemon.publish_routed_envelope(PublishRoutedEnvelopeRequest { envelope })
        })
    }

    /// Drain coordination envelopes for one routed target through CoreDaemon cursor semantics.
    pub fn drain_routed_envelopes(
        &self,
        target: EnvelopeTarget,
        after: Option<botster_core::EnvelopeCursor>,
        limit: usize,
    ) -> CoreTicket<Result<RoutedEnvelopeDrainOutcome, CoreDaemonError>> {
        self.core_daemon.submit(move |daemon| {
            daemon.drain_routed_envelopes(DrainRoutedEnvelopesRequest {
                target,
                after,
                limit,
            })
        })
    }

    /// Acknowledge one routed envelope delivery through CoreDaemon.
    pub fn acknowledge_routed_envelope(
        &self,
        target: EnvelopeTarget,
        envelope_id: EnvelopeId,
    ) -> CoreTicket<Result<RoutedEnvelopeDeliveryStateResult, CoreDaemonError>> {
        self.core_daemon.submit(move |daemon| {
            daemon.acknowledge_routed_envelope(AcknowledgeRoutedEnvelopeRequest {
                target,
                envelope_id,
            })
        })
    }

    /// Return one CoreDaemon routed-envelope delivery state without mutation.
    pub fn routed_envelope_delivery_state(
        &self,
        target: &EnvelopeTarget,
        envelope_id: &EnvelopeId,
    ) -> CoreTicket<RoutedEnvelopeDeliveryStateResult> {
        let target = target.clone();
        let envelope_id = envelope_id.clone();
        self.core_daemon
            .submit(move |daemon| daemon.routed_envelope_delivery_state(&target, &envelope_id))
    }

    /// Release worker-backed sessions before an intentional daemon restart.
    pub fn release_sessions_for_restart(&mut self) {
        self.stop_data_plane_with_release(true);
    }

    /// Release worker-backed sessions before an intentional hub restart.
    pub fn release_for_restart(&mut self) {
        self.release_sessions_for_restart();
    }

    fn stop_data_plane_with_release(&mut self, release_for_restart: bool) {
        if let Some(mut driver) = self.data_plane.take()
            && let Err(reason) = driver.stop_and_join(release_for_restart)
        {
            let _ = reason;
            std::process::abort();
        }
    }

    /// Scan daemon registry records for worker-backed restart/adoption evidence.
    pub fn adoption_scan(&self) -> CoreTicket<Result<Vec<SessionAdoptionReport>, CoreDaemonError>> {
        self.core_daemon.submit(|daemon| daemon.adoption_scan())
    }

    pub(crate) fn mark_session_stale(
        &self,
        session_id: &SessionId,
        now_seconds: u64,
    ) -> CoreTicket<Result<(), CoreDaemonError>> {
        let session_id = session_id.clone();
        self.core_daemon
            .submit(move |daemon| daemon.mark_stale(&session_id, now_seconds))
    }

    /// Start adopting one live worker-backed session after daemon restart.
    /// Core answers with `CoreCompletion::Adopt`.
    pub fn begin_adopt_session(&self, session_id: &SessionId) -> CoreOperationTracker {
        CoreOperationTracker::new(
            self.core_daemon
                .begin(CoreOperation::Adopt(session_id.clone())),
        )
    }

    /// Start an orderly shutdown of one session. Core answers with
    /// `CoreCompletion::ShutdownSession`.
    pub fn begin_shutdown_session(&self, session_id: SessionId) -> CoreOperationTracker {
        CoreOperationTracker::new(
            self.core_daemon
                .begin(CoreOperation::ShutdownSession(session_id)),
        )
    }

    pub(crate) fn begin_shutdown_session_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        session_id: SessionId,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(
            self.core_daemon
                .begin_for_owner(waiter_id, CoreOperation::ShutdownSession(session_id)),
        )
    }

    /// Shut down one session and forget the outcome. Used by cleanup paths
    /// whose caller cannot act on the result.
    pub(crate) fn shutdown_session_detached(&self, session_id: SessionId) {
        let tracker = self.begin_shutdown_session(session_id);
        if let Ok(mut detached) = self.detached_operations.lock() {
            detached.push(tracker);
        }
    }

    /// Startup reconciliation runs before the owner loop exists and may wait.
    fn reconcile_sessions(&mut self, now_seconds: u64) -> Result<(), CoreDaemonError> {
        self.reconciliation = HubSessionReconciliation::default();
        let reports = self
            .core_daemon
            .submit(|daemon| daemon.adoption_scan())
            .wait(STARTUP_CORE_WAIT)
            .map_err(core_bridge_error)??;
        for report in reports {
            match report.state {
                SessionAdoptionState::Adoptable => {
                    let session_id = report.record.session_id.clone();
                    let adoption_result = self
                        .core_daemon
                        .submit(move |daemon| daemon.adopt_session(&session_id, now_seconds))
                        .wait(STARTUP_CORE_WAIT)
                        .map_err(core_bridge_error)
                        .and_then(|result| result);
                    match adoption_result {
                        Ok(session) => {
                            self.reconciliation
                                .recovered_sessions
                                .push(session.session_id);
                        }
                        Err(error) if is_stale_worker_control_socket_adoption_error(&error) => {
                            self.mark_session_stale_now(&report.record.session_id, now_seconds)?;
                            self.reconciliation
                                .stale_sessions
                                .push(report.record.session_id);
                        }
                        Err(error) => return Err(error),
                    }
                }
                SessionAdoptionState::MissingProtocolEvidence => {
                    // A live worker without matching protocol evidence is
                    // incompatible with this Hub. Record it; startup refuses
                    // and deletes nothing.
                    self.reconciliation
                        .incompatible_sessions
                        .push(report.record.session_id);
                }
                SessionAdoptionState::InProcessDaemonNotRestartDurable
                // Hub always builds CoreDaemonConfig with a worker path, so this is
                // only reachable for stale records written by an older or invalid embedder.
                | SessionAdoptionState::StaleWorker { .. }
                | SessionAdoptionState::UnhealthyWorker { .. }
                | SessionAdoptionState::DuplicateWorker { .. } => {
                    self.mark_session_stale_now(&report.record.session_id, now_seconds)?;
                    self.reconciliation
                        .stale_sessions
                        .push(report.record.session_id);
                }
                SessionAdoptionState::Terminal => {
                    if report.record.state == RegistrySessionState::Running {
                        self.mark_session_stale_now(&report.record.session_id, now_seconds)?;
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

impl HubSessionTypeSpawner {
    /// Host destroys queued payloads after all scoped producers stop.
    pub(crate) fn dispose_terminal_pending(&self) -> bool {
        let queues = {
            // Poison does not remove entries from these sealed queue containers.
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut managed = self
                .managed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (std::mem::take(&mut *pending), std::mem::take(&mut *managed))
        };
        // Payload destructors run after every queue lock is released.
        drop(queues);
        self.managed_pending.store(false, Ordering::Release);
        true
    }

    #[cfg(test)]
    pub(crate) fn test_seed_terminal_pending(
        self: &Arc<Self>,
        gate: Option<mpsc::Receiver<()>>,
    ) -> mpsc::Receiver<(String, bool)> {
        assert_eq!(self.test_terminal_pending_counts(), (0, 0, false));
        let (dropped, observed) = mpsc::channel();
        let probe = |gate| TerminalSpawnerProbe {
            spawner: Arc::downgrade(self),
            dropped: dropped.clone(),
            gate,
        };
        let (response, _) = mpsc::channel();
        self.pending
            .lock()
            .unwrap()
            .push_back(PendingSessionTypeSpawn {
                plugin_key: PluginKey("terminal-spawner".into()),
                session_type_id: "plain".into(),
                request: SessionTypeRequest {
                    environment: BTreeMap::from([("PAYLOAD".into(), "spawn payload".repeat(128))]),
                    ..SessionTypeRequest::default()
                },
                package_records: Vec::new(),
                response,
                _dispose_probe: Some(probe(gate)),
            });
        let (response, _) = mpsc::channel();
        self.managed
            .lock()
            .unwrap()
            .push_back(PendingManagedSessionSpawn {
                plugin_key: PluginKey("terminal-spawner".into()),
                target_id: "terminal-target".into(),
                branch: "terminal-branch".into(),
                session_type_id: "managed".into(),
                request: ManagedSessionTypeRequest {
                    prompt: Some("managed payload".repeat(128)),
                    ..ManagedSessionTypeRequest::default()
                },
                package_records: Vec::new(),
                accepted_at: Instant::now(),
                response,
                _dispose_probe: Some(probe(None)),
            });
        self.managed_pending.store(true, Ordering::Release);
        observed
    }

    #[cfg(test)]
    pub(crate) fn test_terminal_pending_counts(&self) -> (usize, usize, bool) {
        (
            self.pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            self.managed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            self.managed_pending.load(Ordering::Acquire),
        )
    }

    pub(crate) fn new() -> Self {
        Self {
            pending: Mutex::new(VecDeque::new()),
            managed: Mutex::new(VecDeque::new()),
            managed_pending: AtomicBool::new(false),
            managed_owner: Mutex::new(None),
            abandoned: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn abandon_session_type_spawn(&self, session_id: String) {
        self.abandoned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(session_id);
    }

    #[cfg(test)]
    pub(crate) fn test_take_abandoned(&self) -> Vec<String> {
        self.take_abandoned()
    }

    #[cfg(test)]
    pub(crate) fn test_enqueue_managed_disconnected(
        &self,
        plugin_key: PluginKey,
        target_id: String,
        branch: String,
        session_type_id: String,
        request: ManagedSessionTypeRequest,
        package_records: Vec<PackageRecord>,
    ) {
        let (response, receiver) = mpsc::channel();
        drop(receiver);
        self.managed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(PendingManagedSessionSpawn {
                plugin_key,
                target_id,
                branch,
                session_type_id,
                request,
                package_records,
                accepted_at: Instant::now(),
                response,
                _dispose_probe: None,
            });
        self.publish_managed_spawn();
    }

    fn take_abandoned(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .abandoned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    fn bind_managed_owner_wake(&self, sender: crate::daemon::control::message::ControlSender) {
        let mut owner = self
            .managed_owner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *owner = Some(sender.clone());
        let pending = self.managed_pending.load(Ordering::Acquire);
        drop(owner);
        if pending {
            let _ = sender.try_send(
                crate::daemon::control::message::ControlMessage::ManagedSessionSpawnQueued,
            );
        }
    }

    fn publish_managed_spawn(&self) {
        self.managed_pending.store(true, Ordering::Release);
        if let Some(owner) = self
            .managed_owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            let _ = owner.try_send(
                crate::daemon::control::message::ControlMessage::ManagedSessionSpawnQueued,
            );
        }
    }

    /// Queue a session-type spawn for the hub owner and wait for its result.
    pub fn spawn(
        &self,
        plugin_key: &PluginKey,
        session_type_id: &str,
        request: SessionTypeRequest,
        package_records: Vec<PackageRecord>,
    ) -> Result<PluginSessionTypeSpawned, String> {
        let (response, receiver) = mpsc::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| "session-type spawn queue lock poisoned".to_string())?;
            pending.push_back(PendingSessionTypeSpawn {
                plugin_key: plugin_key.clone(),
                session_type_id: session_type_id.to_string(),
                request,
                package_records,
                response,
                #[cfg(test)]
                _dispose_probe: None,
            });
        }

        receiver
            .recv_timeout(Duration::from_millis(SESSION_TYPE_SPAWN_TIMEOUT_MS))
            .map_err(|_| "session-type spawn did not complete before timeout".to_string())?
    }

    fn take_pending(&self) -> Option<PendingSessionTypeSpawn> {
        self.pending
            .lock()
            .expect("session-type spawn queue lock")
            .pop_front()
    }

    /// Queue the one atomic managed-worktree/session spawn operation.
    pub fn ensure_worktree_and_spawn(
        &self,
        plugin_key: &PluginKey,
        target_id: &str,
        branch: &str,
        session_type_id: &str,
        request: ManagedSessionTypeRequest,
        package_records: Vec<PackageRecord>,
    ) -> Result<PluginManagedSessionSpawned, ManagedGitError> {
        if !package_allows_managed_git_spawn(&package_records, plugin_key) {
            return Err(ManagedGitError::new(
                "capability_denied",
                "plugin package lacks managed session-type spawn capability",
            ));
        }
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
            session_type_id: session_type_id.to_string(),
            request,
            package_records,
            accepted_at: Instant::now(),
            response,
            #[cfg(test)]
            _dispose_probe: None,
        });
        drop(managed);
        self.publish_managed_spawn();
        receiver
            .recv_timeout(Duration::from_millis(SESSION_TYPE_SPAWN_TIMEOUT_MS))
            .map_err(|_| {
                ManagedGitError::new(
                    "ensure_timed_out",
                    "managed session spawn did not complete before timeout",
                )
            })?
    }

    fn take_managed(&self) -> Option<PendingManagedSessionSpawn> {
        let mut pending = self
            .managed
            .lock()
            .expect("managed session spawn queue lock");
        let result = pending.pop_front();
        let has_more = !pending.is_empty();
        drop(pending);
        if has_more {
            self.publish_managed_spawn();
        }
        result
    }
}

fn managed_session_core_error_class(error: &CoreDaemonError) -> &'static str {
    match error {
        CoreDaemonError::SessionReservation(refusal) => match refusal {
            SessionReservationRefusal::Occupied => "session_reservation.occupied",
            SessionReservationRefusal::Busy => "session_reservation.busy",
            SessionReservationRefusal::Unsupported => "session_reservation.unsupported",
            SessionReservationRefusal::IdentityExhausted => {
                "session_reservation.identity_exhausted"
            }
            SessionReservationRefusal::Unavailable => "session_reservation.unavailable",
            SessionReservationRefusal::InvalidToken => "session_reservation.invalid_token",
            SessionReservationRefusal::Capacity => "session_reservation.capacity",
        },
        CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
            MultiplexerEngineError::Runtime(runtime_error),
        ))
        | CoreDaemonError::Engine(ManagedSessionRuntimeError::Runtime(runtime_error)) => {
            match runtime_error.kind {
                SessionRuntimeErrorKind::SpawnFailed => "runtime.spawn_failed",
                SessionRuntimeErrorKind::SessionNotFound => "runtime.session_not_found",
                SessionRuntimeErrorKind::InputFailed => "runtime.input_failed",
                SessionRuntimeErrorKind::OutputFailed => "runtime.output_failed",
                SessionRuntimeErrorKind::ShutdownFailed => "runtime.shutdown_failed",
                SessionRuntimeErrorKind::CleanupFailed => "runtime.cleanup_failed",
            }
        }
        CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
            MultiplexerEngineError::SessionAlreadyExists { .. },
        )) => "engine.multiplexer.session_already_exists",
        CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
            MultiplexerEngineError::UnknownSession { .. },
        )) => "engine.multiplexer.unknown_session",
        CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
            MultiplexerEngineError::MetadataTooLarge,
        )) => "engine.multiplexer.metadata_too_large",
        CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
            MultiplexerEngineError::ReservedSpawn(ReservedSessionSpawnError::Refused(_)),
        )) => "engine.multiplexer.reserved_spawn.refused",
        CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
            MultiplexerEngineError::ReservedSpawn(ReservedSessionSpawnError::Admitted(_)),
        )) => "engine.multiplexer.reserved_spawn.admitted",
        CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
            MultiplexerEngineError::InstallationAfterLaunch(_),
        )) => "engine.multiplexer.installation_after_launch",
        CoreDaemonError::Engine(ManagedSessionRuntimeError::UnsupportedSessionRequest {
            ..
        }) => "engine.unsupported_session_request",
        CoreDaemonError::Engine(ManagedSessionRuntimeError::TerminalBackendConstruction {
            ..
        }) => "engine.terminal_backend_construction",
        CoreDaemonError::Engine(ManagedSessionRuntimeError::TerminalBackendOperation {
            ..
        }) => "engine.terminal_backend_operation",
        CoreDaemonError::Registry(_) => "registry",
        CoreDaemonError::UnknownSession(_) => "unknown_session",
        CoreDaemonError::SessionNotReadable(_) => "session_not_readable",
        CoreDaemonError::MissingWorkerPath => "missing_worker_path",
        CoreDaemonError::Shutdown => "shutdown",
        CoreDaemonError::WakePump(_) => "wake_pump",
        CoreDaemonError::MissingScreenResponse(_) => "missing_screen_response",
        CoreDaemonError::MissingModeFlagsResponse(_) => "missing_mode_flags_response",
        CoreDaemonError::ControlPlaneFailed(_) => "control_plane_failed",
        CoreDaemonError::ExplicitResizeBusy(_) => "explicit_resize_busy",
        CoreDaemonError::PendingLimit(_) => "pending_limit",
        CoreDaemonError::DeadlineExpired => "deadline_expired",
        CoreDaemonError::Cancelled => "cancelled",
        CoreDaemonError::WorkerLinkFailed(_) => "worker_link_failed",
        CoreDaemonError::UnknownCapture(_) => "unknown_capture",
        CoreDaemonError::SnapshotPageOutOfRange { .. } => "snapshot_page_out_of_range",
        CoreDaemonError::BindTerminalAdapter(error) => match error {
            BindTerminalAdapterError::BindBeforeAttach { .. } => {
                "bind_terminal_adapter.bind_before_attach"
            }
            BindTerminalAdapterError::UnknownSubscription { .. } => {
                "bind_terminal_adapter.unknown_subscription"
            }
            BindTerminalAdapterError::StaleGeneration { .. } => {
                "bind_terminal_adapter.stale_generation"
            }
            BindTerminalAdapterError::AlreadyBound { .. } => "bind_terminal_adapter.already_bound",
            BindTerminalAdapterError::ControlPlaneFailed { .. } => {
                "bind_terminal_adapter.control_plane_failed"
            }
        },
    }
}

fn package_allows_session_type_spawn(
    package_records: &[PackageRecord],
    plugin_key: &PluginKey,
) -> bool {
    package_records.iter().any(|record| {
        record.manifest.name == plugin_key.0
            && matches!(record.state, PackageState::Enabled)
            && record.manifest.capabilities.iter().any(|capability| {
                capability.surface == botster_core::CapabilitySurface::SessionActions
                    && capability.scope.as_deref() == Some("session_type_spawn")
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
                    && capability.scope.as_deref() == Some("session_type_managed_git_spawn")
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

fn session_type_plugin_metadata(
    mut metadata: CoreSessionMetadata,
    plugin_key: &PluginKey,
) -> CoreSessionMetadata {
    metadata
        .entries
        .insert("client".to_string(), format!("plugin:{}", plugin_key.0));
    metadata
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

pub(crate) fn complete_plugin_surface_render_with_lifecycle(
    lifecycle: &HubPluginLifecycle,
    package_name: &str,
    result: PluginInvocationResult,
) -> Result<UiNode, crate::McpToolError> {
    let value = completed_plugin_payload(result, "plugin surface render")?;
    let node: UiNode = serde_json::from_value(value).map_err(|error| {
        crate::McpToolError::new("invalid_surface", format!("invalid plugin UiNode: {error}"))
    })?;
    validate_plugin_surface_node(&node, &lifecycle.entity_provider_families_for(package_name))?;
    Ok(node)
}

pub(crate) fn complete_plugin_surface_action_with_lifecycle(
    lifecycle: &HubPluginLifecycle,
    package_name: &str,
    request: &UiActionRequest,
    result: PluginInvocationResult,
) -> Result<UiActionResult, crate::McpToolError> {
    let value = completed_plugin_payload(result, "plugin surface action")?;
    let result: UiActionResult = serde_json::from_value(value).map_err(|error| {
        crate::McpToolError::new(
            "invalid_action_result",
            format!("invalid plugin UiActionResult: {error}"),
        )
    })?;
    validate_plugin_surface_action_result(
        &result,
        request,
        &lifecycle.entity_provider_families_for(package_name),
    )?;
    Ok(result)
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
    /// Explicit Hub memory policy failed validation.
    Config(crate::config::HubConfigError),
    /// Core daemon operation failed.
    CoreDaemon(CoreDaemonError),
    /// The Hub capability runtime could not open its plugin database.
    Capability(botster_core::CapabilityRuntimeError),
    /// Registry records name live workers whose protocol evidence does not
    /// match this Hub. Startup stops; nothing is terminated or deleted.
    IncompatibleWorkers { sessions: Vec<String> },
    /// Durable hub state failed to load.
    State(HubStateStoreError),
    /// Credential provider or persisted credential references failed validation.
    Credentials(CredentialPolicyError),
}

impl fmt::Display for HubRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "{error}"),
            Self::CoreDaemon(error) => write!(formatter, "{error}"),
            Self::Capability(error) => write!(formatter, "{error}"),
            Self::IncompatibleWorkers { sessions } => write!(
                formatter,
                "incompatible session workers: {}; stop them before starting this Hub",
                sessions.join(",")
            ),
            Self::State(error) => write!(formatter, "{error}"),
            Self::Credentials(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for HubRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::CoreDaemon(error) => Some(error),
            Self::Capability(error) => Some(error),
            Self::IncompatibleWorkers { .. } => None,
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
    EventPlane(EventPlaneStatus),
    EventPlaneCleanup,
    EntityFamilyCleanup(PackageEntityCleanupError),
}

impl HubLuaPluginLoadError {
    pub(crate) const fn is_package_scoped_startup_failure(&self) -> bool {
        match self {
            Self::Package(_) | Self::Lua(_) | Self::Lifecycle(_) => true,
            Self::EventPlaneCleanup | Self::EntityFamilyCleanup(_) => false,
            // List package failures explicitly. A new event-plane status must
            // stop startup until code classifies it as package-scoped.
            Self::EventPlane(status) => matches!(
                status,
                EventPlaneStatus::RejectedUndeclared
                    | EventPlaneStatus::RejectedForeign
                    | EventPlaneStatus::RejectedInvalid
                    | EventPlaneStatus::RejectedOversize
                    | EventPlaneStatus::RejectedWildcard
                    | EventPlaneStatus::RejectedCausalScope
                    | EventPlaneStatus::RejectedAudience
            ),
        }
    }

    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::Package(_) => "package_policy_rejected",
            Self::Lua(_) => "lua_load_failed",
            Self::Lifecycle(_) => "plugin_lifecycle_rejected",
            Self::EventPlane(status) => status.as_str(),
            Self::EventPlaneCleanup => "event_plane_cleanup_failed",
            Self::EntityFamilyCleanup(PackageEntityCleanupError::GenerationExhausted) => {
                "entity_family_generation_exhausted"
            }
            Self::EntityFamilyCleanup(PackageEntityCleanupError::Busy) => {
                "entity_family_cleanup_busy"
            }
        }
    }
}

impl fmt::Display for HubLuaPluginLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Package(error) => write!(formatter, "{error:?}"),
            Self::Lua(error) => write!(formatter, "{error}"),
            Self::Lifecycle(error) => write!(formatter, "{error:?}"),
            Self::EventPlane(status) => write!(formatter, "{}", status.as_str()),
            Self::EventPlaneCleanup => {
                formatter.write_str("event router cleanup requires recovery")
            }
            Self::EntityFamilyCleanup(PackageEntityCleanupError::GenerationExhausted) => {
                formatter.write_str("entity family cleanup exhausted generation identifiers")
            }
            Self::EntityFamilyCleanup(PackageEntityCleanupError::Busy) => {
                formatter.write_str("a previous entity family cleanup remains owned")
            }
        }
    }
}

impl Error for HubLuaPluginLoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Package(_) => None,
            Self::Lua(error) => Some(error),
            Self::Lifecycle(_) => None,
            Self::EventPlane(_) => None,
            Self::EventPlaneCleanup | Self::EntityFamilyCleanup(_) => None,
        }
    }
}

struct PendingEventPlaneReplace {
    contracts: Vec<crate::package_event_router::EmittedContract>,
    subscriptions: Vec<EventSubscription>,
}

/// One bounded transition from the provider floor to pending mutation delivery.
pub enum PackageEntitySnapshotStep {
    Waiting,
    Fault,
    Discarded(TakenPackageEntityMutation),
    Ready(TakenPackageEntityMutation),
    Pending,
    Complete(PackageEntityFamilyProgress),
}

/// One mutation and its separate completion lease.
pub struct TakenPackageEntityMutation {
    pub(crate) generation: u64,
    pub mutation: PackageEntityMutation,
    finish: PackageEntityFanoutFinish,
}

impl TakenPackageEntityMutation {
    pub fn into_parts(self) -> (PackageEntityMutation, PackageEntityFanoutFinish) {
        (self.mutation, self.finish)
    }
}

/// The owner retains this lease while a Host worker owns the mutation.
pub struct PackageEntityFanoutFinish {
    lease: Option<EntityMutationLease>,
    pub scheduled_resync: bool,
}

impl PackageEntityFanoutFinish {
    pub(crate) fn take_terminal_family(&mut self) -> Option<String> {
        self.lease
            .as_mut()
            .map(|lease| std::mem::take(&mut lease.family))
    }
}

fn settle_entity_publish_op(
    family: &PackageEntityFamilyState,
    scope_id: u64,
    publication_token: u64,
    mutation_seq: u64,
    result: &PackageEntityPublishResult,
) -> CausalOp {
    let pending = LeaseIdentity::PendingEntityPublish { publication_token };
    if !result.ok {
        return CausalOp::Release {
            scope_id,
            identity: pending,
        };
    }
    let mut next = [None, None, None];
    if matches!(
        result.status,
        PackageEntityPublishStatus::Accepted | PackageEntityPublishStatus::PendingGap
    ) {
        next[0] = Some(LeaseIdentity::AdmittedEntityMutation {
            family_token: family
                .causal_token
                .expect("scoped publication has a family token"),
            seq: mutation_seq,
        });
    }
    if result.resync_needed {
        next[1] = Some(LeaseIdentity::ProviderResyncNeed {
            family_token: family
                .causal_token
                .expect("scoped publication has a family token"),
        });
    }
    if next.iter().all(Option::is_none) {
        CausalOp::Release {
            scope_id,
            identity: pending,
        }
    } else {
        CausalOp::Transfer {
            scope_id,
            from: pending,
            to: next,
        }
    }
}

/// Bound on Core waits that run before the owner loop exists (startup
/// reconciliation) or on threads that never serve it (in-process CLI).
/// The production `CoreTicket::wait` callers are startup reconciliation and
/// its stale-record repair. `CoreOperationTracker::wait` remains for the CLI
/// and test threads. The daemon owner uses only nonblocking `poll` calls.
pub(crate) const STARTUP_CORE_WAIT: Duration = Duration::from_secs(30);

/// Map a lost or timed-out bridge wait onto the Core error surface.
/// Core-typed view of a bridge outcome that never reached Core.
///
/// A refused admission is a pending-operation limit on the Hub side of the
/// bridge; a stopped or timed-out bridge reads as shutdown.
pub(crate) fn core_bridge_error(error: CoreTicketError) -> CoreDaemonError {
    match error {
        CoreTicketError::Timeout | CoreTicketError::DriverStopped => CoreDaemonError::Shutdown,
        CoreTicketError::Overloaded => {
            CoreDaemonError::PendingLimit(botster_core_daemon::PendingLimitKind::Spawns)
        }
    }
}

/// One Core operation from `begin` to its completion.
///
/// Stage one waits for the pending id on the begin ticket; stage two waits
/// for the matching keyed [`CoreCompletion`]. Neither stage blocks.
#[derive(Debug)]
pub struct CoreOperationTracker {
    stage: CoreOperationStage,
}

#[derive(Debug)]
enum CoreOperationStage {
    Begin {
        ticket: CoreTicket<Result<PendingOperationId, CoreDaemonError>>,
        completion: CoreTicket<CoreCompletion>,
    },
    Pending {
        id: PendingOperationId,
        completion: CoreTicket<CoreCompletion>,
    },
    Done,
}

impl CoreOperationTracker {
    pub(crate) fn new(ticket: CoreOperationTicket) -> Self {
        let (ticket, completion) = ticket.into_parts();
        Self {
            stage: CoreOperationStage::Begin { ticket, completion },
        }
    }

    /// Pending id once `begin` returned it.
    #[must_use]
    pub fn pending_id(&self) -> Option<PendingOperationId> {
        match self.stage {
            CoreOperationStage::Pending { id, .. } => Some(id),
            _ => None,
        }
    }

    /// Non-blocking progress. `Ready(Err)` carries a `begin` rejection or a
    /// completion error; `Ready(Ok)` carries the completion.
    pub fn poll(
        &mut self,
        _runtime: &HubRuntime,
    ) -> CoreTicketPoll<Result<CoreCompletion, CoreDaemonError>> {
        self.poll_without_reaping()
    }

    fn poll_without_reaping(&mut self) -> CoreTicketPoll<Result<CoreCompletion, CoreDaemonError>> {
        if let CoreOperationStage::Begin { ticket, .. } = &mut self.stage {
            match ticket.poll() {
                CoreTicketPoll::Pending => return CoreTicketPoll::Pending,
                CoreTicketPoll::Lost => {
                    self.stage = CoreOperationStage::Done;
                    return CoreTicketPoll::Lost;
                }
                CoreTicketPoll::Refused => {
                    self.stage = CoreOperationStage::Done;
                    return CoreTicketPoll::Refused;
                }
                CoreTicketPoll::Ready(Err(error)) => {
                    self.stage = CoreOperationStage::Done;
                    return CoreTicketPoll::Ready(Err(error));
                }
                CoreTicketPoll::Ready(Ok(id)) => {
                    let previous = std::mem::replace(&mut self.stage, CoreOperationStage::Done);
                    let CoreOperationStage::Begin { completion, .. } = previous else {
                        unreachable!("the Core operation is in its begin phase");
                    };
                    self.stage = CoreOperationStage::Pending { id, completion };
                }
            }
        }
        match &mut self.stage {
            CoreOperationStage::Pending { completion, .. } => match completion.poll() {
                CoreTicketPoll::Ready(completion) => {
                    self.stage = CoreOperationStage::Done;
                    CoreTicketPoll::Ready(Ok(completion))
                }
                CoreTicketPoll::Pending => CoreTicketPoll::Pending,
                CoreTicketPoll::Lost => {
                    self.stage = CoreOperationStage::Done;
                    CoreTicketPoll::Lost
                }
                CoreTicketPoll::Refused => {
                    self.stage = CoreOperationStage::Done;
                    CoreTicketPoll::Refused
                }
            },
            CoreOperationStage::Done => CoreTicketPoll::Lost,
            CoreOperationStage::Begin { .. } => CoreTicketPoll::Pending,
        }
    }

    /// Bounded blocking completion for threads that do not serve the owner loop.
    pub fn wait(
        mut self,
        runtime: &HubRuntime,
        timeout: Duration,
    ) -> Result<CoreCompletion, CoreDaemonError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.poll(runtime) {
                CoreTicketPoll::Ready(result) => return result,
                CoreTicketPoll::Lost => {
                    return Err(core_bridge_error(CoreTicketError::DriverStopped));
                }
                CoreTicketPoll::Refused => {
                    return Err(core_bridge_error(CoreTicketError::Overloaded));
                }
                CoreTicketPoll::Pending => {
                    if Instant::now() >= deadline {
                        return Err(core_bridge_error(CoreTicketError::Timeout));
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }
}

/// Plugin-facing Core work the owner polls between plugin invocations.
enum InflightPluginCore {
    Coordination {
        ticket: crate::data_plane::driver::ChargedCoreTicket<crate::lua_runtime::CoordinationReply>,
        response: crate::lua_runtime::CoordinationReplySender,
        rejected: Option<crate::data_plane::driver::CoreRejectedRequest>,
        _storage: Option<(crate::lua_memory::LuaCallbackCharge, crate::lua_memory::LuaCallbackCharge)>,
    },
    SessionTypeSpawn {
        start: SessionTypeSpawnStart,
        response: mpsc::Sender<Result<PluginSessionTypeSpawned, String>>,
    },
}

enum PluginSpawnStage {
    RetryRetained,
    Reserve,
    Lookup,
    SpawnReserved,
    Release,
}

pub(crate) struct PluginSpawnFailure {
    pub error: CoreDaemonError,
    pub disposition: Option<SessionReservationRelease>,
}

pub(crate) enum PluginSpawnPoll {
    Pending,
    Ready(Result<CoreSession, PluginSpawnFailure>),
}

/// One session-type spawn in flight on the Core owner thread.
struct SessionTypeSpawnStart {
    tracker: CoreOperationTracker,
    stage: PluginSpawnStage,
    reservation: Option<SessionReservation>,
    spawn: SpawnSessionRequest,
    spawn_error: Option<CoreDaemonError>,
    reserve_operation_id: Option<PendingOperationId>,
    retry_tokens: Vec<SessionReservation>,
    retry_keep: Vec<SessionReservation>,
    context_published: bool,
    context: HubSessionContext,
    session_type_id: String,
    context_id: String,
    context_keys: Vec<String>,
}

fn spawn_fail(
    error: CoreDaemonError,
    disposition: Option<SessionReservationRelease>,
) -> PluginSpawnPoll {
    PluginSpawnPoll::Ready(Err(PluginSpawnFailure { error, disposition }))
}

impl SessionTypeSpawnStart {
    fn poll(&mut self, runtime: &HubRuntime) -> PluginSpawnPoll {
        loop {
            if let Some(pending_id) = self.tracker.pending_id()
                && matches!(self.stage, PluginSpawnStage::Reserve)
            {
                self.reserve_operation_id = Some(pending_id);
            }
            match self.stage {
                PluginSpawnStage::RetryRetained => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused
                    | CoreTicketPoll::Lost
                    | CoreTicketPoll::Ready(Err(_)) => {
                        if !self.retry_tokens.is_empty() {
                            self.retry_keep.push(self.retry_tokens.remove(0));
                        }
                        self.continue_retry_or_reserve(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                        result,
                        ..
                    })) => {
                        let held = self.retry_tokens.remove(0);
                        match result {
                            Ok(SessionReservationRelease::Released) => {}
                            Ok(_) | Err(_) => self.retry_keep.push(held),
                        }
                        self.continue_retry_or_reserve(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::Reserve => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
                        return spawn_fail(core_bridge_error(CoreTicketError::Overloaded), None);
                    }
                    CoreTicketPoll::Lost => {
                        let Some(reserve_id) = self.reserve_operation_id else {
                            return spawn_fail(CoreDaemonError::Shutdown, None);
                        };
                        self.tracker = runtime.begin_lookup_session_reservation(
                            self.spawn.request.session_id.clone(),
                            reserve_id,
                        );
                        self.stage = PluginSpawnStage::Lookup;
                    }
                    CoreTicketPoll::Ready(Err(error)) => {
                        return spawn_fail(error, None);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession { result, .. })) => {
                        match result {
                            Ok(reserved) => {
                                self.reservation = Some(reserved.clone());
                                if runtime.publish_spawn_context(&self.context).is_err() {
                                    self.spawn_error = Some(CoreDaemonError::Shutdown);
                                    self.release_or_retain(runtime);
                                    continue;
                                }
                                self.context_published = true;
                                self.tracker = runtime
                                    .begin_spawn_reserved(reserved, self.spawn.clone());
                                self.stage = PluginSpawnStage::SpawnReserved;
                            }
                            Err(error) => return spawn_fail(error, None),
                        }
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::Lookup => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
                        return spawn_fail(core_bridge_error(CoreTicketError::Overloaded), None);
                    }
                    CoreTicketPoll::Lost | CoreTicketPoll::Ready(Err(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::LookupSessionReservation {
                        result,
                        ..
                    })) => match result {
                        Ok(Some(reserved)) => {
                            self.reservation = Some(reserved.clone());
                            self.spawn_error = Some(CoreDaemonError::Shutdown);
                            self.tracker =
                                runtime.begin_release_session_reservation(reserved);
                            self.stage = PluginSpawnStage::Release;
                        }
                        Ok(None) => {
                            return spawn_fail(CoreDaemonError::Shutdown, None);
                        }
                        Err(error) => return spawn_fail(error, None),
                    },
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::SpawnReserved => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
                        self.spawn_error = Some(core_bridge_error(CoreTicketError::Overloaded));
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Lost => {
                        self.spawn_error = Some(CoreDaemonError::Shutdown);
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Ready(Err(error)) => {
                        self.spawn_error = Some(error);
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                        result: ReservedSpawnResult::Installed { session },
                        ..
                    })) => {
                        return PluginSpawnPoll::Ready(Ok(session));
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                        result: ReservedSpawnResult::Refused { error },
                        ..
                    }))
                    | CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                        result: ReservedSpawnResult::AdmittedFailure { error, .. },
                        ..
                    })) => {
                        self.spawn_error = Some(error);
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::Release => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused
                    | CoreTicketPoll::Lost
                    | CoreTicketPoll::Ready(Err(_)) => {
                        self.keep_reservation(runtime);
                        return spawn_fail(
                            self.spawn_error.take().unwrap_or(CoreDaemonError::Shutdown),
                            Some(SessionReservationRelease::RetainedUnconfirmed),
                        );
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                        result,
                        ..
                    })) => {
                        let error = self
                            .spawn_error
                            .take()
                            .unwrap_or(CoreDaemonError::Shutdown);
                        match result {
                            Ok(SessionReservationRelease::Released) => {
                                return spawn_fail(
                                    error,
                                    Some(SessionReservationRelease::Released),
                                );
                            }
                            Ok(disposition) => {
                                self.keep_reservation(runtime);
                                return spawn_fail(error, Some(disposition));
                            }
                            Err(_) => {
                                self.keep_reservation(runtime);
                                return spawn_fail(
                                    error,
                                    Some(SessionReservationRelease::RetainedUnconfirmed),
                                );
                            }
                        }
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
            }
        }
    }

    fn release_or_retain(&mut self, runtime: &HubRuntime) {
        if let Some(held) = self.reservation.clone() {
            self.tracker = runtime.begin_release_session_reservation(held);
            self.stage = PluginSpawnStage::Release;
        }
    }

    fn keep_reservation(&mut self, runtime: &HubRuntime) {
        if let Some(held) = self.reservation.take() {
            runtime.retain_reservation(held);
        }
    }

    fn continue_retry_or_reserve(&mut self, runtime: &HubRuntime) {
        if self.retry_tokens.is_empty() {
            runtime.merge_retained_reservations(std::mem::take(&mut self.retry_keep));
            self.tracker =
                runtime.begin_reserve_session(self.spawn.request.session_id.clone());
            self.stage = PluginSpawnStage::Reserve;
        } else {
            self.tracker =
                runtime.begin_release_session_reservation(self.retry_tokens[0].clone());
        }
    }
}

impl ManagedSessionSpawnStart {
    pub(crate) fn poll(&mut self, runtime: &HubRuntime) -> PluginSpawnPoll {
        loop {
            if let Some(pending_id) = self.tracker.pending_id()
                && matches!(self.stage, PluginSpawnStage::Reserve)
            {
                self.reserve_operation_id = Some(pending_id);
            }
            match self.stage {
                PluginSpawnStage::RetryRetained => {
                    return spawn_fail(CoreDaemonError::Shutdown, None);
                }
                PluginSpawnStage::Reserve => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
                        return spawn_fail(core_bridge_error(CoreTicketError::Overloaded), None);
                    }
                    CoreTicketPoll::Lost => {
                        let Some(reserve_id) = self.reserve_operation_id else {
                            return spawn_fail(CoreDaemonError::Shutdown, None);
                        };
                        self.tracker = runtime.begin_lookup_session_reservation_for_owner(
                            self.waiter_id,
                            self.spawn.request.session_id.clone(),
                            reserve_id,
                        );
                        self.stage = PluginSpawnStage::Lookup;
                    }
                    CoreTicketPoll::Ready(Err(error)) => {
                        return spawn_fail(error, None);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession { result, .. })) => {
                        match result {
                            Ok(reserved) => {
                                self.reservation = Some(reserved.clone());
                                if runtime.publish_spawn_context(&self.context).is_err() {
                                    self.spawn_error = Some(CoreDaemonError::Shutdown);
                                    self.release_or_retain(runtime);
                                    continue;
                                }
                                self.context_published = true;
                                self.tracker = runtime.begin_spawn_reserved_for_owner(
                                    self.waiter_id,
                                    reserved,
                                    self.spawn.clone(),
                                );
                                self.stage = PluginSpawnStage::SpawnReserved;
                            }
                            Err(error) => return spawn_fail(error, None),
                        }
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::Lookup => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
                        return spawn_fail(core_bridge_error(CoreTicketError::Overloaded), None);
                    }
                    CoreTicketPoll::Lost | CoreTicketPoll::Ready(Err(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::LookupSessionReservation {
                        result,
                        ..
                    })) => match result {
                        Ok(Some(reserved)) => {
                            self.reservation = Some(reserved.clone());
                            self.spawn_error = Some(CoreDaemonError::Shutdown);
                            self.tracker = runtime.begin_release_session_reservation_for_owner(
                                self.waiter_id,
                                reserved,
                            );
                            self.stage = PluginSpawnStage::Release;
                        }
                        Ok(None) => {
                            return spawn_fail(CoreDaemonError::Shutdown, None);
                        }
                        Err(error) => return spawn_fail(error, None),
                    },
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::SpawnReserved => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
                        self.spawn_error = Some(core_bridge_error(CoreTicketError::Overloaded));
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Lost => {
                        self.spawn_error = Some(CoreDaemonError::Shutdown);
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Ready(Err(error)) => {
                        self.spawn_error = Some(error);
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                        result: ReservedSpawnResult::Installed { session },
                        ..
                    })) => {
                        return PluginSpawnPoll::Ready(Ok(session));
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                        result: ReservedSpawnResult::Refused { error },
                        ..
                    }))
                    | CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                        result: ReservedSpawnResult::AdmittedFailure { error, .. },
                        ..
                    })) => {
                        self.spawn_error = Some(error);
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::Release => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused
                    | CoreTicketPoll::Lost
                    | CoreTicketPoll::Ready(Err(_)) => {
                        return spawn_fail(
                            self.spawn_error.take().unwrap_or(CoreDaemonError::Shutdown),
                            Some(SessionReservationRelease::RetainedUnconfirmed),
                        );
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                        result,
                        ..
                    })) => {
                        let error = self
                            .spawn_error
                            .take()
                            .unwrap_or(CoreDaemonError::Shutdown);
                        let disposition = result.ok();
                        return spawn_fail(error, disposition);
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
            }
        }
    }

    fn release_or_retain(&mut self, runtime: &HubRuntime) {
        if let Some(held) = self.reservation.clone() {
            self.tracker =
                runtime.begin_release_session_reservation_for_owner(self.waiter_id, held);
            self.stage = PluginSpawnStage::Release;
        }
    }
}

/// Inputs for one attach-and-bind turn on the Core owner thread.
pub(crate) struct AttachBindPlan {
    pub client_id: ClientId,
    pub session_id: SessionId,
    pub subscription_id: SubscriptionId,
    pub capabilities: TerminalCapabilitySet,
    pub now_seconds: u64,
    pub adapter: Box<dyn botster_core::contract::terminal_wake::WakingTerminalAdapter + Send>,
}

/// Where an attach-and-bind turn failed. Core holds no route afterwards.
#[derive(Debug)]
pub(crate) enum AttachBindFailure {
    /// `attach` itself failed; the adapter declaration was cancelled.
    Attach(CoreDaemonError),
    /// Attach succeeded but no live generation was visible; the route was detached.
    MissingGeneration,
    /// Adapter bind failed; the route was detached.
    Bind(CoreDaemonError),
}

/// Adapter bind inputs for one attached generation.
pub(crate) struct BindRoutePlan {
    pub client_id: ClientId,
    pub session_id: SessionId,
    pub subscription_id: SubscriptionId,
    pub generation: TerminalSubscriptionGeneration,
    pub capabilities: TerminalCapabilitySet,
    pub now_seconds: u64,
    pub adapter: Box<dyn botster_core::contract::terminal_wake::WakingTerminalAdapter + Send>,
}

pub(crate) fn attach_and_bind_on_core(
    daemon: &mut botster_core_daemon::CoreDaemon,
    plan: AttachBindPlan,
) -> Result<TerminalSubscriptionGeneration, AttachBindFailure> {
    let AttachBindPlan {
        client_id,
        session_id,
        subscription_id,
        capabilities,
        now_seconds,
        mut adapter,
    } = plan;
    let generation = match attach_route_on_core(
        daemon,
        client_id.clone(),
        session_id.clone(),
        subscription_id.clone(),
        now_seconds,
    ) {
        Ok(generation) => generation,
        Err(error) => {
            adapter.close();
            return Err(error);
        }
    };
    bind_route_on_core(
        daemon,
        BindRoutePlan {
            client_id,
            session_id,
            subscription_id,
            generation,
            capabilities,
            now_seconds,
            adapter,
        },
    )?;
    Ok(generation)
}

pub(crate) fn attach_route_on_core(
    daemon: &mut botster_core_daemon::CoreDaemon,
    client_id: ClientId,
    session_id: SessionId,
    subscription_id: SubscriptionId,
    now_seconds: u64,
) -> Result<TerminalSubscriptionGeneration, AttachBindFailure> {
    let previous = daemon
        .terminal_subscription_owner(&session_id, &subscription_id)
        .filter(|(owner, _)| *owner == &client_id)
        .map(|(_, generation)| generation);
    if let Some(generation) = previous {
        let _ = daemon.detach_terminal_subscription(
            client_id.clone(),
            session_id.clone(),
            subscription_id.clone(),
            generation,
            now_seconds,
        );
    }
    if let Err(error) = daemon.expect_terminal_adapter(
        client_id.clone(),
        session_id.clone(),
        subscription_id.clone(),
    ) {
        return Err(AttachBindFailure::Attach(error));
    }
    if let Err(error) = daemon.attach(
        client_id.clone(),
        session_id.clone(),
        subscription_id.clone(),
        now_seconds,
    ) {
        let _ = daemon.cancel_expected_terminal_adapter(
            &client_id,
            session_id.clone(),
            subscription_id.clone(),
        );
        return Err(AttachBindFailure::Attach(error));
    }
    let Some(generation) = daemon.terminal_subscription_generation(&session_id, &subscription_id)
    else {
        let _ = daemon.cancel_expected_terminal_adapter(
            &client_id,
            session_id.clone(),
            subscription_id.clone(),
        );
        let _ = daemon.detach(client_id, session_id, subscription_id, now_seconds);
        return Err(AttachBindFailure::MissingGeneration);
    };
    Ok(generation)
}

pub(crate) fn bind_route_on_core(
    daemon: &mut botster_core_daemon::CoreDaemon,
    plan: BindRoutePlan,
) -> Result<(), AttachBindFailure> {
    let BindRoutePlan {
        client_id,
        session_id,
        subscription_id,
        generation,
        capabilities,
        now_seconds,
        adapter,
    } = plan;
    if let Err(error) = daemon.bind_waking_terminal_adapter(
        client_id.clone(),
        session_id.clone(),
        subscription_id.clone(),
        generation,
        capabilities,
        adapter,
    ) {
        let _ = daemon.detach_terminal_subscription(
            client_id,
            session_id,
            subscription_id,
            generation,
            now_seconds,
        );
        return Err(AttachBindFailure::Bind(error));
    }
    Ok(())
}

impl HubRuntime {
    /// Retire detached Core operations whose keyed completion arrived.
    pub(crate) fn reap_detached_core_operations(&self) {
        self.reap_detached_operations();
    }

    fn reap_detached_operations(&self) {
        let Ok(mut detached) = self.detached_operations.lock() else {
            return;
        };
        for tracker in detached.iter_mut() {
            match tracker.poll_without_reaping() {
                CoreTicketPoll::Pending => {}
                CoreTicketPoll::Ready(_) | CoreTicketPoll::Lost | CoreTicketPoll::Refused => {}
            }
        }
        detached.retain(|tracker| !matches!(tracker.stage, CoreOperationStage::Done));
    }

    /// Run one closure on the Core owner thread and read its result later.
    pub(crate) fn submit_core<T, F>(&self, operation: F) -> CoreTicket<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut botster_core_daemon::CoreDaemon) -> T + Send + 'static,
    {
        self.core_daemon.submit(operation)
    }

    /// Run one Core closure for an explicitly registered owner phase.
    pub(crate) fn submit_core_for_owner<T, F>(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        operation: F,
    ) -> CoreTicket<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut botster_core_daemon::CoreDaemon) -> T + Send + 'static,
    {
        self.core_daemon.submit_for_owner(waiter_id, operation)
    }

    pub(crate) fn submit_core_for_optional_owner<T, F>(
        &self,
        waiter_id: Option<crate::owner_identity::WaiterId>,
        operation: F,
    ) -> CoreTicket<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut botster_core_daemon::CoreDaemon) -> T + Send + 'static,
    {
        match waiter_id {
            Some(waiter_id) => self.core_daemon.submit_for_owner(waiter_id, operation),
            None => self.core_daemon.submit(operation),
        }
    }

    /// Start one two-phase Core operation for an admitted owner waiter.
    pub(crate) fn begin_core_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        operation: CoreOperation,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin_for_owner(waiter_id, operation))
    }

    /// Current retention accounting, read on the Core owner thread.
    pub fn retention_accounting(&self) -> CoreTicket<RetentionAccounting> {
        self.core_daemon
            .submit(|daemon| daemon.retention_accounting())
    }

    /// Retention policy this runtime handed to Core.
    #[must_use]
    pub fn retention_policy(&self) -> RetentionPolicy {
        self.config.retention.core_policy()
    }

    fn mark_session_stale_now(
        &self,
        session_id: &SessionId,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.mark_session_stale(session_id, now_seconds)
            .wait(STARTUP_CORE_WAIT)
            .map_err(core_bridge_error)?
    }
}

#[cfg(test)]
impl HubRuntime {
    /// Test helper: spawn one session and wait for Core's completion.
    pub(crate) fn spawn_session_for_test(
        &self,
        request: SessionSpawnRequest,
        metadata: CoreSessionMetadata,
    ) -> Result<CoreSession, CoreDaemonError> {
        match self
            .begin_spawn(request, metadata)
            .wait(self, STARTUP_CORE_WAIT)?
        {
            CoreCompletion::Spawn { result, .. } => result,
            _ => Err(CoreDaemonError::Shutdown),
        }
    }

    /// Test helper: shut one session down and wait for Core's completion.
    pub(crate) fn shutdown_session_for_test(
        &self,
        session_id: SessionId,
    ) -> Result<(), CoreDaemonError> {
        match self
            .begin_shutdown_session(session_id)
            .wait(self, STARTUP_CORE_WAIT)?
        {
            CoreCompletion::ShutdownSession { result, .. } => result,
            _ => Err(CoreDaemonError::Shutdown),
        }
    }

    /// Test helper: current Core terminal subscription inventory.
    pub(crate) fn list_terminal_subscriptions_for_test(
        &self,
    ) -> Vec<botster_core::TerminalSubscriptionRecord> {
        self.list_terminal_subscriptions(crate::host_executor::HOST_PREPARED_BYTE_CAPACITY)
            .wait(STARTUP_CORE_WAIT)
            .expect("Core inventory")
            .expect("inventory fits the test allowance")
            .records
    }

    /// Test helper: mark one session stale and wait for Core.
    pub(crate) fn mark_session_stale_for_test(
        &self,
        session_id: &SessionId,
        now_seconds: u64,
    ) -> Result<(), CoreDaemonError> {
        self.mark_session_stale_now(session_id, now_seconds)
    }

    /// Test helper: one lifecycle baseline page read to completion.
    pub(crate) fn lifecycle_baseline_page_for_test(
        &self,
        snapshot: Option<&SessionLifecycleCursor>,
        after: Option<&SessionId>,
        budget: LifecycleBaselineBudget,
    ) -> Result<SessionLifecycleBaselinePage, SessionLifecyclePageError> {
        self.lifecycle_baseline_page(snapshot, after, budget)
            .wait(STARTUP_CORE_WAIT)
            .expect("Core baseline page")
    }
}

fn json_null() -> serde_json::Value {
    serde_json::Value::Null
}

#[cfg(test)]
thread_local! {
    static TEST_LIFECYCLE_JOURNAL_CAPACITY: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Run `f` with a test-only Core lifecycle journal capacity for this thread.
#[cfg(test)]
pub fn with_test_lifecycle_journal_capacity<R>(capacity: usize, f: impl FnOnce() -> R) -> R {
    TEST_LIFECYCLE_JOURNAL_CAPACITY.with(|slot| {
        let previous = slot.replace(Some(capacity));
        struct Reset(Option<usize>);
        impl Drop for Reset {
            fn drop(&mut self) {
                TEST_LIFECYCLE_JOURNAL_CAPACITY.with(|slot| slot.set(self.0));
            }
        }
        let _reset = Reset(previous);
        f()
    })
}

fn start_data_plane(
    core_config: CoreDaemonConfig,
) -> (
    crate::data_plane::CloseWorkSource,
    crate::data_plane::DataPlaneDriver,
    SharedCoreDaemon,
) {
    let close_work = crate::data_plane::CloseWorkSource::new();
    let (driver, core_daemon) =
        crate::data_plane::DataPlaneDriver::start(core_config, close_work.clone());
    (close_work, driver, core_daemon)
}

fn core_daemon_config(config: &HubConfig) -> CoreDaemonConfig {
    // Host profile supplies the initial/reset Ghostty color baseline. After
    // attach, current colors come from data-plane GHOSTSNP only.
    #[allow(unused_mut)]
    let mut core = CoreDaemonConfig::new(&config.data_directory)
        .with_worker_path(session_worker_path(config))
        .with_terminal_color_profile(default_terminal_color_profile())
        .with_retention_policy(config.retention.core_policy());
    #[cfg(test)]
    if let Some(capacity) = TEST_LIFECYCLE_JOURNAL_CAPACITY.with(std::cell::Cell::get) {
        core = core.with_lifecycle_journal_capacity(capacity);
    }
    core
}

/// Product default Ghostty special colors for pre-attach OSC 10/11/12 replies.
///
/// Foreground/cursor `#FFFFFF`, background `#282C34` at Ghostty reserved indexes.
fn default_terminal_color_profile() -> TerminalColorProfile {
    const COLOR_INDEX_FOREGROUND: u16 = 0x1000;
    const COLOR_INDEX_BACKGROUND: u16 = 0x1001;
    const COLOR_INDEX_CURSOR: u16 = 0x1002;
    let mut colors = std::collections::HashMap::new();
    colors.insert(
        COLOR_INDEX_FOREGROUND,
        Rgb {
            r: 0xff,
            g: 0xff,
            b: 0xff,
        },
    );
    colors.insert(
        COLOR_INDEX_BACKGROUND,
        Rgb {
            r: 0x28,
            g: 0x2c,
            b: 0x34,
        },
    );
    colors.insert(
        COLOR_INDEX_CURSOR,
        Rgb {
            r: 0xff,
            g: 0xff,
            b: 0xff,
        },
    );
    TerminalColorProfile { colors }
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

fn validate_plugin_surface_binding_families(
    node: &UiNode,
    admitted_families: &BTreeSet<String>,
) -> Result<(), crate::McpToolError> {
    let value = serde_json::to_value(node).map_err(|error| {
        crate::McpToolError::new(
            "invalid_surface",
            format!("failed to inspect plugin UiNode bindings: {error}"),
        )
    })?;
    validate_plugin_surface_binding_value(&value, admitted_families)
}

fn validate_plugin_surface_binding_value(
    value: &serde_json::Value,
    admitted_families: &BTreeSet<String>,
) -> Result<(), crate::McpToolError> {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                validate_plugin_surface_binding_value(value, admitted_families)?;
            }
        }
        serde_json::Value::Object(object) => {
            if let Some(path) = object.get("$bind").and_then(serde_json::Value::as_str) {
                validate_plugin_surface_binding_path(path, admitted_families)?;
            }
            match object.get("$kind").and_then(serde_json::Value::as_str) {
                Some("bind_list") => {
                    if let Some(path) = object.get("source").and_then(serde_json::Value::as_str) {
                        validate_plugin_surface_binding_path(path, admitted_families)?;
                    }
                }
                Some("bind_if") => {
                    if let Some(path) = object.get("path").and_then(serde_json::Value::as_str) {
                        validate_plugin_surface_binding_path(path, admitted_families)?;
                    }
                }
                Some("entity_options") => {
                    if let Some(path) = object.get("source").and_then(serde_json::Value::as_str) {
                        validate_plugin_surface_binding_path(path, admitted_families)?;
                    }
                    if let Some(exclude) =
                        object.get("exclude").and_then(serde_json::Value::as_object)
                        && let Some(path) =
                            exclude.get("source").and_then(serde_json::Value::as_str)
                    {
                        validate_plugin_surface_binding_path(path, admitted_families)?;
                    }
                }
                _ => {}
            }
            for value in object.values() {
                validate_plugin_surface_binding_value(value, admitted_families)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_plugin_surface_binding_path(
    path: &str,
    admitted_families: &BTreeSet<String>,
) -> Result<(), crate::McpToolError> {
    if !path.starts_with('/') || path == "/session" || path.starts_with("/session/") {
        return Ok(());
    }
    if path
        .strip_prefix('/')
        .and_then(|path| path.split('/').next())
        .is_some_and(|family| admitted_families.contains(family))
    {
        return Ok(());
    }
    Err(crate::McpToolError::new(
        "invalid_surface",
        format!("plugin UiNode binding family is not admitted by this Hub: {path}"),
    ))
}

fn validate_plugin_surface_node(
    node: &UiNode,
    admitted_families: &BTreeSet<String>,
) -> Result<(), crate::McpToolError> {
    node.validate_authored().map_err(|error| {
        crate::McpToolError::new("invalid_surface", format!("invalid plugin UiNode: {error}"))
    })?;
    validate_plugin_surface_binding_families(node, admitted_families)
}

fn validate_plugin_surface_action_result(
    result: &UiActionResult,
    request: &UiActionRequest,
    admitted_families: &BTreeSet<String>,
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
        validate_plugin_surface_binding_families(replacement, admitted_families).map_err(
            |error| {
                crate::McpToolError::new(
                    "invalid_action_result",
                    format!(
                        "invalid plugin UiActionResult replacement: {}",
                        error.message
                    ),
                )
            },
        )?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        DataDirectoryOption, HostIdentityOptions, HubStartupOptions, RuntimeEnvironment,
        SessionDefaults, TransportBindings,
    };

    pub(super) fn family_runtime(name: &str) -> HubRuntime {
        static NEXT_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let directory_id = NEXT_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let config = HubStartupOptions {
            host: HostIdentityOptions {
                id: name.to_string(),
                display_name: name.to_string(),
                fingerprint: None,
            },
            data_directory: DataDirectoryOption::Explicit(
                std::env::temp_dir().join(format!("{name}-{}-{directory_id}", std::process::id())),
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
        .unwrap();
        HubRuntime::new(config).unwrap()
    }

    #[test]
    fn lua_host_api_clones_share_the_runtime_memory_account() {
        let runtime = family_runtime("lua-memory-new");
        let api = runtime.lua_plugin_host_api();
        let cloned_api = api.clone();
        assert!(Arc::ptr_eq(&runtime.lua_memory, &api.memory));
        assert!(Arc::ptr_eq(&api.memory, &cloned_api.memory));
        let charge = api.memory.reserve_vm().unwrap();
        assert_eq!(
            cloned_api.memory.usage().0,
            crate::config::lua_memory_limits().per_vm_bytes
        );
        drop(charge);
        assert_eq!(runtime.lua_memory.usage(), (0, 0));
    }

    #[test]
    fn restored_runtime_shares_its_own_lua_memory_account() {
        let first = family_runtime("lua-memory-restored");
        let config = first.config().clone();
        let first_account = Arc::clone(&first.lua_memory);
        drop(first);
        let state = HubState::from_config(&config);
        let restored = HubRuntime::from_validated_state(config, state).unwrap();
        let api = restored.lua_plugin_host_api();
        assert!(!Arc::ptr_eq(&first_account, &restored.lua_memory));
        assert!(Arc::ptr_eq(&restored.lua_memory, &api.memory));
        assert_eq!(api.memory.limits(), crate::config::lua_memory_limits());
    }

    fn check_terminal_spawner_disposal(poison: bool) {
        use crate::host_disposal::{Job, Parts, Poll};
        use crate::host_executor::{
            HOST_OPERATION_CAPACITY, HOST_PREPARED_BYTE_CAPACITY, HostJobIdentity,
        };

        let name = if poison {
            "terminal-spawner-poison"
        } else {
            "terminal-spawner"
        };
        let mut runtime = family_runtime(name);
        let spawner = runtime.session_type_spawner();
        let (release, gate) = mpsc::channel();
        let observed = spawner.test_seed_terminal_pending(Some(gate));
        if poison {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _pending = spawner.pending.lock().unwrap();
                let _managed = spawner.managed.lock().unwrap();
                panic!("poison the nonempty spawner queues");
            }));
            assert!(result.is_err());
            assert!(spawner.pending.is_poisoned());
            assert!(spawner.managed.is_poisoned());
        }
        let permit = runtime.host_executor().try_reserve().unwrap();
        let spare_permits = (1..HOST_OPERATION_CAPACITY)
            .map(|_| runtime.host_executor().try_reserve().unwrap())
            .collect::<Vec<_>>();
        assert!(runtime.host_executor().try_reserve().is_none());
        let identity = HostJobIdentity::first(crate::owner_identity::WaiterId(71));
        let lifecycle = runtime.take_plugin_lifecycle().unwrap();
        let mut engine_job = Job::new(Parts {
            storage: None,
            identity,
            permit,
            payload: Box::new(lifecycle),
            model: None,
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        let permit = loop {
            match engine_job.poll() {
                Poll::Disposed(permit) => break permit,
                Poll::Pending => assert!(Instant::now() < deadline),
                _ => panic!("engine disposal must return its original permit"),
            }
            thread::yield_now();
        };
        assert_eq!(spawner.test_terminal_pending_counts(), (1, 1, true));
        assert!(matches!(
            observed.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        let mut bridge_job = Job::new_plugin_bridges(
            Parts {
                storage: None,
                identity,
                permit,
                payload: Box::new(()),
                model: None,
            },
            runtime.terminal_plugin_bridges(),
        );
        let (thread_name, locks_released) = observed.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(thread_name.starts_with("botster-hub-host"));
        assert!(locks_released);
        assert_eq!(spawner.test_terminal_pending_counts(), (0, 0, true));
        assert!(matches!(bridge_job.poll(), Poll::Pending));
        assert_eq!(
            runtime.host_executor().outstanding(),
            HOST_OPERATION_CAPACITY
        );
        assert_eq!(
            runtime.host_executor().prepared_bytes(),
            HOST_OPERATION_CAPACITY * HOST_PREPARED_BYTE_CAPACITY
        );
        assert!(runtime.host_executor().try_reserve().is_none());
        release.send(()).unwrap();
        let (thread_name, locks_released) = observed.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(thread_name.starts_with("botster-hub-host"));
        assert!(locks_released);
        let deadline = Instant::now() + Duration::from_secs(5);
        let permit = loop {
            match bridge_job.poll() {
                Poll::Disposed(permit) => break permit,
                Poll::Pending => assert!(Instant::now() < deadline),
                _ => {
                    panic!("bridge disposal must return the same permit after payload destruction")
                }
            }
            thread::yield_now();
        };
        assert_eq!(spawner.test_terminal_pending_counts(), (0, 0, false));
        assert!(matches!(
            observed.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert_eq!(
            runtime.host_executor().outstanding(),
            HOST_OPERATION_CAPACITY
        );
        drop(spare_permits);
        assert_eq!(runtime.host_executor().outstanding(), 1);
        assert_eq!(
            runtime.host_executor().prepared_bytes(),
            HOST_PREPARED_BYTE_CAPACITY
        );
        drop(permit);
        assert_eq!(runtime.host_executor().outstanding(), 0);
        assert_eq!(runtime.host_executor().prepared_bytes(), 0);
        runtime.release_for_restart();
        drop(runtime);
        let root = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
        if root.exists() {
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn terminal_spawner_queues_clear_on_host_before_the_original_slot_returns() {
        check_terminal_spawner_disposal(false);
    }

    #[test]
    fn terminal_spawner_poisoned_queues_clear_on_host_before_the_original_slot_returns() {
        check_terminal_spawner_disposal(true);
    }

    #[test]
    fn causal_fifo_preserves_transfer_before_release_at_capacity() {
        let runtime = family_runtime("causal-finish-fifo");
        let pending = LeaseIdentity::PendingEntityPublish {
            publication_token: 0,
        };
        let admitted = LeaseIdentity::AdmittedEntityMutation {
            family_token: 1,
            seq: 1,
        };
        let scope_id = runtime
            .causal_scopes
            .mint_with_lease(Some(pending.clone()))
            .unwrap();
        for _ in 0..CAUSAL_OWNER_CAPACITY - 2 {
            assert!(matches!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: 0,
                    identity: pending.clone(),
                }),
                CausalAdmitResult::Applied
            ));
        }
        assert!(matches!(
            runtime.admit_causal_op(CausalOp::Transfer {
                scope_id,
                from: pending,
                to: [Some(admitted.clone()), None, None],
            }),
            CausalAdmitResult::Applied
        ));
        assert!(matches!(
            runtime.admit_causal_op(CausalOp::Release {
                scope_id,
                identity: admitted
            }),
            CausalAdmitResult::Applied
        ));
        let deadline = Instant::now() + Duration::from_secs(3);
        while runtime.causal_owner_ops_pending() {
            runtime.apply_causal_owner_ops();
            assert!(Instant::now() < deadline, "the finish FIFO must drain");
        }
        assert!(
            runtime.causal_scopes.identities(scope_id).is_none(),
            "a later release must not overtake its transfer"
        );
    }

    #[test]
    fn causal_finish_fifo_moves_one_operation_per_owner_phase() {
        let runtime = family_runtime("causal-finish-phase");
        let mut scopes = Vec::new();
        for _ in 0..6 {
            let identity = LeaseIdentity::EventInFlight;
            let scope_id = runtime
                .causal_scopes
                .mint_with_lease(Some(identity.clone()))
                .unwrap();
            scopes.push(scope_id);
            assert!(matches!(
                runtime.admit_causal_op(CausalOp::Release { scope_id, identity }),
                CausalAdmitResult::Applied
            ));
        }
        let mut previous = runtime.causal_operation_count();
        for _ in 0..128 {
            runtime.apply_causal_owner_ops();
            let remaining = runtime.causal_operation_count();
            assert!(
                previous - remaining <= 1,
                "one phase must not drain multiple finish operations"
            );
            previous = remaining;
        }
        assert_eq!(runtime.causal_operation_count(), 0);
        for scope in scopes {
            assert!(runtime.causal_scopes.identities(scope).is_none());
        }
    }

    #[test]
    fn family_cleanup_finds_old_fanout_without_live_state() {
        let runtime = family_runtime("orphan-fanout-cleanup");
        let family = "producer.item";
        let scope = runtime
            .causal_scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        for (generation, family_token) in [(0, 10), (1, 20)] {
            let lease = EntityMutationLease {
                admission: None,
                family_token,
                scope_id: scope,
                family: family.into(),
                generation,
                seq: 1,
            };
            assert!(runtime.causal_scopes.acquire(
                scope,
                LeaseIdentity::AdmittedEntityMutation {
                    family_token,
                    seq: 1
                }
            ));
            runtime
                .package_entities
                .lock()
                .expect("package entity model lock")
                .fanout
                .try_push(LeasedFanoutMutation {
                    generation,
                    mutation: PackageEntityMutation::Upsert {
                        admission: None,
                        entity_type: family.into(),
                        snapshot_seq: 1,
                        id: "item".into(),
                        entity: serde_json::json!({"id": "item"}),
                    },
                    lease: Some(lease),
                })
                .unwrap();
        }
        assert!(!runtime.test_family_exists(family));
        runtime.advance_package_entity_epoch().unwrap();
        runtime.drop_package_entity_families("producer", BTreeSet::new());
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        let identities = runtime.causal_scopes.identities(scope).unwrap();
        assert!(
            !identities.contains(&LeaseIdentity::AdmittedEntityMutation {
                family_token: 10,
                seq: 1
            })
        );
        assert!(identities.contains(&LeaseIdentity::AdmittedEntityMutation {
            family_token: 20,
            seq: 1
        }));
        let remaining = runtime.take_one_package_entity_fanout().unwrap();
        assert_eq!(remaining.generation, 1);
        assert!(runtime.take_one_package_entity_fanout().is_none());
        assert_eq!(
            runtime.finish_package_entity_fanout(&remaining.finish),
            CausalTransitionStatus::Applied
        );
    }

    pub(crate) fn publication_provider_runtime(label: &str) -> (HubRuntime, std::path::PathBuf) {
        publication_provider_runtime_with_families(label, &["producer.item"])
    }

    fn publication_provider_runtime_with_families(
        label: &str,
        families: &[&str],
    ) -> (HubRuntime, std::path::PathBuf) {
        publication_provider_runtime_with_body(label, families, "")
    }

    fn publication_provider_runtime_with_body(
        label: &str,
        families: &[&str],
        before_snapshot: &str,
    ) -> (HubRuntime, std::path::PathBuf) {
        let mut runtime = family_runtime(label);
        let root =
            std::env::temp_dir().join(format!("fanout-provider-{label}-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("botster-package.json"),
            serde_json::json!({
                "name": "producer", "version": "1.0.0", "kind": "plugin",
                "botster": ">=0.1.0", "capabilities": [],
                "source": { "type": "path", "path": root.canonicalize().unwrap() },
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            })
            .to_string(),
        )
        .unwrap();
        let handlers = families
            .iter()
            .enumerate()
            .map(|(index, family)| {
                let family = serde_json::to_string(family).unwrap();
                let handler = if index == 0 {
                    "items".into()
                } else {
                    format!("items{index}")
                };
                format!(
                    r#"{{
                id = "{handler}", kind = "entity_provider", descriptor_id = {family},
                descriptor = {{ entity_type = {family}, id_field = "id" }},
                call = function() {before_snapshot} return {{
                    type = "entity_snapshot", entity_type = {family}, snapshot_seq = 0, items = {{}}
                }} end
            }}"#
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        std::fs::write(
            root.join("plugin.lua"),
            format!("return botster.register({{ handlers = {{ {handlers} }} }})"),
        )
        .unwrap();
        let mut policy = crate::default_package_policy();
        policy
            .install_local_path(&root, "install test provider")
            .unwrap();
        policy.enable("producer", "enable test provider").unwrap();
        runtime
            .load_lua_plugin_package(policy.registry(), "producer")
            .unwrap();
        (runtime, root)
    }

    #[test]
    fn lua_load_and_reload_refuse_before_replacing_live_state() {
        use botster_core::{
            CapabilityOperation, CapabilityOperationId, CapabilityRuntimeEvent,
            CapabilityRuntimeRequest, TimerCapabilityRequest,
        };

        let (mut runtime, root) = publication_provider_runtime("lua-memory-refusal");
        let entrypoint = root.join("plugin.lua");
        let source = std::fs::read_to_string(&entrypoint).unwrap();
        std::fs::write(
            &entrypoint,
            format!(
                "events.on('hub', 'worktree_created', function(event) return {{ received = event.event }} end)\n{source}"
            ),
        )
        .unwrap();
        let mut policy = crate::default_package_policy();
        policy
            .install_local_path(&root, "install memory test provider")
            .unwrap();
        policy
            .enable("producer", "enable memory test provider")
            .unwrap();
        runtime
            .load_lua_plugin_package(policy.registry(), "producer")
            .unwrap();
        let memory = Arc::clone(&runtime.lua_memory);
        let limits = memory.limits();
        assert_eq!(memory.usage(), (limits.per_vm_bytes, 0));
        let registration = runtime
            .plugin_lifecycle()
            .entity_provider_registrations()
            .select("producer", "producer.item")
            .unwrap();
        let generation = runtime
            .package_event_router
            .current_package_generation("producer");
        assert!(registration.is_live());
        assert!(matches!(generation, Ok(value) if value > 0));
        assert_eq!(
            runtime
                .package_event_router
                .test_subscription_count("producer"),
            1
        );
        assert!(runtime.last_capability_cleanup().is_none());
        let plugin_key = PluginKey("producer".into());
        let timer = runtime
            .submit_capability_request(CapabilityRuntimeRequest {
                plugin_key: plugin_key.clone(),
                operation_id: CapabilityOperationId("lua-memory-surviving-timer".into()),
                operation: CapabilityOperation::Timer(TimerCapabilityRequest::Interval {
                    interval_ms: 5,
                }),
                timeout_ms: 1_000,
                callback: None,
            })
            .unwrap()
            .resource
            .expect("the interval timer must have a resource");
        assert_eq!(timer.plugin_key, plugin_key);
        assert_eq!(runtime.active_plugin_timer_resources(), 1);
        let held: Vec<_> = (1..limits.total_vm_bytes / limits.per_vm_bytes)
            .map(|_| memory.reserve_vm().unwrap())
            .collect();
        assert_eq!(memory.usage().0, limits.total_vm_bytes);
        for (reload, now_ms, sequence) in [(false, 5, 1), (true, 10, 2)] {
            let error = if reload {
                runtime
                    .reload_lua_plugin_package(
                        RequestId("lua-memory-refusal".into()),
                        policy.registry(),
                        "producer",
                    )
                    .unwrap_err()
            } else {
                runtime
                    .load_lua_plugin_package(policy.registry(), "producer")
                    .unwrap_err()
            };
            match error {
                HubLuaPluginLoadError::Lua(crate::lua_runtime::LuaPluginRuntimeError::Load(
                    message,
                )) => {
                    assert_eq!(
                        message,
                        format!(
                            "Lua VM memory capacity exhausted: requested {} bytes, 0 available",
                            limits.per_vm_bytes,
                        )
                    );
                }
                other => panic!("expected the VM capacity refusal, got {other:?}"),
            }
            assert!(registration.is_live());
            assert_eq!(
                runtime
                    .package_event_router
                    .current_package_generation("producer"),
                generation
            );
            assert_eq!(
                runtime
                    .package_event_router
                    .test_subscription_count("producer"),
                1
            );
            assert!(runtime.last_capability_cleanup().is_none());
            assert_eq!(memory.usage(), (limits.total_vm_bytes, 0));
            let events = runtime
                .drain_capability_events_at(&plugin_key, now_ms)
                .unwrap();
            assert!(events.iter().any(|event| matches!(
                event,
                CapabilityRuntimeEvent::TimerFired(event)
                    if event.resource == timer && event.sequence == sequence
            )));
            assert_eq!(runtime.active_plugin_timer_resources(), 1);
        }
        drop(held);
        runtime
            .reload_lua_plugin_package(
                RequestId("lua-memory-retry".into()),
                policy.registry(),
                "producer",
            )
            .unwrap();
        assert!(!registration.is_live());
        let replaced_generation = runtime
            .package_event_router
            .current_package_generation("producer")
            .unwrap();
        assert!(replaced_generation > generation.unwrap());
        assert_eq!(
            runtime
                .package_event_router
                .test_subscription_count("producer"),
            1
        );
        assert_eq!(runtime.active_plugin_timer_resources(), 0);
        assert!(
            runtime
                .last_capability_cleanup()
                .unwrap()
                .removed_resources
                .contains(&timer)
        );
        assert_eq!(memory.usage(), (limits.per_vm_bytes, 0));
        drop(runtime);
        assert_eq!(memory.usage(), (0, 0));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn provider_registration_rejects_queued_publications_after_reload_and_unload() {
        let (mut runtime, root) = publication_provider_runtime("registration-replacement");
        let mut policy = crate::default_package_policy();
        policy
            .install_local_path(&root, "install replacement provider")
            .unwrap();
        policy
            .enable("producer", "enable replacement provider")
            .unwrap();
        let bridge = runtime.entity_publish_bridge();
        let frame = || {
            serde_json::json!({
                "type": "entity_remove", "entity_type": "producer.item",
                "snapshot_seq": 1, "id": "item"
            })
        };
        for reload in [true, false] {
            let scope = runtime.causal_scopes.mint().unwrap();
            let response =
                bridge.test_queue_publish(PluginKey("producer".into()), frame(), Some(scope));
            assert_eq!(bridge.pending_publish_count(), 1);
            assert_eq!(bridge.retained_counts().0, 1);
            if reload {
                runtime
                    .reload_lua_plugin_package(
                        RequestId("registration-reload".into()),
                        policy.registry(),
                        "producer",
                    )
                    .unwrap();
            } else {
                let _ = runtime
                    .plugin_lifecycle()
                    .unload_package(RequestId("registration-unload".into()), "producer");
            }
            runtime.test_fulfill_pending_publishes();
            assert!(
                response
                    .try_recv()
                    .unwrap()
                    .unwrap_err()
                    .contains("registration")
            );
            while runtime.causal_operation_count() > 0 {
                runtime.apply_causal_owner_ops();
            }
            assert!(!runtime.causal_scopes.is_live(scope));
            assert!(!runtime.test_family_exists("producer.item"));
            assert_eq!(bridge.pending_publish_count(), 0);
            assert_eq!(bridge.retained_counts(), (0, 0));
        }
        runtime
            .load_lua_plugin_package(policy.registry(), "producer")
            .unwrap();
        let response = bridge.test_queue_publish(PluginKey("producer".into()), frame(), None);
        runtime.test_fulfill_pending_publishes();
        runtime.test_fulfill_pending_publishes();
        assert!(response.try_recv().unwrap().is_ok());
        assert!(runtime.test_family_exists("producer.item"));
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn family_tokens_distinguish_shared_scope_and_same_generation_recreation() {
        let long_family = format!("producer.{}", "a".repeat(300_000));
        let names = ["producer.item", "producer.other", long_family.as_str()];
        let (runtime, root) =
            publication_provider_runtime_with_families("family-token-incarnations", &names);
        let scope = runtime
            .causal_scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        let publish = |name: &str| {
            assert!(runtime.causal_scopes.acquire(
                scope,
                LeaseIdentity::PendingEntityPublish {
                    publication_token: 0
                }
            ));
            runtime.test_admit_publish("producer", serde_json::json!({
                "type": "entity_remove", "entity_type": name, "snapshot_seq": 1, "id": "item"
            }), Some(scope)).unwrap();
            runtime.apply_causal_owner_ops();
            runtime.take_one_package_entity_fanout().unwrap()
        };
        let mut mutations: Vec<_> = names.iter().map(|name| publish(name)).collect();
        let tokens: BTreeSet<_> = mutations
            .iter()
            .map(|item| item.finish.lease.as_ref().unwrap().family_token)
            .collect();
        assert_eq!(tokens.len(), 3);
        assert_eq!(runtime.causal_scopes.lease_count(scope), Some(4));
        assert!(
            mutations
                .iter()
                .all(|item| item.generation == mutations[0].generation)
        );

        let mut old = mutations.remove(0);
        let retired = runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .remove(names[0])
            .unwrap();
        assert!(retired.pending_leases.is_empty());
        assert!(retired.resync.leases.is_empty());
        let new = publish(names[0]);
        assert_eq!(old.generation, new.generation);
        assert_ne!(
            old.finish.lease.as_ref().unwrap().family_token,
            new.finish.lease.as_ref().unwrap().family_token
        );
        old.finish.scheduled_resync = true;
        assert_eq!(
            runtime.finish_package_entity_fanout(&old.finish),
            CausalTransitionStatus::Applied
        );
        runtime.apply_causal_owner_ops();
        assert!(
            !runtime
                .package_entities
                .lock()
                .expect("package entity model lock")
                .families[names[0]]
                .resync
                .needed
        );
        let identities = runtime.causal_scopes.identities(scope).unwrap();
        assert!(
            !identities.contains(&LeaseIdentity::AdmittedEntityMutation {
                family_token: old.finish.lease.as_ref().unwrap().family_token,
                seq: 1,
            })
        );
        assert!(identities.contains(&LeaseIdentity::AdmittedEntityMutation {
            family_token: new.finish.lease.as_ref().unwrap().family_token,
            seq: 1,
        }));
        mutations.push(new);
        for item in mutations {
            assert_eq!(
                runtime.finish_package_entity_fanout(&item.finish),
                CausalTransitionStatus::Applied
            );
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(
            runtime.causal_scopes.identities(scope),
            Some(BTreeSet::from([LeaseIdentity::EventInFlight]))
        );
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn family_token_exhaustion_preserves_publications_and_existing_leases() {
        let (runtime, root) = publication_provider_runtime_with_families(
            "family-token-exhaustion",
            &["producer.item", "producer.other"],
        );
        let scope = runtime
            .causal_scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        let frame = |name: &str, seq| {
            serde_json::json!({
                "type": "entity_remove", "entity_type": name, "snapshot_seq": seq, "id": "original-payload"
            })
        };
        runtime.package_entities.lock().unwrap().next_family_token = u64::MAX;
        for seq in [1, 2] {
            assert!(runtime.causal_scopes.acquire(
                scope,
                LeaseIdentity::PendingEntityPublish {
                    publication_token: 0
                }
            ));
            runtime
                .test_admit_publish("producer", frame("producer.item", seq), Some(scope))
                .unwrap();
            runtime.apply_causal_owner_ops();
            assert_eq!(
                runtime.package_entities.lock().unwrap().next_family_token,
                0
            );
            assert_eq!(
                runtime
                    .package_entities
                    .lock()
                    .expect("package entity model lock")
                    .families["producer.item"]
                    .causal_token,
                Some(u64::MAX)
            );
        }
        let expected = prepare_publish_mutation(frame("producer.other", 1)).unwrap();
        for tokenless_exists in [false, true] {
            if tokenless_exists {
                runtime.test_set_family_seq("producer.other", 0);
            }
            let receiver = runtime.entity_publish_bridge.test_queue_publish(
                PluginKey("producer".into()),
                frame("producer.other", 1),
                Some(scope),
            );
            let original = runtime
                .begin_entity_publish()
                .expect("exhaustion retains the original payload for disposal");
            let mut expected = expected.clone();
            expected.set_admission(original.admission().cloned());
            assert_eq!(original, expected);
            assert_eq!(
                runtime.test_family_exists("producer.other"),
                tokenless_exists
            );
            if tokenless_exists {
                let model = runtime
                    .package_entities
                    .lock()
                    .expect("package entity model lock");
                let families = &model.families;
                let family = &families["producer.other"];
                assert_eq!(family.causal_token, None);
                assert_eq!(family.last_accepted_seq, 0);
                assert_eq!(family.high_water_seq, 0);
                assert!(family.pending_by_seq.is_empty());
                assert!(family.pending_leases.is_empty());
                assert!(family.resync.leases.is_empty());
            }
            assert!(receiver.try_recv().is_err());
            drop(original);
            runtime.complete_entity_publish_disposal();
            assert_eq!(
                runtime.finish_entity_publish_retirement(),
                CausalTransitionStatus::Applied
            );
            runtime.apply_causal_owner_ops();
            assert!(
                receiver
                    .try_recv()
                    .unwrap()
                    .unwrap_err()
                    .contains("entity_family_token_exhausted")
            );
            assert_eq!(runtime.causal_scopes.lease_count(scope), Some(3));
        }
        while let Some(item) = runtime.take_one_package_entity_fanout() {
            assert_eq!(
                runtime.finish_package_entity_fanout(&item.finish),
                CausalTransitionStatus::Applied
            );
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(
            runtime.causal_scopes.identities(scope),
            Some(BTreeSet::from([LeaseIdentity::EventInFlight]))
        );
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn provider_tokens_preserve_distinct_leases_through_exhaustion_and_retry() {
        let (runtime, root) = publication_provider_runtime("provider-tokens");
        let scope_id = runtime
            .causal_scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        runtime.mark_package_entity_resync_needed("producer.item");
        runtime.test_store_resync_lease(scope_id, "producer.item");
        let prepare = || {
            runtime.prepare_plugin_entity_snapshot(
                "producer.item",
                "subscription",
                RequestId("same-request".into()),
                None,
            )
        };
        let (_, first) = prepare().unwrap();
        runtime.next_provider_token.set(u64::MAX);
        let (_, last) = prepare().unwrap();
        assert_eq!(first.causal_lease, Some((scope_id, 1)));
        assert_eq!(last.causal_lease, Some((scope_id, u64::MAX)));
        assert!(prepare().is_err());
        assert_eq!(runtime.causal_scopes.lease_count(scope_id), Some(3));
        for _ in 0..CAUSAL_OWNER_CAPACITY {
            assert_eq!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: 0,
                    identity: LeaseIdentity::EventInFlight,
                }),
                CausalAdmitResult::Applied
            );
        }
        assert_eq!(
            runtime.retire_plugin_entity_snapshot(&first),
            CausalTransitionStatus::Waiting
        );
        assert_eq!(first.causal_lease, Some((scope_id, 1)));
        for _ in 0..CAUSAL_OWNER_CAPACITY {
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(runtime.causal_operation_count(), 0);
        assert_eq!(
            runtime.retire_plugin_entity_snapshot(&first),
            CausalTransitionStatus::Applied
        );
        runtime.apply_causal_owner_ops();
        assert_eq!(
            runtime.causal_scopes.identities(scope_id).unwrap(),
            BTreeSet::from([
                LeaseIdentity::EventInFlight,
                LeaseIdentity::ProviderInFlight {
                    invocation_token: u64::MAX,
                },
            ])
        );
        assert_eq!(
            runtime.retire_plugin_entity_snapshot(&last),
            CausalTransitionStatus::Applied
        );
        runtime.apply_causal_owner_ops();
        assert_eq!(runtime.causal_scopes.lease_count(scope_id), Some(1));
        drop(first);
        drop(last);
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fanout_sequence_exhaustion_preserves_publication_state() {
        let (runtime, root) = publication_provider_runtime("fanout-admission-exhaustion");
        let frame = |seq| {
            serde_json::json!({
                "type": "entity_upsert", "entity_type": "producer.item",
                "snapshot_seq": seq, "id": "item", "entity": { "id": "item" }
            })
        };
        runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .fanout
            .set_next_sequence_for_test(u64::MAX);
        let error = runtime
            .test_admit_publish("producer", frame(1), None)
            .unwrap_err();
        assert!(error.contains("entity_fanout_sequence_exhausted"));
        assert!(!runtime.test_family_exists("producer.item"));

        runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .fanout
            .set_next_sequence_for_test(u64::MAX - 1);
        let result = runtime
            .test_admit_publish("producer", frame(2), None)
            .unwrap();
        assert_eq!(result.status, PackageEntityPublishStatus::PendingGap);
        let snapshot = |family: &PackageEntityFamilyState| {
            (
                family.generation,
                family.causal_token,
                family.last_accepted_seq,
                family.high_water_seq,
                family.pending_by_seq.clone(),
                family.pending_leases.clone(),
                family.resync.needed,
                family.resync.leases.clone(),
            )
        };
        let before = snapshot(
            &runtime
                .package_entities
                .lock()
                .expect("package entity model lock")
                .families["producer.item"],
        );
        runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .fanout
            .set_next_sequence_for_test(u64::MAX);
        let scope_id = runtime
            .causal_scopes
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                publication_token: 0,
            }))
            .unwrap();
        let error = runtime
            .test_admit_publish("producer", frame(1), Some(scope_id))
            .unwrap_err();
        assert!(error.contains("entity_fanout_sequence_exhausted"));
        let model = runtime
            .package_entities
            .lock()
            .expect("package entity model lock");
        let families = &model.families;
        let after = &families["producer.item"];
        assert_eq!(snapshot(after), before);
        assert!(model.fanout.is_empty());
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert!(runtime.causal_scopes.identities(scope_id).is_none());
        drop(model);
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn provider_expectation_capacity_precedes_selection_and_returns_after_host_disposal() {
        let (runtime, root) = publication_provider_runtime("provider-expectation-capacity");
        let budget = SharedViewBudget::with_capacity(4096);
        let full = budget.reserve(4096).unwrap();
        let prepare = || {
            ProviderRequestPlan::prepare(
                runtime.plugin_lifecycle(),
                &budget,
                "producer.item",
                "sub",
                RequestId("metadata-test".into()),
                None,
            )
        };
        let error = prepare().unwrap_err();
        assert_eq!(error.code, "entity_provider_metadata_capacity");
        assert_eq!(budget.used(), 4096);
        drop(full);
        let plan = prepare().unwrap();
        let charged = budget.used();
        assert!(charged > 0);
        let invocation = runtime
            .select_plugin_entity_snapshot(&plan.expected)
            .unwrap();
        assert_eq!(
            budget.used(),
            charged,
            "selection shares the expectation charge"
        );
        let executor = runtime.host_executor();
        let identity =
            crate::host_executor::HostJobIdentity::first(crate::owner_identity::WaiterId(707));
        executor
            .submit(
                identity,
                crate::host_executor::HostCommand::PluginEntity(
                    crate::plugin_entity::Command::DiscardProvider {
                        plan: Some(plan),
                        invocation: Some(invocation),
                        input: None,
                        refusal: None,
                    },
                ),
                executor.try_reserve().unwrap(),
            )
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match executor.poll_completion() {
                crate::host_executor::HostCompletionPoll::Ready(completion) => {
                    assert_eq!(completion.identity, identity);
                    assert_eq!(
                        budget.used(),
                        0,
                        "Host disposal returns the final expectation charge"
                    );
                    break;
                }
                crate::host_executor::HostCompletionPoll::Empty => std::thread::yield_now(),
                crate::host_executor::HostCompletionPoll::Stopped => panic!("Host stopped"),
            }
            assert!(std::time::Instant::now() < deadline);
        }
        assert!(prepare().is_ok(), "released metadata capacity is reusable");
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lifetime_budget_follows_provider_snapshot_after_causal_retirement() {
        use crate::daemon::control::reply::{RetainedPluginResult, RetainedPluginResultBudget};
        use crate::plugin_entity::{Command, Completion};
        let (runtime, root) = publication_provider_runtime("lifetime-provider-payload");
        let bridge = runtime.entity_publish_bridge();
        let scope = runtime.causal_scopes.mint().unwrap();
        let reply = bridge.test_queue_publish(
            PluginKey("producer".into()),
            serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                "snapshot_seq":100, "id":"item"}),
            Some(scope),
        );
        runtime.step_entity_publish();
        assert!(reply.try_recv().unwrap().unwrap().ok);
        let (request, invocation) = runtime
            .prepare_plugin_entity_snapshot(
                "producer.item",
                "sub",
                RequestId("provider-payload".into()),
                None,
            )
            .unwrap();
        let result = runtime.invoke_plugin(request).result;
        runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .get_mut("producer.item")
            .unwrap()
            .resync
            .degraded = true;
        assert!(runtime.release_one_degraded_package_entity_resync_lease("producer.item"));
        assert_eq!(
            runtime.retire_plugin_entity_snapshot(&invocation),
            CausalTransitionStatus::Applied
        );
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert!(!runtime.causal_scopes.is_live(scope));
        assert_eq!(bridge.retained_counts().0, 1);
        let result_budget = RetainedPluginResultBudget::new();
        let bytes = serde_json::to_vec(&result).unwrap().len();
        let result = RetainedPluginResult::new(result, result_budget.try_reserve(bytes).unwrap());
        let mut permit = runtime.host_executor().try_reserve().unwrap();
        let Completion::Prepared { payload, .. } = crate::plugin_entity::execute(
            Command::Prepare {
                invocation,
                result,
                inconsistent: false,
                target: None,
            },
            &mut permit,
        ) else {
            panic!("provider result prepares a payload")
        };
        assert_eq!(payload.sequence(), Some(0));
        assert_eq!(bridge.retained_counts().0, 1);
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let target = Arc::new(crate::plugin_entity::Target {
            subscription_id: "sub".into(),
            entity_type: "producer.item".into(),
            sender: crate::subscription::entity::EntityFrameSender::Async(sender),
        });
        let Completion::Delivered { payload, .. } = crate::plugin_entity::execute(
            Command::Deliver {
                payload,
                target,
                publication_live: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                budget: crate::shared_view::SharedViewBudget::new(),
                resync_reason: None,
            },
            &mut permit,
        ) else {
            panic!("delivery retains its snapshot")
        };
        assert_eq!(bridge.retained_counts().0, 1);
        assert!(matches!(
            crate::plugin_entity::execute(Command::Reclaim(payload), &mut permit),
            Completion::Reclaimed
        ));
        assert_eq!(bridge.retained_counts(), (0, 0));
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lifetime_budget_survives_family_cleanup_until_payload_and_table_retire() {
        use super::family_cleanup::FamilyCleanupStep;
        let (runtime, root) = publication_provider_runtime("lifetime-cleanup");
        let bridge = runtime.entity_publish_bridge();
        let scope = runtime.causal_scopes.mint().unwrap();
        for (seq, scope_id) in [(2, Some(scope)), (3, None)] {
            let reply = bridge.test_queue_publish(
                PluginKey("producer".into()),
                serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                    "snapshot_seq":seq, "id":"item"}),
                scope_id,
            );
            runtime.step_entity_publish();
            assert!(reply.try_recv().unwrap().unwrap().ok);
        }
        let mut cleanup = HostPackageCleanup {
            unloaded_families: vec![("producer".into(), BTreeSet::from(["producer.item".into()]))],
            ..HostPackageCleanup::default()
        };
        runtime
            .begin_direct_package_entity_cleanup(&mut cleanup)
            .unwrap();
        assert_eq!(bridge.retained_counts().0, 2);
        let mut disposed = 0;
        loop {
            match runtime.step_direct_package_entity_cleanup(&mut cleanup) {
                FamilyCleanupStep::Pending => {}
                FamilyCleanupStep::Payload(payload) => {
                    assert_eq!(bridge.retained_counts().0, 2);
                    drop(payload);
                    disposed += 1;
                    assert_eq!(
                        bridge.retained_counts().0,
                        if disposed == 1 { 2 } else { 1 }
                    );
                    runtime.complete_direct_package_entity_cleanup_item(&mut cleanup);
                }
                FamilyCleanupStep::Complete => break,
                FamilyCleanupStep::Waiting | FamilyCleanupStep::Fault => {
                    panic!("cleanup capacity is available")
                }
            }
        }
        assert_eq!(disposed, 2);
        assert_eq!(bridge.retained_counts().0, 1);
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(bridge.retained_counts(), (0, 0));
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lifetime_budget_bounds_resync_scopes_after_publications_finish() {
        let (runtime, root) = publication_provider_runtime_with_body(
            "lifetime-resync-limit",
            &["producer.item"],
            r#"
                local ok, error = pcall(botster.entity_publish, {
                    type = "entity_remove", entity_type = "producer.item", snapshot_seq = 400, id = "item"
                })
                assert(not ok)
                assert(string.find(tostring(error), "capacity exhausted", 1, true))
            "#,
        );
        let bridge = runtime.entity_publish_bridge();
        for seq in 100..356 {
            let scope = runtime.causal_scopes.mint().unwrap();
            let reply = bridge.test_queue_publish(
                PluginKey("producer".into()),
                serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                    "snapshot_seq":seq, "id":"item"}),
                Some(scope),
            );
            runtime.step_entity_publish();
            while runtime.causal_operation_count() > 0 {
                runtime.apply_causal_owner_ops();
            }
            assert_eq!(
                reply.try_recv().unwrap().unwrap().status,
                PackageEntityPublishStatus::ResyncScheduled
            );
            assert_eq!(bridge.pending_publish_count(), 0);
            assert_eq!(bridge.retained_counts().0, (seq - 99) as usize);
        }
        let rejected = bridge.test_queue_publish(
            PluginKey("producer".into()),
            serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                "snapshot_seq":400, "id":"item"}),
            None,
        );
        assert!(
            rejected
                .try_recv()
                .unwrap()
                .unwrap_err()
                .contains("capacity exhausted")
        );
        assert_eq!(runtime.test_resync_scope_ids("producer.item").len(), 256);
        let (request, invocation) = runtime
            .prepare_plugin_entity_snapshot(
                "producer.item",
                "sub",
                RequestId("provider-at-capacity".into()),
                None,
            )
            .unwrap();
        let result = runtime.invoke_plugin(request).result;
        assert_eq!(
            runtime.retire_plugin_entity_snapshot(&invocation),
            CausalTransitionStatus::Applied
        );
        let (sequence, _) = runtime
            .complete_plugin_entity_snapshot(invocation, result)
            .unwrap();
        assert_eq!(sequence, 0);
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(bridge.retained_counts().0, 256);
        runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .get_mut("producer.item")
            .unwrap()
            .resync
            .degraded = true;
        for remaining in (0..256).rev() {
            assert!(runtime.release_one_degraded_package_entity_resync_lease("producer.item"));
            assert_eq!(bridge.retained_counts().0, remaining + 1);
            runtime.apply_causal_owner_ops();
            assert_eq!(bridge.retained_counts().0, remaining);
        }
        assert_eq!(bridge.retained_counts(), (0, 0));
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lifetime_budget_preserves_coalescing_and_provider_inheritance_before_transfer() {
        let (runtime, root) = publication_provider_runtime("lifetime-provider-inheritance");
        let bridge = runtime.entity_publish_bridge();
        let scope = runtime
            .causal_scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        let publish = |seq| {
            let reply = bridge.test_queue_publish(
                PluginKey("producer".into()),
                serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                    "snapshot_seq":seq, "id":"item"}),
                Some(scope),
            );
            runtime.step_entity_publish();
            assert!(reply.try_recv().unwrap().unwrap().ok);
        };
        publish(100);
        let first = runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .families["producer.item"]
            .resync
            .leases[&scope]
            .clone()
            .unwrap();
        publish(101);
        assert_eq!(bridge.retained_counts().0, 2);
        let (_, invocation) = runtime
            .prepare_plugin_entity_snapshot(
                "producer.item",
                "sub",
                RequestId("inherit-before-transfer".into()),
                None,
            )
            .unwrap();
        assert_eq!(invocation.admission.as_ref(), Some(&first));
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(bridge.retained_counts().0, 1);
        {
            let mut model = runtime
                .package_entities
                .lock()
                .expect("package entity model lock");
            let families = &mut model.families;
            let family = families.get_mut("producer.item").unwrap();
            family.forget_resync_lease(scope);
            assert!(matches!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: scope,
                    identity: LeaseIdentity::ProviderResyncNeed {
                        family_token: family.causal_token.unwrap()
                    }
                }),
                CausalAdmitResult::Applied
            ));
        }
        publish(102);
        let (_, replacement) = runtime
            .prepare_plugin_entity_snapshot(
                "producer.item",
                "sub",
                RequestId("inherit-recreated-need".into()),
                None,
            )
            .unwrap();
        assert_ne!(replacement.admission.as_ref(), Some(&first));
        assert_eq!(bridge.retained_counts().0, 2);
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(
            runtime.retire_plugin_entity_snapshot(&invocation),
            CausalTransitionStatus::Applied
        );
        runtime.apply_causal_owner_ops();
        drop(first);
        assert_eq!(bridge.retained_counts().0, 2);
        drop(invocation);
        assert_eq!(bridge.retained_counts().0, 1);
        runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .families
            .get_mut("producer.item")
            .unwrap()
            .resync
            .degraded = true;
        assert!(runtime.release_one_degraded_package_entity_resync_lease("producer.item"));
        assert_eq!(
            runtime.retire_plugin_entity_snapshot(&replacement),
            CausalTransitionStatus::Applied
        );
        drop(replacement);
        assert_eq!(bridge.retained_counts().0, 1);
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(bridge.retained_counts(), (0, 0));
        assert_eq!(
            runtime.causal_scopes.identities(scope),
            Some(BTreeSet::from([LeaseIdentity::EventInFlight]))
        );
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lifetime_budget_returns_capacity_while_the_event_root_remains_live() {
        let (runtime, root) = publication_provider_runtime("lifetime-sequential");
        let bridge = runtime.entity_publish_bridge();
        let scope = runtime
            .causal_scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        for seq in 1..=512 {
            let reply = bridge.test_queue_publish(
                PluginKey("producer".into()),
                serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                    "snapshot_seq":seq, "id":"item"}),
                Some(scope),
            );
            runtime.step_entity_publish();
            while runtime.entity_publish_retirement_pending() {
                runtime.advance_entity_publish();
                runtime.finish_entity_publish_retirement();
            }
            assert!(reply.try_recv().unwrap().unwrap().ok);
            let item = runtime.take_one_package_entity_fanout().unwrap();
            let TakenPackageEntityMutation {
                mutation, finish, ..
            } = item;
            drop(mutation);
            assert_eq!(
                runtime.finish_package_entity_fanout(&finish),
                CausalTransitionStatus::Applied
            );
            drop(finish);
            while runtime.causal_operation_count() > 0 {
                runtime.apply_causal_owner_ops();
            }
            assert_eq!(bridge.retained_counts(), (0, 0));
            assert_eq!(
                runtime.causal_scopes.identities(scope),
                Some(BTreeSet::from([LeaseIdentity::EventInFlight]))
            );
        }
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_disposal_preserves_resync_in_both_release_orders() {
        for resync_first in [false, true] {
            let (runtime, root) = publication_provider_runtime(if resync_first {
                "resync-first"
            } else {
                "disposal-first"
            });
            let scope = runtime.causal_scopes.mint().unwrap();
            let bridge = runtime.entity_publish_bridge();
            let response = bridge.test_queue_publish(PluginKey("producer".into()),
                serde_json::json!({"type": "entity_remove", "entity_type": "producer.item", "snapshot_seq": 100, "id": "item"}), Some(scope));
            let payload = runtime
                .begin_entity_publish()
                .expect("out-of-window publication returns its original payload");
            runtime.mark_entity_publish_daemon_owned();
            let pending = LeaseIdentity::PendingEntityPublish {
                publication_token: 1,
            };
            let resync = LeaseIdentity::ProviderResyncNeed {
                family_token: runtime.test_family_causal_token("producer.item"),
            };
            if resync_first {
                assert!(matches!(
                    runtime.admit_causal_op(CausalOp::Release {
                        scope_id: scope,
                        identity: resync.clone()
                    }),
                    CausalAdmitResult::Applied
                ));
                runtime.apply_causal_owner_ops();
                runtime.apply_causal_owner_ops();
                assert_eq!(
                    runtime.causal_scopes.identities(scope).unwrap(),
                    BTreeSet::from([pending.clone()])
                );
            }
            drop(payload);
            runtime.complete_entity_publish_disposal();
            assert_eq!(
                runtime.finish_entity_publish_retirement(),
                CausalTransitionStatus::Applied
            );
            assert_eq!(
                response.try_recv().unwrap().unwrap().status,
                PackageEntityPublishStatus::ResyncScheduled
            );
            if !resync_first {
                runtime.apply_causal_owner_ops();
                runtime.apply_causal_owner_ops();
                assert_eq!(
                    runtime.causal_scopes.identities(scope).unwrap(),
                    BTreeSet::from([resync.clone()])
                );
                assert!(matches!(
                    runtime.admit_causal_op(CausalOp::Release {
                        scope_id: scope,
                        identity: resync
                    }),
                    CausalAdmitResult::Applied
                ));
            }
            runtime.apply_causal_owner_ops();
            assert!(!runtime.causal_scopes.is_live(scope));
            drop(runtime);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn direct_family_cleanup_checks_exhaustion_before_effects() {
        let mut runtime = family_runtime("direct-family-cleanup");
        let registry = PackageRegistry::new(Default::default());
        assert!(
            runtime
                .load_lua_plugin_package(&registry, "absent")
                .is_err()
        );
        assert_eq!(runtime.package_entities.lock().unwrap().epoch, 0);
        runtime.test_set_family_seq("producer.item", 1);
        runtime
            .unload_plugin_package(RequestId("direct-unload".into()), "producer")
            .expect("direct unload reserves its boundary");
        assert_eq!(runtime.package_entities.lock().unwrap().epoch, 1);
        assert_eq!(
            runtime.package_entity_family_generation("producer.item"),
            None
        );

        runtime.test_set_family_seq("producer.item", 2);
        let generation = runtime.package_entity_family_generation("producer.item");
        runtime.test_exhaust_package_entity_epochs();
        assert!(matches!(
            runtime.unload_plugin_package(RequestId("refused-unload".into()), "producer"),
            Err(PackageEntityCleanupError::GenerationExhausted)
        ));
        assert_eq!(
            runtime.package_entity_family_generation("producer.item"),
            generation
        );
        assert_eq!(runtime.package_entities.lock().unwrap().epoch, u64::MAX);
        let error = runtime
            .load_lua_plugin_package(&registry, "absent")
            .expect_err("load must reserve capacity for rollback before execution");
        assert!(matches!(
            error,
            HubLuaPluginLoadError::EntityFamilyCleanup(
                PackageEntityCleanupError::GenerationExhausted
            )
        ));
        assert!(!error.is_package_scoped_startup_failure());
        assert_eq!(error.code(), "entity_family_generation_exhausted");
    }

    #[test]
    fn old_family_releases_preserve_recreated_mutation_and_resync_leases() {
        let runtime = family_runtime("family-release-generation");
        let family = "producer.item";
        runtime.test_set_family_seq(family, 1);
        runtime.test_set_family_seq("other.item", 1);
        let old_token = runtime.test_family_causal_token(family);
        let old_generation = runtime.package_entity_family_generation(family).unwrap();
        let scope_id = runtime
            .causal_scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        let identities = |family_token| {
            [
                LeaseIdentity::AdmittedEntityMutation {
                    family_token,
                    seq: 1,
                },
                LeaseIdentity::ProviderResyncNeed { family_token },
            ]
        };
        for identity in identities(old_token) {
            assert!(runtime.causal_scopes.acquire(scope_id, identity));
        }
        let retained = runtime.causal_scopes.test_with_inner_held(|| {
            for _ in 0..CAUSAL_OWNER_CAPACITY {
                assert_eq!(
                    runtime.admit_causal_op(CausalOp::Release {
                        scope_id: u64::MAX,
                        identity: LeaseIdentity::EventInFlight
                    }),
                    CausalAdmitResult::Applied
                );
            }
            identities(old_token).map(|identity| {
                let CausalAdmitResult::Retry(op) =
                    runtime.admit_causal_op(CausalOp::Release { scope_id, identity })
                else {
                    panic!("the caller must retain the rejected old release")
                };
                op
            })
        });
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        runtime
            .drop_package_entity_families_for("producer")
            .unwrap();
        assert_eq!(
            runtime.package_entity_family_generation("other.item"),
            Some(old_generation)
        );
        runtime.test_set_family_seq(family, 1);
        let new_generation = runtime.package_entity_family_generation(family).unwrap();
        assert_ne!(new_generation, old_generation);
        let new_token = runtime.test_family_causal_token(family);
        assert_ne!(new_token, old_token);
        for identity in identities(new_token) {
            assert!(runtime.causal_scopes.acquire(scope_id, identity));
        }
        for op in retained {
            assert_eq!(runtime.admit_causal_op(op), CausalAdmitResult::Applied);
        }
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        let live = runtime.causal_scopes.identities(scope_id).unwrap();
        for identity in identities(new_token) {
            assert!(live.contains(&identity));
        }
        for identity in identities(old_token) {
            assert!(!live.contains(&identity));
        }
        runtime.take_package_entity_resync_notification();
        assert_eq!(
            runtime.finish_package_entity_fanout(&PackageEntityFanoutFinish {
                lease: Some(EntityMutationLease {
                    admission: None,
                    family_token: old_token,
                    scope_id,
                    family: family.into(),
                    generation: old_generation,
                    seq: 1,
                }),
                scheduled_resync: true,
            }),
            CausalTransitionStatus::Applied
        );
        assert!(!runtime.take_package_entity_resync_notification());
        let model = runtime
            .package_entities
            .lock()
            .expect("package entity model lock");
        let families = &model.families;
        assert_eq!(families[family].generation, new_generation);
        assert!(!families[family].resync.needed);
    }

    #[test]
    fn retained_causal_reservations_suppress_family_release_readiness_until_capacity_returns() {
        let runtime = family_runtime("causal-reservation-readiness");
        let family_token = runtime.test_family_causal_token("producer.item");
        let scope = runtime
            .causal_scopes()
            .mint_with_lease(Some(LeaseIdentity::ProviderResyncNeed { family_token }))
            .unwrap();
        runtime.test_store_resync_lease(scope, "producer.item");
        assert!(runtime.causal_family_release_ready());
        let mut reservations: Vec<_> = (0..CAUSAL_OWNER_CAPACITY)
            .map(|_| runtime.reserve_causal_transition().unwrap())
            .collect();
        assert_eq!(runtime.causal_operation_count(), 0);
        assert!(!runtime.causal_family_release_ready());
        drop(reservations.pop());
        assert!(runtime.take_causal_capacity_notification());
        assert!(runtime.causal_family_release_ready());
        runtime.retry_family_resync_release();
        assert_eq!(runtime.causal_operation_count(), 1);
        assert!(!runtime.causal_family_release_ready());
        assert!(runtime.causal_scopes().is_live(scope));
        runtime.apply_causal_owner_ops();
        assert!(!runtime.causal_scopes().is_live(scope));
        drop(reservations);
    }

    #[test]
    fn causal_receipt_waits_for_its_exact_table_application_under_contention() {
        let runtime = family_runtime("causal-receipt-order");
        let scopes = runtime.causal_scopes();
        let scope = scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        let provider = LeaseIdentity::ProviderInFlight {
            invocation_token: 17,
        };
        assert!(scopes.acquire(scope, provider));
        let retained_reservation = runtime.reserve_causal_transition().unwrap();
        let first = runtime
            .reserve_causal_transition()
            .unwrap()
            .commit(CausalOp::Release {
                scope_id: scope,
                identity: LeaseIdentity::EventInFlight,
            });
        let retained = retained_reservation.commit(CausalOp::Release {
            scope_id: scope,
            identity: provider,
        });
        assert!(!first.is_applied());
        assert!(!retained.is_applied());
        scopes.test_with_inner_held(|| runtime.apply_causal_owner_ops());
        assert_eq!(runtime.causal_operation_count(), 2);
        assert!(!first.is_applied());
        assert!(!retained.is_applied());
        runtime.apply_causal_owner_ops();
        assert!(first.is_applied());
        assert!(!retained.is_applied());
        assert_eq!(
            scopes.identities(scope).unwrap(),
            BTreeSet::from([provider])
        );
        runtime.apply_causal_owner_ops();
        assert!(retained.is_applied());
        assert!(!scopes.is_live(scope));
        assert_eq!(runtime.causal_operation_count(), 0);
    }

    #[test]
    fn poisoned_causal_table_keeps_retry_ownership_without_ready_polling() {
        let runtime = family_runtime("causal-poison-readiness");
        let op = CausalOp::Release {
            scope_id: 1,
            identity: LeaseIdentity::EventInFlight,
        };
        assert_eq!(
            runtime.admit_causal_op(op.clone()),
            CausalAdmitResult::Applied
        );
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime
                .causal_scopes
                .test_with_inner_held(|| panic!("poison causal inner"));
        }));
        assert!(poisoned.is_err());
        assert_eq!(runtime.causal_operation_count(), 1);
        assert!(runtime.causal_owner_ops_pending());
        assert!(!runtime.causal_owner_ops_ready());
        assert_eq!(runtime.causal_queue.take_head(), Some(op));
    }

    #[test]
    fn family_resync_release_index_matches_live_state_after_each_transition() {
        let runtime = family_runtime("family-release-index");
        let assert_index = || {
            let model = runtime
                .package_entities
                .lock()
                .expect("package entity model lock");
            let families = &model.families;
            let expected = families
                .iter()
                .filter(|(_, family)| !family.resync.needed && !family.resync.leases.is_empty())
                .map(|(name, family)| (name.clone(), family.generation))
                .collect::<BTreeSet<_>>();
            assert_eq!(model.resync_releases, expected);
        };
        assert_index();
        for family in ["producer.a", "producer.b", "other.item"] {
            runtime.test_store_resync_lease(1, family);
            assert_index();
            runtime.mark_package_entity_resync_needed(family);
            assert_index();
            runtime.rearm_package_entity_resync(family);
            assert_index();
            runtime.begin_package_entity_provider_snapshot(family, 1);
            assert_index();
        }
        runtime.step_package_entity_provider_snapshot("producer.a");
        assert_index();
        runtime.retry_family_resync_release();
        assert_index();
        runtime
            .drop_package_entity_families_for("producer")
            .unwrap();
        assert_index();
        runtime.test_store_resync_lease(2, "producer.a");
        assert_index();
        assert_eq!(
            runtime.package_entity_family_generation("producer.a"),
            Some(1)
        );
        runtime.retry_family_resync_release();
        assert_index();
        runtime.retry_family_resync_release();
        assert_index();
        assert!(
            runtime
                .package_entities
                .lock()
                .unwrap()
                .resync_releases
                .is_empty()
        );
    }

    #[test]
    fn rejected_family_release_stays_in_cursor_before_next_payload() {
        use super::family_cleanup::FamilyCleanupStep;
        let runtime = family_runtime("retained-family-release");
        let family = "producer.item";
        let identity = LeaseIdentity::AdmittedEntityMutation {
            family_token: 1,
            seq: 1,
        };
        let scope = runtime
            .causal_scopes
            .mint_with_lease(Some(identity.clone()))
            .unwrap();
        for seq in [1, 2] {
            runtime.test_store_family_payload(PackageEntityMutation::Upsert {
                admission: None,
                entity_type: family.into(),
                snapshot_seq: seq,
                id: "item".into(),
                entity: serde_json::json!({"id": "item"}),
            });
        }
        runtime.test_store_pending_lease(scope, family, 1);
        let mut cleanup = HostPackageCleanup {
            unloaded_families: vec![("producer".into(), BTreeSet::from([family.into()]))],
            ..HostPackageCleanup::default()
        };
        runtime
            .begin_direct_package_entity_cleanup(&mut cleanup)
            .unwrap();
        assert!(matches!(
            runtime.step_direct_package_entity_cleanup(&mut cleanup),
            FamilyCleanupStep::Pending
        ));
        let FamilyCleanupStep::Payload(payload) =
            runtime.step_direct_package_entity_cleanup(&mut cleanup)
        else {
            panic!("select first payload");
        };
        assert_eq!(payload.snapshot_seq(), 1);
        drop(payload);
        runtime.complete_direct_package_entity_cleanup_item(&mut cleanup);
        runtime.causal_scopes.test_with_inner_held(|| {
            for _ in 0..CAUSAL_OWNER_CAPACITY {
                assert_eq!(
                    runtime.admit_causal_op(CausalOp::Release {
                        scope_id: u64::MAX,
                        identity: LeaseIdentity::EventInFlight,
                    }),
                    CausalAdmitResult::Applied
                );
            }
            runtime.causal_scopes.take_progress_notification();
            for _ in 0..2 {
                assert!(matches!(
                    runtime.step_direct_package_entity_cleanup(&mut cleanup),
                    FamilyCleanupStep::Waiting
                ));
                assert_eq!(
                    cleanup.family_cursor.release,
                    Some(CausalOp::Release {
                        scope_id: scope,
                        identity: identity.clone()
                    })
                );
                assert_eq!(runtime.causal_operation_count(), CAUSAL_OWNER_CAPACITY);
                assert!(
                    !runtime.causal_scopes.take_progress_notification(),
                    "a full refusal must not wake itself"
                );
            }
        });
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert!(runtime.take_causal_capacity_notification());
        assert!(matches!(
            runtime.step_direct_package_entity_cleanup(&mut cleanup),
            FamilyCleanupStep::Pending
        ));
        assert!(cleanup.family_cursor.release.is_none());
        runtime.apply_causal_owner_ops();
        assert!(!runtime.causal_scopes.is_live(scope));
        let FamilyCleanupStep::Payload(payload) =
            runtime.step_direct_package_entity_cleanup(&mut cleanup)
        else {
            panic!("select second payload only after release admission");
        };
        assert_eq!(payload.snapshot_seq(), 2);
        drop(payload);
        runtime.complete_direct_package_entity_cleanup_item(&mut cleanup);
        runtime.drain_direct_package_entity_cleanup(&mut cleanup);
    }

    #[test]
    fn retained_family_cleanup_preserves_recreation_and_holds_payload_lease() {
        use super::family_cleanup::FamilyCleanupStep;
        let runtime = family_runtime("retained-family-cleanup");
        let family = "producer.item";
        let payload = || PackageEntityMutation::Upsert {
            admission: None,
            entity_type: family.into(),
            snapshot_seq: 1,
            id: "item".into(),
            entity: serde_json::json!({"id": "item"}),
        };
        runtime.test_store_family_payload(payload());
        let scope = runtime
            .causal_scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        let old_token = runtime.test_family_causal_token(family);
        let identity = |family_token| LeaseIdentity::AdmittedEntityMutation {
            family_token,
            seq: 1,
        };
        let resync_identity = |family_token| LeaseIdentity::ProviderResyncNeed { family_token };
        assert!(runtime.causal_scopes.acquire(scope, identity(old_token)));
        assert!(
            runtime
                .causal_scopes
                .acquire(scope, resync_identity(old_token))
        );
        runtime.test_store_pending_lease(scope, family, 1);
        runtime.test_store_resync_lease(scope, family);
        let mut cleanup = HostPackageCleanup {
            unloaded_families: vec![("producer".into(), BTreeSet::from([family.into()]))],
            ..HostPackageCleanup::default()
        };
        runtime
            .begin_direct_package_entity_cleanup(&mut cleanup)
            .unwrap();
        assert!(matches!(
            runtime.step_direct_package_entity_cleanup(&mut cleanup),
            FamilyCleanupStep::Pending
        ));
        assert!(!runtime.test_family_exists(family));
        runtime.test_store_family_payload(payload());
        runtime.test_store_pending_lease(scope, family, 1);
        let new_token = runtime.test_family_causal_token(family);
        assert_ne!(old_token, new_token);
        assert!(runtime.causal_scopes.acquire(scope, identity(new_token)));
        assert!(
            runtime
                .causal_scopes
                .acquire(scope, resync_identity(new_token))
        );
        runtime.test_store_resync_lease(scope, family);
        let FamilyCleanupStep::Payload(old_payload) =
            runtime.step_direct_package_entity_cleanup(&mut cleanup)
        else {
            panic!("select the old detached payload");
        };
        assert!(
            runtime
                .causal_scopes
                .identities(scope)
                .unwrap()
                .contains(&identity(old_token))
        );
        assert_eq!(
            runtime
                .package_entities
                .lock()
                .expect("package entity model lock")
                .families[family]
                .pending_by_seq
                .len(),
            1
        );
        drop(old_payload);
        runtime.complete_direct_package_entity_cleanup_item(&mut cleanup);
        for _ in 0..20 {
            match runtime.step_direct_package_entity_cleanup(&mut cleanup) {
                FamilyCleanupStep::Complete => break,
                FamilyCleanupStep::Pending => {}
                FamilyCleanupStep::Waiting | FamilyCleanupStep::Fault => {
                    panic!("the causal table is available")
                }
                FamilyCleanupStep::Payload(_) => panic!("new payload must remain live"),
            }
        }
        assert!(cleanup.unloaded_families.is_empty());
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        let live = runtime.causal_scopes.identities(scope).unwrap();
        assert!(!live.contains(&identity(old_token)));
        assert!(live.contains(&identity(new_token)));
        assert!(!live.contains(&resync_identity(old_token)));
        assert!(live.contains(&resync_identity(new_token)));
        let model = runtime
            .package_entities
            .lock()
            .expect("package entity model lock");
        let families = &model.families;
        assert_eq!(families[family].generation, 1);
        assert_eq!(families[family].pending_by_seq.len(), 1);
        assert_eq!(families[family].pending_leases.len(), 1);
        assert_eq!(families[family].resync.leases.len(), 1);
    }

    #[test]
    fn family_boundary_retry_and_exhaustion_preserve_exact_state() {
        let runtime = family_runtime("family-boundary-generation");
        runtime.test_set_family_seq("producer.item", 1);
        let mut cleanup = HostPackageCleanup::default();
        cleanup
            .unloaded_families
            .push(("producer".into(), BTreeSet::from(["producer.item".into()])));
        runtime
            .begin_direct_package_entity_cleanup(&mut cleanup)
            .unwrap();
        let epoch = runtime.package_entities.lock().unwrap().epoch;
        runtime
            .begin_direct_package_entity_cleanup(&mut cleanup)
            .unwrap();
        assert_eq!(runtime.package_entities.lock().unwrap().epoch, epoch);
        assert_eq!(cleanup.family_epoch, Some(epoch));
        assert_eq!(
            runtime.package_entity_family_generation("producer.item"),
            Some(0)
        );
        runtime.test_exhaust_package_entity_epochs();
        let mut refused = HostPackageCleanup::default();
        refused
            .unloaded_families
            .push(("producer".into(), BTreeSet::from(["producer.item".into()])));
        assert_eq!(
            runtime.begin_direct_package_entity_cleanup(&mut refused),
            Err(PackageEntityCleanupError::GenerationExhausted)
        );
        assert_eq!(refused.family_epoch, None);
        assert_eq!(refused.unloaded_families.len(), 1);
        assert_eq!(
            runtime.package_entity_family_generation("producer.item"),
            Some(0)
        );
    }

    fn completed_entity_snapshot(payload: serde_json::Value) -> PluginInvocationResult {
        PluginInvocationResult::Completed(botster_core::PluginInvocationSuccess {
            request_id: RequestId("entity-snapshot-test".to_string()),
            handler: botster_core::PluginHandlerRef {
                plugin_key: PluginKey("project-pipelines".to_string()),
                kind: PluginHandlerKind::EntityProvider,
                handler_id: "runs".to_string(),
            },
            payload: Some(BoundaryJson(payload)),
        })
    }

    #[test]
    fn convert_plugin_entity_snapshot_accepts_valid_snapshot() {
        let expected = EntityKind("project-pipelines.run".to_string());
        let (snapshot_seq, items) = HubRuntime::convert_plugin_entity_snapshot(
            &expected,
            completed_entity_snapshot(serde_json::json!({
                "type": "entity_snapshot",
                "entity_type": "project-pipelines.run",
                "snapshot_seq": 7,
                "items": [{ "id": "run-1", "status": "ready" }]
            })),
        )
        .expect("valid provider snapshot");

        assert_eq!(snapshot_seq, 7);
        assert_eq!(
            items,
            vec![serde_json::json!({ "id": "run-1", "status": "ready" })]
        );
    }

    #[test]
    fn convert_plugin_entity_snapshot_rejects_wrong_family() {
        let expected = EntityKind("project-pipelines.run".to_string());
        let error = HubRuntime::convert_plugin_entity_snapshot(
            &expected,
            completed_entity_snapshot(serde_json::json!({
                "type": "entity_snapshot",
                "entity_type": "project-pipelines.ticket",
                "snapshot_seq": 1,
                "items": []
            })),
        )
        .expect_err("wrong provider family must fail");

        assert_eq!(error.code, "invalid_entity_provider");
        assert!(error.message.contains("returned wrong family"));
    }

    #[test]
    fn convert_plugin_entity_snapshot_rejects_duplicate_ids() {
        let expected = EntityKind("project-pipelines.run".to_string());
        let error = HubRuntime::convert_plugin_entity_snapshot(
            &expected,
            completed_entity_snapshot(serde_json::json!({
                "type": "entity_snapshot",
                "entity_type": "project-pipelines.run",
                "snapshot_seq": 1,
                "items": [{ "id": "run-1" }, { "id": "run-1" }]
            })),
        )
        .expect_err("duplicate provider record ids must fail");

        assert_eq!(error.code, "invalid_entity_provider");
        assert!(error.message.contains("duplicate record id run-1"));
    }

    #[test]
    fn convert_plugin_entity_snapshot_rejects_non_snapshot_frame() {
        let expected = EntityKind("project-pipelines.run".to_string());
        let error = HubRuntime::convert_plugin_entity_snapshot(
            &expected,
            completed_entity_snapshot(serde_json::json!({
                "type": "entity_upsert",
                "entity_type": "project-pipelines.run",
                "snapshot_seq": 1,
                "id": "run-1",
                "entity": { "id": "run-1" }
            })),
        )
        .expect_err("non-snapshot provider frame must fail");

        assert_eq!(error.code, "invalid_entity_provider");
        assert!(
            error
                .message
                .contains("authoritative whole-family snapshot")
        );
    }

    #[test]
    fn convert_plugin_entity_snapshot_rejects_invalid_record() {
        let expected = EntityKind("project-pipelines.run".to_string());
        let error = HubRuntime::convert_plugin_entity_snapshot(
            &expected,
            completed_entity_snapshot(serde_json::json!({
                "type": "entity_snapshot",
                "entity_type": "project-pipelines.run",
                "snapshot_seq": 1,
                "items": [{ "status": "missing-id" }]
            })),
        )
        .expect_err("provider record without its id must fail");

        assert_eq!(error.code, "invalid_entity_provider");
    }

    #[test]
    fn startup_plugin_failure_classification_is_fail_closed() {
        let package_failures = [
            EventPlaneStatus::RejectedUndeclared,
            EventPlaneStatus::RejectedForeign,
            EventPlaneStatus::RejectedInvalid,
            EventPlaneStatus::RejectedOversize,
            EventPlaneStatus::RejectedWildcard,
            EventPlaneStatus::RejectedCausalScope,
            EventPlaneStatus::RejectedAudience,
        ];
        for status in package_failures {
            assert!(
                HubLuaPluginLoadError::EventPlane(status).is_package_scoped_startup_failure(),
                "{status:?} must isolate only the failing package"
            );
        }
        let infrastructure_failures = [
            EventPlaneStatus::ShedFull,
            EventPlaneStatus::ShedBusy,
            EventPlaneStatus::RejectedOverRate,
            EventPlaneStatus::RejectedOverFanout,
        ];
        for status in infrastructure_failures {
            assert!(
                !HubLuaPluginLoadError::EventPlane(status).is_package_scoped_startup_failure(),
                "{status:?} must stop startup"
            );
        }
        assert!(
            !HubLuaPluginLoadError::EventPlane(EventPlaneStatus::Accepted)
                .is_package_scoped_startup_failure()
        );
        assert!(
            HubLuaPluginLoadError::Package(PackageRegistryError::without_record(
                "broken.plugin",
                crate::PackageAction::Show,
                crate::PackageAdmissionReason::PackageNotInstalled,
                "classification test".to_string(),
            ))
            .is_package_scoped_startup_failure()
        );
        assert!(
            HubLuaPluginLoadError::Lua(LuaPluginRuntimeError::Load("broken".to_string()))
                .is_package_scoped_startup_failure()
        );
        assert!(
            HubLuaPluginLoadError::Lifecycle(crate::HubLifecycleError::MissingEntrypoint {
                package_name: "broken.plugin".to_string(),
            })
            .is_package_scoped_startup_failure()
        );
    }

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

        validate_plugin_surface_node(&node, &BTreeSet::new())
            .expect("/session and item-relative bindings are admitted");
    }

    #[test]
    fn plugin_surface_render_admission_scopes_bound_identity_to_item_templates() {
        let admitted = binding_test_node(serde_json::json!({
            "$kind": "bind_list",
            "source": "/session",
            "where": { "lifecycle_class": "current" },
            "item_template": {
                "type": "inline",
                "id": { "$bind": "@/session_uuid" },
                "children": [{
                    "type": "button",
                    "id": { "$kind": "bind_list_descendant_id", "key": "remove" },
                    "props": {
                        "label": { "$bind": "@/lifecycle_class" },
                        "action": { "id": "contract.action" }
                    }
                }]
            }
        }));
        validate_plugin_surface_node(&admitted, &BTreeSet::new())
            .expect("render admission accepts bound item-template identity");

        for rejected in [
            serde_json::json!({
                "type": "button",
                "id": { "$bind": "@/session_uuid" },
                "props": {
                    "label": "Select session",
                    "action": { "id": "contract.action" }
                }
            }),
            serde_json::json!({
                "type": "panel",
                "id": "binding-root",
                "children": [{
                    "type": "button",
                    "id": { "$bind": "@/session_uuid" },
                    "props": {
                        "label": "Select session",
                        "action": { "id": "contract.action" }
                    }
                }]
            }),
        ] {
            let node = serde_json::from_value(rejected).expect("authored UiNode");
            let error = validate_plugin_surface_node(&node, &BTreeSet::new())
                .expect_err("unresolved render id must fail");
            assert_eq!(error.code, "invalid_surface");
            assert!(error.message.contains("bind_list item_template"));
        }

        for rejected in [
            serde_json::json!({
                "type": "button",
                "id": { "$kind": "bind_list_descendant_id", "key": "remove" },
                "props": {
                    "label": "Remove session",
                    "action": { "id": "contract.action" }
                }
            }),
            serde_json::json!({
                "type": "panel",
                "id": "binding-root",
                "children": [{
                    "$kind": "bind_list",
                    "source": "/session",
                    "item_template": {
                        "type": "button",
                        "id": { "$kind": "bind_list_descendant_id", "key": "remove" },
                        "props": {
                            "label": "Remove session",
                            "action": { "id": "contract.action" }
                        }
                    }
                }]
            }),
        ] {
            let node = serde_json::from_value(rejected).expect("authored keyed UiNode");
            let error = validate_plugin_surface_node(&node, &BTreeSet::new())
                .expect_err("misplaced descendant identity must fail");
            assert_eq!(error.code, "invalid_surface");
            assert!(error.message.contains("bind_list descendant identity"));
        }
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
            let error = validate_plugin_surface_binding_families(&node, &BTreeSet::new())
                .expect_err("foreign absolute binding family must be rejected");
            assert_eq!(error.code, "invalid_surface");
            assert!(error.message.contains(source), "{error:?}");
        }
    }

    #[test]
    fn plugin_surface_binding_admission_accepts_only_exact_declared_plugin_family() {
        let admitted = BTreeSet::from(["project-pipelines.run".to_string()]);
        let node = binding_test_node(serde_json::json!({
            "$kind": "bind_list",
            "source": "/project-pipelines.run",
            "item_template": {
                "type": "text",
                "id": "run-row",
                "props": { "text": { "$bind": "@/id" } }
            }
        }));
        validate_plugin_surface_binding_families(&node, &admitted)
            .expect("exact declared plugin family is admitted");

        for source in [
            "/project-pipelines.ticket",
            "/project-pipelines.runaway",
            "/other.run",
        ] {
            let node = binding_test_node(serde_json::json!({
                "$kind": "bind_list",
                "source": source,
                "item_template": {
                    "type": "text",
                    "id": "row",
                    "props": { "text": "row" }
                }
            }));
            validate_plugin_surface_binding_families(&node, &admitted)
                .expect_err("undeclared or foreign family must remain rejected");
        }
    }

    #[test]
    fn plugin_surface_entity_options_admission_accepts_session_and_declared_exclude() {
        let admitted = BTreeSet::from(["project-pipelines.run".to_string()]);
        let node = binding_test_node(serde_json::json!({
            "type": "select",
            "id": "session-select",
            "props": {
                "name": "session",
                "label": "Session",
                "options_source": {
                    "$kind": "entity_options",
                    "source": "/session",
                    "value_field": "session_uuid",
                    "display_fields": ["label"],
                    "order": ["label", "session_uuid"],
                    "exclude": {
                        "source": "/project-pipelines.run",
                        "value_field": "session_uuid"
                    }
                }
            }
        }));
        validate_plugin_surface_node(&node, &admitted)
            .expect("session source and declared package exclude are admitted");
    }

    #[test]
    fn plugin_surface_entity_options_admission_rejects_undeclared_source_and_exclude() {
        let admitted = BTreeSet::from(["project-pipelines.run".to_string()]);
        for options_source in [
            serde_json::json!({
                "$kind": "entity_options",
                "source": "/project-pipelines.ticket",
                "value_field": "id",
                "display_fields": ["label"],
                "order": ["label"]
            }),
            serde_json::json!({
                "$kind": "entity_options",
                "source": "/session",
                "value_field": "session_uuid",
                "display_fields": ["label"],
                "order": ["label"],
                "exclude": {
                    "source": "/project-pipelines.ticket",
                    "value_field": "session_uuid"
                }
            }),
        ] {
            let node = binding_test_node(serde_json::json!({
                "type": "select",
                "id": "session-select",
                "props": {
                    "name": "session",
                    "label": "Session",
                    "options_source": options_source
                }
            }));
            let error = validate_plugin_surface_node(&node, &admitted)
                .expect_err("undeclared entity-options family must fail");
            assert_eq!(error.code, "invalid_surface");
        }
    }

    #[test]
    fn plugin_surface_entity_options_action_result_uses_same_admission() {
        let admitted = BTreeSet::from(["project-pipelines.run".to_string()]);
        let (request, mut result) = binding_action_result("/session");
        result.replacement = Some(Box::new(
            serde_json::from_value(serde_json::json!({
                "type": "select",
                "id": "session-select",
                "props": {
                    "name": "session",
                    "label": "Session",
                    "options_source": {
                        "$kind": "entity_options",
                        "source": "/session",
                        "value_field": "session_uuid",
                        "display_fields": ["label"],
                        "order": ["label"],
                        "exclude": {
                            "source": "/project-pipelines.run",
                            "value_field": "session_uuid"
                        }
                    }
                }
            }))
            .expect("entity-options replacement"),
        ));
        validate_plugin_surface_action_result(&result, &request, &admitted)
            .expect("action-result entity-options admitted with declared families");

        result.replacement = Some(Box::new(
            serde_json::from_value(serde_json::json!({
                "type": "select",
                "id": "session-select",
                "props": {
                    "name": "session",
                    "label": "Session",
                    "options_source": {
                        "$kind": "entity_options",
                        "source": "/session",
                        "value_field": "session_uuid",
                        "display_fields": ["label"],
                        "order": ["label"],
                        "exclude": {
                            "source": "/project-pipelines.ticket",
                            "value_field": "session_uuid"
                        }
                    }
                }
            }))
            .expect("rejected entity-options replacement"),
        ));
        let error = validate_plugin_surface_action_result(&result, &request, &admitted)
            .expect_err("undeclared exclude family must fail action result");
        assert_eq!(error.code, "invalid_action_result");
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
                        "type": "button",
                        "id": "binding-action-row",
                        "props": {
                            "label": { "$bind": "@/lifecycle_class" },
                            "action": { "id": "contract.action" }
                        }
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
        validate_plugin_surface_action_result(&accepted, &request, &BTreeSet::new())
            .expect("/session replacement binding must be admitted");

        let (_, rejected) = binding_action_result("/workspace");
        let error = validate_plugin_surface_action_result(&rejected, &request, &BTreeSet::new())
            .expect_err("foreign replacement binding must be rejected");
        assert_eq!(error.code, "invalid_action_result");
        assert!(error.message.contains("/workspace"), "{error:?}");
    }

    #[test]
    fn plugin_surface_authored_admission_rejects_malformed_required_label_bind() {
        let node: UiNode = serde_json::from_value(serde_json::json!({
            "type": "button",
            "id": "bound-button",
            "props": {
                "label": { "$bind": "@/lifecycle_class", "fallback": "current" },
                "action": { "id": "contract.action" }
            }
        }))
        .expect("authored button wire shape");

        let error = validate_plugin_surface_node(&node, &BTreeSet::new())
            .expect_err("malformed required label binding must fail Hub admission");
        assert_eq!(error.code, "invalid_surface");
        assert!(
            error.message.contains("may only contain $bind"),
            "{error:?}"
        );
    }

    #[test]
    fn plugin_surface_action_replacement_rejects_bound_root_and_static_child_identity() {
        for replacement in [
            serde_json::json!({
                "type": "button",
                "id": { "$bind": "@/session_uuid" },
                "props": {
                    "label": "Select session",
                    "action": { "id": "contract.action" }
                }
            }),
            serde_json::json!({
                "type": "panel",
                "id": "replacement-root",
                "children": [{
                    "type": "button",
                    "id": { "$bind": "@/session_uuid" },
                    "props": {
                        "label": "Select session",
                        "action": { "id": "contract.action" }
                    }
                }]
            }),
        ] {
            let request = serde_json::from_value::<UiActionRequest>(serde_json::json!({
                "request_id": "binding-action-request",
                "surface_id": "contract.sessions",
                "action_id": "contract.action",
                "node_id": "session-stable-current",
                "kind": "submit"
            }))
            .expect("action request");
            let result = serde_json::from_value::<UiActionResult>(serde_json::json!({
                "request_id": "binding-action-request",
                "surface_id": "contract.sessions",
                "action_id": "contract.action",
                "node_id": "session-stable-current",
                "state": "accepted",
                "replacement": replacement
            }))
            .expect("action result");
            let error = validate_plugin_surface_action_result(&result, &request, &BTreeSet::new())
                .expect_err("unresolved replacement id must fail");
            assert_eq!(error.code, "invalid_action_result");
            assert!(error.message.contains("bind_list item_template"));
        }
    }

    #[test]
    fn managed_session_core_error_diagnostic_is_kind_based_and_path_neutral() {
        let spawn_failed = CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
            MultiplexerEngineError::Runtime(botster_core::SessionRuntimeError::new(
                SessionRuntimeErrorKind::SpawnFailed,
                "connect worker control socket failed: /private/raw/path: worker control socket parent must be owned by the effective user with private permissions",
            )),
        ));
        assert_eq!(
            managed_session_core_error_class(&spawn_failed),
            "runtime.spawn_failed"
        );
        assert!(!managed_session_core_error_class(&spawn_failed).contains('/'));

        let generic = CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
            MultiplexerEngineError::Runtime(botster_core::SessionRuntimeError::new(
                SessionRuntimeErrorKind::SpawnFailed,
                "runtime detail that must not cross the diagnostic boundary",
            )),
        ));
        assert_eq!(
            managed_session_core_error_class(&generic),
            "runtime.spawn_failed"
        );
    }

    #[test]
    fn session_reservation_refusal_classes_are_kind_based_and_path_neutral() {
        let mapped = [
            (
                SessionReservationRefusal::Occupied,
                "session_reservation.occupied",
            ),
            (SessionReservationRefusal::Busy, "session_reservation.busy"),
            (
                SessionReservationRefusal::Unsupported,
                "session_reservation.unsupported",
            ),
            (
                SessionReservationRefusal::IdentityExhausted,
                "session_reservation.identity_exhausted",
            ),
            (
                SessionReservationRefusal::Unavailable,
                "session_reservation.unavailable",
            ),
            (
                SessionReservationRefusal::InvalidToken,
                "session_reservation.invalid_token",
            ),
            (
                SessionReservationRefusal::Capacity,
                "session_reservation.capacity",
            ),
        ];
        for (refusal, class) in mapped {
            assert_eq!(
                managed_session_core_error_class(&CoreDaemonError::SessionReservation(refusal)),
                class
            );
            assert!(!class.contains('/'));
        }
    }

    #[test]
    fn multiplexer_reserved_spawn_classes_are_kind_based_and_path_neutral() {
        let runtime = botster_core::SessionRuntimeError::new(
            SessionRuntimeErrorKind::SpawnFailed,
            "connect worker control socket failed: /private/raw/path",
        );
        let mapped = [
            (
                MultiplexerEngineError::ReservedSpawn(ReservedSessionSpawnError::Refused(
                    runtime.clone(),
                )),
                "engine.multiplexer.reserved_spawn.refused",
            ),
            (
                MultiplexerEngineError::ReservedSpawn(ReservedSessionSpawnError::Admitted(
                    runtime,
                )),
                "engine.multiplexer.reserved_spawn.admitted",
            ),
            (
                MultiplexerEngineError::InstallationAfterLaunch(Box::new(
                    MultiplexerEngineError::MetadataTooLarge,
                )),
                "engine.multiplexer.installation_after_launch",
            ),
        ];
        for (error, class) in mapped {
            assert_eq!(
                managed_session_core_error_class(&CoreDaemonError::Engine(
                    ManagedSessionRuntimeError::Multiplexer(error)
                )),
                class
            );
            assert!(!class.contains('/'));
        }
    }

    #[test]
    fn explicit_resize_busy_class_is_path_neutral_and_distinct_from_control_plane_failure() {
        let session_id = SessionId("/private/session/resize-busy".to_string());
        assert_eq!(
            managed_session_core_error_class(&CoreDaemonError::ExplicitResizeBusy(
                session_id.clone()
            )),
            "explicit_resize_busy"
        );
        assert_eq!(
            managed_session_core_error_class(&CoreDaemonError::ControlPlaneFailed(session_id)),
            "control_plane_failed"
        );
    }

    #[test]
    fn bind_terminal_adapter_mapping_is_total_over_published_variants() {
        let session_id = SessionId("session".to_string());
        let subscription_id = SubscriptionId("sub".to_string());
        let mapped = [
            (
                BindTerminalAdapterError::BindBeforeAttach {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                },
                "bind_terminal_adapter.bind_before_attach",
            ),
            (
                BindTerminalAdapterError::UnknownSubscription {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                },
                "bind_terminal_adapter.unknown_subscription",
            ),
            (
                BindTerminalAdapterError::StaleGeneration {
                    live: None,
                    requested: TerminalSubscriptionGeneration(1),
                },
                "bind_terminal_adapter.stale_generation",
            ),
            (
                BindTerminalAdapterError::AlreadyBound {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                    generation: TerminalSubscriptionGeneration(1),
                },
                "bind_terminal_adapter.already_bound",
            ),
            (
                BindTerminalAdapterError::ControlPlaneFailed {
                    session_id: session_id.clone(),
                },
                "bind_terminal_adapter.control_plane_failed",
            ),
        ];
        for (error, class) in mapped {
            assert_eq!(
                managed_session_core_error_class(&CoreDaemonError::BindTerminalAdapter(error)),
                class
            );
        }
        let control_plane =
            managed_session_core_error_class(&CoreDaemonError::ControlPlaneFailed(session_id));
        let bind_control_plane = managed_session_core_error_class(
            &CoreDaemonError::BindTerminalAdapter(BindTerminalAdapterError::ControlPlaneFailed {
                session_id: SessionId("session".to_string()),
            }),
        );
        assert_eq!(control_plane, "control_plane_failed");
        assert_eq!(
            bind_control_plane,
            "bind_terminal_adapter.control_plane_failed"
        );
        assert_ne!(control_plane, bind_control_plane);
        assert!(!control_plane.contains('/'));
        assert!(!bind_control_plane.contains('/'));
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
    fn settle_entity_publish_transfers_exact_seq_and_closes_on_error() {
        let scopes = crate::package_event_router::CausalScopeTable::new();
        let mut family = PackageEntityFamilyState {
            causal_token: Some(1),
            ..Default::default()
        };
        let accepted = scopes
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                publication_token: 0,
            }))
            .expect("mint");
        assert_eq!(
            scopes.try_apply_or_wait(settle_entity_publish_op(
                &mut family,
                accepted,
                0,
                32,
                &PackageEntityPublishResult {
                    ok: true,
                    status: PackageEntityPublishStatus::PendingGap,
                    last_accepted_seq: 0,
                    high_water_seq: 32,
                    resync_needed: true,
                    resync_degraded: false,
                },
            )),
            crate::package_event_router::CausalWaitResult::Applied
        );
        assert_eq!(
            scopes.identities(accepted),
            Some(BTreeSet::from([
                LeaseIdentity::AdmittedEntityMutation {
                    family_token: 1,
                    seq: 32,
                },
                LeaseIdentity::ProviderResyncNeed { family_token: 1 },
            ]))
        );

        let errored = scopes
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                publication_token: 0,
            }))
            .expect("mint error scope");
        assert_eq!(
            scopes.try_apply_or_wait(settle_entity_publish_op(
                &mut family,
                errored,
                0,
                1,
                &PackageEntityPublishResult {
                    ok: false,
                    status: PackageEntityPublishStatus::StaleSequence,
                    last_accepted_seq: 5,
                    high_water_seq: 5,
                    resync_needed: false,
                    resync_degraded: false,
                },
            )),
            crate::package_event_router::CausalWaitResult::Applied
        );
        assert!(!scopes.is_live(errored));
        assert_eq!(scopes.lease_count(errored), None);
    }
}

struct PublicationRetirement {
    drain: Option<(String, u64)>,
    disposed: bool,
    daemon_owned: bool,
    response: std::sync::mpsc::Sender<Result<PackageEntityPublishResult, String>>,
    result: Result<PackageEntityPublishResult, String>,
    release: Option<CausalOp>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PublicationWait {
    Ready,
    Capacity,
    Table,
    Fault,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PublicationAdvance {
    Again,
    Complete,
    Fault,
}
