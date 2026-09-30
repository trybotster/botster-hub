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
    PluginWorkerDebugSnapshot, RequestId, ReservedSessionSpawnError, Rgb, RoutedEnvelope,
    RoutedEnvelopeDrainOutcome, RoutedEnvelopePublishOutcome, SessionId, SessionLifecycleState,
    SessionReservation, SessionReservationRefusal, SessionReservationRelease,
    SessionRuntimeErrorKind, SessionSpawnRequest, SubscriptionId, TerminalCapabilitySet,
    TerminalColorProfile, TerminalSubscriptionGeneration,
};
use botster_core_daemon::operation::ReservedSpawnResult;
use botster_core_daemon::{
    AcknowledgeRoutedEnvelopeRequest, CaptureId, CaptureOwner, CaptureSnapshotRequest,
    CoreCompletion, CoreDaemonConfig, CoreDaemonError, CoreOperation, DaemonSession,
    DetachTerminalSubscriptionResult, DrainRoutedEnvelopesRequest, LifecycleBaselineBudget,
    ObserveLifecycleBudget, ObserveLifecycleCursor, ObserveLifecycleSlice, PendingOperationId,
    PublishRoutedEnvelopeRequest, ReadCursorRequest, ReadModeFlagsRequest, ReadScreenRequest,
    RegistrySessionState, RetentionAccounting, RetentionPolicy, RoutedEnvelopeDeliveryStateResult,
    SessionAdoptionReport, SessionAdoptionState, SessionLifecycleBaselinePage,
    SessionLifecycleCursor, SessionLifecyclePage, SessionLifecyclePageError,
    SessionRegistryStateLookup, SnapshotPage, SpawnSessionRequest,
};
use botster_ui_contract::{UiActionRequest, UiActionResult, UiNode};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
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
use crate::persistence::{
    FileCommitError, FileCommitOutcome, FileHubStateStore, HubState, HubStateAuthority,
    HubStateStore, HubStateStoreError,
};
use crate::session_types::{
    EnsuredManagedWorktree, HubSessionContext, ManagedSessionTypeRequest, SessionTypeRequest,
    materialize_managed_session_type, show_session_type_for_target,
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
pub(crate) mod session_reservations;
mod session_spawn;
use provider::{ProviderExpectation, ProviderRequestPlan};
#[cfg(test)]
pub(crate) use session_spawn::await_remove_before_next_poll;
pub(crate) use session_spawn::{SessionSpawnCleanupPoll, SessionTypeSpawnStart};
#[allow(unused_imports)] // The owner continuation will use the conversion outcome.
pub(crate) use session_spawn::{
    SpawnConversionOutcome, SpawnConversionReceipt, SpawnDeliveryOutcome, SpawnDeliveryReceipt,
};
pub(crate) use session_spawn::{SpawnReplySender, spawn_reply_channel};
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
struct CreatedWorktreeCleanup {
    worktree_id: String,
    session_id: SessionId,
    prepared: crate::managed_git_worktrees::PreparedManagedWorktree,
    reservation: SessionReservation,
    shutdown: Option<CoreOperationTracker>,
    remove: Option<CoreOperationTracker>,
    removed: bool,
    release: Option<CoreOperationTracker>,
    release_attempted: bool,
}

pub struct HubRuntime {
    config: HubConfig,
    startup_materialization_paths: crate::session_types::StartupMaterializationPaths,
    lua_memory: Arc<crate::lua_memory::LuaMemoryAccount>,
    /// Per-plugin structured log records (`botster.log`, `ReadPluginLogs`).
    plugin_logs: Arc<crate::plugin_logs::PluginLogBook>,
    #[cfg(test)]
    lua_plugin_runtimes:
        std::sync::Arc<Mutex<Vec<std::sync::Weak<crate::lua_runtime::LuaPluginRuntime>>>>,
    // Readers clone the current Arc under this short lock. Publication swaps
    // one Arc, so the owner never clones a durable state collection.
    state: SharedHubState,
    package_registry: SharedPackageRegistry,
    state_authority: Option<Arc<HubStateAuthority>>,
    core_daemon: SharedCoreDaemon,
    detached_operations: Mutex<Vec<CoreOperationTracker>>,
    inflight_plugin_core:
        Mutex<crate::lua_memory::charged_collection::ChargedVec<InflightPluginCore>>,
    retained_plugin_reservations: Mutex<Vec<SessionReservation>>,
    session_reservations: session_reservations::SessionReservationRecords,
    created_worktree_cleanups: Mutex<Vec<CreatedWorktreeCleanup>>,
    confirmed_worktree_rollbacks: Mutex<Vec<crate::managed_git_worktrees::PreparedManagedWorktree>>,
    submitted_created_worktree_rollbacks: Mutex<BTreeSet<String>>,
    #[cfg(test)]
    rollback_git_hold: Mutex<Option<Arc<crate::host_executor::TestHostGate>>>,
    #[cfg(test)]
    managed_accept_ones: AtomicUsize,
    #[cfg(test)]
    inherited_managed_cleanup_transfers: AtomicUsize,
    #[cfg(test)]
    retry_retained_again_on_pending: AtomicBool,
    #[cfg(test)]
    resubmit_release_on_pending: AtomicBool,
    #[cfg(test)]
    coordination_core_submits: AtomicUsize,
    #[cfg(test)]
    retained_reservation_takes: AtomicUsize,
    close_work: crate::data_plane::CloseWorkSource,
    data_plane: Option<crate::data_plane::DataPlaneDriver>,
    reconciliation: HubSessionReconciliation,
    plugin_lifecycle: Option<HubPluginLifecycle>,
    /// The one clock of this Hub: plugin clocks, log rate limits, and timers.
    clock: crate::hub_clock::HubClock,
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
    /// Cross-thread wakes for owner work; producers raise, the owner waits.
    owner_signal: Arc<crate::daemon::owner_signal::OwnerSignal>,
    causal_scopes: Arc<crate::package_event_router::CausalScopeTable>,
    causal_queue: CausalOwnerQueue,
    direct_family_cleanup: std::cell::RefCell<Option<HostPackageCleanup>>,
    event_plane_owner_ops: std::cell::RefCell<crate::package_event_router::EventPlaneOwnerOps>,
    event_plane_owner_ops_changed: std::cell::Cell<bool>,
    event_plane_cleanup_faults: std::cell::RefCell<Vec<EventPlaneCleanupFault>>,
    acknowledged_spawn_ids: Mutex<BTreeSet<String>>,
    /// Test seam: the next plugin admissions return this refusal instead of
    /// reaching Core.
    #[cfg(test)]
    forced_admission: Arc<Mutex<Option<(Option<String>, ForcedAdmission)>>>,
}

/// What an entity provider admission on the Host worker needs.
pub(crate) struct ProviderAdmission {
    pub(crate) lifecycle: HubPluginLifecycle,
    signal: Arc<crate::daemon::owner_signal::OwnerSignal>,
    #[cfg(test)]
    pub(crate) forced: Arc<Mutex<Option<(Option<String>, ForcedAdmission)>>>,
}

impl ProviderAdmission {
    /// The plugin-engine epoch. Read it before an attempt: Core arms its retry
    /// wake on a refusal, so a release after this read moves the epoch.
    pub(crate) fn engine_seen(&self) -> crate::daemon::owner_signal::Seen {
        self.signal
            .seen(crate::daemon::owner_signal::SignalKey::PluginEngine)
    }

    pub(crate) fn try_admit(
        self,
        class: PluginInvocationClass,
        request: PluginInvocationRequest,
    ) -> PluginAdmissionResult {
        #[cfg(test)]
        if let Some(forced) = forced_for(&self.forced, &request.handler.plugin_key.0) {
            return forced.result(class, request.request_id);
        }
        self.lifecycle.try_admit(class, request)
    }
}

#[cfg(test)]
fn forced_for(
    forced: &Mutex<Option<(Option<String>, ForcedAdmission)>>,
    plugin_key: &str,
) -> Option<ForcedAdmission> {
    match &*forced
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        Some((scope, forced)) if scope.as_deref().is_none_or(|scope| scope == plugin_key) => {
            Some(*forced)
        }
        _ => None,
    }
}

/// A refusal a test forces on plugin admission (see `set_test_forced_admission`).
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForcedAdmission {
    Backpressured,
    LockBusy,
}

#[cfg(test)]
impl ForcedAdmission {
    pub(crate) fn result(
        self,
        class: PluginInvocationClass,
        request_id: RequestId,
    ) -> PluginAdmissionResult {
        match self {
            Self::Backpressured => PluginAdmissionResult::Backpressured {
                request_id,
                class,
                cause: botster_core::PluginBackpressureCause::ClassQueue,
                reason: "test-forced plugin admission backpressure".to_string(),
                backpressure: None,
            },
            Self::LockBusy => PluginAdmissionResult::LockBusy { request_id, class },
        }
    }
}

type SharedCoreDaemon = crate::data_plane::driver::CoreDaemonHandle;
type SharedSessionContexts = Arc<Mutex<BTreeMap<String, Arc<StoredSessionContext>>>>;
/// The outcome of one package entity publish: the caller's result, the
/// mutation the model discarded (or never admitted), the causal release for a
/// discarded pending publication, and the fanout drain the admission started.
struct EntityPublishAdmission {
    result: Result<PackageEntityPublishResult, String>,
    discarded: Option<PackageEntityMutation>,
    release: Option<CausalOp>,
    drain: Option<(String, u64)>,
}

/// A publish the entity model admitted.
struct AdmittedEntityPublish {
    result: PackageEntityPublishResult,
    discarded: Option<PackageEntityMutation>,
    drain: Option<(String, u64)>,
}

/// One event-plane cleanup fault: the plane's last status, if any, and the owner-work error.
type EventPlaneCleanupFault = (
    Option<Result<u64, EventPlaneStatus>>,
    crate::package_event_router::EventOwnerWorkError,
);

pub(crate) struct StoredSessionContext {
    identity: botster_core::SessionReservationIdentity,
    context: HubSessionContext,
    // Both aliases share this owner. Reads need their own response allowance.
    _charge: crate::lua_memory::LuaCallbackCharge,
}

fn stored_context_bytes(context: &HubSessionContext) -> Option<usize> {
    use crate::lua_memory::layout;
    let values = layout::btree_nodes_checked::<String, String>(context.values.len())?;
    let aliases = layout::btree_nodes_checked::<String, Arc<StoredSessionContext>>(2)?;
    let mut bytes = layout::arc_bytes::<StoredSessionContext>()
        .checked_add(values)?
        .checked_add(aliases)?
        .checked_add(context.context_id.len().checked_mul(2)?)?
        .checked_add(context.session_id.0.len().checked_mul(2)?)?;
    for (key, value) in &context.values {
        bytes = bytes.checked_add(key.len())?.checked_add(value.len())?;
    }
    Some(bytes)
}
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

    pub(crate) fn from_retained(state: SharedView<HubState>, revision: u64) -> Self {
        Self(RwLock::new(PublishedHubState { revision, state }))
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

/// The committed package registry that Lua session-type reads and spawn
/// resolution use. The daemon owner publishes each committed registry view
/// here, and a reader clones the current view for each call. The view's
/// storage is charged once, when the owner reserves it; a replaced view is
/// freed when its last reader drops it.
pub struct PackageRegistryPublication(RwLock<SharedView<PackageRegistry>>);

impl PackageRegistryPublication {
    pub(crate) fn empty(budget: &Arc<SharedViewBudget>) -> Result<Self, HubStateStoreError> {
        let empty =
            PackageRegistry::from_snapshot(crate::packages::PackageRegistrySnapshot::empty())
                .expect("the empty package registry snapshot is valid");
        Ok(Self(RwLock::new(crate::daemon::reserve_package_registry(
            budget, empty,
        )?)))
    }

    /// The current committed registry. A poisoned lock keeps its value:
    /// publication replaces one view and cannot leave it partial.
    pub(crate) fn current(&self) -> SharedView<PackageRegistry> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn publish(&self, view: SharedView<PackageRegistry>) {
        let replaced = std::mem::replace(
            &mut *self
                .0
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            view,
        );
        // The replaced view drops outside the lock.
        drop(replaced);
    }
}

/// Shared committed package registry exposed to Lua plugin workers.
pub type SharedPackageRegistry = Arc<PackageRegistryPublication>;

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
    /// The entity model was poisoned by an earlier Host panic; entity work
    /// requires a daemon restart.
    ModelPoisoned,
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
    pending: Mutex<crate::lua_memory::charged_collection::ChargedVecDeque<PendingSessionTypeSpawn>>,
    ordinary_pending: AtomicBool,
    managed: Mutex<VecDeque<PendingManagedSessionSpawn>>,
    managed_pending: AtomicBool,
    managed_owner: Mutex<Option<crate::daemon::control::message::ControlSender>>,
    managed_active: Mutex<BTreeMap<String, crate::owner_identity::WaiterId>>,
    ordinary_owner_thread: Mutex<Option<thread::ThreadId>>,
    abandoned: Mutex<Vec<(String, botster_core::SessionReservationIdentity)>>,
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

pub(crate) enum OrdinarySpawnReply {
    Legacy(mpsc::Sender<Result<PluginSessionTypeSpawned, std::borrow::Cow<'static, str>>>),
    Admitted(SpawnReplySender<AdmittedSpawnDelivery>),
}

pub(crate) enum AdmittedSpawnDelivery {
    Unavailable(&'static str),
    Spawned {
        result: PluginSessionTypeSpawned,
        conversion: SpawnConversionReceipt,
        _variable: crate::lua_memory::LuaCallbackCharge,
    },
    Refused {
        message: String,
        _variable: crate::lua_memory::LuaCallbackCharge,
        _lua_render: crate::lua_memory::LuaCallbackCharge,
    },
}

impl AdmittedSpawnDelivery {
    pub(crate) fn refused(
        message: String,
        variable: crate::lua_memory::LuaCallbackCharge,
        lua_render: crate::lua_memory::LuaCallbackCharge,
    ) -> Self {
        Self::Refused {
            message,
            _variable: variable,
            _lua_render: lua_render,
        }
    }

    pub(crate) fn spawned(
        result: PluginSessionTypeSpawned,
        conversion: SpawnConversionReceipt,
        variable: crate::lua_memory::LuaCallbackCharge,
    ) -> Self {
        Self::Spawned {
            result,
            conversion,
            _variable: variable,
        }
    }
}

pub(crate) struct PendingSessionTypeSpawn {
    #[cfg(test)]
    _dispose_probe: Option<TerminalSpawnerProbe>,
    pub(crate) session_type_id: String,
    pub(crate) request: SessionTypeRequest,
    /// The committed registry the admitting call read; resolution and the
    /// capability check use this same view.
    pub(crate) package_records: SharedView<PackageRegistry>,
    pub(crate) response: OrdinarySpawnReply,
    // The admitted Lua projection remains funded after the caller times out.
    pub(crate) parent: Option<crate::lua_memory::LuaCallbackCharge>,
}

pub(crate) struct PendingManagedSessionSpawn {
    #[cfg(test)]
    _dispose_probe: Option<TerminalSpawnerProbe>,
    pub(crate) plugin_key: PluginKey,
    pub(crate) target_id: String,
    pub(crate) branch: String,
    pub(crate) session_type_id: String,
    pub(crate) request: ManagedSessionTypeRequest,
    /// The committed registry the admitting call read; validation and
    /// materialization use this same view.
    pub(crate) package_records: SharedView<PackageRegistry>,
    pub(crate) accepted_at: Instant,
    response: ManagedSpawnReply,
    // Lua ingress moves its open parent through the queued request.
    pub(crate) parent: Option<crate::lua_memory::LuaCallbackCharge>,
}

pub(crate) struct ManagedSpawnDelivery {
    pub(crate) result: Result<PluginManagedSessionSpawned, ManagedGitError>,
    // The result and its strings drop before this open parent.
    pub(crate) parent: Option<crate::lua_memory::LuaCallbackCharge>,
}

enum ManagedSpawnReply {
    Legacy(mpsc::Sender<Result<PluginManagedSessionSpawned, ManagedGitError>>),
    Admitted(mpsc::Sender<ManagedSpawnDelivery>),
}

impl PendingManagedSessionSpawn {
    pub(crate) fn respond(
        mut self,
        result: Result<PluginManagedSessionSpawned, ManagedGitError>,
    ) -> Result<(), ManagedSpawnDelivery> {
        let delivery = ManagedSpawnDelivery {
            result,
            parent: self.parent.take(),
        };
        match self.response {
            ManagedSpawnReply::Legacy(sender) => {
                debug_assert!(delivery.parent.is_none());
                sender
                    .send(delivery.result)
                    .map_err(|error| ManagedSpawnDelivery {
                        result: error.0,
                        parent: delivery.parent,
                    })
            }
            ManagedSpawnReply::Admitted(sender) => sender.send(delivery).map_err(|error| error.0),
        }
    }

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
            package_records: package_view_for_test(package_records),
            accepted_at: Instant::now(),
            response: ManagedSpawnReply::Legacy(response),
            parent: None,
            _dispose_probe: None,
        }
    }
}

/// One managed session spawn in flight on the Core owner thread.
pub(crate) struct ManagedSessionSpawnStart {
    pub(crate) tracker: CoreOperationTracker,
    pub(crate) context: Option<HubSessionContext>,
    waiter_id: crate::owner_identity::WaiterId,
    stage: PluginSpawnStage,
    pub(crate) reservation: Option<SessionReservation>,
    spawn: SpawnSessionRequest,
    spawn_error: Option<CoreDaemonError>,
    reserve_operation_id: Option<PendingOperationId>,
    context_published: bool,
    /// Record storage charged before Reserve; registered after Reserve.
    record_charge: Option<session_reservations::RecordCharge>,
    /// A removed-during-launch release is in flight at the handoff.
    handoff_release: bool,
}

/// Structured Lua-facing session-type spawn response.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PluginSessionTypeSpawned {
    pub session_id: String,
    pub lifecycle: String,
    pub session_type_id: String,
    pub context_id: String,
    pub context_keys: Vec<String>,
    #[serde(skip)]
    pub(crate) reservation_identity: Option<botster_core::SessionReservationIdentity>,
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
    #[serde(skip)]
    pub(crate) reservation_identity: Option<botster_core::SessionReservationIdentity>,
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
        let startup_materialization_paths =
            crate::session_types::StartupMaterializationPaths::capture(&config);
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
        let owner_signal = Arc::<crate::daemon::owner_signal::OwnerSignal>::default();
        let (close_work, data_plane, core_daemon) =
            start_data_plane(core_config, Arc::clone(&owner_signal));
        let package_event_router = Arc::new(
            crate::package_event_router::PackageEventRouter::with_owner_signal(
                config.package_event_plane,
                Arc::clone(&owner_signal),
            ),
        );
        let inflight_account = Arc::clone(&lua_memory);
        let clock = crate::hub_clock::HubClock::system();
        Ok(Self {
            capability_runtime: Arc::new(Mutex::new(
                HubCapabilityRuntime::from_config_with_clock(&config, clock.clone())
                    .map_err(HubRuntimeError::Capability)?,
            )),
            clock,
            session_type_spawner: Arc::new(HubSessionTypeSpawner::new_with_account(Arc::clone(
                &lua_memory,
            ))),
            host_executor: crate::host_executor::HostExecutor::new(),
            coordination_bridge: HubCoordinationBridge::new(Arc::clone(&lua_memory)),
            entity_publish_bridge: HubEntityPublishBridge::new(
                plugin_lifecycle.entity_provider_registrations(),
            ),
            entity_publish_wait: Cell::new(PublicationWait::Ready),
            package_entities: Arc::new(Mutex::new(PackageEntities::default())),
            entity_model_owner: entity_model::Owner::default(),
            next_provider_token: Cell::new(1),
            package_entity_resync_changed: std::cell::Cell::new(false),
            config,
            startup_materialization_paths,
            plugin_logs: Arc::new(crate::plugin_logs::PluginLogBook::new(Arc::clone(
                &lua_memory,
            ))),
            lua_memory,
            #[cfg(test)]
            lua_plugin_runtimes: std::sync::Arc::new(Mutex::new(Vec::new())),
            package_registry: Arc::new(PackageRegistryPublication::empty(&state.budget())?),
            state,
            state_authority: None,
            core_daemon,
            detached_operations: Mutex::new(Vec::new()),
            inflight_plugin_core: Mutex::new(
                crate::lua_memory::charged_collection::ChargedVec::new(inflight_account),
            ),
            retained_plugin_reservations: Mutex::new(Vec::new()),
            session_reservations: session_reservations::SessionReservationRecords::default(),
            created_worktree_cleanups: Mutex::new(Vec::new()),
            confirmed_worktree_rollbacks: Mutex::new(Vec::new()),
            submitted_created_worktree_rollbacks: Mutex::new(BTreeSet::new()),
            #[cfg(test)]
            rollback_git_hold: Mutex::new(None),
            #[cfg(test)]
            managed_accept_ones: AtomicUsize::new(0),
            #[cfg(test)]
            inherited_managed_cleanup_transfers: AtomicUsize::new(0),
            #[cfg(test)]
            retry_retained_again_on_pending: AtomicBool::new(false),
            #[cfg(test)]
            coordination_core_submits: AtomicUsize::new(0),
            #[cfg(test)]
            retained_reservation_takes: AtomicUsize::new(0),
            #[cfg(test)]
            resubmit_release_on_pending: AtomicBool::new(false),
            close_work,
            data_plane: Some(data_plane),
            reconciliation: HubSessionReconciliation::default(),
            plugin_lifecycle: Some(plugin_lifecycle),
            last_capability_cleanup: None,
            session_contexts: Arc::new(Mutex::new(BTreeMap::new())),
            package_event_router,
            owner_signal,
            causal_scopes: Arc::new(crate::package_event_router::CausalScopeTable::new()),
            causal_queue: CausalOwnerQueue::default(),
            direct_family_cleanup: std::cell::RefCell::new(None),
            event_plane_owner_ops: std::cell::RefCell::new(
                crate::package_event_router::EventPlaneOwnerOps::default(),
            ),
            event_plane_owner_ops_changed: std::cell::Cell::new(false),
            event_plane_cleanup_faults: std::cell::RefCell::new(Vec::new()),
            acknowledged_spawn_ids: Mutex::new(BTreeSet::new()),
            #[cfg(test)]
            forced_admission: Arc::new(Mutex::new(None)),
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
        Self::load_from_store_with_credentials(
            config,
            store,
            CredentialProviderKind::OsKeychain,
            &OsKeychainCredentialStore::new(),
        )
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
        let (state, authority) = store.load_retained(&config)?;
        let (publication, authority) = if let Some(mut authority) = authority {
            let prior = SharedView::from_reserved(
                state,
                authority
                    .take_startup_charge()
                    .expect("File retained load reserves its startup view"),
            );
            let mut candidate = (*prior).clone();
            let (view, revision) = if adopt_unrecorded_managed_worktrees(
                &candidate.spawn_targets,
                &mut candidate.worktrees,
                &managed_worktree_root(&config),
            ) {
                match authority.store().save_retained_startup_state(
                    &authority,
                    0,
                    Some(prior),
                    candidate,
                ) {
                    Ok(FileCommitOutcome::Synced { state, revision }) => (state, revision),
                    Ok(FileCommitOutcome::PublishedUncertain(write)) => {
                        return Err(HubRuntimeError::State(
                            HubStateStoreError::PublishedUncertain(write),
                        ));
                    }
                    Err(FileCommitError::Preparation(error))
                    | Err(FileCommitError::BeforePublication { error, .. }) => {
                        return Err(HubRuntimeError::State(error));
                    }
                    Err(FileCommitError::Stale(_)) => {
                        return Err(HubRuntimeError::State(HubStateStoreError::StaleRevision));
                    }
                    Err(FileCommitError::RevisionExhausted(_)) => {
                        return Err(HubRuntimeError::State(
                            HubStateStoreError::RevisionExhausted,
                        ));
                    }
                }
            } else {
                (prior, 0)
            };
            validate_hub_credentials(&view, provider_kind, credential_store)?;
            (
                HubStatePublication::from_retained(view, revision),
                Some(Arc::new(authority)),
            )
        } else {
            let mut state = state;
            if adopt_unrecorded_managed_worktrees(
                &state.spawn_targets,
                &mut state.worktrees,
                &managed_worktree_root(&config),
            ) {
                store.save_exclusive_startup_state(&state)?;
            }
            validate_hub_credentials(&state, provider_kind, credential_store)?;
            (HubStatePublication::new(state)?, None)
        };
        Self::from_initialized_state(config, publication, authority)
    }

    #[cfg(test)]
    fn from_validated_state(config: HubConfig, state: HubState) -> HubRuntimeResult<Self> {
        Self::from_initialized_state(config, HubStatePublication::new(state)?, None)
    }

    fn from_initialized_state(
        config: HubConfig,
        publication: HubStatePublication,
        state_authority: Option<Arc<HubStateAuthority>>,
    ) -> HubRuntimeResult<Self> {
        let startup_materialization_paths =
            crate::session_types::StartupMaterializationPaths::capture(&config);
        let lua_memory = crate::lua_memory::LuaMemoryAccount::new(
            crate::config::lua_memory_limits(),
        )
        .map_err(|_| {
            HubRuntimeError::Config(crate::config::HubConfigError::InvalidCapacity {
                field: "lua_memory",
            })
        })?;
        let state = Arc::new(publication);
        let core_config = core_daemon_config(&config);
        let plugin_worker_config = config.plugin_worker_config();
        let plugin_lifecycle = HubPluginLifecycle::with_config(plugin_worker_config);
        let owner_signal = Arc::<crate::daemon::owner_signal::OwnerSignal>::default();
        let (close_work, data_plane, core_daemon) =
            start_data_plane(core_config, Arc::clone(&owner_signal));
        let package_event_router = Arc::new(
            crate::package_event_router::PackageEventRouter::with_owner_signal(
                config.package_event_plane,
                Arc::clone(&owner_signal),
            ),
        );
        let inflight_account = Arc::clone(&lua_memory);
        let clock = crate::hub_clock::HubClock::system();
        let mut runtime = Self {
            capability_runtime: Arc::new(Mutex::new(
                HubCapabilityRuntime::from_config_with_clock(&config, clock.clone())
                    .map_err(HubRuntimeError::Capability)?,
            )),
            clock,
            session_type_spawner: Arc::new(HubSessionTypeSpawner::new_with_account(Arc::clone(
                &lua_memory,
            ))),
            host_executor: crate::host_executor::HostExecutor::new(),
            coordination_bridge: HubCoordinationBridge::new(Arc::clone(&lua_memory)),
            entity_publish_bridge: HubEntityPublishBridge::new(
                plugin_lifecycle.entity_provider_registrations(),
            ),
            entity_publish_wait: Cell::new(PublicationWait::Ready),
            package_entities: Arc::new(Mutex::new(PackageEntities::default())),
            entity_model_owner: entity_model::Owner::default(),
            next_provider_token: Cell::new(1),
            package_entity_resync_changed: std::cell::Cell::new(false),
            config,
            startup_materialization_paths,
            plugin_logs: Arc::new(crate::plugin_logs::PluginLogBook::new(Arc::clone(
                &lua_memory,
            ))),
            lua_memory,
            #[cfg(test)]
            lua_plugin_runtimes: std::sync::Arc::new(Mutex::new(Vec::new())),
            package_registry: Arc::new(PackageRegistryPublication::empty(&state.budget())?),
            state,
            state_authority,
            core_daemon,
            detached_operations: Mutex::new(Vec::new()),
            inflight_plugin_core: Mutex::new(
                crate::lua_memory::charged_collection::ChargedVec::new(inflight_account),
            ),
            retained_plugin_reservations: Mutex::new(Vec::new()),
            session_reservations: session_reservations::SessionReservationRecords::default(),
            created_worktree_cleanups: Mutex::new(Vec::new()),
            confirmed_worktree_rollbacks: Mutex::new(Vec::new()),
            submitted_created_worktree_rollbacks: Mutex::new(BTreeSet::new()),
            #[cfg(test)]
            rollback_git_hold: Mutex::new(None),
            #[cfg(test)]
            managed_accept_ones: AtomicUsize::new(0),
            #[cfg(test)]
            inherited_managed_cleanup_transfers: AtomicUsize::new(0),
            #[cfg(test)]
            retry_retained_again_on_pending: AtomicBool::new(false),
            #[cfg(test)]
            coordination_core_submits: AtomicUsize::new(0),
            #[cfg(test)]
            retained_reservation_takes: AtomicUsize::new(0),
            #[cfg(test)]
            resubmit_release_on_pending: AtomicBool::new(false),
            close_work,
            data_plane: Some(data_plane),
            reconciliation: HubSessionReconciliation::default(),
            plugin_lifecycle: Some(plugin_lifecycle),
            last_capability_cleanup: None,
            session_contexts: Arc::new(Mutex::new(BTreeMap::new())),
            package_event_router,
            owner_signal,
            causal_scopes: Arc::new(crate::package_event_router::CausalScopeTable::new()),
            causal_queue: CausalOwnerQueue::default(),
            direct_family_cleanup: std::cell::RefCell::new(None),
            event_plane_owner_ops: std::cell::RefCell::new(
                crate::package_event_router::EventPlaneOwnerOps::default(),
            ),
            event_plane_owner_ops_changed: std::cell::Cell::new(false),
            event_plane_cleanup_faults: std::cell::RefCell::new(Vec::new()),
            acknowledged_spawn_ids: Mutex::new(BTreeSet::new()),
            #[cfg(test)]
            forced_admission: Arc::new(Mutex::new(None)),
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

    pub(crate) fn startup_materialization_paths(
        &self,
    ) -> &crate::session_types::StartupMaterializationPaths {
        &self.startup_materialization_paths
    }

    /// The one clock of this hub. Every plugin VM and the timer runtime read it.
    #[must_use]
    pub fn clock(&self) -> &crate::hub_clock::HubClock {
        &self.clock
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

    pub(crate) fn state_authority(&self) -> Option<Arc<HubStateAuthority>> {
        self.state_authority.as_ref().map(Arc::clone)
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

    /// Return the committed package registry view shared with Lua plugins.
    #[cfg(test)]
    pub(crate) fn package_registry_publication(&self) -> SharedPackageRegistry {
        Arc::clone(&self.package_registry)
    }

    /// Publish the daemon owner's committed package registry view.
    pub(crate) fn publish_package_registry_view(&self, view: SharedView<PackageRegistry>) {
        self.package_registry.publish(view);
    }

    /// Publish a package registry for a host without a daemon owner. The view
    /// is charged to this Hub's state budget.
    pub fn publish_package_registry(
        &self,
        registry: PackageRegistry,
    ) -> Result<(), HubStateStoreError> {
        let view = crate::daemon::reserve_package_registry(&self.state.budget(), registry)?;
        self.package_registry.publish(view);
        Ok(())
    }

    /// Return the shared worktree projection used by Lua helpers.
    #[must_use]
    pub fn worktrees(&self) -> SharedWorktrees {
        Arc::clone(&self.state)
    }

    /// Return Host primitives that retain this Hub's shared Lua memory account.
    #[must_use]
    pub(crate) fn plugin_logs(&self) -> &Arc<crate::plugin_logs::PluginLogBook> {
        &self.plugin_logs
    }

    pub fn lua_plugin_host_api(&self) -> LuaPluginHostApi {
        LuaPluginHostApi {
            memory: Arc::clone(&self.lua_memory),
            logs: Arc::clone(&self.plugin_logs),
            capabilities: self.capability_runtime.clone(),
            clock: self.clock.clone(),
            coordination: self.coordination_bridge(),
            entity_publish: self.entity_publish_bridge(),
            session_types: self.session_type_spawner.clone(),
            spawn_targets: Arc::clone(&self.state),
            worktrees: Arc::clone(&self.state),
            package_registry: Arc::clone(&self.package_registry),
            package_event_router: self.package_event_router.clone(),
            causal_scopes: self.causal_scopes.clone(),
            #[cfg(test)]
            lua_plugin_runtimes: std::sync::Arc::clone(&self.lua_plugin_runtimes),
        }
    }

    #[must_use]
    pub fn package_event_router(&self) -> &Arc<crate::package_event_router::PackageEventRouter> {
        &self.package_event_router
    }

    pub(crate) fn owner_signal(&self) -> &Arc<crate::daemon::owner_signal::OwnerSignal> {
        &self.owner_signal
    }

    /// The per-session edges the data-plane thread stores for the doorbell.
    pub(crate) fn doorbell_edges(&self) -> &crate::data_plane::doorbell_edges::DoorbellEdges {
        self.core_daemon.doorbell_edges()
    }

    /// Block the data-plane thread inside one operation, then fill the Core
    /// request queue until a submission is refused. Dropping the returned
    /// sender releases the data plane.
    #[cfg(test)]
    pub(crate) fn test_fill_core_request_queue(&self) -> std::sync::mpsc::Sender<()> {
        // Waiter ids from the top of the range, never reused by a later fill,
        // stay clear of the owner's own ids.
        static NEXT_FILL_WAITER: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(u64::MAX - 1);
        let fill_waiter = || {
            crate::owner_identity::WaiterId(
                NEXT_FILL_WAITER.fetch_sub(1, std::sync::atomic::Ordering::Relaxed),
            )
        };
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let _blocker = self
            .core_daemon
            .submit_for_owner(fill_waiter(), move |_: &mut _| {
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
            });
        // timer: deadline — the shared test hang guard; the data plane enters at once
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the data plane runs the blocking operation");
        for _ in 0..=crate::data_plane::driver::CORE_REQUEST_CAPACITY {
            let ticket: crate::CoreTicket<()> = self
                .core_daemon
                .submit_for_owner(fill_waiter(), |_: &mut _| {});
            if ticket.refused_wait().is_some() {
                return release_tx;
            }
        }
        panic!("the Core request queue must fill while the data plane is blocked");
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

    #[cfg(test)]
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
        // A poisoned model runs nothing; the reservation returns its slot.
        let _ = self.with_direct_entity_model(|model| {
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
        let Ok(model) = self.package_entities.lock() else {
            crate::hub_log::hub_log!("package_entity_model_poisoned access=next_epoch");
            return Err(PackageEntityCleanupError::ModelPoisoned);
        };
        model.next_epoch()
    }

    fn advance_package_entity_epoch(&self) -> Result<u64, PackageEntityCleanupError> {
        let Ok(mut model) = self.package_entities.lock() else {
            crate::hub_log::hub_log!("package_entity_model_poisoned access=advance_epoch");
            return Err(PackageEntityCleanupError::ModelPoisoned);
        };
        model.advance_epoch()
    }

    #[cfg(test)]
    pub(crate) fn test_exhaust_package_entity_epochs(&self) {
        self.package_entities.lock().unwrap().epoch = u64::MAX;
    }

    /// `None` when the family is absent or the model is poisoned.
    pub(crate) fn package_entity_family_generation(&self, family: &str) -> Option<u64> {
        self.package_entities
            .lock()
            .ok()?
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
    pub(crate) fn test_refuse_registered_owner_begins(&self, count: usize) {
        self.core_daemon.test_refuse_registered_owner_begins(count);
    }

    #[cfg(test)]
    pub(crate) fn test_refuse_next_owner_begins_remaining(&self) -> usize {
        self.core_daemon.test_refuse_next_owner_begins_remaining()
    }

    #[cfg(test)]
    pub(crate) fn test_set_retry_retained_again_on_pending(&self, enabled: bool) {
        self.retry_retained_again_on_pending
            .store(enabled, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn test_retry_retained_again_on_pending(&self) -> bool {
        self.retry_retained_again_on_pending.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn test_set_resubmit_release_on_pending(&self, enabled: bool) {
        self.resubmit_release_on_pending
            .store(enabled, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn test_resubmit_release_on_pending(&self) -> bool {
        self.resubmit_release_on_pending.load(Ordering::Acquire)
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
        let EntityPublishAdmission {
            result,
            discarded,
            release,
            drain,
        } = self.admit_package_entity_publish(registration, mutation, scope_id, 0, reservation);
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
        HostPackageRuntime::new(
            self.plugin_lifecycle().clone(),
            self.lua_plugin_host_api(),
            self.plugin_lifecycle().stranded_handle(),
        )
    }

    /// A Host runtime whose staging is funded by a fresh Host permit's
    /// prepared-byte reservation, for loads outside a Host attempt (daemon
    /// startup and direct calls). The permit's operation slot is released at
    /// once; the retained reservation stays with the funding. Without a permit
    /// the runtime is unfunded and any stage is refused, typed.
    fn funded_host_package_runtime(&self) -> HostPackageRuntime {
        let mut context = self.host_package_runtime();
        if let Some(permit) = self.host_executor.try_reserve() {
            let reserved = permit.reserved_prepared_bytes();
            if reserved > 0 {
                context.fund_staging(package_effect::StagingFunding::new(
                    Arc::new(permit.retain_prepared_reservation(
                        crate::host_executor::ReservationHolder::StagingFunding,
                    )),
                    reserved,
                ));
            }
        }
        context
    }

    /// Strand a package: automatic reloads refuse it, and it receives no
    /// event and no invocation even while its runtime is loaded, until an
    /// operator resolves it. Startup restores it from a durable quarantine;
    /// the owner sets it at once when a compensation fails.
    pub(crate) fn mark_package_stranded(&self, package_name: &str) {
        self.plugin_lifecycle().mark_stranded(package_name);
    }

    /// Packages automatic reloads must refuse until an operator resolves them.
    pub(crate) fn stranded_packages(&self) -> BTreeSet<String> {
        self.plugin_lifecycle().stranded_packages()
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

    #[cfg(test)]
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
        let mut context = self.funded_host_package_runtime();
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
        let mut context = self.funded_host_package_runtime();
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

    /// Install one plugin's runtime grants without loading its package.
    ///
    /// Test-only. In production the Hub installs grants from package
    /// admission when it loads the package.
    #[cfg(any(test, feature = "test-internals"))]
    pub fn test_install_plugin_grants(
        &mut self,
        plugin_key: &PluginKey,
        grants: impl IntoIterator<Item = botster_core::Capability>,
    ) {
        self.capability_runtime
            .lock()
            .expect("hub capability runtime lock")
            .set_plugin_grants(plugin_key, grants);
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
    pub(crate) fn plugin_lifecycle(&self) -> &HubPluginLifecycle {
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
        let request = self.prepare_plugin_mcp_tool(
            call,
            request_id,
            None,
            crate::plugin_caller::PluginCaller::Operator,
        )?;
        Self::complete_plugin_mcp_tool(self.invoke_plugin(request).result)
    }

    /// Prepare one plugin MCP call for non-blocking worker admission.
    pub(crate) fn prepare_plugin_mcp_tool(
        &self,
        call: crate::McpCallRequest,
        request_id: RequestId,
        client_id: Option<ClientId>,
        caller: crate::plugin_caller::PluginCaller,
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
                metadata: caller.to_metadata(),
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

    pub(crate) fn fulfill_pending_session_type_spawns(&self) {
        for (session_id, reservation_identity) in self.session_type_spawner.take_abandoned() {
            self.cleanup_undelivered_session_type_spawn(&PluginSessionTypeSpawned {
                session_id: session_id.clone(),
                lifecycle: String::new(),
                session_type_id: String::new(),
                context_id: format!("ctx-{session_id}"),
                context_keys: Vec::new(),
                reservation_identity: Some(reservation_identity),
            });
        }
        while let Some(pending) = self.session_type_spawner.take_pending_legacy() {
            self.accept_legacy_session_type_spawn(pending);
        }
        self.advance_inflight_plugin_core();
    }

    pub(crate) fn accept_legacy_session_type_spawn(&self, pending: PendingSessionTypeSpawn) {
        let OrdinarySpawnReply::Legacy(ref response) = pending.response else {
            unreachable!("the legacy queue reader selected one legacy response");
        };
        let _ = response.send(Err(std::borrow::Cow::Borrowed(
            "session-type spawn requires the daemon owner",
        )));
    }

    /// Poll every plugin-facing Core operation and deliver finished results.
    fn advance_inflight_plugin_core(&self) {
        let Ok(mut inflight) = self.inflight_plugin_core.lock() else {
            return;
        };
        let mut index = 0;
        while index < inflight.len() {
            match inflight.get_mut(index) {
                Some(InflightPluginCore::Coordination {
                    ticket, rejected, ..
                }) => {
                    let finished = if rejected.is_some() {
                        CoreTicketPoll::Refused
                    } else {
                        ticket.poll()
                    };
                    match finished {
                        CoreTicketPoll::Pending => index += 1,
                        CoreTicketPoll::Ready(result) => {
                            // Bind the record so its unbound fields drop after the send.
                            let record = inflight.swap_remove(index);
                            let InflightPluginCore::Coordination { response, .. } = record;
                            let _ = response.send(result);
                        }
                        CoreTicketPoll::Lost => {
                            // Bind the record so its unbound fields drop after the send.
                            let record = inflight.swap_remove(index);
                            let InflightPluginCore::Coordination { response, .. } = record;
                            let _ =
                                response.send(crate::lua_runtime::CoordinationDelivery::Refused(
                                    crate::lua_runtime::CoordinationRefusal::HelperStopped,
                                ));
                        }
                        CoreTicketPoll::Refused => {
                            let record = inflight.swap_remove(index);
                            let InflightPluginCore::Coordination {
                                response, rejected, ..
                            } = record;
                            let refusal = if rejected.as_ref().is_some_and(|rejected| {
                                rejected.reason == crate::data_plane::driver::CoreRefusal::Stopped
                            }) {
                                crate::lua_runtime::CoordinationRefusal::HelperStopped
                            } else {
                                crate::lua_runtime::CoordinationRefusal::HelperFull
                            };
                            drop(rejected);
                            let _ = response
                                .send(crate::lua_runtime::CoordinationDelivery::Refused(refusal));
                        }
                    }
                }
                None => break,
            }
        }
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
        let records = pending.package_records.packages();
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
        let records = pending.package_records.packages();
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
        self.retry_retained_reservation_releases();
        self.retry_created_worktree_releases();
        let crate::session_types::MaterializedSessionType {
            spawn_request,
            context,
            metadata,
            ..
        } = materialized;
        let metadata = session_type_plugin_metadata(metadata, &pending.plugin_key);
        let spawn = SpawnSessionRequest {
            request: spawn_request,
            metadata,
        };
        let session_id = spawn.request.session_id.clone();
        // The reservation record is charged before Core reserves the id.
        let record_charge = self
            .charge_session_reservation(&session_id.0)
            .map_err(|error| {
                ManagedGitError::new(
                    "session_record_capacity",
                    format!(
                        "the Hub state budget cannot hold the session reservation record: requested {} bytes, {} available",
                        error.requested, error.available
                    ),
                )
            })?;
        let tracker = self.begin_reserve_session_for_owner(owner_waiter, session_id);
        Ok(ManagedSessionSpawnStart {
            tracker,
            context: Some(context),
            waiter_id: owner_waiter,
            stage: PluginSpawnStage::Reserve,
            reservation: None,
            spawn,
            spawn_error: None,
            reserve_operation_id: None,
            context_published: false,
            record_charge: Some(record_charge),
            handoff_release: false,
        })
    }

    /// Finish one managed session spawn from its Core completion.
    pub(crate) fn finish_managed_session_spawn(
        &self,
        start: &ManagedSessionSpawnStart,
        prepared: &PreparedManagedWorktree,
        result: Result<CoreSession, PluginSpawnFailure>,
    ) -> Result<PluginManagedSessionSpawned, ManagedGitError> {
        let outcome = result.map_err(|failure| {
            eprintln!(
                "managed_session_spawn_failed session_id={} core_error={}",
                start.spawn.request.session_id.0,
                managed_session_core_error_class(&failure.error)
            );
            if start.context_published
                && failure.disposition == Some(SessionReservationRelease::Released)
                && let Some(reservation) = start.reservation.as_ref()
            {
                self.retract_spawn_context_aliases(
                    &format!("ctx-{}", start.spawn.request.session_id.0),
                    &start.spawn.request.session_id.0,
                    reservation.identity(),
                );
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
            reservation_identity: start.reservation.as_ref().map(SessionReservation::identity),
        })
    }

    pub(crate) fn cleanup_managed_session(&self, spawned: &PluginManagedSessionSpawned) {
        let session_id = SessionId(spawned.session_id.clone());
        let Some(identity) = spawned.reservation_identity else {
            return;
        };
        if !self.spawn_context_matches(&session_id.0, identity) {
            return;
        }
        let tracked = self
            .created_worktree_cleanups
            .lock()
            .ok()
            .is_some_and(|held| held.iter().any(|cleanup| cleanup.session_id == session_id));
        if !tracked {
            self.shutdown_session_detached(session_id.clone());
        }
        self.retract_spawn_context_aliases(
            &format!("ctx-{}", session_id.0),
            &session_id.0,
            identity,
        );
    }

    /// Whether a created-worktree cleanup owns this session's token.
    pub(crate) fn created_worktree_cleanup_tracks(&self, session_id: &str) -> bool {
        self.created_worktree_cleanups
            .lock()
            .ok()
            .is_some_and(|held| {
                held.iter()
                    .any(|cleanup| cleanup.session_id.0 == session_id)
            })
    }

    pub(crate) fn queue_created_worktree_cleanup(
        &self,
        session_id: SessionId,
        prepared: crate::managed_git_worktrees::PreparedManagedWorktree,
        reservation: SessionReservation,
    ) {
        if !prepared.created_worktree {
            return;
        }
        if let Ok(mut cleanups) = self.created_worktree_cleanups.lock()
            && !cleanups
                .iter()
                .any(|cleanup| cleanup.worktree_id == prepared.worktree_id)
        {
            let shutdown = self.begin_shutdown_session(session_id.clone());
            cleanups.push(CreatedWorktreeCleanup {
                worktree_id: prepared.worktree_id.clone(),
                session_id,
                prepared,
                reservation,
                shutdown: Some(shutdown),
                remove: None,
                removed: false,
                release: None,
                release_attempted: false,
            });
        }
        self.retry_created_worktree_releases();
    }

    pub(crate) fn begin_submitted_worktree_rollback(&self, worktree_id: &str) {
        if let Ok(mut held) = self.submitted_created_worktree_rollbacks.lock() {
            held.insert(worktree_id.to_string());
        }
    }

    pub(crate) fn clear_submitted_worktree_rollback(&self, worktree_id: &str) {
        if let Ok(mut held) = self.submitted_created_worktree_rollbacks.lock() {
            held.remove(worktree_id);
        }
    }

    pub(crate) fn finish_submitted_worktree_rollback(&self, worktree_id: &str) {
        self.clear_submitted_worktree_rollback(worktree_id);
        self.session_type_spawner.publish_managed_spawn();
    }

    pub(crate) fn submitted_worktree_rollback(&self, worktree_id: &str) -> bool {
        self.submitted_created_worktree_rollbacks
            .lock()
            .ok()
            .is_some_and(|held| held.contains(worktree_id))
    }

    #[cfg(test)]
    pub(crate) fn test_install_rollback_git_hold(
        &self,
        gate: Arc<crate::host_executor::TestHostGate>,
    ) {
        if let Ok(mut held) = self.rollback_git_hold.lock() {
            *held = Some(gate);
        }
    }

    #[cfg(test)]
    pub(crate) fn test_rollback_git_hold(&self) -> Option<Arc<crate::host_executor::TestHostGate>> {
        self.rollback_git_hold
            .lock()
            .ok()
            .and_then(|held| held.clone())
    }

    #[cfg(test)]
    pub(crate) fn test_note_managed_accept_one(&self) {
        self.managed_accept_ones.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(crate) fn test_managed_accept_ones(&self) -> usize {
        self.managed_accept_ones.load(Ordering::Acquire)
    }

    pub(crate) fn peek_pending_managed_worktree_id(&self) -> Option<String> {
        self.session_type_spawner.peek_managed_worktree_id()
    }

    pub(crate) fn retry_retained_reservation_releases(&self) {
        let _ = self;
    }

    fn continue_created_worktree_cleanup_phases(&self) {
        let Ok(mut cleanups) = self.created_worktree_cleanups.lock() else {
            return;
        };
        for cleanup in cleanups.iter_mut() {
            if cleanup.shutdown.is_some() || cleanup.remove.is_some() || cleanup.release.is_some() {
                continue;
            }
            if !cleanup.removed {
                cleanup.remove = Some(self.begin_remove_session(&cleanup.session_id));
                continue;
            }
            if cleanup.release_attempted {
                continue;
            }
            cleanup.release =
                Some(self.begin_release_session_reservation(cleanup.reservation.clone()));
            cleanup.release_attempted = true;
        }
    }

    pub(crate) fn retry_created_worktree_releases(&self) {
        let Ok(mut cleanups) = self.created_worktree_cleanups.lock() else {
            return;
        };
        for cleanup in cleanups.iter_mut() {
            if cleanup.shutdown.is_some() || cleanup.remove.is_some() || cleanup.release.is_some() {
                continue;
            }
            if !cleanup.removed {
                cleanup.remove = Some(self.begin_remove_session(&cleanup.session_id));
                continue;
            }
            cleanup.release =
                Some(self.begin_release_session_reservation(cleanup.reservation.clone()));
            cleanup.release_attempted = true;
        }
    }

    fn advance_created_worktree_cleanups(&self) {
        let Ok(mut cleanups) = self.created_worktree_cleanups.lock() else {
            return;
        };
        let mut keep = Vec::new();
        let mut confirmed = Vec::new();
        for mut cleanup in cleanups.drain(..) {
            if let Some(tracker) = cleanup.shutdown.as_mut() {
                match tracker.poll(self) {
                    CoreTicketPoll::Pending => {
                        keep.push(cleanup);
                        continue;
                    }
                    CoreTicketPoll::Ready(_) | CoreTicketPoll::Lost | CoreTicketPoll::Refused => {
                        cleanup.shutdown = None;
                    }
                }
            }
            if let Some(tracker) = cleanup.remove.as_mut() {
                match tracker.poll(self) {
                    CoreTicketPoll::Pending => {
                        keep.push(cleanup);
                        continue;
                    }
                    CoreTicketPoll::Ready(_) | CoreTicketPoll::Lost | CoreTicketPoll::Refused => {
                        cleanup.remove = None;
                        cleanup.removed = true;
                    }
                }
            }
            let Some(tracker) = cleanup.release.as_mut() else {
                keep.push(cleanup);
                continue;
            };
            match tracker.poll(self) {
                CoreTicketPoll::Pending => keep.push(cleanup),
                CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                    result: Ok(SessionReservationRelease::Released),
                    ..
                })) => {
                    self.session_reservations
                        .retire(&cleanup.session_id.0, cleanup.reservation.identity());
                    confirmed.push(cleanup.prepared);
                }
                CoreTicketPoll::Ready(_) | CoreTicketPoll::Lost | CoreTicketPoll::Refused => {
                    cleanup.release = None;
                    keep.push(cleanup);
                }
            }
        }
        *cleanups = keep;
        drop(cleanups);
        self.continue_created_worktree_cleanup_phases();
        if confirmed.is_empty() {
            return;
        }
        if let Ok(mut held) = self.confirmed_worktree_rollbacks.lock() {
            held.extend(confirmed);
        }
        self.session_type_spawner.publish_managed_spawn();
    }

    pub(crate) fn defer_confirmed_worktree_rollback(
        &self,
        prepared: crate::managed_git_worktrees::PreparedManagedWorktree,
    ) {
        if let Ok(mut held) = self.confirmed_worktree_rollbacks.lock() {
            held.push(prepared);
        }
    }

    pub(crate) fn take_one_confirmed_worktree_rollback(
        &self,
    ) -> Option<crate::managed_git_worktrees::PreparedManagedWorktree> {
        self.confirmed_worktree_rollbacks
            .lock()
            .ok()
            .and_then(|mut held| {
                if held.is_empty() {
                    None
                } else {
                    Some(held.remove(0))
                }
            })
    }

    pub(crate) fn created_worktree_cleanup_active(&self, worktree_id: &str) -> bool {
        self.created_worktree_cleanups.lock().is_ok_and(|held| {
            held.iter()
                .any(|cleanup| cleanup.worktree_id == worktree_id)
        })
    }

    pub(crate) fn take_confirmed_worktree_rollback(
        &self,
        worktree_id: &str,
    ) -> Option<crate::managed_git_worktrees::PreparedManagedWorktree> {
        let mut held = self.confirmed_worktree_rollbacks.lock().ok()?;
        let index = held
            .iter()
            .position(|prepared| prepared.worktree_id == worktree_id)?;
        Some(held.remove(index))
    }

    #[cfg(test)]
    pub(crate) fn note_inherited_managed_cleanup_transfer(&self) {
        self.inherited_managed_cleanup_transfers
            .fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(crate) fn test_inherited_managed_cleanup_transfers(&self) -> usize {
        self.inherited_managed_cleanup_transfers
            .load(Ordering::Acquire)
    }

    pub(crate) fn confirmed_worktree_rollback_exists(&self, worktree_id: &str) -> bool {
        self.confirmed_worktree_rollbacks.lock().is_ok_and(|held| {
            held.iter()
                .any(|prepared| prepared.worktree_id == worktree_id)
        })
    }

    pub(crate) fn wake_remaining_confirmed_worktree_rollbacks(&self) {
        let has_confirmed = self
            .confirmed_worktree_rollbacks
            .lock()
            .ok()
            .is_some_and(|held| !held.is_empty());
        if has_confirmed {
            self.session_type_spawner.publish_managed_spawn();
        }
    }

    #[cfg(test)]
    pub(crate) fn test_queue_removed_created_worktree_cleanup(
        &self,
        session_id: SessionId,
        prepared: crate::managed_git_worktrees::PreparedManagedWorktree,
        reservation: SessionReservation,
    ) {
        let worktree_id = prepared.worktree_id.clone();
        if let Ok(mut cleanups) = self.created_worktree_cleanups.lock() {
            cleanups.push(CreatedWorktreeCleanup {
                worktree_id,
                session_id,
                prepared,
                reservation,
                shutdown: None,
                remove: None,
                removed: true,
                release: None,
                release_attempted: false,
            });
        }
    }

    #[cfg(test)]
    pub(crate) fn test_created_worktree_cleanup_release_idle(&self) -> bool {
        self.created_worktree_cleanups
            .lock()
            .ok()
            .is_some_and(|held| {
                held.iter().all(|cleanup| {
                    cleanup.shutdown.is_none()
                        && cleanup.remove.is_none()
                        && cleanup.release.is_none()
                })
            })
    }

    #[cfg(test)]
    pub(crate) fn test_created_worktree_cleanup_count(&self) -> usize {
        self.created_worktree_cleanups
            .lock()
            .map(|held| held.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn test_confirmed_worktree_rollback_count(&self) -> usize {
        self.confirmed_worktree_rollbacks
            .lock()
            .map(|held| held.len())
            .unwrap_or(0)
    }

    fn fulfill_pending_plugin_requests(&self) {
        self.advance_created_worktree_cleanups();
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

    #[cfg(test)]
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
        let (pending, (reservation, acquired)) = selected?;
        let EntityPublishAdmission {
            result,
            discarded,
            release,
            drain,
        } = if acquired {
            self.admit_package_entity_publish(
                pending.registration,
                pending.mutation,
                pending.scope_id,
                pending.token,
                reservation,
            )
        } else {
            EntityPublishAdmission {
                result: Err("causal scope no longer exists".into()),
                discarded: Some(pending.mutation),
                release: None,
                drain: None,
            }
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
    ) -> EntityPublishAdmission {
        let pending_identity = LeaseIdentity::PendingEntityPublish { publication_token };
        let mut reservation = Some(reservation);
        let (result, discarded, drain) = match self.admit_package_entity_publish_inner(
            &registration,
            mutation,
            scope_id,
            publication_token,
            &mut reservation,
        ) {
            Ok(AdmittedEntityPublish {
                result,
                discarded,
                drain,
            }) => (Ok(result), discarded, drain),
            Err((error, mutation)) => (Err(error), Some(mutation), None),
        };
        let release = scope_id
            .filter(|_| discarded.is_some())
            .map(|scope_id| CausalOp::Release {
                scope_id,
                identity: pending_identity,
            });
        EntityPublishAdmission {
            result,
            discarded,
            release,
            drain,
        }
    }

    fn admit_package_entity_publish_inner(
        &self,
        registration: &crate::lifecycle::EntityProviderRegistration,
        mutation: PackageEntityMutation,
        scope_id: Option<u64>,
        publication_token: u64,
        reservation: &mut Option<CausalReservation>,
    ) -> Result<AdmittedEntityPublish, (String, PackageEntityMutation)> {
        let mut mutation = Some(mutation);
        let transition = match self.with_direct_entity_model(|model| {
            model.admit(
                registration,
                mutation.take().expect("the publish retains its mutation"),
                scope_id,
                publication_token,
            )
        }) {
            Ok(transition) => transition?,
            Err(entity_model::EntityModelPoisoned) => {
                return Err((
                    entity_model::ENTITY_MODEL_POISONED_MESSAGE.to_string(),
                    mutation
                        .take()
                        .expect("a poisoned model never takes the mutation"),
                ));
            }
        };
        if let Some(op) = transition.causal {
            reservation
                .take()
                .expect("publication reserved its transition")
                .commit(op);
        }
        self.note_package_entity_resync_changed();
        Ok(AdmittedEntityPublish {
            result: transition.result,
            discarded: transition.discarded,
            drain: transition.drain,
        })
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
                let Some(op) = self.prepare_finish_op(lease, finish.scheduled_resync) else {
                    // The model is poisoned: hand back what was taken, stop.
                    mutations.push(mutation);
                    break;
                };
                reservation.commit(op);
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
            .ok()
            .flatten()
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
        let Some(op) = self.prepare_finish_op(lease, finish.scheduled_resync) else {
            return CausalTransitionStatus::Fault;
        };
        reservation.commit(op);
        CausalTransitionStatus::Applied
    }

    /// `None` when the model is poisoned.
    fn prepare_finish_op(
        &self,
        lease: &EntityMutationLease,
        scheduled_resync: bool,
    ) -> Option<CausalOp> {
        let (op, changed) = self
            .with_direct_entity_model(|model| model.finish(lease, scheduled_resync))
            .ok()?;
        if changed {
            self.note_package_entity_resync_changed();
        }
        Some(op)
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
    ) -> Result<PackageEntityFamilyProgress, entity_model::EntityModelPoisoned> {
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
        let Ok((generation, step)) =
            self.with_direct_entity_model(|model| model.step_snapshot(entity_type))
        else {
            return PackageEntitySnapshotStep::Fault;
        };
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
        let _ = self.with_direct_entity_model(|model| model.mark_resync(entity_type));
    }

    /// Explicitly re-arm resync after a new catching-up subscription (or other
    /// progress event that must clear degradation).
    pub fn rearm_package_entity_resync(&self, entity_type: &str) {
        self.note_package_entity_resync_changed();
        let _ = self.with_direct_entity_model(|model| model.rearm_resync(entity_type));
    }

    /// Retain a resync change until the owner records the scheduling work.
    pub(crate) fn note_package_entity_resync_changed(&self) {
        self.package_entity_resync_changed.set(true);
    }

    pub(crate) fn take_package_entity_resync_notification(&self) -> bool {
        self.package_entity_resync_changed.replace(false)
    }

    #[cfg(test)]
    pub(crate) fn package_entity_resync_next_attempt(&self, entity_type: &str) -> Option<Instant> {
        let model = self
            .package_entities
            .lock()
            .expect("package entity model lock");
        model.resync_observation(entity_type).deadline
    }

    /// Record a resync attempt; returns whether the family entered degraded.
    pub fn record_package_entity_resync_attempt(&self, entity_type: &str) -> bool {
        let degraded = self
            .with_direct_entity_model(|model| model.record_resync_attempt(entity_type))
            .unwrap_or(false);
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
        let Ok(Some(op)) =
            self.with_direct_entity_model(|model| model.take_resync_release(entity_type, true))
        else {
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

    /// Test-only: the owner phases still registered for a waiter, which the
    /// owner must collect; phases an operation collected itself are gone.
    #[cfg(test)]
    pub(crate) fn test_registered_owner_identities(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
    ) -> Vec<crate::owner_identity::OwnerWorkIdentity> {
        self.core_daemon.test_registered_owner_identities(waiter_id)
    }

    #[cfg(test)]
    pub(crate) fn test_core_waiter_probe(
        &self,
    ) -> impl Fn(crate::owner_identity::WaiterId) -> bool + Send + 'static {
        let core = self.core_daemon.clone();
        move |waiter_id| core.test_retains_waiter(waiter_id)
    }

    #[cfg(test)]
    pub(crate) fn test_lua_callback_usage_probe(&self) -> impl Fn() -> usize + Send + 'static {
        let memory = Arc::clone(&self.lua_memory);
        move || memory.usage().1
    }

    #[cfg(test)]
    pub(crate) fn test_lua_memory(&self) -> Arc<crate::lua_memory::LuaMemoryAccount> {
        Arc::clone(&self.lua_memory)
    }

    #[cfg(test)]
    pub(crate) fn test_fulfill_pending_coordination_requests(&self) {
        self.fulfill_pending_coordination_requests();
    }

    #[cfg(test)]
    pub(crate) fn test_coordination_core_submits(&self) -> usize {
        self.coordination_core_submits
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn test_inflight_capacity(&self) -> usize {
        self.inflight_plugin_core.lock().unwrap().capacity()
    }

    #[cfg(test)]
    pub(crate) fn test_inflight_charge_bytes(&self) -> usize {
        self.inflight_plugin_core.lock().unwrap().charge_bytes()
    }

    #[cfg(test)]
    pub(crate) fn test_callback_charge_owners(&self) -> Vec<(String, usize)> {
        let mut owners = Vec::new();
        let bridge = self.coordination_bridge();
        owners.push((
            format!(
                "pending charge_bytes capacity={}",
                bridge.test_pending_capacity()
            ),
            bridge.test_pending_charge_bytes(),
        ));
        owners.push((
            format!(
                "inflight charge_bytes capacity={}",
                self.test_inflight_capacity()
            ),
            self.test_inflight_charge_bytes(),
        ));
        let runtimes = self
            .lua_plugin_runtimes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for runtime in runtimes.iter().filter_map(std::sync::Weak::upgrade) {
            let key = runtime.test_plugin_key();
            owners.push((
                format!("instruction_error:{key}"),
                runtime.test_instruction_error_bytes(),
            ));
            owners.push((
                format!("capacity_string:{key}"),
                runtime.test_capacity_string_bytes(),
            ));
        }
        owners
    }

    #[cfg(test)]
    pub(crate) fn test_callback_charge_breakdown_probe(
        &self,
    ) -> impl Fn() -> Vec<(String, usize)> + Send + 'static {
        let pending = self.coordination_bridge();
        let runtimes = std::sync::Arc::clone(&self.lua_plugin_runtimes);
        move || {
            let mut owners = Vec::new();
            owners.push((
                format!(
                    "pending charge_bytes capacity={}",
                    pending.test_pending_capacity()
                ),
                pending.test_pending_charge_bytes(),
            ));
            let runtimes = runtimes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for runtime in runtimes.iter().filter_map(std::sync::Weak::upgrade) {
                let key = runtime.test_plugin_key();
                owners.push((
                    format!("instruction_error:{key}"),
                    runtime.test_instruction_error_bytes(),
                ));
                owners.push((
                    format!("capacity_string:{key}"),
                    runtime.test_capacity_string_bytes(),
                ));
            }
            owners
        }
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
            let slot = match crate::lua_memory::charged_collection::SlotReservation::try_reserve(
                &self.inflight_plugin_core,
            ) {
                Ok(slot) => slot,
                Err(_) => {
                    let _ =
                        pending
                            .response
                            .send(crate::lua_runtime::CoordinationDelivery::Refused(
                                crate::lua_runtime::CoordinationRefusal::CallbackCapacity,
                            ));
                    continue;
                }
            };
            let (core_storage, storage) = match pending.storage {
                Some(storage) => (
                    Some(storage.core),
                    Some((storage.continuation, storage.disposal)),
                ),
                None => (None, None),
            };
            let caller = pending.caller;
            #[cfg(test)]
            self.coordination_core_submits
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let submission = self.core_daemon.submit_retained(
                move |daemon| pending.operation.execute(daemon),
                move || caller.claim(),
                core_storage,
            );
            slot.insert(InflightPluginCore::Coordination {
                ticket: submission.ticket,
                response: pending.response,
                rejected: submission.rejected,
                _storage: storage,
                _entry: pending.entry,
            });
        }
        self.advance_inflight_plugin_core();
    }

    fn cleanup_undelivered_session_type_spawn(&self, spawned: &PluginSessionTypeSpawned) {
        let session_id = SessionId(spawned.session_id.clone());
        let Some(identity) = spawned.reservation_identity else {
            return;
        };
        if !self.spawn_context_matches(&session_id.0, identity) {
            return;
        }
        self.shutdown_session_detached(session_id.clone());
        self.retract_spawn_context_aliases(&spawned.context_id, &session_id.0, identity);
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
        if let Some((scope_id, invocation_token)) = invocation.causal_lease
            && !self.causal_scopes.acquire_with_admission(
                scope_id,
                LeaseIdentity::ProviderInFlight { invocation_token },
                invocation.admission.clone(),
            )
        {
            return Err(crate::McpToolError::new(
                "causal_scope_busy",
                "could not acquire provider causal lease",
            ));
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
        if let PluginInvocationResult::Failed(failure) = &result
            && failure.kind == PluginInvocationFailureKind::CompletionTooLarge
        {
            return Err(crate::McpToolError::new(
                "entity_provider_frame_too_large",
                &failure.reason,
            ));
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

    /// Look up the handler for one delivery, only under the consumer plugin
    /// generation its subscription was admitted with.
    pub fn package_event_handler(
        &self,
        delivery: &crate::package_event_router::ReadyDelivery,
    ) -> Result<crate::lifecycle::HubPluginEventHandler, crate::lifecycle::EventDeliveryRefusal>
    {
        self.plugin_lifecycle().event_handler_for(
            &delivery.holder.plugin_key,
            delivery.holder.plugin_generation,
            &delivery.owner,
            &delivery.name,
            &delivery.holder.handler_id,
        )
    }

    /// Admit one package-event delivery while the generation it matched is
    /// still installed.
    pub fn try_admit_package_event(
        &self,
        delivery: &crate::package_event_router::ReadyDelivery,
        class: PluginInvocationClass,
        request: PluginInvocationRequest,
    ) -> Result<PluginAdmissionResult, crate::lifecycle::EventDeliveryRefusal> {
        #[cfg(test)]
        if let Some(forced) = forced_for(&self.forced_admission, &delivery.holder.plugin_key) {
            return Ok(forced.result(class, request.request_id));
        }
        self.plugin_lifecycle().try_admit_event(
            delivery.holder.plugin_generation,
            &delivery.owner,
            &delivery.name,
            class,
            request,
        )
    }

    /// Admit one plugin invocation without waiting.
    #[must_use]
    pub fn try_admit_plugin(
        &self,
        class: PluginInvocationClass,
        request: PluginInvocationRequest,
    ) -> PluginAdmissionResult {
        #[cfg(test)]
        if let Some(forced) = forced_for(&self.forced_admission, &request.handler.plugin_key.0) {
            return forced.result(class, request.request_id);
        }
        self.plugin_lifecycle().try_admit(class, request)
    }

    pub(crate) fn plugin_provider_admission(&self) -> ProviderAdmission {
        ProviderAdmission {
            lifecycle: self.plugin_lifecycle().clone(),
            signal: Arc::clone(&self.owner_signal),
            #[cfg(test)]
            forced: Arc::clone(&self.forced_admission),
        }
    }

    /// Force the next plugin admissions to refuse, or stop forcing.
    #[cfg(test)]
    pub(crate) fn set_test_forced_admission(&self, forced: Option<ForcedAdmission>) {
        *self
            .forced_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = forced.map(|forced| (None, forced));
    }

    /// Force admissions for one plugin only to refuse.
    #[cfg(test)]
    pub(crate) fn set_test_forced_admission_for(&self, plugin_key: &str, forced: ForcedAdmission) {
        *self
            .forced_admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some((Some(plugin_key.to_string()), forced));
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
    /// Every engine notification also raises `PluginEngine`, the key a
    /// refused plugin admission parks on.
    pub fn install_plugin_completion_notifier(
        &self,
        notifier: botster_core::PluginCompletionNotifier,
    ) {
        let signal = Arc::clone(&self.owner_signal);
        self.plugin_lifecycle()
            .install_completion_notifier(Arc::new(move || {
                signal.raise(crate::daemon::owner_signal::SignalKey::PluginEngine);
                notifier();
            }));
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

    pub(crate) fn begin_release_session_reservation(
        &self,
        reservation: SessionReservation,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(
            self.core_daemon
                .begin(CoreOperation::ReleaseSessionReservation(reservation)),
        )
    }

    pub(crate) fn take_retained_reservations(&self) -> Vec<SessionReservation> {
        #[cfg(test)]
        self.retained_reservation_takes
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
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

    /// Owner-side records of explicit session reservations after Reserve.
    pub(crate) fn session_reservations(&self) -> &session_reservations::SessionReservationRecords {
        &self.session_reservations
    }

    /// Charge one reservation record to the Hub state budget before Reserve.
    pub(crate) fn charge_session_reservation(
        &self,
        session_id: &str,
    ) -> Result<session_reservations::RecordCharge, crate::shared_view::SharedViewCapacityError>
    {
        session_reservations::SessionReservationRecords::charge(
            &self.state_publication().budget(),
            session_id,
        )
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

    pub(crate) fn retained_reservations(&self) -> Vec<SessionReservation> {
        self.retained_plugin_reservations
            .lock()
            .map(|held| held.clone())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn test_session_context(
        &self,
        key: &str,
    ) -> Option<crate::session_types::HubSessionContext> {
        self.session_contexts
            .lock()
            .ok()?
            .get(key)
            .map(|entry| entry.context.clone())
    }

    #[cfg(test)]
    pub(crate) fn test_publish_spawn_context(
        &self,
        context: &HubSessionContext,
    ) -> botster_core::SessionReservationIdentity {
        let reservation = botster_core::SessionAdmission::default()
            .reserve(context.session_id.clone())
            .expect("the isolated test admission accepts the session");
        let bytes = stored_context_bytes(context).unwrap();
        let charge = self.lua_memory.reserve_callback_total(bytes).unwrap();
        self.publish_spawn_context(context.clone(), &reservation, charge)
            .unwrap();
        reservation.identity()
    }

    pub(crate) fn publish_spawn_context(
        &self,
        context: HubSessionContext,
        reservation: &SessionReservation,
        charge: crate::lua_memory::LuaCallbackCharge,
    ) -> Result<(), String> {
        if charge.bytes()
            < stored_context_bytes(&context)
                .ok_or_else(|| "session context size overflow".to_string())?
        {
            return Err("session context has no allocation allowance".to_string());
        }
        let mut contexts = self
            .session_contexts
            .lock()
            .map_err(|_| "session context lock poisoned".to_string())?;
        let entry = Arc::new(StoredSessionContext {
            identity: reservation.identity(),
            context,
            _charge: charge,
        });
        let context_id = entry.context.context_id.clone();
        let session_id = entry.context.session_id.0.clone();
        contexts.insert(context_id, Arc::clone(&entry));
        contexts.insert(session_id, entry);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn retract_spawn_context(
        &self,
        context: &HubSessionContext,
        identity: botster_core::SessionReservationIdentity,
    ) {
        self.retract_spawn_context_aliases(&context.context_id, &context.session_id.0, identity);
    }

    fn retract_spawn_context_aliases(
        &self,
        context_id: &str,
        session_id: &str,
        identity: botster_core::SessionReservationIdentity,
    ) {
        let Ok(mut contexts) = self.session_contexts.lock() else {
            return;
        };
        let old_context = if contexts
            .get(context_id)
            .is_some_and(|stored| stored.identity == identity)
        {
            contexts.remove(context_id)
        } else {
            None
        };
        let old_session = if contexts
            .get(session_id)
            .is_some_and(|stored| stored.identity == identity)
        {
            contexts.remove(session_id)
        } else {
            None
        };
        if contexts.is_empty() {
            // Rust 1.97 can retain an empty BTreeMap leaf root after removal.
            // Destroy that root before either removed entry releases its charge.
            drop(std::mem::take(&mut *contexts));
        }
        drop(old_context);
        drop(old_session);
    }

    fn spawn_context_matches(
        &self,
        session_id: &str,
        identity: botster_core::SessionReservationIdentity,
    ) -> bool {
        self.session_contexts.lock().is_ok_and(|contexts| {
            contexts
                .get(session_id)
                .is_some_and(|entry| entry.identity == identity)
        })
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

    /// Read hub-owned context by context id or session id.
    #[must_use]
    pub fn session_context(&self, id: &str) -> Option<HubSessionContext> {
        self.session_contexts
            .lock()
            .expect("session contexts mutex")
            .get(id)
            .map(|entry| entry.context.clone())
    }

    /// Attach one route and bind its terminal adapter in one Core owner turn.
    ///
    /// The sequence is: detach the same client's previous generation when one
    /// exists, declare the adapter, attach, look up the new generation, bind
    /// the adapter. Any failure after attach detaches again so Core holds no
    /// route without an adapter. Nothing here waits on the owner thread.
    #[cfg(test)]
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

    pub(crate) fn take_ordinary_spawn_notification(&self) -> bool {
        self.session_type_spawner
            .ordinary_pending
            .swap(false, Ordering::AcqRel)
    }

    pub(crate) fn take_pending_spawn_for_owner(&self) -> Option<PendingSessionTypeSpawn> {
        self.session_type_spawner.take_pending_for_owner()
    }

    pub(crate) fn host_executor(&self) -> &crate::host_executor::HostExecutor {
        &self.host_executor
    }

    #[cfg(test)]
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

    /// Start a cursor read for the doorbell. Core answers with
    /// `CoreCompletion::ReadCursor`.
    pub(crate) fn begin_read_cursor_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    ) -> CoreOperationTracker {
        CoreOperationTracker::new(self.core_daemon.begin_for_owner(
            waiter_id,
            CoreOperation::ReadCursor(ReadCursorRequest {
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
    pub(crate) fn managed_attempt_active(&self, worktree_id: &str) -> bool {
        self.managed_active
            .lock()
            .is_ok_and(|held| held.contains_key(worktree_id))
    }

    pub(crate) fn begin_managed_attempt(
        &self,
        worktree_id: String,
        waiter_id: crate::owner_identity::WaiterId,
    ) {
        let mut held = self.managed_active.lock().expect("managed attempt lock");
        assert!(held.insert(worktree_id, waiter_id).is_none());
    }

    pub(crate) fn finish_managed_attempt(
        &self,
        worktree_id: &str,
        waiter_id: crate::owner_identity::WaiterId,
    ) {
        let mut held = self.managed_active.lock().expect("managed attempt lock");
        if held.get(worktree_id) == Some(&waiter_id) {
            held.remove(worktree_id);
        }
        drop(held);
        self.publish_managed_spawn();
    }

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
            (pending.take(), std::mem::take(&mut *managed))
        };
        // Payload destructors run after every queue lock is released.
        drop(queues);
        self.managed_pending.store(false, Ordering::Release);
        self.ordinary_pending.store(false, Ordering::Release);
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
            .try_push_back(PendingSessionTypeSpawn {
                session_type_id: "plain".into(),
                request: SessionTypeRequest {
                    environment: BTreeMap::from([("PAYLOAD".into(), "spawn payload".repeat(128))]),
                    ..SessionTypeRequest::default()
                },
                package_records: package_view_for_test(Vec::new()),
                response: OrdinarySpawnReply::Legacy(response),
                parent: None,
                _dispose_probe: Some(probe(gate)),
            })
            .expect("the terminal test queue must fit");
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
                package_records: package_view_for_test(Vec::new()),
                accepted_at: Instant::now(),
                response: ManagedSpawnReply::Legacy(response),
                parent: None,
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

    #[cfg(test)]
    pub(crate) fn new() -> Self {
        let account = crate::lua_memory::LuaMemoryAccount::new(crate::config::lua_memory_limits())
            .expect("the test Lua memory limits are valid");
        Self::new_with_account(account)
    }

    pub(crate) fn new_with_account(account: Arc<crate::lua_memory::LuaMemoryAccount>) -> Self {
        Self {
            pending: Mutex::new(crate::lua_memory::charged_collection::ChargedVecDeque::new(
                account,
            )),
            ordinary_pending: AtomicBool::new(false),
            managed: Mutex::new(VecDeque::new()),
            managed_pending: AtomicBool::new(false),
            managed_owner: Mutex::new(None),
            managed_active: Mutex::new(BTreeMap::new()),
            ordinary_owner_thread: Mutex::new(None),
            abandoned: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn abandon_session_type_spawn(
        &self,
        session_id: String,
        identity: botster_core::SessionReservationIdentity,
    ) {
        self.abandoned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((session_id, identity));
    }

    #[cfg(test)]
    pub(crate) fn test_take_abandoned(
        &self,
    ) -> Vec<(String, botster_core::SessionReservationIdentity)> {
        self.take_abandoned()
    }

    #[cfg(test)]
    pub(crate) fn test_managed_queue_len(&self) -> usize {
        self.managed.lock().map(|queue| queue.len()).unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn test_publish_managed_spawn(&self) {
        self.publish_managed_spawn();
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
        drop(self.test_enqueue_managed_with_reply(
            plugin_key,
            target_id,
            branch,
            session_type_id,
            request,
            package_records,
        ));
    }

    #[cfg(test)]
    pub(crate) fn test_enqueue_managed_with_reply(
        &self,
        plugin_key: PluginKey,
        target_id: String,
        branch: String,
        session_type_id: String,
        request: ManagedSessionTypeRequest,
        package_records: Vec<PackageRecord>,
    ) -> mpsc::Receiver<Result<PluginManagedSessionSpawned, ManagedGitError>> {
        let (response, receiver) = mpsc::channel();
        self.managed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(PendingManagedSessionSpawn {
                plugin_key,
                target_id,
                branch,
                session_type_id,
                request,
                package_records: package_view_for_test(package_records),
                accepted_at: Instant::now(),
                response: ManagedSpawnReply::Legacy(response),
                parent: None,
                _dispose_probe: None,
            });
        self.publish_managed_spawn();
        receiver
    }

    fn take_abandoned(&self) -> Vec<(String, botster_core::SessionReservationIdentity)> {
        std::mem::take(
            &mut *self
                .abandoned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// The Hub owner's control channel, or `None` before the owner runs.
    pub(crate) fn owner_sender(&self) -> Option<crate::daemon::control::message::ControlSender> {
        self.managed_owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    fn bind_managed_owner_wake(&self, sender: crate::daemon::control::message::ControlSender) {
        *self
            .ordinary_owner_thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(thread::current().id());
        let mut owner = self
            .managed_owner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *owner = Some(sender.clone());
        let pending = self.managed_pending.load(Ordering::Acquire)
            || self.ordinary_pending.load(Ordering::Acquire);
        drop(owner);
        if pending {
            let _ = sender.try_send(
                crate::daemon::control::message::ControlMessage::ManagedSessionSpawnQueued,
            );
        }
    }

    fn publish_managed_spawn(&self) {
        self.managed_pending.store(true, Ordering::Release);
        self.ring_spawn_doorbell();
    }

    fn publish_session_type_spawn(&self) {
        self.ordinary_pending.store(true, Ordering::Release);
        self.ring_spawn_doorbell();
    }

    fn ring_spawn_doorbell(&self) {
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
        _plugin_key: &PluginKey,
        _session_type_id: &str,
        _request: SessionTypeRequest,
        _package_records: Vec<PackageRecord>,
    ) -> Result<PluginSessionTypeSpawned, std::borrow::Cow<'static, str>> {
        Err(std::borrow::Cow::Borrowed(
            "session-type spawn requires the daemon owner",
        ))
    }

    /// Admit one Lua request through the existing ordinary queue and wait on its worker.
    pub(crate) fn spawn_admitted(
        &self,
        input: crate::lua_runtime::spawn_input::SpawnInput,
        package_records: SharedView<PackageRegistry>,
    ) -> Result<AdmittedSpawnDelivery, std::borrow::Cow<'static, str>> {
        if self
            .ordinary_owner_thread
            .lock()
            .map_err(|_| std::borrow::Cow::Borrowed("session-type owner thread lock poisoned"))?
            .is_some_and(|owner| owner == thread::current().id())
        {
            return Err(std::borrow::Cow::Borrowed(
                "session-type spawn cannot wait on the Hub owner thread",
            ));
        }
        if self
            .managed_owner
            .lock()
            .map_err(|_| std::borrow::Cow::Borrowed("session-type owner wake lock poisoned"))?
            .as_ref()
            .is_none_or(crate::daemon::control::message::ControlSender::is_closed)
        {
            return Err(std::borrow::Cow::Borrowed(
                "session-type spawn requires the daemon owner",
            ));
        }
        if !package_allows_session_type_spawn(&package_records, input.plugin_key()) {
            return Err(std::borrow::Cow::Borrowed(
                "plugin package lacks session_type_spawn capability",
            ));
        }
        let (mut parent, _, session_type_id, request) = input.into_parts();
        let bytes = crate::lua_memory::layout::single_reply_bytes::<AdmittedSpawnDelivery>(true)
            .ok_or(std::borrow::Cow::Borrowed(
                crate::lua_memory::LUA_CALLBACK_CAPACITY_EXHAUSTED,
            ))?;
        parent.grow(bytes).map_err(|_| {
            std::borrow::Cow::Borrowed(crate::lua_memory::LUA_CALLBACK_CAPACITY_EXHAUSTED)
        })?;
        let channel_charge = parent
            .split_fixed(bytes)
            .expect("the admitted parent owns the reply channel bytes");
        let (response, receiver) = spawn_reply_channel(channel_charge).map_err(|_| {
            std::borrow::Cow::Borrowed(crate::lua_memory::LUA_CALLBACK_CAPACITY_EXHAUSTED)
        })?;
        let item = PendingSessionTypeSpawn {
            #[cfg(test)]
            _dispose_probe: None,
            session_type_id,
            request,
            package_records,
            response: OrdinarySpawnReply::Admitted(response),
            parent: Some(parent),
        };
        let queued = {
            let mut queue = self.pending.lock().map_err(|_| {
                std::borrow::Cow::Borrowed("session-type spawn queue lock poisoned")
            })?;
            queue.try_push_back_owned(item)
        };
        if let Err((_, item)) = queued {
            drop(item);
            return Err(std::borrow::Cow::Borrowed(
                crate::lua_memory::LUA_CALLBACK_CAPACITY_EXHAUSTED,
            ));
        }
        self.publish_session_type_spawn();
        receiver
            .recv_timeout(Duration::from_millis(SESSION_TYPE_SPAWN_TIMEOUT_MS))
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => {
                    std::borrow::Cow::Borrowed("session-type spawn did not complete before timeout")
                }
                mpsc::RecvTimeoutError::Disconnected => {
                    std::borrow::Cow::Borrowed("session-type spawn owner dropped its reply")
                }
            })
    }

    fn take_pending_legacy(&self) -> Option<PendingSessionTypeSpawn> {
        let mut queue = self.pending.lock().expect("session-type spawn queue lock");
        if !matches!(queue.front()?.response, OrdinarySpawnReply::Legacy(_)) {
            return None;
        }
        // Clear under the queue lock. A later producer publishes after enqueue.
        self.ordinary_pending.store(false, Ordering::Release);
        let pending = queue.pop_front();
        let remaining = !queue.is_empty();
        drop(queue);
        if remaining {
            self.publish_session_type_spawn();
        }
        pending
    }

    fn take_pending_for_owner(&self) -> Option<PendingSessionTypeSpawn> {
        let mut queue = self.pending.lock().expect("session-type spawn queue lock");
        self.ordinary_pending.store(false, Ordering::Release);
        let pending = queue.pop_front();
        let remaining = !queue.is_empty();
        drop(queue);
        if remaining {
            self.publish_session_type_spawn();
        }
        pending
    }

    /// Queue the one atomic managed-worktree/session spawn operation.
    /// `package_records` is the committed registry view the caller read once;
    /// the capability check and the queued spawn use that same view.
    pub(crate) fn ensure_worktree_and_spawn(
        &self,
        plugin_key: &PluginKey,
        target_id: &str,
        branch: &str,
        session_type_id: &str,
        request: ManagedSessionTypeRequest,
        package_records: SharedView<PackageRegistry>,
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
            response: ManagedSpawnReply::Legacy(response),
            parent: None,
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

    fn peek_managed_worktree_id(&self) -> Option<String> {
        self.managed
            .lock()
            .ok()?
            .front()
            .map(|pending| managed_worktree_id(&pending.target_id, &pending.branch))
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

#[cfg(test)]
#[path = "runtime_ordinary_spawn_queue_tests.rs"]
mod ordinary_spawn_queue_tests;

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
            SessionReservationRefusal::SessionIdTooLong => {
                "session_reservation.session_id_too_long"
            }
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
        CoreDaemonError::Engine(ManagedSessionRuntimeError::NotSubscribed { .. }) => {
            "engine.not_subscribed"
        }
        CoreDaemonError::Registry(_) => "registry",
        CoreDaemonError::UnknownSession(_) => "unknown_session",
        CoreDaemonError::SessionNotReadable(_) => "session_not_readable",
        CoreDaemonError::MissingWorkerPath => "missing_worker_path",
        CoreDaemonError::Shutdown => "shutdown",
        CoreDaemonError::WakePump(_) => "wake_pump",
        CoreDaemonError::LifecycleCommitExhausted { .. } => "lifecycle_commit_exhausted",
        CoreDaemonError::MissingScreenResponse(_) => "missing_screen_response",
        CoreDaemonError::MissingModeFlagsResponse(_) => "missing_mode_flags_response",
        CoreDaemonError::SessionEnded(_) => "session_ended",
        CoreDaemonError::CursorReadUnsupported => "cursor_read_unsupported",
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

fn package_allows_session_type_spawn(packages: &PackageRegistry, plugin_key: &PluginKey) -> bool {
    packages.package(&plugin_key.0).is_some_and(|record| {
        matches!(record.state, PackageState::Enabled)
            && record.manifest.capabilities.iter().any(|capability| {
                capability.surface == botster_core::CapabilitySurface::SessionActions
                    && capability.scope.as_deref() == Some("session_type_spawn")
            })
    })
}

fn package_allows_managed_git_spawn(packages: &PackageRegistry, plugin_key: &PluginKey) -> bool {
    packages.package(&plugin_key.0).is_some_and(|record| {
        matches!(record.state, PackageState::Enabled)
            && record.manifest.capabilities.iter().any(|capability| {
                capability.surface == botster_core::CapabilitySurface::SessionActions
                    && capability.scope.as_deref() == Some("session_type_managed_git_spawn")
            })
    })
}

/// A committed registry view built from `records`, for tests that queue
/// spawns directly.
#[cfg(test)]
pub(crate) fn package_view_for_test(records: Vec<PackageRecord>) -> SharedView<PackageRegistry> {
    let mut snapshot = crate::packages::PackageRegistrySnapshot::empty();
    snapshot.records = records;
    let registry =
        PackageRegistry::from_snapshot(snapshot).expect("test package records form a registry");
    crate::daemon::reserve_package_registry(&SharedViewBudget::new(), registry)
        .expect("test package registry view")
}

/// A publication holding a committed registry view built from `records`.
#[cfg(test)]
pub(crate) fn package_publication_for_test(records: Vec<PackageRecord>) -> SharedPackageRegistry {
    let publication = Arc::new(
        PackageRegistryPublication::empty(&SharedViewBudget::new())
            .expect("empty test package registry"),
    );
    publication.publish(package_view_for_test(records));
    publication
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
    /// A replacement's commit failed after its preview passed: the previous
    /// event generation is unloaded, the new one is not committed, and the
    /// previous plugin still runs without its event subscriptions.
    EventPlaneStranded(EventPlaneStatus),
    /// A second event generation was staged while one was pending; package
    /// mutations are serialized, so this is an invariant break.
    EventPlaneStageOverlap,
    /// The staged generation's retained storage exceeds the attempt's
    /// prepared-byte reservation. Nothing changed.
    EventPlaneUnfunded {
        required: usize,
        reserved: usize,
    },
    /// Activation found the router lock poisoned, or its staged generation
    /// gone, after the plugin was installed.
    EventPlaneActivationFaulted,
    EventPlaneCleanup,
    EntityFamilyCleanup(PackageEntityCleanupError),
}

impl HubLuaPluginLoadError {
    pub(crate) const fn is_package_scoped_startup_failure(&self) -> bool {
        match self {
            Self::Package(_) | Self::Lua(_) | Self::Lifecycle(_) => true,
            Self::EventPlaneStranded(_)
            | Self::EventPlaneStageOverlap
            | Self::EventPlaneUnfunded { .. }
            | Self::EventPlaneActivationFaulted
            | Self::EventPlaneCleanup
            | Self::EntityFamilyCleanup(_) => false,
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
            Self::EventPlaneStranded(_) => "event_plane_replacement_stranded",
            Self::EventPlaneStageOverlap => "event_plane_stage_overlap",
            Self::EventPlaneUnfunded { .. } => "event_plane_stage_unfunded",
            Self::EventPlaneActivationFaulted => "event_plane_activation_faulted",
            Self::EventPlaneCleanup => "event_plane_cleanup_failed",
            Self::EntityFamilyCleanup(PackageEntityCleanupError::GenerationExhausted) => {
                "entity_family_generation_exhausted"
            }
            Self::EntityFamilyCleanup(PackageEntityCleanupError::Busy) => {
                "entity_family_cleanup_busy"
            }
            Self::EntityFamilyCleanup(PackageEntityCleanupError::ModelPoisoned) => {
                "causal_recovery_required"
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
            Self::EventPlaneStranded(status) => write!(
                formatter,
                "event plane replacement failed after its preview ({}): the previous \
                 event generation is unloaded, the new one is not committed, and the \
                 previous plugin version still runs without event subscriptions",
                status.as_str()
            ),
            Self::EventPlaneStageOverlap => {
                formatter.write_str("another package event generation is already staged")
            }
            Self::EventPlaneUnfunded { required, reserved } => write!(
                formatter,
                "staging the package event generation needs {required} bytes; the attempt reserved {reserved}"
            ),
            Self::EventPlaneActivationFaulted => formatter.write_str(
                "the package event generation could not be activated after the plugin was installed",
            ),
            Self::EventPlaneCleanup => {
                formatter.write_str("event router cleanup requires recovery")
            }
            Self::EntityFamilyCleanup(PackageEntityCleanupError::GenerationExhausted) => {
                formatter.write_str("entity family cleanup exhausted generation identifiers")
            }
            Self::EntityFamilyCleanup(PackageEntityCleanupError::Busy) => {
                formatter.write_str("a previous entity family cleanup remains owned")
            }
            Self::EntityFamilyCleanup(PackageEntityCleanupError::ModelPoisoned) => {
                formatter.write_str(entity_model::ENTITY_MODEL_POISONED_MESSAGE)
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
            Self::EventPlane(_)
            | Self::EventPlaneStranded(_)
            | Self::EventPlaneStageOverlap
            | Self::EventPlaneUnfunded { .. }
            | Self::EventPlaneActivationFaulted => None,
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
    accepted_id: Option<PendingOperationId>,
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
            accepted_id: None,
        }
    }

    /// Test-only: wait until every phase this tracker owns is published,
    /// without collecting any.
    #[cfg(test)]
    pub(crate) fn test_wait_published(&self, deadline: Instant) -> bool {
        match &self.stage {
            CoreOperationStage::Begin { ticket, completion } => {
                ticket.test_wait_published(deadline) && completion.test_wait_published(deadline)
            }
            CoreOperationStage::Pending { completion, .. } => {
                completion.test_wait_published(deadline)
            }
            CoreOperationStage::Done => true,
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

    /// Preserve the accepted identity for exact recovery after completion loss.
    pub(crate) fn accepted_id(&self) -> Option<PendingOperationId> {
        self.accepted_id
    }

    /// Non-blocking progress. `Ready(Err)` carries a `begin` rejection or a
    /// completion error; `Ready(Ok)` carries the completion.
    pub fn poll(
        &mut self,
        _runtime: &HubRuntime,
    ) -> CoreTicketPoll<Result<CoreCompletion, CoreDaemonError>> {
        self.poll_without_reaping()
    }

    /// Collect at most the two phases this tracker owns, then read their results.
    pub(crate) fn poll_terminal(
        &mut self,
    ) -> CoreTicketPoll<Result<CoreCompletion, CoreDaemonError>> {
        match &self.stage {
            CoreOperationStage::Begin { ticket, completion } => {
                ticket.collect_ready_phase();
                completion.collect_ready_phase();
            }
            CoreOperationStage::Pending { completion, .. } => completion.collect_ready_phase(),
            CoreOperationStage::Done => {}
        }
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
                    self.accepted_id = Some(id);
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
#[allow(clippy::large_enum_variant)] // retained in-flight record for the owner-less runtime path; entries are polled in place
#[allow(private_interfaces)]
pub(crate) enum InflightPluginCore {
    Coordination {
        ticket: crate::data_plane::driver::ChargedCoreTicket<crate::lua_runtime::CoordinationReply>,
        response: crate::lua_runtime::CoordinationReplySender,
        rejected: Option<crate::data_plane::driver::CoreRejectedRequest>,
        _storage: Option<(
            crate::lua_memory::LuaCallbackCharge,
            crate::lua_memory::LuaCallbackCharge,
        )>,
        _entry: Option<crate::lua_memory::LuaCallbackCharge>,
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

fn spawn_fail(
    error: CoreDaemonError,
    disposition: Option<SessionReservationRelease>,
) -> PluginSpawnPoll {
    PluginSpawnPoll::Ready(Err(PluginSpawnFailure { error, disposition }))
}

impl ManagedSessionSpawnStart {
    pub(crate) fn poll(
        &mut self,
        runtime: &HubRuntime,
        parent: &mut Option<crate::lua_memory::LuaCallbackCharge>,
    ) -> PluginSpawnPoll {
        loop {
            match self.stage {
                PluginSpawnStage::RetryRetained => {
                    return spawn_fail(CoreDaemonError::Shutdown, None);
                }
                PluginSpawnStage::Reserve => {
                    let polled = self.tracker.poll(runtime);
                    // Poll can accept begin and lose completion in the same call.
                    self.reserve_operation_id = self.tracker.accepted_id();
                    match polled {
                        CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                        CoreTicketPoll::Refused => {
                            return spawn_fail(
                                core_bridge_error(CoreTicketError::Overloaded),
                                None,
                            );
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
                        CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession {
                            result,
                            ..
                        })) => match result {
                            Ok(reserved) => {
                                self.reservation = Some(reserved.clone());
                                // Register before SpawnReserved so any later
                                // removal of this id finds the record. A record
                                // that already exists means a lost release
                                // obligation: do not spawn; release this token.
                                if let Some(charge) = self.record_charge.take()
                                    && runtime
                                        .session_reservations()
                                        .register(charge, reserved.identity())
                                        .is_err()
                                {
                                    eprintln!(
                                        "session reservation record invariant failed for {}",
                                        self.spawn.request.session_id.0
                                    );
                                    self.spawn_error = Some(CoreDaemonError::SessionReservation(
                                        SessionReservationRefusal::Occupied,
                                    ));
                                    self.release_or_retain(runtime);
                                    continue;
                                }
                                let context_charge = self
                                    .context
                                    .as_ref()
                                    .and_then(stored_context_bytes)
                                    .and_then(|bytes| match parent.as_mut() {
                                        Some(parent) => {
                                            parent.grow(bytes).ok()?;
                                            parent.split_fixed(bytes)
                                        }
                                        None => {
                                            runtime.lua_memory.reserve_callback_total(bytes).ok()
                                        }
                                    });
                                let published = context_charge.ok_or(()).and_then(|charge| {
                                    let context = self.context.take().ok_or(())?;
                                    runtime
                                        .publish_spawn_context(context, &reserved, charge)
                                        .map_err(|_| ())
                                });
                                if published.is_err() {
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
                        },
                        CoreTicketPoll::Ready(Ok(_)) => {
                            return spawn_fail(CoreDaemonError::Shutdown, None);
                        }
                    }
                }
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
                        let error = self.spawn_error.take().unwrap_or(CoreDaemonError::Shutdown);
                        match result {
                            Ok(SessionReservationRelease::Released) => {
                                self.retire_record(runtime);
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
            self.tracker =
                runtime.begin_release_session_reservation_for_owner(self.waiter_id, held);
            self.stage = PluginSpawnStage::Release;
        }
    }

    fn keep_reservation(&mut self, runtime: &HubRuntime) {
        if let Some(held) = self.reservation.take() {
            runtime
                .session_reservations()
                .retire(&self.spawn.request.session_id.0, held.identity());
            runtime.retain_reservation(held);
        }
    }

    /// Delete this token's record (identity-exact).
    fn retire_record(&self, runtime: &HubRuntime) {
        if let Some(held) = self.reservation.as_ref() {
            runtime
                .session_reservations()
                .retire(&self.spawn.request.session_id.0, held.identity());
        }
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.spawn.request.session_id.0
    }

    /// After delivery, give the installed token to its record. Returns
    /// `true` when ownership is settled; `false` means the session was
    /// removed while it launched and `poll_handoff` must settle the release.
    pub(crate) fn begin_handoff(&mut self, runtime: &HubRuntime) -> bool {
        use session_reservations::Handoff;
        let Some(token) = self.reservation.take() else {
            return true;
        };
        match runtime
            .session_reservations()
            .install(&self.spawn.request.session_id.0, token)
        {
            Handoff::Kept => true,
            Handoff::Unregistered(token) => {
                runtime.retain_reservation(token);
                true
            }
            Handoff::ReleaseNow(token) => {
                self.tracker = runtime
                    .begin_release_session_reservation_for_owner(self.waiter_id, token.clone());
                self.reservation = Some(token);
                self.handoff_release = true;
                false
            }
        }
    }

    /// Settle a removed-during-launch release. Returns `true` when done.
    pub(crate) fn poll_handoff(&mut self, runtime: &HubRuntime) -> bool {
        if !self.handoff_release {
            return true;
        }
        let released = match self.tracker.poll(runtime) {
            CoreTicketPoll::Pending => return false,
            CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                result: Ok(SessionReservationRelease::Released),
                ..
            })) => true,
            _ => false,
        };
        self.retire_record(runtime);
        if let Some(token) = self.reservation.take()
            && !released
        {
            eprintln!(
                "session {} was already removed; its reservation release is unconfirmed and the token is retained for retry",
                self.spawn.request.session_id.0
            );
            runtime.retain_reservation(token);
        }
        self.handoff_release = false;
        true
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
            adapter.close(
                botster_core::contract::terminal_adapter::TerminalRouteCloseReason::BindRejected,
            );
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
        self.advance_created_worktree_cleanups();
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
    owner_signal: Arc<crate::daemon::owner_signal::OwnerSignal>,
) -> (
    crate::data_plane::CloseWorkSource,
    crate::data_plane::DataPlaneDriver,
    SharedCoreDaemon,
) {
    let close_work = crate::data_plane::CloseWorkSource::new();
    let (driver, core_daemon) =
        crate::data_plane::DataPlaneDriver::start(core_config, close_work.clone(), owner_signal);
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

pub(crate) fn session_worker_path(config: &HubConfig) -> PathBuf {
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
#[path = "runtime_tests.rs"]
pub(crate) mod tests;

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
