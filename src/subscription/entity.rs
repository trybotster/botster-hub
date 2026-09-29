//! Entity subscription registration, fanout, overflow, and resync.
//!
//! This module owns one subscription lifecycle: register, snapshot, patch,
//! overflow, resync, and fanout. The daemon transport owns the accept loop,
//! connection cleanup, and control dispatch.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(test)]
use botster_core::SessionLifecycleState;
use botster_core_daemon::SessionLifecycleCursor;
#[cfg(test)]
use botster_core_daemon::{RegistrySessionState, SessionLifecycleBaseline, SessionLifecycleRecord};
use botster_hub_client::{
    DaemonDiagnostic, DaemonEntityFrame, DaemonLifecycleCounters, DaemonOperatorError,
    DaemonResponse, DaemonResponseKind, DaemonSessionEntity,
};
use serde_json::Value;

use crate::HubDaemon;
use crate::admission::budgets::DAEMON_MAX_FRAME_BYTES;
use crate::client_api_dto::response::daemon_response_base;
use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_turn::{OwnerTurnBudget, OwnerTurnCharge};
use crate::host_executor::{
    HostCommand, HostCompletion, HostCompletionPoll, HostError, HostExecutor, HostJobIdentity,
    HostResult, HostSubmitError, HostWorkPermit, SessionTypeCatalogReclamation,
};

const SESSION_DELIVERY_MAX_ITEMS: usize = 16;
const SESSION_DELIVERY_MAX_BYTES: usize = 64 * 1024;
const SESSION_DELIVERY_MAX_ELAPSED: Duration = Duration::from_millis(8);

/// A transport publishes queue capacity before it can block on a frame write.
#[derive(Debug, Clone)]
pub(crate) struct EntitySubscriptionCapacityWake(Arc<EntitySubscriptionCapacityWakeInner>);

#[derive(Debug, Default)]
struct EntitySubscriptionCapacityWakeInner {
    pending: AtomicBool,
    owner: Mutex<Option<ControlSender>>,
}

impl Default for EntitySubscriptionCapacityWake {
    fn default() -> Self {
        Self(Arc::new(EntitySubscriptionCapacityWakeInner::default()))
    }
}

impl EntitySubscriptionCapacityWake {
    pub(crate) fn bind(&self, sender: ControlSender) {
        let mut owner = self
            .0
            .owner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *owner = Some(sender.clone());
        let pending = self.0.pending.load(Ordering::Acquire);
        drop(owner);
        if pending {
            let _ = sender.try_send(ControlMessage::EntitySubscriptionCapacityReleased);
        }
    }

    pub(crate) fn publish(&self) {
        self.0.pending.store(true, Ordering::Release);
        let owner = self
            .0
            .owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some(owner) = owner {
            let _ = owner.try_send(ControlMessage::EntitySubscriptionCapacityReleased);
        }
    }

    pub(crate) fn take(&self) -> bool {
        self.0.pending.swap(false, Ordering::AcqRel)
    }
}

struct DeliveryPage {
    items: usize,
    bytes: usize,
    more: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotPageCut {
    Complete,
    ItemBudget,
    ByteBudget,
    Elapsed,
    OversizedRow,
}

struct SnapshotItemPage {
    items: Vec<Value>,
    last_id: Option<String>,
    page_charge: usize,
    bytes: usize,
    cut: SnapshotPageCut,
}

#[derive(Debug, Clone)]
pub(crate) enum EntityFrameSender {
    #[cfg(test)]
    Blocking(SyncSender<DaemonEntityFrame>),
    Async(tokio::sync::mpsc::Sender<crate::entity_delivery::EntityDelivery>),
}

#[derive(Debug)]
enum EntityFrameTrySendError {
    Full(DaemonEntityFrame),
    Disconnected,
}

impl EntityFrameSender {
    /// Call only on a host worker, which drops any rejected frame.
    pub(crate) fn send_prepared_from_worker(
        &self,
        delivery: crate::entity_delivery::PreparedEntityDelivery,
    ) -> Result<(), crate::entity_delivery::EntitySendError> {
        use crate::entity_delivery::{EntityDelivery, EntitySendError};
        match self {
            #[cfg(test)]
            Self::Blocking(sender) => {
                sender
                    .try_send(delivery.into_typed())
                    .map_err(|error| match error {
                        mpsc::TrySendError::Full(_) => EntitySendError::Full,
                        mpsc::TrySendError::Disconnected(_) => EntitySendError::Disconnected,
                    })
            }
            Self::Async(sender) => {
                sender
                    .try_send(EntityDelivery::Encoded(delivery))
                    .map_err(|error| match error {
                        tokio::sync::mpsc::error::TrySendError::Full(_) => EntitySendError::Full,
                        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                            EntitySendError::Disconnected
                        }
                    })
            }
        }
    }

    fn try_send_kind(&self, frame: DaemonEntityFrame) -> Result<(), EntityFrameTrySendError> {
        match self {
            #[cfg(test)]
            Self::Blocking(sender) => sender.try_send(frame).map_err(|error| match error {
                mpsc::TrySendError::Full(frame) => EntityFrameTrySendError::Full(frame),
                mpsc::TrySendError::Disconnected(_) => EntityFrameTrySendError::Disconnected,
            }),
            Self::Async(sender) => sender.try_send(frame.into()).map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(frame) => {
                    EntityFrameTrySendError::Full(match frame {
                        crate::entity_delivery::EntityDelivery::Typed(frame) => frame,
                        crate::entity_delivery::EntityDelivery::Encoded(_) => {
                            unreachable!("typed send returns its typed frame")
                        }
                    })
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    EntityFrameTrySendError::Disconnected
                }
            }),
        }
    }

    pub(crate) fn try_send(&self, frame: DaemonEntityFrame) -> Result<(), ()> {
        self.try_send_kind(frame).map_err(|_| ())
    }
}

fn entity_frame_exceeds_limit(frame: &DaemonEntityFrame) -> bool {
    serde_json::to_vec(frame)
        .expect("daemon entity frame values always serialize")
        .len()
        > DAEMON_MAX_FRAME_BYTES
}

#[derive(Debug)]
pub(crate) struct EntitySubscriptionState {
    sender: EntityFrameSender,
    entity_type: String,
    #[allow(dead_code)]
    cursor: Option<SessionLifecycleCursor>,
    entities: BTreeMap<String, DaemonSessionEntity>,
    definition_generation: u64,
    /// Internal catalog build version last delivered to this subscriber.
    definition_version: u64,
    definition_entities: BTreeMap<String, Value>,
    /// A session-type subscriber registered while the catalog was being
    /// rebuilt off the owner; the first delivered catalog is its snapshot.
    awaiting_initial_snapshot: bool,
    resync_reason: Option<String>,
    /// A provider disappeared; retry its terminal frame after queue capacity returns.
    terminating: bool,
    /// Local WebRTC grant that owns this subscription, when registered over DataChannel.
    /// Used so PeerClosed can sweep rows that arrived after cleanup_once's id snapshot.
    pub(crate) owner_grant_id: Option<String>,
    /// Highest package-entity snapshot/delta seq successfully applied to this stream.
    /// Built-in `session` / `session_type` families leave this `None`.
    package_last_applied_seq: Option<u64>,
    /// Package-entity subscriber is gated to targeted snapshots until caught up.
    pub(crate) package_catching_up: bool,
    package_delivery: Option<PackageSubscriptionDelivery>,
    /// Resume key for one bounded session-delivery page.
    delivery_after: Option<String>,
    /// Removes first, then projection rows. Prevents a high remove id from
    /// skipping a later lower upsert id.
    delivery_phase: DeliveryPhase,
    /// Per-subscriber monotonic snapshot sequence.
    next_seq: u64,
    /// JSON values accumulated while assembling a complete first snapshot.
    assembled_items: Vec<Value>,
    /// Encoded item bytes plus JSON array separators for `assembled_items`.
    assembled_item_bytes: usize,
    /// True until a delivery page reports no remaining work.
    needs_delivery: bool,
}

#[derive(Debug)]
struct PackageSubscriptionDelivery {
    target: std::sync::Arc<crate::plugin_entity::Target>,
    publication: Option<PackagePublication>,
}

#[derive(Debug)]
struct PackagePublication {
    identity: HostJobIdentity,
    live: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for PackagePublication {
    fn drop(&mut self) {
        self.live.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Install worker-prepared metadata before the first delivery starts.
pub(crate) fn install_package_entity_subscription(
    state: &mut DaemonControlState,
    registration: crate::plugin_entity::Registration,
    target: std::sync::Arc<crate::plugin_entity::Target>,
    owner_grant_id: Option<String>,
) -> Result<
    crate::admission::reservations::PreparedSubscriptionIdentity,
    crate::plugin_entity::Registration,
> {
    if state
        .entity_subscriptions
        .contains_key(&registration.subscription_id)
    {
        return Err(registration);
    }
    let crate::plugin_entity::Registration {
        subscription_id,
        target_key,
        entity_type,
        reservation,
    } = registration;
    state
        .plugin_entities
        .targets
        .insert(target_key, std::sync::Arc::clone(&target));
    state.entity_subscriptions.insert(
        subscription_id,
        EntitySubscriptionState {
            sender: target.sender.clone(),
            entity_type,
            cursor: None,
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_version: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id,
            package_last_applied_seq: None,
            package_catching_up: true,
            package_delivery: Some(PackageSubscriptionDelivery {
                target,
                publication: None,
            }),
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        },
    );
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;
    state.lifecycle_counters.high_water_entity_subscriptions = state
        .lifecycle_counters
        .high_water_entity_subscriptions
        .max(state.lifecycle_counters.live_entity_subscriptions);
    Ok(reservation)
}

/// Inspect one subscription. The caller advances the cursor even when delivery is unnecessary.
pub(crate) fn next_package_entity_target(
    state: &DaemonControlState,
    after: Option<&crate::plugin_entity::Target>,
) -> Option<std::sync::Arc<crate::plugin_entity::Target>> {
    let lower = after.map_or(Bound::Unbounded, |target| {
        Bound::Excluded(target.subscription_id.as_str())
    });
    state
        .plugin_entities
        .targets
        .range::<str, _>((lower, Bound::Unbounded))
        .next()
        .map(|(_, target)| std::sync::Arc::clone(target))
}

/// Read catch-up state only for the exact live target.
pub(crate) fn exact_package_entity_target_catching_up(
    state: &DaemonControlState,
    target: &std::sync::Arc<crate::plugin_entity::Target>,
) -> bool {
    state
        .entity_subscriptions
        .get(&target.subscription_id)
        .is_some_and(|subscription| {
            !subscription.terminating
                && subscription.package_catching_up
                && subscription
                    .package_delivery
                    .as_ref()
                    .is_some_and(|delivery| std::sync::Arc::ptr_eq(target, &delivery.target))
        })
}

/// Arm one publication only when the current subscription needs the payload.
pub(crate) fn arm_package_entity_delivery(
    state: &mut DaemonControlState,
    target: &std::sync::Arc<crate::plugin_entity::Target>,
    identity: HostJobIdentity,
    sequence: u64,
    snapshot: bool,
    family_floor: u64,
) -> Option<(
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    Option<String>,
)> {
    let subscription = state
        .entity_subscriptions
        .get_mut(&target.subscription_id)?;
    if subscription.terminating {
        return None;
    }
    let delivery = subscription.package_delivery.as_mut()?;
    if !std::sync::Arc::ptr_eq(target, &delivery.target) {
        return None;
    }
    let applied = subscription.package_last_applied_seq;
    if snapshot {
        // A floor above an in-sync subscriber's applied sequence means admitted
        // deltas are queued for it, not that it needs a snapshot. It takes one
        // only when the snapshot brings it to the floor; a stale resync
        // snapshot from a provider behind the floor would otherwise mark it
        // catching up and drop every queued delta.
        let in_sync = !subscription.package_catching_up && subscription.resync_reason.is_none();
        if applied.is_some_and(|applied| sequence < applied)
            || (in_sync
                && applied
                    .is_some_and(|applied| applied >= family_floor || sequence < family_floor))
        {
            return None;
        }
    } else if subscription.package_catching_up
        || applied.and_then(|applied| applied.checked_add(1)) != Some(sequence)
    {
        if applied.is_none_or(|applied| sequence > applied) {
            subscription.package_catching_up = true;
        }
        return None;
    }
    let live = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    delivery.publication = Some(PackagePublication {
        identity,
        live: std::sync::Arc::clone(&live),
    });
    state.lifecycle_counters.entity_delivery_attempts = state
        .lifecycle_counters
        .entity_delivery_attempts
        .saturating_add(1);
    Some((live, subscription.resync_reason.clone()))
}

/// Apply a completion only to the exact subscription and publication that admitted it.
pub(crate) fn complete_package_entity_delivery(
    state: &mut DaemonControlState,
    target: &std::sync::Arc<crate::plugin_entity::Target>,
    identity: HostJobIdentity,
    sequence: u64,
    snapshot: bool,
    family_floor: u64,
    status: crate::plugin_entity::DeliveryStatus,
) -> bool {
    let Some(subscription) = state.entity_subscriptions.get_mut(&target.subscription_id) else {
        return false;
    };
    let Some(delivery) = subscription.package_delivery.as_mut() else {
        return false;
    };
    if !std::sync::Arc::ptr_eq(target, &delivery.target)
        || !delivery
            .publication
            .as_ref()
            .is_some_and(|publication| publication.identity == identity)
    {
        return false;
    }
    delivery.publication = None;
    if subscription.terminating {
        state
            .maintenance
            .wakes
            .mark(crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery);
        return false;
    }
    use crate::plugin_entity::DeliveryStatus;
    match status {
        DeliveryStatus::Sent => {
            subscription.package_last_applied_seq = Some(
                subscription
                    .package_last_applied_seq
                    .map_or(sequence, |applied| applied.max(sequence)),
            );
            if snapshot {
                subscription.package_catching_up = sequence < family_floor;
            }
            subscription.resync_reason = None;
            state.lifecycle_counters.entity_delivery_successes = state
                .lifecycle_counters
                .entity_delivery_successes
                .saturating_add(1);
        }
        DeliveryStatus::Full | DeliveryStatus::Capacity | DeliveryStatus::Invalid => {
            subscription.package_catching_up = true;
            subscription.resync_reason = Some(
                match status {
                    DeliveryStatus::Invalid => "entity_provider_frame_too_large",
                    _ => "subscriber_overflow",
                }
                .into(),
            );
            state.lifecycle_counters.entity_delivery_overflows = state
                .lifecycle_counters
                .entity_delivery_overflows
                .saturating_add(1);
        }
        DeliveryStatus::Disconnected => {
            state.entity_subscriptions.remove(&target.subscription_id);
            state
                .plugin_entities
                .targets
                .remove(&target.subscription_id);
            state.lifecycle_counters.entity_delivery_failures = state
                .lifecycle_counters
                .entity_delivery_failures
                .saturating_add(1);
            state.lifecycle_counters.live_entity_subscriptions =
                state.entity_subscriptions.len() as u64;
            return false;
        }
        DeliveryStatus::Cancelled => {}
    }
    subscription.package_catching_up
}

#[cfg(test)]
impl EntitySubscriptionState {
    pub(crate) fn send_frame_for_test(&self, frame: DaemonEntityFrame) -> Result<(), ()> {
        self.sender.try_send(frame)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryPhase {
    Assembling { source_seq: u64 },
    Removes,
    Rows,
}

/// Session-type catalog built off the owner thread and cached by generation.
///
/// The owner never lists session types itself. When the generation moves, it
/// hands the package records and durable state to one worker thread and
/// applies the catalog on a later turn.
#[derive(Default)]
pub(crate) struct SessionTypeCatalogCache {
    terminal: Option<crate::host_disposal::Job>,
    /// Construction assigns this cache an Owner identity. Each admitted build records its exact Host identity.
    last_identity: Option<HostJobIdentity>,
    generation: Option<u64>,
    entities: BTreeMap<String, Value>,
    /// Logical encoded bytes retained by `entities` after its host permit releases.
    logical_bytes: usize,
    prepared_charge: Option<crate::host_executor::HostPreparedCharge>,
    pending: Option<(HostJobIdentity, u64)>,
    completion: Option<HostCompletion>,
    requested_generation: Option<u64>,
    waiting_for_capacity: bool,
    retained_reclamation: Option<(
        HostJobIdentity,
        SessionTypeCatalogReclamation,
        HostWorkPermit,
    )>,
    failure: Option<(u64, HostError)>,
    /// Count of external observations of the repository catalog (completed
    /// session-type Host reads, prepares, and materializations). A repository
    /// file edit does not advance the durable generation, so these
    /// observations are what make a cached result stale.
    observation: u64,
    /// Observation count the pending build started under.
    pending_observation: u64,
    /// Observation count the cached result (entities or failure) was built under.
    built_observation: u64,
    /// Count of accepted build results. Subscribers compare it to skip
    /// redelivery. It is internal and never a public sequence.
    version: u64,
}

enum SessionTypeCatalogRefresh<'a> {
    /// Durable generation, internal build version, and the catalog.
    Ready(u64, u64, &'a BTreeMap<String, Value>),
    Pending,
    /// Internal result version and the error. A catalog error is not
    /// terminal for a session_type subscription.
    Failed(u64, HostError),
}

impl SessionTypeCatalogCache {
    pub(crate) fn for_owner(waiter: crate::owner_identity::WaiterId) -> Self {
        Self {
            last_identity: Some(HostJobIdentity::first(waiter)),
            ..Self::default()
        }
    }

    pub(crate) fn dispose_terminal(&mut self, executor: &HostExecutor) -> bool {
        if let Some(job) = self.terminal.as_mut() {
            if let crate::host_disposal::Poll::Disposed(permit) = job.poll() {
                drop(permit);
                self.terminal.take();
                self.pending = None;
                self.logical_bytes = 0;
                return true;
            }
            return false;
        }
        let mut payload: Option<Box<dyn Send>> = None;
        let (identity, permit) = if let Some(completion) = self.completion.take() {
            let (identity, result, permit) = completion.into_parts();
            payload = Some(Box::new(result));
            (identity, permit)
        } else if let Some((identity, reclamation, permit)) = self.retained_reclamation.take() {
            payload = Some(Box::new(reclamation));
            (identity, permit)
        } else if self.pending.is_some() {
            return false;
        } else if self.entities.is_empty()
            && self.prepared_charge.is_none()
            && self.failure.is_none()
        {
            return true;
        } else {
            let Some(identity) = self.last_identity else {
                return false;
            };
            let Some(permit) = executor.try_reserve() else {
                return false;
            };
            (identity, permit)
        };
        self.terminal = Some(crate::host_disposal::Job::new(
            crate::host_disposal::Parts {
                storage: None,
                identity,
                permit,
                model: None,
                payload: Box::new((
                    std::mem::take(&mut self.entities),
                    self.prepared_charge.take(),
                    self.failure.take(),
                    payload,
                )),
            },
        ));
        false
    }

    fn accepts(&self, identity: HostJobIdentity) -> bool {
        self.pending
            .is_some_and(|(expected, _)| expected == identity)
    }

    fn retain_completion(&mut self, completion: HostCompletion) -> Option<HostJobIdentity> {
        if !self.accepts(completion.identity) || self.completion.is_some() {
            return None;
        }
        let identity = completion.identity;
        self.completion = Some(completion);
        Some(identity)
    }
    /// Return the requested catalog or submit one bounded off-owner build.
    fn refresh(
        &mut self,
        daemon: &HubDaemon,
        generation: u64,
        waiter_ids: &crate::owner_identity::WaiterIdSource,
    ) -> SessionTypeCatalogRefresh<'_> {
        self.requested_generation = Some(generation);
        let Some(runtime) = daemon.runtime() else {
            return SessionTypeCatalogRefresh::Pending;
        };
        self.retry_reclamation(runtime.host_executor());
        // A cached result for this generation is always published. A newer
        // external observation only adds one follow-up build; it never hides
        // the result subscribers can use now.
        if self.generation == Some(generation) {
            if let Some(error) = self.submit_follow_up(daemon, generation, waiter_ids) {
                return SessionTypeCatalogRefresh::Failed(self.version, error);
            }
            return SessionTypeCatalogRefresh::Ready(generation, self.version, &self.entities);
        }
        if let Some((failed_generation, _)) = self.failure.as_ref()
            && *failed_generation == generation
        {
            if let Some(error) = self.submit_follow_up(daemon, generation, waiter_ids) {
                return SessionTypeCatalogRefresh::Failed(self.version, error);
            }
            let error = self
                .failure
                .as_ref()
                .expect("catalog failure was checked")
                .1
                .clone();
            return SessionTypeCatalogRefresh::Failed(self.version, error);
        }
        // Only new build admission waits for a retained reclamation.
        if self.retained_reclamation.is_some() {
            self.waiting_for_capacity = true;
            return SessionTypeCatalogRefresh::Pending;
        }
        if self.pending.is_some() {
            return SessionTypeCatalogRefresh::Pending;
        }
        let Some(permit) = runtime.host_executor().try_reserve() else {
            self.waiting_for_capacity = true;
            return SessionTypeCatalogRefresh::Pending;
        };
        let Some(waiter_id) = waiter_ids.next() else {
            drop(permit);
            let error = HostError::new(
                "host_waiter_id_exhausted",
                "host waiter identity capacity is exhausted",
            );
            self.install_failure(generation, error.clone());
            return SessionTypeCatalogRefresh::Failed(self.version, error);
        };
        let identity = HostJobIdentity {
            waiter_id,
            phase: 1,
        };
        // The owner registers the identity before the job can publish completion.
        self.pending = Some((identity, generation));
        self.pending_observation = self.observation;
        self.last_identity = Some(identity);
        self.waiting_for_capacity = false;
        // These Arcs retain existing shared allocations. The job does not clone
        // the registry or state payload, so it adds no logical input bytes.
        let submitted = runtime.host_executor().submit(
            identity,
            HostCommand::BuildSessionTypeCatalog {
                generation,
                packages: daemon.package_registry_view(),
                state: runtime.state(),
            },
            permit,
        );
        if let Err(error) = submitted {
            self.pending = None;
            let (code, message) = match error.error {
                HostSubmitError::Full => (
                    "host_executor_full",
                    "host executor queue refused a reserved catalog build",
                ),
                HostSubmitError::Stopped
                | HostSubmitError::PhaseExhausted
                | HostSubmitError::WrongExecutor => (
                    "host_executor_stopped",
                    "host executor is unavailable for the catalog build",
                ),
            };
            let error = HostError::new(code, message);
            self.install_failure(generation, error.clone());
            return SessionTypeCatalogRefresh::Failed(self.version, error);
        }
        SessionTypeCatalogRefresh::Pending
    }

    /// Apply one matching completion. Superseded and duplicate results are discarded.
    fn absorb(&mut self, completion: HostCompletion, executor: &HostExecutor) -> bool {
        let (expected_identity, expected_generation) = self
            .pending
            .expect("a matching catalog completion must have a pending build");
        debug_assert_eq!(completion.identity, expected_identity);
        let (result, prepared_charge, reclamation) = if matches!(
            &*completion.result,
            HostResult::SessionTypeCatalogReady { .. }
        ) {
            let (result, prepared_charge, permit) = completion.release_for_reclamation();
            (result, prepared_charge, Some(permit))
        } else {
            let (result, prepared_charge) = completion.release();
            (result, prepared_charge, None)
        };
        self.waiting_for_capacity = false;
        self.pending = None;
        let build_observation = self.pending_observation;
        let desired_generation = self.requested_generation.unwrap_or(expected_generation);
        let result_generation = match &result {
            HostResult::SessionTypeCatalogReady { generation, .. }
            | HostResult::Failed { generation, .. } => *generation,
            // A materialization result cannot satisfy a catalog identity.
            // The mismatch path drops its payload and attached charge together.
            HostResult::OrdinarySessionTypeMaterialized(_)
            | HostResult::EntityModelComplete(_)
            | HostResult::PluginEntity(_)
            | HostResult::EventOwner(_)
            | HostResult::ClientEventCleanup(_)
            | HostResult::EntrypointsStopped
            | HostResult::StatusResponsePrepared(_)
            | HostResult::StatusResponseDelivered { .. }
            | HostResult::CoordinationResponseDelivered
            | HostResult::PluginResponseAbandoned
            | HostResult::PluginResponseDelivered { .. }
            | HostResult::Mutation(_)
            | HostResult::ManagedWorktreeCreated(_)
            | HostResult::ManagedWorktreeFailed(_)
            | HostResult::ManagedWorktreeFinalized
            | HostResult::ManagedWorktreeRecoveryRequired { .. } => {
                drop(reclamation);
                self.failure = Some((
                    expected_generation,
                    HostError::new(
                        "host_completion_kind_mismatch",
                        "a mutation completion used a catalog identity",
                    ),
                ));
                // A new failure result gets its own version. A matching Ready
                // result still publishes first, so generation is kept.
                self.version = self.version.saturating_add(1);
                return true;
            }
        };
        if result_generation != expected_generation || result_generation != desired_generation {
            if let HostResult::SessionTypeCatalogReady { entities, .. } = result {
                let permit = reclamation.expect("catalog result retained its operation slot");
                self.submit_reclamation(
                    executor,
                    expected_identity.next_phase().unwrap_or(expected_identity),
                    SessionTypeCatalogReclamation {
                        entities,
                        prepared_charge: Some(prepared_charge),
                    },
                    permit,
                );
            }
            return true;
        }
        match result {
            HostResult::SessionTypeCatalogReady {
                generation,
                entities,
                logical_bytes,
            } => {
                let permit = reclamation.expect("catalog result retained its operation slot");
                let superseded = SessionTypeCatalogReclamation {
                    entities: std::mem::replace(&mut self.entities, entities),
                    prepared_charge: self.prepared_charge.replace(prepared_charge),
                };
                self.generation = Some(generation);
                self.logical_bytes = logical_bytes;
                self.failure = None;
                self.built_observation = build_observation;
                self.version = self.version.saturating_add(1);
                self.submit_reclamation(
                    executor,
                    expected_identity.next_phase().unwrap_or(expected_identity),
                    superseded,
                    permit,
                );
            }
            HostResult::Failed { generation, error } => {
                drop(prepared_charge);
                eprintln!("session type catalog build failed: {}", error.message);
                self.install_failure(generation, error);
                self.built_observation = build_observation;
            }
            HostResult::OrdinarySessionTypeMaterialized(_)
            | HostResult::EntityModelComplete(_)
            | HostResult::PluginEntity(_)
            | HostResult::EventOwner(_)
            | HostResult::ClientEventCleanup(_)
            | HostResult::EntrypointsStopped
            | HostResult::StatusResponsePrepared(_)
            | HostResult::StatusResponseDelivered { .. }
            | HostResult::CoordinationResponseDelivered
            | HostResult::PluginResponseAbandoned
            | HostResult::PluginResponseDelivered { .. }
            | HostResult::Mutation(_)
            | HostResult::ManagedWorktreeCreated(_)
            | HostResult::ManagedWorktreeFailed(_)
            | HostResult::ManagedWorktreeFinalized
            | HostResult::ManagedWorktreeRecoveryRequired { .. } => {
                unreachable!("non-catalog result was handled above")
            }
        }
        true
    }

    fn submit_reclamation(
        &mut self,
        executor: &HostExecutor,
        identity: HostJobIdentity,
        reclamation: SessionTypeCatalogReclamation,
        permit: HostWorkPermit,
    ) {
        match executor.submit(
            identity,
            HostCommand::ReclaimSessionTypeCatalog(reclamation),
            permit,
        ) {
            Ok(()) => self.waiting_for_capacity = false,
            Err(failure) => {
                let HostCommand::ReclaimSessionTypeCatalog(reclamation) = *failure.command else {
                    unreachable!("catalog reclamation retains its command kind")
                };
                let permit = failure.permit;
                self.retained_reclamation = Some((identity, reclamation, permit));
                self.waiting_for_capacity = true;
            }
        }
    }

    fn retry_reclamation(&mut self, executor: &HostExecutor) {
        let Some((identity, reclamation, permit)) = self.retained_reclamation.take() else {
            return;
        };
        self.submit_reclamation(executor, identity, reclamation, permit);
    }

    fn waiting_for_capacity(&self) -> bool {
        self.waiting_for_capacity
    }

    fn executor_stopped(&mut self) -> bool {
        let Some((_, generation)) = self.pending.take() else {
            return false;
        };
        self.install_failure(
            generation,
            HostError::new(
                "host_executor_stopped",
                "host executor stopped before the catalog build completed",
            ),
        );
        true
    }

    /// Record one external observation of the repository catalog.
    pub(crate) fn observe_external(&mut self) {
        self.observation = self.observation.saturating_add(1);
    }

    /// Submit one build when an external observation is newer than the
    /// cached result. The cached result stays published this turn.
    /// A capacity refusal (no permit, or a full queue) waits for the host
    /// capacity notification, which marks subscriber delivery and retries.
    /// A terminal executor or waiter-identity failure becomes the catalog
    /// failure and is returned, so this refresh delivers it.
    fn submit_follow_up(
        &mut self,
        daemon: &HubDaemon,
        generation: u64,
        waiter_ids: &crate::owner_identity::WaiterIdSource,
    ) -> Option<HostError> {
        if self.built_observation == self.observation
            || self.pending.is_some()
            || self.retained_reclamation.is_some()
        {
            return None;
        }
        let runtime = daemon.runtime()?;
        let Some(permit) = runtime.host_executor().try_reserve() else {
            self.waiting_for_capacity = true;
            return None;
        };
        let Some(waiter_id) = waiter_ids.next() else {
            drop(permit);
            let error = HostError::new(
                "host_waiter_id_exhausted",
                "host waiter identity capacity is exhausted",
            );
            self.install_failure(generation, error.clone());
            self.built_observation = self.observation;
            return Some(error);
        };
        let identity = HostJobIdentity {
            waiter_id,
            phase: 1,
        };
        // The owner registers the identity before the job can publish completion.
        self.pending = Some((identity, generation));
        self.pending_observation = self.observation;
        self.last_identity = Some(identity);
        self.waiting_for_capacity = false;
        let submitted = runtime.host_executor().submit(
            identity,
            HostCommand::BuildSessionTypeCatalog {
                generation,
                packages: daemon.package_registry_view(),
                state: runtime.state(),
            },
            permit,
        );
        let Err(error) = submitted else {
            return None;
        };
        self.pending = None;
        match error.error {
            HostSubmitError::Full => {
                self.waiting_for_capacity = true;
                None
            }
            HostSubmitError::Stopped
            | HostSubmitError::PhaseExhausted
            | HostSubmitError::WrongExecutor => {
                let error = HostError::new(
                    "host_executor_stopped",
                    "host executor is unavailable for the catalog build",
                );
                self.install_failure(generation, error.clone());
                self.built_observation = self.observation;
                Some(error)
            }
        }
    }

    /// Install a new failed result. Every installed result gets a distinct
    /// version, so each subscriber receives a new failure exactly once, and
    /// repeated reads of the same cached failure keep its version.
    fn install_failure(&mut self, generation: u64, error: HostError) {
        self.generation = None;
        self.failure = Some((generation, error));
        self.version = self.version.saturating_add(1);
    }
}

/// A session-type Host operation read the repository catalog. Mark the cached
/// catalog stale and schedule a drive when session_type subscribers exist.
pub(crate) fn note_session_type_catalog_observation(state: &mut DaemonControlState) {
    state.session_type_catalog.observe_external();
    if state
        .entity_subscriptions
        .values()
        .any(|subscription| subscription.entity_type == "session_type")
    {
        state.maintenance.try_wake();
    }
}

pub(crate) fn register_builtin_entity_subscription(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    entity_type: String,
    subscription_id: String,
    sender: EntityFrameSender,
    owner_grant_id: Option<String>,
) -> DaemonTransportResult<DaemonResponse> {
    if entity_type != "session" && entity_type != "session_type" {
        return Err(DaemonTransportError::Protocol(
            "package entity subscriptions require asynchronous provider admission",
        ));
    }
    if state.entity_subscriptions.contains_key(&subscription_id) {
        return Ok(entity_subscription_error(
            "duplicate_entity_subscription",
            &subscription_id,
            "entity subscription id is already active",
        ));
    }
    if entity_type == "session_type" {
        // The catalog is built off the owner thread. A fresh cache answers
        // with the snapshot now; otherwise the subscription starts empty and
        // the drive loop sends the initial snapshot when the build lands.
        let generation = daemon
            .runtime()
            .map(|runtime| runtime.state().session_type_generation)
            .ok_or(DaemonTransportError::DaemonNotRunning)?;
        let catalog = {
            let DaemonControlState {
                session_type_catalog,
                waiter_ids,
                ..
            } = state;
            match session_type_catalog.refresh(daemon, generation, waiter_ids) {
                SessionTypeCatalogRefresh::Ready(generation, version, entities) => {
                    Ok(Some((generation, version, entities.clone())))
                }
                SessionTypeCatalogRefresh::Pending => Ok(None),
                SessionTypeCatalogRefresh::Failed(version, error) => {
                    Err((version, error.code.clone(), error.message.clone()))
                }
            }
        };
        // A catalog error is not terminal. The subscription opens, receives
        // the error, and receives its initial snapshot when a later build
        // succeeds. The failure version is marked only after the error is
        // sent; a full queue leaves it for the drive to deliver.
        let (catalog, delivered_failure) = match catalog {
            Ok(catalog) => (catalog, 0),
            Err((version, code, message)) => {
                let error = DaemonEntityFrame::Error {
                    subscription_id: subscription_id.clone(),
                    entity_type: entity_type.clone(),
                    code,
                    message,
                };
                match sender.try_send_kind(error) {
                    Ok(()) => (None, version),
                    Err(EntityFrameTrySendError::Full(_)) => {
                        state.maintenance.try_wake();
                        (None, 0)
                    }
                    Err(EntityFrameTrySendError::Disconnected) => {
                        return Err(DaemonTransportError::ControlThreadStopped);
                    }
                }
            }
        };
        if let Some((generation, _, entities)) = &catalog {
            let snapshot = DaemonEntityFrame::Snapshot {
                subscription_id: subscription_id.clone(),
                entity_type: entity_type.clone(),
                snapshot_seq: *generation,
                items: entities.values().cloned().collect(),
                resync_reason: None,
            };
            if entity_frame_exceeds_limit(&snapshot) {
                return Ok(entity_subscription_error(
                    "entity_provider_frame_too_large",
                    &subscription_id,
                    "session type snapshot exceeds daemon frame limit",
                ));
            }
            sender
                .try_send(snapshot)
                .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        }
        let (snapshot_seq, definition_version, entities, awaiting_initial_snapshot) = match catalog
        {
            Some((generation, version, entities)) => (generation, version, entities, false),
            None => (0, delivered_failure, BTreeMap::new(), true),
        };
        state.entity_subscriptions.insert(
            subscription_id.clone(),
            EntitySubscriptionState {
                sender,
                entity_type,
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: snapshot_seq,
                definition_version,
                definition_entities: entities,
                awaiting_initial_snapshot,
                resync_reason: None,
                terminating: false,
                owner_grant_id,
                package_last_applied_seq: None,
                package_catching_up: false,
                package_delivery: None,
                delivery_after: None,
                delivery_phase: DeliveryPhase::Removes,
                next_seq: snapshot_seq,
                assembled_items: Vec::new(),
                assembled_item_bytes: 0,
                needs_delivery: false,
            },
        );
        state.lifecycle_counters.live_entity_subscriptions =
            state.entity_subscriptions.len() as u64;
        state.lifecycle_counters.high_water_entity_subscriptions = state
            .lifecycle_counters
            .high_water_entity_subscriptions
            .max(state.lifecycle_counters.live_entity_subscriptions);
        return Ok(daemon_response_base(DaemonResponseKind::EntitySubscribed));
    }
    // The journal pull runs on the Core owner thread; the maintenance
    // scheduler pulls and applies it on the next owner slices.
    // A new subscriber needs delivery even when the journal has no changes.
    // Journal confirmation must retain that delivery wake until the snapshot is sent.
    state.maintenance.projection_dirty = true;
    state.maintenance.note_authoritative_mutation();
    let cursor = state.maintenance.projection.cursor.clone();
    let snapshot_seq = cursor.as_ref().map(|cursor| cursor.sequence).unwrap_or(0);
    let subscription = EntitySubscriptionState {
        sender,
        entity_type: "session".to_string(),
        cursor,
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Assembling {
            source_seq: snapshot_seq,
        },
        next_seq: snapshot_seq,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    };
    state.maintenance.journal_caught_up_confirmed = false;
    state
        .entity_subscriptions
        .insert(subscription_id.clone(), subscription);
    state.lifecycle_counters.live_entity_subscriptions = state
        .lifecycle_counters
        .live_entity_subscriptions
        .saturating_add(1);
    state.lifecycle_counters.high_water_entity_subscriptions = state
        .lifecycle_counters
        .high_water_entity_subscriptions
        .max(state.lifecycle_counters.live_entity_subscriptions);
    if state.released_entity_generations > 0 {
        state.released_entity_generations -= 1;
        state.lifecycle_counters.reconnect_registrations = state
            .lifecycle_counters
            .reconnect_registrations
            .saturating_add(1);
    }
    state
        .maintenance
        .wakes
        .mark(crate::daemon_maintenance::MaintenanceSliceKind::JournalPull);
    Ok(daemon_response_base(DaemonResponseKind::EntitySubscribed))
}

pub(crate) fn seed_lifecycle_reconciliation(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
) {
    if daemon.runtime().is_none() {
        return;
    };
    crate::daemon_maintenance::start_baseline_recovery(&mut state.maintenance);
}

fn drive_session_type_subscriptions(
    subscriptions: &mut BTreeMap<String, EntitySubscriptionState>,
    generation: u64,
    version: u64,
    entities: &BTreeMap<String, Value>,
) {
    subscriptions.retain(|subscription_id, subscription| {
        if subscription.entity_type != "session_type" {
            return true;
        }

        if subscription.awaiting_initial_snapshot || subscription.resync_reason.is_some() {
            let initial = subscription.awaiting_initial_snapshot;
            let snapshot_seq = if initial {
                generation
            } else {
                subscription.next_seq.saturating_add(1)
            };
            let frame = DaemonEntityFrame::Snapshot {
                subscription_id: subscription_id.clone(),
                entity_type: "session_type".to_string(),
                snapshot_seq,
                items: entities.values().cloned().collect(),
                resync_reason: subscription.resync_reason.clone(),
            };
            if entity_frame_exceeds_limit(&frame) {
                let error = DaemonEntityFrame::Error {
                    subscription_id: subscription_id.clone(),
                    entity_type: "session_type".to_string(),
                    code: "entity_provider_frame_too_large".to_string(),
                    message: "session type snapshot exceeds daemon frame limit".to_string(),
                };
                return match subscription.sender.try_send_kind(error) {
                    Ok(()) | Err(EntityFrameTrySendError::Disconnected) => false,
                    Err(EntityFrameTrySendError::Full(_)) => true,
                };
            }
            return match subscription.sender.try_send_kind(frame) {
                Ok(()) => {
                    subscription.next_seq = snapshot_seq;
                    subscription.definition_generation = generation;
                    subscription.definition_version = version;
                    subscription.definition_entities = entities.clone();
                    subscription.resync_reason = None;
                    subscription.awaiting_initial_snapshot = false;
                    true
                }
                Err(EntityFrameTrySendError::Full(_)) => true,
                Err(EntityFrameTrySendError::Disconnected) => false,
            };
        }

        // A same-generation rebuild after an external observation has a new
        // version and is diffed against the delivered definitions.
        if subscription.definition_generation == generation
            && subscription.definition_version == version
        {
            return true;
        }

        let mut frames = subscription
            .definition_entities
            .keys()
            .filter(|id| !entities.contains_key(*id))
            .map(|id| DaemonEntityFrame::Remove {
                subscription_id: subscription_id.clone(),
                entity_type: "session_type".to_string(),
                snapshot_seq: 0,
                id: id.clone(),
            })
            .collect::<Vec<_>>();
        frames.extend(
            entities
                .iter()
                .filter(|(id, entity)| subscription.definition_entities.get(*id) != Some(*entity))
                .map(|(id, entity)| DaemonEntityFrame::Upsert {
                    subscription_id: subscription_id.clone(),
                    entity_type: "session_type".to_string(),
                    snapshot_seq: 0,
                    id: id.clone(),
                    entity: entity.clone(),
                }),
        );
        for frame in frames {
            let snapshot_seq = subscription.next_seq.saturating_add(1);
            let frame = with_session_type_snapshot_seq(frame, snapshot_seq);
            match subscription.sender.try_send_kind(frame) {
                Ok(()) => {
                    subscription.next_seq = snapshot_seq;
                }
                Err(EntityFrameTrySendError::Full(_)) => {
                    subscription.resync_reason = Some("subscriber_overflow".to_string());
                    return true;
                }
                Err(EntityFrameTrySendError::Disconnected) => return false,
            }
        }
        subscription.definition_generation = generation;
        subscription.definition_version = version;
        subscription.definition_entities = entities.clone();
        true
    });
}

fn with_session_type_snapshot_seq(
    frame: DaemonEntityFrame,
    snapshot_seq: u64,
) -> DaemonEntityFrame {
    match frame {
        DaemonEntityFrame::Remove {
            subscription_id,
            entity_type,
            id,
            ..
        } => DaemonEntityFrame::Remove {
            subscription_id,
            entity_type,
            snapshot_seq,
            id,
        },
        DaemonEntityFrame::Upsert {
            subscription_id,
            entity_type,
            id,
            entity,
            ..
        } => DaemonEntityFrame::Upsert {
            subscription_id,
            entity_type,
            snapshot_seq,
            id,
            entity,
        },
        DaemonEntityFrame::Patch {
            subscription_id,
            entity_type,
            id,
            patch,
            ..
        } => DaemonEntityFrame::Patch {
            subscription_id,
            entity_type,
            snapshot_seq,
            id,
            patch,
        },
        DaemonEntityFrame::Snapshot {
            subscription_id,
            entity_type,
            items,
            resync_reason,
            ..
        } => DaemonEntityFrame::Snapshot {
            subscription_id,
            entity_type,
            snapshot_seq,
            items,
            resync_reason,
        },
        other => other,
    }
}

fn retire_unloaded_entity_subscriptions(
    state: &mut DaemonControlState,
    has_provider_family: impl Fn(&str) -> bool,
) {
    let before = state.entity_subscriptions.len();
    state.entity_subscriptions.retain(|id, subscription| {
        if subscription.entity_type == "session"
            || subscription.entity_type == "session_type"
            || (!subscription.terminating && has_provider_family(&subscription.entity_type))
        {
            return true;
        }
        subscription.terminating = true;
        state.plugin_entities.targets.remove(id);
        if let Some(publication) = subscription
            .package_delivery
            .as_ref()
            .and_then(|delivery| delivery.publication.as_ref())
        {
            publication
                .live
                .store(false, std::sync::atomic::Ordering::Release);
            if state
                .plugin_entities
                .accepts_host_completion(publication.identity)
            {
                return true;
            }
        }
        let error = DaemonEntityFrame::Error {
            subscription_id: id.clone(),
            entity_type: subscription.entity_type.clone(),
            code: "entity_provider_unloaded".to_string(),
            message: "entity provider was unloaded".to_string(),
        };
        match subscription.sender.try_send_kind(error) {
            Ok(()) | Err(EntityFrameTrySendError::Disconnected) => false,
            Err(EntityFrameTrySendError::Full(_)) => true,
        }
    });
    note_released_entity_generations(state, before);
}

pub(crate) fn drive_entity_subscriptions(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    if state.entity_subscriptions.is_empty() {
        return;
    }
    let Some(runtime) = daemon.runtime() else {
        state.entity_subscriptions.clear();
        state.plugin_entities.targets.clear();
        state.lifecycle_counters.live_entity_subscriptions = 0;
        return;
    };
    retire_unloaded_entity_subscriptions(state, |entity_type| {
        runtime.has_plugin_entity_provider_family(entity_type)
    });

    if state
        .entity_subscriptions
        .values()
        .any(|subscription| subscription.entity_type == "session_type")
    {
        // The catalog refresh reads the daemon immutably; the mutable runtime
        // borrow below starts only after it.
        let generation = runtime.state().session_type_generation;
        let outcome = {
            let DaemonControlState {
                session_type_catalog,
                waiter_ids,
                ..
            } = state;
            match session_type_catalog.refresh(daemon, generation, waiter_ids) {
                SessionTypeCatalogRefresh::Ready(generation, version, entities) => {
                    Ok(Some((generation, version, entities)))
                }
                SessionTypeCatalogRefresh::Pending => Ok(None),
                SessionTypeCatalogRefresh::Failed(version, error) => {
                    Err((version, error.code.clone(), error.message.clone()))
                }
            }
        };
        match outcome {
            Ok(Some((generation, version, entities))) => {
                drive_session_type_subscriptions(
                    &mut state.entity_subscriptions,
                    generation,
                    version,
                    entities,
                );
            }
            Ok(None) => {}
            Err((version, code, message)) => {
                let before = state.entity_subscriptions.len();
                let pending = drive_session_type_catalog_failure(
                    &mut state.entity_subscriptions,
                    version,
                    &code,
                    &message,
                );
                note_released_entity_generations(state, before);
                if pending {
                    state.maintenance.try_wake();
                }
            }
        }
    }
    let Some(runtime) = daemon.runtime_mut() else {
        return;
    };

    let started = Instant::now();
    let mut delivered = 0usize;
    let mut delivered_bytes = 0usize;
    let caught_up = crate::daemon_maintenance::refresh_projection_if_inventory_ahead(
        runtime,
        &mut state.maintenance,
    );
    let complete =
        state.maintenance.projection.baseline_complete && !state.maintenance.projection.gap;
    if complete {
        let resync_ids = state
            .entity_subscriptions
            .iter()
            .filter(|(_, subscription)| {
                subscription.entity_type == "session"
                    && subscription.resync_reason.is_some()
                    && !matches!(
                        subscription.delivery_phase,
                        DeliveryPhase::Assembling { .. }
                    )
            })
            .map(|(id, _)| id.clone())
            .take(1)
            .collect::<Vec<_>>();
        if let Some(subscription_id) = resync_ids.first() {
            let reason = state
                .entity_subscriptions
                .get(subscription_id)
                .and_then(|subscription| subscription.resync_reason.clone())
                .unwrap_or_else(|| "projection_gap".to_string());
            let before = state.entity_subscriptions.len();
            state.entity_subscriptions.retain(|id, subscription| {
                if id != subscription_id {
                    return true;
                }
                try_resync_from_projection(
                    id,
                    subscription,
                    &state.maintenance.projection,
                    caught_up,
                    reason.clone(),
                    &mut state.lifecycle_counters,
                )
            });
            note_released_entity_generations(state, before);
            state.maintenance.try_wake();
        } else {
            let mut more = false;
            let before = state.entity_subscriptions.len();
            state
                .entity_subscriptions
                .retain(|subscription_id, subscription| {
                    if subscription.entity_type != "session" {
                        return true;
                    }
                    if started.elapsed() >= SESSION_DELIVERY_MAX_ELAPSED
                        || delivered >= SESSION_DELIVERY_MAX_ITEMS
                        || delivered_bytes >= SESSION_DELIVERY_MAX_BYTES
                    {
                        more = true;
                        return true;
                    }
                    let (alive, page) = if matches!(
                        subscription.delivery_phase,
                        DeliveryPhase::Assembling { .. }
                    ) {
                        match continue_session_snapshot_assembly(
                            subscription_id,
                            subscription,
                            &state.maintenance.projection,
                            caught_up,
                            &mut state.lifecycle_counters,
                            SESSION_DELIVERY_MAX_ITEMS.saturating_sub(delivered),
                            SESSION_DELIVERY_MAX_BYTES.saturating_sub(delivered_bytes),
                            SESSION_DELIVERY_MAX_BYTES,
                            SESSION_DELIVERY_MAX_ELAPSED.saturating_sub(started.elapsed()),
                        ) {
                            SnapshotAssemble::Continue { page } => (true, page),
                            SnapshotAssemble::Closed {
                                frame_too_large: true,
                            }
                            | SnapshotAssemble::Closed {
                                frame_too_large: false,
                            } => (
                                false,
                                DeliveryPage {
                                    items: 0,
                                    bytes: 0,
                                    more: false,
                                },
                            ),
                        }
                    } else {
                        deliver_projection_delta_page(
                            subscription_id,
                            subscription,
                            &state.maintenance.projection,
                            &mut state.lifecycle_counters,
                            SESSION_DELIVERY_MAX_ITEMS.saturating_sub(delivered),
                            SESSION_DELIVERY_MAX_BYTES.saturating_sub(delivered_bytes),
                            SESSION_DELIVERY_MAX_ELAPSED.saturating_sub(started.elapsed()),
                        )
                    };
                    delivered = delivered.saturating_add(page.items);
                    delivered_bytes = delivered_bytes.saturating_add(page.bytes);
                    more |= page.more;
                    alive
                });
            note_released_entity_generations(state, before);
            if more {
                state.maintenance.try_wake();
            } else {
                state.maintenance.projection_dirty = false;
            }
        }
    }

    if complete {
        state
            .pending_runtime
            .retain_sessions_present_in(|session_id| {
                state.maintenance.projection.rows.contains_key(session_id)
            });
    }
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;
}

/// Drain bounded host completions and mark catalog delivery only after publication.
pub(crate) fn absorb_session_type_catalog_completions(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    owner_turn: &mut OwnerTurnBudget,
) {
    if state.host_completion_drain_faulted {
        return;
    }
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    let executor = runtime.host_executor();
    if owner_turn
        .try_charge(Instant::now(), OwnerTurnCharge::inspection(0))
        .is_ok()
        && executor.take_completion_notification()
    {
        state.host_completion_drain_pending = true;
    }
    if owner_turn
        .try_charge(Instant::now(), OwnerTurnCharge::inspection(0))
        .is_ok()
        && executor.take_capacity_notification()
    {
        state.host_capacity_wake_pending = true;
    }
    let mut catalog_changed = false;
    if state.host_completion_drain_pending
        && owner_turn
            .try_charge(Instant::now(), OwnerTurnCharge::inspection(0))
            .is_ok()
    {
        match executor.poll_completion() {
            HostCompletionPoll::Ready(completion) => {
                route_host_completion(state, completion);
            }
            HostCompletionPoll::Empty => {
                state.host_completion_drain_pending = false;
            }
            HostCompletionPoll::Stopped => {
                state.host_completion_drain_pending = false;
                catalog_changed |= state.session_type_catalog.executor_stopped();
            }
        }
    }
    if owner_turn
        .try_charge(Instant::now(), OwnerTurnCharge::inspection(0))
        .is_ok()
        && executor.take_capacity_notification()
    {
        state.host_capacity_wake_pending = true;
    }
    publish_catalog_capacity_wake(state, owner_turn);
    if catalog_changed {
        state
            .maintenance
            .wakes
            .mark(crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery);
    }
}

/// Route by the original waiter before each family validates its exact phase and receipt.
pub(crate) fn route_host_completion(state: &mut DaemonControlState, completion: HostCompletion) {
    route_host_completion_mode(state, completion, false);
}

pub(crate) fn route_terminal_host_completion(
    state: &mut DaemonControlState,
    completion: HostCompletion,
) -> Result<(), HostCompletion> {
    if state
        .host_completions
        .contains_key(&completion.identity.waiter_id)
        || (state.session_type_catalog.accepts(completion.identity)
            && (state.session_type_catalog.completion.is_some()
                || state.session_type_catalog.terminal.is_some()))
    {
        return Err(completion);
    }
    route_host_completion_mode(state, completion, true);
    Ok(())
}

fn route_host_completion_mode(
    state: &mut DaemonControlState,
    completion: HostCompletion,
    terminal: bool,
) {
    if state
        .client_events
        .owns_waiter(completion.identity.waiter_id)
    {
        let waiter = completion.identity.waiter_id;
        state.client_events.retain_completion(completion);
        crate::daemon::client_events::mark_ready(state, waiter);
    } else if state.publication_owner.accepts(completion.identity) {
        state.publication_owner.retain_completion(completion);
        crate::daemon::owner_loop::mark_publication_owner_ready(state);
    } else if state
        .package_entity_resync_scan
        .accepts(completion.identity)
    {
        state
            .package_entity_resync_scan
            .retain_completion(completion);
        crate::subscription::entity_resync::mark_ready(state);
    } else if state.event_owner.accepts(completion.identity) {
        state.event_owner.retain_completion(completion);
        crate::daemon::owner_loop::mark_event_owner_ready(state);
    } else if state.session_type_catalog.accepts(completion.identity) {
        let waiter_id = completion.identity.waiter_id;
        if state
            .session_type_catalog
            .retain_completion(completion)
            .is_some()
        {
            let _ = state.owner_ready.mark(
                waiter_id,
                crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                crate::daemon::control::pending::READY_HOST_COMPLETION,
            );
        }
    } else if state
        .plugin_entities
        .accepts_host_completion(completion.identity)
    {
        let waiter = completion.identity.waiter_id;
        let wake_retirement = state
            .plugin_entities
            .running_delivery_target(completion.identity)
            .and_then(|id| state.entity_subscriptions.get(id))
            .is_some_and(|subscription| {
                subscription.terminating
                    && subscription
                        .package_delivery
                        .as_ref()
                        .and_then(|delivery| delivery.publication.as_ref())
                        .is_some_and(|publication| publication.identity == completion.identity)
            });
        state.plugin_entities.retain_host_completion(completion);
        if wake_retirement {
            state
                .maintenance
                .wakes
                .mark(crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery);
        }
        crate::daemon::control::entities::mark_plugin_entity_ready(
            state,
            waiter,
            crate::daemon::owner_schedule::ReadyClass::HostCompletion,
            crate::daemon::control::pending::READY_HOST_COMPLETION,
        );
    } else if terminal {
        let waiter = completion.identity.waiter_id;
        state.host_completions.insert(waiter, completion);
    } else {
        crate::daemon::control::pending::absorb_host_completion(state, completion);
    }
}

pub(crate) fn drive_session_type_catalog_ready_item(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    item: crate::daemon::owner_schedule::ReadyItem,
) -> bool {
    let waiter_id = item.key().waiter_id();
    if !state
        .session_type_catalog
        .pending
        .is_some_and(|(identity, _)| identity.waiter_id == waiter_id)
    {
        return false;
    }
    let Some(completion) = state.session_type_catalog.completion.take() else {
        return true;
    };
    let Some(runtime) = daemon.runtime() else {
        return true;
    };
    if state
        .session_type_catalog
        .absorb(completion, runtime.host_executor())
    {
        state
            .maintenance
            .wakes
            .mark(crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery);
    }
    true
}

fn publish_catalog_capacity_wake(state: &mut DaemonControlState, owner_turn: &mut OwnerTurnBudget) {
    if !state.host_capacity_wake_pending
        || owner_turn
            .try_charge(Instant::now(), OwnerTurnCharge::inspection(0))
            .is_err()
    {
        return;
    }
    if state.session_type_catalog.waiting_for_capacity() {
        state
            .maintenance
            .wakes
            .mark(crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery);
    }
    if state.package_entity_resync_scan.waiting_for_host {
        state.package_entity_resync_scan.waiting_for_host = false;
        crate::subscription::entity_resync::mark_ready(state);
    }
    if state.publication_owner.waiting_for_host {
        state.publication_owner.waiting_for_host = false;
        crate::daemon::owner_loop::mark_publication_owner_ready(state);
    }
    if state.event_owner.waiting_for_host {
        crate::daemon::owner_loop::mark_event_owner_ready(state);
    }
    while state.client_events.has_capacity_waiters() {
        if owner_turn
            .try_charge(Instant::now(), OwnerTurnCharge::opaque_move())
            .is_err()
        {
            return;
        }
        if let Some(waiter) = state.client_events.pop_capacity_waiter() {
            crate::daemon::client_events::mark_ready(state, waiter);
        }
    }
    while state.plugin_controls.has_capacity_waiters() {
        if owner_turn
            .try_charge(Instant::now(), OwnerTurnCharge::opaque_move())
            .is_err()
        {
            return;
        }
        if let Some(waiter_id) = state.plugin_controls.pop_capacity_waiter() {
            crate::daemon::control::pending::mark_owner_ready(
                state,
                waiter_id,
                crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                crate::daemon::control::pending::READY_HOST_COMPLETION,
            );
        }
    }
    while !state.coordination_capacity_waiters.is_empty() {
        if owner_turn
            .try_charge(Instant::now(), OwnerTurnCharge::opaque_move())
            .is_err()
        {
            return;
        }
        if let Some(waiter_id) = state.coordination_capacity_waiters.pop_first()
            && !crate::daemon::control::pending::mark_owner_ready(
                state,
                waiter_id,
                crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                crate::daemon::control::pending::READY_HOST_COMPLETION,
            )
        {
            state.coordination_fault =
                Some(crate::daemon::control::coordination::CoordinationFault::SchedulerExhausted);
        }
    }
    while state.plugin_entities.has_capacity_waiters() {
        if owner_turn
            .try_charge(Instant::now(), OwnerTurnCharge::opaque_move())
            .is_err()
        {
            return;
        }
        if let Some(waiter) = state.plugin_entities.pop_capacity_waiter() {
            crate::daemon::control::entities::mark_plugin_entity_ready(
                state,
                waiter,
                crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                crate::daemon::control::pending::READY_HOST_COMPLETION,
            );
        }
    }
    state.host_capacity_wake_pending = false;
}

/// Deliver one catalog failure version to each session_type subscriber that
/// has not received it. A catalog error is not terminal: the subscription
/// stays open. A subscriber with a delivered baseline gets `resync_reason`,
/// so the next successful build replaces its state with a full snapshot even
/// when the definitions equal that baseline. Returns true while a full
/// subscriber queue still holds back a delivery.
fn drive_session_type_catalog_failure(
    subscriptions: &mut BTreeMap<String, EntitySubscriptionState>,
    version: u64,
    code: &str,
    message: &str,
) -> bool {
    let mut pending = false;
    subscriptions.retain(|subscription_id, subscription| {
        if subscription.entity_type != "session_type" || subscription.definition_version == version
        {
            return true;
        }
        let error = DaemonEntityFrame::Error {
            subscription_id: subscription_id.clone(),
            entity_type: "session_type".to_string(),
            code: code.to_string(),
            message: message.to_string(),
        };
        match subscription.sender.try_send_kind(error) {
            Ok(()) => {
                subscription.definition_version = version;
                if !subscription.awaiting_initial_snapshot {
                    subscription.resync_reason = Some(code.to_string());
                }
                true
            }
            Err(EntityFrameTrySendError::Disconnected) => false,
            Err(EntityFrameTrySendError::Full(_)) => {
                pending = true;
                true
            }
        }
    });
    pending
}

fn note_released_entity_generations(state: &mut DaemonControlState, before: usize) {
    let released = before.saturating_sub(state.entity_subscriptions.len()) as u64;
    if released == 0 {
        return;
    }
    state.released_entity_generations = state.released_entity_generations.saturating_add(released);
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;
}

pub(crate) fn drive_package_entity_fanout(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    crate::daemon::control::entities::begin_package_entity_fanout(daemon, state);
}

#[allow(clippy::too_many_arguments)]
fn take_snapshot_item_page(
    projection: &crate::session_projection::SessionProjection,
    after: Option<&str>,
    assembled_item_count: usize,
    envelope_bytes: usize,
    max_items: usize,
    remaining_byte_budget: usize,
    fresh_page_capacity: usize,
    max_elapsed: Duration,
) -> SnapshotItemPage {
    let started = Instant::now();
    let mut items = Vec::new();
    let mut last_id = after.map(str::to_string);
    let mut page_charge = 0usize;
    let mut cut = SnapshotPageCut::Complete;
    if envelope_bytes > fresh_page_capacity {
        return SnapshotItemPage {
            items,
            last_id,
            page_charge,
            bytes: envelope_bytes,
            cut: SnapshotPageCut::OversizedRow,
        };
    }
    if envelope_bytes > remaining_byte_budget {
        return SnapshotItemPage {
            items,
            last_id,
            page_charge,
            bytes: envelope_bytes,
            cut: SnapshotPageCut::ByteBudget,
        };
    }
    for (id, row) in rows_after(&projection.rows, after) {
        if items.len() >= max_items {
            cut = SnapshotPageCut::ItemBudget;
            break;
        }
        if started.elapsed() >= max_elapsed {
            cut = SnapshotPageCut::Elapsed;
            break;
        }
        let entity = crate::session_projection::SessionProjection::project_entity(&row.record);
        let value = serde_json::to_value(&entity).expect("serialize session entity");
        let encoded_item_len = serde_json::to_vec(&value)
            .map(|body| body.len())
            .unwrap_or(0);
        let candidate_charge = encoded_item_len.saturating_add(snapshot_separator_bytes(
            assembled_item_count.saturating_add(items.len()),
            1,
        ));
        if envelope_bytes.saturating_add(candidate_charge) > fresh_page_capacity {
            cut = SnapshotPageCut::OversizedRow;
            break;
        }
        if envelope_bytes
            .saturating_add(page_charge)
            .saturating_add(candidate_charge)
            > remaining_byte_budget
        {
            cut = SnapshotPageCut::ByteBudget;
            break;
        }
        items.push(value);
        page_charge = page_charge.saturating_add(candidate_charge);
        last_id = Some(id.clone());
    }
    SnapshotItemPage {
        items,
        last_id,
        page_charge,
        bytes: envelope_bytes.saturating_add(page_charge),
        cut,
    }
}

fn store_snapshot_items(state: &mut EntitySubscriptionState, items: &[Value]) {
    for value in items {
        if let Ok(entity) = serde_json::from_value::<DaemonSessionEntity>(value.clone()) {
            state.entities.insert(entity.session_uuid.clone(), entity);
        }
    }
}

enum SnapshotAssemble {
    Continue { page: DeliveryPage },
    Closed { frame_too_large: bool },
}

#[allow(clippy::too_many_arguments)]
fn continue_session_snapshot_assembly(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    projection: &crate::session_projection::SessionProjection,
    caught_up: bool,
    counters: &mut DaemonLifecycleCounters,
    max_items: usize,
    max_bytes: usize,
    fresh_page_capacity: usize,
    max_elapsed: Duration,
) -> SnapshotAssemble {
    if !caught_up || !projection.baseline_complete || projection.gap {
        state.needs_delivery = true;
        return SnapshotAssemble::Continue {
            page: DeliveryPage {
                items: 0,
                bytes: 0,
                more: true,
            },
        };
    }
    let live_seq = projection
        .cursor
        .as_ref()
        .map(|cursor| cursor.sequence)
        .unwrap_or(0);
    let source_seq = match state.delivery_phase {
        DeliveryPhase::Assembling { source_seq } => source_seq,
        _ => live_seq,
    };
    if source_seq != live_seq || !matches!(state.delivery_phase, DeliveryPhase::Assembling { .. }) {
        reset_snapshot_assembly(state, live_seq);
    }
    let envelope = snapshot_envelope_bytes(subscription_id, state);
    let page = take_snapshot_item_page(
        projection,
        state.delivery_after.as_deref(),
        state.assembled_items.len(),
        envelope,
        max_items.max(1),
        max_bytes,
        fresh_page_capacity.max(1),
        max_elapsed,
    );
    if page.cut == SnapshotPageCut::OversizedRow {
        return close_oversized_session_snapshot(subscription_id, state, counters);
    }
    if state
        .assembled_item_bytes
        .saturating_add(page.page_charge)
        .saturating_add(envelope)
        > DAEMON_MAX_FRAME_BYTES
    {
        return close_oversized_session_snapshot(subscription_id, state, counters);
    }
    let page_item_count = page.items.len();
    store_snapshot_items(state, &page.items);
    state.assembled_items.extend(page.items);
    state.assembled_item_bytes = state.assembled_item_bytes.saturating_add(page.page_charge);
    state.delivery_after = page.last_id;
    if page.cut != SnapshotPageCut::Complete {
        state.needs_delivery = true;
        return SnapshotAssemble::Continue {
            page: DeliveryPage {
                items: page_item_count,
                bytes: page.bytes,
                more: true,
            },
        };
    }
    let snapshot = DaemonEntityFrame::Snapshot {
        subscription_id: subscription_id.to_string(),
        entity_type: "session".to_string(),
        snapshot_seq: state.next_seq,
        items: std::mem::take(&mut state.assembled_items),
        resync_reason: state.resync_reason.clone(),
    };
    counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
    match state.sender.try_send_kind(snapshot) {
        Ok(()) => {
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
            state.resync_reason = None;
            state.delivery_phase = DeliveryPhase::Removes;
            state.delivery_after = None;
            state.assembled_item_bytes = 0;
            state.needs_delivery = false;
            SnapshotAssemble::Continue {
                page: DeliveryPage {
                    items: page_item_count,
                    bytes: page.bytes,
                    more: false,
                },
            }
        }
        Err(EntityFrameTrySendError::Full(frame)) => {
            if let DaemonEntityFrame::Snapshot { items: queued, .. } = frame {
                state.assembled_items = queued;
            }
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
            state.needs_delivery = true;
            SnapshotAssemble::Continue {
                page: DeliveryPage {
                    items: 0,
                    bytes: 0,
                    more: true,
                },
            }
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
            SnapshotAssemble::Closed {
                frame_too_large: false,
            }
        }
    }
}

fn reset_snapshot_assembly(state: &mut EntitySubscriptionState, source_seq: u64) {
    state.entities.clear();
    state.assembled_items.clear();
    state.assembled_item_bytes = 0;
    state.delivery_after = None;
    state.delivery_phase = DeliveryPhase::Assembling { source_seq };
}

#[cfg(test)]
fn encoded_item_bytes(items: &[Value]) -> usize {
    items
        .iter()
        .map(|item| serde_json::to_vec(item).map(|body| body.len()).unwrap_or(0))
        .sum()
}

fn snapshot_separator_bytes(existing_items: usize, new_items: usize) -> usize {
    if new_items == 0 {
        return 0;
    }
    new_items
        .saturating_sub(1)
        .saturating_add(usize::from(existing_items > 0))
}

fn snapshot_envelope_bytes(subscription_id: &str, state: &EntitySubscriptionState) -> usize {
    let empty = DaemonEntityFrame::Snapshot {
        subscription_id: subscription_id.to_string(),
        entity_type: "session".to_string(),
        snapshot_seq: state.next_seq,
        items: Vec::new(),
        resync_reason: state.resync_reason.clone(),
    };
    serde_json::to_vec(&empty)
        .map(|body| body.len())
        .unwrap_or(256)
}

fn close_oversized_session_snapshot(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    counters: &mut DaemonLifecycleCounters,
) -> SnapshotAssemble {
    counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
    let error = DaemonEntityFrame::Error {
        subscription_id: subscription_id.to_string(),
        entity_type: "session".to_string(),
        code: "entity_provider_frame_too_large".to_string(),
        message: "session snapshot exceeds daemon frame limit".to_string(),
    };
    match state.sender.try_send_kind(error) {
        Ok(()) => {
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
        }
        Err(EntityFrameTrySendError::Full(_)) => {
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
        }
    }
    state.needs_delivery = false;
    state.resync_reason = None;
    state.delivery_phase = DeliveryPhase::Removes;
    state.delivery_after = None;
    state.assembled_items.clear();
    state.assembled_item_bytes = 0;
    SnapshotAssemble::Closed {
        frame_too_large: true,
    }
}

fn rows_after<'a, V>(
    rows: &'a BTreeMap<String, V>,
    after: Option<&str>,
) -> impl Iterator<Item = (&'a String, &'a V)> + 'a {
    let start = match after {
        Some(after) => Bound::Excluded(after),
        None => Bound::Unbounded,
    };
    rows.range::<str, _>((start, Bound::Unbounded))
}

fn encoded_frame_len(frame: &DaemonEntityFrame) -> usize {
    serde_json::to_vec(frame)
        .map(|body| body.len())
        .unwrap_or(0)
}

fn try_resync_from_projection(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    projection: &crate::session_projection::SessionProjection,
    caught_up: bool,
    reason: String,
    counters: &mut DaemonLifecycleCounters,
) -> bool {
    state.next_seq = state.next_seq.saturating_add(1);
    state.entities.clear();
    state.delivery_after = None;
    state.delivery_phase = DeliveryPhase::Assembling {
        source_seq: projection
            .cursor
            .as_ref()
            .map(|cursor| cursor.sequence)
            .unwrap_or(0),
    };
    state.resync_reason = Some(reason);
    state.needs_delivery = true;
    match continue_session_snapshot_assembly(
        subscription_id,
        state,
        projection,
        caught_up,
        counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_ELAPSED,
    ) {
        SnapshotAssemble::Continue { page } => {
            if page.more {
                state.needs_delivery = true;
            }
            true
        }
        SnapshotAssemble::Closed {
            frame_too_large: true,
        }
        | SnapshotAssemble::Closed {
            frame_too_large: false,
        } => false,
    }
}

fn deliver_projection_delta_page(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    projection: &crate::session_projection::SessionProjection,
    counters: &mut DaemonLifecycleCounters,
    max_items: usize,
    max_bytes: usize,
    max_elapsed: Duration,
) -> (bool, DeliveryPage) {
    let started = Instant::now();
    let mut page = DeliveryPage {
        items: 0,
        bytes: 0,
        more: false,
    };
    let mut last_id = state.delivery_after.clone();
    if state.delivery_phase == DeliveryPhase::Removes {
        let after = state.delivery_after.clone();
        let mut visited = Vec::new();
        for (id, _) in rows_after(&state.entities, after.as_deref()) {
            if started.elapsed() >= max_elapsed || visited.len() >= max_items.saturating_add(1) {
                page.more = true;
                break;
            }
            visited.push(id.clone());
        }
        for id in visited {
            if page.items >= max_items || started.elapsed() >= max_elapsed {
                page.more = true;
                break;
            }
            last_id = Some(id.clone());
            if projection.rows.contains_key(&id) {
                continue;
            }
            state.next_seq = state.next_seq.saturating_add(1);
            let frame = DaemonEntityFrame::Remove {
                subscription_id: subscription_id.to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: state.next_seq,
                id: id.clone(),
            };
            let bytes = encoded_frame_len(&frame);
            if bytes > max_bytes.saturating_sub(page.bytes) {
                state.next_seq = state.next_seq.saturating_sub(1);
                if page.items == 0 {
                    counters.entity_delivery_overflows =
                        counters.entity_delivery_overflows.saturating_add(1);
                    state.resync_reason = Some("subscriber_overflow".to_string());
                    return (true, page);
                }
                page.more = true;
                break;
            }
            match send_session_delta(state, counters, frame, projection) {
                SendDelta::Alive { bytes } => {
                    page.items += 1;
                    page.bytes = page.bytes.saturating_add(bytes);
                    last_id = Some(id);
                }
                SendDelta::Overflow => return (true, page),
                SendDelta::Dead => return (false, page),
            }
        }
        if !page.more {
            state.delivery_phase = DeliveryPhase::Rows;
            state.delivery_after = None;
            last_id = None;
        }
    }
    if !page.more && state.delivery_phase == DeliveryPhase::Rows {
        let after = state.delivery_after.clone();
        for (id, row) in rows_after(&projection.rows, after.as_deref()) {
            if page.items >= max_items || started.elapsed() >= max_elapsed {
                page.more = true;
                break;
            }
            let entity = crate::session_projection::SessionProjection::project_entity(&row.record);
            let frame = match state.entities.get(id) {
                None => {
                    state.next_seq = state.next_seq.saturating_add(1);
                    DaemonEntityFrame::Upsert {
                        subscription_id: subscription_id.to_string(),
                        entity_type: "session".to_string(),
                        snapshot_seq: state.next_seq,
                        id: id.clone(),
                        entity: serde_json::to_value(&entity).expect("serialize session entity"),
                    }
                }
                Some(previous) if previous != &entity => {
                    state.next_seq = state.next_seq.saturating_add(1);
                    DaemonEntityFrame::Patch {
                        subscription_id: subscription_id.to_string(),
                        entity_type: "session".to_string(),
                        snapshot_seq: state.next_seq,
                        id: id.clone(),
                        patch: crate::session_projection::SessionProjection::entity_patch(
                            previous, &entity,
                        ),
                    }
                }
                Some(_) => {
                    last_id = Some(id.clone());
                    continue;
                }
            };
            let bytes = encoded_frame_len(&frame);
            if bytes > max_bytes.saturating_sub(page.bytes) {
                state.next_seq = state.next_seq.saturating_sub(1);
                if page.items == 0 {
                    counters.entity_delivery_overflows =
                        counters.entity_delivery_overflows.saturating_add(1);
                    state.resync_reason = Some("subscriber_overflow".to_string());
                    return (true, page);
                }
                page.more = true;
                break;
            }
            match send_session_delta(state, counters, frame, projection) {
                SendDelta::Alive { bytes } => {
                    page.items += 1;
                    page.bytes = page.bytes.saturating_add(bytes);
                    last_id = Some(id.clone());
                }
                SendDelta::Overflow => return (true, page),
                SendDelta::Dead => return (false, page),
            }
        }
        if !page.more {
            state.delivery_phase = DeliveryPhase::Removes;
            state.delivery_after = None;
            state.needs_delivery = false;
            return (true, page);
        }
    }
    state.delivery_after = last_id;
    (true, page)
}

enum SendDelta {
    Alive { bytes: usize },
    Overflow,
    Dead,
}

fn send_session_delta(
    state: &mut EntitySubscriptionState,
    counters: &mut DaemonLifecycleCounters,
    frame: DaemonEntityFrame,
    projection: &crate::session_projection::SessionProjection,
) -> SendDelta {
    let bytes = serde_json::to_vec(&frame)
        .map(|body| body.len())
        .unwrap_or(0);
    counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
    match state.sender.try_send_kind(frame.clone()) {
        Ok(()) => {
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
            match frame {
                DaemonEntityFrame::Remove { id, .. } => {
                    state.entities.remove(&id);
                }
                DaemonEntityFrame::Upsert { id, entity, .. } => {
                    if let Ok(parsed) = serde_json::from_value(entity) {
                        state.entities.insert(id, parsed);
                    }
                }
                DaemonEntityFrame::Patch { id, .. } => {
                    if let Some(row) = projection.rows.get(&id) {
                        state.entities.insert(
                            id,
                            crate::session_projection::SessionProjection::project_entity(
                                &row.record,
                            ),
                        );
                    }
                }
                _ => {}
            }
            SendDelta::Alive { bytes }
        }
        Err(EntityFrameTrySendError::Full(_)) => {
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
            state.resync_reason = Some("subscriber_overflow".to_string());
            SendDelta::Overflow
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
            SendDelta::Dead
        }
    }
}

#[cfg(test)]
fn try_resync_subscription(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    baseline: SessionLifecycleBaseline,
    reason: String,
    counters: &mut DaemonLifecycleCounters,
) -> bool {
    let cursor = baseline.cursor.clone();
    let (entities, snapshot) = entity_snapshot(subscription_id, baseline, Some(reason));
    match state.sender.try_send_kind(snapshot) {
        Ok(()) => {
            counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
            state.cursor = Some(cursor);
            state.entities = entities;
            state.resync_reason = None;
            true
        }
        Err(EntityFrameTrySendError::Full(_)) => {
            counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
            true
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
            false
        }
    }
}

#[cfg(test)]
fn entity_snapshot(
    subscription_id: &str,
    baseline: SessionLifecycleBaseline,
    resync_reason: Option<String>,
) -> (BTreeMap<String, DaemonSessionEntity>, DaemonEntityFrame) {
    let entities = baseline
        .sessions
        .iter()
        .map(project_session_entity)
        .map(|entity| (entity.session_uuid.clone(), entity))
        .collect::<BTreeMap<_, _>>();
    let frame = DaemonEntityFrame::Snapshot {
        subscription_id: subscription_id.to_string(),
        entity_type: "session".to_string(),
        snapshot_seq: baseline.cursor.sequence,
        items: entities
            .values()
            .map(|entity| serde_json::to_value(entity).expect("serialize session entity"))
            .collect(),
        resync_reason,
    };
    (entities, frame)
}

#[cfg(test)]
fn project_session_entity(record: &SessionLifecycleRecord) -> DaemonSessionEntity {
    let (lifecycle, exit_code, failure_reason) = match &record.lifecycle {
        Some(SessionLifecycleState::Starting) => (Some("starting".to_string()), None, None),
        Some(SessionLifecycleState::Running) => (Some("running".to_string()), None, None),
        Some(SessionLifecycleState::Stopping) => (Some("stopping".to_string()), None, None),
        Some(SessionLifecycleState::Exited { code }) => (Some("exited".to_string()), *code, None),
        Some(SessionLifecycleState::Failed { reason }) => {
            (Some("failed".to_string()), None, Some(reason.clone()))
        }
        None if record.session.registry_state == RegistrySessionState::Exited => {
            (Some("exited".to_string()), None, None)
        }
        None => (None, None, None),
    };
    let lifecycle_class =
        session_lifecycle_class(&record.session.registry_state, record.lifecycle.as_ref());
    let metadata = &record.metadata.entries;
    let traits = metadata
        .get("botster.session_type.traits")
        .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
        .unwrap_or_default();
    DaemonSessionEntity {
        session_uuid: record.session.session_id.0.clone(),
        registry_state: match record.session.registry_state {
            RegistrySessionState::Running => "running",
            RegistrySessionState::Stopping => "stopping",
            RegistrySessionState::Exited => "exited",
            RegistrySessionState::Stale => "stale",
        }
        .to_string(),
        lifecycle,
        lifecycle_class: lifecycle_class.to_string(),
        rows: record.session.size.rows,
        cols: record.session.size.cols,
        updated_at: record.session.updated_at,
        exit_code,
        failure_reason,
        session_type_id: metadata.get("botster.session_type.id").cloned(),
        session_type_source: metadata.get("botster.session_type.source").cloned(),
        role: metadata.get("botster.session_type.role").cloned(),
        traits,
        interaction: metadata.get("botster.session_type.interaction").cloned(),
        session_type_lifecycle: metadata.get("botster.session_type.lifecycle").cloned(),
    }
}

#[cfg(test)]
use crate::session_projection::WORKER_LOST_REASON;

#[cfg(test)]
fn session_lifecycle_class(
    registry_state: &RegistrySessionState,
    lifecycle: Option<&SessionLifecycleState>,
) -> &'static str {
    if registry_state == &RegistrySessionState::Stale {
        // Stale is usually unknown (a row Hub could not adopt). A worker Core
        // saw die is known to be gone, so that session has ended.
        match lifecycle {
            Some(SessionLifecycleState::Failed { reason }) if reason == WORKER_LOST_REASON => {
                "ended"
            }
            _ => "indeterminate",
        }
    } else {
        match lifecycle {
            Some(
                SessionLifecycleState::Starting
                | SessionLifecycleState::Running
                | SessionLifecycleState::Stopping,
            ) => "current",
            Some(SessionLifecycleState::Exited { .. } | SessionLifecycleState::Failed { .. }) => {
                "ended"
            }
            None if registry_state == &RegistrySessionState::Exited => "ended",
            None => "indeterminate",
        }
    }
}

#[cfg(test)]
fn session_entity_patch(previous: &DaemonSessionEntity, current: &DaemonSessionEntity) -> Value {
    let previous = serde_json::to_value(previous).expect("serialize previous session entity");
    let current = serde_json::to_value(current).expect("serialize current session entity");
    let previous = previous.as_object().expect("session entity object");
    let current = current.as_object().expect("session entity object");
    Value::Object(
        current
            .iter()
            .filter(|(key, value)| previous.get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

pub(crate) fn entity_subscription_error(
    code: &str,
    subscription_id: &str,
    message: &str,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: subscription_id.to_string(),
        operation: "subscribe_entities".to_string(),
        message: message.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(
            "subscribe_entities",
            message,
        )],
    });
    response
}

#[cfg(test)]
#[path = "entity_tests.rs"]
mod tests;
