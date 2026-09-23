//! Entity subscription registration, fanout, overflow, and resync.
//!
//! This module owns one subscription lifecycle: register, snapshot, patch,
//! overflow, resync, and fanout. The daemon transport owns the accept loop,
//! connection cleanup, and control dispatch.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::sync::mpsc::{self, SyncSender};
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
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::control::message::{ControlMessage, ControlSender};
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
        let mut owner = self.0.owner.lock().unwrap_or_else(|error| error.into_inner());
        *owner = Some(sender.clone());
        let pending = self.0.pending.load(Ordering::Acquire);
        drop(owner);
        if pending {
            let _ = sender.try_send(ControlMessage::EntitySubscriptionCapacityReleased);
        }
    }

    pub(crate) fn publish(&self) {
        self.0.pending.store(true, Ordering::Release);
        let owner = self.0.owner.lock().unwrap_or_else(|error| error.into_inner()).clone();
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
        if applied.is_some_and(|applied| sequence < applied)
            || !(subscription.package_catching_up
                || subscription.resync_reason.is_some()
                || applied.is_none_or(|applied| applied < family_floor))
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
}

enum SessionTypeCatalogRefresh<'a> {
    Ready(u64, &'a BTreeMap<String, Value>),
    Pending,
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
        if self.retained_reclamation.is_some() {
            self.waiting_for_capacity = true;
            return SessionTypeCatalogRefresh::Pending;
        }
        if self.generation == Some(generation) {
            return SessionTypeCatalogRefresh::Ready(generation, &self.entities);
        }
        if let Some((failed_generation, error)) = self.failure.as_ref()
            && *failed_generation == generation
        {
            return SessionTypeCatalogRefresh::Failed(generation, error.clone());
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
            self.generation = None;
            self.failure = Some((
                generation,
                HostError {
                    code: "host_waiter_id_exhausted".to_string(),
                    message: "host waiter identity capacity is exhausted".to_string(),
                },
            ));
            return SessionTypeCatalogRefresh::Failed(
                generation,
                self.failure
                    .as_ref()
                    .expect("catalog failure was set")
                    .1
                    .clone(),
            );
        };
        let identity = HostJobIdentity {
            waiter_id,
            phase: 1,
        };
        // The owner registers the identity before the job can publish completion.
        self.pending = Some((identity, generation));
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
            self.generation = None;
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
            self.failure = Some((generation, HostError::new(code, message)));
            return SessionTypeCatalogRefresh::Failed(
                generation,
                self.failure
                    .as_ref()
                    .expect("catalog failure was set")
                    .1
                    .clone(),
            );
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
            &completion.result,
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
        let desired_generation = self.requested_generation.unwrap_or(expected_generation);
        let result_generation = match &result {
            HostResult::SessionTypeCatalogReady { generation, .. }
            | HostResult::Failed { generation, .. } => *generation,
            HostResult::EntityModelComplete(_)
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
                self.generation = None;
                self.failure = Some((generation, error));
            }
            HostResult::EntityModelComplete(_)
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
                let HostCommand::ReclaimSessionTypeCatalog(reclamation) = failure.command else {
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
        self.generation = None;
        self.failure = Some((
            generation,
            HostError::new(
                "host_executor_stopped",
                "host executor stopped before the catalog build completed",
            ),
        ));
        true
    }

    fn clear_failure(&mut self, generation: u64) {
        if self
            .failure
            .as_ref()
            .is_some_and(|(failed_generation, _)| *failed_generation == generation)
        {
            self.failure = None;
        }
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
                SessionTypeCatalogRefresh::Ready(generation, entities) => {
                    Ok(Some((generation, entities.clone())))
                }
                SessionTypeCatalogRefresh::Pending => Ok(None),
                SessionTypeCatalogRefresh::Failed(generation, error) => {
                    Err((generation, error.code.clone(), error.message.clone()))
                }
            }
        };
        let catalog = match catalog {
            Ok(catalog) => catalog,
            Err((generation, code, message)) => {
                state.session_type_catalog.clear_failure(generation);
                return Ok(entity_subscription_error(&code, &subscription_id, &message));
            }
        };
        if let Some((generation, entities)) = &catalog {
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
        let (snapshot_seq, entities, awaiting_initial_snapshot) = match catalog {
            Some((generation, entities)) => (generation, entities, false),
            None => (0, BTreeMap::new(), true),
        };
        state.entity_subscriptions.insert(
            subscription_id.clone(),
            EntitySubscriptionState {
                sender,
                entity_type,
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: snapshot_seq,
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
                    subscription.definition_entities = entities.clone();
                    subscription.resync_reason = None;
                    subscription.awaiting_initial_snapshot = false;
                    true
                }
                Err(EntityFrameTrySendError::Full(_)) => true,
                Err(EntityFrameTrySendError::Disconnected) => false,
            };
        }

        if subscription.definition_generation == generation {
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
                SessionTypeCatalogRefresh::Ready(generation, entities) => {
                    Ok(Some((generation, entities)))
                }
                SessionTypeCatalogRefresh::Pending => Ok(None),
                SessionTypeCatalogRefresh::Failed(generation, error) => {
                    Err((generation, error.code.clone(), error.message.clone()))
                }
            }
        };
        match outcome {
            Ok(Some((generation, entities))) => {
                drive_session_type_subscriptions(
                    &mut state.entity_subscriptions,
                    generation,
                    entities,
                );
            }
            Ok(None) => {}
            Err((generation, code, message)) => {
                let before = state.entity_subscriptions.len();
                let pending = drive_session_type_catalog_failure(
                    &mut state.entity_subscriptions,
                    &code,
                    &message,
                );
                note_released_entity_generations(state, before);
                if pending {
                    state.maintenance.try_wake();
                } else {
                    state.session_type_catalog.clear_failure(generation);
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

fn drive_session_type_catalog_failure(
    subscriptions: &mut BTreeMap<String, EntitySubscriptionState>,
    code: &str,
    message: &str,
) -> bool {
    let mut pending = false;
    subscriptions.retain(|subscription_id, subscription| {
        if subscription.entity_type != "session_type" {
            return true;
        }
        let error = DaemonEntityFrame::Error {
            subscription_id: subscription_id.clone(),
            entity_type: "session_type".to_string(),
            code: code.to_string(),
            message: message.to_string(),
        };
        match subscription.sender.try_send_kind(error) {
            Ok(()) | Err(EntityFrameTrySendError::Disconnected) => false,
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

pub(crate) fn session_subscribers_need_delivery(state: &DaemonControlState) -> bool {
    state.entity_subscriptions.values().any(|subscription| {
        subscription.entity_type == "session"
            && (subscription.needs_delivery || subscription.resync_reason.is_some())
    })
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
fn session_lifecycle_class(
    registry_state: &RegistrySessionState,
    lifecycle: Option<&SessionLifecycleState>,
) -> &'static str {
    if registry_state == &RegistrySessionState::Stale {
        "indeterminate"
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
mod tests {
    use super::*;
    use std::fs;
    use std::sync::mpsc;

    use botster_core::{RequestId, SessionId};
    use botster_core_daemon::{
        RegistrySessionState, SessionLifecycleBaseline, SessionLifecycleCursor,
    };
    use botster_hub_client::{
        DaemonEntityFrame, DaemonLifecycleCounters, DaemonResponseKind, DaemonSessionEntity,
    };
    use serde_json::Value;

    use crate::HubDaemon;
    use crate::daemon::owner_loop::DaemonControlState;
    use crate::owner_identity::WaiterIdSource;

    fn retirement_subscription(
        sender: mpsc::SyncSender<DaemonEntityFrame>,
        entity_type: &str,
    ) -> EntitySubscriptionState {
        EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: entity_type.to_string(),
            cursor: None,
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        }
    }

    fn queued_retirement_frame(subscription_id: &str) -> DaemonEntityFrame {
        DaemonEntityFrame::Error {
            subscription_id: subscription_id.to_string(),
            entity_type: "queued.family".to_string(),
            code: "queued".to_string(),
            message: "queued frame".to_string(),
        }
    }

    fn retirement_daemon(label: &str) -> (HubDaemon, std::path::PathBuf) {
        let data_directory = std::env::temp_dir().join(format!(
            "botster-hub-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: label.to_string(),
                display_name: "Entity Retirement Test".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            transports: crate::TransportBindings::default(),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build retirement config");
        (HubDaemon::start(config).expect("start retirement daemon"), data_directory)
    }

    #[test]
    fn provider_retirement_waits_for_one_dequeue_without_other_owner_work() {
        let (mut daemon, data_directory) = retirement_daemon("retire-after-dequeue");
        let mut state = DaemonControlState::default();
        for kind in crate::daemon_maintenance::MaintenanceSliceKind::ALL {
            assert!(state.maintenance.wakes.take(kind));
        }
        assert!(!state.maintenance.wakes.has_any());
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.send(queued_retirement_frame("retiring")).expect("fill subscriber queue");
        state.entity_subscriptions.insert(
            "retiring".to_string(),
            retirement_subscription(sender, "retiring.family"),
        );
        state.lifecycle_counters.live_entity_subscriptions = 1;

        retire_unloaded_entity_subscriptions(&mut state, |_| false);
        assert!(state.entity_subscriptions["retiring"].terminating);
        assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 1);
        assert_eq!(state.released_entity_generations, 0);

        receiver.recv().expect("drain exactly one queued frame");
        let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(1);
        state.entity_capacity_wake.bind(control_tx);
        assert!(!state.maintenance.wakes.has_any());
        state.entity_capacity_wake.publish();
        assert!(matches!(
            control_rx.try_recv(),
            Ok(ControlMessage::EntitySubscriptionCapacityReleased)
        ));
        crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
        assert!(!state.entity_subscriptions.contains_key("retiring"));
        assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
        assert_eq!(state.released_entity_generations, 1);
        assert!(matches!(
            receiver.recv().expect("terminal frame after capacity"),
            DaemonEntityFrame::Error { code, .. } if code == "entity_provider_unloaded"
        ));
        daemon.stop();
        drop(daemon);
        fs::remove_dir_all(data_directory).expect("remove retirement data directory");
    }

    #[test]
    fn full_control_queue_keeps_entity_capacity_flag_until_owner_turn() {
        let (mut daemon, data_directory) = retirement_daemon("retire-full-control");
        let mut state = DaemonControlState::default();
        for kind in crate::daemon_maintenance::MaintenanceSliceKind::ALL {
            assert!(state.maintenance.wakes.take(kind));
        }
        assert!(!state.maintenance.wakes.has_any());
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.send(queued_retirement_frame("retiring")).expect("fill subscriber queue");
        state.entity_subscriptions.insert(
            "retiring".to_string(),
            retirement_subscription(sender, "retiring.family"),
        );
        state.lifecycle_counters.live_entity_subscriptions = 1;
        retire_unloaded_entity_subscriptions(&mut state, |_| false);
        receiver.recv().expect("release subscriber queue capacity");

        let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(1);
        state.entity_capacity_wake.bind(control_tx.clone());
        control_tx.try_send(ControlMessage::DataPlaneProgress).expect("fill control queue");
        assert!(!state.maintenance.wakes.has_any());
        state.entity_capacity_wake.publish();
        assert!(matches!(control_rx.try_recv(), Ok(ControlMessage::DataPlaneProgress)));
        assert!(control_rx.try_recv().is_err(), "capacity notice was dropped");
        crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
        assert!(!state.entity_subscriptions.contains_key("retiring"));
        assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
        assert!(matches!(
            receiver.recv().expect("terminal frame after dropped notice"),
            DaemonEntityFrame::Error { code, .. } if code == "entity_provider_unloaded"
        ));
        daemon.stop();
        drop(daemon);
        fs::remove_dir_all(data_directory).expect("remove retirement data directory");
    }

    #[test]
    fn provider_reload_does_not_revive_a_terminating_subscription() {
        let mut state = DaemonControlState::default();
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.send(queued_retirement_frame("retiring")).expect("fill subscriber queue");
        state.entity_subscriptions.insert(
            "retiring".to_string(),
            retirement_subscription(sender, "retiring.family"),
        );
        state.lifecycle_counters.live_entity_subscriptions = 1;
        retire_unloaded_entity_subscriptions(&mut state, |_| false);
        assert!(state.entity_subscriptions["retiring"].terminating);
        receiver.recv().expect("release subscriber queue capacity");

        retire_unloaded_entity_subscriptions(&mut state, |_| true);
        assert!(!state.entity_subscriptions.contains_key("retiring"));
        assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
        assert_eq!(state.released_entity_generations, 1);
        assert!(matches!(
            receiver.recv().expect("terminal frame survives reload"),
            DaemonEntityFrame::Error { code, .. } if code == "entity_provider_unloaded"
        ));
    }

    #[test]
    fn held_target_cannot_rearm_after_terminal_intent() {
        use std::sync::Arc;
        use std::sync::atomic::Ordering;

        let mut state = DaemonControlState::default();
        let delivery = crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery;
        assert!(state.maintenance.wakes.take(delivery));
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let target = Arc::new(crate::plugin_entity::Target {
            subscription_id: "retiring".to_string(),
            entity_type: "retiring.family".to_string(),
            sender: EntityFrameSender::Async(sender.clone()),
        });
        install_package_entity_subscription(
            &mut state,
            crate::plugin_entity::Registration {
                subscription_id: "retiring".to_string(),
                target_key: "retiring".to_string(),
                entity_type: "retiring.family".to_string(),
                reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                    "retiring".to_string(),
                ),
            },
            Arc::clone(&target),
            None,
        )
        .expect("install provider subscription");
        let identity =
            crate::owner_identity::OwnerWorkIdentity::first(crate::owner_identity::WaiterId(904));
        let (live, _) =
            arm_package_entity_delivery(&mut state, &target, identity, 1, true, 1)
                .expect("arm held target");
        sender
            .try_send(crate::entity_delivery::EntityDelivery::Typed(
                queued_retirement_frame("retiring"),
            ))
            .expect("fill the subscriber queue");

        retire_unloaded_entity_subscriptions(&mut state, |_| false);
        assert!(state.entity_subscriptions["retiring"].terminating);
        assert!(!live.load(Ordering::Acquire));
        assert!(!state.plugin_entities.targets.contains_key("retiring"));
        assert!(arm_package_entity_delivery(&mut state, &target, identity, 1, true, 1).is_none());
        assert!(!state.maintenance.wakes.take(delivery));
        assert!(!complete_package_entity_delivery(
            &mut state,
            &target,
            identity,
            1,
            true,
            1,
            crate::plugin_entity::DeliveryStatus::Sent,
        ));
        assert_eq!(state.entity_subscriptions["retiring"].package_last_applied_seq, None);
        assert!(state.maintenance.wakes.take(delivery));
        receiver.try_recv().expect("drain queued frame");
        retire_unloaded_entity_subscriptions(&mut state, |_| true);
        assert!(!state.entity_subscriptions.contains_key("retiring"));
        assert!(matches!(
            receiver.try_recv().expect("terminal frame after held work"),
            crate::entity_delivery::EntityDelivery::Typed(DaemonEntityFrame::Error { code, .. })
                if code == "entity_provider_unloaded"
        ));
    }

    #[test]
    fn running_publication_waits_for_the_routed_host_receipt() {
        use std::sync::Arc;
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};

        for malformed in [false, true] {
            let mut state = DaemonControlState::default();
            let delivery = crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery;
            assert!(state.maintenance.wakes.take(delivery));
            let mut executor = crate::host_executor::HostExecutor::new();
            let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
            let target = Arc::new(crate::plugin_entity::Target {
                subscription_id: "retiring".to_string(),
                entity_type: "retiring.family".to_string(),
                sender: EntityFrameSender::Async(sender),
            });
            install_package_entity_subscription(
                &mut state,
                crate::plugin_entity::Registration {
                    subscription_id: "retiring".to_string(),
                    target_key: "retiring".to_string(),
                    entity_type: "retiring.family".to_string(),
                    reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                        "retiring".to_string(),
                    ),
                },
                Arc::clone(&target),
                None,
            )
            .expect("install provider subscription");
            let waiter = crate::owner_identity::WaiterId(if malformed { 906 } else { 905 });
            let expected = crate::owner_identity::OwnerWorkIdentity::first(waiter);
            let (live, _) = arm_package_entity_delivery(
                &mut state, &target, expected, 1, true, 1,
            )
            .expect("arm running publication");
            let command = if malformed {
                crate::plugin_entity::Command::Discard {
                    payload: None,
                    registration: None,
                    reservation_identity: None,
                }
            } else {
                crate::plugin_entity::Command::Deliver {
                    payload: crate::plugin_entity::Payload::mutation(
                        crate::package_entity_fanout::PackageEntityMutation::Upsert {
                            admission: None,
                            entity_type: "retiring.family".to_string(),
                            snapshot_seq: 1,
                            id: "one".to_string(),
                            entity: serde_json::json!({"id": "one"}),
                        },
                    ),
                    target: Arc::clone(&target),
                    publication_live: Arc::clone(&live),
                    budget: crate::shared_view::SharedViewBudget::new(),
                    resync_reason: None,
                }
            };
            let permit = state.budget.reserve().expect("reserve owner row");
            let identity = state.plugin_entities.test_insert_delivery_work(
                waiter, permit, Arc::clone(&target), Arc::clone(&live),
                &mut executor, command, false,
            );
            assert_eq!(identity, expected);
            assert!(state.plugin_entities.accepts_host_completion(identity));

            retire_unloaded_entity_subscriptions(&mut state, |_| false);
            assert!(state.entity_subscriptions["retiring"].terminating);
            assert!(!live.load(Ordering::Acquire));
            assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 1);
            while let Ok(frame) = receiver.try_recv() {
                assert!(!matches!(frame,
                    crate::entity_delivery::EntityDelivery::Typed(DaemonEntityFrame::Error { .. })
                ), "terminal must wait for the Host receipt");
            }

            let deadline = Instant::now() + Duration::from_secs(5);
            let completion = loop {
                match executor.poll_completion() {
                    crate::host_executor::HostCompletionPoll::Ready(completion) => break completion,
                    crate::host_executor::HostCompletionPoll::Empty => {
                        assert!(Instant::now() < deadline, "Host receipt must arrive");
                        std::thread::yield_now();
                    }
                    crate::host_executor::HostCompletionPoll::Stopped => {
                        panic!("Host executor stopped before its receipt")
                    }
                }
            };
            assert_eq!(completion.identity, identity);
            if malformed {
                assert!(matches!(&completion.result,
                    crate::host_executor::HostResult::PluginEntity(
                        crate::plugin_entity::Completion::Reclaimed
                    )
                ));
            }
            assert!(!state.maintenance.wakes.take(delivery));
            route_host_completion(&mut state, completion);
            assert!(state.maintenance.wakes.take(delivery));
            retire_unloaded_entity_subscriptions(&mut state, |_| false);
            assert!(!state.entity_subscriptions.contains_key("retiring"));
            assert_eq!(state.released_entity_generations, 1);
            let mut terminal = false;
            while let Ok(frame) = receiver.try_recv() {
                assert!(!terminal, "no frame may follow the terminal Error");
                if matches!(frame,
                    crate::entity_delivery::EntityDelivery::Typed(
                        DaemonEntityFrame::Error { code, .. }
                    ) if code == "entity_provider_unloaded"
                ) {
                    terminal = true;
                }
            }
            assert!(terminal, "terminal follows the exact Host receipt");
        }
    }

    #[test]
    fn rejected_delivery_does_not_hold_the_terminal_frame() {
        use std::sync::Arc;

        let mut state = DaemonControlState::default();
        let mut executor = crate::host_executor::HostExecutor::new();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let target = Arc::new(crate::plugin_entity::Target {
            subscription_id: "retiring".to_string(),
            entity_type: "retiring.family".to_string(),
            sender: EntityFrameSender::Async(sender),
        });
        install_package_entity_subscription(
            &mut state,
            crate::plugin_entity::Registration {
                subscription_id: "retiring".to_string(),
                target_key: "retiring".to_string(),
                entity_type: "retiring.family".to_string(),
                reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                    "retiring".to_string(),
                ),
            },
            Arc::clone(&target),
            None,
        )
        .expect("install provider subscription");
        let waiter = crate::owner_identity::WaiterId(907);
        let expected = crate::owner_identity::OwnerWorkIdentity::first(waiter);
        let (live, _) = arm_package_entity_delivery(
            &mut state, &target, expected, 1, true, 1,
        )
        .expect("arm publication");
        let permit = state.budget.reserve().expect("reserve owner row");
        let identity = state.plugin_entities.test_insert_delivery_work(
            waiter, permit, Arc::clone(&target), Arc::clone(&live),
            &mut executor,
            crate::plugin_entity::Command::Discard {
                payload: None,
                registration: None,
                reservation_identity: None,
            },
            true,
        );
        assert_eq!(identity, expected);
        assert!(!state.plugin_entities.accepts_host_completion(identity));

        retire_unloaded_entity_subscriptions(&mut state, |_| false);
        assert!(!state.entity_subscriptions.contains_key("retiring"));
        assert_eq!(state.released_entity_generations, 1);
        assert!(matches!(
            receiver.try_recv().expect("rejected submission has no Host receipt"),
            crate::entity_delivery::EntityDelivery::Typed(DaemonEntityFrame::Error { code, .. })
                if code == "entity_provider_unloaded"
        ));
    }

    #[test]
    fn provider_retirement_keeps_a_shared_connection_sibling() {
        use crate::transport::unix::connection::{
            ConnectionCleanupGuard, ConnectionTerminalReason, handle_connection_cleanup,
        };
        let (mut daemon, data_directory) = retirement_daemon("retire-sibling");
        let mut state = DaemonControlState::default();
        let (retiring_tx, retiring_rx) = mpsc::sync_channel(1);
        let (sibling_tx, sibling_rx) = mpsc::sync_channel(1);
        let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(1);
        let cleanup_permit = control_tx.clone().try_reserve_owned().expect("cleanup permit");
        let permit = state.budget.reserve_connection().expect("connection permit");
        let mut guard = ConnectionCleanupGuard::new(
            cleanup_permit,
            "shared-client".to_string(),
            ConnectionTerminalReason::Eof,
            permit,
        );
        guard.add_entity_subscription("retiring".to_string());
        guard.add_entity_subscription("sibling".to_string());
        state.entity_subscriptions.insert(
            "retiring".to_string(),
            retirement_subscription(retiring_tx, "retiring.family"),
        );
        state.entity_subscriptions.insert(
            "sibling".to_string(),
            retirement_subscription(sibling_tx, "live.family"),
        );
        state.lifecycle_counters.live_entity_subscriptions = 2;
        retire_unloaded_entity_subscriptions(&mut state, |family| family == "live.family");
        assert!(!state.entity_subscriptions.contains_key("retiring"));
        assert!(state.entity_subscriptions.contains_key("sibling"));
        assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 1);
        assert_eq!(state.released_entity_generations, 1);
        assert!(matches!(
            retiring_rx.recv().expect("retired sibling terminal frame"),
            DaemonEntityFrame::Error { code, .. } if code == "entity_provider_unloaded"
        ));
        state.entity_subscriptions["sibling"]
            .send_frame_for_test(queued_retirement_frame("sibling"))
            .expect("surviving sibling sender remains live");
        assert!(matches!(
            sibling_rx.recv().expect("surviving sibling delivery"),
            DaemonEntityFrame::Error { code, .. } if code == "queued"
        ));
        drop(guard);
        let ControlMessage::ConnectionCleanup(cleanup) = control_rx.try_recv().expect("cleanup")
        else {
            panic!("shared connection must publish ConnectionCleanup");
        };
        handle_connection_cleanup(&mut daemon, &mut state, control_tx, cleanup);
        assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
        assert_eq!(state.released_entity_generations, 2);
        daemon.stop();
        drop(daemon);
        fs::remove_dir_all(data_directory).expect("remove retirement data directory");
    }

    #[test]
    fn confirmed_unix_disconnect_retires_terminating_subscription() {
        use crate::transport::unix::connection::{
            ConnectionCleanupGuard, ConnectionTerminalReason, handle_connection_cleanup,
        };
        let (mut daemon, data_directory) = retirement_daemon("retire-disconnect");
        let mut state = DaemonControlState::default();
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.send(queued_retirement_frame("retiring")).expect("fill subscriber queue");
        state.entity_subscriptions.insert(
            "retiring".to_string(),
            retirement_subscription(sender, "retiring.family"),
        );
        state.lifecycle_counters.live_entity_subscriptions = 1;
        retire_unloaded_entity_subscriptions(&mut state, |_| false);
        assert!(state.entity_subscriptions["retiring"].terminating);

        let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(1);
        let cleanup_permit = control_tx.clone().try_reserve_owned().expect("cleanup permit");
        let permit = state.budget.reserve_connection().expect("connection permit");
        let mut guard = ConnectionCleanupGuard::new(
            cleanup_permit,
            "retiring-client".to_string(),
            ConnectionTerminalReason::Eof,
            permit,
        );
        guard.add_entity_subscription("retiring".to_string());
        drop(guard);
        let ControlMessage::ConnectionCleanup(cleanup) = control_rx.try_recv().expect("cleanup")
        else {
            panic!("disconnect must publish ConnectionCleanup");
        };
        handle_connection_cleanup(&mut daemon, &mut state, control_tx, cleanup);
        assert!(!state.entity_subscriptions.contains_key("retiring"));
        assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
        assert_eq!(state.released_entity_generations, 1);
        assert!(matches!(
            receiver.recv().expect("original queued frame remains"),
            DaemonEntityFrame::Error { code, .. } if code == "queued"
        ));
        daemon.stop();
        drop(daemon);
        fs::remove_dir_all(data_directory).expect("remove retirement data directory");
    }

    #[test]
    fn terminal_catalog_keeps_its_own_identity_after_waiter_exhaustion() {
        let executor = HostExecutor::new();
        let mut state = DaemonControlState::default();
        let identity = state
            .session_type_catalog
            .last_identity
            .expect("construction assigns the catalog's identity");
        assert_eq!(
            state.waiter_ids.next().unwrap().0,
            identity.waiter_id.0 + 2,
            "the state allocates the lifecycle identity after the cache identity"
        );
        state.waiter_ids = WaiterIdSource::with_next(u64::MAX);
        assert!(state.waiter_ids.next().is_none());
        assert!(state.session_type_catalog.pending.is_none());
        state.session_type_catalog.failure = Some((
            1,
            HostError::new(
                "host_waiter_id_exhausted",
                "catalog could not admit a build",
            ),
        ));
        let mut slots: Vec<_> = (0..crate::host_executor::HOST_OPERATION_CAPACITY)
            .map(|_| executor.try_reserve().unwrap())
            .collect();
        assert!(!state.session_type_catalog.dispose_terminal(&executor));
        assert!(state.session_type_catalog.failure.is_some());
        assert_eq!(state.session_type_catalog.last_identity, Some(identity));
        drop(slots.pop());
        assert!(!state.session_type_catalog.dispose_terminal(&executor));
        assert_eq!(state.session_type_catalog.last_identity, Some(identity));
        assert!(state.waiter_ids.next().is_none());
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while !state.session_type_catalog.dispose_terminal(&executor) {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(executor.outstanding(), slots.len());
        assert!(state.session_type_catalog.failure.is_none());
        drop(slots);
        assert_eq!(executor.outstanding(), 0);
        assert_eq!(executor.prepared_bytes(), 0);
    }

    #[test]
    fn terminal_catalog_cache_waits_for_capacity_in_the_original_host_pool() {
        let executor = HostExecutor::new();
        let identity = HostJobIdentity::first(crate::owner_identity::WaiterId(73));
        let (result, charge) = HostCompletion::for_test(
            identity,
            HostResult::SessionTypeCatalogReady {
                generation: 1,
                entities: BTreeMap::from([("type".into(), serde_json::json!({"id": "retained"}))]),
                logical_bytes: 20,
            },
            executor.try_reserve().unwrap(),
        )
        .release();
        let HostResult::SessionTypeCatalogReady {
            entities,
            logical_bytes,
            ..
        } = result
        else {
            unreachable!()
        };
        let mut cache = SessionTypeCatalogCache {
            last_identity: Some(identity),
            entities,
            logical_bytes,
            prepared_charge: Some(charge),
            ..Default::default()
        };
        let mut slots = Vec::new();
        while let Some(permit) = executor.try_reserve() {
            slots.push(permit);
        }
        assert!(!slots.is_empty());
        // The settled cache charge also consumes the existing prepared-byte budget.
        assert!(slots.len() < crate::host_executor::HOST_OPERATION_CAPACITY);
        let bytes = executor.prepared_bytes();
        assert!(!cache.dispose_terminal(&executor));
        assert_eq!(cache.entities.len(), 1);
        assert!(cache.prepared_charge.is_some());
        assert_eq!(executor.prepared_bytes(), bytes);
        drop(slots.pop());
        assert!(!cache.dispose_terminal(&executor));
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while !cache.terminal.as_ref().unwrap().test_disposed() {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(executor.outstanding(), slots.len() + 1);
        assert!(cache.dispose_terminal(&executor));
        assert_eq!(executor.outstanding(), slots.len());
        drop(slots);
        assert_eq!(executor.prepared_bytes(), 0);
    }

    #[test]
    fn terminal_catalog_without_an_existing_identity_retains_its_failure() {
        let executor = HostExecutor::new();
        let mut cache = SessionTypeCatalogCache {
            failure: Some((
                1,
                HostError::new(
                    "host_waiter_id_exhausted",
                    "catalog has no admitted identity",
                ),
            )),
            ..Default::default()
        };
        assert!(!cache.dispose_terminal(&executor));
        assert!(cache.failure.is_some());
        assert_eq!(executor.outstanding(), 0);
    }

    #[test]
    fn terminal_catalog_duplicate_returns_the_whole_receipt_and_retains_both_slots() {
        let executor = HostExecutor::new();
        let mut state = DaemonControlState::default();
        let identity = HostJobIdentity::first(state.waiter_ids.next().unwrap());
        state.session_type_catalog.pending = Some((identity, 1));
        let result = |value: &str| HostResult::SessionTypeCatalogReady {
            generation: 1,
            entities: BTreeMap::from([("type".into(), serde_json::json!({"id": value}))]),
            logical_bytes: 20,
        };
        let original =
            HostCompletion::for_test(identity, result("first"), executor.try_reserve().unwrap());
        let duplicate =
            HostCompletion::for_test(identity, result("second"), executor.try_reserve().unwrap());
        route_terminal_host_completion(&mut state, original).unwrap();
        let duplicate = route_terminal_host_completion(&mut state, duplicate)
            .expect_err("terminal routing must return the duplicate receipt");
        assert_eq!(executor.outstanding(), 2);
        let HostResult::SessionTypeCatalogReady { entities, .. } = &state
            .session_type_catalog
            .completion
            .as_ref()
            .unwrap()
            .result
        else {
            panic!("the original catalog result remains");
        };
        assert_eq!(entities["type"]["id"], "first");
        let HostResult::SessionTypeCatalogReady { entities, .. } = &duplicate.result else {
            panic!("the duplicate catalog result remains");
        };
        assert_eq!(entities["type"]["id"], "second");
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while !state.session_type_catalog.dispose_terminal(&executor) {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(
            executor.outstanding(),
            1,
            "the rejected receipt still retains its original slot"
        );
        let (identity, result, permit) = duplicate.into_parts();
        let mut disposal = crate::host_disposal::Job::new(crate::host_disposal::Parts {
            storage: None,
            identity,
            permit,
            payload: Box::new(result),
            model: None,
        });
        loop {
            if let crate::host_disposal::Poll::Disposed(permit) = disposal.poll() {
                drop(permit);
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(executor.outstanding(), 0);
    }

    #[test]
    fn replacement_subscription_rejects_the_previous_publication() {
        use std::sync::Arc;
        use std::sync::atomic::Ordering;
        let mut state = DaemonControlState::default();
        let install = |state: &mut DaemonControlState| {
            let (sender, _receiver) = tokio::sync::mpsc::channel(1);
            let target = Arc::new(crate::plugin_entity::Target {
                subscription_id: "sub".into(),
                entity_type: "task".into(),
                sender: EntityFrameSender::Async(sender),
            });
            install_package_entity_subscription(
                state,
                crate::plugin_entity::Registration {
                    subscription_id: "sub".into(),
                    target_key: "sub".into(),
                    entity_type: "task".into(),
                    reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                        "sub".into(),
                    ),
                },
                Arc::clone(&target),
                None,
            )
            .unwrap();
            target
        };
        let old_target = install(&mut state);
        let identity =
            crate::owner_identity::OwnerWorkIdentity::first(crate::owner_identity::WaiterId(904));
        let (live, _) =
            arm_package_entity_delivery(&mut state, &old_target, identity, 19, true, 19).unwrap();
        crate::daemon::control::entities::remove_entity_subscription(&mut state, "sub");
        assert!(!live.load(Ordering::Acquire));
        let new_target = install(&mut state);
        assert!(!complete_package_entity_delivery(
            &mut state,
            &old_target,
            identity,
            19,
            true,
            19,
            crate::plugin_entity::DeliveryStatus::Sent
        ));
        assert_eq!(
            state.entity_subscriptions["sub"].package_last_applied_seq,
            None
        );
        assert!(Arc::ptr_eq(
            &next_package_entity_target(&state, None).unwrap(),
            &new_target
        ));
        assert!(next_package_entity_target(&state, Some(&new_target)).is_none());
    }

    fn drive_all_maintenance(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
        for kind in crate::daemon_maintenance::MaintenanceSliceKind::ALL {
            if kind == crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery {
                drive_entity_subscriptions(daemon, state);
            } else if let Some(runtime) = daemon.runtime() {
                crate::daemon_maintenance::run_maintenance_kind_to_completion(
                    runtime,
                    &mut state.maintenance,
                    kind,
                );
            }
        }
    }

    #[test]
    fn stale_catalog_completion_releases_capacity_without_publishing() {
        let executor = crate::host_executor::HostExecutor::new();
        let waiter_ids = WaiterIdSource::default();
        let identity = HostJobIdentity {
            waiter_id: waiter_ids.next().expect("allocate waiter identity"),
            phase: 1,
        };
        let permit = executor.try_reserve().expect("reserve catalog build");
        let mut cache = SessionTypeCatalogCache {
            generation: Some(0),
            entities: BTreeMap::from([("current".to_string(), serde_json::json!({}))]),
            logical_bytes: 9,
            pending: Some((identity, 1)),
            requested_generation: Some(2),
            ..SessionTypeCatalogCache::default()
        };
        let completion = HostCompletion::for_test(
            identity,
            HostResult::SessionTypeCatalogReady {
                generation: 1,
                entities: BTreeMap::from([("old".to_string(), serde_json::json!({}))]),
                logical_bytes: 5,
            },
            permit,
        );
        let held_permits = (1..crate::host_executor::HOST_OPERATION_CAPACITY)
            .map(|_| {
                executor
                    .try_reserve()
                    .expect("fill host operation capacity")
            })
            .collect::<Vec<_>>();
        assert!(executor.try_reserve().is_none());

        assert!(cache.absorb(completion, &executor));
        assert!(cache.pending.is_none());
        assert_eq!(cache.generation, Some(0));
        assert!(cache.entities.contains_key("current"));
        assert_eq!(cache.logical_bytes, 9);
        assert!(cache.retained_reclamation.is_none());
        drop(held_permits);
    }

    #[test]
    fn accepted_catalog_replaces_its_retained_charge_and_failure_releases_it() {
        let executor = crate::host_executor::HostExecutor::new();
        let waiter_ids = WaiterIdSource::default();
        let first_identity = HostJobIdentity {
            waiter_id: waiter_ids.next().expect("allocate first waiter identity"),
            phase: 1,
        };
        let mut cache = SessionTypeCatalogCache {
            generation: Some(1),
            entities: BTreeMap::from([("old".to_string(), serde_json::json!({}))]),
            logical_bytes: 5,
            pending: Some((first_identity, 2)),
            requested_generation: Some(2),
            ..SessionTypeCatalogCache::default()
        };
        let replacement = HostCompletion::for_test(
            first_identity,
            HostResult::SessionTypeCatalogReady {
                generation: 2,
                entities: BTreeMap::from([("new".to_string(), serde_json::json!({}))]),
                logical_bytes: 7,
            },
            executor.try_reserve().expect("reserve replacement"),
        );

        assert!(cache.absorb(replacement, &executor));
        assert_eq!(cache.generation, Some(2));
        assert!(cache.entities.contains_key("new"));
        assert!(!cache.entities.contains_key("old"));
        assert_eq!(cache.logical_bytes, 7);

        let failure_identity = HostJobIdentity {
            waiter_id: waiter_ids.next().expect("allocate failure waiter identity"),
            phase: 1,
        };
        cache.pending = Some((failure_identity, 3));
        cache.requested_generation = Some(3);
        let failure = HostCompletion::for_test(
            failure_identity,
            HostResult::Failed {
                generation: 3,
                error: HostError::new("catalog_failed", "catalog failed"),
            },
            executor.try_reserve().expect("reserve failed replacement"),
        );

        assert!(cache.absorb(failure, &executor));
        assert!(cache.generation.is_none());
        assert!(cache.entities.contains_key("new"));
        assert_eq!(cache.logical_bytes, 7);
        assert!(cache.failure.is_some());
    }

    #[test]
    fn stopped_executor_turns_a_pending_catalog_into_a_typed_failure() {
        let waiter_ids = WaiterIdSource::default();
        let mut cache = SessionTypeCatalogCache {
            pending: Some((
                HostJobIdentity {
                    waiter_id: waiter_ids.next().expect("allocate waiter identity"),
                    phase: 1,
                },
                4,
            )),
            requested_generation: Some(4),
            ..SessionTypeCatalogCache::default()
        };

        assert!(cache.executor_stopped());
        assert!(cache.pending.is_none());
        assert!(matches!(
            cache.failure,
            Some((
                4,
                HostError {
                    ref code,
                    ref message,
                },
            )) if code == "host_executor_stopped"
                && message.contains("before the catalog build completed")
        ));
    }

    #[test]
    fn catalog_failure_closes_only_session_type_subscriptions() {
        let (session_type_sender, session_type_receiver) = mpsc::sync_channel(1);
        let (session_sender, session_receiver) = mpsc::sync_channel(1);
        let mut session =
            session_type_subscription_state(session_sender, 0, 0, BTreeMap::new(), None);
        session.entity_type = "session".to_string();
        let mut subscriptions = BTreeMap::from([
            (
                "session-type".to_string(),
                session_type_subscription_state(session_type_sender, 0, 0, BTreeMap::new(), None),
            ),
            ("session".to_string(), session),
        ]);

        assert!(!drive_session_type_catalog_failure(
            &mut subscriptions,
            "catalog_failed",
            "catalog failed",
        ));
        assert_eq!(subscriptions.len(), 1);
        assert!(subscriptions.contains_key("session"));
        assert!(session_receiver.try_recv().is_err());
        assert!(matches!(
            session_type_receiver.try_recv(),
            Ok(DaemonEntityFrame::Error {
                ref subscription_id,
                ref entity_type,
                ref code,
                ..
            }) if subscription_id == "session-type"
                && entity_type == "session_type"
                && code == "catalog_failed"
        ));

        let mut cache = SessionTypeCatalogCache {
            failure: Some((3, HostError::new("catalog_failed", "catalog failed"))),
            ..SessionTypeCatalogCache::default()
        };
        cache.clear_failure(2);
        assert!(cache.failure.is_some());
        cache.clear_failure(3);
        assert!(cache.failure.is_none());
    }

    #[test]
    fn session_lifecycle_class_is_total_and_stale_first() {
        let concrete = [
            (SessionLifecycleState::Starting, "current"),
            (SessionLifecycleState::Running, "current"),
            (SessionLifecycleState::Stopping, "current"),
            (SessionLifecycleState::Exited { code: Some(0) }, "ended"),
            (
                SessionLifecycleState::Failed {
                    reason: "failed".to_string(),
                },
                "ended",
            ),
        ];
        for (lifecycle, expected) in &concrete {
            assert_eq!(
                session_lifecycle_class(&RegistrySessionState::Running, Some(lifecycle)),
                *expected
            );
            assert_eq!(
                session_lifecycle_class(&RegistrySessionState::Stale, Some(lifecycle)),
                "indeterminate"
            );
        }
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Running, None),
            "indeterminate"
        );
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Exited, None),
            "ended"
        );
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Stale, None),
            "indeterminate"
        );
    }

    #[test]
    fn session_entity_patch_explicitly_updates_required_lifecycle_class() {
        let entity = |registry_state: &str, lifecycle: Option<&str>, lifecycle_class: &str| {
            DaemonSessionEntity {
                session_uuid: "session-1".to_string(),
                registry_state: registry_state.to_string(),
                lifecycle: lifecycle.map(str::to_string),
                lifecycle_class: lifecycle_class.to_string(),
                rows: 24,
                cols: 80,
                updated_at: 1,
                exit_code: None,
                failure_reason: None,
                session_type_id: None,
                session_type_source: None,
                role: None,
                traits: Vec::new(),
                interaction: None,
                session_type_lifecycle: None,
            }
        };
        let current = entity("running", Some("running"), "current");
        let ended = entity("exited", Some("exited"), "ended");
        let no_lifecycle = entity("running", None, "indeterminate");
        let stale = entity("stale", Some("running"), "indeterminate");

        assert_eq!(
            session_entity_patch(&current, &ended)["lifecycle_class"],
            "ended"
        );
        assert_eq!(
            session_entity_patch(&current, &no_lifecycle)["lifecycle_class"],
            "indeterminate"
        );
        assert_eq!(
            session_entity_patch(&current, &stale)["lifecycle_class"],
            "indeterminate"
        );
    }

    #[test]
    fn live_session_entity_subscription_emits_exact_stale_transition_patch() {
        let data_directory = std::env::temp_dir().join(format!(
            "botster-hub-stale-transition-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "stale-transition-test".to_string(),
                display_name: "Stale Transition Test".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            transports: crate::TransportBindings::default(),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build stale transition config");
        let mut daemon = HubDaemon::start(config).expect("start stale transition daemon");
        let session_id = SessionId("stale-transition-session".to_string());
        daemon
            .runtime_mut()
            .expect("runtime initialized")
            .spawn_session_for_test(
                botster_core::SessionSpawnRequest {
                    request_id: RequestId("stale-transition-spawn".to_string()),
                    session_id: session_id.clone(),
                    executable: "/bin/sh".to_string(),
                    arguments: vec![
                        "-c".to_string(),
                        "while IFS= read -r line; do printf '%s\\n' \"$line\"; done".to_string(),
                    ],
                    working_directory: botster_core::SpawnWorkingDirectory {
                        path: ".".to_string(),
                    },
                    environment: botster_core::SpawnEnvironment::default(),
                    initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                },
                botster_core::CoreSessionMetadata::new(),
            )
            .expect("spawn worker-backed session");

        let mut state = DaemonControlState::default();
        seed_lifecycle_reconciliation(&mut daemon, &mut state);
        for _ in 0..16 {
            drive_all_maintenance(&mut daemon, &mut state);
        }
        let (sender, receiver) = mpsc::sync_channel(4);
        let response = register_builtin_entity_subscription(
            &mut daemon,
            &mut state,
            "session".to_string(),
            "stale-transition-subscription".to_string(),
            EntityFrameSender::Blocking(sender),
            None,
        )
        .expect("register entity subscription");
        assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
        let mut first = receiver.try_recv().ok();
        for _ in 0..32 {
            if first.is_some() {
                break;
            }
            drive_all_maintenance(&mut daemon, &mut state);
            first = receiver.try_recv().ok();
        }
        let first = first.expect("initial authoritative snapshot");
        match first {
            DaemonEntityFrame::Snapshot { ref items, .. } => {
                assert!(
                    items.iter().any(|entity| {
                        entity.get("session_uuid").and_then(Value::as_str) == Some(&session_id.0)
                            && entity.get("lifecycle_class").and_then(Value::as_str)
                                == Some("current")
                    }),
                    "first snapshot must contain the live session"
                );
            }
            other => panic!("expected populated snapshot, got {other:?}"),
        }

        daemon
            .runtime()
            .expect("runtime initialized")
            .mark_session_stale_for_test(&session_id, 2)
            .expect("mark live session stale through core daemon");
        for _ in 0..16 {
            drive_all_maintenance(&mut daemon, &mut state);
        }
        assert!(matches!(
            receiver.recv().expect("stale transition patch"),
            DaemonEntityFrame::Patch {
                ref id,
                ref patch,
                ..
            } if id == &session_id.0
                && patch == &serde_json::json!({
                    "registry_state": "stale",
                    "lifecycle_class": "indeterminate",
                    "updated_at": 2
                })
        ));

        daemon
            .runtime_mut()
            .expect("runtime initialized")
            .shutdown_session_for_test(session_id)
            .expect("stop worker-backed test session");
        daemon.stop();
        let _ = fs::remove_dir_all(data_directory);
    }

    #[test]
    fn existing_session_subscriber_receives_spawn_upsert_without_another_request() {
        let data_directory = std::env::temp_dir().join(format!(
            "botster-hub-existing-sub-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "existing-sub-test".to_string(),
                display_name: "Existing Subscriber Test".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            transports: crate::TransportBindings::default(),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build existing subscriber config");
        let mut daemon = HubDaemon::start(config).expect("start existing subscriber daemon");
        let mut state = DaemonControlState::default();
        seed_lifecycle_reconciliation(&mut daemon, &mut state);
        for _ in 0..16 {
            drive_all_maintenance(&mut daemon, &mut state);
        }
        let (sender, receiver) = mpsc::sync_channel(8);
        let response = register_builtin_entity_subscription(
            &mut daemon,
            &mut state,
            "session".to_string(),
            "existing-sub".to_string(),
            EntityFrameSender::Blocking(sender),
            None,
        )
        .expect("register idle session subscription");
        assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
        let mut first = receiver.try_recv().ok();
        for _ in 0..32 {
            if first.is_some() {
                break;
            }
            drive_all_maintenance(&mut daemon, &mut state);
            first = receiver.try_recv().ok();
        }
        match first.expect("first snapshot before spawn") {
            DaemonEntityFrame::Snapshot { .. } => {}
            other => panic!("expected first snapshot, got {other:?}"),
        }
        let session_id = SessionId("assemble-ready-spawn".to_string());
        daemon
            .runtime_mut()
            .expect("runtime initialized")
            .spawn_session_for_test(
                botster_core::SessionSpawnRequest {
                    request_id: RequestId("existing-sub-spawn".to_string()),
                    session_id: session_id.clone(),
                    executable: "/bin/sleep".to_string(),
                    arguments: vec!["8".to_string()],
                    working_directory: botster_core::SpawnWorkingDirectory {
                        path: ".".to_string(),
                    },
                    environment: botster_core::SpawnEnvironment::default(),
                    initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                },
                botster_core::CoreSessionMetadata::new(),
            )
            .expect("spawn after first snapshot");
        state.maintenance.note_authoritative_mutation();
        let mut saw_ready = false;
        for _ in 0..16 {
            drive_all_maintenance(&mut daemon, &mut state);
            while let Ok(frame) = receiver.try_recv() {
                match frame {
                    DaemonEntityFrame::Upsert { id, .. } | DaemonEntityFrame::Patch { id, .. }
                        if id == session_id.0 =>
                    {
                        saw_ready = true;
                    }
                    DaemonEntityFrame::Snapshot { items, .. }
                        if items.iter().any(|entity| {
                            entity.get("session_uuid").and_then(Value::as_str)
                                == Some(session_id.0.as_str())
                        }) =>
                    {
                        saw_ready = true;
                    }
                    DaemonEntityFrame::Error { code, message, .. } => {
                        panic!("existing subscriber error: {code}: {message}");
                    }
                    _ => {}
                }
            }
            if saw_ready {
                break;
            }
        }
        assert!(
            saw_ready,
            "existing subscriber must receive assemble-ready-spawn without another client request"
        );
        let _ = daemon
            .runtime_mut()
            .expect("runtime initialized")
            .shutdown_session_for_test(session_id);
        daemon.stop();
        let _ = fs::remove_dir_all(data_directory);
    }

    #[test]
    fn entity_overflow_requires_empty_snapshot_resync_and_failed_delivery_disconnects() {
        let fixture =
            botster_hub_test_support::session_lifecycle_subscription_conformance_scenario();
        let overflow_reason = fixture.overflow.resync_reason.clone();
        assert!(fixture.overflow.empty_snapshot_valid);
        assert!(fixture.overflow.snapshot_precedes_later_deltas);
        assert!(
            fixture
                .overflow
                .failed_snapshot_delivery_closes_subscription
        );
        let cursor = SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
            sequence: 9,
        };
        let baseline = || SessionLifecycleBaseline {
            cursor: cursor.clone(),
            sessions: Vec::new(),
        };
        let (sender, receiver) = mpsc::sync_channel(1);
        sender
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: "subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 8,
                items: Vec::new(),
                resync_reason: None,
            })
            .expect("fill bounded subscriber queue");
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: Some(SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
                sequence: 8,
            }),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: Some(overflow_reason.clone()),
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        };
        let mut counters = DaemonLifecycleCounters::default();

        assert!(try_resync_subscription(
            "subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert_eq!(
            state.resync_reason.as_deref(),
            Some(overflow_reason.as_str())
        );
        let _ = receiver.recv().expect("drain stale queued frame");
        assert!(try_resync_subscription(
            "subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert!(state.resync_reason.is_none());
        assert!(matches!(
            receiver.recv().expect("receive empty resync snapshot"),
            DaemonEntityFrame::Snapshot {
                snapshot_seq: 9,
                ref items,
                resync_reason: Some(ref reason),
                ..
            } if items.is_empty() && reason == &overflow_reason
        ));

        drop(receiver);
        state.resync_reason = Some(overflow_reason.clone());
        assert!(!try_resync_subscription(
            "subscription",
            &mut state,
            baseline(),
            overflow_reason,
            &mut counters,
        ));
        assert_eq!(counters.entity_delivery_attempts, 3);
        assert_eq!(counters.entity_delivery_successes, 1);
        assert_eq!(counters.entity_delivery_overflows, 1);
        assert_eq!(counters.entity_delivery_failures, 1);
    }

    #[test]
    fn session_type_resync_replaces_oversized_snapshot_with_typed_error() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut subscriptions = BTreeMap::from([(
            "oversized-session-types".to_string(),
            EntitySubscriptionState {
                sender: EntityFrameSender::Blocking(sender),
                entity_type: "session_type".to_string(),
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: 1,
                definition_entities: BTreeMap::new(),
                awaiting_initial_snapshot: false,
                resync_reason: Some("subscriber_overflow".to_string()),
                terminating: false,
                owner_grant_id: None,
                package_last_applied_seq: None,
                package_catching_up: false,
                package_delivery: None,
                delivery_after: None,
                delivery_phase: DeliveryPhase::Removes,
                next_seq: 0,
                assembled_items: Vec::new(),
                assembled_item_bytes: 0,
                needs_delivery: false,
            },
        )]);
        let entities = BTreeMap::from([(
            "device/oversized".to_string(),
            serde_json::json!({ "description": "x".repeat(DAEMON_MAX_FRAME_BYTES) }),
        )]);

        drive_session_type_subscriptions(&mut subscriptions, 2, &entities);

        assert!(
            subscriptions.is_empty(),
            "typed error closes the subscription"
        );
        assert!(matches!(
            receiver.recv().expect("receive bounded typed error"),
            DaemonEntityFrame::Error {
                ref subscription_id,
                ref entity_type,
                ref code,
                ..
            } if subscription_id == "oversized-session-types"
                && entity_type == "session_type"
                && code == "entity_provider_frame_too_large"
        ));
    }

    fn session_type_subscription_state(
        sender: mpsc::SyncSender<DaemonEntityFrame>,
        definition_generation: u64,
        next_seq: u64,
        definition_entities: BTreeMap<String, Value>,
        resync_reason: Option<String>,
    ) -> EntitySubscriptionState {
        EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session_type".to_string(),
            cursor: None,
            entities: BTreeMap::new(),
            definition_generation,
            awaiting_initial_snapshot: false,
            definition_entities,
            resync_reason,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        }
    }

    fn session_type_delta_seqs(
        receiver: &mpsc::Receiver<DaemonEntityFrame>,
    ) -> Vec<(String, u64, String)> {
        receiver
            .try_iter()
            .filter_map(|frame| match frame {
                DaemonEntityFrame::Upsert {
                    subscription_id,
                    snapshot_seq,
                    id,
                    ..
                }
                | DaemonEntityFrame::Remove {
                    subscription_id,
                    snapshot_seq,
                    id,
                    ..
                } => Some((subscription_id, snapshot_seq, id)),
                DaemonEntityFrame::Snapshot {
                    subscription_id,
                    snapshot_seq,
                    ..
                } => Some((subscription_id, snapshot_seq, "snapshot".to_string())),
                DaemonEntityFrame::Error { .. } | DaemonEntityFrame::Patch { .. } => None,
            })
            .collect()
    }

    #[test]
    fn session_type_same_generation_multi_row_uses_contiguous_subscriber_seq() {
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut subscriptions = BTreeMap::from([(
            "held-session-types".to_string(),
            session_type_subscription_state(
                sender,
                1,
                1,
                BTreeMap::from([
                    (
                        "device/alpha".to_string(),
                        serde_json::json!({ "label": "Alpha" }),
                    ),
                    (
                        "device/beta".to_string(),
                        serde_json::json!({ "label": "Beta" }),
                    ),
                ]),
                None,
            ),
        )]);
        let entities = BTreeMap::from([
            (
                "device/alpha".to_string(),
                serde_json::json!({ "label": "Alpha 2" }),
            ),
            (
                "device/beta".to_string(),
                serde_json::json!({ "label": "Beta 2" }),
            ),
        ]);

        drive_session_type_subscriptions(&mut subscriptions, 2, &entities);

        let frames = session_type_delta_seqs(&receiver);
        assert_eq!(
            frames,
            vec![
                (
                    "held-session-types".to_string(),
                    2,
                    "device/alpha".to_string()
                ),
                (
                    "held-session-types".to_string(),
                    3,
                    "device/beta".to_string()
                ),
            ],
            "one generation with two published diffs must deliver N+1 then N+2 on the held subscription"
        );
        assert_eq!(
            subscriptions
                .get("held-session-types")
                .map(|subscription| subscription.next_seq),
            Some(3)
        );
    }

    #[test]
    fn session_type_skipped_generation_uses_contiguous_subscriber_seq() {
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut subscriptions = BTreeMap::from([(
            "held-session-types".to_string(),
            session_type_subscription_state(
                sender,
                1,
                1,
                BTreeMap::from([(
                    "device/alpha".to_string(),
                    serde_json::json!({ "label": "Alpha" }),
                )]),
                None,
            ),
        )]);
        let entities = BTreeMap::from([(
            "device/alpha".to_string(),
            serde_json::json!({ "label": "Alpha 3" }),
        )]);

        drive_session_type_subscriptions(&mut subscriptions, 3, &entities);

        let frames = session_type_delta_seqs(&receiver);
        assert_eq!(
            frames,
            vec![(
                "held-session-types".to_string(),
                2,
                "device/alpha".to_string()
            )],
            "a skipped dirty generation must still deliver the next contiguous subscriber seq, not the generation number"
        );
        assert_eq!(
            subscriptions
                .get("held-session-types")
                .map(|subscription| subscription.next_seq),
            Some(2)
        );
    }

    #[test]
    fn session_type_overflow_resync_advances_subscriber_seq_not_generation() {
        let (sender, receiver) = mpsc::sync_channel(2);
        let mut subscriptions = BTreeMap::from([(
            "held-session-types".to_string(),
            session_type_subscription_state(
                sender,
                7,
                7,
                BTreeMap::from([(
                    "device/alpha".to_string(),
                    serde_json::json!({ "label": "Alpha" }),
                )]),
                Some("subscriber_overflow".to_string()),
            ),
        )]);
        let entities = BTreeMap::from([(
            "device/alpha".to_string(),
            serde_json::json!({ "label": "Alpha recovered" }),
        )]);

        drive_session_type_subscriptions(&mut subscriptions, 3, &entities);

        let frames = session_type_delta_seqs(&receiver);
        assert_eq!(
            frames,
            vec![("held-session-types".to_string(), 8, "snapshot".to_string())],
            "overflow resync must send next_seq+1 and must not move snapshot_seq backwards to the generation"
        );
        let subscription = subscriptions
            .get("held-session-types")
            .expect("held subscription remains open");
        assert_eq!(subscription.next_seq, 8);
        assert_eq!(subscription.definition_generation, 3);
        assert!(subscription.resync_reason.is_none());
    }

    #[test]
    fn async_entity_overflow_requires_empty_snapshot_resync_and_closed_delivery_disconnects() {
        let overflow_reason = "subscriber_overflow".to_string();
        let cursor = SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
            sequence: 9,
        };
        let baseline = || SessionLifecycleBaseline {
            cursor: cursor.clone(),
            sessions: Vec::new(),
        };
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(
                DaemonEntityFrame::Snapshot {
                    subscription_id: "async-subscription".to_string(),
                    entity_type: "session".to_string(),
                    snapshot_seq: 8,
                    items: Vec::new(),
                    resync_reason: None,
                }
                .into(),
            )
            .expect("fill bounded async subscriber queue");
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Async(sender),
            entity_type: "session".to_string(),
            cursor: Some(SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
                sequence: 8,
            }),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: Some(overflow_reason.clone()),
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        };
        let mut counters = DaemonLifecycleCounters::default();

        assert!(try_resync_subscription(
            "async-subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert_eq!(
            state.resync_reason.as_deref(),
            Some(overflow_reason.as_str()),
            "a full production WebRTC queue must retain its pending resync"
        );
        let _ = receiver.try_recv().expect("drain stale async frame");
        assert!(try_resync_subscription(
            "async-subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert!(state.resync_reason.is_none());
        assert!(matches!(
            receiver.try_recv().expect("receive async resync snapshot"),
            crate::entity_delivery::EntityDelivery::Typed(DaemonEntityFrame::Snapshot {
                snapshot_seq: 9,
                ref items,
                resync_reason: Some(ref reason),
                ..
            }) if items.is_empty() && reason == &overflow_reason
        ));

        drop(receiver);
        state.resync_reason = Some(overflow_reason.clone());
        assert!(!try_resync_subscription(
            "async-subscription",
            &mut state,
            baseline(),
            overflow_reason,
            &mut counters,
        ));
        assert_eq!(counters.entity_delivery_attempts, 3);
        assert_eq!(counters.entity_delivery_successes, 1);
        assert_eq!(counters.entity_delivery_overflows, 1);
        assert_eq!(counters.entity_delivery_failures, 1);
    }

    #[test]
    fn delivery_page_does_not_skip_a_low_id_after_a_high_remove() {
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.replace_complete_baseline(
            SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                sequence: 2,
            },
            Vec::new(),
        );
        let record = |id: &str| botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        projection.ingest_baseline_rows(2, [record("a")]);
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::from([(
                "z".to_string(),
                crate::session_projection::SessionProjection::project_entity(&record("z")),
            )]),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let (alive, first) = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        assert!(alive);
        assert!(first.more);
        let (alive, second) = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        assert!(alive);
        assert!(!second.more);
        let frames: Vec<_> = receiver.try_iter().collect();
        assert!(frames.iter().any(|frame| matches!(
            frame,
            DaemonEntityFrame::Remove { id, .. } if id == "z"
        )));
        assert!(frames.iter().any(|frame| matches!(
            frame,
            DaemonEntityFrame::Upsert { id, .. } if id == "a"
        )));
    }

    #[test]
    fn delivery_page_keeps_snapshot_seq_monotonic_when_id_order_reverses_journal_order() {
        let record = |id: &str| botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(1, [record("z")]);
        projection.ingest_baseline_rows(2, [record("a")]);
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 2,
        });
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Rows,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let _ = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        let _ = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        let seqs: Vec<u64> = receiver
            .try_iter()
            .filter_map(|frame| match frame {
                DaemonEntityFrame::Upsert { snapshot_seq, .. }
                | DaemonEntityFrame::Patch { snapshot_seq, .. } => Some(snapshot_seq),
                _ => None,
            })
            .collect();
        assert_eq!(seqs, vec![1, 2]);
    }

    #[test]
    fn overflow_resync_does_not_move_snapshot_seq_backwards() {
        let record = |id: &str| botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(11, [record("a"), record("b"), record("c")]);
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 11,
        });
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Rows,
            next_seq: 110,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let _ = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        state.resync_reason = Some("subscriber_overflow".to_string());
        assert!(try_resync_from_projection(
            "sub",
            &mut state,
            &projection,
            true,
            "subscriber_overflow".to_string(),
            &mut counters,
        ));
        let seqs: Vec<u64> = receiver
            .try_iter()
            .filter_map(|frame| match frame {
                DaemonEntityFrame::Upsert { snapshot_seq, .. }
                | DaemonEntityFrame::Patch { snapshot_seq, .. }
                | DaemonEntityFrame::Snapshot { snapshot_seq, .. }
                | DaemonEntityFrame::Remove { snapshot_seq, .. } => Some(snapshot_seq),
                DaemonEntityFrame::Error { .. } => None,
            })
            .collect();
        assert!(seqs.len() >= 2);
        for window in seqs.windows(2) {
            assert!(window[0] < window[1], "sequences moved backwards: {seqs:?}");
        }
        assert!(seqs[0] > 110 || seqs.iter().any(|seq| *seq > 110));
    }

    #[test]
    fn paged_delivery_stays_within_owner_turn_for_a_large_registry() {
        let record = |id: String| botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..256).map(|index| record(format!("session-{index:03}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let mut delivered = 0;
        for subscriber in 0..2 {
            let (sender, receiver) = mpsc::sync_channel(256);
            let mut state = EntitySubscriptionState {
                sender: EntityFrameSender::Blocking(sender),
                entity_type: "session".to_string(),
                cursor: projection.cursor.clone(),
                entities: BTreeMap::new(),
                definition_generation: 0,
                definition_entities: BTreeMap::new(),
                awaiting_initial_snapshot: false,
                resync_reason: None,
                terminating: false,
                owner_grant_id: None,
                package_last_applied_seq: None,
                package_catching_up: false,
                package_delivery: None,
                delivery_after: None,
                delivery_phase: DeliveryPhase::Rows,
                next_seq: 0,
                assembled_items: Vec::new(),
                assembled_item_bytes: 0,
                needs_delivery: false,
            };
            let mut counters = DaemonLifecycleCounters::default();
            loop {
                let (alive, page) = deliver_projection_delta_page(
                    &format!("sub-{subscriber}"),
                    &mut state,
                    &projection,
                    &mut counters,
                    SESSION_DELIVERY_MAX_ITEMS,
                    SESSION_DELIVERY_MAX_BYTES,
                    SESSION_DELIVERY_MAX_ELAPSED,
                );
                assert!(alive);
                assert!(page.items <= SESSION_DELIVERY_MAX_ITEMS);
                assert!(page.bytes <= SESSION_DELIVERY_MAX_BYTES);
                if !page.more {
                    break;
                }
            }
            delivered += receiver.try_iter().count();
        }
        assert_eq!(delivered, 512);
    }

    #[test]
    fn first_session_snapshot_is_complete_and_assembled_in_pages() {
        let record = |id: String| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..24).map(|index| record(format!("session-{index:02}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(32);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let mut pages = 0;
        let envelope = snapshot_envelope_bytes("sub", &state);
        let mut charged_item_bytes = 0usize;
        loop {
            let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                SESSION_DELIVERY_MAX_BYTES,
                Duration::MAX,
            ) else {
                panic!("assembly must stay alive");
            };
            assert!(page.items <= SESSION_DELIVERY_MAX_ITEMS);
            assert!(page.bytes <= SESSION_DELIVERY_MAX_BYTES);
            charged_item_bytes =
                charged_item_bytes.saturating_add(page.bytes.saturating_sub(envelope));
            pages += 1;
            if !page.more {
                break;
            }
            assert!(
                matches!(state.delivery_phase, DeliveryPhase::Assembling { .. }),
                "must keep assembling until the complete snapshot"
            );
            assert!(receiver.try_iter().next().is_none());
            assert!(pages < 8);
        }
        assert!(pages > 1);
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            DaemonEntityFrame::Snapshot {
                items,
                resync_reason,
                ..
            } => {
                assert_eq!(items.len(), 24);
                assert_eq!(resync_reason, &None);
            }
            other => panic!("expected one complete snapshot, got {other:?}"),
        }
        let encoded = serde_json::to_vec(&frames[0])
            .expect("encode sent frame")
            .len();
        assert_eq!(charged_item_bytes.saturating_add(envelope), encoded);
        assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
        assert!(!state.needs_delivery);
    }

    #[test]
    fn catch_up_restarts_when_a_prefix_id_changes() {
        let record = |id: &str, updated_at: u64| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..24).map(|index| record(&format!("session-{index:02}"), 1)),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("first page");
        };
        assert!(page.more);
        assert!(receiver.try_iter().next().is_none());
        projection.ingest_baseline_rows(2, [record("session-00", 9)]);
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 2,
        });
        loop {
            let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                SESSION_DELIVERY_MAX_BYTES,
                Duration::MAX,
            ) else {
                panic!("restarted assembly");
            };
            if !page.more {
                break;
            }
        }
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            DaemonEntityFrame::Snapshot { items, .. } => {
                let first = items
                    .iter()
                    .find(|item| {
                        item.get("session_uuid").and_then(Value::as_str) == Some("session-00")
                    })
                    .expect("prefix row");
                assert_eq!(first.get("updated_at").and_then(Value::as_u64), Some(9));
                assert_eq!(items.len(), 24);
            }
            other => panic!("expected complete snapshot, got {other:?}"),
        }
    }

    #[test]
    fn oversized_first_snapshot_closes_the_subscription() {
        let huge = SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId("x".repeat(DAEMON_MAX_FRAME_BYTES)),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(1, [huge]);
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let first = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        );
        assert!(matches!(
            first,
            SnapshotAssemble::Closed {
                frame_too_large: true
            }
        ));
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        assert!(matches!(
            &frames[0],
            DaemonEntityFrame::Error { code, .. } if code == "entity_provider_frame_too_large"
        ));
        assert!(!state.needs_delivery);
        assert!(state.resync_reason.is_none());
    }

    #[test]
    fn no_removal_scan_stays_within_owner_turn() {
        let record = |id: String| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..256).map(|index| record(format!("session-{index:03}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, _receiver) = mpsc::sync_channel(16);
        let mut entities = BTreeMap::new();
        for (id, row) in &projection.rows {
            entities.insert(
                id.clone(),
                crate::session_projection::SessionProjection::project_entity(&row.record),
            );
        }
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities,
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let (alive, page) = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_ELAPSED,
        );
        assert!(alive);
        assert!(page.items <= SESSION_DELIVERY_MAX_ITEMS);
        assert!(page.bytes <= SESSION_DELIVERY_MAX_BYTES);
        assert!(page.more);
        assert!(state.delivery_after.is_some());
    }

    #[test]
    fn near_limit_snapshot_assembly_stays_within_owner_turn() {
        let record = |id: String| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..20).map(|index| record(format!("s{index:02}-{}", "x".repeat(40 * 1024)))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(2);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        // This case proves bounded per-call assembly work, not wall-clock latency.
        // Duration::MAX cannot cut a page, so page cuts are byte-driven: one item per page.
        const MAX_NEAR_LIMIT_PAGES: usize = 21;
        for page_index in 0..MAX_NEAR_LIMIT_PAGES {
            let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                SESSION_DELIVERY_MAX_BYTES,
                Duration::MAX,
            ) else {
                panic!("near-limit assembly");
            };
            assert!(page.items >= 1);
            assert!(page.items <= SESSION_DELIVERY_MAX_ITEMS);
            assert!(page.bytes <= SESSION_DELIVERY_MAX_BYTES);
            if page.more {
                assert_eq!(receiver.try_iter().count(), 0);
                assert!(!state.assembled_items.is_empty());
                assert!(
                    page_index + 1 < MAX_NEAR_LIMIT_PAGES,
                    "twenty one-item pages cannot need more than twenty useful calls"
                );
            } else {
                break;
            }
        }
        assert!(state.assembled_items.is_empty());
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            DaemonEntityFrame::Snapshot { items, .. } => {
                assert_eq!(items.len(), 20);
                let encoded = serde_json::to_vec(frames.first().expect("frame"))
                    .expect("encode")
                    .len();
                assert!(encoded <= DAEMON_MAX_FRAME_BYTES);
            }
            other => panic!("expected complete snapshot, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_size_charges_json_array_separators() {
        assert_eq!(snapshot_separator_bytes(0, 0), 0);
        assert_eq!(snapshot_separator_bytes(0, 1), 0);
        assert_eq!(snapshot_separator_bytes(0, 3), 2);
        assert_eq!(snapshot_separator_bytes(4, 1), 1);
        assert_eq!(snapshot_separator_bytes(4, 2), 2);
    }

    #[test]
    fn separators_close_when_item_bytes_fit_but_commas_do_not() {
        let record = |id: String| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let probe = serde_json::to_value(
            crate::session_projection::SessionProjection::project_entity(&record("sep-00".into())),
        )
        .expect("probe");
        let probe_len = serde_json::to_vec(&probe).expect("encode probe").len();
        let envelope = {
            let (sender, _receiver) = mpsc::sync_channel(1);
            snapshot_envelope_bytes(
                "sub",
                &EntitySubscriptionState {
                    sender: EntityFrameSender::Blocking(sender),
                    entity_type: "session".to_string(),
                    cursor: None,
                    entities: BTreeMap::new(),
                    definition_generation: 0,
                    definition_entities: BTreeMap::new(),
                    awaiting_initial_snapshot: false,
                    resync_reason: None,
                    terminating: false,
                    owner_grant_id: None,
                    package_last_applied_seq: None,
                    package_catching_up: false,
                    package_delivery: None,
                    delivery_after: None,
                    delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
                    next_seq: 1,
                    assembled_items: Vec::new(),
                    assembled_item_bytes: 0,
                    needs_delivery: true,
                },
            )
        };
        let mut pad = DAEMON_MAX_FRAME_BYTES
            .saturating_sub(envelope)
            .saturating_div(2)
            .saturating_sub(probe_len)
            .saturating_add(64);
        let projection = loop {
            let mut projection = crate::session_projection::SessionProjection::default();
            projection.ingest_baseline_rows(
                1,
                [
                    record(format!("sep-00-{}", "y".repeat(pad))),
                    record(format!("sep-01-{}", "y".repeat(pad))),
                ],
            );
            projection.seal_baseline(SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                sequence: 1,
            });
            let encoded_items: Vec<_> = projection
                .rows
                .values()
                .map(|row| {
                    serde_json::to_value(
                        crate::session_projection::SessionProjection::project_entity(&row.record),
                    )
                    .expect("item")
                })
                .collect();
            let item_only = encoded_item_bytes(&encoded_items);
            let without_commas = item_only.saturating_add(envelope);
            let with_commas =
                without_commas.saturating_add(snapshot_separator_bytes(0, encoded_items.len()));
            if without_commas <= DAEMON_MAX_FRAME_BYTES && with_commas > DAEMON_MAX_FRAME_BYTES {
                break projection;
            }
            if without_commas > DAEMON_MAX_FRAME_BYTES {
                pad = pad.saturating_sub(8);
            } else {
                pad = pad.saturating_add(1);
            }
            assert!(pad > 32, "failed to find separator boundary pad");
        };
        let (sender, receiver) = mpsc::sync_channel(2);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        // This case proves separator accounting, not owner-turn latency.
        // Duration::MAX cannot cut a page, so Closed cannot be elapsed-empty.
        const MAX_SEPARATOR_PAGES: usize = 3;
        let mut closed_too_large = false;
        for page_index in 0..MAX_SEPARATOR_PAGES {
            match continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                8,
                DAEMON_MAX_FRAME_BYTES,
                DAEMON_MAX_FRAME_BYTES,
                Duration::MAX,
            ) {
                SnapshotAssemble::Closed {
                    frame_too_large: true,
                } => {
                    closed_too_large = true;
                    break;
                }
                SnapshotAssemble::Closed {
                    frame_too_large: false,
                } => panic!("closed without frame_too_large"),
                SnapshotAssemble::Continue { page } => {
                    assert!(page.items > 0, "empty-item continue is not separator proof");
                    assert!(page.more, "completed snapshot without charging separators");
                    assert!(
                        page_index + 1 < MAX_SEPARATOR_PAGES,
                        "two items cannot need more than two useful pages"
                    );
                }
            }
        }
        assert!(closed_too_large, "separator close did not fire");
        let frames: Vec<_> = receiver.try_iter().collect();
        assert!(matches!(
            frames.first(),
            Some(DaemonEntityFrame::Error { code, .. })
                if code == "entity_provider_frame_too_large"
        ));
    }

    fn assemble_record(id: String) -> SessionLifecycleRecord {
        SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        }
    }

    fn assembling_subscription(
        sender: mpsc::SyncSender<DaemonEntityFrame>,
        source_seq: u64,
    ) -> EntitySubscriptionState {
        EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: None,
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq },
            next_seq: source_seq,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        }
    }

    fn snapshot_item_ids(frame: &DaemonEntityFrame) -> Vec<String> {
        match frame {
            DaemonEntityFrame::Snapshot { items, .. } => items
                .iter()
                .filter_map(|item| {
                    item.get("session_uuid")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .collect(),
            other => panic!("expected snapshot, got {other:?}"),
        }
    }

    #[test]
    fn first_session_snapshot_holds_until_the_projection_is_caught_up() {
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..24).map(|index| assemble_record(format!("session-{index:02}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        let mut counters = DaemonLifecycleCounters::default();
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            false,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_ELAPSED,
        ) else {
            panic!("hold must stay alive");
        };
        assert_eq!(page.items, 0);
        assert!(page.more);
        assert!(state.needs_delivery);
        assert!(matches!(
            state.delivery_phase,
            DeliveryPhase::Assembling { .. }
        ));
        assert!(receiver.try_iter().next().is_none());
    }

    #[test]
    fn first_session_snapshot_completes_when_caught_up() {
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..24).map(|index| assemble_record(format!("session-{index:02}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = assembling_subscription(sender, 1);
        let mut counters = DaemonLifecycleCounters::default();
        loop {
            let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                SESSION_DELIVERY_MAX_BYTES,
                Duration::MAX,
            ) else {
                panic!("caught-up assembly must stay alive");
            };
            if !page.more {
                break;
            }
        }
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        assert_eq!(snapshot_item_ids(&frames[0]).len(), 24);
        assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
        assert!(!state.needs_delivery);
    }

    fn sealed_projection(
        records: impl IntoIterator<Item = SessionLifecycleRecord>,
    ) -> crate::session_projection::SessionProjection {
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(1, records);
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        projection
    }

    fn encoded_session_item(record: &SessionLifecycleRecord) -> usize {
        let value = serde_json::to_value(
            crate::session_projection::SessionProjection::project_entity(record),
        )
        .expect("item");
        serde_json::to_vec(&value).expect("encode").len()
    }

    fn stub_snapshot_envelope_bytes() -> usize {
        let empty = DaemonEntityFrame::Snapshot {
            subscription_id: String::new(),
            entity_type: "session".to_string(),
            snapshot_seq: 0,
            items: Vec::new(),
            resync_reason: None,
        };
        serde_json::to_vec(&empty).expect("stub envelope").len()
    }

    fn pad_record_to_item_len(prefix: &str, target_len: usize) -> SessionLifecycleRecord {
        let mut pad = 1usize;
        for _ in 0..target_len.saturating_add(32) {
            let record = assemble_record(format!("{prefix}-{}", "x".repeat(pad)));
            let len = encoded_session_item(&record);
            if len == target_len {
                return record;
            }
            if len < target_len {
                pad = pad.saturating_add(target_len - len);
            } else if pad > 1 {
                pad -= 1;
            } else {
                panic!("cannot hit item len {target_len}, got {len}");
            }
        }
        panic!("failed to find pad for item len {target_len}");
    }

    #[test]
    fn snapshot_item_budget_cuts_and_resumes() {
        let projection = sealed_projection([
            assemble_record("session-00".into()),
            assemble_record("session-01".into()),
        ]);
        let (sender, _receiver) = mpsc::sync_channel(4);
        let state = assembling_subscription(sender, 1);
        let envelope = snapshot_envelope_bytes("sub", &state);
        let first = take_snapshot_item_page(
            &projection,
            None,
            0,
            envelope,
            1,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        );
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.cut, SnapshotPageCut::ItemBudget);
        let second = take_snapshot_item_page(
            &projection,
            first.last_id.as_deref(),
            first.items.len(),
            envelope,
            1,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        );
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.cut, SnapshotPageCut::Complete);
        assert_ne!(
            first.items[0].get("session_uuid"),
            second.items[0].get("session_uuid")
        );
    }

    #[test]
    fn snapshot_byte_budget_includes_envelope_and_yields_empty_cuts() {
        let first_record = assemble_record("session-00".into());
        let second_record = assemble_record("session-01".into());
        let c1 = encoded_session_item(&first_record);
        let projection = sealed_projection([first_record, second_record]);
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        let envelope = snapshot_envelope_bytes("sub", &state);
        let mut counters = DaemonLifecycleCounters::default();

        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            envelope.saturating_add(c1),
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("exact envelope+c1 must yield, not close");
        };
        assert_eq!(page.items, 1);
        assert!(page.more);
        assert_eq!(page.bytes, envelope.saturating_add(c1));
        assert!(receiver.try_iter().next().is_none());

        let mut empty_state = assembling_subscription(mpsc::sync_channel(4).0, 1);
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut empty_state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            envelope.saturating_add(c1).saturating_sub(1),
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("envelope+c1-1 must yield empty, not close");
        };
        assert_eq!(page.items, 0);
        assert!(page.more);
        assert!(empty_state.needs_delivery);
        assert!(matches!(
            empty_state.delivery_phase,
            DeliveryPhase::Assembling { .. }
        ));

        let mut no_envelope_state = assembling_subscription(mpsc::sync_channel(4).0, 1);
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut no_envelope_state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            c1,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("c1 without envelope headroom must yield empty, not close");
        };
        assert_eq!(page.items, 0);
        assert!(page.more);
        assert!(no_envelope_state.needs_delivery);
    }

    #[test]
    fn snapshot_page_charges_the_real_envelope_not_a_stub() {
        let first_record = assemble_record("session-00".into());
        let c1 = encoded_session_item(&first_record);
        let projection = sealed_projection([first_record, assemble_record("session-01".into())]);
        let (sender, _receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        state.next_seq = 7;
        state.resync_reason = Some("catch_up".into());
        let real_envelope = snapshot_envelope_bytes("sub", &state);
        let stub_envelope = stub_snapshot_envelope_bytes();
        assert!(real_envelope > stub_envelope);
        assert_eq!(real_envelope, snapshot_envelope_bytes("sub", &state));

        let stub_fit = take_snapshot_item_page(
            &projection,
            None,
            0,
            real_envelope,
            SESSION_DELIVERY_MAX_ITEMS,
            stub_envelope.saturating_add(c1),
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        );
        assert!(stub_fit.items.is_empty());
        assert_eq!(stub_fit.cut, SnapshotPageCut::ByteBudget);

        let real_fit = take_snapshot_item_page(
            &projection,
            None,
            0,
            real_envelope,
            SESSION_DELIVERY_MAX_ITEMS,
            real_envelope.saturating_add(c1),
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        );
        assert_eq!(real_fit.items.len(), 1);
        assert_eq!(real_fit.cut, SnapshotPageCut::ByteBudget);
        assert_eq!(real_fit.bytes, real_envelope.saturating_add(c1));
    }

    #[test]
    fn oversized_row_uses_the_fresh_page_capacity_parameter() {
        let record = pad_record_to_item_len("big", SESSION_DELIVERY_MAX_BYTES.saturating_sub(8));
        let item_len = encoded_session_item(&record);
        let projection = sealed_projection([record]);
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        let envelope = snapshot_envelope_bytes("sub", &state);
        assert!(envelope.saturating_add(item_len) > SESSION_DELIVERY_MAX_BYTES);
        let mut counters = DaemonLifecycleCounters::default();
        let closed = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        );
        assert!(matches!(
            closed,
            SnapshotAssemble::Closed {
                frame_too_large: true
            }
        ));
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        assert!(matches!(
            &frames[0],
            DaemonEntityFrame::Error { code, .. } if code == "entity_provider_frame_too_large"
        ));

        let (sender, receiver) = mpsc::sync_channel(4);
        let mut roomy = assembling_subscription(sender, 1);
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut roomy,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            DAEMON_MAX_FRAME_BYTES,
            DAEMON_MAX_FRAME_BYTES,
            Duration::MAX,
        ) else {
            panic!("larger capacity must accept the same row");
        };
        assert_eq!(page.items, 1);
        assert!(!page.more);
        assert_eq!(
            snapshot_item_ids(&receiver.try_iter().next().expect("snapshot")).len(),
            1
        );
    }

    #[test]
    fn later_page_separator_boundary_closes_oversized_without_livelock() {
        let first = assemble_record("session-00".into());
        let envelope = {
            let (sender, _receiver) = mpsc::sync_channel(1);
            snapshot_envelope_bytes("sub", &assembling_subscription(sender, 1))
        };
        let oversized = pad_record_to_item_len(
            "session-01",
            SESSION_DELIVERY_MAX_BYTES.saturating_sub(envelope),
        );
        assert_eq!(
            envelope.saturating_add(encoded_session_item(&oversized)),
            SESSION_DELIVERY_MAX_BYTES
        );
        let projection = sealed_projection([first.clone(), oversized]);
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        let mut counters = DaemonLifecycleCounters::default();
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            1,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("first item must assemble");
        };
        assert_eq!(page.items, 1);
        assert!(page.more);
        let closed = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        );
        assert!(matches!(
            closed,
            SnapshotAssemble::Closed {
                frame_too_large: true
            }
        ));
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        assert!(matches!(
            &frames[0],
            DaemonEntityFrame::Error { code, .. } if code == "entity_provider_frame_too_large"
        ));

        let admitted = pad_record_to_item_len(
            "session-01",
            SESSION_DELIVERY_MAX_BYTES
                .saturating_sub(envelope)
                .saturating_sub(1),
        );
        assert!(
            envelope
                .saturating_add(encoded_session_item(&admitted))
                .saturating_add(1)
                <= SESSION_DELIVERY_MAX_BYTES
        );
        let control = sealed_projection([first, admitted]);
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &control,
            true,
            &mut counters,
            1,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("control first item");
        };
        assert_eq!(page.items, 1);
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &control,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("control second item must fit with comma headroom");
        };
        assert_eq!(page.items, 1);
        assert!(!page.more);
        assert_eq!(
            snapshot_item_ids(&receiver.try_iter().next().expect("snapshot")).len(),
            2
        );
    }

    #[test]
    fn empty_elapsed_cut_yields_instead_of_closing() {
        let projection = sealed_projection([
            assemble_record("session-00".into()),
            assemble_record("session-01".into()),
        ]);
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        let mut counters = DaemonLifecycleCounters::default();
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::ZERO,
        ) else {
            panic!("elapsed empty cut must yield, not close");
        };
        assert_eq!(page.items, 0);
        assert!(page.more);
        assert!(state.needs_delivery);
        assert!(matches!(
            state.delivery_phase,
            DeliveryPhase::Assembling { .. }
        ));
        assert!(receiver.try_iter().next().is_none());

        loop {
            let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                SESSION_DELIVERY_MAX_BYTES,
                Duration::MAX,
            ) else {
                panic!("later full-budget call must complete");
            };
            if !page.more {
                break;
            }
        }
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        assert_eq!(snapshot_item_ids(&frames[0]).len(), 2);
        assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
        assert!(!state.needs_delivery);
    }

    #[test]
    fn empty_snapshot_yields_when_remaining_budget_is_below_envelope() {
        let projection = sealed_projection([]);
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        let envelope = snapshot_envelope_bytes("sub", &state);
        assert!(envelope > 1);
        let mut counters = DaemonLifecycleCounters::default();
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            envelope.saturating_sub(1),
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("remaining envelope-1 must yield, not send");
        };
        assert_eq!(page.items, 0);
        assert!(page.more);
        assert!(state.needs_delivery);
        assert!(matches!(
            state.delivery_phase,
            DeliveryPhase::Assembling { .. }
        ));
        assert!(receiver.try_iter().next().is_none());

        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("full-budget empty projection must complete");
        };
        assert_eq!(page.items, 0);
        assert!(!page.more);
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            DaemonEntityFrame::Snapshot { items, .. } => assert!(items.is_empty()),
            other => panic!("expected one empty snapshot, got {other:?}"),
        }
        assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
        assert!(!state.needs_delivery);
    }

    #[test]
    fn empty_oversized_envelope_closes_instead_of_yielding_forever() {
        let projection = sealed_projection([]);
        let subscription_id = "e".repeat(SESSION_DELIVERY_MAX_BYTES);
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        let envelope = snapshot_envelope_bytes(&subscription_id, &state);
        assert!(envelope > SESSION_DELIVERY_MAX_BYTES);
        let mut counters = DaemonLifecycleCounters::default();
        let closed = continue_session_snapshot_assembly(
            &subscription_id,
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        );
        assert!(matches!(
            closed,
            SnapshotAssemble::Closed {
                frame_too_large: true
            }
        ));
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        assert!(matches!(
            &frames[0],
            DaemonEntityFrame::Error { code, .. } if code == "entity_provider_frame_too_large"
        ));
        assert!(!state.needs_delivery);
        assert!(matches!(state.delivery_phase, DeliveryPhase::Removes));
    }

    #[test]
    fn exhausted_budget_preserves_catalog_capacity_release() {
        let directory = std::env::temp_dir().join(format!(
            "botster-catalog-capacity-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("the system clock follows the epoch")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "catalog-capacity-test".into(),
                display_name: "Catalog Capacity Test".into(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build the catalog capacity configuration");
        let mut daemon = HubDaemon::start(config).expect("start the catalog capacity daemon");
        let executor = daemon
            .runtime()
            .expect("the runtime is active")
            .host_executor();
        let mut permits = (0..crate::host_executor::HOST_OPERATION_CAPACITY)
            .map(|_| executor.try_reserve().expect("reserve a host operation"))
            .collect::<Vec<_>>();
        let mut state = DaemonControlState::default();
        let delivery = crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery;
        state.maintenance.wakes.take(delivery);
        assert!(matches!(
            state
                .session_type_catalog
                .refresh(&daemon, 1, &state.waiter_ids),
            SessionTypeCatalogRefresh::Pending
        ));
        assert!(state.session_type_catalog.waiting_for_capacity());
        drop(permits.pop());
        assert!(executor.take_capacity_notification());
        state.host_capacity_wake_pending = true;
        let now = Instant::now();
        let mut budget = OwnerTurnBudget::new(now);
        budget
            .try_charge(
                now,
                OwnerTurnCharge::inspection(
                    crate::daemon::owner_turn::OWNER_TURN_INSPECTED_BYTE_LIMIT,
                ),
            )
            .expect("consume the byte budget");
        publish_catalog_capacity_wake(&mut state, &mut budget);
        assert!(state.host_capacity_wake_pending);
        assert!(!state.maintenance.wakes.take(delivery));

        let mut fresh = OwnerTurnBudget::new(Instant::now());
        publish_catalog_capacity_wake(&mut state, &mut fresh);
        assert!(!state.host_capacity_wake_pending);
        assert!(state.maintenance.wakes.take(delivery));
        assert!(matches!(
            state
                .session_type_catalog
                .refresh(&daemon, 1, &state.waiter_ids),
            SessionTypeCatalogRefresh::Pending
        ));
        assert!(state.session_type_catalog.pending.is_some());
        assert!(!state.session_type_catalog.waiting_for_capacity());
        publish_catalog_capacity_wake(&mut state, &mut fresh);
        assert!(!state.maintenance.wakes.take(delivery));
        drop(permits);
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove the catalog capacity directory");
    }
}
