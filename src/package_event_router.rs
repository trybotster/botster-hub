//! Send-safe package event router.
//!
//! This module owns contracts, exact subscriptions, token buckets, occupancy,
//! and transient queues. It must not import HubRuntime, CoreDaemon, mlua, plugin
//! persistence, or the owner loop.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, TryLockError};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::config::PackageEventPlanePolicy;
use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::event_plane_counters::{
    AgeIdentity, EventPlaneCounters, ProducerAgeList, ProducerAgeRef, QueueAgeMetric,
};
use crate::package_event_schema::{CompiledEventSchema, worktree_lifecycle_schema};
use crate::subscription::package_events::ClientEventMailbox;
use botster_hub_client::DaemonQueueKind;

pub const HUB_EVENT_OWNER: &str = "hub";

const WORKTREE_EVENT_NAMES: &[&str] = &[
    "worktree_created",
    "worktree_create_failed",
    "worktree_deleted",
    "worktree_delete_failed",
];

/// Typed ingress, subscribe, and shed results shared with Lua.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventPlaneStatus {
    Accepted,
    RejectedUndeclared,
    RejectedForeign,
    RejectedInvalid,
    RejectedOversize,
    RejectedOverRate,
    RejectedOverFanout,
    RejectedWildcard,
    RejectedCausalScope,
    RejectedAudience,
    ShedFull,
    ShedBusy,
}

impl EventPlaneStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::RejectedUndeclared => "rejected_undeclared",
            Self::RejectedForeign => "rejected_foreign",
            Self::RejectedInvalid => "rejected_invalid",
            Self::RejectedOversize => "rejected_oversize",
            Self::RejectedOverRate => "rejected_over_rate",
            Self::RejectedOverFanout => "rejected_over_fanout",
            Self::RejectedWildcard => "rejected_wildcard",
            Self::RejectedCausalScope => "rejected_causal_scope",
            Self::RejectedAudience => "rejected_audience",
            Self::ShedFull => "shed_full",
            Self::ShedBusy => "shed_busy",
        }
    }

    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::Accepted => 0,
            Self::RejectedUndeclared => 1,
            Self::RejectedForeign => 2,
            Self::RejectedInvalid => 3,
            Self::RejectedOversize => 4,
            Self::RejectedOverRate => 5,
            Self::RejectedOverFanout => 6,
            Self::RejectedWildcard => 7,
            Self::RejectedCausalScope => 8,
            Self::RejectedAudience => 9,
            Self::ShedFull => 10,
            Self::ShedBusy => 11,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventAudience {
    Plugins,
    Clients,
}

impl EventAudience {
    #[allow(dead_code)]
    fn parse(value: &str) -> Result<Self, EventPlaneStatus> {
        match value {
            "plugins" => Ok(Self::Plugins),
            "clients" => Ok(Self::Clients),
            _ => Err(EventPlaneStatus::RejectedInvalid),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventKey {
    pub owner: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct EmittedContract {
    pub owner: String,
    pub name: String,
    pub audience: BTreeSet<EventAudience>,
    pub schema: CompiledEventSchema,
    pub package_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerOpKind {
    Unload,
    Reload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerOp {
    pub kind: OwnerOpKind,
    pub owner: String,
    pub generation: u64,
}

#[derive(Debug)]
pub enum OwnerApplyResult {
    Applied,
    Work(EventOwnerWork),
}

static NEXT_EVENT_OWNER_WORK_ID: AtomicU64 = AtomicU64::new(1);

/// An exact dispatch identity. A retry always receives a new serial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOwnerWorkId {
    serial: u64,
    operation: OwnerOp,
}

impl EventOwnerWorkId {
    #[must_use]
    pub fn operation(&self) -> &OwnerOp {
        &self.operation
    }
}

/// Worker ownership of one unload. The caller retains Host admission until completion.
#[derive(Debug)]
#[must_use]
pub struct EventOwnerWork {
    identity: EventOwnerWorkId,
    metadata_applied: bool,
    retired_payloads: Vec<RetiringPayload>,
}

/// The worker completed payload destruction and retired the retained byte charges.
#[derive(Debug)]
pub struct EventOwnerCompletion {
    identity: EventOwnerWorkId,
}

impl EventOwnerCompletion {
    #[must_use]
    pub fn identity(&self) -> &EventOwnerWorkId {
        &self.identity
    }
}

/// Poison is terminal. The caller must retain the work without a retry loop.
#[derive(Debug)]
pub enum EventOwnerWorkError {
    RouterPoisoned(EventOwnerWork),
}

/// A replacement error preserves its commit result and any terminal cleanup work.
#[derive(Debug)]
pub struct EventPlaneReplaceError {
    result: Result<u64, EventPlaneStatus>,
    cleanup: Option<EventOwnerWorkError>,
}

impl EventPlaneReplaceError {
    #[must_use]
    pub fn into_parts(self) -> (Result<u64, EventPlaneStatus>, Option<EventOwnerWorkError>) {
        (self.result, self.cleanup)
    }
}

impl From<EventPlaneStatus> for EventPlaneReplaceError {
    fn from(status: EventPlaneStatus) -> Self {
        Self {
            result: Err(status),
            cleanup: None,
        }
    }
}

/// One owner operation. Waiting requires a worker completion notification.
#[derive(Debug)]
#[must_use]
pub enum OwnerStep {
    Idle,
    Applied(OwnerOp),
    Work(EventOwnerWork),
    Waiting,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnloadTestPhase {
    Locked,
    Detached,
    Destroyed,
}

#[cfg(test)]
type UnloadTestProbe = Box<dyn FnMut(UnloadTestPhase) + Send>;

impl EventOwnerWork {
    fn new(operation: OwnerOp) -> Self {
        let serial = NEXT_EVENT_OWNER_WORK_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .expect("event owner work identity exhausted");
        Self {
            identity: EventOwnerWorkId { serial, operation },
            metadata_applied: false,
            retired_payloads: Vec::new(),
        }
    }

    #[must_use]
    pub fn identity(&self) -> &EventOwnerWorkId {
        &self.identity
    }

    /// Run only on a worker in daemon production. This method can wait for the router.
    ///
    /// The worker must hold no router guard before this call. Router guards can nest
    /// counter registry locks, but never causal locks or another router acquisition.
    /// Metadata cleanup is atomic. Its lock duration scales with router metadata.
    /// The mutex provides no fairness or maximum acquisition time.
    pub fn run(
        mut self,
        router: &PackageEventRouter,
    ) -> Result<EventOwnerCompletion, EventOwnerWorkError> {
        #[cfg(test)]
        let mut probe = router
            .unload_test_probe
            .try_lock()
            .expect("test probe is available")
            .take();
        if !self.metadata_applied {
            let mut inner = match router.inner.lock() {
                Ok(inner) => inner,
                Err(_) => return Err(EventOwnerWorkError::RouterPoisoned(self)),
            };
            #[cfg(test)]
            if let Some(probe) = &mut probe {
                probe(UnloadTestPhase::Locked);
            }
            let op = &self.identity.operation;
            if op.kind == OwnerOpKind::Unload {
                self.retired_payloads =
                    apply_unload(&mut inner, &router.counters, &op.owner, op.generation);
            }
            self.metadata_applied = true;
        }

        #[cfg(test)]
        if let Some(probe) = &mut probe {
            probe(UnloadTestPhase::Detached);
        }
        // Keep envelope metadata and its byte charge until payload destruction finishes.
        drop(std::mem::take(&mut self.retired_payloads));

        #[cfg(test)]
        if let Some(probe) = &mut probe {
            probe(UnloadTestPhase::Destroyed);
        }
        let mut inner = match router.inner.lock() {
            Ok(inner) => inner,
            Err(_) => return Err(EventOwnerWorkError::RouterPoisoned(self)),
        };
        let op = &self.identity.operation;
        retire_destroyed_payloads(&mut inner, &router.counters, &op.owner, op.generation);
        drop(inner);
        Ok(EventOwnerCompletion {
            identity: self.identity,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventSubscription {
    pub plugin_key: String,
    pub owner: String,
    pub name: String,
    pub handler_id: String,
    pub generation: u64,
    pub event_generation: u64,
    pub plugin_generation: u64,
}

/// Connection-scoped client holder. Identity is `(connection_id, subscription_id)`.
#[derive(Clone)]
pub(crate) struct ClientEventHolder {
    pub connection_id: String,
    pub subscription_id: String,
    pub owner: String,
    pub name: String,
    pub subjects: BTreeSet<String>,
    pub mailbox: Arc<ClientEventMailbox>,
    pub gap: Arc<std::sync::atomic::AtomicBool>,
}

struct RegisteredClientEventHolder {
    holder: ClientEventHolder,
    event_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HolderId {
    pub consumer_plugin_key: u64,
    pub generation: u64,
}

#[derive(Debug)]
pub struct ReadyDelivery {
    pub envelope_id: u64,
    pub owner: String,
    pub name: String,
    pub payload: Arc<[u8]>,
    pub payload_json: Value,
    pub size: usize,
    pub holder: EventSubscription,
    pull_id: u64,
}

impl ReadyDelivery {
    #[must_use]
    pub(crate) fn pull_id(&self) -> u64 {
        self.pull_id
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventPlaneSnapshot {
    pub producer_events: BTreeMap<String, usize>,
    pub producer_bytes: BTreeMap<String, usize>,
    pub consumer_events: BTreeMap<String, usize>,
    pub consumer_bytes: BTreeMap<String, usize>,
    pub global_in_flight_bytes: usize,
    pub admitted_holders: usize,
    pub queued_holders: usize,
}

struct Envelope {
    #[allow(dead_code)]
    id: u64,
    owner: String,
    name: String,
    payload: Arc<[u8]>,
    payload_json: Value,
    size: usize,
    enqueued_at: Instant,
    remaining_holders: usize,
    producer_age_ref: Option<ProducerAgeRef>,
    retirement: Option<EnvelopeRetirement>,
}

struct EnvelopeRetirement {
    cleanup: (String, u64),
    destroyed: Arc<AtomicBool>,
}

#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct CleanupVisits {
    contracts: usize,
    subscriptions: usize,
    clients: usize,
    queues: usize,
    copies: usize,
    retiring: usize,
}

#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PreviewVisits {
    contracts: usize,
    event_buckets: usize,
    subscriptions: usize,
    proposals: usize,
}

#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SnapshotVisits {
    generations: usize,
    contracts: usize,
    event_buckets: usize,
    subscriptions: usize,
    plugins: usize,
    memberships: usize,
}

/// The empty envelope retains admission while this value owns its payload.
#[derive(Debug)]
struct RetiringPayload {
    payload: Arc<[u8]>,
    payload_json: Value,
    destroyed: Arc<AtomicBool>,
}

impl Drop for RetiringPayload {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.payload));
        drop(std::mem::take(&mut self.payload_json));
        self.destroyed.store(true, Ordering::Release);
    }
}

struct QueuedCopy {
    envelope_id: u64,
    holder: EventSubscription,
}

struct AdmittedHolder {
    retired: bool,
}

#[derive(Default)]
struct ProducerOccupancy {
    events: usize,
    bytes: usize,
    outstanding_prior: usize,
    current_generation: u64,
    current_cell: Option<Arc<QueueAgeMetric>>,
}

#[derive(Default)]
struct ConsumerQueue {
    events: usize,
    bytes: usize,
    copies: VecDeque<QueuedCopy>,
    age_cell: Option<Arc<QueueAgeMetric>>,
    generation: u64,
}

struct TokenBucket {
    tokens: f64,
    last: Instant,
}

struct RouterInner {
    policy: PackageEventPlanePolicy,
    contracts: HashMap<String, HashMap<String, EmittedContract>>,
    contract_name_counts: HashMap<String, usize>,
    subscriptions: HashMap<String, HashMap<String, Vec<EventSubscription>>>,
    client_holders: HashMap<String, HashMap<String, Vec<RegisteredClientEventHolder>>>,
    client_by_id: HashMap<(String, String), (String, String)>,
    subscriptions_per_plugin: HashMap<String, usize>,
    subscription_events_by_plugin: HashMap<String, HashMap<(String, String), usize>>,
    producer: HashMap<String, ProducerOccupancy>,
    consumers: HashMap<String, ConsumerQueue>,
    ready_consumers: BTreeSet<String>,
    last_ready_consumer: Option<String>,
    #[cfg(test)]
    ready_key_visits: usize,
    #[cfg(test)]
    ready_key_clones: usize,
    queued_by_producer: HashMap<String, HashMap<String, usize>>,
    retiring_by_cleanup: HashMap<(String, u64), HashSet<u64>>,
    #[cfg(test)]
    cleanup_visits: CleanupVisits,
    #[cfg(test)]
    preview_visits: PreviewVisits,
    #[cfg(test)]
    snapshot_visits: SnapshotVisits,
    envelopes: HashMap<u64, Envelope>,
    global_in_flight_bytes: usize,
    admitted: HashMap<u64, HashMap<(String, u64), AdmittedHolder>>,
    buckets: HashMap<String, TokenBucket>,
    next_envelope: u64,
    next_pull: u64,
    outstanding_pulls: HashSet<u64>,
    package_generation: HashMap<String, u64>,
    producer_age_lists: HashMap<(String, u64), ProducerAgeList>,
}

/// Send + Sync router. Owner and plugin callback APIs use `try_lock` only.
/// Worker unloads and replacement finalization use [`EventOwnerWork::run`].
pub struct PackageEventRouter {
    inner: Mutex<RouterInner>,
    counters: Arc<EventPlaneCounters>,
    delivery_wake: AtomicBool,
    next_holder_key: AtomicU64,
    fail_next_age_reserve: AtomicBool,
    policy: PackageEventPlanePolicy,
    #[cfg(test)]
    unload_test_probe: Mutex<Option<UnloadTestProbe>>,
}

impl PackageEventRouter {
    #[must_use]
    pub fn new(policy: PackageEventPlanePolicy) -> Self {
        let mut hub_contracts = HashMap::new();
        let schema = worktree_lifecycle_schema();
        for name in WORKTREE_EVENT_NAMES {
            hub_contracts.insert(
                (*name).to_string(),
                EmittedContract {
                    owner: HUB_EVENT_OWNER.to_string(),
                    name: (*name).to_string(),
                    audience: BTreeSet::from([EventAudience::Plugins]),
                    schema: schema.clone(),
                    package_generation: 0,
                },
            );
        }
        let contract_name_counts = hub_contracts.keys().map(|name| (name.clone(), 1)).collect();
        let contracts = HashMap::from([(HUB_EVENT_OWNER.to_string(), hub_contracts)]);
        let counters = Arc::new(EventPlaneCounters::new());
        let hub_cell = Arc::new(QueueAgeMetric::new(0));
        let hub_list =
            ProducerAgeList::new(policy.producer_queue_max_events, 0, Arc::clone(&hub_cell));
        counters.register_cell(
            AgeIdentity {
                kind: DaemonQueueKind::Producer,
                identity: HUB_EVENT_OWNER.to_string(),
                generation: Some(0),
            },
            Arc::clone(&hub_cell),
        );
        let mut producer = HashMap::new();
        producer.insert(
            HUB_EVENT_OWNER.to_string(),
            ProducerOccupancy {
                current_generation: 0,
                current_cell: Some(hub_cell),
                ..ProducerOccupancy::default()
            },
        );
        let mut producer_age_lists = HashMap::new();
        producer_age_lists.insert((HUB_EVENT_OWNER.to_string(), 0), hub_list);
        Self {
            inner: Mutex::new(RouterInner {
                policy,
                contracts,
                contract_name_counts,
                subscriptions: HashMap::new(),
                client_holders: HashMap::new(),
                client_by_id: HashMap::new(),
                subscriptions_per_plugin: HashMap::new(),
                subscription_events_by_plugin: HashMap::new(),
                producer,
                consumers: HashMap::new(),
                ready_consumers: BTreeSet::new(),
                last_ready_consumer: None,
                #[cfg(test)]
                ready_key_visits: 0,
                #[cfg(test)]
                ready_key_clones: 0,
                queued_by_producer: HashMap::new(),
                retiring_by_cleanup: HashMap::new(),
                #[cfg(test)]
                cleanup_visits: CleanupVisits::default(),
                #[cfg(test)]
                preview_visits: PreviewVisits::default(),
                #[cfg(test)]
                snapshot_visits: SnapshotVisits::default(),
                envelopes: HashMap::new(),
                global_in_flight_bytes: 0,
                admitted: HashMap::new(),
                buckets: HashMap::new(),
                next_envelope: 1,
                next_pull: 1,
                outstanding_pulls: HashSet::new(),
                package_generation: HashMap::new(),
                producer_age_lists,
            }),
            counters,
            delivery_wake: AtomicBool::new(false),
            next_holder_key: AtomicU64::new(1),
            fail_next_age_reserve: AtomicBool::new(false),
            policy,
            #[cfg(test)]
            unload_test_probe: Mutex::new(None),
        }
    }

    #[must_use]
    pub const fn policy(&self) -> PackageEventPlanePolicy {
        self.policy
    }

    #[must_use]
    pub fn counters(&self) -> &Arc<EventPlaneCounters> {
        &self.counters
    }

    #[cfg(test)]
    pub fn test_fail_next_age_reserve(&self) {
        self.fail_next_age_reserve.store(true, Ordering::SeqCst);
    }

    pub fn current_package_generation(&self, owner: &str) -> Result<u64, EventPlaneStatus> {
        let inner = lock_inner(&self.inner)?;
        Ok(inner.package_generation.get(owner).copied().unwrap_or(0))
    }

    pub fn begin_package_generation(&self, owner: &str) -> Result<u64, EventPlaneStatus> {
        if owner == HUB_EVENT_OWNER {
            return Ok(0);
        }
        let mut inner = lock_inner(&self.inner)?;
        Ok(bump_package_generation(&mut inner, owner))
    }

    pub fn try_register_contracts(
        &self,
        contracts: Vec<EmittedContract>,
    ) -> Result<(), EventPlaneStatus> {
        let mut inner = lock_inner(&self.inner)?;
        let mut owners = BTreeSet::new();
        for contract in &contracts {
            if contract.owner == HUB_EVENT_OWNER {
                return Err(EventPlaneStatus::RejectedForeign);
            }
            owners.insert(contract.owner.clone());
        }
        for owner in &owners {
            bump_package_generation(&mut inner, owner);
        }
        for mut contract in contracts {
            let generation = inner
                .package_generation
                .get(&contract.owner)
                .copied()
                .unwrap_or(0);
            contract.package_generation = generation;
            inner.insert_contract(contract);
        }
        for owner in owners {
            let generation = inner.package_generation.get(&owner).copied().unwrap_or(0);
            commit_diagnostic_state(&mut inner, &self.counters, &owner, generation);
        }
        Ok(())
    }

    /// Commit one package generation, its contracts, and its exact subscriptions
    /// under a single `try_lock`. Callers must invoke this only after Lua and
    /// lifecycle admission succeed. Contention returns `shed_busy` with no
    /// partial mutation. A later subscribe failure rolls the generation back.
    pub fn try_commit_package_generation(
        &self,
        owner: &str,
        contracts: Vec<EmittedContract>,
        subscriptions: Vec<EventSubscription>,
    ) -> Result<u64, EventPlaneStatus> {
        if owner == HUB_EVENT_OWNER {
            return Err(EventPlaneStatus::RejectedForeign);
        }
        let mut inner = lock_inner(&self.inner)?;
        for contract in &contracts {
            if contract.owner == HUB_EVENT_OWNER || contract.owner != owner {
                return Err(EventPlaneStatus::RejectedForeign);
            }
        }
        commit_package_generation_locked(
            &mut inner,
            &self.counters,
            owner,
            contracts,
            subscriptions,
        )
    }

    /// Unload the live generation and commit the replacement under one lock.
    /// Daemon production must call this method on a Host worker.
    /// Payload destruction and final accounting run after the replacement lock is released.
    pub fn try_replace_package_generation(
        &self,
        owner: &str,
        contracts: Vec<EmittedContract>,
        subscriptions: Vec<EventSubscription>,
    ) -> Result<u64, EventPlaneReplaceError> {
        if owner == HUB_EVENT_OWNER {
            return Err(EventPlaneStatus::RejectedForeign.into());
        }
        let mut inner = lock_inner(&self.inner)?;
        for contract in &contracts {
            if contract.owner == HUB_EVENT_OWNER || contract.owner != owner {
                return Err(EventPlaneStatus::RejectedForeign.into());
            }
        }
        preview_package_replacement(&mut inner, owner, &contracts, &subscriptions)?;
        let unload_generation = inner.package_generation.get(owner).copied().unwrap_or(0);
        let retired_payloads = apply_unload(&mut inner, &self.counters, owner, unload_generation);
        let result = commit_package_generation_locked(
            &mut inner,
            &self.counters,
            owner,
            contracts,
            subscriptions,
        );
        drop(inner);
        let mut work = EventOwnerWork::new(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: owner.to_string(),
            generation: unload_generation,
        });
        work.metadata_applied = true;
        work.retired_payloads = retired_payloads;
        if let Err(cleanup) = work.run(self) {
            return Err(EventPlaneReplaceError {
                result,
                cleanup: Some(cleanup),
            });
        }
        result.map_err(EventPlaneReplaceError::from)
    }

    pub fn try_subscribe(&self, subscription: EventSubscription) -> EventPlaneStatus {
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(status) => return status,
        };
        let plugin_key = subscription.plugin_key.clone();
        let status = subscribe_locked(&mut inner, subscription);
        if status == EventPlaneStatus::Accepted {
            bind_consumer_cell(&mut inner, &self.counters, &plugin_key);
        }
        status
    }

    pub(crate) fn try_subscribe_client(&self, holder: ClientEventHolder) -> EventPlaneStatus {
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(status) => return status,
        };
        subscribe_client_locked(&mut inner, holder)
    }

    pub(crate) fn try_unsubscribe_client(
        &self,
        connection_id: &str,
        subscription_id: &str,
    ) -> EventPlaneStatus {
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(status) => return status,
        };
        unsubscribe_client_locked(&mut inner, connection_id, subscription_id)
    }

    pub(crate) fn try_cleanup_client_connection(&self, connection_id: &str) -> EventPlaneStatus {
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(status) => return status,
        };
        cleanup_client_connection_locked(&mut inner, connection_id);
        EventPlaneStatus::Accepted
    }

    pub fn try_ingress(
        &self,
        caller_owner: &str,
        name: &str,
        payload: &Value,
        now: Instant,
    ) -> EventPlaneStatus {
        let started = Instant::now();
        self.counters.record_admission_attempt();
        let status = self.try_ingress_now(caller_owner, name, payload, now);
        if status != EventPlaneStatus::Accepted {
            self.counters.record_ingress_status(status.index());
        }
        self.counters.record_admission_latency(
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
        status
    }

    fn try_ingress_now(
        &self,
        caller_owner: &str,
        name: &str,
        payload: &Value,
        now: Instant,
    ) -> EventPlaneStatus {
        if is_wildcard(caller_owner) || is_wildcard(name) {
            return EventPlaneStatus::RejectedWildcard;
        }
        if caller_owner.trim().is_empty() || name.trim().is_empty() {
            return EventPlaneStatus::RejectedInvalid;
        }
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(status) => return status,
        };
        let Some(contract) = inner
            .contracts
            .get(caller_owner)
            .and_then(|events| events.get(name))
            .cloned()
        else {
            if caller_owner != HUB_EVENT_OWNER && name_owned_by_other(&inner, caller_owner, name) {
                return EventPlaneStatus::RejectedForeign;
            }
            return EventPlaneStatus::RejectedUndeclared;
        };
        if contract.owner != caller_owner {
            return EventPlaneStatus::RejectedForeign;
        }
        if contract.schema.validate(payload).is_err() {
            return EventPlaneStatus::RejectedInvalid;
        }
        let Ok(encoded) = serde_json::to_vec(payload) else {
            return EventPlaneStatus::RejectedInvalid;
        };
        if encoded.len() > inner.policy.payload_max_bytes {
            return EventPlaneStatus::RejectedOversize;
        }
        if !consume_token(&mut inner, caller_owner, now) {
            return EventPlaneStatus::RejectedOverRate;
        }
        let selected: Vec<EventSubscription> = inner
            .subscriptions
            .get(caller_owner)
            .and_then(|events| events.get(name))
            .into_iter()
            .flatten()
            .filter(|subscription| {
                inner
                    .contracts
                    .get(&subscription.owner)
                    .and_then(|events| events.get(&subscription.name))
                    .is_some_and(|contract| contract.audience.contains(&EventAudience::Plugins))
            })
            .cloned()
            .collect();
        if selected.len() > inner.policy.fanout_per_emit_max {
            return EventPlaneStatus::RejectedOverFanout;
        }
        let size = encoded.len();
        let producer_event_max = inner.policy.producer_queue_max_events;
        let producer_byte_max = inner.policy.producer_queue_max_bytes;
        let global_max = inner.policy.global_in_flight_bytes;
        let global_bytes = inner.global_bytes();
        let producer = inner.producer.entry(caller_owner.to_string()).or_default();
        if producer.events + 1 > producer_event_max
            || producer.bytes + size > producer_byte_max
            || global_bytes + size > global_max
        {
            return EventPlaneStatus::ShedFull;
        }
        let consumer_event_max = inner.policy.consumer_queue_max_events;
        let consumer_byte_max = inner.policy.consumer_queue_max_bytes;
        let mut accepted = Vec::new();
        let mut projected: HashMap<String, (usize, usize)> = HashMap::new();
        for subscription in selected {
            let consumer = inner
                .consumers
                .entry(subscription.plugin_key.clone())
                .or_default();
            let (events, bytes) = projected
                .entry(subscription.plugin_key.clone())
                .or_insert((consumer.events, consumer.bytes));
            let Some(next_events) = events.checked_add(1) else {
                continue;
            };
            let Some(next_bytes) = bytes.checked_add(size) else {
                continue;
            };
            if next_events > consumer_event_max || next_bytes > consumer_byte_max {
                continue;
            }
            *events = next_events;
            *bytes = next_bytes;
            accepted.push(subscription);
        }
        if accepted.is_empty() {
            return if inner
                .subscriptions
                .get(caller_owner)
                .and_then(|events| events.get(name))
                .is_none_or(Vec::is_empty)
            {
                deliver_to_client_holders(&inner, caller_owner, name, payload, encoded.len());
                EventPlaneStatus::Accepted
            } else {
                EventPlaneStatus::ShedFull
            };
        }
        let envelope_id = inner.next_envelope;
        let Some(next_envelope) = envelope_id.checked_add(1) else {
            return EventPlaneStatus::ShedFull;
        };
        deliver_to_client_holders(&inner, caller_owner, name, payload, encoded.len());
        inner.next_envelope = next_envelope;
        let payload_arc: Arc<[u8]> = encoded.into();
        let producer_age_ref = reserve_producer_age(
            &mut inner,
            &self.counters,
            caller_owner,
            now,
            self.fail_next_age_reserve.swap(false, Ordering::SeqCst),
        );
        inner.envelopes.insert(
            envelope_id,
            Envelope {
                id: envelope_id,
                owner: caller_owner.to_string(),
                name: name.to_string(),
                payload: payload_arc,
                payload_json: payload.clone(),
                size,
                enqueued_at: now,
                remaining_holders: accepted.len(),
                producer_age_ref,
                retirement: None,
            },
        );
        inner.global_in_flight_bytes += size;
        let (producer_cell, producer_events, producer_bytes, producer_generation, producer_prior) = {
            let producer = inner.producer.entry(caller_owner.to_string()).or_default();
            producer.events += 1;
            producer.bytes += size;
            (
                producer.current_cell.clone(),
                producer.events as u64,
                producer.bytes as u64,
                producer.current_generation,
                producer.outstanding_prior as u64,
            )
        };
        if let Some(cell) = producer_cell {
            let oldest = inner
                .producer_age_lists
                .get(&(caller_owner.to_string(), producer_generation))
                .map(ProducerAgeList::oldest_nanos)
                .unwrap_or(u64::MAX);
            cell.store(
                producer_events,
                oldest,
                producer_prior,
                false,
                producer_bytes,
            );
        }
        self.counters
            .set_global_in_flight_bytes(inner.global_bytes() as u64);
        for subscription in accepted {
            enqueue_consumer_copy(&mut inner, &self.counters, envelope_id, size, subscription);
        }
        drop(inner);
        self.delivery_wake.store(true, Ordering::SeqCst);
        EventPlaneStatus::Accepted
    }

    pub fn take_delivery_wake(&self) -> bool {
        self.delivery_wake.swap(false, Ordering::SeqCst)
    }

    pub fn peek_delivery_wake(&self) -> bool {
        self.delivery_wake.load(Ordering::SeqCst)
    }

    pub fn set_delivery_wake(&self) {
        self.delivery_wake.store(true, Ordering::SeqCst);
    }

    pub fn pull_ready_batch(
        &self,
        max_items: usize,
        max_bytes: usize,
        started: Instant,
        max_elapsed: Duration,
    ) -> Result<Vec<ReadyDelivery>, EventPlaneStatus> {
        let mut inner = lock_inner(&self.inner)?;
        let mut ready = Vec::new();
        let mut used_bytes = 0;
        let consumer_count = inner.ready_consumers.len();
        #[cfg(test)]
        {
            inner.ready_key_visits = 0;
            inner.ready_key_clones = 0;
        }
        // No new consumer enters during this pull. One pass prevents retries
        // of byte-blocked heads while the cursor rotates between calls.
        for _ in 0..consumer_count {
            if ready.len() >= max_items || started.elapsed() >= max_elapsed {
                break;
            }
            let next = inner
                .last_ready_consumer
                .as_ref()
                .and_then(|cursor| {
                    inner
                        .ready_consumers
                        .range::<String, _>((
                            std::ops::Bound::Excluded(cursor),
                            std::ops::Bound::Unbounded,
                        ))
                        .next()
                })
                .or_else(|| inner.ready_consumers.first());
            let Some(next) = next else { break };
            let plugin_key = next.clone();
            inner.last_ready_consumer = Some(plugin_key.clone());
            #[cfg(test)]
            {
                inner.ready_key_visits += 1;
                inner.ready_key_clones += 2;
            }
            loop {
                if ready.len() >= max_items || started.elapsed() >= max_elapsed {
                    break;
                }
                let Some(copy) = inner.pop_queued_copy(&plugin_key) else {
                    break;
                };
                let Some((size, expired)) = inner.live_envelope(copy.envelope_id).map(|envelope| {
                    (
                        envelope.size,
                        envelope.enqueued_at.elapsed() > inner.policy.queue_age,
                    )
                }) else {
                    continue;
                };
                if inner
                    .consumers
                    .get_mut(&plugin_key)
                    .map(|queue| {
                        queue.events = queue.events.saturating_sub(1);
                        queue.bytes = queue.bytes.saturating_sub(size);
                    })
                    .is_none()
                {
                    continue;
                }
                if expired {
                    self.counters.record_router_queue_age_expiry();
                    retire_holder_locked(
                        &mut inner,
                        &self.counters,
                        copy.envelope_id,
                        &copy.holder.plugin_key,
                        copy.holder.generation,
                    );
                    update_consumer_age(&mut inner, &plugin_key, &self.counters);
                    continue;
                }
                if used_bytes + size > max_bytes && !ready.is_empty() {
                    inner.note_queued_copy(&copy.holder);
                    if let Some(queue) = inner.consumers.get_mut(&plugin_key) {
                        if queue.copies.is_empty() {
                            inner.ready_consumers.insert(plugin_key.clone());
                        }
                    }
                    if let Some(queue) = inner.consumers.get_mut(&plugin_key) {
                        queue.events += 1;
                        queue.bytes += size;
                        queue.copies.push_front(copy);
                    }
                    update_consumer_age(&mut inner, &plugin_key, &self.counters);
                    break;
                }
                used_bytes += size;
                self.counters.record_delivery_attempt();
                if let Some(envelope) = inner.live_envelope(copy.envelope_id) {
                    self.counters.record_delivery_latency(
                        u64::try_from(envelope.enqueued_at.elapsed().as_micros())
                            .unwrap_or(u64::MAX),
                    );
                }
                update_consumer_age(&mut inner, &plugin_key, &self.counters);
                let (owner, name, payload, payload_json) = inner
                    .live_envelope(copy.envelope_id)
                    .map(|envelope| {
                        (
                            envelope.owner.clone(),
                            envelope.name.clone(),
                            envelope.payload.clone(),
                            envelope.payload_json.clone(),
                        )
                    })
                    .expect("envelope exists");
                let pull_id = inner.next_pull;
                inner.next_pull = inner.next_pull.saturating_add(1);
                inner.outstanding_pulls.insert(pull_id);
                ready.push(ReadyDelivery {
                    envelope_id: copy.envelope_id,
                    owner,
                    name,
                    payload,
                    payload_json,
                    size,
                    holder: copy.holder,
                    pull_id,
                });
            }
        }
        if !ready.is_empty() || !inner.queued_by_producer.is_empty() {
            self.delivery_wake.store(true, Ordering::SeqCst);
        }
        Ok(ready)
    }

    pub fn note_admitted(
        &self,
        envelope_id: u64,
        plugin_key: &str,
        generation: u64,
    ) -> Result<(), EventPlaneStatus> {
        let mut inner = lock_inner(&self.inner)?;
        if inner.live_envelope(envelope_id).is_some() {
            inner
                .admitted
                .entry(envelope_id)
                .or_default()
                .entry((plugin_key.to_string(), generation))
                .or_insert(AdmittedHolder { retired: false });
        }
        Ok(())
    }

    pub fn retire_holder(
        &self,
        envelope_id: u64,
        plugin_key: &str,
        generation: u64,
    ) -> Result<bool, EventPlaneStatus> {
        let mut inner = lock_inner(&self.inner)?;
        Ok(retire_holder_locked(
            &mut inner,
            &self.counters,
            envelope_id,
            plugin_key,
            generation,
        ))
    }

    pub(crate) fn retire_pulled(
        &self,
        pull_id: u64,
        envelope_id: u64,
        plugin_key: &str,
        generation: u64,
    ) -> Result<bool, EventPlaneStatus> {
        let mut inner = lock_inner(&self.inner)?;
        inner.outstanding_pulls.remove(&pull_id);
        Ok(retire_holder_locked(
            &mut inner,
            &self.counters,
            envelope_id,
            plugin_key,
            generation,
        ))
    }

    pub(crate) fn requeue_delivery(
        &self,
        delivery: ReadyDelivery,
    ) -> Result<(), (Box<ReadyDelivery>, EventPlaneStatus)> {
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(status) => return Err((Box::new(delivery), status)),
        };
        if !inner.outstanding_pulls.remove(&delivery.pull_id) {
            return Err((Box::new(delivery), EventPlaneStatus::RejectedInvalid));
        }
        if inner.live_envelope(delivery.envelope_id).is_none() {
            return Ok(());
        }
        let consumer_event_max = inner.policy.consumer_queue_max_events;
        let consumer_byte_max = inner.policy.consumer_queue_max_bytes;
        let consumer = inner
            .consumers
            .entry(delivery.holder.plugin_key.clone())
            .or_default();
        if consumer.events + 1 > consumer_event_max
            || consumer.bytes + delivery.size > consumer_byte_max
        {
            inner.outstanding_pulls.insert(delivery.pull_id);
            return Err((Box::new(delivery), EventPlaneStatus::ShedFull));
        }
        consumer.events += 1;
        consumer.bytes += delivery.size;
        inner.note_queued_copy(&delivery.holder);
        let consumer = inner
            .consumers
            .get_mut(&delivery.holder.plugin_key)
            .expect("the accepted requeue has a consumer");
        let plugin_key = delivery.holder.plugin_key.clone();
        let was_empty = consumer.copies.is_empty();
        consumer.copies.push_front(QueuedCopy {
            envelope_id: delivery.envelope_id,
            holder: delivery.holder,
        });
        if was_empty {
            inner.ready_consumers.insert(plugin_key.clone());
        }
        update_consumer_age(&mut inner, &plugin_key, &self.counters);
        self.delivery_wake.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) fn complete_pulled_delivery(
        &self,
        delivery: ReadyDelivery,
    ) -> Result<(), (Box<ReadyDelivery>, EventPlaneStatus)> {
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(status) => return Err((Box::new(delivery), status)),
        };
        if !inner.outstanding_pulls.remove(&delivery.pull_id) {
            return Err((Box::new(delivery), EventPlaneStatus::RejectedInvalid));
        }
        retire_holder_locked(
            &mut inner,
            &self.counters,
            delivery.envelope_id,
            &delivery.holder.plugin_key,
            delivery.holder.generation,
        );
        Ok(())
    }

    pub fn try_apply(&self, op: &OwnerOp) -> OwnerApplyResult {
        if op.kind == OwnerOpKind::Unload && op.owner != HUB_EVENT_OWNER {
            return OwnerApplyResult::Work(EventOwnerWork::new(op.clone()));
        }
        OwnerApplyResult::Applied
    }

    pub fn snapshot(&self) -> Result<EventPlaneSnapshot, EventPlaneStatus> {
        let inner = lock_inner(&self.inner)?;
        Ok(EventPlaneSnapshot {
            producer_events: inner
                .producer
                .iter()
                .map(|(owner, occupancy)| (owner.clone(), occupancy.events))
                .collect(),
            producer_bytes: inner
                .producer
                .iter()
                .map(|(owner, occupancy)| (owner.clone(), occupancy.bytes))
                .collect(),
            consumer_events: inner
                .consumers
                .iter()
                .map(|(plugin, queue)| (plugin.clone(), queue.events))
                .collect(),
            consumer_bytes: inner
                .consumers
                .iter()
                .map(|(plugin, queue)| (plugin.clone(), queue.bytes))
                .collect(),
            global_in_flight_bytes: inner.global_bytes(),
            admitted_holders: inner
                .admitted
                .values()
                .flat_map(|holders| holders.values())
                .filter(|holder| !holder.retired)
                .count(),
            queued_holders: inner
                .consumers
                .values()
                .map(|queue| queue.copies.len())
                .sum(),
        })
    }

    pub fn next_holder_generation(&self) -> u64 {
        self.next_holder_key.fetch_add(1, Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn test_with_inner_held<R>(&self, body: impl FnOnce() -> R) -> R {
        let _guard = self.inner.try_lock().expect("test hold must acquire inner");
        body()
    }

    #[cfg(test)]
    #[must_use]
    pub fn test_outstanding_pulls(&self) -> usize {
        lock_inner(&self.inner)
            .map(|inner| inner.outstanding_pulls.len())
            .unwrap_or(usize::MAX)
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn test_observability_snapshot(
        &self,
    ) -> botster_hub_client::DaemonObservabilityCounters {
        self.counters.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn test_refresh_consumer_age(&self, plugin_key: &str) {
        let mut inner = lock_inner(&self.inner).expect("test refresh must lock");
        update_consumer_age(&mut inner, plugin_key, &self.counters);
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn test_client_holder_count(&self, connection_id: &str) -> usize {
        lock_inner(&self.inner)
            .map(|inner| {
                inner
                    .client_by_id
                    .keys()
                    .filter(|(holder_connection, _)| holder_connection == connection_id)
                    .count()
            })
            .unwrap_or(0)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn test_subscription_count(&self, plugin_key: &str) -> usize {
        lock_inner(&self.inner)
            .map(|inner| {
                inner
                    .subscriptions_per_plugin
                    .get(plugin_key)
                    .copied()
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn test_has_contract(&self, owner: &str, name: &str) -> bool {
        lock_inner(&self.inner)
            .map(|inner| {
                inner
                    .contracts
                    .get(owner)
                    .is_some_and(|events| events.contains_key(name))
            })
            .unwrap_or(false)
    }
}

impl RouterInner {
    fn global_bytes(&self) -> usize {
        self.global_in_flight_bytes
    }

    fn live_envelope(&self, envelope_id: u64) -> Option<&Envelope> {
        self.envelopes
            .get(&envelope_id)
            .filter(|envelope| envelope.retirement.is_none())
    }

    fn note_queued_copy(&mut self, holder: &EventSubscription) {
        *self
            .queued_by_producer
            .entry(holder.owner.clone())
            .or_default()
            .entry(holder.plugin_key.clone())
            .or_default() += 1;
    }

    fn forget_queued_copy(&mut self, holder: &EventSubscription) {
        let consumers = self
            .queued_by_producer
            .get_mut(&holder.owner)
            .expect("a queued copy has producer membership");
        let count = consumers
            .get_mut(&holder.plugin_key)
            .expect("a queued copy has consumer membership");
        *count = count.checked_sub(1).expect("each queued copy leaves once");
        if *count == 0 {
            consumers.remove(&holder.plugin_key);
        }
        // Delivery readiness also depends on the absence of empty buckets.
        if consumers.is_empty() {
            self.queued_by_producer.remove(&holder.owner);
        }
    }

    fn pop_queued_copy(&mut self, plugin_key: &str) -> Option<QueuedCopy> {
        let queue = self.consumers.get_mut(plugin_key)?;
        let copy = queue.copies.pop_front()?;
        if queue.copies.is_empty() {
            self.ready_consumers.remove(plugin_key);
        }
        self.forget_queued_copy(&copy.holder);
        Some(copy)
    }

    fn insert_contract(&mut self, contract: EmittedContract) {
        let name = contract.name.clone();
        let previous = self
            .contracts
            .entry(contract.owner.clone())
            .or_default()
            .insert(name.clone(), contract);
        if previous.is_none() {
            *self.contract_name_counts.entry(name).or_default() += 1;
        }
    }

    fn remove_contract(&mut self, owner: &str, name: &str) {
        let Some(events) = self.contracts.get_mut(owner) else {
            return;
        };
        if events.remove(name).is_some() {
            let count = self
                .contract_name_counts
                .get_mut(name)
                .expect("a contract has name membership");
            *count = count.checked_sub(1).expect("each contract leaves once");
            if *count == 0 {
                self.contract_name_counts.remove(name);
            }
        }
        if events.is_empty() {
            self.contracts.remove(owner);
        }
    }
}

fn lock_inner(
    mutex: &Mutex<RouterInner>,
) -> Result<std::sync::MutexGuard<'_, RouterInner>, EventPlaneStatus> {
    match mutex.try_lock() {
        Ok(guard) => Ok(guard),
        Err(TryLockError::WouldBlock) => Err(EventPlaneStatus::ShedBusy),
        Err(TryLockError::Poisoned(poisoned)) => {
            drop(poisoned.into_inner());
            Err(EventPlaneStatus::ShedBusy)
        }
    }
}

fn is_wildcard(value: &str) -> bool {
    value.contains('*') || value.contains('?')
}

fn name_owned_by_other(inner: &RouterInner, caller: &str, name: &str) -> bool {
    // The caller checks its own exact contract before this lookup.
    debug_assert!(
        !inner
            .contracts
            .get(caller)
            .is_some_and(|events| events.contains_key(name))
    );
    inner.contract_name_counts.contains_key(name)
}

fn consume_token(inner: &mut RouterInner, owner: &str, now: Instant) -> bool {
    let rate = f64::from(inner.policy.package_rate_per_sec);
    let burst = f64::from(inner.policy.package_burst);
    let bucket = inner
        .buckets
        .entry(owner.to_string())
        .or_insert(TokenBucket {
            tokens: burst,
            last: now,
        });
    let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
    bucket.tokens = (bucket.tokens + elapsed * rate).min(burst);
    bucket.last = now;
    if bucket.tokens < 1.0 {
        return false;
    }
    bucket.tokens -= 1.0;
    true
}

struct AdmissionSnapshot {
    owner: String,
    generation: Option<u64>,
    had_contract_owner: bool,
    contracts: HashMap<String, Option<EmittedContract>>,
    subscription_owners: HashSet<String>,
    subscriptions: HashMap<(String, String), Option<Vec<EventSubscription>>>,
    plugins: HashMap<String, PluginAdmissionSnapshot>,
}

struct PluginAdmissionSnapshot {
    count: Option<usize>,
    events: Option<HashMap<(String, String), usize>>,
}

fn snapshot_admission(
    inner: &mut RouterInner,
    owner: &str,
    contracts: &[EmittedContract],
    subscriptions: &[EventSubscription],
) -> AdmissionSnapshot {
    #[cfg(test)]
    {
        inner.snapshot_visits = SnapshotVisits {
            generations: 1,
            ..SnapshotVisits::default()
        };
    }
    let mut snapshot = AdmissionSnapshot {
        owner: owner.to_string(),
        generation: inner.package_generation.get(owner).copied(),
        had_contract_owner: inner.contracts.contains_key(owner),
        contracts: HashMap::new(),
        subscription_owners: HashSet::new(),
        subscriptions: HashMap::new(),
        plugins: HashMap::new(),
    };
    // Capture each previous value once, before any proposed write can replace it.
    for contract in contracts {
        snapshot
            .contracts
            .entry(contract.name.clone())
            .or_insert_with(|| {
                #[cfg(test)]
                {
                    inner.snapshot_visits.contracts += 1;
                }
                inner
                    .contracts
                    .get(owner)
                    .and_then(|events| events.get(&contract.name))
                    .cloned()
            });
    }
    for subscription in subscriptions {
        let key = (subscription.owner.clone(), subscription.name.clone());
        snapshot
            .subscriptions
            .entry(key.clone())
            .or_insert_with(|| {
                let events = inner.subscriptions.get(&key.0);
                if events.is_some() {
                    snapshot.subscription_owners.insert(key.0.clone());
                }
                let previous = events.and_then(|events| events.get(&key.1));
                #[cfg(test)]
                {
                    inner.snapshot_visits.event_buckets += 1;
                    inner.snapshot_visits.subscriptions += previous.map_or(0, Vec::len);
                }
                previous.cloned()
            });
        snapshot
            .plugins
            .entry(subscription.plugin_key.clone())
            .or_insert_with(|| {
                let events = inner
                    .subscription_events_by_plugin
                    .get(&subscription.plugin_key);
                #[cfg(test)]
                {
                    inner.snapshot_visits.plugins += 1;
                    inner.snapshot_visits.memberships += events.map_or(0, HashMap::len);
                }
                PluginAdmissionSnapshot {
                    count: inner
                        .subscriptions_per_plugin
                        .get(&subscription.plugin_key)
                        .copied(),
                    events: events.cloned(),
                }
            });
    }
    snapshot
}

fn restore_admission(inner: &mut RouterInner, snapshot: AdmissionSnapshot) {
    for (name, previous) in snapshot.contracts {
        match previous {
            Some(contract) => {
                inner.insert_contract(contract);
            }
            None => {
                inner.remove_contract(&snapshot.owner, &name);
            }
        }
    }
    if snapshot.had_contract_owner {
        inner.contracts.entry(snapshot.owner.clone()).or_default();
    }
    for ((owner, name), previous) in snapshot.subscriptions {
        match previous {
            Some(subscriptions) => {
                inner
                    .subscriptions
                    .entry(owner)
                    .or_default()
                    .insert(name, subscriptions);
            }
            None => {
                if let Some(events) = inner.subscriptions.get_mut(&owner) {
                    events.remove(&name);
                    if events.is_empty() && !snapshot.subscription_owners.contains(&owner) {
                        inner.subscriptions.remove(&owner);
                    }
                }
            }
        }
    }
    for (plugin, previous) in snapshot.plugins {
        match previous.count {
            Some(count) => {
                inner.subscriptions_per_plugin.insert(plugin.clone(), count);
            }
            None => {
                inner.subscriptions_per_plugin.remove(&plugin);
            }
        }
        match previous.events {
            Some(events) => {
                inner.subscription_events_by_plugin.insert(plugin, events);
            }
            None => {
                inner.subscription_events_by_plugin.remove(&plugin);
            }
        }
    }
    match snapshot.generation {
        Some(generation) => {
            inner.package_generation.insert(snapshot.owner, generation);
        }
        None => {
            inner.package_generation.remove(&snapshot.owner);
        }
    }
}

fn preview_package_replacement(
    inner: &mut RouterInner,
    owner: &str,
    contracts: &[EmittedContract],
    subscriptions: &[EventSubscription],
) -> Result<(), EventPlaneStatus> {
    #[cfg(test)]
    {
        inner.preview_visits = PreviewVisits::default();
    }
    let unload_generation = inner.package_generation.get(owner).copied().unwrap_or(0);
    let mut proposed_contracts = HashMap::new();
    for contract in contracts {
        #[cfg(test)]
        {
            inner.preview_visits.contracts += 1;
        }
        if contract.owner == HUB_EVENT_OWNER || contract.owner != owner {
            return Err(EventPlaneStatus::RejectedForeign);
        }
        // Match commit order: the last proposed contract for a name wins.
        proposed_contracts.insert(contract.name.as_str(), contract);
    }
    let mut removed_plugins: HashMap<String, usize> = HashMap::new();
    let mut removed_events: HashMap<(String, String), usize> = HashMap::new();
    for key in unload_subscription_keys(inner, owner) {
        let Some(holders) = inner
            .subscriptions
            .get(&key.0)
            .and_then(|events| events.get(&key.1))
        else {
            continue;
        };
        #[cfg(test)]
        {
            inner.preview_visits.event_buckets += 1;
            inner.preview_visits.subscriptions += holders.len();
        }
        for subscription in holders {
            if unload_removes_subscription(subscription, owner, unload_generation) {
                *removed_plugins
                    .entry(subscription.plugin_key.clone())
                    .or_default() += 1;
                *removed_events.entry(key.clone()).or_default() += 1;
            }
        }
    }
    let mut plugin_counts = HashMap::new();
    let mut event_counts = HashMap::new();
    for subscription in subscriptions {
        #[cfg(test)]
        {
            inner.preview_visits.proposals += 1;
        }
        let key = (subscription.owner.clone(), subscription.name.clone());
        let proposed = if subscription.owner == owner {
            proposed_contracts.get(subscription.name.as_str()).copied()
        } else {
            None
        };
        let contract = proposed.or_else(|| {
            inner
                .contracts
                .get(&key.0)
                .and_then(|events| events.get(&key.1))
                .filter(|contract| {
                    contract.owner != owner || contract.package_generation > unload_generation
                })
        });
        let plugin_count = plugin_counts
            .entry(subscription.plugin_key.clone())
            .or_insert_with(|| {
                inner
                    .subscriptions_per_plugin
                    .get(&subscription.plugin_key)
                    .copied()
                    .unwrap_or(0)
                    .checked_sub(
                        removed_plugins
                            .get(&subscription.plugin_key)
                            .copied()
                            .unwrap_or(0),
                    )
                    .expect("selected removals cannot exceed the plugin count")
            });
        let event_count = event_counts.entry(key.clone()).or_insert_with(|| {
            inner
                .subscriptions
                .get(&key.0)
                .and_then(|events| events.get(&key.1))
                .map_or(0, Vec::len)
                .checked_sub(removed_events.get(&key).copied().unwrap_or(0))
                .expect("selected removals cannot exceed the event count")
        });
        let status = subscription_admission_status(
            &inner.policy,
            subscription,
            contract,
            *plugin_count,
            *event_count,
        );
        if status != EventPlaneStatus::Accepted {
            return Err(status);
        }
        *plugin_count += 1;
        *event_count += 1;
    }
    Ok(())
}

fn subscription_admission_status(
    policy: &PackageEventPlanePolicy,
    subscription: &EventSubscription,
    contract: Option<&EmittedContract>,
    plugin_count: usize,
    event_count: usize,
) -> EventPlaneStatus {
    if is_wildcard(&subscription.owner) || is_wildcard(&subscription.name) {
        return EventPlaneStatus::RejectedWildcard;
    }
    if subscription.owner.trim().is_empty() || subscription.name.trim().is_empty() {
        return EventPlaneStatus::RejectedInvalid;
    }
    let Some(contract) = contract else {
        return EventPlaneStatus::RejectedUndeclared;
    };
    if !contract.audience.contains(&EventAudience::Plugins) {
        return EventPlaneStatus::RejectedAudience;
    }
    if plugin_count >= policy.subscriptions_per_plugin_max {
        return EventPlaneStatus::RejectedInvalid;
    }
    if event_count >= policy.subscribers_per_event_max {
        return EventPlaneStatus::RejectedOverFanout;
    }
    EventPlaneStatus::Accepted
}

fn commit_package_generation_locked(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    contracts: Vec<EmittedContract>,
    subscriptions: Vec<EventSubscription>,
) -> Result<u64, EventPlaneStatus> {
    debug_assert!(
        contracts.iter().all(|contract| contract.owner == owner),
        "the caller validates contract ownership before admission"
    );
    if contracts.is_empty() && subscriptions.is_empty() {
        return Ok(inner.package_generation.get(owner).copied().unwrap_or(0));
    }
    let snapshot = snapshot_admission(inner, owner, &contracts, &subscriptions);
    let generation = bump_package_generation(inner, owner);
    for mut contract in contracts {
        contract.package_generation = generation;
        inner.insert_contract(contract);
    }
    for subscription in subscriptions {
        let status = subscribe_locked(inner, subscription);
        if status != EventPlaneStatus::Accepted {
            restore_admission(inner, snapshot);
            return Err(status);
        }
    }
    commit_diagnostic_state(inner, counters, owner, generation);
    Ok(generation)
}

fn subscribe_locked(inner: &mut RouterInner, subscription: EventSubscription) -> EventPlaneStatus {
    let key = (subscription.owner.clone(), subscription.name.clone());
    let contract = inner
        .contracts
        .get(&key.0)
        .and_then(|events| events.get(&key.1));
    let plugin_count = inner
        .subscriptions_per_plugin
        .get(&subscription.plugin_key)
        .copied()
        .unwrap_or(0);
    let event_count = inner
        .subscriptions
        .get(&key.0)
        .and_then(|events| events.get(&key.1))
        .map_or(0, Vec::len);
    let status = subscription_admission_status(
        &inner.policy,
        &subscription,
        contract,
        plugin_count,
        event_count,
    );
    if status != EventPlaneStatus::Accepted {
        return status;
    }
    let event_generation = contract
        .expect("accepted subscription has a contract")
        .package_generation;
    let plugin_generation = inner
        .package_generation
        .get(&subscription.plugin_key)
        .copied()
        .unwrap_or(0);
    let event_subs = inner
        .subscriptions
        .entry(key.0.clone())
        .or_default()
        .entry(key.1.clone())
        .or_default();
    let mut subscription = subscription;
    subscription.event_generation = event_generation;
    subscription.plugin_generation = plugin_generation;
    let plugin_key = subscription.plugin_key.clone();
    event_subs.push(subscription);
    *inner
        .subscription_events_by_plugin
        .entry(plugin_key.clone())
        .or_default()
        .entry(key)
        .or_default() += 1;
    *inner
        .subscriptions_per_plugin
        .entry(plugin_key)
        .or_insert(0) += 1;
    EventPlaneStatus::Accepted
}

fn subscribe_client_locked(inner: &mut RouterInner, holder: ClientEventHolder) -> EventPlaneStatus {
    if is_wildcard(&holder.owner) || is_wildcard(&holder.name) {
        return EventPlaneStatus::RejectedWildcard;
    }
    if holder.owner.trim().is_empty() || holder.name.trim().is_empty() {
        return EventPlaneStatus::RejectedInvalid;
    }
    let key = (holder.owner.clone(), holder.name.clone());
    let Some(contract) = inner
        .contracts
        .get(&key.0)
        .and_then(|events| events.get(&key.1))
    else {
        return EventPlaneStatus::RejectedUndeclared;
    };
    if !contract.audience.contains(&EventAudience::Clients) {
        return EventPlaneStatus::RejectedAudience;
    }
    let event_generation = contract.package_generation;
    let identity = (holder.connection_id.clone(), holder.subscription_id.clone());
    if inner.client_by_id.contains_key(&identity) {
        return EventPlaneStatus::RejectedInvalid;
    }
    let max_subscribers = inner.policy.subscribers_per_event_max;
    let holders = inner
        .client_holders
        .entry(key.0)
        .or_default()
        .entry(key.1)
        .or_default();
    if holders.len() >= max_subscribers {
        return EventPlaneStatus::RejectedOverFanout;
    }
    inner
        .client_by_id
        .insert(identity, (holder.owner.clone(), holder.name.clone()));
    holders.push(RegisteredClientEventHolder {
        holder,
        event_generation,
    });
    EventPlaneStatus::Accepted
}

fn unsubscribe_client_locked(
    inner: &mut RouterInner,
    connection_id: &str,
    subscription_id: &str,
) -> EventPlaneStatus {
    let identity = (connection_id.to_string(), subscription_id.to_string());
    let Some(key) = inner.client_by_id.remove(&identity) else {
        return EventPlaneStatus::RejectedInvalid;
    };
    if let Some(events) = inner.client_holders.get_mut(&key.0)
        && let Some(holders) = events.get_mut(&key.1)
    {
        holders.retain(|registered| {
            let holder = &registered.holder;
            !(holder.connection_id == connection_id && holder.subscription_id == subscription_id)
        });
        if holders.is_empty() {
            events.remove(&key.1);
        }
        if events.is_empty() {
            inner.client_holders.remove(&key.0);
        }
    }
    EventPlaneStatus::Accepted
}

fn cleanup_client_connection_locked(inner: &mut RouterInner, connection_id: &str) {
    let identities: Vec<(String, String)> = inner
        .client_by_id
        .keys()
        .filter(|(holder_connection, _)| holder_connection == connection_id)
        .cloned()
        .collect();
    for (holder_connection, subscription_id) in identities {
        let _ = unsubscribe_client_locked(inner, &holder_connection, &subscription_id);
    }
}

fn deliver_to_client_holders(
    inner: &RouterInner,
    owner: &str,
    name: &str,
    payload: &Value,
    size: usize,
) {
    let Some(holders) = inner
        .client_holders
        .get(owner)
        .and_then(|events| events.get(name))
    else {
        return;
    };
    for registered in holders {
        let holder = &registered.holder;
        if !client_subject_matches(&holder.subjects, payload) {
            continue;
        }
        if holder
            .mailbox
            .try_push(
                &holder.subscription_id,
                &holder.owner,
                &holder.name,
                payload.clone(),
                size,
            )
            .is_err()
        {
            holder.gap.store(true, std::sync::atomic::Ordering::SeqCst);
            holder
                .mailbox
                .set_gap(&holder.subscription_id, &holder.owner, &holder.name);
        }
    }
}

fn client_subject_matches(subjects: &BTreeSet<String>, payload: &Value) -> bool {
    if subjects.is_empty() {
        return true;
    }
    payload
        .get("subject")
        .and_then(Value::as_str)
        .is_some_and(|subject| subjects.contains(subject))
}

fn bump_package_generation(inner: &mut RouterInner, owner: &str) -> u64 {
    let next = inner
        .package_generation
        .get(owner)
        .copied()
        .unwrap_or(0)
        .saturating_add(1);
    inner.package_generation.insert(owner.to_string(), next);
    next
}

fn apply_unload(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    generation: u64,
) -> Vec<RetiringPayload> {
    if owner == HUB_EVENT_OWNER {
        return Vec::new();
    }
    #[cfg(test)]
    {
        inner.cleanup_visits = CleanupVisits::default();
    }
    if let Some(contracts) = inner.contracts.get(owner) {
        #[cfg(test)]
        {
            inner.cleanup_visits.contracts += contracts.len();
        }
        let removed: Vec<String> = contracts
            .iter()
            .filter(|(_, contract)| contract.package_generation <= generation)
            .map(|(name, _)| name.clone())
            .collect();
        for name in removed {
            inner.remove_contract(owner, &name);
        }
        if inner.contracts.get(owner).is_some_and(HashMap::is_empty) {
            inner.contracts.remove(owner);
        }
    }
    for key in unload_subscription_keys(inner, owner) {
        remove_unloaded_subscriptions(inner, &key, owner, generation);
    }
    let mut removed_client_ids = Vec::new();
    if let Some(events) = inner.client_holders.get_mut(owner) {
        events.retain(|_, holders| {
            #[cfg(test)]
            {
                inner.cleanup_visits.clients += holders.len();
            }
            holders.retain(|registered| {
                if registered.event_generation <= generation {
                    let holder = &registered.holder;
                    removed_client_ids
                        .push((holder.connection_id.clone(), holder.subscription_id.clone()));
                    false
                } else {
                    true
                }
            });
            !holders.is_empty()
        });
        if events.is_empty() {
            inner.client_holders.remove(owner);
        }
    }
    for identity in removed_client_ids {
        inner.client_by_id.remove(&identity);
    }
    let retired_payloads = drop_queued_for_owner(inner, counters, owner, generation);
    retire_owner_diagnostics(inner, counters, owner, generation);
    retired_payloads
}

fn unload_subscription_keys(inner: &RouterInner, owner: &str) -> BTreeSet<(String, String)> {
    let mut event_keys: BTreeSet<(String, String)> = inner
        .subscriptions
        .get(owner)
        .into_iter()
        .flat_map(|events| events.keys())
        .map(|name| (owner.to_string(), name.clone()))
        .collect();
    if let Some(events) = inner.subscription_events_by_plugin.get(owner) {
        event_keys.extend(events.keys().cloned());
    }
    event_keys
}

fn remove_unloaded_subscriptions(
    inner: &mut RouterInner,
    key: &(String, String),
    owner: &str,
    generation: u64,
) {
    let Some(events) = inner.subscriptions.get_mut(&key.0) else {
        return;
    };
    let Some(subscriptions) = events.get_mut(&key.1) else {
        return;
    };
    #[cfg(test)]
    {
        inner.cleanup_visits.subscriptions += subscriptions.len();
    }
    let mut removed_plugins = Vec::new();
    subscriptions.retain(|subscription| {
        if unload_removes_subscription(subscription, owner, generation) {
            removed_plugins.push(subscription.plugin_key.clone());
            false
        } else {
            true
        }
    });
    if subscriptions.is_empty() {
        events.remove(&key.1);
    }
    if events.is_empty() {
        inner.subscriptions.remove(&key.0);
    }
    for plugin in removed_plugins {
        let count = inner
            .subscriptions_per_plugin
            .get_mut(&plugin)
            .expect("a subscription has a plugin count");
        *count = count.checked_sub(1).expect("each subscription leaves once");
        if *count == 0 {
            inner.subscriptions_per_plugin.remove(&plugin);
        }
        let events = inner
            .subscription_events_by_plugin
            .get_mut(&plugin)
            .expect("a subscription has plugin membership");
        let count = events
            .get_mut(key)
            .expect("a subscription has event membership");
        *count = count.checked_sub(1).expect("each subscription leaves once");
        if *count == 0 {
            events.remove(key);
        }
        if events.is_empty() {
            inner.subscription_events_by_plugin.remove(&plugin);
        }
    }
}

fn unload_removes_subscription(
    subscription: &EventSubscription,
    owner: &str,
    generation: u64,
) -> bool {
    (subscription.owner == owner && subscription.event_generation <= generation)
        || (subscription.plugin_key == owner && subscription.plugin_generation <= generation)
}

fn drop_queued_for_owner(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    generation: u64,
) -> Vec<RetiringPayload> {
    // Copies can outlive their subscriptions through a delayed requeue.
    let mut consumers: BTreeSet<String> = inner
        .queued_by_producer
        .get(owner)
        .into_iter()
        .flat_map(|consumers| consumers.keys())
        .cloned()
        .collect();
    if inner.consumers.contains_key(owner) {
        consumers.insert(owner.to_string());
    }
    let mut dropped = Vec::new();
    let mut changed_consumers = Vec::new();
    for plugin_key in consumers {
        let queue = inner
            .consumers
            .get_mut(&plugin_key)
            .expect("copy membership names an existing consumer");
        #[cfg(test)]
        {
            inner.cleanup_visits.queues += 1;
            inner.cleanup_visits.copies += queue.copies.len();
        }
        let previous_len = queue.copies.len();
        queue.copies.retain_mut(|copy| {
            if unload_removes_subscription(&copy.holder, owner, generation) {
                dropped.push(QueuedCopy {
                    envelope_id: copy.envelope_id,
                    holder: std::mem::take(&mut copy.holder),
                });
                false
            } else {
                true
            }
        });
        let removed = previous_len - queue.copies.len();
        if removed > 0 {
            if queue.copies.is_empty() {
                inner.ready_consumers.remove(&plugin_key);
            }
            queue.events = queue
                .events
                .checked_sub(removed)
                .expect("each queued copy releases one consumer event");
            changed_consumers.push(plugin_key);
        }
    }
    let mut retired_payloads = Vec::new();
    for copy in dropped {
        inner.forget_queued_copy(&copy.holder);
        if let Some(envelope) = inner.live_envelope(copy.envelope_id) {
            let size = envelope.size;
            if let Some(queue) = inner.consumers.get_mut(&copy.holder.plugin_key) {
                queue.bytes = queue.bytes.saturating_sub(size);
            }
        }
        if mark_holder_retired(
            inner,
            copy.envelope_id,
            &copy.holder.plugin_key,
            copy.holder.generation,
        ) {
            let cleanup = (owner.to_string(), generation);
            inner
                .retiring_by_cleanup
                .entry(cleanup.clone())
                .or_default()
                .insert(copy.envelope_id);
            let envelope = inner
                .envelopes
                .get_mut(&copy.envelope_id)
                .expect("the final holder retains its envelope");
            let destroyed = Arc::new(AtomicBool::new(false));
            envelope.retirement = Some(EnvelopeRetirement {
                cleanup,
                destroyed: Arc::clone(&destroyed),
            });
            retired_payloads.push(RetiringPayload {
                payload: std::mem::take(&mut envelope.payload),
                payload_json: std::mem::take(&mut envelope.payload_json),
                destroyed,
            });
        }
    }
    for plugin_key in changed_consumers {
        update_consumer_age(inner, &plugin_key, counters);
    }
    retired_payloads
}

fn retire_holder_locked(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    envelope_id: u64,
    plugin_key: &str,
    generation: u64,
) -> bool {
    if !mark_holder_retired(inner, envelope_id, plugin_key, generation) {
        return false;
    }
    retire_envelope_locked(inner, counters, envelope_id);
    true
}

/// Mark one holder exactly once. The final holder keeps its envelope until retirement.
fn mark_holder_retired(
    inner: &mut RouterInner,
    envelope_id: u64,
    plugin_key: &str,
    generation: u64,
) -> bool {
    if inner.live_envelope(envelope_id).is_none() {
        return false;
    }
    let holders = inner.admitted.entry(envelope_id).or_default();
    let key = (plugin_key.to_string(), generation);
    if let Some(holder) = holders.get_mut(&key) {
        if holder.retired {
            return false;
        }
        holder.retired = true;
    } else {
        holders.insert(key, AdmittedHolder { retired: true });
    }
    let Some(envelope) = inner.envelopes.get_mut(&envelope_id) else {
        return false;
    };
    envelope.remaining_holders = envelope.remaining_holders.saturating_sub(1);
    if envelope.remaining_holders > 0 {
        return false;
    }
    true
}

fn retire_envelope_locked(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    envelope_id: u64,
) {
    let Some(envelope) = inner.envelopes.remove(&envelope_id) else {
        return;
    };
    inner.admitted.remove(&envelope_id);
    if let Some(retirement) = &envelope.retirement {
        let ids = inner
            .retiring_by_cleanup
            .get_mut(&retirement.cleanup)
            .expect("a retiring envelope has cleanup membership");
        assert!(
            ids.remove(&envelope_id),
            "each retiring envelope leaves once"
        );
        if ids.is_empty() {
            inner.retiring_by_cleanup.remove(&retirement.cleanup);
        }
    }
    let owner = &envelope.owner;
    let size = envelope.size;
    let age_ref = envelope.producer_age_ref;
    inner.global_in_flight_bytes = inner
        .global_in_flight_bytes
        .checked_sub(size)
        .expect("each envelope releases its admitted byte charge once");
    if let Some(producer) = inner.producer.get_mut(owner) {
        producer.events = producer.events.saturating_sub(1);
        producer.bytes = producer.bytes.saturating_sub(size);
    }
    retire_producer_age(inner, counters, owner, age_ref);
    counters.set_global_in_flight_bytes(inner.global_bytes() as u64);
}

/// A restart reaches the same operation bucket even though its dispatch serial changes.
fn retire_destroyed_payloads(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    generation: u64,
) {
    let cleanup = (owner.to_string(), generation);
    let Some(ids) = inner.retiring_by_cleanup.get(&cleanup) else {
        return;
    };
    #[cfg(test)]
    {
        inner.cleanup_visits.retiring += ids.len();
    }
    let destroyed: Vec<u64> = ids
        .iter()
        .filter(|id| {
            inner
                .envelopes
                .get(*id)
                .and_then(|envelope| envelope.retirement.as_ref())
                .is_some_and(|retirement| retirement.destroyed.load(Ordering::Acquire))
        })
        .copied()
        .collect();
    for envelope_id in destroyed {
        retire_envelope_locked(inner, counters, envelope_id);
    }
}

fn reserve_producer_age(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    now: Instant,
    force_fail: bool,
) -> Option<ProducerAgeRef> {
    let generation = inner
        .producer
        .get(owner)
        .map(|occupancy| occupancy.current_generation)
        .or_else(|| inner.package_generation.get(owner).copied())
        .unwrap_or(0);
    let key = (owner.to_string(), generation);
    if force_fail {
        if let Some(cell) = inner
            .producer
            .get(owner)
            .and_then(|occupancy| occupancy.current_cell.clone())
        {
            cell.latch_invalid();
        }
        counters.record_age_sample_failure();
        return None;
    }
    let Some(list) = inner.producer_age_lists.get_mut(&key) else {
        if let Some(cell) = inner
            .producer
            .get(owner)
            .and_then(|occupancy| occupancy.current_cell.clone())
        {
            cell.latch_invalid();
        } else {
            counters.register_missing(AgeIdentity {
                kind: DaemonQueueKind::Producer,
                identity: owner.to_string(),
                generation: None,
            });
        }
        counters.record_age_sample_failure();
        return None;
    };
    let nanos = counters.nanos_of(now);
    match list.push(nanos) {
        Some(slot) => {
            list.publish();
            Some(ProducerAgeRef { generation, slot })
        }
        None => {
            list.cell().latch_invalid();
            counters.record_age_sample_failure();
            None
        }
    }
}

fn enqueue_consumer_copy(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    envelope_id: u64,
    size: usize,
    subscription: EventSubscription,
) {
    if !inner.consumers.contains_key(&subscription.plugin_key) {
        inner
            .consumers
            .insert(subscription.plugin_key.clone(), ConsumerQueue::default());
    }
    inner.note_queued_copy(&subscription);
    let (front_id, count, bytes, cell, gate) = {
        let consumer = inner
            .consumers
            .get_mut(&subscription.plugin_key)
            .expect("consumer queue exists");
        consumer.events += 1;
        consumer.bytes += size;
        if consumer.copies.is_empty() {
            inner
                .ready_consumers
                .insert(subscription.plugin_key.clone());
        }
        consumer.copies.push_back(QueuedCopy {
            envelope_id,
            holder: subscription,
        });
        (
            consumer.copies.front().map(|copy| copy.envelope_id),
            consumer.events as u64,
            consumer.bytes as u64,
            consumer.age_cell.clone(),
            consumer
                .age_cell
                .as_ref()
                .map(|cell| cell.gate())
                .unwrap_or(0),
        )
    };
    let oldest = front_id
        .and_then(|envelope_id| inner.live_envelope(envelope_id))
        .map(|envelope| counters.nanos_of(envelope.enqueued_at))
        .unwrap_or(u64::MAX);
    if let Some(cell) = cell {
        cell.store(count, oldest, gate, false, bytes);
    } else {
        counters.record_age_sample_failure();
    }
}

fn update_consumer_age(inner: &mut RouterInner, plugin_key: &str, counters: &EventPlaneCounters) {
    let front_id = inner
        .consumers
        .get(plugin_key)
        .and_then(|queue| queue.copies.front().map(|copy| copy.envelope_id));
    let oldest = front_id
        .and_then(|envelope_id| inner.live_envelope(envelope_id))
        .map(|envelope| counters.nanos_of(envelope.enqueued_at))
        .unwrap_or(u64::MAX);
    let Some(queue) = inner.consumers.get(plugin_key) else {
        return;
    };
    let count = queue.events as u64;
    let bytes = queue.bytes as u64;
    let Some(cell) = queue.age_cell.clone() else {
        counters.record_age_sample_failure();
        return;
    };
    cell.store(count, oldest, cell.gate(), false, bytes);
}

fn retire_producer_age(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    age_ref: Option<ProducerAgeRef>,
) {
    let Some(age_ref) = age_ref else {
        return;
    };
    let key = (owner.to_string(), age_ref.generation);
    let Some(list) = inner.producer_age_lists.get_mut(&key) else {
        return;
    };
    debug_assert_eq!(list.generation(), age_ref.generation);
    list.remove(age_ref.slot);
    list.publish();
    let live = list.live();
    let writes_closed = list.cell().is_write_closed();
    let current_generation = inner
        .producer
        .get(owner)
        .map(|occupancy| occupancy.current_generation)
        .unwrap_or(age_ref.generation);
    if age_ref.generation != current_generation
        && let Some(occupancy) = inner.producer.get_mut(owner)
    {
        occupancy.outstanding_prior = occupancy.outstanding_prior.saturating_sub(1);
        if let Some(cell) = occupancy.current_cell.as_ref() {
            cell.store(
                occupancy.events as u64,
                inner
                    .producer_age_lists
                    .get(&(owner.to_string(), occupancy.current_generation))
                    .map(ProducerAgeList::oldest_nanos)
                    .unwrap_or(u64::MAX),
                occupancy.outstanding_prior as u64,
                false,
                occupancy.bytes as u64,
            );
        }
    }
    // Unload closes the cell before the worker destroys payloads. Final retirement
    // must remove the empty list even when no replacement generation exists.
    if live == 0 && (age_ref.generation != current_generation || writes_closed) {
        if let Some(list) = inner.producer_age_lists.remove(&key) {
            list.cell().close_writes();
        }
        counters.retire_identity(&AgeIdentity {
            kind: DaemonQueueKind::Producer,
            identity: owner.to_string(),
            generation: Some(age_ref.generation),
        });
    }
}

fn commit_diagnostic_state(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    generation: u64,
) {
    if let Some(occupancy) = inner.producer.get_mut(owner)
        && occupancy.current_generation != generation
    {
        occupancy.outstanding_prior = occupancy.events;
        if let Some(cell) = occupancy.current_cell.take() {
            cell.close_writes();
            counters.retire_identity(&AgeIdentity {
                kind: DaemonQueueKind::Producer,
                identity: owner.to_string(),
                generation: Some(occupancy.current_generation),
            });
        }
    }
    let cell = Arc::new(QueueAgeMetric::new(generation));
    let prior = inner
        .producer
        .get(owner)
        .map(|occupancy| occupancy.outstanding_prior)
        .unwrap_or(0);
    cell.store(0, u64::MAX, prior as u64, false, 0);
    let list = ProducerAgeList::new(
        inner.policy.producer_queue_max_events,
        generation,
        Arc::clone(&cell),
    );
    inner
        .producer_age_lists
        .insert((owner.to_string(), generation), list);
    let occupancy = inner.producer.entry(owner.to_string()).or_default();
    occupancy.current_generation = generation;
    occupancy.current_cell = Some(Arc::clone(&cell));
    counters.register_cell(
        AgeIdentity {
            kind: DaemonQueueKind::Producer,
            identity: owner.to_string(),
            generation: Some(generation),
        },
        cell,
    );
    let mut plugin_keys: BTreeSet<String> = inner
        .subscriptions
        .get(owner)
        .into_iter()
        .flat_map(|events| events.values())
        .flatten()
        .map(|subscription| subscription.plugin_key.clone())
        .collect();
    if inner.subscription_events_by_plugin.contains_key(owner) {
        plugin_keys.insert(owner.to_string());
    }
    for plugin_key in plugin_keys {
        bind_consumer_cell(inner, counters, &plugin_key);
    }
}

fn bind_consumer_cell(inner: &mut RouterInner, counters: &EventPlaneCounters, plugin_key: &str) {
    let generation = inner
        .package_generation
        .get(plugin_key)
        .copied()
        .or_else(|| {
            inner
                .consumers
                .get(plugin_key)
                .map(|queue| queue.generation)
        })
        .unwrap_or(0);
    if inner
        .consumers
        .get(plugin_key)
        .and_then(|queue| queue.age_cell.as_ref())
        .is_some_and(|cell| cell.generation() == generation && !cell.is_write_closed())
    {
        return;
    }
    if let Some(queue) = inner.consumers.get(plugin_key)
        && let Some(old) = queue.age_cell.as_ref()
    {
        counters.retire_cell(
            &AgeIdentity {
                kind: DaemonQueueKind::Consumer,
                identity: plugin_key.to_string(),
                generation: Some(queue.generation),
            },
            old,
        );
    }
    let consumer_cell = Arc::new(QueueAgeMetric::new(generation));
    counters.register_cell(
        AgeIdentity {
            kind: DaemonQueueKind::Consumer,
            identity: plugin_key.to_string(),
            generation: Some(generation),
        },
        Arc::clone(&consumer_cell),
    );
    let queue = inner.consumers.entry(plugin_key.to_string()).or_default();
    queue.age_cell = Some(Arc::clone(&consumer_cell));
    queue.generation = generation;
    consumer_cell.store(queue.events as u64, u64::MAX, 0, false, queue.bytes as u64);
}

fn retire_owner_diagnostics(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    generation: u64,
) {
    // Producer retirement names the exact generation, never the replacement cell.
    counters.retire_identity(&AgeIdentity {
        kind: DaemonQueueKind::Producer,
        identity: owner.to_string(),
        generation: Some(generation),
    });
    let producer_key = (owner.to_string(), generation);
    if inner
        .producer_age_lists
        .get(&producer_key)
        .is_some_and(|list| list.live() == 0)
    {
        inner.producer_age_lists.remove(&producer_key);
    }
    if let Some(queue) = inner.consumers.get_mut(owner)
        && queue.generation <= generation
        && let Some(cell) = queue.age_cell.as_ref()
    {
        counters.retire_identity(&AgeIdentity {
            kind: DaemonQueueKind::Consumer,
            identity: owner.to_string(),
            generation: Some(queue.generation),
        });
        cell.store(queue.events as u64, u64::MAX, 0, false, queue.bytes as u64);
        if queue.copies.is_empty() {
            cell.close_writes();
        }
    }
}

/// Required causal transfer or release that has not yet been admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CausalOp {
    Transfer {
        scope_id: u64,
        from: LeaseIdentity,
        to: Vec<LeaseIdentity>,
    },
    Release {
        scope_id: u64,
        identity: LeaseIdentity,
    },
}

/// One-shot causal admission. `Retry` returns ownership to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum CausalAdmitResult {
    Applied,
    Retry(CausalOp),
}

/// A waiting or faulted operation remains owned by its caller.
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub(crate) enum CausalWaitResult {
    Applied,
    Waiting(CausalOp),
    Fault(CausalOp),
}

/// Owner-thread-only keyed operations. No mutex. Workers never read this map.
#[derive(Debug, Default)]
pub struct EventPlaneOwnerOps {
    pending: BTreeMap<String, VecDeque<OwnerOp>>,
    ready: BTreeSet<String>,
    in_flight: BTreeMap<String, EventOwnerWorkId>,
    after: Option<String>,
}

impl EventPlaneOwnerOps {
    pub fn record(&mut self, op: OwnerOp) {
        if !self.in_flight.contains_key(&op.owner) {
            self.ready.insert(op.owner.clone());
        }
        self.pending
            .entry(op.owner.clone())
            .or_default()
            .push_back(op);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    #[must_use]
    pub fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    /// Attempt one operation. Advance the owner cursor even when its worker must wait.
    /// The caller must reserve Host admission before this method can return Work.
    /// If submission fails, the caller must call [`Self::retry`] with the returned work identity.
    pub fn apply_ready(&mut self, router: &PackageEventRouter) -> OwnerStep {
        use std::ops::Bound::{Excluded, Unbounded};

        let next = self.after.as_deref().and_then(|after| {
            self.ready
                .range::<str, _>((Excluded(after), Unbounded))
                .next()
        });
        let Some(owner) = next.or_else(|| self.ready.first()).cloned() else {
            self.after = None;
            return if self.pending.is_empty() {
                OwnerStep::Idle
            } else {
                OwnerStep::Waiting
            };
        };
        let queue = self.pending.get_mut(&owner).expect("selected owner exists");
        let front = queue.front().expect("pending owner has an operation");
        let result = match router.try_apply(front) {
            OwnerApplyResult::Applied => {
                OwnerStep::Applied(queue.pop_front().expect("selected operation exists"))
            }
            OwnerApplyResult::Work(work) => {
                self.ready.remove(&owner);
                self.in_flight.insert(owner.clone(), work.identity.clone());
                OwnerStep::Work(work)
            }
        };
        if queue.is_empty() {
            self.pending.remove(&owner);
            self.ready.remove(&owner);
        }
        self.after = Some(owner);
        result
    }

    /// Accept one exact completion. A stale or duplicate completion cannot pop an operation.
    pub fn complete(&mut self, completion: EventOwnerCompletion) -> Option<OwnerOp> {
        if !self.matches_in_flight(&completion.identity) {
            return None;
        }
        let owner = &completion.identity.operation.owner;
        self.in_flight.remove(owner);
        let queue = self.pending.get_mut(owner).expect("validated owner exists");
        let completed = queue.pop_front();
        if queue.is_empty() {
            self.pending.remove(owner);
        } else {
            self.ready.insert(owner.clone());
        }
        completed
    }

    /// Restore readiness after failed submission or confirmed worker termination.
    /// The caller must retain admission and must confirm that the old worker cannot run.
    /// Invalidate the old serial now, before another dispatch can start.
    pub fn retry(&mut self, identity: &EventOwnerWorkId) -> bool {
        if !self.matches_in_flight(identity) {
            return false;
        }
        self.in_flight.remove(&identity.operation.owner);
        self.ready.insert(identity.operation.owner.clone());
        true
    }

    /// Restart the same operation with retained admission and a new serial.
    /// The caller must confirm that the old worker cannot run.
    /// Before rescheduling a rejected submission, call [`Self::retry`] with the returned work identity.
    /// Repeating metadata cleanup preserves retiring rows. The operation bucket recovers their charge
    /// after the previous worker destroys its payloads during completion or unwind.
    pub fn restart(&mut self, identity: &EventOwnerWorkId) -> Option<EventOwnerWork> {
        if !self.matches_in_flight(identity) {
            return None;
        }
        let work = EventOwnerWork::new(identity.operation.clone());
        self.in_flight
            .insert(identity.operation.owner.clone(), work.identity.clone());
        Some(work)
    }

    fn matches_in_flight(&self, identity: &EventOwnerWorkId) -> bool {
        self.in_flight.get(&identity.operation.owner) == Some(identity)
            && self
                .pending
                .get(&identity.operation.owner)
                .and_then(|queue| queue.front())
                == Some(&identity.operation)
    }

    #[cfg(test)]
    #[must_use]
    pub fn pending_for(&self, owner: &str) -> Vec<OwnerOp> {
        self.pending
            .get(owner)
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }
}

/// Worker undo: admit, or retract the identity the worker still owns.
pub fn release_or_retract(
    scopes: &CausalScopeTable,
    scope_id: u64,
    identity: LeaseIdentity,
) -> CausalAdmitResult {
    match scopes.try_admit(CausalOp::Release {
        scope_id,
        identity: identity.clone(),
    }) {
        CausalAdmitResult::Applied => CausalAdmitResult::Applied,
        CausalAdmitResult::Retry(op) => {
            if scopes.try_retract(scope_id, identity) {
                CausalAdmitResult::Applied
            } else {
                CausalAdmitResult::Retry(op)
            }
        }
    }
}

pub const CAUSAL_PENDING_MAX: usize = 256;
pub const CAUSAL_FLUSH_MAX: usize = 32;

/// Causal-scope lease table. Send + Sync. Lives beside the router.
pub struct CausalScopeTable {
    inner: Mutex<CausalInner>,
    pending: Mutex<VecDeque<CausalOp>>,
    pending_len: AtomicUsize,
    next_id: AtomicU64,
    owner_wake: OnceLock<ControlSender>,
    progress_pending: AtomicBool,
    interests: AtomicUsize,
    drain_blocked: AtomicUsize,
    faulted: AtomicBool,
    #[cfg(test)]
    progress_test_probe: Mutex<Option<Arc<dyn Fn(CausalTestPoint) + Send + Sync>>>,
}

const CAUSAL_PENDING_UNLOCK: usize = 1;
const CAUSAL_INNER_UNLOCK: usize = 2;
const CAUSAL_CAPACITY: usize = 4;

#[derive(Debug, PartialEq, Eq)]
enum CausalLockError {
    Busy,
    Poisoned,
}

/// Declare this notice before every causal guard so notification follows all unlocks.
struct CausalUnlockNotice<'a> {
    table: &'a CausalScopeTable,
    acquired: Cell<usize>,
    capacity: Cell<bool>,
    queued: Cell<bool>,
    poisoned: Cell<bool>,
}

impl<'a> CausalUnlockNotice<'a> {
    fn new(table: &'a CausalScopeTable) -> Self {
        Self {
            table,
            acquired: Cell::new(0),
            capacity: Cell::new(false),
            queued: Cell::new(false),
            poisoned: Cell::new(false),
        }
    }
}

impl Drop for CausalUnlockNotice<'_> {
    fn drop(&mut self) {
        let acquired = self.acquired.get();
        let poisoned = self.poisoned.get()
            || (acquired & CAUSAL_PENDING_UNLOCK != 0 && self.table.pending.is_poisoned())
            || (acquired & CAUSAL_INNER_UNLOCK != 0 && self.table.inner.is_poisoned());
        let new_fault = poisoned && !self.table.faulted.swap(true, Ordering::SeqCst);
        self.table
            .drain_blocked
            .fetch_and(!acquired, Ordering::SeqCst);
        let completed = acquired
            | if self.capacity.get() {
                CAUSAL_CAPACITY
            } else {
                0
            };
        let interested = self.table.interests.fetch_and(!completed, Ordering::SeqCst) & completed;
        if interested != 0 || self.queued.get() || new_fault {
            #[cfg(test)]
            self.table
                .run_progress_test_probe(CausalTestPoint::BeforeNotify);
            self.table.publish_progress();
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CausalTestPoint {
    BeforePendingArm,
    BeforePendingRetry,
    BeforeInnerArm,
    BeforeInnerRetry,
    BeforeNotify,
}

#[derive(Debug, Default)]
struct CausalInner {
    scopes: HashMap<u64, CausalScope>,
}

#[derive(Debug)]
struct CausalScope {
    leases: u32,
    identities: BTreeSet<LeaseIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LeaseIdentity {
    EventInFlight {
        request_id: String,
    },
    PendingEntityPublish {
        plugin_key: String,
    },
    AdmittedEntityMutation {
        family: String,
        generation: u64,
        seq: u64,
    },
    ProviderResyncNeed {
        family: String,
        generation: u64,
    },
    ProviderInFlight {
        request_id: String,
    },
}

impl Default for CausalScopeTable {
    fn default() -> Self {
        Self::new()
    }
}

impl CausalScopeTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CausalInner::default()),
            pending: Mutex::new(VecDeque::new()),
            pending_len: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            owner_wake: OnceLock::new(),
            progress_pending: AtomicBool::new(false),
            interests: AtomicUsize::new(0),
            drain_blocked: AtomicUsize::new(0),
            faulted: AtomicBool::new(false),
            #[cfg(test)]
            progress_test_probe: Mutex::new(None),
        }
    }

    /// Bind once at owner startup. Repeating the same channel is harmless.
    pub(crate) fn bind_owner_wake(&self, sender: ControlSender) {
        if let Err(sender) = self.owner_wake.set(sender) {
            assert!(
                self.owner_wake
                    .get()
                    .expect("owner is bound")
                    .same_channel(&sender),
                "a causal table keeps its original owner channel"
            );
        }
        if self.progress_pending.load(Ordering::SeqCst) {
            self.send_progress_doorbell();
        }
    }

    /// Harvest this retained bit after control traffic, even if the doorbell could not fit.
    pub(crate) fn take_progress_notification(&self) -> bool {
        self.progress_pending.swap(false, Ordering::SeqCst)
    }

    fn publish_progress(&self) {
        if !self.progress_pending.swap(true, Ordering::SeqCst) {
            self.send_progress_doorbell();
        }
    }

    fn send_progress_doorbell(&self) {
        if let Some(sender) = self.owner_wake.get() {
            let _ = sender.try_send(ControlMessage::CausalProgressPublished);
        }
    }

    fn causal_lock<'a, T>(
        &'a self,
        mutex: &'a Mutex<T>,
        notice: &CausalUnlockNotice<'_>,
        kind: usize,
        wait: bool,
        drain: bool,
    ) -> Result<MutexGuard<'a, T>, CausalLockError> {
        let first = self.try_causal_guard(mutex, notice, kind);
        match first {
            Err(CausalLockError::Busy) if wait => {
                #[cfg(test)]
                self.run_progress_test_probe(if kind == CAUSAL_PENDING_UNLOCK {
                    CausalTestPoint::BeforePendingArm
                } else {
                    CausalTestPoint::BeforeInnerArm
                });
                if drain {
                    self.drain_blocked.fetch_or(kind, Ordering::SeqCst);
                }
                self.interests.fetch_or(kind, Ordering::SeqCst);
                #[cfg(test)]
                self.run_progress_test_probe(if kind == CAUSAL_PENDING_UNLOCK {
                    CausalTestPoint::BeforePendingRetry
                } else {
                    CausalTestPoint::BeforeInnerRetry
                });
                // The second attempt closes an unlock before interest registration.
                let result = self.try_causal_guard(mutex, notice, kind);
                if result.is_ok() && drain {
                    self.drain_blocked.fetch_and(!kind, Ordering::SeqCst);
                }
                result
            }
            result => result,
        }
    }

    fn try_causal_guard<'a, T>(
        &'a self,
        mutex: &'a Mutex<T>,
        notice: &CausalUnlockNotice<'_>,
        kind: usize,
    ) -> Result<MutexGuard<'a, T>, CausalLockError> {
        match mutex.try_lock() {
            Ok(guard) => {
                notice.acquired.set(notice.acquired.get() | kind);
                Ok(guard)
            }
            Err(TryLockError::WouldBlock) => Err(CausalLockError::Busy),
            Err(TryLockError::Poisoned(poisoned)) => {
                notice.acquired.set(notice.acquired.get() | kind);
                notice.poisoned.set(true);
                drop(poisoned.into_inner());
                Err(CausalLockError::Poisoned)
            }
        }
    }

    fn causal_fault(&self, notice: &CausalUnlockNotice<'_>) -> bool {
        let faulted = self.faulted.load(Ordering::SeqCst)
            || self.pending.is_poisoned()
            || self.inner.is_poisoned();
        notice.poisoned.set(faulted);
        faulted
    }

    #[cfg(test)]
    fn run_progress_test_probe(&self, point: CausalTestPoint) {
        let probe = self
            .progress_test_probe
            .try_lock()
            .expect("test probe lock")
            .clone();
        if let Some(probe) = probe {
            probe(point);
        }
    }

    #[must_use]
    pub fn mint(&self) -> Option<u64> {
        self.mint_with_lease(None)
    }

    #[must_use]
    pub fn mint_with_lease(&self, identity: Option<LeaseIdentity>) -> Option<u64> {
        let notice = CausalUnlockNotice::new(self);
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut inner = self
            .causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, false, false)
            .ok()?;
        let mut identities = BTreeSet::new();
        let mut leases = 0;
        if let Some(identity) = identity {
            identities.insert(identity);
            leases = 1;
        }
        inner.scopes.insert(id, CausalScope { leases, identities });
        Some(id)
    }

    pub fn acquire(&self, scope_id: u64, identity: LeaseIdentity) -> bool {
        let notice = CausalUnlockNotice::new(self);
        let Ok(mut inner) =
            self.causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, false, false)
        else {
            return false;
        };
        let Some(scope) = inner.scopes.get_mut(&scope_id) else {
            return false;
        };
        if scope.identities.insert(identity) {
            scope.leases = scope.leases.saturating_add(1);
        }
        true
    }

    /// Replace one identity with zero or more identities under a single lock so
    /// the scope row cannot disappear between release and acquire.
    pub fn transfer(
        &self,
        scope_id: u64,
        from: LeaseIdentity,
        to: impl IntoIterator<Item = LeaseIdentity>,
    ) -> CausalAdmitResult {
        self.try_admit(CausalOp::Transfer {
            scope_id,
            from,
            to: to.into_iter().collect(),
        })
    }

    pub fn release(&self, scope_id: u64, identity: LeaseIdentity) -> CausalAdmitResult {
        self.try_admit(CausalOp::Release { scope_id, identity })
    }

    /// Undo one identity immediately when the caller still owns it.
    pub fn try_retract(&self, scope_id: u64, identity: LeaseIdentity) -> bool {
        let notice = CausalUnlockNotice::new(self);
        let Ok(mut inner) =
            self.causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, false, false)
        else {
            return false;
        };
        apply_causal_locked(&mut inner, &CausalOp::Release { scope_id, identity });
        true
    }

    /// Admit one causal op or return it. Never waits or spins.
    ///
    /// Later operations always append behind already-queued operations.
    pub fn try_admit(&self, op: CausalOp) -> CausalAdmitResult {
        match self.admit_causal(op, false) {
            CausalWaitResult::Applied => CausalAdmitResult::Applied,
            CausalWaitResult::Waiting(op) | CausalWaitResult::Fault(op) => {
                CausalAdmitResult::Retry(op)
            }
        }
    }

    /// Register the owner waiter before calling. Waiting never transfers operation ownership.
    pub(crate) fn try_admit_or_wait(&self, op: CausalOp) -> CausalWaitResult {
        self.admit_causal(op, true)
    }

    fn admit_causal(&self, op: CausalOp, wait: bool) -> CausalWaitResult {
        let notice = CausalUnlockNotice::new(self);
        if self.causal_fault(&notice) {
            return CausalWaitResult::Fault(op);
        }
        let mut pending =
            match self.causal_lock(&self.pending, &notice, CAUSAL_PENDING_UNLOCK, wait, false) {
                Ok(pending) => pending,
                Err(CausalLockError::Busy) => return CausalWaitResult::Waiting(op),
                Err(CausalLockError::Poisoned) => return CausalWaitResult::Fault(op),
            };
        if pending.is_empty() {
            match self.causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, false, false) {
                Ok(mut inner) => {
                    apply_causal_locked(&mut inner, &op);
                    return CausalWaitResult::Applied;
                }
                Err(CausalLockError::Poisoned) => return CausalWaitResult::Fault(op),
                Err(CausalLockError::Busy) => {}
            }
        }
        if pending.len() < CAUSAL_PENDING_MAX {
            notice.queued.set(pending.is_empty());
            pending.push_back(op);
            self.pending_len.fetch_add(1, Ordering::SeqCst);
            return CausalWaitResult::Applied;
        }
        if wait {
            self.interests.fetch_or(CAUSAL_CAPACITY, Ordering::SeqCst);
        }
        CausalWaitResult::Waiting(op)
    }

    /// Attempt at most one queued operation without waiting for either lock.
    pub fn flush_pending(&self) -> usize {
        let notice = CausalUnlockNotice::new(self);
        if self.causal_fault(&notice) || !self.pending_ops() {
            return 0;
        }
        let Ok(mut pending) =
            self.causal_lock(&self.pending, &notice, CAUSAL_PENDING_UNLOCK, true, true)
        else {
            return 0;
        };
        let Ok(mut inner) = self.causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, true, true)
        else {
            return 0;
        };
        let Some(op) = pending.pop_front() else {
            return 0;
        };
        self.pending_len.fetch_sub(1, Ordering::SeqCst);
        notice.capacity.set(true);
        apply_causal_locked(&mut inner, &op);
        1
    }

    #[must_use]
    pub fn pending_ops(&self) -> bool {
        self.pending_len.load(Ordering::SeqCst) > 0
    }

    /// A mutex unlock re-enables draining after contention. Poison requires owner recovery.
    #[must_use]
    pub(crate) fn pending_ready(&self) -> bool {
        self.pending_ops()
            && self.drain_blocked.load(Ordering::SeqCst) == 0
            && !self.faulted.load(Ordering::SeqCst)
            && !self.pending.is_poisoned()
            && !self.inner.is_poisoned()
    }

    #[doc(hidden)]
    pub fn test_with_inner_held<R>(&self, body: impl FnOnce() -> R) -> R {
        let notice = CausalUnlockNotice::new(self);
        let _guard = self
            .causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, false, false)
            .expect("test hold must acquire causal inner");
        body()
    }

    #[must_use]
    pub fn is_live(&self, scope_id: u64) -> bool {
        let notice = CausalUnlockNotice::new(self);
        match self.causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, false, false) {
            Ok(inner) => inner
                .scopes
                .get(&scope_id)
                .is_some_and(|scope| scope.leases > 0),
            Err(_) => true,
        }
    }

    #[must_use]
    pub fn lease_count(&self, scope_id: u64) -> Option<u32> {
        let notice = CausalUnlockNotice::new(self);
        self.causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, false, false)
            .ok()
            .and_then(|inner| inner.scopes.get(&scope_id).map(|scope| scope.leases))
    }

    #[must_use]
    pub fn pending_publish_leases(&self) -> Vec<(u64, LeaseIdentity)> {
        let notice = CausalUnlockNotice::new(self);
        let Ok(inner) = self.causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, false, false)
        else {
            return Vec::new();
        };
        inner
            .scopes
            .iter()
            .filter_map(|(scope_id, scope)| {
                scope.identities.iter().find_map(|identity| {
                    matches!(identity, LeaseIdentity::PendingEntityPublish { .. })
                        .then(|| (*scope_id, identity.clone()))
                })
            })
            .collect()
    }

    #[doc(hidden)]
    #[must_use]
    pub fn identities(&self, scope_id: u64) -> Option<BTreeSet<LeaseIdentity>> {
        let notice = CausalUnlockNotice::new(self);
        self.causal_lock(&self.inner, &notice, CAUSAL_INNER_UNLOCK, false, false)
            .ok()
            .and_then(|inner| {
                inner
                    .scopes
                    .get(&scope_id)
                    .map(|scope| scope.identities.clone())
            })
    }
}

fn apply_causal_locked(inner: &mut CausalInner, op: &CausalOp) {
    match op {
        CausalOp::Transfer { scope_id, from, to } => {
            if let Some(scope) = inner.scopes.get_mut(scope_id) {
                if scope.identities.remove(from) {
                    scope.leases = scope.leases.saturating_sub(1);
                }
                for identity in to {
                    if scope.identities.insert(identity.clone()) {
                        scope.leases = scope.leases.saturating_add(1);
                    }
                }
                if scope.leases == 0 {
                    inner.scopes.remove(scope_id);
                }
            }
        }
        CausalOp::Release { scope_id, identity } => {
            if let Some(scope) = inner.scopes.get_mut(scope_id) {
                if scope.identities.remove(identity) {
                    scope.leases = scope.leases.saturating_sub(1);
                }
                if scope.leases == 0 {
                    inner.scopes.remove(scope_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PackageEventPlaneOptions;
    use crate::subscription::package_events::ClientEventMailbox;
    use std::thread;
    use std::time::Duration as StdDuration;

    fn causal_test_op(sequence: u64) -> CausalOp {
        CausalOp::Release {
            scope_id: sequence,
            identity: LeaseIdentity::AdmittedEntityMutation {
                family: "producer.item".into(),
                generation: 4,
                seq: sequence,
            },
        }
    }

    fn assert_causal_locks_free(table: &CausalScopeTable) {
        let _pending = table
            .pending
            .try_lock()
            .expect("pending is unlocked before notification");
        let _inner = table
            .inner
            .try_lock()
            .expect("inner is unlocked before notification");
    }

    #[test]
    fn causal_progress_survives_binding_and_a_full_control_channel() {
        let table = CausalScopeTable::new();
        table.test_with_inner_held(|| {
            assert_eq!(
                table.try_admit(causal_test_op(1)),
                CausalAdmitResult::Applied
            );
        });
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(ControlMessage::CausalProgressPublished)
            .expect("fill channel");
        table.bind_owner_wake(sender.clone());
        table.bind_owner_wake(sender);
        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::CausalProgressPublished)
        ));
        assert!(
            receiver.try_recv().is_err(),
            "the doorbell never exceeds channel capacity"
        );
        assert!(
            table.take_progress_notification(),
            "the bit survives a refused doorbell"
        );
        assert!(!table.take_progress_notification());
        assert!(table.pending_ready());
        assert_eq!(table.flush_pending(), 1);
        table.test_with_inner_held(|| {
            assert_eq!(
                table.try_admit(causal_test_op(2)),
                CausalAdmitResult::Applied
            );
            assert_eq!(
                table.try_admit(causal_test_op(3)),
                CausalAdmitResult::Applied
            );
        });
        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::CausalProgressPublished)
        ));
        assert!(receiver.try_recv().is_err(), "progress coalesces");
        assert!(table.take_progress_notification());
    }

    #[test]
    fn causal_pending_unlock_before_registration_or_retry_cannot_lose_progress() {
        for release_point in [
            CausalTestPoint::BeforePendingArm,
            CausalTestPoint::BeforePendingRetry,
        ] {
            let table = Arc::new(CausalScopeTable::new());
            let (held_tx, held_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let worker_table = Arc::clone(&table);
            let worker = thread::spawn(move || {
                {
                    let notice = CausalUnlockNotice::new(&worker_table);
                    let _pending = worker_table
                        .causal_lock(
                            &worker_table.pending,
                            &notice,
                            CAUSAL_PENDING_UNLOCK,
                            false,
                            false,
                        )
                        .expect("hold pending");
                    held_tx.send(()).expect("holder started");
                    release_rx.recv().expect("release holder");
                }
                done_tx.send(()).expect("holder unlocked");
            });
            held_rx.recv().expect("pending held");
            let done_rx = Mutex::new(done_rx);
            let weak = Arc::downgrade(&table);
            *table.progress_test_probe.try_lock().unwrap() = Some(Arc::new(move |point| {
                if point == release_point {
                    release_tx.send(()).expect("release at the race boundary");
                    done_rx.lock().unwrap().recv().expect("unlock before retry");
                }
                if point == CausalTestPoint::BeforeNotify {
                    assert_causal_locks_free(&weak.upgrade().expect("table lives"));
                }
            }));
            assert_eq!(
                table.try_admit_or_wait(causal_test_op(1)),
                CausalWaitResult::Applied
            );
            worker.join().expect("holder joins");
            assert_eq!(table.pending_len.load(Ordering::SeqCst), 0);
            assert_eq!(table.interests.load(Ordering::SeqCst), 0);
            assert!(table.take_progress_notification());
        }
    }

    #[test]
    fn causal_pending_unlock_wakes_an_owned_waiter_after_both_attempts_fail() {
        let table = Arc::new(CausalScopeTable::new());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        table.bind_owner_wake(sender);
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_table = Arc::clone(&table);
        let worker = thread::spawn(move || {
            let notice = CausalUnlockNotice::new(&worker_table);
            let _pending = worker_table
                .causal_lock(
                    &worker_table.pending,
                    &notice,
                    CAUSAL_PENDING_UNLOCK,
                    false,
                    false,
                )
                .expect("hold pending");
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        held_rx.recv().unwrap();
        let op = causal_test_op(9);
        assert_eq!(
            table.try_admit_or_wait(op.clone()),
            CausalWaitResult::Waiting(op.clone())
        );
        assert!(!table.take_progress_notification());
        assert!(receiver.try_recv().is_err());
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::CausalProgressPublished)
        ));
        assert!(table.take_progress_notification());
        assert_eq!(table.try_admit_or_wait(op), CausalWaitResult::Applied);
        assert!(!table.take_progress_notification());
    }

    #[test]
    fn causal_full_queue_waits_for_capacity_and_rearms_after_a_competing_insert() {
        let table = Arc::new(CausalScopeTable::new());
        table.test_with_inner_held(|| {
            for sequence in 0..CAUSAL_PENDING_MAX {
                assert_eq!(
                    table.try_admit(causal_test_op(sequence as u64)),
                    CausalAdmitResult::Applied
                );
            }
        });
        assert!(table.take_progress_notification());
        let weak = Arc::downgrade(&table);
        *table.progress_test_probe.try_lock().unwrap() = Some(Arc::new(move |point| {
            if point == CausalTestPoint::BeforeNotify {
                assert_causal_locks_free(&weak.upgrade().expect("table lives"));
            }
        }));
        let waiting = causal_test_op(900);
        for _ in 0..3 {
            assert_eq!(
                table.try_admit_or_wait(waiting.clone()),
                CausalWaitResult::Waiting(waiting.clone())
            );
            assert!(
                !table.take_progress_notification(),
                "full refusal must not wake itself"
            );
        }
        assert_eq!(table.flush_pending(), 1);
        assert!(table.take_progress_notification());
        assert_eq!(
            table.try_admit(causal_test_op(901)),
            CausalAdmitResult::Applied
        );
        assert_eq!(
            table.try_admit_or_wait(waiting.clone()),
            CausalWaitResult::Waiting(waiting.clone())
        );
        assert!(!table.take_progress_notification());
        assert_eq!(table.flush_pending(), 1);
        assert!(
            table.take_progress_notification(),
            "the next free slot wakes the re-armed waiter"
        );
        assert_eq!(
            table.try_admit_or_wait(waiting.clone()),
            CausalWaitResult::Applied
        );
        let pending = table.pending.try_lock().unwrap();
        assert_eq!(
            pending.back(),
            Some(&waiting),
            "the admitted release keeps FIFO order"
        );
        assert_eq!(pending.len(), CAUSAL_PENDING_MAX);
    }

    #[test]
    fn causal_inner_unlock_resumes_blocked_flush_before_capacity_admission() {
        let table = CausalScopeTable::new();
        let waiting = causal_test_op(900);
        table.test_with_inner_held(|| {
            for sequence in 0..CAUSAL_PENDING_MAX {
                assert_eq!(
                    table.try_admit(causal_test_op(sequence as u64)),
                    CausalAdmitResult::Applied
                );
            }
            assert!(table.take_progress_notification());
            assert_eq!(
                table.try_admit_or_wait(waiting.clone()),
                CausalWaitResult::Waiting(waiting.clone())
            );
            assert_eq!(table.flush_pending(), 0);
            assert!(table.pending_ops());
            assert!(!table.pending_ready(), "the owner parks until inner unlock");
            assert!(
                !table.take_progress_notification(),
                "a failed flush must not poll itself"
            );
        });
        assert!(
            table.take_progress_notification(),
            "inner unlock wakes flushing"
        );
        assert!(table.pending_ready());
        assert_eq!(table.flush_pending(), 1);
        assert!(
            table.take_progress_notification(),
            "the pop wakes the capacity waiter"
        );
        assert_eq!(table.try_admit_or_wait(waiting), CausalWaitResult::Applied);
    }

    #[test]
    fn causal_pending_unlock_resumes_blocked_flush() {
        let table = CausalScopeTable::new();
        table.test_with_inner_held(|| {
            assert_eq!(
                table.try_admit(causal_test_op(1)),
                CausalAdmitResult::Applied
            );
        });
        assert!(table.take_progress_notification());
        {
            let notice = CausalUnlockNotice::new(&table);
            let _pending = table
                .causal_lock(&table.pending, &notice, CAUSAL_PENDING_UNLOCK, false, false)
                .unwrap();
            assert_eq!(table.flush_pending(), 0);
            assert!(!table.pending_ready());
            assert!(!table.take_progress_notification());
        }
        assert!(table.take_progress_notification());
        assert!(table.pending_ready());
        assert_eq!(table.flush_pending(), 1);
        assert!(!table.pending_ready());
    }

    #[test]
    fn causal_inner_readers_and_early_returns_publish_only_after_unlock() {
        let table = Arc::new(CausalScopeTable::new());
        let weak = Arc::downgrade(&table);
        *table.progress_test_probe.try_lock().unwrap() = Some(Arc::new(move |point| {
            if point == CausalTestPoint::BeforeNotify {
                assert_causal_locks_free(&weak.upgrade().unwrap());
            }
        }));
        let actions: &[fn(&CausalScopeTable)] = &[
            |table| {
                let _ = table.mint();
            },
            |table| {
                assert!(!table.acquire(
                    u64::MAX,
                    LeaseIdentity::EventInFlight {
                        request_id: "absent".into()
                    }
                ));
            },
            |table| {
                let _ = table.try_retract(
                    1,
                    LeaseIdentity::EventInFlight {
                        request_id: "absent".into(),
                    },
                );
            },
            |table| {
                let _ = table.is_live(1);
            },
            |table| {
                let _ = table.lease_count(1);
            },
            |table| {
                let _ = table.pending_publish_leases();
            },
            |table| {
                let _ = table.identities(1);
            },
            |table| {
                table.test_with_inner_held(|| {});
            },
        ];
        for action in actions {
            table.interests.store(CAUSAL_INNER_UNLOCK, Ordering::SeqCst);
            table
                .drain_blocked
                .store(CAUSAL_INNER_UNLOCK, Ordering::SeqCst);
            action(&table);
            assert!(table.take_progress_notification());
            assert_eq!(table.drain_blocked.load(Ordering::SeqCst), 0);
            assert_eq!(table.interests.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn causal_poison_returns_the_owned_operation_and_publishes_one_fault() {
        for kind in [CAUSAL_PENDING_UNLOCK, CAUSAL_INNER_UNLOCK] {
            let table = CausalScopeTable::new();
            table.test_with_inner_held(|| {
                assert_eq!(
                    table.try_admit(causal_test_op(1)),
                    CausalAdmitResult::Applied
                );
            });
            assert!(table.take_progress_notification());
            let fault = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let notice = CausalUnlockNotice::new(&table);
                if kind == CAUSAL_PENDING_UNLOCK {
                    let _pending = table
                        .causal_lock(&table.pending, &notice, kind, false, false)
                        .unwrap();
                    panic!("poison pending");
                } else {
                    let _inner = table
                        .causal_lock(&table.inner, &notice, kind, false, false)
                        .unwrap();
                    panic!("poison inner");
                }
            }));
            assert!(fault.is_err());
            assert!(
                table.take_progress_notification(),
                "poison wakes retained owner work"
            );
            let op = causal_test_op(2);
            assert_eq!(
                table.try_admit_or_wait(op.clone()),
                CausalWaitResult::Fault(op.clone())
            );
            assert_eq!(table.try_admit(op.clone()), CausalAdmitResult::Retry(op));
            assert!(!table.pending_ready());
            assert_eq!(table.flush_pending(), 0);
            assert_eq!(
                table.pending_len.load(Ordering::SeqCst),
                1,
                "fault preserves admitted backlog"
            );
            assert!(
                !table.take_progress_notification(),
                "a persistent fault does not poll"
            );
        }
    }

    fn router() -> PackageEventRouter {
        PackageEventRouter::new(PackageEventPlanePolicy::default())
    }

    fn sample_contract(owner: &str, name: &str) -> EmittedContract {
        EmittedContract {
            owner: owner.to_string(),
            name: name.to_string(),
            audience: BTreeSet::from([EventAudience::Plugins]),
            schema: CompiledEventSchema::compile(&serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": { "ok": { "type": "boolean" } },
                "required": ["ok"]
            }))
            .expect("sample schema"),
            package_generation: 0,
        }
    }

    fn subscribe(router: &PackageEventRouter, plugin: &str, owner: &str, name: &str) {
        assert_eq!(
            router.try_subscribe(EventSubscription {
                plugin_key: plugin.to_string(),
                owner: owner.to_string(),
                name: name.to_string(),
                handler_id: format!("event:{owner}:{name}"),
                generation: 1,
                ..EventSubscription::default()
            }),
            EventPlaneStatus::Accepted
        );
    }

    fn run_work(router: &PackageEventRouter, work: EventOwnerWork) -> EventOwnerCompletion {
        let completion = thread::scope(|scope| {
            scope
                .spawn(move || work.run(router))
                .join()
                .expect("worker joins")
                .expect("worker completes")
        });
        assert_router_accounting(router);
        completion
    }

    fn run_unload(router: &PackageEventRouter, owner: &str, generation: u64) {
        let OwnerApplyResult::Work(work) = router.try_apply(&OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: owner.into(),
            generation,
        }) else {
            panic!("an unload must return worker ownership");
        };
        run_work(router, work);
    }

    fn apply_owner_step(ops: &mut EventPlaneOwnerOps, router: &PackageEventRouter) -> Vec<OwnerOp> {
        match ops.apply_ready(router) {
            OwnerStep::Applied(op) => vec![op],
            OwnerStep::Work(work) => {
                let identity = work.identity().clone();
                assert_eq!(
                    ops.pending_for(&identity.operation.owner).first(),
                    Some(&identity.operation)
                );
                let completion = run_work(router, work);
                ops.complete(completion).into_iter().collect()
            }
            OwnerStep::Idle | OwnerStep::Waiting => Vec::new(),
        }
    }

    fn assert_router_accounting(router: &PackageEventRouter) {
        let inner = lock_inner(&router.inner).expect("accounting lock");
        assert_router_indexes(&inner);
        let bytes: usize = inner.envelopes.values().map(|envelope| envelope.size).sum();
        assert_eq!(inner.global_bytes(), bytes);
        assert_eq!(
            router.counters.snapshot().global_in_flight_bytes,
            bytes as u64
        );
        for (owner, occupancy) in &inner.producer {
            let envelopes: Vec<_> = inner
                .envelopes
                .values()
                .filter(|envelope| &envelope.owner == owner)
                .collect();
            assert_eq!(occupancy.events, envelopes.len(), "producer {owner}");
            assert_eq!(
                occupancy.bytes,
                envelopes
                    .iter()
                    .map(|envelope| envelope.size)
                    .sum::<usize>()
            );
        }
        for (consumer, queue) in &inner.consumers {
            assert_eq!(queue.events, queue.copies.len(), "consumer {consumer}");
            assert_eq!(
                queue.bytes,
                queue
                    .copies
                    .iter()
                    .map(|copy| {
                        inner
                            .live_envelope(copy.envelope_id)
                            .expect("queued envelope is live")
                            .size
                    })
                    .sum::<usize>()
            );
        }
        assert!(
            inner
                .admitted
                .keys()
                .all(|id| inner.envelopes.contains_key(id))
        );
        assert!(inner.admitted.values().all(|holders| !holders.is_empty()));
    }

    fn assert_router_indexes(inner: &RouterInner) {
        let mut names = HashMap::new();
        for events in inner.contracts.values() {
            for name in events.keys() {
                *names.entry(name.clone()).or_insert(0) += 1;
            }
        }
        assert_eq!(inner.contract_name_counts, names);
        let ready: BTreeSet<String> = inner
            .consumers
            .iter()
            .filter(|(_, queue)| !queue.copies.is_empty())
            .map(|(consumer, _)| consumer.clone())
            .collect();
        assert_eq!(inner.ready_consumers, ready);
        let mut subscriptions: HashMap<String, HashMap<(String, String), usize>> = HashMap::new();
        let mut subscription_counts: HashMap<String, usize> = HashMap::new();
        for (owner, events) in &inner.subscriptions {
            for (name, holders) in events {
                for holder in holders {
                    assert_eq!(&holder.owner, owner);
                    assert_eq!(&holder.name, name);
                    *subscriptions
                        .entry(holder.plugin_key.clone())
                        .or_default()
                        .entry((owner.clone(), name.clone()))
                        .or_default() += 1;
                    *subscription_counts
                        .entry(holder.plugin_key.clone())
                        .or_default() += 1;
                }
            }
        }
        assert_eq!(inner.subscription_events_by_plugin, subscriptions);
        assert_eq!(inner.subscriptions_per_plugin, subscription_counts);
        let mut queued: HashMap<String, HashMap<String, usize>> = HashMap::new();
        for (consumer, queue) in &inner.consumers {
            for copy in &queue.copies {
                assert_eq!(&copy.holder.plugin_key, consumer);
                *queued
                    .entry(copy.holder.owner.clone())
                    .or_default()
                    .entry(consumer.clone())
                    .or_default() += 1;
            }
        }
        assert_eq!(inner.queued_by_producer, queued);
        let mut retiring: HashMap<(String, u64), HashSet<u64>> = HashMap::new();
        for (id, envelope) in &inner.envelopes {
            if let Some(retirement) = &envelope.retirement {
                assert_eq!(envelope.remaining_holders, 0);
                assert!(envelope.payload.is_empty());
                assert!(envelope.payload_json.is_null());
                retiring
                    .entry(retirement.cleanup.clone())
                    .or_default()
                    .insert(*id);
            }
        }
        assert_eq!(inner.retiring_by_cleanup, retiring);
    }

    #[test]
    fn contract_name_membership_tracks_shared_names_and_generations() {
        let router = router();
        router
            .try_register_contracts(vec![
                sample_contract("first", "shared"),
                sample_contract("second", "shared"),
            ])
            .expect("shared contracts");
        router
            .try_register_contracts(vec![sample_contract("first", "shared")])
            .expect("overwrite");
        {
            let inner = lock_inner(&router.inner).expect("names");
            assert_router_indexes(&inner);
            assert_eq!(inner.contract_name_counts["shared"], 2);
        }
        run_unload(&router, "first", 1);
        run_unload(&router, "second", 1);
        {
            let inner = lock_inner(&router.inner).expect("surviving name");
            assert_router_indexes(&inner);
            assert_eq!(inner.contract_name_counts["shared"], 1);
        }
        assert_eq!(
            router.try_ingress(
                "outsider",
                "shared",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::RejectedForeign
        );
        assert!(
            !lock_inner(&router.inner)
                .expect("no token charge")
                .buckets
                .contains_key("outsider")
        );
        run_unload(&router, "first", 2);
        assert_eq!(
            router.try_ingress(
                "outsider",
                "shared",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::RejectedUndeclared
        );
        let inner = lock_inner(&router.inner).expect("last name removed");
        assert_router_indexes(&inner);
        assert!(!inner.contract_name_counts.contains_key("shared"));
        assert!(!inner.buckets.contains_key("outsider"));
    }

    #[test]
    fn ready_selection_ignores_empty_history_and_rotates_partial_queues() {
        for historical_count in [0, 1024] {
            let router = router();
            router
                .try_register_contracts(vec![sample_contract("producer", "ready")])
                .expect("contract");
            subscribe(&router, "a", "producer", "ready");
            subscribe(&router, "b", "producer", "ready");
            {
                let mut inner = lock_inner(&router.inner).expect("historical queues");
                for index in 0..historical_count {
                    inner
                        .consumers
                        .insert(format!("history-{index}"), ConsumerQueue::default());
                }
            }
            for _ in 0..2 {
                assert_eq!(
                    router.try_ingress(
                        "producer",
                        "ready",
                        &serde_json::json!({"ok": true}),
                        Instant::now()
                    ),
                    EventPlaneStatus::Accepted
                );
            }
            for (items, elapsed) in [(0, StdDuration::from_secs(1)), (1, StdDuration::ZERO)] {
                assert!(
                    router
                        .pull_ready_batch(items, usize::MAX, Instant::now(), elapsed)
                        .expect("budget guard")
                        .is_empty()
                );
                let inner = lock_inner(&router.inner).expect("no selection");
                assert_eq!((inner.ready_key_visits, inner.ready_key_clones), (0, 0));
                assert_eq!(inner.last_ready_consumer, None);
            }
            let mut ids = HashMap::new();
            for expected in ["a", "b", "a", "b"] {
                let delivery = router
                    .pull_ready_batch(1, usize::MAX, Instant::now(), StdDuration::from_secs(1))
                    .expect("one delivery")
                    .pop()
                    .expect("ready copy");
                assert_eq!(delivery.holder.plugin_key, expected);
                if let Some(previous) = ids.insert(expected, delivery.envelope_id) {
                    assert!(
                        previous < delivery.envelope_id,
                        "each consumer keeps FIFO order"
                    );
                }
                {
                    let inner = lock_inner(&router.inner).expect("selection cost");
                    assert_eq!((inner.ready_key_visits, inner.ready_key_clones), (1, 2));
                }
                router.complete_pulled_delivery(delivery).expect("complete");
                assert_router_accounting(&router);
            }
            assert!(
                lock_inner(&router.inner)
                    .expect("all drained")
                    .ready_consumers
                    .is_empty()
            );
        }
    }

    #[test]
    fn readiness_preserves_front_requeue_and_byte_budget_put_back() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "ready")])
            .expect("contract");
        subscribe(&router, "consumer", "producer", "ready");
        for _ in 0..2 {
            assert_eq!(
                router.try_ingress(
                    "producer",
                    "ready",
                    &serde_json::json!({"ok": true}),
                    Instant::now()
                ),
                EventPlaneStatus::Accepted
            );
        }
        let first = router
            .pull_ready_batch(8, 1, Instant::now(), StdDuration::from_secs(1))
            .expect("first item exceeds byte budget")
            .pop()
            .expect("first delivery");
        let first_id = first.envelope_id;
        assert_router_accounting(&router);
        router.requeue_delivery(first).expect("front requeue");
        assert_router_accounting(&router);
        let batch = router
            .pull_ready_batch(8, usize::MAX, Instant::now(), StdDuration::from_secs(1))
            .expect("drain");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].envelope_id, first_id);
        assert!(batch[1].envelope_id > first_id);
        assert_router_accounting(&router);
        for delivery in batch {
            router.complete_pulled_delivery(delivery).expect("complete");
        }
        assert_router_accounting(&router);
    }

    #[test]
    fn ready_selection_visits_each_consumer_once_when_byte_blocked() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "ready")])
            .expect("contract");
        for consumer in ["a", "b", "c"] {
            subscribe(&router, consumer, "producer", "ready");
        }
        assert_eq!(
            router.try_ingress(
                "producer",
                "ready",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let batch = router
            .pull_ready_batch(8, 1, Instant::now(), StdDuration::from_secs(1))
            .expect("byte limited");
        assert_eq!(batch.len(), 1);
        {
            let inner = lock_inner(&router.inner).expect("selection cost");
            assert_eq!((inner.ready_key_visits, inner.ready_key_clones), (3, 6));
        }
        assert_router_accounting(&router);
        for delivery in batch {
            router.complete_pulled_delivery(delivery).expect("complete");
        }
    }

    #[test]
    fn unload_visits_do_not_grow_with_unrelated_owners() {
        let mut visits = Vec::new();
        for unrelated_count in [0, 64] {
            let router = router();
            let mut owners = vec!["target".to_string(), "remote".to_string()];
            owners.extend((0..unrelated_count).map(|index| format!("unrelated-{index}")));
            let contracts = owners
                .iter()
                .map(|owner| {
                    let mut contract = sample_contract(owner, "ready");
                    contract.audience.insert(EventAudience::Clients);
                    contract
                })
                .collect();
            router.try_register_contracts(contracts).expect("contracts");
            subscribe(&router, "target", "target", "ready");
            subscribe(&router, "shared", "target", "ready");
            subscribe(&router, "target", "remote", "ready");
            for owner in &owners {
                if owner.starts_with("unrelated-") {
                    subscribe(&router, &format!("consumer-{owner}"), owner, "ready");
                }
                if owner != "remote" {
                    let mailbox = Arc::new(ClientEventMailbox::new(router.policy()));
                    let gap = mailbox
                        .register_gap_slot("sub", owner, "ready")
                        .expect("gap");
                    assert_eq!(
                        router.try_subscribe_client(ClientEventHolder {
                            connection_id: owner.clone(),
                            subscription_id: "sub".into(),
                            owner: owner.clone(),
                            name: "ready".into(),
                            subjects: BTreeSet::new(),
                            mailbox,
                            gap,
                        }),
                        EventPlaneStatus::Accepted
                    );
                }
                assert_eq!(
                    router.try_ingress(
                        owner,
                        "ready",
                        &serde_json::json!({"ok": true}),
                        Instant::now()
                    ),
                    EventPlaneStatus::Accepted
                );
            }
            assert_router_accounting(&router);
            run_unload(&router, "target", 1);
            let inner = lock_inner(&router.inner).expect("cleanup visits");
            visits.push(inner.cleanup_visits);
            assert_eq!(inner.envelopes.len(), unrelated_count);
            assert!(!inner.subscription_events_by_plugin.contains_key("target"));
            assert!(!inner.queued_by_producer.contains_key("target"));
            assert!(
                !inner
                    .client_by_id
                    .contains_key(&("target".into(), "sub".into()))
            );
        }
        assert_eq!(visits[0], visits[1]);
        assert_eq!(
            visits[0],
            CleanupVisits {
                contracts: 1,
                subscriptions: 3,
                clients: 1,
                queues: 2,
                copies: 3,
                retiring: 2,
            }
        );
    }

    #[test]
    fn unload_retains_mixed_queue_order_at_the_configured_limit() {
        let router = PackageEventRouter::new(PackageEventPlanePolicy {
            consumer_queue_max_events: 6,
            ..PackageEventPlanePolicy::default()
        });
        router
            .try_register_contracts(vec![
                sample_contract("removed", "ready"),
                sample_contract("kept", "ready"),
            ])
            .expect("contracts");
        subscribe(&router, "consumer", "removed", "ready");
        subscribe(&router, "consumer", "kept", "ready");
        let owners = ["removed", "kept", "removed", "kept", "removed", "kept"];
        for owner in owners {
            assert_eq!(
                router.try_ingress(
                    owner,
                    "ready",
                    &serde_json::json!({"ok": true}),
                    Instant::now()
                ),
                EventPlaneStatus::Accepted
            );
        }
        let expected = {
            let inner = lock_inner(&router.inner).expect("original order");
            inner.consumers["consumer"]
                .copies
                .iter()
                .filter(|copy| copy.holder.owner == "kept")
                .map(|copy| copy.envelope_id)
                .collect::<Vec<_>>()
        };
        run_unload(&router, "removed", 1);
        let inner = lock_inner(&router.inner).expect("surviving order");
        assert_eq!(inner.cleanup_visits.copies, 6);
        assert_eq!(
            inner.consumers["consumer"]
                .copies
                .iter()
                .map(|copy| copy.envelope_id)
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn producer_unload_finds_a_copy_requeued_after_subscription_removal() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "ready")])
            .expect("contract");
        subscribe(&router, "consumer", "producer", "ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "ready",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let mut batch = router
            .pull_ready_batch(1, usize::MAX, Instant::now(), StdDuration::from_secs(1))
            .expect("pull");
        let delivery = batch.pop().expect("delivery");
        run_unload(&router, "consumer", 0);
        assert_eq!(router.test_subscription_count("consumer"), 0);
        router
            .requeue_delivery(delivery)
            .expect("requeue the outstanding live delivery");
        assert_router_accounting(&router);
        assert_eq!(router.snapshot().expect("queued again").queued_holders, 1);
        run_unload(&router, "producer", 1);
        assert_eq!(
            router.snapshot().expect("retired").global_in_flight_bytes,
            0
        );
        assert!(
            lock_inner(&router.inner)
                .expect("membership")
                .queued_by_producer
                .is_empty()
        );
        router.take_delivery_wake();
        assert!(
            router
                .pull_ready_batch(1, usize::MAX, Instant::now(), StdDuration::from_secs(1))
                .expect("empty pull")
                .is_empty()
        );
        assert!(!router.take_delivery_wake());
    }

    #[test]
    fn projected_fanout_enforces_event_and_byte_capacity_per_consumer() {
        let payload = serde_json::json!({"ok": true});
        let size = serde_json::to_vec(&payload).expect("payload").len();
        for bytes_limit in [false, true] {
            let router = PackageEventRouter::new(PackageEventPlanePolicy {
                consumer_queue_max_events: if bytes_limit { 10 } else { 2 },
                consumer_queue_max_bytes: if bytes_limit { size * 2 } else { size * 10 },
                ..PackageEventPlanePolicy::default()
            });
            router
                .try_register_contracts(vec![sample_contract("producer", "ready")])
                .expect("contract");
            subscribe(&router, "constrained", "producer", "ready");
            assert_eq!(
                router.try_ingress("producer", "ready", &payload, Instant::now()),
                EventPlaneStatus::Accepted
            );
            assert_eq!(
                router.try_subscribe(EventSubscription {
                    plugin_key: "constrained".into(),
                    owner: "producer".into(),
                    name: "ready".into(),
                    handler_id: "second".into(),
                    generation: 2,
                    ..EventSubscription::default()
                }),
                EventPlaneStatus::Accepted
            );
            subscribe(&router, "unaffected", "producer", "ready");
            assert_eq!(
                router.try_ingress("producer", "ready", &payload, Instant::now()),
                EventPlaneStatus::Accepted
            );
            assert_router_accounting(&router);
            let snapshot = router.snapshot().expect("partial fanout");
            assert_eq!(snapshot.consumer_events["constrained"], 2);
            assert_eq!(snapshot.consumer_bytes["constrained"], size * 2);
            assert_eq!(snapshot.consumer_events["unaffected"], 1);
            assert_eq!(snapshot.queued_holders, 3);
            assert_eq!(snapshot.producer_events["producer"], 2);
            assert_eq!(snapshot.global_in_flight_bytes, size * 2);
            assert_eq!(
                router.try_ingress("producer", "ready", &payload, Instant::now()),
                EventPlaneStatus::Accepted
            );
            assert_router_accounting(&router);
            assert_eq!(
                router.snapshot().expect("unaffected fills").consumer_events["unaffected"],
                2
            );
            assert_eq!(
                router.try_ingress("producer", "ready", &payload, Instant::now()),
                EventPlaneStatus::ShedFull
            );
            assert_router_accounting(&router);
        }
    }

    #[test]
    fn failed_commit_restores_subscription_membership_after_partial_admission() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "ready")])
            .expect("contract");
        subscribe(&router, "existing", "producer", "ready");
        let subscription = EventSubscription {
            plugin_key: "new-consumer".into(),
            owner: "producer".into(),
            name: "ready".into(),
            handler_id: "valid".into(),
            generation: 2,
            ..EventSubscription::default()
        };
        let missing = EventSubscription {
            name: "missing".into(),
            ..subscription.clone()
        };
        assert_eq!(
            router.try_commit_package_generation(
                "replacement",
                Vec::new(),
                vec![subscription, missing]
            ),
            Err(EventPlaneStatus::RejectedUndeclared)
        );
        assert_router_accounting(&router);
        assert_eq!(router.test_subscription_count("existing"), 1);
        assert_eq!(router.test_subscription_count("new-consumer"), 0);
        assert_eq!(
            router
                .current_package_generation("replacement")
                .expect("rolled back generation"),
            0
        );
        run_unload(&router, "new-consumer", 0);
        assert_eq!(router.test_subscription_count("existing"), 1);
    }

    #[test]
    fn scoped_rollback_restores_overwritten_and_absent_keys_in_original_order() {
        let router = router();
        router
            .try_register_contracts(vec![
                sample_contract("owner", "ready"),
                sample_contract("owner", "untouched"),
                sample_contract("remote", "ready"),
            ])
            .expect("contracts");
        subscribe(&router, "existing", "owner", "ready");
        subscribe(&router, "peer", "owner", "ready");
        subscribe(&router, "existing", "remote", "ready");
        let original = lock_inner(&router.inner)
            .expect("original state")
            .subscriptions
            .clone();
        let mut overwritten = sample_contract("owner", "ready");
        overwritten.audience.insert(EventAudience::Clients);
        let first_write = sample_contract("owner", "ready");
        let mut proposed = Vec::new();
        for (index, (plugin, owner, name)) in [
            ("existing", "owner", "ready"),
            ("existing", "owner", "ready"),
            ("new-consumer", "owner", "new"),
            ("new-consumer", "remote", "ready"),
            ("new-consumer", "owner", "missing"),
        ]
        .into_iter()
        .enumerate()
        {
            proposed.push(EventSubscription {
                plugin_key: plugin.into(),
                owner: owner.into(),
                name: name.into(),
                handler_id: format!("proposed-{index}"),
                generation: index as u64 + 10,
                ..EventSubscription::default()
            });
        }
        assert_eq!(
            router.try_commit_package_generation(
                "owner",
                vec![first_write, overwritten, sample_contract("owner", "new")],
                proposed
            ),
            Err(EventPlaneStatus::RejectedUndeclared)
        );
        assert_router_accounting(&router);
        let inner = lock_inner(&router.inner).expect("restored state");
        assert_eq!(inner.subscriptions, original);
        assert_eq!(inner.package_generation["owner"], 1);
        assert_eq!(inner.contracts["owner"].len(), 2);
        assert_eq!(inner.contracts["owner"]["ready"].package_generation, 1);
        assert_eq!(
            inner.contracts["owner"]["ready"].audience,
            BTreeSet::from([EventAudience::Plugins])
        );
        assert!(inner.contracts["owner"].contains_key("untouched"));
        assert!(!inner.subscriptions_per_plugin.contains_key("new-consumer"));
        assert!(
            !inner
                .subscription_events_by_plugin
                .contains_key("new-consumer")
        );
        assert_eq!(
            inner.snapshot_visits,
            SnapshotVisits {
                generations: 1,
                contracts: 2,
                event_buckets: 4,
                subscriptions: 3,
                plugins: 2,
                memberships: 2,
            }
        );
    }

    #[test]
    fn scoped_rollback_preserves_absent_and_present_empty_owner_maps() {
        for was_present in [false, true] {
            let router = router();
            if was_present {
                let mut inner = lock_inner(&router.inner).expect("empty owner state");
                inner.contracts.insert("owner".into(), HashMap::new());
                inner.subscriptions.insert("owner".into(), HashMap::new());
                inner.package_generation.insert("owner".into(), 0);
            }
            let valid = EventSubscription {
                plugin_key: "consumer".into(),
                owner: "owner".into(),
                name: "ready".into(),
                handler_id: "valid".into(),
                generation: 1,
                ..EventSubscription::default()
            };
            let invalid = EventSubscription {
                name: "missing".into(),
                ..valid.clone()
            };
            assert_eq!(
                router.try_commit_package_generation(
                    "owner",
                    vec![sample_contract("owner", "ready")],
                    vec![valid, invalid]
                ),
                Err(EventPlaneStatus::RejectedUndeclared)
            );
            assert_router_accounting(&router);
            let inner = lock_inner(&router.inner).expect("restored empty state");
            assert_eq!(
                inner.contracts.get("owner").map(HashMap::len),
                was_present.then_some(0)
            );
            assert_eq!(
                inner.subscriptions.get("owner").map(HashMap::len),
                was_present.then_some(0)
            );
            assert_eq!(
                inner.package_generation.get("owner").copied(),
                was_present.then_some(0)
            );
            assert!(!inner.subscription_events_by_plugin.contains_key("consumer"));
        }
    }

    #[test]
    fn replacement_preview_and_commit_agree_at_subscription_boundaries() {
        for case in [
            "plugin-at-limit",
            "plugin-over-limit",
            "fanout-at-limit",
            "fanout-over-limit",
            "consumer-removed",
            "last-contract-wins",
            "last-contract-rejects",
            "removed-contract",
        ] {
            let router = PackageEventRouter::new(PackageEventPlanePolicy {
                subscriptions_per_plugin_max: 2,
                subscribers_per_event_max: 2,
                fanout_per_emit_max: 2,
                ..PackageEventPlanePolicy::default()
            });
            router
                .try_register_contracts(vec![
                    sample_contract("owner", "ready"),
                    sample_contract("remote", "ready"),
                ])
                .expect("contracts");
            subscribe(&router, "limited", "owner", "ready");
            subscribe(&router, "limited", "remote", "ready");
            subscribe(&router, "owner", "remote", "ready");
            assert_eq!(
                router.try_ingress(
                    "owner",
                    "ready",
                    &serde_json::json!({"ok": true}),
                    Instant::now()
                ),
                EventPlaneStatus::Accepted
            );
            let mut contracts = vec![sample_contract("owner", "ready")];
            let mut targets = vec![("limited", "owner")];
            let expected = match case {
                "plugin-over-limit" => {
                    targets.push(("limited", "owner"));
                    Err(EventPlaneStatus::RejectedInvalid)
                }
                "fanout-at-limit" => {
                    targets = vec![("one", "owner"), ("two", "owner")];
                    Ok(())
                }
                "fanout-over-limit" => {
                    targets = vec![("one", "owner"), ("two", "owner"), ("three", "owner")];
                    Err(EventPlaneStatus::RejectedOverFanout)
                }
                "consumer-removed" => {
                    targets = vec![("owner", "remote")];
                    Ok(())
                }
                "last-contract-wins" => {
                    let mut clients = sample_contract("owner", "ready");
                    clients.audience = BTreeSet::from([EventAudience::Clients]);
                    contracts.insert(0, clients);
                    Ok(())
                }
                "last-contract-rejects" => {
                    let mut clients = sample_contract("owner", "ready");
                    clients.audience = BTreeSet::from([EventAudience::Clients]);
                    contracts.push(clients);
                    Err(EventPlaneStatus::RejectedAudience)
                }
                "removed-contract" => {
                    contracts.clear();
                    Err(EventPlaneStatus::RejectedUndeclared)
                }
                _ => Ok(()),
            };
            let subscriptions = targets
                .into_iter()
                .enumerate()
                .map(|(index, (plugin, owner))| EventSubscription {
                    plugin_key: plugin.into(),
                    owner: owner.into(),
                    name: "ready".into(),
                    handler_id: format!("replacement-{index}"),
                    generation: index as u64 + 10,
                    ..EventSubscription::default()
                })
                .collect::<Vec<_>>();
            let preview = {
                let mut inner = lock_inner(&router.inner).expect("preview");
                preview_package_replacement(&mut inner, "owner", &contracts, &subscriptions)
            };
            assert_eq!(preview, expected, "{case}");
            let actual = thread::scope(|scope| {
                scope
                    .spawn(|| {
                        router.try_replace_package_generation("owner", contracts, subscriptions)
                    })
                    .join()
                    .expect("replacement worker")
            })
            .map(|_| ())
            .map_err(|error| {
                let (result, cleanup) = error.into_parts();
                assert!(cleanup.is_none());
                result.expect_err("admission rejection")
            });
            assert_eq!(actual, expected, "{case}");
            assert_router_accounting(&router);
            let inner = lock_inner(&router.inner).expect("generation after replacement");
            if expected.is_err() {
                assert_eq!(inner.package_generation["owner"], 1);
                assert_eq!(inner.envelopes.len(), 1);
            } else {
                assert_eq!(inner.package_generation["owner"], 2);
                assert!(inner.envelopes.is_empty());
            }
        }
    }

    #[test]
    fn replacement_visits_count_cloned_memberships_without_scanning_unrelated_owners() {
        let mut visits = Vec::new();
        for unrelated in [0, 64] {
            let router = router();
            let mut contracts = vec![sample_contract("owner", "ready")];
            for name in ["one", "two", "three"] {
                contracts.push(sample_contract("remote", name));
            }
            for index in 0..unrelated {
                contracts.push(sample_contract(&format!("unrelated-{index}"), "ready"));
            }
            router.try_register_contracts(contracts).expect("contracts");
            subscribe(&router, "limited", "owner", "ready");
            for name in ["one", "two", "three"] {
                subscribe(&router, "limited", "remote", name);
            }
            subscribe(&router, "owner", "remote", "one");
            for index in 0..unrelated {
                subscribe(
                    &router,
                    &format!("consumer-{index}"),
                    &format!("unrelated-{index}"),
                    "ready",
                );
            }
            let contracts = vec![
                sample_contract("owner", "ready"),
                sample_contract("owner", "ready"),
            ];
            let subscriptions = [("limited", "owner", "ready"), ("owner", "remote", "one")]
                .into_iter()
                .enumerate()
                .map(|(index, (plugin, owner, name))| EventSubscription {
                    plugin_key: plugin.into(),
                    owner: owner.into(),
                    name: name.into(),
                    handler_id: format!("new-{index}"),
                    generation: index as u64 + 10,
                    ..EventSubscription::default()
                })
                .collect();
            thread::scope(|scope| {
                scope
                    .spawn(|| {
                        router.try_replace_package_generation("owner", contracts, subscriptions)
                    })
                    .join()
                    .expect("worker")
                    .expect("replacement");
            });
            assert_router_accounting(&router);
            let inner = lock_inner(&router.inner).expect("visit counts");
            visits.push((inner.preview_visits, inner.snapshot_visits));
        }
        assert_eq!(visits[0], visits[1]);
        assert_eq!(
            visits[0],
            (
                PreviewVisits {
                    contracts: 2,
                    event_buckets: 2,
                    subscriptions: 3,
                    proposals: 2
                },
                SnapshotVisits {
                    generations: 1,
                    contracts: 1,
                    event_buckets: 2,
                    subscriptions: 1,
                    plugins: 2,
                    memberships: 3,
                }
            )
        );
    }

    #[test]
    fn router_module_forbids_hub_runtime_and_blocking_lock() {
        let source = include_str!("package_event_router.rs");
        let production = source.split("mod tests").next().unwrap_or(source);
        for needle in [
            "crate::HubRuntime",
            "crate::runtime::",
            "botster_core_daemon",
            "mlua::",
            "crate::persistence",
        ] {
            assert!(
                !production.contains(needle),
                "router must not import {needle}"
            );
        }
        let run_start = production.find("    pub fn run(\n").expect("worker method");
        let run_end = production[run_start..]
            .find("\n#[derive(Debug, Clone, PartialEq, Eq, Default)]")
            .map(|end| run_start + end)
            .expect("worker method ends before subscriptions");
        let worker = &production[run_start..run_end];
        assert_eq!(worker.matches("router.inner.lock()").count(), 2);
        assert_eq!(worker.matches(".lock(").count(), 2);
        assert_eq!(worker.matches("fn ").count(), 1);
        assert!(!worker.contains("Mutex::lock"));
        let owner_apis = format!("{}{}", &production[..run_start], &production[run_end..]);
        let without_try = owner_apis.replace("try_lock", "TRY");
        assert!(
            !without_try.contains("Mutex::lock"),
            "router must not call Mutex::lock"
        );
        assert!(
            !without_try.contains(".lock()"),
            "router must not call blocking lock"
        );
    }

    #[test]
    fn policy_is_the_validated_startup_value() {
        let options = PackageEventPlaneOptions {
            payload_max_bytes: 2048,
            producer_queue_max_bytes: 4096,
            consumer_queue_max_bytes: 4096,
            global_in_flight_bytes: 8192,
            ..PackageEventPlaneOptions::default()
        };
        let startup = crate::HubStartupOptions {
            package_event_plane: options.clone(),
            data_directory: crate::DataDirectoryOption::Explicit("/tmp/event-plane-policy".into()),
            ..crate::HubStartupOptions::default()
        };
        let config = startup
            .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
            .expect("config");
        let router = PackageEventRouter::new(config.package_event_plane);
        assert_eq!(router.policy().payload_max_bytes, 2048);
    }

    #[test]
    fn held_lock_try_ingress_returns_shed_busy_without_blocking() {
        let router = Arc::new(router());
        let started = Instant::now();
        let status = router.test_with_inner_held(|| {
            let router = Arc::clone(&router);
            thread::spawn(move || {
                router.try_ingress(
                    HUB_EVENT_OWNER,
                    "worktree_created",
                    &serde_json::json!({ "event": "worktree_created" }),
                    Instant::now(),
                )
            })
            .join()
            .expect("join")
        });
        assert_eq!(status, EventPlaneStatus::ShedBusy);
        assert!(started.elapsed() < StdDuration::from_millis(5));
        let snapshot = router.snapshot().expect("snapshot");
        assert_eq!(snapshot.global_in_flight_bytes, 0);
    }

    #[test]
    fn concurrent_emitters_cannot_over_admit() {
        let router = Arc::new(router());
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer-a", "producer", "sample.ready");
        subscribe(&router, "consumer-b", "producer", "sample.ready");
        let mut joins = Vec::new();
        for _ in 0..8 {
            let router = Arc::clone(&router);
            joins.push(thread::spawn(move || {
                router.try_ingress(
                    "producer",
                    "sample.ready",
                    &serde_json::json!({ "ok": true }),
                    Instant::now(),
                )
            }));
        }
        for join in joins {
            let _ = join.join().expect("join");
        }
        let snapshot = router.snapshot().expect("snapshot");
        let producer_events = snapshot
            .producer_events
            .get("producer")
            .copied()
            .unwrap_or(0);
        assert!(producer_events <= 256);
        assert!(snapshot.global_in_flight_bytes <= 16 * 1024 * 1024);
    }

    #[test]
    fn counters_return_to_baseline_after_delivery_and_shed() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("batch");
        assert_eq!(batch.len(), 1);
        router
            .note_admitted(batch[0].envelope_id, "consumer", 1)
            .expect("admit");
        router
            .retire_holder(batch[0].envelope_id, "consumer", 1)
            .expect("retire");
        let snapshot = router.snapshot().expect("snapshot");
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(
            snapshot
                .producer_events
                .get("producer")
                .copied()
                .unwrap_or(0),
            0
        );
        assert_eq!(
            router.try_ingress(
                "unknown",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::RejectedForeign
        );
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": "no" }),
                Instant::now()
            ),
            EventPlaneStatus::RejectedInvalid
        );
        assert_eq!(
            router.try_subscribe(EventSubscription {
                plugin_key: "consumer".into(),
                owner: "*".into(),
                name: "sample.ready".into(),
                handler_id: "x".into(),
                generation: 1,
                ..EventSubscription::default()
            }),
            EventPlaneStatus::RejectedWildcard
        );
        let busy = router.test_with_inner_held(|| {
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now(),
            )
        });
        assert_eq!(busy, EventPlaneStatus::ShedBusy);
        let after = router.snapshot().expect("snapshot");
        assert_eq!(after.global_in_flight_bytes, 0);
    }

    #[test]
    fn pending_owner_ops_keep_old_generation_until_applied() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        let mut ops = EventPlaneOwnerOps::default();
        ops.record(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "producer".into(),
            generation: 1,
        });
        let status = router.test_with_inner_held(|| {
            assert_eq!(ops.pending_for("producer").len(), 1);
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now(),
            )
        });
        assert_ne!(status, EventPlaneStatus::RejectedUndeclared);
        assert_eq!(ops.pending_for("producer").len(), 1);
        let applied = apply_owner_step(&mut ops, &router);
        assert_eq!(applied.len(), 1);
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::RejectedUndeclared
        );
    }

    #[test]
    fn two_owners_unload_independently() {
        let router = router();
        router
            .try_register_contracts(vec![
                sample_contract("one", "ready"),
                sample_contract("two", "ready"),
            ])
            .expect("register");
        let mut ops = EventPlaneOwnerOps::default();
        router.test_with_inner_held(|| {
            ops.record(OwnerOp {
                kind: OwnerOpKind::Unload,
                owner: "one".into(),
                generation: 1,
            });
            ops.record(OwnerOp {
                kind: OwnerOpKind::Unload,
                owner: "two".into(),
                generation: 1,
            });
            assert_eq!(ops.pending_for("one").len(), 1);
            assert_eq!(ops.pending_for("two").len(), 1);
        });
        let applied = apply_owner_step(&mut ops, &router);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].owner, "one");
        assert!(!ops.is_empty());
        let applied = apply_owner_step(&mut ops, &router);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].owner, "two");
        assert!(ops.is_empty());
    }

    #[test]
    fn unload_then_reload_applies_in_order() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "old")])
            .expect("register");
        let mut ops = EventPlaneOwnerOps::default();
        ops.record(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "producer".into(),
            generation: 1,
        });
        ops.record(OwnerOp {
            kind: OwnerOpKind::Reload,
            owner: "producer".into(),
            generation: 2,
        });
        let applied = apply_owner_step(&mut ops, &router);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].kind, OwnerOpKind::Unload);
        assert!(!ops.is_empty());
        let applied = apply_owner_step(&mut ops, &router);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].kind, OwnerOpKind::Reload);
        assert!(ops.is_empty());
        assert_eq!(
            router.try_ingress(
                "producer",
                "old",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::RejectedUndeclared
        );
    }

    #[test]
    fn owner_operations_rotate_after_contention_and_preserve_each_owner_order() {
        let router = router();
        let mut ops = EventPlaneOwnerOps::default();
        ops.record(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "one".into(),
            generation: 0,
        });
        for generation in 1..=1_000 {
            ops.record(OwnerOp {
                kind: OwnerOpKind::Reload,
                owner: "one".into(),
                generation,
            });
        }
        ops.record(OwnerOp {
            kind: OwnerOpKind::Reload,
            owner: "two".into(),
            generation: 1,
        });
        let work = router.test_with_inner_held(|| {
            let OwnerStep::Work(work) = ops.apply_ready(&router) else {
                panic!("the owner must transfer unload work without acquiring the router");
            };
            work
        });
        assert_eq!(ops.pending_for("one").len(), 1_001);
        let applied = apply_owner_step(&mut ops, &router);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].owner, "two");
        assert!(!ops.has_ready());
        assert!(matches!(ops.apply_ready(&router), OwnerStep::Waiting));
        let completion = run_work(&router, work);
        assert_eq!(
            ops.complete(completion)
                .expect("unload completes")
                .generation,
            0
        );
        for generation in 1..=1_000 {
            let applied = apply_owner_step(&mut ops, &router);
            assert_eq!(applied.len(), 1);
            assert_eq!(applied[0].owner, "one");
            assert_eq!(applied[0].generation, generation);
        }
        assert!(ops.is_empty());
        assert!(matches!(ops.apply_ready(&router), OwnerStep::Idle));
    }

    #[test]
    fn owner_completions_reject_retries_duplicates_and_repeated_operations() {
        let router = router();
        let mut ops = EventPlaneOwnerOps::default();
        let op = OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "producer".into(),
            generation: 1,
        };
        ops.record(op.clone());
        ops.record(op.clone());
        let OwnerStep::Work(first) = ops.apply_ready(&router) else {
            panic!("first work");
        };
        let first_id = first.identity().clone();
        let first_completion = run_work(&router, first);
        assert!(ops.retry(&first_id));
        assert!(!ops.retry(&first_id));
        assert!(ops.complete(first_completion).is_none());
        assert_eq!(ops.pending_for("producer"), vec![op.clone(), op.clone()]);
        assert!(ops.has_ready());

        let OwnerStep::Work(second) = ops.apply_ready(&router) else {
            panic!("retry work");
        };
        let second_id = second.identity().clone();
        assert_ne!(first_id, second_id);
        assert_eq!(ops.complete(run_work(&router, second)), Some(op.clone()));
        let OwnerStep::Work(third) = ops.apply_ready(&router) else {
            panic!("repeated operation work");
        };
        assert_ne!(&second_id, third.identity());
        assert!(
            ops.complete(EventOwnerCompletion {
                identity: second_id
            })
            .is_none()
        );
        assert_eq!(ops.pending_for("producer"), vec![op.clone()]);
        assert_eq!(ops.complete(run_work(&router, third)), Some(op));
        assert!(ops.is_empty());
        assert!(!ops.has_ready());
        assert!(matches!(ops.apply_ready(&router), OwnerStep::Idle));
    }

    #[test]
    fn owner_restart_keeps_exact_operation_and_rejects_stale_receipts() {
        let router = router();
        let mut ops = EventPlaneOwnerOps::default();
        let op = OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "producer".into(),
            generation: 1,
        };
        ops.record(op.clone());
        ops.record(op.clone());
        let OwnerStep::Work(first) = ops.apply_ready(&router) else {
            panic!("first work");
        };
        let first_id = first.identity().clone();
        let first_completion = run_work(&router, first);
        let other = OwnerOp {
            owner: "other".into(),
            ..op.clone()
        };
        ops.record(other.clone());

        let restarted = ops.restart(&first_id).expect("restart current operation");
        let restarted_id = restarted.identity().clone();
        assert_ne!(first_id, restarted_id);
        assert_eq!(restarted_id.operation(), &op);
        assert!(ops.restart(&first_id).is_none());
        assert!(!ops.retry(&first_id));
        assert!(ops.complete(first_completion).is_none());
        assert_eq!(ops.pending_for("producer"), vec![op.clone(), op.clone()]);
        assert!(!ops.ready.contains("producer"));
        let OwnerStep::Work(other_work) = ops.apply_ready(&router) else {
            panic!("other owner remains ready");
        };
        assert_eq!(other_work.identity().operation(), &other);
        assert_eq!(ops.complete(run_work(&router, other_work)), Some(other));
        assert!(!ops.has_ready());
        assert_eq!(ops.complete(run_work(&router, restarted)), Some(op.clone()));
        assert!(ops.restart(&restarted_id).is_none());
        assert!(ops.has_ready());
        let OwnerStep::Work(next) = ops.apply_ready(&router) else {
            panic!("next operation follows completion");
        };
        assert_ne!(next.identity(), &restarted_id);
        assert_eq!(ops.complete(run_work(&router, next)), Some(op));
        assert!(ops.is_empty());
    }

    #[test]
    fn stale_unload_keeps_newer_producer_and_consumer_copies() {
        for unload_producer in [true, false] {
            let router = router();
            router
                .try_register_contracts(vec![sample_contract("producer", "ready")])
                .expect("old producer");
            router
                .begin_package_generation("consumer")
                .expect("old consumer");
            subscribe(&router, "consumer", "producer", "ready");
            let old_payload = serde_json::json!({"ok": false});
            assert_eq!(
                router.try_ingress("producer", "ready", &old_payload, Instant::now()),
                EventPlaneStatus::Accepted
            );

            router
                .try_register_contracts(vec![sample_contract("producer", "ready")])
                .expect("new producer");
            router
                .begin_package_generation("consumer")
                .expect("new consumer");
            assert_eq!(
                router.try_subscribe(EventSubscription {
                    plugin_key: "consumer".into(),
                    owner: "producer".into(),
                    name: "ready".into(),
                    handler_id: "replacement".into(),
                    generation: 2,
                    ..EventSubscription::default()
                }),
                EventPlaneStatus::Accepted
            );
            let new_payload = serde_json::json!({"ok": true});
            assert_eq!(
                router.try_ingress("producer", "ready", &new_payload, Instant::now()),
                EventPlaneStatus::Accepted
            );
            assert_router_accounting(&router);

            run_unload(
                &router,
                if unload_producer {
                    "producer"
                } else {
                    "consumer"
                },
                1,
            );
            assert_router_accounting(&router);
            let snapshot = router.snapshot().expect("remaining occupancy");
            assert_eq!(snapshot.queued_holders, 1);
            assert_eq!(snapshot.producer_events["producer"], 1);
            let mut batch = router
                .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
                .expect("new delivery");
            assert_eq!(batch.len(), 1);
            let delivery = batch.pop().expect("replacement copy");
            assert_eq!(delivery.payload_json, new_payload);
            assert_eq!(delivery.holder.event_generation, 2);
            assert_eq!(delivery.holder.plugin_generation, 2);
            router
                .complete_pulled_delivery(delivery)
                .expect("complete new copy");
            assert_router_accounting(&router);
            assert_eq!(
                router.snapshot().expect("retired").global_in_flight_bytes,
                0
            );
        }
    }

    #[test]
    fn same_generation_consumer_rebind_keeps_the_new_diagnostic_cell() {
        let router = router();
        let mut inner = router.inner.lock().unwrap();
        bind_consumer_cell(&mut inner, router.counters(), "consumer");
        let old = inner.consumers["consumer"]
            .age_cell
            .as_ref()
            .unwrap()
            .clone();
        old.store(2, 10, 0, false, 200);
        old.close_writes();
        bind_consumer_cell(&mut inner, router.counters(), "consumer");
        let new = inner.consumers["consumer"].age_cell.as_ref().unwrap();
        assert!(!Arc::ptr_eq(&old, new));
        assert!(!new.is_write_closed());
        assert_eq!(
            old.sample(),
            crate::event_plane_counters::AgeSample::Empty { count: 0, bytes: 0 }
        );
        let rows: Vec<_> = router
            .counters()
            .snapshot()
            .queue_ages
            .into_iter()
            .filter(|row| row.kind == DaemonQueueKind::Consumer && row.identity == "consumer")
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].state,
            botster_hub_client::DaemonQueueAgeState::Empty
        );
    }

    #[test]
    fn stale_unload_preserves_new_client_registration_and_consumer_cell() {
        let router = router();
        let mut contract = sample_contract("producer", "ready");
        contract.audience.insert(EventAudience::Clients);
        router
            .try_register_contracts(vec![contract.clone()])
            .expect("old producer");
        let mailbox = Arc::new(ClientEventMailbox::new(router.policy()));
        for (id, replacement) in [("old", false), ("new", true)] {
            if replacement {
                router
                    .try_register_contracts(vec![contract.clone()])
                    .expect("new producer");
            }
            let gap = mailbox
                .register_gap_slot(id, "producer", "ready")
                .expect("gap slot");
            assert_eq!(
                router.try_subscribe_client(ClientEventHolder {
                    connection_id: "connection".into(),
                    subscription_id: id.into(),
                    owner: "producer".into(),
                    name: "ready".into(),
                    subjects: BTreeSet::new(),
                    mailbox: Arc::clone(&mailbox),
                    gap,
                }),
                EventPlaneStatus::Accepted
            );
        }
        router
            .begin_package_generation("consumer")
            .expect("old consumer");
        router
            .begin_package_generation("consumer")
            .expect("new consumer");
        subscribe(&router, "consumer", "producer", "ready");
        let before = consumer_row(&router, "consumer");
        run_unload(&router, "consumer", 1);
        assert_eq!(consumer_row(&router, "consumer"), before);
        run_unload(&router, "producer", 1);
        assert_eq!(router.test_client_holder_count("connection"), 1);
        assert_eq!(
            router.try_ingress(
                "producer",
                "ready",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        assert!(mailbox.take_ready_event().is_some());
        assert!(mailbox.take_ready_event().is_none());
        assert_eq!(consumer_row(&router, "consumer").queue_count, Some(1));
        assert_router_accounting(&router);
        run_unload(&router, "producer", 2);
        assert_eq!(router.test_client_holder_count("connection"), 0);
        assert_router_accounting(&router);
    }

    #[test]
    fn unload_retains_one_mib_charge_until_worker_destruction_and_reap() {
        const BYTES: usize = 1024 * 1024;
        let policy = PackageEventPlanePolicy {
            payload_max_bytes: BYTES,
            producer_queue_max_bytes: BYTES,
            consumer_queue_max_bytes: BYTES,
            global_in_flight_bytes: BYTES,
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        let mut large = sample_contract("producer", "large");
        large.schema = CompiledEventSchema::compile(&serde_json::json!({
            "type": "object", "properties": {"text": {"type": "string"}}
        }))
        .expect("large schema");
        router
            .try_register_contracts(vec![large, sample_contract("unrelated", "ready")])
            .expect("contracts");
        subscribe(&router, "consumer-a", "producer", "large");
        subscribe(&router, "consumer-b", "producer", "large");
        subscribe(&router, "other-consumer", "unrelated", "ready");
        let payload = serde_json::json!({"text": "x".repeat(BYTES - 11)});
        assert_eq!(serde_json::to_vec(&payload).expect("encoding").len(), BYTES);
        assert_eq!(
            router.try_ingress("producer", "large", &payload, Instant::now()),
            EventPlaneStatus::Accepted
        );
        assert_router_accounting(&router);
        let delivery = router
            .pull_ready_batch(1, BYTES, Instant::now(), StdDuration::from_secs(1))
            .expect("pull")
            .pop()
            .expect("one delivery");
        router
            .note_admitted(
                delivery.envelope_id,
                &delivery.holder.plugin_key,
                delivery.holder.generation,
            )
            .expect("admit");
        assert!(
            !router
                .retire_holder(
                    delivery.envelope_id,
                    &delivery.holder.plugin_key,
                    delivery.holder.generation
                )
                .expect("first holder retires")
        );
        assert_router_accounting(&router);
        let weak_payload = {
            let inner = lock_inner(&router.inner).expect("payload lock");
            Arc::downgrade(
                &inner
                    .live_envelope(delivery.envelope_id)
                    .expect("remaining holder")
                    .payload,
            )
        };
        let mut ops = EventPlaneOwnerOps::default();
        ops.record(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "producer".into(),
            generation: 1,
        });
        let OwnerStep::Work(work) = ops.apply_ready(&router) else {
            panic!("unload work")
        };
        let (phase_tx, phase_rx) = std::sync::mpsc::sync_channel(0);
        let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(0);
        *router.unload_test_probe.try_lock().expect("probe") = Some(Box::new(move |phase| {
            if phase != UnloadTestPhase::Locked {
                phase_tx.send(phase).expect("phase sent");
                resume_rx.recv().expect("owner resumes worker");
            }
        }));
        thread::scope(|scope| {
            let worker = scope.spawn(|| work.run(&router));
            assert_eq!(
                phase_rx.recv().expect("detached"),
                UnloadTestPhase::Detached
            );
            assert!(!ops.has_ready());
            assert!(matches!(ops.apply_ready(&router), OwnerStep::Waiting));
            assert_router_accounting(&router);
            assert!(weak_payload.upgrade().is_some());
            assert_eq!(
                router
                    .snapshot()
                    .expect("retained snapshot")
                    .global_in_flight_bytes,
                BYTES
            );
            assert_eq!(
                router.try_ingress(
                    "unrelated",
                    "ready",
                    &serde_json::json!({"ok": true}),
                    Instant::now()
                ),
                EventPlaneStatus::ShedFull
            );
            router
                .requeue_delivery(delivery)
                .expect("late requeue is an empty success");
            assert!(
                router
                    .pull_ready_batch(8, BYTES, Instant::now(), StdDuration::from_secs(1))
                    .expect("no retired payload delivery")
                    .is_empty()
            );
            assert_eq!(router.test_outstanding_pulls(), 0);
            assert_router_accounting(&router);
            resume_tx.send(()).expect("destroy payload");

            assert_eq!(
                phase_rx.recv().expect("destroyed"),
                UnloadTestPhase::Destroyed
            );
            assert!(weak_payload.upgrade().is_none());
            assert_eq!(
                router
                    .snapshot()
                    .expect("charge before reap")
                    .global_in_flight_bytes,
                BYTES
            );
            assert_eq!(
                router.try_ingress(
                    "unrelated",
                    "ready",
                    &serde_json::json!({"ok": true}),
                    Instant::now()
                ),
                EventPlaneStatus::ShedFull
            );
            assert_router_accounting(&router);
            resume_tx.send(()).expect("reap charge");
            let completion = worker
                .join()
                .expect("worker joins")
                .expect("worker completes");
            assert!(ops.complete(completion).is_some());
        });
        assert_router_accounting(&router);
        let snapshot = router.snapshot().expect("finished snapshot");
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        assert_eq!(snapshot.producer_events["producer"], 0);
        assert_eq!(snapshot.producer_bytes["producer"], 0);
        assert!(snapshot.consumer_events.values().all(|events| *events == 0));
        assert!(snapshot.consumer_bytes.values().all(|bytes| *bytes == 0));
        assert_eq!(snapshot.admitted_holders, 0);
        assert_eq!(
            router.try_ingress(
                "unrelated",
                "ready",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        assert_router_accounting(&router);
    }

    #[test]
    fn worker_termination_after_detachment_keeps_charge_until_exact_retry() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "ready")])
            .expect("contract");
        subscribe(&router, "consumer", "producer", "ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "ready",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let before = router.snapshot().expect("before").global_in_flight_bytes;
        let mut ops = EventPlaneOwnerOps::default();
        ops.record(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "producer".into(),
            generation: 1,
        });
        let OwnerStep::Work(work) = ops.apply_ready(&router) else {
            panic!("unload work")
        };
        let identity = work.identity().clone();
        *router.unload_test_probe.try_lock().expect("probe") = Some(Box::new(|phase| {
            if phase == UnloadTestPhase::Detached {
                panic!("injected worker termination after payload detachment");
            }
        }));
        thread::scope(|scope| {
            assert!(scope.spawn(|| work.run(&router)).join().is_err());
        });
        assert!(!router.inner.is_poisoned());
        assert_eq!(
            router
                .snapshot()
                .expect("retained charge")
                .global_in_flight_bytes,
            before
        );
        assert_router_accounting(&router);
        assert!(!ops.has_ready());
        assert!(ops.retry(&identity));
        assert!(
            ops.complete(EventOwnerCompletion {
                identity: identity.clone()
            })
            .is_none()
        );
        let OwnerStep::Work(retry) = ops.apply_ready(&router) else {
            panic!("retry work")
        };
        assert_ne!(retry.identity(), &identity);
        assert!(ops.complete(run_work(&router, retry)).is_some());
        assert!(ops.is_empty());
        assert_eq!(
            router
                .snapshot()
                .expect("reaped charge")
                .global_in_flight_bytes,
            0
        );
        assert_router_accounting(&router);
        let inner = lock_inner(&router.inner).expect("retired rows");
        assert!(inner.envelopes.is_empty());
        assert!(inner.admitted.is_empty());
    }

    #[test]
    fn consumer_restart_recovers_only_its_operation_bucket() {
        let router = router();
        router
            .try_register_contracts(vec![
                sample_contract("producer", "ready"),
                sample_contract("other-producer", "ready"),
            ])
            .expect("contracts");
        subscribe(&router, "consumer", "producer", "ready");
        subscribe(&router, "other-consumer", "other-producer", "ready");
        let payload = serde_json::json!({"ok": true});
        let size = serde_json::to_vec(&payload).expect("payload").len();
        for owner in ["producer", "other-producer"] {
            assert_eq!(
                router.try_ingress(owner, "ready", &payload, Instant::now()),
                EventPlaneStatus::Accepted
            );
        }
        let mut ops = EventPlaneOwnerOps::default();
        ops.record(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "consumer".into(),
            generation: 0,
        });
        let OwnerStep::Work(work) = ops.apply_ready(&router) else {
            panic!("consumer work")
        };
        let identity = work.identity().clone();
        *router.unload_test_probe.try_lock().expect("probe") = Some(Box::new(|phase| {
            if phase == UnloadTestPhase::Detached {
                panic!("worker terminates after consumer payload detachment");
            }
        }));
        thread::scope(|scope| {
            assert!(scope.spawn(|| work.run(&router)).join().is_err());
        });
        assert_router_accounting(&router);
        {
            let inner = lock_inner(&router.inner).expect("retained recovery bucket");
            assert_eq!(inner.retiring_by_cleanup[&("consumer".into(), 0)].len(), 1);
            assert!(
                !inner
                    .retiring_by_cleanup
                    .contains_key(&("producer".into(), 1))
            );
        }
        run_unload(&router, "other-producer", 1);
        assert_eq!(
            router
                .snapshot()
                .expect("consumer charge remains")
                .global_in_flight_bytes,
            size
        );
        assert_eq!(
            lock_inner(&router.inner)
                .expect("reap visits")
                .cleanup_visits
                .retiring,
            1
        );
        let restarted = ops
            .restart(&identity)
            .expect("restart with retained admission");
        assert_ne!(restarted.identity(), &identity);
        assert!(ops.complete(run_work(&router, restarted)).is_some());
        assert!(ops.is_empty());
        assert_eq!(
            router
                .snapshot()
                .expect("consumer charge reaped")
                .global_in_flight_bytes,
            0
        );
        assert!(
            lock_inner(&router.inner)
                .expect("no recovery buckets")
                .retiring_by_cleanup
                .is_empty()
        );
    }

    #[test]
    fn replacement_registers_recovery_before_destroying_payloads() {
        let router = Arc::new(router());
        router
            .try_register_contracts(vec![sample_contract("producer", "old")])
            .expect("contract");
        subscribe(&router, "consumer", "producer", "old");
        assert_eq!(
            router.try_ingress(
                "producer",
                "old",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let before = router.snapshot().expect("before").global_in_flight_bytes;
        let probe_router = Arc::clone(&router);
        *router.unload_test_probe.try_lock().expect("probe") = Some(Box::new(move |phase| {
            if phase == UnloadTestPhase::Detached {
                assert_router_accounting(&probe_router);
                let inner = lock_inner(&probe_router.inner).expect("replacement recovery");
                assert_eq!(inner.retiring_by_cleanup[&("producer".into(), 1)].len(), 1);
                assert_eq!(inner.global_bytes(), before);
                assert!(inner.envelopes.values().all(|envelope| {
                    envelope
                        .retirement
                        .as_ref()
                        .is_some_and(|retirement| !retirement.destroyed.load(Ordering::Acquire))
                }));
            }
        }));
        let result = thread::scope(|scope| {
            scope
                .spawn(|| {
                    router.try_replace_package_generation(
                        "producer",
                        vec![sample_contract("producer", "new")],
                        Vec::new(),
                    )
                })
                .join()
                .expect("replacement worker")
        });
        assert_eq!(result.expect("replacement"), 2);
        assert_router_accounting(&router);
        assert_eq!(
            router
                .snapshot()
                .expect("retired payload")
                .global_in_flight_bytes,
            0
        );
        assert!(
            lock_inner(&router.inner)
                .expect("retired bucket")
                .retiring_by_cleanup
                .is_empty()
        );
    }

    #[test]
    fn replacement_poison_returns_committed_generation_and_owned_cleanup() {
        let router = Arc::new(router());
        router
            .try_register_contracts(vec![sample_contract("producer", "old")])
            .expect("old generation");
        subscribe(&router, "consumer", "producer", "old");
        assert_eq!(
            router.try_ingress(
                "producer",
                "old",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let probe_router = Arc::clone(&router);
        *router.unload_test_probe.try_lock().expect("probe") = Some(Box::new(move |phase| {
            if phase == UnloadTestPhase::Destroyed {
                let router = Arc::clone(&probe_router);
                assert!(
                    thread::spawn(move || {
                        let _guard = router
                            .inner
                            .try_lock()
                            .expect("poison outside the worker guard");
                        panic!("injected router poison");
                    })
                    .join()
                    .is_err()
                );
            }
        }));
        let error = thread::scope(|scope| {
            scope
                .spawn(|| {
                    router.try_replace_package_generation(
                        "producer",
                        vec![sample_contract("producer", "new")],
                        Vec::new(),
                    )
                })
                .join()
                .expect("replacement worker returns")
                .expect_err("terminal cleanup error")
        });
        let (result, cleanup) = error.into_parts();
        assert_eq!(result, Ok(2));
        let Some(EventOwnerWorkError::RouterPoisoned(work)) = cleanup else {
            panic!("poison retains owned work")
        };
        assert_eq!(work.identity().operation().generation, 1);
        assert!(work.metadata_applied);
        assert!(work.retired_payloads.is_empty());
        let Err(TryLockError::Poisoned(poisoned)) = router.inner.try_lock() else {
            panic!("poisoned guard");
        };
        let inner = poisoned.into_inner();
        assert_router_indexes(&inner);
        assert_eq!(inner.package_generation["producer"], 2);
        assert!(
            inner
                .contracts
                .get("producer")
                .is_some_and(|events| events.contains_key("new"))
        );
        assert_eq!(inner.envelopes.len(), 1);
        assert_eq!(
            inner.global_bytes(),
            inner
                .envelopes
                .values()
                .map(|envelope| envelope.size)
                .sum::<usize>()
        );
    }

    #[test]
    fn repeated_delivery_retirement_prunes_holders_without_readmitting_late_acks() {
        let router = PackageEventRouter::new(PackageEventPlanePolicy {
            package_burst: 1_000,
            ..PackageEventPlanePolicy::default()
        });
        router
            .try_register_contracts(vec![sample_contract("producer", "ready")])
            .expect("contract");
        subscribe(&router, "one", "producer", "ready");
        subscribe(&router, "two", "producer", "ready");
        for _ in 0..256 {
            assert_eq!(
                router.try_ingress(
                    "producer",
                    "ready",
                    &serde_json::json!({"ok": true}),
                    Instant::now()
                ),
                EventPlaneStatus::Accepted
            );
            assert_router_accounting(&router);
            let first = router
                .pull_ready_batch(1, 64 * 1024, Instant::now(), StdDuration::from_secs(1))
                .expect("first pull")
                .pop()
                .expect("first copy");
            assert_router_accounting(&router);
            router.requeue_delivery(first).expect("requeue");
            assert_router_accounting(&router);
            let mut deliveries = router
                .pull_ready_batch(2, 64 * 1024, Instant::now(), StdDuration::from_secs(1))
                .expect("both copies");
            assert_eq!(deliveries.len(), 2);
            let first = deliveries.pop().expect("first copy");
            let id = first.envelope_id;
            let plugin = first.holder.plugin_key.clone();
            let generation = first.holder.generation;
            router
                .note_admitted(id, &plugin, generation)
                .expect("admit");
            assert!(
                !router
                    .retire_holder(id, &plugin, generation)
                    .expect("retire first")
            );
            router
                .note_admitted(id, &plugin, generation)
                .expect("late duplicate admission");
            assert!(
                !router
                    .retire_holder(id, &plugin, generation)
                    .expect("duplicate retirement")
            );
            router
                .complete_pulled_delivery(first)
                .expect("complete retired pull");
            assert_eq!(
                router
                    .snapshot()
                    .expect("one holder remains")
                    .producer_events["producer"],
                1
            );
            assert_router_accounting(&router);
            router
                .complete_pulled_delivery(deliveries.pop().expect("last copy"))
                .expect("complete final holder");
            router
                .note_admitted(id, &plugin, generation)
                .expect("late acknowledgement after envelope removal");
            assert!(
                !router
                    .retire_holder(id, &plugin, generation)
                    .expect("late retirement")
            );
            assert_router_accounting(&router);
            let inner = lock_inner(&router.inner).expect("retired metadata");
            assert!(inner.admitted.is_empty());
            assert!(inner.envelopes.is_empty());
            assert!(inner.outstanding_pulls.is_empty());
        }
    }

    #[test]
    fn unload_releases_empty_and_destroyed_producer_age_lists() {
        let router = router();
        for generation in 1..=64 {
            router
                .try_register_contracts(vec![sample_contract("producer", "ready")])
                .expect("generation");
            if generation % 2 == 0 {
                subscribe(&router, "consumer", "producer", "ready");
                assert_eq!(
                    router.try_ingress(
                        "producer",
                        "ready",
                        &serde_json::json!({"ok": true}),
                        Instant::now()
                    ),
                    EventPlaneStatus::Accepted
                );
            }
            run_unload(&router, "producer", generation);
            assert_router_accounting(&router);
            let inner = lock_inner(&router.inner).expect("retired age lists");
            assert_eq!(
                inner.producer_age_lists.len(),
                1,
                "only the Hub list remains"
            );
            assert!(
                inner
                    .producer_age_lists
                    .contains_key(&(HUB_EVENT_OWNER.into(), 0))
            );
            assert!(inner.admitted.is_empty());
        }
    }

    #[test]
    fn failed_submission_restores_readiness_without_applying_unload() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "ready")])
            .expect("contract");
        let mut ops = EventPlaneOwnerOps::default();
        ops.record(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "producer".into(),
            generation: 1,
        });
        let OwnerStep::Work(work) = ops.apply_ready(&router) else {
            panic!("unsubmitted work")
        };
        let identity = work.identity().clone();
        assert!(!ops.has_ready());
        assert!(router.test_has_contract("producer", "ready"));
        drop(work);
        assert!(ops.retry(&identity));
        assert!(ops.has_ready());
        assert!(router.test_has_contract("producer", "ready"));
        assert_eq!(apply_owner_step(&mut ops, &router).len(), 1);
        assert!(!router.test_has_contract("producer", "ready"));
        assert!(ops.is_empty());
    }

    #[test]
    fn worker_router_lock_sheds_unrelated_publication_without_blocking_owner_dispatch() {
        let router = router();
        let mut contracts = Vec::new();
        for index in 0..128 {
            contracts.push(sample_contract("producer", &format!("event-{index}")));
        }
        contracts.push(sample_contract("unrelated", "ready"));
        router
            .try_register_contracts(contracts)
            .expect("populated contracts");
        subscribe(&router, "consumer", "producer", "event-0");
        subscribe(&router, "other-consumer", "unrelated", "ready");
        for _ in 0..64 {
            assert_eq!(
                router.try_ingress(
                    "producer",
                    "event-0",
                    &serde_json::json!({"ok": true}),
                    Instant::now()
                ),
                EventPlaneStatus::Accepted
            );
        }
        let mut ops = EventPlaneOwnerOps::default();
        ops.record(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "producer".into(),
            generation: 1,
        });
        let OwnerStep::Work(work) = ops.apply_ready(&router) else {
            panic!("worker work")
        };
        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *router.unload_test_probe.try_lock().expect("probe") = Some(Box::new(move |phase| {
            if phase == UnloadTestPhase::Locked {
                locked_tx.send(()).expect("locked notification");
                release_rx.recv().expect("release worker guard");
            }
        }));
        thread::scope(|scope| {
            let worker = scope.spawn(|| work.run(&router));
            locked_rx.recv().expect("worker owns router lock");
            assert_eq!(router.snapshot(), Err(EventPlaneStatus::ShedBusy));
            assert_eq!(
                router.try_ingress(
                    "unrelated",
                    "ready",
                    &serde_json::json!({"ok": true}),
                    Instant::now()
                ),
                EventPlaneStatus::ShedBusy
            );
            ops.record(OwnerOp {
                kind: OwnerOpKind::Reload,
                owner: "unrelated".into(),
                generation: 1,
            });
            assert!(ops.has_ready());
            let OwnerStep::Applied(op) = ops.apply_ready(&router) else {
                panic!("unrelated owner remains runnable")
            };
            assert_eq!(op.owner, "unrelated");
            assert!(matches!(ops.apply_ready(&router), OwnerStep::Waiting));
            release_tx.send(()).expect("release worker");
            assert!(
                ops.complete(
                    worker
                        .join()
                        .expect("worker joins")
                        .expect("worker completes")
                )
                .is_some()
            );
        });
        assert!(ops.is_empty());
        assert_eq!(
            router.try_ingress(
                "unrelated",
                "ready",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        assert_router_accounting(&router);
    }

    #[test]
    fn populated_unload_reports_concurrent_unrelated_publication_without_a_lock_gate() {
        // This measurement reports contention. It does not establish a time bound.
        let policy = PackageEventPlanePolicy {
            package_burst: u32::MAX,
            producer_queue_max_events: 512,
            consumer_queue_max_events: 512,
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        let mut contracts = Vec::new();
        for owner in 0..32 {
            for event in 0..16 {
                contracts.push(sample_contract(
                    &format!("producer-{owner}"),
                    &format!("event-{event}"),
                ));
            }
        }
        let mut unrelated = sample_contract("unrelated", "ready");
        unrelated.audience = BTreeSet::from([EventAudience::Clients]);
        contracts.push(unrelated);
        router
            .try_register_contracts(contracts)
            .expect("populated contracts");
        for owner in 0..32 {
            for event in 0..16 {
                subscribe(
                    &router,
                    &format!("consumer-{owner}"),
                    &format!("producer-{owner}"),
                    &format!("event-{event}"),
                );
            }
            for _ in 0..128 {
                assert_eq!(
                    router.try_ingress(
                        &format!("producer-{owner}"),
                        "event-0",
                        &serde_json::json!({"ok": true}),
                        Instant::now()
                    ),
                    EventPlaneStatus::Accepted
                );
            }
        }
        let mut ops = EventPlaneOwnerOps::default();
        ops.record(OwnerOp {
            kind: OwnerOpKind::Unload,
            owner: "producer-0".into(),
            generation: 1,
        });
        let OwnerStep::Work(work) = ops.apply_ready(&router) else {
            panic!("worker work")
        };
        let barrier = std::sync::Barrier::new(2);
        let done = AtomicBool::new(false);
        let mut accepted = 0;
        let mut busy = 0;
        let mut longest_call = StdDuration::ZERO;
        let elapsed = thread::scope(|scope| {
            let worker = scope.spawn(|| {
                barrier.wait();
                let started = Instant::now();
                let completion = work.run(&router).expect("worker completes");
                let elapsed = started.elapsed();
                done.store(true, Ordering::Release);
                (completion, elapsed)
            });
            barrier.wait();
            while !done.load(Ordering::Acquire) {
                let started = Instant::now();
                match router.try_ingress(
                    "unrelated",
                    "ready",
                    &serde_json::json!({"ok": true}),
                    Instant::now(),
                ) {
                    EventPlaneStatus::Accepted => accepted += 1,
                    EventPlaneStatus::ShedBusy => busy += 1,
                    other => panic!("unexpected unrelated publication result: {other:?}"),
                }
                longest_call = longest_call.max(started.elapsed());
                thread::yield_now();
            }
            let (completion, elapsed) = worker.join().expect("worker joins");
            assert!(ops.complete(completion).is_some());
            elapsed
        });
        eprintln!(
            "populated unload: contracts=513 subscriptions=512 envelopes=4096; worker_us={} accepted={} shed_busy={} longest_publish_us={}",
            elapsed.as_micros(),
            accepted,
            busy,
            longest_call.as_micros()
        );
        assert!(ops.is_empty());
        assert_eq!(
            router.try_ingress(
                "unrelated",
                "ready",
                &serde_json::json!({"ok": true}),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        assert_router_accounting(&router);
    }

    #[test]
    fn expired_queued_copy_does_not_deliver() {
        let policy = PackageEventPlanePolicy {
            queue_age: StdDuration::from_millis(1),
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        assert_router_accounting(&router);
        thread::sleep(StdDuration::from_millis(3));
        let batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("batch");
        assert!(batch.is_empty());
        let snapshot = router.snapshot().expect("snapshot");
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        assert_router_accounting(&router);
    }

    #[test]
    fn unload_subtracts_every_removed_subscription() {
        let policy = PackageEventPlanePolicy {
            subscriptions_per_plugin_max: 2,
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        router
            .try_register_contracts(vec![
                sample_contract("producer", "one"),
                sample_contract("producer", "two"),
            ])
            .expect("register");
        subscribe(&router, "consumer", "producer", "one");
        subscribe(&router, "consumer", "producer", "two");
        let generation = router
            .current_package_generation("producer")
            .expect("generation");
        run_unload(&router, "producer", generation);
        router
            .begin_package_generation("consumer")
            .expect("consumer gen");
        router
            .try_register_contracts(vec![
                sample_contract("producer", "one"),
                sample_contract("producer", "two"),
            ])
            .expect("register again");
        subscribe(&router, "consumer", "producer", "one");
        subscribe(&router, "consumer", "producer", "two");
        assert_eq!(
            router.try_subscribe(EventSubscription {
                plugin_key: "consumer".into(),
                owner: "producer".into(),
                name: "one".into(),
                handler_id: "extra".into(),
                generation: 9,
                ..EventSubscription::default()
            }),
            EventPlaneStatus::RejectedInvalid
        );
    }

    #[test]
    fn old_generation_unload_keeps_replacement_contracts() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        let old = router
            .current_package_generation("producer")
            .expect("old gen");
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("replacement");
        subscribe(&router, "consumer", "producer", "sample.ready");
        run_unload(&router, "producer", old);
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
    }

    #[test]
    fn mint_with_lease_is_live_before_any_later_acquire() {
        let table = CausalScopeTable::new();
        let scope = table
            .mint_with_lease(Some(LeaseIdentity::EventInFlight {
                request_id: "req-1".into(),
            }))
            .expect("mint");
        assert!(table.is_live(scope));
        assert_eq!(table.lease_count(scope), Some(1));
    }

    #[test]
    fn held_router_retire_can_be_retried() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("batch");
        let delivery = &batch[0];
        router
            .note_admitted(
                delivery.envelope_id,
                &delivery.holder.plugin_key,
                delivery.holder.generation,
            )
            .expect("admit");
        let busy = router.test_with_inner_held(|| {
            router.retire_holder(
                delivery.envelope_id,
                &delivery.holder.plugin_key,
                delivery.holder.generation,
            )
        });
        assert_eq!(busy, Err(EventPlaneStatus::ShedBusy));
        assert!(
            router
                .retire_holder(
                    delivery.envelope_id,
                    &delivery.holder.plugin_key,
                    delivery.holder.generation,
                )
                .expect("retry")
        );
        let snapshot = router.snapshot().expect("snapshot");
        assert_eq!(snapshot.global_in_flight_bytes, 0);
    }

    #[test]
    fn transfer_keeps_scope_live_across_identity_handoff() {
        let table = CausalScopeTable::new();
        let scope = table
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                plugin_key: "producer".into(),
            }))
            .expect("mint");
        assert_eq!(
            table.transfer(
                scope,
                LeaseIdentity::PendingEntityPublish {
                    plugin_key: "producer".into(),
                },
                [LeaseIdentity::AdmittedEntityMutation {
                    generation: 0,
                    family: "producer.item".into(),
                    seq: 32,
                }],
            ),
            CausalAdmitResult::Applied
        );
        assert!(table.is_live(scope));
        assert_eq!(table.lease_count(scope), Some(1));
        assert_eq!(
            table.identities(scope),
            Some(BTreeSet::from([LeaseIdentity::AdmittedEntityMutation {
                generation: 0,
                family: "producer.item".into(),
                seq: 32,
            }]))
        );
    }

    #[test]
    fn oversize_payload_is_rejected_oversize_without_occupancy() {
        let policy = PackageEventPlanePolicy {
            payload_max_bytes: 4,
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::RejectedOversize
        );
        let snapshot = router.snapshot().expect("snapshot");
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(
            snapshot
                .producer_events
                .get("producer")
                .copied()
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn exhausted_tokens_are_rejected_over_rate_without_occupancy() {
        let policy = PackageEventPlanePolicy {
            package_rate_per_sec: 1,
            package_burst: 1,
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        let now = Instant::now();
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                now
            ),
            EventPlaneStatus::Accepted
        );
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                now
            ),
            EventPlaneStatus::RejectedOverRate
        );
        let snapshot = router.snapshot().expect("snapshot");
        assert_eq!(snapshot.queued_holders, 1);
        assert_eq!(
            snapshot
                .producer_events
                .get("producer")
                .copied()
                .unwrap_or(0),
            1
        );
        let batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("batch");
        assert_eq!(batch.len(), 1);
        router
            .note_admitted(batch[0].envelope_id, "consumer", 1)
            .expect("admit");
        router
            .retire_holder(batch[0].envelope_id, "consumer", 1)
            .expect("retire");
        let after = router.snapshot().expect("after");
        assert_eq!(after.global_in_flight_bytes, 0);
        assert_eq!(after.queued_holders, 0);
    }

    #[test]
    fn ingress_over_fanout_is_rejected_without_occupancy() {
        let policy = PackageEventPlanePolicy {
            fanout_per_emit_max: 1,
            subscribers_per_event_max: 2,
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer-a", "producer", "sample.ready");
        subscribe(&router, "consumer-b", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::RejectedOverFanout
        );
        let snapshot = router.snapshot().expect("snapshot");
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(
            snapshot
                .producer_events
                .get("producer")
                .copied()
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn commit_package_generation_is_atomic_and_held_lock_is_shed_busy() {
        let router = router();
        let contracts = vec![sample_contract("producer", "sample.ready")];
        let subscriptions = vec![EventSubscription {
            plugin_key: "consumer".into(),
            owner: "producer".into(),
            name: "sample.ready".into(),
            handler_id: "event:producer:sample.ready:1".into(),
            generation: 1,
            ..EventSubscription::default()
        }];
        let generation = router
            .try_commit_package_generation("producer", contracts.clone(), subscriptions.clone())
            .expect("commit");
        assert!(generation > 0);
        assert!(router.test_has_contract("producer", "sample.ready"));
        assert_eq!(router.test_subscription_count("consumer"), 1);

        let undeclared = vec![EventSubscription {
            plugin_key: "consumer".into(),
            owner: "producer".into(),
            name: "missing".into(),
            handler_id: "missing".into(),
            generation: 2,
            ..EventSubscription::default()
        }];
        assert_eq!(
            router.try_commit_package_generation("other", Vec::new(), undeclared),
            Err(EventPlaneStatus::RejectedUndeclared)
        );
        assert!(!router.test_has_contract("other", "missing"));
        assert_eq!(router.current_package_generation("other").expect("gen"), 0);

        let busy = router.test_with_inner_held(|| {
            router.try_commit_package_generation("producer", contracts, subscriptions)
        });
        assert_eq!(busy, Err(EventPlaneStatus::ShedBusy));
        assert_eq!(
            router
                .current_package_generation("producer")
                .expect("unchanged"),
            generation
        );
    }

    #[test]
    fn held_causal_table_queues_transfer_and_release_until_flush() {
        let table = CausalScopeTable::new();
        let scope = table
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                plugin_key: "producer".into(),
            }))
            .expect("mint");
        table.test_with_inner_held(|| {
            assert_eq!(
                table.transfer(
                    scope,
                    LeaseIdentity::PendingEntityPublish {
                        plugin_key: "producer".into(),
                    },
                    [LeaseIdentity::AdmittedEntityMutation {
                        generation: 0,
                        family: "f".into(),
                        seq: 1,
                    }],
                ),
                CausalAdmitResult::Applied
            );
            assert_eq!(
                table.release(
                    scope,
                    LeaseIdentity::AdmittedEntityMutation {
                        generation: 0,
                        family: "f".into(),
                        seq: 1,
                    },
                ),
                CausalAdmitResult::Applied
            );
            assert!(table.pending_ops());
        });
        assert_eq!(
            table.identities(scope),
            Some(BTreeSet::from([LeaseIdentity::PendingEntityPublish {
                plugin_key: "producer".into(),
            }]))
        );
        assert_eq!(table.flush_pending(), 1);
        assert!(table.pending_ops());
        assert_eq!(table.lease_count(scope), Some(1));
        assert_eq!(table.flush_pending(), 1);
        assert!(!table.pending_ops());
        assert!(!table.is_live(scope));
    }

    #[test]
    fn causal_flush_contention_retains_the_next_exact_lease_operation() {
        let table = CausalScopeTable::new();
        let identity = LeaseIdentity::EventInFlight {
            request_id: "request".into(),
        };
        let scope = table
            .mint_with_lease(Some(identity.clone()))
            .expect("scope");
        table.test_with_inner_held(|| {
            assert_eq!(
                table.release(scope, identity.clone()),
                CausalAdmitResult::Applied
            );
            assert_eq!(table.flush_pending(), 0);
            assert_eq!(table.pending_len.load(Ordering::SeqCst), 1);
        });
        {
            let _pending = table.pending.try_lock().expect("hold pending queue");
            assert_eq!(table.flush_pending(), 0);
            assert!(table.pending_ops());
            assert_eq!(table.identities(scope), Some(BTreeSet::from([identity])));
        }
        assert_eq!(table.flush_pending(), 1);
        assert_eq!(table.pending_len.load(Ordering::SeqCst), 0);
        assert!(!table.pending_ops());
        assert!(!table.is_live(scope));
        assert_eq!(table.flush_pending(), 0);
    }

    #[test]
    fn ordered_pending_path_keeps_fifo_and_returns_the_257th() {
        let table = CausalScopeTable::new();
        let mut scopes = Vec::new();
        for index in 0..=CAUSAL_PENDING_MAX {
            let scope = table
                .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                    plugin_key: format!("p{index}"),
                }))
                .expect("mint");
            scopes.push(scope);
        }
        let overflow = table.test_with_inner_held(|| {
            for (index, scope) in scopes.iter().take(CAUSAL_PENDING_MAX).enumerate() {
                assert_eq!(
                    table.transfer(
                        *scope,
                        LeaseIdentity::PendingEntityPublish {
                            plugin_key: format!("p{index}"),
                        },
                        [LeaseIdentity::AdmittedEntityMutation {
                            generation: 0,
                            family: "f".into(),
                            seq: index as u64,
                        }],
                    ),
                    CausalAdmitResult::Applied
                );
            }
            table.transfer(
                scopes[CAUSAL_PENDING_MAX],
                LeaseIdentity::PendingEntityPublish {
                    plugin_key: format!("p{}", CAUSAL_PENDING_MAX),
                },
                [LeaseIdentity::AdmittedEntityMutation {
                    generation: 0,
                    family: "f".into(),
                    seq: CAUSAL_PENDING_MAX as u64,
                }],
            )
        });
        let CausalAdmitResult::Retry(overflow) = overflow else {
            panic!("the 257th transfer must return to the caller: {overflow:?}");
        };
        let first = table.flush_pending();
        assert!(first > 0);
        assert!(
            first == 1,
            "one owner turn must not drain without a bound: {first}"
        );
        assert_eq!(
            table.identities(scopes[0]),
            Some(BTreeSet::from([LeaseIdentity::AdmittedEntityMutation {
                generation: 0,
                family: "f".into(),
                seq: 0,
            }]))
        );
        while table.pending_ops() {
            let _ = table.flush_pending();
        }
        assert_eq!(table.try_admit(overflow), CausalAdmitResult::Applied);
        while table.pending_ops() {
            let _ = table.flush_pending();
        }
        for (index, scope) in scopes.iter().enumerate() {
            assert_eq!(
                table.identities(*scope),
                Some(BTreeSet::from([LeaseIdentity::AdmittedEntityMutation {
                    generation: 0,
                    family: "f".into(),
                    seq: index as u64,
                }]))
            );
        }
    }

    #[test]
    fn same_scope_release_cannot_bypass_a_parked_transfer() {
        let table = CausalScopeTable::new();
        let live = table
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                plugin_key: "producer".into(),
            }))
            .expect("live");
        let mut fillers = Vec::new();
        for index in 0..(CAUSAL_PENDING_MAX - 1) {
            fillers.push(
                table
                    .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                        plugin_key: format!("p{index}"),
                    }))
                    .expect("mint"),
            );
        }
        table.test_with_inner_held(|| {
            for (index, scope) in fillers.iter().enumerate() {
                assert_eq!(
                    table.transfer(
                        *scope,
                        LeaseIdentity::PendingEntityPublish {
                            plugin_key: format!("p{index}"),
                        },
                        [LeaseIdentity::AdmittedEntityMutation {
                            generation: 0,
                            family: "f".into(),
                            seq: index as u64,
                        }],
                    ),
                    CausalAdmitResult::Applied
                );
            }
            assert_eq!(
                table.transfer(
                    live,
                    LeaseIdentity::PendingEntityPublish {
                        plugin_key: "producer".into(),
                    },
                    [LeaseIdentity::AdmittedEntityMutation {
                        generation: 0,
                        family: "producer.item".into(),
                        seq: 1,
                    }],
                ),
                CausalAdmitResult::Applied
            );
        });
        let first = table.flush_pending();
        assert!(first > 0);
        assert!(first == 1);
        assert!(table.pending_ops());
        assert_eq!(
            table.release(
                live,
                LeaseIdentity::AdmittedEntityMutation {
                    generation: 0,
                    family: "producer.item".into(),
                    seq: 1,
                },
            ),
            CausalAdmitResult::Applied
        );
        while table.pending_ops() {
            let _ = table.flush_pending();
        }
        assert!(
            !table.is_live(live),
            "parked transfer must apply before the later release"
        );
    }

    #[test]
    fn never_queued_release_closes_after_full_table_and_held_inner() {
        let table = CausalScopeTable::new();
        let live = table
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                plugin_key: "producer".into(),
            }))
            .expect("live");
        let mut fillers = Vec::new();
        for index in 0..CAUSAL_PENDING_MAX {
            fillers.push(
                table
                    .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                        plugin_key: format!("p{index}"),
                    }))
                    .expect("mint"),
            );
        }
        let overflow = table.test_with_inner_held(|| {
            for (index, scope) in fillers.iter().enumerate() {
                assert_eq!(
                    table.transfer(
                        *scope,
                        LeaseIdentity::PendingEntityPublish {
                            plugin_key: format!("p{index}"),
                        },
                        [LeaseIdentity::AdmittedEntityMutation {
                            generation: 0,
                            family: "f".into(),
                            seq: index as u64,
                        }],
                    ),
                    CausalAdmitResult::Applied
                );
            }
            release_or_retract(
                &table,
                live,
                LeaseIdentity::PendingEntityPublish {
                    plugin_key: "producer".into(),
                },
            )
        });
        let CausalAdmitResult::Retry(overflow) = overflow else {
            panic!("held full path must return the release: {overflow:?}");
        };
        assert_eq!(
            table.identities(live),
            Some(BTreeSet::from([LeaseIdentity::PendingEntityPublish {
                plugin_key: "producer".into(),
            }]))
        );
        while table.pending_ops() {
            let _ = table.flush_pending();
        }
        assert_eq!(table.try_admit(overflow), CausalAdmitResult::Applied);
        while table.pending_ops() {
            let _ = table.flush_pending();
        }
        assert!(!table.is_live(live));
    }

    #[test]
    fn replace_package_generation_restores_snapshot_on_failed_subscribe() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "old")])
            .expect("old");
        let old = router
            .current_package_generation("producer")
            .expect("old gen");
        assert_eq!(
            router
                .try_replace_package_generation(
                    "producer",
                    vec![sample_contract("producer", "new")],
                    vec![EventSubscription {
                        plugin_key: "consumer".into(),
                        owner: "producer".into(),
                        name: "missing".into(),
                        handler_id: "missing".into(),
                        generation: 1,
                        ..EventSubscription::default()
                    }],
                )
                .map_err(|error| {
                    let (result, cleanup) = error.into_parts();
                    assert!(cleanup.is_none());
                    result.expect_err("replacement admission failed")
                }),
            Err(EventPlaneStatus::RejectedUndeclared)
        );
        assert!(router.test_has_contract("producer", "old"));
        assert!(!router.test_has_contract("producer", "new"));
        assert_eq!(
            router.current_package_generation("producer").expect("kept"),
            old
        );
    }

    #[test]
    fn failed_replace_keeps_queued_old_generation_delivery() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "old")])
            .expect("old");
        subscribe(&router, "consumer", "producer", "old");
        assert_eq!(
            router.try_ingress(
                "producer",
                "old",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let before = router.snapshot().expect("before");
        assert_eq!(before.queued_holders, 1);
        assert_eq!(
            router
                .try_replace_package_generation(
                    "producer",
                    vec![sample_contract("producer", "new")],
                    vec![EventSubscription {
                        plugin_key: "consumer".into(),
                        owner: "producer".into(),
                        name: "missing".into(),
                        handler_id: "missing".into(),
                        generation: 1,
                        ..EventSubscription::default()
                    }],
                )
                .map_err(|error| {
                    let (result, cleanup) = error.into_parts();
                    assert!(cleanup.is_none());
                    result.expect_err("replacement admission failed")
                }),
            Err(EventPlaneStatus::RejectedUndeclared)
        );
        assert!(router.test_has_contract("producer", "old"));
        let after = router.snapshot().expect("after");
        assert_eq!(after.queued_holders, 1);
        assert_eq!(after.global_in_flight_bytes, before.global_in_flight_bytes);
        let batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("batch");
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn mixed_plugin_and_client_ingress_does_not_deliver_before_rejection() {
        let policy = PackageEventPlanePolicy {
            consumer_queue_max_events: 1,
            fanout_per_emit_max: 1,
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        let mut contract = sample_contract("producer", "notice");
        contract.audience = BTreeSet::from([EventAudience::Plugins, EventAudience::Clients]);
        router
            .try_register_contracts(vec![contract])
            .expect("register");
        subscribe(&router, "consumer", "producer", "notice");
        assert_eq!(
            router.try_ingress(
                "producer",
                "notice",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );

        let mailbox = std::sync::Arc::new(ClientEventMailbox::new(policy));
        let gap = mailbox
            .register_gap_slot("sub", "producer", "notice")
            .expect("register gap");
        assert_eq!(
            router.try_subscribe_client(ClientEventHolder {
                connection_id: "conn".into(),
                subscription_id: "sub".into(),
                owner: "producer".into(),
                name: "notice".into(),
                subjects: BTreeSet::new(),
                mailbox: mailbox.clone(),
                gap,
            }),
            EventPlaneStatus::Accepted
        );
        assert_eq!(
            router.try_ingress(
                "producer",
                "notice",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::ShedFull
        );
        assert!(
            mailbox.take_ready_event().is_none(),
            "ShedFull must not deliver to clients"
        );

        let fanout_router = PackageEventRouter::new(PackageEventPlanePolicy {
            fanout_per_emit_max: 1,
            ..PackageEventPlanePolicy::default()
        });
        let mut fanout_contract = sample_contract("producer", "notice");
        fanout_contract.audience = BTreeSet::from([EventAudience::Plugins, EventAudience::Clients]);
        fanout_router
            .try_register_contracts(vec![fanout_contract])
            .expect("register");
        subscribe(&fanout_router, "consumer-a", "producer", "notice");
        subscribe(&fanout_router, "consumer-b", "producer", "notice");
        let fanout_mailbox =
            std::sync::Arc::new(ClientEventMailbox::new(PackageEventPlanePolicy::default()));
        let fanout_gap = fanout_mailbox
            .register_gap_slot("sub", "producer", "notice")
            .expect("register gap");
        assert_eq!(
            fanout_router.try_subscribe_client(ClientEventHolder {
                connection_id: "conn".into(),
                subscription_id: "sub".into(),
                owner: "producer".into(),
                name: "notice".into(),
                subjects: BTreeSet::new(),
                mailbox: fanout_mailbox.clone(),
                gap: fanout_gap,
            }),
            EventPlaneStatus::Accepted
        );
        assert_eq!(
            fanout_router.try_ingress(
                "producer",
                "notice",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::RejectedOverFanout
        );
        assert!(
            fanout_mailbox.take_ready_event().is_none(),
            "over-fanout must not deliver to clients"
        );

        let accepted_mailbox =
            std::sync::Arc::new(ClientEventMailbox::new(PackageEventPlanePolicy::default()));
        let accepted_router = PackageEventRouter::new(PackageEventPlanePolicy::default());
        let mut accepted_contract = sample_contract("producer", "notice");
        accepted_contract.audience =
            BTreeSet::from([EventAudience::Plugins, EventAudience::Clients]);
        accepted_router
            .try_register_contracts(vec![accepted_contract])
            .expect("register");
        subscribe(&accepted_router, "consumer", "producer", "notice");
        let accepted_gap = accepted_mailbox
            .register_gap_slot("sub", "producer", "notice")
            .expect("register gap");
        assert_eq!(
            accepted_router.try_subscribe_client(ClientEventHolder {
                connection_id: "conn".into(),
                subscription_id: "sub".into(),
                owner: "producer".into(),
                name: "notice".into(),
                subjects: BTreeSet::new(),
                mailbox: accepted_mailbox.clone(),
                gap: accepted_gap,
            }),
            EventPlaneStatus::Accepted
        );
        assert_eq!(
            accepted_router.try_ingress(
                "producer",
                "notice",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        match accepted_mailbox.take_ready_event() {
            Some(botster_hub_client::DaemonEvent::PackageEvent {
                subscription_id, ..
            }) => {
                assert_eq!(subscription_id, "sub");
            }
            other => panic!("accepted mixed ingress must deliver to clients: {other:?}"),
        }
    }

    #[test]
    fn clients_only_subscription_is_rejected() {
        let router = router();
        let mut contract = sample_contract("producer", "notice");
        contract.audience = BTreeSet::from([EventAudience::Clients]);
        router
            .try_register_contracts(vec![contract])
            .expect("register");
        assert_eq!(
            router.try_subscribe(EventSubscription {
                plugin_key: "consumer".into(),
                owner: "producer".into(),
                name: "notice".into(),
                handler_id: "event".into(),
                generation: 1,
                ..EventSubscription::default()
            }),
            EventPlaneStatus::RejectedAudience
        );
    }

    #[test]
    fn requeue_and_complete_keep_ownership_when_router_is_busy() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "notice")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "notice");
        assert_eq!(
            router.try_ingress(
                "producer",
                "notice",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let before = router.snapshot().expect("before pull");
        assert_eq!(before.queued_holders, 1);
        let mut batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("pull");
        let delivery = batch.pop().expect("one pulled copy");
        assert!(batch.is_empty());
        assert_eq!(router.test_outstanding_pulls(), 1);
        let pulled = router.snapshot().expect("after pull");
        assert_eq!(pulled.queued_holders, 0);
        let returned = router.test_with_inner_held(|| router.requeue_delivery(delivery));
        let delivery = match returned {
            Err((delivery, EventPlaneStatus::ShedBusy)) => *delivery,
            other => panic!("busy requeue must return ownership: {other:?}"),
        };
        let busy = router.snapshot().expect("busy requeue");
        assert_eq!(busy.queued_holders, 0);
        assert_eq!(busy.global_in_flight_bytes, before.global_in_flight_bytes);
        assert_eq!(busy.admitted_holders, before.admitted_holders);
        assert_eq!(router.test_outstanding_pulls(), 1);
        router
            .requeue_delivery(delivery)
            .unwrap_or_else(|_| panic!("requeue after release"));
        let restored = router.snapshot().expect("restored");
        assert_eq!(restored.queued_holders, before.queued_holders);
        assert_eq!(
            restored.global_in_flight_bytes,
            before.global_in_flight_bytes
        );
        assert_eq!(router.test_outstanding_pulls(), 0);

        let mut batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("pull again");
        let delivery = batch.pop().expect("one pulled copy");
        let returned = router.test_with_inner_held(|| router.complete_pulled_delivery(delivery));
        let delivery = match returned {
            Err((delivery, EventPlaneStatus::ShedBusy)) => *delivery,
            other => panic!("busy complete must return ownership: {other:?}"),
        };
        assert_eq!(router.test_outstanding_pulls(), 1);
        router
            .complete_pulled_delivery(delivery)
            .unwrap_or_else(|_| panic!("complete after release"));
        let done = router.snapshot().expect("completed");
        assert_eq!(done.queued_holders, 0);
        assert_eq!(done.global_in_flight_bytes, 0);
        assert_eq!(router.test_outstanding_pulls(), 0);
    }

    #[test]
    fn requeue_rejects_duplicate_pull_and_respects_consumer_bounds() {
        let policy = PackageEventPlanePolicy {
            consumer_queue_max_events: 1,
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        router
            .try_register_contracts(vec![sample_contract("producer", "notice")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "notice");
        let payload = serde_json::json!({ "ok": true });
        assert_eq!(
            router.try_ingress("producer", "notice", &payload, Instant::now()),
            EventPlaneStatus::Accepted
        );
        let mut batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("pull");
        let delivery = batch.pop().expect("one pulled copy");
        let forged = ReadyDelivery {
            envelope_id: delivery.envelope_id,
            owner: delivery.owner.clone(),
            name: delivery.name.clone(),
            payload: delivery.payload.clone(),
            payload_json: delivery.payload_json.clone(),
            size: delivery.size,
            holder: delivery.holder.clone(),
            pull_id: delivery.pull_id,
        };
        router
            .requeue_delivery(delivery)
            .unwrap_or_else(|_| panic!("first requeue"));
        match router.requeue_delivery(forged) {
            Err((_, EventPlaneStatus::RejectedInvalid)) => {}
            other => panic!("duplicate requeue must be rejected: {other:?}"),
        }
        assert_eq!(router.test_outstanding_pulls(), 0);

        let mut batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("pull again");
        let first = batch.pop().expect("first copy");
        assert_eq!(
            router.try_ingress("producer", "notice", &payload, Instant::now()),
            EventPlaneStatus::Accepted
        );
        match router.requeue_delivery(first) {
            Err((delivery, EventPlaneStatus::ShedFull)) => {
                assert_eq!(router.test_outstanding_pulls(), 1);
                router
                    .complete_pulled_delivery(*delivery)
                    .unwrap_or_else(|_| panic!("complete after bound reject"));
            }
            other => panic!("full consumer queue must reject requeue: {other:?}"),
        }
        assert_eq!(router.test_outstanding_pulls(), 0);
    }

    #[test]
    fn counters_snapshot_succeeds_while_inner_lock_is_held() {
        let router = PackageEventRouter::new(PackageEventPlanePolicy::default());
        router
            .counters()
            .record_ingress_status(EventPlaneStatus::ShedBusy.index());
        let snapshot = router.test_with_inner_held(|| {
            assert_eq!(
                router.snapshot().expect_err("held inner is shed busy"),
                EventPlaneStatus::ShedBusy
            );
            router.counters().snapshot()
        });
        assert_eq!(
            snapshot.event_shed_by_reason.get("shed_busy").copied(),
            Some(1)
        );
    }

    #[test]
    fn diagnostic_reserve_failure_does_not_change_acceptance() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("owner", "ready")])
            .expect("register");
        assert_eq!(
            router.try_subscribe(EventSubscription {
                plugin_key: "consumer".to_string(),
                owner: "owner".to_string(),
                name: "ready".to_string(),
                handler_id: "handler".to_string(),
                generation: 0,
                event_generation: 0,
                plugin_generation: 0,
            }),
            EventPlaneStatus::Accepted
        );
        let payload = serde_json::json!({"ok": true});
        assert_eq!(
            router.try_ingress("owner", "ready", &payload, Instant::now()),
            EventPlaneStatus::Accepted
        );
        router.test_fail_next_age_reserve();
        assert_eq!(
            router.try_ingress("owner", "ready", &payload, Instant::now()),
            EventPlaneStatus::Accepted
        );
        assert!(router.counters().snapshot().event_age_sample_failures >= 1);
    }

    #[test]
    fn registry_lock_does_not_block_ingress_or_retirement() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("owner", "ready")])
            .expect("register");
        assert_eq!(
            router.try_subscribe(EventSubscription {
                plugin_key: "consumer".to_string(),
                owner: "owner".to_string(),
                name: "ready".to_string(),
                handler_id: "handler".to_string(),
                generation: 0,
                event_generation: 0,
                plugin_generation: 0,
            }),
            EventPlaneStatus::Accepted
        );
        let payload = serde_json::json!({"ok": true});
        router.counters().test_with_registry_held(|| {
            assert_eq!(
                router.try_ingress("owner", "ready", &payload, Instant::now()),
                EventPlaneStatus::Accepted
            );
            let mut batch = router
                .pull_ready_batch(1, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
                .expect("pull");
            let delivery = batch.pop().expect("delivery");
            router
                .complete_pulled_delivery(delivery)
                .unwrap_or_else(|_| panic!("retire while registry held"));
        });
    }

    fn consumer_row(
        router: &PackageEventRouter,
        identity: &str,
    ) -> botster_hub_client::DaemonQueueAgeObservation {
        router
            .counters()
            .snapshot()
            .queue_ages
            .into_iter()
            .find(|row| row.kind == DaemonQueueKind::Consumer && row.identity == identity)
            .expect("consumer row")
    }

    #[test]
    fn existing_consumer_age_store_is_allocation_free() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let guard = crate::event_plane_counters::alloc_scope::AllocGuard::enter();
        router.test_refresh_consumer_age("consumer");
        assert_eq!(
            guard.count(),
            0,
            "refreshing an existing consumer age cell must not allocate"
        );
    }

    #[test]
    fn consumer_oldest_age_tracks_front_envelope_across_mutations() {
        let router = router();
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        thread::sleep(StdDuration::from_millis(3));
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let after_second = consumer_row(&router, "consumer");
        assert_eq!(
            after_second.state,
            botster_hub_client::DaemonQueueAgeState::Usable
        );
        assert_eq!(after_second.queue_count, Some(2));
        let oldest_after_second = after_second.oldest_age_us.expect("oldest after second");
        assert!(
            oldest_after_second >= 1_000,
            "second enqueue must keep the first envelope age, got {oldest_after_second}"
        );

        let mut batch = router
            .pull_ready_batch(1, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("pull");
        let delivery = batch.pop().expect("first delivery");
        let after_pull = consumer_row(&router, "consumer");
        assert_eq!(after_pull.queue_count, Some(1));
        let oldest_after_pull = after_pull.oldest_age_us.expect("oldest after pull");
        assert!(
            oldest_after_pull < oldest_after_second,
            "pulling the front must expose the newer remaining envelope"
        );
        router
            .requeue_delivery(delivery)
            .unwrap_or_else(|_| panic!("requeue"));
        let after_requeue = consumer_row(&router, "consumer");
        assert_eq!(after_requeue.queue_count, Some(2));
        let oldest_after_requeue = after_requeue.oldest_age_us.expect("oldest after requeue");
        assert!(
            oldest_after_requeue >= oldest_after_second.saturating_sub(2_000),
            "requeue to the front must restore the older envelope age"
        );
    }

    #[test]
    fn consumer_expiry_and_byte_limit_requeue_refresh_oldest_age() {
        let policy = PackageEventPlanePolicy {
            queue_age: StdDuration::from_millis(1),
            consumer_queue_max_bytes: 64,
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        thread::sleep(StdDuration::from_millis(3));
        let expired = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("expire pull");
        assert!(expired.is_empty(), "expired copies must not be delivered");
        let after_expiry = consumer_row(&router, "consumer");
        assert_eq!(
            after_expiry.state,
            botster_hub_client::DaemonQueueAgeState::Empty
        );
        assert_eq!(after_expiry.queue_count, Some(0));

        let router = PackageEventRouter::new(PackageEventPlanePolicy::default());
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        thread::sleep(StdDuration::from_millis(3));
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let before = consumer_row(&router, "consumer");
        let oldest_before = before.oldest_age_us.expect("oldest before byte cut");
        let batch = router
            .pull_ready_batch(8, 1, Instant::now(), StdDuration::from_millis(8))
            .expect("byte-limit pull");
        assert_eq!(batch.len(), 1, "first copy fills the byte budget");
        let after_cut = consumer_row(&router, "consumer");
        assert_eq!(after_cut.queue_count, Some(1));
        let oldest_after_cut = after_cut.oldest_age_us.expect("oldest after byte cut");
        assert!(
            oldest_after_cut < oldest_before,
            "byte-limit requeue must keep the remaining front envelope, not the pulled one"
        );
    }

    #[test]
    fn repeated_requeue_occupancy_stays_net_zero_until_queue_age_expires() {
        let policy = PackageEventPlanePolicy {
            queue_age: StdDuration::from_millis(40),
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        router
            .try_register_contracts(vec![sample_contract("producer", "sample.ready")])
            .expect("register");
        subscribe(&router, "consumer", "producer", "sample.ready");
        assert_eq!(
            router.try_ingress(
                "producer",
                "sample.ready",
                &serde_json::json!({ "ok": true }),
                Instant::now()
            ),
            EventPlaneStatus::Accepted
        );
        let baseline = router.snapshot().expect("baseline");
        let baseline_events = *baseline
            .consumer_events
            .get("consumer")
            .expect("consumer occupancy");
        let baseline_bytes = *baseline
            .consumer_bytes
            .get("consumer")
            .expect("consumer bytes");
        let mut cycles = 0_u32;
        let deadline = Instant::now() + StdDuration::from_millis(200);
        loop {
            let before = router.snapshot().expect("before pull");
            let before_events = *before.consumer_events.get("consumer").unwrap_or(&0);
            let mut batch = router
                .pull_ready_batch(1, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
                .expect("pull");
            if batch.is_empty() {
                break;
            }
            cycles += 1;
            let after_pull = router.snapshot().expect("after pull");
            let after_events = *after_pull.consumer_events.get("consumer").unwrap_or(&0);
            assert_eq!(
                after_events,
                before_events.saturating_sub(1),
                "pull decrements occupancy before admission"
            );
            let delivery = batch.pop().expect("delivery");
            router
                .requeue_delivery(delivery)
                .unwrap_or_else(|_| panic!("requeue"));
            let after_requeue = router.snapshot().expect("after requeue");
            assert_eq!(
                *after_requeue
                    .consumer_events
                    .get("consumer")
                    .expect("requeue occupancy"),
                before_events,
                "requeue restores occupancy, so capacity cannot bound the cycle"
            );
            assert_eq!(
                *after_requeue
                    .consumer_bytes
                    .get("consumer")
                    .expect("requeue bytes"),
                baseline_bytes
            );
            assert!(
                Instant::now() < deadline,
                "queue_age must retire the holder before the test deadline; cycles={cycles}"
            );
        }
        assert!(
            cycles >= 1,
            "the cycle must pull and requeue at least once before expiry"
        );
        let final_snapshot = router.snapshot().expect("final");
        assert_eq!(
            *final_snapshot.consumer_events.get("consumer").unwrap_or(&0),
            0
        );
        assert_eq!(final_snapshot.queued_holders, 0);
        assert_eq!(final_snapshot.admitted_holders, 0);
        assert_eq!(
            baseline_events, 1,
            "one delivery is the occupancy that must stay net-zero across cycles"
        );
        let observ = router.test_observability_snapshot();
        assert!(
            observ.event_router_queue_age_expiries >= 1,
            "pull_ready_batch must record router queue-age expiry when the preserved enqueued_at expires, got {}",
            observ.event_router_queue_age_expiries
        );
    }
}
