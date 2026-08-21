//! Send-safe package event router.
//!
//! This module owns contracts, exact subscriptions, token buckets, occupancy,
//! and transient queues. It must not import HubRuntime, CoreDaemon, mlua, plugin
//! persistence, or the owner loop.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::config::PackageEventPlanePolicy;
use crate::daemon_event_subscriptions::ClientEventMailbox;
use crate::event_plane_counters::{
    AgeIdentity, EventPlaneCounters, ProducerAgeList, ProducerAgeRef, QueueAgeMetric,
};
use crate::package_event_schema::{CompiledEventSchema, worktree_lifecycle_schema};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerApplyResult {
    Applied,
    WouldBlock,
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
}

struct QueuedCopy {
    envelope_id: u64,
    holder: EventSubscription,
}

struct AdmittedHolder {
    #[allow(dead_code)]
    envelope_id: u64,
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
    contracts: HashMap<(String, String), EmittedContract>,
    subscriptions: HashMap<(String, String), Vec<EventSubscription>>,
    client_holders: HashMap<(String, String), Vec<ClientEventHolder>>,
    client_by_id: HashMap<(String, String), (String, String)>,
    subscriptions_per_plugin: HashMap<String, usize>,
    producer: HashMap<String, ProducerOccupancy>,
    consumers: HashMap<String, ConsumerQueue>,
    envelopes: HashMap<u64, Envelope>,
    admitted: HashMap<(u64, String, u64), AdmittedHolder>,
    buckets: HashMap<String, TokenBucket>,
    next_envelope: u64,
    next_pull: u64,
    outstanding_pulls: HashSet<u64>,
    package_generation: HashMap<String, u64>,
    producer_age_lists: HashMap<(String, u64), ProducerAgeList>,
}

/// Send + Sync router. Every public API uses `try_lock` only.
pub struct PackageEventRouter {
    inner: Mutex<RouterInner>,
    counters: Arc<EventPlaneCounters>,
    delivery_wake: AtomicBool,
    next_holder_key: AtomicU64,
    fail_next_age_reserve: AtomicBool,
    policy: PackageEventPlanePolicy,
}

impl PackageEventRouter {
    #[must_use]
    pub fn new(policy: PackageEventPlanePolicy) -> Self {
        let mut contracts = HashMap::new();
        let schema = worktree_lifecycle_schema();
        for name in WORKTREE_EVENT_NAMES {
            let key = (HUB_EVENT_OWNER.to_string(), (*name).to_string());
            contracts.insert(
                key,
                EmittedContract {
                    owner: HUB_EVENT_OWNER.to_string(),
                    name: (*name).to_string(),
                    audience: BTreeSet::from([EventAudience::Plugins]),
                    schema: schema.clone(),
                    package_generation: 0,
                },
            );
        }
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
                subscriptions: HashMap::new(),
                client_holders: HashMap::new(),
                client_by_id: HashMap::new(),
                subscriptions_per_plugin: HashMap::new(),
                producer,
                consumers: HashMap::new(),
                envelopes: HashMap::new(),
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
            inner
                .contracts
                .insert((contract.owner.clone(), contract.name.clone()), contract);
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
    pub fn try_replace_package_generation(
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
        preview_package_replacement(&inner, owner, &contracts, &subscriptions)?;
        let unload_generation = inner.package_generation.get(owner).copied().unwrap_or(0);
        apply_unload(&mut inner, &self.counters, owner, unload_generation);
        commit_package_generation_locked(
            &mut inner,
            &self.counters,
            owner,
            contracts,
            subscriptions,
        )
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
            .get(&(caller_owner.to_string(), name.to_string()))
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
            .get(&(caller_owner.to_string(), name.to_string()))
            .into_iter()
            .flatten()
            .filter(|subscription| {
                inner
                    .contracts
                    .get(&(subscription.owner.clone(), subscription.name.clone()))
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
        for subscription in selected {
            let consumer = inner
                .consumers
                .entry(subscription.plugin_key.clone())
                .or_default();
            if consumer.events + 1 > consumer_event_max || consumer.bytes + size > consumer_byte_max
            {
                continue;
            }
            accepted.push(subscription);
        }
        if accepted.is_empty() {
            return if inner
                .subscriptions
                .get(&(caller_owner.to_string(), name.to_string()))
                .is_none_or(Vec::is_empty)
            {
                deliver_to_client_holders(&inner, caller_owner, name, payload, encoded.len());
                EventPlaneStatus::Accepted
            } else {
                EventPlaneStatus::ShedFull
            };
        }
        deliver_to_client_holders(&inner, caller_owner, name, payload, encoded.len());
        let envelope_id = inner.next_envelope;
        inner.next_envelope = inner.next_envelope.saturating_add(1);
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
            },
        );
        let producer = inner.producer.entry(caller_owner.to_string()).or_default();
        producer.events += 1;
        producer.bytes += size;
        let mut consumer_keys = Vec::new();
        for subscription in accepted {
            let plugin_key = subscription.plugin_key.clone();
            let consumer = inner.consumers.entry(plugin_key.clone()).or_default();
            consumer.events += 1;
            consumer.bytes += size;
            consumer.copies.push_back(QueuedCopy {
                envelope_id,
                holder: subscription,
            });
            consumer_keys.push(plugin_key);
        }
        for plugin_key in consumer_keys {
            update_consumer_age(&mut inner, &plugin_key, &self.counters);
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
        let consumer_keys: Vec<String> = inner.consumers.keys().cloned().collect();
        for plugin_key in consumer_keys {
            if ready.len() >= max_items || started.elapsed() >= max_elapsed {
                break;
            }
            loop {
                if ready.len() >= max_items || started.elapsed() >= max_elapsed {
                    break;
                }
                let Some(copy) = inner
                    .consumers
                    .get_mut(&plugin_key)
                    .and_then(|queue| queue.copies.pop_front())
                else {
                    break;
                };
                let Some((size, expired)) =
                    inner.envelopes.get(&copy.envelope_id).map(|envelope| {
                        (
                            envelope.size,
                            envelope.enqueued_at.elapsed() > inner.policy.queue_age,
                        )
                    })
                else {
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
                if let Some(envelope) = inner.envelopes.get(&copy.envelope_id) {
                    self.counters.record_delivery_latency(
                        u64::try_from(envelope.enqueued_at.elapsed().as_micros())
                            .unwrap_or(u64::MAX),
                    );
                }
                update_consumer_age(&mut inner, &plugin_key, &self.counters);
                let (owner, name, payload, payload_json) = inner
                    .envelopes
                    .get(&copy.envelope_id)
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
        if !ready.is_empty()
            || inner
                .consumers
                .values()
                .any(|queue| !queue.copies.is_empty())
        {
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
        inner.admitted.insert(
            (envelope_id, plugin_key.to_string(), generation),
            AdmittedHolder {
                envelope_id,
                retired: false,
            },
        );
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
        if !inner.envelopes.contains_key(&delivery.envelope_id) {
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
        let plugin_key = delivery.holder.plugin_key.clone();
        consumer.copies.push_front(QueuedCopy {
            envelope_id: delivery.envelope_id,
            holder: delivery.holder,
        });
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
        inner.admitted.insert(
            (
                delivery.envelope_id,
                delivery.holder.plugin_key.clone(),
                delivery.holder.generation,
            ),
            AdmittedHolder {
                envelope_id: delivery.envelope_id,
                retired: false,
            },
        );
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
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(_) => return OwnerApplyResult::WouldBlock,
        };
        match op.kind {
            OwnerOpKind::Unload => {
                apply_unload(&mut inner, &self.counters, &op.owner, op.generation)
            }
            OwnerOpKind::Reload => {}
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
                    .contains_key(&(owner.to_string(), name.to_string()))
            })
            .unwrap_or(false)
    }
}

impl RouterInner {
    fn global_bytes(&self) -> usize {
        self.envelopes.values().map(|envelope| envelope.size).sum()
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
    inner
        .contracts
        .values()
        .any(|contract| contract.name == name && contract.owner != caller)
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
    contracts: HashMap<(String, String), EmittedContract>,
    subscriptions: HashMap<(String, String), Vec<EventSubscription>>,
    subscriptions_per_plugin: HashMap<String, usize>,
    package_generation: HashMap<String, u64>,
}

fn snapshot_admission(inner: &RouterInner) -> AdmissionSnapshot {
    AdmissionSnapshot {
        contracts: inner.contracts.clone(),
        subscriptions: inner.subscriptions.clone(),
        subscriptions_per_plugin: inner.subscriptions_per_plugin.clone(),
        package_generation: inner.package_generation.clone(),
    }
}

fn restore_admission(inner: &mut RouterInner, snapshot: AdmissionSnapshot) {
    inner.contracts = snapshot.contracts;
    inner.subscriptions = snapshot.subscriptions;
    inner.subscriptions_per_plugin = snapshot.subscriptions_per_plugin;
    inner.package_generation = snapshot.package_generation;
}

fn preview_package_replacement(
    inner: &RouterInner,
    owner: &str,
    contracts: &[EmittedContract],
    subscriptions: &[EventSubscription],
) -> Result<(), EventPlaneStatus> {
    let unload_generation = inner.package_generation.get(owner).copied().unwrap_or(0);
    let mut contracts_view = inner.contracts.clone();
    contracts_view.retain(|(contract_owner, _), contract| {
        contract_owner != owner || contract.package_generation > unload_generation
    });
    for contract in contracts {
        if contract.owner == HUB_EVENT_OWNER || contract.owner != owner {
            return Err(EventPlaneStatus::RejectedForeign);
        }
        contracts_view.insert(
            (contract.owner.clone(), contract.name.clone()),
            contract.clone(),
        );
    }
    let mut plugin_counts = inner.subscriptions_per_plugin.clone();
    let mut event_counts: HashMap<(String, String), usize> = HashMap::new();
    for (key, event_subs) in &inner.subscriptions {
        let mut remaining = 0;
        for subscription in event_subs {
            let drop_producer =
                subscription.owner == owner && subscription.event_generation <= unload_generation;
            let drop_consumer = subscription.plugin_key == owner
                && subscription.plugin_generation <= unload_generation;
            if drop_producer || drop_consumer {
                if let Some(count) = plugin_counts.get_mut(&subscription.plugin_key) {
                    *count = count.saturating_sub(1);
                }
            } else {
                remaining += 1;
            }
        }
        event_counts.insert(key.clone(), remaining);
    }
    for subscription in subscriptions {
        if is_wildcard(&subscription.owner) || is_wildcard(&subscription.name) {
            return Err(EventPlaneStatus::RejectedWildcard);
        }
        if subscription.owner.trim().is_empty() || subscription.name.trim().is_empty() {
            return Err(EventPlaneStatus::RejectedInvalid);
        }
        let key = (subscription.owner.clone(), subscription.name.clone());
        let Some(contract) = contracts_view.get(&key) else {
            return Err(EventPlaneStatus::RejectedUndeclared);
        };
        if !contract.audience.contains(&EventAudience::Plugins) {
            return Err(EventPlaneStatus::RejectedAudience);
        }
        let plugin_count = plugin_counts
            .get(&subscription.plugin_key)
            .copied()
            .unwrap_or(0);
        if plugin_count >= inner.policy.subscriptions_per_plugin_max {
            return Err(EventPlaneStatus::RejectedInvalid);
        }
        let event_count = event_counts.get(&key).copied().unwrap_or(0);
        if event_count >= inner.policy.subscribers_per_event_max {
            return Err(EventPlaneStatus::RejectedOverFanout);
        }
        *plugin_counts
            .entry(subscription.plugin_key.clone())
            .or_insert(0) += 1;
        *event_counts.entry(key).or_insert(0) += 1;
    }
    Ok(())
}

fn commit_package_generation_locked(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    contracts: Vec<EmittedContract>,
    subscriptions: Vec<EventSubscription>,
) -> Result<u64, EventPlaneStatus> {
    if contracts.is_empty() && subscriptions.is_empty() {
        return Ok(inner.package_generation.get(owner).copied().unwrap_or(0));
    }
    let snapshot = snapshot_admission(inner);
    let generation = bump_package_generation(inner, owner);
    for mut contract in contracts {
        contract.package_generation = generation;
        inner
            .contracts
            .insert((contract.owner.clone(), contract.name.clone()), contract);
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
    if is_wildcard(&subscription.owner) || is_wildcard(&subscription.name) {
        return EventPlaneStatus::RejectedWildcard;
    }
    if subscription.owner.trim().is_empty() || subscription.name.trim().is_empty() {
        return EventPlaneStatus::RejectedInvalid;
    }
    let key = (subscription.owner.clone(), subscription.name.clone());
    let Some(contract) = inner.contracts.get(&key) else {
        return EventPlaneStatus::RejectedUndeclared;
    };
    if !contract.audience.contains(&EventAudience::Plugins) {
        return EventPlaneStatus::RejectedAudience;
    }
    let event_generation = contract.package_generation;
    let plugin_generation = inner
        .package_generation
        .get(&subscription.plugin_key)
        .copied()
        .unwrap_or(0);
    let plugin_count = inner
        .subscriptions_per_plugin
        .get(&subscription.plugin_key)
        .copied()
        .unwrap_or(0);
    if plugin_count >= inner.policy.subscriptions_per_plugin_max {
        return EventPlaneStatus::RejectedInvalid;
    }
    let max_subscribers = inner.policy.subscribers_per_event_max;
    let event_subs = inner.subscriptions.entry(key).or_default();
    if event_subs.len() >= max_subscribers {
        return EventPlaneStatus::RejectedOverFanout;
    }
    let mut subscription = subscription;
    subscription.event_generation = event_generation;
    subscription.plugin_generation = plugin_generation;
    let plugin_key = subscription.plugin_key.clone();
    event_subs.push(subscription);
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
    let Some(contract) = inner.contracts.get(&key) else {
        return EventPlaneStatus::RejectedUndeclared;
    };
    if !contract.audience.contains(&EventAudience::Clients) {
        return EventPlaneStatus::RejectedAudience;
    }
    let identity = (holder.connection_id.clone(), holder.subscription_id.clone());
    if inner.client_by_id.contains_key(&identity) {
        return EventPlaneStatus::RejectedInvalid;
    }
    let max_subscribers = inner.policy.subscribers_per_event_max;
    let holders = inner.client_holders.entry(key).or_default();
    if holders.len() >= max_subscribers {
        return EventPlaneStatus::RejectedOverFanout;
    }
    inner
        .client_by_id
        .insert(identity, (holder.owner.clone(), holder.name.clone()));
    holders.push(holder);
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
    if let Some(holders) = inner.client_holders.get_mut(&key) {
        holders.retain(|holder| {
            !(holder.connection_id == connection_id && holder.subscription_id == subscription_id)
        });
        if holders.is_empty() {
            inner.client_holders.remove(&key);
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
        .get(&(owner.to_string(), name.to_string()))
    else {
        return;
    };
    for holder in holders {
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
) {
    if owner == HUB_EVENT_OWNER {
        return;
    }
    inner.contracts.retain(|(contract_owner, _), contract| {
        contract_owner != owner || contract.package_generation > generation
    });
    let mut removed_counts: HashMap<String, usize> = HashMap::new();
    for subscriptions in inner.subscriptions.values_mut() {
        subscriptions.retain(|subscription| {
            let drop_producer =
                subscription.owner == owner && subscription.event_generation <= generation;
            let drop_consumer =
                subscription.plugin_key == owner && subscription.plugin_generation <= generation;
            if drop_producer || drop_consumer {
                *removed_counts
                    .entry(subscription.plugin_key.clone())
                    .or_insert(0) += 1;
                false
            } else {
                true
            }
        });
    }
    inner
        .subscriptions
        .retain(|_, subscriptions| !subscriptions.is_empty());
    let mut removed_client_ids = Vec::new();
    for holders in inner.client_holders.values_mut() {
        holders.retain(|holder| {
            let drop_holder = holder.owner == owner;
            if drop_holder {
                removed_client_ids
                    .push((holder.connection_id.clone(), holder.subscription_id.clone()));
            }
            !drop_holder
        });
    }
    inner
        .client_holders
        .retain(|_, holders| !holders.is_empty());
    for identity in removed_client_ids {
        inner.client_by_id.remove(&identity);
    }
    for (plugin, removed) in removed_counts {
        if let Some(count) = inner.subscriptions_per_plugin.get_mut(&plugin) {
            *count = count.saturating_sub(removed);
        }
    }
    drop_queued_for_owner(inner, counters, owner);
    retire_owner_diagnostics(inner, counters, owner, generation);
}

fn drop_queued_for_owner(inner: &mut RouterInner, counters: &EventPlaneCounters, owner: &str) {
    let mut dropped = Vec::new();
    for (plugin_key, queue) in inner.consumers.iter_mut() {
        let mut kept = VecDeque::new();
        while let Some(copy) = queue.copies.pop_front() {
            let drop_copy = copy.holder.owner == owner
                || copy.holder.plugin_key == owner
                || plugin_key == owner;
            if drop_copy {
                queue.events = queue.events.saturating_sub(1);
                dropped.push(copy);
            } else {
                kept.push_back(copy);
            }
        }
        queue.copies = kept;
    }
    for copy in dropped {
        if let Some(envelope) = inner.envelopes.get(&copy.envelope_id) {
            let size = envelope.size;
            if let Some(queue) = inner.consumers.get_mut(&copy.holder.plugin_key) {
                queue.bytes = queue.bytes.saturating_sub(size);
            }
        }
        retire_holder_locked(
            inner,
            counters,
            copy.envelope_id,
            &copy.holder.plugin_key,
            copy.holder.generation,
        );
    }
}

fn retire_holder_locked(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    envelope_id: u64,
    plugin_key: &str,
    generation: u64,
) -> bool {
    let key = (envelope_id, plugin_key.to_string(), generation);
    if let Some(holder) = inner.admitted.get_mut(&key) {
        if holder.retired {
            return false;
        }
        holder.retired = true;
    } else {
        inner.admitted.insert(
            key,
            AdmittedHolder {
                envelope_id,
                retired: true,
            },
        );
    }
    let Some(envelope) = inner.envelopes.get_mut(&envelope_id) else {
        return false;
    };
    envelope.remaining_holders = envelope.remaining_holders.saturating_sub(1);
    if envelope.remaining_holders > 0 {
        return false;
    }
    let owner = envelope.owner.clone();
    let size = envelope.size;
    let age_ref = envelope.producer_age_ref;
    inner.envelopes.remove(&envelope_id);
    if let Some(producer) = inner.producer.get_mut(&owner) {
        producer.events = producer.events.saturating_sub(1);
        producer.bytes = producer.bytes.saturating_sub(size);
    }
    retire_producer_age(inner, counters, &owner, age_ref);
    true
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

fn update_consumer_age(inner: &mut RouterInner, plugin_key: &str, counters: &EventPlaneCounters) {
    let front_id = inner
        .consumers
        .get(plugin_key)
        .and_then(|queue| queue.copies.front().map(|copy| copy.envelope_id));
    let oldest = front_id
        .and_then(|envelope_id| inner.envelopes.get(&envelope_id))
        .map(|envelope| counters.nanos_of(envelope.enqueued_at))
        .unwrap_or(u64::MAX);
    let Some(queue) = inner.consumers.get(plugin_key) else {
        return;
    };
    let count = queue.events as u64;
    let Some(cell) = queue.age_cell.clone() else {
        counters.record_age_sample_failure();
        return;
    };
    cell.store(count, oldest, cell.gate(), false);
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
            );
        }
    }
    if age_ref.generation != current_generation && live == 0 {
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
    counters.prune_retired();
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
    cell.store(0, u64::MAX, prior as u64, false);
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
    let plugin_keys: Vec<String> = inner
        .subscriptions
        .values()
        .flatten()
        .filter(|subscription| subscription.plugin_key == owner || subscription.owner == owner)
        .map(|subscription| subscription.plugin_key.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
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
    if let Some(old) = queue.age_cell.replace(Arc::clone(&consumer_cell)) {
        old.close_writes();
        counters.retire_identity(&AgeIdentity {
            kind: DaemonQueueKind::Consumer,
            identity: plugin_key.to_string(),
            generation: Some(queue.generation),
        });
    }
    queue.generation = generation;
    consumer_cell.store(queue.events as u64, u64::MAX, 0, false);
}

fn retire_owner_diagnostics(
    inner: &mut RouterInner,
    counters: &EventPlaneCounters,
    owner: &str,
    generation: u64,
) {
    counters.retire_identity(&AgeIdentity {
        kind: DaemonQueueKind::Producer,
        identity: owner.to_string(),
        generation: Some(generation),
    });
    counters.retire_identity(&AgeIdentity {
        kind: DaemonQueueKind::Consumer,
        identity: owner.to_string(),
        generation: Some(generation),
    });
    if let Some(queue) = inner.consumers.get_mut(owner)
        && let Some(cell) = queue.age_cell.as_ref()
    {
        cell.store(queue.events as u64, u64::MAX, 0, false);
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

/// Owner-thread-only keyed operations. No mutex. Workers never read this map.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EventPlaneOwnerOps {
    pending: BTreeMap<String, VecDeque<OwnerOp>>,
}

impl EventPlaneOwnerOps {
    pub fn record(&mut self, op: OwnerOp) {
        self.pending
            .entry(op.owner.clone())
            .or_default()
            .push_back(op);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.values().all(VecDeque::is_empty)
    }

    pub fn apply_ready(&mut self, router: &PackageEventRouter) -> Vec<OwnerOp> {
        let mut applied = Vec::new();
        let owners: Vec<String> = self.pending.keys().cloned().collect();
        for owner in owners {
            let Some(queue) = self.pending.get_mut(&owner) else {
                continue;
            };
            while let Some(front) = queue.front() {
                match router.try_apply(front) {
                    OwnerApplyResult::Applied => {
                        if let Some(op) = queue.pop_front() {
                            applied.push(op);
                        }
                    }
                    OwnerApplyResult::WouldBlock => break,
                }
            }
        }
        self.pending.retain(|_, queue| !queue.is_empty());
        applied
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
    EventInFlight { request_id: String },
    PendingEntityPublish { plugin_key: String },
    AdmittedEntityMutation { family: String, seq: u64 },
    ProviderResyncNeed { family: String },
    ProviderInFlight { request_id: String },
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
        }
    }

    #[must_use]
    pub fn mint(&self) -> Option<u64> {
        self.mint_with_lease(None)
    }

    #[must_use]
    pub fn mint_with_lease(&self, identity: Option<LeaseIdentity>) -> Option<u64> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut inner = lock_causal(&self.inner)?;
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
        let Ok(mut inner) = self.inner.try_lock() else {
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
        let Ok(mut inner) = self.inner.try_lock() else {
            return false;
        };
        apply_causal_locked(&mut inner, &CausalOp::Release { scope_id, identity });
        true
    }

    /// Admit one causal op or return it. Never waits or spins.
    ///
    /// Later operations always append behind already-queued operations.
    pub fn try_admit(&self, op: CausalOp) -> CausalAdmitResult {
        let Ok(mut pending) = self.pending.try_lock() else {
            return CausalAdmitResult::Retry(op);
        };
        if pending.is_empty()
            && let Ok(mut inner) = self.inner.try_lock()
        {
            apply_causal_locked(&mut inner, &op);
            return CausalAdmitResult::Applied;
        }
        if pending.len() < CAUSAL_PENDING_MAX {
            pending.push_back(op);
            self.pending_len.fetch_add(1, Ordering::SeqCst);
            return CausalAdmitResult::Applied;
        }
        CausalAdmitResult::Retry(op)
    }

    pub fn flush_pending(&self) -> usize {
        let Ok(mut pending) = self.pending.try_lock() else {
            return 0;
        };
        let Ok(mut inner) = self.inner.try_lock() else {
            return 0;
        };
        let started = Instant::now();
        let mut applied = 0;
        while applied < CAUSAL_FLUSH_MAX && started.elapsed() < Duration::from_millis(8) {
            let Some(op) = pending.pop_front() else {
                break;
            };
            self.pending_len.fetch_sub(1, Ordering::SeqCst);
            apply_causal_locked(&mut inner, &op);
            applied += 1;
        }
        applied
    }

    #[must_use]
    pub fn pending_ops(&self) -> bool {
        self.pending_len.load(Ordering::SeqCst) > 0
    }

    #[doc(hidden)]
    pub fn test_with_inner_held<R>(&self, body: impl FnOnce() -> R) -> R {
        let _guard = self
            .inner
            .try_lock()
            .expect("test hold must acquire causal inner");
        body()
    }

    #[must_use]
    pub fn is_live(&self, scope_id: u64) -> bool {
        match self.inner.try_lock() {
            Ok(inner) => inner
                .scopes
                .get(&scope_id)
                .is_some_and(|scope| scope.leases > 0),
            Err(TryLockError::WouldBlock) => true,
            Err(TryLockError::Poisoned(poisoned)) => {
                drop(poisoned.into_inner());
                true
            }
        }
    }

    #[must_use]
    pub fn lease_count(&self, scope_id: u64) -> Option<u32> {
        self.inner
            .try_lock()
            .ok()
            .and_then(|inner| inner.scopes.get(&scope_id).map(|scope| scope.leases))
    }

    #[must_use]
    pub fn pending_publish_leases(&self) -> Vec<(u64, LeaseIdentity)> {
        let Ok(inner) = self.inner.try_lock() else {
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
        self.inner.try_lock().ok().and_then(|inner| {
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

fn lock_causal(mutex: &Mutex<CausalInner>) -> Option<std::sync::MutexGuard<'_, CausalInner>> {
    match mutex.try_lock() {
        Ok(guard) => Some(guard),
        Err(TryLockError::WouldBlock) => None,
        Err(TryLockError::Poisoned(poisoned)) => {
            drop(poisoned.into_inner());
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PackageEventPlaneOptions;
    use crate::daemon_event_subscriptions::ClientEventMailbox;
    use std::thread;
    use std::time::Duration as StdDuration;

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
        let without_try = production.replace("try_lock", "TRY");
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
        let applied = ops.apply_ready(&router);
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
        let applied = ops.apply_ready(&router);
        assert_eq!(applied.len(), 2);
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
        let applied = ops.apply_ready(&router);
        assert_eq!(applied[0].kind, OwnerOpKind::Unload);
        assert_eq!(applied[1].kind, OwnerOpKind::Reload);
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
        thread::sleep(StdDuration::from_millis(3));
        let batch = router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), StdDuration::from_millis(8))
            .expect("batch");
        assert!(batch.is_empty());
        let snapshot = router.snapshot().expect("snapshot");
        assert_eq!(snapshot.global_in_flight_bytes, 0);
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
        assert_eq!(
            router.try_apply(&OwnerOp {
                kind: OwnerOpKind::Unload,
                owner: "producer".into(),
                generation,
            }),
            OwnerApplyResult::Applied
        );
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
        assert_eq!(
            router.try_apply(&OwnerOp {
                kind: OwnerOpKind::Unload,
                owner: "producer".into(),
                generation: old,
            }),
            OwnerApplyResult::Applied
        );
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
        assert_eq!(table.flush_pending(), 2);
        assert!(!table.pending_ops());
        assert!(!table.is_live(scope));
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
            first <= CAUSAL_FLUSH_MAX,
            "one owner turn must not drain without a bound: {first}"
        );
        assert_eq!(
            table.identities(scopes[0]),
            Some(BTreeSet::from([LeaseIdentity::AdmittedEntityMutation {
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
                        family: "producer.item".into(),
                        seq: 1,
                    }],
                ),
                CausalAdmitResult::Applied
            );
        });
        let first = table.flush_pending();
        assert!(first > 0);
        assert!(first <= CAUSAL_FLUSH_MAX);
        assert!(table.pending_ops());
        assert_eq!(
            table.release(
                live,
                LeaseIdentity::AdmittedEntityMutation {
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
            router.try_replace_package_generation(
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
            ),
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
            router.try_replace_package_generation(
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
            ),
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
}
