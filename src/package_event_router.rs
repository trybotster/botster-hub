//! Send-safe package event router.
//!
//! This module owns contracts, exact subscriptions, token buckets, occupancy,
//! and transient queues. It must not import HubRuntime, CoreDaemon, mlua, plugin
//! persistence, or the owner loop.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::config::PackageEventPlanePolicy;
use crate::package_event_schema::{CompiledEventSchema, worktree_lifecycle_schema};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HolderId {
    pub consumer_plugin_key: u64,
    pub generation: u64,
}

#[derive(Debug, Clone)]
pub struct ReadyDelivery {
    pub envelope_id: u64,
    pub owner: String,
    pub name: String,
    pub payload: Arc<[u8]>,
    pub payload_json: Value,
    pub size: usize,
    pub holder: EventSubscription,
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

struct ProducerOccupancy {
    events: usize,
    bytes: usize,
}

#[derive(Default)]
struct ConsumerQueue {
    events: usize,
    bytes: usize,
    copies: VecDeque<QueuedCopy>,
}

struct TokenBucket {
    tokens: f64,
    last: Instant,
}

struct RouterInner {
    policy: PackageEventPlanePolicy,
    contracts: HashMap<(String, String), EmittedContract>,
    subscriptions: HashMap<(String, String), Vec<EventSubscription>>,
    subscriptions_per_plugin: HashMap<String, usize>,
    producer: HashMap<String, ProducerOccupancy>,
    consumers: HashMap<String, ConsumerQueue>,
    envelopes: HashMap<u64, Envelope>,
    admitted: HashMap<(u64, String, u64), AdmittedHolder>,
    buckets: HashMap<String, TokenBucket>,
    next_envelope: u64,
    package_generation: HashMap<String, u64>,
}

/// Send + Sync router. Every public API uses `try_lock` only.
pub struct PackageEventRouter {
    inner: Mutex<RouterInner>,
    delivery_wake: AtomicBool,
    next_holder_key: AtomicU64,
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
        Self {
            inner: Mutex::new(RouterInner {
                policy,
                contracts,
                subscriptions: HashMap::new(),
                subscriptions_per_plugin: HashMap::new(),
                producer: HashMap::new(),
                consumers: HashMap::new(),
                envelopes: HashMap::new(),
                admitted: HashMap::new(),
                buckets: HashMap::new(),
                next_envelope: 1,
                package_generation: HashMap::new(),
            }),
            delivery_wake: AtomicBool::new(false),
            next_holder_key: AtomicU64::new(1),
            policy,
        }
    }

    #[must_use]
    pub const fn policy(&self) -> PackageEventPlanePolicy {
        self.policy
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
        Ok(())
    }

    pub fn try_subscribe(&self, subscription: EventSubscription) -> EventPlaneStatus {
        if is_wildcard(&subscription.owner) || is_wildcard(&subscription.name) {
            return EventPlaneStatus::RejectedWildcard;
        }
        if subscription.owner.trim().is_empty() || subscription.name.trim().is_empty() {
            return EventPlaneStatus::RejectedInvalid;
        }
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(status) => return status,
        };
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

    pub fn try_ingress(
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
        let producer =
            inner
                .producer
                .entry(caller_owner.to_string())
                .or_insert(ProducerOccupancy {
                    events: 0,
                    bytes: 0,
                });
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
                .or_insert_with(ConsumerQueue::default);
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
                EventPlaneStatus::Accepted
            } else {
                EventPlaneStatus::ShedFull
            };
        }
        let envelope_id = inner.next_envelope;
        inner.next_envelope = inner.next_envelope.saturating_add(1);
        let payload_arc: Arc<[u8]> = encoded.into();
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
            },
        );
        let producer =
            inner
                .producer
                .entry(caller_owner.to_string())
                .or_insert(ProducerOccupancy {
                    events: 0,
                    bytes: 0,
                });
        producer.events += 1;
        producer.bytes += size;
        for subscription in accepted {
            let consumer = inner
                .consumers
                .entry(subscription.plugin_key.clone())
                .or_insert_with(ConsumerQueue::default);
            consumer.events += 1;
            consumer.bytes += size;
            consumer.copies.push_back(QueuedCopy {
                envelope_id,
                holder: subscription,
            });
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
                    retire_holder_locked(
                        &mut inner,
                        copy.envelope_id,
                        &copy.holder.plugin_key,
                        copy.holder.generation,
                    );
                    continue;
                }
                if used_bytes + size > max_bytes && !ready.is_empty() {
                    if let Some(queue) = inner.consumers.get_mut(&plugin_key) {
                        queue.events += 1;
                        queue.bytes += size;
                        queue.copies.push_front(copy);
                    }
                    break;
                }
                used_bytes += size;
                let envelope = inner
                    .envelopes
                    .get(&copy.envelope_id)
                    .expect("envelope exists");
                ready.push(ReadyDelivery {
                    envelope_id: copy.envelope_id,
                    owner: envelope.owner.clone(),
                    name: envelope.name.clone(),
                    payload: envelope.payload.clone(),
                    payload_json: envelope.payload_json.clone(),
                    size,
                    holder: copy.holder,
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
            envelope_id,
            plugin_key,
            generation,
        ))
    }

    pub fn try_apply(&self, op: &OwnerOp) -> OwnerApplyResult {
        let mut inner = match lock_inner(&self.inner) {
            Ok(inner) => inner,
            Err(_) => return OwnerApplyResult::WouldBlock,
        };
        match op.kind {
            OwnerOpKind::Unload => apply_unload(&mut inner, &op.owner, op.generation),
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

    #[cfg(test)]
    pub fn test_with_inner_held<R>(&self, body: impl FnOnce() -> R) -> R {
        let _guard = self.inner.try_lock().expect("test hold must acquire inner");
        body()
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

fn apply_unload(inner: &mut RouterInner, owner: &str, generation: u64) {
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
    for (plugin, removed) in removed_counts {
        if let Some(count) = inner.subscriptions_per_plugin.get_mut(&plugin) {
            *count = count.saturating_sub(removed);
        }
    }
    drop_queued_for_owner(inner, owner);
}

fn drop_queued_for_owner(inner: &mut RouterInner, owner: &str) {
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
            copy.envelope_id,
            &copy.holder.plugin_key,
            copy.holder.generation,
        );
    }
}

fn retire_holder_locked(
    inner: &mut RouterInner,
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
    inner.envelopes.remove(&envelope_id);
    if let Some(producer) = inner.producer.get_mut(&owner) {
        producer.events = producer.events.saturating_sub(1);
        producer.bytes = producer.bytes.saturating_sub(size);
    }
    true
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

/// Causal-scope lease table. Send + Sync. Lives beside the router.
#[derive(Debug)]
pub struct CausalScopeTable {
    inner: Mutex<CausalInner>,
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

    pub fn release(&self, scope_id: u64, identity: LeaseIdentity) -> bool {
        let Ok(mut inner) = self.inner.try_lock() else {
            return false;
        };
        let Some(scope) = inner.scopes.get_mut(&scope_id) else {
            return false;
        };
        if scope.identities.remove(&identity) {
            scope.leases = scope.leases.saturating_sub(1);
        }
        if scope.leases == 0 {
            inner.scopes.remove(&scope_id);
            return true;
        }
        false
    }

    #[must_use]
    pub fn is_live(&self, scope_id: u64) -> bool {
        self.inner
            .try_lock()
            .ok()
            .and_then(|inner| inner.scopes.get(&scope_id).map(|scope| scope.leases > 0))
            .unwrap_or(true)
    }

    #[must_use]
    pub fn lease_count(&self, scope_id: u64) -> Option<u32> {
        self.inner
            .try_lock()
            .ok()
            .and_then(|inner| inner.scopes.get(&scope_id).map(|scope| scope.leases))
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
}
