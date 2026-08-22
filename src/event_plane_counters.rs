//! Bounded event-plane observability stored beside `PackageEventRouter`.
//!
//! Saturation-time reads use atomics and short registry read guards. They never
//! take `PackageEventRouter::inner`. Event and retirement paths update cells
//! through a direct `Arc` and never take the registry lock.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use botster_hub_client::{
    DaemonLatencyHistogram, DaemonObservabilityCounters, DaemonQueueAgeObservation,
    DaemonQueueAgeState, DaemonQueueKind,
};

pub const LATENCY_BUCKETS: usize = 13;
pub const EVENT_PLANE_STATUS_COUNT: usize = 12;
const NIL: u32 = u32::MAX;
const EMPTY_OLDEST: u64 = u64::MAX;

/// Oldest-age cell for one producer generation, consumer identity, or mailbox.
pub struct QueueAgeMetric {
    version: AtomicU64,
    count: AtomicU64,
    bytes: AtomicU64,
    oldest_nanos: AtomicU64,
    gate: AtomicU64,
    generation: u64,
    invalid: AtomicBool,
    write_closed: AtomicBool,
}

impl QueueAgeMetric {
    #[must_use]
    pub fn new(generation: u64) -> Self {
        Self {
            version: AtomicU64::new(0),
            count: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            oldest_nanos: AtomicU64::new(EMPTY_OLDEST),
            gate: AtomicU64::new(0),
            generation,
            invalid: AtomicBool::new(false),
            write_closed: AtomicBool::new(false),
        }
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn is_write_closed(&self) -> bool {
        self.write_closed.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn gate(&self) -> u64 {
        self.gate.load(Ordering::Relaxed)
    }

    pub fn close_writes(&self) {
        self.write_closed.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn occupied_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Two-phase odd/even write. No-op after the cell is write-closed.
    pub fn store(&self, count: u64, oldest_nanos: u64, gate: u64, latch_invalid: bool, bytes: u64) {
        if self.write_closed.load(Ordering::Relaxed) {
            return;
        }
        self.version.fetch_add(1, Ordering::AcqRel);
        self.count.store(count, Ordering::Relaxed);
        self.bytes.store(bytes, Ordering::Relaxed);
        self.oldest_nanos.store(oldest_nanos, Ordering::Relaxed);
        self.gate.store(gate, Ordering::Relaxed);
        if latch_invalid {
            self.invalid.store(true, Ordering::Relaxed);
        }
        self.version.fetch_add(1, Ordering::Release);
    }

    pub fn latch_invalid(&self) {
        if self.write_closed.load(Ordering::Relaxed) {
            return;
        }
        self.version.fetch_add(1, Ordering::AcqRel);
        self.invalid.store(true, Ordering::Relaxed);
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Bounded consistency read: at most one retry, no lock, no allocation.
    #[must_use]
    pub fn sample(&self) -> AgeSample {
        match self.sample_once() {
            Some(sample) => sample,
            None => self.sample_once().unwrap_or(AgeSample::Indeterminate),
        }
    }

    fn sample_once(&self) -> Option<AgeSample> {
        let v1 = self.version.load(Ordering::Acquire);
        if v1 % 2 == 1 {
            return None;
        }
        let count = self.count.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let oldest_nanos = self.oldest_nanos.load(Ordering::Relaxed);
        let gate = self.gate.load(Ordering::Relaxed);
        let invalid = self.invalid.load(Ordering::Relaxed);
        fence(Ordering::Acquire);
        let v2 = self.version.load(Ordering::Relaxed);
        if v1 != v2 {
            return None;
        }
        if invalid || gate > 0 {
            return Some(AgeSample::Indeterminate);
        }
        if count == 0 {
            return Some(AgeSample::Empty { count: 0, bytes });
        }
        Some(AgeSample::Usable {
            count,
            bytes,
            oldest_nanos,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgeSample {
    Usable {
        count: u64,
        bytes: u64,
        oldest_nanos: u64,
    },
    Empty {
        count: u64,
        bytes: u64,
    },
    Indeterminate,
}

struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS],
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }

    fn observe(&self, us: u64) {
        self.buckets[bucket_index(us)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }

    fn snapshot(&self) -> DaemonLatencyHistogram {
        DaemonLatencyHistogram::new(
            self.buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            self.count.load(Ordering::Relaxed),
            self.sum_us.load(Ordering::Relaxed),
            self.max_us.load(Ordering::Relaxed),
        )
    }
}

/// One arithmetic step: `leading_zeros` plus a min. No loop, no scan.
#[must_use]
pub fn bucket_index(us: u64) -> usize {
    if us == 0 {
        return 0;
    }
    let log = (63 - us.leading_zeros()) as usize;
    log.min(LATENCY_BUCKETS - 1)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AgeIdentity {
    pub kind: DaemonQueueKind,
    pub identity: String,
    pub generation: Option<u64>,
}

struct AgeRegistryEntry {
    cell: Option<Arc<QueueAgeMetric>>,
    retired: bool,
}

/// Intrusive doubly-linked producer age list over preallocated slots.
pub struct ProducerAgeList {
    slots: Box<[ProducerAgeSlot]>,
    head: u32,
    tail: u32,
    free: u32,
    live: usize,
    generation: u64,
    cell: Arc<QueueAgeMetric>,
    #[cfg(test)]
    ops: AtomicU64,
}

pub struct ProducerAgeSlot {
    pub nanos: u64,
    prev: u32,
    next: u32,
}

impl ProducerAgeList {
    #[must_use]
    pub fn new(capacity: usize, generation: u64, cell: Arc<QueueAgeMetric>) -> Self {
        let cap = capacity.max(1) as u32;
        let mut slots = Vec::with_capacity(cap as usize);
        for index in 0..cap {
            let next = if index + 1 == cap { NIL } else { index + 1 };
            slots.push(ProducerAgeSlot {
                nanos: 0,
                prev: NIL,
                next,
            });
        }
        Self {
            slots: slots.into_boxed_slice(),
            head: NIL,
            tail: NIL,
            free: 0,
            live: 0,
            generation,
            cell,
            #[cfg(test)]
            ops: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn cell(&self) -> &Arc<QueueAgeMetric> {
        &self.cell
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn live(&self) -> usize {
        self.live
    }

    #[cfg(test)]
    #[must_use]
    pub fn test_ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn test_reset_ops(&self) {
        self.ops.store(0, Ordering::Relaxed);
    }

    fn count_op(&self) {
        #[cfg(test)]
        self.ops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn push(&mut self, nanos: u64) -> Option<u32> {
        self.count_op();
        if self.free == NIL {
            return None;
        }
        let slot = self.free;
        self.free = self.slots[slot as usize].next;
        self.slots[slot as usize].nanos = nanos;
        self.slots[slot as usize].next = NIL;
        self.slots[slot as usize].prev = self.tail;
        if self.tail == NIL {
            self.head = slot;
        } else {
            self.slots[self.tail as usize].next = slot;
        }
        self.tail = slot;
        self.live += 1;
        Some(slot)
    }

    pub fn remove(&mut self, slot: u32) {
        self.count_op();
        if (slot as usize) >= self.slots.len() {
            return;
        }
        let prev = self.slots[slot as usize].prev;
        let next = self.slots[slot as usize].next;
        if prev == NIL {
            self.head = next;
        } else {
            self.slots[prev as usize].next = next;
        }
        if next == NIL {
            self.tail = prev;
        } else {
            self.slots[next as usize].prev = prev;
        }
        self.slots[slot as usize].prev = NIL;
        self.slots[slot as usize].next = self.free;
        self.free = slot;
        self.live = self.live.saturating_sub(1);
    }

    #[must_use]
    pub fn oldest_nanos(&self) -> u64 {
        if self.head == NIL {
            EMPTY_OLDEST
        } else {
            self.slots[self.head as usize].nanos
        }
    }

    pub fn publish(&self) {
        self.cell.store(
            self.live as u64,
            self.oldest_nanos(),
            self.cell.gate(),
            false,
            self.cell.occupied_bytes(),
        );
    }
}

/// Generation-specific producer slot carried on an envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerAgeRef {
    pub generation: u64,
    pub slot: u32,
}

pub struct EventPlaneCounters {
    origin: Instant,
    shed_by_reason: [AtomicU64; EVENT_PLANE_STATUS_COUNT],
    admission_attempts: AtomicU64,
    delivery_attempts: AtomicU64,
    admission_latency: LatencyHistogram,
    delivery_latency: LatencyHistogram,
    handler_timed_out: AtomicU64,
    handler_failed: AtomicU64,
    handler_cancelled: AtomicU64,
    handler_backpressured: AtomicU64,
    handler_worker_stopped: AtomicU64,
    handler_completed_ok: AtomicU64,
    router_queue_age_expiries: AtomicU64,
    mailbox_queue_age_expiries: AtomicU64,
    mailbox_overflow_gaps: AtomicU64,
    event_gaps: AtomicU64,
    age_sample_failures: AtomicU64,
    last_owner_turn_us: AtomicU64,
    max_owner_turn_us: AtomicU64,
    last_ready_operation_wait_us: AtomicU64,
    max_ready_operation_wait_us: AtomicU64,
    stalled_write_timeouts: AtomicU64,
    global_in_flight_bytes: AtomicU64,
    registry: RwLock<HashMap<AgeIdentity, AgeRegistryEntry>>,
}

impl Default for EventPlaneCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl EventPlaneCounters {
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
            shed_by_reason: std::array::from_fn(|_| AtomicU64::new(0)),
            admission_attempts: AtomicU64::new(0),
            delivery_attempts: AtomicU64::new(0),
            admission_latency: LatencyHistogram::new(),
            delivery_latency: LatencyHistogram::new(),
            handler_timed_out: AtomicU64::new(0),
            handler_failed: AtomicU64::new(0),
            handler_cancelled: AtomicU64::new(0),
            handler_backpressured: AtomicU64::new(0),
            handler_worker_stopped: AtomicU64::new(0),
            handler_completed_ok: AtomicU64::new(0),
            router_queue_age_expiries: AtomicU64::new(0),
            mailbox_queue_age_expiries: AtomicU64::new(0),
            mailbox_overflow_gaps: AtomicU64::new(0),
            event_gaps: AtomicU64::new(0),
            age_sample_failures: AtomicU64::new(0),
            last_owner_turn_us: AtomicU64::new(0),
            max_owner_turn_us: AtomicU64::new(0),
            last_ready_operation_wait_us: AtomicU64::new(0),
            max_ready_operation_wait_us: AtomicU64::new(0),
            stalled_write_timeouts: AtomicU64::new(0),
            global_in_flight_bytes: AtomicU64::new(0),
            registry: RwLock::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn origin(&self) -> Instant {
        self.origin
    }

    #[must_use]
    pub fn nanos_of(&self, instant: Instant) -> u64 {
        instant
            .saturating_duration_since(self.origin)
            .as_nanos()
            .min(u128::from(u64::MAX - 1)) as u64
    }

    #[must_use]
    pub fn age_us(&self, oldest_nanos: u64) -> Option<u64> {
        if oldest_nanos == EMPTY_OLDEST {
            return None;
        }
        let now = self.origin.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        Some(now.saturating_sub(oldest_nanos) / 1_000)
    }

    pub fn record_admission_attempt(&self) {
        self.admission_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_admission_latency(&self, us: u64) {
        self.admission_latency.observe(us);
    }

    pub fn record_delivery_attempt(&self) {
        self.delivery_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_delivery_latency(&self, us: u64) {
        self.delivery_latency.observe(us);
    }

    pub fn record_ingress_status(&self, index: usize) {
        if let Some(slot) = self.shed_by_reason.get(index) {
            slot.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_handler_timed_out(&self) {
        self.handler_timed_out.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_handler_failed(&self) {
        self.handler_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_handler_cancelled(&self) {
        self.handler_cancelled.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_handler_backpressured(&self) {
        self.handler_backpressured.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_handler_worker_stopped(&self) {
        self.handler_worker_stopped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_handler_completed_ok(&self) {
        self.handler_completed_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_router_queue_age_expiry(&self) {
        self.router_queue_age_expiries
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_mailbox_queue_age_expiry(&self) {
        self.mailbox_queue_age_expiries
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_mailbox_overflow_gap(&self) {
        self.mailbox_overflow_gaps.fetch_add(1, Ordering::Relaxed);
        self.event_gaps.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_event_gap(&self) {
        self.event_gaps.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_age_sample_failure(&self) {
        self.age_sample_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_owner_turn(&self, us: u64) {
        self.last_owner_turn_us.store(us, Ordering::Relaxed);
        self.max_owner_turn_us.fetch_max(us, Ordering::Relaxed);
    }

    pub fn record_ready_operation_wait(&self, us: u64) {
        self.last_ready_operation_wait_us
            .store(us, Ordering::Relaxed);
        self.max_ready_operation_wait_us
            .fetch_max(us, Ordering::Relaxed);
    }

    pub fn record_stalled_write_timeout(&self) {
        self.stalled_write_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Control-path insert. Event paths must not call this.
    pub fn register_cell(&self, identity: AgeIdentity, cell: Arc<QueueAgeMetric>) {
        let Ok(mut registry) = self.registry.write() else {
            return;
        };
        registry.retain(|_, entry| !entry.retired);
        registry.insert(
            identity,
            AgeRegistryEntry {
                cell: Some(cell),
                retired: false,
            },
        );
    }

    /// Control-path missing-cell marker. Event paths must not call this.
    pub fn register_missing(&self, identity: AgeIdentity) {
        let Ok(mut registry) = self.registry.write() else {
            return;
        };
        registry.insert(
            identity,
            AgeRegistryEntry {
                cell: None,
                retired: false,
            },
        );
    }

    /// Control-path retirement. Event paths must not call this.
    pub fn retire_identity(&self, identity: &AgeIdentity) {
        let Ok(mut registry) = self.registry.write() else {
            return;
        };
        if let Some(entry) = registry.get_mut(identity) {
            entry.retired = true;
            if let Some(cell) = &entry.cell {
                cell.store(0, EMPTY_OLDEST, 0, false, 0);
                cell.close_writes();
            }
        }
    }

    /// Retire only this cell instance. A delayed drop of an older mailbox Arc
    /// must not close a replacement cell registered under the same identity.
    pub fn retire_cell(&self, identity: &AgeIdentity, cell: &Arc<QueueAgeMetric>) {
        let Ok(mut registry) = self.registry.write() else {
            return;
        };
        let Some(entry) = registry.get_mut(identity) else {
            return;
        };
        let Some(registered) = &entry.cell else {
            return;
        };
        if !Arc::ptr_eq(registered, cell) {
            return;
        }
        entry.retired = true;
        registered.store(0, EMPTY_OLDEST, 0, false, 0);
        registered.close_writes();
    }

    pub fn prune_retired(&self) {
        let Ok(mut registry) = self.registry.write() else {
            return;
        };
        registry.retain(|_, entry| !entry.retired);
    }

    #[must_use]
    pub fn registry_len(&self) -> usize {
        self.registry
            .read()
            .map(|registry| registry.len())
            .unwrap_or(0)
    }

    #[must_use]
    pub fn live_registry_len(&self) -> usize {
        self.registry
            .read()
            .map(|registry| registry.values().filter(|entry| !entry.retired).count())
            .unwrap_or(0)
    }

    /// Saturation-safe snapshot. Never takes the router inner lock.
    #[must_use]
    pub fn snapshot(&self) -> DaemonObservabilityCounters {
        let mut event_shed_by_reason = BTreeMap::new();
        for (index, slot) in self.shed_by_reason.iter().enumerate() {
            let count = slot.load(Ordering::Relaxed);
            if count > 0 {
                event_shed_by_reason.insert(status_name(index).to_string(), count);
            }
        }
        let queue_ages = self.snapshot_queue_ages();
        let mut snapshot = DaemonObservabilityCounters::default();
        snapshot.event_shed_by_reason = event_shed_by_reason;
        snapshot.event_admission_attempts = self.admission_attempts.load(Ordering::Relaxed);
        snapshot.event_delivery_attempts = self.delivery_attempts.load(Ordering::Relaxed);
        snapshot.event_admission_latency = self.admission_latency.snapshot();
        snapshot.event_delivery_latency = self.delivery_latency.snapshot();
        snapshot.event_handler_timed_out = self.handler_timed_out.load(Ordering::Relaxed);
        snapshot.event_handler_failed = self.handler_failed.load(Ordering::Relaxed);
        snapshot.event_handler_cancelled = self.handler_cancelled.load(Ordering::Relaxed);
        snapshot.event_handler_backpressured = self.handler_backpressured.load(Ordering::Relaxed);
        snapshot.event_handler_worker_stopped = self.handler_worker_stopped.load(Ordering::Relaxed);
        snapshot.event_handler_completed_ok = self.handler_completed_ok.load(Ordering::Relaxed);
        snapshot.event_router_queue_age_expiries =
            self.router_queue_age_expiries.load(Ordering::Relaxed);
        snapshot.event_mailbox_queue_age_expiries =
            self.mailbox_queue_age_expiries.load(Ordering::Relaxed);
        snapshot.event_mailbox_overflow_gaps = self.mailbox_overflow_gaps.load(Ordering::Relaxed);
        snapshot.event_gaps = self.event_gaps.load(Ordering::Relaxed);
        snapshot.event_age_sample_failures = self.age_sample_failures.load(Ordering::Relaxed);
        snapshot.last_owner_turn_us = self.last_owner_turn_us.load(Ordering::Relaxed);
        snapshot.max_owner_turn_us = self.max_owner_turn_us.load(Ordering::Relaxed);
        snapshot.last_ready_operation_wait_us =
            self.last_ready_operation_wait_us.load(Ordering::Relaxed);
        snapshot.max_ready_operation_wait_us =
            self.max_ready_operation_wait_us.load(Ordering::Relaxed);
        snapshot.stalled_write_timeouts = self.stalled_write_timeouts.load(Ordering::Relaxed);
        snapshot.queue_ages = queue_ages;
        snapshot.global_in_flight_bytes = self.global_in_flight_bytes.load(Ordering::Relaxed);
        snapshot
    }

    pub fn set_global_in_flight_bytes(&self, bytes: u64) {
        self.global_in_flight_bytes.store(bytes, Ordering::Relaxed);
    }

    fn snapshot_queue_ages(&self) -> Vec<DaemonQueueAgeObservation> {
        let Ok(registry) = self.registry.read() else {
            return Vec::new();
        };
        let mut rows = Vec::with_capacity(registry.len());
        for (identity, entry) in registry.iter() {
            rows.push(self.observation_for(identity, entry));
        }
        rows.sort_by(|left, right| {
            left.kind
                .as_str()
                .cmp(right.kind.as_str())
                .then(left.identity.cmp(&right.identity))
                .then(left.producer_generation.cmp(&right.producer_generation))
        });
        rows
    }

    fn observation_for(
        &self,
        identity: &AgeIdentity,
        entry: &AgeRegistryEntry,
    ) -> DaemonQueueAgeObservation {
        let producer_generation = match identity.kind {
            DaemonQueueKind::Producer => identity.generation,
            _ => None,
        };
        let Some(cell) = &entry.cell else {
            return DaemonQueueAgeObservation::new(
                identity.kind,
                identity.identity.clone(),
                producer_generation,
                DaemonQueueAgeState::Indeterminate,
                None,
                None,
            );
        };
        match cell.sample() {
            AgeSample::Usable {
                count,
                bytes,
                oldest_nanos,
            } => {
                let mut row = DaemonQueueAgeObservation::new(
                    identity.kind,
                    identity.identity.clone(),
                    producer_generation,
                    DaemonQueueAgeState::Usable,
                    self.age_us(oldest_nanos),
                    Some(count),
                );
                row.queue_bytes = Some(bytes);
                row
            }
            AgeSample::Empty { count, bytes } => {
                let mut row = DaemonQueueAgeObservation::new(
                    identity.kind,
                    identity.identity.clone(),
                    producer_generation,
                    DaemonQueueAgeState::Empty,
                    None,
                    Some(count),
                );
                row.queue_bytes = Some(bytes);
                row
            }
            AgeSample::Indeterminate => DaemonQueueAgeObservation::new(
                identity.kind,
                identity.identity.clone(),
                producer_generation,
                DaemonQueueAgeState::Indeterminate,
                None,
                None,
            ),
        }
    }

    /// Hold the registry write lock while `body` runs. Test-only contention control.
    #[cfg(test)]
    pub fn test_with_registry_held<R>(&self, body: impl FnOnce() -> R) -> R {
        let _guard = self.registry.write().expect("test registry hold");
        body()
    }
}

fn status_name(index: usize) -> &'static str {
    match index {
        0 => "accepted",
        1 => "rejected_undeclared",
        2 => "rejected_foreign",
        3 => "rejected_invalid",
        4 => "rejected_oversize",
        5 => "rejected_over_rate",
        6 => "rejected_over_fanout",
        7 => "rejected_wildcard",
        8 => "rejected_causal_scope",
        9 => "rejected_audience",
        10 => "shed_full",
        11 => "shed_busy",
        _ => "unknown",
    }
}

#[cfg(test)]
pub mod alloc_scope {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        static ACTIVE: Cell<bool> = const { Cell::new(false) };
        static COUNT: Cell<u64> = const { Cell::new(0) };
    }

    pub struct CountingAlloc;

    unsafe impl GlobalAlloc for CountingAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if ACTIVE.with(Cell::get) {
                COUNT.with(|count| count.set(count.get() + 1));
            }
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            if ACTIVE.with(Cell::get) {
                COUNT.with(|count| count.set(count.get() + 1));
            }
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            if ACTIVE.with(Cell::get) {
                COUNT.with(|count| count.set(count.get() + 1));
            }
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    pub struct AllocGuard;

    impl AllocGuard {
        #[must_use]
        pub fn enter() -> Self {
            COUNT.with(|count| count.set(0));
            ACTIVE.with(|active| active.set(true));
            Self
        }

        #[must_use]
        pub fn count(&self) -> u64 {
            COUNT.with(Cell::get)
        }
    }

    impl Drop for AllocGuard {
        fn drop(&mut self) {
            ACTIVE.with(|active| active.set(false));
        }
    }
}

#[cfg(test)]
#[global_allocator]
static EVENT_PLANE_TEST_ALLOC: alloc_scope::CountingAlloc = alloc_scope::CountingAlloc;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn histogram_bucket_is_one_leading_zeros_step() {
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(1), 0);
        assert_eq!(bucket_index(2), 1);
        assert_eq!(bucket_index(3), 1);
        assert_eq!(bucket_index(4), 2);
        assert_eq!(bucket_index(1 << 11), 11);
        assert_eq!(bucket_index(1 << 12), 12);
        assert_eq!(bucket_index(u64::MAX), 12);
    }

    #[test]
    fn stable_even_version_yields_usable_age() {
        let cell = QueueAgeMetric::new(7);
        cell.store(3, 1_000, 0, false, 0);
        match cell.sample() {
            AgeSample::Usable {
                count,
                oldest_nanos,
                ..
            } => {
                assert_eq!(count, 3);
                assert_eq!(oldest_nanos, 1_000);
            }
            other => panic!("expected usable, got {other:?}"),
        }
        assert_eq!(cell.generation(), 7);
    }

    #[test]
    fn zero_count_yields_empty_not_zero_age() {
        let cell = QueueAgeMetric::new(1);
        cell.store(0, EMPTY_OLDEST, 0, false, 0);
        assert_eq!(cell.sample(), AgeSample::Empty { count: 0, bytes: 0 });
    }

    #[test]
    fn odd_version_is_indeterminate_after_one_retry() {
        let cell = QueueAgeMetric::new(1);
        cell.store(4, 50, 0, false, 0);
        cell.version.fetch_add(1, Ordering::AcqRel);
        assert_eq!(cell.sample(), AgeSample::Indeterminate);
    }

    #[test]
    fn mutation_between_bracket_loads_is_indeterminate() {
        let cell = Arc::new(QueueAgeMetric::new(1));
        cell.store(2, 10, 0, false, 0);
        let reader = {
            let cell = Arc::clone(&cell);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(5));
                cell.sample()
            })
        };
        cell.store(3, 20, 0, false, 0);
        let _ = reader.join().expect("join reader");
        cell.store(4, 30, 0, false, 0);
        let v1 = cell.version.load(Ordering::Acquire);
        cell.count.store(99, Ordering::Relaxed);
        fence(Ordering::Acquire);
        let v2 = cell.version.load(Ordering::Relaxed);
        assert_eq!(v1, v2);
        assert_ne!(cell.count.load(Ordering::Relaxed), 4);
        cell.store(4, 30, 0, false, 0);
        match cell.sample() {
            AgeSample::Usable { count, .. } => assert_eq!(count, 4),
            other => panic!("expected usable after completed write, got {other:?}"),
        }
    }

    #[test]
    fn aba_count_sequence_advances_version_by_four() {
        let cell = QueueAgeMetric::new(1);
        cell.store(5, 1, 0, false, 0);
        let before = cell.version.load(Ordering::Relaxed);
        cell.store(6, 2, 0, false, 0);
        cell.store(5, 3, 0, false, 0);
        let after = cell.version.load(Ordering::Relaxed);
        assert_eq!(after.saturating_sub(before), 4);
        match cell.sample() {
            AgeSample::Usable { count, .. } => assert_eq!(count, 5),
            other => panic!("expected usable, got {other:?}"),
        }
    }

    #[test]
    fn invalid_latch_makes_every_later_sample_indeterminate() {
        let cell = QueueAgeMetric::new(2);
        cell.store(3, 9, 0, false, 0);
        cell.latch_invalid();
        assert_eq!(cell.sample(), AgeSample::Indeterminate);
        cell.store(4, 10, 0, false, 0);
        assert_eq!(cell.sample(), AgeSample::Indeterminate);
    }

    #[test]
    fn open_gate_is_indeterminate() {
        let cell = QueueAgeMetric::new(3);
        cell.store(2, 8, 4, false, 0);
        assert_eq!(cell.sample(), AgeSample::Indeterminate);
        cell.store(2, 8, 0, false, 0);
        assert!(matches!(cell.sample(), AgeSample::Usable { .. }));
    }

    #[test]
    fn write_closed_cell_ignores_later_stores() {
        let cell = QueueAgeMetric::new(1);
        cell.store(2, 4, 0, false, 0);
        cell.close_writes();
        cell.store(9, 99, 0, false, 0);
        match cell.sample() {
            AgeSample::Usable {
                count,
                oldest_nanos,
                ..
            } => {
                assert_eq!(count, 2);
                assert_eq!(oldest_nanos, 4);
            }
            other => panic!("expected frozen usable sample, got {other:?}"),
        }
    }

    #[test]
    fn producer_age_list_middle_remove_preserves_oldest() {
        let cell = Arc::new(QueueAgeMetric::new(1));
        let mut list = ProducerAgeList::new(4, 1, cell);
        let a = list.push(10).expect("a");
        let b = list.push(20).expect("b");
        let _c = list.push(30).expect("c");
        assert_eq!(list.oldest_nanos(), 10);
        list.remove(b);
        assert_eq!(list.live(), 2);
        assert_eq!(list.oldest_nanos(), 10);
        let d = list.push(40).expect("reuse free slot");
        assert_eq!(d, b);
        assert_eq!(list.oldest_nanos(), 10);
        list.remove(a);
        assert_eq!(list.oldest_nanos(), 30);
    }

    #[test]
    fn producer_age_list_push_and_remove_ops_are_constant() {
        let cell = Arc::new(QueueAgeMetric::new(1));
        let mut small = ProducerAgeList::new(8, 1, Arc::clone(&cell));
        small.test_reset_ops();
        small.push(1).expect("push");
        let push_small = small.test_ops();
        let mut large = ProducerAgeList::new(10_000, 1, cell);
        for index in 0..5_000 {
            large.push(index as u64).expect("fill");
        }
        large.test_reset_ops();
        large.push(9_999).expect("push at occupancy");
        assert_eq!(large.test_ops(), push_small);
        large.test_reset_ops();
        large.remove(0);
        let remove_head = large.test_ops();
        large.test_reset_ops();
        large.remove(2_500);
        assert_eq!(large.test_ops(), remove_head);
    }

    #[test]
    fn snapshot_missing_cell_is_indeterminate_without_count() {
        let counters = EventPlaneCounters::new();
        counters.register_missing(AgeIdentity {
            kind: DaemonQueueKind::Producer,
            identity: "owner".to_string(),
            generation: None,
        });
        let row = &counters.snapshot().queue_ages[0];
        assert_eq!(row.state, DaemonQueueAgeState::Indeterminate);
        assert!(row.oldest_age_us.is_none());
        assert!(row.queue_count.is_none());
        assert!(row.producer_generation.is_none());
    }

    #[test]
    fn retired_registry_entry_reports_empty_and_prune_drops_it() {
        let counters = EventPlaneCounters::new();
        let cell = Arc::new(QueueAgeMetric::new(4));
        cell.store(2, 10, 0, false, 0);
        let identity = AgeIdentity {
            kind: DaemonQueueKind::Consumer,
            identity: "plugin".to_string(),
            generation: Some(4),
        };
        counters.register_cell(identity.clone(), Arc::clone(&cell));
        counters.retire_identity(&identity);
        let row = &counters.snapshot().queue_ages[0];
        assert_eq!(row.state, DaemonQueueAgeState::Empty);
        assert_eq!(row.queue_count, Some(0));
        assert!(row.oldest_age_us.is_none());
        assert_eq!(counters.registry_len(), 1);
        counters.prune_retired();
        assert_eq!(counters.registry_len(), 0);
        assert_eq!(cell.sample(), AgeSample::Empty { count: 0, bytes: 0 });
    }

    #[test]
    fn live_empty_queue_is_not_pruned() {
        let counters = EventPlaneCounters::new();
        let cell = Arc::new(QueueAgeMetric::new(1));
        cell.store(0, EMPTY_OLDEST, 0, false, 0);
        counters.register_cell(
            AgeIdentity {
                kind: DaemonQueueKind::Consumer,
                identity: "live".to_string(),
                generation: Some(1),
            },
            cell,
        );
        counters.prune_retired();
        assert_eq!(counters.live_registry_len(), 1);
        assert_eq!(
            counters.snapshot().queue_ages[0].state,
            DaemonQueueAgeState::Empty
        );
    }

    #[test]
    fn diagnostic_alloc_scope_counts_only_while_active() {
        let _outside = Box::new(1u8);
        let guard = alloc_scope::AllocGuard::enter();
        let _inside = Box::new(2u8);
        let counted = guard.count();
        drop(guard);
        let _after = Box::new(3u8);
        assert!(counted >= 1, "active scope must observe the boxed alloc");
    }

    #[test]
    fn snapshot_does_not_need_router_and_includes_shed() {
        let counters = EventPlaneCounters::new();
        counters.record_ingress_status(10);
        counters.record_handler_timed_out();
        counters.record_stalled_write_timeout();
        let snap = counters.snapshot();
        assert_eq!(snap.event_shed_by_reason.get("shed_full").copied(), Some(1));
        assert_eq!(snap.event_handler_timed_out, 1);
        assert_eq!(snap.stalled_write_timeouts, 1);
    }
}

#[cfg(all(test, loom))]
mod queue_age_model {
    use loom::sync::atomic::{AtomicU64, Ordering, fence};
    use loom::thread;

    struct Model {
        version: AtomicU64,
        count: AtomicU64,
        oldest: AtomicU64,
    }

    impl Model {
        fn write(&self, count: u64, oldest: u64) {
            self.version.fetch_add(1, Ordering::AcqRel);
            self.count.store(count, Ordering::Relaxed);
            self.oldest.store(oldest, Ordering::Relaxed);
            self.version.fetch_add(1, Ordering::Release);
        }

        fn read(&self) -> Option<(u64, u64)> {
            let v1 = self.version.load(Ordering::Acquire);
            if v1 % 2 == 1 {
                return None;
            }
            let count = self.count.load(Ordering::Relaxed);
            let oldest = self.oldest.load(Ordering::Relaxed);
            fence(Ordering::Acquire);
            let v2 = self.version.load(Ordering::Relaxed);
            if v1 != v2 {
                return None;
            }
            Some((count, oldest))
        }
    }

    #[test]
    fn queue_age_model_rejects_mixed_samples() {
        loom::model(|| {
            let model = loom::sync::Arc::new(Model {
                version: AtomicU64::new(0),
                count: AtomicU64::new(0),
                oldest: AtomicU64::new(0),
            });
            let writer = {
                let model = loom::sync::Arc::clone(&model);
                thread::spawn(move || {
                    model.write(1, 10);
                    model.write(2, 20);
                })
            };
            let reader = {
                let model = loom::sync::Arc::clone(&model);
                thread::spawn(move || model.read())
            };
            writer.join().unwrap();
            if let Some((count, oldest)) = reader.join().unwrap() {
                assert!(
                    (count == 0 && oldest == 0)
                        || (count == 1 && oldest == 10)
                        || (count == 2 && oldest == 20)
                );
            }
        });
    }
}
