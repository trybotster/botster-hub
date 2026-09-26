//! Connection-scoped package-event subscriptions on the host control plane.
//!
//! This module owns subject admission, per-connection mailboxes, gap bits, and
//! the coalesced writer wake. It does not take over a Unix socket and does not
//! put frames on [`EntityFrameSender`].

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, TryLockError, Weak};
use std::time::Instant;

use botster_hub_client::{
    DaemonDiagnostic, DaemonEvent, DaemonOperatorError, DaemonResponse, DaemonResponseKind,
};
use serde_json::Value;
use tokio::sync::Notify;

use crate::client_api_dto::response::daemon_response_base;
use crate::config::PackageEventPlanePolicy;
use crate::event_plane_counters::{AgeIdentity, EventPlaneCounters, QueueAgeMetric};
use crate::package_event_router::{ClientEventHolder, EventPlaneStatus, PackageEventRouter};
use botster_hub_client::DaemonQueueKind;

pub const MAX_SUBJECTS_PER_SUBSCRIPTION: usize = 16;
pub const MAX_SUBJECT_UTF8_BYTES: usize = 256;
pub const MAX_SUBJECT_AGGREGATE_BYTES: usize = 4_096;
pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 64;
pub(crate) const CONNECTION_EVENT_MAX: usize = 128;
pub(crate) const CONNECTION_BYTE_MAX: usize = 2 * 1024 * 1024;
pub(crate) const SUBSCRIPTION_EVENT_RESERVE: usize = 4;
pub(crate) const SUBSCRIPTION_BYTE_RESERVE: usize = 65_536;

const GLOB_CHARS: [char; 6] = ['*', '?', '[', ']', '{', '}'];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientEventAdmitError {
    EmptySubject,
    WildcardSubject,
    DuplicateSubject,
    TooManySubjects,
    SubjectTooLong,
    AggregateTooLarge,
    TooManySubscriptions,
    DuplicateSubscription,
    UnknownSubscription,
    NotNegotiated,
    ConnectionCapacity,
    Router(EventPlaneStatus),
}

impl ClientEventAdmitError {
    #[must_use]
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::EmptySubject => "empty_subject",
            Self::WildcardSubject => "rejected_wildcard",
            Self::DuplicateSubject => "duplicate_subject",
            Self::TooManySubjects => "too_many_subjects",
            Self::SubjectTooLong => "subject_too_long",
            Self::AggregateTooLarge => "subject_aggregate_too_large",
            Self::TooManySubscriptions => "too_many_event_subscriptions",
            Self::DuplicateSubscription => "duplicate_event_subscription",
            Self::UnknownSubscription => "unknown_event_subscription",
            Self::NotNegotiated => "package_event_subscriptions_not_negotiated",
            Self::ConnectionCapacity => "package_event_connection_capacity",
            Self::Router(status) => status.as_str(),
        }
    }

    #[must_use]
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::EmptySubject => "subject values must not be empty",
            Self::WildcardSubject => "subject values must be exact strings",
            Self::DuplicateSubject => "subject values must be unique",
            Self::TooManySubjects => "a subscription admits at most 16 subject values",
            Self::SubjectTooLong => "each subject value admits at most 256 UTF-8 bytes",
            Self::AggregateTooLarge => "subject values admit at most 4096 UTF-8 bytes together",
            Self::TooManySubscriptions => {
                "a host-control connection admits at most 64 package-event subscriptions"
            }
            Self::DuplicateSubscription => {
                "this connection already holds that event subscription id"
            }
            Self::UnknownSubscription => "no event subscription exists on this connection",
            Self::NotNegotiated => {
                "package_event_subscriptions was not negotiated on this host Hello"
            }
            Self::ConnectionCapacity => {
                "the connection cannot reserve package-event mailbox capacity"
            }
            Self::Router(status) => match status {
                EventPlaneStatus::RejectedUndeclared => "event is not an admitted contract",
                EventPlaneStatus::RejectedForeign => "event owner does not match the caller",
                EventPlaneStatus::RejectedWildcard => "event owner and name must be exact",
                EventPlaneStatus::RejectedAudience => "event contract does not admit clients",
                EventPlaneStatus::ShedBusy => "event router is busy",
                EventPlaneStatus::RejectedInvalid => "event owner or name is invalid",
                EventPlaneStatus::RejectedOverFanout => "event already has too many subscribers",
                _ => "event subscription was rejected",
            },
        }
    }
}

pub(crate) fn client_event_operator_error(
    error: ClientEventAdmitError,
    request_id: &str,
    operation: &str,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: error.code().to_string(),
        request_id: request_id.to_string(),
        operation: operation.to_string(),
        message: error.message().to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(operation, error.message())],
    });
    response
}

#[derive(Debug, Clone)]
struct QueuedClientEvent {
    subscription_id: String,
    owner: String,
    name: String,
    payload: Value,
    enqueued_at: Instant,
    size: usize,
}

struct MailboxInner {
    events: VecDeque<QueuedClientEvent>,
    bytes: usize,
}

struct ClientGapSlot {
    owner: String,
    name: String,
    bit: Arc<AtomicBool>,
}

/// Bounded per-connection mailbox plus per-subscription gap bits.
pub(crate) struct ClientEventMailbox {
    inner: Mutex<MailboxInner>,
    slots: Mutex<HashMap<String, Arc<ClientGapSlot>>>,
    wake: Notify,
    wake_bit: AtomicBool,
    retired: AtomicBool,
    event_max: usize,
    byte_max: usize,
    queue_age: std::time::Duration,
    counters: Option<Arc<EventPlaneCounters>>,
    age_cell: Arc<QueueAgeMetric>,
    identity: String,
    subscription_id: Option<String>,
    connection_pool: Option<Arc<ConnectionEventPool>>,
    connection: Option<Weak<ClientEventConnection>>,
}

impl std::fmt::Debug for ClientEventMailbox {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientEventMailbox")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct ConnectionEventPool {
    inner: Mutex<ConnectionResidency>,
}

#[derive(Debug, Default)]
struct ConnectionResidency {
    events: usize,
    bytes: usize,
    subscriptions: HashMap<String, (usize, usize)>,
}

impl ClientEventMailbox {
    #[cfg(test)]
    pub(crate) fn new(policy: PackageEventPlanePolicy) -> Self {
        Self::new_with_counters(policy, None, "mailbox", None, None)
    }

    fn new_with_counters(
        policy: PackageEventPlanePolicy,
        counters: Option<Arc<EventPlaneCounters>>,
        identity: &str,
        connection_pool: Option<Arc<ConnectionEventPool>>,
        subscription_id: Option<String>,
    ) -> Self {
        let age_cell = Arc::new(QueueAgeMetric::new(0));
        Self {
            inner: Mutex::new(MailboxInner {
                events: VecDeque::new(),
                bytes: 0,
            }),
            slots: Mutex::new(HashMap::new()),
            wake: Notify::new(),
            wake_bit: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            event_max: policy.consumer_queue_max_events,
            byte_max: policy.consumer_queue_max_bytes,
            queue_age: policy.queue_age,
            counters,
            age_cell,
            identity: identity.to_string(),
            subscription_id,
            connection_pool,
            connection: None,
        }
    }

    fn register_age(&self) {
        if let Some(counters) = self.counters.as_ref() {
            counters.register_cell(self.mailbox_identity(), Arc::clone(&self.age_cell));
        }
    }

    fn mailbox_identity(&self) -> AgeIdentity {
        AgeIdentity {
            kind: DaemonQueueKind::ClientMailbox,
            identity: self.identity.clone(),
            generation: Some(0),
        }
    }

    fn publish_age(&self, inner: &MailboxInner) {
        let count = inner.events.len() as u64;
        let oldest = inner
            .events
            .front()
            .and_then(|event| {
                self.counters
                    .as_ref()
                    .map(|counters| counters.nanos_of(event.enqueued_at))
            })
            .unwrap_or(u64::MAX);
        let bytes = inner.bytes as u64;
        self.age_cell.store(count, oldest, 0, false, bytes);
    }

    // The caller holds the mailbox lock or exclusive ownership during Drop.
    fn retire_from_registry(&self) {
        if let Some(counters) = &self.counters {
            counters.retire_cell(&self.mailbox_identity(), &self.age_cell);
        }
    }

    #[must_use]
    pub(crate) fn notify(&self) -> &Notify {
        &self.wake
    }

    fn signal_wake(&self) {
        if let Some(connection) = self.connection() {
            connection.wake_reader();
        }
        self.wake_bit.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
    }

    #[must_use]
    pub(crate) fn take_wake(&self) -> bool {
        self.wake_bit.swap(false, Ordering::SeqCst)
    }

    pub(crate) fn try_push(
        &self,
        subscription_id: &str,
        owner: &str,
        name: &str,
        payload: Value,
        size: usize,
    ) -> Result<(), EventPlaneStatus> {
        let mut inner = lock_mailbox(&self.inner)?;
        if self.is_retired() {
            return Err(EventPlaneStatus::RejectedInvalid);
        }
        let mut residency = match self.connection_pool.as_ref() {
            Some(pool) => Some(
                pool.inner
                    .try_lock()
                    .map_err(|_| EventPlaneStatus::ShedBusy)?,
            ),
            None => None,
        };
        let connection_full = residency.as_ref().is_some_and(|residency| {
            let protected_events = residency
                .subscriptions
                .iter()
                .filter(|(id, _)| Some(id.as_str()) != self.subscription_id.as_deref())
                .map(|(_, (events, _))| SUBSCRIPTION_EVENT_RESERVE.saturating_sub(*events))
                .sum::<usize>();
            let protected_bytes = residency
                .subscriptions
                .iter()
                .filter(|(id, _)| Some(id.as_str()) != self.subscription_id.as_deref())
                .map(|(_, (_, bytes))| SUBSCRIPTION_BYTE_RESERVE.saturating_sub(*bytes))
                .sum::<usize>();
            residency.events + 1 > CONNECTION_EVENT_MAX.saturating_sub(protected_events)
                || residency.bytes + size > CONNECTION_BYTE_MAX.saturating_sub(protected_bytes)
        });
        if connection_full {
            drop(residency);
            drop(inner);
            if let Some(counters) = &self.counters {
                counters.record_mailbox_overflow_gap();
            }
            self.mark_gap(subscription_id, owner, name);
            return Err(EventPlaneStatus::ShedFull);
        }
        if inner.events.len() + 1 > self.event_max || inner.bytes + size > self.byte_max {
            drop(inner);
            if let Some(counters) = &self.counters {
                counters.record_mailbox_overflow_gap();
            }
            self.mark_gap(subscription_id, owner, name);
            return Err(EventPlaneStatus::ShedFull);
        }
        inner.bytes += size;
        if let Some(residency) = residency.as_mut() {
            residency.events += 1;
            residency.bytes += size;
            if let Some(subscription_id) = self.subscription_id.as_ref()
                && let Some((events, bytes)) = residency.subscriptions.get_mut(subscription_id)
            {
                *events += 1;
                *bytes += size;
            }
        }
        inner.events.push_back(QueuedClientEvent {
            subscription_id: subscription_id.to_string(),
            owner: owner.to_string(),
            name: name.to_string(),
            payload,
            enqueued_at: Instant::now(),
            size,
        });
        self.publish_age(&inner);
        drop(inner);
        self.signal_wake();
        Ok(())
    }

    pub(crate) fn register_gap_slot(
        &self,
        subscription_id: &str,
        owner: &str,
        name: &str,
    ) -> Result<Arc<AtomicBool>, EventPlaneStatus> {
        let mut slots = match self.slots.try_lock() {
            Ok(slots) => slots,
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => {
                return Err(EventPlaneStatus::ShedBusy);
            }
        };
        let bit = Arc::new(AtomicBool::new(false));
        slots.insert(
            subscription_id.to_string(),
            Arc::new(ClientGapSlot {
                owner: owner.to_string(),
                name: name.to_string(),
                bit: bit.clone(),
            }),
        );
        Ok(bit)
    }

    pub(crate) fn set_gap(&self, subscription_id: &str, owner: &str, name: &str) {
        self.mark_gap(subscription_id, owner, name);
    }

    fn mark_gap(&self, subscription_id: &str, owner: &str, name: &str) {
        if let Ok(mut slots) = self.slots.try_lock() {
            let slot = slots.entry(subscription_id.to_string()).or_insert_with(|| {
                Arc::new(ClientGapSlot {
                    owner: owner.to_string(),
                    name: name.to_string(),
                    bit: Arc::new(AtomicBool::new(false)),
                })
            });
            slot.bit.store(true, Ordering::SeqCst);
        }
        self.signal_wake();
    }

    #[must_use]
    pub(crate) fn has_ready_event(&self) -> bool {
        match self.slots.try_lock() {
            Ok(slots) if slots.values().any(|slot| slot.bit.load(Ordering::SeqCst)) => {
                return true;
            }
            Err(_) => return true,
            Ok(_) => {}
        }
        let Ok(inner) = lock_mailbox(&self.inner) else {
            return false;
        };
        !inner.events.is_empty()
    }

    pub(crate) fn take_ready_event(&self) -> Option<DaemonEvent> {
        if self.is_retired() {
            return None;
        }
        if let Some(event) = self.take_gap() {
            return Some(event);
        }
        let mut inner = lock_mailbox(&self.inner).ok()?;
        let mut residency = match self.connection_pool.as_ref() {
            Some(pool) => Some(pool.inner.try_lock().ok()?),
            None => None,
        };
        let queued = inner.events.pop_front()?;
        inner.bytes = inner.bytes.saturating_sub(queued.size);
        if let Some(residency) = residency.as_mut() {
            residency.events = residency.events.saturating_sub(1);
            residency.bytes = residency.bytes.saturating_sub(queued.size);
            if let Some(subscription_id) = self.subscription_id.as_ref()
                && let Some((events, bytes)) = residency.subscriptions.get_mut(subscription_id)
            {
                *events = events.saturating_sub(1);
                *bytes = bytes.saturating_sub(queued.size);
            }
        }
        self.publish_age(&inner);
        if queued.enqueued_at.elapsed() > self.queue_age {
            drop(inner);
            if let Some(counters) = &self.counters {
                counters.record_mailbox_queue_age_expiry();
                counters.record_event_gap();
            }
            self.mark_gap(&queued.subscription_id, &queued.owner, &queued.name);
            return self.take_ready_event();
        }
        Some(DaemonEvent::PackageEvent {
            subscription_id: queued.subscription_id,
            owner: queued.owner,
            name: queued.name,
            payload: queued.payload,
        })
    }

    fn take_gap(&self) -> Option<DaemonEvent> {
        let slots = match self.slots.try_lock() {
            Ok(slots) => slots,
            Err(_) => return None,
        };
        let (subscription_id, owner, name) = slots.iter().find_map(|(subscription_id, slot)| {
            slot.bit.load(Ordering::SeqCst).then(|| {
                (
                    subscription_id.clone(),
                    slot.owner.clone(),
                    slot.name.clone(),
                )
            })
        })?;
        if let Some(slot) = slots.get(&subscription_id) {
            slot.bit.store(false, Ordering::SeqCst);
        }
        Some(DaemonEvent::EventGap {
            subscription_id,
            owner,
            name,
        })
    }

    /// The worker retains the slot and its residency until payload destruction finishes.
    fn reclaim(&self) -> Result<(), PoisonedCleanupLock> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| PoisonedCleanupLock::Mailbox)?;
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| PoisonedCleanupLock::Mailbox)?;
        let mut residency = match self.connection_pool.as_ref() {
            Some(pool) => Some(pool.inner.lock().map_err(|_| PoisonedCleanupLock::Pool)?),
            None => None,
        };
        self.retire();
        let removed_events = inner.events.len();
        let removed_bytes = inner.bytes;
        drop(std::mem::take(&mut inner.events));
        inner.bytes = 0;
        slots.clear();
        self.publish_age(&inner);
        if let Some(residency) = residency.as_mut() {
            residency.events = residency.events.saturating_sub(removed_events);
            residency.bytes = residency.bytes.saturating_sub(removed_bytes);
            if let Some(subscription_id) = self.subscription_id.as_ref() {
                residency.subscriptions.remove(subscription_id);
            }
        }
        self.retire_from_registry();
        Ok(())
    }

    pub(crate) fn connection(&self) -> Option<Arc<ClientEventConnection>> {
        self.connection.as_ref()?.upgrade()
    }

    pub(crate) fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.signal_wake();
    }

    #[must_use]
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
            || self
                .connection()
                .is_some_and(|connection| connection.closing.load(Ordering::Acquire))
    }

    #[cfg(test)]
    pub(crate) fn test_with_inner_held<R>(&self, body: impl FnOnce() -> R) -> R {
        let _guard = self
            .inner
            .try_lock()
            .expect("test hold must acquire mailbox");
        body()
    }

    #[cfg(test)]
    pub(crate) fn test_with_slots_held<R>(&self, body: impl FnOnce() -> R) -> R {
        let _guard = self.slots.try_lock().expect("test hold must acquire slots");
        body()
    }
}

impl Drop for ClientEventMailbox {
    fn drop(&mut self) {
        self.retire_from_registry();
    }
}

struct ClientEventSlot {
    mailbox: Arc<ClientEventMailbox>,
    active: AtomicBool,
}

/// One admitted connection owns all provisional, active, retiring, and recovery slots.
/// The transport guard or its disconnect obligation retains the connection permit.
#[derive(Debug)]
pub(crate) struct ClientEventConnection {
    pub(crate) identity: Arc<str>,
    pool: Arc<ConnectionEventPool>,
    slots: Mutex<BTreeMap<Arc<str>, Arc<ClientEventSlot>>>,
    reader: OnceLock<Weak<ClientEventReader>>,
    reader_waiting: AtomicBool,
    closing: AtomicBool,
    fully_reclaimed: AtomicBool,
    requested: AtomicBool,
    faulted: AtomicBool,
    #[cfg(test)]
    panic_cleanup: AtomicBool,
    #[cfg(test)]
    cleanup_started: AtomicBool,
}

impl std::fmt::Debug for ClientEventSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientEventSlot")
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

/// The Unix transport retains one wake and one exact connection handle for its whole lifetime.
#[derive(Default)]
pub(crate) struct ClientEventReader {
    connection: OnceLock<Arc<ClientEventConnection>>,
    wake: Notify,
    wake_bit: AtomicBool,
    #[cfg(test)]
    empty_read: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    timer_ticks: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Debug for ClientEventReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientEventReader")
            .finish_non_exhaustive()
    }
}

impl ClientEventReader {
    fn signal(&self) {
        self.wake_bit.store(true, Ordering::Release);
        self.wake.notify_waiters();
    }

    pub(crate) fn mailbox(&self) -> Option<Arc<ClientEventMailbox>> {
        self.connection.get()?.mailbox()
    }

    pub(crate) async fn wait(&self) {
        let notified = self.wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.wake_bit.swap(false, Ordering::AcqRel)
            || self
                .mailbox()
                .is_some_and(|mailbox| mailbox.has_ready_event())
        {
            return;
        }
        notified.await;
    }

    #[cfg(test)]
    pub(crate) fn test_on_empty_read(&self, body: impl FnOnce() + Send + 'static) {
        *self.empty_read.lock().unwrap() = Some(Box::new(body));
        self.signal();
    }

    #[cfg(test)]
    pub(crate) fn test_after_empty_read(&self) {
        let body = self.empty_read.lock().unwrap().take();
        if let Some(body) = body {
            // Consume the setup wake before the test publishes between the read and registration.
            self.wake_bit.store(false, Ordering::Release);
            body();
        }
    }

    #[cfg(test)]
    pub(crate) fn test_note_timer(&self) {
        self.timer_ticks.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn test_timer_ticks(&self) -> usize {
        self.timer_ticks.load(Ordering::Relaxed)
    }
}

struct ClientSlotsGuard<'a> {
    connection: &'a ClientEventConnection,
    guard: Option<std::sync::MutexGuard<'a, BTreeMap<Arc<str>, Arc<ClientEventSlot>>>>,
}

impl std::ops::Deref for ClientSlotsGuard<'_> {
    type Target = BTreeMap<Arc<str>, Arc<ClientEventSlot>>;
    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for ClientSlotsGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().unwrap()
    }
}

impl Drop for ClientSlotsGuard<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        if self.connection.reader_waiting.swap(false, Ordering::AcqRel) {
            self.connection.wake_reader();
        }
    }
}

impl ClientEventConnection {
    pub(crate) fn bind_reader(self: &Arc<Self>, reader: &Arc<ClientEventReader>) {
        let _ = self.reader.set(Arc::downgrade(reader));
        let _ = reader.connection.set(Arc::clone(self));
        reader.signal();
    }

    fn wake_reader(&self) {
        if let Some(reader) = self.reader.get().and_then(Weak::upgrade) {
            reader.signal();
        }
    }

    fn try_slots(&self) -> Result<ClientSlotsGuard<'_>, EventPlaneStatus> {
        match self.slots.try_lock() {
            Ok(guard) => Ok(ClientSlotsGuard {
                connection: self,
                guard: Some(guard),
            }),
            Err(TryLockError::Poisoned(_)) => Err(EventPlaneStatus::RejectedInvalid),
            Err(TryLockError::WouldBlock) => {
                self.reader_waiting.store(true, Ordering::Release);
                // Recheck after arming so a concurrent unlock cannot lose the wake.
                self.slots
                    .try_lock()
                    .map(|guard| ClientSlotsGuard {
                        connection: self,
                        guard: Some(guard),
                    })
                    .map_err(|error| match error {
                        TryLockError::WouldBlock => EventPlaneStatus::ShedBusy,
                        TryLockError::Poisoned(_) => EventPlaneStatus::RejectedInvalid,
                    })
            }
        }
    }

    fn lock_slots(&self) -> Result<ClientSlotsGuard<'_>, PoisonedCleanupLock> {
        self.slots
            .lock()
            .map(|guard| ClientSlotsGuard {
                connection: self,
                guard: Some(guard),
            })
            .map_err(|_| PoisonedCleanupLock::Connection)
    }

    fn mailbox(&self) -> Option<Arc<ClientEventMailbox>> {
        let slots = self.try_slots().ok()?;
        slots
            .values()
            .filter(|slot| slot.active.load(Ordering::Acquire) && !slot.mailbox.is_retired())
            .find(|slot| slot.mailbox.has_ready_event())
            .or_else(|| {
                slots
                    .values()
                    .find(|slot| slot.active.load(Ordering::Acquire) && !slot.mailbox.is_retired())
            })
            .map(|slot| Arc::clone(&slot.mailbox))
    }

    pub(crate) fn request_cleanup(&self) {
        if !self.fully_reclaimed.load(Ordering::Acquire) {
            self.requested.store(true, Ordering::Release);
        }
    }

    pub(crate) fn close(&self) {
        self.closing.store(true, Ordering::Release);
        self.request_cleanup();
    }

    pub(crate) fn cleanup_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
            && !self.faulted.load(Ordering::Acquire)
            && !self.fully_reclaimed.load(Ordering::Acquire)
    }

    pub(crate) fn fully_reclaimed(&self) -> bool {
        self.fully_reclaimed.load(Ordering::Acquire)
    }

    pub(crate) fn retain_fault(&self) {
        self.faulted.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn test_slot_count(&self) -> usize {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    #[cfg(test)]
    pub(crate) fn test_poison(&self, target: &str) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if target == "connection" {
                let _guard = self.slots.lock().unwrap();
                panic!("poison connection slots");
            }
            if target == "pool" {
                let _guard = self.pool.inner.lock().unwrap();
                panic!("poison connection residency");
            }
        }));
    }

    #[cfg(test)]
    pub(crate) fn test_panic_cleanup(&self) {
        self.panic_cleanup.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn test_cleanup_started(&self) -> bool {
        self.cleanup_started.load(Ordering::Acquire)
    }
}

/// Shared lookup for connection mailboxes. Cleanup keeps the original connection record.
#[derive(Clone, Default)]
pub(crate) struct ClientEventPlane {
    connections: Arc<Mutex<HashMap<Arc<str>, Arc<ClientEventConnection>>>>,
}

impl std::fmt::Debug for ClientEventPlane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientEventPlane")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The lock whose poisoning stopped a client cleanup.
pub(crate) enum PoisonedCleanupLock {
    Plane,
    Connection,
    Router,
    Mailbox,
    Pool,
}

#[derive(Debug)]
pub(crate) struct ClientCleanupWork {
    plane: ClientEventPlane,
    pub(crate) connection: Arc<ClientEventConnection>,
}

#[derive(Debug)]
pub(crate) struct ClientCleanupCompletion {
    pub(crate) connection: Arc<ClientEventConnection>,
    pub(crate) closed: bool,
}

#[derive(Debug)]
pub(crate) struct ClientCleanupFailure {
    pub(crate) work: ClientCleanupWork,
    pub(crate) fault: PoisonedCleanupLock,
}

impl ClientCleanupWork {
    pub(crate) fn new(plane: ClientEventPlane, connection: Arc<ClientEventConnection>) -> Self {
        Self { plane, connection }
    }

    pub(crate) fn run(
        self,
        router: &PackageEventRouter,
    ) -> Result<ClientCleanupCompletion, ClientCleanupFailure> {
        match self.apply(router) {
            Ok(closed) => Ok(ClientCleanupCompletion {
                connection: self.connection,
                closed,
            }),
            Err(fault) => {
                self.connection.faulted.store(true, Ordering::Release);
                Err(ClientCleanupFailure { work: self, fault })
            }
        }
    }

    fn apply(&self, router: &PackageEventRouter) -> Result<bool, PoisonedCleanupLock> {
        #[cfg(test)]
        if self.connection.panic_cleanup.swap(false, Ordering::AcqRel) {
            panic!("client cleanup worker panic");
        }
        // Admission never holds the lookup guard while it acquires the slot guard.
        self.connection.requested.store(false, Ordering::Release);
        let closing = self.connection.closing.load(Ordering::Acquire);
        let mut after: Option<Arc<str>> = None;
        loop {
            let selected = {
                let slots = self.connection.lock_slots()?;
                let next = match after.as_ref() {
                    Some(after) => slots
                        .range::<str, _>((
                            std::ops::Bound::Excluded(after.as_ref()),
                            std::ops::Bound::Unbounded,
                        ))
                        .next(),
                    None => slots.first_key_value(),
                };
                next.map(|(id, slot)| (Arc::clone(id), Arc::clone(slot)))
            };
            let Some((subscription_id, slot)) = selected else {
                break;
            };
            if closing || slot.mailbox.is_retired() {
                slot.mailbox.retire();
                // The original slot remains admitted while either lock or payload cleanup waits.
                #[cfg(test)]
                self.connection
                    .cleanup_started
                    .store(true, Ordering::Release);
                router.cleanup_client_holder(
                    &self.connection.identity,
                    &subscription_id,
                    &slot.mailbox,
                )?;
                slot.mailbox.reclaim()?;
                let mut slots = self.connection.lock_slots()?;
                if slots
                    .get(subscription_id.as_ref())
                    .is_some_and(|current| Arc::ptr_eq(current, &slot))
                {
                    slots.remove(subscription_id.as_ref());
                }
            }
            after = Some(subscription_id);
        }
        let closed = closing && self.connection.lock_slots()?.is_empty();
        if closed {
            let mut connections = self
                .plane
                .connections
                .lock()
                .map_err(|_| PoisonedCleanupLock::Plane)?;
            if connections
                .get(self.connection.identity.as_ref())
                .is_some_and(|current| Arc::ptr_eq(current, &self.connection))
            {
                connections.remove(self.connection.identity.as_ref());
            }
            self.connection
                .fully_reclaimed
                .store(true, Ordering::Release);
        }
        Ok(closed)
    }
}

impl ClientEventPlane {
    pub(crate) fn admit_connection(
        &self,
        connection_id: &str,
    ) -> Result<Arc<ClientEventConnection>, ClientEventAdmitError> {
        let mut connections = self
            .connections
            .try_lock()
            .map_err(|_| ClientEventAdmitError::Router(EventPlaneStatus::ShedBusy))?;
        if let Some(connection) = connections.get(connection_id) {
            return Ok(Arc::clone(connection));
        }
        let identity: Arc<str> = Arc::from(connection_id);
        let connection = Arc::new(ClientEventConnection {
            identity: Arc::clone(&identity),
            pool: Arc::new(ConnectionEventPool::default()),
            slots: Mutex::new(BTreeMap::new()),
            reader: OnceLock::new(),
            reader_waiting: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            fully_reclaimed: AtomicBool::new(false),
            requested: AtomicBool::new(false),
            faulted: AtomicBool::new(false),
            #[cfg(test)]
            panic_cleanup: AtomicBool::new(false),
            #[cfg(test)]
            cleanup_started: AtomicBool::new(false),
        });
        connections.insert(identity, Arc::clone(&connection));
        Ok(connection)
    }

    fn connection(&self, connection_id: &str) -> Option<Arc<ClientEventConnection>> {
        self.connections
            .try_lock()
            .ok()?
            .get(connection_id)
            .cloned()
    }

    #[must_use]
    pub(crate) fn mailbox(&self, connection_id: &str) -> Option<Arc<ClientEventMailbox>> {
        self.connection(connection_id)?.mailbox()
    }

    #[must_use]
    pub(crate) fn subscription_mailbox(
        &self,
        connection_id: &str,
        subscription_id: &str,
    ) -> Option<Arc<ClientEventMailbox>> {
        let connection = self.connection(connection_id)?;
        let slots = connection.try_slots().ok()?;
        let slot = slots.get(subscription_id)?;
        (slot.active.load(Ordering::Acquire) && !slot.mailbox.is_retired())
            .then(|| Arc::clone(&slot.mailbox))
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_subscribe(
        &self,
        connection_id: &str,
        subscription_id: &str,
        owner: &str,
        name: &str,
        subjects: Vec<String>,
        policy: PackageEventPlanePolicy,
        router: &PackageEventRouter,
    ) -> Result<(), ClientEventAdmitError> {
        self.subscribe_mailbox(
            connection_id,
            subscription_id,
            owner,
            name,
            subjects,
            policy,
            router,
        )
        .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn subscribe_mailbox(
        &self,
        connection_id: &str,
        subscription_id: &str,
        owner: &str,
        name: &str,
        subjects: Vec<String>,
        policy: PackageEventPlanePolicy,
        router: &PackageEventRouter,
    ) -> Result<Arc<ClientEventMailbox>, ClientEventAdmitError> {
        let compiled = compile_subjects(&subjects)?;
        let connection = self.admit_connection(connection_id)?;
        let mut slots = connection
            .try_slots()
            .map_err(ClientEventAdmitError::Router)?;
        if connection.closing.load(Ordering::Acquire) || connection.faulted.load(Ordering::Acquire)
        {
            return Err(ClientEventAdmitError::Router(
                EventPlaneStatus::RejectedInvalid,
            ));
        }
        if slots.contains_key(subscription_id) {
            return Err(ClientEventAdmitError::DuplicateSubscription);
        }
        if slots.len() >= MAX_SUBSCRIPTIONS_PER_CONNECTION {
            return Err(ClientEventAdmitError::TooManySubscriptions);
        }
        let mut residency = connection
            .pool
            .inner
            .try_lock()
            .map_err(|_| ClientEventAdmitError::Router(EventPlaneStatus::ShedBusy))?;
        if residency.events > CONNECTION_EVENT_MAX.saturating_sub(SUBSCRIPTION_EVENT_RESERVE)
            || residency.bytes > CONNECTION_BYTE_MAX.saturating_sub(SUBSCRIPTION_BYTE_RESERVE)
        {
            return Err(ClientEventAdmitError::ConnectionCapacity);
        }
        let mut mailbox = ClientEventMailbox::new_with_counters(
            policy,
            Some(Arc::clone(router.counters())),
            &format!("{connection_id}/{subscription_id}"),
            Some(Arc::clone(&connection.pool)),
            Some(subscription_id.to_string()),
        );
        mailbox.connection = Some(Arc::downgrade(&connection));
        let mailbox = Arc::new(mailbox);
        // Retain the provisional slot before residency, gap, or router effects can require cleanup.
        slots.insert(
            Arc::from(subscription_id),
            Arc::new(ClientEventSlot {
                mailbox: Arc::clone(&mailbox),
                active: AtomicBool::new(false),
            }),
        );
        mailbox.register_age();
        residency
            .subscriptions
            .insert(subscription_id.to_string(), (0, 0));
        drop(residency);
        let result = mailbox
            .register_gap_slot(subscription_id, owner, name)
            .and_then(|gap| {
                let status = router.try_subscribe_client(ClientEventHolder {
                    connection_id: connection_id.to_string(),
                    subscription_id: subscription_id.to_string(),
                    owner: owner.to_string(),
                    name: name.to_string(),
                    subjects: compiled,
                    mailbox: Arc::clone(&mailbox),
                    gap,
                });
                if status == EventPlaneStatus::Accepted {
                    Ok(())
                } else {
                    Err(status)
                }
            });
        match result {
            Ok(()) => {
                slots
                    .get_mut(subscription_id)
                    .expect("provisional slot remains owned")
                    .active
                    .store(true, Ordering::Release);
                Ok(mailbox)
            }
            Err(status) => {
                mailbox.retire();
                connection.request_cleanup();
                Err(ClientEventAdmitError::Router(status))
            }
        }
    }

    pub(crate) fn retire_subscription(
        &self,
        connection_id: &str,
        subscription_id: &str,
    ) -> Result<Arc<ClientEventMailbox>, ClientEventAdmitError> {
        let connection = self
            .connections
            .try_lock()
            .map_err(|_| ClientEventAdmitError::Router(EventPlaneStatus::ShedBusy))?
            .get(connection_id)
            .cloned()
            .ok_or(ClientEventAdmitError::UnknownSubscription)?;
        let slots = connection
            .try_slots()
            .map_err(ClientEventAdmitError::Router)?;
        let slot = slots
            .get(subscription_id)
            .filter(|slot| slot.active.load(Ordering::Acquire) && !slot.mailbox.is_retired())
            .ok_or(ClientEventAdmitError::UnknownSubscription)?;
        slot.mailbox.retire();
        connection.request_cleanup();
        Ok(Arc::clone(&slot.mailbox))
    }

    #[cfg(test)]
    pub(crate) fn test_residency(&self, connection_id: &str) -> Option<(usize, usize, usize)> {
        let connection = self.connection(connection_id)?;
        let residency = connection.pool.inner.try_lock().ok()?;
        Some((
            residency.events,
            residency.bytes,
            residency.subscriptions.len(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn test_poison_lookup(&self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self.connections.lock().unwrap();
            panic!("poison connection lookup");
        }));
    }
}

pub(crate) fn compile_subjects(
    subjects: &[String],
) -> Result<BTreeSet<String>, ClientEventAdmitError> {
    if subjects.len() > MAX_SUBJECTS_PER_SUBSCRIPTION {
        return Err(ClientEventAdmitError::TooManySubjects);
    }
    let mut compiled = BTreeSet::new();
    let mut aggregate = 0usize;
    for subject in subjects {
        if subject.is_empty() {
            return Err(ClientEventAdmitError::EmptySubject);
        }
        if subject.chars().any(|ch| GLOB_CHARS.contains(&ch)) {
            return Err(ClientEventAdmitError::WildcardSubject);
        }
        let bytes = subject.len();
        if bytes > MAX_SUBJECT_UTF8_BYTES {
            return Err(ClientEventAdmitError::SubjectTooLong);
        }
        aggregate = aggregate.saturating_add(bytes);
        if aggregate > MAX_SUBJECT_AGGREGATE_BYTES {
            return Err(ClientEventAdmitError::AggregateTooLarge);
        }
        if !compiled.insert(subject.clone()) {
            return Err(ClientEventAdmitError::DuplicateSubject);
        }
    }
    Ok(compiled)
}

fn lock_mailbox(
    mutex: &Mutex<MailboxInner>,
) -> Result<std::sync::MutexGuard<'_, MailboxInner>, EventPlaneStatus> {
    match mutex.try_lock() {
        Ok(guard) => Ok(guard),
        Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => {
            Err(EventPlaneStatus::ShedBusy)
        }
    }
}

pub(crate) fn subscribe_events_response() -> DaemonResponse {
    daemon_response_base(DaemonResponseKind::EventSubscribed)
}

pub(crate) fn unsubscribe_events_response() -> DaemonResponse {
    daemon_response_base(DaemonResponseKind::EventUnsubscribed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PackageEventPlanePolicy;
    use crate::package_event_router::{EmittedContract, EventAudience};
    use crate::package_event_schema::CompiledEventSchema;
    use serde_json::json;
    use std::collections::BTreeSet;
    use std::sync::Barrier;
    use std::sync::mpsc;
    use std::time::Duration;

    impl ClientEventPlane {
        fn test_close(&self, connection_id: &str, _router: &PackageEventRouter) {
            self.connection(connection_id).unwrap().close();
        }

        fn test_has_cleanup(&self) -> bool {
            self.connections
                .lock()
                .unwrap()
                .values()
                .any(|connection| connection.cleanup_requested())
        }

        fn test_reclaim(&self, router: &PackageEventRouter) -> bool {
            let connections: Vec<_> = self.connections.lock().unwrap().values().cloned().collect();
            for connection in connections {
                if connection.cleanup_requested() {
                    ClientCleanupWork::new(self.clone(), connection)
                        .run(router)
                        .unwrap();
                }
            }
            self.test_has_cleanup()
        }

        fn test_unsubscribe(
            &self,
            connection_id: &str,
            subscription_id: &str,
            router: &PackageEventRouter,
        ) -> Result<(), ClientEventAdmitError> {
            self.retire_subscription(connection_id, subscription_id)?;
            self.test_reclaim(router);
            Ok(())
        }

        fn test_retire_mailbox(
            &self,
            _connection_id: &str,
            _subscription_id: &str,
            mailbox: &Arc<ClientEventMailbox>,
            router: &PackageEventRouter,
        ) {
            mailbox.retire();
            if let Some(connection) = mailbox.connection() {
                connection.request_cleanup();
            }
            self.test_reclaim(router);
        }
    }

    fn admitted_router(audience: EventAudience) -> PackageEventRouter {
        let router = PackageEventRouter::new(PackageEventPlanePolicy::default());
        router
            .try_register_contracts(vec![EmittedContract {
                owner: "owner".into(),
                name: "ready".into(),
                audience: BTreeSet::from([audience]),
                schema: CompiledEventSchema::compile(&json!({
                    "type": "object",
                    "additionalProperties": true
                }))
                .expect("schema"),
                package_generation: 1,
            }])
            .expect("register");
        router
    }

    fn run_while_pool_is_contended<R: Send + 'static>(
        pool: &Arc<ConnectionEventPool>,
        operation: impl FnOnce() -> R + Send + 'static,
    ) -> R {
        let guard = pool
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let start = Arc::new(Barrier::new(2));
        let worker_start = Arc::clone(&start);
        let (result_tx, result_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            worker_start.wait();
            result_tx.send(operation()).expect("send result");
        });
        start.wait();
        let result = result_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("operation must not wait for the contended pool");
        drop(guard);
        worker.join().expect("contention worker");
        result
    }

    fn assert_pool_empty(pool: &ConnectionEventPool, expected_subscriptions: usize) {
        let residency = pool
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(residency.events, 0);
        assert_eq!(residency.bytes, 0);
        assert_eq!(residency.subscriptions.len(), expected_subscriptions);
    }

    #[test]
    fn dequeue_sheds_pool_contention_then_releases_exact_residency() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe(
                "connection",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("subscribe");
        let mailbox = plane
            .subscription_mailbox("connection", "sub")
            .expect("mailbox");
        mailbox
            .try_push("sub", "owner", "ready", json!({"value": 1}), 17)
            .expect("push");
        let pool = mailbox.connection_pool.as_ref().expect("pool").clone();
        let contended_mailbox = Arc::clone(&mailbox);
        let event =
            run_while_pool_is_contended(&pool, move || contended_mailbox.take_ready_event());
        assert!(event.is_none());
        let event = mailbox.take_ready_event();
        assert!(matches!(event, Some(DaemonEvent::PackageEvent { .. })));
        assert_pool_empty(&pool, 1);
    }

    #[test]
    fn expiry_sheds_pool_contention_then_releases_exact_residency() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy {
            queue_age: Duration::from_millis(1),
            ..PackageEventPlanePolicy::default()
        };
        plane
            .try_subscribe(
                "connection",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("subscribe");
        let mailbox = plane
            .subscription_mailbox("connection", "sub")
            .expect("mailbox");
        mailbox
            .try_push("sub", "owner", "ready", json!({"value": 1}), 19)
            .expect("push");
        std::thread::sleep(Duration::from_millis(3));
        let pool = mailbox.connection_pool.as_ref().expect("pool").clone();
        let contended_mailbox = Arc::clone(&mailbox);
        let event =
            run_while_pool_is_contended(&pool, move || contended_mailbox.take_ready_event());
        assert!(event.is_none());
        let event = mailbox.take_ready_event();
        assert!(matches!(event, Some(DaemonEvent::EventGap { .. })));
        assert_pool_empty(&pool, 1);
    }

    #[test]
    fn unsubscribe_queues_exact_release_without_waiting_for_pool() {
        let router = Arc::new(admitted_router(EventAudience::Clients));
        let plane = Arc::new(ClientEventPlane::default());
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe(
                "connection",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("subscribe");
        let mailbox = plane
            .subscription_mailbox("connection", "sub")
            .expect("mailbox");
        mailbox
            .try_push("sub", "owner", "ready", json!({"value": 1}), 23)
            .expect("push");
        let pool = mailbox.connection_pool.as_ref().expect("pool").clone();
        let worker_plane = Arc::clone(&plane);
        run_while_pool_is_contended(&pool, move || {
            worker_plane.retire_subscription("connection", "sub")
        })
        .expect("unsubscribe");
        assert!(mailbox.is_retired());
        assert!(mailbox.take_wake());
        assert!(mailbox.take_ready_event().is_none());
        plane.test_reclaim(&router);
        assert_pool_empty(&pool, 0);
    }

    #[test]
    fn connection_cleanup_retires_diagnostics_only_after_mailbox_lock_release() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        plane
            .try_subscribe(
                "connection",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                PackageEventPlanePolicy::default(),
                &router,
            )
            .unwrap();
        let mailbox = plane.subscription_mailbox("connection", "sub").unwrap();
        mailbox
            .try_push("sub", "owner", "ready", json!({"value": 1}), 23)
            .unwrap();
        mailbox.test_with_inner_held(|| {
            plane.test_close("connection", &router);
            assert!(plane.test_has_cleanup());
            assert!(mailbox.is_retired());
            assert!(
                !mailbox.age_cell.is_write_closed(),
                "retirement must wait for the current metric writer"
            );
            assert!(
                router
                    .counters()
                    .snapshot()
                    .queue_ages
                    .iter()
                    .any(|row| row.identity == "connection/sub")
            );
        });
        assert!(!plane.test_reclaim(&router));
        assert!(mailbox.age_cell.is_write_closed());
        assert_eq!(
            mailbox.age_cell.sample(),
            crate::event_plane_counters::AgeSample::Empty { count: 0, bytes: 0 }
        );
        assert!(
            !router
                .counters()
                .snapshot()
                .queue_ages
                .iter()
                .any(|row| row.identity == "connection/sub")
        );
    }

    #[test]
    fn old_mailbox_retirement_preserves_same_id_replacement() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe(
                "connection",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("subscribe old");
        let old = plane
            .subscription_mailbox("connection", "sub")
            .expect("old mailbox");
        plane
            .test_unsubscribe("connection", "sub", &router)
            .expect("unsubscribe old");
        plane
            .try_subscribe(
                "connection",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("subscribe replacement");
        let replacement = plane
            .subscription_mailbox("connection", "sub")
            .expect("replacement mailbox");
        plane.test_retire_mailbox("connection", "sub", &old, &router);
        let current = plane
            .subscription_mailbox("connection", "sub")
            .expect("replacement survives old retirement");
        assert!(Arc::ptr_eq(&current, &replacement));
    }

    #[test]
    fn old_mailbox_retirement_before_resubscribe_allows_replacement() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe(
                "connection",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("subscribe old");
        let old = plane
            .subscription_mailbox("connection", "sub")
            .expect("old mailbox");
        plane.test_retire_mailbox("connection", "sub", &old, &router);
        assert!(plane.subscription_mailbox("connection", "sub").is_none());
        plane
            .try_subscribe(
                "connection",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("subscribe replacement after retirement");
    }

    #[test]
    fn connection_cleanup_queues_exact_release_without_waiting_for_pool() {
        let router = Arc::new(admitted_router(EventAudience::Clients));
        let plane = Arc::new(ClientEventPlane::default());
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe(
                "connection",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("subscribe");
        let mailbox = plane
            .subscription_mailbox("connection", "sub")
            .expect("mailbox");
        mailbox
            .try_push("sub", "owner", "ready", json!({"value": 1}), 29)
            .expect("push");
        let pool = mailbox.connection_pool.as_ref().expect("pool").clone();
        let worker_plane = Arc::clone(&plane);
        let worker_router = Arc::clone(&router);
        run_while_pool_is_contended(&pool, move || {
            worker_plane.test_close("connection", &worker_router);
        });
        plane.test_reclaim(&router);
        assert_pool_empty(&pool, 0);
        assert!(plane.mailbox("connection").is_none());
    }

    #[test]
    fn admitted_siblings_keep_fixed_event_reserves() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        for subscription_id in ["noisy", "sibling"] {
            plane
                .try_subscribe(
                    "connection",
                    subscription_id,
                    "owner",
                    "ready",
                    Vec::new(),
                    policy,
                    &router,
                )
                .expect("subscribe");
        }
        let noisy = plane
            .subscription_mailbox("connection", "noisy")
            .expect("noisy mailbox");
        let sibling = plane
            .subscription_mailbox("connection", "sibling")
            .expect("sibling mailbox");
        for index in 0..(CONNECTION_EVENT_MAX - SUBSCRIPTION_EVENT_RESERVE) {
            noisy
                .try_push("noisy", "owner", "ready", serde_json::json!(index), 1)
                .expect("noisy borrows only unreserved capacity");
        }
        assert_eq!(
            noisy.try_push("noisy", "owner", "ready", serde_json::json!("full"), 1),
            Err(EventPlaneStatus::ShedFull)
        );
        for index in 0..SUBSCRIPTION_EVENT_RESERVE {
            sibling
                .try_push("sibling", "owner", "ready", serde_json::json!(index), 1)
                .expect("sibling reserve remains usable");
        }
        assert_eq!(
            plane.test_residency("connection"),
            Some((CONNECTION_EVENT_MAX, CONNECTION_EVENT_MAX, 2))
        );
    }

    #[test]
    fn later_subscription_is_rejected_until_capacity_drains() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe(
                "connection",
                "first",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("first subscribe");
        let first = plane
            .subscription_mailbox("connection", "first")
            .expect("first mailbox");
        for index in 0..CONNECTION_EVENT_MAX {
            first
                .try_push("first", "owner", "ready", serde_json::json!(index), 1)
                .expect("one subscription uses full depth");
        }
        assert_eq!(
            plane.try_subscribe(
                "connection",
                "later",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            ),
            Err(ClientEventAdmitError::ConnectionCapacity)
        );
        for _ in 0..SUBSCRIPTION_EVENT_RESERVE {
            assert!(first.take_ready_event().is_some());
        }
        plane
            .try_subscribe(
                "connection",
                "later",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("later subscribe after drain");
    }

    #[test]
    fn subject_boundaries_use_utf8_bytes_and_exact_sets() {
        let sixteen: Vec<String> = (0..16).map(|index| format!("s{index}")).collect();
        assert!(compile_subjects(&sixteen).is_ok());
        let mut seventeen = sixteen.clone();
        seventeen.push("s16".into());
        assert_eq!(
            compile_subjects(&seventeen),
            Err(ClientEventAdmitError::TooManySubjects)
        );

        let exact_256 = "a".repeat(256);
        assert!(compile_subjects(std::slice::from_ref(&exact_256)).is_ok());
        assert_eq!(
            compile_subjects(&["a".repeat(257)]),
            Err(ClientEventAdmitError::SubjectTooLong)
        );

        let multi_byte = "é".repeat(128);
        assert_eq!(multi_byte.len(), 256);
        assert!(compile_subjects(&[multi_byte]).is_ok());
        assert_eq!(
            compile_subjects(&["é".repeat(129)]),
            Err(ClientEventAdmitError::SubjectTooLong)
        );

        let aggregate: Vec<String> = (0..16)
            .map(|index| format!("{index:02}{}", "a".repeat(254)))
            .collect();
        assert_eq!(aggregate.iter().map(String::len).sum::<usize>(), 4_096);
        assert!(compile_subjects(&aggregate).is_ok());
        let just_under: Vec<String> = {
            let mut values: Vec<String> = (0..15)
                .map(|index| format!("{index:02}{}", "a".repeat(254)))
                .collect();
            values.push(format!("zz{}", "a".repeat(253)));
            values
        };
        assert!(just_under.iter().map(String::len).sum::<usize>() < 4_096);
        assert!(compile_subjects(&just_under).is_ok());

        assert_eq!(
            compile_subjects(&[String::new()]),
            Err(ClientEventAdmitError::EmptySubject)
        );
        assert_eq!(
            compile_subjects(&["foo*".into()]),
            Err(ClientEventAdmitError::WildcardSubject)
        );
        assert_eq!(
            compile_subjects(&["a".into(), "a".into()]),
            Err(ClientEventAdmitError::DuplicateSubject)
        );
    }

    #[test]
    fn connection_scope_keeps_holders_independent() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe(
                "conn-a",
                "same",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("a");
        plane
            .try_subscribe(
                "conn-b",
                "same",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("b");
        assert_eq!(
            plane.try_subscribe(
                "conn-a",
                "same",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router
            ),
            Err(ClientEventAdmitError::DuplicateSubscription)
        );
        assert_eq!(
            plane.test_unsubscribe("conn-b", "missing", &router),
            Err(ClientEventAdmitError::UnknownSubscription)
        );
        plane
            .test_unsubscribe("conn-b", "same", &router)
            .expect("b unsub");
        assert!(plane.mailbox("conn-a").is_some());
        assert!(plane.mailbox("conn-b").is_none());
        plane.test_close("conn-a", &router);
        assert!(plane.mailbox("conn-a").is_none());
    }

    #[test]
    fn disconnect_cleanup_keeps_holder_until_router_accepts() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe("conn", "sub", "owner", "ready", Vec::new(), policy, &router)
            .expect("subscribe");
        assert_eq!(router.test_client_holder_count("conn"), 1);
        router.test_with_inner_held(|| {
            plane.test_close("conn", &router);
        });
        assert!(
            plane.test_has_cleanup(),
            "shed_busy must leave cleanup ownership on the ledger"
        );
        assert!(
            plane.connection("conn").is_some(),
            "the original connection stays until worker reclamation"
        );
        assert_eq!(router.test_client_holder_count("conn"), 1);
        assert!(
            !plane.test_reclaim(&router),
            "retry must finish after the router lock is free"
        );
        assert_eq!(router.test_client_holder_count("conn"), 0);
        assert!(plane.mailbox("conn").is_none());
        assert!(!plane.test_has_cleanup());
    }

    #[test]
    fn missed_notify_is_recovered_by_wake_bit() {
        let mailbox = ClientEventMailbox::new(PackageEventPlanePolicy::default());
        assert!(!mailbox.take_wake());
        mailbox
            .try_push("sub", "owner", "ready", json!({ "ok": true }), 8)
            .expect("push");
        assert!(mailbox.take_wake());
        assert!(!mailbox.take_wake());
        assert!(mailbox.has_ready_event());
    }

    #[test]
    fn sixty_fifth_subscription_is_rejected() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        for index in 0..MAX_SUBSCRIPTIONS_PER_CONNECTION {
            plane
                .try_subscribe(
                    "conn",
                    &format!("sub-{index}"),
                    "owner",
                    "ready",
                    Vec::new(),
                    policy,
                    &router,
                )
                .expect("subscribe under cap");
        }
        assert_eq!(
            plane.try_subscribe(
                "conn",
                "sub-64",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router
            ),
            Err(ClientEventAdmitError::TooManySubscriptions)
        );
    }

    #[test]
    fn mailbox_gap_is_outside_the_queue_and_writes_first() {
        let policy = PackageEventPlanePolicy {
            consumer_queue_max_events: 1,
            ..PackageEventPlanePolicy::default()
        };
        let mailbox = ClientEventMailbox::new(policy);
        mailbox
            .try_push("sub", "owner", "ready", json!({ "ok": true }), 8)
            .expect("first event");
        assert_eq!(
            mailbox.try_push("sub", "owner", "ready", json!({ "ok": true }), 8),
            Err(EventPlaneStatus::ShedFull)
        );
        match mailbox.take_ready_event() {
            Some(DaemonEvent::EventGap {
                subscription_id, ..
            }) => {
                assert_eq!(subscription_id, "sub");
            }
            other => panic!("gap must come first: {other:?}"),
        }
        match mailbox.take_ready_event() {
            Some(DaemonEvent::PackageEvent { .. }) => {}
            other => panic!("queued event remains after gap: {other:?}"),
        }
    }

    #[test]
    fn mailbox_contention_records_gap_without_replaying_events() {
        let mailbox = ClientEventMailbox::new(PackageEventPlanePolicy::default());
        mailbox.test_with_inner_held(|| {
            assert_eq!(
                mailbox.try_push("sub", "owner", "ready", json!({ "ok": true }), 8),
                Err(EventPlaneStatus::ShedBusy)
            );
            mailbox.set_gap("sub", "owner", "ready");
        });
        match mailbox.take_ready_event() {
            Some(DaemonEvent::EventGap {
                subscription_id,
                owner,
                name,
            }) => {
                assert_eq!(subscription_id, "sub");
                assert_eq!(owner, "owner");
                assert_eq!(name, "ready");
            }
            other => panic!("gap must survive lock contention: {other:?}"),
        }
        assert!(
            mailbox.take_ready_event().is_none(),
            "contention must not replay event history"
        );
    }

    #[test]
    fn gap_slot_registration_rejects_busy_and_shed_sets_visible_gap() {
        let policy = PackageEventPlanePolicy {
            consumer_queue_max_events: 1,
            ..PackageEventPlanePolicy::default()
        };
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        plane
            .try_subscribe(
                "conn",
                "sub-a",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("first subscribe");
        let mailbox = plane.mailbox("conn").expect("mailbox");
        mailbox.test_with_slots_held(|| {
            assert_eq!(
                plane.try_subscribe(
                    "conn",
                    "sub-b",
                    "owner",
                    "ready",
                    Vec::new(),
                    policy,
                    &router
                ),
                Ok(())
            );
        });
        plane
            .test_unsubscribe("conn", "sub-b", &router)
            .expect("sibling mailbox admission is isolated");
        let payload = json!({ "ok": true });
        assert_eq!(
            router.try_ingress("owner", "ready", &payload, Instant::now()),
            EventPlaneStatus::Accepted
        );
        mailbox.test_with_slots_held(|| {
            assert_eq!(
                router.try_ingress("owner", "ready", &payload, Instant::now()),
                EventPlaneStatus::Accepted
            );
        });
        match mailbox.take_ready_event() {
            Some(DaemonEvent::EventGap {
                subscription_id,
                owner,
                name,
            }) => {
                assert_eq!(subscription_id, "sub-a");
                assert_eq!(owner, "owner");
                assert_eq!(name, "ready");
            }
            other => {
                panic!("real shed must surface EventGap without a manual bit store: {other:?}")
            }
        }
    }

    #[test]
    fn connection_cleanup_keeps_other_connection_work_owned() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        for connection_id in ["conn-a", "conn-b"] {
            plane
                .try_subscribe(
                    connection_id,
                    "sub",
                    "owner",
                    "ready",
                    Vec::new(),
                    PackageEventPlanePolicy::default(),
                    &router,
                )
                .unwrap();
        }
        let first = plane.connection("conn-a").unwrap();
        first.close();
        let work = ClientCleanupWork::new(plane.clone(), first);
        plane.test_close("conn-b", &router);
        work.run(&router).unwrap();
        assert!(plane.test_has_cleanup());
        assert_eq!(router.test_client_holder_count("conn-b"), 1);
        assert!(!plane.test_reclaim(&router));
        assert_eq!(router.test_client_holder_count("conn-b"), 0);
    }

    fn mailbox_row(
        counters: &crate::event_plane_counters::EventPlaneCounters,
        identity: &str,
    ) -> botster_hub_client::DaemonQueueAgeObservation {
        counters
            .snapshot()
            .queue_ages
            .into_iter()
            .find(|row| row.kind == DaemonQueueKind::ClientMailbox && row.identity == identity)
            .expect("mailbox row")
    }

    #[test]
    fn mailbox_pop_and_unsubscribe_keep_age_current() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe("conn", "sub", "owner", "ready", Vec::new(), policy, &router)
            .expect("subscribe");
        let mailbox = plane.mailbox("conn").expect("mailbox");
        mailbox
            .try_push("sub", "owner", "ready", json!({ "ok": true }), 8)
            .expect("push");
        let counters = router.counters();
        let row = mailbox_row(counters, "conn/sub");
        assert_eq!(row.state, botster_hub_client::DaemonQueueAgeState::Usable);
        assert_eq!(row.queue_count, Some(1));
        assert!(row.oldest_age_us.is_some());
        match mailbox.take_ready_event() {
            Some(DaemonEvent::PackageEvent { .. }) => {}
            other => panic!("expected event: {other:?}"),
        }
        let row = mailbox_row(counters, "conn/sub");
        assert_eq!(row.state, botster_hub_client::DaemonQueueAgeState::Empty);
        assert_eq!(row.queue_count, Some(0));
        assert!(row.oldest_age_us.is_none());
        plane
            .test_unsubscribe("conn", "sub", &router)
            .expect("unsubscribe");
        assert!(plane.mailbox("conn").is_none());
        assert!(!counters.snapshot().queue_ages.iter().any(|row| {
            row.kind == DaemonQueueKind::ClientMailbox && row.identity == "conn/sub"
        }));
        assert!(mailbox.age_cell.is_write_closed());
        assert_eq!(
            mailbox.age_cell.sample(),
            crate::event_plane_counters::AgeSample::Empty { count: 0, bytes: 0 }
        );
    }

    #[test]
    fn mailbox_expiry_and_connection_churn_bound_the_registry() {
        let policy = PackageEventPlanePolicy {
            queue_age: std::time::Duration::from_millis(1),
            ..PackageEventPlanePolicy::default()
        };
        let router = PackageEventRouter::new(policy);
        router
            .try_register_contracts(vec![EmittedContract {
                owner: "owner".into(),
                name: "ready".into(),
                audience: BTreeSet::from([EventAudience::Clients]),
                schema: CompiledEventSchema::compile(&json!({
                    "type": "object",
                    "additionalProperties": true
                }))
                .expect("schema"),
                package_generation: 1,
            }])
            .expect("register");
        let plane = ClientEventPlane::default();
        plane
            .try_subscribe(
                "conn-old",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("subscribe");
        let mailbox = plane.mailbox("conn-old").expect("mailbox");
        mailbox
            .try_push("sub", "owner", "ready", json!({ "ok": true }), 8)
            .expect("push");
        std::thread::sleep(std::time::Duration::from_millis(3));
        match mailbox.take_ready_event() {
            Some(DaemonEvent::EventGap { .. }) => {}
            other => panic!("expired mailbox event must become a gap: {other:?}"),
        }
        let counters = router.counters();
        let row = mailbox_row(counters, "conn-old/sub");
        assert_eq!(row.state, botster_hub_client::DaemonQueueAgeState::Empty);
        assert_eq!(row.queue_count, Some(0));
        drop(mailbox);
        plane.test_close("conn-old", &router);
        plane.test_reclaim(&router);
        assert!(plane.mailbox("conn-old").is_none());
        for index in 0..8 {
            let connection_id = format!("churn-{index}");
            plane
                .try_subscribe(
                    &connection_id,
                    "sub",
                    "owner",
                    "ready",
                    Vec::new(),
                    policy,
                    &router,
                )
                .expect("churn subscribe");
            plane.test_close(&connection_id, &router);
            plane.test_reclaim(&router);
        }
        let mailbox_rows = |counters: &crate::event_plane_counters::EventPlaneCounters| {
            counters
                .snapshot()
                .queue_ages
                .into_iter()
                .filter(|row| row.kind == DaemonQueueKind::ClientMailbox)
                .collect::<Vec<_>>()
        };
        assert!(
            mailbox_rows(counters).is_empty(),
            "completed cleanup must remove retired mailbox rows: {:?}",
            mailbox_rows(counters)
        );
        plane
            .try_subscribe(
                "conn-live",
                "sub",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("live subscribe");
        let live_mailboxes = mailbox_rows(counters);
        assert_eq!(
            live_mailboxes.len(),
            1,
            "only the live mailbox must remain registered: {live_mailboxes:?}"
        );
        assert_eq!(live_mailboxes[0].identity, "conn-live/sub");
    }

    #[test]
    fn delayed_mailbox_drop_does_not_retire_a_replacement_cell() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe(
                "conn",
                "sub-old",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("old subscribe");
        let old = plane.mailbox("conn").expect("old mailbox");
        old.try_push("sub-old", "owner", "ready", json!({ "ok": true }), 8)
            .expect("old push");
        plane.test_close("conn", &router);
        plane.test_reclaim(&router);
        plane
            .try_subscribe(
                "conn",
                "sub-new",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("replacement subscribe");
        let new = plane.mailbox("conn").expect("new mailbox");
        new.try_push("sub-new", "owner", "ready", json!({ "ok": true }), 8)
            .expect("new push");
        drop(old);
        let row = mailbox_row(router.counters(), "conn/sub-new");
        assert_eq!(row.state, botster_hub_client::DaemonQueueAgeState::Usable);
        assert_eq!(row.queue_count, Some(1));
        assert!(row.oldest_age_us.is_some());
        match new.take_ready_event() {
            Some(DaemonEvent::PackageEvent {
                subscription_id, ..
            }) => assert_eq!(subscription_id, "sub-new"),
            other => panic!("replacement mailbox must stay readable: {other:?}"),
        }
    }
}
