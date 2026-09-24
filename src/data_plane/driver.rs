//! One owned Hub thread that waits on Core wakes, drives targeted pumps, and
//! runs host operations against the single Core owner.
//!
//! The Hub owner thread never blocks on Core. It submits work through
//! [`CoreDaemonHandle::submit`] or [`CoreDaemonHandle::begin`] and reads the
//! outcome later from a keyed [`CoreTicket`] polled from its own turn. The
//! data-plane thread publishes ticket results before an independent completion
//! wake. Pump facts use `ControlMessage::DataPlaneProgress` instead.

use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core_daemon::{
    CoreCompletion, CoreDaemon, CoreDaemonConfig, CoreDaemonError, CoreOperation,
    PendingOperationId, WakePumpControl, WakePumpWait,
};

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::data_plane::close_work::CloseWorkSource;
use crate::owner_identity::{OwnerWorkIdentity, WaiterId, WaiterIdSource};
use crate::subscription::closed_events::session_close_event_decision;

pub(crate) const DATA_PLANE_WATCHDOG: Duration = Duration::from_secs(1);
pub(crate) const DATA_PLANE_STOP_SLACK: Duration = Duration::from_millis(500);
pub(crate) const DATA_PLANE_STOP_BOUND: Duration = Duration::from_millis(
    DATA_PLANE_WATCHDOG.as_millis() as u64 * 2 + DATA_PLANE_STOP_SLACK.as_millis() as u64,
);
pub(crate) const DATA_PLANE_MAX_CLOSE_KEYS: usize = 8;
/// Host operations the owner may leave queued. A submission that finds the
/// queue full is refused at once with a [`CoreTicketPoll::Refused`] ticket;
/// admission never waits for the data-plane thread.
pub(crate) const CORE_REQUEST_CAPACITY: usize = 64;
/// Each admitted owner waiter can register one two-phase Core operation.
/// The owner permit bound therefore also bounds every unconsumed identity.
pub(crate) const CORE_OWNER_COMPLETION_CAPACITY: usize =
    crate::daemon::owner_budget::OWNER_BUDGET_CAPACITY * 2
        + crate::daemon::owner_loop::BACKGROUND_CORE_WORK_CLASSES;
const CORE_REQUESTS_PER_TURN: usize = CORE_REQUEST_CAPACITY;
const STOP_ACTION_SHUTDOWN: u8 = 0;
const STOP_ACTION_RELEASE_FOR_RESTART: u8 = 1;
const DATA_PLANE_PROGRESS: u8 = 1 << 0;
const DATA_PLANE_JOURNAL_ADVANCED: u8 = 1 << 1;
const DATA_PLANE_TERMINAL_INVENTORY_CHANGED: u8 = 1 << 2;

pub(crate) const DATA_PLANE_DRIVER_STOP_TIMEOUT: &str = "data_plane_driver_stop_timeout";

pub(crate) struct DataPlaneDriver {
    core: CoreDaemonHandle,
    stop_action: Arc<AtomicU8>,
    done: Receiver<()>,
    thread: Option<JoinHandle<()>>,
    owner_wake: Arc<Mutex<Option<ControlSender>>>,
    progress_latch: Arc<DataPlaneProgressLatch>,
}

/// Coalesced data-plane facts. The producer sets each bit before its
/// best-effort doorbell, and the owner clears the bits only after reading them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DataPlaneProgress {
    pub(crate) progressed: bool,
    pub(crate) journal_advanced: bool,
    pub(crate) terminal_inventory_changed: bool,
}

#[derive(Debug, Default)]
struct DataPlaneProgressLatch {
    bits: AtomicU8,
}

impl DataPlaneProgressLatch {
    fn publish(&self, progress: DataPlaneProgress, owner_wake: &Mutex<Option<ControlSender>>) {
        let mut bits = 0;
        if progress.progressed {
            bits |= DATA_PLANE_PROGRESS;
        }
        if progress.journal_advanced {
            bits |= DATA_PLANE_JOURNAL_ADVANCED;
        }
        if progress.terminal_inventory_changed {
            bits |= DATA_PLANE_TERMINAL_INVENTORY_CHANGED;
        }
        if bits == 0 {
            return;
        }
        self.bits.fetch_or(bits, Ordering::Release);
        if let Ok(slot) = owner_wake.lock()
            && let Some(sender) = slot.as_ref()
        {
            let _ = sender.try_send(ControlMessage::DataPlaneProgress);
        }
    }

    fn take(&self) -> DataPlaneProgress {
        let bits = self.bits.swap(0, Ordering::AcqRel);
        DataPlaneProgress {
            progressed: bits & DATA_PLANE_PROGRESS != 0,
            journal_advanced: bits & DATA_PLANE_JOURNAL_ADVANCED != 0,
            terminal_inventory_changed: bits & DATA_PLANE_TERMINAL_INVENTORY_CHANGED != 0,
        }
    }
}

type PendingCoreOperations = BTreeMap<PendingOperationId, CoreResultPublisher>;
type CoreRequestOperation =
    dyn FnOnce(&mut CoreDaemon, &mut PendingCoreOperations) + Send + 'static;

struct CoreRequest {
    operation: Box<CoreRequestOperation>,
    // Free the closure allocation before releasing its charge.
    charge: Option<crate::lua_memory::LuaCallbackCharge>,
}

impl CoreRequest {
    fn new<F>(operation: F) -> Self
    where
        F: FnOnce(&mut CoreDaemon, &mut PendingCoreOperations) + Send + 'static,
    {
        Self {
            operation: Box::new(operation),
            charge: None,
        }
    }

    fn run(self, daemon: &mut CoreDaemon, pending: &mut PendingCoreOperations) {
        let Self { operation, charge } = self;
        operation(daemon, pending);
        drop(charge);
    }

    fn charged<F>(operation: F, charge: crate::lua_memory::LuaCallbackCharge) -> Self
    where
        F: FnOnce(&mut CoreDaemon, &mut PendingCoreOperations) + Send + 'static,
    {
        Self {
            operation: Box::new(operation),
            charge: Some(charge),
        }
    }
}

#[cfg(feature = "allocation-oracle")]
pub(crate) mod allocation_oracle;

#[cfg(test)]
pub(crate) mod local_reply_tests;

#[derive(Debug)]
struct CoreCompletionWake {
    pending: AtomicBool,
    owner: Mutex<Option<ControlSender>>,
    terminal_owner: Mutex<Option<thread::Thread>>,
    identities: Mutex<CoreCompletionIdentities>,
}

#[derive(Debug, Default)]
struct CoreCompletionIdentities {
    next_phase: BTreeMap<WaiterId, u64>,
    registered: BTreeSet<OwnerWorkIdentity>,
    ready: BTreeSet<OwnerWorkIdentity>,
}

impl CoreCompletionWake {
    fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            owner: Mutex::new(None),
            terminal_owner: Mutex::new(None),
            identities: Mutex::new(CoreCompletionIdentities::default()),
        }
    }

    fn register_phases(
        &self,
        waiter_id: WaiterId,
        phase_count: usize,
    ) -> Option<Vec<OwnerWorkIdentity>> {
        let mut state = self
            .identities
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state
            .registered
            .range(
                OwnerWorkIdentity {
                    waiter_id,
                    phase: 0,
                }..=OwnerWorkIdentity {
                    waiter_id,
                    phase: u64::MAX,
                },
            )
            .next()
            .is_some()
        {
            return None;
        }
        let next_len = state.registered.len().checked_add(phase_count)?;
        if next_len > CORE_OWNER_COMPLETION_CAPACITY || phase_count == 0 {
            return None;
        };
        let previous = state.next_phase.get(&waiter_id).copied().unwrap_or(0);
        let mut identities = Vec::with_capacity(phase_count);
        let mut phase = previous;
        for _ in 0..phase_count {
            phase = phase.checked_add(1)?;
            identities.push(OwnerWorkIdentity { waiter_id, phase });
        }
        state.registered.extend(identities.iter().copied());
        state.next_phase.insert(waiter_id, phase);
        Some(identities)
    }

    fn retire(&self, identity: OwnerWorkIdentity) -> bool {
        let mut state = self
            .identities
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.ready.remove(&identity);
        state.registered.remove(&identity)
    }

    fn awaits_collection(&self, identity: OwnerWorkIdentity) -> bool {
        self.identities
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .registered
            .contains(&identity)
    }

    fn collect_ready(&self, identity: OwnerWorkIdentity) -> bool {
        let mut state = self
            .identities
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !state.ready.remove(&identity) {
            return false;
        }
        state.registered.remove(&identity);
        true
    }

    fn retire_waiter(&self, waiter_id: WaiterId) -> usize {
        let mut state = self
            .identities
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let identities = state
            .registered
            .range(
                OwnerWorkIdentity {
                    waiter_id,
                    phase: 0,
                }..=OwnerWorkIdentity {
                    waiter_id,
                    phase: u64::MAX,
                },
            )
            .copied()
            .collect::<Vec<_>>();
        for identity in &identities {
            state.ready.remove(identity);
            state.registered.remove(identity);
        }
        state.next_phase.remove(&waiter_id);
        identities.len()
    }

    fn bind(&self, sender: ControlSender) {
        let mut owner = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        *owner = Some(sender.clone());
        let should_wake = self.pending.load(Ordering::Acquire);
        drop(owner);
        if should_wake {
            let _ = sender.try_send(ControlMessage::CoreCompletionPublished);
        }
    }

    fn publish(&self, identity: OwnerWorkIdentity) {
        let should_wake = {
            let mut state = self
                .identities
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if !state.registered.contains(&identity) {
                false
            } else {
                state.ready.insert(identity) && !self.pending.swap(true, Ordering::AcqRel)
            }
        };
        if should_wake {
            self.notify_owner();
        }
    }

    fn notify_owner(&self) {
        let terminal = self
            .terminal_owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some(terminal) = terminal {
            terminal.unpark();
            return;
        }
        let sender = self
            .owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some(sender) = sender {
            let _ = sender.try_send(ControlMessage::CoreCompletionPublished);
        }
    }

    fn bind_terminal_owner(&self) {
        *self
            .terminal_owner
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(thread::current());
        if self.pending.load(Ordering::Acquire) {
            self.notify_owner();
        }
    }

    fn take_terminal_identity(&self, waiter_id: WaiterId) -> Option<OwnerWorkIdentity> {
        let mut state = self
            .identities
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let identity = state
            .ready
            .range(
                OwnerWorkIdentity {
                    waiter_id,
                    phase: 0,
                }..=OwnerWorkIdentity {
                    waiter_id,
                    phase: u64::MAX,
                },
            )
            .next()
            .copied()?;
        state.ready.remove(&identity);
        state.registered.remove(&identity);
        Some(identity)
    }

    fn take(&self) -> bool {
        self.pending.swap(false, Ordering::AcqRel)
    }

    fn take_identities(&self, limit: usize) -> Vec<OwnerWorkIdentity> {
        let mut state = self
            .identities
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let identities = state.ready.iter().copied().take(limit).collect::<Vec<_>>();
        for identity in &identities {
            state.ready.remove(identity);
            state.registered.remove(identity);
        }
        let has_remaining = !state.ready.is_empty();
        let should_wake = if has_remaining {
            !self.pending.swap(true, Ordering::AcqRel)
        } else {
            self.pending.store(false, Ordering::Release);
            false
        };
        drop(state);
        if should_wake {
            self.notify_owner();
        }
        identities
    }

    fn restore_identities(&self, identities: &[OwnerWorkIdentity]) {
        if identities.is_empty() {
            return;
        }
        let should_wake = {
            let mut state = self
                .identities
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            debug_assert!(
                state.registered.len()
                    + identities
                        .iter()
                        .filter(|identity| !state.registered.contains(identity))
                        .count()
                    <= CORE_OWNER_COMPLETION_CAPACITY,
                "restore the taken batch before reusing registration capacity"
            );
            for identity in identities {
                state.registered.insert(*identity);
                state.ready.insert(*identity);
            }
            !self.pending.swap(true, Ordering::AcqRel)
        };
        if should_wake {
            self.notify_owner();
        }
    }

    #[cfg(test)]
    fn live_identity_counts(&self) -> (usize, usize, usize) {
        let state = self
            .identities
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        (
            state.registered.len(),
            state.ready.len(),
            state.next_phase.len(),
        )
    }
}

/// Outcome of one non-blocking [`CoreTicket::poll`].
#[derive(Debug)]
pub enum CoreTicketPoll<T> {
    /// The data-plane thread has not published the result yet.
    Pending,
    /// The result.
    Ready(T),
    /// The data-plane thread stopped before it ran the operation.
    Lost,
    /// Admission refused the operation because the bounded request queue was
    /// full. Nothing was queued; the caller decides whether to retry later.
    Refused,
}

/// Why a blocking [`CoreTicket::wait`] returned without a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreTicketError {
    /// The bound elapsed first.
    Timeout,
    /// The data-plane thread stopped before it ran the operation.
    DriverStopped,
    /// The bounded request queue was full; the operation was never queued.
    Overloaded,
}

impl std::fmt::Display for CoreTicketError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Timeout => "core operation wait timed out",
            Self::DriverStopped => "core data-plane driver stopped",
            Self::Overloaded => "core request queue is full",
        })
    }
}

impl std::error::Error for CoreTicketError {}

/// Result slot for one host operation submitted to the Core owner thread.
///
/// The Hub owner thread reads it with [`Self::poll`] from its own turn. Only
/// threads that do not serve the owner loop, such as the in-process CLI, may
/// block on [`Self::wait`].
#[derive(Debug)]
pub struct CoreTicket<T> {
    slot: CoreTicketSlot<T>,
}

/// A charged ticket permits only nonblocking reads.
#[derive(Debug)]
pub(crate) struct ChargedCoreTicket<T> {
    ticket: CoreTicket<T>,
}

/// One owner row keeps its phase history until final retirement.
#[derive(Debug)]
pub(crate) struct CoreWaiterRetirement {
    wake: Arc<CoreCompletionWake>,
    waiter_id: WaiterId,
}

/// One local reply uses the existing owner completion collector.
/// The receipt that owns this publisher must not be cloned.
#[derive(Debug)]
#[allow(dead_code)] // The daemon spawn receipt will own this publisher.
pub(crate) struct CoreReplyPublisher<T>(CoreTicketPublisher<T>);

#[allow(dead_code)] // The daemon spawn receipt will publish its conversion result.
impl<T> CoreReplyPublisher<T> {
    pub(crate) fn publish(self, value: T) {
        self.0.publish(value);
    }
}

impl CoreWaiterRetirement {
    #[allow(dead_code)] // The daemon spawn continuation will register this receipt.
    fn local_reply<T>(
        &self,
        charge: crate::lua_memory::LuaCallbackCharge,
    ) -> Result<(ChargedCoreTicket<T>, CoreReplyPublisher<T>), crate::lua_memory::LuaCallbackCharge>
    {
        let Some(bytes) = retained_reply_bytes::<T>() else {
            return Err(charge);
        };
        if charge.bytes() < bytes {
            return Err(charge);
        }
        let Some(identities) = self.wake.register_phases(self.waiter_id, 1) else {
            return Err(charge);
        };
        let identity = identities[0];
        let lease = crate::lua_memory::LuaCallbackStorageLease::new(charge);
        let (ticket, publisher) =
            CoreTicket::channel_with_lease(identity, Arc::clone(&self.wake), true, Some(lease));
        Ok((ChargedCoreTicket { ticket }, CoreReplyPublisher(publisher)))
    }
}

pub(crate) struct CoreSubmission<T> {
    pub(crate) ticket: ChargedCoreTicket<T>,
    pub(crate) rejected: Option<CoreRejectedRequest>,
}

pub(crate) struct CoreSubmissionStorage {
    pub(crate) request: crate::lua_memory::LuaCallbackCharge,
    pub(crate) reply: crate::lua_memory::LuaCallbackCharge,
}

pub(crate) fn retained_request_bytes<T, F>() -> usize {
    std::mem::size_of::<(F, CoreTicketPublisher<T>)>()
}

pub(crate) fn retained_reply_bytes<T>() -> Option<usize> {
    crate::lua_memory::layout::single_reply_bytes::<CoreTicketResult<T>>(false)
}

pub(crate) struct CoreRejectedRequest {
    #[allow(dead_code)] // rejected request payload retained until the refusal drops
    request: CoreRequest,
    pub(crate) reason: CoreRefusal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoreRefusal {
    Registration,
    Abandoned,
    Full,
    Stopped,
}

impl Drop for CoreWaiterRetirement {
    fn drop(&mut self) {
        self.wake.retire_waiter(self.waiter_id);
    }
}

impl<T> ChargedCoreTicket<T> {
    pub(crate) fn poll(&mut self) -> CoreTicketPoll<T> {
        self.ticket.poll()
    }
}

#[derive(Debug)]
enum CoreTicketSlot<T> {
    /// The operation was queued; its keyed result arrives on this channel.
    Queued {
        identity: OwnerWorkIdentity,
        receiver: Receiver<CoreTicketResult<T>>,
        owner_wake: Option<Arc<CoreCompletionWake>>,
        #[allow(dead_code)] // callback storage lease held until the queued ticket drops
        storage_lease: Option<crate::lua_memory::LuaCallbackStorageLease>,
    },
    /// Admission refused the operation; there is nothing to wait for.
    Refused,
}

#[derive(Debug)]
struct CoreTicketResult<T> {
    identity: OwnerWorkIdentity,
    value: T,
}

#[derive(Debug)]
struct CoreTicketPublisher<T> {
    identity: OwnerWorkIdentity,
    sender: Option<SyncSender<CoreTicketResult<T>>>,
    wake: Arc<CoreCompletionWake>,
    notify_owner: bool,
    #[allow(dead_code)] // callback storage lease held until the publisher drops
    storage_lease: Option<crate::lua_memory::LuaCallbackStorageLease>,
}

impl<T> CoreTicketPublisher<T> {
    fn publish(self, value: T) {
        let result = CoreTicketResult {
            identity: self.identity,
            value,
        };
        if self
            .sender
            .as_ref()
            .expect("live publisher owns its sender")
            .try_send(result)
            .is_ok()
        {
            if self.notify_owner {
                // The result is readable before its identity. The identity is
                // readable before the independent bit and doorbell publish.
                self.wake.publish(self.identity);
            }
        }
    }
}

impl<T> Drop for CoreTicketPublisher<T> {
    fn drop(&mut self) {
        drop(self.sender.take());
        if self.notify_owner {
            // Close the channel before waking its waiter. If no result exists,
            // the same completion collector makes the lost ticket observable.
            self.wake.publish(self.identity);
        }
    }
}

#[derive(Debug)]
struct CoreResultPublisher(CoreTicketPublisher<CoreCompletion>);

impl CoreResultPublisher {
    fn publish(self, completion: CoreCompletion) {
        self.0.publish(completion);
    }
}

/// Both keyed phases of one long-running Core operation.
#[derive(Debug)]
pub(crate) struct CoreOperationTicket {
    begin: CoreTicket<Result<PendingOperationId, CoreDaemonError>>,
    completion: CoreTicket<CoreCompletion>,
}

impl CoreOperationTicket {
    pub(crate) fn into_parts(
        self,
    ) -> (
        CoreTicket<Result<PendingOperationId, CoreDaemonError>>,
        CoreTicket<CoreCompletion>,
    ) {
        (self.begin, self.completion)
    }
}

impl<T> CoreTicket<T> {
    /// Collect only this ticket's ready phase during terminal progression.
    /// A ticket without an owner wake has no registration to collect.
    pub(crate) fn collect_ready_phase(&self) {
        if let CoreTicketSlot::Queued {
            identity,
            owner_wake: Some(wake),
            ..
        } = &self.slot
        {
            wake.collect_ready(*identity);
        }
    }

    fn channel(
        identity: OwnerWorkIdentity,
        wake: Arc<CoreCompletionWake>,
        notify_owner: bool,
    ) -> (Self, CoreTicketPublisher<T>) {
        Self::channel_with_lease(identity, wake, notify_owner, None)
    }

    fn channel_with_lease(
        identity: OwnerWorkIdentity,
        wake: Arc<CoreCompletionWake>,
        notify_owner: bool,
        storage_lease: Option<crate::lua_memory::LuaCallbackStorageLease>,
    ) -> (Self, CoreTicketPublisher<T>) {
        let (sender, receiver) = mpsc::sync_channel(1);
        (
            Self {
                slot: CoreTicketSlot::Queued {
                    identity,
                    receiver,
                    owner_wake: notify_owner.then(|| Arc::clone(&wake)),
                    storage_lease: storage_lease.clone(),
                },
            },
            CoreTicketPublisher {
                identity,
                sender: Some(sender),
                wake,
                notify_owner,
                storage_lease,
            },
        )
    }

    fn queued(identity: OwnerWorkIdentity, receiver: Receiver<CoreTicketResult<T>>) -> Self {
        Self {
            slot: CoreTicketSlot::Queued {
                identity,
                receiver,
                owner_wake: None,
                storage_lease: None,
            },
        }
    }

    fn lost(identity: OwnerWorkIdentity) -> Self {
        let (_sender, receiver) = mpsc::sync_channel(1);
        Self::queued(identity, receiver)
    }

    fn refused() -> Self {
        Self {
            slot: CoreTicketSlot::Refused,
        }
    }

    /// A ticket that already holds its answer. Test seams install a Core
    /// result with it so an owner phase applies exactly that result.
    #[cfg(test)]
    pub(crate) fn resolved(value: T) -> Self {
        let identity = OwnerWorkIdentity::first(crate::owner_identity::WaiterId(1));
        let (sender, receiver) = mpsc::sync_channel(1);
        sender
            .send(CoreTicketResult { identity, value })
            .expect("a resolved ticket holds exactly one value");
        Self::queued(identity, receiver)
    }

    /// Non-blocking read; owner-thread use.
    pub(crate) fn poll(&mut self) -> CoreTicketPoll<T> {
        match &self.slot {
            CoreTicketSlot::Refused => CoreTicketPoll::Refused,
            CoreTicketSlot::Queued {
                identity,
                receiver,
                owner_wake,
                ..
            } => {
                // The result can arrive before its completion identity. The
                // owner must collect that identity before it starts a new phase.
                // Owner registration precedes request submission. An absent
                // identity therefore means collection or explicit retirement.
                if owner_wake
                    .as_ref()
                    .is_some_and(|wake| wake.awaits_collection(*identity))
                {
                    return CoreTicketPoll::Pending;
                }
                match receiver.try_recv() {
                    Ok(result) if result.identity == *identity => {
                        CoreTicketPoll::Ready(result.value)
                    }
                    // A stale phase cannot make this ticket ready. Dropping the
                    // rejected result also releases every value-owned charge.
                    Ok(_) => CoreTicketPoll::Pending,
                    Err(TryRecvError::Empty) => CoreTicketPoll::Pending,
                    Err(TryRecvError::Disconnected) => CoreTicketPoll::Lost,
                }
            }
        }
    }

    /// Bounded blocking read for threads that do not serve the owner loop.
    pub fn wait(self, timeout: Duration) -> Result<T, CoreTicketError> {
        match self.slot {
            CoreTicketSlot::Refused => Err(CoreTicketError::Overloaded),
            CoreTicketSlot::Queued {
                identity, receiver, ..
            } => {
                let deadline = Instant::now() + timeout;
                loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    match receiver.recv_timeout(remaining) {
                        Ok(result) if result.identity == identity => return Ok(result.value),
                        // A stale phase is dropped with its value-owned charge.
                        Ok(_) if !remaining.is_zero() => {}
                        Ok(_) | Err(RecvTimeoutError::Timeout) => {
                            return Err(CoreTicketError::Timeout);
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            return Err(CoreTicketError::DriverStopped);
                        }
                    }
                }
            }
        }
    }
}

/// Outcome of one nonblocking request admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoreAdmission {
    /// The request is queued for the next data-plane turn.
    Queued,
    /// The bounded queue was full; the request was dropped unqueued.
    Refused,
    /// The driver stopped accepting requests.
    Stopped,
}

/// Admit one request without waiting. The queue bound is the only
/// backpressure: a full queue refuses, it never blocks the submitter.
fn admit_request(
    requests: &SyncSender<CoreRequest>,
    accepting: &AtomicBool,
    request: CoreRequest,
) -> CoreAdmission {
    if !accepting.load(Ordering::Acquire) {
        return CoreAdmission::Stopped;
    }
    match requests.try_send(request) {
        Ok(()) => CoreAdmission::Queued,
        Err(TrySendError::Full(_)) => CoreAdmission::Refused,
        Err(TrySendError::Disconnected(_)) => CoreAdmission::Stopped,
    }
}

/// Hub-owned bounded operation bridge to the single Core owner thread.
#[derive(Clone)]
pub(crate) struct CoreDaemonHandle {
    requests: SyncSender<CoreRequest>,
    control: WakePumpControl,
    accepting: Arc<AtomicBool>,
    admission: Arc<Mutex<()>>,
    request_pending: Arc<AtomicBool>,
    owner_waiting: Arc<AtomicBool>,
    waiter_ids: Arc<WaiterIdSource>,
    completion_wake: Arc<CoreCompletionWake>,
    #[cfg(test)]
    refuse_next_owner_begins: Arc<AtomicUsize>,
    #[cfg(test)]
    refuse_registered_owner_begins: Arc<AtomicUsize>,
    #[cfg(test)]
    lose_next_owner_begins: Arc<AtomicUsize>,
    #[cfg(test)]
    lose_reserve_completion_for: Arc<Mutex<Option<WaiterId>>>,
    #[cfg(test)]
    release_session_reservation_begins: Arc<AtomicUsize>,
}

impl CoreDaemonHandle {
    /// Register a local reply after the owner collects its previous Core phases.
    /// The charge funds channel storage. Shared registration storage remains separate.
    #[allow(dead_code)] // The daemon spawn continuation will register this receipt.
    pub(crate) fn local_reply_for_owner<T>(
        &self,
        retirement: &CoreWaiterRetirement,
        charge: crate::lua_memory::LuaCallbackCharge,
    ) -> Result<(ChargedCoreTicket<T>, CoreReplyPublisher<T>), crate::lua_memory::LuaCallbackCharge>
    {
        if !Arc::ptr_eq(&self.completion_wake, &retirement.wake) {
            return Err(charge);
        }
        retirement.local_reply(charge)
    }

    #[cfg(test)]
    pub(crate) fn test_lose_reserve_completion_for(&self, waiter_id: WaiterId) {
        let mut selected = self.lose_reserve_completion_for.lock().unwrap();
        assert!(selected.replace(waiter_id).is_none());
    }

    #[cfg(test)]
    pub(crate) fn test_refuse_registered_owner_begins(&self, count: usize) {
        self.refuse_registered_owner_begins
            .store(count, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn test_refuse_next_owner_begins(&self, count: usize) {
        self.refuse_next_owner_begins
            .store(count, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn test_refuse_next_owner_begins_remaining(&self) -> usize {
        self.refuse_next_owner_begins.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn test_lose_next_owner_begins(&self, count: usize) {
        self.lose_next_owner_begins.store(count, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn test_release_session_reservation_begins(&self) -> usize {
        self.release_session_reservation_begins
            .load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn take_forced_owner_begin(&self) -> Option<CoreOperationTicket> {
        if self
            .lose_next_owner_begins
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
        {
            return Some(CoreOperationTicket {
                begin: CoreTicket::lost(OwnerWorkIdentity::first(WaiterId(0))),
                completion: CoreTicket::lost(OwnerWorkIdentity::first(WaiterId(0))),
            });
        }
        if self
            .refuse_next_owner_begins
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
        {
            return Some(CoreOperationTicket {
                begin: CoreTicket::refused(),
                completion: CoreTicket::refused(),
            });
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn test_retains_waiter(&self, waiter_id: WaiterId) -> bool {
        self.completion_wake
            .identities
            .lock()
            .unwrap()
            .next_phase
            .contains_key(&waiter_id)
    }
    pub(crate) fn bind_terminal_owner(&self) {
        self.completion_wake.bind_terminal_owner();
    }

    pub(crate) fn take_terminal_completion(
        &self,
        waiter_id: WaiterId,
    ) -> Option<OwnerWorkIdentity> {
        self.completion_wake.take_terminal_identity(waiter_id)
    }
    pub(crate) fn submit_retained_for_owner<T, F>(
        &self,
        retirement: &CoreWaiterRetirement,
        operation: F,
        claim: impl FnOnce() -> bool,
        storage: Option<CoreSubmissionStorage>,
    ) -> CoreSubmission<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut CoreDaemon) -> T + Send + 'static,
    {
        assert!(Arc::ptr_eq(&self.completion_wake, &retirement.wake));
        let identity = self
            .completion_wake
            .register_phases(retirement.waiter_id, 1)
            .and_then(|identities| identities.into_iter().next());
        self.submit_retained_identity(identity, true, operation, claim, storage)
    }

    pub(crate) fn submit_retained<T, F>(
        &self,
        operation: F,
        claim: impl FnOnce() -> bool,
        storage: Option<CoreSubmissionStorage>,
    ) -> CoreSubmission<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut CoreDaemon) -> T + Send + 'static,
    {
        let identity = self.waiter_ids.next().map(OwnerWorkIdentity::first);
        self.submit_retained_identity(identity, false, operation, claim, storage)
    }

    fn submit_retained_identity<T, F>(
        &self,
        identity: Option<OwnerWorkIdentity>,
        notify_owner: bool,
        operation: F,
        claim: impl FnOnce() -> bool,
        storage: Option<CoreSubmissionStorage>,
    ) -> CoreSubmission<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut CoreDaemon) -> T + Send + 'static,
    {
        let (request_charge, reply_charge) = match storage {
            Some(storage) => (Some(storage.request), Some(storage.reply)),
            None => (None, None),
        };
        let Some(identity) = identity else {
            let operation = move |daemon: &mut CoreDaemon, _: &mut PendingCoreOperations| {
                drop(operation(daemon));
            };
            let admitted = request_charge
                .as_ref()
                .map(crate::lua_memory::LuaCallbackCharge::bytes)
                .unwrap_or_else(retained_request_bytes::<T, F>);
            assert!(std::mem::size_of_val(&operation) <= admitted);
            let request = match request_charge {
                Some(charge) => CoreRequest::charged(operation, charge),
                None => CoreRequest::new(operation),
            };
            return CoreSubmission {
                ticket: ChargedCoreTicket {
                    ticket: CoreTicket::refused(),
                },
                rejected: Some(CoreRejectedRequest {
                    request,
                    reason: CoreRefusal::Registration,
                }),
            };
        };
        let lease = reply_charge.map(crate::lua_memory::LuaCallbackStorageLease::new);
        let (ticket, publisher) = CoreTicket::channel_with_lease(
            identity,
            Arc::clone(&self.completion_wake),
            notify_owner,
            lease,
        );
        let operation = move |daemon: &mut CoreDaemon, _: &mut PendingCoreOperations| {
            publisher.publish(operation(daemon));
        };
        let admitted = request_charge
            .as_ref()
            .map(crate::lua_memory::LuaCallbackCharge::bytes)
            .unwrap_or_else(retained_request_bytes::<T, F>);
        assert!(std::mem::size_of_val(&operation) <= admitted);
        let request = match request_charge {
            Some(charge) => CoreRequest::charged(operation, charge),
            None => CoreRequest::new(operation),
        };
        let admission = self.admission.lock().expect("Core request admission mutex");
        let rejected = if !self.accepting.load(Ordering::Acquire) {
            Some(CoreRejectedRequest {
                request,
                reason: CoreRefusal::Stopped,
            })
        } else if !claim() {
            Some(CoreRejectedRequest {
                request,
                reason: CoreRefusal::Abandoned,
            })
        } else {
            match self.requests.try_send(request) {
                Ok(()) => None,
                Err(TrySendError::Full(request)) => Some(CoreRejectedRequest {
                    request,
                    reason: CoreRefusal::Full,
                }),
                Err(TrySendError::Disconnected(request)) => Some(CoreRejectedRequest {
                    request,
                    reason: CoreRefusal::Stopped,
                }),
            }
        };
        if rejected.is_some() {
            if notify_owner {
                self.completion_wake.retire(identity);
            }
        } else {
            self.request_pending.store(true, Ordering::Release);
            if self.owner_waiting.swap(false, Ordering::AcqRel) {
                self.control.interrupt();
            }
        }
        drop(admission);
        CoreSubmission {
            ticket: ChargedCoreTicket { ticket },
            rejected,
        }
    }

    pub(crate) fn waiter_retirement(&self, waiter_id: WaiterId) -> CoreWaiterRetirement {
        CoreWaiterRetirement {
            wake: Arc::clone(&self.completion_wake),
            waiter_id,
        }
    }

    /// Queue one host operation for the Core owner thread and return its
    /// result slot. Returns at once; the operation runs on the next
    /// data-plane turn. A full queue yields a ticket that polls
    /// [`CoreTicketPoll::Refused`] and nothing is queued: the admission mutex
    /// is held only across the accepting check and one `try_send`, so the
    /// owner thread and `stop_and_join` never wait on a saturated queue.
    pub(crate) fn submit<T, F>(&self, operation: F) -> CoreTicket<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut CoreDaemon) -> T + Send + 'static,
    {
        let Some(waiter_id) = self.waiter_ids.next() else {
            return CoreTicket::refused();
        };
        let identity = OwnerWorkIdentity::first(waiter_id);
        let (ticket, publisher) =
            CoreTicket::channel(identity, Arc::clone(&self.completion_wake), false);
        let request = CoreRequest::new(move |daemon, _| {
            publisher.publish(operation(daemon));
        });
        let _admission = self.admission.lock().expect("Core request admission mutex");
        match admit_request(&self.requests, &self.accepting, request) {
            CoreAdmission::Queued => {}
            CoreAdmission::Refused => {
                return CoreTicket::refused();
            }
            CoreAdmission::Stopped => {
                return CoreTicket::lost(identity);
            }
        }
        self.request_pending.store(true, Ordering::Release);
        if self.owner_waiting.swap(false, Ordering::AcqRel) {
            self.control.interrupt();
        }
        drop(_admission);
        ticket
    }

    /// Queue one Core request whose result must ready an owner waiter.
    pub(crate) fn submit_for_owner<T, F>(&self, waiter_id: WaiterId, operation: F) -> CoreTicket<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut CoreDaemon) -> T + Send + 'static,
    {
        let Some(identity) = self
            .completion_wake
            .register_phases(waiter_id, 1)
            .and_then(|identities| identities.into_iter().next())
        else {
            return CoreTicket::refused();
        };
        let (ticket, publisher) =
            CoreTicket::channel(identity, Arc::clone(&self.completion_wake), true);
        let request = CoreRequest::new(move |daemon, _| publisher.publish(operation(daemon)));
        let _admission = self.admission.lock().expect("Core request admission mutex");
        match admit_request(&self.requests, &self.accepting, request) {
            CoreAdmission::Queued => {}
            CoreAdmission::Refused => {
                self.completion_wake.retire(identity);
                return CoreTicket::refused();
            }
            CoreAdmission::Stopped => {
                self.completion_wake.retire(identity);
                return CoreTicket::lost(identity);
            }
        }
        self.request_pending.store(true, Ordering::Release);
        if self.owner_waiting.swap(false, Ordering::AcqRel) {
            self.control.interrupt();
        }
        drop(_admission);
        ticket
    }

    /// Start one Core operation with registered begin and completion phases.
    pub(crate) fn begin(&self, operation: CoreOperation) -> CoreOperationTicket {
        #[cfg(test)]
        if matches!(operation, CoreOperation::ReleaseSessionReservation(_)) {
            self.release_session_reservation_begins
                .fetch_add(1, Ordering::AcqRel);
        }
        let Some(waiter_id) = self.waiter_ids.next() else {
            return CoreOperationTicket {
                begin: CoreTicket::refused(),
                completion: CoreTicket::refused(),
            };
        };
        let begin_identity = OwnerWorkIdentity::first(waiter_id);
        let completion_identity = begin_identity
            .next_phase()
            .expect("the first Core phase always has a successor");
        // Both phases are registered before the request can start in Core.
        let (begin, begin_publisher) =
            CoreTicket::channel(begin_identity, Arc::clone(&self.completion_wake), false);
        let (completion, completion_publisher) = CoreTicket::channel(
            completion_identity,
            Arc::clone(&self.completion_wake),
            false,
        );
        let request = CoreRequest::new(move |daemon, pending| {
            let result = daemon.begin(operation);
            if let Ok(id) = result {
                pending.insert(id, CoreResultPublisher(completion_publisher));
            }
            begin_publisher.publish(result);
        });
        let _admission = self.admission.lock().expect("Core request admission mutex");
        match admit_request(&self.requests, &self.accepting, request) {
            CoreAdmission::Queued => {}
            CoreAdmission::Refused => {
                return CoreOperationTicket {
                    begin: CoreTicket::refused(),
                    completion,
                };
            }
            CoreAdmission::Stopped => {
                return CoreOperationTicket {
                    begin: CoreTicket::lost(begin_identity),
                    completion,
                };
            }
        }
        self.request_pending.store(true, Ordering::Release);
        if self.owner_waiting.swap(false, Ordering::AcqRel) {
            self.control.interrupt();
        }
        drop(_admission);
        CoreOperationTicket { begin, completion }
    }

    /// Start one two-phase Core operation for an admitted owner waiter.
    pub(crate) fn begin_for_owner(
        &self,
        waiter_id: WaiterId,
        operation: CoreOperation,
    ) -> CoreOperationTicket {
        #[cfg(test)]
        if let Some(forced) = self.take_forced_owner_begin() {
            return forced;
        }
        #[cfg(test)]
        if matches!(operation, CoreOperation::ReleaseSessionReservation(_)) {
            self.release_session_reservation_begins
                .fetch_add(1, Ordering::AcqRel);
        }
        let Some(identities) = self.completion_wake.register_phases(waiter_id, 2) else {
            return CoreOperationTicket {
                begin: CoreTicket::refused(),
                completion: CoreTicket::refused(),
            };
        };
        let [begin_identity, completion_identity] = identities.as_slice() else {
            unreachable!("two Core phases were registered");
        };
        let begin_identity = *begin_identity;
        let completion_identity = *completion_identity;
        let (begin, begin_publisher) =
            CoreTicket::channel(begin_identity, Arc::clone(&self.completion_wake), true);
        let (completion, completion_publisher) =
            CoreTicket::channel(completion_identity, Arc::clone(&self.completion_wake), true);
        let completion_wake = Arc::clone(&self.completion_wake);
        #[cfg(test)]
        let lose_reserve_completion = {
            let mut selected = self.lose_reserve_completion_for.lock().unwrap();
            if matches!(&operation, CoreOperation::ReserveSession(_))
                && *selected == Some(waiter_id)
            {
                selected.take();
                true
            } else {
                false
            }
        };
        let request = CoreRequest::new(move |daemon, pending| {
            let result = daemon.begin(operation);
            #[cfg(test)]
            if result.is_ok() && lose_reserve_completion {
                // Core keeps executing. Only this accepted reply is lost.
                drop(completion_publisher);
                begin_publisher.publish(result);
                return;
            }
            if let Ok(id) = result {
                pending.insert(id, CoreResultPublisher(completion_publisher));
            } else {
                completion_wake.retire(completion_identity);
            }
            begin_publisher.publish(result);
        });
        let _admission = self.admission.lock().expect("Core request admission mutex");
        #[cfg(test)]
        let injected_refusal = self
            .refuse_registered_owner_begins
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok();
        #[cfg(not(test))]
        let injected_refusal = false;
        let admission = if injected_refusal {
            drop(request);
            CoreAdmission::Refused
        } else {
            admit_request(&self.requests, &self.accepting, request)
        };
        match admission {
            CoreAdmission::Queued => {}
            CoreAdmission::Refused => {
                self.completion_wake.retire(begin_identity);
                self.completion_wake.retire(completion_identity);
                return CoreOperationTicket {
                    begin: CoreTicket::refused(),
                    completion: CoreTicket::refused(),
                };
            }
            CoreAdmission::Stopped => {
                self.completion_wake.retire(begin_identity);
                self.completion_wake.retire(completion_identity);
                return CoreOperationTicket {
                    begin: CoreTicket::lost(begin_identity),
                    completion: CoreTicket::lost(completion_identity),
                };
            }
        }
        self.request_pending.store(true, Ordering::Release);
        if self.owner_waiting.swap(false, Ordering::AcqRel) {
            self.control.interrupt();
        }
        drop(_admission);
        CoreOperationTicket { begin, completion }
    }

    pub(crate) fn waiter_ids(&self) -> &WaiterIdSource {
        &self.waiter_ids
    }

    pub(crate) fn take_completion_notification(&self) -> bool {
        self.completion_wake.take()
    }

    pub(crate) fn take_owner_completion_identities(&self, limit: usize) -> Vec<OwnerWorkIdentity> {
        self.completion_wake.take_identities(limit)
    }

    pub(crate) fn restore_owner_completion_identities(&self, identities: &[OwnerWorkIdentity]) {
        self.completion_wake.restore_identities(identities);
    }

    pub(crate) fn retire_owner_completion(&self, identity: OwnerWorkIdentity) -> bool {
        self.completion_wake.retire(identity)
    }

    pub(crate) fn retire_owner_waiter(&self, waiter_id: WaiterId) -> usize {
        self.completion_wake.retire_waiter(waiter_id)
    }
}

impl DataPlaneDriver {
    pub(crate) fn start(
        core_config: CoreDaemonConfig,
        close_work: CloseWorkSource,
    ) -> (Self, CoreDaemonHandle) {
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let (request_tx, request_rx) = mpsc::sync_channel(CORE_REQUEST_CAPACITY);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let accepting = Arc::new(AtomicBool::new(true));
        let admission = Arc::new(Mutex::new(()));
        let request_pending = Arc::new(AtomicBool::new(false));
        let owner_waiting = Arc::new(AtomicBool::new(false));
        let waiter_ids = Arc::new(WaiterIdSource::default());
        let completion_wake = Arc::new(CoreCompletionWake::new());
        let thread_request_pending = Arc::clone(&request_pending);
        let thread_owner_waiting = Arc::clone(&owner_waiting);
        let stop_action = Arc::new(AtomicU8::new(STOP_ACTION_SHUTDOWN));
        let thread_stop_action = Arc::clone(&stop_action);
        let owner_wake = Arc::new(Mutex::new(None));
        let thread_owner_wake = Arc::clone(&owner_wake);
        let progress_latch = Arc::new(DataPlaneProgressLatch::default());
        let thread_progress_latch = Arc::clone(&progress_latch);
        let thread = std::thread::Builder::new()
            .name("botster-hub-data-plane".to_string())
            .spawn(move || {
                let mut daemon = CoreDaemon::new(core_config);
                let control = daemon.wake_pump_control();
                ready_tx.send(control).expect("publish Core pump control");
                run_loop(
                    &mut daemon,
                    request_rx,
                    close_work,
                    thread_owner_wake,
                    thread_progress_latch,
                    thread_request_pending,
                    thread_owner_waiting,
                );
                if thread_stop_action.load(Ordering::Acquire) == STOP_ACTION_RELEASE_FOR_RESTART {
                    daemon.release_for_restart();
                } else {
                    let _ = daemon.shutdown(None, current_unix_seconds());
                }
                let _ = done_tx.send(());
            })
            .expect("start botster-hub-data-plane");
        let core = CoreDaemonHandle {
            requests: request_tx,
            control: ready_rx.recv().expect("receive Core pump control"),
            accepting,
            admission,
            request_pending,
            owner_waiting,
            waiter_ids,
            completion_wake,
            #[cfg(test)]
            refuse_next_owner_begins: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            refuse_registered_owner_begins: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            lose_next_owner_begins: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            lose_reserve_completion_for: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            release_session_reservation_begins: Arc::new(AtomicUsize::new(0)),
        };
        let driver = Self {
            core: core.clone(),
            stop_action,
            done: done_rx,
            thread: Some(thread),
            owner_wake,
            progress_latch,
        };
        (driver, core)
    }

    pub(crate) fn bind_owner_wake(&self, sender: ControlSender) {
        self.core.completion_wake.bind(sender.clone());
        if let Ok(mut slot) = self.owner_wake.lock() {
            *slot = Some(sender);
        }
    }

    pub(crate) fn take_progress(&self) -> DataPlaneProgress {
        self.progress_latch.take()
    }

    pub(crate) fn progress_pending(&self) -> bool {
        self.progress_latch.bits.load(Ordering::Acquire) != 0
    }

    pub(crate) fn stop_and_join(&mut self, release_for_restart: bool) -> Result<(), &'static str> {
        let _admission = self
            .core
            .admission
            .lock()
            .expect("Core request admission mutex");
        self.core.accepting.store(false, Ordering::Release);
        self.stop_action.store(
            if release_for_restart {
                STOP_ACTION_RELEASE_FOR_RESTART
            } else {
                STOP_ACTION_SHUTDOWN
            },
            Ordering::Release,
        );
        self.core.control.request_stop();
        if let Some(thread) = self.thread.as_ref() {
            thread.thread().unpark();
        }
        drop(_admission);
        match self.done.recv_timeout(DATA_PLANE_STOP_BOUND) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                if let Some(thread) = self.thread.take() {
                    let _ = thread.join();
                }
                Ok(())
            }
            Err(RecvTimeoutError::Timeout) => Err(DATA_PLANE_DRIVER_STOP_TIMEOUT),
        }
    }
}

impl Drop for DataPlaneDriver {
    fn drop(&mut self) {
        if self.thread.is_some() {
            match self.stop_and_join(false) {
                Ok(()) => {}
                Err(_) => std::process::abort(),
            }
        }
    }
}

fn run_loop(
    core_daemon: &mut CoreDaemon,
    requests: Receiver<CoreRequest>,
    close_work: CloseWorkSource,
    owner_wake: Arc<Mutex<Option<ControlSender>>>,
    progress_latch: Arc<DataPlaneProgressLatch>,
    request_pending: Arc<AtomicBool>,
    owner_waiting: Arc<AtomicBool>,
) {
    let mut pending_operations = PendingCoreOperations::new();
    loop {
        owner_waiting.store(true, Ordering::Release);
        let wait_timeout = if request_pending.swap(false, Ordering::AcqRel) {
            owner_waiting.store(false, Ordering::Release);
            Duration::ZERO
        } else {
            DATA_PLANE_WATCHDOG
        };
        let waited = core_daemon.wait_pump(wait_timeout);
        owner_waiting.store(false, Ordering::Release);
        let waited = if matches!(waited, WakePumpWait::Interrupted) {
            core_daemon.wait_pump(Duration::ZERO)
        } else {
            waited
        };
        let batch = match waited {
            WakePumpWait::Wakes(batch) => Some(batch),
            WakePumpWait::Interrupted => None,
            WakePumpWait::Stopped => break,
            _ => None,
        };
        let now_seconds = current_unix_seconds();
        let mut progress = false;
        let mut terminal_inventory_changed = false;
        if let Some(batch) = batch
            && let Ok(outcome) = core_daemon.pump_woken(&batch, now_seconds)
        {
            progress |= outcome.pumped_routes > 0 || !batch.ingress_sessions.is_empty();
            terminal_inventory_changed |= outcome.terminal_inventory_changed;
        }
        run_core_requests(core_daemon, &requests, &mut pending_operations);
        publish_completions(core_daemon, &mut pending_operations);
        {
            let close_batch = close_work.take_batch(DATA_PLANE_MAX_CLOSE_KEYS);
            for state in close_batch {
                let lookup = core_daemon
                    .session_registry_state(&botster_core::SessionId(state.key.session_id.clone()));
                match session_close_event_decision(lookup) {
                    Some(emit) => {
                        let key = state.key.clone();
                        state.report_if_live(emit);
                        close_work.retire(&key.session_id, &key.subscription_id, key.generation);
                    }
                    None => close_work.requeue(state),
                }
            }
        }
        let journal_advanced = core_daemon.take_journal_advanced_wake();
        progress_latch.publish(
            DataPlaneProgress {
                progressed: progress,
                journal_advanced,
                terminal_inventory_changed,
            },
            &owner_wake,
        );
    }
    for request in requests.try_iter().take(CORE_REQUEST_CAPACITY) {
        request.run(core_daemon, &mut pending_operations);
    }
    publish_completions(core_daemon, &mut pending_operations);
}

fn run_core_requests(
    core_daemon: &mut CoreDaemon,
    requests: &Receiver<CoreRequest>,
    pending_operations: &mut PendingCoreOperations,
) {
    for request in requests.try_iter().take(CORE_REQUESTS_PER_TURN) {
        request.run(core_daemon, pending_operations);
    }
}

/// Publish each finished Core operation to its exact registered phase.
fn publish_completions(
    core_daemon: &mut CoreDaemon,
    pending_operations: &mut PendingCoreOperations,
) {
    for completion in core_daemon.take_completions() {
        if let Some(publisher) = pending_operations.remove(&completion.id()) {
            publisher.publish(completion);
        }
    }
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug)]
    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn identity(waiter_id: u64, phase: u64) -> OwnerWorkIdentity {
        OwnerWorkIdentity {
            waiter_id: crate::owner_identity::WaiterId(waiter_id),
            phase,
        }
    }

    fn owner_channel<T>(
        identity: OwnerWorkIdentity,
        wake: Arc<CoreCompletionWake>,
    ) -> (CoreTicket<T>, CoreTicketPublisher<T>) {
        assert_eq!(
            wake.register_phases(identity.waiter_id, 1),
            Some(vec![identity])
        );
        CoreTicket::channel(identity, wake, true)
    }

    fn noop_request() -> CoreRequest {
        CoreRequest::new(|_, _| {})
    }

    fn terminal_tracker(
        wake: &Arc<CoreCompletionWake>,
        phase_count: usize,
    ) -> (
        crate::runtime::CoreOperationTracker,
        CoreTicketPublisher<Result<PendingOperationId, CoreDaemonError>>,
        CoreTicketPublisher<CoreCompletion>,
    ) {
        let identities = wake.register_phases(WaiterId(95), phase_count).unwrap();
        assert_eq!(identities[0], identity(95, 1));
        assert_eq!(identities[1], identity(95, 2));
        let (begin, begin_publisher) = CoreTicket::channel(identities[0], Arc::clone(wake), true);
        let (completion, completion_publisher) =
            CoreTicket::channel(identities[1], Arc::clone(wake), true);
        (
            crate::runtime::CoreOperationTracker::new(CoreOperationTicket { begin, completion }),
            begin_publisher,
            completion_publisher,
        )
    }

    #[test]
    fn terminal_tracker_collects_both_ready_phases_in_one_poll() {
        let wake = Arc::new(CoreCompletionWake::new());
        let (mut tracker, begin, completion) = terminal_tracker(&wake, 2);
        let id = PendingOperationId(95);
        begin.publish(Ok(id));
        completion.publish(CoreCompletion::RemoveSession {
            id,
            result: Ok(true),
        });
        assert!(wake.take());
        assert!(matches!(
            tracker.poll_terminal(),
            CoreTicketPoll::Ready(Ok(CoreCompletion::RemoveSession { id: found, .. })) if found == id
        ));
        assert!(!wake.awaits_collection(identity(95, 1)));
        assert!(!wake.awaits_collection(identity(95, 2)));
        assert!(wake.take_identities(2).is_empty());
    }

    #[test]
    fn terminal_tracker_preserves_unready_phase_and_observes_later_wake() {
        let wake = Arc::new(CoreCompletionWake::new());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        wake.bind(sender);
        let (mut tracker, begin, completion) = terminal_tracker(&wake, 2);
        let id = PendingOperationId(96);
        begin.publish(Ok(id));
        assert!(receiver.try_recv().is_ok());
        assert!(wake.take());
        assert!(matches!(tracker.poll_terminal(), CoreTicketPoll::Pending));
        assert!(!wake.awaits_collection(identity(95, 1)));
        assert!(wake.awaits_collection(identity(95, 2)));
        assert_eq!(tracker.pending_id(), Some(id));
        assert!(matches!(tracker.poll_terminal(), CoreTicketPoll::Pending));
        assert!(wake.awaits_collection(identity(95, 2)));
        assert!(!wake.take());
        completion.publish(CoreCompletion::RemoveSession {
            id,
            result: Ok(true),
        });
        assert!(receiver.try_recv().is_ok());
        assert!(wake.take());
        assert!(matches!(
            tracker.poll_terminal(),
            CoreTicketPoll::Ready(Ok(_))
        ));
        assert!(!wake.awaits_collection(identity(95, 2)));
    }

    #[test]
    fn terminal_tracker_does_not_collect_an_unrelated_same_waiter_phase() {
        let wake = Arc::new(CoreCompletionWake::new());
        // This synthetic batch tests exact selection, not concurrent production operations.
        let (mut tracker, begin, completion) = terminal_tracker(&wake, 3);
        let (mut other, other_publisher) =
            CoreTicket::channel(identity(95, 3), Arc::clone(&wake), true);
        let id = PendingOperationId(97);
        begin.publish(Ok(id));
        completion.publish(CoreCompletion::RemoveSession {
            id,
            result: Ok(true),
        });
        other_publisher.publish(7_u8);
        assert!(matches!(
            tracker.poll_terminal(),
            CoreTicketPoll::Ready(Ok(_))
        ));
        assert!(wake.awaits_collection(identity(95, 3)));
        assert!(matches!(other.poll(), CoreTicketPoll::Pending));
        assert_eq!(wake.take_identities(1), vec![identity(95, 3)]);
        assert!(matches!(other.poll(), CoreTicketPoll::Ready(7)));
    }

    fn callback_account() -> Arc<crate::lua_memory::LuaMemoryAccount> {
        crate::lua_memory::LuaMemoryAccount::new(crate::lua_memory::LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 4096,
            total_callback_bytes: 8192,
        })
        .unwrap()
    }

    struct ChargedDropProbe(Arc<crate::lua_memory::LuaMemoryAccount>);

    impl Drop for ChargedDropProbe {
        fn drop(&mut self) {
            assert!(self.0.usage().1 > 0, "payload drops before its charge");
        }
    }

    #[test]
    fn charged_request_releases_its_closure_before_its_charge() {
        let memory = callback_account();
        let probe = ChargedDropProbe(Arc::clone(&memory));
        let operation = move |_: &mut CoreDaemon, _: &mut PendingCoreOperations| drop(probe);
        let expected = std::mem::size_of_val(&operation);
        let charge = memory.reserve_callback_bytes(expected).unwrap();
        let request = CoreRequest::charged(operation, charge);
        assert_eq!(memory.usage().1, expected);
        drop(request);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn charged_channel_keeps_its_charge_through_unread_result_destruction() {
        let memory = callback_account();
        let lease = crate::lua_memory::LuaCallbackStorageLease::new(
            memory.reserve_callback_bytes(1).unwrap(),
        );
        let wake = Arc::new(CoreCompletionWake::new());
        let (ticket, publisher) =
            CoreTicket::channel_with_lease(identity(1, 1), wake, false, Some(lease));
        publisher.publish(ChargedDropProbe(Arc::clone(&memory)));
        assert_eq!(memory.usage().1, 1);
        drop(ticket);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn charged_channel_keeps_its_charge_after_receiver_abandonment() {
        let memory = callback_account();
        let lease = crate::lua_memory::LuaCallbackStorageLease::new(
            memory.reserve_callback_bytes(1).unwrap(),
        );
        let wake = Arc::new(CoreCompletionWake::new());
        let (ticket, publisher) =
            CoreTicket::channel_with_lease(identity(1, 1), wake, false, Some(lease));
        drop(ticket);
        assert_eq!(memory.usage().1, 1);
        publisher.publish(ChargedDropProbe(Arc::clone(&memory)));
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn row_retirement_preserves_phases_until_the_last_operation_finishes() {
        let wake = Arc::new(CoreCompletionWake::new());
        let waiter_id = WaiterId(71);
        let retirement = CoreWaiterRetirement {
            wake: Arc::clone(&wake),
            waiter_id,
        };
        let first = wake.register_phases(waiter_id, 1).unwrap()[0];
        let (first_ticket, first_publisher) = CoreTicket::channel(first, Arc::clone(&wake), true);
        drop(first_ticket);
        assert!(wake.retire(first));
        let second = wake.register_phases(waiter_id, 1).unwrap()[0];
        assert_eq!(second.waiter_id, first.waiter_id);
        assert_eq!(second.phase, first.phase + 1);
        let (mut second_ticket, second_publisher) =
            CoreTicket::channel(second, Arc::clone(&wake), true);
        first_publisher.publish(1_u8);
        assert!(wake.take_identities(1).is_empty());
        assert!(matches!(second_ticket.poll(), CoreTicketPoll::Pending));
        second_publisher.publish(2_u8);
        assert_eq!(wake.take_identities(1), vec![second]);
        assert!(matches!(second_ticket.poll(), CoreTicketPoll::Ready(2)));
        drop(second_ticket);
        assert_eq!(wake.live_identity_counts(), (0, 0, 1));
        drop(retirement);
        assert_eq!(wake.live_identity_counts(), (0, 0, 0));
    }

    #[test]
    fn local_host_receipt_must_be_collected_before_same_waiter_core_begin() {
        let wake = Arc::new(CoreCompletionWake::new());
        let waiter_id = WaiterId(72);
        let retirement = CoreWaiterRetirement {
            wake: Arc::clone(&wake),
            waiter_id,
        };
        let bytes = retained_reply_bytes::<()>().unwrap();
        let memory = crate::lua_memory::LuaMemoryAccount::new(crate::lua_memory::LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: bytes,
            total_callback_bytes: bytes,
        })
        .unwrap();
        let charge = memory.reserve_callback_total(bytes).unwrap();
        let (mut receipt, publisher) = retirement.local_reply(charge).unwrap();
        assert!(wake.register_phases(waiter_id, 2).is_none());
        publisher.publish(());
        assert!(matches!(receipt.poll(), CoreTicketPoll::Pending));
        assert_eq!(wake.take_identities(1), vec![identity(72, 1)]);
        assert!(matches!(receipt.poll(), CoreTicketPoll::Ready(())));
        drop(receipt);
        let core_phases = wake.register_phases(waiter_id, 2).expect("Core can begin after Host collection");
        assert_eq!(core_phases, vec![identity(72, 2), identity(72, 3)]);
        drop(retirement);
        assert_eq!(wake.live_identity_counts(), (0, 0, 0));
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn three_sequential_owner_operations_register_fresh_phases() {
        let wake = Arc::new(CoreCompletionWake::new());
        let waiter_id = WaiterId(73);
        let retirement = CoreWaiterRetirement {
            wake: Arc::clone(&wake),
            waiter_id,
        };
        let mut phases = Vec::new();
        for expected in 1..=3 {
            let identities = wake.register_phases(waiter_id, 2).expect("fresh phases");
            assert_eq!(identities[0].phase, expected * 2 - 1);
            assert_eq!(identities[1].phase, expected * 2);
            for identity in identities {
                assert!(wake.retire(identity));
                phases.push(identity.phase);
            }
        }
        assert_eq!(phases, vec![1, 2, 3, 4, 5, 6]);
        drop(retirement);
    }

    #[test]
    fn progress_latch_preserves_coalesced_inventory_when_the_doorbell_queue_is_full() {
        let latch = DataPlaneProgressLatch::default();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(ControlMessage::RejectedConnection)
            .expect("fill owner doorbell queue");
        let owner_wake = Mutex::new(Some(sender));

        latch.publish(
            DataPlaneProgress {
                progressed: true,
                journal_advanced: false,
                terminal_inventory_changed: true,
            },
            &owner_wake,
        );
        latch.publish(
            DataPlaneProgress {
                progressed: false,
                journal_advanced: true,
                terminal_inventory_changed: true,
            },
            &owner_wake,
        );

        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::RejectedConnection)
        ));
        assert_eq!(
            latch.take(),
            DataPlaneProgress {
                progressed: true,
                journal_advanced: true,
                terminal_inventory_changed: true,
            }
        );
        assert_eq!(latch.take(), DataPlaneProgress::default());

        latch.publish(
            DataPlaneProgress {
                progressed: false,
                journal_advanced: false,
                terminal_inventory_changed: true,
            },
            &owner_wake,
        );
        latch.publish(
            DataPlaneProgress {
                progressed: true,
                journal_advanced: true,
                terminal_inventory_changed: false,
            },
            &owner_wake,
        );
        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::DataPlaneProgress)
        ));
        assert_eq!(
            latch.take(),
            DataPlaneProgress {
                progressed: true,
                journal_advanced: true,
                terminal_inventory_changed: true,
            }
        );
    }

    #[test]
    fn owner_result_cannot_advance_before_its_identity_is_collected() {
        let wake = Arc::new(CoreCompletionWake::new());
        let (mut ticket, publisher) = owner_channel(identity(81, 1), Arc::clone(&wake));
        // Stop at the production ordering boundary between result and wake.
        publisher
            .sender
            .as_ref()
            .expect("sender")
            .try_send(CoreTicketResult {
                identity: identity(81, 1),
                value: 42_u8,
            })
            .expect("publish result before identity");
        assert!(matches!(ticket.poll(), CoreTicketPoll::Pending));
        assert!(wake.take_identities(1).is_empty());
        wake.publish(identity(81, 1));
        assert!(matches!(ticket.poll(), CoreTicketPoll::Pending));
        assert_eq!(wake.take_identities(1), vec![identity(81, 1)]);
        assert!(matches!(ticket.poll(), CoreTicketPoll::Ready(42)));
        assert_eq!(
            wake.register_phases(WaiterId(81), 2),
            Some(vec![identity(81, 2), identity(81, 3)])
        );
        drop(publisher);
        assert!(
            wake.take_identities(2).is_empty(),
            "late publisher drop cannot wake a new phase"
        );
    }

    #[test]
    fn dropped_owner_publisher_wakes_a_lost_ticket_after_channel_close() {
        let wake = Arc::new(CoreCompletionWake::new());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        wake.bind(sender);
        let (mut ticket, publisher) = owner_channel::<u8>(identity(82, 1), Arc::clone(&wake));
        drop(publisher);
        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::CoreCompletionPublished)
        ));
        assert!(matches!(ticket.poll(), CoreTicketPoll::Pending));
        assert_eq!(wake.take_identities(1), vec![identity(82, 1)]);
        assert!(matches!(ticket.poll(), CoreTicketPoll::Lost));
        assert_eq!(
            wake.register_phases(WaiterId(82), 1),
            Some(vec![identity(82, 2)])
        );
    }

    #[test]
    fn keyed_result_is_published_before_an_independent_owner_wake() {
        let wake = Arc::new(CoreCompletionWake::new());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        wake.bind(sender);
        let progress = DataPlaneProgressLatch::default();
        let (mut ticket, publisher) = owner_channel(identity(7, 1), Arc::clone(&wake));

        publisher.publish(41_u8);

        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::CoreCompletionPublished)
        ));
        assert_eq!(progress.take(), DataPlaneProgress::default());
        assert!(wake.take());
        assert_eq!(wake.take_identities(1), vec![identity(7, 1)]);
        assert!(matches!(ticket.poll(), CoreTicketPoll::Ready(41)));
    }

    #[test]
    fn coalesced_wake_readies_only_the_matching_tickets() {
        let wake = Arc::new(CoreCompletionWake::new());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        wake.bind(sender);
        let (mut first, first_publisher) = owner_channel(identity(8, 1), Arc::clone(&wake));
        let (mut second, second_publisher) = owner_channel(identity(9, 1), Arc::clone(&wake));

        second_publisher.publish(22_u8);

        assert!(matches!(first.poll(), CoreTicketPoll::Pending));
        assert!(matches!(second.poll(), CoreTicketPoll::Pending));
        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::CoreCompletionPublished)
        ));
        assert!(receiver.try_recv().is_err());

        first_publisher.publish(11_u8);
        assert!(receiver.try_recv().is_err());
        assert!(wake.take());
        assert_eq!(
            wake.take_identities(2),
            vec![identity(8, 1), identity(9, 1)]
        );
        assert!(matches!(first.poll(), CoreTicketPoll::Ready(11)));
        assert!(matches!(second.poll(), CoreTicketPoll::Ready(22)));
    }

    #[test]
    fn completion_bit_survives_a_full_owner_doorbell_queue() {
        let wake = Arc::new(CoreCompletionWake::new());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(ControlMessage::RejectedConnection)
            .expect("fill owner doorbell queue");
        wake.bind(sender);
        let (mut ticket, publisher) = owner_channel(identity(10, 1), Arc::clone(&wake));

        publisher.publish(31_u8);

        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::RejectedConnection)
        ));
        assert!(receiver.try_recv().is_err());
        assert!(wake.take());
        assert_eq!(wake.take_identities(1), vec![identity(10, 1)]);
        assert!(matches!(ticket.poll(), CoreTicketPoll::Ready(31)));
    }

    #[test]
    fn retired_identity_is_removed_before_owner_consumption() {
        let wake = Arc::new(CoreCompletionWake::new());
        let expected = identity(12, 1);
        let (mut ticket, publisher) = owner_channel(expected, Arc::clone(&wake));

        publisher.publish(7_u8);
        assert!(wake.retire(expected));

        assert!(wake.take_identities(1).is_empty());
        assert!(matches!(ticket.poll(), CoreTicketPoll::Ready(7)));
    }

    #[test]
    fn blocking_consumer_results_do_not_enter_the_owner_mailbox() {
        let wake = Arc::new(CoreCompletionWake::new());
        let expected = identity(14, 1);
        let (ticket, publisher) = CoreTicket::channel(expected, Arc::clone(&wake), false);

        publisher.publish(9_u8);

        assert_eq!(ticket.wait(Duration::ZERO), Ok(9));
        assert!(!wake.take());
        assert!(wake.take_identities(1).is_empty());
    }

    #[test]
    fn consumed_identities_release_capacity_and_return_to_idle() {
        let wake = Arc::new(CoreCompletionWake::new());
        let first = identity(15, 1);
        let second = identity(16, 1);
        let (first_ticket, first_publisher) = owner_channel(first, Arc::clone(&wake));
        let (second_ticket, second_publisher) = owner_channel(second, Arc::clone(&wake));

        first_publisher.publish(1_u8);
        second_publisher.publish(2_u8);
        assert!(wake.take());
        assert_eq!(wake.take_identities(1), vec![first]);
        assert_eq!(wake.take_identities(1), vec![second]);
        assert!(!wake.take());
        assert!(wake.take_identities(1).is_empty());
        drop((first_ticket, second_ticket));

        let replacement = identity(17, 1);
        assert_eq!(
            wake.register_phases(replacement.waiter_id, 1),
            Some(vec![replacement])
        );
        assert!(wake.retire(replacement));
    }

    #[test]
    fn identity_capacity_covers_two_phases_for_each_owner_permit() {
        assert_eq!(
            CORE_OWNER_COMPLETION_CAPACITY,
            crate::daemon::owner_budget::OWNER_BUDGET_CAPACITY * 2
                + crate::daemon::owner_loop::BACKGROUND_CORE_WORK_CLASSES
        );
        let wake = CoreCompletionWake::new();

        for waiter in 1..=crate::daemon::owner_budget::OWNER_BUDGET_CAPACITY {
            assert_eq!(
                wake.register_phases(WaiterId(waiter as u64), 2)
                    .expect("each owner permit reserves two phases")
                    .len(),
                2
            );
        }
        for offset in 0..crate::daemon::owner_loop::BACKGROUND_CORE_WORK_CLASSES {
            assert!(wake
                .register_phases(WaiterId(u64::MAX - offset as u64), 1)
                .is_some());
        }
        assert!(
            wake.register_phases(
                WaiterId(u64::MAX - crate::daemon::owner_loop::BACKGROUND_CORE_WORK_CLASSES as u64),
                1
            )
            .is_none()
        );
        for waiter in 1..=crate::daemon::owner_budget::OWNER_BUDGET_CAPACITY {
            assert_eq!(wake.retire_waiter(WaiterId(waiter as u64)), 2);
        }
    }

    #[test]
    fn wired_owner_identity_lifecycle_reuses_capacity_across_mixed_batches() {
        const BATCH_SIZE: usize = 17;

        let root = std::env::temp_dir().join(format!(
            "botster-core-owner-capacity-{}-{}",
            std::process::id(),
            current_unix_seconds()
        ));
        std::fs::create_dir_all(&root).expect("create Core identity test directory");
        let mut daemon = CoreDaemon::new(CoreDaemonConfig::new(&root));
        let control = daemon.wake_pump_control();
        let failed_root = root.join("stopped");
        std::fs::create_dir_all(&failed_root).expect("create stopped Core test directory");
        let mut failed_daemon = CoreDaemon::new(CoreDaemonConfig::new(&failed_root));
        let failed_control = failed_daemon.wake_pump_control();
        failed_control.request_stop();
        assert!(matches!(
            failed_daemon.wait_pump(Duration::ZERO),
            WakePumpWait::Stopped
        ));
        failed_daemon
            .shutdown(None, current_unix_seconds())
            .expect("stop Core so each tested begin fails deterministically");
        let (requests, request_rx) = mpsc::sync_channel::<CoreRequest>(CORE_REQUEST_CAPACITY);
        let accepting = Arc::new(AtomicBool::new(true));
        let completion_wake = Arc::new(CoreCompletionWake::new());
        let handle = CoreDaemonHandle {
            requests,
            control,
            accepting,
            admission: Arc::new(Mutex::new(())),
            request_pending: Arc::new(AtomicBool::new(false)),
            owner_waiting: Arc::new(AtomicBool::new(false)),
            waiter_ids: Arc::new(WaiterIdSource::default()),
            completion_wake: Arc::clone(&completion_wake),
            #[cfg(test)]
            refuse_next_owner_begins: Arc::new(AtomicUsize::new(0)),
            refuse_registered_owner_begins: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            lose_next_owner_begins: Arc::new(AtomicUsize::new(0)),
            lose_reserve_completion_for: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            release_session_reservation_begins: Arc::new(AtomicUsize::new(0)),
        };
        let mut pending = PendingCoreOperations::new();
        let mut next_waiter = 1_u64;
        let mut registered_phases = 0_usize;

        while registered_phases <= CORE_OWNER_COMPLETION_CAPACITY {
            let mut successes = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                let waiter_id = WaiterId(next_waiter);
                next_waiter += 1;
                successes.push((waiter_id, handle.submit_for_owner(waiter_id, |_| 7_u8)));
                registered_phases += 1;
            }
            for _ in 0..BATCH_SIZE {
                request_rx
                    .try_recv()
                    .expect("each successful submission queues one Core request")
                    .run(&mut daemon, &mut pending);
            }
            assert_eq!(
                handle.take_owner_completion_identities(BATCH_SIZE).len(),
                BATCH_SIZE
            );
            for (waiter_id, mut ticket) in successes {
                assert!(matches!(ticket.poll(), CoreTicketPoll::Ready(7)));
                assert_eq!(handle.retire_owner_waiter(waiter_id), 0);
            }
            assert_eq!(completion_wake.live_identity_counts(), (0, 0, 0));

            let completed_waiter = WaiterId(next_waiter);
            next_waiter += 1;
            let completed = handle.begin_for_owner(
                completed_waiter,
                CoreOperation::RemoveSession(botster_core::SessionId(format!(
                    "completed-missing-{next_waiter}"
                ))),
            );
            registered_phases += 2;
            request_rx
                .try_recv()
                .expect("the successful begin queues one Core request")
                .run(&mut daemon, &mut pending);
            publish_completions(&mut daemon, &mut pending);
            assert_eq!(handle.take_owner_completion_identities(2).len(), 2);
            let (mut begin, mut completion) = completed.into_parts();
            assert!(matches!(begin.poll(), CoreTicketPoll::Ready(Ok(_))));
            assert!(matches!(completion.poll(), CoreTicketPoll::Ready(_)));
            assert_eq!(handle.retire_owner_waiter(completed_waiter), 0);
            assert_eq!(completion_wake.live_identity_counts(), (0, 0, 0));

            let failed_waiter = WaiterId(next_waiter);
            next_waiter += 1;
            let failed = handle.begin_for_owner(
                failed_waiter,
                CoreOperation::RemoveSession(botster_core::SessionId(format!(
                    "missing-{next_waiter}"
                ))),
            );
            registered_phases += 2;
            request_rx
                .try_recv()
                .expect("the failed begin queues one Core request")
                .run(&mut failed_daemon, &mut pending);
            assert_eq!(handle.take_owner_completion_identities(2).len(), 1);
            let (mut begin, mut completion) = failed.into_parts();
            assert!(matches!(begin.poll(), CoreTicketPoll::Ready(Err(_))));
            assert!(matches!(completion.poll(), CoreTicketPoll::Lost));
            assert_eq!(handle.retire_owner_waiter(failed_waiter), 0);
            assert_eq!(completion_wake.live_identity_counts(), (0, 0, 0));

            for _ in 0..CORE_REQUEST_CAPACITY {
                handle
                    .requests
                    .try_send(noop_request())
                    .expect("fill the bounded Core request queue");
            }
            let refused_waiter = WaiterId(next_waiter);
            next_waiter += 1;
            let mut refused = handle.submit_for_owner(refused_waiter, |_| 9_u8);
            registered_phases += 1;
            assert!(matches!(refused.poll(), CoreTicketPoll::Refused));
            assert_eq!(request_rx.try_iter().count(), CORE_REQUEST_CAPACITY);
            assert_eq!(handle.retire_owner_waiter(refused_waiter), 0);
            assert_eq!(completion_wake.live_identity_counts(), (0, 0, 0));

            let retired_waiter = WaiterId(next_waiter);
            next_waiter += 1;
            let mut retired = handle.submit_for_owner(retired_waiter, |_| 11_u8);
            registered_phases += 1;
            assert_eq!(handle.retire_owner_waiter(retired_waiter), 1);
            request_rx
                .try_recv()
                .expect("the retired request remains accepted Core work")
                .run(&mut daemon, &mut pending);
            assert!(matches!(retired.poll(), CoreTicketPoll::Ready(11)));
            assert!(handle.take_owner_completion_identities(1).is_empty());
            assert_eq!(completion_wake.live_identity_counts(), (0, 0, 0));
        }

        assert!(registered_phases > CORE_OWNER_COMPLETION_CAPACITY);
        assert_eq!(completion_wake.live_identity_counts(), (0, 0, 0));
        std::fs::remove_dir_all(root).expect("remove Core identity test directory");
    }

    #[test]
    fn stale_phase_cannot_ready_a_sibling_and_releases_its_value() {
        let wake = Arc::new(CoreCompletionWake::new());
        let expected = identity(11, 3);
        let sibling = identity(11, 2);
        let (mut ticket, publisher) = CoreTicket::channel(expected, wake, false);
        let drops = Arc::new(AtomicUsize::new(0));
        publisher
            .sender
            .as_ref()
            .expect("sender")
            .try_send(CoreTicketResult {
                identity: sibling,
                value: DropProbe(Arc::clone(&drops)),
            })
            .expect("inject stale phase");

        assert!(matches!(ticket.poll(), CoreTicketPoll::Pending));
        assert_eq!(drops.load(Ordering::Acquire), 1);

        publisher.publish(DropProbe(Arc::clone(&drops)));
        let CoreTicketPoll::Ready(result) = ticket.poll() else {
            panic!("the matching phase must be ready");
        };
        drop(result);
        assert_eq!(drops.load(Ordering::Acquire), 2);
    }

    #[test]
    fn duplicate_result_is_rejected_and_releases_its_value() {
        let wake = Arc::new(CoreCompletionWake::new());
        let expected = identity(13, 1);
        let (mut ticket, publisher) = CoreTicket::channel(expected, wake, false);
        let drops = Arc::new(AtomicUsize::new(0));

        publisher
            .sender
            .as_ref()
            .expect("sender")
            .try_send(CoreTicketResult {
                identity: expected,
                value: DropProbe(Arc::clone(&drops)),
            })
            .expect("publish first result");
        let duplicate = publisher
            .sender
            .as_ref()
            .expect("sender")
            .try_send(CoreTicketResult {
                identity: expected,
                value: DropProbe(Arc::clone(&drops)),
            });
        assert!(matches!(duplicate, Err(TrySendError::Full(_))));
        drop(duplicate);

        assert_eq!(drops.load(Ordering::Acquire), 1);
        let CoreTicketPoll::Ready(result) = ticket.poll() else {
            panic!("the first matching result must remain ready");
        };
        drop(result);
        assert_eq!(drops.load(Ordering::Acquire), 2);
    }

    fn check_unregistered_submit_preserves_owner(full: bool, accepting: bool, connected: bool) {
        for owner_ready in [false, true] {
            let root = std::env::temp_dir().join(format!(
                "botster-unregistered-submit-{}-{full}-{accepting}-{connected}-{owner_ready}",
                std::process::id()
            ));
            std::fs::create_dir_all(&root).expect("create Core submit test directory");
            let mut daemon = CoreDaemon::new(CoreDaemonConfig::new(&root));
            let (requests, receiver) = mpsc::sync_channel::<CoreRequest>(CORE_REQUEST_CAPACITY);
            if full {
                for _ in 0..CORE_REQUEST_CAPACITY {
                    requests
                        .try_send(noop_request())
                        .expect("fill request queue");
                }
            }
            let receiver = connected.then_some(receiver);
            let wake = Arc::new(CoreCompletionWake::new());
            let expected = identity(1, 1);
            let (mut owner_ticket, owner_publisher) = owner_channel(expected, Arc::clone(&wake));
            let mut owner_publisher = Some(owner_publisher);
            if owner_ready {
                owner_publisher.take().unwrap().publish(7_u8);
            }
            let handle = CoreDaemonHandle {
                requests,
                control: daemon.wake_pump_control(),
                accepting: Arc::new(AtomicBool::new(accepting)),
                admission: Arc::new(Mutex::new(())),
                request_pending: Arc::new(AtomicBool::new(false)),
                owner_waiting: Arc::new(AtomicBool::new(false)),
                waiter_ids: Arc::new(WaiterIdSource::default()),
                completion_wake: Arc::clone(&wake),
                refuse_next_owner_begins: Arc::new(AtomicUsize::new(0)),
                refuse_registered_owner_begins: Arc::new(AtomicUsize::new(0)),
                lose_next_owner_begins: Arc::new(AtomicUsize::new(0)),
                lose_reserve_completion_for: Arc::new(Mutex::new(None)),
                release_session_reservation_begins: Arc::new(AtomicUsize::new(0)),
            };
            let drops = Arc::new(AtomicUsize::new(0));
            let probe = DropProbe(Arc::clone(&drops));
            let mut ticket = handle.submit::<(), _>(move |_| {
                drop(probe);
                panic!("an unadmitted request must not execute");
            });
            if accepting && connected {
                assert!(matches!(ticket.poll(), CoreTicketPoll::Refused));
            } else {
                assert!(matches!(ticket.poll(), CoreTicketPoll::Lost));
            }
            drop(ticket);
            assert_eq!(drops.load(Ordering::Acquire), 1);
            assert!(!handle.request_pending.load(Ordering::Acquire));
            assert_eq!(
                wake.live_identity_counts(),
                (1, usize::from(owner_ready), 1)
            );
            assert!(matches!(owner_ticket.poll(), CoreTicketPoll::Pending));
            assert_eq!(wake.take(), owner_ready);
            if let Some(publisher) = owner_publisher {
                publisher.publish(7_u8);
                assert!(wake.take());
            }
            assert_eq!(handle.take_owner_completion_identities(1), vec![expected]);
            assert!(matches!(owner_ticket.poll(), CoreTicketPoll::Ready(7)));
            assert!(handle.take_owner_completion_identities(1).is_empty());
            assert!(!wake.take());
            assert_eq!(handle.retire_owner_waiter(expected.waiter_id), 0);
            assert_eq!(wake.live_identity_counts(), (0, 0, 0));
            if let Some(receiver) = receiver {
                assert_eq!(
                    receiver.try_iter().count(),
                    if full { CORE_REQUEST_CAPACITY } else { 0 }
                );
            }
            drop((owner_ticket, handle, daemon));
            std::fs::remove_dir_all(root).expect("remove Core submit test directory");
        }
    }

    #[test]
    fn unregistered_submit_refusal_preserves_colliding_owner_identity() {
        check_unregistered_submit_preserves_owner(true, true, true);
    }

    #[test]
    fn unregistered_submit_stopped_admission_preserves_colliding_owner_identity() {
        check_unregistered_submit_preserves_owner(false, false, true);
    }

    #[test]
    fn unregistered_submit_disconnected_queue_preserves_colliding_owner_identity() {
        check_unregistered_submit_preserves_owner(false, true, false);
    }

    #[test]
    fn full_queue_refuses_without_waiting_and_keeps_queued_work() {
        let (requests, receiver) = mpsc::sync_channel::<CoreRequest>(CORE_REQUEST_CAPACITY);
        let accepting = AtomicBool::new(true);
        for _ in 0..CORE_REQUEST_CAPACITY {
            assert_eq!(
                admit_request(&requests, &accepting, noop_request()),
                CoreAdmission::Queued
            );
        }
        // The receiver is never drained: the next admission must refuse at
        // once instead of parking the submitter behind the data plane.
        let started = Instant::now();
        assert_eq!(
            admit_request(&requests, &accepting, noop_request()),
            CoreAdmission::Refused
        );
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "refusal must not wait for queue space"
        );
        // Refusal drops nothing already queued.
        assert_eq!(receiver.try_iter().count(), CORE_REQUEST_CAPACITY);
        // Space freed by the consumer admits again.
        assert_eq!(
            admit_request(&requests, &accepting, noop_request()),
            CoreAdmission::Queued
        );
    }

    #[test]
    fn stop_admission_wins_over_a_saturated_queue() {
        let (requests, _receiver) = mpsc::sync_channel::<CoreRequest>(CORE_REQUEST_CAPACITY);
        let accepting = Arc::new(AtomicBool::new(true));
        let admission = Arc::new(Mutex::new(()));
        for _ in 0..CORE_REQUEST_CAPACITY {
            assert_eq!(
                admit_request(&requests, &accepting, noop_request()),
                CoreAdmission::Queued
            );
        }
        // A submitter racing a saturated queue holds the admission mutex only
        // for one try_send, so the stop path acquires it promptly.
        let submitter = {
            let requests = requests.clone();
            let accepting = Arc::clone(&accepting);
            let admission = Arc::clone(&admission);
            std::thread::spawn(move || {
                let _guard = admission.lock().expect("admission");
                admit_request(&requests, &accepting, noop_request())
            })
        };
        let refused = submitter.join().expect("submitter thread");
        assert_eq!(refused, CoreAdmission::Refused);
        let started = Instant::now();
        {
            let _guard = admission.lock().expect("admission");
            accepting.store(false, Ordering::Release);
        }
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "stop admission must not wait behind a full queue"
        );
        assert_eq!(
            admit_request(&requests, &accepting, noop_request()),
            CoreAdmission::Stopped
        );
    }

    #[test]
    fn refused_ticket_resolves_without_a_result() {
        let mut ticket: CoreTicket<u8> = CoreTicket::refused();
        assert!(matches!(ticket.poll(), CoreTicketPoll::Refused));
        assert_eq!(
            ticket.wait(Duration::from_millis(1)),
            Err(CoreTicketError::Overloaded)
        );
        let mut lost: CoreTicket<u8> =
            CoreTicket::lost(OwnerWorkIdentity::first(crate::owner_identity::WaiterId(1)));
        assert!(matches!(lost.poll(), CoreTicketPoll::Lost));
    }
}
