//! One owned Hub thread that waits on Core wakes, drives targeted pumps, and
//! runs host operations against the single Core owner.
//!
//! The Hub owner thread never blocks on Core. It submits work through
//! [`CoreDaemonHandle::submit`] or [`CoreDaemonHandle::begin`] and reads the
//! outcome later from a [`CoreTicket`] or from the [`CoreCompletionReceiver`],
//! both polled from its own turn. The data-plane thread wakes the owner with
//! `ControlMessage::DataPlaneProgress` whenever a pump moved data, a ticket
//! result was published, or Core finished a pending operation.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use botster_core_daemon::{
    CoreCompletion, CoreDaemon, CoreDaemonConfig, CoreDaemonError, CoreOperation,
    PendingOperationId, WakePumpControl, WakePumpWait,
};

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::data_plane::close_work::CloseWorkSource;
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

type CoreRequest = Box<dyn FnOnce(&mut CoreDaemon) + Send + 'static>;

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

#[derive(Debug)]
enum CoreTicketSlot<T> {
    /// The operation was queued; its result arrives on this channel.
    Queued(Receiver<T>),
    /// Admission refused the operation; there is nothing to wait for.
    Refused,
}

impl<T> CoreTicket<T> {
    fn queued(receiver: Receiver<T>) -> Self {
        Self {
            slot: CoreTicketSlot::Queued(receiver),
        }
    }

    fn lost() -> Self {
        let (_sender, receiver) = mpsc::sync_channel(1);
        Self::queued(receiver)
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
        let (sender, receiver) = mpsc::sync_channel(1);
        sender
            .send(value)
            .expect("a resolved ticket holds exactly one value");
        Self::queued(receiver)
    }

    /// Non-blocking read; owner-thread use.
    pub(crate) fn poll(&mut self) -> CoreTicketPoll<T> {
        match &self.slot {
            CoreTicketSlot::Refused => CoreTicketPoll::Refused,
            CoreTicketSlot::Queued(receiver) => match receiver.try_recv() {
                Ok(value) => CoreTicketPoll::Ready(value),
                Err(TryRecvError::Empty) => CoreTicketPoll::Pending,
                Err(TryRecvError::Disconnected) => CoreTicketPoll::Lost,
            },
        }
    }

    /// Bounded blocking read for threads that do not serve the owner loop.
    pub fn wait(self, timeout: Duration) -> Result<T, CoreTicketError> {
        match self.slot {
            CoreTicketSlot::Refused => Err(CoreTicketError::Overloaded),
            CoreTicketSlot::Queued(receiver) => match receiver.recv_timeout(timeout) {
                Ok(value) => Ok(value),
                Err(RecvTimeoutError::Timeout) => Err(CoreTicketError::Timeout),
                Err(RecvTimeoutError::Disconnected) => Err(CoreTicketError::DriverStopped),
            },
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

/// Owner-side receiver of Core completions published by the data-plane thread.
#[derive(Debug)]
pub(crate) struct CoreCompletionReceiver {
    receiver: Receiver<CoreCompletion>,
}

impl CoreCompletionReceiver {
    /// Drain every completion published so far. Never blocks.
    pub(crate) fn take(&self) -> Vec<CoreCompletion> {
        let mut completions = Vec::new();
        while let Ok(completion) = self.receiver.try_recv() {
            completions.push(completion);
        }
        completions
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
}

impl CoreDaemonHandle {
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
        let (completed_tx, completed_rx) = mpsc::sync_channel(1);
        let request: CoreRequest = Box::new(move |daemon| {
            let _ = completed_tx.send(operation(daemon));
        });
        let _admission = self.admission.lock().expect("Core request admission mutex");
        match admit_request(&self.requests, &self.accepting, request) {
            CoreAdmission::Queued => {}
            CoreAdmission::Refused => return CoreTicket::refused(),
            CoreAdmission::Stopped => return CoreTicket::lost(),
        }
        self.request_pending.store(true, Ordering::Release);
        if self.owner_waiting.swap(false, Ordering::AcqRel) {
            self.control.interrupt();
        }
        drop(_admission);
        CoreTicket::queued(completed_rx)
    }

    /// Start one Core operation. The ticket carries the pending id; the
    /// result arrives later on the [`CoreCompletionReceiver`].
    pub(crate) fn begin(
        &self,
        operation: CoreOperation,
    ) -> CoreTicket<Result<PendingOperationId, CoreDaemonError>> {
        self.submit(move |daemon| daemon.begin(operation))
    }
}

impl DataPlaneDriver {
    pub(crate) fn start(
        core_config: CoreDaemonConfig,
        close_work: CloseWorkSource,
    ) -> (Self, CoreDaemonHandle, CoreCompletionReceiver) {
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let (request_tx, request_rx) = mpsc::sync_channel(CORE_REQUEST_CAPACITY);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (completion_tx, completion_rx) = mpsc::channel();
        let accepting = Arc::new(AtomicBool::new(true));
        let admission = Arc::new(Mutex::new(()));
        let request_pending = Arc::new(AtomicBool::new(false));
        let owner_waiting = Arc::new(AtomicBool::new(false));
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
                    completion_tx,
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
        };
        let driver = Self {
            core: core.clone(),
            stop_action,
            done: done_rx,
            thread: Some(thread),
            owner_wake,
            progress_latch,
        };
        (
            driver,
            core,
            CoreCompletionReceiver {
                receiver: completion_rx,
            },
        )
    }

    pub(crate) fn bind_owner_wake(&self, sender: ControlSender) {
        if let Ok(mut slot) = self.owner_wake.lock() {
            *slot = Some(sender);
        }
    }

    pub(crate) fn take_progress(&self) -> DataPlaneProgress {
        self.progress_latch.take()
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
    completions: mpsc::Sender<CoreCompletion>,
    close_work: CloseWorkSource,
    owner_wake: Arc<Mutex<Option<ControlSender>>>,
    progress_latch: Arc<DataPlaneProgressLatch>,
    request_pending: Arc<AtomicBool>,
    owner_waiting: Arc<AtomicBool>,
) {
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
        progress |= run_core_requests(core_daemon, &requests) > 0;
        progress |= publish_completions(core_daemon, &completions);
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
        request(core_daemon);
    }
    let _ = publish_completions(core_daemon, &completions);
}

fn run_core_requests(core_daemon: &mut CoreDaemon, requests: &Receiver<CoreRequest>) -> usize {
    let mut ran = 0;
    for request in requests.try_iter().take(CORE_REQUESTS_PER_TURN) {
        request(core_daemon);
        ran += 1;
    }
    ran
}

/// Move finished Core operations to the owner. Returns whether any moved.
fn publish_completions(
    core_daemon: &mut CoreDaemon,
    completions: &mpsc::Sender<CoreCompletion>,
) -> bool {
    let finished = core_daemon.take_completions();
    let published = !finished.is_empty();
    for completion in finished {
        if completions.send(completion).is_err() {
            break;
        }
    }
    published
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
    use std::time::Instant;

    fn noop_request() -> CoreRequest {
        Box::new(|_| {})
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
        let mut lost: CoreTicket<u8> = CoreTicket::lost();
        assert!(matches!(lost.poll(), CoreTicketPoll::Lost));
    }
}
