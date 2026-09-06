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
/// Host operations the owner may leave queued before `submit` applies
/// backpressure to the submitting thread.
pub(crate) const CORE_REQUEST_CAPACITY: usize = 64;
const CORE_REQUESTS_PER_TURN: usize = CORE_REQUEST_CAPACITY;
const STOP_ACTION_SHUTDOWN: u8 = 0;
const STOP_ACTION_RELEASE_FOR_RESTART: u8 = 1;

pub(crate) const DATA_PLANE_DRIVER_STOP_TIMEOUT: &str = "data_plane_driver_stop_timeout";

pub(crate) struct DataPlaneDriver {
    core: CoreDaemonHandle,
    stop_action: Arc<AtomicU8>,
    done: Receiver<()>,
    thread: Option<JoinHandle<()>>,
    owner_wake: Arc<Mutex<Option<ControlSender>>>,
}

type CoreRequest = Box<dyn FnOnce(&mut CoreDaemon) + Send + 'static>;

/// Outcome of one non-blocking [`CoreTicket::poll`].
#[derive(Debug)]
pub(crate) enum CoreTicketPoll<T> {
    /// The data-plane thread has not published the result yet.
    Pending,
    /// The result.
    Ready(T),
    /// The data-plane thread stopped before it ran the operation.
    Lost,
}

/// Why a blocking [`CoreTicket::wait`] returned without a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoreTicketError {
    /// The bound elapsed first.
    Timeout,
    /// The data-plane thread stopped before it ran the operation.
    DriverStopped,
}

impl std::fmt::Display for CoreTicketError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Timeout => "core operation wait timed out",
            Self::DriverStopped => "core data-plane driver stopped",
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
pub(crate) struct CoreTicket<T> {
    receiver: Receiver<T>,
}

impl<T> CoreTicket<T> {
    fn lost() -> Self {
        let (_sender, receiver) = mpsc::sync_channel(1);
        Self { receiver }
    }

    /// Non-blocking read; owner-thread use.
    pub(crate) fn poll(&mut self) -> CoreTicketPoll<T> {
        match self.receiver.try_recv() {
            Ok(value) => CoreTicketPoll::Ready(value),
            Err(TryRecvError::Empty) => CoreTicketPoll::Pending,
            Err(TryRecvError::Disconnected) => CoreTicketPoll::Lost,
        }
    }

    /// Bounded blocking read for threads that do not serve the owner loop.
    pub(crate) fn wait(self, timeout: Duration) -> Result<T, CoreTicketError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(value) => Ok(value),
            Err(RecvTimeoutError::Timeout) => Err(CoreTicketError::Timeout),
            Err(RecvTimeoutError::Disconnected) => Err(CoreTicketError::DriverStopped),
        }
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
    /// data-plane turn.
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
        if !self.accepting.load(Ordering::Acquire) {
            return CoreTicket::lost();
        }
        match self.requests.try_send(request) {
            Ok(()) => {}
            Err(TrySendError::Full(request)) => {
                if self.requests.send(request).is_err() {
                    return CoreTicket::lost();
                }
            }
            Err(TrySendError::Disconnected(_)) => return CoreTicket::lost(),
        }
        self.request_pending.store(true, Ordering::Release);
        if self.owner_waiting.swap(false, Ordering::AcqRel) {
            self.control.interrupt();
        }
        drop(_admission);
        CoreTicket {
            receiver: completed_rx,
        }
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
        if let Some(batch) = batch
            && core_daemon.pump_woken(&batch, now_seconds).is_ok()
        {
            progress |= !batch.adapter_routes.is_empty() || !batch.ingress_sessions.is_empty();
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
        if (progress || journal_advanced)
            && let Ok(slot) = owner_wake.lock()
            && let Some(sender) = slot.as_ref()
        {
            let _ = sender.try_send(ControlMessage::DataPlaneProgress { journal_advanced });
        }
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
