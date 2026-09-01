//! One owned Hub thread that waits on Core wakes and drives targeted pumps.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use botster_core_daemon::{CoreDaemon, CoreDaemonConfig, WakePumpControl, WakePumpWait};

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::data_plane::close_work::CloseWorkSource;
use crate::subscription::closed_events::session_close_event_decision;

pub(crate) const DATA_PLANE_WATCHDOG: Duration = Duration::from_secs(1);
pub(crate) const DATA_PLANE_STOP_SLACK: Duration = Duration::from_millis(500);
pub(crate) const DATA_PLANE_STOP_BOUND: Duration = Duration::from_millis(
    DATA_PLANE_WATCHDOG.as_millis() as u64 * 2 + DATA_PLANE_STOP_SLACK.as_millis() as u64,
);
pub(crate) const DATA_PLANE_MAX_CLOSE_KEYS: usize = 8;
const CORE_REQUEST_CAPACITY: usize = 64;
const CORE_REQUESTS_PER_TURN: usize = CORE_REQUEST_CAPACITY;
const STOP_ACTION_SHUTDOWN: u8 = 0;
const STOP_ACTION_RELEASE_FOR_RESTART: u8 = 1;

pub(crate) const TEST_PAUSE_DATA_PLANE_ENV: &str = "BOTSTER_HUB_TEST_PAUSE_DATA_PLANE";
pub(crate) const TEST_PARK_DATA_PLANE_TURN_ENV: &str = "BOTSTER_HUB_TEST_PARK_DATA_PLANE_TURN";
pub(crate) const TEST_DATA_PLANE_WATCHDOG_MS_ENV: &str = "BOTSTER_HUB_TEST_DATA_PLANE_WATCHDOG_MS";
pub(crate) const TEST_DATA_PLANE_OBSERVATION_ENV: &str = "BOTSTER_HUB_TEST_DATA_PLANE_OBSERVATION";
pub(crate) const DATA_PLANE_DRIVER_STOP_TIMEOUT: &str = "data_plane_driver_stop_timeout";

pub(crate) struct DataPlaneDriver {
    core: CoreDaemonHandle,
    stop_action: Arc<AtomicU8>,
    done: Receiver<()>,
    thread: Option<JoinHandle<()>>,
    owner_wake: Arc<Mutex<Option<ControlSender>>>,
}

type CoreRequest = Box<dyn FnOnce(&mut CoreDaemon) + Send + 'static>;

/// Hub-owned bounded request bridge to the single Core owner thread.
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
    pub(crate) fn call<T, F>(&self, operation: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(&mut CoreDaemon) -> T + Send + 'static,
    {
        let (completed_tx, completed_rx) = mpsc::sync_channel(1);
        let request: CoreRequest = Box::new(move |daemon| {
            let _ = completed_tx.send(operation(daemon));
        });
        let _admission = self.admission.lock().expect("Core request admission mutex");
        assert!(
            self.accepting.load(Ordering::Acquire),
            "Core owner thread stopped accepting requests"
        );
        match self.requests.try_send(request) {
            Ok(()) => {}
            Err(TrySendError::Full(request)) => {
                self.requests
                    .send(request)
                    .expect("Core owner request channel");
            }
            Err(TrySendError::Disconnected(_)) => panic!("Core owner request channel"),
        }
        self.request_pending.store(true, Ordering::Release);
        if self.owner_waiting.swap(false, Ordering::AcqRel) {
            self.control.interrupt();
        }
        drop(_admission);
        completed_rx.recv().expect("Core owner request completion")
    }
}

#[derive(Default)]
struct DriverCounters {
    pumps: AtomicU64,
    close_keys: AtomicU64,
    owner_wakes: AtomicU64,
    adapter_routes: AtomicU64,
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
        (driver, core)
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
            Err(RecvTimeoutError::Timeout) => {
                record_stop_timeout();
                Err(DATA_PLANE_DRIVER_STOP_TIMEOUT)
            }
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
    request_pending: Arc<AtomicBool>,
    owner_waiting: Arc<AtomicBool>,
) {
    let counters = DriverCounters::default();
    loop {
        park_turn_for_test();
        owner_waiting.store(true, Ordering::Release);
        let wait_timeout = if request_pending.swap(false, Ordering::AcqRel) {
            owner_waiting.store(false, Ordering::Release);
            Duration::ZERO
        } else {
            watchdog()
        };
        if pause_data_plane() {
            match core_daemon.wait_pump(wait_timeout) {
                WakePumpWait::Stopped => break,
                WakePumpWait::Interrupted | WakePumpWait::Wakes(_) => {}
                _ => {}
            }
            owner_waiting.store(false, Ordering::Release);
            run_core_requests(core_daemon, &requests);
            continue;
        }
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
        let now_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        let mut pumped = false;
        if let Some(batch) = batch {
            counters.adapter_routes.fetch_add(
                u64::try_from(batch.adapter_routes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            if core_daemon.pump_woken(&batch, now_seconds).is_ok() {
                counters.pumps.fetch_add(1, Ordering::Relaxed);
                pumped = !batch.adapter_routes.is_empty() || !batch.ingress_sessions.is_empty();
            }
        }
        run_core_requests(core_daemon, &requests);
        {
            let close_batch = close_work.take_batch(DATA_PLANE_MAX_CLOSE_KEYS);
            counters
                .close_keys
                .fetch_add(close_batch.len() as u64, Ordering::Relaxed);
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
        if pumped
            && let Ok(slot) = owner_wake.lock()
            && let Some(sender) = slot.as_ref()
            && sender.try_send(ControlMessage::DataPlaneProgress).is_ok()
        {
            counters.owner_wakes.fetch_add(1, Ordering::Relaxed);
        }
        observe_for_test(&counters, close_work.live_count());
    }
    for request in requests.try_iter().take(CORE_REQUEST_CAPACITY) {
        request(core_daemon);
    }
}

fn run_core_requests(core_daemon: &mut CoreDaemon, requests: &Receiver<CoreRequest>) {
    for request in requests.try_iter().take(CORE_REQUESTS_PER_TURN) {
        request(core_daemon);
    }
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

fn watchdog() -> Duration {
    if std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
        && let Ok(raw) = std::env::var(TEST_DATA_PLANE_WATCHDOG_MS_ENV)
        && let Ok(ms) = raw.parse::<u64>()
    {
        return Duration::from_millis(ms);
    }
    DATA_PLANE_WATCHDOG
}

fn pause_data_plane() -> bool {
    std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
        && std::env::var(TEST_PAUSE_DATA_PLANE_ENV).as_deref() == Ok("1")
}

fn park_turn_for_test() {
    if std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
        && std::env::var(TEST_PARK_DATA_PLANE_TURN_ENV).as_deref() == Ok("1")
    {
        std::thread::park();
    }
}

fn observe_for_test(counters: &DriverCounters, live_close_routes: usize) {
    if std::env::var("BOTSTER_ENV").as_deref() != Ok("test") {
        return;
    }
    let Ok(path) = std::env::var(TEST_DATA_PLANE_OBSERVATION_ENV) else {
        return;
    };
    if path.is_empty() {
        return;
    }
    let body = serde_json::json!({
        "pumps": counters.pumps.load(Ordering::Relaxed),
        "close_keys": counters.close_keys.load(Ordering::Relaxed),
        "owner_wakes": counters.owner_wakes.load(Ordering::Relaxed),
        "adapter_routes": counters.adapter_routes.load(Ordering::Relaxed),
        "live_close_routes": live_close_routes,
    })
    .to_string();
    let _ = std::fs::write(path, body);
}

fn record_stop_timeout() {
    if std::env::var("BOTSTER_ENV").as_deref() != Ok("test") {
        return;
    }
    if let Ok(path) = std::env::var("BOTSTER_HUB_TEST_DATA_PLANE_STOP_MARKER")
        && !path.is_empty()
    {
        let _ = std::fs::write(path, DATA_PLANE_DRIVER_STOP_TIMEOUT);
    }
}
