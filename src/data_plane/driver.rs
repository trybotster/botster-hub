//! One owned Hub thread that waits on Core wakes and drives targeted pumps.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use botster_core::SessionId;
use botster_core::contract::terminal_wake::TerminalWakeSource;
use botster_core_daemon::CoreDaemon;

use crate::data_plane::close_work::CloseWorkSource;
use crate::subscription::closed_events::session_close_event_decision;

/// `CoreDaemon` holds `Rc` worker state and is therefore `!Send`.
///
/// Every driver access goes through this mutex, the owner thread does not
/// touch Core while the driver holds the lock, and `CoreDaemon` is dropped
/// only after the driver joins. That is the Send bound the host can prove.
#[derive(Clone)]
struct SharedCore(Arc<Mutex<CoreDaemon>>);

unsafe impl Send for SharedCore {}
unsafe impl Sync for SharedCore {}

pub(crate) const DATA_PLANE_WATCHDOG: Duration = Duration::from_secs(1);
pub(crate) const DATA_PLANE_STOP_SLACK: Duration = Duration::from_millis(500);
pub(crate) const DATA_PLANE_STOP_BOUND: Duration = Duration::from_millis(
    DATA_PLANE_WATCHDOG.as_millis() as u64 * 2 + DATA_PLANE_STOP_SLACK.as_millis() as u64,
);
pub(crate) const DATA_PLANE_MAX_CLOSE_KEYS: usize = 8;
const STOP_SESSION_ID: &str = "hub-data-plane-stop";

pub(crate) const TEST_PAUSE_DATA_PLANE_ENV: &str = "BOTSTER_HUB_TEST_PAUSE_DATA_PLANE";
pub(crate) const TEST_PARK_DATA_PLANE_TURN_ENV: &str = "BOTSTER_HUB_TEST_PARK_DATA_PLANE_TURN";
pub(crate) const TEST_DATA_PLANE_WATCHDOG_MS_ENV: &str = "BOTSTER_HUB_TEST_DATA_PLANE_WATCHDOG_MS";
pub(crate) const TEST_DATA_PLANE_OBSERVATION_ENV: &str = "BOTSTER_HUB_TEST_DATA_PLANE_OBSERVATION";
pub(crate) const DATA_PLANE_DRIVER_STOP_TIMEOUT: &str = "data_plane_driver_stop_timeout";

pub(crate) struct DataPlaneDriver {
    stop: Arc<AtomicBool>,
    wake_source: TerminalWakeSource,
    done: Receiver<()>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct DriverCounters {
    pumps: AtomicU64,
    close_keys: AtomicU64,
}

impl DataPlaneDriver {
    pub(crate) fn start(
        core_daemon: Arc<Mutex<CoreDaemon>>,
        wake_source: TerminalWakeSource,
        close_work: CloseWorkSource,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let thread_stop = Arc::clone(&stop);
        let thread_wakes = wake_source.clone();
        let shared = SharedCore(core_daemon);
        let thread = std::thread::Builder::new()
            .name("botster-hub-data-plane".to_string())
            .spawn(move || {
                run_loop(shared, thread_wakes, close_work, thread_stop);
                let _ = done_tx.send(());
            })
            .expect("start botster-hub-data-plane");
        Self {
            stop,
            wake_source,
            done: done_rx,
            thread: Some(thread),
        }
    }

    pub(crate) fn stop_and_join(&mut self) -> Result<(), &'static str> {
        self.stop.store(true, Ordering::SeqCst);
        self.wake_source
            .session_handle(SessionId(STOP_SESSION_ID.to_string()))
            .notify();
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
            match self.stop_and_join() {
                Ok(()) => {}
                Err(_) => std::process::abort(),
            }
        }
    }
}

fn run_loop(
    core_daemon: SharedCore,
    wake_source: TerminalWakeSource,
    close_work: CloseWorkSource,
    stop: Arc<AtomicBool>,
) {
    let counters = DriverCounters::default();
    let _stop_handle = wake_source.session_handle(SessionId(STOP_SESSION_ID.to_string()));
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        park_turn_for_test();
        if stop.load(Ordering::SeqCst) {
            break;
        }
        if pause_data_plane() {
            let _ = wake_source.wait_wakes(watchdog());
            continue;
        }
        let batch = wake_source.wait_wakes(watchdog());
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let now_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        {
            let mut core = core_daemon.0.lock().expect("core daemon mutex");
            if core.pump_woken(&batch, now_seconds).is_ok() {
                counters.pumps.fetch_add(1, Ordering::Relaxed);
            }
            let close_batch = close_work.take_batch(DATA_PLANE_MAX_CLOSE_KEYS);
            counters
                .close_keys
                .fetch_add(close_batch.len() as u64, Ordering::Relaxed);
            for state in close_batch {
                let lookup = core.session_registry_state(&SessionId(state.key.session_id.clone()));
                match session_close_event_decision(lookup) {
                    Some(true) => state.report_if_live(true),
                    Some(false) => state.report_if_live(false),
                    None => {}
                }
            }
        }
        observe_for_test(&counters, close_work.live_count());
    }
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
