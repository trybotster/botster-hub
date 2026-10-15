//! The daemon (design section 5): the start order, the owner and `io` threads, and the stop order.
//!
//! [`Daemon::start`] runs start steps 1 to 7 and returns once the Hub listens and adoption has finished, so the caller
//! writes the ready line (HC HS-0). [`Daemon::wait`] is `main`'s wait on the lifecycle channel; it returns when the stop
//! order has run to the end.

use crate::bridge::{Bridge, LaneCaps, OutboxCaps};
use crate::client::handoff::{RouteConfig, RouteParams};
use crate::client::server::{ProtocolConfig, Server, ServerConfig, SharedLimits, FIRST_CLIENT};
use crate::failure::{FailCode, StartFailure};
use crate::fsutil::ensure_dir;
use crate::owner::{guarded, Adoption, Owner, TurnCounts};
use crate::phase::{
    request_stop, Ended, Lifecycle, Phase, SharedPhase, StopCause, Wake, LIFECYCLE_CAPACITY,
};
use crate::roots::Roots;
use crate::start::{self, HubLock, Identity};
use crate::store::{remove_session_dir, restore_spawns, SpawnStore, StoreConfig};
use crate::watch::WatchLimits;
use botster_core::contract::prelude::{
    CoreApi, CoreLimits, Feature, Features, Now, OpenConfig, SessionId,
};
use botster_core::Core;
use mio::net::UnixListener;
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token, Waker};
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The parameters of a daemon (small test values; HP LM-5 rule).
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub lanes: LaneCaps,
    pub counts: TurnCounts,
    pub core_limits: CoreLimits,
    /// The sibling `botster-worker` (HC RT-3: no `PATH` fallback).
    pub worker_path: PathBuf,
    /// Whether `io` handles SIGTERM and SIGINT. A test daemon stops through `Shutdown` instead.
    pub handle_signals: bool,
    /// The spawn operation store (design section 4a).
    pub store: StoreConfig,
    /// The watch table (design section 4; HC BD-1 `max_watches` at least 32).
    pub watches: WatchLimits,
    /// The route reservations (client protocol design, section 2.1).
    pub routes: RouteConfig,
    /// A control connection's outbox. `replies` is `welcome.limits.max_outstanding` (HC EN-4).
    pub outbox: OutboxCaps,
    /// The client protocol's own numbers (PC's layer).
    pub protocol: ProtocolConfig,
    /// Stop step 3: the longest time `io` writes the outboxes before it removes the socket (design section 5).
    pub stop_flush: Duration,
    /// HC section 6 `activity_idle_ms`: a session is `idle` after this long with no output. It is Core's TM-4 silence
    /// threshold of each session (design section 4, "The view row").
    pub activity_idle: Duration,
}

impl DaemonConfig {
    /// The production values, with the worker next to this executable.
    pub fn production(worker_path: PathBuf) -> DaemonConfig {
        DaemonConfig {
            lanes: LaneCaps {
                control: 16,
                request: 256,
                bulk: 64,
            },
            counts: TurnCounts {
                control: 16,
                request: 32,
                bulk: 4,
                events: 64,
                fanout: 256,
                baseline_rows: 256,
            },
            watches: WatchLimits {
                max_watches: 32,
                chunk_rows: 64,
            },
            core_limits: CoreLimits::default(),
            worker_path,
            handle_signals: true,
            store: StoreConfig {
                max_records: 4096,
                store_queue: 64,
                // `operation_retention_seconds` is the Hub's default (HC CF-2 `[limits]`, reported in `welcome.limits`).
                retention_ms: 86_400_000,
                max_record_bytes: 4096,
                turn_sweeps: 8,
            },
            // The Hub's defaults (HC CF-2 `[limits] route_connect_seconds` and `[routes] query_client_deadline_ms`, reported
            // in `welcome.limits` and `route_limits`); the HC sets no number for either.
            routes: RouteConfig {
                max_reservations: 256,
                connect_window: Duration::from_secs(10),
                query_deadline: Duration::from_millis(200),
                pending_ends: 1024,
            },
            // The Hub's defaults: HC CF-2 lists `max_outstanding` with no number, and neither the HC nor the design sets
            // the flush bound.
            outbox: OutboxCaps {
                replies: 64,
                frames: 1024,
                watch_queue: 256,
            },
            protocol: ProtocolConfig::production(),
            stop_flush: Duration::from_millis(500),
            activity_idle: Duration::from_millis(3000),
        }
    }

    /// The server's parameters. Each `welcome.limits` number comes from the one part that enforces it.
    fn server(&self, identity: &Identity, uid: u32) -> ServerConfig {
        let shared = SharedLimits {
            max_watches: self.watches.max_watches as u64,
            max_outstanding: self.outbox.replies as u64,
            operation_retention_seconds: self.store.retention_ms / 1000,
            route_connect_seconds: self.routes.connect_window.as_secs(),
            query_client_deadline_ms: self.routes.query_deadline.as_millis() as u64,
            max_paste_bytes: self.core_limits.max_paste_bytes,
            max_file_bytes: self.core_limits.max_file_bytes,
        };
        ServerConfig::compose(
            &self.protocol,
            shared,
            identity.hub_id.clone(),
            identity.incarnation.clone(),
            uid,
            self.outbox,
        )
    }
}

const OWNER_WAKE: Token = Token(0);
const CORE_WAKE: Token = Token(1);
const IO_WAKE: Token = Token(0);
const LISTENER: Token = Token(1);
const SIGNALS: Token = Token(2);

/// A started daemon: it listens, and adoption has finished.
pub struct Daemon {
    pub identity: Identity,
    pub socket: PathBuf,
    bridge: Arc<Bridge>,
    phase: Arc<SharedPhase>,
    lifecycle: Receiver<Lifecycle>,
    lock: Option<HubLock>,
    owner: JoinHandle<()>,
    io: JoinHandle<()>,
}

/// How the daemon ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exit {
    /// The first stop cause, if a thread sent one.
    pub cause: Option<StopCause>,
    pub owner: Ended,
    pub io: Ended,
}

impl Exit {
    /// 0 for a requested stop (a signal or `Shutdown`) with both threads clean; 1 otherwise.
    pub fn code(&self) -> u8 {
        let requested = matches!(self.cause, Some(StopCause::Signal | StopCause::Requested));
        if requested && self.owner == Ended::Clean && self.io == Ended::Clean {
            0
        } else {
            1
        }
    }
}

fn now() -> Now {
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Now {
        monotonic: Instant::now(),
        unix,
    }
}

impl Daemon {
    /// Start steps 1 to 7 (design section 5).
    ///
    /// # Errors
    /// The typed failure for the ready line. The lock is released when this returns an error.
    pub fn start(roots: &Roots, uid: u32, config: DaemonConfig) -> Result<Daemon, StartFailure> {
        // Steps 1 to 5.
        let lock = start::take_lock(roots)?;
        let identity = start::identity(roots, &lock)?;
        start::check_config(&roots.config_file(), uid)?;
        start::check_runtime_dirs(&roots.runtime_dirs, uid)?;
        let core_dir = start::core_dir(roots, &lock)?;
        let core = Core::open(OpenConfig {
            data_dir: core_dir,
            worker_path: Some(config.worker_path.clone()),
            limits: config.core_limits.clone(),
        })
        .map_err(|error| {
            StartFailure::new(
                FailCode::DataRootUnusable,
                format!("{:?}: {}", error.code, error.detail),
            )
        })?;

        // Step 5, last check: the silence feature is required (it is optional in Core, A2-6). There is no fallback Hub timer.
        require_silence(&core.features())?;

        // Step 6: the owner thread, then `AdoptAll`.
        let phase = SharedPhase::new();
        let internal =
            |error: std::io::Error| StartFailure::new(FailCode::Internal, error.to_string());
        let owner_poll = Poll::new().map_err(internal)?;
        let owner_waker =
            Arc::new(Waker::new(owner_poll.registry(), OWNER_WAKE).map_err(internal)?);
        phase.set_owner_waker(owner_waker.clone());
        let bridge = Arc::new(Bridge::new(
            config.lanes,
            phase.clone(),
            owner_waker.clone(),
        ));
        let (lifecycle_tx, lifecycle) = sync_channel(LIFECYCLE_CAPACITY);
        let (adopted_tx, adopted) = sync_channel(1);
        // The store index is loaded strictly before the owner starts: a record that cannot be resolved fails the start
        // `data_root_unusable` before anything is bound (design section 4a, "Load at start").
        let store = SpawnStore::open(&roots.data_root, config.store, owner_waker.clone())?;
        let owner = Owner::new(
            core,
            bridge.clone(),
            phase.clone(),
            lifecycle_tx.clone(),
            config.counts,
            config.watches,
        )
        .with_store(store)
        .with_activity_idle(config.activity_idle)
        .with_routes(RouteParams::new(
            &config.routes,
            roots.data_root.join("sessions"),
        ));
        // `welcome.terminal_format`'s common set: the owner keeps it (R-124), and the server reads it.
        let formats = owner.routes().map(|routes| routes.formats());
        let server_config = config.server(&identity, uid);
        let owner_wake: Arc<dyn Wake> = owner_waker.clone();
        let owner = {
            let phase = phase.clone();
            let lifecycle_tx = lifecycle_tx.clone();
            std::thread::Builder::new()
                .name("core".into())
                .spawn(move || run_owner(owner, owner_poll, phase, lifecycle_tx, adopted_tx))
                .map_err(internal)?
        };
        // The rows after adoption and the sessions of the `starting` records: the session directories to keep.
        let keep = match adopted.recv() {
            Ok(Ok(keep)) => keep,
            Ok(Err(message)) => return Err(adoption_failed(owner, message)),
            Err(_) => return Err(adoption_failed(owner, "the owner ended first".into())),
        };
        delete_orphan_session_dirs(roots, &keep);

        // Step 7: bind `hub.sock` (HC TB-L1; 0600) after adoption (lead ruling Q4), then start `io`.
        let unavailable = |error: std::io::Error| {
            StartFailure::new(
                FailCode::SocketUnavailable,
                format!("{}: {error}", roots.socket.display()),
            )
        };
        let bound = (|| {
            if let Some(parent) = roots.socket.parent() {
                ensure_dir(parent)?;
            }
            // The lock holder owns the socket path: a socket left by a Hub that ended without its stop is removed.
            match fs::remove_file(&roots.socket) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error),
                _ => {}
            }
            let listener = UnixListener::bind(&roots.socket)?;
            fs::set_permissions(&roots.socket, fs::Permissions::from_mode(0o600))?;
            Ok(listener)
        })();
        let listener = match bound {
            Ok(listener) => listener,
            Err(error) => {
                stop_owner(&phase, &lifecycle_tx, owner);
                return Err(unavailable(error));
            }
        };
        let client = Client {
            bridge: bridge.clone(),
            owner_wake,
            formats: formats.expect("the owner has its route tables"),
            config: server_config,
            stop_flush: config.stop_flush,
        };
        let io = match spawn_io(
            listener,
            &roots.socket,
            &phase,
            &lifecycle_tx,
            config.handle_signals,
            client,
        ) {
            Ok(io) => io,
            Err(error) => {
                let _ = fs::remove_file(&roots.socket);
                stop_owner(&phase, &lifecycle_tx, owner);
                return Err(unavailable(error));
            }
        };
        Ok(Daemon {
            identity,
            socket: roots.socket.clone(),
            bridge,
            phase,
            lifecycle,
            lock: Some(lock),
            owner,
            io,
        })
    }

    /// The bridge, for the protocol layer and for tests.
    pub fn bridge(&self) -> &Arc<Bridge> {
        &self.bridge
    }

    pub fn phase(&self) -> Phase {
        self.phase.get()
    }

    /// `main`'s wait (design section 5, the stop order): it releases the lock after `SocketRemoved`, and returns when both
    /// threads have ended.
    pub fn wait(mut self) -> Exit {
        let mut state = WaitState::default();
        while state.owner.is_none() || state.io.is_none() {
            let Ok(message) = self.lifecycle.recv() else {
                break;
            };
            self.on_message(&mut state, message);
        }
        self.release_lock();
        let _ = self.owner.join();
        let _ = self.io.join();
        Exit {
            cause: state.cause,
            owner: state.owner.unwrap_or(Ended::Panicked),
            io: state.io.unwrap_or(Ended::Panicked),
        }
    }

    /// One lifecycle message of `main`'s wait.
    fn on_message(&mut self, state: &mut WaitState, message: Lifecycle) {
        match message {
            Lifecycle::StopRequested(first) => state.cause = state.cause.or(Some(first)),
            Lifecycle::OwnerGone(ended) => state.owner = Some(ended),
            Lifecycle::SocketRemoved => self.release_lock(),
            Lifecycle::IoGone(ended) => {
                state.io = Some(ended);
                // `io` ends cleanly only after `LockReleased`; an earlier end is a panic, and main starts the stop.
                if self.phase.advance(Phase::Running, Phase::Stopping) {
                    state.cause = state.cause.or(Some(StopCause::IoFailed));
                }
            }
        }
        // `io` panic (design section 5): after `OwnerGone`, main removes `hub.sock` itself, then releases the lock.
        if state.io == Some(Ended::Panicked) && state.owner.is_some() && self.lock.is_some() {
            let _ = fs::remove_file(&self.socket);
            self.phase.advance(Phase::OwnerDone, Phase::SocketRemoved);
            self.release_lock();
        }
    }

    /// Releases the lock once, after the socket is removed (HC RT-4), and moves the phase to `LockReleased`.
    fn release_lock(&mut self) {
        if let Some(lock) = self.lock.take() {
            lock.release();
            self.phase
                .advance(Phase::SocketRemoved, Phase::LockReleased);
        }
    }
}

/// What `main`'s wait has seen.
#[derive(Debug, Default)]
struct WaitState {
    cause: Option<StopCause>,
    owner: Option<Ended>,
    io: Option<Ended>,
}

fn adoption_failed(owner: JoinHandle<()>, message: String) -> StartFailure {
    // The owner has moved to `Stopping` itself and drops `Core` on its own thread; wait for it, so that the lock is released
    // only after Core's own lock.
    let _ = owner.join();
    StartFailure::new(FailCode::AdoptionFailed, message)
}

fn stop_owner(phase: &SharedPhase, lifecycle: &SyncSender<Lifecycle>, owner: JoinHandle<()>) {
    request_stop(phase, lifecycle, StopCause::OwnerFailed);
    let _ = owner.join();
}

/// RT-2: a session directory is deleted with its row, so a directory with no row and no `starting` record after
/// adoption is an orphan (design section 4a, "The session directory"; step 6). Only directories are deleted, read with
/// no-follow: a symlink is never followed and never deleted. This sweep is the one recovery path for every leftover
/// directory. It runs before the bind, so no spawn runs during it.
fn delete_orphan_session_dirs(roots: &Roots, keep: &BTreeSet<SessionId>) {
    let sessions = roots.data_root.join("sessions");
    // The sessions root itself is not followed either: a symlinked root is left alone (#213 H213-1).
    if !fs::symlink_metadata(&sessions).is_ok_and(|m| m.is_dir()) {
        return;
    }
    let Ok(entries) = fs::read_dir(&sessions) else {
        return;
    };
    for entry in entries.flatten() {
        // `DirEntry::file_type` does not follow a symlink.
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !keep.contains(&SessionId(name.clone())) {
            let _ = remove_session_dir(&sessions, &name);
        }
    }
}

/// The owner thread. The owner state lives outside the `catch_unwind` closure, so the panic path still answers every token
/// and drops `Core` on this thread (design section 5, "Owner panic").
fn run_owner(
    mut owner: Owner<Core>,
    mut poll: Poll,
    phase: Arc<SharedPhase>,
    lifecycle: SyncSender<Lifecycle>,
    adopted: SyncSender<Result<BTreeSet<SessionId>, String>>,
) {
    let ended = guarded(&mut owner, |owner| {
        own_core_until_done(owner, &mut poll, &phase, &adopted)
    });
    if ended == Ended::Panicked || !matches!(owner.adoption(), Adoption::Done) {
        request_stop(&phase, &lifecycle, StopCause::OwnerFailed);
    }
    // The drain quiesces the store too, so no store write follows `OwnerDone` (the lock is released after it).
    owner.drain();
    let ended = owner_end(ended, owner.store_failed());
    drop(owner.into_core());
    phase.advance(Phase::Stopping, Phase::OwnerDone);
    let _ = lifecycle.try_send(Lifecycle::OwnerGone(ended));
}

/// Start step 5: Core must have the `silence` feature, because the Hub has no timer of its own for `activity`
/// (design section 4, "The view row"; R-125).
///
/// # Errors
/// `core_feature_missing silence`.
fn require_silence(features: &Features) -> Result<(), StartFailure> {
    if features.names.contains(&Feature::Silence) {
        Ok(())
    } else {
        Err(StartFailure::new(FailCode::CoreFeatureMissing, "silence"))
    }
}

/// How the owner ended, once the drain has run: a store failure found during the drain (the quiesce writes the pending
/// terminal states) makes a clean end a failed one, so a requested stop does not exit 0 over a lost write.
fn owner_end(ended: Ended, store_failed: bool) -> Ended {
    if ended == Ended::Clean && store_failed {
        Ended::StoreFailed
    } else {
        ended
    }
}

fn own_core_until_done(
    owner: &mut Owner<Core>,
    poll: &mut Poll,
    phase: &SharedPhase,
    adopted: &SyncSender<Result<BTreeSet<SessionId>, String>>,
) {
    let wake = owner.core().wake_handle();
    let fd = wake.fd();
    if let Err(error) = poll
        .registry()
        .register(&mut SourceFd(&fd), CORE_WAKE, Interest::READABLE)
    {
        let _ = adopted.try_send(Err(format!("the Core wake: {error}")));
        return;
    }
    owner.begin_adoption();
    let mut reported = false;
    let mut events = Events::with_capacity(8);
    loop {
        if phase.stopping() {
            return;
        }
        let report = owner.turn(now());
        if !reported {
            match owner.adoption() {
                Adoption::Done => {
                    reported = true;
                    // Step 6b: a typed A26 stub (R-121 forbids an interim rule for Core `v1`). `starting` records stay as
                    // they are on disk; recovery ends, so `get_spawn` answers their durable state.
                    if let Some(store) = owner.store_mut() {
                        let _ = restore_spawns(store);
                    }
                    // The store index was loaded and checked before the owner started (`SpawnStore::open`), so the
                    // `starting` records are known here (Sol S2-HD-13).
                    let mut keep: BTreeSet<SessionId> = owner.view().keys().cloned().collect();
                    if let Some(store) = owner.store() {
                        keep.extend(
                            store
                                .starting()
                                .into_iter()
                                .map(|r| SessionId(r.session_id)),
                        );
                    }
                    let _ = adopted.try_send(Ok(keep));
                }
                Adoption::Failed(message) => {
                    let _ = adopted.try_send(Err(message.clone()));
                    return;
                }
                Adoption::NotStarted | Adoption::Running => {}
            }
        }
        if report.runnable {
            continue;
        }
        let timeout = owner
            .next_deadline()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        if let Err(error) = poll.poll(&mut events, timeout) {
            if error.kind() != std::io::ErrorKind::Interrupted {
                panic!("the owner's poll failed: {error}");
            }
        }
    }
}

/// What `io` needs for the client protocol server.
struct Client {
    bridge: Arc<Bridge>,
    owner_wake: Arc<dyn Wake>,
    formats: Arc<crate::client::formats::CommonFormats>,
    config: ServerConfig,
    stop_flush: Duration,
}

fn spawn_io(
    mut listener: UnixListener,
    socket: &std::path::Path,
    phase: &Arc<SharedPhase>,
    lifecycle: &SyncSender<Lifecycle>,
    handle_signals: bool,
    client: Client,
) -> std::io::Result<JoinHandle<()>> {
    let poll = Poll::new()?;
    let waker = Arc::new(Waker::new(poll.registry(), IO_WAKE)?);
    let io_wake: Arc<dyn Wake> = waker.clone();
    let server = Server::new(
        poll.registry().try_clone()?,
        client.bridge,
        phase.clone(),
        io_wake,
        client.owner_wake,
        client.formats,
        client.config,
    );
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;
    let signals = if handle_signals {
        let mut signals = signal_hook_mio::v1_0::Signals::new([
            signal_hook::consts::SIGTERM,
            signal_hook::consts::SIGINT,
        ])?;
        poll.registry()
            .register(&mut signals, SIGNALS, Interest::READABLE)?;
        Some(signals)
    } else {
        None
    };
    phase.set_io_waker(waker);
    let socket = socket.to_path_buf();
    let phase = phase.clone();
    let lifecycle = lifecycle.clone();
    std::thread::Builder::new()
        .name("io".into())
        .spawn(move || {
            let mut io = Io {
                poll,
                listener: Some(listener),
                signals,
                socket,
                phase,
                lifecycle,
                server,
                stop_flush: client.stop_flush,
                flush_until: None,
            };
            let ended = match catch_unwind(AssertUnwindSafe(|| io.run())) {
                Ok(()) => Ended::Clean,
                Err(_) => Ended::Panicked,
            };
            let _ = io.lifecycle.try_send(Lifecycle::IoGone(ended));
        })
}

/// The `io` thread: the listener, the signal pipe and the client protocol server (PC's `client::Server`). `io` keeps the
/// poll and the stop order; the server keeps the connections.
struct Io {
    poll: Poll,
    listener: Option<UnixListener>,
    signals: Option<signal_hook_mio::v1_0::Signals>,
    socket: PathBuf,
    phase: Arc<SharedPhase>,
    lifecycle: SyncSender<Lifecycle>,
    server: Server,
    stop_flush: Duration,
    /// Stop step 3: the end of the flush, set when `io` first sees `OwnerDone`.
    flush_until: Option<Instant>,
}

impl Io {
    fn run(&mut self) {
        let mut events = Events::with_capacity(64);
        loop {
            match self.phase.get() {
                Phase::Running | Phase::Stopping => {}
                // Step 3: write the outboxes for at most `stop_flush`, so a client that never reads cannot hold the stop.
                Phase::OwnerDone => {
                    let now = Instant::now();
                    let until = *self.flush_until.get_or_insert(now + self.stop_flush);
                    if self.server.flush() || now >= until {
                        self.remove_socket();
                    }
                }
                Phase::SocketRemoved => {}
                // Step 5: `close { hub_shutdown, lock_released: true }` where `io` can still write, then the end.
                Phase::LockReleased => {
                    self.server.close_all();
                    return;
                }
            }
            let flush = self.flush_until.filter(|_| self.listener.is_some());
            let deadline = match (self.server.next_deadline(), flush) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            let timeout = deadline.map(|d| d.saturating_duration_since(Instant::now()));
            if let Err(error) = self.poll.poll(&mut events, timeout) {
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                panic!("the io poll failed: {error}");
            }
            for event in events.iter() {
                match event.token() {
                    // The outboxes and the phase share the wake: drain at each one.
                    IO_WAKE => self.server.drain_outboxes(),
                    LISTENER => self.accept(),
                    SIGNALS => self.signals(),
                    token if token.0 >= FIRST_CLIENT => self.server.ready(token, event),
                    _ => {}
                }
            }
            let now = Instant::now();
            if self.server.next_deadline().is_some_and(|d| d <= now) {
                self.server.expire(now);
            }
        }
    }

    fn accept(&mut self) {
        let Some(listener) = &self.listener else {
            return;
        };
        while let Ok((stream, _)) = listener.accept() {
            self.server.accept(stream, Instant::now());
        }
    }

    fn signals(&mut self) {
        let Some(signals) = &mut self.signals else {
            return;
        };
        if signals.pending().next().is_some() {
            request_stop(&self.phase, &self.lifecycle, StopCause::Signal);
        }
    }

    /// Stop step 3: close the listener and remove `hub.sock`, then `SocketRemoved`.
    fn remove_socket(&mut self) {
        if let Some(mut listener) = self.listener.take() {
            let _ = self.poll.registry().deregister(&mut listener);
            drop(listener);
            let _ = fs::remove_file(&self.socket);
            if self.phase.advance(Phase::OwnerDone, Phase::SocketRemoved) {
                let _ = self.lifecycle.try_send(Lifecycle::SocketRemoved);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phase::Wake;
    use crate::roots::{Env, Platform};

    struct NoWake;

    impl Wake for NoWake {
        fn wake(&self) {}
    }

    /// `io` panic (design section 5): main starts the stop, keeps the socket and the lock until `OwnerGone`, then removes
    /// the socket and releases the lock, in that order (RT-4).
    #[test]
    fn io_panic_main_removes_socket_after_owner_then_releases_lock() {
        let tmp = tempfile::Builder::new()
            .prefix("bh-")
            .tempdir_in("/tmp")
            .unwrap();
        let roots = crate::roots::derive(&Env {
            botster_config_dir: Some(tmp.path().join("c").into()),
            botster_data_dir: Some(tmp.path().join("d").into()),
            botster_hub_socket: None,
            xdg_config_home: None,
            xdg_runtime_dir: None,
            home: PathBuf::from("/nonexistent"),
            uid: 0,
            platform: Platform::current(),
        });
        let lock = start::take_lock(&roots).unwrap();
        fs::create_dir_all(roots.socket.parent().unwrap()).unwrap();
        fs::write(&roots.socket, b"").unwrap();
        let phase = SharedPhase::new();
        let (_tx, lifecycle) = sync_channel(LIFECYCLE_CAPACITY);
        let noop = || std::thread::spawn(|| {});
        let mut daemon = Daemon {
            identity: Identity {
                hub_id: "h".into(),
                incarnation: "i".into(),
            },
            socket: roots.socket.clone(),
            bridge: Arc::new(Bridge::new(
                LaneCaps {
                    control: 1,
                    request: 1,
                    bulk: 1,
                },
                phase.clone(),
                Arc::new(NoWake),
            )),
            phase: phase.clone(),
            lifecycle,
            lock: Some(lock),
            owner: noop(),
            io: noop(),
        };
        let mut state = WaitState::default();
        daemon.on_message(&mut state, Lifecycle::IoGone(Ended::Panicked));
        assert_eq!(phase.get(), Phase::Stopping);
        assert_eq!(state.cause, Some(StopCause::IoFailed));
        assert!(
            roots.socket.exists(),
            "the socket stays until the owner is gone"
        );
        assert!(
            start::take_lock(&roots).is_err(),
            "the lock stays until the owner is gone"
        );
        // The owner sees `Stopping`, drains, and moves to `OwnerDone`.
        assert!(phase.advance(Phase::Stopping, Phase::OwnerDone));
        daemon.on_message(&mut state, Lifecycle::OwnerGone(Ended::Clean));
        assert!(!roots.socket.exists());
        assert_eq!(phase.get(), Phase::LockReleased);
        start::take_lock(&roots).unwrap();
    }

    /// Exit 0 only for a requested stop with both threads clean.
    #[test]
    fn exit_codes() {
        let exit = |cause, owner, io| Exit { cause, owner, io }.code();
        assert_eq!(exit(Some(StopCause::Signal), Ended::Clean, Ended::Clean), 0);
        assert_eq!(
            exit(Some(StopCause::Requested), Ended::Clean, Ended::Clean),
            0
        );
        assert_eq!(
            exit(Some(StopCause::Requested), Ended::Panicked, Ended::Clean),
            1
        );
        assert_eq!(
            exit(Some(StopCause::Requested), Ended::Clean, Ended::Panicked),
            1
        );
        assert_eq!(
            exit(Some(StopCause::OwnerFailed), Ended::Clean, Ended::Clean),
            1
        );
        assert_eq!(
            exit(Some(StopCause::IoFailed), Ended::Clean, Ended::Clean),
            1
        );
        assert_eq!(
            exit(Some(StopCause::StoreFailed), Ended::Clean, Ended::Clean),
            1
        );
        assert_eq!(exit(None, Ended::Clean, Ended::Clean), 1);
        // A store failure found while the requested stop quiesces: exit 1 though the cause is `Requested`.
        assert_eq!(
            exit(Some(StopCause::Requested), Ended::StoreFailed, Ended::Clean),
            1
        );
    }

    /// Start step 5: a Core without `silence` fails the start `core_feature_missing silence`.
    #[test]
    fn a_core_without_silence_fails_the_start() {
        let mut features = Features::default();
        let failure = require_silence(&features).unwrap_err();
        assert_eq!(failure.code, FailCode::CoreFeatureMissing);
        assert_eq!(failure.message, "silence");
        features.names.insert(Feature::Silence);
        assert!(require_silence(&features).is_ok());
    }

    /// A clean owner end with a store failure is `StoreFailed`; nothing else changes.
    #[test]
    fn a_store_failure_makes_a_clean_owner_end_a_failed_one() {
        assert_eq!(owner_end(Ended::Clean, true), Ended::StoreFailed);
        assert_eq!(owner_end(Ended::Clean, false), Ended::Clean);
        assert_eq!(owner_end(Ended::Panicked, true), Ended::Panicked);
    }
}
