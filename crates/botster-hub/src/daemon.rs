//! The daemon (design section 5): the start order, the owner and `io` threads, and the stop order.
//!
//! [`Daemon::start`] runs start steps 1 to 7 and returns once the Hub listens and adoption has finished, so the caller
//! writes the ready line (HC HS-0). [`Daemon::wait`] is `main`'s wait on the lifecycle channel; it returns when the stop
//! order has run to the end.

use crate::bridge::{Bridge, LaneCaps};
use crate::failure::{FailCode, StartFailure};
use crate::fsutil::ensure_dir;
use crate::owner::{Adoption, Owner, TurnCounts};
use crate::phase::{
    request_stop, Ended, Lifecycle, Phase, SharedPhase, StopCause, LIFECYCLE_CAPACITY,
};
use crate::roots::Roots;
use crate::start::{self, HubLock, Identity};
use botster_core::contract::prelude::{CoreApi, CoreLimits, Now, OpenConfig, SessionId};
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
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
            },
            core_limits: CoreLimits::default(),
            worker_path,
            handle_signals: true,
        }
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

        // Step 6: the owner thread, then `AdoptAll`.
        let phase = SharedPhase::new();
        let internal =
            |error: std::io::Error| StartFailure::new(FailCode::Internal, error.to_string());
        let owner_poll = Poll::new().map_err(internal)?;
        let owner_waker =
            Arc::new(Waker::new(owner_poll.registry(), OWNER_WAKE).map_err(internal)?);
        phase.set_owner_waker(owner_waker.clone());
        let bridge = Arc::new(Bridge::new(config.lanes, phase.clone(), owner_waker));
        let (lifecycle_tx, lifecycle) = sync_channel(LIFECYCLE_CAPACITY);
        let (adopted_tx, adopted) = sync_channel(1);
        let owner = Owner::new(
            core,
            bridge.clone(),
            phase.clone(),
            lifecycle_tx.clone(),
            config.counts,
        );
        let owner = {
            let phase = phase.clone();
            let lifecycle_tx = lifecycle_tx.clone();
            std::thread::Builder::new()
                .name("core".into())
                .spawn(move || run_owner(owner, owner_poll, phase, lifecycle_tx, adopted_tx))
                .map_err(internal)?
        };
        let rows = match adopted.recv() {
            Ok(Ok(rows)) => rows,
            Ok(Err(message)) => return Err(adoption_failed(owner, message)),
            Err(_) => return Err(adoption_failed(owner, "the owner ended first".into())),
        };
        delete_orphan_session_dirs(roots, &rows);

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
        let io = match spawn_io(
            listener,
            &roots.socket,
            &phase,
            &lifecycle_tx,
            config.handle_signals,
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
        let mut cause = None;
        let mut owner = None;
        let mut io = None;
        while owner.is_none() || io.is_none() {
            let Ok(message) = self.lifecycle.recv() else {
                break;
            };
            match message {
                Lifecycle::StopRequested(first) => cause = cause.or(Some(first)),
                Lifecycle::OwnerGone(ended) => owner = Some(ended),
                Lifecycle::SocketRemoved => self.release_lock(),
                Lifecycle::IoGone(ended) => {
                    io = Some(ended);
                    if ended == Ended::Panicked {
                        // `io` panic: main stops the owner, removes the socket itself and releases the lock.
                        self.phase.advance(Phase::Running, Phase::Stopping);
                        let _ = fs::remove_file(&self.socket);
                        self.phase.advance(Phase::OwnerDone, Phase::SocketRemoved);
                    }
                }
            }
            // A thread that ends before any stop: main moves `Running` to `Stopping` (it sends no message).
            if (owner.is_some() || io.is_some())
                && self.phase.advance(Phase::Running, Phase::Stopping)
            {
                cause = cause.or(Some(StopCause::OwnerFailed));
            }
            if owner.is_some() && io == Some(Ended::Panicked) {
                self.release_lock();
            }
        }
        self.release_lock();
        let _ = self.owner.join();
        let _ = self.io.join();
        Exit {
            cause,
            owner: owner.unwrap_or(Ended::Panicked),
            io: io.unwrap_or(Ended::Panicked),
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

/// RT-2: a session directory is deleted with its row, so a directory with no row after adoption is an orphan.
fn delete_orphan_session_dirs(roots: &Roots, rows: &BTreeSet<SessionId>) {
    let Ok(entries) = fs::read_dir(roots.data_root.join("sessions")) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let known = name
            .to_str()
            .is_some_and(|n| rows.contains(&SessionId(n.into())));
        if !known {
            let _ = fs::remove_dir_all(entry.path());
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
    let ended = match catch_unwind(AssertUnwindSafe(|| {
        owner_loop(&mut owner, &mut poll, &phase, &adopted)
    })) {
        Ok(()) => Ended::Clean,
        Err(_) => Ended::Panicked,
    };
    if ended == Ended::Panicked || !matches!(owner.adoption(), Adoption::Done) {
        request_stop(&phase, &lifecycle, StopCause::OwnerFailed);
    }
    owner.drain();
    drop(owner.into_core());
    phase.advance(Phase::Stopping, Phase::OwnerDone);
    let _ = lifecycle.try_send(Lifecycle::OwnerGone(ended));
}

fn owner_loop(
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
                    let _ = adopted.try_send(Ok(owner.view().keys().cloned().collect()));
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
            .core()
            .next_deadline()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        if let Err(error) = poll.poll(&mut events, timeout) {
            if error.kind() != std::io::ErrorKind::Interrupted {
                panic!("the owner's poll failed: {error}");
            }
        }
    }
}

fn spawn_io(
    mut listener: UnixListener,
    socket: &std::path::Path,
    phase: &Arc<SharedPhase>,
    lifecycle: &SyncSender<Lifecycle>,
    handle_signals: bool,
) -> std::io::Result<JoinHandle<()>> {
    let poll = Poll::new()?;
    let waker = Arc::new(Waker::new(poll.registry(), IO_WAKE)?);
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
            };
            let ended = match catch_unwind(AssertUnwindSafe(|| io.run())) {
                Ok(()) => Ended::Clean,
                Err(_) => Ended::Panicked,
            };
            let _ = io.lifecycle.try_send(Lifecycle::IoGone(ended));
        })
}

/// The `io` thread: the listener and the signal pipe. The client protocol server plugs in at the next step; until then a
/// connection is accepted and closed at once.
struct Io {
    poll: Poll,
    listener: Option<UnixListener>,
    signals: Option<signal_hook_mio::v1_0::Signals>,
    socket: PathBuf,
    phase: Arc<SharedPhase>,
    lifecycle: SyncSender<Lifecycle>,
}

impl Io {
    fn run(&mut self) {
        let mut events = Events::with_capacity(16);
        loop {
            match self.phase.get() {
                Phase::Running | Phase::Stopping => {}
                Phase::OwnerDone => self.remove_socket(),
                Phase::SocketRemoved => {}
                // Step 5: `close { hub_shutdown, lock_released: true }` goes to each connection here (protocol step).
                Phase::LockReleased => return,
            }
            if let Err(error) = self.poll.poll(&mut events, None) {
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                panic!("the io poll failed: {error}");
            }
            for event in events.iter() {
                match event.token() {
                    LISTENER => self.accept(),
                    SIGNALS => self.signals(),
                    _ => {}
                }
            }
        }
    }

    fn accept(&mut self) {
        let Some(listener) = &self.listener else {
            return;
        };
        // No protocol yet: each connection is closed at once (dropped).
        while let Ok((_stream, _)) = listener.accept() {}
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
