//! Unix listener, socket ownership, and accept-loop admission.
//!
//! Hub proves socket ownership with a nonblocking `flock` on `<socket>.owner`
//! held for its lifetime. The lock file inode is persistent: ownership is
//! released by closing the locked descriptor, never by unlinking the path, so
//! every contender locks the same inode and exclusivity cannot split. A socket
//! path is reused only when the lock is held, the path is a socket owned by
//! this user inside a directory this user owns, and nothing accepts
//! connections on it. Any other existing path fails closed.
use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use botster_hub_client::{
    DaemonDiagnostic, DaemonEndpoint, DaemonTransportError as ClientDaemonTransportError,
    ServerFrame,
};
#[cfg(not(target_os = "macos"))]
use notify::{RecursiveMode, Watcher};
use tokio::io::BufReader as AsyncBufReader;
use tokio::net::{UnixListener as TokioUnixListener, UnixStream as TokioUnixStream};
use tokio::sync::{Semaphore, mpsc as tokio_mpsc, watch};

use crate::HubConfig;
use crate::admission::budgets::{DAEMON_HANDSHAKE_TIMEOUT, DAEMON_MAX_REJECTION_TASKS};
use crate::admission::unix_hello::daemon_hello_ack;
use crate::daemon::control::message::ControlMessage;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::transport::unix::mux_write::{
    UnixInbound, UnixInboundError, read_async_inbound, write_async_server_frame,
};

pub(crate) static NEXT_SOCKET_CLIENT_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

/// Suffix appended to the socket path for the owner lock file.
pub(crate) const SOCKET_OWNER_LOCK_SUFFIX: &str = ".owner";

/// Exclusive ownership of one Hub socket path for this process lifetime.
///
/// Dropping the lock unlocks the `flock` explicitly, then closes the
/// descriptor. A child forked by another thread holds a copy of the open file
/// description until it execs, so closing alone could leave the lock held and
/// block the next Hub. The lock file stays on disk so the next contender locks
/// the same inode.
pub(crate) struct SocketOwnerLock {
    file: fs::File,
}

impl Drop for SocketOwnerLock {
    fn drop(&mut self) {
        // The pathname is kept: an unlink here would let one contender lock
        // the orphaned inode while another locks a fresh file at the same path.
        let _ = self.file.sync_all();
        // SAFETY: `flock` takes a valid open descriptor and an integer flag.
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

impl SocketOwnerLock {
    /// Lock file path for a socket path.
    #[must_use]
    pub(crate) fn lock_path(socket_path: &Path) -> PathBuf {
        let mut name = socket_path
            .file_name()
            .map(|name| name.to_os_string())
            .unwrap_or_default();
        name.push(SOCKET_OWNER_LOCK_SUFFIX);
        socket_path.with_file_name(name)
    }
}

/// Acquire the nonblocking owner lock. Failure means another live Hub owns the path.
pub(crate) fn acquire_socket_owner_lock(
    socket_path: &Path,
) -> DaemonTransportResult<SocketOwnerLock> {
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent).map_err(DaemonTransportError::Io)?;
    }
    let path = SocketOwnerLock::lock_path(socket_path);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(DaemonTransportError::Io)?;
    // SAFETY: `flock` takes a valid open descriptor and two integer flags.
    let locked = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if locked != 0 {
        let error = std::io::Error::last_os_error();
        return Err(if error.kind() == std::io::ErrorKind::WouldBlock {
            DaemonTransportError::AlreadyRunning
        } else {
            DaemonTransportError::Io(error)
        });
    }
    Ok(SocketOwnerLock { file })
}

pub(crate) fn accept_connections(
    listener: TokioUnixListener,
    control_tx: tokio_mpsc::Sender<ControlMessage>,
    shutdown_rx: watch::Receiver<bool>,
    admission: Arc<Semaphore>,
) -> impl Future<Output = ()> + Send {
    // Register the directory watch before returning the future. Besides making
    // startup deterministic, this closes the gap between binding the initial
    // listener and beginning to poll the accept loop.
    let socket_events = SocketPathEvents::new(&listener);
    accept_connections_with_events(listener, control_tx, shutdown_rx, admission, socket_events)
}

async fn accept_connections_with_events(
    mut listener: TokioUnixListener,
    control_tx: tokio_mpsc::Sender<ControlMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
    admission: Arc<Semaphore>,
    socket_events: Result<SocketPathEvents, String>,
) {
    let watched_path = socket_events
        .as_ref()
        .ok()
        .map(|events| events.path.clone());
    let mut socket_events = match socket_events {
        Ok(events) => Some(events),
        Err(error) => {
            eprintln!("botster-hub daemon socket watch error: {error}");
            None
        }
    };
    if let Some(events) = socket_events.as_ref()
        && events.missing_at_start
    {
        let _ = rebind_listener(&mut listener, &events.path);
    }

    let rejection_admission = Arc::new(Semaphore::new(DAEMON_MAX_REJECTION_TASKS));
    let mut rejection_tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        match admission.clone().try_acquire_owned() {
                            Ok(admission_permit) => {
                                let cleanup_permit = match control_tx.clone().reserve_owned().await {
                                    Ok(permit) => permit,
                                    Err(_) => return,
                                };
                                if control_tx
                                    .send(ControlMessage::AcceptedConnection {
                                        stream,
                                        admission_permit,
                                        cleanup_permit,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(_) => {
                                let permit = tokio::select! {
                                    permit = rejection_admission.clone().acquire_owned() => {
                                        permit.expect("rejection semaphore remains owned by accept loop")
                                    }
                                    changed = shutdown_rx.changed() => {
                                        let _ = changed;
                                        return;
                                    }
                                };
                                let rejection_tx = control_tx.clone();
                                rejection_tasks.spawn(async move {
                                    let _permit = permit;
                                    reject_connection_async(stream).await;
                                    let _ = rejection_tx
                                        .send(ControlMessage::RejectedConnection)
                                        .await;
                                });
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("botster-hub daemon accept error: {error}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            event = async {
                match socket_events.as_mut() {
                    Some(events) => events.source.next().await,
                    None => std::future::pending().await,
                }
            } => {
                match event {
                    Some(Err(error)) => {
                        eprintln!("botster-hub daemon socket watch error: {error}");
                    }
                    None => {
                        eprintln!("botster-hub daemon socket watch stopped");
                        socket_events = None;
                        continue;
                    }
                    Some(Ok(())) => {}
                }
                if let Some(path) = watched_path.as_ref() {
                    let _outcome = rebind_listener(&mut listener, path);
                    #[cfg(test)]
                    if let Some(observer) = socket_events
                        .as_ref()
                        .and_then(|events| events.rebind_observer.as_ref())
                    {
                        let _ = observer.send(_outcome);
                    }
                }
            }
            changed = shutdown_rx.changed() => {
                let _ = changed;
                return;
            }
            result = rejection_tasks.join_next(), if !rejection_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("botster-hub daemon rejection task error: {error}");
                }
            }
        }
    }
}

struct SocketPathEvents {
    source: SocketPathSource,
    path: PathBuf,
    missing_at_start: bool,
    /// Test-only: every rebind attempt the accept loop makes, in order.
    #[cfg(test)]
    rebind_observer: Option<tokio_mpsc::UnboundedSender<RebindOutcome>>,
}

/// What wakes the accept loop when the socket's directory changes. The loop
/// checks the path's current state, not the event, so one wake per burst is
/// enough.
enum SocketPathSource {
    /// macOS: a kqueue `EVFILT_VNODE` watch on the parent directory. The
    /// kernel posts `NOTE_WRITE` when an entry is added, removed, or renamed,
    /// as part of that operation. FSEvents, which `notify` uses on macOS, is
    /// delivered later through fseventsd and can be dropped under churn.
    #[cfg(target_os = "macos")]
    Kqueue(DirectoryVnodeWatch),
    /// Elsewhere: `notify` (inotify on Linux, which the kernel also posts
    /// synchronously).
    #[cfg(not(target_os = "macos"))]
    Notify {
        // The watcher must remain alive for its callback to keep receiving events.
        _watcher: notify::RecommendedWatcher,
        events: tokio_mpsc::Receiver<notify::Result<notify::Event>>,
    },
    /// Test-only: changes injected by the test.
    #[cfg(test)]
    Injected(tokio_mpsc::Receiver<()>),
    /// The watch failed and stopped.
    Stopped,
}

impl SocketPathSource {
    /// Wait for the next change. `None` once the source has stopped.
    async fn next(&mut self) -> Option<Result<(), String>> {
        match self {
            #[cfg(target_os = "macos")]
            Self::Kqueue(watch) => match watch.changed().await {
                Ok(DirectoryChange::Entries) => Some(Ok(())),
                Ok(DirectoryChange::DirectoryGone) => {
                    // The watched directory itself was removed or renamed: no
                    // later change to it can be observed.
                    *self = Self::Stopped;
                    None
                }
                Err(error) => Some(Err(error.to_string())),
            },
            #[cfg(not(target_os = "macos"))]
            Self::Notify { events, .. } => events
                .recv()
                .await
                .map(|event| event.map(|_| ()).map_err(|error| error.to_string())),
            #[cfg(test)]
            Self::Injected(events) => events.recv().await.map(Ok),
            Self::Stopped => None,
        }
    }
}

impl SocketPathEvents {
    fn new(listener: &TokioUnixListener) -> Result<Self, String> {
        let path = listener
            .local_addr()
            .map_err(|error| error.to_string())?
            .as_pathname()
            .map(Path::to_path_buf)
            .ok_or_else(|| "Unix listener has no public socket path".to_string())?;
        let parent = path
            .parent()
            .ok_or_else(|| "socket path has no parent directory".to_string())?;
        let source = watch_directory(parent)?;
        let missing_at_start = !path.exists();
        Ok(Self {
            source,
            path,
            missing_at_start,
            #[cfg(test)]
            rebind_observer: None,
        })
    }

    #[cfg(test)]
    fn closed(path: PathBuf) -> Self {
        let (events_tx, events) = tokio_mpsc::channel(1);
        drop(events_tx);
        Self::injected(path, events)
    }

    #[cfg(test)]
    fn injected(path: PathBuf, events: tokio_mpsc::Receiver<()>) -> Self {
        Self {
            source: SocketPathSource::Injected(events),
            path,
            missing_at_start: false,
            rebind_observer: None,
        }
    }

    #[cfg(test)]
    fn observe_rebinds(mut self) -> (Self, tokio_mpsc::UnboundedReceiver<RebindOutcome>) {
        let (observer, outcomes) = tokio_mpsc::unbounded_channel();
        self.rebind_observer = Some(observer);
        (self, outcomes)
    }
}

#[cfg(target_os = "macos")]
fn watch_directory(parent: &Path) -> Result<SocketPathSource, String> {
    DirectoryVnodeWatch::new(parent)
        .map(SocketPathSource::Kqueue)
        .map_err(|error| error.to_string())
}

#[cfg(not(target_os = "macos"))]
fn watch_directory(parent: &Path) -> Result<SocketPathSource, String> {
    let (events_tx, events) = tokio_mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |event| {
        // Coalesce bursts: one queued wake is enough.
        let _ = events_tx.try_send(event);
    })
    .map_err(|error| error.to_string())?;
    watcher
        .watch(parent, RecursiveMode::NonRecursive)
        .map_err(|error| error.to_string())?;
    Ok(SocketPathSource::Notify {
        _watcher: watcher,
        events,
    })
}

/// A change the kernel reported for the watched directory.
#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectoryChange {
    /// An entry was added, removed, or renamed.
    Entries,
    /// The directory itself was deleted, renamed, or revoked.
    DirectoryGone,
}

/// A kqueue `EVFILT_VNODE` watch on one directory, polled by Tokio's reactor
/// through the kqueue descriptor itself: no thread and no timer.
///
/// The kernel registration happens at construction, which may run outside a
/// Tokio runtime (the accept loop's future is built synchronously), so no
/// change after construction is missed. The reactor registration waits for
/// the first `changed`, which runs inside the loop; changes queue in the
/// kqueue until then.
#[cfg(target_os = "macos")]
struct DirectoryVnodeWatch {
    queue: VnodeQueue,
    // Held open for the registration's lifetime; O_EVTONLY does not block
    // the volume from unmounting.
    _directory: std::os::fd::OwnedFd,
}

#[cfg(target_os = "macos")]
enum VnodeQueue {
    Unregistered(Option<std::os::fd::OwnedFd>),
    Registered(tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>),
}

#[cfg(target_os = "macos")]
const DIRECTORY_GONE: u32 = libc::NOTE_DELETE | libc::NOTE_RENAME | libc::NOTE_REVOKE;

#[cfg(target_os = "macos")]
impl DirectoryVnodeWatch {
    fn new(directory: &Path) -> std::io::Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        use std::os::unix::ffi::OsStrExt;

        let path = std::ffi::CString::new(directory.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "socket directory path contains a NUL byte",
            )
        })?;
        // SAFETY: `path` is a valid NUL-terminated string for the call.
        let raw = unsafe { libc::open(path.as_ptr(), libc::O_EVTONLY | libc::O_CLOEXEC) };
        if raw < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `raw` is a descriptor this call just opened and owns.
        let directory = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: kqueue takes no arguments; a negative result is an error.
        let raw = unsafe { libc::kqueue() };
        if raw < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `raw` is a kqueue descriptor this call just created.
        // A kqueue is not inherited across fork, so no CLOEXEC is needed.
        let queue = unsafe { OwnedFd::from_raw_fd(raw) };
        let change = libc::kevent {
            ident: directory.as_raw_fd() as libc::uintptr_t,
            filter: libc::EVFILT_VNODE,
            // EV_CLEAR: one report per burst of changes, reset when read.
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: libc::NOTE_WRITE | DIRECTORY_GONE,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: one valid change record in, no event buffer out.
        let result = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            queue: VnodeQueue::Unregistered(Some(queue)),
            _directory: directory,
        })
    }

    /// Wait for the kernel's next report on the directory. Must run inside a
    /// Tokio runtime.
    async fn changed(&mut self) -> std::io::Result<DirectoryChange> {
        use std::os::fd::AsRawFd;

        if let VnodeQueue::Unregistered(queue) = &mut self.queue {
            let queue = queue
                .take()
                .expect("an unregistered queue retains its descriptor");
            self.queue = VnodeQueue::Registered(tokio::io::unix::AsyncFd::with_interest(
                queue,
                tokio::io::Interest::READABLE,
            )?);
        }
        let VnodeQueue::Registered(queue) = &self.queue else {
            unreachable!("the queue was registered above");
        };
        loop {
            let mut ready = queue.readable().await?;
            let mut event = libc::kevent {
                ident: 0,
                filter: 0,
                flags: 0,
                fflags: 0,
                data: 0,
                udata: std::ptr::null_mut(),
            };
            let immediately = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: no changes in, one event buffer out, and a zero
            // timeout so the call never blocks the reactor thread.
            let count = unsafe {
                libc::kevent(
                    queue.get_ref().as_raw_fd(),
                    std::ptr::null(),
                    0,
                    &mut event,
                    1,
                    &immediately,
                )
            };
            if count < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if count == 0 {
                // Nothing pending: the readiness was spurious or already read.
                ready.clear_ready();
                continue;
            }
            if event.flags & libc::EV_ERROR != 0 {
                return Err(std::io::Error::from_raw_os_error(event.data as i32));
            }
            if event.fflags & DIRECTORY_GONE != 0 {
                return Ok(DirectoryChange::DirectoryGone);
            }
            return Ok(DirectoryChange::Entries);
        }
    }
}

/// The result of one rebind attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RebindOutcome {
    /// The path exists, so nothing was rebound.
    PathPresent,
    /// The listener was rebound to the path.
    Rebound,
    /// The bind failed; the error was logged.
    Failed,
}

fn rebind_listener(listener: &mut TokioUnixListener, path: &Path) -> RebindOutcome {
    if path.exists() {
        return RebindOutcome::PathPresent;
    }
    match TokioUnixListener::bind(path) {
        Ok(rebound) => {
            *listener = rebound;
            RebindOutcome::Rebound
        }
        Err(error) => {
            eprintln!("botster-hub daemon socket rebind error: {error}");
            RebindOutcome::Failed
        }
    }
}

pub(crate) async fn reject_connection_async(stream: TokioUnixStream) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = AsyncBufReader::new(read_half);
    match read_async_inbound(&mut reader, Some(DAEMON_HANDSHAKE_TIMEOUT)).await {
        Ok(UnixInbound::Hello(_)) => {}
        Ok(_) | Err(UnixInboundError::Protocol(_)) | Err(UnixInboundError::Transport(_)) => {
            return;
        }
    }
    let ack = daemon_hello_ack(vec![DaemonDiagnostic::backpressure(
        "daemon_connection_admission",
        "daemon connection capacity reached",
    )]);
    let _ = write_async_server_frame(&mut write_half, &ServerFrame::HelloAck { ack }).await;
}

pub(crate) fn socket_path(config: &HubConfig) -> DaemonTransportResult<PathBuf> {
    config
        .transports
        .local_socket
        .as_ref()
        .map(|binding| binding.path.clone())
        .ok_or(DaemonTransportError::MissingSocketBinding)
}

pub(crate) fn daemon_endpoint(config: &HubConfig) -> DaemonTransportResult<DaemonEndpoint> {
    socket_path(config).map(DaemonEndpoint::new)
}

/// Validate and clear the socket path while the owner lock is held.
///
/// The lock proves no other Hub of this user is live, not that an arbitrary
/// existing path belongs to Botster. The path is removed only when it is a
/// socket owned by this user, inside a directory this user owns, and nothing
/// accepts on it. Everything else fails closed and unlinks nothing.
pub(crate) fn prepare_socket_path(
    path: &Path,
    _owner: &SocketOwnerLock,
) -> DaemonTransportResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| DaemonTransportError::Protocol("socket path has no parent directory"))?;
    fs::create_dir_all(parent).map_err(DaemonTransportError::Io)?;
    let uid = current_uid();
    let parent_metadata = fs::symlink_metadata(parent).map_err(DaemonTransportError::Io)?;
    if !parent_metadata.file_type().is_dir() || parent_metadata.uid() != uid {
        return Err(DaemonTransportError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "socket directory is not a directory owned by this user",
        )));
    }
    let existing = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(DaemonTransportError::Io(error)),
    };
    if !existing.file_type().is_socket() {
        return Err(DaemonTransportError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "socket path exists and is not a socket",
        )));
    }
    if existing.uid() != uid {
        return Err(DaemonTransportError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "socket path is owned by another user",
        )));
    }
    match UnixStream::connect(path) {
        Ok(_) => Err(DaemonTransportError::AlreadyRunning),
        Err(_) => {
            fs::remove_file(path).map_err(DaemonTransportError::Io)?;
            Ok(())
        }
    }
}

pub(crate) fn cleanup_socket_path(path: &Path, owner: SocketOwnerLock) {
    let _ = fs::remove_file(path);
    drop(owner);
}

fn current_uid() -> u32 {
    // SAFETY: `geteuid` has no preconditions.
    unsafe { libc::geteuid() }
}

impl From<UnixInboundError> for DaemonTransportError {
    fn from(error: UnixInboundError) -> Self {
        match error {
            UnixInboundError::Transport(error) => error.into(),
            UnixInboundError::Protocol(code) => Self::Protocol(code.as_str()),
        }
    }
}

impl From<ClientDaemonTransportError> for UnixInboundError {
    fn from(error: ClientDaemonTransportError) -> Self {
        Self::Transport(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_released_socket_owner_does_not_block_while_a_descriptor_copy_survives() {
        let socket = temp_socket_path("r");
        let owner = acquire_socket_owner_lock(&socket).expect("owner");
        // The same open file description a forked child inherits.
        let inherited = owner
            .file
            .try_clone()
            .expect("copy of the locked description");
        drop(owner);
        let reopened = acquire_socket_owner_lock(&socket);
        drop(inherited);
        assert!(reopened.is_ok(), "{:?}", reopened.err());
        let _ = fs::remove_file(SocketOwnerLock::lock_path(&socket));
    }

    fn temp_socket_path(tag: &str) -> PathBuf {
        static NEXT_TEST_SOCKET: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let tag = tag.chars().next().unwrap_or('x');
        std::env::temp_dir().join(format!(
            "bhl-{tag}-{}-{}-{}.sock",
            std::process::id(),
            NEXT_TEST_SOCKET.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }

    #[test]
    fn owner_lock_is_exclusive_and_released_on_drop() {
        let socket = temp_socket_path("lock");
        let first = acquire_socket_owner_lock(&socket).expect("first lock");
        assert!(SocketOwnerLock::lock_path(&socket).exists());
        assert!(matches!(
            acquire_socket_owner_lock(&socket),
            Err(DaemonTransportError::AlreadyRunning)
        ));
        drop(first);
        let second = acquire_socket_owner_lock(&socket).expect("lock after release");
        let lock_path = SocketOwnerLock::lock_path(&socket);
        drop(second);
        assert!(
            lock_path.exists(),
            "the lock inode stays on disk so contenders share it"
        );
        let _ = fs::remove_file(&lock_path);
    }

    #[test]
    fn contender_holding_the_original_inode_excludes_a_later_contender() {
        let socket = temp_socket_path("split");
        let owner = acquire_socket_owner_lock(&socket).expect("owner lock");
        // A contender opens the lock file while the owner still holds it.
        let early = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(SocketOwnerLock::lock_path(&socket))
            .expect("open the owner's lock inode");
        // SAFETY: `flock` on an open descriptor with integer flags.
        assert_ne!(
            unsafe { libc::flock(early.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0,
            "the owner still holds the lock"
        );
        drop(owner);
        // The early contender now takes the same inode.
        assert_eq!(
            unsafe { libc::flock(early.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0,
            "the released inode is lockable by the early contender"
        );
        // A later contender must see that lock, not create a second inode.
        assert!(
            matches!(
                acquire_socket_owner_lock(&socket),
                Err(DaemonTransportError::AlreadyRunning)
            ),
            "a later contender must contend on the same inode"
        );
        drop(early);
        let later = acquire_socket_owner_lock(&socket).expect("lock after the early contender");
        let lock_path = SocketOwnerLock::lock_path(&socket);
        drop(later);
        let _ = fs::remove_file(lock_path);
    }

    #[test]
    fn prepare_removes_only_a_stale_socket_owned_by_this_user() {
        let socket = temp_socket_path("stale");
        let owner = acquire_socket_owner_lock(&socket).expect("lock");
        prepare_socket_path(&socket, &owner).expect("absent path is fine");
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind");
        drop(listener);
        assert!(socket.exists());
        prepare_socket_path(&socket, &owner).expect("stale socket is removed");
        assert!(!socket.exists());
        drop(owner);
    }

    #[test]
    fn prepare_fails_closed_for_a_live_listener_and_a_non_socket_path() {
        let socket = temp_socket_path("live");
        let owner = acquire_socket_owner_lock(&socket).expect("lock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind");
        assert!(matches!(
            prepare_socket_path(&socket, &owner),
            Err(DaemonTransportError::AlreadyRunning)
        ));
        assert!(socket.exists(), "a live listener is never unlinked");
        drop(listener);
        let _ = fs::remove_file(&socket);

        fs::write(&socket, b"not a socket").expect("regular file");
        let error = prepare_socket_path(&socket, &owner).expect_err("regular file fails closed");
        assert!(matches!(error, DaemonTransportError::Io(_)));
        assert!(socket.exists(), "an unrelated path is never unlinked");
        let _ = fs::remove_file(&socket);
        drop(owner);
    }

    /// The platform watch (kqueue on macOS) reports the unlink of a live
    /// socket, and the accept loop rebinds with no transport traffic. The
    /// rebind is signalled by the loop itself; the deadline only bounds a
    /// lost kernel event.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unlinking_live_socket_rebinds_without_transport_traffic() {
        let socket = temp_socket_path("event-rebind");
        let owner = acquire_socket_owner_lock(&socket).expect("lock");
        prepare_socket_path(&socket, &owner).expect("prepare");
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let listener = TokioUnixListener::from_std(listener).expect("Tokio listener");
        let (control_tx, _control_rx) = tokio_mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // The watch is registered before the unlink, so no control message,
        // connection, or terminal frame is needed to prompt the rebind.
        let (events, mut rebinds) = SocketPathEvents::new(&listener)
            .expect("watch the socket directory")
            .observe_rebinds();
        fs::remove_file(&socket).expect("unlink live socket");
        let accept_task = tokio::spawn(accept_connections_with_events(
            listener,
            control_tx,
            shutdown_rx,
            Arc::new(Semaphore::new(1)),
            Ok(events),
        ));

        // timer: deadline — bounds a lost kernel event; the rebind is signalled.
        let outcome = tokio::time::timeout(Duration::from_secs(5), rebinds.recv())
            .await
            .expect("the directory change should reach the accept loop")
            .expect("the accept loop reports its rebinds");
        assert_eq!(outcome, RebindOutcome::Rebound);
        assert!(
            fs::symlink_metadata(&socket).is_ok_and(|metadata| metadata.file_type().is_socket())
        );
        assert!(matches!(
            acquire_socket_owner_lock(&socket),
            Err(DaemonTransportError::AlreadyRunning)
        ));

        shutdown_tx.send(true).expect("signal shutdown");
        tokio::time::timeout(Duration::from_secs(1), accept_task)
            .await
            .expect("accept loop should stop")
            .expect("accept task should not panic");
        cleanup_socket_path(&socket, owner);
        let _ = fs::remove_file(SocketOwnerLock::lock_path(&socket));
    }

    /// One injected change drives one rebind, reported to the observer; a
    /// later change with the socket present rebinds nothing.
    #[tokio::test]
    async fn an_injected_change_rebinds_the_unlinked_socket() {
        let socket = temp_socket_path("injected-rebind");
        let owner = acquire_socket_owner_lock(&socket).expect("lock");
        prepare_socket_path(&socket, &owner).expect("prepare");
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let listener = TokioUnixListener::from_std(listener).expect("Tokio listener");
        let (control_tx, _control_rx) = tokio_mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (inject, injected) = tokio_mpsc::channel(1);
        let (events, mut rebinds) =
            SocketPathEvents::injected(socket.clone(), injected).observe_rebinds();
        let accept_task = tokio::spawn(accept_connections_with_events(
            listener,
            control_tx,
            shutdown_rx,
            Arc::new(Semaphore::new(1)),
            Ok(events),
        ));

        fs::remove_file(&socket).expect("unlink live socket");
        inject.send(()).await.expect("inject the change");
        // timer: deadline — bounds a broken accept loop; the rebind is signalled.
        let outcome = tokio::time::timeout(Duration::from_secs(5), rebinds.recv())
            .await
            .expect("the injected change reaches the accept loop");
        assert_eq!(outcome, Some(RebindOutcome::Rebound));
        assert!(
            fs::symlink_metadata(&socket).is_ok_and(|metadata| metadata.file_type().is_socket())
        );
        inject.send(()).await.expect("inject a second change");
        // timer: deadline — as above.
        let outcome = tokio::time::timeout(Duration::from_secs(5), rebinds.recv())
            .await
            .expect("the second change reaches the accept loop");
        assert_eq!(outcome, Some(RebindOutcome::PathPresent));

        shutdown_tx.send(true).expect("signal shutdown");
        tokio::time::timeout(Duration::from_secs(1), accept_task)
            .await
            .expect("accept loop should stop")
            .expect("accept task should not panic");
        cleanup_socket_path(&socket, owner);
        let _ = fs::remove_file(SocketOwnerLock::lock_path(&socket));
    }

    /// The accept loop's future is built synchronously, sometimes outside
    /// any Tokio runtime. The watch must build there, and a change made
    /// before the reactor registers must still be reported.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_vnode_watch_built_outside_a_runtime_reports_an_earlier_change() {
        let directory = temp_socket_path("outside").with_extension("d");
        fs::create_dir(&directory).expect("create directory");
        let entry = directory.join("entry");
        fs::write(&entry, b"x").expect("create entry");
        let mut watch = DirectoryVnodeWatch::new(&directory).expect("watch outside a runtime");
        fs::remove_file(&entry).expect("remove entry before any runtime exists");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let change = runtime.block_on(async {
            // timer: deadline — bounds a lost kernel event.
            tokio::time::timeout(Duration::from_secs(5), watch.changed())
                .await
                .expect("the queued unlink is reported")
                .expect("the watch reads its event")
        });
        assert_eq!(change, DirectoryChange::Entries);
        fs::remove_dir(&directory).expect("remove directory");
    }

    /// The kernel reports an entry removal, then the directory's own removal,
    /// to the vnode watch.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn the_directory_vnode_watch_reports_an_unlink_then_the_directory_gone() {
        let directory = temp_socket_path("vnode").with_extension("d");
        fs::create_dir(&directory).expect("create directory");
        let entry = directory.join("entry");
        fs::write(&entry, b"x").expect("create entry");
        let mut watch = DirectoryVnodeWatch::new(&directory).expect("watch directory");

        fs::remove_file(&entry).expect("remove entry");
        // timer: deadline — bounds a missing kernel event.
        let change = tokio::time::timeout(Duration::from_secs(5), watch.changed())
            .await
            .expect("the unlink is reported")
            .expect("the watch reads its event");
        assert_eq!(change, DirectoryChange::Entries);

        fs::remove_dir(&directory).expect("remove directory");
        // timer: deadline — as above.
        let change = tokio::time::timeout(Duration::from_secs(5), watch.changed())
            .await
            .expect("the directory removal is reported")
            .expect("the watch reads its event");
        assert_eq!(change, DirectoryChange::DirectoryGone);
    }

    async fn assert_degraded_watch_still_accepts(
        socket: PathBuf,
        owner: SocketOwnerLock,
        listener: TokioUnixListener,
        socket_events: Result<SocketPathEvents, String>,
    ) {
        let (control_tx, mut control_rx) = tokio_mpsc::channel(2);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let accept_task = tokio::spawn(accept_connections_with_events(
            listener,
            control_tx,
            shutdown_rx,
            Arc::new(Semaphore::new(1)),
            socket_events,
        ));

        let client = TokioUnixStream::connect(&socket)
            .await
            .expect("connect to degraded listener");
        let message = tokio::time::timeout(Duration::from_secs(2), control_rx.recv())
            .await
            .expect("degraded listener should accept promptly")
            .expect("control channel remains open");
        let ControlMessage::AcceptedConnection {
            stream,
            admission_permit,
            cleanup_permit,
        } = message
        else {
            panic!("degraded listener returned an unexpected control message");
        };
        drop(stream);
        drop(admission_permit);
        drop(cleanup_permit);
        drop(client);

        shutdown_tx.send(true).expect("signal shutdown");
        tokio::time::timeout(Duration::from_secs(1), accept_task)
            .await
            .expect("degraded accept loop should stop without spinning")
            .expect("degraded accept loop should not panic");
        cleanup_socket_path(&socket, owner);
        let _ = fs::remove_file(SocketOwnerLock::lock_path(&socket));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_registration_failure_keeps_accepting_connections() {
        let socket = temp_socket_path("watch-registration-failure");
        let owner = acquire_socket_owner_lock(&socket).expect("lock");
        prepare_socket_path(&socket, &owner).expect("prepare");
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let listener = TokioUnixListener::from_std(listener).expect("Tokio listener");

        assert_degraded_watch_still_accepts(
            socket,
            owner,
            listener,
            Err("injected watch registration failure".to_string()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_watch_channel_keeps_accepting_without_spinning() {
        let socket = temp_socket_path("watch-channel-closed");
        let owner = acquire_socket_owner_lock(&socket).expect("lock");
        prepare_socket_path(&socket, &owner).expect("prepare");
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let listener = TokioUnixListener::from_std(listener).expect("Tokio listener");
        let socket_events = SocketPathEvents::closed(socket.clone());

        assert_degraded_watch_still_accepts(socket, owner, listener, Ok(socket_events)).await;
    }
}
