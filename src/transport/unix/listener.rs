//! Unix listener, socket ownership, and accept-loop admission.
//!
//! Hub proves socket ownership with a nonblocking `flock` on `<socket>.owner`
//! held for its lifetime. A socket path is reused only when the lock is held,
//! the path is a socket owned by this user inside a directory this user owns,
//! and nothing accepts connections on it. Any other existing path fails closed.
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
/// Dropping the lock releases the `flock` and removes the lock file.
pub(crate) struct SocketOwnerLock {
    file: fs::File,
    path: PathBuf,
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

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SocketOwnerLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        // Closing the file releases the lock.
        let _ = self.file.sync_all();
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
    Ok(SocketOwnerLock { file, path })
}

pub(crate) async fn accept_connections(
    mut listener: TokioUnixListener,
    control_tx: tokio_mpsc::Sender<ControlMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
    admission: Arc<Semaphore>,
    mut rebind_rx: tokio_mpsc::Receiver<PathBuf>,
) {
    let rejection_admission = Arc::new(Semaphore::new(DAEMON_MAX_REJECTION_TASKS));
    let mut rejection_tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        match admission.clone().try_acquire_owned() {
                            Ok(admission_permit) => {
                                if control_tx
                                    .send(ControlMessage::AcceptedConnection {
                                        stream,
                                        admission_permit,
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
            rebind = rebind_rx.recv() => {
                let Some(path) = rebind else {
                    return;
                };
                if path.exists() {
                    continue;
                }
                match TokioUnixListener::bind(&path) {
                    Ok(rebound) => {
                        listener = rebound;
                    }
                    Err(error) => {
                        eprintln!("botster-hub daemon socket rebind error: {error}");
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

/// Ask the accept loop to recreate a listener when the public path is gone.
pub(crate) fn rebind_missing_socket_path(rebind_tx: &tokio_mpsc::Sender<PathBuf>, path: &Path) {
    let _ = rebind_tx.try_send(path.to_path_buf());
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

    fn temp_socket_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "botster-hub-listener-{tag}-{}-{}.sock",
            std::process::id(),
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
        assert!(first.path().exists());
        assert!(matches!(
            acquire_socket_owner_lock(&socket),
            Err(DaemonTransportError::AlreadyRunning)
        ));
        drop(first);
        let second = acquire_socket_owner_lock(&socket).expect("lock after release");
        drop(second);
        assert!(!SocketOwnerLock::lock_path(&socket).exists());
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
}
