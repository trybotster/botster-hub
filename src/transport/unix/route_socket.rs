//! One Unix socket per attached terminal route.
//!
//! The control socket carries requests, responses, events, and entity
//! frames. Each attached route gets its own socket for terminal frames, so a
//! client that stops reading one route stalls only that route, and the kernel
//! socket buffer is the only flow control.
//!
//! Handshake: `Attach` answers with a socket path. The Hub binds it in a
//! private directory, accepts exactly one connection within
//! [`DAEMON_HANDSHAKE_TIMEOUT`], and unlinks the path at accept. The random
//! name and the 0700 directory are the only capability; there is no Hello.
use std::fs;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};

use tokio::io::{AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::UnixListener as TokioUnixListener;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use botster_hub_client::UnixTerminalContainerHeader;

use crate::admission::budgets::{DAEMON_CLIENT_WRITE_TIMEOUT, DAEMON_HANDSHAKE_TIMEOUT};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::transport::shared::ingress::IngressStore;
use crate::transport::unix::UnixTerminalAdapterHandle;
use crate::transport::unix::mux_write::{UnixInbound, read_async_inbound};

/// Route sockets live under `/tmp`, not next to the control socket: a Unix
/// socket path is limited to about 100 bytes, and the data directory can eat
/// most of that.
const ROUTE_DIR_PARENT: &str = "/tmp";
const ROUTE_NAME_ENTROPY_BYTES: usize = 8;
const ROUTE_DIR_ENTROPY_BYTES: usize = 4;
/// Names the socket path that owns a route directory. It is not a socket, so
/// it never collides with a random 16-hex route name.
const OWNER_FILE: &str = "owner";

/// The private directory holding route sockets. Created at daemon start with
/// mode 0700, and removed at drop.
pub(crate) struct RouteSocketDir {
    path: PathBuf,
}

impl RouteSocketDir {
    /// Create the route directory for the Hub that owns `socket_path`.
    ///
    /// The name is `/tmp/br-<uid>-<64-bit hash of the socket path>`, so the
    /// next start of the same Hub finds and sweeps what a crash left behind.
    /// A hash is not an identity: the directory holds an `owner` file with
    /// the exact socket path, and an existing directory is swept only when
    /// that file matches. Any other path at the name (another Hub's directory,
    /// another user's, a symlink) is left alone and a random name is used
    /// instead, so nobody can block the start and two Hubs never remove each
    /// other's routes.
    pub(crate) fn create(socket_path: &Path) -> DaemonTransportResult<Self> {
        // SAFETY: `geteuid` has no preconditions.
        let uid = unsafe { libc::geteuid() };
        let identity = socket_path.as_os_str().as_encoded_bytes();
        let preferred =
            Path::new(ROUTE_DIR_PARENT).join(format!("br-{uid}-{:016x}", path_hash(identity)));
        if let Ok(metadata) = fs::symlink_metadata(&preferred)
            && metadata.file_type().is_dir()
            && metadata.uid() == uid
            && fs::read(preferred.join(OWNER_FILE)).is_ok_and(|owner| owner == identity)
        {
            fs::remove_dir_all(&preferred).map_err(DaemonTransportError::Io)?;
        }
        // The mode is applied at creation, so no other user can ever enter
        // the directory. The umask can only narrow it.
        match fs::DirBuilder::new().mode(0o700).create(&preferred) {
            Ok(()) => Self::claimed(preferred, uid, identity),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let mut entropy = [0_u8; ROUTE_DIR_ENTROPY_BYTES];
                getrandom::fill(&mut entropy).map_err(|_| {
                    DaemonTransportError::Io(std::io::Error::other(
                        "route directory entropy failed",
                    ))
                })?;
                let suffix: String = entropy.iter().map(|byte| format!("{byte:02x}")).collect();
                let random = Path::new(ROUTE_DIR_PARENT).join(format!("br-{uid}-r{suffix}"));
                fs::DirBuilder::new()
                    .mode(0o700)
                    .create(&random)
                    .map_err(DaemonTransportError::Io)?;
                Self::claimed(random, uid, identity)
            }
            Err(error) => Err(DaemonTransportError::Io(error)),
        }
    }

    /// Check the new directory and write its owner file.
    fn claimed(path: PathBuf, uid: u32, identity: &[u8]) -> DaemonTransportResult<Self> {
        let dir = Self::verified(path, uid)?;
        fs::write(dir.path.join(OWNER_FILE), identity).map_err(DaemonTransportError::Io)?;
        Ok(dir)
    }

    fn verified(path: PathBuf, uid: u32) -> DaemonTransportResult<Self> {
        let metadata = fs::symlink_metadata(&path).map_err(DaemonTransportError::Io)?;
        if !metadata.file_type().is_dir() || metadata.uid() != uid {
            return Err(DaemonTransportError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "route socket directory is not a directory owned by this user",
            )));
        }
        Ok(Self { path })
    }

    /// Bind a fresh, randomly named route socket. Call inside the runtime.
    pub(crate) fn bind(&self) -> DaemonTransportResult<RouteListener> {
        let mut entropy = [0_u8; ROUTE_NAME_ENTROPY_BYTES];
        getrandom::fill(&mut entropy).map_err(|_| {
            DaemonTransportError::Io(std::io::Error::other("route socket name entropy failed"))
        })?;
        let name: String = entropy.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = self.path.join(name);
        let listener =
            std::os::unix::net::UnixListener::bind(&path).map_err(DaemonTransportError::Io)?;
        // Own the path from here: an error below unlinks it.
        let guard = PathGuard(path);
        listener
            .set_nonblocking(true)
            .map_err(DaemonTransportError::Io)?;
        let listener = TokioUnixListener::from_std(listener).map_err(DaemonTransportError::Io)?;
        Ok(RouteListener {
            listener,
            path: guard,
        })
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// FNV-1a, 64-bit: stable across runs and Rust versions, unlike
/// `DefaultHasher`. It only picks a name; the `owner` file decides identity.
fn path_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

impl Drop for RouteSocketDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Unlinks a socket path when dropped.
struct PathGuard(PathBuf);

impl Drop for PathGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// A bound route socket waiting for its one client.
pub(crate) struct RouteListener {
    listener: TokioUnixListener,
    path: PathGuard,
}

impl RouteListener {
    /// The path the client connects to.
    pub(crate) fn path(&self) -> &Path {
        &self.path.0
    }
}

/// Serve one route socket until the route ends.
///
/// Waits for `written` (the Attach response is on the control socket), then
/// for the client within [`DAEMON_HANDSHAKE_TIMEOUT`]. Then runs the output
/// writer and the input reader
/// side by side. Either side ending ends the route's socket: the writer at
/// the route's close, the reader when the client leaves or breaks the
/// protocol. A client that leaves is a lost connection for this route only.
pub(crate) async fn serve_route_socket(
    listener: RouteListener,
    handle: UnixTerminalAdapterHandle,
    route: String,
    generation: u64,
    written: tokio::sync::oneshot::Receiver<()>,
) {
    // The client cannot know the path before the response reaches it, so the
    // connect window opens when the response is written. A response that
    // never gets written (its sender dropped) ends the route.
    tokio::select! {
        written = written => {
            if written.is_err() {
                handle.close_from_host();
                return;
            }
        }
        () = handle.closed() => return,
    }
    let accepted = tokio::select! {
        accepted = tokio::time::timeout(DAEMON_HANDSHAKE_TIMEOUT, listener.listener.accept()) => accepted,
        () = handle.closed() => return,
    };
    let Ok(Ok((stream, _))) = accepted else {
        // Nobody connected in time: the route has no transport.
        handle.close_from_host();
        return;
    };
    // The path served its purpose at accept.
    drop(listener);
    let (read_half, write_half) = stream.into_split();
    let write = write_route_output(write_half, &handle);
    let read = read_route_input(read_half, &handle, &route, generation);
    tokio::pin!(write, read);
    tokio::select! {
        () = &mut write => {}
        () = &mut read => {}
    }
    handle.close_from_host();
}

/// Write the route's frames in order. Ends when the route closes or the
/// socket fails; a frame partly written at close is finished within
/// [`DAEMON_CLIENT_WRITE_TIMEOUT`] so the client never sees half a frame
/// followed by more frames.
async fn write_route_output(mut writer: OwnedWriteHalf, handle: &UnixTerminalAdapterHandle) {
    loop {
        let Some(frame) = handle.snapshot_active() else {
            if handle.is_closed() {
                let _ = writer.shutdown().await;
                return;
            }
            handle.wait_for_write().await;
            continue;
        };
        let Some(header) = UnixTerminalContainerHeader::new(
            frame.route.as_str(),
            frame.generation,
            frame.stream_epoch,
            frame.frame.len(),
        ) else {
            // A Core-validated route and body always fit; a frame that does
            // not is a contract violation and ends this route only.
            handle.close();
            return;
        };
        let body = frame.frame.shared_bytes();
        let write = async {
            writer.write_all(header.as_bytes()).await?;
            writer.write_all(body).await
        };
        tokio::pin!(write);
        let written = tokio::select! {
            written = &mut write => written,
            () = handle.closed() => {
                tokio::time::timeout(DAEMON_CLIENT_WRITE_TIMEOUT, write)
                    .await
                    .unwrap_or_else(|_| {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "route socket write deadline elapsed after close",
                        ))
                    })
            }
        };
        if written.is_err() {
            return;
        }
        handle.complete_active();
    }
}

/// Read the client's input frames into the route's ingress. A full ingress
/// stops this socket's reads until Core frees room or the route closes; no
/// other route or control traffic waits.
async fn read_route_input(
    read_half: OwnedReadHalf,
    handle: &UnixTerminalAdapterHandle,
    route: &str,
    generation: u64,
) {
    let mut reader = AsyncBufReader::new(read_half);
    loop {
        match read_async_inbound(&mut reader, None).await {
            Ok(UnixInbound::Terminal(frame)) => {
                if frame.route != route || frame.generation != generation {
                    continue;
                }
                let mut bytes = frame.body;
                // A full ingress pauses this socket's reads until Core frees
                // room or the route closes; other routes are unaffected.
                while let IngressStore::Full(returned) = handle.try_push_ingress(bytes) {
                    bytes = returned;
                    handle.ingress_room().await;
                }
            }
            // Requests and Hello belong on the control socket.
            Ok(UnixInbound::Hello(_) | UnixInbound::Request { .. }) | Err(_) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    use botster_core::contract::terminal_adapter::TerminalAdapter;
    use botster_hub_client::{DaemonRouteStream, DaemonTransportError};
    use botster_terminal_protocol::{RouteId, RoutedTerminalFrame, encode_output};

    use super::*;
    use crate::transport::unix::UnixConnectionMux;

    /// Entries in a route directory other than its owner file.
    fn route_sockets_in(dir: &Path) -> usize {
        fs::read_dir(dir)
            .expect("read dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != OWNER_FILE)
            .count()
    }

    fn frame(route: &str, marker: &str) -> RoutedTerminalFrame {
        RoutedTerminalFrame::new(
            RouteId::new(route).expect("route"),
            1,
            0,
            encode_output(marker.as_bytes()).expect("output frame"),
        )
    }

    /// One bound route: its adapter, handle, and a client-side stream.
    struct Route {
        adapter: crate::transport::unix::UnixTerminalAdapter,
        handle: UnixTerminalAdapterHandle,
        client: DaemonRouteStream,
        _dir: RouteSocketDir,
    }

    async fn connected_route(name: &str) -> Route {
        let dir = RouteSocketDir::create(&unique_socket_path()).expect("route dir");
        let mux = UnixConnectionMux::new();
        let (adapter, handle) = mux.create_adapter();
        let listener = dir.bind().expect("bind route socket");
        let path = listener.path().to_owned();
        let (written_tx, written_rx) = tokio::sync::oneshot::channel();
        written_tx.send(()).expect("the response is written");
        tokio::spawn(serve_route_socket(
            listener,
            handle.clone(),
            "sub".to_string(),
            1,
            written_rx,
        ));
        let stream = UnixStream::connect(&path).expect("connect route socket");
        Route {
            adapter,
            handle,
            client: DaemonRouteStream::from_stream(stream),
            _dir: dir,
        }
    }

    fn unique_socket_path() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        PathBuf::from(format!(
            "/nonexistent/route-socket-test-{}-{}/hub.sock",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }

    #[test]
    fn the_route_directory_is_private_short_and_removed_at_drop() {
        let socket = unique_socket_path();
        let first = RouteSocketDir::create(&socket).expect("create");
        let mode = fs::metadata(first.path())
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "no access for group or others: {mode:o}");
        let path = first.path().to_owned();
        let listener_path_len = path.as_os_str().len() + 1 + 2 * ROUTE_NAME_ENTROPY_BYTES;
        assert!(
            listener_path_len < 100,
            "route socket paths stay short: {listener_path_len}"
        );
        drop(first);
        assert!(!path.exists(), "the directory goes with the Hub");
    }

    /// The name follows the socket path, so a restart finds and sweeps what
    /// a crash left behind.
    #[test]
    fn a_restart_sweeps_the_previous_runs_leftovers() {
        let socket = unique_socket_path();
        let first = RouteSocketDir::create(&socket).expect("create");
        let path = first.path().to_owned();
        fs::write(path.join("leftover"), b"x").expect("leftover");
        std::mem::forget(first);
        let second = RouteSocketDir::create(&socket).expect("recreate");
        assert_eq!(
            second.path(),
            path,
            "the name is deterministic per socket path"
        );
        assert!(
            !path.join("leftover").exists(),
            "the sweep removes leftovers"
        );
        let other = RouteSocketDir::create(&unique_socket_path()).expect("another hub");
        assert_ne!(
            other.path(),
            second.path(),
            "another socket path, another directory"
        );
    }

    /// A different socket path whose directory sits at the preferred name
    /// (a hash collision, simulated by writing another owner) is never swept:
    /// its owner file differs, so a random name takes over.
    #[test]
    fn another_hubs_directory_at_the_preferred_name_is_never_removed() {
        let socket = unique_socket_path();
        let mine = RouteSocketDir::create(&socket).expect("create");
        let path = mine.path().to_owned();
        std::mem::forget(mine);
        fs::write(path.join(OWNER_FILE), b"/some/other/hub.sock").expect("another owner");
        fs::write(path.join("live-route-socket"), b"x").expect("another hub's file");
        let second = RouteSocketDir::create(&socket).expect("start beside it");
        assert_ne!(second.path(), path, "a different owner is never reused");
        assert!(path.join("live-route-socket").exists(), "and never swept");
        drop(second);
        fs::remove_dir_all(&path).expect("clean up the simulated neighbour");
    }

    /// A path at the preferred name that this user does not own as a
    /// directory is never entered or removed; a random name takes over.
    #[test]
    fn a_squatted_preferred_name_falls_back_to_a_random_directory() {
        let socket = unique_socket_path();
        let preferred = RouteSocketDir::create(&socket).expect("create");
        let path = preferred.path().to_owned();
        drop(preferred);
        std::os::unix::fs::symlink("/nonexistent", &path).expect("squat with a symlink");
        let fallback = RouteSocketDir::create(&socket).expect("fallback");
        assert_ne!(fallback.path(), path);
        assert!(
            fs::symlink_metadata(&path)
                .expect("squatter stays")
                .file_type()
                .is_symlink(),
            "the squatter is left alone"
        );
        drop(fallback);
        fs::remove_file(&path).expect("clean up the squatter");
    }

    #[tokio::test]
    async fn a_frame_written_to_the_adapter_reaches_the_client_and_the_path_is_gone() {
        let mut route = connected_route("frame").await;
        assert_eq!(route.adapter.try_write(&frame("sub", "hello")), Ok(()));
        route
            .client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bound the read");
        let received = tokio::task::spawn_blocking(move || {
            let received = route.client.read_frame().expect("terminal frame");
            (route, received)
        })
        .await
        .expect("join");
        assert_eq!(received.1.route, "sub");
        assert_eq!(
            route_sockets_in(received.0._dir.path()),
            0,
            "the path is unlinked at accept"
        );
    }

    #[tokio::test]
    async fn closing_the_route_ends_the_socket_with_eof() {
        let route = connected_route("close").await;
        route.handle.close();
        let mut client = route.client;
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bound the read");
        let result = tokio::task::spawn_blocking(move || client.read_frame())
            .await
            .expect("join");
        assert!(matches!(
            result,
            Err(DaemonTransportError::ClientDisconnected)
        ));
    }

    #[tokio::test]
    async fn a_client_that_leaves_ends_the_route_as_a_host_close() {
        let route = connected_route("leave").await;
        drop(route.client);
        tokio::time::timeout(Duration::from_secs(5), route.handle.closed())
            .await
            .expect("the route closes when its client leaves");
        assert!(route.handle.host_closed());
    }

    #[tokio::test]
    async fn no_connection_within_the_handshake_window_closes_the_route() {
        let dir = RouteSocketDir::create(&unique_socket_path()).expect("route dir");
        let mux = UnixConnectionMux::new();
        let (_adapter, handle) = mux.create_adapter();
        let listener = dir.bind().expect("bind route socket");
        let (written_tx, written_rx) = tokio::sync::oneshot::channel();
        written_tx.send(()).expect("the response is written");
        tokio::spawn(serve_route_socket(
            listener,
            handle.clone(),
            "sub".to_string(),
            1,
            written_rx,
        ));
        tokio::time::timeout(
            DAEMON_HANDSHAKE_TIMEOUT + Duration::from_secs(3),
            handle.closed(),
        )
        .await
        .expect("an unconnected route closes");
        assert_eq!(route_sockets_in(dir.path()), 0);
    }

    /// The connect window opens when the Attach response is written, not when
    /// the route task starts: a delayed control write cannot use it up.
    #[tokio::test]
    async fn the_connect_window_opens_only_when_the_response_is_written() {
        let dir = RouteSocketDir::create(&unique_socket_path()).expect("route dir");
        let mux = UnixConnectionMux::new();
        let (_adapter, handle) = mux.create_adapter();
        let listener = dir.bind().expect("bind route socket");
        let path = listener.path().to_owned();
        let (written_tx, written_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(serve_route_socket(
            listener,
            handle.clone(),
            "sub".to_string(),
            1,
            written_rx,
        ));
        tokio::time::sleep(DAEMON_HANDSHAKE_TIMEOUT + Duration::from_secs(1)).await;
        assert!(!handle.is_closed(), "the window has not opened yet");
        written_tx.send(()).expect("the response is written");
        let stream = UnixStream::connect(&path).expect("connect after the delayed write");
        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), handle.closed())
            .await
            .expect("the client that connected then left ends the route");
    }

    /// A response that is never written (its signal is dropped) ends the route.
    #[tokio::test]
    async fn a_response_that_is_never_written_ends_the_route() {
        let dir = RouteSocketDir::create(&unique_socket_path()).expect("route dir");
        let mux = UnixConnectionMux::new();
        let (_adapter, handle) = mux.create_adapter();
        let listener = dir.bind().expect("bind route socket");
        let (written_tx, written_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(serve_route_socket(
            listener,
            handle.clone(),
            "sub".to_string(),
            1,
            written_rx,
        ));
        drop(written_tx);
        tokio::time::timeout(Duration::from_secs(5), handle.closed())
            .await
            .expect("an unwritten response releases the route");
    }
}
