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

const ROUTE_DIR_SUFFIX: &str = ".routes";
const ROUTE_NAME_ENTROPY_BYTES: usize = 8;

/// The private directory holding route sockets. Created fresh at daemon
/// start with mode 0700 and removed at drop.
pub(crate) struct RouteSocketDir {
    path: PathBuf,
}

impl RouteSocketDir {
    /// Create `<socket path>.routes`, discarding what a crashed Hub left.
    pub(crate) fn create(socket_path: &Path) -> DaemonTransportResult<Self> {
        let mut name = socket_path
            .file_name()
            .ok_or(DaemonTransportError::Protocol(
                "socket path has no file name",
            ))?
            .to_os_string();
        name.push(ROUTE_DIR_SUFFIX);
        let path = socket_path.with_file_name(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                // SAFETY: `geteuid` has no preconditions.
                let uid = unsafe { libc::geteuid() };
                if !metadata.file_type().is_dir() || metadata.uid() != uid {
                    return Err(DaemonTransportError::Io(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "route socket directory is not a directory owned by this user",
                    )));
                }
                fs::remove_dir_all(&path).map_err(DaemonTransportError::Io)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(DaemonTransportError::Io(error)),
        }
        // The mode is applied at creation, so no other user can ever enter
        // the directory. The umask can only narrow it.
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(DaemonTransportError::Io)?;
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
/// Waits for the client, then runs the output writer and the input reader
/// side by side. Either side ending ends the route's socket: the writer at
/// the route's close, the reader when the client leaves or breaks the
/// protocol. A client that leaves is a lost connection for this route only.
pub(crate) async fn serve_route_socket(
    listener: RouteListener,
    handle: UnixTerminalAdapterHandle,
    route: String,
    generation: u64,
) {
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
                loop {
                    match handle.try_push_ingress(bytes) {
                        IngressStore::Full(returned) => {
                            bytes = returned;
                            handle.ingress_room().await;
                        }
                        IngressStore::Stored | IngressStore::Closed | IngressStore::Malformed => {
                            break;
                        }
                    }
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

    fn frame(route: &str, marker: &str) -> RoutedTerminalFrame {
        RoutedTerminalFrame::new(
            RouteId::new(route).expect("route"),
            1,
            0,
            encode_output(marker.as_bytes()).expect("output frame"),
        )
    }

    fn unique_socket_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("bsr-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("test dir");
        dir.join("hub.sock")
    }

    /// One bound route: its adapter, handle, and a client-side stream.
    struct Route {
        adapter: crate::transport::unix::UnixTerminalAdapter,
        handle: UnixTerminalAdapterHandle,
        client: DaemonRouteStream,
        _dir: RouteSocketDir,
    }

    async fn connected_route(name: &str) -> Route {
        let dir = RouteSocketDir::create(&unique_socket_path(name)).expect("route dir");
        let mux = UnixConnectionMux::new();
        let (adapter, handle) = mux.create_adapter();
        let listener = dir.bind().expect("bind route socket");
        let path = listener.path().to_owned();
        tokio::spawn(serve_route_socket(
            listener,
            handle.clone(),
            "sub".to_string(),
            1,
        ));
        let stream = UnixStream::connect(&path).expect("connect route socket");
        Route {
            adapter,
            handle,
            client: DaemonRouteStream::from_stream(stream),
            _dir: dir,
        }
    }

    #[test]
    fn the_route_directory_is_private_and_recreated_fresh() {
        let socket = unique_socket_path("dir");
        let dir = RouteSocketDir::create(&socket).expect("create");
        let mode = fs::metadata(dir.path())
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "no access for group or others: {mode:o}");
        fs::write(dir.path().join("stale"), b"x").expect("leftover");
        let path = dir.path().to_owned();
        std::mem::forget(dir);
        let fresh = RouteSocketDir::create(&socket).expect("recreate");
        assert_eq!(fresh.path(), path);
        assert!(!path.join("stale").exists(), "a crashed Hub's leftovers go");
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
        let entries = fs::read_dir(received.0._dir.path())
            .expect("read dir")
            .count();
        assert_eq!(entries, 0, "the path is unlinked at accept");
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
        let dir = RouteSocketDir::create(&unique_socket_path("late")).expect("route dir");
        let mux = UnixConnectionMux::new();
        let (_adapter, handle) = mux.create_adapter();
        let listener = dir.bind().expect("bind route socket");
        tokio::spawn(serve_route_socket(
            listener,
            handle.clone(),
            "sub".to_string(),
            1,
        ));
        tokio::time::timeout(
            DAEMON_HANDSHAKE_TIMEOUT + Duration::from_secs(3),
            handle.closed(),
        )
        .await
        .expect("an unconnected route closes");
        assert_eq!(fs::read_dir(dir.path()).expect("read dir").count(), 0);
    }
}
