//! The control connection's handshake (HC HS-1, HS-3, HS-9): the socket-owner check (RT-5), `connect`, `hello`, then
//! `welcome` or `close`.
//!
//! Before `welcome`, the client does not know `max_frame_bytes`, so the caller bounds the first server frame
//! (`handshake_max_bytes`). After `welcome`, a client uses only `welcome.limits` (HC BD-3).

use crate::framing::{read_frame, write_frame, FrameError};
use crate::owner::{check_owner, OwnerError};
use botster_hub_contract::prelude::{ClientFrame, Close, Hello, ServerFrame, Welcome};
use std::fmt;
use std::io;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

/// A control connection after `welcome`.
#[derive(Debug)]
pub struct Control {
    pub stream: UnixStream,
    pub welcome: Welcome,
    pub socket_path: PathBuf,
}

/// Why a control connection was not made.
#[derive(Debug)]
pub enum ConnectError {
    Owner(OwnerError),
    /// The connect itself failed (for example, no Hub listens). The reconnect attempts retry this (CLI-AT4).
    Connect(io::Error),
    Frame(FrameError),
    /// The Hub answered `close { reason }` (HS-2 `version_unsupported`, HS-8 `busy`, HS-9).
    Closed(Close),
    /// The first server frame did not parse, or it was neither `welcome` nor `close`.
    Unexpected(String),
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectError::Owner(e) => write!(f, "{e}"),
            ConnectError::Connect(e) => write!(f, "cannot connect to the Hub: {e}"),
            ConnectError::Frame(e) => write!(f, "{e}"),
            ConnectError::Closed(close) => {
                write!(f, "the Hub closed the connection: {:?}", close.reason)
            }
            ConnectError::Unexpected(what) => {
                write!(f, "unexpected first frame from the Hub: {what}")
            }
        }
    }
}

impl std::error::Error for ConnectError {}

/// Connects to the Hub at `socket` as the user `uid`, and does the handshake.
pub fn connect_as(
    socket: &Path,
    uid: u32,
    hello: &Hello,
    handshake_max_bytes: u64,
) -> Result<Control, ConnectError> {
    check_owner(socket, uid).map_err(ConnectError::Owner)?;
    let mut stream = UnixStream::connect(socket).map_err(ConnectError::Connect)?;
    let frame = serde_json::to_vec(&ClientFrame::Hello(hello.clone()))
        .map_err(|e| ConnectError::Unexpected(e.to_string()))?;
    write_frame(&mut stream, &frame, handshake_max_bytes).map_err(ConnectError::Frame)?;
    let body = read_frame(&mut stream, handshake_max_bytes).map_err(ConnectError::Frame)?;
    match serde_json::from_slice::<ServerFrame>(&body) {
        Ok(ServerFrame::Welcome(welcome)) => Ok(Control {
            stream,
            welcome,
            socket_path: socket.to_path_buf(),
        }),
        Ok(ServerFrame::Close(close)) => Err(ConnectError::Closed(close)),
        Ok(other) => Err(ConnectError::Unexpected(format!("{other:?}"))),
        Err(error) => Err(ConnectError::Unexpected(error.to_string())),
    }
}

/// [`connect_as`] for this process's effective uid.
pub fn connect(
    socket: &Path,
    hello: &Hello,
    handshake_max_bytes: u64,
) -> Result<Control, ConnectError> {
    connect_as(
        socket,
        rustix::process::geteuid().as_raw(),
        hello,
        handshake_max_bytes,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_hub_contract::prelude::{ClientInfo, VersionRange};
    use serde_json::{json, Value};
    use std::os::unix::net::UnixListener;
    use std::thread;

    pub(crate) fn hello() -> Hello {
        Hello {
            protocol: "botster-hub".into(),
            versions: VersionRange { min: 1, max: 1 },
            client: ClientInfo {
                name: "test".into(),
                version: "0".into(),
            },
            features: vec![],
            terminal_formats: vec!["ghostsnp/2+raw".into()],
        }
    }

    pub(crate) fn welcome_json() -> Value {
        json!({"type": "welcome", "hub_id": "h", "incarnation": "i1", "version": 1, "features": [],
            "terminal_format": "ghostsnp/2+raw",
            "limits": {"max_frame_bytes": 4096, "max_chunk_bytes": 1024, "max_outstanding": 8, "max_watches": 64,
                "route_connect_seconds": 10, "idempotency_window_seconds": 60, "cursor_idle_seconds": 60,
                "operation_retention_seconds": 3600, "max_paste_bytes": 65536, "max_file_bytes": 1048576,
                "query_client_deadline_ms": 200}})
    }

    /// A one-connection fake Hub: it reads one frame, records it, and answers `answer`.
    fn fake_hub(answer: Value) -> (tempfile::TempDir, PathBuf, thread::JoinHandle<Value>) {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hub.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let first = read_frame(&mut conn, 4096).unwrap();
            write_frame(&mut conn, &serde_json::to_vec(&answer).unwrap(), 4096).unwrap();
            serde_json::from_slice(&first).unwrap()
        });
        (dir, socket, server)
    }

    fn me() -> u32 {
        rustix::process::geteuid().as_raw()
    }

    #[test]
    fn hello_is_the_first_frame_and_welcome_completes_the_handshake() {
        let (_dir, socket, server) = fake_hub(welcome_json());
        let control = connect_as(&socket, me(), &hello(), 4096).unwrap();
        assert_eq!(control.welcome.incarnation, "i1");
        assert_eq!(control.welcome.limits.max_frame_bytes, 4096);
        let sent = server.join().unwrap();
        assert_eq!(sent["type"], "hello");
        assert_eq!(sent["protocol"], "botster-hub");
        assert_eq!(sent["terminal_formats"], json!(["ghostsnp/2+raw"]));
    }

    #[test]
    fn a_close_answer_is_a_typed_refusal() {
        let (_dir, socket, server) =
            fake_hub(json!({"type": "close", "reason": {"busy": {"retry_after_ms": 50}}}));
        match connect_as(&socket, me(), &hello(), 4096) {
            Err(ConnectError::Closed(_)) => {}
            other => panic!("{other:?}"),
        }
        server.join().unwrap();
    }

    #[test]
    fn a_foreign_socket_is_refused_before_any_connect() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hub.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        match connect_as(&socket, me().wrapping_add(1), &hello(), 4096) {
            Err(ConnectError::Owner(OwnerError::ForeignOwner { .. })) => {}
            other => panic!("{other:?}"),
        }
        assert!(listener.accept().is_err(), "no connection was made");
    }

    #[test]
    fn no_listener_is_a_connect_error() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hub.sock");
        drop(UnixListener::bind(&socket).unwrap());
        assert!(matches!(
            connect_as(&socket, me(), &hello(), 4096),
            Err(ConnectError::Connect(_))
        ));
    }
}
