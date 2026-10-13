//! Route connect on `local-stream` (HC TB-L2): a new connection to the Hub's listener whose first and only handshake frame
//! is `{ "frame": "route_connect", route_id, route_token }` in the control framing. After it, the connection carries TS-3
//! route frames, `[u32 big-endian length][u8 type][body]`, straight to the session's worker (BP-1).
//!
//! The worker's first frame on a connected route is `attached` (type `0x01`, TS-4). A refusal is the Hub's control-framed
//! `close { reason }` (`route_token_invalid`, `route_token_used`, `busy`, `hub_shutdown`; HS-9). So the client reads one
//! frame: type `0x01` is the route, and a `close` is a typed refusal. A JSON frame starts with `{` (`0x7B`), which is in
//! the ignorable route range (A.4), so the client tells them apart by this order (`attached` first), not by the byte alone.

use crate::framing::{read_frame, write_frame, FrameError};
use crate::owner::{check_owner, OwnerError};
use botster_hub_contract::prelude::{RouteConnect, ServerFrame};
use serde::Serialize;
use std::fmt;
use std::io;
use std::os::unix::net::UnixStream;
use std::path::Path;

/// The route frame type of `attached` (HC A.4).
pub const ATTACHED: u8 = 0x01;

/// The tagged first frame of a route connection. The contract crate's `RouteConnect` has no `frame` tag, so this wraps it
/// (TB-L2; the conformance driver's `route_connect` step sends the same object).
#[derive(Debug, Serialize)]
pub struct RouteConnectFrame<'a> {
    frame: &'static str,
    #[serde(flatten)]
    connect: &'a RouteConnect,
}

impl<'a> RouteConnectFrame<'a> {
    pub fn new(connect: &'a RouteConnect) -> Self {
        RouteConnectFrame {
            frame: "route_connect",
            connect,
        }
    }
}

/// A connected route: the stream, and the `attached` frame's body (type byte included) that the client already read.
#[derive(Debug)]
pub struct Route {
    pub stream: UnixStream,
    pub attached: Vec<u8>,
}

/// Why a route did not connect.
#[derive(Debug)]
pub enum RouteError {
    Owner(OwnerError),
    Connect(io::Error),
    Frame(FrameError),
    /// The Hub refused the handshake with `close { reason }`.
    Refused(serde_json::Value),
    /// The first frame was neither `attached` nor a `close`.
    Unexpected(String),
}

impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RouteError::Owner(e) => write!(f, "{e}"),
            RouteError::Connect(e) => write!(f, "cannot connect the route: {e}"),
            RouteError::Frame(e) => write!(f, "{e}"),
            RouteError::Refused(reason) => write!(f, "the Hub refused the route: {reason}"),
            RouteError::Unexpected(what) => write!(f, "unexpected first route frame: {what}"),
        }
    }
}

impl std::error::Error for RouteError {}

/// Connects the route that `attach` reserved, as the user `uid`.
///
/// - `handshake_max_bytes` bounds the handshake frame and a `close` answer (`welcome.limits.max_frame_bytes`).
/// - `max_route_frame_bytes` bounds the `attached` frame (`route_limits.max_frame_bytes`, TS-1).
pub fn connect_route_as(
    socket: &Path,
    uid: u32,
    connect: &RouteConnect,
    handshake_max_bytes: u64,
    max_route_frame_bytes: u64,
) -> Result<Route, RouteError> {
    check_owner(socket, uid).map_err(RouteError::Owner)?;
    let mut stream = UnixStream::connect(socket).map_err(RouteError::Connect)?;
    let frame = serde_json::to_vec(&RouteConnectFrame::new(connect))
        .map_err(|e| RouteError::Unexpected(e.to_string()))?;
    write_frame(&mut stream, &frame, handshake_max_bytes).map_err(RouteError::Frame)?;
    let first = read_frame(&mut stream, handshake_max_bytes.max(max_route_frame_bytes))
        .map_err(RouteError::Frame)?;
    match first.first() {
        Some(&ATTACHED) if first.len() as u64 <= max_route_frame_bytes => Ok(Route {
            stream,
            attached: first,
        }),
        Some(&ATTACHED) => Err(RouteError::Frame(FrameError::TooLarge {
            len: first.len() as u64,
            max: max_route_frame_bytes,
        })),
        _ => match serde_json::from_slice::<ServerFrame>(&first) {
            Ok(ServerFrame::Close(close)) => Err(RouteError::Refused(
                serde_json::to_value(close.reason).unwrap_or_default(),
            )),
            Ok(other) => Err(RouteError::Unexpected(format!("{other:?}"))),
            Err(error) => Err(RouteError::Unexpected(error.to_string())),
        },
    }
}

/// [`connect_route_as`] for this process's effective uid.
pub fn connect_route(
    socket: &Path,
    connect: &RouteConnect,
    handshake_max_bytes: u64,
    max_route_frame_bytes: u64,
) -> Result<Route, RouteError> {
    connect_route_as(
        socket,
        rustix::process::geteuid().as_raw(),
        connect,
        handshake_max_bytes,
        max_route_frame_bytes,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::io::Write;
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::thread;

    fn me() -> u32 {
        rustix::process::geteuid().as_raw()
    }

    fn connect() -> RouteConnect {
        RouteConnect {
            route_id: "r1".into(),
            route_token: "t".repeat(43),
        }
    }

    /// A one-connection fake listener: it reads the handshake frame and writes `answer` (already framed) back.
    fn fake(answer: Vec<u8>) -> (tempfile::TempDir, PathBuf, thread::JoinHandle<Value>) {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hub.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let first = read_frame(&mut conn, 4096).unwrap();
            conn.write_all(&answer).unwrap();
            serde_json::from_slice(&first).unwrap()
        });
        (dir, socket, server)
    }

    fn framed(body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_frame(&mut out, body, u64::MAX).unwrap();
        out
    }

    #[test]
    fn the_handshake_is_the_tagged_route_connect_frame_in_control_framing() {
        let attached = [&[ATTACHED][..], br#"{"features":[]}"#].concat();
        let (_dir, socket, server) = fake(framed(&attached));
        let route = connect_route_as(&socket, me(), &connect(), 4096, 4096).unwrap();
        assert_eq!(route.attached, attached);
        assert_eq!(
            server.join().unwrap(),
            json!({"frame": "route_connect", "route_id": "r1", "route_token": "t".repeat(43)})
        );
    }

    #[test]
    fn a_close_after_the_handshake_is_a_typed_refusal() {
        let (_dir, socket, server) =
            fake(framed(br#"{"type":"close","reason":"route_token_used"}"#));
        match connect_route_as(&socket, me(), &connect(), 4096, 4096) {
            Err(RouteError::Refused(reason)) => assert_eq!(reason, json!("route_token_used")),
            other => panic!("{other:?}"),
        }
        server.join().unwrap();
    }

    #[test]
    fn an_attached_frame_over_the_route_bound_is_refused() {
        let attached = [&[ATTACHED][..], &[b' '; 63][..]].concat();
        let (_dir, socket, server) = fake(framed(&attached));
        assert!(matches!(
            connect_route_as(&socket, me(), &connect(), 4096, 32),
            Err(RouteError::Frame(FrameError::TooLarge { len: 64, max: 32 }))
        ));
        server.join().unwrap();
    }

    #[test]
    fn a_foreign_socket_is_refused_before_the_token_leaves_the_client() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hub.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(matches!(
            connect_route_as(&socket, me().wrapping_add(1), &connect(), 4096, 4096),
            Err(RouteError::Owner(OwnerError::ForeignOwner { .. }))
        ));
        assert!(listener.accept().is_err(), "the token was never sent");
    }
}
