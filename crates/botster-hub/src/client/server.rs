//! The client protocol server on the `io` thread (design sections 1, 2.3 and 3; P5's `io` loop calls it).
//!
//! The `io` loop keeps the poll, the listener, the signals and the stop order. It calls:
//! - [`Server::accept`] for each accepted stream;
//! - [`Server::ready`] for each event of a client token (at or above [`FIRST_CLIENT`]);
//! - [`Server::drain_outboxes`] on the `io` wake, when an owner answer is queued;
//! - [`Server::expire`] when [`Server::next_deadline`] passes (handshake deadlines);
//! - [`Server::flush`] at stop step 3, until it returns true or `stop_flush_ms` passes;
//! - [`Server::close_all`] at stop step 5, after the lock is released.
//!
//! A connection is **unclassified** until its first frame (`hello` or `route_connect`), read exactly with no read-ahead.
//! A route connection leaves `io` with its `route_connect` request; the owner's answer comes back through the
//! connection's outbox, and `io` writes the close of a refused one. Nothing here blocks.

use crate::bridge::{Answer, Bridge, OutItem, Outbox, OutboxCaps, Refused, Request};
use crate::client::classify::{classify, First};
use crate::client::formats::CommonFormats;
use crate::client::handoff::{Caller, RouteConnectAnswer, RouteConnectRequest};
use crate::client::peer::peer_uid;
use crate::client::route::{close_frame, MAX_ROUTE_CONNECT_CLOSE};
use crate::client::wire::{framed, FrameReader, OutBuf, ReadError};
use crate::phase::{SharedPhase, Wake};
use botster_hub_contract::prelude::{
    BusyData, Category, ClientFrame, CloseReason, Error, ErrorCode, Hello, HubShutdownData, Limits,
    Outcome, ProtocolErrorData, Req, Res, ServerFrame, VersionUnsupportedData, Welcome, PROTOCOL,
};
use botster_route_codec::prelude::ProtocolErrorCode;
use mio::event::Event;
use mio::net::UnixStream;
use mio::{Interest, Registry, Token};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The first token of a client connection. The `io` loop keeps the tokens below it (listener, signals, wake).
pub const FIRST_CLIENT: usize = 16;

/// The items that `io` takes from one outbox per drain.
const TAKE: usize = 64;

/// The server's parameters (small test values allowed; HP LM-5 rule).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub hub_id: String,
    pub incarnation: String,
    /// `hub_current`. The Hub supports `[hub_current - 1, hub_current]`, and never version 0 (HS-2).
    pub version: u16,
    /// The features that the Hub offers (HS-3).
    pub features: Vec<String>,
    /// `welcome.limits` (HS-3). `max_frame_bytes` bounds every control frame, the handshake included.
    pub limits: Limits,
    /// Open connections, every state (HS-8).
    pub max_connections: usize,
    /// Connections that have not sent their first frame.
    pub max_unclassified: usize,
    /// The time an unclassified connection has for its first frame.
    pub handshake_deadline: Duration,
    /// A connection's outgoing byte bound; at least [`MAX_ROUTE_CONNECT_CLOSE`].
    pub out_bytes: usize,
    /// A control connection's outbox (`replies` is `max_outstanding`, EN-4).
    pub outbox: OutboxCaps,
    /// `busy.retry_after_ms` of a refused connection or route connect.
    pub busy_retry_ms: u64,
    /// The Hub's own uid: a peer with another uid is refused `auth_failed` (TB-L3).
    pub uid: u32,
}

/// The numbers that this layer owns (HC CF-2 `[limits]`; the Hub's defaults, no HC number).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolConfig {
    /// `hub_current` (HS-2).
    pub version: u16,
    /// The features that the Hub offers (HS-3).
    pub features: Vec<String>,
    /// Every control frame, the handshake included (TR-1, EN-5).
    pub max_frame_bytes: u64,
    /// A chunked binding's chunk payload (TR-3); `local-stream` does not chunk.
    pub max_chunk_bytes: u64,
    pub idempotency_window_seconds: u64,
    pub cursor_idle_seconds: u64,
    pub max_connections: usize,
    pub max_unclassified: usize,
    pub handshake_deadline: Duration,
    /// A connection's outgoing byte bound: at least `max_frame_bytes` and the longest route-connect close.
    pub out_bytes: usize,
    pub busy_retry_ms: u64,
}

impl ProtocolConfig {
    pub fn production() -> ProtocolConfig {
        ProtocolConfig {
            version: 1,
            features: Vec::new(),
            max_frame_bytes: 1024 * 1024,
            max_chunk_bytes: 16 * 1024,
            idempotency_window_seconds: 600,
            cursor_idle_seconds: 300,
            max_connections: 256,
            max_unclassified: 32,
            handshake_deadline: Duration::from_secs(5),
            out_bytes: 2 * 1024 * 1024,
            busy_retry_ms: 100,
        }
    }
}

/// The `welcome.limits` numbers that other components enforce, so each has one source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedLimits {
    /// The watch table.
    pub max_watches: u64,
    /// The outbox `replies` cap (EN-4).
    pub max_outstanding: u64,
    /// The spawn store's retention.
    pub operation_retention_seconds: u64,
    /// The route window (TS-2).
    pub route_connect_seconds: u64,
    /// The routes' query deadline (EV-8).
    pub query_client_deadline_ms: u64,
    /// Core's `max_paste_bytes` (R-119: Core enforces it per route).
    pub max_paste_bytes: u64,
    /// Core's `max_file_bytes`.
    pub max_file_bytes: u64,
}

impl ServerConfig {
    /// The server's configuration from this layer's numbers and the other components' numbers.
    ///
    /// # Panics
    /// `max_outstanding` is not the outbox `replies` cap, or `out_bytes` cannot hold one frame of `max_frame_bytes`.
    pub fn compose(
        protocol: &ProtocolConfig,
        shared: SharedLimits,
        hub_id: String,
        incarnation: String,
        uid: u32,
        outbox: OutboxCaps,
    ) -> ServerConfig {
        assert_eq!(
            shared.max_outstanding, outbox.replies as u64,
            "welcome.limits.max_outstanding is the outbox replies cap"
        );
        assert!(
            protocol.out_bytes as u64 >= protocol.max_frame_bytes + 4,
            "out_bytes holds one whole frame"
        );
        ServerConfig {
            hub_id,
            incarnation,
            version: protocol.version,
            features: protocol.features.clone(),
            limits: Limits {
                max_frame_bytes: protocol.max_frame_bytes,
                max_chunk_bytes: protocol.max_chunk_bytes,
                max_outstanding: shared.max_outstanding,
                max_watches: shared.max_watches,
                route_connect_seconds: shared.route_connect_seconds,
                idempotency_window_seconds: protocol.idempotency_window_seconds,
                cursor_idle_seconds: protocol.cursor_idle_seconds,
                operation_retention_seconds: shared.operation_retention_seconds,
                max_paste_bytes: shared.max_paste_bytes,
                max_file_bytes: shared.max_file_bytes,
                query_client_deadline_ms: shared.query_client_deadline_ms,
            },
            max_connections: protocol.max_connections,
            max_unclassified: protocol.max_unclassified,
            handshake_deadline: protocol.handshake_deadline,
            out_bytes: protocol.out_bytes,
            outbox,
            busy_retry_ms: protocol.busy_retry_ms,
            uid,
        }
    }
}

enum State {
    /// Waiting for the first frame.
    Unclassified {
        reader: FrameReader,
        deadline: Instant,
    },
    /// The `route_connect` is with the owner, and the stream travels in the request.
    Route,
    /// After `welcome`.
    Control(Control),
    /// A close is queued: the connection ends when its bytes are written.
    Closing,
}

struct Control {
    #[allow(dead_code)] // The kinds use the selected version and the hello (next step).
    version: u16,
    #[allow(dead_code)]
    hello: Hello,
    reader: FrameReader,
}

struct Conn {
    /// `None` once the stream left with a `route_connect`.
    stream: Option<UnixStream>,
    uid: u32,
    state: State,
    outbox: Arc<Outbox>,
    out: OutBuf,
}

/// The client connections of one Hub.
pub struct Server {
    registry: Registry,
    bridge: Arc<Bridge>,
    phase: Arc<SharedPhase>,
    io_wake: Arc<dyn Wake>,
    owner_wake: Arc<dyn Wake>,
    config: ServerConfig,
    /// `welcome.terminal_format`'s common set, which the owner keeps (R-124).
    formats: Arc<CommonFormats>,
    conns: HashMap<Token, Conn>,
    next_token: usize,
}

fn busy(retry_after_ms: u64) -> CloseReason {
    CloseReason::Busy(BusyData { retry_after_ms })
}

fn protocol_error(code: ProtocolErrorCode) -> CloseReason {
    CloseReason::ProtocolError(ProtocolErrorData { code })
}

/// Writes a close to a stream that is not registered, once, best effort, and drops it.
fn refuse(mut stream: UnixStream, reason: CloseReason) {
    let _ = stream.write(&close_frame(reason));
}

/// The version that HS-2 selects: the highest version in both ranges, or the Hub's range when they do not meet.
pub fn select_version(
    hub_current: u16,
    client_min: u16,
    client_max: u16,
) -> Result<u16, (u16, u16)> {
    let hub_min = hub_current.saturating_sub(1).max(1);
    let selected = hub_current.min(client_max);
    if selected >= hub_min.max(client_min) {
        Ok(selected)
    } else {
        Err((hub_min, hub_current))
    }
}

/// An error `res` that the server answers itself.
pub fn error_res(
    id: u64,
    code: ErrorCode,
    category: Category,
    retryable: bool,
    message: &str,
) -> Res {
    Res {
        id: botster_hub_contract::prelude::RequestId(id),
        outcome: Outcome::Err {
            err: Error {
                code,
                category,
                retryable,
                effect: None,
                message: message.into(),
                reason: None,
                detail: None,
                field_errors: None,
                retry_after_ms: None,
            },
        },
    }
}

impl Server {
    /// `registry` is a clone of the `io` poll's registry. `io_wake` wakes the `io` loop for [`Server::drain_outboxes`];
    /// `owner_wake` is the owner's capacity wake. `formats` is the owner's `Routes::formats()`.
    ///
    /// # Panics
    /// `out_bytes` is under [`MAX_ROUTE_CONNECT_CLOSE`]: the one close of a route connect could not fit.
    pub fn new(
        registry: Registry,
        bridge: Arc<Bridge>,
        phase: Arc<SharedPhase>,
        io_wake: Arc<dyn Wake>,
        owner_wake: Arc<dyn Wake>,
        formats: Arc<CommonFormats>,
        config: ServerConfig,
    ) -> Server {
        assert!(
            config.out_bytes >= MAX_ROUTE_CONNECT_CLOSE,
            "out_bytes must hold the longest route-connect close"
        );
        Server {
            registry,
            bridge,
            phase,
            io_wake,
            owner_wake,
            config,
            formats,
            conns: HashMap::new(),
            next_token: FIRST_CLIENT,
        }
    }

    /// The open connections.
    pub fn connections(&self) -> usize {
        self.conns.len()
    }

    fn unclassified(&self) -> usize {
        self.conns
            .values()
            .filter(|c| matches!(c.state, State::Unclassified { .. }))
            .count()
    }

    fn outbox(&self, caps: OutboxCaps) -> Arc<Outbox> {
        Outbox::new(
            caps,
            self.phase.clone(),
            self.io_wake.clone(),
            self.owner_wake.clone(),
        )
    }

    /// Takes one accepted stream: the connection bound (HS-8), the peer uid (TB-L3), then registration.
    pub fn accept(&mut self, mut stream: UnixStream, now: Instant) {
        if self.conns.len() >= self.config.max_connections
            || self.unclassified() >= self.config.max_unclassified
        {
            return refuse(stream, busy(self.config.busy_retry_ms));
        }
        match peer_uid(&stream) {
            Ok(uid) if uid == self.config.uid => {}
            _ => return refuse(stream, CloseReason::AuthFailed),
        }
        let token = Token(self.next_token);
        self.next_token += 1;
        if self
            .registry
            .register(&mut stream, token, Interest::READABLE | Interest::WRITABLE)
            .is_err()
        {
            return;
        }
        let conn = Conn {
            stream: Some(stream),
            uid: self.config.uid,
            state: State::Unclassified {
                reader: FrameReader::default(),
                deadline: now + self.config.handshake_deadline,
            },
            outbox: self.outbox(self.config.outbox),
            out: OutBuf::new(self.config.out_bytes),
        };
        self.conns.insert(token, conn);
    }

    /// One readiness event of a client token.
    pub fn ready(&mut self, token: Token, event: &Event) {
        if event.is_readable() || event.is_read_closed() {
            self.read(token);
        }
        self.write(token);
    }

    fn read(&mut self, token: Token) {
        let max = self.config.limits.max_frame_bytes;
        loop {
            let Some(conn) = self.conns.get_mut(&token) else {
                return;
            };
            let Some(stream) = conn.stream.as_mut() else {
                return;
            };
            let reader = match &mut conn.state {
                State::Unclassified { reader, .. } => reader,
                State::Control(control) => &mut control.reader,
                State::Route | State::Closing => return,
            };
            match reader.read(stream, max) {
                Ok(Some(body)) => self.frame(token, body),
                Ok(None) => return,
                Err(ReadError::TooLarge { .. }) => {
                    return self.close(token, protocol_error(ProtocolErrorCode::FrameTooLarge))
                }
                Err(ReadError::Eof | ReadError::Io(_)) => return self.drop_conn(token),
            }
        }
    }

    fn frame(&mut self, token: Token, body: Vec<u8>) {
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        match conn.state {
            State::Unclassified { .. } => match classify(&body) {
                Ok(First::Route(connect)) => self.route_connect(token, connect),
                Ok(First::Control(hello)) => self.hello(token, hello),
                Err(reason) => self.close(token, reason),
            },
            State::Control(_) => self.envelope(token, &body),
            State::Route | State::Closing => {}
        }
    }

    /// HS-1 to HS-3: the version, the features, then `welcome`.
    fn hello(&mut self, token: Token, hello: Hello) {
        if hello.protocol != PROTOCOL {
            return self.close(token, CloseReason::HandshakeOrder);
        }
        let version =
            match select_version(self.config.version, hello.versions.min, hello.versions.max) {
                Ok(version) => version,
                Err((hub_min, hub_max)) => {
                    return self.close(
                        token,
                        CloseReason::VersionUnsupported(VersionUnsupportedData {
                            hub_min,
                            hub_max,
                        }),
                    )
                }
            };
        let features = self
            .config
            .features
            .iter()
            .filter(|f| hello.features.contains(f))
            .cloned()
            .collect();
        let welcome = Welcome {
            hub_id: self.config.hub_id.clone(),
            incarnation: self.config.incarnation.clone(),
            version,
            features,
            // R-124: the first of the client's formats that every announced worker supports; absent with none.
            terminal_format: self.formats.pick(&hello.terminal_formats),
            limits: self.config.limits,
        };
        let frame = server_frame(&ServerFrame::Welcome(welcome));
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        conn.state = State::Control(Control {
            version,
            hello,
            reader: FrameReader::default(),
        });
        if conn.out.push(&frame).is_err() {
            self.drop_conn(token);
        }
    }

    /// The control envelope after `welcome` (HC sections 3 and 5). The kinds come in the next step; until then every
    /// request is answered `unsupported`.
    fn envelope(&mut self, token: Token, body: &[u8]) {
        let frame = match serde_json::from_slice::<ClientFrame>(body) {
            Ok(frame) => frame,
            Err(_) => return self.close(token, protocol_error(ProtocolErrorCode::MalformedFrame)),
        };
        match frame {
            ClientFrame::Hello(_) => {
                self.close(token, protocol_error(ProtocolErrorCode::HandshakeOrder))
            }
            ClientFrame::Req(req) => self.request(token, req),
            ClientFrame::Cancel(_) => {}
        }
    }

    fn request(&mut self, token: Token, req: Req) {
        let res = error_res(
            req.id.0,
            ErrorCode::Unsupported,
            Category::Caller,
            false,
            "this kind is not served yet",
        );
        self.push_res(token, &res);
    }

    fn push_res(&mut self, token: Token, res: &Res) {
        let frame = server_frame(&ServerFrame::Res(res.clone()));
        if let Some(conn) = self.conns.get_mut(&token) {
            if conn.out.push(&frame).is_err() {
                self.drop_conn(token);
            }
        }
    }

    /// Design 2.3, item 1: one reserved reply, the stream out of the poll, then `submit`. A refused `submit` returns the
    /// stream, and `io` answers its close through the same reply.
    fn route_connect(
        &mut self,
        token: Token,
        connect: botster_hub_contract::prelude::RouteConnect,
    ) {
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        let Some(mut stream) = conn.stream.take() else {
            return;
        };
        let _ = self.registry.deregister(&mut stream);
        // A route connection has one reply. `out_bytes` holds its close (asserted in `new`).
        conn.outbox = Outbox::new(
            OutboxCaps {
                replies: 1,
                frames: 0,
                watch_queue: 0,
            },
            self.phase.clone(),
            self.io_wake.clone(),
            self.owner_wake.clone(),
        );
        conn.state = State::Route;
        let reply = conn
            .outbox
            .reserve(0)
            .expect("a new outbox has its one slot");
        let stream = std::os::unix::net::UnixStream::from(stream);
        // Core takes the descriptor as a std stream in its default (blocking) mode.
        if stream.set_nonblocking(false).is_err() {
            drop(reply);
            return self.drop_conn(token);
        }
        let request = Request::RouteConnect(Box::new(RouteConnectRequest {
            route_id: connect.route_id,
            route_token: connect.route_token,
            caller: Caller { uid: conn.uid },
            stream,
        }));
        if let Err(rejected) = self.bridge.submit(request, reply) {
            let reason = match rejected.refused {
                Refused::Backpressured { .. } | Refused::NotReady => {
                    busy(self.config.busy_retry_ms)
                }
                Refused::ShuttingDown => CloseReason::HubShutdown(HubShutdownData {
                    lock_released: false,
                }),
            };
            let Request::RouteConnect(request) = rejected.request else {
                unreachable!("the bridge returns the request it refused");
            };
            rejected
                .reply
                .send(Answer::RouteConnect(RouteConnectAnswer::Refused {
                    reason,
                    stream: crate::client::handoff::ReturnedStream(Arc::new(request.stream)),
                }));
        }
    }

    /// Queues a close and stops reading. The connection ends when the close is written.
    fn close(&mut self, token: Token, reason: CloseReason) {
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        conn.state = State::Closing;
        if conn.out.push(&close_frame(reason)).is_err() {
            return self.drop_conn(token);
        }
        self.write(token);
    }

    fn drop_conn(&mut self, token: Token) {
        if let Some(mut conn) = self.conns.remove(&token) {
            conn.outbox.close();
            if let Some(stream) = conn.stream.as_mut() {
                let _ = self.registry.deregister(stream);
            }
        }
    }

    fn write(&mut self, token: Token) {
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        let Some(stream) = conn.stream.as_mut() else {
            return;
        };
        match conn.out.flush(stream) {
            Ok(true) if matches!(conn.state, State::Closing) => self.drop_conn(token),
            Ok(_) => {}
            Err(_) => self.drop_conn(token),
        }
    }

    /// Writes the owner's answers that wait in the outboxes.
    pub fn drain_outboxes(&mut self) {
        let tokens: Vec<Token> = self
            .conns
            .iter()
            .filter(|(_, c)| c.outbox.has_items())
            .map(|(t, _)| *t)
            .collect();
        for token in tokens {
            let Some(conn) = self.conns.get(&token) else {
                continue;
            };
            let items = conn.outbox.take(TAKE);
            let route = matches!(conn.state, State::Route);
            for item in items {
                if route {
                    self.route_answer(token, item);
                } else {
                    self.control_item(token, item);
                }
            }
            self.write(token);
        }
    }

    /// Design 2.3, item 4: a refused connect gets its close; a handed one is Core's. Either way the entry goes.
    fn route_answer(&mut self, token: Token, item: OutItem) {
        if let OutItem::Reply {
            answer: Answer::RouteConnect(RouteConnectAnswer::Refused { reason, stream }),
            ..
        } = item
        {
            // Best effort and non-blocking: a fresh connection's send buffer takes the at most 78 bytes.
            if stream.0.set_nonblocking(true).is_ok() {
                let _ = (&*stream.0).write(&close_frame(reason));
            }
        }
        // `Handed`, or a token dropped on a panic path (the stream closed with the request).
        self.drop_conn(token);
    }

    fn control_item(&mut self, token: Token, item: OutItem) {
        if let OutItem::Reply { request_id, .. } = item {
            let res = error_res(
                request_id,
                ErrorCode::Internal,
                Category::Fault,
                false,
                "no request of this connection reaches the owner yet",
            );
            self.push_res(token, &res);
        }
    }

    /// The earliest handshake deadline, for the `io` poll timeout.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.conns
            .values()
            .filter_map(|c| match &c.state {
                State::Unclassified { deadline, .. } => Some(*deadline),
                _ => None,
            })
            .min()
    }

    /// Closes every unclassified connection whose first frame did not arrive in time.
    pub fn expire(&mut self, now: Instant) {
        let due: Vec<Token> = self
            .conns
            .iter()
            .filter(|(_, c)| matches!(&c.state, State::Unclassified { deadline, .. } if now >= *deadline))
            .map(|(t, _)| *t)
            .collect();
        for token in due {
            self.close(token, CloseReason::HandshakeOrder);
        }
    }

    /// Stop step 3: writes what the outboxes hold. True when every connection's bytes are written.
    pub fn flush(&mut self) -> bool {
        self.drain_outboxes();
        let tokens: Vec<Token> = self.conns.keys().copied().collect();
        for token in tokens {
            self.write(token);
        }
        self.conns
            .values()
            .all(|c| c.out.is_empty() && !c.outbox.has_items())
    }

    /// Stop step 5: `close { hub_shutdown, lock_released: true }` where it fits, one write, then every connection ends.
    pub fn close_all(&mut self) {
        self.drain_outboxes();
        let shutdown = close_frame(CloseReason::HubShutdown(HubShutdownData {
            lock_released: true,
        }));
        let tokens: Vec<Token> = self.conns.keys().copied().collect();
        for token in tokens {
            if let Some(conn) = self.conns.get_mut(&token) {
                if !matches!(conn.state, State::Route | State::Closing) {
                    let _ = conn.out.push(&shutdown);
                }
            }
            self.write(token);
            self.drop_conn(token);
        }
    }
}

fn server_frame(frame: &ServerFrame) -> Vec<u8> {
    framed(&serde_json::to_vec(frame).expect("a server frame always encodes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::LaneCaps;
    use crate::client::wire::framed;
    use crate::phase::tests::Count;
    use mio::net::UnixListener;
    use mio::{Events, Poll};
    use serde_json::{json, Value};
    use std::io::Read;
    use std::os::unix::net::UnixStream as StdStream;
    use std::path::PathBuf;

    const LISTENER: Token = Token(0);

    fn me() -> u32 {
        rustix::process::geteuid().as_raw()
    }

    fn limits() -> Limits {
        serde_json::from_value(json!({"max_frame_bytes": 4096, "max_chunk_bytes": 1024, "max_outstanding": 8,
            "max_watches": 32, "route_connect_seconds": 10, "idempotency_window_seconds": 60,
            "cursor_idle_seconds": 60, "operation_retention_seconds": 3600, "max_paste_bytes": 65536,
            "max_file_bytes": 1048576, "query_client_deadline_ms": 200}))
        .unwrap()
    }

    fn config() -> ServerConfig {
        ServerConfig {
            hub_id: "h".into(),
            incarnation: "i1".into(),
            version: 1,
            features: vec!["a".into(), "b".into()],
            limits: limits(),
            max_connections: 4,
            max_unclassified: 4,
            handshake_deadline: Duration::from_secs(5),
            out_bytes: 8192,
            outbox: OutboxCaps {
                replies: 8,
                frames: 4,
                watch_queue: 4,
            },
            busy_retry_ms: 50,
            uid: me(),
        }
    }

    struct Rig {
        _dir: tempfile::TempDir,
        path: PathBuf,
        poll: Poll,
        listener: UnixListener,
        server: Server,
        bridge: Arc<Bridge>,
        formats: Arc<CommonFormats>,
        phase: Arc<SharedPhase>,
    }

    fn rig_with(config: ServerConfig) -> Rig {
        let dir = tempfile::Builder::new()
            .prefix("bh-")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("hub.sock");
        let poll = Poll::new().unwrap();
        let mut listener = UnixListener::bind(&path).unwrap();
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)
            .unwrap();
        let phase = SharedPhase::new();
        let bridge = Arc::new(Bridge::new(
            LaneCaps {
                control: 2,
                request: 4,
                bulk: 2,
            },
            phase.clone(),
            Arc::new(Count::default()),
        ));
        let formats = Arc::new(CommonFormats::default());
        let server = Server::new(
            poll.registry().try_clone().unwrap(),
            bridge.clone(),
            phase.clone(),
            Arc::new(Count::default()),
            Arc::new(Count::default()),
            formats.clone(),
            config,
        );
        Rig {
            _dir: dir,
            path,
            poll,
            listener,
            server,
            bridge,
            formats,
            phase,
        }
    }

    fn rig() -> Rig {
        rig_with(config())
    }

    impl Rig {
        /// Runs the poll until it is quiet for 50 ms, handing every event to the server, as the `io` loop does.
        fn pump(&mut self) {
            let mut events = Events::with_capacity(16);
            loop {
                self.poll
                    .poll(&mut events, Some(Duration::from_millis(50)))
                    .unwrap();
                if events.is_empty() {
                    return;
                }
                for event in events.iter() {
                    if event.token() == LISTENER {
                        while let Ok((stream, _)) = self.listener.accept() {
                            self.server.accept(stream, Instant::now());
                        }
                    } else {
                        self.server.ready(event.token(), event);
                    }
                }
                self.server.drain_outboxes();
            }
        }

        fn connect(&mut self) -> StdStream {
            let client = StdStream::connect(&self.path).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            self.pump();
            client
        }
    }

    fn send(client: &mut StdStream, value: &Value) {
        client
            .write_all(&framed(&serde_json::to_vec(value).unwrap()))
            .unwrap();
    }

    fn receive(client: &mut StdStream) -> Value {
        let mut prefix = [0u8; 4];
        client.read_exact(&mut prefix).unwrap();
        let mut body = vec![0u8; u32::from_be_bytes(prefix) as usize];
        client.read_exact(&mut body).unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn at_eof(client: &mut StdStream) -> bool {
        let mut byte = [0u8; 1];
        matches!(client.read(&mut byte), Ok(0))
    }

    fn hello(min: u16, max: u16) -> Value {
        json!({"type": "hello", "protocol": "botster-hub", "versions": {"min": min, "max": max},
            "client": {"name": "t", "version": "0"}, "features": ["b", "c"],
            "terminal_formats": ["ghostsnp/2+raw", "ghostsnp/1+raw"]})
    }

    fn shared() -> SharedLimits {
        SharedLimits {
            max_watches: 32,
            max_outstanding: 8,
            operation_retention_seconds: 86_400,
            route_connect_seconds: 10,
            query_client_deadline_ms: 200,
            max_paste_bytes: 65_536,
            max_file_bytes: 1_048_576,
        }
    }

    #[test]
    fn compose_takes_each_number_from_its_one_source() {
        let protocol = ProtocolConfig::production();
        let outbox = OutboxCaps {
            replies: 8,
            frames: 4,
            watch_queue: 4,
        };
        let config = ServerConfig::compose(&protocol, shared(), "h".into(), "i".into(), 7, outbox);
        let l = config.limits;
        assert_eq!(
            (
                l.max_watches,
                l.max_outstanding,
                l.operation_retention_seconds
            ),
            (32, 8, 86_400)
        );
        assert_eq!(
            (
                l.route_connect_seconds,
                l.query_client_deadline_ms,
                l.max_paste_bytes,
                l.max_file_bytes
            ),
            (10, 200, 65_536, 1_048_576)
        );
        assert_eq!(l.max_frame_bytes, protocol.max_frame_bytes);
        assert_eq!((config.uid, config.outbox), (7, outbox));
    }

    #[test]
    #[should_panic(expected = "max_outstanding")]
    fn compose_refuses_a_max_outstanding_that_the_outbox_does_not_enforce() {
        let outbox = OutboxCaps {
            replies: 9,
            frames: 4,
            watch_queue: 4,
        };
        ServerConfig::compose(
            &ProtocolConfig::production(),
            shared(),
            "h".into(),
            "i".into(),
            7,
            outbox,
        );
    }

    #[test]
    fn select_version_takes_the_highest_common_version() {
        assert_eq!(select_version(2, 1, 5), Ok(2));
        assert_eq!(select_version(2, 1, 1), Ok(1));
        assert_eq!(select_version(2, 3, 4), Err((1, 2)));
        assert_eq!(select_version(1, 0, 0), Err((1, 1)), "never version 0");
        assert_eq!(select_version(1, 1, 1), Ok(1));
    }

    #[test]
    fn hello_gets_welcome_with_the_common_features_and_format() {
        let mut rig = rig();
        rig.formats.set(Some(vec!["ghostsnp/1+raw".into()]));
        let mut client = rig.connect();
        send(&mut client, &hello(1, 1));
        rig.pump();
        let welcome = receive(&mut client);
        assert_eq!(welcome["type"], "welcome");
        assert_eq!(welcome["hub_id"], "h");
        assert_eq!(welcome["incarnation"], "i1");
        assert_eq!(welcome["version"], 1);
        assert_eq!(welcome["features"], json!(["b"]));
        assert_eq!(welcome["terminal_format"], "ghostsnp/1+raw");
        assert_eq!(welcome["limits"]["max_frame_bytes"], 4096);
    }

    #[test]
    fn with_no_announced_worker_welcome_has_no_format() {
        let mut rig = rig();
        let mut client = rig.connect();
        send(&mut client, &hello(1, 1));
        rig.pump();
        let welcome = receive(&mut client);
        assert!(welcome.get("terminal_format").is_none(), "{welcome}");
    }

    #[test]
    fn no_common_version_closes_version_unsupported() {
        let mut rig = rig();
        let mut client = rig.connect();
        send(&mut client, &hello(2, 3));
        rig.pump();
        assert_eq!(
            receive(&mut client),
            json!({"type": "close", "reason": {"version_unsupported": {"hub_min": 1, "hub_max": 1}}})
        );
        assert!(at_eof(&mut client));
        assert_eq!(rig.server.connections(), 0);
    }

    #[test]
    fn a_request_before_hello_closes_handshake_order() {
        let mut rig = rig();
        let mut client = rig.connect();
        send(
            &mut client,
            &json!({"type": "req", "id": "1", "verb": "list", "kind": "session"}),
        );
        rig.pump();
        assert_eq!(
            receive(&mut client),
            json!({"type": "close", "reason": "handshake_order"})
        );
        assert!(at_eof(&mut client));
    }

    #[test]
    fn a_first_frame_over_the_bound_closes_frame_too_large() {
        let mut rig = rig();
        let mut client = rig.connect();
        client.write_all(&[0, 0, 0x10, 0x01]).unwrap();
        rig.pump();
        assert_eq!(
            receive(&mut client),
            json!({"type": "close", "reason": {"protocol_error": {"code": "frame_too_large"}}})
        );
    }

    #[test]
    fn a_foreign_uid_is_closed_auth_failed() {
        let mut rig = rig_with(ServerConfig {
            uid: me().wrapping_add(1),
            ..config()
        });
        let mut client = rig.connect();
        assert_eq!(
            receive(&mut client),
            json!({"type": "close", "reason": "auth_failed"})
        );
        assert!(at_eof(&mut client));
        assert_eq!(rig.server.connections(), 0);
    }

    #[test]
    fn over_the_connection_bound_the_hub_closes_busy() {
        let mut rig = rig_with(ServerConfig {
            max_connections: 1,
            ..config()
        });
        let _first = rig.connect();
        let mut second = rig.connect();
        assert_eq!(
            receive(&mut second),
            json!({"type": "close", "reason": {"busy": {"retry_after_ms": 50}}})
        );
        assert_eq!(rig.server.connections(), 1);
    }

    #[test]
    fn a_silent_connection_is_closed_at_its_handshake_deadline() {
        let mut rig = rig();
        let mut client = rig.connect();
        let deadline = rig.server.next_deadline().unwrap();
        rig.server.expire(deadline - Duration::from_millis(1));
        assert_eq!(rig.server.connections(), 1);
        rig.server.expire(deadline);
        assert_eq!(
            receive(&mut client),
            json!({"type": "close", "reason": "handshake_order"})
        );
        assert_eq!(rig.server.next_deadline(), None);
    }

    fn route_connect(client: &mut StdStream, pipelined: &[u8]) {
        let mut bytes = framed(
            &serde_json::to_vec(
                &json!({"frame": "route_connect", "route_id": "r1", "route_token": "t"}),
            )
            .unwrap(),
        );
        bytes.extend_from_slice(pipelined);
        client.write_all(&bytes).unwrap();
    }

    #[test]
    fn a_route_connect_reaches_the_owner_with_its_stream_and_no_byte_read_past_the_frame() {
        let mut rig = rig();
        rig.bridge.set_ready();
        let mut client = rig.connect();
        route_connect(&mut client, &[0, 0, 0, 1, 0x83]);
        rig.pump();
        let mut items = rig.bridge.take_turn(1, 4, 1);
        assert_eq!(items.len(), 1);
        let item = items.pop().unwrap();
        let Request::RouteConnect(request) = item.request else {
            panic!("{:?}", item.request);
        };
        assert_eq!(
            (request.route_id.as_str(), request.caller.uid),
            ("r1", me())
        );
        let mut stream = request.stream;
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut pipelined = [0u8; 5];
        stream.read_exact(&mut pipelined).unwrap();
        assert_eq!(
            pipelined,
            [0, 0, 0, 1, 0x83],
            "the worker gets the client's first route frame"
        );
        // Handed: the owner's answer removes the connection; the stream is the worker's.
        item.reply
            .send(Answer::RouteConnect(RouteConnectAnswer::Handed));
        rig.server.drain_outboxes();
        assert_eq!(rig.server.connections(), 0);
    }

    #[test]
    fn a_refused_route_connect_gets_its_close() {
        let mut rig = rig();
        rig.bridge.set_ready();
        let mut client = rig.connect();
        route_connect(&mut client, &[]);
        rig.pump();
        let item = rig.bridge.take_turn(1, 4, 1).pop().unwrap();
        let Request::RouteConnect(request) = item.request else {
            panic!();
        };
        item.reply
            .send(Answer::RouteConnect(RouteConnectAnswer::Refused {
                reason: CloseReason::RouteTokenUsed,
                stream: crate::client::handoff::ReturnedStream(Arc::new(request.stream)),
            }));
        rig.server.drain_outboxes();
        assert_eq!(
            receive(&mut client),
            json!({"type": "close", "reason": "route_token_used"})
        );
        assert!(at_eof(&mut client));
        assert_eq!(rig.server.connections(), 0);
    }

    #[test]
    fn a_route_connect_before_adoption_is_closed_busy() {
        let mut rig = rig();
        let mut client = rig.connect();
        route_connect(&mut client, &[]);
        rig.pump();
        assert_eq!(
            receive(&mut client),
            json!({"type": "close", "reason": {"busy": {"retry_after_ms": 50}}})
        );
        assert!(at_eof(&mut client));
        assert_eq!(rig.server.connections(), 0);
    }

    #[test]
    fn a_route_connect_while_stopping_is_closed_hub_shutdown() {
        let mut rig = rig();
        rig.bridge.set_ready();
        let lifecycle = std::sync::mpsc::sync_channel(8);
        crate::phase::request_stop(&rig.phase, &lifecycle.0, crate::phase::StopCause::Requested);
        let mut client = rig.connect();
        route_connect(&mut client, &[]);
        rig.pump();
        assert_eq!(
            receive(&mut client),
            json!({"type": "close", "reason": {"hub_shutdown": {"lock_released": false}}})
        );
    }

    #[test]
    fn close_all_sends_hub_shutdown_with_the_lock_released() {
        let mut rig = rig();
        let mut client = rig.connect();
        send(&mut client, &hello(1, 1));
        rig.pump();
        receive(&mut client);
        assert!(rig.server.flush());
        rig.server.close_all();
        assert_eq!(
            receive(&mut client),
            json!({"type": "close", "reason": {"hub_shutdown": {"lock_released": true}}})
        );
        assert!(at_eof(&mut client));
        assert_eq!(rig.server.connections(), 0);
    }

    #[test]
    fn a_request_after_welcome_is_answered() {
        let mut rig = rig();
        let mut client = rig.connect();
        send(&mut client, &hello(1, 1));
        rig.pump();
        receive(&mut client);
        send(
            &mut client,
            &json!({"type": "req", "id": "7", "verb": "list", "kind": "session"}),
        );
        rig.pump();
        let res = receive(&mut client);
        assert_eq!(res["type"], "res");
        assert_eq!(res["id"], "7");
        assert_eq!(res["err"]["code"], "unsupported");
    }
}
