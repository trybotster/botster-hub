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
use crate::client::kinds::{self, ConnKinds, Ctx, Step};
use crate::client::peer::peer_uid;
use crate::client::route::{close_frame, MAX_ROUTE_CONNECT_CLOSE};
use crate::client::rows::SessionTypes;
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
    /// The working directory of a created command that names none (HC A.9 sets no default; the operator's home).
    pub default_cwd: String,
    /// The Hub's socket path, which `attach` answers as `transport.socket_path` (TS-1, TB-L2).
    pub socket_path: String,
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
            default_cwd: "/".into(),
            socket_path: String::new(),
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
    Control(Box<Control>),
    /// A close is queued: the connection ends when its bytes are written.
    Closing,
}

struct Control {
    #[allow(dead_code)] // The kinds that follow use the selected version and the hello.
    version: u16,
    #[allow(dead_code)]
    hello: Hello,
    reader: FrameReader,
    kinds: ConnKinds,
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
    /// The session_type registry (design section 7a). The protocol layer owns it; it is built here, at start.
    types: Arc<SessionTypes>,
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
            types: Arc::new(SessionTypes::device()),
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
        conn.state = State::Control(Box::new(Control {
            version,
            hello,
            reader: FrameReader::default(),
            kinds: ConnKinds::default(),
        }));
        if conn.out.push(&frame).is_err() {
            self.drop_conn(token);
        }
    }

    /// The control envelope after `welcome` (HC sections 3 and 5).
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

    /// The kinds (design section 7a). An open request id is unique (EN-1). A request for the owner takes one reply slot;
    /// without a slot the answer is `too_many_requests` and the connection stays open (EN-4).
    fn request(&mut self, token: Token, req: Req) {
        let now = Instant::now();
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        let State::Control(control) = &mut conn.state else {
            return;
        };
        let id = req.id.0;
        if control.kinds.outstanding(id) {
            return self.close(token, protocol_error(ProtocolErrorCode::DuplicateRequestId));
        }
        let formats = control.hello.terminal_formats.clone();
        let ctx = kind_ctx(&self.config, &self.types, &formats);
        let step = control
            .kinds
            .begin(&ctx, token.0 as u64, &conn.outbox, &req, now);
        match step {
            Step::Reply(res) => self.push_res(token, &res),
            Step::Local(res, frames) => {
                self.push_res(token, &res);
                for frame in frames {
                    self.push_frame(token, &frame);
                }
            }
            Step::Owner(request, pending) => {
                let Some(reply) = conn.outbox.reserve(id) else {
                    let res = kinds::too_many_requests(id);
                    return self.push_res(token, &res);
                };
                control.kinds.expect(id, pending);
                if let Err(rejected) = self.bridge.submit(request, reply) {
                    // The answer goes through the outbox like any other: one path from the reply to the `res`.
                    rejected
                        .reply
                        .send(Answer::Failed(rejected.refused.failure()));
                }
            }
        }
    }

    fn push_res(&mut self, token: Token, res: &Res) {
        self.push_frame(token, &ServerFrame::Res(res.clone()));
    }

    /// A frame that does not fit the connection's outgoing bound ends the connection: a slow reader never gets a gap.
    fn push_frame(&mut self, token: Token, frame: &ServerFrame) {
        let bytes = server_frame(frame);
        if let Some(conn) = self.conns.get_mut(&token) {
            if conn.out.push(&bytes).is_err() {
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
        // A drain takes `TAKE` items from each outbox. What is left needs another turn of the `io` loop: no wake arrives
        // for items that were already queued. The work of one turn stays bounded.
        if self.conns.values().any(|c| c.outbox.has_items()) {
            self.io_wake.wake();
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
        let Some(conn) = self.conns.get_mut(&token) else {
            return;
        };
        let State::Control(control) = &mut conn.state else {
            return;
        };
        let ctx = kind_ctx(&self.config, &self.types, &[]);
        let frames = match item {
            OutItem::Reply { request_id, answer } => control.kinds.answer(&ctx, request_id, answer),
            OutItem::Frame { watch, frame } => control.kinds.frame(&ctx, watch, frame),
        };
        for frame in frames {
            self.push_frame(token, &frame);
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

fn kind_ctx<'a>(
    config: &'a ServerConfig,
    types: &'a SessionTypes,
    terminal_formats: &'a [String],
) -> Ctx<'a> {
    Ctx {
        caller: Caller { uid: config.uid },
        terminal_formats,
        socket_path: &config.socket_path,
        hub_id: &config.hub_id,
        types,
        max_frame_bytes: usize::try_from(config.limits.max_frame_bytes).unwrap_or(usize::MAX),
        cursor_idle: Duration::from_secs(config.limits.cursor_idle_seconds),
        max_watches: usize::try_from(config.limits.max_watches).unwrap_or(usize::MAX),
        busy_retry_ms: config.busy_retry_ms,
        default_cwd: &config.default_cwd,
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
            default_cwd: "/tmp".into(),
            socket_path: "/tmp/hub.sock".into(),
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
        /// The real owner over the fake Core, driven by `pump` (the owner thread's job in a Hub). Only [`owned`] has one:
        /// the route and handshake tests read the bridge themselves.
        owner: Option<crate::owner::Owner<botster_fake_core::FakeCore>>,
        _lifecycle: Option<std::sync::mpsc::Receiver<crate::phase::Lifecycle>>,
        /// The wakes of the `io` loop that the server and the outboxes made.
        io_wakes: Arc<Count>,
        /// The wake count the last drain answered.
        drained_at: usize,
        clock: Instant,
    }

    fn rig_with(config: ServerConfig) -> Rig {
        build_rig(config, false)
    }

    /// A rig with the real owner, which `pump` drives.
    fn owned(config: ServerConfig) -> Rig {
        build_rig(config, true)
    }

    fn build_rig(mut config: ServerConfig, with_owner: bool) -> Rig {
        let dir = tempfile::Builder::new()
            .prefix("bh-")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join("hub.sock");
        config.socket_path = path.to_string_lossy().into_owned();
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
        let io_wakes = Arc::new(Count::default());
        let (lifecycle_tx, lifecycle) =
            std::sync::mpsc::sync_channel(crate::phase::LIFECYCLE_CAPACITY);
        let core = {
            use botster_core::contract::prelude::{CoreLimits, OpenConfig};
            let store = botster_fake_core::FakeStore::new();
            let open = OpenConfig {
                data_dir: "/fake".into(),
                worker_path: Some("/fake/botster-worker".into()),
                limits: CoreLimits::default(),
            };
            botster_fake_core::FakeCore::open(&store, open, 7).unwrap()
        };
        let owner = with_owner.then(|| {
            // The spawn store is a real one on a short path; the owner ends its recovery as step 6b does.
            let store = crate::store::SpawnStore::open(
                dir.path(),
                crate::store::StoreConfig {
                    max_records: 16,
                    store_queue: 8,
                    retention_ms: 60_000,
                    max_record_bytes: 4096,
                    turn_sweeps: 1,
                },
                Arc::new(Count::default()),
            )
            .unwrap();
            let mut owner = crate::owner::Owner::new(
                core,
                bridge.clone(),
                phase.clone(),
                lifecycle_tx,
                crate::owner::TurnCounts {
                    control: 2,
                    request: 4,
                    bulk: 2,
                    events: 16,
                    fanout: 16,
                    baseline_rows: 16,
                },
                crate::watch::WatchLimits {
                    max_watches: 32,
                    chunk_rows: 1,
                },
            )
            .with_routes(crate::client::handoff::RouteParams {
                max_reservations: 4,
                connect_window: Duration::from_secs(10),
                query_deadline: Duration::from_millis(200),
                sessions_root: "/data/sessions".into(),
                pending_ends: 4,
            })
            .with_store(store);
            owner.store_mut().unwrap().end_recovery();
            owner.begin_adoption();
            owner
        });
        let server = Server::new(
            poll.registry().try_clone().unwrap(),
            bridge.clone(),
            phase.clone(),
            io_wakes.clone(),
            Arc::new(Count::default()),
            formats.clone(),
            config,
        );
        let mut rig = Rig {
            _dir: dir,
            path,
            poll,
            listener,
            server,
            bridge,
            formats,
            phase,
            owner,
            _lifecycle: Some(lifecycle),
            clock: Instant::now(),
            io_wakes,
            drained_at: 0,
        };
        // Adoption ends before the first client: the bridge is ready.
        rig.owner_settle();
        rig
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
                for event in events.iter() {
                    if event.token() == LISTENER {
                        while let Ok((stream, _)) = self.listener.accept() {
                            self.server.accept(stream, Instant::now());
                        }
                    } else {
                        self.server.ready(event.token(), event);
                    }
                }
                self.owner_settle();
                // The `io` loop drains at its wake token, and only then: the owner's fills and the server's re-arm wake it.
                if self.io_wakes.get() != self.drained_at {
                    self.drained_at = self.io_wakes.get();
                    self.server.drain_outboxes();
                } else if events.is_empty() {
                    return;
                }
            }
        }

        /// Runs owner turns until no work is left, and the spawn flow (the store thread is real) is idle.
        fn owner_settle(&mut self) {
            let Some(owner) = self.owner.as_mut() else {
                return;
            };
            for _ in 0..5000 {
                self.clock += Duration::from_millis(1);
                let now = botster_core::contract::prelude::Now {
                    monotonic: self.clock,
                    unix: 1_000_000,
                };
                let report = owner.turn(now);
                if !report.runnable && owner.spawn_idle() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            panic!("the owner did not settle");
        }

        /// A session through the bridge, as a client's spawn would make one.
        fn make_session(&mut self, name: &str) {
            use botster_core::contract::prelude::{Op, SessionId, Size, SpawnRequest};
            let outbox = Outbox::new(
                OutboxCaps {
                    replies: 2,
                    frames: 2,
                    watch_queue: 2,
                },
                self.phase.clone(),
                Arc::new(Count::default()),
                Arc::new(Count::default()),
            );
            let request = SpawnRequest {
                argv: vec!["/bin/sh".into()],
                env: Default::default(),
                cwd: "/".into(),
                size: Size {
                    rows: 24,
                    cols: 80,
                    cell_px: None,
                },
                labels: crate::client::rows::create_labels(
                    &SessionId(name.into()),
                    None,
                    crate::client::rows::COMMAND_TYPE,
                ),
                color_profile: None,
                notification_policy: None,
                size_policy: None,
            };
            let op = Request::Op(Box::new(Op::Create {
                session: SessionId(name.into()),
                request,
            }));
            self.bridge.submit(op, outbox.reserve(1).unwrap()).unwrap();
            self.owner_settle();
            self.server.drain_outboxes();
        }

        /// Starts a created session, as `Spawn` does after the create: a route attaches to a running session.
        fn start_session(&mut self, name: &str) {
            use botster_core::contract::prelude::{Op, SessionId};
            let outbox = Outbox::new(
                OutboxCaps {
                    replies: 2,
                    frames: 2,
                    watch_queue: 2,
                },
                self.phase.clone(),
                Arc::new(Count::default()),
                Arc::new(Count::default()),
            );
            let op = Request::Op(Box::new(Op::Start {
                id: SessionId(name.into()),
            }));
            self.bridge.submit(op, outbox.reserve(1).unwrap()).unwrap();
            self.owner_settle();
            self.server.drain_outboxes();
        }

        fn set_label(&mut self, name: &str, label: &str) {
            use botster_core::contract::prelude::SessionId;
            let outbox = Outbox::new(
                OutboxCaps {
                    replies: 2,
                    frames: 2,
                    watch_queue: 2,
                },
                self.phase.clone(),
                Arc::new(Count::default()),
                Arc::new(Count::default()),
            );
            let request = Request::SetLabels {
                session: SessionId(name.into()),
                set: [(
                    crate::client::rows::LABEL_KEY.to_string(),
                    label.to_string(),
                )]
                .into(),
                remove: Vec::new(),
            };
            self.bridge
                .submit(request, outbox.reserve(1).unwrap())
                .unwrap();
            self.owner_settle();
            self.pump();
        }

        /// A connection past `welcome`.
        fn control(&mut self) -> StdStream {
            let mut client = self.connect();
            send(&mut client, &hello(1, 1));
            self.pump();
            receive(&mut client);
            client
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

    fn req(
        client: &mut StdStream,
        id: u64,
        verb: &str,
        kind: &str,
        target: Option<&str>,
        args: Value,
    ) {
        let mut frame = json!({"type": "req", "id": id.to_string(), "verb": verb, "kind": kind});
        if let Some(target) = target {
            frame["target"] = json!(target);
        }
        if !args.is_null() {
            frame["args"] = args;
        }
        send(client, &frame);
    }

    fn ids(items: &Value) -> Vec<String> {
        items
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["id"].as_str().unwrap().to_owned())
            .collect()
    }

    /// An unknown kind or verb is `unsupported` and the connection stays open (EN-6, VB-7).
    #[test]
    fn an_unknown_kind_or_verb_is_unsupported_and_the_connection_stays_open() {
        let mut rig = owned(config());
        let mut client = rig.control();
        req(&mut client, 1, "list", "nope", None, Value::Null);
        req(&mut client, 2, "frobnicate", "session", None, Value::Null);
        rig.pump();
        let mut codes = [receive(&mut client), receive(&mut client)];
        codes.sort_by_key(|r| r["id"].as_str().unwrap().to_owned());
        assert_eq!(codes[0]["err"]["code"], "unsupported");
        assert_eq!(codes[1]["err"]["code"], "unsupported");
        req(&mut client, 3, "list", "session", None, Value::Null);
        rig.pump();
        assert_eq!(receive(&mut client)["ok"]["items"], json!([]));
    }

    /// VB-1, VB-2: `list` and `get` of sessions; the row is the builder's (R-125); an unknown target is `not_found`.
    #[test]
    fn list_and_get_answer_session_rows() {
        let mut rig = owned(config());
        rig.make_session("b");
        rig.make_session("a");
        let mut client = rig.control();
        req(&mut client, 1, "list", "session", None, Value::Null);
        req(&mut client, 2, "get", "session", Some("a"), Value::Null);
        req(&mut client, 3, "get", "session", Some("nope"), Value::Null);
        req(&mut client, 4, "get", "session", None, Value::Null);
        rig.pump();
        let mut res: Vec<Value> = (0..4).map(|_| receive(&mut client)).collect();
        res.sort_by_key(|r| r["id"].as_str().unwrap().to_owned());
        assert_eq!(ids(&res[0]["ok"]["items"]), ["a", "b"]);
        let row = &res[1]["ok"];
        assert_eq!(row["id"], "a");
        assert_eq!(row["hub_id"], "h");
        assert_eq!(row["label"], "a");
        assert_eq!(row["session_type"]["id"], "command");
        assert_eq!(row["activity"], "idle");
        assert_eq!(res[2]["err"]["code"], "not_found");
        assert_eq!(res[3]["err"]["code"], "invalid_arguments");
    }

    /// VB-1, `vb_1_pages_from_one_cut`: the pages of a cursor come from the cut of the first call, so a session made
    /// between two pages is not in them and none is skipped or repeated.
    #[test]
    fn pages_come_from_one_cut() {
        let mut rig = owned(config());
        for name in ["a", "b", "c"] {
            rig.make_session(name);
        }
        let mut client = rig.control();
        req(&mut client, 1, "list", "session", None, json!({"limit": 2}));
        rig.pump();
        let first = receive(&mut client);
        assert_eq!(ids(&first["ok"]["items"]), ["a", "b"]);
        let cursor = first["ok"]["next_cursor"].as_str().unwrap().to_owned();
        rig.make_session("bb");
        req(
            &mut client,
            2,
            "list",
            "session",
            None,
            json!({"limit": 2, "cursor": cursor}),
        );
        rig.pump();
        let second = receive(&mut client);
        assert_eq!(ids(&second["ok"]["items"]), ["c"]);
        assert!(second["ok"].get("next_cursor").is_none());
        // The cursor is gone after its last page; a new listing sees the new session.
        req(
            &mut client,
            3,
            "list",
            "session",
            None,
            json!({"cursor": cursor}),
        );
        req(&mut client, 4, "list", "session", None, Value::Null);
        rig.pump();
        let mut res = [receive(&mut client), receive(&mut client)];
        res.sort_by_key(|r| r["id"].as_str().unwrap().to_owned());
        assert_eq!(res[0]["err"]["code"], "expired_cursor");
        assert_eq!(ids(&res[1]["ok"]["items"]), ["a", "b", "bb", "c"]);
    }

    /// VB-1: an equality filter over core fields; a field that is not a core field is `invalid_arguments`.
    #[test]
    fn list_filters_by_core_field() {
        let mut rig = owned(config());
        rig.make_session("a");
        rig.make_session("b");
        rig.set_label("b", "build");
        let mut client = rig.control();
        req(
            &mut client,
            1,
            "list",
            "session",
            None,
            json!({"filter": {"label": "build"}}),
        );
        req(
            &mut client,
            2,
            "list",
            "session",
            None,
            json!({"filter": {"colour": "red"}}),
        );
        rig.pump();
        let mut res = [receive(&mut client), receive(&mut client)];
        res.sort_by_key(|r| r["id"].as_str().unwrap().to_owned());
        assert_eq!(ids(&res[0]["ok"]["items"]), ["b"]);
        assert_eq!(res[1]["err"]["code"], "invalid_arguments");
    }

    /// WT-1, WT-2, WT-6: `res` first, then one baseline; deltas after `snapshot_end`; `unwatch` ends the frames.
    #[test]
    fn a_session_watch_has_a_baseline_then_deltas_then_silence() {
        let mut rig = owned(config());
        rig.make_session("a");
        let mut client = rig.control();
        req(&mut client, 1, "watch", "session", None, Value::Null);
        rig.pump();
        let res = receive(&mut client);
        assert_eq!(res["id"], "1");
        let watch = res["ok"]["watch_id"].as_str().unwrap().to_owned();
        assert_eq!(
            receive(&mut client),
            json!({"type": "watch", "frame": "snapshot_begin", "watch_id": watch})
        );
        let items = receive(&mut client);
        assert_eq!(items["frame"], "snapshot_items");
        assert_eq!(ids(&items["items"]), ["a"]);
        assert_eq!(
            receive(&mut client),
            json!({"type": "watch", "frame": "snapshot_end", "watch_id": watch})
        );
        rig.make_session("b");
        rig.pump();
        let upsert = receive(&mut client);
        assert_eq!(
            (upsert["frame"].as_str(), upsert["id"].as_str()),
            (Some("upsert"), Some("b"))
        );
        assert_eq!(upsert["value"]["id"], "b");
        req(
            &mut client,
            2,
            "unwatch",
            "session",
            Some(&watch),
            Value::Null,
        );
        rig.pump();
        assert_eq!(receive(&mut client)["id"], "2");
        rig.make_session("c");
        rig.pump();
        req(&mut client, 3, "list", "session", None, Value::Null);
        rig.pump();
        // The next frame is the answer to the list: nothing of the watch follows the unwatch `res`.
        let next = receive(&mut client);
        assert_eq!(next["id"], "3");
        assert_eq!(ids(&next["ok"]["items"]), ["a", "b", "c"]);
        // The watch is gone: a second unwatch is `not_found`.
        req(
            &mut client,
            4,
            "unwatch",
            "session",
            Some(&watch),
            Value::Null,
        );
        rig.pump();
        assert_eq!(receive(&mut client)["err"]["code"], "not_found");
    }

    /// WT-2 `wt_2_filter_entry_exit`: a row that starts to match gets `upsert`, and a row that stops matching gets `remove`.
    #[test]
    fn a_filtered_watch_sends_entry_and_exit() {
        let mut rig = owned(config());
        rig.make_session("a");
        let mut client = rig.control();
        req(
            &mut client,
            1,
            "watch",
            "session",
            None,
            json!({"filter": {"label": "build"}}),
        );
        rig.pump();
        receive(&mut client);
        receive(&mut client);
        let items = receive(&mut client);
        assert_eq!(
            items,
            json!({"type": "watch", "frame": "snapshot_end", "watch_id": "1"}),
            "no row matches"
        );
        rig.set_label("a", "build");
        let entry = receive(&mut client);
        assert_eq!(
            (entry["frame"].as_str(), entry["id"].as_str()),
            (Some("upsert"), Some("a"))
        );
        rig.set_label("a", "test");
        let exit = receive(&mut client);
        assert_eq!(
            (exit["frame"].as_str(), exit["id"].as_str()),
            (Some("remove"), Some("a"))
        );
    }

    /// BD-1: a connection has at most `max_watches` watches; the next is `busy`, and the connection stays open.
    #[test]
    fn the_watch_bound_is_busy() {
        let mut config = config();
        config.limits.max_watches = 1;
        let mut rig = owned(config);
        let mut client = rig.control();
        req(&mut client, 1, "watch", "session_type", None, Value::Null);
        rig.pump();
        assert_eq!(receive(&mut client)["id"], "1");
        for _ in 0..3 {
            receive(&mut client);
        }
        req(&mut client, 2, "watch", "session", None, Value::Null);
        rig.pump();
        let res = receive(&mut client);
        assert_eq!(res["err"]["code"], "busy");
        assert_eq!(res["err"]["retryable"], true);
    }

    /// `session_type` reads come from the registry: `list`, `get`, `watch` (R-125, S2-HC-3: the `command` type).
    #[test]
    fn session_type_reads_come_from_the_registry() {
        let mut rig = owned(config());
        let mut client = rig.control();
        req(&mut client, 1, "list", "session_type", None, Value::Null);
        req(
            &mut client,
            2,
            "get",
            "session_type",
            Some("command"),
            Value::Null,
        );
        req(
            &mut client,
            3,
            "get",
            "session_type",
            Some("nope"),
            Value::Null,
        );
        rig.pump();
        let mut res: Vec<Value> = (0..3).map(|_| receive(&mut client)).collect();
        res.sort_by_key(|r| r["id"].as_str().unwrap().to_owned());
        assert_eq!(ids(&res[0]["ok"]["items"]), ["command"]);
        assert_eq!(res[1]["ok"]["source"], "device");
        assert_eq!(res[1]["ok"]["editable"], false);
        assert_eq!(res[2]["err"]["code"], "not_found");
        req(&mut client, 4, "watch", "session_type", None, Value::Null);
        rig.pump();
        assert_eq!(receive(&mut client)["id"], "4");
        assert_eq!(receive(&mut client)["frame"], "snapshot_begin");
        assert_eq!(ids(&receive(&mut client)["items"]), ["command"]);
        assert_eq!(receive(&mut client)["frame"], "snapshot_end");
    }

    /// EN-1: a request id that is open on the connection closes it `duplicate_request_id`. EN-4: past `max_outstanding`,
    /// the answer is `too_many_requests` and the connection stays open.
    #[test]
    fn open_ids_are_unique_and_the_outstanding_bound_does_not_close() {
        let mut rig = owned(config());
        let mut client = rig.control();
        // Eight open requests fill the replies; the ninth is refused. The owner has not run, so none is answered yet.
        for id in 1..=9 {
            req(&mut client, id, "list", "session", None, Value::Null);
        }
        rig.pump();
        let refused = receive(&mut client);
        assert_eq!(refused["id"], "9");
        assert_eq!(refused["err"]["code"], "too_many_requests");
        let mut answered = 0;
        for _ in 0..8 {
            let res = receive(&mut client);
            assert!(
                res.get("ok").is_some() || res["err"]["code"] == "busy",
                "{res}"
            );
            answered += 1;
        }
        assert_eq!(answered, 8);
        // A duplicate of an open id.
        for _ in 0..2 {
            req(&mut client, 20, "get", "session", Some("x"), Value::Null);
        }
        rig.pump();
        let close = receive(&mut client);
        assert_eq!(close["type"], "close");
        assert_eq!(
            close["reason"]["protocol_error"]["code"],
            "duplicate_request_id"
        );
    }

    fn body_len(value: &Value) -> usize {
        serde_json::to_vec(value).unwrap().len()
    }

    /// KINDS-1: a drain takes at most `TAKE` items from a connection, and it asks the `io` loop for another turn while
    /// items are left: no other wake comes for items that are already queued. `pump` follows the wakes as the loop does.
    #[test]
    fn a_drain_with_items_left_wakes_io_again() {
        let mut config = config();
        config.outbox = OutboxCaps {
            replies: 8,
            frames: 200,
            watch_queue: 200,
        };
        config.out_bytes = 1 << 20;
        let mut rig = owned(config);
        for n in 0..70 {
            rig.make_session(&format!("s{n:02}"));
        }
        let mut client = rig.control();
        req(&mut client, 1, "watch", "session", None, Value::Null);
        // A reader thread keeps the socket empty while the test thread pumps: the baseline is larger than one drain and
        // than the socket buffer.
        let mut reader = client.try_clone().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || loop {
            let frame = receive(&mut reader);
            let ended = frame["frame"] == "snapshot_end";
            tx.send(frame).unwrap();
            if ended {
                return;
            }
        });
        let mut frames = Vec::new();
        for _ in 0..200 {
            rig.pump();
            frames.extend(rx.try_iter());
            if frames.last().is_some_and(|f| f["frame"] == "snapshot_end") {
                break;
            }
        }
        thread.join().unwrap();
        frames.extend(rx.try_iter());
        // The `res`, `snapshot_begin`, 70 rows and `snapshot_end`.
        assert_eq!(frames.len(), 1 + 1 + 70 + 1);
    }

    /// KINDS-2, EN-5: the Hub never sends a frame over `max_frame_bytes`. A `get` whose row is one byte over is
    /// `response_too_large`; a row that fits exactly is sent.
    #[test]
    fn a_get_over_the_bound_is_response_too_large_and_an_exact_fit_is_sent() {
        let mut rig = owned(config());
        rig.make_session("a");
        let mut client = rig.control();
        let max = 4096;
        rig.set_label("a", &"x".repeat(100));
        req(&mut client, 1, "get", "session", Some("a"), Value::Null);
        rig.pump();
        let base = body_len(&receive(&mut client));
        let fit = 100 + (max - base);
        rig.set_label("a", &"x".repeat(fit));
        req(&mut client, 2, "get", "session", Some("a"), Value::Null);
        rig.pump();
        let exact = receive(&mut client);
        assert_eq!(body_len(&exact), max, "the answer fits exactly");
        assert_eq!(exact["ok"]["id"], "a");
        rig.set_label("a", &"x".repeat(fit + 1));
        req(&mut client, 3, "get", "session", Some("a"), Value::Null);
        rig.pump();
        let over = receive(&mut client);
        assert_eq!(over["err"]["code"], "response_too_large", "{over}");
        // The connection is fine.
        req(
            &mut client,
            4,
            "get",
            "session_type",
            Some("command"),
            Value::Null,
        );
        rig.pump();
        assert_eq!(receive(&mut client)["ok"]["id"], "command");
    }

    /// KINDS-2, BD-2: a delta over the bound ends the watch with `watch_closed { item_too_large }`; nothing of the watch
    /// follows it.
    #[test]
    fn a_delta_over_the_bound_closes_the_watch() {
        let mut rig = owned(config());
        rig.make_session("a");
        let mut client = rig.control();
        req(&mut client, 1, "watch", "session", None, Value::Null);
        rig.pump();
        for _ in 0..4 {
            receive(&mut client);
        }
        rig.set_label("a", &"x".repeat(5000));
        let closed = receive(&mut client);
        assert_eq!(
            closed,
            json!({"type": "watch", "frame": "watch_closed", "watch_id": "1", "reason": "item_too_large"})
        );
        rig.set_label("a", "small");
        req(&mut client, 2, "list", "session", None, Value::Null);
        rig.pump();
        assert_eq!(
            receive(&mut client)["id"],
            "2",
            "no frame of the closed watch follows"
        );
    }

    /// KINDS-3, BD-1: the bound counts each watch once. Two watches in one write fill `max_watches` 2; the third is `busy`.
    #[test]
    fn pipelined_watches_count_once_against_the_bound() {
        let mut config = config();
        config.limits.max_watches = 2;
        let mut rig = owned(config);
        let mut client = rig.control();
        for id in 1..=3 {
            req(&mut client, id, "watch", "session", None, Value::Null);
        }
        rig.pump();
        let mut oks = 0;
        let mut busy = 0;
        let mut seen = 0;
        while seen < 3 {
            let frame = receive(&mut client);
            if frame["type"] == "res" {
                seen += 1;
                if frame.get("ok").is_some() {
                    oks += 1;
                } else if frame["err"]["code"] == "busy" {
                    busy += 1;
                }
            }
        }
        assert_eq!((oks, busy), (2, 1));
    }

    fn create_args(argv: &[&str]) -> Value {
        json!({"command": {"argv": argv}})
    }

    impl Rig {
        /// Core loses the session's worker with a known termination: the HC state is `ended`.
        fn lose_worker(&mut self, name: &str) {
            self.owner
                .as_ref()
                .unwrap()
                .core()
                .control("lose_worker", &json!({ "session": name }))
                .unwrap();
            self.pump();
        }
    }

    /// EN-7 `en_7_accepted_returns_operation`, A.9: `create` answers the receipt; the id is minted by the Hub (32 hex
    /// digits); the row is `starting` or later, its label is the id, and its type is the `command` device type (S2-HC-3).
    #[test]
    fn create_a_command_answers_the_receipt_and_the_session_exists() {
        let mut rig = owned(config());
        let mut client = rig.control();
        req(
            &mut client,
            1,
            "create",
            "session",
            None,
            create_args(&["/bin/sh"]),
        );
        rig.pump();
        let res = receive(&mut client);
        let ok = &res["ok"];
        assert_eq!(ok["state"], "starting");
        assert_eq!(ok["session"]["hub_id"], "h");
        let session = ok["session"]["session_id"].as_str().unwrap().to_owned();
        assert_eq!(session.len(), 32);
        assert!(session.bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(!ok["operation_id"].as_str().unwrap().is_empty());
        req(
            &mut client,
            2,
            "get",
            "session",
            Some(&session),
            Value::Null,
        );
        rig.pump();
        let row = receive(&mut client);
        assert_eq!(row["ok"]["label"], session.as_str());
        assert_eq!(
            row["ok"]["session_type"],
            json!({"id": "command", "source": "device"})
        );
    }

    /// R-125 (as corrected): the create's `label` is the initial label; a size and an env reach the spawn.
    #[test]
    fn create_with_a_label_and_a_size() {
        let mut rig = owned(config());
        let mut client = rig.control();
        let args = json!({"command": {"argv": ["/bin/sh"], "cwd": "/", "env": {"A": "1"}},
            "size": {"rows": 30, "cols": 100}, "label": "build"});
        req(&mut client, 1, "create", "session", None, args);
        rig.pump();
        let session = receive(&mut client)["ok"]["session"]["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
        req(
            &mut client,
            2,
            "get",
            "session",
            Some(&session),
            Value::Null,
        );
        rig.pump();
        let row = receive(&mut client);
        assert_eq!(row["ok"]["label"], "build");
        assert_eq!(row["ok"]["size"], json!({"rows": 30, "cols": 100}));
    }

    /// A bad create is `invalid_arguments` with no session made and no record written (A.9, S2-HC-3).
    #[test]
    fn a_bad_create_is_refused_before_anything_is_made() {
        let mut rig = owned(config());
        let mut client = rig.control();
        let bad = [
            json!({}),
            json!({"command": {"argv": []}}),
            json!({"command": {"argv": [""]}}),
            json!({"command": {"argv": ["/bin/sh"]}, "target_id": "t"}),
            json!({"command": {"argv": ["/bin/sh"]}, "agent_scopes": ["x"]}),
            json!({"command": {"argv": ["/bin/sh"]}, "size": {"rows": 0, "cols": 80}}),
            json!({"session_type_id": "command"}),
            json!({"session_type_id": "nope"}),
        ];
        for (n, args) in bad.into_iter().enumerate() {
            req(
                &mut client,
                n as u64 + 1,
                "create",
                "session",
                None,
                args.clone(),
            );
            rig.pump();
            let res = receive(&mut client);
            assert_eq!(res["err"]["code"], "invalid_arguments", "{args}");
        }
        req(&mut client, 20, "list", "session", None, Value::Null);
        rig.pump();
        assert_eq!(receive(&mut client)["ok"]["items"], json!([]));
    }

    /// EN-8 and EN-7b are not served: a mutation with a key or an operation id does not run (no retry can double it).
    #[test]
    fn a_mutation_with_an_idempotency_key_does_not_run() {
        let mut rig = owned(config());
        let mut client = rig.control();
        send(
            &mut client,
            &json!({"type": "req", "id": "1", "verb": "create", "kind": "session",
                "idempotency_key": "k", "args": create_args(&["/bin/sh"])}),
        );
        rig.pump();
        let res = receive(&mut client);
        assert_eq!(res["err"]["code"], "unsupported");
        assert_eq!(res["err"]["reason"], "idempotency_not_served");
        assert_eq!(res["err"]["effect"], "none");
        req(&mut client, 2, "list", "session", None, Value::Null);
        rig.pump();
        assert_eq!(receive(&mut client)["ok"]["items"], json!([]));
    }

    /// KR-1 `kr_1_session_update_label_and_task` (the label part), VB-4: the answer is the row; `null` deletes the label,
    /// which falls back to the id; `task`, `if_rev` and a stranger are refused; an unknown session is `not_found`.
    /// WRITE-1, EN-5: an update whose row answer is over the bound ran. The label is stored, and the error says
    /// `effect: unknown`, not `none`.
    #[test]
    fn an_update_with_an_answer_over_the_bound_says_its_effect_is_unknown() {
        let mut rig = owned(config());
        rig.make_session("a");
        let mut client = rig.control();
        let max = 4096;
        rig.set_label("a", &"x".repeat(100));
        req(&mut client, 1, "get", "session", Some("a"), Value::Null);
        rig.pump();
        let base = body_len(&receive(&mut client));
        let over = 100 + (max - base) + 1;
        req(
            &mut client,
            2,
            "update",
            "session",
            Some("a"),
            json!({"patch": {"label": "y".repeat(over)}}),
        );
        rig.pump();
        let answer = receive(&mut client);
        assert_eq!(answer["err"]["code"], "response_too_large", "{answer}");
        assert_eq!(answer["err"]["effect"], "unknown", "{answer}");
        let stored = rig.owner.as_ref().unwrap().view()
            [&botster_core::contract::prelude::SessionId("a".into())]
            .labels
            .get(crate::client::rows::LABEL_KEY)
            .cloned();
        assert_eq!(stored, Some("y".repeat(over)), "the label was written");
    }

    #[test]
    fn update_sets_the_label_and_answers_the_row() {
        let mut rig = owned(config());
        rig.make_session("a");
        let mut client = rig.control();
        req(
            &mut client,
            1,
            "update",
            "session",
            Some("a"),
            json!({"patch": {"label": "build"}}),
        );
        rig.pump();
        assert_eq!(receive(&mut client)["ok"]["label"], "build");
        req(
            &mut client,
            2,
            "update",
            "session",
            Some("a"),
            json!({"patch": {"label": null}}),
        );
        rig.pump();
        assert_eq!(receive(&mut client)["ok"]["label"], "a");
        let refused = [
            (json!({"patch": {"task": "x"}}), "unsupported"),
            (
                json!({"patch": {"label": "x"}, "if_rev": "r"}),
                "unsupported",
            ),
            (json!({"patch": {"id": "z"}}), "invalid_arguments"),
            (json!({"patch": {"colour": "red"}}), "invalid_arguments"),
            (json!({"patch": {"label": 5}}), "invalid_arguments"),
            (json!({}), "invalid_arguments"),
        ];
        for (n, (args, code)) in refused.into_iter().enumerate() {
            req(
                &mut client,
                n as u64 + 10,
                "update",
                "session",
                Some("a"),
                args.clone(),
            );
            rig.pump();
            assert_eq!(receive(&mut client)["err"]["code"], code, "{args}");
        }
        req(
            &mut client,
            30,
            "update",
            "session",
            Some("nope"),
            json!({"patch": {"label": "x"}}),
        );
        req(
            &mut client,
            31,
            "update",
            "session",
            None,
            json!({"patch": {"label": "x"}}),
        );
        rig.pump();
        let mut res = [receive(&mut client), receive(&mut client)];
        res.sort_by_key(|r| r["id"].as_str().unwrap().to_owned());
        assert_eq!(res[0]["err"]["code"], "not_found");
        assert_eq!(res[1]["err"]["code"], "invalid_arguments");
    }

    /// VB-5, A.2c: `delete` is for an `ended` or `lost` session; a running one is `wrong_state`; after the delete the row is
    /// gone; an unknown session is `not_found`.
    #[test]
    fn delete_removes_an_ended_session_only() {
        let mut rig = owned(config());
        rig.make_session("a");
        let mut client = rig.control();
        req(&mut client, 1, "delete", "session", Some("a"), Value::Null);
        rig.pump();
        let res = receive(&mut client);
        assert_eq!(res["err"]["code"], "wrong_state", "{res}");
        assert_eq!(res["err"]["effect"], "none");
        rig.lose_worker("a");
        req(&mut client, 2, "get", "session", Some("a"), Value::Null);
        rig.pump();
        let row = receive(&mut client);
        assert_eq!(
            (
                row["ok"]["state"].as_str(),
                row["ok"]["failure_reason"].as_str()
            ),
            (Some("ended"), Some("worker_lost"))
        );
        req(&mut client, 3, "delete", "session", Some("a"), Value::Null);
        rig.pump();
        assert_eq!(receive(&mut client)["ok"], json!({}));
        req(&mut client, 4, "get", "session", Some("a"), Value::Null);
        req(&mut client, 5, "delete", "session", Some("a"), Value::Null);
        rig.pump();
        let mut res = [receive(&mut client), receive(&mut client)];
        res.sort_by_key(|r| r["id"].as_str().unwrap().to_owned());
        assert_eq!(res[0]["err"]["code"], "not_found");
        assert_eq!(res[1]["err"]["code"], "not_found");
    }

    fn invoke(client: &mut StdStream, id: u64, target: &str, action: &str, args: Value) {
        let mut frame = json!({"type": "req", "id": id.to_string(), "verb": "invoke",
            "kind": "session", "target": target, "action": action});
        if !args.is_null() {
            frame["args"] = args;
        }
        send(client, &frame);
    }

    /// `attach` on `session` through `client`, then the route connection with the answer's token. It returns the route id
    /// and the route's client end, which the test keeps open.
    fn attach_and_connect(
        rig: &mut Rig,
        client: &mut StdStream,
        id: u64,
        session: &str,
    ) -> (String, StdStream) {
        invoke(client, id, session, "attach", json!({"history": "none"}));
        rig.pump();
        let attached = receive(client);
        let route_id = attached["ok"]["route_id"]
            .as_str()
            .expect("{attached}")
            .to_owned();
        let token = attached["ok"]["transport"]["route_token"].as_str().unwrap();
        let mut route = rig.connect();
        send(
            &mut route,
            &json!({"frame": "route_connect", "route_id": route_id, "route_token": token}),
        );
        rig.pump();
        (route_id, route)
    }

    /// TS-1: `attach` answers the reservation and the transport; the route is in `list(route)` only after the connect
    /// (HS-5, A.3).
    #[test]
    fn attach_reserves_and_the_connect_makes_the_route_visible() {
        let mut rig = owned(config());
        rig.make_session("a");
        rig.start_session("a");
        let mut client = rig.control();
        invoke(&mut client, 1, "a", "attach", json!({"history": "none"}));
        rig.pump();
        let attached = receive(&mut client);
        let ok = &attached["ok"];
        assert_eq!(ok["transport"]["binding"], "local-stream", "{attached}");
        assert_eq!(
            ok["transport"]["socket_path"],
            rig.path.to_string_lossy().as_ref()
        );
        assert!(ok["transport"]["route_token"].as_str().unwrap().len() >= 32);
        assert!(ok["route_limits"]["max_frame_bytes"].as_u64().unwrap() > 0);
        req(&mut client, 2, "list", "route", None, Value::Null);
        rig.pump();
        assert_eq!(
            receive(&mut client)["ok"]["items"],
            json!([]),
            "reserved only"
        );
        let (route_id, _route) = attach_and_connect(&mut rig, &mut client, 3, "a");
        req(&mut client, 4, "list", "route", None, Value::Null);
        rig.pump();
        let listed = receive(&mut client);
        let items = listed["ok"]["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{listed}");
        assert_eq!(items[0]["id"], route_id.as_str());
        assert_eq!(
            items[0]["session"],
            json!({"hub_id": "h", "session_id": "a"})
        );
        assert_eq!(items[0]["binding"], "local-stream");
        assert!(items[0]["since"].as_u64().unwrap() > 0);
        req(&mut client, 5, "get", "route", Some(&route_id), Value::Null);
        req(&mut client, 6, "get", "route", Some("nope"), Value::Null);
        rig.pump();
        let mut got = [receive(&mut client), receive(&mut client)];
        got.sort_by_key(|r| r["id"].as_str().unwrap().to_owned());
        assert_eq!(got[0]["ok"]["id"], route_id.as_str());
        assert_eq!(got[1]["err"]["code"], "not_found");
        // A filter keeps to the core fields.
        req(
            &mut client,
            7,
            "list",
            "route",
            None,
            json!({"filter": {"session": {"hub_id": "h", "session_id": "b"}}}),
        );
        rig.pump();
        assert_eq!(receive(&mut client)["ok"]["items"], json!([]));
    }

    /// TS-10, BP-5: `detach` ends the route at Core, and Core's `RouteClosed` removes it from the table.
    #[test]
    fn detach_and_delete_end_a_route_of_the_caller_only() {
        let mut rig = owned(config());
        rig.make_session("a");
        rig.start_session("a");
        rig.make_session("b");
        let mut client = rig.control();
        let (first, _r1) = attach_and_connect(&mut rig, &mut client, 1, "a");
        // A route on another session is not found, and the route stays.
        invoke(&mut client, 2, "b", "detach", json!({"route_id": first}));
        rig.pump();
        let wrong = receive(&mut client);
        assert_eq!(wrong["err"]["code"], "not_found", "{wrong}");
        assert_eq!(wrong["err"]["effect"], "none");
        invoke(&mut client, 3, "a", "detach", json!({"route_id": first}));
        rig.pump();
        assert_eq!(receive(&mut client)["ok"], json!({}));
        req(&mut client, 4, "list", "route", None, Value::Null);
        rig.pump();
        assert_eq!(
            receive(&mut client)["ok"]["items"],
            json!([]),
            "RouteClosed removed it"
        );
        let (second, _r2) = attach_and_connect(&mut rig, &mut client, 5, "a");
        req(
            &mut client,
            6,
            "delete",
            "route",
            Some(&second),
            Value::Null,
        );
        rig.pump();
        assert_eq!(receive(&mut client)["ok"], json!({}));
        req(&mut client, 7, "list", "route", None, Value::Null);
        req(
            &mut client,
            8,
            "delete",
            "route",
            Some(&second),
            Value::Null,
        );
        rig.pump();
        let mut res = [receive(&mut client), receive(&mut client)];
        res.sort_by_key(|r| r["id"].as_str().unwrap().to_owned());
        assert_eq!(res[0]["ok"]["items"], json!([]));
        assert_eq!(res[1]["err"]["code"], "not_found", "already gone");
    }

    /// TS-1: the refusals before a route is reserved, and the parts that this Hub does not serve yet.
    #[test]
    fn attach_refusals_and_unserved_parts() {
        let mut rig = owned(config());
        rig.make_session("a");
        let mut client = rig.control();
        let cases = [
            (
                json!({"history": "none", "replace_route": "r"}),
                "unsupported",
                None,
            ),
            (
                json!({"history": "none", "binding": "webrtc"}),
                "unsupported",
                Some("binding_auth_unavailable"),
            ),
            (
                json!({"history": "none", "writable": false}),
                "unsupported",
                None,
            ),
            (json!({"history": "some"}), "invalid_arguments", None),
            (json!({}), "invalid_arguments", None),
        ];
        for (n, (args, code, reason)) in cases.into_iter().enumerate() {
            invoke(&mut client, 10 + n as u64, "a", "attach", args);
            rig.pump();
            let res = receive(&mut client);
            assert_eq!(res["err"]["code"], code, "{res}");
            assert_eq!(res["err"]["effect"], "none", "{res}");
            assert_eq!(res["err"]["reason"].as_str(), reason, "{res}");
        }
        invoke(
            &mut client,
            30,
            "nope",
            "attach",
            json!({"history": "none"}),
        );
        rig.pump();
        assert_eq!(receive(&mut client)["err"]["code"], "not_found");
        req(&mut client, 31, "watch", "route", None, Value::Null);
        rig.pump();
        assert_eq!(receive(&mut client)["err"]["code"], "unsupported");
        // No terminal format in common with the session's worker: `unsupported { terminal_format }`.
        let mut odd = rig.connect();
        let mut hello = hello(1, 1);
        hello["terminal_formats"] = json!(["nope/1"]);
        send(&mut odd, &hello);
        rig.pump();
        receive(&mut odd);
        invoke(&mut odd, 1, "a", "attach", json!({"history": "none"}));
        rig.pump();
        let res = receive(&mut odd);
        assert_eq!(res["err"]["code"], "unsupported", "{res}");
        assert_eq!(res["err"]["reason"], "terminal_format");
    }
}
