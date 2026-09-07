//! Reusable same-device client protocol for a running `botster-hub` daemon.
//!
//! This crate owns the client-to-hub daemon socket request, response, event,
//! handshake, and connection helpers. It intentionally contains no hub runtime,
//! TUI, Lua, or daemon-to-session-worker protocol dependencies.
//!
//! # Host-control protocol 9
//!
//! Every frame on the Unix socket is one length-prefixed container:
//!
//! ```text
//! UnixFrame = [u32 LE frame_len][u8 container][payload]    frame_len = 1 + payload.len()
//! container 1 CONTROL   payload = UTF-8 JSON `ClientFrame` (client to Hub) or `ServerFrame` (Hub to client)
//! container 2 TERMINAL  payload = [u16 LE route_len][route UTF-8][u64 LE generation][u32 LE stream_epoch][body]
//! ```
//!
//! A terminal container body is opaque to this crate. Hub to client it is one
//! Core terminal-stream body; client to Hub it is one Core terminal input frame.
//! `generation` is the fixed attachment generation for the life of the route.
//! `stream_epoch` is Core routing metadata captured when the frame was queued:
//! Hub copies it verbatim and never advances it; a client adopts a new epoch
//! only from a `ROUTE_RESYNC` whose `from_epoch` equals its accepted epoch and
//! whose envelope epoch equals `to_epoch`, and drops any data frame carrying a
//! different epoch. Client to Hub, the field is always 0 (reserved); Hub
//! validates only the route and the fixed generation and never rejects input
//! on that field.
//! Control requests carry a client-chosen `request_id` (canonical decimal `u64`,
//! strictly increasing per connection). Hub may complete requests out of order;
//! the response echoes the id.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io::{BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
#[cfg(test)]
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use botster_ui_contract::{
    PackageNoticeReactionDescriptor, PackageSurfaceDescriptor, UiActionRequest, UiActionResult,
    UiNode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use botster_terminal_protocol::ensure_compatible as ensure_terminal_compatible;
pub use botster_terminal_protocol::{
    TerminalCompatibility, TerminalCompatibilityError, TerminalCompatibilityRequirement,
};

mod typescript;

pub const PROTOCOL: &str = "botster-hub-daemon-v1";
/// Host-control protocol version. Any other version is rejected at Hello; there is no negotiation.
pub const PROTOCOL_VERSION: u16 = 9;
pub const CONFORMANCE_FIXTURE_REVISION: u16 = 49;
/// Oldest conformance revision accepted by the default first-party client requirement.
///
/// Protocol 9 is a cold cut: the floor equals the current revision.
pub const DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION: u16 = 49;
/// Maximum byte length of a `request_id`: a canonical positive decimal `u64`, no leading zeros.
pub const MAX_REQUEST_ID_BYTES: usize = 20;
/// Outstanding (unanswered) control requests one connection may hold.
///
/// A valid request beyond this bound receives a correlated `too_many_requests`
/// operator error response. Terminal streams on that connection are unaffected.
pub const MAX_OUTSTANDING_REQUESTS: usize = 32;
/// Maximum control request frame payload. A larger declared frame closes the connection.
pub const MAX_CONTROL_REQUEST_BYTES: usize = 1 << 20;
/// Maximum control response frame payload. Large data is paged, never oversized.
pub const MAX_CONTROL_RESPONSE_BYTES: usize = 1 << 20;
/// Bytes per `ReadSnapshotPage` payload.
pub const SNAPSHOT_PAGE_BYTES: usize = 256 * 1024;
/// Open snapshot captures one connection may hold.
pub const MAX_OPEN_CAPTURES_PER_CONNECTION: usize = 4;
/// Seconds a snapshot capture stays readable after `CaptureSnapshot`.
pub const SNAPSHOT_CAPTURE_TTL_SECONDS: u32 = 60;
/// Operator error code for the 33rd outstanding request on one connection.
pub const OPERATOR_ERROR_TOO_MANY_REQUESTS: &str = "too_many_requests";
/// Bytes of the Unix frame length prefix.
pub const UNIX_FRAME_LENGTH_PREFIX_BYTES: usize = 4;
/// Unix container tag for UTF-8 JSON `ClientFrame` / `ServerFrame` payloads.
pub const UNIX_CONTAINER_CONTROL: u8 = 1;
/// Unix container tag for one routed terminal body.
pub const UNIX_CONTAINER_TERMINAL: u8 = 2;
/// Maximum route id length inside a terminal container (UTF-8 bytes).
pub const MAX_UNIX_TERMINAL_ROUTE_BYTES: usize = 1024;
/// Fixed terminal container bytes around the route: `u16` route length, `u64`
/// generation, and `u32` stream epoch.
pub const UNIX_TERMINAL_CONTAINER_FIXED_BYTES: usize = 2 + 8 + 4;
/// Largest Unix frame (container byte plus payload) this crate reads or writes.
///
/// Control payloads are bounded separately by [`MAX_CONTROL_REQUEST_BYTES`] and
/// [`MAX_CONTROL_RESPONSE_BYTES`]. A terminal body is bounded by the Core
/// route egress cap (4 MiB); this ceiling adds the container header and the
/// longest route so no Core body is ever too large to frame.
pub const MAX_UNIX_FRAME_BYTES: usize =
    (4 << 20) + 1 + UNIX_TERMINAL_CONTAINER_FIXED_BYTES + MAX_UNIX_TERMINAL_ROUTE_BYTES;
/// Version of the local WebRTC delivery chunk framing protocol.
pub const LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION: u16 = 2;
/// Serialized local WebRTC delivery frames must remain strictly below this size.
pub const LOCAL_WEBRTC_MAX_FRAME_BYTES: usize = 64 * 1024;
/// Maximum serialized encrypted delivery envelope accepted for reassembly.
pub const LOCAL_WEBRTC_MAX_DELIVERY_BYTES: usize = 16 * 1024 * 1024;
/// Fixed header bytes of one binary local WebRTC terminal chunk.
///
/// ```text
/// offset  0  u8      version = 2
/// offset  1  u64 LE  message_id      per channel, per direction, strictly increasing from 1
/// offset  9  u32 LE  chunk_index
/// offset 13  u32 LE  chunk_count
/// offset 17  u32 LE  total_bytes     plaintext length of the whole message
/// offset 21  u64 LE  generation      fixed attachment generation of the route
/// offset 29  u32 LE  stream_epoch    Core routing metadata, copied verbatim
/// offset 33  [12-byte nonce][AES-GCM ciphertext || 16-byte tag]
/// ```
///
/// The route is the subscription DataChannel label. Reassembly identity is
/// `(label, direction, message_id)`; chunks of one message are contiguous on
/// the ordered channel in `chunk_index` order, every chunk repeats the same
/// `generation` and `stream_epoch`, and each sealed slice decrypts to one
/// contiguous slice of the plaintext body. A mismatch drops the message and
/// closes that channel only.
pub const LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES: usize = 1 + 8 + 4 + 4 + 4 + 8 + 4;
/// AES-GCM nonce bytes in a sealed terminal chunk slice.
pub const LOCAL_WEBRTC_TERMINAL_CHUNK_NONCE_BYTES: usize = 12;
/// AES-GCM tag bytes in a sealed terminal chunk slice.
pub const LOCAL_WEBRTC_TERMINAL_CHUNK_TAG_BYTES: usize = 16;
pub const FEATURE_SESSIONS: &str = "sessions";
pub const FEATURE_PLUGIN_SURFACE_RENDER: &str = "plugin_surface_render";
pub const FEATURE_PLUGIN_SURFACE_ACTION: &str = "plugin_surface_action";
pub const FEATURE_PACKAGE_ROUTES: &str = "package_routes";
pub const FEATURE_PACKAGE_NAVIGATION: &str = "package_navigation";
pub const FEATURE_SPAWN_TARGETS: &str = "spawn_targets";
pub const FEATURE_WORKTREES: &str = "worktrees";
pub const FEATURE_TERMINAL_READBACK: &str = "terminal_readback";
pub const FEATURE_SESSION_ENTITY_SUBSCRIPTIONS: &str = "session_entity_subscriptions";
pub const FEATURE_SESSION_TYPE_ENTITY_SUBSCRIPTIONS: &str = "session_type_entity_subscriptions";
pub const FEATURE_PLUGIN_ENTITY_SUBSCRIPTIONS: &str = "plugin_entity_subscriptions";
pub const FEATURE_HUB_SOURCE_UPDATE: &str = "hub_source_update";
/// Optional Hub Unix adapter plane. Bind happens only when Hello requires this.
pub const FEATURE_UNIX_TERMINAL_ADAPTER: &str = "unix_terminal_adapter";
/// Optional host event when a bound terminal subscription closes and the socket stays up.
pub const FEATURE_TERMINAL_SUBSCRIPTION_CLOSED: &str = "terminal_subscription_closed";
/// Hub closed this bound adapter while the connection stayed alive.
pub const TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER: &str = "host_adapter_closed";
/// Core closed this bound adapter while the connection stayed alive.
pub const TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER: &str = "core_adapter_closed";
/// Reserved WebRTC subscription channel opened after the reservation expired.
pub const TERMINAL_SUBSCRIPTION_CLOSED_RESERVATION_EXPIRED: &str = "reservation_expired";
/// Optional Hub WebRTC adapter plane. Bind happens only when DataChannel Hello requires this.
pub const FEATURE_WEBRTC_TERMINAL_ADAPTER: &str = "webrtc_terminal_adapter";
/// Optional named attach occupancy on `DaemonStatus`. Empty occupancy without this token is not absence proof.
pub const FEATURE_ATTACH_OCCUPANCY: &str = "attach_occupancy";
/// Optional host-control package-event subscriptions. Not a terminal feature.
pub const FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS: &str = "package_event_subscriptions";

/// Authenticated plaintext carried by one complete local WebRTC control delivery.
///
/// Terminal bodies never travel in JSON chunks; they use the binary chunk
/// layout documented on [`LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonLocalWebrtcDeliveryKind {
    /// One serialized [`ServerFrame`]: a correlated response, hello ack, event, entity frame, or close.
    ServerFrame,
}

/// One frame of an encrypted daemon delivery sent over the local WebRTC DataChannel.
///
/// `payload` is a contiguous UTF-8 slice of the serialized encrypted AES-GCM
/// envelope. Clients must validate all declared bounds before concatenating the
/// payloads and decrypt only after the complete envelope has been reassembled.
/// The chunk header exists for reassembly only; request correlation uses the
/// `request_id` inside the decrypted [`ServerFrame`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonLocalWebrtcDeliveryChunk {
    pub version: u16,
    pub delivery_kind: DaemonLocalWebrtcDeliveryKind,
    pub message_id: String,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub total_bytes: u32,
    pub payload: String,
}

/// One control frame sent by a client. Serialized as JSON with a `frame` tag.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum ClientFrame {
    /// First frame on a connection.
    Hello { hello: DaemonHello },
    /// One operator request. `request_id` is a canonical decimal `u64`,
    /// strictly increasing for the connection lifetime.
    Request {
        request_id: String,
        request: DaemonRequest,
    },
}

/// One control frame sent by Hub. Serialized as JSON with a `frame` tag.
///
/// Hub may complete requests out of order. Events on one subscription stay
/// ordered relative to each other; no other cross-frame ordering is promised.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum ServerFrame {
    /// Reply to [`ClientFrame::Hello`].
    HelloAck { ack: DaemonHelloAck },
    /// Correlated reply to one [`ClientFrame::Request`].
    Response {
        request_id: String,
        response: DaemonResponse,
    },
    /// Unsolicited host event.
    Event { event: DaemonEvent },
    /// One entity subscription frame.
    Entity { entity: DaemonEntityFrame },
    /// Typed reason sent before Hub closes this connection, when Hub can still write.
    Close { reason: DaemonCloseReason },
}

/// Typed reason for a Hub-initiated connection close.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum DaemonCloseReason {
    /// The client violated the framing or correlation contract.
    ProtocolError { code: DaemonProtocolErrorCode },
    /// Hub is shutting down.
    DaemonShutdown,
}

/// Protocol violations that close the offending connection.
///
/// Closing one connection never affects another connection's sessions or routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonProtocolErrorCode {
    /// A frame did not decode as its declared container.
    MalformedFrame,
    /// A declared frame length exceeded the request bound.
    FrameTooLarge,
    /// An unknown container tag.
    UnknownContainer,
    /// An unknown `frame` tag.
    UnknownFrame,
    /// A `request_id` that is not a canonical decimal `u64`.
    InvalidRequestId,
    /// A `request_id` that did not increase.
    NonincreasingRequestId,
    /// A request before or instead of Hello, or a second Hello.
    HandshakeOrder,
    /// A terminal container with an invalid route or generation.
    InvalidRoute,
    /// A terminal input body that failed the fixed header check.
    InvalidInputHeader,
}

impl DaemonProtocolErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedFrame => "malformed_frame",
            Self::FrameTooLarge => "frame_too_large",
            Self::UnknownContainer => "unknown_container",
            Self::UnknownFrame => "unknown_frame",
            Self::InvalidRequestId => "invalid_request_id",
            Self::NonincreasingRequestId => "nonincreasing_request_id",
            Self::HandshakeOrder => "handshake_order",
            Self::InvalidRoute => "invalid_route",
            Self::InvalidInputHeader => "invalid_input_header",
        }
    }
}

/// Encode a request id in its canonical wire form.
#[must_use]
pub fn encode_request_id(request_id: u64) -> String {
    request_id.to_string()
}

/// Parse a canonical request id: 1..=20 ASCII digits, no leading zero, nonzero.
#[must_use]
pub fn parse_request_id(request_id: &str) -> Option<u64> {
    let bytes = request_id.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_REQUEST_ID_BYTES || bytes[0] == b'0' {
        return None;
    }
    if !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    request_id.parse::<u64>().ok()
}

/// Strictly increasing request id source for one connection.
#[derive(Debug, Default, Clone)]
pub struct RequestIdSequence {
    last: u64,
}

impl RequestIdSequence {
    #[must_use]
    pub const fn new() -> Self {
        Self { last: 0 }
    }

    /// Next id. Ids start at 1 and never repeat within one connection.
    pub fn next(&mut self) -> u64 {
        self.last = self.last.saturating_add(1);
        self.last
    }

    /// Last id handed out, or 0 before the first request.
    #[must_use]
    pub const fn last(&self) -> u64 {
        self.last
    }
}

/// Client-side outcome of one submitted request that did not receive a response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonRequestError {
    /// The caller's deadline passed before the correlated response arrived.
    DeadlineExpired,
    /// The caller cancelled the request locally. Hub may still complete it.
    Cancelled,
    /// The connection closed before the correlated response arrived.
    ConnectionClosed,
    /// The local guard refused a 33rd outstanding request.
    TooManyOutstandingRequests,
}

impl fmt::Display for DaemonRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExpired => write!(formatter, "daemon request deadline expired"),
            Self::Cancelled => write!(formatter, "daemon request cancelled"),
            Self::ConnectionClosed => {
                write!(formatter, "daemon connection closed before the response")
            }
            Self::TooManyOutstandingRequests => write!(
                formatter,
                "daemon connection already holds {MAX_OUTSTANDING_REQUESTS} outstanding requests"
            ),
        }
    }
}

impl Error for DaemonRequestError {}

/// One decoded terminal container. The body stays opaque to this crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonUnixTerminalFrame {
    /// Subscription id of the route. 1..=1024 UTF-8 bytes.
    pub route: String,
    /// Fixed attachment generation minted by Core at attach.
    pub generation: u64,
    /// Core stream epoch captured when the frame was queued. See the crate docs.
    pub stream_epoch: u32,
    /// Hub to client: one Core terminal-stream body. Client to Hub: one Core input frame.
    pub body: Vec<u8>,
}

/// Header of one Unix terminal container, built on the stack for `writev`.
///
/// Layout after the length prefix and container byte, with offsets relative
/// to the container payload:
///
/// ```text
/// offset 0             u16 LE  route_len       1..=1024
/// offset 2             route   UTF-8, route_len bytes
/// offset 2+route_len   u64 LE  generation      fixed attachment generation
/// offset 10+route_len  u32 LE  stream_epoch    Core routing metadata
/// offset 14+route_len  body
/// ```
#[derive(Debug, Clone)]
pub struct UnixTerminalContainerHeader {
    bytes: [u8; UNIX_FRAME_LENGTH_PREFIX_BYTES
        + 1
        + UNIX_TERMINAL_CONTAINER_FIXED_BYTES
        + MAX_UNIX_TERMINAL_ROUTE_BYTES],
    len: usize,
}

impl UnixTerminalContainerHeader {
    /// Build the header for a body of `body_len` bytes.
    ///
    /// Returns `None` when the route is empty, longer than
    /// [`MAX_UNIX_TERMINAL_ROUTE_BYTES`], or the frame would exceed
    /// [`MAX_UNIX_FRAME_BYTES`].
    #[must_use]
    pub fn new(route: &str, generation: u64, stream_epoch: u32, body_len: usize) -> Option<Self> {
        let route_bytes = route.as_bytes();
        if route_bytes.is_empty() || route_bytes.len() > MAX_UNIX_TERMINAL_ROUTE_BYTES {
            return None;
        }
        let payload_len = UNIX_TERMINAL_CONTAINER_FIXED_BYTES + route_bytes.len() + body_len;
        let frame_len = 1 + payload_len;
        if frame_len > MAX_UNIX_FRAME_BYTES {
            return None;
        }
        let mut bytes = [0u8; UNIX_FRAME_LENGTH_PREFIX_BYTES
            + 1
            + UNIX_TERMINAL_CONTAINER_FIXED_BYTES
            + MAX_UNIX_TERMINAL_ROUTE_BYTES];
        let mut cursor = 0;
        bytes[cursor..cursor + 4].copy_from_slice(&(frame_len as u32).to_le_bytes());
        cursor += 4;
        bytes[cursor] = UNIX_CONTAINER_TERMINAL;
        cursor += 1;
        bytes[cursor..cursor + 2].copy_from_slice(&(route_bytes.len() as u16).to_le_bytes());
        cursor += 2;
        bytes[cursor..cursor + route_bytes.len()].copy_from_slice(route_bytes);
        cursor += route_bytes.len();
        bytes[cursor..cursor + 8].copy_from_slice(&generation.to_le_bytes());
        cursor += 8;
        bytes[cursor..cursor + 4].copy_from_slice(&stream_epoch.to_le_bytes());
        cursor += 4;
        Some(Self { bytes, len: cursor })
    }

    /// The encoded header bytes, ready for the first `writev` slice.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// Encode one complete terminal container (header plus body) into a new buffer.
#[must_use]
pub fn encode_unix_terminal_frame(
    route: &str,
    generation: u64,
    stream_epoch: u32,
    body: &[u8],
) -> Option<Vec<u8>> {
    let header = UnixTerminalContainerHeader::new(route, generation, stream_epoch, body.len())?;
    let mut frame = Vec::with_capacity(header.as_bytes().len() + body.len());
    frame.extend_from_slice(header.as_bytes());
    frame.extend_from_slice(body);
    Some(frame)
}

/// Encode one control container carrying `frame` as UTF-8 JSON.
pub fn encode_control_frame<T: Serialize>(frame: &T) -> DaemonTransportResult<Vec<u8>> {
    let json = serde_json::to_vec(frame).map_err(DaemonTransportError::Json)?;
    if json.len() > MAX_CONTROL_REQUEST_BYTES.max(MAX_CONTROL_RESPONSE_BYTES) {
        return Err(DaemonTransportError::Protocol(
            "control frame exceeds the protocol byte bound",
        ));
    }
    let frame_len = 1 + json.len();
    let mut bytes = Vec::with_capacity(UNIX_FRAME_LENGTH_PREFIX_BYTES + frame_len);
    bytes.extend_from_slice(&(frame_len as u32).to_le_bytes());
    bytes.push(UNIX_CONTAINER_CONTROL);
    bytes.extend_from_slice(&json);
    Ok(bytes)
}

/// Encode one [`ClientFrame`] as a control container.
pub fn encode_client_frame(frame: &ClientFrame) -> DaemonTransportResult<Vec<u8>> {
    encode_control_frame(frame)
}

/// Encode one [`ServerFrame`] as a control container.
pub fn encode_server_frame(frame: &ServerFrame) -> DaemonTransportResult<Vec<u8>> {
    encode_control_frame(frame)
}

/// Decode the payload of one Unix frame (container byte plus payload) without the length prefix.
///
/// `decode_control` turns a control payload into the caller's frame type so the
/// same decoder serves Hub (expects `ClientFrame`) and clients (expect `ServerFrame`).
pub fn decode_unix_frame<T: for<'de> Deserialize<'de>>(
    frame: &[u8],
) -> Result<DaemonUnixFrame<T>, DaemonProtocolErrorCode> {
    let Some((&container, payload)) = frame.split_first() else {
        return Err(DaemonProtocolErrorCode::MalformedFrame);
    };
    match container {
        UNIX_CONTAINER_CONTROL => serde_json::from_slice(payload)
            .map(DaemonUnixFrame::Control)
            .map_err(|_| DaemonProtocolErrorCode::MalformedFrame),
        UNIX_CONTAINER_TERMINAL => {
            if payload.len() < UNIX_TERMINAL_CONTAINER_FIXED_BYTES {
                return Err(DaemonProtocolErrorCode::MalformedFrame);
            }
            let route_len = usize::from(u16::from_le_bytes([payload[0], payload[1]]));
            if route_len == 0
                || route_len > MAX_UNIX_TERMINAL_ROUTE_BYTES
                || payload.len() < UNIX_TERMINAL_CONTAINER_FIXED_BYTES + route_len
            {
                return Err(DaemonProtocolErrorCode::InvalidRoute);
            }
            let route = std::str::from_utf8(&payload[2..2 + route_len])
                .map_err(|_| DaemonProtocolErrorCode::InvalidRoute)?;
            if route.chars().any(char::is_control) {
                return Err(DaemonProtocolErrorCode::InvalidRoute);
            }
            let generation_start = 2 + route_len;
            let mut generation = [0u8; 8];
            generation.copy_from_slice(&payload[generation_start..generation_start + 8]);
            let epoch_start = generation_start + 8;
            let mut stream_epoch = [0u8; 4];
            stream_epoch.copy_from_slice(&payload[epoch_start..epoch_start + 4]);
            Ok(DaemonUnixFrame::Terminal(DaemonUnixTerminalFrame {
                route: route.to_string(),
                generation: u64::from_le_bytes(generation),
                stream_epoch: u32::from_le_bytes(stream_epoch),
                body: payload[epoch_start + 4..].to_vec(),
            }))
        }
        _ => Err(DaemonProtocolErrorCode::UnknownContainer),
    }
}

/// One decoded Unix frame, generic over the control payload type.
#[derive(Debug, Clone, PartialEq)]
pub enum DaemonUnixFrame<T> {
    Control(T),
    Terminal(DaemonUnixTerminalFrame),
}

/// Incremental reader for length-prefixed Unix frames.
///
/// A read timeout keeps the partial frame in the reader; the next call resumes.
/// One reader belongs to one socket.
#[derive(Debug, Default)]
pub struct DaemonUnixFrameReader {
    prefix: [u8; UNIX_FRAME_LENGTH_PREFIX_BYTES],
    prefix_filled: usize,
    frame: Vec<u8>,
    frame_len: usize,
    frame_filled: usize,
}

impl DaemonUnixFrameReader {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// True when a partial frame is buffered.
    #[must_use]
    pub fn has_partial_frame(&self) -> bool {
        self.prefix_filled > 0 || self.frame_filled > 0
    }

    /// Read one complete raw frame (container byte plus payload).
    ///
    /// `max_frame_bytes` bounds the declared length; a larger declaration is a
    /// protocol error and the caller closes the socket.
    pub fn read_raw_frame<R: Read>(
        &mut self,
        reader: &mut R,
        max_frame_bytes: usize,
    ) -> DaemonTransportResult<Vec<u8>> {
        while self.prefix_filled < UNIX_FRAME_LENGTH_PREFIX_BYTES {
            let read = reader
                .read(&mut self.prefix[self.prefix_filled..])
                .map_err(normalize_socket_io_error)?;
            if read == 0 {
                return Err(if self.prefix_filled == 0 {
                    DaemonTransportError::ClientDisconnected
                } else {
                    DaemonTransportError::Protocol("truncated unix frame length prefix")
                });
            }
            self.prefix_filled += read;
            if self.prefix_filled == UNIX_FRAME_LENGTH_PREFIX_BYTES {
                let declared = u32::from_le_bytes(self.prefix) as usize;
                if declared == 0 || declared > max_frame_bytes {
                    return Err(DaemonTransportError::Protocol(
                        "unix frame length is zero or exceeds the bound",
                    ));
                }
                self.frame_len = declared;
                self.frame = vec![0u8; declared];
                self.frame_filled = 0;
            }
        }
        while self.frame_filled < self.frame_len {
            let read = reader
                .read(&mut self.frame[self.frame_filled..])
                .map_err(normalize_socket_io_error)?;
            if read == 0 {
                return Err(DaemonTransportError::Protocol("truncated unix frame"));
            }
            self.frame_filled += read;
        }
        self.prefix_filled = 0;
        self.frame_len = 0;
        self.frame_filled = 0;
        Ok(std::mem::take(&mut self.frame))
    }

    /// Read and decode one frame whose control payload is a [`ServerFrame`].
    pub fn read_frame<R: Read>(
        &mut self,
        reader: &mut R,
    ) -> DaemonTransportResult<DaemonUnixMuxFrame> {
        let raw = self.read_raw_frame(reader, MAX_UNIX_FRAME_BYTES)?;
        match decode_unix_frame::<ServerFrame>(&raw) {
            Ok(DaemonUnixFrame::Control(frame)) => Ok(DaemonUnixMuxFrame::Server(frame)),
            Ok(DaemonUnixFrame::Terminal(frame)) => Ok(DaemonUnixMuxFrame::Terminal(frame)),
            Err(code) => Err(DaemonTransportError::ProtocolViolation(code)),
        }
    }
}

/// One decoded frame on a client's Unix connection.
#[derive(Debug, Clone, PartialEq)]
pub enum DaemonUnixMuxFrame {
    Server(ServerFrame),
    Terminal(DaemonUnixTerminalFrame),
}

/// Write one [`ClientFrame`] to the socket.
pub fn write_client_frame(
    stream: &mut UnixStream,
    frame: &ClientFrame,
) -> DaemonTransportResult<()> {
    let bytes = encode_client_frame(frame)?;
    stream.write_all(&bytes).map_err(normalize_socket_io_error)
}

/// Write one [`ServerFrame`] to the socket.
pub fn write_server_frame(
    stream: &mut UnixStream,
    frame: &ServerFrame,
) -> DaemonTransportResult<()> {
    let bytes = encode_server_frame(frame)?;
    stream.write_all(&bytes).map_err(normalize_socket_io_error)
}

/// Write one terminal container (header then body) to the socket.
pub fn write_unix_terminal_frame(
    stream: &mut UnixStream,
    route: &str,
    generation: u64,
    stream_epoch: u32,
    body: &[u8],
) -> DaemonTransportResult<()> {
    let header = UnixTerminalContainerHeader::new(route, generation, stream_epoch, body.len())
        .ok_or(DaemonTransportError::Protocol(
            "invalid terminal route or frame size",
        ))?;
    stream
        .write_all(header.as_bytes())
        .map_err(normalize_socket_io_error)?;
    stream.write_all(body).map_err(normalize_socket_io_error)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonEndpoint {
    pub socket_path: PathBuf,
}

impl DaemonEndpoint {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }
}

/// Connect to a daemon and send one operator request.
pub fn request(
    endpoint: &DaemonEndpoint,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    request_with_requirement(
        endpoint,
        request,
        &DaemonCompatibilityRequirement::current(),
    )
}

/// Connect with an explicit compatibility requirement and send one request.
pub fn request_with_requirement(
    endpoint: &DaemonEndpoint,
    request: DaemonRequest,
    requirement: &DaemonCompatibilityRequirement,
) -> DaemonTransportResult<DaemonResponse> {
    let mut connection = DaemonConnection::connect_with_requirement(endpoint, requirement)?;
    connection.request(&request)
}

/// Persistent daemon connection for clients that own attach subscription state.
///
/// ```no_run
/// let endpoint = botster_hub_client::DaemonEndpoint::new("/tmp/botster-hub.sock");
/// let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint)?;
/// let response = connection.request(&botster_hub_client::DaemonRequest::Status)?;
/// # Ok::<(), botster_hub_client::DaemonTransportError>(())
/// ```
///
/// Requests are correlated by `request_id`. [`Self::submit`] sends one request
/// and returns its id; [`Self::wait_response`] blocks for that id and parks any
/// other correlated response, event, entity frame, or terminal frame that
/// arrives first. [`Self::request`] combines both.
pub struct DaemonConnection {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
    frames: DaemonUnixFrameReader,
    ids: RequestIdSequence,
    outstanding: Vec<u64>,
    parked_responses: Vec<(u64, DaemonResponse)>,
    skipped_terminal: Vec<DaemonUnixTerminalFrame>,
    skipped_events: Vec<DaemonEvent>,
    skipped_entity_frames: Vec<DaemonEntityFrame>,
    required_features: Vec<String>,
    closed: Option<DaemonCloseReason>,
}

impl DaemonConnection {
    /// Connect to the daemon and complete the socket protocol handshake.
    pub fn connect(endpoint: &DaemonEndpoint) -> DaemonTransportResult<Self> {
        Self::connect_with_requirement(endpoint, &DaemonCompatibilityRequirement::current())
    }

    /// Connect with an explicit Hello requirement.
    pub fn connect_with_requirement(
        endpoint: &DaemonEndpoint,
        requirement: &DaemonCompatibilityRequirement,
    ) -> DaemonTransportResult<Self> {
        Self::connect_with_terminal_requirement(endpoint, requirement, None)
    }

    /// Connect with a host requirement and an optional terminal requirement.
    pub fn connect_with_terminal_requirement(
        endpoint: &DaemonEndpoint,
        requirement: &DaemonCompatibilityRequirement,
        terminal_compatibility: Option<&TerminalCompatibilityRequirement>,
    ) -> DaemonTransportResult<Self> {
        let (stream, _ack) = connect_and_hello_with_terminal_requirement(
            endpoint,
            requirement,
            terminal_compatibility,
        )?;
        Self::from_hello_complete_stream(stream, requirement.required_features.clone())
    }

    /// Wrap a stream whose Hello handshake already completed.
    pub fn from_hello_complete_stream(
        stream: UnixStream,
        required_features: Vec<String>,
    ) -> DaemonTransportResult<Self> {
        let reader = BufReader::new(stream.try_clone().map_err(normalize_socket_io_error)?);
        Ok(Self {
            stream,
            reader,
            frames: DaemonUnixFrameReader::new(),
            ids: RequestIdSequence::new(),
            outstanding: Vec::new(),
            parked_responses: Vec::new(),
            skipped_terminal: Vec::new(),
            skipped_events: Vec::new(),
            skipped_entity_frames: Vec::new(),
            required_features,
            closed: None,
        })
    }

    /// Ids submitted on this connection that have no response yet.
    #[must_use]
    pub fn outstanding_request_ids(&self) -> &[u64] {
        &self.outstanding
    }

    /// Typed close reason Hub sent before closing, when one arrived.
    #[must_use]
    pub fn close_reason(&self) -> Option<&DaemonCloseReason> {
        self.closed.as_ref()
    }

    /// Send one request without waiting. Returns its `request_id`.
    ///
    /// Refuses locally when [`MAX_OUTSTANDING_REQUESTS`] ids are unanswered.
    pub fn submit(&mut self, request: &DaemonRequest) -> DaemonTransportResult<u64> {
        if self.outstanding.len() >= MAX_OUTSTANDING_REQUESTS {
            return Err(DaemonTransportError::Request(
                DaemonRequestError::TooManyOutstandingRequests,
            ));
        }
        let request_id = self.ids.next();
        write_client_frame(
            &mut self.stream,
            &ClientFrame::Request {
                request_id: encode_request_id(request_id),
                request: request.clone(),
            },
        )?;
        self.outstanding.push(request_id);
        Ok(request_id)
    }

    /// Forget a submitted id. A later response for it is discarded.
    pub fn cancel(&mut self, request_id: u64) -> bool {
        let before = self.outstanding.len();
        self.outstanding.retain(|id| *id != request_id);
        self.parked_responses.retain(|(id, _)| *id != request_id);
        before != self.outstanding.len()
    }

    /// Block until the correlated response for `request_id` arrives.
    pub fn wait_response(&mut self, request_id: u64) -> DaemonTransportResult<DaemonResponse> {
        if let Some(index) = self
            .parked_responses
            .iter()
            .position(|(id, _)| *id == request_id)
        {
            return Ok(self.parked_responses.remove(index).1);
        }
        if !self.outstanding.contains(&request_id) {
            return Err(DaemonTransportError::Request(DaemonRequestError::Cancelled));
        }
        loop {
            match self.read_next_frame()? {
                DaemonUnixMuxFrame::Server(ServerFrame::Response {
                    request_id: id,
                    response,
                }) => {
                    let Some(id) = parse_request_id(&id) else {
                        return Err(DaemonTransportError::ProtocolViolation(
                            DaemonProtocolErrorCode::InvalidRequestId,
                        ));
                    };
                    if id == request_id {
                        return Ok(response);
                    }
                    if self.outstanding.contains(&id) {
                        self.parked_responses.push((id, response));
                    }
                }
                DaemonUnixMuxFrame::Server(ServerFrame::Event { event }) => {
                    self.skipped_events.push(event);
                }
                DaemonUnixMuxFrame::Server(ServerFrame::Entity { entity: frame }) => {
                    self.skipped_entity_frames.push(frame);
                }
                DaemonUnixMuxFrame::Server(ServerFrame::Close { reason }) => {
                    self.closed = Some(reason.clone());
                    return Err(DaemonTransportError::ClosedByHub(reason));
                }
                DaemonUnixMuxFrame::Server(ServerFrame::HelloAck { .. }) => {
                    return Err(DaemonTransportError::Protocol(
                        "unexpected hello ack after the handshake",
                    ));
                }
                DaemonUnixMuxFrame::Terminal(frame) => self.skipped_terminal.push(frame),
            }
        }
    }

    /// Send one request over this persistent connection and wait for its response.
    pub fn request(&mut self, request: &DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
        let request_id = self.submit(request)?;
        self.wait_response(request_id)
    }

    fn read_next_frame(&mut self) -> DaemonTransportResult<DaemonUnixMuxFrame> {
        let frame = self.frames.read_frame(&mut self.reader)?;
        if let DaemonUnixMuxFrame::Server(ServerFrame::Response { request_id, .. }) = &frame
            && let Some(id) = parse_request_id(request_id)
        {
            self.outstanding.retain(|outstanding| *outstanding != id);
        }
        Ok(frame)
    }

    /// Read the next frame of any kind. Parked frames are returned first.
    ///
    /// A `Response` frame removes its id from the outstanding set. Responses
    /// for ids this connection never submitted are still returned; callers
    /// discard them.
    pub fn next_frame(&mut self) -> DaemonTransportResult<DaemonUnixMuxFrame> {
        if let Some((id, response)) = self.parked_responses.pop() {
            return Ok(DaemonUnixMuxFrame::Server(ServerFrame::Response {
                request_id: encode_request_id(id),
                response,
            }));
        }
        if !self.skipped_events.is_empty() {
            return Ok(DaemonUnixMuxFrame::Server(ServerFrame::Event {
                event: self.skipped_events.remove(0),
            }));
        }
        if !self.skipped_entity_frames.is_empty() {
            return Ok(DaemonUnixMuxFrame::Server(ServerFrame::Entity {
                entity: self.skipped_entity_frames.remove(0),
            }));
        }
        if !self.skipped_terminal.is_empty() {
            return Ok(DaemonUnixMuxFrame::Terminal(
                self.skipped_terminal.remove(0),
            ));
        }
        let frame = self.read_next_frame()?;
        if let DaemonUnixMuxFrame::Server(ServerFrame::Close { reason }) = &frame {
            self.closed = Some(reason.clone());
        }
        Ok(frame)
    }

    /// Read the next frame, or `None` when `timeout` elapses first.
    ///
    /// Restores the previous socket read timeout. A timeout keeps any partial
    /// frame for the next read.
    pub fn poll_frame(
        &mut self,
        timeout: Duration,
    ) -> DaemonTransportResult<Option<DaemonUnixMuxFrame>> {
        let previous = self
            .stream
            .read_timeout()
            .map_err(normalize_socket_io_error)?;
        self.set_read_timeout(Some(timeout.max(Duration::from_millis(1))))?;
        let result = match self.next_frame() {
            Ok(frame) => Ok(Some(frame)),
            Err(DaemonTransportError::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        };
        let restore = self.set_read_timeout(previous);
        match result {
            Ok(value) => {
                restore?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Write one opaque terminal input frame for one route on this muxed connection.
    ///
    /// This is not a control request. Hub does not send a paired response;
    /// results arrive on the terminal stream. `stream_epoch` is reserved for
    /// the input direction and must be 0; Hub validates only the route and the
    /// fixed generation.
    pub fn send_terminal_frame(
        &mut self,
        route: &str,
        generation: u64,
        stream_epoch: u32,
        body: &[u8],
    ) -> DaemonTransportResult<()> {
        write_unix_terminal_frame(&mut self.stream, route, generation, stream_epoch, body)
    }

    /// Opaque terminal frames skipped while waiting for a host response.
    pub fn take_skipped_terminal(&mut self) -> Vec<DaemonUnixTerminalFrame> {
        std::mem::take(&mut self.skipped_terminal)
    }

    /// Host events skipped while waiting for a host response.
    pub fn take_skipped_events(&mut self) -> Vec<DaemonEvent> {
        std::mem::take(&mut self.skipped_events)
    }

    /// Entity frames skipped while waiting for a host response.
    pub fn take_skipped_entity_frames(&mut self) -> Vec<DaemonEntityFrame> {
        std::mem::take(&mut self.skipped_entity_frames)
    }

    /// Host Hello `required_features` from the connection handshake.
    #[must_use]
    pub fn required_features(&self) -> &[String] {
        &self.required_features
    }

    /// Subscribe to package events on this multiplexed host-control connection.
    ///
    /// Returns a typed compatibility error and sends no request when Hello did
    /// not require [`FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS`].
    pub fn subscribe_events(
        &mut self,
        subscription_id: impl Into<String>,
        owner: impl Into<String>,
        name: impl Into<String>,
        subjects: Vec<String>,
    ) -> DaemonTransportResult<DaemonResponse> {
        if !hello_requires_package_event_subscriptions(&self.required_features) {
            return Err(package_event_subscriptions_not_negotiated_error());
        }
        self.request(&DaemonRequest::SubscribeEvents {
            subscription_id: subscription_id.into(),
            owner: owner.into(),
            name: name.into(),
            subjects,
        })
    }

    /// Remove one package-event subscription owned by this connection.
    pub fn unsubscribe_events(
        &mut self,
        subscription_id: impl Into<String>,
    ) -> DaemonTransportResult<DaemonResponse> {
        if !hello_requires_package_event_subscriptions(&self.required_features) {
            return Err(package_event_subscriptions_not_negotiated_error());
        }
        self.request(&DaemonRequest::UnsubscribeEvents {
            subscription_id: subscription_id.into(),
        })
    }

    /// Bound how long a caller waits for the next unsolicited host event.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> DaemonTransportResult<()> {
        self.stream
            .set_read_timeout(timeout)
            .map_err(normalize_socket_io_error)
    }

    /// Receive the next unsolicited host event without sending a control request.
    ///
    /// Returns events already skipped while waiting for a response first. Does
    /// not write to the socket.
    pub fn next_event(&mut self) -> DaemonTransportResult<DaemonEvent> {
        if !self.skipped_events.is_empty() {
            return Ok(self.skipped_events.remove(0));
        }
        loop {
            match self.read_next_frame()? {
                DaemonUnixMuxFrame::Server(ServerFrame::Event { event }) => return Ok(event),
                DaemonUnixMuxFrame::Server(ServerFrame::Entity { entity: frame }) => {
                    self.skipped_entity_frames.push(frame);
                }
                DaemonUnixMuxFrame::Server(ServerFrame::Response {
                    request_id,
                    response,
                }) => self.park_response(&request_id, response)?,
                DaemonUnixMuxFrame::Server(ServerFrame::Close { reason }) => {
                    self.closed = Some(reason.clone());
                    return Err(DaemonTransportError::ClosedByHub(reason));
                }
                DaemonUnixMuxFrame::Server(ServerFrame::HelloAck { .. }) => {
                    return Err(DaemonTransportError::Protocol(
                        "unexpected hello ack after the handshake",
                    ));
                }
                DaemonUnixMuxFrame::Terminal(frame) => self.skipped_terminal.push(frame),
            }
        }
    }

    fn park_response(
        &mut self,
        request_id: &str,
        response: DaemonResponse,
    ) -> DaemonTransportResult<()> {
        let Some(id) = parse_request_id(request_id) else {
            return Err(DaemonTransportError::ProtocolViolation(
                DaemonProtocolErrorCode::InvalidRequestId,
            ));
        };
        if self.outstanding.contains(&id) || self.ids.last() >= id {
            self.parked_responses.push((id, response));
        }
        Ok(())
    }

    /// Receive the next unsolicited terminal frame without sending a control request.
    ///
    /// Returns frames already skipped while waiting for a response first. Does
    /// not write to the socket.
    pub fn next_terminal(&mut self) -> DaemonTransportResult<DaemonUnixTerminalFrame> {
        if !self.skipped_terminal.is_empty() {
            return Ok(self.skipped_terminal.remove(0));
        }
        loop {
            match self.read_next_frame()? {
                DaemonUnixMuxFrame::Terminal(frame) => return Ok(frame),
                DaemonUnixMuxFrame::Server(ServerFrame::Event { event }) => {
                    self.skipped_events.push(event);
                }
                DaemonUnixMuxFrame::Server(ServerFrame::Entity { entity: frame }) => {
                    self.skipped_entity_frames.push(frame);
                }
                DaemonUnixMuxFrame::Server(ServerFrame::Response {
                    request_id,
                    response,
                }) => self.park_response(&request_id, response)?,
                DaemonUnixMuxFrame::Server(ServerFrame::Close { reason }) => {
                    self.closed = Some(reason.clone());
                    return Err(DaemonTransportError::ClosedByHub(reason));
                }
                DaemonUnixMuxFrame::Server(ServerFrame::HelloAck { .. }) => {
                    return Err(DaemonTransportError::Protocol(
                        "unexpected hello ack after the handshake",
                    ));
                }
            }
        }
    }

    /// Receive the next unsolicited terminal frame, or `None` when `timeout` elapses.
    ///
    /// Does not write a control request. Restores the previous socket read timeout.
    /// `timeout` is an absolute deadline: skipped host events do not restart it.
    /// A timeout keeps any partial frame for the next read.
    pub fn poll_terminal(
        &mut self,
        timeout: Duration,
    ) -> DaemonTransportResult<Option<DaemonUnixTerminalFrame>> {
        if !self.skipped_terminal.is_empty() {
            return Ok(Some(self.skipped_terminal.remove(0)));
        }
        let deadline = Instant::now() + timeout;
        let previous = self
            .stream
            .read_timeout()
            .map_err(normalize_socket_io_error)?;
        let result = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break Ok(None);
            }
            if let Err(error) = self.set_read_timeout(Some(remaining)) {
                break Err(error);
            }
            match self.read_next_frame() {
                Ok(DaemonUnixMuxFrame::Terminal(frame)) => break Ok(Some(frame)),
                Ok(DaemonUnixMuxFrame::Server(ServerFrame::Event { event })) => {
                    self.skipped_events.push(event);
                }
                Ok(DaemonUnixMuxFrame::Server(ServerFrame::Entity { entity: frame })) => {
                    self.skipped_entity_frames.push(frame);
                }
                Ok(DaemonUnixMuxFrame::Server(ServerFrame::Response {
                    request_id,
                    response,
                })) => {
                    if let Err(error) = self.park_response(&request_id, response) {
                        break Err(error);
                    }
                }
                Ok(DaemonUnixMuxFrame::Server(ServerFrame::Close { reason })) => {
                    self.closed = Some(reason.clone());
                    break Err(DaemonTransportError::ClosedByHub(reason));
                }
                Ok(DaemonUnixMuxFrame::Server(ServerFrame::HelloAck { .. })) => {
                    break Err(DaemonTransportError::Protocol(
                        "unexpected hello ack after the handshake",
                    ));
                }
                Err(DaemonTransportError::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(error) => break Err(error),
            }
        };
        let restore = self.set_read_timeout(previous);
        match result {
            Ok(value) => {
                restore?;
                Ok(value)
            }
            Err(error) => {
                let _ = restore;
                Err(error)
            }
        }
    }
}

/// One held-open, connection-scoped session entity subscription.
pub struct DaemonEntitySubscription {
    connection: DaemonConnection,
    subscription_id: String,
}

impl DaemonEntitySubscription {
    /// Bound how long a caller waits for the next pushed frame.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> DaemonTransportResult<()> {
        self.connection.set_read_timeout(timeout)
    }

    /// Read the next authoritative snapshot or ordered entity delta.
    pub fn next_frame(&mut self) -> DaemonTransportResult<DaemonEntityFrame> {
        loop {
            match self.connection.next_frame()? {
                DaemonUnixMuxFrame::Server(ServerFrame::Entity { entity: frame }) => {
                    return Ok(frame);
                }
                DaemonUnixMuxFrame::Server(ServerFrame::Close { reason }) => {
                    return Err(DaemonTransportError::ClosedByHub(reason));
                }
                DaemonUnixMuxFrame::Server(_) | DaemonUnixMuxFrame::Terminal(_) => {}
            }
        }
    }

    /// Explicitly end this connection-owned subscription.
    pub fn unsubscribe(mut self) -> DaemonTransportResult<()> {
        let response = self
            .connection
            .request(&DaemonRequest::UnsubscribeEntities {
                subscription_id: self.subscription_id.clone(),
            })?;
        if response.kind != DaemonResponseKind::EntityUnsubscribed {
            return Err(DaemonTransportError::Protocol(
                "unexpected entity unsubscribe response",
            ));
        }
        Ok(())
    }
}

/// Open a fresh held-open subscription for the built-in session entity family.
pub fn subscribe_session_entities(
    endpoint: &DaemonEndpoint,
    subscription_id: impl Into<String>,
) -> DaemonTransportResult<DaemonEntitySubscription> {
    subscribe_entities(endpoint, "session", subscription_id)
}

/// Open a fresh held-open subscription for one admitted entity family.
pub fn subscribe_entities(
    endpoint: &DaemonEndpoint,
    entity_type: impl Into<String>,
    subscription_id: impl Into<String>,
) -> DaemonTransportResult<DaemonEntitySubscription> {
    let entity_type = entity_type.into();
    let subscription_id = subscription_id.into();
    let mut connection = DaemonConnection::connect(endpoint)?;
    let response = connection.request(&DaemonRequest::SubscribeEntities {
        entity_type,
        subscription_id: subscription_id.clone(),
    })?;
    if response.kind != DaemonResponseKind::EntitySubscribed {
        return Err(DaemonTransportError::Protocol(
            "entity subscription was not accepted",
        ));
    }
    Ok(DaemonEntitySubscription {
        connection,
        subscription_id,
    })
}

/// Attach and restore the current screen. Terminal bytes flow through the bound adapter.
pub fn stream_attach(
    endpoint: &DaemonEndpoint,
    session_id: &str,
    subscription_id: &str,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    let mut connection = DaemonConnection::connect(endpoint)?;
    let result = stream_attach_connected(&mut connection, session_id, subscription_id, output);
    detach_stream_subscription(&mut connection, session_id, subscription_id);
    result
}

fn stream_attach_connected(
    connection: &mut DaemonConnection,
    session_id: &str,
    subscription_id: &str,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    let response = connection.request(&DaemonRequest::Attach {
        session_id: session_id.to_string(),
        subscription_id: subscription_id.to_string(),
    })?;
    if response.kind == DaemonResponseKind::OperatorError {
        return Err(DaemonTransportError::Protocol(
            "attach failed before adapter bind",
        ));
    }
    let _ = write_read_screen(connection, session_id, output)?;
    Ok(())
}

fn detach_stream_subscription(
    connection: &mut DaemonConnection,
    session_id: &str,
    subscription_id: &str,
) {
    let _ = connection.request(&DaemonRequest::Detach {
        session_id: session_id.to_string(),
        subscription_id: subscription_id.to_string(),
    });
}

/// Connect to the daemon with the current first-party compatibility requirement.
pub fn connect_and_hello(endpoint: &DaemonEndpoint) -> DaemonTransportResult<UnixStream> {
    connect_and_hello_with_requirement(endpoint, &DaemonCompatibilityRequirement::current())
}

/// Connect to the daemon and validate the running hub against an explicit requirement.
///
/// ```no_run
/// let endpoint = botster_hub_client::DaemonEndpoint::new("/tmp/botster-hub.sock");
/// let mut requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
/// requirement.client_name = "example-client".to_string();
///
/// let _stream = botster_hub_client::connect_and_hello_with_requirement(
///     &endpoint,
///     &requirement,
/// )?;
/// # Ok::<(), botster_hub_client::DaemonTransportError>(())
/// ```
pub fn connect_and_hello_with_requirement(
    endpoint: &DaemonEndpoint,
    requirement: &DaemonCompatibilityRequirement,
) -> DaemonTransportResult<UnixStream> {
    let (stream, _ack) = connect_and_hello_with_terminal_requirement(endpoint, requirement, None)?;
    Ok(stream)
}

/// Connect with a host requirement and an optional Core terminal requirement.
///
/// A missing terminal requirement is not a mismatch. Status-only clients can
/// omit it. This helper does not force a terminal requirement into the default
/// host Hello path.
pub fn connect_and_hello_with_terminal_requirement(
    endpoint: &DaemonEndpoint,
    requirement: &DaemonCompatibilityRequirement,
    terminal_compatibility: Option<&TerminalCompatibilityRequirement>,
) -> DaemonTransportResult<(UnixStream, DaemonHelloAck)> {
    let mut stream = UnixStream::connect(&endpoint.socket_path).map_err(|error| {
        if matches!(
            error.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
        ) {
            DaemonTransportError::NotRunning
        } else {
            normalize_socket_io_error(error)
        }
    })?;
    write_client_frame(
        &mut stream,
        &ClientFrame::Hello {
            hello: DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: requirement.clone(),
                terminal_compatibility: terminal_compatibility.cloned(),
            },
        },
    )?;
    let ack = read_hello_ack(&mut stream)?;
    if ack.protocol != PROTOCOL {
        return Err(DaemonTransportError::Protocol(
            "unexpected hello ack protocol",
        ));
    }
    ensure_compatible(requirement, &ack.compatibility)
        .map_err(DaemonTransportError::Compatibility)?;
    Ok((stream, ack))
}

/// Read the Hello ack directly from the stream, without buffering past it.
fn read_hello_ack(stream: &mut UnixStream) -> DaemonTransportResult<DaemonHelloAck> {
    let mut frames = DaemonUnixFrameReader::new();
    match frames.read_frame(stream)? {
        DaemonUnixMuxFrame::Server(ServerFrame::HelloAck { ack }) => Ok(ack),
        DaemonUnixMuxFrame::Server(ServerFrame::Close { reason }) => {
            Err(DaemonTransportError::ClosedByHub(reason))
        }
        DaemonUnixMuxFrame::Server(_) | DaemonUnixMuxFrame::Terminal(_) => Err(
            DaemonTransportError::Protocol("expected a hello ack as the first server frame"),
        ),
    }
}

fn normalize_socket_io_error(error: std::io::Error) -> DaemonTransportError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
    ) {
        DaemonTransportError::ClientDisconnected
    } else {
        DaemonTransportError::Io(error)
    }
}

fn write_read_screen(
    connection: &mut DaemonConnection,
    session_id: &str,
    output: &mut impl Write,
) -> DaemonTransportResult<bool> {
    let response = connection.request(&DaemonRequest::ReadScreen {
        session_id: session_id.to_string(),
    })?;
    let Some(screen) = response.read_screen else {
        return Ok(false);
    };
    if screen.unavailable.is_some() || screen.text.trim().is_empty() {
        return Ok(false);
    }
    output
        .write_all(screen.text.as_bytes())
        .map_err(DaemonTransportError::Io)?;
    if !screen.text.ends_with('\n') {
        output.write_all(b"\n").map_err(DaemonTransportError::Io)?;
    }
    output.flush().map_err(DaemonTransportError::Io)?;
    Ok(true)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonHello {
    pub protocol: String,
    /// Reserved for future client-admission policy.
    ///
    /// Current hubs deserialize this field but intentionally ignore it; clients
    /// validate hub compatibility from `DaemonHelloAck` and `DaemonStatus`.
    #[serde(default)]
    pub compatibility: DaemonCompatibilityRequirement,
    /// Optional Core terminal-plane requirement. Absence is not a mismatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_compatibility: Option<TerminalCompatibilityRequirement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonHelloAck {
    pub protocol: String,
    pub compatibility: DaemonCompatibility,
    /// Independent Core terminal compatibility. Host `compatibility` stays separate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_compatibility: Option<TerminalCompatibility>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonCompatibility {
    pub protocol: String,
    pub protocol_version: u16,
    pub features: Vec<String>,
    pub conformance_fixture_revision: u16,
}

impl DaemonCompatibility {
    #[must_use]
    pub fn current() -> Self {
        Self {
            protocol: PROTOCOL.to_string(),
            protocol_version: PROTOCOL_VERSION,
            features: current_feature_list()
                .into_iter()
                .map(str::to_string)
                .collect(),
            conformance_fixture_revision: CONFORMANCE_FIXTURE_REVISION,
        }
    }

    #[must_use]
    pub fn supports_feature(&self, feature: &str) -> bool {
        self.features.iter().any(|supported| supported == feature)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonCompatibilityRequirement {
    pub protocol: String,
    pub protocol_version: u16,
    pub required_features: Vec<String>,
    pub minimum_conformance_fixture_revision: u16,
    pub client_name: String,
}

impl DaemonCompatibilityRequirement {
    /// Build the default first-party daemon compatibility requirement.
    ///
    /// The default requirement contains capabilities that all first-party clients need.
    /// A client must use a request-specific requirement for an optional capability.
    ///
    /// ```
    /// let mut requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    /// requirement.client_name = "botster-tui".to_string();
    ///
    /// assert_eq!(requirement.protocol, botster_hub_client::PROTOCOL);
    /// assert!(!requirement
    ///     .required_features
    ///     .contains(&botster_terminal_protocol::FEATURE_TERMINAL_STREAMING.to_string()));
    /// ```
    #[must_use]
    pub fn current() -> Self {
        Self {
            protocol: PROTOCOL.to_string(),
            protocol_version: PROTOCOL_VERSION,
            required_features: default_required_feature_list()
                .into_iter()
                .map(str::to_string)
                .collect(),
            minimum_conformance_fixture_revision: DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION,
            client_name: "botster-hub-client".to_string(),
        }
    }

    /// Build the requirement for `StartHubUpdate` and `GetHubUpdateExecution`.
    #[must_use]
    pub fn for_hub_source_update() -> Self {
        let mut requirement = Self::current();
        requirement
            .required_features
            .push(FEATURE_HUB_SOURCE_UPDATE.to_string());
        requirement.minimum_conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        requirement
    }

    /// Build the host requirement used with a terminal-plane ready-then-history Hello.
    #[must_use]
    pub fn for_ready_then_history_attach() -> Self {
        let mut requirement = Self::current();
        requirement.minimum_conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        requirement
    }

    /// Build the requirement for the optional Unix terminal adapter plane.
    #[must_use]
    pub fn for_unix_terminal_adapter() -> Self {
        let mut requirement = Self::current();
        requirement
            .required_features
            .push(FEATURE_UNIX_TERMINAL_ADAPTER.to_string());
        requirement.minimum_conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        requirement
    }

    /// Build the requirement for the optional WebRTC terminal adapter plane.
    #[must_use]
    pub fn for_webrtc_terminal_adapter() -> Self {
        let mut requirement = Self::current();
        requirement
            .required_features
            .push(FEATURE_WEBRTC_TERMINAL_ADAPTER.to_string());
        requirement.minimum_conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        requirement
    }

    /// Build the requirement for WebRTC adapter close events on protocol 7.
    #[must_use]
    pub fn for_webrtc_terminal_subscription_closed() -> Self {
        let mut requirement = Self::for_webrtc_terminal_adapter();
        requirement
            .required_features
            .push(FEATURE_TERMINAL_SUBSCRIPTION_CLOSED.to_string());
        requirement
    }

    /// Build the requirement for the public attach occupancy Status field.
    #[must_use]
    pub fn for_attach_occupancy() -> Self {
        let mut requirement = Self::current();
        requirement
            .required_features
            .push(FEATURE_ATTACH_OCCUPANCY.to_string());
        requirement.minimum_conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        requirement
    }

    /// Build the requirement for host-control package-event subscriptions.
    #[must_use]
    pub fn for_package_event_subscriptions() -> Self {
        let mut requirement = Self::current();
        requirement
            .required_features
            .push(FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.to_string());
        requirement.minimum_conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        requirement
    }
}

impl Default for DaemonCompatibilityRequirement {
    fn default() -> Self {
        Self::current()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonCompatibilityError {
    pub diagnostic: String,
    pub diagnostics: Vec<DaemonDiagnostic>,
}

impl fmt::Display for DaemonCompatibilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.diagnostic)
    }
}

impl Error for DaemonCompatibilityError {}

pub fn ensure_compatible(
    requirement: &DaemonCompatibilityRequirement,
    compatibility: &DaemonCompatibility,
) -> Result<(), DaemonCompatibilityError> {
    if compatibility.protocol != requirement.protocol {
        return Err(compatibility_error(
            requirement,
            compatibility,
            format!(
                "unsupported protocol {}; expected {}",
                compatibility.protocol, requirement.protocol
            ),
        ));
    }

    if compatibility.protocol_version != requirement.protocol_version {
        return Err(compatibility_error(
            requirement,
            compatibility,
            format!(
                "unsupported protocol version {}; client requires {}",
                compatibility.protocol_version, requirement.protocol_version
            ),
        ));
    }

    if compatibility.conformance_fixture_revision < requirement.minimum_conformance_fixture_revision
    {
        return Err(compatibility_error(
            requirement,
            compatibility,
            format!(
                "unsupported conformance fixture revision {}; requires at least {}",
                compatibility.conformance_fixture_revision,
                requirement.minimum_conformance_fixture_revision
            ),
        ));
    }

    let missing: Vec<&str> = requirement
        .required_features
        .iter()
        .map(String::as_str)
        .filter(|feature| !compatibility.supports_feature(feature))
        .collect();
    if !missing.is_empty() {
        return Err(compatibility_error(
            requirement,
            compatibility,
            format!("missing required feature(s): {}", missing.join(", ")),
        ));
    }

    Ok(())
}

fn compatibility_error(
    requirement: &DaemonCompatibilityRequirement,
    compatibility: &DaemonCompatibility,
    reason: String,
) -> DaemonCompatibilityError {
    DaemonCompatibilityError {
        diagnostic: format!(
            "{} is incompatible with running botster-hub: {}; required protocol={} min_version={} required_features=[{}] min_conformance_fixture_revision={}; running protocol={} version={} features=[{}] conformance_fixture_revision={}",
            requirement.client_name,
            reason,
            requirement.protocol,
            requirement.protocol_version,
            requirement.required_features.join(","),
            requirement.minimum_conformance_fixture_revision,
            compatibility.protocol,
            compatibility.protocol_version,
            compatibility.features.join(","),
            compatibility.conformance_fixture_revision
        ),
        diagnostics: vec![compatibility_diagnostic(&reason)],
    }
}

fn compatibility_diagnostic(reason: &str) -> DaemonDiagnostic {
    reason
        .strip_prefix("missing required feature(s): ")
        .and_then(|features| features.split(',').next())
        .map(str::trim)
        .filter(|feature| !feature.is_empty())
        .map(DaemonDiagnostic::unsupported_feature)
        .unwrap_or_else(|| DaemonDiagnostic::compatibility_mismatch(reason))
}

fn current_feature_list() -> Vec<&'static str> {
    let mut features = default_required_feature_list();
    features.push(FEATURE_HUB_SOURCE_UPDATE);
    features.push(FEATURE_UNIX_TERMINAL_ADAPTER);
    features.push(FEATURE_TERMINAL_SUBSCRIPTION_CLOSED);
    features.push(FEATURE_WEBRTC_TERMINAL_ADAPTER);
    features.push(FEATURE_ATTACH_OCCUPANCY);
    features.push(FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS);
    features
}

/// True when host Hello `required_features` asked for package-event subscriptions.
#[must_use]
pub fn hello_requires_package_event_subscriptions<S: AsRef<str>>(
    required_features: impl IntoIterator<Item = S>,
) -> bool {
    required_features
        .into_iter()
        .any(|feature| feature.as_ref() == FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS)
}

fn package_event_subscriptions_not_negotiated_error() -> DaemonTransportError {
    DaemonTransportError::Compatibility(DaemonCompatibilityError {
        diagnostic: "package_event_subscriptions was not negotiated on this host Hello".to_string(),
        diagnostics: vec![DaemonDiagnostic::unsupported_feature(
            FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS,
        )],
    })
}

/// Open a multiplexed host-control connection that can subscribe to package events.
pub fn connect_for_package_event_subscriptions(
    endpoint: &DaemonEndpoint,
) -> DaemonTransportResult<DaemonConnection> {
    DaemonConnection::connect_with_requirement(
        endpoint,
        &DaemonCompatibilityRequirement::for_package_event_subscriptions(),
    )
}

fn default_required_feature_list() -> Vec<&'static str> {
    vec![
        FEATURE_SESSIONS,
        FEATURE_PLUGIN_SURFACE_RENDER,
        FEATURE_PLUGIN_SURFACE_ACTION,
        FEATURE_PACKAGE_ROUTES,
        FEATURE_PACKAGE_NAVIGATION,
        FEATURE_SPAWN_TARGETS,
        FEATURE_WORKTREES,
        FEATURE_TERMINAL_READBACK,
        FEATURE_SESSION_ENTITY_SUBSCRIPTIONS,
        FEATURE_SESSION_TYPE_ENTITY_SUBSCRIPTIONS,
        FEATURE_PLUGIN_ENTITY_SUBSCRIPTIONS,
    ]
}

/// Client request variants for the local daemon protocol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonRequest {
    Status,
    CheckHubUpdate,
    StartHubUpdate {
        scope: DaemonHubUpdateScope,
    },
    GetHubUpdateExecution,
    ListSessions,
    SubscribeEntities {
        entity_type: String,
        subscription_id: String,
    },
    UnsubscribeEntities {
        subscription_id: String,
    },
    SubscribeEvents {
        subscription_id: String,
        owner: String,
        name: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        subjects: Vec<String>,
    },
    UnsubscribeEvents {
        subscription_id: String,
    },
    RemoveSession {
        session_id: String,
    },
    Whoami {
        caller_session_id: Option<String>,
    },
    PostMessage {
        caller_session_id: Option<String>,
        target_session_id: String,
        envelope_id: Option<String>,
        body: String,
    },
    ReceiveMessages {
        caller_session_id: String,
        after: Option<u64>,
        limit: usize,
    },
    AckMessage {
        caller_session_id: String,
        envelope_id: String,
    },
    NotifySession {
        session_id: String,
        data: String,
    },
    Spawn {
        session_id: String,
        command: String,
    },
    Attach {
        session_id: String,
        subscription_id: String,
    },
    Detach {
        session_id: String,
        subscription_id: String,
    },
    ShutdownSession {
        session_id: String,
    },
    ReadScreen {
        session_id: String,
    },
    ReadModeFlags {
        session_id: String,
    },
    /// Open a paged GHOSTSNP capture. The response describes pages; no bytes travel here.
    CaptureSnapshot {
        session_id: String,
    },
    /// Read one page of an open capture. At most [`SNAPSHOT_PAGE_BYTES`] per response.
    ReadSnapshotPage {
        session_id: String,
        capture_id: String,
        page: u32,
    },
    ListSessionTypes,
    ListSessionTypesForTarget {
        target_id: String,
    },
    ShowSessionType {
        session_type_id: String,
    },
    ShowSessionTypeDefinition {
        session_type_id: String,
    },
    CreateSessionType {
        source: DaemonSessionTypeMutationSource,
        definition: DaemonSessionTypeDefinition,
    },
    UpdateSessionType {
        source: DaemonSessionTypeMutationSource,
        definition: DaemonSessionTypeDefinition,
    },
    DeleteSessionType {
        source: DaemonSessionTypeMutationSource,
        session_type_id: String,
    },
    ResolveSessionType {
        session_type_id: String,
        #[serde(default)]
        request: DaemonSessionTypeRequest,
    },
    SpawnSessionType {
        session_type_id: String,
        session_id: String,
        #[serde(default)]
        request: DaemonSessionTypeRequest,
    },
    ReadSessionContext {
        session_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<String>,
    },
    ListSpawnTargets,
    ShowSpawnTarget {
        target_id: String,
    },
    CreateSpawnTarget {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        root: PathBuf,
        #[serde(default = "default_true")]
        enabled: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kind: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_ref: Option<String>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, String>,
    },
    UpdateSpawnTarget {
        target_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        root: Option<PathBuf>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        enabled: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kind: Option<String>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_present_nullable"
        )]
        base_ref: Option<Option<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<BTreeMap<String, String>>,
    },
    DeleteSpawnTarget {
        target_id: String,
    },
    ValidateSpawnTarget {
        target_id: String,
    },
    ListWorktrees,
    ShowWorktree {
        worktree_id: String,
    },
    CreateWorktree {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        worktree_id: Option<String>,
        target_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        path: PathBuf,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, String>,
    },
    DeleteWorktree {
        worktree_id: String,
    },
    ListApps,
    ResolveAppLaunch {
        package_name: String,
        entrypoint_id: String,
    },
    ResolvePackageRoute {
        package_name: String,
        route_id: String,
    },
    ListPackageNavigation,
    ListPackages,
    ListAvailablePackages {
        registry_path: PathBuf,
    },
    InspectAvailablePackage {
        registry_path: PathBuf,
        entry_id: String,
    },
    PreviewPackageInstall {
        registry_path: PathBuf,
        entry_id: String,
    },
    InstallPackageRegistryEntry {
        registry_path: PathBuf,
        entry_id: String,
    },
    InstallPackageLocalPath {
        path: PathBuf,
    },
    CheckPackageUpdate {
        package_name: String,
    },
    PreviewPackageUpdate {
        package_name: String,
        pin: DaemonPackagePin,
    },
    ApplyPackageUpdate {
        package_name: String,
        pin: DaemonPackagePin,
    },
    ShowPackage {
        package_name: String,
    },
    SetPackageConfiguration {
        package_name: String,
        values: BTreeMap<String, Value>,
    },
    ReloadPackage {
        package_name: String,
    },
    RefreshLocalPackages,
    EnablePackageLocalPath {
        path: PathBuf,
    },
    EnablePackage {
        package_name: String,
    },
    DisablePackage {
        package_name: String,
    },
    RemovePackage {
        package_name: String,
    },
    StartPackageEntrypoint {
        package_name: String,
        entrypoint_id: String,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        environment_overrides: BTreeMap<String, String>,
    },
    IssueLocalWebrtcBootstrap {
        package_name: String,
        entrypoint_id: String,
        origin: String,
    },
    LocalWebrtcSignal {
        grant_id: String,
        grant_secret: String,
        origin: String,
        offer: Value,
    },
    StopPackageEntrypoint {
        package_name: String,
        entrypoint_id: String,
    },
    RestartPackageEntrypoint {
        package_name: String,
        entrypoint_id: String,
    },
    PackageEntrypointStatus {
        package_name: String,
        entrypoint_id: String,
    },
    PluginLifecycleStatus,
    PluginMcpListTools,
    PluginMcpCallTool {
        name: String,
        arguments: Value,
    },
    PluginSurfaceRender {
        package_name: String,
        surface_id: String,
        payload: Value,
    },
    PluginSurfaceAction {
        package_name: String,
        request: UiActionRequest,
    },
    DaemonShutdown,
}

/// Server response variants for one local daemon request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonResponse {
    pub kind: DaemonResponseKind,
    pub status: Option<DaemonStatus>,
    pub sessions: Vec<DaemonSession>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub session_types: Vec<DaemonSessionType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_type_definition: Option<DaemonSessionTypeEditableDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_session_type: Option<DaemonResolvedSessionType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_context: Option<DaemonSessionContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_screen: Option<DaemonReadScreen>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode_flags: Option<DaemonModeFlags>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_attach: Option<DaemonTerminalAttach>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reservation: Option<DaemonTerminalReservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_reservation: Option<DaemonSubscriptionReservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_snapshot: Option<DaemonCaptureSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_page: Option<DaemonSnapshotPage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub spawn_targets: Vec<DaemonSpawnTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawn_target_validation: Option<DaemonSpawnTargetValidation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worktrees: Vec<DaemonWorktree>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub apps: Vec<DaemonApp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_app_launch: Option<DaemonResolvedAppLaunch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_package_route: Option<DaemonPackageRouteDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub package_navigation: Vec<DaemonPackageNavigationEntry>,
    pub packages: Vec<DaemonPackage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub available_packages: Vec<DaemonAvailablePackage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub install_plan: Option<DaemonPackageInstallPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_status: Option<DaemonPackageUpdateStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hub_update: Option<DaemonHubUpdate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hub_update_execution: Option<DaemonHubUpdateExecution>,
    pub package_decision: Option<DaemonPackageDecision>,
    pub lifecycle: Vec<DaemonPluginLifecycle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_worker_counters: Option<DaemonPluginWorkerCounters>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_resource_counters: Option<DaemonPluginResourceCounters>,
    #[serde(default)]
    pub plugin_tools: Vec<Value>,
    #[serde(default)]
    pub plugin_tool_result: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_surface: Option<DaemonPluginSurface>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_action_result: Option<UiActionResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_webrtc_bootstrap: Option<DaemonLocalWebrtcBootstrap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_webrtc_answer: Option<DaemonLocalWebrtcAnswer>,
    pub events: Vec<DaemonEvent>,
    pub cleanup: Option<DaemonSessionCleanup>,
    pub coordination: Option<DaemonCoordination>,
    pub error: Option<DaemonOperatorError>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonPluginSurface {
    pub package_name: String,
    pub surface_id: String,
    pub body: UiNode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ui_tree_snapshot: Option<DaemonUiTreeSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonUiTreeSnapshot {
    pub package_name: String,
    pub surface_id: String,
    pub body: UiNode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonResponseKind {
    Status,
    HubUpdate,
    HubUpdateExecution,
    Sessions,
    EntitySubscribed,
    EntityUnsubscribed,
    EventSubscribed,
    EventUnsubscribed,
    SessionRemoved,
    Spawned,
    Events,
    SessionTypes,
    SessionTypeDefinition,
    ResolvedSessionType,
    SessionContext,
    ReadScreen,
    ReadModeFlags,
    TerminalAttached,
    TerminalReservation,
    CaptureSnapshot,
    SnapshotPage,
    SpawnTargets,
    SpawnTargetValidation,
    Worktrees,
    Apps,
    ResolvedAppLaunch,
    ResolvedPackageRoute,
    PackageNavigation,
    Packages,
    AvailablePackages,
    PackageInstallPlan,
    PackageUpdateStatus,
    PackageDecision,
    PluginLifecycle,
    PluginMcpTools,
    PluginMcpToolResult,
    PluginSurface,
    PluginActionResult,
    LocalWebrtcBootstrap,
    LocalWebrtcAnswer,
    SessionCleanup,
    Identity,
    MessagePosted,
    Messages,
    MessageAcked,
    SessionNotified,
    OperatorError,
    Shutdown,
}

/// Why a readback has no terminal history to return.
///
/// The session's registry exit record and exit code stay readable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryUnavailableReason {
    /// Retained ended history was evicted by the retention policy.
    Evicted,
    /// Hub restarted after the session ended; retained history is Core RAM only.
    Restart,
    /// The final state exceeded the per-object retention cap and was not stored.
    Oversize,
    /// The worker snapshot export or capture failed.
    CaptureFailed,
}

impl HistoryUnavailableReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Evicted => "evicted",
            Self::Restart => "restart",
            Self::Oversize => "oversize",
            Self::CaptureFailed => "capture_failed",
        }
    }
}

/// Backend-neutral restored screen text. Fits one response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonReadScreen {
    pub session_id: String,
    /// Empty when `unavailable` is set.
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<HistoryUnavailableReason>,
}

/// Authoritative terminal mode flags and geometry for one session.
///
/// Live mode changes travel on the terminal stream (Core `MODES`); this is the
/// host readback. Marked non-exhaustive so additive mode fields remain
/// source-compatible for external Rust consumers that construct this DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonModeFlags {
    pub session_id: String,
    pub kitty_enabled: bool,
    pub cursor_visible: bool,
    pub bracketed_paste: bool,
    pub mouse_mode: u8,
    pub alt_screen: bool,
    pub focus_reporting: bool,
    pub application_cursor: bool,
    pub rows: u16,
    pub cols: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<HistoryUnavailableReason>,
}

impl DaemonModeFlags {
    /// Build a full mode-flags response body.
    ///
    /// One field per public DTO member so constructors stay aligned with the
    /// non-exhaustive wire shape without a separate builder layer.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: impl Into<String>,
        kitty_enabled: bool,
        cursor_visible: bool,
        bracketed_paste: bool,
        mouse_mode: u8,
        alt_screen: bool,
        focus_reporting: bool,
        application_cursor: bool,
        rows: u16,
        cols: u16,
        unavailable: Option<HistoryUnavailableReason>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            kitty_enabled,
            cursor_visible,
            bracketed_paste,
            mouse_mode,
            alt_screen,
            focus_reporting,
            application_cursor,
            rows,
            cols,
            unavailable,
        }
    }
}

/// One admitted Unix terminal route, returned by `Attach` on a muxed Unix connection.
///
/// The client sends `generation` in every terminal input container for this
/// route and adopts a higher generation only from `ATTACH_STATE` or
/// `ROUTE_RESYNC` frames on the stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonTerminalAttach {
    pub session_id: String,
    pub subscription_id: String,
    /// Core-minted route generation at attach.
    pub generation: u64,
}

impl DaemonTerminalAttach {
    #[must_use]
    pub fn new(
        session_id: impl Into<String>,
        subscription_id: impl Into<String>,
        generation: u64,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            subscription_id: subscription_id.into(),
            generation,
        }
    }
}

/// Hub-reserved subscription DataChannel label for one admitted terminal route.
///
/// Non-exhaustive so later additive fields stay source-compatible for external
/// Rust consumers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonTerminalReservation {
    pub session_id: String,
    pub subscription_id: String,
    /// Core-minted terminal subscription generation for this route.
    pub generation: u64,
    /// Hub peer generation that owns the reservation.
    pub peer_generation: u64,
    /// Exact DataChannel label the peer must create. Opaque to the peer.
    pub label: String,
    /// Whole seconds the peer has to open the labeled channel.
    pub expires_in_seconds: u32,
}

/// Hub-reserved DataChannel label for one entity or package-event subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonSubscriptionReservation {
    pub kind: DaemonSubscriptionReservationKind,
    pub subscription_id: String,
    pub generation: u64,
    pub peer_generation: u64,
    pub label: String,
    pub expires_in_seconds: u32,
}

impl DaemonSubscriptionReservation {
    #[must_use]
    pub fn new(
        kind: DaemonSubscriptionReservationKind,
        subscription_id: impl Into<String>,
        generation: u64,
        peer_generation: u64,
        label: impl Into<String>,
        expires_in_seconds: u32,
    ) -> Self {
        Self {
            kind,
            subscription_id: subscription_id.into(),
            generation,
            peer_generation,
            label: label.into(),
            expires_in_seconds,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonSubscriptionReservationKind {
    Entity,
    PackageEvent,
}

impl DaemonTerminalReservation {
    /// Build a reservation body.
    #[must_use]
    pub fn new(
        session_id: impl Into<String>,
        subscription_id: impl Into<String>,
        generation: u64,
        peer_generation: u64,
        label: impl Into<String>,
        expires_in_seconds: u32,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            subscription_id: subscription_id.into(),
            generation,
            peer_generation,
            label: label.into(),
            expires_in_seconds,
        }
    }
}

/// One open paged snapshot capture.
///
/// A capture lives [`SNAPSHOT_CAPTURE_TTL_SECONDS`] or until the connection
/// closes; at most [`MAX_OPEN_CAPTURES_PER_CONNECTION`] are open per connection.
/// When `unavailable` is set, `capture_id` is empty and `pages` is zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonCaptureSnapshot {
    pub session_id: String,
    pub capture_id: String,
    pub total_bytes: u64,
    pub page_bytes: u32,
    pub pages: u32,
    pub rows: u16,
    pub cols: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<HistoryUnavailableReason>,
}

/// One page of an open capture. Opaque GHOSTSNP bytes; must not be rendered as text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSnapshotPage {
    pub session_id: String,
    pub capture_id: String,
    pub page: u32,
    #[serde(flatten)]
    pub payload: DaemonOpaqueHistoryPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonCoordination {
    pub identity: Option<DaemonIdentity>,
    pub publish: Option<DaemonEnvelopePublish>,
    pub messages: Vec<DaemonEnvelope>,
    pub next_cursor: Option<u64>,
    pub ack: Option<DaemonEnvelopeAck>,
    pub notify: Option<DaemonNotify>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonIdentity {
    pub client_id: String,
    pub role: String,
    pub identity_source: String,
    pub caller_session_id: Option<String>,
    pub host_id: String,
    pub host_display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonEnvelopePublish {
    pub deliveries: Vec<DaemonEnvelopeDelivery>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonEnvelopeDelivery {
    pub envelope_id: String,
    pub target: String,
    pub cursor: u64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonEnvelope {
    pub envelope_id: String,
    pub source: String,
    pub content_type: String,
    pub body: String,
    pub created_at: u64,
    pub cursor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonEnvelopeAck {
    pub envelope_id: Option<String>,
    pub target: Option<String>,
    pub cursor: Option<u64>,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonNotify {
    pub decision: String,
    pub state_count: usize,
    pub states: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionTypeRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub context: DaemonSessionTypeContextInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum DaemonSessionTypeMutationSource {
    Device,
    Repo { target_id: String },
    Package { package_name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionTypeDefinition {
    pub id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    pub role: String,
    pub interaction: String,
    #[serde(default)]
    pub traits: Vec<String>,
    pub lifecycle: String,
    #[serde(default)]
    pub execution: DaemonSessionTypeExecution,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub working_directory: DaemonSessionTypeWorkingDirectory,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub allowed_environment_overrides: Vec<String>,
    #[serde(default)]
    pub context: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum DaemonSessionTypeExecution {
    #[default]
    RelativeExecutable,
    ShellCommand,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum DaemonSessionTypeWorkingDirectory {
    #[default]
    PackageRoot,
    Relative {
        path: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionTypeContextInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionType {
    pub session_type_id: String,
    pub source_name: String,
    pub id: String,
    pub source: String,
    pub editable: bool,
    #[serde(default)]
    pub overridden_sources: Vec<DaemonSessionTypeSource>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    pub role: String,
    pub interaction: String,
    #[serde(default)]
    pub traits: Vec<String>,
    pub lifecycle: String,
    #[serde(default)]
    pub execution: DaemonSessionTypeExecution,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub working_directory_policy: String,
    #[serde(default)]
    pub allowed_environment_overrides: Vec<String>,
    #[serde(default)]
    pub context_keys: Vec<String>,
    pub target_id: String,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionTypeSource {
    pub kind: String,
    pub name: String,
}

/// Authored definition for one editable session type, plus the source that owns it.
///
/// `definition` is exactly the payload `UpdateSessionType` accepts, and `source`
/// is exactly the mutation source it requires, so a client can read this row,
/// change one field, and submit it back without losing the authored
/// working-directory path or environment that `DaemonSessionType` omits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionTypeEditableDefinition {
    pub session_type_id: String,
    pub source: DaemonSessionTypeMutationSource,
    pub definition: DaemonSessionTypeDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonResolvedSessionType {
    pub session_type: DaemonSessionType,
    pub session_id: String,
    pub executable: String,
    #[serde(default)]
    pub arguments: Vec<String>,
    pub working_directory: String,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    pub context_id: String,
    #[serde(default)]
    pub context_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionContext {
    pub context_id: String,
    pub session_id: String,
    #[serde(default)]
    pub values: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSpawnTarget {
    pub target_id: String,
    pub label: String,
    pub root: PathBuf,
    pub enabled: bool,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSpawnTargetValidation {
    pub target_id: String,
    pub ok: bool,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonWorktree {
    pub worktree_id: String,
    pub target_id: String,
    pub label: String,
    pub path: PathBuf,
    pub status: String,
    #[serde(default = "default_registered_management")]
    pub management: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<DaemonWorktreeGitMetadata>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonWorktreeGitMetadata {
    pub repository_root: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonWorktreeLifecycleEvent {
    pub event: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

const fn default_true() -> bool {
    true
}

fn default_registered_management() -> String {
    "registered".to_string()
}

fn deserialize_present_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackage {
    pub package_name: String,
    pub version: String,
    pub classification: String,
    #[serde(default = "default_daemon_package_source_kind")]
    pub source_kind: String,
    pub state: String,
    pub requested_capabilities: Vec<DaemonCapability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub surfaces: Vec<PackageSurfaceDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notice_reactions: Vec<PackageNoticeReactionDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<DaemonPackageRouteDescriptor>,
    #[serde(default)]
    pub runnable_entrypoints: Vec<DaemonPackageRunnableEntrypoint>,
    #[serde(default)]
    pub configuration: DaemonPackageConfiguration,
    #[serde(default)]
    pub availability: DaemonPackageAvailability,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependency_availability: Vec<DaemonPackageDependencyAvailability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_availability: Vec<DaemonPackageFeatureAvailability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<DaemonPackageActionState>,
    pub provider_profile_admitted: bool,
}

fn default_daemon_package_source_kind() -> String {
    "unknown".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonApp {
    pub package_name: String,
    pub app_id: String,
    pub entrypoint_id: String,
    pub kind: String,
    pub launch_mode: String,
    pub lifecycle_state: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonPackageDiagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<DaemonPackageActionState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_reasons: Vec<String>,
    pub launch_target: DaemonAppLaunchTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<DaemonPackageRouteDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonAppLaunchTarget {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonResolvedAppLaunch {
    pub package_name: String,
    pub app_id: String,
    pub entrypoint_id: String,
    pub kind: String,
    pub launch_mode: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    pub working_directory: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageRouteDescriptor {
    pub package_name: String,
    pub route_id: String,
    pub route_path: String,
    pub target: DaemonPackageRouteTarget,
    pub title: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    pub layout_mode: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<DaemonCapability>,
    pub enabled: bool,
    pub blocked: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonPackageDiagnostic>,
    pub supports_settings: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageRouteTarget {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageNavigationEntry {
    pub package_name: String,
    pub item_id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub route_id: String,
    pub route_path: String,
    pub target: DaemonPackageRouteTarget,
    pub source: DaemonPackageNavigationSource,
    pub enabled: bool,
    pub blocked: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonPackageDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageNavigationSource {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonLocalWebrtcBootstrap {
    pub grant_id: String,
    pub grant_secret: String,
    pub package_name: String,
    pub entrypoint_id: String,
    pub expected_origin: String,
    pub expires_at: u64,
    pub signaling_transport: String,
    pub data_plane: String,
    pub ordered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retransmits: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_packet_lifetime_ms: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonLocalWebrtcAnswer {
    pub grant_id: String,
    pub answer: Value,
    #[serde(default)]
    pub diagnostics: Vec<DaemonDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageAvailability {
    pub state: DaemonPackageAvailabilityState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<DaemonPackageAvailabilityReason>,
}

impl Default for DaemonPackageAvailability {
    fn default() -> Self {
        Self {
            state: DaemonPackageAvailabilityState::Available,
            reasons: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonPackageAvailabilityState {
    Available,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageAvailabilityReason {
    pub reason: String,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<DaemonCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirement: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageDependencyAvailability {
    pub id: String,
    pub package_name: String,
    pub state: DaemonPackageAvailabilityState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<DaemonPackageAvailabilityReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageFeatureAvailability {
    pub id: String,
    pub state: DaemonPackageAvailabilityState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<DaemonPackageAvailabilityReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonAvailablePackage {
    pub entry_id: String,
    pub package_name: String,
    pub version: String,
    pub classification: String,
    pub source_kind: String,
    pub source_label: String,
    pub first_party: bool,
    pub state: String,
    pub requested_capabilities: Vec<DaemonCapability>,
    pub compatibility: DaemonPackageCompatibility,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<DaemonPackagePin>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<DaemonPackageActionState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageActionState {
    pub action_id: String,
    pub status: DaemonPackageActionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonPackageDiagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_references: Vec<DaemonPackageActionRequiredReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<DaemonPackageActionRequest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonPackageActionStatus {
    Available,
    Blocked,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageActionRequiredReference {
    pub kind: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageActionRequest {
    pub request_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<DaemonPackagePin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageInstallPlan {
    pub entry: DaemonAvailablePackage,
    pub effects: Vec<DaemonPackageInstallEffect>,
    pub diagnostics: Vec<DaemonPackageDiagnostic>,
    pub mutates_registry: bool,
    pub starts_entrypoints: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageInstallEffect {
    pub kind: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageUpdateStatus {
    pub package_name: String,
    pub update_available: bool,
    pub reload_required: bool,
    pub restart_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<DaemonPackagePin>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonPackageDiagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<DaemonPackageActionState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageCompatibility {
    pub botster_requirement: String,
    pub result: String,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackagePin {
    pub revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rev: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    pub update_policy: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub effective_values: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_required: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonPackageDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonCapability {
    pub surface: String,
    pub scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageRunnableEntrypoint {
    pub id: String,
    pub kind: String,
    pub launch_mode: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub working_directory: DaemonPackageWorkingDirectory,
    #[serde(default)]
    pub environment: Vec<DaemonPackageEnvironmentRequirement>,
    #[serde(default)]
    pub capabilities: Vec<DaemonCapability>,
    pub may_supervise: bool,
    pub process: DaemonPackageProcess,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<DaemonPackageActionState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageWorkingDirectory {
    pub policy: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageEnvironmentRequirement {
    pub name: String,
    pub required: bool,
    pub default: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageProcess {
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exited_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_status: Option<String>,
    #[serde(default)]
    pub diagnostics: Vec<DaemonPackageDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageDiagnostic {
    pub kind: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackageDecision {
    pub package_name: String,
    pub action: String,
    pub state: String,
    pub classification: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPluginLifecycle {
    pub package_name: String,
    pub state: String,
    pub loaded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPluginWorkerCounters {
    pub configured_queue_capacity: usize,
    pub configured_executor_concurrency: usize,
    pub live_plugin_executors: usize,
    pub live_executor_workers: usize,
    pub queued_jobs: usize,
    pub in_flight_jobs: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPluginResourceCounters {
    pub active_timer_resources: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub lifecycle_state: String,
    pub compatibility: DaemonCompatibility,
    pub software: DaemonSoftwareIdentity,
    pub installation: DaemonInstallationIdentity,
    pub host_id: String,
    pub host_display_name: String,
    pub schema_version: u16,
    pub data_dir_configured: bool,
    pub core_initialized: bool,
    pub state_source: String,
    pub package_count: usize,
    pub enabled_package_count: usize,
    pub provider_count: usize,
    pub enabled_provider_count: usize,
    pub session_count: usize,
    pub recovered_sessions: Vec<String>,
    pub stale_sessions: Vec<String>,
    #[serde(default, skip_serializing_if = "DaemonLifecycleCounters::is_empty")]
    pub lifecycle_counters: DaemonLifecycleCounters,
    /// Named Hub∪Core attach occupancy. Absence of a pair is release proof only when `attach_occupancy` is advertised.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub live_attach_occupancy: Vec<DaemonAttachOccupancy>,
    /// Bounded event-plane and owner-loop observations. Omitted when empty.
    #[serde(default, skip_serializing_if = "DaemonObservabilityCounters::is_empty")]
    pub observability: DaemonObservabilityCounters,
    /// Retained ended-session history policy and accounting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<DaemonRetentionAccounting>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonDiagnostic>,
}

/// Retained ended-session history: configured policy and current accounting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonRetentionAccounting {
    pub max_object_bytes: u64,
    pub max_total_bytes: u64,
    pub max_sessions: u32,
    pub total_bytes: u64,
    pub sessions: u32,
    pub evictions: u64,
}

/// One live attach occupancy row visible to a sibling Unix client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonAttachOccupancy {
    pub session_id: String,
    pub subscription_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSoftwareIdentity {
    pub product_id: String,
    pub product_name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_revision: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonInstallationMode {
    Development,
    Unmanaged,
    Managed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonInstallationIdentity {
    pub mode: DaemonInstallationMode,
    pub provenance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonInstallationDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonInstallationDiagnostic {
    pub kind: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonHubUpdateState {
    Current,
    Available,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonHubUpdate {
    pub state: DaemonHubUpdateState,
    pub current_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonHubUpdateScope {
    Core,
    All,
}

impl DaemonHubUpdateScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonHubUpdateExecutionState {
    Started,
    Running,
    Complete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonHubUpdateExecution {
    pub update_id: String,
    pub scope: DaemonHubUpdateScope,
    pub state: DaemonHubUpdateExecutionState,
    pub updater_pid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Sanitized daemon transport and subscription lifecycle observations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonLifecycleCounters {
    pub accepted_connections: u64,
    pub rejected_connections: u64,
    pub live_connections: u64,
    pub high_water_live_connections: u64,
    pub live_entity_subscriptions: u64,
    pub high_water_entity_subscriptions: u64,
    pub live_attach_subscriptions: u64,
    pub high_water_attach_subscriptions: u64,
    pub reconnect_registrations: u64,
    pub cleanup_completed: u64,
    pub cleanup_failed: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cleanup_by_reason: BTreeMap<String, u64>,
    pub reconciliation_wakes: u64,
    pub lifecycle_change_reads: u64,
    pub lifecycle_baseline_reads: u64,
    pub lifecycle_resync_reads: u64,
    pub lifecycle_session_drains: u64,
    pub entity_delivery_attempts: u64,
    pub entity_delivery_successes: u64,
    pub entity_delivery_overflows: u64,
    pub entity_delivery_failures: u64,
    pub stalled_writes: u64,
    /// Package entity provider resync attempts across families.
    #[serde(default)]
    pub package_entity_resync_attempts: u64,
    /// Times a family entered resync_degraded after max attempts.
    #[serde(default)]
    pub package_entity_resync_degraded: u64,
    /// Package entity mutations accepted for fanout.
    #[serde(default)]
    pub package_entity_publish_accepted: u64,
}

impl DaemonLifecycleCounters {
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// Bounded event-plane, owner-turn, and ready-wait observations.
///
/// Marked non-exhaustive so additive counters remain source-compatible for
/// external Rust consumers that construct this DTO.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonObservabilityCounters {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub event_shed_by_reason: BTreeMap<String, u64>,
    #[serde(default)]
    pub event_admission_attempts: u64,
    #[serde(default)]
    pub event_delivery_attempts: u64,
    #[serde(default, skip_serializing_if = "DaemonLatencyHistogram::is_empty")]
    pub event_admission_latency: DaemonLatencyHistogram,
    #[serde(default, skip_serializing_if = "DaemonLatencyHistogram::is_empty")]
    pub event_delivery_latency: DaemonLatencyHistogram,
    #[serde(default)]
    pub event_handler_timed_out: u64,
    #[serde(default)]
    pub event_handler_failed: u64,
    #[serde(default)]
    pub event_handler_cancelled: u64,
    #[serde(default)]
    pub event_handler_backpressured: u64,
    #[serde(default)]
    pub event_handler_worker_stopped: u64,
    #[serde(default)]
    pub event_handler_completed_ok: u64,
    #[serde(default)]
    pub event_router_queue_age_expiries: u64,
    #[serde(default)]
    pub event_mailbox_queue_age_expiries: u64,
    #[serde(default)]
    pub event_mailbox_overflow_gaps: u64,
    #[serde(default)]
    pub event_gaps: u64,
    #[serde(default)]
    pub event_age_sample_failures: u64,
    #[serde(default)]
    pub last_owner_turn_us: u64,
    #[serde(default)]
    pub max_owner_turn_us: u64,
    #[serde(default)]
    pub last_ready_operation_wait_us: u64,
    #[serde(default)]
    pub max_ready_operation_wait_us: u64,
    /// Timeout subset of [`DaemonLifecycleCounters::stalled_writes`].
    #[serde(default)]
    pub stalled_write_timeouts: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queue_ages: Vec<DaemonQueueAgeObservation>,
    /// Occupied envelope bytes across all producer queues. Saturation-safe.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub global_in_flight_bytes: u64,
}

impl DaemonObservabilityCounters {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Fixed-bucket latency histogram. Bucket index is `leading_zeros` of microseconds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonLatencyHistogram {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<u64>,
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub sum_us: u64,
    #[serde(default)]
    pub max_us: u64,
}

impl DaemonLatencyHistogram {
    fn is_empty(&self) -> bool {
        self.count == 0 && self.sum_us == 0 && self.max_us == 0 && self.buckets.is_empty()
    }

    #[must_use]
    pub fn new(buckets: Vec<u64>, count: u64, sum_us: u64, max_us: u64) -> Self {
        Self {
            buckets,
            count,
            sum_us,
            max_us,
        }
    }
}

/// One producer, consumer, or client-mailbox oldest-age observation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonQueueAgeObservation {
    pub kind: DaemonQueueKind,
    /// Producer owner, consumer plugin key, or client connection id.
    pub identity: String,
    /// Present only for `Producer`; identifies which generation the sample belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_generation: Option<u64>,
    pub state: DaemonQueueAgeState,
    /// Present only when `state == Usable`. Microseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_age_us: Option<u64>,
    /// Queue count from the same validated bracket. Absent on indeterminate rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_count: Option<u64>,
    /// Occupied bytes from the same validated bracket. Absent on indeterminate rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_bytes: Option<u64>,
}

impl DaemonQueueAgeObservation {
    #[must_use]
    pub fn new(
        kind: DaemonQueueKind,
        identity: impl Into<String>,
        producer_generation: Option<u64>,
        state: DaemonQueueAgeState,
        oldest_age_us: Option<u64>,
        queue_count: Option<u64>,
    ) -> Self {
        Self {
            kind,
            identity: identity.into(),
            producer_generation,
            state,
            oldest_age_us,
            queue_count,
            queue_bytes: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DaemonQueueKind {
    #[default]
    Producer,
    Consumer,
    ClientMailbox,
    #[serde(other)]
    Unknown,
}

impl DaemonQueueKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Producer => "producer",
            Self::Consumer => "consumer",
            Self::ClientMailbox => "client_mailbox",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DaemonQueueAgeState {
    /// A sample the reader validated; `oldest_age_us` is present.
    Usable,
    /// The queue was observed with `count == 0`. Not a zero age.
    Empty,
    /// The bracket was unstable, the cell was latched invalid, the generation
    /// gate was open, or the cell was missing. Never a value.
    #[default]
    Indeterminate,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSession {
    pub session_id: String,
    pub lifecycle: String,
}

/// Sanitized authoritative row for the built-in `session` entity family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionEntity {
    pub session_uuid: String,
    pub registry_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<String>,
    pub lifecycle_class: String,
    pub rows: u16,
    pub cols: u16,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_type_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_type_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default)]
    pub traits: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_type_lifecycle: Option<String>,
}

/// Entity-frame vocabulary scoped to one daemon subscription.
///
/// Hub validates every record before transport. Session consumers can retain a
/// typed projection by deserializing records as [`DaemonSessionEntity`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonEntityFrame {
    #[serde(rename = "entity_snapshot")]
    Snapshot {
        subscription_id: String,
        entity_type: String,
        snapshot_seq: u64,
        items: Vec<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resync_reason: Option<String>,
    },
    #[serde(rename = "entity_upsert")]
    Upsert {
        subscription_id: String,
        entity_type: String,
        snapshot_seq: u64,
        id: String,
        entity: Value,
    },
    #[serde(rename = "entity_patch")]
    Patch {
        subscription_id: String,
        entity_type: String,
        snapshot_seq: u64,
        id: String,
        patch: Value,
    },
    #[serde(rename = "entity_remove")]
    Remove {
        subscription_id: String,
        entity_type: String,
        snapshot_seq: u64,
        id: String,
    },
    #[serde(rename = "entity_error")]
    Error {
        subscription_id: String,
        entity_type: String,
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSessionCleanup {
    pub session_id: String,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonOperatorError {
    pub code: String,
    pub request_id: String,
    pub operation: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonDiagnostic {
    pub kind: DaemonDiagnosticKind,
    pub operation: Option<String>,
    pub feature: Option<String>,
    pub message: Option<String>,
}

impl DaemonDiagnostic {
    #[must_use]
    pub fn connected(operation: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::Connected,
            operation: Some(operation.into()),
            feature: None,
            message: None,
        }
    }

    /// Build a client-side diagnostic for a transport that disconnected after
    /// the daemon protocol had already been established.
    ///
    /// The daemon does not emit this value as a response frame; clients produce
    /// it locally when their own connection lifecycle proves a post-connect
    /// disconnect.
    #[must_use]
    pub fn disconnected(message: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::Disconnected,
            operation: None,
            feature: None,
            message: Some(message.into()),
        }
    }

    #[must_use]
    pub fn compatibility_mismatch(message: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::CompatibilityMismatch,
            operation: None,
            feature: None,
            message: Some(message.into()),
        }
    }

    #[must_use]
    pub fn unsupported_feature(feature: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::UnsupportedFeature,
            operation: None,
            feature: Some(feature.into()),
            message: None,
        }
    }

    #[must_use]
    pub fn terminal_stream_unavailable(
        operation: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: DaemonDiagnosticKind::TerminalStreamUnavailable,
            operation: Some(operation.into()),
            feature: Some(botster_terminal_protocol::FEATURE_TERMINAL_STREAMING.to_string()),
            message: Some(message.into()),
        }
    }

    #[must_use]
    pub fn action_failure(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::ActionFailure,
            operation: Some(operation.into()),
            feature: None,
            message: Some(message.into()),
        }
    }

    #[must_use]
    pub fn worker_compatibility(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::WorkerCompatibility,
            operation: Some(operation.into()),
            feature: Some(FEATURE_TERMINAL_READBACK.to_string()),
            message: Some(message.into()),
        }
    }

    #[must_use]
    pub fn daemon_startup_failure(message: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::DaemonStartupFailure,
            operation: None,
            feature: None,
            message: Some(message.into()),
        }
    }

    #[must_use]
    pub fn backpressure(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::Backpressure,
            operation: Some(operation.into()),
            feature: None,
            message: Some(message.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonDiagnosticKind {
    Connected,
    /// Client-side-only classification for post-connect transport loss.
    ///
    /// The daemon protocol does not emit this kind as a response frame.
    Disconnected,
    CompatibilityMismatch,
    UnsupportedFeature,
    TerminalStreamUnavailable,
    WorkerCompatibility,
    ActionFailure,
    DaemonStartupFailure,
    Backpressure,
}

/// Binary encoding used by opaque terminal history payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonHistoryEncoding {
    Base64,
}

/// Validated opaque terminal engine state serialized as flat daemon event fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonOpaqueHistoryPayload {
    pub payload_base64: String,
    pub payload_encoding: DaemonHistoryEncoding,
    pub bytes: usize,
}

#[derive(Deserialize)]
struct UncheckedDaemonOpaqueHistoryPayload {
    payload_base64: String,
    payload_encoding: DaemonHistoryEncoding,
    bytes: usize,
}

impl DaemonOpaqueHistoryPayload {
    #[must_use]
    pub fn from_bytes(payload: &[u8]) -> Self {
        Self {
            payload_base64: base64::engine::general_purpose::STANDARD.encode(payload),
            payload_encoding: DaemonHistoryEncoding::Base64,
            bytes: payload.len(),
        }
    }

    /// Decode the opaque bytes after validating their declared length.
    pub fn decoded_bytes(&self) -> Result<Vec<u8>, String> {
        decode_validated_base64_payload(&self.payload_base64, self.bytes, "opaque history")
    }
}

impl TryFrom<UncheckedDaemonOpaqueHistoryPayload> for DaemonOpaqueHistoryPayload {
    type Error = String;

    fn try_from(payload: UncheckedDaemonOpaqueHistoryPayload) -> Result<Self, Self::Error> {
        decode_validated_base64_payload(&payload.payload_base64, payload.bytes, "opaque history")?;
        Ok(Self {
            payload_base64: payload.payload_base64,
            payload_encoding: payload.payload_encoding,
            bytes: payload.bytes,
        })
    }
}

impl<'de> Deserialize<'de> for DaemonOpaqueHistoryPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let payload = UncheckedDaemonOpaqueHistoryPayload::deserialize(deserializer)?;
        Self::try_from(payload).map_err(serde::de::Error::custom)
    }
}

fn decode_validated_base64_payload(
    payload_base64: &str,
    bytes: usize,
    label: &str,
) -> Result<Vec<u8>, String> {
    let payload = base64::engine::general_purpose::STANDARD
        .decode(payload_base64)
        .map_err(|error| format!("invalid {label} base64: {error}"))?;
    if payload.len() != bytes {
        return Err(format!(
            "{label} byte length mismatch: declared {bytes}, decoded {}",
            payload.len()
        ));
    }
    Ok(payload)
}

/// Unsolicited host events.
///
/// Terminal output, snapshots, attach state, input results, mode changes, and
/// process exit never appear here. They travel only on the terminal stream as
/// Core scheme 2 bodies inside terminal containers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonEvent {
    SessionLifecycle {
        session_id: String,
        state: String,
    },
    RuntimeObservation {
        kind: String,
    },
    WorktreeLifecycle {
        event: DaemonWorktreeLifecycleEvent,
    },
    /// Bound terminal subscription closed while this connection stayed alive.
    ///
    /// This is an unsolicited host event. It is not a request reply and not
    /// `AttachFailed`.
    TerminalSubscriptionClosed {
        session_id: String,
        subscription_id: String,
        generation: u64,
        reason: String,
    },
    /// Unsolicited live package event for one host-control subscription.
    PackageEvent {
        subscription_id: String,
        owner: String,
        name: String,
        payload: Value,
    },
    /// Coalesced gap after a shed or expired delivery. No replay or history.
    EventGap {
        subscription_id: String,
        owner: String,
        name: String,
    },
}

/// Header of one binary local WebRTC terminal chunk. See
/// [`LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES`] for the byte layout.
///
/// The route is the subscription DataChannel label. Both directions use this
/// header: Hub to client carries slices of one Core terminal-stream body;
/// client to Hub carries slices of one Core terminal input frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalWebrtcTerminalChunkHeader {
    /// Per-channel, per-direction message counter. Strictly increasing from 1.
    pub message_id: u64,
    pub chunk_index: u32,
    pub chunk_count: u32,
    /// Plaintext body length of the whole message.
    pub total_bytes: u32,
    /// Fixed attachment generation. A chunk whose generation differs from the bound route is discarded.
    pub generation: u64,
    /// Core stream epoch captured when the frame was queued, copied verbatim by Hub.
    pub stream_epoch: u32,
}

impl LocalWebrtcTerminalChunkHeader {
    /// Encode the fixed header.
    #[must_use]
    pub fn encode(&self) -> [u8; LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES] {
        let mut bytes = [0u8; LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES];
        bytes[0] = LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION as u8;
        bytes[1..9].copy_from_slice(&self.message_id.to_le_bytes());
        bytes[9..13].copy_from_slice(&self.chunk_index.to_le_bytes());
        bytes[13..17].copy_from_slice(&self.chunk_count.to_le_bytes());
        bytes[17..21].copy_from_slice(&self.total_bytes.to_le_bytes());
        bytes[21..29].copy_from_slice(&self.generation.to_le_bytes());
        bytes[29..33].copy_from_slice(&self.stream_epoch.to_le_bytes());
        bytes
    }

    /// Split one DataChannel message into its header and sealed slice.
    ///
    /// Returns `None` when the version is wrong, the message is shorter than
    /// the header plus nonce plus tag, or the declared counts are inconsistent.
    #[must_use]
    pub fn decode(message: &[u8]) -> Option<(Self, &[u8])> {
        let sealed_min =
            LOCAL_WEBRTC_TERMINAL_CHUNK_NONCE_BYTES + LOCAL_WEBRTC_TERMINAL_CHUNK_TAG_BYTES;
        if message.len() < LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES + sealed_min
            || message.len() >= LOCAL_WEBRTC_MAX_FRAME_BYTES
            || message[0] != LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION as u8
        {
            return None;
        }
        let read_u32 = |start: usize| {
            u32::from_le_bytes([
                message[start],
                message[start + 1],
                message[start + 2],
                message[start + 3],
            ])
        };
        let mut message_id = [0u8; 8];
        message_id.copy_from_slice(&message[1..9]);
        let mut generation = [0u8; 8];
        generation.copy_from_slice(&message[21..29]);
        let header = Self {
            message_id: u64::from_le_bytes(message_id),
            chunk_index: read_u32(9),
            chunk_count: read_u32(13),
            total_bytes: read_u32(17),
            generation: u64::from_le_bytes(generation),
            stream_epoch: read_u32(29),
        };
        if header.chunk_count == 0
            || header.chunk_index >= header.chunk_count
            || header.total_bytes as usize > LOCAL_WEBRTC_MAX_DELIVERY_BYTES
            || header.chunk_count > header.total_bytes.max(1)
        {
            return None;
        }
        Some((header, &message[LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES..]))
    }
}

pub type DaemonTransportResult<T> = Result<T, DaemonTransportError>;

/// Deterministic TypeScript definitions for the browser-visible daemon protocol.
#[must_use]
pub fn daemon_protocol_typescript() -> String {
    typescript::daemon_protocol_typescript()
}

#[derive(Debug)]
pub enum DaemonTransportError {
    Io(std::io::Error),
    Json(serde_json::Error),
    MissingSocketBinding,
    AlreadyRunning,
    NotRunning,
    ClientDisconnected,
    Protocol(&'static str),
    /// A typed framing or correlation violation observed on the wire.
    ProtocolViolation(DaemonProtocolErrorCode),
    /// Hub sent a typed close reason before closing.
    ClosedByHub(DaemonCloseReason),
    /// A submitted request did not complete.
    Request(DaemonRequestError),
    Compatibility(DaemonCompatibilityError),
    ControlThreadStopped,
}

impl fmt::Display for DaemonTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "daemon transport io error: {error}"),
            Self::Json(error) => write!(formatter, "daemon transport json error: {error}"),
            Self::MissingSocketBinding => {
                write!(formatter, "local socket binding is not configured")
            }
            Self::AlreadyRunning => write!(formatter, "botster-hub daemon is already running"),
            Self::NotRunning => write!(formatter, "botster-hub daemon is not running"),
            Self::ClientDisconnected => write!(formatter, "daemon client disconnected"),
            Self::Protocol(message) => write!(formatter, "daemon protocol error: {message}"),
            Self::ProtocolViolation(code) => {
                write!(formatter, "daemon protocol violation: {}", code.as_str())
            }
            Self::ClosedByHub(reason) => match reason {
                DaemonCloseReason::ProtocolError { code } => write!(
                    formatter,
                    "daemon closed the connection: protocol_error {}",
                    code.as_str()
                ),
                DaemonCloseReason::DaemonShutdown => {
                    write!(formatter, "daemon closed the connection: daemon_shutdown")
                }
            },
            Self::Request(error) => write!(formatter, "{error}"),
            Self::Compatibility(error) => write!(formatter, "{error}"),
            Self::ControlThreadStopped => write!(formatter, "daemon control thread stopped"),
        }
    }
}

impl Error for DaemonTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Compatibility(error) => Some(error),
            Self::Request(error) => Some(error),
            Self::MissingSocketBinding
            | Self::AlreadyRunning
            | Self::NotRunning
            | Self::ClientDisconnected
            | Self::Protocol(_)
            | Self::ProtocolViolation(_)
            | Self::ClosedByHub(_)
            | Self::ControlThreadStopped => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn entity_frames_round_trip_canonical_wire_vocabulary() {
        let entity = DaemonSessionEntity {
            session_uuid: "session".to_string(),
            registry_state: "running".to_string(),
            lifecycle: Some("running".to_string()),
            lifecycle_class: "current".to_string(),
            rows: 24,
            cols: 80,
            updated_at: 7,
            exit_code: None,
            failure_reason: None,
            session_type_id: None,
            session_type_source: None,
            role: None,
            traits: Vec::new(),
            interaction: None,
            session_type_lifecycle: None,
        };
        let frames = vec![
            DaemonEntityFrame::Snapshot {
                subscription_id: "subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 1,
                items: vec![serde_json::to_value(&entity).expect("serialize session entity")],
                resync_reason: None,
            },
            DaemonEntityFrame::Upsert {
                subscription_id: "subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 2,
                id: "session".to_string(),
                entity: serde_json::to_value(entity).expect("serialize session entity"),
            },
            DaemonEntityFrame::Patch {
                subscription_id: "subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 3,
                id: "session".to_string(),
                patch: serde_json::json!({"lifecycle": "exited", "exit_code": 0}),
            },
            DaemonEntityFrame::Remove {
                subscription_id: "subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 4,
                id: "session".to_string(),
            },
            DaemonEntityFrame::Error {
                subscription_id: "subscription".to_string(),
                entity_type: "session_type".to_string(),
                code: "entity_provider_frame_too_large".to_string(),
                message: "session type snapshot exceeds daemon frame limit".to_string(),
            },
        ];

        for frame in frames {
            let value = serde_json::to_value(&frame).expect("serialize entity frame");
            assert!(value.get("type").is_some());
            assert_eq!(
                serde_json::from_value::<DaemonEntityFrame>(value)
                    .expect("deserialize entity frame"),
                frame
            );
        }
    }

    fn empty_test_response(kind: DaemonResponseKind) -> DaemonResponse {
        DaemonResponse {
            kind,
            status: None,
            sessions: Vec::new(),
            session_types: Vec::new(),
            session_type_definition: None,
            resolved_session_type: None,
            session_context: None,
            read_screen: None,
            mode_flags: None,
            terminal_attach: None,
            terminal_reservation: None,
            subscription_reservation: None,
            capture_snapshot: None,
            snapshot_page: None,
            spawn_targets: Vec::new(),
            spawn_target_validation: None,
            worktrees: Vec::new(),
            apps: Vec::new(),
            resolved_app_launch: None,
            resolved_package_route: None,
            package_navigation: Vec::new(),
            packages: Vec::new(),
            available_packages: Vec::new(),
            install_plan: None,
            update_status: None,
            hub_update: None,
            hub_update_execution: None,
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_worker_counters: None,
            plugin_resource_counters: None,
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            plugin_surface: None,
            plugin_action_result: None,
            local_webrtc_bootstrap: None,
            local_webrtc_answer: None,
            events: Vec::new(),
            cleanup: None,
            coordination: None,
            error: None,
            diagnostics: Vec::new(),
        }
    }

    /// Read one scripted client request and return its id.
    fn expect_request(
        frames: &mut DaemonUnixFrameReader,
        stream: &mut UnixStream,
        expected: &DaemonRequest,
    ) -> String {
        let raw = frames
            .read_raw_frame(stream, MAX_UNIX_FRAME_BYTES)
            .expect("read scripted client frame");
        match decode_unix_frame::<ClientFrame>(&raw).expect("decode client frame") {
            DaemonUnixFrame::Control(ClientFrame::Request {
                request_id,
                request,
            }) => {
                assert_eq!(&request, expected);
                request_id
            }
            other => panic!("expected a client request, got {other:?}"),
        }
    }

    fn read_screen_response(session_id: &str, text: &str) -> DaemonResponse {
        DaemonResponse {
            read_screen: Some(DaemonReadScreen {
                session_id: session_id.to_string(),
                text: text.to_string(),
                unavailable: None,
            }),
            ..empty_test_response(DaemonResponseKind::ReadScreen)
        }
    }

    fn test_connection(client: UnixStream) -> DaemonConnection {
        DaemonConnection::from_hello_complete_stream(client, Vec::new()).expect("connection")
    }

    #[test]
    fn stream_attach_retains_late_output_across_running_lifecycle_readbacks() {
        let (mut server, client) = UnixStream::pair().expect("pair unix streams");
        let server_handle = thread::spawn(move || {
            let mut frames = DaemonUnixFrameReader::new();
            let attach_id = expect_request(
                &mut frames,
                &mut server,
                &DaemonRequest::Attach {
                    session_id: "session".to_string(),
                    subscription_id: "subscription".to_string(),
                },
            );
            write_server_frame(
                &mut server,
                &ServerFrame::Response {
                    request_id: attach_id,
                    response: empty_test_response(DaemonResponseKind::Events),
                },
            )
            .expect("write attach response");
            let read_id = expect_request(
                &mut frames,
                &mut server,
                &DaemonRequest::ReadScreen {
                    session_id: "session".to_string(),
                },
            );
            write_server_frame(
                &mut server,
                &ServerFrame::Response {
                    request_id: read_id,
                    response: read_screen_response("session", "late-output"),
                },
            )
            .expect("write read_screen response");
        });
        let mut output = Vec::new();
        let mut connection = test_connection(client);

        stream_attach_connected(&mut connection, "session", "subscription", &mut output)
            .expect("stream attach writes current ReadScreen text");
        drop(connection);
        server_handle.join().expect("scripted server completes");

        assert_eq!(output, b"late-output\n");
    }

    #[test]
    fn stream_attach_prints_nothing_when_history_is_unavailable() {
        let (mut server, client) = UnixStream::pair().expect("pair unix streams");
        let server_handle = thread::spawn(move || {
            let mut frames = DaemonUnixFrameReader::new();
            let attach_id = expect_request(
                &mut frames,
                &mut server,
                &DaemonRequest::Attach {
                    session_id: "session".to_string(),
                    subscription_id: "subscription".to_string(),
                },
            );
            write_server_frame(
                &mut server,
                &ServerFrame::Response {
                    request_id: attach_id,
                    response: empty_test_response(DaemonResponseKind::Events),
                },
            )
            .expect("write attach response");
            let read_id = expect_request(
                &mut frames,
                &mut server,
                &DaemonRequest::ReadScreen {
                    session_id: "session".to_string(),
                },
            );
            let mut response = read_screen_response("session", "");
            response.read_screen.as_mut().expect("screen").unavailable =
                Some(HistoryUnavailableReason::Restart);
            write_server_frame(
                &mut server,
                &ServerFrame::Response {
                    request_id: read_id,
                    response,
                },
            )
            .expect("write read_screen response");
        });
        let mut output = Vec::new();
        let mut connection = test_connection(client);

        stream_attach_connected(&mut connection, "session", "subscription", &mut output)
            .expect("unavailable history is not an attach failure");
        drop(connection);
        server_handle.join().expect("scripted server completes");

        assert!(output.is_empty());
    }

    #[test]
    fn wait_response_parks_out_of_order_responses_and_skips_other_frames() {
        let (mut server, client) = UnixStream::pair().expect("pair unix streams");
        let mut connection = test_connection(client);
        let first = connection
            .submit(&DaemonRequest::Status)
            .expect("submit first");
        let second = connection
            .submit(&DaemonRequest::ListSessions)
            .expect("submit second");
        assert_eq!(connection.outstanding_request_ids(), &[first, second]);

        let server_handle = thread::spawn(move || {
            let mut frames = DaemonUnixFrameReader::new();
            let first_id = expect_request(&mut frames, &mut server, &DaemonRequest::Status);
            let second_id = expect_request(&mut frames, &mut server, &DaemonRequest::ListSessions);
            write_unix_terminal_frame(&mut server, "route", 5, 0, b"opaque").expect("terminal");
            write_server_frame(
                &mut server,
                &ServerFrame::Event {
                    event: DaemonEvent::SessionLifecycle {
                        session_id: "session".to_string(),
                        state: "running".to_string(),
                    },
                },
            )
            .expect("event");
            write_server_frame(
                &mut server,
                &ServerFrame::Response {
                    request_id: second_id,
                    response: empty_test_response(DaemonResponseKind::Sessions),
                },
            )
            .expect("second response first");
            write_server_frame(
                &mut server,
                &ServerFrame::Response {
                    request_id: "999".to_string(),
                    response: empty_test_response(DaemonResponseKind::Status),
                },
            )
            .expect("unknown id response");
            write_server_frame(
                &mut server,
                &ServerFrame::Response {
                    request_id: first_id,
                    response: empty_test_response(DaemonResponseKind::Status),
                },
            )
            .expect("first response last");
        });

        let response = connection.wait_response(first).expect("first completes");
        assert_eq!(response.kind, DaemonResponseKind::Status);
        assert_eq!(connection.outstanding_request_ids(), &[] as &[u64]);
        let response = connection.wait_response(second).expect("parked second");
        assert_eq!(response.kind, DaemonResponseKind::Sessions);
        assert_eq!(connection.take_skipped_terminal().len(), 1);
        assert_eq!(connection.take_skipped_events().len(), 1);
        assert!(matches!(
            connection.wait_response(999),
            Err(DaemonTransportError::Request(DaemonRequestError::Cancelled))
        ));
        server_handle.join().expect("scripted server completes");
    }

    #[test]
    fn submit_refuses_a_thirty_third_outstanding_request_locally() {
        let (server, client) = UnixStream::pair().expect("pair unix streams");
        let mut connection = test_connection(client);
        for _ in 0..MAX_OUTSTANDING_REQUESTS {
            connection.submit(&DaemonRequest::Status).expect("submit");
        }
        assert!(matches!(
            connection.submit(&DaemonRequest::Status),
            Err(DaemonTransportError::Request(
                DaemonRequestError::TooManyOutstandingRequests
            ))
        ));
        assert!(connection.cancel(1));
        assert!(!connection.cancel(1));
        connection
            .submit(&DaemonRequest::Status)
            .expect("a cancelled slot frees the guard");
        drop(server);
    }

    #[test]
    fn hub_close_frame_surfaces_a_typed_reason() {
        let (mut server, client) = UnixStream::pair().expect("pair unix streams");
        let mut connection = test_connection(client);
        write_server_frame(
            &mut server,
            &ServerFrame::Close {
                reason: DaemonCloseReason::ProtocolError {
                    code: DaemonProtocolErrorCode::InvalidRequestId,
                },
            },
        )
        .expect("close");
        let error = connection.next_event().expect_err("close ends the wait");
        assert!(matches!(
            error,
            DaemonTransportError::ClosedByHub(DaemonCloseReason::ProtocolError {
                code: DaemonProtocolErrorCode::InvalidRequestId
            })
        ));
        assert!(connection.close_reason().is_some());
    }

    #[test]
    fn teardown_io_kinds_normalize_to_client_disconnected() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::UnexpectedEof,
        ] {
            assert!(matches!(
                normalize_socket_io_error(std::io::Error::from(kind)),
                DaemonTransportError::ClientDisconnected
            ));
        }

        let error =
            normalize_socket_io_error(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert!(matches!(
            error,
            DaemonTransportError::Io(error)
                if error.kind() == std::io::ErrorKind::PermissionDenied
        ));
    }

    #[test]
    fn compatibility_accepts_current_descriptor() {
        ensure_compatible(
            &DaemonCompatibilityRequirement::current(),
            &DaemonCompatibility::current(),
        )
        .expect("current client and hub are compatible");
    }

    #[test]
    fn default_requirement_accepts_daemon_before_optional_source_update() {
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon
            .features
            .retain(|feature| feature != FEATURE_HUB_SOURCE_UPDATE);

        ensure_compatible(&DaemonCompatibilityRequirement::current(), &previous_daemon)
            .expect("the optional source-update capability must not break default clients");
    }

    #[test]
    fn source_update_requirement_rejects_old_daemon_and_accepts_current_daemon() {
        let requirement = DaemonCompatibilityRequirement::for_hub_source_update();
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon
            .features
            .retain(|feature| feature != FEATURE_HUB_SOURCE_UPDATE);

        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("a source-update client must reject an old daemon");
        assert!(error.diagnostic.contains(&format!(
            "unsupported conformance fixture revision {}; requires at least {CONFORMANCE_FIXTURE_REVISION}",
            DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION
        )));

        previous_daemon.conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("a source-update client must require the advertised feature");
        assert!(
            error
                .diagnostic
                .contains("missing required feature(s): hub_source_update")
        );

        ensure_compatible(&requirement, &DaemonCompatibility::current())
            .expect("a source-update client accepts the current daemon");
    }

    #[test]
    fn default_requirement_accepts_daemon_before_optional_ready_then_history() {
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon.features.retain(|feature| {
            feature != botster_terminal_protocol::FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY
        });

        ensure_compatible(&DaemonCompatibilityRequirement::current(), &previous_daemon)
            .expect("the optional ready-then-history capability must not break default clients");
    }

    #[test]
    fn ready_then_history_requirement_rejects_old_daemon_and_accepts_current_daemon() {
        let requirement = DaemonCompatibilityRequirement::for_ready_then_history_attach();
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon.features.retain(|feature| {
            feature != botster_terminal_protocol::FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY
        });

        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("a ready-then-history client must reject an old daemon");
        assert!(error.diagnostic.contains(&format!(
            "unsupported conformance fixture revision {}; requires at least {CONFORMANCE_FIXTURE_REVISION}",
            DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION
        )));

        previous_daemon.conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        ensure_compatible(&requirement, &previous_daemon)
            .expect("ready-then-history is a terminal-plane requirement, not a host feature");

        ensure_compatible(&requirement, &DaemonCompatibility::current())
            .expect("a ready-then-history client accepts the current daemon");
    }

    #[test]
    fn default_requirement_accepts_daemon_before_optional_unix_terminal_adapter() {
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon
            .features
            .retain(|feature| feature != FEATURE_UNIX_TERMINAL_ADAPTER);

        ensure_compatible(&DaemonCompatibilityRequirement::current(), &previous_daemon)
            .expect("the optional unix adapter capability must not break default clients");
    }

    #[test]
    fn unix_terminal_adapter_requirement_rejects_old_daemon_and_accepts_current_daemon() {
        let requirement = DaemonCompatibilityRequirement::for_unix_terminal_adapter();
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon
            .features
            .retain(|feature| feature != FEATURE_UNIX_TERMINAL_ADAPTER);

        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("a unix-adapter client must reject an old daemon");
        assert!(error.diagnostic.contains(&format!(
            "unsupported conformance fixture revision {}; requires at least {CONFORMANCE_FIXTURE_REVISION}",
            DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION
        )));

        previous_daemon.conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("a unix-adapter client must require the advertised feature");
        assert!(
            error
                .diagnostic
                .contains("missing required feature(s): unix_terminal_adapter")
        );

        ensure_compatible(&requirement, &DaemonCompatibility::current())
            .expect("a unix-adapter client accepts the current daemon");
    }

    #[test]
    fn default_requirement_accepts_daemon_before_optional_webrtc_terminal_adapter() {
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon
            .features
            .retain(|feature| feature != FEATURE_WEBRTC_TERMINAL_ADAPTER);

        ensure_compatible(&DaemonCompatibilityRequirement::current(), &previous_daemon)
            .expect("the optional webrtc adapter capability must not break default clients");
    }

    #[test]
    fn webrtc_terminal_adapter_requirement_rejects_old_daemon_and_accepts_current_daemon() {
        let requirement = DaemonCompatibilityRequirement::for_webrtc_terminal_adapter();
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon
            .features
            .retain(|feature| feature != FEATURE_WEBRTC_TERMINAL_ADAPTER);

        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("a webrtc-adapter client must reject an old daemon");
        assert!(error.diagnostic.contains(&format!(
            "unsupported conformance fixture revision {}; requires at least {CONFORMANCE_FIXTURE_REVISION}",
            DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION
        )));

        previous_daemon.conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("a webrtc-adapter client must require the advertised feature");
        assert!(
            error
                .diagnostic
                .contains("missing required feature(s): webrtc_terminal_adapter")
        );

        ensure_compatible(&requirement, &DaemonCompatibility::current())
            .expect("a webrtc-adapter client accepts the current daemon");
        assert!(
            !requirement
                .required_features
                .iter()
                .any(|feature| feature == FEATURE_TERMINAL_SUBSCRIPTION_CLOSED),
            "the adapter helper must not require terminal_subscription_closed"
        );
    }

    #[test]
    fn webrtc_terminal_subscription_closed_requirement_requires_both_features() {
        let requirement = DaemonCompatibilityRequirement::for_webrtc_terminal_subscription_closed();
        assert!(
            requirement
                .required_features
                .iter()
                .any(|feature| feature == FEATURE_WEBRTC_TERMINAL_ADAPTER)
        );
        assert!(
            requirement
                .required_features
                .iter()
                .any(|feature| feature == FEATURE_TERMINAL_SUBSCRIPTION_CLOSED)
        );
        assert!(
            !DaemonCompatibilityRequirement::current()
                .required_features
                .iter()
                .any(|feature| feature == FEATURE_TERMINAL_SUBSCRIPTION_CLOSED)
        );

        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        previous_daemon
            .features
            .retain(|feature| feature != FEATURE_TERMINAL_SUBSCRIPTION_CLOSED);
        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("close-event clients must require the negotiated feature");
        assert!(
            error
                .diagnostic
                .contains("missing required feature(s): terminal_subscription_closed")
        );
        ensure_compatible(&requirement, &DaemonCompatibility::current())
            .expect("a negotiated close-event client accepts the current daemon");
    }

    #[test]
    fn default_requirement_accepts_daemon_before_optional_attach_occupancy() {
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon
            .features
            .retain(|feature| feature != FEATURE_ATTACH_OCCUPANCY);

        ensure_compatible(&DaemonCompatibilityRequirement::current(), &previous_daemon)
            .expect("the optional occupancy capability must not break default clients");
        assert!(
            !DaemonCompatibilityRequirement::current()
                .required_features
                .iter()
                .any(|feature| feature == FEATURE_ATTACH_OCCUPANCY)
        );
    }

    #[test]
    fn attach_occupancy_requirement_rejects_old_daemon_and_accepts_current_daemon() {
        let requirement = DaemonCompatibilityRequirement::for_attach_occupancy();
        let mut previous_daemon = DaemonCompatibility::current();
        previous_daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
        previous_daemon
            .features
            .retain(|feature| feature != FEATURE_ATTACH_OCCUPANCY);

        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("an occupancy client must reject an old daemon");
        assert!(error.diagnostic.contains(&format!(
            "unsupported conformance fixture revision {}; requires at least {CONFORMANCE_FIXTURE_REVISION}",
            DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION
        )));

        previous_daemon.conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        let error = ensure_compatible(&requirement, &previous_daemon)
            .expect_err("an occupancy client must require the advertised feature");
        assert!(
            error
                .diagnostic
                .contains("missing required feature(s): attach_occupancy")
        );
        ensure_compatible(&requirement, &DaemonCompatibility::current())
            .expect("an occupancy client accepts the current daemon");
    }

    #[test]
    fn terminal_subscription_closed_is_a_server_event_not_a_request_reply() {
        let event = DaemonEvent::TerminalSubscriptionClosed {
            session_id: "session".to_string(),
            subscription_id: "sub".to_string(),
            generation: 2,
            reason: TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER.to_string(),
        };
        let frame = ServerFrame::Event {
            event: event.clone(),
        };
        let value = serde_json::to_value(&frame).expect("frame serializes");
        assert_eq!(value["frame"], "event");
        assert_eq!(value["event"]["type"], "terminal_subscription_closed");
        assert!(value.get("request_id").is_none());
        let encoded = encode_server_frame(&frame).expect("encode");
        match decode_unix_frame::<ServerFrame>(&encoded[UNIX_FRAME_LENGTH_PREFIX_BYTES..])
            .expect("decode")
        {
            DaemonUnixFrame::Control(ServerFrame::Event { event: decoded }) => {
                assert_eq!(decoded, event);
            }
            other => panic!("close event must not decode as {other:?}"),
        }
    }

    #[test]
    fn package_event_dtos_omit_replay_and_empty_subjects() {
        let subscribe = DaemonRequest::SubscribeEvents {
            subscription_id: "sub".to_string(),
            owner: "owner".to_string(),
            name: "ready".to_string(),
            subjects: Vec::new(),
        };
        let subscribe_value = serde_json::to_value(&subscribe).expect("subscribe serializes");
        assert_eq!(subscribe_value["type"], "subscribe_events");
        assert!(subscribe_value.get("subjects").is_none());
        assert!(subscribe_value.get("sequence").is_none());
        assert!(subscribe_value.get("cursor").is_none());
        assert!(subscribe_value.get("replay").is_none());
        assert!(subscribe_value.get("durable").is_none());

        let event = DaemonEvent::PackageEvent {
            subscription_id: "sub".to_string(),
            owner: "owner".to_string(),
            name: "ready".to_string(),
            payload: serde_json::json!({ "ok": true }),
        };
        let event_value = serde_json::to_value(&event).expect("event serializes");
        assert_eq!(event_value["type"], "package_event");
        assert!(event_value.get("sequence").is_none());
        assert!(event_value.get("cursor").is_none());
        assert!(event_value.get("replay").is_none());

        let gap = DaemonEvent::EventGap {
            subscription_id: "sub".to_string(),
            owner: "owner".to_string(),
            name: "ready".to_string(),
        };
        let gap_value = serde_json::to_value(&gap).expect("gap serializes");
        assert_eq!(gap_value["type"], "event_gap");
        assert!(gap_value.get("sequence").is_none());
    }

    #[test]
    fn next_event_reads_unsolicited_package_event_without_a_control_request() {
        let (mut server, client) = UnixStream::pair().expect("pair");
        let event = DaemonEvent::PackageEvent {
            subscription_id: "sub".to_string(),
            owner: "owner".to_string(),
            name: "ready".to_string(),
            payload: serde_json::json!({ "ok": true }),
        };
        let server_handle = thread::spawn(move || {
            write_server_frame(&mut server, &ServerFrame::Event { event })
                .expect("write unsolicited event");
        });
        let mut connection = DaemonConnection::from_hello_complete_stream(
            client,
            vec![FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.to_string()],
        )
        .expect("connection");
        match connection.next_event().expect("next event") {
            DaemonEvent::PackageEvent {
                subscription_id,
                name,
                ..
            } => {
                assert_eq!(subscription_id, "sub");
                assert_eq!(name, "ready");
            }
            other => panic!("expected PackageEvent, got {other:?}"),
        }
        server_handle.join().expect("server writes");
    }

    #[test]
    fn poll_terminal_keeps_a_split_frame_and_returns_none_on_timeout() {
        let (mut server, client) = UnixStream::pair().expect("pair");
        let mut connection = test_connection(client);
        let frame = encode_unix_terminal_frame("sub", 1, 0, b"a").expect("encode");
        server.write_all(&frame[..9]).expect("write prefix");
        assert!(
            connection
                .poll_terminal(Duration::from_millis(30))
                .expect("prefix poll")
                .is_none()
        );
        server.write_all(&frame[9..]).expect("write suffix");
        let decoded = connection
            .poll_terminal(Duration::from_secs(1))
            .expect("suffix poll")
            .expect("terminal frame");
        assert_eq!(decoded.route, "sub");
        assert_eq!(decoded.body, b"a");
    }

    #[test]
    fn poll_terminal_deadline_covers_continuous_nonterminal_events() {
        let (mut server, client) = UnixStream::pair().expect("pair");
        let mut connection = test_connection(client);
        let event = DaemonEvent::PackageEvent {
            subscription_id: "sub".to_string(),
            owner: "owner".to_string(),
            name: "ready".to_string(),
            payload: serde_json::json!({ "ok": true }),
        };
        let server_handle = thread::spawn(move || {
            let started = Instant::now();
            while started.elapsed() < Duration::from_millis(600) {
                if write_server_frame(
                    &mut server,
                    &ServerFrame::Event {
                        event: event.clone(),
                    },
                )
                .is_err()
                {
                    return;
                }
                thread::sleep(Duration::from_millis(5));
            }
            thread::sleep(Duration::from_millis(250));
        });
        let started = Instant::now();
        let polled = connection
            .poll_terminal(Duration::from_millis(80))
            .expect("poll events-only stream");
        assert!(polled.is_none(), "events must not count as terminal");
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "absolute deadline must not restart on events, elapsed={:?}",
            started.elapsed()
        );
        assert!(
            !server_handle.is_finished(),
            "poll must return while the event writer is still alive"
        );
        assert!(!connection.take_skipped_events().is_empty());
        drop(connection);
        let _ = server_handle.join();
    }

    #[test]
    fn package_event_requirement_is_operation_specific() {
        let previous = {
            let mut daemon = DaemonCompatibility::current();
            daemon.conformance_fixture_revision = DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION;
            daemon
                .features
                .retain(|feature| feature != FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS);
            daemon
        };
        ensure_compatible(&DaemonCompatibilityRequirement::current(), &previous)
            .expect("default requirement still accepts the previous descriptor");
        let requirement = DaemonCompatibilityRequirement::for_package_event_subscriptions();
        let error = ensure_compatible(&requirement, &previous)
            .expect_err("event requirement rejects the previous descriptor");
        assert!(
            error
                .diagnostic
                .contains("unsupported conformance fixture revision")
        );
        let mut current = previous;
        current.conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION;
        current
            .features
            .push(FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.to_string());
        ensure_compatible(&requirement, &current)
            .expect("event requirement accepts the new descriptor");
        assert!(hello_requires_package_event_subscriptions(
            &requirement.required_features
        ));
        assert!(!hello_requires_package_event_subscriptions(
            &DaemonCompatibilityRequirement::current().required_features
        ));
    }

    #[test]
    fn compatibility_reports_unsupported_protocol_version() {
        let mut requirement = DaemonCompatibilityRequirement::current();
        requirement.protocol_version = PROTOCOL_VERSION + 1;
        requirement.client_name = "version-test-client".to_string();

        let error = ensure_compatible(&requirement, &DaemonCompatibility::current())
            .expect_err("newer client requirement should fail against current hub");

        assert!(error.diagnostic.contains("version-test-client"));
        assert!(error.diagnostic.contains(&format!(
            "unsupported protocol version {PROTOCOL_VERSION}; client requires {}",
            PROTOCOL_VERSION + 1
        )));
    }

    #[test]
    fn compatibility_rejects_a_stale_client_before_removed_operations_dispatch() {
        let mut stale = DaemonCompatibilityRequirement::current();
        stale.protocol_version = PROTOCOL_VERSION - 1;
        stale.minimum_conformance_fixture_revision = CONFORMANCE_FIXTURE_REVISION - 1;
        stale
            .required_features
            .retain(|feature| feature != FEATURE_SESSION_TYPE_ENTITY_SUBSCRIPTIONS);
        stale.client_name = "stale-first-party-client".to_string();

        let error = ensure_compatible(&stale, &DaemonCompatibility::current())
            .expect_err("stale request semantics must fail before dispatch");

        assert!(error.diagnostic.contains("stale-first-party-client"));
        assert!(error.diagnostic.contains("unsupported protocol version"));
        assert!(!error.diagnostic.contains("unknown_operation"));
    }

    #[test]
    fn compatibility_reports_missing_required_feature() {
        let mut requirement = DaemonCompatibilityRequirement::current();
        requirement
            .required_features
            .push("future_feature".to_string());
        requirement.client_name = "feature-test-client".to_string();

        let error = ensure_compatible(&requirement, &DaemonCompatibility::current())
            .expect_err("future feature should fail against current hub");

        assert!(error.diagnostic.contains("feature-test-client"));
        assert!(
            error
                .diagnostic
                .contains("missing required feature(s): future_feature")
        );
        assert_eq!(
            error.diagnostics,
            vec![DaemonDiagnostic::unsupported_feature("future_feature")]
        );
    }

    #[test]
    fn backpressure_diagnostic_is_serde_stable_and_generated() {
        let diagnostic = DaemonDiagnostic::backpressure(
            "daemon_client_egress",
            "daemon client terminal egress observed 1 bounded write failure(s)",
        );
        let value = serde_json::to_value(&diagnostic).expect("diagnostic serializes");

        assert_eq!(value["kind"], "backpressure");
        assert!(daemon_protocol_typescript().contains("| \"backpressure\""));

        let round_tripped: DaemonDiagnostic =
            serde_json::from_value(value).expect("diagnostic deserializes");
        assert_eq!(round_tripped, diagnostic);
    }

    #[test]
    fn response_diagnostics_default_when_missing_for_backward_compatibility() {
        let response = serde_json::json!({
            "kind": "status",
            "status": {
                "lifecycle_state": "running",
                "compatibility": DaemonCompatibility::current(),
                "software": {
                    "product_id": "botster-hub",
                    "product_name": "Botster Hub",
                    "version": "0.1.0"
                },
                "installation": {
                    "mode": "development",
                    "provenance": "development_build"
                },
                "host_id": "hub",
                "host_display_name": "Hub",
                "schema_version": 1,
                "data_dir_configured": true,
                "core_initialized": true,
                "state_source": "initialized",
                "package_count": 0,
                "enabled_package_count": 0,
                "provider_count": 0,
                "enabled_provider_count": 0,
                "session_count": 0,
                "recovered_sessions": [],
                "stale_sessions": []
            },
            "sessions": [],
            "packages": [],
            "package_decision": null,
            "lifecycle": [],
            "plugin_tools": [],
            "plugin_tool_result": null,
            "events": [],
            "cleanup": null,
            "coordination": null,
            "error": null
        });

        let response: DaemonResponse =
            serde_json::from_value(response).expect("missing diagnostics should default");

        assert!(response.diagnostics.is_empty());
        assert!(response.status.expect("status body").diagnostics.is_empty());
    }

    #[test]
    fn snapshot_pages_round_trip_opaque_binary_payloads() {
        let page = DaemonSnapshotPage {
            session_id: "session".to_string(),
            capture_id: "capture-1".to_string(),
            page: 2,
            payload: DaemonOpaqueHistoryPayload::from_bytes(&[0, 255, 1]),
        };
        let value = serde_json::to_value(&page).expect("page serializes");
        assert_eq!(
            value,
            serde_json::json!({
                "session_id": "session",
                "capture_id": "capture-1",
                "page": 2,
                "payload_base64": "AP8B",
                "payload_encoding": "base64",
                "bytes": 3
            })
        );
        let round_tripped: DaemonSnapshotPage =
            serde_json::from_value(value).expect("page deserializes");
        assert_eq!(round_tripped, page);
        assert_eq!(
            round_tripped.payload.decoded_bytes().expect("decode"),
            vec![0, 255, 1]
        );
    }

    #[test]
    fn opaque_history_rejects_invalid_base64_and_mismatched_length() {
        for value in [
            serde_json::json!({
                "session_id": "session",
                "capture_id": "capture-1",
                "page": 0,
                "payload_base64": "not base64",
                "payload_encoding": "base64",
                "bytes": 3
            }),
            serde_json::json!({
                "session_id": "session",
                "capture_id": "capture-1",
                "page": 0,
                "payload_base64": "AP8B",
                "payload_encoding": "base64",
                "bytes": 4
            }),
        ] {
            serde_json::from_value::<DaemonSnapshotPage>(value)
                .expect_err("invalid opaque history metadata must fail deserialization");
        }
    }

    #[test]
    fn capture_snapshot_and_readbacks_carry_typed_unavailable_reasons() {
        let request = DaemonRequest::ReadSnapshotPage {
            session_id: "session".to_string(),
            capture_id: "capture-1".to_string(),
            page: 3,
        };
        assert_eq!(
            serde_json::to_value(&request).expect("request serializes"),
            serde_json::json!({
                "type": "read_snapshot_page",
                "session_id": "session",
                "capture_id": "capture-1",
                "page": 3
            })
        );
        let capture = DaemonCaptureSnapshot {
            session_id: "session".to_string(),
            capture_id: String::new(),
            total_bytes: 0,
            page_bytes: SNAPSHOT_PAGE_BYTES as u32,
            pages: 0,
            rows: 24,
            cols: 80,
            unavailable: Some(HistoryUnavailableReason::Oversize),
        };
        let value = serde_json::to_value(&capture).expect("capture serializes");
        assert_eq!(value["unavailable"], "oversize");
        assert_eq!(value["page_bytes"], 262_144);
        let available = DaemonCaptureSnapshot {
            capture_id: "capture-1".to_string(),
            total_bytes: 600_000,
            pages: 3,
            unavailable: None,
            ..capture
        };
        let value = serde_json::to_value(&available).expect("capture serializes");
        assert!(value.get("unavailable").is_none());
        for reason in [
            HistoryUnavailableReason::Evicted,
            HistoryUnavailableReason::Restart,
            HistoryUnavailableReason::Oversize,
            HistoryUnavailableReason::CaptureFailed,
        ] {
            assert_eq!(
                serde_json::to_value(reason).expect("reason serializes"),
                serde_json::json!(reason.as_str())
            );
        }
        let screen = DaemonReadScreen {
            session_id: "session".to_string(),
            text: String::new(),
            unavailable: Some(HistoryUnavailableReason::Restart),
        };
        assert_eq!(
            serde_json::to_value(&screen).expect("screen serializes")["unavailable"],
            "restart"
        );
        let generated = daemon_protocol_typescript();
        assert!(generated.contains("export type HistoryUnavailableReason ="));
        assert!(generated.contains("export interface DaemonSnapshotPage"));
        assert!(generated.contains("snapshot_page?: DaemonSnapshotPage | null;"));
        assert!(generated.contains(
            r#"| { type: "read_snapshot_page"; session_id: string; capture_id: string; page: number }"#
        ));
        assert!(generated.contains("unavailable?: HistoryUnavailableReason | null;"));
    }

    #[test]
    fn readme_runtime_example_reports_current_protocol_and_conformance() {
        let readme = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../README.md"));
        assert!(
            readme.contains(&format!("protocol_version={PROTOCOL_VERSION}")),
            "README runtime example must report PROTOCOL_VERSION={PROTOCOL_VERSION}"
        );
        assert!(
            readme.contains(&format!(
                "conformance_fixture_revision={CONFORMANCE_FIXTURE_REVISION}"
            )),
            "README runtime example must report CONFORMANCE_FIXTURE_REVISION={CONFORMANCE_FIXTURE_REVISION}"
        );
    }

    #[test]
    fn protocol_nine_rejects_protocol_eight_and_pins_the_conformance_floor() {
        assert_eq!(PROTOCOL_VERSION, 9);
        assert_eq!(CONFORMANCE_FIXTURE_REVISION, 49);

        let protocol_eight = DaemonCompatibilityRequirement {
            protocol_version: 8,
            minimum_conformance_fixture_revision: 48,
            ..DaemonCompatibilityRequirement::current()
        };
        let error = ensure_compatible(&protocol_eight, &DaemonCompatibility::current())
            .expect_err("protocol-8 client must fail closed against protocol 9");
        assert!(error.diagnostic.contains("unsupported protocol version 9"));

        let hub_at_forty_eight = DaemonCompatibility {
            conformance_fixture_revision: 48,
            ..DaemonCompatibility::current()
        };
        ensure_compatible(
            &DaemonCompatibilityRequirement::current(),
            &hub_at_forty_eight,
        )
        .expect_err("a protocol-9 client rejects a revision-48 Hub");
    }

    #[test]
    fn terminal_stream_events_are_absent_from_host_events() {
        for legacy in [
            "terminal_output",
            "snapshot",
            "scrollback",
            "process_exit",
            "attach_state",
        ] {
            let value = serde_json::json!({
                "type": legacy,
                "session_id": "session",
                "subscription_id": "subscription",
                "payload_base64": "AP8B",
                "payload_encoding": "base64",
                "bytes": 3,
                "code": 0,
                "state": "attached"
            });
            serde_json::from_value::<DaemonEvent>(value)
                .expect_err("terminal stream kinds are not host events");
        }
        let generated = daemon_protocol_typescript();
        assert!(!generated.contains("\"terminal_output\""));
        assert!(!generated.contains("\"attach_state\""));
        assert!(!generated.contains("DaemonUnixTerminalEnvelope"));
        assert!(generated.contains("export type ClientFrame ="));
        assert!(generated.contains("export type ServerFrame ="));
        assert!(generated.contains("export type DaemonCloseReason ="));
        assert!(generated.contains("export type DaemonProtocolErrorCode ="));
        assert!(
            generated
                .contains("| { frame: \"request\"; request_id: string; request: DaemonRequest }")
        );
        assert!(
            generated.contains(
                "| { frame: \"response\"; request_id: string; response: DaemonResponse }"
            )
        );
        assert!(generated.contains("export const PROTOCOL_VERSION = 9;"));
        assert!(generated.contains("export const MAX_OUTSTANDING_REQUESTS = 32;"));
    }

    #[test]
    fn daemon_package_runnable_entrypoints_are_serde_stable() {
        let legacy = serde_json::json!({
            "package_name": "legacy.plugin",
            "version": "1.0.0",
            "classification": "plugin",
            "state": "enabled",
            "requested_capabilities": [],
            "provider_profile_admitted": false
        });
        let package: DaemonPackage =
            serde_json::from_value(legacy).expect("legacy package should deserialize");
        assert!(package.runnable_entrypoints.is_empty());

        let current = serde_json::json!({
            "package_name": "workflow.plugin",
            "version": "1.0.0",
            "classification": "plugin",
            "state": "enabled",
            "requested_capabilities": [],
            "runnable_entrypoints": [{
                "id": "web",
                "kind": "web_app",
                "command": "bin/botster-web",
                "args": ["--host", "127.0.0.1"],
                "working_directory": { "policy": "package_root", "path": null },
                "environment": [{
                    "name": "BOTSTER_WEB_PORT",
                    "required": false,
                    "default": "5173",
                    "description": "Local web client port"
                }],
                "launch_mode": "background",
                "capabilities": [{ "surface": "Network", "scope": "localhost" }],
                "may_supervise": true,
                "process": {
                    "state": "running",
                    "pid": 1234,
                    "started_at": 1781060000,
                    "exit_status": "none",
                    "diagnostics": []
                }
            }],
            "provider_profile_admitted": false
        });
        let package: DaemonPackage =
            serde_json::from_value(current).expect("current package should deserialize");
        let entrypoint = &package.runnable_entrypoints[0];

        assert_eq!(entrypoint.id, "web");
        assert_eq!(entrypoint.args, ["--host", "127.0.0.1"]);
        assert_eq!(entrypoint.environment[0].default.as_deref(), Some("5173"));
        assert!(entrypoint.may_supervise);
        assert_eq!(entrypoint.process.state, "running");
        assert_eq!(entrypoint.process.pid, Some(1234));
        assert_eq!(entrypoint.process.started_at, Some(1781060000));
        assert_eq!(entrypoint.process.exited_at, None);
        assert_eq!(entrypoint.process.exit_status.as_deref(), Some("none"));
    }

    #[test]
    fn daemon_package_configuration_is_serde_stable_and_redacted() {
        let request = DaemonRequest::SetPackageConfiguration {
            package_name: "workflow.plugin".to_string(),
            values: BTreeMap::from([
                (
                    "endpoint".to_string(),
                    serde_json::json!({"type":"url","value":"https://example.invalid/hook"}),
                ),
                (
                    "api_token".to_string(),
                    serde_json::json!({"type":"secret","state":"write_only"}),
                ),
            ]),
        };
        assert_eq!(
            serde_json::to_value(&request).expect("serialize set configuration request"),
            serde_json::json!({
                "type": "set_package_configuration",
                "package_name": "workflow.plugin",
                "values": {
                    "api_token": { "type": "secret", "state": "write_only" },
                    "endpoint": { "type": "url", "value": "https://example.invalid/hook" }
                }
            })
        );

        let package: DaemonPackage = serde_json::from_value(serde_json::json!({
            "package_name": "workflow.plugin",
            "version": "1.0.0",
            "classification": "plugin",
            "state": "installed",
            "requested_capabilities": [],
            "runnable_entrypoints": [],
            "configuration": {
                "schema": {
                    "fields": [
                        { "key": "api_token", "type": "secret", "label": "API token", "required": true }
                    ]
                },
                "effective_values": {
                    "api_token": { "type": "secret", "state": "redacted" }
                },
                "missing_required": [],
                "diagnostics": []
            },
            "provider_profile_admitted": false
        }))
        .expect("package configuration row deserializes");

        assert_eq!(
            package.configuration.effective_values["api_token"],
            serde_json::json!({"type":"secret","state":"redacted"})
        );
        let row_json = serde_json::to_string(&package).expect("serialize package row");
        assert!(!row_json.contains("write_only"));
        assert!(!row_json.contains("super-secret-token"));
    }

    #[test]
    fn package_entrypoint_lifecycle_request_is_serde_stable() {
        let request = DaemonRequest::StartPackageEntrypoint {
            package_name: "workflow.plugin".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        };
        let value = serde_json::to_value(&request).expect("serialize request");
        assert_eq!(
            value,
            serde_json::json!({
                "type": "start_package_entrypoint",
                "package_name": "workflow.plugin",
                "entrypoint_id": "web"
            })
        );

        let old_request: DaemonRequest = serde_json::from_value(serde_json::json!({
            "type": "start_package_entrypoint",
            "package_name": "workflow.plugin",
            "entrypoint_id": "web"
        }))
        .expect("deserialize old start entrypoint request");
        assert_eq!(old_request, request);

        let request = DaemonRequest::StartPackageEntrypoint {
            package_name: "workflow.plugin".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_HUB_SOCKET".to_string(),
                "/tmp/botster-hub.sock".to_string(),
            )]),
        };
        let value = serde_json::to_value(request).expect("serialize request with env");
        assert_eq!(
            value,
            serde_json::json!({
                "type": "start_package_entrypoint",
                "package_name": "workflow.plugin",
                "entrypoint_id": "web",
                "environment_overrides": {
                    "BOTSTER_HUB_SOCKET": "/tmp/botster-hub.sock"
                }
            })
        );
    }

    #[test]
    fn issue_local_webrtc_bootstrap_request_is_serde_stable() {
        let request = DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            origin: "http://127.0.0.1:41739".to_string(),
        };
        let value = serde_json::to_value(&request).expect("serialize request");
        assert_eq!(
            value,
            serde_json::json!({
                "type": "issue_local_webrtc_bootstrap",
                "package_name": "botster-web",
                "entrypoint_id": "web-client",
                "origin": "http://127.0.0.1:41739"
            })
        );
        let round_tripped: DaemonRequest =
            serde_json::from_value(value).expect("deserialize bootstrap issuance request");
        assert_eq!(round_tripped, request);
    }

    #[test]
    fn retired_drain_json_request_does_not_deserialize() {
        let err = serde_json::from_str::<DaemonRequest>(
            r#"{"type":"drain","session_id":"missing-session"}"#,
        );
        assert!(
            err.is_err(),
            "retired drain JSON must not deserialize: {err:?}"
        );
    }

    #[test]
    fn generated_typescript_protocol_matches_checked_artifact() {
        let generated = daemon_protocol_typescript();
        let checked = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/generated/daemon-protocol.ts"
        ))
        .expect("checked generated protocol artifact is readable");

        assert_eq!(generated, checked);
    }

    #[test]
    fn spawn_target_base_ref_update_distinguishes_omit_set_and_clear() {
        let omitted: DaemonRequest = serde_json::from_value(serde_json::json!({
            "type": "update_spawn_target",
            "target_id": "tgt_example"
        }))
        .expect("deserialize omitted base ref");
        assert!(matches!(
            omitted,
            DaemonRequest::UpdateSpawnTarget { base_ref: None, .. }
        ));

        let set: DaemonRequest = serde_json::from_value(serde_json::json!({
            "type": "update_spawn_target",
            "target_id": "tgt_example",
            "base_ref": "main"
        }))
        .expect("deserialize set base ref");
        assert!(matches!(
            set,
            DaemonRequest::UpdateSpawnTarget {
                base_ref: Some(Some(ref value)),
                ..
            } if value == "main"
        ));

        let clear: DaemonRequest = serde_json::from_value(serde_json::json!({
            "type": "update_spawn_target",
            "target_id": "tgt_example",
            "base_ref": null
        }))
        .expect("deserialize cleared base ref");
        assert!(matches!(
            clear,
            DaemonRequest::UpdateSpawnTarget {
                base_ref: Some(None),
                ..
            }
        ));
        assert_eq!(
            serde_json::to_value(clear).expect("serialize cleared base ref")["base_ref"],
            serde_json::Value::Null
        );

        let legacy_target: DaemonSpawnTarget = serde_json::from_value(serde_json::json!({
            "target_id": "legacy",
            "label": "Legacy",
            "root": "/tmp/example",
            "enabled": true,
            "kind": "directory"
        }))
        .expect("deserialize legacy spawn target");
        assert_eq!(legacy_target.base_ref, None);
        let legacy_worktree: DaemonWorktree = serde_json::from_value(serde_json::json!({
            "worktree_id": "legacy",
            "target_id": "legacy",
            "label": "Legacy",
            "path": "/tmp/example",
            "status": "present"
        }))
        .expect("deserialize legacy worktree");
        assert_eq!(legacy_worktree.management, "registered");
    }

    #[test]
    fn mode_flags_protocol_is_serde_stable_and_generated() {
        let request = DaemonRequest::ReadModeFlags {
            session_id: "mode-session".to_string(),
        };
        assert_eq!(
            serde_json::to_value(&request).expect("mode request serializes"),
            serde_json::json!({
                "type": "read_mode_flags",
                "session_id": "mode-session",
            })
        );

        let response = DaemonResponse {
            kind: DaemonResponseKind::ReadModeFlags,
            mode_flags: Some(DaemonModeFlags::new(
                "mode-session",
                false,
                true,
                false,
                9,
                false,
                false,
                false,
                40,
                120,
                None,
            )),
            ..daemon_response_example(DaemonResponseKind::ReadModeFlags)
        };
        let value = serde_json::to_value(response).expect("mode response serializes");
        assert_eq!(
            value["mode_flags"],
            serde_json::json!({
                "session_id": "mode-session",
                "kitty_enabled": false,
                "cursor_visible": true,
                "bracketed_paste": false,
                "mouse_mode": 9,
                "alt_screen": false,
                "focus_reporting": false,
                "application_cursor": false,
                "rows": 40,
                "cols": 120,
            })
        );

        let generated = daemon_protocol_typescript();
        assert!(generated.contains(r#"| { type: "read_mode_flags"; session_id: string }"#));
        assert!(generated.contains("mode_flags?: DaemonModeFlags | null;"));
        assert!(!generated.contains("mode_generation"));
        assert!(!generated.contains("mode_revision"));
        assert!(!generated.contains(r#"| { type: "mode_gated_input"; session_id: string; data: string; mode_generation: number; mode_revision: number }"#));
        assert!(!generated.contains(r#"| { type: "send_input""#));
        assert!(!generated.contains(r#"| { type: "resize""#));
        assert!(!generated.contains(r#"| { type: "drain""#));
        assert!(!generated.contains("drain_session"));
        assert!(!generated.contains("drain_subscription"));
        assert!(generated.contains("terminal_reservation?: DaemonTerminalReservation | null;"));
        assert!(generated.contains("export interface DaemonTerminalReservation"));
        assert!(!generated.contains("mode_gated_input"));
        assert!(!generated.contains("send_input"));
    }

    #[test]
    fn terminal_reservation_protocol_is_serde_stable_and_generated() {
        let reservation =
            DaemonTerminalReservation::new("session", "subscription", 4, 2, "r-opaque", 30);
        let response = DaemonResponse {
            kind: DaemonResponseKind::TerminalReservation,
            terminal_reservation: Some(reservation),
            ..daemon_response_example(DaemonResponseKind::TerminalReservation)
        };
        let value = serde_json::to_value(response).expect("reservation serializes");
        assert_eq!(
            value["terminal_reservation"],
            serde_json::json!({
                "session_id": "session",
                "subscription_id": "subscription",
                "generation": 4,
                "peer_generation": 2,
                "label": "r-opaque",
                "expires_in_seconds": 30,
            })
        );
        assert_eq!(value["kind"], "terminal_reservation");
    }

    #[test]
    fn subscription_reservation_is_optional_round_trips_and_is_forward_readable() {
        let response = DaemonResponse {
            kind: DaemonResponseKind::EntitySubscribed,
            subscription_reservation: Some(DaemonSubscriptionReservation::new(
                DaemonSubscriptionReservationKind::Entity,
                "entities",
                7,
                3,
                "r-subscription",
                30,
            )),
            ..daemon_response_example(DaemonResponseKind::EntitySubscribed)
        };
        let value = serde_json::to_value(&response).expect("reservation serializes");
        assert_eq!(value["subscription_reservation"]["kind"], "entity");
        let round_trip: DaemonResponse =
            serde_json::from_value(value.clone()).expect("reservation round trips");
        assert_eq!(round_trip, response);

        let omitted = serde_json::to_value(daemon_response_example(
            DaemonResponseKind::EntitySubscribed,
        ))
        .expect("response serializes");
        assert!(omitted.get("subscription_reservation").is_none());

        #[derive(Deserialize)]
        struct PreviousResponse {
            kind: DaemonResponseKind,
        }
        let previous: PreviousResponse =
            serde_json::from_value(value).expect("older reader ignores additive field");
        assert_eq!(previous.kind, DaemonResponseKind::EntitySubscribed);

        let generated = daemon_protocol_typescript();
        assert!(
            generated.contains("subscription_reservation?: DaemonSubscriptionReservation | null;")
        );
        assert!(generated.contains("export interface DaemonSubscriptionReservation"));
        assert!(generated.contains("export type DaemonSubscriptionReservationKind ="));
    }

    #[test]
    fn plugin_worker_counters_are_optional_sanitized_and_generated() {
        let response = DaemonResponse {
            plugin_worker_counters: None,
            plugin_resource_counters: None,
            ..daemon_response_example(DaemonResponseKind::PluginLifecycle)
        };
        let value = serde_json::to_value(&response).expect("response serializes");
        assert!(value.get("plugin_worker_counters").is_none());
        assert!(value.get("plugin_resource_counters").is_none());
        let round_trip: DaemonResponse =
            serde_json::from_value(value).expect("response without counters deserializes");
        assert_eq!(round_trip.plugin_worker_counters, None);
        assert_eq!(round_trip.plugin_resource_counters, None);

        let generated = daemon_protocol_typescript();
        assert!(generated.contains("plugin_worker_counters?: DaemonPluginWorkerCounters | null;"));
        assert!(generated.contains("export interface DaemonPluginWorkerCounters"));
        assert!(
            generated.contains("plugin_resource_counters?: DaemonPluginResourceCounters | null;")
        );
        assert!(generated.contains("export interface DaemonPluginResourceCounters"));
        let populated =
            serde_json::to_value(daemon_response_example(DaemonResponseKind::PluginLifecycle))
                .expect("populated plugin lifecycle response serializes");
        assert_generated_interface_fields(
            "DaemonPluginWorkerCounters",
            &populated["plugin_worker_counters"],
        );
        for field in [
            "configured_queue_capacity",
            "configured_executor_concurrency",
            "live_plugin_executors",
            "live_executor_workers",
            "queued_jobs",
            "in_flight_jobs",
        ] {
            assert!(generated.contains(&format!("  {field}: number;")));
        }
        assert_eq!(
            populated["plugin_resource_counters"]["active_timer_resources"],
            0
        );
        assert!(generated.contains("  active_timer_resources: number;"));
    }

    #[test]
    fn generated_typescript_marks_vec_skip_diagnostics_fields_optional() {
        let hello_ack = DaemonHelloAck {
            protocol: PROTOCOL.to_string(),
            compatibility: DaemonCompatibility::current(),
            terminal_compatibility: None,
            diagnostics: Vec::new(),
        };
        assert_serde_omits_empty_diagnostics(
            "DaemonHelloAck",
            serde_json::to_value(hello_ack).expect("hello ack serializes"),
        );

        let response = DaemonResponse {
            diagnostics: Vec::new(),
            ..daemon_response_example(DaemonResponseKind::Status)
        };
        assert_serde_omits_empty_diagnostics(
            "DaemonResponse",
            serde_json::to_value(response).expect("response serializes"),
        );

        let status = DaemonStatus {
            diagnostics: Vec::new(),
            ..daemon_response_example(DaemonResponseKind::Status)
                .status
                .expect("status example")
        };
        assert_serde_omits_empty_diagnostics(
            "DaemonStatus",
            serde_json::to_value(status).expect("status serializes"),
        );

        let operator_error = DaemonOperatorError {
            diagnostics: Vec::new(),
            ..daemon_response_example(DaemonResponseKind::OperatorError)
                .error
                .expect("operator error example")
        };
        assert_serde_omits_empty_diagnostics(
            "DaemonOperatorError",
            serde_json::to_value(operator_error).expect("operator error serializes"),
        );

        let package = DaemonPackage {
            surfaces: Vec::new(),
            routes: Vec::new(),
            ..daemon_response_example(DaemonResponseKind::Packages).packages[0].clone()
        };
        let value = serde_json::to_value(package).expect("package serializes");
        assert!(
            value.get("surfaces").is_none(),
            "empty package surface descriptors should be omitted for legacy package JSON"
        );
    }

    #[test]
    fn lifecycle_counters_are_backward_compatible_sanitized_and_generated() {
        let counters = DaemonLifecycleCounters {
            accepted_connections: 3,
            live_connections: 1,
            cleanup_by_reason: BTreeMap::from([("eof".to_string(), 2)]),
            ..DaemonLifecycleCounters::default()
        };
        let value = serde_json::to_value(&counters).expect("lifecycle counters serialize");
        assert_eq!(value["accepted_connections"], 3);
        assert_eq!(value["cleanup_by_reason"]["eof"], 2);
        let debug = format!("{value:?}");
        assert!(!debug.contains("session_id"));
        assert!(!debug.contains("subscription_id"));

        let generated = daemon_protocol_typescript();
        assert!(generated.contains("lifecycle_counters?: DaemonLifecycleCounters;"));
        assert!(generated.contains("live_attach_occupancy?: DaemonAttachOccupancy[];"));
        assert!(generated.contains("export interface DaemonAttachOccupancy"));
        assert!(generated.contains("export interface DaemonLifecycleCounters"));
        assert!(generated.contains("cleanup_by_reason?: Record<string, number>;"));
    }

    #[test]
    fn observability_counters_are_optional_omitted_when_empty_and_forward_tolerant() {
        let status = daemon_response_example(DaemonResponseKind::Status)
            .status
            .expect("status example");
        let value = serde_json::to_value(&status).expect("status serializes");
        assert!(
            value.get("observability").is_none(),
            "empty observability must be omitted from the wire"
        );

        let old: DaemonStatus = serde_json::from_value(serde_json::json!({
            "lifecycle_state": "running",
            "compatibility": DaemonCompatibility::current(),
            "software": status.software,
            "installation": status.installation,
            "host_id": "hub",
            "host_display_name": "Hub",
            "schema_version": 1,
            "data_dir_configured": true,
            "core_initialized": true,
            "state_source": "initialized",
            "package_count": 0,
            "enabled_package_count": 0,
            "provider_count": 0,
            "enabled_provider_count": 0,
            "session_count": 0,
            "recovered_sessions": [],
            "stale_sessions": []
        }))
        .expect("old shaped status deserializes without observability");
        assert!(old.observability.is_empty());

        let unknown_state: DaemonQueueAgeState =
            serde_json::from_value(serde_json::json!("future_state"))
                .expect("unknown state is other");
        assert_eq!(unknown_state, DaemonQueueAgeState::Unknown);
        let unknown_kind: DaemonQueueKind =
            serde_json::from_value(serde_json::json!("future_kind"))
                .expect("unknown kind is other");
        assert_eq!(unknown_kind, DaemonQueueKind::Unknown);

        let generated = daemon_protocol_typescript();
        assert!(generated.contains("observability?: DaemonObservabilityCounters;"));
        assert!(generated.contains("oldest_age_us?: number;"));
        assert!(generated.contains("producer_generation?: number;"));
        assert!(generated.contains("queue_count?: number;"));
        assert!(generated.contains("queue_bytes?: number;"));
        assert!(generated.contains("global_in_flight_bytes?: number;"));
        assert!(generated.contains("export type DaemonQueueKind ="));
        assert!(generated.contains("export type DaemonQueueAgeState ="));
        assert!(generated.contains("| (string & {});"));
        assert_eq!(PROTOCOL_VERSION, 8);
        assert_eq!(CONFORMANCE_FIXTURE_REVISION, 48);
        assert_eq!(DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION, 36);
    }

    #[test]
    fn queue_age_observation_field_presence_matches_s6a_table() {
        fn round_trip(row: DaemonQueueAgeObservation) -> serde_json::Value {
            serde_json::to_value(&row).expect("serialize observation")
        }

        let usable = round_trip(DaemonQueueAgeObservation {
            kind: DaemonQueueKind::Producer,
            identity: "owner".to_string(),
            producer_generation: Some(3),
            state: DaemonQueueAgeState::Usable,
            oldest_age_us: Some(12),
            queue_count: Some(4),
            ..DaemonQueueAgeObservation::default()
        });
        assert_eq!(usable["state"], "usable");
        assert_eq!(usable["oldest_age_us"], 12);
        assert_eq!(usable["queue_count"], 4);
        assert_eq!(usable["producer_generation"], 3);

        let empty = round_trip(DaemonQueueAgeObservation {
            kind: DaemonQueueKind::Producer,
            identity: "owner".to_string(),
            producer_generation: Some(3),
            state: DaemonQueueAgeState::Empty,
            oldest_age_us: None,
            queue_count: Some(0),
            ..DaemonQueueAgeObservation::default()
        });
        assert_eq!(empty["state"], "empty");
        assert!(empty.get("oldest_age_us").is_none());
        assert_eq!(empty["queue_count"], 0);
        assert_eq!(empty["producer_generation"], 3);

        let indeterminate = round_trip(DaemonQueueAgeObservation {
            kind: DaemonQueueKind::Producer,
            identity: "owner".to_string(),
            producer_generation: Some(3),
            state: DaemonQueueAgeState::Indeterminate,
            oldest_age_us: None,
            queue_count: None,
            ..DaemonQueueAgeObservation::default()
        });
        assert_eq!(indeterminate["state"], "indeterminate");
        assert!(indeterminate.get("oldest_age_us").is_none());
        assert!(indeterminate.get("queue_count").is_none());
        assert_eq!(indeterminate["producer_generation"], 3);

        let missing = round_trip(DaemonQueueAgeObservation {
            kind: DaemonQueueKind::Producer,
            identity: "owner".to_string(),
            producer_generation: None,
            state: DaemonQueueAgeState::Indeterminate,
            oldest_age_us: None,
            queue_count: None,
            ..DaemonQueueAgeObservation::default()
        });
        assert!(missing.get("producer_generation").is_none());
        assert!(missing.get("queue_count").is_none());

        let consumer = round_trip(DaemonQueueAgeObservation {
            kind: DaemonQueueKind::Consumer,
            identity: "plugin".to_string(),
            producer_generation: None,
            state: DaemonQueueAgeState::Usable,
            oldest_age_us: Some(8),
            queue_count: Some(1),
            ..DaemonQueueAgeObservation::default()
        });
        assert!(consumer.get("producer_generation").is_none());
        assert_eq!(consumer["queue_count"], 1);
    }

    #[test]
    fn plugin_surface_snapshot_is_serde_stable_and_generated() {
        let surface = DaemonPluginSurface {
            package_name: "workflow.plugin".to_string(),
            surface_id: "workflow.surface".to_string(),
            body: serde_json::from_value(
                serde_json::json!({ "type": "text", "props": { "text": "surface" } }),
            )
            .expect("typed surface"),
            ui_tree_snapshot: Some(DaemonUiTreeSnapshot {
                package_name: "workflow.plugin".to_string(),
                surface_id: "workflow.surface".to_string(),
                body: serde_json::from_value(
                    serde_json::json!({ "type": "text", "props": { "text": "surface" } }),
                )
                .expect("typed snapshot"),
            }),
        };
        let value = serde_json::to_value(&surface).expect("plugin surface serializes");
        assert_generated_interface_fields("DaemonPluginSurface", &value);
        assert_generated_interface_fields(
            "DaemonUiTreeSnapshot",
            value
                .get("ui_tree_snapshot")
                .expect("plugin surface serializes ui tree snapshot"),
        );
        assert!(
            generated_interface("DaemonPluginSurface")
                .contains("  ui_tree_snapshot?: DaemonUiTreeSnapshot | null;"),
            "generated TypeScript should mark additive snapshot field optional"
        );

        let legacy_surface = DaemonPluginSurface {
            ui_tree_snapshot: None,
            ..surface
        };
        let legacy_value =
            serde_json::to_value(&legacy_surface).expect("legacy plugin surface serializes");
        assert!(
            legacy_value.get("ui_tree_snapshot").is_none(),
            "plugin surface should omit absent ui_tree_snapshot"
        );
    }

    #[test]
    fn plugin_surface_action_rejects_the_removed_split_envelope() {
        let old_shape = serde_json::json!({
            "type": "plugin_surface_action",
            "package_name": "workflow.plugin",
            "surface_id": "workflow.surface",
            "action_id": "workflow.refresh",
            "payload": { "source": "toolbar" }
        });

        assert!(
            serde_json::from_value::<DaemonRequest>(old_shape).is_err(),
            "protocol 4 must require the canonical nested UiActionRequest"
        );
    }

    #[test]
    fn daemon_packages_reference_the_canonical_ui_contract_surface_descriptor() {
        let package = DaemonPackage {
            surfaces: vec![PackageSurfaceDescriptor {
                id: "project-pipelines.home".to_string(),
                kind: botster_ui_contract::PackageSurfaceKind::App,
                title: "Project Pipelines".to_string(),
                description: Some("Pipeline workbench".to_string()),
                icon: Some("workflow".to_string()),
                order: Some(10),
                category: Some("workflows".to_string()),
                supports: vec![
                    botster_ui_contract::PackageSurfaceOperation::Render,
                    botster_ui_contract::PackageSurfaceOperation::Action,
                ],
            }],
            routes: Vec::new(),
            ..daemon_response_example(DaemonResponseKind::Packages).packages[0].clone()
        };

        let value = serde_json::to_value(package).expect("package serializes");
        assert_eq!(
            value["surfaces"][0],
            serde_json::json!({
                "id": "project-pipelines.home",
                "kind": "app",
                "title": "Project Pipelines",
                "description": "Pipeline workbench",
                "icon": "workflow",
                "order": 10,
                "category": "workflows",
                "supports": ["render", "action"]
            })
        );

        let generated = daemon_protocol_typescript();
        assert!(generated.contains("surfaces?: PackageSurfaceDescriptor[];"));
        assert!(generated.contains("PackageSurfaceDescriptor, UiActionRequest"));
        assert!(!generated.contains("export interface DaemonPackageSurfaceDescriptor"));
    }

    #[test]
    fn daemon_package_notice_reactions_are_optional_on_the_wire_and_imported() {
        let empty = DaemonPackage {
            notice_reactions: Vec::new(),
            ..daemon_response_example(DaemonResponseKind::Packages).packages[0].clone()
        };
        let empty_value = serde_json::to_value(&empty).expect("empty package serializes");
        assert!(
            empty_value.get("notice_reactions").is_none(),
            "empty notice_reactions must omit on the wire"
        );

        let package = DaemonPackage {
            notice_reactions: vec![PackageNoticeReactionDescriptor {
                owner: "event-plane-producer".to_string(),
                name: "sample.ready".to_string(),
                subject_scope: botster_ui_contract::PackageNoticeSubjectScope::Session,
                text_pointer: "/notice".to_string(),
                ttl_ms: 5_000,
                severity: botster_ui_contract::PackageNoticeSeverity::Info,
            }],
            ..daemon_response_example(DaemonResponseKind::Packages).packages[0].clone()
        };
        let value = serde_json::to_value(&package).expect("package serializes");
        assert_eq!(
            value["notice_reactions"],
            serde_json::json!([{
                "owner": "event-plane-producer",
                "name": "sample.ready",
                "subject_scope": "session",
                "text_pointer": "/notice",
                "ttl_ms": 5000,
                "severity": "info"
            }])
        );
        let round_trip: DaemonPackage =
            serde_json::from_value(value).expect("notice reactions round-trip");
        assert_eq!(round_trip.notice_reactions, package.notice_reactions);

        let generated = daemon_protocol_typescript();
        assert!(generated.contains(
            "import type { PackageNoticeReactionDescriptor, PackageSurfaceDescriptor, UiActionRequest, UiActionResult, UiNode } from \"@trybotster/ui-contract\";"
        ));
        assert!(generated.contains("notice_reactions?: PackageNoticeReactionDescriptor[];"));
        assert!(!generated.contains("export interface PackageNoticeReactionDescriptor"));
    }

    #[test]
    fn generated_typescript_exposes_package_configuration_protocol() {
        let generated = daemon_protocol_typescript();
        let package = generated_interface("DaemonPackage");
        let configuration = generated_interface("DaemonPackageConfiguration");

        assert!(
            generated.contains(
                r#"| { type: "set_package_configuration"; package_name: string; values: Record<string, JsonValue> }"#
            ),
            "generated TypeScript should include set_package_configuration request"
        );
        assert!(
            package.contains("  configuration: DaemonPackageConfiguration;"),
            "DaemonPackage.configuration is serialized by Rust and should be generated as required"
        );
        assert!(
            !package.contains("  configuration?: DaemonPackageConfiguration;"),
            "DaemonPackage.configuration should not be generated as optional"
        );
        assert!(
            configuration.contains("  schema?: JsonValue | null;"),
            "optional schema should match serde skip_serializing_if"
        );
        assert!(
            configuration.contains("  effective_values?: Record<string, JsonValue>;"),
            "optional effective_values should match serde skip_serializing_if"
        );
        assert!(
            configuration.contains("  missing_required?: string[];"),
            "optional missing_required should match serde skip_serializing_if"
        );
        assert!(
            configuration.contains("  diagnostics?: DaemonPackageDiagnostic[];"),
            "optional diagnostics should match serde skip_serializing_if"
        );
    }

    #[test]
    fn package_route_descriptors_are_serde_stable_and_generated() {
        let route = DaemonPackageRouteDescriptor {
            package_name: "workflow.plugin".to_string(),
            route_id: "surface:workflow.home".to_string(),
            route_path: "/packages/workflow.plugin/surfaces/workflow.home".to_string(),
            target: DaemonPackageRouteTarget {
                kind: "plugin_surface".to_string(),
                entrypoint_id: None,
                surface_id: Some("workflow.home".to_string()),
            },
            title: "Workflow".to_string(),
            label: "Workflow".to_string(),
            app_id: Some("workflow.home".to_string()),
            surface_id: Some("workflow.home".to_string()),
            icon: Some("workflow".to_string()),
            category: Some("workflows".to_string()),
            layout_mode: "plugin_surface".to_string(),
            required_capabilities: vec![DaemonCapability {
                surface: "Surfaces".to_string(),
                scope: None,
            }],
            enabled: true,
            blocked: false,
            diagnostics: Vec::new(),
            supports_settings: true,
        };
        let package = DaemonPackage {
            routes: vec![route.clone()],
            ..daemon_response_example(DaemonResponseKind::Packages).packages[0].clone()
        };
        let value = serde_json::to_value(package).expect("package route serializes");
        assert_eq!(
            value["routes"][0],
            serde_json::json!({
                "package_name": "workflow.plugin",
                "route_id": "surface:workflow.home",
                "route_path": "/packages/workflow.plugin/surfaces/workflow.home",
                "target": {
                    "kind": "plugin_surface",
                    "surface_id": "workflow.home"
                },
                "title": "Workflow",
                "label": "Workflow",
                "app_id": "workflow.home",
                "surface_id": "workflow.home",
                "icon": "workflow",
                "category": "workflows",
                "layout_mode": "plugin_surface",
                "required_capabilities": [{"surface": "Surfaces", "scope": null}],
                "enabled": true,
                "blocked": false,
                "supports_settings": true
            })
        );

        let request = DaemonRequest::ResolvePackageRoute {
            package_name: "workflow.plugin".to_string(),
            route_id: "surface:workflow.home".to_string(),
        };
        let request_value = serde_json::to_value(request).expect("request serializes");
        assert_eq!(
            request_value,
            serde_json::json!({
                "type": "resolve_package_route",
                "package_name": "workflow.plugin",
                "route_id": "surface:workflow.home"
            })
        );

        let generated = daemon_protocol_typescript();
        assert!(generated.contains("routes?: DaemonPackageRouteDescriptor[];"));
        assert!(generated.contains("route?: DaemonPackageRouteDescriptor | null;"));
        assert!(
            generated.contains("resolved_package_route?: DaemonPackageRouteDescriptor | null;")
        );
        assert!(generated.contains("export interface DaemonPackageRouteDescriptor"));
        assert!(
            generated.contains(
                r#"| { type: "resolve_package_route"; package_name: string; route_id: string }"#
            ),
            "generated TypeScript should include resolve_package_route request"
        );
    }

    #[test]
    fn package_navigation_entries_are_serde_stable_and_generated_without_order_authority() {
        let entry = DaemonPackageNavigationEntry {
            package_name: "workflow.plugin".to_string(),
            item_id: "home".to_string(),
            label: "Workflow".to_string(),
            icon: Some("workflow".to_string()),
            description: Some("Workflow home".to_string()),
            route_id: "surface:workflow.home".to_string(),
            route_path: "/packages/workflow.plugin/surfaces/workflow.home".to_string(),
            target: DaemonPackageRouteTarget {
                kind: "plugin_surface".to_string(),
                entrypoint_id: None,
                surface_id: Some("workflow.home".to_string()),
            },
            source: DaemonPackageNavigationSource {
                kind: "surface".to_string(),
                surface_id: Some("workflow.home".to_string()),
                entrypoint_id: None,
            },
            enabled: true,
            blocked: false,
            diagnostics: Vec::new(),
        };
        let response = DaemonResponse {
            kind: DaemonResponseKind::PackageNavigation,
            package_navigation: vec![entry],
            ..daemon_response_example(DaemonResponseKind::PackageNavigation)
        };
        let value = serde_json::to_value(response).expect("navigation response serializes");
        assert_eq!(
            value["package_navigation"][0],
            serde_json::json!({
                "package_name": "workflow.plugin",
                "item_id": "home",
                "label": "Workflow",
                "icon": "workflow",
                "description": "Workflow home",
                "route_id": "surface:workflow.home",
                "route_path": "/packages/workflow.plugin/surfaces/workflow.home",
                "target": {
                    "kind": "plugin_surface",
                    "surface_id": "workflow.home"
                },
                "source": {
                    "kind": "surface",
                    "surface_id": "workflow.home"
                },
                "enabled": true,
                "blocked": false
            })
        );
        let navigation_entry = value["package_navigation"][0].to_string();
        assert!(!navigation_entry.contains("order"));
        assert!(!navigation_entry.contains("priority"));

        let request = DaemonRequest::ListPackageNavigation;
        let request_value = serde_json::to_value(request).expect("request serializes");
        assert_eq!(
            request_value,
            serde_json::json!({ "type": "list_package_navigation" })
        );

        let generated = daemon_protocol_typescript();
        assert!(generated.contains("package_navigation?: DaemonPackageNavigationEntry[];"));
        assert!(generated.contains("export interface DaemonPackageNavigationEntry"));
        assert!(generated.contains("export interface DaemonPackageNavigationSource"));
        assert!(generated.contains(r#"| { type: "list_package_navigation" }"#));
        let navigation = generated_interface("DaemonPackageNavigationEntry");
        assert!(!navigation.contains("order"));
        assert!(!navigation.contains("priority"));
    }

    #[test]
    fn daemon_package_configuration_optional_fields_match_serde_omission() {
        let package = DaemonPackage {
            package_name: "workflow.plugin".to_string(),
            version: "1.0.0".to_string(),
            classification: "plugin".to_string(),
            source_kind: "path".to_string(),
            state: "enabled".to_string(),
            requested_capabilities: Vec::new(),
            surfaces: Vec::new(),
            notice_reactions: Vec::new(),
            routes: Vec::new(),
            runnable_entrypoints: Vec::new(),
            configuration: DaemonPackageConfiguration::default(),
            availability: DaemonPackageAvailability::default(),
            dependency_availability: Vec::new(),
            feature_availability: Vec::new(),
            actions: Vec::new(),
            provider_profile_admitted: false,
        };
        let value = serde_json::to_value(package).expect("package serializes");

        assert!(
            value.get("configuration").is_some(),
            "DaemonPackage should serialize configuration even when it is empty"
        );
        let configuration = value
            .get("configuration")
            .and_then(Value::as_object)
            .expect("configuration serializes as an object");
        assert!(
            configuration.get("schema").is_none(),
            "empty configuration should omit schema"
        );
        assert!(
            configuration.get("effective_values").is_none(),
            "empty configuration should omit effective_values"
        );
        assert!(
            configuration.get("missing_required").is_none(),
            "empty configuration should omit missing_required"
        );
        assert!(
            configuration.get("diagnostics").is_none(),
            "empty configuration should omit diagnostics"
        );
        assert!(
            value.get("actions").is_none(),
            "empty package action descriptors should omit actions for additive compatibility"
        );
    }

    #[test]
    fn daemon_package_availability_defaults_for_legacy_rows() {
        let package: DaemonPackage = serde_json::from_value(serde_json::json!({
            "package_name": "legacy.plugin",
            "version": "1.0.0",
            "classification": "plugin",
            "state": "enabled",
            "requested_capabilities": [],
            "runnable_entrypoints": [],
            "configuration": {},
            "provider_profile_admitted": false
        }))
        .expect("legacy package row without availability should deserialize");

        assert_eq!(
            package.availability.state,
            DaemonPackageAvailabilityState::Available
        );
        assert!(package.availability.reasons.is_empty());
        assert!(package.dependency_availability.is_empty());
        assert!(package.feature_availability.is_empty());
    }

    #[test]
    fn daemon_request_variants_are_serde_stable_and_generated() {
        for request in daemon_request_examples() {
            let expected_tag = daemon_request_tag(&request);
            let value = serde_json::to_value(&request).expect("request serializes");

            assert_eq!(value["type"], expected_tag);
            assert_generated_union_variant_fields("DaemonRequest", "type", expected_tag, &value);

            let round_tripped: DaemonRequest =
                serde_json::from_value(value).expect("request deserializes");
            assert_eq!(round_tripped, request);
        }
    }

    #[test]
    fn daemon_response_kinds_are_serde_stable_and_generated() {
        for kind in daemon_response_kind_examples() {
            let expected_kind = daemon_response_kind_tag(kind);
            let response = daemon_response_example(kind);
            let value = serde_json::to_value(&response).expect("response serializes");

            assert_eq!(value["kind"], expected_kind);
            assert!(
                daemon_protocol_typescript().contains(&format!("\"{expected_kind}\"")),
                "generated TypeScript should include response kind {expected_kind}"
            );
            assert_generated_interface_fields("DaemonResponse", &value);

            let round_tripped: DaemonResponse =
                serde_json::from_value(value).expect("response deserializes");
            assert_eq!(round_tripped, response);
        }
    }

    #[test]
    fn daemon_event_variants_are_serde_stable_and_generated() {
        for event in daemon_event_examples() {
            let expected_tag = daemon_event_tag(&event);
            let value = serde_json::to_value(&event).expect("event serializes");

            assert_eq!(value["type"], expected_tag);
            assert_generated_union_variant_fields("DaemonEvent", "type", expected_tag, &value);

            let round_tripped: DaemonEvent =
                serde_json::from_value(value).expect("event deserializes");
            assert_eq!(round_tripped, event);
        }
    }

    #[test]
    fn generated_typescript_local_webrtc_fields_match_serde_json() {
        let response = daemon_response_example(DaemonResponseKind::LocalWebrtcBootstrap);
        let value = serde_json::to_value(response).expect("response serializes");

        assert_generated_interface_fields(
            "DaemonLocalWebrtcBootstrap",
            &value["local_webrtc_bootstrap"],
        );
        assert!(
            value["local_webrtc_bootstrap"]
                .get("max_retransmits")
                .is_none(),
            "empty bootstrap max_retransmits should be omitted in serde JSON"
        );
        assert!(
            generated_interface("DaemonLocalWebrtcBootstrap")
                .contains("  max_retransmits?: number | null;"),
            "generated TypeScript should mark omitted max_retransmits optional"
        );
        assert_generated_interface_field_type(
            "DaemonLocalWebrtcBootstrap",
            "max_retransmits",
            "number | null",
        );
        assert!(
            value["local_webrtc_bootstrap"]
                .get("max_packet_lifetime_ms")
                .is_none(),
            "empty bootstrap max_packet_lifetime_ms should be omitted in serde JSON"
        );
        assert!(
            generated_interface("DaemonLocalWebrtcBootstrap")
                .contains("  max_packet_lifetime_ms?: number | null;"),
            "generated TypeScript should mark omitted max_packet_lifetime_ms optional"
        );
        assert_generated_interface_field_type(
            "DaemonLocalWebrtcBootstrap",
            "max_packet_lifetime_ms",
            "number | null",
        );
        assert_generated_interface_fields("DaemonLocalWebrtcAnswer", &value["local_webrtc_answer"]);
        assert_generated_interface_field_type(
            "DaemonLocalWebrtcAnswer",
            "diagnostics",
            "DaemonDiagnostic[]",
        );
    }

    #[test]
    fn generated_interface_helper_rejects_extra_required_typescript_field() {
        let value = serde_json::json!({ "grant_id": "grant-1" });
        let interface =
            "export interface TestDto {\n  grant_id: string;\n  stale_required: string;\n}\n";

        let result = std::panic::catch_unwind(|| {
            assert_interface_fields("TestDto", interface, &value);
        });

        assert!(
            result.is_err(),
            "helper should fail when generated TypeScript has a required field absent from serde"
        );
    }

    #[test]
    fn generated_interface_helper_allows_absent_optional_typescript_field() {
        let value = serde_json::json!({ "grant_id": "grant-1" });
        let interface = "export interface TestDto {\n  grant_id: string;\n  omitted_optional?: string | null;\n}\n";

        assert_interface_fields("TestDto", interface, &value);
    }

    #[test]
    fn generated_interface_helper_rejects_changed_typescript_field_type() {
        let interface = "export interface TestDto {\n  expires_at: string;\n}\n";

        let result = std::panic::catch_unwind(|| {
            assert_interface_field_type("TestDto", interface, "expires_at", "number");
        });

        assert!(
            result.is_err(),
            "helper should fail when a generated TypeScript field has the wrong obvious type"
        );
    }

    fn assert_serde_omits_empty_diagnostics(type_name: &str, value: Value) {
        assert!(
            value.get("diagnostics").is_none(),
            "{type_name} should omit empty diagnostics in serde JSON"
        );
        assert!(
            generated_interface(type_name).contains("  diagnostics?: DaemonDiagnostic[];"),
            "generated TypeScript should include {type_name}"
        );
    }

    fn assert_generated_interface_fields(type_name: &str, value: &Value) {
        let interface = generated_interface(type_name);
        assert_interface_fields(type_name, &interface, value);
    }

    fn assert_interface_fields(type_name: &str, interface: &str, value: &Value) {
        let object = value
            .as_object()
            .unwrap_or_else(|| panic!("{type_name} serde example should be an object"));
        let fields = parse_interface_fields(type_name, interface);

        for key in object.keys() {
            assert!(
                fields.contains_key(key),
                "generated TypeScript {type_name} should include serde field {key}"
            );
        }

        for (field_name, field) in fields {
            if field.optional {
                continue;
            }

            assert!(
                object.contains_key(&field_name),
                "generated TypeScript {type_name} required field {field_name} should be present in serde example"
            );
        }
    }

    fn assert_generated_interface_field_type(
        type_name: &str,
        field_name: &str,
        expected_ts_type: &str,
    ) {
        let interface = generated_interface(type_name);
        assert_interface_field_type(type_name, &interface, field_name, expected_ts_type);
    }

    fn assert_interface_field_type(
        type_name: &str,
        interface: &str,
        field_name: &str,
        expected_ts_type: &str,
    ) {
        let fields = parse_interface_fields(type_name, interface);
        let field = fields.get(field_name).unwrap_or_else(|| {
            panic!("generated TypeScript {type_name} should include field {field_name}")
        });

        assert_eq!(
            field.ts_type, expected_ts_type,
            "generated TypeScript {type_name}.{field_name} should have expected type"
        );
    }

    #[derive(Debug)]
    struct TypeScriptInterfaceField {
        optional: bool,
        ts_type: String,
    }

    fn parse_interface_fields(
        type_name: &str,
        interface: &str,
    ) -> BTreeMap<String, TypeScriptInterfaceField> {
        let mut fields = BTreeMap::new();

        for line in interface.lines() {
            let Some(field_line) = line.strip_prefix("  ") else {
                continue;
            };
            let Some(field_line) = field_line.strip_suffix(';') else {
                continue;
            };
            let Some((field_name, ts_type)) = field_line.split_once(": ") else {
                continue;
            };
            let (field_name, optional) = match field_name.strip_suffix('?') {
                Some(field_name) => (field_name, true),
                None => (field_name, false),
            };

            fields.insert(
                field_name.to_string(),
                TypeScriptInterfaceField {
                    optional,
                    ts_type: ts_type.to_string(),
                },
            );
        }

        assert!(
            !fields.is_empty(),
            "generated TypeScript interface should expose parseable fields for {type_name}"
        );

        fields
    }

    #[test]
    fn session_type_tagged_unions_are_serde_stable_and_generated() {
        let sources = [
            DaemonSessionTypeMutationSource::Device,
            DaemonSessionTypeMutationSource::Repo {
                target_id: "target-1".to_string(),
            },
            DaemonSessionTypeMutationSource::Package {
                package_name: "botster.example".to_string(),
            },
        ];
        for source in sources {
            let value = serde_json::to_value(&source).expect("mutation source serializes");
            let tag = value["source"]
                .as_str()
                .expect("mutation source has source discriminator");
            assert_generated_union_variant_fields(
                "DaemonSessionTypeMutationSource",
                "source",
                tag,
                &value,
            );
            assert_eq!(
                serde_json::from_value::<DaemonSessionTypeMutationSource>(value)
                    .expect("mutation source deserializes"),
                source
            );
        }

        let policies = [
            DaemonSessionTypeWorkingDirectory::PackageRoot,
            DaemonSessionTypeWorkingDirectory::Relative {
                path: "subdir".to_string(),
            },
        ];
        for policy in policies {
            let value = serde_json::to_value(&policy).expect("working directory serializes");
            let tag = value["policy"]
                .as_str()
                .expect("working directory has policy discriminator");
            assert_generated_union_variant_fields(
                "DaemonSessionTypeWorkingDirectory",
                "policy",
                tag,
                &value,
            );
            assert_eq!(
                serde_json::from_value::<DaemonSessionTypeWorkingDirectory>(value)
                    .expect("working directory deserializes"),
                policy
            );
        }

        let executions = [
            DaemonSessionTypeExecution::RelativeExecutable,
            DaemonSessionTypeExecution::ShellCommand,
        ];
        for execution in executions {
            let value = serde_json::to_value(&execution).expect("execution serializes");
            let tag = value["mode"]
                .as_str()
                .expect("execution has mode discriminator");
            assert_generated_union_variant_fields(
                "DaemonSessionTypeExecution",
                "mode",
                tag,
                &value,
            );
            assert_eq!(
                serde_json::from_value::<DaemonSessionTypeExecution>(value)
                    .expect("execution deserializes"),
                execution
            );
        }

        let defaulted: DaemonSessionTypeDefinition = serde_json::from_value(serde_json::json!({
            "id": "init",
            "label": "Init",
            "role": "botster.agent",
            "interaction": "interactive",
            "lifecycle": "task",
            "command": "bin/init"
        }))
        .expect("definition without execution uses the compatible default");
        assert_eq!(
            defaulted.execution,
            DaemonSessionTypeExecution::RelativeExecutable
        );
    }

    fn assert_generated_union_variant_fields(
        union_name: &str,
        discriminator: &str,
        tag: &str,
        value: &Value,
    ) {
        let object = value
            .as_object()
            .unwrap_or_else(|| panic!("{union_name}::{tag} serde example should be an object"));
        let variant = generated_union_variant(union_name, discriminator, tag);
        for key in object.keys() {
            assert!(
                variant.contains(&format!("; {key}:"))
                    || variant.contains(&format!("; {key}?:"))
                    || variant.contains(&format!("{{ {key}:"))
                    || variant.contains(&format!("{{ {key}?:")),
                "generated TypeScript {union_name} variant {tag} should include serde field {key}"
            );
        }
    }

    fn generated_interface(type_name: &str) -> String {
        let generated = daemon_protocol_typescript();
        let start = generated
            .find(&format!("export interface {type_name} {{"))
            .unwrap_or_else(|| panic!("generated TypeScript should include {type_name}"));
        let rest = &generated[start..];
        let end = rest
            .find("\n}\n")
            .unwrap_or_else(|| panic!("generated TypeScript interface should close {type_name}"));
        rest[..end + 3].to_string()
    }

    fn generated_union_variant(union_name: &str, discriminator: &str, tag: &str) -> String {
        let generated = daemon_protocol_typescript();
        let start = generated
            .find(&format!("export type {union_name} ="))
            .unwrap_or_else(|| panic!("generated TypeScript should include {union_name}"));
        generated[start..]
            .lines()
            .take_while(|line| !line.trim_end().ends_with(';'))
            .chain(
                generated[start..]
                    .lines()
                    .find(|line| line.trim_end().ends_with(';')),
            )
            .find(|line| line.contains(&format!("{discriminator}: \"{tag}\"")))
            .unwrap_or_else(|| {
                panic!("generated TypeScript {union_name} should include variant {tag}")
            })
            .to_string()
    }

    fn daemon_session_type_definition_example() -> DaemonSessionTypeDefinition {
        DaemonSessionTypeDefinition {
            id: "init".to_string(),
            label: "Workflow agent".to_string(),
            description: Some("Interactive workflow agent".to_string()),
            icon: Some("terminal".to_string()),
            role: "botster.agent".to_string(),
            interaction: "interactive".to_string(),
            traits: vec!["pipeline-step".to_string()],
            lifecycle: "task".to_string(),
            execution: DaemonSessionTypeExecution::RelativeExecutable,
            command: "bin/init".to_string(),
            args: vec!["--json".to_string()],
            working_directory: DaemonSessionTypeWorkingDirectory::PackageRoot,
            environment: BTreeMap::new(),
            allowed_environment_overrides: vec!["BOTSTER_MODE".to_string()],
            context: vec!["prompt".to_string()],
            target_id: None,
        }
    }

    fn daemon_session_type_example() -> DaemonSessionType {
        DaemonSessionType {
            session_type_id: "workflow.plugin/init".to_string(),
            source_name: "workflow.plugin".to_string(),
            id: "init".to_string(),
            source: "package".to_string(),
            editable: false,
            overridden_sources: Vec::new(),
            diagnostics: Vec::new(),
            label: "Workflow agent".to_string(),
            description: Some("Interactive workflow agent".to_string()),
            icon: Some("terminal".to_string()),
            role: "botster.agent".to_string(),
            interaction: "interactive".to_string(),
            traits: vec!["pipeline-step".to_string()],
            lifecycle: "task".to_string(),
            execution: DaemonSessionTypeExecution::RelativeExecutable,
            command: "bin/init".to_string(),
            args: vec!["--json".to_string()],
            working_directory_policy: "package_root".to_string(),
            allowed_environment_overrides: vec!["BOTSTER_MODE".to_string()],
            context_keys: vec!["prompt".to_string()],
            target_id: "package:workflow.plugin".to_string(),
            available: true,
        }
    }

    fn daemon_request_examples() -> Vec<DaemonRequest> {
        vec![
            DaemonRequest::Status,
            DaemonRequest::CheckHubUpdate,
            DaemonRequest::StartHubUpdate {
                scope: DaemonHubUpdateScope::All,
            },
            DaemonRequest::GetHubUpdateExecution,
            DaemonRequest::ListSessions,
            DaemonRequest::SubscribeEntities {
                entity_type: "session".to_string(),
                subscription_id: "entities".to_string(),
            },
            DaemonRequest::UnsubscribeEntities {
                subscription_id: "entities".to_string(),
            },
            DaemonRequest::SubscribeEvents {
                subscription_id: "events".to_string(),
                owner: "event-plane-producer".to_string(),
                name: "sample.ready".to_string(),
                subjects: Vec::new(),
            },
            DaemonRequest::UnsubscribeEvents {
                subscription_id: "events".to_string(),
            },
            DaemonRequest::RemoveSession {
                session_id: "session".to_string(),
            },
            DaemonRequest::Whoami {
                caller_session_id: Some("caller".to_string()),
            },
            DaemonRequest::PostMessage {
                caller_session_id: Some("caller".to_string()),
                target_session_id: "target".to_string(),
                envelope_id: Some("envelope".to_string()),
                body: "hello".to_string(),
            },
            DaemonRequest::ReceiveMessages {
                caller_session_id: "caller".to_string(),
                after: Some(1),
                limit: 10,
            },
            DaemonRequest::AckMessage {
                caller_session_id: "caller".to_string(),
                envelope_id: "envelope".to_string(),
            },
            DaemonRequest::NotifySession {
                session_id: "session".to_string(),
                data: "ready".to_string(),
            },
            DaemonRequest::Spawn {
                session_id: "session".to_string(),
                command: "echo hello".to_string(),
            },
            DaemonRequest::Attach {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            },
            DaemonRequest::Detach {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            },
            DaemonRequest::ShutdownSession {
                session_id: "session".to_string(),
            },
            DaemonRequest::ReadScreen {
                session_id: "session".to_string(),
            },
            DaemonRequest::ReadModeFlags {
                session_id: "session".to_string(),
            },
            DaemonRequest::CaptureSnapshot {
                session_id: "session".to_string(),
            },
            DaemonRequest::ReadSnapshotPage {
                session_id: "session".to_string(),
                capture_id: "capture-1".to_string(),
                page: 0,
            },
            DaemonRequest::ListSessionTypes,
            DaemonRequest::ListSessionTypesForTarget {
                target_id: "repo:main".to_string(),
            },
            DaemonRequest::ShowSessionType {
                session_type_id: "init".to_string(),
            },
            DaemonRequest::ShowSessionTypeDefinition {
                session_type_id: "init".to_string(),
            },
            DaemonRequest::CreateSessionType {
                source: DaemonSessionTypeMutationSource::Device,
                definition: daemon_session_type_definition_example(),
            },
            DaemonRequest::UpdateSessionType {
                source: DaemonSessionTypeMutationSource::Repo {
                    target_id: "repo:main".to_string(),
                },
                definition: daemon_session_type_definition_example(),
            },
            DaemonRequest::DeleteSessionType {
                source: DaemonSessionTypeMutationSource::Device,
                session_type_id: "init".to_string(),
            },
            DaemonRequest::ResolveSessionType {
                session_type_id: "init".to_string(),
                request: DaemonSessionTypeRequest::default(),
            },
            DaemonRequest::SpawnSessionType {
                session_type_id: "init".to_string(),
                session_id: "session".to_string(),
                request: DaemonSessionTypeRequest::default(),
            },
            DaemonRequest::ReadSessionContext {
                session_id: "session".to_string(),
                context_id: Some("ctx-session".to_string()),
                key: Some("prompt".to_string()),
            },
            DaemonRequest::ListSpawnTargets,
            DaemonRequest::ShowSpawnTarget {
                target_id: "tgt_example".to_string(),
            },
            DaemonRequest::CreateSpawnTarget {
                target_id: Some("tgt_example".to_string()),
                label: Some("Example".to_string()),
                root: PathBuf::from("/tmp/example"),
                enabled: true,
                kind: Some("directory".to_string()),
                base_ref: None,
                metadata: BTreeMap::from([("purpose".to_string(), "test".to_string())]),
            },
            DaemonRequest::UpdateSpawnTarget {
                target_id: "tgt_example".to_string(),
                label: Some("Example Updated".to_string()),
                root: Some(PathBuf::from("/tmp/example-updated")),
                enabled: Some(false),
                kind: Some("directory".to_string()),
                base_ref: None,
                metadata: Some(BTreeMap::new()),
            },
            DaemonRequest::DeleteSpawnTarget {
                target_id: "tgt_example".to_string(),
            },
            DaemonRequest::ValidateSpawnTarget {
                target_id: "tgt_example".to_string(),
            },
            DaemonRequest::ListWorktrees,
            DaemonRequest::ShowWorktree {
                worktree_id: "wt_example".to_string(),
            },
            DaemonRequest::CreateWorktree {
                worktree_id: Some("wt_example".to_string()),
                target_id: "tgt_example".to_string(),
                label: Some("Example Worktree".to_string()),
                path: PathBuf::from("/tmp/example/worktree"),
                metadata: BTreeMap::from([("purpose".to_string(), "test".to_string())]),
            },
            DaemonRequest::DeleteWorktree {
                worktree_id: "wt_example".to_string(),
            },
            DaemonRequest::ListApps,
            DaemonRequest::ResolveAppLaunch {
                package_name: "workflow.plugin".to_string(),
                entrypoint_id: "terminal".to_string(),
            },
            DaemonRequest::ResolvePackageRoute {
                package_name: "workflow.plugin".to_string(),
                route_id: "surface:workflow.home".to_string(),
            },
            DaemonRequest::ListPackageNavigation,
            DaemonRequest::ListPackages,
            DaemonRequest::ListAvailablePackages {
                registry_path: PathBuf::from("/tmp/registry"),
            },
            DaemonRequest::InspectAvailablePackage {
                registry_path: PathBuf::from("/tmp/registry"),
                entry_id: "workflow-plugin".to_string(),
            },
            DaemonRequest::PreviewPackageInstall {
                registry_path: PathBuf::from("/tmp/registry"),
                entry_id: "workflow-plugin".to_string(),
            },
            DaemonRequest::InstallPackageRegistryEntry {
                registry_path: PathBuf::from("/tmp/registry"),
                entry_id: "workflow-plugin".to_string(),
            },
            DaemonRequest::InstallPackageLocalPath {
                path: PathBuf::from("/tmp/plugin"),
            },
            DaemonRequest::ShowPackage {
                package_name: "workflow.plugin".to_string(),
            },
            DaemonRequest::SetPackageConfiguration {
                package_name: "workflow.plugin".to_string(),
                values: BTreeMap::from([(
                    "endpoint".to_string(),
                    serde_json::json!({"type":"url","value":"https://example.invalid/hook"}),
                )]),
            },
            DaemonRequest::ReloadPackage {
                package_name: "workflow.plugin".to_string(),
            },
            DaemonRequest::RefreshLocalPackages,
            DaemonRequest::EnablePackageLocalPath {
                path: PathBuf::from("/tmp/plugin"),
            },
            DaemonRequest::EnablePackage {
                package_name: "workflow.plugin".to_string(),
            },
            DaemonRequest::DisablePackage {
                package_name: "workflow.plugin".to_string(),
            },
            DaemonRequest::RemovePackage {
                package_name: "workflow.plugin".to_string(),
            },
            DaemonRequest::StartPackageEntrypoint {
                package_name: "workflow.plugin".to_string(),
                entrypoint_id: "web".to_string(),
                environment_overrides: BTreeMap::from([(
                    "BOTSTER_HUB_SOCKET".to_string(),
                    "/tmp/botster.sock".to_string(),
                )]),
            },
            DaemonRequest::IssueLocalWebrtcBootstrap {
                package_name: "botster-web".to_string(),
                entrypoint_id: "web-client".to_string(),
                origin: "http://127.0.0.1:49152".to_string(),
            },
            DaemonRequest::LocalWebrtcSignal {
                grant_id: "grant".to_string(),
                grant_secret: "secret".to_string(),
                origin: "http://127.0.0.1:49152".to_string(),
                offer: serde_json::json!({
                    "type": "offer",
                    "sdp": "v=0\r\n"
                }),
            },
            DaemonRequest::StopPackageEntrypoint {
                package_name: "workflow.plugin".to_string(),
                entrypoint_id: "web".to_string(),
            },
            DaemonRequest::RestartPackageEntrypoint {
                package_name: "workflow.plugin".to_string(),
                entrypoint_id: "web".to_string(),
            },
            DaemonRequest::PackageEntrypointStatus {
                package_name: "workflow.plugin".to_string(),
                entrypoint_id: "web".to_string(),
            },
            DaemonRequest::PluginLifecycleStatus,
            DaemonRequest::PluginMcpListTools,
            DaemonRequest::PluginMcpCallTool {
                name: "tool".to_string(),
                arguments: serde_json::json!({ "value": true }),
            },
            DaemonRequest::PluginSurfaceRender {
                package_name: "workflow.plugin".to_string(),
                surface_id: "home".to_string(),
                payload: serde_json::json!({ "route": "/" }),
            },
            DaemonRequest::PluginSurfaceAction {
                package_name: "workflow.plugin".to_string(),
                request: serde_json::from_value(serde_json::json!({
                    "request_id": "request-1",
                    "surface_id": "home",
                    "action_id": "refresh",
                    "kind": "submit",
                    "payload": { "id": "run" }
                }))
                .expect("typed action request"),
            },
            DaemonRequest::DaemonShutdown,
        ]
    }

    fn daemon_request_tag(request: &DaemonRequest) -> &'static str {
        match request {
            DaemonRequest::Status => "status",
            DaemonRequest::CheckHubUpdate => "check_hub_update",
            DaemonRequest::StartHubUpdate { .. } => "start_hub_update",
            DaemonRequest::GetHubUpdateExecution => "get_hub_update_execution",
            DaemonRequest::ListSessions => "list_sessions",
            DaemonRequest::SubscribeEntities { .. } => "subscribe_entities",
            DaemonRequest::UnsubscribeEntities { .. } => "unsubscribe_entities",
            DaemonRequest::SubscribeEvents { .. } => "subscribe_events",
            DaemonRequest::UnsubscribeEvents { .. } => "unsubscribe_events",
            DaemonRequest::RemoveSession { .. } => "remove_session",
            DaemonRequest::Whoami { .. } => "whoami",
            DaemonRequest::PostMessage { .. } => "post_message",
            DaemonRequest::ReceiveMessages { .. } => "receive_messages",
            DaemonRequest::AckMessage { .. } => "ack_message",
            DaemonRequest::NotifySession { .. } => "notify_session",
            DaemonRequest::Spawn { .. } => "spawn",
            DaemonRequest::Attach { .. } => "attach",
            DaemonRequest::Detach { .. } => "detach",
            DaemonRequest::ShutdownSession { .. } => "shutdown_session",
            DaemonRequest::ReadScreen { .. } => "read_screen",
            DaemonRequest::ReadModeFlags { .. } => "read_mode_flags",
            DaemonRequest::CaptureSnapshot { .. } => "capture_snapshot",
            DaemonRequest::ReadSnapshotPage { .. } => "read_snapshot_page",
            DaemonRequest::ListSessionTypes => "list_session_types",
            DaemonRequest::ListSessionTypesForTarget { .. } => "list_session_types_for_target",
            DaemonRequest::ShowSessionType { .. } => "show_session_type",
            DaemonRequest::ShowSessionTypeDefinition { .. } => "show_session_type_definition",
            DaemonRequest::CreateSessionType { .. } => "create_session_type",
            DaemonRequest::UpdateSessionType { .. } => "update_session_type",
            DaemonRequest::DeleteSessionType { .. } => "delete_session_type",
            DaemonRequest::ResolveSessionType { .. } => "resolve_session_type",
            DaemonRequest::SpawnSessionType { .. } => "spawn_session_type",
            DaemonRequest::ReadSessionContext { .. } => "read_session_context",
            DaemonRequest::ListSpawnTargets => "list_spawn_targets",
            DaemonRequest::ShowSpawnTarget { .. } => "show_spawn_target",
            DaemonRequest::CreateSpawnTarget { .. } => "create_spawn_target",
            DaemonRequest::UpdateSpawnTarget { .. } => "update_spawn_target",
            DaemonRequest::DeleteSpawnTarget { .. } => "delete_spawn_target",
            DaemonRequest::ValidateSpawnTarget { .. } => "validate_spawn_target",
            DaemonRequest::ListWorktrees => "list_worktrees",
            DaemonRequest::ShowWorktree { .. } => "show_worktree",
            DaemonRequest::CreateWorktree { .. } => "create_worktree",
            DaemonRequest::DeleteWorktree { .. } => "delete_worktree",
            DaemonRequest::ListApps => "list_apps",
            DaemonRequest::ResolveAppLaunch { .. } => "resolve_app_launch",
            DaemonRequest::ResolvePackageRoute { .. } => "resolve_package_route",
            DaemonRequest::ListPackageNavigation => "list_package_navigation",
            DaemonRequest::ListPackages => "list_packages",
            DaemonRequest::ListAvailablePackages { .. } => "list_available_packages",
            DaemonRequest::InspectAvailablePackage { .. } => "inspect_available_package",
            DaemonRequest::PreviewPackageInstall { .. } => "preview_package_install",
            DaemonRequest::InstallPackageRegistryEntry { .. } => "install_package_registry_entry",
            DaemonRequest::InstallPackageLocalPath { .. } => "install_package_local_path",
            DaemonRequest::CheckPackageUpdate { .. } => "check_package_update",
            DaemonRequest::PreviewPackageUpdate { .. } => "preview_package_update",
            DaemonRequest::ApplyPackageUpdate { .. } => "apply_package_update",
            DaemonRequest::ShowPackage { .. } => "show_package",
            DaemonRequest::SetPackageConfiguration { .. } => "set_package_configuration",
            DaemonRequest::ReloadPackage { .. } => "reload_package",
            DaemonRequest::RefreshLocalPackages => "refresh_local_packages",
            DaemonRequest::EnablePackageLocalPath { .. } => "enable_package_local_path",
            DaemonRequest::EnablePackage { .. } => "enable_package",
            DaemonRequest::DisablePackage { .. } => "disable_package",
            DaemonRequest::RemovePackage { .. } => "remove_package",
            DaemonRequest::StartPackageEntrypoint { .. } => "start_package_entrypoint",
            DaemonRequest::IssueLocalWebrtcBootstrap { .. } => "issue_local_webrtc_bootstrap",
            DaemonRequest::LocalWebrtcSignal { .. } => "local_webrtc_signal",
            DaemonRequest::StopPackageEntrypoint { .. } => "stop_package_entrypoint",
            DaemonRequest::RestartPackageEntrypoint { .. } => "restart_package_entrypoint",
            DaemonRequest::PackageEntrypointStatus { .. } => "package_entrypoint_status",
            DaemonRequest::PluginLifecycleStatus => "plugin_lifecycle_status",
            DaemonRequest::PluginMcpListTools => "plugin_mcp_list_tools",
            DaemonRequest::PluginMcpCallTool { .. } => "plugin_mcp_call_tool",
            DaemonRequest::PluginSurfaceRender { .. } => "plugin_surface_render",
            DaemonRequest::PluginSurfaceAction { .. } => "plugin_surface_action",
            DaemonRequest::DaemonShutdown => "daemon_shutdown",
        }
    }

    fn daemon_response_kind_examples() -> Vec<DaemonResponseKind> {
        vec![
            DaemonResponseKind::Status,
            DaemonResponseKind::HubUpdate,
            DaemonResponseKind::HubUpdateExecution,
            DaemonResponseKind::Sessions,
            DaemonResponseKind::EntitySubscribed,
            DaemonResponseKind::EntityUnsubscribed,
            DaemonResponseKind::EventSubscribed,
            DaemonResponseKind::EventUnsubscribed,
            DaemonResponseKind::SessionRemoved,
            DaemonResponseKind::Spawned,
            DaemonResponseKind::Events,
            DaemonResponseKind::SessionTypes,
            DaemonResponseKind::SessionTypeDefinition,
            DaemonResponseKind::ResolvedSessionType,
            DaemonResponseKind::SessionContext,
            DaemonResponseKind::ReadScreen,
            DaemonResponseKind::ReadModeFlags,
            DaemonResponseKind::TerminalAttached,
            DaemonResponseKind::TerminalReservation,
            DaemonResponseKind::CaptureSnapshot,
            DaemonResponseKind::SnapshotPage,
            DaemonResponseKind::SpawnTargets,
            DaemonResponseKind::SpawnTargetValidation,
            DaemonResponseKind::Worktrees,
            DaemonResponseKind::Apps,
            DaemonResponseKind::ResolvedAppLaunch,
            DaemonResponseKind::ResolvedPackageRoute,
            DaemonResponseKind::PackageNavigation,
            DaemonResponseKind::Packages,
            DaemonResponseKind::AvailablePackages,
            DaemonResponseKind::PackageInstallPlan,
            DaemonResponseKind::PackageUpdateStatus,
            DaemonResponseKind::PackageDecision,
            DaemonResponseKind::PluginLifecycle,
            DaemonResponseKind::PluginMcpTools,
            DaemonResponseKind::PluginMcpToolResult,
            DaemonResponseKind::PluginSurface,
            DaemonResponseKind::PluginActionResult,
            DaemonResponseKind::LocalWebrtcBootstrap,
            DaemonResponseKind::LocalWebrtcAnswer,
            DaemonResponseKind::SessionCleanup,
            DaemonResponseKind::Identity,
            DaemonResponseKind::MessagePosted,
            DaemonResponseKind::Messages,
            DaemonResponseKind::MessageAcked,
            DaemonResponseKind::SessionNotified,
            DaemonResponseKind::OperatorError,
            DaemonResponseKind::Shutdown,
        ]
    }

    fn daemon_response_kind_tag(kind: DaemonResponseKind) -> &'static str {
        match kind {
            DaemonResponseKind::Status => "status",
            DaemonResponseKind::HubUpdate => "hub_update",
            DaemonResponseKind::HubUpdateExecution => "hub_update_execution",
            DaemonResponseKind::Sessions => "sessions",
            DaemonResponseKind::EntitySubscribed => "entity_subscribed",
            DaemonResponseKind::EntityUnsubscribed => "entity_unsubscribed",
            DaemonResponseKind::EventSubscribed => "event_subscribed",
            DaemonResponseKind::EventUnsubscribed => "event_unsubscribed",
            DaemonResponseKind::SessionRemoved => "session_removed",
            DaemonResponseKind::Spawned => "spawned",
            DaemonResponseKind::Events => "events",
            DaemonResponseKind::SessionTypes => "session_types",
            DaemonResponseKind::SessionTypeDefinition => "session_type_definition",
            DaemonResponseKind::ResolvedSessionType => "resolved_session_type",
            DaemonResponseKind::SessionContext => "session_context",
            DaemonResponseKind::ReadScreen => "read_screen",
            DaemonResponseKind::ReadModeFlags => "read_mode_flags",
            DaemonResponseKind::TerminalAttached => "terminal_attached",
            DaemonResponseKind::TerminalReservation => "terminal_reservation",
            DaemonResponseKind::CaptureSnapshot => "capture_snapshot",
            DaemonResponseKind::SnapshotPage => "snapshot_page",
            DaemonResponseKind::SpawnTargets => "spawn_targets",
            DaemonResponseKind::SpawnTargetValidation => "spawn_target_validation",
            DaemonResponseKind::Worktrees => "worktrees",
            DaemonResponseKind::Apps => "apps",
            DaemonResponseKind::ResolvedAppLaunch => "resolved_app_launch",
            DaemonResponseKind::ResolvedPackageRoute => "resolved_package_route",
            DaemonResponseKind::PackageNavigation => "package_navigation",
            DaemonResponseKind::Packages => "packages",
            DaemonResponseKind::AvailablePackages => "available_packages",
            DaemonResponseKind::PackageInstallPlan => "package_install_plan",
            DaemonResponseKind::PackageUpdateStatus => "package_update_status",
            DaemonResponseKind::PackageDecision => "package_decision",
            DaemonResponseKind::PluginLifecycle => "plugin_lifecycle",
            DaemonResponseKind::PluginMcpTools => "plugin_mcp_tools",
            DaemonResponseKind::PluginMcpToolResult => "plugin_mcp_tool_result",
            DaemonResponseKind::PluginSurface => "plugin_surface",
            DaemonResponseKind::PluginActionResult => "plugin_action_result",
            DaemonResponseKind::LocalWebrtcBootstrap => "local_webrtc_bootstrap",
            DaemonResponseKind::LocalWebrtcAnswer => "local_webrtc_answer",
            DaemonResponseKind::SessionCleanup => "session_cleanup",
            DaemonResponseKind::Identity => "identity",
            DaemonResponseKind::MessagePosted => "message_posted",
            DaemonResponseKind::Messages => "messages",
            DaemonResponseKind::MessageAcked => "message_acked",
            DaemonResponseKind::SessionNotified => "session_notified",
            DaemonResponseKind::OperatorError => "operator_error",
            DaemonResponseKind::Shutdown => "shutdown",
        }
    }

    fn daemon_response_example(kind: DaemonResponseKind) -> DaemonResponse {
        DaemonResponse {
            kind,
            status: Some(DaemonStatus {
                lifecycle_state: "running".to_string(),
                compatibility: DaemonCompatibility::current(),
                software: DaemonSoftwareIdentity {
                    product_id: "botster-hub".to_string(),
                    product_name: "Botster Hub".to_string(),
                    version: "0.1.0".to_string(),
                    build_revision: Some("abc123".to_string()),
                },
                installation: DaemonInstallationIdentity {
                    mode: DaemonInstallationMode::Managed,
                    provenance: "managed_receipt".to_string(),
                    release_channel: Some("stable".to_string()),
                    provider: Some("http_json".to_string()),
                    diagnostics: Vec::new(),
                },
                host_id: "hub".to_string(),
                host_display_name: "Hub".to_string(),
                schema_version: 1,
                data_dir_configured: true,
                core_initialized: true,
                state_source: "initialized".to_string(),
                package_count: 1,
                enabled_package_count: 1,
                provider_count: 0,
                enabled_provider_count: 0,
                session_count: 1,
                recovered_sessions: vec!["session".to_string()],
                stale_sessions: Vec::new(),
                lifecycle_counters: DaemonLifecycleCounters::default(),
                live_attach_occupancy: Vec::new(),
                observability: DaemonObservabilityCounters::default(),
                retention: Some(DaemonRetentionAccounting {
                    max_object_bytes: 16 << 20,
                    max_total_bytes: 64 << 20,
                    max_sessions: 200,
                    total_bytes: 4096,
                    sessions: 1,
                    evictions: 0,
                }),
                diagnostics: vec![DaemonDiagnostic::connected("status")],
            }),
            sessions: vec![DaemonSession {
                session_id: "session".to_string(),
                lifecycle: "running".to_string(),
            }],
            session_types: vec![daemon_session_type_example()],
            session_type_definition: Some(DaemonSessionTypeEditableDefinition {
                session_type_id: "device/init".to_string(),
                source: DaemonSessionTypeMutationSource::Device,
                definition: daemon_session_type_definition_example(),
            }),
            resolved_session_type: Some(DaemonResolvedSessionType {
                session_type: daemon_session_type_example(),
                session_id: "session".to_string(),
                executable: "/tmp/workflow.plugin/bin/init".to_string(),
                arguments: vec!["--json".to_string()],
                working_directory: "/tmp/workflow.plugin".to_string(),
                environment: BTreeMap::from([(
                    "BOTSTER_SESSION_ID".to_string(),
                    "session".to_string(),
                )]),
                context_id: "ctx-session".to_string(),
                context_keys: vec!["prompt".to_string()],
            }),
            session_context: Some(DaemonSessionContext {
                context_id: "ctx-session".to_string(),
                session_id: "session".to_string(),
                values: BTreeMap::from([("prompt".to_string(), "hello".to_string())]),
            }),
            read_screen: Some(DaemonReadScreen {
                session_id: "session".to_string(),
                text: "ready".to_string(),
                unavailable: None,
            }),
            mode_flags: Some(DaemonModeFlags::new(
                "session", false, true, false, 9, false, false, false, 24, 80, None,
            )),
            terminal_attach: Some(DaemonTerminalAttach::new("session", "subscription", 1)),
            terminal_reservation: Some(DaemonTerminalReservation::new(
                "session",
                "subscription",
                1,
                1,
                "r-example",
                30,
            )),
            subscription_reservation: None,
            capture_snapshot: Some(DaemonCaptureSnapshot {
                session_id: "session".to_string(),
                capture_id: "capture-1".to_string(),
                total_bytes: 5,
                page_bytes: SNAPSHOT_PAGE_BYTES as u32,
                pages: 1,
                rows: 24,
                cols: 80,
                unavailable: None,
            }),
            snapshot_page: Some(DaemonSnapshotPage {
                session_id: "session".to_string(),
                capture_id: "capture-1".to_string(),
                page: 0,
                payload: DaemonOpaqueHistoryPayload::from_bytes(&[0, 255, 71, 84, 89]),
            }),
            spawn_targets: vec![DaemonSpawnTarget {
                target_id: "tgt_example".to_string(),
                label: "Example".to_string(),
                root: PathBuf::from("/tmp/example"),
                enabled: true,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::from([("purpose".to_string(), "test".to_string())]),
            }],
            spawn_target_validation: Some(DaemonSpawnTargetValidation {
                target_id: "tgt_example".to_string(),
                ok: true,
                status: "ok".to_string(),
            }),
            worktrees: vec![DaemonWorktree {
                worktree_id: "wt_example".to_string(),
                target_id: "tgt_example".to_string(),
                label: "Example Worktree".to_string(),
                path: PathBuf::from("/tmp/example/worktree"),
                status: "present".to_string(),
                management: "registered".to_string(),
                git: Some(DaemonWorktreeGitMetadata {
                    repository_root: PathBuf::from("/tmp/example/worktree"),
                    branch: Some("main".to_string()),
                    head: Some("ref: refs/heads/main".to_string()),
                }),
                metadata: BTreeMap::from([("purpose".to_string(), "test".to_string())]),
            }],
            apps: vec![DaemonApp {
                package_name: "workflow.plugin".to_string(),
                app_id: "web".to_string(),
                entrypoint_id: "web".to_string(),
                kind: "web_app".to_string(),
                launch_mode: "background".to_string(),
                lifecycle_state: "running".to_string(),
                diagnostics: Vec::new(),
                actions: Vec::new(),
                blocked_reasons: Vec::new(),
                launch_target: DaemonAppLaunchTarget {
                    kind: "web".to_string(),
                    local_url: Some("http://127.0.0.1:49152".to_string()),
                },
                route: None,
            }],
            resolved_app_launch: Some(DaemonResolvedAppLaunch {
                package_name: "workflow.plugin".to_string(),
                app_id: "terminal".to_string(),
                entrypoint_id: "terminal".to_string(),
                kind: "terminal_app".to_string(),
                launch_mode: "foreground_stdio".to_string(),
                command: "botster-tui".to_string(),
                args: vec!["--data-dir".to_string(), "/tmp/botster".to_string()],
                working_directory: "/tmp/workflow".to_string(),
                environment: BTreeMap::from([(
                    "BOTSTER_HUB_SOCKET".to_string(),
                    "/tmp/botster.sock".to_string(),
                )]),
            }),
            resolved_package_route: Some(DaemonPackageRouteDescriptor {
                package_name: "workflow.plugin".to_string(),
                route_id: "surface:workflow.home".to_string(),
                route_path: "/packages/workflow.plugin/surfaces/workflow.home".to_string(),
                target: DaemonPackageRouteTarget {
                    kind: "plugin_surface".to_string(),
                    entrypoint_id: None,
                    surface_id: Some("workflow.home".to_string()),
                },
                title: "Workflow".to_string(),
                label: "Workflow".to_string(),
                app_id: Some("workflow.home".to_string()),
                surface_id: Some("workflow.home".to_string()),
                icon: Some("workflow".to_string()),
                category: Some("workflows".to_string()),
                layout_mode: "plugin_surface".to_string(),
                required_capabilities: vec![DaemonCapability {
                    surface: "Surfaces".to_string(),
                    scope: None,
                }],
                enabled: true,
                blocked: false,
                diagnostics: Vec::new(),
                supports_settings: true,
            }),
            package_navigation: vec![DaemonPackageNavigationEntry {
                package_name: "workflow.plugin".to_string(),
                item_id: "home".to_string(),
                label: "Workflow".to_string(),
                icon: Some("workflow".to_string()),
                description: Some("Workflow home".to_string()),
                route_id: "surface:workflow.home".to_string(),
                route_path: "/packages/workflow.plugin/surfaces/workflow.home".to_string(),
                target: DaemonPackageRouteTarget {
                    kind: "plugin_surface".to_string(),
                    entrypoint_id: None,
                    surface_id: Some("workflow.home".to_string()),
                },
                source: DaemonPackageNavigationSource {
                    kind: "surface".to_string(),
                    surface_id: Some("workflow.home".to_string()),
                    entrypoint_id: None,
                },
                enabled: true,
                blocked: false,
                diagnostics: Vec::new(),
            }],
            packages: vec![DaemonPackage {
                package_name: "workflow.plugin".to_string(),
                version: "1.0.0".to_string(),
                classification: "plugin".to_string(),
                source_kind: "path".to_string(),
                state: "enabled".to_string(),
                requested_capabilities: vec![DaemonCapability {
                    surface: "Network".to_string(),
                    scope: Some("localhost".to_string()),
                }],
                surfaces: Vec::new(),
                notice_reactions: Vec::new(),
                routes: Vec::new(),
                runnable_entrypoints: Vec::new(),
                configuration: DaemonPackageConfiguration::default(),
                availability: DaemonPackageAvailability::default(),
                dependency_availability: Vec::new(),
                feature_availability: Vec::new(),
                actions: Vec::new(),
                provider_profile_admitted: false,
            }],
            available_packages: vec![DaemonAvailablePackage {
                entry_id: "workflow-plugin".to_string(),
                package_name: "workflow.plugin".to_string(),
                version: "1.0.0".to_string(),
                classification: "plugin".to_string(),
                source_kind: "git".to_string(),
                source_label: "https://example.invalid/workflow.git".to_string(),
                first_party: true,
                state: "available".to_string(),
                requested_capabilities: Vec::new(),
                compatibility: DaemonPackageCompatibility {
                    botster_requirement: ">=0.1.0".to_string(),
                    result: "compatible".to_string(),
                    diagnostics: Vec::new(),
                },
                pin: Some(DaemonPackagePin {
                    revision: "main".to_string(),
                    branch: Some("main".to_string()),
                    tag: None,
                    rev: None,
                    checksum: None,
                    update_policy: "manual".to_string(),
                }),
                actions: Vec::new(),
            }],
            install_plan: Some(DaemonPackageInstallPlan {
                entry: DaemonAvailablePackage {
                    entry_id: "workflow-plugin".to_string(),
                    package_name: "workflow.plugin".to_string(),
                    version: "1.0.0".to_string(),
                    classification: "plugin".to_string(),
                    source_kind: "git".to_string(),
                    source_label: "https://example.invalid/workflow.git".to_string(),
                    first_party: true,
                    state: "available".to_string(),
                    requested_capabilities: Vec::new(),
                    compatibility: DaemonPackageCompatibility {
                        botster_requirement: ">=0.1.0".to_string(),
                        result: "compatible".to_string(),
                        diagnostics: Vec::new(),
                    },
                    pin: None,
                    actions: Vec::new(),
                },
                effects: vec![DaemonPackageInstallEffect {
                    kind: "add_package_record".to_string(),
                    message: "would add package record".to_string(),
                }],
                diagnostics: Vec::new(),
                mutates_registry: false,
                starts_entrypoints: false,
            }),
            update_status: Some(DaemonPackageUpdateStatus {
                package_name: "workflow.plugin".to_string(),
                update_available: false,
                reload_required: false,
                restart_required: false,
                pin: None,
                diagnostics: vec![DaemonPackageDiagnostic {
                    kind: "update_unavailable".to_string(),
                    message: "update resolution is unavailable for this package source".to_string(),
                }],
                actions: Vec::new(),
            }),
            hub_update: Some(DaemonHubUpdate {
                state: DaemonHubUpdateState::Current,
                current_version: "0.1.0".to_string(),
                available_version: Some("0.1.0".to_string()),
                build_revision: Some("abc123".to_string()),
                reason: Some("up_to_date".to_string()),
                action: None,
            }),
            hub_update_execution: Some(DaemonHubUpdateExecution {
                update_id: "update-example".to_string(),
                scope: DaemonHubUpdateScope::All,
                state: DaemonHubUpdateExecutionState::Running,
                updater_pid: 42,
                error: None,
            }),
            package_decision: Some(DaemonPackageDecision {
                package_name: "workflow.plugin".to_string(),
                action: "enable".to_string(),
                state: "enabled".to_string(),
                classification: "plugin".to_string(),
            }),
            lifecycle: vec![DaemonPluginLifecycle {
                package_name: "workflow.plugin".to_string(),
                state: "loaded".to_string(),
                loaded: true,
            }],
            plugin_worker_counters: Some(DaemonPluginWorkerCounters {
                configured_queue_capacity: 64,
                configured_executor_concurrency: 2,
                live_plugin_executors: 1,
                live_executor_workers: 2,
                queued_jobs: 0,
                in_flight_jobs: 0,
            }),
            plugin_resource_counters: Some(DaemonPluginResourceCounters {
                active_timer_resources: 0,
            }),
            plugin_tools: vec![serde_json::json!({ "name": "tool" })],
            plugin_tool_result: serde_json::json!({ "content": [] }),
            plugin_surface: Some(DaemonPluginSurface {
                package_name: "workflow.plugin".to_string(),
                surface_id: "workflow.surface".to_string(),
                body: serde_json::from_value(
                    serde_json::json!({ "type": "text", "props": { "text": "surface" } }),
                )
                .expect("typed surface"),
                ui_tree_snapshot: Some(DaemonUiTreeSnapshot {
                    package_name: "workflow.plugin".to_string(),
                    surface_id: "workflow.surface".to_string(),
                    body: serde_json::from_value(
                        serde_json::json!({ "type": "text", "props": { "text": "surface" } }),
                    )
                    .expect("typed snapshot"),
                }),
            }),
            plugin_action_result: Some(
                serde_json::from_value(serde_json::json!({
                    "request_id": "request-1",
                    "surface_id": "home",
                    "action_id": "refresh",
                    "state": "accepted"
                }))
                .expect("typed action result"),
            ),
            local_webrtc_bootstrap: Some(DaemonLocalWebrtcBootstrap {
                grant_id: "grant".to_string(),
                grant_secret: "secret".to_string(),
                package_name: "workflow.plugin".to_string(),
                entrypoint_id: "web".to_string(),
                expected_origin: "http://127.0.0.1:49152".to_string(),
                expires_at: 123,
                signaling_transport: "daemon_request".to_string(),
                data_plane: "webrtc_data_channel".to_string(),
                ordered: true,
                max_retransmits: None,
                max_packet_lifetime_ms: None,
            }),
            local_webrtc_answer: Some(DaemonLocalWebrtcAnswer {
                grant_id: "grant".to_string(),
                answer: serde_json::json!({
                    "type": "answer",
                    "sdp": "v=0\r\n"
                }),
                diagnostics: vec![DaemonDiagnostic::connected("local_webrtc_signal")],
            }),
            events: daemon_event_examples(),
            cleanup: Some(DaemonSessionCleanup {
                session_id: "session".to_string(),
                outcome: "stopped".to_string(),
            }),
            coordination: Some(DaemonCoordination {
                identity: Some(DaemonIdentity {
                    client_id: "client".to_string(),
                    role: "operator".to_string(),
                    identity_source: "session".to_string(),
                    caller_session_id: Some("caller".to_string()),
                    host_id: "hub".to_string(),
                    host_display_name: "Hub".to_string(),
                }),
                publish: Some(DaemonEnvelopePublish {
                    deliveries: vec![DaemonEnvelopeDelivery {
                        envelope_id: "envelope".to_string(),
                        target: "target".to_string(),
                        cursor: 1,
                        status: "delivered".to_string(),
                    }],
                }),
                messages: vec![DaemonEnvelope {
                    envelope_id: "envelope".to_string(),
                    source: "source".to_string(),
                    content_type: "text/plain".to_string(),
                    body: "hello".to_string(),
                    created_at: 1,
                    cursor: Some(1),
                }],
                next_cursor: Some(2),
                ack: Some(DaemonEnvelopeAck {
                    envelope_id: Some("envelope".to_string()),
                    target: Some("target".to_string()),
                    cursor: Some(1),
                    status: "acked".to_string(),
                }),
                notify: Some(DaemonNotify {
                    decision: "delivered".to_string(),
                    state_count: 1,
                    states: vec!["ready".to_string()],
                }),
            }),
            error: Some(DaemonOperatorError {
                code: "operator_error".to_string(),
                request_id: "request".to_string(),
                operation: "test".to_string(),
                message: "failed".to_string(),
                diagnostics: vec![DaemonDiagnostic::action_failure("test", "failed")],
            }),
            diagnostics: vec![DaemonDiagnostic::connected("test")],
        }
    }

    fn daemon_event_examples() -> Vec<DaemonEvent> {
        vec![
            DaemonEvent::SessionLifecycle {
                session_id: "session".to_string(),
                state: "running".to_string(),
            },
            DaemonEvent::RuntimeObservation {
                kind: "observation".to_string(),
            },
            DaemonEvent::WorktreeLifecycle {
                event: DaemonWorktreeLifecycleEvent {
                    event: "worktree_created".to_string(),
                    worktree_id: Some("worktree".to_string()),
                    target_id: Some("target".to_string()),
                    status: Some("present".to_string()),
                    label: Some("Worktree".to_string()),
                    display_path: Some("workspace".to_string()),
                    failure_kind: None,
                    message: None,
                },
            },
            DaemonEvent::PackageEvent {
                subscription_id: "events".to_string(),
                owner: "event-plane-producer".to_string(),
                name: "sample.ready".to_string(),
                payload: serde_json::json!({ "ok": true, "token": "live" }),
            },
            DaemonEvent::EventGap {
                subscription_id: "events".to_string(),
                owner: "event-plane-producer".to_string(),
                name: "sample.ready".to_string(),
            },
        ]
    }

    fn daemon_event_tag(event: &DaemonEvent) -> &'static str {
        match event {
            DaemonEvent::SessionLifecycle { .. } => "session_lifecycle",
            DaemonEvent::RuntimeObservation { .. } => "runtime_observation",
            DaemonEvent::WorktreeLifecycle { .. } => "worktree_lifecycle",
            DaemonEvent::TerminalSubscriptionClosed { .. } => "terminal_subscription_closed",
            DaemonEvent::PackageEvent { .. } => "package_event",
            DaemonEvent::EventGap { .. } => "event_gap",
        }
    }

    #[test]
    fn newline_json_hello_ack_reports_an_invalid_frame_length() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        server
            .write_all(br#"{"protocol":"botster-hub-daemon-v1","compatibility":{}}"#)
            .expect("write old hello ack");
        server.write_all(b"\n").expect("write newline");

        let error = read_hello_ack(&mut client).expect_err("old hello ack should fail");

        assert!(matches!(error, DaemonTransportError::Protocol(_)));
        assert_eq!(
            error.to_string(),
            "daemon protocol error: unix frame length is zero or exceeds the bound"
        );
    }

    #[test]
    fn valid_frame_lengths_with_json_low_byte_are_accepted() {
        for body_len in [123, 379] {
            let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
            let body = vec![0_u8; body_len];
            server
                .write_all(&(body.len() as u32).to_le_bytes())
                .expect("write length prefix");
            server.write_all(&body).expect("write frame body");

            let mut reader = DaemonUnixFrameReader::new();
            let frame = reader
                .read_raw_frame(&mut client, MAX_UNIX_FRAME_BYTES)
                .expect("valid bounded frame");

            assert_eq!(frame, body);
        }
    }

    #[test]
    fn frame_reader_distinguishes_empty_and_truncated_prefix_eof() {
        let (server, mut client) = UnixStream::pair().expect("pair unix streams");
        drop(server);
        let mut reader = DaemonUnixFrameReader::new();
        assert!(matches!(
            reader.read_raw_frame(&mut client, MAX_UNIX_FRAME_BYTES),
            Err(DaemonTransportError::ClientDisconnected)
        ));

        for prefix_len in 1..UNIX_FRAME_LENGTH_PREFIX_BYTES {
            let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
            server
                .write_all(&123_u32.to_le_bytes()[..prefix_len])
                .expect("write partial prefix");
            drop(server);
            let mut reader = DaemonUnixFrameReader::new();
            let error = reader
                .read_raw_frame(&mut client, MAX_UNIX_FRAME_BYTES)
                .expect_err("partial prefix must fail at EOF");
            assert_eq!(
                error.to_string(),
                "daemon protocol error: truncated unix frame length prefix"
            );
        }
    }

    #[test]
    fn zero_declared_frame_length_is_a_bound_error() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        server
            .write_all(&0_u32.to_le_bytes())
            .expect("write zero length prefix");
        let mut reader = DaemonUnixFrameReader::new();
        let error = reader
            .read_raw_frame(&mut client, MAX_UNIX_FRAME_BYTES)
            .expect_err("zero declared length must fail");
        assert_eq!(
            error.to_string(),
            "daemon protocol error: unix frame length is zero or exceeds the bound"
        );
    }

    #[test]
    fn malformed_hello_ack_frame_reports_a_typed_protocol_violation() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        let payload = br#"{"frame":"hello_ack","ack":{"protocol":1}}"#;
        let frame_len = (1 + payload.len()) as u32;
        server
            .write_all(&frame_len.to_le_bytes())
            .expect("write length prefix");
        server
            .write_all(&[UNIX_CONTAINER_CONTROL])
            .expect("write container");
        server
            .write_all(payload)
            .expect("write malformed hello ack");

        let error = read_hello_ack(&mut client).expect_err("malformed ack should fail");

        assert!(matches!(
            error,
            DaemonTransportError::ProtocolViolation(DaemonProtocolErrorCode::MalformedFrame)
        ));
    }

    #[test]
    fn unknown_container_and_oversized_length_are_protocol_errors() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        server
            .write_all(&3u32.to_le_bytes())
            .expect("write length prefix");
        server
            .write_all(&[9, 0, 0])
            .expect("write unknown container");
        let mut frames = DaemonUnixFrameReader::new();
        let error = frames
            .read_frame(&mut client)
            .expect_err("unknown container must fail");
        assert!(matches!(
            error,
            DaemonTransportError::ProtocolViolation(DaemonProtocolErrorCode::UnknownContainer)
        ));

        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        server
            .write_all(&((MAX_UNIX_FRAME_BYTES as u32) + 1).to_le_bytes())
            .expect("write oversized prefix");
        let mut frames = DaemonUnixFrameReader::new();
        let error = frames
            .read_frame(&mut client)
            .expect_err("oversized declared length must fail before any allocation");
        assert!(matches!(error, DaemonTransportError::Protocol(_)));
    }

    #[test]
    fn request_ids_are_canonical_decimal_u64_values() {
        assert_eq!(parse_request_id("1"), Some(1));
        assert_eq!(parse_request_id("18446744073709551615"), Some(u64::MAX));
        assert_eq!(parse_request_id(""), None);
        assert_eq!(parse_request_id("0"), None);
        assert_eq!(parse_request_id("01"), None);
        assert_eq!(parse_request_id("-1"), None);
        assert_eq!(parse_request_id("1a"), None);
        assert_eq!(parse_request_id("18446744073709551616"), None);
        assert_eq!(parse_request_id("100000000000000000000"), None);
        assert_eq!(encode_request_id(42), "42");
        assert_eq!(encode_request_id(42).len() <= MAX_REQUEST_ID_BYTES, true);

        let mut ids = RequestIdSequence::new();
        assert_eq!(ids.last(), 0);
        assert_eq!(ids.next(), 1);
        assert_eq!(ids.next(), 2);
        assert_eq!(ids.last(), 2);
    }

    #[test]
    fn client_and_server_frames_are_tagged_by_frame() {
        let request = ClientFrame::Request {
            request_id: "7".to_string(),
            request: DaemonRequest::Status,
        };
        let value = serde_json::to_value(&request).expect("client frame serializes");
        assert_eq!(value["frame"], "request");
        assert_eq!(value["request_id"], "7");
        assert_eq!(value["request"]["type"], "status");
        assert_eq!(
            serde_json::from_value::<ClientFrame>(value).expect("client frame deserializes"),
            request
        );

        let hello = ClientFrame::Hello {
            hello: DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        };
        let value = serde_json::to_value(&hello).expect("hello serializes");
        assert_eq!(value["frame"], "hello");
        assert_eq!(value["hello"]["protocol"], PROTOCOL);

        let frames = vec![
            ServerFrame::HelloAck {
                ack: DaemonHelloAck {
                    protocol: PROTOCOL.to_string(),
                    compatibility: DaemonCompatibility::current(),
                    terminal_compatibility: None,
                    diagnostics: Vec::new(),
                },
            },
            ServerFrame::Response {
                request_id: "7".to_string(),
                response: daemon_response_example(DaemonResponseKind::Status),
            },
            ServerFrame::Event {
                event: DaemonEvent::SessionLifecycle {
                    session_id: "session".to_string(),
                    state: "running".to_string(),
                },
            },
            ServerFrame::Entity {
                entity: DaemonEntityFrame::Remove {
                    subscription_id: "subscription".to_string(),
                    entity_type: "session".to_string(),
                    snapshot_seq: 4,
                    id: "session".to_string(),
                },
            },
            ServerFrame::Close {
                reason: DaemonCloseReason::ProtocolError {
                    code: DaemonProtocolErrorCode::NonincreasingRequestId,
                },
            },
        ];
        let tags = ["hello_ack", "response", "event", "entity", "close"];
        for (frame, tag) in frames.into_iter().zip(tags) {
            let value = serde_json::to_value(&frame).expect("server frame serializes");
            assert_eq!(value["frame"], tag);
            assert_eq!(
                serde_json::from_value::<ServerFrame>(value).expect("server frame deserializes"),
                frame
            );
        }
        let close = serde_json::to_value(ServerFrame::Close {
            reason: DaemonCloseReason::ProtocolError {
                code: DaemonProtocolErrorCode::FrameTooLarge,
            },
        })
        .expect("close serializes");
        assert_eq!(close["reason"]["reason"], "protocol_error");
        assert_eq!(close["reason"]["code"], "frame_too_large");
    }

    #[test]
    fn terminal_container_round_trips_route_generation_and_opaque_body() {
        let body = [2u8, 1, 0, 0, 3, 0, 0, 0, b'a', b'b', b'c'];
        let frame = encode_unix_terminal_frame("sub-1", 9, 4, &body).expect("encode container");
        let frame_len = u32::from_le_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(frame_len, frame.len() - UNIX_FRAME_LENGTH_PREFIX_BYTES);
        assert_eq!(frame[4], UNIX_CONTAINER_TERMINAL);
        let header = UnixTerminalContainerHeader::new("sub-1", 9, 4, body.len()).expect("header");
        assert_eq!(&frame[..header.as_bytes().len()], header.as_bytes());
        assert_eq!(
            header.as_bytes().len(),
            UNIX_FRAME_LENGTH_PREFIX_BYTES + 1 + UNIX_TERMINAL_CONTAINER_FIXED_BYTES + 5
        );
        // Container payload offsets: route_len at 0, route at 2, generation at
        // 2 + route_len, stream_epoch at 10 + route_len, body at 14 + route_len.
        let payload = &frame[UNIX_FRAME_LENGTH_PREFIX_BYTES + 1..];
        assert_eq!(&payload[0..2], &5u16.to_le_bytes());
        assert_eq!(&payload[2..7], b"sub-1");
        assert_eq!(&payload[7..15], &9u64.to_le_bytes());
        assert_eq!(&payload[15..19], &4u32.to_le_bytes());
        assert_eq!(&payload[19..], &body);

        match decode_unix_frame::<ServerFrame>(&frame[UNIX_FRAME_LENGTH_PREFIX_BYTES..])
            .expect("decode container")
        {
            DaemonUnixFrame::Terminal(decoded) => {
                assert_eq!(decoded.route, "sub-1");
                assert_eq!(decoded.generation, 9);
                assert_eq!(decoded.stream_epoch, 4);
                assert_eq!(decoded.body, body);
            }
            DaemonUnixFrame::Control(_) => panic!("terminal container decoded as control"),
        }

        assert!(UnixTerminalContainerHeader::new("", 1, 0, 0).is_none());
        let long_route = "r".repeat(MAX_UNIX_TERMINAL_ROUTE_BYTES + 1);
        assert!(UnixTerminalContainerHeader::new(&long_route, 1, 0, 0).is_none());
        let max_route = "r".repeat(MAX_UNIX_TERMINAL_ROUTE_BYTES);
        assert!(UnixTerminalContainerHeader::new(&max_route, 1, 0, 0).is_some());
        assert!(UnixTerminalContainerHeader::new("route", 1, 0, MAX_UNIX_FRAME_BYTES).is_none());

        let mut control_route = frame.clone();
        control_route[UNIX_FRAME_LENGTH_PREFIX_BYTES + 1 + 2] = b'\n';
        assert_eq!(
            decode_unix_frame::<ServerFrame>(&control_route[UNIX_FRAME_LENGTH_PREFIX_BYTES..])
                .expect_err("control characters are not a route"),
            DaemonProtocolErrorCode::InvalidRoute
        );
    }

    #[test]
    fn frame_reader_keeps_a_partial_frame_across_read_timeouts() {
        let (mut server, mut client) = UnixStream::pair().expect("pair");
        client
            .set_read_timeout(Some(Duration::from_millis(30)))
            .expect("timeout");
        let frame = encode_unix_terminal_frame("sub", 3, 0, b"a").expect("encode");
        let mut frames = DaemonUnixFrameReader::new();
        server.write_all(&frame[..3]).expect("write partial prefix");
        let first = frames.read_frame(&mut client);
        assert!(
            matches!(
                &first,
                Err(DaemonTransportError::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    )
            ),
            "partial prefix must time out, got {first:?}"
        );
        assert!(frames.has_partial_frame());
        server.write_all(&frame[3..9]).expect("write partial body");
        let second = frames.read_frame(&mut client);
        assert!(second.is_err(), "partial body must time out");
        assert!(frames.has_partial_frame());
        server.write_all(&frame[9..]).expect("write rest");
        match frames.read_frame(&mut client).expect("complete frame") {
            DaemonUnixMuxFrame::Terminal(decoded) => {
                assert_eq!(decoded.route, "sub");
                assert_eq!(decoded.generation, 3);
                assert_eq!(decoded.body, b"a");
            }
            other => panic!("expected terminal frame, got {other:?}"),
        }
        assert!(!frames.has_partial_frame());
    }

    #[test]
    fn local_webrtc_terminal_chunk_header_round_trips_and_bounds_declared_counts() {
        let header = LocalWebrtcTerminalChunkHeader {
            message_id: 7,
            chunk_index: 1,
            chunk_count: 3,
            total_bytes: 30_000,
            generation: 11,
            stream_epoch: 2,
        };
        let encoded = header.encode();
        assert_eq!(encoded[0], 2);
        assert_eq!(&encoded[1..9], &7u64.to_le_bytes());
        assert_eq!(&encoded[9..13], &1u32.to_le_bytes());
        assert_eq!(&encoded[13..17], &3u32.to_le_bytes());
        assert_eq!(&encoded[17..21], &30_000u32.to_le_bytes());
        assert_eq!(&encoded[21..29], &11u64.to_le_bytes());
        assert_eq!(&encoded[29..33], &2u32.to_le_bytes());
        let sealed =
            vec![
                0u8;
                LOCAL_WEBRTC_TERMINAL_CHUNK_NONCE_BYTES + 5 + LOCAL_WEBRTC_TERMINAL_CHUNK_TAG_BYTES
            ];
        let mut message = header.encode().to_vec();
        message.extend_from_slice(&sealed);
        assert_eq!(
            message.len(),
            LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES + sealed.len()
        );
        let (decoded, slice) = LocalWebrtcTerminalChunkHeader::decode(&message).expect("decode");
        assert_eq!(decoded, header);
        assert_eq!(slice, &sealed[..]);

        let mut wrong_version = message.clone();
        wrong_version[0] = 1;
        assert!(LocalWebrtcTerminalChunkHeader::decode(&wrong_version).is_none());
        let short = &message[..LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES + 3];
        assert!(LocalWebrtcTerminalChunkHeader::decode(short).is_none());
        let inverted = LocalWebrtcTerminalChunkHeader {
            chunk_index: 3,
            ..header
        };
        let mut inverted_message = inverted.encode().to_vec();
        inverted_message.extend_from_slice(&sealed);
        assert!(LocalWebrtcTerminalChunkHeader::decode(&inverted_message).is_none());
        let oversized = LocalWebrtcTerminalChunkHeader {
            total_bytes: (LOCAL_WEBRTC_MAX_DELIVERY_BYTES + 1) as u32,
            chunk_count: u32::MAX,
            chunk_index: 0,
            ..header
        };
        let mut oversized_message = oversized.encode().to_vec();
        oversized_message.extend_from_slice(&sealed);
        assert!(LocalWebrtcTerminalChunkHeader::decode(&oversized_message).is_none());
    }

    #[test]
    fn local_webrtc_delivery_chunk_is_serde_stable_and_generated() {
        let chunk = DaemonLocalWebrtcDeliveryChunk {
            version: LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
            delivery_kind: DaemonLocalWebrtcDeliveryKind::ServerFrame,
            message_id: "response-fixture".to_string(),
            chunk_index: 1,
            chunk_count: 3,
            total_bytes: 123_456,
            payload: "ciphertext-slice".to_string(),
        };
        assert_eq!(
            serde_json::from_value::<DaemonLocalWebrtcDeliveryChunk>(
                serde_json::to_value(&chunk).unwrap()
            )
            .unwrap(),
            chunk
        );
        assert!(daemon_protocol_typescript().contains("type DaemonLocalWebrtcDeliveryKind"));
        assert!(daemon_protocol_typescript().contains("interface DaemonLocalWebrtcDeliveryChunk"));
    }

    #[test]
    fn hub_maintenance_contract_is_serde_stable_and_package_rows_do_not_claim_hub_identity() {
        let response = daemon_response_example(DaemonResponseKind::HubUpdate);
        let value = serde_json::to_value(&response).expect("serialize maintenance response");
        assert_eq!(value["status"]["software"]["product_id"], "botster-hub");
        assert_eq!(value["status"]["installation"]["mode"], "managed");
        assert_eq!(value["hub_update"]["state"], "current");
        assert_eq!(value["hub_update"]["reason"], "up_to_date");
        assert!(
            value["available_packages"][0]["compatibility"]
                .get("hub_version")
                .is_none()
        );

        let generated = daemon_protocol_typescript();
        assert!(generated.contains("{ type: \"check_hub_update\" }"));
        assert!(generated.contains("export interface DaemonSoftwareIdentity"));
        assert!(generated.contains("export interface DaemonInstallationIdentity"));
        assert!(generated.contains("export interface DaemonHubUpdate"));
        assert!(generated.contains("hub_update?: DaemonHubUpdate | null;"));
        assert!(!generated.contains("  hub_version: string;"));
    }

    #[test]
    fn hub_source_update_execution_contract_is_serde_stable_and_generated() {
        assert_eq!(
            serde_json::to_value(DaemonRequest::StartHubUpdate {
                scope: DaemonHubUpdateScope::All,
            })
            .unwrap(),
            serde_json::json!({ "type": "start_hub_update", "scope": "all" })
        );
        assert_eq!(
            serde_json::to_value(DaemonRequest::GetHubUpdateExecution).unwrap(),
            serde_json::json!({ "type": "get_hub_update_execution" })
        );
        let response = daemon_response_example(DaemonResponseKind::HubUpdateExecution);
        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["kind"], "hub_update_execution");
        assert_eq!(value["hub_update_execution"]["scope"], "all");
        assert_eq!(value["hub_update_execution"]["state"], "running");
        assert_eq!(value["hub_update_execution"]["updater_pid"], 42);

        let generated = daemon_protocol_typescript();
        assert!(generated.contains("{ type: \"start_hub_update\"; scope: DaemonHubUpdateScope }"));
        assert!(generated.contains("{ type: \"get_hub_update_execution\" }"));
        assert!(generated.contains("export type DaemonHubUpdateScope ="));
        assert!(generated.contains("export type DaemonHubUpdateExecutionState ="));
        assert!(generated.contains("export interface DaemonHubUpdateExecution"));
        assert!(generated.contains("hub_update_execution?: DaemonHubUpdateExecution | null;"));
    }

    #[test]
    fn hub_maintenance_optional_fields_follow_serde_omission() {
        let mut response = daemon_response_example(DaemonResponseKind::Status);
        response.hub_update = None;
        let status = response.status.as_mut().expect("status example");
        status.software.build_revision = None;
        status.installation.release_channel = None;
        status.installation.provider = None;
        status.installation.diagnostics.clear();
        let value = serde_json::to_value(response).expect("serialize omitted maintenance fields");
        assert!(value.get("hub_update").is_none());
        assert!(value["status"]["software"].get("build_revision").is_none());
        assert!(
            value["status"]["installation"]
                .get("release_channel")
                .is_none()
        );
        assert!(value["status"]["installation"].get("provider").is_none());
        assert!(value["status"]["installation"].get("diagnostics").is_none());
    }

    #[test]
    fn protocol_nine_and_conformance_forty_nine_define_the_cold_cut_boundary() {
        assert_eq!(PROTOCOL_VERSION, 9);
        assert_eq!(CONFORMANCE_FIXTURE_REVISION, 49);

        let requirement = DaemonCompatibilityRequirement::current();
        let protocol_error = ensure_compatible(
            &requirement,
            &DaemonCompatibility {
                protocol_version: 5,
                ..DaemonCompatibility::current()
            },
        )
        .expect_err("new client rejects protocol five Hub");
        assert!(
            protocol_error
                .diagnostic
                .contains("unsupported protocol version 5")
        );

        let conformance_error = ensure_compatible(
            &requirement,
            &DaemonCompatibility {
                conformance_fixture_revision: 29,
                ..DaemonCompatibility::current()
            },
        )
        .expect_err("new client rejects conformance twenty nine Hub");
        assert!(
            conformance_error
                .diagnostic
                .contains("unsupported conformance fixture revision 29")
        );

        let stale_requirement = DaemonCompatibilityRequirement {
            protocol_version: 5,
            minimum_conformance_fixture_revision: 29,
            ..DaemonCompatibilityRequirement::current()
        };
        ensure_compatible(&stale_requirement, &DaemonCompatibility::current())
            .expect_err("stale client rejects the cold-cut protocol");

        #[derive(Deserialize)]
        struct StaleStatus {
            compatibility: DaemonCompatibility,
            host_id: String,
            schema_version: u16,
        }
        let status_value = serde_json::to_value(
            daemon_response_example(DaemonResponseKind::Status)
                .status
                .expect("status example"),
        )
        .expect("serialize current status");
        let stale: StaleStatus =
            serde_json::from_value(status_value).expect("stale status ignores additive identity");
        assert_eq!(stale.compatibility.protocol_version, 9);
        assert_eq!(stale.host_id, "hub");
        assert_eq!(stale.schema_version, 1);
    }

    #[test]
    fn additive_session_type_definition_read_rides_the_conformance_floor() {
        // `ensure_compatible` compares protocol version with exact equality and
        // conformance revision with a floor. Protocol 9 is a cold cut, so the
        // default floor equals the current revision.
        assert_eq!(PROTOCOL_VERSION, 9);
        assert_eq!(CONFORMANCE_FIXTURE_REVISION, 49);
        assert_eq!(DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION, 49);
        assert_eq!(
            current_feature_list(),
            vec![
                FEATURE_SESSIONS,
                FEATURE_PLUGIN_SURFACE_RENDER,
                FEATURE_PLUGIN_SURFACE_ACTION,
                FEATURE_PACKAGE_ROUTES,
                FEATURE_PACKAGE_NAVIGATION,
                FEATURE_SPAWN_TARGETS,
                FEATURE_WORKTREES,
                FEATURE_TERMINAL_READBACK,
                FEATURE_SESSION_ENTITY_SUBSCRIPTIONS,
                FEATURE_SESSION_TYPE_ENTITY_SUBSCRIPTIONS,
                FEATURE_PLUGIN_ENTITY_SUBSCRIPTIONS,
                FEATURE_HUB_SOURCE_UPDATE,
                FEATURE_UNIX_TERMINAL_ADAPTER,
                FEATURE_TERMINAL_SUBSCRIPTION_CLOSED,
                FEATURE_WEBRTC_TERMINAL_ADAPTER,
                FEATURE_ATTACH_OCCUPANCY,
                FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS,
            ],
            "the daemon advertises host-plane capabilities only",
        );
        assert_eq!(
            default_required_feature_list(),
            vec![
                FEATURE_SESSIONS,
                FEATURE_PLUGIN_SURFACE_RENDER,
                FEATURE_PLUGIN_SURFACE_ACTION,
                FEATURE_PACKAGE_ROUTES,
                FEATURE_PACKAGE_NAVIGATION,
                FEATURE_SPAWN_TARGETS,
                FEATURE_WORKTREES,
                FEATURE_TERMINAL_READBACK,
                FEATURE_SESSION_ENTITY_SUBSCRIPTIONS,
                FEATURE_SESSION_TYPE_ENTITY_SUBSCRIPTIONS,
                FEATURE_PLUGIN_ENTITY_SUBSCRIPTIONS,
            ],
            "the default client requirement excludes optional capabilities",
        );

        let pinned_at_forty_nine = DaemonCompatibilityRequirement {
            minimum_conformance_fixture_revision: 49,
            ..DaemonCompatibilityRequirement::current()
        };
        ensure_compatible(&pinned_at_forty_nine, &DaemonCompatibility::current())
            .expect("a protocol-9 client pinned at conformance 49 accepts a revision-49 Hub");

        assert_eq!(
            daemon_request_tag(&DaemonRequest::ShowSessionTypeDefinition {
                session_type_id: "init".to_string(),
            }),
            "show_session_type_definition"
        );
        assert_eq!(
            daemon_response_kind_tag(DaemonResponseKind::SessionTypeDefinition),
            "session_type_definition"
        );
    }
}
