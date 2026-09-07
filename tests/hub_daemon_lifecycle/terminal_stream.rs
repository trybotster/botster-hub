//! Version 9 terminal-route helpers shared by the lifecycle proofs.
//!
//! Hub delivers terminal data as binary route frames on the terminal
//! container, never as host events. These helpers decode one frame into a
//! typed [`RouteEvent`], remember the generation Core minted for each route
//! the test attached, and read raw sockets with the length-prefixed reader.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use super::common::InputSpec;
use botster_hub_client::{
    ClientFrame, DaemonCompatibilityRequirement, DaemonConnection, DaemonEndpoint, DaemonEvent,
    DaemonRequest, DaemonResponse, DaemonTransportResult, DaemonUnixFrameReader,
    DaemonUnixMuxFrame, DaemonUnixTerminalFrame, RequestIdSequence, ServerFrame,
    connect_and_hello_with_requirement, write_client_frame, write_unix_terminal_frame,
};
use botster_terminal_protocol::{
    AttachStateCode, HistoryUnavailableReason, InputResultBody, ModesBody, RouteResyncBody,
    TerminalFrame, TerminalKind, decode_attach_state, decode_history_unavailable,
    decode_input_result, decode_modes, decode_process_exit, decode_route_resync,
};

/// One decoded terminal body, by Core `TerminalKind`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StreamBody {
    AttachState(AttachStateCode),
    Modes(ModesBody),
    SnapshotReady(Vec<u8>),
    SnapshotHistory(Vec<u8>),
    SnapshotFinish,
    Output(Vec<u8>),
    ProcessExit(Option<i32>),
    InputResult(InputResultBody),
    HistoryUnavailable(HistoryUnavailableReason),
    RouteResync(RouteResyncBody),
}

/// One terminal frame read from a route, with its routing header.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RouteEvent {
    pub(crate) route: String,
    pub(crate) generation: u64,
    pub(crate) stream_epoch: u32,
    pub(crate) body: StreamBody,
}

impl RouteEvent {
    pub(crate) fn on_route(&self, route: &str) -> bool {
        self.route == route
    }

    pub(crate) fn attach_state(&self) -> Option<AttachStateCode> {
        match self.body {
            StreamBody::AttachState(state) => Some(state),
            _ => None,
        }
    }

    pub(crate) fn is_attached(&self) -> bool {
        self.attach_state() == Some(AttachStateCode::Attached)
    }

    pub(crate) fn is_attach_failed(&self) -> bool {
        self.attach_state() == Some(AttachStateCode::Failed)
    }

    pub(crate) fn is_process_exit(&self) -> bool {
        matches!(self.body, StreamBody::ProcessExit(_))
    }

    pub(crate) fn is_snapshot_ready(&self) -> bool {
        matches!(self.body, StreamBody::SnapshotReady(_))
    }

    pub(crate) fn is_snapshot_history(&self) -> bool {
        matches!(self.body, StreamBody::SnapshotHistory(_))
    }

    pub(crate) fn is_snapshot_finish(&self) -> bool {
        matches!(self.body, StreamBody::SnapshotFinish)
    }

    /// READY or HISTORY payload bytes.
    pub(crate) fn snapshot_bytes(&self) -> Option<&[u8]> {
        match &self.body {
            StreamBody::SnapshotReady(bytes) | StreamBody::SnapshotHistory(bytes) => {
                Some(bytes.as_slice())
            }
            _ => None,
        }
    }

    pub(crate) fn output(&self) -> Option<&[u8]> {
        match &self.body {
            StreamBody::Output(bytes) => Some(bytes.as_slice()),
            _ => None,
        }
    }

    pub(crate) fn output_contains(&self, marker: &str) -> bool {
        self.output()
            .is_some_and(|bytes| bytes_contain(bytes, marker.as_bytes()))
    }
}

pub(crate) fn bytes_contain(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Decode one route frame. Frames whose body fails the protocol codec are
/// reported as `None`; a proof that needs the raw bytes reads the frame itself.
pub(crate) fn decode_route_event(frame: &DaemonUnixTerminalFrame) -> Option<RouteEvent> {
    let decoded = TerminalFrame::from_bytes(&frame.body).ok()?;
    let body = match decoded.kind() {
        TerminalKind::Output => StreamBody::Output(decoded.body().to_vec()),
        TerminalKind::SnapshotReady => StreamBody::SnapshotReady(decoded.body().to_vec()),
        TerminalKind::SnapshotHistory => StreamBody::SnapshotHistory(decoded.body().to_vec()),
        TerminalKind::SnapshotFinish => StreamBody::SnapshotFinish,
        TerminalKind::ProcessExit => {
            StreamBody::ProcessExit(decode_process_exit(&decoded).ok()?.code)
        }
        TerminalKind::Modes => StreamBody::Modes(decode_modes(&decoded).ok()?),
        TerminalKind::AttachState => StreamBody::AttachState(decode_attach_state(&decoded).ok()?),
        TerminalKind::InputResult => StreamBody::InputResult(decode_input_result(&decoded).ok()?),
        TerminalKind::HistoryUnavailable => {
            StreamBody::HistoryUnavailable(decode_history_unavailable(&decoded).ok()?)
        }
        TerminalKind::RouteResync => StreamBody::RouteResync(decode_route_resync(&decoded).ok()?),
    };
    Some(RouteEvent {
        route: frame.route.clone(),
        generation: frame.generation,
        stream_epoch: frame.stream_epoch,
        body,
    })
}

pub(crate) fn decode_route_events(frames: &[DaemonUnixTerminalFrame]) -> Vec<RouteEvent> {
    frames.iter().filter_map(decode_route_event).collect()
}

/// Concatenated OUTPUT bytes, optionally limited to one route.
pub(crate) fn concatenated_output(events: &[RouteEvent], route: Option<&str>) -> Vec<u8> {
    events
        .iter()
        .filter(|event| route.is_none_or(|route| event.on_route(route)))
        .filter_map(RouteEvent::output)
        .flatten()
        .copied()
        .collect()
}

pub(crate) fn events_contain_output(
    events: &[RouteEvent],
    route: Option<&str>,
    marker: &str,
) -> bool {
    events
        .iter()
        .filter(|event| route.is_none_or(|route| event.on_route(route)))
        .any(|event| event.output_contains(marker))
}

/// Raw frame bytes with the terminal container decoded, for proofs that
/// look at OUTPUT payloads without the protocol codec.
pub(crate) fn frame_output_bytes(frame: &DaemonUnixTerminalFrame) -> Option<Vec<u8>> {
    decode_route_event(frame).and_then(|event| event.output().map(<[u8]>::to_vec))
}

/// OUTPUT bytes of one plaintext terminal frame body (any transport).
pub(crate) fn terminal_body_output(bytes: &[u8]) -> Option<Vec<u8>> {
    let frame = TerminalFrame::from_bytes(bytes).ok()?;
    (frame.kind() == TerminalKind::Output).then(|| frame.body().to_vec())
}

/// Per-route input operation ids.
///
/// Ids are strictly increasing per route from 1. A paste transaction reuses
/// the BEGIN id on every CHUNK, COMMIT, and ABORT, which is how Core
/// correlates the transaction.
#[derive(Debug, Default)]
pub(crate) struct RouteOperationIds {
    last: BTreeMap<String, u64>,
    active_paste: BTreeMap<String, u64>,
}

impl RouteOperationIds {
    fn next(&mut self, route: &str) -> u64 {
        let last = self.last.entry(route.to_string()).or_insert(0);
        *last += 1;
        *last
    }

    /// The id `input` must carry on `route`.
    pub(crate) fn assign(&mut self, route: &str, input: &InputSpec) -> u64 {
        match input {
            InputSpec::PasteBegin { .. } => {
                let id = self.next(route);
                self.active_paste.insert(route.to_string(), id);
                id
            }
            InputSpec::PasteChunk { .. } => self.active_paste_id(route),
            InputSpec::PasteCommit | InputSpec::PasteAbort => {
                let id = self.active_paste_id(route);
                self.active_paste.remove(route);
                id
            }
            InputSpec::Raw(_) | InputSpec::Resize { .. } | InputSpec::Focus(_) => self.next(route),
        }
    }

    fn active_paste_id(&self, route: &str) -> u64 {
        *self
            .active_paste
            .get(route)
            .unwrap_or_else(|| panic!("no paste transaction is open on route {route}"))
    }
}

/// A client connection that remembers the generation of each route it attached.
///
/// Input frames need the fixed attachment generation Core minted at attach.
/// The wrapper records it from every `TerminalAttached` response so proofs
/// address a route by subscription id only.
pub(crate) struct LifecycleConnection {
    inner: DaemonConnection,
    routes: BTreeMap<String, u64>,
    operation_ids: RouteOperationIds,
}

impl LifecycleConnection {
    pub(crate) fn connect(endpoint: &DaemonEndpoint) -> DaemonTransportResult<Self> {
        DaemonConnection::connect(endpoint).map(Self::from_connection)
    }

    pub(crate) fn connect_with_requirement(
        endpoint: &DaemonEndpoint,
        requirement: &DaemonCompatibilityRequirement,
    ) -> DaemonTransportResult<Self> {
        DaemonConnection::connect_with_requirement(endpoint, requirement).map(Self::from_connection)
    }

    pub(crate) fn from_connection(inner: DaemonConnection) -> Self {
        Self {
            inner,
            routes: BTreeMap::new(),
            operation_ids: RouteOperationIds::default(),
        }
    }

    pub(crate) fn into_inner(self) -> DaemonConnection {
        self.inner
    }

    /// Send one request and record the route generation of an attach answer.
    pub(crate) fn request(
        &mut self,
        request: &DaemonRequest,
    ) -> DaemonTransportResult<DaemonResponse> {
        let response = self.inner.request(request)?;
        self.note_attach(&response);
        Ok(response)
    }

    fn note_attach(&mut self, response: &DaemonResponse) {
        if let Some(attach) = &response.terminal_attach {
            self.routes
                .insert(attach.subscription_id.clone(), attach.generation);
        }
    }

    /// Generation of a route this connection attached.
    pub(crate) fn route_generation(&self, route: &str) -> u64 {
        *self
            .routes
            .get(route)
            .unwrap_or_else(|| panic!("route {route} was not attached on this connection"))
    }

    /// Send one input command on an attached route. Input carries stream epoch 0.
    pub(crate) fn send_terminal_frame(
        &mut self,
        route: &str,
        input: &InputSpec,
    ) -> DaemonTransportResult<()> {
        self.send_terminal_frame_with_id(route, input).map(|_| ())
    }

    /// Send one input command and return the operation id it carried.
    pub(crate) fn send_terminal_frame_with_id(
        &mut self,
        route: &str,
        input: &InputSpec,
    ) -> DaemonTransportResult<u64> {
        let generation = self.route_generation(route);
        let operation_id = self.operation_ids.assign(route, input);
        self.inner
            .send_terminal_frame(route, generation, 0, &input.encode(operation_id))?;
        Ok(operation_id)
    }

    /// Send raw input body bytes with an explicit generation, for proofs of
    /// stale routes or malformed input.
    pub(crate) fn send_terminal_bytes_at_generation(
        &mut self,
        route: &str,
        generation: u64,
        body: &[u8],
    ) -> DaemonTransportResult<()> {
        self.inner.send_terminal_frame(route, generation, 0, body)
    }

    /// Decoded route events available within `timeout`, including frames the
    /// connection skipped while waiting for a response.
    pub(crate) fn poll_route_events(&mut self, timeout: Duration) -> Vec<RouteEvent> {
        let mut events = Vec::new();
        if let Ok(Some(frame)) = self.inner.poll_terminal(timeout)
            && let Some(event) = decode_route_event(&frame)
        {
            events.push(event);
        }
        for frame in self.inner.take_skipped_terminal() {
            if let Some(event) = decode_route_event(&frame) {
                events.push(event);
            }
        }
        events
    }
}

impl Deref for LifecycleConnection {
    type Target = DaemonConnection;

    fn deref(&self) -> &DaemonConnection {
        &self.inner
    }
}

impl DerefMut for LifecycleConnection {
    fn deref_mut(&mut self) -> &mut DaemonConnection {
        &mut self.inner
    }
}

/// A raw socket client for proofs that read every frame themselves.
///
/// One socket carries both directions; the length-prefixed reader keeps a
/// partial frame across read timeouts.
pub(crate) struct RawUnixClient {
    stream: UnixStream,
    frames: DaemonUnixFrameReader,
    ids: RequestIdSequence,
    routes: BTreeMap<String, u64>,
    operation_ids: RouteOperationIds,
}

impl RawUnixClient {
    /// Connect with the Unix terminal adapter requirement and complete Hello.
    pub(crate) fn connect_unix_terminal_adapter(endpoint: &DaemonEndpoint) -> Self {
        let stream = connect_and_hello_with_requirement(
            endpoint,
            &DaemonCompatibilityRequirement::for_unix_terminal_adapter(),
        )
        .expect("unix adapter hello");
        Self::from_stream(stream)
    }

    pub(crate) fn from_stream(stream: UnixStream) -> Self {
        Self {
            stream,
            frames: DaemonUnixFrameReader::new(),
            ids: RequestIdSequence::new(),
            routes: BTreeMap::new(),
            operation_ids: RouteOperationIds::default(),
        }
    }

    pub(crate) fn stream(&self) -> &UnixStream {
        &self.stream
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) {
        self.stream
            .set_read_timeout(timeout)
            .expect("set raw client read timeout");
    }

    /// Write one correlated request and return its id.
    pub(crate) fn write_request(&mut self, request: &DaemonRequest) -> u64 {
        let request_id = self.ids.next();
        write_client_frame(
            &mut self.stream,
            &ClientFrame::Request {
                request_id: request_id.to_string(),
                request: request.clone(),
            },
        )
        .expect("write request");
        request_id
    }

    /// Read one frame of any kind.
    pub(crate) fn read_frame(&mut self) -> DaemonTransportResult<DaemonUnixMuxFrame> {
        self.frames.read_frame(&mut self.stream)
    }

    fn note_attach(&mut self, response: &DaemonResponse) {
        if let Some(attach) = &response.terminal_attach {
            self.routes
                .insert(attach.subscription_id.clone(), attach.generation);
        }
    }

    pub(crate) fn route_generation(&self, route: &str) -> u64 {
        *self
            .routes
            .get(route)
            .unwrap_or_else(|| panic!("route {route} was not attached on this raw client"))
    }

    /// Send one request; collect terminal frames and host events that arrive
    /// before its correlated response.
    pub(crate) fn request_collecting(
        &mut self,
        request: &DaemonRequest,
        frames: &mut Vec<DaemonUnixTerminalFrame>,
        events: &mut Vec<DaemonEvent>,
    ) -> DaemonResponse {
        let request_id = self.write_request(request);
        loop {
            match self.read_frame().expect("read mux") {
                DaemonUnixMuxFrame::Server(ServerFrame::Response {
                    request_id: answered,
                    response,
                }) => {
                    assert_eq!(
                        answered,
                        request_id.to_string(),
                        "response must correlate with the outstanding request"
                    );
                    self.note_attach(&response);
                    return response;
                }
                DaemonUnixMuxFrame::Terminal(frame) => frames.push(frame),
                DaemonUnixMuxFrame::Server(ServerFrame::Event { event }) => events.push(event),
                DaemonUnixMuxFrame::Server(ServerFrame::Entity { .. }) => {}
                DaemonUnixMuxFrame::Server(ServerFrame::Close { reason }) => {
                    panic!("hub closed the connection before the response: {reason:?}")
                }
                DaemonUnixMuxFrame::Server(ServerFrame::HelloAck { .. }) => {
                    panic!("unexpected second hello ack")
                }
            }
        }
    }

    /// Send one request; keep terminal frames and drop host events.
    pub(crate) fn request_skipping(
        &mut self,
        request: &DaemonRequest,
        frames: &mut Vec<DaemonUnixTerminalFrame>,
    ) -> DaemonResponse {
        let mut events = Vec::new();
        self.request_collecting(request, frames, &mut events)
    }

    /// Read unsolicited frames until the socket is quiet for `timeout`.
    /// A control response here is a proof failure.
    pub(crate) fn poll_unsolicited(
        &mut self,
        timeout: Duration,
        frames: &mut Vec<DaemonUnixTerminalFrame>,
    ) {
        self.set_read_timeout(Some(timeout));
        loop {
            match self.read_frame() {
                Ok(DaemonUnixMuxFrame::Terminal(frame)) => frames.push(frame),
                Ok(DaemonUnixMuxFrame::Server(ServerFrame::Response { response, .. })) => {
                    panic!("unsolicited mux wait received a control response: {response:?}")
                }
                Ok(DaemonUnixMuxFrame::Server(_)) => {}
                Err(_) => break,
            }
        }
        self.set_read_timeout(None);
    }

    /// Read unsolicited terminal frames until `done` holds or `deadline` passes.
    pub(crate) fn read_terminal_until(
        &mut self,
        frames: &mut Vec<DaemonUnixTerminalFrame>,
        deadline: Instant,
        mut done: impl FnMut(&[DaemonUnixTerminalFrame]) -> bool,
    ) {
        self.set_read_timeout(Some(Duration::from_millis(200)));
        while Instant::now() < deadline && !done(frames) {
            match self.read_frame() {
                Ok(DaemonUnixMuxFrame::Terminal(frame)) => frames.push(frame),
                Ok(DaemonUnixMuxFrame::Server(ServerFrame::Response { response, .. })) => {
                    panic!("unsolicited terminal wait received a control response: {response:?}")
                }
                Ok(DaemonUnixMuxFrame::Server(_)) | Err(_) => {}
            }
        }
        self.set_read_timeout(None);
    }

    /// Send one input command on an attached route and return its operation id.
    pub(crate) fn send_terminal_input(&mut self, route: &str, input: &InputSpec) -> u64 {
        let generation = self.route_generation(route);
        let operation_id = self.operation_ids.assign(route, input);
        write_unix_terminal_frame(
            &mut self.stream,
            route,
            generation,
            0,
            &input.encode(operation_id),
        )
        .expect("write unix terminal frame");
        operation_id
    }

    /// Send raw input body bytes with an explicit generation, for proofs of
    /// stale routes or malformed input.
    pub(crate) fn send_terminal_bytes_at_generation(
        &mut self,
        route: &str,
        generation: u64,
        body: &[u8],
    ) {
        write_unix_terminal_frame(&mut self.stream, route, generation, 0, body)
            .expect("write unix terminal frame");
    }
}

/// Frames on `route` that decode as attached.
pub(crate) fn frames_attached(frames: &[DaemonUnixTerminalFrame], route: &str) -> bool {
    frames
        .iter()
        .filter(|frame| frame.route == route)
        .filter_map(decode_route_event)
        .any(|event| event.is_attached())
}

/// Frames on `route` that decode as process exit.
pub(crate) fn frames_process_exit(frames: &[DaemonUnixTerminalFrame], route: &str) -> bool {
    frames
        .iter()
        .filter(|frame| frame.route == route)
        .filter_map(decode_route_event)
        .any(|event| event.is_process_exit())
}

/// OUTPUT bytes across `frames` contain `marker`.
pub(crate) fn frames_contain_output(frames: &[DaemonUnixTerminalFrame], marker: &str) -> bool {
    frames
        .iter()
        .filter_map(frame_output_bytes)
        .any(|bytes| bytes_contain(&bytes, marker.as_bytes()))
}

/// Concatenated OUTPUT bytes across `frames`, in arrival order.
pub(crate) fn frames_output_bytes(frames: &[DaemonUnixTerminalFrame]) -> Vec<u8> {
    frames
        .iter()
        .filter_map(frame_output_bytes)
        .flatten()
        .collect()
}
