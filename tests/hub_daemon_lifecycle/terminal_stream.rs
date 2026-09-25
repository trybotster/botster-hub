//! Version 9 terminal-route helpers shared by the lifecycle proofs.
//!
//! Hub delivers terminal data as binary route frames on the terminal
//! container, never as host events. These helpers decode one frame into a
//! typed [`RouteEvent`], remember the generation Core minted for each route
//! the test attached, and read raw sockets with the length-prefixed reader.

use std::collections::BTreeMap;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use botster_hub_client::{
    ClientFrame, DaemonCompatibilityRequirement, DaemonEndpoint, DaemonEvent, DaemonRequest,
    DaemonResponse, DaemonTransportResult, DaemonUnixFrameReader, DaemonUnixMuxFrame,
    DaemonUnixTerminalFrame, RequestIdSequence, ServerFrame, connect_and_hello_with_requirement,
    write_client_frame, write_unix_terminal_frame,
};
pub(crate) use botster_hub_test_support::unix_route::{
    RouteEvent, RouteOperationIds, UnixRouteClient, bytes_contain, decode_route_event,
    encode_input_with_operation_id,
};
use botster_terminal_protocol::{TerminalFrame, TerminalKind};
pub(crate) use botster_terminal_protocol_client::TerminalEvent;
use botster_terminal_protocol_client::TerminalInputCommand;

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
    /// Entity frames that arrived while a request waited for its response.
    pub(crate) entity_frames: Vec<botster_hub_client::DaemonEntityFrame>,
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
            entity_frames: Vec::new(),
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
                DaemonUnixMuxFrame::Server(ServerFrame::Entity { entity }) => {
                    self.entity_frames.push(entity);
                }
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
    pub(crate) fn send_terminal_input(&mut self, route: &str, input: &TerminalInputCommand) -> u64 {
        let generation = self.route_generation(route);
        let operation_id = self.operation_ids.assign(route, input);
        write_unix_terminal_frame(
            &mut self.stream,
            route,
            generation,
            0,
            &encode_input_with_operation_id(input, operation_id),
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
