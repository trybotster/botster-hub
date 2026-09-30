//! Version 9 terminal-route helpers shared by the lifecycle proofs.
//!
//! Hub delivers terminal data as binary route frames on the terminal
//! container, never as host events. These helpers decode one frame into a
//! typed [`RouteEvent`], remember the generation Core minted for each route
//! the test attached, and read raw sockets with the length-prefixed reader.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use botster_hub_client::{
    ClientFrame, DaemonCompatibilityRequirement, DaemonEndpoint, DaemonEvent, DaemonRequest,
    DaemonResponse, DaemonTransportError, DaemonTransportResult, DaemonUnixFrameReader,
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

/// How long `read_frame` waits on one route socket before trying the next.
const ROUTE_READ_SLICE: Duration = Duration::from_millis(2);

/// One frame read from the control socket or a route socket.
pub(crate) enum RawFrame {
    Server(ServerFrame),
    Terminal(DaemonUnixTerminalFrame),
}

/// One socket and the reader that keeps its partial frame across timeouts.
struct RawSocket {
    stream: UnixStream,
    frames: DaemonUnixFrameReader,
    /// A route socket carries terminal frames; the control socket, server frames.
    route: bool,
}

impl RawSocket {
    fn control(stream: UnixStream) -> Self {
        Self {
            stream,
            frames: DaemonUnixFrameReader::new(),
            route: false,
        }
    }

    fn route(stream: UnixStream) -> Self {
        Self {
            route: true,
            ..Self::control(stream)
        }
    }

    /// One frame within `timeout` (`None` waits), or `Ok(None)` on timeout.
    fn read_within(
        &mut self,
        timeout: Option<Duration>,
    ) -> DaemonTransportResult<Option<RawFrame>> {
        // macOS refuses a timeout on a socket the Hub already shut down; such
        // a socket never blocks, so its buffered frames are still readable.
        if let Err(error) = self.stream.set_read_timeout(timeout)
            && error.raw_os_error() != Some(22)
        {
            return Err(DaemonTransportError::Io(error));
        }
        let read = if self.route {
            self.frames
                .read_terminal_frame(&mut self.stream)
                .map(RawFrame::Terminal)
        } else {
            self.frames
                .read_frame(&mut self.stream)
                .map(RawFrame::Server)
        };
        match read {
            Ok(frame) => Ok(Some(frame)),
            Err(DaemonTransportError::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            // End of stream inside a frame: the Hub closed the socket while a
            // frame this client had not read was half written.
            Err(DaemonTransportError::Protocol(message))
                if message.starts_with("truncated unix frame") =>
            {
                Err(DaemonTransportError::ClientDisconnected)
            }
            Err(error) => Err(error),
        }
    }
}

/// A raw socket client for proofs that read every frame themselves.
///
/// The control socket carries requests, responses, events, and entities;
/// each attached route has its own socket for terminal frames. Nothing is
/// read until a proof asks, so a proof that never reads leaves every socket
/// to the kernel buffer. `read_frame` takes the next frame from any socket.
pub(crate) struct RawUnixClient {
    control: RawSocket,
    route_sockets: BTreeMap<String, RawSocket>,
    read_timeout: Cell<Option<Duration>>,
    /// Connect the route socket each Attach names. Off, the route socket
    /// is never connected.
    connect_routes: bool,
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
            control: RawSocket::control(stream),
            route_sockets: BTreeMap::new(),
            read_timeout: Cell::new(None),
            connect_routes: true,
            ids: RequestIdSequence::new(),
            routes: BTreeMap::new(),
            operation_ids: RouteOperationIds::default(),
            entity_frames: Vec::new(),
        }
    }

    /// Never connect the route sockets that Attach responses name.
    pub(crate) fn without_route_sockets(mut self) -> Self {
        self.connect_routes = false;
        self
    }

    /// Close this client's end of `route`'s socket only.
    pub(crate) fn close_route_socket(&mut self, route: &str) {
        self.route_sockets.remove(route);
    }

    /// End the control connection and leave the route sockets open.
    pub(crate) fn close_control(&mut self) {
        let _ = self.control.stream.shutdown(std::net::Shutdown::Both);
    }

    /// Whether `route`'s socket reaches end of stream within `timeout`,
    /// discarding any frames that arrive first. A socket this client already
    /// saw end (and dropped) counts as ended.
    pub(crate) fn route_socket_reaches_eof(&mut self, route: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let Some(socket) = self.route_sockets.get_mut(route) else {
            return self.routes.contains_key(route);
        };
        while Instant::now() < deadline {
            match socket.read_within(Some(Duration::from_millis(100))) {
                Ok(_) => {}
                Err(DaemonTransportError::ClientDisconnected) => return true,
                Err(error) => panic!("route socket {route} failed: {error}"),
            }
        }
        false
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) {
        self.read_timeout.set(timeout);
    }

    /// Write one correlated request and return its id.
    pub(crate) fn write_request(&mut self, request: &DaemonRequest) -> u64 {
        let request_id = self.ids.next();
        write_client_frame(
            &mut self.control.stream,
            &ClientFrame::Request {
                request_id: request_id.to_string(),
                request: request.clone(),
            },
        )
        .expect("write request");
        request_id
    }

    /// Read one frame of any kind from the control socket or any route
    /// socket. A route socket that ended is dropped. A timeout is an error.
    pub(crate) fn read_frame(&mut self) -> DaemonTransportResult<RawFrame> {
        let deadline = self
            .read_timeout
            .get()
            .map(|timeout| Instant::now() + timeout);
        loop {
            let remaining =
                deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
            if self.route_sockets.is_empty() {
                // Nothing to interleave: wait on the control socket alone.
                return match self
                    .control
                    .read_within(remaining.map(|r| r.max(Duration::from_millis(1))))?
                {
                    Some(frame) => Ok(frame),
                    None => Err(timed_out()),
                };
            }
            if let Some(frame) = self.control.read_within(Some(ROUTE_READ_SLICE))? {
                return Ok(frame);
            }
            let mut ended = Vec::new();
            let mut found = None;
            for (route, socket) in &mut self.route_sockets {
                match socket.read_within(Some(ROUTE_READ_SLICE)) {
                    Ok(Some(frame)) => {
                        found = Some(frame);
                        break;
                    }
                    Ok(None) => {}
                    Err(DaemonTransportError::ClientDisconnected) => ended.push(route.clone()),
                    Err(error) => return Err(error),
                }
            }
            for route in ended {
                self.route_sockets.remove(&route);
            }
            if let Some(frame) = found {
                return Ok(frame);
            }
            if remaining.is_some_and(|remaining| remaining.is_zero()) {
                return Err(timed_out());
            }
        }
    }

    /// Move every terminal frame already waiting on a route socket into
    /// `frames`. A response on the control socket no longer travels behind
    /// the terminal frames that preceded it, so a proof that collects frames
    /// while it requests must sweep the route sockets itself.
    fn drain_route_sockets(&mut self, frames: &mut Vec<DaemonUnixTerminalFrame>) {
        let mut ended = Vec::new();
        for (route, socket) in &mut self.route_sockets {
            loop {
                match socket.read_within(Some(Duration::from_millis(1))) {
                    Ok(Some(RawFrame::Terminal(frame))) => frames.push(frame),
                    Ok(Some(RawFrame::Server(_))) => {
                        panic!("control frame on route socket {route}")
                    }
                    Ok(None) => break,
                    Err(DaemonTransportError::ClientDisconnected) => {
                        ended.push(route.clone());
                        break;
                    }
                    Err(error) => panic!("route socket {route} failed: {error}"),
                }
            }
        }
        for route in ended {
            self.route_sockets.remove(&route);
        }
    }

    fn note_attach(&mut self, response: &DaemonResponse) {
        if let Some(attach) = &response.terminal_attach {
            self.routes
                .insert(attach.subscription_id.clone(), attach.generation);
            if self.connect_routes
                && let Some(path) = &attach.route_socket
            {
                let stream = UnixStream::connect(path).expect("connect route socket");
                self.route_sockets
                    .insert(attach.subscription_id.clone(), RawSocket::route(stream));
            }
        }
    }

    pub(crate) fn route_generation(&self, route: &str) -> u64 {
        *self
            .routes
            .get(route)
            .unwrap_or_else(|| panic!("route {route} was not attached on this raw client"))
    }

    fn route_socket_mut(&mut self, route: &str) -> &mut UnixStream {
        &mut self
            .route_sockets
            .get_mut(route)
            .unwrap_or_else(|| panic!("route {route} has no open route socket"))
            .stream
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
                RawFrame::Server(ServerFrame::Response {
                    request_id: answered,
                    response,
                }) => {
                    assert_eq!(
                        answered,
                        request_id.to_string(),
                        "response must correlate with the outstanding request"
                    );
                    self.note_attach(&response);
                    self.drain_route_sockets(frames);
                    return response;
                }
                RawFrame::Terminal(frame) => frames.push(frame),
                RawFrame::Server(ServerFrame::Event { event }) => events.push(event),
                RawFrame::Server(ServerFrame::Entity { entity }) => {
                    self.entity_frames.push(entity);
                }
                RawFrame::Server(ServerFrame::Close { reason }) => {
                    panic!("hub closed the connection before the response: {reason:?}")
                }
                RawFrame::Server(ServerFrame::HelloAck { .. }) => {
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
                Ok(RawFrame::Terminal(frame)) => frames.push(frame),
                Ok(RawFrame::Server(ServerFrame::Response { response, .. })) => {
                    panic!("unsolicited mux wait received a control response: {response:?}")
                }
                Ok(RawFrame::Server(_)) => {}
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
                Ok(RawFrame::Terminal(frame)) => frames.push(frame),
                Ok(RawFrame::Server(ServerFrame::Response { response, .. })) => {
                    panic!("unsolicited terminal wait received a control response: {response:?}")
                }
                Ok(RawFrame::Server(_)) | Err(_) => {}
            }
        }
        self.set_read_timeout(None);
    }

    /// Send one input command on an attached route and return its operation id.
    pub(crate) fn send_terminal_input(&mut self, route: &str, input: &TerminalInputCommand) -> u64 {
        let generation = self.route_generation(route);
        let operation_id = self.operation_ids.assign(route, input);
        write_unix_terminal_frame(
            self.route_socket_mut(route),
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
        write_unix_terminal_frame(self.route_socket_mut(route), route, generation, 0, body)
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

fn timed_out() -> DaemonTransportError {
    DaemonTransportError::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "raw client read timed out",
    ))
}
