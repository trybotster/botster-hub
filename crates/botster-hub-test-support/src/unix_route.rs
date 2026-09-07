//! Muxed Unix client for scheme 2 terminal route tests.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use botster_core_test_support::route_observer::RouteObserver;
use botster_hub_client::{
    DaemonCompatibilityRequirement, DaemonConnection, DaemonEndpoint, DaemonRequest,
    DaemonResponse, DaemonResponseKind, DaemonTransportResult, DaemonUnixTerminalFrame,
};
use botster_terminal_protocol::{AttachStateCode, InputOutcome, TerminalFrame};
use botster_terminal_protocol_client::{
    TerminalEvent, TerminalInputCommand, decode_terminal_event, encode_terminal_input,
};

/// One decoded scheme 2 frame with its Unix route header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteEvent {
    pub route: String,
    pub generation: u64,
    pub stream_epoch: u32,
    pub event: TerminalEvent,
}

impl RouteEvent {
    #[must_use]
    pub fn on_route(&self, route: &str) -> bool {
        self.route == route
    }

    #[must_use]
    pub fn attach_state(&self) -> Option<AttachStateCode> {
        match self.event {
            TerminalEvent::AttachState(state) => Some(state),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_attached(&self) -> bool {
        self.attach_state() == Some(AttachStateCode::Attached)
    }

    #[must_use]
    pub fn is_attach_failed(&self) -> bool {
        self.attach_state() == Some(AttachStateCode::Failed)
    }

    #[must_use]
    pub fn is_process_exit(&self) -> bool {
        matches!(self.event, TerminalEvent::ProcessExit(_))
    }

    #[must_use]
    pub fn is_snapshot_ready(&self) -> bool {
        matches!(self.event, TerminalEvent::SnapshotReady(_))
    }

    #[must_use]
    pub fn is_snapshot_history(&self) -> bool {
        matches!(self.event, TerminalEvent::SnapshotHistory(_))
    }

    #[must_use]
    pub fn is_snapshot_finish(&self) -> bool {
        matches!(self.event, TerminalEvent::SnapshotFinish)
    }

    #[must_use]
    pub fn snapshot_bytes(&self) -> Option<&[u8]> {
        match &self.event {
            TerminalEvent::SnapshotReady(frame) | TerminalEvent::SnapshotHistory(frame) => {
                Some(frame.body())
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn output(&self) -> Option<&[u8]> {
        match &self.event {
            TerminalEvent::Output(frame) => Some(frame.body()),
            _ => None,
        }
    }

    #[must_use]
    pub fn output_contains(&self, marker: &str) -> bool {
        self.output()
            .is_some_and(|bytes| bytes_contain(bytes, marker.as_bytes()))
    }
}

#[must_use]
pub fn bytes_contain(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Decode one Unix terminal frame through the shared client codec.
#[must_use]
pub fn decode_route_event(frame: &DaemonUnixTerminalFrame) -> Option<RouteEvent> {
    let decoded = TerminalFrame::from_bytes(&frame.body).ok()?;
    let event = decode_terminal_event(&decoded).ok()?;
    Some(RouteEvent {
        route: frame.route.clone(),
        generation: frame.generation,
        stream_epoch: frame.stream_epoch,
        event,
    })
}

const MAX_ABANDONED_INPUTS: usize = 32;

/// One unresolved operation that a successful detach made unobservable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbandonedInput {
    pub route: String,
    pub generation: u64,
    pub operation_id: u64,
    pub outcome: InputOutcome,
}

/// Per-route operation identifiers for scheme 2 input commands.
#[derive(Debug, Default)]
pub struct RouteOperationIds {
    last: BTreeMap<String, u64>,
    active_paste: BTreeMap<String, u64>,
}

impl RouteOperationIds {
    fn next(&mut self, route: &str) -> u64 {
        let last = self.last.entry(route.to_string()).or_insert(0);
        *last += 1;
        *last
    }

    /// Return the identifier that `input` must carry on `route`.
    pub fn assign(&mut self, route: &str, input: &TerminalInputCommand) -> u64 {
        match input {
            TerminalInputCommand::PasteBegin { .. } => {
                let id = self.next(route);
                self.active_paste.insert(route.to_string(), id);
                id
            }
            TerminalInputCommand::PasteChunk { .. } => self.active_paste_id(route),
            TerminalInputCommand::PasteCommit { .. } | TerminalInputCommand::PasteAbort { .. } => {
                let id = self.active_paste_id(route);
                self.active_paste.remove(route);
                id
            }
            _ => self.next(route),
        }
    }

    fn active_paste_id(&self, route: &str) -> u64 {
        *self
            .active_paste
            .get(route)
            .unwrap_or_else(|| panic!("no paste transaction is open on route {route}"))
    }

    fn remove_route(&mut self, route: &str) {
        self.last.remove(route);
        self.active_paste.remove(route);
    }
}

/// Encode a command after the route owner assigns its operation identifier.
#[must_use]
pub fn encode_input_with_operation_id(input: &TerminalInputCommand, operation_id: u64) -> Vec<u8> {
    let input = match input {
        TerminalInputCommand::RawBytes { data, .. } => TerminalInputCommand::RawBytes {
            operation_id,
            data: data.clone(),
        },
        TerminalInputCommand::Key {
            action,
            key,
            mods,
            consumed_mods,
            composing,
            unshifted_codepoint,
            text,
            ..
        } => TerminalInputCommand::Key {
            operation_id,
            action: *action,
            key: *key,
            mods: *mods,
            consumed_mods: *consumed_mods,
            composing: *composing,
            unshifted_codepoint: *unshifted_codepoint,
            text: text.clone(),
        },
        TerminalInputCommand::Mouse {
            action,
            button,
            mods,
            col,
            row,
            x_px,
            y_px,
            ..
        } => TerminalInputCommand::Mouse {
            operation_id,
            action: *action,
            button: *button,
            mods: *mods,
            col: *col,
            row: *row,
            x_px: *x_px,
            y_px: *y_px,
        },
        TerminalInputCommand::Focus { focused, .. } => TerminalInputCommand::Focus {
            operation_id,
            focused: *focused,
        },
        TerminalInputCommand::Resize {
            rows,
            cols,
            width_px,
            height_px,
            ..
        } => TerminalInputCommand::Resize {
            operation_id,
            rows: *rows,
            cols: *cols,
            width_px: *width_px,
            height_px: *height_px,
        },
        TerminalInputCommand::PasteBegin {
            total_len,
            allow_unsafe,
            ..
        } => TerminalInputCommand::PasteBegin {
            operation_id,
            total_len: *total_len,
            allow_unsafe: *allow_unsafe,
        },
        TerminalInputCommand::PasteChunk { index, data, .. } => TerminalInputCommand::PasteChunk {
            operation_id,
            index: *index,
            data: data.clone(),
        },
        TerminalInputCommand::PasteCommit { .. } => {
            TerminalInputCommand::PasteCommit { operation_id }
        }
        TerminalInputCommand::PasteAbort { .. } => {
            TerminalInputCommand::PasteAbort { operation_id }
        }
    };
    encode_terminal_input(&input)
        .expect("test input command encodes")
        .into_bytes()
}

/// A Unix connection that tracks each attached route and its observer.
pub struct UnixRouteClient {
    inner: DaemonConnection,
    routes: BTreeMap<String, u64>,
    operation_ids: RouteOperationIds,
    observers: BTreeMap<String, RouteObserver>,
    abandoned_inputs: VecDeque<AbandonedInput>,
    abandoned_input_count: u64,
    pending_events: VecDeque<RouteEvent>,
}

impl UnixRouteClient {
    pub fn connect(endpoint: &DaemonEndpoint) -> DaemonTransportResult<Self> {
        DaemonConnection::connect(endpoint).map(Self::from_connection)
    }

    pub fn connect_with_requirement(
        endpoint: &DaemonEndpoint,
        requirement: &DaemonCompatibilityRequirement,
    ) -> DaemonTransportResult<Self> {
        DaemonConnection::connect_with_requirement(endpoint, requirement).map(Self::from_connection)
    }

    fn from_connection(inner: DaemonConnection) -> Self {
        Self {
            inner,
            routes: BTreeMap::new(),
            operation_ids: RouteOperationIds::default(),
            observers: BTreeMap::new(),
            abandoned_inputs: VecDeque::with_capacity(MAX_ABANDONED_INPUTS),
            abandoned_input_count: 0,
            pending_events: VecDeque::new(),
        }
    }

    pub fn request(&mut self, request: &DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
        let response = self.inner.request(request)?;
        self.note_attach(&response);
        self.capture_skipped_route_events();
        self.note_detach(request, &response);
        Ok(response)
    }

    fn note_attach(&mut self, response: &DaemonResponse) {
        if let Some(attach) = &response.terminal_attach {
            self.routes
                .insert(attach.subscription_id.clone(), attach.generation);
            self.observers.insert(
                attach.subscription_id.clone(),
                RouteObserver::new(
                    "hub",
                    &attach.session_id,
                    &attach.subscription_id,
                    attach.generation,
                ),
            );
        }
    }

    fn note_detach(&mut self, request: &DaemonRequest, response: &DaemonResponse) {
        if let DaemonRequest::Detach {
            subscription_id, ..
        } = request
            && response.kind == DaemonResponseKind::Events
        {
            let generation = self.routes.remove(subscription_id);
            if let Some(observer) = self.observers.remove(subscription_id) {
                let generation = generation.unwrap_or_else(|| observer.generation());
                for operation_id in observer.state().outstanding() {
                    self.abandoned_input_count = self.abandoned_input_count.saturating_add(1);
                    if self.abandoned_inputs.len() == MAX_ABANDONED_INPUTS {
                        self.abandoned_inputs.pop_front();
                    }
                    self.abandoned_inputs.push_back(AbandonedInput {
                        route: subscription_id.clone(),
                        generation,
                        operation_id,
                        outcome: InputOutcome::OutcomeUnknown,
                    });
                }
            }
            self.operation_ids.remove_route(subscription_id);
        }
    }

    fn capture_skipped_route_events(&mut self) {
        for frame in self.inner.take_skipped_terminal() {
            let event = self.decode_and_observe(frame);
            self.pending_events.push_back(event);
        }
    }

    #[must_use]
    pub fn route_generation(&self, route: &str) -> u64 {
        *self.routes.get(route).unwrap_or_else(|| {
                panic!(
                    "route {route} was not attached on this connection; abandoned_input_count={} abandoned_inputs={:?}",
                    self.abandoned_input_count,
                    self.abandoned_inputs
                )
        })
    }

    #[must_use]
    pub fn observer(&self, route: &str) -> Option<&RouteObserver> {
        self.observers.get(route)
    }

    pub fn observer_mut(&mut self, route: &str) -> Option<&mut RouteObserver> {
        self.observers.get_mut(route)
    }

    /// Unresolved operations that recent successful detaches made unknown.
    pub fn abandoned_inputs(&self) -> impl Iterator<Item = &AbandonedInput> {
        self.abandoned_inputs.iter()
    }

    /// Total unresolved operations abandoned during this client's lifetime.
    #[must_use]
    pub const fn abandoned_input_count(&self) -> u64 {
        self.abandoned_input_count
    }

    /// Send one command and return its route-scoped operation identifier.
    pub fn send_terminal_frame(
        &mut self,
        route: &str,
        input: &TerminalInputCommand,
    ) -> DaemonTransportResult<u64> {
        let generation = self.route_generation(route);
        let operation_id = self.operation_ids.assign(route, input);
        let abandoned_input_count = self.abandoned_input_count;
        let abandoned_inputs = self.abandoned_inputs.clone();
        if !input.continues_paste()
            && let Some(observer) = self.observers.get_mut(route)
        {
            observer
                .expect_result(operation_id)
                .unwrap_or_else(|failure| {
                    panic!(
                        "{failure} abandoned_input_count={abandoned_input_count} abandoned_inputs={abandoned_inputs:?}"
                    )
                });
        }
        self.inner.send_terminal_frame(
            route,
            generation,
            0,
            &encode_input_with_operation_id(input, operation_id),
        )?;
        Ok(operation_id)
    }

    pub fn send_terminal_bytes_at_generation(
        &mut self,
        route: &str,
        generation: u64,
        body: &[u8],
    ) -> DaemonTransportResult<()> {
        self.inner.send_terminal_frame(route, generation, 0, body)
    }

    pub fn poll_route_events(&mut self, timeout: Duration) -> Vec<RouteEvent> {
        let mut events = self.pending_events.drain(..).collect::<Vec<_>>();
        if let Some(frame) = self.inner.poll_terminal(timeout).unwrap_or_else(|error| {
                panic!(
                    "failed to poll Unix route events: {error}; abandoned_input_count={} abandoned_inputs={:?}",
                    self.abandoned_input_count,
                    self.abandoned_inputs
                )
        }) {
            events.push(self.decode_and_observe(frame));
        }
        self.capture_skipped_route_events();
        events.extend(self.pending_events.drain(..));
        events
    }

    fn decode_and_observe(&mut self, frame: DaemonUnixTerminalFrame) -> RouteEvent {
        let abandoned_input_count = self.abandoned_input_count;
        let abandoned_inputs = self.abandoned_inputs.clone();
        let route = frame.route.clone();
        let decoded = TerminalFrame::from_bytes(&frame.body).unwrap_or_else(|error| {
            if let Some(observer) = self.observers.get(&route) {
                panic!(
                    "{} abandoned_input_count={abandoned_input_count} abandoned_inputs={abandoned_inputs:?}",
                    observer.failure("decode", error.to_string())
                );
            }
            panic!(
                "undecodable terminal body on unattached route {route}: {error}; abandoned_input_count={abandoned_input_count} abandoned_inputs={abandoned_inputs:?}"
            );
        });
        let event = decode_terminal_event(&decoded).unwrap_or_else(|error| {
            if let Some(observer) = self.observers.get(&route) {
                panic!(
                    "{} abandoned_input_count={abandoned_input_count} abandoned_inputs={abandoned_inputs:?}",
                    observer.failure("decode", error.to_string())
                );
            }
            panic!(
                "undecodable terminal event on unattached route {route}: {error}; abandoned_input_count={abandoned_input_count} abandoned_inputs={abandoned_inputs:?}"
            );
        });
        if let Some(observer) = self.observers.get_mut(&route)
            && frame.generation == observer.generation()
        {
            observer
                .observe(frame.stream_epoch, &decoded)
                .unwrap_or_else(|failure| {
                    panic!(
                        "{failure} abandoned_input_count={abandoned_input_count} abandoned_inputs={abandoned_inputs:?}"
                    )
                });
        }
        RouteEvent {
            route,
            generation: frame.generation,
            stream_epoch: frame.stream_epoch,
            event,
        }
    }
}
