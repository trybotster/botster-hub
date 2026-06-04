//! Same-device daemon socket transport for the thin operator CLI.
//!
//! This module is a framing adapter over `HubClientApi`. The daemon owns one
//! mutable `HubRuntime` on the accept/control thread; socket threads submit discrete
//! requests and never hold runtime access while writing to a client.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

use botster_core::{
    RequestId, SessionId, SessionLifecycleState, SubscriptionId, TerminalAttachState,
};
use serde::{Deserialize, Serialize};

use crate::{
    HubClientApi, HubClientEvent, HubClientRequest, HubClientResponseBody, HubClientSession,
    HubConfig, HubDaemon, HubDaemonStatus, HubStateLoadSource, PackageRegistry,
};

const PROTOCOL: &str = "botster-hub-daemon-v1";
const ATTACH_DRAIN_INTERVAL: Duration = Duration::from_millis(25);

/// Run the local daemon socket until a shutdown request is received.
pub fn serve_daemon(config: HubConfig) -> DaemonTransportResult<HubDaemonStatus> {
    let socket_path = socket_path(&config)?;
    prepare_socket_path(&socket_path)?;
    let listener = UnixListener::bind(&socket_path).map_err(DaemonTransportError::Io)?;
    listener
        .set_nonblocking(true)
        .map_err(DaemonTransportError::Io)?;

    let (control_tx, control_rx) = mpsc::channel();
    let mut daemon = HubDaemon::start(config)?;
    let packages = daemon.package_registry().clone();
    let mut logical_clock = 1;
    let mut drain_cursors = BTreeMap::<String, u64>::new();

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let tx = control_tx.clone();
                thread::spawn(move || {
                    if let Err(error) = handle_connection(stream, tx) {
                        eprintln!("botster-hub daemon connection error: {error}");
                    }
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                while let Ok(message) = control_rx.try_recv() {
                    if handle_control_message(
                        &mut daemon,
                        &packages,
                        &mut logical_clock,
                        &mut drain_cursors,
                        message,
                    ) {
                        let status = daemon.stop();
                        cleanup_socket_path(&socket_path);
                        return Ok(status);
                    }
                }
                if !socket_path.exists() {
                    rebind_missing_socket_path(&socket_path);
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(DaemonTransportError::Io(error)),
        }
    }
}

/// Connect to a daemon and send one operator request.
pub fn request(
    config: &HubConfig,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let mut stream = connect_and_hello(config)?;
    write_frame(&mut stream, &request)?;
    read_frame(&mut stream)
}

/// Attach and stream terminal bytes until the session exits or the connection closes.
pub fn stream_attach(
    config: &HubConfig,
    session_id: SessionId,
    subscription_id: SubscriptionId,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    let mut stream = connect_and_hello(config)?;
    let session_id_value = session_id.0.clone();
    let subscription_id_value = subscription_id.0.clone();
    let result = stream_attach_connected(
        &mut stream,
        &session_id_value,
        &subscription_id_value,
        output,
    );
    detach_stream_subscription(&mut stream, &session_id_value, &subscription_id_value);
    result
}

fn stream_attach_connected(
    stream: &mut UnixStream,
    session_id: &str,
    subscription_id: &str,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    write_frame(
        stream,
        &DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
    )?;
    let response: DaemonResponse = read_frame(stream)?;
    write_terminal_events(&response.events, output)?;
    if response.events.iter().any(DaemonEvent::is_process_exit) {
        return Ok(());
    }
    let mut idle_drains = 0;

    loop {
        thread::sleep(ATTACH_DRAIN_INTERVAL);
        write_frame(
            stream,
            &DaemonRequest::Drain {
                session_id: session_id.to_string(),
            },
        )?;
        let response: DaemonResponse = read_frame(stream)?;
        if response.events.is_empty() {
            idle_drains += 1;
        } else {
            idle_drains = 0;
        }
        write_terminal_events(&response.events, output)?;
        if response.events.iter().any(DaemonEvent::is_process_exit) {
            return Ok(());
        }
        if idle_drains >= 20 {
            write_frame(stream, &DaemonRequest::ListSessions)?;
            let response: DaemonResponse = read_frame(stream)?;
            if response
                .sessions
                .iter()
                .any(|session| session.session_id == session_id && session.lifecycle == "exited")
            {
                return Ok(());
            }
            return Ok(());
        }
    }
}

fn detach_stream_subscription(stream: &mut UnixStream, session_id: &str, subscription_id: &str) {
    if write_frame(
        stream,
        &DaemonRequest::Detach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        },
    )
    .is_ok()
    {
        let _ = read_frame::<DaemonResponse>(stream);
    }
}

fn handle_connection(
    mut stream: UnixStream,
    control_tx: Sender<ControlMessage>,
) -> DaemonTransportResult<()> {
    stream
        .set_nonblocking(false)
        .map_err(DaemonTransportError::Io)?;
    let mut reader = BufReader::new(stream.try_clone().map_err(DaemonTransportError::Io)?);
    let hello: DaemonHello = read_frame_from_reader(&mut reader)?;
    if hello.protocol != PROTOCOL {
        return Err(DaemonTransportError::Protocol("unexpected hello protocol"));
    }
    write_frame(
        &mut stream,
        &DaemonHelloAck {
            protocol: PROTOCOL.to_string(),
        },
    )?;
    let mut attached_subscriptions = Vec::<AttachedSubscription>::new();

    loop {
        let request = match read_frame_from_reader::<DaemonRequest>(&mut reader) {
            Ok(request) => request,
            Err(DaemonTransportError::ClientDisconnected) => {
                detach_connection_subscriptions(&control_tx, &attached_subscriptions);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        let close_after_response = matches!(request, DaemonRequest::DaemonShutdown);
        let active_change = AttachedSubscriptionChange::from_request(&request);
        control_tx
            .send(ControlMessage::Request { request, reply_tx })
            .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        let response = reply_rx
            .recv()
            .map_err(|_| DaemonTransportError::ControlThreadStopped)??;
        apply_attached_subscription_change(&mut attached_subscriptions, active_change);
        if let Err(error) = write_frame(&mut stream, &response) {
            detach_connection_subscriptions(&control_tx, &attached_subscriptions);
            return Err(error);
        }
        if close_after_response {
            detach_connection_subscriptions(&control_tx, &attached_subscriptions);
            return Ok(());
        }
    }
}

fn detach_connection_subscriptions(
    control_tx: &Sender<ControlMessage>,
    attached_subscriptions: &[AttachedSubscription],
) {
    for subscription in attached_subscriptions {
        let (reply_tx, reply_rx) = mpsc::channel();
        if control_tx
            .send(ControlMessage::Request {
                request: DaemonRequest::Detach {
                    session_id: subscription.session_id.clone(),
                    subscription_id: subscription.subscription_id.clone(),
                },
                reply_tx,
            })
            .is_ok()
        {
            let _ = reply_rx.recv_timeout(Duration::from_secs(1));
        }
    }
}

fn apply_attached_subscription_change(
    attached_subscriptions: &mut Vec<AttachedSubscription>,
    active_change: Option<AttachedSubscriptionChange>,
) {
    match active_change {
        Some(AttachedSubscriptionChange::Attach(subscription)) => {
            if !attached_subscriptions.contains(&subscription) {
                attached_subscriptions.push(subscription);
            }
        }
        Some(AttachedSubscriptionChange::Detach(subscription)) => {
            attached_subscriptions.retain(|attached| attached != &subscription);
        }
        None => {}
    }
}

fn handle_control_message(
    daemon: &mut HubDaemon,
    packages: &PackageRegistry,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    message: ControlMessage,
) -> bool {
    let ControlMessage::Request { request, reply_tx } = message;
    let response = handle_control_request(daemon, packages, logical_clock, drain_cursors, request);
    let should_stop = matches!(
        response,
        Ok(DaemonResponse {
            kind: DaemonResponseKind::Shutdown,
            ..
        })
    );
    let _ = reply_tx.send(response);
    should_stop
}

fn handle_control_request(
    daemon: &mut HubDaemon,
    packages: &PackageRegistry,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let status = daemon.status();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };

    match request {
        DaemonRequest::Status => {
            let response = api.handle_request(
                runtime,
                packages,
                HubClientRequest::Status {
                    request_id: request_id("daemon-status"),
                },
            )?;
            let HubClientResponseBody::Status(client_status) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(DaemonResponse::status(status, client_status.session_count))
        }
        DaemonRequest::ListSessions => {
            let response = api.handle_request(
                runtime,
                packages,
                HubClientRequest::ListSessions {
                    request_id: request_id("daemon-sessions-list"),
                },
            )?;
            let HubClientResponseBody::Sessions(sessions) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(DaemonResponse::sessions(sessions))
        }
        DaemonRequest::Spawn {
            session_id,
            command,
        } => {
            let response = api.handle_request(
                runtime,
                packages,
                HubClientRequest::Spawn {
                    request_id: request_id("daemon-sessions-spawn"),
                    session_id: SessionId(session_id),
                    command,
                    now_seconds: tick(logical_clock),
                },
            )?;
            let HubClientResponseBody::Spawned(spawned) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            drain_cursors.insert(spawned.session.session_id.0.clone(), *logical_clock);
            Ok(DaemonResponse::spawned(
                spawned.session.into(),
                events_from_client(spawned.events),
            ))
        }
        DaemonRequest::Attach {
            session_id,
            subscription_id,
        } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                packages,
                HubClientRequest::Attach {
                    request_id: request_id("daemon-sessions-attach"),
                    session_id: SessionId(session_id),
                    subscription_id: SubscriptionId(subscription_id),
                    now_seconds: now,
                },
            )?;
            events_response(response.body)
        }
        DaemonRequest::Detach {
            session_id,
            subscription_id,
        } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                packages,
                HubClientRequest::Detach {
                    request_id: request_id("daemon-sessions-detach"),
                    session_id: SessionId(session_id),
                    subscription_id: SubscriptionId(subscription_id),
                    now_seconds: now,
                },
            )?;
            events_response(response.body)
        }
        DaemonRequest::SendInput { session_id, data } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                packages,
                HubClientRequest::Input {
                    request_id: request_id("daemon-sessions-send-input"),
                    session_id: SessionId(session_id),
                    data: data.into_bytes(),
                    now_seconds: now,
                },
            )?;
            events_response(response.body)
        }
        DaemonRequest::Resize {
            session_id,
            rows,
            cols,
        } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                packages,
                HubClientRequest::Resize {
                    request_id: request_id("daemon-sessions-resize"),
                    session_id: SessionId(session_id),
                    rows,
                    cols,
                    now_seconds: now,
                },
            )?;
            events_response(response.body)
        }
        DaemonRequest::Drain { session_id } => {
            let cursor = drain_cursors
                .entry(session_id.clone())
                .or_insert_with(|| tick(logical_clock));
            let response = api.handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("daemon-sessions-drain"),
                    session_id: SessionId(session_id),
                    last_output_at: *cursor,
                },
            )?;
            *cursor = tick(logical_clock);
            events_response(response.body)
        }
        DaemonRequest::DaemonShutdown => Ok(DaemonResponse {
            kind: DaemonResponseKind::Shutdown,
            status: Some(DaemonStatus::from_status(
                &status,
                runtime
                    .list_sessions()
                    .map_err(crate::HubRuntimeError::from)?
                    .len(),
            )),
            sessions: Vec::new(),
            events: Vec::new(),
        }),
    }
}

fn events_response(body: HubClientResponseBody) -> DaemonTransportResult<DaemonResponse> {
    let HubClientResponseBody::Events(events) = body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(DaemonResponse::events(events_from_client(events)))
}

fn request_id(value: &str) -> RequestId {
    RequestId(value.to_string())
}

fn tick(logical_clock: &mut u64) -> u64 {
    let current = *logical_clock;
    *logical_clock += 1;
    current
}

fn socket_path(config: &HubConfig) -> DaemonTransportResult<PathBuf> {
    config
        .transports
        .local_socket
        .as_ref()
        .map(|binding| binding.path.clone())
        .ok_or(DaemonTransportError::MissingSocketBinding)
}

fn prepare_socket_path(path: &PathBuf) -> DaemonTransportResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(DaemonTransportError::Io)?;
    }
    match UnixStream::connect(path) {
        Ok(mut stream) => {
            write_frame(
                &mut stream,
                &DaemonHello {
                    protocol: PROTOCOL.to_string(),
                },
            )?;
            let ack = read_frame::<DaemonHelloAck>(&mut stream);
            if ack.is_ok() {
                return Err(DaemonTransportError::AlreadyRunning);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {}
    }
    if path.exists() {
        fs::remove_file(path).map_err(DaemonTransportError::Io)?;
    }
    Ok(())
}

fn rebind_missing_socket_path(_path: &PathBuf) {
    // The current std-only listener cannot recreate the public pathname without
    // replacing the accept loop. Keep the daemon alive; clients report
    // not-running until a future listener-rebind pass repairs the path.
}

fn cleanup_socket_path(path: &PathBuf) {
    let _ = fs::remove_file(path);
}

fn connect_and_hello(config: &HubConfig) -> DaemonTransportResult<UnixStream> {
    let path = socket_path(config)?;
    let mut stream = UnixStream::connect(path).map_err(|error| {
        if matches!(
            error.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
        ) {
            DaemonTransportError::NotRunning
        } else {
            DaemonTransportError::Io(error)
        }
    })?;
    write_frame(
        &mut stream,
        &DaemonHello {
            protocol: PROTOCOL.to_string(),
        },
    )?;
    let ack: DaemonHelloAck = read_frame(&mut stream)?;
    if ack.protocol == PROTOCOL {
        Ok(stream)
    } else {
        Err(DaemonTransportError::NotRunning)
    }
}

fn write_frame<T: Serialize>(stream: &mut UnixStream, frame: &T) -> DaemonTransportResult<()> {
    let bytes = serde_json::to_vec(frame).map_err(DaemonTransportError::Json)?;
    stream.write_all(&bytes).map_err(DaemonTransportError::Io)?;
    stream.write_all(b"\n").map_err(DaemonTransportError::Io)
}

fn read_frame<T: for<'de> Deserialize<'de>>(stream: &mut UnixStream) -> DaemonTransportResult<T> {
    let mut reader = BufReader::new(stream.try_clone().map_err(DaemonTransportError::Io)?);
    read_frame_from_reader(&mut reader)
}

fn read_frame_from_reader<T: for<'de> Deserialize<'de>>(
    reader: &mut BufReader<UnixStream>,
) -> DaemonTransportResult<T> {
    let mut line = String::new();
    let bytes = reader
        .read_line(&mut line)
        .map_err(DaemonTransportError::Io)?;
    if bytes == 0 {
        return Err(DaemonTransportError::ClientDisconnected);
    }
    serde_json::from_str(&line).map_err(DaemonTransportError::Json)
}

fn write_terminal_events(
    events: &[DaemonEvent],
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    for event in events {
        if let DaemonEvent::TerminalOutput { data, .. } = event {
            output
                .write_all(data.as_bytes())
                .map_err(DaemonTransportError::Io)?;
            output.flush().map_err(DaemonTransportError::Io)?;
        }
    }
    Ok(())
}

fn events_from_client(events: Vec<HubClientEvent>) -> Vec<DaemonEvent> {
    events.into_iter().map(DaemonEvent::from).collect()
}

#[derive(Debug)]
enum ControlMessage {
    Request {
        request: DaemonRequest,
        reply_tx: Sender<DaemonTransportResult<DaemonResponse>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttachedSubscription {
    session_id: String,
    subscription_id: String,
}

enum AttachedSubscriptionChange {
    Attach(AttachedSubscription),
    Detach(AttachedSubscription),
}

impl AttachedSubscriptionChange {
    fn from_request(request: &DaemonRequest) -> Option<Self> {
        match request {
            DaemonRequest::Attach {
                session_id,
                subscription_id,
            } => Some(Self::Attach(AttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            DaemonRequest::Detach {
                session_id,
                subscription_id,
            } => Some(Self::Detach(AttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DaemonHello {
    protocol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DaemonHelloAck {
    protocol: String,
}

/// Client request variants for the local daemon protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonRequest {
    Status,
    ListSessions,
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
    SendInput {
        session_id: String,
        data: String,
    },
    Resize {
        session_id: String,
        rows: u16,
        cols: u16,
    },
    Drain {
        session_id: String,
    },
    DaemonShutdown,
}

/// Server response variants for one local daemon request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonResponse {
    pub kind: DaemonResponseKind,
    pub status: Option<DaemonStatus>,
    pub sessions: Vec<DaemonSession>,
    pub events: Vec<DaemonEvent>,
}

impl DaemonResponse {
    fn status(status: HubDaemonStatus, session_count: usize) -> Self {
        Self {
            kind: DaemonResponseKind::Status,
            status: Some(DaemonStatus::from_status(&status, session_count)),
            sessions: Vec::new(),
            events: Vec::new(),
        }
    }

    fn sessions(sessions: Vec<HubClientSession>) -> Self {
        Self {
            kind: DaemonResponseKind::Sessions,
            status: None,
            sessions: sessions.into_iter().map(Into::into).collect(),
            events: Vec::new(),
        }
    }

    fn spawned(session: DaemonSession, events: Vec<DaemonEvent>) -> Self {
        Self {
            kind: DaemonResponseKind::Spawned,
            status: None,
            sessions: vec![session],
            events,
        }
    }

    fn events(events: Vec<DaemonEvent>) -> Self {
        Self {
            kind: DaemonResponseKind::Events,
            status: None,
            sessions: Vec::new(),
            events,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonResponseKind {
    Status,
    Sessions,
    Spawned,
    Events,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub lifecycle_state: String,
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
}

impl DaemonStatus {
    fn from_status(status: &HubDaemonStatus, session_count: usize) -> Self {
        Self {
            lifecycle_state: match status.lifecycle_state {
                crate::HubDaemonState::Created => "created",
                crate::HubDaemonState::Running => "running",
                crate::HubDaemonState::Stopped => "stopped",
            }
            .to_string(),
            host_id: status.host_id.clone(),
            host_display_name: status.host_display_name.clone(),
            schema_version: status.schema_version,
            data_dir_configured: status.data_dir_configured,
            core_initialized: status.core_initialized,
            state_source: match status.state_source {
                HubStateLoadSource::Loaded => "loaded",
                HubStateLoadSource::Initialized => "initialized",
            }
            .to_string(),
            package_count: status.package_count,
            enabled_package_count: status.enabled_package_count,
            provider_count: status.provider_count,
            enabled_provider_count: status.enabled_provider_count,
            session_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSession {
    pub session_id: String,
    pub lifecycle: String,
}

impl From<HubClientSession> for DaemonSession {
    fn from(session: HubClientSession) -> Self {
        Self {
            session_id: session.session_id.0,
            lifecycle: lifecycle_label(&session.lifecycle).to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonEvent {
    SessionLifecycle {
        session_id: String,
        state: String,
    },
    TerminalOutput {
        session_id: String,
        subscription_id: String,
        data: String,
    },
    Snapshot {
        session_id: String,
        subscription_id: String,
        bytes: usize,
    },
    Scrollback {
        session_id: String,
        subscription_id: String,
        bytes: usize,
    },
    ProcessExit {
        session_id: String,
        subscription_id: String,
        code: Option<i32>,
    },
    AttachState {
        session_id: String,
        subscription_id: String,
        state: String,
    },
    RuntimeObservation {
        kind: String,
    },
}

impl DaemonEvent {
    fn is_process_exit(&self) -> bool {
        matches!(self, Self::ProcessExit { .. })
    }
}

impl From<HubClientEvent> for DaemonEvent {
    fn from(event: HubClientEvent) -> Self {
        match event {
            HubClientEvent::SessionLifecycle { session_id, state } => Self::SessionLifecycle {
                session_id: session_id.0,
                state: lifecycle_label(&state).to_string(),
            },
            HubClientEvent::TerminalOutput {
                session_id,
                subscription_id,
                data,
            } => Self::TerminalOutput {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                data: String::from_utf8_lossy(&data).to_string(),
            },
            HubClientEvent::Snapshot {
                session_id,
                subscription_id,
                bytes,
            } => Self::Snapshot {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                bytes,
            },
            HubClientEvent::Scrollback {
                session_id,
                subscription_id,
                bytes,
            } => Self::Scrollback {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                bytes,
            },
            HubClientEvent::ProcessExit {
                session_id,
                subscription_id,
                code,
            } => Self::ProcessExit {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                code,
            },
            HubClientEvent::AttachState {
                session_id,
                subscription_id,
                state,
            } => Self::AttachState {
                session_id: session_id.0,
                subscription_id: subscription_id.0,
                state: attach_state_label(&state).to_string(),
            },
            HubClientEvent::RuntimeObservation { kind } => Self::RuntimeObservation {
                kind: match kind {
                    crate::HubClientObservationKind::SessionActivity => "session_activity",
                    crate::HubClientObservationKind::Subscription => "subscription",
                    crate::HubClientObservationKind::Backpressure => "backpressure",
                }
                .to_string(),
            },
        }
    }
}

fn lifecycle_label(state: &SessionLifecycleState) -> &'static str {
    match state {
        SessionLifecycleState::Starting => "starting",
        SessionLifecycleState::Running => "running",
        SessionLifecycleState::Stopping => "stopping",
        SessionLifecycleState::Exited { .. } => "exited",
        SessionLifecycleState::Failed { .. } => "failed",
    }
}

fn attach_state_label(state: &TerminalAttachState) -> &'static str {
    match state {
        TerminalAttachState::Attaching => "attaching",
        TerminalAttachState::Attached => "attached",
        TerminalAttachState::Detached => "detached",
    }
}

/// Daemon socket transport error.
#[derive(Debug)]
pub enum DaemonTransportError {
    MissingSocketBinding,
    NotRunning,
    AlreadyRunning,
    ClientDisconnected,
    Protocol(&'static str),
    UnexpectedResponse,
    DaemonNotRunning,
    ControlThreadStopped,
    Io(std::io::Error),
    Json(serde_json::Error),
    Daemon(crate::HubDaemonError),
    Client(crate::HubClientError),
    Runtime(crate::HubRuntimeError),
}

impl fmt::Display for DaemonTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSocketBinding => write!(formatter, "local socket transport is disabled"),
            Self::NotRunning => write!(formatter, "daemon not running"),
            Self::AlreadyRunning => write!(formatter, "daemon already running"),
            Self::ClientDisconnected => write!(formatter, "client disconnected"),
            Self::Protocol(message) => write!(formatter, "daemon protocol error: {message}"),
            Self::UnexpectedResponse => write!(formatter, "unexpected daemon response"),
            Self::DaemonNotRunning => write!(formatter, "daemon runtime is not running"),
            Self::ControlThreadStopped => write!(formatter, "daemon control thread stopped"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Daemon(error) => write!(formatter, "{error}"),
            Self::Client(error) => write!(formatter, "{error:?}"),
            Self::Runtime(error) => write!(formatter, "{error:?}"),
        }
    }
}

impl Error for DaemonTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Daemon(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::HubDaemonError> for DaemonTransportError {
    fn from(error: crate::HubDaemonError) -> Self {
        Self::Daemon(error)
    }
}

impl From<crate::HubClientError> for DaemonTransportError {
    fn from(error: crate::HubClientError) -> Self {
        Self::Client(error)
    }
}

impl From<crate::HubRuntimeError> for DaemonTransportError {
    fn from(error: crate::HubRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

/// Result alias for daemon socket transport operations.
pub type DaemonTransportResult<T> = Result<T, DaemonTransportError>;
