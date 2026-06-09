//! Reusable same-device client protocol for a running `botster-hub` daemon.
//!
//! This crate owns the client-to-hub daemon socket request, response, event,
//! handshake, and connection helpers. It intentionally contains no hub runtime,
//! TUI, Lua, or daemon-to-session-worker protocol dependencies.

use std::error::Error;
use std::fmt;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL: &str = "botster-hub-daemon-v1";
const ATTACH_DRAIN_INTERVAL: Duration = Duration::from_millis(25);

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
    let mut stream = connect_and_hello(endpoint)?;
    write_frame(&mut stream, &request)?;
    read_frame(&mut stream)
}

/// Persistent daemon connection for clients that own attach subscription state.
pub struct DaemonConnection {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
}

impl DaemonConnection {
    /// Connect to the daemon and complete the socket protocol handshake.
    pub fn connect(endpoint: &DaemonEndpoint) -> DaemonTransportResult<Self> {
        let stream = connect_and_hello(endpoint)?;
        let reader = BufReader::new(stream.try_clone().map_err(DaemonTransportError::Io)?);
        Ok(Self { stream, reader })
    }

    /// Send one request over this persistent connection.
    pub fn request(&mut self, request: &DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
        write_frame(&mut self.stream, request)?;
        read_frame_from_reader(&mut self.reader)
    }
}

/// Attach and stream terminal bytes until the session exits or the connection closes.
pub fn stream_attach(
    endpoint: &DaemonEndpoint,
    session_id: &str,
    subscription_id: &str,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    let mut stream = connect_and_hello(endpoint)?;
    let result = stream_attach_connected(&mut stream, session_id, subscription_id, output);
    detach_stream_subscription(&mut stream, session_id, subscription_id);
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

pub fn connect_and_hello(endpoint: &DaemonEndpoint) -> DaemonTransportResult<UnixStream> {
    let mut stream = UnixStream::connect(&endpoint.socket_path).map_err(|error| {
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

pub fn write_frame<T: Serialize>(stream: &mut UnixStream, frame: &T) -> DaemonTransportResult<()> {
    let bytes = serde_json::to_vec(frame).map_err(DaemonTransportError::Json)?;
    stream.write_all(&bytes).map_err(DaemonTransportError::Io)?;
    stream.write_all(b"\n").map_err(DaemonTransportError::Io)
}

pub fn read_frame<T: for<'de> Deserialize<'de>>(
    stream: &mut UnixStream,
) -> DaemonTransportResult<T> {
    let mut reader = BufReader::new(stream.try_clone().map_err(DaemonTransportError::Io)?);
    read_frame_from_reader(&mut reader)
}

pub fn read_frame_from_reader<T: for<'de> Deserialize<'de>>(
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonHello {
    pub protocol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonHelloAck {
    pub protocol: String,
}

/// Client request variants for the local daemon protocol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonRequest {
    Status,
    ListSessions,
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
    SendInput {
        session_id: String,
        data: String,
    },
    Resize {
        session_id: String,
        rows: u16,
        cols: u16,
    },
    ShutdownSession {
        session_id: String,
    },
    Drain {
        session_id: String,
    },
    ListPackages,
    EnablePackageLocalPath {
        path: PathBuf,
    },
    EnablePackage {
        package_name: String,
    },
    DisablePackage {
        package_name: String,
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
        surface_id: String,
        action_id: String,
        payload: Value,
    },
    DaemonShutdown,
}

/// Server response variants for one local daemon request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonResponse {
    pub kind: DaemonResponseKind,
    pub status: Option<DaemonStatus>,
    pub sessions: Vec<DaemonSession>,
    pub packages: Vec<DaemonPackage>,
    pub package_decision: Option<DaemonPackageDecision>,
    pub lifecycle: Vec<DaemonPluginLifecycle>,
    #[serde(default)]
    pub plugin_tools: Vec<Value>,
    #[serde(default)]
    pub plugin_tool_result: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_surface: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_action_result: Option<Value>,
    pub events: Vec<DaemonEvent>,
    pub cleanup: Option<DaemonSessionCleanup>,
    pub coordination: Option<DaemonCoordination>,
    pub error: Option<DaemonOperatorError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonResponseKind {
    Status,
    Sessions,
    Spawned,
    Events,
    Packages,
    PackageDecision,
    PluginLifecycle,
    PluginMcpTools,
    PluginMcpToolResult,
    PluginSurface,
    PluginActionResult,
    SessionCleanup,
    Identity,
    MessagePosted,
    Messages,
    MessageAcked,
    SessionNotified,
    OperatorError,
    Shutdown,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonPackage {
    pub package_name: String,
    pub version: String,
    pub classification: String,
    pub state: String,
    pub requested_capabilities: Vec<DaemonCapability>,
    pub provider_profile_admitted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonCapability {
    pub surface: String,
    pub scope: Option<String>,
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
    pub recovered_sessions: Vec<String>,
    pub stale_sessions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSession {
    pub session_id: String,
    pub lifecycle: String,
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
    #[must_use]
    pub fn is_process_exit(&self) -> bool {
        matches!(self, Self::ProcessExit { .. })
    }
}

pub type DaemonTransportResult<T> = Result<T, DaemonTransportError>;

#[derive(Debug)]
pub enum DaemonTransportError {
    Io(std::io::Error),
    Json(serde_json::Error),
    MissingSocketBinding,
    AlreadyRunning,
    NotRunning,
    ClientDisconnected,
    Protocol(&'static str),
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
            Self::ControlThreadStopped => write!(formatter, "daemon control thread stopped"),
        }
    }
}

impl Error for DaemonTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::MissingSocketBinding
            | Self::AlreadyRunning
            | Self::NotRunning
            | Self::ClientDisconnected
            | Self::Protocol(_)
            | Self::ControlThreadStopped => None,
        }
    }
}
