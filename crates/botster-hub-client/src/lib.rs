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
pub const PROTOCOL_VERSION: u16 = 1;
pub const CONFORMANCE_FIXTURE_REVISION: u16 = 1;
pub const FEATURE_SESSIONS: &str = "sessions";
pub const FEATURE_TERMINAL_STREAMING: &str = "terminal_streaming";
pub const FEATURE_RESIZE: &str = "resize";
pub const FEATURE_PLUGIN_SURFACE_RENDER: &str = "plugin_surface_render";
pub const FEATURE_PLUGIN_SURFACE_ACTION: &str = "plugin_surface_action";
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
    read_daemon_response(&mut stream)
}

/// Persistent daemon connection for clients that own attach subscription state.
///
/// ```no_run
/// let endpoint = botster_hub_client::DaemonEndpoint::new("/tmp/botster-hub.sock");
/// let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint)?;
/// let response = connection.request(&botster_hub_client::DaemonRequest::Status)?;
/// # Ok::<(), botster_hub_client::DaemonTransportError>(())
/// ```
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
        read_daemon_response_from_reader(&mut self.reader)
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
            compatibility: requirement.clone(),
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
    Ok(stream)
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
    let line = read_frame_line(reader)?;
    serde_json::from_str(&line).map_err(DaemonTransportError::Json)
}

fn read_frame_line(reader: &mut BufReader<UnixStream>) -> DaemonTransportResult<String> {
    let mut line = String::new();
    let bytes = reader
        .read_line(&mut line)
        .map_err(DaemonTransportError::Io)?;
    if bytes == 0 {
        return Err(DaemonTransportError::ClientDisconnected);
    }
    Ok(line)
}

fn read_value_frame_from_reader(
    reader: &mut BufReader<UnixStream>,
) -> DaemonTransportResult<Value> {
    let line = read_frame_line(reader)?;
    serde_json::from_str(&line).map_err(DaemonTransportError::Json)
}

fn read_hello_ack(stream: &mut UnixStream) -> DaemonTransportResult<DaemonHelloAck> {
    let mut reader = BufReader::new(stream.try_clone().map_err(DaemonTransportError::Io)?);
    let value = read_value_frame_from_reader(&mut reader)?;
    if hello_ack_missing_compatibility(&value) {
        return Err(precompatibility_hub_error());
    }
    serde_json::from_value(value).map_err(DaemonTransportError::Json)
}

fn read_daemon_response(stream: &mut UnixStream) -> DaemonTransportResult<DaemonResponse> {
    let mut reader = BufReader::new(stream.try_clone().map_err(DaemonTransportError::Io)?);
    read_daemon_response_from_reader(&mut reader)
}

fn read_daemon_response_from_reader(
    reader: &mut BufReader<UnixStream>,
) -> DaemonTransportResult<DaemonResponse> {
    let value = read_value_frame_from_reader(reader)?;
    if status_missing_compatibility(&value) {
        return Err(precompatibility_hub_error());
    }
    serde_json::from_value(value).map_err(DaemonTransportError::Json)
}

fn hello_ack_missing_compatibility(value: &Value) -> bool {
    value.as_object().is_some_and(|object| {
        object.contains_key("protocol") && !object.contains_key("compatibility")
    })
}

fn status_missing_compatibility(value: &Value) -> bool {
    value
        .get("status")
        .and_then(Value::as_object)
        .is_some_and(|status| !status.contains_key("compatibility"))
}

fn precompatibility_hub_error() -> DaemonTransportError {
    DaemonTransportError::Compatibility(DaemonCompatibilityError {
        diagnostic: "hub predates compatibility handshake".to_string(),
        diagnostics: vec![DaemonDiagnostic::compatibility_mismatch(
            "hub predates compatibility handshake",
        )],
    })
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
    /// Reserved for future client-admission policy.
    ///
    /// Current hubs deserialize this field but intentionally ignore it; clients
    /// validate hub compatibility from `DaemonHelloAck` and `DaemonStatus`.
    #[serde(default)]
    pub compatibility: DaemonCompatibilityRequirement,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonHelloAck {
    pub protocol: String,
    pub compatibility: DaemonCompatibility,
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
    pub minimum_protocol_version: u16,
    pub required_features: Vec<String>,
    pub minimum_conformance_fixture_revision: u16,
    pub client_name: String,
}

impl DaemonCompatibilityRequirement {
    /// Build the current first-party daemon compatibility requirement.
    ///
    /// ```
    /// let mut requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    /// requirement.client_name = "botster-tui".to_string();
    /// requirement
    ///     .required_features
    ///     .push(botster_hub_client::FEATURE_TERMINAL_STREAMING.to_string());
    ///
    /// assert_eq!(requirement.protocol, botster_hub_client::PROTOCOL);
    /// assert!(requirement
    ///     .required_features
    ///     .contains(&botster_hub_client::FEATURE_TERMINAL_STREAMING.to_string()));
    /// ```
    #[must_use]
    pub fn current() -> Self {
        Self {
            protocol: PROTOCOL.to_string(),
            minimum_protocol_version: PROTOCOL_VERSION,
            required_features: current_feature_list()
                .into_iter()
                .map(str::to_string)
                .collect(),
            minimum_conformance_fixture_revision: CONFORMANCE_FIXTURE_REVISION,
            client_name: "botster-hub-client".to_string(),
        }
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

    if compatibility.protocol_version < requirement.minimum_protocol_version {
        return Err(compatibility_error(
            requirement,
            compatibility,
            format!(
                "unsupported protocol version {}; requires at least {}",
                compatibility.protocol_version, requirement.minimum_protocol_version
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
            requirement.minimum_protocol_version,
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
    vec![
        FEATURE_SESSIONS,
        FEATURE_TERMINAL_STREAMING,
        FEATURE_RESIZE,
        FEATURE_PLUGIN_SURFACE_RENDER,
        FEATURE_PLUGIN_SURFACE_ACTION,
    ]
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonDiagnostic>,
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
    pub compatibility: DaemonCompatibility,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonDiagnostic>,
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
            feature: Some(FEATURE_TERMINAL_STREAMING.to_string()),
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
    pub fn daemon_startup_failure(message: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::DaemonStartupFailure,
            operation: None,
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
    ActionFailure,
    DaemonStartupFailure,
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
            Self::MissingSocketBinding
            | Self::AlreadyRunning
            | Self::NotRunning
            | Self::ClientDisconnected
            | Self::Protocol(_)
            | Self::ControlThreadStopped => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_accepts_current_descriptor() {
        ensure_compatible(
            &DaemonCompatibilityRequirement::current(),
            &DaemonCompatibility::current(),
        )
        .expect("current client and hub are compatible");
    }

    #[test]
    fn compatibility_reports_unsupported_protocol_version() {
        let mut requirement = DaemonCompatibilityRequirement::current();
        requirement.minimum_protocol_version = PROTOCOL_VERSION + 1;
        requirement.client_name = "version-test-client".to_string();

        let error = ensure_compatible(&requirement, &DaemonCompatibility::current())
            .expect_err("newer client requirement should fail against current hub");

        assert!(error.diagnostic.contains("version-test-client"));
        assert!(error
            .diagnostic
            .contains("unsupported protocol version 1; requires at least 2"));
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
        assert!(error
            .diagnostic
            .contains("missing required feature(s): future_feature"));
        assert_eq!(
            error.diagnostics,
            vec![DaemonDiagnostic::unsupported_feature("future_feature")]
        );
    }

    #[test]
    fn response_diagnostics_default_when_missing_for_backward_compatibility() {
        let response = serde_json::json!({
            "kind": "status",
            "status": {
                "lifecycle_state": "running",
                "compatibility": DaemonCompatibility::current(),
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
    fn hello_ack_missing_compatibility_reports_precompatibility_hub() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        server
            .write_all(br#"{"protocol":"botster-hub-daemon-v1"}"#)
            .expect("write old hello ack");
        server.write_all(b"\n").expect("write newline");

        let error = read_hello_ack(&mut client).expect_err("old hello ack should fail");

        assert!(matches!(error, DaemonTransportError::Compatibility(_)));
        assert_eq!(error.to_string(), "hub predates compatibility handshake");
    }

    #[test]
    fn status_missing_compatibility_reports_precompatibility_hub() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        server
            .write_all(
                br#"{"kind":"status","status":{"lifecycle_state":"running","host_id":"hub","host_display_name":"Hub","schema_version":1,"data_dir_configured":true,"core_initialized":true,"state_source":"initialized","package_count":0,"enabled_package_count":0,"provider_count":0,"enabled_provider_count":0,"session_count":0,"recovered_sessions":[],"stale_sessions":[]},"sessions":[],"packages":[],"lifecycle":[],"events":[],"package_decision":null,"cleanup":null,"coordination":null,"error":null}"#,
            )
            .expect("write old status response");
        server.write_all(b"\n").expect("write newline");

        let error = read_daemon_response(&mut client).expect_err("old status response should fail");

        assert!(matches!(error, DaemonTransportError::Compatibility(_)));
        assert_eq!(error.to_string(), "hub predates compatibility handshake");
    }

    #[test]
    fn malformed_hello_ack_still_reports_json_error() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        server
            .write_all(br#"{"protocol":"botster-hub-daemon-v1","compatibility":"wrong"}"#)
            .expect("write malformed hello ack");
        server.write_all(b"\n").expect("write newline");

        let error = read_hello_ack(&mut client).expect_err("malformed ack should fail");

        assert!(matches!(error, DaemonTransportError::Json(_)));
    }

    #[test]
    fn malformed_status_still_reports_json_error() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        server
            .write_all(
                br#"{"kind":"status","status":{"compatibility":"wrong"},"sessions":[],"packages":[],"lifecycle":[],"events":[],"package_decision":null,"cleanup":null,"coordination":null,"error":null}"#,
            )
            .expect("write malformed status response");
        server.write_all(b"\n").expect("write newline");

        let error = read_daemon_response(&mut client).expect_err("malformed status should fail");

        assert!(matches!(error, DaemonTransportError::Json(_)));
    }
}
