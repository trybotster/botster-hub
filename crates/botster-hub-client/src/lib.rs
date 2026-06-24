//! Reusable same-device client protocol for a running `botster-hub` daemon.
//!
//! This crate owns the client-to-hub daemon socket request, response, event,
//! handshake, and connection helpers. It intentionally contains no hub runtime,
//! TUI, Lua, or daemon-to-session-worker protocol dependencies.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

mod typescript;

pub const PROTOCOL: &str = "botster-hub-daemon-v1";
pub const PROTOCOL_VERSION: u16 = 1;
pub const CONFORMANCE_FIXTURE_REVISION: u16 = 2;
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
    InstallPackageLocalPath {
        path: PathBuf,
    },
    ShowPackage {
        package_name: String,
    },
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
    #[serde(default)]
    pub runnable_entrypoints: Vec<DaemonPackageRunnableEntrypoint>,
    pub provider_profile_admitted: bool,
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
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub working_directory: DaemonPackageWorkingDirectory,
    #[serde(default)]
    pub environment: Vec<DaemonPackageEnvironmentRequirement>,
    pub mode: String,
    #[serde(default)]
    pub capabilities: Vec<DaemonCapability>,
    pub may_supervise: bool,
    pub process: DaemonPackageProcess,
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

/// Events returned by daemon attach and drain requests.
///
/// `Snapshot` and `Scrollback` are history events for a terminal
/// subscription. Their `data` field is the renderable UTF-8 terminal history,
/// and `bytes` is metadata describing the original raw byte count before the
/// hub decoded that history for clients. Clients should render history events
/// in the order received before appending later `TerminalOutput` for the same
/// subscription.
///
/// ```
/// let snapshot = botster_hub_client::DaemonEvent::Snapshot {
///     session_id: "session".to_string(),
///     subscription_id: "subscription".to_string(),
///     data: "restored history\r\n".to_string(),
///     bytes: 18,
/// };
///
/// let live = botster_hub_client::DaemonEvent::TerminalOutput {
///     session_id: "session".to_string(),
///     subscription_id: "subscription".to_string(),
///     data: "live output\r\n".to_string(),
/// };
///
/// let events = vec![snapshot, live];
/// assert!(matches!(events[0], botster_hub_client::DaemonEvent::Snapshot { .. }));
/// assert!(matches!(events[1], botster_hub_client::DaemonEvent::TerminalOutput { .. }));
/// ```
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
    /// Initial renderable terminal history for a subscription.
    ///
    /// `data` is the UTF-8 string clients render. `bytes` is the raw event data
    /// length before decoding, not a second payload field. If an older hub or
    /// unsupported history source reports a positive byte count without
    /// renderable data, clients should surface an opaque-history/live-only
    /// fallback instead of fabricating scrollback. The current DTO requires
    /// `data`, so byte-only JSON does not deserialize as this variant.
    Snapshot {
        session_id: String,
        subscription_id: String,
        data: String,
        bytes: usize,
    },
    /// Additional renderable terminal history for a subscription.
    ///
    /// Semantics match `Snapshot`: render `data` in event order before later
    /// `TerminalOutput`, and treat `bytes` only as raw-length metadata.
    Scrollback {
        session_id: String,
        subscription_id: String,
        data: String,
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
        assert!(
            error
                .diagnostic
                .contains("unsupported protocol version 1; requires at least 2")
        );
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
    fn snapshot_and_scrollback_events_carry_renderable_data() {
        let events = vec![
            DaemonEvent::Snapshot {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                data: "snapshot-data".to_string(),
                bytes: 13,
            },
            DaemonEvent::Scrollback {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                data: "scrollback-data".to_string(),
                bytes: 15,
            },
        ];

        let value = serde_json::to_value(&events).expect("events serialize");

        assert_eq!(
            value,
            serde_json::json!([
                {
                    "type": "snapshot",
                    "session_id": "session",
                    "subscription_id": "subscription",
                    "data": "snapshot-data",
                    "bytes": 13
                },
                {
                    "type": "scrollback",
                    "session_id": "session",
                    "subscription_id": "subscription",
                    "data": "scrollback-data",
                    "bytes": 15
                }
            ])
        );

        let round_tripped: Vec<DaemonEvent> =
            serde_json::from_value(value).expect("events deserialize");
        assert_eq!(round_tripped, events);
    }

    #[test]
    fn history_events_deserialize_before_later_terminal_output() {
        let value = serde_json::json!([
            {
                "type": "snapshot",
                "session_id": "session",
                "subscription_id": "subscription",
                "data": "snapshot-data",
                "bytes": 13
            },
            {
                "type": "scrollback",
                "session_id": "session",
                "subscription_id": "subscription",
                "data": "scrollback-data",
                "bytes": 15
            },
            {
                "type": "terminal_output",
                "session_id": "session",
                "subscription_id": "subscription",
                "data": "live-data"
            }
        ]);

        let events: Vec<DaemonEvent> =
            serde_json::from_value(value).expect("ordered terminal events deserialize");

        assert!(matches!(events[0], DaemonEvent::Snapshot { .. }));
        assert!(matches!(events[1], DaemonEvent::Scrollback { .. }));
        assert!(matches!(events[2], DaemonEvent::TerminalOutput { .. }));
    }

    #[test]
    fn byte_only_history_json_is_not_current_dto_shape() {
        let value = serde_json::json!({
            "type": "snapshot",
            "session_id": "session",
            "subscription_id": "subscription",
            "bytes": 13
        });

        let error = serde_json::from_value::<DaemonEvent>(value)
            .expect_err("current history events require renderable data");
        assert!(
            error.to_string().contains("data"),
            "missing data should fail loudly, got {error}"
        );
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
                "kind": "web",
                "command": "bin/botster-web",
                "args": ["--host", "127.0.0.1"],
                "working_directory": { "policy": "package_root", "path": null },
                "environment": [{
                    "name": "BOTSTER_WEB_PORT",
                    "required": false,
                    "default": "5173",
                    "description": "Local web client port"
                }],
                "mode": "dev",
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
    fn daemon_request_variants_are_serde_stable_and_generated() {
        for request in daemon_request_examples() {
            let expected_tag = daemon_request_tag(&request);
            let value = serde_json::to_value(&request).expect("request serializes");

            assert_eq!(value["type"], expected_tag);
            assert!(
                daemon_protocol_typescript().contains(&format!("type: \"{expected_tag}\"")),
                "generated TypeScript should include request variant {expected_tag}"
            );

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
            assert!(
                daemon_protocol_typescript().contains(&format!("type: \"{expected_tag}\"")),
                "generated TypeScript should include event variant {expected_tag}"
            );

            let round_tripped: DaemonEvent =
                serde_json::from_value(value).expect("event deserializes");
            assert_eq!(round_tripped, event);
        }
    }

    fn daemon_request_examples() -> Vec<DaemonRequest> {
        vec![
            DaemonRequest::Status,
            DaemonRequest::ListSessions,
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
            DaemonRequest::SendInput {
                session_id: "session".to_string(),
                data: "input".to_string(),
            },
            DaemonRequest::Resize {
                session_id: "session".to_string(),
                rows: 24,
                cols: 80,
            },
            DaemonRequest::ShutdownSession {
                session_id: "session".to_string(),
            },
            DaemonRequest::Drain {
                session_id: "session".to_string(),
            },
            DaemonRequest::ListPackages,
            DaemonRequest::InstallPackageLocalPath {
                path: PathBuf::from("/tmp/plugin"),
            },
            DaemonRequest::ShowPackage {
                package_name: "workflow.plugin".to_string(),
            },
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
                surface_id: "home".to_string(),
                action_id: "refresh".to_string(),
                payload: serde_json::json!({ "id": "run" }),
            },
            DaemonRequest::DaemonShutdown,
        ]
    }

    fn daemon_request_tag(request: &DaemonRequest) -> &'static str {
        match request {
            DaemonRequest::Status => "status",
            DaemonRequest::ListSessions => "list_sessions",
            DaemonRequest::Whoami { .. } => "whoami",
            DaemonRequest::PostMessage { .. } => "post_message",
            DaemonRequest::ReceiveMessages { .. } => "receive_messages",
            DaemonRequest::AckMessage { .. } => "ack_message",
            DaemonRequest::NotifySession { .. } => "notify_session",
            DaemonRequest::Spawn { .. } => "spawn",
            DaemonRequest::Attach { .. } => "attach",
            DaemonRequest::Detach { .. } => "detach",
            DaemonRequest::SendInput { .. } => "send_input",
            DaemonRequest::Resize { .. } => "resize",
            DaemonRequest::ShutdownSession { .. } => "shutdown_session",
            DaemonRequest::Drain { .. } => "drain",
            DaemonRequest::ListPackages => "list_packages",
            DaemonRequest::InstallPackageLocalPath { .. } => "install_package_local_path",
            DaemonRequest::ShowPackage { .. } => "show_package",
            DaemonRequest::EnablePackageLocalPath { .. } => "enable_package_local_path",
            DaemonRequest::EnablePackage { .. } => "enable_package",
            DaemonRequest::DisablePackage { .. } => "disable_package",
            DaemonRequest::RemovePackage { .. } => "remove_package",
            DaemonRequest::StartPackageEntrypoint { .. } => "start_package_entrypoint",
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
            DaemonResponseKind::Sessions,
            DaemonResponseKind::Spawned,
            DaemonResponseKind::Events,
            DaemonResponseKind::Packages,
            DaemonResponseKind::PackageDecision,
            DaemonResponseKind::PluginLifecycle,
            DaemonResponseKind::PluginMcpTools,
            DaemonResponseKind::PluginMcpToolResult,
            DaemonResponseKind::PluginSurface,
            DaemonResponseKind::PluginActionResult,
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
            DaemonResponseKind::Sessions => "sessions",
            DaemonResponseKind::Spawned => "spawned",
            DaemonResponseKind::Events => "events",
            DaemonResponseKind::Packages => "packages",
            DaemonResponseKind::PackageDecision => "package_decision",
            DaemonResponseKind::PluginLifecycle => "plugin_lifecycle",
            DaemonResponseKind::PluginMcpTools => "plugin_mcp_tools",
            DaemonResponseKind::PluginMcpToolResult => "plugin_mcp_tool_result",
            DaemonResponseKind::PluginSurface => "plugin_surface",
            DaemonResponseKind::PluginActionResult => "plugin_action_result",
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
                diagnostics: vec![DaemonDiagnostic::connected("status")],
            }),
            sessions: vec![DaemonSession {
                session_id: "session".to_string(),
                lifecycle: "running".to_string(),
            }],
            packages: vec![DaemonPackage {
                package_name: "workflow.plugin".to_string(),
                version: "1.0.0".to_string(),
                classification: "plugin".to_string(),
                state: "enabled".to_string(),
                requested_capabilities: vec![DaemonCapability {
                    surface: "Network".to_string(),
                    scope: Some("localhost".to_string()),
                }],
                runnable_entrypoints: Vec::new(),
                provider_profile_admitted: false,
            }],
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
            plugin_tools: vec![serde_json::json!({ "name": "tool" })],
            plugin_tool_result: serde_json::json!({ "content": [] }),
            plugin_surface: Some(serde_json::json!({ "type": "text", "value": "surface" })),
            plugin_action_result: Some(serde_json::json!({ "state": "accepted" })),
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
            DaemonEvent::TerminalOutput {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                data: "output".to_string(),
            },
            DaemonEvent::Snapshot {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                data: "snapshot".to_string(),
                bytes: 8,
            },
            DaemonEvent::Scrollback {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                data: "scrollback".to_string(),
                bytes: 10,
            },
            DaemonEvent::ProcessExit {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                code: Some(0),
            },
            DaemonEvent::AttachState {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                state: "attached".to_string(),
            },
            DaemonEvent::RuntimeObservation {
                kind: "observation".to_string(),
            },
        ]
    }

    fn daemon_event_tag(event: &DaemonEvent) -> &'static str {
        match event {
            DaemonEvent::SessionLifecycle { .. } => "session_lifecycle",
            DaemonEvent::TerminalOutput { .. } => "terminal_output",
            DaemonEvent::Snapshot { .. } => "snapshot",
            DaemonEvent::Scrollback { .. } => "scrollback",
            DaemonEvent::ProcessExit { .. } => "process_exit",
            DaemonEvent::AttachState { .. } => "attach_state",
            DaemonEvent::RuntimeObservation { .. } => "runtime_observation",
        }
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
