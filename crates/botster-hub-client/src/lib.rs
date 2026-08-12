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

use base64::Engine as _;
use botster_ui_contract::{PackageSurfaceDescriptor, UiActionRequest, UiActionResult, UiNode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod typescript;

pub const PROTOCOL: &str = "botster-hub-daemon-v1";
pub const PROTOCOL_VERSION: u16 = 7;
pub const CONFORMANCE_FIXTURE_REVISION: u16 = 36;
/// Version of the local WebRTC delivery chunk framing protocol.
pub const LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION: u16 = 2;
/// Serialized local WebRTC delivery frames must remain strictly below this size.
pub const LOCAL_WEBRTC_MAX_FRAME_BYTES: usize = 64 * 1024;
/// Maximum serialized encrypted delivery envelope accepted for reassembly.
pub const LOCAL_WEBRTC_MAX_DELIVERY_BYTES: usize = 16 * 1024 * 1024;
pub const FEATURE_SESSIONS: &str = "sessions";
pub const FEATURE_TERMINAL_STREAMING: &str = "terminal_streaming";
pub const FEATURE_RESIZE: &str = "resize";
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
/// Race-free mode-dependent terminal input via `ModeGatedInput` + mode freshness.
pub const FEATURE_MODE_GATED_INPUT: &str = "mode_gated_input";
const ATTACH_DRAIN_INTERVAL: Duration = Duration::from_millis(25);

/// Authenticated plaintext carried by one complete local WebRTC delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonLocalWebrtcDeliveryKind {
    DaemonResponse,
    DaemonEntityFrame,
}

/// One frame of an encrypted daemon delivery sent over the local WebRTC DataChannel.
///
/// `payload` is a contiguous UTF-8 slice of the serialized encrypted AES-GCM
/// envelope. Clients must validate all declared bounds before concatenating the
/// payloads and decrypt only after the complete envelope has been reassembled.
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
        let reader = BufReader::new(stream.try_clone().map_err(normalize_socket_io_error)?);
        Ok(Self { stream, reader })
    }

    /// Send one request over this persistent connection.
    pub fn request(&mut self, request: &DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
        write_frame(&mut self.stream, request)?;
        read_daemon_response_from_reader(&mut self.reader)
    }
}

/// One held-open, connection-scoped session entity subscription.
pub struct DaemonEntitySubscription {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
    subscription_id: String,
}

impl DaemonEntitySubscription {
    /// Bound how long a caller waits for the next pushed frame.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> DaemonTransportResult<()> {
        self.stream
            .set_read_timeout(timeout)
            .map_err(normalize_socket_io_error)
    }

    /// Read the next authoritative snapshot or ordered entity delta.
    pub fn next_frame(&mut self) -> DaemonTransportResult<DaemonEntityFrame> {
        read_frame_from_reader(&mut self.reader)
    }

    /// Explicitly end this connection-owned subscription.
    pub fn unsubscribe(mut self) -> DaemonTransportResult<()> {
        write_frame(
            &mut self.stream,
            &DaemonRequest::UnsubscribeEntities {
                subscription_id: self.subscription_id.clone(),
            },
        )?;
        loop {
            let value = read_value_frame_from_reader(&mut self.reader)?;
            if value.get("kind").is_none() {
                let _: DaemonEntityFrame =
                    serde_json::from_value(value).map_err(DaemonTransportError::Json)?;
                continue;
            }
            let response: DaemonResponse =
                serde_json::from_value(value).map_err(DaemonTransportError::Json)?;
            if response.kind != DaemonResponseKind::EntityUnsubscribed {
                return Err(DaemonTransportError::Protocol(
                    "unexpected entity unsubscribe response",
                ));
            }
            return Ok(());
        }
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
    let mut stream = connect_and_hello(endpoint)?;
    let mut reader = BufReader::new(stream.try_clone().map_err(normalize_socket_io_error)?);
    write_frame(
        &mut stream,
        &DaemonRequest::SubscribeEntities {
            entity_type,
            subscription_id: subscription_id.clone(),
        },
    )?;
    let response = read_daemon_response_from_reader(&mut reader)?;
    if response.kind != DaemonResponseKind::EntitySubscribed {
        return Err(DaemonTransportError::Protocol(
            "entity subscription was not accepted",
        ));
    }
    Ok(DaemonEntitySubscription {
        stream,
        reader,
        subscription_id,
    })
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
    let mut lifecycle_exited = false;

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
        if response.events.iter().any(DaemonEvent::is_process_exit) || lifecycle_exited {
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
                lifecycle_exited = true;
            }
            idle_drains = 0;
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
            normalize_socket_io_error(error)
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
    stream
        .write_all(&bytes)
        .map_err(normalize_socket_io_error)?;
    stream.write_all(b"\n").map_err(normalize_socket_io_error)
}

pub fn read_frame<T: for<'de> Deserialize<'de>>(
    stream: &mut UnixStream,
) -> DaemonTransportResult<T> {
    let mut reader = BufReader::new(stream.try_clone().map_err(normalize_socket_io_error)?);
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
        .map_err(normalize_socket_io_error)?;
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
    let mut reader = BufReader::new(stream.try_clone().map_err(normalize_socket_io_error)?);
    let value = read_value_frame_from_reader(&mut reader)?;
    if hello_ack_missing_compatibility(&value) {
        return Err(precompatibility_hub_error());
    }
    serde_json::from_value(value).map_err(DaemonTransportError::Json)
}

fn read_daemon_response(stream: &mut UnixStream) -> DaemonTransportResult<DaemonResponse> {
    let mut reader = BufReader::new(stream.try_clone().map_err(normalize_socket_io_error)?);
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
        if let DaemonEvent::TerminalOutput { payload, .. } = event {
            let data = payload
                .decoded_bytes()
                .map_err(|error| DaemonTransportError::Io(std::io::Error::other(error)))?;
            output.write_all(&data).map_err(DaemonTransportError::Io)?;
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
    pub protocol_version: u16,
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
            protocol_version: PROTOCOL_VERSION,
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
    vec![
        FEATURE_SESSIONS,
        FEATURE_TERMINAL_STREAMING,
        FEATURE_RESIZE,
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
        FEATURE_MODE_GATED_INPUT,
    ]
}

/// Client request variants for the local daemon protocol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonRequest {
    Status,
    CheckHubUpdate,
    ListSessions,
    SubscribeEntities {
        entity_type: String,
        subscription_id: String,
    },
    UnsubscribeEntities {
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
    SendInput {
        session_id: String,
        data: String,
    },
    /// Race-free mode-dependent PTY input. Requires a freshness token from
    /// [`DaemonRequest::ReadModeFlags`]. Plain [`DaemonRequest::SendInput`] must
    /// not be used for Kitty keyboard or mouse encodings.
    ModeGatedInput {
        session_id: String,
        data: String,
        mode_generation: u64,
        mode_revision: u64,
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
    ReadScreen {
        session_id: String,
    },
    ReadModeFlags {
        session_id: String,
    },
    CaptureSnapshot {
        session_id: String,
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
    pub mode_gated_input: Option<DaemonModeGatedInputResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_snapshot: Option<DaemonCaptureSnapshot>,
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
    Sessions,
    EntitySubscribed,
    EntityUnsubscribed,
    SessionRemoved,
    Spawned,
    Events,
    SessionTypes,
    SessionTypeDefinition,
    ResolvedSessionType,
    SessionContext,
    ReadScreen,
    ReadModeFlags,
    ModeGatedInput,
    CaptureSnapshot,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonReadScreen {
    pub session_id: String,
    pub text: String,
}

/// Authoritative terminal mode flags plus mode-freshness token for gated input.
///
/// Marked non-exhaustive so additive mode fields remain source-compatible for
/// external Rust consumers that construct this DTO.
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
    /// Worker/session mode-owner epoch. Changes only on new worker ownership.
    pub mode_generation: u64,
    /// Monotonic complete-mode counter within [`Self::mode_generation`].
    pub mode_revision: u64,
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
        mode_generation: u64,
        mode_revision: u64,
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
            mode_generation,
            mode_revision,
        }
    }
}

/// Result of a race-free mode-gated terminal input admit attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonModeGatedInputResult {
    pub session_id: String,
    /// Whether the worker wrote **all** input bytes to the PTY.
    pub admitted: bool,
    /// Number of request payload bytes actually written to the PTY.
    pub bytes_written: usize,
    pub kitty_enabled: bool,
    pub cursor_visible: bool,
    pub bracketed_paste: bool,
    pub mouse_mode: u8,
    pub alt_screen: bool,
    pub focus_reporting: bool,
    pub application_cursor: bool,
    pub mode_generation: u64,
    pub mode_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
}

impl DaemonModeGatedInputResult {
    /// Build a full mode-gated input result body.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: impl Into<String>,
        admitted: bool,
        bytes_written: usize,
        kitty_enabled: bool,
        cursor_visible: bool,
        bracketed_paste: bool,
        mouse_mode: u8,
        alt_screen: bool,
        focus_reporting: bool,
        application_cursor: bool,
        mode_generation: u64,
        mode_revision: u64,
        error_kind: Option<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            admitted,
            bytes_written,
            kitty_enabled,
            cursor_visible,
            bracketed_paste,
            mouse_mode,
            alt_screen,
            focus_reporting,
            application_cursor,
            mode_generation,
            mode_revision,
            error_kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonCaptureSnapshot {
    pub session_id: String,
    pub rows: u16,
    pub cols: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_format: Option<String>,
    pub payload_bytes: usize,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<DaemonDiagnostic>,
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
    pub fn worker_compatibility(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: DaemonDiagnosticKind::WorkerCompatibility,
            operation: Some(operation.into()),
            feature: Some(FEATURE_MODE_GATED_INPUT.to_string()),
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

/// Validated live PTY output serialized as flat daemon event fields.
///
/// Field names match Snapshot/Scrollback so generated clients share one envelope,
/// but these bytes are renderable terminal output and must be concatenated without
/// UTF-8 repair. Do not reuse [`DaemonOpaqueHistoryPayload`] here: that type is
/// opaque engine state that must not be rendered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonLiveOutputPayload {
    pub payload_base64: String,
    pub payload_encoding: DaemonHistoryEncoding,
    pub bytes: usize,
}

#[derive(Deserialize)]
struct UncheckedDaemonLiveOutputPayload {
    payload_base64: String,
    payload_encoding: DaemonHistoryEncoding,
    bytes: usize,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

impl DaemonLiveOutputPayload {
    #[must_use]
    pub fn from_bytes(payload: &[u8]) -> Self {
        Self {
            payload_base64: base64::engine::general_purpose::STANDARD.encode(payload),
            payload_encoding: DaemonHistoryEncoding::Base64,
            bytes: payload.len(),
        }
    }

    /// Decode the live output bytes after validating their declared length.
    pub fn decoded_bytes(&self) -> Result<Vec<u8>, String> {
        decode_validated_base64_payload(&self.payload_base64, self.bytes, "live output")
    }
}

impl TryFrom<UncheckedDaemonLiveOutputPayload> for DaemonLiveOutputPayload {
    type Error = String;

    fn try_from(payload: UncheckedDaemonLiveOutputPayload) -> Result<Self, Self::Error> {
        if payload.extra.contains_key("data") {
            return Err("legacy terminal_output data field is rejected".to_string());
        }
        decode_validated_base64_payload(&payload.payload_base64, payload.bytes, "live output")?;
        Ok(Self {
            payload_base64: payload.payload_base64,
            payload_encoding: payload.payload_encoding,
            bytes: payload.bytes,
        })
    }
}

impl<'de> Deserialize<'de> for DaemonLiveOutputPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let payload = UncheckedDaemonLiveOutputPayload::deserialize(deserializer)?;
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

/// Events returned by daemon attach and drain requests.
///
/// `Snapshot` and `Scrollback` carry opaque binary engine state for a terminal
/// subscription. Their payloads are not terminal text and must not be rendered.
/// Clients use `ReadScreen` for backend-neutral restored text, then append later
/// `TerminalOutput` for the same subscription.
///
/// ```
/// let snapshot = botster_hub_client::DaemonEvent::Snapshot {
///     session_id: "session".to_string(),
///     subscription_id: "subscription".to_string(),
///     history: botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(
///         &[0, 255, 71, 84, 89, 1],
///     ),
/// };
///
/// let live = botster_hub_client::DaemonEvent::TerminalOutput {
///     session_id: "session".to_string(),
///     subscription_id: "subscription".to_string(),
///     payload: botster_hub_client::DaemonLiveOutputPayload::from_bytes(b"live output\r\n"),
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
    /// Live PTY bytes for a subscription.
    ///
    /// `payload` serializes to validated base64 fields. Clients concatenate the
    /// decoded bytes without UTF-8 repair. Legacy `{ "data": "..." }` JSON is
    /// rejected.
    TerminalOutput {
        session_id: String,
        subscription_id: String,
        #[serde(flatten)]
        payload: DaemonLiveOutputPayload,
    },
    /// Initial opaque engine state for a subscription.
    ///
    /// `history` serializes to validated base64 fields and must not be rendered.
    /// Clients use `ReadScreen` when they need backend-neutral restored text.
    Snapshot {
        session_id: String,
        subscription_id: String,
        #[serde(flatten)]
        history: DaemonOpaqueHistoryPayload,
    },
    /// Additional opaque engine state for a subscription.
    ///
    /// Semantics match `Snapshot`; only `TerminalOutput` and `ReadScreen.text`
    /// are renderable terminal text.
    Scrollback {
        session_id: String,
        subscription_id: String,
        #[serde(flatten)]
        history: DaemonOpaqueHistoryPayload,
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
    WorktreeLifecycle {
        event: DaemonWorktreeLifecycleEvent,
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

    fn empty_test_response(sessions: Vec<DaemonSession>, events: Vec<DaemonEvent>) -> Value {
        serde_json::json!({
            "kind": "events",
            "status": null,
            "sessions": sessions,
            "packages": [],
            "package_decision": null,
            "lifecycle": [],
            "events": events,
            "cleanup": null,
            "coordination": null,
            "error": null
        })
    }

    fn expect_request(stream: &mut UnixStream, expected: &DaemonRequest) {
        let request: DaemonRequest = read_frame(stream).expect("read scripted client request");
        assert_eq!(&request, expected);
    }

    #[test]
    fn stream_attach_retains_late_output_across_running_lifecycle_readbacks() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        let server_handle = thread::spawn(move || {
            let attach = DaemonRequest::Attach {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            };
            expect_request(&mut server, &attach);
            write_frame(&mut server, &empty_test_response(Vec::new(), Vec::new()))
                .expect("write attach response");

            let drain = DaemonRequest::Drain {
                session_id: "session".to_string(),
            };
            for _ in 0..20 {
                expect_request(&mut server, &drain);
                write_frame(&mut server, &empty_test_response(Vec::new(), Vec::new()))
                    .expect("write empty drain response");
            }

            expect_request(&mut server, &DaemonRequest::ListSessions);
            write_frame(
                &mut server,
                &empty_test_response(
                    vec![DaemonSession {
                        session_id: "session".to_string(),
                        lifecycle: "running".to_string(),
                    }],
                    Vec::new(),
                ),
            )
            .expect("write running lifecycle response");

            for _ in 0..20 {
                expect_request(&mut server, &drain);
                write_frame(&mut server, &empty_test_response(Vec::new(), Vec::new()))
                    .expect("write empty drain response");
            }

            expect_request(&mut server, &DaemonRequest::ListSessions);
            write_frame(
                &mut server,
                &empty_test_response(
                    vec![DaemonSession {
                        session_id: "session".to_string(),
                        lifecycle: "running".to_string(),
                    }],
                    Vec::new(),
                ),
            )
            .expect("write second running lifecycle response");

            expect_request(&mut server, &drain);
            write_frame(
                &mut server,
                &empty_test_response(
                    Vec::new(),
                    vec![
                        DaemonEvent::TerminalOutput {
                            session_id: "session".to_string(),
                            subscription_id: "subscription".to_string(),
                            payload: DaemonLiveOutputPayload::from_bytes(b"late-output"),
                        },
                        DaemonEvent::ProcessExit {
                            session_id: "session".to_string(),
                            subscription_id: "subscription".to_string(),
                            code: Some(0),
                        },
                    ],
                ),
            )
            .expect("write late terminal output and process exit");
        });
        let mut output = Vec::new();

        stream_attach_connected(&mut client, "session", "subscription", &mut output)
            .expect("stream remains attached through running lifecycle readback");
        drop(client);
        server_handle.join().expect("scripted server completes");

        assert_eq!(output, b"late-output");
    }

    #[test]
    fn stream_attach_completes_when_idle_session_is_exited() {
        let (mut server, mut client) = UnixStream::pair().expect("pair unix streams");
        let server_handle = thread::spawn(move || {
            expect_request(
                &mut server,
                &DaemonRequest::Attach {
                    session_id: "session".to_string(),
                    subscription_id: "subscription".to_string(),
                },
            );
            write_frame(&mut server, &empty_test_response(Vec::new(), Vec::new()))
                .expect("write attach response");

            let drain = DaemonRequest::Drain {
                session_id: "session".to_string(),
            };
            for _ in 0..20 {
                expect_request(&mut server, &drain);
                write_frame(&mut server, &empty_test_response(Vec::new(), Vec::new()))
                    .expect("write empty drain response");
            }

            expect_request(&mut server, &DaemonRequest::ListSessions);
            write_frame(
                &mut server,
                &empty_test_response(
                    vec![DaemonSession {
                        session_id: "session".to_string(),
                        lifecycle: "exited".to_string(),
                    }],
                    Vec::new(),
                ),
            )
            .expect("write exited lifecycle response");

            expect_request(&mut server, &drain);
            write_frame(
                &mut server,
                &empty_test_response(
                    Vec::new(),
                    vec![DaemonEvent::TerminalOutput {
                        session_id: "session".to_string(),
                        subscription_id: "subscription".to_string(),
                        payload: DaemonLiveOutputPayload::from_bytes(b"final-output"),
                    }],
                ),
            )
            .expect("write final drain response");
        });
        let mut output = Vec::new();

        stream_attach_connected(&mut client, "session", "subscription", &mut output)
            .expect("exited lifecycle readback completes attachment");
        drop(client);
        server_handle.join().expect("scripted server completes");

        assert_eq!(output, b"final-output");
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
    fn snapshot_and_scrollback_events_round_trip_opaque_binary_payloads() {
        let events = vec![
            DaemonEvent::Snapshot {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                history: DaemonOpaqueHistoryPayload::from_bytes(&[0, 255, 1]),
            },
            DaemonEvent::Scrollback {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                history: DaemonOpaqueHistoryPayload::from_bytes(&[255, 0, 2]),
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
                    "payload_base64": "AP8B",
                    "payload_encoding": "base64",
                    "bytes": 3
                },
                {
                    "type": "scrollback",
                    "session_id": "session",
                    "subscription_id": "subscription",
                    "payload_base64": "/wAC",
                    "payload_encoding": "base64",
                    "bytes": 3
                }
            ])
        );

        let round_tripped: Vec<DaemonEvent> =
            serde_json::from_value(value).expect("events deserialize");
        assert_eq!(round_tripped, events);
    }

    #[test]
    fn opaque_history_rejects_invalid_base64_and_mismatched_length() {
        for value in [
            serde_json::json!({
                "type": "snapshot",
                "session_id": "session",
                "subscription_id": "subscription",
                "payload_base64": "not base64",
                "payload_encoding": "base64",
                "bytes": 3
            }),
            serde_json::json!({
                "type": "snapshot",
                "session_id": "session",
                "subscription_id": "subscription",
                "payload_base64": "AP8B",
                "payload_encoding": "base64",
                "bytes": 4
            }),
        ] {
            serde_json::from_value::<DaemonEvent>(value)
                .expect_err("invalid opaque history metadata must fail deserialization");
        }
    }

    #[test]
    fn terminal_writer_ignores_opaque_history_and_preserves_live_output() {
        let events = vec![
            DaemonEvent::Snapshot {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                history: DaemonOpaqueHistoryPayload::from_bytes(b"must-not-render"),
            },
            DaemonEvent::TerminalOutput {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                payload: DaemonLiveOutputPayload::from_bytes(b"live-output"),
            },
        ];
        let mut output = Vec::new();

        write_terminal_events(&events, &mut output).expect("terminal events write");

        assert_eq!(output, b"live-output");
    }

    #[test]
    fn live_output_events_round_trip_exact_bytes() {
        let cases: &[&[u8]] = &[
            b"",
            b"ascii",
            b"\x00",
            b"\x1b[31mred\x1b[0m",
            b"\xff",
            b"\xc0",
            "€".as_bytes(),
        ];
        for payload in cases {
            let event = DaemonEvent::TerminalOutput {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                payload: DaemonLiveOutputPayload::from_bytes(payload),
            };
            let value = serde_json::to_value(&event).expect("live output serializes");
            assert_eq!(value["type"], "terminal_output");
            assert_eq!(value["payload_encoding"], "base64");
            assert_eq!(value["bytes"], payload.len());
            assert!(value.get("data").is_none());
            let round_tripped: DaemonEvent =
                serde_json::from_value(value).expect("live output deserializes");
            let DaemonEvent::TerminalOutput {
                payload: decoded, ..
            } = round_tripped
            else {
                panic!("expected terminal output");
            };
            assert_eq!(
                decoded.decoded_bytes().expect("validated payload"),
                *payload
            );
        }
    }

    #[test]
    fn live_output_split_utf8_frames_concatenate_without_replacement() {
        let first = DaemonLiveOutputPayload::from_bytes(&[0xE2]);
        let second = DaemonLiveOutputPayload::from_bytes(&[0x82, 0xAC]);
        let mut concatenated = first.decoded_bytes().expect("first fragment");
        concatenated.extend(second.decoded_bytes().expect("second fragment"));
        assert_eq!(concatenated, "€".as_bytes());
        assert!(
            !concatenated
                .windows(3)
                .any(|window| window == [0xEF, 0xBF, 0xBD])
        );
    }

    #[test]
    fn live_output_rejects_invalid_base64_unknown_encoding_and_length_mismatch() {
        for value in [
            serde_json::json!({
                "type": "terminal_output",
                "session_id": "session",
                "subscription_id": "subscription",
                "payload_base64": "not base64",
                "payload_encoding": "base64",
                "bytes": 3
            }),
            serde_json::json!({
                "type": "terminal_output",
                "session_id": "session",
                "subscription_id": "subscription",
                "payload_base64": "AP8B",
                "payload_encoding": "hex",
                "bytes": 3
            }),
            serde_json::json!({
                "type": "terminal_output",
                "session_id": "session",
                "subscription_id": "subscription",
                "payload_base64": "AP8B",
                "payload_encoding": "base64",
                "bytes": 4
            }),
        ] {
            serde_json::from_value::<DaemonEvent>(value)
                .expect_err("invalid live output metadata must fail deserialization");
        }
    }

    #[test]
    fn live_output_rejects_retired_data_key_on_an_otherwise_valid_envelope() {
        let event = DaemonEvent::TerminalOutput {
            session_id: "session".to_string(),
            subscription_id: "subscription".to_string(),
            payload: DaemonLiveOutputPayload::from_bytes(b"live-after-attach\r\n"),
        };
        let mut value = serde_json::to_value(&event).expect("serialize current live envelope");
        assert!(value.get("data").is_none());
        value["data"] = serde_json::json!("live-after-attach\r\n");
        value["future_hint"] = serde_json::json!(1);

        let error = serde_json::from_value::<DaemonEvent>(value)
            .expect_err("retired data key must fail even when the current envelope is valid");
        assert!(
            error
                .to_string()
                .contains("legacy terminal_output data field is rejected"),
            "expected retired-field rejection, got {error}"
        );

        let mut forward = serde_json::to_value(&event).expect("serialize current live envelope");
        forward["future_hint"] = serde_json::json!(1);
        serde_json::from_value::<DaemonEvent>(forward)
            .expect("unrelated unknown fields remain forward-tolerant");
    }

    #[test]
    fn terminal_writer_writes_decoded_payload_bytes() {
        let events = vec![
            DaemonEvent::Snapshot {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                history: DaemonOpaqueHistoryPayload::from_bytes(b"must-not-render"),
            },
            DaemonEvent::Scrollback {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                history: DaemonOpaqueHistoryPayload::from_bytes(&[0xff]),
            },
            DaemonEvent::TerminalOutput {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                payload: DaemonLiveOutputPayload::from_bytes(&[0x00, 0x1b, 0xff, 0xc0]),
            },
        ];
        let mut output = Vec::new();
        write_terminal_events(&events, &mut output).expect("terminal events write");
        assert_eq!(output, [0x00, 0x1b, 0xff, 0xc0]);
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
    fn protocol_seven_rejects_protocol_six_and_accepts_conformance_floor_thirty_five() {
        assert_eq!(PROTOCOL_VERSION, 7);
        assert_eq!(CONFORMANCE_FIXTURE_REVISION, 36);

        let protocol_six = DaemonCompatibilityRequirement {
            protocol_version: 6,
            minimum_conformance_fixture_revision: 35,
            ..DaemonCompatibilityRequirement::current()
        };
        let error = ensure_compatible(&protocol_six, &DaemonCompatibility::current())
            .expect_err("protocol-6 client must fail closed against protocol 7");
        assert!(error.diagnostic.contains("unsupported protocol version 7"));

        let protocol_seven_floor_thirty_five = DaemonCompatibilityRequirement {
            protocol_version: 7,
            minimum_conformance_fixture_revision: 35,
            ..DaemonCompatibilityRequirement::current()
        };
        ensure_compatible(
            &protocol_seven_floor_thirty_five,
            &DaemonCompatibility::current(),
        )
        .expect("protocol-7 client with conformance floor 35 accepts hub revision 36");
    }

    #[test]
    fn generated_typescript_terminal_output_uses_payload_fields() {
        let generated = daemon_protocol_typescript();
        assert!(generated.contains(
            "{ type: \"terminal_output\"; session_id: string; subscription_id: string; payload_base64: string; payload_encoding: \"base64\"; bytes: number }"
        ));
        assert!(!generated.contains(
            "{ type: \"terminal_output\"; session_id: string; subscription_id: string; data: string }"
        ));
    }

    #[test]
    fn history_events_deserialize_before_later_terminal_output() {
        let value = serde_json::json!([
            {
                "type": "snapshot",
                "session_id": "session",
                "subscription_id": "subscription",
                "payload_base64": "AP8B",
                "payload_encoding": "base64",
                "bytes": 3
            },
            {
                "type": "scrollback",
                "session_id": "session",
                "subscription_id": "subscription",
                "payload_base64": "/wAC",
                "payload_encoding": "base64",
                "bytes": 3
            },
            {
                "type": "terminal_output",
                "session_id": "session",
                "subscription_id": "subscription",
                "payload_base64": "bGl2ZS1kYXRh",
                "payload_encoding": "base64",
                "bytes": 9
            }
        ]);

        let events: Vec<DaemonEvent> =
            serde_json::from_value(value).expect("ordered terminal events deserialize");

        assert!(matches!(events[0], DaemonEvent::Snapshot { .. }));
        assert!(matches!(events[1], DaemonEvent::Scrollback { .. }));
        assert!(matches!(events[2], DaemonEvent::TerminalOutput { .. }));
    }

    #[test]
    fn history_json_without_binary_payload_is_not_current_dto_shape() {
        let value = serde_json::json!({
            "type": "snapshot",
            "session_id": "session",
            "subscription_id": "subscription",
            "bytes": 13
        });

        let error = serde_json::from_value::<DaemonEvent>(value)
            .expect_err("current history events require an opaque payload");
        assert!(
            error.to_string().contains("payload"),
            "missing payload should fail loudly, got {error}"
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
                1,
                2,
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
                "mode_generation": 1,
                "mode_revision": 2,
            })
        );

        let generated = daemon_protocol_typescript();
        assert!(generated.contains(r#"| { type: "read_mode_flags"; session_id: string }"#));
        assert!(generated.contains("mode_flags?: DaemonModeFlags | null;"));
        assert!(generated.contains("mode_generation: number;"));
        assert!(generated.contains("mode_revision: number;"));
        assert!(generated.contains(r#"| { type: "mode_gated_input"; session_id: string; data: string; mode_generation: number; mode_revision: number }"#));
        assert!(generated.contains("mode_gated_input?: DaemonModeGatedInputResult | null;"));
        assert!(generated.contains("export interface DaemonModeGatedInputResult"));
        assert!(generated.contains(FEATURE_MODE_GATED_INPUT));
    }

    #[test]
    fn mode_gated_input_protocol_is_serde_stable_and_generated() {
        let request = DaemonRequest::ModeGatedInput {
            session_id: "mode-session".to_string(),
            data: "x".to_string(),
            mode_generation: 1,
            mode_revision: 2,
        };
        assert_eq!(
            serde_json::to_value(&request).expect("mode gated request serializes"),
            serde_json::json!({
                "type": "mode_gated_input",
                "session_id": "mode-session",
                "data": "x",
                "mode_generation": 1,
                "mode_revision": 2,
            })
        );

        let response = DaemonResponse {
            kind: DaemonResponseKind::ModeGatedInput,
            mode_gated_input: Some(DaemonModeGatedInputResult::new(
                "mode-session",
                true,
                1,
                false,
                true,
                false,
                9,
                false,
                false,
                false,
                1,
                2,
                None,
            )),
            ..daemon_response_example(DaemonResponseKind::ModeGatedInput)
        };
        let value = serde_json::to_value(response).expect("mode gated response serializes");
        assert_eq!(
            value["mode_gated_input"],
            serde_json::json!({
                "session_id": "mode-session",
                "admitted": true,
                "bytes_written": 1,
                "kitty_enabled": false,
                "cursor_visible": true,
                "bracketed_paste": false,
                "mouse_mode": 9,
                "alt_screen": false,
                "focus_reporting": false,
                "application_cursor": false,
                "mode_generation": 1,
                "mode_revision": 2,
            })
        );
    }

    #[test]
    fn new_client_rejects_hub_missing_mode_gated_input_feature() {
        let requirement = DaemonCompatibilityRequirement::current();
        // Keep conformance at the current floor so the feature-token check is the
        // first rejection (not the conf floor). Models old Hub on protocol 6 that
        // never advertised mode_gated_input.
        let mut old_hub = DaemonCompatibility::current();
        old_hub
            .features
            .retain(|feature| feature != FEATURE_MODE_GATED_INPUT);
        let error = ensure_compatible(&requirement, &old_hub).expect_err("missing feature");
        assert!(
            error
                .diagnostic
                .contains("missing required feature(s): mode_gated_input"),
            "unexpected diagnostic: {}",
            error.diagnostic
        );
    }

    #[test]
    fn old_client_accepts_hub_with_mode_gated_input_at_protocol_6() {
        let mut old_client = DaemonCompatibilityRequirement::current();
        old_client
            .required_features
            .retain(|feature| feature != FEATURE_MODE_GATED_INPUT);
        old_client.minimum_conformance_fixture_revision = 33;
        ensure_compatible(&old_client, &DaemonCompatibility::current())
            .expect("old client accepts newer hub with extra feature and conf floor");
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
        assert!(generated.contains("export interface DaemonLifecycleCounters"));
        assert!(generated.contains("cleanup_by_reason?: Record<string, number>;"));
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
            DaemonRequest::ListSessions,
            DaemonRequest::SubscribeEntities {
                entity_type: "session".to_string(),
                subscription_id: "entities".to_string(),
            },
            DaemonRequest::UnsubscribeEntities {
                subscription_id: "entities".to_string(),
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
            DaemonRequest::SendInput {
                session_id: "session".to_string(),
                data: "input".to_string(),
            },
            DaemonRequest::ModeGatedInput {
                session_id: "session".to_string(),
                data: "input".to_string(),
                mode_generation: 1,
                mode_revision: 1,
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
            DaemonRequest::ReadScreen {
                session_id: "session".to_string(),
            },
            DaemonRequest::ReadModeFlags {
                session_id: "session".to_string(),
            },
            DaemonRequest::CaptureSnapshot {
                session_id: "session".to_string(),
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
            DaemonRequest::ListSessions => "list_sessions",
            DaemonRequest::SubscribeEntities { .. } => "subscribe_entities",
            DaemonRequest::UnsubscribeEntities { .. } => "unsubscribe_entities",
            DaemonRequest::RemoveSession { .. } => "remove_session",
            DaemonRequest::Whoami { .. } => "whoami",
            DaemonRequest::PostMessage { .. } => "post_message",
            DaemonRequest::ReceiveMessages { .. } => "receive_messages",
            DaemonRequest::AckMessage { .. } => "ack_message",
            DaemonRequest::NotifySession { .. } => "notify_session",
            DaemonRequest::Spawn { .. } => "spawn",
            DaemonRequest::Attach { .. } => "attach",
            DaemonRequest::Detach { .. } => "detach",
            DaemonRequest::SendInput { .. } => "send_input",
            DaemonRequest::ModeGatedInput { .. } => "mode_gated_input",
            DaemonRequest::Resize { .. } => "resize",
            DaemonRequest::ShutdownSession { .. } => "shutdown_session",
            DaemonRequest::Drain { .. } => "drain",
            DaemonRequest::ReadScreen { .. } => "read_screen",
            DaemonRequest::ReadModeFlags { .. } => "read_mode_flags",
            DaemonRequest::CaptureSnapshot { .. } => "capture_snapshot",
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
            DaemonResponseKind::Sessions,
            DaemonResponseKind::EntitySubscribed,
            DaemonResponseKind::EntityUnsubscribed,
            DaemonResponseKind::SessionRemoved,
            DaemonResponseKind::Spawned,
            DaemonResponseKind::Events,
            DaemonResponseKind::SessionTypes,
            DaemonResponseKind::SessionTypeDefinition,
            DaemonResponseKind::ResolvedSessionType,
            DaemonResponseKind::SessionContext,
            DaemonResponseKind::ReadScreen,
            DaemonResponseKind::ReadModeFlags,
            DaemonResponseKind::ModeGatedInput,
            DaemonResponseKind::CaptureSnapshot,
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
            DaemonResponseKind::Sessions => "sessions",
            DaemonResponseKind::EntitySubscribed => "entity_subscribed",
            DaemonResponseKind::EntityUnsubscribed => "entity_unsubscribed",
            DaemonResponseKind::SessionRemoved => "session_removed",
            DaemonResponseKind::Spawned => "spawned",
            DaemonResponseKind::Events => "events",
            DaemonResponseKind::SessionTypes => "session_types",
            DaemonResponseKind::SessionTypeDefinition => "session_type_definition",
            DaemonResponseKind::ResolvedSessionType => "resolved_session_type",
            DaemonResponseKind::SessionContext => "session_context",
            DaemonResponseKind::ReadScreen => "read_screen",
            DaemonResponseKind::ReadModeFlags => "read_mode_flags",
            DaemonResponseKind::ModeGatedInput => "mode_gated_input",
            DaemonResponseKind::CaptureSnapshot => "capture_snapshot",
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
            }),
            mode_flags: Some(DaemonModeFlags::new(
                "session", false, true, false, 9, false, false, false, 1, 1,
            )),
            mode_gated_input: Some(DaemonModeGatedInputResult::new(
                "session", true, 1, false, true, false, 9, false, false, false, 1, 1, None,
            )),
            capture_snapshot: Some(DaemonCaptureSnapshot {
                session_id: "session".to_string(),
                rows: 24,
                cols: 80,
                payload_format: Some("opaque-snapshot-example-v1".to_string()),
                payload_bytes: 5,
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
            DaemonEvent::TerminalOutput {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                payload: DaemonLiveOutputPayload::from_bytes(b"output"),
            },
            DaemonEvent::Snapshot {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                history: DaemonOpaqueHistoryPayload::from_bytes(b"snapshot"),
            },
            DaemonEvent::Scrollback {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
                history: DaemonOpaqueHistoryPayload::from_bytes(b"scrollback"),
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
            DaemonEvent::WorktreeLifecycle { .. } => "worktree_lifecycle",
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

    #[test]
    fn local_webrtc_delivery_chunk_is_serde_stable_and_generated() {
        let chunk = DaemonLocalWebrtcDeliveryChunk {
            version: LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
            delivery_kind: DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame,
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
    fn protocol_six_and_conformance_thirty_two_define_the_cold_cut_boundary() {
        assert_eq!(PROTOCOL_VERSION, 7);
        assert_eq!(CONFORMANCE_FIXTURE_REVISION, 36);

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
        assert_eq!(stale.compatibility.protocol_version, 7);
        assert_eq!(stale.host_id, "hub");
        assert_eq!(stale.schema_version, 1);
    }

    #[test]
    fn additive_session_type_definition_read_rides_the_conformance_floor() {
        // `ensure_compatible` compares protocol version with exact equality and
        // conformance revision with a floor, so an additive request must ride the
        // conformance revision: bumping the protocol would break every existing
        // first-party client that never issues this request.
        assert_eq!(PROTOCOL_VERSION, 7);
        assert_eq!(CONFORMANCE_FIXTURE_REVISION, 36);
        assert_eq!(
            current_feature_list(),
            vec![
                FEATURE_SESSIONS,
                FEATURE_TERMINAL_STREAMING,
                FEATURE_RESIZE,
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
                FEATURE_MODE_GATED_INPUT,
            ],
            "the authoring read is a request, not a negotiated capability",
        );

        let pinned_at_thirty_one = DaemonCompatibilityRequirement {
            minimum_conformance_fixture_revision: 31,
            ..DaemonCompatibilityRequirement::current()
        };
        ensure_compatible(&pinned_at_thirty_one, &DaemonCompatibility::current())
            .expect("a protocol-6 client pinned at conformance 31 still accepts a revision-32 Hub");

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
