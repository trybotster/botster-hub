//! Stable local client API boundary over hub policy and runtime facades.
//!
//! This module is transport-neutral. Socket, CLI, TUI, and browser-local bridge
//! adapters can frame these request/response/event types later, but the in-process
//! API already proves the production path through [`crate::HubRuntime`].

use std::collections::BTreeMap;

use botster_core::{
    BotsterEngineObservation, ClientId, CoreSession, CoreSessionMetadata, RequestId, SessionId,
    SessionLifecycleState, SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory,
    SubscriptionId, TerminalAttachState, TransportEgress,
};

use crate::lifecycle::HubPluginLifecycleStatus;
use crate::packages::{PackageClassification, PackageRecord, PackageRegistry, PackageState};
use crate::{HubRuntime, HubRuntimeError, host_profile};

/// Transport-neutral local client API handler.
#[derive(Debug, Clone)]
pub struct HubClientApi {
    identity: HubClientIdentity,
    admission: HubClientAdmission,
}

impl HubClientApi {
    /// Build an API handler for an admitted local operator.
    #[must_use]
    pub fn local_operator(client_id: impl Into<String>) -> Self {
        Self {
            identity: HubClientIdentity {
                client_id: ClientId(client_id.into()),
                role: HubClientRole::LocalOperator,
            },
            admission: HubClientAdmission::local_operator(),
        }
    }

    /// Build an API handler with explicit identity and admission policy.
    #[must_use]
    pub const fn new(identity: HubClientIdentity, admission: HubClientAdmission) -> Self {
        Self {
            identity,
            admission,
        }
    }

    /// Return the API client identity used for runtime-facing requests.
    #[must_use]
    pub const fn identity(&self) -> &HubClientIdentity {
        &self.identity
    }

    /// Handle one local client request through hub-owned facades.
    pub fn handle_request(
        &self,
        runtime: &mut HubRuntime,
        packages: &PackageRegistry,
        request: HubClientRequest,
    ) -> HubClientResult<HubClientResponse> {
        let operation = request.operation();
        let request_id = request.request_id().clone();
        if !self.admission.allows(operation) {
            return Err(HubClientError::AdmissionDenied {
                request_id,
                operation,
                role: self.identity.role,
            });
        }

        let body = match request {
            HubClientRequest::Status { .. } => HubClientResponseBody::Status(HubClientStatus {
                profile_id: host_profile().id.to_string(),
                host_id: runtime.config().host.id.clone(),
                session_count: runtime.list_sessions().len(),
                package_count: packages.packages().len(),
            }),
            HubClientRequest::ListSessions { .. } => HubClientResponseBody::Sessions(
                runtime
                    .list_sessions()
                    .into_iter()
                    .map(HubClientSession::from)
                    .collect(),
            ),
            HubClientRequest::Spawn {
                session_id,
                command,
                ..
            } => {
                let request = spawn_request(runtime, request_id.clone(), session_id, command);
                let outcome = runtime
                    .spawn_session(request, client_session_metadata())
                    .map_err(|_| HubClientError::Runtime {
                        request_id: request_id.clone(),
                        operation,
                    })?;
                HubClientResponseBody::Spawned(HubClientSpawned {
                    session: HubClientSession::from(outcome.session),
                    events: outcome
                        .observations
                        .into_iter()
                        .map(HubClientEvent::from_observation)
                        .collect(),
                })
            }
            HubClientRequest::Attach {
                session_id,
                subscription_id,
                now_seconds,
                ..
            } => {
                let output = runtime
                    .attach_client(
                        self.identity.client_id.clone(),
                        session_id,
                        subscription_id,
                        now_seconds,
                    )
                    .map_err(|_| HubClientError::Runtime {
                        request_id: request_id.clone(),
                        operation,
                    })?;
                HubClientResponseBody::Events(events_from_output(output))
            }
            HubClientRequest::Detach {
                session_id,
                subscription_id,
                now_seconds,
                ..
            } => {
                let output = runtime
                    .detach_client(
                        self.identity.client_id.clone(),
                        session_id,
                        subscription_id,
                        now_seconds,
                    )
                    .map_err(|_| HubClientError::Runtime {
                        request_id: request_id.clone(),
                        operation,
                    })?;
                HubClientResponseBody::Events(events_from_output(output))
            }
            HubClientRequest::Input {
                session_id,
                data,
                now_seconds,
                ..
            } => {
                let output = runtime
                    .write_bytes(
                        self.identity.client_id.clone(),
                        session_id,
                        data,
                        now_seconds,
                    )
                    .map_err(|_| HubClientError::Runtime {
                        request_id: request_id.clone(),
                        operation,
                    })?;
                HubClientResponseBody::Events(events_from_output(output))
            }
            HubClientRequest::Resize {
                session_id,
                rows,
                cols,
                now_seconds,
                ..
            } => {
                let output = runtime
                    .resize(
                        self.identity.client_id.clone(),
                        session_id,
                        rows,
                        cols,
                        now_seconds,
                    )
                    .map_err(|_| HubClientError::Runtime {
                        request_id: request_id.clone(),
                        operation,
                    })?;
                HubClientResponseBody::Events(events_from_output(output))
            }
            HubClientRequest::DrainRuntime {
                session_id,
                last_output_at,
                ..
            } => {
                let output = runtime
                    .drain_runtime_once(&session_id, last_output_at)
                    .map_err(|_| HubClientError::Runtime {
                        request_id: request_id.clone(),
                        operation,
                    })?;
                HubClientResponseBody::Events(events_from_output(output))
            }
            HubClientRequest::Shutdown {
                session_id,
                reason,
                now_seconds,
                ..
            } => {
                let output = runtime
                    .shutdown_session(session_id, reason, now_seconds)
                    .map_err(|_| HubClientError::Runtime {
                        request_id: request_id.clone(),
                        operation,
                    })?;
                HubClientResponseBody::Events(events_from_output(output))
            }
            HubClientRequest::ReadScreen {
                session_id,
                now_seconds,
                ..
            } => {
                let output = runtime
                    .read_screen(request_id.clone(), session_id, now_seconds)
                    .map_err(|error| {
                        hub_client_runtime_error(error, request_id.clone(), operation)
                    })?;
                HubClientResponseBody::Events(events_from_output(output))
            }
            HubClientRequest::CaptureSnapshot {
                session_id,
                now_seconds,
                ..
            } => {
                let output = runtime
                    .capture_snapshot(request_id.clone(), session_id, now_seconds)
                    .map_err(|error| {
                        hub_client_runtime_error(error, request_id.clone(), operation)
                    })?;
                HubClientResponseBody::Events(events_from_output(output))
            }
            HubClientRequest::ListPackages { .. } => HubClientResponseBody::Packages(
                packages
                    .packages()
                    .into_iter()
                    .map(HubClientPackage::from)
                    .collect(),
            ),
            HubClientRequest::PluginLifecycleStatus { .. } => {
                HubClientResponseBody::PluginLifecycle(
                    runtime
                        .plugin_lifecycle_status(packages)
                        .into_iter()
                        .map(HubClientPluginLifecycle::from)
                        .collect(),
                )
            }
        };

        Ok(HubClientResponse { request_id, body })
    }
}

/// Local client identity presented to hub admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientIdentity {
    /// Stable local client id routed into core client-facing methods.
    pub client_id: ClientId,
    /// Hub-owned local client role.
    pub role: HubClientRole,
}

/// Hub-owned local client roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubClientRole {
    /// Trusted same-device operator.
    LocalOperator,
    /// Unadmitted or unauthenticated local process.
    Unadmitted,
}

/// Explicit local client admission policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubClientAdmission {
    allow_status: bool,
    allow_runtime: bool,
    allow_packages: bool,
    allow_lifecycle: bool,
}

impl HubClientAdmission {
    /// Admission for trusted local dogfood clients.
    #[must_use]
    pub const fn local_operator() -> Self {
        Self {
            allow_status: true,
            allow_runtime: true,
            allow_packages: true,
            allow_lifecycle: true,
        }
    }

    /// Admission that denies every request category.
    #[must_use]
    pub const fn deny_all() -> Self {
        Self {
            allow_status: false,
            allow_runtime: false,
            allow_packages: false,
            allow_lifecycle: false,
        }
    }

    const fn allows(&self, operation: HubClientOperation) -> bool {
        match operation {
            HubClientOperation::Status | HubClientOperation::ListSessions => self.allow_status,
            HubClientOperation::Spawn
            | HubClientOperation::Attach
            | HubClientOperation::Detach
            | HubClientOperation::Input
            | HubClientOperation::Resize
            | HubClientOperation::DrainRuntime
            | HubClientOperation::Shutdown
            | HubClientOperation::ReadScreen
            | HubClientOperation::CaptureSnapshot => self.allow_runtime,
            HubClientOperation::ListPackages => self.allow_packages,
            HubClientOperation::PluginLifecycleStatus => self.allow_lifecycle,
        }
    }
}

/// Stable local client request protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubClientRequest {
    /// Return path-neutral hub status.
    Status { request_id: RequestId },
    /// Return current core-recorded sessions.
    ListSessions { request_id: RequestId },
    /// Spawn a session from hub defaults, without client-supplied host paths.
    Spawn {
        request_id: RequestId,
        session_id: SessionId,
        command: String,
    },
    /// Attach to one session stream. This does not hydrate global state.
    Attach {
        request_id: RequestId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    },
    /// Detach from one session stream.
    Detach {
        request_id: RequestId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    },
    /// Send terminal input bytes to one session.
    Input {
        request_id: RequestId,
        session_id: SessionId,
        data: Vec<u8>,
        now_seconds: u64,
    },
    /// Resize one session terminal.
    Resize {
        request_id: RequestId,
        session_id: SessionId,
        rows: u16,
        cols: u16,
        now_seconds: u64,
    },
    /// Drain runtime output for one session through the core subscription path.
    DrainRuntime {
        request_id: RequestId,
        session_id: SessionId,
        last_output_at: u64,
    },
    /// Shut down one session through the hub runtime.
    Shutdown {
        request_id: RequestId,
        session_id: SessionId,
        reason: String,
        now_seconds: u64,
    },
    /// Request a screen read where core supports it.
    ReadScreen {
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    },
    /// Request a snapshot where core supports it.
    CaptureSnapshot {
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    },
    /// Return sanitized package/provider records.
    ListPackages { request_id: RequestId },
    /// Return read-only plugin lifecycle status.
    PluginLifecycleStatus { request_id: RequestId },
}

impl HubClientRequest {
    fn request_id(&self) -> &RequestId {
        match self {
            Self::Status { request_id }
            | Self::ListSessions { request_id }
            | Self::Spawn { request_id, .. }
            | Self::Attach { request_id, .. }
            | Self::Detach { request_id, .. }
            | Self::Input { request_id, .. }
            | Self::Resize { request_id, .. }
            | Self::DrainRuntime { request_id, .. }
            | Self::Shutdown { request_id, .. }
            | Self::ReadScreen { request_id, .. }
            | Self::CaptureSnapshot { request_id, .. }
            | Self::ListPackages { request_id }
            | Self::PluginLifecycleStatus { request_id } => request_id,
        }
    }

    fn operation(&self) -> HubClientOperation {
        match self {
            Self::Status { .. } => HubClientOperation::Status,
            Self::ListSessions { .. } => HubClientOperation::ListSessions,
            Self::Spawn { .. } => HubClientOperation::Spawn,
            Self::Attach { .. } => HubClientOperation::Attach,
            Self::Detach { .. } => HubClientOperation::Detach,
            Self::Input { .. } => HubClientOperation::Input,
            Self::Resize { .. } => HubClientOperation::Resize,
            Self::DrainRuntime { .. } => HubClientOperation::DrainRuntime,
            Self::Shutdown { .. } => HubClientOperation::Shutdown,
            Self::ReadScreen { .. } => HubClientOperation::ReadScreen,
            Self::CaptureSnapshot { .. } => HubClientOperation::CaptureSnapshot,
            Self::ListPackages { .. } => HubClientOperation::ListPackages,
            Self::PluginLifecycleStatus { .. } => HubClientOperation::PluginLifecycleStatus,
        }
    }
}

/// Local client operation category used by admission and errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubClientOperation {
    Status,
    ListSessions,
    Spawn,
    Attach,
    Detach,
    Input,
    Resize,
    DrainRuntime,
    Shutdown,
    ReadScreen,
    CaptureSnapshot,
    ListPackages,
    PluginLifecycleStatus,
}

/// Stable response envelope for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientResponse {
    /// Request correlation id.
    pub request_id: RequestId,
    /// Typed response body.
    pub body: HubClientResponseBody,
}

/// Stable response body variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubClientResponseBody {
    Status(HubClientStatus),
    Sessions(Vec<HubClientSession>),
    Spawned(HubClientSpawned),
    Events(Vec<HubClientEvent>),
    Packages(Vec<HubClientPackage>),
    PluginLifecycle(Vec<HubClientPluginLifecycle>),
}

/// Path-neutral hub status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientStatus {
    pub profile_id: String,
    pub host_id: String,
    pub session_count: usize,
    pub package_count: usize,
}

/// Client-facing session summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientSession {
    pub session_id: SessionId,
    pub lifecycle: SessionLifecycleState,
}

impl From<CoreSession> for HubClientSession {
    fn from(session: CoreSession) -> Self {
        Self {
            session_id: session.session_id,
            lifecycle: session.lifecycle,
        }
    }
}

/// Spawn response summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientSpawned {
    pub session: HubClientSession,
    pub events: Vec<HubClientEvent>,
}

/// Client event stream emitted from hub runtime output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubClientEvent {
    SessionLifecycle {
        session_id: SessionId,
        state: SessionLifecycleState,
    },
    TerminalOutput {
        session_id: SessionId,
        subscription_id: SubscriptionId,
        data: Vec<u8>,
    },
    Snapshot {
        session_id: SessionId,
        subscription_id: SubscriptionId,
        bytes: usize,
    },
    Scrollback {
        session_id: SessionId,
        subscription_id: SubscriptionId,
        bytes: usize,
    },
    ProcessExit {
        session_id: SessionId,
        subscription_id: SubscriptionId,
        code: Option<i32>,
    },
    AttachState {
        session_id: SessionId,
        subscription_id: SubscriptionId,
        state: TerminalAttachState,
    },
    RuntimeObservation {
        kind: HubClientObservationKind,
    },
}

impl HubClientEvent {
    fn from_observation(observation: BotsterEngineObservation) -> Self {
        match observation {
            BotsterEngineObservation::SessionLifecycle { session_id, state } => {
                Self::SessionLifecycle { session_id, state }
            }
            BotsterEngineObservation::SessionActivity { .. } => Self::RuntimeObservation {
                kind: HubClientObservationKind::SessionActivity,
            },
            BotsterEngineObservation::Subscription(_) => Self::RuntimeObservation {
                kind: HubClientObservationKind::Subscription,
            },
            BotsterEngineObservation::Backpressure(_) => Self::RuntimeObservation {
                kind: HubClientObservationKind::Backpressure,
            },
        }
    }
}

/// Sanitized observation family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubClientObservationKind {
    SessionActivity,
    Subscription,
    Backpressure,
}

/// Sanitized package/provider summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPackage {
    pub package_name: String,
    pub version: String,
    pub classification: HubClientPackageClassification,
    pub state: HubClientPackageState,
    pub requested_capabilities: Vec<HubClientCapability>,
    pub provider_profile_admitted: bool,
}

impl From<&PackageRecord> for HubClientPackage {
    fn from(record: &PackageRecord) -> Self {
        Self {
            package_name: record.manifest.name.clone(),
            version: record.manifest.version.clone(),
            classification: record.classification.into(),
            state: record.state.into(),
            requested_capabilities: record
                .manifest
                .capabilities
                .iter()
                .map(|capability| HubClientCapability {
                    surface: format!("{:?}", capability.surface),
                    scope: capability.scope.clone(),
                })
                .collect(),
            provider_profile_admitted: record.admitted_host_profile.is_some(),
        }
    }
}

/// Package classification in client API vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubClientPackageClassification {
    Plugin,
    Provider,
}

impl From<PackageClassification> for HubClientPackageClassification {
    fn from(classification: PackageClassification) -> Self {
        match classification {
            PackageClassification::Plugin => Self::Plugin,
            PackageClassification::Provider => Self::Provider,
        }
    }
}

/// Package state in client API vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubClientPackageState {
    Installed,
    Enabled,
    Disabled,
}

impl From<PackageState> for HubClientPackageState {
    fn from(state: PackageState) -> Self {
        match state {
            PackageState::Installed => Self::Installed,
            PackageState::Enabled => Self::Enabled,
            PackageState::Disabled => Self::Disabled,
        }
    }
}

/// Sanitized capability summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientCapability {
    pub surface: String,
    pub scope: Option<String>,
}

/// Read-only plugin lifecycle status in client API vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPluginLifecycle {
    pub package_name: String,
    pub state: HubClientPackageState,
    pub loaded: bool,
}

impl From<HubPluginLifecycleStatus> for HubClientPluginLifecycle {
    fn from(status: HubPluginLifecycleStatus) -> Self {
        Self {
            package_name: status.package_name,
            state: status.state.into(),
            loaded: status.loaded,
        }
    }
}

/// Client API error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubClientError {
    AdmissionDenied {
        request_id: RequestId,
        operation: HubClientOperation,
        role: HubClientRole,
    },
    UnsupportedDaemonOperation {
        request_id: RequestId,
        operation: HubClientOperation,
        daemon_operation: &'static str,
    },
    Runtime {
        request_id: RequestId,
        operation: HubClientOperation,
    },
}

/// Result alias for client API requests.
pub type HubClientResult<T> = Result<T, HubClientError>;

fn hub_client_runtime_error(
    error: HubRuntimeError,
    request_id: RequestId,
    operation: HubClientOperation,
) -> HubClientError {
    match error {
        HubRuntimeError::UnsupportedDaemonOperation(daemon_operation) => {
            HubClientError::UnsupportedDaemonOperation {
                request_id,
                operation,
                daemon_operation,
            }
        }
        HubRuntimeError::CoreDaemon(_)
        | HubRuntimeError::State(_)
        | HubRuntimeError::UnknownSession(_) => HubClientError::Runtime {
            request_id,
            operation,
        },
    }
}

fn spawn_request(
    runtime: &HubRuntime,
    request_id: RequestId,
    session_id: SessionId,
    command: String,
) -> SessionSpawnRequest {
    SessionSpawnRequest {
        request_id,
        session_id,
        executable: runtime.config().session_defaults.shell.clone(),
        arguments: vec!["-c".to_string(), command],
        working_directory: SpawnWorkingDirectory {
            path: runtime
                .config()
                .session_defaults
                .working_directory
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| ".".to_string()),
        },
        environment: SpawnEnvironment::default(),
        initial_pty_size: Some(botster_core::ResizePayload {
            rows: runtime.config().session_defaults.initial_rows,
            cols: runtime.config().session_defaults.initial_cols,
        }),
    }
}

fn client_session_metadata() -> CoreSessionMetadata {
    CoreSessionMetadata::from_entries(BTreeMap::from([(
        "session_type".to_string(),
        "local_client_api".to_string(),
    )]))
}

fn events_from_output(output: botster_core::BotsterEngineOutput) -> Vec<HubClientEvent> {
    let mut events = Vec::new();

    events.extend(
        output
            .observations
            .into_iter()
            .map(HubClientEvent::from_observation),
    );

    events.extend(
        output
            .client_egress
            .into_iter()
            .filter_map(|(_, frame)| match frame {
                TransportEgress::TerminalOutput {
                    session_id,
                    subscription_id,
                    data,
                } => Some(HubClientEvent::TerminalOutput {
                    session_id,
                    subscription_id,
                    data,
                }),
                TransportEgress::Snapshot {
                    session_id,
                    subscription_id,
                    data,
                } => Some(HubClientEvent::Snapshot {
                    session_id,
                    subscription_id,
                    bytes: data.len(),
                }),
                TransportEgress::Scrollback {
                    session_id,
                    subscription_id,
                    data,
                } => Some(HubClientEvent::Scrollback {
                    session_id,
                    subscription_id,
                    bytes: data.len(),
                }),
                TransportEgress::ProcessExit {
                    session_id,
                    subscription_id,
                    code,
                } => Some(HubClientEvent::ProcessExit {
                    session_id,
                    subscription_id,
                    code,
                }),
                TransportEgress::AttachState {
                    session_id,
                    subscription_id,
                    state,
                } => Some(HubClientEvent::AttachState {
                    session_id,
                    subscription_id,
                    state,
                }),
                TransportEgress::FocusChanged { .. }
                | TransportEgress::Binary { .. }
                | TransportEgress::BoundaryPayload { .. }
                | TransportEgress::Pong { .. }
                | TransportEgress::Close { .. } => None,
            }),
    );

    events
}

#[allow(dead_code)]
fn _runtime_error_type_is_not_public_payload(_: HubRuntimeError) {}
