//! Stable local client API boundary over hub policy and runtime facades.
//!
//! This module is transport-neutral. Socket, CLI, TUI, and browser-local bridge
//! adapters can frame these request/response/event types later, but the in-process
//! API already proves the production path through [`crate::HubRuntime`].

use std::collections::BTreeMap;

use botster_core::{
    BotsterEngineObservation, CapabilitySurface, ClientId, CoreSession, CoreSessionMetadata,
    EnvelopeCursor, EnvelopeDeliveryState, EnvelopeId, EnvelopeTarget, PackageSurfaceKind,
    PackageSurfaceOperation, RequestId, RoutedEnvelope, RoutedEnvelopeDrainOutcome,
    RoutedEnvelopePublishOutcome, SessionId, SessionLifecycleState, SessionRuntimeErrorKind,
    SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId,
    TerminalAttachState, TransportEgress, UiActionResult, UiNode,
};
use botster_core_daemon::{
    GuardedWriteDecision, GuardedWriteDeliveryState, GuardedWriteRequest, GuardedWriteResult,
    ReadinessEvidence,
};

use crate::lifecycle::HubPluginLifecycleStatus;
use crate::packages::{
    PackageClassification, PackageConfigurationView, PackageRecord, PackageRegistry,
    PackageRunnableEntrypointKind, PackageRunnableMode, PackageRunnableProcessState,
    PackageRunnableWorkingDirectory, PackageState,
};
use crate::{HubRuntime, HubRuntimeError, daemon_session_to_core_session, host_profile};

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
                session_count: runtime
                    .list_sessions()
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?
                    .len(),
                package_count: packages.packages().len(),
            }),
            HubClientRequest::ListSessions { .. } => HubClientResponseBody::Sessions(
                runtime
                    .list_sessions()
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?
                    .into_iter()
                    .map(daemon_session_to_core_session)
                    .map(HubClientSession::from)
                    .collect(),
            ),
            HubClientRequest::Spawn {
                session_id,
                command,
                now_seconds,
                ..
            } => {
                let request = spawn_request(runtime, request_id.clone(), session_id, command);
                let outcome = runtime
                    .spawn_session(request, client_session_metadata(), now_seconds)
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::Spawned(HubClientSpawned {
                    session: HubClientSession::from(outcome),
                    events: Vec::new(),
                })
            }
            HubClientRequest::Attach {
                session_id,
                subscription_id,
                now_seconds,
                ..
            } => {
                runtime
                    .attach_client(
                        self.identity.client_id.clone(),
                        session_id,
                        subscription_id,
                        now_seconds,
                    )
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::Events(Vec::new())
            }
            HubClientRequest::Detach {
                session_id,
                subscription_id,
                now_seconds,
                ..
            } => {
                runtime
                    .detach_client(
                        self.identity.client_id.clone(),
                        session_id,
                        subscription_id,
                        now_seconds,
                    )
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::Events(Vec::new())
            }
            HubClientRequest::Input {
                session_id,
                data,
                now_seconds,
                ..
            } => {
                runtime
                    .write_bytes(
                        self.identity.client_id.clone(),
                        session_id,
                        data,
                        now_seconds,
                    )
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::Events(Vec::new())
            }
            HubClientRequest::Resize {
                session_id,
                rows,
                cols,
                now_seconds,
                ..
            } => {
                runtime
                    .resize(
                        self.identity.client_id.clone(),
                        session_id,
                        rows,
                        cols,
                        now_seconds,
                    )
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::Events(Vec::new())
            }
            HubClientRequest::DrainRuntime {
                session_id,
                last_output_at,
                ..
            } => {
                let output = runtime
                    .drain_runtime_once(&session_id, last_output_at)
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::Events(events_from_drain(output))
            }
            HubClientRequest::Shutdown {
                session_id,
                now_seconds,
                ..
            } => {
                runtime
                    .shutdown_session(session_id, now_seconds)
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::Events(Vec::new())
            }
            HubClientRequest::GuardedNotificationWrite {
                session_id,
                package_name,
                data,
                readiness,
                now_seconds,
                ..
            } => {
                if !package_allows_guarded_write(packages, &package_name) {
                    return Err(HubClientError::PackageCapabilityDenied {
                        request_id,
                        operation,
                        package_name,
                    });
                }
                let result = runtime
                    .guarded_write(GuardedWriteRequest {
                        session_id,
                        client_id: self.identity.client_id.clone(),
                        data,
                        readiness,
                        now_seconds,
                    })
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::GuardedWrite(HubClientGuardedWrite::from(result))
            }
            HubClientRequest::NotifySession {
                session_id,
                data,
                readiness,
                now_seconds,
                ..
            } => {
                let result = runtime
                    .guarded_write(GuardedWriteRequest {
                        session_id,
                        client_id: self.identity.client_id.clone(),
                        data,
                        readiness,
                        now_seconds,
                    })
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::GuardedWrite(HubClientGuardedWrite::from(result))
            }
            HubClientRequest::PublishRoutedEnvelope { envelope, .. } => {
                let outcome = runtime
                    .publish_routed_envelope(envelope)
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::RoutedEnvelopePublish(HubClientRoutedEnvelopePublish::from(
                    outcome,
                ))
            }
            HubClientRequest::DrainRoutedEnvelopes {
                target,
                after,
                limit,
                ..
            } => {
                let outcome = runtime
                    .drain_routed_envelopes(target, after, limit)
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?;
                HubClientResponseBody::RoutedEnvelopeDrain(HubClientRoutedEnvelopeDrain::from(
                    outcome,
                ))
            }
            HubClientRequest::AcknowledgeRoutedEnvelope {
                target,
                envelope_id,
                ..
            } => {
                let state = runtime
                    .acknowledge_routed_envelope(target, envelope_id)
                    .map_err(|error| runtime_error(request_id.clone(), operation, error))?
                    .state;
                HubClientResponseBody::RoutedEnvelopeAck(HubClientRoutedEnvelopeAck { state })
            }
            HubClientRequest::ReadScreen { .. } => {
                return Err(HubClientError::UnsupportedDaemonOperation {
                    request_id,
                    operation,
                    daemon_operation: "read_screen",
                });
            }
            HubClientRequest::CaptureSnapshot { .. } => {
                return Err(HubClientError::UnsupportedDaemonOperation {
                    request_id,
                    operation,
                    daemon_operation: "capture_snapshot",
                });
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
            HubClientRequest::PluginSurfaceRender {
                package_name,
                surface_id,
                payload,
                ..
            } => HubClientResponseBody::PluginSurface(
                runtime
                    .render_plugin_surface(&package_name, &surface_id, payload)
                    .map_err(|error| plugin_error(request_id.clone(), operation, error))?,
            ),
            HubClientRequest::PluginSurfaceAction {
                package_name,
                surface_id,
                action_id,
                payload,
                ..
            } => HubClientResponseBody::PluginActionResult(
                runtime
                    .dispatch_plugin_surface_action(&package_name, &surface_id, &action_id, payload)
                    .map_err(|error| plugin_error(request_id.clone(), operation, error))?,
            ),
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
            | HubClientOperation::GuardedNotificationWrite
            | HubClientOperation::NotifySession
            | HubClientOperation::PublishRoutedEnvelope
            | HubClientOperation::DrainRoutedEnvelopes
            | HubClientOperation::AcknowledgeRoutedEnvelope
            | HubClientOperation::ReadScreen
            | HubClientOperation::CaptureSnapshot => self.allow_runtime,
            HubClientOperation::ListPackages => self.allow_packages,
            HubClientOperation::PluginLifecycleStatus
            | HubClientOperation::PluginSurfaceRender
            | HubClientOperation::PluginSurfaceAction => self.allow_lifecycle,
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
        now_seconds: u64,
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
        now_seconds: u64,
    },
    /// Request a hub-admitted guarded notification write into one session.
    GuardedNotificationWrite {
        request_id: RequestId,
        session_id: SessionId,
        package_name: String,
        data: Vec<u8>,
        readiness: ReadinessEvidence,
        now_seconds: u64,
    },
    /// Request a native hub-owned guarded notification write into one session.
    NotifySession {
        request_id: RequestId,
        session_id: SessionId,
        data: Vec<u8>,
        readiness: ReadinessEvidence,
        now_seconds: u64,
    },
    /// Publish one routed envelope through core.
    PublishRoutedEnvelope {
        request_id: RequestId,
        envelope: RoutedEnvelope,
    },
    /// Drain routed envelopes for one target through core cursor semantics.
    DrainRoutedEnvelopes {
        request_id: RequestId,
        target: EnvelopeTarget,
        after: Option<EnvelopeCursor>,
        limit: usize,
    },
    /// Acknowledge one routed envelope target copy through core.
    AcknowledgeRoutedEnvelope {
        request_id: RequestId,
        target: EnvelopeTarget,
        envelope_id: EnvelopeId,
    },
    /// Request a screen read where the daemon API supports it.
    ReadScreen {
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    },
    /// Request a snapshot where the daemon API supports it.
    CaptureSnapshot {
        request_id: RequestId,
        session_id: SessionId,
        now_seconds: u64,
    },
    /// Return sanitized package/provider records.
    ListPackages { request_id: RequestId },
    /// Return read-only plugin lifecycle status.
    PluginLifecycleStatus { request_id: RequestId },
    /// Render one plugin-owned surface through its worker-owned route handler.
    PluginSurfaceRender {
        request_id: RequestId,
        package_name: String,
        surface_id: String,
        payload: serde_json::Value,
    },
    /// Dispatch one plugin-owned semantic UI action through its worker handler.
    PluginSurfaceAction {
        request_id: RequestId,
        package_name: String,
        surface_id: String,
        action_id: String,
        payload: serde_json::Value,
    },
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
            | Self::GuardedNotificationWrite { request_id, .. }
            | Self::NotifySession { request_id, .. }
            | Self::PublishRoutedEnvelope { request_id, .. }
            | Self::DrainRoutedEnvelopes { request_id, .. }
            | Self::AcknowledgeRoutedEnvelope { request_id, .. }
            | Self::ReadScreen { request_id, .. }
            | Self::CaptureSnapshot { request_id, .. }
            | Self::ListPackages { request_id }
            | Self::PluginLifecycleStatus { request_id }
            | Self::PluginSurfaceRender { request_id, .. }
            | Self::PluginSurfaceAction { request_id, .. } => request_id,
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
            Self::GuardedNotificationWrite { .. } => HubClientOperation::GuardedNotificationWrite,
            Self::NotifySession { .. } => HubClientOperation::NotifySession,
            Self::PublishRoutedEnvelope { .. } => HubClientOperation::PublishRoutedEnvelope,
            Self::DrainRoutedEnvelopes { .. } => HubClientOperation::DrainRoutedEnvelopes,
            Self::AcknowledgeRoutedEnvelope { .. } => HubClientOperation::AcknowledgeRoutedEnvelope,
            Self::ReadScreen { .. } => HubClientOperation::ReadScreen,
            Self::CaptureSnapshot { .. } => HubClientOperation::CaptureSnapshot,
            Self::ListPackages { .. } => HubClientOperation::ListPackages,
            Self::PluginLifecycleStatus { .. } => HubClientOperation::PluginLifecycleStatus,
            Self::PluginSurfaceRender { .. } => HubClientOperation::PluginSurfaceRender,
            Self::PluginSurfaceAction { .. } => HubClientOperation::PluginSurfaceAction,
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
    GuardedNotificationWrite,
    NotifySession,
    PublishRoutedEnvelope,
    DrainRoutedEnvelopes,
    AcknowledgeRoutedEnvelope,
    ReadScreen,
    CaptureSnapshot,
    ListPackages,
    PluginLifecycleStatus,
    PluginSurfaceRender,
    PluginSurfaceAction,
}

/// Stable response envelope for one request.
#[derive(Debug, Clone, PartialEq)]
pub struct HubClientResponse {
    /// Request correlation id.
    pub request_id: RequestId,
    /// Typed response body.
    pub body: HubClientResponseBody,
}

/// Stable response body variants.
#[derive(Debug, Clone, PartialEq)]
pub enum HubClientResponseBody {
    Status(HubClientStatus),
    Sessions(Vec<HubClientSession>),
    Spawned(HubClientSpawned),
    Events(Vec<HubClientEvent>),
    GuardedWrite(HubClientGuardedWrite),
    RoutedEnvelopePublish(HubClientRoutedEnvelopePublish),
    RoutedEnvelopeDrain(HubClientRoutedEnvelopeDrain),
    RoutedEnvelopeAck(HubClientRoutedEnvelopeAck),
    Packages(Vec<HubClientPackage>),
    PluginLifecycle(Vec<HubClientPluginLifecycle>),
    PluginSurface(UiNode),
    PluginActionResult(UiActionResult),
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

/// Client-facing guarded write result. Delivery states are produced by core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientGuardedWrite {
    pub decision: GuardedWriteDecision,
    pub states: Vec<GuardedWriteDeliveryState>,
}

impl From<GuardedWriteResult> for HubClientGuardedWrite {
    fn from(result: GuardedWriteResult) -> Self {
        Self {
            decision: result.decision,
            states: result.states,
        }
    }
}

/// Client-facing routed envelope publish outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientRoutedEnvelopePublish {
    pub deliveries: Vec<EnvelopeDeliveryState>,
}

impl From<RoutedEnvelopePublishOutcome> for HubClientRoutedEnvelopePublish {
    fn from(outcome: RoutedEnvelopePublishOutcome) -> Self {
        Self {
            deliveries: outcome.deliveries,
        }
    }
}

/// Client-facing routed envelope drain outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientRoutedEnvelopeDrain {
    pub envelopes: Vec<RoutedEnvelope>,
    pub next_cursor: Option<EnvelopeCursor>,
}

impl From<RoutedEnvelopeDrainOutcome> for HubClientRoutedEnvelopeDrain {
    fn from(outcome: RoutedEnvelopeDrainOutcome) -> Self {
        Self {
            envelopes: outcome.envelopes,
            next_cursor: outcome.next_cursor,
        }
    }
}

/// Client-facing routed envelope acknowledgement outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientRoutedEnvelopeAck {
    pub state: Option<EnvelopeDeliveryState>,
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
        data: Vec<u8>,
    },
    Scrollback {
        session_id: SessionId,
        subscription_id: SubscriptionId,
        data: Vec<u8>,
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
            BotsterEngineObservation::RoutedEnvelope(_) => Self::RuntimeObservation {
                kind: HubClientObservationKind::RoutedEnvelope,
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
    RoutedEnvelope,
}

/// Sanitized package/provider summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPackage {
    pub package_name: String,
    pub version: String,
    pub classification: HubClientPackageClassification,
    pub state: HubClientPackageState,
    pub requested_capabilities: Vec<HubClientCapability>,
    pub surfaces: Vec<HubClientPackageSurfaceDescriptor>,
    pub runnable_entrypoints: Vec<HubClientPackageRunnableEntrypoint>,
    pub configuration: HubClientPackageConfiguration,
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
            surfaces: record
                .manifest
                .surfaces
                .iter()
                .map(|surface| HubClientPackageSurfaceDescriptor {
                    id: surface.id.clone(),
                    kind: package_surface_kind_label(&surface.kind).to_string(),
                    title: surface.title.clone(),
                    description: surface.description.clone(),
                    icon: surface.icon.clone(),
                    order: surface.order,
                    category: surface.category.clone(),
                    supports: surface
                        .supports
                        .iter()
                        .map(|operation| package_surface_operation_label(operation).to_string())
                        .collect(),
                })
                .collect(),
            runnable_entrypoints: record
                .runnable_entrypoints
                .iter()
                .map(|entrypoint| HubClientPackageRunnableEntrypoint {
                    id: entrypoint.id.clone(),
                    kind: runnable_entrypoint_kind_label(entrypoint.kind).to_string(),
                    command: entrypoint.command.clone(),
                    args: entrypoint.args.clone(),
                    working_directory: match &entrypoint.working_directory {
                        PackageRunnableWorkingDirectory::PackageRoot => {
                            HubClientPackageWorkingDirectory {
                                policy: "package_root".to_string(),
                                path: None,
                            }
                        }
                        PackageRunnableWorkingDirectory::EntrypointDir => {
                            HubClientPackageWorkingDirectory {
                                policy: "entrypoint_dir".to_string(),
                                path: None,
                            }
                        }
                        PackageRunnableWorkingDirectory::Relative { path } => {
                            HubClientPackageWorkingDirectory {
                                policy: "relative".to_string(),
                                path: Some(path.clone()),
                            }
                        }
                    },
                    environment: entrypoint
                        .environment
                        .iter()
                        .map(|requirement| HubClientPackageEnvironmentRequirement {
                            name: requirement.name.clone(),
                            required: requirement.required,
                            default: requirement.default.clone(),
                            description: requirement.description.clone(),
                        })
                        .collect(),
                    mode: runnable_mode_label(entrypoint.mode).to_string(),
                    capabilities: entrypoint
                        .capabilities
                        .iter()
                        .map(|capability| HubClientCapability {
                            surface: format!("{:?}", capability.surface),
                            scope: capability.scope.clone(),
                        })
                        .collect(),
                    may_supervise: entrypoint.may_supervise,
                    process: HubClientPackageProcess {
                        state: runnable_process_state_label(entrypoint.process.state).to_string(),
                        pid: None,
                        started_at: None,
                        exited_at: None,
                        exit_status: None,
                        diagnostics: entrypoint
                            .process
                            .diagnostics
                            .iter()
                            .map(|diagnostic| HubClientPackageDiagnostic {
                                kind: diagnostic.kind.clone(),
                                message: diagnostic.message.clone(),
                            })
                            .collect(),
                    },
                })
                .collect(),
            configuration: HubClientPackageConfiguration::from(record.configuration_view()),
            provider_profile_admitted: record.admitted_host_profile.is_some(),
        }
    }
}

/// Sanitized package UI surface descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPackageSurfaceDescriptor {
    pub id: String,
    pub kind: String,
    pub title: String,
    pub description: Option<String>,
    pub icon: Option<String>,
    pub order: Option<i64>,
    pub category: Option<String>,
    pub supports: Vec<String>,
}

/// Sanitized package configuration metadata and effective values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPackageConfiguration {
    pub schema: Option<serde_json::Value>,
    pub effective_values: BTreeMap<String, serde_json::Value>,
    pub missing_required: Vec<String>,
    pub diagnostics: Vec<HubClientPackageDiagnostic>,
}

impl From<PackageConfigurationView> for HubClientPackageConfiguration {
    fn from(view: PackageConfigurationView) -> Self {
        Self {
            schema: view
                .schema
                .map(|schema| serde_json::to_value(schema).unwrap_or(serde_json::Value::Null)),
            effective_values: view
                .effective_values
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
                    )
                })
                .collect(),
            missing_required: view.missing_required,
            diagnostics: view
                .diagnostics
                .into_iter()
                .map(|diagnostic| HubClientPackageDiagnostic {
                    kind: diagnostic.kind,
                    message: match diagnostic.field {
                        Some(field) => format!("{}: {}", field, diagnostic.message),
                        None => diagnostic.message,
                    },
                })
                .collect(),
        }
    }
}

/// Sanitized runnable entrypoint summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPackageRunnableEntrypoint {
    pub id: String,
    pub kind: String,
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: HubClientPackageWorkingDirectory,
    pub environment: Vec<HubClientPackageEnvironmentRequirement>,
    pub mode: String,
    pub capabilities: Vec<HubClientCapability>,
    pub may_supervise: bool,
    pub process: HubClientPackageProcess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPackageWorkingDirectory {
    pub policy: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPackageEnvironmentRequirement {
    pub name: String,
    pub required: bool,
    pub default: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPackageProcess {
    pub state: String,
    pub pid: Option<u32>,
    pub started_at: Option<u64>,
    pub exited_at: Option<u64>,
    pub exit_status: Option<String>,
    pub diagnostics: Vec<HubClientPackageDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubClientPackageDiagnostic {
    pub kind: String,
    pub message: String,
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

fn runnable_entrypoint_kind_label(kind: PackageRunnableEntrypointKind) -> &'static str {
    match kind {
        PackageRunnableEntrypointKind::Client => "client",
        PackageRunnableEntrypointKind::Web => "web",
        PackageRunnableEntrypointKind::Mcp => "mcp",
        PackageRunnableEntrypointKind::Daemon => "daemon",
        PackageRunnableEntrypointKind::Provider => "provider",
    }
}

fn runnable_mode_label(mode: PackageRunnableMode) -> &'static str {
    match mode {
        PackageRunnableMode::Dev => "dev",
        PackageRunnableMode::Local => "local",
    }
}

fn runnable_process_state_label(state: PackageRunnableProcessState) -> &'static str {
    match state {
        PackageRunnableProcessState::NotStarted => "not_started",
        PackageRunnableProcessState::Starting => "starting",
        PackageRunnableProcessState::Running => "running",
        PackageRunnableProcessState::Exited => "exited",
        PackageRunnableProcessState::Failed => "failed",
        PackageRunnableProcessState::Stopped => "stopped",
    }
}

fn package_surface_kind_label(kind: &PackageSurfaceKind) -> &'static str {
    match kind {
        PackageSurfaceKind::App => "app",
        PackageSurfaceKind::Settings => "settings",
        PackageSurfaceKind::DashboardWidget => "dashboard_widget",
        PackageSurfaceKind::Diagnostics => "diagnostics",
    }
}

fn package_surface_operation_label(operation: &PackageSurfaceOperation) -> &'static str {
    match operation {
        PackageSurfaceOperation::Render => "render",
        PackageSurfaceOperation::Action => "action",
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
    Runtime {
        request_id: RequestId,
        operation: HubClientOperation,
        kind: HubClientRuntimeErrorKind,
    },
    UnsupportedDaemonOperation {
        request_id: RequestId,
        operation: HubClientOperation,
        daemon_operation: &'static str,
    },
    PackageCapabilityDenied {
        request_id: RequestId,
        operation: HubClientOperation,
        package_name: String,
    },
    Plugin {
        request_id: RequestId,
        operation: HubClientOperation,
        code: String,
        message: String,
    },
}

/// Result alias for client API requests.
pub type HubClientResult<T> = Result<T, HubClientError>;

/// Stable runtime error categories safe to expose across client transports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubClientRuntimeErrorKind {
    UnknownSession,
    SessionAlreadyExists,
    SpawnFailed,
    Runtime,
    State,
}

fn runtime_error(
    request_id: RequestId,
    operation: HubClientOperation,
    error: impl Into<HubRuntimeError>,
) -> HubClientError {
    let kind = match error.into() {
        HubRuntimeError::CoreDaemon(botster_core_daemon::CoreDaemonError::UnknownSession(_)) => {
            HubClientRuntimeErrorKind::UnknownSession
        }
        HubRuntimeError::CoreDaemon(botster_core_daemon::CoreDaemonError::Engine(
            botster_core::DefaultBotsterEngineError::Runtime(error),
        )) if error.kind == SessionRuntimeErrorKind::SessionNotFound => {
            HubClientRuntimeErrorKind::UnknownSession
        }
        HubRuntimeError::CoreDaemon(botster_core_daemon::CoreDaemonError::Engine(
            botster_core::DefaultBotsterEngineError::Runtime(error),
        )) if error.kind == SessionRuntimeErrorKind::SpawnFailed => {
            HubClientRuntimeErrorKind::SpawnFailed
        }
        HubRuntimeError::CoreDaemon(botster_core_daemon::CoreDaemonError::Engine(
            botster_core::DefaultBotsterEngineError::TerminalBackendConstruction { .. },
        )) => HubClientRuntimeErrorKind::SpawnFailed,
        HubRuntimeError::CoreDaemon(botster_core_daemon::CoreDaemonError::Engine(
            botster_core::DefaultBotsterEngineError::Multiplexer(
                botster_core::MultiplexerEngineError::SessionAlreadyExists { .. },
            ),
        )) => HubClientRuntimeErrorKind::SessionAlreadyExists,
        HubRuntimeError::CoreDaemon(_) => HubClientRuntimeErrorKind::Runtime,
        HubRuntimeError::State(_) => HubClientRuntimeErrorKind::State,
    };
    HubClientError::Runtime {
        request_id,
        operation,
        kind,
    }
}

fn plugin_error(
    request_id: RequestId,
    operation: HubClientOperation,
    error: crate::McpToolError,
) -> HubClientError {
    HubClientError::Plugin {
        request_id,
        operation,
        code: error.code,
        message: error.message,
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

fn events_from_drain(output: botster_core_daemon::DrainResult) -> Vec<HubClientEvent> {
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
                    data,
                }),
                TransportEgress::Scrollback {
                    session_id,
                    subscription_id,
                    data,
                } => Some(HubClientEvent::Scrollback {
                    session_id,
                    subscription_id,
                    data,
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

fn package_allows_guarded_write(packages: &PackageRegistry, package_name: &str) -> bool {
    let Some(record) = packages.package(package_name) else {
        return false;
    };
    if !matches!(record.state, PackageState::Enabled) {
        return false;
    }

    record.manifest.capabilities.iter().any(|capability| {
        capability.surface == CapabilitySurface::SessionActions
            && capability
                .scope
                .as_deref()
                .is_none_or(|scope| scope == "guarded_session_notification_write")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_projection_preserves_snapshot_and_scrollback_payloads_before_live_output() {
        let session_id = SessionId("projection-session".to_string());
        let subscription_id = SubscriptionId("projection-subscription".to_string());
        let client_id = ClientId("projection-client".to_string());
        let events = events_from_drain(botster_core_daemon::DrainResult {
            client_egress: vec![
                (
                    client_id.clone(),
                    TransportEgress::Snapshot {
                        session_id: session_id.clone(),
                        subscription_id: subscription_id.clone(),
                        data: b"snapshot-history".to_vec(),
                    },
                ),
                (
                    client_id.clone(),
                    TransportEgress::Scrollback {
                        session_id: session_id.clone(),
                        subscription_id: subscription_id.clone(),
                        data: b"scrollback-history".to_vec(),
                    },
                ),
                (
                    client_id,
                    TransportEgress::TerminalOutput {
                        session_id: session_id.clone(),
                        subscription_id: subscription_id.clone(),
                        data: b"live-output".to_vec(),
                    },
                ),
            ],
            observations: Vec::new(),
            backpressure: Vec::new(),
        });

        assert_eq!(
            events,
            vec![
                HubClientEvent::Snapshot {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                    data: b"snapshot-history".to_vec(),
                },
                HubClientEvent::Scrollback {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                    data: b"scrollback-history".to_vec(),
                },
                HubClientEvent::TerminalOutput {
                    session_id,
                    subscription_id,
                    data: b"live-output".to_vec(),
                },
            ]
        );
    }
}
