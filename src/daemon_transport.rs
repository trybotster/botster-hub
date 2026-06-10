//! Same-device daemon socket transport for the thin operator CLI.
//!
//! This module is a framing adapter over `HubClientApi`. The daemon owns one
//! mutable `HubRuntime` on the accept/control thread; socket threads submit discrete
//! requests and never hold runtime access while writing to a client.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

use botster_core::{
    EndpointId, EnvelopeCursor, EnvelopeDeliveryState, EnvelopeId, EnvelopeTarget,
    ExtensionRuntime, RequestId, RoutedEnvelope, RoutedEnvelopePayload, SessionId,
    SessionLifecycleState, SubscriptionId, TerminalAttachState, UiActionResult,
    UiActionResultState, UiNode,
};
use botster_core_daemon::{
    GuardedWriteDecision, GuardedWriteDeliveryState, ReadinessEvidence, RegistrySessionState,
};
use botster_hub_client::DaemonTransportError as ClientDaemonTransportError;
pub use botster_hub_client::{
    DaemonCapability, DaemonCompatibility, DaemonConnection as ClientDaemonConnection,
    DaemonCoordination, DaemonDiagnostic, DaemonEndpoint, DaemonEnvelope, DaemonEnvelopeAck,
    DaemonEnvelopeDelivery, DaemonEnvelopePublish, DaemonEvent, DaemonHello, DaemonHelloAck,
    DaemonIdentity, DaemonNotify, DaemonOperatorError, DaemonPackage, DaemonPackageDecision,
    DaemonPackageDiagnostic, DaemonPackageEnvironmentRequirement, DaemonPackageProcess,
    DaemonPackageRunnableEntrypoint, DaemonPackageWorkingDirectory, DaemonPluginLifecycle,
    DaemonRequest, DaemonResponse, DaemonResponseKind, DaemonSession, DaemonSessionCleanup,
    DaemonStatus, PROTOCOL, read_frame, read_frame_from_reader, write_frame,
};
use serde_json::Value;
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

use crate::{EntrypointProcessSnapshot, EntrypointSupervisorError};
use crate::{
    FileHubStateStore, HubClientApi, HubClientEvent, HubClientPackage,
    HubClientPackageClassification, HubClientPluginLifecycle, HubClientRequest,
    HubClientResponseBody, HubClientSession, HubConfig, HubDaemon, HubDaemonStatus,
    HubStateLoadSource, HubStateStore, McpToolDescriptor, PackageAction, PackageAdmissionReason,
    PackageDecision, PackageRegistryError,
};

const MESSAGE_CONTENT_TYPE: &str = "application/vnd.botster.coordination.message+text";

/// Run the local daemon socket until a shutdown request is received.
pub fn serve_daemon(config: HubConfig) -> DaemonTransportResult<HubDaemonStatus> {
    let socket_path = socket_path(&config)?;
    prepare_socket_path(&socket_path)?;
    let listener = UnixListener::bind(&socket_path).map_err(DaemonTransportError::Io)?;
    listener
        .set_nonblocking(true)
        .map_err(DaemonTransportError::Io)?;

    let (control_tx, control_rx) = mpsc::channel();
    install_signal_forwarder(control_tx.clone())?;
    let mut daemon = HubDaemon::start(config)?;
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
    let endpoint = daemon_endpoint(config)?;
    botster_hub_client::request(&endpoint, request).map_err(DaemonTransportError::from)
}

/// Persistent daemon connection for clients that own attach subscription state.
pub struct DaemonConnection {
    inner: ClientDaemonConnection,
}

impl DaemonConnection {
    /// Connect to the daemon and complete the socket protocol handshake.
    pub fn connect(config: &HubConfig) -> DaemonTransportResult<Self> {
        let endpoint = daemon_endpoint(config)?;
        let inner =
            ClientDaemonConnection::connect(&endpoint).map_err(DaemonTransportError::from)?;
        Ok(Self { inner })
    }

    /// Send one request over this persistent connection.
    pub fn request(&mut self, request: &DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
        self.inner
            .request(request)
            .map_err(DaemonTransportError::from)
    }
}

/// Attach and stream terminal bytes until the session exits or the connection closes.
pub fn stream_attach(
    config: &HubConfig,
    session_id: SessionId,
    subscription_id: SubscriptionId,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    let endpoint = daemon_endpoint(config)?;
    botster_hub_client::stream_attach(&endpoint, &session_id.0, &subscription_id.0, output)
        .map_err(DaemonTransportError::from)
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
            compatibility: DaemonCompatibility::current(),
            diagnostics: vec![DaemonDiagnostic::connected("hello")],
        },
    )?;
    let mut attached_subscriptions = Vec::<AttachedSubscription>::new();

    loop {
        let request = match read_frame_from_reader::<DaemonRequest>(&mut reader) {
            Ok(request) => request,
            Err(ClientDaemonTransportError::ClientDisconnected) => {
                detach_connection_subscriptions(&control_tx, &attached_subscriptions);
                return Ok(());
            }
            Err(error) => return Err(error.into()),
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
            return Err(error.into());
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
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    message: ControlMessage,
) -> bool {
    let ControlMessage::Request { request, reply_tx } = message;
    let response =
        handle_control_request(daemon, logical_clock, drain_cursors, request).or_else(|error| {
            match error {
                DaemonTransportError::Client(error) => Ok(daemon_operator_error(error)),
                DaemonTransportError::Package(error) => Ok(daemon_package_error(error)),
                DaemonTransportError::State(error) => Ok(daemon_state_error(error)),
                DaemonTransportError::Entrypoint(error) => Ok(daemon_entrypoint_error(error)),
                error => Err(error),
            }
        });
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
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::ListPackages => list_packages_response(daemon),
        DaemonRequest::PluginLifecycleStatus => plugin_lifecycle_response(daemon),
        DaemonRequest::InstallPackageLocalPath { path } => {
            let decision = {
                let record = daemon
                    .package_registry_mut()
                    .install_local_path(path, "daemon socket install local package")?;
                PackageDecision {
                    package_name: record.manifest.name.clone(),
                    action: PackageAction::Install,
                    state: record.state,
                    classification: record.classification,
                    admitted_host_profile: None,
                    audit_reason: record.last_audit_reason.clone(),
                }
            };
            persist_package_registry(daemon)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::ShowPackage { package_name } => show_package_response(daemon, &package_name),
        DaemonRequest::EnablePackageLocalPath { path } => {
            let package_name = {
                let record = daemon
                    .package_registry_mut()
                    .install_local_path(path, "daemon socket enable local package")?;
                record.manifest.name.clone()
            };
            let decision = daemon
                .package_registry_mut()
                .enable(&package_name, "daemon socket enable local package")?;
            persist_package_registry(daemon)?;
            load_package_after_enable(daemon, &package_name)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::EnablePackage { package_name } => {
            let decision = daemon
                .package_registry_mut()
                .enable(&package_name, "daemon socket enable package")?;
            persist_package_registry(daemon)?;
            load_package_after_enable(daemon, &package_name)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::DisablePackage { package_name } => {
            daemon.entrypoint_supervisor().stop_package(&package_name);
            let decision = daemon
                .package_registry_mut()
                .disable(&package_name, "daemon socket disable package")?;
            persist_package_registry(daemon)?;
            unload_package_after_disable(daemon, &package_name)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::RemovePackage { package_name } => {
            daemon.entrypoint_supervisor().stop_package(&package_name);
            unload_package_after_disable(daemon, &package_name)?;
            let decision = daemon
                .package_registry_mut()
                .remove(&package_name, "daemon socket remove package")?;
            persist_package_registry(daemon)?;
            package_decision_response(daemon, decision)
        }
        DaemonRequest::StartPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            let packages = daemon.package_registry().clone();
            daemon
                .entrypoint_supervisor()
                .start(&packages, &package_name, &entrypoint_id)?;
            show_package_response(daemon, &package_name)
        }
        DaemonRequest::StopPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            daemon
                .entrypoint_supervisor()
                .stop(&package_name, &entrypoint_id);
            show_package_response(daemon, &package_name)
        }
        DaemonRequest::RestartPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            let packages = daemon.package_registry().clone();
            daemon
                .entrypoint_supervisor()
                .restart(&packages, &package_name, &entrypoint_id)?;
            show_package_response(daemon, &package_name)
        }
        DaemonRequest::PackageEntrypointStatus {
            package_name,
            entrypoint_id,
        } => {
            daemon
                .entrypoint_supervisor()
                .status(&package_name, &entrypoint_id);
            show_package_response(daemon, &package_name)
        }
        other => handle_runtime_control_request(daemon, logical_clock, drain_cursors, other),
    }
}

fn handle_runtime_control_request(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let status = daemon.status();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let packages = daemon.package_registry().clone();
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };

    match request {
        DaemonRequest::Status => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Status {
                    request_id: request_id("daemon-status"),
                },
            )?;
            let HubClientResponseBody::Status(client_status) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_status(status, client_status.session_count))
        }
        DaemonRequest::ListSessions => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ListSessions {
                    request_id: request_id("daemon-sessions-list"),
                },
            )?;
            let HubClientResponseBody::Sessions(sessions) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_sessions(sessions))
        }
        DaemonRequest::Spawn {
            session_id,
            command,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
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
            Ok(daemon_spawned(
                daemon_session_from_client(spawned.session),
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
                &packages,
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
                &packages,
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
                &packages,
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
                &packages,
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
        DaemonRequest::ShutdownSession { session_id } => {
            let now = tick(logical_clock);
            match classify_shutdown_session(runtime, &session_id)? {
                ShutdownSessionClassification::Active => {}
                ShutdownSessionClassification::Cleanup(cleanup) => {
                    return Ok(daemon_session_cleanup(cleanup));
                }
                ShutdownSessionClassification::Missing => {
                    return Ok(daemon_unknown_session_cleanup(&session_id));
                }
            }
            let shutdown_session_id = session_id.clone();
            let response = match api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Shutdown {
                    request_id: request_id("daemon-sessions-shutdown"),
                    session_id: SessionId(shutdown_session_id),
                    now_seconds: now,
                },
            ) {
                Ok(response) => response,
                Err(error) => {
                    if shutdown_error_is_unknown_session(&error) {
                        return Ok(daemon_session_cleanup(DaemonSessionCleanup {
                            session_id: session_id.clone(),
                            outcome: "already_exited".to_string(),
                        }));
                    }
                    return match classify_shutdown_session(runtime, &session_id)? {
                        ShutdownSessionClassification::Cleanup(cleanup) => {
                            Ok(daemon_session_cleanup(cleanup))
                        }
                        ShutdownSessionClassification::Missing => {
                            Ok(daemon_unknown_session_cleanup(&session_id))
                        }
                        ShutdownSessionClassification::Active => {
                            Err(DaemonTransportError::Client(error))
                        }
                    };
                }
            };
            events_response(response.body)
        }
        DaemonRequest::Drain { session_id } => {
            let cursor = drain_cursors
                .entry(session_id.clone())
                .or_insert_with(|| tick(logical_clock));
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("daemon-sessions-drain"),
                    session_id: SessionId(session_id),
                    last_output_at: *cursor,
                },
            )?;
            let response = events_response(response.body)?;
            if !response.events.is_empty() {
                *cursor = tick(logical_clock);
            }
            Ok(response)
        }
        DaemonRequest::Whoami { caller_session_id } => Ok(daemon_coordination(
            DaemonResponseKind::Identity,
            daemon_coordination_identity(DaemonIdentity {
                client_id: "botster-hub-daemon-socket".to_string(),
                role: "local_operator".to_string(),
                identity_source: if caller_session_id.is_some() {
                    "BOTSTER_SESSION_UUID".to_string()
                } else {
                    "local_operator".to_string()
                },
                caller_session_id,
                host_id: status.host_id.clone(),
                host_display_name: status.host_display_name.clone(),
            }),
        )),
        DaemonRequest::PostMessage {
            caller_session_id,
            target_session_id,
            envelope_id,
            body,
        } => {
            let now = tick(logical_clock);
            let envelope = RoutedEnvelope::new(
                EnvelopeId(
                    envelope_id
                        .unwrap_or_else(|| format!("hub-message-{}-{now}", target_session_id)),
                ),
                EndpointId(
                    caller_session_id
                        .map(|session_id| format!("session:{session_id}"))
                        .unwrap_or_else(|| "botster-hub-mcp".to_string()),
                ),
                vec![EnvelopeTarget::Session {
                    session_id: SessionId(target_session_id),
                }],
                RoutedEnvelopePayload {
                    content_type: MESSAGE_CONTENT_TYPE.to_string(),
                    body: body.into_bytes(),
                    extension: None,
                },
                now,
            );
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::PublishRoutedEnvelope {
                    request_id: request_id("daemon-mcp-post-message"),
                    envelope,
                },
            )?;
            let HubClientResponseBody::RoutedEnvelopePublish(publish) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::MessagePosted,
                daemon_coordination_publish(publish.deliveries),
            ))
        }
        DaemonRequest::ReceiveMessages {
            caller_session_id,
            after,
            limit,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::DrainRoutedEnvelopes {
                    request_id: request_id("daemon-mcp-receive-messages"),
                    target: EnvelopeTarget::Session {
                        session_id: SessionId(caller_session_id),
                    },
                    after: after.map(EnvelopeCursor),
                    limit: limit.clamp(1, 128),
                },
            )?;
            let HubClientResponseBody::RoutedEnvelopeDrain(drain) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::Messages,
                daemon_coordination_messages(drain.envelopes, drain.next_cursor),
            ))
        }
        DaemonRequest::AckMessage {
            caller_session_id,
            envelope_id,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::AcknowledgeRoutedEnvelope {
                    request_id: request_id("daemon-mcp-ack-message"),
                    target: EnvelopeTarget::Session {
                        session_id: SessionId(caller_session_id),
                    },
                    envelope_id: EnvelopeId(envelope_id),
                },
            )?;
            let HubClientResponseBody::RoutedEnvelopeAck(ack) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::MessageAcked,
                daemon_coordination_ack(ack.state),
            ))
        }
        DaemonRequest::NotifySession { session_id, data } => {
            let now = tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::NotifySession {
                    request_id: request_id("daemon-mcp-notify-session"),
                    session_id: SessionId(session_id),
                    data: data.into_bytes(),
                    readiness: ReadinessEvidence::default(),
                    now_seconds: now,
                },
            )?;
            let HubClientResponseBody::GuardedWrite(write) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::SessionNotified,
                daemon_coordination_notify(write.decision, write.states),
            ))
        }
        DaemonRequest::PluginMcpListTools => {
            Ok(daemon_plugin_tools(runtime.list_plugin_mcp_tools()))
        }
        DaemonRequest::PluginMcpCallTool { name, arguments } => {
            match runtime.call_plugin_mcp_tool(crate::McpCallRequest { name, arguments }) {
                Ok(result) => Ok(daemon_plugin_tool_result(result)),
                Err(error) => Ok(daemon_plugin_tool_error(error)),
            }
        }
        DaemonRequest::PluginSurfaceRender {
            package_name,
            surface_id,
            payload,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::PluginSurfaceRender {
                    request_id: request_id("daemon-plugin-surface-render"),
                    package_name,
                    surface_id,
                    payload,
                },
            )?;
            let HubClientResponseBody::PluginSurface(surface) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_plugin_surface(surface))
        }
        DaemonRequest::PluginSurfaceAction {
            package_name,
            surface_id,
            action_id,
            payload,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::PluginSurfaceAction {
                    request_id: request_id("daemon-plugin-surface-action"),
                    package_name,
                    surface_id,
                    action_id,
                    payload,
                },
            )?;
            let HubClientResponseBody::PluginActionResult(result) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_plugin_action_result(result))
        }
        DaemonRequest::DaemonShutdown => Ok(DaemonResponse {
            kind: DaemonResponseKind::Shutdown,
            status: Some(daemon_status_from_status(
                &status,
                runtime
                    .list_sessions()
                    .map_err(crate::HubRuntimeError::from)?
                    .len(),
            )),
            sessions: Vec::new(),
            packages: Vec::new(),
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            plugin_surface: None,
            plugin_action_result: None,
            events: Vec::new(),
            cleanup: None,
            coordination: None,
            error: None,
            diagnostics: vec![DaemonDiagnostic::connected("shutdown")],
        }),
        DaemonRequest::ListPackages
        | DaemonRequest::InstallPackageLocalPath { .. }
        | DaemonRequest::ShowPackage { .. }
        | DaemonRequest::PluginLifecycleStatus
        | DaemonRequest::EnablePackageLocalPath { .. }
        | DaemonRequest::EnablePackage { .. }
        | DaemonRequest::DisablePackage { .. }
        | DaemonRequest::RemovePackage { .. }
        | DaemonRequest::StartPackageEntrypoint { .. }
        | DaemonRequest::StopPackageEntrypoint { .. }
        | DaemonRequest::RestartPackageEntrypoint { .. }
        | DaemonRequest::PackageEntrypointStatus { .. } => {
            unreachable!("package requests are handled before runtime borrow")
        }
    }
}

fn load_package_after_enable(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let package_registry = daemon.package_registry().clone();
    let prepared = package_registry.prepare_local_package(
        package_name,
        "daemon socket load enabled local plugin package",
    )?;
    if prepared.selected_entrypoint.runtime == ExtensionRuntime::Lua {
        daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?
            .load_lua_plugin_package(&package_registry, package_name)
            .map_err(crate::HubDaemonError::from)?;
    }
    Ok(())
}

fn unload_package_after_disable(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let _ = daemon
        .runtime_mut()
        .ok_or(DaemonTransportError::DaemonNotRunning)?
        .unload_plugin_package(
            request_id(&format!("daemon-disable-{package_name}")),
            package_name,
        );
    Ok(())
}

fn list_packages_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };
    let response = api.handle_request(
        runtime,
        &packages,
        HubClientRequest::ListPackages {
            request_id: request_id("daemon-packages-list"),
        },
    )?;
    let HubClientResponseBody::Packages(mut packages) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    let snapshots = daemon.entrypoint_supervisor().snapshots();
    apply_entrypoint_snapshots(&mut packages, snapshots);
    Ok(daemon_packages(packages))
}

fn show_package_response(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let mut package = daemon
        .package_registry()
        .package(package_name)
        .map(HubClientPackage::from)
        .ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                PackageAction::Show,
                PackageAdmissionReason::PackageNotInstalled,
                "daemon socket show package".to_string(),
            )
        })?;
    let snapshots = daemon.entrypoint_supervisor().snapshots();
    apply_entrypoint_snapshots(std::slice::from_mut(&mut package), snapshots);
    Ok(daemon_packages(vec![package]))
}

fn plugin_lifecycle_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };
    let response = api.handle_request(
        runtime,
        &packages,
        HubClientRequest::PluginLifecycleStatus {
            request_id: request_id("daemon-plugin-lifecycle-status"),
        },
    )?;
    let HubClientResponseBody::PluginLifecycle(lifecycle) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(daemon_plugin_lifecycle(lifecycle))
}

fn package_decision_response(
    daemon: &mut HubDaemon,
    decision: PackageDecision,
) -> DaemonTransportResult<DaemonResponse> {
    let mut response = list_packages_response(daemon)?;
    response.kind = DaemonResponseKind::PackageDecision;
    response.package_decision = Some(daemon_package_decision_from_policy(decision));
    Ok(response)
}

fn apply_entrypoint_snapshots(
    packages: &mut [HubClientPackage],
    snapshots: Vec<EntrypointProcessSnapshot>,
) {
    for snapshot in snapshots {
        let Some(package) = packages
            .iter_mut()
            .find(|package| package.package_name == snapshot.package_name)
        else {
            continue;
        };
        let Some(entrypoint) = package
            .runnable_entrypoints
            .iter_mut()
            .find(|entrypoint| entrypoint.id == snapshot.entrypoint_id)
        else {
            continue;
        };
        entrypoint.process.state = snapshot.state;
        entrypoint.process.pid = snapshot.pid;
        entrypoint.process.started_at = snapshot.started_at;
        entrypoint.process.exited_at = snapshot.exited_at;
        entrypoint.process.exit_status = snapshot.exit_status;
        entrypoint.process.diagnostics = snapshot
            .diagnostics
            .into_iter()
            .map(|diagnostic| crate::HubClientPackageDiagnostic {
                kind: diagnostic.kind,
                message: diagnostic.message,
            })
            .collect();
    }
}

fn persist_package_registry(daemon: &HubDaemon) -> DaemonTransportResult<()> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let snapshot = daemon.package_registry().snapshot();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store.update(&config, |state| {
        state.package_registry = snapshot;
    })?;
    Ok(())
}

fn events_response(body: HubClientResponseBody) -> DaemonTransportResult<DaemonResponse> {
    let HubClientResponseBody::Events(events) = body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(daemon_events(events_from_client(events)))
}

enum ShutdownSessionClassification {
    Active,
    Cleanup(DaemonSessionCleanup),
    Missing,
}

fn classify_shutdown_session(
    runtime: &mut crate::HubRuntime,
    session_id: &str,
) -> Result<ShutdownSessionClassification, crate::HubRuntimeError> {
    let Some(session) = runtime
        .list_sessions()
        .map_err(crate::HubRuntimeError::from)?
        .into_iter()
        .find(|session| session.session_id.0 == session_id)
    else {
        return Ok(ShutdownSessionClassification::Missing);
    };

    match session.registry_state {
        RegistrySessionState::Running => Ok(ShutdownSessionClassification::Active),
        RegistrySessionState::Stopping | RegistrySessionState::Exited => Ok(
            ShutdownSessionClassification::Cleanup(DaemonSessionCleanup {
                session_id: session_id.to_string(),
                outcome: "already_exited".to_string(),
            }),
        ),
        RegistrySessionState::Stale => Ok(ShutdownSessionClassification::Cleanup(
            DaemonSessionCleanup {
                session_id: session_id.to_string(),
                outcome: "stale_session".to_string(),
            },
        )),
    }
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

fn daemon_endpoint(config: &HubConfig) -> DaemonTransportResult<DaemonEndpoint> {
    socket_path(config).map(DaemonEndpoint::new)
}

fn install_signal_forwarder(control_tx: Sender<ControlMessage>) -> DaemonTransportResult<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM]).map_err(DaemonTransportError::Io)?;
    thread::spawn(move || {
        if signals.forever().next().is_some() {
            let (reply_tx, _reply_rx) = mpsc::channel();
            let _ = control_tx.send(ControlMessage::Request {
                request: DaemonRequest::DaemonShutdown,
                reply_tx,
            });
        }
    });
    Ok(())
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
                    compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
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

fn events_from_client(events: Vec<HubClientEvent>) -> Vec<DaemonEvent> {
    events.into_iter().map(daemon_event_from_client).collect()
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

fn daemon_response_base(kind: DaemonResponseKind) -> DaemonResponse {
    DaemonResponse {
        kind,
        status: None,
        sessions: Vec::new(),
        packages: Vec::new(),
        package_decision: None,
        lifecycle: Vec::new(),
        plugin_tools: Vec::new(),
        plugin_tool_result: Value::Null,
        plugin_surface: None,
        plugin_action_result: None,
        events: Vec::new(),
        cleanup: None,
        coordination: None,
        error: None,
        diagnostics: Vec::new(),
    }
}

fn daemon_status(status: HubDaemonStatus, session_count: usize) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Status);
    response.status = Some(daemon_status_from_status(&status, session_count));
    response.diagnostics = vec![DaemonDiagnostic::connected("status")];
    response
}

fn daemon_sessions(sessions: Vec<HubClientSession>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Sessions);
    response.sessions = sessions
        .into_iter()
        .map(daemon_session_from_client)
        .collect();
    response
}

fn daemon_spawned(session: DaemonSession, events: Vec<DaemonEvent>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Spawned);
    response.sessions = vec![session];
    response.events = events;
    response
}

fn daemon_events(events: Vec<DaemonEvent>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Events);
    response.events = events;
    response
}

fn daemon_packages(packages: Vec<HubClientPackage>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Packages);
    response.packages = packages
        .into_iter()
        .map(daemon_package_from_client)
        .collect();
    response
}

fn daemon_plugin_lifecycle(lifecycle: Vec<HubClientPluginLifecycle>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginLifecycle);
    response.lifecycle = lifecycle
        .into_iter()
        .map(daemon_plugin_lifecycle_from_client)
        .collect();
    response
}

fn daemon_session_cleanup(cleanup: DaemonSessionCleanup) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SessionCleanup);
    response.cleanup = Some(cleanup);
    response
}

fn daemon_unknown_session_cleanup(session_id: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "unknown_session".to_string(),
        request_id: "daemon-sessions-shutdown".to_string(),
        operation: "shutdown".to_string(),
        message: format!("unknown session: {session_id}"),
        diagnostics: Vec::new(),
    });
    response
}

fn daemon_operator_error(error: crate::HubClientError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_client(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

fn daemon_package_error(error: crate::PackageRegistryError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_package(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

fn daemon_state_error(error: crate::HubStateStoreError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_state(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

fn daemon_entrypoint_error(error: EntrypointSupervisorError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_entrypoint(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

fn daemon_coordination(
    kind: DaemonResponseKind,
    coordination: DaemonCoordination,
) -> DaemonResponse {
    let mut response = daemon_response_base(kind);
    response.coordination = Some(coordination);
    response
}

fn daemon_plugin_tools(plugin_tools: Vec<McpToolDescriptor>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginMcpTools);
    response.plugin_tools = plugin_tools
        .into_iter()
        .map(|tool| serde_json::to_value(tool).unwrap_or(Value::Null))
        .collect();
    response
}

fn daemon_plugin_tool_result(plugin_tool_result: Value) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginMcpToolResult);
    response.plugin_tool_result = plugin_tool_result;
    response
}

fn daemon_plugin_surface(plugin_surface: UiNode) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginSurface);
    response.plugin_surface = Some(serde_json::to_value(plugin_surface).unwrap_or(Value::Null));
    response
}

fn daemon_plugin_action_result(plugin_action_result: UiActionResult) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginActionResult);
    if matches!(
        plugin_action_result.state,
        UiActionResultState::Rejected | UiActionResultState::Error
    ) {
        response.diagnostics = vec![DaemonDiagnostic::action_failure(
            "plugin_surface_action",
            "plugin surface action did not complete successfully",
        )];
    }
    response.plugin_action_result =
        Some(serde_json::to_value(plugin_action_result).unwrap_or(Value::Null));
    response
}

fn daemon_plugin_tool_error(error: crate::McpToolError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: error.code,
        request_id: "daemon-plugin-mcp-call".to_string(),
        operation: "plugin_mcp_call".to_string(),
        message: error.message,
        diagnostics: Vec::new(),
    });
    response
}

fn daemon_coordination_identity(identity: DaemonIdentity) -> DaemonCoordination {
    DaemonCoordination {
        identity: Some(identity),
        publish: None,
        messages: Vec::new(),
        next_cursor: None,
        ack: None,
        notify: None,
    }
}

fn daemon_coordination_publish(deliveries: Vec<EnvelopeDeliveryState>) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: Some(DaemonEnvelopePublish {
            deliveries: deliveries
                .into_iter()
                .map(daemon_envelope_delivery_from_state)
                .collect(),
        }),
        messages: Vec::new(),
        next_cursor: None,
        ack: None,
        notify: None,
    }
}

fn daemon_coordination_messages(
    envelopes: Vec<RoutedEnvelope>,
    next_cursor: Option<EnvelopeCursor>,
) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: None,
        messages: envelopes
            .into_iter()
            .map(daemon_envelope_from_routed)
            .collect(),
        next_cursor: next_cursor.map(|cursor| cursor.0),
        ack: None,
        notify: None,
    }
}

fn daemon_coordination_ack(state: Option<EnvelopeDeliveryState>) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: None,
        messages: Vec::new(),
        next_cursor: None,
        ack: Some(daemon_envelope_ack_from_state(state)),
        notify: None,
    }
}

fn daemon_coordination_notify(
    decision: GuardedWriteDecision,
    states: Vec<GuardedWriteDeliveryState>,
) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: None,
        messages: Vec::new(),
        next_cursor: None,
        ack: None,
        notify: Some(DaemonNotify {
            decision: format!("{decision:?}"),
            state_count: states.len(),
            states: states
                .into_iter()
                .map(guarded_write_delivery_state_label)
                .map(ToString::to_string)
                .collect(),
        }),
    }
}

fn daemon_envelope_delivery_from_state(state: EnvelopeDeliveryState) -> DaemonEnvelopeDelivery {
    DaemonEnvelopeDelivery {
        envelope_id: state.envelope_id.0,
        target: envelope_target_label(&state.target),
        cursor: state.cursor.0,
        status: format!("{:?}", state.status).to_ascii_lowercase(),
    }
}

fn daemon_envelope_from_routed(envelope: RoutedEnvelope) -> DaemonEnvelope {
    DaemonEnvelope {
        envelope_id: envelope.id.0,
        source: envelope.source.0,
        content_type: envelope.payload.content_type,
        body: String::from_utf8_lossy(&envelope.payload.body).to_string(),
        created_at: envelope.created_at,
        cursor: envelope.cursor.map(|cursor| cursor.0),
    }
}

fn daemon_envelope_ack_from_state(state: Option<EnvelopeDeliveryState>) -> DaemonEnvelopeAck {
    match state {
        Some(state) => DaemonEnvelopeAck {
            envelope_id: Some(state.envelope_id.0),
            target: Some(envelope_target_label(&state.target)),
            cursor: Some(state.cursor.0),
            status: format!("{:?}", state.status).to_ascii_lowercase(),
        },
        None => DaemonEnvelopeAck {
            envelope_id: None,
            target: None,
            cursor: None,
            status: "unknown".to_string(),
        },
    }
}

fn daemon_package_from_client(package: HubClientPackage) -> DaemonPackage {
    DaemonPackage {
        package_name: package.package_name,
        version: package.version,
        classification: package_classification_label(package.classification).to_string(),
        state: package_state_label(package.state).to_string(),
        requested_capabilities: package
            .requested_capabilities
            .into_iter()
            .map(|capability| DaemonCapability {
                surface: capability.surface,
                scope: capability.scope,
            })
            .collect(),
        runnable_entrypoints: package
            .runnable_entrypoints
            .into_iter()
            .map(|entrypoint| DaemonPackageRunnableEntrypoint {
                id: entrypoint.id,
                kind: entrypoint.kind,
                command: entrypoint.command,
                args: entrypoint.args,
                working_directory: DaemonPackageWorkingDirectory {
                    policy: entrypoint.working_directory.policy,
                    path: entrypoint.working_directory.path,
                },
                environment: entrypoint
                    .environment
                    .into_iter()
                    .map(|requirement| DaemonPackageEnvironmentRequirement {
                        name: requirement.name,
                        required: requirement.required,
                        default: requirement.default,
                        description: requirement.description,
                    })
                    .collect(),
                mode: entrypoint.mode,
                capabilities: entrypoint
                    .capabilities
                    .into_iter()
                    .map(|capability| DaemonCapability {
                        surface: capability.surface,
                        scope: capability.scope,
                    })
                    .collect(),
                may_supervise: entrypoint.may_supervise,
                process: DaemonPackageProcess {
                    state: entrypoint.process.state,
                    pid: entrypoint.process.pid,
                    started_at: entrypoint.process.started_at,
                    exited_at: entrypoint.process.exited_at,
                    exit_status: entrypoint.process.exit_status,
                    diagnostics: entrypoint
                        .process
                        .diagnostics
                        .into_iter()
                        .map(|diagnostic| DaemonPackageDiagnostic {
                            kind: diagnostic.kind,
                            message: diagnostic.message,
                        })
                        .collect(),
                },
            })
            .collect(),
        provider_profile_admitted: package.provider_profile_admitted,
    }
}

fn daemon_package_decision_from_policy(decision: PackageDecision) -> DaemonPackageDecision {
    DaemonPackageDecision {
        package_name: decision.package_name,
        action: package_action_label(decision.action).to_string(),
        state: package_state_label(decision.state.into()).to_string(),
        classification: package_classification_label(decision.classification.into()).to_string(),
    }
}

fn daemon_plugin_lifecycle_from_client(
    lifecycle: HubClientPluginLifecycle,
) -> DaemonPluginLifecycle {
    DaemonPluginLifecycle {
        package_name: lifecycle.package_name,
        state: package_state_label(lifecycle.state).to_string(),
        loaded: lifecycle.loaded,
    }
}

fn daemon_status_from_status(status: &HubDaemonStatus, session_count: usize) -> DaemonStatus {
    DaemonStatus {
        lifecycle_state: match status.lifecycle_state {
            crate::HubDaemonState::Created => "created",
            crate::HubDaemonState::Running => "running",
            crate::HubDaemonState::Stopped => "stopped",
        }
        .to_string(),
        compatibility: DaemonCompatibility::current(),
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
        recovered_sessions: status
            .recovered_sessions
            .iter()
            .map(|session_id| session_id.0.clone())
            .collect(),
        stale_sessions: status
            .stale_sessions
            .iter()
            .map(|session_id| session_id.0.clone())
            .collect(),
        diagnostics: Vec::new(),
    }
}

fn daemon_session_from_client(session: HubClientSession) -> DaemonSession {
    DaemonSession {
        session_id: session.session_id.0,
        lifecycle: lifecycle_label(&session.lifecycle).to_string(),
    }
}

fn daemon_operator_error_from_client(error: crate::HubClientError) -> DaemonOperatorError {
    match error {
        crate::HubClientError::AdmissionDenied {
            request_id,
            operation,
            role,
        } => DaemonOperatorError {
            code: "admission_denied".to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message: format!("{role:?} is not allowed to run {operation:?}"),
            diagnostics: Vec::new(),
        },
        crate::HubClientError::Runtime {
            request_id,
            operation,
            kind,
        } => {
            let operation_label = operation_label(operation).to_string();
            let message = format!("runtime failed while handling {operation:?}: {kind:?}");
            DaemonOperatorError {
                code: runtime_error_code(kind).to_string(),
                request_id: request_id.0,
                diagnostics: runtime_error_diagnostics(operation, kind, &message),
                operation: operation_label,
                message,
            }
        }
        crate::HubClientError::UnsupportedDaemonOperation {
            request_id,
            operation,
            daemon_operation,
        } => DaemonOperatorError {
            code: "unsupported_daemon_operation".to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message: format!("{daemon_operation} is not supported by the daemon"),
            diagnostics: vec![DaemonDiagnostic::unsupported_feature(daemon_operation)],
        },
        crate::HubClientError::PackageCapabilityDenied {
            request_id,
            operation,
            package_name,
        } => DaemonOperatorError {
            code: "package_capability_denied".to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message: format!("{package_name} is not allowed to run {operation:?}"),
            diagnostics: Vec::new(),
        },
        crate::HubClientError::Plugin {
            request_id,
            operation,
            code,
            message,
        } => DaemonOperatorError {
            code,
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message,
            diagnostics: Vec::new(),
        },
    }
}

fn daemon_operator_error_from_package(error: crate::PackageRegistryError) -> DaemonOperatorError {
    let package_name = package_error_display_name(&error);
    DaemonOperatorError {
        code: "package_policy_error".to_string(),
        request_id: "daemon-package-mutation".to_string(),
        operation: package_action_label(error.action).to_string(),
        message: format!(
            "package {} denied for {}: {:?}",
            package_name,
            package_action_label(error.action),
            error.reason
        ),
        diagnostics: Vec::new(),
    }
}

fn package_error_display_name(error: &crate::PackageRegistryError) -> &str {
    match error.reason {
        PackageAdmissionReason::InvalidLocalManifest(_)
        | PackageAdmissionReason::UnsafeLocalPath(_) => "<local-package>",
        _ => &error.package_name,
    }
}

fn daemon_operator_error_from_state(error: crate::HubStateStoreError) -> DaemonOperatorError {
    DaemonOperatorError {
        code: "hub_state_error".to_string(),
        request_id: "daemon-package-mutation".to_string(),
        operation: "persist_package_registry".to_string(),
        message: format!("failed to persist package registry: {error}"),
        diagnostics: Vec::new(),
    }
}

fn daemon_operator_error_from_entrypoint(error: EntrypointSupervisorError) -> DaemonOperatorError {
    let (code, message) = match error {
        EntrypointSupervisorError::PackageNotInstalled(package_name) => (
            "package_not_installed",
            format!("package {package_name} is not installed"),
        ),
        EntrypointSupervisorError::PackageDisabled(package_name) => (
            "package_disabled",
            format!("package {package_name} is not enabled"),
        ),
        EntrypointSupervisorError::PackageNotLocal(package_name) => (
            "package_not_local",
            format!("package {package_name} is not a local package"),
        ),
        EntrypointSupervisorError::EntrypointNotFound {
            package_name,
            entrypoint_id,
        } => (
            "entrypoint_not_found",
            format!("package {package_name} has no runnable entrypoint {entrypoint_id}"),
        ),
        EntrypointSupervisorError::EntrypointNotSupervisable {
            package_name,
            entrypoint_id,
        } => (
            "entrypoint_not_supervisable",
            format!("package {package_name} entrypoint {entrypoint_id} is not marked supervisable"),
        ),
        EntrypointSupervisorError::Io(error) => (
            "entrypoint_io_error",
            format!("entrypoint process error: {error}"),
        ),
    };
    DaemonOperatorError {
        code: code.to_string(),
        request_id: "daemon-package-entrypoint".to_string(),
        operation: "package_entrypoint".to_string(),
        message,
        diagnostics: Vec::new(),
    }
}

fn daemon_event_from_client(event: HubClientEvent) -> DaemonEvent {
    match event {
        HubClientEvent::SessionLifecycle { session_id, state } => DaemonEvent::SessionLifecycle {
            session_id: session_id.0,
            state: lifecycle_label(&state).to_string(),
        },
        HubClientEvent::TerminalOutput {
            session_id,
            subscription_id,
            data,
        } => DaemonEvent::TerminalOutput {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            data: String::from_utf8_lossy(&data).to_string(),
        },
        HubClientEvent::Snapshot {
            session_id,
            subscription_id,
            bytes,
        } => DaemonEvent::Snapshot {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            bytes,
        },
        HubClientEvent::Scrollback {
            session_id,
            subscription_id,
            bytes,
        } => DaemonEvent::Scrollback {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            bytes,
        },
        HubClientEvent::ProcessExit {
            session_id,
            subscription_id,
            code,
        } => DaemonEvent::ProcessExit {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            code,
        },
        HubClientEvent::AttachState {
            session_id,
            subscription_id,
            state,
        } => DaemonEvent::AttachState {
            session_id: session_id.0,
            subscription_id: subscription_id.0,
            state: attach_state_label(&state).to_string(),
        },
        HubClientEvent::RuntimeObservation { kind } => DaemonEvent::RuntimeObservation {
            kind: match kind {
                crate::HubClientObservationKind::SessionActivity => "session_activity",
                crate::HubClientObservationKind::Subscription => "subscription",
                crate::HubClientObservationKind::Backpressure => "backpressure",
                crate::HubClientObservationKind::RoutedEnvelope => "routed_envelope",
            }
            .to_string(),
        },
    }
}

fn shutdown_error_is_unknown_session(error: &crate::HubClientError) -> bool {
    matches!(
        error,
        crate::HubClientError::Runtime {
            operation: crate::HubClientOperation::Shutdown,
            kind: crate::HubClientRuntimeErrorKind::UnknownSession,
            ..
        }
    )
}

fn runtime_error_code(kind: crate::HubClientRuntimeErrorKind) -> &'static str {
    match kind {
        crate::HubClientRuntimeErrorKind::UnknownSession => "unknown_session",
        crate::HubClientRuntimeErrorKind::Runtime => "runtime_error",
        crate::HubClientRuntimeErrorKind::State => "state_error",
    }
}

fn runtime_error_diagnostics(
    operation: crate::HubClientOperation,
    kind: crate::HubClientRuntimeErrorKind,
    message: &str,
) -> Vec<DaemonDiagnostic> {
    if kind == crate::HubClientRuntimeErrorKind::UnknownSession
        && matches!(
            operation,
            crate::HubClientOperation::Attach | crate::HubClientOperation::DrainRuntime
        )
    {
        return vec![DaemonDiagnostic::terminal_stream_unavailable(
            operation_label(operation),
            message,
        )];
    }

    Vec::new()
}

fn operation_label(operation: crate::HubClientOperation) -> &'static str {
    match operation {
        crate::HubClientOperation::Status => "status",
        crate::HubClientOperation::ListSessions => "list_sessions",
        crate::HubClientOperation::Spawn => "spawn",
        crate::HubClientOperation::Attach => "attach",
        crate::HubClientOperation::Detach => "detach",
        crate::HubClientOperation::Input => "input",
        crate::HubClientOperation::Resize => "resize",
        crate::HubClientOperation::DrainRuntime => "drain_runtime",
        crate::HubClientOperation::Shutdown => "shutdown",
        crate::HubClientOperation::GuardedNotificationWrite => "guarded_notification_write",
        crate::HubClientOperation::NotifySession => "notify_session",
        crate::HubClientOperation::PublishRoutedEnvelope => "publish_routed_envelope",
        crate::HubClientOperation::DrainRoutedEnvelopes => "drain_routed_envelopes",
        crate::HubClientOperation::AcknowledgeRoutedEnvelope => "acknowledge_routed_envelope",
        crate::HubClientOperation::ReadScreen => "read_screen",
        crate::HubClientOperation::CaptureSnapshot => "capture_snapshot",
        crate::HubClientOperation::ListPackages => "list_packages",
        crate::HubClientOperation::PluginLifecycleStatus => "plugin_lifecycle_status",
        crate::HubClientOperation::PluginSurfaceRender => "plugin_surface_render",
        crate::HubClientOperation::PluginSurfaceAction => "plugin_surface_action",
    }
}

fn envelope_target_label(target: &EnvelopeTarget) -> String {
    match target {
        EnvelopeTarget::Endpoint { endpoint_id } => format!("endpoint:{}", endpoint_id.0),
        EnvelopeTarget::Client { client_id } => format!("client:{}", client_id.0),
        EnvelopeTarget::Session { session_id } => format!("session:{}", session_id.0),
        EnvelopeTarget::Subscription {
            session_id,
            subscription_id,
        } => format!("subscription:{}:{}", session_id.0, subscription_id.0),
        EnvelopeTarget::Plugin { plugin_key } => format!("plugin:{}", plugin_key.0),
        EnvelopeTarget::Stream { stream } => format!("stream:{stream}"),
        EnvelopeTarget::Topic { topic } => format!("topic:{topic}"),
    }
}

fn package_classification_label(classification: HubClientPackageClassification) -> &'static str {
    match classification {
        HubClientPackageClassification::Plugin => "plugin",
        HubClientPackageClassification::Provider => "provider",
    }
}

fn package_state_label(state: crate::HubClientPackageState) -> &'static str {
    match state {
        crate::HubClientPackageState::Installed => "installed",
        crate::HubClientPackageState::Enabled => "enabled",
        crate::HubClientPackageState::Disabled => "disabled",
    }
}

fn guarded_write_delivery_state_label(state: GuardedWriteDeliveryState) -> &'static str {
    match state {
        GuardedWriteDeliveryState::Accepted => "accepted",
        GuardedWriteDeliveryState::Deferred => "deferred",
        GuardedWriteDeliveryState::Rejected => "rejected",
        GuardedWriteDeliveryState::Written => "written",
        GuardedWriteDeliveryState::Delivered => "delivered",
        GuardedWriteDeliveryState::Acknowledged => "acknowledged",
    }
}

fn package_action_label(action: PackageAction) -> &'static str {
    match action {
        PackageAction::Install => "install",
        PackageAction::Show => "show",
        PackageAction::Enable => "enable",
        PackageAction::Disable => "disable",
        PackageAction::Remove => "remove",
        PackageAction::Pin => "pin",
        PackageAction::Prepare => "prepare",
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
    Compatibility(botster_hub_client::DaemonCompatibilityError),
    UnexpectedResponse,
    DaemonNotRunning,
    ControlThreadStopped,
    Io(std::io::Error),
    Json(serde_json::Error),
    Daemon(crate::HubDaemonError),
    Client(crate::HubClientError),
    Package(crate::PackageRegistryError),
    State(crate::HubStateStoreError),
    Entrypoint(EntrypointSupervisorError),
    Runtime(crate::HubRuntimeError),
    Lifecycle(crate::HubLifecycleError),
}

impl fmt::Display for DaemonTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSocketBinding => write!(formatter, "local socket transport is disabled"),
            Self::NotRunning => write!(formatter, "daemon not running"),
            Self::AlreadyRunning => write!(formatter, "daemon already running"),
            Self::ClientDisconnected => write!(formatter, "client disconnected"),
            Self::Protocol(message) => write!(formatter, "daemon protocol error: {message}"),
            Self::Compatibility(error) => write!(formatter, "{error}"),
            Self::UnexpectedResponse => write!(formatter, "unexpected daemon response"),
            Self::DaemonNotRunning => write!(formatter, "daemon runtime is not running"),
            Self::ControlThreadStopped => write!(formatter, "daemon control thread stopped"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Daemon(error) => write!(formatter, "{error}"),
            Self::Client(error) => write!(formatter, "{error:?}"),
            Self::Package(error) => write!(formatter, "{error:?}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::Entrypoint(error) => write!(formatter, "{error:?}"),
            Self::Runtime(error) => write!(formatter, "{error:?}"),
            Self::Lifecycle(error) => write!(formatter, "{error:?}"),
        }
    }
}

impl Error for DaemonTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Compatibility(error) => Some(error),
            Self::Daemon(error) => Some(error),
            Self::State(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::HubDaemonError> for DaemonTransportError {
    fn from(error: crate::HubDaemonError) -> Self {
        Self::Daemon(error)
    }
}

impl From<ClientDaemonTransportError> for DaemonTransportError {
    fn from(error: ClientDaemonTransportError) -> Self {
        match error {
            ClientDaemonTransportError::Io(error) => Self::Io(error),
            ClientDaemonTransportError::Json(error) => Self::Json(error),
            ClientDaemonTransportError::MissingSocketBinding => Self::MissingSocketBinding,
            ClientDaemonTransportError::AlreadyRunning => Self::AlreadyRunning,
            ClientDaemonTransportError::NotRunning => Self::NotRunning,
            ClientDaemonTransportError::ClientDisconnected => Self::ClientDisconnected,
            ClientDaemonTransportError::Protocol(message) => Self::Protocol(message),
            ClientDaemonTransportError::Compatibility(error) => Self::Compatibility(error),
            ClientDaemonTransportError::ControlThreadStopped => Self::ControlThreadStopped,
        }
    }
}

impl From<crate::HubClientError> for DaemonTransportError {
    fn from(error: crate::HubClientError) -> Self {
        Self::Client(error)
    }
}

impl From<crate::PackageRegistryError> for DaemonTransportError {
    fn from(error: crate::PackageRegistryError) -> Self {
        Self::Package(error)
    }
}

impl From<crate::HubStateStoreError> for DaemonTransportError {
    fn from(error: crate::HubStateStoreError) -> Self {
        Self::State(error)
    }
}

impl From<EntrypointSupervisorError> for DaemonTransportError {
    fn from(error: EntrypointSupervisorError) -> Self {
        Self::Entrypoint(error)
    }
}

impl From<crate::HubRuntimeError> for DaemonTransportError {
    fn from(error: crate::HubRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<crate::HubLifecycleError> for DaemonTransportError {
    fn from(error: crate::HubLifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}

/// Result alias for daemon socket transport operations.
pub type DaemonTransportResult<T> = Result<T, DaemonTransportError>;
