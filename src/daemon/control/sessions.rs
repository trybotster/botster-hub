//! Session and remaining runtime-borrow request family.

use std::collections::BTreeMap;

use botster_core::{
    ClientId, EndpointId, EnvelopeCursor, EnvelopeId, EnvelopeTarget, RoutedEnvelope,
    RoutedEnvelopePayload, SessionId, SubscriptionId,
};
use botster_core_daemon::ReadinessEvidence;
use botster_hub_client::{
    DaemonDiagnostic, DaemonIdentity, DaemonRequest, DaemonResponse, DaemonResponseKind,
};
use serde_json::Value;

use crate::HubDaemon;
use crate::admission::unix_hello::{
    UnixTerminalAdmission, WebrtcTerminalAdmission, terminal_compatibility_attach_error,
};
use crate::client_api::HubClientApi;
use crate::client_api_dto::plugin::{
    daemon_coordination_ack, daemon_coordination_identity, daemon_coordination_messages,
    daemon_coordination_notify, daemon_coordination_publish,
};
use crate::client_api_dto::response::{
    daemon_capture_snapshot, daemon_coordination, daemon_events, daemon_mode_flags,
    daemon_mode_gated_input, daemon_plugin_action_result, daemon_plugin_surface,
    daemon_plugin_tool_result, daemon_plugin_tools, daemon_read_screen,
    daemon_resolved_session_type, daemon_response_base, daemon_session_cleanup,
    daemon_session_context, daemon_session_type_definition, daemon_session_types, daemon_sessions,
    daemon_spawned, daemon_status, daemon_unknown_session_cleanup,
};
use crate::client_api_dto::session::{
    daemon_session_from_client, session_type_definition_from_daemon,
    session_type_mutation_source_from_daemon, session_type_request_from_daemon,
};
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, daemon_plugin_tool_error};
use crate::daemon::owner_loop::PendingRuntimeState;
use crate::daemon::shutdown::{
    ShutdownSessionClassification, classify_shutdown_session, recover_after_core_shutdown_error,
};
use crate::daemon_projection::daemon_status_from_status;
use crate::maintenance::{installation_identity, software_identity};
use crate::subscription::attach_routes::{
    AttachStreamOwner, BoundAdapterHandle, UnixBindRequest, WebrtcBindRequest,
    bind_unix_adapter_after_attaching, bind_webrtc_adapter_after_attaching,
    fail_closed_pre_bind_attach, forward_attach_bootstrap, live_generation_for_route,
};
use crate::subscription::closed_events::{
    suppress_unix_session_close_events, suppress_webrtc_session_close_events,
};
use crate::subscription::entity::entity_subscription_error;
use crate::{HubClientRequest, HubClientResponseBody};

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    pending_runtime: &mut PendingRuntimeState,
    observability: DaemonObservability<'_>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let status = daemon.status();
    let api = HubClientApi::local_operator(
        observability
            .client_id
            .map(str::to_string)
            .unwrap_or_else(|| super::runtime_client_id(&request)),
    );
    let packages = daemon.package_registry().clone();
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };

    match request {
        DaemonRequest::SubscribeEntities { .. } | DaemonRequest::UnsubscribeEntities { .. } => {
            Err(DaemonTransportError::Protocol(
                "entity subscriptions require the held-open stream handler",
            ))
        }
        DaemonRequest::SubscribeEvents { .. } | DaemonRequest::UnsubscribeEvents { .. } => {
            Err(DaemonTransportError::Protocol(
                "package event subscriptions require the host event handler",
            ))
        }
        DaemonRequest::RemoveSession { session_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::RemoveSession {
                    request_id: request_id("daemon-session-remove"),
                    session_id: SessionId(session_id.clone()),
                },
            )?;
            let HubClientResponseBody::SessionRemoved(removed) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            if !removed {
                return Ok(entity_subscription_error(
                    "session_not_terminal",
                    "daemon-session-remove",
                    "session must be terminal before it can be removed",
                ));
            }
            suppress_unix_session_close_events(pending_runtime, &session_id);
            suppress_webrtc_session_close_events(pending_runtime, &session_id);
            Ok(daemon_response_base(DaemonResponseKind::SessionRemoved))
        }
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
            Ok(daemon_status(
                status,
                client_status.session_count,
                observability.egress.diagnostics(),
                observability.lifecycle.clone(),
                runtime.event_plane_counters_snapshot(),
            ))
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
                    now_seconds: crate::daemon::owner_loop::tick(logical_clock),
                },
            )?;
            let HubClientResponseBody::Spawned(spawned) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            drain_cursors.insert(spawned.session.session_id.0.clone(), *logical_clock);
            Ok(daemon_spawned(
                daemon_session_from_client(spawned.session),
                super::events::events_from_client(spawned.events),
            ))
        }
        DaemonRequest::Attach {
            session_id,
            subscription_id,
        } => {
            let now = crate::daemon::owner_loop::tick(logical_clock);
            let client_id = observability
                .client_id
                .unwrap_or("botster-hub-daemon-socket")
                .to_string();
            if let Some(UnixTerminalAdmission::Rejected { code, diagnostic }) =
                pending_runtime.admission.unix_admissions.get(&client_id)
            {
                return Ok(terminal_compatibility_attach_error(
                    code,
                    diagnostic.clone(),
                ));
            }
            if let Some(grant_id) = observability.grant_id
                && let Some(WebrtcTerminalAdmission::Rejected { code, diagnostic }) =
                    pending_runtime.admission.webrtc_admissions.get(grant_id)
            {
                return Ok(terminal_compatibility_attach_error(
                    code,
                    diagnostic.clone(),
                ));
            }
            let owner = AttachStreamOwner {
                client_id: client_id.clone(),
                grant_id: observability.grant_id.map(str::to_string),
            };
            let previous_generation = live_generation_for_route(
                &runtime.list_terminal_subscriptions(),
                &client_id,
                &session_id,
                &subscription_id,
            );
            pending_runtime.start_attach(owner, session_id.clone(), subscription_id.clone());
            if let Some(generation) = previous_generation {
                let _ = runtime.detach_terminal_subscription(
                    ClientId(client_id.clone()),
                    SessionId(session_id.clone()),
                    SubscriptionId(subscription_id.clone()),
                    generation,
                    now,
                );
            }
            let bootstrap_egress = match pending_runtime.begin_core_attach(
                runtime,
                &session_id,
                &subscription_id,
                now,
            ) {
                Ok(egress) => egress,
                Err(_) => {
                    pending_runtime.cancel_stream(&session_id, &subscription_id);
                    return Ok(super::attach_bind_operator_error(
                        "invalid_request",
                        "attach failed before adapter bind",
                    ));
                }
            };
            let unix_admission = pending_runtime
                .admission
                .unix_admissions
                .get(&client_id)
                .cloned();
            let webrtc_admission = observability.grant_id.and_then(|grant_id| {
                pending_runtime
                    .admission
                    .webrtc_admissions
                    .get(grant_id)
                    .cloned()
            });
            if observability.grant_id.is_some() {
                let Some(WebrtcTerminalAdmission::Admitted {
                    required_features,
                    mux,
                    terminal_requirement,
                }) = webrtc_admission.as_ref()
                else {
                    fail_closed_pre_bind_attach(
                        pending_runtime,
                        runtime,
                        &client_id,
                        &session_id,
                        &subscription_id,
                        now,
                        None,
                    );
                    return Ok(super::attach_bind_operator_error(
                        "invalid_request",
                        "Attach requires an admitted WebRTC adapter",
                    ));
                };
                return match bind_webrtc_adapter_after_attaching(
                    pending_runtime,
                    runtime,
                    WebrtcBindRequest {
                        client_id: &client_id,
                        session_id: &session_id,
                        subscription_id: &subscription_id,
                        required_features,
                        terminal_requirement: terminal_requirement.as_ref(),
                        now_seconds: now,
                        mux: Some(mux),
                    },
                ) {
                    Ok(handle) => {
                        if let Some(handle) = handle {
                            forward_attach_bootstrap(
                                &BoundAdapterHandle::WebRtc(handle),
                                &bootstrap_egress,
                            );
                        }
                        Ok(daemon_events(Vec::new()))
                    }
                    Err(_) => Ok(super::attach_bind_operator_error(
                        "invalid_request",
                        "Attach failed to bind a WebRTC adapter",
                    )),
                };
            }
            let Some(UnixTerminalAdmission::Admitted {
                capabilities, mux, ..
            }) = unix_admission.as_ref()
            else {
                fail_closed_pre_bind_attach(
                    pending_runtime,
                    runtime,
                    &client_id,
                    &session_id,
                    &subscription_id,
                    now,
                    None,
                );
                return Ok(super::attach_bind_operator_error(
                    "invalid_request",
                    "Attach requires an admitted Unix adapter",
                ));
            };
            match bind_unix_adapter_after_attaching(
                pending_runtime,
                runtime,
                UnixBindRequest {
                    client_id: &client_id,
                    session_id: &session_id,
                    subscription_id: &subscription_id,
                    capabilities: capabilities.clone(),
                    now_seconds: now,
                    mux: Some(mux),
                },
            ) {
                Ok(handle) => {
                    if let Some(handle) = handle {
                        forward_attach_bootstrap(
                            &BoundAdapterHandle::Unix(handle),
                            &bootstrap_egress,
                        );
                    }
                    Ok(daemon_events(Vec::new()))
                }
                Err(_) => Ok(super::attach_bind_operator_error(
                    "invalid_request",
                    "Attach failed to bind a Unix adapter",
                )),
            }
        }
        DaemonRequest::Detach {
            session_id,
            subscription_id,
        } => {
            let now = crate::daemon::owner_loop::tick(logical_clock);
            let tracked_session_id = session_id.clone();
            let tracked_subscription_id = subscription_id.clone();
            let generation = observability.client_id.and_then(|client_id| {
                live_generation_for_route(
                    &runtime.list_terminal_subscriptions(),
                    client_id,
                    &tracked_session_id,
                    &tracked_subscription_id,
                )
            });
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
            if let Some(client_id) = observability.client_id
                && let Some(UnixTerminalAdmission::Admitted { mux, .. }) =
                    pending_runtime.admission.unix_admissions.get(client_id)
                && let Some(generation) = generation
            {
                mux.suppress_generation(
                    tracked_session_id.clone(),
                    tracked_subscription_id.clone(),
                    generation.0,
                );
            }
            if let Some(grant_id) = observability.grant_id
                && let Some(WebrtcTerminalAdmission::Admitted { mux, .. }) =
                    pending_runtime.admission.webrtc_admissions.get(grant_id)
                && let Some(generation) = generation
            {
                mux.suppress_generation(
                    tracked_session_id.clone(),
                    tracked_subscription_id.clone(),
                    generation.0,
                );
            }
            pending_runtime.close_adapter(&tracked_session_id, &tracked_subscription_id);
            pending_runtime.cancel_stream(&tracked_session_id, &tracked_subscription_id);
            super::events_response(response.body)
        }
        DaemonRequest::SendInput { session_id, data } => {
            let data = data.into_bytes();
            let now = crate::daemon::owner_loop::tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Input {
                    request_id: request_id("daemon-sessions-send-input"),
                    session_id: SessionId(session_id),
                    data,
                    now_seconds: now,
                },
            )?;
            super::events_response(response.body)
        }
        DaemonRequest::ModeGatedInput {
            session_id,
            data,
            mode_generation,
            mode_revision,
        } => {
            let data = data.into_bytes();
            let now = crate::daemon::owner_loop::tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ModeGatedInput {
                    request_id: request_id("daemon-sessions-mode-gated-input"),
                    session_id: SessionId(session_id),
                    data,
                    mode_generation,
                    mode_revision,
                    now_seconds: now,
                },
            )?;
            let HubClientResponseBody::ModeGatedInput(result) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_mode_gated_input(result))
        }
        DaemonRequest::Resize {
            session_id,
            rows,
            cols,
        } => {
            let now = crate::daemon::owner_loop::tick(logical_clock);
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
            super::events_response(response.body)
        }
        DaemonRequest::ShutdownSession { session_id } => {
            let now = crate::daemon::owner_loop::tick(logical_clock);
            match classify_shutdown_session(runtime, &session_id, now) {
                Ok(ShutdownSessionClassification::Cleanup(cleanup)) => {
                    // Keep adapters open. Classify already asked Core to write
                    // ProcessExited. Host close abandons that in-flight frame.
                    return Ok(daemon_session_cleanup(cleanup));
                }
                Ok(ShutdownSessionClassification::Missing) => {
                    pending_runtime.close_adapters_for_session(&session_id);
                    return Ok(daemon_unknown_session_cleanup(&session_id));
                }
                Ok(ShutdownSessionClassification::Active)
                | Ok(ShutdownSessionClassification::Stopping)
                | Err(_) => {}
            }
            suppress_unix_session_close_events(pending_runtime, &session_id);
            suppress_webrtc_session_close_events(pending_runtime, &session_id);
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
                    pending_runtime.close_adapters_for_session(&session_id);
                    let response = recover_after_core_shutdown_error(
                        runtime,
                        &session_id,
                        error,
                        logical_clock,
                    )?;
                    return Ok(response);
                }
            };
            super::events_response(response.body)
        }
        DaemonRequest::Drain {
            session_id,
            subscription_id,
        } => {
            let session_known = runtime.list_sessions().ok().is_some_and(|sessions| {
                sessions
                    .iter()
                    .any(|session| session.session_id.0 == session_id)
            });
            if !session_known {
                return Ok(super::missing_session_drain_error(&session_id));
            }
            if let Some(subscription_id) = subscription_id {
                pending_runtime.authorize_drain(
                    &session_id,
                    &subscription_id,
                    observability.client_id,
                    observability.grant_id,
                )?;
            }
            Ok(daemon_events(Vec::new()))
        }
        DaemonRequest::ReadScreen { session_id } => {
            let now = crate::daemon::owner_loop::tick(logical_clock);
            let _ = runtime.observe_session_lifecycle(&SessionId(session_id.clone()), now);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ReadScreen {
                    request_id: request_id("daemon-sessions-read-screen"),
                    session_id: SessionId(session_id),
                    now_seconds: now,
                },
            )?;
            let HubClientResponseBody::ReadScreen(screen) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_read_screen(screen))
        }
        DaemonRequest::ReadModeFlags { session_id } => {
            let now = crate::daemon::owner_loop::tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ReadModeFlags {
                    request_id: request_id("daemon-sessions-read-mode-flags"),
                    session_id: SessionId(session_id),
                    now_seconds: now,
                },
            )?;
            let HubClientResponseBody::ModeFlags(mode_flags) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_mode_flags(mode_flags))
        }
        DaemonRequest::CaptureSnapshot { session_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::CaptureSnapshot {
                    request_id: request_id("daemon-sessions-capture-snapshot"),
                    session_id: SessionId(session_id),
                    now_seconds: crate::daemon::owner_loop::tick(logical_clock),
                },
            )?;
            let HubClientResponseBody::CaptureSnapshot(snapshot) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_capture_snapshot(snapshot))
        }
        DaemonRequest::ListSessionTypes => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ListSessionTypes {
                    request_id: request_id("daemon-session-types-list"),
                },
            )?;
            let HubClientResponseBody::SessionTypes(templates) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(templates))
        }
        DaemonRequest::ListSessionTypesForTarget { target_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ListSessionTypesForTarget {
                    request_id: request_id("daemon-session-types-list-for-target"),
                    target_id,
                },
            )?;
            let HubClientResponseBody::SessionTypes(templates) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(templates))
        }
        DaemonRequest::ShowSessionType { session_type_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ShowSessionType {
                    request_id: request_id("daemon-session-types-show"),
                    session_type_id,
                },
            )?;
            let HubClientResponseBody::SessionTypes(templates) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(templates))
        }
        DaemonRequest::ShowSessionTypeDefinition { session_type_id } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ShowSessionTypeDefinition {
                    request_id: request_id("daemon-session-types-definition"),
                    session_type_id,
                },
            )?;
            let HubClientResponseBody::SessionTypeDefinition(definition) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_type_definition(*definition))
        }
        DaemonRequest::CreateSessionType { source, definition } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::CreateSessionType {
                    request_id: request_id("daemon-session-types-create"),
                    source: session_type_mutation_source_from_daemon(source),
                    definition: session_type_definition_from_daemon(definition),
                },
            )?;
            let HubClientResponseBody::SessionTypes(session_types) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(session_types))
        }
        DaemonRequest::UpdateSessionType { source, definition } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::UpdateSessionType {
                    request_id: request_id("daemon-session-types-update"),
                    source: session_type_mutation_source_from_daemon(source),
                    definition: session_type_definition_from_daemon(definition),
                },
            )?;
            let HubClientResponseBody::SessionTypes(session_types) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(session_types))
        }
        DaemonRequest::DeleteSessionType {
            source,
            session_type_id,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::DeleteSessionType {
                    request_id: request_id("daemon-session-types-delete"),
                    source: session_type_mutation_source_from_daemon(source),
                    session_type_id,
                },
            )?;
            let HubClientResponseBody::SessionTypes(session_types) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(session_types))
        }
        DaemonRequest::ResolveSessionType {
            session_type_id,
            request,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ResolveSessionType {
                    request_id: request_id("daemon-session-types-resolve"),
                    session_type_id,
                    session_type_request: session_type_request_from_daemon(None, request),
                },
            )?;
            let HubClientResponseBody::ResolvedSessionType(resolved) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_resolved_session_type(*resolved))
        }
        DaemonRequest::SpawnSessionType {
            session_type_id,
            session_id,
            request,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::SpawnSessionType {
                    request_id: request_id("daemon-session-types-spawn"),
                    session_type_id,
                    session_type_request: session_type_request_from_daemon(
                        Some(SessionId(session_id)),
                        request,
                    ),
                    now_seconds: crate::daemon::owner_loop::tick(logical_clock),
                },
            )?;
            let HubClientResponseBody::Spawned(spawned) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            drain_cursors.insert(spawned.session.session_id.0.clone(), *logical_clock);
            Ok(daemon_spawned(
                daemon_session_from_client(spawned.session),
                super::events::events_from_client(spawned.events),
            ))
        }
        DaemonRequest::ReadSessionContext {
            session_id,
            context_id,
            key,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ReadSessionContext {
                    request_id: request_id("daemon-session-context-read"),
                    session_id: SessionId(session_id),
                    context_id,
                    key,
                },
            )?;
            let HubClientResponseBody::SessionContext(context) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_context(context))
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
            let now = crate::daemon::owner_loop::tick(logical_clock);
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
                    content_type: super::messaging::MESSAGE_CONTENT_TYPE.to_string(),
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
            let now = crate::daemon::owner_loop::tick(logical_clock);
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
            request,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::PluginSurfaceAction {
                    request_id: request_id("daemon-plugin-surface-action"),
                    package_name,
                    action: request,
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
                Vec::new(),
                observability.lifecycle.clone(),
                software_identity(),
                installation_identity(),
                runtime.event_plane_counters_snapshot(),
            )),
            sessions: Vec::new(),
            session_types: Vec::new(),
            session_type_definition: None,
            resolved_session_type: None,
            session_context: None,
            read_screen: None,
            mode_flags: None,
            mode_gated_input: None,
            capture_snapshot: None,
            spawn_targets: Vec::new(),
            spawn_target_validation: None,
            worktrees: Vec::new(),
            apps: Vec::new(),
            resolved_app_launch: None,
            resolved_package_route: None,
            package_navigation: Vec::new(),
            packages: Vec::new(),
            available_packages: Vec::new(),
            install_plan: None,
            update_status: None,
            hub_update: None,
            hub_update_execution: None,
            package_decision: None,
            lifecycle: Vec::new(),
            plugin_worker_counters: None,
            plugin_resource_counters: None,
            plugin_tools: Vec::new(),
            plugin_tool_result: Value::Null,
            plugin_surface: None,
            plugin_action_result: None,
            local_webrtc_bootstrap: None,
            local_webrtc_answer: None,
            events: Vec::new(),
            cleanup: None,
            coordination: None,
            error: None,
            diagnostics: vec![DaemonDiagnostic::connected("shutdown")],
        }),
        DaemonRequest::IssueLocalWebrtcBootstrap { .. }
        | DaemonRequest::LocalWebrtcSignal { .. } => Err(DaemonTransportError::UnexpectedResponse),
        DaemonRequest::CheckHubUpdate
        | DaemonRequest::StartHubUpdate { .. }
        | DaemonRequest::GetHubUpdateExecution => {
            unreachable!("Hub update requests are handled before runtime borrow")
        }
        DaemonRequest::ListApps
        | DaemonRequest::ResolveAppLaunch { .. }
        | DaemonRequest::ResolvePackageRoute { .. }
        | DaemonRequest::ListPackageNavigation
        | DaemonRequest::ListPackages
        | DaemonRequest::ListSpawnTargets
        | DaemonRequest::ShowSpawnTarget { .. }
        | DaemonRequest::CreateSpawnTarget { .. }
        | DaemonRequest::UpdateSpawnTarget { .. }
        | DaemonRequest::DeleteSpawnTarget { .. }
        | DaemonRequest::ValidateSpawnTarget { .. }
        | DaemonRequest::ListWorktrees
        | DaemonRequest::ShowWorktree { .. }
        | DaemonRequest::CreateWorktree { .. }
        | DaemonRequest::DeleteWorktree { .. }
        | DaemonRequest::ListAvailablePackages { .. }
        | DaemonRequest::InspectAvailablePackage { .. }
        | DaemonRequest::PreviewPackageInstall { .. }
        | DaemonRequest::InstallPackageRegistryEntry { .. }
        | DaemonRequest::InstallPackageLocalPath { .. }
        | DaemonRequest::CheckPackageUpdate { .. }
        | DaemonRequest::PreviewPackageUpdate { .. }
        | DaemonRequest::ApplyPackageUpdate { .. }
        | DaemonRequest::ShowPackage { .. }
        | DaemonRequest::SetPackageConfiguration { .. }
        | DaemonRequest::ReloadPackage { .. }
        | DaemonRequest::RefreshLocalPackages
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
