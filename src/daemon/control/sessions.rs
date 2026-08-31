//! Session request family.

use std::collections::BTreeMap;

use botster_core::{ClientId, SessionId, SubscriptionId};
use botster_hub_client::{DaemonRequest, DaemonResponse, DaemonResponseKind};

use crate::HubDaemon;
use crate::admission::reservations::{ReserveError, now_seconds};
use crate::admission::unix_hello::{
    UnixTerminalAdmission, WebrtcTerminalAdmission, terminal_compatibility_attach_error,
};
use crate::client_api::HubClientApi;
use crate::client_api_dto::response::{
    daemon_capture_snapshot, daemon_events, daemon_mode_flags, daemon_read_screen,
    daemon_response_base, daemon_session_cleanup, daemon_session_context, daemon_sessions,
    daemon_spawned, daemon_status, daemon_terminal_reservation, daemon_unknown_session_cleanup,
};
use crate::client_api_dto::session::daemon_session_from_client;
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::PendingRuntimeState;
use crate::daemon::shutdown::{
    ShutdownSessionClassification, classify_shutdown_session, recover_after_core_shutdown_error,
};
use crate::subscription::attach_routes::{
    AttachStreamOwner, BoundAdapterHandle, UnixBindRequest, bind_unix_adapter_after_attaching,
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
            if let Some(grant_id) = observability.grant_id
                && let Some(WebrtcTerminalAdmission::Admitted {
                    peer_generation, ..
                }) = pending_runtime.admission.webrtc_admissions.get(grant_id)
                && pending_runtime.admission.reservations.has_live_for_route(
                    &session_id,
                    &subscription_id,
                    *peer_generation,
                    now_seconds(),
                )
            {
                return Ok(super::attach_bind_operator_error(
                    "reservation_label_conflict",
                    "a live reservation already exists for this route",
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
                    peer_generation, ..
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
                let Some(generation) = live_generation_for_route(
                    &runtime.list_terminal_subscriptions(),
                    &client_id,
                    &session_id,
                    &subscription_id,
                ) else {
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
                        "attach failed before adapter bind",
                    ));
                };
                let _ = bootstrap_egress;
                pending_runtime.record_generation(&session_id, &subscription_id, generation);
                return match pending_runtime.admission.reservations.reserve(
                    session_id.clone(),
                    subscription_id.clone(),
                    generation.0,
                    *peer_generation,
                    now_seconds(),
                ) {
                    Ok(reservation) => Ok(daemon_terminal_reservation(reservation)),
                    Err(ReserveError::LabelConflict) => Ok(super::attach_bind_operator_error(
                        "reservation_label_conflict",
                        "a live reservation already exists for this route",
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
            pending_runtime
                .admission
                .reservations
                .forget_route(&tracked_session_id, &tracked_subscription_id);
            pending_runtime.cancel_stream(&tracked_session_id, &tracked_subscription_id);
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
        _ => unreachable!("session runtime family received a non-session request"),
    }
}
