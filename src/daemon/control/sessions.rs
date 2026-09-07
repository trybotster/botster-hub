//! Session request family.
//!
//! Every Core-touching request starts one owner-thread operation and returns
//! [`ControlStep::Pending`]; the continuation finishes the response when the
//! Core ticket or completion arrives. Reads that the owner projection can
//! answer (`Status` session count, `ListSessions`) never touch Core.

use botster_core::{ClientId, SessionId, SubscriptionId};
use botster_core_daemon::{CaptureId, CaptureOwner, CoreCompletion, CoreDaemonError};
use botster_hub_client::{
    DaemonCaptureSnapshot, DaemonDiagnostic, DaemonModeFlags, DaemonOperatorError,
    DaemonReadScreen, DaemonRequest, DaemonResponse, DaemonResponseKind, DaemonRetentionAccounting,
    DaemonSession, DaemonSnapshotPage, DaemonTerminalAttach, HistoryUnavailableReason,
};

use crate::HubDaemon;
use crate::admission::reservations::{ReserveError, now_seconds};
use crate::admission::unix_hello::{
    UnixTerminalAdmission, WebrtcTerminalAdmission, terminal_compatibility_attach_error,
};
use crate::client_api::{client_session_metadata, spawn_request};
use crate::client_api_dto::response::{
    daemon_events, daemon_response_base, daemon_session_cleanup, daemon_session_context,
    daemon_spawned, daemon_status, daemon_terminal_reservation, daemon_unknown_session_cleanup,
};
use crate::client_api_dto::session::lifecycle_label;
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::DaemonTransportError;
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::shutdown::{
    ShutdownSessionClassification, begin_shutdown_classification, shutdown_error_response,
};
use crate::data_plane::driver::CoreTicketPoll;
use crate::runtime::{AttachBindFailure, AttachBindPlan, CoreOperationTracker};
use crate::subscription::attach_routes::{
    AttachStreamOwner, BoundAdapterHandle, overlay_live_attach_occupancy,
};
use crate::subscription::closed_events::{
    suppress_unix_session_close_events, suppress_webrtc_session_close_events,
};
use crate::subscription::entity::entity_subscription_error;

/// Typed operator error for one Core failure on a session request.
/// Operator-facing text for one attach-and-bind failure, with the Core cause.
fn attach_bind_failure_message(failure: &AttachBindFailure) -> String {
    match failure {
        AttachBindFailure::Attach(error) => {
            format!("attach failed before adapter bind: {error}")
        }
        AttachBindFailure::MissingGeneration => {
            "attach failed before adapter bind: no live generation".to_string()
        }
        AttachBindFailure::Bind(error) => {
            format!("Attach failed to bind a Unix adapter: {error}")
        }
    }
}

pub(crate) fn core_operator_error(
    operation: &'static str,
    request_id: &str,
    error: &CoreDaemonError,
) -> DaemonResponse {
    let code = match error {
        CoreDaemonError::UnknownSession(_) => "unknown_session",
        CoreDaemonError::UnknownCapture(_) => "unknown_capture",
        CoreDaemonError::SnapshotPageOutOfRange { .. } => "snapshot_page_out_of_range",
        CoreDaemonError::PendingLimit(_) => "pending_limit",
        CoreDaemonError::Cancelled => "cancelled",
        CoreDaemonError::Shutdown => "daemon_shutdown",
        _ => "core_error",
    };
    let message = error.to_string();
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: request_id.to_string(),
        operation: operation.to_string(),
        message: message.clone(),
        diagnostics: vec![DaemonDiagnostic::action_failure(operation, message)],
    });
    response.diagnostics = response
        .error
        .as_ref()
        .map(|error| error.diagnostics.clone())
        .unwrap_or_default();
    response
}

fn lost_core(operation: &'static str, request_id: &str) -> DaemonResponse {
    core_operator_error(operation, request_id, &CoreDaemonError::Shutdown)
}

fn history_unavailable(
    reason: Option<botster_terminal_protocol::HistoryUnavailableReason>,
) -> Option<HistoryUnavailableReason> {
    reason.map(|reason| match reason {
        botster_terminal_protocol::HistoryUnavailableReason::Evicted => {
            HistoryUnavailableReason::Evicted
        }
        botster_terminal_protocol::HistoryUnavailableReason::Restart => {
            HistoryUnavailableReason::Restart
        }
        botster_terminal_protocol::HistoryUnavailableReason::Oversize => {
            HistoryUnavailableReason::Oversize
        }
        botster_terminal_protocol::HistoryUnavailableReason::CaptureFailed => {
            HistoryUnavailableReason::CaptureFailed
        }
    })
}

/// Sessions the owner projection currently holds, as daemon rows.
pub(crate) fn projected_sessions(state: &DaemonControlState) -> Vec<DaemonSession> {
    state
        .maintenance
        .projection
        .rows
        .values()
        .map(|row| DaemonSession {
            session_id: row.record.session.session_id.0.clone(),
            lifecycle: row
                .record
                .lifecycle
                .as_ref()
                .map(lifecycle_label)
                .unwrap_or(match row.record.session.registry_state {
                    botster_core_daemon::RegistrySessionState::Exited
                    | botster_core_daemon::RegistrySessionState::Stale => "exited",
                    _ => "starting",
                })
                .to_string(),
        })
        .collect()
}

fn retention_accounting_dto(
    policy: botster_core_daemon::RetentionPolicy,
    accounting: botster_core_daemon::RetentionAccounting,
) -> DaemonRetentionAccounting {
    DaemonRetentionAccounting {
        max_object_bytes: policy.max_object_bytes as u64,
        max_total_bytes: policy.max_total_bytes as u64,
        max_sessions: u32::try_from(policy.max_sessions).unwrap_or(u32::MAX),
        total_bytes: accounting.total_bytes as u64,
        sessions: u32::try_from(accounting.sessions).unwrap_or(u32::MAX),
        evictions: accounting.evictions,
    }
}

/// Poll one Core operation tracker; maps loss and begin errors to responses.
fn poll_tracker(
    tracker: &mut CoreOperationTracker,
    daemon: &HubDaemon,
    operation: &'static str,
    request_id: &str,
) -> Result<CoreCompletion, ControlPoll> {
    let Some(runtime) = daemon.runtime() else {
        return Err(ControlPoll::Ready(Err(
            DaemonTransportError::DaemonNotRunning,
        )));
    };
    match tracker.poll(runtime) {
        CoreTicketPoll::Pending => Err(ControlPoll::Pending),
        CoreTicketPoll::Lost => Err(ControlPoll::Ready(Ok(lost_core(operation, request_id)))),
        CoreTicketPoll::Ready(Err(error)) => Err(ControlPoll::Ready(Ok(core_operator_error(
            operation, request_id, &error,
        )))),
        CoreTicketPoll::Ready(Ok(completion)) => Ok(completion),
    }
}

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    request: DaemonRequest,
) -> ControlStep {
    let status = daemon.status();
    if daemon.runtime().is_none() {
        return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
    }
    let client_id = observability
        .client_id
        .clone()
        .unwrap_or_else(|| super::runtime_client_id(&request));

    match request {
        DaemonRequest::RemoveSession { session_id } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let mut tracker = runtime.begin_remove_session(&SessionId(session_id.clone()));
            let id = request_id("daemon-session-remove");
            ControlStep::pending(move |daemon, state| {
                let completion = match poll_tracker(&mut tracker, daemon, "remove_session", &id.0) {
                    Ok(completion) => completion,
                    Err(poll) => return poll,
                };
                let CoreCompletion::RemoveSession { result, .. } = completion else {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                };
                match result {
                    Ok(true) => {
                        suppress_unix_session_close_events(&mut state.pending_runtime, &session_id);
                        suppress_webrtc_session_close_events(
                            &mut state.pending_runtime,
                            &session_id,
                        );
                        ControlPoll::Ready(Ok(daemon_response_base(
                            DaemonResponseKind::SessionRemoved,
                        )))
                    }
                    Ok(false) => ControlPoll::Ready(Ok(entity_subscription_error(
                        "session_not_terminal",
                        "daemon-session-remove",
                        "session must be terminal before it can be removed",
                    ))),
                    Err(error) => {
                        ControlPoll::Ready(Ok(core_operator_error("remove_session", &id.0, &error)))
                    }
                }
            })
        }
        DaemonRequest::Status => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let policy = runtime.retention_policy();
            let mut ticket = runtime.submit_core(|daemon| {
                (
                    daemon.retention_accounting(),
                    daemon.list_terminal_subscriptions(),
                )
            });
            let egress = observability.egress.clone();
            let lifecycle = observability.lifecycle.clone();
            ControlStep::pending(move |daemon, state| {
                let (accounting, inventory) = match ticket.poll() {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Lost => {
                        return ControlPoll::Ready(Ok(lost_core("status", "daemon-status")));
                    }
                    CoreTicketPoll::Ready(value) => value,
                };
                let Some(runtime) = daemon.runtime() else {
                    return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
                };
                let mut response = daemon_status(
                    status.clone(),
                    state.maintenance.projection.rows.len(),
                    egress.clone(),
                    lifecycle.clone(),
                    runtime.event_plane_counters_snapshot(),
                    Some(retention_accounting_dto(policy, accounting)),
                );
                if let Some(status) = response.status.as_mut() {
                    overlay_live_attach_occupancy(
                        status,
                        &inventory,
                        &state.pending_runtime.live_attach_routes,
                        &state.pending_runtime,
                    );
                }
                ControlPoll::Ready(Ok(response))
            })
        }
        DaemonRequest::ListSessions => {
            let mut response = daemon_response_base(DaemonResponseKind::Sessions);
            response.sessions = projected_sessions(state);
            ControlStep::ready(response)
        }
        DaemonRequest::Spawn {
            session_id,
            command,
        } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let id = request_id("daemon-sessions-spawn");
            let spawn = spawn_request(runtime, id.clone(), SessionId(session_id), command);
            let mut tracker = runtime.begin_spawn(spawn, client_session_metadata());
            ControlStep::pending(move |daemon, state| {
                let completion = match poll_tracker(&mut tracker, daemon, "spawn", &id.0) {
                    Ok(completion) => completion,
                    Err(poll) => return poll,
                };
                let CoreCompletion::Spawn { result, .. } = completion else {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                };
                match result {
                    Ok(session) => {
                        let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
                        state
                            .drain_cursors
                            .insert(session.session_id.0.clone(), now);
                        ControlPoll::Ready(Ok(daemon_spawned(
                            DaemonSession {
                                session_id: session.session_id.0,
                                lifecycle: lifecycle_label(&session.lifecycle).to_string(),
                            },
                            Vec::new(),
                        )))
                    }
                    Err(error) => {
                        ControlPoll::Ready(Ok(core_operator_error("spawn", &id.0, &error)))
                    }
                }
            })
        }
        DaemonRequest::Attach {
            session_id,
            subscription_id,
        } => handle_attach(
            daemon,
            state,
            &observability,
            client_id,
            session_id,
            subscription_id,
        ),
        DaemonRequest::Detach {
            session_id,
            subscription_id,
        } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
            let generation = state
                .pending_runtime
                .recorded_generation(&session_id, &subscription_id);
            let unix_mux = observability.client_id.as_deref().and_then(|client_id| {
                match state
                    .pending_runtime
                    .admission
                    .unix_admissions
                    .get(client_id)
                {
                    Some(UnixTerminalAdmission::Admitted { mux, .. }) => Some(mux.clone()),
                    _ => None,
                }
            });
            let webrtc_mux = observability.grant_id.as_deref().and_then(|grant_id| {
                match state
                    .pending_runtime
                    .admission
                    .webrtc_admissions
                    .get(grant_id)
                {
                    Some(WebrtcTerminalAdmission::Admitted { mux, .. }) => Some(mux.clone()),
                    _ => None,
                }
            });
            if let Some(generation) = generation {
                if let Some(mux) = unix_mux.as_ref() {
                    mux.suppress_generation(
                        session_id.clone(),
                        subscription_id.clone(),
                        generation.0,
                    );
                }
                if let Some(mux) = webrtc_mux.as_ref() {
                    mux.suppress_generation(
                        session_id.clone(),
                        subscription_id.clone(),
                        generation.0,
                    );
                }
            }
            let mut ticket = runtime.detach_client(
                ClientId(client_id),
                SessionId(session_id.clone()),
                SubscriptionId(subscription_id.clone()),
                now,
            );
            let id = request_id("daemon-sessions-detach");
            ControlStep::pending(move |_, state| {
                let result = match ticket.poll() {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Lost => Err(CoreDaemonError::Shutdown),
                    CoreTicketPoll::Ready(result) => result,
                };
                match result {
                    Ok(()) => {
                        if let Some(generation) = generation {
                            if let Some(mux) = unix_mux.as_ref() {
                                mux.commit_generation_suppression(
                                    &session_id,
                                    &subscription_id,
                                    generation.0,
                                );
                            }
                            if let Some(mux) = webrtc_mux.as_ref() {
                                mux.commit_generation_suppression(
                                    &session_id,
                                    &subscription_id,
                                    generation.0,
                                );
                            }
                        }
                        state
                            .pending_runtime
                            .close_adapter(&session_id, &subscription_id);
                        state
                            .pending_runtime
                            .admission
                            .reservations
                            .forget_route(&session_id, &subscription_id);
                        state
                            .pending_runtime
                            .cancel_stream(&session_id, &subscription_id);
                        ControlPoll::Ready(Ok(daemon_events(Vec::new())))
                    }
                    Err(error) => {
                        if let Some(generation) = generation {
                            if let Some(mux) = unix_mux.as_ref() {
                                mux.unsuppress_generation(
                                    &session_id,
                                    &subscription_id,
                                    generation.0,
                                );
                            }
                            if let Some(mux) = webrtc_mux.as_ref() {
                                mux.unsuppress_generation(
                                    &session_id,
                                    &subscription_id,
                                    generation.0,
                                );
                            }
                        }
                        ControlPoll::Ready(Ok(core_operator_error("detach", &id.0, &error)))
                    }
                }
            })
        }
        DaemonRequest::ShutdownSession { session_id } => {
            handle_shutdown_session(daemon, state, session_id)
        }
        DaemonRequest::ReadScreen { session_id } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
            let id = request_id("daemon-sessions-read-screen");
            let mut tracker =
                runtime.begin_read_screen(id.clone(), SessionId(session_id.clone()), now);
            ControlStep::pending(move |daemon, _| {
                let completion = match poll_tracker(&mut tracker, daemon, "read_screen", &id.0) {
                    Ok(completion) => completion,
                    Err(poll) => return poll,
                };
                let CoreCompletion::ReadScreen { result, .. } = completion else {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                };
                ControlPoll::Ready(Ok(match result {
                    Ok(screen) => {
                        let mut response = daemon_response_base(DaemonResponseKind::ReadScreen);
                        response.read_screen = Some(DaemonReadScreen {
                            session_id: session_id.clone(),
                            text: screen.text.to_string(),
                            unavailable: history_unavailable(screen.unavailable),
                        });
                        response
                    }
                    Err(error) => core_operator_error("read_screen", &id.0, &error),
                }))
            })
        }
        DaemonRequest::ReadModeFlags { session_id } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
            let id = request_id("daemon-sessions-read-mode-flags");
            let mut tracker =
                runtime.begin_read_mode_flags(id.clone(), SessionId(session_id.clone()), now);
            ControlStep::pending(move |daemon, _| {
                let completion = match poll_tracker(&mut tracker, daemon, "read_mode_flags", &id.0)
                {
                    Ok(completion) => completion,
                    Err(poll) => return poll,
                };
                let CoreCompletion::ReadModeFlags { result, .. } = completion else {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                };
                ControlPoll::Ready(Ok(match result {
                    Ok(readback) => {
                        let mode = readback.mode_flags;
                        let mut response = daemon_response_base(DaemonResponseKind::ReadModeFlags);
                        response.mode_flags = Some(DaemonModeFlags::new(
                            session_id.clone(),
                            mode.kitty_enabled,
                            mode.cursor_visible,
                            mode.bracketed_paste,
                            mode.mouse_mode,
                            mode.alt_screen,
                            mode.focus_reporting,
                            mode.application_cursor,
                            readback.rows,
                            readback.cols,
                            history_unavailable(readback.unavailable),
                        ));
                        response
                    }
                    Err(error) => core_operator_error("read_mode_flags", &id.0, &error),
                }))
            })
        }
        DaemonRequest::CaptureSnapshot { session_id } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
            let id = request_id("daemon-sessions-capture-snapshot");
            let owner = CaptureOwner(capture_owner_id(&observability, &client_id));
            let mut tracker = runtime.begin_capture_snapshot(
                id.clone(),
                SessionId(session_id.clone()),
                now,
                owner,
            );
            ControlStep::pending(move |daemon, _| {
                let completion = match poll_tracker(&mut tracker, daemon, "capture_snapshot", &id.0)
                {
                    Ok(completion) => completion,
                    Err(poll) => return poll,
                };
                let CoreCompletion::CaptureSnapshot { result, .. } = completion else {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                };
                ControlPoll::Ready(Ok(match result {
                    Ok(capture) => {
                        let mut response =
                            daemon_response_base(DaemonResponseKind::CaptureSnapshot);
                        response.capture_snapshot = Some(DaemonCaptureSnapshot {
                            session_id: session_id.clone(),
                            capture_id: capture.capture_id.0,
                            total_bytes: capture.total_bytes,
                            page_bytes: capture.page_bytes,
                            pages: capture.pages,
                            rows: capture.rows,
                            cols: capture.cols,
                            unavailable: history_unavailable(capture.unavailable),
                        });
                        response
                    }
                    Err(error) => core_operator_error("capture_snapshot", &id.0, &error),
                }))
            })
        }
        DaemonRequest::ReadSnapshotPage {
            session_id,
            capture_id,
            page,
        } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let id = request_id("daemon-sessions-read-snapshot-page");
            let mut ticket = runtime.read_snapshot_page(CaptureId(capture_id.clone()), page);
            ControlStep::pending(move |_, _| {
                let result = match ticket.poll() {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Lost => Err(CoreDaemonError::Shutdown),
                    CoreTicketPoll::Ready(result) => result,
                };
                ControlPoll::Ready(Ok(match result {
                    Ok(snapshot_page) => {
                        let mut response = daemon_response_base(DaemonResponseKind::SnapshotPage);
                        response.snapshot_page = Some(DaemonSnapshotPage {
                            session_id: session_id.clone(),
                            capture_id: capture_id.clone(),
                            page,
                            payload: botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(
                                snapshot_page.as_slice(),
                            ),
                        });
                        response
                    }
                    Err(error) => core_operator_error("read_snapshot_page", &id.0, &error),
                }))
            })
        }
        DaemonRequest::ReadSessionContext {
            session_id,
            context_id,
            key,
        } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let lookup = context_id.as_deref().unwrap_or(session_id.as_str());
            let Some(context) = runtime.session_context(lookup) else {
                return ControlStep::ready(entity_subscription_error(
                    "unknown_context",
                    "daemon-session-context-read",
                    "session context was not found",
                ));
            };
            if context.session_id.0 != session_id {
                return ControlStep::ready(entity_subscription_error(
                    "context_session_mismatch",
                    "daemon-session-context-read",
                    "session context does not belong to the requested session",
                ));
            }
            let context = if let Some(key) = key {
                crate::HubSessionContext {
                    values: context
                        .values
                        .get(&key)
                        .map(|value| std::collections::BTreeMap::from([(key, value.clone())]))
                        .unwrap_or_default(),
                    ..context
                }
            } else {
                context
            };
            ControlStep::ready(daemon_session_context(context))
        }
        _ => unreachable!("session runtime family received a non-session request"),
    }
}

fn capture_owner_id(observability: &DaemonObservability, client_id: &str) -> String {
    observability
        .grant_id
        .as_deref()
        .map(|grant_id| format!("grant:{grant_id}"))
        .unwrap_or_else(|| format!("client:{client_id}"))
}

fn handle_attach(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: &DaemonObservability,
    client_id: String,
    session_id: String,
    subscription_id: String,
) -> ControlStep {
    let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
    let pending_runtime = &mut state.pending_runtime;
    if let Some(UnixTerminalAdmission::Rejected { code, diagnostic }) =
        pending_runtime.admission.unix_admissions.get(&client_id)
    {
        return ControlStep::ready(terminal_compatibility_attach_error(
            code,
            diagnostic.clone(),
        ));
    }
    if let Some(grant_id) = observability.grant_id.as_deref()
        && let Some(WebrtcTerminalAdmission::Rejected {
            code, diagnostic, ..
        }) = pending_runtime.admission.webrtc_admissions.get(grant_id)
    {
        return ControlStep::ready(terminal_compatibility_attach_error(
            code,
            diagnostic.clone(),
        ));
    }
    let webrtc_admission = observability.grant_id.as_deref().and_then(|grant_id| {
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
            return ControlStep::ready(super::attach_bind_operator_error(
                "invalid_request",
                "Attach requires an admitted WebRTC adapter",
            ));
        };
        let peer_generation = *peer_generation;
        if pending_runtime.admission.reservations.has_live_for_route(
            &session_id,
            &subscription_id,
            peer_generation,
            now_seconds(),
        ) {
            return ControlStep::ready(super::attach_bind_operator_error(
                "reservation_label_conflict",
                "a live reservation already exists for this route",
            ));
        }
        let owner = AttachStreamOwner {
            client_id: client_id.clone(),
            grant_id: observability.grant_id.clone(),
        };
        pending_runtime.start_attach(owner, session_id.clone(), subscription_id.clone());
        let runtime = daemon.runtime().expect("runtime checked by caller");
        let mut ticket = runtime.attach_route(
            ClientId(client_id.clone()),
            SessionId(session_id.clone()),
            SubscriptionId(subscription_id.clone()),
            now,
        );
        return ControlStep::pending(move |daemon, state| {
            let result = match ticket.poll() {
                CoreTicketPoll::Pending => return ControlPoll::Pending,
                CoreTicketPoll::Lost => Err(AttachBindFailure::Attach(CoreDaemonError::Shutdown)),
                CoreTicketPoll::Ready(result) => result,
            };
            let pending_runtime = &mut state.pending_runtime;
            let generation = match result {
                Ok(generation) => generation,
                Err(failure) => {
                    pending_runtime.cancel_stream(&session_id, &subscription_id);
                    return ControlPoll::Ready(Ok(super::attach_bind_operator_error(
                        "invalid_request",
                        &attach_bind_failure_message(&failure),
                    )));
                }
            };
            pending_runtime.record_generation(&session_id, &subscription_id, generation);
            let reserved = pending_runtime.admission.reservations.reserve(
                session_id.clone(),
                subscription_id.clone(),
                generation.0,
                peer_generation,
                now_seconds(),
            );
            let response = match reserved {
                Ok(reservation) => {
                    let budget_result = pending_runtime
                        .admission
                        .connection_budgets
                        .get_mut(&peer_generation)
                        .ok_or(
                            crate::admission::connection_budget::ChannelBudgetError::ChannelLimit,
                        )
                        .and_then(|budget| {
                            budget
                                .reserve(
                                    reservation.label.clone(),
                                    crate::admission::connection_budget::ChannelClass::Terminal,
                                )
                                .map(|_| ())
                        });
                    if budget_result.is_err() {
                        let _ = pending_runtime
                            .admission
                            .reservations
                            .forget_label(&reservation.label, peer_generation);
                        Err(super::attach_bind_operator_error(
                            "connection_channel_limit",
                            "the WebRTC connection channel budget rejected the reservation",
                        ))
                    } else {
                        Ok(daemon_terminal_reservation(reservation))
                    }
                }
                Err(ReserveError::LabelConflict) => Err(super::attach_bind_operator_error(
                    "reservation_label_conflict",
                    "a live reservation already exists for this route",
                )),
            };
            match response {
                Ok(response) => ControlPoll::Ready(Ok(response)),
                Err(error) => {
                    // The route exists in Core without an adapter; release it.
                    if let Some(runtime) = daemon.runtime() {
                        let _ = runtime.detach_owned_generation(
                            ClientId(client_id.clone()),
                            SessionId(session_id.clone()),
                            SubscriptionId(subscription_id.clone()),
                            now,
                        );
                    }
                    pending_runtime.cancel_stream(&session_id, &subscription_id);
                    ControlPoll::Ready(Ok(error))
                }
            }
        });
    }
    let Some(UnixTerminalAdmission::Admitted {
        capabilities, mux, ..
    }) = pending_runtime
        .admission
        .unix_admissions
        .get(&client_id)
        .cloned()
    else {
        return ControlStep::ready(super::attach_bind_operator_error(
            "invalid_request",
            "Attach requires an admitted Unix adapter",
        ));
    };
    let owner = AttachStreamOwner {
        client_id: client_id.clone(),
        grant_id: None,
    };
    pending_runtime.start_attach(owner, session_id.clone(), subscription_id.clone());
    let (adapter, handle) = mux.create_adapter();
    let runtime = daemon.runtime().expect("runtime checked by caller");
    let mut ticket = runtime.attach_and_bind_terminal(AttachBindPlan {
        client_id: ClientId(client_id),
        session_id: SessionId(session_id.clone()),
        subscription_id: SubscriptionId(subscription_id.clone()),
        capabilities,
        now_seconds: now,
        adapter: Box::new(adapter),
    });
    ControlStep::pending(move |_, state| {
        let result = match ticket.poll() {
            CoreTicketPoll::Pending => return ControlPoll::Pending,
            CoreTicketPoll::Lost => Err(AttachBindFailure::Attach(CoreDaemonError::Shutdown)),
            CoreTicketPoll::Ready(result) => result,
        };
        let pending_runtime = &mut state.pending_runtime;
        match result {
            Ok(generation) => {
                pending_runtime.mark_adapter_bound(
                    &session_id,
                    &subscription_id,
                    generation,
                    BoundAdapterHandle::Unix(handle.clone()),
                );
                mux.register(
                    session_id.clone(),
                    subscription_id.clone(),
                    generation.0,
                    handle.clone(),
                );
                let mut response = daemon_response_base(DaemonResponseKind::TerminalAttached);
                response.terminal_attach = Some(DaemonTerminalAttach::new(
                    session_id.clone(),
                    subscription_id.clone(),
                    generation.0,
                ));
                ControlPoll::Ready(Ok(response))
            }
            Err(failure) => {
                handle.close();
                pending_runtime.cancel_stream(&session_id, &subscription_id);
                ControlPoll::Ready(Ok(super::attach_bind_operator_error(
                    "invalid_request",
                    &attach_bind_failure_message(&failure),
                )))
            }
        }
    })
}

enum ShutdownStage {
    Classify(
        crate::data_plane::driver::CoreTicket<
            Result<ShutdownSessionClassification, CoreDaemonError>,
        >,
    ),
    Shutdown(CoreOperationTracker),
    Recover {
        ticket: crate::data_plane::driver::CoreTicket<
            Result<ShutdownSessionClassification, CoreDaemonError>,
        >,
        error: CoreDaemonError,
    },
}

fn handle_shutdown_session(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    session_id: String,
) -> ControlStep {
    let runtime = daemon.runtime().expect("runtime checked by caller");
    let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
    let mut stage =
        ShutdownStage::Classify(begin_shutdown_classification(runtime, &session_id, now));
    let id = request_id("daemon-sessions-shutdown");
    ControlStep::pending(move |daemon, state| {
        loop {
            match &mut stage {
                ShutdownStage::Classify(ticket) => {
                    let classification = match ticket.poll() {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Lost => Err(CoreDaemonError::Shutdown),
                        CoreTicketPoll::Ready(result) => result,
                    };
                    match classification {
                        Ok(ShutdownSessionClassification::Cleanup(cleanup)) => {
                            // Keep adapters open. Classify already asked Core to
                            // write ProcessExited. Host close abandons that frame.
                            return ControlPoll::Ready(Ok(daemon_session_cleanup(cleanup)));
                        }
                        Ok(ShutdownSessionClassification::Missing) => {
                            state
                                .pending_runtime
                                .close_adapters_for_session(&session_id);
                            return ControlPoll::Ready(Ok(daemon_unknown_session_cleanup(
                                &session_id,
                            )));
                        }
                        Ok(ShutdownSessionClassification::Active)
                        | Ok(ShutdownSessionClassification::Stopping)
                        | Err(_) => {}
                    }
                    suppress_unix_session_close_events(&mut state.pending_runtime, &session_id);
                    suppress_webrtc_session_close_events(&mut state.pending_runtime, &session_id);
                    let Some(runtime) = daemon.runtime() else {
                        return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
                    };
                    stage = ShutdownStage::Shutdown(
                        runtime.begin_shutdown_session(SessionId(session_id.clone())),
                    );
                }
                ShutdownStage::Shutdown(tracker) => {
                    let completion = match poll_tracker(tracker, daemon, "shutdown_session", &id.0)
                    {
                        Ok(completion) => completion,
                        Err(poll) => return poll,
                    };
                    let CoreCompletion::ShutdownSession { result, .. } = completion else {
                        return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                    };
                    match result {
                        Ok(()) => return ControlPoll::Ready(Ok(daemon_events(Vec::new()))),
                        Err(error) => {
                            state
                                .pending_runtime
                                .close_adapters_for_session(&session_id);
                            let Some(runtime) = daemon.runtime() else {
                                return ControlPoll::Ready(Err(
                                    DaemonTransportError::DaemonNotRunning,
                                ));
                            };
                            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
                            stage = ShutdownStage::Recover {
                                ticket: begin_shutdown_classification(runtime, &session_id, now),
                                error,
                            };
                        }
                    }
                }
                ShutdownStage::Recover { ticket, error } => {
                    let classification = match ticket.poll() {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Lost => Err(CoreDaemonError::Shutdown),
                        CoreTicketPoll::Ready(result) => result,
                    };
                    return ControlPoll::Ready(Ok(match classification {
                        Ok(classification) => {
                            shutdown_error_response(classification, error, &session_id, &id.0)
                        }
                        Err(_) => core_operator_error("shutdown_session", &id.0, error),
                    }));
                }
            }
        }
    })
}
