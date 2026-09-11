//! Session request family.
//!
//! Every Core-touching request starts one owner-thread operation and returns
//! [`ControlStep::Pending`]; the continuation finishes the response when the
//! Core ticket or completion arrives. Reads that the owner projection can
//! answer (`Status` session count, `ListSessions`) never touch Core.

use botster_core::{
    ClientId, SessionId, SessionReservation, SessionReservationRelease, SubscriptionId,
    TerminalSubscriptionGeneration,
};
use botster_core_daemon::{
    CaptureId, CaptureOwner, CoreCompletion, CoreDaemonError, DetachTerminalSubscriptionResult,
    PendingOperationId, SpawnSessionRequest,
};
use botster_core_daemon::operation::ReservedSpawnResult;
use botster_hub_client::{
    DaemonCaptureSnapshot, DaemonDiagnostic, DaemonModeFlags, DaemonOperatorError,
    DaemonReadScreen, DaemonRequest, DaemonResponse, DaemonResponseKind, DaemonSession,
    DaemonSnapshotPage, DaemonTerminalAttach, HistoryUnavailableReason,
};

use crate::HubDaemon;
use crate::admission::reservations::{ReserveError, now_seconds};
use crate::admission::unix_hello::{
    UnixTerminalAdmission, WebrtcTerminalAdmission, terminal_compatibility_attach_error,
};
use crate::client_api::{client_session_metadata, spawn_request};
use crate::client_api_dto::response::{
    daemon_events, daemon_response_base, daemon_session_cleanup, daemon_session_context,
    daemon_spawned, daemon_terminal_reservation, daemon_unknown_session_cleanup,
};
use crate::client_api_dto::session::lifecycle_label;
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::DaemonTransportError;
use crate::daemon::owner_budget::{
    CoreWorkPoll, OWNER_BUDGET_EXHAUSTED, ObligationPoll, OwnerPermit, drive_core_slot,
};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::shutdown::{
    ShutdownSessionClassification, begin_shutdown_classification, shutdown_error_response,
};
use crate::data_plane::driver::{CoreTicket, CoreTicketError, CoreTicketPoll};
use crate::runtime::core_bridge_error;
use crate::runtime::{AttachBindFailure, AttachBindPlan, CoreOperationTracker};
use crate::subscription::attach_routes::RouteReservation;
use crate::subscription::attach_routes::{AttachStreamOwner, BoundAdapterHandle};
use crate::subscription::closed_events::{
    suppress_unix_session_close_events, suppress_webrtc_session_close_events,
};
use crate::subscription::entity::entity_subscription_error;
use crate::subscription::route_cleanup::{
    ATTACH_ROUTE_LIMIT, release_failed_attach_route, reserve_attach_route,
};

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

pub(super) fn lost_core(operation: &'static str, request_id: &str) -> DaemonResponse {
    core_operator_error(operation, request_id, &CoreDaemonError::Shutdown)
}

/// Typed refusal: the bounded Core request queue was full, nothing ran.
pub(super) fn overloaded_core(operation: &'static str, request_id: &str) -> DaemonResponse {
    core_operator_error(
        operation,
        request_id,
        &core_bridge_error(CoreTicketError::Overloaded),
    )
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

fn retained_release_code(release: SessionReservationRelease) -> &'static str {
    match release {
        SessionReservationRelease::Released => "released",
        SessionReservationRelease::RetainedPending => "retained_pending",
        SessionReservationRelease::RetainedUnconfirmed => "cleanup_unconfirmed",
        SessionReservationRelease::RetainedSession => "retained_session",
    }
}

fn retain_explicit_reservation(
    state: &mut DaemonControlState,
    reservation: SessionReservation,
) {
    if state
        .retained_explicit_reservations
        .iter()
        .any(|held| held == &reservation)
    {
        return;
    }
    state.retained_explicit_reservations.push(reservation);
}

fn poll_spawn_ticket(
    tracker: &mut CoreOperationTracker,
    daemon: &HubDaemon,
) -> CoreTicketPoll<Result<CoreCompletion, CoreDaemonError>> {
    let Some(runtime) = daemon.runtime() else {
        return CoreTicketPoll::Ready(Err(CoreDaemonError::Shutdown));
    };
    tracker.poll(runtime)
}

fn submit_release(
    daemon: &HubDaemon,
    waiter_id: crate::owner_identity::WaiterId,
    reservation: SessionReservation,
) -> Option<CoreOperationTracker> {
    let runtime = daemon.runtime()?;
    Some(runtime.begin_release_session_reservation_for_owner(waiter_id, reservation))
}

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
        CoreTicketPoll::Refused => Err(ControlPoll::Ready(Ok(overloaded_core(
            operation, request_id,
        )))),
        CoreTicketPoll::Ready(Err(error)) => Err(ControlPoll::Ready(Ok(core_operator_error(
            operation, request_id, &error,
        )))),
        CoreTicketPoll::Ready(Ok(completion)) => Ok(completion),
    }
}

fn finish_held_reservation(
    state: &mut DaemonControlState,
    reservation: SessionReservation,
    request_id: &str,
    operation: &'static str,
    error: CoreDaemonError,
) -> ControlPoll {
    retain_explicit_reservation(state, reservation);
    ControlPoll::Ready(Ok(core_operator_error(operation, request_id, &error)))
}

fn handle_daemon_spawn(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    session_id: String,
    command: String,
) -> ControlStep {
    let runtime = daemon.runtime().expect("runtime checked above");
    let waiter_id = state.current_waiter_id.expect("owner waiter is assigned");
    let id = request_id("daemon-sessions-spawn");
    let spawn = SpawnSessionRequest {
        request: spawn_request(runtime, id.clone(), SessionId(session_id.clone()), command),
        metadata: client_session_metadata(),
    };
    enum Stage {
        RetryRetained,
        Reserve,
        Lookup,
        SpawnReserved,
        Release,
    }
    let mut retry_tokens = std::mem::take(&mut state.retained_explicit_reservations);
    let mut retry_keep = Vec::new();
    let mut stage = if retry_tokens.is_empty() {
        Stage::Reserve
    } else {
        Stage::RetryRetained
    };
    let mut tracker = if matches!(stage, Stage::Reserve) {
        runtime.begin_reserve_session_for_owner(waiter_id, SessionId(session_id.clone()))
    } else {
        runtime.begin_release_session_reservation_for_owner(waiter_id, retry_tokens[0].clone())
    };
    let mut reservation: Option<SessionReservation> = None;
    let mut spawn_error: Option<CoreDaemonError> = None;
    let mut reserve_operation_id: Option<PendingOperationId> = None;
    ControlStep::pending(move |daemon, state| loop {
        if let Some(pending_id) = tracker.pending_id() {
            if matches!(stage, Stage::Reserve) {
                reserve_operation_id = Some(pending_id);
            }
        }
        match stage {
            Stage::RetryRetained => {
                if retry_tokens.is_empty() {
                    state.retained_explicit_reservations.append(&mut retry_keep);
                    let Some(runtime) = daemon.runtime() else {
                        return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
                    };
                    tracker = runtime.begin_reserve_session_for_owner(
                        waiter_id,
                        SessionId(session_id.clone()),
                    );
                    stage = Stage::Reserve;
                    continue;
                }
                match poll_spawn_ticket(&mut tracker, daemon) {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Refused
                    | CoreTicketPoll::Lost
                    | CoreTicketPoll::Ready(Err(_)) => {
                        retry_keep.push(retry_tokens.remove(0));
                        if retry_tokens.is_empty() {
                            continue;
                        }
                        match submit_release(daemon, waiter_id, retry_tokens[0].clone()) {
                            Some(next) => {
                                tracker = next;
                                continue;
                            }
                            None => {
                                retry_keep.append(&mut retry_tokens);
                                state.retained_explicit_reservations.append(&mut retry_keep);
                                return ControlPoll::Ready(Err(
                                    DaemonTransportError::DaemonNotRunning,
                                ));
                            }
                        }
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                        result: Ok(SessionReservationRelease::Released),
                        ..
                    })) => {
                        retry_tokens.remove(0);
                        if retry_tokens.is_empty() {
                            continue;
                        }
                        match submit_release(daemon, waiter_id, retry_tokens[0].clone()) {
                            Some(next) => tracker = next,
                            None => {
                                retry_keep.append(&mut retry_tokens);
                                state.retained_explicit_reservations.append(&mut retry_keep);
                                return ControlPoll::Ready(Err(
                                    DaemonTransportError::DaemonNotRunning,
                                ));
                            }
                        }
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation { .. })) => {
                        retry_keep.push(retry_tokens.remove(0));
                        if retry_tokens.is_empty() {
                            continue;
                        }
                        match submit_release(daemon, waiter_id, retry_tokens[0].clone()) {
                            Some(next) => tracker = next,
                            None => {
                                retry_keep.append(&mut retry_tokens);
                                state.retained_explicit_reservations.append(&mut retry_keep);
                                return ControlPoll::Ready(Err(
                                    DaemonTransportError::DaemonNotRunning,
                                ));
                            }
                        }
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                    }
                }
            }
            Stage::Reserve => match poll_spawn_ticket(&mut tracker, daemon) {
                CoreTicketPoll::Pending => return ControlPoll::Pending,
                CoreTicketPoll::Refused => {
                    return ControlPoll::Ready(Ok(overloaded_core("reserve_session", &id.0)));
                }
                CoreTicketPoll::Lost => {
                    let Some(reserve_id) = reserve_operation_id else {
                        return ControlPoll::Ready(Ok(lost_core("reserve_session", &id.0)));
                    };
                    let Some(runtime) = daemon.runtime() else {
                        return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
                    };
                    tracker = runtime.begin_lookup_session_reservation_for_owner(
                        waiter_id,
                        SessionId(session_id.clone()),
                        reserve_id,
                    );
                    stage = Stage::Lookup;
                }
                CoreTicketPoll::Ready(Err(error)) => {
                    return ControlPoll::Ready(Ok(core_operator_error(
                        "reserve_session",
                        &id.0,
                        &error,
                    )));
                }
                CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession { result, .. })) => {
                    match result {
                        Ok(reserved) => {
                            reservation = Some(reserved.clone());
                            let Some(runtime) = daemon.runtime() else {
                                return ControlPoll::Ready(Err(
                                    DaemonTransportError::DaemonNotRunning,
                                ));
                            };
                            tracker = runtime.begin_spawn_reserved_for_owner(
                                waiter_id,
                                reserved,
                                spawn.clone(),
                            );
                            stage = Stage::SpawnReserved;
                        }
                        Err(error) => {
                            return ControlPoll::Ready(Ok(core_operator_error(
                                "reserve_session",
                                &id.0,
                                &error,
                            )));
                        }
                    }
                }
                CoreTicketPoll::Ready(Ok(_)) => {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                }
            },
            Stage::Lookup => match poll_spawn_ticket(&mut tracker, daemon) {
                CoreTicketPoll::Pending => return ControlPoll::Pending,
                CoreTicketPoll::Refused => {
                    return ControlPoll::Ready(Ok(overloaded_core(
                        "lookup_session_reservation",
                        &id.0,
                    )));
                }
                CoreTicketPoll::Lost => {
                    return ControlPoll::Ready(Ok(lost_core("lookup_session_reservation", &id.0)));
                }
                CoreTicketPoll::Ready(Err(error)) => {
                    return ControlPoll::Ready(Ok(core_operator_error(
                        "lookup_session_reservation",
                        &id.0,
                        &error,
                    )));
                }
                CoreTicketPoll::Ready(Ok(CoreCompletion::LookupSessionReservation {
                    result,
                    ..
                })) => match result {
                    Ok(Some(reserved)) => {
                        reservation = Some(reserved.clone());
                        spawn_error = Some(CoreDaemonError::Shutdown);
                        match submit_release(daemon, waiter_id, reserved) {
                            Some(next) => {
                                tracker = next;
                                stage = Stage::Release;
                            }
                            None => {
                                return finish_held_reservation(
                                    state,
                                    reservation.take().expect("looked-up reservation"),
                                    &id.0,
                                    "lookup_session_reservation",
                                    CoreDaemonError::Shutdown,
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        return ControlPoll::Ready(Ok(lost_core("reserve_session", &id.0)));
                    }
                    Err(error) => {
                        return ControlPoll::Ready(Ok(core_operator_error(
                            "lookup_session_reservation",
                            &id.0,
                            &error,
                        )));
                    }
                },
                CoreTicketPoll::Ready(Ok(_)) => {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                }
            },
            Stage::SpawnReserved => match poll_spawn_ticket(&mut tracker, daemon) {
                CoreTicketPoll::Pending => return ControlPoll::Pending,
                CoreTicketPoll::Refused => {
                    return finish_held_reservation(
                        state,
                        reservation.take().expect("reserved identity"),
                        &id.0,
                        "spawn_reserved",
                        core_bridge_error(CoreTicketError::Overloaded),
                    );
                }
                CoreTicketPoll::Lost => {
                    return finish_held_reservation(
                        state,
                        reservation.take().expect("reserved identity"),
                        &id.0,
                        "spawn_reserved",
                        CoreDaemonError::Shutdown,
                    );
                }
                CoreTicketPoll::Ready(Err(error)) => {
                    return finish_held_reservation(
                        state,
                        reservation.take().expect("reserved identity"),
                        &id.0,
                        "spawn_reserved",
                        error,
                    );
                }
                CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                    result: ReservedSpawnResult::Installed { session },
                    ..
                })) => {
                    let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
                    state
                        .drain_cursors
                        .insert(session.session_id.0.clone(), now);
                    return ControlPoll::Ready(Ok(daemon_spawned(
                        DaemonSession {
                            session_id: session.session_id.0,
                            lifecycle: lifecycle_label(&session.lifecycle).to_string(),
                        },
                        Vec::new(),
                    )));
                }
                CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                    result: ReservedSpawnResult::Refused { error },
                    ..
                }))
                | CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                    result: ReservedSpawnResult::AdmittedFailure { error, .. },
                    ..
                })) => {
                    spawn_error = Some(error);
                    let held = reservation.clone().expect("reserved identity");
                    match submit_release(daemon, waiter_id, held) {
                        Some(next) => {
                            tracker = next;
                            stage = Stage::Release;
                        }
                        None => {
                            return finish_held_reservation(
                                state,
                                reservation.take().expect("reserved identity"),
                                &id.0,
                                "spawn_reserved",
                                spawn_error.take().unwrap(),
                            );
                        }
                    }
                }
                CoreTicketPoll::Ready(Ok(_)) => {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                }
            },
            Stage::Release => match poll_spawn_ticket(&mut tracker, daemon) {
                CoreTicketPoll::Pending => return ControlPoll::Pending,
                CoreTicketPoll::Refused | CoreTicketPoll::Lost | CoreTicketPoll::Ready(Err(_)) => {
                    let error = spawn_error.take().unwrap_or(CoreDaemonError::Shutdown);
                    return finish_held_reservation(
                        state,
                        reservation.take().expect("reserved identity"),
                        &id.0,
                        "release_session_reservation",
                        error,
                    );
                }
                CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                    result,
                    ..
                })) => {
                    let error = spawn_error.take().unwrap_or(CoreDaemonError::Shutdown);
                    return ControlPoll::Ready(Ok(match result {
                        Ok(SessionReservationRelease::Released) => {
                            core_operator_error("spawn_reserved", &id.0, &error)
                        }
                        Ok(release) => {
                            if let Some(held) = reservation.take() {
                                retain_explicit_reservation(state, held);
                            }
                            let mut response = core_operator_error(
                                "release_session_reservation",
                                &id.0,
                                &error,
                            );
                            if let Some(operator) = response.error.as_mut() {
                                operator.code = retained_release_code(release).to_string();
                                operator.message = format!(
                                    "spawn failed and Core retained reservation ownership ({release:?}): {error}"
                                );
                            }
                            response
                        }
                        Err(release_error) => {
                            if let Some(held) = reservation.take() {
                                retain_explicit_reservation(state, held);
                            }
                            core_operator_error(
                                "release_session_reservation",
                                &id.0,
                                &release_error,
                            )
                        }
                    }));
                }
                CoreTicketPoll::Ready(Ok(_)) => {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                }
            },
        }
    })
}

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    request: DaemonRequest,
) -> ControlStep {
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
            let mut tracker = runtime.begin_remove_session_for_owner(
                state.current_waiter_id.expect("owner waiter is assigned"),
                &SessionId(session_id.clone()),
            );
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
        DaemonRequest::Status => super::status::handle(daemon, state, observability, false),
        DaemonRequest::ListSessions => {
            let mut response = daemon_response_base(DaemonResponseKind::Sessions);
            response.sessions = projected_sessions(state);
            ControlStep::ready(response)
        }
        DaemonRequest::Spawn {
            session_id,
            command,
        } => handle_daemon_spawn(daemon, state, session_id, command),
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
            // The identity captured now fences every owner-side mutation
            // after Core answers; the generation makes the Core detach exact
            // so a same-client reattach keeps its own generation.
            let identity = state
                .pending_runtime
                .stream_identity(&session_id, &subscription_id);
            let mut ticket = runtime.detach_route_exact_or_owned_for_owner(
                state.current_waiter_id.expect("owner waiter is assigned"),
                ClientId(client_id),
                SessionId(session_id.clone()),
                SubscriptionId(subscription_id.clone()),
                generation,
                now,
            );
            let id = request_id("daemon-sessions-detach");
            ControlStep::pending(move |_, state| {
                let result = match ticket.poll() {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Lost => Err(CoreDaemonError::Shutdown),
                    CoreTicketPoll::Refused => Err(core_bridge_error(CoreTicketError::Overloaded)),
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
                        // Only the stream this request started against is
                        // closed; a replacement attached meanwhile keeps its
                        // adapter, reservation, and stream.
                        let owned = identity.as_ref().is_some_and(|identity| {
                            state.pending_runtime.stream_matches(
                                &session_id,
                                &subscription_id,
                                identity,
                            )
                        });
                        if owned {
                            let identity = identity.as_ref().expect("owned");
                            let _ = state.pending_runtime.close_adapter_if(
                                &session_id,
                                &subscription_id,
                                identity,
                            );
                            let labels = state
                                .pending_runtime
                                .admission
                                .reservations
                                .forget_route(&session_id, &subscription_id);
                            crate::daemon::owner_loop::retire_reservation_deadlines(state, labels);
                            let _ = state.pending_runtime.cancel_stream_if(
                                &session_id,
                                &subscription_id,
                                identity,
                            );
                        }
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
            let tracker =
                std::sync::Arc::new(std::sync::Mutex::new(runtime.begin_read_screen_for_owner(
                    state.current_waiter_id.expect("owner waiter is assigned"),
                    id.clone(),
                    SessionId(session_id.clone()),
                    now,
                )));
            let retire_tracker = std::sync::Arc::clone(&tracker);
            ControlStep::pending_retirable(
                move |daemon, _| {
                    let mut tracker = tracker.lock().expect("read tracker lock");
                    let completion = match poll_tracker(&mut tracker, daemon, "read_screen", &id.0)
                    {
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
                },
                move |_, state, waiter_id, permit| {
                    retain_operation_retirement(state, waiter_id, permit, retire_tracker)
                },
            )
        }
        DaemonRequest::ReadModeFlags { session_id } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
            let id = request_id("daemon-sessions-read-mode-flags");
            let tracker = std::sync::Arc::new(std::sync::Mutex::new(
                runtime.begin_read_mode_flags_for_owner(
                    state.current_waiter_id.expect("owner waiter is assigned"),
                    id.clone(),
                    SessionId(session_id.clone()),
                    now,
                ),
            ));
            let retire_tracker = std::sync::Arc::clone(&tracker);
            ControlStep::pending_retirable(
                move |daemon, _| {
                    let mut tracker = tracker.lock().expect("read tracker lock");
                    let completion =
                        match poll_tracker(&mut tracker, daemon, "read_mode_flags", &id.0) {
                            Ok(completion) => completion,
                            Err(poll) => return poll,
                        };
                    let CoreCompletion::ReadModeFlags { result, .. } = completion else {
                        return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                    };
                    ControlPoll::Ready(Ok(match result {
                        Ok(readback) => {
                            let mode = readback.mode_flags;
                            let mut response =
                                daemon_response_base(DaemonResponseKind::ReadModeFlags);
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
                },
                move |_, state, waiter_id, permit| {
                    retain_operation_retirement(state, waiter_id, permit, retire_tracker)
                },
            )
        }
        DaemonRequest::CaptureSnapshot { session_id } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
            let id = request_id("daemon-sessions-capture-snapshot");
            let owner = CaptureOwner(capture_owner_id(&observability, &client_id));
            // The tracker is shared with the retire hook: a retired capture
            // request cancels the pending operation or releases the capture
            // it produced, holding its permit until Core accepted that.
            let tracker = std::sync::Arc::new(std::sync::Mutex::new(
                runtime.begin_capture_snapshot_for_owner(
                    state.current_waiter_id.expect("owner waiter is assigned"),
                    id.clone(),
                    SessionId(session_id.clone()),
                    now,
                    owner,
                ),
            ));
            let retire_tracker = std::sync::Arc::clone(&tracker);
            ControlStep::pending_retirable(
                move |daemon, _| {
                    let mut tracker = tracker.lock().expect("capture tracker lock");
                    let completion =
                        match poll_tracker(&mut tracker, daemon, "capture_snapshot", &id.0) {
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
                },
                move |_, state, waiter_id, permit| {
                    retain_operation_retirement(state, waiter_id, permit, retire_tracker)
                },
            )
        }
        DaemonRequest::ReadSnapshotPage {
            session_id,
            capture_id,
            page,
        } => {
            let runtime = daemon.runtime().expect("runtime checked above");
            let id = request_id("daemon-sessions-read-snapshot-page");
            let mut ticket = runtime.read_snapshot_page_for_owner(
                state.current_waiter_id.expect("owner waiter is assigned"),
                CaptureId(capture_id.clone()),
                page,
            );
            ControlStep::pending(move |_, _| {
                let result = match ticket.poll() {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Lost => Err(CoreDaemonError::Shutdown),
                    CoreTicketPoll::Refused => Err(core_bridge_error(CoreTicketError::Overloaded)),
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

/// Retain the release of one exact Core generation as a budgeted obligation.
///
/// Used when a deferred attach completed in Core after its owner stopped
/// being the current attachment (connection closed, route replaced). The
/// generation is exact, so a replacement stream's generation is never
/// touched. `permit` was reserved before the attach was admitted and stays
/// held until Core accepts the release. One ticket is in flight; a refused
/// admission resubmits on the next owner turn; a lost driver ends the work.
pub(crate) fn retain_exact_detach(
    state: &mut DaemonControlState,
    permit: OwnerPermit,
    client_id: String,
    session_id: String,
    subscription_id: String,
    generation: TerminalSubscriptionGeneration,
) {
    let waiter_id = state
        .waiter_ids
        .next()
        .expect("an admitted cleanup permit must have an available waiter identifier");
    let mut slot: Option<CoreTicket<Result<(), CoreDaemonError>>> = None;
    crate::daemon::owner_budget::retain_owner_obligation(
        state,
        waiter_id,
        permit,
        "exact_generation_detach",
        move |daemon, state, waiter_id| match drive_core_slot(
            &mut slot,
            daemon,
            state,
            waiter_id,
            |runtime, state, waiter_id| {
                let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
                runtime.detach_route_exact_or_owned_for_owner(
                    waiter_id,
                    ClientId(client_id.clone()),
                    SessionId(session_id.clone()),
                    SubscriptionId(subscription_id.clone()),
                    Some(generation),
                    now,
                )
            },
        ) {
            CoreWorkPoll::Pending => ObligationPoll::Pending,
            CoreWorkPoll::Retry => ObligationPoll::ReadyAgain,
            CoreWorkPoll::Lost | CoreWorkPoll::Ready(_) => ObligationPoll::Done,
        },
    );
}

/// Retire one deferred Core operation whose request was abandoned: cancel
/// the pending operation when it has not run, keep the tracker until its
/// completion is consumed (Core may still emit one after cancel admission),
/// and release a capture the completion produced. The permit stays held
/// until Core accepted the last of those.
fn retain_operation_retirement(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
    permit: OwnerPermit,
    tracker: std::sync::Arc<std::sync::Mutex<CoreOperationTracker>>,
) {
    let mut cancel_slot: Option<CoreTicket<bool>> = None;
    let mut release_slot: Option<CoreTicket<bool>> = None;
    let mut cancel_requested = false;
    let mut release_capture: Option<CaptureId> = None;
    crate::daemon::owner_budget::retain_owner_obligation(
        state,
        waiter_id,
        permit,
        "operation_retirement",
        move |daemon, state, waiter_id| {
            if let Some(capture) = release_capture.clone() {
                return match drive_core_slot(
                    &mut release_slot,
                    daemon,
                    state,
                    waiter_id,
                    |runtime, _, waiter_id| {
                        runtime.submit_core_for_owner(waiter_id, move |daemon| {
                            daemon.release_capture(&capture)
                        })
                    },
                ) {
                    CoreWorkPoll::Pending => ObligationPoll::Pending,
                    CoreWorkPoll::Retry => ObligationPoll::ReadyAgain,
                    CoreWorkPoll::Lost | CoreWorkPoll::Ready(_) => ObligationPoll::Done,
                };
            }
            let pending_id = tracker.lock().expect("capture tracker lock").pending_id();
            if !cancel_requested && let Some(id) = pending_id {
                match drive_core_slot(
                    &mut cancel_slot,
                    daemon,
                    state,
                    waiter_id,
                    |runtime, _, waiter_id| {
                        runtime.submit_core_for_owner(waiter_id, move |daemon| daemon.cancel(id))
                    },
                ) {
                    CoreWorkPoll::Pending => return ObligationPoll::Pending,
                    CoreWorkPoll::Retry => return ObligationPoll::ReadyAgain,
                    CoreWorkPoll::Lost => return ObligationPoll::Done,
                    CoreWorkPoll::Ready(_) => cancel_requested = true,
                }
            }
            // Cancelled or not, the completion decides whether a capture
            // exists that must be released.
            let Some(runtime) = daemon.runtime() else {
                return ObligationPoll::Done;
            };
            let poll = tracker.lock().expect("capture tracker lock").poll(runtime);
            match poll {
                CoreTicketPoll::Pending => ObligationPoll::Pending,
                CoreTicketPoll::Lost | CoreTicketPoll::Refused | CoreTicketPoll::Ready(Err(_)) => {
                    ObligationPoll::Done
                }
                CoreTicketPoll::Ready(Ok(CoreCompletion::CaptureSnapshot {
                    result: Ok(capture),
                    ..
                })) => {
                    release_capture = Some(capture.capture_id);
                    ObligationPoll::ReadyAgain
                }
                CoreTicketPoll::Ready(Ok(_)) => ObligationPoll::Done,
            }
        },
    );
}

fn attach_route_limit_error() -> DaemonResponse {
    super::attach_bind_operator_error(
        ATTACH_ROUTE_LIMIT,
        "this connection already holds the maximum attach routes",
    )
}

fn owner_budget_error() -> DaemonResponse {
    super::attach_bind_operator_error(
        OWNER_BUDGET_EXHAUSTED,
        "the daemon holds its maximum retained requests and cleanup; retry later",
    )
}

fn stale_attach_error() -> DaemonResponse {
    super::attach_bind_operator_error(
        "invalid_request",
        "the attaching connection closed or the route was replaced before the attach completed",
    )
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
        // A route can only be attached by a peer that holds its budget
        // permit, so peer cleanup (the only place the permit is taken) is
        // the only owner a WebRTC route can be left with.
        let holds_permit = owner
            .grant_id
            .as_deref()
            .is_some_and(|grant_id| state.budget.peer_holds_permit(grant_id));
        if !holds_permit {
            return ControlStep::ready(owner_budget_error());
        }
        // Reserve the route key and the cleanup permit before any Core work
        // exists for this attach; both are released on every failure path.
        let reservation =
            reserve_attach_route(pending_runtime, &owner, &session_id, &subscription_id);
        if reservation == RouteReservation::Full {
            return ControlStep::ready(attach_route_limit_error());
        }
        let Some(cleanup_permit) = state.budget.reserve() else {
            release_failed_attach_route(
                &mut state.pending_runtime,
                &owner,
                &session_id,
                &subscription_id,
                reservation,
            );
            return ControlStep::ready(owner_budget_error());
        };
        let mut cleanup_permit = Some(cleanup_permit);
        let identity = state.pending_runtime.start_attach(
            owner.clone(),
            session_id.clone(),
            subscription_id.clone(),
        );
        let runtime = daemon.runtime().expect("runtime checked by caller");
        let mut ticket = runtime.attach_route_for_owner(
            state.current_waiter_id.expect("owner waiter is assigned"),
            ClientId(client_id.clone()),
            SessionId(session_id.clone()),
            SubscriptionId(subscription_id.clone()),
            now,
        );
        return ControlStep::pending(move |_, state| {
            let result = match ticket.poll() {
                CoreTicketPoll::Pending => return ControlPoll::Pending,
                CoreTicketPoll::Lost => Err(AttachBindFailure::Attach(CoreDaemonError::Shutdown)),
                CoreTicketPoll::Refused => Err(AttachBindFailure::Attach(core_bridge_error(
                    CoreTicketError::Overloaded,
                ))),
                CoreTicketPoll::Ready(result) => result,
            };
            let permit = cleanup_permit
                .take()
                .expect("cleanup permit held until the attach completes");
            let generation = match result {
                Ok(generation) => generation,
                Err(failure) => {
                    state.budget.release(permit);
                    let _ = state.pending_runtime.cancel_stream_if(
                        &session_id,
                        &subscription_id,
                        &identity,
                    );
                    release_failed_attach_route(
                        &mut state.pending_runtime,
                        &owner,
                        &session_id,
                        &subscription_id,
                        reservation,
                    );
                    return ControlPoll::Ready(Ok(super::attach_bind_operator_error(
                        "invalid_request",
                        &attach_bind_failure_message(&failure),
                    )));
                }
            };
            // Fence: the route is attached in Core, but only the attachment
            // that started it may record it. Otherwise release exactly it.
            if !state.pending_runtime.record_generation_if(
                &session_id,
                &subscription_id,
                &identity,
                generation,
            ) {
                retain_exact_detach(
                    state,
                    permit,
                    client_id.clone(),
                    session_id.clone(),
                    subscription_id.clone(),
                    generation,
                );
                release_failed_attach_route(
                    &mut state.pending_runtime,
                    &owner,
                    &session_id,
                    &subscription_id,
                    reservation,
                );
                return ControlPoll::Ready(Ok(stale_attach_error()));
            }
            let reserved = state.pending_runtime.admission.reservations.reserve(
                session_id.clone(),
                subscription_id.clone(),
                generation.0,
                peer_generation,
                now_seconds(),
            );
            let response = match reserved {
                Ok(reservation) => {
                    let budget_result = state
                        .pending_runtime
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
                        let _ = state
                            .pending_runtime
                            .admission
                            .reservations
                            .forget_label(&reservation.label, peer_generation);
                        Err(super::attach_bind_operator_error(
                            "connection_channel_limit",
                            "the WebRTC connection channel budget rejected the reservation",
                        ))
                    } else if crate::daemon::owner_loop::arm_reservation_deadline(
                        state,
                        reservation.label.clone(),
                        peer_generation,
                        reservation.expires_in_seconds,
                    ) {
                        Ok(daemon_terminal_reservation(reservation))
                    } else {
                        if let Some(budget) = state
                            .pending_runtime
                            .admission
                            .connection_budgets
                            .get_mut(&peer_generation)
                        {
                            let _ = budget.release(&reservation.label);
                        }
                        let _ = state
                            .pending_runtime
                            .admission
                            .reservations
                            .forget_label(&reservation.label, peer_generation);
                        Err(super::attach_bind_operator_error(
                            "owner_budget_exhausted",
                            "the daemon exhausted unique owner waiter identifiers",
                        ))
                    }
                }
                Err(ReserveError::LabelConflict) => Err(super::attach_bind_operator_error(
                    "reservation_label_conflict",
                    "a live reservation already exists for this route",
                )),
            };
            match response {
                Ok(response) => {
                    state.budget.release(permit);
                    ControlPoll::Ready(Ok(response))
                }
                Err(error) => {
                    // The route exists in Core without an adapter; release
                    // exactly the generation this attach created.
                    let _ = state.pending_runtime.cancel_stream_if(
                        &session_id,
                        &subscription_id,
                        &identity,
                    );
                    retain_exact_detach(
                        state,
                        permit,
                        client_id.clone(),
                        session_id.clone(),
                        subscription_id.clone(),
                        generation,
                    );
                    release_failed_attach_route(
                        &mut state.pending_runtime,
                        &owner,
                        &session_id,
                        &subscription_id,
                        reservation,
                    );
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
    // Reserve the route key and the cleanup permit before any Core work
    // exists for this attach; both are released on every failure path.
    let reservation = reserve_attach_route(pending_runtime, &owner, &session_id, &subscription_id);
    if reservation == RouteReservation::Full {
        return ControlStep::ready(attach_route_limit_error());
    }
    let Some(cleanup_permit) = state.budget.reserve() else {
        release_failed_attach_route(
            &mut state.pending_runtime,
            &owner,
            &session_id,
            &subscription_id,
            reservation,
        );
        return ControlStep::ready(owner_budget_error());
    };
    let mut cleanup_permit = Some(cleanup_permit);
    let identity = state.pending_runtime.start_attach(
        owner.clone(),
        session_id.clone(),
        subscription_id.clone(),
    );
    let (adapter, handle) = mux.create_adapter();
    let runtime = daemon.runtime().expect("runtime checked by caller");
    let mut ticket = runtime.attach_and_bind_terminal_for_owner(
        state.current_waiter_id.expect("owner waiter is assigned"),
        AttachBindPlan {
            client_id: ClientId(client_id.clone()),
            session_id: SessionId(session_id.clone()),
            subscription_id: SubscriptionId(subscription_id.clone()),
            capabilities,
            now_seconds: now,
            adapter: Box::new(adapter),
        },
    );
    ControlStep::pending(move |_, state| {
        let result = match ticket.poll() {
            CoreTicketPoll::Pending => return ControlPoll::Pending,
            CoreTicketPoll::Lost => Err(AttachBindFailure::Attach(CoreDaemonError::Shutdown)),
            CoreTicketPoll::Refused => Err(AttachBindFailure::Attach(core_bridge_error(
                CoreTicketError::Overloaded,
            ))),
            CoreTicketPoll::Ready(result) => result,
        };
        let permit = cleanup_permit
            .take()
            .expect("cleanup permit held until the attach completes");
        match result {
            Ok(generation) => {
                // Fence before any mutation: the stream must still be this
                // attachment and the connection mux must accept the route.
                // `register` fails closed once `close_all` ran, so a bind
                // that lands after connection teardown never outlives it.
                let live =
                    state
                        .pending_runtime
                        .stream_matches(&session_id, &subscription_id, &identity)
                        && mux.register(
                            session_id.clone(),
                            subscription_id.clone(),
                            generation.0,
                            handle.clone(),
                        );
                let bound = live
                    && state.pending_runtime.mark_adapter_bound_if(
                        &session_id,
                        &subscription_id,
                        &identity,
                        generation,
                        BoundAdapterHandle::Unix(handle.clone()),
                    );
                if !bound {
                    // Late success for a dead attachment: release exactly
                    // this generation and leave any replacement alone.
                    handle.close();
                    retain_exact_detach(
                        state,
                        permit,
                        client_id.clone(),
                        session_id.clone(),
                        subscription_id.clone(),
                        generation,
                    );
                    release_failed_attach_route(
                        &mut state.pending_runtime,
                        &owner,
                        &session_id,
                        &subscription_id,
                        reservation,
                    );
                    return ControlPoll::Ready(Ok(stale_attach_error()));
                }
                state.budget.release(permit);
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
                state.budget.release(permit);
                let _ = state.pending_runtime.cancel_stream_if(
                    &session_id,
                    &subscription_id,
                    &identity,
                );
                release_failed_attach_route(
                    &mut state.pending_runtime,
                    &owner,
                    &session_id,
                    &subscription_id,
                    reservation,
                );
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
    let waiter_id = state.current_waiter_id.expect("owner waiter is assigned");
    let mut stage = ShutdownStage::Classify(begin_shutdown_classification(
        runtime,
        waiter_id,
        &session_id,
        now,
    ));
    let id = request_id("daemon-sessions-shutdown");
    ControlStep::pending(move |daemon, state| {
        loop {
            match &mut stage {
                ShutdownStage::Classify(ticket) => {
                    let classification = match ticket.poll() {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Lost => Err(CoreDaemonError::Shutdown),
                        CoreTicketPoll::Refused => {
                            Err(core_bridge_error(CoreTicketError::Overloaded))
                        }
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
                    stage = ShutdownStage::Shutdown(runtime.begin_shutdown_session_for_owner(
                        state.current_waiter_id.expect("owner waiter is assigned"),
                        SessionId(session_id.clone()),
                    ));
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
                                ticket: begin_shutdown_classification(
                                    runtime,
                                    waiter_id,
                                    &session_id,
                                    now,
                                ),
                                error,
                            };
                        }
                    }
                }
                ShutdownStage::Recover { ticket, error } => {
                    let classification = match ticket.poll() {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Lost => Err(CoreDaemonError::Shutdown),
                        CoreTicketPoll::Refused => {
                            Err(core_bridge_error(CoreTicketError::Overloaded))
                        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::control::DaemonObservability;
    use crate::daemon::owner_loop::drive_ready_test_turn;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn observability() -> DaemonObservability {
        DaemonObservability {
            egress: Vec::new(),
            lifecycle: botster_hub_client::DaemonLifecycleCounters::default(),
            client_id: None,
            grant_id: None,
            transport_request_id: None,
        }
    }

    fn spawn_fixture(name: &str) -> (crate::HubDaemon, DaemonControlState, std::path::PathBuf) {
        spawn_fixture_with_worker(name, None)
    }

    const MATCHED_WORKER: &str = "/tmp/core-d1a-candidate-20260911-5/botster-session-worker";
    const MATCHED_WORKER_SHA256: &str =
        "1dfdd4f300409bf00a6694d1979650799b6280972bd24dd6cb592b21c0382f73";

    fn spawn_fixture_with_worker(
        name: &str,
        worker: Option<std::path::PathBuf>,
    ) -> (crate::HubDaemon, DaemonControlState, std::path::PathBuf) {
        spawn_fixture_case(name, worker, None)
    }

    fn spawn_fixture_case(
        name: &str,
        worker: Option<std::path::PathBuf>,
        shell: Option<String>,
    ) -> (crate::HubDaemon, DaemonControlState, std::path::PathBuf) {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::path::PathBuf::from("/private/tmp").join(format!(
            "s1-spawn-{name}-{}-{stamp}",
            std::process::id()
        ));
        let mut core_engine = crate::config::CoreEngineOptions::default();
        core_engine.session_worker_path = worker;
        let mut session_defaults = crate::config::SessionDefaults::default();
        if let Some(shell) = shell {
            session_defaults.shell = shell;
        }
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: format!("s1-{name}"),
                display_name: "S1 Spawn Test".into(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(root.clone()),
            session_defaults,
            core_engine,
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let daemon = crate::HubDaemon::start(config).unwrap();
        let mut state = DaemonControlState::default();
        state.current_waiter_id = Some(
            state
                .waiter_ids
                .next()
                .expect("fresh owner identity source"),
        );
        (daemon, state, root)
    }

    fn pump_core(daemon: &mut crate::HubDaemon, state: &mut DaemonControlState) {
        drive_ready_test_turn(daemon, state);
        if let Some(runtime) = daemon.runtime() {
            let identities = runtime.take_owner_core_completions(8);
            if !identities.is_empty() {
                let mut budget =
                    crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
                crate::daemon::control::pending::absorb_core_completions(
                    state,
                    &identities,
                    &mut budget,
                );
            }
        }
    }

    #[test]
    fn retained_release_codes_distinguish_core_outcomes() {
        assert_eq!(
            retained_release_code(SessionReservationRelease::Released),
            "released"
        );
        assert_eq!(
            retained_release_code(SessionReservationRelease::RetainedPending),
            "retained_pending"
        );
        assert_eq!(
            retained_release_code(SessionReservationRelease::RetainedUnconfirmed),
            "cleanup_unconfirmed"
        );
        assert_eq!(
            retained_release_code(SessionReservationRelease::RetainedSession),
            "retained_session"
        );
    }

    #[test]
    fn reserve_queue_full_retries_without_retaining_a_token() {
        let (mut daemon, mut state, root) = spawn_fixture("reserve-full");
        daemon
            .runtime()
            .unwrap()
            .test_refuse_next_owner_begins(1);
        let ControlStep::Pending(mut pending) = handle_runtime(
            &mut daemon,
            &mut state,
            observability(),
            DaemonRequest::Spawn {
                session_id: "s1-reserve-full".into(),
                command: "true".into(),
            },
        ) else {
            panic!("spawn must defer");
        };
        let ControlPoll::Ready(Ok(response)) =
            pending.continuation.poll(&mut daemon, &mut state)
        else {
            panic!("queue-full reserve must return immediately");
        };
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("pending_limit")
        );
        assert!(state.retained_explicit_reservations.is_empty());
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn lost_reserve_without_an_id_does_not_retain_a_token() {
        let (mut daemon, mut state, root) = spawn_fixture("reserve-lost");
        daemon
            .runtime()
            .unwrap()
            .test_lose_next_owner_begins(1);
        let ControlStep::Pending(mut pending) = handle_runtime(
            &mut daemon,
            &mut state,
            observability(),
            DaemonRequest::Spawn {
                session_id: "s1-reserve-lost".into(),
                command: "true".into(),
            },
        ) else {
            panic!("spawn must defer");
        };
        let poll = pending.continuation.poll(&mut daemon, &mut state);
        let ControlPoll::Ready(Ok(response)) = poll else {
            panic!("lost reserve without an id must complete");
        };
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("daemon_shutdown")
        );
        assert!(state.retained_explicit_reservations.is_empty());
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn spawn_reserved_queue_full_releases_the_held_reservation() {
        let (mut daemon, mut state, root) = spawn_fixture("spawn-full");
        let ControlStep::Pending(mut pending) = handle_runtime(
            &mut daemon,
            &mut state,
            observability(),
            DaemonRequest::Spawn {
                session_id: "s1-spawn-full".into(),
                command: "true".into(),
            },
        ) else {
            panic!("spawn must defer");
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut armed = false;
        loop {
            assert!(Instant::now() < deadline, "spawn lifecycle stalled");
            drive_ready_test_turn(&mut daemon, &mut state);
            if !armed {
                daemon
                    .runtime()
                    .unwrap()
                    .test_refuse_next_owner_begins(1);
                armed = true;
            }
            match pending.continuation.poll(&mut daemon, &mut state) {
                ControlPoll::Pending | ControlPoll::Again => std::thread::yield_now(),
                ControlPoll::Ready(Ok(_)) => break,
                ControlPoll::Ready(Err(_)) => panic!("spawn transport failed"),
                _ => std::thread::yield_now(),
            }
        }
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    fn wait_reservation(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        tracker: &mut crate::runtime::CoreOperationTracker,
    ) -> SessionReservation {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "reserve did not complete");
            drive_ready_test_turn(daemon, state);
            match tracker.poll(daemon.runtime().unwrap()) {
                CoreTicketPoll::Pending => std::thread::yield_now(),
                CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession {
                    result: Ok(reserved),
                    ..
                })) => return reserved,
                _ => panic!("reserve must complete with a token"),
            }
        }
    }

    #[test]
    fn retry_retained_full_queue_keeps_both_tokens_and_finishes() {
        let (mut daemon, mut state, root) = spawn_fixture("retry-two");
        let waiter = state.current_waiter_id.unwrap();
        let mut first = daemon.runtime().unwrap().begin_reserve_session_for_owner(
            waiter,
            SessionId("s1-retry-a".into()),
        );
        let a = wait_reservation(&mut daemon, &mut state, &mut first);
        let mut second = daemon.runtime().unwrap().begin_reserve_session_for_owner(
            waiter,
            SessionId("s1-retry-b".into()),
        );
        let b = wait_reservation(&mut daemon, &mut state, &mut second);
        state.retained_explicit_reservations.push(a);
        state.retained_explicit_reservations.push(b);
        daemon
            .runtime()
            .unwrap()
            .test_refuse_next_owner_begins(8);
        let ControlStep::Pending(mut pending) = handle_runtime(
            &mut daemon,
            &mut state,
            observability(),
            DaemonRequest::Spawn {
                session_id: "s1-retry-c".into(),
                command: "true".into(),
            },
        ) else {
            panic!("spawn must defer");
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut polls = 0_u32;
        let response = loop {
            assert!(Instant::now() < deadline, "retry-retained hang");
            polls += 1;
            assert!(polls < 32, "retry-retained spun");
            drive_ready_test_turn(&mut daemon, &mut state);
            match pending.continuation.poll(&mut daemon, &mut state) {
                ControlPoll::Pending | ControlPoll::Again => std::thread::yield_now(),
                ControlPoll::Ready(Ok(response)) => break response,
                ControlPoll::Ready(Err(_)) => panic!("spawn transport failed"),
                _ => std::thread::yield_now(),
            }
        };
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("pending_limit")
        );
        assert_eq!(state.retained_explicit_reservations.len(), 2);
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn concurrent_spawns_take_retained_tokens_once() {
        let (mut daemon, mut state, root) = spawn_fixture("concurrent");
        let waiter_a = state.current_waiter_id.unwrap();
        let waiter_b = state.waiter_ids.next().expect("second waiter");
        let mut reserve = daemon.runtime().unwrap().begin_reserve_session_for_owner(
            waiter_a,
            SessionId("s1-concurrent-held".into()),
        );
        let held = wait_reservation(&mut daemon, &mut state, &mut reserve);
        state.retained_explicit_reservations.push(held);
        daemon
            .runtime()
            .unwrap()
            .test_refuse_next_owner_begins(16);
        state.current_waiter_id = Some(waiter_a);
        let ControlStep::Pending(mut pending_a) = handle_runtime(
            &mut daemon,
            &mut state,
            observability(),
            DaemonRequest::Spawn {
                session_id: "s1-concurrent-a".into(),
                command: "true".into(),
            },
        ) else {
            panic!("spawn a must defer");
        };
        assert!(
            state.retained_explicit_reservations.is_empty(),
            "Spawn A must take retained tokens before Spawn B is created"
        );
        state.current_waiter_id = Some(waiter_b);
        let ControlStep::Pending(mut pending_b) = handle_runtime(
            &mut daemon,
            &mut state,
            observability(),
            DaemonRequest::Spawn {
                session_id: "s1-concurrent-b".into(),
                command: "true".into(),
            },
        ) else {
            panic!("spawn b must defer");
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut polls = 0_u32;
        let mut done_a = false;
        let mut done_b = false;
        while !(done_a && done_b) {
            assert!(Instant::now() < deadline, "concurrent spawn hang");
            polls += 1;
            assert!(polls < 64, "concurrent spawn spun");
            drive_ready_test_turn(&mut daemon, &mut state);
            if !done_a {
                match pending_a.continuation.poll(&mut daemon, &mut state) {
                    ControlPoll::Ready(Ok(_)) => done_a = true,
                    ControlPoll::Ready(Err(_)) => panic!("spawn a transport failed"),
                    _ => {}
                }
            }
            if !done_b {
                match pending_b.continuation.poll(&mut daemon, &mut state) {
                    ControlPoll::Ready(Ok(_)) => done_b = true,
                    ControlPoll::Ready(Err(_)) => panic!("spawn b transport failed"),
                    _ => {}
                }
            }
        }
        assert_eq!(state.retained_explicit_reservations.len(), 1);
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn accepting_queue_releases_a_retained_token_once() {
        let (mut daemon, mut state, root) = spawn_fixture("accept-release");
        let waiter = state.current_waiter_id.unwrap();
        let mut reserve = daemon.runtime().unwrap().begin_reserve_session_for_owner(
            waiter,
            SessionId("s1-accept-held".into()),
        );
        let held = wait_reservation(&mut daemon, &mut state, &mut reserve);
        state.retained_explicit_reservations.push(held);
        let ControlStep::Pending(mut pending) = handle_runtime(
            &mut daemon,
            &mut state,
            observability(),
            DaemonRequest::Spawn {
                session_id: "s1-accept-next".into(),
                command: "true".into(),
            },
        ) else {
            panic!("spawn must defer");
        };
        assert!(state.retained_explicit_reservations.is_empty());
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut polls = 0_u32;
        let mut armed = false;
        let response = loop {
            assert!(Instant::now() < deadline, "accepting-queue hang");
            polls += 1;
            assert!(polls < 64, "accepting-queue spun");
            pump_core(&mut daemon, &mut state);
            if !armed && daemon.runtime().unwrap().test_release_session_reservation_begins() >= 1 {
                daemon
                    .runtime()
                    .unwrap()
                    .test_refuse_next_owner_begins(8);
                armed = true;
            }
            match pending.continuation.poll(&mut daemon, &mut state) {
                ControlPoll::Pending | ControlPoll::Again => std::thread::yield_now(),
                ControlPoll::Ready(Ok(response)) => break response,
                ControlPoll::Ready(Err(_)) => panic!("spawn transport failed"),
                _ => std::thread::yield_now(),
            }
        };
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            1
        );
        assert!(state.retained_explicit_reservations.is_empty());
        assert!(response.error.is_some());
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    fn write_worker_script(root: &std::path::Path, body: &str) -> std::path::PathBuf {
        let path = root.join("scripted-worker");
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(&path, body).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        path
    }

    fn matched_worker_path() -> std::path::PathBuf {
        let path = std::path::PathBuf::from(MATCHED_WORKER);
        let hashed = std::process::Command::new("shasum")
            .args(["-a", "256", MATCHED_WORKER])
            .output()
            .expect("hash matched worker");
        let text = String::from_utf8(hashed.stdout).expect("hash utf8");
        assert!(
            text.starts_with(MATCHED_WORKER_SHA256),
            "matched worker hash {text}"
        );
        path
    }

    fn spawn_error_code(response: &botster_hub_client::DaemonResponse) -> Option<&str> {
        response.error.as_ref().map(|error| error.code.as_str())
    }

    fn poll_spawn_until_ready(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        pending: &mut crate::daemon::control::pending::PendingStep,
    ) -> botster_hub_client::DaemonResponse {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            assert!(Instant::now() < deadline, "scripted-worker spawn hang");
            pump_core(daemon, state);
            match pending.continuation.poll(daemon, state) {
                ControlPoll::Pending | ControlPoll::Again => std::thread::yield_now(),
                ControlPoll::Ready(Ok(response)) => return response,
                ControlPoll::Ready(Err(_)) => panic!("spawn transport failed"),
                _ => std::thread::yield_now(),
            }
        }
    }

    fn spawn_until_ready(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        session_id: &str,
        command: &str,
    ) -> botster_hub_client::DaemonResponse {
        let ControlStep::Pending(mut pending) = handle_runtime(
            daemon,
            state,
            observability(),
            DaemonRequest::Spawn {
                session_id: session_id.into(),
                command: command.into(),
            },
        ) else {
            panic!("spawn must defer");
        };
        poll_spawn_until_ready(daemon, state, &mut pending)
    }

    #[test]
    fn missing_session_worker_reaches_a_terminal_spawn_outcome() {
        let worker = std::path::PathBuf::from("/no/such/botster-session-worker");
        let (mut daemon, mut state, root) =
            spawn_fixture_with_worker("missing-worker", Some(worker));
        let response = spawn_until_ready(
            &mut daemon,
            &mut state,
            "s1-missing-worker",
            "true",
        );
        assert_eq!(spawn_error_code(&response), Some("core_error"));
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            1
        );
        assert!(state.retained_explicit_reservations.is_empty());
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn worker_exit_before_connect_releases_the_reservation() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::path::PathBuf::from("/private/tmp").join(format!(
            "s1-spawn-exit-before-{}-{stamp}",
            std::process::id()
        ));
        let worker = write_worker_script(
            &root,
            "#!/bin/sh\nprintf 'botster-session-worker-ready %s\\n' \"$$\"\nexit 1\n",
        );
        let (mut daemon, mut state, fixture_root) =
            spawn_fixture_with_worker("exit-before", Some(worker));
        let response = spawn_until_ready(
            &mut daemon,
            &mut state,
            "s1-exit-before",
            "true",
        );
        assert_eq!(spawn_error_code(&response), Some("core_error"));
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            1
        );
        assert!(state.retained_explicit_reservations.is_empty());
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(fixture_root);
    }

    #[test]
    fn worker_reads_spawn_frame_then_exits_retains_unconfirmed() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::path::PathBuf::from("/private/tmp").join(format!(
            "s1-spawn-frame-exit-{}-{stamp}",
            std::process::id()
        ));
        let python = String::from_utf8(
            std::process::Command::new("python3")
                .args(["-c", "import sys; print(sys.executable)"])
                .output()
                .expect("python3 executable")
                .stdout,
        )
        .expect("python path utf8");
        let worker = write_worker_script(
            &root,
            &format!(
                r#"#!{python}
import os, socket, sys
path = None
args = sys.argv
for i, arg in enumerate(args):
    if arg == "--control-socket" and i + 1 < len(args):
        path = args[i + 1]
        break
if not path:
    sys.exit(5)
if os.path.exists(path):
    os.unlink(path)
parent = os.path.dirname(path)
if parent:
    os.makedirs(parent, exist_ok=True)
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(path)
srv.listen(1)
sys.stdout.write("botster-session-worker-ready %s\n" % os.getpid())
sys.stdout.flush()
conn, _unused = srv.accept()
hello = conn.recv(5, socket.MSG_WAITALL)
if hello is None or len(hello) != 5:
    sys.exit(2)
n = conn.recv(4, socket.MSG_WAITALL)
if n is None or len(n) != 4:
    sys.exit(3)
length = int.from_bytes(n, "little")
body = b""
while len(body) < length:
    chunk = conn.recv(length - len(body))
    if not chunk:
        break
    body += chunk
if len(body) != length:
    sys.exit(4)
sys.exit(0)
"#,
                python = python.trim()
            ),
        );
        let (mut daemon, mut state, fixture_root) =
            spawn_fixture_with_worker("frame-exit", Some(worker));
        let first = spawn_until_ready(
            &mut daemon,
            &mut state,
            "s1-frame-exit",
            "true",
        );
        assert_eq!(
            spawn_error_code(&first),
            Some("cleanup_unconfirmed"),
            "{first:?}"
        );
        assert_eq!(state.retained_explicit_reservations.len(), 1);
        let held = state.retained_explicit_reservations[0].clone();
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            1
        );
        let second = spawn_until_ready(
            &mut daemon,
            &mut state,
            "s1-frame-exit-retry",
            "true",
        );
        assert_eq!(spawn_error_code(&second), Some("cleanup_unconfirmed"));
        assert!(
            state
                .retained_explicit_reservations
                .iter()
                .any(|token| token == &held),
            "next Spawn must keep the unconfirmed token"
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(fixture_root);
    }

    #[test]
    fn matched_worker_missing_session_executable_releases() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) = spawn_fixture_case(
            "matched-missing",
            Some(worker),
            Some("/no/such/botster-session-executable".into()),
        );
        let response = spawn_until_ready(
            &mut daemon,
            &mut state,
            "s1-matched-missing",
            "true",
        );
        assert_eq!(spawn_error_code(&response), Some("core_error"));
        let message = response
            .error
            .as_ref()
            .map(|error| error.message.as_str())
            .unwrap_or("");
        assert!(
            message.contains("worker startup failed"),
            "SPF1 NotCreated must surface as worker startup failed: {message}"
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            1
        );
        assert!(state.retained_explicit_reservations.is_empty());
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn matched_worker_real_command_installs() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) =
            spawn_fixture_with_worker("matched-install", Some(worker));
        let response = spawn_until_ready(
            &mut daemon,
            &mut state,
            "s1-matched-install",
            "true",
        );
        assert!(response.error.is_none(), "{response:?}");
        assert_eq!(response.kind, DaemonResponseKind::Spawned);
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            0
        );
        assert!(state.retained_explicit_reservations.is_empty());
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }
}
