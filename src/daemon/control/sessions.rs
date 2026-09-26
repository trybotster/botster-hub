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
use botster_core_daemon::operation::ReservedSpawnResult;
use botster_core_daemon::{
    CaptureId, CaptureOwner, CoreCompletion, CoreDaemonError, SpawnSessionRequest,
};
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
/// Reserve one WebRTC terminal channel for a running session: the route key,
/// the Hub attach stream, the labeled reservation, its channel budget, and
/// its deadline. Every failure releases what this call took.
///
/// This runs in the owner turn that completes the Attach's Core query, so it
/// rechecks what that query's wait could have changed (peer admission and
/// generation, the peer's budget permit, and a live reservation for the same
/// route) before it starts a stream: starting one would cancel another
/// attach's stream on this route.
pub(crate) fn reserve_webrtc_terminal(
    state: &mut DaemonControlState,
    owner: &AttachStreamOwner,
    session_id: &str,
    subscription_id: &str,
    peer_generation: u64,
) -> DaemonResponse {
    let session_id = session_id.to_string();
    let subscription_id = subscription_id.to_string();
    let owner = owner.clone();
    let Some(grant_id) = owner.grant_id.clone() else {
        return super::attach_bind_operator_error(
            "invalid_request",
            "Attach requires an admitted WebRTC adapter",
        );
    };
    let admitted = matches!(
        state.pending_runtime.admission.webrtc_admissions.get(&grant_id),
        Some(WebrtcTerminalAdmission::Admitted {
            peer_generation: current,
            ..
        }) if *current == peer_generation
    );
    if !admitted {
        return super::attach_bind_operator_error(
            "invalid_request",
            "Attach requires an admitted WebRTC adapter",
        );
    }
    if !state.budget.peer_holds_permit(&grant_id) {
        return owner_budget_error();
    }
    if state
        .pending_runtime
        .admission
        .reservations
        .has_live_for_route(
            &session_id,
            &subscription_id,
            peer_generation,
            now_seconds(),
        )
    {
        return super::attach_bind_operator_error(
            "reservation_label_conflict",
            "a live reservation already exists for this route",
        );
    }
    let route = reserve_attach_route(
        &mut state.pending_runtime,
        &owner,
        &session_id,
        &subscription_id,
    );
    if route == RouteReservation::Full {
        return attach_route_limit_error();
    }
    let identity = state.pending_runtime.start_attach(
        owner.clone(),
        session_id.clone(),
        subscription_id.clone(),
    );
    let reserved = state.pending_runtime.admission.reservations.reserve(
        session_id.clone(),
        subscription_id.clone(),
        peer_generation,
        now_seconds(),
        owner.clone(),
        identity.clone(),
        route,
    );
    let response = match reserved {
        Ok(reservation) => {
            let budget_result = state
                .pending_runtime
                .admission
                .connection_budgets
                .get_mut(&peer_generation)
                .ok_or(crate::admission::connection_budget::ChannelBudgetError::ChannelLimit)
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
        Ok(response) => response,
        Err(error) => {
            crate::daemon::control::connection::abandon_unbound_terminal(
                state,
                &owner,
                &identity,
                route,
                &session_id,
                &subscription_id,
            );
            error
        }
    }
}

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

/// Ordinary Spawn reports the client operation `spawn` in the error and its
/// diagnostic. The internal Core phase appears only in the message.
fn spawn_operator_error(
    request_id: &str,
    session_id: &str,
    phase: &'static str,
    error: &CoreDaemonError,
) -> DaemonResponse {
    let mut response = core_operator_error("spawn", request_id, error);
    let (code, message) = match error {
        CoreDaemonError::SessionReservation(botster_core::SessionReservationRefusal::Occupied) => (
            Some("session_already_exists"),
            format!("session {session_id} already exists"),
        ),
        CoreDaemonError::Engine(botster_core::ManagedSessionRuntimeError::Runtime(runtime))
            if runtime.kind == botster_core::SessionRuntimeErrorKind::SpawnFailed =>
        {
            (
                Some("spawn_failed"),
                format!(
                    "session worker could not start session {session_id}: {}",
                    runtime.message
                ),
            )
        }
        _ => (None, format!("{phase}: {error}")),
    };
    set_spawn_error(&mut response, code, message);
    response
}

fn set_spawn_error(response: &mut DaemonResponse, code: Option<&str>, message: String) {
    if let Some(operator) = response.error.as_mut() {
        if let Some(code) = code {
            operator.code = code.to_string();
        }
        operator.diagnostics = vec![DaemonDiagnostic::action_failure("spawn", message.clone())];
        operator.message = message;
        response.diagnostics = operator.diagnostics.clone();
    }
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

/// Delete the reservation record for this exact token.
fn retire_reservation_record(daemon: &HubDaemon, session_id: &str, token: &SessionReservation) {
    if let Some(runtime) = daemon.runtime() {
        runtime
            .session_reservations()
            .retire(session_id, token.identity());
    }
}

/// A reservation record already existed for this id; this spawn did not run.
fn session_record_invariant_error(request_id: &str, session_id: &str) -> DaemonResponse {
    let mut response = core_operator_error("spawn", request_id, &CoreDaemonError::Shutdown);
    set_spawn_error(
        &mut response,
        Some("session_record_invariant"),
        format!(
            "session {session_id} already has a reservation record; the spawn did not run and its new reservation was released or retained"
        ),
    );
    response
}

/// The Hub state budget cannot hold the reservation record; nothing started.
fn session_record_capacity_error(
    request_id: &str,
    session_id: &str,
    error: crate::shared_view::SharedViewCapacityError,
) -> DaemonResponse {
    let mut response = core_operator_error("spawn", request_id, &CoreDaemonError::Shutdown);
    set_spawn_error(
        &mut response,
        Some("session_record_capacity"),
        format!(
            "the Hub state budget cannot hold the reservation record for session {session_id}: requested {} bytes, {} available",
            error.requested, error.available
        ),
    );
    response
}

fn retain_explicit_reservation(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    reservation: SessionReservation,
) {
    if let Some(runtime) = daemon.runtime() {
        runtime.retain_reservation(reservation);
        state.retained_explicit_reservations = runtime.retained_reservations();
    }
}

fn merge_retry_keep(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    retry_keep: &mut Vec<SessionReservation>,
) {
    if let Some(runtime) = daemon.runtime() {
        runtime.merge_retained_reservations(std::mem::take(retry_keep));
        state.retained_explicit_reservations = runtime.retained_reservations();
    }
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
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    reservation: SessionReservation,
    request_id: &str,
    session_id: &str,
    phase: &'static str,
    error: CoreDaemonError,
) -> ControlPoll {
    // The retained list becomes the token's only owner in this same step.
    retire_reservation_record(daemon, session_id, &reservation);
    retain_explicit_reservation(daemon, state, reservation);
    ControlPoll::Ready(Ok(held_reservation_error(
        request_id, session_id, phase, &error,
    )))
}

/// Hub kept the reservation because no release outcome was confirmed.
fn held_reservation_error(
    request_id: &str,
    session_id: &str,
    phase: &'static str,
    cause: &CoreDaemonError,
) -> DaemonResponse {
    let mut response = spawn_operator_error(request_id, session_id, phase, cause);
    set_spawn_error(
        &mut response,
        Some("cleanup_unconfirmed"),
        format!(
            "spawn of session {session_id} failed at {phase}; Hub retained the session reservation and cleanup is unconfirmed: {cause}"
        ),
    );
    response
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
        /// The session was removed while it launched; release its token.
        ReleaseRemoved,
    }
    // The reservation record is charged before Core reserves the id, so a
    // refusal leaves nothing to clean up.
    let mut record_charge = match runtime.charge_session_reservation(&session_id) {
        Ok(charge) => Some(charge),
        Err(error) => {
            return ControlStep::ready(session_record_capacity_error(&id.0, &session_id, error));
        }
    };
    let mut retry_tokens = runtime.take_retained_reservations();
    state.retained_explicit_reservations.clear();
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
    let mut spawned_response: Option<DaemonResponse> = None;
    let mut record_invariant_failed = false;
    ControlStep::pending_spawn(move |daemon, state| {
        loop {
            match stage {
                Stage::RetryRetained => {
                    if retry_tokens.is_empty() {
                        merge_retry_keep(daemon, state, &mut retry_keep);
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
                    #[cfg(test)]
                    if let Some(runtime) = daemon.runtime() {
                        if runtime.test_retry_retained_again_on_pending() {
                            return ControlPoll::Again;
                        }
                        if runtime.test_resubmit_release_on_pending() {
                            if let Some(next) =
                                submit_release(daemon, waiter_id, retry_tokens[0].clone())
                            {
                                tracker = next;
                            }
                        }
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
                                    merge_retry_keep(daemon, state, &mut retry_keep);
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
                                    merge_retry_keep(daemon, state, &mut retry_keep);
                                    return ControlPoll::Ready(Err(
                                        DaemonTransportError::DaemonNotRunning,
                                    ));
                                }
                            }
                        }
                        CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                            ..
                        })) => {
                            retry_keep.push(retry_tokens.remove(0));
                            if retry_tokens.is_empty() {
                                continue;
                            }
                            match submit_release(daemon, waiter_id, retry_tokens[0].clone()) {
                                Some(next) => tracker = next,
                                None => {
                                    retry_keep.append(&mut retry_tokens);
                                    merge_retry_keep(daemon, state, &mut retry_keep);
                                    return ControlPoll::Ready(Err(
                                        DaemonTransportError::DaemonNotRunning,
                                    ));
                                }
                            }
                        }
                        CoreTicketPoll::Ready(Ok(_)) => {
                            return ControlPoll::Ready(Err(
                                DaemonTransportError::UnexpectedResponse,
                            ));
                        }
                    }
                }
                Stage::Reserve => match poll_spawn_ticket(&mut tracker, daemon) {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Refused => {
                        return ControlPoll::Ready(Ok(spawn_operator_error(
                            &id.0,
                            &session_id,
                            "reserve_session",
                            &core_bridge_error(CoreTicketError::Overloaded),
                        )));
                    }
                    CoreTicketPoll::Lost => {
                        // Poll can accept begin and lose completion in the same call.
                        let Some(reserve_id) = tracker.accepted_id() else {
                            return ControlPoll::Ready(Ok(spawn_operator_error(
                                &id.0,
                                &session_id,
                                "reserve_session",
                                &CoreDaemonError::Shutdown,
                            )));
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
                        return ControlPoll::Ready(Ok(spawn_operator_error(
                            &id.0,
                            &session_id,
                            "reserve_session",
                            &error,
                        )));
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession {
                        result, ..
                    })) => match result {
                        Ok(reserved) => {
                            reservation = Some(reserved.clone());
                            let Some(runtime) = daemon.runtime() else {
                                return ControlPoll::Ready(Err(
                                    DaemonTransportError::DaemonNotRunning,
                                ));
                            };
                            // Register before SpawnReserved so any later
                            // removal of this id finds the record.
                            let registered = match record_charge.take() {
                                Some(charge) => runtime
                                    .session_reservations()
                                    .register(charge, reserved.identity())
                                    .is_ok(),
                                None => false,
                            };
                            if !registered {
                                // A record for this id already exists: an
                                // earlier release obligation was lost. Keep
                                // that record, do not spawn, and release this
                                // new token.
                                eprintln!(
                                    "session reservation record invariant failed for {session_id}"
                                );
                                record_invariant_failed = true;
                                match submit_release(daemon, waiter_id, reserved) {
                                    Some(next) => {
                                        tracker = next;
                                        stage = Stage::Release;
                                    }
                                    None => {
                                        let held = reservation.take().expect("reserved identity");
                                        retain_explicit_reservation(daemon, state, held);
                                        return ControlPoll::Ready(Ok(
                                            session_record_invariant_error(&id.0, &session_id),
                                        ));
                                    }
                                }
                                continue;
                            }
                            tracker = runtime.begin_spawn_reserved_for_owner(
                                waiter_id,
                                reserved,
                                spawn.clone(),
                            );
                            stage = Stage::SpawnReserved;
                        }
                        Err(error) => {
                            return ControlPoll::Ready(Ok(spawn_operator_error(
                                &id.0,
                                &session_id,
                                "reserve_session",
                                &error,
                            )));
                        }
                    },
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                    }
                },
                Stage::Lookup => match poll_spawn_ticket(&mut tracker, daemon) {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Refused => {
                        return ControlPoll::Ready(Ok(spawn_operator_error(
                            &id.0,
                            &session_id,
                            "lookup_session_reservation",
                            &core_bridge_error(CoreTicketError::Overloaded),
                        )));
                    }
                    CoreTicketPoll::Lost => {
                        return ControlPoll::Ready(Ok(spawn_operator_error(
                            &id.0,
                            &session_id,
                            "lookup_session_reservation",
                            &CoreDaemonError::Shutdown,
                        )));
                    }
                    CoreTicketPoll::Ready(Err(error)) => {
                        return ControlPoll::Ready(Ok(spawn_operator_error(
                            &id.0,
                            &session_id,
                            "lookup_session_reservation",
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
                                        daemon,
                                        state,
                                        reservation.take().expect("looked-up reservation"),
                                        &id.0,
                                        &session_id,
                                        "lookup_session_reservation",
                                        CoreDaemonError::Shutdown,
                                    );
                                }
                            }
                        }
                        Ok(None) => {
                            return ControlPoll::Ready(Ok(spawn_operator_error(
                                &id.0,
                                &session_id,
                                "reserve_session",
                                &CoreDaemonError::Shutdown,
                            )));
                        }
                        Err(error) => {
                            return ControlPoll::Ready(Ok(spawn_operator_error(
                                &id.0,
                                &session_id,
                                "lookup_session_reservation",
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
                            daemon,
                            state,
                            reservation.take().expect("reserved identity"),
                            &id.0,
                            &session_id,
                            "spawn_reserved",
                            core_bridge_error(CoreTicketError::Overloaded),
                        );
                    }
                    CoreTicketPoll::Lost => {
                        return finish_held_reservation(
                            daemon,
                            state,
                            reservation.take().expect("reserved identity"),
                            &id.0,
                            &session_id,
                            "spawn_reserved",
                            CoreDaemonError::Shutdown,
                        );
                    }
                    CoreTicketPoll::Ready(Err(error)) => {
                        return finish_held_reservation(
                            daemon,
                            state,
                            reservation.take().expect("reserved identity"),
                            &id.0,
                            &session_id,
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
                        let response = daemon_spawned(
                            DaemonSession {
                                session_id: session.session_id.0,
                                lifecycle: lifecycle_label(&session.lifecycle).to_string(),
                            },
                            Vec::new(),
                        );
                        let Some(runtime) = daemon.runtime() else {
                            return ControlPoll::Ready(Ok(response));
                        };
                        let token = reservation.take().expect("reserved identity");
                        match runtime.session_reservations().install(&session_id, token) {
                            crate::runtime::session_reservations::Handoff::Kept => {
                                return ControlPoll::Ready(Ok(response));
                            }
                            crate::runtime::session_reservations::Handoff::Unregistered(token) => {
                                retain_explicit_reservation(daemon, state, token);
                                return ControlPoll::Ready(Ok(response));
                            }
                            crate::runtime::session_reservations::Handoff::ReleaseNow(token) => {
                                // A client removed the session while it
                                // launched. This spawn still owns the token.
                                tracker = runtime.begin_release_session_reservation_for_owner(
                                    waiter_id,
                                    token.clone(),
                                );
                                reservation = Some(token);
                                spawned_response = Some(response);
                                stage = Stage::ReleaseRemoved;
                            }
                        }
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
                                    daemon,
                                    state,
                                    reservation.take().expect("reserved identity"),
                                    &id.0,
                                    &session_id,
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
                Stage::ReleaseRemoved => {
                    let released = match poll_spawn_ticket(&mut tracker, daemon) {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                            result: Ok(SessionReservationRelease::Released),
                            ..
                        })) => true,
                        _ => false,
                    };
                    let token = reservation.take().expect("removed reservation");
                    retire_reservation_record(daemon, &session_id, &token);
                    let mut response = spawned_response.take().expect("installed spawn response");
                    if !released {
                        retain_explicit_reservation(daemon, state, token);
                        response.diagnostics.push(DaemonDiagnostic::action_failure(
                            "spawn",
                            format!(
                                "session {session_id} was already removed; its reservation release is unconfirmed and the token is retained for retry"
                            ),
                        ));
                    }
                    return ControlPoll::Ready(Ok(response));
                }
                Stage::Release => match poll_spawn_ticket(&mut tracker, daemon) {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Refused
                    | CoreTicketPoll::Lost
                    | CoreTicketPoll::Ready(Err(_)) => {
                        if record_invariant_failed {
                            let held = reservation.take().expect("reserved identity");
                            retain_explicit_reservation(daemon, state, held);
                            return ControlPoll::Ready(Ok(session_record_invariant_error(
                                &id.0,
                                &session_id,
                            )));
                        }
                        let error = spawn_error.take().unwrap_or(CoreDaemonError::Shutdown);
                        return finish_held_reservation(
                            daemon,
                            state,
                            reservation.take().expect("reserved identity"),
                            &id.0,
                            &session_id,
                            "release_session_reservation",
                            error,
                        );
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                        result,
                        ..
                    })) => {
                        let error = spawn_error.take().unwrap_or(CoreDaemonError::Shutdown);
                        if record_invariant_failed {
                            // This token was never registered; the existing
                            // record belongs to the earlier obligation.
                            if !matches!(result, Ok(SessionReservationRelease::Released))
                                && let Some(held) = reservation.take()
                            {
                                retain_explicit_reservation(daemon, state, held);
                            }
                            return ControlPoll::Ready(Ok(session_record_invariant_error(
                                &id.0,
                                &session_id,
                            )));
                        }
                        if let Some(held) = reservation.as_ref() {
                            retire_reservation_record(daemon, &session_id, held);
                        }
                        return ControlPoll::Ready(Ok(match result {
                            Ok(SessionReservationRelease::Released) => {
                                spawn_operator_error(&id.0, &session_id, "spawn_reserved", &error)
                            }
                            Ok(release) => {
                                if let Some(held) = reservation.take() {
                                    retain_explicit_reservation(daemon, state, held);
                                }
                                let mut response = spawn_operator_error(
                                    &id.0,
                                    &session_id,
                                    "release_session_reservation",
                                    &error,
                                );
                                set_spawn_error(
                                    &mut response,
                                    Some(retained_release_code(release)),
                                    format!(
                                        "spawn failed and Core retained reservation ownership ({release:?}): {error}"
                                    ),
                                );
                                response
                            }
                            Err(release_error) => {
                                if let Some(held) = reservation.take() {
                                    retain_explicit_reservation(daemon, state, held);
                                }
                                held_reservation_error(
                                    &id.0,
                                    &session_id,
                                    "release_session_reservation",
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
            let waiter_id = state.current_waiter_id.expect("owner waiter is assigned");
            // The receipt applies only to the record that exists now.
            let captured = runtime.session_reservations().capture(&session_id);
            let mut tracker =
                runtime.begin_remove_session_for_owner(waiter_id, &SessionId(session_id.clone()));
            let id = request_id("daemon-session-remove");
            let mut releasing: Option<SessionReservation> = None;
            ControlStep::pending(move |daemon, state| {
                if releasing.is_some() {
                    let released = match poll_spawn_ticket(&mut tracker, daemon) {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                            result: Ok(SessionReservationRelease::Released),
                            ..
                        })) => true,
                        _ => false,
                    };
                    let token = releasing.take().expect("releasing token");
                    retire_reservation_record(daemon, &session_id, &token);
                    let mut response = daemon_response_base(DaemonResponseKind::SessionRemoved);
                    if !released {
                        retain_explicit_reservation(daemon, state, token);
                        response.diagnostics.push(DaemonDiagnostic::action_failure(
                            "remove_session",
                            format!(
                                "session {session_id} was removed; its reservation release is unconfirmed and will be retried"
                            ),
                        ));
                    }
                    return ControlPoll::Ready(Ok(response));
                }
                let completion = match poll_tracker(&mut tracker, daemon, "remove_session", &id.0) {
                    Ok(completion) => completion,
                    Err(poll) => return poll,
                };
                let CoreCompletion::RemoveSession { result, .. } = completion else {
                    return ControlPoll::Ready(Err(DaemonTransportError::UnexpectedResponse));
                };
                match result {
                    Ok(true) => {
                        suppress_unix_session_close_events(&state.pending_runtime, &session_id);
                        suppress_webrtc_session_close_events(&state.pending_runtime, &session_id);
                        let removal = match (daemon.runtime(), captured) {
                            (Some(runtime), Some(captured)) => runtime
                                .session_reservations()
                                .removed(&session_id, captured),
                            _ => crate::runtime::session_reservations::Removal::None,
                        };
                        if let crate::runtime::session_reservations::Removal::ReleaseNow(token) =
                            removal
                        {
                            let Some(runtime) = daemon.runtime() else {
                                return ControlPoll::Ready(Err(
                                    DaemonTransportError::DaemonNotRunning,
                                ));
                            };
                            tracker = runtime.begin_release_session_reservation_for_owner(
                                waiter_id,
                                token.clone(),
                            );
                            releasing = Some(token);
                            return ControlPoll::Again;
                        }
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
        // A WebRTC attach declares nothing in Core. One read-only Core query
        // rejects a session that is not running before anything is reserved;
        // a session that ends before its channel binds gets the channel's
        // bind_failed reject. Core attaches and binds the route in one call
        // when the reserved channel's Hello arrives, so a busy session cannot
        // overflow a route that has no adapter yet.
        let runtime = daemon.runtime().expect("runtime checked by caller");
        let lookup = SessionId(session_id.clone());
        let mut ticket = runtime.submit_core_for_owner(
            state.current_waiter_id.expect("owner waiter is assigned"),
            move |daemon| daemon.session_registry_state(&lookup),
        );
        return ControlStep::pending(move |_, state| {
            let refusal = match ticket.poll() {
                CoreTicketPoll::Pending => return ControlPoll::Pending,
                CoreTicketPoll::Ready(Ok(
                    botster_core_daemon::SessionRegistryStateLookup::Found(
                        botster_core_daemon::RegistrySessionState::Running,
                    ),
                )) => None,
                CoreTicketPoll::Ready(Ok(_)) => Some(format!(
                    "attach failed before adapter bind: session {session_id} is not running"
                )),
                CoreTicketPoll::Ready(Err(error)) => {
                    Some(format!("attach failed before adapter bind: {error}"))
                }
                CoreTicketPoll::Lost => Some(attach_bind_failure_message(
                    &AttachBindFailure::Attach(CoreDaemonError::Shutdown),
                )),
                CoreTicketPoll::Refused => Some(attach_bind_failure_message(
                    &AttachBindFailure::Attach(core_bridge_error(CoreTicketError::Overloaded)),
                )),
            };
            if let Some(message) = refusal {
                return ControlPoll::Ready(Ok(super::attach_bind_operator_error(
                    "invalid_request",
                    &message,
                )));
            }
            ControlPoll::Ready(Ok(reserve_webrtc_terminal(
                state,
                &owner,
                &session_id,
                &subscription_id,
                peer_generation,
            )))
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
                    suppress_unix_session_close_events(&state.pending_runtime, &session_id);
                    suppress_webrtc_session_close_events(&state.pending_runtime, &session_id);
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
    use crate::daemon::control::managed_git::accept_one;
    use crate::daemon::owner_loop::{drive_ready_test_turn, publish_completion_wakes};
    use crate::host_executor::TestHostGate;
    use std::sync::Arc;
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

    fn collect_phase_pair(
        daemon: &HubDaemon,
        receiver: &mut tokio::sync::mpsc::Receiver<crate::daemon::control::message::ControlMessage>,
        waiter_id: crate::owner_identity::WaiterId,
        first_phase: u64,
    ) -> Vec<crate::owner_identity::OwnerWorkIdentity> {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let identities = executor.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let mut identities = Vec::new();
                while identities.len() < 2 {
                    receiver.recv().await.expect("Core must notify the owner");
                    identities.extend(daemon.runtime().unwrap().take_owner_core_completions(2));
                }
                identities
            })
            .await
            .expect("both registered phases must publish")
        });
        assert_eq!(
            identities,
            vec![
                crate::owner_identity::OwnerWorkIdentity {
                    waiter_id,
                    phase: first_phase
                },
                crate::owner_identity::OwnerWorkIdentity {
                    waiter_id,
                    phase: first_phase + 1
                },
            ]
        );
        identities
    }

    fn insert_phase_test_row(
        state: &mut DaemonControlState,
        waiter_id: crate::owner_identity::WaiterId,
        continuation: crate::daemon::control::pending::ControlContinuation,
    ) {
        use crate::daemon::control::pending::{OwnerRequestCompletion, PendingControlRequest};
        let permit = state.budget.reserve().unwrap();
        state.pending_requests.insert(
            waiter_id,
            PendingControlRequest {
                waiter_id,
                ready_class: crate::daemon::owner_schedule::ReadyClass::CoreCompletion,
                ready_key: None,
                deadline_key: None,
                last_core_phase: 0,
                last_host_phase: 0,
                completion: OwnerRequestCompletion::default(),
                reply_tx: crate::daemon::control::message::ControlReplySender::absent(),
                response_delivery_rx: None,
                grant_id: None,
                client: None,
                core_retirement: None,
                permit: Some(permit),
                must_finish: true,
                past_deadline: false,
                continuation,
                retire: None,
            },
        );
    }

    fn absorb_phase_test_pair(
        state: &mut DaemonControlState,
        identities: &[crate::owner_identity::OwnerWorkIdentity],
    ) {
        assert_eq!(
            crate::daemon::control::pending::absorb_core_completions(
                state,
                identities,
                &mut crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now()),
            ),
            identities.len()
        );
    }

    fn run_phase_test_ready(daemon: &mut HubDaemon, state: &mut DaemonControlState) -> bool {
        let item = state
            .owner_ready
            .pop_next()
            .expect("spawn cleanup must become ready after injected post-registration refusal");
        let mut finished = false;
        crate::daemon::control::pending::poll_ready_request_item(
            daemon,
            state,
            item,
            &mut |_, state, mut entry, _| {
                finished = true;
                drop(entry.continuation);
                state.budget.release(entry.permit.take().unwrap());
                false
            },
        );
        finished
    }

    #[test]
    fn managed_spawn_cleanup_wakes_after_registered_refusal() {
        use crate::daemon::control::pending::ControlContinuation;
        let (mut daemon, mut state, root) = spawn_fixture("managed-phase-gap");
        let waiter = daemon.runtime().unwrap().next_waiter_id().unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
        daemon.runtime().unwrap().bind_data_plane_owner_wake(sender);
        let worktree = root.join("existing-worktree");
        std::fs::create_dir_all(&worktree).unwrap();
        let mut record = plugin_spawn_package(&root.join("p1-plugin"));
        record.manifest.capabilities.push(botster_core::Capability {
            surface: botster_core::CapabilitySurface::SessionActions,
            scope: Some("session_type_managed_git_spawn".into()),
        });
        record.session_types[0].target_id = Some("t1".into());
        let runtime = daemon.runtime().unwrap();
        let mut next = (*runtime.state()).clone();
        next.spawn_targets.push(crate::spawn_targets::SpawnTarget {
            target_id: "t1".into(),
            label: "t1".into(),
            root: worktree.clone(),
            enabled: true,
            kind: "directory".into(),
            base_ref: None,
            metadata: Default::default(),
        });
        runtime.replace_state(next).unwrap();
        let (response, response_rx) = std::sync::mpsc::channel();
        let pending = crate::runtime::PendingManagedSessionSpawn::test_new(
            botster_core::PluginKey("p1.plugin".into()),
            "t1".into(),
            "topic".into(),
            "agent".into(),
            crate::session_types::ManagedSessionTypeRequest::default(),
            vec![record],
            response,
        );
        let prepared = crate::managed_git_worktrees::PreparedManagedWorktree {
            target_id: "t1".into(),
            repository_root: worktree.clone(),
            common_dir: worktree.clone(),
            branch: "topic".into(),
            path: worktree,
            worktree_id: "wt-phase-gap".into(),
            base_ref: "HEAD".into(),
            base_commit: "0".repeat(40),
            head_commit: "0".repeat(40),
            created_worktree: false,
            created_branch: false,
        };
        let start = runtime
            .spawn_prepared_managed_session(&pending, &prepared, waiter)
            .unwrap();
        let session_id = start
            .context
            .as_ref()
            .expect("the prepared managed spawn retains its context")
            .session_id
            .clone();
        let operation =
            crate::daemon::control::managed_git::ManagedSpawnOperation::test_spawn_phase(
                waiter, pending, prepared, start,
            );
        insert_phase_test_row(
            &mut state,
            waiter,
            ControlContinuation::ManagedSpawn(Box::new(operation)),
        );
        let reserve = collect_phase_pair(&daemon, &mut receiver, waiter, 1);
        // Production needs capacity to open between refusal and immediate cleanup admission.
        // This seam injects refusal after registration without a timing race.
        daemon
            .runtime()
            .unwrap()
            .test_refuse_registered_owner_begins(1);
        absorb_phase_test_pair(&mut state, &reserve);
        assert!(!run_phase_test_ready(&mut daemon, &mut state));
        let ControlContinuation::ManagedSpawn(operation) =
            &state.pending_requests[&waiter].continuation
        else {
            panic!("the managed continuation must retain the pending release");
        };
        let reservation = operation.test_spawn_reservation().unwrap();
        assert_eq!(reservation.session_id(), &session_id);
        let release = collect_phase_pair(&daemon, &mut receiver, waiter, 5);
        absorb_phase_test_pair(&mut state, &release);
        assert!(run_phase_test_ready(&mut daemon, &mut state));
        assert_eq!(
            response_rx.try_recv().unwrap().unwrap_err().kind,
            "spawn_failed"
        );
        assert_eq!(
            reservation.state(),
            botster_core::SessionReservationState::Released
        );
        assert!(daemon.runtime().unwrap().retained_reservations().is_empty());
        assert!(
            daemon
                .runtime()
                .unwrap()
                .test_session_context(&session_id.0)
                .is_none()
        );
        assert!(state.pending_requests.is_empty());
        assert!(state.owner_ready.is_empty());
        assert_eq!(state.budget.outstanding(), 0);
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn direct_retry_retained_wakes_after_registered_refusal() {
        use crate::daemon::control::pending::{READY_INITIAL, mark_owner_ready};
        let (mut daemon, mut state, root) = spawn_fixture("direct-phase-gap");
        let runtime = daemon.runtime().unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
        runtime.bind_data_plane_owner_wake(sender);
        let setup_waiter = runtime.next_waiter_id().unwrap();
        let mut tokens = Vec::new();
        for (index, name) in ["phase-gap-retained-a", "phase-gap-retained-b"]
            .into_iter()
            .enumerate()
        {
            let mut reserve =
                runtime.begin_reserve_session_for_owner(setup_waiter, SessionId(name.into()));
            collect_phase_pair(&daemon, &mut receiver, setup_waiter, 1 + index as u64 * 2);
            let CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession {
                result: Ok(token), ..
            })) = reserve.poll(runtime)
            else {
                panic!("the fixture must reserve both retained tokens");
            };
            runtime.retain_reservation(token.clone());
            tokens.push(token);
        }
        let waiter = runtime.next_waiter_id().unwrap();
        state.current_waiter_id = Some(waiter);
        // The first retained release refuses after registration. The next release is admitted.
        runtime.test_refuse_registered_owner_begins(1);
        let ControlStep::Pending(step) = handle_daemon_spawn(
            &daemon,
            &mut state,
            "phase-gap-retained-a".into(),
            "exit 0".into(),
        ) else {
            panic!("the direct spawn must retain its continuation");
        };
        state.current_waiter_id = None;
        insert_phase_test_row(&mut state, waiter, step.continuation);
        assert!(mark_owner_ready(
            &mut state,
            waiter,
            crate::daemon::owner_schedule::ReadyClass::CoreCompletion,
            READY_INITIAL
        ));
        assert!(!run_phase_test_ready(&mut daemon, &mut state));
        let release = collect_phase_pair(&daemon, &mut receiver, waiter, 3);
        absorb_phase_test_pair(&mut state, &release);
        assert!(!run_phase_test_ready(&mut daemon, &mut state));
        assert_eq!(
            tokens[1].state(),
            botster_core::SessionReservationState::Released
        );
        // The requested ID remains reserved by the first token, so no child can launch.
        let reserve = collect_phase_pair(&daemon, &mut receiver, waiter, 5);
        absorb_phase_test_pair(&mut state, &reserve);
        assert!(run_phase_test_ready(&mut daemon, &mut state));
        assert_eq!(
            daemon.runtime().unwrap().retained_reservations(),
            vec![tokens[0].clone()]
        );
        assert_eq!(
            state.retained_explicit_reservations,
            vec![tokens[0].clone()]
        );
        assert!(state.pending_requests.is_empty());
        assert!(state.owner_ready.is_empty());
        assert_eq!(state.budget.outstanding(), 0);
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

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
        let root = std::path::PathBuf::from("/private/tmp")
            .join(format!("s1-spawn-{name}-{}-{stamp}", std::process::id()));
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
            runtime.reap_detached_core_operations();
            let identities = runtime.take_owner_core_completions(8);
            if !identities.is_empty() {
                let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
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
        daemon.runtime().unwrap().test_refuse_next_owner_begins(1);
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
        let ControlPoll::Ready(Ok(response)) = pending.continuation.poll(&mut daemon, &mut state)
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
        daemon.runtime().unwrap().test_lose_next_owner_begins(1);
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
                daemon.runtime().unwrap().test_refuse_next_owner_begins(1);
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
    fn merge_retained_keeps_token_pushed_during_take() {
        let (mut daemon, mut state, root) = spawn_fixture("merge-keep");
        let waiter = state.current_waiter_id.unwrap();
        let mut first = daemon
            .runtime()
            .unwrap()
            .begin_reserve_session_for_owner(waiter, SessionId("s1-merge-a".into()));
        let a = wait_reservation(&mut daemon, &mut state, &mut first);
        let mut second = daemon
            .runtime()
            .unwrap()
            .begin_reserve_session_for_owner(waiter, SessionId("s1-merge-b".into()));
        let b = wait_reservation(&mut daemon, &mut state, &mut second);
        let runtime = daemon.runtime().unwrap();
        runtime.retain_reservation(a.clone());
        let taken = runtime.take_retained_reservations();
        runtime.retain_reservation(b.clone());
        runtime.merge_retained_reservations(taken);
        let held = runtime.retained_reservations();
        assert_eq!(held.len(), 2);
        assert!(held.iter().any(|token| token == &a));
        assert!(held.iter().any(|token| token == &b));
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn retract_spawn_context_keeps_unrelated_identity() {
        let (mut daemon, _state, root) = spawn_fixture("retract-ctx");
        let runtime = daemon.runtime().unwrap();
        let live = crate::session_types::HubSessionContext {
            context_id: "live-ctx".into(),
            session_id: SessionId("s1-live".into()),
            values: Default::default(),
        };
        let other = crate::session_types::HubSessionContext {
            context_id: "live-ctx".into(),
            session_id: SessionId("s1-live".into()),
            values: Default::default(),
        };
        let live_identity = runtime.test_publish_spawn_context(&live);
        let other_identity = botster_core::SessionAdmission::default()
            .reserve(other.session_id.clone())
            .unwrap()
            .identity();
        assert_ne!(live_identity, other_identity);
        runtime.retract_spawn_context(&other, other_identity);
        assert_eq!(
            runtime
                .test_session_context("s1-live")
                .map(|context| context.context_id),
            Some("live-ctx".into())
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn retry_retained_full_queue_keeps_both_tokens_and_finishes() {
        let (mut daemon, mut state, root) = spawn_fixture("retry-two");
        let waiter = state.current_waiter_id.unwrap();
        let mut first = daemon
            .runtime()
            .unwrap()
            .begin_reserve_session_for_owner(waiter, SessionId("s1-retry-a".into()));
        let a = wait_reservation(&mut daemon, &mut state, &mut first);
        let mut second = daemon
            .runtime()
            .unwrap()
            .begin_reserve_session_for_owner(waiter, SessionId("s1-retry-b".into()));
        let b = wait_reservation(&mut daemon, &mut state, &mut second);
        daemon.runtime().unwrap().retain_reservation(a);
        daemon.runtime().unwrap().retain_reservation(b);
        daemon.runtime().unwrap().test_refuse_next_owner_begins(8);
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
        let mut loops = 0_u32;
        let mut again = 0_u32;
        let response = loop {
            loops += 1;
            let begins = daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins();
            let refusals = daemon
                .runtime()
                .unwrap()
                .test_refuse_next_owner_begins_remaining();
            assert!(
                Instant::now() < deadline,
                "retry-retained hang loops={loops} again={again} begins={begins} refusals={refusals}"
            );
            drive_ready_test_turn(&mut daemon, &mut state);
            match pending.continuation.poll(&mut daemon, &mut state) {
                ControlPoll::Pending => std::thread::yield_now(),
                ControlPoll::Again => {
                    again += 1;
                    panic!(
                        "retry-retained Again while waiting on Core loops={loops} again={again} begins={begins} refusals={refusals}"
                    );
                }
                ControlPoll::Ready(Ok(response)) => break response,
                ControlPoll::Ready(Err(_)) => panic!("spawn transport failed"),
                _ => std::thread::yield_now(),
            }
        };
        assert_eq!(again, 0);
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
        let mut reserve = daemon
            .runtime()
            .unwrap()
            .begin_reserve_session_for_owner(waiter_a, SessionId("s1-concurrent-held".into()));
        let held = wait_reservation(&mut daemon, &mut state, &mut reserve);
        daemon.runtime().unwrap().retain_reservation(held);
        daemon.runtime().unwrap().test_refuse_next_owner_begins(16);
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
        let mut loops = 0_u32;
        let mut again = 0_u32;
        let mut done_a = false;
        let mut done_b = false;
        while !(done_a && done_b) {
            loops += 1;
            let begins = daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins();
            let refusals = daemon
                .runtime()
                .unwrap()
                .test_refuse_next_owner_begins_remaining();
            assert!(
                Instant::now() < deadline,
                "concurrent spawn hang loops={loops} again={again} begins={begins} refusals={refusals}"
            );
            drive_ready_test_turn(&mut daemon, &mut state);
            if !done_a {
                match pending_a.continuation.poll(&mut daemon, &mut state) {
                    ControlPoll::Ready(Ok(_)) => done_a = true,
                    ControlPoll::Ready(Err(_)) => panic!("spawn a transport failed"),
                    ControlPoll::Again => {
                        again += 1;
                        panic!(
                            "concurrent spawn A Again while waiting on Core loops={loops} again={again} begins={begins} refusals={refusals}"
                        );
                    }
                    _ => {}
                }
            }
            if !done_b {
                match pending_b.continuation.poll(&mut daemon, &mut state) {
                    ControlPoll::Ready(Ok(_)) => done_b = true,
                    ControlPoll::Ready(Err(_)) => panic!("spawn b transport failed"),
                    ControlPoll::Again => {
                        again += 1;
                        panic!(
                            "concurrent spawn B Again while waiting on Core loops={loops} again={again} begins={begins} refusals={refusals}"
                        );
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(again, 0);
        assert_eq!(state.retained_explicit_reservations.len(), 1);
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    fn drive_accepting_queue(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        pending: &mut crate::daemon::control::pending::PendingStep,
    ) -> botster_hub_client::DaemonResponse {
        assert!(state.retained_explicit_reservations.is_empty());
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut loops = 0_u32;
        let mut again = 0_u32;
        let mut armed = false;
        let response = loop {
            loops += 1;
            let begins = daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins();
            let refusals = daemon
                .runtime()
                .unwrap()
                .test_refuse_next_owner_begins_remaining();
            assert!(
                Instant::now() < deadline,
                "accepting-queue hang loops={loops} again={again} begins={begins} refusals={refusals}"
            );
            pump_core(daemon, state);
            if !armed
                && daemon
                    .runtime()
                    .unwrap()
                    .test_release_session_reservation_begins()
                    >= 1
            {
                daemon.runtime().unwrap().test_refuse_next_owner_begins(8);
                armed = true;
            }
            match pending.continuation.poll(daemon, state) {
                ControlPoll::Pending => std::thread::yield_now(),
                ControlPoll::Again => {
                    again += 1;
                    let begins = daemon
                        .runtime()
                        .unwrap()
                        .test_release_session_reservation_begins();
                    let refusals = daemon
                        .runtime()
                        .unwrap()
                        .test_refuse_next_owner_begins_remaining();
                    panic!(
                        "accepting-queue Again while waiting on Core loops={loops} again={again} begins={begins} refusals={refusals}"
                    );
                }
                ControlPoll::Ready(Ok(response)) => break response,
                ControlPoll::Ready(Err(_)) => panic!("spawn transport failed"),
                _ => std::thread::yield_now(),
            }
        };
        let begins = daemon
            .runtime()
            .unwrap()
            .test_release_session_reservation_begins();
        let refusals = daemon
            .runtime()
            .unwrap()
            .test_refuse_next_owner_begins_remaining();
        assert_eq!(
            again, 0,
            "accepting-queue Again loops={loops} again={again} begins={begins} refusals={refusals}"
        );
        assert_eq!(
            begins, 1,
            "accepting-queue release begins loops={loops} again={again} begins={begins} refusals={refusals}"
        );
        assert_eq!(
            refusals, 7,
            "accepting-queue consumed one refused begin loops={loops} again={again} begins={begins} refusals={refusals}"
        );
        assert!(state.retained_explicit_reservations.is_empty());
        assert!(response.error.is_some());
        response
    }

    fn start_accepting_queue(
        label: &str,
        held_id: &str,
        next_id: &str,
    ) -> (
        crate::HubDaemon,
        DaemonControlState,
        std::path::PathBuf,
        crate::daemon::control::pending::PendingStep,
    ) {
        let (mut daemon, mut state, root) = spawn_fixture(label);
        let waiter = state.current_waiter_id.unwrap();
        let mut reserve = daemon
            .runtime()
            .unwrap()
            .begin_reserve_session_for_owner(waiter, SessionId(held_id.into()));
        let held = wait_reservation(&mut daemon, &mut state, &mut reserve);
        daemon.runtime().unwrap().retain_reservation(held);
        let ControlStep::Pending(pending) = handle_runtime(
            &mut daemon,
            &mut state,
            observability(),
            DaemonRequest::Spawn {
                session_id: next_id.into(),
                command: "true".into(),
            },
        ) else {
            panic!("spawn must defer");
        };
        (daemon, state, root, pending)
    }

    #[test]
    fn accepting_queue_releases_a_retained_token_once() {
        let (mut daemon, mut state, root, mut pending) =
            start_accepting_queue("accept-release", "s1-accept-held", "s1-accept-next");
        let _response = drive_accepting_queue(&mut daemon, &mut state, &mut pending);
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .expect("panic message")
    }

    #[test]
    fn accepting_queue_fails_when_retry_returns_again() {
        let (mut daemon, mut state, root, mut pending) = start_accepting_queue(
            "accept-again",
            "s1-accept-again-held",
            "s1-accept-again-next",
        );
        daemon
            .runtime()
            .unwrap()
            .test_set_retry_retained_again_on_pending(true);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drive_accepting_queue(&mut daemon, &mut state, &mut pending);
        }));
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let message = match result {
            Err(payload) => panic_message(payload),
            Ok(()) => panic!("Again ablation must fail"),
        };
        assert!(
            message.contains("Again while waiting on Core"),
            "unexpected panic: {message}"
        );
        assert!(message.contains("again=1"), "unexpected panic: {message}");
    }

    #[test]
    fn accepting_queue_fails_when_release_is_resubmitted() {
        let (mut daemon, mut state, root, mut pending) = start_accepting_queue(
            "accept-resubmit",
            "s1-accept-resubmit-held",
            "s1-accept-resubmit-next",
        );
        daemon
            .runtime()
            .unwrap()
            .test_set_resubmit_release_on_pending(true);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drive_accepting_queue(&mut daemon, &mut state, &mut pending);
        }));
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let message = match result {
            Err(payload) => panic_message(payload),
            Ok(()) => panic!("resubmit ablation must fail"),
        };
        assert!(
            message.contains("release begins") || message.contains("consumed one refused"),
            "unexpected panic: {message}"
        );
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

    fn write_frame_exit_worker(root: &std::path::Path) -> std::path::PathBuf {
        let python = String::from_utf8(
            std::process::Command::new("python3")
                .args(["-c", "import sys; print(sys.executable)"])
                .output()
                .expect("python3 executable")
                .stdout,
        )
        .expect("python path utf8");
        write_worker_script(
            root,
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
        )
    }

    fn matched_worker_path() -> std::path::PathBuf {
        static WORKER: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
        WORKER
            .get_or_init(|| {
                let candidate = |variable| {
                    std::env::var_os(variable)
                        .map(std::path::PathBuf::from)
                        .unwrap_or_else(|| panic!("missing required candidate path in {variable}"))
                };
                let hub = candidate("BOTSTER_HUB_BIN");
                let worker = candidate("BOTSTER_SESSION_WORKER_BIN");
                let manifest = candidate("BOTSTER_CANDIDATE_MANIFEST");
                botster_hub_test_support::verify_candidate_manifest(&manifest, &hub, &worker)
                    .unwrap_or_else(|error| {
                        panic!("candidate manifest verification failed: {error}")
                    });
                worker
            })
            .clone()
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
        let response = spawn_until_ready(&mut daemon, &mut state, "s1-missing-worker", "true");
        assert_eq!(spawn_error_code(&response), Some("spawn_failed"));
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
        let response = spawn_until_ready(&mut daemon, &mut state, "s1-exit-before", "true");
        assert_eq!(spawn_error_code(&response), Some("spawn_failed"));
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
        let first = spawn_until_ready(&mut daemon, &mut state, "s1-frame-exit", "true");
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
        let second = spawn_until_ready(&mut daemon, &mut state, "s1-frame-exit-retry", "true");
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
        let response = spawn_until_ready(&mut daemon, &mut state, "s1-matched-missing", "true");
        assert_eq!(spawn_error_code(&response), Some("spawn_failed"));
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
        let response = spawn_until_ready(&mut daemon, &mut state, "s1-matched-install", "true");
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

    fn request_until_ready(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        request: DaemonRequest,
    ) -> botster_hub_client::DaemonResponse {
        match handle_runtime(daemon, state, observability(), request) {
            ControlStep::Ready(Ok(response)) => response,
            ControlStep::Pending(mut pending) => {
                poll_spawn_until_ready(daemon, state, &mut pending)
            }
            _ => panic!("request must complete"),
        }
    }

    fn remove_when_terminal(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        session_id: &str,
    ) -> botster_hub_client::DaemonResponse {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            // The test pump can clear the owner waiter between requests.
            if state.current_waiter_id.is_none() {
                state.current_waiter_id = state.waiter_ids.next();
            }
            let response = request_until_ready(
                daemon,
                state,
                DaemonRequest::RemoveSession {
                    session_id: session_id.into(),
                },
            );
            if response.kind == DaemonResponseKind::SessionRemoved || Instant::now() >= deadline {
                return response;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    #[test]
    fn removed_ordinary_session_releases_its_reservation_for_reuse() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) =
            spawn_fixture_with_worker("reservation-reuse", Some(worker));
        let first = spawn_until_ready(&mut daemon, &mut state, "s1-reuse", "true");
        assert_eq!(first.kind, DaemonResponseKind::Spawned, "{first:?}");
        assert_eq!(daemon.runtime().unwrap().session_reservations().len(), 1);

        let removed = remove_when_terminal(&mut daemon, &mut state, "s1-reuse");
        assert_eq!(
            removed.kind,
            DaemonResponseKind::SessionRemoved,
            "{removed:?}"
        );
        assert!(removed.diagnostics.is_empty(), "{removed:?}");
        assert_eq!(daemon.runtime().unwrap().session_reservations().len(), 0);
        assert!(state.retained_explicit_reservations.is_empty());

        let reused = spawn_until_ready(&mut daemon, &mut state, "s1-reuse", "true");
        assert_eq!(reused.kind, DaemonResponseKind::Spawned, "{reused:?}");
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_live_session_keeps_its_id_after_a_refused_removal() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) =
            spawn_fixture_with_worker("reservation-live", Some(worker));
        let first = spawn_until_ready(&mut daemon, &mut state, "s1-live", "sleep 30");
        assert_eq!(first.kind, DaemonResponseKind::Spawned, "{first:?}");
        let refused = request_until_ready(
            &mut daemon,
            &mut state,
            DaemonRequest::RemoveSession {
                session_id: "s1-live".into(),
            },
        );
        assert_ne!(refused.kind, DaemonResponseKind::SessionRemoved);
        assert_eq!(daemon.runtime().unwrap().session_reservations().len(), 1);
        let duplicate = spawn_until_ready(&mut daemon, &mut state, "s1-live", "true");
        assert_eq!(spawn_error_code(&duplicate), Some("session_already_exists"));
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_refused_record_charge_is_a_typed_refusal_with_no_reservation() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) =
            spawn_fixture_with_worker("reservation-charge", Some(worker));
        let budget = daemon.runtime().unwrap().state_publication().budget();
        let hold = budget
            .reserve(budget.available())
            .expect("fill the Hub state budget");
        let refused = spawn_until_ready_or_ready(&mut daemon, &mut state, "s1-charge", "true");
        assert_eq!(spawn_error_code(&refused), Some("session_record_capacity"));
        assert_eq!(daemon.runtime().unwrap().session_reservations().len(), 0);
        drop(hold);
        // Nothing was reserved, so the same id spawns once the budget allows.
        let spawned = spawn_until_ready(&mut daemon, &mut state, "s1-charge", "true");
        assert_eq!(spawned.kind, DaemonResponseKind::Spawned, "{spawned:?}");
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn an_existing_record_stops_the_spawn_and_releases_its_new_token() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) =
            spawn_fixture_with_worker("reservation-invariant", Some(worker));
        let runtime = daemon.runtime().unwrap();
        let stray = botster_core::SessionAdmission::default()
            .reserve(botster_core::SessionId("s1-invariant".into()))
            .unwrap();
        runtime
            .session_reservations()
            .register(
                runtime.charge_session_reservation("s1-invariant").unwrap(),
                stray.identity(),
            )
            .unwrap();
        let refused = spawn_until_ready_or_ready(&mut daemon, &mut state, "s1-invariant", "true");
        assert_eq!(spawn_error_code(&refused), Some("session_record_invariant"));
        let runtime = daemon.runtime().unwrap();
        assert!(runtime.session_reservations().capture("s1-invariant") == Some(stray.identity()));
        assert_eq!(runtime.test_release_session_reservation_begins(), 1);
        assert!(state.retained_explicit_reservations.is_empty());
        let sessions = request_until_ready(&mut daemon, &mut state, DaemonRequest::ListSessions);
        assert!(
            sessions
                .sessions
                .iter()
                .all(|session| session.session_id != "s1-invariant")
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    fn spawn_until_ready_or_ready(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        session_id: &str,
        command: &str,
    ) -> botster_hub_client::DaemonResponse {
        request_until_ready(
            daemon,
            state,
            DaemonRequest::Spawn {
                session_id: session_id.into(),
                command: command.into(),
            },
        )
    }

    #[test]
    fn occupied_reserve_fails_without_clearing_the_installed_session() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) = spawn_fixture_with_worker("occupied", Some(worker));
        let first = spawn_until_ready(&mut daemon, &mut state, "s1-occupied", "true");
        assert!(first.error.is_none(), "{first:?}");
        assert_eq!(first.kind, DaemonResponseKind::Spawned);
        let second = spawn_until_ready(&mut daemon, &mut state, "s1-occupied", "true");
        assert!(second.error.is_some(), "{second:?}");
        let message = second
            .error
            .as_ref()
            .map(|error| error.message.to_ascii_lowercase())
            .unwrap_or_default();
        assert!(
            message.contains("occupied"),
            "second Spawn must refuse Occupied: {second:?}"
        );
        assert!(daemon.runtime().unwrap().retained_reservations().is_empty());
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    fn plugin_spawn_package(root: &std::path::Path) -> crate::packages::PackageRecord {
        std::fs::create_dir_all(root.join("bin")).unwrap();
        std::fs::write(root.join("bin/agent"), "#!/bin/sh\nexec true\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                root.join("bin/agent"),
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
        serde_json::from_value(serde_json::json!({
            "manifest": {
                "name": "p1.plugin",
                "version": "1.0.0",
                "kind": "plugin",
                "botster": ">=0.1.0",
                "source": { "type": "path", "path": root.display().to_string() },
                "capabilities": [{"surface": "session_actions", "scope": "session_type_spawn"}],
                "entrypoints": []
            },
            "state": "enabled",
            "classification": "plugin",
            "trust": { "classification": "first_party", "first_party": true },
            "provenance": { "source": root.display().to_string(), "checksum": null },
            "update_policy": "manual",
            "last_audit_reason": "test",
            "session_types": [{
                "id": "agent",
                "label": "Agent",
                "role": "botster.agent",
                "interaction": "interactive",
                "lifecycle": "task",
                "command": "bin/agent",
                "args": []
            }]
        }))
        .expect("plugin spawn package record")
    }

    fn load_plugin_spawn_tool(
        daemon: &mut crate::HubDaemon,
        root: &std::path::Path,
        mut record: crate::packages::PackageRecord,
    ) -> tokio::sync::mpsc::Receiver<crate::daemon::control::message::ControlMessage> {
        std::fs::write(
            root.join("plugin.lua"),
            r#"
return botster.register({
  tools = {{
    name = "p1.spawn",
    description = "Spawn the test session type.",
    handler = "spawn",
    call = function(args)
      return botster.capabilities.session_types.spawn(args)
    end,
  }},
})
"#,
        )
        .expect("write spawn tool");
        record.manifest.capabilities.push(botster_core::Capability {
            surface: botster_core::CapabilitySurface::Mcp,
            scope: None,
        });
        record.manifest.entrypoints = serde_json::from_value(serde_json::json!([
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ]))
        .expect("Lua entrypoint");
        let mut snapshot = crate::packages::PackageRegistrySnapshot::empty();
        snapshot.records.push(record);
        let registry = crate::packages::PackageRegistry::from_snapshot(snapshot)
            .expect("admit spawn tool package");
        // The plugin reads the daemon's committed registry, so publish it
        // through the daemon before the plugin loads.
        daemon
            .replace_package_registry(registry.clone())
            .expect("publish the spawn tool package registry");
        daemon
            .runtime_mut()
            .unwrap()
            .load_lua_plugin_package(&registry, "p1.plugin")
            .expect("load spawn tool");
        let (owner_wake, owner_rx) = tokio::sync::mpsc::channel(64);
        let runtime = daemon.runtime().unwrap();
        runtime.bind_data_plane_owner_wake(owner_wake.clone());
        runtime.bind_host_owner_wake(owner_wake.clone());
        runtime.bind_managed_spawn_owner_wake(owner_wake);
        owner_rx
    }

    fn invoke_plugin_spawn_tool(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        owner_rx: &mut tokio::sync::mpsc::Receiver<crate::daemon::control::message::ControlMessage>,
        session_id: &str,
        target_id: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        invoke_plugin_spawn_tool_with_capacity_hold(
            daemon, state, owner_rx, session_id, target_id, false,
        )
    }

    fn invoke_plugin_spawn_tool_with_capacity_hold(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        owner_rx: &mut tokio::sync::mpsc::Receiver<crate::daemon::control::message::ControlMessage>,
        session_id: &str,
        target_id: Option<&str>,
        hold_remaining_capacity: bool,
    ) -> Result<serde_json::Value, String> {
        let runtime = daemon.runtime().unwrap();
        let mut args = serde_json::json!({
            "session_type_id": "p1.plugin/agent",
            "session_id": session_id,
        });
        if let Some(target_id) = target_id {
            args["target_id"] = serde_json::json!(target_id);
        }
        let request = runtime
            .prepare_plugin_mcp_tool(
                crate::McpCallRequest {
                    name: "p1.spawn".into(),
                    arguments: args,
                },
                botster_core::RequestId(format!("plugin-spawn-{session_id}")),
                None,
            )
            .expect("prepare spawn tool");
        let capacity_hold = hold_remaining_capacity.then(|| {
            let memory = runtime.test_lua_memory();
            let remaining = memory
                .limits()
                .total_callback_bytes
                .checked_sub(memory.usage().1)
                .expect("callback usage stays within the limit");
            memory
                .reserve_shared_callback_storage(remaining)
                .expect("hold remaining capacity after MCP setup")
        });
        let lifecycle = runtime.plugin_lifecycle_handle();
        let outcome = std::thread::scope(|scope| {
            let worker = scope.spawn(move || lifecycle.invoke(request).result);
            let deadline = Instant::now() + Duration::from_secs(35);
            while !worker.is_finished() {
                while owner_rx.try_recv().is_ok() {}
                pump_core(daemon, state);
                assert!(
                    Instant::now() < deadline,
                    "plugin spawn worker did not finish after owner turns; pending_rows={}",
                    state.pending_requests.len()
                );
                std::thread::yield_now();
            }
            worker.join().expect("join plugin spawn worker")
        });
        drop(capacity_hold);
        crate::runtime::HubRuntime::complete_plugin_mcp_tool(outcome).map_err(|error| error.message)
    }

    #[test]
    fn plugin_spawn_capacity_refuses_before_a_new_owner_row() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let worker_root = std::path::PathBuf::from("/private/tmp").join(format!(
            "s1-plugin-capacity-worker-{}-{stamp}",
            std::process::id()
        ));
        let worker = write_frame_exit_worker(&worker_root);
        let (mut daemon, mut state, root) =
            spawn_fixture_with_worker("plugin-capacity", Some(worker));
        let package_root = root.join("p1-plugin");
        let record = plugin_spawn_package(&package_root);
        let mut owner_rx = load_plugin_spawn_tool(&mut daemon, &package_root, record);
        let first = invoke_plugin_spawn_tool(
            &mut daemon,
            &mut state,
            &mut owner_rx,
            "s1-plugin-capacity-held",
            None,
        )
        .expect_err("the first spawn must retain its owner row");
        assert!(first.contains("cleanup_unconfirmed"), "{first}");
        assert_eq!(state.pending_requests.len(), 1);
        let (first_waiter, first_identity) = state
            .pending_requests
            .iter()
            .find_map(|(waiter, entry)| match &entry.continuation {
                crate::daemon::control::pending::ControlContinuation::SessionType(operation) => {
                    operation
                        .test_reservation_identity()
                        .map(|identity| (*waiter, identity))
                }
                _ => None,
            })
            .expect("the first owner row retains its reservation");
        let release_count = daemon
            .runtime()
            .unwrap()
            .test_release_session_reservation_begins();
        let refused = invoke_plugin_spawn_tool_with_capacity_hold(
            &mut daemon,
            &mut state,
            &mut owner_rx,
            "s1-plugin-capacity-refused",
            None,
            true,
        )
        .expect_err("spawn admission must refuse callback capacity");
        assert!(
            refused.contains(crate::lua_memory::LUA_CALLBACK_CAPACITY_EXHAUSTED),
            "spawn admission returned {refused}"
        );
        assert_eq!(state.pending_requests.len(), 1);
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            release_count,
            "capacity refusal must not retry cleanup"
        );
        let waiter = daemon.runtime().unwrap().next_waiter_id().unwrap();
        let mut reserve = daemon.runtime().unwrap().begin_reserve_session_for_owner(
            waiter,
            SessionId("s1-plugin-capacity-refused".into()),
        );
        let reservation = wait_reservation(&mut daemon, &mut state, &mut reserve);
        let mut release = daemon
            .runtime()
            .unwrap()
            .begin_release_session_reservation_for_owner(waiter, reservation);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                Instant::now() < deadline,
                "test reservation release stalled"
            );
            drive_ready_test_turn(&mut daemon, &mut state);
            match release.poll(daemon.runtime().unwrap()) {
                CoreTicketPoll::Pending => std::thread::yield_now(),
                CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                    result: Ok(SessionReservationRelease::Released),
                    ..
                })) => break,
                other => panic!("test reservation release did not confirm: {other:?}"),
            }
        }
        let first_row = state
            .pending_requests
            .get(&first_waiter)
            .expect("capacity refusal must keep the first owner row");
        assert!(first_row.must_finish);
        assert!(first_row.permit.is_some());
        let crate::daemon::control::pending::ControlContinuation::SessionType(operation) =
            &first_row.continuation
        else {
            panic!("the first owner row keeps its session-type stage");
        };
        assert_eq!(operation.test_reservation_identity(), Some(first_identity));
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(package_root);
        let _ = std::fs::remove_dir_all(worker_root);
    }

    #[test]
    fn plugin_spawn_missing_capability_reuses_callback_capacity() {
        let (mut daemon, mut state, root) = spawn_fixture_with_worker("plugin-capability", None);
        let package_root = root.join("p1-plugin");
        let mut record = plugin_spawn_package(&package_root);
        record.manifest.capabilities.clear();
        let mut owner_rx = load_plugin_spawn_tool(&mut daemon, &package_root, record);
        let baseline = daemon.runtime().unwrap().test_lua_memory().usage().1;
        for session_id in ["s1-plugin-capability-a", "s1-plugin-capability-b"] {
            let refused =
                invoke_plugin_spawn_tool(&mut daemon, &mut state, &mut owner_rx, session_id, None)
                    .expect_err("MCP-only package must refuse session-type spawn");
            assert!(
                refused.contains("session_type_spawn capability"),
                "{refused}"
            );
            assert!(state.pending_requests.is_empty());
            assert_eq!(
                daemon
                    .runtime()
                    .unwrap()
                    .test_release_session_reservation_begins(),
                0
            );
            assert_eq!(
                daemon.runtime().unwrap().test_lua_memory().usage().1,
                baseline
            );
        }
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(package_root);
    }

    #[test]
    fn plugin_spawn_requires_the_exact_session_type_spawn_scope() {
        let (mut daemon, mut state, root) = spawn_fixture_with_worker("plugin-unscoped", None);
        let package_root = root.join("p1-plugin");
        let mut record = plugin_spawn_package(&package_root);
        record.manifest.capabilities = vec![botster_core::Capability {
            surface: botster_core::CapabilitySurface::SessionActions,
            scope: None,
        }];
        let mut owner_rx = load_plugin_spawn_tool(&mut daemon, &package_root, record);
        let refused = invoke_plugin_spawn_tool(
            &mut daemon,
            &mut state,
            &mut owner_rx,
            "s1-plugin-unscoped",
            None,
        )
        .expect_err("an unscoped SessionActions grant must not allow session-type spawn");
        assert!(
            refused.contains("session_type_spawn capability"),
            "{refused}"
        );
        assert!(state.pending_requests.is_empty());
        assert!(
            daemon
                .runtime()
                .unwrap()
                .list_sessions()
                .wait(std::time::Duration::from_secs(5))
                .expect("core bridge")
                .expect("list sessions")
                .iter()
                .all(|session| session.session_id.0 != "s1-plugin-unscoped"),
            "a refused plugin call must not spawn a session"
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(package_root);
    }

    /// A client removes the session after Core installs it and before the
    /// plugin conversion completes. The removal only marks the record; the
    /// spawn's handoff then releases the token, and the id is reusable.
    #[test]
    fn removal_before_conversion_releases_the_token_at_handoff() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) =
            spawn_fixture_with_worker("reservation-race", Some(worker));
        let runtime = daemon.runtime().unwrap();
        let waiter = runtime.next_waiter_id().unwrap();
        let parent = runtime.test_lua_memory().reserve_callback_total(0).unwrap();
        let charge = runtime.charge_session_reservation("s1-race").unwrap();
        let mut start = runtime.test_begin_owner_spawn_with_record(
            waiter,
            SessionId("s1-race".into()),
            parent,
            "true",
            Some(charge),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            assert!(Instant::now() < deadline, "owner spawn did not install");
            pump_core(&mut daemon, &mut state);
            match start.poll(daemon.runtime().unwrap()) {
                crate::runtime::PluginSpawnPoll::Ready(Ok(_)) => break,
                crate::runtime::PluginSpawnPoll::Ready(Err(failure)) => {
                    panic!("owner spawn failed: {}", failure.error)
                }
                crate::runtime::PluginSpawnPoll::Pending => std::thread::yield_now(),
            }
        }
        let identity = start.test_reservation_identity().expect("installed token");
        assert!(
            daemon
                .runtime()
                .unwrap()
                .session_reservations()
                .capture("s1-race")
                == Some(identity)
        );

        // The client removal lands before conversion.
        let removed = remove_when_terminal(&mut daemon, &mut state, "s1-race");
        assert_eq!(
            removed.kind,
            DaemonResponseKind::SessionRemoved,
            "{removed:?}"
        );
        assert!(
            daemon
                .runtime()
                .unwrap()
                .session_reservations()
                .removed_during_launch("s1-race", identity)
        );

        // Conversion completes: the handoff releases the token.
        assert!(!start.begin_handoff(daemon.runtime().unwrap()));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(Instant::now() < deadline, "handoff release did not settle");
            pump_core(&mut daemon, &mut state);
            if start.poll_handoff(daemon.runtime().unwrap()) {
                break;
            }
            std::thread::yield_now();
        }
        assert_eq!(daemon.runtime().unwrap().session_reservations().len(), 0);
        assert!(daemon.runtime().unwrap().retained_reservations().is_empty());
        drop(start);

        let reused = spawn_until_ready(&mut daemon, &mut state, "s1-race", "true");
        assert_eq!(reused.kind, DaemonResponseKind::Spawned, "{reused:?}");
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn removed_plugin_session_releases_its_reservation_for_reuse() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) = spawn_fixture_with_worker("plugin-reuse", Some(worker));
        let package_root = root.join("p1-plugin");
        let record = plugin_spawn_package(&package_root);
        let mut owner_rx = load_plugin_spawn_tool(&mut daemon, &package_root, record);
        invoke_plugin_spawn_tool(
            &mut daemon,
            &mut state,
            &mut owner_rx,
            "s1-plugin-reuse",
            None,
        )
        .expect("first plugin spawn");
        assert_eq!(daemon.runtime().unwrap().session_reservations().len(), 1);

        let removed = remove_when_terminal(&mut daemon, &mut state, "s1-plugin-reuse");
        assert_eq!(
            removed.kind,
            DaemonResponseKind::SessionRemoved,
            "{removed:?}"
        );
        assert!(removed.diagnostics.is_empty(), "{removed:?}");
        assert_eq!(daemon.runtime().unwrap().session_reservations().len(), 0);

        invoke_plugin_spawn_tool(
            &mut daemon,
            &mut state,
            &mut owner_rx,
            "s1-plugin-reuse",
            None,
        )
        .expect("the removed plugin session id spawns again");
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(package_root);
    }

    #[test]
    fn plugin_occupied_spawn_leaves_live_session_context() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) = spawn_fixture_with_worker("plugin-occ", Some(worker));
        let installed = spawn_until_ready(&mut daemon, &mut state, "s1-plugin-live", "true");
        assert!(installed.error.is_none(), "{installed:?}");
        let live = crate::session_types::HubSessionContext {
            context_id: "ctx-original".into(),
            session_id: SessionId("s1-plugin-live".into()),
            values: Default::default(),
        };
        daemon.runtime().unwrap().test_publish_spawn_context(&live);
        let package_root = root.join("p1-plugin");
        let record = plugin_spawn_package(&package_root);
        let mut owner_rx = load_plugin_spawn_tool(&mut daemon, &package_root, record);
        let refused = invoke_plugin_spawn_tool(
            &mut daemon,
            &mut state,
            &mut owner_rx,
            "s1-plugin-live",
            None,
        );
        let refused = refused.expect_err("plugin spawn must refuse Occupied");
        assert!(
            refused.to_ascii_lowercase().contains("occupied"),
            "Occupied reservation refusal required, got {refused}"
        );
        let stored = daemon
            .runtime()
            .unwrap()
            .test_session_context("s1-plugin-live")
            .expect("live context remains");
        assert_eq!(stored.context_id, "ctx-original");
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_session_context("ctx-original")
                .map(|context| context.session_id.0),
            Some("s1-plugin-live".into())
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(package_root);
    }

    #[test]
    fn plugin_unconfirmed_spawn_keeps_its_owner_row_across_the_next_spawn() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let worker_root = std::path::PathBuf::from("/private/tmp").join(format!(
            "s1-plugin-retry-worker-{}-{stamp}",
            std::process::id()
        ));
        let worker = write_frame_exit_worker(&worker_root);
        let (mut daemon, mut state, root) = spawn_fixture_with_worker("plugin-retry", Some(worker));
        let package_root = root.join("p1-plugin");
        let record = plugin_spawn_package(&package_root);
        let mut owner_rx = load_plugin_spawn_tool(&mut daemon, &package_root, record);
        let first = invoke_plugin_spawn_tool(
            &mut daemon,
            &mut state,
            &mut owner_rx,
            "s1-plugin-retry-a",
            None,
        );
        let first = first.expect_err("first plugin spawn must report unconfirmed cleanup");
        assert!(
            first.contains("cleanup_unconfirmed"),
            "first plugin spawn must report cleanup_unconfirmed: {first}"
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            1
        );
        assert!(
            daemon.runtime().unwrap().retained_reservations().is_empty(),
            "the owner row holds the reservation, not the legacy token store"
        );
        assert_eq!(state.pending_requests.len(), 1);
        let (first_waiter, first_identity) = state
            .pending_requests
            .iter()
            .find_map(|(waiter, entry)| match &entry.continuation {
                crate::daemon::control::pending::ControlContinuation::SessionType(operation) => {
                    operation
                        .test_reservation_identity()
                        .map(|identity| (*waiter, identity))
                }
                _ => None,
            })
            .expect("the first owner row retains its exact reservation");
        let first_charge = daemon.runtime().unwrap().test_lua_memory().usage().1;
        assert!(first_charge > 0, "the callback account remains charged");
        for _ in 0..4 {
            pump_core(&mut daemon, &mut state);
            let entry = state
                .pending_requests
                .get(&first_waiter)
                .expect("owner turns retain the first row without a new wake");
            assert!(entry.must_finish);
            assert!(entry.permit.is_some());
            let crate::daemon::control::pending::ControlContinuation::SessionType(operation) =
                &entry.continuation
            else {
                panic!("the first row remains a session-type spawn");
            };
            assert_eq!(operation.test_reservation_identity(), Some(first_identity));
        }
        let second = invoke_plugin_spawn_tool(
            &mut daemon,
            &mut state,
            &mut owner_rx,
            "s1-plugin-retry-b",
            None,
        );
        let second = second.expect_err("second plugin spawn must fail");
        assert!(
            second.contains("cleanup_unconfirmed"),
            "second plugin spawn must report cleanup_unconfirmed: {second}"
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            2,
            "the second spawn releases only its own reservation"
        );
        let first_row = state
            .pending_requests
            .get(&first_waiter)
            .expect("the second spawn must not retire the first owner row");
        assert!(first_row.must_finish);
        assert!(first_row.permit.is_some());
        let crate::daemon::control::pending::ControlContinuation::SessionType(operation) =
            &first_row.continuation
        else {
            panic!("the first owner row keeps its session-type stage");
        };
        assert_eq!(operation.test_reservation_identity(), Some(first_identity));
        assert_eq!(state.pending_requests.len(), 2);
        assert!(
            daemon.runtime().unwrap().test_lua_memory().usage().1 >= first_charge,
            "aggregate callback usage must not fall across the second spawn"
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(package_root);
        let _ = std::fs::remove_dir_all(worker_root);
    }

    #[test]
    fn managed_retained_unconfirmed_keeps_a_created_worktree() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let worker_root = std::path::PathBuf::from("/private/tmp").join(format!(
            "s1-managed-retain-worker-{}-{stamp}",
            std::process::id()
        ));
        let worker = write_frame_exit_worker(&worker_root);
        let (mut daemon, mut state, root) =
            spawn_fixture_with_worker("managed-retain", Some(worker));
        let worktree = root.join("created-worktree");
        std::fs::create_dir_all(&worktree).unwrap();
        let marker = worktree.join("keep-me");
        std::fs::write(&marker, b"present").unwrap();
        let package_root = root.join("p1-plugin");
        let mut record = plugin_spawn_package(&package_root);
        record.manifest.capabilities.push(botster_core::Capability {
            surface: botster_core::CapabilitySurface::SessionActions,
            scope: Some("session_type_managed_git_spawn".into()),
        });
        record.session_types[0].target_id = Some("t1".into());
        {
            let runtime = daemon.runtime().unwrap();
            let mut next = (*runtime.state()).clone();
            next.spawn_targets.push(crate::spawn_targets::SpawnTarget {
                target_id: "t1".into(),
                label: "t1".into(),
                root: worktree.clone(),
                enabled: true,
                kind: "directory".into(),
                base_ref: None,
                metadata: Default::default(),
            });
            runtime.replace_state(next).expect("admit spawn target");
        }
        let (response, receiver) = std::sync::mpsc::channel();
        let pending = crate::runtime::PendingManagedSessionSpawn::test_new(
            botster_core::PluginKey("p1.plugin".into()),
            "t1".into(),
            "topic".into(),
            "agent".into(),
            crate::session_types::ManagedSessionTypeRequest::default(),
            vec![record],
            response,
        );
        let prepared = crate::managed_git_worktrees::PreparedManagedWorktree {
            target_id: "t1".into(),
            repository_root: worktree.clone(),
            common_dir: worktree.clone(),
            branch: "topic".into(),
            path: worktree.clone(),
            worktree_id: "wt-keep".into(),
            base_ref: "HEAD".into(),
            base_commit: "0".repeat(40),
            head_commit: "0".repeat(40),
            created_worktree: true,
            created_branch: false,
        };
        let waiter = state.current_waiter_id.unwrap();
        let start = daemon
            .runtime()
            .unwrap()
            .spawn_prepared_managed_session(&pending, &prepared, waiter)
            .expect("start managed spawn");
        let mut operation =
            crate::daemon::control::managed_git::ManagedSpawnOperation::test_spawn_phase(
                waiter, pending, prepared, start,
            );
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            assert!(Instant::now() < deadline, "managed spawn hang");
            pump_core(&mut daemon, &mut state);
            match operation.test_poll_spawn(&mut daemon, &mut state) {
                ControlPoll::Pending => std::thread::yield_now(),
                ControlPoll::Ready(_) => break,
                _ => std::thread::yield_now(),
            }
        }
        let err = receiver
            .try_recv()
            .expect("managed spawn sent a result")
            .expect_err("managed spawn must fail unconfirmed");
        assert_eq!(err.kind, "spawn_failed");
        assert!(
            operation.test_skipped_rollback(),
            "RetainedUnconfirmed must not start worktree rollback"
        );
        assert!(marker.exists(), "created worktree must remain");
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(worker_root);
    }

    #[test]
    fn managed_conversion_abandon_retracts_session_context() {
        let (mut daemon, _state, root) = spawn_fixture("conv-abandon");
        let live = crate::session_types::HubSessionContext {
            context_id: "ctx-abandon".into(),
            session_id: SessionId("s1-abandon".into()),
            values: Default::default(),
        };
        let identity = daemon.runtime().unwrap().test_publish_spawn_context(&live);
        daemon
            .runtime()
            .unwrap()
            .session_type_spawner()
            .abandon_session_type_spawn("s1-abandon".into(), identity);
        daemon.runtime().unwrap().test_fulfill_plugin_spawns();
        assert!(
            daemon
                .runtime()
                .unwrap()
                .test_session_context("s1-abandon")
                .is_none(),
            "abandoned session context must be retracted"
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    fn init_git_repo(path: &std::path::Path) {
        std::fs::create_dir_all(path).unwrap();
        for args in [
            ["init", "-b", "main"].as_slice(),
            ["config", "user.email", "botster@example.invalid"].as_slice(),
            ["config", "user.name", "Botster Test"].as_slice(),
        ] {
            assert!(
                std::process::Command::new("git")
                    .current_dir(path)
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        std::fs::write(path.join("README.md"), "fixture\n").unwrap();
        assert!(
            std::process::Command::new("git")
                .current_dir(path)
                .args(["add", "README.md"])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            std::process::Command::new("git")
                .current_dir(path)
                .args(["commit", "-m", "fixture"])
                .status()
                .unwrap()
                .success()
        );
    }

    fn pump_until_join<T>(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        handle: std::thread::JoinHandle<T>,
    ) -> T {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            pump_core(daemon, state);
            if handle.is_finished() {
                break;
            }
            assert!(Instant::now() < deadline, "ensure_worktree_and_spawn hang");
            std::thread::yield_now();
        }
        handle.join().expect("join managed spawn")
    }

    fn s2_prepare(
        name: &str,
        worker: Option<std::path::PathBuf>,
    ) -> (
        crate::HubDaemon,
        DaemonControlState,
        std::path::PathBuf,
        crate::packages::PackageRecord,
    ) {
        let (daemon, state, root) = spawn_fixture_with_worker(name, worker);
        let repo = root.join("repo");
        init_git_repo(&repo);
        let package_root = root.join("p1-plugin");
        let mut record = plugin_spawn_package(&package_root);
        record.manifest.capabilities.push(botster_core::Capability {
            surface: botster_core::CapabilitySurface::SessionActions,
            scope: Some("session_type_managed_git_spawn".into()),
        });
        record.session_types[0].target_id = Some("t1".into());
        {
            let runtime = daemon.runtime().unwrap();
            let mut next = (*runtime.state()).clone();
            next.spawn_targets.push(crate::spawn_targets::SpawnTarget {
                target_id: "t1".into(),
                label: "t1".into(),
                root: repo,
                enabled: true,
                kind: "git".into(),
                base_ref: Some("main".into()),
                metadata: Default::default(),
            });
            runtime.replace_state(next).expect("admit git target");
        }
        (daemon, state, root, record)
    }

    #[test]
    fn ensure_worktree_and_spawn_creates_a_worktree() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) = spawn_fixture_with_worker("s2-create", Some(worker));
        let repo = root.join("repo");
        init_git_repo(&repo);
        let package_root = root.join("p1-plugin");
        let mut record = plugin_spawn_package(&package_root);
        record.manifest.capabilities.push(botster_core::Capability {
            surface: botster_core::CapabilitySurface::SessionActions,
            scope: Some("session_type_managed_git_spawn".into()),
        });
        record.session_types[0].target_id = Some("t1".into());
        {
            let runtime = daemon.runtime().unwrap();
            let mut next = (*runtime.state()).clone();
            next.spawn_targets.push(crate::spawn_targets::SpawnTarget {
                target_id: "t1".into(),
                label: "t1".into(),
                root: repo.clone(),
                enabled: true,
                kind: "git".into(),
                base_ref: Some("main".into()),
                metadata: Default::default(),
            });
            runtime.replace_state(next).expect("admit git target");
        }
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        let handle = std::thread::spawn(move || {
            spawner.ensure_worktree_and_spawn(
                &botster_core::PluginKey("p1.plugin".into()),
                "t1",
                "topic",
                "agent",
                crate::session_types::ManagedSessionTypeRequest::default(),
                crate::runtime::package_view_for_test(vec![record]),
            )
        });
        let spawned = pump_until_join(&mut daemon, &mut state, handle).expect("created spawn");
        assert!(spawned.created_worktree);
        assert!(!spawned.reused_worktree);
        assert!(std::path::Path::new(&spawned.worktree_path).exists());
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_created_worktree_cleanup_count(),
            0,
            "delivered spawn must not keep a cleanup marker"
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_confirmed_worktree_rollback_count(),
            0
        );
        assert!(
            hub_worktree_ids(&daemon).contains(&spawned.worktree_id),
            "delivered spawn must persist the HubState worktree row"
        );
        // The delivered managed session's token belongs to its record, and an
        // authoritative removal releases it.
        assert!(
            daemon
                .runtime()
                .unwrap()
                .session_reservations()
                .capture(&spawned.session_id)
                .is_some()
        );
        let removed = remove_when_terminal(&mut daemon, &mut state, &spawned.session_id);
        assert_eq!(
            removed.kind,
            DaemonResponseKind::SessionRemoved,
            "{removed:?}"
        );
        assert!(removed.diagnostics.is_empty(), "{removed:?}");
        assert!(
            daemon
                .runtime()
                .unwrap()
                .session_reservations()
                .capture(&spawned.session_id)
                .is_none()
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_worktree_and_spawn_reuses_an_existing_worktree() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) = spawn_fixture_with_worker("s2-reuse", Some(worker));
        let repo = root.join("repo");
        init_git_repo(&repo);
        let package_root = root.join("p1-plugin");
        let mut record = plugin_spawn_package(&package_root);
        record.manifest.capabilities.push(botster_core::Capability {
            surface: botster_core::CapabilitySurface::SessionActions,
            scope: Some("session_type_managed_git_spawn".into()),
        });
        record.session_types[0].target_id = Some("t1".into());
        {
            let runtime = daemon.runtime().unwrap();
            let mut next = (*runtime.state()).clone();
            next.spawn_targets.push(crate::spawn_targets::SpawnTarget {
                target_id: "t1".into(),
                label: "t1".into(),
                root: repo.clone(),
                enabled: true,
                kind: "git".into(),
                base_ref: Some("main".into()),
                metadata: Default::default(),
            });
            runtime.replace_state(next).expect("admit git target");
        }
        let first_record = record.clone();
        let third_record = record.clone();
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        let first_spawner = spawner.clone();
        let first = std::thread::spawn(move || {
            first_spawner.ensure_worktree_and_spawn(
                &botster_core::PluginKey("p1.plugin".into()),
                "t1",
                "topic",
                "agent",
                crate::session_types::ManagedSessionTypeRequest::default(),
                crate::runtime::package_view_for_test(vec![first_record]),
            )
        });
        let first = pump_until_join(&mut daemon, &mut state, first).expect("first create");
        assert!(first.created_worktree);
        let path = first.worktree_path.clone();
        let second = std::thread::spawn(move || {
            spawner.ensure_worktree_and_spawn(
                &botster_core::PluginKey("p1.plugin".into()),
                "t1",
                "topic",
                "agent",
                crate::session_types::ManagedSessionTypeRequest::default(),
                crate::runtime::package_view_for_test(vec![record]),
            )
        });
        let second = pump_until_join(&mut daemon, &mut state, second).expect("reuse spawn");
        assert!(second.reused_worktree);
        assert!(!second.created_worktree);
        assert_eq!(second.worktree_path, path);
        assert!(std::path::Path::new(&path).exists());
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .session_reservations()
                .installed_len(),
            2
        );

        // A reused-worktree spawn whose caller left has no created-worktree
        // cleanup; its token still moves to its record, the sole release owner.
        daemon
            .runtime()
            .unwrap()
            .session_type_spawner()
            .test_enqueue_managed_disconnected(
                botster_core::PluginKey("p1.plugin".into()),
                "t1".into(),
                "topic".into(),
                "agent".into(),
                crate::session_types::ManagedSessionTypeRequest::default(),
                vec![third_record],
            );
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            pump_core(&mut daemon, &mut state);
            if daemon
                .runtime()
                .unwrap()
                .session_reservations()
                .installed_len()
                == 3
                && state.pending_requests.is_empty()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "undelivered reused spawn must hand its token to a record; records={}",
                daemon.runtime().unwrap().session_reservations().len()
            );
            std::thread::yield_now();
        }
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_created_worktree_cleanup_count(),
            0
        );
        assert!(
            std::path::Path::new(&path).exists(),
            "a reused worktree is never removed"
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_worktree_and_spawn_serial_same_branch_reuses_without_damage() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root, record) = s2_prepare("s2-conflict", Some(worker));
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        let a_spawner = spawner.clone();
        let a_record = record.clone();
        let a = std::thread::spawn(move || {
            a_spawner.ensure_worktree_and_spawn(
                &botster_core::PluginKey("p1.plugin".into()),
                "t1",
                "topic",
                "agent",
                crate::session_types::ManagedSessionTypeRequest::default(),
                crate::runtime::package_view_for_test(vec![a_record]),
            )
        });
        let b = std::thread::spawn(move || {
            spawner.ensure_worktree_and_spawn(
                &botster_core::PluginKey("p1.plugin".into()),
                "t1",
                "topic",
                "agent",
                crate::session_types::ManagedSessionTypeRequest::default(),
                crate::runtime::package_view_for_test(vec![record]),
            )
        });
        let overlap = Instant::now() + Duration::from_secs(5);
        while daemon
            .runtime()
            .unwrap()
            .session_type_spawner()
            .test_managed_queue_len()
            < 2
        {
            assert!(Instant::now() < overlap, "both managed spawns must queue");
            std::thread::yield_now();
        }
        pump_core(&mut daemon, &mut state);
        daemon
            .runtime()
            .unwrap()
            .session_type_spawner()
            .test_publish_managed_spawn();
        pump_core(&mut daemon, &mut state);
        let deadline = Instant::now() + Duration::from_secs(20);
        while !(a.is_finished() && b.is_finished()) {
            pump_core(&mut daemon, &mut state);
            assert!(Instant::now() < deadline, "concurrent managed spawn hang");
            std::thread::yield_now();
        }
        let a = a.join().expect("join a").expect("first serial spawn");
        let b = b.join().expect("join b").expect("second serial spawn");
        assert_ne!(a.session_id, b.session_id);
        assert_eq!(a.worktree_id, b.worktree_id);
        assert_eq!(a.worktree_path, b.worktree_path);
        assert_eq!(
            usize::from(a.created_worktree) + usize::from(b.created_worktree),
            1
        );
        assert_eq!(
            usize::from(a.reused_worktree) + usize::from(b.reused_worktree),
            1
        );
        assert!(
            std::path::Path::new(&a.worktree_path).exists(),
            "serial reuse must keep the worktree: {}",
            a.worktree_path
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_worktree_and_spawn_retains_unconfirmed_created_worktree() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let worker_root = std::path::PathBuf::from("/private/tmp").join(format!(
            "s2-unconfirmed-worker-{}-{stamp}",
            std::process::id()
        ));
        let worker = write_frame_exit_worker(&worker_root);
        let (mut daemon, mut state, root, record) = s2_prepare("s2-unconfirmed", Some(worker));
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        let handle = std::thread::spawn(move || {
            spawner.ensure_worktree_and_spawn(
                &botster_core::PluginKey("p1.plugin".into()),
                "t1",
                "topic",
                "agent",
                crate::session_types::ManagedSessionTypeRequest::default(),
                crate::runtime::package_view_for_test(vec![record]),
            )
        });
        let err = pump_until_join(&mut daemon, &mut state, handle)
            .expect_err("frame-exit must fail unconfirmed");
        assert_eq!(err.kind, "spawn_failed");
        let managed = root.join("managed-worktrees");
        let found = walkdir_exists(&managed);
        assert!(found, "created worktree must remain under {managed:?}");
        assert!(
            !daemon.runtime().unwrap().retained_reservations().is_empty(),
            "RetainedUnconfirmed must keep the reservation marker"
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_created_worktree_cleanup_count(),
            0,
            "unconfirmed spawn must not queue Host rollback"
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(worker_root);
    }

    fn walkdir_exists(root: &std::path::Path) -> bool {
        git_worktree_present(root)
    }

    fn git_worktree_present(root: &std::path::Path) -> bool {
        let Ok(entries) = std::fs::read_dir(root) else {
            return false;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.join(".git").exists() {
                return true;
            }
            if path.is_dir() && git_worktree_present(&path) {
                return true;
            }
        }
        false
    }

    #[test]
    fn ensure_worktree_and_spawn_late_caller_does_not_rollback() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root, record) = s2_prepare("s2-late", Some(worker));
        daemon
            .runtime()
            .unwrap()
            .session_type_spawner()
            .test_enqueue_managed_disconnected(
                botster_core::PluginKey("p1.plugin".into()),
                "t1".into(),
                "topic".into(),
                "agent".into(),
                crate::session_types::ManagedSessionTypeRequest::default(),
                vec![record],
            );
        let deadline = Instant::now() + Duration::from_secs(20);
        let managed = root.join("managed-worktrees");
        loop {
            pump_core(&mut daemon, &mut state);
            if walkdir_exists(&managed) {
                break;
            }
            assert!(Instant::now() < deadline, "late caller spawn hang");
            std::thread::yield_now();
        }
        assert!(
            walkdir_exists(&managed),
            "disconnected caller must not roll back before confirmed shutdown"
        );
        let gone = Instant::now() + Duration::from_secs(20);
        loop {
            pump_core(&mut daemon, &mut state);
            if !walkdir_exists(&managed) {
                break;
            }
            let runtime = daemon.runtime().unwrap();
            assert!(
                Instant::now() < gone,
                "created worktree must roll back after Released then Host FinalizeRollback; cleanups={} confirmed={} releases={}",
                runtime.test_created_worktree_cleanup_count(),
                runtime.test_confirmed_worktree_rollback_count(),
                runtime.test_release_session_reservation_begins()
            );
            std::thread::yield_now();
        }
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_created_worktree_cleanup_count(),
            0
        );
        let record_gone = Instant::now() + Duration::from_secs(10);
        loop {
            pump_core(&mut daemon, &mut state);
            if hub_worktree_ids(&daemon).is_empty() {
                break;
            }
            assert!(
                Instant::now() < record_gone,
                "Released Host rollback must commit worktree record removal; rows={:?}",
                hub_worktree_ids(&daemon)
            );
            std::thread::yield_now();
        }
        // The created-worktree cleanup owned the token and retired its record
        // on its confirmed release.
        assert_eq!(daemon.runtime().unwrap().session_reservations().len(), 0);
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_worktree_and_spawn_failure_leaves_unrelated_worktree() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root, record) = s2_prepare("s2-unrelated", Some(worker));
        let first_record = record.clone();
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        let first_spawner = spawner.clone();
        let first = std::thread::spawn(move || {
            first_spawner.ensure_worktree_and_spawn(
                &botster_core::PluginKey("p1.plugin".into()),
                "t1",
                "keep",
                "agent",
                crate::session_types::ManagedSessionTypeRequest::default(),
                crate::runtime::package_view_for_test(vec![first_record]),
            )
        });
        let first = pump_until_join(&mut daemon, &mut state, first).expect("keep worktree");
        let keep = first.worktree_path.clone();
        let live = crate::session_types::HubSessionContext {
            context_id: format!("ctx-{}", first.session_id),
            session_id: SessionId(first.session_id.clone()),
            values: Default::default(),
        };
        daemon.runtime().unwrap().test_publish_spawn_context(&live);
        let package_root = root.join("p1-plugin");
        let mut owner_rx = load_plugin_spawn_tool(&mut daemon, &package_root, record);
        let refused = invoke_plugin_spawn_tool(
            &mut daemon,
            &mut state,
            &mut owner_rx,
            &first.session_id,
            Some("t1"),
        );
        let refused = refused.expect_err("Occupied must refuse");
        assert!(
            refused.to_ascii_lowercase().contains("occupied"),
            "{refused}"
        );
        assert!(
            std::path::Path::new(&keep).exists(),
            "unrelated worktree must survive"
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_session_context(&format!("ctx-{}", first.session_id))
                .map(|context| context.session_id.0),
            Some(first.session_id.clone())
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_worktree_and_spawn_reuse_after_undelivered_keeps_the_created_worktree() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root, record) = s2_prepare("s2-reuse-late", Some(worker));
        let second_record = record.clone();
        daemon
            .runtime()
            .unwrap()
            .session_type_spawner()
            .test_enqueue_managed_disconnected(
                botster_core::PluginKey("p1.plugin".into()),
                "t1".into(),
                "topic".into(),
                "agent".into(),
                crate::session_types::ManagedSessionTypeRequest::default(),
                vec![record],
            );
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        let second_reply = spawner.test_enqueue_managed_with_reply(
            botster_core::PluginKey("p1.plugin".into()),
            "t1".into(),
            "topic".into(),
            "agent".into(),
            crate::session_types::ManagedSessionTypeRequest::default(),
            vec![second_record],
        );
        assert_eq!(spawner.test_managed_queue_len(), 2);
        let deadline = Instant::now() + Duration::from_secs(20);
        let second = loop {
            pump_core(&mut daemon, &mut state);
            match second_reply.try_recv() {
                Ok(result) => break result.expect("reuse spawn"),
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    assert!(Instant::now() < deadline, "queued reuse spawn hang");
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("queued reuse reply disconnected")
                }
            }
            std::thread::yield_now();
        };
        assert!(second.reused_worktree);
        assert!(!second.created_worktree);
        assert!(
            std::path::Path::new(&second.worktree_path).exists(),
            "reused worktree must exist: {}",
            second.worktree_path
        );
        let hold = Instant::now() + Duration::from_secs(2);
        while Instant::now() < hold {
            pump_core(&mut daemon, &mut state);
            assert!(
                std::path::Path::new(&second.worktree_path).exists(),
                "Released after reuse must not roll back the surviving worktree"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_confirmed_worktree_rollback_count(),
            0,
            "reuse must drain identity-matched rollback before Host FinalizeRollback"
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_worktree_and_spawn_undelivered_reuse_rolls_back_original_creation() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root, record) =
            s2_prepare("s2-reuse-undelivered", Some(worker));
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        for package_records in [vec![record.clone()], vec![record]] {
            spawner.test_enqueue_managed_disconnected(
                botster_core::PluginKey("p1.plugin".into()),
                "t1".into(),
                "topic".into(),
                "agent".into(),
                crate::session_types::ManagedSessionTypeRequest::default(),
                package_records,
            );
        }
        assert_eq!(spawner.test_managed_queue_len(), 2);
        let managed = root.join("managed-worktrees");
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            pump_core(&mut daemon, &mut state);
            if daemon
                .runtime()
                .unwrap()
                .test_inherited_managed_cleanup_transfers()
                == 1
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "reuse did not inherit the original cleanup"
            );
            std::thread::yield_now();
        }
        let gone = Instant::now() + Duration::from_secs(20);
        loop {
            pump_core(&mut daemon, &mut state);
            let runtime = daemon.runtime().unwrap();
            if !walkdir_exists(&managed)
                && hub_worktree_ids(&daemon).is_empty()
                && runtime.test_created_worktree_cleanup_count() == 0
                && runtime.test_confirmed_worktree_rollback_count() == 0
            {
                break;
            }
            assert!(
                Instant::now() < gone,
                "undelivered reuse did not roll back the original worktree"
            );
            std::thread::yield_now();
        }
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_worktree_and_spawn_refused_reuse_preserves_original_cleanup() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root, record) = s2_prepare("s2-reuse-refused", Some(worker));
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        spawner.test_enqueue_managed_disconnected(
            botster_core::PluginKey("p1.plugin".into()),
            "t1".into(),
            "topic".into(),
            "agent".into(),
            crate::session_types::ManagedSessionTypeRequest::default(),
            vec![record],
        );
        let refused = spawner.test_enqueue_managed_with_reply(
            botster_core::PluginKey("p1.plugin".into()),
            "t1".into(),
            "topic".into(),
            "agent".into(),
            crate::session_types::ManagedSessionTypeRequest::default(),
            Vec::new(),
        );
        let managed = root.join("managed-worktrees");
        let deadline = Instant::now() + Duration::from_secs(20);
        let refusal = loop {
            pump_core(&mut daemon, &mut state);
            match refused.try_recv() {
                Ok(result) => break result.expect_err("missing package must refuse reuse"),
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    assert!(Instant::now() < deadline, "reuse refusal did not complete");
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("reuse refusal reply disconnected")
                }
            }
            std::thread::yield_now();
        };
        assert_ne!(refusal.kind, "spawn_failed");
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_inherited_managed_cleanup_transfers(),
            0
        );
        let gone = Instant::now() + Duration::from_secs(20);
        loop {
            pump_core(&mut daemon, &mut state);
            if !walkdir_exists(&managed) && hub_worktree_ids(&daemon).is_empty() {
                break;
            }
            assert!(
                Instant::now() < gone,
                "refused reuse lost the original cleanup"
            );
            std::thread::yield_now();
        }
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn managed_terminal_drain_retains_inherited_creation() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root, _record) =
            s2_prepare("s2-terminal-inherited", Some(worker));
        let runtime = daemon.runtime().unwrap();
        let waiter = runtime.next_waiter_id().unwrap();
        let permit = runtime.host_executor().try_reserve().unwrap();
        let prepared = crate::managed_git_worktrees::PreparedManagedWorktree {
            target_id: "t1".into(),
            repository_root: root.join("repo"),
            common_dir: root.join("repo/.git"),
            branch: "topic".into(),
            path: root.join("managed-worktrees/topic"),
            worktree_id: "wt-terminal-inherited".into(),
            base_ref: "main".into(),
            base_commit: "0".repeat(40),
            head_commit: "0".repeat(40),
            created_worktree: true,
            created_branch: true,
        };
        let operation =
            crate::daemon::control::managed_git::ManagedSpawnOperation::test_inherited_terminal(
                waiter, prepared, permit,
            );
        insert_phase_test_row(
            &mut state,
            waiter,
            crate::daemon::control::pending::ControlContinuation::ManagedSpawn(Box::new(operation)),
        );
        crate::daemon::control::pending::dispose_terminal_requests(runtime, &mut state);
        assert_eq!(
            runtime.test_confirmed_worktree_rollback_count(),
            1,
            "terminal disposal must retain the original creation right"
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while !state.pending_requests.is_empty() {
            crate::daemon::control::pending::dispose_terminal_requests(runtime, &mut state);
            assert!(
                Instant::now() < deadline,
                "managed terminal disposal did not finish"
            );
            std::thread::yield_now();
        }
        assert_eq!(runtime.test_confirmed_worktree_rollback_count(), 1);
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    fn hub_worktree_ids(daemon: &crate::HubDaemon) -> Vec<String> {
        daemon
            .runtime()
            .unwrap()
            .state()
            .worktrees
            .iter()
            .map(|worktree| worktree.worktree_id.clone())
            .collect()
    }

    #[test]
    fn ensure_worktree_and_spawn_stale_rollback_does_not_remove_a_live_worktree() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root, record) = s2_prepare("s2-stale", Some(worker));
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        let handle = std::thread::spawn(move || {
            spawner.ensure_worktree_and_spawn(
                &botster_core::PluginKey("p1.plugin".into()),
                "t1",
                "topic",
                "agent",
                crate::session_types::ManagedSessionTypeRequest::default(),
                crate::runtime::package_view_for_test(vec![record]),
            )
        });
        let spawned = pump_until_join(&mut daemon, &mut state, handle).expect("live spawn");
        let live_id = spawned.worktree_id.clone();
        let live_path = spawned.worktree_path.clone();
        let stale = crate::managed_git_worktrees::PreparedManagedWorktree {
            target_id: "t1".into(),
            repository_root: root.join("repo"),
            common_dir: root.join("repo"),
            branch: "stale".into(),
            path: root.join("missing-stale-worktree"),
            worktree_id: "wt-stale".into(),
            base_ref: "HEAD".into(),
            base_commit: "0".repeat(40),
            head_commit: "0".repeat(40),
            created_worktree: true,
            created_branch: false,
        };
        daemon
            .runtime()
            .unwrap()
            .defer_confirmed_worktree_rollback(stale);
        daemon
            .runtime()
            .unwrap()
            .session_type_spawner()
            .test_publish_managed_spawn();
        let hold = Instant::now() + Duration::from_secs(2);
        while Instant::now() < hold {
            pump_core(&mut daemon, &mut state);
            std::thread::yield_now();
        }
        assert!(
            std::path::Path::new(&live_path).exists(),
            "stale rollback must not remove the live worktree"
        );
        assert!(
            hub_worktree_ids(&daemon).contains(&live_id),
            "stale rollback must not remove the live HubState row"
        );
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn retry_created_worktree_releases_does_not_gate_on_advisory_state() {
        let source = include_str!("../../runtime.rs");
        let start = source
            .find("pub(crate) fn retry_created_worktree_releases")
            .expect("retry_created_worktree_releases");
        let body = source[start..]
            .split("fn advance_created_worktree_cleanups")
            .next()
            .expect("retry body");
        assert!(
            !body.contains("reservation.state()"),
            "U-1: explicit-event retry must not gate on advisory state(): {body}"
        );
        assert!(
            body.contains("begin_release_session_reservation"),
            "explicit-event retry must issue a new release"
        );
    }

    fn wait_spawn_installed(
        daemon: &mut crate::HubDaemon,
        state: &mut DaemonControlState,
        tracker: &mut crate::runtime::CoreOperationTracker,
    ) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(Instant::now() < deadline, "spawn reserved did not install");
            pump_core(daemon, state);
            match tracker.poll(daemon.runtime().unwrap()) {
                CoreTicketPoll::Pending => std::thread::yield_now(),
                CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                    result: ReservedSpawnResult::Installed { .. },
                    ..
                })) => return,
                other => panic!("spawn reserved must install, got {other:?}"),
            }
        }
    }

    #[test]
    fn created_worktree_cleanup_retries_release_on_a_live_session() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let worker = matched_worker_path();
        let (mut daemon, mut state, root) = spawn_fixture_with_worker("s2-u1-live", Some(worker));
        let waiter = state.current_waiter_id.unwrap();
        let session_id = SessionId(format!("s2-u1-live-{stamp}"));
        let mut reserve = daemon
            .runtime()
            .unwrap()
            .begin_reserve_session_for_owner(waiter, session_id.clone());
        let reservation = wait_reservation(&mut daemon, &mut state, &mut reserve);
        let spawn = SpawnSessionRequest {
            request: crate::client_api::spawn_request(
                daemon.runtime().unwrap(),
                crate::daemon::control::request_id("s2-u1-live"),
                session_id.clone(),
                "sleep 30".into(),
            ),
            metadata: crate::client_api::client_session_metadata(),
        };
        let mut spawn = daemon.runtime().unwrap().begin_spawn_reserved_for_owner(
            waiter,
            reservation.clone(),
            spawn,
        );
        wait_spawn_installed(&mut daemon, &mut state, &mut spawn);
        let prepared = crate::managed_git_worktrees::PreparedManagedWorktree {
            target_id: "t1".into(),
            repository_root: root.clone(),
            common_dir: root.clone(),
            branch: "topic".into(),
            path: root.join("u1-live-worktree"),
            worktree_id: "wt-u1-live".into(),
            base_ref: "HEAD".into(),
            base_commit: "0".repeat(40),
            head_commit: "0".repeat(40),
            created_worktree: true,
            created_branch: false,
        };
        daemon
            .runtime()
            .unwrap()
            .test_queue_removed_created_worktree_cleanup(session_id.clone(), prepared, reservation);
        daemon.runtime().unwrap().retry_created_worktree_releases();
        let first = Instant::now() + Duration::from_secs(5);
        loop {
            pump_core(&mut daemon, &mut state);
            if daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins()
                >= 1
                && daemon
                    .runtime()
                    .unwrap()
                    .test_created_worktree_cleanup_count()
                    == 1
                && daemon
                    .runtime()
                    .unwrap()
                    .test_created_worktree_cleanup_release_idle()
            {
                break;
            }
            assert!(Instant::now() < first, "first live release must complete");
            std::thread::yield_now();
        }
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            1
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_confirmed_worktree_rollback_count(),
            0,
            "live session must not Host-rollback after RetainedSession"
        );
        daemon.runtime().unwrap().retry_created_worktree_releases();
        let second = Instant::now() + Duration::from_secs(5);
        loop {
            pump_core(&mut daemon, &mut state);
            if daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins()
                >= 2
            {
                break;
            }
            assert!(
                Instant::now() < second,
                "explicit event must issue a second release"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_release_session_reservation_begins(),
            2
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_confirmed_worktree_rollback_count(),
            0
        );
        let mut shutdown = daemon
            .runtime()
            .unwrap()
            .begin_shutdown_session(session_id.clone());
        let stop = Instant::now() + Duration::from_secs(10);
        loop {
            pump_core(&mut daemon, &mut state);
            match shutdown.poll(daemon.runtime().unwrap()) {
                CoreTicketPoll::Pending => {}
                CoreTicketPoll::Ready(_) | CoreTicketPoll::Lost | CoreTicketPoll::Refused => break,
            }
            assert!(Instant::now() < stop, "shutdown hang");
            std::thread::yield_now();
        }
        let mut remove = daemon.runtime().unwrap().begin_remove_session(&session_id);
        loop {
            pump_core(&mut daemon, &mut state);
            match remove.poll(daemon.runtime().unwrap()) {
                CoreTicketPoll::Pending => {}
                CoreTicketPoll::Ready(_) | CoreTicketPoll::Lost | CoreTicketPoll::Refused => break,
            }
            assert!(Instant::now() < stop, "remove hang");
            std::thread::yield_now();
        }
        daemon.runtime().unwrap().retry_created_worktree_releases();
        let released = Instant::now() + Duration::from_secs(10);
        loop {
            pump_core(&mut daemon, &mut state);
            if daemon
                .runtime()
                .unwrap()
                .test_confirmed_worktree_rollback_count()
                >= 1
            {
                break;
            }
            assert!(
                Instant::now() < released,
                "Released after shutdown must queue Host rollback"
            );
            std::thread::yield_now();
        }
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn accept_confirmed_rollback_waits_for_owner_capacity() {
        let (mut daemon, mut state, root) = spawn_fixture("s2-capacity-wait");
        let mut held = Vec::new();
        while let Some(permit) = state.budget.reserve() {
            held.push(permit);
        }
        let prepared = crate::managed_git_worktrees::PreparedManagedWorktree {
            target_id: "t1".into(),
            repository_root: root.clone(),
            common_dir: root.clone(),
            branch: "topic".into(),
            path: root.join("missing-capacity-worktree"),
            worktree_id: "wt-capacity".into(),
            base_ref: "HEAD".into(),
            base_commit: "0".repeat(40),
            head_commit: "0".repeat(40),
            created_worktree: true,
            created_branch: false,
        };
        daemon
            .runtime()
            .unwrap()
            .defer_confirmed_worktree_rollback(prepared);
        assert!(
            !daemon.runtime().unwrap().take_managed_spawn_notification(),
            "capacity defer must not self-wake"
        );
        accept_one(&mut daemon, &mut state);
        assert!(state.managed_spawn_waiting_for_owner);
        assert_eq!(daemon.runtime().unwrap().test_managed_accept_ones(), 1);
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .test_confirmed_worktree_rollback_count(),
            1
        );
        for _ in 0..8 {
            publish_completion_wakes(&daemon, &mut state);
            drive_ready_test_turn(&mut daemon, &mut state);
        }
        assert_eq!(
            daemon.runtime().unwrap().test_managed_accept_ones(),
            1,
            "accept_one must stay bounded while owner capacity is exhausted"
        );
        state.budget.release(held.pop().expect("held permit"));
        publish_completion_wakes(&daemon, &mut state);
        drive_ready_test_turn(&mut daemon, &mut state);
        assert!(
            daemon.runtime().unwrap().test_managed_accept_ones() >= 2,
            "budget release must wake the waiting rollback"
        );
        assert!(!state.managed_spawn_waiting_for_owner);
        for permit in held {
            state.budget.release(permit);
        }
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_worktree_and_spawn_defers_reuse_until_in_flight_rollback_finishes() {
        let worker = matched_worker_path();
        let (mut daemon, mut state, root, record) = s2_prepare("s2-hold-rollback", Some(worker));
        let second_record = record.clone();
        daemon
            .runtime()
            .unwrap()
            .session_type_spawner()
            .test_enqueue_managed_disconnected(
                botster_core::PluginKey("p1.plugin".into()),
                "t1".into(),
                "topic".into(),
                "agent".into(),
                crate::session_types::ManagedSessionTypeRequest::default(),
                vec![record],
            );
        let gate = Arc::new(TestHostGate::default());
        daemon
            .runtime()
            .unwrap()
            .test_install_rollback_git_hold(Arc::clone(&gate));
        let managed = root.join("managed-worktrees");
        let started = Instant::now() + Duration::from_secs(20);
        loop {
            pump_core(&mut daemon, &mut state);
            if gate.has_started() {
                break;
            }
            assert!(
                Instant::now() < started,
                "Host rollback must reach the pre-git hold"
            );
            std::thread::yield_now();
        }
        assert!(
            walkdir_exists(&managed),
            "held rollback must not have deleted the worktree yet"
        );
        let spawner = daemon.runtime().unwrap().session_type_spawner();
        let handle = std::thread::spawn(move || {
            spawner.ensure_worktree_and_spawn(
                &botster_core::PluginKey("p1.plugin".into()),
                "t1",
                "topic",
                "agent",
                crate::session_types::ManagedSessionTypeRequest::default(),
                crate::runtime::package_view_for_test(vec![second_record]),
            )
        });
        let wait_reuse = Instant::now() + Duration::from_secs(1);
        while Instant::now() < wait_reuse {
            pump_core(&mut daemon, &mut state);
            assert!(
                !handle.is_finished(),
                "reuse must defer while rollback is in flight"
            );
            std::thread::yield_now();
        }
        gate.release();
        let second = pump_until_join(&mut daemon, &mut state, handle).unwrap_or_else(|error| {
            panic!("reuse after rollback: {}: {}", error.kind, error.message)
        });
        assert!(
            second.created_worktree,
            "reuse after an in-flight rollback must create, not reuse a path being deleted"
        );
        assert!(!second.reused_worktree);
        daemon.stop();
        let _ = std::fs::remove_dir_all(root);
    }
}
