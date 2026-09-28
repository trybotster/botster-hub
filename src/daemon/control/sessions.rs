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
use botster_core_daemon::{CaptureId, CaptureOwner, CoreCompletion, CoreDaemonError};
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
use crate::client_api_dto::response::{
    daemon_events, daemon_response_base, daemon_session_cleanup, daemon_session_context,
    daemon_spawned, daemon_terminal_reservation, daemon_unknown_session_cleanup,
};
use crate::client_api_dto::session::lifecycle_label;
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::DaemonTransportError;
use crate::daemon::owner_budget::{
    CoreWorkPoll, OWNER_BUDGET_EXHAUSTED, ObligationPoll, drive_core_slot,
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
/// generation, the peer's owner admission, and a live reservation for the same
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
    if !state.budget.peer_admitted(&grant_id) {
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
        crate::admission::reservations::TerminalReservationRequest {
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            peer_generation,
            now_seconds: now_seconds(),
            owner: owner.clone(),
            identity: identity.clone(),
            route,
        },
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
        CoreDaemonError::Engine(botster_core::DefaultBotsterEngineError::NotSubscribed {
            ..
        }) => "not_attached",
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

fn session_credential_error(
    request_id: &str,
    error: crate::session_credential::CredentialUnavailable,
) -> DaemonResponse {
    let mut response = core_operator_error("spawn", request_id, &CoreDaemonError::Shutdown);
    set_spawn_error(
        &mut response,
        Some("credential_unavailable"),
        error.to_string(),
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

#[expect(
    clippy::result_large_err,
    reason = "Ready carries the flat all-optional DaemonResponse (about 6 KB) inline; the client-protocol DaemonResponse redesign replaces it instead of boxing every Ready site"
)]
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
    let spawn = match crate::client_api::credentialed_raw_spawn(
        runtime,
        id.clone(),
        SessionId(session_id.clone()),
        command,
        crate::session_credential::os_entropy,
    ) {
        Ok(spawn) => spawn,
        Err(error) => return ControlStep::ready(session_credential_error(&id.0, error)),
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
            let mut recording: Option<(
                Box<super::restart_records::RecordWrite>,
                botster_hub_client::DaemonResponse,
            )> = None;
            ControlStep::pending(move |daemon, state| {
                if let Some((write, _)) = recording.as_mut() {
                    if !write.poll(daemon, state, waiter_id, &session_id) {
                        return ControlPoll::Pending;
                    }
                    let (_, response) = recording.take().expect("recording was checked");
                    return ControlPoll::Ready(Ok(response));
                }
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
                    return super::restart_records::finish_removed_session(
                        daemon,
                        state,
                        waiter_id,
                        &session_id,
                        response,
                        &mut recording,
                    );
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
                        super::restart_records::finish_removed_session(
                            daemon,
                            state,
                            waiter_id,
                            &session_id,
                            daemon_response_base(DaemonResponseKind::SessionRemoved),
                            &mut recording,
                        )
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
                move |_, state, waiter_id| {
                    retain_operation_retirement(state, waiter_id, retire_tracker)
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
                move |_, state, waiter_id| {
                    retain_operation_retirement(state, waiter_id, retire_tracker)
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
            // it produced, kept as an obligation until Core accepted that.
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
                move |_, state, waiter_id| {
                    retain_operation_retirement(state, waiter_id, retire_tracker)
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
/// touched. The obligation stays until Core accepts the release. One ticket is in flight; a refused
/// admission resubmits on the next owner turn; a lost driver ends the work.
pub(crate) fn retain_exact_detach(
    state: &mut DaemonControlState,
    client_id: String,
    session_id: String,
    subscription_id: String,
    generation: TerminalSubscriptionGeneration,
) {
    let waiter_id = state
        .waiter_ids
        .next()
        .expect("a cleanup obligation must have an available waiter identifier");
    let mut slot: Option<CoreTicket<Result<(), CoreDaemonError>>> = None;
    crate::daemon::owner_budget::retain_owner_obligation(
        state,
        waiter_id,
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
/// and release a capture the completion produced. The obligation stays
/// until Core accepted the last of those.
fn retain_operation_retirement(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
    tracker: std::sync::Arc<std::sync::Mutex<CoreOperationTracker>>,
) {
    let mut cancel_slot: Option<CoreTicket<bool>> = None;
    let mut release_slot: Option<CoreTicket<bool>> = None;
    let mut cancel_requested = false;
    let mut release_capture: Option<CaptureId> = None;
    crate::daemon::owner_budget::retain_owner_obligation(
        state,
        waiter_id,
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
        // A route can only be attached by an admitted peer, so peer cleanup
        // (the only place the admission ends) is the only owner a WebRTC
        // route can be left with.
        let peer_is_admitted = owner
            .grant_id
            .as_deref()
            .is_some_and(|grant_id| state.budget.peer_admitted(grant_id));
        if !peer_is_admitted {
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
    // Reserve the route key before any Core work exists for this attach;
    // it is released on every failure path.
    let reservation = reserve_attach_route(pending_runtime, &owner, &session_id, &subscription_id);
    if reservation == RouteReservation::Full {
        return ControlStep::ready(attach_route_limit_error());
    }
    if !state.admits_work() {
        release_failed_attach_route(
            &mut state.pending_runtime,
            &owner,
            &session_id,
            &subscription_id,
            reservation,
        );
        return ControlStep::ready(owner_budget_error());
    }
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
#[path = "sessions_tests.rs"]
mod tests;
