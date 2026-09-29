//! `RestartSession`: start an ended session-type session again under its id.
//!
//! One pending owner step. It checks the session against the projection and
//! the durable restart record, has Core release the ended session in place,
//! then runs the ordinary session-type spawn with the same session id from
//! the record. That spawn resolves the CURRENT session type and mints a new
//! session token. A failed spawn leaves the row ended and the record kept, so
//! the client can ask again.
//!
//! Every refusal is typed and none is a retry hint. The readiness signal for a
//! client is the session entity's `restartable` flag, not a timer.

use botster_core::SessionId;
use botster_hub_client::DaemonRequest;

use crate::client_api::HubClientApi;
use crate::client_api_dto::response::daemon_spawned;
use crate::client_api_dto::session::daemon_session_from_client;
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::DaemonControlState;
use crate::restart_records::RestartRefusal;
use crate::runtime::CoreOperationTracker;
use crate::{
    HubClientError, HubClientOperation, HubClientRequest, HubClientResponseBody, HubDaemon,
};

fn refuse(kind: &'static str, message: String) -> ControlStep {
    ControlStep::Ready(Err(DaemonTransportError::Client(
        HubClientError::SessionType {
            request_id: request_id("daemon-session-restart"),
            operation: HubClientOperation::SpawnSessionType,
            kind,
            message,
        },
    )))
}

enum Stage {
    Releasing(CoreOperationTracker),
    Spawning(Box<crate::client_api::HubClientPending>),
}

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    request: DaemonRequest,
) -> ControlStep {
    let DaemonRequest::RestartSession { session_id } = request else {
        unreachable!("only RestartSession reaches the restart handler")
    };
    let projection = &state.maintenance.projection;
    if !projection.rows.contains_key(&session_id) {
        return refuse(
            "unknown_session",
            format!("session {session_id} is not known to this Hub"),
        );
    }
    if !projection.is_ended(&session_id) {
        return refuse(
            "restart_not_ended",
            format!(
                "session {session_id} has not ended; a running, stopping or indeterminate session cannot be restarted"
            ),
        );
    }
    let view = daemon.state_view().1;
    let Some(record) = view.restart_records.get(&session_id) else {
        return refuse(
            "restart_record_unavailable",
            format!(
                "session {session_id} has no durable restart record, so it is not restartable now"
            ),
        );
    };
    let session_type_id = record.session_type_id.clone();
    let daemon_request = match record.to_request(SessionId(session_id.clone())) {
        Ok(request) => request,
        Err(RestartRefusal::EnvironmentNotRetained { keys }) => {
            return refuse(
                "restart_environment_not_retained",
                format!(
                    "session {session_id} was spawned with client environment values that are not kept: {}",
                    keys.join(", ")
                ),
            );
        }
    };
    drop(view);

    let api = HubClientApi::local_operator(observability.client_id.clone().unwrap_or_else(|| {
        super::runtime_client_id(&DaemonRequest::RestartSession {
            session_id: session_id.clone(),
        })
    }));
    let packages = daemon.package_registry_view();
    let waiter_id = state.current_waiter_id.expect("owner waiter is assigned");
    let Some(runtime) = daemon.runtime() else {
        return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
    };
    let Some(release) = begin_release(runtime, waiter_id, &session_id) else {
        return refuse(
            "restart_not_ready",
            format!("Core cannot release session {session_id} yet"),
        );
    };
    let mut stage = Stage::Releasing(release);
    let mut spawn_request = Some(daemon_request);
    ControlStep::pending(move |daemon, state| {
        loop {
            match &mut stage {
                Stage::Releasing(tracker) => {
                    let Some(runtime) = daemon.runtime() else {
                        return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
                    };
                    let completion = match tracker.poll(runtime) {
                        crate::data_plane::driver::CoreTicketPoll::Pending => {
                            return ControlPoll::Pending;
                        }
                        crate::data_plane::driver::CoreTicketPoll::Ready(Ok(completion)) => {
                            completion
                        }
                        _ => {
                            return ControlPoll::Ready(refuse_now(
                                "restart_not_ready",
                                format!("Core did not release session {session_id}"),
                            ));
                        }
                    };
                    if !released(&completion) {
                        // The previous run's process group is still alive.
                        // Core fires nothing when it exits, so this is a
                        // typed refusal and never a wait.
                        return ControlPoll::Ready(refuse_now(
                            "restart_not_ready",
                            format!(
                                "session {session_id} still has a live process group from its previous run"
                            ),
                        ));
                    }
                    let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
                    let Some(runtime) = daemon.runtime_mut() else {
                        return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
                    };
                    let session_type_request = spawn_request
                        .take()
                        .expect("the spawn request is used once");
                    let step = api.handle_request_for_owner(
                        runtime,
                        &packages,
                        HubClientRequest::SpawnSessionType {
                            request_id: request_id("daemon-session-restart-spawn"),
                            session_type_id: session_type_id.clone(),
                            session_type_request,
                            now_seconds: now,
                        },
                        waiter_id,
                    );
                    match step {
                        crate::client_api::HubClientStep::Ready(result) => {
                            return ControlPoll::Ready(spawned(result));
                        }
                        crate::client_api::HubClientStep::Pending(pending) => {
                            stage = Stage::Spawning(Box::new(pending));
                        }
                    }
                }
                Stage::Spawning(pending) => {
                    let Some(runtime) = daemon.runtime() else {
                        return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
                    };
                    let Some(result) = pending.poll(runtime) else {
                        return ControlPoll::Pending;
                    };
                    return ControlPoll::Ready(spawned(result));
                }
            }
        }
    })
}

/// Start Core's in-place release of the ended session. It keeps the registry
/// row and journals no removal, so the entity never disappears.
fn begin_release(
    runtime: &crate::HubRuntime,
    waiter_id: crate::owner_identity::WaiterId,
    session_id: &str,
) -> Option<CoreOperationTracker> {
    Some(
        runtime.begin_release_ended_session_for_owner(waiter_id, SessionId(session_id.to_string())),
    )
}

/// True when Core released the row (`Ok(true)`), so the id can be reserved.
/// `Ok(false)` with the row ended means the previous run's process group is
/// still alive.
fn released(completion: &botster_core_daemon::CoreCompletion) -> bool {
    matches!(
        completion,
        botster_core_daemon::CoreCompletion::ReleaseEndedSession {
            result: Ok(true),
            ..
        }
    )
}

fn refuse_now(
    kind: &'static str,
    message: String,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    match refuse(kind, message) {
        ControlStep::Ready(result) => result,
        _ => unreachable!("a refusal is ready"),
    }
}

fn spawned(
    result: Result<crate::HubClientResponse, HubClientError>,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let response = result.map_err(DaemonTransportError::Client)?;
    let HubClientResponseBody::Spawned(spawned) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(daemon_spawned(
        daemon_session_from_client(spawned.session),
        super::events::events_from_client(spawned.events),
    ))
}
