//! Host update and daemon-shutdown request family.

use std::sync::mpsc;

use botster_hub_client::{
    DaemonDiagnostic, DaemonHubUpdate, DaemonHubUpdateScope, DaemonHubUpdateState, DaemonRequest,
    DaemonResponseKind, DaemonRetentionAccounting,
};

use crate::HubDaemon;
use crate::client_api_dto::response::{
    daemon_hub_update, daemon_hub_update_execution, daemon_response_base,
};
use crate::daemon::control::DaemonObservability;
use crate::daemon::control::message::{ControlReplySender, ControlSender};
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::error::{DaemonTransportError, hub_update_execution_error};
use crate::daemon::owner_loop::{
    DaemonControlState, send_control_response, wait_for_response_delivery,
};
use crate::daemon_projection::daemon_status_from_status;
use crate::data_plane::driver::CoreTicketPoll;
use crate::maintenance::{
    HubUpdateCheckPlan, execute_managed_update_check, installation_identity, plan_hub_update_check,
    software_identity,
};
use crate::source_update::{current_update_execution, mark_update_failed, start_update_handoff};

pub(crate) fn handle_request(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    request: &DaemonRequest,
    reply_tx: ControlReplySender,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> Option<bool> {
    match request {
        DaemonRequest::CheckHubUpdate => Some(check_hub_update(
            state,
            transport_handle,
            control_tx,
            reply_tx,
            response_delivery_rx,
        )),
        DaemonRequest::StartHubUpdate { scope } => Some(start_hub_update(
            daemon,
            *scope,
            reply_tx,
            response_delivery_rx,
        )),
        DaemonRequest::GetHubUpdateExecution => Some(get_hub_update_execution(
            daemon,
            reply_tx,
            response_delivery_rx,
        )),
        _ => None,
    }
}

fn check_hub_update(
    state: &mut DaemonControlState,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    reply_tx: ControlReplySender,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    match plan_hub_update_check() {
        HubUpdateCheckPlan::Immediate(update) => send_control_response(
            reply_tx,
            Ok(daemon_hub_update(update)),
            response_delivery_rx,
        ),
        HubUpdateCheckPlan::Managed(_check) if state.pending_hub_update_reply.is_some() => {
            send_control_response(
                reply_tx,
                Ok(daemon_hub_update(DaemonHubUpdate {
                    state: DaemonHubUpdateState::Unavailable,
                    current_version: software_identity().version,
                    available_version: None,
                    build_revision: None,
                    reason: Some("busy".to_string()),
                    action: Some("retry".to_string()),
                })),
                response_delivery_rx,
            )
        }
        HubUpdateCheckPlan::Managed(check) => {
            state.pending_hub_update_reply = Some(reply_tx);
            let completion_tx = control_tx.clone();
            transport_handle.spawn_blocking(move || {
                let update = execute_managed_update_check(check);
                let _ = completion_tx.blocking_send(
                    crate::daemon::control::message::ControlMessage::HubUpdateCheckCompleted {
                        update,
                    },
                );
            });
            false
        }
    }
}

fn start_hub_update(
    daemon: &HubDaemon,
    scope: DaemonHubUpdateScope,
    reply_tx: ControlReplySender,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    let data_directory = match daemon.runtime() {
        Some(runtime) => runtime.config().data_directory.clone(),
        None => {
            return send_control_response(
                reply_tx,
                Ok(hub_update_execution_error(
                    "hub_update_runtime_unavailable",
                    "start_hub_update",
                    "the Hub runtime is not available",
                )),
                response_delivery_rx,
            );
        }
    };
    match start_update_handoff(&data_directory, scope) {
        Ok((execution, handoff)) => {
            let update_id = execution.update_id.clone();
            let response_received = reply_tx
                .send(Ok(daemon_hub_update_execution(execution)))
                .is_ok();
            wait_for_response_delivery(response_received, response_received, response_delivery_rx);
            if response_received {
                if let Err(error) = handoff.release() {
                    let _ = mark_update_failed(&data_directory, &update_id, &error);
                }
            } else {
                handoff.stop();
                let _ = mark_update_failed(
                    &data_directory,
                    &update_id,
                    "client disconnected before update handoff",
                );
            }
            false
        }
        Err(error) => send_control_response(
            reply_tx,
            Ok(hub_update_execution_error(
                if error.contains("already active") {
                    "hub_update_busy"
                } else {
                    "hub_update_start_failed"
                },
                "start_hub_update",
                &error,
            )),
            response_delivery_rx,
        ),
    }
}

fn get_hub_update_execution(
    daemon: &HubDaemon,
    reply_tx: ControlReplySender,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    let response = match daemon.runtime() {
        Some(runtime) => match current_update_execution(&runtime.config().data_directory) {
            Ok(Some(execution)) => daemon_hub_update_execution(execution),
            Ok(None) => hub_update_execution_error(
                "hub_update_execution_not_found",
                "get_hub_update_execution",
                "no Hub update execution record exists",
            ),
            Err(error) => hub_update_execution_error(
                "hub_update_execution_read_failed",
                "get_hub_update_execution",
                &error,
            ),
        },
        None => hub_update_execution_error(
            "hub_update_runtime_unavailable",
            "get_hub_update_execution",
            "the Hub runtime is not available",
        ),
    };
    send_control_response(reply_tx, Ok(response), response_delivery_rx)
}

pub(crate) fn hub_update_check_completed(
    state: &mut DaemonControlState,
    update: DaemonHubUpdate,
) -> bool {
    state
        .pending_hub_update_reply
        .take()
        .is_some_and(|reply_tx| {
            send_control_response(reply_tx, Ok(daemon_hub_update(update)), None)
        })
}

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    request: DaemonRequest,
) -> ControlStep {
    let status = daemon.status();
    let Some(runtime) = daemon.runtime() else {
        return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
    };
    match request {
        DaemonRequest::DaemonShutdown => {
            let Some(permit) = runtime.host_executor().try_reserve() else {
                return ControlStep::ready(crate::daemon::control::attach_bind_operator_error(
                    "host_executor_full",
                    "the host executor has no shutdown slot; retry after capacity is released",
                ));
            };
            let waiter_id = state.current_waiter_id.expect("owner waiter is assigned");
            state.shutdown_waiter = Some(waiter_id);
            let policy = runtime.retention_policy();
            let mut ticket =
                runtime.submit_core_for_owner(waiter_id, |daemon| daemon.retention_accounting());
            let session_count = state.maintenance.projection.rows.len();
            let lifecycle = observability.lifecycle.clone();
            let mut response = None;
            let mut permit = Some(permit);
            let mut stop_submitted = false;
            let mut entity_cancel_after = None;
            ControlStep::pending(move |daemon, state| {
                let Some(runtime) = daemon.runtime() else {
                    state.shutdown_waiter = None;
                    return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
                };
                if response.is_none() {
                    let accounting = match ticket.poll() {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Lost | CoreTicketPoll::Refused => None,
                        CoreTicketPoll::Ready(accounting) => Some(accounting),
                    };
                    let mut prepared_response = daemon_response_base(DaemonResponseKind::Shutdown);
                    prepared_response.status = Some(daemon_status_from_status(
                        &status,
                        session_count,
                        Vec::new(),
                        lifecycle.clone(),
                        software_identity(),
                        installation_identity(),
                        runtime.event_plane_counters_snapshot(),
                        accounting.map(|accounting| DaemonRetentionAccounting {
                            max_object_bytes: policy.max_object_bytes as u64,
                            max_total_bytes: policy.max_total_bytes as u64,
                            max_sessions: u32::try_from(policy.max_sessions).unwrap_or(u32::MAX),
                            total_bytes: accounting.total_bytes as u64,
                            sessions: u32::try_from(accounting.sessions).unwrap_or(u32::MAX),
                            evictions: accounting.evictions,
                        }),
                    ));
                    prepared_response.diagnostics = vec![DaemonDiagnostic::connected("shutdown")];

                    response = Some(prepared_response);
                }
                if !stop_submitted {
                    if crate::daemon::control::entities::cancel_next_plugin_entity_for_shutdown(
                        state,
                        &mut entity_cancel_after,
                    ) {
                        return ControlPoll::Again;
                    }
                    if crate::daemon::control::entities::plugin_entity_cleanup_pending(state) {
                        return ControlPoll::Pending;
                    }
                    if runtime.event_plane_owner_ops_pending() {
                        return ControlPoll::Pending;
                    }
                    // The dispatcher removes this waiter while it runs this continuation.
                    if !state.pending_requests.is_empty() {
                        return ControlPoll::Pending;
                    }
                    match runtime.host_executor().submit(
                        crate::host_executor::HostJobIdentity {
                            waiter_id,
                            phase: 1,
                        },
                        crate::host_executor::HostCommand::StopEntrypoints,
                        permit.take().expect("shutdown retains its host slot"),
                    ) {
                        Ok(()) => {
                            stop_submitted = true;
                            return ControlPoll::Pending;
                        }
                        Err(_) => {
                            state.shutdown_waiter = None;
                            return ControlPoll::Ready(Err(DaemonTransportError::Protocol(
                                "host shutdown submission failed",
                            )));
                        }
                    }
                }
                let Some(completion) = state.host_completions.remove(&waiter_id) else {
                    return ControlPoll::Pending;
                };
                let (_, result, permit) = completion.into_parts();
                drop(permit);
                state.shutdown_waiter = None;
                match result {
                    crate::host_executor::HostResult::EntrypointsStopped => {
                        ControlPoll::Ready(Ok(response
                            .take()
                            .expect("shutdown response was prepared")))
                    }
                    _ => ControlPoll::Ready(Err(DaemonTransportError::Protocol(
                        "host entrypoint shutdown failed",
                    ))),
                }
            })
        }
        _ => unreachable!("host runtime family received a non-host request"),
    }
}
