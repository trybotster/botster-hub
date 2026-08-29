//! Host update and daemon-shutdown request family.

use std::sync::mpsc;

use botster_hub_client::{
    DaemonDiagnostic, DaemonHubUpdate, DaemonHubUpdateScope, DaemonHubUpdateState, DaemonRequest,
    DaemonResponse, DaemonResponseKind,
};
use serde_json::Value;

use crate::HubDaemon;
use crate::client_api_dto::response::{daemon_hub_update, daemon_hub_update_execution};
use crate::daemon::control::DaemonObservability;
use crate::daemon::control::message::{ControlReplySender, ControlSender};
use crate::daemon::error::{
    DaemonTransportError, DaemonTransportResult, hub_update_execution_error,
};
use crate::daemon::owner_loop::{
    DaemonControlState, send_control_response, wait_for_response_delivery,
};
use crate::daemon_projection::daemon_status_from_status;
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
    observability: DaemonObservability<'_>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let status = daemon.status();
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };
    match request {
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
        _ => unreachable!("host runtime family received a non-host request"),
    }
}
