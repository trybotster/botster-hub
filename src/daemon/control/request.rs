//! ControlMessage::Request owner: live-peer gate, family dispatch, deferral,
//! and post-processing.

use std::sync::mpsc;
use std::time::Instant;

use botster_hub_client::{
    DaemonHubUpdate, DaemonHubUpdateState, DaemonRequest, DaemonResponse, DaemonResponseKind,
};

use crate::HubDaemon;
use crate::client_api_dto::response::daemon_hub_update;
use crate::daemon::control::message::{ControlMessage, ControlReplySender, ControlSender};
use crate::daemon::control::pending::{ControlStep, PendingControlRequest, poll_pending_requests};
use crate::daemon::control::{
    DaemonObservability, control_request_operation_label, events, handle_control_request, host,
    webrtc,
};
use crate::daemon::error::{
    DaemonTransportError, DaemonTransportResult, daemon_entrypoint_error,
    daemon_local_webrtc_error, daemon_operator_error, daemon_package_compensation_error,
    daemon_package_error, daemon_snapshot_stream_forbidden_error, daemon_spawn_target_error,
    daemon_state_error, daemon_worktree_error,
};
use crate::daemon::owner_loop::{
    DaemonControlState, request_succeeded, send_control_response, should_mark_pump_after_control,
};
use crate::maintenance::software_identity;
use crate::subscription::attach_routes::{
    attached_subscription_change_for_response, record_attached_subscription_change,
};

pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    message: ControlMessage,
) -> bool {
    let ControlMessage::Request {
        request,
        reply_tx,
        response_delivery_rx,
        grant_id,
        client_id,
        enqueued_at,
    } = message
    else {
        unreachable!("request owner received a non-request control message");
    };
    if let Some(runtime) = daemon.runtime() {
        runtime.event_plane_counters().record_ready_operation_wait(
            u64::try_from(enqueued_at.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
    }
    // Late WebRTC Requests after PeerClosed must not create durable ownership or run
    // stale control against a gone peer. Socket path leaves grant_id = None.
    if let Some(grant_id) = grant_id.as_deref()
        && !daemon.local_webrtc().has_live_peer(grant_id)
    {
        let operation = control_request_operation_label(request.as_ref());
        return send_control_response(
            reply_tx,
            Ok(webrtc::local_webrtc_peer_gone_request_error(operation)),
            response_delivery_rx,
        );
    }
    if matches!(
        request.as_ref(),
        DaemonRequest::SubscribeEvents { .. } | DaemonRequest::UnsubscribeEvents { .. }
    ) {
        let connection_id = grant_id
            .clone()
            .or_else(|| client_id.clone())
            .unwrap_or_default();
        let response = events::handle_client_event_request(
            daemon,
            state,
            &connection_id,
            request.as_ref().clone(),
        );
        return send_control_response(reply_tx, Ok(response), response_delivery_rx);
    }
    if matches!(
        request.as_ref(),
        DaemonRequest::CheckHubUpdate
            | DaemonRequest::StartHubUpdate { .. }
            | DaemonRequest::GetHubUpdateExecution
    ) {
        return host::handle_request(
            daemon,
            state,
            transport_handle,
            control_tx.clone(),
            request.as_ref(),
            reply_tx,
            response_delivery_rx,
        )
        .expect("host family");
    }
    let request = *request;
    let step = handle_control_request(
        daemon,
        state,
        DaemonObservability {
            egress: state.egress_diagnostics.diagnostics(),
            lifecycle: state.lifecycle_counters.clone(),
            client_id: client_id.clone(),
            grant_id: grant_id.clone(),
        },
        control_tx,
        request.clone(),
    );
    let entry = PendingControlRequest {
        request,
        reply_tx,
        response_delivery_rx,
        grant_id,
        client_id,
        accepted_at: Instant::now(),
        continuation: Box::new(|_, _| crate::daemon::control::pending::ControlPoll::Pending),
    };
    match step {
        ControlStep::Ready(response) => finish(daemon, state, entry, response),
        ControlStep::Pending(continuation) => {
            state.pending_requests.push(PendingControlRequest {
                continuation,
                ..entry
            });
            false
        }
    }
}

/// Poll deferred requests and answer the finished ones.
pub(crate) fn poll_deferred(daemon: &mut HubDaemon, state: &mut DaemonControlState) -> bool {
    poll_pending_requests(daemon, state, finish)
}

/// Post-process one complete response and send it. Returns `true` after a
/// `shutdown` response.
fn finish(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    entry: PendingControlRequest,
    response: DaemonTransportResult<DaemonResponse>,
) -> bool {
    let PendingControlRequest {
        request,
        reply_tx,
        response_delivery_rx,
        grant_id,
        ..
    } = entry;
    let reconcile_after_request = matches!(
        request,
        DaemonRequest::Spawn { .. }
            | DaemonRequest::Attach { .. }
            | DaemonRequest::ShutdownSession { .. }
            | DaemonRequest::RemoveSession { .. }
    );
    let response = response.or_else(|error| match error {
        DaemonTransportError::Client(error) => Ok(daemon_operator_error(error)),
        DaemonTransportError::Package(error) => Ok(daemon_package_error(error)),
        DaemonTransportError::SpawnTarget(error) => Ok(daemon_spawn_target_error(error)),
        DaemonTransportError::Worktree(error) => Ok(daemon_worktree_error(error)),
        DaemonTransportError::State(error) => Ok(daemon_state_error(error)),
        DaemonTransportError::Entrypoint(error) => Ok(daemon_entrypoint_error(error)),
        DaemonTransportError::LocalWebrtc(error) => Ok(daemon_local_webrtc_error(error)),
        error @ DaemonTransportError::PackageCompensation { .. } => {
            Ok(daemon_package_compensation_error(error))
        }
        error @ DaemonTransportError::SnapshotStreamForbidden { .. } => {
            Ok(daemon_snapshot_stream_forbidden_error(error))
        }
        error => Err(error),
    });
    if matches!(request, DaemonRequest::Detach { .. })
        && response
            .as_ref()
            .is_ok_and(|response| response.kind != DaemonResponseKind::OperatorError)
    {
        *state
            .lifecycle_counters
            .cleanup_by_reason
            .entry("explicit_detach".to_string())
            .or_insert(0) += 1;
    }
    if let DaemonRequest::ShutdownSession { session_id } = &request
        && response
            .as_ref()
            .is_ok_and(|response| response.kind == DaemonResponseKind::OperatorError)
    {
        let host_closed = state
            .pending_runtime
            .live_attach_routes
            .iter()
            .filter(|(bound_session, subscription_id)| {
                bound_session == session_id
                    && !state
                        .pending_runtime
                        .is_adapter_bound(bound_session, subscription_id)
            })
            .count();
        if host_closed > 0 {
            *state
                .lifecycle_counters
                .cleanup_by_reason
                .entry("shutdown_error_host_close".to_string())
                .or_insert(0) += host_closed as u64;
        }
    }
    if let Ok(response) = response.as_ref() {
        let change = attached_subscription_change_for_response(&request, response);
        record_attached_subscription_change(
            &mut state.pending_runtime,
            &mut state.attach_close,
            &mut state.lifecycle_counters,
            change,
            grant_id.as_deref(),
        );
    }
    let succeeded = request_succeeded(response.as_ref());
    if succeeded {
        if let DaemonRequest::Spawn { session_id, .. } = &request {
            state
                .maintenance
                .acknowledged_spawn_ids
                .insert(session_id.clone());
            if let Some(runtime) = daemon.runtime() {
                runtime.record_acknowledged_spawn(session_id.clone());
            }
        }
        if reconcile_after_request {
            state.maintenance.note_authoritative_mutation();
        } else if matches!(request, DaemonRequest::PluginSurfaceAction { .. })
            && daemon
                .runtime()
                .is_some_and(crate::HubRuntime::package_entity_work_pending)
        {
            state.maintenance.try_wake();
        }
    }
    if should_mark_pump_after_control(&request, succeeded) {
        state.background.mark_pump();
    }
    if daemon.runtime().is_some_and(|runtime| {
        runtime.package_event_router().peek_delivery_wake()
            || runtime.event_plane_owner_ops_pending()
            || runtime.package_entity_work_pending()
            || runtime.package_entity_resync_still_needed()
    }) {
        state.maintenance.try_wake();
    }
    if response
        .as_ref()
        .is_ok_and(|response| response.kind == DaemonResponseKind::Shutdown)
        && let Some(update_reply_tx) = state.pending_hub_update_reply.take()
    {
        let _ = send_control_response(
            update_reply_tx,
            Ok(daemon_hub_update(DaemonHubUpdate {
                state: DaemonHubUpdateState::Unavailable,
                current_version: software_identity().version,
                available_version: None,
                build_revision: None,
                reason: Some("daemon_shutdown".to_string()),
                action: Some("retry".to_string()),
            })),
            None,
        );
    }
    // Reply first so surface-action publish can return before fanout delivery.
    // Attach writes `attaching` before Core attach work. Bound adapters then
    // carry terminal frames without a later host control pulse.
    // Authoritative mutations already set one coalesced wake. Status and
    // other reads must not force an extra owner-loop slice.
    send_control_response(reply_tx, response, response_delivery_rx)
}

#[allow(dead_code)]
fn reply_sender_type_check(_: ControlReplySender, _: Option<mpsc::Receiver<()>>) {}
