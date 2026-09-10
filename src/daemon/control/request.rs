//! ControlMessage::Request owner: live-peer gate, family dispatch, deferral,
//! and post-processing.

use std::sync::mpsc;
use std::time::Instant;

use botster_hub_client::{
    DaemonHubUpdate, DaemonHubUpdateState, DaemonRequest, DaemonResponseKind,
};

use crate::HubDaemon;
use crate::client_api_dto::response::daemon_hub_update;
use crate::daemon::control::attach_bind_operator_error;
use crate::daemon::control::message::{ControlMessage, ControlReplySender, ControlSender};
use crate::daemon::control::pending::{
    ControlStep, OwnerRequestCompletion, PendingControlRequest, READY_DEADLINE, READY_INITIAL,
    mark_owner_ready, poll_ready_request_item, request_must_finish,
};
use crate::daemon::control::reply::ControlReply;
use crate::daemon::control::{
    DaemonObservability, control_request_operation_label, events, handle_control_request, host,
    webrtc,
};
use crate::daemon::error::{
    DaemonTransportError, daemon_entrypoint_error, daemon_local_webrtc_error,
    daemon_operator_error, daemon_package_compensation_error, daemon_package_error,
    daemon_snapshot_stream_forbidden_error, daemon_spawn_target_error, daemon_state_error,
    daemon_worktree_error,
};
use crate::daemon::owner_budget::OWNER_BUDGET_EXHAUSTED;
use crate::daemon::owner_loop::{
    DaemonControlState, request_succeeded, send_control_reply, send_control_response,
};
use crate::daemon::owner_schedule::ReadyClass;
use crate::maintenance::software_identity;
use crate::subscription::attach_routes::{
    AttachedSubscriptionChange, record_attached_subscription_change,
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
        transport_request_id,
        reply_tx,
        response_delivery_rx,
        grant_id,
        client_id,
        enqueued_at,
    } = message
    else {
        unreachable!("request owner received a non-request control message");
    };
    if state.shutdown_waiter.is_some() {
        return send_control_response(
            reply_tx,
            Ok(attach_bind_operator_error(
                "daemon_shutting_down",
                "the daemon is finishing accepted requests before shutdown",
            )),
            response_delivery_rx,
        );
    }
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
    // Reserve the budget permit before any Core work is admitted. The permit
    // stays with the pending entry until the response is finished or the
    // entry is retired; the transport's per-connection limit ends with the
    // connection, this one does not.
    let client = grant_id.clone().or_else(|| client_id.clone());
    let Some(permit) = state.budget.reserve() else {
        return send_control_response(
            reply_tx,
            Ok(attach_bind_operator_error(
                OWNER_BUDGET_EXHAUSTED,
                "the daemon holds its maximum retained requests and cleanup; retry later",
            )),
            response_delivery_rx,
        );
    };
    let Some(waiter_id) = state.waiter_ids.next() else {
        state.budget.release(permit);
        return send_control_response(
            reply_tx,
            Ok(attach_bind_operator_error(
                OWNER_BUDGET_EXHAUSTED,
                "the daemon exhausted unique owner waiter identifiers",
            )),
            response_delivery_rx,
        );
    };
    let observability = if matches!(
        request,
        DaemonRequest::Status | DaemonRequest::DaemonShutdown
    ) {
        // Status captures diagnostics only after its original Host permit is admitted.
        DaemonObservability {
            egress: Vec::new(),
            lifecycle: Default::default(),
            client_id: None,
            grant_id: None,
            transport_request_id,
        }
    } else {
        DaemonObservability {
            egress: state.egress_diagnostics.diagnostics(),
            lifecycle: state.lifecycle_counters.clone(),
            client_id: client_id.clone(),
            grant_id: grant_id.clone(),
            transport_request_id,
        }
    };
    let must_finish = request_must_finish(&request);
    let completion = OwnerRequestCompletion::from_request(&request);
    state.current_waiter_id = Some(waiter_id);
    let step = handle_control_request(daemon, state, observability, control_tx, request);
    state.current_waiter_id = None;
    let entry = PendingControlRequest {
        waiter_id,
        ready_class: ReadyClass::CoreCompletion,
        ready_key: None,
        deadline_key: None,
        last_core_phase: 0,
        last_host_phase: 0,
        completion,
        must_finish,
        reply_tx,
        response_delivery_rx,
        grant_id,
        client,
        permit: Some(permit),
        past_deadline: false,
        continuation: crate::daemon::control::pending::ControlContinuation::callback(|_, _| {
            crate::daemon::control::pending::ControlPoll::Pending
        }),
        retire: None,
    };
    match step {
        ControlStep::Ready(response) => finish(daemon, state, entry, ControlReply::plain(response)),
        ControlStep::Pending(pending) => {
            let ready_class = pending.ready_class;
            state.pending_requests.insert(
                waiter_id,
                PendingControlRequest {
                    continuation: pending.continuation,
                    retire: pending.retire,
                    ready_class,
                    ..entry
                },
            );
            let now = Instant::now();
            let deadline = now + crate::daemon::owner_budget::RETAINED_OPERATION_DEADLINE;
            let arm = state
                .deadlines
                .arm(waiter_id, deadline, now)
                .expect("an initial owner deadline always makes progress");
            state
                .pending_requests
                .get_mut(&waiter_id)
                .expect("the pending waiter was inserted")
                .deadline_key = Some(arm.key());
            mark_owner_ready(state, waiter_id, ready_class, READY_INITIAL);
            if arm.is_due() {
                mark_owner_ready(state, waiter_id, ReadyClass::Deadline, READY_DEADLINE);
            }
            false
        }
    }
}

pub(crate) fn poll_one_ready(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    item: crate::daemon::owner_schedule::ReadyItem,
) -> bool {
    poll_ready_request_item(daemon, state, item, &mut finish)
}

/// Post-process one complete response and send it. Returns `true` after a
/// `shutdown` response.
pub(crate) fn finish_status_delivery(
    state: &mut DaemonControlState,
    entry: PendingControlRequest,
    shutdown: bool,
    received: bool,
) -> bool {
    if let Some(permit) = entry.permit {
        if let Some(recovery) = state.host_recovery.get_mut(&entry.waiter_id) {
            recovery.retain_owner_permit(permit);
        } else {
            state.budget.release(permit);
        }
    }
    if shutdown {
        state.shutdown_waiter = None;
        finish_shutdown_update_reply(state);
    }
    crate::daemon::owner_loop::wait_for_response_delivery(
        shutdown,
        received,
        entry.response_delivery_rx,
    );
    shutdown
}

fn finish_shutdown_update_reply(state: &mut DaemonControlState) {
    if let Some(update_reply_tx) = state.pending_hub_update_reply.take() {
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
}

fn finish(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    entry: PendingControlRequest,
    response: ControlReply,
) -> bool {
    let PendingControlRequest {
        waiter_id,
        completion,
        reply_tx,
        response_delivery_rx,
        grant_id,
        client,
        permit,
        ..
    } = entry;
    if let Some(permit) = permit {
        if let Some(recovery) = state.host_recovery.get_mut(&waiter_id) {
            recovery.retain_owner_permit(permit);
        } else {
            state.budget.release(permit);
        }
    }
    let reconcile_after_request = completion.reconciles_after_success();
    let ControlReply::Typed {
        response,
        charge: plugin_result_charge,
    } = response
    else {
        return send_control_reply(reply_tx, response, response_delivery_rx);
    };
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
    if completion.is_detach()
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
    if let Some(session_id) = completion.shutdown_session_id()
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
        let change = (response.kind != DaemonResponseKind::OperatorError)
            .then(|| completion.route_change())
            .flatten();
        // A Detach whose route key is now owned by a replacement stream must
        // not remove the replacement's live-attach bookkeeping.
        let change = match change {
            Some(AttachedSubscriptionChange::Detach(ref subscription))
                if state
                    .pending_runtime
                    .stream_identity(&subscription.session_id, &subscription.subscription_id)
                    .is_some() =>
            {
                None
            }
            change => change,
        };
        // An explicit detach releases the key from the owner's route set;
        // the attach reserved it before starting.
        if let (Some(budget_key), Some(AttachedSubscriptionChange::Detach(subscription))) =
            (client.as_deref(), change.as_ref())
        {
            state.pending_runtime.release_route(
                budget_key,
                &subscription.session_id,
                &subscription.subscription_id,
            );
        }
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
        if let Some(session_id) = completion.spawned_session_id() {
            state
                .maintenance
                .acknowledged_spawn_ids
                .insert(session_id.to_owned());
            if let Some(runtime) = daemon.runtime() {
                runtime.record_acknowledged_spawn(session_id);
            }
        }
        if reconcile_after_request {
            state.maintenance.note_authoritative_mutation();
        } else if completion.is_plugin_surface_action()
            && daemon
                .runtime()
                .is_some_and(crate::HubRuntime::package_entity_work_pending)
        {
            state.maintenance.try_wake();
        }
    }
    if completion.marks_pump(succeeded) {
        crate::daemon::owner_loop::mark_pump_ready(state);
    }
    if daemon.runtime().is_some_and(|runtime| {
        runtime.package_event_router().peek_delivery_wake()
            || runtime.package_entity_work_pending()
            || runtime.package_entity_resync_still_needed()
    }) {
        state.maintenance.try_wake();
    }
    if response
        .as_ref()
        .is_ok_and(|response| response.kind == DaemonResponseKind::Shutdown)
    {
        finish_shutdown_update_reply(state);
    }
    // Reply first so surface-action publish can return before fanout delivery.
    // Attach writes `attaching` before Core attach work. Bound adapters then
    // carry terminal frames without a later host control pulse.
    // Authoritative mutations already set one coalesced wake. Status and
    // other reads must not force an extra owner-loop slice.
    send_control_reply(
        reply_tx,
        ControlReply::Typed {
            response,
            charge: plugin_result_charge,
        },
        response_delivery_rx,
    )
}

#[allow(dead_code)]
fn reply_sender_type_check(_: ControlReplySender, _: Option<mpsc::Receiver<()>>) {}
