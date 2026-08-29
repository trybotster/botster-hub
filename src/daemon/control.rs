//! Control-plane dispatchers.

pub(crate) mod connection;
pub(crate) mod entities;
pub(crate) mod events;
pub(crate) mod host;
pub(crate) mod message;
pub(crate) mod messaging;
pub(crate) mod packages;
pub(crate) mod plugins;
pub(crate) mod session_types;
pub(crate) mod sessions;
pub(crate) mod spawn_targets;
pub(crate) mod webrtc;

use std::collections::BTreeMap;
use std::path::Path;

use botster_core::RequestId;
use botster_hub_client::{
    DaemonDiagnostic, DaemonHubUpdate, DaemonHubUpdateState, DaemonLifecycleCounters,
    DaemonOperatorError, DaemonRequest, DaemonResponse, DaemonResponseKind,
};

use crate::client_api_dto::response::{
    daemon_events, daemon_hub_update, daemon_hub_update_execution, daemon_local_webrtc_answer,
    daemon_response_base,
};
use crate::daemon::error::{
    DaemonTransportError, DaemonTransportResult, daemon_entrypoint_error,
    daemon_local_webrtc_error, daemon_operator_error, daemon_package_compensation_error,
    daemon_package_error, daemon_snapshot_stream_forbidden_error, daemon_spawn_target_error,
    daemon_state_error, daemon_worktree_error, hub_update_execution_error,
};
use crate::daemon::owner_loop::{
    DaemonControlState, DaemonEgressDiagnostics, PendingRuntimeState, record_egress_write_failure,
    request_succeeded, send_control_response, should_mark_pump_after_control,
    wait_for_response_delivery,
};
use crate::maintenance::{
    HubUpdateCheckPlan, execute_managed_update_check, plan_hub_update_check, software_identity,
};
use crate::source_update::{current_update_execution, mark_update_failed, start_update_handoff};
use crate::subscription::attach_routes::{
    AttachedSubscription, AttachedSubscriptionChange, attached_subscription_change_for_response,
    overlay_live_attach_occupancy, record_attached_subscription_change,
};
use crate::transport::webrtc::LocalWebrtcAttachedSubscription;
use crate::transport::webrtc::LocalWebrtcSignalRequest;
use crate::{
    HubClientResponseBody, HubDaemon, SpawnTargetCreate, SpawnTargetError, SpawnTargetUpdate,
    WorktreeCreate,
};
pub(crate) use message::{ControlMessage, ControlSender};
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
pub(crate) struct DaemonObservability<'a> {
    pub(crate) egress: &'a DaemonEgressDiagnostics,
    pub(crate) lifecycle: &'a DaemonLifecycleCounters,
    pub(crate) client_id: Option<&'a str>,
    pub(crate) grant_id: Option<&'a str>,
}

pub(crate) fn request_id(value: &str) -> RequestId {
    RequestId(value.to_string())
}

pub(crate) fn runtime_client_id(request: &DaemonRequest) -> String {
    match request {
        DaemonRequest::Attach {
            subscription_id, ..
        }
        | DaemonRequest::Detach {
            subscription_id, ..
        } => format!("botster-hub-daemon-subscription-{subscription_id}"),
        _ => "botster-hub-daemon-socket".to_string(),
    }
}

pub(crate) fn events_response(
    body: HubClientResponseBody,
) -> DaemonTransportResult<DaemonResponse> {
    let HubClientResponseBody::Events(events) = body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(daemon_events(events::events_from_client(events)))
}

pub(crate) fn attach_bind_operator_error(code: &'static str, message: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: "daemon-attach-bind".to_string(),
        operation: "attach".to_string(),
        message: message.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure("attach", message)],
    });
    response
}

pub(crate) fn missing_session_drain_error(session_id: &str) -> DaemonResponse {
    let message = format!("unknown session: {session_id}");
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.diagnostics = vec![DaemonDiagnostic::terminal_stream_unavailable(
        "drain_runtime",
        message.clone(),
    )];
    response.error = Some(DaemonOperatorError {
        code: "unknown_session".to_string(),
        request_id: "daemon-sessions-drain".to_string(),
        operation: "drain_runtime".to_string(),
        message,
        diagnostics: response.diagnostics.clone(),
    });
    response
}

pub(crate) fn control_request_operation_label(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Status => "status",
        DaemonRequest::ListSessions => "list_sessions",
        DaemonRequest::Spawn { .. } => "spawn",
        DaemonRequest::Attach { .. } => "attach",
        DaemonRequest::Detach { .. } => "detach",
        DaemonRequest::SendInput { .. } => "send_input",
        DaemonRequest::ModeGatedInput { .. } => "mode_gated_input",
        DaemonRequest::Drain { .. } => "drain",
        DaemonRequest::Resize { .. } => "resize",
        DaemonRequest::ShutdownSession { .. } => "shutdown_session",
        DaemonRequest::RemoveSession { .. } => "remove_session",
        DaemonRequest::DaemonShutdown => "daemon_shutdown",
        DaemonRequest::CheckHubUpdate => "check_hub_update",
        DaemonRequest::StartHubUpdate { .. } => "start_hub_update",
        DaemonRequest::GetHubUpdateExecution => "get_hub_update_execution",
        _ => "request",
    }
}

pub(crate) fn handle_control_message(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    local_webrtc_terminal_record_path: &Path,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    message: ControlMessage,
) -> bool {
    match message {
        ControlMessage::AcceptedConnection { .. } | ControlMessage::RejectedConnection => false,
        ControlMessage::RegisterUnixAdmission {
            client_id,
            admission,
            reply_tx,
            host_required_features,
        } => connection::register_unix_admission(
            state,
            client_id,
            admission,
            reply_tx,
            host_required_features,
        ),
        ControlMessage::RegisterWebrtcAdmission {
            grant_id,
            admission,
            host_required_features,
        } => connection::register_webrtc_admission(
            daemon,
            state,
            grant_id,
            admission,
            host_required_features,
        ),
        ControlMessage::SubscribeEntities {
            entity_type,
            subscription_id,
            frame_tx,
            reply_tx,
            grant_id,
        } => entities::subscribe(
            daemon,
            state,
            entity_type,
            subscription_id,
            frame_tx,
            reply_tx,
            grant_id,
        ),
        ControlMessage::UnsubscribeEntities {
            subscription_id,
            reply_tx,
            grant_id,
        } => entities::unsubscribe(daemon, state, subscription_id, reply_tx, grant_id),
        ControlMessage::Request {
            request,
            reply_tx,
            response_delivery_rx,
            grant_id,
            client_id,
            enqueued_at,
        } => {
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
            if matches!(request.as_ref(), DaemonRequest::CheckHubUpdate) {
                return match plan_hub_update_check() {
                    HubUpdateCheckPlan::Immediate(update) => send_control_response(
                        reply_tx,
                        Ok(daemon_hub_update(update)),
                        response_delivery_rx,
                    ),
                    HubUpdateCheckPlan::Managed(_check)
                        if state.pending_hub_update_reply.is_some() =>
                    {
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
                            let _ = completion_tx
                                .blocking_send(ControlMessage::HubUpdateCheckCompleted { update });
                        });
                        false
                    }
                };
            }
            if let DaemonRequest::StartHubUpdate { scope } = request.as_ref() {
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
                return match start_update_handoff(&data_directory, *scope) {
                    Ok((execution, handoff)) => {
                        let update_id = execution.update_id.clone();
                        let response_received = reply_tx
                            .send(Ok(daemon_hub_update_execution(execution)))
                            .is_ok();
                        wait_for_response_delivery(
                            response_received,
                            response_received,
                            response_delivery_rx,
                        );
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
                };
            }
            if matches!(request.as_ref(), DaemonRequest::GetHubUpdateExecution) {
                let response = match daemon.runtime() {
                    Some(runtime) => {
                        match current_update_execution(&runtime.config().data_directory) {
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
                        }
                    }
                    None => hub_update_execution_error(
                        "hub_update_runtime_unavailable",
                        "get_hub_update_execution",
                        "the Hub runtime is not available",
                    ),
                };
                return send_control_response(reply_tx, Ok(response), response_delivery_rx);
            }
            let request = *request;
            let drain_owned_before = match &request {
                DaemonRequest::Drain {
                    session_id,
                    subscription_id: Some(subscription_id),
                } => state
                    .pending_runtime
                    .stream_owner_client_id(session_id, subscription_id)
                    .is_some(),
                _ => false,
            };
            let reconcile_after_request = matches!(
                request,
                DaemonRequest::Spawn { .. }
                    | DaemonRequest::Attach { .. }
                    | DaemonRequest::Resize { .. }
                    | DaemonRequest::SendInput { .. }
                    | DaemonRequest::ModeGatedInput { .. }
                    | DaemonRequest::ShutdownSession { .. }
                    | DaemonRequest::RemoveSession { .. }
            );
            let mut response = handle_control_request(
                daemon,
                &mut state.logical_clock,
                &mut state.drain_cursors,
                &mut state.pending_runtime,
                DaemonObservability {
                    egress: &state.egress_diagnostics,
                    lifecycle: &state.lifecycle_counters,
                    client_id: client_id.as_deref(),
                    grant_id: grant_id.as_deref(),
                },
                control_tx,
                request.clone(),
            )
            .or_else(|error| match error {
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
                let change = match change {
                    Some(AttachedSubscriptionChange::Detach(_))
                        if matches!(request, DaemonRequest::Drain { .. })
                            && !drain_owned_before =>
                    {
                        None
                    }
                    change => change,
                };
                record_attached_subscription_change(
                    &mut state.pending_runtime,
                    &mut state.attach_close,
                    &mut state.lifecycle_counters,
                    change,
                    grant_id.as_deref(),
                );
            }
            if let Ok(response) = response.as_mut()
                && let Some(status) = response.status.as_mut()
            {
                overlay_live_attach_occupancy(
                    status,
                    daemon,
                    &state.pending_runtime.live_attach_routes,
                    &state.pending_runtime,
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
                if matches!(request, DaemonRequest::ReadScreen { .. })
                    && daemon
                        .runtime()
                        .is_some_and(crate::HubRuntime::take_journal_advanced_wake)
                {
                    state.maintenance.note_authoritative_mutation();
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
            // Attach writes `attaching` before Core attach work; Drain advances the stream.
            // Authoritative mutations already set one coalesced wake. Status and
            // other reads must not force an extra owner-loop slice.
            send_control_response(reply_tx, response, response_delivery_rx)
        }
        ControlMessage::HubUpdateCheckCompleted { update } => state
            .pending_hub_update_reply
            .take()
            .is_some_and(|reply_tx| {
                send_control_response(reply_tx, Ok(daemon_hub_update(update)), None)
            }),
        ControlMessage::LocalWebrtcPeerClosed {
            grant_id,
            attached_subscriptions,
            entity_subscription_ids,
            terminal_record,
        } => {
            let cleanup_reason = format!("webrtc_{}", terminal_record.cause);
            *state
                .lifecycle_counters
                .cleanup_by_reason
                .entry(cleanup_reason)
                .or_default() += 1;
            state.lifecycle_counters.cleanup_completed =
                state.lifecycle_counters.cleanup_completed.saturating_add(1);
            if let Err(error) = webrtc::persist_local_webrtc_terminal_record(
                local_webrtc_terminal_record_path,
                &terminal_record,
            ) {
                eprintln!(
                    "local WebRTC sender terminal record persistence failed: kind={:?}",
                    error.kind()
                );
            }
            let remove_result = daemon.local_webrtc().remove_peer(&grant_id);
            let mut removed_grants: BTreeSet<String> =
                remove_result.removed_grant_ids.into_iter().collect();
            // Always include the closing grant so entity/attach sweep runs even if the peer
            // map entry was already gone (idempotent PeerClosed).
            removed_grants.insert(grant_id.clone());

            // Snapshot IDs are only removed when the current row is unowned or still owned by a
            // removed grant. A reused subscription_id owned by a different live peer is preserved.
            let mut removed_entity_ids = BTreeSet::new();
            for subscription_id in entity_subscription_ids {
                let should_remove = match state.entity_subscriptions.get(&subscription_id) {
                    None => false,
                    Some(subscription) => match subscription.owner_grant_id.as_deref() {
                        None => true,
                        Some(owner) => removed_grants.contains(owner),
                    },
                };
                if should_remove {
                    removed_entity_ids.insert(subscription_id);
                }
            }
            // Independent of the peer-side snapshot: remove every daemon entity subscription
            // owned by any grant this forget removed (primary + fail-closed siblings).
            for (id, subscription) in &state.entity_subscriptions {
                if let Some(owner) = subscription.owner_grant_id.as_deref()
                    && removed_grants.contains(owner)
                {
                    removed_entity_ids.insert(id.clone());
                }
            }
            for subscription_id in removed_entity_ids {
                if state
                    .entity_subscriptions
                    .remove(&subscription_id)
                    .is_some()
                {
                    state.lifecycle_counters.live_entity_subscriptions = state
                        .lifecycle_counters
                        .live_entity_subscriptions
                        .saturating_sub(1);
                    state.released_entity_generations =
                        state.released_entity_generations.saturating_add(1);
                }
            }

            // Merge attach candidates from the PeerClosed snapshot and any fail-closed siblings.
            // Owner-check every row: a delayed snapshot must not detach an attach that a
            // different live grant now owns after (session_id, subscription_id) reuse.
            let mut detach_candidates = attached_subscriptions;
            for subscription in remove_result.attached_subscriptions {
                if !detach_candidates.iter().any(|existing| {
                    existing.session_id == subscription.session_id
                        && existing.subscription_id == subscription.subscription_id
                }) {
                    detach_candidates.push(subscription);
                }
            }
            // Independent of the peer-side snapshot: include every attach currently owned by a
            // removed grant so residual Attach rows that raced after cleanup_once still get cleaned.
            for ((session_id, subscription_id), owner) in
                &state.pending_runtime.attach_owner_grant_ids
            {
                if removed_grants.contains(owner.as_str())
                    && !detach_candidates.iter().any(|existing| {
                        existing.session_id == *session_id
                            && existing.subscription_id == *subscription_id
                    })
                {
                    detach_candidates.push(LocalWebrtcAttachedSubscription {
                        session_id: session_id.clone(),
                        subscription_id: subscription_id.clone(),
                    });
                }
            }
            let detach_list: Vec<LocalWebrtcAttachedSubscription> = detach_candidates
                .into_iter()
                .filter(|subscription| {
                    match state
                        .pending_runtime
                        .attach_owner_grant_ids
                        .get(&(
                            subscription.session_id.clone(),
                            subscription.subscription_id.clone(),
                        ))
                        .map(String::as_str)
                    {
                        // Unowned residual (socket path or missing index): allow cleanup.
                        None => true,
                        // Only detach when the current owner is one of the grants this forget removes.
                        Some(owner) => removed_grants.contains(owner),
                    }
                })
                .collect();
            // Occupancy set is the counter source of truth. PeerClosed must release
            // live_attach_routes here so a replacement Attach can become live.
            for subscription in &detach_list {
                record_attached_subscription_change(
                    &mut state.pending_runtime,
                    &mut state.attach_close,
                    &mut state.lifecycle_counters,
                    Some(AttachedSubscriptionChange::Detach(AttachedSubscription {
                        session_id: subscription.session_id.clone(),
                        subscription_id: subscription.subscription_id.clone(),
                    })),
                    None,
                );
            }
            let mut bound_detach = Vec::new();
            let mut unbound_detach = Vec::new();
            for subscription in detach_list {
                if state
                    .pending_runtime
                    .is_adapter_bound(&subscription.session_id, &subscription.subscription_id)
                {
                    bound_detach.push(subscription);
                } else {
                    unbound_detach.push(subscription);
                }
            }
            if !bound_detach.is_empty() {
                *state
                    .lifecycle_counters
                    .cleanup_by_reason
                    .entry("bound_adapter_close".to_string())
                    .or_insert(0) += bound_detach.len() as u64;
            }
            for grant_id in &removed_grants {
                state.pending_runtime.close_adapters_for_grant(grant_id);
            }
            for subscription in &bound_detach {
                state
                    .pending_runtime
                    .cancel_stream(&subscription.session_id, &subscription.subscription_id);
            }
            for grant_id in &removed_grants {
                state
                    .pending_runtime
                    .admission
                    .webrtc_admissions
                    .remove(grant_id);
                state
                    .pending_runtime
                    .admission
                    .host_compatibility
                    .remove(grant_id);
                if let Some(runtime) = daemon.runtime() {
                    state
                        .event_plane
                        .cleanup_connection(grant_id, runtime.package_event_router());
                }
            }
            // Residual same-grant index rows can survive a no-op Core Detach. Drop them
            // after occupancy release. Preserve replacement owners.
            state
                .pending_runtime
                .attach_owner_grant_ids
                .retain(|_, owner| !removed_grants.contains(owner.as_str()));
            webrtc::detach_local_webrtc_subscriptions(
                daemon,
                &mut state.logical_clock,
                &mut state.drain_cursors,
                &mut state.pending_runtime,
                control_tx,
                DaemonObservability {
                    egress: &state.egress_diagnostics,
                    lifecycle: &state.lifecycle_counters,
                    client_id: None,
                    grant_id: None,
                },
                unbound_detach,
            );
            false
        }
        ControlMessage::EgressWriteFailed {
            delivery_kind,
            write_class,
        } => {
            record_egress_write_failure(
                &mut state.egress_diagnostics,
                &mut state.lifecycle_counters,
                daemon.runtime(),
                delivery_kind,
                write_class,
            );
            false
        }
    }
}

pub(crate) fn handle_control_request(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    pending_runtime: &mut PendingRuntimeState,
    observability: DaemonObservability<'_>,
    control_tx: ControlSender,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::ListApps => packages::list_apps_response(daemon),
        DaemonRequest::ListSpawnTargets => spawn_targets::list_spawn_targets_response(daemon),
        DaemonRequest::ShowSpawnTarget { target_id } => {
            spawn_targets::show_spawn_target_response(daemon, &target_id)
        }
        DaemonRequest::CreateSpawnTarget {
            target_id,
            label,
            root,
            enabled,
            kind,
            base_ref,
            metadata,
        } => {
            // Only pre-check session-types once the root is known to be a directory.
            // Non-directory roots must fall through to create_spawn_target's
            // root_not_directory rather than a misleading invalid_repo_session_types.
            if enabled && root.is_dir() {
                session_types::ensure_repo_session_types_valid_for_enabled_root(&root)?;
            }
            let before_session_types = session_types::session_type_definition_map(daemon)?;
            let response = spawn_targets::mutate_spawn_targets_response(daemon, |targets| {
                crate::create_spawn_target(
                    targets,
                    SpawnTargetCreate {
                        target_id,
                        label,
                        root,
                        enabled,
                        kind,
                        base_ref,
                        metadata,
                    },
                )
            })?;
            session_types::advance_session_type_generation_if_changed(
                daemon,
                &before_session_types,
            )?;
            Ok(response)
        }
        DaemonRequest::UpdateSpawnTarget {
            target_id,
            label,
            root,
            enabled,
            kind,
            base_ref,
            metadata,
        } => {
            let recovery_disable = enabled == Some(false);
            if !recovery_disable {
                session_types::ensure_update_would_not_enable_invalid_repo_session_types(
                    daemon,
                    &target_id,
                    root.as_ref(),
                    enabled,
                )?;
            }
            let before_session_types = match session_types::session_type_definition_map(daemon) {
                Ok(before) => Some(before),
                Err(error)
                    if recovery_disable
                        && session_types::is_invalid_repo_session_types_error(&error) =>
                {
                    None
                }
                Err(error) => return Err(error),
            };
            let response = spawn_targets::mutate_spawn_targets_with_worktrees_response(
                daemon,
                |targets, worktrees| {
                    if kind.as_deref().is_some_and(|kind| kind != "git")
                        && worktrees.iter().any(|worktree| {
                            worktree.target_id == target_id
                                && worktree.management == "hub_managed_git"
                        })
                    {
                        return Err(SpawnTargetError::new(
                            "managed_worktrees_exist",
                            "Git target cannot be reclassified while managed worktrees reference it",
                        ));
                    }
                    crate::update_spawn_target(
                        targets,
                        &target_id,
                        SpawnTargetUpdate {
                            label,
                            root,
                            enabled,
                            kind,
                            base_ref,
                            metadata,
                        },
                    )
                },
            )?;
            match before_session_types {
                Some(before) => {
                    session_types::advance_session_type_generation_if_changed(daemon, &before)?;
                }
                None => {
                    session_types::force_advance_session_type_generation(daemon)?;
                }
            }
            Ok(response)
        }
        DaemonRequest::DeleteSpawnTarget { target_id } => {
            let before_session_types = match session_types::session_type_definition_map(daemon) {
                Ok(before) => Some(before),
                Err(error) if session_types::is_invalid_repo_session_types_error(&error) => None,
                Err(error) => return Err(error),
            };
            let response = spawn_targets::mutate_spawn_targets_with_worktrees_response(
                daemon,
                |targets, worktrees| {
                    if worktrees.iter().any(|worktree| {
                        worktree.target_id == target_id && worktree.management == "hub_managed_git"
                    }) {
                        return Err(SpawnTargetError::new(
                            "managed_worktrees_exist",
                            "Git target cannot be deleted while managed worktrees reference it",
                        ));
                    }
                    crate::delete_spawn_target(targets, &target_id)
                },
            )?;
            match before_session_types {
                Some(before) => {
                    session_types::advance_session_type_generation_if_changed(daemon, &before)?;
                }
                None => {
                    session_types::force_advance_session_type_generation(daemon)?;
                }
            }
            Ok(response)
        }
        DaemonRequest::ValidateSpawnTarget { target_id } => Ok(
            crate::client_api_dto::response::daemon_spawn_target_validation(
                crate::validate_spawn_target(
                    &daemon
                        .runtime()
                        .ok_or(DaemonTransportError::DaemonNotRunning)?
                        .state()
                        .spawn_targets,
                    &target_id,
                ),
            ),
        ),
        DaemonRequest::ListWorktrees => spawn_targets::list_worktrees_response(daemon),
        DaemonRequest::ShowWorktree { worktree_id } => {
            spawn_targets::show_worktree_response(daemon, &worktree_id)
        }
        DaemonRequest::CreateWorktree {
            worktree_id,
            target_id,
            label,
            path,
            metadata,
        } => spawn_targets::create_worktree_response(
            daemon,
            WorktreeCreate {
                worktree_id,
                target_id,
                label,
                path,
                metadata,
            },
        ),
        DaemonRequest::DeleteWorktree { worktree_id } => {
            spawn_targets::delete_worktree_response(daemon, &worktree_id)
        }
        DaemonRequest::ResolveAppLaunch {
            package_name,
            entrypoint_id,
        } => packages::resolve_app_launch_response(daemon, &package_name, &entrypoint_id),
        DaemonRequest::ResolvePackageRoute {
            package_name,
            route_id,
        } => packages::resolve_package_route_response(daemon, &package_name, &route_id),
        DaemonRequest::ListPackageNavigation => packages::list_package_navigation_response(daemon),
        DaemonRequest::ListPackages => packages::list_packages_response(daemon),
        DaemonRequest::ListAvailablePackages { registry_path } => {
            packages::available_packages_response(daemon, registry_path)
        }
        DaemonRequest::InspectAvailablePackage {
            registry_path,
            entry_id,
        } => packages::inspect_available_package_response(daemon, registry_path, &entry_id),
        DaemonRequest::PreviewPackageInstall {
            registry_path,
            entry_id,
        } => packages::preview_package_install_response(daemon, registry_path, &entry_id),
        DaemonRequest::InstallPackageRegistryEntry {
            registry_path,
            entry_id,
        } => packages::mutations::install_registry_package(daemon, registry_path, entry_id),
        DaemonRequest::PluginLifecycleStatus => packages::plugin_lifecycle_response(daemon),
        DaemonRequest::InstallPackageLocalPath { path } => {
            packages::mutations::install_local_package(daemon, path)
        }
        DaemonRequest::CheckPackageUpdate { package_name } => {
            packages::check_package_update_response(daemon, &package_name)
        }
        DaemonRequest::PreviewPackageUpdate { package_name, pin } => {
            packages::preview_package_update_response(daemon, &package_name, pin)
        }
        DaemonRequest::ApplyPackageUpdate { package_name, pin } => {
            packages::mutations::apply_package_update(daemon, package_name, pin)
        }
        DaemonRequest::ShowPackage { package_name } => {
            packages::show_package_response(daemon, &package_name)
        }
        DaemonRequest::SetPackageConfiguration {
            package_name,
            values,
        } => packages::mutations::configure_package(daemon, package_name, values),
        DaemonRequest::ReloadPackage { package_name } => {
            packages::mutations::reload_package(daemon, package_name)
        }
        DaemonRequest::RefreshLocalPackages => packages::mutations::refresh_local_packages(daemon),
        DaemonRequest::EnablePackageLocalPath { path } => {
            packages::mutations::enable_package_local_path(daemon, path)
        }
        DaemonRequest::EnablePackage { package_name } => {
            packages::mutations::enable_package(daemon, package_name)
        }
        DaemonRequest::DisablePackage { package_name } => {
            packages::mutations::disable_package(daemon, package_name)
        }
        DaemonRequest::RemovePackage { package_name } => {
            packages::mutations::remove_package(daemon, package_name)
        }
        DaemonRequest::StartPackageEntrypoint {
            package_name,
            entrypoint_id,
            environment_overrides,
        } => {
            let config = daemon
                .runtime()
                .ok_or(DaemonTransportError::DaemonNotRunning)?
                .config()
                .clone();
            let packages = daemon.package_registry().clone();
            let launch = packages::supervised_launch_contract(
                &config,
                &packages,
                &package_name,
                &entrypoint_id,
                &environment_overrides,
            )?;
            daemon.entrypoint_supervisor().start(
                &packages,
                &package_name,
                &entrypoint_id,
                &launch.args,
                &launch.environment,
            )?;
            packages::show_package_response(daemon, &package_name)
        }
        DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name,
            entrypoint_id,
            origin,
        } => webrtc::issue_local_webrtc_bootstrap_response(
            daemon,
            &package_name,
            &entrypoint_id,
            &origin,
        ),
        DaemonRequest::LocalWebrtcSignal {
            grant_id,
            grant_secret,
            origin,
            offer,
        } => {
            let signal = LocalWebrtcSignalRequest {
                grant_id,
                grant_secret,
                origin,
                offer,
            };
            let answer = daemon.local_webrtc().signal(signal, control_tx.clone())?;
            Ok(daemon_local_webrtc_answer(answer))
        }
        DaemonRequest::StopPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            daemon
                .entrypoint_supervisor()
                .stop(&package_name, &entrypoint_id);
            packages::show_package_response(daemon, &package_name)
        }
        DaemonRequest::RestartPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            let config = daemon
                .runtime()
                .ok_or(DaemonTransportError::DaemonNotRunning)?
                .config()
                .clone();
            let packages = daemon.package_registry().clone();
            let launch = packages::supervised_launch_contract(
                &config,
                &packages,
                &package_name,
                &entrypoint_id,
                &BTreeMap::new(),
            )?;
            daemon.entrypoint_supervisor().restart(
                &packages,
                &package_name,
                &entrypoint_id,
                &launch.args,
                &launch.environment,
            )?;
            packages::show_package_response(daemon, &package_name)
        }
        DaemonRequest::PackageEntrypointStatus {
            package_name,
            entrypoint_id,
        } => {
            daemon
                .entrypoint_supervisor()
                .status(&package_name, &entrypoint_id);
            packages::show_package_response(daemon, &package_name)
        }
        other => handle_runtime_control_request(
            daemon,
            logical_clock,
            drain_cursors,
            pending_runtime,
            observability,
            other,
        ),
    }
}

pub(crate) fn handle_runtime_control_request(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    pending_runtime: &mut PendingRuntimeState,
    observability: DaemonObservability<'_>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    sessions::handle_runtime(
        daemon,
        logical_clock,
        drain_cursors,
        pending_runtime,
        observability,
        request,
    )
}

#[cfg(test)]
mod ownership_guards {
    fn daemon_sources() -> Vec<(String, String)> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/daemon");
        let mut pending = vec![root.clone()];
        let mut files = Vec::new();
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).expect("read src/daemon") {
                let path = entry.expect("entry").path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                    continue;
                }
                let rel = path
                    .strip_prefix(root.parent().expect("src"))
                    .expect("under src")
                    .to_string_lossy()
                    .replace('\\', "/");
                let source = std::fs::read_to_string(&path).expect("read");
                files.push((format!("src/{rel}"), source));
            }
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        files
    }

    #[test]
    fn daemon_modules_reject_unix_transport_mechanism_symbols() {
        for (path, source) in daemon_sources() {
            let production = source.split("mod tests").next().unwrap_or(&source);
            for needle in [
                "async fn accept_connections",
                "async fn handle_connection_async",
                "struct MuxWriteState",
                "async fn read_async_frame",
                "fn prepare_socket_path",
                "fn unix_event_flush_stalled",
            ] {
                assert!(
                    !production.contains(needle),
                    "{path} must not contain {needle}"
                );
            }
        }
    }

    #[test]
    fn webrtc_liveness_gates_remain_four_distinct_sites() {
        let connection = include_str!("control/connection.rs");
        let control = include_str!("control.rs");
        let entities = include_str!("control/entities.rs");
        assert!(
            connection.contains("has_live_peer(&grant_id)"),
            "RegisterWebrtcAdmission insert gate must stay in connection.rs"
        );
        assert!(
            !connection.contains("local_webrtc_peer_gone_request_error"),
            "connection insert gate drops rather than returning a request error"
        );
        let request_gate = control
            .split("ControlMessage::Request {")
            .nth(1)
            .expect("Request arm")
            .split("        ControlMessage::HubUpdateCheckCompleted")
            .next()
            .expect("Request arm end");
        assert!(
            request_gate.contains("has_live_peer(grant_id)"),
            "Request pre-dispatch gate must stay in control.rs"
        );
        assert!(
            request_gate.contains("local_webrtc_peer_gone_request_error"),
            "Request gate must use local_webrtc_peer_gone_request_error"
        );
        let sessions_call = request_gate.find("handle_control_request");
        let live_peer = request_gate.find("has_live_peer(grant_id)");
        assert!(
            live_peer.is_some()
                && sessions_call.is_some()
                && live_peer.expect("gate") < sessions_call.expect("delegate"),
            "Request has_live_peer gate must precede family delegation"
        );
        assert!(
            entities.contains("has_live_peer(grant_id)")
                && entities.contains("local_webrtc_peer_gone"),
            "SubscribeEntities reply gate must stay in entities.rs"
        );
        assert!(
            entities.contains("EntityUnsubscribed") && entities.contains("owner_grant_id"),
            "UnsubscribeEntities owner-checked gate must stay in entities.rs"
        );
    }

    #[test]
    fn daemon_control_does_not_remove_grant_rows() {
        for (path, source) in daemon_sources() {
            let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
            assert!(
                !production.contains("prune_expired_grants"),
                "{path} must not prune grant rows"
            );
            assert!(
                !production.contains("GrantRegistry"),
                "{path} must not name GrantRegistry"
            );
        }
    }

    #[test]
    fn runtime_dispatcher_delegates_to_sessions() {
        let dispatcher = include_str!("control.rs");
        let runtime = dispatcher
            .split("pub(crate) fn handle_runtime_control_request(")
            .nth(1)
            .expect("runtime dispatcher")
            .split("#[cfg(test)]")
            .next()
            .expect("runtime dispatcher end");
        assert!(
            runtime.contains("sessions::handle_runtime("),
            "handle_runtime_control_request must delegate"
        );
        assert!(
            !runtime.contains("HubClientApi"),
            "runtime dispatcher must not construct HubClientApi"
        );
    }
}
