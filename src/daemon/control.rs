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

use crate::client_api_dto::response::{daemon_events, daemon_hub_update, daemon_response_base};
use crate::daemon::error::{
    DaemonTransportError, DaemonTransportResult, daemon_entrypoint_error,
    daemon_local_webrtc_error, daemon_operator_error, daemon_package_compensation_error,
    daemon_package_error, daemon_snapshot_stream_forbidden_error, daemon_spawn_target_error,
    daemon_state_error, daemon_worktree_error,
};
use crate::daemon::owner_loop::{
    DaemonControlState, DaemonEgressDiagnostics, PendingRuntimeState, record_egress_write_failure,
    request_succeeded, send_control_response, should_mark_pump_after_control,
};
use crate::maintenance::software_identity;
use crate::subscription::attach_routes::{
    AttachedSubscriptionChange, attached_subscription_change_for_response,
    overlay_live_attach_occupancy, record_attached_subscription_change,
};
use crate::{HubClientResponseBody, HubDaemon};
pub(crate) use message::{ControlMessage, ControlSender};

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
        ControlMessage::HubUpdateCheckCompleted { update } => {
            host::hub_update_check_completed(state, update)
        }
        ControlMessage::LocalWebrtcPeerClosed {
            grant_id,
            attached_subscriptions,
            entity_subscription_ids,
            terminal_record,
        } => webrtc::handle_peer_closed(
            daemon,
            state,
            local_webrtc_terminal_record_path,
            control_tx,
            grant_id,
            attached_subscriptions,
            entity_subscription_ids,
            terminal_record,
        ),
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
        DaemonRequest::ListApps
        | DaemonRequest::ResolveAppLaunch { .. }
        | DaemonRequest::ResolvePackageRoute { .. }
        | DaemonRequest::ListPackageNavigation
        | DaemonRequest::ListPackages
        | DaemonRequest::ListAvailablePackages { .. }
        | DaemonRequest::InspectAvailablePackage { .. }
        | DaemonRequest::PreviewPackageInstall { .. }
        | DaemonRequest::InstallPackageRegistryEntry { .. }
        | DaemonRequest::InstallPackageLocalPath { .. }
        | DaemonRequest::CheckPackageUpdate { .. }
        | DaemonRequest::PreviewPackageUpdate { .. }
        | DaemonRequest::ApplyPackageUpdate { .. }
        | DaemonRequest::ShowPackage { .. }
        | DaemonRequest::SetPackageConfiguration { .. }
        | DaemonRequest::ReloadPackage { .. }
        | DaemonRequest::RefreshLocalPackages
        | DaemonRequest::EnablePackageLocalPath { .. }
        | DaemonRequest::EnablePackage { .. }
        | DaemonRequest::DisablePackage { .. }
        | DaemonRequest::RemovePackage { .. }
        | DaemonRequest::StartPackageEntrypoint { .. }
        | DaemonRequest::StopPackageEntrypoint { .. }
        | DaemonRequest::RestartPackageEntrypoint { .. }
        | DaemonRequest::PackageEntrypointStatus { .. } => {
            packages::handle_request(daemon, request)
        }
        DaemonRequest::ListSpawnTargets
        | DaemonRequest::ShowSpawnTarget { .. }
        | DaemonRequest::CreateSpawnTarget { .. }
        | DaemonRequest::UpdateSpawnTarget { .. }
        | DaemonRequest::DeleteSpawnTarget { .. }
        | DaemonRequest::ValidateSpawnTarget { .. }
        | DaemonRequest::ListWorktrees
        | DaemonRequest::ShowWorktree { .. }
        | DaemonRequest::CreateWorktree { .. }
        | DaemonRequest::DeleteWorktree { .. } => spawn_targets::handle_request(daemon, request),
        DaemonRequest::PluginLifecycleStatus => plugins::handle_request(daemon, request),
        DaemonRequest::IssueLocalWebrtcBootstrap { .. }
        | DaemonRequest::LocalWebrtcSignal { .. } => {
            webrtc::handle_request(daemon, control_tx, request)
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
    match request {
        DaemonRequest::SubscribeEntities { .. } | DaemonRequest::UnsubscribeEntities { .. } => {
            entities::reject_json_request(request)
        }
        DaemonRequest::SubscribeEvents { .. } | DaemonRequest::UnsubscribeEvents { .. } => {
            events::reject_json_request(request)
        }
        DaemonRequest::Status
        | DaemonRequest::ListSessions
        | DaemonRequest::RemoveSession { .. }
        | DaemonRequest::Spawn { .. }
        | DaemonRequest::Attach { .. }
        | DaemonRequest::Detach { .. }
        | DaemonRequest::SendInput { .. }
        | DaemonRequest::ModeGatedInput { .. }
        | DaemonRequest::Resize { .. }
        | DaemonRequest::ShutdownSession { .. }
        | DaemonRequest::Drain { .. }
        | DaemonRequest::ReadScreen { .. }
        | DaemonRequest::ReadModeFlags { .. }
        | DaemonRequest::CaptureSnapshot { .. }
        | DaemonRequest::ReadSessionContext { .. } => sessions::handle_runtime(
            daemon,
            logical_clock,
            drain_cursors,
            pending_runtime,
            observability,
            request,
        ),
        DaemonRequest::ListSessionTypes
        | DaemonRequest::ListSessionTypesForTarget { .. }
        | DaemonRequest::ShowSessionType { .. }
        | DaemonRequest::ShowSessionTypeDefinition { .. }
        | DaemonRequest::CreateSessionType { .. }
        | DaemonRequest::UpdateSessionType { .. }
        | DaemonRequest::DeleteSessionType { .. }
        | DaemonRequest::ResolveSessionType { .. }
        | DaemonRequest::SpawnSessionType { .. } => session_types::handle_runtime(
            daemon,
            logical_clock,
            drain_cursors,
            pending_runtime,
            observability,
            request,
        ),
        DaemonRequest::Whoami { .. }
        | DaemonRequest::PostMessage { .. }
        | DaemonRequest::ReceiveMessages { .. }
        | DaemonRequest::AckMessage { .. }
        | DaemonRequest::NotifySession { .. } => {
            messaging::handle_runtime(daemon, logical_clock, observability, request)
        }
        DaemonRequest::PluginMcpListTools
        | DaemonRequest::PluginMcpCallTool { .. }
        | DaemonRequest::PluginSurfaceRender { .. }
        | DaemonRequest::PluginSurfaceAction { .. } => {
            plugins::handle_runtime(daemon, observability, request)
        }
        DaemonRequest::DaemonShutdown => host::handle_runtime(daemon, observability, request),
        DaemonRequest::IssueLocalWebrtcBootstrap { .. }
        | DaemonRequest::LocalWebrtcSignal { .. } => Err(DaemonTransportError::UnexpectedResponse),
        DaemonRequest::CheckHubUpdate
        | DaemonRequest::StartHubUpdate { .. }
        | DaemonRequest::GetHubUpdateExecution => {
            unreachable!("Hub update requests are handled before runtime borrow")
        }
        DaemonRequest::ListApps
        | DaemonRequest::ResolveAppLaunch { .. }
        | DaemonRequest::ResolvePackageRoute { .. }
        | DaemonRequest::ListPackageNavigation
        | DaemonRequest::ListPackages
        | DaemonRequest::ListSpawnTargets
        | DaemonRequest::ShowSpawnTarget { .. }
        | DaemonRequest::CreateSpawnTarget { .. }
        | DaemonRequest::UpdateSpawnTarget { .. }
        | DaemonRequest::DeleteSpawnTarget { .. }
        | DaemonRequest::ValidateSpawnTarget { .. }
        | DaemonRequest::ListWorktrees
        | DaemonRequest::ShowWorktree { .. }
        | DaemonRequest::CreateWorktree { .. }
        | DaemonRequest::DeleteWorktree { .. }
        | DaemonRequest::ListAvailablePackages { .. }
        | DaemonRequest::InspectAvailablePackage { .. }
        | DaemonRequest::PreviewPackageInstall { .. }
        | DaemonRequest::InstallPackageRegistryEntry { .. }
        | DaemonRequest::InstallPackageLocalPath { .. }
        | DaemonRequest::CheckPackageUpdate { .. }
        | DaemonRequest::PreviewPackageUpdate { .. }
        | DaemonRequest::ApplyPackageUpdate { .. }
        | DaemonRequest::ShowPackage { .. }
        | DaemonRequest::SetPackageConfiguration { .. }
        | DaemonRequest::ReloadPackage { .. }
        | DaemonRequest::RefreshLocalPackages
        | DaemonRequest::PluginLifecycleStatus
        | DaemonRequest::EnablePackageLocalPath { .. }
        | DaemonRequest::EnablePackage { .. }
        | DaemonRequest::DisablePackage { .. }
        | DaemonRequest::RemovePackage { .. }
        | DaemonRequest::StartPackageEntrypoint { .. }
        | DaemonRequest::StopPackageEntrypoint { .. }
        | DaemonRequest::RestartPackageEntrypoint { .. }
        | DaemonRequest::PackageEntrypointStatus { .. } => {
            unreachable!("package requests are handled before runtime borrow")
        }
    }
}
