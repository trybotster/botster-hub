//! Control-plane dispatchers.

pub(crate) mod connection;
pub(crate) mod entities;
pub(crate) mod events;
pub(crate) mod host;
mod host_family;
pub(crate) mod host_work;
pub(crate) mod managed_git;
pub(crate) mod message;
pub(crate) mod messaging;
pub(crate) mod packages;
pub(crate) mod pending;
pub(crate) mod plugins;
pub(crate) mod reply;
pub(crate) mod request;
pub(crate) mod session_types;
pub(crate) mod sessions;
pub(crate) mod status;
pub(crate) mod webrtc;

use botster_core::RequestId;
use botster_hub_client::{
    DaemonDiagnostic, DaemonLifecycleCounters, DaemonOperatorError, DaemonRequest, DaemonResponse,
    DaemonResponseKind,
};

use crate::HubDaemon;
use crate::client_api_dto::response::daemon_response_base;
use crate::daemon::control::pending::ControlStep;
use crate::daemon::error::DaemonTransportError;
use crate::daemon::owner_loop::{DaemonControlState, record_egress_write_failure};
pub(crate) use message::{ControlMessage, ControlSender};

/// Owned snapshot of owner diagnostics and connection identity for one
/// request. Owned so a deferred continuation can keep it past the turn.
#[derive(Clone)]
pub(crate) struct DaemonObservability {
    pub(crate) egress: Vec<DaemonDiagnostic>,
    pub(crate) lifecycle: DaemonLifecycleCounters,
    pub(crate) client_id: Option<String>,
    pub(crate) grant_id: Option<String>,
    pub(crate) transport_request_id: Option<String>,
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

pub(crate) fn attach_bind_operator_error(code: &'static str, message: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: "daemon-attach-bind".to_string(),
        operation: "attach".to_string(),
        message: message.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure("attach", message)],
    });
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

pub(crate) fn control_request_operation_label(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Status => "status",
        DaemonRequest::ListSessions => "list_sessions",
        DaemonRequest::Spawn { .. } => "spawn",
        DaemonRequest::Attach { .. } => "attach",
        DaemonRequest::Detach { .. } => "detach",
        DaemonRequest::ShutdownSession { .. } => "shutdown_session",
        DaemonRequest::RemoveSession { .. } => "remove_session",
        DaemonRequest::DaemonShutdown => "daemon_shutdown",
        DaemonRequest::CheckHubUpdate => "check_hub_update",
        DaemonRequest::StartHubUpdate { .. } => "start_hub_update",
        DaemonRequest::GetHubUpdateExecution => "get_hub_update_execution",
        _ => "request",
    }
}

pub(crate) fn dispatch_control_message(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    message: ControlMessage,
) -> bool {
    match message {
        ControlMessage::DataPlaneProgress => {
            record_data_plane_progress(daemon, state);
            false
        }
        ControlMessage::CoreCompletionPublished => false,
        message @ ControlMessage::AcceptedConnection { .. }
        | message @ ControlMessage::ConnectionCleanup(_)
        | message @ ControlMessage::RejectedConnection
        | message @ ControlMessage::RegisterUnixAdmission { .. }
        | message @ ControlMessage::RegisterWebrtcAdmission { .. }
        | message @ ControlMessage::InspectReservation { .. }
        | message @ ControlMessage::BindReservedSubscription { .. }
        | message @ ControlMessage::RetireReservedSubscription { .. }
        | message @ ControlMessage::AuthorizeSubscriptionSend { .. }
        | message @ ControlMessage::AuthorizeSubscriptionHelloAck { .. } => {
            connection::handle(daemon, state, message)
        }
        message @ ControlMessage::SubscribeEntities { .. }
        | message @ ControlMessage::UnsubscribeEntities { .. } => {
            entities::handle(daemon, state, message)
        }
        message @ ControlMessage::Request { .. } => {
            request::handle(daemon, state, transport_handle, control_tx, message)
        }
        ControlMessage::HubUpdateCheckCompleted { update } => {
            host::hub_update_check_completed(state, update)
        }
        message @ ControlMessage::LocalWebrtcPeerClosed { .. } => {
            webrtc::handle_peer_closed(daemon, state, control_tx, message)
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
        ControlMessage::PluginResultCapacityReleased
        | ControlMessage::PluginCompletionPublished
        | ControlMessage::EntityPublishProgress
        | ControlMessage::CausalProgressPublished
        | ControlMessage::HostProgressPublished
        | ControlMessage::ManagedSessionSpawnQueued => {
            crate::daemon::owner_loop::publish_completion_wakes(daemon, state);
            false
        }
    }
}

#[cfg(test)]
pub(crate) fn handle_control_message(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    message: ControlMessage,
) -> bool {
    let shutdown = dispatch_control_message(daemon, state, transport_handle, control_tx, message);
    shutdown || crate::daemon::owner_loop::drive_ready_test_turn(daemon, state)
}

pub(crate) fn record_data_plane_progress(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
) -> bool {
    let Some(runtime) = daemon.runtime() else {
        return false;
    };
    let progress = runtime.take_data_plane_progress();
    if !progress.progressed && !progress.journal_advanced && !progress.terminal_inventory_changed {
        return false;
    }
    runtime.reap_detached_core_operations();
    if progress.journal_advanced {
        state.maintenance.note_journal_advanced();
        state.maintenance.note_authoritative_mutation();
    }
    if progress.terminal_inventory_changed {
        state.note_terminal_inventory_changed();
    }
    if state.maintenance_reads.in_flight() {
        state.maintenance.try_wake();
    }
    true
}

pub(crate) fn handle_control_request(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    control_tx: ControlSender,
    request: DaemonRequest,
) -> ControlStep {
    if let Some(response) = host_work::recovery_response(state, &request) {
        return ControlStep::ready(response);
    }
    if host_work::handles(&request) {
        return host_work::handle(daemon, state, request)
            .expect("a classified host request has a running owner waiter");
    }
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
            unreachable!("package requests use the host executor")
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
        | DaemonRequest::DeleteWorktree { .. } => {
            unreachable!("spawn-target and worktree requests use the host executor")
        }
        DaemonRequest::PluginLifecycleStatus => plugins::handle_request(daemon, request).into(),
        DaemonRequest::IssueLocalWebrtcBootstrap { .. }
        | DaemonRequest::LocalWebrtcSignal { .. } => {
            webrtc::handle_request(daemon, control_tx, request).into()
        }
        other => handle_runtime_control_request(daemon, state, observability, other),
    }
}

pub(crate) fn handle_runtime_control_request(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    request: DaemonRequest,
) -> ControlStep {
    match request {
        DaemonRequest::SubscribeEntities { .. } | DaemonRequest::UnsubscribeEntities { .. } => {
            entities::reject_json_request(request).into()
        }
        DaemonRequest::SubscribeEvents { .. } | DaemonRequest::UnsubscribeEvents { .. } => {
            events::reject_json_request(request).into()
        }
        DaemonRequest::Status
        | DaemonRequest::ListSessions
        | DaemonRequest::RemoveSession { .. }
        | DaemonRequest::Spawn { .. }
        | DaemonRequest::Attach { .. }
        | DaemonRequest::Detach { .. }
        | DaemonRequest::ShutdownSession { .. }
        | DaemonRequest::ReadScreen { .. }
        | DaemonRequest::ReadModeFlags { .. }
        | DaemonRequest::CaptureSnapshot { .. }
        | DaemonRequest::ReadSnapshotPage { .. }
        | DaemonRequest::ReadSessionContext { .. } => {
            sessions::handle_runtime(daemon, state, observability, request)
        }
        DaemonRequest::ListSessionTypes
        | DaemonRequest::ListSessionTypesForTarget { .. }
        | DaemonRequest::ShowSessionType { .. }
        | DaemonRequest::ShowSessionTypeDefinition { .. }
        | DaemonRequest::CreateSessionType { .. }
        | DaemonRequest::UpdateSessionType { .. }
        | DaemonRequest::DeleteSessionType { .. }
        | DaemonRequest::ResolveSessionType { .. } => {
            unreachable!("session-type reads and mutations use the host executor")
        }
        DaemonRequest::SpawnSessionType { .. } => {
            session_types::handle_runtime(daemon, state, observability, request)
        }
        DaemonRequest::Whoami { .. }
        | DaemonRequest::PostMessage { .. }
        | DaemonRequest::ReceiveMessages { .. }
        | DaemonRequest::AckMessage { .. }
        | DaemonRequest::NotifySession { .. } => {
            messaging::handle_runtime(daemon, state, observability, request)
        }
        DaemonRequest::PluginMcpListTools
        | DaemonRequest::PluginMcpCallTool { .. }
        | DaemonRequest::PluginSurfaceRender { .. }
        | DaemonRequest::PluginSurfaceAction { .. } => {
            plugins::handle_runtime(daemon, state, observability, request)
        }
        DaemonRequest::DaemonShutdown => {
            host::handle_runtime(daemon, state, observability, request)
        }
        DaemonRequest::IssueLocalWebrtcBootstrap { .. }
        | DaemonRequest::LocalWebrtcSignal { .. } => {
            ControlStep::Ready(Err(DaemonTransportError::UnexpectedResponse))
        }
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
