use botster_core::{SessionId, SessionLifecycleState};
use botster_core_daemon::GuardedWriteDeliveryState;
use botster_hub_client::{
    DaemonEvent, DaemonSession, DaemonSessionType, DaemonSessionTypeContextInput,
    DaemonSessionTypeDefinition, DaemonSessionTypeMutationSource, DaemonSessionTypeRequest,
    DaemonSessionTypeWorkingDirectory,
};

use crate::{
    HubClientEvent, HubClientSession, PackageSessionType, PackageSessionTypeWorkingDirectory,
    SessionTypeContextInput, SessionTypeMutationSource, SessionTypeRequest,
};

pub(crate) fn daemon_session_type_from_client(
    template: crate::HubSessionType,
) -> DaemonSessionType {
    DaemonSessionType {
        session_type_id: template.session_type_id,
        source_name: template.source_name,
        id: template.id,
        source: template.source,
        editable: template.editable,
        overridden_sources: template
            .overridden_sources
            .into_iter()
            .map(|source| botster_hub_client::DaemonSessionTypeSource {
                kind: source.kind,
                name: source.name,
            })
            .collect(),
        diagnostics: template.diagnostics,
        label: template.label,
        description: template.description,
        icon: template.icon,
        role: template.role,
        interaction: template.interaction,
        traits: template.traits,
        lifecycle: template.lifecycle,
        execution: match template.execution {
            crate::PackageSessionTypeExecution::RelativeExecutable => {
                botster_hub_client::DaemonSessionTypeExecution::RelativeExecutable
            }
            crate::PackageSessionTypeExecution::ShellCommand => {
                botster_hub_client::DaemonSessionTypeExecution::ShellCommand
            }
        },
        command: template.command,
        args: template.args,
        working_directory_policy: template.working_directory_policy,
        allowed_environment_overrides: template.allowed_environment_overrides,
        context_keys: template.context_keys,
        target_id: template.target_id,
        available: template.available,
    }
}

pub(crate) fn session_type_request_from_daemon(
    session_id: Option<SessionId>,
    request: DaemonSessionTypeRequest,
) -> SessionTypeRequest {
    SessionTypeRequest {
        target_id: request.target_id,
        session_id,
        cwd: request.cwd,
        environment: request.environment,
        context: session_type_context_from_daemon(request.context),
    }
}

pub(crate) fn session_type_mutation_source_from_daemon(
    source: DaemonSessionTypeMutationSource,
) -> SessionTypeMutationSource {
    match source {
        DaemonSessionTypeMutationSource::Device => SessionTypeMutationSource::Device,
        DaemonSessionTypeMutationSource::Repo { target_id } => {
            SessionTypeMutationSource::Repo { target_id }
        }
        DaemonSessionTypeMutationSource::Package { package_name } => {
            SessionTypeMutationSource::Package { package_name }
        }
    }
}

pub(crate) fn daemon_session_type_mutation_source(
    source: SessionTypeMutationSource,
) -> DaemonSessionTypeMutationSource {
    match source {
        SessionTypeMutationSource::Device => DaemonSessionTypeMutationSource::Device,
        SessionTypeMutationSource::Repo { target_id } => {
            DaemonSessionTypeMutationSource::Repo { target_id }
        }
        SessionTypeMutationSource::Package { package_name } => {
            DaemonSessionTypeMutationSource::Package { package_name }
        }
    }
}

pub(crate) fn daemon_session_type_definition_from_client(
    definition: PackageSessionType,
) -> DaemonSessionTypeDefinition {
    DaemonSessionTypeDefinition {
        id: definition.id,
        label: definition.label,
        description: definition.description,
        icon: definition.icon,
        role: definition.role,
        interaction: definition.interaction,
        traits: definition.traits,
        lifecycle: definition.lifecycle,
        execution: match definition.execution {
            crate::PackageSessionTypeExecution::RelativeExecutable => {
                botster_hub_client::DaemonSessionTypeExecution::RelativeExecutable
            }
            crate::PackageSessionTypeExecution::ShellCommand => {
                botster_hub_client::DaemonSessionTypeExecution::ShellCommand
            }
        },
        command: definition.command,
        args: definition.args,
        working_directory: match definition.working_directory {
            PackageSessionTypeWorkingDirectory::PackageRoot => {
                DaemonSessionTypeWorkingDirectory::PackageRoot
            }
            PackageSessionTypeWorkingDirectory::Relative { path } => {
                DaemonSessionTypeWorkingDirectory::Relative { path }
            }
        },
        environment: definition.environment,
        allowed_environment_overrides: definition.allowed_environment_overrides,
        context: definition.context,
        target_id: definition.target_id,
    }
}

pub(crate) fn session_type_definition_from_daemon(
    definition: DaemonSessionTypeDefinition,
) -> PackageSessionType {
    PackageSessionType {
        id: definition.id,
        label: definition.label,
        description: definition.description,
        icon: definition.icon,
        role: definition.role,
        interaction: definition.interaction,
        traits: definition.traits,
        lifecycle: definition.lifecycle,
        execution: match definition.execution {
            botster_hub_client::DaemonSessionTypeExecution::RelativeExecutable => {
                crate::PackageSessionTypeExecution::RelativeExecutable
            }
            botster_hub_client::DaemonSessionTypeExecution::ShellCommand => {
                crate::PackageSessionTypeExecution::ShellCommand
            }
        },
        command: definition.command,
        args: definition.args,
        working_directory: match definition.working_directory {
            DaemonSessionTypeWorkingDirectory::PackageRoot => {
                PackageSessionTypeWorkingDirectory::PackageRoot
            }
            DaemonSessionTypeWorkingDirectory::Relative { path } => {
                PackageSessionTypeWorkingDirectory::Relative { path }
            }
        },
        environment: definition.environment,
        allowed_environment_overrides: definition.allowed_environment_overrides,
        context: definition.context,
        target_id: definition.target_id,
    }
}

pub(crate) fn session_type_context_from_daemon(
    context: DaemonSessionTypeContextInput,
) -> SessionTypeContextInput {
    SessionTypeContextInput {
        worktree_path: context.worktree_path,
        repo_path: context.repo_path,
        branch_name: context.branch_name,
        prompt: context.prompt,
        ticket_id: context.ticket_id,
        workspace_id: context.workspace_id,
        metadata: context.metadata,
    }
}

pub(crate) fn daemon_session_from_client(session: HubClientSession) -> DaemonSession {
    DaemonSession {
        session_id: session.session_id.0,
        lifecycle: lifecycle_label(&session.lifecycle).to_string(),
    }
}

pub(crate) fn daemon_event_from_client(event: HubClientEvent) -> DaemonEvent {
    match event {
        HubClientEvent::SessionLifecycle { session_id, state } => DaemonEvent::SessionLifecycle {
            session_id: session_id.0,
            state: lifecycle_label(&state).to_string(),
        },
        HubClientEvent::RuntimeObservation { kind } => DaemonEvent::RuntimeObservation {
            kind: match kind {
                crate::HubClientObservationKind::SessionActivity => "session_activity",
                crate::HubClientObservationKind::Subscription => "subscription",
                crate::HubClientObservationKind::Backpressure => "backpressure",
                crate::HubClientObservationKind::RoutedEnvelope => "routed_envelope",
            }
            .to_string(),
        },
    }
}

pub(crate) fn guarded_write_delivery_state_label(state: GuardedWriteDeliveryState) -> &'static str {
    match state {
        GuardedWriteDeliveryState::Accepted => "accepted",
        GuardedWriteDeliveryState::Deferred => "deferred",
        GuardedWriteDeliveryState::Rejected => "rejected",
        GuardedWriteDeliveryState::Written => "written",
        GuardedWriteDeliveryState::Delivered => "delivered",
        GuardedWriteDeliveryState::Acknowledged => "acknowledged",
    }
}

pub(crate) fn lifecycle_label(state: &SessionLifecycleState) -> &'static str {
    match state {
        SessionLifecycleState::Starting => "starting",
        SessionLifecycleState::Running => "running",
        SessionLifecycleState::Stopping => "stopping",
        SessionLifecycleState::Exited { .. } => "exited",
        SessionLifecycleState::Failed { .. } => "failed",
    }
}
