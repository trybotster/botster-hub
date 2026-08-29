//! Spawn-target and worktree request family.

use botster_hub_client::{
    DaemonEvent, DaemonRequest, DaemonResponse, DaemonWorktreeLifecycleEvent,
};

use crate::HubDaemon;
use crate::client_api_dto::response::{daemon_spawn_targets, daemon_worktrees};
use crate::client_api_dto::workspace::{worktree_failure_event, worktree_lifecycle_event};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, daemon_worktree_error};
use crate::persistence::{FileHubStateStore, HubStateStore};
use crate::{
    SpawnTarget, SpawnTargetCreate, SpawnTargetError, SpawnTargetUpdate, Worktree, WorktreeCreate,
};

pub(crate) fn handle_request(
    daemon: &mut HubDaemon,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::ListSpawnTargets => list_spawn_targets_response(daemon),
        DaemonRequest::ShowSpawnTarget { target_id } => {
            show_spawn_target_response(daemon, &target_id)
        }
        DaemonRequest::CreateSpawnTarget {
            target_id,
            label,
            root,
            enabled,
            kind,
            base_ref,
            metadata,
        } => create_spawn_target_response(
            daemon,
            SpawnTargetCreate {
                target_id,
                label,
                root,
                enabled,
                kind,
                base_ref,
                metadata,
            },
        ),
        DaemonRequest::UpdateSpawnTarget {
            target_id,
            label,
            root,
            enabled,
            kind,
            base_ref,
            metadata,
        } => update_spawn_target_response(
            daemon,
            target_id,
            SpawnTargetUpdate {
                label,
                root,
                enabled,
                kind,
                base_ref,
                metadata,
            },
        ),
        DaemonRequest::DeleteSpawnTarget { target_id } => {
            delete_spawn_target_response(daemon, target_id)
        }
        DaemonRequest::ValidateSpawnTarget { target_id } => {
            validate_spawn_target_response(daemon, &target_id)
        }
        DaemonRequest::ListWorktrees => list_worktrees_response(daemon),
        DaemonRequest::ShowWorktree { worktree_id } => show_worktree_response(daemon, &worktree_id),
        DaemonRequest::CreateWorktree {
            worktree_id,
            target_id,
            label,
            path,
            metadata,
        } => create_worktree_response(
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
            delete_worktree_response(daemon, &worktree_id)
        }
        _ => unreachable!("spawn-target family received a non-spawn-target request"),
    }
}

pub(crate) fn persist_spawn_targets(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<SpawnTarget>) -> crate::SpawnTargetResult<SpawnTarget>,
) -> DaemonTransportResult<SpawnTarget> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut changed = None;
    let state = store.update(&config, |state| {
        let target = update(&mut state.spawn_targets);
        changed = Some(target);
    })?;
    let target = changed
        .expect("spawn target update closure always runs")
        .map_err(DaemonTransportError::SpawnTarget)?;
    daemon.replace_state(state);
    Ok(target)
}

pub(crate) fn persist_spawn_targets_with_worktrees(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<SpawnTarget>, &[Worktree]) -> crate::SpawnTargetResult<SpawnTarget>,
) -> DaemonTransportResult<SpawnTarget> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut changed = None;
    let state = store.update(&config, |state| {
        let worktrees = state.worktrees.clone();
        changed = Some(update(&mut state.spawn_targets, &worktrees));
    })?;
    let target = changed
        .expect("spawn target update closure always runs")
        .map_err(DaemonTransportError::SpawnTarget)?;
    daemon.replace_state(state);
    Ok(target)
}

pub(crate) fn persist_worktrees(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<Worktree>, &[SpawnTarget]) -> crate::WorktreeResult<Worktree>,
) -> DaemonTransportResult<Worktree> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut changed = None;
    let state = store.update(&config, |state| {
        let targets = state.spawn_targets.clone();
        let worktree = update(&mut state.worktrees, &targets);
        changed = Some(worktree);
    })?;
    let worktree = changed
        .expect("worktree update closure always runs")
        .map_err(DaemonTransportError::Worktree)?;
    daemon.replace_state(state);
    Ok(worktree)
}

pub(crate) fn list_spawn_targets_response(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<DaemonResponse> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    Ok(daemon_spawn_targets(crate::list_spawn_targets(
        &runtime.state().spawn_targets,
    )))
}

pub(crate) fn show_spawn_target_response(
    daemon: &mut HubDaemon,
    target_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    Ok(daemon_spawn_targets(vec![crate::show_spawn_target(
        &runtime.state().spawn_targets,
        target_id,
    )?]))
}

pub(crate) fn mutate_spawn_targets_response(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<SpawnTarget>) -> crate::SpawnTargetResult<SpawnTarget>,
) -> DaemonTransportResult<DaemonResponse> {
    let target = persist_spawn_targets(daemon, update)?;
    Ok(daemon_spawn_targets(vec![target]))
}

pub(crate) fn mutate_spawn_targets_with_worktrees_response(
    daemon: &mut HubDaemon,
    update: impl FnOnce(&mut Vec<SpawnTarget>, &[Worktree]) -> crate::SpawnTargetResult<SpawnTarget>,
) -> DaemonTransportResult<DaemonResponse> {
    let target = persist_spawn_targets_with_worktrees(daemon, update)?;
    Ok(daemon_spawn_targets(vec![target]))
}

pub(crate) fn list_worktrees_response(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<DaemonResponse> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    Ok(daemon_worktrees(crate::list_worktrees(
        &runtime.state().worktrees,
        &runtime.state().spawn_targets,
    )))
}

pub(crate) fn show_worktree_response(
    daemon: &mut HubDaemon,
    worktree_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    Ok(daemon_worktrees(vec![crate::show_worktree(
        &runtime.state().worktrees,
        &runtime.state().spawn_targets,
        worktree_id,
    )?]))
}

pub(crate) fn create_worktree_response(
    daemon: &mut HubDaemon,
    request: WorktreeCreate,
) -> DaemonTransportResult<DaemonResponse> {
    let requested_worktree_id = request.worktree_id.clone();
    let requested_target_id = request.target_id.clone();
    match persist_worktrees(daemon, |worktrees, targets| {
        crate::create_worktree(worktrees, targets, request)
    }) {
        Ok(worktree) => {
            let event = worktree_lifecycle_event(
                "worktree_created",
                Some(&worktree),
                &daemon_targets(daemon),
                None,
            );
            let mut response = daemon_worktrees(vec![worktree]);
            emit_worktree_lifecycle_event(daemon, &mut response, event);
            Ok(response)
        }
        Err(DaemonTransportError::Worktree(error)) => {
            let event = worktree_failure_event(
                "worktree_create_failed",
                requested_worktree_id,
                Some(requested_target_id),
                &error,
            );
            let mut response = daemon_worktree_error(error);
            emit_worktree_lifecycle_event(daemon, &mut response, event);
            Ok(response)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn delete_worktree_response(
    daemon: &mut HubDaemon,
    worktree_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    match persist_worktrees(daemon, |worktrees, targets| {
        crate::delete_worktree(worktrees, targets, worktree_id)
    }) {
        Ok(worktree) => {
            let event = worktree_lifecycle_event(
                "worktree_deleted",
                Some(&worktree),
                &daemon_targets(daemon),
                None,
            );
            let mut response = daemon_worktrees(vec![worktree]);
            emit_worktree_lifecycle_event(daemon, &mut response, event);
            Ok(response)
        }
        Err(DaemonTransportError::Worktree(error)) => {
            let event = worktree_failure_event(
                "worktree_delete_failed",
                Some(worktree_id.to_string()),
                None,
                &error,
            );
            let mut response = daemon_worktree_error(error);
            emit_worktree_lifecycle_event(daemon, &mut response, event);
            Ok(response)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn daemon_targets(daemon: &HubDaemon) -> Vec<SpawnTarget> {
    daemon
        .runtime()
        .map(|runtime| runtime.state().spawn_targets.clone())
        .unwrap_or_default()
}

pub(crate) fn emit_worktree_lifecycle_event(
    daemon: &HubDaemon,
    response: &mut DaemonResponse,
    event: DaemonWorktreeLifecycleEvent,
) {
    if let Some(runtime) = daemon.runtime()
        && let Ok(payload) = serde_json::to_value(&event)
    {
        let _ = runtime.package_event_router().try_ingress(
            crate::package_event_router::HUB_EVENT_OWNER,
            &event.event,
            &payload,
            std::time::Instant::now(),
        );
        if runtime.package_event_router().peek_delivery_wake() {
            // Delivery is owner-loop work. The mutating response does not wait.
        }
    }
    response
        .events
        .push(DaemonEvent::WorktreeLifecycle { event });
}

pub(crate) fn create_spawn_target_response(
    daemon: &mut HubDaemon,
    request: SpawnTargetCreate,
) -> DaemonTransportResult<DaemonResponse> {
    // Only pre-check session-types once the root is known to be a directory.
    // Non-directory roots must fall through to create_spawn_target's
    // root_not_directory rather than a misleading invalid_repo_session_types.
    if request.enabled && request.root.is_dir() {
        super::session_types::ensure_repo_session_types_valid_for_enabled_root(&request.root)?;
    }
    let before_session_types = super::session_types::session_type_definition_map(daemon)?;
    let response = mutate_spawn_targets_response(daemon, |targets| {
        crate::create_spawn_target(targets, request)
    })?;
    super::session_types::advance_session_type_generation_if_changed(
        daemon,
        &before_session_types,
    )?;
    Ok(response)
}

pub(crate) fn update_spawn_target_response(
    daemon: &mut HubDaemon,
    target_id: String,
    request: SpawnTargetUpdate,
) -> DaemonTransportResult<DaemonResponse> {
    let recovery_disable = request.enabled == Some(false);
    if !recovery_disable {
        super::session_types::ensure_update_would_not_enable_invalid_repo_session_types(
            daemon,
            &target_id,
            request.root.as_ref(),
            request.enabled,
        )?;
    }
    let before_session_types = match super::session_types::session_type_definition_map(daemon) {
        Ok(before) => Some(before),
        Err(error)
            if recovery_disable
                && super::session_types::is_invalid_repo_session_types_error(&error) =>
        {
            None
        }
        Err(error) => return Err(error),
    };
    let response = mutate_spawn_targets_with_worktrees_response(daemon, |targets, worktrees| {
        if request.kind.as_deref().is_some_and(|kind| kind != "git")
            && worktrees.iter().any(|worktree| {
                worktree.target_id == target_id && worktree.management == "hub_managed_git"
            })
        {
            return Err(SpawnTargetError::new(
                "managed_worktrees_exist",
                "Git target cannot be reclassified while managed worktrees reference it",
            ));
        }
        crate::update_spawn_target(targets, &target_id, request)
    })?;
    match before_session_types {
        Some(before) => {
            super::session_types::advance_session_type_generation_if_changed(daemon, &before)?;
        }
        None => {
            super::session_types::force_advance_session_type_generation(daemon)?;
        }
    }
    Ok(response)
}

pub(crate) fn delete_spawn_target_response(
    daemon: &mut HubDaemon,
    target_id: String,
) -> DaemonTransportResult<DaemonResponse> {
    let before_session_types = match super::session_types::session_type_definition_map(daemon) {
        Ok(before) => Some(before),
        Err(error) if super::session_types::is_invalid_repo_session_types_error(&error) => None,
        Err(error) => return Err(error),
    };
    let response = mutate_spawn_targets_with_worktrees_response(daemon, |targets, worktrees| {
        if worktrees.iter().any(|worktree| {
            worktree.target_id == target_id && worktree.management == "hub_managed_git"
        }) {
            return Err(SpawnTargetError::new(
                "managed_worktrees_exist",
                "Git target cannot be deleted while managed worktrees reference it",
            ));
        }
        crate::delete_spawn_target(targets, &target_id)
    })?;
    match before_session_types {
        Some(before) => {
            super::session_types::advance_session_type_generation_if_changed(daemon, &before)?;
        }
        None => {
            super::session_types::force_advance_session_type_generation(daemon)?;
        }
    }
    Ok(response)
}

pub(crate) fn validate_spawn_target_response(
    daemon: &mut HubDaemon,
    target_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    Ok(
        crate::client_api_dto::response::daemon_spawn_target_validation(
            crate::validate_spawn_target(
                &daemon
                    .runtime()
                    .ok_or(DaemonTransportError::DaemonNotRunning)?
                    .state()
                    .spawn_targets,
                target_id,
            ),
        ),
    )
}
