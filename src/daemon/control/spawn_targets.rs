//! Spawn-target and worktree request family.

use botster_hub_client::{DaemonEvent, DaemonResponse, DaemonWorktreeLifecycleEvent};

use crate::HubDaemon;
use crate::client_api_dto::response::{daemon_spawn_targets, daemon_worktrees};
use crate::client_api_dto::workspace::{worktree_failure_event, worktree_lifecycle_event};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, daemon_worktree_error};
use crate::persistence::{FileHubStateStore, HubStateStore};
use crate::{SpawnTarget, Worktree, WorktreeCreate};

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
