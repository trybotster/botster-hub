use botster_hub_client::{
    DaemonSpawnTarget, DaemonWorktree, DaemonWorktreeGitMetadata, DaemonWorktreeLifecycleEvent,
};

use crate::{SpawnTarget, Worktree, WorktreeError};

pub(crate) fn daemon_spawn_target(target: SpawnTarget) -> DaemonSpawnTarget {
    DaemonSpawnTarget {
        target_id: target.target_id,
        label: target.label,
        root: target.root,
        enabled: target.enabled,
        kind: target.kind,
        base_ref: target.base_ref,
        metadata: target.metadata,
    }
}

pub(crate) fn worktree_lifecycle_event(
    event: &str,
    worktree: Option<&Worktree>,
    targets: &[SpawnTarget],
    failure: Option<(&str, &str)>,
) -> DaemonWorktreeLifecycleEvent {
    DaemonWorktreeLifecycleEvent {
        event: event.to_string(),
        worktree_id: worktree.map(|worktree| worktree.worktree_id.clone()),
        target_id: worktree.map(|worktree| worktree.target_id.clone()),
        status: worktree.map(|worktree| worktree.status.clone()),
        label: worktree.map(|worktree| worktree.label.clone()),
        display_path: worktree
            .and_then(|worktree| sanitized_worktree_display_path(worktree, targets)),
        failure_kind: failure.map(|(kind, _)| kind.to_string()),
        message: failure.map(|(_, message)| message.to_string()),
    }
}

pub(crate) fn worktree_failure_event(
    event: &str,
    worktree_id: Option<String>,
    target_id: Option<String>,
    error: &WorktreeError,
) -> DaemonWorktreeLifecycleEvent {
    DaemonWorktreeLifecycleEvent {
        event: event.to_string(),
        worktree_id,
        target_id,
        status: None,
        label: None,
        display_path: None,
        failure_kind: Some(error.kind.to_string()),
        message: Some(sanitize_worktree_error_message(&error.message)),
    }
}

pub(crate) fn sanitized_worktree_display_path(
    worktree: &Worktree,
    targets: &[SpawnTarget],
) -> Option<String> {
    let target = targets
        .iter()
        .find(|target| target.target_id == worktree.target_id)?;
    let relative = worktree.path.strip_prefix(&target.root).ok()?;
    if relative.as_os_str().is_empty() {
        None
    } else {
        Some(relative.to_string_lossy().into_owned())
    }
}

pub(crate) fn sanitize_worktree_error_message(message: &str) -> String {
    if message.contains('/') {
        "worktree operation failed".to_string()
    } else {
        message.to_string()
    }
}

pub(crate) fn daemon_worktree(worktree: Worktree) -> DaemonWorktree {
    DaemonWorktree {
        worktree_id: worktree.worktree_id,
        target_id: worktree.target_id,
        label: worktree.label,
        path: worktree.path,
        status: worktree.status,
        management: worktree.management,
        git: worktree.git.map(|git| DaemonWorktreeGitMetadata {
            repository_root: git.repository_root,
            branch: git.branch,
            head: git.head,
        }),
        metadata: worktree.metadata,
    }
}
