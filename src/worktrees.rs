//! Hub-owned worktree registry over admitted spawn targets.
//!
//! Worktrees are generic working-directory references scoped by spawn target
//! policy. Git metadata is opportunistic; admission does not require git.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::spawn_targets::SpawnTarget;

/// Persisted hub-owned worktree row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Worktree {
    /// Stable id used by clients and plugins.
    pub worktree_id: String,
    /// Spawn target that owns the root policy for this row.
    pub target_id: String,
    /// Human-facing display label.
    #[serde(default)]
    pub label: String,
    /// Admitted local directory path under the spawn target root.
    pub path: PathBuf,
    /// Reconciled path status. Current values are `present`, `missing`, and `stale`.
    #[serde(default = "default_present_status")]
    pub status: String,
    /// Row ownership. Existing records default to externally registered directories.
    #[serde(default = "default_registered_management")]
    pub management: String,
    /// Optional git metadata detected when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<WorktreeGitMetadata>,
    /// Small sanitized metadata for clients/plugins.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl Worktree {
    /// Return a client-facing copy with defaults filled and filesystem status reconciled.
    #[must_use]
    pub fn reconciled(mut self, targets: &[SpawnTarget]) -> Self {
        if self.label.trim().is_empty() {
            self.label = self.worktree_id.clone();
        }
        self.status = reconcile_status(&self, targets).to_string();
        self
    }
}

/// Opportunistic git metadata for a worktree path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeGitMetadata {
    /// Repository root when a `.git` directory or file is found.
    pub repository_root: PathBuf,
    /// Current branch name when HEAD is a symbolic ref.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Current HEAD value when readable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
}

/// Create request accepted by daemon callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeCreate {
    pub worktree_id: Option<String>,
    pub target_id: String,
    pub label: Option<String>,
    pub path: PathBuf,
    pub metadata: BTreeMap<String, String>,
}

/// Registry policy errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeError {
    pub kind: &'static str,
    pub message: String,
}

impl WorktreeError {
    fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for WorktreeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for WorktreeError {}

pub type WorktreeResult<T> = Result<T, WorktreeError>;

fn default_present_status() -> String {
    "present".to_string()
}

fn default_registered_management() -> String {
    "registered".to_string()
}

/// Return all worktrees in deterministic id order with status reconciled.
#[must_use]
pub fn list_worktrees(worktrees: &[Worktree], targets: &[SpawnTarget]) -> Vec<Worktree> {
    let mut worktrees = worktrees
        .iter()
        .cloned()
        .map(|worktree| worktree.reconciled(targets))
        .collect::<Vec<_>>();
    worktrees.sort_by(|left, right| left.worktree_id.cmp(&right.worktree_id));
    worktrees
}

/// Return one worktree by id with status reconciled.
pub fn show_worktree(
    worktrees: &[Worktree],
    targets: &[SpawnTarget],
    worktree_id: &str,
) -> WorktreeResult<Worktree> {
    worktrees
        .iter()
        .find(|worktree| worktree.worktree_id == worktree_id)
        .cloned()
        .map(|worktree| worktree.reconciled(targets))
        .ok_or_else(|| WorktreeError::new("not_found", "worktree was not found"))
}

/// Insert one worktree.
pub fn create_worktree(
    worktrees: &mut Vec<Worktree>,
    targets: &[SpawnTarget],
    request: WorktreeCreate,
) -> WorktreeResult<Worktree> {
    let worktree_id = request.worktree_id.unwrap_or_else(generated_worktree_id);
    validate_worktree_id(&worktree_id)?;
    if worktrees
        .iter()
        .any(|worktree| worktree.worktree_id == worktree_id)
    {
        return Err(WorktreeError::new(
            "duplicate_worktree",
            "worktree id already exists",
        ));
    }
    let target = targets
        .iter()
        .find(|target| target.target_id == request.target_id)
        .ok_or_else(|| WorktreeError::new("target_not_found", "spawn target was not found"))?;
    if !target.enabled {
        return Err(WorktreeError::new(
            "target_disabled",
            "spawn target is disabled",
        ));
    }
    validate_metadata(&request.metadata)?;
    let path = normalize_worktree_path(&target.root, request.path)?;
    let label = request.label.unwrap_or_else(|| worktree_id.clone());
    let worktree = Worktree {
        worktree_id,
        target_id: target.target_id.clone(),
        label,
        path: path.clone(),
        status: "present".to_string(),
        management: default_registered_management(),
        git: detect_git_metadata(&path),
        metadata: request.metadata,
    }
    .reconciled(targets);
    worktrees.push(worktree.clone());
    Ok(worktree)
}

/// Remove one worktree record without deleting filesystem contents.
pub fn delete_worktree(
    worktrees: &mut Vec<Worktree>,
    targets: &[SpawnTarget],
    worktree_id: &str,
) -> WorktreeResult<Worktree> {
    let position = worktrees
        .iter()
        .position(|worktree| worktree.worktree_id == worktree_id)
        .ok_or_else(|| WorktreeError::new("not_found", "worktree was not found"))?;
    Ok(worktrees.remove(position).reconciled(targets))
}

fn validate_worktree_id(worktree_id: &str) -> WorktreeResult<()> {
    let valid = !worktree_id.trim().is_empty()
        && worktree_id.bytes().all(|byte| {
            byte == b'_' || byte == b'-' || byte == b':' || byte.is_ascii_alphanumeric()
        });
    if valid {
        Ok(())
    } else {
        Err(WorktreeError::new(
            "invalid_worktree_id",
            "worktree id contains unsupported characters",
        ))
    }
}

fn normalize_worktree_path(target_root: &Path, path: PathBuf) -> WorktreeResult<PathBuf> {
    let root = target_root.canonicalize().map_err(|error| {
        WorktreeError::new(
            "target_root_unavailable",
            format!("spawn target root could not be resolved: {error}"),
        )
    })?;
    let candidate = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    if !candidate.is_dir() {
        return Err(WorktreeError::new(
            "path_not_directory",
            "worktree path must be an existing directory",
        ));
    }
    let canonical = candidate.canonicalize().map_err(|error| {
        WorktreeError::new(
            "path_not_directory",
            format!("worktree path could not be resolved: {error}"),
        )
    })?;
    if !canonical.starts_with(&root) {
        return Err(WorktreeError::new(
            "path_outside_target",
            "worktree path must stay under the spawn target root",
        ));
    }
    Ok(canonical)
}

fn reconcile_status(worktree: &Worktree, targets: &[SpawnTarget]) -> &'static str {
    let Some(target) = targets
        .iter()
        .find(|target| target.target_id == worktree.target_id)
    else {
        return "stale";
    };
    if worktree.management == "hub_managed_git" {
        return crate::managed_git_worktrees::reconcile_managed_worktree(worktree, target);
    }
    let Ok(root) = target.root.canonicalize() else {
        return "stale";
    };
    let Ok(path) = worktree.path.canonicalize() else {
        return "missing";
    };
    if path.is_dir() && path.starts_with(root) {
        "present"
    } else {
        "stale"
    }
}

fn detect_git_metadata(path: &Path) -> Option<WorktreeGitMetadata> {
    let git_path = path.join(".git");
    if !git_path.exists() {
        return None;
    }
    let head_path = if git_path.is_dir() {
        git_path.join("HEAD")
    } else {
        git_path.clone()
    };
    let head = fs::read_to_string(head_path)
        .ok()
        .map(|head| head.trim().to_string())
        .filter(|head| !head.is_empty());
    let branch = head
        .as_deref()
        .and_then(|head| head.strip_prefix("ref: refs/heads/"))
        .map(ToString::to_string);
    Some(WorktreeGitMetadata {
        repository_root: path.to_path_buf(),
        branch,
        head,
    })
}

fn validate_metadata(metadata: &BTreeMap<String, String>) -> WorktreeResult<()> {
    for key in metadata.keys() {
        if key.trim().is_empty() {
            return Err(WorktreeError::new(
                "invalid_metadata",
                "worktree metadata keys must not be empty",
            ));
        }
    }
    Ok(())
}

fn generated_worktree_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("wt_{nanos:x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(root: PathBuf) -> SpawnTarget {
        SpawnTarget {
            target_id: "tgt".to_string(),
            label: "Target".to_string(),
            root,
            enabled: true,
            kind: "directory".to_string(),
            base_ref: None,
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn status_marks_missing_and_stale_without_failing() {
        let root = std::env::temp_dir().join(format!(
            "botster-worktree-status-{}",
            generated_worktree_id()
        ));
        let child = root.join("child");
        fs::create_dir_all(&child).expect("create child");
        let worktree = Worktree {
            worktree_id: "wt".to_string(),
            target_id: "tgt".to_string(),
            label: "Worktree".to_string(),
            path: child.clone(),
            status: "present".to_string(),
            management: "registered".to_string(),
            git: None,
            metadata: BTreeMap::new(),
        };

        assert_eq!(
            worktree.clone().reconciled(&[target(root.clone())]).status,
            "present"
        );
        fs::remove_dir_all(&child).expect("remove child");
        assert_eq!(
            worktree.clone().reconciled(&[target(root.clone())]).status,
            "missing"
        );
        assert_eq!(worktree.reconciled(&[]).status, "stale");
        let _ = fs::remove_dir_all(root);
    }
}
