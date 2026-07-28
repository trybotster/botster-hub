//! Hub-owned spawn target registry.
//!
//! Spawn targets are admitted hub policy state. They are not botster-core
//! concepts, and plugins reference their stable ids instead of resolving local
//! filesystem paths themselves.

use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Persisted hub-owned spawn target row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnTarget {
    /// Stable id used by clients and plugins.
    pub target_id: String,
    /// Human-facing display label.
    #[serde(default)]
    pub label: String,
    /// Admitted local directory root.
    pub root: PathBuf,
    /// Disabled targets stay persisted but are unavailable for spawning.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Generic target kind. Initial registry admits plain directories.
    #[serde(default = "default_directory_kind")]
    pub kind: String,
    /// Hub-owned base ref used by managed Git worktree creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    /// Small sanitized metadata for clients/plugins.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl SpawnTarget {
    /// Return a client-facing copy with defaults filled.
    #[must_use]
    pub fn normalized(mut self) -> Self {
        if self.label.trim().is_empty() {
            self.label = self.target_id.clone();
        }
        if self.kind.trim().is_empty() {
            self.kind = default_directory_kind();
        }
        self
    }
}

/// Create request accepted by daemon/CLI callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnTargetCreate {
    pub target_id: Option<String>,
    pub label: Option<String>,
    pub root: PathBuf,
    pub enabled: bool,
    pub kind: Option<String>,
    pub base_ref: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

/// Update request accepted by daemon/CLI callers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpawnTargetUpdate {
    pub label: Option<String>,
    pub root: Option<PathBuf>,
    pub enabled: Option<bool>,
    pub kind: Option<String>,
    pub base_ref: Option<Option<String>>,
    pub metadata: Option<BTreeMap<String, String>>,
}

/// Structured validation result used by plugin APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnTargetValidation {
    pub target_id: String,
    pub ok: bool,
    pub status: String,
}

/// Registry policy errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnTargetError {
    pub kind: &'static str,
    pub message: String,
}

impl SpawnTargetError {
    pub(crate) fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for SpawnTargetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for SpawnTargetError {}

pub type SpawnTargetResult<T> = Result<T, SpawnTargetError>;

#[must_use]
pub const fn default_true() -> bool {
    true
}

fn default_directory_kind() -> String {
    "directory".to_string()
}

const SPAWN_TARGET_GIT_ADMISSION_TIMEOUT: Duration = Duration::from_secs(2);

/// Return all targets in deterministic id order.
#[must_use]
pub fn list_spawn_targets(targets: &[SpawnTarget]) -> Vec<SpawnTarget> {
    let mut targets = targets
        .iter()
        .cloned()
        .map(SpawnTarget::normalized)
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| left.target_id.cmp(&right.target_id));
    targets
}

/// Return one target by id.
pub fn show_spawn_target(
    targets: &[SpawnTarget],
    target_id: &str,
) -> SpawnTargetResult<SpawnTarget> {
    targets
        .iter()
        .find(|target| target.target_id == target_id)
        .cloned()
        .map(SpawnTarget::normalized)
        .ok_or_else(|| SpawnTargetError::new("not_found", "spawn target was not found"))
}

/// Validate a target reference for plugin/runtime use.
#[must_use]
pub fn validate_spawn_target(targets: &[SpawnTarget], target_id: &str) -> SpawnTargetValidation {
    match targets.iter().find(|target| target.target_id == target_id) {
        Some(target) if target.enabled => SpawnTargetValidation {
            target_id: target_id.to_string(),
            ok: true,
            status: "ok".to_string(),
        },
        Some(_) => SpawnTargetValidation {
            target_id: target_id.to_string(),
            ok: false,
            status: "disabled".to_string(),
        },
        None => SpawnTargetValidation {
            target_id: target_id.to_string(),
            ok: false,
            status: "not_found".to_string(),
        },
    }
}

/// Insert one target.
pub fn create_spawn_target(
    targets: &mut Vec<SpawnTarget>,
    request: SpawnTargetCreate,
) -> SpawnTargetResult<SpawnTarget> {
    let target_id = request.target_id.unwrap_or_else(generated_target_id);
    validate_target_id(&target_id)?;
    if targets.iter().any(|target| target.target_id == target_id) {
        return Err(SpawnTargetError::new(
            "duplicate_target",
            "spawn target id already exists",
        ));
    }
    let root = normalize_root(request.root)?;
    let label = request.label.unwrap_or_else(|| target_id.clone());
    let kind = request.kind.unwrap_or_else(default_directory_kind);
    validate_requested_kind(&kind)?;
    let base_ref = admitted_git_base_ref(&root, &kind, request.base_ref, true)?;
    validate_metadata(&request.metadata)?;
    let target = SpawnTarget {
        target_id,
        label,
        root,
        enabled: request.enabled,
        kind,
        base_ref,
        metadata: request.metadata,
    }
    .normalized();
    targets.push(target.clone());
    Ok(target)
}

/// Update one target.
pub fn update_spawn_target(
    targets: &mut [SpawnTarget],
    target_id: &str,
    request: SpawnTargetUpdate,
) -> SpawnTargetResult<SpawnTarget> {
    let target = targets
        .iter_mut()
        .find(|target| target.target_id == target_id)
        .ok_or_else(|| SpawnTargetError::new("not_found", "spawn target was not found"))?;
    let label = request.label.unwrap_or_else(|| target.label.clone());
    let root = request
        .root
        .map(normalize_root)
        .transpose()?
        .unwrap_or_else(|| target.root.clone());
    let enabled = request.enabled.unwrap_or(target.enabled);
    if let Some(kind) = request.kind.as_deref() {
        validate_requested_kind(kind)?;
    }
    let kind = request.kind.unwrap_or_else(|| target.kind.clone());
    let default_base_ref = target.kind != "git" && kind == "git" && request.base_ref.is_none();
    let requested_base_ref = match request.base_ref {
        Some(base_ref) => base_ref,
        None => target.base_ref.clone(),
    };
    let base_ref = admitted_git_base_ref(&root, &kind, requested_base_ref, default_base_ref)?;
    let metadata = request.metadata.unwrap_or_else(|| target.metadata.clone());
    validate_metadata(&metadata)?;
    target.label = label;
    target.root = root;
    target.enabled = enabled;
    target.kind = kind;
    target.base_ref = base_ref;
    target.metadata = metadata;
    *target = target.clone().normalized();
    Ok(target.clone())
}

/// Remove one target.
pub fn delete_spawn_target(
    targets: &mut Vec<SpawnTarget>,
    target_id: &str,
) -> SpawnTargetResult<SpawnTarget> {
    let position = targets
        .iter()
        .position(|target| target.target_id == target_id)
        .ok_or_else(|| SpawnTargetError::new("not_found", "spawn target was not found"))?;
    Ok(targets.remove(position).normalized())
}

fn validate_target_id(target_id: &str) -> SpawnTargetResult<()> {
    let valid = !target_id.trim().is_empty()
        && target_id.bytes().all(|byte| {
            byte == b'_' || byte == b'-' || byte == b':' || byte.is_ascii_alphanumeric()
        });
    if valid {
        Ok(())
    } else {
        Err(SpawnTargetError::new(
            "invalid_target_id",
            "spawn target id contains unsupported characters",
        ))
    }
}

fn normalize_root(root: PathBuf) -> SpawnTargetResult<PathBuf> {
    if !root.is_dir() {
        return Err(SpawnTargetError::new(
            "root_not_directory",
            "spawn target root must be an existing directory",
        ));
    }
    root.canonicalize().map_err(|error| {
        SpawnTargetError::new(
            "root_not_directory",
            format!("spawn target root could not be resolved: {error}"),
        )
    })
}

fn validate_requested_kind(kind: &str) -> SpawnTargetResult<()> {
    if matches!(kind, "directory" | "git") {
        Ok(())
    } else {
        Err(SpawnTargetError::new(
            "invalid_target_kind",
            "spawn target kind must be directory or git",
        ))
    }
}

fn admitted_git_base_ref(
    root: &Path,
    kind: &str,
    requested: Option<String>,
    default_from_head: bool,
) -> SpawnTargetResult<Option<String>> {
    admitted_git_base_ref_using(
        OsStr::new("git"),
        root,
        kind,
        requested,
        default_from_head,
        Instant::now() + SPAWN_TARGET_GIT_ADMISSION_TIMEOUT,
    )
}

fn admitted_git_base_ref_using(
    executable: &OsStr,
    root: &Path,
    kind: &str,
    requested: Option<String>,
    default_from_head: bool,
    deadline: Instant,
) -> SpawnTargetResult<Option<String>> {
    if kind != "git" {
        return Ok(None);
    }
    admission_git_output(
        executable,
        root,
        &["rev-parse", "--is-inside-work-tree"],
        deadline,
        "repository_unavailable",
    )
    .and_then(|inside| {
        if inside.trim() != "true" {
            return Err(SpawnTargetError::new(
                "repository_unavailable",
                "Git spawn target root is not a usable repository",
            ));
        }
        let base_ref = match requested {
            Some(base_ref) if !base_ref.trim().is_empty() => base_ref,
            Some(_) => {
                return Err(SpawnTargetError::new(
                    "base_ref_required",
                    "Git spawn target requires a non-empty base ref",
                ));
            }
            None if default_from_head => admission_git_output(
                executable,
                root,
                &["symbolic-ref", "--quiet", "--short", "HEAD"],
                deadline,
                "base_ref_required",
            )
            .map_err(|_| {
                SpawnTargetError::new(
                    "base_ref_required",
                    "Git spawn target requires an explicit base ref when HEAD is detached",
                )
            })?
            .trim()
            .to_string(),
            None => {
                return Err(SpawnTargetError::new(
                    "base_ref_required",
                    "Git spawn target requires a stored base ref",
                ));
            }
        };
        admission_git_output(
            executable,
            root,
            &["rev-parse", "--verify", &format!("{base_ref}^{{commit}}")],
            deadline,
            "invalid_base_ref",
        )
        .map_err(|_| {
            SpawnTargetError::new(
                "invalid_base_ref",
                "Git spawn target base ref does not resolve to a commit",
            )
        })?;
        Ok(Some(base_ref))
    })
}

fn admission_git_output(
    executable: &OsStr,
    root: &Path,
    args: &[&str],
    deadline: Instant,
    failure_kind: &'static str,
) -> SpawnTargetResult<String> {
    crate::managed_git_worktrees::git_stdout_using(
        executable,
        Some(root),
        args,
        deadline,
        failure_kind,
    )
    .map_err(|error| {
        if error.kind == "git_unavailable" {
            SpawnTargetError::new(
                "git_unavailable",
                "Git is unavailable for spawn target admission",
            )
        } else if error.kind == "ensure_timed_out" {
            SpawnTargetError::new(
                "repository_unavailable",
                "Git spawn target repository validation timed out",
            )
        } else {
            SpawnTargetError::new(failure_kind, error.message)
        }
    })
}

fn validate_metadata(metadata: &BTreeMap<String, String>) -> SpawnTargetResult<()> {
    for key in metadata.keys() {
        if key.trim().is_empty() {
            return Err(SpawnTargetError::new(
                "invalid_metadata",
                "spawn target metadata keys must not be empty",
            ));
        }
    }
    Ok(())
}

fn generated_target_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("tgt_{nanos:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    #[test]
    fn validates_missing_disabled_and_enabled_targets() {
        let targets = vec![
            SpawnTarget {
                target_id: "enabled".to_string(),
                label: "Enabled".to_string(),
                root: PathBuf::from("."),
                enabled: true,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            },
            SpawnTarget {
                target_id: "disabled".to_string(),
                label: "Disabled".to_string(),
                root: PathBuf::from("."),
                enabled: false,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            },
        ];

        assert_eq!(validate_spawn_target(&targets, "enabled").status, "ok");
        assert_eq!(
            validate_spawn_target(&targets, "disabled").status,
            "disabled"
        );
        assert_eq!(
            validate_spawn_target(&targets, "missing").status,
            "not_found"
        );
    }

    #[test]
    fn rejects_unknown_target_kinds_at_mutation_boundaries() {
        let root = std::env::temp_dir().join(format!(
            "botster-spawn-target-kind-{}",
            generated_target_id()
        ));
        fs::create_dir_all(&root).expect("create target root");
        let mut targets = Vec::new();
        let create_error = create_spawn_target(
            &mut targets,
            SpawnTargetCreate {
                target_id: Some("mistyped".to_string()),
                label: None,
                root: root.clone(),
                enabled: true,
                kind: Some("Git".to_string()),
                base_ref: None,
                metadata: BTreeMap::new(),
            },
        )
        .expect_err("unknown create kind");
        assert_eq!(create_error.kind, "invalid_target_kind");

        targets.push(SpawnTarget {
            target_id: "legacy".to_string(),
            label: "Legacy".to_string(),
            root: root.clone(),
            enabled: true,
            kind: "legacy-custom".to_string(),
            base_ref: None,
            metadata: BTreeMap::new(),
        });
        update_spawn_target(
            &mut targets,
            "legacy",
            SpawnTargetUpdate {
                label: Some("Still readable".to_string()),
                ..SpawnTargetUpdate::default()
            },
        )
        .expect("legacy kind remains readable when kind is not mutated");
        let update_error = update_spawn_target(
            &mut targets,
            "legacy",
            SpawnTargetUpdate {
                kind: Some("git ".to_string()),
                ..SpawnTargetUpdate::default()
            },
        )
        .expect_err("unknown update kind");
        assert_eq!(update_error.kind, "invalid_target_kind");
        assert_eq!(targets[0].kind, "legacy-custom");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn git_target_admission_bounds_a_hung_git_process() {
        let root = std::env::temp_dir().join(format!(
            "botster-spawn-target-hung-git-{}",
            generated_target_id()
        ));
        fs::create_dir_all(&root).expect("create target root");
        let executable = root.join("hung-git");
        fs::write(&executable, "#!/bin/sh\nwhile :; do :; done\n").expect("write hung Git");
        let mut permissions = fs::metadata(&executable)
            .expect("hung Git metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&executable, permissions).expect("chmod hung Git");

        let error = admitted_git_base_ref_using(
            executable.as_os_str(),
            &root,
            "git",
            Some("main".to_string()),
            false,
            Instant::now() + Duration::from_millis(100),
        )
        .expect_err("hung Git admission must time out");
        assert_eq!(error.kind, "repository_unavailable");
        assert!(!error.message.contains(root.to_string_lossy().as_ref()));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn git_target_defaults_base_ref_once_and_updates_atomically() {
        let root = std::env::temp_dir().join(format!(
            "botster-spawn-target-git-{}",
            generated_target_id()
        ));
        fs::create_dir_all(&root).expect("create Git target root");
        run_git(None, &["init", "-b", "main", path_str(&root)]);
        run_git(
            Some(&root),
            &["config", "user.email", "botster@example.invalid"],
        );
        run_git(Some(&root), &["config", "user.name", "Botster Test"]);
        fs::write(root.join("README.md"), "target\n").expect("write fixture");
        run_git(Some(&root), &["add", "README.md"]);
        run_git(Some(&root), &["commit", "-m", "fixture"]);

        let mut targets = Vec::new();
        let created = create_spawn_target(
            &mut targets,
            SpawnTargetCreate {
                target_id: Some("git-target".to_string()),
                label: None,
                root: root.clone(),
                enabled: true,
                kind: Some("git".to_string()),
                base_ref: None,
                metadata: BTreeMap::new(),
            },
        )
        .expect("admit Git target");
        assert_eq!(created.base_ref.as_deref(), Some("main"));

        run_git(Some(&root), &["switch", "-c", "other"]);
        let updated = update_spawn_target(
            &mut targets,
            "git-target",
            SpawnTargetUpdate {
                label: Some("Updated".to_string()),
                ..SpawnTargetUpdate::default()
            },
        )
        .expect("retain stored base ref");
        assert_eq!(updated.base_ref.as_deref(), Some("main"));

        let before = targets[0].clone();
        let clear_error = update_spawn_target(
            &mut targets,
            "git-target",
            SpawnTargetUpdate {
                label: Some("Must Not Apply".to_string()),
                base_ref: Some(None),
                ..SpawnTargetUpdate::default()
            },
        )
        .expect_err("Git target cannot clear its stored base ref");
        assert_eq!(clear_error.kind, "base_ref_required");
        assert_eq!(targets[0], before, "failed update must be atomic");

        let invalid_error = update_spawn_target(
            &mut targets,
            "git-target",
            SpawnTargetUpdate {
                base_ref: Some(Some("missing-ref".to_string())),
                ..SpawnTargetUpdate::default()
            },
        )
        .expect_err("invalid base ref must be rejected");
        assert_eq!(invalid_error.kind, "invalid_base_ref");
        assert_eq!(targets[0], before, "invalid ref must not mutate the row");

        let empty_error = update_spawn_target(
            &mut targets,
            "git-target",
            SpawnTargetUpdate {
                base_ref: Some(Some(" ".to_string())),
                ..SpawnTargetUpdate::default()
            },
        )
        .expect_err("empty explicit base ref must be rejected");
        assert_eq!(empty_error.kind, "base_ref_required");
        assert_eq!(targets[0], before, "empty ref must not mutate the row");

        let _ = fs::remove_dir_all(root);
    }

    fn path_str(path: &std::path::Path) -> &str {
        path.to_str().expect("test path is UTF-8")
    }

    fn run_git(root: Option<&std::path::Path>, args: &[&str]) {
        let mut command = Command::new("git");
        if let Some(root) = root {
            command.arg("-C").arg(root);
        }
        assert!(
            command.args(args).status().expect("run git").success(),
            "git command failed: {args:?}"
        );
    }
}
