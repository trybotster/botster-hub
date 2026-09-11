//! Hub-owned managed Git worktree preparation and rollback.
//!
//! The public Lua surface never receives these mutation primitives. The runtime
//! runs them on one bounded worker and only exposes the combined ensure/spawn
//! operation.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::spawn_targets::SpawnTarget;
use crate::worktrees::{Worktree, WorktreeGitMetadata};

pub const MANAGED_GIT_OPERATION_TIMEOUT: Duration = Duration::from_secs(25);
pub const MANAGED_GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
const MANAGED_GIT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const MANAGED_GIT_DISCOVERY_NAME_BYTE_CAPACITY: usize = 4096;
const MANAGED_GIT_OUTPUT_BYTE_CAPACITY: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagedWorktreeDecision {
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManagedGitError {
    pub kind: &'static str,
    pub message: String,
    #[serde(skip)]
    recovery: Option<Box<PreparedManagedWorktree>>,
}

impl ManagedGitError {
    pub(crate) fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            recovery: None,
        }
    }

    fn with_recovery(mut self, prepared: PreparedManagedWorktree) -> Self {
        self.recovery = Some(Box::new(prepared));
        self
    }

    pub(crate) fn take_recovery(&mut self) -> Option<PreparedManagedWorktree> {
        self.recovery.take().map(|prepared| *prepared)
    }
}

#[derive(Debug, Clone)]
pub struct ManagedGitRequest {
    pub target: SpawnTarget,
    pub branch: String,
    pub managed_root: PathBuf,
    pub persisted_worktree: Option<Worktree>,
    pub accepted_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedManagedWorktree {
    pub target_id: String,
    pub repository_root: PathBuf,
    pub common_dir: PathBuf,
    pub branch: String,
    pub path: PathBuf,
    pub worktree_id: String,
    pub base_ref: String,
    pub base_commit: String,
    pub head_commit: String,
    pub created_worktree: bool,
    pub created_branch: bool,
}

impl PreparedManagedWorktree {
    #[must_use]
    pub fn worktree(&self) -> Worktree {
        Worktree {
            worktree_id: self.worktree_id.clone(),
            target_id: self.target_id.clone(),
            label: self.branch.clone(),
            path: self.path.clone(),
            status: "present".to_string(),
            management: "hub_managed_git".to_string(),
            git: Some(WorktreeGitMetadata {
                repository_root: self.repository_root.clone(),
                branch: Some(self.branch.clone()),
                head: Some(self.head_commit.clone()),
            }),
            metadata: BTreeMap::from([
                ("base_ref".to_string(), self.base_ref.clone()),
                ("base_commit".to_string(), self.base_commit.clone()),
                (
                    "common_dir".to_string(),
                    self.common_dir.display().to_string(),
                ),
            ]),
        }
    }
}

#[derive(Debug)]
struct ListedWorktree {
    path: PathBuf,
    head: Option<String>,
    branch: Option<String>,
}

pub fn prepare_managed_worktree(
    request: &ManagedGitRequest,
) -> Result<PreparedManagedWorktree, ManagedGitError> {
    // Reserve the final five seconds of the 25-second operation budget for
    // owner-thread persistence/spawn and worker rollback/reconciliation.
    let deadline = request.accepted_at + MANAGED_GIT_COMMAND_TIMEOUT;
    if Instant::now() >= deadline {
        return Err(timed_out());
    }
    preflight_git(deadline)?;
    if !request.target.enabled {
        return Err(ManagedGitError::new(
            "target_disabled",
            "spawn target is disabled",
        ));
    }
    if request.target.kind != "git" {
        return Err(ManagedGitError::new(
            "target_not_git",
            "spawn target is not declared Git-capable",
        ));
    }
    let base_ref = request.target.base_ref.clone().ok_or_else(|| {
        ManagedGitError::new(
            "base_ref_required",
            "Git spawn target has no stored base ref",
        )
    })?;
    let repository_root = request.target.root.canonicalize().map_err(|_| {
        ManagedGitError::new(
            "repository_unavailable",
            "Git spawn target repository is unavailable",
        )
    })?;
    let inside = git_stdout(
        Some(&repository_root),
        &["rev-parse", "--is-inside-work-tree"],
        deadline,
        "repository_unavailable",
    )?;
    if inside.trim() != "true" {
        return Err(ManagedGitError::new(
            "repository_unavailable",
            "Git spawn target repository is unavailable",
        ));
    }
    git_status(
        Some(&repository_root),
        &["check-ref-format", "--branch", &request.branch],
        deadline,
        "invalid_branch",
    )?;
    let common_dir = resolve_common_dir(&repository_root, deadline)?;
    let base_commit = git_stdout(
        Some(&repository_root),
        &["rev-parse", "--verify", &format!("{base_ref}^{{commit}}")],
        deadline,
        "invalid_base_ref",
    )?
    .trim()
    .to_string();
    let path = managed_worktree_path(
        &request.managed_root,
        &request.target.target_id,
        &request.branch,
    );
    if let Some(persisted) = &request.persisted_worktree
        && (persisted.worktree_id
            != managed_worktree_id(&request.target.target_id, &request.branch)
            || persisted.target_id != request.target.target_id
            || persisted.management != "hub_managed_git"
            || canonical_or_original(&persisted.path) != canonical_or_original(&path)
            || persisted
                .git
                .as_ref()
                .and_then(|git| git.branch.as_deref())
                .is_some_and(|branch| branch != request.branch))
    {
        return Err(ManagedGitError::new(
            "worktree_record_mismatch",
            "managed worktree record conflicts with the requested repository and branch",
        ));
    }
    let listed = list_worktrees(&repository_root, deadline)?;
    if let Some(owner) = listed
        .iter()
        .find(|worktree| worktree.branch.as_deref() == Some(request.branch.as_str()))
        && canonical_or_original(&owner.path) != canonical_or_original(&path)
    {
        return Err(ManagedGitError::new(
            "branch_in_use",
            "requested branch is checked out by another worktree",
        ));
    }
    if path.exists() {
        let exact = listed
            .iter()
            .find(|worktree| canonical_or_original(&worktree.path) == canonical_or_original(&path));
        let Some(exact) = exact else {
            return Err(ManagedGitError::new(
                "path_collision",
                "managed worktree path is owned by another resource",
            ));
        };
        if exact.branch.as_deref() != Some(request.branch.as_str())
            || resolve_common_dir(&path, deadline)? != common_dir
        {
            return Err(ManagedGitError::new(
                "worktree_mismatch",
                "managed worktree does not match the requested repository and branch",
            ));
        }
        return Ok(PreparedManagedWorktree {
            target_id: request.target.target_id.clone(),
            repository_root,
            common_dir,
            branch: request.branch.clone(),
            path: path.canonicalize().unwrap_or(path),
            worktree_id: managed_worktree_id(&request.target.target_id, &request.branch),
            base_ref,
            base_commit,
            head_commit: exact.head.clone().unwrap_or_default(),
            created_worktree: false,
            created_branch: false,
        });
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|_| {
            ManagedGitError::new(
                "managed_root_unavailable",
                "managed worktree root could not be created",
            )
        })?;
    }
    let branch_ref = format!("refs/heads/{}", request.branch);
    let branch_exists = git_exit(
        Some(&repository_root),
        &["show-ref", "--verify", "--quiet", &branch_ref],
        deadline,
    )?
    .success();
    let expected_head_commit = if branch_exists {
        git_stdout(
            Some(&repository_root),
            &["rev-parse", "--verify", &format!("{branch_ref}^{{commit}}")],
            deadline,
            "worktree_conflict",
        )?
        .trim()
        .to_string()
    } else {
        base_commit.clone()
    };
    // Arm the complete identity descriptor before `git worktree add`. Every
    // later failure can then compensate the external effect without guessing.
    let rollback = PreparedManagedWorktree {
        target_id: request.target.target_id.clone(),
        repository_root: repository_root.clone(),
        common_dir: common_dir.clone(),
        branch: request.branch.clone(),
        path: path.clone(),
        worktree_id: managed_worktree_id(&request.target.target_id, &request.branch),
        base_ref: base_ref.clone(),
        base_commit: base_commit.clone(),
        head_commit: expected_head_commit,
        created_worktree: true,
        created_branch: !branch_exists,
    };
    let mut args = vec!["worktree", "add"];
    if branch_exists {
        args.push(path.to_str().ok_or_else(|| {
            ManagedGitError::new("invalid_managed_path", "managed worktree path is invalid")
        })?);
        args.push(&request.branch);
    } else {
        args.extend([
            "-b",
            &request.branch,
            path.to_str().ok_or_else(|| {
                ManagedGitError::new("invalid_managed_path", "managed worktree path is invalid")
            })?,
            &base_commit,
        ]);
    }
    if let Err(failure) = git_status(Some(&repository_root), &args, deadline, "worktree_conflict") {
        if path.exists() {
            let listed = list_worktrees(&repository_root, deadline)?;
            if listed.iter().any(|worktree| {
                canonical_or_original(&worktree.path) == canonical_or_original(&path)
            }) {
                return Err(failure);
            }
        }
        return compensate_failed_creation(&rollback, failure);
    }
    let reconciled = (|| {
        let canonical_path = path.canonicalize().map_err(|_| {
            ManagedGitError::new(
                "worktree_reconciliation_failed",
                "created worktree could not be reconciled",
            )
        })?;
        let head_commit = git_stdout(
            Some(&canonical_path),
            &["rev-parse", "--verify", "HEAD^{commit}"],
            deadline,
            "worktree_reconciliation_failed",
        )?
        .trim()
        .to_string();
        Ok::<_, ManagedGitError>((canonical_path, head_commit))
    })();
    let (canonical_path, head_commit) = match reconciled {
        Ok(reconciled) => reconciled,
        Err(failure) => return compensate_failed_creation(&rollback, failure),
    };
    Ok(PreparedManagedWorktree {
        target_id: request.target.target_id.clone(),
        repository_root,
        common_dir,
        branch: request.branch.clone(),
        path: canonical_path,
        worktree_id: managed_worktree_id(&request.target.target_id, &request.branch),
        base_ref,
        base_commit,
        head_commit,
        created_worktree: true,
        created_branch: !branch_exists,
    })
}

/// Create the managed-worktree external effect. The caller must retain cleanup
/// ownership until it commits or rolls back the returned descriptor.
pub(crate) fn create_managed_worktree_effect(
    request: &ManagedGitRequest,
) -> Result<PreparedManagedWorktree, ManagedGitError> {
    prepare_managed_worktree(request)
}

fn compensate_failed_creation(
    rollback: &PreparedManagedWorktree,
    failure: ManagedGitError,
) -> Result<PreparedManagedWorktree, ManagedGitError> {
    if !rollback.path.exists() {
        return Err(failure);
    }
    match rollback_prepared_worktree(rollback, Instant::now() + MANAGED_GIT_COMMAND_TIMEOUT) {
        Ok(()) => Err(failure),
        Err(compensation) => Err(ManagedGitError::new(
            "reconciliation_failed",
            format!(
                "{}; managed worktree compensation failed: {}",
                failure.message, compensation.message
            ),
        )
        .with_recovery(rollback.clone())),
    }
}

pub(crate) fn finalize_managed_worktree(
    prepared: &PreparedManagedWorktree,
    decision: ManagedWorktreeDecision,
    deadline: Instant,
) -> Result<(), ManagedGitError> {
    match decision {
        ManagedWorktreeDecision::Commit => Ok(()),
        ManagedWorktreeDecision::Rollback => rollback_prepared_worktree(prepared, deadline),
    }
}

pub fn rollback_prepared_worktree(
    prepared: &PreparedManagedWorktree,
    deadline: Instant,
) -> Result<(), ManagedGitError> {
    if !prepared.created_worktree {
        return Ok(());
    }
    if resolve_common_dir(&prepared.path, deadline)? != prepared.common_dir {
        return Err(ManagedGitError::new(
            "rollback_identity_mismatch",
            "managed worktree identity changed before rollback",
        ));
    }
    let branch = git_stdout(
        Some(&prepared.path),
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        deadline,
        "rollback_identity_mismatch",
    )?;
    let head = git_stdout(
        Some(&prepared.path),
        &["rev-parse", "--verify", "HEAD^{commit}"],
        deadline,
        "rollback_identity_mismatch",
    )?;
    if branch.trim() != prepared.branch || head.trim() != prepared.head_commit {
        return Err(ManagedGitError::new(
            "rollback_identity_mismatch",
            "managed worktree changed before rollback",
        ));
    }
    let status = git_stdout(
        Some(&prepared.path),
        &["status", "--porcelain", "--untracked-files=all"],
        deadline,
        "rollback_identity_mismatch",
    )?;
    if !status.trim().is_empty() {
        return Err(ManagedGitError::new(
            "rollback_identity_mismatch",
            "managed worktree has content changes and was preserved",
        ));
    }
    git_status(
        Some(&prepared.repository_root),
        &[
            "worktree",
            "remove",
            "--force",
            prepared.path.to_str().ok_or_else(|| {
                ManagedGitError::new("rollback_failed", "managed worktree path is invalid")
            })?,
        ],
        deadline,
        "rollback_failed",
    )?;
    if prepared.created_branch {
        let branch_commit = git_stdout(
            Some(&prepared.repository_root),
            &[
                "rev-parse",
                "--verify",
                &format!("refs/heads/{}^{{commit}}", prepared.branch),
            ],
            deadline,
            "rollback_failed",
        )?;
        if branch_commit.trim() != prepared.head_commit {
            return Err(ManagedGitError::new(
                "rollback_identity_mismatch",
                "created branch changed before rollback",
            ));
        }
        git_status(
            Some(&prepared.repository_root),
            &["branch", "-D", &prepared.branch],
            deadline,
            "rollback_failed",
        )?;
    }
    Ok(())
}

/// Adopt exact deterministic managed worktrees left before registry persistence.
pub fn adopt_unrecorded_managed_worktrees(
    targets: &[SpawnTarget],
    worktrees: &mut Vec<Worktree>,
    managed_root: &Path,
) -> bool {
    let mut changed = false;
    for worktree in worktrees
        .iter_mut()
        .filter(|worktree| worktree.management == "hub_managed_git")
    {
        let status = if worktree
            .metadata
            .get("recovery_required")
            .map(String::as_str)
            == Some("true")
        {
            if worktree.path.exists() {
                "stale"
            } else {
                "missing"
            }
        } else {
            targets
                .iter()
                .find(|target| target.target_id == worktree.target_id)
                .map_or("stale", |target| {
                    reconcile_managed_worktree(worktree, target)
                })
        };
        if worktree.status != status {
            worktree.status = status.to_string();
            changed = true;
        }
    }
    changed |= discover_unrecorded_managed_worktrees(targets, worktrees, managed_root);
    changed
}

fn discover_unrecorded_managed_worktrees(
    targets: &[SpawnTarget],
    worktrees: &mut Vec<Worktree>,
    managed_root: &Path,
) -> bool {
    let Ok(owned_root) = managed_root.canonicalize() else {
        return false;
    };
    let Ok(target_entries) = fs::read_dir(&owned_root) else {
        return false;
    };
    let mut changed = false;
    for target_entry in target_entries.flatten() {
        let Ok(target_metadata) = fs::symlink_metadata(target_entry.path()) else {
            continue;
        };
        if !target_metadata.is_dir() || target_metadata.file_type().is_symlink() {
            continue;
        }
        let Some(target_id) = target_entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Ok(target_directory) = target_entry.path().canonicalize() else {
            continue;
        };
        if target_directory.parent() != Some(owned_root.as_path())
            || !target_directory.starts_with(&owned_root)
        {
            report_unidentified_managed_entry(&target_entry.path(), "target_path_escape");
            continue;
        }
        let Ok(worktree_entries) = fs::read_dir(&target_directory) else {
            continue;
        };
        for worktree_entry in worktree_entries.flatten() {
            let Ok(worktree_metadata) = fs::symlink_metadata(worktree_entry.path()) else {
                continue;
            };
            if !worktree_metadata.is_dir() || worktree_metadata.file_type().is_symlink() {
                continue;
            }
            let Ok(path) = worktree_entry.path().canonicalize() else {
                continue;
            };
            if path.parent() != Some(target_directory.as_path()) || !path.starts_with(&owned_root) {
                report_unidentified_managed_entry(&worktree_entry.path(), "path_escape");
                continue;
            }
            let encoded_branch_name = worktree_entry.file_name();
            let Some(encoded_branch) = encoded_branch_name.to_str().filter(|name| {
                name.len() <= MANAGED_GIT_DISCOVERY_NAME_BYTE_CAPACITY && is_well_formed_hex(name)
            }) else {
                report_unidentified_managed_entry(&path, "invalid_branch_encoding");
                continue;
            };
            let worktree_id = format!("managed:{target_id}:{encoded_branch}");
            if worktrees
                .iter()
                .any(|worktree| worktree.worktree_id == worktree_id)
            {
                continue;
            }
            let branch = hex_decode(encoded_branch)
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .filter(|branch| !branch.is_empty());
            if branch.as_ref().is_some_and(|branch| {
                managed_worktree_path(&owned_root, &target_id, branch)
                    .canonicalize()
                    .ok()
                    .as_ref()
                    != Some(&path)
            }) {
                report_unidentified_managed_entry(&path, "path_identity_mismatch");
                continue;
            }
            let deadline = Instant::now() + MANAGED_GIT_DISCOVERY_TIMEOUT;
            let observed_branch = git_stdout(
                Some(&path),
                &["symbolic-ref", "--quiet", "--short", "HEAD"],
                deadline,
                "stale",
            )
            .ok()
            .map(|value| value.trim().to_string());
            let common_dir = resolve_common_dir(&path, deadline).ok();
            let head_commit = git_stdout(
                Some(&path),
                &["rev-parse", "--verify", "HEAD^{commit}"],
                deadline,
                "stale",
            )
            .ok()
            .map(|value| value.trim().to_string());
            let target = targets.iter().find(|target| target.target_id == target_id);
            let live_identity = observed_branch.as_deref() == branch.as_deref()
                && branch.is_some()
                && common_dir.is_some()
                && head_commit.is_some();
            let recovery_reason = match (live_identity, target) {
                (false, _) => "git_identity_unavailable",
                (true, None) => "target_unknown",
                (true, Some(target)) if !target.enabled => "target_disabled",
                (true, Some(target)) if target.kind != "git" || target.base_ref.is_none() => {
                    "target_not_git"
                }
                (true, Some(target)) => match resolve_common_dir(
                    &target.root,
                    Instant::now() + MANAGED_GIT_DISCOVERY_TIMEOUT,
                ) {
                    Ok(target_common_dir) if Some(&target_common_dir) == common_dir.as_ref() => {
                        "unrecorded_create"
                    }
                    Ok(_) => "repository_mismatch",
                    Err(_) => "target_identity_unavailable",
                },
            };
            let git = common_dir.as_ref().and_then(|common_dir| {
                live_identity.then(|| WorktreeGitMetadata {
                    repository_root: repository_root_from_common_dir(common_dir),
                    branch: branch.clone(),
                    head: head_commit.clone(),
                })
            });
            let mut metadata = BTreeMap::from([
                ("recovery_required".to_string(), "true".to_string()),
                ("recovery_reason".to_string(), recovery_reason.to_string()),
            ]);
            if let Some(common_dir) = common_dir {
                metadata.insert("common_dir".to_string(), common_dir.display().to_string());
            }
            worktrees.push(Worktree {
                worktree_id,
                target_id: target_id.clone(),
                label: branch.unwrap_or_else(|| encoded_branch.to_string()),
                path,
                status: "stale".to_string(),
                management: "hub_managed_git".to_string(),
                git,
                metadata,
            });
            changed = true;
        }
    }
    changed
}

fn repository_root_from_common_dir(common_dir: &Path) -> PathBuf {
    if common_dir.file_name() == Some(OsStr::new(".git")) {
        common_dir
            .parent()
            .map_or_else(|| common_dir.to_path_buf(), Path::to_path_buf)
    } else {
        common_dir.to_path_buf()
    }
}

fn report_unidentified_managed_entry(path: &Path, reason: &str) {
    let path = format!("{path:?}");
    let bounded_path = path
        .chars()
        .take(MANAGED_GIT_DISCOVERY_NAME_BYTE_CAPACITY)
        .collect::<String>();
    eprintln!("managed_git_recovery_required reason={reason} path={bounded_path}");
}

#[must_use]
pub fn reconcile_managed_worktree(worktree: &Worktree, target: &SpawnTarget) -> &'static str {
    if !worktree.path.exists() {
        return "missing";
    }
    let Some(git) = &worktree.git else {
        return "stale";
    };
    let deterministic_suffix_matches = worktree.path.file_name()
        == Some(OsStr::new(&hex_encode(
            git.branch.as_deref().unwrap_or_default().as_bytes(),
        )))
        && worktree.path.parent().and_then(Path::file_name)
            == Some(OsStr::new(&worktree.target_id));
    if !deterministic_suffix_matches
        || canonical_or_original(&git.repository_root) != canonical_or_original(&target.root)
    {
        return "stale";
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    let Ok(common_dir) = resolve_common_dir(&worktree.path, deadline) else {
        return "stale";
    };
    let Ok(target_common_dir) = resolve_common_dir(&target.root, deadline) else {
        return "stale";
    };
    let Ok(branch) = git_stdout(
        Some(&worktree.path),
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        deadline,
        "stale",
    ) else {
        return "stale";
    };
    if common_dir == target_common_dir && git.branch.as_deref() == Some(branch.trim()) {
        "present"
    } else {
        "stale"
    }
}

#[must_use]
pub fn managed_worktree_path(root: &Path, target_id: &str, branch: &str) -> PathBuf {
    root.join(target_id).join(hex_encode(branch.as_bytes()))
}

#[must_use]
pub fn managed_worktree_id(target_id: &str, branch: &str) -> String {
    format!("managed:{target_id}:{}", hex_encode(branch.as_bytes()))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn hex_decode(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Some((high << 4) | low)
        })
        .collect()
}

fn is_well_formed_hex(encoded: &str) -> bool {
    !encoded.is_empty()
        && encoded.len().is_multiple_of(2)
        && encoded
            .as_bytes()
            .iter()
            .copied()
            .all(|byte| hex_nibble(byte).is_some())
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn preflight_git(deadline: Instant) -> Result<(), ManagedGitError> {
    git_status(None, &["--version"], deadline, "git_unavailable")
}

fn list_worktrees(
    repository_root: &Path,
    deadline: Instant,
) -> Result<Vec<ListedWorktree>, ManagedGitError> {
    let output = git_stdout(
        Some(repository_root),
        &["worktree", "list", "--porcelain"],
        deadline,
        "worktree_list_failed",
    )?;
    let mut rows = Vec::new();
    let mut path = None;
    let mut head = None;
    let mut branch = None;
    for line in output.lines().chain(std::iter::once("")) {
        if line.is_empty() {
            if let Some(path) = path.take() {
                rows.push(ListedWorktree {
                    path,
                    head: head.take(),
                    branch: branch.take(),
                });
            }
        } else if let Some(value) = line.strip_prefix("worktree ") {
            path = Some(PathBuf::from(value));
        } else if let Some(value) = line.strip_prefix("HEAD ") {
            head = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("branch refs/heads/") {
            branch = Some(value.to_string());
        }
    }
    Ok(rows)
}

fn resolve_common_dir(root: &Path, deadline: Instant) -> Result<PathBuf, ManagedGitError> {
    let value = git_stdout(
        Some(root),
        &["rev-parse", "--git-common-dir"],
        deadline,
        "repository_unavailable",
    )?;
    let path = PathBuf::from(value.trim());
    let path = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    path.canonicalize().map_err(|_| {
        ManagedGitError::new(
            "repository_unavailable",
            "Git repository identity could not be resolved",
        )
    })
}

fn git_status(
    root: Option<&Path>,
    args: &[&str],
    deadline: Instant,
    failure_kind: &'static str,
) -> Result<(), ManagedGitError> {
    git_status_using(OsStr::new("git"), root, args, deadline, failure_kind)
}

fn git_status_using(
    executable: &OsStr,
    root: Option<&Path>,
    args: &[&str],
    deadline: Instant,
    failure_kind: &'static str,
) -> Result<(), ManagedGitError> {
    let status = git_exit_using(executable, root, args, deadline)?;
    if status.success() {
        Ok(())
    } else {
        Err(ManagedGitError::new(
            failure_kind,
            "managed Git operation was rejected",
        ))
    }
}

fn git_stdout(
    root: Option<&Path>,
    args: &[&str],
    deadline: Instant,
    failure_kind: &'static str,
) -> Result<String, ManagedGitError> {
    git_stdout_using(OsStr::new("git"), root, args, deadline, failure_kind)
}

pub(crate) fn git_stdout_using(
    executable: &OsStr,
    root: Option<&Path>,
    args: &[&str],
    deadline: Instant,
    failure_kind: &'static str,
) -> Result<String, ManagedGitError> {
    let mut command = Command::new(executable);
    if let Some(root) = root {
        command.arg("-C").arg(root);
    }
    command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    configure_owned_process_group(&mut command);
    let mut child = command
        .spawn()
        .map_err(|_| ManagedGitError::new("git_unavailable", "Git is unavailable"))?;
    let stdout = child.stdout.take().ok_or_else(|| {
        ManagedGitError::new(failure_kind, "managed Git output could not be captured")
    })?;
    let reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout
            .take(MANAGED_GIT_OUTPUT_BYTE_CAPACITY + 1)
            .read_to_end(&mut bytes)
            .map(|_| bytes)
    });
    let status = wait_for_child(&mut child, deadline);
    let stdout = reader
        .join()
        .map_err(|_| ManagedGitError::new(failure_kind, "managed Git output reader failed"))?
        .map_err(|_| ManagedGitError::new(failure_kind, "managed Git output could not be read"))?;
    let status = status?;
    if stdout.len() as u64 > MANAGED_GIT_OUTPUT_BYTE_CAPACITY {
        return Err(ManagedGitError::new(
            "git_output_too_large",
            "managed Git output exceeds its bounded read capacity",
        ));
    }
    if !status.success() {
        return Err(ManagedGitError::new(
            failure_kind,
            "managed Git operation was rejected",
        ));
    }
    String::from_utf8(stdout).map_err(|_| {
        ManagedGitError::new(
            failure_kind,
            "managed Git operation returned invalid output",
        )
    })
}

fn git_exit(
    root: Option<&Path>,
    args: &[&str],
    deadline: Instant,
) -> Result<ExitStatus, ManagedGitError> {
    git_exit_using(OsStr::new("git"), root, args, deadline)
}

fn git_exit_using(
    executable: &OsStr,
    root: Option<&Path>,
    args: &[&str],
    deadline: Instant,
) -> Result<ExitStatus, ManagedGitError> {
    let mut command = Command::new(executable);
    if let Some(root) = root {
        command.arg("-C").arg(root);
    }
    command
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_owned_process_group(&mut command);
    let mut child = command
        .spawn()
        .map_err(|_| ManagedGitError::new("git_unavailable", "Git is unavailable"))?;
    wait_for_child(&mut child, deadline)
}

fn wait_for_child(child: &mut Child, deadline: Instant) -> Result<ExitStatus, ManagedGitError> {
    let command_deadline = Instant::now()
        .checked_add(MANAGED_GIT_COMMAND_TIMEOUT)
        .map_or(deadline, |candidate| candidate.min(deadline));
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if Instant::now() < command_deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                terminate_owned_child_group(child)?;
                return Err(timed_out());
            }
            Err(_) => {
                terminate_owned_child_group(child)?;
                return Err(ManagedGitError::new(
                    "git_failed",
                    "managed Git child could not be observed",
                ));
            }
        }
    }
}

fn configure_owned_process_group(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

fn terminate_owned_child_group(child: &mut Child) -> Result<(), ManagedGitError> {
    let pid = child.id();
    let mut child_reaped = child.try_wait().ok().flatten().is_some();
    signal_owned_child(pid, libc::SIGTERM)?;
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        child_reaped |= child.try_wait().ok().flatten().is_some();
        if child_reaped && !owned_process_group_exists(pid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
    signal_owned_child(pid, libc::SIGKILL)?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        child_reaped |= child.try_wait().ok().flatten().is_some();
        if child_reaped && !owned_process_group_exists(pid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err(ManagedGitError::new(
        "git_cleanup_timed_out",
        "managed Git child cleanup did not finish within the bounded deadline",
    ))
}

fn owned_process_group_exists(pid: u32) -> bool {
    if unsafe { libc::killpg(pid as libc::pid_t, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

fn signal_owned_child(pid: u32, signal: libc::c_int) -> Result<(), ManagedGitError> {
    if unsafe { libc::killpg(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let group_error = std::io::Error::last_os_error();
    if group_error.raw_os_error() != Some(libc::ESRCH) {
        return Err(ManagedGitError::new(
            "git_cleanup_failed",
            "managed Git process group could not be signalled",
        ));
    }
    if unsafe { libc::kill(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let child_error = std::io::Error::last_os_error();
    if child_error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(ManagedGitError::new(
            "git_cleanup_failed",
            "managed Git child could not be signalled",
        ))
    }
}

fn canonical_or_original(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn timed_out() -> ManagedGitError {
    ManagedGitError::new(
        "ensure_timed_out",
        "managed Git operation did not complete before its deadline",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_REPOSITORY: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn branch_encoding_is_collision_free_for_path_separators() {
        let root = Path::new("/managed");
        assert_ne!(
            managed_worktree_path(root, "tgt", "feature/a"),
            managed_worktree_path(root, "tgt", "feature-a")
        );
        assert!(
            !managed_worktree_path(root, "tgt", "feature/a")
                .strip_prefix(root)
                .expect("managed path beneath root")
                .to_string_lossy()
                .contains("feature/a")
        );
    }

    #[test]
    fn ensures_all_three_resolution_cases_and_reuses_dirty_worktrees() {
        let fixture = GitFixture::new();
        let target = fixture.target();

        let missing = prepare_managed_worktree(&fixture.request(target.clone(), "feature/new"))
            .expect("create missing branch and worktree");
        assert!(missing.created_branch);
        assert!(missing.created_worktree);
        assert_eq!(missing.base_commit, fixture.head());

        fs::write(missing.path.join("dirty.txt"), "keep me").expect("dirty managed worktree");
        let reused = prepare_managed_worktree(&fixture.request(target.clone(), "feature/new"))
            .expect("reuse exact managed worktree");
        assert!(!reused.created_branch);
        assert!(!reused.created_worktree);
        assert_eq!(
            fs::read_to_string(reused.path.join("dirty.txt")).expect("read dirty file"),
            "keep me"
        );

        fixture.git(&["branch", "feature/existing"]);
        let existing = prepare_managed_worktree(&fixture.request(target, "feature/existing"))
            .expect("add worktree for existing local branch");
        assert!(!existing.created_branch);
        assert!(existing.created_worktree);
    }

    #[test]
    fn rejects_branch_owned_by_another_worktree_without_cleanup() {
        let fixture = GitFixture::new();
        fixture.git(&["branch", "feature/owned"]);
        let foreign = fixture.root.join("foreign");
        fixture.git(&[
            "worktree",
            "add",
            foreign.to_str().expect("foreign path"),
            "feature/owned",
        ]);

        let error = prepare_managed_worktree(&fixture.request(fixture.target(), "feature/owned"))
            .expect_err("foreign branch ownership must conflict");
        assert_eq!(error.kind, "branch_in_use");
        assert!(foreign.exists());
    }

    #[test]
    fn rejects_wrong_repository_at_deterministic_path_without_cleanup() {
        let fixture = GitFixture::new();
        let foreign = GitFixture::new();
        let path = managed_worktree_path(&fixture.managed_root, "tgt_managed", "feature/collision");
        fs::create_dir_all(path.parent().expect("managed path parent"))
            .expect("create managed path parent");
        foreign.git(&[
            "worktree",
            "add",
            "-b",
            "foreign-collision",
            path.to_str().expect("foreign collision path is UTF-8"),
        ]);

        let error =
            prepare_managed_worktree(&fixture.request(fixture.target(), "feature/collision"))
                .expect_err("wrong repository path ownership must conflict");
        assert_eq!(error.kind, "path_collision");
        assert!(path.exists(), "foreign worktree must not be removed");
    }

    #[test]
    fn rollback_removes_only_resources_created_by_the_call() {
        let fixture = GitFixture::new();
        let prepared =
            prepare_managed_worktree(&fixture.request(fixture.target(), "feature/rollback"))
                .expect("prepare rollback worktree");
        rollback_prepared_worktree(&prepared, Instant::now() + Duration::from_secs(5))
            .expect("rollback call-created worktree and branch");
        assert!(!prepared.path.exists());
        assert!(
            !fixture
                .git_status(&[
                    "show-ref",
                    "--verify",
                    "--quiet",
                    "refs/heads/feature/rollback"
                ])
                .success()
        );
    }

    #[test]
    fn rollback_removes_call_created_worktree_but_preserves_existing_branch() {
        let fixture = GitFixture::new();
        fixture.git(&["branch", "feature/existing-rollback"]);
        let prepared = prepare_managed_worktree(
            &fixture.request(fixture.target(), "feature/existing-rollback"),
        )
        .expect("prepare existing branch worktree");
        assert!(prepared.created_worktree);
        assert!(!prepared.created_branch);

        rollback_prepared_worktree(&prepared, Instant::now() + Duration::from_secs(5))
            .expect("rollback call-created worktree");
        assert!(!prepared.path.exists());
        assert!(
            fixture
                .git_status(&[
                    "show-ref",
                    "--verify",
                    "--quiet",
                    "refs/heads/feature/existing-rollback"
                ])
                .success(),
            "pre-existing branch must remain"
        );
    }

    #[test]
    fn explicit_effect_decision_commits_or_rolls_back_the_created_worktree() {
        let committed_fixture = GitFixture::new();
        let committed = prepare_managed_worktree(
            &committed_fixture.request(committed_fixture.target(), "feature/commit-effect"),
        )
        .expect("create committed worktree effect");
        finalize_managed_worktree(
            &committed,
            ManagedWorktreeDecision::Commit,
            Instant::now() + Duration::from_secs(5),
        )
        .expect("commit worktree effect");
        assert!(committed.path.exists());

        let rolled_back_fixture = GitFixture::new();
        let rolled_back = prepare_managed_worktree(
            &rolled_back_fixture.request(rolled_back_fixture.target(), "feature/rollback-effect"),
        )
        .expect("create rollback worktree effect");
        finalize_managed_worktree(
            &rolled_back,
            ManagedWorktreeDecision::Rollback,
            Instant::now() + Duration::from_secs(5),
        )
        .expect("roll back worktree effect");
        assert!(!rolled_back.path.exists());
    }

    #[test]
    fn a_failure_after_creation_compensates_with_the_prearmed_descriptor() {
        let fixture = GitFixture::new();
        let prepared =
            prepare_managed_worktree(&fixture.request(fixture.target(), "feature/failed-effect"))
                .expect("create failed worktree effect fixture");
        let failure = ManagedGitError::new(
            "worktree_reconciliation_failed",
            "created worktree could not be reconciled",
        );

        let returned = compensate_failed_creation(&prepared, failure.clone())
            .expect_err("compensated creation must return its original failure");

        assert_eq!(returned, failure);
        assert!(!prepared.path.exists());
        assert!(
            !fixture
                .git_status(&[
                    "show-ref",
                    "--verify",
                    "--quiet",
                    "refs/heads/feature/failed-effect"
                ])
                .success(),
            "compensation must remove the call-created branch"
        );
    }

    #[test]
    fn failed_creation_compensation_returns_the_recovery_descriptor() {
        let fixture = GitFixture::new();
        let prepared = prepare_managed_worktree(
            &fixture.request(fixture.target(), "feature/failed-compensation"),
        )
        .expect("create compensation failure fixture");
        fs::write(prepared.path.join("user-change"), "preserve\n")
            .expect("change compensation fixture");
        let failure = ManagedGitError::new(
            "worktree_reconciliation_failed",
            "created worktree could not be reconciled",
        );

        let mut returned = compensate_failed_creation(&prepared, failure)
            .expect_err("unsafe compensation must require recovery");
        let recovery = returned
            .take_recovery()
            .expect("compensation failure must retain its descriptor");

        assert_eq!(returned.kind, "reconciliation_failed");
        assert_eq!(recovery, prepared);
        assert!(recovery.path.exists(), "changed user content must remain");
    }

    #[test]
    fn rollback_preserves_call_created_resources_when_content_changed() {
        let fixture = GitFixture::new();
        let prepared =
            prepare_managed_worktree(&fixture.request(fixture.target(), "feature/dirty-rollback"))
                .expect("prepare dirty rollback worktree");
        fs::write(prepared.path.join("untracked.txt"), "preserve\n")
            .expect("write concurrent content");

        let error = rollback_prepared_worktree(&prepared, Instant::now() + Duration::from_secs(5))
            .expect_err("dirty worktree must be preserved");
        assert_eq!(error.kind, "rollback_identity_mismatch");
        assert!(prepared.path.exists());
        assert!(
            fixture
                .git_status(&[
                    "show-ref",
                    "--verify",
                    "--quiet",
                    "refs/heads/feature/dirty-rollback"
                ])
                .success(),
            "created branch must be preserved with changed content"
        );
    }

    #[test]
    fn rejects_expired_invalid_targets_and_mismatched_rows_without_mutation() {
        let fixture = GitFixture::new();

        let mut expired = fixture.request(fixture.target(), "feature/expired");
        expired.accepted_at = Instant::now() - MANAGED_GIT_OPERATION_TIMEOUT;
        assert_eq!(
            prepare_managed_worktree(&expired)
                .expect_err("expired request")
                .kind,
            "ensure_timed_out"
        );

        let mut directory = fixture.target();
        directory.kind = "directory".to_string();
        assert_eq!(
            prepare_managed_worktree(&fixture.request(directory, "feature/directory"))
                .expect_err("directory target")
                .kind,
            "target_not_git"
        );

        let mut disabled = fixture.target();
        disabled.enabled = false;
        assert_eq!(
            prepare_managed_worktree(&fixture.request(disabled, "feature/disabled"))
                .expect_err("disabled target")
                .kind,
            "target_disabled"
        );

        let mut missing_base = fixture.target();
        missing_base.base_ref = None;
        assert_eq!(
            prepare_managed_worktree(&fixture.request(missing_base, "feature/missing-base"))
                .expect_err("missing stored base ref")
                .kind,
            "base_ref_required"
        );

        let mut invalid_base = fixture.target();
        invalid_base.base_ref = Some("missing-ref".to_string());
        assert_eq!(
            prepare_managed_worktree(&fixture.request(invalid_base, "feature/invalid-base"))
                .expect_err("invalid stored base ref")
                .kind,
            "invalid_base_ref"
        );
        assert_eq!(
            prepare_managed_worktree(&fixture.request(fixture.target(), "invalid branch"))
                .expect_err("invalid branch name")
                .kind,
            "invalid_branch"
        );

        let mut mismatched = fixture.request(fixture.target(), "feature/mismatch");
        mismatched.persisted_worktree = Some(Worktree {
            worktree_id: managed_worktree_id("tgt_managed", "feature/mismatch"),
            target_id: "other-target".to_string(),
            label: "Mismatch".to_string(),
            path: fixture.managed_root.join("wrong"),
            status: "stale".to_string(),
            management: "hub_managed_git".to_string(),
            git: None,
            metadata: BTreeMap::new(),
        });
        assert_eq!(
            prepare_managed_worktree(&mismatched)
                .expect_err("mismatched persisted row")
                .kind,
            "worktree_record_mismatch"
        );
        assert!(
            !managed_worktree_path(&fixture.managed_root, "tgt_managed", "feature/mismatch")
                .exists(),
            "record mismatch must be rejected before Git mutation"
        );
    }

    #[cfg(unix)]
    #[test]
    fn controlled_git_runner_reports_unavailable_and_kills_timed_out_child() {
        let root = std::env::temp_dir().join(format!(
            "botster-managed-git-runner-{}-{}",
            std::process::id(),
            NEXT_REPOSITORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).expect("create runner fixture");

        let missing = root.join("missing-git");
        let unavailable = git_status_using(
            missing.as_os_str(),
            None,
            &["--version"],
            Instant::now() + Duration::from_secs(1),
            "git_unavailable",
        )
        .expect_err("missing Git runner");
        assert_eq!(unavailable.kind, "git_unavailable");
        assert!(!unavailable.message.contains(&root.display().to_string()));

        let unexecutable = root.join("unexecutable-git");
        fs::write(&unexecutable, "#!/bin/sh\nexit 0\n").expect("write unexecutable runner");
        let unavailable = git_status_using(
            unexecutable.as_os_str(),
            None,
            &["--version"],
            Instant::now() + Duration::from_secs(1),
            "git_unavailable",
        )
        .expect_err("unexecutable Git runner");
        assert_eq!(unavailable.kind, "git_unavailable");

        let mut slow_command = Command::new("/bin/sleep");
        slow_command.arg("5");
        configure_owned_process_group(&mut slow_command);
        let mut child = slow_command.spawn().expect("spawn controlled slow child");
        let pid = child.id().to_string();
        let timeout = wait_for_child(&mut child, Instant::now() + Duration::from_millis(100))
            .expect_err("slow Git child must time out");
        assert_eq!(timeout.kind, "ensure_timed_out");
        assert!(
            !Command::new("ps")
                .args(["-p", &pid])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("inspect timed out child")
                .success(),
            "timed-out child must be killed and reaped"
        );

        let mut direct_child = Command::new("/bin/sleep")
            .arg("5")
            .spawn()
            .expect("spawn non-group-leader Git cleanup fixture");
        let started = Instant::now();
        terminate_owned_child_group(&mut direct_child)
            .expect("fall back to signalling the direct child");
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(
            direct_child
                .try_wait()
                .expect("confirm direct Git child was reaped")
                .is_some()
        );

        let descendant_pid_file = root.join("descendant.pid");
        let descendant = root.join("descendant-git");
        fs::write(
            &descendant,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$$\" > '{}'\nsleep 30 &\nprintf '%s\\n' \"$!\" > '{}'\nwait\n",
                descendant_pid_file.display(),
                descendant_pid_file.display()
            ),
        )
        .expect("write descendant runner");
        let mut permissions = fs::metadata(&descendant)
            .expect("descendant runner metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&descendant, permissions).expect("chmod descendant runner");
        let timeout = git_exit_using(
            descendant.as_os_str(),
            None,
            &[],
            Instant::now() + Duration::from_secs(5),
        )
        .expect_err("descendant Git runner must time out");
        assert_eq!(timeout.kind, "ensure_timed_out");
        let descendant_pid = fs::read_to_string(&descendant_pid_file)
            .expect("read descendant pid")
            .trim()
            .to_string();
        assert!(
            !Command::new("ps")
                .args(["-p", &descendant_pid])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("inspect descendant")
                .success(),
            "timed-out Git process group must leave no descendant"
        );

        let noisy = root.join("noisy-git");
        fs::write(
            &noisy,
            "#!/bin/sh\ndd if=/dev/zero bs=262144 count=1 2>/dev/null\n",
        )
        .expect("write noisy runner");
        let mut permissions = fs::metadata(&noisy)
            .expect("noisy runner metadata")
            .permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o755);
        fs::set_permissions(&noisy, permissions).expect("chmod noisy runner");
        let output = git_stdout_using(
            noisy.as_os_str(),
            None,
            &["--version"],
            Instant::now() + Duration::from_secs(2),
            "git_failed",
        )
        .expect("large piped output must be drained while the child runs");
        assert_eq!(output.len(), 262_144);

        let oversized = root.join("oversized-git");
        fs::write(
            &oversized,
            "#!/bin/sh\ndd if=/dev/zero bs=1048577 count=1 2>/dev/null\n",
        )
        .expect("write oversized runner");
        let mut permissions = fs::metadata(&oversized)
            .expect("oversized runner metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&oversized, permissions).expect("chmod oversized runner");
        let oversized_error = git_stdout_using(
            oversized.as_os_str(),
            None,
            &["--version"],
            Instant::now() + Duration::from_secs(2),
            "git_failed",
        )
        .expect_err("oversized Git output must be bounded");
        assert_eq!(oversized_error.kind, "git_output_too_large");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn restart_adopts_only_exact_deterministic_worktrees() {
        let fixture = GitFixture::new();
        let target = fixture.target();
        let prepared = prepare_managed_worktree(&fixture.request(target.clone(), "feature/adopt"))
            .expect("prepare adoptable worktree");
        let mut rows = Vec::new();
        assert!(adopt_unrecorded_managed_worktrees(
            &[target],
            &mut rows,
            &fixture.managed_root
        ));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].worktree_id, prepared.worktree_id);
        assert_eq!(rows[0].path, prepared.path);
        assert_eq!(rows[0].status, "stale");
        assert_eq!(
            rows[0]
                .metadata
                .get("recovery_required")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            rows[0].metadata.get("recovery_reason").map(String::as_str),
            Some("unrecorded_create")
        );
        assert!(!adopt_unrecorded_managed_worktrees(
            &[fixture.target()],
            &mut rows,
            &fixture.managed_root
        ));
        assert_eq!(
            reconcile_managed_worktree(&rows[0], &fixture.target()),
            "present"
        );
        let mut stale = rows[0].clone();
        stale.git.as_mut().expect("Git metadata").branch = Some("wrong".to_string());
        assert_eq!(
            reconcile_managed_worktree(&stale, &fixture.target()),
            "stale"
        );
        fs::remove_dir_all(&prepared.path).expect("remove managed worktree path");
        assert!(adopt_unrecorded_managed_worktrees(
            &[fixture.target()],
            &mut rows,
            &fixture.managed_root
        ));
        assert_eq!(rows[0].status, "missing");
    }

    #[test]
    fn restart_discovers_an_unrecorded_worktree_after_target_deletion() {
        let fixture = GitFixture::new();
        let prepared =
            prepare_managed_worktree(&fixture.request(fixture.target(), "feature/deleted-target"))
                .expect("prepare deleted-target recovery worktree");
        let mut rows = Vec::new();

        assert!(adopt_unrecorded_managed_worktrees(
            &[],
            &mut rows,
            &fixture.managed_root,
        ));

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].worktree_id, prepared.worktree_id);
        assert_eq!(rows[0].status, "stale");
        assert_eq!(
            rows[0].metadata.get("recovery_reason").map(String::as_str),
            Some("target_unknown")
        );
    }

    #[test]
    fn restart_discovers_an_unrecorded_worktree_after_target_disable() {
        let fixture = GitFixture::new();
        let mut disabled = fixture.target();
        let prepared =
            prepare_managed_worktree(&fixture.request(disabled.clone(), "feature/disabled-target"))
                .expect("prepare disabled-target recovery worktree");
        disabled.enabled = false;
        let mut rows = Vec::new();

        assert!(adopt_unrecorded_managed_worktrees(
            &[disabled],
            &mut rows,
            &fixture.managed_root,
        ));

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].worktree_id, prepared.worktree_id);
        assert_eq!(
            rows[0].metadata.get("recovery_reason").map(String::as_str),
            Some("target_disabled")
        );
    }

    #[test]
    fn restart_reports_the_existing_id_when_git_identity_is_unavailable() {
        let fixture = GitFixture::new();
        let prepared = prepare_managed_worktree(
            &fixture.request(fixture.target(), "feature/missing-repository"),
        )
        .expect("prepare missing-repository recovery worktree");
        fs::remove_dir_all(&fixture.repository).expect("remove backing repository");
        let mut rows = Vec::new();

        assert!(adopt_unrecorded_managed_worktrees(
            &[],
            &mut rows,
            &fixture.managed_root,
        ));

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].worktree_id, prepared.worktree_id);
        assert!(rows[0].git.is_none());
        assert_eq!(
            rows[0].metadata.get("recovery_reason").map(String::as_str),
            Some("git_identity_unavailable")
        );
    }

    #[test]
    fn restart_keeps_an_already_recorded_managed_worktree_authoritative() {
        let fixture = GitFixture::new();
        let prepared = prepare_managed_worktree(
            &fixture.request(fixture.target(), "feature/already-recorded"),
        )
        .expect("prepare recorded worktree");
        let committed = prepared.worktree();
        let mut rows = vec![committed.clone()];

        assert!(!adopt_unrecorded_managed_worktrees(
            &[fixture.target()],
            &mut rows,
            &fixture.managed_root,
        ));

        assert_eq!(rows, vec![committed]);
    }

    #[cfg(unix)]
    #[test]
    fn restart_rejects_malformed_and_escaping_managed_entries() {
        use std::os::unix::fs::symlink;

        let fixture = GitFixture::new();
        let target_directory = fixture.managed_root.join("tgt_malformed");
        fs::create_dir_all(target_directory.join("not-hex"))
            .expect("create malformed managed entry");
        let outside = fixture.root.join("outside");
        fs::create_dir_all(outside.join("66656174757265")).expect("create outside managed entry");
        symlink(&outside, fixture.managed_root.join("tgt_escape"))
            .expect("create escaping target symlink");
        symlink(
            outside.join("66656174757265"),
            target_directory.join("66656174757265"),
        )
        .expect("create escaping worktree symlink");
        let mut rows = Vec::new();

        assert!(!adopt_unrecorded_managed_worktrees(
            &[],
            &mut rows,
            &fixture.managed_root,
        ));

        assert!(rows.is_empty());
        assert!(
            outside.exists(),
            "recovery discovery must not delete unknown entries"
        );
    }

    struct GitFixture {
        root: PathBuf,
        repository: PathBuf,
        managed_root: PathBuf,
    }

    impl GitFixture {
        fn new() -> Self {
            let id = NEXT_REPOSITORY.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir()
                .join(format!("botster-managed-git-{}-{id}", std::process::id()));
            let repository = root.join("repository");
            let managed_root = root.join("managed");
            fs::create_dir_all(&repository).expect("create repository");
            run_git(
                None,
                &["init", "-b", "main", repository.to_str().expect("repo")],
            );
            run_git(
                Some(&repository),
                &["config", "user.email", "botster@example.invalid"],
            );
            run_git(Some(&repository), &["config", "user.name", "Botster Test"]);
            fs::write(repository.join("README.md"), "fixture\n").expect("write fixture");
            run_git(Some(&repository), &["add", "README.md"]);
            run_git(Some(&repository), &["commit", "-m", "fixture"]);
            Self {
                root,
                repository,
                managed_root,
            }
        }

        fn target(&self) -> SpawnTarget {
            SpawnTarget {
                target_id: "tgt_managed".to_string(),
                label: "Managed".to_string(),
                root: self.repository.clone(),
                enabled: true,
                kind: "git".to_string(),
                base_ref: Some("main".to_string()),
                metadata: BTreeMap::new(),
            }
        }

        fn request(&self, target: SpawnTarget, branch: &str) -> ManagedGitRequest {
            ManagedGitRequest {
                target,
                branch: branch.to_string(),
                managed_root: self.managed_root.clone(),
                persisted_worktree: None,
                accepted_at: Instant::now(),
            }
        }

        fn head(&self) -> String {
            String::from_utf8(
                Command::new("git")
                    .arg("-C")
                    .arg(&self.repository)
                    .args(["rev-parse", "HEAD"])
                    .output()
                    .expect("git head")
                    .stdout,
            )
            .expect("utf8 head")
            .trim()
            .to_string()
        }

        fn git(&self, args: &[&str]) {
            run_git(Some(&self.repository), args);
        }

        fn git_status(&self, args: &[&str]) -> ExitStatus {
            Command::new("git")
                .arg("-C")
                .arg(&self.repository)
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("git status")
        }
    }

    impl Drop for GitFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn run_git(root: Option<&Path>, args: &[&str]) {
        let mut command = Command::new("git");
        if let Some(root) = root {
            command.arg("-C").arg(root);
        }
        let status = command.args(args).status().expect("run git");
        assert!(status.success(), "git command failed: {args:?}");
    }
}
