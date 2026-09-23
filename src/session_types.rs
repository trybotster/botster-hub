//! Hub-owned session type resolution, authority, and context policy.
//!
//! Packages may contribute declarations, but the hub validates and materializes
//! them into generic core spawn requests before `botster-core` sees anything.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use botster_core::{
    CoreSessionMetadata, PackageSource, RequestId, ResizePayload, SessionId, SessionSpawnRequest,
    SpawnEnvironment, SpawnEnvironmentVariable, SpawnWorkingDirectory,
};
use serde::{Deserialize, Serialize};

use crate::config::HubConfig;
use crate::packages::{PackageRecord, PackageState};
use crate::persistence::HubState;
use crate::spawn_targets::{SpawnTarget, list_spawn_targets};

mod bounded_catalog;

/// Package-, device-, or repo-provided session type definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageSessionType {
    pub id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    pub role: String,
    pub interaction: String,
    #[serde(default)]
    pub traits: Vec<String>,
    pub lifecycle: String,
    #[serde(default)]
    pub execution: PackageSessionTypeExecution,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub working_directory: PackageSessionTypeWorkingDirectory,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub allowed_environment_overrides: Vec<String>,
    #[serde(default)]
    pub context: Vec<String>,
    #[serde(default)]
    pub target_id: Option<String>,
}

/// Hub-owned command execution policy for a session type.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PackageSessionTypeExecution {
    /// Resolve `command` as a safe relative executable under the source root.
    #[default]
    RelativeExecutable,
    /// Run `command` through the Hub-configured shell as one complete command.
    ShellCommand,
}

/// Working-directory policy for a package session type.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum PackageSessionTypeWorkingDirectory {
    #[default]
    PackageRoot,
    Relative {
        path: String,
    },
}

/// Client request data used when resolving or spawning a session_type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionTypeRequest {
    pub target_id: Option<String>,
    pub session_id: Option<SessionId>,
    pub cwd: Option<String>,
    pub environment: BTreeMap<String, String>,
    pub context: SessionTypeContextInput,
}

/// Trusted hub context inputs supplied by a higher-level workflow.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionTypeContextInput {
    pub worktree_path: Option<String>,
    pub repo_path: Option<String>,
    pub branch_name: Option<String>,
    pub prompt: Option<String>,
    pub ticket_id: Option<String>,
    pub workspace_id: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

/// Semantic caller inputs accepted by the atomic managed-worktree path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagedSessionTypeRequest {
    pub environment: BTreeMap<String, String>,
    pub prompt: Option<String>,
    pub ticket_id: Option<String>,
    pub workspace_id: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

/// Opaque Hub-derived worktree facts accepted only by trusted materialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnsuredManagedWorktree {
    pub target_id: String,
    pub repository_root: PathBuf,
    pub worktree_path: PathBuf,
    pub branch: String,
    pub base_ref: String,
    pub base_commit: String,
}

/// Source identity for an effective session type definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HubSessionTypeSource {
    pub kind: String,
    pub name: String,
}

/// Sanitized effective session type row exposed to clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HubSessionType {
    pub session_type_id: String,
    pub source_name: String,
    pub id: String,
    pub source: String,
    pub editable: bool,
    pub overridden_sources: Vec<HubSessionTypeSource>,
    pub diagnostics: Vec<String>,
    pub label: String,
    pub description: Option<String>,
    pub icon: Option<String>,
    pub role: String,
    pub interaction: String,
    pub traits: Vec<String>,
    pub lifecycle: String,
    pub execution: PackageSessionTypeExecution,
    pub command: String,
    pub args: Vec<String>,
    pub working_directory_policy: String,
    pub allowed_environment_overrides: Vec<String>,
    pub context_keys: Vec<String>,
    pub target_id: String,
    pub available: bool,
}

/// Authored session_type definition exposed to a caller permitted to edit it.
///
/// Unlike [`HubSessionType`], which is sanitized for every subscriber, this
/// carries the authored working-directory policy *and* path plus the authored
/// environment — exactly the payload [`SessionTypeMutation::Update`] consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubSessionTypeDefinition {
    pub session_type_id: String,
    pub source: SessionTypeMutationSource,
    pub definition: PackageSessionType,
}

/// Resolved session_type DTO exposed before spawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSessionType {
    pub session_type: HubSessionType,
    pub session_id: SessionId,
    pub executable: String,
    pub arguments: Vec<String>,
    pub working_directory: String,
    pub environment: BTreeMap<String, String>,
    pub context_id: String,
    pub context_keys: Vec<String>,
}

/// Context stored by the hub for a spawned session_type session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubSessionContext {
    pub context_id: String,
    pub session_id: SessionId,
    pub values: BTreeMap<String, String>,
}

/// Resolved spawn request plus context payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedSessionType {
    pub resolved: ResolvedSessionType,
    pub spawn_request: SessionSpawnRequest,
    pub context: HubSessionContext,
    pub metadata: CoreSessionMetadata,
}

/// Session type policy error with path-neutral messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTypeError {
    pub kind: &'static str,
    pub message: String,
}

impl SessionTypeError {
    pub(crate) fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

pub type SessionTypeResult<T> = Result<T, SessionTypeError>;

/// Hub-authorized source for a session type mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionTypeMutationSource {
    Device,
    Repo { target_id: String },
    Package { package_name: String },
}

/// One source-aware session type mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionTypeMutation {
    Create(PackageSessionType),
    Update(PackageSessionType),
    Delete { id: String },
}

const DEVICE_SESSION_TYPE_SOURCE: &str = "device";
const PACKAGE_SESSION_TYPE_SOURCE: &str = "package";
const REPO_SESSION_TYPE_SOURCE: &str = "repo";
const DEFAULT_DEVICE_TARGET_ID: &str = "device:local";
const REPO_SESSION_TYPES_FILE: &str = ".botster/session-types.json";
const REPO_SESSION_TYPES_TEMP_FILE: &str = ".botster/session-types.json.tmp";
const REPO_SESSION_TYPES_FILE_BYTE_CAPACITY: usize = 4 * 1024 * 1024;

/// Exact prior state for one repo session-type file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RepoSessionTypeFileSnapshot {
    Missing,
    Present(Vec<u8>),
}

/// A repository file reached rename but its directory sync did not confirm.
#[must_use]
pub(crate) enum RepoSessionTypeFileCommit {
    Synced,
    PublishedUncertain(SessionTypeError),
}

#[derive(Debug, Clone, Deserialize)]
struct RepoSessionTypesFile {
    #[serde(default)]
    session_types: Vec<PackageSessionType>,
}

pub(crate) struct PreparedSessionTypeMutation {
    state: HubState,
    repo_write: Option<(PathBuf, Vec<PackageSessionType>)>,
}

impl PreparedSessionTypeMutation {
    pub(crate) fn into_parts(self) -> (HubState, Option<(PathBuf, Vec<PackageSessionType>)>) {
        (self.state, self.repo_write)
    }
}

pub(crate) fn commit_repo_session_type_mutation(
    repo_write: Option<(PathBuf, Vec<PackageSessionType>)>,
) -> SessionTypeResult<()> {
    if let Some((root, definitions)) = repo_write {
        let bytes = encode_repo_session_type_bytes(&definitions)?;
        match commit_repo_session_type_bytes(&root, &bytes)? {
            RepoSessionTypeFileCommit::Synced => {}
            RepoSessionTypeFileCommit::PublishedUncertain(error) => return Err(error),
        }
    }
    Ok(())
}

/// Apply a source-aware mutation and return the next durable Hub state.
pub fn mutate_session_type(
    config: &HubConfig,
    state: &HubState,
    source: SessionTypeMutationSource,
    mutation: SessionTypeMutation,
) -> SessionTypeResult<HubState> {
    let prepared = prepare_session_type_mutation(config, state, source, mutation)?;
    let (state, repo_write) = prepared.into_parts();
    commit_repo_session_type_mutation(repo_write)?;
    Ok(state)
}

pub(crate) fn prepare_session_type_mutation(
    config: &HubConfig,
    state: &HubState,
    source: SessionTypeMutationSource,
    mutation: SessionTypeMutation,
) -> SessionTypeResult<PreparedSessionTypeMutation> {
    if let SessionTypeMutationSource::Package { package_name } = &source {
        return Err(SessionTypeError::new(
            "read_only_session_type_source",
            format!("package session types are read-only: {package_name}"),
        ));
    }

    let mut next = state.clone();
    let repo_write = match source {
        SessionTypeMutationSource::Device => {
            if next.device_session_type_sources.is_empty() {
                next.device_session_type_sources.push(
                    crate::persistence::DeviceSessionTypeSource {
                        root: config.data_directory.join("session-types"),
                        session_types: Vec::new(),
                    },
                );
            }
            let source = next
                .device_session_type_sources
                .first_mut()
                .expect("device source inserted above");
            apply_definition_mutation(&mut source.session_types, mutation)?;
            None
        }
        SessionTypeMutationSource::Repo { target_id } => {
            let target = list_spawn_targets(&state.spawn_targets)
                .into_iter()
                .find(|target| target.target_id == target_id && target.enabled)
                .ok_or_else(|| {
                    SessionTypeError::new(
                        "target_not_admitted",
                        "repo session types require an enabled admitted target",
                    )
                })?;
            let root = target.root.canonicalize().map_err(|_| {
                SessionTypeError::new("target_not_admitted", "admitted target is unavailable")
            })?;
            let mut definitions = repo_session_types(&root)?;
            apply_definition_mutation(&mut definitions, mutation)?;
            Some((root, definitions))
        }
        SessionTypeMutationSource::Package { .. } => unreachable!("handled above"),
    };
    next.session_type_generation = next.session_type_generation.saturating_add(1);
    Ok(PreparedSessionTypeMutation {
        state: next,
        repo_write,
    })
}

fn apply_definition_mutation(
    definitions: &mut Vec<PackageSessionType>,
    mutation: SessionTypeMutation,
) -> SessionTypeResult<()> {
    match mutation {
        SessionTypeMutation::Create(definition) => {
            validate_session_type(&definition)?;
            if definitions
                .iter()
                .any(|existing| existing.id == definition.id)
            {
                return Err(SessionTypeError::new(
                    "session_type_already_exists",
                    "session type already exists in the requested source",
                ));
            }
            definitions.push(definition);
        }
        SessionTypeMutation::Update(definition) => {
            validate_session_type(&definition)?;
            let existing = definitions
                .iter_mut()
                .find(|existing| existing.id == definition.id)
                .ok_or_else(|| {
                    SessionTypeError::new(
                        "unknown_session_type",
                        "session type does not exist in the requested source",
                    )
                })?;
            *existing = definition;
        }
        SessionTypeMutation::Delete { id } => {
            let previous = definitions.len();
            definitions.retain(|definition| definition.id != id);
            if definitions.len() == previous {
                return Err(SessionTypeError::new(
                    "unknown_session_type",
                    "session type does not exist in the requested source",
                ));
            }
        }
    }
    validate_session_types(definitions)
        .map_err(|message| SessionTypeError::new("invalid_session_types", message))
}

pub(crate) fn encode_repo_session_type_bytes(
    definitions: &[PackageSessionType],
) -> SessionTypeResult<Vec<u8>> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "session_types": definitions,
    }))
    .map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session types could not be serialized: {error}"),
        )
    })?;
    if bytes.len() > REPO_SESSION_TYPES_FILE_BYTE_CAPACITY {
        return Err(SessionTypeError::new(
            "repo_session_types_too_large",
            format!(
                "repo-local session type file exceeds {} bytes",
                REPO_SESSION_TYPES_FILE_BYTE_CAPACITY
            ),
        ));
    }
    Ok(bytes)
}

/// Sync the file before rename and the directory after rename.
/// A post-rename failure remains an uncertain publication.
pub(crate) fn commit_repo_session_type_bytes(
    root: &Path,
    bytes: &[u8],
) -> SessionTypeResult<RepoSessionTypeFileCommit> {
    use rustix::fs::{Mode, OFlags, openat, renameat};

    let canonical_root = root.canonicalize().map_err(|error| {
        SessionTypeError::new(
            "target_not_admitted",
            format!("admitted target is unavailable: {error}"),
        )
    })?;
    let root_file = File::open(&canonical_root).map_err(|error| {
        SessionTypeError::new(
            "target_not_admitted",
            format!("admitted target could not be opened: {error}"),
        )
    })?;
    let directory = root.join(".botster");
    let created_directory = match fs::symlink_metadata(&directory) {
        Ok(_) => false,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            return Err(SessionTypeError::new(
                "repo_session_type_write_failed",
                format!("repo session type directory could not be checked: {error}"),
            ));
        }
    };
    fs::create_dir_all(&directory).map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session type directory could not be created: {error}"),
        )
    })?;
    let canonical_directory = directory.canonicalize().map_err(|_| {
        SessionTypeError::new(
            "target_not_admitted",
            "repo session type directory is unavailable",
        )
    })?;
    if !canonical_directory.starts_with(&canonical_root) {
        return Err(SessionTypeError::new(
            "target_not_admitted",
            "repo session type directory escapes the admitted target",
        ));
    }
    let descriptor = openat(
        &root_file,
        ".botster",
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|error| {
        SessionTypeError::new(
            "target_not_admitted",
            format!("repo session type directory could not be opened safely: {error}"),
        )
    })?;
    let directory_file = File::from(descriptor);
    if !directory_file
        .metadata()
        .map_err(|error| {
            SessionTypeError::new(
                "target_not_admitted",
                format!("repo session type directory could not be checked: {error}"),
            )
        })?
        .is_dir()
    {
        return Err(SessionTypeError::new(
            "target_not_admitted",
            "repo session type directory is not a directory",
        ));
    }
    let descriptor = openat(
        &directory_file,
        "session-types.json.tmp",
        OFlags::WRONLY | OFlags::CREATE | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session type temporary file could not be opened: {error}"),
        )
    })?;
    let mut file = File::from(descriptor);
    let metadata = file.metadata().map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session type temporary file could not be checked: {error}"),
        )
    })?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(SessionTypeError::new(
            "repo_session_type_write_failed",
            "repo session type temporary file is not an exclusive regular file",
        ));
    }
    file.set_len(0).map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session type temporary file could not be truncated: {error}"),
        )
    })?;
    file.write_all(bytes).map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session type temporary file could not be written: {error}"),
        )
    })?;
    file.sync_all().map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session type temporary file could not be synced: {error}"),
        )
    })?;
    renameat(
        &directory_file,
        "session-types.json.tmp",
        &directory_file,
        "session-types.json",
    )
    .map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session type file could not be replaced: {error}"),
        )
    })?;
    #[cfg(test)]
    if repo_sync_failure_is_due(root) {
        return Ok(RepoSessionTypeFileCommit::PublishedUncertain(
            SessionTypeError::new(
                "repo_session_type_sync_uncertain",
                "injected repository directory sync failure after rename",
            ),
        ));
    }
    let sync_result = (|| {
        directory_file.sync_all()?;
        if created_directory {
            root_file.sync_all()?;
        }
        let current = openat(
            &root_file,
            ".botster",
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(std::io::Error::from)?;
        let current = File::from(current);
        let held = directory_file.metadata()?;
        let named = current.metadata()?;
        if !named.is_dir() || held.dev() != named.dev() || held.ino() != named.ino() {
            return Err(std::io::Error::other(
                "repo session type directory changed after rename",
            ));
        }
        Ok::<_, std::io::Error>(())
    })();
    match sync_result {
        Ok(()) => Ok(RepoSessionTypeFileCommit::Synced),
        Err(error) => Ok(RepoSessionTypeFileCommit::PublishedUncertain(
            SessionTypeError::new(
                "repo_session_type_sync_uncertain",
                format!("repo session type file was renamed but directory sync failed: {error}"),
            ),
        )),
    }
}

#[cfg(test)]
pub(crate) fn inject_next_repo_directory_sync_failure(root: impl AsRef<Path>) {
    let canonical_root = root
        .as_ref()
        .canonicalize()
        .expect("injected repo sync failure needs an existing root");
    repo_sync_failures()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(canonical_root);
}

#[cfg(test)]
fn repo_sync_failures() -> &'static Mutex<BTreeSet<PathBuf>> {
    static FAILURES: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();
    FAILURES.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(test)]
fn repo_sync_failure_is_due(root: &Path) -> bool {
    repo_sync_failures()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(root)
}

/// Read the exact prior repo file with a hard logical-byte bound.
pub(crate) fn snapshot_repo_session_type_file(
    root: &Path,
    logical_byte_limit: usize,
) -> SessionTypeResult<RepoSessionTypeFileSnapshot> {
    let path = root.join(REPO_SESSION_TYPES_FILE);
    let canonical_root = root.canonicalize().map_err(|_| {
        SessionTypeError::new("target_not_admitted", "admitted target is unavailable")
    })?;
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RepoSessionTypeFileSnapshot::Missing);
        }
        Err(error) => {
            return Err(SessionTypeError::new(
                "repo_session_type_read_failed",
                format!("repo session type file could not be read: {error}"),
            ));
        }
    };
    let canonical_path = path.canonicalize().map_err(|_| {
        SessionTypeError::new(
            "repo_session_type_read_failed",
            "repo session type file could not be resolved",
        )
    })?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(SessionTypeError::new(
            "target_not_admitted",
            "repo session type file escapes the admitted target",
        ));
    }
    let limit = logical_byte_limit.min(REPO_SESSION_TYPES_FILE_BYTE_CAPACITY);
    if file
        .metadata()
        .map_err(|error| {
            SessionTypeError::new(
                "repo_session_type_read_failed",
                format!("repo session type file metadata could not be read: {error}"),
            )
        })?
        .len()
        > limit as u64
    {
        return Err(SessionTypeError::new(
            "repo_session_types_too_large",
            "repo session type rollback exceeds the prepared-operation byte limit",
        ));
    }
    let read_limit = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            SessionTypeError::new(
                "repo_session_type_read_failed",
                format!("repo session type file could not be read: {error}"),
            )
        })?;
    if bytes.len() > limit {
        return Err(SessionTypeError::new(
            "repo_session_types_too_large",
            "repo session type rollback exceeds the prepared-operation byte limit",
        ));
    }
    Ok(RepoSessionTypeFileSnapshot::Present(bytes))
}

/// Restore the exact prior repo file contents or absence through the atomic path.
pub(crate) fn restore_repo_session_type_file(
    root: &Path,
    prior: &RepoSessionTypeFileSnapshot,
) -> SessionTypeResult<()> {
    let canonical_root = root.canonicalize().map_err(|_| {
        SessionTypeError::new("target_not_admitted", "admitted target is unavailable")
    })?;
    let directory = root.join(".botster");
    let path = root.join(REPO_SESSION_TYPES_FILE);
    match prior {
        RepoSessionTypeFileSnapshot::Missing => {
            if !directory.exists() {
                return Ok(());
            }
            let canonical_directory = directory.canonicalize().map_err(|_| {
                SessionTypeError::new(
                    "repo_session_type_restore_failed",
                    "repo session type directory could not be resolved",
                )
            })?;
            if !canonical_directory.starts_with(&canonical_root) {
                return Err(SessionTypeError::new(
                    "target_not_admitted",
                    "repo session type directory escapes the admitted target",
                ));
            }
            match fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(SessionTypeError::new(
                    "repo_session_type_restore_failed",
                    format!("repo session type file absence could not be restored: {error}"),
                )),
            }
        }
        RepoSessionTypeFileSnapshot::Present(bytes) => {
            if bytes.len() > REPO_SESSION_TYPES_FILE_BYTE_CAPACITY {
                return Err(SessionTypeError::new(
                    "repo_session_types_too_large",
                    "repo session type rollback exceeds the file byte limit",
                ));
            }
            fs::create_dir_all(&directory).map_err(|error| {
                SessionTypeError::new(
                    "repo_session_type_restore_failed",
                    format!("repo session type directory could not be created: {error}"),
                )
            })?;
            let canonical_directory = directory.canonicalize().map_err(|_| {
                SessionTypeError::new(
                    "repo_session_type_restore_failed",
                    "repo session type directory could not be resolved",
                )
            })?;
            if !canonical_directory.starts_with(&canonical_root) {
                return Err(SessionTypeError::new(
                    "target_not_admitted",
                    "repo session type directory escapes the admitted target",
                ));
            }
            let temporary = root.join(REPO_SESSION_TYPES_TEMP_FILE);
            fs::write(&temporary, bytes).map_err(|error| {
                SessionTypeError::new(
                    "repo_session_type_restore_failed",
                    format!("repo session type rollback could not be written: {error}"),
                )
            })?;
            fs::rename(&temporary, path).map_err(|error| {
                SessionTypeError::new(
                    "repo_session_type_restore_failed",
                    format!("repo session type rollback could not be installed: {error}"),
                )
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SessionTypeSourceRank {
    Package = 0,
    Device = 1,
    Repo = 2,
}

#[derive(Debug, Clone)]
struct SourceSessionType {
    rank: SessionTypeSourceRank,
    source: String,
    source_name: String,
    root: PathBuf,
    session_type: PackageSessionType,
    available: bool,
}

/// Return effective session types after applying package < device < repo precedence.
pub fn list_session_types(
    records: &[&PackageRecord],
    state: &HubState,
) -> SessionTypeResult<Vec<HubSessionType>> {
    let sources = source_session_types(records, state)?;
    effective_session_type_rows(sources)
}

/// Project effective rows from staged repo definitions without reading that repo file.
pub(crate) fn list_session_types_with_staged_repo(
    records: &[&PackageRecord],
    state: &HubState,
    target_id: &str,
    definitions: &[PackageSessionType],
) -> SessionTypeResult<Vec<HubSessionType>> {
    let sources =
        source_session_types_with_staged_repo(records, state, Some((target_id, definitions)))?;
    effective_session_type_rows(sources)
}

/// Build effective rows while charging source clones and bounded repository reads.
pub(crate) fn list_session_types_bounded(
    records: &[&PackageRecord],
    state: &HubState,
    logical_byte_limit: usize,
) -> SessionTypeResult<Option<(Vec<HubSessionType>, usize)>> {
    bounded_catalog::list_all(records, state, logical_byte_limit)
}

/// Resolve and materialize the effective session_type into the generic core spawn contract.
pub fn materialize_session_type(
    config: &HubConfig,
    records: &[&PackageRecord],
    state: &HubState,
    session_type_id: &str,
    request: SessionTypeRequest,
) -> SessionTypeResult<MaterializedSessionType> {
    let (source, mut effective_row, spawn_target) = resolve_materialization_source(
        records,
        state,
        session_type_id,
        request.target_id.as_deref(),
    )?;
    if !source.available {
        return Err(SessionTypeError::new(
            "session_type_unavailable",
            "session type source is not enabled",
        ));
    }

    let session_type = &source.session_type;
    validate_session_type(session_type)?;
    // Command always resolves under the definition's source root (device/package/repo).
    let command_root = source.root.clone();
    // Cwd binds to the admitted spawn point when spawning at T (Option A dual-root).
    let cwd_root = spawn_target
        .as_ref()
        .map(|target| target.root.clone())
        .unwrap_or_else(|| source.root.clone());
    let resolved_target_id = spawn_target
        .as_ref()
        .map(|target| target.target_id.clone())
        .or_else(|| request.target_id.clone())
        .or_else(|| session_type.target_id.clone())
        .unwrap_or_else(|| source_default_target_id(&source));
    effective_row.target_id = resolved_target_id.clone();

    let default_cwd = resolve_working_directory(&cwd_root, session_type)?;
    let working_directory = if let Some(cwd) = &request.cwd {
        let path = PathBuf::from(cwd);
        if !path.is_absolute() || !path.starts_with(&cwd_root) {
            return Err(SessionTypeError::new(
                "cwd_not_admitted",
                "requested cwd is outside the admitted spawn target",
            ));
        }
        path
    } else {
        default_cwd
    };
    if !working_directory.starts_with(&cwd_root) {
        return Err(SessionTypeError::new(
            "cwd_not_admitted",
            "session_type working directory escapes the admitted spawn target",
        ));
    }

    let mut environment = session_type.environment.clone();
    let allowed = session_type
        .allowed_environment_overrides
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for (name, value) in &request.environment {
        validate_environment_name(name)?;
        if !allowed.contains(name) {
            return Err(SessionTypeError::new(
                "environment_not_admitted",
                format!("environment override is not admitted: {name}"),
            ));
        }
        environment.insert(name.clone(), value.clone());
    }

    let session_id = request
        .session_id
        .unwrap_or_else(|| SessionId(format!("session-type-{}", session_type.id)));
    let context_id = format!("ctx-{}", session_id.0);
    let context_inputs = ContextAssemblyInputs {
        session_id: &session_id,
        context_id: &context_id,
        target_id: &resolved_target_id,
        package_root: &command_root,
        working_directory: &working_directory,
    };
    let context = assemble_context(
        config,
        context_inputs,
        request.context,
        &session_type.context,
    );
    inject_context_environment(config, &mut environment, &session_id, &context_id);

    let row = effective_row;
    let metadata = session_type_metadata(&row);
    let (executable, arguments) = resolve_execution(config, &command_root, session_type);
    let resolved = ResolvedSessionType {
        session_type: row,
        session_id: session_id.clone(),
        executable,
        arguments,
        working_directory: working_directory.display().to_string(),
        environment: environment.clone(),
        context_id: context_id.clone(),
        context_keys: context.values.keys().cloned().collect(),
    };
    let spawn_request = SessionSpawnRequest {
        request_id: RequestId(format!("session-type-{context_id}")),
        session_id,
        executable: resolved.executable.clone(),
        arguments: resolved.arguments.clone(),
        working_directory: SpawnWorkingDirectory {
            path: resolved.working_directory.clone(),
        },
        environment: SpawnEnvironment {
            variables: environment
                .into_iter()
                .map(|(name, value)| SpawnEnvironmentVariable { name, value })
                .collect(),
        },
        initial_pty_size: Some(ResizePayload {
            rows: config.session_defaults.initial_rows,
            cols: config.session_defaults.initial_cols,
        }),
    };

    Ok(MaterializedSessionType {
        resolved,
        spawn_request,
        context,
        metadata,
    })
}

/// Materialize an atomic managed-worktree spawn without weakening ordinary cwd admission.
pub(crate) fn materialize_managed_session_type(
    config: &HubConfig,
    records: &[&PackageRecord],
    state: &HubState,
    session_type_id: &str,
    session_id: SessionId,
    request: ManagedSessionTypeRequest,
    ensured: &EnsuredManagedWorktree,
) -> SessionTypeResult<MaterializedSessionType> {
    let (source, mut effective_row) =
        find_source_session_type_for_target(records, state, session_type_id, &ensured.target_id)?;
    if !source.available {
        return Err(SessionTypeError::new(
            "session_type_unavailable",
            "session type source is not enabled",
        ));
    }
    validate_session_type(&source.session_type)?;
    effective_row.target_id = ensured.target_id.clone();
    let managed_root = ensured.worktree_path.canonicalize().map_err(|_| {
        SessionTypeError::new(
            "managed_worktree_unavailable",
            "managed worktree is unavailable",
        )
    })?;
    let working_directory = match &source.session_type.working_directory {
        PackageSessionTypeWorkingDirectory::PackageRoot => managed_root.clone(),
        PackageSessionTypeWorkingDirectory::Relative { path } => {
            let candidate = managed_root.join(path).canonicalize().map_err(|_| {
                SessionTypeError::new(
                    "cwd_not_admitted",
                    "managed session_type working directory is unavailable",
                )
            })?;
            if !candidate.starts_with(&managed_root) {
                return Err(SessionTypeError::new(
                    "cwd_not_admitted",
                    "managed session_type working directory escapes the worktree",
                ));
            }
            candidate
        }
    };
    let mut environment = source.session_type.environment.clone();
    let allowed = source
        .session_type
        .allowed_environment_overrides
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for (name, value) in &request.environment {
        validate_environment_name(name)?;
        if !allowed.contains(name) {
            return Err(SessionTypeError::new(
                "environment_not_admitted",
                format!("environment override is not admitted: {name}"),
            ));
        }
        environment.insert(name.clone(), value.clone());
    }
    let context_id = format!("ctx-{}", session_id.0);
    let mut metadata = request.metadata;
    metadata.insert("base_ref".to_string(), ensured.base_ref.clone());
    metadata.insert("base_commit".to_string(), ensured.base_commit.clone());
    let context = assemble_context(
        config,
        ContextAssemblyInputs {
            session_id: &session_id,
            context_id: &context_id,
            target_id: &ensured.target_id,
            package_root: &ensured.repository_root,
            working_directory: &working_directory,
        },
        SessionTypeContextInput {
            worktree_path: Some(managed_root.display().to_string()),
            repo_path: Some(ensured.repository_root.display().to_string()),
            branch_name: Some(ensured.branch.clone()),
            prompt: request.prompt,
            ticket_id: request.ticket_id,
            workspace_id: request.workspace_id,
            metadata,
        },
        &source.session_type.context,
    );
    inject_context_environment(config, &mut environment, &session_id, &context_id);
    let command_root = if source.rank == SessionTypeSourceRank::Repo {
        &managed_root
    } else {
        &source.root
    };
    let row = effective_row;
    let metadata = session_type_metadata(&row);
    let (executable, arguments) = resolve_execution(config, command_root, &source.session_type);
    let resolved = ResolvedSessionType {
        session_type: row,
        session_id: session_id.clone(),
        executable,
        arguments,
        working_directory: working_directory.display().to_string(),
        environment: environment.clone(),
        context_id: context_id.clone(),
        context_keys: context.values.keys().cloned().collect(),
    };
    let spawn_request = SessionSpawnRequest {
        request_id: RequestId(format!("managed-session-type-{context_id}")),
        session_id,
        executable: resolved.executable.clone(),
        arguments: resolved.arguments.clone(),
        working_directory: SpawnWorkingDirectory {
            path: resolved.working_directory.clone(),
        },
        environment: SpawnEnvironment {
            variables: environment
                .into_iter()
                .map(|(name, value)| SpawnEnvironmentVariable { name, value })
                .collect(),
        },
        initial_pty_size: Some(ResizePayload {
            rows: config.session_defaults.initial_rows,
            cols: config.session_defaults.initial_cols,
        }),
    };
    Ok(MaterializedSessionType {
        resolved,
        spawn_request,
        context,
        metadata,
    })
}

/// Return only enabled effective session_types eligible at one admitted spawn point.
///
/// Target eligibility is applied **before** package < device < repo precedence so a
/// repo-only bare id on another target cannot hide a device Global type at `T`.
/// Rows project `target_id = T` (list context), not storage provenance. Sorted by
/// `session_type_id` lexicographic.
///
/// This is the same set materialize/show/resolve accept for `target_id = T`
/// (available winners only). Overridden qualified losers are not listed and
/// cannot spawn at T.
pub fn list_session_types_for_target(
    records: &[&PackageRecord],
    state: &HubState,
    target_id: &str,
) -> SessionTypeResult<Vec<HubSessionType>> {
    let mut rows = target_scoped_effective_winners(records, state, target_id)?
        .into_iter()
        .map(|(_, row)| row)
        .filter(|row| row.available)
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.session_type_id.cmp(&right.session_type_id));
    Ok(rows)
}

/// Bounded form used by Lua callbacks. `None` means the complete projection
/// would exceed the caller's already-reserved Rust callback allowance.
pub(crate) fn list_session_types_for_target_bounded(
    records: &[PackageRecord],
    state: &HubState,
    target_id: &str,
    logical_byte_limit: usize,
) -> SessionTypeResult<Option<Vec<HubSessionType>>> {
    bounded_catalog::list(records, state, target_id, logical_byte_limit)
}

pub(crate) fn show_session_type_for_target_bounded(
    records: &[PackageRecord],
    state: &HubState,
    target_id: &str,
    session_type_id: &str,
    logical_byte_limit: usize,
) -> SessionTypeResult<Option<HubSessionType>> {
    bounded_catalog::show(
        records,
        state,
        target_id,
        session_type_id,
        logical_byte_limit,
    )
}

/// Return one enabled effective session_type eligible at one admitted spawn point.
pub fn show_session_type_for_target(
    records: &[&PackageRecord],
    state: &HubState,
    target_id: &str,
    session_type_id: &str,
) -> SessionTypeResult<HubSessionType> {
    match find_source_session_type_for_target(records, state, session_type_id, target_id) {
        Ok((source, row)) => {
            if !source.available || !row.available {
                return Err(SessionTypeError::new(
                    "session_type_not_eligible",
                    "session type is not eligible for the requested target",
                ));
            }
            Ok(row)
        }
        Err(error) if error.kind == "unknown_session_type" => {
            // Distinguish "id does not exist anywhere" from "exists but not at T"
            // (including qualified ids of precedence losers that list omits).
            match find_source_session_type_with_row(records, state, session_type_id) {
                Ok(_) => Err(SessionTypeError::new(
                    "session_type_not_eligible",
                    "session type is not eligible for the requested target",
                )),
                Err(global_error) => Err(global_error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Return one effective session_type row by bare or full id.
pub fn show_session_type(
    records: &[&PackageRecord],
    state: &HubState,
    session_type_id: &str,
) -> SessionTypeResult<HubSessionType> {
    find_source_session_type_with_row(records, state, session_type_id).map(|(_, row)| row)
}

/// Return the authored definition backing one editable session_type.
///
/// The sanitized [`HubSessionType`] row derives a working-directory policy string
/// and omits the authored environment, so a client that reads a row and submits it
/// through [`SessionTypeMutation::Update`] — which replaces the definition
/// wholesale — silently drops both. This read returns exactly what `Update`
/// consumes, so a read-modify-write edit is lossless. Package-owned ids are
/// refused with the same error kind [`mutate_session_type`] returns, so
/// package-authored environments are never exposed.
pub fn show_session_type_definition(
    records: &[&PackageRecord],
    state: &HubState,
    session_type_id: &str,
) -> SessionTypeResult<HubSessionTypeDefinition> {
    let (source, row) = find_source_session_type_with_row(records, state, session_type_id)?;
    let mutation_source = match source.rank {
        SessionTypeSourceRank::Device => SessionTypeMutationSource::Device,
        SessionTypeSourceRank::Repo => SessionTypeMutationSource::Repo {
            target_id: source.source_name.clone(),
        },
        SessionTypeSourceRank::Package => {
            return Err(SessionTypeError::new(
                "read_only_session_type_source",
                format!(
                    "package session types are read-only: {}",
                    source.source_name
                ),
            ));
        }
    };
    Ok(HubSessionTypeDefinition {
        session_type_id: row.session_type_id,
        source: mutation_source,
        definition: source.session_type,
    })
}

fn session_type_row_from_source(source: &SourceSessionType) -> HubSessionType {
    HubSessionType {
        session_type_id: source_session_type_id(source),
        source_name: source.source_name.clone(),
        id: source.session_type.id.clone(),
        source: source.source.clone(),
        editable: source.rank != SessionTypeSourceRank::Package,
        overridden_sources: Vec::new(),
        diagnostics: Vec::new(),
        label: source.session_type.label.clone(),
        description: source.session_type.description.clone(),
        icon: source.session_type.icon.clone(),
        role: source.session_type.role.clone(),
        interaction: source.session_type.interaction.clone(),
        traits: source.session_type.traits.clone(),
        lifecycle: source.session_type.lifecycle.clone(),
        execution: source.session_type.execution.clone(),
        command: source.session_type.command.clone(),
        args: source.session_type.args.clone(),
        working_directory_policy: match &source.session_type.working_directory {
            PackageSessionTypeWorkingDirectory::PackageRoot => "package_root".to_string(),
            PackageSessionTypeWorkingDirectory::Relative { .. } => "relative".to_string(),
        },
        allowed_environment_overrides: source.session_type.allowed_environment_overrides.clone(),
        context_keys: source.session_type.context.clone(),
        target_id: source_default_target_id(source),
        available: source.available,
    }
}

fn effective_session_type_rows(
    sources: Vec<SourceSessionType>,
) -> SessionTypeResult<Vec<HubSessionType>> {
    let mut by_id = BTreeMap::<String, Vec<SourceSessionType>>::new();
    for source in sources {
        by_id
            .entry(source.session_type.id.clone())
            .or_default()
            .push(source);
    }

    by_id
        .into_values()
        .map(|sources| {
            let winner = choose_effective_session_type(sources.clone())?;
            Ok(effective_session_type_row(&winner, &sources))
        })
        .collect()
}

fn session_type_metadata(session_type: &HubSessionType) -> CoreSessionMetadata {
    let mut entries = BTreeMap::from([
        (
            "botster.session_type.id".to_string(),
            session_type.session_type_id.clone(),
        ),
        (
            "botster.session_type.source".to_string(),
            session_type.source.clone(),
        ),
        (
            "botster.session_type.role".to_string(),
            session_type.role.clone(),
        ),
        (
            "botster.session_type.interaction".to_string(),
            session_type.interaction.clone(),
        ),
        (
            "botster.session_type.lifecycle".to_string(),
            session_type.lifecycle.clone(),
        ),
    ]);
    entries.insert(
        "botster.session_type.traits".to_string(),
        serde_json::to_string(&session_type.traits).expect("string traits serialize"),
    );
    CoreSessionMetadata::from_entries(entries)
}

fn source_session_type_id(source: &SourceSessionType) -> String {
    format!("{}/{}", source.source_name, source.session_type.id)
}

fn source_default_target_id(source: &SourceSessionType) -> String {
    source
        .session_type
        .target_id
        .clone()
        .unwrap_or_else(|| match source.rank {
            SessionTypeSourceRank::Package => package_target_id(&source.source_name),
            SessionTypeSourceRank::Device => DEFAULT_DEVICE_TARGET_ID.to_string(),
            SessionTypeSourceRank::Repo => source.source_name.clone(),
        })
}

/// Validate that `target_id` names an enabled admitted spawn point.
fn ensure_enabled_admitted_target(
    state: &HubState,
    target_id: &str,
) -> SessionTypeResult<SpawnTarget> {
    match list_spawn_targets(&state.spawn_targets)
        .into_iter()
        .find(|target| target.target_id == target_id)
    {
        Some(target) if target.enabled => Ok(target),
        Some(_) => Err(SessionTypeError::new(
            "target_not_admitted",
            "spawn target is not enabled",
        )),
        None => Err(SessionTypeError::new(
            "target_not_found",
            "spawn target was not found",
        )),
    }
}

fn ensure_enabled_admitted_target_borrowed<'a>(
    state: &'a HubState,
    target_id: &str,
) -> SessionTypeResult<&'a SpawnTarget> {
    match state
        .spawn_targets
        .iter()
        .find(|target| target.target_id == target_id)
    {
        Some(target) if target.enabled => Ok(target),
        Some(_) => Err(SessionTypeError::new(
            "target_not_admitted",
            "spawn target is not enabled",
        )),
        None => Err(SessionTypeError::new(
            "target_not_found",
            "spawn target was not found",
        )),
    }
}

/// Option A eligibility: device Global types are multi-target at every admitted T.
///
/// - **Device**: eligible at every enabled admitted T unless an exclusive authored
///   `target_id` pin points elsewhere.
/// - **Repo**: only for that target's repo source (`source_name == T`).
/// - **Package**: default `package:{name}` pin or explicit authored `target_id` must equal T.
fn is_eligible_for_target(source: &SourceSessionType, target_id: &str) -> bool {
    if !source.available {
        return false;
    }
    match source.rank {
        SessionTypeSourceRank::Device => match source.session_type.target_id.as_deref() {
            Some(pin) => pin == target_id,
            None => true,
        },
        SessionTypeSourceRank::Repo => source.source_name == target_id,
        SessionTypeSourceRank::Package => source_default_target_id(source) == target_id,
    }
}

/// Management-catalog lookup: global sources, then package < device < repo precedence.
fn find_source_session_type_with_row(
    records: &[&PackageRecord],
    state: &HubState,
    session_type_id: &str,
) -> SessionTypeResult<(SourceSessionType, HubSessionType)> {
    let sources = source_session_types(records, state)?;
    let matches = sources
        .iter()
        .filter(|source| {
            source.session_type.id == session_type_id
                || source_session_type_id(source) == session_type_id
        })
        .cloned()
        .collect::<Vec<_>>();
    let winner = choose_effective_session_type(matches.clone())?;
    let peers = sources
        .into_iter()
        .filter(|source| source.session_type.id == winner.session_type.id)
        .collect::<Vec<_>>();
    let row = effective_session_type_row(&winner, &peers);
    Ok((winner, row))
}

/// Canonical target-scoped effective winners used by list, show, resolve, and spawn.
///
/// Steps: validate T → filter sources eligible for T → package < device < repo
/// precedence within that set → project `target_id = T`. Selection for spawn must
/// match only these winners (bare id or the winner's qualified id), never an
/// overridden loser's qualified id that list does not return.
fn target_scoped_effective_winners(
    records: &[&PackageRecord],
    state: &HubState,
    target_id: &str,
) -> SessionTypeResult<Vec<(SourceSessionType, HubSessionType)>> {
    let _target = ensure_enabled_admitted_target(state, target_id)?;
    let sources = source_session_types(records, state)?;
    let eligible = sources
        .into_iter()
        .filter(|source| is_eligible_for_target(source, target_id))
        .collect::<Vec<_>>();

    let mut by_id = BTreeMap::<String, Vec<SourceSessionType>>::new();
    for source in eligible {
        by_id
            .entry(source.session_type.id.clone())
            .or_default()
            .push(source);
    }

    by_id
        .into_values()
        .map(|peers| {
            let winner = choose_effective_session_type(peers.clone())?;
            let mut row = effective_session_type_row(&winner, &peers);
            row.target_id = target_id.to_string();
            Ok((winner, row))
        })
        .collect()
}

/// Spawn-point lookup: select only from the same effective set list returns.
fn find_source_session_type_for_target(
    records: &[&PackageRecord],
    state: &HubState,
    session_type_id: &str,
    target_id: &str,
) -> SessionTypeResult<(SourceSessionType, HubSessionType)> {
    let winners = target_scoped_effective_winners(records, state, target_id)?;
    // Bare id matches the winner only. Qualified id must be the winner's
    // effective id — never a lower-precedence peer that list hides.
    let matches = winners
        .into_iter()
        .filter(|(_, row)| row.id == session_type_id || row.session_type_id == session_type_id)
        .collect::<Vec<_>>();
    match matches.len() {
        0 => Err(SessionTypeError::new(
            "unknown_session_type",
            "session type was not found",
        )),
        1 => Ok(matches.into_iter().next().expect("len checked")),
        _ => Err(SessionTypeError::new(
            "ambiguous_session_type",
            "session type id matches more than one source at the same precedence",
        )),
    }
}

/// Resolve the definition and optional admitted spawn point for materialization.
fn resolve_materialization_source(
    records: &[&PackageRecord],
    state: &HubState,
    session_type_id: &str,
    request_target_id: Option<&str>,
) -> SessionTypeResult<(SourceSessionType, HubSessionType, Option<SpawnTarget>)> {
    if let Some(target_id) = request_target_id {
        // Enabled admitted spawn points use target-scoped eligibility (Option A).
        match ensure_enabled_admitted_target(state, target_id) {
            Ok(target) => {
                let (source, row) = find_source_session_type_for_target(
                    records,
                    state,
                    session_type_id,
                    target_id,
                )?;
                return Ok((source, row, Some(target)));
            }
            Err(error) if error.kind == "target_not_admitted" => {
                // Known but disabled spawn point — fail closed with the same kind.
                return Err(error);
            }
            Err(_) => {
                // Not an admitted spawn point. May still be a source pin
                // (`package:{name}`, `device:local`). Wrong pins stay
                // `target_not_admitted` for package/repo compatibility.
                let (source, row) =
                    find_source_session_type_with_row(records, state, session_type_id)?;
                let default_target_id = source_default_target_id(&source);
                if target_id != default_target_id {
                    return Err(SessionTypeError::new(
                        "target_not_admitted",
                        "requested spawn target is not admitted for this session_type",
                    ));
                }
                return Ok((source, row, None));
            }
        }
    }

    let (source, row) = find_source_session_type_with_row(records, state, session_type_id)?;
    let default_target_id = source_default_target_id(&source);
    let resolved_target_id = source
        .session_type
        .target_id
        .clone()
        .unwrap_or_else(|| default_target_id.clone());

    // Bare resolve without an explicit target keeps prior default-target semantics
    // (device:local / package:name / repo target). When that id is an enabled
    // admitted spawn point, bind cwd to it; device Global bare resolve stays on
    // the device source root.
    if let Ok(target) = ensure_enabled_admitted_target(state, &resolved_target_id) {
        if !is_eligible_for_target(&source, &target.target_id) {
            return Err(SessionTypeError::new(
                "target_not_admitted",
                "requested spawn target is not admitted for this session_type",
            ));
        }
        return Ok((source, row, Some(target)));
    }

    if resolved_target_id != default_target_id {
        return Err(SessionTypeError::new(
            "target_not_admitted",
            "requested spawn target is not admitted for this session_type",
        ));
    }
    Ok((source, row, None))
}

fn effective_session_type_row(
    winner: &SourceSessionType,
    sources: &[SourceSessionType],
) -> HubSessionType {
    let mut row = session_type_row_from_source(winner);
    row.overridden_sources = sources
        .iter()
        .filter(|source| source.rank < winner.rank)
        .map(|source| HubSessionTypeSource {
            kind: source.source.clone(),
            name: source.source_name.clone(),
        })
        .collect();
    if !row.overridden_sources.is_empty() {
        row.diagnostics.push(format!(
            "overrides {} lower-precedence definition(s)",
            row.overridden_sources.len()
        ));
    }
    row
}

fn choose_effective_session_type(
    mut matches: Vec<SourceSessionType>,
) -> SessionTypeResult<SourceSessionType> {
    if matches.is_empty() {
        return Err(SessionTypeError::new(
            "unknown_session_type",
            "session type was not found",
        ));
    }
    matches.sort_by_key(|source| source.rank);
    let best_rank = matches
        .last()
        .expect("matches is not empty after early return")
        .rank;
    let mut best = matches
        .into_iter()
        .filter(|source| source.rank == best_rank)
        .collect::<Vec<_>>();
    match best.len() {
        1 => Ok(best.remove(0)),
        _ => Err(SessionTypeError::new(
            "ambiguous_session_type",
            "session type id matches more than one source at the same precedence",
        )),
    }
}

fn choose_effective_session_type_ref(
    matches: &[SourceSessionType],
) -> SessionTypeResult<&SourceSessionType> {
    let Some(best_rank) = matches.iter().map(|source| source.rank).max() else {
        return Err(SessionTypeError::new(
            "unknown_session_type",
            "session type was not found",
        ));
    };
    let mut best = matches.iter().filter(|source| source.rank == best_rank);
    let winner = best.next().expect("best rank came from one source");
    if best.next().is_some() {
        return Err(SessionTypeError::new(
            "ambiguous_session_type",
            "session type id matches more than one source at the same precedence",
        ));
    }
    Ok(winner)
}

fn source_session_types(
    records: &[&PackageRecord],
    state: &HubState,
) -> SessionTypeResult<Vec<SourceSessionType>> {
    source_session_types_with_staged_repo(records, state, None)
}

fn source_session_types_with_staged_repo(
    records: &[&PackageRecord],
    state: &HubState,
    staged_repo: Option<(&str, &[PackageSessionType])>,
) -> SessionTypeResult<Vec<SourceSessionType>> {
    let mut sources = Vec::new();
    for record in records {
        let root = package_root(record).ok();
        for session_type in &record.session_types {
            validate_session_type(session_type)?;
            if let Some(root) = &root {
                sources.push(SourceSessionType {
                    rank: SessionTypeSourceRank::Package,
                    source: PACKAGE_SESSION_TYPE_SOURCE.to_string(),
                    source_name: record.manifest.name.clone(),
                    root: root.clone(),
                    session_type: session_type.clone(),
                    available: record.state == PackageState::Enabled,
                });
            }
        }
    }

    for device_source in &state.device_session_type_sources {
        validate_session_types(&device_source.session_types)
            .map_err(|message| SessionTypeError::new("invalid_device_session_types", message))?;
        for session_type in &device_source.session_types {
            sources.push(SourceSessionType {
                rank: SessionTypeSourceRank::Device,
                source: DEVICE_SESSION_TYPE_SOURCE.to_string(),
                source_name: DEVICE_SESSION_TYPE_SOURCE.to_string(),
                root: device_source.root.clone(),
                session_type: session_type.clone(),
                available: true,
            });
        }
    }

    for target in state.spawn_targets.iter().filter(|target| target.enabled) {
        if !target.enabled {
            continue;
        }
        let repo_session_types = match staged_repo {
            Some((target_id, definitions)) if target.target_id == target_id => definitions.to_vec(),
            _ => repo_session_types(&target.root)?,
        };
        validate_session_types(&repo_session_types)
            .map_err(|message| SessionTypeError::new("invalid_repo_session_types", message))?;
        for session_type in repo_session_types {
            sources.push(SourceSessionType {
                rank: SessionTypeSourceRank::Repo,
                source: REPO_SESSION_TYPE_SOURCE.to_string(),
                source_name: target.target_id.clone(),
                root: target.root.clone(),
                session_type,
                available: true,
            });
        }
    }

    Ok(sources)
}

fn repo_session_types(root: &Path) -> SessionTypeResult<Vec<PackageSessionType>> {
    let path = root.join(REPO_SESSION_TYPES_FILE);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(SessionTypeError::new(
                "invalid_repo_session_types",
                format!("repo-local session type file could not be read: {error}"),
            ));
        }
    };
    if bytes.len() > REPO_SESSION_TYPES_FILE_BYTE_CAPACITY {
        return Err(SessionTypeError::new(
            "repo_session_types_too_large",
            format!(
                "repo-local session type file exceeds {} bytes",
                REPO_SESSION_TYPES_FILE_BYTE_CAPACITY
            ),
        ));
    }
    let file: RepoSessionTypesFile = serde_json::from_slice(&bytes).map_err(|error| {
        SessionTypeError::new(
            "invalid_repo_session_types",
            format!("repo-local session type file is invalid: {error}"),
        )
    })?;
    Ok(file.session_types)
}

/// Validate repo-local `.botster/session-types.json` at `root` with the same
/// loader ListSessionTypes uses. Missing file is valid (empty contribution).
pub fn validate_repo_session_types_at(root: &Path) -> SessionTypeResult<()> {
    let session_types = repo_session_types(root)?;
    validate_session_types(&session_types)
        .map_err(|message| SessionTypeError::new("invalid_repo_session_types", message))?;
    Ok(())
}

fn validate_session_type(session_type: &PackageSessionType) -> SessionTypeResult<()> {
    if !bounded_token(&session_type.id, 128, false) {
        return Err(SessionTypeError::new(
            "invalid_session_type",
            "session type id must be a non-empty token of at most 128 characters",
        ));
    }
    if session_type.label.trim().is_empty() || session_type.label.len() > 120 {
        return Err(SessionTypeError::new(
            "invalid_session_type",
            "session type label must be between 1 and 120 characters",
        ));
    }
    if session_type
        .description
        .as_ref()
        .is_some_and(|value| value.len() > 1024)
        || session_type
            .icon
            .as_ref()
            .is_some_and(|value| value.len() > 256)
    {
        return Err(SessionTypeError::new(
            "invalid_session_type",
            "session type presentation metadata exceeds its size limit",
        ));
    }
    if !bounded_token(&session_type.role, 128, true) {
        return Err(SessionTypeError::new(
            "invalid_session_type_role",
            "session type role must be a namespaced token",
        ));
    }
    if !bounded_token(&session_type.interaction, 64, false)
        || !bounded_token(&session_type.lifecycle, 64, false)
    {
        return Err(SessionTypeError::new(
            "invalid_session_type_semantics",
            "session type interaction and lifecycle must be bounded tokens",
        ));
    }
    if session_type.traits.len() > 32
        || session_type
            .traits
            .iter()
            .any(|value| !bounded_token(value, 128, false))
        || session_type
            .traits
            .iter()
            .enumerate()
            .any(|(index, value)| session_type.traits[..index].contains(value))
    {
        return Err(SessionTypeError::new(
            "invalid_session_type_traits",
            "session type traits must be unique bounded tokens",
        ));
    }
    match session_type.execution {
        PackageSessionTypeExecution::RelativeExecutable => {
            validate_relative_manifest_path(&session_type.command, "command")?;
        }
        PackageSessionTypeExecution::ShellCommand => {
            if session_type.command.trim().is_empty() {
                return Err(SessionTypeError::new(
                    "invalid_session_type_command",
                    "session type shell command must not be empty",
                ));
            }
        }
    }
    if let PackageSessionTypeWorkingDirectory::Relative { path } = &session_type.working_directory {
        validate_relative_manifest_path(path, "working directory")?;
    }
    for name in session_type.environment.keys() {
        validate_environment_name(name)?;
    }
    for name in &session_type.allowed_environment_overrides {
        validate_environment_name(name)?;
    }
    Ok(())
}

fn bounded_token(value: &str, maximum: usize, require_namespace: bool) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && (!require_namespace || value.contains('.'))
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

pub fn validate_session_types(session_types: &[PackageSessionType]) -> Result<(), String> {
    for (index, session_type) in session_types.iter().enumerate() {
        validate_session_type(session_type).map_err(|error| error.message)?;
        if session_types[..index]
            .iter()
            .any(|existing| existing.id == session_type.id)
        {
            return Err("duplicate session type id".to_string());
        }
    }
    Ok(())
}

fn package_root(record: &PackageRecord) -> SessionTypeResult<PathBuf> {
    match &record.manifest.source {
        Some(PackageSource::Path { path }) => Ok(PathBuf::from(path)),
        _ => Err(SessionTypeError::new(
            "session_type_unavailable",
            "session type package has no local package root",
        )),
    }
}

fn resolve_working_directory(
    package_root: &Path,
    session_type: &PackageSessionType,
) -> SessionTypeResult<PathBuf> {
    match &session_type.working_directory {
        PackageSessionTypeWorkingDirectory::PackageRoot => Ok(package_root.to_path_buf()),
        PackageSessionTypeWorkingDirectory::Relative { path } => Ok(package_root.join(path)),
    }
}

fn resolve_command_path(package_root: &Path, command: &str) -> PathBuf {
    package_root.join(command)
}

fn resolve_execution(
    config: &HubConfig,
    command_root: &Path,
    session_type: &PackageSessionType,
) -> (String, Vec<String>) {
    match session_type.execution {
        PackageSessionTypeExecution::RelativeExecutable => (
            resolve_command_path(command_root, &session_type.command)
                .display()
                .to_string(),
            session_type.args.clone(),
        ),
        PackageSessionTypeExecution::ShellCommand => {
            let mut arguments = vec![
                "-c".to_string(),
                session_type.command.clone(),
                "botster-session-type".to_string(),
            ];
            arguments.extend(session_type.args.clone());
            (config.session_defaults.shell.clone(), arguments)
        }
    }
}

fn validate_relative_manifest_path(value: &str, label: &str) -> SessionTypeResult<()> {
    let relative = Path::new(value);
    if value.trim().is_empty()
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(SessionTypeError::new(
            "invalid_session_type_path",
            format!("session type {label} is unsafe"),
        ));
    }
    Ok(())
}

fn validate_environment_name(name: &str) -> SessionTypeResult<()> {
    let valid = !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        && !name.as_bytes()[0].is_ascii_digit();
    if valid {
        Ok(())
    } else {
        Err(SessionTypeError::new(
            "invalid_environment",
            "invalid environment variable name",
        ))
    }
}

fn inject_context_environment(
    config: &HubConfig,
    environment: &mut BTreeMap<String, String>,
    session_id: &SessionId,
    context_id: &str,
) {
    environment.insert("BOTSTER_SESSION_ID".to_string(), session_id.0.clone());
    environment.insert("BOTSTER_CONTEXT_ID".to_string(), context_id.to_string());
    environment.insert(
        "BOTSTER_HUB_DATA_DIR".to_string(),
        absolute_path(&config.data_directory).display().to_string(),
    );
    environment.insert("BOTSTER_HUB_SOCKET".to_string(), hub_socket_path(config));
    if let Ok(current_exe) = std::env::current_exe() {
        environment.insert(
            "BOTSTER_HUB_BIN".to_string(),
            current_exe.display().to_string(),
        );
    }
}

struct ContextAssemblyInputs<'a> {
    session_id: &'a SessionId,
    context_id: &'a str,
    target_id: &'a str,
    package_root: &'a Path,
    working_directory: &'a Path,
}

fn assemble_context(
    config: &HubConfig,
    trusted: ContextAssemblyInputs<'_>,
    input: SessionTypeContextInput,
    declared_keys: &[String],
) -> HubSessionContext {
    let mut values = BTreeMap::new();
    values.insert("session_id".to_string(), trusted.session_id.0.clone());
    values.insert("context_id".to_string(), trusted.context_id.to_string());
    values.insert("target_id".to_string(), trusted.target_id.to_string());
    values.insert(
        "session_dir".to_string(),
        absolute_path(&config.data_directory)
            .join("sessions")
            .display()
            .to_string(),
    );
    values.insert("hub_socket".to_string(), hub_socket_path(config));
    values.insert(
        "repo_path".to_string(),
        input
            .repo_path
            .unwrap_or_else(|| trusted.package_root.display().to_string()),
    );
    values.insert(
        "worktree_path".to_string(),
        input
            .worktree_path
            .unwrap_or_else(|| trusted.working_directory.display().to_string()),
    );
    insert_optional(&mut values, "branch_name", input.branch_name);
    insert_optional(&mut values, "prompt", input.prompt);
    insert_optional(&mut values, "ticket_id", input.ticket_id);
    insert_optional(&mut values, "workspace_id", input.workspace_id);
    for (key, value) in input.metadata {
        if key
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            values.insert(format!("metadata.{key}"), value);
        }
    }
    for key in declared_keys {
        values.entry(key.clone()).or_default();
    }
    HubSessionContext {
        context_id: trusted.context_id.to_string(),
        session_id: trusted.session_id.clone(),
        values,
    }
}

fn insert_optional(values: &mut BTreeMap<String, String>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        values.insert(key.to_string(), value);
    }
}

fn hub_socket_path(config: &HubConfig) -> String {
    config
        .transports
        .local_socket
        .as_ref()
        .map(|socket| absolute_path(&socket.path).display().to_string())
        .unwrap_or_default()
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn package_target_id(package_name: &str) -> String {
    format!("package:{package_name}")
}

#[cfg(test)]
mod bounded_catalog_tests {
    use super::*;
    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
    use crate::packages::PackageRegistry;
    use crate::persistence::DeviceSessionTypeSource;

    fn temporary_root(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("botster-session-type-{label}-{unique}"))
    }

    #[test]
    fn repo_file_snapshot_restores_exact_bytes_and_absence() {
        let root = temporary_root("rollback");
        fs::create_dir_all(root.join(".botster")).expect("create repo metadata directory");
        let path = root.join(REPO_SESSION_TYPES_FILE);
        let original = br#"{"session_types":[]}"#;
        fs::write(&path, original).expect("write original repo metadata");

        let present = snapshot_repo_session_type_file(&root, original.len())
            .expect("snapshot present repo metadata");
        fs::write(&path, b"replacement").expect("replace repo metadata");
        restore_repo_session_type_file(&root, &present).expect("restore repo metadata bytes");
        assert_eq!(
            fs::read(&path).expect("read restored repo metadata"),
            original
        );

        fs::remove_file(&path).expect("remove repo metadata");
        let missing = snapshot_repo_session_type_file(&root, 1).expect("snapshot absence");
        fs::write(&path, b"new").expect("create repo metadata after snapshot");
        restore_repo_session_type_file(&root, &missing).expect("restore repo metadata absence");
        assert!(!path.exists());
        fs::remove_dir_all(root).expect("remove rollback test directory");
    }

    #[test]
    fn repo_file_snapshot_enforces_the_logical_byte_limit() {
        let root = temporary_root("rollback-bound");
        fs::create_dir_all(root.join(".botster")).expect("create repo metadata directory");
        fs::write(root.join(REPO_SESSION_TYPES_FILE), b"1234")
            .expect("write bounded repo metadata");

        let error = snapshot_repo_session_type_file(&root, 3)
            .expect_err("snapshot must reject bytes above its limit");
        assert_eq!(error.kind, "repo_session_types_too_large");
        fs::remove_dir_all(root).expect("remove rollback bound test directory");
    }

    #[test]
    fn oversized_repo_metadata_stops_at_the_bounded_reader() {
        let root = temporary_root("bound");
        fs::create_dir_all(root.join(".botster")).expect("create repo metadata directory");
        fs::write(root.join(REPO_SESSION_TYPES_FILE), vec![b' '; 33])
            .expect("write oversized repo metadata");
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(root.join("data")),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build test config");
        let mut state = HubState::from_config(&config);
        state.spawn_targets.push(SpawnTarget {
            target_id: "bounded-repo".to_string(),
            label: "Bounded repo".to_string(),
            root: root.clone(),
            enabled: true,
            kind: "directory".to_string(),
            base_ref: None,
            metadata: BTreeMap::new(),
        });

        let result = list_session_types_bounded(&[], &state, 64)
            .expect("bounded catalog returns a resource result");
        assert!(result.is_none());

        fs::remove_dir_all(root).expect("remove bounded catalog test directory");
    }

    #[test]
    fn bounded_show_preserves_not_eligible_for_a_qualified_hidden_source() {
        let root = temporary_root("not-eligible");
        fs::create_dir_all(&root).expect("create target root");
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(root.join("data")),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build test config");
        let mut state = HubState::from_config(&config);
        state.spawn_targets.push(SpawnTarget {
            target_id: "requested-target".to_string(),
            label: "Requested target".to_string(),
            root: root.clone(),
            enabled: true,
            kind: "directory".to_string(),
            base_ref: None,
            metadata: BTreeMap::new(),
        });
        state.device_session_type_sources = vec![DeviceSessionTypeSource {
            root: root.clone(),
            session_types: vec![PackageSessionType {
                id: "hidden".to_string(),
                label: "Hidden".to_string(),
                description: None,
                icon: None,
                role: "botster.agent".to_string(),
                interaction: "interactive".to_string(),
                traits: Vec::new(),
                lifecycle: "task".to_string(),
                execution: PackageSessionTypeExecution::RelativeExecutable,
                command: "bin/agent".to_string(),
                args: Vec::new(),
                working_directory: PackageSessionTypeWorkingDirectory::PackageRoot,
                environment: BTreeMap::new(),
                allowed_environment_overrides: Vec::new(),
                context: Vec::new(),
                target_id: Some("another-target".to_string()),
            }],
        }];

        let error = show_session_type_for_target_bounded(
            &[],
            &state,
            "requested-target",
            "device/hidden",
            64 * 1024,
        )
        .expect_err("qualified hidden source remains ineligible");
        assert_eq!(error.kind, "session_type_not_eligible");
        fs::remove_dir_all(root).expect("remove not-eligible test root");
    }

    #[test]
    fn bounded_show_rejects_a_disabled_package_source_as_not_eligible() {
        let root = temporary_root("disabled-package");
        fs::create_dir_all(&root).expect("create package root");
        fs::write(root.join("plugin.lua"), "return botster.register({})\n").expect("write plugin");
        fs::write(
            root.join("botster-package.json"),
            r#"{
              "name":"disabled.source","version":"1.0.0","kind":"plugin",
              "botster":">=0.1.0","source":{"type":"path","path":"."},
              "capabilities":[],"entrypoints":[{"runtime":"lua","path":"plugin.lua","bootstrap":false}],
              "session_types":[{"id":"agent","label":"Agent","role":"botster.agent",
                "interaction":"interactive","lifecycle":"task","command":"bin/agent"}]
            }"#,
        )
        .expect("write manifest");
        let mut registry = PackageRegistry::new(botster_core::CapabilitySet::new());
        registry
            .install_local_path(&root, "install disabled source")
            .expect("install package without enabling it");
        let records = registry.packages().into_iter().cloned().collect::<Vec<_>>();

        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(root.join("data")),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build test config");
        let mut state = HubState::from_config(&config);
        state.spawn_targets.push(SpawnTarget {
            target_id: "package:disabled.source".to_string(),
            label: "Disabled package".to_string(),
            root: root.clone(),
            enabled: true,
            kind: "directory".to_string(),
            base_ref: None,
            metadata: BTreeMap::new(),
        });

        let error = show_session_type_for_target_bounded(
            &records,
            &state,
            "package:disabled.source",
            "disabled.source/agent",
            64 * 1024,
        )
        .expect_err("disabled package row is unavailable");
        assert_eq!(error.kind, "session_type_not_eligible");
        fs::remove_dir_all(root).expect("remove disabled-package root");
    }
}
