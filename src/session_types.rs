//! Hub-owned session type resolution, authority, and context policy.
//!
//! Packages may contribute declarations, but the hub validates and materializes
//! them into generic core spawn requests before `botster-core` sees anything.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

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

#[allow(dead_code)] // The full-definition counting seed will use this storage term.
mod content_budget;

#[allow(dead_code)] // The admitted full-definition parser will use this counting seed.
mod definition_budget;

#[allow(dead_code)] // The full-definition materialization permit will own these errors.
mod materialization_error;

#[allow(dead_code)] // The charged materialization caller is not connected yet.
mod materialization_walk;

#[allow(dead_code)] // The charged materialization boundary owns this model.
mod materialization_timeline;

#[allow(dead_code)] // The full-definition counting walk will supply scratch events.
mod scratch_budget;

#[allow(dead_code)] // The full-definition counting seed will dispatch its tagged fields here.
mod tagged_budget;

#[cfg(feature = "allocation-oracle")]
pub(crate) mod parser_probe;

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

/// A Host product and the variable allowance that moves with its payload.
/// Only this module can construct the product after charged materialization.
pub(crate) struct ChargedSessionTypeMaterialization {
    materialized: MaterializedSessionType,
    // The Core owner destroys every moved field before these allowances.
    allowance: ChargedMaterializationAllowance,
}

/// One open parent funds later Core replies. Fixed output charges stay sealed.
pub(crate) struct ChargedMaterializationAllowance {
    pub(crate) parent: crate::lua_memory::LuaCallbackCharge,
    row: crate::lua_memory::LuaCallbackCharge,
    environment: crate::lua_memory::LuaCallbackCharge,
    prefix: crate::lua_memory::LuaCallbackCharge,
    execution: crate::lua_memory::LuaCallbackCharge,
    metadata: crate::lua_memory::LuaCallbackCharge,
    context: crate::lua_memory::LuaCallbackCharge,
    environment_injection: crate::lua_memory::LuaCallbackCharge,
    output_copies: crate::lua_memory::LuaCallbackCharge,
}

#[cfg(test)]
impl ChargedMaterializationAllowance {
    pub(crate) fn empty_for_test(mut parent: crate::lua_memory::LuaCallbackCharge) -> Self {
        let row = parent.split_fixed(0).expect("the test parent remains open");
        let environment = parent.split_fixed(0).expect("the test parent remains open");
        let prefix = parent.split_fixed(0).expect("the test parent remains open");
        let execution = parent.split_fixed(0).expect("the test parent remains open");
        let metadata = parent.split_fixed(0).expect("the test parent remains open");
        let context = parent.split_fixed(0).expect("the test parent remains open");
        let environment_injection = parent.split_fixed(0).expect("the test parent remains open");
        let output_copies = parent.split_fixed(0).expect("the test parent remains open");
        Self {
            parent,
            row,
            environment,
            prefix,
            execution,
            metadata,
            context,
            environment_injection,
            output_copies,
        }
    }
}

impl std::fmt::Debug for ChargedSessionTypeMaterialization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ChargedSessionTypeMaterialization")
    }
}

/// Host owns the charged input and the checked materializer until it returns.
/// This type has no production constructor until the complete parser bound is proved.
pub(crate) struct SpawnHostWork {
    run: Box<
        dyn FnOnce()
                -> Result<ChargedSessionTypeMaterialization, ChargedMaterializationFailure>
            + Send,
    >,
    receipt: crate::data_plane::driver::CoreReplyPublisher<()>,
    // The closure allocation must stay funded through its destruction.
    _storage: crate::lua_memory::LuaCallbackCharge,
}

impl std::fmt::Debug for SpawnHostWork {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SpawnHostWork")
    }
}

impl SpawnHostWork {
    /// Keep the charged source parser and config live in the Host job.
    pub(crate) fn new_ordinary(
        mut parent: crate::lua_memory::LuaCallbackCharge,
        receipt: crate::data_plane::driver::CoreReplyPublisher<()>,
        config: &HubConfig,
        state: crate::runtime::HubStateView,
        package_records: Arc<Vec<PackageRecord>>,
        plugin_key: botster_core::PluginKey,
        session_type_id: String,
        request: SessionTypeRequest,
    ) -> Result<
        Self,
        (
            &'static str,
            crate::lua_memory::LuaCallbackCharge,
            crate::data_plane::driver::CoreReplyPublisher<()>,
        ),
    > {
        let config = match ChargedMaterializationConfig::from_config(config, &mut parent) {
            Ok(config) => config,
            Err(reason) => return Err((reason, parent, receipt)),
        };
        Self::new_checked(parent, receipt, move |parent| {
            materialize_ordinary_charged(
                parent,
                config,
                &state,
                &package_records,
                &plugin_key,
                &session_type_id,
                request,
            )
        })
    }

    pub(crate) fn new_checked<F>(
        mut parent: crate::lua_memory::LuaCallbackCharge,
        receipt: crate::data_plane::driver::CoreReplyPublisher<()>,
        run: F,
    ) -> Result<
        Self,
        (
            &'static str,
            crate::lua_memory::LuaCallbackCharge,
            crate::data_plane::driver::CoreReplyPublisher<()>,
        ),
    >
    where
        F: FnOnce(crate::lua_memory::LuaCallbackCharge)
                -> Result<ChargedSessionTypeMaterialization, ChargedMaterializationFailure>
            + Send
            + 'static,
    {
        let bytes = std::mem::size_of::<(F, crate::lua_memory::LuaCallbackCharge)>();
        if parent.grow(bytes).is_err() {
            return Err(("host closure capacity exhausted", parent, receipt));
        }
        let storage = parent
            .split_fixed(bytes)
            .expect("the parent admitted the Host closure");
        Ok(Self {
            run: Box::new(move || run(parent)),
            receipt,
            _storage: storage,
        })
    }

    pub(crate) fn run(self) -> SpawnHostCompletion {
        let result = (self.run)();
        SpawnHostCompletion {
            result,
            receipt: self.receipt,
        }
    }
}

/// Owner acknowledgement keeps the exact receipt publisher with the Host result.
#[derive(Debug)]
pub(crate) struct SpawnHostCompletion {
    pub(crate) result:
        Result<ChargedSessionTypeMaterialization, ChargedMaterializationFailure>,
    pub(crate) receipt: crate::data_plane::driver::CoreReplyPublisher<()>,
}

pub(crate) struct ChargedSessionTypeFailure {
    error: SessionTypeError,
    // The message and any retained materialization input precede this charge.
    _variable: crate::lua_memory::LuaCallbackCharge,
}

impl ChargedSessionTypeFailure {
    pub(crate) fn into_parts(
        self,
    ) -> (SessionTypeError, crate::lua_memory::LuaCallbackCharge) {
        (self.error, self._variable)
    }
}

pub(crate) enum ChargedMaterializationFailure {
    Capacity(&'static str),
    Semantic(ChargedSessionTypeFailure),
    Unavailable(&'static str),
}

impl std::fmt::Debug for ChargedMaterializationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capacity(reason) => formatter.debug_tuple("Capacity").field(reason).finish(),
            Self::Semantic(_) => formatter.write_str("Semantic"),
            Self::Unavailable(reason) => {
                formatter.debug_tuple("Unavailable").field(reason).finish()
            }
        }
    }
}

impl std::fmt::Debug for ChargedSessionTypeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ChargedSessionTypeFailure")
    }
}

impl ChargedSessionTypeMaterialization {
    /// The next owner must retain the allowance until its payload is destroyed.
    pub(crate) fn into_parts(
        self,
    ) -> (MaterializedSessionType, ChargedMaterializationAllowance) {
        (self.materialized, self.allowance)
    }
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

#[derive(Clone, Copy)]
struct MaterializationConfigView<'a> {
    data_directory: &'a Path,
    shell: &'a str,
    initial_rows: u16,
    initial_cols: u16,
    local_socket: Option<&'a Path>,
}

impl<'a> From<&'a HubConfig> for MaterializationConfigView<'a> {
    fn from(config: &'a HubConfig) -> Self {
        Self {
            data_directory: &config.data_directory,
            shell: &config.session_defaults.shell,
            initial_rows: config.session_defaults.initial_rows,
            initial_cols: config.session_defaults.initial_cols,
            local_socket: config.transports.local_socket.as_ref().map(|socket| socket.path.as_path()),
        }
    }
}

/// Host owns these five config values and their allocation charge.
#[allow(dead_code)]
struct ChargedMaterializationConfig {
    data_directory: PathBuf,
    shell: String,
    initial_rows: u16,
    initial_cols: u16,
    local_socket: Option<PathBuf>,
    _storage: crate::lua_memory::LuaCallbackCharge,
}

#[allow(dead_code)]
impl ChargedMaterializationConfig {
    fn from_config(
        config: &HubConfig,
        parent: &mut crate::lua_memory::LuaCallbackCharge,
    ) -> Result<Self, &'static str> {
        let view = MaterializationConfigView::from(config);
        let bytes = view
            .data_directory
            .as_os_str()
            .as_encoded_bytes()
            .len()
            .checked_add(view.shell.len())
            .and_then(|bytes| {
                bytes.checked_add(
                    view.local_socket
                        .map_or(0, |path| path.as_os_str().as_encoded_bytes().len()),
                )
            })
            .ok_or("materialization config size overflow")?;
        parent
            .grow(bytes)
            .map_err(|_| "materialization config capacity exhausted")?;
        let data_directory = view.data_directory.to_path_buf();
        let shell = view.shell.to_string();
        let local_socket = view.local_socket.map(Path::to_path_buf);
        let storage = parent
            .split_fixed(bytes)
            .ok_or("materialization config transfer failed")?;
        Ok(Self {
            data_directory,
            shell,
            initial_rows: view.initial_rows,
            initial_cols: view.initial_cols,
            local_socket,
            _storage: storage,
        })
    }

    fn view(&self) -> MaterializationConfigView<'_> {
        MaterializationConfigView {
            data_directory: &self.data_directory,
            shell: &self.shell,
            initial_rows: self.initial_rows,
            initial_cols: self.initial_cols,
            local_socket: self.local_socket.as_deref(),
        }
    }
}

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
        write_repo_session_types(&root, &definitions)?;
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

fn write_repo_session_types(
    root: &Path,
    definitions: &[PackageSessionType],
) -> SessionTypeResult<()> {
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
    let directory = root.join(".botster");
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
    if !canonical_directory.starts_with(root) {
        return Err(SessionTypeError::new(
            "target_not_admitted",
            "repo session type directory escapes the admitted target",
        ));
    }
    let temporary = root.join(REPO_SESSION_TYPES_TEMP_FILE);
    fs::write(&temporary, bytes).map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session type temporary file could not be written: {error}"),
        )
    })?;
    fs::rename(&temporary, root.join(REPO_SESSION_TYPES_FILE)).map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session type file could not be replaced: {error}"),
        )
    })
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

/// The pinned Vec push rule can hold the old and new buffers together.
/// Apply this term to each collection, never to the combined logical input.
#[allow(dead_code)]
fn vector_growth_peak<T>(len: usize) -> Option<usize> {
    if len == 0 {
        return Some(0);
    }
    let size = std::mem::size_of::<T>();
    let first = if size == 1 {
        8
    } else if size <= 1024 {
        4
    } else {
        1
    };
    len.max(first).checked_mul(size)?.checked_mul(3)
}

/// Bound one owned definition clone while its source remains live.
#[allow(dead_code)]
fn definition_clone_peak(definition: &PackageSessionType) -> Option<usize> {
    let PackageSessionType {
        id,
        label,
        description,
        icon,
        role,
        interaction,
        traits,
        lifecycle,
        execution,
        command,
        args,
        working_directory,
        environment,
        allowed_environment_overrides,
        context,
        target_id,
    } = definition;
    match execution {
        PackageSessionTypeExecution::RelativeExecutable
        | PackageSessionTypeExecution::ShellCommand => {}
    }
    let mut bytes = [
        id.len(),
        label.len(),
        role.len(),
        interaction.len(),
        lifecycle.len(),
        command.len(),
        description.as_ref().map_or(0, String::len),
        icon.as_ref().map_or(0, String::len),
        target_id.as_ref().map_or(0, String::len),
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)?;
    match working_directory {
        PackageSessionTypeWorkingDirectory::PackageRoot => {}
        PackageSessionTypeWorkingDirectory::Relative { path } => {
            bytes = bytes.checked_add(path.len())?;
        }
    }
    for strings in [
        traits,
        args,
        allowed_environment_overrides,
        context,
    ] {
        bytes = bytes.checked_add(vector_growth_peak::<String>(strings.len())?)?;
        for string in strings {
            bytes = bytes.checked_add(string.len())?;
        }
    }
    bytes = bytes.checked_add(crate::lua_memory::layout::btree_nodes_checked::<
        String,
        String,
    >(environment.len())?)?;
    for (key, value) in environment {
        bytes = bytes.checked_add(key.len())?.checked_add(value.len())?;
    }
    Some(bytes)
}

/// Charge the selected source clone before `SourceSessionType::clone`.
/// The definition term includes every environment and nested string field.
#[allow(dead_code)]
fn source_clone_peak(source: &SourceSessionType) -> Option<usize> {
    let SourceSessionType {
        rank,
        source: source_kind,
        source_name,
        root,
        session_type,
        available,
    } = source;
    let _ = (rank, available);
    source_parts_clone_peak(source_kind, source_name, root, session_type)
}

fn source_parts_clone_peak(
    source_kind: &str,
    source_name: &str,
    root: &Path,
    session_type: &PackageSessionType,
) -> Option<usize> {
    source_kind
        .len()
        .checked_add(source_name.len())?
        .checked_add(root.as_os_str().as_encoded_bytes().len())?
        .checked_add(definition_clone_peak(session_type)?)
}

/// Fund the source vector and each source clone before allocating either one.
/// The parent remains open while the caller builds all source families.
#[allow(dead_code)]
struct ChargedSourceBuilder<'a> {
    sources: Vec<SourceSessionType>,
    parent: &'a mut crate::lua_memory::LuaCallbackCharge,
    reserved: usize,
}

#[allow(dead_code)]
impl<'a> ChargedSourceBuilder<'a> {
    fn new(parent: &'a mut crate::lua_memory::LuaCallbackCharge) -> Self {
        Self {
            sources: Vec::new(),
            parent,
            reserved: 0,
        }
    }

    fn push(
        &mut self,
        rank: SessionTypeSourceRank,
        source: &str,
        source_name: &str,
        root: &Path,
        session_type: &PackageSessionType,
        available: bool,
    ) -> Result<(), &'static str> {
        let next_len = self
            .sources
            .len()
            .checked_add(1)
            .ok_or("source count overflow")?;
        let old_vector = vector_growth_peak::<SourceSessionType>(self.sources.len())
            .ok_or("source vector size overflow")?;
        let next_vector = vector_growth_peak::<SourceSessionType>(next_len)
            .ok_or("source vector size overflow")?;
        let clone = source_parts_clone_peak(source, source_name, root, session_type)
            .ok_or("source clone size overflow")?;
        let additional = next_vector
            .checked_sub(old_vector)
            .and_then(|bytes| bytes.checked_add(clone))
            .ok_or("source clone size overflow")?;
        self.parent
            .grow(additional)
            .map_err(|_| "source clone capacity exhausted")?;
        self.reserved = self
            .reserved
            .checked_add(additional)
            .expect("the parent admitted the source charge");
        self.sources.push(SourceSessionType {
            rank,
            source: source.to_string(),
            source_name: source_name.to_string(),
            root: root.to_path_buf(),
            session_type: session_type.clone(),
            available,
        });
        Ok(())
    }

    /// Keep the read bytes and parsed tree funded while cloning repo rows.
    fn validate(
        &mut self,
        check: impl FnOnce() -> SessionTypeResult<()>,
    ) -> Result<(), ChargedSourceLoadFailure> {
        let error_bytes = materialization_error::construction_error_storage_bytes(None)
            .ok_or(ChargedSourceLoadFailure::Capacity("source validation size overflow"))?;
        let original = self.parent.bytes();
        self.parent
            .grow(error_bytes)
            .map_err(|_| ChargedSourceLoadFailure::Capacity("source validation capacity exhausted"))?;
        match check() {
            Ok(()) => {
                assert!(self.parent.shrink_to(original));
                Ok(())
            }
            Err(error) => {
                let storage = self
                    .parent
                    .split_fixed(error_bytes)
                    .expect("the parent admitted the validation error");
                Err(ChargedSourceLoadFailure::Validation {
                    error,
                    _storage: storage,
                })
            }
        }
    }

    fn push_package_records(
        &mut self,
        records: &[PackageRecord],
    ) -> Result<(), ChargedSourceLoadFailure> {
        for record in records {
            let root = match &record.manifest.source {
                Some(PackageSource::Path { path }) => Some(Path::new(path)),
                _ => None,
            };
            for definition in &record.session_types {
                self.validate(|| validate_session_type(definition))?;
                if let Some(root) = root {
                    self.push(
                        SessionTypeSourceRank::Package,
                        PACKAGE_SESSION_TYPE_SOURCE,
                        &record.manifest.name,
                        root,
                        definition,
                        record.state == PackageState::Enabled,
                    )
                    .map_err(ChargedSourceLoadFailure::Capacity)?;
                }
            }
        }
        Ok(())
    }

    fn push_device_sources(&mut self, state: &HubState) -> Result<(), ChargedSourceLoadFailure> {
        for device in &state.device_session_type_sources {
            self.validate(|| {
                validate_session_types(&device.session_types).map_err(|message| {
                    SessionTypeError::new("invalid_device_session_types", message)
                })
            })?;
            for definition in &device.session_types {
                self.push(
                    SessionTypeSourceRank::Device,
                    DEVICE_SESSION_TYPE_SOURCE,
                    DEVICE_SESSION_TYPE_SOURCE,
                    &device.root,
                    definition,
                    true,
                )
                .map_err(ChargedSourceLoadFailure::Capacity)?;
            }
        }
        Ok(())
    }

    fn push_repo_file(
        &mut self,
        root: &Path,
        target_id: &str,
    ) -> Result<(), ChargedSourceLoadFailure> {
        let Some(read) = read_repo_file_charged(root, self.parent)
            .map_err(ChargedSourceLoadFailure::Read)?
        else {
            return Ok(());
        };
        let parsed = parse_repo_file_charged(read, self.parent)
            .map_err(ChargedSourceLoadFailure::Parse)?;
        self.validate(|| {
            validate_session_types(&parsed.definitions)
                .map_err(|message| SessionTypeError::new("invalid_repo_session_types", message))
        })?;
        for definition in &parsed.definitions {
            self.push(
                SessionTypeSourceRank::Repo,
                REPO_SESSION_TYPE_SOURCE,
                target_id,
                root,
                definition,
                true,
            )
            .map_err(ChargedSourceLoadFailure::Capacity)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Option<ChargedSourceSessionTypes> {
        let storage = self.parent.split_fixed(self.reserved)?;
        Some(ChargedSourceSessionTypes {
            sources: std::mem::take(&mut self.sources),
            _storage: storage,
        })
    }
}

#[allow(dead_code)]
struct ChargedSourceSessionTypes {
    sources: Vec<SourceSessionType>,
    // Source rows drop before their storage charge.
    _storage: crate::lua_memory::LuaCallbackCharge,
}

#[allow(dead_code)]
enum ChargedSourceResolveFailure {
    Capacity,
    Semantic(ChargedSessionTypeFailure),
}

#[allow(dead_code)] // The charged Host constructor will consume this selection.
impl ChargedSourceSessionTypes {
    fn resolve<'a>(
        &'a self,
        state: &'a HubState,
        session_type_id: &str,
        request_target_id: Option<&str>,
        parent: &mut crate::lua_memory::LuaCallbackCharge,
    ) -> Result<
        (
            usize,
            HubSessionType,
            crate::lua_memory::LuaCallbackCharge,
            Option<&'a SpawnTarget>,
        ),
        ChargedSourceResolveFailure,
    > {
        let error_bytes = materialization_error::construction_error_storage_bytes(None)
            .expect("the fixed source-selection error envelope fits usize");
        let original = parent.bytes();
        parent
            .grow(error_bytes)
            .map_err(|_| ChargedSourceResolveFailure::Capacity)?;
        let selected = resolve_materialization_source_borrowed(
            &self.sources,
            state,
            session_type_id,
            request_target_id,
        );
        let (winner, target) = match selected {
            Ok(selected) => {
                assert!(parent.shrink_to(original));
                selected
            }
            Err(error) => {
                let storage = parent
                    .split_fixed(error_bytes)
                    .expect("the parent admitted the source-selection error");
                return Err(ChargedSourceResolveFailure::Semantic(ChargedSessionTypeFailure {
                    error,
                    _variable: storage,
                }));
            }
        };
        let index = self
            .sources
            .iter()
            .position(|source| std::ptr::eq(source, winner))
            .expect("the winner belongs to the charged source set");
        let scoped_target = request_target_id.filter(|target_id| {
            state
                .spawn_targets
                .iter()
                .any(|target| target.enabled && target.target_id == *target_id)
        });
        let peers = self.sources.iter().filter(|source| {
            source.session_type.id == winner.session_type.id
                && scoped_target.is_none_or(|target_id| eligible_borrowed(source, target_id))
        });
        let (row, row_storage) = charged_effective_session_type_row(parent, winner, peers)
            .map_err(|_| ChargedSourceResolveFailure::Capacity)?;
        Ok((index, row, row_storage, target))
    }
}

#[allow(dead_code)]
enum ChargedSourceLoadFailure {
    Capacity(&'static str),
    Read(ChargedRepoReadFailure),
    Parse(ChargedRepoParseFailure),
    Validation {
        error: SessionTypeError,
        // The error drops before its allowance.
        _storage: crate::lua_memory::LuaCallbackCharge,
    },
}

struct CountFormattedBytes(usize);

impl std::fmt::Write for CountFormattedBytes {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}

struct BoundedFormattedMessage {
    value: String,
    maximum: usize,
}

impl std::fmt::Write for BoundedFormattedMessage {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        if self
            .value
            .len()
            .checked_add(text.len())
            .is_none_or(|length| length > self.maximum)
        {
            return Err(std::fmt::Error);
        }
        self.value.push_str(text);
        Ok(())
    }
}

fn funded_source_error(
    parent: &mut crate::lua_memory::LuaCallbackCharge,
    kind: &'static str,
    known_capacity: Option<usize>,
    display_temporary_peak: usize,
    render: impl Fn(&mut dyn std::fmt::Write) -> std::fmt::Result,
) -> Option<ChargedSessionTypeFailure> {
    let capacity = match known_capacity {
        Some(capacity) => capacity,
        None => {
            let mut count = CountFormattedBytes(0);
            render(&mut count).ok()?;
            count.0
        }
    };
    let bytes = materialization_error::formatted_error_storage_bytes(kind.len(), capacity)?
        .checked_add(display_temporary_peak)?;
    let original = parent.bytes();
    parent.grow(bytes).ok()?;
    let mut message = BoundedFormattedMessage {
        value: String::with_capacity(capacity),
        maximum: capacity,
    };
    if render(&mut message).is_err() {
        drop(message);
        assert!(parent.shrink_to(original));
        return None;
    }
    let Some(variable) = parent.split_fixed(bytes) else {
        drop(message);
        assert!(parent.shrink_to(original));
        return None;
    };
    Some(ChargedSessionTypeFailure {
        error: SessionTypeError::new(kind, message.value),
        _variable: variable,
    })
}

impl ChargedSourceLoadFailure {
    /// Capacity refusals remain distinct from existing source errors.
    fn into_semantic_failure(
        self,
        parent: &mut crate::lua_memory::LuaCallbackCharge,
    ) -> Result<ChargedSessionTypeFailure, Self> {
        match self {
            Self::Validation { error, _storage } => Ok(ChargedSessionTypeFailure {
                error,
                _variable: _storage,
            }),
            Self::Parse(ChargedRepoParseFailure::Invalid(wrapped)) => {
                let (error, variable) = wrapped.into_parts();
                Ok(ChargedSessionTypeFailure {
                    error,
                    _variable: variable,
                })
            }
            Self::Read(ChargedRepoReadFailure::TooLarge) => funded_source_error(
                parent,
                "repo_session_types_too_large",
                None,
                0,
                |output| {
                    write!(
                        output,
                        "repo-local session type file exceeds {} bytes",
                        REPO_SESSION_TYPES_FILE_BYTE_CAPACITY
                    )
                },
            )
            .ok_or(Self::Read(ChargedRepoReadFailure::TooLarge)),
            #[cfg(target_os = "macos")]
            Self::Read(ChargedRepoReadFailure::Io(error)) => {
                if error.raw_os_error().is_none() {
                    // File::open and File::read return OS errors here.
                    // Reject a future custom error before its Display runs.
                    return Err(Self::Read(ChargedRepoReadFailure::Io(error)));
                }
                // Pinned macOS Rust 1.97 io::Error Display keeps one
                // lossy-conversion String live. Its 127/254/508 growth
                // peaks at 762 bytes. Other targets need their own proof.
                let known_capacity = "repo-local session type file could not be read: "
                    .len()
                    .checked_add(404);
                let failure = known_capacity.and_then(|capacity| {
                    funded_source_error(
                        parent,
                        "invalid_repo_session_types",
                        Some(capacity),
                        762,
                        |output| {
                            write!(output, "repo-local session type file could not be read: {error}")
                        },
                    )
                });
                failure.ok_or(Self::Read(ChargedRepoReadFailure::Io(error)))
            }
            #[cfg(not(target_os = "macos"))]
            Self::Read(ChargedRepoReadFailure::Io(error)) => {
                Err(Self::Read(ChargedRepoReadFailure::Io(error)))
            }
            other => Err(other),
        }
    }
}

/// Build the same package, device, and enabled repo source set under one parent.
/// Winner selection and materialization must use this product before activation.
#[allow(dead_code)]
fn load_charged_sources(
    records: &[PackageRecord],
    state: &HubState,
    parent: &mut crate::lua_memory::LuaCallbackCharge,
) -> Result<ChargedSourceSessionTypes, ChargedSourceLoadFailure> {
    let mut builder = ChargedSourceBuilder::new(parent);
    builder.push_package_records(records)?;
    builder.push_device_sources(state)?;
    for target in state.spawn_targets.iter().filter(|target| target.enabled) {
        builder.push_repo_file(&target.root, &target.target_id)?;
    }
    builder
        .finish()
        .ok_or(ChargedSourceLoadFailure::Capacity("source charge transfer failed"))
}

/// Read complete definitions under the original input parent on Host.
/// The charged file buffer remains live during Serde. The separate parser
/// charge is admitted from that same parent before decoder allocation.
fn materialize_ordinary_charged(
    mut parent: crate::lua_memory::LuaCallbackCharge,
    config: ChargedMaterializationConfig,
    state: &HubState,
    package_records: &[PackageRecord],
    _plugin_key: &botster_core::PluginKey,
    session_type_id: &str,
    request: SessionTypeRequest,
) -> Result<ChargedSessionTypeMaterialization, ChargedMaterializationFailure> {
    let sources = match load_charged_sources(package_records, state, &mut parent) {
        Ok(sources) => sources,
        Err(error) => {
            return Err(match error.into_semantic_failure(&mut parent) {
                Ok(failure) => ChargedMaterializationFailure::Semantic(failure),
                Err(ChargedSourceLoadFailure::Capacity(reason))
                | Err(ChargedSourceLoadFailure::Read(ChargedRepoReadFailure::Capacity(reason)))
                | Err(ChargedSourceLoadFailure::Parse(ChargedRepoParseFailure::Capacity(reason))) => {
                    ChargedMaterializationFailure::Capacity(reason)
                }
                Err(_) => ChargedMaterializationFailure::Unavailable(
                    "repository source error capacity exhausted",
                ),
            });
        }
    };
    let (index, row, row_storage, target) = match sources.resolve(
        state,
        session_type_id,
        request.target_id.as_deref(),
        &mut parent,
    ) {
        Ok(selected) => selected,
        Err(error) => {
            let failure = match error {
                ChargedSourceResolveFailure::Capacity => ChargedMaterializationFailure::Capacity(
                    "session type source capacity exhausted",
                ),
                ChargedSourceResolveFailure::Semantic(failure) => {
                    ChargedMaterializationFailure::Semantic(failure)
                }
            };
            drop(sources);
            drop(config);
            return Err(failure);
        }
    };
    let (environment, environment_storage) = match charged_effective_environment(
        &mut parent,
        &sources.sources[index].session_type,
        &request,
    ) {
        Ok(environment) => environment,
        Err(error) => {
            let failure = match error.into_charged(&mut parent) {
                Ok(failure) => ChargedMaterializationFailure::Semantic(failure),
                Err(reason) => ChargedMaterializationFailure::Capacity(reason),
            };
            drop(row);
            drop(row_storage);
            drop(sources);
            drop(config);
            return Err(failure);
        }
    };
    let prefix = match charged_deterministic_prefix(
        &mut parent,
        &sources.sources[index],
        target,
        &request,
    ) {
        Ok(prefix) => prefix,
        Err(error) => {
            let failure = match error.into_charged(&mut parent) {
                Ok(failure) => ChargedMaterializationFailure::Semantic(failure),
                Err(reason) => ChargedMaterializationFailure::Capacity(reason),
            };
            drop(environment);
            drop(environment_storage);
            drop(row);
            drop(row_storage);
            drop(sources);
            drop(config);
            return Err(failure);
        }
    };
    let execution = match charged_execution(
        &mut parent,
        config.view().shell,
        &sources.sources[index],
    ) {
        Ok(execution) => execution,
        Err(reason) => {
            drop(prefix);
            drop(environment);
            drop(environment_storage);
            drop(row);
            drop(row_storage);
            drop(sources);
            drop(config);
            return Err(ChargedMaterializationFailure::Capacity(reason));
        }
    };
    let metadata = match charged_session_type_metadata(&mut parent, &row) {
        Ok(metadata) => metadata,
        Err(reason) => {
            drop(execution);
            drop(prefix);
            drop(environment);
            drop(environment_storage);
            drop(row);
            drop(row_storage);
            drop(sources);
            drop(config);
            return Err(ChargedMaterializationFailure::Capacity(reason));
        }
    };
    // Source, row, environment, prefix, execution, and metadata charges overlap.
    // The remaining output must admit its peak before its first allocation.
    drop(metadata);
    drop(execution);
    drop(prefix);
    drop(environment);
    drop(environment_storage);
    drop(row);
    drop(row_storage);
    drop(sources);
    drop(config);
    Err(ChargedMaterializationFailure::Unavailable(
        "charged output requires the startup path policy",
    ))
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
    materialize_session_type_view(config.into(), records, state, session_type_id, request)
}

fn materialize_session_type_view(
    config: MaterializationConfigView<'_>,
    records: &[&PackageRecord],
    state: &HubState,
    session_type_id: &str,
    request: SessionTypeRequest,
) -> SessionTypeResult<MaterializedSessionType> {
    let (source, row, target) = resolve_materialization_source(
        records,
        state,
        session_type_id,
        request.target_id.as_deref(),
    )?;
    materialize_session_type_from_resolved(config, request, &source, row, target.as_ref())
}

/// Use one resolved source set; the charged Host path can supply its own set.
fn materialize_session_type_from_resolved(
    config: MaterializationConfigView<'_>,
    request: SessionTypeRequest,
    source: &SourceSessionType,
    mut effective_row: HubSessionType,
    spawn_target: Option<&SpawnTarget>,
) -> SessionTypeResult<MaterializedSessionType> {
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
        .unwrap_or_else(|| source_default_target_id(source));
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
            rows: config.initial_rows,
            cols: config.initial_cols,
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
    let materialization_config = MaterializationConfigView::from(config);
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
        materialization_config,
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
    inject_context_environment(materialization_config, &mut environment, &session_id, &context_id);
    let command_root = if source.rank == SessionTypeSourceRank::Repo {
        &managed_root
    } else {
        &source.root
    };
    let row = effective_row;
    let metadata = session_type_metadata(&row);
    let (executable, arguments) =
        resolve_execution(materialization_config, command_root, &source.session_type);
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
            let winner = choose_effective_session_type_ref(sources.iter())?;
            Ok(effective_session_type_row(winner, sources.iter()))
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
    let matches = sources.iter().filter(|source| {
        source.session_type.id == session_type_id
            || source_session_type_id(source) == session_type_id
    });
    let winner = choose_effective_session_type_ref(matches)?;
    let peers = sources
        .iter()
        .filter(|source| source.session_type.id == winner.session_type.id);
    let row = effective_session_type_row(winner, peers);
    Ok((winner.clone(), row))
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
            let winner = choose_effective_session_type_ref(peers.iter())?;
            let mut row = effective_session_type_row(winner, peers.iter());
            row.target_id = target_id.to_string();
            Ok((winner.clone(), row))
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

/// Select the same winner from charged sources without cloning source rows.
fn source_matches_id(source: &SourceSessionType, id: &str) -> bool {
    source.session_type.id == id
        || id
            .strip_prefix(source.source_name.as_str())
            .and_then(|suffix| suffix.strip_prefix('/'))
            == Some(source.session_type.id.as_str())
}

fn default_target_matches(source: &SourceSessionType, target_id: &str) -> bool {
    if let Some(pin) = source.session_type.target_id.as_deref() {
        return pin == target_id;
    }
    match source.rank {
        SessionTypeSourceRank::Package => target_id
            .strip_prefix("package:")
            .is_some_and(|name| name == source.source_name),
        SessionTypeSourceRank::Device => target_id == DEFAULT_DEVICE_TARGET_ID,
        SessionTypeSourceRank::Repo => target_id == source.source_name,
    }
}

fn eligible_borrowed(source: &SourceSessionType, target_id: &str) -> bool {
    if !source.available {
        return false;
    }
    match source.rank {
        SessionTypeSourceRank::Device => source
            .session_type
            .target_id
            .as_deref()
            .is_none_or(|pin| pin == target_id),
        // A repo definition stays bound to its source target, even with a pin.
        // The existing is_eligible_for_target applies this same rule.
        SessionTypeSourceRank::Repo => source.source_name == target_id,
        SessionTypeSourceRank::Package => default_target_matches(source, target_id),
    }
}

fn choose_source_borrowed<'a>(
    sources: &'a [SourceSessionType],
    session_type_id: &str,
    target_id: Option<&str>,
) -> SessionTypeResult<&'a SourceSessionType> {
    if let Some(target_id) = target_id {
        let mut selected = None;
        for source in sources.iter().filter(|source| eligible_borrowed(source, target_id)) {
            // Only one winner per authored ID is visible to target-scoped spawn.
            let winner = choose_effective_session_type_ref(
                sources.iter().filter(|peer| {
                    peer.session_type.id == source.session_type.id
                        && eligible_borrowed(peer, target_id)
                }),
            )?;
            if !std::ptr::eq(source, winner) || !source_matches_id(winner, session_type_id) {
                continue;
            }
            if selected.replace(winner).is_some() {
                return Err(SessionTypeError::new(
                    "ambiguous_session_type",
                    "session type id matches more than one source at the same precedence",
                ));
            }
        }
        return selected.ok_or_else(|| {
            SessionTypeError::new("unknown_session_type", "session type was not found")
        });
    }
    choose_effective_session_type_ref(
        sources
            .iter()
            .filter(|source| source_matches_id(source, session_type_id)),
    )
}

fn resolve_materialization_source_borrowed<'a>(
    sources: &'a [SourceSessionType],
    state: &'a HubState,
    session_type_id: &str,
    request_target_id: Option<&str>,
) -> SessionTypeResult<(&'a SourceSessionType, Option<&'a SpawnTarget>)> {
    if let Some(target_id) = request_target_id {
        match ensure_enabled_admitted_target_borrowed(state, target_id) {
            Ok(target) => {
                let source = choose_source_borrowed(sources, session_type_id, Some(target_id))?;
                return Ok((source, Some(target)));
            }
            Err(error) if error.kind == "target_not_admitted" => return Err(error),
            Err(_) => {
                let source = choose_source_borrowed(sources, session_type_id, None)?;
                if !default_target_matches(source, target_id) {
                    return Err(SessionTypeError::new(
                        "target_not_admitted",
                        "requested spawn target is not admitted for this session_type",
                    ));
                }
                return Ok((source, None));
            }
        }
    }

    let source = choose_source_borrowed(sources, session_type_id, None)?;
    let resolved_target_id = source.session_type.target_id.as_deref().unwrap_or_else(|| {
        match source.rank {
            SessionTypeSourceRank::Package => "",
            SessionTypeSourceRank::Device => DEFAULT_DEVICE_TARGET_ID,
            SessionTypeSourceRank::Repo => &source.source_name,
        }
    });
    // Package defaults need a prefix comparison without allocating package:name.
    if source.session_type.target_id.is_none()
        && source.rank == SessionTypeSourceRank::Package
    {
        if let Some(target) = state.spawn_targets.iter().find(|target| {
            target.enabled && default_target_matches(source, &target.target_id)
        }) {
            return Ok((source, Some(target)));
        }
        return Ok((source, None));
    }
    if let Ok(target) = ensure_enabled_admitted_target_borrowed(state, resolved_target_id) {
        if !eligible_borrowed(source, &target.target_id) {
            return Err(SessionTypeError::new(
                "target_not_admitted",
                "requested spawn target is not admitted for this session_type",
            ));
        }
        return Ok((source, Some(target)));
    }
    if !default_target_matches(source, resolved_target_id) {
        return Err(SessionTypeError::new(
            "target_not_admitted",
            "requested spawn target is not admitted for this session_type",
        ));
    }
    Ok((source, None))
}

fn effective_session_type_row<'a>(
    winner: &SourceSessionType,
    sources: impl Iterator<Item = &'a SourceSessionType>,
) -> HubSessionType {
    let mut row = session_type_row_from_source(winner);
    row.overridden_sources = sources
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

/// Admit the selected row while the complete charged source set remains live.
fn charged_effective_session_type_row<'a, I>(
    parent: &mut crate::lua_memory::LuaCallbackCharge,
    winner: &SourceSessionType,
    peers: I,
) -> Result<(HubSessionType, crate::lua_memory::LuaCallbackCharge), &'static str>
where
    I: Iterator<Item = &'a SourceSessionType> + Clone,
{
    let definition = &winner.session_type;
    let target_bytes = definition.target_id.as_ref().map_or_else(
        || match winner.rank {
            SessionTypeSourceRank::Package => "package:".len().checked_add(winner.source_name.len()),
            SessionTypeSourceRank::Device => Some(DEFAULT_DEVICE_TARGET_ID.len()),
            SessionTypeSourceRank::Repo => Some(winner.source_name.len()),
        },
        |pin| Some(pin.len()),
    ).ok_or("row target size overflow")?;
    let mut bytes = [
        winner.source_name.len().checked_add(1).and_then(|n| n.checked_add(definition.id.len())).ok_or("row ID size overflow")?,
        winner.source_name.len(), definition.id.len(), winner.source.len(),
        definition.label.len(), definition.description.as_ref().map_or(0, String::len),
        definition.icon.as_ref().map_or(0, String::len), definition.role.len(),
        definition.interaction.len(), definition.lifecycle.len(), definition.command.len(),
        target_bytes,
        match &definition.working_directory {
            PackageSessionTypeWorkingDirectory::PackageRoot => "package_root".len(),
            PackageSessionTypeWorkingDirectory::Relative { .. } => "relative".len(),
        },
    ].into_iter().try_fold(0usize, usize::checked_add).ok_or("row string size overflow")?;
    for strings in [
        &definition.traits,
        &definition.args,
        &definition.allowed_environment_overrides,
        &definition.context,
    ] {
        bytes = bytes.checked_add(vector_growth_peak::<String>(strings.len()).ok_or("row vector size overflow")?).ok_or("row vector size overflow")?;
        for value in strings {
            bytes = bytes.checked_add(value.len()).ok_or("row string size overflow")?;
        }
    }
    let mut overridden = 0usize;
    for source in peers.clone().filter(|source| source.rank < winner.rank) {
        overridden = overridden.checked_add(1).ok_or("row peer count overflow")?;
        bytes = bytes.checked_add(source.source.len()).and_then(|n| n.checked_add(source.source_name.len())).ok_or("row peer string size overflow")?;
    }
    bytes = bytes.checked_add(vector_growth_peak::<HubSessionTypeSource>(overridden).ok_or("row peer vector size overflow")?).ok_or("row peer vector size overflow")?;
    // Match the sole diagnostic push in effective_session_type_row below.
    if overridden > 0 {
        let mut count = CountFormattedBytes(0);
        std::fmt::write(&mut count, format_args!("overrides {} lower-precedence definition(s)", overridden)).map_err(|_| "row diagnostic size overflow")?;
        bytes = bytes.checked_add(vector_growth_peak::<String>(1).ok_or("row diagnostic vector size overflow")?).and_then(|n| n.checked_add(count.0)).ok_or("row diagnostic size overflow")?;
    }
    parent.grow(bytes).map_err(|_| "row capacity exhausted")?;
    let row = effective_session_type_row(winner, peers);
    let storage = parent.split_fixed(bytes).expect("the parent admitted the complete row");
    Ok((row, storage))
}

/// Copy definition and admitted request values under one environment allowance.
#[allow(dead_code)] // The charged Host materializer consumes this product.
enum ChargedEnvironmentFailure<'a> {
    InvalidName,
    NotAdmitted(&'a str),
    Capacity(&'static str),
}

#[allow(dead_code)] // The charged Host materializer converts this descriptor.
impl ChargedEnvironmentFailure<'_> {
    fn into_charged(
        self,
        parent: &mut crate::lua_memory::LuaCallbackCharge,
    ) -> Result<ChargedSessionTypeFailure, &'static str> {
        let failure = match self {
            Self::InvalidName => funded_source_error(
                parent,
                "invalid_environment",
                Some("invalid environment variable name".len()),
                0,
                |output| output.write_str("invalid environment variable name"),
            ),
            Self::NotAdmitted(name) => funded_source_error(
                parent,
                "environment_not_admitted",
                "environment override is not admitted: ".len().checked_add(name.len()),
                0,
                |output| write!(output, "environment override is not admitted: {name}"),
            ),
            Self::Capacity(reason) => return Err(reason),
        };
        failure.ok_or("environment error capacity exhausted")
    }
}

#[allow(dead_code)] // The charged Host materializer consumes this product.
fn charged_effective_environment<'a>(
    parent: &mut crate::lua_memory::LuaCallbackCharge,
    definition: &PackageSessionType,
    request: &'a SessionTypeRequest,
) -> Result<
    (BTreeMap<String, String>, crate::lua_memory::LuaCallbackCharge),
    ChargedEnvironmentFailure<'a>,
> {
    for name in request.environment.keys() {
        if !valid_environment_name(name) {
            return Err(ChargedEnvironmentFailure::InvalidName);
        }
        if !definition
            .allowed_environment_overrides
            .iter()
            .any(|allowed| allowed == name)
        {
            return Err(ChargedEnvironmentFailure::NotAdmitted(name));
        }
    }
    let largest_len = definition
        .environment
        .len()
        .checked_add(request.environment.len())
        // Context injection can add five reserved names after this copy.
        .and_then(|count| count.checked_add(5))
        .ok_or(ChargedEnvironmentFailure::Capacity("environment row count overflow"))?;
    let mut bytes = crate::lua_memory::layout::btree_nodes_checked::<String, String>(largest_len)
        .ok_or(ChargedEnvironmentFailure::Capacity("environment node size overflow"))?;
    for (name, value) in definition.environment.iter().chain(&request.environment) {
        bytes = bytes
            .checked_add(name.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or(ChargedEnvironmentFailure::Capacity("environment string size overflow"))?;
    }
    parent
        .grow(bytes)
        .map_err(|_| ChargedEnvironmentFailure::Capacity("environment capacity exhausted"))?;
    let mut environment = definition.environment.clone();
    for (name, value) in &request.environment {
        environment.insert(name.clone(), value.clone());
    }
    let storage = parent
        .split_fixed(bytes)
        .expect("the parent admitted the environment copy");
    Ok((environment, storage))
}

/// Deterministic spawn identity and cwd values remain funded beside the sources.
struct ChargedDeterministicPrefix {
    target_id: String,
    session_id: SessionId,
    context_id: String,
    working_directory: PathBuf,
    _storage: crate::lua_memory::LuaCallbackCharge,
}

enum DeterministicPrefixFailure {
    Capacity(&'static str),
    CwdNotAdmitted,
}

impl DeterministicPrefixFailure {
    fn into_charged(
        self,
        parent: &mut crate::lua_memory::LuaCallbackCharge,
    ) -> Result<ChargedSessionTypeFailure, &'static str> {
        match self {
            Self::Capacity(reason) => Err(reason),
            Self::CwdNotAdmitted => funded_source_error(
                parent,
                "cwd_not_admitted",
                Some("requested cwd is outside the admitted spawn target".len()),
                0,
                |output| output.write_str("requested cwd is outside the admitted spawn target"),
            )
            .ok_or("cwd error capacity exhausted"),
        }
    }
}

fn charged_deterministic_prefix(
    parent: &mut crate::lua_memory::LuaCallbackCharge,
    source: &SourceSessionType,
    target: Option<&SpawnTarget>,
    request: &SessionTypeRequest,
) -> Result<ChargedDeterministicPrefix, DeterministicPrefixFailure> {
    let definition = &source.session_type;
    let target_source = target
        .map(|target| target.target_id.as_str())
        .or(request.target_id.as_deref())
        .or(definition.target_id.as_deref());
    let target_bytes = match target_source {
        Some(target_id) => target_id.len(),
        None if source.rank == SessionTypeSourceRank::Package => "package:"
            .len()
            .checked_add(source.source_name.len())
            .ok_or(DeterministicPrefixFailure::Capacity("target ID size overflow"))?,
        None if source.rank == SessionTypeSourceRank::Device => DEFAULT_DEVICE_TARGET_ID.len(),
        None => source.source_name.len(),
    };
    let cwd_root = target.map_or(source.root.as_path(), |target| target.root.as_path());
    let cwd_input = request.cwd.as_deref().map(Path::new);
    if cwd_input.is_some_and(|path| !path.is_absolute() || !path.starts_with(cwd_root)) {
        return Err(DeterministicPrefixFailure::CwdNotAdmitted);
    }
    let cwd_bytes = if let Some(cwd) = cwd_input {
        cwd.as_os_str().as_encoded_bytes().len()
    } else {
        match &definition.working_directory {
            PackageSessionTypeWorkingDirectory::PackageRoot => {
                cwd_root.as_os_str().as_encoded_bytes().len()
            }
            PackageSessionTypeWorkingDirectory::Relative { path } => cwd_root
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .checked_add(1)
                .and_then(|length| length.checked_add(path.len()))
                .ok_or(DeterministicPrefixFailure::Capacity("cwd size overflow"))?,
        }
    };
    let session_bytes = request.session_id.as_ref().map_or_else(
        || "session-type-".len().checked_add(definition.id.len()),
        |session_id| Some(session_id.0.len()),
    ).ok_or(DeterministicPrefixFailure::Capacity("session ID size overflow"))?;
    let context_bytes = "ctx-"
        .len()
        .checked_add(session_bytes)
        .ok_or(DeterministicPrefixFailure::Capacity("context ID size overflow"))?;
    let bytes = target_bytes
        .checked_add(cwd_bytes)
        .and_then(|bytes| bytes.checked_add(session_bytes))
        .and_then(|bytes| bytes.checked_add(context_bytes))
        .ok_or(DeterministicPrefixFailure::Capacity("spawn prefix size overflow"))?;
    parent
        .grow(bytes)
        .map_err(|_| DeterministicPrefixFailure::Capacity("spawn prefix capacity exhausted"))?;
    let mut target_id = String::with_capacity(target_bytes);
    if let Some(source) = target_source {
        target_id.push_str(source);
    } else if source.rank == SessionTypeSourceRank::Package {
        target_id.push_str("package:");
        target_id.push_str(&source.source_name);
    } else if source.rank == SessionTypeSourceRank::Device {
        target_id.push_str(DEFAULT_DEVICE_TARGET_ID);
    } else {
        target_id.push_str(&source.source_name);
    }
    let mut session = String::with_capacity(session_bytes);
    if let Some(explicit) = &request.session_id {
        session.push_str(&explicit.0);
    } else {
        session.push_str("session-type-");
        session.push_str(&definition.id);
    }
    let mut context_id = String::with_capacity(context_bytes);
    context_id.push_str("ctx-");
    context_id.push_str(&session);
    let mut working_directory = PathBuf::with_capacity(cwd_bytes);
    if let Some(cwd) = cwd_input {
        working_directory.push(cwd);
    } else {
        working_directory.push(cwd_root);
        if let PackageSessionTypeWorkingDirectory::Relative { path } =
            &definition.working_directory
        {
            // Every source passed validate_relative_manifest_path before selection.
            // Therefore push cannot replace cwd_root with an absolute path.
            working_directory.push(path);
        }
    }
    let storage = parent
        .split_fixed(bytes)
        .expect("the parent admitted the deterministic spawn prefix");
    Ok(ChargedDeterministicPrefix {
        target_id,
        session_id: SessionId(session),
        context_id,
        working_directory,
        _storage: storage,
    })
}

/// Command fields use the definition root, independent of the spawn target root.
struct ChargedExecution {
    executable: String,
    arguments: Vec<String>,
    // The relative command path stays live until its allowance is released.
    _display_path: Option<PathBuf>,
    _storage: crate::lua_memory::LuaCallbackCharge,
}

fn charged_execution(
    parent: &mut crate::lua_memory::LuaCallbackCharge,
    shell: &str,
    source: &SourceSessionType,
) -> Result<ChargedExecution, &'static str> {
    let definition = &source.session_type;
    let original = parent.bytes();
    let (display_path, executable_bytes, extra_args) = match definition.execution {
        PackageSessionTypeExecution::RelativeExecutable => {
            let path_bytes = source
                .root
                .as_os_str()
                .as_encoded_bytes()
                .len()
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(definition.command.len()))
                .ok_or("command path size overflow")?;
            parent
                .grow(path_bytes)
                .map_err(|_| "command path capacity exhausted")?;
            let mut path = PathBuf::with_capacity(path_bytes);
            path.push(&source.root);
            path.push(&definition.command);
            let mut count = CountFormattedBytes(0);
            if std::fmt::write(&mut count, format_args!("{}", path.display())).is_err() {
                drop(path);
                assert!(parent.shrink_to(original));
                return Err("command display size overflow");
            }
            (Some(path), count.0, 0usize)
        }
        PackageSessionTypeExecution::ShellCommand => (None, shell.len(), 3usize),
    };
    let output_bytes = (|| {
        let argument_count = definition.args.len().checked_add(extra_args)?;
        // with_capacity allocates once; vector_growth_peak covers push growth.
        let vector_bytes = argument_count.checked_mul(std::mem::size_of::<String>())?;
        let argument_bytes = definition.args.iter().try_fold(0usize, |bytes, argument| {
            bytes.checked_add(argument.len())
        })?;
        let extra_bytes = if extra_args == 0 {
            0
        } else {
            "-c".len()
                .checked_add(definition.command.len())?
                .checked_add("botster-session-type".len())?
        };
        executable_bytes
            .checked_add(vector_bytes)?
            .checked_add(argument_bytes)?
            .checked_add(extra_bytes)
    })();
    let Some(output_bytes) = output_bytes else {
        drop(display_path);
        assert!(parent.shrink_to(original));
        return Err("command output size overflow");
    };
    if parent.grow(output_bytes).is_err() {
        drop(display_path);
        assert!(parent.shrink_to(original));
        return Err("command output capacity exhausted");
    }
    let mut executable = String::with_capacity(executable_bytes);
    if let Some(path) = &display_path {
        std::fmt::write(&mut executable, format_args!("{}", path.display()))
            .expect("the counted command display fits");
    } else {
        executable.push_str(shell);
    }
    let mut arguments = Vec::with_capacity(definition.args.len() + extra_args);
    if extra_args != 0 {
        arguments.push("-c".to_string());
        arguments.push(definition.command.clone());
        arguments.push("botster-session-type".to_string());
    }
    arguments.extend(definition.args.iter().cloned());
    let storage = parent
        .split_fixed(parent.bytes() - original)
        .expect("the parent admitted the command output");
    Ok(ChargedExecution {
        executable,
        arguments,
        _display_path: display_path,
        _storage: storage,
    })
}

struct CountWrittenBytes(usize);

impl std::io::Write for CountWrittenBytes {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("metadata size overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct ChargedSessionTypeMetadata {
    value: CoreSessionMetadata,
    _storage: crate::lua_memory::LuaCallbackCharge,
}

/// These borrowed values must come from the reviewed startup path policy.
#[derive(Clone, Copy)]
struct MaterializationPathText<'a> {
    data_directory: &'a str,
    session_directory: &'a str,
    hub_socket: &'a str,
    hub_bin: Option<&'a str>,
}

/// The environment map has already reserved five context-injection nodes.
fn charged_inject_context_environment(
    parent: &mut crate::lua_memory::LuaCallbackCharge,
    environment: &mut BTreeMap<String, String>,
    prefix: &ChargedDeterministicPrefix,
    paths: MaterializationPathText<'_>,
) -> Result<crate::lua_memory::LuaCallbackCharge, &'static str> {
    const KEYS: [&str; 5] = [
        "BOTSTER_SESSION_ID",
        "BOTSTER_CONTEXT_ID",
        "BOTSTER_HUB_DATA_DIR",
        "BOTSTER_HUB_SOCKET",
        "BOTSTER_HUB_BIN",
    ];
    let count = if paths.hub_bin.is_some() { 5 } else { 4 };
    let key_bytes = KEYS[..count]
        .iter()
        .try_fold(0usize, |bytes, key| bytes.checked_add(key.len()))
        .ok_or("context environment key size overflow")?;
    let value_bytes = prefix
        .session_id
        .0
        .len()
        .checked_add(prefix.context_id.len())
        .and_then(|bytes| bytes.checked_add(paths.data_directory.len()))
        .and_then(|bytes| bytes.checked_add(paths.hub_socket.len()))
        .and_then(|bytes| bytes.checked_add(paths.hub_bin.map_or(0, str::len)))
        .ok_or("context environment value size overflow")?;
    let bytes = key_bytes
        .checked_add(value_bytes)
        .ok_or("context environment size overflow")?;
    parent
        .grow(bytes)
        .map_err(|_| "context environment capacity exhausted")?;
    environment.insert(KEYS[0].to_string(), prefix.session_id.0.clone());
    environment.insert(KEYS[1].to_string(), prefix.context_id.clone());
    environment.insert(KEYS[2].to_string(), paths.data_directory.to_string());
    environment.insert(KEYS[3].to_string(), paths.hub_socket.to_string());
    if let Some(hub_bin) = paths.hub_bin {
        environment.insert(KEYS[4].to_string(), hub_bin.to_string());
    }
    Ok(parent
        .split_fixed(bytes)
        .expect("the parent admitted context environment values"))
}

struct ChargedSessionTypeContext {
    value: HubSessionContext,
    _storage: crate::lua_memory::LuaCallbackCharge,
}

fn charged_context(
    parent: &mut crate::lua_memory::LuaCallbackCharge,
    prefix: &ChargedDeterministicPrefix,
    source_root: &Path,
    input: SessionTypeContextInput,
    declared_keys: &[String],
    paths: MaterializationPathText<'_>,
) -> Result<ChargedSessionTypeContext, &'static str> {
    const BASE_KEYS: [&str; 7] = [
        "session_id",
        "context_id",
        "target_id",
        "session_dir",
        "hub_socket",
        "repo_path",
        "worktree_path",
    ];
    let optional = [
        ("branch_name", input.branch_name.as_ref()),
        ("prompt", input.prompt.as_ref()),
        ("ticket_id", input.ticket_id.as_ref()),
        ("workspace_id", input.workspace_id.as_ref()),
    ];
    let optional_count = optional.iter().filter(|(_, value)| value.is_some()).count();
    let optional_key_bytes = optional.iter().try_fold(0usize, |bytes, (key, value)| {
        if value.is_some() {
            bytes.checked_add(key.len())
        } else {
            Some(bytes)
        }
    }).ok_or("context optional key size overflow")?;
    let mut metadata_count = 0usize;
    let mut metadata_key_bytes = 0usize;
    for key in input.metadata.keys() {
        if key.bytes().all(|byte| byte == b'_' || byte.is_ascii_alphanumeric()) {
            metadata_count = metadata_count.checked_add(1).ok_or("context row count overflow")?;
            metadata_key_bytes = metadata_key_bytes
                .checked_add("metadata.".len())
                .and_then(|bytes| bytes.checked_add(key.len()))
                .ok_or("context metadata key size overflow")?;
        }
    }
    let declared_key_bytes = declared_keys.iter().try_fold(0usize, |bytes, key| {
        bytes.checked_add(key.len())
    }).ok_or("context declared key size overflow")?;
    let base_key_bytes = BASE_KEYS.iter().try_fold(0usize, |bytes, key| {
        bytes.checked_add(key.len())
    }).ok_or("context base key size overflow")?;
    let map_rows = BASE_KEYS.len()
        .checked_add(optional_count)
        .and_then(|rows| rows.checked_add(metadata_count))
        .and_then(|rows| rows.checked_add(declared_keys.len()))
        .ok_or("context row count overflow")?;
    let map_bytes = crate::lua_memory::layout::btree_nodes_checked::<String, String>(map_rows)
        .ok_or("context map size overflow")?;
    // These counts match the unwrap_or_else fallback allocations below.
    let mut repo_count = CountFormattedBytes(0);
    if input.repo_path.is_none() {
        std::fmt::write(&mut repo_count, format_args!("{}", source_root.display()))
            .map_err(|_| "context repo path size overflow")?;
    }
    let mut worktree_count = CountFormattedBytes(0);
    if input.worktree_path.is_none() {
        std::fmt::write(
            &mut worktree_count,
            format_args!("{}", prefix.working_directory.display()),
        )
        .map_err(|_| "context worktree path size overflow")?;
    }
    let copied_values = prefix
        .session_id
        .0
        .len()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(prefix.context_id.len().checked_mul(2)?))
        .and_then(|bytes| bytes.checked_add(prefix.target_id.len()))
        .and_then(|bytes| bytes.checked_add(paths.session_directory.len()))
        .and_then(|bytes| bytes.checked_add(paths.hub_socket.len()))
        .and_then(|bytes| bytes.checked_add(repo_count.0))
        .and_then(|bytes| bytes.checked_add(worktree_count.0))
        .ok_or("context value size overflow")?;
    let bytes = map_bytes
        .checked_add(base_key_bytes)
        .and_then(|bytes| bytes.checked_add(optional_key_bytes))
        .and_then(|bytes| bytes.checked_add(metadata_key_bytes))
        .and_then(|bytes| bytes.checked_add(declared_key_bytes))
        .and_then(|bytes| bytes.checked_add(copied_values))
        .ok_or("context size overflow")?;
    parent.grow(bytes).map_err(|_| "context capacity exhausted")?;
    let mut values = BTreeMap::new();
    values.insert("session_id".to_string(), prefix.session_id.0.clone());
    values.insert("context_id".to_string(), prefix.context_id.clone());
    values.insert("target_id".to_string(), prefix.target_id.clone());
    values.insert("session_dir".to_string(), paths.session_directory.to_string());
    values.insert("hub_socket".to_string(), paths.hub_socket.to_string());
    // Keep these fallback conditions aligned with the display counts above.
    let repo_path = input.repo_path.unwrap_or_else(|| {
        let mut path = String::with_capacity(repo_count.0);
        std::fmt::write(&mut path, format_args!("{}", source_root.display()))
            .expect("the counted repo path display fits");
        path
    });
    values.insert("repo_path".to_string(), repo_path);
    let worktree_path = input.worktree_path.unwrap_or_else(|| {
        let mut path = String::with_capacity(worktree_count.0);
        std::fmt::write(
            &mut path,
            format_args!("{}", prefix.working_directory.display()),
        )
        .expect("the counted worktree path display fits");
        path
    });
    values.insert("worktree_path".to_string(), worktree_path);
    insert_optional(&mut values, "branch_name", input.branch_name);
    insert_optional(&mut values, "prompt", input.prompt);
    insert_optional(&mut values, "ticket_id", input.ticket_id);
    insert_optional(&mut values, "workspace_id", input.workspace_id);
    for (key, value) in input.metadata {
        if key.bytes().all(|byte| byte == b'_' || byte.is_ascii_alphanumeric()) {
            let mut named = String::with_capacity("metadata.".len() + key.len());
            named.push_str("metadata.");
            named.push_str(&key);
            values.insert(named, value);
        }
    }
    for key in declared_keys {
        values.entry(key.clone()).or_default();
    }
    let storage = parent.split_fixed(bytes).expect("the parent admitted the context");
    Ok(ChargedSessionTypeContext {
        value: HubSessionContext {
            context_id: prefix.context_id.clone(),
            session_id: prefix.session_id.clone(),
            values,
        },
        _storage: storage,
    })
}

/// Finish the output after a reviewed startup policy supplies the path text.
/// Every copy is admitted while all source and output payloads remain live.
#[allow(dead_code)]
fn charged_final_materialization(
    mut parent: crate::lua_memory::LuaCallbackCharge,
    mut row: HubSessionType,
    row_storage: crate::lua_memory::LuaCallbackCharge,
    environment: BTreeMap<String, String>,
    environment_storage: crate::lua_memory::LuaCallbackCharge,
    prefix: ChargedDeterministicPrefix,
    execution: ChargedExecution,
    metadata: ChargedSessionTypeMetadata,
    context: ChargedSessionTypeContext,
    environment_injection: crate::lua_memory::LuaCallbackCharge,
    initial_rows: u16,
    initial_cols: u16,
) -> Result<ChargedSessionTypeMaterialization, &'static str> {
    let mut cwd_count = CountFormattedBytes(0);
    std::fmt::write(
        &mut cwd_count,
        format_args!("{}", prefix.working_directory.display()),
    )
    .map_err(|_| "output cwd size overflow")?;
    let argument_bytes = execution.arguments.iter().try_fold(0usize, |bytes, argument| {
        bytes.checked_add(argument.len())
    }).ok_or("output argument size overflow")?;
    let argument_slots = execution.arguments.len()
        .checked_mul(std::mem::size_of::<String>())
        .ok_or("output argument vector overflow")?;
    let environment_strings = environment.iter().try_fold(0usize, |bytes, (key, value)| {
        bytes.checked_add(key.len())?.checked_add(value.len())
    }).ok_or("output environment size overflow")?;
    let environment_nodes = crate::lua_memory::layout::btree_nodes_checked::<String, String>(
        environment.len(),
    ).ok_or("output environment node overflow")?;
    let environment_slots = environment.len()
        .checked_mul(std::mem::size_of::<SpawnEnvironmentVariable>())
        .ok_or("output environment vector overflow")?;
    let context_key_bytes = context.value.values.keys().try_fold(0usize, |bytes, key| {
        bytes.checked_add(key.len())
    }).ok_or("output context key size overflow")?;
    let context_key_slots = context.value.values.len()
        .checked_mul(std::mem::size_of::<String>())
        .ok_or("output context key vector overflow")?;
    let request_id_bytes = "session-type-".len()
        .checked_add(prefix.context_id.len())
        .ok_or("output request ID size overflow")?;
    let copy_bytes = cwd_count.0.checked_mul(2)
        .and_then(|bytes| bytes.checked_add(prefix.session_id.0.len()))
        .and_then(|bytes| bytes.checked_add(prefix.context_id.len()))
        .and_then(|bytes| bytes.checked_add(request_id_bytes))
        .and_then(|bytes| bytes.checked_add(execution.executable.len()))
        .and_then(|bytes| bytes.checked_add(argument_bytes))
        .and_then(|bytes| bytes.checked_add(argument_slots))
        .and_then(|bytes| bytes.checked_add(environment_strings))
        .and_then(|bytes| bytes.checked_add(environment_nodes))
        .and_then(|bytes| bytes.checked_add(environment_slots))
        .and_then(|bytes| bytes.checked_add(context_key_bytes))
        .and_then(|bytes| bytes.checked_add(context_key_slots))
        .ok_or("output copy size overflow")?;
    parent.grow(copy_bytes).map_err(|_| "output copy capacity exhausted")?;

    let ChargedDeterministicPrefix {
        target_id,
        session_id,
        context_id,
        working_directory,
        _storage: prefix_storage,
    } = prefix;
    let ChargedExecution {
        executable,
        arguments,
        _display_path,
        _storage: execution_storage,
    } = execution;
    let ChargedSessionTypeMetadata { value: metadata, _storage: metadata_storage } = metadata;
    let ChargedSessionTypeContext { value: context, _storage: context_storage } = context;
    row.target_id = target_id;
    let mut cwd = String::with_capacity(cwd_count.0);
    std::fmt::write(&mut cwd, format_args!("{}", working_directory.display()))
        .expect("the counted cwd display fits");
    let mut context_keys = Vec::with_capacity(context.values.len());
    context_keys.extend(context.values.keys().cloned());
    let resolved = ResolvedSessionType {
        session_type: row,
        session_id: session_id.clone(),
        executable,
        arguments,
        working_directory: cwd,
        environment: environment.clone(),
        context_id: context_id.clone(),
        context_keys,
    };
    let mut request_id = String::with_capacity(request_id_bytes);
    request_id.push_str("session-type-");
    request_id.push_str(&context_id);
    let mut variables = Vec::with_capacity(environment.len());
    variables.extend(environment.into_iter().map(|(name, value)| SpawnEnvironmentVariable {
        name,
        value,
    }));
    let spawn_request = SessionSpawnRequest {
        request_id: RequestId(request_id),
        session_id,
        executable: resolved.executable.clone(),
        arguments: resolved.arguments.clone(),
        working_directory: SpawnWorkingDirectory {
            path: resolved.working_directory.clone(),
        },
        environment: SpawnEnvironment { variables },
        initial_pty_size: Some(ResizePayload { rows: initial_rows, cols: initial_cols }),
    };
    let output_copies = parent.split_fixed(copy_bytes)
        .expect("the parent admitted the output copies");
    Ok(ChargedSessionTypeMaterialization {
        materialized: MaterializedSessionType { resolved, spawn_request, context, metadata },
        allowance: ChargedMaterializationAllowance {
            parent,
            row: row_storage,
            environment: environment_storage,
            prefix: prefix_storage,
            execution: execution_storage,
            metadata: metadata_storage,
            context: context_storage,
            environment_injection,
            output_copies,
        },
    })
}

fn charged_session_type_metadata(
    parent: &mut crate::lua_memory::LuaCallbackCharge,
    row: &HubSessionType,
) -> Result<ChargedSessionTypeMetadata, &'static str> {
    const KEYS: [&str; 6] = [
        "botster.session_type.id",
        "botster.session_type.source",
        "botster.session_type.role",
        "botster.session_type.interaction",
        "botster.session_type.lifecycle",
        "botster.session_type.traits",
    ];
    let values = [
        row.session_type_id.as_str(),
        row.source.as_str(),
        row.role.as_str(),
        row.interaction.as_str(),
        row.lifecycle.as_str(),
    ];
    let mut count = CountWrittenBytes(0);
    serde_json::to_writer(&mut count, &row.traits)
        .map_err(|_| "metadata traits size overflow")?;
    let key_bytes = KEYS.iter().try_fold(0usize, |bytes, key| bytes.checked_add(key.len()))
        .ok_or("metadata key size overflow")?;
    let value_bytes = values
        .iter()
        .try_fold(count.0, |bytes, value| bytes.checked_add(value.len()))
        .ok_or("metadata value size overflow")?;
    let map_bytes = crate::lua_memory::layout::btree_nodes_checked::<String, String>(KEYS.len())
        .ok_or("metadata map size overflow")?;
    let bytes = map_bytes
        .checked_add(key_bytes)
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .ok_or("metadata size overflow")?;
    parent
        .grow(bytes)
        .map_err(|_| "metadata capacity exhausted")?;
    let mut trait_bytes = Vec::with_capacity(count.0);
    serde_json::to_writer(&mut trait_bytes, &row.traits)
        .expect("string traits serialize after their length was counted");
    let traits = String::from_utf8(trait_bytes).expect("JSON is UTF-8");
    let mut entries = BTreeMap::new();
    for (key, value) in KEYS[..5].iter().zip(values) {
        entries.insert((*key).to_string(), value.to_string());
    }
    entries.insert(KEYS[5].to_string(), traits);
    let storage = parent
        .split_fixed(bytes)
        .expect("the parent admitted the metadata output");
    Ok(ChargedSessionTypeMetadata {
        value: CoreSessionMetadata::from_entries(entries),
        _storage: storage,
    })
}

fn choose_effective_session_type_ref<'a>(
    matches: impl Iterator<Item = &'a SourceSessionType> + Clone,
) -> SessionTypeResult<&'a SourceSessionType> {
    let Some(best_rank) = matches.clone().map(|source| source.rank).max() else {
        return Err(SessionTypeError::new(
            "unknown_session_type",
            "session type was not found",
        ));
    };
    let mut best = matches.filter(|source| source.rank == best_rank);
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

/// A repo file and its allocation allowance. The bytes drop before the charge.
struct ChargedRepoFileBytes {
    bytes: Vec<u8>,
    _storage: crate::lua_memory::LuaCallbackCharge,
}

/// The typed tree retains its allowance after the read buffer is destroyed.
#[allow(dead_code)]
struct ChargedRepoDefinitions {
    definitions: Vec<PackageSessionType>,
    _storage: crate::lua_memory::LuaCallbackCharge,
}

#[allow(dead_code)]
enum ChargedRepoParseFailure {
    Capacity(&'static str),
    Invalid(materialization_error::ChargedMaterializationError),
}

#[allow(dead_code)]
#[derive(Debug)]
enum ChargedRepoReadFailure {
    Capacity(&'static str),
    TooLarge,
    // Keep the original I/O error for the existing formatted error message.
    Io(std::io::Error),
}

/// Count the full schema before the allocating Serde pass.
/// The source-vector and Host-product peaks still need the same parent.
#[allow(dead_code)]
fn parse_repo_file_charged(
    file: ChargedRepoFileBytes,
    parent: &mut crate::lua_memory::LuaCallbackCharge,
) -> Result<ChargedRepoDefinitions, ChargedRepoParseFailure> {
    let peak = materialization_walk::counted_parser_peak(&file.bytes, parent)
        .map_err(ChargedRepoParseFailure::Capacity)?;
    parent
        .grow(peak)
        .map_err(|_| ChargedRepoParseFailure::Capacity("repo parser capacity exhausted"))?;
    let mut storage = parent
        .split_fixed(peak)
        .ok_or(ChargedRepoParseFailure::Capacity("repo parser capacity exhausted"))?;
    let parsed = serde_json::from_slice::<RepoSessionTypesFile>(&file.bytes);
    let result = match parsed {
        Ok(parsed) => Ok(ChargedRepoDefinitions {
            definitions: parsed.session_types,
            _storage: storage,
        }),
        Err(error) => {
            let wrapped = materialization_error::wrap_repo_error(&error, &mut storage).ok_or(
                ChargedRepoParseFailure::Capacity("repo error capacity exhausted"),
            )?;
            Err(ChargedRepoParseFailure::Invalid(wrapped))
        }
    };
    drop(file);
    result
}

/// Read at most the existing repo limit under one open callback parent.
/// The charged materializer will use this instead of the uncharged loader.
#[allow(dead_code)]
fn read_repo_file_charged(
    root: &Path,
    parent: &mut crate::lua_memory::LuaCallbackCharge,
) -> Result<Option<ChargedRepoFileBytes>, ChargedRepoReadFailure> {
    let path_capacity = root
        .as_os_str()
        .as_encoded_bytes()
        .len()
        .checked_add(1)
        .and_then(|length| length.checked_add(REPO_SESSION_TYPES_FILE.len()))
        .ok_or(ChargedRepoReadFailure::Capacity("repo path size overflow"))?;
    let original = parent.bytes();
    parent
        .grow(path_capacity)
        .map_err(|_| ChargedRepoReadFailure::Capacity("repo path capacity exhausted"))?;
    let mut path = PathBuf::with_capacity(path_capacity);
    path.push(root);
    path.push(REPO_SESSION_TYPES_FILE);
    let opened = File::open(&path);
    drop(path);
    assert!(parent.shrink_to(original));
    let file = match opened {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ChargedRepoReadFailure::Io(error)),
    };
    let metadata_size = file.metadata().ok().and_then(|value| usize::try_from(value.len()).ok());
    let initial_capacity = metadata_size.unwrap_or(0);
    read_repo_file_charged_from_open(file, initial_capacity, parent).map(Some)
}

/// The test can supply an old metadata size to exercise file growth.
fn read_repo_file_charged_from_open(
    mut file: File,
    initial_capacity: usize,
    parent: &mut crate::lua_memory::LuaCallbackCharge,
) -> Result<ChargedRepoFileBytes, ChargedRepoReadFailure> {
    if initial_capacity > REPO_SESSION_TYPES_FILE_BYTE_CAPACITY {
        return Err(ChargedRepoReadFailure::TooLarge);
    }
    let original = parent.bytes();
    parent
        .grow(initial_capacity)
        .map_err(|_| ChargedRepoReadFailure::Capacity("repo file capacity exhausted"))?;
    let mut bytes = vec![0_u8; initial_capacity];
    bytes.clear();
    let mut chunk = [0_u8; 8192];
    let reading = loop {
        let count = match file.read(&mut chunk) {
            Ok(count) => count,
            Err(error) => break Err(ChargedRepoReadFailure::Io(error)),
        };
        if count == 0 {
            break Ok(());
        }
        let Some(next_len) = bytes.len().checked_add(count) else {
            break Err(ChargedRepoReadFailure::Capacity("repo file size overflow"));
        };
        if next_len > REPO_SESSION_TYPES_FILE_BYTE_CAPACITY {
            break Err(ChargedRepoReadFailure::TooLarge);
        }
        if next_len > bytes.capacity() {
            let next_capacity = bytes
                .capacity()
                .saturating_mul(2)
                .max(next_len)
                .min(REPO_SESSION_TYPES_FILE_BYTE_CAPACITY);
            if parent.grow(next_capacity).is_err() {
                break Err(ChargedRepoReadFailure::Capacity("repo file capacity exhausted"));
            }
            let mut grown = vec![0_u8; next_capacity];
            grown[..bytes.len()].copy_from_slice(&bytes);
            grown.truncate(bytes.len());
            drop(bytes);
            bytes = grown;
            assert!(parent.shrink_to(original + next_capacity));
        }
        bytes.extend_from_slice(&chunk[..count]);
    };
    if let Err(error) = reading {
        drop(bytes);
        assert!(parent.shrink_to(original));
        return Err(error);
    }
    let storage = parent
        .split_fixed(bytes.capacity())
        .ok_or(ChargedRepoReadFailure::Capacity("repo file capacity exhausted"))?;
    Ok(ChargedRepoFileBytes {
        bytes,
        _storage: storage,
    })
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
    config: MaterializationConfigView<'_>,
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
            (config.shell.to_string(), arguments)
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
    if valid_environment_name(name) {
        Ok(())
    } else {
        Err(SessionTypeError::new(
            "invalid_environment",
            "invalid environment variable name",
        ))
    }
}

fn valid_environment_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        && !name.as_bytes()[0].is_ascii_digit()
}

fn inject_context_environment(
    config: MaterializationConfigView<'_>,
    environment: &mut BTreeMap<String, String>,
    session_id: &SessionId,
    context_id: &str,
) {
    environment.insert("BOTSTER_SESSION_ID".to_string(), session_id.0.clone());
    environment.insert("BOTSTER_CONTEXT_ID".to_string(), context_id.to_string());
    environment.insert(
        "BOTSTER_HUB_DATA_DIR".to_string(),
        absolute_path(config.data_directory).display().to_string(),
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
    config: MaterializationConfigView<'_>,
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
        absolute_path(config.data_directory)
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

fn hub_socket_path(config: MaterializationConfigView<'_>) -> String {
    config
        .local_socket
        .map(|socket| absolute_path(socket).display().to_string())
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
mod source_selection_tests {
    use super::*;
    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
    use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};
    use crate::persistence::DeviceSessionTypeSource;

    #[test]
    fn charged_config_projection_preserves_materialization_values() {
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(
                std::env::temp_dir().join("charged-config-projection"),
            ),
            ..HubStartupOptions::default()
        }
            .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
            .unwrap();
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 8 * 1024 * 1024,
            total_callback_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let projected = ChargedMaterializationConfig::from_config(&config, &mut parent).unwrap();
        let borrowed = MaterializationConfigView::from(&config);
        let owned = projected.view();
        assert_eq!(owned.data_directory, borrowed.data_directory);
        assert_eq!(owned.shell, borrowed.shell);
        assert_eq!(owned.initial_rows, borrowed.initial_rows);
        assert_eq!(owned.initial_cols, borrowed.initial_cols);
        assert_eq!(owned.local_socket, borrowed.local_socket);
        drop(projected);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn charged_repo_read_failure_preserves_existing_error_text() {
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 8 * 1024 * 1024,
            total_callback_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let failure = ChargedSourceLoadFailure::Read(ChargedRepoReadFailure::TooLarge)
            .into_semantic_failure(&mut parent)
            .ok()
            .expect("the existing size error must fit");
        assert_eq!(failure.error.kind, "repo_session_types_too_large");
        assert_eq!(
            failure.error.message,
            format!(
                "repo-local session type file exceeds {} bytes",
                REPO_SESSION_TYPES_FILE_BYTE_CAPACITY
            )
        );
        drop(failure);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn charged_repo_loader_retains_source_after_parser_drops() {
        let root = std::env::temp_dir().join(format!(
            "charged-repo-source-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(root.join(".botster")).unwrap();
        std::fs::write(
            root.join(REPO_SESSION_TYPES_FILE),
            br#"{"session_types":[{"id":"worker","label":"Worker","role":"agent.worker","interaction":"terminal","lifecycle":"task","command":"bin/worker","environment":{"FULL_VALUE":"value with spaces"}}]}"#,
        )
        .unwrap();
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 8 * 1024 * 1024,
            total_callback_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let mut builder = ChargedSourceBuilder::new(&mut parent);
        assert!(builder.push_repo_file(&root, "target-1").is_ok());
        let sources = builder.finish().unwrap();
        assert_eq!(sources.sources.len(), 1);
        assert_eq!(
            sources.sources[0].session_type.environment["FULL_VALUE"],
            "value with spaces"
        );
        drop(sources);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn charged_repo_reader_preserves_full_fields_and_handles_growth() {
        let root = std::env::temp_dir().join(format!(
            "charged-repo-growth-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("session-types.json");
        let input = serde_json::to_vec(&serde_json::json!({
            "unknown_file_field": {"nested": [1, {"keep_ignoring": true}]},
            "session_types": [{
                "id": "worker", "label": "Worker", "role": "agent.worker",
                "interaction": "terminal", "lifecycle": "task", "command": "bin/worker",
                "environment": {"FULL_VALUE": "value with spaces and = signs"},
                "working_directory": {"policy": "relative", "path": "nested/work"},
                "unknown_definition_field": {"nested": ["still", "accepted"]}
            }]
        }))
        .unwrap();
        std::fs::write(&path, &input).unwrap();
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 8 * 1024 * 1024,
            total_callback_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let read = read_repo_file_charged_from_open(File::open(&path).unwrap(), 1, &mut parent)
            .unwrap();
        assert_eq!(read.bytes, input);
        let parsed = match parse_repo_file_charged(read, &mut parent) {
            Ok(parsed) => parsed,
            Err(_) => panic!("the full charged schema must parse"),
        };
        assert_eq!(parsed.definitions[0].environment["FULL_VALUE"], "value with spaces and = signs");
        assert!(parsed.definitions[0].traits.is_empty());
        assert!(parsed.definitions[0].args.is_empty());
        assert!(parsed.definitions[0].target_id.is_none());
        assert!(matches!(
            &parsed.definitions[0].working_directory,
            PackageSessionTypeWorkingDirectory::Relative { .. }
        ));
        assert!(definition_clone_peak(&parsed.definitions[0]).unwrap() > 0);
        let mut sources = ChargedSourceBuilder::new(&mut parent);
        sources
            .push(
                SessionTypeSourceRank::Repo,
                REPO_SESSION_TYPE_SOURCE,
                "target-1",
                &root,
                &parsed.definitions[0],
                true,
            )
            .unwrap();
        let sources = sources.finish().unwrap();
        drop(parsed);
        assert_eq!(
            sources.sources[0].session_type.environment["FULL_VALUE"],
            "value with spaces and = signs"
        );
        drop(sources);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn charged_repo_reader_rejects_growth_past_existing_limit() {
        let root = std::env::temp_dir().join(format!(
            "charged-repo-limit-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("session-types.json");
        std::fs::write(&path, vec![b'x'; REPO_SESSION_TYPES_FILE_BYTE_CAPACITY + 1]).unwrap();
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 8 * 1024 * 1024,
            total_callback_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let result = read_repo_file_charged_from_open(File::open(&path).unwrap(), 1, &mut parent);
        assert!(matches!(
            result,
            Err(ChargedRepoReadFailure::TooLarge)
        ));
        drop(parent);
        assert_eq!(memory.usage().1, 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn definition(label: &str) -> PackageSessionType {
        serde_json::from_value(serde_json::json!({
            "id": "worker", "label": label, "role": "agent.worker",
            "interaction": "terminal", "lifecycle": "task", "command": "bin/worker",
            "environment": {"FULL_VALUE": "value with spaces and = signs"},
            "working_directory": {"policy": "relative", "path": "nested/work"}
        }))
        .unwrap()
    }

    fn source(rank: SessionTypeSourceRank, name: &str) -> SourceSessionType {
        SourceSessionType {
            rank,
            source: match rank {
                SessionTypeSourceRank::Package => "package",
                SessionTypeSourceRank::Device => "device",
                SessionTypeSourceRank::Repo => "repo",
            }
            .into(),
            source_name: name.into(),
            root: PathBuf::from("/owned/source"),
            session_type: definition(name),
            available: true,
        }
    }

    fn assert_error<T: std::fmt::Debug>(result: SessionTypeResult<T>, kind: &str, message: &str) {
        let error = result.unwrap_err();
        assert_eq!(error.kind, kind);
        assert_eq!(error.message, message);
    }

    #[test]
    fn borrowed_selector_preserves_rank_permutations_and_ties() {
        use SessionTypeSourceRank::{Device, Package, Repo};
        for ranks in [
            [Package, Device, Repo],
            [Package, Repo, Device],
            [Device, Package, Repo],
            [Device, Repo, Package],
            [Repo, Package, Device],
            [Repo, Device, Package],
        ] {
            let sources = ranks.map(|rank| source(rank, "candidate"));
            let winner = choose_effective_session_type_ref(sources.iter()).unwrap();
            assert_eq!(winner.rank, Repo);
            assert!(std::ptr::eq(
                winner,
                sources.iter().find(|item| item.rank == Repo).unwrap()
            ));
        }
        for ranks in [
            vec![Package, Package],
            vec![Package, Device, Device],
            vec![Device, Package, Device],
            vec![Device, Device, Package],
            vec![Repo, Device, Repo],
            vec![Package, Repo, Repo],
        ] {
            let sources: Vec<_> = ranks.into_iter().map(|rank| source(rank, "tie")).collect();
            assert_error(
                choose_effective_session_type_ref(sources.iter()),
                "ambiguous_session_type",
                "session type id matches more than one source at the same precedence",
            );
            assert_error(
                effective_session_type_rows(sources),
                "ambiguous_session_type",
                "session type id matches more than one source at the same precedence",
            );
        }
        let lower_tie = [
            source(Package, "a"),
            source(Package, "b"),
            source(Device, "winner"),
        ];
        assert_eq!(
            choose_effective_session_type_ref(lower_tie.iter())
                .unwrap()
                .source_name,
            "winner"
        );
        assert_error(
            choose_effective_session_type_ref(std::iter::empty()),
            "unknown_session_type",
            "session type was not found",
        );
    }

    #[test]
    fn rows_preserve_peer_order_and_complete_diagnostics() {
        use SessionTypeSourceRank::{Device, Package, Repo};
        let sources = vec![
            source(Device, "device"),
            source(Package, "first"),
            source(Repo, "repo"),
            source(Package, "second"),
        ];
        let mut expected = session_type_row_from_source(&sources[2]);
        expected.overridden_sources = vec![
            HubSessionTypeSource {
                kind: "device".into(),
                name: "device".into(),
            },
            HubSessionTypeSource {
                kind: "package".into(),
                name: "first".into(),
            },
            HubSessionTypeSource {
                kind: "package".into(),
                name: "second".into(),
            },
        ];
        expected.diagnostics = vec!["overrides 3 lower-precedence definition(s)".into()];
        assert_eq!(
            effective_session_type_rows(sources).unwrap(),
            vec![expected]
        );
    }

    struct Fixture {
        root: PathBuf,
        state: HubState,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "botster-source-selection-{label}-{}-{unique}",
                std::process::id()
            ));
            fs::create_dir_all(&root).unwrap();
            let config = HubStartupOptions {
                data_directory: DataDirectoryOption::Explicit(root.join("data")),
                ..HubStartupOptions::default()
            }
            .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
            .unwrap();
            let mut state = HubState::from_config(&config);
            state.spawn_targets.clear();
            state.device_session_type_sources = vec![DeviceSessionTypeSource {
                root: root.clone(),
                session_types: vec![definition("device")],
            }];
            Self { root, state }
        }

        fn repo(&mut self, id: &str, definitions: &[PackageSessionType]) {
            let root = self.root.join(id);
            fs::create_dir_all(root.join(".botster")).unwrap();
            fs::write(
                root.join(REPO_SESSION_TYPES_FILE),
                serde_json::to_vec(&serde_json::json!({"session_types": definitions})).unwrap(),
            )
            .unwrap();
            self.state.spawn_targets.push(SpawnTarget {
                target_id: id.into(),
                label: id.into(),
                root,
                enabled: true,
                kind: "directory".into(),
                base_ref: None,
                metadata: BTreeMap::new(),
            });
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[test]
    fn charged_loader_keeps_device_and_repo_sources_after_reads() {
        let mut fixture = Fixture::new("charged-all-sources");
        fixture.repo("repo", &[definition("repo")]);
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 8 * 1024 * 1024,
            total_callback_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let sources = match load_charged_sources(&[], &fixture.state, &mut parent) {
            Ok(sources) => sources,
            Err(_) => panic!("the charged source set must load"),
        };
        assert_eq!(sources.sources.len(), 2);
        assert_eq!(sources.sources[0].rank, SessionTypeSourceRank::Device);
        assert_eq!(sources.sources[1].rank, SessionTypeSourceRank::Repo);
        assert_eq!(
            sources.sources[1].session_type.environment["FULL_VALUE"],
            "value with spaces and = signs"
        );
        drop(sources);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn charged_winner_keeps_target_row_and_repo_environment() {
        let mut fixture = Fixture::new("charged-winner");
        fixture.repo("repo", &[definition("repo")]);
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 8 * 1024 * 1024,
            total_callback_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let sources = load_charged_sources(&[], &fixture.state, &mut parent).unwrap_or_else(|_| {
            panic!("the complete charged source set must load")
        });
        let (winner, expected_row, expected_target) = resolve_materialization_source(
            &[], &fixture.state, "worker", Some("repo"),
        )
        .unwrap();
        let (index, row, row_charge, target) = sources
            .resolve(&fixture.state, "worker", Some("repo"), &mut parent)
            .unwrap_or_else(|_| panic!("the charged winner must resolve"));
        assert_eq!(sources.sources[index].session_type, winner.session_type);
        assert_eq!(sources.sources[index].session_type.environment, winner.session_type.environment);
        assert_eq!(row, expected_row);
        assert_eq!(target, expected_target.as_ref());
        assert!(row_charge.bytes() > 0);
        drop(row);
        drop(row_charge);
        drop(sources);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn charged_prefix_matches_ordinary_identity_and_cwd() {
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(
                std::env::temp_dir().join("charged-prefix-parity"),
            ),
            ..HubStartupOptions::default()
        }
            .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
            .unwrap();
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .unwrap();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        for rank in [
            SessionTypeSourceRank::Package,
            SessionTypeSourceRank::Device,
            SessionTypeSourceRank::Repo,
        ] {
            for shell_command in [false, true] {
                let mut source = source(rank, "prefix-source");
                if shell_command {
                    source.session_type.execution = PackageSessionTypeExecution::ShellCommand;
                    source.session_type.command = "printf 'shell command'".into();
                    source.session_type.args = vec!["one".into(), "two".into()];
                }
                source.session_type.context = vec!["declared".into(), "session_id".into()];
                source.session_type.allowed_environment_overrides.push(
                    "BOTSTER_SESSION_ID".into(),
                );
                for request in [
                    SessionTypeRequest::default(),
                    SessionTypeRequest {
                        session_id: Some(SessionId("explicit-session".into())),
                        cwd: Some("/owned/source/nested/work".into()),
                        context: SessionTypeContextInput {
                            repo_path: Some("/requested/repo".into()),
                            branch_name: Some("feature/context".into()),
                            metadata: BTreeMap::from([
                                ("VALID".into(), "value".into()),
                                ("not-valid".into(), "ignored".into()),
                            ]),
                            ..SessionTypeContextInput::default()
                        },
                        ..SessionTypeRequest::default()
                    },
                    SessionTypeRequest {
                        environment: BTreeMap::from([(
                            "BOTSTER_SESSION_ID".into(),
                            "request-value".into(),
                        )]),
                        ..SessionTypeRequest::default()
                    },
                ] {
                    let row = effective_session_type_row(&source, std::iter::once(&source));
                    let ordinary = materialize_session_type_from_resolved(
                        (&config).into(),
                        request.clone(),
                        &source,
                        row,
                        None,
                    )
                    .unwrap();
                    let prefix = charged_deterministic_prefix(&mut parent, &source, None, &request)
                        .unwrap_or_else(|_| panic!("the charged prefix must fit"));
                    assert_eq!(prefix.target_id, ordinary.resolved.session_type.target_id);
                    assert_eq!(prefix.session_id, ordinary.resolved.session_id);
                    assert_eq!(prefix.context_id, ordinary.resolved.context_id);
                    assert_eq!(
                        prefix.working_directory.display().to_string(),
                        ordinary.resolved.working_directory
                    );
                    let execution = charged_execution(
                        &mut parent,
                        &config.session_defaults.shell,
                        &source,
                    )
                        .unwrap_or_else(|_| panic!("the charged command must fit"));
                    assert_eq!(execution.executable, ordinary.resolved.executable);
                    assert_eq!(execution.arguments, ordinary.resolved.arguments);
                    let metadata = charged_session_type_metadata(
                        &mut parent,
                        &ordinary.resolved.session_type,
                    )
                    .unwrap_or_else(|_| panic!("the charged metadata must fit"));
                    assert_eq!(metadata.value, ordinary.metadata);
                    let data_directory = absolute_path(&config.data_directory)
                        .display()
                        .to_string();
                    let session_directory = absolute_path(&config.data_directory)
                        .join("sessions")
                        .display()
                        .to_string();
                    let hub_socket = hub_socket_path((&config).into());
                    let hub_bin = std::env::current_exe()
                        .ok()
                        .map(|path| path.display().to_string());
                    let paths = MaterializationPathText {
                        data_directory: &data_directory,
                        session_directory: &session_directory,
                        hub_socket: &hub_socket,
                        hub_bin: hub_bin.as_deref(),
                    };
                    let context = charged_context(
                        &mut parent,
                        &prefix,
                        &source.root,
                        request.context.clone(),
                        &source.session_type.context,
                        paths,
                    )
                    .unwrap_or_else(|_| panic!("the charged context must fit"));
                    assert_eq!(context.value, ordinary.context);
                    let (mut environment, environment_storage) = charged_effective_environment(
                        &mut parent,
                        &source.session_type,
                        &request,
                    )
                    .unwrap_or_else(|_| panic!("the charged environment must fit"));
                    let injection_storage = charged_inject_context_environment(
                        &mut parent,
                        &mut environment,
                        &prefix,
                        paths,
                    )
                    .unwrap_or_else(|_| panic!("the charged context environment must fit"));
                    assert_eq!(environment, ordinary.resolved.environment);
                    if request.environment.contains_key("BOTSTER_SESSION_ID") {
                        assert_eq!(
                            environment["BOTSTER_SESSION_ID"],
                            ordinary.resolved.session_id.0,
                        );
                    }
                    drop(environment);
                    drop(injection_storage);
                    drop(environment_storage);
                    drop(context);
                    drop(metadata);
                    drop(execution);
                    drop(prefix);
                }
            }
        }
        let source = source(SessionTypeSourceRank::Package, "prefix-source");
        let invalid = SessionTypeRequest {
            cwd: Some("/elsewhere".into()),
            ..SessionTypeRequest::default()
        };
        let failure = charged_deterministic_prefix(&mut parent, &source, None, &invalid)
            .err()
            .expect("the requested cwd must be refused")
            .into_charged(&mut parent)
            .unwrap_or_else(|_| panic!("the refusal must fit"));
        assert_eq!(failure.error.kind, "cwd_not_admitted");
        assert_eq!(
            failure.error.message,
            "requested cwd is outside the admitted spawn target"
        );
        drop(failure);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn charged_final_output_matches_ordinary_and_releases_allowance() {
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(
                std::env::temp_dir().join("charged-final-output-parity"),
            ),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .unwrap();
        let mut source = source(SessionTypeSourceRank::Device, "final-output");
        source.session_type.allowed_environment_overrides.push("BOTSTER_SESSION_ID".into());
        let request = SessionTypeRequest {
            session_id: Some(SessionId("charged-explicit".into())),
            environment: BTreeMap::from([(
                "BOTSTER_SESSION_ID".into(),
                "request-value".into(),
            )]),
            ..SessionTypeRequest::default()
        };
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let (row, row_storage) = charged_effective_session_type_row(
            &mut parent,
            &source,
            std::iter::once(&source),
        ).unwrap();
        let ordinary = materialize_session_type_from_resolved(
            (&config).into(), request.clone(), &source, row.clone(), None,
        ).unwrap();
        let prefix = charged_deterministic_prefix(&mut parent, &source, None, &request)
            .unwrap_or_else(|_| panic!("the charged prefix must fit"));
        let execution = charged_execution(&mut parent, &config.session_defaults.shell, &source)
            .unwrap();
        let metadata = charged_session_type_metadata(&mut parent, &row).unwrap();
        let data_directory = absolute_path(&config.data_directory).display().to_string();
        let session_directory = absolute_path(&config.data_directory)
            .join("sessions").display().to_string();
        let hub_socket = hub_socket_path((&config).into());
        let hub_bin = std::env::current_exe().ok().map(|path| path.display().to_string());
        let paths = MaterializationPathText {
            data_directory: &data_directory,
            session_directory: &session_directory,
            hub_socket: &hub_socket,
            hub_bin: hub_bin.as_deref(),
        };
        let context = charged_context(
            &mut parent, &prefix, &source.root, request.context.clone(),
            &source.session_type.context, paths,
        ).unwrap();
        let (mut environment, environment_storage) = charged_effective_environment(
            &mut parent, &source.session_type, &request,
        ).unwrap_or_else(|_| panic!("the charged environment must fit"));
        let environment_injection = charged_inject_context_environment(
            &mut parent, &mut environment, &prefix, paths,
        ).unwrap();
        let charged = charged_final_materialization(
            parent, row, row_storage, environment, environment_storage, prefix, execution,
            metadata, context, environment_injection,
            config.session_defaults.initial_rows, config.session_defaults.initial_cols,
        ).unwrap();
        let (materialized, allowance) = charged.into_parts();
        assert_eq!(materialized, ordinary);
        assert_eq!(materialized.resolved.environment["BOTSTER_SESSION_ID"], "charged-explicit");
        drop(materialized);
        drop(allowance);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn charged_environment_keeps_definition_and_override_values() {
        let mut definition = definition("environment");
        definition.allowed_environment_overrides = vec!["FULL_VALUE".into(), "EXTRA".into()];
        let request = SessionTypeRequest {
            environment: BTreeMap::from([
                ("FULL_VALUE".into(), "new value".into()),
                ("EXTRA".into(), "second value".into()),
            ]),
            ..SessionTypeRequest::default()
        };
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .unwrap();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let (environment, charge) =
            charged_effective_environment(&mut parent, &definition, &request)
                .unwrap_or_else(|_| panic!("the environment copy must fit"));
        assert_eq!(environment["FULL_VALUE"], "new value");
        assert_eq!(environment["EXTRA"], "second value");
        assert!(charge.bytes() > 0);
        drop(environment);
        drop(charge);
        let refused = SessionTypeRequest {
            environment: BTreeMap::from([("DENIED".into(), "value".into())]),
            ..SessionTypeRequest::default()
        };
        let failure = match charged_effective_environment(&mut parent, &definition, &refused) {
            Err(failure) => failure.into_charged(&mut parent).unwrap_or_else(|_| {
                panic!("the environment refusal must fit")
            }),
            Ok(_) => panic!("the environment override must be refused"),
        };
        assert_eq!(failure.error.kind, "environment_not_admitted");
        assert_eq!(
            failure.error.message,
            "environment override is not admitted: DENIED"
        );
        drop(failure);
        let invalid = SessionTypeRequest {
            environment: BTreeMap::from([("BAD-NAME".into(), "value".into())]),
            ..SessionTypeRequest::default()
        };
        let failure = match charged_effective_environment(&mut parent, &definition, &invalid) {
            Err(failure) => failure.into_charged(&mut parent).unwrap_or_else(|_| {
                panic!("the invalid name refusal must fit")
            }),
            Ok(_) => panic!("the invalid name must be refused"),
        };
        assert_eq!(failure.error.kind, "invalid_environment");
        assert_eq!(failure.error.message, "invalid environment variable name");
        drop(failure);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn bare_and_qualified_lookup_preserve_diagnostic_peers_and_owned_values() {
        let mut fixture = Fixture::new("qualified");
        fixture.repo("repo", &[definition("repo")]);
        let (bare, row) = find_source_session_type_with_row(&[], &fixture.state, "worker").unwrap();
        let (qualified, qualified_row) =
            find_source_session_type_with_row(&[], &fixture.state, "repo/worker").unwrap();
        assert_eq!(row, qualified_row);
        assert_eq!(
            row.overridden_sources,
            vec![HubSessionTypeSource {
                kind: "device".into(),
                name: "device".into()
            }]
        );
        assert_eq!(
            row.diagnostics,
            vec!["overrides 1 lower-precedence definition(s)"]
        );
        assert_eq!(bare.session_type, qualified.session_type);
        assert_eq!(bare.source_name, "repo");
        let (device, device_row) =
            find_source_session_type_with_row(&[], &fixture.state, "device/worker").unwrap();
        assert_eq!(device.source_name, "device");
        assert!(device_row.overridden_sources.is_empty());
        drop(fixture);
        assert_eq!(
            qualified.session_type.environment["FULL_VALUE"],
            "value with spaces and = signs"
        );
        assert_eq!(
            qualified.session_type.working_directory,
            PackageSessionTypeWorkingDirectory::Relative {
                path: "nested/work".into()
            }
        );
        assert_eq!(qualified.session_type.command, "bin/worker");
    }

    #[test]
    fn target_lookup_selects_effective_winners_and_rejects_qualified_losers() {
        let mut fixture = Fixture::new("target-winner");
        fixture.repo("repo", &[definition("repo")]);
        let winners = target_scoped_effective_winners(&[], &fixture.state, "repo").unwrap();
        assert_eq!(winners.len(), 1);
        assert_eq!(winners[0].0.source_name, "repo");
        assert_eq!(winners[0].1.target_id, "repo");
        assert_eq!(
            winners[0].1.diagnostics,
            vec!["overrides 1 lower-precedence definition(s)"]
        );
        let (owned, row) =
            find_source_session_type_for_target(&[], &fixture.state, "repo/worker", "repo")
                .unwrap();
        assert_eq!(row, winners[0].1);
        assert_error(
            find_source_session_type_for_target(&[], &fixture.state, "device/worker", "repo"),
            "unknown_session_type",
            "session type was not found",
        );
        drop(fixture);
        assert_eq!(
            owned.session_type.environment["FULL_VALUE"],
            "value with spaces and = signs"
        );
        assert_eq!(
            owned.session_type.working_directory,
            PackageSessionTypeWorkingDirectory::Relative {
                path: "nested/work".into()
            }
        );
    }

    #[test]
    fn target_eligibility_precedes_rank_and_preserves_exact_tie_errors() {
        let mut fixture = Fixture::new("eligibility");
        fixture.repo("requested", &[]);
        fixture.repo("other", &[definition("other-repo")]);
        let winners = target_scoped_effective_winners(&[], &fixture.state, "requested").unwrap();
        assert_eq!(winners.len(), 1);
        assert_eq!(winners[0].0.rank, SessionTypeSourceRank::Device);
        assert!(winners[0].1.overridden_sources.is_empty());
        assert_error(
            find_source_session_type_for_target(&[], &fixture.state, "other/worker", "requested"),
            "unknown_session_type",
            "session type was not found",
        );
        fixture
            .state
            .device_session_type_sources
            .push(DeviceSessionTypeSource {
                root: fixture.root.clone(),
                session_types: vec![definition("second-device")],
            });
        assert_error(
            target_scoped_effective_winners(&[], &fixture.state, "requested"),
            "ambiguous_session_type",
            "session type id matches more than one source at the same precedence",
        );
        assert_error(
            find_source_session_type_with_row(&[], &fixture.state, "device/worker"),
            "ambiguous_session_type",
            "session type id matches more than one source at the same precedence",
        );
    }
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
