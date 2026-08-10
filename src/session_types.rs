//! Hub-owned session type resolution, authority, and context policy.
//!
//! Packages may contribute declarations, but the hub validates and materializes
//! them into generic core spawn requests before `botster-core` sees anything.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use botster_core::{
    CoreSessionMetadata, PackageSource, RequestId, ResizePayload, SessionId, SessionSpawnRequest,
    SpawnEnvironment, SpawnEnvironmentVariable, SpawnWorkingDirectory,
};
use serde::{Deserialize, Serialize};

use crate::config::HubConfig;
use crate::packages::{PackageRecord, PackageState};
use crate::persistence::HubState;
use crate::spawn_targets::{SpawnTarget, list_spawn_targets};

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

#[derive(Debug, Clone, Deserialize)]
struct RepoSessionTypesFile {
    #[serde(default)]
    session_types: Vec<PackageSessionType>,
}

/// Apply a source-aware mutation and return the next durable Hub state.
pub fn mutate_session_type(
    config: &HubConfig,
    state: &HubState,
    source: SessionTypeMutationSource,
    mutation: SessionTypeMutation,
) -> SessionTypeResult<HubState> {
    if let SessionTypeMutationSource::Package { package_name } = &source {
        return Err(SessionTypeError::new(
            "read_only_session_type_source",
            format!("package session types are read-only: {package_name}"),
        ));
    }

    let mut next = state.clone();
    match source {
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
            write_repo_session_types(&root, &definitions)?;
        }
        SessionTypeMutationSource::Package { .. } => unreachable!("handled above"),
    }
    next.session_type_generation = next.session_type_generation.saturating_add(1);
    Ok(next)
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
    let bytes = serde_json::to_vec_pretty(&serde_json::json!({
        "session_types": definitions,
    }))
    .map_err(|error| {
        SessionTypeError::new(
            "repo_session_type_write_failed",
            format!("repo session types could not be serialized: {error}"),
        )
    })?;
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
    let resolved = ResolvedSessionType {
        session_type: row,
        session_id: session_id.clone(),
        executable: resolve_command_path(&command_root, &session_type.command)
            .display()
            .to_string(),
        arguments: session_type.args.clone(),
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
    let resolved = ResolvedSessionType {
        session_type: row,
        session_id: session_id.clone(),
        executable: resolve_command_path(command_root, &source.session_type.command)
            .display()
            .to_string(),
        arguments: source.session_type.args.clone(),
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
pub fn list_session_types_for_target(
    records: &[&PackageRecord],
    state: &HubState,
    target_id: &str,
) -> SessionTypeResult<Vec<HubSessionType>> {
    let _target = ensure_enabled_admitted_target(state, target_id)?;
    let sources = source_session_types(records, state)?;
    let eligible = sources
        .into_iter()
        .filter(|source| is_eligible_for_target(source, target_id))
        .collect::<Vec<_>>();
    let mut rows = effective_session_type_rows(eligible)?;
    for row in &mut rows {
        row.target_id = target_id.to_string();
    }
    rows.retain(|row| row.available);
    rows.sort_by(|left, right| left.session_type_id.cmp(&right.session_type_id));
    Ok(rows)
}

/// Return one enabled effective session_type eligible at one admitted spawn point.
pub fn show_session_type_for_target(
    records: &[&PackageRecord],
    state: &HubState,
    target_id: &str,
    session_type_id: &str,
) -> SessionTypeResult<HubSessionType> {
    match find_source_session_type_for_target(records, state, session_type_id, target_id) {
        Ok((source, mut row)) => {
            if !source.available || !row.available {
                return Err(SessionTypeError::new(
                    "session_type_not_eligible",
                    "session type is not eligible for the requested target",
                ));
            }
            row.target_id = target_id.to_string();
            Ok(row)
        }
        Err(error) if error.kind == "unknown_session_type" => {
            // Distinguish "id does not exist anywhere" from "exists but not at T".
            // Global unknown stays unknown; ineligible-at-T becomes session_type_not_eligible.
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

/// Spawn-point lookup: validate T, filter eligible sources for T, then precedence.
fn find_source_session_type_for_target(
    records: &[&PackageRecord],
    state: &HubState,
    session_type_id: &str,
    target_id: &str,
) -> SessionTypeResult<(SourceSessionType, HubSessionType)> {
    let _target = ensure_enabled_admitted_target(state, target_id)?;
    let sources = source_session_types(records, state)?;
    let eligible = sources
        .into_iter()
        .filter(|source| is_eligible_for_target(source, target_id))
        .collect::<Vec<_>>();
    let matches = eligible
        .iter()
        .filter(|source| {
            source.session_type.id == session_type_id
                || source_session_type_id(source) == session_type_id
        })
        .cloned()
        .collect::<Vec<_>>();
    let winner = choose_effective_session_type(matches)?;
    let peers = eligible
        .into_iter()
        .filter(|source| source.session_type.id == winner.session_type.id)
        .collect::<Vec<_>>();
    let mut row = effective_session_type_row(&winner, &peers);
    row.target_id = target_id.to_string();
    Ok((winner, row))
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

fn source_session_types(
    records: &[&PackageRecord],
    state: &HubState,
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

    for target in list_spawn_targets(&state.spawn_targets) {
        if !target.enabled {
            continue;
        }
        let repo_session_types = repo_session_types(&target.root)?;
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
    match fs::read(&path) {
        Ok(bytes) => {
            let file: RepoSessionTypesFile = serde_json::from_slice(&bytes).map_err(|error| {
                SessionTypeError::new(
                    "invalid_repo_session_types",
                    format!("repo-local session type file is invalid: {error}"),
                )
            })?;
            Ok(file.session_types)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(SessionTypeError::new(
            "invalid_repo_session_types",
            format!("repo-local session type file could not be read: {error}"),
        )),
    }
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
        || session_type.traits.iter().collect::<BTreeSet<_>>().len() != session_type.traits.len()
    {
        return Err(SessionTypeError::new(
            "invalid_session_type_traits",
            "session type traits must be unique bounded tokens",
        ));
    }
    validate_relative_manifest_path(&session_type.command, "command")?;
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
    let mut ids = BTreeSet::new();
    for session_type in session_types {
        validate_session_type(session_type).map_err(|error| error.message)?;
        if !ids.insert(session_type.id.as_str()) {
            return Err(format!("duplicate session type id {}", session_type.id));
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
            format!("invalid environment variable name: {name}"),
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
