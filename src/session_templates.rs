//! Hub-owned session template resolution and context policy.
//!
//! Packages may contribute declarations, but the hub validates and materializes
//! them into generic core spawn requests before `botster-core` sees anything.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use botster_core::{
    PackageSource, RequestId, ResizePayload, SessionId, SessionSpawnRequest, SpawnEnvironment,
    SpawnEnvironmentVariable, SpawnWorkingDirectory,
};
use serde::{Deserialize, Serialize};

use crate::config::HubConfig;
use crate::packages::{PackageRecord, PackageState};
use crate::persistence::HubState;
use crate::spawn_targets::list_spawn_targets;

/// Package-provided session template declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageSessionTemplate {
    pub id: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub working_directory: PackageSessionTemplateWorkingDirectory,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub allowed_environment_overrides: Vec<String>,
    #[serde(default)]
    pub context: Vec<String>,
    #[serde(default)]
    pub target_id: Option<String>,
}

/// Working-directory policy for a package session template.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum PackageSessionTemplateWorkingDirectory {
    #[default]
    PackageRoot,
    Relative {
        path: String,
    },
}

/// Client request data used when resolving or spawning a template.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionTemplateRequest {
    pub target_id: Option<String>,
    pub session_id: Option<SessionId>,
    pub cwd: Option<String>,
    pub environment: BTreeMap<String, String>,
    pub context: SessionTemplateContextInput,
}

/// Trusted hub context inputs supplied by a higher-level workflow.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionTemplateContextInput {
    pub worktree_path: Option<String>,
    pub repo_path: Option<String>,
    pub branch_name: Option<String>,
    pub prompt: Option<String>,
    pub ticket_id: Option<String>,
    pub workspace_id: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

/// Sanitized template row exposed to clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubSessionTemplate {
    pub template_id: String,
    pub package_name: String,
    pub id: String,
    pub source: String,
    pub command: String,
    pub args: Vec<String>,
    pub working_directory_policy: String,
    pub allowed_environment_overrides: Vec<String>,
    pub context_keys: Vec<String>,
    pub target_id: String,
    pub available: bool,
}

/// Resolved template DTO exposed before spawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSessionTemplate {
    pub template: HubSessionTemplate,
    pub session_id: SessionId,
    pub executable: String,
    pub arguments: Vec<String>,
    pub working_directory: String,
    pub environment: BTreeMap<String, String>,
    pub context_id: String,
    pub context_keys: Vec<String>,
}

/// Context stored by the hub for a spawned template session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubSessionContext {
    pub context_id: String,
    pub session_id: SessionId,
    pub values: BTreeMap<String, String>,
}

/// Resolved spawn request plus context payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedSessionTemplate {
    pub resolved: ResolvedSessionTemplate,
    pub spawn_request: SessionSpawnRequest,
    pub context: HubSessionContext,
}

/// Template policy error with path-neutral messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTemplateError {
    pub kind: &'static str,
    pub message: String,
}

impl SessionTemplateError {
    fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

pub type SessionTemplateResult<T> = Result<T, SessionTemplateError>;

const DEVICE_TEMPLATE_SOURCE: &str = "device";
const PACKAGE_TEMPLATE_SOURCE: &str = "package";
const REPO_TEMPLATE_SOURCE: &str = "repo";
const DEFAULT_DEVICE_TARGET_ID: &str = "device:local";
const REPO_SESSION_TEMPLATES_FILE: &str = ".botster/session-templates.json";

#[derive(Debug, Clone, Deserialize)]
struct RepoSessionTemplatesFile {
    #[serde(default)]
    session_templates: Vec<PackageSessionTemplate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TemplateSourceRank {
    Package = 0,
    Device = 1,
    Repo = 2,
}

#[derive(Debug, Clone)]
struct SourceTemplate {
    rank: TemplateSourceRank,
    source: String,
    source_name: String,
    root: PathBuf,
    template: PackageSessionTemplate,
    available: bool,
}

/// Return effective session templates after applying package < device < repo precedence.
pub fn list_session_templates(
    records: &[&PackageRecord],
    state: &HubState,
) -> SessionTemplateResult<Vec<HubSessionTemplate>> {
    let sources = source_templates(records, state)?;
    Ok(effective_templates(sources)?
        .into_iter()
        .map(|source| template_row_from_source(&source))
        .collect())
}

/// Resolve and materialize the effective template into the generic core spawn contract.
pub fn materialize_session_template(
    config: &HubConfig,
    records: &[&PackageRecord],
    state: &HubState,
    template_id: &str,
    request: SessionTemplateRequest,
) -> SessionTemplateResult<MaterializedSessionTemplate> {
    let source = find_source_template(records, state, template_id)?;
    if !source.available {
        return Err(SessionTemplateError::new(
            "template_unavailable",
            "session template source is not enabled",
        ));
    }

    let template = &source.template;
    validate_template(template)?;
    let source_root = source.root.clone();
    let default_target_id = source_default_target_id(&source);
    let resolved_target_id = request
        .target_id
        .clone()
        .or_else(|| template.target_id.clone())
        .unwrap_or_else(|| default_target_id.clone());
    if resolved_target_id != default_target_id {
        return Err(SessionTemplateError::new(
            "target_not_admitted",
            "requested spawn target is not admitted for this template",
        ));
    }

    let default_cwd = resolve_working_directory(&source_root, template)?;
    let working_directory = if let Some(cwd) = &request.cwd {
        let path = PathBuf::from(cwd);
        if !path.is_absolute() || !path.starts_with(&source_root) {
            return Err(SessionTemplateError::new(
                "cwd_not_admitted",
                "requested cwd is outside the admitted spawn target",
            ));
        }
        path
    } else {
        default_cwd
    };

    let mut environment = template.environment.clone();
    let allowed = template
        .allowed_environment_overrides
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for (name, value) in &request.environment {
        validate_environment_name(name)?;
        if !allowed.contains(name) {
            return Err(SessionTemplateError::new(
                "environment_not_admitted",
                format!("environment override is not admitted: {name}"),
            ));
        }
        environment.insert(name.clone(), value.clone());
    }

    let session_id = request
        .session_id
        .unwrap_or_else(|| SessionId(format!("session-template-{}", template.id)));
    let context_id = format!("ctx-{}", session_id.0);
    let context_inputs = ContextAssemblyInputs {
        session_id: &session_id,
        context_id: &context_id,
        target_id: &resolved_target_id,
        package_root: &source_root,
        working_directory: &working_directory,
    };
    let context = assemble_context(config, context_inputs, request.context, &template.context);
    inject_context_environment(config, &mut environment, &session_id, &context_id);

    let row = template_row_from_source(&source);
    let resolved = ResolvedSessionTemplate {
        template: row,
        session_id: session_id.clone(),
        executable: resolve_command_path(&source_root, &template.command)
            .display()
            .to_string(),
        arguments: template.args.clone(),
        working_directory: working_directory.display().to_string(),
        environment: environment.clone(),
        context_id: context_id.clone(),
        context_keys: context.values.keys().cloned().collect(),
    };
    let spawn_request = SessionSpawnRequest {
        request_id: RequestId(format!("session-template-{context_id}")),
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

    Ok(MaterializedSessionTemplate {
        resolved,
        spawn_request,
        context,
    })
}

/// Return one effective template row by bare or full id.
pub fn show_session_template(
    records: &[&PackageRecord],
    state: &HubState,
    template_id: &str,
) -> SessionTemplateResult<HubSessionTemplate> {
    find_source_template(records, state, template_id)
        .map(|source| template_row_from_source(&source))
}

fn template_row_from_source(source: &SourceTemplate) -> HubSessionTemplate {
    HubSessionTemplate {
        template_id: source_template_id(source),
        package_name: source.source_name.clone(),
        id: source.template.id.clone(),
        source: source.source.clone(),
        command: source.template.command.clone(),
        args: source.template.args.clone(),
        working_directory_policy: match &source.template.working_directory {
            PackageSessionTemplateWorkingDirectory::PackageRoot => "package_root".to_string(),
            PackageSessionTemplateWorkingDirectory::Relative { .. } => "relative".to_string(),
        },
        allowed_environment_overrides: source.template.allowed_environment_overrides.clone(),
        context_keys: source.template.context.clone(),
        target_id: source_default_target_id(source),
        available: source.available,
    }
}

fn source_template_id(source: &SourceTemplate) -> String {
    format!("{}/{}", source.source_name, source.template.id)
}

fn source_default_target_id(source: &SourceTemplate) -> String {
    source
        .template
        .target_id
        .clone()
        .unwrap_or_else(|| match source.rank {
            TemplateSourceRank::Package => package_target_id(&source.source_name),
            TemplateSourceRank::Device => DEFAULT_DEVICE_TARGET_ID.to_string(),
            TemplateSourceRank::Repo => source.source_name.clone(),
        })
}

fn find_source_template(
    records: &[&PackageRecord],
    state: &HubState,
    template_id: &str,
) -> SessionTemplateResult<SourceTemplate> {
    let sources = source_templates(records, state)?;
    let matches = sources
        .into_iter()
        .filter(|source| {
            source.template.id == template_id || source_template_id(source) == template_id
        })
        .collect::<Vec<_>>();
    choose_effective_template(matches)
}

fn effective_templates(sources: Vec<SourceTemplate>) -> SessionTemplateResult<Vec<SourceTemplate>> {
    let mut by_id = BTreeMap::<String, Vec<SourceTemplate>>::new();
    for source in sources {
        by_id
            .entry(source.template.id.clone())
            .or_default()
            .push(source);
    }
    by_id
        .into_values()
        .map(choose_effective_template)
        .collect::<SessionTemplateResult<Vec<_>>>()
}

fn choose_effective_template(
    mut matches: Vec<SourceTemplate>,
) -> SessionTemplateResult<SourceTemplate> {
    if matches.is_empty() {
        return Err(SessionTemplateError::new(
            "unknown_template",
            "session template was not found",
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
        _ => Err(SessionTemplateError::new(
            "ambiguous_template",
            "session template id matches more than one source at the same precedence",
        )),
    }
}

fn source_templates(
    records: &[&PackageRecord],
    state: &HubState,
) -> SessionTemplateResult<Vec<SourceTemplate>> {
    let mut sources = Vec::new();
    for record in records {
        let root = package_root(record).ok();
        for template in &record.session_templates {
            validate_template(template)?;
            if let Some(root) = &root {
                sources.push(SourceTemplate {
                    rank: TemplateSourceRank::Package,
                    source: PACKAGE_TEMPLATE_SOURCE.to_string(),
                    source_name: record.manifest.name.clone(),
                    root: root.clone(),
                    template: template.clone(),
                    available: record.state == PackageState::Enabled,
                });
            }
        }
    }

    for device_source in &state.device_session_template_sources {
        validate_session_templates(&device_source.session_templates).map_err(|message| {
            SessionTemplateError::new("invalid_device_session_templates", message)
        })?;
        for template in &device_source.session_templates {
            sources.push(SourceTemplate {
                rank: TemplateSourceRank::Device,
                source: DEVICE_TEMPLATE_SOURCE.to_string(),
                source_name: DEVICE_TEMPLATE_SOURCE.to_string(),
                root: device_source.root.clone(),
                template: template.clone(),
                available: true,
            });
        }
    }

    for target in list_spawn_targets(&state.spawn_targets) {
        if !target.enabled {
            continue;
        }
        let repo_templates = repo_session_templates(&target.root)?;
        validate_session_templates(&repo_templates).map_err(|message| {
            SessionTemplateError::new("invalid_repo_session_templates", message)
        })?;
        for template in repo_templates {
            sources.push(SourceTemplate {
                rank: TemplateSourceRank::Repo,
                source: REPO_TEMPLATE_SOURCE.to_string(),
                source_name: target.target_id.clone(),
                root: target.root.clone(),
                template,
                available: true,
            });
        }
    }

    Ok(sources)
}

fn repo_session_templates(root: &Path) -> SessionTemplateResult<Vec<PackageSessionTemplate>> {
    let path = root.join(REPO_SESSION_TEMPLATES_FILE);
    match fs::read(&path) {
        Ok(bytes) => {
            let file: RepoSessionTemplatesFile =
                serde_json::from_slice(&bytes).map_err(|error| {
                    SessionTemplateError::new(
                        "invalid_repo_session_templates",
                        format!("repo-local session template file is invalid: {error}"),
                    )
                })?;
            Ok(file.session_templates)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(SessionTemplateError::new(
            "invalid_repo_session_templates",
            format!("repo-local session template file could not be read: {error}"),
        )),
    }
}

fn validate_template(template: &PackageSessionTemplate) -> SessionTemplateResult<()> {
    if template.id.trim().is_empty() {
        return Err(SessionTemplateError::new(
            "invalid_template",
            "session template id is empty",
        ));
    }
    validate_relative_manifest_path(&template.command, "command")?;
    if let PackageSessionTemplateWorkingDirectory::Relative { path } = &template.working_directory {
        validate_relative_manifest_path(path, "working directory")?;
    }
    for name in template.environment.keys() {
        validate_environment_name(name)?;
    }
    for name in &template.allowed_environment_overrides {
        validate_environment_name(name)?;
    }
    Ok(())
}

pub fn validate_session_templates(templates: &[PackageSessionTemplate]) -> Result<(), String> {
    let mut ids = BTreeSet::new();
    for template in templates {
        validate_template(template).map_err(|error| error.message)?;
        if !ids.insert(template.id.as_str()) {
            return Err(format!("duplicate session template id {}", template.id));
        }
    }
    Ok(())
}

fn package_root(record: &PackageRecord) -> SessionTemplateResult<PathBuf> {
    match &record.manifest.source {
        Some(PackageSource::Path { path }) => Ok(PathBuf::from(path)),
        _ => Err(SessionTemplateError::new(
            "template_unavailable",
            "session template package has no local package root",
        )),
    }
}

fn resolve_working_directory(
    package_root: &Path,
    template: &PackageSessionTemplate,
) -> SessionTemplateResult<PathBuf> {
    match &template.working_directory {
        PackageSessionTemplateWorkingDirectory::PackageRoot => Ok(package_root.to_path_buf()),
        PackageSessionTemplateWorkingDirectory::Relative { path } => Ok(package_root.join(path)),
    }
}

fn resolve_command_path(package_root: &Path, command: &str) -> PathBuf {
    package_root.join(command)
}

fn validate_relative_manifest_path(value: &str, label: &str) -> SessionTemplateResult<()> {
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
        return Err(SessionTemplateError::new(
            "invalid_template_path",
            format!("session template {label} is unsafe"),
        ));
    }
    Ok(())
}

fn validate_environment_name(name: &str) -> SessionTemplateResult<()> {
    let valid = !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        && !name.as_bytes()[0].is_ascii_digit();
    if valid {
        Ok(())
    } else {
        Err(SessionTemplateError::new(
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
    input: SessionTemplateContextInput,
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
