//! Hub-owned spawn target registry.
//!
//! Spawn targets are admitted hub policy state. They are not botster-core
//! concepts, and plugins reference their stable ids instead of resolving local
//! filesystem paths themselves.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub metadata: BTreeMap<String, String>,
}

/// Update request accepted by daemon/CLI callers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpawnTargetUpdate {
    pub label: Option<String>,
    pub root: Option<PathBuf>,
    pub enabled: Option<bool>,
    pub kind: Option<String>,
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
    fn new(kind: &'static str, message: impl Into<String>) -> Self {
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
    validate_metadata(&request.metadata)?;
    let target = SpawnTarget {
        target_id,
        label,
        root,
        enabled: request.enabled,
        kind,
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
    if let Some(label) = request.label {
        target.label = label;
    }
    if let Some(root) = request.root {
        target.root = normalize_root(root)?;
    }
    if let Some(enabled) = request.enabled {
        target.enabled = enabled;
    }
    if let Some(kind) = request.kind {
        target.kind = kind;
    }
    if let Some(metadata) = request.metadata {
        validate_metadata(&metadata)?;
        target.metadata = metadata;
    }
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

    #[test]
    fn validates_missing_disabled_and_enabled_targets() {
        let targets = vec![
            SpawnTarget {
                target_id: "enabled".to_string(),
                label: "Enabled".to_string(),
                root: PathBuf::from("."),
                enabled: true,
                kind: "directory".to_string(),
                metadata: BTreeMap::new(),
            },
            SpawnTarget {
                target_id: "disabled".to_string(),
                label: "Disabled".to_string(),
                root: PathBuf::from("."),
                enabled: false,
                kind: "directory".to_string(),
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
}
