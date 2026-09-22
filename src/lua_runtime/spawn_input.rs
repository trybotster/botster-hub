//! Project admitted JSON into an ordinary spawn request.

use std::collections::BTreeMap;

use botster_core::{PluginKey, SessionId};
use serde_json::Value;

use crate::lua_memory::{LuaCallbackCharge, layout};
use crate::session_types::{SessionTypeContextInput, SessionTypeRequest};

/// The JSON builder keeps its original allowance open through projection.
struct JsonInput {
    value: Value,
    storage: LuaCallbackCharge,
}

/// The original request retains P after JSON storage J is destroyed.
pub(crate) struct SpawnInput {
    plugin_key: PluginKey,
    session_type_id: String,
    request: SessionTypeRequest,
    variable: LuaCallbackCharge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionError {
    MissingSessionType,
    ContextNotObject,
    MapNotObject(&'static str),
    MapValueNotString { field: &'static str, index: usize },
    Capacity,
}

/// Keep JSON and its allowance until the callback finishes error conversion.
struct ProjectionFailure {
    input: JsonInput,
    reason: ProjectionError,
}

fn optional_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn map_bytes(value: Option<&Value>, field: &'static str) -> Result<usize, ProjectionError> {
    let Some(value) = value else {
        return Ok(0);
    };
    let map = value
        .as_object()
        .ok_or(ProjectionError::MapNotObject(field))?;
    let mut bytes = layout::btree_nodes_checked::<String, String>(map.len())
        .ok_or(ProjectionError::Capacity)?;
    for (index, (key, value)) in map.iter().enumerate() {
        let value = value
            .as_str()
            .ok_or(ProjectionError::MapValueNotString { field, index })?;
        bytes = bytes
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or(ProjectionError::Capacity)?;
    }
    Ok(bytes)
}

/// Validate in the same order as the existing JSON projection.
fn projection_bytes(value: &Value, plugin_key: &PluginKey) -> Result<usize, ProjectionError> {
    let session_type_id = value
        .get("session_type_id")
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .ok_or(ProjectionError::MissingSessionType)?;
    let mut bytes = plugin_key
        .0
        .len()
        .checked_add(session_type_id.len())
        .ok_or(ProjectionError::Capacity)?;
    for key in ["target_id", "session_id", "cwd"] {
        bytes = bytes
            .checked_add(optional_string(value, key).map_or(0, str::len))
            .ok_or(ProjectionError::Capacity)?;
    }
    bytes = bytes
        .checked_add(map_bytes(value.get("environment"), "environment")?)
        .ok_or(ProjectionError::Capacity)?;
    if let Some(context) = value.get("context") {
        if !context.is_object() {
            return Err(ProjectionError::ContextNotObject);
        }
        for key in [
            "worktree_path",
            "repo_path",
            "branch_name",
            "prompt",
            "ticket_id",
            "workspace_id",
        ] {
            bytes = bytes
                .checked_add(optional_string(context, key).map_or(0, str::len))
                .ok_or(ProjectionError::Capacity)?;
        }
        bytes = bytes
            .checked_add(map_bytes(context.get("metadata"), "context.metadata")?)
            .ok_or(ProjectionError::Capacity)?;
    }
    Ok(bytes)
}

fn copy_string(value: &Value, key: &str) -> Option<String> {
    optional_string(value, key).map(str::to_owned)
}

/// Insert directly into the charged tree without a temporary collection.
fn copy_map(value: Option<&Value>) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    if let Some(value) = value {
        for (key, value) in value.as_object().expect("projection validated the object") {
            result.insert(
                key.to_owned(),
                value
                    .as_str()
                    .expect("projection validated the string")
                    .to_owned(),
            );
        }
    }
    result
}

fn project(mut input: JsonInput, plugin_key: &PluginKey) -> Result<SpawnInput, ProjectionFailure> {
    let bytes = match projection_bytes(&input.value, plugin_key) {
        Ok(bytes) => bytes,
        Err(reason) => return Err(ProjectionFailure { input, reason }),
    };
    // J remains live while the original open allowance admits P.
    if input.storage.grow(bytes).is_err() {
        return Err(ProjectionFailure {
            input,
            reason: ProjectionError::Capacity,
        });
    }
    let value = &input.value;
    let session_type_id = value
        .get("session_type_id")
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .expect("projection validated the session type")
        .to_owned();
    let context = value.get("context");
    let request = SessionTypeRequest {
        target_id: copy_string(value, "target_id"),
        session_id: copy_string(value, "session_id").map(SessionId),
        cwd: copy_string(value, "cwd"),
        environment: copy_map(value.get("environment")),
        context: SessionTypeContextInput {
            worktree_path: context.and_then(|value| copy_string(value, "worktree_path")),
            repo_path: context.and_then(|value| copy_string(value, "repo_path")),
            branch_name: context.and_then(|value| copy_string(value, "branch_name")),
            prompt: context.and_then(|value| copy_string(value, "prompt")),
            ticket_id: context.and_then(|value| copy_string(value, "ticket_id")),
            workspace_id: context.and_then(|value| copy_string(value, "workspace_id")),
            metadata: copy_map(context.and_then(|value| value.get("metadata"))),
        },
    };
    let plugin_key = plugin_key.clone();
    drop(input.value);
    assert!(input.storage.shrink_to(bytes));
    Ok(SpawnInput {
        plugin_key,
        session_type_id,
        request,
        variable: input.storage,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};

    fn input(value: Value, limit: usize) -> (JsonInput, Arc<LuaMemoryAccount>) {
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: limit,
            total_vm_bytes: limit,
            per_callback_bytes: limit,
            total_callback_bytes: limit,
        })
        .unwrap();
        let bytes = super::super::lua_json::retained_bytes(&value).unwrap();
        let storage = memory.reserve_callback_total(bytes).unwrap();
        (JsonInput { value, storage }, memory)
    }

    #[test]
    fn projection_preserves_full_request_and_retains_charge_after_json() {
        let value = serde_json::json!({
            "id": "worker",
            "target_id": "target",
            "session_id": "explicit-id",
            "cwd": "/workspace",
            "environment": {"FIRST": "one", "SECOND": "two"},
            "context": {
                "worktree_path": "/worktree", "repo_path": "/repository",
                "branch_name": "branch", "prompt": "prompt", "ticket_id": "ticket",
                "workspace_id": "workspace", "metadata": {"name": "value"}
            }
        });
        let expected = super::super::session_type_request_from_lua(&value).unwrap();
        let (input, memory) = input(value, 64 * 1024);
        let projected = match project(input, &PluginKey("plugin".into())) {
            Ok(projected) => projected,
            Err(_) => panic!("the complete request must fit"),
        };
        assert_eq!(projected.request, expected);
        assert_eq!(projected.session_type_id, "worker");
        assert_eq!(projected.plugin_key.0, "plugin");
        assert!(projected.variable.bytes() > 0);
        assert_eq!(memory.usage().1, projected.variable.bytes());
        drop(projected);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn projection_refusal_keeps_original_json_and_charge() {
        let value = serde_json::json!({"id": "worker", "cwd": "/workspace"});
        let bytes = super::super::lua_json::retained_bytes(&value).unwrap();
        let (input, memory) = input(value.clone(), bytes);
        let failure = match project(input, &PluginKey("plugin".into())) {
            Err(failure) => failure,
            Ok(_) => panic!("the overlapping projection must refuse"),
        };
        assert_eq!(failure.reason, ProjectionError::Capacity);
        assert_eq!(failure.input.value, value);
        assert_eq!(memory.usage().1, bytes);
        drop(failure);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn projection_preserves_id_precedence_and_validation_order() {
        let plugin = PluginKey("plugin".into());
        assert_eq!(
            projection_bytes(
                &serde_json::json!({"session_type_id": false, "id": "valid"}),
                &plugin
            ),
            Err(ProjectionError::MissingSessionType)
        );
        assert_eq!(
            projection_bytes(
                &serde_json::json!({"id": "worker", "environment": {"b": false, "a": 1}, "context": false}),
                &plugin
            ),
            Err(ProjectionError::MapValueNotString {
                field: "environment",
                index: 0
            })
        );
    }
}
