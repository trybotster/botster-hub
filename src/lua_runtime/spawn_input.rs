//! Project admitted JSON into an ordinary spawn request.

use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::sync::Arc;

use botster_core::{PluginKey, SessionId};
use mlua::{Lua, Value as LuaValue};
use serde_json::Value;

use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount, layout};
use crate::session_types::{SessionTypeContextInput, SessionTypeRequest};

/// The JSON builder keeps its original allowance open through projection.
pub(super) struct JsonInput {
    value: Value,
    storage: LuaCallbackCharge,
}

impl JsonInput {
    pub(super) fn new(value: Value, storage: LuaCallbackCharge) -> Self {
        Self { value, storage }
    }
}

/// The original request retains P after JSON storage J is destroyed.
pub(crate) struct SpawnInput {
    plugin_key: PluginKey,
    session_type_id: String,
    request: SessionTypeRequest,
    variable: LuaCallbackCharge,
}

impl SpawnInput {
    pub(crate) fn into_parts(self) -> (PluginKey, String, SessionTypeRequest, LuaCallbackCharge) {
        (
            self.plugin_key,
            self.session_type_id,
            self.request,
            self.variable,
        )
    }
}

/// Admit J plus build scratch K, then project P while J remains live.
pub(super) fn admit(
    lua: &Lua,
    args: &LuaValue,
    plugin_key: &PluginKey,
    memory: &Arc<LuaMemoryAccount>,
    capacity: &mlua::String,
) -> Result<SpawnInput, super::callback::CallbackFailure> {
    let mut parent = memory
        .reserve_callback_total(0)
        .map_err(|_| super::callback::CallbackFailure::Raise(capacity.clone()))?;
    let admission = match super::lua_json::value_size_scoped(memory, lua, args) {
        Ok(admission) => admission,
        Err(error) => {
            return Err(super::lua_json::raise_admission_error(
                error, lua, parent, capacity,
            ));
        }
    };
    let json_and_scratch = admission
        .json_bytes
        .checked_add(admission.scratch_peak)
        .ok_or_else(|| super::callback::CallbackFailure::Raise(capacity.clone()))?;
    parent
        .grow(json_and_scratch)
        .map_err(|_| super::callback::CallbackFailure::Raise(capacity.clone()))?;
    let value = match admission.build_scoped(lua, args, &mut parent) {
        Ok(value) => value,
        Err(error) => {
            return Err(super::lua_json::raise_admission_error(
                super::AdmissionError::Runtime(error),
                lua,
                parent,
                capacity,
            ));
        }
    };
    project(JsonInput::new(value, parent), plugin_key)
        .map_err(|error| error.raise(lua, capacity))
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
pub(super) struct ProjectionFailure {
    input: JsonInput,
    reason: ProjectionError,
}

struct CountedMessage(usize);

impl Write for CountedMessage {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}

impl ProjectionFailure {
    fn write_message(&self, output: &mut impl Write) -> fmt::Result {
        match self.reason {
            ProjectionError::MissingSessionType => {
                output.write_str("session_types.spawn requires session_type_id")
            }
            ProjectionError::ContextNotObject => {
                output.write_str("session_types.spawn context must be an object")
            }
            ProjectionError::MapNotObject(field) => {
                write!(output, "session_types.spawn {field} must be an object")
            }
            ProjectionError::MapValueNotString { field, index } => {
                let map = if field == "environment" {
                    self.input.value.get("environment")
                } else {
                    self.input
                        .value
                        .get("context")
                        .and_then(|context| context.get("metadata"))
                }
                .and_then(Value::as_object)
                .expect("the failed map remains in the charged JSON");
                let key = map
                    .iter()
                    .nth(index)
                    .map(|(key, _)| key.as_str())
                    .expect("the failed key remains in the charged JSON");
                write!(output, "session_types.spawn {field}.{key} must be a string")
            }
            ProjectionError::Capacity => Err(fmt::Error),
        }
    }

    /// Build the exact old error text while JSON and its allowance remain live.
    pub(super) fn raise(
        mut self,
        lua: &Lua,
        capacity: &mlua::String,
    ) -> super::callback::CallbackFailure {
        if self.reason == ProjectionError::Capacity {
            return super::callback::CallbackFailure::Raise(capacity.clone());
        }
        let mut counted = CountedMessage(0);
        if self.write_message(&mut counted).is_err() || self.input.storage.grow(counted.0).is_err()
        {
            return super::callback::CallbackFailure::Raise(capacity.clone());
        }
        let mut message = String::with_capacity(counted.0);
        if self.write_message(&mut message).is_err() {
            return super::callback::CallbackFailure::Raise(capacity.clone());
        }
        let raised = lua.create_string(&message).ok();
        drop(message);
        drop(self);
        super::callback::CallbackFailure::Raise(raised.unwrap_or_else(|| capacity.clone()))
    }
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

pub(super) fn project(
    mut input: JsonInput,
    plugin_key: &PluginKey,
) -> Result<SpawnInput, ProjectionFailure> {
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

    #[test]
    fn projection_error_raises_exact_message_after_releasing_json() {
        let value = serde_json::json!({
            "id": "worker",
            "environment": {"BROKEN": false}
        });
        let (json_input, memory) = input(value.clone(), 64 * 1024);
        let failure = match project(json_input, &PluginKey("plugin".into())) {
            Err(failure) => failure,
            Ok(_) => panic!("the invalid environment must refuse"),
        };
        let lua = Lua::new();
        let capacity = lua.create_string("capacity").unwrap();
        let super::super::callback::CallbackFailure::Raise(message) =
            failure.raise(&lua, &capacity)
        else {
            panic!("the charged error must use the Lua Raise path");
        };
        assert_eq!(
            message.to_str().unwrap(),
            "session_types.spawn environment.BROKEN must be a string"
        );
        assert_eq!(memory.usage().1, 0);

        // J alone fits. The Rust error String does not fit beside J.
        let json_bytes = super::super::lua_json::retained_bytes(&value).unwrap();
        let (json_input, memory) = input(value, json_bytes);
        let failure = match project(json_input, &PluginKey("plugin".into())) {
            Err(failure) => failure,
            Ok(_) => panic!("the invalid environment must refuse"),
        };
        let super::super::callback::CallbackFailure::Raise(message) =
            failure.raise(&lua, &capacity)
        else {
            panic!("the capacity error must use the Lua Raise path");
        };
        assert_eq!(message.to_str().unwrap(), "capacity");
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn staged_lua_admission_retains_only_projected_input() {
        let lua = Lua::new();
        let args = lua
            .load("return {id='worker', session_id='first', environment={FIRST='one'}}")
            .eval::<LuaValue>()
            .unwrap();
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .unwrap();
        let capacity = lua.create_string("capacity").unwrap();
        let input = match admit(&lua, &args, &PluginKey("plugin".into()), &memory, &capacity) {
            Ok(input) => input,
            Err(_) => panic!("the staged input must fit"),
        };
        assert_eq!(input.session_type_id, "worker");
        assert_eq!(
            input.request.session_id.as_ref().unwrap().0.as_str(),
            "first"
        );
        assert_eq!(memory.usage().1, input.variable.bytes());
        drop(input);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn staged_lua_admission_reuses_released_scratch_capacity() {
        use mlua::LuaSerdeExt;

        let lua = Lua::new();
        let args = lua
            .load("return {id='worker', session_id='first', environment={FIRST='one'}}")
            .eval::<LuaValue>()
            .unwrap();
        let plugin = PluginKey("plugin".into());
        let large = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .unwrap();
        let admission = super::super::lua_json::value_size_scoped(&large, &lua, &args).unwrap();
        let json: Value = lua.from_value(args.clone()).unwrap();
        let projection = projection_bytes(&json, &plugin).unwrap();
        assert!(admission.scratch_peak > 0);
        assert!(projection > 0);
        let limit = admission.json_bytes + projection.max(admission.scratch_peak);
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: limit,
            total_callback_bytes: limit,
        })
        .unwrap();
        let capacity = lua.create_string("capacity").unwrap();
        let input = admit(&lua, &args, &plugin, &memory, &capacity)
            .unwrap_or_else(|_| panic!("J+P fits after K is released"));
        assert_eq!(input.session_type_id, "worker");
        drop(input);
        assert_eq!(memory.usage().1, 0);
    }
}
