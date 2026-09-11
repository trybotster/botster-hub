//! Charged Lua conversion for plugin session-type spawn results.

use std::sync::Arc;

use mlua::{Lua, LuaSerdeExt, Value};

use crate::lua_memory::{LuaCallbackAdmissionError, LuaMemoryAccount};
use crate::runtime::{PluginManagedSessionSpawned, PluginSessionTypeSpawned, SharedSessionTypeSpawner};

pub(super) fn convert_session_type_spawned(
    lua: &Lua,
    memory: Option<&Arc<LuaMemoryAccount>>,
    spawner: &SharedSessionTypeSpawner,
    spawned: PluginSessionTypeSpawned,
    conversion_failure: &mlua::String,
) -> mlua::Result<Value> {
    convert_json(
        lua,
        memory,
        || spawner.abandon_session_type_spawn(spawned.session_id.clone()),
        &spawned,
        conversion_failure,
    )
}

pub(super) fn convert_managed_spawned(
    lua: &Lua,
    memory: Option<&Arc<LuaMemoryAccount>>,
    spawned: &PluginManagedSessionSpawned,
    conversion_failure: &mlua::String,
) -> mlua::Result<Value> {
    convert_json(lua, memory, || {}, spawned, conversion_failure)
}

fn convert_json<T: serde::Serialize>(
    lua: &Lua,
    memory: Option<&Arc<LuaMemoryAccount>>,
    abandon: impl FnOnce(),
    value: &T,
    conversion_failure: &mlua::String,
) -> mlua::Result<Value> {
    let encoded = serde_json::to_vec(value).map_err(|error| {
        mlua::Error::RuntimeError(format!("session spawn result encoding failed: {error}"))
    })?;
    let conversion = encoded
        .len()
        .checked_add(17)
        .ok_or_else(|| mlua::Error::RuntimeError("session spawn conversion size overflow".into()))?;
    let charge = match memory {
        Some(account) => match account.reserve_callback_total(conversion) {
            Ok(charge) => Some(charge),
            Err(LuaCallbackAdmissionError::Quota | LuaCallbackAdmissionError::Capacity(_)) => {
                abandon();
                return Ok(Value::String(conversion_failure.clone()));
            }
        },
        None => None,
    };
    match lua.to_value(value) {
        Ok(converted) => {
            drop(charge);
            Ok(converted)
        }
        Err(_) => {
            drop(charge);
            abandon();
            Ok(Value::String(conversion_failure.clone()))
        }
    }
}
