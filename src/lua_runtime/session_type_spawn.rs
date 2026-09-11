//! Charged Lua conversion for plugin session-type spawn results.

use std::io::{self, Write};
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
    spawner: &SharedSessionTypeSpawner,
    spawned: &PluginManagedSessionSpawned,
    conversion_failure: &mlua::String,
) -> mlua::Result<Value> {
    convert_json(
        lua,
        memory,
        || spawner.abandon_session_type_spawn(spawned.session_id.clone()),
        spawned,
        conversion_failure,
    )
}

/// Counts JSON bytes without retaining a buffer.
///
/// `to_value` allocates A6-style handles, not JSON. These spawn results have a
/// fixed shape (strings plus one `Vec<String>`), so JSON length is a conservative
/// upper bound for that conversion.
struct CountingSink(usize);

impl Write for CountingSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn convert_json<T: serde::Serialize>(
    lua: &Lua,
    memory: Option<&Arc<LuaMemoryAccount>>,
    abandon: impl FnOnce(),
    value: &T,
    conversion_failure: &mlua::String,
) -> mlua::Result<Value> {
    let mut sink = CountingSink(0);
    serde_json::to_writer(&mut sink, value).map_err(|error| {
        mlua::Error::RuntimeError(format!("session spawn result encoding failed: {error}"))
    })?;
    let conversion = sink
        .0
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
