//! Charged Lua conversion for plugin session-type spawn results.

use std::io::{self, Write};
use std::sync::Arc;

use mlua::{Lua, LuaSerdeExt, Value};

use crate::lua_memory::{LuaCallbackAdmissionError, LuaMemoryAccount};
use crate::runtime::{
    PluginManagedSessionSpawned, PluginSessionTypeSpawned, SharedSessionTypeSpawner,
};

/// Report conversion through the exact owner receipt instead of a session ID.
#[allow(dead_code)] // The daemon callback will use this conversion path.
pub(super) fn convert_session_type_spawn_delivery(
    lua: &Lua,
    memory: &Arc<LuaMemoryAccount>,
    spawned: &PluginSessionTypeSpawned,
    receipt: crate::runtime::SpawnConversionReceipt,
    conversion_failure: &mlua::String,
) -> mlua::Result<Value> {
    let mut receipt = Some(receipt);
    let result = convert_json(
        lua,
        Some(memory),
        || {
            if let Some(receipt) = receipt.take() {
                receipt.abandon();
            }
        },
        spawned,
        conversion_failure,
    );
    if let Some(receipt) = receipt.take() {
        if result.is_ok() {
            receipt.converted();
        } else {
            receipt.abandon();
        }
    }
    result
}

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
    let conversion = sink.0.checked_add(17).ok_or_else(|| {
        mlua::Error::RuntimeError("session spawn conversion size overflow".into())
    })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};
    use crate::runtime::{HubSessionTypeSpawner, PluginManagedSessionSpawned};

    fn spawned() -> PluginManagedSessionSpawned {
        PluginManagedSessionSpawned {
            session_id: "s1-abandon".into(),
            target_id: "t1".into(),
            branch: "topic".into(),
            worktree_id: "wt".into(),
            worktree_path: "/tmp/wt".into(),
            base_ref: "HEAD".into(),
            base_commit: "0".repeat(40),
            created_worktree: true,
            created_branch: false,
            reused_worktree: false,
        }
    }

    #[test]
    fn managed_conversion_quota_abandons_the_session() {
        let lua = Lua::new();
        let account = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1024,
            total_vm_bytes: 1024,
            per_callback_bytes: 1,
            total_callback_bytes: 1,
        })
        .expect("tiny callback account");
        let spawner = std::sync::Arc::new(HubSessionTypeSpawner::new());
        let failure = lua.create_string("conversion failed").unwrap();
        let value =
            convert_managed_spawned(&lua, Some(&account), &spawner, &spawned(), &failure).unwrap();
        assert!(matches!(value, Value::String(_)));
        assert_eq!(
            spawner.test_take_abandoned(),
            vec!["s1-abandon".to_string()]
        );
    }

    #[test]
    fn managed_conversion_success_does_not_abandon() {
        let lua = Lua::new();
        let account = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .expect("callback account");
        let spawner = std::sync::Arc::new(HubSessionTypeSpawner::new());
        let failure = lua.create_string("conversion failed").unwrap();
        let value =
            convert_managed_spawned(&lua, Some(&account), &spawner, &spawned(), &failure).unwrap();
        assert!(!matches!(value, Value::String(ref s) if s == &failure));
        assert!(spawner.test_take_abandoned().is_empty());
    }
}
