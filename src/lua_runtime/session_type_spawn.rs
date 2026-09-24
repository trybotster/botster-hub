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
        || {
            if let Some(identity) = spawned.reservation_identity {
                spawner.abandon_session_type_spawn(spawned.session_id.clone(), identity);
            }
        },
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
        || {
            if let Some(identity) = spawned.reservation_identity {
                spawner.abandon_session_type_spawn(spawned.session_id.clone(), identity);
            }
        },
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

    fn delivery_memory() -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1024 * 1024,
            total_vm_bytes: 1024 * 1024,
            per_callback_bytes: 1024 * 1024,
            total_callback_bytes: 1024 * 1024,
        })
        .unwrap()
    }

    fn delivery_spawned() -> PluginSessionTypeSpawned {
        PluginSessionTypeSpawned {
            session_id: "receipt-success".into(),
            lifecycle: "running".into(),
            session_type_id: "shell".into(),
            context_id: "context".into(),
            context_keys: vec!["key".into()],
            reservation_identity: None,
        }
    }

    #[test]
    fn delivery_conversion_success_reports_converted_once() {
        use crate::data_plane::driver::local_reply_tests::SpawnReceiptFixture;
        use crate::runtime::SpawnConversionOutcome;

        let account = delivery_memory();
        let lua = Lua::new();
        let failure = lua.create_string("conversion failed").unwrap();
        let spawned = delivery_spawned();
        let (fixture, receipt) = SpawnReceiptFixture::new(&account);
        let receipt_bytes = account.usage().1;
        let value =
            convert_session_type_spawn_delivery(&lua, &account, &spawned, receipt, &failure)
                .unwrap();
        let Value::Table(table) = value else {
            panic!("conversion must return a table")
        };
        assert_eq!(
            table.get::<String>("session_id").unwrap(),
            spawned.session_id
        );
        assert_eq!(table.get::<String>("lifecycle").unwrap(), spawned.lifecycle);
        assert_eq!(
            table.get::<String>("session_type_id").unwrap(),
            spawned.session_type_id
        );
        assert_eq!(
            table.get::<String>("context_id").unwrap(),
            spawned.context_id
        );
        assert_eq!(
            table.get::<Vec<String>>("context_keys").unwrap(),
            spawned.context_keys
        );
        assert_eq!(account.usage().1, receipt_bytes);
        fixture.assert_outcome(SpawnConversionOutcome::Converted);
        assert_eq!(account.usage().1, 0);
    }

    #[test]
    fn delivery_lua_allocation_failure_reports_abandoned_once() {
        use crate::data_plane::driver::local_reply_tests::SpawnReceiptFixture;
        use crate::runtime::SpawnConversionOutcome;

        let account = delivery_memory();
        let lua = Lua::new();
        let failure = lua
            .create_string("session_types.spawn could not allocate its Lua result")
            .unwrap();
        let mut spawned = delivery_spawned();
        spawned.context_id = "large-context-".repeat(4096);
        let (fixture, receipt) = SpawnReceiptFixture::new(&account);
        let receipt_bytes = account.usage().1;
        let mut sink = CountingSink(0);
        serde_json::to_writer(&mut sink, &spawned).unwrap();
        // Prove that Rust admission succeeds before forcing Lua allocation failure.
        let admission = account.reserve_callback_total(sink.0 + 17).unwrap();
        drop(admission);
        lua.set_memory_limit(lua.used_memory() + 1024).unwrap();
        let result =
            convert_session_type_spawn_delivery(&lua, &account, &spawned, receipt, &failure);
        lua.set_memory_limit(0).unwrap();
        let Value::String(actual) = result.unwrap() else {
            panic!("Lua allocation failure must return its precreated string")
        };
        assert_eq!(
            actual.to_str().unwrap(),
            "session_types.spawn could not allocate its Lua result",
        );
        assert_eq!(actual, failure);
        assert_eq!(account.usage().1, receipt_bytes);
        fixture.assert_outcome(SpawnConversionOutcome::Abandoned);
        assert_eq!(account.usage().1, 0);
    }

    #[test]
    fn admitted_delivery_retains_disjoint_charges_through_conversion() {
        use crate::data_plane::driver::{
            local_reply_tests::SpawnReceiptFixture, retained_reply_bytes,
        };
        use crate::lua_memory::{LuaCallbackCharge, layout};
        use crate::runtime::{SpawnConversionOutcome, SpawnConversionReceipt, spawn_reply_channel};
        use std::time::Duration;

        type Delivery = (
            PluginSessionTypeSpawned,
            SpawnConversionReceipt,
            LuaCallbackCharge,
        );
        let account = delivery_memory();
        let lua = Lua::new();
        let failure = lua.create_string("conversion failed").unwrap();
        let spawned = delivery_spawned();
        let payload_bytes = spawned.session_id.capacity()
            + spawned.lifecycle.capacity()
            + spawned.session_type_id.capacity()
            + spawned.context_id.capacity()
            + spawned.context_keys.capacity() * std::mem::size_of::<String>()
            + spawned
                .context_keys
                .iter()
                .map(String::capacity)
                .sum::<usize>();
        let receipt_bytes = retained_reply_bytes::<SpawnConversionOutcome>().unwrap();
        let channel_bytes = layout::single_reply_bytes::<Delivery>(true).unwrap();
        let total = receipt_bytes + channel_bytes + payload_bytes;
        let mut admitted = account.reserve_callback_total(total).unwrap();
        let (fixture, receipt) =
            SpawnReceiptFixture::from_charge(admitted.split(receipt_bytes).unwrap());
        let (sender, receiver) =
            spawn_reply_channel::<Delivery>(admitted.split(channel_bytes).unwrap()).unwrap();
        assert_eq!(admitted.bytes(), payload_bytes);
        assert!(sender.try_send((spawned, receipt, admitted)).is_ok());
        assert_eq!(account.usage().1, total);
        let (spawned, receipt, payload) = receiver.recv_timeout(Duration::ZERO).unwrap();
        assert_eq!(account.usage().1, total);
        let value =
            convert_session_type_spawn_delivery(&lua, &account, &spawned, receipt, &failure)
                .unwrap();
        assert!(matches!(value, Value::Table(_)));
        assert_eq!(account.usage().1, total);
        drop(receiver);
        assert_eq!(account.usage().1, receipt_bytes + payload_bytes);
        drop(spawned);
        drop(payload);
        assert_eq!(account.usage().1, receipt_bytes);
        fixture.assert_outcome(SpawnConversionOutcome::Converted);
        assert_eq!(account.usage().1, 0);
    }

    #[test]
    fn delivery_conversion_quota_reports_abandoned_once() {
        use crate::data_plane::driver::{
            local_reply_tests::SpawnReceiptFixture, retained_reply_bytes,
        };
        use crate::runtime::SpawnConversionOutcome;

        let bytes = retained_reply_bytes::<SpawnConversionOutcome>().unwrap();
        let account = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: bytes + 1,
            total_callback_bytes: bytes + 1,
        })
        .unwrap();
        let lua = Lua::new();
        let failure = lua.create_string("conversion failed").unwrap();
        let spawned = PluginSessionTypeSpawned {
            session_id: "receipt-quota".into(),
            lifecycle: "running".into(),
            session_type_id: "shell".into(),
            context_id: "context".into(),
            context_keys: vec!["key".into()],
            reservation_identity: None,
        };
        let (fixture, receipt) = SpawnReceiptFixture::new(&account);
        let result =
            convert_session_type_spawn_delivery(&lua, &account, &spawned, receipt, &failure)
                .unwrap();
        assert!(matches!(result, Value::String(ref value) if value == &failure));
        assert_eq!(account.usage().1, bytes);
        fixture.assert_outcome(SpawnConversionOutcome::Abandoned);
        assert_eq!(account.usage().1, 0);
    }

    fn spawned() -> PluginManagedSessionSpawned {
        let identity = botster_core::SessionAdmission::default()
            .reserve(botster_core::SessionId("s1-abandon".into()))
            .unwrap()
            .identity();
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
            reservation_identity: Some(identity),
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
        let spawned = spawned();
        let value =
            convert_managed_spawned(&lua, Some(&account), &spawner, &spawned, &failure).unwrap();
        assert!(matches!(value, Value::String(_)));
        assert_eq!(
            spawner.test_take_abandoned(),
            vec![("s1-abandon".to_string(), spawned.reservation_identity.unwrap())]
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
