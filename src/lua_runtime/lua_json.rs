//! Iterative two-pass Lua → `serde_json::Value` conversion.
//!
//! No native-stack recursion and no depth cap. Cycles use the live frame
//! pointers, matching `lua.from_value`'s `RecursionGuard`.
//!
//! Scratch is charged on the callback account before each allocation:
//! - Frame slots: `ChargedVec<Frame>` grows with `try_reserve_exact`.
//! - Live mlua `ValueRef` handles (`layout::lua_reference_bytes()` =
//!   `ArcInner<c_int>`): array frame = table + current element (2×);
//!   object frame = table + key + value (3×).
//! Array detection matches mlua `from_value` (`detect_mixed_tables = false`):
//! `raw_len() > 0` or the array metatable.

use std::os::raw::c_void;
use std::sync::Arc;

use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde_json::{Map, Number};

use crate::lua_memory::charged_collection::ChargedVec;
use crate::lua_memory::layout::{btree_nodes_checked, lua_reference_bytes};
use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount, LuaMemoryCapacityError};

pub(crate) struct JsonAdmission {
    pub json_bytes: usize,
    pub scratch_peak: usize,
}

pub(crate) fn value_size(
    memory: &Arc<LuaMemoryAccount>,
    lua: &Lua,
    value: &Value,
) -> Result<JsonAdmission, super::AdmissionError> {
    match value {
        Value::Table(table) => walk_size(memory, lua, table.clone()),
        other => Ok(JsonAdmission {
            json_bytes: leaf_size(other)?,
            scratch_peak: 0,
        }),
    }
}

pub(crate) fn value_build(
    memory: &Arc<LuaMemoryAccount>,
    lua: &Lua,
    value: &Value,
) -> Result<serde_json::Value, mlua::Error> {
    match value {
        Value::Table(table) => walk_build(memory, lua, table.clone()),
        other => leaf_build(other),
    }
}

pub(crate) fn retained_bytes(value: &serde_json::Value) -> Option<usize> {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            Some(0)
        }
        serde_json::Value::String(text) => Some(text.capacity()),
        serde_json::Value::Array(items) => {
            let mut bytes = items
                .capacity()
                .checked_mul(std::mem::size_of::<serde_json::Value>())?;
            for item in items {
                bytes = bytes.checked_add(retained_bytes(item)?)?;
            }
            Some(bytes)
        }
        serde_json::Value::Object(map) => {
            let mut heap = 0usize;
            for (key, item) in map {
                heap = heap
                    .checked_add(key.capacity())?
                    .checked_add(retained_bytes(item)?)?;
            }
            btree_nodes_checked::<String, serde_json::Value>(map.len())
                .and_then(|nodes| nodes.checked_add(heap))
        }
    }
}

const fn array_ref_bytes() -> usize {
    2 * lua_reference_bytes()
}

const fn object_ref_bytes() -> usize {
    3 * lua_reference_bytes()
}

struct Frame {
    table: Table,
    ptr: *const c_void,
    refs: LuaCallbackCharge,
    kind: Kind,
    pending_key: Option<Value>,
    json: usize,
    count: usize,
    build_array: Option<Vec<serde_json::Value>>,
    build_map: Option<Map<String, serde_json::Value>>,
}

enum Kind {
    Array {
        next: usize,
        len: usize,
    },
    Object {
        pairs: ChargedVec<(Value, Value)>,
        next: usize,
    },
}

enum Child {
    End,
    Leaf(Value),
    Table(Table),
}

fn walk_size(
    memory: &Arc<LuaMemoryAccount>,
    lua: &Lua,
    table: Table,
) -> Result<JsonAdmission, super::AdmissionError> {
    let before = memory.callback_used();
    let mut peak = 0usize;
    let array_mt = lua.array_metatable().to_pointer();
    let mut stack = ChargedVec::new(Arc::clone(memory));
    push_frame(
        &mut stack, memory, table, array_mt, false, &mut peak, before,
    )?;
    loop {
        let child = {
            let Some(top) = stack.last_mut() else {
                break;
            };
            take_child(top)?
        };
        match child {
            Child::Table(child) => {
                if stack.iter().any(|frame| frame.ptr == child.to_pointer()) {
                    return Err(super::AdmissionError::Runtime(mlua::Error::RuntimeError(
                        "recursive table detected".into(),
                    )));
                }
                push_frame(
                    &mut stack, memory, child, array_mt, false, &mut peak, before,
                )?;
            }
            Child::Leaf(value) => {
                let json = leaf_size(&value)?;
                let top = stack.last_mut().expect("leaf under a frame");
                add_json(top, json)?;
            }
            Child::End => {
                let done = stack.pop().expect("end of a frame");
                let json = finish_size(&done)?;
                drop(done);
                note_peak(memory, before, &mut peak);
                if stack.len() == 0 {
                    return Ok(JsonAdmission {
                        json_bytes: json,
                        scratch_peak: peak,
                    });
                }
                let parent = stack.last_mut().expect("nested table has a parent");
                add_json(parent, json)?;
            }
        }
        note_peak(memory, before, &mut peak);
    }
    Err(super::AdmissionError::Runtime(mlua::Error::RuntimeError(
        "json walk ended without a root".into(),
    )))
}

fn walk_build(
    memory: &Arc<LuaMemoryAccount>,
    lua: &Lua,
    table: Table,
) -> Result<serde_json::Value, mlua::Error> {
    let array_mt = lua.array_metatable().to_pointer();
    let mut stack = ChargedVec::new(Arc::clone(memory));
    let mut unused_peak = 0usize;
    let before = memory.callback_used();
    push_frame(
        &mut stack,
        memory,
        table,
        array_mt,
        true,
        &mut unused_peak,
        before,
    )
    .map_err(build_capacity)?;
    loop {
        let child = {
            let Some(top) = stack.last_mut() else {
                break;
            };
            take_child(top)?
        };
        match child {
            Child::Table(child) => {
                if stack.iter().any(|frame| frame.ptr == child.to_pointer()) {
                    return Err(mlua::Error::RuntimeError("recursive table detected".into()));
                }
                push_frame(
                    &mut stack,
                    memory,
                    child,
                    array_mt,
                    true,
                    &mut unused_peak,
                    before,
                )
                .map_err(build_capacity)?;
            }
            Child::Leaf(value) => {
                let json = leaf_build(&value)?;
                let top = stack.last_mut().expect("leaf under a frame");
                add_build(top, json)?;
            }
            Child::End => {
                let done = stack.pop().expect("end of a frame");
                let json = finish_build(done)?;
                if stack.len() == 0 {
                    return Ok(json);
                }
                let parent = stack.last_mut().expect("nested table has a parent");
                add_build(parent, json)?;
            }
        }
    }
    Err(mlua::Error::RuntimeError(
        "json walk ended without a root".into(),
    ))
}

fn push_frame(
    stack: &mut ChargedVec<Frame>,
    memory: &Arc<LuaMemoryAccount>,
    table: Table,
    array_mt: *const c_void,
    build: bool,
    peak: &mut usize,
    before: usize,
) -> Result<(), super::AdmissionError> {
    let ptr = table.to_pointer();
    let (kind, refs) = if is_array(&table, array_mt)? {
        let len = table.raw_len();
        if len > i64::MAX as usize {
            return Err(super::AdmissionError::Capacity);
        }
        (
            Kind::Array { next: 1, len },
            memory
                .reserve_callback_bytes(array_ref_bytes())
                .map_err(capacity)?,
        )
    } else {
        let pairs = load_pairs(memory, &table)?;
        (
            Kind::Object { pairs, next: 0 },
            memory
                .reserve_callback_bytes(object_ref_bytes())
                .map_err(capacity)?,
        )
    };
    let json = match &kind {
        Kind::Array { len, .. } => len
            .checked_mul(std::mem::size_of::<serde_json::Value>())
            .ok_or(super::AdmissionError::Capacity)?,
        Kind::Object { .. } => 0,
    };
    let (build_array, build_map) = if build {
        match &kind {
            Kind::Array { len, .. } => (Some(Vec::with_capacity(*len)), None),
            Kind::Object { .. } => (None, Some(Map::new())),
        }
    } else {
        (None, None)
    };
    stack
        .try_push(Frame {
            table,
            ptr,
            refs,
            kind,
            pending_key: None,
            json,
            count: 0,
            build_array,
            build_map,
        })
        .map_err(capacity)?;
    note_peak(memory, before, peak);
    Ok(())
}

fn load_pairs(
    memory: &Arc<LuaMemoryAccount>,
    table: &Table,
) -> Result<ChargedVec<(Value, Value)>, super::AdmissionError> {
    let mut pairs = ChargedVec::new(Arc::clone(memory));
    for pair in table.pairs::<Value, Value>() {
        pairs.try_push(pair?).map_err(capacity)?;
    }
    Ok(pairs)
}

fn take_child(frame: &mut Frame) -> Result<Child, mlua::Error> {
    match &mut frame.kind {
        Kind::Array { next, len } => {
            if *next > *len {
                return Ok(Child::End);
            }
            let item = frame.table.raw_get::<Value>(index_key(*next)?)?;
            *next += 1;
            Ok(child_from_value(item))
        }
        Kind::Object { pairs, next } => {
            if *next >= pairs.len() {
                return Ok(Child::End);
            }
            let (key, item) = pairs.get(*next).expect("pair cursor in range").clone();
            *next += 1;
            frame.pending_key = Some(key);
            Ok(child_from_value(item))
        }
    }
}

fn child_from_value(value: Value) -> Child {
    match value {
        Value::Table(table) => Child::Table(table),
        other => Child::Leaf(other),
    }
}

fn add_json(frame: &mut Frame, json: usize) -> Result<(), super::AdmissionError> {
    match &mut frame.kind {
        Kind::Array { .. } => {
            frame.json = frame
                .json
                .checked_add(json)
                .ok_or(super::AdmissionError::Capacity)?;
        }
        Kind::Object { .. } => {
            let key = frame
                .pending_key
                .take()
                .ok_or_else(|| mlua::Error::RuntimeError("object child missing key".into()))?;
            let key_len = map_key_len(&key)?;
            frame.json = frame
                .json
                .checked_add(key_len)
                .and_then(|bytes| bytes.checked_add(json))
                .ok_or(super::AdmissionError::Capacity)?;
            frame.count = frame
                .count
                .checked_add(1)
                .ok_or(super::AdmissionError::Capacity)?;
        }
    }
    Ok(())
}

fn add_build(frame: &mut Frame, json: serde_json::Value) -> Result<(), mlua::Error> {
    match &mut frame.kind {
        Kind::Array { .. } => {
            frame
                .build_array
                .as_mut()
                .expect("array build accumulator")
                .push(json);
        }
        Kind::Object { .. } => {
            let key = frame
                .pending_key
                .take()
                .ok_or_else(|| mlua::Error::RuntimeError("object child missing key".into()))?;
            frame
                .build_map
                .as_mut()
                .expect("object build accumulator")
                .insert(map_key_string(&key)?, json);
        }
    }
    Ok(())
}

fn finish_size(frame: &Frame) -> Result<usize, super::AdmissionError> {
    match &frame.kind {
        Kind::Array { len, .. } => {
            debug_assert_eq!(
                frame.build_array.as_ref().map(Vec::len).unwrap_or(*len),
                *len
            );
            Ok(frame.json)
        }
        Kind::Object { .. } => btree_nodes_checked::<String, serde_json::Value>(frame.count)
            .and_then(|nodes| nodes.checked_add(frame.json))
            .ok_or(super::AdmissionError::Capacity),
    }
}

fn finish_build(frame: Frame) -> Result<serde_json::Value, mlua::Error> {
    match frame.kind {
        Kind::Array { len, .. } => {
            let items = frame.build_array.expect("array build accumulator");
            debug_assert_eq!(items.len(), len);
            debug_assert_eq!(items.len(), items.capacity());
            Ok(serde_json::Value::Array(items))
        }
        Kind::Object { .. } => Ok(serde_json::Value::Object(
            frame.build_map.expect("object build accumulator"),
        )),
    }
}

fn is_array(table: &Table, array_mt: *const c_void) -> Result<bool, mlua::Error> {
    if table.raw_len() > 0 {
        return Ok(true);
    }
    Ok(table
        .metatable()
        .is_some_and(|mt| mt.to_pointer() == array_mt))
}

fn note_peak(memory: &Arc<LuaMemoryAccount>, before: usize, peak: &mut usize) {
    let live = memory.callback_used().saturating_sub(before);
    if live > *peak {
        *peak = live;
    }
}

fn capacity(_: LuaMemoryCapacityError) -> super::AdmissionError {
    super::AdmissionError::Capacity
}

fn build_capacity(_: super::AdmissionError) -> mlua::Error {
    mlua::Error::RuntimeError("Lua callback memory capacity exhausted".into())
}

fn leaf_size(value: &Value) -> Result<usize, super::AdmissionError> {
    match value {
        Value::Nil | Value::Boolean(_) | Value::Integer(_) | Value::Number(_) => Ok(0),
        Value::LightUserData(data) if data.0.is_null() => Ok(0),
        Value::String(text) => Ok(utf8_len(text)?),
        Value::Table(_) => Err(super::AdmissionError::Runtime(mlua::Error::RuntimeError(
            "table leaf reached the size walk".into(),
        ))),
        other => Err(unsupported(other)),
    }
}

fn leaf_build(value: &Value) -> Result<serde_json::Value, mlua::Error> {
    match value {
        Value::Nil => Ok(serde_json::Value::Null),
        Value::Boolean(flag) => Ok(serde_json::Value::Bool(*flag)),
        Value::Integer(integer) => Ok(serde_json::Value::Number((*integer).into())),
        Value::Number(number) => Ok(Number::from_f64(*number)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)),
        Value::LightUserData(data) if data.0.is_null() => Ok(serde_json::Value::Null),
        Value::String(text) => Ok(serde_json::Value::String(utf8_string(text)?)),
        Value::Table(_) => Err(mlua::Error::RuntimeError(
            "table leaf reached the build walk".into(),
        )),
        other => Err(mlua::Error::RuntimeError(unsupported_message(other))),
    }
}

fn index_key(index: usize) -> Result<i64, mlua::Error> {
    i64::try_from(index)
        .map_err(|_| mlua::Error::RuntimeError("coordination table is too large".into()))
}

fn utf8_len(text: &mlua::String) -> Result<usize, mlua::Error> {
    Ok(text
        .to_str()
        .map_err(|_| mlua::Error::RuntimeError("coordination requires UTF-8 strings".into()))?
        .len())
}

fn utf8_string(text: &mlua::String) -> Result<String, mlua::Error> {
    let utf8 = text
        .to_str()
        .map_err(|_| mlua::Error::RuntimeError("coordination requires UTF-8 strings".into()))?;
    let mut out = String::with_capacity(utf8.len());
    out.push_str(&utf8);
    Ok(out)
}

fn map_key_len(key: &Value) -> Result<usize, super::AdmissionError> {
    match key {
        Value::String(text) => Ok(utf8_len(text)?),
        _ => Err(super::AdmissionError::Runtime(mlua::Error::RuntimeError(
            "expected a string key".into(),
        ))),
    }
}

fn map_key_string(key: &Value) -> Result<String, mlua::Error> {
    match key {
        Value::String(text) => utf8_string(text),
        _ => Err(mlua::Error::RuntimeError("expected a string key".into())),
    }
}

fn unsupported(value: &Value) -> super::AdmissionError {
    super::AdmissionError::Runtime(mlua::Error::RuntimeError(unsupported_message(value)))
}

fn unsupported_message(value: &Value) -> String {
    let mut message = String::with_capacity(22 + value.type_name().len());
    message.push_str("unsupported value type `");
    message.push_str(value.type_name());
    message.push('`');
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_runtime::PendingCoordinationOperation;
    use botster_core::PluginKey;
    use mlua::{Lua, LuaSerdeExt, Value};

    fn eval(source: &str) -> (Lua, Value) {
        let lua = Lua::new();
        let value: Value = lua.load(source).eval().unwrap();
        (lua, value)
    }

    fn memory() -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(crate::lua_memory::LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .unwrap()
    }

    fn assert_equivalent(source: &str) {
        let (lua, value) = eval(source);
        let from = lua.from_value::<serde_json::Value>(value.clone());
        let account = memory();
        let built = value_build(&account, &lua, &value);
        match (from, built) {
            (Ok(from), Ok(built)) => {
                assert_eq!(built, from, "{source}");
                let admitted = value_size(&account, &lua, &value).unwrap();
                let retained = retained_bytes(&built).unwrap();
                assert!(
                    retained <= admitted.json_bytes,
                    "{source}: retained {retained} > json {}",
                    admitted.json_bytes
                );
            }
            (Err(_), Err(_)) => {}
            (from, built) => panic!("{source}: from_value={from:?} built={built:?}"),
        }
    }

    #[test]
    fn lua_from_value_equivalence_corpus() {
        for source in [
            "return {}",
            "return {a=1}",
            "return {a='x', b=true}",
            "return {1, 2, 3}",
            "return {1, extra='x'}",
            "return {[2]=true}",
            "return {nested={inner={leaf='z'}}}",
            "return {items={{id='a'},{id='b'}}}",
            "return {k1='v1',k2='v2',k3='v3',k4='v4',k5='v5'}",
        ] {
            assert_equivalent(source);
        }
    }

    #[test]
    fn empty_array_metatable_is_an_array() {
        let lua = Lua::new();
        lua.globals()
            .set("array_mt", lua.array_metatable())
            .unwrap();
        let value: Value = lua
            .load("return setmetatable({}, array_mt)")
            .eval()
            .unwrap();
        let from = lua.from_value::<serde_json::Value>(value.clone()).unwrap();
        let account = memory();
        let built = value_build(&account, &lua, &value).unwrap();
        assert_eq!(built, from);
        assert_eq!(built, serde_json::json!([]));
    }

    #[test]
    fn mixed_sequence_matches_from_value_array() {
        let (lua, value) = eval("return {1, 2, extra='x'}");
        let account = memory();
        let built = value_build(&account, &lua, &value).unwrap();
        let from = lua.from_value::<serde_json::Value>(value).unwrap();
        assert_eq!(built, from);
        assert_eq!(built, serde_json::json!([1, 2]));
    }

    #[test]
    fn invalid_utf8_string_is_rejected() {
        let (lua, value) = eval("return string.char(255)");
        let account = memory();
        assert!(matches!(
            value_size(&account, &lua, &value),
            Err(super::super::AdmissionError::Runtime(_))
        ));
        assert!(value_build(&account, &lua, &value).is_err());
    }

    #[test]
    fn deep_acyclic_table_matches_from_value() {
        let lua = Lua::new();
        let value: Value = lua
            .load(
                r#"
                local t = 'leaf'
                for _ = 1, 128 do
                    t = { n = t }
                end
                return t
                "#,
            )
            .eval()
            .unwrap();
        let from = lua.from_value::<serde_json::Value>(value.clone()).unwrap();
        let account = memory();
        let admitted = value_size(&account, &lua, &value).unwrap();
        let built = value_build(&account, &lua, &value).unwrap();
        assert_eq!(built, from);
        assert!(admitted.json_bytes > 0);
        assert!(retained_bytes(&built).unwrap() <= admitted.json_bytes);
    }

    #[test]
    fn recursive_table_matches_from_value() {
        let lua = Lua::new();
        let value: Value = lua.load("local t = {}; t.n = t; return t").eval().unwrap();
        assert!(lua.from_value::<serde_json::Value>(value.clone()).is_err());
        let account = memory();
        assert!(matches!(
            value_size(&account, &lua, &value),
            Err(super::super::AdmissionError::Runtime(_))
        ));
        assert!(value_build(&account, &lua, &value).is_err());
    }

    fn admit_publish(source: &str) -> crate::lua_runtime::PendingCoordinationOperation {
        let lua = Lua::new();
        let args: Value = lua.load(source).eval().unwrap();
        super::super::admit_publish_operation(&memory(), &PluginKey("pk".into()), &lua, args)
            .unwrap()
            .0
    }

    fn admit_drain(source: &str) -> crate::lua_runtime::PendingCoordinationOperation {
        let lua = Lua::new();
        let args: Value = lua.load(source).eval().unwrap();
        super::super::admit_drain_operation(&memory(), &lua, args)
            .unwrap()
            .0
    }

    #[test]
    fn publish_preserves_created_at_defaults_and_extension() {
        let PendingCoordinationOperation::Publish { envelope } = admit_publish(
            r#"
            return {
                id = 'e1',
                created_at = 9,
                body = 'hi',
                target = { type = 'topic', topic = 't' },
                extension = { k = 'v', items = {1, 2} }
            }
            "#,
        ) else {
            panic!("publish");
        };
        assert_eq!(envelope.id.0, "e1");
        assert_eq!(envelope.source.0, "plugin:pk");
        assert_eq!(envelope.created_at, 9);
        assert_eq!(envelope.payload.body, b"hi");
        assert_eq!(envelope.payload.content_type, "application/json");
        assert_eq!(
            envelope.payload.extension.as_ref().map(|value| &value.0),
            Some(&serde_json::json!({"k":"v","items":[1,2]}))
        );
    }

    #[test]
    fn publish_defaults_missing_and_invalid_fields() {
        let PendingCoordinationOperation::Publish { envelope } = admit_publish(
            r#"
            return {
                id = 'e1',
                created_at = 'nope',
                body = 12,
                content_type = 3,
                target = { type = 'topic', topic = 't' }
            }
            "#,
        ) else {
            panic!("publish");
        };
        assert_eq!(envelope.created_at, 0);
        assert!(envelope.payload.body.is_empty());
        assert_eq!(envelope.payload.content_type, "application/json");
        assert!(envelope.payload.extension.is_none());
    }

    #[test]
    fn drain_after_and_limit_match_as_u64() {
        let PendingCoordinationOperation::Drain { after, limit, .. } = admit_drain(
            r#"return { target = { type = 'topic', topic = 't' }, after = 0, limit = 0 }"#,
        ) else {
            panic!("drain");
        };
        assert_eq!(after.map(|cursor| cursor.0), Some(0));
        assert_eq!(limit, 0);

        let PendingCoordinationOperation::Drain { after, limit, .. } = admit_drain(
            r#"return { target = { type = 'topic', topic = 't' }, after = 'x', limit = 'x' }"#,
        ) else {
            panic!("drain");
        };
        assert!(after.is_none());
        assert_eq!(limit, 16);

        let PendingCoordinationOperation::Drain { after, limit, .. } =
            admit_drain(r#"return { target = { type = 'topic', topic = 't' } }"#)
        else {
            panic!("drain");
        };
        assert!(after.is_none());
        assert_eq!(limit, 16);
    }
}
