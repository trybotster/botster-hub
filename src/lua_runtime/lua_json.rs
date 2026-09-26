//! Iterative two-pass Lua → `serde_json::Value` conversion.
//!
//! # Live Lua `ValueRef` ownership (mlua 0.11.6)
//!
//! `lua_reference_bytes()` is `ArcInner<c_int>` (`value_ref.rs:25`), charged per
//! live handle. Object keys are Rust `String`s plus a cursor; values are fetched
//! with `raw_get(key)`. `pairs()` / `raw_get` are raw, so no Lua code runs
//! between the key-collection pass and later `raw_get` of the same unique
//! string key.
//!
//! Per frame, charged for the frame's lifetime:
//! - array: table + 1 fetched value + 1 transient `metatable()` probe = 3
//! - object: table + 1 fetched value + 1 `metatable()` probe = 3
//!   Key collection holds 2 extra transient pair refs for the scan, then drops
//!   them. A table-valued child reuses the fetched value as the child frame's
//!   table ref (not an extra width-proportional hold).
//!
//! Peak live refs ≈ `3 × depth + 2` (scan) + 1 walk-level `array_metatable`
//! handle. Old `from_value`: per level the table, `TablePairs` `key:
//! Option<Value>`, and the value (3), plus `RecursionGuard`'s `FxHashSet`
//! (Rust heap). Equal peaks are not required; both are O(depth).
//!
//! # `ref_free` (Jason-approved documented exclusion)
//!
//! mlua 0.11.6 `ExtraData.ref_free: Vec<c_int>` (`state/raw.rs:917` push on
//! drop; `state/extra.rs:268-292` pop on reuse) is private Rust storage whose
//! capacity is retained for the Lua state's life, including Vec doubling and
//! old+new realloc overlap. Jason approved leaving that retained capacity
//! outside the memory-accounting guarantee. It is not funded. Do not add
//! partial funding or a new limit to mask it. This conversion's live `ValueRef`
//! count is O(depth) (`3 × depth + 2` scan transients + 1 `array_metatable`
//! handle), which bounds the number of `ref_free` entries this walk adds. It
//! does not bound capacity growth, which depends on the existing capacity;
//! that growth is part of the approved exclusion.
//!
//! Still in scope: live XRc (`lua_reference_bytes()`), walk scratch, collection
//! capacity, and Lua allocator storage under the VM charge.
//!
//! # Ref-thread
//!
//! Registry/ref-thread slots go through the Lua allocator (VM charge). The
//! high-water mark is retained for the state's life.
//!
//! Size-pass scratch is charged before growth (`ChargedVec` overlap). Build
//! consumes a pre-split reservation and never `reserve_*` again.

use std::fmt::{self, Write};
use std::mem::size_of;
use std::os::raw::c_void;
use std::sync::Arc;

use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde_json::{Map, Number};

use crate::lua_memory::charged_collection::ChargedVec;
use crate::lua_memory::layout::{btree_nodes_checked, lua_reference_bytes};
use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount, LuaMemoryCapacityError};

#[derive(Debug)]
pub(crate) struct JsonAdmission {
    pub json_bytes: usize,
    pub scratch_peak: usize,
    pub stack_cap: usize,
    /// Peak live Lua refs for the oracle/tests. Kept in every build so admission
    /// layout stays identical when those readers are cfg'd out.
    #[cfg_attr(not(any(test, feature = "allocation-oracle")), allow(dead_code))]
    pub live_refs_peak: usize,
}

pub(crate) struct PrepaidScratch {
    _charge: Option<LuaCallbackCharge>,
    stack_cap: usize,
}

impl JsonAdmission {
    #[cfg(any(test, feature = "allocation-oracle"))]
    pub(crate) fn prepaid(
        &self,
        memory: &Arc<LuaMemoryAccount>,
    ) -> Result<PrepaidScratch, super::AdmissionError> {
        Ok(PrepaidScratch {
            _charge: if self.scratch_peak == 0 {
                None
            } else {
                Some(
                    memory
                        .reserve_callback_bytes(self.scratch_peak)
                        .map_err(capacity)?,
                )
            },
            stack_cap: self.stack_cap,
        })
    }

    /// Keep J+K on one open charge until build scratch has been destroyed.
    pub(crate) fn build_scoped(
        &self,
        lua: &Lua,
        value: &Value,
        entry: &mut LuaCallbackCharge,
    ) -> Result<serde_json::Value, mlua::Error> {
        // Keep both Ok and Err on this path so scratch drops before shrink.
        let built = match value {
            Value::Table(table) => walk_build(lua, table.clone(), self.stack_cap),
            other => leaf_build(other),
        };
        let retained = entry
            .bytes()
            .checked_sub(self.scratch_peak)
            .expect("the parent admitted J+K before JSON build");
        assert!(entry.shrink_to(retained));
        built
    }

    /// Transfer scratch and seal the entry when no later growth is allowed.
    pub(crate) fn bind_sealed(
        &self,
        entry: &mut LuaCallbackCharge,
    ) -> Result<PrepaidScratch, super::AdmissionError> {
        Ok(PrepaidScratch {
            _charge: if self.scratch_peak == 0 {
                None
            } else {
                Some(
                    entry
                        .split(self.scratch_peak)
                        .ok_or(super::AdmissionError::Capacity)?,
                )
            },
            stack_cap: self.stack_cap,
        })
    }
}

pub(crate) fn value_size(
    memory: &Arc<LuaMemoryAccount>,
    lua: &Lua,
    value: &Value,
) -> Result<JsonAdmission, super::AdmissionError> {
    value_size_scoped(memory, lua, value).map_err(|error| match error {
        super::AdmissionError::Json(problem) => {
            super::AdmissionError::Runtime(problem.legacy_error())
        }
        other => other,
    })
}

/// Preserve an allocation-free description of walker-owned errors for spawn.
pub(crate) fn value_size_scoped(
    memory: &Arc<LuaMemoryAccount>,
    lua: &Lua,
    value: &Value,
) -> Result<JsonAdmission, super::AdmissionError> {
    match value {
        Value::Table(table) => walk_size(memory, lua, table.clone()),
        other => Ok(JsonAdmission {
            json_bytes: leaf_size(other)?,
            scratch_peak: 0,
            stack_cap: 0,
            live_refs_peak: 0,
        }),
    }
}

pub(crate) fn value_build(
    lua: &Lua,
    value: &Value,
    prepaid: PrepaidScratch,
) -> Result<serde_json::Value, mlua::Error> {
    match value {
        Value::Table(table) => walk_build(lua, table.clone(), prepaid.stack_cap),
        other => leaf_build(other),
    }
}

#[cfg(test)]
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

/// table + 1 fetched value + `metatable()` probe.
const fn frame_ref_bytes() -> usize {
    3 * lua_reference_bytes()
}

/// Transient `pairs()` key+value during key collection.
const fn pair_scan_ref_bytes() -> usize {
    2 * lua_reference_bytes()
}

struct Scratch {
    stack_slots: usize,
    deferred_slots: usize,
    live_refs: usize,
    live_ref_count: usize,
    live_ref_count_peak: usize,
    peak: usize,
    stack_cap: usize,
}

impl Scratch {
    fn note(&mut self) {
        self.peak = self
            .peak
            .max(self.stack_slots + self.deferred_slots + self.live_refs);
        self.live_ref_count_peak = self.live_ref_count_peak.max(self.live_ref_count);
    }

    fn add_refs(&mut self, bytes: usize, count: usize) {
        self.live_refs += bytes;
        self.live_ref_count += count;
        self.note();
    }

    fn sub_refs(&mut self, bytes: usize, count: usize) {
        self.live_refs = self.live_refs.saturating_sub(bytes);
        self.live_ref_count = self.live_ref_count.saturating_sub(count);
        self.note();
    }

    fn grow_stack(&mut self, old_cap: usize, new_cap: usize) {
        let slot = size_of::<SizeFrame>();
        self.peak = self.peak.max(
            old_cap
                .saturating_mul(slot)
                .saturating_add(new_cap.saturating_mul(slot))
                + self.deferred_slots
                + self.live_refs,
        );
        self.stack_slots = new_cap.saturating_mul(slot);
        self.stack_cap = self.stack_cap.max(new_cap);
    }

    fn grow_keys(&mut self, rest: usize, old_cap: usize, new_cap: usize) {
        let slot = size_of::<CollectedKey>();
        self.peak = self.peak.max(
            self.stack_slots
                + rest
                + old_cap.saturating_mul(slot)
                + new_cap.saturating_mul(slot)
                + self.live_refs,
        );
        self.deferred_slots = rest + new_cap.saturating_mul(slot);
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum KeyFail {
    Integer(i64),
    Number(f64),
    Boolean(bool),
    Nil,
    InvalidUtf8,
    Seq,
    Map,
    Unsupported(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum JsonProblem {
    Deserialize(&'static str),
    Runtime(&'static str),
    Key(KeyFail),
    Unsupported(&'static str),
}

struct CountedMessage(usize);

impl Write for CountedMessage {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}

impl JsonProblem {
    fn write_inner(self, output: &mut impl Write) -> fmt::Result {
        match self {
            Self::Deserialize(message) | Self::Runtime(message) => output.write_str(message),
            Self::Key(fail) => write_key_fail(fail, output),
            Self::Unsupported(name) => write!(output, "unsupported value type `{name}`"),
        }
    }

    fn write_callback_message(self, output: &mut impl Write) -> fmt::Result {
        if !matches!(self, Self::Runtime(_)) {
            output.write_str("deserialize error: ")?;
        }
        self.write_inner(output)
    }

    pub(super) fn legacy_error(self) -> mlua::Error {
        let mut inner = String::new();
        self.write_inner(&mut inner)
            .expect("a JSON error descriptor must format");
        match self {
            Self::Runtime(_) => mlua::Error::RuntimeError(inner),
            _ => mlua::Error::DeserializeError(inner),
        }
    }

    pub(super) fn raise(
        self,
        lua: &Lua,
        mut parent: LuaCallbackCharge,
        capacity: &mlua::String,
    ) -> super::callback::CallbackFailure {
        let mut counted = CountedMessage(0);
        if self.write_callback_message(&mut counted).is_err() || parent.grow(counted.0).is_err() {
            return super::callback::CallbackFailure::Raise(capacity.clone());
        }
        let mut message = String::with_capacity(counted.0);
        if self.write_callback_message(&mut message).is_err() {
            return super::callback::CallbackFailure::Raise(capacity.clone());
        }
        let raised = lua.create_string(&message).ok();
        drop(message);
        drop(parent);
        super::callback::CallbackFailure::Raise(raised.unwrap_or_else(|| capacity.clone()))
    }
}

/// Keep the scoped callback parent alive through Rust error formatting.
pub(super) fn raise_admission_error(
    error: super::AdmissionError,
    lua: &Lua,
    mut parent: LuaCallbackCharge,
    capacity: &mlua::String,
) -> super::callback::CallbackFailure {
    match error {
        super::AdmissionError::Capacity => {
            super::callback::CallbackFailure::Raise(capacity.clone())
        }
        super::AdmissionError::Json(problem) => problem.raise(lua, parent, capacity),
        super::AdmissionError::Runtime(error) => {
            let write_message = |output: &mut dyn Write| match &error {
                mlua::Error::RuntimeError(message) => output.write_str(message),
                other => write!(output, "{other}"),
            };
            let mut counted = CountedMessage(0);
            if write_message(&mut counted).is_err() || parent.grow(counted.0).is_err() {
                return super::callback::CallbackFailure::Raise(capacity.clone());
            }
            let mut message = String::with_capacity(counted.0);
            if write_message(&mut message).is_err() {
                return super::callback::CallbackFailure::Raise(capacity.clone());
            }
            let raised = lua.create_string(&message).ok();
            drop(message);
            drop(error);
            drop(parent);
            super::callback::CallbackFailure::Raise(raised.unwrap_or_else(|| capacity.clone()))
        }
    }
}

fn write_key_fail(fail: KeyFail, output: &mut impl Write) -> fmt::Result {
    match fail {
        KeyFail::Integer(value) => {
            write!(
                output,
                "invalid type: integer `{value}`, expected a string key"
            )
        }
        KeyFail::Number(value) => {
            write!(
                output,
                "invalid type: floating point `{value}`, expected a string key"
            )
        }
        KeyFail::Boolean(value) => {
            write!(
                output,
                "invalid type: boolean `{value}`, expected a string key"
            )
        }
        KeyFail::Nil => output.write_str("invalid type: unit value, expected a string key"),
        KeyFail::InvalidUtf8 => output.write_str("invalid type: byte array, expected a string key"),
        KeyFail::Seq => output.write_str("invalid type: sequence, expected a string key"),
        KeyFail::Map => output.write_str("invalid type: map, expected a string key"),
        KeyFail::Unsupported(name) => write!(output, "unsupported value type `{name}`"),
    }
}

enum CollectedKey {
    Utf8 {
        text: String,
        _charge: Option<LuaCallbackCharge>,
    },
    Fail(KeyFail),
}

struct SizeFrame {
    table: Table,
    ptr: *const c_void,
    refs: LuaCallbackCharge,
    kind: SizeKind,
    pending_key_len: Option<usize>,
    json: usize,
    count: usize,
    key_slots: usize,
    key_string_bytes: usize,
}

enum SizeKind {
    Array {
        next: usize,
        len: usize,
    },
    Object {
        keys: ChargedVec<CollectedKey>,
        next: usize,
        scanned: bool,
    },
}

struct BuildFrame {
    table: Table,
    ptr: *const c_void,
    kind: BuildKind,
    pending_key: Option<String>,
    array: Option<Vec<serde_json::Value>>,
    map: Option<Map<String, serde_json::Value>>,
}

enum BuildKind {
    Array {
        next: usize,
        len: usize,
    },
    Object {
        keys: Vec<CollectedKey>,
        next: usize,
        scanned: bool,
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
    let mut scratch = Scratch {
        stack_slots: 0,
        deferred_slots: 0,
        live_refs: 0,
        live_ref_count: 0,
        live_ref_count_peak: 0,
        peak: 0,
        stack_cap: 0,
    };
    let mt_refs = memory
        .reserve_callback_bytes(lua_reference_bytes())
        .map_err(capacity)?;
    scratch.add_refs(lua_reference_bytes(), 1);
    let array_mt = lua.array_metatable().to_pointer();
    let mut stack = ChargedVec::new(Arc::clone(memory));
    push_size_frame(&mut stack, memory, table, array_mt, &mut scratch)?;
    loop {
        let child = {
            let Some(top) = stack.last_mut() else {
                break;
            };
            take_size_child(top, memory, &mut scratch)?
        };
        match child {
            Child::Table(child) => {
                if stack.iter().any(|frame| frame.ptr == child.to_pointer()) {
                    return Err(super::AdmissionError::Json(JsonProblem::Deserialize(
                        "recursive table detected",
                    )));
                }
                push_size_frame(&mut stack, memory, child, array_mt, &mut scratch)?;
            }
            Child::Leaf(value) => {
                let json = leaf_size(&value)?;
                let top = stack.last_mut().expect("leaf under a frame");
                add_size(top, json)?;
            }
            Child::End => {
                let done = stack.pop().expect("end of a frame");
                let json = finish_size(&done)?;
                scratch.live_refs = scratch
                    .live_refs
                    .saturating_sub(done.refs.bytes() + done.key_string_bytes);
                scratch.live_ref_count = scratch.live_ref_count.saturating_sub(3);
                scratch.deferred_slots = scratch.deferred_slots.saturating_sub(done.key_slots);
                drop(done);
                scratch.note();
                if stack.len() == 0 {
                    drop(mt_refs);
                    return Ok(JsonAdmission {
                        json_bytes: json,
                        scratch_peak: scratch.peak,
                        stack_cap: scratch.stack_cap.max(1),
                        live_refs_peak: scratch.live_ref_count_peak,
                    });
                }
                let parent = stack.last_mut().expect("nested table has a parent");
                add_size(parent, json)?;
            }
        }
    }
    Err(super::AdmissionError::Json(JsonProblem::Runtime(
        "json walk ended without a root",
    )))
}

fn walk_build(lua: &Lua, table: Table, stack_cap: usize) -> Result<serde_json::Value, mlua::Error> {
    let array_mt = lua.array_metatable().to_pointer();
    let mut stack = Vec::with_capacity(stack_cap.max(1));
    push_build_frame(&mut stack, table, array_mt)?;
    loop {
        let child = {
            let Some(top) = stack.last_mut() else {
                break;
            };
            take_build_child(top)?
        };
        match child {
            Child::Table(child) => {
                if stack.iter().any(|frame| frame.ptr == child.to_pointer()) {
                    return Err(mlua::Error::DeserializeError(
                        "recursive table detected".into(),
                    ));
                }
                debug_assert!(stack.len() < stack.capacity());
                push_build_frame(&mut stack, child, array_mt)?;
            }
            Child::Leaf(value) => {
                let json = leaf_build(&value)?;
                let top = stack.last_mut().expect("leaf under a frame");
                add_build(top, json)?;
            }
            Child::End => {
                let done = stack.pop().expect("end of a frame");
                let json = finish_build(done)?;
                if stack.is_empty() {
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

fn push_size_frame(
    stack: &mut ChargedVec<SizeFrame>,
    memory: &Arc<LuaMemoryAccount>,
    table: Table,
    array_mt: *const c_void,
    scratch: &mut Scratch,
) -> Result<(), super::AdmissionError> {
    let ptr = table.to_pointer();
    let array = is_array(&table, array_mt).map_err(super::AdmissionError::Runtime)?;
    let (kind, refs, json) = if array {
        let len = table.raw_len();
        if len > i64::MAX as usize {
            return Err(super::AdmissionError::Capacity);
        }
        (
            SizeKind::Array { next: 1, len },
            memory
                .reserve_callback_bytes(frame_ref_bytes())
                .map_err(capacity)?,
            len.checked_mul(size_of::<serde_json::Value>())
                .ok_or(super::AdmissionError::Capacity)?,
        )
    } else {
        (
            SizeKind::Object {
                keys: ChargedVec::new(Arc::clone(memory)),
                next: 0,
                scanned: false,
            },
            memory
                .reserve_callback_bytes(frame_ref_bytes())
                .map_err(capacity)?,
            0,
        )
    };
    scratch.add_refs(refs.bytes(), 3);
    let old_cap = stack.capacity();
    stack
        .try_push(SizeFrame {
            table,
            ptr,
            refs,
            kind,
            pending_key_len: None,
            json,
            count: 0,
            key_slots: 0,
            key_string_bytes: 0,
        })
        .map_err(capacity)?;
    let new_cap = stack.capacity();
    if new_cap > old_cap {
        scratch.grow_stack(old_cap, new_cap);
    } else {
        scratch.note();
    }
    Ok(())
}

fn push_build_frame(
    stack: &mut Vec<BuildFrame>,
    table: Table,
    array_mt: *const c_void,
) -> Result<(), mlua::Error> {
    let ptr = table.to_pointer();
    let (kind, array, map, json_len) = if is_array(&table, array_mt)? {
        let len = table.raw_len();
        (
            BuildKind::Array { next: 1, len },
            Some(Vec::with_capacity(len)),
            None,
            len,
        )
    } else {
        (
            BuildKind::Object {
                keys: Vec::new(),
                next: 0,
                scanned: false,
            },
            None,
            Some(Map::new()),
            0,
        )
    };
    let _ = json_len;
    stack.push(BuildFrame {
        table,
        ptr,
        kind,
        pending_key: None,
        array,
        map,
    });
    Ok(())
}

fn take_size_child(
    frame: &mut SizeFrame,
    memory: &Arc<LuaMemoryAccount>,
    scratch: &mut Scratch,
) -> Result<Child, super::AdmissionError> {
    // Pinned mlua raw_get::<Value> cannot return user error text: raw_get
    // invokes no metamethod and Value::from_lua is infallible. Recheck this
    // boundary before changing the Value type argument.
    if matches!(&frame.kind, SizeKind::Object { scanned: false, .. }) {
        scan_size_keys(frame, memory, scratch)?;
        if let SizeKind::Object { scanned, .. } = &mut frame.kind {
            *scanned = true;
        }
    }
    match &mut frame.kind {
        SizeKind::Array { next, len } => {
            if *next > *len {
                return Ok(Child::End);
            }
            let item = frame
                .table
                .raw_get::<Value>(index_key(*next))
                .map_err(super::AdmissionError::Runtime)?;
            *next += 1;
            Ok(child_from_value(item))
        }
        SizeKind::Object { keys, next, .. } => {
            if *next >= keys.len() {
                return Ok(Child::End);
            }
            match keys.get(*next).expect("key cursor") {
                CollectedKey::Fail(fail) => {
                    Err(super::AdmissionError::Json(JsonProblem::Key(*fail)))
                }
                CollectedKey::Utf8 { text, .. } => {
                    let item = frame
                        .table
                        .raw_get::<Value>(text.as_str())
                        .map_err(super::AdmissionError::Runtime)?;
                    frame.pending_key_len = Some(text.len());
                    *next += 1;
                    Ok(child_from_value(item))
                }
            }
        }
    }
}

fn scan_size_keys(
    frame: &mut SizeFrame,
    memory: &Arc<LuaMemoryAccount>,
    scratch: &mut Scratch,
) -> Result<(), super::AdmissionError> {
    let pair_refs = memory
        .reserve_callback_bytes(pair_scan_ref_bytes())
        .map_err(capacity)?;
    let table = frame.table.clone();
    scratch.add_refs(pair_scan_ref_bytes(), 2);
    for_each_admitted::<Value, Value>(&table, |key, _item| {
        let collected = collect_key(memory, &key)?;
        if let CollectedKey::Utf8 { text, .. } = &collected {
            let bytes = text.capacity();
            frame.key_string_bytes = frame
                .key_string_bytes
                .checked_add(bytes)
                .ok_or(super::AdmissionError::Capacity)?;
            scratch.live_refs += bytes;
        }
        let (old_cap, new_cap) = {
            let SizeKind::Object { keys, .. } = &mut frame.kind else {
                unreachable!("object scan");
            };
            let old_cap = keys.capacity();
            keys.try_push(collected).map_err(capacity)?;
            (old_cap, keys.capacity())
        };
        if new_cap > old_cap {
            let rest = scratch.deferred_slots.saturating_sub(frame.key_slots);
            scratch.grow_keys(rest, old_cap, new_cap);
            frame.key_slots = new_cap.saturating_mul(size_of::<CollectedKey>());
        }
        scratch.note();
        Ok(())
    })?;
    drop(pair_refs);
    scratch.sub_refs(pair_scan_ref_bytes(), 2);
    Ok(())
}

fn for_each_admitted<K: mlua::FromLua, V: mlua::FromLua>(
    table: &Table,
    mut function: impl FnMut(K, V) -> Result<(), super::AdmissionError>,
) -> Result<(), super::AdmissionError> {
    // The production caller uses Value, Value. Other FromLua types can add
    // error text, so this generic helper must not imply that their errors fit.
    let mut failure = None;
    let result = table.for_each::<K, V>(|key, value| {
        match function(key, value) {
            Ok(()) => Ok(()),
            Err(error) => {
                failure = Some(error);
                // Stop iteration without allocating. Return the original error below.
                Err(mlua::Error::StackError)
            }
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    result.map_err(super::AdmissionError::Runtime)?;
    Ok(())
}

fn take_build_child(frame: &mut BuildFrame) -> Result<Child, mlua::Error> {
    if matches!(&frame.kind, BuildKind::Object { scanned: false, .. }) {
        scan_build_keys(frame)?;
        if let BuildKind::Object { scanned, .. } = &mut frame.kind {
            *scanned = true;
        }
    }
    match &mut frame.kind {
        BuildKind::Array { next, len } => {
            if *next > *len {
                return Ok(Child::End);
            }
            let item = frame.table.raw_get::<Value>(index_key(*next))?;
            *next += 1;
            Ok(child_from_value(item))
        }
        BuildKind::Object { keys, next, .. } => {
            if *next >= keys.len() {
                return Ok(Child::End);
            }
            let collected = std::mem::replace(&mut keys[*next], CollectedKey::Fail(KeyFail::Nil));
            *next += 1;
            match collected {
                CollectedKey::Fail(fail) => Err(raise_key_fail(fail)),
                CollectedKey::Utf8 { text, .. } => {
                    let item = frame.table.raw_get::<Value>(text.as_str())?;
                    frame.pending_key = Some(text);
                    Ok(child_from_value(item))
                }
            }
        }
    }
}

fn scan_build_keys(frame: &mut BuildFrame) -> Result<(), mlua::Error> {
    let table = frame.table.clone();
    let mut count = 0usize;
    table.for_each::<Value, Value>(|_key, _item| {
        count += 1;
        Ok(())
    })?;
    let BuildKind::Object { keys, .. } = &mut frame.kind else {
        unreachable!("object scan");
    };
    keys.reserve_exact(count);
    table.for_each::<Value, Value>(|key, _item| {
        keys.push(collect_key_unfunded(&key)?);
        Ok(())
    })?;
    Ok(())
}

fn child_from_value(value: Value) -> Child {
    match value {
        Value::Table(table) => Child::Table(table),
        other => Child::Leaf(other),
    }
}

fn add_size(frame: &mut SizeFrame, json: usize) -> Result<(), super::AdmissionError> {
    match &frame.kind {
        SizeKind::Array { .. } => {
            frame.json = frame
                .json
                .checked_add(json)
                .ok_or(super::AdmissionError::Capacity)?;
        }
        SizeKind::Object { .. } => {
            let key_len = frame
                .pending_key_len
                .take()
                .ok_or(super::AdmissionError::Json(JsonProblem::Deserialize(
                    "object child missing key",
                )))?;
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

fn add_build(frame: &mut BuildFrame, json: serde_json::Value) -> Result<(), mlua::Error> {
    match &frame.kind {
        BuildKind::Array { .. } => {
            frame
                .array
                .as_mut()
                .expect("array build accumulator")
                .push(json);
        }
        BuildKind::Object { .. } => {
            let key = frame
                .pending_key
                .take()
                .ok_or_else(|| mlua::Error::DeserializeError("object child missing key".into()))?;
            frame
                .map
                .as_mut()
                .expect("object build accumulator")
                .insert(key, json);
        }
    }
    Ok(())
}

fn finish_size(frame: &SizeFrame) -> Result<usize, super::AdmissionError> {
    match &frame.kind {
        SizeKind::Array { .. } => Ok(frame.json),
        SizeKind::Object { .. } => btree_nodes_checked::<String, serde_json::Value>(frame.count)
            .and_then(|nodes| nodes.checked_add(frame.json))
            .ok_or(super::AdmissionError::Capacity),
    }
}

fn finish_build(frame: BuildFrame) -> Result<serde_json::Value, mlua::Error> {
    match frame.kind {
        BuildKind::Array { len, .. } => {
            let items = frame.array.expect("array build accumulator");
            debug_assert_eq!(items.len(), len);
            debug_assert_eq!(items.len(), items.capacity());
            Ok(serde_json::Value::Array(items))
        }
        BuildKind::Object { .. } => Ok(serde_json::Value::Object(
            frame.map.expect("object build accumulator"),
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

fn capacity(_: LuaMemoryCapacityError) -> super::AdmissionError {
    super::AdmissionError::Capacity
}

fn collect_key(
    memory: &Arc<LuaMemoryAccount>,
    key: &Value,
) -> Result<CollectedKey, super::AdmissionError> {
    match key {
        Value::String(text) => match text.to_str() {
            Ok(utf8) => {
                let charge = memory
                    .reserve_callback_bytes(utf8.len())
                    .map_err(capacity)?;
                Ok(copy_key(&utf8, Some(charge)))
            }
            Err(_) => Ok(CollectedKey::Fail(KeyFail::InvalidUtf8)),
        },
        other => {
            Ok(collect_key_unfunded(other).expect("non-string key classification cannot fail"))
        }
    }
}

fn collect_key_unfunded(key: &Value) -> Result<CollectedKey, mlua::Error> {
    match key {
        Value::String(text) => match text.to_str() {
            Ok(utf8) => Ok(copy_key(&utf8, None)),
            Err(_) => Ok(CollectedKey::Fail(KeyFail::InvalidUtf8)),
        },
        Value::Integer(value) => Ok(CollectedKey::Fail(KeyFail::Integer(*value))),
        Value::Number(value) => Ok(CollectedKey::Fail(KeyFail::Number(*value))),
        Value::Boolean(value) => Ok(CollectedKey::Fail(KeyFail::Boolean(*value))),
        Value::Nil => Ok(CollectedKey::Fail(KeyFail::Nil)),
        Value::Table(table) => Ok(CollectedKey::Fail(if table.raw_len() > 0 {
            KeyFail::Seq
        } else {
            KeyFail::Map
        })),
        other => Ok(CollectedKey::Fail(KeyFail::Unsupported(other.type_name()))),
    }
}

fn copy_key(utf8: &str, charge: Option<LuaCallbackCharge>) -> CollectedKey {
    let mut output = String::with_capacity(utf8.len());
    output.push_str(utf8);
    CollectedKey::Utf8 {
        text: output,
        _charge: charge,
    }
}

fn raise_key_fail(fail: KeyFail) -> mlua::Error {
    JsonProblem::Key(fail).legacy_error()
}

fn leaf_size(value: &Value) -> Result<usize, super::AdmissionError> {
    match value {
        Value::Nil | Value::Boolean(_) | Value::Integer(_) | Value::Number(_) => Ok(0),
        Value::LightUserData(data) if data.0.is_null() => Ok(0),
        Value::String(text) => text.to_str().map(|text| text.len()).map_err(|_| {
            super::AdmissionError::Json(JsonProblem::Deserialize(
                "invalid type: byte array, expected any valid JSON value",
            ))
        }),
        Value::Table(_) => Err(super::AdmissionError::Json(JsonProblem::Deserialize(
            "table leaf reached the size walk",
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
        Value::Table(_) => Err(mlua::Error::DeserializeError(
            "table leaf reached the build walk".into(),
        )),
        other => Err(mlua::Error::DeserializeError(unsupported_message(other))),
    }
}

fn index_key(index: usize) -> i64 {
    i64::try_from(index).expect("array raw_len already rejected lengths above i64::MAX")
}

fn utf8_string(text: &mlua::String) -> Result<String, mlua::Error> {
    let utf8 = text.to_str().map_err(|_| {
        mlua::Error::DeserializeError(
            "invalid type: byte array, expected any valid JSON value".into(),
        )
    })?;
    let mut out = String::with_capacity(utf8.len());
    out.push_str(&utf8);
    Ok(out)
}

fn unsupported(value: &Value) -> super::AdmissionError {
    super::AdmissionError::Json(JsonProblem::Unsupported(value.type_name()))
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
    use mlua::{Lua, LuaSerdeExt, UserData, Value};

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

    fn convert(
        lua: &Lua,
        value: &Value,
    ) -> Result<(JsonAdmission, serde_json::Value), (String, String)> {
        let account = memory();
        let admission = value_size(&account, lua, value).map_err(|error| {
            (
                lua.from_value::<serde_json::Value>(value.clone())
                    .err()
                    .map(|error| error.to_string())
                    .unwrap_or_default(),
                format!("{error:?}"),
            )
        })?;
        let prepaid = admission
            .prepaid(&account)
            .map_err(|error| (String::new(), format!("{error:?}")))?;
        match value_build(lua, value, prepaid) {
            Ok(built) => Ok((admission, built)),
            Err(error) => Err((
                lua.from_value::<serde_json::Value>(value.clone())
                    .err()
                    .map(|error| error.to_string())
                    .unwrap_or_default(),
                error.to_string(),
            )),
        }
    }

    fn assert_equivalent(source: &str) {
        let (lua, value) = eval(source);
        let from = lua.from_value::<serde_json::Value>(value.clone());
        match (from, convert(&lua, &value)) {
            (Ok(from), Ok((admission, built))) => {
                assert_eq!(built, from, "{source}");
                let retained = retained_bytes(&built).unwrap();
                assert!(
                    retained <= admission.json_bytes,
                    "{source}: retained {retained} > json {}",
                    admission.json_bytes
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
            "return {1, 2, nil, 4}",
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
        let built = convert(&lua, &value).unwrap().1;
        assert_eq!(built, from);
        assert_eq!(built, serde_json::json!([]));
    }

    #[test]
    fn mixed_sequence_matches_from_value_array() {
        let (lua, value) = eval("return {1, 2, extra='x'}");
        let built = convert(&lua, &value).unwrap().1;
        let from = lua.from_value::<serde_json::Value>(value).unwrap();
        assert_eq!(built, from);
        assert_eq!(built, serde_json::json!([1, 2]));
    }

    #[test]
    fn array_nil_hole_inside_raw_len_matches_from_value() {
        let (lua, value) = eval("return {1, 2, nil, 4}");
        let from = lua.from_value::<serde_json::Value>(value.clone()).unwrap();
        let built = convert(&lua, &value).unwrap().1;
        assert_eq!(built, from);
        assert_eq!(built, serde_json::json!([1, 2, null, 4]));
    }

    fn assert_same_error(lua: &Lua, value: &Value) {
        let from = lua
            .from_value::<serde_json::Value>(value.clone())
            .unwrap_err()
            .to_string();
        let ours = display_size_err(lua, value);
        assert_eq!(ours, from);
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
        let (admission, built) = convert(&lua, &value).unwrap();
        assert_eq!(built, from);
        assert!(admission.json_bytes > 0);
        assert!(retained_bytes(&built).unwrap() <= admission.json_bytes);
    }

    #[test]
    fn recursive_table_matches_from_value() {
        let lua = Lua::new();
        let value: Value = lua.load("local t = {}; t.n = t; return t").eval().unwrap();
        let from = lua
            .from_value::<serde_json::Value>(value.clone())
            .unwrap_err()
            .to_string();
        let ours = match value_size(&memory(), &lua, &value) {
            Err(super::super::AdmissionError::Runtime(error)) => error.to_string(),
            other => panic!("{other:?}"),
        };
        assert_eq!(ours, from);
    }

    fn display_size_err(lua: &Lua, value: &Value) -> String {
        match value_size(&memory(), lua, value) {
            Err(super::super::AdmissionError::Runtime(error)) => error.to_string(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn scoped_size_error_raises_exact_text_and_releases_its_charge() {
        for source in [
            "return { fn = function() end }",
            "local t = {}; t.n = t; return t",
            "return {[2]=true}",
            "return {[1.5]=true}",
            "return { text = string.char(255) }",
        ] {
            let (lua, value) = eval(source);
            let expected = display_size_err(&lua, &value);
            let account = memory();
            let parent = account.reserve_callback_total(0).unwrap();
            let error = value_size_scoped(&account, &lua, &value).unwrap_err();
            let capacity = lua.create_string("capacity").unwrap();
            let super::super::callback::CallbackFailure::Raise(message) =
                raise_admission_error(error, &lua, parent, &capacity)
            else {
                panic!("the scoped error must use the Lua Raise path");
            };
            assert_eq!(message.to_str().unwrap(), expected, "{source}");
            assert_eq!(account.usage().1, 0, "{source}");
        }
    }

    #[test]
    fn error_texts_match_from_value_byte_for_byte() {
        for source in [
            "return { fn = function() end }",
            "local t = {}; t.n = t; return t",
            "return {[2]=true}",
            "return {[1.5]=true}",
            "return {[true]=true}",
            "return {[{}]=true}",
            "return {[{1}]=true}",
            "return {a = string.char(255)}",
            "return {[string.char(255)] = 1}",
        ] {
            let (lua, value) = eval(source);
            assert_same_error(&lua, &value);
        }
        struct Marker;
        impl UserData for Marker {}
        let lua = Lua::new();
        let value = Value::UserData(lua.create_userdata(Marker).unwrap());
        assert_same_error(&lua, &value);
    }

    #[test]
    fn first_lua_next_error_wins() {
        for source in [
            "local t = {}; t[true] = 1; t.fn = function() end; return t",
            "local t = {}; t.fn = function() end; t[true] = 1; return t",
            "local t = {}; t.nested = { inner = function() end }; t.fn = function() end; return t",
        ] {
            let (lua, value) = eval(source);
            assert_same_error(&lua, &value);
        }
        let (lua, value) = eval(
            "local t = {}; t.ok = { inner = function() end }; t.bad = function() end; return t",
        );
        assert_same_error(&lua, &value);
    }

    #[test]
    fn wide_object_live_refs_do_not_grow_with_width() {
        let account = memory();
        let mut peaks = Vec::new();
        for width in [8usize, 32, 64] {
            let lua = Lua::new();
            lua.globals().set("width", width as i32).unwrap();
            let value: Value = lua
                .load(
                    r#"
                    local t = {}
                    for i = 1, width do
                        t['k' .. i] = 'v' .. i
                    end
                    return t
                    "#,
                )
                .eval()
                .unwrap();
            let admission = value_size(&account, &lua, &value).unwrap();
            peaks.push(admission.live_refs_peak);
        }
        assert_eq!(peaks[0], peaks[1]);
        assert_eq!(peaks[1], peaks[2]);
    }

    #[test]
    fn wide_object_of_tables_live_refs_do_not_grow_with_width() {
        let account = memory();
        let mut peaks = Vec::new();
        for width in [8usize, 32, 64] {
            let lua = Lua::new();
            lua.globals().set("width", width as i32).unwrap();
            let value: Value = lua
                .load(
                    r#"
                    local t = {}
                    for i = 1, width do
                        t['k' .. i] = {}
                    end
                    return t
                    "#,
                )
                .eval()
                .unwrap();
            let admission = value_size(&account, &lua, &value).unwrap();
            peaks.push(admission.live_refs_peak);
        }
        assert_eq!(peaks[0], peaks[1]);
        assert_eq!(peaks[1], peaks[2]);
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

    fn unused_field_cases() -> [&'static str; 3] {
        [
            r#"return { id = 'e1', target = { type = 'topic', topic = 't' }, unused = function() end }"#,
            r#"local u = {}; u.n = u; return { id = 'e1', target = { type = 'topic', topic = 't' }, unused = u }"#,
            r#"return { id = 'e1', target = { type = 'topic', topic = 't' }, unused = string.char(255) }"#,
        ]
    }

    #[test]
    fn publish_unused_fields_match_from_value() {
        for source in unused_field_cases() {
            let lua = Lua::new();
            let args: Value = lua.load(source).eval().unwrap();
            let from = lua
                .from_value::<serde_json::Value>(args.clone())
                .unwrap_err()
                .to_string();
            let ours = match super::super::admit_publish_operation(
                &memory(),
                &PluginKey("pk".into()),
                &lua,
                args,
            ) {
                Err(super::super::AdmissionError::Runtime(error)) => error.to_string(),
                other => panic!("{source}: expected runtime error, got success or capacity"),
            };
            assert_eq!(ours, from, "{source}");
        }
    }

    #[test]
    fn drain_unused_fields_match_from_value() {
        for source in [
            r#"return { target = { type = 'topic', topic = 't' }, unused = function() end }"#,
            r#"local u = {}; u.n = u; return { target = { type = 'topic', topic = 't' }, unused = u }"#,
            r#"return { target = { type = 'topic', topic = 't' }, unused = string.char(255) }"#,
        ] {
            let lua = Lua::new();
            let args: Value = lua.load(source).eval().unwrap();
            let from = lua
                .from_value::<serde_json::Value>(args.clone())
                .unwrap_err()
                .to_string();
            let ours = match super::super::admit_drain_operation(&memory(), &lua, args) {
                Err(super::super::AdmissionError::Runtime(error)) => error.to_string(),
                other => panic!("{source}: expected runtime error, got success or capacity"),
            };
            assert_eq!(ours, from, "{source}");
        }
    }
    #[test]
    fn admitted_iteration_preserves_errors_before_and_inside_the_callback() {
        struct RefusedKey;
        impl mlua::FromLua for RefusedKey {
            fn from_lua(_: Value, _: &Lua) -> mlua::Result<Self> {
                Err(mlua::Error::StackError)
            }
        }
        let lua = Lua::new();
        let table: Table = lua.load("return {first = 1, second = 2}").eval().unwrap();
        let mut calls = 0;
        let result = for_each_admitted::<RefusedKey, Value>(&table, |_, _| {
            calls += 1;
            Ok(())
        });
        assert_eq!(calls, 0);
        assert!(matches!(
            result,
            Err(super::super::AdmissionError::Runtime(
                mlua::Error::StackError
            ))
        ));

        let result = for_each_admitted::<Value, Value>(&table, |_, _| {
            calls += 1;
            Err(super::super::AdmissionError::Capacity)
        });
        assert_eq!(calls, 1, "capacity refusal must stop iteration immediately");
        assert!(matches!(
            result,
            Err(super::super::AdmissionError::Capacity)
        ));

        let result = for_each_admitted::<Value, Value>(&table, |_, _| {
            Err(super::super::AdmissionError::Runtime(
                mlua::Error::RuntimeError("original callback failure".into()),
            ))
        });
        let Err(super::super::AdmissionError::Runtime(error)) = result else {
            panic!("the original runtime error must survive iteration");
        };
        assert!(
            matches!(error, mlua::Error::RuntimeError(ref message) if message == "original callback failure")
        );
    }
}
