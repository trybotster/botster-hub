//! Runtime basics every plugin receives with no grant: `botster.json` and
//! `botster.clock`. They run inside the VM, never suspend, and return the one
//! result shape (`result.rs`).

use std::fmt;
use std::io;
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use super::result::{self, ErrorKind};
use super::{AdmissionError, lua_json};
use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount};

const ENCODE_USAGE: &str = "json.encode takes { value = <any>, arrays = nil | \"empty\" }";
const DECODE_USAGE: &str = "json.decode takes { text = <string> }";

#[cfg(test)]
thread_local! {
    /// The callback account's usage while the built JSON value exists.
    static ENCODE_BUILT_USAGE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

pub(super) fn install(
    lua: &Lua,
    botster: &Table,
    memory: Arc<LuaMemoryAccount>,
) -> mlua::Result<()> {
    botster.set("json", json_table(lua, memory)?)?;
    botster.set("clock", clock_table(lua)?)?;
    Ok(())
}

fn json_table(lua: &Lua, memory: Arc<LuaMemoryAccount>) -> mlua::Result<Table> {
    let json = lua.create_table()?;
    let decode_memory = Arc::clone(&memory);
    json.set(
        "encode",
        lua.create_function(move |lua, args: Value| {
            let Value::Table(args) = args else {
                return result::err(lua, ErrorKind::InvalidRequest, ENCODE_USAGE);
            };
            let value: Value = args.raw_get("value")?;
            let empty_as_array = match args.raw_get::<Value>("arrays")? {
                Value::Nil => false,
                Value::String(mode) if mode.as_bytes().as_ref() == b"empty" => true,
                _ => return result::err(lua, ErrorKind::InvalidRequest, ENCODE_USAGE),
            };
            match encode(lua, &memory, &value, empty_as_array) {
                // The charge funds the bytes until the Lua string owns a copy.
                Ok((text, _charge)) => result::ok(lua, Value::String(lua.create_string(&text)?)),
                Err((kind, message)) => result::err(lua, kind, &message),
            }
        })?,
    )?;
    json.set(
        "decode",
        lua.create_function(move |lua, args: Value| {
            let Value::Table(args) = args else {
                return result::err(lua, ErrorKind::InvalidRequest, DECODE_USAGE);
            };
            let Value::String(text) = args.raw_get::<Value>("text")? else {
                return result::err(lua, ErrorKind::InvalidRequest, DECODE_USAGE);
            };
            let bytes = text.as_bytes();
            // The parser's unescape scratch never exceeds the input length, so
            // funding that length up front bounds every Rust allocation here.
            let Ok(_scratch) = decode_memory.reserve_callback_bytes(bytes.len()) else {
                return result::err(
                    lua,
                    ErrorKind::QuotaExceeded,
                    "the Lua callback memory capacity is exhausted",
                );
            };
            let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
            let decoded = LuaSeed(lua)
                .deserialize(&mut deserializer)
                .and_then(|value| deserializer.end().map(|()| value));
            match decoded {
                Ok(value) => result::ok(lua, value),
                Err(error) => result::err(lua, ErrorKind::InvalidRequest, &error.to_string()),
            }
        })?,
    )?;
    json.set("null", lua.null())?;
    Ok(json)
}

/// Encode under the callback account: the walker measures the value and its
/// scratch before building, and the output grows its charge before each
/// allocation, so nothing is allocated unfunded.
fn encode(
    lua: &Lua,
    memory: &Arc<LuaMemoryAccount>,
    value: &Value,
    empty_as_array: bool,
) -> Result<(Vec<u8>, LuaCallbackCharge), (ErrorKind, String)> {
    let capacity = || {
        (
            ErrorKind::QuotaExceeded,
            "the Lua callback memory capacity is exhausted".to_string(),
        )
    };
    let mut charge = memory.reserve_callback_total(0).map_err(|_| capacity())?;
    let admission =
        lua_json::value_size_scoped(memory, lua, value).map_err(|error| match error {
            AdmissionError::Capacity => capacity(),
            AdmissionError::Runtime(error) => (ErrorKind::InvalidRequest, error.to_string()),
            AdmissionError::Json(problem) => (
                ErrorKind::InvalidRequest,
                problem.legacy_error().to_string(),
            ),
        })?;
    let json_and_scratch = admission
        .json_bytes
        .checked_add(admission.scratch_peak)
        .ok_or_else(capacity)?;
    charge.grow(json_and_scratch).map_err(|_| capacity())?;
    let mut json = admission
        .build_scoped(lua, value, &mut charge)
        .map_err(|error| (ErrorKind::InvalidRequest, error.to_string()))?;
    #[cfg(test)]
    ENCODE_BUILT_USAGE.with(|usage| usage.set(Some(memory.usage().1)));
    if empty_as_array {
        empty_objects_to_arrays(&mut json);
    }
    let mut output = ChargedWriter {
        bytes: Vec::new(),
        charge: &mut charge,
    };
    serde_json::to_writer(&mut output, &json).map_err(|_| capacity())?;
    let bytes = output.bytes;
    drop(json);
    Ok((bytes, charge))
}

/// Replacing an empty map with an empty vector allocates nothing.
fn empty_objects_to_arrays(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) if map.is_empty() => {
            *value = serde_json::Value::Array(Vec::new());
        }
        serde_json::Value::Object(map) => map.values_mut().for_each(empty_objects_to_arrays),
        serde_json::Value::Array(items) => items.iter_mut().for_each(empty_objects_to_arrays),
        _ => {}
    }
}

/// A byte buffer that funds each capacity increase before it allocates.
struct ChargedWriter<'a> {
    bytes: Vec<u8>,
    charge: &'a mut LuaCallbackCharge,
}

impl io::Write for ChargedWriter<'_> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let needed = self
            .bytes
            .len()
            .checked_add(data.len())
            .ok_or(io::ErrorKind::OutOfMemory)?;
        if needed > self.bytes.capacity() {
            let target = needed.max(self.bytes.capacity().saturating_mul(2));
            let additional = target - self.bytes.capacity();
            self.charge
                .grow(additional)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
            self.bytes.reserve_exact(target - self.bytes.len());
        }
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Build Lua values straight from the parser. Every allocation is Lua VM
/// memory, which the per-VM limit bounds; no Rust copy of unknown size exists.
/// JSON arrays carry mlua's array metatable, so they encode back as arrays.
struct LuaSeed<'lua>(&'lua Lua);

impl<'de> DeserializeSeed<'de> for LuaSeed<'_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for LuaSeed<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(self.0.null())
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Boolean(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Integer(value))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(i64::try_from(value).map_or(Value::Number(value as f64), Value::Integer))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        Ok(Value::Number(value))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        self.0
            .create_string(value)
            .map(Value::String)
            .map_err(E::custom)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<Value, A::Error> {
        let table = self.0.create_table().map_err(de::Error::custom)?;
        table
            .set_metatable(Some(self.0.array_metatable()))
            .map_err(de::Error::custom)?;
        let mut index = 1_i64;
        while let Some(item) = items.next_element_seed(LuaSeed(self.0))? {
            table.raw_set(index, item).map_err(de::Error::custom)?;
            index += 1;
        }
        Ok(Value::Table(table))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut entries: A) -> Result<Value, A::Error> {
        let table = self.0.create_table().map_err(de::Error::custom)?;
        while let Some(key) = entries.next_key_seed(LuaKeySeed(self.0))? {
            let value = entries.next_value_seed(LuaSeed(self.0))?;
            table.raw_set(key, value).map_err(de::Error::custom)?;
        }
        Ok(Value::Table(table))
    }
}

/// Object keys become Lua strings directly, without a Rust `String`.
struct LuaKeySeed<'lua>(&'lua Lua);

impl<'de> DeserializeSeed<'de> for LuaKeySeed<'_> {
    type Value = mlua::String;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for LuaKeySeed<'_> {
    type Value = mlua::String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object key")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        self.0.create_string(value).map_err(E::custom)
    }
}

fn clock_table(lua: &Lua) -> mlua::Result<Table> {
    static STARTED: OnceLock<Instant> = OnceLock::new();
    let started = *STARTED.get_or_init(Instant::now);
    let clock = lua.create_table()?;
    clock.set(
        "now",
        lua.create_function(|lua, _: Value| {
            let millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
                .unwrap_or(0);
            result::ok(lua, Value::Integer(millis))
        })?,
    )?;
    clock.set(
        "monotonic",
        lua.create_function(move |lua, _: Value| {
            let millis = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
            result::ok(lua, Value::Integer(millis))
        })?,
    )?;
    Ok(clock)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_memory::LuaMemoryLimits;
    use mlua::{LuaOptions, StdLib};

    fn vm(callback_bytes: usize) -> (Lua, Arc<LuaMemoryAccount>) {
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8,
            LuaOptions::default(),
        )
        .unwrap();
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1024 * 1024,
            total_vm_bytes: 1024 * 1024,
            per_callback_bytes: callback_bytes,
            total_callback_bytes: callback_bytes,
        })
        .unwrap();
        let botster = lua.create_table().unwrap();
        install(&lua, &botster, Arc::clone(&memory)).unwrap();
        lua.globals().set("botster", botster).unwrap();
        (lua, memory)
    }

    #[test]
    fn json_round_trips_through_the_result_shape() {
        let (lua, memory) = vm(64 * 1024);
        lua.load(
            r#"
            local json = botster.json
            local encoded = json.encode({ value = { name = "run", count = 2, tags = { "a", "b" } } })
            assert(encoded.ok, encoded.error and encoded.error.message)
            local decoded = json.decode({ text = encoded.value })
            assert(decoded.ok)
            assert(decoded.value.name == "run" and decoded.value.count == 2)
            assert(decoded.value.tags[2] == "b")
            -- decoded arrays keep their array identity when encoded again
            local again = json.encode({ value = decoded.value.tags })
            assert(again.ok and again.value == '["a","b"]', again.value)
            -- empty tables encode as objects unless the caller asks for arrays
            assert(json.encode({ value = {} }).value == "{}")
            assert(json.encode({ value = { list = {} }, arrays = "empty" }).value == '{"list":[]}')
            -- null survives both directions
            local with_null = json.decode({ text = '{"a":null}' })
            assert(with_null.ok and with_null.value.a == json.null)
            assert(json.encode({ value = json.null }).value == "null")
            "#,
        )
        .exec()
        .unwrap();
        assert_eq!(
            memory.usage().1,
            0,
            "every encode and decode charge is released"
        );
    }

    #[test]
    fn json_reports_typed_errors_instead_of_raising() {
        let (lua, _memory) = vm(64 * 1024);
        lua.load(
            r#"
            local json = botster.json
            local function kind(result) return (not result.ok) and result.error.kind end
            assert(kind(json.decode({ text = "{" })) == "invalid_request")
            assert(kind(json.decode({ text = "[1] trailing" })) == "invalid_request")
            assert(kind(json.decode({})) == "invalid_request")
            assert(kind(json.decode("{}")) == "invalid_request")
            assert(kind(json.encode({ value = function() end })) == "invalid_request")
            assert(kind(json.encode({ value = 1, arrays = "all" })) == "invalid_request")
            local refused = json.decode({ text = "{" })
            assert(refused.error.retryable == false and type(refused.error.message) == "string")
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn json_encode_funds_the_built_value_while_it_exists() {
        let (lua, memory) = vm(64 * 1024);
        ENCODE_BUILT_USAGE.with(|usage| usage.set(None));
        let encoded: String = lua
            .load(
                r#"
                local result = botster.json.encode({ value = { text = string.rep("x", 2048), n = { 1, 2, 3 } } })
                assert(result.ok, result.error and result.error.message)
                return result.value
                "#,
            )
            .eval()
            .unwrap();
        let built = ENCODE_BUILT_USAGE
            .with(std::cell::Cell::get)
            .expect("encode built a value");
        // The built value is funded before the output writer charges any
        // byte, so the account covers at least the encoded text then.
        assert!(
            built >= encoded.len(),
            "built value charged {built} bytes, encoded text is {} bytes",
            encoded.len()
        );
        assert_eq!(memory.usage().1, 0, "the charge is released after encode");
    }

    #[test]
    fn json_refuses_work_beyond_the_callback_account() {
        let (lua, memory) = vm(256);
        lua.load(
            r#"
            local big = string.rep("x", 4096)
            local encoded = botster.json.encode({ value = { text = big } })
            assert(not encoded.ok and encoded.error.kind == "quota_exceeded", encoded.error and encoded.error.kind)
            local decoded = botster.json.decode({ text = '"' .. big .. '"' })
            assert(not decoded.ok and decoded.error.kind == "quota_exceeded")
            "#,
        )
        .exec()
        .unwrap();
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn clock_reports_wall_and_monotonic_milliseconds() {
        let (lua, _memory) = vm(64 * 1024);
        lua.load(
            r#"
            local now = botster.clock.now()
            assert(now.ok and math.type(now.value) == "integer")
            assert(now.value > 1700000000000, now.value)
            local first = botster.clock.monotonic({})
            local second = botster.clock.monotonic()
            assert(first.ok and second.ok and second.value >= first.value)
            "#,
        )
        .exec()
        .unwrap();
    }
}
