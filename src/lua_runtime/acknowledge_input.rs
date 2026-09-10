//! Input admission for `coordination.acknowledge`.

use std::sync::Arc;

use botster_core::{
    ClientId, EndpointId, EnvelopeId, EnvelopeTarget, PluginKey, SessionId, SubscriptionId,
};
use botster_core_daemon::{
    AcknowledgeRoutedEnvelopeRequest, CoreDaemon, CoreDaemonError,
    RoutedEnvelopeDeliveryStateResult,
};
use mlua::{Function, Lua, LuaSerdeExt, Table, Value};

use super::HubCoordinationBridge;
use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount};

/// The payload owns its charge until Core consumes or discards the payload.
pub(crate) struct AcknowledgeInput {
    request: AcknowledgeRoutedEnvelopeRequest,
    // Rust drops fields in declaration order. Free the strings before the charge.
    charge: Option<LuaCallbackCharge>,
}

impl AcknowledgeInput {
    pub(crate) fn acknowledge(
        self,
        daemon: &mut CoreDaemon,
    ) -> Result<RoutedEnvelopeDeliveryStateResult, CoreDaemonError> {
        let Self { request, charge } = self;
        let result = daemon.acknowledge_routed_envelope(request);
        drop(charge);
        result
    }
}

#[derive(Debug, PartialEq, Eq)]
enum AdmissionError {
    Quota,
    Capacity,
    InvalidTarget,
}

fn admit(
    memory: Option<&Arc<LuaMemoryAccount>>,
    kind: &str,
    first: &str,
    second: &str,
    envelope_id: &str,
) -> Result<AcknowledgeInput, AdmissionError> {
    let bytes = first
        .len()
        .checked_add(second.len())
        .and_then(|bytes| bytes.checked_add(envelope_id.len()))
        .ok_or(AdmissionError::Quota)?;
    let charge = match memory {
        Some(memory) => {
            if bytes > memory.limits().per_callback_bytes {
                return Err(AdmissionError::Quota);
            }
            Some(
                memory
                    .reserve_callback_bytes(bytes)
                    .map_err(|_| AdmissionError::Capacity)?,
            )
        }
        None => None,
    };
    // The trusted Lua wrapper supplies a known variant and only its consumed fields.
    // Each String requests its exact byte length after admission.
    let target = match kind {
        "endpoint" => EnvelopeTarget::Endpoint {
            endpoint_id: EndpointId(first.to_owned()),
        },
        "client" => EnvelopeTarget::Client {
            client_id: ClientId(first.to_owned()),
        },
        "session" => EnvelopeTarget::Session {
            session_id: SessionId(first.to_owned()),
        },
        "subscription" => EnvelopeTarget::Subscription {
            session_id: SessionId(first.to_owned()),
            subscription_id: SubscriptionId(second.to_owned()),
        },
        "plugin" => EnvelopeTarget::Plugin {
            plugin_key: PluginKey(first.to_owned()),
        },
        "stream" => EnvelopeTarget::Stream {
            stream: first.to_owned(),
        },
        "topic" => EnvelopeTarget::Topic {
            topic: first.to_owned(),
        },
        _ => return Err(AdmissionError::InvalidTarget),
    };
    Ok(AcknowledgeInput {
        request: AcknowledgeRoutedEnvelopeRequest {
            target,
            envelope_id: EnvelopeId(envelope_id.to_owned()),
        },
        charge,
    })
}

pub(super) fn callback(
    lua: &Lua,
    bridge: HubCoordinationBridge,
    memory: Option<Arc<LuaMemoryAccount>>,
) -> mlua::Result<Function> {
    let quota =
        lua.create_string("coordination.acknowledge exceeded the Lua callback memory limit")?;
    let capacity = lua.create_string("Lua callback memory capacity exhausted")?;
    let conversion =
        lua.create_string("coordination.acknowledge could not allocate its Lua result")?;
    let invalid_target =
        lua.create_string("coordination.acknowledge requires a recognized target.type")?;
    let invalid_utf8 = lua.create_string("coordination.acknowledge requires UTF-8 strings")?;
    // Only the trusted wrapper can call this function. Foreign values never enter Rust.
    let callback = lua.create_function(
        move |lua,
              (kind, first, second, envelope_id): (
            mlua::String,
            mlua::String,
            mlua::String,
            mlua::String,
        )| {
            let kind_bytes = kind.as_bytes();
            let first_bytes = first.as_bytes();
            let second_bytes = second.as_bytes();
            let envelope_bytes = envelope_id.as_bytes();
            let strings = (
                std::str::from_utf8(&kind_bytes),
                std::str::from_utf8(&first_bytes),
                std::str::from_utf8(&second_bytes),
                std::str::from_utf8(&envelope_bytes),
            );
            let (Ok(kind), Ok(first), Ok(second), Ok(envelope_id)) = strings else {
                return Ok(Value::String(invalid_utf8.clone()));
            };
            let input = match admit(memory.as_ref(), kind, first, second, envelope_id) {
                Ok(input) => input,
                Err(AdmissionError::Quota) => return Ok(Value::String(quota.clone())),
                Err(AdmissionError::Capacity) => return Ok(Value::String(capacity.clone())),
                Err(AdmissionError::InvalidTarget) => {
                    return Ok(Value::String(invalid_target.clone()));
                }
            };
            // Bridge storage and result storage have separate owners and accounting work.
            let result = match bridge.acknowledge(input) {
                Ok(outcome) => lua.to_value(&outcome),
                Err(message) => lua.create_string(&message).map(Value::String),
            };
            Ok(result.unwrap_or_else(|_| Value::String(conversion.clone())))
        },
    )?;
    wrap(lua, callback)
}

fn wrap(lua: &Lua, callback: Function) -> mlua::Result<Function> {
    let array_metatable = lua.array_metatable();
    // Lua cannot inspect the protected array metatable. This callback receives only tables.
    let is_array = lua.create_function(move |_, table: Table| {
        Ok(table.raw_len() > 0
            || table
                .metatable()
                .is_some_and(|mt| mt.to_pointer() == array_metatable.to_pointer()))
    })?;
    lua.load(r#"
        local callback, is_array = ...
        local type, rawget, error, utf8_len = type, rawget, error, utf8.len
        local fields = {
            endpoint = "endpoint_id", client = "client_id", session = "session_id",
            subscription = "session_id", plugin = "plugin_key", stream = "stream", topic = "topic"
        }
        local function string_field(value, path)
            if type(value) ~= "string" or utf8_len(value) == nil then
                error("coordination.acknowledge requires " .. path .. " as a UTF-8 string", 0)
            end
            return value
        end
        return function(args)
            if type(args) ~= "table" or is_array(args) or rawget(args, "target") == nil then
                error("coordination target is required", 0)
            end
            local target = rawget(args, "target")
            if type(target) ~= "table" or is_array(target) then
                error("coordination.acknowledge requires target as a table", 0)
            end
            local kind = string_field(rawget(target, "type"), "target.type")
            local field = rawget(fields, kind)
            if field == nil then
                error("coordination.acknowledge requires target.type as endpoint, client, session, subscription, plugin, stream, or topic", 0)
            end
            local first = rawget(target, field)
            local second = ""
            if kind == "subscription" then
                second = rawget(target, "subscription_id")
                -- Serde checks present fields before it reports missing fields.
                if first ~= nil then string_field(first, "target.session_id") end
                if second ~= nil then string_field(second, "target.subscription_id") end
            end
            first = string_field(first, "target." .. field)
            if kind == "subscription" then second = string_field(second, "target.subscription_id") end
            local envelope_id = string_field(rawget(args, "envelope_id"), "envelope_id")
            local result = callback(kind, first, second, envelope_id)
            if type(result) == "string" then error(result, 0) end
            return result
        end
    "#).set_name("@hub/acknowledge_input").call((callback, is_array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_memory::LuaMemoryLimits;
    use crate::lua_runtime::{HubCoordinationResponse, PendingCoordinationOperation};
    use std::time::{Duration, Instant};

    fn account(per: usize, total: usize) -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: per,
            total_callback_bytes: total,
        })
        .unwrap()
    }

    fn validation_lua() -> Lua {
        let lua = Lua::new();
        let accept = lua
            .create_function(
                |lua,
                 (kind, first, second, envelope_id): (
                    mlua::String,
                    mlua::String,
                    mlua::String,
                    mlua::String,
                )| {
                    let input = admit(
                        None,
                        &kind.to_str()?,
                        &first.to_str()?,
                        &second.to_str()?,
                        &envelope_id.to_str()?,
                    )
                    .unwrap();
                    lua.to_value(&input.request)
                },
            )
            .unwrap();
        lua.globals()
            .set("ack", wrap(&lua, accept).unwrap())
            .unwrap();
        lua.globals().set("null", lua.null()).unwrap();
        lua.globals()
            .set("array_mt", lua.array_metatable())
            .unwrap();
        lua
    }

    fn old(lua: &Lua, value: Value) -> Result<AcknowledgeRoutedEnvelopeRequest, String> {
        let value = lua
            .from_value::<serde_json::Value>(value)
            .map_err(|e| e.to_string())?;
        let target =
            super::super::target_from_json(value.get("target")).map_err(|e| e.to_string())?;
        let envelope_id = value
            .get("envelope_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "coordination.acknowledge requires envelope_id".to_owned())?;
        Ok(AcknowledgeRoutedEnvelopeRequest {
            target,
            envelope_id: EnvelopeId(envelope_id.to_owned()),
        })
    }

    #[test]
    fn all_variants_preserve_consumed_values() {
        let lua = validation_lua();
        let ack: Function = lua.globals().get("ack").unwrap();
        for (kind, field) in [
            ("endpoint", "endpoint_id"),
            ("client", "client_id"),
            ("session", "session_id"),
            ("subscription", "session_id"),
            ("plugin", "plugin_key"),
            ("stream", "stream"),
            ("topic", "topic"),
        ] {
            for value in ["", " ", "a\0b", "caf\u{e9}"] {
                let target = lua.create_table().unwrap();
                target.set("type", kind).unwrap();
                target.set(field, value).unwrap();
                if kind == "subscription" {
                    target.set("subscription_id", value).unwrap();
                }
                let args = lua.create_table().unwrap();
                args.set("target", target).unwrap();
                args.set("envelope_id", value).unwrap();
                let expected = old(&lua, Value::Table(args.clone())).unwrap();
                let actual = ack.call::<Value>(args).unwrap();
                assert_eq!(
                    lua.from_value::<AcknowledgeRoutedEnvelopeRequest>(actual)
                        .unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn invalid_consumed_fields_preserve_classification_and_order() {
        let lua = validation_lua();
        let check: Function = lua.load("return function(args) local ok, value = pcall(ack, args); assert(not ok and type(value) == 'string'); return value end").eval().unwrap();
        for (source, old_fragment, new_fragment) in [
            (
                "nil",
                "coordination target is required",
                "coordination target is required",
            ),
            (
                "false",
                "coordination target is required",
                "coordination target is required",
            ),
            (
                "{1, target={type='topic',topic='t'},envelope_id='e'}",
                "coordination target is required",
                "coordination target is required",
            ),
            (
                "setmetatable({target={type='topic',topic='t'},envelope_id='e'},array_mt)",
                "coordination target is required",
                "coordination target is required",
            ),
            (
                "{target=null}",
                "invalid coordination target",
                "target as a table",
            ),
            (
                "{target={1,type='topic',topic='t'}}",
                "invalid coordination target",
                "target as a table",
            ),
            (
                "{target=setmetatable({type='topic',topic='t'},array_mt)}",
                "invalid coordination target",
                "target as a table",
            ),
            (
                "{target={}}",
                "missing field `type`",
                "target.type as a UTF-8 string",
            ),
            (
                "{target={type=1}}",
                "invalid coordination target",
                "target.type as a UTF-8 string",
            ),
            (
                "{target={type='unknown'}}",
                "unknown variant",
                "target.type as endpoint",
            ),
            (
                "{target={type='topic',topic=false}}",
                "invalid coordination target",
                "target.topic as a UTF-8 string",
            ),
            (
                "{target={type='subscription',subscription_id=false}}",
                "invalid coordination target",
                "target.subscription_id as a UTF-8 string",
            ),
            (
                "{target={type='subscription'}}",
                "missing field `session_id`",
                "target.session_id as a UTF-8 string",
            ),
            (
                "{target={type='subscription',session_id='s'}}",
                "missing field `subscription_id`",
                "target.subscription_id as a UTF-8 string",
            ),
            (
                "{target={type='topic',topic='t'},envelope_id=false}",
                "requires envelope_id",
                "envelope_id as a UTF-8 string",
            ),
            (
                "setmetatable({}, {__index={target={type='topic',topic='t'},envelope_id='e'}})",
                "coordination target is required",
                "coordination target is required",
            ),
        ] {
            let value: Value = lua.load(format!("return {source}")).eval().unwrap();
            let previous = old(&lua, value.clone()).unwrap_err();
            let current: String = check.call(value).unwrap();
            assert!(previous.contains(old_fragment), "{source}: {previous}");
            assert!(current.contains(new_fragment), "{source}: {current}");
            eprintln!("acknowledge diagnostic: {source}\nold: {previous}\nnew: {current}");
        }
        lua.load(
            r#"
            local invalid = {false, 1, {}, function() end, null, string.char(255)}
            for _, value in ipairs(invalid) do
                local ok, message = pcall(ack, {target={type='topic',topic=value}})
                assert(not ok and type(message)=='string' and message:find('target.topic', 1, true))
                ok, message = pcall(ack, {target={type='topic',topic='t'},envelope_id=value})
                assert(not ok and type(message)=='string' and message:find('envelope_id', 1, true))
            end
        "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn unused_values_are_ignored_without_metamethods() {
        let lua = validation_lua();
        let args: Value = lua
            .load(
                r#"
            local cycle = {}; cycle.self = cycle
            return {target={type='topic',topic='t',unused=function() end,[{}]=cycle},
                    envelope_id='e',unused=cycle}
        "#,
            )
            .eval()
            .unwrap();
        assert!(old(&lua, args.clone()).is_err());
        lua.globals()
            .get::<Function>("ack")
            .unwrap()
            .call::<Value>(args)
            .unwrap();
        lua.load(r#"
            local function fail() error('metamethod ran') end
            local target = setmetatable({type='topic',topic='t'}, {__index=fail,__pairs=fail,__len=fail,__eq=fail,__metatable=false})
            local args = setmetatable({target=target,envelope_id='e'}, {__index=fail,__pairs=fail,__len=fail})
            assert(ack(args).envelope_id=='e')
            local saved = ack
            type, rawget, error, utf8.len = fail, fail, fail, fail
            assert(saved(args).target.topic=='t')
        "#).exec().unwrap();
    }

    #[test]
    fn exact_input_charge_distinguishes_quota_and_capacity() {
        let memory = account(6, 6);
        assert!(matches!(
            admit(Some(&memory), "subscription", "ab", "cd", "efg"),
            Err(AdmissionError::Quota)
        ));
        assert_eq!(memory.usage().1, 0);
        let input = admit(Some(&memory), "subscription", "ab", "cd", "ef").unwrap();
        assert_eq!(memory.usage().1, 6);
        assert!(matches!(
            admit(Some(&memory), "topic", "t", "", "e"),
            Err(AdmissionError::Capacity)
        ));
        assert_eq!(memory.usage().1, 6);
        assert_eq!(input.request.envelope_id.0, "ef");
        drop(input);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn closure_disposal_and_owner_refusal_release_input() {
        let memory = account(2, 2);
        let input = admit(Some(&memory), "topic", "t", "", "e").unwrap();
        let operation = move || drop(input);
        assert_eq!(memory.usage().1, 2);
        drop(operation);
        assert_eq!(memory.usage().1, 0);
        let bridge = HubCoordinationBridge::new();
        let input = admit(Some(&memory), "topic", "t", "", "e").unwrap();
        assert!(
            bridge
                .acknowledge(input)
                .unwrap_err()
                .contains("not at plugin load")
        );
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn wrapper_refusals_return_lua_strings_without_retaining_input() {
        let memory = account(2, 2);
        let lua = Lua::new();
        lua.globals()
            .set(
                "ack",
                callback(
                    &lua,
                    HubCoordinationBridge::new(),
                    Some(Arc::clone(&memory)),
                )
                .unwrap(),
            )
            .unwrap();
        let held = memory.reserve_callback_bytes(2).unwrap();
        lua.load(
            r#"
            local function fails(args, fragment)
                local ok, message = pcall(ack, args)
                assert(not ok and type(message)=='string' and message:find(fragment, 1, true))
            end
            fails({target={type='topic',topic='t'},envelope_id='e'}, 'capacity exhausted')
            fails({target={type='topic',topic='tt'},envelope_id='e'}, 'memory limit')
            fails({target={type='topic',topic=function() end}}, 'target.topic')
        "#,
        )
        .exec()
        .unwrap();
        assert_eq!(memory.usage().1, 2);
        drop(held);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn timeout_retains_input_until_terminal_disposal() {
        let memory = account(2, 2);
        let bridge = HubCoordinationBridge::new();
        let input = admit(Some(&memory), "topic", "t", "", "e").unwrap();
        let producer = bridge.clone();
        let result = std::thread::spawn(move || producer.acknowledge(input))
            .join()
            .unwrap();
        assert!(
            result
                .unwrap_err()
                .contains("did not complete before timeout")
        );
        assert_eq!(bridge.test_pending_count(), 1);
        assert_eq!(memory.usage().1, 2);
        assert!(bridge.dispose_terminal_pending());
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn real_wrapper_queues_charged_input_and_returns_delivery_state() {
        let memory = account(2, 2);
        let bridge = HubCoordinationBridge::new();
        let producer = bridge.clone();
        let callback_memory = Arc::clone(&memory);
        let worker = std::thread::spawn(move || {
            let lua = Lua::new();
            lua.globals()
                .set(
                    "ack",
                    callback(&lua, producer, Some(callback_memory)).unwrap(),
                )
                .unwrap();
            lua.globals().set("null", lua.null()).unwrap();
            lua.load("local result=ack({target={type='topic',topic='t'},envelope_id='e'}); assert(result.state == null)").exec().unwrap();
        });
        // This poll schedules the test consumer. It does not prove a production wake.
        // The test must reply before the bridge's existing 1000 ms timeout.
        let deadline = Instant::now() + Duration::from_millis(500);
        let pending = loop {
            if let Some(pending) = bridge.take_pending() {
                break pending;
            }
            assert!(
                Instant::now() < deadline,
                "the wrapper did not queue its request"
            );
            std::thread::yield_now();
        };
        assert_eq!(memory.usage().1, 2);
        let PendingCoordinationOperation::Acknowledge { input } = pending.operation else {
            panic!("wrong operation")
        };
        assert_eq!(input.request.envelope_id.0, "e");
        drop(input);
        assert_eq!(memory.usage().1, 0);
        assert!(
            pending
                .response
                .send(Ok(HubCoordinationResponse::Acknowledge(
                    RoutedEnvelopeDeliveryStateResult { state: None },
                )))
                .is_ok()
        );
        worker.join().unwrap();
    }
}
