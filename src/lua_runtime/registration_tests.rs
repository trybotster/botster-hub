//! Compare trusted Lua registration with the previous Rust registration behavior.

use super::*;

fn reference(lua: &Lua) -> mlua::Result<(Function, Function)> {
    let events = lua.create_table()?;
    events.set(
        "on",
        lua.create_function(
            move |lua, (owner, name, handler): (Value, Value, Option<Value>)| {
                let (Value::String(owner), Value::String(name), Some(Value::Function(handler))) =
                    (owner, name, handler)
                else {
                    return Err(mlua::Error::RuntimeError(
                        EventPlaneStatus::RejectedInvalid.as_str().to_string(),
                    ));
                };
                let owner = owner.to_str()?.to_string();
                let name = name.to_str()?.to_string();
                if owner.trim().is_empty()
                    || name.trim().is_empty()
                    || owner.contains('*')
                    || name.contains('*')
                    || owner.contains('?')
                    || name.contains('?')
                {
                    return Err(mlua::Error::RuntimeError(
                        EventPlaneStatus::RejectedWildcard.as_str().to_string(),
                    ));
                }
                let registration = lua.globals().get::<Table>("__botster_registration")?;
                let handler_table: Table = match registration.get("handlers") {
                    Ok(handler_table) => handler_table,
                    Err(_) => {
                        let handler_table = lua.create_table()?;
                        registration.set("handlers", handler_table.clone())?;
                        handler_table
                    }
                };
                let handler_id = format!("event:{owner}:{name}:{}", handler_table.raw_len() + 1);
                let handlers = lua.globals().get::<Table>("__botster_handlers")?;
                handlers.set(handler_id.clone(), handler)?;
                let entry = lua.create_table()?;
                entry.set("id", handler_id)?;
                entry.set("kind", "event")?;
                entry.set("event_owner", owner)?;
                entry.set("event", name)?;
                handler_table.set(handler_table.raw_len() + 1, entry)?;
                Ok(())
            },
        )?,
    )?;
    let register = lua.create_function(|lua, registration: Table| {
        let handlers = lua.globals().get::<Table>("__botster_handlers")?;
        let pending_registration = lua.globals().get::<Table>("__botster_registration")?;
        if let Ok(pending_handlers) = pending_registration.get::<Table>("handlers") {
            let custom_handlers: Table = match registration.get("handlers") {
                Ok(custom_handlers) => custom_handlers,
                Err(_) => {
                    let custom_handlers = lua.create_table()?;
                    registration.set("handlers", custom_handlers.clone())?;
                    custom_handlers
                }
            };
            let mut index = custom_handlers.raw_len();
            for pending_handler in pending_handlers.sequence_values::<Table>() {
                index += 1;
                custom_handlers.set(index, pending_handler?)?;
            }
        }
        if let Ok(tools) = registration.get::<Table>("tools") {
            for tool in tools.sequence_values::<Table>() {
                let tool = tool?;
                let handler_id: String = tool.get("handler")?;
                let handler: Function = tool.get("call")?;
                handlers.set(handler_id, handler)?;
            }
        }
        if let Ok(custom_handlers) = registration.get::<Table>("handlers") {
            for custom_handler in custom_handlers.sequence_values::<Table>() {
                let custom_handler = custom_handler?;
                let handler_id: String = custom_handler.get("id")?;
                if let Ok(handler) = custom_handler.get::<Function>("call") {
                    handlers.set(handler_id, handler)?;
                } else if custom_handler.get::<String>("kind").ok().as_deref()
                    == Some("entity_provider")
                {
                    return Err(mlua::Error::RuntimeError(
                        "entity provider declarations require a call handler".to_string(),
                    ));
                }
            }
        }
        lua.globals()
            .set("__botster_registration", registration.clone())?;
        Ok(registration)
    })?;
    Ok((events.get("on")?, register))
}

fn environment(previous: bool) -> Lua {
    let lua = Lua::new();
    lua.globals()
        .set("__botster_handlers", lua.create_table().unwrap())
        .unwrap();
    lua.globals()
        .set("__botster_registration", lua.create_table().unwrap())
        .unwrap();
    let (on, register) = if previous {
        reference(&lua).unwrap()
    } else {
        lua.load(include_str!("registration.lua"))
            .call((lua.globals(), lua.null()))
            .unwrap()
    };
    lua.globals().set("on", on).unwrap();
    lua.globals().set("register", register).unwrap();
    lua.globals().set("null", lua.null()).unwrap();
    lua
}

fn compare(source: &str) {
    for previous in [true, false] {
        environment(previous).load(source).exec().unwrap();
    }
}

#[test]
fn registration_preserves_ids_tables_and_return_values() {
    compare(
        r#"
        local event = function() return 'event' end
        assert(select('#', on('owner', 'created', event)) == 0)
        local id = 'event:owner:created:1'
        assert(__botster_handlers[id] == event)
        local pending = __botster_registration.handlers[1]
        assert(pending.id == id and pending.kind == 'event')
        assert(pending.event_owner == 'owner' and pending.event == 'created')
        local tool = function() return 'tool' end
        local custom = function() return 'custom' end
        local registration = {
            tools = {{handler = 12, call = tool}},
            handlers = {{id = 'custom', kind = 'tool', call = custom}},
        }
        assert(register(registration) == registration)
        assert(__botster_registration == registration)
        assert(registration.handlers[2] == pending)
        assert(__botster_handlers['12'] == tool)
        assert(__botster_handlers.custom == custom)
        assert(__botster_handlers[id] == event)
    "#,
    );
}

#[test]
fn registration_preserves_get_set_and_raw_sequence_behavior() {
    compare(
        r#"
        local writes, reads = {}, {}
        local raw_handlers = {}
        __botster_handlers = setmetatable({}, {
            __newindex = function(_, key, value)
                writes[#writes+1] = key
                raw_handlers[key] = value
            end,
        })
        local declaration = {id = 'custom', call = function() end}
        local sequence = setmetatable({declaration}, {
            __index = function() error('sequence must use raw lookup', 0) end,
            __len = function() error('sequence must use raw length', 0) end,
        })
        local backing = {handlers = sequence}
        local registration = setmetatable({}, {
            __index = function(_, key)
                reads[#reads+1] = key
                return backing[key]
            end,
            __newindex = function(_, key, value) backing[key] = value end,
        })
        register(registration)
        assert(#writes == 1 and writes[1] == 'custom')
        assert(raw_handlers.custom == declaration.call)
        assert(reads[1] == 'tools' and reads[2] == 'handlers')
        __botster_registration = setmetatable({}, {
            __index = function() error('ignored field failure', 0) end,
            __newindex = function(target, key, value) rawset(target, key, value) end,
        })
        on('owner', 'event', function() end)
        assert(__botster_registration.handlers[1].id == 'event:owner:event:1')
    "#,
    );
}

#[test]
fn registration_preserves_plugin_error_messages() {
    let source = r#"
        __botster_handlers = setmetatable({}, {
            __newindex = function() error('runtime error: plugin composition failed', 0) end,
        })
        on('owner', 'event', function() end)
    "#;
    let previous = sanitize_lua_error(environment(true).load(source).exec().unwrap_err());
    let current = sanitize_lua_error(environment(false).load(source).exec().unwrap_err());
    assert_eq!(previous, current);
}

#[test]
fn registration_preserves_unicode_blank_and_wildcard_validation() {
    compare(
        r#"
        for _, value in ipairs({'', ' ', '\t', utf8.char(0x85), utf8.char(0x2003), utf8.char(0x3000), '*', '?'}) do
            local ok, message = pcall(on, value, 'event', function() end)
            assert(not ok and tostring(message):find('rejected_wildcard', 1, true))
        end
        for _, value in ipairs({false, 1, {}}) do
            local ok, message = pcall(on, value, 'event', function() end)
            assert(not ok and tostring(message):find('rejected_invalid', 1, true))
        end
        on(' owner ', 'event', function() end)
        assert(__botster_registration.handlers[1].event_owner == ' owner ')
    "#,
    );
}

#[test]
fn registration_preserves_utf8_diagnostic_content() {
    for bytes in [
        vec![255],
        vec![0xc2],
        vec![0xe2, 0x82],
        vec![0xf0, 0x9f, 0x92],
        vec![0xe2, 0x28, 0xa1],
        vec![0xed, 0xa0, 0x80],
        vec![0xf4, 0x90, 0x80, 0x80],
    ] {
        let mut messages = Vec::new();
        for previous in [true, false] {
            let lua = environment(previous);
            lua.globals()
                .set("invalid", lua.create_string(&bytes).unwrap())
                .unwrap();
            let message: String = lua
                .load(
                    r#"
                local ok, message = pcall(on, invalid, 'event', function() end)
                assert(not ok)
                return tostring(message)
            "#,
                )
                .eval()
                .unwrap();
            messages.push(message.lines().next().unwrap().to_owned());
        }
        assert_eq!(messages[0], messages[1], "{bytes:?}");
    }
}

#[test]
fn registration_preserves_type_diagnostic_content() {
    for source in [
        "register(1)",
        "register(1.5)",
        "register()",
        "register(null)",
        "register({tools = {1}})",
        "register({handlers = {1}})",
        "register({tools = {{handler = 'tool', call = 1}}})",
        "register({tools = {{handler = {}, call = function() end}}})",
        "register({tools = {{handler = null, call = function() end}}})",
    ] {
        let previous = sanitize_lua_error(environment(true).load(source).exec().unwrap_err());
        let current = sanitize_lua_error(environment(false).load(source).exec().unwrap_err());
        assert_eq!(current, format!("runtime error: {previous}"), "{source}");
    }
}

#[test]
fn registration_captures_trusted_globals() {
    let lua = environment(false);
    lua.load(
        r#"
        local saved_rawget = rawget
        local function replaced() error('rebound global ran', 0) end
        type, pcall, rawlen, rawget, tostring = replaced, replaced, replaced, replaced, replaced
        string.find, string.byte, utf8.len, utf8.codes = replaced, replaced, replaced, replaced
        on('owner', 'event', function() end)
        local result = register({})
        assert(saved_rawget(result.handlers, 1).id == 'event:owner:event:1')
    "#,
    )
    .exec()
    .unwrap();
}
