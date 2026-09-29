//! The Lua spec runner.
//!
//! A spec runs in its own driver VM. The driver VM is unsandboxed and
//! separate from every plugin VM: the plugin under test never sees the
//! `botster.test` module. Each test starts a fresh real Hub daemon
//! ([`KitHub`]) and drives it one settled step at a time.

use std::cell::RefCell;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use botster_hub_client::{DaemonRequest, DaemonResponse};
use mlua::{Function, Lua, LuaSerdeExt, MultiValue, Scope, SerializeOptions, Table, Value};

use crate::{
    EnvelopeId, EnvelopeTarget, KitError, KitHub, KitOptions, RegistrySessionState, SessionId,
    SessionLifecycleRecord, SessionLifecycleState, session_record,
};

/// The result of one `kit.test`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestOutcome {
    pub name: String,
    /// `None` when the test passed.
    pub failure: Option<String>,
}

type Registered = Arc<Mutex<Vec<(String, Function)>>>;

/// Run every test of one spec file. `plugin_directory` is where `t:load`
/// resolves relative paths and where `require` looks for helper modules.
pub fn run_spec_file(plugin_directory: &Path, spec_path: &Path) -> Vec<TestOutcome> {
    let name = spec_path.display().to_string();
    let source = match std::fs::read_to_string(spec_path) {
        Ok(source) => source,
        Err(error) => return vec![failed(&name, format!("cannot read the spec: {error}"))],
    };
    let lua = Lua::new();
    let registered: Registered = Arc::new(Mutex::new(Vec::new()));
    if let Err(error) = install(&lua, plugin_directory, spec_path, &registered) {
        return vec![failed(
            &name,
            format!("cannot set up the driver VM: {error}"),
        )];
    }
    if let Err(error) = lua
        .load(source)
        .set_name(format!("@{}", spec_path.display()))
        .exec()
    {
        return vec![failed(&name, error.to_string())];
    }
    let tests = registered.lock().expect("registered tests").clone();
    tests
        .into_iter()
        .map(
            |(test_name, body)| match run_test(&lua, plugin_directory, &test_name, &body) {
                Ok(()) => TestOutcome {
                    name: test_name,
                    failure: None,
                },
                Err(error) => failed(&test_name, error.to_string()),
            },
        )
        .collect()
}

fn failed(name: &str, message: String) -> TestOutcome {
    TestOutcome {
        name: name.to_string(),
        failure: Some(message),
    }
}

fn install(
    lua: &Lua,
    plugin_directory: &Path,
    spec_path: &Path,
    registered: &Registered,
) -> mlua::Result<()> {
    let package: Table = lua.globals().get("package")?;
    let mut roots = vec![plugin_directory.to_path_buf()];
    if let Some(parent) = spec_path.parent() {
        roots.push(parent.to_path_buf());
    }
    let path = roots
        .iter()
        .flat_map(|root| {
            [
                format!("{}/?.lua", root.display()),
                format!("{}/?/init.lua", root.display()),
            ]
        })
        .collect::<Vec<_>>()
        .join(";");
    package.set("path", path)?;

    let kit = lua.create_table()?;
    let registry = Arc::clone(registered);
    kit.set(
        "test",
        lua.create_function(move |_, (name, body): (String, Function)| {
            registry
                .lock()
                .expect("registered tests")
                .push((name, body));
            Ok(())
        })?,
    )?;
    // A session description. `t:sessions_baseline` and friends read its
    // fields; the constructor only returns the table it is given.
    kit.set(
        "session",
        lua.create_function(|_, description: Table| Ok(description))?,
    )?;
    let module = kit.clone();
    let preload: Table = package.get("preload")?;
    preload.set(
        "botster.test",
        lua.create_function(move |_, _: MultiValue| Ok(module.clone()))?,
    )?;
    Ok(())
}

fn run_test(lua: &Lua, plugin_directory: &Path, name: &str, body: &Function) -> mlua::Result<()> {
    let label: String = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect();
    let options = KitOptions::temporary(&label)
        .map_err(|error| mlua::Error::runtime(format!("kit root: {error}")))?;
    let kit = KitHub::start(options).map_err(kit_error)?;
    let kit = RefCell::new(kit);
    let watched = RefCell::new(HashSet::<String>::new());
    lua.scope(|scope| {
        let driver = build_driver(lua, scope, &kit, &watched, plugin_directory)?;
        body.call::<()>(driver)
    })
}

fn kit_error(error: KitError) -> mlua::Error {
    mlua::Error::runtime(error.to_string())
}

fn to_lua(lua: &Lua, value: &serde_json::Value) -> mlua::Result<Value> {
    lua.to_value_with(
        value,
        SerializeOptions::new()
            .serialize_none_to_null(false)
            .serialize_unit_to_null(false),
    )
}

fn serialize<T: serde::Serialize>(lua: &Lua, value: &T) -> mlua::Result<Value> {
    let json = serde_json::to_value(value).map_err(mlua::Error::external)?;
    to_lua(lua, &json)
}

/// `{ ok, error = { kind, message }, result, response }` for one Hub reply.
fn response_table(lua: &Lua, response: &DaemonResponse) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("ok", response.error.is_none())?;
    if let Some(error) = &response.error {
        let detail = lua.create_table()?;
        detail.set("kind", error.code.clone())?;
        detail.set("message", error.message.clone())?;
        table.set("error", detail)?;
    }
    table.set("result", serialize(lua, &response.plugin_tool_result)?)?;
    table.set("response", serialize(lua, response)?)?;
    Ok(table)
}

/// A typed refusal for a feature that the Hub cannot serve yet.
fn unsupported(lua: &Lua, feature: &str, gate: &str) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.set("ok", false)?;
    let detail = lua.create_table()?;
    detail.set("kind", "unsupported_by_kit")?;
    detail.set("feature", feature)?;
    detail.set("gate", gate)?;
    detail.set(
        "message",
        format!("{feature} is not supported by the kit yet (gate {gate})"),
    )?;
    table.set("error", detail)?;
    Ok(table)
}

fn unsupported_error(feature: &str, gate: &str) -> mlua::Error {
    mlua::Error::runtime(format!(
        "unsupported_by_kit: {feature} is not supported by the kit yet (gate {gate})"
    ))
}

fn session_from_lua(description: &Table) -> mlua::Result<SessionLifecycleRecord> {
    let id: String = description
        .get("id")
        .map_err(|_| mlua::Error::runtime("a session needs a string id"))?;
    let state: String = description
        .get("state")
        .unwrap_or_else(|_| "running".to_string());
    let (registry, lifecycle) = match state.as_str() {
        "starting" => (
            RegistrySessionState::Running,
            SessionLifecycleState::Starting,
        ),
        "running" => (
            RegistrySessionState::Running,
            SessionLifecycleState::Running,
        ),
        "stopping" => (
            RegistrySessionState::Stopping,
            SessionLifecycleState::Stopping,
        ),
        "exited" => (
            RegistrySessionState::Exited,
            SessionLifecycleState::Exited {
                code: description.get::<Option<i32>>("code")?,
            },
        ),
        "failed" => (
            RegistrySessionState::Stale,
            SessionLifecycleState::Failed {
                reason: description
                    .get::<Option<String>>("reason")?
                    .unwrap_or_else(|| "failed".to_string()),
            },
        ),
        other => {
            return Err(mlua::Error::runtime(format!(
                "unknown session state {other:?}; use starting, running, stopping, exited, or failed"
            )));
        }
    };
    Ok(session_record(&id, registry, Some(lifecycle)))
}

fn sessions_from_lua(list: &Table) -> mlua::Result<Vec<SessionLifecycleRecord>> {
    list.sequence_values::<Table>()
        .map(|description| session_from_lua(&description?))
        .collect()
}

/// Replace each envelope's byte-array body with a string, so a spec compares
/// text.
fn envelope_json(envelope: &impl serde::Serialize) -> serde_json::Value {
    let mut value = serde_json::to_value(envelope).unwrap_or(serde_json::Value::Null);
    if let Some(body) = value.pointer_mut("/payload/body")
        && let Some(bytes) = body.as_array().and_then(|items| {
            items
                .iter()
                .map(|item| item.as_u64().and_then(|byte| u8::try_from(byte).ok()))
                .collect::<Option<Vec<u8>>>()
        })
        && let Ok(text) = String::from_utf8(bytes)
    {
        *body = serde_json::Value::String(text);
    }
    value
}

fn json_of(lua: &Lua, value: Value) -> mlua::Result<serde_json::Value> {
    if matches!(value, Value::Nil) {
        return Ok(serde_json::Value::Null);
    }
    lua.from_value(value)
}

fn json_equal(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    use serde_json::Value::{Array, Number, Object};
    match (left, right) {
        (Number(a), Number(b)) => a.as_f64() == b.as_f64(),
        (Array(a), Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| json_equal(x, y))
        }
        (Object(a), Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, x)| b.get(key).is_some_and(|y| json_equal(x, y)))
        }
        _ => left == right,
    }
}

/// `expected` is a subset of `actual`: every expected key is present and
/// matches; arrays match element by element.
fn json_matches(actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
    use serde_json::Value::{Array, Object};
    match (actual, expected) {
        (Object(a), Object(e)) => e
            .iter()
            .all(|(key, x)| a.get(key).is_some_and(|y| json_matches(y, x))),
        (Array(a), Array(e)) => {
            a.len() == e.len() && a.iter().zip(e).all(|(x, y)| json_matches(x, y))
        }
        _ => json_equal(actual, expected),
    }
}

fn pretty(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn build_driver<'scope, 'env>(
    lua: &Lua,
    scope: &'scope Scope<'scope, 'env>,
    kit: &'env RefCell<KitHub>,
    watched: &'env RefCell<HashSet<String>>,
    plugin_directory: &'env Path,
) -> mlua::Result<Table> {
    let t = lua.create_table()?;

    let load = move |lua: &Lua, path: &str| -> mlua::Result<Result<Table, Table>> {
        let directory: PathBuf = plugin_directory.join(path);
        let response = kit
            .borrow_mut()
            .enable_package(&directory)
            .map_err(kit_error)?;
        if let Some(error) = &response.error {
            let detail = lua.create_table()?;
            detail.set("kind", error.code.clone())?;
            detail.set("message", error.message.clone())?;
            return Ok(Err(detail));
        }
        let name = package_name(&directory)?;
        Ok(Ok(plugin_table(lua, scope, kit, watched, name)?))
    };
    let load = Arc::new(load);

    let strict = Arc::clone(&load);
    t.set(
        "load",
        scope.create_function(move |lua, (_, path): (Value, String)| {
            match strict(lua, &path)? {
                Ok(plugin) => Ok(plugin),
                Err(error) => {
                    let kind: String = error.get("kind")?;
                    let message: String = error.get("message")?;
                    Err(mlua::Error::runtime(format!(
                        "the plugin at {path:?} failed to load: {kind}: {message}"
                    )))
                }
            }
        })?,
    )?;
    let lenient = Arc::clone(&load);
    t.set(
        "try_load",
        scope.create_function(move |lua, (_, path): (Value, String)| {
            match lenient(lua, &path)? {
                Ok(plugin) => Ok((Value::Table(plugin), Value::Nil)),
                Err(error) => Ok((Value::Nil, Value::Table(error))),
            }
        })?,
    )?;

    t.set(
        "sessions_baseline",
        scope.create_function(move |_, (_, sessions): (Value, Table)| {
            kit.borrow_mut()
                .sessions_baseline(sessions_from_lua(&sessions)?)
                .map_err(kit_error)
        })?,
    )?;
    t.set(
        "session_upsert",
        scope.create_function(move |_, (_, session): (Value, Table)| {
            kit.borrow_mut()
                .session_upsert(session_from_lua(&session)?)
                .map_err(kit_error)
        })?,
    )?;
    t.set(
        "session_remove",
        scope.create_function(move |_, (_, id): (Value, String)| {
            kit.borrow_mut().session_remove(&id).map_err(kit_error)
        })?,
    )?;
    t.set(
        "settle",
        scope.create_function(move |_, _: Value| kit.borrow_mut().settle().map_err(kit_error))?,
    )?;
    t.set(
        "advance",
        scope.create_function(move |lua, (_, ms): (Value, u64)| {
            let fired = kit.borrow_mut().advance(ms).map_err(kit_error)?;
            let list = lua.create_table()?;
            for timer in fired {
                let item = lua.create_table()?;
                item.set("package", timer.package)?;
                item.set("resource_id", timer.resource_id)?;
                item.set("sequence", timer.sequence)?;
                list.push(item)?;
            }
            Ok(list)
        })?,
    )?;
    t.set(
        "request",
        scope.create_function(move |lua, (_, request): (Value, Value)| {
            let request: DaemonRequest = lua.from_value(request).map_err(|error| {
                mlua::Error::runtime(format!(
                    "t:request takes a DaemonRequest table with a type tag: {error}"
                ))
            })?;
            let response = kit.borrow_mut().request(request).map_err(kit_error)?;
            response_table(lua, &response)
        })?,
    )?;
    t.set(
        "receive_routed",
        scope.create_function(move |lua, (_, session_id): (Value, String)| {
            let outcome = kit
                .borrow()
                .receive_routed(
                    EnvelopeTarget::Session {
                        session_id: SessionId(session_id),
                    },
                    None,
                    256,
                )
                .map_err(kit_error)?;
            let list = lua.create_table()?;
            for envelope in &outcome.envelopes {
                list.push(to_lua(lua, &envelope_json(envelope))?)?;
            }
            Ok(list)
        })?,
    )?;
    t.set(
        "ack_routed",
        scope.create_function(
            move |_, (_, session_id, envelope_id): (Value, String, String)| {
                kit.borrow()
                    .ack_routed(
                        EnvelopeTarget::Session {
                            session_id: SessionId(session_id),
                        },
                        EnvelopeId(envelope_id),
                    )
                    .map(|state| format!("{state:?}"))
                    .map_err(kit_error)
            },
        )?,
    )?;

    // Doubles below Hub policy need slice 6 (gate G3).
    t.set(
        "double",
        scope.create_function(|_, _: MultiValue| -> mlua::Result<()> {
            Err(unsupported_error("http/fs/process doubles", "G3"))
        })?,
    )?;

    t.set(
        "eq",
        scope.create_function(
            |lua, (_, actual, expected, message): (Value, Value, Value, Option<String>)| {
                let actual = json_of(lua, actual)?;
                let expected = json_of(lua, expected)?;
                if json_equal(&actual, &expected) {
                    return Ok(());
                }
                Err(mlua::Error::runtime(format!(
                    "{}expected equal values\n  actual:   {}\n  expected: {}",
                    message.map(|text| format!("{text}: ")).unwrap_or_default(),
                    pretty(&actual),
                    pretty(&expected)
                )))
            },
        )?,
    )?;
    t.set(
        "match",
        scope.create_function(
            |lua, (_, actual, expected, message): (Value, Value, Value, Option<String>)| {
                let actual = json_of(lua, actual)?;
                let expected = json_of(lua, expected)?;
                if json_matches(&actual, &expected) {
                    return Ok(());
                }
                Err(mlua::Error::runtime(format!(
                    "{}the value does not contain the expected fields\n  actual:   {}\n  expected: {}",
                    message.map(|text| format!("{text}: ")).unwrap_or_default(),
                    pretty(&actual),
                    pretty(&expected)
                )))
            },
        )?,
    )?;
    t.set(
        "ok",
        scope.create_function(|_, (_, value, message): (Value, Value, Option<String>)| {
            if matches!(value, Value::Nil | Value::Boolean(false)) {
                return Err(mlua::Error::runtime(
                    message.unwrap_or_else(|| "expected a truthy value".to_string()),
                ));
            }
            Ok(())
        })?,
    )?;
    Ok(t)
}

fn package_name(directory: &Path) -> mlua::Result<String> {
    let manifest = directory.join("botster-package.json");
    let bytes = std::fs::read(&manifest)
        .map_err(|error| mlua::Error::runtime(format!("{}: {error}", manifest.display())))?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| mlua::Error::runtime(format!("{}: {error}", manifest.display())))?;
    value
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| mlua::Error::runtime(format!("{} has no name", manifest.display())))
}

fn plugin_table<'scope, 'env>(
    lua: &Lua,
    scope: &'scope Scope<'scope, 'env>,
    kit: &'env RefCell<KitHub>,
    watched: &'env RefCell<HashSet<String>>,
    name: String,
) -> mlua::Result<Table> {
    let p = lua.create_table()?;
    p.set("name", name.clone())?;

    let tool_name = name.clone();
    p.set(
        "call_tool",
        scope.create_function(
            move |lua, (_, tool, arguments, options): (Value, String, Value, Option<Table>)| {
                let _ = &tool_name;
                if let Some(options) = options {
                    // The Hub has no verified caller for plugin tools yet.
                    for feature in ["caller", "token"] {
                        if !matches!(options.get::<Value>(feature)?, Value::Nil) {
                            return unsupported(lua, feature, "G1");
                        }
                    }
                }
                let arguments = match arguments {
                    Value::Nil => serde_json::json!({}),
                    other => json_of(lua, other)?,
                };
                let response = kit
                    .borrow_mut()
                    .call_tool(&tool, arguments)
                    .map_err(kit_error)?;
                response_table(lua, &response)
            },
        )?,
    )?;

    let db_name = name.clone();
    p.set(
        "db_get",
        scope.create_function(move |lua, (_, key): (Value, String)| {
            let records = kit.borrow().plugin_db(&db_name).map_err(kit_error)?;
            match records.get(&key) {
                Some(payload) => to_lua(lua, payload),
                None => Ok(Value::Nil),
            }
        })?,
    )?;
    let db_all_name = name.clone();
    p.set(
        "db",
        scope.create_function(move |lua, _: Value| {
            let records = kit.borrow().plugin_db(&db_all_name).map_err(kit_error)?;
            serialize(lua, &records)
        })?,
    )?;

    p.set(
        "entities",
        scope.create_function(move |lua, (_, entity_type): (Value, String)| {
            // The first read subscribes as a client does; the Hub replies
            // with the current snapshot, then every later frame arrives.
            if watched.borrow_mut().insert(entity_type.clone()) {
                let mut kit = kit.borrow_mut();
                kit.subscribe_entities(&entity_type).map_err(kit_error)?;
                kit.settle().map_err(kit_error)?;
            }
            let frames = kit.borrow().entity_frames(&entity_type);
            serialize(lua, &frames)
        })?,
    )?;

    let events_name = name.clone();
    p.set(
        "emitted_events",
        scope.create_function(move |lua, _: Value| {
            let events = kit.borrow().emitted_events().map_err(kit_error)?;
            let own: Vec<_> = events
                .into_iter()
                .filter(|event| {
                    event.get("owner").and_then(|owner| owner.as_str()) == Some(&events_name)
                })
                .collect();
            serialize(lua, &own)
        })?,
    )?;

    p.set(
        "routed",
        scope.create_function(move |lua, (_, session_id): (Value, String)| {
            let envelopes = kit
                .borrow()
                .routed(EnvelopeTarget::Session {
                    session_id: SessionId(session_id),
                })
                .map_err(kit_error)?;
            let list = lua.create_table()?;
            for envelope in &envelopes {
                list.push(to_lua(lua, &envelope_json(envelope))?)?;
            }
            Ok(list)
        })?,
    )?;

    let logs_name = name;
    p.set(
        "logs",
        scope.create_function(move |lua, _: Value| {
            let logs = kit.borrow_mut().logs(&logs_name).map_err(kit_error)?;
            serialize(lua, &logs.records)
        })?,
    )?;
    p.set(
        "tools",
        scope.create_function(move |lua, _: Value| {
            let tools = kit.borrow_mut().list_tools().map_err(kit_error)?;
            serialize(lua, &tools)
        })?,
    )?;

    // Views and posts need platform slices 4b and 5 (gates G4, G5).
    p.set(
        "views",
        scope.create_function(|_, _: MultiValue| -> mlua::Result<()> {
            Err(unsupported_error("views", "G4"))
        })?,
    )?;
    p.set(
        "posts",
        scope.create_function(|_, _: MultiValue| -> mlua::Result<()> {
            Err(unsupported_error("posts and doorbells", "G5"))
        })?,
    )?;
    Ok(p)
}
