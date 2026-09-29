//! `botster.capabilities.sessions`: read the Hub's sessions from a plugin.
//!
//! The Hub owner answers each read from its own session projection in one
//! bounded pass (see `plugin_host_call`); no second copy of the rows exists. A
//! session is named `{ hub_id, session_id }`. `hub_id` defaults to this Hub,
//! and any other Hub is refused with `remote_hub_unsupported`.
//!
//! Grants, on the `session_actions` surface: `session_read:any` reads every
//! session. `session_read` reads only the sessions the plugin spawned; the Hub
//! does not record spawning plugins yet, so that width sees none.

use std::sync::Arc;
use std::time::Duration;

use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde_json::json;

use super::result::{self, ErrorKind};
use crate::plugin_host_call::{self, HostCallError, PluginHostCall, PluginHostReply};
use crate::runtime::{HubSessionTypeSpawner, SharedPackageRegistry, SharedSpawnTargets};
use botster_hub_client::DaemonSessionEntity;

const SCOPE_OWN: &str = "session_read";
const SCOPE_ANY: &str = "session_read:any";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Width {
    None,
    Own,
    Any,
}

fn width(packages: &SharedPackageRegistry, plugin_key: &str) -> Width {
    let registry = packages.current();
    let Some(record) = registry.package(plugin_key) else {
        return Width::None;
    };
    if !matches!(record.state, crate::packages::PackageState::Enabled) {
        return Width::None;
    }
    let mut own = false;
    for capability in &record.manifest.capabilities {
        if capability.surface != botster_core::CapabilitySurface::SessionActions {
            continue;
        }
        match capability.scope.as_deref() {
            Some(SCOPE_ANY) => return Width::Any,
            Some(SCOPE_OWN) => own = true,
            _ => {}
        }
    }
    if own { Width::Own } else { Width::None }
}

fn field(args: &Value, name: &str) -> Value {
    match args {
        Value::Table(table) => table.raw_get::<Value>(name).unwrap_or(Value::Nil),
        _ => Value::Nil,
    }
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => text.to_str().ok().map(|text| text.to_string()),
        _ => None,
    }
}

/// The local hub id, read from the published Hub state.
fn local_hub_id(state: &SharedSpawnTargets) -> Option<String> {
    state
        .try_snapshot()
        .ok()
        .map(|(_, state)| state.host.id.clone())
}

/// `Ok(())` when `hub_id` is absent or names this Hub.
fn require_local(lua: &Lua, hub_id: &Value, local: &str) -> Result<(), mlua::Result<Table>> {
    match hub_id {
        Value::Nil => Ok(()),
        Value::String(text) if text.to_str().is_ok_and(|text| text == local) => Ok(()),
        Value::String(_) => Err(result::err(
            lua,
            ErrorKind::RemoteHubUnsupported,
            "only the local hub is supported",
        )),
        _ => Err(result::err(
            lua,
            ErrorKind::InvalidRequest,
            "hub_id must be a string",
        )),
    }
}

fn denied(lua: &Lua) -> mlua::Result<Table> {
    result::err(
        lua,
        ErrorKind::CapabilityDenied,
        "the plugin's package was not admitted with the required capability",
    )
}

fn row(entity: &DaemonSessionEntity, hub_id: &str) -> serde_json::Value {
    let mut value = serde_json::to_value(entity).expect("serialize a session entity");
    let fields = value
        .as_object_mut()
        .expect("a session entity is an object");
    if let Some(session_id) = fields.remove("session_uuid") {
        fields.insert("session_id".to_string(), session_id);
    }
    fields.insert("hub_id".to_string(), json!(hub_id));
    value
}

/// Ask the Hub owner; a refusal is already a Lua result table.
fn ask(
    lua: &Lua,
    spawner: &HubSessionTypeSpawner,
    call: PluginHostCall,
) -> Result<PluginHostReply, mlua::Result<Table>> {
    // Same deadline as the other plugin-to-owner host calls.
    let timeout = Duration::from_millis(super::COORDINATION_REQUEST_TIMEOUT_MS);
    match plugin_host_call::call(spawner.owner_sender(), call, timeout) {
        Ok(PluginHostReply::NotReady) => Err(result::err(
            lua,
            ErrorKind::Unavailable,
            "the sessions view is not ready",
        )),
        Ok(reply) => Ok(reply),
        Err(HostCallError::Backpressured) => Err(result::err(
            lua,
            ErrorKind::Backpressured,
            "the hub owner is busy; retry",
        )),
        Err(HostCallError::TimedOut) => Err(result::err(
            lua,
            ErrorKind::TimedOut,
            "the hub owner did not answer in time",
        )),
        Err(HostCallError::OwnerUnavailable) => Err(result::err(
            lua,
            ErrorKind::Unavailable,
            "the hub owner is not running",
        )),
    }
}

pub(super) fn table(
    lua: &Lua,
    plugin_key: String,
    spawner: Arc<HubSessionTypeSpawner>,
    packages: SharedPackageRegistry,
    state: SharedSpawnTargets,
) -> Result<Table, mlua::Error> {
    let table = lua.create_table()?;
    {
        let (spawner, packages, state, plugin_key) = (
            Arc::clone(&spawner),
            Arc::clone(&packages),
            state.clone(),
            plugin_key.clone(),
        );
        table.set(
            "list",
            lua.create_function(move |lua, args: Value| {
                let Some(local) = local_hub_id(&state) else {
                    return result::err(
                        lua,
                        ErrorKind::Unavailable,
                        "the hub state is unavailable",
                    );
                };
                if let Err(refusal) = require_local(lua, &field(&args, "hub_id"), &local) {
                    return refusal;
                }
                let after = match field(&args, "after") {
                    Value::Nil => None,
                    Value::String(text) => text.to_str().ok().map(|text| text.to_string()),
                    _ => {
                        return result::err(
                            lua,
                            ErrorKind::InvalidRequest,
                            "after must be a session id string",
                        );
                    }
                };
                let wanted = match text(&field(&args, "owner")).as_deref() {
                    None => Width::Own,
                    Some("any") => Width::Any,
                    Some(_) => {
                        return result::err(
                            lua,
                            ErrorKind::InvalidRequest,
                            "owner must be 'any' or absent",
                        );
                    }
                };
                let granted = width(&packages, &plugin_key);
                if granted == Width::None || (wanted == Width::Any && granted != Width::Any) {
                    return denied(lua);
                }
                let reply = match ask(lua, &spawner, PluginHostCall::SessionsPage { after }) {
                    Ok(reply) => reply,
                    Err(refusal) => return refusal,
                };
                let PluginHostReply::SessionsPage { rows, next_after } = reply else {
                    return result::err(lua, ErrorKind::Failed, "unexpected owner reply");
                };
                // Only rows the plugin may see are returned, and the cursor is
                // one of them. Own-session reads see none until spawning
                // plugins are recorded, so they get no rows and no cursor.
                let (rows, next_after) = if wanted == Width::Own {
                    (Vec::new(), None)
                } else {
                    (rows, next_after)
                };
                let rows: Vec<serde_json::Value> =
                    rows.iter().map(|entity| row(entity, &local)).collect();
                let value = lua.to_value(&json!({
                    "sessions": rows,
                    "next_after": next_after,
                }))?;
                result::ok(lua, value)
            })?,
        )?;
    }
    table.set(
        "get",
        lua.create_function(move |lua, args: Value| {
            let Some(local) = local_hub_id(&state) else {
                return result::err(lua, ErrorKind::Unavailable, "the hub state is unavailable");
            };
            // A plain string names a session on this Hub.
            let reference = field(&args, "session");
            let (hub_id, session_id) = match &reference {
                Value::String(text) => (Value::Nil, Value::String(text.clone())),
                Value::Table(table) => (
                    table.raw_get::<Value>("hub_id").unwrap_or(Value::Nil),
                    table.raw_get::<Value>("session_id").unwrap_or(Value::Nil),
                ),
                _ => {
                    return result::err(
                        lua,
                        ErrorKind::InvalidRequest,
                        "sessions.get takes { session = { hub_id?, session_id } }",
                    );
                }
            };
            if let Err(refusal) = require_local(lua, &hub_id, &local) {
                return refusal;
            }
            let Some(session_id) = text(&session_id) else {
                return result::err(
                    lua,
                    ErrorKind::InvalidRequest,
                    "session_id must be a string",
                );
            };
            let granted = width(&packages, &plugin_key);
            if granted == Width::None {
                return denied(lua);
            }
            // Ask first, so an unready projection is `unavailable` for every
            // width; then show only what the width allows.
            let reply = match ask(lua, &spawner, PluginHostCall::SessionGet { session_id }) {
                Ok(reply) => reply,
                Err(refusal) => return refusal,
            };
            let PluginHostReply::Session(entity) = reply else {
                return result::err(lua, ErrorKind::Failed, "unexpected owner reply");
            };
            match entity {
                // Own-session reads see nothing until spawning plugins are recorded.
                Some(entity) if granted == Width::Any => {
                    let value = lua.to_value(&row(&entity, &local))?;
                    result::ok(lua, value)
                }
                _ => result::err(lua, ErrorKind::NotFound, "the session was not found"),
            }
        })?,
    )?;
    Ok(table)
}
