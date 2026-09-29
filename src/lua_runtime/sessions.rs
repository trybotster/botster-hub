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
//!
//! Memory: each call funds its request, its strings and its reply channel from
//! the plugin's callback account before it reads an argument, and holds that
//! charge until it returns. The owner funds the rows it copies, and the JSON
//! copy made here, before it allocates them. The reply carries that charge, so
//! it stays charged until the Lua result is built and the reply drops.

use std::sync::Arc;
use std::time::Duration;

use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde_json::json;

use super::result::{self, ErrorKind};
use crate::daemon::control::message::ControlSender;
use crate::lua_memory::{LuaCallbackCharge, LuaCallbackGrowthError, LuaMemoryAccount};
use crate::plugin_host_call::{self, HostCallError, HostOutcome, PluginHostCall, Refusal};
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

/// A string argument. The lease funds its bytes before it is copied.
enum Text {
    Absent,
    Present(String),
    Invalid,
    Unfunded(LuaCallbackGrowthError),
}

fn field(args: &Value, name: &str) -> Value {
    match args {
        Value::Table(table) => table.raw_get::<Value>(name).unwrap_or(Value::Nil),
        _ => Value::Nil,
    }
}

fn text(value: &Value, lease: &mut LuaCallbackCharge) -> Text {
    match value {
        Value::Nil => Text::Absent,
        Value::String(text) => {
            let bytes = text.as_bytes();
            if std::str::from_utf8(&bytes).is_err() {
                return Text::Invalid;
            }
            if let Err(error) = lease.grow(bytes.len()) {
                return Text::Unfunded(error);
            }
            Text::Present(String::from_utf8_lossy(&bytes).into_owned())
        }
        _ => Text::Invalid,
    }
}

fn unfunded_refusal(lua: &Lua, error: LuaCallbackGrowthError) -> mlua::Result<Table> {
    match error {
        LuaCallbackGrowthError::Capacity(_) => capacity_refusal(lua),
        LuaCallbackGrowthError::Quota | LuaCallbackGrowthError::Sealed => result::err(
            lua,
            ErrorKind::QuotaExceeded,
            "the argument is larger than one callback may hold",
        ),
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

/// What one call needs. The Lua functions build it from the runtime; the tests
/// build it by hand.
struct Context {
    owner: Arc<dyn Fn() -> Option<ControlSender> + Send + Sync>,
    memory: Arc<LuaMemoryAccount>,
    local_hub_id: String,
    granted: Width,
}

/// Fund the call's fixed allocations before any argument is read. A refusal
/// is already a Lua result table.
fn fund(
    lua: &Lua,
    memory: &Arc<LuaMemoryAccount>,
) -> Result<LuaCallbackCharge, mlua::Result<Table>> {
    let Some(bytes) = plugin_host_call::fixed_call_bytes() else {
        return Err(result::err(
            lua,
            ErrorKind::Failed,
            "the call's size is not computable",
        ));
    };
    memory
        .reserve_callback_total(bytes)
        .map_err(|_| capacity_refusal(lua))
}

fn capacity_refusal(lua: &Lua) -> mlua::Result<Table> {
    result::err(
        lua,
        ErrorKind::Backpressured,
        "the plugin's callback memory is full; retry",
    )
}

/// Ask the Hub owner; a refusal is already a Lua result table.
fn ask(
    lua: &Lua,
    context: &Context,
    call: PluginHostCall,
    lease: LuaCallbackCharge,
) -> Result<(HostOutcome, LuaCallbackCharge), mlua::Result<Table>> {
    // Same deadline as the other plugin-to-owner host calls.
    let timeout = Duration::from_millis(super::COORDINATION_REQUEST_TIMEOUT_MS);
    let answer = plugin_host_call::call(
        (context.owner)(),
        call,
        Arc::clone(&context.memory),
        context.local_hub_id.len(),
        lease,
        timeout,
    );
    match answer {
        Ok(reply) => match reply.outcome {
            HostOutcome::NotReady => Err(result::err(
                lua,
                ErrorKind::Unavailable,
                "the sessions view is not ready",
            )),
            HostOutcome::Refused(Refusal::Capacity) => Err(capacity_refusal(lua)),
            HostOutcome::Refused(Refusal::Quota) => Err(result::err(
                lua,
                ErrorKind::QuotaExceeded,
                "the session rows are larger than one callback may hold",
            )),
            outcome => Ok((outcome, reply.lease)),
        },
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

fn list(lua: &Lua, context: &Context, args: Value) -> mlua::Result<Table> {
    let mut lease = match fund(lua, &context.memory) {
        Ok(charge) => charge,
        Err(refusal) => return refusal,
    };
    let local = context.local_hub_id.as_str();
    if let Err(refusal) = require_local(lua, &field(&args, "hub_id"), local) {
        return refusal;
    }
    let after = match text(&field(&args, "after"), &mut lease) {
        Text::Absent => None,
        Text::Present(after) => Some(after),
        Text::Unfunded(error) => return unfunded_refusal(lua, error),
        Text::Invalid => {
            return result::err(
                lua,
                ErrorKind::InvalidRequest,
                "after must be a session id string",
            );
        }
    };
    // Compared in place: the value is never copied.
    let wanted = match field(&args, "owner") {
        Value::Nil => Width::Own,
        Value::String(owner) if &*owner.as_bytes() == b"any" => Width::Any,
        _ => {
            return result::err(
                lua,
                ErrorKind::InvalidRequest,
                "owner must be 'any' or absent",
            );
        }
    };
    if context.granted == Width::None || (wanted == Width::Any && context.granted != Width::Any) {
        return denied(lua);
    }
    // The lease now covers the rows and their JSON copy; keep it until the
    // Lua result is built.
    let (outcome, _lease) = match ask(lua, context, PluginHostCall::SessionsPage { after }, lease) {
        Ok(answered) => answered,
        Err(refusal) => return refusal,
    };
    let HostOutcome::SessionsPage { rows, next_after } = outcome else {
        return result::err(lua, ErrorKind::Failed, "unexpected owner reply");
    };
    // Only rows the plugin may see are returned, and the cursor is one of
    // them. Own-session reads see none until spawning plugins are recorded,
    // so they get no rows and no cursor.
    let (rows, next_after) = if wanted == Width::Own {
        (Vec::new(), None)
    } else {
        (rows, next_after)
    };
    // Move each JSON row into the response: the funded peak holds the Rust
    // rows and one JSON copy, never a second JSON copy.
    let mut sessions = Vec::with_capacity(rows.len());
    for entity in rows {
        sessions.push(row(&entity, local));
    }
    let mut response = serde_json::Map::with_capacity(2);
    response.insert("sessions".to_string(), serde_json::Value::Array(sessions));
    response.insert(
        "next_after".to_string(),
        next_after.map_or(serde_json::Value::Null, serde_json::Value::String),
    );
    let value = lua.to_value(&serde_json::Value::Object(response))?;
    result::ok(lua, value)
}

fn get(lua: &Lua, context: &Context, args: Value) -> mlua::Result<Table> {
    let mut lease = match fund(lua, &context.memory) {
        Ok(charge) => charge,
        Err(refusal) => return refusal,
    };
    let local = context.local_hub_id.as_str();
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
    if let Err(refusal) = require_local(lua, &hub_id, local) {
        return refusal;
    }
    let session_id = match text(&session_id, &mut lease) {
        Text::Present(session_id) => session_id,
        Text::Unfunded(error) => return unfunded_refusal(lua, error),
        Text::Absent | Text::Invalid => {
            return result::err(
                lua,
                ErrorKind::InvalidRequest,
                "session_id must be a string",
            );
        }
    };
    if context.granted == Width::None {
        return denied(lua);
    }
    // Ask first, so an unready projection is `unavailable` for every width;
    // then show only what the width allows.
    let (outcome, _lease) = match ask(
        lua,
        context,
        PluginHostCall::SessionGet { session_id },
        lease,
    ) {
        Ok(answered) => answered,
        Err(refusal) => return refusal,
    };
    let HostOutcome::Session(entity) = outcome else {
        return result::err(lua, ErrorKind::Failed, "unexpected owner reply");
    };
    match entity {
        // Own-session reads see nothing until spawning plugins are recorded.
        Some(entity) if context.granted == Width::Any => {
            let value = lua.to_value(&row(&entity, local))?;
            result::ok(lua, value)
        }
        _ => result::err(lua, ErrorKind::NotFound, "the session was not found"),
    }
}

pub(super) fn table(
    lua: &Lua,
    plugin_key: String,
    spawner: Arc<HubSessionTypeSpawner>,
    packages: SharedPackageRegistry,
    state: SharedSpawnTargets,
    memory: Arc<LuaMemoryAccount>,
) -> Result<Table, mlua::Error> {
    let table = lua.create_table()?;
    // One closure builds the call's context from the runtime's current state.
    let context = Arc::new(move |lua: &Lua| -> Result<Context, mlua::Result<Table>> {
        let Some(local_hub_id) = local_hub_id(&state) else {
            return Err(result::err(
                lua,
                ErrorKind::Unavailable,
                "the hub state is unavailable",
            ));
        };
        let spawner = Arc::clone(&spawner);
        Ok(Context {
            owner: Arc::new(move || spawner.owner_sender()),
            memory: Arc::clone(&memory),
            local_hub_id,
            granted: width(&packages, &plugin_key),
        })
    });
    let list_context = Arc::clone(&context);
    table.set(
        "list",
        lua.create_function(move |lua, args: Value| match list_context(lua) {
            Ok(context) => list(lua, &context, args),
            Err(refusal) => refusal,
        })?,
    )?;
    table.set(
        "get",
        lua.create_function(move |lua, args: Value| match context(lua) {
            Ok(context) => get(lua, &context, args),
            Err(refusal) => refusal,
        })?,
    )?;
    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin_host_call::tests as fixtures;
    use crate::session_projection::SessionProjection;

    const LOCAL: &str = "hub-local";

    fn context(memory: Arc<LuaMemoryAccount>, granted: Width) -> Context {
        Context {
            owner: Arc::new(|| panic!("the owner must not be asked")),
            memory,
            local_hub_id: LOCAL.to_string(),
            granted,
        }
    }

    /// An owner thread that answers each call from `projection`.
    fn owner_over(
        projection: SessionProjection,
        memory_check: Option<Arc<LuaMemoryAccount>>,
    ) -> Arc<dyn Fn() -> Option<ControlSender> + Send + Sync> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        std::thread::spawn(move || {
            while let Some(message) = receiver.blocking_recv() {
                if let crate::daemon::control::message::ControlMessage::PluginHostCall(request) =
                    message
                {
                    let request = *request;
                    if let Some(memory) = &memory_check {
                        assert!(
                            Arc::ptr_eq(memory, &request.memory),
                            "the request carries the caller's account"
                        );
                    }
                    let (reply, sender) = plugin_host_call::answer(&projection, request);
                    let _ = sender.try_send(reply);
                }
            }
        });
        Arc::new(move || Some(sender.clone()))
    }

    fn kind(result: &Table) -> String {
        let error: Table = result.get("error").expect("an error result");
        error.get("kind").expect("an error kind")
    }

    #[test]
    fn an_exhausted_account_refuses_before_any_argument_is_read_or_owner_is_asked() {
        let lua = Lua::new();
        let fixed = plugin_host_call::fixed_call_bytes().expect("fixed bytes");
        // The account holds one call's fixed bytes, and one is already held.
        let memory = fixtures::account(fixed, fixed);
        let held = memory.reserve_callback_total(fixed).expect("held");
        let context = context(Arc::clone(&memory), Width::Any);
        // Arguments that would be refused otherwise show the account refuses first.
        let list = super::list(&lua, &context, Value::Nil).unwrap();
        assert_eq!(kind(&list), "backpressured");
        let get = super::get(&lua, &context, Value::Nil).unwrap();
        assert_eq!(kind(&get), "backpressured");
        assert_eq!(memory.usage().1, fixed, "a refusal charges nothing");
        drop(held);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn a_call_releases_every_charge_when_it_returns_on_each_path() {
        let lua = Lua::new();
        let memory = fixtures::account(8 * 1024 * 1024, 64 * 1024 * 1024);
        let projection = fixtures::sealed(&["s00", "s01", "s02"]);
        let mut context = context(Arc::clone(&memory), Width::Any);
        context.owner = owner_over(projection, Some(Arc::clone(&memory)));
        let args = lua
            .to_value(&json!({ "owner": "any" }))
            .expect("list arguments");
        let listed = list(&lua, &context, args).unwrap();
        assert_eq!(listed.get::<bool>("ok").unwrap(), true);
        assert_eq!(
            memory.usage().1,
            0,
            "a served list releases fixed and reply charges"
        );
        let args = lua
            .to_value(&json!({ "session": { "session_id": "s01" } }))
            .expect("get arguments");
        let got = get(&lua, &context, args).unwrap();
        assert_eq!(got.get::<bool>("ok").unwrap(), true);
        assert_eq!(
            memory.usage().1,
            0,
            "a served get releases fixed and reply charges"
        );
        // The owner is not running: the call fails and releases.
        context.owner = Arc::new(|| None);
        let args = lua
            .to_value(&json!({ "owner": "any" }))
            .expect("list arguments");
        let unavailable = list(&lua, &context, args).unwrap();
        assert_eq!(kind(&unavailable), "unavailable");
        assert_eq!(
            memory.usage().1,
            0,
            "a failed call releases its fixed charge"
        );
    }

    #[test]
    fn an_account_that_cannot_fund_the_rows_refuses_and_holds_nothing() {
        let lua = Lua::new();
        let fixed = plugin_host_call::fixed_call_bytes().expect("fixed bytes");
        let projection = fixtures::sealed(&["s00", "s01"]);
        // The ceiling holds one row, but the fixed bytes already take part of
        // the total, so the row cannot be funded.
        let one_row = plugin_host_call::row_funding_bytes(
            SessionProjection::entity_bound_bytes(&fixtures::record("s00")),
            LOCAL.len(),
        );
        let memory = fixtures::account(fixed + one_row, fixed + one_row);
        // Another callback already holds one byte of the total.
        let _other = memory.reserve_callback_total(1).expect("other callback");
        let mut context = context(Arc::clone(&memory), Width::Any);
        context.owner = owner_over(projection, None);
        let args = lua
            .to_value(&json!({ "owner": "any" }))
            .expect("list arguments");
        let refused = list(&lua, &context, args).unwrap();
        assert_eq!(kind(&refused), "backpressured");
        assert_eq!(
            memory.usage().1,
            1,
            "the owner made no copy; only the other callback's byte stays charged"
        );
    }

    #[test]
    fn a_row_above_the_callback_ceiling_is_quota_exceeded() {
        let lua = Lua::new();
        let fixed = plugin_host_call::fixed_call_bytes().expect("fixed bytes");
        let projection = fixtures::sealed(&["s00"]);
        let memory = fixtures::account(fixed + 1024, 64 * 1024 * 1024);
        let mut context = context(Arc::clone(&memory), Width::Any);
        context.owner = owner_over(projection, None);
        let args = lua
            .to_value(&json!({ "owner": "any" }))
            .expect("list arguments");
        let refused = list(&lua, &context, args).unwrap();
        assert_eq!(kind(&refused), "quota_exceeded");
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn a_key_is_funded_by_its_length_before_it_is_copied() {
        let lua = Lua::new();
        let fixed = plugin_host_call::fixed_call_bytes().expect("fixed bytes");
        // The ceiling holds the fixed bytes and 100 more; the key has 200.
        let memory = fixtures::account(fixed + 100, 64 * 1024 * 1024);
        let small = context(Arc::clone(&memory), Width::Any);
        let args = lua
            .to_value(&json!({ "session": { "session_id": "x".repeat(200) } }))
            .expect("get arguments");
        let refused = get(&lua, &small, args).unwrap();
        assert_eq!(kind(&refused), "quota_exceeded");
        let args = lua
            .to_value(&json!({ "owner": "any", "after": "y".repeat(200) }))
            .expect("list arguments");
        let refused = list(&lua, &small, args).unwrap();
        assert_eq!(kind(&refused), "quota_exceeded");
        assert_eq!(memory.usage().1, 0);
        // A total that cannot hold the key is a retryable refusal.
        let memory = fixtures::account(8 * 1024 * 1024, 8 * 1024 * 1024);
        let _other = memory
            .reserve_callback_total(8 * 1024 * 1024 - fixed - 100)
            .expect("other callbacks hold most of the total");
        let context = context(Arc::clone(&memory), Width::Any);
        let args = lua
            .to_value(&json!({ "owner": "any", "after": "y".repeat(200) }))
            .expect("list arguments");
        let refused = list(&lua, &context, args).unwrap();
        assert_eq!(kind(&refused), "backpressured");
    }

    #[test]
    fn a_queued_request_stays_charged_after_the_deadline_and_releases_when_it_drops() {
        let lua = Lua::new();
        let memory = fixtures::account(8 * 1024 * 1024, 64 * 1024 * 1024);
        // An owner queue that nobody serves.
        let (sender, queue) = tokio::sync::mpsc::channel(8);
        let mut context = context(Arc::clone(&memory), Width::Any);
        context.owner = Arc::new(move || Some(sender.clone()));
        let fixed = plugin_host_call::fixed_call_bytes().expect("fixed bytes");
        let args = lua
            .to_value(&json!({ "owner": "any", "after": "s-key" }))
            .expect("list arguments");
        let timed_out = list(&lua, &context, args).unwrap();
        assert_eq!(kind(&timed_out), "timed_out");
        assert_eq!(
            memory.usage().1,
            fixed + "s-key".len(),
            "the queued request still holds its charge after the call returned"
        );
        drop(queue);
        assert_eq!(memory.usage().1, 0, "the charge releases with the request");
    }

    #[test]
    fn the_funding_bound_covers_the_rust_row_and_its_json_copy() {
        // A row with long metadata values and many traits.
        let mut record = fixtures::record("session-with-a-long-id-0123456789");
        let entries = &mut record.metadata.entries;
        entries.insert("botster.session_type.id".into(), "t".repeat(200));
        entries.insert("botster.session_type.source".into(), "s".repeat(200));
        entries.insert("botster.session_type.role".into(), "r".repeat(100));
        entries.insert("botster.session_type.interaction".into(), "i".repeat(100));
        entries.insert("botster.session_type.lifecycle".into(), "l".repeat(100));
        let traits: Vec<String> = (0..40).map(|index| format!("trait-{index}")).collect();
        entries.insert(
            "botster.session_type.traits".into(),
            serde_json::to_string(&traits).expect("traits json"),
        );
        let entity = SessionProjection::project_entity(&record);
        let json = row(&entity, LOCAL);
        let actual_entity = std::mem::size_of::<DaemonSessionEntity>()
            + entity.session_uuid.len()
            + entity.registry_state.len()
            + entity.lifecycle.as_ref().map_or(0, String::len)
            + entity.lifecycle_class.len()
            + entity.session_type_id.as_ref().map_or(0, String::len)
            + entity.session_type_source.as_ref().map_or(0, String::len)
            + entity.role.as_ref().map_or(0, String::len)
            + entity.interaction.as_ref().map_or(0, String::len)
            + entity
                .session_type_lifecycle
                .as_ref()
                .map_or(0, String::len)
            + entity
                .traits
                .iter()
                .map(|value| std::mem::size_of::<String>() + value.len())
                .sum::<usize>();
        let actual_json =
            crate::lua_memory::layout::json_value_retained_bytes(&json).expect("json bytes");
        let bound = SessionProjection::entity_bound_bytes(&record);
        assert!(
            bound >= actual_entity,
            "entity bound {bound} < actual {actual_entity}"
        );
        let funded = plugin_host_call::row_funding_bytes(bound, LOCAL.len());
        assert!(
            funded >= actual_entity + actual_json,
            "funded {funded} < actual {} (entity {actual_entity} + json {actual_json})",
            actual_entity + actual_json
        );
    }
}
