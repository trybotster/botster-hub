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

/// What one call needs. It borrows everything: building it allocates nothing,
/// so it is built after the call is funded and costs no memory of its own.
struct Context<'a> {
    owner: &'a (dyn Fn() -> Option<ControlSender> + Send + Sync),
    memory: &'a Arc<LuaMemoryAccount>,
    local_hub_id: &'a str,
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
        Arc::clone(context.memory),
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

/// Run `call` with the call's fixed bytes funded first. Nothing that can
/// allocate runs before this reserves; a refusal is already a Lua result table.
fn funded(
    lua: &Lua,
    memory: &Arc<LuaMemoryAccount>,
    call: impl FnOnce(LuaCallbackCharge) -> mlua::Result<Table>,
) -> mlua::Result<Table> {
    match fund(lua, memory) {
        Ok(lease) => call(lease),
        Err(refusal) => refusal,
    }
}

fn list(
    lua: &Lua,
    context: &Context,
    mut lease: LuaCallbackCharge,
    args: Value,
) -> mlua::Result<Table> {
    let local = context.local_hub_id;
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

fn get(
    lua: &Lua,
    context: &Context,
    mut lease: LuaCallbackCharge,
    args: Value,
) -> mlua::Result<Table> {
    let local = context.local_hub_id;
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

/// What the runtime gives every call. Cloning its `Arc` per closure happens
/// once, at table creation.
struct Shared {
    plugin_key: String,
    owner: Box<dyn Fn() -> Option<ControlSender> + Send + Sync>,
    packages: SharedPackageRegistry,
    state: SharedSpawnTargets,
    memory: Arc<LuaMemoryAccount>,
}

/// Fund the call, then build its context from borrows. The hub state is read
/// as a shared view (a reference count, no copy of the hub id).
fn enter(
    lua: &Lua,
    shared: &Shared,
    args: Value,
    call: fn(&Lua, &Context, LuaCallbackCharge, Value) -> mlua::Result<Table>,
) -> mlua::Result<Table> {
    funded(lua, &shared.memory, |lease| {
        let Ok((_, hub)) = shared.state.try_snapshot() else {
            return result::err(lua, ErrorKind::Unavailable, "the hub state is unavailable");
        };
        let context = Context {
            owner: &*shared.owner,
            memory: &shared.memory,
            local_hub_id: &hub.host.id,
            granted: width(&shared.packages, &shared.plugin_key),
        };
        call(lua, &context, lease, args)
    })
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
    let shared = Arc::new(Shared {
        plugin_key,
        owner: Box::new(move || spawner.owner_sender()),
        packages,
        state,
        memory,
    });
    let list_shared = Arc::clone(&shared);
    table.set(
        "list",
        lua.create_function(move |lua, args: Value| enter(lua, &list_shared, args, list))?,
    )?;
    table.set(
        "get",
        lua.create_function(move |lua, args: Value| enter(lua, &shared, args, get))?,
    )?;
    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin_host_call::tests as fixtures;
    use crate::session_projection::SessionProjection;

    const LOCAL: &str = "hub-local";

    /// The pieces of a call, owned, so a test can change one and call again.
    struct Harness {
        owner: Arc<dyn Fn() -> Option<ControlSender> + Send + Sync>,
        memory: Arc<LuaMemoryAccount>,
        granted: Width,
    }

    impl Harness {
        fn list(&self, lua: &Lua, args: Value) -> mlua::Result<Table> {
            funded(lua, &self.memory, |lease| {
                super::list(lua, &self.context(), lease, args)
            })
        }

        fn get(&self, lua: &Lua, args: Value) -> mlua::Result<Table> {
            funded(lua, &self.memory, |lease| {
                super::get(lua, &self.context(), lease, args)
            })
        }

        fn context(&self) -> Context<'_> {
            Context {
                owner: &*self.owner,
                memory: &self.memory,
                local_hub_id: LOCAL,
                granted: self.granted,
            }
        }
    }

    fn context(memory: Arc<LuaMemoryAccount>, granted: Width) -> Harness {
        Harness {
            owner: Arc::new(|| panic!("the owner must not be asked")),
            memory,
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
        let list = context.list(&lua, Value::Nil).unwrap();
        assert_eq!(kind(&list), "backpressured");
        let get = context.get(&lua, Value::Nil).unwrap();
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
        let listed = context.list(&lua, args).unwrap();
        assert_eq!(listed.get::<bool>("ok").unwrap(), true);
        assert_eq!(
            memory.usage().1,
            0,
            "a served list releases fixed and reply charges"
        );
        let args = lua
            .to_value(&json!({ "session": { "session_id": "s01" } }))
            .expect("get arguments");
        let got = context.get(&lua, args).unwrap();
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
        let unavailable = context.list(&lua, args).unwrap();
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
        let refused = context.list(&lua, args).unwrap();
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
        let refused = context.list(&lua, args).unwrap();
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
        let refused = small.get(&lua, args).unwrap();
        assert_eq!(kind(&refused), "quota_exceeded");
        let args = lua
            .to_value(&json!({ "owner": "any", "after": "y".repeat(200) }))
            .expect("list arguments");
        let refused = small.list(&lua, args).unwrap();
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
        let refused = context.list(&lua, args).unwrap();
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
        let timed_out = context.list(&lua, args).unwrap();
        assert_eq!(kind(&timed_out), "timed_out");
        assert_eq!(
            memory.usage().1,
            fixed + "s-key".len(),
            "the queued request still holds its charge after the call returned"
        );
        drop(queue);
        assert_eq!(memory.usage().1, 0, "the charge releases with the request");
    }

    /// The bytes a projected entity holds, by capacity: the entity, each string's
    /// capacity, the traits vector's capacity and each trait's capacity.
    fn actual_entity_bytes(entity: &DaemonSessionEntity) -> usize {
        let capacity = |value: &Option<String>| value.as_ref().map_or(0, String::capacity);
        std::mem::size_of::<DaemonSessionEntity>()
            + entity.session_uuid.capacity()
            + entity.registry_state.capacity()
            + capacity(&entity.lifecycle)
            + entity.lifecycle_class.capacity()
            + capacity(&entity.failure_reason)
            + capacity(&entity.session_type_id)
            + capacity(&entity.session_type_source)
            + capacity(&entity.role)
            + capacity(&entity.interaction)
            + capacity(&entity.session_type_lifecycle)
            + entity.traits.capacity() * std::mem::size_of::<String>()
            + entity.traits.iter().map(String::capacity).sum::<usize>()
    }

    fn assert_funded(record: &botster_core_daemon::SessionLifecycleRecord, what: &str) {
        let entity = SessionProjection::project_entity(record);
        let json = row(&entity, LOCAL);
        let actual_entity = actual_entity_bytes(&entity);
        let actual_json =
            crate::lua_memory::layout::json_value_retained_bytes(&json).expect("json bytes");
        let bound = SessionProjection::entity_bound_bytes(record);
        assert!(
            bound >= actual_entity,
            "{what}: entity bound {bound} < actual capacity bytes {actual_entity}"
        );
        let funded = plugin_host_call::row_funding_bytes(bound, LOCAL.len());
        assert!(
            funded >= actual_entity + actual_json,
            "{what}: funded {funded} < actual {} (entity {actual_entity} + json {actual_json})",
            actual_entity + actual_json
        );
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
        assert_funded(&record, "long metadata and forty traits");
        // A failed lifecycle carries its reason.
        let mut failed = fixtures::record("failed-session");
        failed.lifecycle = Some(botster_core::SessionLifecycleState::Failed {
            reason: "r".repeat(3000),
        });
        assert_funded(&failed, "a failed session with a long reason");
    }

    #[test]
    fn the_funding_bound_covers_a_traits_vector_that_grows_by_push() {
        // Empty strings are the smallest elements: the vector holds the most
        // headers per JSON byte, and it grows by doubling, so its capacity can
        // reach twice its length. 4097 is one past a power-of-two boundary.
        for count in [1usize, 3, 4, 5, 4096, 4097] {
            let mut record = fixtures::record("many-traits");
            let traits = vec![String::new(); count];
            record.metadata.entries.insert(
                "botster.session_type.traits".into(),
                serde_json::to_string(&traits).expect("traits json"),
            );
            assert_funded(&record, &format!("{count} empty traits"));
        }
    }
}
