//! Minimal safe Lua plugin runtime behind the core `PluginRuntime` boundary.
//!
//! This module intentionally exposes a narrow ABI: plugin registration,
//! handler invocation by stable id, and selected hub capability helpers.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use botster_core::{
    BoundaryJson, CapabilityOperation, CapabilityOperationId, CapabilityRuntimeErrorKind,
    CapabilityRuntimeRequest, EndpointId, EnvelopeCursor, EnvelopeId, EnvelopeTarget,
    PluginCancellationToken, PluginCapabilityRuntime, PluginDescriptorKind, PluginDescriptorRef,
    PluginHandlerKind, PluginHandlerRef, PluginHandlerRegistration, PluginInvocationFailure,
    PluginInvocationFailureKind, PluginInvocationRequest, PluginInvocationResult,
    PluginInvocationSuccess, PluginKey, PluginOwnedDescriptor, PluginResourceKind,
    PluginResourceRef, PluginRuntime, PluginStoreCapabilityRequest, PluginStoreKey,
    PluginStoreOperation, RoutedEnvelope, RoutedEnvelopeDrainOutcome, RoutedEnvelopePayload,
    RoutedEnvelopePublishOutcome, TimerCapabilityRequest,
};
use botster_core_daemon::RoutedEnvelopeDeliveryStateResult;
use mlua::{Function, HookTriggers, Lua, LuaOptions, LuaSerdeExt, StdLib, Table, Value, VmState};
use serde_json::json;

use crate::capabilities::{HubCapabilityRuntime, PluginStoreBatchMutation, PluginStoreBatchResult};
use crate::lifecycle::{HubPluginEventHandler, HubPluginRuntimeBundle};
use crate::packages::{PackageConfigurationView, PackageRecord, PreparedLocalPackage};
use crate::runtime::{SharedSessionTemplateSpawner, SharedSpawnTargets, SharedWorktrees};
use crate::session_templates::{
    ManagedSessionTemplateRequest, SessionTemplateContextInput, SessionTemplateRequest,
};

const DEFAULT_INSTRUCTION_BUDGET: u64 = 500_000;
const COORDINATION_REQUEST_TIMEOUT_MS: u64 = 1_000;
/// Shared host capability runtime used by Lua capability helpers.
pub type SharedHubCapabilityRuntime = Arc<Mutex<HubCapabilityRuntime>>;

/// Narrow CoreDaemon-backed coordination bridge exposed to Lua helpers.
#[derive(Clone)]
pub struct HubCoordinationBridge {
    owner_thread: thread::ThreadId,
    pending: Arc<Mutex<VecDeque<PendingCoordinationRequest>>>,
}

impl HubCoordinationBridge {
    pub(crate) fn new() -> Self {
        Self {
            owner_thread: thread::current().id(),
            pending: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn publish(&self, envelope: RoutedEnvelope) -> Result<RoutedEnvelopePublishOutcome, String> {
        let response = self.request(PendingCoordinationOperation::Publish { envelope })?;
        match response {
            HubCoordinationResponse::Publish(outcome) => Ok(outcome),
            _ => Err("coordination publish returned unexpected response".to_string()),
        }
    }

    fn drain(
        &self,
        target: EnvelopeTarget,
        after: Option<EnvelopeCursor>,
        limit: usize,
    ) -> Result<RoutedEnvelopeDrainOutcome, String> {
        let response = self.request(PendingCoordinationOperation::Drain {
            target,
            after,
            limit,
        })?;
        match response {
            HubCoordinationResponse::Drain(outcome) => Ok(outcome),
            _ => Err("coordination drain returned unexpected response".to_string()),
        }
    }

    fn acknowledge(
        &self,
        target: EnvelopeTarget,
        envelope_id: EnvelopeId,
    ) -> Result<RoutedEnvelopeDeliveryStateResult, String> {
        let response = self.request(PendingCoordinationOperation::Acknowledge {
            target,
            envelope_id,
        })?;
        match response {
            HubCoordinationResponse::Acknowledge(outcome) => Ok(outcome),
            _ => Err("coordination acknowledge returned unexpected response".to_string()),
        }
    }

    fn request(
        &self,
        operation: PendingCoordinationOperation,
    ) -> Result<HubCoordinationResponse, String> {
        if thread::current().id() == self.owner_thread {
            return Err(
                "botster.coordination is only available during handler invocation, not at plugin load"
                    .to_string(),
            );
        }

        let (response, receiver) = mpsc::channel();
        self.pending
            .lock()
            .map_err(|_| "coordination queue lock poisoned".to_string())?
            .push_back(PendingCoordinationRequest {
                operation,
                response,
            });
        receiver
            .recv_timeout(Duration::from_millis(COORDINATION_REQUEST_TIMEOUT_MS))
            .map_err(|_| "coordination request did not complete before timeout".to_string())?
    }

    pub(crate) fn take_pending(&self) -> Option<PendingCoordinationRequest> {
        self.pending
            .lock()
            .expect("coordination queue lock")
            .pop_front()
    }
}

pub(crate) struct PendingCoordinationRequest {
    pub(crate) operation: PendingCoordinationOperation,
    pub(crate) response: mpsc::Sender<Result<HubCoordinationResponse, String>>,
}

pub(crate) enum PendingCoordinationOperation {
    Publish {
        envelope: RoutedEnvelope,
    },
    Drain {
        target: EnvelopeTarget,
        after: Option<EnvelopeCursor>,
        limit: usize,
    },
    Acknowledge {
        target: EnvelopeTarget,
        envelope_id: EnvelopeId,
    },
}

pub(crate) enum HubCoordinationResponse {
    Publish(RoutedEnvelopePublishOutcome),
    Drain(RoutedEnvelopeDrainOutcome),
    Acknowledge(RoutedEnvelopeDeliveryStateResult),
}

struct LuaHostApi {
    configuration: PackageConfigurationView,
    capabilities: SharedHubCapabilityRuntime,
    coordination: HubCoordinationBridge,
    session_templates: SharedSessionTemplateSpawner,
    spawn_targets: SharedSpawnTargets,
    worktrees: SharedWorktrees,
    package_records: Vec<PackageRecord>,
}

/// Shared hub-owned primitives exposed to one Lua plugin runtime.
#[derive(Clone)]
pub struct LuaPluginHostApi {
    pub capabilities: SharedHubCapabilityRuntime,
    pub coordination: HubCoordinationBridge,
    pub session_templates: SharedSessionTemplateSpawner,
    pub spawn_targets: SharedSpawnTargets,
    pub worktrees: SharedWorktrees,
}

/// Real Lua runtime for one loaded plugin package.
pub struct LuaPluginRuntime {
    plugin_key: PluginKey,
    lua: Mutex<Lua>,
    instruction_budget: Arc<AtomicU64>,
    stopped: AtomicBool,
}

impl LuaPluginRuntime {
    /// Load a prepared local Lua package and return the core worker bundle.
    pub fn load_prepared(
        prepared: &PreparedLocalPackage,
        configuration: PackageConfigurationView,
        api: LuaPluginHostApi,
        package_records: Vec<PackageRecord>,
    ) -> Result<HubPluginRuntimeBundle, LuaPluginRuntimeError> {
        let plugin_key = PluginKey(prepared.package_name.clone());
        let selected_entrypoint_path =
            prepared.selected_entrypoint_path.as_ref().ok_or_else(|| {
                LuaPluginRuntimeError::Load("local package has no lua entrypoint".to_string())
            })?;
        let host_api = LuaHostApi {
            configuration,
            capabilities: api.capabilities,
            coordination: api.coordination,
            session_templates: api.session_templates,
            spawn_targets: api.spawn_targets,
            worktrees: api.worktrees,
            package_records,
        };
        let loaded = LoadedLuaPlugin::load(plugin_key.clone(), selected_entrypoint_path, host_api)?;
        Ok(HubPluginRuntimeBundle {
            runtime: Arc::new(loaded.runtime),
            handlers: loaded.handlers,
            event_handlers: loaded.event_handlers,
            descriptors: loaded.descriptors,
            resources: loaded.resources,
            entrypoint: Some(selected_entrypoint_path.to_string_lossy().into_owned()),
            metadata: Some(BoundaryJson(json!({
                "runtime": "lua",
                "abi": "botster.lua.v1",
            }))),
        })
    }

    fn new(
        plugin_key: PluginKey,
        entrypoint: &Path,
        host_api: LuaHostApi,
    ) -> Result<(Self, LuaRegistration), LuaPluginRuntimeError> {
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8,
            LuaOptions::default(),
        )?;
        let budget = Arc::new(AtomicU64::new(DEFAULT_INSTRUCTION_BUDGET));
        let hook_budget = budget.clone();
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(1_000),
            move |_lua, _debug| {
                let previous = hook_budget.fetch_sub(1_000, Ordering::Relaxed);
                if previous <= 1_000 {
                    return Err(mlua::Error::RuntimeError(
                        "lua instruction budget exceeded".to_string(),
                    ));
                }
                Ok(VmState::Continue)
            },
        )?;
        install_botster_api(&lua, plugin_key.clone(), host_api)?;
        let source = std::fs::read_to_string(entrypoint).map_err(|error| {
            LuaPluginRuntimeError::Load(format!("failed to read Lua entrypoint: {error}"))
        })?;
        let value: Value = lua
            .load(&source)
            .set_name(entrypoint.to_string_lossy().as_ref())
            .eval()
            .map_err(LuaPluginRuntimeError::from)?;
        let registration = registration_from_value(&lua, value)?;

        Ok((
            Self {
                plugin_key,
                lua: Mutex::new(lua),
                instruction_budget: budget,
                stopped: AtomicBool::new(false),
            },
            registration,
        ))
    }
}

impl PluginRuntime for LuaPluginRuntime {
    fn invoke(
        &self,
        request: PluginInvocationRequest,
        cancellation: PluginCancellationToken,
    ) -> PluginInvocationResult {
        if self.stopped.load(Ordering::SeqCst) {
            return failed(
                request,
                PluginInvocationFailureKind::WorkerStopped,
                "lua runtime stopped",
            );
        }
        if cancellation.is_cancelled() {
            return failed(
                request,
                PluginInvocationFailureKind::Cancelled,
                "invocation cancelled",
            );
        }
        if request.handler.plugin_key != self.plugin_key {
            return failed(
                request,
                PluginInvocationFailureKind::HandlerFailed,
                "handler belongs to a different plugin",
            );
        }

        let lua = self.lua.lock().expect("lua runtime mutex");
        self.instruction_budget
            .store(DEFAULT_INSTRUCTION_BUDGET, Ordering::Relaxed);
        let handlers = match lua.globals().get::<Table>("__botster_handlers") {
            Ok(handlers) => handlers,
            Err(error) => {
                return failed(
                    request,
                    PluginInvocationFailureKind::HandlerFailed,
                    format!("handler registry missing: {error}"),
                );
            }
        };
        let function = match handlers.get::<Function>(request.handler.handler_id.as_str()) {
            Ok(function) => function,
            Err(_) => {
                return failed(
                    request,
                    PluginInvocationFailureKind::HandlerFailed,
                    "plugin handler is not registered in Lua",
                );
            }
        };
        let payload = match lua.to_value(&request.payload.0) {
            Ok(payload) => payload,
            Err(error) => {
                return failed(
                    request,
                    PluginInvocationFailureKind::HandlerFailed,
                    format!("failed to encode invocation payload: {error}"),
                );
            }
        };

        match function.call::<Value>(payload) {
            Ok(Value::Nil) => PluginInvocationResult::Completed(PluginInvocationSuccess {
                request_id: request.request_id,
                handler: request.handler,
                payload: None,
            }),
            Ok(value) => match lua.from_value::<serde_json::Value>(value) {
                Ok(value) => PluginInvocationResult::Completed(PluginInvocationSuccess {
                    request_id: request.request_id,
                    handler: request.handler,
                    payload: Some(BoundaryJson(value)),
                }),
                Err(error) => failed(
                    request,
                    PluginInvocationFailureKind::HandlerFailed,
                    format!("failed to decode Lua handler response: {error}"),
                ),
            },
            Err(error) => failed(
                request,
                PluginInvocationFailureKind::HandlerFailed,
                sanitize_lua_error(error),
            ),
        }
    }

    fn stop(&self, _plugin_key: &PluginKey) {
        self.stopped.store(true, Ordering::SeqCst);
        if let Ok(lua) = self.lua.lock()
            && let Ok(handlers) = lua.create_table()
        {
            let _ = lua.globals().set("__botster_handlers", handlers);
        }
    }
}

struct LoadedLuaPlugin {
    runtime: LuaPluginRuntime,
    handlers: Vec<PluginHandlerRegistration>,
    event_handlers: Vec<HubPluginEventHandler>,
    descriptors: Vec<PluginOwnedDescriptor>,
    resources: Vec<PluginResourceRef>,
}

impl LoadedLuaPlugin {
    fn load(
        plugin_key: PluginKey,
        entrypoint: &Path,
        host_api: LuaHostApi,
    ) -> Result<Self, LuaPluginRuntimeError> {
        let (runtime, registration) =
            LuaPluginRuntime::new(plugin_key.clone(), entrypoint, host_api)?;
        let mut handlers = Vec::new();
        let mut event_handlers = Vec::new();
        let mut descriptors = Vec::new();
        let mut resources = Vec::new();

        for tool in registration.tools {
            let handler = PluginHandlerRef {
                plugin_key: plugin_key.clone(),
                kind: PluginHandlerKind::McpTool,
                handler_id: tool.handler.clone(),
            };
            handlers.push(PluginHandlerRegistration {
                handler: handler.clone(),
                required_capability: None,
            });
            descriptors.push(PluginOwnedDescriptor {
                descriptor: PluginDescriptorRef {
                    plugin_key: plugin_key.clone(),
                    kind: PluginDescriptorKind::McpTool,
                    descriptor_id: tool.name.clone(),
                },
                handler: Some(handler),
                body: BoundaryJson(json!({
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": tool.input_schema,
                })),
            });
            resources.push(PluginResourceRef {
                plugin_key: plugin_key.clone(),
                kind: PluginResourceKind::McpRegistration,
                resource_id: tool.name,
            });
        }

        for handler in registration.handlers {
            let descriptor_kind = descriptor_kind_for_handler_kind(handler.kind.clone());
            let handler_ref = PluginHandlerRef {
                plugin_key: plugin_key.clone(),
                kind: handler.kind.clone(),
                handler_id: handler.id.clone(),
            };
            handlers.push(PluginHandlerRegistration {
                handler: handler_ref.clone(),
                required_capability: None,
            });
            if handler.kind == PluginHandlerKind::Event {
                let event_name = handler.event_name.ok_or_else(|| {
                    LuaPluginRuntimeError::Lua(
                        "event handlers require an event or event_name field".to_string(),
                    )
                })?;
                if event_name.trim().is_empty() {
                    return Err(LuaPluginRuntimeError::Lua(
                        "event handlers require a non-empty event name".to_string(),
                    ));
                }
                event_handlers.push(HubPluginEventHandler {
                    event_name,
                    handler: handler_ref.clone(),
                });
            }
            if let Some(kind) = descriptor_kind {
                descriptors.push(PluginOwnedDescriptor {
                    descriptor: PluginDescriptorRef {
                        plugin_key: plugin_key.clone(),
                        kind,
                        descriptor_id: handler.descriptor_id.clone(),
                    },
                    handler: Some(handler_ref),
                    body: BoundaryJson(handler.body),
                });
            }
        }

        Ok(Self {
            runtime,
            handlers,
            event_handlers,
            descriptors,
            resources,
        })
    }
}

#[derive(Debug)]
pub enum LuaPluginRuntimeError {
    Load(String),
    Lua(String),
}

impl fmt::Display for LuaPluginRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load(message) | Self::Lua(message) => formatter.write_str(message),
        }
    }
}

impl Error for LuaPluginRuntimeError {}

impl From<mlua::Error> for LuaPluginRuntimeError {
    fn from(error: mlua::Error) -> Self {
        Self::Lua(sanitize_lua_error(error))
    }
}

#[derive(Debug)]
struct LuaRegistration {
    tools: Vec<LuaToolRegistration>,
    handlers: Vec<LuaHandlerRegistration>,
}

#[derive(Debug)]
struct LuaToolRegistration {
    name: String,
    description: String,
    input_schema: serde_json::Value,
    handler: String,
}

#[derive(Debug)]
struct LuaHandlerRegistration {
    id: String,
    kind: PluginHandlerKind,
    descriptor_id: String,
    event_name: Option<String>,
    body: serde_json::Value,
}

fn empty_object() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
    })
}

fn install_botster_api(
    lua: &Lua,
    plugin_key: PluginKey,
    host_api: LuaHostApi,
) -> Result<(), LuaPluginRuntimeError> {
    let globals = lua.globals();
    globals.set("__botster_handlers", lua.create_table()?)?;
    globals.set("os", Value::Nil)?;
    globals.set("io", Value::Nil)?;
    globals.set("package", Value::Nil)?;

    let events = lua.create_table()?;
    events.set(
        "on",
        lua.create_function(|lua, (event_name, handler): (String, Function)| {
            if event_name.trim().is_empty() {
                return Err(mlua::Error::RuntimeError(
                    "events.on requires a non-empty event name".to_string(),
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
            let handler_id = format!("event:{event_name}:{}", handler_table.raw_len() + 1);
            let handlers = lua.globals().get::<Table>("__botster_handlers")?;
            handlers.set(handler_id.clone(), handler)?;
            let entry = lua.create_table()?;
            entry.set("id", handler_id)?;
            entry.set("kind", "event")?;
            entry.set("event", event_name)?;
            handler_table.set(handler_table.raw_len() + 1, entry)?;
            Ok(())
        })?,
    )?;
    globals.set("__botster_registration", lua.create_table()?)?;
    globals.set("events", events)?;

    let botster = lua.create_table()?;
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
                }
            }
        }
        lua.globals()
            .set("__botster_registration", registration.clone())?;
        Ok(registration)
    })?;
    botster.set("register", register)?;

    let capabilities_table = lua.create_table()?;
    let timer_capabilities = host_api.capabilities.clone();
    let timer_plugin_key = plugin_key.clone();
    capabilities_table.set(
        "timer_once",
        lua.create_function(move |lua, delay_ms: u64| {
            let operation_id = CapabilityOperationId(format!("lua-timer-{delay_ms}"));
            let request = CapabilityRuntimeRequest {
                plugin_key: timer_plugin_key.clone(),
                operation_id: operation_id.clone(),
                operation: CapabilityOperation::Timer(TimerCapabilityRequest::Once { delay_ms }),
                timeout_ms: 1_000,
                callback: None,
            };
            let mut runtime = timer_capabilities.lock().map_err(|_| {
                mlua::Error::RuntimeError("capability runtime lock poisoned".to_string())
            })?;
            let handle = runtime
                .submit(request)
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            let events = runtime
                .drain_events(&timer_plugin_key)
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            lua.to_value(&json!({
                "operation_id": handle.operation_id.0,
                "resource_id": handle.resource.map(|resource| resource.resource_id),
                "event_count": events.len(),
            }))
        })?,
    )?;
    capabilities_table.set(
        "plugin_db",
        plugin_db_table(lua, plugin_key.clone(), host_api.capabilities.clone())?,
    )?;
    capabilities_table.set(
        "session_templates",
        session_templates_table(
            lua,
            plugin_key.clone(),
            host_api.session_templates,
            host_api.package_records,
        )?,
    )?;
    capabilities_table.set(
        "spawn_targets",
        spawn_targets_table(lua, host_api.spawn_targets.clone())?,
    )?;
    capabilities_table.set(
        "worktrees",
        worktrees_table(lua, host_api.worktrees, host_api.spawn_targets)?,
    )?;
    capabilities_table.set("config", config_table(lua, host_api.configuration)?)?;
    botster.set("capabilities", capabilities_table)?;
    botster.set(
        "coordination",
        coordination_table(lua, plugin_key, host_api.coordination)?,
    )?;
    globals.set("botster", botster)?;
    Ok(())
}

fn spawn_targets_table(lua: &Lua, spawn_targets: SharedSpawnTargets) -> Result<Table, mlua::Error> {
    let table = lua.create_table()?;
    let list_targets = spawn_targets.clone();
    table.set(
        "list",
        lua.create_function(move |lua, ()| {
            let targets = list_targets.lock().map_err(|_| {
                mlua::Error::RuntimeError("spawn target registry lock poisoned".to_string())
            })?;
            lua.to_value(&crate::spawn_targets::list_spawn_targets(&targets))
        })?,
    )?;
    table.set(
        "validate",
        lua.create_function(move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let target_id = value
                .get("target_id")
                .or_else(|| value.get("id"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    mlua::Error::RuntimeError(
                        "spawn_targets.validate requires target_id".to_string(),
                    )
                })?;
            let targets = spawn_targets.lock().map_err(|_| {
                mlua::Error::RuntimeError("spawn target registry lock poisoned".to_string())
            })?;
            lua.to_value(&crate::spawn_targets::validate_spawn_target(
                &targets, target_id,
            ))
        })?,
    )?;
    Ok(table)
}

fn worktrees_table(
    lua: &Lua,
    worktrees: SharedWorktrees,
    spawn_targets: SharedSpawnTargets,
) -> Result<Table, mlua::Error> {
    let table = lua.create_table()?;
    let list_worktrees = worktrees.clone();
    let list_targets = spawn_targets.clone();
    table.set(
        "list",
        lua.create_function(move |lua, ()| {
            let targets = list_targets.lock().map_err(|_| {
                mlua::Error::RuntimeError("spawn target registry lock poisoned".to_string())
            })?;
            let worktrees = list_worktrees.lock().map_err(|_| {
                mlua::Error::RuntimeError("worktree registry lock poisoned".to_string())
            })?;
            lua.to_value(&crate::worktrees::list_worktrees(&worktrees, &targets))
        })?,
    )?;
    table.set(
        "show",
        lua.create_function(move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let worktree_id = value
                .get("worktree_id")
                .or_else(|| value.get("id"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    mlua::Error::RuntimeError("worktrees.show requires worktree_id".to_string())
                })?;
            let targets = spawn_targets.lock().map_err(|_| {
                mlua::Error::RuntimeError("spawn target registry lock poisoned".to_string())
            })?;
            let worktrees = worktrees.lock().map_err(|_| {
                mlua::Error::RuntimeError("worktree registry lock poisoned".to_string())
            })?;
            match crate::worktrees::show_worktree(&worktrees, &targets, worktree_id) {
                Ok(worktree) => lua.to_value(&json!({
                    "ok": true,
                    "status": worktree.status,
                    "worktree": worktree,
                })),
                Err(error) if error.kind == "not_found" => lua.to_value(&json!({
                    "ok": false,
                    "status": error.kind,
                    "worktree_id": worktree_id,
                    "message": error.message,
                })),
                Err(error) => Err(mlua::Error::RuntimeError(format!(
                    "worktrees.show failed: {error}"
                ))),
            }
        })?,
    )?;
    Ok(table)
}

fn config_table(lua: &Lua, configuration: PackageConfigurationView) -> Result<Table, mlua::Error> {
    let config = lua.create_table()?;
    let payload = json!({
        "values": configuration.effective_values,
        "missing_required": configuration.missing_required,
        "diagnostics": configuration.diagnostics,
    });
    config.set(
        "get",
        lua.create_function(move |lua, package_name: Value| {
            if !matches!(package_name, Value::Nil) {
                return Err(mlua::Error::RuntimeError(
                    "config.get reads only the loaded plugin configuration and accepts no package name"
                        .to_string(),
                ));
            }
            lua.to_value(&payload)
        })?,
    )?;
    Ok(config)
}

fn session_templates_table(
    lua: &Lua,
    plugin_key: PluginKey,
    session_templates: SharedSessionTemplateSpawner,
    package_records: Vec<PackageRecord>,
) -> Result<Table, mlua::Error> {
    let table = lua.create_table()?;
    let list_templates = session_templates.clone();
    let list_records = package_records.clone();
    table.set(
        "list",
        lua.create_function(move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let target_id = required_string(&value, "target_id", "session_templates.list")?;
            let templates = list_templates
                .list(target_id, list_records.clone())
                .map_err(|error| {
                    mlua::Error::RuntimeError(format!("session_templates.list failed: {error}"))
                })?;
            lua.to_value(&templates)
        })?,
    )?;
    let show_templates = session_templates.clone();
    let show_records = package_records.clone();
    table.set(
        "show",
        lua.create_function(move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let target_id = required_string(&value, "target_id", "session_templates.show")?;
            let template_id = required_string(&value, "template_id", "session_templates.show")?;
            let template = show_templates
                .show(target_id, template_id, show_records.clone())
                .map_err(|error| {
                    mlua::Error::RuntimeError(format!("session_templates.show failed: {error}"))
                })?;
            lua.to_value(&template)
        })?,
    )?;
    let spawn_templates = session_templates.clone();
    let spawn_plugin_key = plugin_key.clone();
    let spawn_records = package_records.clone();
    table.set(
        "spawn",
        lua.create_function(move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let template_id = value
                .get("template_id")
                .or_else(|| value.get("id"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    mlua::Error::RuntimeError(
                        "session_templates.spawn requires template_id".to_string(),
                    )
                })?;
            let request = session_template_request_from_lua(&value)?;
            let result = spawn_templates
                .spawn(
                    &spawn_plugin_key,
                    template_id,
                    request,
                    spawn_records.clone(),
                )
                .map_err(|error| {
                    mlua::Error::RuntimeError(format!("session_templates.spawn failed: {error}"))
                })?;
            lua.to_value(&result)
        })?,
    )?;
    table.set(
        "ensure_worktree_and_spawn",
        lua.create_function(move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            reject_trusted_managed_fields(&value)?;
            let target_id = required_string(
                &value,
                "target_id",
                "session_templates.ensure_worktree_and_spawn",
            )?;
            let branch = required_string(
                &value,
                "branch",
                "session_templates.ensure_worktree_and_spawn",
            )?;
            let template_id = required_string(
                &value,
                "template_id",
                "session_templates.ensure_worktree_and_spawn",
            )?;
            let request = managed_session_template_request_from_lua(&value)?;
            match session_templates.ensure_worktree_and_spawn(
                &plugin_key,
                target_id,
                branch,
                template_id,
                request,
                package_records.clone(),
            ) {
                Ok(spawned) => lua.to_value(&json!({"ok": true, "result": spawned})),
                Err(error) => lua.to_value(&json!({"ok": false, "error": error})),
            }
        })?,
    )?;
    Ok(table)
}

fn required_string<'a>(
    value: &'a serde_json::Value,
    key: &str,
    operation: &str,
) -> Result<&'a str, mlua::Error> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| mlua::Error::RuntimeError(format!("{operation} requires {key}")))
}

fn reject_trusted_managed_fields(value: &serde_json::Value) -> Result<(), mlua::Error> {
    const TOP_LEVEL: &[&str] = &[
        "cwd",
        "session_id",
        "repo_path",
        "worktree_path",
        "branch_name",
        "base_ref",
        "base_commit",
    ];
    const CONTEXT: &[&str] = &[
        "cwd",
        "repo_path",
        "worktree_path",
        "branch_name",
        "base_ref",
        "base_commit",
        "target_id",
    ];
    if TOP_LEVEL.iter().any(|key| value.get(key).is_some())
        || value
            .get("context")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|context| CONTEXT.iter().any(|key| context.contains_key(*key)))
    {
        return Err(mlua::Error::RuntimeError(
            "session_templates.ensure_worktree_and_spawn rejects caller-supplied trusted fields"
                .to_string(),
        ));
    }
    Ok(())
}

fn managed_session_template_request_from_lua(
    value: &serde_json::Value,
) -> Result<ManagedSessionTemplateRequest, mlua::Error> {
    let context = value.get("context");
    if context.is_some_and(|context| !context.is_object()) {
        return Err(mlua::Error::RuntimeError(
            "session_templates.ensure_worktree_and_spawn context must be an object".to_string(),
        ));
    }
    Ok(ManagedSessionTemplateRequest {
        environment: string_map(value.get("environment"), "environment")?,
        prompt: context.and_then(|value| optional_string(value, "prompt")),
        ticket_id: context.and_then(|value| optional_string(value, "ticket_id")),
        workspace_id: context.and_then(|value| optional_string(value, "workspace_id")),
        metadata: string_map(
            context.and_then(|value| value.get("metadata")),
            "context.metadata",
        )?,
    })
}

fn session_template_request_from_lua(
    value: &serde_json::Value,
) -> Result<SessionTemplateRequest, mlua::Error> {
    Ok(SessionTemplateRequest {
        target_id: optional_string(value, "target_id"),
        session_id: optional_string(value, "session_id").map(botster_core::SessionId),
        cwd: optional_string(value, "cwd"),
        environment: string_map(value.get("environment"), "environment")?,
        context: session_template_context_from_lua(value.get("context"))?,
    })
}

fn session_template_context_from_lua(
    value: Option<&serde_json::Value>,
) -> Result<SessionTemplateContextInput, mlua::Error> {
    let Some(value) = value else {
        return Ok(SessionTemplateContextInput::default());
    };
    if !value.is_object() {
        return Err(mlua::Error::RuntimeError(
            "session_templates.spawn context must be an object".to_string(),
        ));
    }
    Ok(SessionTemplateContextInput {
        worktree_path: optional_string(value, "worktree_path"),
        repo_path: optional_string(value, "repo_path"),
        branch_name: optional_string(value, "branch_name"),
        prompt: optional_string(value, "prompt"),
        ticket_id: optional_string(value, "ticket_id"),
        workspace_id: optional_string(value, "workspace_id"),
        metadata: string_map(value.get("metadata"), "context.metadata")?,
    })
}

fn optional_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

fn string_map(
    value: Option<&serde_json::Value>,
    label: &str,
) -> Result<BTreeMap<String, String>, mlua::Error> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let Some(object) = value.as_object() else {
        return Err(mlua::Error::RuntimeError(format!(
            "session_templates.spawn {label} must be an object"
        )));
    };
    object
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| (key.clone(), value.to_string()))
                .ok_or_else(|| {
                    mlua::Error::RuntimeError(format!(
                        "session_templates.spawn {label}.{key} must be a string"
                    ))
                })
        })
        .collect()
}

fn plugin_db_table(
    lua: &Lua,
    plugin_key: PluginKey,
    capabilities: SharedHubCapabilityRuntime,
) -> Result<Table, mlua::Error> {
    let plugin_db = lua.create_table()?;
    for (name, action) in [
        ("get", "get"),
        ("set", "set"),
        ("patch", "patch"),
        ("delete", "delete"),
        ("list", "list"),
    ] {
        let runtime = capabilities.clone();
        let key = plugin_key.clone();
        plugin_db.set(
            name,
            lua.create_function(move |lua, args: Value| {
                let operation = plugin_store_operation_from_lua(lua, action, args)?;
                execute_plugin_store_for_lua(lua, runtime.clone(), key.clone(), operation, action)
            })?,
        )?;
    }
    let batch_runtime = capabilities.clone();
    let batch_plugin_key = plugin_key.clone();
    plugin_db.set(
        "batch",
        lua.create_function(move |lua, args: Value| {
            execute_plugin_store_batch_for_lua(
                lua,
                batch_runtime.clone(),
                batch_plugin_key.clone(),
                args,
            )
        })?,
    )?;
    Ok(plugin_db)
}

fn execute_plugin_store_batch_for_lua(
    lua: &Lua,
    capabilities: SharedHubCapabilityRuntime,
    plugin_key: PluginKey,
    args: Value,
) -> Result<Value, mlua::Error> {
    let value = lua.from_value::<serde_json::Value>(args)?;
    let Some(object) = value.as_object() else {
        return lua.to_value(&PluginStoreBatchResult::failure(
            botster_core::CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "plugin_db.batch requires an object",
            ),
            None,
            None,
        ));
    };
    if object.len() != 1 || !object.contains_key("mutations") {
        return lua.to_value(&PluginStoreBatchResult::failure(
            botster_core::CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "plugin_db.batch accepts only mutations",
            ),
            None,
            None,
        ));
    }
    let Some(raw_mutations) = value.get("mutations").and_then(serde_json::Value::as_array) else {
        return lua.to_value(&PluginStoreBatchResult::failure(
            botster_core::CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "plugin_db.batch mutations must be an array",
            ),
            None,
            None,
        ));
    };
    let mut mutations = Vec::with_capacity(raw_mutations.len());
    for (index, raw_mutation) in raw_mutations.iter().enumerate() {
        match serde_json::from_value::<PluginStoreBatchMutation>(raw_mutation.clone()) {
            Ok(mutation) => mutations.push(mutation),
            Err(error) => {
                let key = raw_mutation
                    .get("key")
                    .and_then(serde_json::Value::as_str)
                    .map(|key| PluginStoreKey(key.to_string()));
                return lua.to_value(&PluginStoreBatchResult::failure(
                    botster_core::CapabilityRuntimeError::new(
                        CapabilityRuntimeErrorKind::InvalidRequest,
                        format!("plugin_db.batch mutations are invalid: {error}"),
                    ),
                    Some(index + 1),
                    key,
                ));
            }
        }
    }
    let prepared = {
        let runtime = capabilities.lock().map_err(|_| {
            mlua::Error::RuntimeError("capability runtime lock poisoned".to_string())
        })?;
        runtime
            .prepare_plugin_store_batch(&plugin_key, &plugin_key.0, mutations)
            .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?
    };

    lua.to_value(&prepared.execute())
}

fn plugin_store_operation_from_lua(
    lua: &Lua,
    action: &str,
    args: Value,
) -> Result<PluginStoreOperation, mlua::Error> {
    let value = lua.from_value::<serde_json::Value>(args)?;
    let key = value
        .get("key")
        .and_then(serde_json::Value::as_str)
        .map(|key| PluginStoreKey(key.to_string()));
    match action {
        "get" => Ok(PluginStoreOperation::Get {
            key: key
                .ok_or_else(|| mlua::Error::RuntimeError("plugin_db.get requires key".into()))?,
        }),
        "set" => Ok(PluginStoreOperation::Set {
            key: key
                .ok_or_else(|| mlua::Error::RuntimeError("plugin_db.set requires key".into()))?,
            schema_version: value
                .get("schema_version")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1),
            payload: value.get("payload").cloned().ok_or_else(|| {
                mlua::Error::RuntimeError("plugin_db.set requires payload".into())
            })?,
            expected_revision: value
                .get("expected_revision")
                .and_then(serde_json::Value::as_u64),
        }),
        "patch" => Ok(PluginStoreOperation::Patch {
            key: key
                .ok_or_else(|| mlua::Error::RuntimeError("plugin_db.patch requires key".into()))?,
            patch: value.get("patch").cloned().ok_or_else(|| {
                mlua::Error::RuntimeError("plugin_db.patch requires patch".into())
            })?,
            expected_revision: value
                .get("expected_revision")
                .and_then(serde_json::Value::as_u64),
        }),
        "delete" => Ok(PluginStoreOperation::Delete {
            key: key
                .ok_or_else(|| mlua::Error::RuntimeError("plugin_db.delete requires key".into()))?,
        }),
        "list" => Ok(PluginStoreOperation::List {
            prefix: value
                .get("prefix")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string),
        }),
        _ => Err(mlua::Error::RuntimeError(
            "unsupported plugin_db operation".to_string(),
        )),
    }
}

fn execute_plugin_store_for_lua(
    lua: &Lua,
    capabilities: SharedHubCapabilityRuntime,
    plugin_key: PluginKey,
    operation: PluginStoreOperation,
    action: &str,
) -> Result<Value, mlua::Error> {
    let prepared = {
        let runtime = capabilities.lock().map_err(|_| {
            mlua::Error::RuntimeError("capability runtime lock poisoned".to_string())
        })?;
        runtime
            .prepare_plugin_store(
                &plugin_key,
                PluginStoreCapabilityRequest {
                    namespace: plugin_key.0.clone(),
                    operation,
                },
            )
            .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?
    };

    match prepared.execute() {
        Ok(result) => lua.to_value(&result),
        Err(error)
            if action == "get" && error.kind == CapabilityRuntimeErrorKind::StoreNotFound =>
        {
            lua.to_value(&json!({ "kind": "record" }))
        }
        Err(error) => Err(mlua::Error::RuntimeError(format!(
            "plugin_db operation failed: {}",
            error.message
        ))),
    }
}

fn coordination_table(
    lua: &Lua,
    plugin_key: PluginKey,
    coordination_bridge: HubCoordinationBridge,
) -> Result<Table, mlua::Error> {
    let coordination = lua.create_table()?;

    let publish_bridge = coordination_bridge.clone();
    let publish_plugin_key = plugin_key.clone();
    coordination.set(
        "publish",
        lua.create_function(move |lua, args: Value| {
            let envelope = routed_envelope_from_lua(lua, publish_plugin_key.clone(), args)?;
            let outcome = publish_bridge
                .publish(envelope)
                .map_err(mlua::Error::RuntimeError)?;
            lua.to_value(&outcome)
        })?,
    )?;

    let drain_bridge = coordination_bridge.clone();
    coordination.set(
        "drain",
        lua.create_function(move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let target = target_from_json(value.get("target"))?;
            let after = value
                .get("after")
                .and_then(serde_json::Value::as_u64)
                .map(EnvelopeCursor);
            let limit = value
                .get("limit")
                .and_then(serde_json::Value::as_u64)
                .and_then(|limit| usize::try_from(limit).ok())
                .unwrap_or(16);
            let outcome = drain_bridge
                .drain(target, after, limit)
                .map_err(mlua::Error::RuntimeError)?;
            lua.to_value(&outcome)
        })?,
    )?;

    let ack_bridge = coordination_bridge;
    coordination.set(
        "acknowledge",
        lua.create_function(move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let target = target_from_json(value.get("target"))?;
            let envelope_id = value
                .get("envelope_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    mlua::Error::RuntimeError(
                        "coordination.acknowledge requires envelope_id".to_string(),
                    )
                })?;
            let outcome = ack_bridge
                .acknowledge(target, EnvelopeId(envelope_id.to_string()))
                .map_err(mlua::Error::RuntimeError)?;
            lua.to_value(&outcome)
        })?,
    )?;

    Ok(coordination)
}

fn routed_envelope_from_lua(
    lua: &Lua,
    plugin_key: PluginKey,
    args: Value,
) -> Result<RoutedEnvelope, mlua::Error> {
    let value = lua.from_value::<serde_json::Value>(args)?;
    let id = value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| mlua::Error::RuntimeError("coordination.publish requires id".to_string()))?;
    let target = target_from_json(value.get("target"))?;
    let body = value
        .get("body")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .as_bytes()
        .to_vec();
    Ok(RoutedEnvelope::new(
        EnvelopeId(id.to_string()),
        EndpointId(format!("plugin:{}", plugin_key.0)),
        vec![target],
        RoutedEnvelopePayload {
            content_type: value
                .get("content_type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("application/json")
                .to_string(),
            body,
            extension: value.get("extension").cloned().map(BoundaryJson),
        },
        value
            .get("created_at")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    ))
}

fn target_from_json(value: Option<&serde_json::Value>) -> Result<EnvelopeTarget, mlua::Error> {
    let value = value
        .cloned()
        .ok_or_else(|| mlua::Error::RuntimeError("coordination target is required".to_string()))?;
    serde_json::from_value(value)
        .map_err(|error| mlua::Error::RuntimeError(format!("invalid coordination target: {error}")))
}

fn registration_from_value(
    lua: &Lua,
    value: Value,
) -> Result<LuaRegistration, LuaPluginRuntimeError> {
    let value = match value {
        Value::Nil => lua.globals().get::<Value>("__botster_registration")?,
        value => value,
    };
    let Value::Table(registration) = value else {
        return Err(LuaPluginRuntimeError::Lua(
            "plugin entrypoint must return botster.register({...})".to_string(),
        ));
    };
    let mut tools = Vec::new();
    if let Ok(tool_table) = registration.get::<Table>("tools") {
        for tool in tool_table.sequence_values::<Table>() {
            let tool = tool.map_err(LuaPluginRuntimeError::from)?;
            let input_schema = match tool.get::<Value>("input_schema") {
                Ok(value) => lua
                    .from_value::<serde_json::Value>(value)
                    .map_err(LuaPluginRuntimeError::from)?,
                Err(_) => empty_object(),
            };
            tools.push(LuaToolRegistration {
                name: tool.get("name").map_err(LuaPluginRuntimeError::from)?,
                description: tool
                    .get("description")
                    .map_err(LuaPluginRuntimeError::from)?,
                input_schema,
                handler: tool.get("handler").map_err(LuaPluginRuntimeError::from)?,
            });
        }
    }
    let mut handlers = Vec::new();
    if let Ok(handler_table) = registration.get::<Table>("handlers") {
        for handler in handler_table.sequence_values::<Table>() {
            let handler = handler.map_err(LuaPluginRuntimeError::from)?;
            let kind: String = handler.get("kind").map_err(LuaPluginRuntimeError::from)?;
            handlers.push(LuaHandlerRegistration {
                id: handler.get("id").map_err(LuaPluginRuntimeError::from)?,
                kind: handler_kind_from_lua(&kind)?,
                descriptor_id: handler
                    .get("descriptor_id")
                    .or_else(|_| handler.get("id"))
                    .map_err(LuaPluginRuntimeError::from)?,
                event_name: handler
                    .get::<Option<String>>("event")
                    .map_err(LuaPluginRuntimeError::from)?
                    .or_else(|| handler.get::<Option<String>>("event_name").ok().flatten()),
                body: match handler.get::<Value>("descriptor") {
                    Ok(value) => lua
                        .from_value::<serde_json::Value>(value)
                        .map_err(LuaPluginRuntimeError::from)?,
                    Err(_) => serde_json::Value::Null,
                },
            });
        }
    }
    Ok(LuaRegistration { tools, handlers })
}

fn descriptor_kind_for_handler_kind(kind: PluginHandlerKind) -> Option<PluginDescriptorKind> {
    match kind {
        PluginHandlerKind::SurfaceRoute => Some(PluginDescriptorKind::SurfaceRoute),
        PluginHandlerKind::UiAction => Some(PluginDescriptorKind::UiAction),
        _ => None,
    }
}

fn handler_kind_from_lua(kind: &str) -> Result<PluginHandlerKind, LuaPluginRuntimeError> {
    match kind {
        "ui_action" => Ok(PluginHandlerKind::UiAction),
        "session_action" => Ok(PluginHandlerKind::SessionAction),
        "command" => Ok(PluginHandlerKind::Command),
        "mcp_tool" => Ok(PluginHandlerKind::McpTool),
        "event" => Ok(PluginHandlerKind::Event),
        "hook" => Ok(PluginHandlerKind::Hook),
        "timer" => Ok(PluginHandlerKind::Timer),
        "surface_route" => Ok(PluginHandlerKind::SurfaceRoute),
        other => Err(LuaPluginRuntimeError::Lua(format!(
            "unsupported lua handler kind: {other}"
        ))),
    }
}

fn failed(
    request: PluginInvocationRequest,
    kind: PluginInvocationFailureKind,
    reason: impl Into<String>,
) -> PluginInvocationResult {
    PluginInvocationResult::Failed(PluginInvocationFailure {
        request_id: request.request_id,
        handler: request.handler,
        kind,
        timeout_ms: Some(request.timeout_ms),
        reason: reason.into(),
    })
}

fn sanitize_lua_error(error: mlua::Error) -> String {
    let message = error.to_string();
    message
        .lines()
        .next()
        .unwrap_or("lua runtime error")
        .replace('\\', "/")
}
