//! Minimal safe Lua plugin runtime behind the core `PluginRuntime` boundary.
//!
//! This module intentionally exposes a narrow ABI: plugin registration,
//! handler invocation by stable id, and selected hub capability helpers.

use std::error::Error;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use botster_core::{
    BoundaryJson, CapabilityOperation, CapabilityOperationId, CapabilityRuntimeRequest,
    PluginCancellationToken, PluginCapabilityRuntime, PluginDescriptorKind, PluginDescriptorRef,
    PluginHandlerKind, PluginHandlerRef, PluginHandlerRegistration, PluginInvocationFailure,
    PluginInvocationFailureKind, PluginInvocationRequest, PluginInvocationResult,
    PluginInvocationSuccess, PluginKey, PluginOwnedDescriptor, PluginResourceKind,
    PluginResourceRef, PluginRuntime, TimerCapabilityRequest,
};
use mlua::{Function, HookTriggers, Lua, LuaOptions, LuaSerdeExt, StdLib, Table, Value, VmState};
use serde_json::json;

use crate::capabilities::HubCapabilityRuntime;
use crate::lifecycle::HubPluginRuntimeBundle;
use crate::packages::PreparedLocalPackage;

const DEFAULT_INSTRUCTION_BUDGET: u64 = 500_000;
/// Shared host capability runtime used by Lua capability helpers.
pub type SharedHubCapabilityRuntime = Arc<Mutex<HubCapabilityRuntime>>;

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
        capabilities: SharedHubCapabilityRuntime,
    ) -> Result<HubPluginRuntimeBundle, LuaPluginRuntimeError> {
        let plugin_key = PluginKey(prepared.package_name.clone());
        let loaded = LoadedLuaPlugin::load(
            plugin_key.clone(),
            &prepared.selected_entrypoint_path,
            capabilities,
        )?;
        Ok(HubPluginRuntimeBundle {
            runtime: Arc::new(loaded.runtime),
            handlers: loaded.handlers,
            descriptors: loaded.descriptors,
            resources: loaded.resources,
            entrypoint: Some(
                prepared
                    .selected_entrypoint_path
                    .to_string_lossy()
                    .into_owned(),
            ),
            metadata: Some(BoundaryJson(json!({
                "runtime": "lua",
                "abi": "botster.lua.v1",
            }))),
        })
    }

    fn new(
        plugin_key: PluginKey,
        entrypoint: &Path,
        capabilities: SharedHubCapabilityRuntime,
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
        install_botster_api(&lua, plugin_key.clone(), capabilities)?;
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
    descriptors: Vec<PluginOwnedDescriptor>,
    resources: Vec<PluginResourceRef>,
}

impl LoadedLuaPlugin {
    fn load(
        plugin_key: PluginKey,
        entrypoint: &Path,
        capabilities: SharedHubCapabilityRuntime,
    ) -> Result<Self, LuaPluginRuntimeError> {
        let (runtime, registration) =
            LuaPluginRuntime::new(plugin_key.clone(), entrypoint, capabilities)?;
        let mut handlers = Vec::new();
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
            handlers.push(PluginHandlerRegistration {
                handler: PluginHandlerRef {
                    plugin_key: plugin_key.clone(),
                    kind: handler.kind,
                    handler_id: handler.id,
                },
                required_capability: None,
            });
        }

        Ok(Self {
            runtime,
            handlers,
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
    capabilities: SharedHubCapabilityRuntime,
) -> Result<(), LuaPluginRuntimeError> {
    let globals = lua.globals();
    globals.set("__botster_handlers", lua.create_table()?)?;
    globals.set("os", Value::Nil)?;
    globals.set("io", Value::Nil)?;
    globals.set("package", Value::Nil)?;

    let botster = lua.create_table()?;
    let register = lua.create_function(|lua, registration: Table| {
        let handlers = lua.globals().get::<Table>("__botster_handlers")?;
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
                let handler: Function = custom_handler.get("call")?;
                handlers.set(handler_id, handler)?;
            }
        }
        Ok(registration)
    })?;
    botster.set("register", register)?;

    let capabilities_table = lua.create_table()?;
    let timer_capabilities = capabilities.clone();
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
    botster.set("capabilities", capabilities_table)?;
    globals.set("botster", botster)?;
    Ok(())
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
            });
        }
    }
    Ok(LuaRegistration { tools, handlers })
}

fn handler_kind_from_lua(kind: &str) -> Result<PluginHandlerKind, LuaPluginRuntimeError> {
    match kind {
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
