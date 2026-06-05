//! First-party local Project Pipelines plugin package wiring.
//!
//! The reduced hub scaffold does not yet execute Lua entrypoints directly.
//! This module supplies the host runtime bundle for the repo-owned
//! `examples/project-pipelines` package while keeping calls behind
//! `HubPluginLifecycle` and `PluginWorkerEngine`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use botster_core::{
    BoundaryJson, Capability, CapabilitySurface, PluginCancellationToken, PluginDescriptorKind,
    PluginDescriptorRef, PluginHandlerKind, PluginHandlerRef, PluginHandlerRegistration,
    PluginInvocationContext, PluginInvocationRequest, PluginInvocationResult,
    PluginInvocationSuccess, PluginKey, PluginOwnedDescriptor, PluginResourceKind,
    PluginResourceRef, PluginRuntime,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::lifecycle::HubPluginRuntimeBundle;
use crate::packages::PreparedLocalPackage;

/// Local package name for the first Project Pipelines plugin.
pub const PROJECT_PIPELINES_PACKAGE: &str = "project-pipelines";

const STATE_FILE: &str = "state.json";

/// Build a host runtime bundle for a prepared local package.
#[must_use]
pub fn runtime_bundle_for_prepared_package(
    prepared: &PreparedLocalPackage,
    data_directory: &Path,
) -> Option<HubPluginRuntimeBundle> {
    if prepared.package_name != PROJECT_PIPELINES_PACKAGE {
        return None;
    }

    let plugin_key = PluginKey(PROJECT_PIPELINES_PACKAGE.to_string());
    let runtime = Arc::new(ProjectPipelinesRuntime::new(
        data_directory
            .join("plugin-data")
            .join(PROJECT_PIPELINES_PACKAGE),
    ));
    let handlers = tool_specs(&plugin_key)
        .iter()
        .map(|spec| PluginHandlerRegistration {
            handler: handler(&plugin_key, spec.id),
            required_capability: Some(Capability {
                surface: CapabilitySurface::PluginDb,
                scope: Some(PROJECT_PIPELINES_PACKAGE.to_string()),
            }),
        })
        .collect::<Vec<_>>();
    let descriptors = tool_specs(&plugin_key)
        .iter()
        .map(|spec| PluginOwnedDescriptor {
            descriptor: PluginDescriptorRef {
                plugin_key: plugin_key.clone(),
                kind: PluginDescriptorKind::McpTool,
                descriptor_id: spec.id.to_string(),
            },
            handler: Some(handler(&plugin_key, spec.id)),
            body: BoundaryJson(json!({
                "name": spec.name,
                "description": spec.description,
                "input_schema": spec.input_schema,
            })),
        })
        .collect::<Vec<_>>();
    let resources = tool_specs(&plugin_key)
        .iter()
        .map(|spec| PluginResourceRef {
            plugin_key: plugin_key.clone(),
            kind: PluginResourceKind::McpRegistration,
            resource_id: spec.id.to_string(),
        })
        .collect::<Vec<_>>();

    Some(HubPluginRuntimeBundle {
        runtime,
        handlers,
        descriptors,
        resources,
        entrypoint: Some(
            prepared
                .selected_entrypoint_path
                .to_string_lossy()
                .into_owned(),
        ),
        metadata: Some(BoundaryJson(json!({
            "runtime": "host_project_pipelines_v1",
            "entrypoint": prepared.selected_entrypoint.path,
        }))),
    })
}

struct ToolSpec {
    id: &'static str,
    name: &'static str,
    description: &'static str,
    input_schema: Value,
}

fn tool_specs(plugin_key: &PluginKey) -> Vec<ToolSpec> {
    let _ = plugin_key;
    vec![
        ToolSpec {
            id: "create",
            name: "project_pipelines.create",
            description: "Create a constrained local Project Pipelines ticket.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": { "type": "string" },
                    "pipeline_id": { "type": "string" }
                },
                "required": ["title"],
                "additionalProperties": false
            }),
        },
        ToolSpec {
            id: "list",
            name: "project_pipelines.list",
            description: "List constrained local Project Pipelines records.",
            input_schema: empty_schema(),
        },
        ToolSpec {
            id: "update",
            name: "project_pipelines.update",
            description: "Update a constrained local Project Pipelines ticket.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "ticket_id": { "type": "string" },
                    "title": { "type": "string" },
                    "status": { "type": "string" }
                },
                "required": ["ticket_id"],
                "additionalProperties": false
            }),
        },
        ToolSpec {
            id: "start",
            name: "project_pipelines.start",
            description: "Start a constrained local Project Pipelines run.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "ticket_id": { "type": "string" },
                    "target_id": { "type": "string" },
                    "worktree": { "type": "string" },
                    "agent_name": { "type": "string" }
                },
                "required": ["ticket_id", "target_id", "worktree"],
                "additionalProperties": false
            }),
        },
        ToolSpec {
            id: "current_context",
            name: "project_pipelines.current_context",
            description: "Return constrained local Project Pipelines context.",
            input_schema: empty_schema(),
        },
        ToolSpec {
            id: "submit_gate",
            name: "project_pipelines.submit_gate",
            description: "Record gate evidence for a constrained local run.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "run_id": { "type": "string" },
                    "gate_id": { "type": "string" },
                    "status": { "type": "string" },
                    "summary": { "type": "string" },
                    "evidence": { "type": "object" }
                },
                "required": ["run_id", "gate_id", "status"],
                "additionalProperties": false
            }),
        },
        ToolSpec {
            id: "request_step_advance",
            name: "project_pipelines.request_step_advance",
            description: "Advance a constrained local run step.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "run_id": { "type": "string" },
                    "summary": { "type": "string" }
                },
                "required": ["run_id"],
                "additionalProperties": false
            }),
        },
    ]
}

fn empty_schema() -> Value {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
}

fn handler(plugin_key: &PluginKey, handler_id: &str) -> PluginHandlerRef {
    PluginHandlerRef {
        plugin_key: plugin_key.clone(),
        kind: PluginHandlerKind::McpTool,
        handler_id: handler_id.to_string(),
    }
}

struct ProjectPipelinesRuntime {
    state_path: PathBuf,
    state: Mutex<ProjectPipelinesState>,
}

impl ProjectPipelinesRuntime {
    fn new(root: PathBuf) -> Self {
        let state_path = root.join(STATE_FILE);
        let state = read_state(&state_path).unwrap_or_default();
        Self {
            state_path,
            state: Mutex::new(state),
        }
    }

    fn handle(&self, handler_id: &str, arguments: Value) -> Value {
        let mut state = self.state.lock().expect("project pipelines state lock");
        let mutates = !matches!(handler_id, "list" | "current_context");
        let result = match handler_id {
            "create" => state.create(arguments),
            "list" | "current_context" => state.snapshot(),
            "update" => state.update(arguments),
            "start" => state.start(arguments),
            "submit_gate" => state.submit_gate(arguments),
            "request_step_advance" => state.request_step_advance(arguments),
            other => json!({
                "ok": false,
                "error": {
                    "code": "unknown_handler",
                    "message": format!("unknown Project Pipelines handler: {other}")
                }
            }),
        };
        if mutates
            && result.get("ok").and_then(Value::as_bool) == Some(true)
            && let Err(error) = write_state(&self.state_path, &state)
        {
            return json!({
                "ok": false,
                "error": {
                    "code": "persist_failed",
                    "message": format!("failed to persist Project Pipelines state: {error}")
                }
            });
        }
        result
    }
}

impl PluginRuntime for ProjectPipelinesRuntime {
    fn invoke(
        &self,
        request: PluginInvocationRequest,
        _cancellation: PluginCancellationToken,
    ) -> PluginInvocationResult {
        let payload = self.handle(&request.handler.handler_id, request.payload.0);
        PluginInvocationResult::Completed(PluginInvocationSuccess {
            request_id: request.request_id,
            handler: request.handler,
            payload: Some(BoundaryJson(payload)),
        })
    }

    fn stop(&self, _plugin_key: &PluginKey) {}
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ProjectPipelinesState {
    tickets: Vec<TicketRecord>,
    runs: Vec<RunRecord>,
    gates: Vec<GateRecord>,
    events: Vec<EventRecord>,
    next_ticket: u64,
    next_run: u64,
    next_step: u64,
}

impl ProjectPipelinesState {
    fn create(&mut self, arguments: Value) -> Value {
        let title = string_arg(&arguments, "title").unwrap_or("Untitled local ticket");
        let pipeline_id = string_arg(&arguments, "pipeline_id").unwrap_or("local_pipeline");
        self.next_ticket += 1;
        let ticket = TicketRecord {
            id: format!("ticket_local_{}", self.next_ticket),
            title: title.to_string(),
            status: "open".to_string(),
            pipeline_id: pipeline_id.to_string(),
            created_at: now_seconds(),
        };
        self.events.push(EventRecord::new(
            "ticket.created",
            json!({ "ticket_id": ticket.id }),
        ));
        self.tickets.push(ticket.clone());
        json!({ "ok": true, "ticket": ticket })
    }

    fn update(&mut self, arguments: Value) -> Value {
        let Some(ticket_id) = string_arg(&arguments, "ticket_id") else {
            return missing_arg("ticket_id");
        };
        let Some(ticket) = self
            .tickets
            .iter_mut()
            .find(|ticket| ticket.id == ticket_id)
        else {
            return not_found("ticket", ticket_id);
        };
        if let Some(title) = string_arg(&arguments, "title") {
            ticket.title = title.to_string();
        }
        if let Some(status) = string_arg(&arguments, "status") {
            ticket.status = status.to_string();
        }
        let ticket = ticket.clone();
        self.events.push(EventRecord::new(
            "ticket.updated",
            json!({ "ticket_id": ticket.id }),
        ));
        json!({ "ok": true, "ticket": ticket })
    }

    fn start(&mut self, arguments: Value) -> Value {
        let Some(ticket_id) = string_arg(&arguments, "ticket_id") else {
            return missing_arg("ticket_id");
        };
        if !self.tickets.iter().any(|ticket| ticket.id == ticket_id) {
            return not_found("ticket", ticket_id);
        }
        let Some(target_id) = string_arg(&arguments, "target_id") else {
            return missing_arg("target_id");
        };
        let Some(worktree) = string_arg(&arguments, "worktree") else {
            return missing_arg("worktree");
        };
        let agent_name = string_arg(&arguments, "agent_name").unwrap_or("codex");
        self.next_run += 1;
        self.next_step += 1;
        let run = RunRecord {
            id: format!("run_local_{}", self.next_run),
            ticket_id: ticket_id.to_string(),
            status: "active".to_string(),
            current_step_id: format!("step_local_{}", self.next_step),
            coordination: CoordinationRecord {
                target_id: target_id.to_string(),
                assigned_worktree: worktree.to_string(),
                request_id: format!("project-pipelines:{}:{}", ticket_id, self.next_run),
                owner_plugin: PROJECT_PIPELINES_PACKAGE.to_string(),
                agent_name: agent_name.to_string(),
                session_uuid: None,
            },
        };
        self.events.push(EventRecord::new(
            "run.started",
            json!({
                "run_id": run.id,
                "request_id": run.coordination.request_id,
                "target_id": run.coordination.target_id,
                "assigned_worktree": run.coordination.assigned_worktree,
                "owner_plugin": run.coordination.owner_plugin
            }),
        ));
        self.runs.push(run.clone());
        json!({ "ok": true, "run": run })
    }

    fn submit_gate(&mut self, arguments: Value) -> Value {
        let Some(run_id) = string_arg(&arguments, "run_id") else {
            return missing_arg("run_id");
        };
        if !self.runs.iter().any(|run| run.id == run_id) {
            return not_found("run", run_id);
        }
        let Some(gate_id) = string_arg(&arguments, "gate_id") else {
            return missing_arg("gate_id");
        };
        let Some(status) = string_arg(&arguments, "status") else {
            return missing_arg("status");
        };
        let gate = GateRecord {
            run_id: run_id.to_string(),
            gate_id: gate_id.to_string(),
            status: status.to_string(),
            summary: string_arg(&arguments, "summary").map(ToString::to_string),
            evidence: arguments
                .get("evidence")
                .cloned()
                .unwrap_or_else(|| json!({})),
            created_at: now_seconds(),
        };
        self.events.push(EventRecord::new(
            "gate.submitted",
            json!({ "run_id": run_id, "gate_id": gate_id, "status": status }),
        ));
        self.gates.push(gate.clone());
        json!({ "ok": true, "gate": gate })
    }

    fn request_step_advance(&mut self, arguments: Value) -> Value {
        let Some(run_id) = string_arg(&arguments, "run_id") else {
            return missing_arg("run_id");
        };
        let Some(run) = self.runs.iter_mut().find(|run| run.id == run_id) else {
            return not_found("run", run_id);
        };
        run.status = "ready_for_review".to_string();
        let run = run.clone();
        self.events.push(EventRecord::new(
            "step.advance_requested",
            json!({
                "run_id": run_id,
                "summary": string_arg(&arguments, "summary")
            }),
        ));
        json!({ "ok": true, "run": run })
    }

    fn snapshot(&self) -> Value {
        json!({
            "ok": true,
            "tickets": self.tickets,
            "runs": self.runs,
            "gates": self.gates,
            "events": self.events,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TicketRecord {
    id: String,
    title: String,
    status: String,
    pipeline_id: String,
    created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunRecord {
    id: String,
    ticket_id: String,
    status: String,
    current_step_id: String,
    coordination: CoordinationRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoordinationRecord {
    target_id: String,
    assigned_worktree: String,
    request_id: String,
    owner_plugin: String,
    agent_name: String,
    session_uuid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GateRecord {
    run_id: String,
    gate_id: String,
    status: String,
    summary: Option<String>,
    evidence: Value,
    created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventRecord {
    kind: String,
    payload: Value,
    created_at: u64,
}

impl EventRecord {
    fn new(kind: impl Into<String>, payload: Value) -> Self {
        Self {
            kind: kind.into(),
            payload,
            created_at: now_seconds(),
        }
    }
}

fn read_state(path: &Path) -> Option<ProjectPipelinesState> {
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_state(path: &Path, state: &ProjectPipelinesState) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(state)?;
    fs::write(path, bytes)
}

fn string_arg<'a>(arguments: &'a Value, key: &str) -> Option<&'a str> {
    arguments.get(key).and_then(Value::as_str)
}

fn missing_arg(key: &str) -> Value {
    json!({
        "ok": false,
        "error": {
            "code": "missing_argument",
            "message": format!("missing required argument: {key}")
        }
    })
}

fn not_found(kind: &str, id: &str) -> Value {
    json!({
        "ok": false,
        "error": {
            "code": "not_found",
            "message": format!("{kind} not found: {id}")
        }
    })
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

/// Build invocation context used by daemon-backed MCP calls.
#[must_use]
pub fn mcp_invocation_context() -> PluginInvocationContext {
    PluginInvocationContext {
        client_id: None,
        session_id: None,
        subscription_id: None,
        surface_id: None,
        origin: Some("botster-hub-mcp-serve".to_string()),
        metadata: Some(BoundaryJson(json!({
            "transport": "daemon",
            "surface": "mcp"
        }))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir()
            .join("botster-hub-project-pipelines")
            .join(name)
            .join(nanos.to_string())
    }

    #[test]
    fn mutating_handler_reports_persist_failed_when_state_write_fails() {
        let blocked_root = unique_test_path("persist-failed");
        fs::create_dir_all(blocked_root.parent().expect("test path parent"))
            .expect("create test parent");
        fs::write(&blocked_root, b"not a directory").expect("create state root blocker");

        let runtime = ProjectPipelinesRuntime::new(blocked_root);
        let result = runtime.handle("create", json!({ "title": "Cannot persist" }));

        assert_eq!(result["ok"], false);
        assert_eq!(result["error"]["code"], "persist_failed");
    }
}
