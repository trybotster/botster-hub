//! Agent-facing MCP stdio adapter and hub tool registry.
//!
//! The stdio protocol is newline-delimited JSON-RPC: one JSON object per line
//! on stdout, with diagnostics reserved for stderr by callers.

use std::error::Error;
use std::fmt;
use std::io::{BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    DaemonRequest, DaemonResponse, DaemonResponseKind, HubConfig, daemon_transport_request,
};
use botster_core::PluginOwnedDescriptor;

const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const SERVER_NAME: &str = "botster-hub";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Run the MCP stdio server until stdin closes.
pub fn serve_mcp_stdio<R, W>(config: HubConfig, input: R, output: W) -> Result<(), McpServeError>
where
    R: BufRead,
    W: Write,
{
    let mut registry = McpToolRegistry::from_provider(NativeHubToolProvider::new(config.clone()));
    registry.register_provider(PluginHubToolProvider::new(config));
    McpStdioServer::new(registry).serve(input, output)
}

struct McpStdioServer {
    registry: McpToolRegistry,
    initialized: bool,
}

impl McpStdioServer {
    fn new(registry: McpToolRegistry) -> Self {
        Self {
            registry,
            initialized: false,
        }
    }

    fn serve<R, W>(&mut self, input: R, mut output: W) -> Result<(), McpServeError>
    where
        R: BufRead,
        W: Write,
    {
        for line in input.lines() {
            let line = line.map_err(McpServeError::Io)?;
            if line.trim().is_empty() {
                continue;
            }

            let response = self.handle_line(&line);
            if let Some(response) = response {
                let bytes = serde_json::to_vec(&response).map_err(McpServeError::Json)?;
                output.write_all(&bytes).map_err(McpServeError::Io)?;
                output.write_all(b"\n").map_err(McpServeError::Io)?;
                output.flush().map_err(McpServeError::Io)?;
            }
        }

        Ok(())
    }

    fn handle_line(&mut self, line: &str) -> Option<JsonRpcResponse> {
        let request = match serde_json::from_str::<JsonRpcRequest>(line) {
            Ok(request) => request,
            Err(error) => {
                return Some(JsonRpcResponse::error(
                    Value::Null,
                    JsonRpcError::parse_error(error.to_string()),
                ));
            }
        };

        let id = request.id.clone()?;

        let Some(method) = request.method.as_deref() else {
            return Some(JsonRpcResponse::error(
                id,
                JsonRpcError::invalid_request("missing method"),
            ));
        };

        match method {
            "initialize" => {
                self.initialized = true;
                Some(JsonRpcResponse::result(id, initialize_result()))
            }
            "tools/list" => {
                if !self.initialized {
                    return Some(JsonRpcResponse::error(
                        id,
                        JsonRpcError::invalid_request("initialize must be called first"),
                    ));
                }
                Some(JsonRpcResponse::result(
                    id,
                    json!({ "tools": self.registry.list_tools() }),
                ))
            }
            "tools/call" => {
                if !self.initialized {
                    return Some(JsonRpcResponse::error(
                        id,
                        JsonRpcError::invalid_request("initialize must be called first"),
                    ));
                }
                let call = match McpCallRequest::from_params(request.params) {
                    Ok(call) => call,
                    Err(error) => {
                        return Some(JsonRpcResponse::error(
                            id,
                            JsonRpcError::invalid_params(error),
                        ));
                    }
                };
                Some(JsonRpcResponse::result(
                    id,
                    tool_call_response(self.registry.call_tool(call)),
                ))
            }
            _ => Some(JsonRpcResponse::error(
                id,
                JsonRpcError::method_not_found(method),
            )),
        }
    }
}

fn tool_call_response(result: Result<McpToolResult, McpToolError>) -> Value {
    match result {
        Ok(result) => result.to_mcp_result(),
        Err(error) => error.to_mcp_result(),
    }
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "serverInfo": {
            "name": SERVER_NAME,
            "version": SERVER_VERSION,
        },
        "capabilities": {
            "tools": {
                "listChanged": false,
            },
        },
    })
}

/// Registry for agent-facing MCP tools.
pub struct McpToolRegistry {
    providers: Vec<Box<dyn McpToolProvider>>,
}

impl McpToolRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    #[must_use]
    pub fn from_provider(provider: impl McpToolProvider + 'static) -> Self {
        let mut registry = Self::new();
        registry.register_provider(provider);
        registry
    }

    pub fn register_provider(&mut self, provider: impl McpToolProvider + 'static) {
        self.providers.push(Box::new(provider));
    }

    #[must_use]
    pub fn list_tools(&self) -> Vec<McpToolDescriptor> {
        let mut tools = self
            .providers
            .iter()
            .flat_map(|provider| provider.list_tools())
            .collect::<Vec<_>>();
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        tools
    }

    pub fn call_tool(&self, call: McpCallRequest) -> Result<McpToolResult, McpToolError> {
        for provider in &self.providers {
            if provider.provides_tool(&call.name) {
                return provider.call_tool(call);
            }
        }

        Err(McpToolError::new(
            "unknown_tool",
            format!("unknown MCP tool: {}", call.name),
        ))
    }
}

impl Default for McpToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Fallible owned-message provider boundary for native and future plugin tools.
pub trait McpToolProvider: Send + Sync {
    fn list_tools(&self) -> Vec<McpToolDescriptor>;
    fn call_tool(&self, call: McpCallRequest) -> Result<McpToolResult, McpToolError>;

    fn provides_tool(&self, name: &str) -> bool {
        self.list_tools().iter().any(|tool| tool.name == name)
    }
}

/// Native hub MCP tools that call the running daemon/client API path.
#[derive(Debug, Clone)]
pub struct NativeHubToolProvider {
    config: HubConfig,
}

impl NativeHubToolProvider {
    #[must_use]
    pub fn new(config: HubConfig) -> Self {
        Self { config }
    }
}

impl McpToolProvider for NativeHubToolProvider {
    fn list_tools(&self) -> Vec<McpToolDescriptor> {
        vec![
            McpToolDescriptor::new(
                "hub.sessions.list",
                "List local hub sessions through the running daemon.",
                json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false,
                }),
            ),
            McpToolDescriptor::new(
                "hub.status",
                "Report sanitized local hub daemon status.",
                json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false,
                }),
            ),
        ]
    }

    fn call_tool(&self, call: McpCallRequest) -> Result<McpToolResult, McpToolError> {
        if !call
            .arguments
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
        {
            return Err(McpToolError::new(
                "invalid_arguments",
                format!("{} does not accept arguments", call.name),
            ));
        }

        match call.name.as_str() {
            "hub.status" => daemon_tool_result(
                daemon_transport_request(&self.config, DaemonRequest::Status),
                "status",
            ),
            "hub.sessions.list" => daemon_tool_result(
                daemon_transport_request(&self.config, DaemonRequest::ListSessions),
                "sessions",
            ),
            _ => Err(McpToolError::new(
                "unknown_tool",
                format!("unknown native hub tool: {}", call.name),
            )),
        }
    }

    fn provides_tool(&self, name: &str) -> bool {
        matches!(name, "hub.status" | "hub.sessions.list")
    }
}

/// Plugin-backed MCP tools delegated to the running daemon-owned `HubRuntime`.
#[derive(Debug, Clone)]
pub struct PluginHubToolProvider {
    config: HubConfig,
}

impl PluginHubToolProvider {
    #[must_use]
    pub fn new(config: HubConfig) -> Self {
        Self { config }
    }
}

impl McpToolProvider for PluginHubToolProvider {
    fn list_tools(&self) -> Vec<McpToolDescriptor> {
        match daemon_transport_request(&self.config, DaemonRequest::PluginMcpListTools) {
            Ok(response) if response.error.is_none() => response.plugin_tools,
            _ => Vec::new(),
        }
    }

    fn call_tool(&self, call: McpCallRequest) -> Result<McpToolResult, McpToolError> {
        let response = daemon_transport_request(
            &self.config,
            DaemonRequest::PluginMcpCallTool {
                name: call.name,
                arguments: call.arguments,
            },
        )
        .map_err(|error| {
            McpToolError::new(
                "daemon_unavailable",
                format!("hub daemon request failed: {error}"),
            )
        })?;
        if let Some(error) = response.error {
            return Err(McpToolError::new(error.code, error.message));
        }
        match response.kind {
            DaemonResponseKind::PluginMcpToolResult => {
                Ok(McpToolResult::structured(response.plugin_tool_result))
            }
            _ => Err(McpToolError::new(
                "daemon_response",
                "daemon returned an unexpected response kind",
            )),
        }
    }
}

/// Convert a plugin-owned MCP descriptor body into an MCP tool descriptor.
#[must_use]
pub fn mcp_descriptor_from_plugin(descriptor: PluginOwnedDescriptor) -> Option<McpToolDescriptor> {
    let name = descriptor.body.0.get("name")?.as_str()?.to_string();
    let description = descriptor
        .body
        .0
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let input_schema = descriptor
        .body
        .0
        .get("input_schema")
        .cloned()
        .or_else(|| descriptor.body.0.get("inputSchema").cloned())
        .unwrap_or_else(|| json!({ "type": "object", "additionalProperties": false }));
    Some(McpToolDescriptor::new(name, description, input_schema))
}

fn daemon_tool_result(
    response: crate::DaemonTransportResult<DaemonResponse>,
    expected: &'static str,
) -> Result<McpToolResult, McpToolError> {
    let response = response.map_err(|error| {
        McpToolError::new(
            "daemon_unavailable",
            format!("hub daemon request failed: {error}"),
        )
    })?;
    if let Some(error) = response.error {
        return Err(McpToolError::new(error.code, error.message));
    }
    match (expected, response.kind) {
        ("status", DaemonResponseKind::Status) => {
            let status = response.status.ok_or_else(|| {
                McpToolError::new("daemon_response", "daemon status response missing status")
            })?;
            Ok(McpToolResult::structured(json!({
                "lifecycle_state": status.lifecycle_state,
                "host_id": status.host_id,
                "host_display_name": status.host_display_name,
                "schema_version": status.schema_version,
                "data_dir_configured": status.data_dir_configured,
                "core_initialized": status.core_initialized,
                "state_source": status.state_source,
                "package_count": status.package_count,
                "enabled_package_count": status.enabled_package_count,
                "provider_count": status.provider_count,
                "enabled_provider_count": status.enabled_provider_count,
                "session_count": status.session_count,
                "recovered_session_count": status.recovered_sessions.len(),
                "stale_session_count": status.stale_sessions.len(),
            })))
        }
        ("sessions", DaemonResponseKind::Sessions) => Ok(McpToolResult::structured(json!({
            "session_count": response.sessions.len(),
            "sessions": response.sessions.into_iter().map(|session| {
                json!({
                    "session_id": session.session_id,
                    "lifecycle": session.lifecycle,
                })
            }).collect::<Vec<_>>(),
        }))),
        _ => Err(McpToolError::new(
            "daemon_response",
            "daemon returned an unexpected response kind",
        )),
    }
}

/// MCP tool descriptor as exposed by `tools/list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolDescriptor {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl McpToolDescriptor {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
        }
    }
}

/// Owned MCP tool call passed through the registry.
#[derive(Debug, Clone, PartialEq)]
pub struct McpCallRequest {
    pub name: String,
    pub arguments: Value,
}

impl McpCallRequest {
    pub fn from_params(params: Option<Value>) -> Result<Self, String> {
        let params = params.ok_or_else(|| "tools/call requires params".to_string())?;
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| "tools/call params.name must be a string".to_string())?
            .to_string();
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !arguments.is_object() {
            return Err("tools/call params.arguments must be an object".to_string());
        }
        Ok(Self { name, arguments })
    }
}

/// Structured tool result returned through MCP `tools/call`.
#[derive(Debug, Clone, PartialEq)]
pub struct McpToolResult {
    structured_content: Value,
}

impl McpToolResult {
    #[must_use]
    pub fn structured(structured_content: Value) -> Self {
        Self { structured_content }
    }

    fn to_mcp_result(&self) -> Value {
        let text = serde_json::to_string(&self.structured_content)
            .unwrap_or_else(|_| "{\"error\":\"unserializable\"}".to_string());
        json!({
            "content": [
                {
                    "type": "text",
                    "text": text,
                }
            ],
            "structuredContent": self.structured_content,
            "isError": false,
        })
    }
}

/// Structured tool execution error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpToolError {
    pub code: String,
    pub message: String,
}

impl McpToolError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    fn to_mcp_result(&self) -> Value {
        json!({
            "content": [
                {
                    "type": "text",
                    "text": self.message,
                }
            ],
            "structuredContent": {
                "error": {
                    "code": self.code,
                    "message": self.message,
                }
            },
            "isError": true,
        })
    }
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    fn result(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Value, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

impl JsonRpcError {
    fn parse_error(message: impl Into<String>) -> Self {
        Self {
            code: -32700,
            message: "parse error".to_string(),
            data: Some(json!({ "detail": message.into() })),
        }
    }

    fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: -32600,
            message: message.into(),
            data: None,
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("method not found: {method}"),
            data: None,
        }
    }

    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }
}

/// Error returned by the MCP stdio server.
#[derive(Debug)]
pub enum McpServeError {
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl fmt::Display for McpServeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "MCP stdio I/O error: {error}"),
            Self::Json(error) => write!(formatter, "MCP JSON error: {error}"),
        }
    }
}

impl Error for McpServeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct FakeProvider {
        failures: BTreeMap<String, McpToolError>,
    }

    impl McpToolProvider for FakeProvider {
        fn list_tools(&self) -> Vec<McpToolDescriptor> {
            vec![McpToolDescriptor::new(
                "fake.echo",
                "Echo test arguments.",
                json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" },
                    },
                    "additionalProperties": false,
                }),
            )]
        }

        fn call_tool(&self, call: McpCallRequest) -> Result<McpToolResult, McpToolError> {
            if let Some(error) = self.failures.get(&call.name) {
                return Err(error.clone());
            }
            Ok(McpToolResult::structured(json!({
                "name": call.name,
                "arguments": call.arguments,
            })))
        }
    }

    #[test]
    fn registry_lists_dispatches_and_reports_structured_errors() {
        let mut registry = McpToolRegistry::new();
        registry.register_provider(FakeProvider::default());

        let tools = registry.list_tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "fake.echo");

        let result = registry
            .call_tool(McpCallRequest {
                name: "fake.echo".to_string(),
                arguments: json!({ "value": "hello" }),
            })
            .expect("fake provider should handle tool");
        assert_eq!(result.structured_content["arguments"]["value"], "hello");

        let error = registry
            .call_tool(McpCallRequest {
                name: "fake.missing".to_string(),
                arguments: json!({}),
            })
            .expect_err("unknown tool should fail");
        assert_eq!(error.code, "unknown_tool");
    }

    #[test]
    fn stdio_server_uses_newline_delimited_json_rpc_without_content_length() {
        let registry = McpToolRegistry::from_provider(FakeProvider::default());
        let mut server = McpStdioServer::new(registry);
        let input = concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fake.echo","arguments":{"value":"ok"}}}"#,
            "\n",
        );
        let mut output = Vec::new();

        server
            .serve(std::io::Cursor::new(input), &mut output)
            .expect("serve MCP requests");

        let output = String::from_utf8(output).expect("utf8 MCP output");
        assert!(!output.contains("Content-Length"));
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 3);
        for line in &lines {
            serde_json::from_str::<Value>(line).expect("each stdout line is one JSON object");
        }
        let initialize = serde_json::from_str::<Value>(lines[0]).expect("initialize json");
        assert_eq!(
            initialize["result"]["protocolVersion"],
            MCP_PROTOCOL_VERSION
        );
        assert_eq!(
            initialize["result"]["capabilities"]["tools"]["listChanged"],
            false
        );
        let call = serde_json::from_str::<Value>(lines[2]).expect("call json");
        assert_eq!(call["result"]["isError"], false);
        assert_eq!(
            call["result"]["structuredContent"]["arguments"]["value"],
            "ok"
        );
    }
}
