//! MCP protocol pieces that do not depend on a transport.
//!
//! The daemon serves MCP over HTTP (`transport::http_mcp`). This module holds
//! what that transport needs and would share with any other: JSON-RPC framing
//! and errors, the tool descriptor and result types, the native tool table,
//! and the mapping from a `tools/call` to the daemon request it stands for.
//! It performs no I/O.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::daemon::control::message::CallerRequest;
use crate::{DaemonResponse, DaemonResponseKind};
use botster_core::PluginOwnedDescriptor;

/// Protocol revisions this server speaks, newest first. `initialize` answers
/// with the client's revision when it is listed, else with the newest.
const SUPPORTED_PROTOCOL_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];
const SERVER_NAME: &str = "botster-hub";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The result of `initialize`, negotiating the protocol revision.
pub(crate) fn initialize_result(requested: Option<&str>) -> Value {
    let version = requested
        .filter(|requested| SUPPORTED_PROTOCOL_VERSIONS.contains(requested))
        .unwrap_or(SUPPORTED_PROTOCOL_VERSIONS[0]);
    json!({
        "protocolVersion": version,
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

/// One inbound JSON-RPC message, classified.
pub(crate) enum Inbound {
    /// A request that needs a response.
    Request {
        id: Value,
        method: String,
        params: Option<Value>,
    },
    /// A notification, or a response to a server request: acknowledged and
    /// never answered.
    NoReply,
    /// A malformed message: answer with this error.
    Invalid(JsonRpcResponse),
}

/// Classify one HTTP body. A batch (JSON array) is refused: MCP 2025-06-18
/// removed batching.
pub(crate) fn parse_inbound(body: &[u8]) -> Inbound {
    let value = match serde_json::from_slice::<Value>(body) {
        Ok(value) => value,
        Err(error) => {
            return Inbound::Invalid(JsonRpcResponse::error(
                Value::Null,
                JsonRpcError::parse_error(error.to_string()),
            ));
        }
    };
    if !value.is_object() {
        return Inbound::Invalid(JsonRpcResponse::error(
            Value::Null,
            JsonRpcError::invalid_request(
                "a message must be one JSON object; batches are not supported",
            ),
        ));
    }
    let is_response = value.get("result").is_some() || value.get("error").is_some();
    let request = match serde_json::from_value::<JsonRpcRequest>(value) {
        Ok(request) => request,
        Err(error) => {
            return Inbound::Invalid(JsonRpcResponse::error(
                Value::Null,
                JsonRpcError::invalid_request(error.to_string()),
            ));
        }
    };
    match (request.method, request.id) {
        (Some(method), Some(id)) if !id.is_null() => Inbound::Request {
            id,
            method,
            params: request.params,
        },
        // A notification carries a method and no id.
        (Some(_), _) => Inbound::NoReply,
        // A response to a server request carries a result or an error.
        (None, _) if is_response => Inbound::NoReply,
        (None, id) => Inbound::Invalid(JsonRpcResponse::error(
            id.unwrap_or(Value::Null),
            JsonRpcError::invalid_request("missing method"),
        )),
    }
}

/// Encode one JSON-RPC response for the transport.
pub(crate) fn encode_response(response: &JsonRpcResponse) -> Vec<u8> {
    serde_json::to_vec(response).unwrap_or_else(|_| {
        br#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"response was not serializable"}}"#
            .to_vec()
    })
}

pub(crate) fn tool_call_response(result: Result<McpToolResult, McpToolError>) -> Value {
    match result {
        Ok(result) => result.to_mcp_result(),
        Err(error) => error.to_mcp_result(),
    }
}

/// A native tool call resolved to the daemon request it stands for, plus the
/// response shape that request must produce.
pub(crate) struct NativeCall {
    pub(crate) request: CallerRequest,
    pub(crate) expected: &'static str,
}

/// Native hub tools, as `tools/list` shows them. The messaging and
/// orchestrator plugins are to own the tool surface; these stay until then.
pub(crate) fn native_tool_descriptors() -> Vec<McpToolDescriptor> {
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
        McpToolDescriptor::new(
            "whoami",
            "Report the native hub MCP identity available to coordination tools.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false,
            }),
        ),
        McpToolDescriptor::new(
            "post_message",
            "Publish a routed coordination message to one target session.",
            post_message_schema(),
        ),
        McpToolDescriptor::new(
            "post_envelope",
            "Alias of post_message for routed-envelope terminology.",
            post_message_schema(),
        ),
        McpToolDescriptor::new(
            "receive_messages",
            "Return the caller session's unacknowledged messages; each one is returned again until ack_message.",
            receive_messages_schema(),
        ),
        McpToolDescriptor::new(
            "receive_envelopes",
            "Alias of receive_messages for routed-envelope terminology.",
            receive_messages_schema(),
        ),
        McpToolDescriptor::new(
            "ack_message",
            "Acknowledge one delivered routed coordination message for the caller session.",
            ack_message_schema(),
        ),
        McpToolDescriptor::new(
            "ack_envelope",
            "Alias of ack_message for routed-envelope terminology.",
            ack_message_schema(),
        ),
        McpToolDescriptor::new(
            "notify_session",
            "Ring one session: the hub types the message into it when its input takes free text, and answers queued.",
            json!({
                "type": "object",
                "properties": {
                    "hub_id": { "type": "string", "description": "The hub of the target session. Only this hub is supported." },
                    "session_id": { "type": "string" },
                    "message": { "type": "string" }
                },
                "required": ["session_id", "message"],
                "additionalProperties": false,
            }),
        ),
    ]
}

/// True when `name` is a native hub tool.
pub(crate) fn is_native_tool(name: &str) -> bool {
    matches!(
        name,
        "hub.status"
            | "hub.sessions.list"
            | "whoami"
            | "post_message"
            | "post_envelope"
            | "receive_messages"
            | "receive_envelopes"
            | "ack_message"
            | "ack_envelope"
            | "notify_session"
    )
}

/// Resolve one native tool call. The caller is never an argument: the
/// daemon derives it from the request's bearer token.
pub(crate) fn native_call(
    call: &McpCallRequest,
    local_hub_id: &str,
) -> Result<NativeCall, McpToolError> {
    let (request, expected) = match call.name.as_str() {
        "hub.status" => {
            require_no_arguments(call)?;
            (CallerRequest::Status, "status")
        }
        "hub.sessions.list" => {
            require_no_arguments(call)?;
            (CallerRequest::ListSessions, "sessions")
        }
        "whoami" => {
            require_no_arguments(call)?;
            (CallerRequest::Whoami, "identity")
        }
        "post_message" | "post_envelope" => {
            require_local_hub(&call.arguments, local_hub_id)?;
            (
                CallerRequest::PostMessage {
                    target_session_id: required_string(&call.arguments, "session_id")?,
                    envelope_id: optional_string(&call.arguments, "envelope_id")?,
                    body: required_string(&call.arguments, "body")?,
                },
                "message_posted",
            )
        }
        "receive_messages" | "receive_envelopes" => {
            reject_target_inbox_arguments(&call.arguments)?;
            (
                CallerRequest::ReceiveMessages {
                    after: optional_u64(&call.arguments, "after")?,
                    limit: optional_usize(&call.arguments, "limit")?.unwrap_or(32),
                },
                "messages",
            )
        }
        "ack_message" | "ack_envelope" => {
            reject_target_inbox_arguments(&call.arguments)?;
            (
                CallerRequest::AckMessage {
                    envelope_id: required_string(&call.arguments, "envelope_id")?,
                },
                "message_acked",
            )
        }
        "notify_session" => {
            require_local_hub(&call.arguments, local_hub_id)?;
            (
                CallerRequest::NotifySession {
                    session_id: required_string(&call.arguments, "session_id")?,
                    data: required_string(&call.arguments, "message")?,
                },
                "session_notified",
            )
        }
        _ => {
            return Err(McpToolError::new(
                "unknown_tool",
                format!("unknown native hub tool: {}", call.name),
            ));
        }
    };
    Ok(NativeCall { request, expected })
}

/// The plugin tool descriptors in a `PluginMcpListTools` response.
pub(crate) fn plugin_tool_descriptors(response: DaemonResponse) -> Vec<McpToolDescriptor> {
    if response.error.is_some() {
        return Vec::new();
    }
    response
        .plugin_tools
        .into_iter()
        .filter_map(|tool| serde_json::from_value(tool).ok())
        .collect()
}

/// The tool result in a `PluginMcpCallTool` response.
pub(crate) fn plugin_tool_result(
    response: crate::DaemonTransportResult<DaemonResponse>,
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

fn post_message_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "hub_id": { "type": "string", "description": "The hub of the target session. Only this hub is supported." },
            "session_id": { "type": "string" },
            "body": { "type": "string" },
            "envelope_id": { "type": "string" }
        },
        "required": ["session_id", "body"],
        "additionalProperties": false,
    })
}

fn receive_messages_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "after": { "type": "integer", "minimum": 0 },
            "limit": { "type": "integer", "minimum": 1, "maximum": 128 }
        },
        "additionalProperties": false,
    })
}

fn ack_message_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "envelope_id": { "type": "string" }
        },
        "required": ["envelope_id"],
        "additionalProperties": false,
    })
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

pub(crate) fn daemon_tool_result(
    response: crate::DaemonTransportResult<DaemonResponse>,
    expected: &'static str,
    hub_id: &str,
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
                "compatibility": {
                    "protocol": status.compatibility.protocol,
                    "protocol_version": status.compatibility.protocol_version,
                    "features": status.compatibility.features,
                    "conformance_fixture_revision": status.compatibility.conformance_fixture_revision,
                },
                "software": status.software,
                "installation": status.installation,
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
                    "hub_id": hub_id,
                    "session_id": session.session_id,
                    "lifecycle": session.lifecycle,
                })
            }).collect::<Vec<_>>(),
        }))),
        ("identity", DaemonResponseKind::Identity)
        | ("message_posted", DaemonResponseKind::MessagePosted)
        | ("messages", DaemonResponseKind::Messages)
        | ("message_acked", DaemonResponseKind::MessageAcked)
        | ("session_notified", DaemonResponseKind::SessionNotified) => {
            let coordination = response.coordination.ok_or_else(|| {
                McpToolError::new(
                    "daemon_response",
                    "daemon coordination response missing body",
                )
            })?;
            serde_json::to_value(coordination)
                .map(McpToolResult::structured)
                .map_err(|_| {
                    McpToolError::new(
                        "daemon_response",
                        "coordination response was not serializable",
                    )
                })
        }
        _ => Err(McpToolError::new(
            "daemon_response",
            "daemon returned an unexpected response kind",
        )),
    }
}

fn require_no_arguments(call: &McpCallRequest) -> Result<(), McpToolError> {
    if call
        .arguments
        .as_object()
        .is_some_and(serde_json::Map::is_empty)
    {
        Ok(())
    } else {
        Err(McpToolError::new(
            "invalid_arguments",
            format!("{} does not accept arguments", call.name),
        ))
    }
}

fn required_string(arguments: &Value, name: &str) -> Result<String, McpToolError> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| McpToolError::new("invalid_arguments", format!("{name} must be a string")))
}

fn optional_string(arguments: &Value, name: &str) -> Result<Option<String>, McpToolError> {
    match arguments.get(name) {
        Some(value) => value.as_str().map(str::to_string).map(Some).ok_or_else(|| {
            McpToolError::new("invalid_arguments", format!("{name} must be a string"))
        }),
        None => Ok(None),
    }
}

fn optional_u64(arguments: &Value, name: &str) -> Result<Option<u64>, McpToolError> {
    match arguments.get(name) {
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            McpToolError::new(
                "invalid_arguments",
                format!("{name} must be an unsigned integer"),
            )
        }),
        None => Ok(None),
    }
}

fn optional_usize(arguments: &Value, name: &str) -> Result<Option<usize>, McpToolError> {
    optional_u64(arguments, name).and_then(|value| {
        value
            .map(usize::try_from)
            .transpose()
            .map_err(|_| McpToolError::new("invalid_arguments", format!("{name} is too large")))
    })
}

/// A session reference names its hub. Only this hub is served today: a remote
/// `hub_id` is refused before any effect, so a remote session that shares an ID
/// with a local one is never mistaken for it.
fn require_local_hub(arguments: &Value, local_hub_id: &str) -> Result<(), McpToolError> {
    match arguments.get("hub_id") {
        None => Ok(()),
        Some(Value::String(hub_id)) if hub_id == local_hub_id => Ok(()),
        Some(Value::String(_)) => Err(McpToolError::new(
            "remote_hub_unsupported",
            "only sessions of this hub can be addressed",
        )),
        Some(_) => Err(McpToolError::new(
            "invalid_arguments",
            "hub_id must be a string",
        )),
    }
}

fn reject_target_inbox_arguments(arguments: &Value) -> Result<(), McpToolError> {
    if arguments.get("session_id").is_some()
        || arguments.get("agent_id").is_some()
        || arguments.get("hub_id").is_some()
    {
        Err(McpToolError::new(
            "invalid_arguments",
            "receive and ack tools are caller-scoped and do not accept session_id or agent_id",
        ))
    } else {
        Ok(())
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
pub(crate) struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    pub(crate) fn result(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub(crate) fn error(id: Value, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

impl JsonRpcError {
    pub(crate) fn parse_error(message: impl Into<String>) -> Self {
        Self {
            code: -32700,
            message: "parse error".to_string(),
            data: Some(json!({ "detail": message.into() })),
        }
    }

    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: -32600,
            message: message.into(),
            data: None,
        }
    }

    pub(crate) fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("method not found: {method}"),
            data: None,
        }
    }

    pub(crate) fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HUB: &str = "hub-1";

    fn call(name: &str, arguments: Value) -> McpCallRequest {
        McpCallRequest {
            name: name.to_string(),
            arguments,
        }
    }

    #[test]
    fn initialize_answers_a_listed_revision_and_otherwise_the_newest() {
        for version in SUPPORTED_PROTOCOL_VERSIONS {
            assert_eq!(initialize_result(Some(version))["protocolVersion"], version);
        }
        assert_eq!(
            initialize_result(Some("1999-01-01"))["protocolVersion"],
            SUPPORTED_PROTOCOL_VERSIONS[0]
        );
        assert_eq!(
            initialize_result(None)["capabilities"]["tools"]["listChanged"],
            false
        );
    }

    #[test]
    fn inbound_messages_are_classified() {
        assert!(matches!(
            parse_inbound(br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#),
            Inbound::Request { .. }
        ));
        assert!(matches!(
            parse_inbound(br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#),
            Inbound::NoReply
        ));
        assert!(matches!(
            parse_inbound(br#"{"jsonrpc":"2.0","id":7,"result":{}}"#),
            Inbound::NoReply
        ));
        for body in [
            &b"not json"[..],
            &b"[]"[..],
            &br#"[{"jsonrpc":"2.0","id":1,"method":"ping"}]"#[..],
            &b"7"[..],
            &br#"{"jsonrpc":"2.0","id":1}"#[..],
        ] {
            assert!(matches!(parse_inbound(body), Inbound::Invalid(_)));
        }
    }

    #[test]
    fn native_tools_map_to_caller_requests_and_never_take_a_caller() {
        let listed = native_tool_descriptors();
        assert!(listed.iter().all(|tool| is_native_tool(&tool.name)));
        assert!(
            native_call(
                &call("post_message", json!({"session_id": "s", "body": "b"})),
                HUB
            )
            .is_ok()
        );
        // A caller or target inbox in the arguments is refused, never honored.
        for name in ["receive_messages", "ack_message"] {
            let error = native_call(
                &call(name, json!({"session_id": "other", "envelope_id": "e"})),
                HUB,
            )
            .err()
            .expect("a target inbox is refused");
            assert_eq!(error.code, "invalid_arguments");
        }
        // Nor is a hub: the inbox is the caller's, on this hub.
        let error = native_call(&call("receive_messages", json!({"hub_id": HUB})), HUB)
            .err()
            .expect("a hub in a caller-scoped call is refused");
        assert_eq!(error.code, "invalid_arguments");
        assert_eq!(
            native_call(&call("nope", json!({})), HUB)
                .err()
                .map(|error| error.code),
            Some("unknown_tool".to_string())
        );
        assert_eq!(
            native_call(&call("hub.status", json!({"x": 1})), HUB)
                .err()
                .map(|error| error.code),
            Some("invalid_arguments".to_string())
        );
    }

    #[test]
    fn a_session_reference_accepts_this_hub_and_refuses_any_other() {
        for tool in ["post_message", "notify_session"] {
            let text = if tool == "post_message" {
                "body"
            } else {
                "message"
            };
            let arguments = |hub: Value| json!({"hub_id": hub, "session_id": "s", text: "x"});
            assert!(native_call(&call(tool, arguments(json!(HUB))), HUB).is_ok());
            // No hub_id at all means this hub.
            assert!(native_call(&call(tool, json!({"session_id": "s", text: "x"})), HUB).is_ok());
            let refused = native_call(&call(tool, arguments(json!("other-hub"))), HUB)
                .err()
                .expect("a remote hub is refused before any effect");
            assert_eq!(refused.code, "remote_hub_unsupported", "{tool}");
            let malformed = native_call(&call(tool, arguments(json!(7))), HUB)
                .err()
                .expect("a non-string hub is refused");
            assert_eq!(malformed.code, "invalid_arguments", "{tool}");
        }
    }

    #[test]
    fn listed_sessions_name_their_hub() {
        use botster_hub_client::DaemonSession;
        let mut response =
            crate::client_api_dto::response::daemon_response_base(DaemonResponseKind::Sessions);
        response.sessions = vec![DaemonSession {
            session_id: "s".to_string(),
            lifecycle: "running".to_string(),
        }];
        let result = daemon_tool_result(Ok(response), "sessions", HUB).expect("a sessions result");
        let listed = &result.structured_content["sessions"][0];
        assert_eq!(listed["hub_id"], HUB);
        assert_eq!(listed["session_id"], "s");
    }
}
