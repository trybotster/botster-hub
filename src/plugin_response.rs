//! Bounded shaping and encoding for plugin control responses.

use botster_core::{PluginInvocationFailureKind, PluginInvocationResult, RequestId};
use botster_hub_client::{
    DaemonOperatorError, DaemonResponse, DaemonResponseKind, MAX_CONTROL_RESPONSE_BYTES,
    MAX_REQUEST_ID_BYTES,
};
use botster_ui_contract::UiActionRequest;
use serde::Serialize;

use crate::bounded_json::EncodeError as EncodeResponseError;
use crate::client_api::{HubClientOperation, HubClientPluginSurface};
use crate::client_api_dto::response::{
    daemon_plugin_action_result, daemon_plugin_surface, daemon_plugin_tool_result,
    daemon_response_base,
};
use crate::daemon::control::reply::RetainedPluginResult;
use crate::daemon::error::{daemon_operator_error, daemon_plugin_tool_error};
use crate::lifecycle::HubPluginLifecycle;
use crate::runtime::{
    complete_plugin_surface_action_with_lifecycle, complete_plugin_surface_render_with_lifecycle,
};
use crate::{HubRuntime, McpToolError};

pub(crate) enum PluginResponseKind {
    McpTool,
    SurfaceRender {
        package_name: String,
        surface_id: String,
    },
    SurfaceAction {
        package_name: String,
        request: UiActionRequest,
    },
}

pub(crate) struct PluginResponseInput {
    pub(crate) kind: PluginResponseKind,
    pub(crate) lifecycle: HubPluginLifecycle,
    pub(crate) result: RetainedPluginResult<Result<PluginInvocationResult, String>>,
    pub(crate) transport_request_id: String,
    pub(crate) inconsistent: bool,
}

pub(crate) struct PreparedPluginResponse {
    pub(crate) kind: DaemonResponseKind,
    pub(crate) encoded_frame: Vec<u8>,
    pub(crate) logical_bytes: usize,
}

/// Shape and encode one plugin response while its raw-result charge remains held.
pub(crate) fn prepare(input: PluginResponseInput) -> PreparedPluginResponse {
    let PluginResponseInput {
        kind,
        lifecycle,
        result,
        transport_request_id,
        inconsistent,
    } = input;
    let (result, retained_charge) = result.into_parts();
    let response = shape_response(
        &kind,
        &lifecycle,
        result,
        &transport_request_id,
        inconsistent,
    );
    let prepared = match encode_response(response, &transport_request_id) {
        Ok(prepared) => prepared,
        Err(EncodeResponseError::TooLarge) => {
            let response = oversized_response(&kind, &transport_request_id);
            encode_protocol_bounded_error(response, &transport_request_id, "oversize")
        }
        Err(EncodeResponseError::Serialize) => {
            let response = encoding_error_response(&kind, &transport_request_id);
            encode_protocol_bounded_error(response, &transport_request_id, "encoding")
        }
    };
    drop(retained_charge);
    prepared
}

fn shape_response(
    kind: &PluginResponseKind,
    lifecycle: &HubPluginLifecycle,
    result: Result<PluginInvocationResult, String>,
    transport_request_id: &str,
    inconsistent: bool,
) -> DaemonResponse {
    if inconsistent {
        let request_id = match &result {
            Ok(PluginInvocationResult::Completed(success)) => &success.request_id.0,
            Ok(PluginInvocationResult::Failed(failure)) => &failure.request_id.0,
            Err(_) => transport_request_id,
        };
        return plugin_completion_inconsistent(
            kind,
            transport_request_id,
            format!("plugin completion identity did not match admitted request {request_id}"),
        );
    }
    if let Ok(PluginInvocationResult::Failed(failure)) = &result
        && failure.kind == PluginInvocationFailureKind::CompletionTooLarge
    {
        return plugin_preparation_error(
            kind,
            transport_request_id,
            "plugin_response_too_large",
            &failure.reason,
        );
    }
    let result =
        result.map_err(|message| McpToolError::new("plugin_completion_inconsistent", message));
    match kind {
        PluginResponseKind::McpTool => {
            match result.and_then(HubRuntime::complete_plugin_mcp_tool) {
                Ok(value) => daemon_plugin_tool_result(value),
                Err(error) => correlated_mcp_error(error, transport_request_id),
            }
        }
        PluginResponseKind::SurfaceRender {
            package_name,
            surface_id,
        } => match result.and_then(|result| {
            complete_plugin_surface_render_with_lifecycle(lifecycle, package_name, result)
        }) {
            Ok(body) => daemon_plugin_surface(HubClientPluginSurface {
                package_name: package_name.clone(),
                surface_id: surface_id.clone(),
                body,
            }),
            Err(error) => correlated_surface_error(
                HubClientOperation::PluginSurfaceRender,
                error,
                transport_request_id,
            ),
        },
        PluginResponseKind::SurfaceAction {
            package_name,
            request,
        } => match result.and_then(|result| {
            complete_plugin_surface_action_with_lifecycle(lifecycle, package_name, request, result)
        }) {
            Ok(result) => daemon_plugin_action_result(result),
            Err(error) => correlated_surface_error(
                HubClientOperation::PluginSurfaceAction,
                error,
                transport_request_id,
            ),
        },
    }
}

fn plugin_completion_inconsistent(
    kind: &PluginResponseKind,
    transport_request_id: &str,
    message: String,
) -> DaemonResponse {
    let error = McpToolError::new("plugin_completion_inconsistent", message);
    match kind {
        PluginResponseKind::McpTool => correlated_mcp_error(error, transport_request_id),
        PluginResponseKind::SurfaceRender { .. } => correlated_surface_error(
            HubClientOperation::PluginSurfaceRender,
            error,
            transport_request_id,
        ),
        PluginResponseKind::SurfaceAction { .. } => correlated_surface_error(
            HubClientOperation::PluginSurfaceAction,
            error,
            transport_request_id,
        ),
    }
}

fn correlated_mcp_error(error: McpToolError, transport_request_id: &str) -> DaemonResponse {
    let mut response = daemon_plugin_tool_error(error);
    response
        .error
        .as_mut()
        .expect("plugin tool errors carry an operator error")
        .request_id = transport_request_id.to_string();
    response
}

fn correlated_surface_error(
    operation: HubClientOperation,
    error: McpToolError,
    transport_request_id: &str,
) -> DaemonResponse {
    daemon_operator_error(crate::client_api::plugin_error(
        RequestId(transport_request_id.to_string()),
        operation,
        error,
    ))
}

fn oversized_response(kind: &PluginResponseKind, transport_request_id: &str) -> DaemonResponse {
    plugin_preparation_error(
        kind,
        transport_request_id,
        "plugin_response_too_large",
        "plugin response exceeds the control response limit",
    )
}

fn encoding_error_response(
    kind: &PluginResponseKind,
    transport_request_id: &str,
) -> DaemonResponse {
    plugin_preparation_error(
        kind,
        transport_request_id,
        "plugin_response_encode_failed",
        "plugin response serialization failed",
    )
}

fn plugin_preparation_error(
    kind: &PluginResponseKind,
    transport_request_id: &str,
    code: &str,
    message: &str,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: transport_request_id.to_string(),
        operation: operation_label(kind).to_string(),
        message: message.to_string(),
        diagnostics: Vec::new(),
    });
    response
}

fn operation_label(kind: &PluginResponseKind) -> &'static str {
    match kind {
        PluginResponseKind::McpTool => "plugin_mcp_call",
        PluginResponseKind::SurfaceRender { .. } => "plugin_surface_render",
        PluginResponseKind::SurfaceAction { .. } => "plugin_surface_action",
    }
}

pub(crate) fn encode_response(
    response: DaemonResponse,
    transport_request_id: &str,
) -> Result<PreparedPluginResponse, EncodeResponseError> {
    #[derive(Serialize)]
    #[serde(tag = "frame", rename_all = "snake_case")]
    enum BorrowedServerFrame<'a> {
        Response {
            request_id: &'a str,
            response: &'a DaemonResponse,
        },
    }

    let frame = BorrowedServerFrame::Response {
        request_id: transport_request_id,
        response: &response,
    };
    let encoded_frame = crate::bounded_json::encode(&frame, MAX_CONTROL_RESPONSE_BYTES)?;
    let kind = response.kind;
    drop(response);
    let logical_bytes = encoded_frame.len();
    Ok(PreparedPluginResponse {
        kind,
        encoded_frame,
        logical_bytes,
    })
}

pub(crate) fn encode_protocol_bounded_error(
    response: DaemonResponse,
    transport_request_id: &str,
    error_kind: &str,
) -> PreparedPluginResponse {
    // Production ingress permits only canonical decimal u64 request IDs.
    // The byte bound also covers any 20-byte internal ID with worst-case JSON escaping.
    assert!(
        transport_request_id.len() <= MAX_REQUEST_ID_BYTES,
        "plugin {error_kind} fallback request id exceeds the {MAX_REQUEST_ID_BYTES}-byte protocol bound"
    );
    match encode_response(response, transport_request_id) {
        Ok(prepared) => prepared,
        Err(EncodeResponseError::TooLarge) => panic!(
            "plugin {error_kind} fallback exceeded the {MAX_CONTROL_RESPONSE_BYTES}-byte response bound with a protocol-bounded request id"
        ),
        Err(EncodeResponseError::Serialize) => {
            panic!("plugin {error_kind} fallback protocol serialization failed")
        }
    }
}

#[cfg(test)]
mod tests {
    use botster_core::{
        BoundaryJson, PluginHandlerKind, PluginHandlerRef, PluginInvocationFailureKind,
        PluginInvocationSuccess, PluginKey, PluginWorkerEngineConfig,
    };
    use botster_hub_client::ServerFrame;

    use super::*;
    use crate::daemon::control::reply::RetainedPluginResultBudget;

    fn lifecycle() -> HubPluginLifecycle {
        HubPluginLifecycle::with_config(PluginWorkerEngineConfig::default())
    }

    fn completed(payload: serde_json::Value) -> PluginInvocationResult {
        PluginInvocationResult::Completed(PluginInvocationSuccess {
            request_id: RequestId("core-request".to_string()),
            handler: PluginHandlerRef {
                plugin_key: PluginKey("example.plugin".to_string()),
                kind: PluginHandlerKind::Command,
                handler_id: "handler".to_string(),
            },
            payload: Some(BoundaryJson(payload)),
        })
    }

    fn input(
        kind: PluginResponseKind,
        result: Result<PluginInvocationResult, String>,
        transport_request_id: &str,
    ) -> (PluginResponseInput, RetainedPluginResultBudget) {
        let budget = RetainedPluginResultBudget::new();
        let charge = budget.try_reserve(1).expect("test result charge");
        (
            PluginResponseInput {
                kind,
                lifecycle: lifecycle(),
                result: RetainedPluginResult::new(result, charge),
                transport_request_id: transport_request_id.to_string(),
                inconsistent: false,
            },
            budget,
        )
    }

    fn decode_frame(prepared: &PreparedPluginResponse) -> ServerFrame {
        serde_json::from_slice(&prepared.encoded_frame).expect("encoded server frame")
    }

    fn decode_response(prepared: &PreparedPluginResponse) -> (String, DaemonResponse) {
        match decode_frame(prepared) {
            ServerFrame::Response {
                request_id,
                response,
            } => (request_id, response),
            frame => panic!("expected response frame, got {frame:?}"),
        }
    }

    #[test]
    fn surface_response_keeps_one_canonical_snapshot_body() {
        let body = serde_json::json!({
            "type": "text",
            "id": "large-body",
            "props": { "text": "x".repeat(MAX_CONTROL_RESPONSE_BYTES / 2) }
        });
        let (input, budget) = input(
            PluginResponseKind::SurfaceRender {
                package_name: "example.plugin".to_string(),
                surface_id: "example.surface".to_string(),
            },
            Ok(completed(body)),
            "19",
        );

        let prepared = prepare(input);

        assert_eq!(prepared.kind, DaemonResponseKind::PluginSurface);
        assert!(prepared.encoded_frame.len() <= MAX_CONTROL_RESPONSE_BYTES);
        assert_eq!(prepared.logical_bytes, prepared.encoded_frame.len());
        assert_eq!(budget.retained_bytes(), 0);
        let (_, response) = decode_response(&prepared);
        let response_json = serde_json::to_value(response).expect("response JSON");
        let surface = &response_json["plugin_surface"];
        assert!(surface.get("body").is_none());
        assert_eq!(surface["ui_tree_snapshot"]["body"]["id"], "large-body");
    }

    #[test]
    fn core_oversized_completion_keeps_transport_correlation() {
        let result = PluginInvocationResult::Failed(botster_core::PluginInvocationFailure {
            request_id: RequestId("core-request".to_string()),
            handler: botster_core::PluginHandlerRef {
                plugin_key: PluginKey("example.plugin".to_string()),
                kind: PluginHandlerKind::Command,
                handler_id: "handler".to_string(),
            },
            kind: PluginInvocationFailureKind::CompletionTooLarge,
            timeout_ms: None,
            reason: "completion exceeded reserved byte budget".to_string(),
        });
        for kind in [
            PluginResponseKind::McpTool,
            PluginResponseKind::SurfaceRender {
                package_name: "example.plugin".to_string(),
                surface_id: "example.surface".to_string(),
            },
            PluginResponseKind::SurfaceAction {
                package_name: "example.plugin".to_string(),
                request: serde_json::from_value(serde_json::json!({
                    "surface_id": "example.surface",
                    "action_id": "run",
                    "request_id": "action-request",
                    "kind": "submit"
                }))
                .expect("action request"),
            },
        ] {
            let (input, budget) = input(kind, Ok(result.clone()), "transport-request");
            let prepared = prepare(input);
            let (request_id, response) = decode_response(&prepared);
            assert_eq!(request_id, "transport-request");
            let error = response.error.expect("typed oversized response");
            assert_eq!(error.code, "plugin_response_too_large");
            assert_eq!(error.message, "completion exceeded reserved byte budget");
            assert_eq!(error.request_id, "transport-request");
            assert_eq!(budget.retained_bytes(), 0);
        }
    }

    #[test]
    fn exact_frame_limit_is_accepted() {
        let request_id = "20";
        let empty_response = daemon_plugin_tool_result(serde_json::Value::String(String::new()));
        let empty_frame = ServerFrame::Response {
            request_id: request_id.to_string(),
            response: empty_response,
        };
        let empty_len = serde_json::to_vec(&empty_frame).expect("empty frame").len();
        let padding = MAX_CONTROL_RESPONSE_BYTES - empty_len;
        let (input, _) = input(
            PluginResponseKind::McpTool,
            Ok(completed(serde_json::Value::String("x".repeat(padding)))),
            request_id,
        );

        let prepared = prepare(input);

        assert_eq!(prepared.kind, DaemonResponseKind::PluginMcpToolResult);
        assert_eq!(prepared.encoded_frame.len(), MAX_CONTROL_RESPONSE_BYTES);
        assert_eq!(prepared.logical_bytes, MAX_CONTROL_RESPONSE_BYTES);
    }

    #[test]
    fn oversized_frame_becomes_a_bounded_correlated_error() {
        let request_id = "18446744073709551615";
        let empty_response = daemon_plugin_tool_result(serde_json::Value::String(String::new()));
        let empty_frame = ServerFrame::Response {
            request_id: request_id.to_string(),
            response: empty_response,
        };
        let padding = MAX_CONTROL_RESPONSE_BYTES
            - serde_json::to_vec(&empty_frame).expect("empty frame").len()
            + 1;
        let (input, _) = input(
            PluginResponseKind::McpTool,
            Ok(completed(serde_json::Value::String("x".repeat(padding)))),
            request_id,
        );

        let prepared = prepare(input);

        assert!(prepared.encoded_frame.len() <= MAX_CONTROL_RESPONSE_BYTES);
        assert_eq!(prepared.kind, DaemonResponseKind::OperatorError);
        let (outer_request_id, response) = decode_response(&prepared);
        let error = response.error.as_ref().expect("operator error");
        assert_eq!(error.code, "plugin_response_too_large");
        assert_eq!(error.request_id, request_id);
        assert_eq!(outer_request_id, request_id);
        assert_eq!(prepared.logical_bytes, prepared.encoded_frame.len());
    }

    #[test]
    fn oversized_fallback_fits_with_a_worst_case_escaped_request_id() {
        let request_id = "\0".repeat(MAX_REQUEST_ID_BYTES);
        assert_eq!(request_id.len(), MAX_REQUEST_ID_BYTES);
        let (input, _) = input(
            PluginResponseKind::McpTool,
            Ok(completed(serde_json::Value::String(
                "x".repeat(MAX_CONTROL_RESPONSE_BYTES),
            ))),
            &request_id,
        );

        let prepared = prepare(input);
        let (outer_request_id, response) = decode_response(&prepared);
        let error = response.error.as_ref().expect("operator error");

        assert_eq!(prepared.kind, DaemonResponseKind::OperatorError);
        assert_eq!(error.code, "plugin_response_too_large");
        assert_eq!(error.request_id, request_id);
        assert_eq!(outer_request_id, request_id);
        assert!(prepared.encoded_frame.len() <= MAX_CONTROL_RESPONSE_BYTES);
        assert_eq!(prepared.logical_bytes, prepared.encoded_frame.len());
    }

    #[test]
    fn metadata_escaping_uses_the_encoded_json_byte_count() {
        let metadata = "quote=\" slash=\\ newline=\n snowman=☃";
        let (input, _) = input(
            PluginResponseKind::McpTool,
            Ok(completed(serde_json::json!({ "metadata": metadata }))),
            "22",
        );

        let prepared = prepare(input);
        let (_, response) = decode_response(&prepared);

        assert_eq!(prepared.logical_bytes, prepared.encoded_frame.len());
        assert_eq!(response.plugin_tool_result["metadata"], metadata);
        assert!(prepared.encoded_frame.len() > metadata.len());
    }

    #[test]
    fn completion_error_keeps_inner_and_outer_transport_correlation() {
        let request_id = "23";
        let (mut input, _) = input(
            PluginResponseKind::SurfaceRender {
                package_name: "example.plugin".to_string(),
                surface_id: "example.surface".to_string(),
            },
            Ok(completed(serde_json::json!({
                "wrong": "handler metadata must not be converted"
            }))),
            request_id,
        );
        input.inconsistent = true;

        let prepared = prepare(input);
        let (outer_request_id, response) = decode_response(&prepared);
        let error = response.error.as_ref().expect("operator error");

        assert_eq!(error.code, "plugin_completion_inconsistent");
        assert_eq!(error.request_id, request_id);
        assert_eq!(error.operation, "plugin_surface_render");
        assert_eq!(outer_request_id, request_id);
        assert_eq!(prepared.kind, DaemonResponseKind::OperatorError);
        assert_eq!(prepared.logical_bytes, prepared.encoded_frame.len());
    }
}
