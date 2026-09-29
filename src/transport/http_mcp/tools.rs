//! MCP methods over the control channel.
//!
//! Every request reaches the owner as a `ControlMessage::CallerRequest` that
//! carries the bearer token. The owner verifies it and runs the request as
//! the session it proves. This module never decides who a caller is: it only
//! turns a refusal from the owner into a 401.

use std::time::Instant;

use botster_core::SessionId;
use botster_hub_client::{DaemonResponse, ServerFrame};
use serde_json::{Value, json};
use tokio::sync::mpsc::error::TrySendError;

use crate::daemon::control::caller::CALLER_UNAUTHENTICATED;
use crate::daemon::control::message::{
    CallerRequest, ControlMessage, ControlSender, control_reply_channel,
};
use crate::daemon::control::reply::ControlReply;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::mcp::{
    Inbound, JsonRpcError, JsonRpcResponse, McpCallRequest, McpToolDescriptor, McpToolError,
    McpToolResult, daemon_tool_result, encode_response, initialize_result, is_native_tool,
    native_call, native_tool_descriptors, parse_inbound, plugin_tool_descriptors,
    plugin_tool_result, tool_call_response,
};
use crate::HubClientError;
use crate::session_credential::CallerToken;
use crate::transport::http_mcp::wire::Refusal;

/// What one HTTP body turns into.
pub(crate) enum Dispatched {
    /// A JSON-RPC response body.
    Body(Vec<u8>),
    /// A notification or a response: 202, no body.
    Accepted,
    /// The request cannot be served; the refusal names why.
    Refused(Refusal),
}

/// Why a request could not reach a result.
enum CallFailure {
    /// The owner did not accept the token.
    Unauthenticated,
    /// The control queue is full.
    Busy,
    /// The owner is gone.
    Stopped,
}

impl CallFailure {
    fn refusal(self) -> Refusal {
        match self {
            Self::Unauthenticated => Refusal::Unauthenticated,
            Self::Busy | Self::Stopped => Refusal::Busy,
        }
    }
}

struct CallerChannel<'a> {
    control_tx: &'a ControlSender,
    token: &'a CallerToken,
}

impl CallerChannel<'_> {
    /// One request to the owner. Requests that reach Core prove the token in
    /// their own Core submission. The others (status, the session list,
    /// plugin tools) are preceded by a proof: a `Whoami`, whose proven
    /// session travels with them.
    async fn call(
        &self,
        request: CallerRequest,
    ) -> Result<DaemonTransportResult<DaemonResponse>, CallFailure> {
        let proven = if request.proves_in_core() {
            None
        } else {
            Some(self.prove().await?)
        };
        self.send(request, proven).await
    }

    /// The session the token proves, from a `Whoami` the owner answers.
    async fn prove(&self) -> Result<SessionId, CallFailure> {
        let response = self
            .send(CallerRequest::Whoami, None)
            .await?
            .map_err(|_| CallFailure::Stopped)?;
        response
            .coordination
            .and_then(|coordination| coordination.identity)
            .and_then(|identity| identity.caller_session_id)
            .map(SessionId)
            .ok_or(CallFailure::Unauthenticated)
    }

    /// One message to the owner. The queue is never waited on: a full queue
    /// is `Busy`, so a connection cannot park behind the owner.
    async fn send(
        &self,
        request: CallerRequest,
        proven: Option<SessionId>,
    ) -> Result<DaemonTransportResult<DaemonResponse>, CallFailure> {
        let (reply_tx, reply_rx) = control_reply_channel();
        self.control_tx
            .try_send(ControlMessage::CallerRequest {
                token: self.token.clone(),
                proven,
                request: Box::new(request),
                reply_tx,
                enqueued_at: Instant::now(),
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => CallFailure::Busy,
                TrySendError::Closed(_) => CallFailure::Stopped,
            })?;
        let reply = reply_rx.await.map_err(|_| CallFailure::Stopped)?;
        let response = match reply {
            ControlReply::Typed {
                response, charge, ..
            } => {
                let response = *response;
                // The reply's storage charge ends here: the body is copied
                // into the HTTP response next.
                drop(charge);
                response
            }
            ControlReply::EncodedPlugin {
                encoded_frame,
                charge,
                ..
            } => {
                let decoded = serde_json::from_slice::<ServerFrame>(&encoded_frame);
                drop(encoded_frame);
                drop(charge);
                match decoded {
                    Ok(ServerFrame::Response { response, .. }) => Ok(response),
                    _ => Err(DaemonTransportError::UnexpectedResponse),
                }
            }
        };
        if refused_token(&response) {
            return Err(CallFailure::Unauthenticated);
        }
        Ok(response)
    }
}

/// True when the owner refused the bearer token: as a typed response, or as
/// the client API error a Core submission returns when its proof fails.
fn refused_token(response: &DaemonTransportResult<DaemonResponse>) -> bool {
    match response {
        Ok(response) => response
            .error
            .as_ref()
            .is_some_and(|error| error.code == CALLER_UNAUTHENTICATED),
        Err(DaemonTransportError::Client(HubClientError::CallerUnauthenticated { .. })) => true,
        Err(_) => false,
    }
}

/// Serve one HTTP body for the session that `token` claims to be.
pub(crate) async fn dispatch(
    control_tx: &ControlSender,
    token: &CallerToken,
    body: &[u8],
) -> Dispatched {
    let channel = CallerChannel { control_tx, token };
    match serve(&channel, body).await {
        Ok(dispatched) => dispatched,
        Err(failure) => Dispatched::Refused(failure.refusal()),
    }
}

async fn serve(channel: &CallerChannel<'_>, body: &[u8]) -> Result<Dispatched, CallFailure> {
    let (id, method, params) = match parse_inbound(body) {
        Inbound::Request { id, method, params } => (id, method, params),
        Inbound::NoReply => {
            verify(channel).await?;
            return Ok(Dispatched::Accepted);
        }
        Inbound::Invalid(response) => {
            verify(channel).await?;
            return Ok(Dispatched::Body(encode_response(&response)));
        }
    };
    let result = match method.as_str() {
        "initialize" => {
            verify(channel).await?;
            let requested = params
                .as_ref()
                .and_then(|params| params.get("protocolVersion"))
                .and_then(Value::as_str);
            initialize_result(requested)
        }
        "ping" => {
            verify(channel).await?;
            json!({})
        }
        "tools/list" => json!({ "tools": list_tools(channel).await? }),
        "tools/call" => {
            let call = match McpCallRequest::from_params(params) {
                Ok(call) => call,
                Err(error) => {
                    verify(channel).await?;
                    return Ok(Dispatched::Body(encode_response(&JsonRpcResponse::error(
                        id,
                        JsonRpcError::invalid_params(error),
                    ))));
                }
            };
            tool_call_response(call_tool(channel, call).await?)
        }
        _ => {
            verify(channel).await?;
            return Ok(Dispatched::Body(encode_response(&JsonRpcResponse::error(
                id,
                JsonRpcError::method_not_found(&method),
            ))));
        }
    };
    Ok(Dispatched::Body(encode_response(&JsonRpcResponse::result(
        id, result,
    ))))
}

/// Prove the token without doing anything else, for methods that would
/// otherwise never reach the owner.
async fn verify(channel: &CallerChannel<'_>) -> Result<(), CallFailure> {
    channel.call(CallerRequest::Whoami).await.map(|_| ())
}

async fn list_tools(channel: &CallerChannel<'_>) -> Result<Vec<McpToolDescriptor>, CallFailure> {
    let mut tools = native_tool_descriptors();
    // The plugin list also verifies the token. A plugin failure other than
    // that leaves the native tools, as it always has.
    if let Ok(response) = channel.call(CallerRequest::PluginMcpListTools).await? {
        for tool in plugin_tool_descriptors(response) {
            if !tools.iter().any(|known| known.name == tool.name) {
                tools.push(tool);
            }
        }
    }
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tools)
}

async fn call_tool(
    channel: &CallerChannel<'_>,
    call: McpCallRequest,
) -> Result<Result<McpToolResult, McpToolError>, CallFailure> {
    if is_native_tool(&call.name) {
        let native = match native_call(&call) {
            Ok(native) => native,
            Err(error) => {
                verify(channel).await?;
                return Ok(Err(error));
            }
        };
        let response = channel.call(native.request).await?;
        return Ok(daemon_tool_result(response, native.expected));
    }
    let response = channel
        .call(CallerRequest::PluginMcpCallTool {
            name: call.name,
            arguments: call.arguments,
        })
        .await?;
    Ok(plugin_tool_result(response))
}
