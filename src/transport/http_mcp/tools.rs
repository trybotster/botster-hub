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

use crate::HubClientError;
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
use crate::session_credential::CallerToken;
use crate::transport::http_mcp::audit::ToolCallRecord;
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

/// Serve one HTTP body for the session that `token` claims to be. A tool
/// call also returns its audit record.
pub(crate) async fn dispatch(
    control_tx: &ControlSender,
    token: &CallerToken,
    body: &[u8],
) -> (Dispatched, Option<ToolCallRecord>) {
    let channel = CallerChannel { control_tx, token };
    match serve(&channel, body).await {
        Ok(served) => served,
        Err(failure) => (Dispatched::Refused(failure.refusal()), None),
    }
}

async fn serve(
    channel: &CallerChannel<'_>,
    body: &[u8],
) -> Result<(Dispatched, Option<ToolCallRecord>), CallFailure> {
    let (id, method, params) = match parse_inbound(body) {
        Inbound::Request { id, method, params } => (id, method, params),
        Inbound::NoReply => {
            verify(channel).await?;
            return Ok((Dispatched::Accepted, None));
        }
        Inbound::Invalid(response) => {
            verify(channel).await?;
            return Ok((Dispatched::Body(encode_response(&response)), None));
        }
    };
    let mut audit = None;
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
                    return Ok((
                        Dispatched::Body(encode_response(&JsonRpcResponse::error(
                            id,
                            JsonRpcError::invalid_params(error),
                        ))),
                        None,
                    ));
                }
            };
            let mut record = ToolCallRecord {
                tool: call.name.clone(),
                target_session_id: call
                    .arguments
                    .get("session_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                outcome: "ok".to_string(),
                caller_verified: true,
            };
            let outcome = match call_tool(channel, call).await {
                Ok(outcome) => outcome,
                Err(CallFailure::Unauthenticated) => {
                    record.outcome = CALLER_UNAUTHENTICATED.to_string();
                    record.caller_verified = false;
                    return Ok((Dispatched::Refused(Refusal::Unauthenticated), Some(record)));
                }
                Err(failure) => return Err(failure),
            };
            if let Err(error) = &outcome {
                record.outcome = error.code.clone();
            }
            audit = Some(record);
            tool_call_response(outcome)
        }
        _ => {
            verify(channel).await?;
            return Ok((
                Dispatched::Body(encode_response(&JsonRpcResponse::error(
                    id,
                    JsonRpcError::method_not_found(&method),
                ))),
                None,
            ));
        }
    };
    Ok((
        Dispatched::Body(encode_response(&JsonRpcResponse::result(id, result))),
        audit,
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use botster_hub_client::{DaemonIdentity, DaemonResponseKind};
    use tokio::sync::mpsc;

    use crate::client_api_dto::plugin::daemon_coordination_identity;
    use crate::client_api_dto::response::daemon_coordination;
    use crate::daemon::control::operator_refusal;

    fn token() -> CallerToken {
        CallerToken::parse(&format!("sess-1.{}", "5a".repeat(32))).expect("a well-formed token")
    }

    fn body(method: &str, params: Value) -> Vec<u8> {
        json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params })
            .to_string()
            .into_bytes()
    }

    fn identity_response(session_id: &str) -> DaemonResponse {
        daemon_coordination(
            DaemonResponseKind::Identity,
            daemon_coordination_identity(DaemonIdentity {
                client_id: "test".to_string(),
                role: "session".to_string(),
                identity_source: "caller_token".to_string(),
                caller_session_id: Some(session_id.to_string()),
                host_id: "hub-1".to_string(),
                host_display_name: "Hub".to_string(),
            }),
        )
    }

    fn unauthenticated_response() -> DaemonResponse {
        operator_refusal(CALLER_UNAUTHENTICATED, "caller_auth", "refused")
    }

    /// A stand-in for the owner: it answers each message in turn with the
    /// given responses and reports what it received (request name, proven).
    fn fake_owner(
        mut control_rx: mpsc::Receiver<ControlMessage>,
        answers: Vec<DaemonResponse>,
    ) -> tokio::task::JoinHandle<Vec<(String, Option<String>)>> {
        tokio::spawn(async move {
            let mut seen = Vec::new();
            for answer in answers {
                let Some(ControlMessage::CallerRequest {
                    proven,
                    request,
                    reply_tx,
                    ..
                }) = control_rx.recv().await
                else {
                    break;
                };
                seen.push((
                    format!("{:?}", request.into_daemon_request())
                        .split([' ', '{', '('])
                        .next()
                        .unwrap_or("")
                        .to_string(),
                    proven.map(|session| session.0),
                ));
                let _ = reply_tx.send(Ok(answer));
            }
            seen
        })
    }

    /// Hang guard for every await on the fake owner: a wait that would never
    /// end fails the test with a message, so a reverted guard is red, not a hang.
    const HANG_GUARD: std::time::Duration = std::time::Duration::from_secs(10);

    async fn served(
        control_tx: &ControlSender,
        body: &[u8],
    ) -> (Dispatched, Option<ToolCallRecord>) {
        // timer: deadline — the dispatcher awaits a reply the fake owner may never send.
        tokio::time::timeout(HANG_GUARD, dispatch(control_tx, &token(), body))
            .await
            .expect("dispatch did not finish: a reply never arrived")
    }

    /// What the fake owner saw. The owner ends when the last sender drops.
    async fn finished(
        owner: tokio::task::JoinHandle<Vec<(String, Option<String>)>>,
    ) -> Vec<(String, Option<String>)> {
        // timer: deadline — the fake owner ends when its channel closes; expiry is a test defect.
        tokio::time::timeout(HANG_GUARD, owner)
            .await
            .expect("the fake owner did not end")
            .expect("the fake owner panicked")
    }

    #[tokio::test]
    async fn initialize_proves_the_token_before_answering() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![unauthenticated_response()]);
        let (dispatched, _) = served(&control_tx, &body("initialize", json!({}))).await;
        assert!(matches!(
            dispatched,
            Dispatched::Refused(Refusal::Unauthenticated)
        ));
        // The only message was the proof: initialize alone never reaches Core.
        drop(control_tx);
        assert_eq!(finished(owner).await, [("Whoami".to_string(), None)]);
    }

    #[tokio::test]
    async fn a_proven_token_gets_the_initialize_answer() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![identity_response("sess-1")]);
        let (dispatched, _) = served(&control_tx, &body("initialize", json!({}))).await;
        let Dispatched::Body(bytes) = dispatched else {
            panic!("initialize is answered");
        };
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply["result"]["serverInfo"]["name"], "botster-hub");
        drop(control_tx);
        // The proof came first: the owner saw exactly one message, a Whoami.
        assert_eq!(finished(owner).await, [("Whoami".to_string(), None)]);
    }

    #[tokio::test]
    async fn a_messaging_tool_sends_one_message_that_proves_itself_in_core() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![unauthenticated_response()]);
        let call = body(
            "tools/call",
            json!({ "name": "post_message", "arguments": { "session_id": "b", "body": "x" } }),
        );
        let (dispatched, _) = served(&control_tx, &call).await;
        // The owner refused the token inside the operation: 401, not a tool error.
        assert!(matches!(
            dispatched,
            Dispatched::Refused(Refusal::Unauthenticated)
        ));
        drop(control_tx);
        assert_eq!(finished(owner).await, [("PostMessage".to_string(), None)]);
    }

    #[tokio::test]
    async fn a_plugin_tool_is_preceded_by_a_proof_and_carries_the_proven_session() {
        let (control_tx, control_rx) = mpsc::channel(8);
        // The proof succeeds; the plugin call is then refused, which ends the test.
        let owner = fake_owner(
            control_rx,
            vec![identity_response("sess-1"), unauthenticated_response()],
        );
        let call = body(
            "tools/call",
            json!({ "name": "plugin.tool", "arguments": {} }),
        );
        let (_, record) = served(&control_tx, &call).await;
        let record = record.expect("a refused tool call is audited");
        assert!(!record.caller_verified);
        assert_eq!(record.outcome, CALLER_UNAUTHENTICATED);
        assert_eq!(record.tool, "plugin.tool");
        drop(control_tx);
        assert_eq!(
            finished(owner).await,
            [
                ("Whoami".to_string(), None),
                ("PluginMcpCallTool".to_string(), Some("sess-1".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn a_full_control_queue_is_busy_and_never_waited_on() {
        let (control_tx, _control_rx) = mpsc::channel(1);
        // Fill the queue with an unrelated message.
        let (filler_reply, _) = crate::daemon::control::message::control_reply_channel();
        control_tx
            .try_send(ControlMessage::CallerRequest {
                token: token(),
                proven: None,
                request: Box::new(CallerRequest::Whoami),
                reply_tx: filler_reply,
                enqueued_at: Instant::now(),
            })
            .expect("the first message fits");
        let (dispatched, _) = served(&control_tx, &body("ping", json!({}))).await;
        assert!(matches!(dispatched, Dispatched::Refused(Refusal::Busy)));
    }

    #[tokio::test]
    async fn a_batch_or_garbage_body_is_answered_only_after_the_token_is_proven() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(
            control_rx,
            vec![identity_response("sess-1"), unauthenticated_response()],
        );
        let (proven, _) = served(&control_tx, b"not json").await;
        assert!(matches!(proven, Dispatched::Body(_)));
        let (refused, _) = served(&control_tx, b"[]").await;
        assert!(matches!(
            refused,
            Dispatched::Refused(Refusal::Unauthenticated)
        ));
        drop(control_tx);
        assert_eq!(finished(owner).await.len(), 2, "each body was proven once");
    }
}
