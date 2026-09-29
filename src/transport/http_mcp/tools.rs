//! MCP methods over the control channel.
//!
//! Every request reaches the owner as a `ControlMessage::CallerRequest` that
//! carries the bearer token. The owner verifies it and runs the request as
//! the session it proves. This module never decides who a caller is: it only
//! turns a refusal from the owner into a 401.

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
use crate::transport::http_mcp::audit::{TargetHub, ToolCallRecord};
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
    fn refusal(&self) -> Refusal {
        match self {
            Self::Unauthenticated => Refusal::Unauthenticated,
            Self::Busy | Self::Stopped => Refusal::Busy,
        }
    }

    /// The outcome an audit line records for a call that got no result.
    fn outcome(&self) -> &'static str {
        match self {
            Self::Unauthenticated => CALLER_UNAUTHENTICATED,
            Self::Busy => "busy",
            Self::Stopped => "unavailable",
        }
    }
}

struct CallerChannel<'a> {
    control_tx: &'a ControlSender,
    token: &'a CallerToken,
}

impl CallerChannel<'_> {
    /// The session the token proves, from a `Whoami` the owner answers. Every
    /// request proves its token first. The proof must be a successful reply
    /// that names the token's own session: a transport error or an
    /// error-bearing reply is `Stopped` (503) and a reply without that
    /// identity is `Unauthenticated`, so a failed proof never passes.
    async fn prove(&self) -> Result<SessionId, CallFailure> {
        let response = self
            .send(CallerRequest::Whoami, None)
            .await?
            .map_err(|_| CallFailure::Stopped)?;
        if response.error.is_some() {
            return Err(CallFailure::Stopped);
        }
        let proven = response
            .coordination
            .and_then(|coordination| coordination.identity)
            .and_then(|identity| identity.caller_session_id)
            .ok_or(CallFailure::Unauthenticated)?;
        if proven == self.token.session_id() {
            Ok(SessionId(proven))
        } else {
            Err(CallFailure::Unauthenticated)
        }
    }

    /// One request to the owner after `prove`. Requests that reach Core prove
    /// the token again in their own Core submission, so the proof and the
    /// effect see one Core state. The others (status, the session list,
    /// plugin tools) run as the session `prove` returned.
    async fn call_as(
        &self,
        request: CallerRequest,
        proven: &SessionId,
    ) -> Result<DaemonTransportResult<DaemonResponse>, CallFailure> {
        let proven = (!request.proves_in_core()).then(|| proven.clone());
        self.send(request, proven).await
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
/// call also returns its audit record, whether or not it got a result.
pub(crate) async fn dispatch(
    control_tx: &ControlSender,
    hub_id: &str,
    token: &CallerToken,
    body: &[u8],
) -> (Dispatched, Option<ToolCallRecord>) {
    let channel = CallerChannel { control_tx, token };
    // Every request proves its token before anything else happens.
    let proven = channel.prove().await;
    let inbound = parse_inbound(body);
    let (id, method, params) = match inbound {
        Inbound::Request { id, method, params } => (id, method, params),
        Inbound::NoReply => {
            return match proven {
                Ok(_) => (Dispatched::Accepted, None),
                Err(failure) => (Dispatched::Refused(failure.refusal()), None),
            };
        }
        Inbound::Invalid(response) => {
            return match proven {
                Ok(_) => (Dispatched::Body(encode_response(&response)), None),
                Err(failure) => (Dispatched::Refused(failure.refusal()), None),
            };
        }
    };
    if method == "tools/call" {
        return serve_tool_call(&channel, hub_id, proven, id, params).await;
    }
    let proven = match proven {
        Ok(proven) => proven,
        Err(failure) => return (Dispatched::Refused(failure.refusal()), None),
    };
    let result = match method.as_str() {
        "initialize" => {
            let requested = params
                .as_ref()
                .and_then(|params| params.get("protocolVersion"))
                .and_then(Value::as_str);
            initialize_result(requested)
        }
        "ping" => json!({}),
        "tools/list" => match list_tools(&channel, &proven).await {
            Ok(tools) => json!({ "tools": tools }),
            Err(failure) => return (Dispatched::Refused(failure.refusal()), None),
        },
        _ => {
            return (
                Dispatched::Body(encode_response(&JsonRpcResponse::error(
                    id,
                    JsonRpcError::method_not_found(&method),
                ))),
                None,
            );
        }
    };
    (
        Dispatched::Body(encode_response(&JsonRpcResponse::result(id, result))),
        None,
    )
}

/// One `tools/call`. The audit record exists for every call that parsed as a
/// tool call, including one refused for a failed proof, a full queue or a
/// closed queue; `caller_verified` says whether the proof succeeded.
async fn serve_tool_call(
    channel: &CallerChannel<'_>,
    hub_id: &str,
    proven: Result<SessionId, CallFailure>,
    id: Value,
    params: Option<Value>,
) -> (Dispatched, Option<ToolCallRecord>) {
    let mut record = ToolCallRecord {
        tool: params
            .as_ref()
            .and_then(|params| params.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        target_session_id: params
            .as_ref()
            .and_then(|params| params.get("arguments"))
            .and_then(|arguments| arguments.get("session_id"))
            .and_then(Value::as_str)
            .map(str::to_string),
        target_hub: match params
            .as_ref()
            .and_then(|params| params.get("arguments"))
            .and_then(|arguments| arguments.get("hub_id"))
        {
            None => TargetHub::Unnamed,
            Some(Value::String(hub_id)) => TargetHub::Named(hub_id.clone()),
            Some(_) => TargetHub::Malformed,
        },
        outcome: "ok".to_string(),
        caller_verified: proven.is_ok(),
    };
    let proven = match proven {
        Ok(proven) => proven,
        Err(failure) => {
            record.outcome = failure.outcome().to_string();
            return (Dispatched::Refused(failure.refusal()), Some(record));
        }
    };
    let call = match McpCallRequest::from_params(params) {
        Ok(call) => call,
        Err(error) => {
            record.outcome = "invalid_params".to_string();
            return (
                Dispatched::Body(encode_response(&JsonRpcResponse::error(
                    id,
                    JsonRpcError::invalid_params(error),
                ))),
                Some(record),
            );
        }
    };
    match call_tool(channel, hub_id, &proven, call).await {
        Ok(outcome) => {
            if let Err(error) = &outcome {
                record.outcome = error.code.clone();
            }
            (
                Dispatched::Body(encode_response(&JsonRpcResponse::result(
                    id,
                    tool_call_response(outcome),
                ))),
                Some(record),
            )
        }
        Err(failure) => {
            // A Core operation that refused the token proved nothing.
            if matches!(failure, CallFailure::Unauthenticated) {
                record.caller_verified = false;
            }
            record.outcome = failure.outcome().to_string();
            (Dispatched::Refused(failure.refusal()), Some(record))
        }
    }
}

async fn list_tools(
    channel: &CallerChannel<'_>,
    proven: &SessionId,
) -> Result<Vec<McpToolDescriptor>, CallFailure> {
    let mut tools = native_tool_descriptors();
    // A plugin failure leaves the native tools, as it always has.
    if let Ok(response) = channel
        .call_as(CallerRequest::PluginMcpListTools, proven)
        .await?
    {
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
    hub_id: &str,
    proven: &SessionId,
    call: McpCallRequest,
) -> Result<Result<McpToolResult, McpToolError>, CallFailure> {
    if is_native_tool(&call.name) {
        let native = match native_call(&call, hub_id) {
            Ok(native) => native,
            Err(error) => return Ok(Err(error)),
        };
        let response = channel.call_as(native.request, proven).await?;
        return Ok(daemon_tool_result(response, native.expected, hub_id));
    }
    let response = channel
        .call_as(
            CallerRequest::PluginMcpCallTool {
                name: call.name,
                arguments: call.arguments,
            },
            proven,
        )
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

    const HUB: &str = "hub-1";

    fn token() -> CallerToken {
        CallerToken::parse(&format!("sess-1.{}", "5a".repeat(32))).expect("a well-formed token")
    }

    fn body(method: &str, params: Value) -> Vec<u8> {
        json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params })
            .to_string()
            .into_bytes()
    }

    fn tool_call(name: &str, arguments: Value) -> Vec<u8> {
        body(
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
        )
    }

    type Answer = DaemonTransportResult<DaemonResponse>;

    fn identity(session_id: &str) -> Answer {
        Ok(daemon_coordination(
            DaemonResponseKind::Identity,
            daemon_coordination_identity(DaemonIdentity {
                client_id: "test".to_string(),
                role: "session".to_string(),
                identity_source: "caller_token".to_string(),
                caller_session_id: Some(session_id.to_string()),
                host_id: HUB.to_string(),
                host_display_name: "Hub".to_string(),
            }),
        ))
    }

    fn unauthenticated() -> Answer {
        Ok(operator_refusal(
            CALLER_UNAUTHENTICATED,
            "caller_auth",
            "refused",
        ))
    }

    /// An error-bearing reply that is not a token refusal.
    fn daemon_error() -> Answer {
        Ok(operator_refusal(
            "daemon_shutting_down",
            "caller_auth",
            "closing",
        ))
    }

    /// A successful reply that carries no identity.
    fn no_identity() -> Answer {
        Ok(daemon_coordination(
            DaemonResponseKind::Identity,
            daemon_coordination_identity(DaemonIdentity {
                client_id: "test".to_string(),
                role: "session".to_string(),
                identity_source: "caller_token".to_string(),
                caller_session_id: None,
                host_id: HUB.to_string(),
                host_display_name: "Hub".to_string(),
            }),
        ))
    }

    fn transport_error() -> Answer {
        Err(DaemonTransportError::UnexpectedResponse)
    }

    /// Hang guard for every await on the fake owner: a wait that would never
    /// end fails the test with a message, so a reverted guard is red, not a hang.
    const HANG_GUARD: std::time::Duration = std::time::Duration::from_secs(10);

    async fn served(
        control_tx: &ControlSender,
        body: &[u8],
    ) -> (Dispatched, Option<ToolCallRecord>) {
        // timer: deadline — the dispatcher awaits a reply the fake owner may never send.
        tokio::time::timeout(HANG_GUARD, dispatch(control_tx, HUB, &token(), body))
            .await
            .expect("dispatch did not finish: a reply never arrived")
    }

    /// A stand-in for the owner: it answers each message in turn with the
    /// given answers and reports what it received (request name, proven).
    fn fake_owner(
        mut control_rx: mpsc::Receiver<ControlMessage>,
        answers: Vec<Answer>,
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
                let _ = reply_tx.send(answer);
            }
            seen
        })
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

    fn whoami() -> (String, Option<String>) {
        ("Whoami".to_string(), None)
    }

    #[tokio::test]
    async fn initialize_proves_the_token_before_answering() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![unauthenticated()]);
        let (dispatched, _) = served(&control_tx, &body("initialize", json!({}))).await;
        assert!(matches!(
            dispatched,
            Dispatched::Refused(Refusal::Unauthenticated)
        ));
        // The only message was the proof: initialize alone never reaches Core.
        drop(control_tx);
        assert_eq!(finished(owner).await, [whoami()]);
    }

    #[tokio::test]
    async fn a_proven_token_gets_the_initialize_answer() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![identity("sess-1")]);
        let (dispatched, _) = served(&control_tx, &body("initialize", json!({}))).await;
        let Dispatched::Body(bytes) = dispatched else {
            panic!("initialize is answered");
        };
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply["result"]["serverInfo"]["name"], "botster-hub");
        drop(control_tx);
        assert_eq!(finished(owner).await, [whoami()]);
    }

    /// A proof that fails for any reason refuses the request, for every
    /// method: initialize, ping, and a tool call with invalid arguments.
    #[tokio::test]
    async fn a_failed_proof_refuses_every_method_whatever_the_failure() {
        // (the owner's answer, the refusal it must produce)
        let cases: Vec<(&str, Answer, Refusal)> = vec![
            (
                "unauthenticated reply",
                unauthenticated(),
                Refusal::Unauthenticated,
            ),
            ("transport error", transport_error(), Refusal::Busy),
            ("error-bearing reply", daemon_error(), Refusal::Busy),
            (
                "reply without an identity",
                no_identity(),
                Refusal::Unauthenticated,
            ),
            (
                "identity of another session",
                identity("sess-2"),
                Refusal::Unauthenticated,
            ),
        ];
        let requests: Vec<(&str, Vec<u8>)> = vec![
            ("initialize", body("initialize", json!({}))),
            ("ping", body("ping", json!({}))),
            ("tools/list", body("tools/list", json!({}))),
            // Invalid arguments: nothing else would ever reach the owner.
            ("invalid arguments", tool_call("post_message", json!({}))),
        ];
        for (label, answer, expected) in cases {
            for (method, request) in &requests {
                let (control_tx, control_rx) = mpsc::channel(8);
                let answer = match &answer {
                    Ok(response) => Ok(response.clone()),
                    Err(_) => transport_error(),
                };
                let owner = fake_owner(control_rx, vec![answer]);
                let (dispatched, record) = served(&control_tx, request).await;
                let Dispatched::Refused(refusal) = dispatched else {
                    panic!("{label}: {method} was answered after a failed proof");
                };
                assert_eq!(refusal, expected, "{label}: {method}");
                if *method == "invalid arguments" {
                    let record = record.expect("a parsed tool call is audited");
                    assert!(
                        !record.caller_verified,
                        "{label}: an unproven caller is recorded"
                    );
                }
                drop(control_tx);
                assert_eq!(finished(owner).await, [whoami()], "{label}: {method}");
            }
        }
    }

    #[tokio::test]
    async fn a_messaging_tool_proves_first_and_again_in_its_own_core_submission() {
        let (control_tx, control_rx) = mpsc::channel(8);
        // The proof succeeds; Core then refuses the token inside the operation.
        let owner = fake_owner(control_rx, vec![identity("sess-1"), unauthenticated()]);
        let call = tool_call("post_message", json!({ "session_id": "b", "body": "x" }));
        let (dispatched, record) = served(&control_tx, &call).await;
        assert!(matches!(
            dispatched,
            Dispatched::Refused(Refusal::Unauthenticated)
        ));
        let record = record.expect("a refused tool call is audited");
        assert!(!record.caller_verified, "the Core proof refused the token");
        assert_eq!(record.outcome, CALLER_UNAUTHENTICATED);
        assert_eq!(record.target_session_id.as_deref(), Some("b"));
        drop(control_tx);
        // The messaging operation carries no `proven` session: it proves in Core.
        assert_eq!(
            finished(owner).await,
            [whoami(), ("PostMessage".to_string(), None)]
        );
    }

    #[tokio::test]
    async fn a_plugin_tool_runs_as_the_proven_session() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![identity("sess-1"), unauthenticated()]);
        let call = tool_call("plugin.tool", json!({}));
        let (_, record) = served(&control_tx, &call).await;
        let record = record.expect("a refused tool call is audited");
        assert_eq!(record.tool, "plugin.tool");
        drop(control_tx);
        assert_eq!(
            finished(owner).await,
            [
                whoami(),
                ("PluginMcpCallTool".to_string(), Some("sess-1".to_string())),
            ]
        );
    }

    #[tokio::test]
    async fn a_full_control_queue_is_busy_and_still_audited() {
        let (control_tx, _control_rx) = mpsc::channel(1);
        // Fill the queue with an unrelated message.
        let (filler_reply, _) = crate::daemon::control::message::control_reply_channel();
        control_tx
            .try_send(ControlMessage::CallerRequest {
                token: token(),
                proven: None,
                request: Box::new(CallerRequest::Whoami),
                reply_tx: filler_reply,
            })
            .expect("the first message fits");
        let (dispatched, _) = served(&control_tx, &body("ping", json!({}))).await;
        assert!(matches!(dispatched, Dispatched::Refused(Refusal::Busy)));
        // A parsed tool call is audited even though the queue refused it.
        let (dispatched, record) = served(&control_tx, &tool_call("whoami", json!({}))).await;
        assert!(matches!(dispatched, Dispatched::Refused(Refusal::Busy)));
        let record = record.expect("a busy tool call is audited");
        assert_eq!(record.tool, "whoami");
        assert_eq!(record.outcome, "busy");
        assert!(!record.caller_verified, "nothing was proven");
    }

    #[tokio::test]
    async fn a_closed_control_queue_is_unavailable_and_still_audited() {
        let (control_tx, control_rx) = mpsc::channel(8);
        drop(control_rx);
        let (dispatched, record) = served(&control_tx, &tool_call("whoami", json!({}))).await;
        assert!(matches!(dispatched, Dispatched::Refused(Refusal::Busy)));
        let record = record.expect("a call on a closed queue is audited");
        assert_eq!(record.outcome, "unavailable");
        assert!(!record.caller_verified);
    }

    #[tokio::test]
    async fn a_malformed_tool_call_is_audited_after_the_proof() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![identity("sess-1")]);
        // No `name`: it parses as JSON-RPC but not as a tool call.
        let (dispatched, record) = served(&control_tx, &body("tools/call", json!({}))).await;
        assert!(matches!(dispatched, Dispatched::Body(_)));
        let record = record.expect("a malformed tool call is audited");
        assert_eq!(record.outcome, "invalid_params");
        assert!(record.caller_verified);
        drop(control_tx);
        assert_eq!(finished(owner).await, [whoami()]);
    }

    #[tokio::test]
    async fn a_batch_or_garbage_body_is_answered_only_after_the_token_is_proven() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![identity("sess-1"), unauthenticated()]);
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

    #[tokio::test]
    async fn a_malformed_hub_argument_is_refused_and_audited_without_a_hub() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![identity("sess-1")]);
        let call = tool_call(
            "post_message",
            json!({ "hub_id": 7, "session_id": "b", "body": "x" }),
        );
        let (dispatched, record) = served(&control_tx, &call).await;
        assert!(matches!(dispatched, Dispatched::Body(_)));
        let record = record.expect("audited");
        assert_eq!(record.outcome, "invalid_arguments");
        assert_eq!(record.target_hub, TargetHub::Malformed);
        drop(control_tx);
        assert_eq!(finished(owner).await, [whoami()]);
    }

    #[tokio::test]
    async fn a_remote_hub_target_is_refused_before_any_message_but_the_proof() {
        let (control_tx, control_rx) = mpsc::channel(8);
        let owner = fake_owner(control_rx, vec![identity("sess-1")]);
        let call = tool_call(
            "post_message",
            json!({ "hub_id": "other-hub", "session_id": "b", "body": "x" }),
        );
        let (dispatched, record) = served(&control_tx, &call).await;
        let Dispatched::Body(bytes) = dispatched else {
            panic!("a remote hub is a tool error, not a transport refusal");
        };
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reply["result"]["isError"], true);
        assert_eq!(
            reply["result"]["structuredContent"]["error"]["code"],
            "remote_hub_unsupported"
        );
        let record = record.expect("audited");
        assert_eq!(record.outcome, "remote_hub_unsupported");
        assert_eq!(
            record.target_hub,
            TargetHub::Named("other-hub".to_string()),
            "the audit keeps the hub the call asked for"
        );
        assert_eq!(record.target_session_id.as_deref(), Some("b"));
        assert!(
            record.caller_verified,
            "a tool error follows a successful proof"
        );
        drop(control_tx);
        // Only the proof reached the owner: no post, no doorbell.
        assert_eq!(finished(owner).await, [whoami()]);
    }
}
