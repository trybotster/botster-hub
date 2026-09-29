//! Coordination messaging request family.
//!
//! Routed-envelope and guarded-write operations run on the Core owner thread
//! through the client API step; the owner polls the step and answers when the
//! result lands.

use botster_core::{
    EndpointId, EnvelopeCursor, EnvelopeId, EnvelopeTarget, RoutedEnvelope, RoutedEnvelopePayload,
    SessionId,
};
use botster_core_daemon::ReadinessEvidence;
use botster_hub_client::{DaemonIdentity, DaemonRequest, DaemonResponse, DaemonResponseKind};

use crate::HubDaemon;
use crate::client_api::{HubClientApi, HubClientStep};
use crate::client_api_dto::plugin::{
    daemon_coordination_ack, daemon_coordination_identity, daemon_coordination_messages,
    daemon_coordination_notify, daemon_coordination_publish,
};
use crate::client_api_dto::response::daemon_coordination;
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::control::{Caller, DaemonObservability, request_id};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::DaemonControlState;
use crate::{HubClientRequest, HubClientResponseBody};

/// The operator has no inbox: receiving and acknowledging are for a session
/// that proved its identity with its bearer token.
fn caller_required(operation: &'static str) -> DaemonResponse {
    crate::daemon::control::operator_refusal(
        "caller_required",
        operation,
        "this request needs a session caller; the operator socket has no inbox",
    )
}

pub(crate) const MESSAGE_CONTENT_TYPE: &str = "application/vnd.botster.coordination.message+text";

/// Defer one client API step until its Core result lands, then map the body.
pub(crate) fn defer_client_step(
    step: HubClientStep,
    map: impl Fn(HubClientResponseBody) -> DaemonTransportResult<DaemonResponse> + Send + 'static,
) -> ControlStep {
    match step {
        HubClientStep::Ready(result) => ControlStep::Ready(
            result
                .map_err(DaemonTransportError::Client)
                .and_then(|response| map(response.body)),
        ),
        HubClientStep::Pending(mut pending) => ControlStep::pending(move |daemon, _| {
            let Some(runtime) = daemon.runtime() else {
                return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
            };
            match pending.poll(runtime) {
                None => ControlPoll::Pending,
                Some(result) => ControlPoll::Ready(
                    result
                        .map_err(DaemonTransportError::Client)
                        .and_then(|response| map(response.body)),
                ),
            }
        }),
    }
}

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    request: DaemonRequest,
) -> ControlStep {
    let status = daemon.status();
    let api = HubClientApi::local_operator(
        observability
            .client_id
            .clone()
            .unwrap_or_else(|| super::runtime_client_id(&request)),
    );
    let packages = daemon.package_registry().clone();
    let caller = observability.caller.clone();
    let hub_id = status.host_id.clone();
    let caller_session = caller.session_id();
    let token = match &caller {
        Caller::Token(token) => Some(token.clone()),
        Caller::Operator | Caller::Proven(_) => None,
    };
    let Some(runtime) = daemon.runtime_mut() else {
        return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
    };

    match request {
        DaemonRequest::Whoami => {
            let identity = move |session_id: Option<SessionId>| {
                let (host_id, host_display_name) =
                    (hub_id.clone(), status.host_display_name.clone());
                daemon_coordination(
                    DaemonResponseKind::Identity,
                    daemon_coordination_identity(match session_id {
                        Some(session_id) => DaemonIdentity {
                            client_id: format!("http-mcp:{}", session_id.0),
                            role: "session".to_string(),
                            identity_source: "caller_token".to_string(),
                            caller_session_id: Some(session_id.0),
                            host_id,
                            host_display_name,
                        },
                        None => DaemonIdentity {
                            client_id: "botster-hub-daemon-socket".to_string(),
                            role: "local_operator".to_string(),
                            identity_source: "local_operator".to_string(),
                            caller_session_id: None,
                            host_id,
                            host_display_name,
                        },
                    }),
                )
            };
            match token {
                // A session proves its token in Core; the identity is the
                // session that proof names.
                Some(token) => {
                    let step = api.handle_request_for_owner(
                        runtime,
                        &packages,
                        HubClientRequest::VerifyCaller {
                            request_id: request_id("daemon-mcp-whoami"),
                            token,
                        },
                        state.current_waiter_id.expect("owner waiter is assigned"),
                    );
                    defer_client_step(step, move |body| {
                        let HubClientResponseBody::CallerVerified(session_id) = body else {
                            return Err(DaemonTransportError::UnexpectedResponse);
                        };
                        Ok(identity(Some(session_id)))
                    })
                }
                None => ControlStep::ready(identity(caller_session)),
            }
        }
        DaemonRequest::PostMessage {
            target_session_id,
            envelope_id,
            body,
        } => {
            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
            let envelope = RoutedEnvelope::new(
                EnvelopeId(
                    envelope_id
                        .unwrap_or_else(|| format!("hub-message-{}-{now}", target_session_id)),
                ),
                EndpointId(match &caller_session {
                    Some(session_id) => {
                        crate::routed_endpoint::session_endpoint(&hub_id, &session_id.0)
                    }
                    None => crate::routed_endpoint::operator_endpoint(&hub_id),
                }),
                vec![EnvelopeTarget::Session {
                    session_id: SessionId(target_session_id),
                }],
                RoutedEnvelopePayload {
                    content_type: MESSAGE_CONTENT_TYPE.to_string(),
                    body: body.into_bytes(),
                    extension: None,
                },
                now,
            );
            let step = api.handle_request_for_owner(
                runtime,
                &packages,
                HubClientRequest::PublishRoutedEnvelope {
                    request_id: request_id("daemon-mcp-post-message"),
                    envelope,
                    caller: token,
                },
                state.current_waiter_id.expect("owner waiter is assigned"),
            );
            defer_client_step(step, move |body| {
                let HubClientResponseBody::RoutedEnvelopePublish(publish) = body else {
                    return Err(DaemonTransportError::UnexpectedResponse);
                };
                Ok(daemon_coordination(
                    DaemonResponseKind::MessagePosted,
                    daemon_coordination_publish(publish.deliveries, &hub_id),
                ))
            })
        }
        DaemonRequest::ReceiveMessages { after, limit } => {
            let Some(caller_session_id) = caller_session else {
                return ControlStep::ready(caller_required("receive_messages"));
            };
            let step = api.handle_request_for_owner(
                runtime,
                &packages,
                HubClientRequest::DrainRoutedEnvelopes {
                    request_id: request_id("daemon-mcp-receive-messages"),
                    target: EnvelopeTarget::Session {
                        session_id: caller_session_id,
                    },
                    after: after.map(EnvelopeCursor),
                    limit: limit.clamp(1, 128),
                    caller: token,
                },
                state.current_waiter_id.expect("owner waiter is assigned"),
            );
            defer_client_step(step, |body| {
                let HubClientResponseBody::RoutedEnvelopeDrain(drain) = body else {
                    return Err(DaemonTransportError::UnexpectedResponse);
                };
                Ok(daemon_coordination(
                    DaemonResponseKind::Messages,
                    daemon_coordination_messages(drain.envelopes, drain.next_cursor),
                ))
            })
        }
        DaemonRequest::AckMessage { envelope_id } => {
            let Some(caller_session_id) = caller_session else {
                return ControlStep::ready(caller_required("ack_message"));
            };
            let step = api.handle_request_for_owner(
                runtime,
                &packages,
                HubClientRequest::AcknowledgeRoutedEnvelope {
                    request_id: request_id("daemon-mcp-ack-message"),
                    target: EnvelopeTarget::Session {
                        session_id: caller_session_id,
                    },
                    envelope_id: EnvelopeId(envelope_id),
                    caller: token,
                },
                state.current_waiter_id.expect("owner waiter is assigned"),
            );
            defer_client_step(step, move |body| {
                let HubClientResponseBody::RoutedEnvelopeAck(ack) = body else {
                    return Err(DaemonTransportError::UnexpectedResponse);
                };
                Ok(daemon_coordination(
                    DaemonResponseKind::MessageAcked,
                    daemon_coordination_ack(ack.state, &hub_id),
                ))
            })
        }
        DaemonRequest::NotifySession { session_id, data } => {
            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
            let step = api.handle_request_for_owner(
                runtime,
                &packages,
                HubClientRequest::NotifySession {
                    request_id: request_id("daemon-mcp-notify-session"),
                    session_id: SessionId(session_id),
                    data: data.into_bytes(),
                    readiness: ReadinessEvidence::default(),
                    now_seconds: now,
                    caller: token,
                },
                state.current_waiter_id.expect("owner waiter is assigned"),
            );
            defer_client_step(step, |body| {
                let HubClientResponseBody::GuardedWrite(write) = body else {
                    return Err(DaemonTransportError::UnexpectedResponse);
                };
                Ok(daemon_coordination(
                    DaemonResponseKind::SessionNotified,
                    daemon_coordination_notify(write.decision, write.states),
                ))
            })
        }
        _ => unreachable!("messaging runtime family received a non-messaging request"),
    }
}
