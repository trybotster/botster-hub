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
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::DaemonControlState;
use crate::{HubClientRequest, HubClientResponseBody};

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
    let Some(runtime) = daemon.runtime_mut() else {
        return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
    };

    match request {
        DaemonRequest::Whoami { caller_session_id } => ControlStep::ready(daemon_coordination(
            DaemonResponseKind::Identity,
            daemon_coordination_identity(DaemonIdentity {
                client_id: "botster-hub-daemon-socket".to_string(),
                role: "local_operator".to_string(),
                identity_source: if caller_session_id.is_some() {
                    "BOTSTER_SESSION_UUID".to_string()
                } else {
                    "local_operator".to_string()
                },
                caller_session_id,
                host_id: status.host_id.clone(),
                host_display_name: status.host_display_name.clone(),
            }),
        )),
        DaemonRequest::PostMessage {
            caller_session_id,
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
                EndpointId(
                    caller_session_id
                        .map(|session_id| format!("session:{session_id}"))
                        .unwrap_or_else(|| "botster-hub-mcp".to_string()),
                ),
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
            let step = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::PublishRoutedEnvelope {
                    request_id: request_id("daemon-mcp-post-message"),
                    envelope,
                },
            );
            defer_client_step(step, |body| {
                let HubClientResponseBody::RoutedEnvelopePublish(publish) = body else {
                    return Err(DaemonTransportError::UnexpectedResponse);
                };
                Ok(daemon_coordination(
                    DaemonResponseKind::MessagePosted,
                    daemon_coordination_publish(publish.deliveries),
                ))
            })
        }
        DaemonRequest::ReceiveMessages {
            caller_session_id,
            after,
            limit,
        } => {
            let step = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::DrainRoutedEnvelopes {
                    request_id: request_id("daemon-mcp-receive-messages"),
                    target: EnvelopeTarget::Session {
                        session_id: SessionId(caller_session_id),
                    },
                    after: after.map(EnvelopeCursor),
                    limit: limit.clamp(1, 128),
                },
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
        DaemonRequest::AckMessage {
            caller_session_id,
            envelope_id,
        } => {
            let step = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::AcknowledgeRoutedEnvelope {
                    request_id: request_id("daemon-mcp-ack-message"),
                    target: EnvelopeTarget::Session {
                        session_id: SessionId(caller_session_id),
                    },
                    envelope_id: EnvelopeId(envelope_id),
                },
            );
            defer_client_step(step, |body| {
                let HubClientResponseBody::RoutedEnvelopeAck(ack) = body else {
                    return Err(DaemonTransportError::UnexpectedResponse);
                };
                Ok(daemon_coordination(
                    DaemonResponseKind::MessageAcked,
                    daemon_coordination_ack(ack.state),
                ))
            })
        }
        DaemonRequest::NotifySession { session_id, data } => {
            let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
            let step = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::NotifySession {
                    request_id: request_id("daemon-mcp-notify-session"),
                    session_id: SessionId(session_id),
                    data: data.into_bytes(),
                    readiness: ReadinessEvidence::default(),
                    now_seconds: now,
                },
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
