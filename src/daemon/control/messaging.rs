//! Coordination messaging request family.

use botster_core::{
    EndpointId, EnvelopeCursor, EnvelopeId, EnvelopeTarget, RoutedEnvelope, RoutedEnvelopePayload,
    SessionId,
};
use botster_core_daemon::ReadinessEvidence;
use botster_hub_client::{DaemonIdentity, DaemonRequest, DaemonResponse, DaemonResponseKind};

use crate::HubDaemon;
use crate::client_api::HubClientApi;
use crate::client_api_dto::plugin::{
    daemon_coordination_ack, daemon_coordination_identity, daemon_coordination_messages,
    daemon_coordination_notify, daemon_coordination_publish,
};
use crate::client_api_dto::response::daemon_coordination;
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::{HubClientRequest, HubClientResponseBody};

pub(crate) const MESSAGE_CONTENT_TYPE: &str = "application/vnd.botster.coordination.message+text";

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    observability: DaemonObservability<'_>,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let status = daemon.status();
    let api = HubClientApi::local_operator(
        observability
            .client_id
            .map(str::to_string)
            .unwrap_or_else(|| super::runtime_client_id(&request)),
    );
    let packages = daemon.package_registry().clone();
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };

    match request {
        DaemonRequest::Whoami { caller_session_id } => Ok(daemon_coordination(
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
            let now = crate::daemon::owner_loop::tick(logical_clock);
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
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::PublishRoutedEnvelope {
                    request_id: request_id("daemon-mcp-post-message"),
                    envelope,
                },
            )?;
            let HubClientResponseBody::RoutedEnvelopePublish(publish) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::MessagePosted,
                daemon_coordination_publish(publish.deliveries),
            ))
        }
        DaemonRequest::ReceiveMessages {
            caller_session_id,
            after,
            limit,
        } => {
            let response = api.handle_request(
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
            )?;
            let HubClientResponseBody::RoutedEnvelopeDrain(drain) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::Messages,
                daemon_coordination_messages(drain.envelopes, drain.next_cursor),
            ))
        }
        DaemonRequest::AckMessage {
            caller_session_id,
            envelope_id,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::AcknowledgeRoutedEnvelope {
                    request_id: request_id("daemon-mcp-ack-message"),
                    target: EnvelopeTarget::Session {
                        session_id: SessionId(caller_session_id),
                    },
                    envelope_id: EnvelopeId(envelope_id),
                },
            )?;
            let HubClientResponseBody::RoutedEnvelopeAck(ack) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::MessageAcked,
                daemon_coordination_ack(ack.state),
            ))
        }
        DaemonRequest::NotifySession { session_id, data } => {
            let now = crate::daemon::owner_loop::tick(logical_clock);
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::NotifySession {
                    request_id: request_id("daemon-mcp-notify-session"),
                    session_id: SessionId(session_id),
                    data: data.into_bytes(),
                    readiness: ReadinessEvidence::default(),
                    now_seconds: now,
                },
            )?;
            let HubClientResponseBody::GuardedWrite(write) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_coordination(
                DaemonResponseKind::SessionNotified,
                daemon_coordination_notify(write.decision, write.states),
            ))
        }
        _ => unreachable!("messaging runtime family received a non-messaging request"),
    }
}
