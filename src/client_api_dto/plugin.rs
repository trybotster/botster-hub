use botster_core::{EnvelopeCursor, EnvelopeDeliveryState, EnvelopeTarget, RoutedEnvelope};
use botster_core_daemon::{GuardedWriteDecision, GuardedWriteDeliveryState};
use botster_hub_client::{
    DaemonCoordination, DaemonEndpointRef, DaemonEnvelope, DaemonEnvelopeAck,
    DaemonEnvelopeDelivery, DaemonEnvelopePublish, DaemonIdentity, DaemonNotify,
    DaemonPluginLifecycle, DaemonPluginLoadFailure, DaemonPluginWorkerCounters,
};

use crate::client_api_dto::session::guarded_write_delivery_state_label;
use crate::daemon_projection::package_state_label;
use crate::{HubClientPluginLifecycle, HubClientPluginWorkerCounters};

pub(crate) fn daemon_coordination_identity(identity: DaemonIdentity) -> DaemonCoordination {
    DaemonCoordination {
        identity: Some(identity),
        publish: None,
        messages: Vec::new(),
        next_cursor: None,
        ack: None,
        notify: None,
    }
}

pub(crate) fn daemon_coordination_publish(
    deliveries: Vec<EnvelopeDeliveryState>,
    hub_id: &str,
) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: Some(DaemonEnvelopePublish {
            deliveries: deliveries
                .into_iter()
                .map(|state| daemon_envelope_delivery_from_state(state, hub_id))
                .collect(),
        }),
        messages: Vec::new(),
        next_cursor: None,
        ack: None,
        notify: None,
    }
}

pub(crate) fn daemon_coordination_messages(
    envelopes: Vec<RoutedEnvelope>,
    next_cursor: Option<EnvelopeCursor>,
) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: None,
        messages: envelopes
            .into_iter()
            .map(daemon_envelope_from_routed)
            .collect(),
        next_cursor: next_cursor.map(|cursor| cursor.0),
        ack: None,
        notify: None,
    }
}

pub(crate) fn daemon_coordination_ack(
    state: Option<EnvelopeDeliveryState>,
    hub_id: &str,
) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: None,
        messages: Vec::new(),
        next_cursor: None,
        ack: Some(daemon_envelope_ack_from_state(state, hub_id)),
        notify: None,
    }
}

pub(crate) fn daemon_coordination_notify(
    decision: GuardedWriteDecision,
    states: Vec<GuardedWriteDeliveryState>,
) -> DaemonCoordination {
    DaemonCoordination {
        identity: None,
        publish: None,
        messages: Vec::new(),
        next_cursor: None,
        ack: None,
        notify: Some(DaemonNotify {
            decision: format!("{decision:?}"),
            state_count: states.len(),
            states: states
                .into_iter()
                .map(guarded_write_delivery_state_label)
                .map(ToString::to_string)
                .collect(),
        }),
    }
}

pub(crate) fn daemon_envelope_delivery_from_state(
    state: EnvelopeDeliveryState,
    hub_id: &str,
) -> DaemonEnvelopeDelivery {
    DaemonEnvelopeDelivery {
        envelope_id: state.envelope_id.0,
        target: envelope_target_label(&state.target, hub_id),
        cursor: state.cursor.0,
        status: format!("{:?}", state.status).to_ascii_lowercase(),
    }
}

/// The structured form of an endpoint. The one place text becomes fields.
pub(crate) fn daemon_endpoint_ref(endpoint: &str) -> DaemonEndpointRef {
    use crate::routed_endpoint::{EndpointRef, parse_endpoint};
    let mut reference = DaemonEndpointRef {
        kind: String::new(),
        hub_id: None,
        session_id: None,
        plugin_key: None,
        raw: None,
    };
    match parse_endpoint(endpoint) {
        EndpointRef::Session { hub_id, session_id } => {
            reference.kind = "session".to_string();
            reference.hub_id = Some(hub_id);
            reference.session_id = Some(session_id);
        }
        EndpointRef::Operator { hub_id } => {
            reference.kind = "operator".to_string();
            reference.hub_id = Some(hub_id);
        }
        EndpointRef::Plugin { plugin_key } => {
            reference.kind = "plugin".to_string();
            reference.plugin_key = Some(plugin_key);
        }
        EndpointRef::Other(raw) => {
            reference.kind = "other".to_string();
            reference.raw = Some(raw);
        }
    }
    reference
}

pub(crate) fn daemon_envelope_from_routed(envelope: RoutedEnvelope) -> DaemonEnvelope {
    DaemonEnvelope {
        envelope_id: envelope.id.0,
        source: daemon_endpoint_ref(&envelope.source.0),
        content_type: envelope.payload.content_type,
        body: String::from_utf8_lossy(&envelope.payload.body).to_string(),
        created_at: envelope.created_at,
        cursor: envelope.cursor.map(|cursor| cursor.0),
    }
}

pub(crate) fn daemon_envelope_ack_from_state(
    state: Option<EnvelopeDeliveryState>,
    hub_id: &str,
) -> DaemonEnvelopeAck {
    match state {
        Some(state) => DaemonEnvelopeAck {
            envelope_id: Some(state.envelope_id.0),
            target: Some(envelope_target_label(&state.target, hub_id)),
            cursor: Some(state.cursor.0),
            status: format!("{:?}", state.status).to_ascii_lowercase(),
        },
        None => DaemonEnvelopeAck {
            envelope_id: None,
            target: None,
            cursor: None,
            status: "unknown".to_string(),
        },
    }
}

pub(crate) fn daemon_plugin_lifecycle_from_client(
    lifecycle: HubClientPluginLifecycle,
) -> DaemonPluginLifecycle {
    DaemonPluginLifecycle {
        package_name: lifecycle.package_name,
        state: package_state_label(lifecycle.state).to_string(),
        loaded: lifecycle.loaded,
        load_failure: lifecycle
            .load_failure
            .map(|failure| DaemonPluginLoadFailure {
                code: failure.code,
                message: failure.message,
            }),
    }
}

pub(crate) fn daemon_plugin_worker_counters_from_client(
    counters: HubClientPluginWorkerCounters,
) -> DaemonPluginWorkerCounters {
    DaemonPluginWorkerCounters {
        configured_queue_capacity: counters.configured_queue_capacity,
        configured_executor_concurrency: counters.configured_executor_concurrency,
        live_plugin_executors: counters.live_plugin_executors,
        live_executor_workers: counters.live_executor_workers,
        queued_jobs: counters.queued_jobs,
        in_flight_jobs: counters.in_flight_jobs,
    }
}

/// A delivery's target as text. A session is named with its hub, like an
/// endpoint: a session ID alone is never meaningful across hubs.
pub(crate) fn envelope_target_label(target: &EnvelopeTarget, hub_id: &str) -> String {
    match target {
        EnvelopeTarget::Endpoint { endpoint_id } => format!("endpoint:{}", endpoint_id.0),
        EnvelopeTarget::Client { client_id } => format!("client:{}", client_id.0),
        EnvelopeTarget::Session { session_id } => {
            crate::routed_endpoint::session_endpoint(hub_id, &session_id.0)
        }
        EnvelopeTarget::Subscription {
            session_id,
            subscription_id,
        } => format!(
            "subscription:{}/{}",
            crate::routed_endpoint::session_endpoint(hub_id, &session_id.0),
            subscription_id.0
        ),
        EnvelopeTarget::Plugin { plugin_key } => format!("plugin:{}", plugin_key.0),
        EnvelopeTarget::Stream { stream } => format!("stream:{stream}"),
        EnvelopeTarget::Topic { topic } => format!("topic:{topic}"),
    }
}
