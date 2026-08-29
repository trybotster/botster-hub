//! Entity subscription control-message family.

use crate::HubDaemon;
use crate::client_api_dto::response::daemon_response_base;
use crate::daemon::control::message::{ControlMessage, ControlReplySender};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::DaemonControlState;
use crate::subscription::entity::{
    EntityFrameSender, entity_subscription_error, register_entity_subscription,
};
use botster_hub_client::{DaemonRequest, DaemonResponse, DaemonResponseKind};

pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    message: ControlMessage,
) -> bool {
    match message {
        ControlMessage::SubscribeEntities {
            entity_type,
            subscription_id,
            frame_tx,
            reply_tx,
            grant_id,
        } => subscribe(
            daemon,
            state,
            entity_type,
            subscription_id,
            frame_tx,
            reply_tx,
            grant_id,
        ),
        ControlMessage::UnsubscribeEntities {
            subscription_id,
            reply_tx,
            grant_id,
        } => unsubscribe(daemon, state, subscription_id, reply_tx, grant_id),
        _ => unreachable!("entity family received a non-entity control message"),
    }
}

pub(crate) fn subscribe(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    entity_type: String,
    subscription_id: String,
    frame_tx: EntityFrameSender,
    reply_tx: ControlReplySender,
    grant_id: Option<String>,
) -> bool {
    // Late WebRTC control messages after PeerClosed must not recreate peer-owned state.
    if let Some(grant_id) = grant_id.as_deref()
        && !daemon.local_webrtc().has_live_peer(grant_id)
    {
        let _ = reply_tx.send(Ok(entity_subscription_error(
            "local_webrtc_peer_gone",
            &subscription_id,
            "local WebRTC peer is no longer live",
        )));
        return false;
    }
    let response = register_entity_subscription(
        daemon,
        state,
        entity_type,
        subscription_id,
        frame_tx,
        grant_id,
    );
    let _ = reply_tx.send(response);
    false
}

pub(crate) fn unsubscribe(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    subscription_id: String,
    reply_tx: Option<ControlReplySender>,
    grant_id: Option<String>,
) -> bool {
    if let Some(grant_id) = grant_id.as_deref()
        && !daemon.local_webrtc().has_live_peer(grant_id)
    {
        // Peer already gone: owner-checked residual cleanup only. Never delete a row now
        // owned by a different live grant (subscription-id reuse after PeerClosed).
        let should_remove = match state.entity_subscriptions.get(&subscription_id) {
            None => false,
            Some(subscription) => match subscription.owner_grant_id.as_deref() {
                None => true,
                Some(owner) => owner == grant_id,
            },
        };
        if should_remove
            && state
                .entity_subscriptions
                .remove(&subscription_id)
                .is_some()
        {
            state.lifecycle_counters.live_entity_subscriptions = state
                .lifecycle_counters
                .live_entity_subscriptions
                .saturating_sub(1);
            state.released_entity_generations = state.released_entity_generations.saturating_add(1);
        }
        if let Some(reply_tx) = reply_tx {
            // Idempotent unsubscribed reply for the stale client even when the row is
            // preserved for a replacement owner.
            let _ = reply_tx.send(Ok(daemon_response_base(
                DaemonResponseKind::EntityUnsubscribed,
            )));
        }
        return false;
    }
    if state
        .entity_subscriptions
        .remove(&subscription_id)
        .is_some()
    {
        state.lifecycle_counters.live_entity_subscriptions = state
            .lifecycle_counters
            .live_entity_subscriptions
            .saturating_sub(1);
        state.released_entity_generations = state.released_entity_generations.saturating_add(1);
    }
    if let Some(reply_tx) = reply_tx {
        let _ = reply_tx.send(Ok(daemon_response_base(
            DaemonResponseKind::EntityUnsubscribed,
        )));
    }
    false
}

pub(crate) fn reject_json_request(request: DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::SubscribeEntities { .. } | DaemonRequest::UnsubscribeEntities { .. } => {
            Err(DaemonTransportError::Protocol(
                "entity subscriptions require the held-open stream handler",
            ))
        }
        _ => unreachable!("entity family received a non-entity request"),
    }
}
