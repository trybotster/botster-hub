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
use tokio::sync::mpsc as tokio_mpsc;

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
            frame_rx,
            reply_tx,
            grant_id,
        } => subscribe(
            daemon,
            state,
            EntitySubscribeRequest {
                entity_type,
                subscription_id,
                frame_tx,
                frame_rx,
                reply_tx,
                grant_id,
            },
        ),
        ControlMessage::UnsubscribeEntities {
            subscription_id,
            reply_tx,
            grant_id,
        } => unsubscribe(daemon, state, subscription_id, reply_tx, grant_id),
        _ => unreachable!("entity family received a non-entity control message"),
    }
}

struct EntitySubscribeRequest {
    entity_type: String,
    subscription_id: String,
    frame_tx: EntityFrameSender,
    frame_rx: Option<tokio_mpsc::Receiver<botster_hub_client::DaemonEntityFrame>>,
    reply_tx: ControlReplySender,
    grant_id: Option<String>,
}

fn subscribe(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    request: EntitySubscribeRequest,
) -> bool {
    let EntitySubscribeRequest {
        entity_type,
        subscription_id,
        frame_tx,
        frame_rx,
        reply_tx,
        grant_id,
    } = request;
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
    let mut response = register_entity_subscription(
        daemon,
        state,
        entity_type,
        subscription_id.clone(),
        frame_tx,
        grant_id.clone(),
    );
    if response
        .as_ref()
        .is_ok_and(|response| response.kind == DaemonResponseKind::EntitySubscribed)
        && let (Some(grant_id), Some(frame_rx)) = (grant_id.as_deref(), frame_rx)
    {
        let Some(peer_generation) = state
            .pending_runtime
            .admission
            .webrtc_admissions
            .get(grant_id)
            .map(|admission| match admission {
                crate::admission::unix_hello::WebrtcTerminalAdmission::Admitted {
                    peer_generation,
                    ..
                }
                | crate::admission::unix_hello::WebrtcTerminalAdmission::Rejected {
                    peer_generation,
                    ..
                } => *peer_generation,
            })
        else {
            remove_entity_subscription(state, &subscription_id);
            let _ = reply_tx.send(Ok(entity_subscription_error(
                "local_webrtc_peer_gone",
                &subscription_id,
                "local WebRTC peer admission is no longer live",
            )));
            return false;
        };
        state.pending_runtime.admission.next_subscription_generation = state
            .pending_runtime
            .admission
            .next_subscription_generation
            .saturating_add(1);
        let generation = state.pending_runtime.admission.next_subscription_generation;
        let reserved = state
            .pending_runtime
            .admission
            .reservations
            .reserve_subscription(
                crate::admission::connection_budget::ChannelClass::Entity,
                subscription_id.clone(),
                generation,
                peer_generation,
                crate::admission::reservations::now_seconds(),
                crate::admission::reservations::ReservationBinding::Entity {
                    receiver: std::sync::Arc::new(std::sync::Mutex::new(Some(frame_rx))),
                },
            );
        match reserved {
            Ok(reservation) => {
                let budget = state
                    .pending_runtime
                    .admission
                    .connection_budgets
                    .get_mut(&peer_generation)
                    .and_then(|budget| {
                        budget
                            .reserve(
                                reservation.label.clone(),
                                crate::admission::connection_budget::ChannelClass::Entity,
                            )
                            .ok()
                    });
                if budget.is_some() {
                    if let Ok(response) = response.as_mut() {
                        response.subscription_reservation = Some(reservation);
                    }
                } else {
                    let _ = state
                        .pending_runtime
                        .admission
                        .reservations
                        .forget_label(&reservation.label, peer_generation);
                    remove_entity_subscription(state, &subscription_id);
                    response = Ok(entity_subscription_error(
                        "connection_channel_limit",
                        &subscription_id,
                        "the WebRTC connection channel budget rejected the reservation",
                    ));
                }
            }
            Err(_) => {
                remove_entity_subscription(state, &subscription_id);
                response = Ok(entity_subscription_error(
                    "reservation_label_conflict",
                    &subscription_id,
                    "a live entity reservation already exists for this route",
                ));
            }
        }
    }
    let _ = reply_tx.send(response);
    false
}

fn unsubscribe(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    subscription_id: String,
    reply_tx: Option<ControlReplySender>,
    grant_id: Option<String>,
) -> bool {
    let peer_generation = grant_id.as_deref().and_then(|grant_id| {
        state
            .pending_runtime
            .admission
            .webrtc_admissions
            .get(grant_id)
            .map(|admission| match admission {
                crate::admission::unix_hello::WebrtcTerminalAdmission::Admitted {
                    peer_generation,
                    ..
                }
                | crate::admission::unix_hello::WebrtcTerminalAdmission::Rejected {
                    peer_generation,
                    ..
                } => *peer_generation,
            })
    });
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
    if let Some(peer_generation) = peer_generation {
        let labels = state
            .pending_runtime
            .admission
            .reservations
            .forget_subscription(
                crate::admission::connection_budget::ChannelClass::Entity,
                &subscription_id,
                peer_generation,
            );
        if let Some(budget) = state
            .pending_runtime
            .admission
            .connection_budgets
            .get_mut(&peer_generation)
        {
            for label in labels {
                let _ = budget.release(&label);
            }
        }
    }
    if let Some(reply_tx) = reply_tx {
        let _ = reply_tx.send(Ok(daemon_response_base(
            DaemonResponseKind::EntityUnsubscribed,
        )));
    }
    false
}

pub(crate) fn remove_entity_subscription(state: &mut DaemonControlState, subscription_id: &str) {
    if state.entity_subscriptions.remove(subscription_id).is_some() {
        state.lifecycle_counters.live_entity_subscriptions = state
            .lifecycle_counters
            .live_entity_subscriptions
            .saturating_sub(1);
        state.released_entity_generations = state.released_entity_generations.saturating_add(1);
    }
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
