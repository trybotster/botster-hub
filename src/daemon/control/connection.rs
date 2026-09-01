//! Unix and WebRTC admission registration.

use std::sync::Arc;

use tokio::sync::oneshot;

use botster_hub_client::{DaemonEvent, TERMINAL_SUBSCRIPTION_CLOSED_RESERVATION_EXPIRED};

use crate::HubDaemon;
use crate::admission::connection_budget::ChannelClass;
use crate::admission::reservations::ReservationBinding;
use crate::admission::reservations::{ReservationLookup, now_seconds};
use crate::admission::unix_hello::{
    HostCompatibilityRecord, UnixTerminalAdmission, WebrtcTerminalAdmission,
};
use crate::daemon::control::message::{
    BindReservedError, BoundSubscription, ControlMessage, ReservationInspectReply,
};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_loop::tick;
use crate::subscription::attach_routes::{WebrtcBindRequest, bind_webrtc_adapter_after_attaching};

pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    message: ControlMessage,
) -> bool {
    match message {
        ControlMessage::AcceptedConnection { .. } | ControlMessage::RejectedConnection => false,
        ControlMessage::RegisterUnixAdmission {
            client_id,
            admission,
            reply_tx,
            host_required_features,
        } => register_unix_admission(
            state,
            client_id,
            admission,
            reply_tx,
            host_required_features,
        ),
        ControlMessage::RegisterWebrtcAdmission {
            grant_id,
            admission,
            host_required_features,
        } => register_webrtc_admission(daemon, state, grant_id, admission, host_required_features),
        ControlMessage::InspectReservation {
            grant_id,
            label,
            reply_tx,
        } => inspect_reservation(daemon, state, grant_id, label, reply_tx),
        ControlMessage::BindReservedSubscription {
            grant_id,
            label,
            reply_tx,
        } => bind_reserved_subscription(daemon, state, grant_id, label, reply_tx),
        ControlMessage::RetireReservedSubscription { grant_id, label } => {
            retire_reserved_subscription(daemon, state, &grant_id, &label);
            false
        }
        ControlMessage::AuthorizeSubscriptionSend {
            grant_id,
            label,
            frame_len,
            reply_tx,
        } => authorize_subscription_send(state, &grant_id, &label, frame_len, reply_tx),
        _ => unreachable!("connection family received a non-connection control message"),
    }
}

fn register_unix_admission(
    state: &mut DaemonControlState,
    client_id: String,
    admission: UnixTerminalAdmission,
    reply_tx: oneshot::Sender<()>,
    host_required_features: Vec<String>,
) -> bool {
    if let UnixTerminalAdmission::Admitted { mux, .. } = &admission {
        mux.bind_close_work(Arc::clone(&state.pending_runtime.close_work));
        mux.bind_close_source(state.pending_runtime.close_source.clone());
    }
    state.pending_runtime.admission.host_compatibility.insert(
        client_id.clone(),
        HostCompatibilityRecord {
            required_features: host_required_features,
        },
    );
    state
        .pending_runtime
        .admission
        .unix_admissions
        .insert(client_id, admission);
    let _ = reply_tx.send(());
    false
}

fn register_webrtc_admission(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: String,
    mut admission: WebrtcTerminalAdmission,
    host_required_features: Vec<String>,
) -> bool {
    if daemon.local_webrtc().has_live_peer(&grant_id) {
        let mux = match &admission {
            WebrtcTerminalAdmission::Admitted { mux, .. }
            | WebrtcTerminalAdmission::Rejected { mux, .. } => mux,
        };
        mux.bind_close_work(Arc::clone(&state.pending_runtime.close_work));
        mux.bind_close_source(state.pending_runtime.close_source.clone());
        state.pending_runtime.admission.host_compatibility.insert(
            grant_id.clone(),
            HostCompatibilityRecord {
                required_features: host_required_features,
            },
        );
        state.pending_runtime.admission.next_peer_generation = state
            .pending_runtime
            .admission
            .next_peer_generation
            .saturating_add(1);
        let generation = state.pending_runtime.admission.next_peer_generation;
        match &mut admission {
            WebrtcTerminalAdmission::Admitted {
                peer_generation, ..
            }
            | WebrtcTerminalAdmission::Rejected {
                peer_generation, ..
            } => *peer_generation = generation,
        }
        let mut budget = crate::admission::connection_budget::ConnectionBudget::default();
        let _ = budget.reserve("control".to_string(), ChannelClass::Control);
        state
            .pending_runtime
            .admission
            .connection_budgets
            .insert(generation, budget);
        state
            .pending_runtime
            .admission
            .webrtc_admissions
            .insert(grant_id, admission);
    }
    false
}

fn inspect_reservation(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: String,
    label: String,
    reply_tx: oneshot::Sender<ReservationInspectReply>,
) -> bool {
    let Some(peer_generation) = admitted_peer_generation(state, &grant_id) else {
        let _ = reply_tx.send(ReservationInspectReply::Unknown);
        return false;
    };
    let now = now_seconds();
    let lookup =
        state
            .pending_runtime
            .admission
            .reservations
            .lookup_label(&label, peer_generation, now);
    let reply = match lookup {
        ReservationLookup::Unknown => {
            if state
                .pending_runtime
                .admission
                .reservations
                .label_peer_generation(&label)
                .is_some_and(|owner| owner != peer_generation)
            {
                ReservationInspectReply::Stale
            } else {
                ReservationInspectReply::Unknown
            }
        }
        ReservationLookup::Bound => ReservationInspectReply::Bound,
        ReservationLookup::Expired => {
            emit_reservation_expired(daemon, state, &grant_id, peer_generation, &label, now);
            match state
                .pending_runtime
                .admission
                .reservations
                .reservation_for_label(&label, peer_generation)
            {
                Some(reservation) => ReservationInspectReply::Expired {
                    session_id: reservation.session_id.clone(),
                    subscription_id: reservation.subscription_id.clone(),
                    generation: reservation.generation,
                },
                None => ReservationInspectReply::Unknown,
            }
        }
        ReservationLookup::Live => match state
            .pending_runtime
            .admission
            .reservations
            .reservation_for_label(&label, peer_generation)
        {
            Some(_)
                if state
                    .pending_runtime
                    .admission
                    .connection_budgets
                    .get(&peer_generation)
                    .and_then(|budget| budget.usage(&label))
                    .is_none() =>
            {
                ReservationInspectReply::OverLimit
            }
            Some(reservation) => ReservationInspectReply::Live {
                class: reservation.class,
                session_id: reservation.session_id.clone(),
                subscription_id: reservation.subscription_id.clone(),
                generation: reservation.generation,
            },
            None => ReservationInspectReply::Unknown,
        },
    };
    if reply == ReservationInspectReply::OverLimit {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
    }
    let _ = reply_tx.send(reply);
    false
}

fn bind_reserved_subscription(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: String,
    label: String,
    reply_tx: oneshot::Sender<Result<BoundSubscription, BindReservedError>>,
) -> bool {
    let Some(peer_generation) = admitted_peer_generation(state, &grant_id) else {
        let _ = reply_tx.send(Err(BindReservedError::Unknown));
        return false;
    };
    let now = now_seconds();
    match state
        .pending_runtime
        .admission
        .reservations
        .lookup_label(&label, peer_generation, now)
    {
        ReservationLookup::Unknown => {
            let error = if state
                .pending_runtime
                .admission
                .reservations
                .label_peer_generation(&label)
                .is_some_and(|owner| owner != peer_generation)
            {
                BindReservedError::Stale
            } else {
                BindReservedError::Unknown
            };
            let _ = reply_tx.send(Err(error));
            return false;
        }
        ReservationLookup::Bound => {
            let _ = reply_tx.send(Err(BindReservedError::Bound));
            return false;
        }
        ReservationLookup::Expired => {
            emit_reservation_expired(daemon, state, &grant_id, peer_generation, &label, now);
            let _ = reply_tx.send(Err(BindReservedError::Expired));
            return false;
        }
        ReservationLookup::Live => {}
    }
    let Some(reservation) = state
        .pending_runtime
        .admission
        .reservations
        .reservation_for_label(&label, peer_generation)
        .cloned()
    else {
        let _ = reply_tx.send(Err(BindReservedError::Unknown));
        return false;
    };
    let Some(usage) = state
        .pending_runtime
        .admission
        .connection_budgets
        .get(&peer_generation)
        .and_then(|budget| budget.usage(&label))
    else {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::OverLimit));
        return false;
    };
    match reservation.class {
        ChannelClass::Entity => {
            let ReservationBinding::Entity { receiver } = &reservation.binding else {
                retire_reserved_subscription(daemon, state, &grant_id, &label);
                let _ = reply_tx.send(Err(BindReservedError::BindFailed));
                return false;
            };
            let Some(receiver) = receiver
                .lock()
                .ok()
                .and_then(|mut receiver| receiver.take())
            else {
                retire_reserved_subscription(daemon, state, &grant_id, &label);
                let _ = reply_tx.send(Err(BindReservedError::Bound));
                return false;
            };
            let _ = state
                .pending_runtime
                .admission
                .reservations
                .mark_bound(&label, peer_generation);
            let _ = reply_tx.send(Ok(BoundSubscription::Entity { receiver, usage }));
            return false;
        }
        ChannelClass::Event => {
            let ReservationBinding::Event { mailbox } = &reservation.binding else {
                retire_reserved_subscription(daemon, state, &grant_id, &label);
                let _ = reply_tx.send(Err(BindReservedError::BindFailed));
                return false;
            };
            let mailbox = Arc::clone(mailbox);
            let _ = state
                .pending_runtime
                .admission
                .reservations
                .mark_bound(&label, peer_generation);
            let _ = reply_tx.send(Ok(BoundSubscription::Event { mailbox, usage }));
            return false;
        }
        ChannelClass::Control => {
            retire_reserved_subscription(daemon, state, &grant_id, &label);
            let _ = reply_tx.send(Err(BindReservedError::BindFailed));
            return false;
        }
        ChannelClass::Terminal => {}
    }
    let Some(WebrtcTerminalAdmission::Admitted {
        required_features,
        mux,
        terminal_requirement,
        ..
    }) = state
        .pending_runtime
        .admission
        .webrtc_admissions
        .get(&grant_id)
        .cloned()
    else {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    };
    let Some(runtime) = daemon.runtime_mut() else {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    };
    let Some(client_id) = state
        .pending_runtime
        .stream_owner_client_id(&reservation.session_id, &reservation.subscription_id)
    else {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    };
    let bind_now = tick(&mut state.logical_clock);
    let aggregate = state
        .pending_runtime
        .admission
        .connection_budgets
        .get(&peer_generation)
        .map(|budget| budget.aggregate());
    let result = bind_webrtc_adapter_after_attaching(
        &mut state.pending_runtime,
        runtime,
        WebrtcBindRequest {
            client_id: &client_id,
            session_id: &reservation.session_id,
            subscription_id: &reservation.subscription_id,
            required_features: &required_features,
            terminal_requirement: terminal_requirement.as_ref(),
            now_seconds: bind_now,
            mux: Some(&mux),
            aggregate,
        },
    );
    match result {
        Ok(Some(handle)) => {
            let _ = state
                .pending_runtime
                .admission
                .reservations
                .mark_bound(&label, peer_generation);
            let _ = reply_tx.send(Ok(BoundSubscription::Terminal { handle, usage }));
        }
        Ok(None) | Err(()) => {
            retire_reserved_subscription(daemon, state, &grant_id, &label);
            let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        }
    }
    false
}

fn retire_reserved_subscription(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: &str,
    label: &str,
) {
    let Some(peer_generation) = admitted_peer_generation(state, grant_id) else {
        return;
    };
    let reservation = state
        .pending_runtime
        .admission
        .reservations
        .reservation_for_label(label, peer_generation)
        .cloned();
    if let Some(reservation) = reservation.as_ref() {
        retire_route_owner(daemon, state, grant_id, reservation);
    }
    if state
        .pending_runtime
        .admission
        .reservations
        .forget_label(label, peer_generation)
        && let Some(budget) = state
            .pending_runtime
            .admission
            .connection_budgets
            .get_mut(&peer_generation)
    {
        let _ = budget.release(label);
    }
}

pub(crate) fn retire_route_owner(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: &str,
    reservation: &crate::admission::reservations::TerminalReservation,
) {
    match reservation.class {
        ChannelClass::Entity => {
            let owned = state
                .entity_subscriptions
                .get(&reservation.subscription_id)
                .is_some_and(|subscription| {
                    subscription.owner_grant_id.as_deref() == Some(grant_id)
                });
            if owned {
                crate::daemon::control::entities::remove_entity_subscription(
                    state,
                    &reservation.subscription_id,
                );
            }
        }
        ChannelClass::Event => {
            if let Some(runtime) = daemon.runtime() {
                state.event_plane.cleanup_subscription(
                    grant_id,
                    &reservation.subscription_id,
                    runtime.package_event_router(),
                );
            }
        }
        ChannelClass::Control | ChannelClass::Terminal => {}
    }
}

fn authorize_subscription_send(
    state: &mut DaemonControlState,
    grant_id: &str,
    label: &str,
    frame_len: usize,
    reply_tx: oneshot::Sender<bool>,
) -> bool {
    let Some(peer_generation) = admitted_peer_generation(state, grant_id) else {
        let _ = reply_tx.send(false);
        return false;
    };
    let bound = state
        .pending_runtime
        .admission
        .reservations
        .reservation_for_label(label, peer_generation)
        .is_some_and(|reservation| {
            reservation.state == crate::admission::reservations::ReservationState::Bound
        });
    let permitted = bound
        && state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&peer_generation)
            .is_some_and(|budget| budget.permits_send(frame_len));
    let _ = reply_tx.send(permitted);
    false
}

fn admitted_peer_generation(state: &DaemonControlState, grant_id: &str) -> Option<u64> {
    match state
        .pending_runtime
        .admission
        .webrtc_admissions
        .get(grant_id)
    {
        Some(WebrtcTerminalAdmission::Admitted {
            peer_generation, ..
        })
        | Some(WebrtcTerminalAdmission::Rejected {
            peer_generation, ..
        }) => Some(*peer_generation),
        None => None,
    }
}

fn emit_reservation_expired(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: &str,
    peer_generation: u64,
    label: &str,
    now: u64,
) {
    let Some(reservation) =
        state
            .pending_runtime
            .admission
            .reservations
            .expire_label(label, peer_generation, now)
    else {
        return;
    };
    retire_route_owner(daemon, state, grant_id, &reservation);
    if let Some(budget) = state
        .pending_runtime
        .admission
        .connection_budgets
        .get_mut(&peer_generation)
    {
        let _ = budget.release(label);
    }
    if let Some(mux) = state
        .pending_runtime
        .admission
        .webrtc_admissions
        .get(grant_id)
        .map(|admission| match admission {
            WebrtcTerminalAdmission::Admitted { mux, .. }
            | WebrtcTerminalAdmission::Rejected { mux, .. } => mux,
        })
    {
        let event = match reservation.class {
            ChannelClass::Terminal => DaemonEvent::TerminalSubscriptionClosed {
                session_id: reservation.session_id,
                subscription_id: reservation.subscription_id,
                generation: reservation.generation,
                reason: TERMINAL_SUBSCRIPTION_CLOSED_RESERVATION_EXPIRED.to_string(),
            },
            ChannelClass::Entity => DaemonEvent::RuntimeObservation {
                kind: format!(
                    "entity_subscription_closed:{}:{}:reservation_expired",
                    reservation.subscription_id, reservation.generation
                ),
            },
            ChannelClass::Event => DaemonEvent::RuntimeObservation {
                kind: format!(
                    "package_event_subscription_closed:{}:{}:reservation_expired",
                    reservation.subscription_id, reservation.generation
                ),
            },
            ChannelClass::Control => return,
        };
        mux.push_host_event(event);
    }
}
