//! Unix and WebRTC admission registration.

use std::sync::Arc;

use tokio::sync::oneshot;

use botster_hub_client::DaemonEvent;

use crate::HubDaemon;
use crate::admission::connection_budget::AggregateSendPermit;
use crate::admission::connection_budget::ChannelClass;
use crate::admission::reservations::ReservationBinding;
use crate::admission::reservations::{ReservationLookup, now_seconds};
use crate::admission::unix_hello::{
    HostCompatibilityRecord, UnixTerminalAdmission, WebrtcTerminalAdmission,
};
use crate::daemon::control::message::{
    BindReservedError, BoundSubscription, ControlMessage, ReservationInspectReply,
};
use crate::daemon::owner_budget::{
    CoreWorkPoll, OWNER_BUDGET_EXHAUSTED, ObligationPoll, drive_core_slot,
};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_loop::tick;
use crate::data_plane::driver::CoreTicket;
use crate::subscription::attach_routes::{BoundAdapterHandle, negotiated_unix_capability_set};

pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    message: ControlMessage,
) -> bool {
    match message {
        ControlMessage::AcceptedConnection { .. }
        | ControlMessage::ConnectionCleanup(_)
        | ControlMessage::RejectedConnection => false,
        ControlMessage::RegisterUnixAdmission {
            event_reader,
            client_id,
            admission,
            reply_tx,
            host_required_features,
        } => register_unix_admission(
            state,
            event_reader,
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
        ControlMessage::AuthorizeSubscriptionHelloAck {
            grant_id,
            label,
            frame_len,
            reply_tx,
        } => authorize_subscription_hello_ack(state, &grant_id, &label, frame_len, reply_tx),
        _ => unreachable!("connection family received a non-connection control message"),
    }
}

fn register_unix_admission(
    state: &mut DaemonControlState,
    event_reader: Arc<crate::subscription::package_events::ClientEventReader>,
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
            event_reader: Some(event_reader),
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
                event_reader: None,
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
        if !state.budget.reserve_peer(&grant_id) {
            let (mux, peer_generation) = match &admission {
                WebrtcTerminalAdmission::Admitted {
                    mux,
                    peer_generation,
                    ..
                }
                | WebrtcTerminalAdmission::Rejected {
                    mux,
                    peer_generation,
                    ..
                } => (mux.clone(), *peer_generation),
            };
            admission = WebrtcTerminalAdmission::Rejected {
                code: OWNER_BUDGET_EXHAUSTED,
                diagnostic: botster_hub_client::DaemonDiagnostic::action_failure(
                    "webrtc_admission",
                    "the daemon holds its maximum retained connections and cleanup",
                ),
                mux,
                peer_generation,
            };
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
            .insert(grant_id.clone(), admission);
        state
            .pending_runtime
            .admission
            .grant_by_peer_generation
            .insert(generation, grant_id);
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
            let bound = state
                .pending_runtime
                .admission
                .reservations
                .mark_bound(&label, peer_generation);
            if bound.is_some() {
                crate::daemon::owner_loop::retire_reservation_deadline(state, &label);
            }
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
            let bound = state
                .pending_runtime
                .admission
                .reservations
                .mark_bound(&label, peer_generation);
            if bound.is_some() {
                crate::daemon::owner_loop::retire_reservation_deadline(state, &label);
            }
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
    if daemon.runtime().is_none() {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    }
    // The reservation's attachment identity fences every step: a
    // replacement stream on the same route has a different epoch.
    let ReservationBinding::Terminal {
        owner: route_owner,
        identity,
        route: route_reservation,
    } = reservation.binding.clone()
    else {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    };
    if daemon.runtime().is_none()
        || !state.pending_runtime.stream_matches(
            &reservation.session_id,
            &reservation.subscription_id,
            &identity,
        )
    {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    }
    let Ok(capabilities) =
        negotiated_unix_capability_set(&required_features, terminal_requirement.as_ref())
    else {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    };
    let Some(aggregate) = state
        .pending_runtime
        .admission
        .connection_budgets
        .get(&peer_generation)
        .map(|budget| budget.aggregate())
    else {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    };
    // The bind holds one budget permit from here until the adapter is bound
    // and delivered, or until the exact generation it created is released.
    let Some(permit) = state.budget.reserve() else {
        retire_reserved_subscription(daemon, state, &grant_id, &label);
        let _ = reply_tx.send(Err(BindReservedError::OverLimit));
        return false;
    };
    let attach_now = tick(&mut state.logical_clock);
    let (adapter, handle) = mux.create_adapter_with_aggregate(aggregate);
    // Core attaches and binds the route in one call on the Core thread, as
    // the Unix path does. No Core route exists before this call, so a busy
    // session cannot overflow a declared route that has no adapter yet.
    let client_id = identity.client_id.clone();
    let mut attach_plan = Some(crate::runtime::AttachBindPlan {
        client_id: botster_core::ClientId(client_id.clone()),
        session_id: botster_core::SessionId(reservation.session_id.clone()),
        subscription_id: botster_core::SubscriptionId(reservation.subscription_id.clone()),
        capabilities,
        now_seconds: attach_now,
        adapter: Box::new(adapter),
    });
    let mut ticket = None;
    let session_id = reservation.session_id.clone();
    let subscription_id = reservation.subscription_id.clone();
    let mut reply_tx = Some(reply_tx);
    let mut usage = Some(usage);
    // Phase two of the obligation: release exactly the generation this
    // attach created when it turned out stale or undeliverable.
    let mut stale_generation: Option<botster_core::TerminalSubscriptionGeneration> = None;
    let mut detach_slot: Option<CoreTicket<Result<(), botster_core_daemon::CoreDaemonError>>> =
        None;
    crate::daemon::owner_budget::allocate_and_retain_owner_obligation(
        state,
        permit,
        "reserved_bind",
        move |daemon, state, waiter_id| {
            if let Some(stale) = stale_generation {
                return match drive_core_slot(
                    &mut detach_slot,
                    daemon,
                    state,
                    waiter_id,
                    |runtime, state, waiter_id| {
                        let now = tick(&mut state.logical_clock);
                        runtime.detach_route_exact_or_owned_for_owner(
                            waiter_id,
                            botster_core::ClientId(client_id.clone()),
                            botster_core::SessionId(session_id.clone()),
                            botster_core::SubscriptionId(subscription_id.clone()),
                            Some(stale),
                            now,
                        )
                    },
                ) {
                    CoreWorkPoll::Pending => ObligationPoll::Pending,
                    CoreWorkPoll::Retry => ObligationPoll::ReadyAgain,
                    CoreWorkPoll::Lost | CoreWorkPoll::Ready(_) => ObligationPoll::Done,
                };
            }
            let attached = match drive_core_slot(
                &mut ticket,
                daemon,
                state,
                waiter_id,
                |runtime, _, waiter_id| {
                    let plan = attach_plan
                        .take()
                        .expect("an attach plan is submitted once");
                    runtime.attach_and_bind_terminal_for_owner(waiter_id, plan)
                },
            ) {
                CoreWorkPoll::Pending => return ObligationPoll::Pending,
                CoreWorkPoll::Retry | CoreWorkPoll::Lost => None,
                CoreWorkPoll::Ready(result) => result.ok(),
            };
            let (Some(reply_tx), Some(usage)) = (reply_tx.take(), usage.take()) else {
                return ObligationPoll::Done;
            };
            let Some(generation) = attached else {
                // Core holds no route: attach_and_bind_on_core undoes its own
                // partial work. Release the Hub stream this attach opened.
                handle.close();
                abandon_unbound_terminal(
                    state,
                    &route_owner,
                    &identity,
                    route_reservation,
                    &session_id,
                    &subscription_id,
                );
                retire_reserved_subscription(daemon, state, &grant_id, &label);
                let _ = reply_tx.send(Err(BindReservedError::BindFailed));
                return ObligationPoll::Done;
            };
            // The route is attached and bound in Core. Fence every
            // owner-side mutation on the attachment identity and on the
            // reservation still being live; otherwise release exactly this
            // generation.
            let still_owned = state.pending_runtime.record_generation_if(
                &session_id,
                &subscription_id,
                &identity,
                generation,
            );
            let reservation_bound = still_owned
                && state
                    .pending_runtime
                    .admission
                    .reservations
                    .mark_bound(&label, peer_generation)
                    .is_some();
            if reservation_bound {
                crate::daemon::owner_loop::retire_reservation_deadline(state, &label);
            }
            if !reservation_bound {
                handle.close();
                abandon_unbound_terminal(
                    state,
                    &route_owner,
                    &identity,
                    route_reservation,
                    &session_id,
                    &subscription_id,
                );
                retire_reserved_subscription(daemon, state, &grant_id, &label);
                let _ = reply_tx.send(Err(BindReservedError::BindFailed));
                stale_generation = Some(generation);
                return ObligationPoll::ReadyAgain;
            }
            let registered = state.pending_runtime.mark_adapter_bound_if(
                &session_id,
                &subscription_id,
                &identity,
                generation,
                BoundAdapterHandle::WebRtc(handle.clone()),
            );
            debug_assert!(registered, "identity matched under the same owner turn");
            mux.register(
                session_id.clone(),
                subscription_id.clone(),
                generation.0,
                handle.clone(),
            );
            if reply_tx
                .send(Ok(BoundSubscription::Terminal {
                    handle: handle.clone(),
                    usage,
                    generation: generation.0,
                }))
                .is_err()
            {
                // The channel gave up waiting: nobody will drive this
                // adapter. Undo the bind for exactly this attachment.
                handle.close();
                let _ = state.pending_runtime.cancel_stream_if(
                    &session_id,
                    &subscription_id,
                    &identity,
                );
                retire_reserved_subscription(daemon, state, &grant_id, &label);
                stale_generation = Some(generation);
                return ObligationPoll::ReadyAgain;
            }
            ObligationPoll::Done
        },
    );
    false
}

/// Release the Hub attach stream of a terminal reservation that never
/// bound: cancel exactly `identity` and return the route key this attach
/// reserved. A replacement stream on the same route is left alone.
pub(crate) fn abandon_unbound_terminal(
    state: &mut DaemonControlState,
    owner: &crate::subscription::attach_routes::AttachStreamOwner,
    identity: &crate::subscription::attach_routes::AttachmentIdentity,
    route: crate::subscription::attach_routes::RouteReservation,
    session_id: &str,
    subscription_id: &str,
) {
    let _ = state
        .pending_runtime
        .cancel_stream_if(session_id, subscription_id, identity);
    crate::subscription::route_cleanup::release_failed_attach_route(
        &mut state.pending_runtime,
        owner,
        session_id,
        subscription_id,
        route,
    );
}

fn retire_reserved_subscription(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: &str,
    label: &str,
) {
    crate::daemon::owner_loop::retire_reservation_deadline(state, label);
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
    _daemon: &mut HubDaemon,
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
            if let crate::admission::reservations::ReservationBinding::Event { mailbox } =
                &reservation.binding
            {
                crate::daemon::client_events::retire_mailbox(state, mailbox);
            }
        }
        ChannelClass::Terminal => {
            // A reservation that never bound owns no Core route; release
            // only the Hub stream it opened. A bound route is released by
            // its adapter close.
            if reservation.state != crate::admission::reservations::ReservationState::Bound
                && let ReservationBinding::Terminal {
                    owner,
                    identity,
                    route,
                } = &reservation.binding
            {
                abandon_unbound_terminal(
                    state,
                    owner,
                    identity,
                    *route,
                    &reservation.session_id,
                    &reservation.subscription_id,
                );
            }
        }
        ChannelClass::Control => {}
    }
}

fn authorize_subscription_send(
    state: &mut DaemonControlState,
    grant_id: &str,
    label: &str,
    frame_len: usize,
    reply_tx: oneshot::Sender<Option<AggregateSendPermit>>,
) -> bool {
    let Some(peer_generation) = admitted_peer_generation(state, grant_id) else {
        let _ = reply_tx.send(None);
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
    let permit = bound.then(|| {
        state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&peer_generation)?
            .authorize_send(label, frame_len)
    });
    let _ = reply_tx.send(permit.flatten());
    false
}

fn authorize_subscription_hello_ack(
    state: &mut DaemonControlState,
    grant_id: &str,
    label: &str,
    frame_len: usize,
    reply_tx: oneshot::Sender<Option<AggregateSendPermit>>,
) -> bool {
    let Some(peer_generation) = admitted_peer_generation(state, grant_id) else {
        let _ = reply_tx.send(None);
        return false;
    };
    // Entity and event channels acknowledge before the bind; a terminal
    // channel acknowledges after its attach+bind, when the reservation is
    // already bound.
    let live = state
        .pending_runtime
        .admission
        .reservations
        .reservation_for_label(label, peer_generation)
        .is_some_and(|reservation| {
            matches!(
                (reservation.class, reservation.state),
                (_, crate::admission::reservations::ReservationState::Live)
                    | (
                        ChannelClass::Terminal,
                        crate::admission::reservations::ReservationState::Bound
                    )
            )
        });
    let permit = live.then(|| {
        state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&peer_generation)?
            .authorize_send(label, frame_len)
    });
    let _ = reply_tx.send(permit.flatten());
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

pub(crate) fn emit_reservation_expired(
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
    crate::daemon::owner_loop::retire_reservation_deadline(state, label);
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
        // Every class reports the expiry by label when it happens, so a
        // client needs no timer of its own. A terminal reservation has no
        // Core generation, so it sends no terminal_subscription_closed.
        mux.push_host_event(DaemonEvent::RuntimeObservation {
            kind: format!(
                "subscription_channel_rejected:{}:{label}",
                crate::transport::webrtc::subscription_channel::RESERVATION_EXPIRED_REASON
            ),
        });
        let event = match reservation.class {
            ChannelClass::Terminal => return,
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
