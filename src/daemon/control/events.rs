//! Package-event subscription family.

use botster_hub_client::{DaemonEvent, DaemonRequest, DaemonResponse};

use crate::HubClientEvent;
use crate::HubDaemon;
use crate::client_api_dto::session::daemon_event_from_client;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::DaemonControlState;

pub(crate) fn events_from_client(events: Vec<HubClientEvent>) -> Vec<DaemonEvent> {
    events.into_iter().map(daemon_event_from_client).collect()
}

/// One attempt at a package-event subscription request. Contention on the
/// plane's or the router's locks never reaches the client: the attempt returns
/// the wait, and the owner re-polls the request when that lock is released.
pub(crate) enum EventAttempt {
    Done(Box<DaemonResponse>),
    Wait(
        crate::daemon::owner_signal::Parked,
        Option<crate::subscription::package_events::PendingRouterSubscribe>,
    ),
}

fn done(response: DaemonResponse) -> EventAttempt {
    EventAttempt::Done(Box::new(response))
}

/// Start a package-event subscription request as an owner-polled request.
pub(crate) fn control_step(
    connection_id: String,
    request: DaemonRequest,
) -> crate::daemon::control::pending::ControlStep {
    use crate::daemon::control::pending::{ControlPoll, ControlStep};
    use crate::subscription::package_events::PendingRouterSubscribe;

    // Only the owner thread touches this slot; the mutex makes it Send.
    let resume: std::sync::Arc<std::sync::Mutex<Option<PendingRouterSubscribe>>> =
        std::sync::Arc::default();
    let abandoned = std::sync::Arc::clone(&resume);
    ControlStep::pending_retirable(
        move |daemon, state| {
            let pending = resume.lock().expect("owner-only resume slot").take();
            match handle_client_event_request(daemon, state, &connection_id, &request, pending) {
                EventAttempt::Done(response) => ControlPoll::Ready(Ok(*response)),
                EventAttempt::Wait(parked, pending) => {
                    *resume.lock().expect("owner-only resume slot") = pending;
                    let waiter = state
                        .current_waiter_id
                        .expect("a polled request has its waiter");
                    state.signal_request_waits.insert(waiter, parked);
                    ControlPoll::Pending
                }
            }
        },
        move |daemon, _, waiter_id| {
            // An abandoned subscribe retires its provisional slot, as a refusal would.
            if let Some(pending) = abandoned.lock().expect("owner-only resume slot").take() {
                pending.abandon();
            }
            if let Some(runtime) = daemon.runtime() {
                runtime.retire_owner_core_waiter(waiter_id);
            }
        },
    )
}

pub(crate) fn handle_client_event_request(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    connection_id: &str,
    request: &DaemonRequest,
    resume: Option<crate::subscription::package_events::PendingRouterSubscribe>,
) -> EventAttempt {
    use crate::subscription::package_events::{
        AdmitRefusal, ClientEventAdmitError, SubscribeProgress, client_event_operator_error,
    };
    use botster_hub_client::hello_requires_package_event_subscriptions;

    if connection_id.is_empty() {
        return done(client_event_operator_error(
            ClientEventAdmitError::NotNegotiated,
            "package-events",
            "subscribe_events",
        ));
    }
    let negotiated = state
        .pending_runtime
        .admission
        .host_compatibility
        .get(connection_id)
        .is_some_and(|record| {
            hello_requires_package_event_subscriptions(&record.required_features)
        });
    let Some(runtime) = daemon.runtime() else {
        return done(client_event_operator_error(
            ClientEventAdmitError::Router(crate::package_event_router::EventPlaneStatus::ShedBusy),
            connection_id,
            "subscribe_events",
        ));
    };
    match request {
        DaemonRequest::SubscribeEvents {
            subscription_id,
            owner,
            name,
            subjects,
        } => {
            let progress = if let Some(pending) = resume {
                pending.resume(runtime.package_event_router())
            } else {
                if !negotiated {
                    return done(client_event_operator_error(
                        ClientEventAdmitError::NotNegotiated,
                        subscription_id,
                        "subscribe_events",
                    ));
                }
                match crate::daemon::client_events::admit_connection(state, connection_id) {
                    Ok(()) => {}
                    Err(AdmitRefusal::Busy(parked)) => return EventAttempt::Wait(parked, None),
                    Err(AdmitRefusal::Error(error)) => {
                        return done(client_event_operator_error(
                            error,
                            subscription_id,
                            "subscribe_events",
                        ));
                    }
                }
                state.event_plane.subscribe_mailbox(
                    connection_id,
                    subscription_id,
                    owner,
                    name,
                    subjects.clone(),
                    runtime.package_event_router().policy(),
                    runtime.package_event_router(),
                )
            };
            match progress {
                Ok(SubscribeProgress::Done(mailbox)) => {
                    done(subscribed(state, connection_id, subscription_id, mailbox))
                }
                Ok(SubscribeProgress::RouterBusy(pending, parked)) => {
                    EventAttempt::Wait(parked, Some(pending))
                }
                Err(AdmitRefusal::Busy(parked)) => EventAttempt::Wait(parked, None),
                Err(AdmitRefusal::Error(error)) => {
                    crate::daemon::client_events::note_cleanup(state, connection_id);
                    done(client_event_operator_error(
                        error,
                        subscription_id,
                        "subscribe_events",
                    ))
                }
            }
        }
        DaemonRequest::UnsubscribeEvents { subscription_id } => {
            if !negotiated {
                return done(client_event_operator_error(
                    ClientEventAdmitError::NotNegotiated,
                    subscription_id,
                    "unsubscribe_events",
                ));
            }
            match state
                .event_plane
                .retire_subscription(connection_id, subscription_id)
            {
                Ok(mailbox) => done(unsubscribed(
                    state,
                    connection_id,
                    subscription_id,
                    &mailbox,
                )),
                Err(AdmitRefusal::Busy(parked)) => EventAttempt::Wait(parked, None),
                Err(AdmitRefusal::Error(error)) => done(client_event_operator_error(
                    error,
                    subscription_id,
                    "unsubscribe_events",
                )),
            }
        }
        _ => done(client_event_operator_error(
            ClientEventAdmitError::NotNegotiated,
            connection_id,
            "subscribe_events",
        )),
    }
}

/// One attempt for a test that expects a response, not contention.
#[cfg(test)]
pub(crate) fn test_respond(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    connection_id: &str,
    request: DaemonRequest,
) -> DaemonResponse {
    match handle_client_event_request(daemon, state, connection_id, &request, None) {
        EventAttempt::Done(response) => *response,
        EventAttempt::Wait(..) => panic!("the test expected no lock contention"),
    }
}

/// Finish an admitted subscription: reserve its WebRTC channel label if the
/// connection is a WebRTC peer.
fn subscribed(
    state: &mut DaemonControlState,
    connection_id: &str,
    subscription_id: &str,
    mailbox: std::sync::Arc<crate::subscription::package_events::ClientEventMailbox>,
) -> DaemonResponse {
    use crate::subscription::package_events::{
        ClientEventAdmitError, client_event_operator_error, subscribe_events_response,
    };
    let subscription_id = subscription_id.to_string();
    let mut response = subscribe_events_response();
    let peer_generation = state
        .pending_runtime
        .admission
        .webrtc_admissions
        .get(connection_id)
        .map(|admission| match admission {
            crate::admission::unix_hello::WebrtcTerminalAdmission::Admitted {
                peer_generation,
                ..
            }
            | crate::admission::unix_hello::WebrtcTerminalAdmission::Rejected {
                peer_generation,
                ..
            } => *peer_generation,
        });
    if let Some(peer_generation) = peer_generation {
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
                crate::admission::connection_budget::ChannelClass::Event,
                subscription_id.clone(),
                generation,
                peer_generation,
                crate::admission::reservations::now_seconds(),
                crate::admission::reservations::ReservationBinding::Event {
                    mailbox: std::sync::Arc::clone(&mailbox),
                },
            );
        match reserved {
            Ok(reservation) => {
                let charged = state
                    .pending_runtime
                    .admission
                    .connection_budgets
                    .get_mut(&peer_generation)
                    .and_then(|budget| {
                        budget
                            .reserve(
                                reservation.label.clone(),
                                crate::admission::connection_budget::ChannelClass::Event,
                            )
                            .ok()
                    })
                    .is_some();
                if charged
                    && crate::daemon::owner_loop::arm_reservation_deadline(
                        state,
                        reservation.label.clone(),
                        peer_generation,
                        reservation.expires_in_seconds,
                    )
                {
                    response.subscription_reservation = Some(reservation);
                } else {
                    if let Some(budget) = state
                        .pending_runtime
                        .admission
                        .connection_budgets
                        .get_mut(&peer_generation)
                    {
                        let _ = budget.release(&reservation.label);
                    }
                    let _ = state
                        .pending_runtime
                        .admission
                        .reservations
                        .forget_label(&reservation.label, peer_generation);
                    crate::daemon::client_events::retire_mailbox(state, &mailbox);
                    return client_event_operator_error(
                        ClientEventAdmitError::ConnectionCapacity,
                        &subscription_id,
                        "subscribe_events",
                    );
                }
            }
            Err(_) => {
                crate::daemon::client_events::retire_mailbox(state, &mailbox);
                return client_event_operator_error(
                    ClientEventAdmitError::DuplicateSubscription,
                    &subscription_id,
                    "subscribe_events",
                );
            }
        }
    }
    response
}

/// Finish a retired subscription: release its WebRTC reservation, if any.
fn unsubscribed(
    state: &mut DaemonControlState,
    connection_id: &str,
    subscription_id: &str,
    mailbox: &std::sync::Arc<crate::subscription::package_events::ClientEventMailbox>,
) -> DaemonResponse {
    use crate::subscription::package_events::unsubscribe_events_response;
    crate::daemon::client_events::retire_mailbox(state, mailbox);
    if let Some(peer_generation) = state
        .pending_runtime
        .admission
        .webrtc_admissions
        .get(connection_id)
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
    {
        let labels = state
            .pending_runtime
            .admission
            .reservations
            .forget_unbound_subscription(
                crate::admission::connection_budget::ChannelClass::Event,
                subscription_id,
                peer_generation,
            );
        crate::daemon::owner_loop::retire_reservation_deadlines(state, labels.iter().cloned());
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
    unsubscribe_events_response()
}

pub(crate) fn reject_json_request(request: DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::SubscribeEvents { .. } | DaemonRequest::UnsubscribeEvents { .. } => {
            Err(DaemonTransportError::Protocol(
                "package event subscriptions require the host event handler",
            ))
        }
        _ => unreachable!("event family received a non-event request"),
    }
}
