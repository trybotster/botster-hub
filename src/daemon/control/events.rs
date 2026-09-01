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

pub(crate) fn handle_client_event_request(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    connection_id: &str,
    request: DaemonRequest,
) -> DaemonResponse {
    use crate::subscription::package_events::{
        ClientEventAdmitError, client_event_operator_error, subscribe_events_response,
        unsubscribe_events_response,
    };
    use botster_hub_client::hello_requires_package_event_subscriptions;

    if connection_id.is_empty() {
        return client_event_operator_error(
            ClientEventAdmitError::NotNegotiated,
            "package-events",
            "subscribe_events",
        );
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
        return client_event_operator_error(
            ClientEventAdmitError::Router(crate::package_event_router::EventPlaneStatus::ShedBusy),
            connection_id,
            "subscribe_events",
        );
    };
    match request {
        DaemonRequest::SubscribeEvents {
            subscription_id,
            owner,
            name,
            subjects,
        } => {
            if !negotiated {
                return client_event_operator_error(
                    ClientEventAdmitError::NotNegotiated,
                    &subscription_id,
                    "subscribe_events",
                );
            }
            match state.event_plane.try_subscribe(
                connection_id,
                &subscription_id,
                &owner,
                &name,
                subjects,
                runtime.package_event_router().policy(),
                runtime.package_event_router(),
            ) {
                Ok(()) => {
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
                        let Some(mailbox) = state
                            .event_plane
                            .subscription_mailbox(connection_id, &subscription_id)
                        else {
                            let _ = state.event_plane.try_unsubscribe(
                                connection_id,
                                &subscription_id,
                                runtime.package_event_router(),
                            );
                            return client_event_operator_error(
                                ClientEventAdmitError::Router(
                                    crate::package_event_router::EventPlaneStatus::ShedBusy,
                                ),
                                &subscription_id,
                                "subscribe_events",
                            );
                        };
                        state.pending_runtime.admission.next_subscription_generation = state
                            .pending_runtime
                            .admission
                            .next_subscription_generation
                            .saturating_add(1);
                        let generation =
                            state.pending_runtime.admission.next_subscription_generation;
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
                                    mailbox,
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
                                if charged {
                                    response.subscription_reservation = Some(reservation);
                                } else {
                                    let _ = state
                                        .pending_runtime
                                        .admission
                                        .reservations
                                        .forget_label(&reservation.label, peer_generation);
                                    let _ = state.event_plane.try_unsubscribe(
                                        connection_id,
                                        &subscription_id,
                                        runtime.package_event_router(),
                                    );
                                    return client_event_operator_error(
                                        ClientEventAdmitError::ConnectionCapacity,
                                        &subscription_id,
                                        "subscribe_events",
                                    );
                                }
                            }
                            Err(_) => {
                                let _ = state.event_plane.try_unsubscribe(
                                    connection_id,
                                    &subscription_id,
                                    runtime.package_event_router(),
                                );
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
                Err(error) => {
                    client_event_operator_error(error, &subscription_id, "subscribe_events")
                }
            }
        }
        DaemonRequest::UnsubscribeEvents { subscription_id } => {
            if !negotiated {
                return client_event_operator_error(
                    ClientEventAdmitError::NotNegotiated,
                    &subscription_id,
                    "unsubscribe_events",
                );
            }
            match state.event_plane.try_unsubscribe(
                connection_id,
                &subscription_id,
                runtime.package_event_router(),
            ) {
                Ok(()) => {
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
                    unsubscribe_events_response()
                }
                Err(error) => {
                    client_event_operator_error(error, &subscription_id, "unsubscribe_events")
                }
            }
        }
        _ => client_event_operator_error(
            ClientEventAdmitError::NotNegotiated,
            connection_id,
            "subscribe_events",
        ),
    }
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
