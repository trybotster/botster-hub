//! Package-event subscription family.

use botster_hub_client::{DaemonEvent, DaemonRequest, DaemonResponse};

use crate::HubClientEvent;
use crate::HubDaemon;
use crate::client_api_dto::session::daemon_event_from_client;
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
                Ok(()) => subscribe_events_response(),
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
                Ok(()) => unsubscribe_events_response(),
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
