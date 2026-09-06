//! Session shutdown classification against Core's control-plane lifecycle.
//!
//! Classification runs on the Core owner thread through one ticket; the
//! session request family polls it and starts the shutdown operation once
//! the session is known to be active or stopping.

use botster_core::{SessionId, SessionLifecycleState};
use botster_core_daemon::{CoreDaemonError, RegistrySessionState, SessionLifecycleLookup};
use botster_hub_client::{DaemonResponse, DaemonSessionCleanup};

use crate::client_api_dto::response::{daemon_session_cleanup, daemon_unknown_session_cleanup};
use crate::data_plane::driver::CoreTicket;

#[derive(Debug, Clone)]
pub(crate) enum ShutdownSessionClassification {
    Active,
    Cleanup(DaemonSessionCleanup),
    Missing,
    Stopping,
}

/// Classify one session on the Core owner thread.
pub(crate) fn begin_shutdown_classification(
    runtime: &crate::HubRuntime,
    session_id: &str,
    now_seconds: u64,
) -> CoreTicket<Result<ShutdownSessionClassification, CoreDaemonError>> {
    let session_id = session_id.to_string();
    runtime.submit_core(move |daemon| {
        let lookup = daemon.observe_session_lifecycle(&SessionId(session_id.clone()), now_seconds);
        classify_lookup(&session_id, lookup)
    })
}

pub(crate) fn classify_lookup(
    session_id: &str,
    lookup: Result<SessionLifecycleLookup, CoreDaemonError>,
) -> Result<ShutdownSessionClassification, CoreDaemonError> {
    match lookup {
        Ok(SessionLifecycleLookup::Found(record)) => {
            Ok(classify_found_session_lifecycle(session_id, &record))
        }
        Ok(SessionLifecycleLookup::Absent) => Ok(ShutdownSessionClassification::Missing),
        Ok(_) => Err(CoreDaemonError::Shutdown),
        Err(CoreDaemonError::UnknownSession(_)) => Ok(ShutdownSessionClassification::Missing),
        Err(error) => Err(error),
    }
}

pub(crate) fn shutdown_error_is_already_gone(error: &CoreDaemonError) -> bool {
    matches!(error, CoreDaemonError::UnknownSession(_))
}

/// Response for a failed Core shutdown given the session's exact class.
pub(crate) fn shutdown_error_response(
    classification: ShutdownSessionClassification,
    error: &CoreDaemonError,
    session_id: &str,
    request_id: &str,
) -> DaemonResponse {
    match classification {
        ShutdownSessionClassification::Cleanup(cleanup) => daemon_session_cleanup(cleanup),
        ShutdownSessionClassification::Missing => daemon_unknown_session_cleanup(session_id),
        ShutdownSessionClassification::Stopping => daemon_session_cleanup(DaemonSessionCleanup {
            session_id: session_id.to_string(),
            outcome: "already_exited".to_string(),
        }),
        ShutdownSessionClassification::Active if shutdown_error_is_already_gone(error) => {
            daemon_session_cleanup(DaemonSessionCleanup {
                session_id: session_id.to_string(),
                outcome: "already_exited".to_string(),
            })
        }
        ShutdownSessionClassification::Active => {
            crate::daemon::control::sessions::core_operator_error(
                "shutdown_session",
                request_id,
                error,
            )
        }
    }
}

pub(crate) fn classify_found_session_lifecycle(
    session_id: &str,
    record: &botster_core_daemon::SessionLifecycleRecord,
) -> ShutdownSessionClassification {
    let complete_lifecycle = matches!(
        record.lifecycle,
        Some(SessionLifecycleState::Exited { .. }) | Some(SessionLifecycleState::Failed { .. })
    );
    let complete_registry = matches!(
        record.session.registry_state,
        RegistrySessionState::Exited | RegistrySessionState::Stale
    );
    let stopping = matches!(record.lifecycle, Some(SessionLifecycleState::Stopping))
        || matches!(
            record.session.registry_state,
            RegistrySessionState::Stopping
        );
    if complete_lifecycle || complete_registry {
        ShutdownSessionClassification::Cleanup(DaemonSessionCleanup {
            session_id: session_id.to_string(),
            outcome: if matches!(record.session.registry_state, RegistrySessionState::Stale)
                || matches!(record.lifecycle, Some(SessionLifecycleState::Failed { .. }))
            {
                "stale_session".to_string()
            } else {
                "already_exited".to_string()
            },
        })
    } else if stopping {
        ShutdownSessionClassification::Stopping
    } else {
        ShutdownSessionClassification::Active
    }
}
