//! Session-type request family.

use std::collections::BTreeMap;
use std::path::Path;

use botster_core::SessionId;
use botster_hub_client::DaemonRequest;
use serde_json::Value;

use crate::HubDaemon;
use crate::client_api::HubClientApi;
use crate::client_api_dto::response::daemon_spawned;
use crate::client_api_dto::session::{
    daemon_session_from_client, daemon_session_type_from_client, session_type_request_from_daemon,
};
use crate::daemon::control::messaging::defer_client_step;
use crate::daemon::control::pending::ControlStep;
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::DaemonControlState;
use crate::{HubClientRequest, HubClientResponseBody};

pub(crate) enum SessionTypeCatalogBuild {
    Ready {
        entities: BTreeMap<String, Value>,
        logical_bytes: usize,
    },
    TooLarge,
}

/// Build the session-type entity catalog from owned inputs.
///
/// Pure over its arguments so the owner can run it on a catalog worker
/// thread and apply the result on a later turn.
pub(crate) fn session_type_catalog_entities(
    packages: &crate::packages::PackageRegistry,
    state: &crate::persistence::HubState,
) -> DaemonTransportResult<BTreeMap<String, Value>> {
    let SessionTypeCatalogBuild::Ready { entities, .. } =
        build_session_type_catalog(packages, state, None)?
    else {
        unreachable!("an unbounded catalog build cannot exceed its limit");
    };
    Ok(entities)
}

/// Build a catalog without retaining more than `logical_byte_limit` bytes.
pub(crate) fn bounded_session_type_catalog_entities(
    packages: &crate::packages::PackageRegistry,
    state: &crate::persistence::HubState,
    logical_byte_limit: usize,
) -> DaemonTransportResult<SessionTypeCatalogBuild> {
    build_session_type_catalog(packages, state, Some(logical_byte_limit))
}

fn build_session_type_catalog(
    packages: &crate::packages::PackageRegistry,
    state: &crate::persistence::HubState,
    logical_byte_limit: Option<usize>,
) -> DaemonTransportResult<SessionTypeCatalogBuild> {
    let records = packages.packages();
    let session_types = if let Some(limit) = logical_byte_limit {
        let Some((rows, input_bytes)) = crate::session_types::list_session_types_bounded(
            &records, state, limit,
        )
        .map_err(|error| {
            DaemonTransportError::Client(crate::HubClientError::SessionType {
                request_id: request_id("daemon-session-types-list"),
                operation: crate::HubClientOperation::ListSessionTypes,
                kind: error.kind,
                message: error.message,
            })
        })?
        else {
            return Ok(SessionTypeCatalogBuild::TooLarge);
        };
        (rows, input_bytes)
    } else {
        let rows = crate::session_types::list_session_types(&records, state).map_err(|error| {
            DaemonTransportError::Client(crate::HubClientError::SessionType {
                request_id: request_id("daemon-session-types-list"),
                operation: crate::HubClientOperation::ListSessionTypes,
                kind: error.kind,
                message: error.message,
            })
        })?;
        (rows, 0)
    };
    let mut entities = BTreeMap::new();
    let mut logical_bytes = 0_usize;
    let mut operation_bytes = session_types.1;
    for session_type in session_types
        .0
        .into_iter()
        .map(daemon_session_type_from_client)
    {
        let id = session_type.session_type_id.clone();
        let value = serde_json::to_value(session_type).map_err(DaemonTransportError::Json)?;
        if !retain_catalog_entity(
            &mut entities,
            &mut logical_bytes,
            &mut operation_bytes,
            id,
            value,
            logical_byte_limit,
        )? {
            return Ok(SessionTypeCatalogBuild::TooLarge);
        }
    }
    Ok(SessionTypeCatalogBuild::Ready {
        entities,
        logical_bytes,
    })
}

fn retain_catalog_entity(
    entities: &mut BTreeMap<String, Value>,
    logical_bytes: &mut usize,
    operation_bytes: &mut usize,
    id: String,
    value: Value,
    logical_byte_limit: Option<usize>,
) -> DaemonTransportResult<bool> {
    if let Some(limit) = logical_byte_limit {
        let encoded_bytes = serde_json::to_vec(&value)
            .map_err(DaemonTransportError::Json)?
            .len();
        let Some(next_bytes) = logical_bytes
            .checked_add(id.len())
            .and_then(|bytes| bytes.checked_add(encoded_bytes))
        else {
            return Ok(false);
        };
        if next_bytes > limit {
            return Ok(false);
        }
        let Some(next_operation_bytes) = operation_bytes
            .checked_add(id.len())
            .and_then(|bytes| bytes.checked_add(encoded_bytes))
        else {
            return Ok(false);
        };
        if next_operation_bytes > limit {
            return Ok(false);
        }
        *logical_bytes = next_bytes;
        *operation_bytes = next_operation_bytes;
    }
    entities.insert(id, value);
    Ok(true)
}

#[cfg(test)]
mod catalog_tests {
    use super::*;

    #[test]
    fn bounded_catalog_construction_stops_before_retaining_an_oversized_row() {
        let mut entities = BTreeMap::new();
        let mut logical_bytes = 0;
        let mut operation_bytes = 0;
        assert!(
            retain_catalog_entity(
                &mut entities,
                &mut logical_bytes,
                &mut operation_bytes,
                "first".to_string(),
                serde_json::json!({ "value": "small" }),
                Some(64),
            )
            .expect("retain first row")
        );
        let retained_bytes = logical_bytes;
        assert!(
            !retain_catalog_entity(
                &mut entities,
                &mut logical_bytes,
                &mut operation_bytes,
                "second".to_string(),
                Value::String("x".repeat(64)),
                Some(64),
            )
            .expect("reject second row")
        );
        assert_eq!(entities.len(), 1);
        assert!(entities.contains_key("first"));
        assert_eq!(logical_bytes, retained_bytes);
        assert!(logical_bytes <= 64);
    }
}

pub(crate) fn is_invalid_repo_session_types_error(error: &DaemonTransportError) -> bool {
    matches!(
        error,
        DaemonTransportError::Client(crate::HubClientError::SessionType {
            kind: "invalid_repo_session_types",
            ..
        })
    )
}

pub(crate) fn ensure_repo_session_types_valid_for_enabled_root(
    root: &Path,
) -> DaemonTransportResult<()> {
    crate::session_types::validate_repo_session_types_at(root).map_err(|error| {
        DaemonTransportError::Client(crate::HubClientError::SessionType {
            request_id: request_id("daemon-session-types-list"),
            operation: crate::HubClientOperation::ListSessionTypes,
            kind: error.kind,
            message: error.message,
        })
    })
}

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    request: DaemonRequest,
) -> ControlStep {
    let api = HubClientApi::local_operator(
        observability
            .client_id
            .clone()
            .unwrap_or_else(|| super::runtime_client_id(&request)),
    );
    let packages = daemon.package_registry_view();
    let Some(runtime) = daemon.runtime_mut() else {
        return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
    };

    let DaemonRequest::SpawnSessionType {
        session_type_id,
        session_id,
        request,
    } = request
    else {
        unreachable!("session-type reads and mutations use the host executor")
    };
    let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
    let step = api.handle_request_for_owner(
        runtime,
        &packages,
        HubClientRequest::SpawnSessionType {
            request_id: request_id("daemon-session-types-spawn"),
            session_type_id,
            session_type_request: session_type_request_from_daemon(
                Some(SessionId(session_id)),
                request,
            ),
            now_seconds: now,
        },
        state.current_waiter_id.expect("owner waiter is assigned"),
    );
    defer_client_step(step, move |body| {
        let HubClientResponseBody::Spawned(spawned) = body else {
            return Err(DaemonTransportError::UnexpectedResponse);
        };
        Ok(daemon_spawned(
            daemon_session_from_client(spawned.session),
            super::events::events_from_client(spawned.events),
        ))
    })
}
