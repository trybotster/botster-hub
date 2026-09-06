//! Session-type request family.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use botster_core::SessionId;
use botster_hub_client::{DaemonRequest, DaemonResponse};
use serde_json::Value;

use crate::HubDaemon;
use crate::client_api::HubClientApi;
use crate::client_api_dto::response::{
    daemon_resolved_session_type, daemon_session_type_definition, daemon_session_types,
    daemon_spawned,
};
use crate::client_api_dto::session::{
    daemon_session_from_client, daemon_session_type_from_client,
    session_type_definition_from_daemon, session_type_mutation_source_from_daemon,
    session_type_request_from_daemon,
};
use crate::daemon::control::messaging::defer_client_step;
use crate::daemon::control::pending::ControlStep;
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::DaemonControlState;
use crate::persistence::{FileHubStateStore, HubStateStore};
use crate::{HubClientRequest, HubClientResponseBody};

/// Build the session-type entity catalog from owned inputs.
///
/// Pure over its arguments so the owner can run it on a catalog worker
/// thread and apply the result on a later turn.
pub(crate) fn session_type_catalog_entities(
    records: &[crate::packages::PackageRecord],
    state: &crate::persistence::HubState,
) -> DaemonTransportResult<BTreeMap<String, Value>> {
    let records = records.iter().collect::<Vec<_>>();
    let session_types =
        crate::session_types::list_session_types(&records, state).map_err(|error| {
            DaemonTransportError::Client(crate::HubClientError::SessionType {
                request_id: request_id("daemon-session-types-list"),
                operation: crate::HubClientOperation::ListSessionTypes,
                kind: error.kind,
                message: error.message,
            })
        })?;
    session_types
        .into_iter()
        .map(daemon_session_type_from_client)
        .map(|session_type| {
            let id = session_type.session_type_id.clone();
            serde_json::to_value(session_type)
                .map(|value| (id, value))
                .map_err(DaemonTransportError::Json)
        })
        .collect::<DaemonTransportResult<BTreeMap<_, _>>>()
}

/// Session-type definitions keyed by id, built on the owner thread for
/// request paths that need them at once.
pub(crate) fn session_type_definition_map(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<BTreeMap<String, Value>> {
    let packages = daemon.package_registry().clone();
    let records = packages.packages().into_iter().cloned().collect::<Vec<_>>();
    let runtime = daemon
        .runtime_mut()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let state = runtime.state().clone();
    session_type_catalog_entities(&records, &state)
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

pub(crate) fn ensure_update_would_not_enable_invalid_repo_session_types(
    daemon: &HubDaemon,
    target_id: &str,
    root: Option<&PathBuf>,
    enabled: Option<bool>,
) -> DaemonTransportResult<()> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let state = runtime.state();
    let Some(target) = state
        .spawn_targets
        .iter()
        .find(|target| target.target_id == target_id)
    else {
        // Let the later update path return not_found.
        return Ok(());
    };
    let resulting_enabled = enabled.unwrap_or(target.enabled);
    if !resulting_enabled {
        return Ok(());
    }
    let resulting_root = root.cloned().unwrap_or_else(|| target.root.clone());
    // Defer non-directory roots to update_spawn_target's root_not_directory.
    if !resulting_root.is_dir() {
        return Ok(());
    }
    ensure_repo_session_types_valid_for_enabled_root(&resulting_root)
}

pub(crate) fn advance_session_type_generation_if_changed(
    daemon: &mut HubDaemon,
    before: &BTreeMap<String, Value>,
) -> DaemonTransportResult<()> {
    if session_type_definition_map(daemon)? == *before {
        return Ok(());
    }
    force_advance_session_type_generation(daemon)
}

pub(crate) fn force_advance_session_type_generation(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<()> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let state = store.update(&config, |state| {
        state.session_type_generation = state.session_type_generation.saturating_add(1);
    })?;
    daemon.replace_state(state);
    Ok(())
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
    let packages = daemon.package_registry().clone();
    let Some(runtime) = daemon.runtime_mut() else {
        return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
    };

    if let DaemonRequest::SpawnSessionType {
        session_type_id,
        session_id,
        request,
    } = request
    {
        let now = crate::daemon::owner_loop::tick(&mut state.logical_clock);
        let step = api.handle_request(
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
        );
        return defer_client_step(step, move |body| {
            let HubClientResponseBody::Spawned(spawned) = body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_spawned(
                daemon_session_from_client(spawned.session),
                super::events::events_from_client(spawned.events),
            ))
        });
    }
    let result: DaemonTransportResult<DaemonResponse> = (|| match request {
        DaemonRequest::ListSessionTypes => {
            let response = api
                .handle_request(
                    runtime,
                    &packages,
                    HubClientRequest::ListSessionTypes {
                        request_id: request_id("daemon-session-types-list"),
                    },
                )
                .ready()?;
            let HubClientResponseBody::SessionTypes(templates) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(templates))
        }
        DaemonRequest::ListSessionTypesForTarget { target_id } => {
            let response = api
                .handle_request(
                    runtime,
                    &packages,
                    HubClientRequest::ListSessionTypesForTarget {
                        request_id: request_id("daemon-session-types-list-for-target"),
                        target_id,
                    },
                )
                .ready()?;
            let HubClientResponseBody::SessionTypes(templates) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(templates))
        }
        DaemonRequest::ShowSessionType { session_type_id } => {
            let response = api
                .handle_request(
                    runtime,
                    &packages,
                    HubClientRequest::ShowSessionType {
                        request_id: request_id("daemon-session-types-show"),
                        session_type_id,
                    },
                )
                .ready()?;
            let HubClientResponseBody::SessionTypes(templates) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(templates))
        }
        DaemonRequest::ShowSessionTypeDefinition { session_type_id } => {
            let response = api
                .handle_request(
                    runtime,
                    &packages,
                    HubClientRequest::ShowSessionTypeDefinition {
                        request_id: request_id("daemon-session-types-definition"),
                        session_type_id,
                    },
                )
                .ready()?;
            let HubClientResponseBody::SessionTypeDefinition(definition) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_type_definition(*definition))
        }
        DaemonRequest::CreateSessionType { source, definition } => {
            let response = api
                .handle_request(
                    runtime,
                    &packages,
                    HubClientRequest::CreateSessionType {
                        request_id: request_id("daemon-session-types-create"),
                        source: session_type_mutation_source_from_daemon(source),
                        definition: session_type_definition_from_daemon(definition),
                    },
                )
                .ready()?;
            let HubClientResponseBody::SessionTypes(session_types) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(session_types))
        }
        DaemonRequest::UpdateSessionType { source, definition } => {
            let response = api
                .handle_request(
                    runtime,
                    &packages,
                    HubClientRequest::UpdateSessionType {
                        request_id: request_id("daemon-session-types-update"),
                        source: session_type_mutation_source_from_daemon(source),
                        definition: session_type_definition_from_daemon(definition),
                    },
                )
                .ready()?;
            let HubClientResponseBody::SessionTypes(session_types) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(session_types))
        }
        DaemonRequest::DeleteSessionType {
            source,
            session_type_id,
        } => {
            let response = api
                .handle_request(
                    runtime,
                    &packages,
                    HubClientRequest::DeleteSessionType {
                        request_id: request_id("daemon-session-types-delete"),
                        source: session_type_mutation_source_from_daemon(source),
                        session_type_id,
                    },
                )
                .ready()?;
            let HubClientResponseBody::SessionTypes(session_types) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_session_types(session_types))
        }
        DaemonRequest::ResolveSessionType {
            session_type_id,
            request,
        } => {
            let response = api
                .handle_request(
                    runtime,
                    &packages,
                    HubClientRequest::ResolveSessionType {
                        request_id: request_id("daemon-session-types-resolve"),
                        session_type_id,
                        session_type_request: session_type_request_from_daemon(None, request),
                    },
                )
                .ready()?;
            let HubClientResponseBody::ResolvedSessionType(resolved) = response.body else {
                return Err(DaemonTransportError::UnexpectedResponse);
            };
            Ok(daemon_resolved_session_type(*resolved))
        }
        _ => unreachable!("session-type runtime family received a non-session-type request"),
    })();
    result.into()
}
