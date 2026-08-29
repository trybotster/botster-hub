//! Session-type request family.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::HubDaemon;
use crate::client_api_dto::session::daemon_session_type_from_client;
use crate::daemon::control::request_id;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::persistence::{FileHubStateStore, HubStateStore};

pub(crate) fn session_type_entity_snapshot(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<(u64, BTreeMap<String, Value>)> {
    let packages = daemon.package_registry().clone();
    let records = packages.packages();
    let runtime = daemon
        .runtime_mut()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let state = runtime.state();
    let generation = state.session_type_generation;
    let session_types =
        crate::session_types::list_session_types(&records, &state).map_err(|error| {
            DaemonTransportError::Client(crate::HubClientError::SessionType {
                request_id: request_id("daemon-session-types-list"),
                operation: crate::HubClientOperation::ListSessionTypes,
                kind: error.kind,
                message: error.message,
            })
        })?;
    let entities = session_types
        .into_iter()
        .map(daemon_session_type_from_client)
        .map(|session_type| {
            let id = session_type.session_type_id.clone();
            serde_json::to_value(session_type)
                .map(|value| (id, value))
                .map_err(DaemonTransportError::Json)
        })
        .collect::<DaemonTransportResult<BTreeMap<_, _>>>()?;
    Ok((generation, entities))
}

pub(crate) fn session_type_definition_map(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<BTreeMap<String, Value>> {
    session_type_entity_snapshot(daemon).map(|(_, entities)| entities)
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
