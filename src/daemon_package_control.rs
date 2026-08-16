//! Daemon package mutation owner.
//!
//! This module owns candidate registry mutation, persistence, runtime side
//! effects, session-type generation, and the mutation response. The transport
//! dispatcher keeps match arms as thin calls.

use std::collections::BTreeMap;
use std::path::PathBuf;

use botster_core::PackageConfigurationValue;
use botster_hub_client::DaemonPackagePin;

use super::{
    DaemonTransportError, DaemonTransportResult, HubDaemon, PackageRollbackFailure,
    advance_session_type_generation_if_changed, list_packages_response, package_decision_response,
    package_pin_from_daemon, package_update_status, request_id, session_type_definition_map,
    show_package_response, supervised_launch_contract,
};
use crate::entrypoint_supervisor::EntrypointSupervisorError;
use crate::persistence::{FileHubStateStore, HubStateStore};
use crate::{
    PackageAction, PackageAdmissionReason, PackageDecision, PackageRegistry, PackageRegistryError,
    PackageState,
};

pub(super) fn install_registry_package(
    daemon: &mut HubDaemon,
    registry_path: PathBuf,
    entry_id: String,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let before_session_types = session_type_definition_map(daemon)?;
    let mut candidate = daemon.package_registry().clone();
    let record = candidate.install_registry_entry(
        registry_path,
        &entry_id,
        "daemon socket install registry package",
    )?;
    let decision = PackageDecision {
        package_name: record.manifest.name.clone(),
        action: PackageAction::Install,
        state: record.state,
        classification: record.classification,
        admitted_host_profile: None,
        audit_reason: record.last_audit_reason.clone(),
    };
    commit_package_registry(daemon, candidate)?;
    advance_session_type_generation_if_changed(daemon, &before_session_types)?;
    package_decision_response(daemon, decision)
}

pub(super) fn install_local_package(
    daemon: &mut HubDaemon,
    path: PathBuf,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let before_session_types = session_type_definition_map(daemon)?;
    let mut candidate = daemon.package_registry().clone();
    let record = candidate.install_local_path(path, "daemon socket install local package")?;
    let decision = PackageDecision {
        package_name: record.manifest.name.clone(),
        action: PackageAction::Install,
        state: record.state,
        classification: record.classification,
        admitted_host_profile: None,
        audit_reason: record.last_audit_reason.clone(),
    };
    commit_package_registry(daemon, candidate)?;
    advance_session_type_generation_if_changed(daemon, &before_session_types)?;
    package_decision_response(daemon, decision)
}

pub(super) fn apply_package_update(
    daemon: &mut HubDaemon,
    package_name: String,
    pin: DaemonPackagePin,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let before_session_types = session_type_definition_map(daemon)?;
    let update_status = package_update_status(daemon, &package_name, Some(pin.clone()))?;
    let pin = package_pin_from_daemon(pin)?;
    let mut candidate = daemon.package_registry().clone();
    let record = candidate.pin(&package_name, pin, "daemon socket apply package update")?;
    let decision = PackageDecision {
        package_name: record.manifest.name.clone(),
        action: PackageAction::ApplyUpdate,
        state: record.state,
        classification: record.classification,
        admitted_host_profile: record.admitted_host_profile.clone(),
        audit_reason: record.last_audit_reason.clone(),
    };
    commit_package_registry(daemon, candidate)?;
    advance_session_type_generation_if_changed(daemon, &before_session_types)?;
    let mut response = package_decision_response(daemon, decision)?;
    response.update_status = Some(update_status);
    Ok(response)
}

pub(super) fn configure_package(
    daemon: &mut HubDaemon,
    package_name: String,
    values: BTreeMap<String, serde_json::Value>,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let values = values
        .into_iter()
        .map(|(key, value)| {
            serde_json::from_value::<PackageConfigurationValue>(value)
                .map(|value| (key.clone(), value))
                .map_err(|error| {
                    PackageRegistryError::without_record(
                        package_name.clone(),
                        PackageAction::Configure,
                        PackageAdmissionReason::InvalidConfiguration(vec![
                            crate::PackageConfigurationDiagnostic {
                                kind: "value_decode_error".to_string(),
                                field: Some(key),
                                message: format!(
                                    "configuration value is not a package configuration value: {error}"
                                ),
                            },
                        ]),
                        "daemon socket configure package".to_string(),
                    )
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let mut candidate = daemon.package_registry().clone();
    candidate.set_configuration(&package_name, values, "daemon socket configure package")?;
    commit_package_registry(daemon, candidate)?;
    show_package_response(daemon, &package_name)
}

pub(super) fn reload_package(
    daemon: &mut HubDaemon,
    package_name: String,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let before_session_types = session_type_definition_map(daemon)?;
    let previous = daemon.package_registry().clone();
    let running_entrypoints = daemon
        .entrypoint_supervisor()
        .snapshots()
        .into_iter()
        .filter(|snapshot| snapshot.package_name == package_name && snapshot.state == "running")
        .map(|snapshot| snapshot.entrypoint_id)
        .collect::<Vec<_>>();
    let (candidate, decision) =
        previous.refreshed_local_package(&package_name, "daemon socket reload local package")?;
    commit_package_registry(daemon, candidate)?;
    if let Err(error) =
        apply_reload_side_effects(daemon, &package_name, decision.state, &running_entrypoints)
    {
        return Err(compensate_runtime_after_failure(
            daemon,
            previous,
            error,
            &BTreeMap::from([(package_name, running_entrypoints)]),
        ));
    }
    advance_session_type_generation_if_changed(daemon, &before_session_types)?;
    package_decision_response(daemon, decision)
}

pub(super) fn refresh_local_packages(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let before_session_types = session_type_definition_map(daemon)?;
    let previous_packages = daemon.package_registry().clone();
    let running_entrypoints = daemon
        .entrypoint_supervisor()
        .snapshots()
        .into_iter()
        .filter(|snapshot| snapshot.state == "running")
        .fold(
            BTreeMap::<String, Vec<String>>::new(),
            |mut running, snapshot| {
                running
                    .entry(snapshot.package_name)
                    .or_default()
                    .push(snapshot.entrypoint_id);
                running
            },
        );
    let (candidate, decisions) = previous_packages
        .refreshed_local_packages("daemon socket refresh local package registrations")?;
    commit_package_registry(daemon, candidate)?;

    for decision in &decisions {
        if let Err(error) = apply_refresh_package_side_effects(
            daemon,
            &previous_packages,
            decision,
            &running_entrypoints,
        ) {
            return Err(compensate_runtime_after_failure(
                daemon,
                previous_packages,
                error,
                &running_entrypoints,
            ));
        }
    }

    let response = list_packages_response(daemon)?;
    advance_session_type_generation_if_changed(daemon, &before_session_types)?;
    Ok(response)
}

pub(super) fn enable_package_local_path(
    daemon: &mut HubDaemon,
    path: PathBuf,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let before_session_types = session_type_definition_map(daemon)?;
    let previous = daemon.package_registry().clone();
    let mut candidate = previous.clone();
    let package_name = candidate
        .install_local_path(path, "daemon socket enable local package")?
        .manifest
        .name
        .clone();
    let decision = candidate.enable(&package_name, "daemon socket enable local package")?;
    commit_enabled_package(
        daemon,
        previous,
        candidate,
        package_name,
        decision,
        before_session_types,
    )
}

pub(super) fn enable_package(
    daemon: &mut HubDaemon,
    package_name: String,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let before_session_types = session_type_definition_map(daemon)?;
    let previous = daemon.package_registry().clone();
    let mut candidate = previous.clone();
    let decision = candidate.enable(&package_name, "daemon socket enable package")?;
    commit_enabled_package(
        daemon,
        previous,
        candidate,
        package_name,
        decision,
        before_session_types,
    )
}

pub(super) fn disable_package(
    daemon: &mut HubDaemon,
    package_name: String,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let before_session_types = session_type_definition_map(daemon)?;
    let mut candidate = daemon.package_registry().clone();
    let decision = candidate.disable(&package_name, "daemon socket disable package")?;
    commit_package_registry(daemon, candidate)?;
    daemon.entrypoint_supervisor().stop_package(&package_name);
    unload_package_after_disable(daemon, &package_name)?;
    record_event_plane_unload(daemon, &package_name);
    advance_session_type_generation_if_changed(daemon, &before_session_types)?;
    package_decision_response(daemon, decision)
}

pub(super) fn remove_package(
    daemon: &mut HubDaemon,
    package_name: String,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    let before_session_types = session_type_definition_map(daemon)?;
    let mut candidate = daemon.package_registry().clone();
    let decision = candidate.remove(&package_name, "daemon socket remove package")?;
    commit_package_registry(daemon, candidate)?;
    daemon.entrypoint_supervisor().stop_package(&package_name);
    unload_package_after_disable(daemon, &package_name)?;
    record_event_plane_unload(daemon, &package_name);
    advance_session_type_generation_if_changed(daemon, &before_session_types)?;
    package_decision_response(daemon, decision)
}

fn commit_enabled_package(
    daemon: &mut HubDaemon,
    previous: PackageRegistry,
    candidate: PackageRegistry,
    package_name: String,
    decision: PackageDecision,
    before_session_types: BTreeMap<String, serde_json::Value>,
) -> DaemonTransportResult<botster_hub_client::DaemonResponse> {
    commit_package_registry(daemon, candidate)?;
    if let Err(error) = load_package_after_enable(daemon, &package_name) {
        return Err(compensate_enable_load_failure(
            daemon,
            previous,
            package_name,
            error,
        ));
    }
    advance_session_type_generation_if_changed(daemon, &before_session_types)?;
    package_decision_response(daemon, decision)
}

fn commit_package_registry(
    daemon: &mut HubDaemon,
    package_registry: PackageRegistry,
) -> DaemonTransportResult<()> {
    let runtime = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let snapshot = package_registry.snapshot();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let state = store.update(&config, |state| {
        state.package_registry = snapshot;
    })?;
    daemon.replace_package_registry(package_registry);
    daemon.replace_state(state);
    Ok(())
}

fn load_package_after_enable(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let package_registry = daemon.package_registry().clone();
    let has_lua = package_registry
        .package(package_name)
        .is_some_and(|record| {
            record
                .manifest
                .entrypoints
                .iter()
                .any(|entrypoint| entrypoint.runtime == botster_core::ExtensionRuntime::Lua)
        });
    if !has_lua {
        return Ok(());
    }
    let prepared = package_registry.prepare_local_package(
        package_name,
        "daemon socket load enabled local plugin package",
    )?;
    if prepared.selected_lua_entrypoint().is_some() {
        daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?
            .load_lua_plugin_package(&package_registry, package_name)
            .map_err(crate::HubDaemonError::from)?;
    }
    Ok(())
}

fn reload_package_after_reload(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let package_registry = daemon.package_registry().clone();
    let prepared = package_registry.prepare_local_package(
        package_name,
        "daemon socket reload enabled local plugin package",
    )?;
    if prepared.selected_lua_entrypoint().is_some() {
        daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?
            .reload_lua_plugin_package(
                request_id(&format!("daemon-reload-{package_name}")),
                &package_registry,
                package_name,
            )
            .map_err(crate::HubDaemonError::from)?;
    }
    Ok(())
}

fn record_event_plane_unload(daemon: &HubDaemon, package_name: &str) {
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    let generation = runtime
        .package_event_router()
        .current_package_generation(package_name)
        .unwrap_or(0);
    runtime.record_event_plane_owner_op(crate::package_event_router::OwnerOp {
        kind: crate::package_event_router::OwnerOpKind::Unload,
        owner: package_name.to_string(),
        generation,
    });
}

fn unload_package_after_disable(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let _ = daemon
        .runtime_mut()
        .ok_or(DaemonTransportError::DaemonNotRunning)?
        .unload_plugin_package(
            request_id(&format!("daemon-disable-{package_name}")),
            package_name,
        );
    Ok(())
}

fn restart_running_package_entrypoints(
    daemon: &mut HubDaemon,
    registry: &PackageRegistry,
    package_name: &str,
    entrypoint_ids: &[String],
) -> DaemonTransportResult<()> {
    if entrypoint_ids.is_empty() {
        return Ok(());
    }
    let config = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?
        .config()
        .clone();
    for entrypoint_id in entrypoint_ids {
        let environment = daemon
            .entrypoint_supervisor()
            .launch_environment(package_name, entrypoint_id);
        let launch = supervised_launch_contract(
            &config,
            registry,
            package_name,
            entrypoint_id,
            &environment,
        )?;
        let snapshot = daemon.entrypoint_supervisor().restart(
            registry,
            package_name,
            entrypoint_id,
            &launch.args,
            &launch.environment,
        )?;
        if snapshot.state != "running" {
            return Err(DaemonTransportError::Entrypoint(
                EntrypointSupervisorError::ReadinessFailed {
                    package_name: package_name.to_string(),
                    entrypoint_id: entrypoint_id.clone(),
                    details: format!("entrypoint state after restart is {}", snapshot.state),
                },
            ));
        }
    }
    Ok(())
}

fn compensate_enable_load_failure(
    daemon: &mut HubDaemon,
    previous: PackageRegistry,
    package_name: String,
    original: DaemonTransportError,
) -> DaemonTransportError {
    let mut rollbacks = Vec::new();
    if let Err(error) = unload_package_after_disable(daemon, &package_name) {
        rollbacks.push(PackageRollbackFailure {
            step: "plugin",
            package_name: Some(package_name),
            error: Box::new(error),
        });
    }
    if let Err(error) = commit_package_registry(daemon, previous) {
        rollbacks.push(PackageRollbackFailure {
            step: "persist",
            package_name: None,
            error: Box::new(error),
        });
    }
    finish_compensation(original, rollbacks)
}

fn compensate_runtime_after_failure(
    daemon: &mut HubDaemon,
    previous: PackageRegistry,
    original: DaemonTransportError,
    running_entrypoints: &BTreeMap<String, Vec<String>>,
) -> DaemonTransportError {
    let mut rollbacks = Vec::new();
    if let Err(error) = commit_package_registry(daemon, previous.clone()) {
        rollbacks.push(PackageRollbackFailure {
            step: "persist",
            package_name: None,
            error: Box::new(error),
        });
    }
    for record in previous.packages() {
        let package_name = record.manifest.name.as_str();
        if record.state == PackageState::Enabled
            && let Err(error) = restore_plugin_from_registry(daemon, &previous, package_name)
        {
            rollbacks.push(PackageRollbackFailure {
                step: "plugin",
                package_name: Some(package_name.to_string()),
                error: Box::new(error),
            });
        }
        if let Some(entrypoint_ids) = running_entrypoints.get(package_name)
            && let Err(error) =
                restart_running_package_entrypoints(daemon, &previous, package_name, entrypoint_ids)
        {
            rollbacks.push(PackageRollbackFailure {
                step: "entrypoint",
                package_name: Some(package_name.to_string()),
                error: Box::new(error),
            });
        }
    }
    finish_compensation(original, rollbacks)
}

fn restore_plugin_from_registry(
    daemon: &mut HubDaemon,
    registry: &PackageRegistry,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let has_lua = registry.package(package_name).is_some_and(|record| {
        record
            .manifest
            .entrypoints
            .iter()
            .any(|entrypoint| entrypoint.runtime == botster_core::ExtensionRuntime::Lua)
    });
    if !has_lua {
        return Ok(());
    }
    let prepared = registry.prepare_local_package(
        package_name,
        "daemon socket restore plugin after failed mutation",
    )?;
    if prepared.selected_lua_entrypoint().is_some() {
        daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?
            .reload_lua_plugin_package(
                request_id(&format!("daemon-restore-{package_name}")),
                registry,
                package_name,
            )
            .map_err(crate::HubDaemonError::from)?;
    }
    Ok(())
}

fn finish_compensation(
    original: DaemonTransportError,
    rollbacks: Vec<PackageRollbackFailure>,
) -> DaemonTransportError {
    if rollbacks.is_empty() {
        original
    } else {
        DaemonTransportError::PackageCompensation {
            original: Box::new(original),
            rollbacks,
        }
    }
}

fn apply_refresh_package_side_effects(
    daemon: &mut HubDaemon,
    previous_packages: &PackageRegistry,
    decision: &PackageDecision,
    running_entrypoints: &BTreeMap<String, Vec<String>>,
) -> DaemonTransportResult<()> {
    if decision.state == PackageState::Enabled {
        reload_package_after_reload(daemon, &decision.package_name)?;
    }
    if let Some(entrypoint_ids) = running_entrypoints.get(&decision.package_name) {
        let live = daemon.package_registry().clone();
        let changed_entrypoint_ids = entrypoint_ids
            .iter()
            .filter(|entrypoint_id| {
                runnable_entrypoint_definition_changed(
                    previous_packages,
                    &live,
                    &decision.package_name,
                    entrypoint_id,
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        restart_running_package_entrypoints(
            daemon,
            &live,
            &decision.package_name,
            &changed_entrypoint_ids,
        )?;
    }
    Ok(())
}

fn apply_reload_side_effects(
    daemon: &mut HubDaemon,
    package_name: &str,
    state: PackageState,
    running_entrypoints: &[String],
) -> DaemonTransportResult<()> {
    if state == PackageState::Enabled {
        reload_package_after_reload(daemon, package_name)?;
    }
    let live = daemon.package_registry().clone();
    restart_running_package_entrypoints(daemon, &live, package_name, running_entrypoints)
}

fn runnable_entrypoint_definition_changed(
    previous_packages: &PackageRegistry,
    refreshed_packages: &PackageRegistry,
    package_name: &str,
    entrypoint_id: &str,
) -> bool {
    let Some(previous) = previous_packages.package(package_name) else {
        return true;
    };
    let Some(refreshed) = refreshed_packages.package(package_name) else {
        return true;
    };
    let previous_entrypoint = previous
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id);
    let refreshed_entrypoint = refreshed
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id);

    previous.manifest != refreshed.manifest || previous_entrypoint != refreshed_entrypoint
}
