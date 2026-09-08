//! Owner-only package runtime effects and compensation.

use std::collections::BTreeMap;

use super::supervised_launch_contract;
use crate::daemon::control::request_id;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, PackageRollbackFailure};
use crate::entrypoint_supervisor::EntrypointSupervisorError;
use crate::host_mutations::PackageRuntimeEffect;
use crate::{HubDaemon, PackageRegistry, PackageState};

fn load_package_after_enable(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let registry = daemon.package_registry().clone();
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
        "daemon socket load enabled local plugin package",
    )?;
    if prepared.selected_lua_entrypoint().is_some() {
        daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?
            .load_lua_plugin_package(&registry, package_name)
            .map_err(crate::HubDaemonError::from)?;
    }
    Ok(())
}

fn reload_package_after_reload(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<()> {
    let registry = daemon.package_registry().clone();
    let prepared = registry.prepare_local_package(
        package_name,
        "daemon socket reload enabled local plugin package",
    )?;
    if prepared.selected_lua_entrypoint().is_some() {
        daemon
            .runtime_mut()
            .ok_or(DaemonTransportError::DaemonNotRunning)?
            .reload_lua_plugin_package(
                request_id(&format!("daemon-reload-{package_name}")),
                &registry,
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

/// Apply the owner-only runtime phase after a package state commit.
pub(crate) fn apply_committed_runtime_effect(
    daemon: &mut HubDaemon,
    effect: &PackageRuntimeEffect,
) -> DaemonTransportResult<()> {
    match effect {
        PackageRuntimeEffect::Enable { package_name, .. } => {
            load_package_after_enable(daemon, package_name)
        }
        PackageRuntimeEffect::Disable { package_name }
        | PackageRuntimeEffect::Remove { package_name } => {
            daemon.entrypoint_supervisor().stop_package(package_name);
            unload_package_after_disable(daemon, package_name)?;
            record_event_plane_unload(daemon, package_name);
            Ok(())
        }
        PackageRuntimeEffect::Reload {
            package_name,
            reload_plugin,
            running_entrypoints,
            ..
        } => {
            if *reload_plugin {
                reload_package_after_reload(daemon, package_name)?;
            }
            let packages = daemon.package_registry().clone();
            restart_running_package_entrypoints(
                daemon,
                &packages,
                package_name,
                running_entrypoints,
            )
        }
        PackageRuntimeEffect::Refresh { packages, .. } => {
            for package in packages {
                if package.reload_plugin {
                    reload_package_after_reload(daemon, &package.package_name)?;
                }
                let registry = daemon.package_registry().clone();
                restart_running_package_entrypoints(
                    daemon,
                    &registry,
                    &package.package_name,
                    &package.restart_entrypoints,
                )?;
            }
            Ok(())
        }
    }
}

/// Restore owner-only runtime state after the host restored durable package state.
pub(crate) fn restore_runtime_after_failed_effect(
    daemon: &mut HubDaemon,
    effect: &PackageRuntimeEffect,
) -> Vec<PackageRollbackFailure> {
    let mut rollbacks = Vec::new();
    match effect {
        PackageRuntimeEffect::Enable { package_name, .. } => {
            if let Err(error) = unload_package_after_disable(daemon, package_name) {
                rollbacks.push(PackageRollbackFailure {
                    step: "plugin",
                    package_name: Some(package_name.clone()),
                    error: Box::new(error),
                });
            }
        }
        PackageRuntimeEffect::Reload {
            package_name,
            previous_packages,
            running_entrypoints,
            ..
        } => restore_registry_runtime(
            daemon,
            previous_packages,
            &BTreeMap::from([(package_name.clone(), running_entrypoints.clone())]),
            &mut rollbacks,
        ),
        PackageRuntimeEffect::Refresh {
            previous_packages,
            running_entrypoints,
            ..
        } => restore_registry_runtime(
            daemon,
            previous_packages,
            running_entrypoints,
            &mut rollbacks,
        ),
        PackageRuntimeEffect::Disable { .. } | PackageRuntimeEffect::Remove { .. } => {}
    }
    rollbacks
}

fn restore_registry_runtime(
    daemon: &mut HubDaemon,
    previous: &PackageRegistry,
    running_entrypoints: &BTreeMap<String, Vec<String>>,
    rollbacks: &mut Vec<PackageRollbackFailure>,
) {
    for record in previous.packages() {
        let package_name = record.manifest.name.as_str();
        if record.state == PackageState::Enabled
            && let Err(error) = restore_plugin_from_registry(daemon, previous, package_name)
        {
            rollbacks.push(PackageRollbackFailure {
                step: "plugin",
                package_name: Some(package_name.to_string()),
                error: Box::new(error),
            });
        }
        if let Some(entrypoint_ids) = running_entrypoints.get(package_name)
            && let Err(error) =
                restart_running_package_entrypoints(daemon, previous, package_name, entrypoint_ids)
        {
            rollbacks.push(PackageRollbackFailure {
                step: "entrypoint",
                package_name: Some(package_name.to_string()),
                error: Box::new(error),
            });
        }
    }
}
