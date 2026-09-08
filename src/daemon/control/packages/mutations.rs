//! Typed package runtime effects executed by host workers.

use std::collections::BTreeMap;

use super::supervised_launch_contract;
use crate::daemon::control::request_id;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, PackageRollbackFailure};
use crate::entrypoint_supervisor::{EntrypointSupervisor, EntrypointSupervisorError};
use crate::host_mutations::PackageRuntimeEffect;
use crate::runtime::package_effect::HostPackageRuntime;
use crate::{HubConfig, PackageRegistry, PackageState};

fn load_package_after_enable(
    runtime: &mut HostPackageRuntime,
    registry: &PackageRegistry,
    package_name: &str,
) -> DaemonTransportResult<()> {
    if !has_lua(registry, package_name) {
        return Ok(());
    }
    let prepared = registry.prepare_local_package(
        package_name,
        "daemon socket load enabled local plugin package",
    )?;
    if prepared.selected_lua_entrypoint().is_some() {
        runtime
            .load_lua_plugin_package(registry, package_name)
            .map_err(crate::HubDaemonError::from)?;
    }
    Ok(())
}

fn reload_package(
    runtime: &mut HostPackageRuntime,
    registry: &PackageRegistry,
    package_name: &str,
    reason: &str,
) -> DaemonTransportResult<()> {
    let prepared = registry.prepare_local_package(package_name, reason)?;
    if prepared.selected_lua_entrypoint().is_some() {
        runtime
            .reload_lua_plugin_package(
                request_id(&format!("daemon-reload-{package_name}")),
                registry,
                package_name,
            )
            .map_err(crate::HubDaemonError::from)?;
    }
    Ok(())
}

fn has_lua(registry: &PackageRegistry, package_name: &str) -> bool {
    registry.package(package_name).is_some_and(|record| {
        record
            .manifest
            .entrypoints
            .iter()
            .any(|entrypoint| entrypoint.runtime == botster_core::ExtensionRuntime::Lua)
    })
}

fn restart_running_package_entrypoints(
    supervisor: &mut EntrypointSupervisor,
    config: &HubConfig,
    registry: &PackageRegistry,
    package_name: &str,
    entrypoint_ids: &[String],
) -> DaemonTransportResult<()> {
    for entrypoint_id in entrypoint_ids {
        let environment = supervisor.launch_environment(package_name, entrypoint_id);
        let launch = supervised_launch_contract(
            config,
            registry,
            package_name,
            entrypoint_id,
            &environment,
        )?;
        let snapshot = supervisor.restart(
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

/// Apply one complete runtime effect after the durable package commit.
pub(crate) fn apply_committed_runtime_effect(
    runtime: &mut HostPackageRuntime,
    supervisor: &mut EntrypointSupervisor,
    config: &HubConfig,
    registry: &PackageRegistry,
    effect: &PackageRuntimeEffect,
) -> DaemonTransportResult<()> {
    match effect {
        PackageRuntimeEffect::Enable { package_name, .. } => {
            load_package_after_enable(runtime, registry, package_name)
        }
        PackageRuntimeEffect::Disable { package_name }
        | PackageRuntimeEffect::Remove { package_name } => {
            supervisor.stop_package(package_name);
            let _ = runtime.unload_plugin_package(
                request_id(&format!("daemon-disable-{package_name}")),
                package_name,
            );
            runtime.record_event_plane_unload(package_name);
            Ok(())
        }
        PackageRuntimeEffect::Reload {
            package_name,
            reload_plugin,
            running_entrypoints,
            ..
        } => {
            if *reload_plugin {
                reload_package(
                    runtime,
                    registry,
                    package_name,
                    "daemon socket reload enabled local plugin package",
                )?;
            }
            restart_running_package_entrypoints(
                supervisor,
                config,
                registry,
                package_name,
                running_entrypoints,
            )
        }
        PackageRuntimeEffect::Refresh { packages, .. } => {
            for package in packages {
                if package.reload_plugin {
                    reload_package(
                        runtime,
                        registry,
                        &package.package_name,
                        "daemon socket reload enabled local plugin package",
                    )?;
                }
                restart_running_package_entrypoints(
                    supervisor,
                    config,
                    registry,
                    &package.package_name,
                    &package.restart_entrypoints,
                )?;
            }
            Ok(())
        }
    }
}

/// Restore runtime effects after the host restores durable package state.
pub(crate) fn restore_runtime_after_failed_effect(
    runtime: &mut HostPackageRuntime,
    supervisor: &mut EntrypointSupervisor,
    config: &HubConfig,
    effect: &PackageRuntimeEffect,
) -> Vec<PackageRollbackFailure> {
    let mut rollbacks = Vec::new();
    match effect {
        PackageRuntimeEffect::Enable { package_name, .. } => {
            let _ = runtime.unload_plugin_package(
                request_id(&format!("daemon-disable-{package_name}")),
                package_name,
            );
        }
        PackageRuntimeEffect::Reload {
            package_name,
            previous_packages,
            running_entrypoints,
            ..
        } => {
            restore_registry_runtime(
                runtime,
                supervisor,
                config,
                previous_packages,
                &BTreeMap::from([(package_name.clone(), running_entrypoints.clone())]),
                &mut rollbacks,
            );
        }
        PackageRuntimeEffect::Refresh {
            previous_packages,
            running_entrypoints,
            ..
        } => {
            restore_registry_runtime(
                runtime,
                supervisor,
                config,
                previous_packages,
                running_entrypoints,
                &mut rollbacks,
            );
        }
        PackageRuntimeEffect::Disable { .. } | PackageRuntimeEffect::Remove { .. } => {}
    }
    rollbacks
}

fn restore_registry_runtime(
    runtime: &mut HostPackageRuntime,
    supervisor: &mut EntrypointSupervisor,
    config: &HubConfig,
    previous: &PackageRegistry,
    running_entrypoints: &BTreeMap<String, Vec<String>>,
    rollbacks: &mut Vec<PackageRollbackFailure>,
) {
    for record in previous.packages() {
        let package_name = record.manifest.name.as_str();
        if record.state == PackageState::Enabled
            && has_lua(previous, package_name)
            && let Err(error) = reload_package(
                runtime,
                previous,
                package_name,
                "daemon socket restore plugin after failed mutation",
            )
        {
            rollbacks.push(PackageRollbackFailure {
                step: "plugin",
                package_name: Some(package_name.to_string()),
                error: Box::new(error),
            });
        }
        if let Some(entrypoint_ids) = running_entrypoints.get(package_name)
            && let Err(error) = restart_running_package_entrypoints(
                supervisor,
                config,
                previous,
                package_name,
                entrypoint_ids,
            )
        {
            rollbacks.push(PackageRollbackFailure {
                step: "entrypoint",
                package_name: Some(package_name.to_string()),
                error: Box::new(error),
            });
        }
    }
}
