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
            .map_err(|error| {
                log_plugin_load_failure("enable", package_name, &prepared, &error);
                plugin_load_error(package_name, error)
            })?;
    }
    Ok(())
}

/// Classify a plugin load failure. Both load paths finish every fallible step
/// (bundle, lifecycle preflight, event-plane stage) before they install the
/// plugin, so those errors leave the previous version running. Activation
/// follows the install, so its failures come after the swap.
fn plugin_load_error(
    package_name: &str,
    error: crate::HubLuaPluginLoadError,
) -> DaemonTransportError {
    use crate::HubLuaPluginLoadError as Load;
    match error {
        Load::Package(_)
        | Load::Lua(_)
        | Load::Lifecycle(_)
        | Load::EventPlane(_)
        | Load::EventPlaneStageOverlap
        | Load::EventPlaneUnfunded { .. } => DaemonTransportError::PluginNotSwapped {
            package_name: package_name.to_string(),
            error: Box::new(error),
        },
        Load::EventPlaneStranded(_)
        | Load::EventPlaneActivationFaulted
        | Load::EventPlaneCleanup
        | Load::EntityFamilyCleanup(_) => crate::HubDaemonError::from(error).into(),
    }
}

/// The package whose plugin load failed without replacing its running plugin.
fn unswapped_package(original: &DaemonTransportError) -> Option<&str> {
    match original {
        DaemonTransportError::PluginNotSwapped { package_name, .. } => Some(package_name),
        _ => None,
    }
}

/// Record a plugin load failure with its local context. Lua reports errors
/// against the package-relative entrypoint, so the absolute path is kept here.
fn log_plugin_load_failure(
    operation: &str,
    package_name: &str,
    prepared: &crate::PreparedLocalPackage,
    error: &crate::HubLuaPluginLoadError,
) {
    crate::hub_log::hub_log!(
        "package_plugin_load_failed operation={operation} package={package_name} package_root={} entrypoint={} code={} error={error}",
        prepared.package_root.display(),
        prepared
            .selected_entrypoint_path
            .as_deref()
            .map_or_else(|| "none".to_string(), |path| path.display().to_string()),
        error.code(),
    );
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
            .map_err(|error| {
                log_plugin_load_failure("reload", package_name, &prepared, &error);
                plugin_load_error(package_name, error)
            })?;
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
        // An explicit operator enable or reload resolves a stranded package:
        // once it succeeds the package starts clean.
        PackageRuntimeEffect::Enable { package_name, .. } => {
            load_package_after_enable(runtime, registry, package_name)?;
            runtime.clear_stranded(package_name);
            Ok(())
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
            )?;
            runtime.clear_stranded(package_name);
            Ok(())
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

/// After a failed compensation, unload every package the effect touched so no
/// handler of any generation serves it. A generation the restore itself staged
/// is aborted first. Each recorded router unload retires the package's queued
/// events; they are counted as stranded here, where the reason is known.
pub(crate) fn quarantine_after_failed_compensation(
    runtime: &mut HostPackageRuntime,
    supervisor: &mut EntrypointSupervisor,
    effect: &PackageRuntimeEffect,
) {
    if let Some(staged) = runtime.take_staged() {
        runtime.abort_staged(staged);
    }
    for package_name in effect.package_names() {
        supervisor.stop_package(package_name);
        runtime.record_package_stranded(package_name);
        let _ = runtime.unload_plugin_package(
            request_id(&format!("daemon-quarantine-{package_name}")),
            package_name,
        );
        runtime.record_event_plane_unload(package_name);
        crate::hub_log::hub_log!("package_quarantined package={package_name}");
    }
}

/// Restore runtime effects after the host restores durable package state.
///
/// A package whose plugin load failed without a swap is left alone: its
/// plugin never changed, and its source on disk is the version that failed,
/// so reloading it could only fail again or replace the version still running.
pub(crate) fn restore_runtime_after_failed_effect(
    runtime: &mut HostPackageRuntime,
    supervisor: &mut EntrypointSupervisor,
    config: &HubConfig,
    effect: &PackageRuntimeEffect,
    original: &DaemonTransportError,
) -> Vec<PackageRollbackFailure> {
    let unswapped = unswapped_package(original);
    let mut rollbacks = Vec::new();
    match effect {
        PackageRuntimeEffect::Enable { package_name, .. } => {
            if unswapped != Some(package_name.as_str()) {
                let _ = runtime.unload_plugin_package(
                    request_id(&format!("daemon-disable-{package_name}")),
                    package_name,
                );
            }
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
                &effect.package_names(),
                unswapped,
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
                &effect.package_names(),
                unswapped,
                &mut rollbacks,
            );
        }
        PackageRuntimeEffect::Disable { .. } | PackageRuntimeEffect::Remove { .. } => {}
    }
    rollbacks
}

#[allow(clippy::too_many_arguments)]
fn restore_registry_runtime(
    runtime: &mut HostPackageRuntime,
    supervisor: &mut EntrypointSupervisor,
    config: &HubConfig,
    previous: &PackageRegistry,
    running_entrypoints: &BTreeMap<String, Vec<String>>,
    targets: &[&str],
    unswapped: Option<&str>,
    rollbacks: &mut Vec<PackageRollbackFailure>,
) {
    for record in previous.packages() {
        let package_name = record.manifest.name.as_str();
        // Restore only the packages this effect changed; every other
        // package, stranded ones included, keeps its current runtime.
        if !targets.contains(&package_name) {
            continue;
        }
        // The failed load precedes this package's entrypoint restarts, so
        // neither its plugin nor its entrypoints changed.
        if unswapped == Some(package_name) {
            continue;
        }
        // A stranded package's previous runtime is its quarantine. This
        // effect may have loaded it; return it to quarantine, never reload
        // it from disk. Only a successful explicit enable or reload resolves it.
        if runtime.is_stranded(package_name) {
            supervisor.stop_package(package_name);
            let _ = runtime.unload_plugin_package(
                request_id(&format!("daemon-requarantine-{package_name}")),
                package_name,
            );
            runtime.record_event_plane_unload(package_name);
            continue;
        }
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
