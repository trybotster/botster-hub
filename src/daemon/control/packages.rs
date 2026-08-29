//! Package, app, route, navigation, and entrypoint request family.

pub(crate) mod mutations;

use std::collections::BTreeMap;
use std::path::PathBuf;

use botster_core::{PackageSource, RunnableEntrypointKind, RunnableEntrypointLaunchMode};
use botster_hub_client::{
    DaemonAvailablePackage, DaemonCapability, DaemonPackageCompatibility, DaemonPackageDiagnostic,
    DaemonPackageInstallEffect, DaemonPackageInstallPlan, DaemonPackagePin,
    DaemonPackageUpdateStatus, DaemonRequest, DaemonResolvedAppLaunch, DaemonResponse,
    DaemonResponseKind,
};

use crate::HubDaemon;
use crate::client_api::HubClientApi;
use crate::client_api_dto::package::{
    daemon_package_decision_from_policy, daemon_package_pin_from_policy,
    package_classification_label, package_compatibility_label, registry_source_kind_label,
    update_status_actions,
};
use crate::client_api_dto::response::{
    daemon_apps, daemon_available_packages, daemon_package_install_plan, daemon_package_navigation,
    daemon_package_update_status, daemon_packages, daemon_resolved_app_launch,
    daemon_resolved_package_route,
};
use crate::daemon::control::request_id;
use crate::daemon::error::{
    DaemonTransportError, DaemonTransportResult, daemon_app_launch_error,
    daemon_package_route_error,
};
use crate::daemon_projection::{
    apps_from_registry, package_route_descriptors, package_state_label,
    runnable_entrypoint_kind_label, runnable_launch_mode_label,
};
use crate::entrypoint_supervisor::EntrypointSupervisorError;
use crate::packages::{PackageResolvedEntrypointLaunch, resolve_entrypoint_launch_contract};
use crate::transport::unix::listener::socket_path;
use crate::{
    EntrypointProcessSnapshot, HubClientPackage, HubClientRequest, HubClientResponseBody,
    HubConfig, PackageAction, PackageAdmissionReason, PackageDecision, PackageRegistry,
    PackageRegistryError, PackageState, resolve_foreground_launch_contract,
};

pub(crate) fn handle_request(
    daemon: &mut HubDaemon,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::ListApps => list_apps_response(daemon),
        DaemonRequest::ResolveAppLaunch {
            package_name,
            entrypoint_id,
        } => resolve_app_launch_response(daemon, &package_name, &entrypoint_id),
        DaemonRequest::ResolvePackageRoute {
            package_name,
            route_id,
        } => resolve_package_route_response(daemon, &package_name, &route_id),
        DaemonRequest::ListPackageNavigation => list_package_navigation_response(daemon),
        DaemonRequest::ListPackages => list_packages_response(daemon),
        DaemonRequest::ListAvailablePackages { registry_path } => {
            available_packages_response(daemon, registry_path)
        }
        DaemonRequest::InspectAvailablePackage {
            registry_path,
            entry_id,
        } => inspect_available_package_response(daemon, registry_path, &entry_id),
        DaemonRequest::PreviewPackageInstall {
            registry_path,
            entry_id,
        } => preview_package_install_response(daemon, registry_path, &entry_id),
        DaemonRequest::InstallPackageRegistryEntry {
            registry_path,
            entry_id,
        } => mutations::install_registry_package(daemon, registry_path, entry_id),
        DaemonRequest::InstallPackageLocalPath { path } => {
            mutations::install_local_package(daemon, path)
        }
        DaemonRequest::CheckPackageUpdate { package_name } => {
            check_package_update_response(daemon, &package_name)
        }
        DaemonRequest::PreviewPackageUpdate { package_name, pin } => {
            preview_package_update_response(daemon, &package_name, pin)
        }
        DaemonRequest::ApplyPackageUpdate { package_name, pin } => {
            mutations::apply_package_update(daemon, package_name, pin)
        }
        DaemonRequest::ShowPackage { package_name } => show_package_response(daemon, &package_name),
        DaemonRequest::SetPackageConfiguration {
            package_name,
            values,
        } => mutations::configure_package(daemon, package_name, values),
        DaemonRequest::ReloadPackage { package_name } => {
            mutations::reload_package(daemon, package_name)
        }
        DaemonRequest::RefreshLocalPackages => mutations::refresh_local_packages(daemon),
        DaemonRequest::EnablePackageLocalPath { path } => {
            mutations::enable_package_local_path(daemon, path)
        }
        DaemonRequest::EnablePackage { package_name } => {
            mutations::enable_package(daemon, package_name)
        }
        DaemonRequest::DisablePackage { package_name } => {
            mutations::disable_package(daemon, package_name)
        }
        DaemonRequest::RemovePackage { package_name } => {
            mutations::remove_package(daemon, package_name)
        }
        DaemonRequest::StartPackageEntrypoint {
            package_name,
            entrypoint_id,
            environment_overrides,
        } => start_package_entrypoint_response(
            daemon,
            package_name,
            entrypoint_id,
            environment_overrides,
        ),
        DaemonRequest::StopPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => stop_package_entrypoint_response(daemon, package_name, entrypoint_id),
        DaemonRequest::RestartPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => restart_package_entrypoint_response(daemon, package_name, entrypoint_id),
        DaemonRequest::PackageEntrypointStatus {
            package_name,
            entrypoint_id,
        } => package_entrypoint_status_response(daemon, package_name, entrypoint_id),
        _ => unreachable!("package family received a non-package request"),
    }
}

fn list_packages_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };
    let response = api.handle_request(
        runtime,
        &packages,
        HubClientRequest::ListPackages {
            request_id: request_id("daemon-packages-list"),
        },
    )?;
    let HubClientResponseBody::Packages(mut packages) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    let snapshots = daemon.entrypoint_supervisor().snapshots();
    apply_entrypoint_snapshots(&mut packages, snapshots);
    Ok(daemon_packages(packages))
}

fn list_package_navigation_response(
    daemon: &mut HubDaemon,
) -> DaemonTransportResult<DaemonResponse> {
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };
    let response = api.handle_request(
        runtime,
        &packages,
        HubClientRequest::ListPackageNavigation {
            request_id: request_id("daemon-package-navigation-list"),
        },
    )?;
    let HubClientResponseBody::PackageNavigation(navigation) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    let packages = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(&packages, record))
        .collect::<Vec<_>>();
    Ok(daemon_package_navigation(navigation, &packages))
}

fn list_apps_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let registry = daemon.package_registry().clone();
    let snapshots = daemon.entrypoint_supervisor().snapshots();
    Ok(daemon_apps(apps_from_registry(&registry, snapshots)))
}

fn resolve_package_route_response(
    daemon: &mut HubDaemon,
    package_name: &str,
    route_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let registry = daemon.package_registry();
    let Some(record) = registry.package(package_name) else {
        return Ok(daemon_package_route_error(
            package_name,
            route_id,
            "package_not_installed",
            "package is not installed",
        ));
    };
    let package = HubClientPackage::from_record(registry, record);
    let route = package_route_descriptors(&package)
        .into_iter()
        .find(|route| route.route_id == route_id);
    match route {
        Some(route) => Ok(daemon_resolved_package_route(route)),
        None => Ok(daemon_package_route_error(
            package_name,
            route_id,
            "route_not_found",
            "package route is not declared",
        )),
    }
}

fn supervised_launch_contract(
    config: &HubConfig,
    registry: &PackageRegistry,
    package_name: &str,
    entrypoint_id: &str,
    environment_overrides: &BTreeMap<String, String>,
) -> DaemonTransportResult<PackageResolvedEntrypointLaunch> {
    let socket = runtime_path(socket_path(config)?);
    let record = registry.package(package_name).ok_or_else(|| {
        DaemonTransportError::Entrypoint(EntrypointSupervisorError::PackageNotInstalled(
            package_name.to_string(),
        ))
    })?;
    if !matches!(record.state, PackageState::Enabled) {
        return Err(DaemonTransportError::Entrypoint(
            EntrypointSupervisorError::PackageDisabled(package_name.to_string()),
        ));
    }
    let Some(entrypoint) = record
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id)
    else {
        return Err(DaemonTransportError::Entrypoint(
            EntrypointSupervisorError::EntrypointNotFound {
                package_name: package_name.to_string(),
                entrypoint_id: entrypoint_id.to_string(),
            },
        ));
    };

    resolve_entrypoint_launch_contract(
        entrypoint,
        &runtime_path(config.data_directory.clone()),
        &socket,
        environment_overrides,
    )
    .map_err(|details| {
        DaemonTransportError::Entrypoint(EntrypointSupervisorError::LaunchContract {
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
            details,
        })
    })
}

fn runtime_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn resolve_app_launch_response(
    daemon: &mut HubDaemon,
    package_name: &str,
    entrypoint_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let config = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?
        .config()
        .clone();
    let data_directory = runtime_path(config.data_directory.clone());
    let socket = runtime_path(socket_path(&config)?);
    let registry = daemon.package_registry();
    let Some(record) = registry.package(package_name) else {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "package_not_installed",
            "package is not installed",
        ));
    };
    if !record.is_enabled() {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "package_not_enabled",
            "package is not enabled",
        ));
    }
    let Some(entrypoint) = record
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id)
    else {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "entrypoint_not_found",
            "entrypoint is not installed for package",
        ));
    };
    if !matches!(entrypoint.kind, RunnableEntrypointKind::TerminalApp) {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "unsupported_app_kind",
            "app is not a terminal_app",
        ));
    }
    if !matches!(
        entrypoint.launch_mode,
        RunnableEntrypointLaunchMode::ForegroundStdio
    ) {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "unsupported_launch_mode",
            "terminal app must use foreground_stdio launch mode",
        ));
    }
    let launch =
        match resolve_foreground_launch_contract(record, entrypoint, &data_directory, &socket) {
            Ok(launch) => launch,
            Err(message) => {
                return Ok(daemon_app_launch_error(
                    package_name,
                    entrypoint_id,
                    "launch_contract_unavailable",
                    message,
                ));
            }
        };

    Ok(daemon_resolved_app_launch(DaemonResolvedAppLaunch {
        package_name: record.manifest.name.clone(),
        app_id: entrypoint.id.clone(),
        entrypoint_id: entrypoint.id.clone(),
        kind: runnable_entrypoint_kind_label(&entrypoint.kind).to_string(),
        launch_mode: runnable_launch_mode_label(&entrypoint.launch_mode).to_string(),
        command: launch.command,
        args: launch.args,
        working_directory: launch.working_directory.display().to_string(),
        environment: launch.environment,
    }))
}

fn available_packages_response(
    daemon: &mut HubDaemon,
    registry_path: PathBuf,
) -> DaemonTransportResult<DaemonResponse> {
    let available = daemon
        .package_registry()
        .available_packages(&registry_path)?;
    Ok(daemon_available_packages(available, &registry_path))
}

fn inspect_available_package_response(
    daemon: &mut HubDaemon,
    registry_path: PathBuf,
    entry_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let available = daemon
        .package_registry()
        .inspect_available_package(&registry_path, entry_id)?;
    Ok(daemon_available_packages(vec![available], &registry_path))
}

fn preview_package_install_response(
    daemon: &mut HubDaemon,
    registry_path: PathBuf,
    entry_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let plan = daemon
        .package_registry()
        .preview_registry_install(registry_path, entry_id)?;
    Ok(daemon_package_install_plan(plan))
}

fn show_package_response(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let registry = daemon.package_registry();
    let mut package = registry
        .package(package_name)
        .map(|record| HubClientPackage::from_record(registry, record))
        .ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                PackageAction::Show,
                PackageAdmissionReason::PackageNotInstalled,
                "daemon socket show package".to_string(),
            )
        })?;
    let snapshots = daemon.entrypoint_supervisor().snapshots();
    apply_entrypoint_snapshots(std::slice::from_mut(&mut package), snapshots);
    Ok(daemon_packages(vec![package]))
}

fn start_package_entrypoint_response(
    daemon: &mut HubDaemon,
    package_name: String,
    entrypoint_id: String,
    environment_overrides: BTreeMap<String, String>,
) -> DaemonTransportResult<DaemonResponse> {
    let config = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?
        .config()
        .clone();
    let packages = daemon.package_registry().clone();
    let launch = supervised_launch_contract(
        &config,
        &packages,
        &package_name,
        &entrypoint_id,
        &environment_overrides,
    )?;
    daemon.entrypoint_supervisor().start(
        &packages,
        &package_name,
        &entrypoint_id,
        &launch.args,
        &launch.environment,
    )?;
    show_package_response(daemon, &package_name)
}

fn stop_package_entrypoint_response(
    daemon: &mut HubDaemon,
    package_name: String,
    entrypoint_id: String,
) -> DaemonTransportResult<DaemonResponse> {
    daemon
        .entrypoint_supervisor()
        .stop(&package_name, &entrypoint_id);
    show_package_response(daemon, &package_name)
}

fn restart_package_entrypoint_response(
    daemon: &mut HubDaemon,
    package_name: String,
    entrypoint_id: String,
) -> DaemonTransportResult<DaemonResponse> {
    let config = daemon
        .runtime()
        .ok_or(DaemonTransportError::DaemonNotRunning)?
        .config()
        .clone();
    let packages = daemon.package_registry().clone();
    let launch = supervised_launch_contract(
        &config,
        &packages,
        &package_name,
        &entrypoint_id,
        &BTreeMap::new(),
    )?;
    daemon.entrypoint_supervisor().restart(
        &packages,
        &package_name,
        &entrypoint_id,
        &launch.args,
        &launch.environment,
    )?;
    show_package_response(daemon, &package_name)
}

fn package_entrypoint_status_response(
    daemon: &mut HubDaemon,
    package_name: String,
    entrypoint_id: String,
) -> DaemonTransportResult<DaemonResponse> {
    daemon
        .entrypoint_supervisor()
        .status(&package_name, &entrypoint_id);
    show_package_response(daemon, &package_name)
}

fn package_decision_response(
    daemon: &mut HubDaemon,
    decision: PackageDecision,
) -> DaemonTransportResult<DaemonResponse> {
    let mut response = list_packages_response(daemon)?;
    response.kind = DaemonResponseKind::PackageDecision;
    response.package_decision = Some(daemon_package_decision_from_policy(decision));
    Ok(response)
}

fn apply_entrypoint_snapshots(
    packages: &mut [HubClientPackage],
    snapshots: Vec<EntrypointProcessSnapshot>,
) {
    for snapshot in snapshots {
        let Some(package) = packages
            .iter_mut()
            .find(|package| package.package_name == snapshot.package_name)
        else {
            continue;
        };
        let Some(entrypoint) = package
            .runnable_entrypoints
            .iter_mut()
            .find(|entrypoint| entrypoint.id == snapshot.entrypoint_id)
        else {
            continue;
        };
        entrypoint.process.state = snapshot.state;
        entrypoint.process.pid = snapshot.pid;
        entrypoint.process.started_at = snapshot.started_at;
        entrypoint.process.exited_at = snapshot.exited_at;
        entrypoint.process.exit_status = snapshot.exit_status;
        entrypoint.process.diagnostics = snapshot
            .diagnostics
            .into_iter()
            .map(|diagnostic| crate::HubClientPackageDiagnostic {
                kind: diagnostic.kind,
                message: diagnostic.message,
            })
            .collect();
    }
}

fn check_package_update_response(
    daemon: &mut HubDaemon,
    package_name: &str,
) -> DaemonTransportResult<DaemonResponse> {
    let update_status = package_update_status(daemon, package_name, None)?;
    Ok(daemon_package_update_status(update_status))
}

fn preview_package_update_response(
    daemon: &mut HubDaemon,
    package_name: &str,
    pin: DaemonPackagePin,
) -> DaemonTransportResult<DaemonResponse> {
    let update_status = package_update_status(daemon, package_name, Some(pin.clone()))?;
    let mut response = daemon_package_update_status(update_status);
    response.install_plan = Some(package_update_plan(daemon, package_name, pin)?);
    Ok(response)
}

fn package_update_status(
    daemon: &mut HubDaemon,
    package_name: &str,
    proposed_pin: Option<DaemonPackagePin>,
) -> DaemonTransportResult<DaemonPackageUpdateStatus> {
    let record = daemon
        .package_registry()
        .package(package_name)
        .ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                PackageAction::CheckUpdate,
                PackageAdmissionReason::PackageNotInstalled,
                "daemon socket check package update".to_string(),
            )
        })?;
    let source_metadata_present = record.source_metadata.is_some();
    let local_path_source = matches!(record.manifest.source, Some(PackageSource::Path { .. }));
    let existing_pin = record.pin.clone();
    let enabled = package_state_label(record.state.into()) == "enabled";
    let live_entrypoint = daemon
        .entrypoint_supervisor()
        .snapshots()
        .into_iter()
        .any(|snapshot| snapshot.package_name == package_name && snapshot.state == "running");
    let pin = proposed_pin.or_else(|| existing_pin.map(daemon_package_pin_from_policy));
    let mut diagnostics = Vec::new();

    if !source_metadata_present {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "update_unavailable".to_string(),
            message:
                "update resolution is unavailable for packages without registry source metadata"
                    .to_string(),
        });
    }
    if pin.is_none() {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "pin_required".to_string(),
            message: "apply update requires explicit pinned source metadata".to_string(),
        });
    }
    if enabled && !local_path_source {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "reload_unavailable".to_string(),
            message: "enabled package changes require an operator disable/enable cycle".to_string(),
        });
    } else if enabled {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "reload_available".to_string(),
            message: "enabled local path package changes can be reloaded with reload_package"
                .to_string(),
        });
    }
    if live_entrypoint {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "restart_required".to_string(),
            message: "running package entrypoints must be restarted after update metadata changes"
                .to_string(),
        });
    }

    let has_pin = pin.is_some();
    let actions = update_status_actions(
        package_name,
        pin.as_ref(),
        has_pin,
        source_metadata_present,
        local_path_source,
    );
    Ok(DaemonPackageUpdateStatus {
        package_name: package_name.to_string(),
        update_available: has_pin && source_metadata_present,
        reload_required: enabled,
        restart_required: live_entrypoint,
        pin,
        diagnostics,
        actions,
    })
}

fn package_update_plan(
    daemon: &mut HubDaemon,
    package_name: &str,
    pin: DaemonPackagePin,
) -> DaemonTransportResult<DaemonPackageInstallPlan> {
    let diagnostics = package_update_status(daemon, package_name, Some(pin.clone()))?.diagnostics;
    let record = daemon
        .package_registry()
        .package(package_name)
        .ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                PackageAction::PreviewUpdate,
                PackageAdmissionReason::PackageNotInstalled,
                "daemon socket preview package update".to_string(),
            )
        })?;
    let source = record.source_metadata.as_ref();
    Ok(DaemonPackageInstallPlan {
        entry: DaemonAvailablePackage {
            entry_id: source
                .map(|source| source.entry_id.clone())
                .unwrap_or_else(|| package_name.to_string()),
            package_name: record.manifest.name.clone(),
            version: record.manifest.version.clone(),
            classification: package_classification_label(record.classification.into()).to_string(),
            source_kind: source
                .map(|source| registry_source_kind_label(source.source_kind).to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            source_label: source
                .map(|source| source.source_label.clone())
                .unwrap_or_else(|| "installed package has no registry source metadata".to_string()),
            first_party: record.trust.first_party,
            state: package_state_label(record.state.into()).to_string(),
            requested_capabilities: record
                .manifest
                .capabilities
                .iter()
                .cloned()
                .map(|capability| DaemonCapability {
                    surface: format!("{:?}", capability.surface),
                    scope: capability.scope,
                })
                .collect(),
            compatibility: DaemonPackageCompatibility {
                botster_requirement: record.compatibility.botster_requirement.clone(),
                result: package_compatibility_label(record.compatibility.result).to_string(),
                diagnostics: record.compatibility.diagnostics.clone(),
            },
            pin: Some(pin),
            actions: Vec::new(),
        },
        effects: vec![DaemonPackageInstallEffect {
            kind: "update_pin_metadata".to_string(),
            message: "would update pinned source metadata without fetching, enabling, or starting entrypoints"
                .to_string(),
        }],
        diagnostics,
        mutates_registry: false,
        starts_entrypoints: false,
    })
}
