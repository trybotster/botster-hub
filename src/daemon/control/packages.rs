//! Package, app, route, navigation, and entrypoint request family.

pub(crate) mod mutations;

use std::collections::BTreeMap;
use std::path::PathBuf;

use botster_hub_client::{DaemonRequest, DaemonResponse};

use crate::HubDaemon;
use crate::client_api_dto::response::daemon_packages;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::entrypoint_supervisor::EntrypointSupervisorError;
use crate::packages::{PackageResolvedEntrypointLaunch, resolve_entrypoint_launch_contract};
use crate::transport::unix::listener::socket_path;
use crate::{
    EntrypointProcessSnapshot, HubClientPackage, HubConfig, PackageAction, PackageAdmissionReason,
    PackageRegistry, PackageRegistryError, PackageState,
};

pub(crate) fn handle_request(
    daemon: &mut HubDaemon,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    match request {
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
