//! Package, app, route, navigation, and entrypoint request family.

pub(crate) mod mutations;

use std::collections::BTreeMap;
use std::path::PathBuf;

use botster_hub_client::{DaemonRequest, DaemonResponse};

use crate::client_api_dto::response::daemon_packages;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::entrypoint_supervisor::EntrypointSupervisor;
use crate::entrypoint_supervisor::EntrypointSupervisorError;
use crate::packages::{PackageResolvedEntrypointLaunch, resolve_entrypoint_launch_contract};
use crate::transport::unix::listener::socket_path;
use crate::{
    EntrypointProcessSnapshot, HubClientPackage, HubConfig, PackageAction, PackageAdmissionReason,
    PackageRegistry, PackageRegistryError, PackageState,
};

/// Run one entrypoint operation on a host worker.
pub(crate) fn handle_request(
    config: &HubConfig,
    registry: &PackageRegistry,
    supervisor: &mut EntrypointSupervisor,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let package_name = match request {
        DaemonRequest::StartPackageEntrypoint {
            package_name,
            entrypoint_id,
            environment_overrides,
        } => {
            let launch = supervised_launch_contract(
                config,
                registry,
                &package_name,
                &entrypoint_id,
                &environment_overrides,
            )?;
            supervisor.start(
                registry,
                &package_name,
                &entrypoint_id,
                &launch.args,
                &launch.environment,
            )?;
            package_name
        }
        DaemonRequest::StopPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            supervisor.stop(&package_name, &entrypoint_id);
            package_name
        }
        DaemonRequest::RestartPackageEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            let launch = supervised_launch_contract(
                config,
                registry,
                &package_name,
                &entrypoint_id,
                &BTreeMap::new(),
            )?;
            supervisor.restart(
                registry,
                &package_name,
                &entrypoint_id,
                &launch.args,
                &launch.environment,
            )?;
            package_name
        }
        DaemonRequest::PackageEntrypointStatus {
            package_name,
            entrypoint_id,
        } => {
            supervisor.status(&package_name, &entrypoint_id);
            package_name
        }
        _ => unreachable!("entrypoint execution received a different request family"),
    };
    show_package_response(registry, supervisor, &package_name)
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
    registry: &PackageRegistry,
    supervisor: &mut EntrypointSupervisor,
    package_name: &str,
) -> DaemonTransportResult<DaemonResponse> {
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
    apply_entrypoint_snapshots(std::slice::from_mut(&mut package), supervisor.snapshots());
    Ok(daemon_packages(vec![package]))
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
