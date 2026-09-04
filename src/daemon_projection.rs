//! Pure daemon DTO projection.
//!
//! Converts hub client, package, lifecycle, and error values into daemon
//! protocol DTOs. This module must not take `&mut HubDaemon`, persist state,
//! supervise entrypoints, or dispatch transport requests.

use std::collections::BTreeMap;
use std::path::PathBuf;

use botster_core::{
    RunnableEntrypointKind, RunnableEntrypointLaunchMode, RunnableEntrypointProcessState,
    RunnableEntrypointResultField,
};
use botster_hub_client::{
    DaemonApp, DaemonAppLaunchTarget, DaemonCapability, DaemonCompatibility, DaemonDiagnostic,
    DaemonInstallationIdentity, DaemonLifecycleCounters, DaemonOperatorError,
    DaemonPackageActionRequest, DaemonPackageActionRequiredReference, DaemonPackageActionState,
    DaemonPackageActionStatus, DaemonPackageDiagnostic, DaemonPackageNavigationEntry,
    DaemonPackageNavigationSource, DaemonPackagePin, DaemonPackageRouteDescriptor,
    DaemonPackageRouteTarget, DaemonSoftwareIdentity, DaemonStatus, FEATURE_PLUGIN_SURFACE_ACTION,
    FEATURE_PLUGIN_SURFACE_RENDER,
};
use botster_ui_contract::{PackageSurfaceDescriptor, PackageSurfaceKind};

use crate::entrypoint_supervisor::EntrypointProcessSnapshot;
use crate::{
    AvailablePackage, AvailablePackageState, HubClientPackage, HubClientPackageNavigationEntry,
    HubClientPackageNavigationTarget, HubDaemonStatus, HubStateLoadSource, PackageAction,
    PackageAdmissionReason, PackageCompatibilityResult, PackageRegistry,
};

pub(crate) fn apps_from_registry(
    registry: &PackageRegistry,
    snapshots: Vec<EntrypointProcessSnapshot>,
) -> Vec<DaemonApp> {
    let snapshots = snapshots
        .into_iter()
        .map(|snapshot| {
            (
                (
                    snapshot.package_name.clone(),
                    snapshot.entrypoint_id.clone(),
                ),
                snapshot,
            )
        })
        .collect::<BTreeMap<_, _>>();
    registry
        .packages()
        .into_iter()
        .flat_map(|record| apps_from_record(record, &snapshots))
        .collect()
}

fn apps_from_record(
    record: &crate::PackageRecord,
    snapshots: &BTreeMap<(String, String), EntrypointProcessSnapshot>,
) -> Vec<DaemonApp> {
    let package_state = package_state_label(record.state.into()).to_string();
    record
        .runnable_entrypoints
        .iter()
        .map(|entrypoint| {
            let snapshot = snapshots.get(&(record.manifest.name.clone(), entrypoint.id.clone()));
            let lifecycle_state = snapshot
                .and_then(|snapshot| snapshot.launch_result.as_ref())
                .map(|result| runnable_process_state_label(&result.process_state).to_string())
                .or_else(|| snapshot.map(|snapshot| snapshot.state.clone()))
                .unwrap_or_else(|| "not_started".to_string());
            let diagnostics: Vec<DaemonPackageDiagnostic> = snapshot
                .map(|snapshot| {
                    snapshot
                        .diagnostics
                        .iter()
                        .map(|diagnostic| DaemonPackageDiagnostic {
                            kind: diagnostic.kind.clone(),
                            message: diagnostic.message.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            let blocked_reasons = app_blocked_reasons(&package_state, entrypoint);
            let actions = app_entrypoint_actions(
                &record.manifest.name,
                &package_state,
                &entrypoint.id,
                entrypoint,
                &lifecycle_state,
            );
            DaemonApp {
                package_name: record.manifest.name.clone(),
                app_id: entrypoint.id.clone(),
                entrypoint_id: entrypoint.id.clone(),
                kind: runnable_entrypoint_kind_label(&entrypoint.kind).to_string(),
                launch_mode: runnable_launch_mode_label(&entrypoint.launch_mode).to_string(),
                lifecycle_state,
                diagnostics: diagnostics.clone(),
                actions,
                blocked_reasons: blocked_reasons.clone(),
                launch_target: DaemonAppLaunchTarget {
                    kind: runnable_entrypoint_kind_label(&entrypoint.kind).to_string(),
                    local_url: app_local_url(entrypoint, snapshot),
                },
                route: Some(app_entrypoint_route_descriptor(
                    record,
                    entrypoint,
                    &package_state,
                    blocked_reasons,
                    diagnostics.clone(),
                )),
            }
        })
        .collect()
}

fn app_blocked_reasons(
    package_state: &str,
    entrypoint: &crate::PackageRunnableEntrypoint,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if package_state != "enabled" {
        reasons.push("package_not_enabled".to_string());
    }
    if matches!(
        entrypoint.launch_mode,
        RunnableEntrypointLaunchMode::Background
    ) && !entrypoint.may_supervise
    {
        reasons.push("entrypoint_not_supervisable".to_string());
    }
    let supported = matches!(
        (&entrypoint.kind, &entrypoint.launch_mode),
        (
            RunnableEntrypointKind::WebApp,
            RunnableEntrypointLaunchMode::Background
        ) | (
            RunnableEntrypointKind::TerminalApp,
            RunnableEntrypointLaunchMode::ForegroundStdio
        )
    );
    if !supported {
        reasons.push("unsupported_launch_mode".to_string());
    }
    reasons
}

fn app_entrypoint_actions(
    package_name: &str,
    package_state: &str,
    entrypoint_id: &str,
    entrypoint: &crate::PackageRunnableEntrypoint,
    lifecycle_state: &str,
) -> Vec<DaemonPackageActionState> {
    if !matches!(
        entrypoint.launch_mode,
        RunnableEntrypointLaunchMode::Background
    ) {
        return Vec::new();
    }
    if !entrypoint.may_supervise {
        return vec![
            unavailable_action(
                "start_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint cannot be supervised by the hub",
            ),
            unavailable_action(
                "stop_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint cannot be supervised by the hub",
            ),
            unavailable_action(
                "restart_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint cannot be supervised by the hub",
            ),
        ];
    }
    if package_state != "enabled" {
        return vec![
            blocked_action(
                "start_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
            blocked_action(
                "stop_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
            blocked_action(
                "restart_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
        ];
    }
    let running = lifecycle_state == "running";
    let mut actions = Vec::new();
    if running {
        actions.push(unavailable_action(
            "start_package_entrypoint",
            "already_running",
            "entrypoint is already running",
        ));
        actions.push(available_package_action(
            "stop_package_entrypoint",
            request_for_entrypoint("stop_package_entrypoint", package_name, entrypoint_id),
        ));
    } else {
        actions.push(available_package_action(
            "start_package_entrypoint",
            request_for_entrypoint("start_package_entrypoint", package_name, entrypoint_id),
        ));
        actions.push(unavailable_action(
            "stop_package_entrypoint",
            "not_running",
            "entrypoint is not running",
        ));
    }
    actions.push(available_package_action(
        "restart_package_entrypoint",
        request_for_entrypoint("restart_package_entrypoint", package_name, entrypoint_id),
    ));
    actions
}

pub(crate) fn app_local_url(
    entrypoint: &crate::PackageRunnableEntrypoint,
    snapshot: Option<&EntrypointProcessSnapshot>,
) -> Option<String> {
    let declares_local_url = entrypoint.readiness.as_ref().is_some_and(|readiness| {
        readiness
            .result_fields
            .iter()
            .any(|field| matches!(field, RunnableEntrypointResultField::LocalUrl))
    });
    if !declares_local_url {
        return None;
    }
    snapshot
        .and_then(|snapshot| snapshot.launch_result.as_ref())
        .and_then(|result| result.local_url.clone())
}

pub(crate) fn package_route_descriptors(
    package: &HubClientPackage,
) -> Vec<DaemonPackageRouteDescriptor> {
    let package_state = package_state_label(package.state).to_string();
    let supports_settings = package.configuration.schema.is_some();
    let mut routes = package
        .surfaces
        .iter()
        .map(|surface| {
            plugin_surface_route_descriptor(
                &package.package_name,
                &package_state,
                &package.requested_capabilities,
                surface,
                supports_settings,
            )
        })
        .collect::<Vec<_>>();
    routes.extend(package.runnable_entrypoints.iter().map(|entrypoint| {
        client_entrypoint_route_descriptor(
            &package.package_name,
            &package_state,
            entrypoint,
            supports_settings,
        )
    }));
    if supports_settings {
        routes.push(settings_route_descriptor(
            &package.package_name,
            &package_state,
            &package.configuration,
        ));
    }
    routes
}

pub(crate) fn package_navigation_entries(
    navigation: Vec<HubClientPackageNavigationEntry>,
    packages: &[HubClientPackage],
) -> Vec<DaemonPackageNavigationEntry> {
    navigation
        .into_iter()
        .map(|entry| package_navigation_entry(entry, packages))
        .collect()
}

fn package_navigation_entry(
    entry: HubClientPackageNavigationEntry,
    packages: &[HubClientPackage],
) -> DaemonPackageNavigationEntry {
    let (route_id, source) = match &entry.target {
        HubClientPackageNavigationTarget::Surface { surface_id } => (
            surface_route_id(surface_id),
            DaemonPackageNavigationSource {
                kind: "surface".to_string(),
                surface_id: Some(surface_id.clone()),
                entrypoint_id: None,
            },
        ),
    };
    let route = packages
        .iter()
        .find(|package| package.package_name == entry.package_name)
        .and_then(|package| {
            package_route_descriptors(package)
                .into_iter()
                .find(|route| route.route_id == route_id)
        });

    match route {
        Some(route) => DaemonPackageNavigationEntry {
            package_name: entry.package_name,
            item_id: entry.item_id,
            label: entry.label,
            icon: entry.icon,
            description: entry.description,
            route_id: route.route_id,
            route_path: route.route_path,
            target: route.target,
            source,
            enabled: route.enabled,
            blocked: route.blocked,
            diagnostics: route.diagnostics,
        },
        None => DaemonPackageNavigationEntry {
            package_name: entry.package_name.clone(),
            item_id: entry.item_id,
            label: entry.label,
            icon: entry.icon,
            description: entry.description,
            route_id,
            route_path: String::new(),
            target: match entry.target {
                HubClientPackageNavigationTarget::Surface { surface_id } => {
                    DaemonPackageRouteTarget {
                        kind: "plugin_surface".to_string(),
                        entrypoint_id: None,
                        surface_id: Some(surface_id),
                    }
                }
            },
            source,
            enabled: false,
            blocked: true,
            diagnostics: vec![DaemonPackageDiagnostic {
                kind: "navigation_target_not_found".to_string(),
                message: "navigation target route is not declared".to_string(),
            }],
        },
    }
}

fn plugin_surface_route_descriptor(
    package_name: &str,
    package_state: &str,
    requested_capabilities: &[crate::HubClientCapability],
    surface: &PackageSurfaceDescriptor,
    supports_settings: bool,
) -> DaemonPackageRouteDescriptor {
    let diagnostics = route_state_diagnostics(package_state);
    DaemonPackageRouteDescriptor {
        package_name: package_name.to_string(),
        route_id: surface_route_id(&surface.id),
        route_path: surface_route_path(package_name, &surface.id),
        target: DaemonPackageRouteTarget {
            kind: "plugin_surface".to_string(),
            entrypoint_id: None,
            surface_id: Some(surface.id.clone()),
        },
        title: surface.title.clone(),
        label: surface.title.clone(),
        app_id: (surface.kind == PackageSurfaceKind::App).then(|| surface.id.clone()),
        surface_id: Some(surface.id.clone()),
        icon: surface.icon.clone(),
        category: surface.category.clone(),
        layout_mode: "plugin_surface".to_string(),
        required_capabilities: requested_capabilities
            .iter()
            .filter(|capability| capability.surface.eq_ignore_ascii_case("surfaces"))
            .map(daemon_capability_from_client)
            .collect(),
        enabled: package_state == "enabled",
        blocked: !diagnostics.is_empty(),
        diagnostics,
        supports_settings,
    }
}

fn client_entrypoint_route_descriptor(
    package_name: &str,
    package_state: &str,
    entrypoint: &crate::HubClientPackageRunnableEntrypoint,
    supports_settings: bool,
) -> DaemonPackageRouteDescriptor {
    let mut diagnostics = route_state_diagnostics(package_state);
    diagnostics.extend(
        client_app_blocked_reasons(package_state, entrypoint)
            .into_iter()
            .map(|reason| DaemonPackageDiagnostic {
                kind: reason,
                message: format!("{package_name}/{} cannot be opened", entrypoint.id),
            }),
    );
    DaemonPackageRouteDescriptor {
        package_name: package_name.to_string(),
        route_id: app_route_id(&entrypoint.id),
        route_path: app_route_path(package_name, &entrypoint.id),
        target: DaemonPackageRouteTarget {
            kind: "app_entrypoint".to_string(),
            entrypoint_id: Some(entrypoint.id.clone()),
            surface_id: None,
        },
        title: entrypoint.id.clone(),
        label: entrypoint.id.clone(),
        app_id: Some(entrypoint.id.clone()),
        surface_id: None,
        icon: None,
        category: Some("apps".to_string()),
        layout_mode: "app_entrypoint".to_string(),
        required_capabilities: entrypoint
            .capabilities
            .iter()
            .map(daemon_capability_from_client)
            .collect(),
        enabled: package_state == "enabled" && diagnostics.is_empty(),
        blocked: !diagnostics.is_empty(),
        diagnostics,
        supports_settings,
    }
}

fn client_app_blocked_reasons(
    package_state: &str,
    entrypoint: &crate::HubClientPackageRunnableEntrypoint,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if package_state != "enabled" {
        reasons.push("package_not_enabled".to_string());
    }
    if entrypoint.launch_mode == "background" && !entrypoint.may_supervise {
        reasons.push("entrypoint_not_supervisable".to_string());
    }
    let supported = (entrypoint.kind == "web_app" && entrypoint.launch_mode == "background")
        || (entrypoint.kind == "terminal_app" && entrypoint.launch_mode == "foreground_stdio");
    if !supported {
        reasons.push("unsupported_launch_mode".to_string());
    }
    reasons
}

fn app_entrypoint_route_descriptor(
    record: &crate::PackageRecord,
    entrypoint: &crate::PackageRunnableEntrypoint,
    package_state: &str,
    blocked_reasons: Vec<String>,
    mut diagnostics: Vec<DaemonPackageDiagnostic>,
) -> DaemonPackageRouteDescriptor {
    diagnostics.extend(
        blocked_reasons
            .iter()
            .map(|reason| DaemonPackageDiagnostic {
                kind: reason.clone(),
                message: format!("{} cannot be opened", entrypoint.id),
            }),
    );
    DaemonPackageRouteDescriptor {
        package_name: record.manifest.name.clone(),
        route_id: app_route_id(&entrypoint.id),
        route_path: app_route_path(&record.manifest.name, &entrypoint.id),
        target: DaemonPackageRouteTarget {
            kind: "app_entrypoint".to_string(),
            entrypoint_id: Some(entrypoint.id.clone()),
            surface_id: None,
        },
        title: entrypoint.id.clone(),
        label: entrypoint.id.clone(),
        app_id: Some(entrypoint.id.clone()),
        surface_id: None,
        icon: None,
        category: Some("apps".to_string()),
        layout_mode: "app_entrypoint".to_string(),
        required_capabilities: entrypoint
            .capabilities
            .iter()
            .map(|capability| DaemonCapability {
                surface: core_capability_surface_label(&capability.surface).to_string(),
                scope: capability.scope.clone(),
            })
            .collect(),
        enabled: package_state == "enabled" && diagnostics.is_empty(),
        blocked: !diagnostics.is_empty(),
        diagnostics,
        supports_settings: record.configuration_view().schema.is_some(),
    }
}

fn settings_route_descriptor(
    package_name: &str,
    package_state: &str,
    configuration: &crate::HubClientPackageConfiguration,
) -> DaemonPackageRouteDescriptor {
    let mut diagnostics = route_state_diagnostics(package_state);
    diagnostics.extend(configuration.diagnostics.iter().map(|diagnostic| {
        DaemonPackageDiagnostic {
            kind: diagnostic.kind.clone(),
            message: diagnostic.message.clone(),
        }
    }));
    for key in &configuration.missing_required {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "missing_required_configuration".to_string(),
            message: format!("configuration field {key} is required"),
        });
    }
    DaemonPackageRouteDescriptor {
        package_name: package_name.to_string(),
        route_id: "settings".to_string(),
        route_path: settings_route_path(package_name),
        target: DaemonPackageRouteTarget {
            kind: "package_settings".to_string(),
            entrypoint_id: None,
            surface_id: None,
        },
        title: "Settings".to_string(),
        label: "Settings".to_string(),
        app_id: None,
        surface_id: None,
        icon: Some("settings".to_string()),
        category: Some("settings".to_string()),
        layout_mode: "settings_form".to_string(),
        required_capabilities: Vec::new(),
        enabled: true,
        blocked: false,
        diagnostics,
        supports_settings: true,
    }
}

fn route_state_diagnostics(package_state: &str) -> Vec<DaemonPackageDiagnostic> {
    if package_state == "enabled" {
        Vec::new()
    } else {
        vec![DaemonPackageDiagnostic {
            kind: "package_not_enabled".to_string(),
            message: "package is not enabled".to_string(),
        }]
    }
}

fn daemon_capability_from_client(capability: &crate::HubClientCapability) -> DaemonCapability {
    DaemonCapability {
        surface: capability.surface.clone(),
        scope: capability.scope.clone(),
    }
}

fn core_capability_surface_label(surface: &botster_core::CapabilitySurface) -> &'static str {
    match surface {
        botster_core::CapabilitySurface::ClientAdmission => "ClientAdmission",
        botster_core::CapabilitySurface::PairingInvites => "PairingInvites",
        botster_core::CapabilitySurface::SignalingRelay => "SignalingRelay",
        botster_core::CapabilitySurface::HubPresence => "HubPresence",
        botster_core::CapabilitySurface::BrowserShell => "BrowserShell",
        botster_core::CapabilitySurface::Secrets => "Secrets",
        botster_core::CapabilitySurface::Crypto => "Crypto",
        botster_core::CapabilitySurface::Network => "Network",
        botster_core::CapabilitySurface::Surfaces => "Surfaces",
        botster_core::CapabilitySurface::SessionActions => "SessionActions",
        botster_core::CapabilitySurface::Mcp => "Mcp",
        botster_core::CapabilitySurface::PluginDb => "PluginDb",
        botster_core::CapabilitySurface::Filesystem => "Filesystem",
        botster_core::CapabilitySurface::Timers => "Timers",
    }
}

fn surface_route_id(surface_id: &str) -> String {
    format!("surface:{surface_id}")
}

fn app_route_id(entrypoint_id: &str) -> String {
    format!("app:{entrypoint_id}")
}

fn surface_route_path(package_name: &str, surface_id: &str) -> String {
    format!("/packages/{package_name}/surfaces/{surface_id}")
}

fn app_route_path(package_name: &str, entrypoint_id: &str) -> String {
    format!("/packages/{package_name}/apps/{entrypoint_id}")
}

fn settings_route_path(package_name: &str) -> String {
    format!("/packages/{package_name}/settings")
}

pub(crate) fn available_package_actions(
    package: &AvailablePackage,
    registry_path: Option<&PathBuf>,
) -> Vec<DaemonPackageActionState> {
    let mut actions = Vec::new();
    let compatible = matches!(
        package.compatibility.result,
        PackageCompatibilityResult::Compatible
    );
    let install_blocked = !matches!(package.state, AvailablePackageState::Available) || !compatible;
    if install_blocked {
        let reason = if compatible {
            "already_installed"
        } else {
            "botster_compatibility"
        };
        let diagnostics = package
            .compatibility
            .diagnostics
            .iter()
            .map(|message| DaemonPackageDiagnostic {
                kind: "botster_compatibility".to_string(),
                message: message.clone(),
            })
            .collect();
        actions.push(blocked_action(
            "install_package_registry_entry",
            reason,
            diagnostics,
            Vec::new(),
        ));
    } else if let Some(registry_path) = registry_path {
        actions.push(available_package_action(
            "install_package_registry_entry",
            Some(DaemonPackageActionRequest {
                request_type: "install_package_registry_entry".to_string(),
                pin: None,
                package_name: Some(package.package_name.clone()),
                entry_id: Some(package.entry_id.clone()),
                entrypoint_id: None,
                registry_path: Some(registry_path.to_string_lossy().to_string()),
            }),
        ));
    } else {
        actions.push(blocked_action(
            "install_package_registry_entry",
            "registry_path_required",
            vec![DaemonPackageDiagnostic {
                kind: "registry_path_required".to_string(),
                message:
                    "install request mapping requires the registry path used to list the package"
                        .to_string(),
            }],
            vec![DaemonPackageActionRequiredReference {
                kind: "registry".to_string(),
                key: "registry_path".to_string(),
            }],
        ));
    }

    for action_id in [
        "enable_package",
        "disable_package",
        "remove_package",
        "start_package_entrypoint",
        "stop_package_entrypoint",
        "restart_package_entrypoint",
        "check_package_update",
        "preview_package_update",
        "apply_package_update",
        "set_package_configuration",
    ] {
        actions.push(unavailable_action(
            action_id,
            "install_required",
            "install the package before running installed-package lifecycle actions",
        ));
    }
    actions.push(unavailable_action(
        "reload_package",
        "unsupported",
        "package reload is not supported by the hub daemon",
    ));
    actions.push(unavailable_action(
        "restart_hub",
        "unsupported",
        "hub restart is not exposed as a package lifecycle action",
    ));
    actions
}

pub(crate) fn available_package_action(
    action_id: &str,
    request: Option<DaemonPackageActionRequest>,
) -> DaemonPackageActionState {
    DaemonPackageActionState {
        action_id: action_id.to_string(),
        status: DaemonPackageActionStatus::Available,
        reason: None,
        diagnostics: Vec::new(),
        required_references: Vec::new(),
        request,
    }
}

pub(crate) fn blocked_action(
    action_id: &str,
    reason: &str,
    diagnostics: Vec<DaemonPackageDiagnostic>,
    required_references: Vec<DaemonPackageActionRequiredReference>,
) -> DaemonPackageActionState {
    DaemonPackageActionState {
        action_id: action_id.to_string(),
        status: DaemonPackageActionStatus::Blocked,
        reason: Some(reason.to_string()),
        diagnostics,
        required_references,
        request: None,
    }
}

pub(crate) fn unavailable_action(
    action_id: &str,
    reason: &str,
    message: &str,
) -> DaemonPackageActionState {
    DaemonPackageActionState {
        action_id: action_id.to_string(),
        status: DaemonPackageActionStatus::Unavailable,
        reason: Some(reason.to_string()),
        diagnostics: vec![DaemonPackageDiagnostic {
            kind: reason.to_string(),
            message: message.to_string(),
        }],
        required_references: Vec::new(),
        request: None,
    }
}

pub(crate) fn request_for_package(
    request_type: &str,
    package_name: &str,
) -> Option<DaemonPackageActionRequest> {
    Some(DaemonPackageActionRequest {
        request_type: request_type.to_string(),
        pin: None,
        package_name: Some(package_name.to_string()),
        entry_id: None,
        entrypoint_id: None,
        registry_path: None,
    })
}

pub(crate) fn request_for_package_with_pin(
    request_type: &str,
    package_name: &str,
    pin: Option<DaemonPackagePin>,
) -> Option<DaemonPackageActionRequest> {
    Some(DaemonPackageActionRequest {
        request_type: request_type.to_string(),
        pin,
        package_name: Some(package_name.to_string()),
        entry_id: None,
        entrypoint_id: None,
        registry_path: None,
    })
}

pub(crate) fn request_for_entrypoint(
    request_type: &str,
    package_name: &str,
    entrypoint_id: &str,
) -> Option<DaemonPackageActionRequest> {
    Some(DaemonPackageActionRequest {
        request_type: request_type.to_string(),
        pin: None,
        package_name: Some(package_name.to_string()),
        entry_id: None,
        entrypoint_id: Some(entrypoint_id.to_string()),
        registry_path: None,
    })
}

pub(crate) fn daemon_status_from_status(
    status: &HubDaemonStatus,
    session_count: usize,
    diagnostics: Vec<DaemonDiagnostic>,
    lifecycle_counters: DaemonLifecycleCounters,
    software: DaemonSoftwareIdentity,
    installation: DaemonInstallationIdentity,
    observability: botster_hub_client::DaemonObservabilityCounters,
) -> DaemonStatus {
    DaemonStatus {
        lifecycle_state: match status.lifecycle_state {
            crate::HubDaemonState::Created => "created",
            crate::HubDaemonState::Running => "running",
            crate::HubDaemonState::Stopped => "stopped",
        }
        .to_string(),
        compatibility: DaemonCompatibility::current(),
        software,
        installation,
        host_id: status.host_id.clone(),
        host_display_name: status.host_display_name.clone(),
        schema_version: status.schema_version,
        data_dir_configured: status.data_dir_configured,
        core_initialized: status.core_initialized,
        state_source: match status.state_source {
            HubStateLoadSource::Loaded => "loaded",
            HubStateLoadSource::Initialized => "initialized",
        }
        .to_string(),
        package_count: status.package_count,
        enabled_package_count: status.enabled_package_count,
        provider_count: status.provider_count,
        enabled_provider_count: status.enabled_provider_count,
        session_count,
        recovered_sessions: status
            .recovered_sessions
            .iter()
            .map(|session_id| session_id.0.clone())
            .collect(),
        stale_sessions: status
            .stale_sessions
            .iter()
            .map(|session_id| session_id.0.clone())
            .collect(),
        lifecycle_counters,
        live_attach_occupancy: Vec::new(),
        observability,
        diagnostics,
    }
}

pub(crate) fn daemon_operator_error_from_client(
    error: crate::HubClientError,
) -> DaemonOperatorError {
    match error {
        crate::HubClientError::InvalidRequest {
            request_id,
            operation,
            message,
        } => DaemonOperatorError {
            code: "invalid_request".to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            diagnostics: vec![DaemonDiagnostic::action_failure(
                operation_label(operation),
                &message,
            )],
            message,
        },
        crate::HubClientError::AdmissionDenied {
            request_id,
            operation,
            role,
        } => DaemonOperatorError {
            code: "admission_denied".to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message: format!("{role:?} is not allowed to run {operation:?}"),
            diagnostics: Vec::new(),
        },
        crate::HubClientError::Runtime {
            request_id,
            operation,
            kind,
        } => {
            let operation_label = operation_label(operation).to_string();
            let message = runtime_error_message(operation, kind);
            DaemonOperatorError {
                code: runtime_error_code(operation, kind).to_string(),
                request_id: request_id.0,
                diagnostics: runtime_error_diagnostics(operation, kind, &message),
                operation: operation_label,
                message,
            }
        }
        crate::HubClientError::PackageCapabilityDenied {
            request_id,
            operation,
            package_name,
        } => DaemonOperatorError {
            code: "package_capability_denied".to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message: format!("{package_name} is not allowed to run {operation:?}"),
            diagnostics: Vec::new(),
        },
        crate::HubClientError::SessionType {
            request_id,
            operation,
            kind,
            message,
        } => DaemonOperatorError {
            code: kind.to_string(),
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message,
            diagnostics: Vec::new(),
        },
        crate::HubClientError::Plugin {
            request_id,
            operation,
            code,
            message,
        } => DaemonOperatorError {
            diagnostics: plugin_error_diagnostics(operation, &code, &message),
            code,
            request_id: request_id.0,
            operation: operation_label(operation).to_string(),
            message,
        },
    }
}

fn plugin_error_diagnostics(
    operation: crate::HubClientOperation,
    code: &str,
    message: &str,
) -> Vec<DaemonDiagnostic> {
    if matches!(
        code,
        "undeclared_plugin_surface" | "unsupported_plugin_surface_operation"
    ) {
        let feature = match operation {
            crate::HubClientOperation::PluginSurfaceRender => FEATURE_PLUGIN_SURFACE_RENDER,
            crate::HubClientOperation::PluginSurfaceAction => FEATURE_PLUGIN_SURFACE_ACTION,
            _ => return Vec::new(),
        };
        return vec![DaemonDiagnostic {
            kind: botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature,
            operation: Some(operation_label(operation).to_string()),
            feature: Some(feature.to_string()),
            message: Some(message.to_string()),
        }];
    }
    if operation == crate::HubClientOperation::PluginSurfaceRender && code == "invalid_surface" {
        return vec![DaemonDiagnostic::action_failure(
            operation_label(operation),
            message.to_string(),
        )];
    }

    Vec::new()
}

pub(crate) fn daemon_operator_error_from_package(
    error: crate::PackageRegistryError,
) -> DaemonOperatorError {
    let package_name = package_error_display_name(&error);
    let operation = package_action_label(error.action).to_string();
    let diagnostics = package_registry_error_diagnostics(&error, &operation);
    DaemonOperatorError {
        code: "package_policy_error".to_string(),
        request_id: "daemon-package-mutation".to_string(),
        operation: operation.clone(),
        message: format!(
            "package {} denied for {}: {:?}",
            package_name, operation, error.reason
        ),
        diagnostics,
    }
}

fn package_registry_error_diagnostics(
    error: &crate::PackageRegistryError,
    operation: &str,
) -> Vec<DaemonDiagnostic> {
    match &error.reason {
        PackageAdmissionReason::InvalidConfiguration(diagnostics) => diagnostics
            .iter()
            .map(|diagnostic| DaemonDiagnostic {
                kind: botster_hub_client::DaemonDiagnosticKind::ActionFailure,
                operation: Some(operation.to_string()),
                feature: Some("package_registry".to_string()),
                message: Some(diagnostic.message.clone()),
            })
            .collect(),
        PackageAdmissionReason::MissingRequiredConfiguration(fields) => fields
            .iter()
            .map(|field| DaemonDiagnostic {
                kind: botster_hub_client::DaemonDiagnosticKind::ActionFailure,
                operation: Some(operation.to_string()),
                feature: Some("package_registry".to_string()),
                message: Some(format!("required configuration field {field} is missing")),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn package_error_display_name(error: &crate::PackageRegistryError) -> &str {
    if error
        .audit_reason
        .contains("refresh local package registrations")
    {
        return &error.package_name;
    }
    match error.reason {
        PackageAdmissionReason::InvalidLocalManifest(_)
        | PackageAdmissionReason::UnsafeLocalPath(_) => "<local-package>",
        _ => &error.package_name,
    }
}

fn runtime_error_code(
    operation: crate::HubClientOperation,
    kind: crate::HubClientRuntimeErrorKind,
) -> &'static str {
    match (operation, kind) {
        (_, crate::HubClientRuntimeErrorKind::UnknownSession) => "unknown_session",
        (_, crate::HubClientRuntimeErrorKind::SessionAlreadyExists) => "session_already_exists",
        (_, crate::HubClientRuntimeErrorKind::SpawnFailed)
        | (
            crate::HubClientOperation::Spawn | crate::HubClientOperation::SpawnSessionType,
            crate::HubClientRuntimeErrorKind::Runtime,
        ) => "spawn_failed",
        (_, crate::HubClientRuntimeErrorKind::ModeReadFailed) => "mode_read_failed",
        (_, crate::HubClientRuntimeErrorKind::Runtime) => "runtime_error",
        (_, crate::HubClientRuntimeErrorKind::State) => "state_error",
    }
}

fn runtime_error_message(
    operation: crate::HubClientOperation,
    kind: crate::HubClientRuntimeErrorKind,
) -> String {
    match (operation, kind) {
        (crate::HubClientOperation::Spawn, crate::HubClientRuntimeErrorKind::SessionAlreadyExists) => {
            "spawn rejected because a session with that id already exists".to_string()
        }
        (crate::HubClientOperation::Spawn, crate::HubClientRuntimeErrorKind::SpawnFailed)
        | (crate::HubClientOperation::Spawn, crate::HubClientRuntimeErrorKind::Runtime) => {
            "spawn failed before the session started; verify the configured session worker and command"
                .to_string()
        }
        (
            crate::HubClientOperation::SpawnSessionType,
            crate::HubClientRuntimeErrorKind::SessionAlreadyExists,
        ) => "session type spawn rejected because a session with that id already exists".to_string(),
        (
            crate::HubClientOperation::SpawnSessionType,
            crate::HubClientRuntimeErrorKind::SpawnFailed
            | crate::HubClientRuntimeErrorKind::Runtime,
        ) => "session type spawn failed before the session started; verify the configured session worker and session type command"
            .to_string(),
        (crate::HubClientOperation::ReadModeFlags, crate::HubClientRuntimeErrorKind::ModeReadFailed) => {
            "session worker failed the authoritative mode read; replace or terminate the incompatible worker"
                .to_string()
        }
        _ => format!("runtime failed while handling {operation:?}: {kind:?}"),
    }
}

fn runtime_error_diagnostics(
    operation: crate::HubClientOperation,
    kind: crate::HubClientRuntimeErrorKind,
    message: &str,
) -> Vec<DaemonDiagnostic> {
    if matches!(
        operation,
        crate::HubClientOperation::Spawn | crate::HubClientOperation::SpawnSessionType
    ) {
        match kind {
            crate::HubClientRuntimeErrorKind::SessionAlreadyExists => {
                let message = if operation == crate::HubClientOperation::SpawnSessionType {
                    "session type spawn rejected because a session with that id already exists"
                } else {
                    "spawn rejected because a session with that id already exists"
                };
                return vec![DaemonDiagnostic::action_failure(
                    operation_label(operation),
                    message,
                )];
            }
            crate::HubClientRuntimeErrorKind::SpawnFailed => {
                let message = if operation == crate::HubClientOperation::SpawnSessionType {
                    "session type spawn failed before the session started; verify the configured session worker and session type command"
                } else {
                    "spawn failed before the session started; verify the configured session worker and command"
                };
                return vec![DaemonDiagnostic::action_failure(
                    operation_label(operation),
                    message,
                )];
            }
            crate::HubClientRuntimeErrorKind::Runtime => {
                let message = if operation == crate::HubClientOperation::SpawnSessionType {
                    "session type spawn failed before the session started; verify the configured session worker and session type command"
                } else {
                    "spawn failed before the session started; verify the configured session worker and command"
                };
                return vec![DaemonDiagnostic::action_failure(
                    operation_label(operation),
                    message,
                )];
            }
            _ => {}
        }
    }

    if kind == crate::HubClientRuntimeErrorKind::UnknownSession
        && matches!(operation, crate::HubClientOperation::Attach)
    {
        return vec![DaemonDiagnostic::terminal_stream_unavailable(
            operation_label(operation),
            message,
        )];
    }

    if kind == crate::HubClientRuntimeErrorKind::ModeReadFailed
        && operation == crate::HubClientOperation::ReadModeFlags
    {
        return vec![DaemonDiagnostic::worker_compatibility(
            operation_label(operation),
            message,
        )];
    }

    Vec::new()
}

fn operation_label(operation: crate::HubClientOperation) -> &'static str {
    match operation {
        crate::HubClientOperation::Status => "status",
        crate::HubClientOperation::ListSessions => "list_sessions",
        crate::HubClientOperation::SubscribeEntities => "subscribe_entities",
        crate::HubClientOperation::UnsubscribeEntities => "unsubscribe_entities",
        crate::HubClientOperation::RemoveSession => "remove_session",
        crate::HubClientOperation::Spawn => "spawn",
        crate::HubClientOperation::Attach => "attach",
        crate::HubClientOperation::Detach => "detach",
        crate::HubClientOperation::Shutdown => "shutdown",
        crate::HubClientOperation::GuardedNotificationWrite => "guarded_notification_write",
        crate::HubClientOperation::NotifySession => "notify_session",
        crate::HubClientOperation::PublishRoutedEnvelope => "publish_routed_envelope",
        crate::HubClientOperation::DrainRoutedEnvelopes => "drain_routed_envelopes",
        crate::HubClientOperation::AcknowledgeRoutedEnvelope => "acknowledge_routed_envelope",
        crate::HubClientOperation::ReadScreen => "read_screen",
        crate::HubClientOperation::ReadModeFlags => "read_mode_flags",
        crate::HubClientOperation::CaptureSnapshot => "capture_snapshot",
        crate::HubClientOperation::ListPackages => "list_packages",
        crate::HubClientOperation::ListPackageNavigation => "list_package_navigation",
        crate::HubClientOperation::ListSessionTypes => "list_session_types",
        crate::HubClientOperation::ListSessionTypesForTarget => "list_session_types_for_target",
        crate::HubClientOperation::ShowSessionType => "show_session_type",
        crate::HubClientOperation::ShowSessionTypeDefinition => "show_session_type_definition",
        crate::HubClientOperation::CreateSessionType => "create_session_type",
        crate::HubClientOperation::UpdateSessionType => "update_session_type",
        crate::HubClientOperation::DeleteSessionType => "delete_session_type",
        crate::HubClientOperation::ResolveSessionType => "resolve_session_type",
        crate::HubClientOperation::SpawnSessionType => "spawn_session_type",
        crate::HubClientOperation::ReadSessionContext => "read_session_context",
        crate::HubClientOperation::PluginLifecycleStatus => "plugin_lifecycle_status",
        crate::HubClientOperation::PluginSurfaceRender => "plugin_surface_render",
        crate::HubClientOperation::PluginSurfaceAction => "plugin_surface_action",
    }
}

pub(crate) fn package_state_label(state: crate::HubClientPackageState) -> &'static str {
    match state {
        crate::HubClientPackageState::Installed => "installed",
        crate::HubClientPackageState::Enabled => "enabled",
        crate::HubClientPackageState::Disabled => "disabled",
    }
}

pub(crate) fn runnable_entrypoint_kind_label(kind: &RunnableEntrypointKind) -> &'static str {
    match kind {
        RunnableEntrypointKind::WebApp => "web_app",
        RunnableEntrypointKind::TerminalApp => "terminal_app",
    }
}

pub(crate) fn runnable_launch_mode_label(mode: &RunnableEntrypointLaunchMode) -> &'static str {
    match mode {
        RunnableEntrypointLaunchMode::Background => "background",
        RunnableEntrypointLaunchMode::ForegroundStdio => "foreground_stdio",
    }
}

fn runnable_process_state_label(state: &RunnableEntrypointProcessState) -> &'static str {
    match state {
        RunnableEntrypointProcessState::NotStarted => "not_started",
        RunnableEntrypointProcessState::Running => "running",
        RunnableEntrypointProcessState::Exited => "exited",
        RunnableEntrypointProcessState::Failed => "failed",
    }
}

pub(crate) fn package_action_label(action: PackageAction) -> &'static str {
    match action {
        PackageAction::Install => "install",
        PackageAction::Show => "show",
        PackageAction::Configure => "configure",
        PackageAction::Reload => "reload",
        PackageAction::Enable => "enable",
        PackageAction::Disable => "disable",
        PackageAction::Remove => "remove",
        PackageAction::CheckUpdate => "check_update",
        PackageAction::PreviewUpdate => "preview_update",
        PackageAction::ApplyUpdate => "apply_update",
        PackageAction::Pin => "pin",
        PackageAction::Prepare => "prepare",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_hub_client::DaemonPackageActionStatus;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn projection_manifest(name: &str) -> crate::HubPackageManifest {
        crate::HubPackageManifest {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            kind: botster_core::ExtensionKind::Plugin,
            botster: ">=0.1.0".to_string(),
            source: None,
            capabilities: Vec::new(),
            entrypoints: Vec::new(),
            dependencies: Vec::new(),
            features: Vec::new(),
            host_profile: None,
            configuration: None,
            runnable_entrypoints: Vec::new(),
            surfaces: Vec::new(),
            navigation: Vec::new(),
            events: crate::HubPackageEvents::default(),
        }
    }

    fn projection_record(
        name: &str,
        state: crate::PackageState,
        entrypoints: Vec<crate::PackageRunnableEntrypoint>,
    ) -> crate::PackageRecord {
        crate::PackageRecord {
            manifest: projection_manifest(name),
            state,
            classification: crate::PackageClassification::Plugin,
            trust: crate::PackageTrust::third_party(),
            provenance: crate::PackageProvenance {
                source: "test".to_string(),
                checksum: None,
            },
            source_metadata: None,
            pin: None,
            update_policy: crate::PackageUpdatePolicy::Manual,
            admitted_capabilities: Vec::new(),
            compatibility: crate::PackageCompatibility {
                botster_requirement: ">=0.1.0".to_string(),
                hub_version: "0.1.0".to_string(),
                result: crate::PackageCompatibilityResult::Compatible,
                diagnostics: Vec::new(),
            },
            runnable_entrypoints: entrypoints,
            session_types: Vec::new(),
            configuration: crate::PackageConfigurationState::default(),
            installed_at: None,
            updated_at: None,
            last_audit_reason: "projection fixture".to_string(),
            admitted_host_profile: None,
        }
    }

    fn web_entrypoint(id: &str, may_supervise: bool) -> crate::PackageRunnableEntrypoint {
        crate::PackageRunnableEntrypoint {
            id: id.to_string(),
            kind: RunnableEntrypointKind::WebApp,
            launch_mode: RunnableEntrypointLaunchMode::Background,
            command: "web".to_string(),
            args: Vec::new(),
            working_directory: crate::PackageRunnableWorkingDirectory::PackageRoot,
            injections: Vec::new(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            readiness: None,
            may_supervise,
            process: crate::PackageRunnableProcess::default(),
        }
    }

    fn action_projection(actions: &[DaemonPackageActionState]) -> Vec<(&str, &str, Option<&str>)> {
        actions
            .iter()
            .map(|action| {
                (
                    action.action_id.as_str(),
                    match action.status {
                        DaemonPackageActionStatus::Available => "available",
                        DaemonPackageActionStatus::Blocked => "blocked",
                        DaemonPackageActionStatus::Unavailable => "unavailable",
                    },
                    action.reason.as_deref(),
                )
            })
            .collect()
    }

    fn empty_client_package(
        name: &str,
        state: crate::HubClientPackageState,
    ) -> crate::HubClientPackage {
        crate::HubClientPackage {
            package_name: name.to_string(),
            version: "1.0.0".to_string(),
            classification: crate::HubClientPackageClassification::Plugin,
            source_kind: "unknown".to_string(),
            state,
            requested_capabilities: Vec::new(),
            surfaces: Vec::new(),
            notice_reactions: Vec::new(),
            navigation: Vec::new(),
            runnable_entrypoints: Vec::new(),
            configuration: crate::HubClientPackageConfiguration {
                schema: None,
                effective_values: BTreeMap::new(),
                missing_required: Vec::new(),
                diagnostics: Vec::new(),
            },
            availability: crate::HubClientPackageAvailability {
                state: crate::HubClientPackageAvailabilityState::Available,
                reasons: Vec::new(),
            },
            dependency_availability: Vec::new(),
            feature_availability: Vec::new(),
            provider_profile_admitted: false,
        }
    }

    #[test]
    fn apps_from_record_projects_blocked_disabled_missing_and_compatible_cases() {
        struct Case {
            name: &'static str,
            state: crate::PackageState,
            entrypoints: Vec<crate::PackageRunnableEntrypoint>,
            expected_count: usize,
            expected_blocked: &'static [&'static str],
            expected_lifecycle: Option<&'static str>,
            expected_start: Option<(&'static str, Option<&'static str>)>,
        }

        let unsupported = crate::PackageRunnableEntrypoint {
            launch_mode: RunnableEntrypointLaunchMode::ForegroundStdio,
            ..web_entrypoint("web", true)
        };
        let cases = [
            Case {
                name: "compatible",
                state: crate::PackageState::Enabled,
                entrypoints: vec![web_entrypoint("web", true)],
                expected_count: 1,
                expected_blocked: &[],
                expected_lifecycle: Some("not_started"),
                expected_start: Some(("available", None)),
            },
            Case {
                name: "disabled",
                state: crate::PackageState::Disabled,
                entrypoints: vec![web_entrypoint("web", true)],
                expected_count: 1,
                expected_blocked: &["package_not_enabled"],
                expected_lifecycle: Some("not_started"),
                expected_start: Some(("blocked", Some("package_not_enabled"))),
            },
            Case {
                name: "not_supervisable",
                state: crate::PackageState::Enabled,
                entrypoints: vec![web_entrypoint("web", false)],
                expected_count: 1,
                expected_blocked: &["entrypoint_not_supervisable"],
                expected_lifecycle: Some("not_started"),
                expected_start: Some(("unavailable", Some("entrypoint_not_supervisable"))),
            },
            Case {
                name: "unsupported_launch_mode",
                state: crate::PackageState::Enabled,
                entrypoints: vec![unsupported],
                expected_count: 1,
                expected_blocked: &["unsupported_launch_mode"],
                expected_lifecycle: Some("not_started"),
                expected_start: None,
            },
            Case {
                name: "missing_entrypoint",
                state: crate::PackageState::Enabled,
                entrypoints: Vec::new(),
                expected_count: 0,
                expected_blocked: &[],
                expected_lifecycle: None,
                expected_start: None,
            },
        ];

        for case in cases {
            let apps = apps_from_record(
                &projection_record("demo", case.state, case.entrypoints),
                &BTreeMap::new(),
            );
            assert_eq!(apps.len(), case.expected_count, "{}", case.name);
            if case.expected_count == 0 {
                continue;
            }
            assert_eq!(
                apps[0].blocked_reasons, case.expected_blocked,
                "{}",
                case.name
            );
            assert_eq!(
                apps[0].lifecycle_state,
                case.expected_lifecycle.unwrap(),
                "{}",
                case.name
            );
            assert_eq!(apps[0].package_name, "demo", "{}", case.name);
            assert_eq!(apps[0].app_id, "web", "{}", case.name);
            assert_eq!(apps[0].kind, "web_app", "{}", case.name);
            if let Some((status, reason)) = case.expected_start {
                let start = apps[0]
                    .actions
                    .iter()
                    .find(|action| action.action_id == "start_package_entrypoint")
                    .expect(case.name);
                assert_eq!(
                    match start.status {
                        DaemonPackageActionStatus::Available => "available",
                        DaemonPackageActionStatus::Blocked => "blocked",
                        DaemonPackageActionStatus::Unavailable => "unavailable",
                    },
                    status,
                    "{}",
                    case.name
                );
                assert_eq!(start.reason.as_deref(), reason, "{}", case.name);
            } else {
                assert!(
                    apps[0].actions.is_empty(),
                    "{} should have no background actions",
                    case.name
                );
            }
        }
    }

    #[test]
    fn package_route_descriptors_project_disabled_missing_and_compatible_cases() {
        let surface = PackageSurfaceDescriptor {
            id: "home".to_string(),
            kind: PackageSurfaceKind::App,
            title: "Home".to_string(),
            description: None,
            icon: Some("home".to_string()),
            order: None,
            category: Some("apps".to_string()),
            supports: Vec::new(),
        };
        let entrypoint = crate::HubClientPackageRunnableEntrypoint {
            id: "web".to_string(),
            kind: "web_app".to_string(),
            launch_mode: "background".to_string(),
            command: "web".to_string(),
            args: Vec::new(),
            working_directory: crate::HubClientPackageWorkingDirectory {
                policy: "package_root".to_string(),
                path: None,
            },
            environment: Vec::new(),
            capabilities: Vec::new(),
            may_supervise: true,
            process: crate::HubClientPackageProcess {
                state: "not_started".to_string(),
                pid: None,
                started_at: None,
                exited_at: None,
                exit_status: None,
                diagnostics: Vec::new(),
            },
        };

        let mut enabled = empty_client_package("demo", crate::HubClientPackageState::Enabled);
        enabled.surfaces = vec![surface.clone()];
        enabled.runnable_entrypoints = vec![entrypoint.clone()];
        enabled.configuration.schema = Some(serde_json::json!({"fields": []}));

        let enabled_routes = package_route_descriptors(&enabled);
        assert_eq!(
            enabled_routes
                .iter()
                .map(|route| {
                    (
                        route.route_id.as_str(),
                        route.route_path.as_str(),
                        route.enabled,
                        route.blocked,
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                ("surface:home", "/packages/demo/surfaces/home", true, false),
                ("app:web", "/packages/demo/apps/web", true, false),
                ("settings", "/packages/demo/settings", true, false),
            ]
        );

        let mut disabled = enabled.clone();
        disabled.state = crate::HubClientPackageState::Disabled;
        let disabled_routes = package_route_descriptors(&disabled);
        assert!(
            disabled_routes.iter().all(|route| route
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.kind == "package_not_enabled" })),
            "disabled package routes should carry package_not_enabled"
        );
        assert!(!disabled_routes[0].enabled);
        assert!(disabled_routes[0].blocked);
        assert!(!disabled_routes[1].enabled);
        assert!(disabled_routes[1].blocked);
        assert!(disabled_routes[2].enabled);
        assert!(!disabled_routes[2].blocked);

        let missing_navigation = package_navigation_entry(
            crate::HubClientPackageNavigationEntry {
                package_name: "demo".to_string(),
                item_id: "missing".to_string(),
                label: "Missing".to_string(),
                icon: None,
                description: None,
                target: crate::HubClientPackageNavigationTarget::Surface {
                    surface_id: "absent".to_string(),
                },
            },
            &[enabled],
        );
        assert_eq!(missing_navigation.route_id, "surface:absent");
        assert!(missing_navigation.blocked);
        assert!(!missing_navigation.enabled);
        assert_eq!(
            missing_navigation.diagnostics[0].kind,
            "navigation_target_not_found"
        );
    }

    #[test]
    fn available_package_actions_project_compatible_and_blocked_install() {
        fn catalog(
            result: crate::PackageCompatibilityResult,
            state: crate::AvailablePackageState,
        ) -> crate::AvailablePackage {
            crate::AvailablePackage {
                entry_id: "demo".to_string(),
                package_name: "demo".to_string(),
                version: "1.0.0".to_string(),
                classification: crate::PackageClassification::Plugin,
                source_kind: crate::PackageRegistryEntrySourceKind::LocalPath,
                source_label: "local".to_string(),
                first_party: false,
                requested_capabilities: Vec::new(),
                compatibility: crate::PackageCompatibility {
                    botster_requirement: ">=0.1.0".to_string(),
                    hub_version: "0.1.0".to_string(),
                    result,
                    diagnostics: vec!["compat".to_string()],
                },
                state,
                pin: None,
            }
        }

        let registry = PathBuf::from("/tmp/botster-registry.json");
        let compatible = available_package_actions(
            &catalog(
                crate::PackageCompatibilityResult::Compatible,
                crate::AvailablePackageState::Available,
            ),
            Some(&registry),
        );
        assert_eq!(
            action_projection(&compatible)[0],
            ("install_package_registry_entry", "available", None)
        );

        let already_installed = available_package_actions(
            &catalog(
                crate::PackageCompatibilityResult::Compatible,
                crate::AvailablePackageState::Installed,
            ),
            Some(&registry),
        );
        assert_eq!(
            action_projection(&already_installed)[0],
            (
                "install_package_registry_entry",
                "blocked",
                Some("already_installed")
            )
        );

        let incompatible = available_package_actions(
            &catalog(
                crate::PackageCompatibilityResult::Incompatible,
                crate::AvailablePackageState::Available,
            ),
            Some(&registry),
        );
        assert_eq!(
            action_projection(&incompatible)[0],
            (
                "install_package_registry_entry",
                "blocked",
                Some("botster_compatibility")
            )
        );

        let missing_registry = available_package_actions(
            &catalog(
                crate::PackageCompatibilityResult::Compatible,
                crate::AvailablePackageState::Available,
            ),
            None,
        );
        assert_eq!(
            action_projection(&missing_registry)[0],
            (
                "install_package_registry_entry",
                "blocked",
                Some("registry_path_required")
            )
        );
    }

    #[test]
    fn daemon_operator_error_projection_covers_client_and_package_denials() {
        let request_id = botster_core::RequestId("req-1".to_string());
        let cases = [
            (
                crate::HubClientError::InvalidRequest {
                    request_id: request_id.clone(),
                    operation: crate::HubClientOperation::Status,
                    message: "bad".to_string(),
                },
                "invalid_request",
                "status",
            ),
            (
                crate::HubClientError::AdmissionDenied {
                    request_id: request_id.clone(),
                    operation: crate::HubClientOperation::Spawn,
                    role: crate::HubClientRole::LocalOperator,
                },
                "admission_denied",
                "spawn",
            ),
            (
                crate::HubClientError::Runtime {
                    request_id: request_id.clone(),
                    operation: crate::HubClientOperation::Attach,
                    kind: crate::HubClientRuntimeErrorKind::UnknownSession,
                },
                "unknown_session",
                "attach",
            ),
            (
                crate::HubClientError::PackageCapabilityDenied {
                    request_id: request_id.clone(),
                    operation: crate::HubClientOperation::PluginSurfaceRender,
                    package_name: "demo".to_string(),
                },
                "package_capability_denied",
                "plugin_surface_render",
            ),
            (
                crate::HubClientError::Plugin {
                    request_id,
                    operation: crate::HubClientOperation::PluginSurfaceRender,
                    code: "undeclared_plugin_surface".to_string(),
                    message: "missing".to_string(),
                },
                "undeclared_plugin_surface",
                "plugin_surface_render",
            ),
        ];
        for (error, code, operation) in cases {
            let projected = daemon_operator_error_from_client(error);
            assert_eq!(projected.code, code, "{operation}");
            assert_eq!(projected.operation, operation, "{code}");
            assert_eq!(projected.request_id, "req-1", "{code}");
        }

        let package_error = daemon_operator_error_from_package(crate::PackageRegistryError {
            package_name: "demo".to_string(),
            action: crate::PackageAction::Enable,
            reason: crate::PackageAdmissionReason::InvalidLocalManifest("broken".to_string()),
            state: None,
            classification: None,
            audit_reason: "enable denied".to_string(),
        });
        assert_eq!(package_error.code, "package_policy_error");
        assert_eq!(package_error.operation, "enable");
        assert_eq!(package_error.request_id, "daemon-package-mutation");
        assert!(package_error.message.contains("<local-package>"));
    }

    #[test]
    fn daemon_status_from_status_projects_lifecycle_counts_and_session_ids() {
        let status = crate::HubDaemonStatus {
            lifecycle_state: crate::HubDaemonState::Running,
            host_id: "host-1".to_string(),
            host_display_name: "Host".to_string(),
            schema_version: 3,
            data_dir_configured: true,
            core_initialized: true,
            state_source: crate::HubStateLoadSource::Loaded,
            package_count: 4,
            enabled_package_count: 2,
            provider_count: 1,
            enabled_provider_count: 1,
            recovered_sessions: vec![botster_core::SessionId("recovered".to_string())],
            stale_sessions: vec![botster_core::SessionId("stale".to_string())],
        };
        let counters = DaemonLifecycleCounters {
            accepted_connections: 9,
            ..DaemonLifecycleCounters::default()
        };
        let software = DaemonSoftwareIdentity {
            product_id: "botster-hub".to_string(),
            product_name: "Botster Hub".to_string(),
            version: "0.1.0".to_string(),
            build_revision: Some("test-rev".to_string()),
        };
        let installation = DaemonInstallationIdentity {
            mode: botster_hub_client::DaemonInstallationMode::Development,
            provenance: "test-fixture".to_string(),
            release_channel: None,
            provider: None,
            diagnostics: Vec::new(),
        };
        let projected = daemon_status_from_status(
            &status,
            7,
            Vec::new(),
            counters,
            software.clone(),
            installation.clone(),
            botster_hub_client::DaemonObservabilityCounters::default(),
        );
        assert_eq!(projected.lifecycle_state, "running");
        assert_eq!(projected.compatibility, DaemonCompatibility::current());
        assert_eq!(projected.software, software);
        assert_eq!(projected.installation, installation);
        assert_eq!(projected.host_id, "host-1");
        assert_eq!(projected.schema_version, 3);
        assert!(projected.data_dir_configured);
        assert!(projected.core_initialized);
        assert_eq!(projected.state_source, "loaded");
        assert_eq!(projected.package_count, 4);
        assert_eq!(projected.enabled_package_count, 2);
        assert_eq!(projected.session_count, 7);
        assert_eq!(projected.recovered_sessions, vec!["recovered".to_string()]);
        assert_eq!(projected.stale_sessions, vec!["stale".to_string()]);
        assert_eq!(projected.lifecycle_counters.accepted_connections, 9);
    }
}
