use std::path::PathBuf;

use botster_hub_client::{
    DaemonApp, DaemonCaptureSnapshot, DaemonCoordination, DaemonDiagnostic, DaemonEvent,
    DaemonHubUpdate, DaemonHubUpdateExecution, DaemonLifecycleCounters, DaemonLocalWebrtcAnswer,
    DaemonLocalWebrtcBootstrap, DaemonModeFlags, DaemonOperatorError, DaemonPackageDiagnostic,
    DaemonPackageInstallEffect, DaemonPackageInstallPlan, DaemonPackageRouteDescriptor,
    DaemonPackageUpdateStatus, DaemonPluginResourceCounters, DaemonPluginSurface, DaemonReadScreen,
    DaemonResolvedAppLaunch, DaemonResolvedSessionType, DaemonResponse, DaemonResponseKind,
    DaemonSession, DaemonSessionCleanup, DaemonSessionContext, DaemonSessionTypeEditableDefinition,
    DaemonSpawnTargetValidation, DaemonTerminalReservation, DaemonUiTreeSnapshot,
};
use botster_ui_contract::{UiActionResult, UiActionResultState};
use serde_json::Value;

use crate::client_api_dto::package::{
    daemon_available_package_from_policy, daemon_package_from_client,
};
use crate::client_api_dto::plugin::{
    daemon_plugin_lifecycle_from_client, daemon_plugin_worker_counters_from_client,
};
use crate::client_api_dto::session::{
    daemon_session_from_client, daemon_session_type_definition_from_client,
    daemon_session_type_from_client, daemon_session_type_mutation_source,
};
use crate::client_api_dto::workspace::{daemon_spawn_target, daemon_worktree};
use crate::daemon_projection::{daemon_status_from_status, package_navigation_entries};
use crate::maintenance::{installation_identity, software_identity};
use crate::{
    AvailablePackage, HubClientCaptureSnapshot, HubClientModeFlags, HubClientPackage,
    HubClientPackageNavigationEntry, HubClientPluginLifecycleReport, HubClientPluginSurface,
    HubClientReadScreen, HubClientSession, HubDaemonStatus, McpToolDescriptor, PackageInstallPlan,
    ResolvedSessionType, SpawnTarget, SpawnTargetValidation, Worktree,
};

pub(crate) fn daemon_response_base(kind: DaemonResponseKind) -> DaemonResponse {
    DaemonResponse {
        kind,
        status: None,
        sessions: Vec::new(),
        session_types: Vec::new(),
        session_type_definition: None,
        resolved_session_type: None,
        session_context: None,
        read_screen: None,
        mode_flags: None,
        terminal_reservation: None,
        capture_snapshot: None,
        spawn_targets: Vec::new(),
        spawn_target_validation: None,
        worktrees: Vec::new(),
        apps: Vec::new(),
        resolved_app_launch: None,
        resolved_package_route: None,
        package_navigation: Vec::new(),
        packages: Vec::new(),
        available_packages: Vec::new(),
        install_plan: None,
        update_status: None,
        hub_update: None,
        hub_update_execution: None,
        package_decision: None,
        lifecycle: Vec::new(),
        plugin_worker_counters: None,
        plugin_resource_counters: None,
        plugin_tools: Vec::new(),
        plugin_tool_result: Value::Null,
        plugin_surface: None,
        plugin_action_result: None,
        local_webrtc_bootstrap: None,
        local_webrtc_answer: None,
        events: Vec::new(),
        cleanup: None,
        coordination: None,
        error: None,
        diagnostics: Vec::new(),
    }
}

pub(crate) fn daemon_status(
    status: HubDaemonStatus,
    session_count: usize,
    mut egress_diagnostics: Vec<DaemonDiagnostic>,
    lifecycle_counters: DaemonLifecycleCounters,
    observability_counters: botster_hub_client::DaemonObservabilityCounters,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Status);
    response.status = Some(daemon_status_from_status(
        &status,
        session_count,
        egress_diagnostics.clone(),
        lifecycle_counters,
        software_identity(),
        installation_identity(),
        observability_counters,
    ));
    response.diagnostics = vec![DaemonDiagnostic::connected("status")];
    response.diagnostics.append(&mut egress_diagnostics);
    response
}

pub(crate) fn daemon_hub_update(update: DaemonHubUpdate) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::HubUpdate);
    response.hub_update = Some(update);
    response
}

pub(crate) fn daemon_hub_update_execution(execution: DaemonHubUpdateExecution) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::HubUpdateExecution);
    response.hub_update_execution = Some(execution);
    response
}

pub(crate) fn daemon_sessions(sessions: Vec<HubClientSession>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Sessions);
    response.sessions = sessions
        .into_iter()
        .map(daemon_session_from_client)
        .collect();
    response
}

pub(crate) fn daemon_spawned(session: DaemonSession, events: Vec<DaemonEvent>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Spawned);
    response.sessions = vec![session];
    response.events = events;
    response
}

pub(crate) fn daemon_events(events: Vec<DaemonEvent>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Events);
    response.events = events;
    response
}

pub(crate) fn daemon_terminal_reservation(
    reservation: DaemonTerminalReservation,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::TerminalReservation);
    response.terminal_reservation = Some(reservation);
    response
}

pub(crate) fn daemon_read_screen(screen: HubClientReadScreen) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ReadScreen);
    response.read_screen = Some(DaemonReadScreen {
        session_id: screen.session_id.0,
        text: screen.text,
    });
    response
}

pub(crate) fn daemon_mode_flags(mode_flags: HubClientModeFlags) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ReadModeFlags);
    response.mode_flags = Some(DaemonModeFlags::new(
        mode_flags.session_id.0,
        mode_flags.kitty_enabled,
        mode_flags.cursor_visible,
        mode_flags.bracketed_paste,
        mode_flags.mouse_mode,
        mode_flags.alt_screen,
        mode_flags.focus_reporting,
        mode_flags.application_cursor,
        mode_flags.mode_generation,
        mode_flags.mode_revision,
    ));
    response
}

pub(crate) fn daemon_capture_snapshot(snapshot: HubClientCaptureSnapshot) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::CaptureSnapshot);
    response.capture_snapshot = Some(DaemonCaptureSnapshot {
        session_id: snapshot.session_id.0,
        rows: snapshot.rows,
        cols: snapshot.cols,
        payload_format: snapshot.payload_format,
        payload_bytes: snapshot.payload_bytes,
    });
    response
}

pub(crate) fn daemon_packages(packages: Vec<HubClientPackage>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Packages);
    response.packages = packages
        .into_iter()
        .map(daemon_package_from_client)
        .collect();
    response
}

pub(crate) fn daemon_package_navigation(
    navigation: Vec<HubClientPackageNavigationEntry>,
    packages: &[HubClientPackage],
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PackageNavigation);
    response.package_navigation = package_navigation_entries(navigation, packages);
    response
}

pub(crate) fn daemon_session_types(templates: Vec<crate::HubSessionType>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SessionTypes);
    response.session_types = templates
        .into_iter()
        .map(daemon_session_type_from_client)
        .collect();
    response
}

pub(crate) fn daemon_session_type_definition(
    definition: crate::HubSessionTypeDefinition,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SessionTypeDefinition);
    response.session_type_definition = Some(DaemonSessionTypeEditableDefinition {
        session_type_id: definition.session_type_id,
        source: daemon_session_type_mutation_source(definition.source),
        definition: daemon_session_type_definition_from_client(definition.definition),
    });
    response
}

pub(crate) fn daemon_resolved_session_type(resolved: ResolvedSessionType) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ResolvedSessionType);
    response.resolved_session_type = Some(DaemonResolvedSessionType {
        session_type: daemon_session_type_from_client(resolved.session_type),
        session_id: resolved.session_id.0,
        executable: resolved.executable,
        arguments: resolved.arguments,
        working_directory: resolved.working_directory,
        environment: resolved.environment,
        context_id: resolved.context_id,
        context_keys: resolved.context_keys,
    });
    response
}

pub(crate) fn daemon_session_context(context: crate::HubSessionContext) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SessionContext);
    response.session_context = Some(DaemonSessionContext {
        context_id: context.context_id,
        session_id: context.session_id.0,
        values: context.values,
    });
    response
}

pub(crate) fn daemon_spawn_targets(targets: Vec<SpawnTarget>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SpawnTargets);
    response.spawn_targets = targets.into_iter().map(daemon_spawn_target).collect();
    response
}

pub(crate) fn daemon_spawn_target_validation(validation: SpawnTargetValidation) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SpawnTargetValidation);
    response.spawn_target_validation = Some(DaemonSpawnTargetValidation {
        target_id: validation.target_id,
        ok: validation.ok,
        status: validation.status,
    });
    response
}

pub(crate) fn daemon_worktrees(worktrees: Vec<Worktree>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Worktrees);
    response.worktrees = worktrees.into_iter().map(daemon_worktree).collect();
    response
}

pub(crate) fn daemon_apps(apps: Vec<DaemonApp>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::Apps);
    response.apps = apps;
    response
}

pub(crate) fn daemon_resolved_app_launch(launch: DaemonResolvedAppLaunch) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ResolvedAppLaunch);
    response.resolved_app_launch = Some(launch);
    response
}

pub(crate) fn daemon_resolved_package_route(route: DaemonPackageRouteDescriptor) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::ResolvedPackageRoute);
    response.resolved_package_route = Some(route);
    response
}

pub(crate) fn daemon_available_packages(
    packages: Vec<AvailablePackage>,
    registry_path: &PathBuf,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::AvailablePackages);
    response.available_packages = packages
        .into_iter()
        .map(|package| daemon_available_package_from_policy(package, Some(registry_path)))
        .collect();
    response
}

pub(crate) fn daemon_package_install_plan(plan: PackageInstallPlan) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PackageInstallPlan);
    response.install_plan = Some(DaemonPackageInstallPlan {
        entry: daemon_available_package_from_policy(plan.entry, None),
        effects: plan
            .effects
            .into_iter()
            .map(|effect| DaemonPackageInstallEffect {
                kind: effect.kind,
                message: effect.message,
            })
            .collect(),
        diagnostics: plan
            .diagnostics
            .into_iter()
            .map(|diagnostic| DaemonPackageDiagnostic {
                kind: diagnostic.kind,
                message: diagnostic.message,
            })
            .collect(),
        mutates_registry: plan.mutates_registry,
        starts_entrypoints: plan.starts_entrypoints,
    });
    response
}

pub(crate) fn daemon_package_update_status(
    update_status: DaemonPackageUpdateStatus,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PackageUpdateStatus);
    response.update_status = Some(update_status);
    response
}

pub(crate) fn daemon_plugin_lifecycle(report: HubClientPluginLifecycleReport) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginLifecycle);
    response.lifecycle = report
        .lifecycle
        .into_iter()
        .map(daemon_plugin_lifecycle_from_client)
        .collect();
    response.plugin_worker_counters = Some(daemon_plugin_worker_counters_from_client(
        report.worker_counters,
    ));
    response.plugin_resource_counters = Some(DaemonPluginResourceCounters {
        active_timer_resources: report.resource_counters.active_timer_resources,
    });
    response
}

pub(crate) fn daemon_session_cleanup(cleanup: DaemonSessionCleanup) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::SessionCleanup);
    response.cleanup = Some(cleanup);
    response
}

pub(crate) fn daemon_unknown_session_cleanup(session_id: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "unknown_session".to_string(),
        request_id: "daemon-sessions-shutdown".to_string(),
        operation: "shutdown".to_string(),
        message: format!("unknown session: {session_id}"),
        diagnostics: Vec::new(),
    });
    response
}

pub(crate) fn daemon_local_webrtc_answer(answer: DaemonLocalWebrtcAnswer) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::LocalWebrtcAnswer);
    response.diagnostics = answer.diagnostics.clone();
    response.local_webrtc_answer = Some(answer);
    response
}

pub(crate) fn daemon_local_webrtc_bootstrap(
    bootstrap: DaemonLocalWebrtcBootstrap,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::LocalWebrtcBootstrap);
    response.local_webrtc_bootstrap = Some(bootstrap);
    response
}

pub(crate) fn daemon_coordination(
    kind: DaemonResponseKind,
    coordination: DaemonCoordination,
) -> DaemonResponse {
    let mut response = daemon_response_base(kind);
    response.coordination = Some(coordination);
    response
}

pub(crate) fn daemon_plugin_tools(plugin_tools: Vec<McpToolDescriptor>) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginMcpTools);
    response.plugin_tools = plugin_tools
        .into_iter()
        .map(|tool| serde_json::to_value(tool).unwrap_or(Value::Null))
        .collect();
    response
}

pub(crate) fn daemon_plugin_tool_result(plugin_tool_result: Value) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginMcpToolResult);
    response.plugin_tool_result = plugin_tool_result;
    response
}

pub(crate) fn daemon_plugin_surface(plugin_surface: HubClientPluginSurface) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginSurface);
    let body = plugin_surface.body;
    response.plugin_surface = Some(DaemonPluginSurface {
        package_name: plugin_surface.package_name.clone(),
        surface_id: plugin_surface.surface_id.clone(),
        body: body.clone(),
        ui_tree_snapshot: Some(DaemonUiTreeSnapshot {
            package_name: plugin_surface.package_name,
            surface_id: plugin_surface.surface_id,
            body,
        }),
    });
    response
}

pub(crate) fn daemon_plugin_action_result(plugin_action_result: UiActionResult) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::PluginActionResult);
    if matches!(
        plugin_action_result.state,
        UiActionResultState::Rejected | UiActionResultState::Error
    ) {
        response.diagnostics = vec![DaemonDiagnostic::action_failure(
            "plugin_surface_action",
            "plugin surface action did not complete successfully",
        )];
    }
    response.plugin_action_result = Some(plugin_action_result);
    response
}
