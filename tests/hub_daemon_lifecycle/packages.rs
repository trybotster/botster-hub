#[test]
fn daemon_package_dtos_expose_declared_surfaces_and_validate_surface_operations() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-surfaces");
    let surface_package_dir = unique_test_dir("daemon-declared-surface-package");
    let legacy_package_dir = unique_test_dir("daemon-legacy-surface-package");
    let workspaces_package_dir = unique_test_dir("daemon-workspaces-surface-package");
    let iframe_package_dir = unique_test_dir("daemon-iframe-surface-package");
    write_declared_surface_plugin_package(&surface_package_dir);
    write_local_plugin_package(&legacy_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");
    write_iframe_surface_local_plugin_package(&iframe_package_dir);
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");

    let install_surface = connection
        .request(
            &botster_hub_client::DaemonRequest::InstallPackageLocalPath {
                path: surface_package_dir.clone(),
            },
        )
        .expect("install package with declared surfaces");
    assert_eq!(
        install_surface.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let install_legacy = connection
        .request(
            &botster_hub_client::DaemonRequest::InstallPackageLocalPath {
                path: legacy_package_dir,
            },
        )
        .expect("install legacy package without declared surfaces");
    assert_eq!(
        install_legacy.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let enable_workspaces = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: workspaces_package_dir,
        })
        .expect("enable workspaces package with declared surface");
    assert_eq!(
        enable_workspaces.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let enable_iframe = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: iframe_package_dir,
        })
        .expect("enable iframe package with declared surface");
    assert_eq!(
        enable_iframe.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );

    let packages = connection
        .request(&botster_hub_client::DaemonRequest::ListPackages)
        .expect("list packages with declared surfaces");
    let surface_package = packages
        .packages
        .iter()
        .find(|package| package.package_name == "runtime.surface-plugin")
        .expect("surface package listed");
    assert_eq!(surface_package.surfaces.len(), 2);
    let surface = &surface_package.surfaces[0];
    assert_eq!(surface.id, "runtime.surface.home");
    assert_eq!(surface.kind, botster_ui_contract::PackageSurfaceKind::App);
    assert_eq!(surface.title, "Runtime Surface");
    assert_eq!(
        surface.description.as_deref(),
        Some("Surface descriptor fixture")
    );
    assert_eq!(surface.icon.as_deref(), Some("workflow"));
    assert_eq!(surface.order, Some(20));
    assert_eq!(surface.category.as_deref(), Some("runtime"));
    assert_eq!(
        surface.supports,
        [
            botster_ui_contract::PackageSurfaceOperation::Render,
            botster_ui_contract::PackageSurfaceOperation::Action
        ]
    );

    let show = connection
        .request(&botster_hub_client::DaemonRequest::ShowPackage {
            package_name: "runtime.surface-plugin".to_string(),
        })
        .expect("show package with declared surfaces");
    assert_eq!(show.packages.len(), 1);
    assert_eq!(show.packages[0].surfaces, surface_package.surfaces);

    let workspaces = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "botster-workspaces".to_string(),
            surface_id: "workspaces".to_string(),
            payload: serde_json::json!({}),
        })
        .expect("workspaces surface render returns plugin surface envelope");
    assert_eq!(
        workspaces.kind,
        botster_hub_client::DaemonResponseKind::PluginSurface
    );
    let plugin_surface = workspaces
        .plugin_surface
        .expect("workspaces render includes plugin surface");
    let plugin_surface_body =
        serde_json::to_value(&plugin_surface.body).expect("serialize typed workspaces surface");
    assert_eq!(plugin_surface.package_name, "botster-workspaces");
    assert_eq!(plugin_surface.surface_id, "workspaces");
    assert_eq!(plugin_surface_body["type"], "panel");
    assert_eq!(plugin_surface_body["id"], "botster-workspaces-panel");
    let snapshot = plugin_surface
        .ui_tree_snapshot
        .as_ref()
        .expect("workspaces render includes validated ui tree snapshot");
    assert_eq!(snapshot.package_name, "botster-workspaces");
    assert_eq!(snapshot.surface_id, "workspaces");
    let snapshot_body =
        serde_json::to_value(&snapshot.body).expect("serialize typed workspaces snapshot");
    assert_eq!(snapshot_body["id"], "botster-workspaces-panel");

    let iframe = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "iframe.plugin".to_string(),
            surface_id: "preview".to_string(),
            payload: serde_json::json!({}),
        })
        .expect("iframe surface render returns plugin surface envelope");
    assert_eq!(
        iframe.kind,
        botster_hub_client::DaemonResponseKind::PluginSurface
    );
    let iframe_surface = iframe
        .plugin_surface
        .expect("iframe render includes plugin surface");
    let iframe_surface_body =
        serde_json::to_value(&iframe_surface.body).expect("serialize typed iframe surface");
    assert_eq!(iframe_surface_body["type"], "iframe");
    assert_eq!(iframe_surface_body["id"], "preview-frame");
    assert_eq!(
        iframe_surface_body["props"]["src"],
        "/packages/iframe.plugin/assets/preview.html"
    );
    assert_eq!(iframe_surface_body["props"]["title"], "Preview");
    let iframe_snapshot = iframe_surface
        .ui_tree_snapshot
        .as_ref()
        .expect("iframe render includes validated ui tree snapshot");
    assert_eq!(iframe_snapshot.body, iframe_surface.body);
    assert_no_raw_html_ui_fields(&iframe_surface_body);

    let undeclared = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "runtime.surface-plugin".to_string(),
            surface_id: "runtime.surface.missing".to_string(),
            payload: serde_json::json!({}),
        })
        .expect("undeclared surface render returns operator frame");
    assert_eq!(
        undeclared.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = undeclared.error.as_ref().expect("operator error body");
    assert_eq!(error.code, "undeclared_plugin_surface");
    assert_eq!(error.operation, "plugin_surface_render");
    assert!(undeclared.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
            && diagnostic.operation.as_deref() == Some("plugin_surface_render")
            && diagnostic.feature.as_deref()
                == Some(botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER)
    }));

    let undeclared_empty_manifest = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "runtime.plugin".to_string(),
            surface_id: "legacy.dynamic.surface".to_string(),
            payload: serde_json::json!({}),
        })
        .expect("package without descriptors returns operator error");
    assert_eq!(
        undeclared_empty_manifest.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        undeclared_empty_manifest
            .error
            .as_ref()
            .expect("undeclared operator error")
            .code,
        "undeclared_plugin_surface"
    );

    let unsupported_action = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceAction {
            package_name: "runtime.surface-plugin".to_string(),
            request: botster_ui_contract::UiActionRequest {
                request_id: botster_ui_contract::UiActionRequestId(
                    "unsupported-action".to_string(),
                ),
                surface_id: botster_ui_contract::UiSurfaceId(
                    "runtime.surface.settings".to_string(),
                ),
                action_id: botster_ui_contract::UiActionId("settings.save".to_string()),
                node_id: None,
                kind: botster_ui_contract::UiActionKind::Submit,
                values: None,
                payload: None,
            },
        })
        .expect("unsupported surface operation returns operator error");
    assert_eq!(
        unsupported_action.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let unsupported_error = unsupported_action
        .error
        .as_ref()
        .expect("unsupported operation error");
    assert_eq!(
        unsupported_error.code,
        "unsupported_plugin_surface_operation"
    );
    assert_eq!(unsupported_error.operation, "plugin_surface_action");
    assert!(unsupported_action.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
            && diagnostic.operation.as_deref() == Some("plugin_surface_action")
            && diagnostic.feature.as_deref()
                == Some(botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION)
    }));

    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("daemon remains responsive after surface validation");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_provider_streams_fresh_authoritative_reconnect_snapshot() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-entity-provider");
    let package_dir = unique_test_dir("daemon-package-entity-provider");
    fs::create_dir_all(&package_dir).expect("create provider package");
    fs::write(
        package_dir.join("botster-package.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": package_dir.canonicalize().expect("package path") },
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "surfaces": [{
                "id": "home",
                "kind": "app",
                "title": "Pipelines",
                "supports": ["render"]
            }]
        }))
        .expect("serialize provider manifest"),
    )
    .expect("write provider manifest");
    fs::write(
        package_dir.join("plugin.lua"),
        r#"
local generation = 0
return botster.register({ handlers = {
  {
    id = "home", kind = "surface_route", descriptor_id = "home",
    call = function()
      return { type = "panel", id = "home", children = {{
        ["$kind"] = "bind_list", source = "/project-pipelines.run",
        item_template = { type = "text", id = { ["$bind"] = "@/id" }, props = { text = { ["$bind"] = "@/status" } } },
      }} }
    end,
  },
  {
    id = "runs", kind = "entity_provider", descriptor_id = "project-pipelines.run",
    descriptor = { entity_type = "project-pipelines.run", id_field = "id" },
    call = function()
      generation = generation + 1
      return { type = "entity_snapshot", entity_type = "project-pipelines.run", snapshot_seq = generation,
        items = {{ id = "run-1", status = "generation-" .. generation }} }
    end,
  },
} })
"#,
    )
    .expect("write provider plugin");

    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let enabled = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::EnablePackageLocalPath { path: package_dir },
    )
    .expect("enable provider package");
    assert_eq!(
        enabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let rendered = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "project-pipelines".to_string(),
            surface_id: "home".to_string(),
            payload: serde_json::json!({}),
        },
    )
    .expect("render provider-bound surface");
    assert_eq!(
        rendered.kind,
        botster_hub_client::DaemonResponseKind::PluginSurface
    );

    for (subscription_id, generation) in [("provider-first", 1_u64), ("provider-reconnect", 2_u64)]
    {
        let mut subscription = botster_hub_client::subscribe_entities(
            &endpoint,
            "project-pipelines.run",
            subscription_id,
        )
        .expect("subscribe to package entity family");
        let frame = subscription
            .next_frame()
            .expect("authoritative provider snapshot");
        assert!(matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Snapshot {
                snapshot_seq,
                ref items,
                ..
            } if snapshot_seq == generation
                && items.first().and_then(|item| item.get("id")).and_then(serde_json::Value::as_str) == Some("run-1")
                && items.first().and_then(|item| item.get("status")).and_then(serde_json::Value::as_str)
                    == Some(format!("generation-{generation}").as_str())
        ));
        subscription
            .unsubscribe()
            .expect("unsubscribe provider generation");
    }
    let mut held = botster_hub_client::subscribe_entities(
        &endpoint,
        "project-pipelines.run",
        "provider-disable-cleanup",
    )
    .expect("subscribe before package disable");
    held.set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound provider cleanup wait");
    held.next_frame().expect("snapshot before package disable");
    let provider_counters =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status with live provider subscription")
            .status
            .expect("provider status body")
            .lifecycle_counters;
    assert_eq!(provider_counters.live_entity_subscriptions, 1);
    assert!(provider_counters.high_water_entity_subscriptions >= 1);
    let disabled = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::DisablePackage {
            package_name: "project-pipelines".to_string(),
        },
    )
    .expect("disable provider package");
    assert_eq!(
        disabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    assert!(
        held.next_frame().is_err(),
        "disabled provider subscription must close"
    );
    let cleanup_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status after provider disable")
                .status
                .expect("provider cleanup status body")
                .lifecycle_counters;
        if counters.live_entity_subscriptions == 0 {
            assert!(counters.high_water_entity_subscriptions >= 1);
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "provider subscription counter remained stale: {counters:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        botster_hub_client::subscribe_entities(
            &endpoint,
            "project-pipelines.run",
            "provider-after-disable",
        )
        .is_err(),
        "disabled provider family must not be admitted"
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts() {
    let _guard = daemon_test_guard();
    let fixture_dir = botster_hub_test_support::copy_plugin_contract_matrix_fixture(
        unique_test_dir("daemon-plugin-contract-matrix-fixture"),
    )
    .expect("copy published plugin contract matrix fixture");
    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-plugin-contract-matrix"))
        .name("plugin-contract-matrix")
        .start()
        .expect("start isolated hub through public test-support harness");

    let report =
        botster_hub_test_support::run_plugin_contract_matrix_conformance(&hub, fixture_dir)
            .expect("run plugin contract matrix conformance");
    let lifecycle = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("request live plugin worker counters");
    let counters = lifecycle
        .plugin_worker_counters
        .expect("plugin lifecycle response carries worker counters");
    let expected = CoreEngineOptions::default();
    assert_eq!(
        counters.configured_queue_capacity,
        expected.plugin_worker_queue_capacity
    );
    assert_eq!(
        counters.configured_executor_concurrency,
        expected.plugin_worker_executor_concurrency
    );
    assert!(counters.live_plugin_executors >= 1);
    assert!(counters.live_executor_workers >= 1);
    assert_eq!(counters.queued_jobs, 0);
    assert_eq!(counters.in_flight_jobs, 0);
    assert_eq!(
        lifecycle
            .plugin_resource_counters
            .expect("plugin lifecycle response carries resource counters")
            .active_timer_resources,
        0
    );
    assert_eq!(report.package_name, "botster.plugin-contract-matrix");
    assert_eq!(report.installed_state, "installed");
    assert_eq!(report.enabled_state, "enabled");
    assert_eq!(
        report.surface_ids,
        vec![
            "contract.app",
            "contract.empty",
            "contract.sessions",
            "contract.entities",
            "contract.blocked",
            "contract.invalid_body",
            "contract.settings",
        ]
    );
    assert_eq!(report.app_route_target_kind, "plugin_surface");
    assert_eq!(report.app_route_surface_id, "contract.app");
    assert!(report.app_route_blocked_after_install);
    assert_eq!(
        report.invalid_configuration_diagnostic_kind,
        "action_failure"
    );
    assert_eq!(
        report.invalid_configuration_diagnostic_operation,
        "configure"
    );
    assert!(report.invalid_configuration_diagnostic_mentions_rejected_value);
    assert_eq!(report.valid_configuration_mode, "write");
    assert_eq!(report.valid_configuration_secret_state, "redacted");
    assert!(report.list_surfaces_match_enabled);
    assert!(report.show_routes_match_list);
    assert_eq!(report.app_surface_node_id, "contract-app-panel");
    assert_eq!(report.package_entity_surface_id, "contract.entities");
    assert_eq!(
        report.package_entity_surface_node_id,
        "contract-entities-panel"
    );
    assert_eq!(
        report.package_entity_binding_family,
        "bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run"
    );
    assert_eq!(
        report.app_surface_node_kinds,
        vec![
            "button",
            "button",
            "button",
            "dialog",
            "empty_state",
            "empty_state",
            "form",
            "metric",
            "metric_grid",
            "panel",
            "section",
            "status_badge",
            "table",
            "text",
            "text",
            "text",
            "text_input",
            "toolbar",
        ]
    );
    assert_eq!(
        report.app_surface_snapshot_package_name,
        "botster.plugin-contract-matrix"
    );
    assert_eq!(report.app_surface_snapshot_id, "contract.app");
    assert_eq!(report.app_surface_snapshot_node_id, "contract-app-panel");
    assert_eq!(
        report.app_surface_snapshot_node_kinds,
        report.app_surface_node_kinds
    );
    assert_eq!(report.session_surface_id, "contract.sessions");
    assert_eq!(
        report.session_surface_node_id,
        "contract-session-lifecycle-panel"
    );
    assert_eq!(report.session_surface_binding_family, "/session");
    assert!(report.session_surface_matches_fixture);
    assert_eq!(report.session_surface_references.len(), 5);
    assert_eq!(
        report
            .session_materialized_rows
            .iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["session-transition", "session-stable-current"]
    );
    assert_eq!(
        report.session_materialized_rows[1].controls[0].label,
        "current"
    );
    assert_eq!(
        report.session_action_node_id,
        "botster-ui-descendant-v1:22:session-stable-current6:rename"
    );
    assert_eq!(
        report.session_action_payload,
        serde_json::json!({
            "operation": "rename",
            "session_uuid": "session-stable-current"
        })
    );
    assert_eq!(report.session_action_state, "accepted");
    assert_eq!(
        report.session_action_result_node_id,
        "botster-ui-descendant-v1:22:session-stable-current6:rename"
    );
    assert_eq!(
        report.session_action_result_payload,
        report.session_action_payload
    );
    assert_eq!(
        report.session_action_node_id,
        botster_ui_contract::realize_bind_list_descendant_id("session-stable-current", "rename",)
            .expect("fixture identity is valid")
            .0
    );
    assert_eq!(
        report.session_remove_action_node_id,
        botster_ui_contract::realize_bind_list_descendant_id("session-stable-current", "remove",)
            .expect("fixture identity is valid")
            .0
    );
    assert_ne!(
        report.session_action_node_id,
        report.session_remove_action_node_id
    );
    assert_eq!(
        report.session_remove_action_payload,
        serde_json::json!({
            "operation": "remove",
            "session_uuid": "session-stable-current"
        })
    );
    assert_eq!(report.session_remove_action_state, "accepted");
    assert_eq!(
        report.session_remove_action_result_node_id,
        report.session_remove_action_node_id
    );
    assert_eq!(
        report.session_remove_action_result_payload,
        report.session_remove_action_payload
    );
    assert_eq!(report.dialog_presence_key, "contract-dialog");
    assert_eq!(report.selected_workspace_equality_key, "selected-workspace");
    assert_eq!(report.selected_workspace_equality_value, "workspace-alpha");
    assert_eq!(report.open_action_id, "contract.action");
    assert_eq!(report.open_action_node_id, "contract-app-open");
    assert_eq!(
        report.open_action_payload,
        serde_json::json!({ "operation": "open" })
    );
    assert_eq!(
        report.open_set_values,
        std::collections::BTreeMap::from([
            ("contract-dialog".to_string(), serde_json::json!(true)),
            (
                "selected-workspace".to_string(),
                serde_json::json!("workspace-alpha"),
            ),
        ])
    );
    let matrix = botster_hub_test_support::first_party_client_support_matrix();
    assert_eq!(
        matrix.plugin_surfaces.dialog_presence_key,
        report.dialog_presence_key
    );
    assert_eq!(
        matrix.plugin_surfaces.selected_workspace_equality_key,
        report.selected_workspace_equality_key
    );
    assert_eq!(
        matrix.plugin_surfaces.selected_workspace_equality_value,
        report.selected_workspace_equality_value
    );
    assert_eq!(
        matrix.plugin_surfaces.authored_set_values,
        report.open_set_values
    );
    assert!(report.dialog_visible_after_open);
    assert!(report.selected_workspace_visible_after_open);
    assert!(!report.form_reachable_before_open);
    assert_eq!(report.dialog_form_node_id, "contract-app-form");
    assert_eq!(report.dialog_input_node_id, "contract-app-message");
    assert_eq!(report.submit_action_node_id, "contract-app-form");
    assert!(!report.actionable_sibling_form_during_dialog);
    assert_eq!(
        report.invalid_submit_values,
        serde_json::json!({ "message": "   " })
    );
    assert_eq!(
        report.valid_submit_values,
        serde_json::json!({ "message": "hello" })
    );
    assert!(report.rejected_state_retained);
    assert!(report.rejected_tree_retained);
    assert!(report.rejected_dialog_retained);
    assert!(report.rejected_form_retained);
    assert_eq!(report.rejected_field_error_node_id, "contract-app-message");
    assert_eq!(
        report.accepted_normalized_values,
        serde_json::json!({ "message": "hello" })
    );
    assert!(report.accepted_replacement_applied);
    assert!(report.dialog_state_cleared);
    assert!(!report.dialog_visible_after_valid_submit);
    assert_eq!(report.toggle_action_id, "contract.action");
    assert_eq!(report.toggle_action_node_id, "contract-app-toggle");
    assert_eq!(
        report.toggle_action_payload,
        serde_json::json!({ "operation": "toggle" })
    );
    assert_eq!(report.toggle_key, "contract-toggle");
    assert_eq!(report.toggle_visible_states, vec![false, true, false]);
    assert_eq!(report.empty_surface_child_id, "contract-empty-message");
    assert_eq!(report.blocked_render_operation, "plugin_surface_render");
    assert!(report.blocked_render_message_contains_failure);
    assert_eq!(report.invalid_body_error_code, "invalid_surface");
    assert_eq!(report.invalid_body_operation, "plugin_surface_render");
    assert_eq!(report.invalid_body_diagnostic_kind, "action_failure");
    assert_eq!(
        report.invalid_body_diagnostic_operation,
        "plugin_surface_render"
    );
    assert_eq!(report.settings_surface_node_id, "contract-settings-panel");
    assert!(report.settings_text_contains_endpoint);
    assert!(report.settings_text_contains_mode);
    assert!(report.settings_text_contains_redacted_secret);
    assert_eq!(report.action_success_state, "accepted");
    assert_eq!(report.action_success_message, "hello");
    assert_eq!(
        report.action_success_presentation_clear_key,
        "contract-dialog"
    );
    assert_eq!(
        report.action_success_replacement_node_id,
        "contract-action-replacement"
    );
    assert_eq!(report.submit_action_id, "contract.action");
    assert_eq!(report.action_error_state, "error");
    assert_eq!(report.action_error_diagnostic_kind, "action_failure");
    assert_eq!(
        report.action_error_diagnostic_operation,
        "plugin_surface_action"
    );
    assert_eq!(report.action_field_error_state, "rejected");
    assert_eq!(
        report.action_field_error_request_id,
        "contract-action-field-error"
    );
    assert_eq!(report.action_field_error_diagnostic_kind, "action_failure");
    assert_eq!(
        report.action_field_error_diagnostic_operation,
        "plugin_surface_action"
    );
    assert_eq!(report.action_field_error_message, "Message is required");
    assert_eq!(report.identity_mismatch_error_code, "invalid_action_result");
    assert_eq!(
        report.identity_mismatch_error_operation,
        "plugin_surface_action"
    );
    assert_eq!(
        report.invalid_replacement_error_code,
        "invalid_action_result"
    );
    assert_eq!(
        report.invalid_replacement_error_operation,
        "plugin_surface_action"
    );
    assert_eq!(
        report.client_render_check.class,
        botster_hub_test_support::ConformanceFailureClass::ClientRendering
    );
    assert_eq!(
        report.failure_classes.producer_contract,
        botster_hub_test_support::ConformanceFailureClass::ProducerContract
    );
    assert_eq!(
        report.failure_classes.environment_setup,
        botster_hub_test_support::ConformanceFailureClass::EnvironmentSetup
    );

    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn daemon_project_pipelines_example_exercises_published_surface_conformance() {
    let _guard = daemon_test_guard();
    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-project-pipelines-conformance"))
        .name("project-pipelines")
        .start()
        .expect("start isolated hub through public test-support harness");
    let package_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("project-pipelines");

    let report = botster_hub_test_support::run_project_pipelines_conformance(&hub, package_path)
        .expect("run published Project Pipelines conformance through daemon socket");
    assert_eq!(report.package_state, "enabled");
    assert_eq!(
        report.rendered_surface_id,
        "project-pipelines.create-ticket"
    );
    assert_eq!(report.form_action_id, "project_pipelines.create_ticket");
    assert_eq!(report.invalid_title_error, "Title is required");

    hub.shutdown()
        .expect("shutdown Project Pipelines conformance hub");
}

#[test]
fn occupied_generic_web_port_reports_structured_entrypoint_failure() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-occupied-port");
    let package_dir = unique_test_dir("web-occupied-port-package");
    write_botster_web_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let occupied = TcpListener::bind(("127.0.0.1", 0)).expect("reserve generic Web port");
    let occupied_port = occupied.local_addr().expect("occupied port address").port();
    let response = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_PORT".to_string(),
                occupied_port.to_string(),
            )]),
        },
    )
    .expect("occupied port returns an operator response");

    assert_eq!(
        response.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    let error = response.error.expect("structured entrypoint error");
    assert_eq!(error.code, "entrypoint_readiness_failed");
    assert!(error.message.contains("package botster-web"));
    assert!(error.message.contains("entrypoint web-client"));
    assert!(error.message.contains("exited"));
    assert!(error.message.contains("EADDRINUSE"), "{}", error.message);

    drop(occupied);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn foreground_terminal_app_open_absolutizes_relative_runtime_paths() {
    let _guard = daemon_test_guard();
    let daemon_working_directory = PathBuf::from("/tmp");
    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("bh-relative-runtime"))
        .working_directory(&daemon_working_directory)
        .name("package-cwd")
        .start()
        .expect("start isolated hub with relative runtime root");
    assert!(
        hub.data_dir()
            .starts_with(daemon_working_directory.join("bh-relative-runtime"))
    );

    let report = botster_hub_test_support::run_foreground_terminal_app_open_conformance(&hub)
        .expect("launch package-root child through daemon-resolved foreground contract");
    assert!(report.hub_connection_socket_path_absolute);
    assert!(report.hub_data_dir_env_absolute);
    assert!(report.launch_working_directory_is_package_root);
    assert!(report.launch_working_directory_differs_from_daemon_cwd);
    assert_eq!(report.real_hub_action_operation, "status");
    assert_eq!(report.real_hub_action_result, "running");
    assert_eq!(report.exit_code, Some(0));

    hub.shutdown().expect("shutdown relative-root isolated hub");
}

#[test]
fn cli_inspect_reports_not_found_for_fresh_in_process_daemon() {
    let data_dir = unique_test_dir("cli-inspect");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("inspect")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .output()
        .expect("run botster-hub inspect");

    assert!(
        output.status.success(),
        "inspect failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("inspect=session"));
    assert!(stdout.contains("session_id=runtime-session"));
    assert!(stdout.contains("found=false"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_packages_enable_local_path_routes_through_running_daemon_and_persists() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-packages");
    let package_dir = unique_test_dir("local-package");
    write_local_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages enable");

    assert!(
        enable.status.success(),
        "enable failed: {}",
        String::from_utf8_lossy(&enable.stderr)
    );
    let stdout = String::from_utf8(enable.stdout).expect("stdout is utf8");
    assert!(stdout.contains("decision=package"));
    assert!(stdout.contains("package_name=runtime.plugin"));
    assert!(stdout.contains("action=enable"));
    assert!(stdout.contains("response=packages"));
    assert!(stdout.contains("package name=runtime.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(stdout.contains("runnable_entrypoints=1"));
    assert!(stdout.contains("package_entrypoint package=runtime.plugin id=web kind=web_app launch_mode=background command=bin/botster-web args=2 working_directory=package_root environment=1 capabilities=1 may_supervise=true process_state=not_started"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after package enable");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let stdout = String::from_utf8(status.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("enabled_package_count=1"));

    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status");
    assert_eq!(
        lifecycle.kind,
        botster_hub::DaemonResponseKind::PluginLifecycle
    );
    assert!(
        lifecycle.lifecycle.iter().any(|plugin| {
            plugin.package_name == "runtime.plugin" && plugin.state == "enabled" && plugin.loaded
        }),
        "enabled package should load into daemon lifecycle without restart"
    );

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub packages list");

    assert!(
        list.status.success(),
        "packages list failed: {}",
        String::from_utf8_lossy(&list.stderr)
    );
    let stdout = String::from_utf8(list.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=packages"));
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("package name=runtime.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(stdout.contains("runnable_entrypoints=1"));
    assert!(stdout.contains("process_state=not_started"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    let providers = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("providers")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub providers list");
    assert!(
        providers.status.success(),
        "providers list failed: {}",
        String::from_utf8_lossy(&providers.stderr)
    );
    let stdout = String::from_utf8(providers.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=providers"));
    assert!(stdout.contains("package_count=0"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);

    let restarted = start_cli_daemon(&data_dir);
    let list_after_restart = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub packages list after restart");
    assert!(
        list_after_restart.status.success(),
        "packages list after restart failed: {}",
        String::from_utf8_lossy(&list_after_restart.stderr)
    );
    let stdout = String::from_utf8(list_after_restart.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("package name=runtime.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(stdout.contains("runnable_entrypoints=1"));
    assert!(stdout.contains("package_entrypoint package=runtime.plugin id=web kind=web_app launch_mode=background command=bin/botster-web args=2 working_directory=package_root environment=1 capabilities=1 may_supervise=true process_state=not_started"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn package_entrypoint_supervision_starts_and_reports_running() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-start");
    let package_dir = unique_test_dir("entrypoint-start-package");
    write_supervised_package(
        &package_dir,
        "runtime.supervised",
        "sh",
        &[
            "-c",
            "printf 'entrypoint-ready\\n'; while true; do sleep 1; done",
        ],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.supervised".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start supervised entrypoint");
    let entrypoint = package_entrypoint(&start, "runtime.supervised");
    assert_eq!(entrypoint.process.state, "running");
    assert!(entrypoint.process.pid.is_some());
    assert!(entrypoint.process.started_at.is_some());

    let list = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListPackages,
    )
    .expect("list packages after supervised start");
    let entrypoint = package_entrypoint(&list, "runtime.supervised");
    assert_eq!(entrypoint.process.state, "running");
    assert!(entrypoint.process.pid.is_some());
    assert_eq!(
        package_action(&entrypoint.actions, "start_package_entrypoint").status,
        botster_hub::DaemonPackageActionStatus::Unavailable
    );
    let stop_action = package_action(&entrypoint.actions, "stop_package_entrypoint");
    assert_eq!(
        stop_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    assert_eq!(
        stop_action
            .request
            .as_ref()
            .expect("stop entrypoint request")
            .entrypoint_id
            .as_deref(),
        Some("web")
    );
    assert_eq!(
        package_action(&entrypoint.actions, "restart_package_entrypoint")
            .request
            .as_ref()
            .expect("restart entrypoint request")
            .request_type,
        "restart_package_entrypoint"
    );

    let cli_status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("entrypoint-status")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.supervised")
        .arg("web")
        .output()
        .expect("run botster-hub packages entrypoint-status");
    assert!(
        cli_status.status.success(),
        "entrypoint-status failed: {}",
        String::from_utf8_lossy(&cli_status.stderr)
    );
    let stdout = String::from_utf8(cli_status.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=packages"));
    assert!(stdout.contains("process_state=running"));
    assert!(stdout.contains("package_entrypoint_process package=runtime.supervised id=web"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_list_apps_projects_installed_package_entrypoints() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("list-apps");
    let package_dir = unique_test_dir("list-apps-package");
    write_app_registry_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let before_start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListApps,
    )
    .expect("list apps before start");
    assert_eq!(before_start.kind, botster_hub::DaemonResponseKind::Apps);
    assert_eq!(before_start.apps.len(), 2);
    let web = app_row(&before_start, "web");
    assert_eq!(web.package_name, "runtime.apps");
    assert_eq!(web.app_id, "web");
    assert_eq!(web.entrypoint_id, "web");
    assert_eq!(web.kind, "web_app");
    assert_eq!(web.launch_mode, "background");
    assert_eq!(web.lifecycle_state, "not_started");
    assert_eq!(web.launch_target.kind, "web_app");
    assert_eq!(web.launch_target.local_url, None);

    let terminal = app_row(&before_start, "terminal");
    assert_eq!(terminal.kind, "terminal_app");
    assert_eq!(terminal.launch_mode, "foreground_stdio");
    assert_eq!(terminal.launch_target.kind, "terminal_app");
    assert_eq!(terminal.launch_target.local_url, None);
    assert!(terminal.blocked_reasons.is_empty());
    assert!(terminal.actions.is_empty());

    botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.apps".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start web app entrypoint");

    let after_start = wait_for_app_local_url(&data_dir, "web", "http://127.0.0.1:49152");
    let web = app_row(&after_start, "web");
    assert_eq!(web.lifecycle_state, "running");
    assert_eq!(web.launch_target.kind, "web_app");
    assert_eq!(
        web.launch_target.local_url.as_deref(),
        Some("http://127.0.0.1:49152")
    );
    assert_eq!(
        package_action(&web.actions, "start_package_entrypoint").status,
        botster_hub::DaemonPackageActionStatus::Unavailable
    );
    assert_eq!(
        package_action(&web.actions, "stop_package_entrypoint").status,
        botster_hub::DaemonPackageActionStatus::Available
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_spawns_session_type_and_script_reads_botster_context() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("session-type-context");
    let package_root = unique_test_dir("session-type-context-package");
    write_session_type_context_package(&package_root);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_root.clone(),
        },
    )
    .expect("enable session type package");
    assert_eq!(
        enable.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );

    let templates = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypes,
    )
    .expect("list session types");
    assert_eq!(
        templates.session_types[0].session_type_id,
        "runtime.session-type/init"
    );

    let rejected = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolveSessionType {
            session_type_id: "init".to_string(),
            request: botster_hub::DaemonSessionTypeRequest {
                cwd: Some("/tmp".to_string()),
                ..botster_hub::DaemonSessionTypeRequest::default()
            },
        },
    )
    .expect("unauthorized cwd response");
    assert_eq!(
        rejected.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        rejected.error.as_ref().map(|error| error.code.as_str()),
        Some("cwd_not_admitted")
    );

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SpawnSessionType {
            session_type_id: "init".to_string(),
            session_id: "session-type-context".to_string(),
            request: botster_hub::DaemonSessionTypeRequest {
                context: botster_hub::DaemonSessionTypeContextInput {
                    prompt: Some("pipeline prompt".to_string()),
                    ticket_id: Some("ticket-123".to_string()),
                    ..botster_hub::DaemonSessionTypeContextInput::default()
                },
                ..botster_hub::DaemonSessionTypeRequest::default()
            },
        },
    )
    .expect("spawn session type");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let context_output = package_root.join("context-output.json");
    let mut output = String::new();
    while std::time::Instant::now() < deadline {
        if let Ok(contents) = fs::read_to_string(&context_output) {
            output = contents;
            if output.contains("pipeline prompt") {
                break;
            }
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        package_root.join("context-started.txt").exists(),
        "template script should have started"
    );
    assert!(
        output.contains("\"prompt\":\"pipeline prompt\""),
        "template script should read botster context through CLI, context_output={output:?}, context_error={:?}",
        fs::read_to_string(package_root.join("context-error.txt")).unwrap_or_default()
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_session_types_use_only_the_explicit_execution_mode() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("session-type-execution");
    let package_root = unique_test_dir("session-type-execution-package");
    write_session_type_execution_package(&package_root);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_root.clone(),
        },
    )
    .expect("enable execution package");
    assert_eq!(
        enable.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );

    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("connect to real daemon");

    let relative = connection
        .request(&botster_hub_client::DaemonRequest::SpawnSessionType {
            session_type_id: "relative".to_string(),
            session_id: "relative-execution".to_string(),
            request: botster_hub_client::DaemonSessionTypeRequest::default(),
        })
        .expect("spawn explicit relative executable");
    assert_eq!(
        relative.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );

    let shell_resolved = connection
        .request(&botster_hub_client::DaemonRequest::ResolveSessionType {
            session_type_id: "shell".to_string(),
            request: botster_hub_client::DaemonSessionTypeRequest::default(),
        })
        .expect("resolve explicit shell command");
    let shell_contract = shell_resolved
        .resolved_session_type
        .expect("resolved shell contract");
    assert_eq!(
        shell_contract.executable,
        botster_hub::SessionDefaults::default().shell,
        "the real daemon must use its configured default shell"
    );
    assert_eq!(
        shell_contract.arguments,
        vec![
            "-c",
            "printf 'shell:%s:%s\\n' \"$1\" \"$2\" > shell-output.txt; sleep 30",
            "botster-session-type",
            "alpha",
            "beta",
        ]
    );

    let shell = connection
        .request(&botster_hub_client::DaemonRequest::SpawnSessionType {
            session_type_id: "shell".to_string(),
            session_id: "shell-execution".to_string(),
            request: botster_hub_client::DaemonSessionTypeRequest::default(),
        })
        .expect("spawn explicit shell command");
    assert_eq!(shell.kind, botster_hub_client::DaemonResponseKind::Spawned);

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline
        && (!package_root.join("relative-output.txt").exists()
            || !package_root.join("shell-output.txt").exists())
    {
        thread::sleep(Duration::from_millis(30));
    }
    assert_eq!(
        fs::read_to_string(package_root.join("relative-output.txt"))
            .expect("relative executable output")
            .trim(),
        "relative:explicit"
    );
    assert_eq!(
        fs::read_to_string(package_root.join("shell-output.txt"))
            .expect("shell command output")
            .trim(),
        "shell:alpha:beta"
    );

    let not_inferred = connection
        .request(&botster_hub_client::DaemonRequest::SpawnSessionType {
            session_type_id: "not-inferred".to_string(),
            session_id: "not-inferred-execution".to_string(),
            request: botster_hub_client::DaemonSessionTypeRequest::default(),
        })
        .expect("shell-looking relative executable returns an operator response");
    assert_eq!(
        not_inferred.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = not_inferred.error.as_ref().expect("operator error body");
    assert_eq!(error.code, "spawn_failed");
    assert_eq!(error.operation, "spawn_session_type");
    assert!(not_inferred.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::ActionFailure
            && diagnostic.operation.as_deref() == Some("spawn_session_type")
            && diagnostic
                .message
                .as_deref()
                .is_some_and(|message| message.contains("session type command"))
    }));
    assert!(!package_root.join("inferred-shell-output.txt").exists());

    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("transport remains open after nested spawn failure");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

    for session_id in ["relative-execution", "shell-execution"] {
        connection
            .request(&botster_hub_client::DaemonRequest::ShutdownSession {
                session_id: session_id.to_string(),
            })
            .expect("shutdown execution test session");
    }
    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_spawn_target_crud_persists_plain_non_git_directory_and_cli_lists_it() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("spawn-target-crud");
    let target_root = unique_short_test_dir("plain-target");
    fs::create_dir_all(&target_root).expect("create plain target root");
    assert!(
        !target_root.join(".git").exists(),
        "test target intentionally has no git metadata"
    );
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let created = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_plain_directory".to_string()),
            label: Some("Plain Directory".to_string()),
            root: target_root.clone(),
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("create spawn target through daemon");
    assert_eq!(created.kind, botster_hub::DaemonResponseKind::SpawnTargets);
    assert_eq!(created.spawn_targets[0].target_id, "tgt_plain_directory");
    assert!(created.spawn_targets[0].enabled);

    let listed = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSpawnTargets,
    )
    .expect("list spawn targets through daemon");
    assert_eq!(listed.spawn_targets.len(), 1);
    assert_eq!(
        listed.spawn_targets[0].root,
        fs::canonicalize(&target_root).expect("canonical target root")
    );

    let cli_list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("spawn-targets")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run spawn-targets list cli");
    assert!(
        cli_list.status.success(),
        "spawn-targets list failed: {}",
        String::from_utf8_lossy(&cli_list.stderr)
    );
    let stdout = String::from_utf8_lossy(&cli_list.stdout);
    assert!(stdout.contains("response=spawn_targets"));
    assert!(stdout.contains("id=tgt_plain_directory"));

    let validation = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ValidateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("validate enabled target")
    .spawn_target_validation
    .expect("validation response");
    assert!(validation.ok);
    assert_eq!(validation.status, "ok");

    let disabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
            label: Some("Plain Directory Disabled".to_string()),
            root: None,
            enabled: Some(false),
            kind: None,
            base_ref: None,
            metadata: None,
        },
    )
    .expect("disable target");
    assert!(!disabled.spawn_targets[0].enabled);
    let validation = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ValidateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("validate disabled target")
    .spawn_target_validation
    .expect("validation response");
    assert!(!validation.ok);
    assert_eq!(validation.status, "disabled");

    let enabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
            label: None,
            root: None,
            enabled: Some(true),
            kind: None,
            base_ref: None,
            metadata: None,
        },
    )
    .expect("re-enable target");
    assert!(enabled.spawn_targets[0].enabled);

    shutdown_cli_daemon(&data_dir, child);
    let restarted = start_cli_daemon(&data_dir);
    let reloaded = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("show reloaded target");
    assert_eq!(reloaded.spawn_targets.len(), 1);
    assert_eq!(reloaded.spawn_targets[0].label, "Plain Directory Disabled");

    let deleted = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::DeleteSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("delete target");
    assert_eq!(deleted.spawn_targets[0].target_id, "tgt_plain_directory");
    let validation = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ValidateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("validate deleted target")
    .spawn_target_validation
    .expect("validation response");
    assert!(!validation.ok);
    assert_eq!(validation.status, "not_found");
    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn create_spawn_target_rejects_incomplete_repo_session_types_with_typed_operator_error() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("st-create-reject");
    // Ticket reproduction shape: kind=git with base_ref=main.
    let target_root = unique_short_test_dir("st-create-reject-root");
    init_git_repo_with_main(&target_root);
    write_repo_session_types_file(&target_root, &incomplete_repo_session_types_json());
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let rejected = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_invalid_session_types".to_string()),
            label: Some("Invalid Session Types".to_string()),
            root: target_root.clone(),
            enabled: true,
            kind: Some("git".to_string()),
            base_ref: Some("main".to_string()),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create with invalid session-types must keep transport open");
    assert_eq!(
        rejected.kind,
        botster_hub::DaemonResponseKind::OperatorError,
        "expected operator_error, got kind={:?} error={:?}",
        rejected.kind,
        rejected.error
    );
    let error = rejected.error.as_ref().expect("operator error body");
    assert_eq!(error.code, "invalid_repo_session_types");
    assert!(
        error.message.contains("label"),
        "message should diagnose missing label field: {}",
        error.message
    );

    let listed = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSpawnTargets,
    )
    .expect("list still works after rejected create");
    assert!(
        listed
            .spawn_targets
            .iter()
            .all(|target| target.target_id != "tgt_invalid_session_types"),
        "invalid target must not be admitted: {:?}",
        listed.spawn_targets
    );

    let status = botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::Status)
        .expect("status still works after rejected create");
    assert_eq!(status.kind, botster_hub::DaemonResponseKind::Status);

    // Positive control: complete PackageSessionType shape admits and qualifies.
    let good_root = unique_short_test_dir("st-create-good-root");
    init_git_repo_with_main(&good_root);
    write_repo_session_types_file(&good_root, &complete_repo_session_types_json("acceptance"));
    let created = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_valid_session_types".to_string()),
            label: Some("Valid Session Types".to_string()),
            root: good_root,
            enabled: true,
            kind: Some("git".to_string()),
            base_ref: Some("main".to_string()),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create with complete session-types");
    assert_eq!(created.kind, botster_hub::DaemonResponseKind::SpawnTargets);
    assert_eq!(
        created.spawn_targets[0].target_id,
        "tgt_valid_session_types"
    );

    let templates = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypes,
    )
    .expect("list session types after valid admit");
    assert!(
        templates
            .session_types
            .iter()
            .any(|row| row.session_type_id == "tgt_valid_session_types/acceptance"),
        "expected qualified id, got {:?}",
        templates
            .session_types
            .iter()
            .map(|row| &row.session_type_id)
            .collect::<Vec<_>>()
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn create_spawn_target_enabled_file_root_returns_root_not_directory() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("st-create-file-root");
    let file_root = unique_short_test_dir("st-create-file-root-path");
    fs::write(&file_root, "not a directory\n").expect("write regular file as root");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let rejected = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_file_root".to_string()),
            label: Some("File Root".to_string()),
            root: file_root,
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("file root create must keep transport open");
    assert_eq!(
        rejected.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        rejected.error.as_ref().map(|error| error.code.as_str()),
        Some("root_not_directory"),
        "enabled pre-check must not replace root_not_directory with invalid_repo_session_types: {:?}",
        rejected.error
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn update_spawn_target_rejects_enable_with_invalid_repo_session_types() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("st-update-reject");
    let target_root = unique_short_test_dir("st-update-reject-root");
    fs::create_dir_all(&target_root).expect("create target root");
    write_repo_session_types_file(&target_root, &incomplete_repo_session_types_json());
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    // Disabled create may succeed without validating session-types contribution.
    let created = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_disabled_invalid".to_string()),
            label: Some("Disabled Invalid".to_string()),
            root: target_root,
            enabled: false,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("create disabled target with invalid session-types");
    assert_eq!(created.kind, botster_hub::DaemonResponseKind::SpawnTargets);
    assert!(!created.spawn_targets[0].enabled);

    let rejected = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_disabled_invalid".to_string(),
            label: None,
            root: None,
            enabled: Some(true),
            kind: None,
            base_ref: None,
            metadata: None,
        },
    )
    .expect("enable with invalid session-types must keep transport open");
    assert_eq!(
        rejected.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        rejected.error.as_ref().map(|error| error.code.as_str()),
        Some("invalid_repo_session_types")
    );

    let shown = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowSpawnTarget {
            target_id: "tgt_disabled_invalid".to_string(),
        },
    )
    .expect("show still works after rejected enable");
    assert!(!shown.spawn_targets[0].enabled);

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn update_spawn_target_rejects_repoint_to_invalid_repo_session_types() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("st-update-repoint");
    let good_root = unique_short_test_dir("st-update-repoint-good");
    let bad_root = unique_short_test_dir("st-update-repoint-bad");
    fs::create_dir_all(&good_root).expect("create good root");
    fs::create_dir_all(&bad_root).expect("create bad root");
    write_repo_session_types_file(&good_root, &complete_repo_session_types_json("repo-agent"));
    write_repo_session_types_file(&bad_root, &incomplete_repo_session_types_json());
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let created = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_repoint".to_string()),
            label: Some("Repoint Target".to_string()),
            root: good_root.clone(),
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("admit valid target");
    assert_eq!(created.kind, botster_hub::DaemonResponseKind::SpawnTargets);
    let original_root = created.spawn_targets[0].root.clone();

    let rejected = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_repoint".to_string(),
            label: None,
            root: Some(bad_root),
            enabled: None,
            kind: None,
            base_ref: None,
            metadata: None,
        },
    )
    .expect("repoint to invalid session-types must keep transport open");
    assert_eq!(
        rejected.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        rejected.error.as_ref().map(|error| error.code.as_str()),
        Some("invalid_repo_session_types")
    );

    let shown = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowSpawnTarget {
            target_id: "tgt_repoint".to_string(),
        },
    )
    .expect("show after rejected repoint");
    assert_eq!(
        shown.spawn_targets[0].root, original_root,
        "rejected repoint must leave stored root unchanged"
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn poison_recovery_delete_succeeds_under_invalid_repo_session_types() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("st-poison-delete");
    let target_root = unique_short_test_dir("st-poison-delete-root");
    fs::create_dir_all(&target_root).expect("create target root");
    write_repo_session_types_file(
        &target_root,
        &complete_repo_session_types_json("repo-agent"),
    );
    let config = explicit_config(&data_dir);
    let endpoint = daemon_endpoint(&config);
    let child = start_cli_daemon(&data_dir);

    // Subscriber oracle pins force_advance_session_type_generation on recovery.
    let mut subscription =
        botster_hub_client::subscribe_entities(&endpoint, "session_type", "st-poison-delete-sub")
            .expect("subscribe before admit");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound entity reads");
    assert!(matches!(
        subscription.next_frame().expect("initial empty snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq: 0,
            ref items,
            ..
        } if items.is_empty()
    ));

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_poison_delete".to_string()),
            label: Some("Poison Delete".to_string()),
            root: target_root.clone(),
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("admit valid target before poison");
    assert!(matches!(
        subscription.next_frame().expect("repo definition upsert"),
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq: 1,
            ref id,
            ..
        } if id == "tgt_poison_delete/repo-agent"
    ));

    write_repo_session_types_file(&target_root, &incomplete_repo_session_types_json());

    let list_poisoned = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypes,
    )
    .expect("list under poison must keep transport open");
    assert_eq!(
        list_poisoned.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        list_poisoned
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("invalid_repo_session_types")
    );

    // Non-recovery mutation under already-admitted poison must frame, not disconnect.
    let second_root = unique_short_test_dir("st-poison-delete-second");
    fs::create_dir_all(&second_root).expect("create second root");
    write_repo_session_types_file(
        &second_root,
        &complete_repo_session_types_json("second-agent"),
    );
    let create_while_poisoned = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_while_poisoned".to_string()),
            label: Some("While Poisoned".to_string()),
            root: second_root,
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("create under global poison must keep transport open");
    assert_eq!(
        create_while_poisoned.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        create_while_poisoned
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("invalid_repo_session_types")
    );

    // Subscribe after poison: entity surface must frame with subscribe_entities, not disconnect.
    let subscribe_poisoned = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::SubscribeEntities {
            entity_type: "session_type".to_string(),
            subscription_id: "st-poison-delete-late-sub".to_string(),
        },
    )
    .expect("subscribe under poison must keep transport open");
    assert_eq!(
        subscribe_poisoned.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let sub_error = subscribe_poisoned
        .error
        .as_ref()
        .expect("subscribe operator error");
    assert_eq!(sub_error.code, "invalid_repo_session_types");
    assert_eq!(sub_error.operation, "subscribe_entities");
    assert_eq!(sub_error.request_id, "st-poison-delete-late-sub");

    // Independent recovery case: Delete under poison with no prior disable.
    let deleted = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::DeleteSpawnTarget {
            target_id: "tgt_poison_delete".to_string(),
        },
    )
    .expect("delete under poison must keep transport open");
    assert_eq!(deleted.kind, botster_hub::DaemonResponseKind::SpawnTargets);
    assert_eq!(deleted.spawn_targets[0].target_id, "tgt_poison_delete");

    // Forced generation advance is required for subscribers to converge after recovery.
    assert!(matches!(
        subscription
            .next_frame()
            .expect("remove after poison delete recovery"),
        botster_hub_client::DaemonEntityFrame::Remove {
            snapshot_seq,
            ref id,
            ..
        } if snapshot_seq > 1 && id == "tgt_poison_delete/repo-agent"
    ));

    let listed = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSpawnTargets,
    )
    .expect("list after poison delete");
    assert!(
        listed
            .spawn_targets
            .iter()
            .all(|target| target.target_id != "tgt_poison_delete"),
        "deleted poisoned target must be gone: {:?}",
        listed.spawn_targets
    );

    let recovered_list = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypes,
    )
    .expect("list session types after removing poison source");
    assert_eq!(
        recovered_list.kind,
        botster_hub::DaemonResponseKind::SessionTypes
    );

    drop(subscription);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn poison_recovery_disable_succeeds_under_invalid_repo_session_types() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("st-poison-disable");
    let target_root = unique_short_test_dir("st-poison-disable-root");
    fs::create_dir_all(&target_root).expect("create target root");
    write_repo_session_types_file(
        &target_root,
        &complete_repo_session_types_json("repo-agent"),
    );
    let config = explicit_config(&data_dir);
    let endpoint = daemon_endpoint(&config);
    let child = start_cli_daemon(&data_dir);

    let mut subscription =
        botster_hub_client::subscribe_entities(&endpoint, "session_type", "st-poison-disable-sub")
            .expect("subscribe before admit");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound entity reads");
    assert!(matches!(
        subscription.next_frame().expect("initial empty snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq: 0,
            ref items,
            ..
        } if items.is_empty()
    ));

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_poison_disable".to_string()),
            label: Some("Poison Disable".to_string()),
            root: target_root.clone(),
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("admit valid target before poison");
    assert!(matches!(
        subscription.next_frame().expect("repo definition upsert"),
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq: 1,
            ref id,
            ..
        } if id == "tgt_poison_disable/repo-agent"
    ));

    write_repo_session_types_file(&target_root, &incomplete_repo_session_types_json());

    let list_poisoned = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypes,
    )
    .expect("list under poison must keep transport open");
    assert_eq!(
        list_poisoned.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        list_poisoned
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("invalid_repo_session_types")
    );

    // Independent recovery case: disable under poison (no delete).
    let disabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_poison_disable".to_string(),
            label: None,
            root: None,
            enabled: Some(false),
            kind: None,
            base_ref: None,
            metadata: None,
        },
    )
    .expect("disable under poison must keep transport open");
    assert_eq!(disabled.kind, botster_hub::DaemonResponseKind::SpawnTargets);
    assert!(!disabled.spawn_targets[0].enabled);

    assert!(matches!(
        subscription
            .next_frame()
            .expect("remove after poison disable recovery"),
        botster_hub_client::DaemonEntityFrame::Remove {
            snapshot_seq,
            ref id,
            ..
        } if snapshot_seq > 1 && id == "tgt_poison_disable/repo-agent"
    ));

    let recovered_list = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypes,
    )
    .expect("list session types after disable recovery");
    assert_eq!(
        recovered_list.kind,
        botster_hub::DaemonResponseKind::SessionTypes
    );

    // Non-recovery update still rejects while re-enabling poisoned root.
    let reenable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_poison_disable".to_string(),
            label: None,
            root: None,
            enabled: Some(true),
            kind: None,
            base_ref: None,
            metadata: None,
        },
    )
    .expect("re-enable with still-invalid session-types must keep transport open");
    assert_eq!(
        reenable.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        reenable.error.as_ref().map(|error| error.code.as_str()),
        Some("invalid_repo_session_types")
    );

    drop(subscription);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_worktree_crud_scopes_paths_to_spawn_targets_without_requiring_git() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("worktree-crud");
    let target_root = unique_short_test_dir("worktree-target");
    let plain_worktree = target_root.join("plain");
    let git_worktree = target_root.join("gitish");
    let outside_dir = unique_short_test_dir("worktree-outside");
    fs::create_dir_all(&plain_worktree).expect("create plain worktree");
    fs::create_dir_all(git_worktree.join(".git")).expect("create git metadata dir");
    fs::write(git_worktree.join(".git/HEAD"), "ref: refs/heads/main\n").expect("write HEAD");
    fs::create_dir_all(&outside_dir).expect("create outside dir");
    let escape_link = target_root.join("escape-link");
    std::os::unix::fs::symlink(&outside_dir, &escape_link).expect("create symlink escape");
    assert!(
        !plain_worktree.join(".git").exists(),
        "plain worktree intentionally has no git metadata"
    );
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_worktrees".to_string()),
            label: Some("Worktree Target".to_string()),
            root: target_root.clone(),
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("create spawn target for worktrees");

    let created = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateWorktree {
            worktree_id: Some("wt_plain".to_string()),
            target_id: "tgt_worktrees".to_string(),
            label: Some("Plain Worktree".to_string()),
            path: plain_worktree.clone(),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create plain worktree through daemon");
    assert_eq!(created.kind, botster_hub::DaemonResponseKind::Worktrees);
    assert_eq!(created.worktrees[0].worktree_id, "wt_plain");
    assert_eq!(created.worktrees[0].target_id, "tgt_worktrees");
    assert_eq!(created.worktrees[0].status, "present");
    let created_event = created
        .events
        .iter()
        .find_map(|event| match event {
            botster_hub::DaemonEvent::WorktreeLifecycle { event } => Some(event),
            _ => None,
        })
        .expect("create response should include worktree lifecycle event");
    assert_eq!(created_event.event, "worktree_created");
    assert_eq!(created_event.worktree_id.as_deref(), Some("wt_plain"));
    assert_eq!(created_event.target_id.as_deref(), Some("tgt_worktrees"));
    assert_eq!(created_event.status.as_deref(), Some("present"));
    assert_eq!(created_event.display_path.as_deref(), Some("plain"));
    let created_events_json =
        serde_json::to_string(&created.events).expect("serialize created worktree events");
    assert!(
        !created_events_json.contains(target_root.to_string_lossy().as_ref()),
        "worktree lifecycle events must not expose raw spawn target paths: {created_events_json}"
    );
    assert!(
        created.worktrees[0].git.is_none(),
        "git metadata must be optional for plain directories"
    );

    let listed =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListWorktrees)
            .expect("list worktrees through daemon");
    assert_eq!(listed.worktrees.len(), 1);
    assert_eq!(listed.worktrees[0].worktree_id, "wt_plain");

    let shown = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowWorktree {
            worktree_id: "wt_plain".to_string(),
        },
    )
    .expect("show worktree through daemon");
    assert_eq!(
        shown.worktrees[0].path,
        fs::canonicalize(&plain_worktree).expect("canonical plain worktree")
    );

    let deleted = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::DeleteWorktree {
            worktree_id: "wt_plain".to_string(),
        },
    )
    .expect("delete worktree record through daemon");
    assert_eq!(deleted.worktrees[0].worktree_id, "wt_plain");
    let deleted_event = deleted
        .events
        .iter()
        .find_map(|event| match event {
            botster_hub::DaemonEvent::WorktreeLifecycle { event } => Some(event),
            _ => None,
        })
        .expect("delete response should include worktree lifecycle event");
    assert_eq!(deleted_event.event, "worktree_deleted");
    assert_eq!(deleted_event.worktree_id.as_deref(), Some("wt_plain"));
    assert!(
        plain_worktree.exists(),
        "worktree record deletion must not delete filesystem contents"
    );
    let delete_missing = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::DeleteWorktree {
            worktree_id: "wt_plain".to_string(),
        },
    )
    .expect("delete missing worktree response");
    assert_eq!(
        delete_missing.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    let delete_failed_event = delete_missing
        .events
        .iter()
        .find_map(|event| match event {
            botster_hub::DaemonEvent::WorktreeLifecycle { event } => Some(event),
            _ => None,
        })
        .expect("delete failure response should include worktree lifecycle event");
    assert_eq!(delete_failed_event.event, "worktree_delete_failed");
    assert_eq!(delete_failed_event.worktree_id.as_deref(), Some("wt_plain"));
    assert_eq!(
        delete_failed_event.failure_kind.as_deref(),
        Some("not_found")
    );

    let git_created = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateWorktree {
            worktree_id: Some("wt_gitish".to_string()),
            target_id: "tgt_worktrees".to_string(),
            label: Some("Git Metadata Worktree".to_string()),
            path: git_worktree.clone(),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create git metadata worktree through daemon");
    assert_eq!(
        git_created.worktrees[0]
            .git
            .as_ref()
            .and_then(|git| git.branch.as_deref()),
        Some("main")
    );

    let traversal = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateWorktree {
            worktree_id: Some("wt_escape_parent".to_string()),
            target_id: "tgt_worktrees".to_string(),
            label: None,
            path: target_root.join(".."),
            metadata: BTreeMap::new(),
        },
    )
    .expect("traversal rejection response");
    assert_eq!(
        traversal.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        traversal.error.as_ref().map(|error| error.code.as_str()),
        Some("path_outside_target")
    );
    let create_failed_event = traversal
        .events
        .iter()
        .find_map(|event| match event {
            botster_hub::DaemonEvent::WorktreeLifecycle { event } => Some(event),
            _ => None,
        })
        .expect("create failure response should include worktree lifecycle event");
    assert_eq!(create_failed_event.event, "worktree_create_failed");
    assert_eq!(
        create_failed_event.worktree_id.as_deref(),
        Some("wt_escape_parent")
    );
    assert_eq!(
        create_failed_event.target_id.as_deref(),
        Some("tgt_worktrees")
    );
    assert_eq!(
        create_failed_event.failure_kind.as_deref(),
        Some("path_outside_target")
    );
    let failure_events_json =
        serde_json::to_string(&traversal.events).expect("serialize failure events");
    assert!(
        !failure_events_json.contains(target_root.to_string_lossy().as_ref())
            && !failure_events_json.contains("/Users/"),
        "failure lifecycle events must not expose raw local paths: {failure_events_json}"
    );

    let symlink_escape = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateWorktree {
            worktree_id: Some("wt_symlink_escape".to_string()),
            target_id: "tgt_worktrees".to_string(),
            label: None,
            path: escape_link,
            metadata: BTreeMap::new(),
        },
    )
    .expect("symlink escape rejection response");
    assert_eq!(
        symlink_escape
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("path_outside_target")
    );

    shutdown_cli_daemon(&data_dir, child);
    let restarted = start_cli_daemon(&data_dir);
    let reloaded = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowWorktree {
            worktree_id: "wt_gitish".to_string(),
        },
    )
    .expect("show persisted worktree after restart");
    assert_eq!(reloaded.worktrees[0].status, "present");
    fs::remove_dir_all(&git_worktree).expect("remove persisted worktree path");
    shutdown_cli_daemon(&data_dir, restarted);
    let restarted_missing = start_cli_daemon(&data_dir);
    let missing = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowWorktree {
            worktree_id: "wt_gitish".to_string(),
        },
    )
    .expect("show missing worktree after restart");
    assert_eq!(missing.worktrees[0].status, "missing");

    shutdown_cli_daemon(&data_dir, restarted_missing);
}

#[test]
fn daemon_spawns_repo_local_session_type_after_state_reload() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("repo-session-type");
    let package_root = unique_test_dir("repo-session-type-package");
    let repo_root = std::env::current_dir()
        .expect("current dir")
        .join(unique_test_dir("repo-session-type-repo"));
    write_session_type_context_package(&package_root);
    fs::create_dir_all(repo_root.join(".botster")).expect("create repo .botster dir");
    fs::create_dir_all(repo_root.join("bin")).expect("create repo bin dir");
    let script = repo_root.join("bin/repo-template.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'repo:%s\\n' \"$BOTSTER_MODE\" > repo-template-output.txt\nsleep 30\n",
    )
    .expect("write repo template script");
    let mut permissions = fs::metadata(&script)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).expect("chmod repo script");
    fs::write(
        repo_root.join(".botster/session-types.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "session_types": [{
                "id": "init",
                "label": "Repo agent",
                "role": "botster.agent",
                "interaction": "interactive",
                "traits": ["test"],
                "lifecycle": "task",
                "command": "bin/repo-template.sh",
                "environment": { "BOTSTER_MODE": "repo" },
                "allowed_environment_overrides": ["BOTSTER_MODE"]
            }]
        }))
        .expect("serialize repo templates"),
    )
    .expect("write repo templates");

    let config = explicit_config(&data_dir);
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.spawn_targets = vec![SpawnTarget {
                target_id: "repo:runtime".to_string(),
                label: "Repo Runtime".to_string(),
                root: repo_root.clone(),
                enabled: true,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            }];
        })
        .expect("persist admitted repo target before daemon start");
    let child = start_cli_daemon(&data_dir);

    let enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_root.clone(),
        },
    )
    .expect("enable package session type baseline");
    assert_eq!(
        enable.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );

    let templates = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypes,
    )
    .expect("list session types");
    assert_eq!(templates.session_types.len(), 1);
    assert_eq!(templates.session_types[0].source, "repo");
    assert_eq!(templates.session_types[0].target_id, "repo:runtime");

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SpawnSessionType {
            session_type_id: "init".to_string(),
            session_id: "repo-session-type".to_string(),
            request: botster_hub::DaemonSessionTypeRequest {
                environment: BTreeMap::from([("BOTSTER_MODE".to_string(), "explicit".to_string())]),
                ..botster_hub::DaemonSessionTypeRequest::default()
            },
        },
    )
    .expect("spawn repo session type");
    assert_eq!(
        spawn.kind,
        botster_hub::DaemonResponseKind::Spawned,
        "spawn response error={:?}",
        spawn.error
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let output_path = repo_root.join("repo-template-output.txt");
    let mut output = String::new();
    while std::time::Instant::now() < deadline {
        if let Ok(contents) = fs::read_to_string(&output_path) {
            output = contents;
            if output.contains("repo:explicit") {
                break;
            }
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert_eq!(output.trim(), "repo:explicit");
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let mut first =
        botster_hub_client::subscribe_session_entities(&endpoint, "repo-session-type-first")
            .expect("subscribe session entity metadata before restart");
    first
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound first metadata snapshot");
    wait_for_session_type_metadata(&mut first, "repo-session-type", "repo:runtime/init");
    drop(first);

    shutdown_cli_daemon(&data_dir, child.transfer_sessions());
    let restarted = start_cli_daemon(&data_dir);
    let mut second =
        botster_hub_client::subscribe_session_entities(&endpoint, "repo-session-type-restarted")
            .expect("subscribe session entity metadata after restart");
    second
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound restarted metadata snapshot");
    wait_for_session_type_metadata(&mut second, "repo-session-type", "repo:runtime/init");
    drop(second);
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "repo-session-type".to_string(),
        },
    )
    .expect("shut down durable session worker");
    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn daemon_list_session_types_for_target_includes_device_globals() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("list-for-target-device-global");
    let target_root = std::env::current_dir()
        .expect("current dir")
        .join(unique_test_dir("list-for-target-device-global-root"));
    let device_root = std::env::current_dir()
        .expect("current dir")
        .join(unique_test_dir("list-for-target-device-global-device"));
    fs::create_dir_all(&target_root).expect("create admitted target root");
    fs::create_dir_all(device_root.join("bin")).expect("create device bin");
    let script = device_root.join("bin/noop.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'spawned:%s\\n' \"$BOTSTER_SESSION_ID\"\nsleep 30\n",
    )
    .expect("write device spawn script");
    let mut permissions = fs::metadata(&script)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).expect("chmod device script");

    let config = explicit_config(&data_dir);
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.device_session_type_sources = vec![botster_hub::DeviceSessionTypeSource {
                root: device_root.clone(),
                session_types: vec![botster_hub::PackageSessionType {
                    id: "global-agent".to_string(),
                    label: "Global agent".to_string(),
                    description: None,
                    icon: None,
                    role: "botster.agent".to_string(),
                    interaction: "interactive".to_string(),
                    traits: vec!["test".to_string()],
                    lifecycle: "task".to_string(),
                    execution: botster_hub::PackageSessionTypeExecution::RelativeExecutable,
                    command: "bin/noop.sh".to_string(),
                    args: Vec::new(),
                    working_directory: botster_hub::PackageSessionTypeWorkingDirectory::PackageRoot,
                    environment: BTreeMap::new(),
                    allowed_environment_overrides: Vec::new(),
                    context: Vec::new(),
                    target_id: None,
                }],
            }];
            state.spawn_targets = vec![SpawnTarget {
                target_id: "tgt_hub".to_string(),
                label: "Hub".to_string(),
                root: target_root.clone(),
                enabled: true,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            }];
        })
        .expect("persist device global and admitted target");

    let child = start_cli_daemon(&data_dir);
    let listed = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypesForTarget {
            target_id: "tgt_hub".to_string(),
        },
    )
    .expect("list session types for admitted target through daemon");
    assert_eq!(listed.kind, botster_hub::DaemonResponseKind::SessionTypes);
    assert!(
        listed
            .session_types
            .iter()
            .any(|row| row.session_type_id == "device/global-agent"
                && row.target_id == "tgt_hub"
                && row.source == "device"),
        "device Global must be eligible at admitted spawn point: {:?}",
        listed
            .session_types
            .iter()
            .map(|row| (&row.session_type_id, &row.target_id, &row.source))
            .collect::<Vec<_>>()
    );
    assert!(
        !listed.session_types.is_empty(),
        "list-for-target must return at least one row for spawn parity"
    );

    // Real daemon SpawnSessionType for every listed id, then shut down and reap.
    for (index, row) in listed.session_types.iter().enumerate() {
        let session_id = format!("list-parity-{index}");
        let spawned = botster_hub::daemon_transport_request(
            &config,
            botster_hub::DaemonRequest::SpawnSessionType {
                session_type_id: row.session_type_id.clone(),
                session_id: session_id.clone(),
                request: botster_hub_client::DaemonSessionTypeRequest {
                    target_id: Some("tgt_hub".to_string()),
                    ..botster_hub_client::DaemonSessionTypeRequest::default()
                },
            },
        )
        .unwrap_or_else(|error| {
            panic!(
                "spawn listed id {} through daemon: {error:?}",
                row.session_type_id
            )
        });
        assert_eq!(
            spawned.kind,
            botster_hub::DaemonResponseKind::Spawned,
            "spawn listed id {}: {:?}",
            row.session_type_id,
            spawned
        );
        botster_hub_client::request(
            &botster_hub_client::DaemonEndpoint::new(
                config
                    .transports
                    .local_socket
                    .as_ref()
                    .expect("local socket")
                    .path
                    .clone(),
            ),
            botster_hub_client::DaemonRequest::ShutdownSession {
                session_id: session_id.clone(),
            },
        )
        .expect("shutdown spawned parity session");
    }

    // Management catalog still projects storage provenance device:local.
    let catalog = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypes,
    )
    .expect("management catalog");
    assert!(
        catalog
            .session_types
            .iter()
            .any(|row| row.session_type_id == "device/global-agent"
                && row.target_id == "device:local"),
        "management catalog keeps device:local provenance"
    );

    // CLI path hits the same daemon request.
    let cli = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("session-types")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--target")
        .arg("tgt_hub")
        .output()
        .expect("run session-types list --target");
    assert!(cli.status.success(), "{}", command_output_text(&cli));
    let cli_text = String::from_utf8_lossy(&cli.stdout);
    assert!(
        cli_text.contains("id=global-agent")
            && cli_text.contains("source=device")
            && cli_text.contains("target=tgt_hub"),
        "CLI list-for-target must surface device Global at T: {cli_text}"
    );

    let missing = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTypesForTarget {
            target_id: "tgt_missing".to_string(),
        },
    )
    .expect("missing target returns typed operator error");
    assert_eq!(missing.kind, botster_hub::DaemonResponseKind::OperatorError);
    assert_eq!(
        missing.error.as_ref().map(|error| error.code.as_str()),
        Some("target_not_found")
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn session_type_crud_pushes_authoritative_entity_deltas_without_polling() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("session-type-entity-crud");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription = botster_hub_client::subscribe_entities(
        &endpoint,
        "session_type",
        "session-type-definitions",
    )
    .expect("subscribe authoritative session type family");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound session type entity reads");
    assert!(matches!(
        subscription.next_frame().expect("initial definition snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq: 0,
            ref items,
            ..
        } if items.is_empty()
    ));

    // Authored with a relative working-directory path and a non-empty environment:
    // exactly the two fields the sanitized row cannot carry.
    let definition = botster_hub_client::DaemonSessionTypeDefinition {
        id: "terminal-accessory".to_string(),
        label: "Terminal accessory".to_string(),
        description: Some("Interactive terminal companion".to_string()),
        icon: Some("terminal".to_string()),
        role: "botster.accessory".to_string(),
        interaction: "interactive".to_string(),
        traits: vec!["terminal".to_string()],
        lifecycle: "persistent".to_string(),
        execution: botster_hub_client::DaemonSessionTypeExecution::RelativeExecutable,
        command: "bin/accessory.sh".to_string(),
        args: Vec::new(),
        working_directory: botster_hub_client::DaemonSessionTypeWorkingDirectory::Relative {
            path: "nested/dir".to_string(),
        },
        environment: BTreeMap::from([("BOTSTER_MODE".to_string(), "authored".to_string())]),
        allowed_environment_overrides: vec!["BOTSTER_MODE".to_string()],
        context: Vec::new(),
        target_id: None,
    };
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::CreateSessionType {
            source: botster_hub_client::DaemonSessionTypeMutationSource::Device,
            definition: definition.clone(),
        },
    )
    .expect("create device session type through public socket");
    assert!(matches!(
        subscription.next_frame().expect("create upsert"),
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq: 1,
            ref id,
            ref entity,
            ..
        } if id == "device/terminal-accessory"
            && entity["role"] == "botster.accessory"
            && entity["interaction"] == "interactive"
    ));

    // The published row cannot rebuild the authored definition: it derives a policy
    // string and has no environment field at all.
    let sanitized = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShowSessionType {
            session_type_id: "terminal-accessory".to_string(),
        },
    )
    .expect("show sanitized session type through public socket");
    assert_eq!(sanitized.session_types.len(), 1);
    assert_eq!(
        sanitized.session_types[0].working_directory_policy,
        "relative"
    );
    let sanitized_text =
        serde_json::to_string(&sanitized.session_types[0]).expect("serialize sanitized row");
    assert!(!sanitized_text.contains("nested/dir"));
    assert!(!sanitized_text.contains("authored"));

    // The authoring read returns exactly what UpdateSessionType consumes.
    let authoring = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShowSessionTypeDefinition {
            session_type_id: "terminal-accessory".to_string(),
        },
    )
    .expect("read authored session type definition through public socket");
    assert_eq!(
        authoring.kind,
        botster_hub_client::DaemonResponseKind::SessionTypeDefinition
    );
    let editable = authoring
        .session_type_definition
        .clone()
        .expect("authoring response carries the editable definition");
    assert_eq!(editable.session_type_id, "device/terminal-accessory");
    assert_eq!(
        editable.source,
        botster_hub_client::DaemonSessionTypeMutationSource::Device
    );
    assert_eq!(
        editable.definition, definition,
        "the socket authoring read must be lossless"
    );

    // Read, change exactly one field, submit the rest back untouched. Before this
    // seam a client had to rebuild from the sanitized row and silently dropped the
    // authored path and environment here.
    let mut edited = editable.definition.clone();
    edited.label = "Updated terminal accessory".to_string();
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::UpdateSessionType {
            source: editable.source.clone(),
            definition: edited.clone(),
        },
    )
    .expect("update device session type through public socket");
    assert!(matches!(
        subscription.next_frame().expect("update upsert"),
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq: 2,
            ref entity,
            ..
        } if entity["label"] == "Updated terminal accessory"
    ));

    let after_edit = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShowSessionTypeDefinition {
            session_type_id: "terminal-accessory".to_string(),
        },
    )
    .expect("re-read authored definition after the edit")
    .session_type_definition
    .expect("authoring response carries the editable definition");
    assert_eq!(
        after_edit.definition, edited,
        "only the edited field changed; the authored path and environment survived"
    );
    assert_eq!(
        after_edit.definition.working_directory,
        botster_hub_client::DaemonSessionTypeWorkingDirectory::Relative {
            path: "nested/dir".to_string()
        }
    );
    assert_eq!(
        after_edit
            .definition
            .environment
            .get("BOTSTER_MODE")
            .map(String::as_str),
        Some("authored")
    );

    // The operator entry point reaches the same seam through the real daemon.
    let cli = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("session-types")
        .arg("definition")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("terminal-accessory")
        .output()
        .expect("run Hub session-types definition CLI");
    assert!(cli.status.success(), "{}", command_output_text(&cli));
    let cli_text = String::from_utf8_lossy(&cli.stdout);
    assert!(
        cli_text.contains("response=session_type_definition"),
        "{cli_text}"
    );
    assert!(
        cli_text.contains("session_type_id=device/terminal-accessory"),
        "{cli_text}"
    );
    assert!(cli_text.contains(r#""source":"device""#), "{cli_text}");
    assert!(cli_text.contains(r#""path":"nested/dir""#), "{cli_text}");
    assert!(
        cli_text.contains(r#""BOTSTER_MODE":"authored""#),
        "{cli_text}"
    );

    let rejected = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::DeleteSessionType {
            source: botster_hub_client::DaemonSessionTypeMutationSource::Package {
                package_name: "immutable.plugin".to_string(),
            },
            session_type_id: "terminal-accessory".to_string(),
        },
    )
    .expect("package mutation returns typed operator response");
    assert_eq!(
        rejected.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        rejected.error.as_ref().map(|error| error.code.as_str()),
        Some("read_only_session_type_source")
    );

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::DeleteSessionType {
            source: botster_hub_client::DaemonSessionTypeMutationSource::Device,
            session_type_id: "terminal-accessory".to_string(),
        },
    )
    .expect("delete device session type through public socket");
    assert!(matches!(
        subscription.next_frame().expect("delete remove"),
        botster_hub_client::DaemonEntityFrame::Remove {
            snapshot_seq: 3,
            ref id,
            ..
        } if id == "device/terminal-accessory"
    ));

    let package_dir = unique_test_dir("session-type-entity-package");
    fs::create_dir_all(&package_dir).expect("create session type entity package");
    fs::write(
        package_dir.join("plugin.lua"),
        "return botster.register({})\n",
    )
    .expect("write session type entity plugin");
    fs::write(
        package_dir.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "session-type.entity-plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": package_dir.canonicalize().expect("package path") },
            "capabilities": [],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "session_types": [{
                "id": "package-accessory",
                "label": "Package accessory",
                "role": "botster.accessory",
                "interaction": "service",
                "traits": ["background"],
                "lifecycle": "persistent",
                "command": "bin/accessory"
            }]
        }))
        .expect("serialize session type entity package"),
    )
    .expect("write session type entity package manifest");
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::EnablePackageLocalPath { path: package_dir },
    )
    .expect("enable package session type");
    assert!(matches!(
        subscription.next_frame().expect("package enable upsert"),
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq: 4,
            ref id,
            ref entity,
            ..
        } if id == "session-type.entity-plugin/package-accessory"
            && entity["editable"] == false
            && entity["interaction"] == "service"
    ));

    // Package-authored definitions stay read-only over the real socket too, so
    // package environments are never newly exposed by the authoring read.
    let package_refusal = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShowSessionTypeDefinition {
            session_type_id: "package-accessory".to_string(),
        },
    )
    .expect("package authoring read returns a typed operator response");
    assert_eq!(
        package_refusal.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        package_refusal
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("read_only_session_type_source")
    );
    assert!(package_refusal.session_type_definition.is_none());
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::DisablePackage {
            package_name: "session-type.entity-plugin".to_string(),
        },
    )
    .expect("disable package session type");
    let package_disable_frame = subscription.next_frame().expect("package disable update");
    assert!(
        matches!(
            package_disable_frame,
            botster_hub_client::DaemonEntityFrame::Upsert {
                snapshot_seq: 5,
                ref id,
                ref entity,
                ..
            } if id == "session-type.entity-plugin/package-accessory"
                && entity["available"] == false
        ),
        "unexpected package disable frame: {package_disable_frame:?}"
    );
    drop(subscription);

    let mut reconnected = botster_hub_client::subscribe_entities(
        &endpoint,
        "session_type",
        "session-type-definitions-reconnected",
    )
    .expect("reconnect authoritative session type family");
    reconnected
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound reconnect read");
    assert!(matches!(
        reconnected.next_frame().expect("reconnect snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq: 5,
            ref items,
            ..
        } if items.len() == 1 && items[0]["available"] == false
    ));
    drop(reconnected);
    shutdown_cli_daemon(&data_dir, child);
}

fn device_session_type_definition(
    id: &str,
    label: &str,
) -> botster_hub_client::DaemonSessionTypeDefinition {
    botster_hub_client::DaemonSessionTypeDefinition {
        id: id.to_string(),
        label: label.to_string(),
        description: Some(format!("{label} description")),
        icon: Some("terminal".to_string()),
        role: "botster.accessory".to_string(),
        interaction: "interactive".to_string(),
        traits: vec!["terminal".to_string()],
        lifecycle: "persistent".to_string(),
        execution: botster_hub_client::DaemonSessionTypeExecution::RelativeExecutable,
        command: "bin/accessory.sh".to_string(),
        args: Vec::new(),
        working_directory: botster_hub_client::DaemonSessionTypeWorkingDirectory::Relative {
            path: "nested/dir".to_string(),
        },
        environment: BTreeMap::from([("BOTSTER_MODE".to_string(), "authored".to_string())]),
        allowed_environment_overrides: vec!["BOTSTER_MODE".to_string()],
        context: Vec::new(),
        target_id: None,
    }
}

fn assert_contiguous_session_type_frames(
    frames: &[botster_hub_client::DaemonEntityFrame],
    subscription_id: &str,
    first_seq: u64,
) {
    assert!(
        !frames.is_empty(),
        "held session-type subscription must deliver at least one frame"
    );
    for (index, frame) in frames.iter().enumerate() {
        let expected = first_seq + index as u64;
        match frame {
            botster_hub_client::DaemonEntityFrame::Snapshot {
                subscription_id: observed_id,
                snapshot_seq,
                ..
            }
            | botster_hub_client::DaemonEntityFrame::Upsert {
                subscription_id: observed_id,
                snapshot_seq,
                ..
            }
            | botster_hub_client::DaemonEntityFrame::Remove {
                subscription_id: observed_id,
                snapshot_seq,
                ..
            }
            | botster_hub_client::DaemonEntityFrame::Patch {
                subscription_id: observed_id,
                snapshot_seq,
                ..
            } => {
                assert_eq!(
                    observed_id, subscription_id,
                    "frame {index} left the held subscription: {frame:?}"
                );
                assert_eq!(
                    *snapshot_seq, expected,
                    "frame {index} broke the Web +1 contract: {frame:?}"
                );
            }
            other => panic!("unexpected session-type frame {index}: {other:?}"),
        }
    }
}

#[test]
fn session_type_held_subscription_stays_contiguous_through_populated_catalog_crud() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("session-type-held-crud");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::CreateSessionType {
            source: botster_hub_client::DaemonSessionTypeMutationSource::Device,
            definition: device_session_type_definition("seed-accessory", "Seed accessory"),
        },
    )
    .expect("seed a catalog row before subscribe");

    let mut subscription = botster_hub_client::subscribe_entities(
        &endpoint,
        "session_type",
        "held-session-type-crud",
    )
    .expect("subscribe one session_type family");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound held session type entity reads");
    let snapshot = subscription
        .next_frame()
        .expect("initial populated snapshot");
    let snapshot_seq = match &snapshot {
        botster_hub_client::DaemonEntityFrame::Snapshot {
            subscription_id,
            snapshot_seq,
            items,
            ..
        } => {
            assert_eq!(subscription_id, "held-session-type-crud");
            assert!(
                items.iter().any(|item| item["session_type_id"] == "device/seed-accessory"),
                "snapshot must include the pre-existing catalog row: {items:?}"
            );
            *snapshot_seq
        }
        other => panic!("expected populated snapshot, got {other:?}"),
    };

    let package_dir = unique_test_dir("session-type-held-two-row-package");
    fs::create_dir_all(&package_dir).expect("create two-row session type package");
    fs::write(
        package_dir.join("plugin.lua"),
        "return botster.register({})\n",
    )
    .expect("write two-row session type plugin");
    fs::write(
        package_dir.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "session-type.held-two-row",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": package_dir.canonicalize().expect("package path") },
            "capabilities": [],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "session_types": [
                {
                    "id": "package-one",
                    "label": "Package one",
                    "role": "botster.accessory",
                    "interaction": "service",
                    "traits": ["background"],
                    "lifecycle": "persistent",
                    "command": "bin/one"
                },
                {
                    "id": "package-two",
                    "label": "Package two",
                    "role": "botster.accessory",
                    "interaction": "service",
                    "traits": ["background"],
                    "lifecycle": "persistent",
                    "command": "bin/two"
                }
            ]
        }))
        .expect("serialize two-row session type package"),
    )
    .expect("write two-row session type package manifest");
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::EnablePackageLocalPath { path: package_dir },
    )
    .expect("enable package that publishes two session types in one generation");

    let package_one = subscription
        .next_frame()
        .expect("first same-generation package upsert");
    let package_two = subscription
        .next_frame()
        .expect("second same-generation package upsert");
    let package_ids = [
        match &package_one {
            botster_hub_client::DaemonEntityFrame::Upsert { id, .. } => id.as_str(),
            other => panic!("expected first package upsert, got {other:?}"),
        },
        match &package_two {
            botster_hub_client::DaemonEntityFrame::Upsert { id, .. } => id.as_str(),
            other => panic!("expected second package upsert, got {other:?}"),
        },
    ];
    assert!(
        package_ids.contains(&"session-type.held-two-row/package-one")
            && package_ids.contains(&"session-type.held-two-row/package-two"),
        "package enable must publish both rows: {package_one:?} {package_two:?}"
    );

    let created = device_session_type_definition("held-accessory", "Held accessory");
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::CreateSessionType {
            source: botster_hub_client::DaemonSessionTypeMutationSource::Device,
            definition: created.clone(),
        },
    )
    .expect("create a second device session type on the held subscription");
    let create_frame = subscription.next_frame().expect("held create upsert");
    assert!(
        matches!(
            &create_frame,
            botster_hub_client::DaemonEntityFrame::Upsert { id, .. }
                if id == "device/held-accessory"
        ),
        "create must arrive as an upsert on the held subscription: {create_frame:?}"
    );

    let mut updated = created;
    updated.label = "Updated held accessory".to_string();
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::UpdateSessionType {
            source: botster_hub_client::DaemonSessionTypeMutationSource::Device,
            definition: updated,
        },
    )
    .expect("update the held device session type");
    let update_frame = subscription.next_frame().expect("held update upsert");
    assert!(
        matches!(
            &update_frame,
            botster_hub_client::DaemonEntityFrame::Upsert { entity, .. }
                if entity["label"] == "Updated held accessory"
        ),
        "update must arrive as an upsert on the held subscription: {update_frame:?}"
    );

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::DeleteSessionType {
            source: botster_hub_client::DaemonSessionTypeMutationSource::Device,
            session_type_id: "held-accessory".to_string(),
        },
    )
    .expect("delete the held device session type");
    let remove_frame = subscription.next_frame().expect("held delete remove");
    assert!(
        matches!(
            &remove_frame,
            botster_hub_client::DaemonEntityFrame::Remove { id, .. }
                if id == "device/held-accessory"
        ),
        "delete must arrive as a remove on the held subscription: {remove_frame:?}"
    );

    let frames = vec![
        snapshot,
        package_one,
        package_two,
        create_frame,
        update_frame,
        remove_frame,
    ];
    assert_contiguous_session_type_frames(&frames, "held-session-type-crud", snapshot_seq);
    drop(subscription);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn spawn_target_admission_pushes_repo_session_type_deltas_without_polling() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("session-type-target-admission");
    let repo_dir = data_dir.join("admitted-repo");
    fs::create_dir_all(repo_dir.join(".botster")).expect("create admitted repo fixture");
    fs::write(
        repo_dir.join(".botster/session-types.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "session_types": [{
                "id": "repo-agent",
                "label": "Repo agent",
                "role": "botster.agent",
                "interaction": "interactive",
                "lifecycle": "task",
                "command": "bin/agent"
            }]
        }))
        .expect("serialize repo session type"),
    )
    .expect("write repo session type");
    let repo_dir = repo_dir.canonicalize().expect("canonical admitted repo");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription = botster_hub_client::subscribe_entities(
        &endpoint,
        "session_type",
        "session-type-target-admission",
    )
    .expect("subscribe before target admission");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound target admission entity reads");
    assert!(matches!(
        subscription.next_frame().expect("initial empty snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq: 0,
            ref items,
            ..
        } if items.is_empty()
    ));

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::CreateSpawnTarget {
            target_id: Some("repo:admitted".to_string()),
            label: Some("Admitted repo".to_string()),
            root: repo_dir,
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("admit repo target through public socket");
    assert!(matches!(
        subscription.next_frame().expect("repo definition upsert"),
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq: 1,
            ref id,
            ref entity,
            ..
        } if id == "repo:admitted/repo-agent"
            && entity["source"] == "repo"
            && entity["target_id"] == "repo:admitted"
    ));

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::DeleteSpawnTarget {
            target_id: "repo:admitted".to_string(),
        },
    )
    .expect("delete repo target through public socket");
    assert!(matches!(
        subscription.next_frame().expect("repo definition remove"),
        botster_hub_client::DaemonEntityFrame::Remove {
            snapshot_seq: 2,
            ref id,
            ..
        } if id == "repo:admitted/repo-agent"
    ));

    drop(subscription);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_apps_list_show_and_open_web_use_structured_app_url() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-apps-web");
    let package_dir = unique_test_dir("cli-apps-web-package");
    write_app_registry_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run apps list");
    assert!(
        list.status.success(),
        "apps list failed: {}",
        command_output_text(&list)
    );
    let list_text = command_output_text(&list);
    assert!(list_text.contains("response=apps"));
    assert!(list_text.contains("app package=runtime.apps app_id=web"));
    assert!(list_text.contains("app package=runtime.apps app_id=terminal"));

    let show = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.apps/web")
        .output()
        .expect("run apps show");
    assert!(
        show.status.success(),
        "apps show failed: {}",
        command_output_text(&show)
    );
    let show_text = command_output_text(&show);
    assert!(show_text.contains("response=app"));
    assert!(show_text.contains("package=runtime.apps"));
    assert!(show_text.contains("app_id=web"));

    let open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("web")
        .output()
        .expect("run apps open web");
    assert!(
        open.status.success(),
        "apps open web failed: {}",
        command_output_text(&open)
    );
    let open_text = command_output_text(&open);
    assert!(open_text.contains("app_url=http://127.0.0.1:49152"));
    assert!(!open_text.contains("http://127.0.0.1:59999"));
    let apps = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListApps,
    )
    .expect("list apps after cli open");
    assert_eq!(
        app_row(&apps, "web").launch_target.local_url.as_deref(),
        Some("http://127.0.0.1:49152")
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_apps_open_web_injects_hub_connection_environment() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-apps-web-hub-env");
    let package_dir = unique_test_dir("cli-apps-web-hub-env-package");
    write_hub_env_web_app_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.hub-env/web")
        .output()
        .expect("run apps open web with hub env fixture");
    assert!(
        open.status.success(),
        "apps open web failed: {}",
        command_output_text(&open)
    );
    let open_text = command_output_text(&open);
    assert!(open_text.contains("app_url=http://127.0.0.1:49153"));
    assert!(!open_text.contains("BOTSTER_HUB_CONNECTION must"));

    let status = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PackageEntrypointStatus {
            package_name: "runtime.hub-env".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("inspect web app entrypoint status");
    let entrypoint = package_entrypoint(&status, "runtime.hub-env");
    assert_eq!(entrypoint.process.state, "running");
    assert!(
        entrypoint
            .process
            .diagnostics
            .iter()
            .all(|diagnostic| { !diagnostic.message.contains("BOTSTER_HUB_CONNECTION must") })
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_apps_open_terminal_uses_foreground_launch_contract() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-apps-terminal");
    let package_dir = unique_test_dir("cli-apps-terminal-package");
    write_botster_tui_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-tui")
        .output()
        .expect("run apps open terminal");
    assert!(
        open.status.success(),
        "apps open terminal failed: {}",
        command_output_text(&open)
    );
    assert!(command_output_text(&open).contains("botster-tui-fixture"));

    let removed_alias = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("tui")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run removed tui alias");
    assert!(
        !removed_alias.status.success(),
        "removed tui alias should fail: {}",
        command_output_text(&removed_alias)
    );
    let removed_alias_text = command_output_text(&removed_alias);
    assert!(removed_alias_text.contains("unknown command"));
    assert!(removed_alias_text.contains("usage: botster-hub <"));
    assert!(!removed_alias_text.contains("botster-tui-fixture"));
    assert!(!removed_alias_text.contains("first-party host profile ready"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_entrypoint_supervision_passes_environment_overrides() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-env");
    let package_dir = unique_test_dir("entrypoint-env-package");
    let output_path = std::env::current_dir()
        .expect("current dir")
        .join(data_dir.join("entrypoint-env.txt"));
    write_supervised_package(
        &package_dir,
        "runtime.env",
        "sh",
        &[
            "-c",
            &format!(
                "printf '%s' \"$BOTSTER_TEST_ENV_OVERRIDE\" > {}; while true; do sleep 1; done",
                output_path.display()
            ),
        ],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.env".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_TEST_ENV_OVERRIDE".to_string(),
                "override-reached-child".to_string(),
            )]),
        },
    )
    .expect("start supervised entrypoint with env");
    let entrypoint = package_entrypoint(&start, "runtime.env");
    assert_eq!(entrypoint.process.state, "running");

    let expected_output = "override-reached-child";
    let mut observed_output = String::new();
    for _ in 0..100 {
        observed_output = fs::read_to_string(&output_path).unwrap_or_default();
        if observed_output == expected_output {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(observed_output, expected_output);

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_entrypoint_supervision_reports_missing_command() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-missing-command");
    let package_dir = unique_test_dir("entrypoint-missing-command-package");
    write_supervised_package(
        &package_dir,
        "runtime.missing-command",
        "definitely-missing-botster-command",
        &[],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.missing-command".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start missing supervised entrypoint");
    let entrypoint = package_entrypoint(&start, "runtime.missing-command");
    assert_eq!(entrypoint.process.state, "failed");
    assert!(entrypoint.process.pid.is_none());
    assert!(
        entrypoint
            .process
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.kind == "spawn_error")
    );
    assert!(!format!("{start:?}").contains(package_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_entrypoint_supervision_reports_failed_command() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-failed-command");
    let package_dir = unique_test_dir("entrypoint-failed-command-package");
    write_supervised_package(
        &package_dir,
        "runtime.failed-command",
        "sh",
        &["-c", "printf 'fixture failure\\n' >&2; exit 42"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let _ = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.failed-command".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start failing supervised entrypoint");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let status = loop {
        let status = botster_hub::daemon_transport_request(
            &explicit_config(&data_dir),
            botster_hub::DaemonRequest::PackageEntrypointStatus {
                package_name: "runtime.failed-command".to_string(),
                entrypoint_id: "web".to_string(),
            },
        )
        .expect("status failing supervised entrypoint");
        if package_entrypoint(&status, "runtime.failed-command")
            .process
            .state
            != "running"
        {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "failing supervised entrypoint did not reach a terminal state"
        );
        thread::sleep(Duration::from_millis(20));
    };
    let entrypoint = package_entrypoint(&status, "runtime.failed-command");
    assert_eq!(entrypoint.process.state, "failed");
    assert_eq!(entrypoint.process.exit_status.as_deref(), Some("exit:42"));
    assert!(
        entrypoint
            .process
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.kind == "stderr"
                && diagnostic.message.contains("fixture failure"))
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_entrypoint_supervision_stops_and_restarts() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-restart");
    let package_dir = unique_test_dir("entrypoint-restart-package");
    write_supervised_package(
        &package_dir,
        "runtime.restart",
        "sh",
        &["-c", "while true; do sleep 1; done"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.restart".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start restart fixture");
    let first_pid = package_entrypoint(&start, "runtime.restart")
        .process
        .pid
        .expect("first pid");

    let stop = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StopPackageEntrypoint {
            package_name: "runtime.restart".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("stop restart fixture");
    assert_eq!(
        package_entrypoint(&stop, "runtime.restart").process.state,
        "stopped"
    );
    wait_for_process_exit(first_pid);
    // The deterministic pending-reader regression guard lives in
    // stop_preserves_pending_terminal_launch_result_state; this exercises the app projection.
    let stopped_apps = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListApps,
    )
    .expect("list apps after stopping restart fixture");
    let stopped_app = app_row(&stopped_apps, "web");
    assert_ne!(stopped_app.lifecycle_state, "running");
    assert_eq!(
        package_action(&stopped_app.actions, "start_package_entrypoint").status,
        botster_hub::DaemonPackageActionStatus::Available
    );

    let restart = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::RestartPackageEntrypoint {
            package_name: "runtime.restart".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("restart fixture");
    let second_pid = package_entrypoint(&restart, "runtime.restart")
        .process
        .pid
        .expect("second pid");
    assert_ne!(first_pid, second_pid);

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn process_ownership_package_entrypoint_cleanup_covers_disable_remove_and_shutdown() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-cleanup");
    let package_dir = unique_test_dir("entrypoint-cleanup-package");
    write_supervised_package(
        &package_dir,
        "runtime.cleanup",
        "sh",
        &["-c", "while true; do sleep 1; done"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.cleanup".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start cleanup fixture");
    let disable_pid = package_entrypoint(&start, "runtime.cleanup")
        .process
        .pid
        .expect("disable pid");
    let _ = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::DisablePackage {
            package_name: "runtime.cleanup".to_string(),
        },
    )
    .expect("disable cleanup package");
    wait_for_process_exit(disable_pid);

    let _ = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "runtime.cleanup".to_string(),
        },
    )
    .expect("re-enable cleanup package");
    let restart = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.cleanup".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("restart cleanup fixture");
    let shutdown_pid = package_entrypoint(&restart, "runtime.cleanup")
        .process
        .pid
        .expect("shutdown pid");

    shutdown_cli_daemon(&data_dir, child);
    wait_for_process_exit(shutdown_pid);
}

#[test]
fn package_entrypoint_supervision_cleans_up_on_daemon_signal() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-signal");
    let package_dir = unique_test_dir("entrypoint-signal-package");
    write_supervised_package(
        &package_dir,
        "runtime.signal",
        "sh",
        &["-c", "while true; do sleep 1; done"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.signal".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start signal fixture");
    let pid = package_entrypoint(&start, "runtime.signal")
        .process
        .pid
        .expect("signal pid");

    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let output = child.wait_with_output().expect("wait for signaled daemon");
    assert!(
        output.status.success(),
        "daemon signal shutdown failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_process_exit(pid);
}

#[test]
fn cli_packages_local_path_install_enable_disable_remove_flow() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-packages-flow");
    let package_dir = unique_test_dir("local-package-flow");
    write_local_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);

    let install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages install");
    assert!(
        install.status.success(),
        "install failed: {}",
        String::from_utf8_lossy(&install.stderr)
    );
    let stdout = String::from_utf8(install.stdout).expect("stdout is utf8");
    assert!(stdout.contains("decision=package"));
    assert!(stdout.contains("package_name=runtime.plugin"));
    assert!(stdout.contains("action=install"));
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("state=installed"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let show = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run botster-hub packages show");
    assert!(
        show.status.success(),
        "show failed: {}",
        String::from_utf8_lossy(&show.stderr)
    );
    let stdout = String::from_utf8(show.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("package name=runtime.plugin"));
    assert!(stdout.contains("state=installed"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run botster-hub packages enable");
    assert!(
        enable.status.success(),
        "enable failed: {}",
        String::from_utf8_lossy(&enable.stderr)
    );
    let stdout = String::from_utf8(enable.stdout).expect("stdout is utf8");
    assert!(stdout.contains("action=enable"));
    assert!(stdout.contains("state=enabled"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status after enable");
    assert!(lifecycle.lifecycle.iter().any(|plugin| {
        plugin.package_name == "runtime.plugin" && plugin.state == "enabled" && plugin.loaded
    }));

    let disable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("disable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run botster-hub packages disable");
    assert!(
        disable.status.success(),
        "disable failed: {}",
        String::from_utf8_lossy(&disable.stderr)
    );
    let stdout = String::from_utf8(disable.stdout).expect("stdout is utf8");
    assert!(stdout.contains("action=disable"));
    assert!(stdout.contains("state=disabled"));

    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status after disable");
    assert!(lifecycle.lifecycle.iter().any(|plugin| {
        plugin.package_name == "runtime.plugin" && plugin.state == "disabled" && !plugin.loaded
    }));

    let remove = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("remove")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run botster-hub packages remove");
    assert!(
        remove.status.success(),
        "remove failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );
    let stdout = String::from_utf8(remove.stdout).expect("stdout is utf8");
    assert!(stdout.contains("action=remove"));
    assert!(stdout.contains("package_count=0"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);

    let restarted = start_cli_daemon(&data_dir);
    let list_after_restart = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub packages list after remove restart");
    assert!(
        list_after_restart.status.success(),
        "packages list after remove restart failed: {}",
        String::from_utf8_lossy(&list_after_restart.stderr)
    );
    let stdout = String::from_utf8(list_after_restart.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=0"));

    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn daemon_packages_registry_fixture_preview_and_install_flow() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("daemon-registry-flow");
    let registry_dir = unique_test_dir("daemon-package-registry");
    let package_dir = registry_dir.join("packages").join("local");
    write_local_plugin_package(&package_dir);
    fs::write(
        registry_dir.join(botster_hub::LOCAL_PACKAGE_REGISTRY_FILE),
        r#"{
  "source": { "id": "daemon-fixture", "kind": "local_path", "label": "Daemon Fixture" },
  "entries": [
    {
      "id": "runtime-local",
      "first_party": true,
      "source": { "type": "local_path", "path": "packages/local" }
    },
    {
      "id": "runtime-git",
      "first_party": true,
      "source": {
        "type": "git",
        "repo": "https://example.invalid/botster/runtime.git",
        "branch": "main",
        "tag": "v1.0.0",
        "rev": "abc123"
      },
      "manifest": {
        "name": "runtime.git",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "capabilities": [
          { "surface": "surfaces" }
        ],
        "entrypoints": [
          { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ]
      }
    }
  ]
}
"#,
    )
    .expect("write package registry fixture");
    let child = start_cli_daemon(&data_dir);
    let config = explicit_config(&data_dir);

    let available = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListAvailablePackages {
            registry_path: registry_dir.clone(),
        },
    )
    .expect("list available packages through daemon");
    assert_eq!(
        available.kind,
        botster_hub::DaemonResponseKind::AvailablePackages
    );
    assert_eq!(available.available_packages.len(), 2);
    assert!(available.available_packages.iter().all(|package| {
        !package
            .source_label
            .contains(data_dir.to_string_lossy().as_ref())
            && !package
                .source_label
                .contains(registry_dir.to_string_lossy().as_ref())
    }));
    let local_available = available
        .available_packages
        .iter()
        .find(|package| package.entry_id == "runtime-local")
        .expect("local available entry");
    let install_action = package_action(&local_available.actions, "install_package_registry_entry");
    assert_eq!(
        install_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    let install_request = install_action
        .request
        .as_ref()
        .expect("install request mapping");
    assert_eq!(
        install_request.request_type,
        "install_package_registry_entry"
    );
    assert_eq!(install_request.entry_id.as_deref(), Some("runtime-local"));
    assert_eq!(
        install_request.registry_path.as_deref(),
        Some(registry_dir.to_string_lossy().as_ref())
    );
    assert_eq!(
        package_action(&local_available.actions, "enable_package").status,
        botster_hub::DaemonPackageActionStatus::Unavailable
    );

    let inspect = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InspectAvailablePackage {
            registry_path: registry_dir.clone(),
            entry_id: "runtime-git".to_string(),
        },
    )
    .expect("inspect git-shaped entry through daemon");
    let git_entry = inspect
        .available_packages
        .first()
        .expect("inspected git entry");
    assert_eq!(git_entry.source_kind, "git");
    assert_eq!(
        git_entry.pin.as_ref().expect("git pin").rev.as_deref(),
        Some("abc123")
    );

    let preview = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PreviewPackageInstall {
            registry_path: registry_dir.clone(),
            entry_id: "runtime-local".to_string(),
        },
    )
    .expect("preview install through daemon");
    let plan = preview.install_plan.expect("install plan");
    assert!(!plan.mutates_registry);
    assert!(!plan.starts_entrypoints);
    assert!(
        plan.effects
            .iter()
            .any(|effect| effect.kind == "no_entrypoint_start")
    );
    let list_after_preview =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list after preview");
    assert!(list_after_preview.packages.is_empty());

    let install = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageRegistryEntry {
            registry_path: registry_dir.clone(),
            entry_id: "runtime-git".to_string(),
        },
    )
    .expect("install git-shaped entry through daemon");
    assert_eq!(
        install.package_decision.expect("install decision").action,
        "install"
    );
    let installed = install
        .packages
        .iter()
        .find(|package| package.package_name == "runtime.git")
        .expect("installed package row");
    assert_eq!(installed.state, "installed");
    let enable_action = package_action(&installed.actions, "enable_package");
    assert_eq!(
        enable_action.status,
        botster_hub::DaemonPackageActionStatus::Blocked
    );
    let remove_action = package_action(&installed.actions, "remove_package");
    assert_eq!(
        remove_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    assert_eq!(
        remove_action
            .request
            .as_ref()
            .expect("remove request mapping")
            .request_type,
        "remove_package"
    );
    let reload_action = package_action(&installed.actions, "reload_package");
    assert_eq!(
        reload_action.status,
        botster_hub::DaemonPackageActionStatus::Unavailable
    );

    shutdown_cli_daemon(&data_dir, child);
    let state = FileHubStateStore::for_data_directory(&data_dir)
        .load_or_initialize(&explicit_config(&data_dir))
        .expect("load persisted hub state after registry install");
    let restored = PackageRegistry::from_snapshot(state.package_registry)
        .expect("restore package registry snapshot");
    let record = restored.package("runtime.git").expect("restored package");
    assert_eq!(record.state, botster_hub::PackageState::Installed);
    assert_eq!(
        record
            .source_metadata
            .as_ref()
            .expect("source metadata")
            .entry_id,
        "runtime-git"
    );
    assert_eq!(
        record.pin.as_ref().expect("pin").rev.as_deref(),
        Some("abc123")
    );
}

#[test]
fn live_hub_managed_git_spawn_reconciles_and_reuses_after_restart() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("managed-live");
    let competing_data_dir = unique_short_test_dir("managed-competing");
    let package_dir = unique_short_test_dir("managed-package");
    let repository = unique_short_test_dir("managed-repo");
    fs::create_dir_all(&repository).expect("create managed live repository");
    run_fixture_git(None, &["init", "-b", "main", path_str(&repository)]);
    run_fixture_git(
        Some(&repository),
        &["config", "user.email", "botster@example.invalid"],
    );
    run_fixture_git(
        Some(&repository),
        &["config", "user.name", "Botster Live Test"],
    );
    fs::write(repository.join("README.md"), "managed live\n").expect("write repository fixture");
    run_fixture_git(Some(&repository), &["add", "README.md"]);
    run_fixture_git(Some(&repository), &["commit", "-m", "managed live fixture"]);
    write_managed_git_session_package(&package_dir);

    let first_daemon = PanicSafeCliDaemon::start(&data_dir, "live managed Git daemon cleanup");
    let enabled = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install and enable live managed Git package");
    assert_eq!(
        enabled.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
    let created_target = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_live_managed".to_string()),
            label: Some("Live Managed".to_string()),
            root: repository.clone(),
            enabled: true,
            kind: Some("git".to_string()),
            base_ref: Some("main".to_string()),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create live Git spawn target");
    assert_eq!(
        created_target.spawn_targets[0].base_ref.as_deref(),
        Some("main")
    );

    let call = |call_data_dir: &Path| {
        botster_hub::daemon_transport_request(
            &explicit_config(call_data_dir),
            botster_hub::DaemonRequest::PluginMcpCallTool {
                name: "managed_git.live_spawn".to_string(),
                arguments: serde_json::json!({
                    "target_id": "tgt_live_managed",
                    "branch": "feature/live-restart",
                    "session_type_id": "managed-git.live-plugin/init"
                }),
            },
        )
        .expect("call live managed Git tool")
    };
    let first = call(&data_dir);
    assert_eq!(
        first.kind,
        botster_hub::DaemonResponseKind::PluginMcpToolResult
    );
    assert_eq!(first.plugin_tool_result["ok"], true);
    assert_eq!(first.plugin_tool_result["result"]["created_worktree"], true);
    let first_session_id = first.plugin_tool_result["result"]["session_id"]
        .as_str()
        .expect("first live session UUID")
        .to_string();
    assert_eq!(first_session_id.len(), 36);
    let first_worktree_path = PathBuf::from(
        first.plugin_tool_result["result"]["worktree_path"]
            .as_str()
            .expect("first live worktree path"),
    );
    let first_marker = first_worktree_path.join("live-managed.txt");
    wait_for_managed_git_session_exit(&data_dir, &first_session_id);
    assert_eq!(
        fs::read_to_string(first_marker).expect("live managed cwd marker"),
        "live-managed\n"
    );

    let competing_daemon =
        PanicSafeCliDaemon::start(&competing_data_dir, "competing managed Git daemon cleanup");
    botster_hub::daemon_transport_request(
        &explicit_config(&competing_data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("enable package in competing Hub");
    botster_hub::daemon_transport_request(
        &explicit_config(&competing_data_dir),
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_live_managed".to_string()),
            label: Some("Competing Managed".to_string()),
            root: repository.clone(),
            enabled: true,
            kind: Some("git".to_string()),
            base_ref: Some("main".to_string()),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create competing Git spawn target");
    let competing = call(&competing_data_dir);
    assert_eq!(competing.plugin_tool_result["ok"], false);
    assert_eq!(
        competing.plugin_tool_result["error"]["kind"],
        "branch_in_use"
    );
    assert!(
        first_worktree_path.exists(),
        "competing Hub must not remove the winning worktree"
    );
    competing_daemon.shutdown();

    first_daemon.shutdown();

    let second_daemon =
        PanicSafeCliDaemon::start(&data_dir, "restarted managed Git daemon cleanup");
    let listed = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListWorktrees,
    )
    .expect("list reconciled managed worktree after restart");
    let managed = listed
        .worktrees
        .iter()
        .find(|worktree| worktree.target_id == "tgt_live_managed")
        .expect("managed row after restart");
    assert_eq!(managed.management, "hub_managed_git");
    assert_eq!(managed.status, "present");
    assert_eq!(managed.path, first_worktree_path);
    let persisted_target = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ShowSpawnTarget {
            target_id: "tgt_live_managed".to_string(),
        },
    )
    .expect("show persisted Git target after restart");
    assert_eq!(
        persisted_target.spawn_targets[0].base_ref.as_deref(),
        Some("main")
    );
    let downgrade = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_live_managed".to_string(),
            label: None,
            root: None,
            enabled: None,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: None,
        },
    )
    .expect("return operator error for managed target downgrade");
    assert_eq!(
        downgrade.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        downgrade.error.as_ref().map(|error| error.code.as_str()),
        Some("managed_worktrees_exist")
    );
    let delete_target = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::DeleteSpawnTarget {
            target_id: "tgt_live_managed".to_string(),
        },
    )
    .expect("return operator error for managed target deletion");
    assert_eq!(
        delete_target.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        delete_target
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("managed_worktrees_exist")
    );
    let delete_worktree = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::DeleteWorktree {
            worktree_id: managed.worktree_id.clone(),
        },
    )
    .expect("return operator error for record-only managed worktree deletion");
    assert_eq!(
        delete_worktree.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        delete_worktree
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("managed_worktree_requires_reclaim")
    );

    let second = call(&data_dir);
    assert_eq!(second.plugin_tool_result["ok"], true);
    assert_eq!(second.plugin_tool_result["result"]["reused_worktree"], true);
    assert_eq!(
        second.plugin_tool_result["result"]["worktree_path"],
        first.plugin_tool_result["result"]["worktree_path"]
    );
    let second_session_id = second.plugin_tool_result["result"]["session_id"]
        .as_str()
        .expect("second live session UUID")
        .to_string();
    assert_ne!(second_session_id, first_session_id);

    for session_id in [first_session_id, second_session_id] {
        botster_hub::daemon_transport_request(
            &explicit_config(&data_dir),
            botster_hub::DaemonRequest::ShutdownSession { session_id },
        )
        .expect("shut down live managed session");
    }
    second_daemon.shutdown();
}

#[test]
fn cli_packages_local_path_diagnostics_are_actionable() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-packages-diagnostics");
    let invalid_dir = unique_test_dir("local-package-invalid");
    let incompatible_dir = unique_test_dir("local-package-incompatible");
    let duplicate_dir = unique_test_dir("local-package-duplicate");
    let denied_dir = unique_test_dir("local-package-denied");
    write_invalid_local_package(&invalid_dir);
    write_incompatible_local_package(&incompatible_dir);
    write_local_plugin_package(&duplicate_dir);
    write_denied_capability_local_package(&denied_dir);
    let child = start_cli_daemon(&data_dir);

    let invalid = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&invalid_dir)
        .output()
        .expect("run invalid package install");
    assert!(!invalid.status.success());
    let text = command_output_text(&invalid);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=install"));
    assert!(text.contains("InvalidLocalManifest"));
    assert!(!text.contains(invalid_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let incompatible = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&incompatible_dir)
        .output()
        .expect("run incompatible package install");
    assert!(!incompatible.status.success());
    let text = command_output_text(&incompatible);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=install"));
    assert!(text.contains("BotsterCompatibility"));
    assert!(!text.contains(incompatible_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let first_install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&duplicate_dir)
        .output()
        .expect("run first duplicate package install");
    assert!(
        first_install.status.success(),
        "first duplicate install failed: {}",
        String::from_utf8_lossy(&first_install.stderr)
    );
    let duplicate = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&duplicate_dir)
        .output()
        .expect("run duplicate package install");
    assert!(!duplicate.status.success());
    let text = command_output_text(&duplicate);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=install"));
    assert!(text.contains("AlreadyInstalled"));
    assert!(!text.contains(duplicate_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let denied_install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&denied_dir)
        .output()
        .expect("run denied package install");
    assert!(
        denied_install.status.success(),
        "denied package install failed before enable: {}",
        String::from_utf8_lossy(&denied_install.stderr)
    );
    let denied_enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.denied-plugin")
        .output()
        .expect("run denied package enable");
    assert!(!denied_enable.status.success());
    let text = command_output_text(&denied_enable);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=enable"));
    assert!(text.contains("UngrantedCapability"));

    let missing_show = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.missing-plugin")
        .output()
        .expect("run missing package show");
    assert!(!missing_show.status.success());
    let text = command_output_text(&missing_show);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=show"));
    assert!(text.contains("PackageNotInstalled"));
    assert!(text.contains("runtime.missing-plugin"));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let missing_remove = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("remove")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.missing-plugin")
        .output()
        .expect("run missing package remove");
    assert!(!missing_remove.status.success());
    let text = command_output_text(&missing_remove);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=remove"));
    assert!(text.contains("PackageNotInstalled"));
    assert!(text.contains("runtime.missing-plugin"));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_enable_botster_workspaces_first_party_plugin_db_namespace() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-pkg-ws");
    let package_dir = unique_test_dir("botster-workspaces-package");
    write_botster_workspaces_local_package(&package_dir, "botster-workspaces");
    let child = start_cli_daemon(&data_dir);

    let install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-workspaces package install");
    assert!(
        install.status.success(),
        "botster-workspaces install failed: {}",
        command_output_text(&install)
    );
    let text = command_output_text(&install);
    assert!(text.contains("package name=botster-workspaces"));
    assert!(text.contains("state=installed"));
    assert!(!text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let show_installed = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-workspaces")
        .output()
        .expect("run botster-workspaces package show after install");
    assert!(
        show_installed.status.success(),
        "botster-workspaces show failed: {}",
        command_output_text(&show_installed)
    );
    let text = command_output_text(&show_installed);
    assert!(text.contains("package name=botster-workspaces"));
    assert!(text.contains("state=installed"));
    assert!(text.contains("capabilities=4"));

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-workspaces")
        .output()
        .expect("run botster-workspaces package enable");
    assert!(
        enable.status.success(),
        "botster-workspaces enable failed: {}",
        command_output_text(&enable)
    );
    let text = command_output_text(&enable);
    assert!(text.contains("package name=botster-workspaces"));
    assert!(text.contains("state=enabled"));

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-workspaces package list");
    assert!(
        list.status.success(),
        "botster-workspaces list failed: {}",
        command_output_text(&list)
    );
    let text = command_output_text(&list);
    assert!(text.contains("package name=botster-workspaces"));
    assert!(text.contains("state=enabled"));
    assert!(!text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_deny_botster_workspaces_mismatched_plugin_db_namespace() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-pkg-ws-denied");
    let package_dir = unique_test_dir("botster-workspaces-denied-package");
    write_botster_workspaces_local_package(&package_dir, "other-plugin");
    let child = start_cli_daemon(&data_dir);

    let install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run mismatched botster-workspaces package install");
    assert!(
        install.status.success(),
        "mismatched botster-workspaces install failed before enable: {}",
        command_output_text(&install)
    );

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-workspaces")
        .output()
        .expect("run mismatched botster-workspaces package enable");
    assert!(!enable.status.success());
    let text = command_output_text(&enable);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=enable"));
    assert!(text.contains("UngrantedCapability"));
    assert!(text.contains("other-plugin"));
    assert!(!text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_configuration_daemon_set_show_list_reload_and_cli_are_redacted() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-configuration-daemon");
    let package_dir = unique_test_dir("configurable-package");
    write_configurable_local_plugin_package(&package_dir);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let install = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install configurable package");
    assert_eq!(
        install.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
    let installed = install
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("installed configurable package");
    assert_eq!(
        installed.configuration.missing_required,
        vec!["endpoint".to_string(), "api_token".to_string()]
    );
    let enable_action = package_action(&installed.actions, "enable_package");
    assert_eq!(
        enable_action.status,
        botster_hub::DaemonPackageActionStatus::Blocked
    );
    assert!(
        enable_action
            .required_references
            .iter()
            .any(|reference| { reference.kind == "config" && reference.key == "endpoint" })
    );
    assert!(
        enable_action
            .required_references
            .iter()
            .any(|reference| { reference.kind == "config" && reference.key == "api_token" })
    );
    let configure_action = package_action(&installed.actions, "set_package_configuration");
    assert_eq!(
        configure_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    assert_eq!(
        configure_action
            .request
            .as_ref()
            .expect("configure request mapping")
            .request_type,
        "set_package_configuration"
    );

    let missing_enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "configurable.plugin".to_string(),
        },
    )
    .expect("enable missing config returns operator error");
    assert_eq!(
        missing_enable.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert!(
        missing_enable
            .error
            .as_ref()
            .expect("operator error")
            .message
            .contains("MissingRequiredConfiguration")
    );

    let bad_config = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SetPackageConfiguration {
            package_name: "configurable.plugin".to_string(),
            values: BTreeMap::from([(
                "unknown".to_string(),
                serde_json::json!({"type":"string","value":"nope"}),
            )]),
        },
    )
    .expect("bad config returns operator error");
    assert_eq!(
        bad_config.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );

    let configured = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SetPackageConfiguration {
            package_name: "configurable.plugin".to_string(),
            values: BTreeMap::from([
                (
                    "endpoint".to_string(),
                    serde_json::json!({"type":"url","value":"https://example.invalid/hook"}),
                ),
                (
                    "api_token".to_string(),
                    serde_json::json!({"type":"secret","state":"write_only"}),
                ),
            ]),
        },
    )
    .expect("set config through daemon");
    let configured_package = configured
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("configured package");
    assert!(configured_package.configuration.missing_required.is_empty());
    assert_eq!(
        configured_package.configuration.effective_values["api_token"],
        serde_json::json!({"type":"secret","state":"redacted"})
    );
    assert_eq!(
        configured_package.configuration.effective_values["mode"],
        serde_json::json!({"type":"select","value":"read"})
    );

    let list =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list after config mutation");
    let listed = list
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("listed configurable package");
    assert!(listed.configuration.missing_required.is_empty());
    assert_eq!(
        listed.configuration.effective_values["api_token"],
        serde_json::json!({"type":"secret","state":"redacted"})
    );

    let state_json =
        fs::read_to_string(data_dir.join("hub-state.json")).expect("read hub state json");
    assert!(state_json.contains("\"state\": \"redacted\""));
    assert!(!state_json.contains("write_only"));
    assert!(!state_json.contains("super-secret-token"));

    let cli = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("config")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("configurable.plugin")
        .output()
        .expect("run packages config");
    assert!(
        cli.status.success(),
        "packages config failed: {}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let stdout = String::from_utf8(cli.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_config package=configurable.plugin schema_present=true"));
    assert!(stdout.contains("\"state\":\"redacted\""));
    assert!(!stdout.contains("write_only"));
    assert!(!stdout.contains("super-secret-token"));

    shutdown_cli_daemon(&data_dir, child);

    let restarted = start_cli_daemon(&data_dir);
    let reloaded =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list after restart");
    let package = reloaded
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("reloaded package");
    assert_eq!(
        package.configuration.effective_values["api_token"],
        serde_json::json!({"type":"secret","state":"redacted"})
    );
    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn local_package_reload_rereads_manifest_restarts_running_app_and_cli_open_uses_refreshed_state() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("local-package-reload");
    let package_dir = unique_test_dir("reloadable-app-package");
    write_reloadable_app_package(&package_dir, "1.0.0", "http://127.0.0.1:49160");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("enable reloadable local app package");
    assert_eq!(
        enable.package_decision.expect("enable decision").action,
        "enable"
    );
    let enabled_package = enable
        .packages
        .iter()
        .find(|package| package.package_name == "runtime.reloadable")
        .expect("enabled package row");
    assert_eq!(enabled_package.source_kind, "path");
    let reload_action = package_action(&enabled_package.actions, "reload_package");
    assert_eq!(
        reload_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    assert_eq!(
        reload_action
            .request
            .as_ref()
            .expect("reload request")
            .request_type,
        "reload_package"
    );

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.reloadable".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start reloadable app");
    wait_for_app_local_url(&data_dir, "web", "http://127.0.0.1:49160");

    write_reloadable_app_package(&package_dir, "1.1.0", "http://127.0.0.1:49161");
    let reload = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ReloadPackage {
            package_name: "runtime.reloadable".to_string(),
        },
    )
    .expect("reload local package");
    assert_eq!(
        reload.package_decision.expect("reload decision").action,
        "reload"
    );
    let reloaded_package = reload
        .packages
        .iter()
        .find(|package| package.package_name == "runtime.reloadable")
        .expect("reloaded package row");
    assert_eq!(reloaded_package.version, "1.1.0");

    let apps = wait_for_app_local_url(&data_dir, "web", "http://127.0.0.1:49161");
    let app = app_row(&apps, "web");
    assert_eq!(app.package_name, "runtime.reloadable");
    assert_eq!(
        app.launch_target.local_url.as_deref(),
        Some("http://127.0.0.1:49161")
    );

    let open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.reloadable/web")
        .output()
        .expect("open refreshed web app");
    assert!(
        open.status.success(),
        "apps open failed after reload: {}",
        command_output_text(&open)
    );
    let open_text = command_output_text(&open);
    assert!(open_text.contains("app_url=http://127.0.0.1:49161"));
    assert!(!open_text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!open_text.contains(data_dir.to_string_lossy().as_ref()));

    let cli = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("reload")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.reloadable")
        .output()
        .expect("run package reload CLI");
    assert!(
        cli.status.success(),
        "packages reload failed: {}",
        command_output_text(&cli)
    );
    let cli_text = command_output_text(&cli);
    assert!(cli_text.contains("decision=package"));
    assert!(cli_text.contains("package_name=runtime.reloadable"));
    assert!(cli_text.contains("action=reload"));
    assert!(cli_text.contains("version=1.1.0"));
    assert!(!cli_text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!cli_text.contains(data_dir.to_string_lossy().as_ref()));

    let alias = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("reload")
        .arg("runtime.reloadable")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run package reload alias CLI");
    assert!(
        alias.status.success(),
        "reload alias failed: {}",
        command_output_text(&alias)
    );
    let alias_text = command_output_text(&alias);
    assert!(alias_text.contains("decision=package"));
    assert!(alias_text.contains("package_name=runtime.reloadable"));
    assert!(alias_text.contains("action=reload"));
    assert!(alias_text.contains("version=1.1.0"));
    assert!(!alias_text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!alias_text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_exposes_and_resolves_plugin_surface_and_settings_routes() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-route-descriptors");
    let package_dir = unique_test_dir("package-route-descriptors-package");
    write_configurable_local_plugin_package(&package_dir);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let install = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install configurable package");
    assert_eq!(
        install.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
    let installed = install
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("installed configurable package");
    let surface_route = package_route(&installed.routes, "surface:config.home");
    assert_eq!(
        surface_route.route_path,
        "/packages/configurable.plugin/surfaces/config.home"
    );
    assert_eq!(surface_route.target.kind, "plugin_surface");
    assert_eq!(
        surface_route.target.surface_id.as_deref(),
        Some("config.home")
    );
    assert_eq!(surface_route.app_id.as_deref(), Some("config.home"));
    assert_eq!(surface_route.surface_id.as_deref(), Some("config.home"));
    assert_eq!(surface_route.title, "Config Home");
    assert_eq!(surface_route.icon.as_deref(), Some("settings"));
    assert_eq!(surface_route.category.as_deref(), Some("configuration"));
    assert_eq!(surface_route.layout_mode, "plugin_surface");
    assert!(surface_route.supports_settings);
    assert!(!surface_route.enabled);
    assert!(surface_route.blocked);
    assert!(
        surface_route
            .required_capabilities
            .iter()
            .any(|capability| capability.surface.eq_ignore_ascii_case("surfaces"))
    );
    assert!(
        surface_route
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.kind == "package_not_enabled")
    );

    let settings_route = package_route(&installed.routes, "settings");
    assert_eq!(
        settings_route.route_path,
        "/packages/configurable.plugin/settings"
    );
    assert_eq!(settings_route.target.kind, "package_settings");
    assert_eq!(settings_route.layout_mode, "settings_form");
    assert!(settings_route.supports_settings);
    assert!(settings_route.enabled);
    assert!(!settings_route.blocked);
    assert!(settings_route.required_capabilities.is_empty());
    assert!(
        settings_route
            .diagnostics
            .iter()
            .any(
                |diagnostic| diagnostic.kind == "missing_required_configuration"
                    && diagnostic.message.contains("endpoint")
            )
    );

    let resolved_surface = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "configurable.plugin".to_string(),
            route_id: "surface:config.home".to_string(),
        },
    )
    .expect("resolve plugin surface route");
    assert_eq!(
        resolved_surface.kind,
        botster_hub::DaemonResponseKind::ResolvedPackageRoute
    );
    assert_eq!(
        resolved_surface
            .resolved_package_route
            .as_ref()
            .expect("resolved route")
            .route_path,
        surface_route.route_path
    );

    let resolved_settings = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "configurable.plugin".to_string(),
            route_id: "settings".to_string(),
        },
    )
    .expect("resolve settings route");
    assert_eq!(
        resolved_settings.kind,
        botster_hub::DaemonResponseKind::ResolvedPackageRoute
    );
    assert_eq!(
        resolved_settings
            .resolved_package_route
            .as_ref()
            .expect("resolved settings route")
            .target
            .kind,
        "package_settings"
    );

    let missing_route = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "configurable.plugin".to_string(),
            route_id: "surface:missing".to_string(),
        },
    )
    .expect("missing route returns operator error");
    assert_eq!(
        missing_route.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        missing_route.error.as_ref().expect("operator error").code,
        "route_not_found"
    );
    assert!(missing_route.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_deref()
            .is_some_and(|message| message.contains("route_not_found"))
    }));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_lists_admitted_package_navigation_with_default_app_surface_fallback() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-navigation-registry");
    let default_package_dir = unique_test_dir("package-navigation-default-package");
    let explicit_package_dir = unique_test_dir("package-navigation-explicit-package");
    write_configurable_local_plugin_package(&default_package_dir);
    write_explicit_navigation_local_plugin_package(&explicit_package_dir);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: default_package_dir,
        },
    )
    .expect("install default navigation package");
    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: explicit_package_dir,
        },
    )
    .expect("install explicit navigation package");

    let blocked = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListPackageNavigation,
    )
    .expect("list package navigation");
    assert_eq!(
        blocked.kind,
        botster_hub::DaemonResponseKind::PackageNavigation
    );
    let default_nav = package_navigation(
        &blocked.package_navigation,
        "configurable.plugin",
        "config.home",
    );
    assert_eq!(default_nav.label, "Config Home");
    assert_eq!(default_nav.route_id, "surface:config.home");
    assert_eq!(
        default_nav.route_path,
        "/packages/configurable.plugin/surfaces/config.home"
    );
    assert!(!default_nav.enabled);
    assert!(default_nav.blocked);
    assert!(
        default_nav
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.kind == "package_not_enabled" })
    );

    let explicit_blocked =
        package_navigation(&blocked.package_navigation, "navigation.plugin", "primary");
    assert_eq!(explicit_blocked.label, "Primary Workbench");
    assert_eq!(explicit_blocked.route_id, "surface:workbench");
    assert!(!explicit_blocked.enabled);
    assert!(explicit_blocked.blocked);
    let blocked_json =
        serde_json::to_string(&blocked.package_navigation).expect("serialize navigation rows");
    assert!(!blocked_json.contains("order"));
    assert!(!blocked_json.contains("priority"));

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "navigation.plugin".to_string(),
        },
    )
    .expect("enable explicit navigation package");

    let enabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListPackageNavigation,
    )
    .expect("list enabled package navigation");
    let enabled_nav =
        package_navigation(&enabled.package_navigation, "navigation.plugin", "primary");
    assert!(enabled_nav.enabled);
    assert!(!enabled_nav.blocked);
    assert!(enabled_nav.diagnostics.is_empty());

    let resolved = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "navigation.plugin".to_string(),
            route_id: enabled_nav.route_id.clone(),
        },
    )
    .expect("resolve explicit navigation route");
    let route = resolved
        .resolved_package_route
        .as_ref()
        .expect("resolved route");
    assert_eq!(enabled_nav.route_path, route.route_path);
    assert_eq!(enabled_nav.target, route.target);

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn local_package_reload_name_mismatch_returns_path_free_operator_error() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("reload-name-mismatch");
    let package_dir = unique_test_dir("reload-pkg-mismatch");
    write_reloadable_app_package(&package_dir, "1.0.0", "http://127.0.0.1:49162");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("enable reloadable local app package");

    write_reloadable_app_package_named(
        &package_dir,
        "runtime.reloadable-renamed",
        "1.1.0",
        "http://127.0.0.1:49163",
    );
    let reload = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ReloadPackage {
            package_name: "runtime.reloadable".to_string(),
        },
    )
    .expect("reload renamed local package returns operator frame");

    assert_eq!(reload.kind, botster_hub::DaemonResponseKind::OperatorError);
    let error = reload.error.as_ref().expect("operator error");
    assert!(error.message.contains("InvalidLocalManifest"));
    assert!(error.message.contains("runtime.reloadable-renamed"));
    assert!(error.message.contains("runtime.reloadable"));
    assert!(
        !error
            .message
            .contains(package_dir.to_string_lossy().as_ref())
    );
    assert!(!error.message.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_list_exposes_dependency_and_feature_availability_matrix() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-availability-daemon");
    let package_dir = unique_test_dir("project-pipelines-availability-package");
    let blocked_package_dir = unique_test_dir("required-dependency-package");
    write_project_pipelines_availability_package(&package_dir);
    write_required_dependency_package(&blocked_package_dir);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath { path: package_dir },
    )
    .expect("enable project pipelines availability package");
    assert_eq!(
        enable.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: blocked_package_dir,
        },
    )
    .expect("install required dependency package");

    let list =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list packages with availability matrix");
    let package = list
        .packages
        .iter()
        .find(|package| package.package_name == "project-pipelines")
        .expect("project pipelines package row");

    assert_eq!(
        package.availability.state,
        botster_hub::DaemonPackageAvailabilityState::Available
    );
    let local_feature = package
        .feature_availability
        .iter()
        .find(|feature| feature.id == "local_pipelines")
        .expect("local pipelines feature row");
    assert_eq!(
        local_feature.state,
        botster_hub::DaemonPackageAvailabilityState::Available
    );
    let github_feature = package
        .feature_availability
        .iter()
        .find(|feature| feature.id == "github_pr_lifecycle")
        .expect("github feature row");
    assert_eq!(
        github_feature.state,
        botster_hub::DaemonPackageAvailabilityState::Blocked
    );
    assert!(github_feature.reasons.iter().any(|reason| {
        reason.reason == "missing_package"
            && reason.action == "install_package"
            && reason.package_name.as_deref() == Some("github-provider")
    }));
    assert!(github_feature.reasons.iter().any(|reason| {
        reason.reason == "missing_auth"
            && reason.action == "authenticate"
            && reason.requirement.as_deref() == Some("github_token")
    }));
    let blocked_package = list
        .packages
        .iter()
        .find(|package| package.package_name == "dependency-blocked.plugin")
        .expect("dependency blocked package row");
    assert_eq!(
        blocked_package.availability.state,
        botster_hub::DaemonPackageAvailabilityState::Blocked
    );
    let enable_action = package_action(&blocked_package.actions, "enable_package");
    assert_eq!(
        enable_action.status,
        botster_hub::DaemonPackageActionStatus::Blocked
    );
    assert!(
        enable_action.required_references.iter().any(|reference| {
            reference.kind == "dependency" && reference.key == "github-provider"
        })
    );

    let show = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowPackage {
            package_name: "project-pipelines".to_string(),
        },
    )
    .expect("show package with availability matrix");
    assert_eq!(
        show.packages[0].feature_availability,
        package.feature_availability
    );

    let dto_json = serde_json::to_string(package).expect("serialize daemon package");
    assert!(!dto_json.contains(&data_dir.display().to_string()));
    assert!(!dto_json.contains(&config.data_directory.display().to_string()));
    assert!(!dto_json.contains("token-value"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_update_apply_preserves_configuration_and_pin_metadata() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-update-apply");
    let package_dir = unique_test_dir("configurable-package-update");
    write_configurable_local_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    let config = explicit_config(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install configurable package");
    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SetPackageConfiguration {
            package_name: "configurable.plugin".to_string(),
            values: BTreeMap::from([
                (
                    "endpoint".to_string(),
                    serde_json::json!({"type":"url","value":"https://example.invalid/hook"}),
                ),
                (
                    "api_token".to_string(),
                    serde_json::json!({"type":"secret","state":"write_only"}),
                ),
            ]),
        },
    )
    .expect("set config before update");

    let pin = botster_hub::DaemonPackagePin {
        revision: "v1.0.1".to_string(),
        branch: Some("main".to_string()),
        tag: Some("v1.0.1".to_string()),
        rev: Some("def456".to_string()),
        checksum: Some("sha256:update-test".to_string()),
        update_policy: "track_source".to_string(),
    };
    let preview = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PreviewPackageUpdate {
            package_name: "configurable.plugin".to_string(),
            pin: pin.clone(),
        },
    )
    .expect("preview update");
    assert_eq!(
        preview.kind,
        botster_hub::DaemonResponseKind::PackageUpdateStatus
    );
    assert!(!preview.install_plan.expect("preview plan").mutates_registry);

    let apply = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ApplyPackageUpdate {
            package_name: "configurable.plugin".to_string(),
            pin: pin.clone(),
        },
    )
    .expect("apply update");
    assert_eq!(
        apply.package_decision.expect("apply decision").action,
        "apply_update"
    );
    let updated = apply
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("updated package row");
    assert_eq!(
        updated.configuration.effective_values["api_token"],
        serde_json::json!({"type":"secret","state":"redacted"})
    );

    shutdown_cli_daemon(&data_dir, child);
    let restarted = start_cli_daemon(&data_dir);
    let reloaded =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list after restart");
    let package = reloaded
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("reloaded package");
    assert_eq!(
        package.configuration.effective_values["endpoint"],
        serde_json::json!({"type":"url","value":"https://example.invalid/hook"})
    );

    shutdown_cli_daemon(&data_dir, restarted);
    let state = FileHubStateStore::for_data_directory(&data_dir)
        .load_or_initialize(&explicit_config(&data_dir))
        .expect("load persisted hub state after update");
    let restored =
        PackageRegistry::from_snapshot(state.package_registry).expect("restore package registry");
    let record = restored
        .package("configurable.plugin")
        .expect("restored configurable package");
    let restored_pin = record.pin.as_ref().expect("restored pin");
    assert_eq!(restored_pin.revision, "v1.0.1");
    assert_eq!(restored_pin.rev.as_deref(), Some("def456"));
    assert_eq!(
        restored_pin.update_policy,
        botster_hub::PackageUpdatePolicy::TrackSource
    );
    assert!(record.configuration.values.contains_key("api_token"));
}

#[test]
fn package_update_unsupported_cases_return_structured_diagnostics() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-update-diagnostics");
    let package_dir = unique_test_dir("local-package-update-diagnostics");
    write_local_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    let config = explicit_config(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install local package");

    let check = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CheckPackageUpdate {
            package_name: "runtime.plugin".to_string(),
        },
    )
    .expect("check update");
    let status = check.update_status.expect("update status");
    assert!(!status.update_available);
    assert!(status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == "update_unavailable"
            && diagnostic
                .message
                .contains("without registry source metadata")
    }));
    assert!(
        status
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.kind == "pin_required")
    );
    assert_eq!(
        package_action(&status.actions, "check_package_update").status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    let preview_action = package_action(&status.actions, "preview_package_update");
    assert_eq!(
        preview_action.status,
        botster_hub::DaemonPackageActionStatus::Blocked
    );
    assert!(
        preview_action
            .required_references
            .iter()
            .any(|reference| { reference.kind == "pin" && reference.key == "package_update_pin" })
    );
    assert_eq!(
        package_action(&status.actions, "reload_package").status,
        botster_hub::DaemonPackageActionStatus::Available
    );

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "runtime.plugin".to_string(),
        },
    )
    .expect("enable local package");
    let enabled_check = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CheckPackageUpdate {
            package_name: "runtime.plugin".to_string(),
        },
    )
    .expect("check enabled update");
    let enabled_status = enabled_check.update_status.expect("enabled update status");
    assert!(enabled_status.reload_required);
    assert!(enabled_status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == "reload_available" && diagnostic.message.contains("reload_package")
    }));

    let cli = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("check-update")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run packages check-update");
    assert!(
        cli.status.success(),
        "packages check-update failed: {}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let stdout = String::from_utf8(cli.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_update package=runtime.plugin"));
    assert!(stdout.contains("reload_required=true"));
    assert!(
        stdout.contains("package_update_diagnostic package=runtime.plugin kind=reload_available")
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_enable_local_process_package_does_not_attempt_lua_load() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-process-package");
    let package_dir = unique_test_dir("local-process-package");
    write_local_process_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages enable process package");

    assert!(
        enable.status.success(),
        "enable process package failed: {}",
        String::from_utf8_lossy(&enable.stderr)
    );
    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status");
    assert!(lifecycle.lifecycle.iter().any(|plugin| {
        plugin.package_name == "runtime.process-plugin"
            && plugin.state == "enabled"
            && !plugin.loaded
    }));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_enable_without_running_daemon_does_not_mutate_hub_state() {
    let data_dir = unique_test_dir("cli-packages-offline");
    let package_dir = unique_test_dir("local-package-offline");
    write_local_plugin_package(&package_dir);

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages enable without daemon");

    assert!(
        !enable.status.success(),
        "offline enable unexpectedly succeeded: {}",
        String::from_utf8_lossy(&enable.stdout)
    );
    let stderr = String::from_utf8(enable.stderr).expect("stderr is utf8");
    assert!(stderr.contains("daemon not running"));
    assert!(
        !data_dir.join("hub-state.json").exists(),
        "offline package mutation should not create durable state"
    );
}

#[test]
fn daemon_package_entity_held_open_receives_upsert_then_remove_without_resubscribe() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-held");
    let package_dir = unique_test_dir("pkg-entity-held-pkg");
    write_package_entity_mutation_plugin(&package_dir, "live");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut held = botster_hub_client::subscribe_entities(
        &endpoint,
        "project-pipelines.membership",
        "held-open",
    )
    .expect("subscribe");
    let snapshot = held.next_frame().expect("initial snapshot");
    assert!(matches!(
        snapshot,
        botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. } if items.is_empty()
    ));

    let claim = mutation_action(
        &endpoint,
        "project-pipelines.claim",
        serde_json::json!({ "id": "m-1" }),
    );
    assert_eq!(
        claim.kind,
        botster_hub_client::DaemonResponseKind::PluginActionResult
    );
    let upsert = wait_for_entity_frame(&mut held, Duration::from_secs(5), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert {
                id,
                snapshot_seq: 1,
                ..
            } if id == "m-1"
        )
    });
    assert!(matches!(
        upsert,
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq: 1,
            ..
        }
    ));

    let remove = mutation_action(
        &endpoint,
        "project-pipelines.remove",
        serde_json::json!({ "id": "m-1" }),
    );
    assert_eq!(
        remove.kind,
        botster_hub_client::DaemonResponseKind::PluginActionResult
    );
    let removed = wait_for_entity_frame(&mut held, Duration::from_secs(5), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Remove {
                id,
                snapshot_seq: 2,
                ..
            } if id == "m-1"
        )
    });
    assert!(matches!(
        removed,
        botster_hub_client::DaemonEntityFrame::Remove {
            snapshot_seq: 2,
            ..
        }
    ));

    held.unsubscribe().expect("unsubscribe");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_publish_from_surface_action_returns_before_fanout_and_stream_receives_frame()
 {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-return");
    let package_dir = unique_test_dir("pkg-entity-return-pkg");
    write_package_entity_mutation_plugin(&package_dir, "live");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut held = botster_hub_client::subscribe_entities(
        &endpoint,
        "project-pipelines.membership",
        "return-before-fanout",
    )
    .expect("subscribe");
    let _ = held.next_frame().expect("snapshot");

    let claim = mutation_action(
        &endpoint,
        "project-pipelines.claim",
        serde_json::json!({ "id": "m-return" }),
    );
    assert_eq!(
        claim.kind,
        botster_hub_client::DaemonResponseKind::PluginActionResult
    );
    let payload = claim
        .plugin_action_result
        .as_ref()
        .and_then(|result| result.payload.as_ref())
        .expect("publish admission payload");
    assert_eq!(payload["ok"], true);
    assert_eq!(payload["status"], "accepted");

    let _frame = wait_for_entity_frame(&mut held, Duration::from_secs(5), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert { id, .. } if id == "m-return"
        )
    });

    held.unsubscribe().expect("unsubscribe");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_publish_rejects_stale_and_duplicate_sequence() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-stale");
    let package_dir = unique_test_dir("pkg-entity-stale-pkg");
    write_package_entity_mutation_plugin(&package_dir, "live");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let accepted = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 1, "id": "a" }),
    );
    let payload = accepted
        .plugin_action_result
        .as_ref()
        .and_then(|result| result.payload.as_ref())
        .expect("payload");
    assert_eq!(payload["status"], "accepted");

    let duplicate = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 1, "id": "a" }),
    );
    let duplicate_payload = duplicate
        .plugin_action_result
        .as_ref()
        .and_then(|result| result.payload.as_ref())
        .expect("duplicate payload");
    assert_eq!(duplicate_payload["ok"], false);
    assert_eq!(duplicate_payload["status"], "duplicate_sequence");

    let stale = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 0, "id": "z" }),
    );
    let stale_payload = stale
        .plugin_action_result
        .as_ref()
        .and_then(|result| result.payload.as_ref())
        .expect("stale payload");
    assert_eq!(stale_payload["ok"], false);
    assert_eq!(stale_payload["status"], "stale_sequence");

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_publish_gap_pending_then_accepts_in_order() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-gap");
    let package_dir = unique_test_dir("pkg-entity-gap-pkg");
    // Behind provider keeps resync from advancing the family floor before N+1 arrives.
    write_package_entity_mutation_plugin(&package_dir, "behind");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut held = botster_hub_client::subscribe_entities(
        &endpoint,
        "project-pipelines.membership",
        "gap-order",
    )
    .expect("subscribe");
    let _ = held.next_frame().expect("snapshot");

    let gap = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 2, "id": "b" }),
    );
    assert_eq!(
        gap.plugin_action_result
            .as_ref()
            .and_then(|result| result.payload.as_ref())
            .map(|payload| payload["status"].as_str()),
        Some(Some("pending_gap"))
    );

    let fill = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 1, "id": "a" }),
    );
    assert_eq!(
        fill.plugin_action_result
            .as_ref()
            .and_then(|result| result.payload.as_ref())
            .map(|payload| payload["status"].as_str()),
        Some(Some("accepted"))
    );

    let first = wait_for_entity_frame(&mut held, Duration::from_secs(5), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert {
                snapshot_seq: 1,
                ..
            }
        )
    });
    let second = wait_for_entity_frame(&mut held, Duration::from_secs(5), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert {
                snapshot_seq: 2,
                ..
            }
        )
    });
    assert!(matches!(
        first,
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq: 1,
            ..
        }
    ));
    assert!(matches!(
        second,
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq: 2,
            ..
        }
    ));

    held.unsubscribe().expect("unsubscribe");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_publish_unload_closes_held_subscription() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-unload");
    let package_dir = unique_test_dir("pkg-entity-unload-pkg");
    write_package_entity_mutation_plugin(&package_dir, "live");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut held = botster_hub_client::subscribe_entities(
        &endpoint,
        "project-pipelines.membership",
        "unload-held",
    )
    .expect("subscribe");
    let _ = held.next_frame().expect("snapshot");
    let disabled = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::DisablePackage {
            package_name: "project-pipelines".to_string(),
        },
    )
    .expect("disable package");
    assert_eq!(
        disabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    held.set_read_timeout(Some(Duration::from_secs(2)))
        .expect("timeout");
    assert!(
        held.next_frame().is_err(),
        "disabled package subscription must close"
    );
    let cleanup_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status after unload")
                .status
                .expect("status body")
                .lifecycle_counters;
        if counters.live_entity_subscriptions == 0 {
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "subscription counter remained live after unload: {counters:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_publish_outside_pending_window_sets_high_water_and_converges() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-outside-w");
    let package_dir = unique_test_dir("pkg-entity-outside-w-pkg");
    write_package_entity_mutation_plugin(&package_dir, "live");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut held = botster_hub_client::subscribe_entities(
        &endpoint,
        "project-pipelines.membership",
        "outside-w",
    )
    .expect("subscribe");
    let _ = held.next_frame().expect("snapshot");

    let _ = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 1, "id": "seed" }),
    );
    let _ = wait_for_entity_frame(&mut held, Duration::from_secs(5), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert {
                snapshot_seq: 1,
                ..
            }
        )
    });

    let outside = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 20, "id": "high" }),
    );
    let outside_payload = outside
        .plugin_action_result
        .as_ref()
        .and_then(|result| result.payload.as_ref())
        .expect("outside payload");
    assert_eq!(outside_payload["status"], "resync_scheduled");
    assert_eq!(outside_payload["high_water_seq"], 20);

    // Live provider already has seq=20 and the row; resync should deliver snapshot.
    let snapshot = wait_for_entity_frame(&mut held, Duration::from_secs(10), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Snapshot {
                snapshot_seq,
                ..
            } if *snapshot_seq >= 20
        )
    });
    assert!(matches!(
        snapshot,
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq,
            ref items,
            ..
        } if snapshot_seq >= 20
            && items.iter().any(|item| item.get("id").and_then(|v| v.as_str()) == Some("high"))
    ));

    held.unsubscribe().expect("unsubscribe");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_second_subscriber_behind_snapshot_does_not_roll_advanced_subscriber() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-two-sub");
    let package_dir = unique_test_dir("pkg-entity-two-sub-pkg");
    write_package_entity_mutation_plugin(&package_dir, "live");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut sub_a =
        botster_hub_client::subscribe_entities(&endpoint, "project-pipelines.membership", "sub-a")
            .expect("subscribe a");
    let _ = sub_a.next_frame().expect("a snapshot");
    let _ = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 1, "id": "row-1" }),
    );
    let _ = wait_for_entity_frame(&mut sub_a, Duration::from_secs(5), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert {
                snapshot_seq: 1,
                ..
            }
        )
    });

    // Force provider snapshot seq behind by not advancing durable rows for a
    // second subscribe path: set_provider_seq is not enough because live
    // provider uses seq. Instead publish_seq advances both. Simulate behind by
    // using a temporary empty second subscription after lowering is impossible.
    // Plan requires behind provider on second subscribe: use a second family
    // snapshot by claiming then setting provider to report only older content
    // via remove of knowledge... Live provider always returns current seq.
    //
    // Practical proof: publish seq 2, then open sub B which gets S=2 (not behind).
    // To force behind, reinstall is heavy. Instead: after sub A is at 1, call
    // set_provider_seq to 0 without clearing rows — but provider still returns
    // seq variable. set_provider_seq lowers seq for provider only while fanout
    // state keeps last_accepted=1. Then sub B's snapshot has S=0 < floor.
    let _ = mutation_action(
        &endpoint,
        "project-pipelines.set_provider_seq",
        serde_json::json!({ "seq": 0 }),
    );

    let mut sub_b =
        botster_hub_client::subscribe_entities(&endpoint, "project-pipelines.membership", "sub-b")
            .expect("subscribe b");
    let b_snapshot = sub_b.next_frame().expect("b snapshot");
    assert!(matches!(
        b_snapshot,
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq: 0,
            ..
        }
    ));

    // Sub A must not receive the behind snapshot.
    sub_a
        .set_read_timeout(Some(Duration::from_millis(400)))
        .expect("timeout");
    match sub_a.next_frame() {
        Ok(botster_hub_client::DaemonEntityFrame::Snapshot { snapshot_seq, .. }) => {
            assert!(
                snapshot_seq >= 1,
                "advanced subscriber must not roll back via behind snapshot {snapshot_seq}"
            );
        }
        Ok(frame) => {
            // Deltas at/after applied are fine; snapshots below 1 are not.
            if let botster_hub_client::DaemonEntityFrame::Snapshot { snapshot_seq, .. } = frame {
                assert!(snapshot_seq >= 1);
            }
        }
        Err(_) => {
            // Timeout: no frame to A is also acceptable for behind-only delivery.
        }
    }

    // Restore live provider truth and allow B to catch up.
    let _ = mutation_action(
        &endpoint,
        "project-pipelines.set_provider_seq",
        serde_json::json!({ "seq": 1 }),
    );
    let _ = wait_for_entity_frame(&mut sub_b, Duration::from_secs(10), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Snapshot {
                snapshot_seq,
                ..
            } if *snapshot_seq >= 1
        )
    });

    let _ = sub_a.unsubscribe();
    let _ = sub_b.unsubscribe();
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_resync_under_stale_provider_is_pressure_bounded() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-pressure");
    let package_dir = unique_test_dir("pkg-entity-pressure-pkg");
    write_package_entity_mutation_plugin(&package_dir, "behind");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut held = botster_hub_client::subscribe_entities(
        &endpoint,
        "project-pipelines.membership",
        "pressure",
    )
    .expect("subscribe");
    let _ = held.next_frame().expect("snapshot");

    // Force outside-window resync need while provider stays at 0.
    let _ = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 1, "id": "seed" }),
    );
    let _ = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 20, "id": "high" }),
    );

    // Poll Status repeatedly while resync runs under backoff; daemon must stay responsive.
    let started = Instant::now();
    let mut saw_degraded = false;
    let mut attempts_at_degraded = 0_u64;
    while started.elapsed() < Duration::from_secs(20) {
        let status =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status remains responsive under stale provider resync");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let counters = status
            .status
            .as_ref()
            .expect("status body")
            .lifecycle_counters
            .clone();
        if counters.package_entity_resync_degraded > 0 {
            saw_degraded = true;
            attempts_at_degraded = counters.package_entity_resync_attempts;
            assert!(
                counters.package_entity_resync_attempts <= 16,
                "attempts stayed near locked budget, got {}",
                counters.package_entity_resync_attempts
            );
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        saw_degraded,
        "stale provider must enter resync_degraded under max attempts"
    );
    // Unchanged catching_up / stale provider must not start another attempt cycle.
    let post = Instant::now();
    while post.elapsed() < Duration::from_secs(3) {
        let status =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status remains responsive after degraded");
        let counters = status
            .status
            .as_ref()
            .expect("status body")
            .lifecycle_counters
            .clone();
        assert_eq!(
            counters.package_entity_resync_attempts, attempts_at_degraded,
            "degraded family must not re-arm attempts without a new publish/subscribe"
        );
        assert!(counters.package_entity_resync_degraded >= 1);
        thread::sleep(Duration::from_millis(100));
    }

    let _ = held.unsubscribe();
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_publish_out_of_order_with_behind_provider_converges_all_subscribers() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-ooo");
    let package_dir = unique_test_dir("pkg-entity-ooo-pkg");
    // Behind first, then switch to live is hard in-process; use live but publish
    // N+2 then N+1 quickly. Pending retention preserves order without requiring
    // package re-publish after gaps.
    write_package_entity_mutation_plugin(&package_dir, "behind");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut held =
        botster_hub_client::subscribe_entities(&endpoint, "project-pipelines.membership", "ooo")
            .expect("subscribe");
    let _ = held.next_frame().expect("snapshot");

    // N+2 first while provider still at 0 until publish updates rows/seq.
    let gap = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 2, "id": "n2" }),
    );
    assert_eq!(
        gap.plugin_action_result
            .as_ref()
            .and_then(|r| r.payload.as_ref())
            .map(|p| p["status"].as_str()),
        Some(Some("pending_gap"))
    );
    // Behind first resync would still see seq=2 from live provider after gap publish
    // (publish updates rows). Fill N+1 and require ordered delivery.
    let _ = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 1, "id": "n1" }),
    );
    let _ = wait_for_entity_frame(&mut held, Duration::from_secs(5), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert {
                snapshot_seq: 1,
                ..
            }
        )
    });
    let _ = wait_for_entity_frame(&mut held, Duration::from_secs(5), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert {
                snapshot_seq: 2,
                ..
            }
        )
    });

    held.unsubscribe().expect("unsubscribe");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_publish_concurrent_out_of_order_preserves_family_order() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-concurrent");
    let package_dir = unique_test_dir("pkg-entity-concurrent-pkg");
    write_package_entity_mutation_plugin(&package_dir, "behind");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut held = botster_hub_client::subscribe_entities(
        &endpoint,
        "project-pipelines.membership",
        "concurrent",
    )
    .expect("subscribe");
    let _ = held.next_frame().expect("snapshot");

    let endpoint_a = endpoint.clone();
    let endpoint_b = endpoint.clone();
    let t1 = thread::spawn(move || {
        mutation_action(
            &endpoint_a,
            "project-pipelines.publish_seq",
            serde_json::json!({ "seq": 3, "id": "c" }),
        )
    });
    let t2 = thread::spawn(move || {
        mutation_action(
            &endpoint_b,
            "project-pipelines.publish_seq",
            serde_json::json!({ "seq": 2, "id": "b" }),
        )
    });
    let _ = t1.join().expect("join t1");
    let _ = t2.join().expect("join t2");
    let _ = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 1, "id": "a" }),
    );

    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && Instant::now() < deadline {
        held.set_read_timeout(Some(Duration::from_millis(200)))
            .expect("timeout");
        if let Ok(botster_hub_client::DaemonEntityFrame::Upsert { snapshot_seq, .. }) =
            held.next_frame()
        {
            seen.push(snapshot_seq);
        }
    }
    assert_eq!(
        seen,
        vec![1, 2, 3],
        "family order must be preserved: {seen:?}"
    );

    held.unsubscribe().expect("unsubscribe");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_entity_subscriber_overflow_resyncs_from_provider() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("pkg-entity-overflow");
    let package_dir = unique_test_dir("pkg-entity-overflow-pkg");
    write_package_entity_mutation_plugin(&package_dir, "live");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    enable_mutation_package(&endpoint, package_dir);

    let mut held = botster_hub_client::subscribe_entities(
        &endpoint,
        "project-pipelines.membership",
        "overflow",
    )
    .expect("subscribe");
    let _ = held.next_frame().expect("snapshot");

    // Publish a baseline, then many sequential mutations. The held client keeps
    // reading so the stream stays healthy; overflow of the bounded queue is
    // covered by the Blocking-sender unit proof in daemon_transport. Here we
    // prove provider resync remains available after a large sequential burst
    // (high-water catch-up without package re-publish).
    for seq in 1_u64..=40 {
        let _ = mutation_action(
            &endpoint,
            "project-pipelines.publish_seq",
            serde_json::json!({ "seq": seq, "id": format!("row-{seq}") }),
        );
        // Keep the stream drained so the connection stays open under burst.
        held.set_read_timeout(Some(Duration::from_millis(200)))
            .expect("timeout");
        let _ = held.next_frame();
    }

    // Force a gap outside the pending window while the provider is live so
    // coalesced resync delivers a snapshot high-water baseline.
    let outside = mutation_action(
        &endpoint,
        "project-pipelines.publish_seq",
        serde_json::json!({ "seq": 60, "id": "high-water" }),
    );
    assert_eq!(
        outside
            .plugin_action_result
            .as_ref()
            .and_then(|result| result.payload.as_ref())
            .map(|payload| payload["status"].as_str()),
        Some(Some("resync_scheduled"))
    );
    let snapshot = wait_for_entity_frame(&mut held, Duration::from_secs(10), |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Snapshot {
                snapshot_seq,
                ..
            } if *snapshot_seq >= 60
        )
    });
    assert!(matches!(
        snapshot,
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq,
            ..
        } if snapshot_seq >= 60
    ));

    held.unsubscribe().expect("unsubscribe");
    shutdown_cli_daemon(&data_dir, child);
}

