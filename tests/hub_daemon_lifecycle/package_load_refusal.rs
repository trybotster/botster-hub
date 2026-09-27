/// A plugin package at `version` whose `<name>.version` tool reports that
/// version, so a caller can tell which loaded version is serving.
fn write_versioned_package(root: &Path, name: &str, version: &str) {
    write_versioned_package_with(root, name, version, "");
}

/// A versioned package whose plugin runs `prelude` before it registers.
fn write_versioned_package_with(root: &Path, name: &str, version: &str, prelude: &str) {
    fs::create_dir_all(root).expect("create versioned package root");
    fs::write(
        root.join("plugin.lua"),
        format!(
            r#"{prelude}
return botster.register({{
  tools = {{{{
    name = "{name}.version",
    description = "Report the loaded package version.",
    handler = "version",
    call = function() return {{ version = "{version}" }} end,
  }}}},
}})
"#
        ),
    )
    .expect("write versioned plugin");
    write_manifest(root, name, version);
}

/// The same package at `version`, with a plugin entrypoint that does not parse.
fn write_syntax_error_package(root: &Path, name: &str, version: &str) {
    fs::create_dir_all(root).expect("create syntax-error package root");
    fs::write(root.join("plugin.lua"), "return botster.register({\n  tools = {\n")
        .expect("write unparsable plugin");
    write_manifest(root, name, version);
}

fn write_manifest(root: &Path, name: &str, version: &str) {
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": name,
            "version": version,
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        }))
        .expect("serialize package manifest"),
    )
    .expect("write package manifest");
}

fn socket_endpoint(data_dir: &Path) -> botster_hub_client::DaemonEndpoint {
    botster_hub_client::DaemonEndpoint::new(
        explicit_config(data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    )
}

/// Assert a typed plugin load refusal whose Lua error names the entrypoint
/// relative to the package and carries no host path.
fn assert_load_refusal(
    refused: &botster_hub_client::DaemonResponse,
    operation: &str,
    package_dir: &Path,
) {
    assert_eq!(
        refused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "{refused:?}"
    );
    let error = refused.error.as_ref().expect("typed refusal");
    assert_eq!(error.code, "lua_load_failed", "{refused:?}");
    assert_eq!(error.operation, operation, "{refused:?}");
    assert!(error.message.contains("plugin.lua:"), "{}", error.message);
    let package_root = package_dir.canonicalize().expect("canonical package root");
    assert!(
        !error.message.contains(&*package_root.to_string_lossy()),
        "{}",
        error.message
    );
}

fn listed_package(
    connection: &mut UnixRouteClient,
    name: &str,
) -> Option<botster_hub_client::DaemonPackage> {
    connection
        .request(&botster_hub_client::DaemonRequest::ListPackages)
        .expect("list packages on the same connection")
        .packages
        .into_iter()
        .find(|package| package.package_name == name)
}

fn assert_connection_serves(connection: &mut UnixRouteClient) {
    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("the connection keeps serving after the refusal");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
}

/// Enable a new package whose plugin does not parse: the enable is refused
/// with a typed error on the same connection, no package record remains, and
/// the connection keeps serving requests.
#[test]
fn plugin_load_failure_refuses_enable_and_keeps_the_connection() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-load-refusal");
    let package_dir = unique_test_dir("daemon-syntax-error-package");
    write_syntax_error_package(&package_dir, "syntax.error", "1.0.0");
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        UnixRouteClient::connect(&socket_endpoint(&data_dir)).expect("external connect");

    let refused = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        })
        .expect("the refusal arrives on the same connection");
    assert_load_refusal(&refused, "enable", &package_dir);
    assert!(
        listed_package(&mut connection, "syntax.error").is_none(),
        "a refused new package leaves no record"
    );
    assert_connection_serves(&mut connection);

    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}

/// Re-enable a disabled package whose plugin no longer parses: the enable is
/// refused and the prior record is restored exactly, still disabled.
#[test]
fn plugin_load_failure_refuses_a_re_enable_and_restores_the_disabled_record() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-reenable-refusal");
    let package_dir = unique_test_dir("daemon-reenable-refusal-package");
    write_versioned_package(&package_dir, "reenable.error", "1.0.0");
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        UnixRouteClient::connect(&socket_endpoint(&data_dir)).expect("external connect");
    let enabled = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        })
        .expect("enable the loadable version");
    assert_eq!(
        enabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision,
        "{enabled:?}"
    );
    let disabled = connection
        .request(&botster_hub_client::DaemonRequest::DisablePackage {
            package_name: "reenable.error".to_string(),
        })
        .expect("disable the package");
    assert_eq!(
        disabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision,
        "{disabled:?}"
    );

    write_syntax_error_package(&package_dir, "reenable.error", "1.0.0");
    let refused = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackage {
            package_name: "reenable.error".to_string(),
        })
        .expect("the refusal arrives on the same connection");
    assert_load_refusal(&refused, "enable", &package_dir);

    let restored =
        listed_package(&mut connection, "reenable.error").expect("the prior record is restored");
    assert_eq!(restored.version, "1.0.0");
    assert_eq!(restored.state, "disabled");
    assert_connection_serves(&mut connection);

    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}

/// Update a package by reloading it at a new version whose plugin does not
/// parse: the reload is refused, the prior record is restored, the prior
/// plugin version keeps serving, and the connection keeps serving.
#[test]
fn plugin_load_failure_refuses_reload_and_keeps_the_connection() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-reload-refusal");
    let package_dir = unique_test_dir("daemon-reload-refusal-package");
    write_versioned_package(&package_dir, "reload.error", "1.0.0");
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        UnixRouteClient::connect(&socket_endpoint(&data_dir)).expect("external connect");
    let enabled = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        })
        .expect("enable the loadable version");
    assert_eq!(
        enabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision,
        "{enabled:?}"
    );

    write_syntax_error_package(&package_dir, "reload.error", "2.0.0");
    let refused = connection
        .request(&botster_hub_client::DaemonRequest::ReloadPackage {
            package_name: "reload.error".to_string(),
        })
        .expect("the refusal arrives on the same connection");
    assert_load_refusal(&refused, "reload", &package_dir);
    let restored =
        listed_package(&mut connection, "reload.error").expect("the prior record is restored");
    assert_eq!(restored.version, "1.0.0");
    assert_eq!(restored.state, "enabled");
    assert_eq!(
        call_plugin_tool(&data_dir, "reload.error.version", serde_json::json!({}))["version"],
        "1.0.0",
        "the prior plugin version keeps serving"
    );
    assert_connection_serves(&mut connection);

    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}

/// A plugin version subscribing to an event nobody declares.
const UNDECLARED_SUBSCRIPTION: &str =
    "events.on('hub', 'botster_undeclared_event', function() return {} end)";

/// Assert an event plane refusal: typed, never compensation, never success.
fn assert_event_plane_refusal(refused: &botster_hub_client::DaemonResponse, operation: &str) {
    assert_eq!(
        refused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "{refused:?}"
    );
    let error = refused.error.as_ref().expect("typed refusal");
    assert_eq!(error.code, "rejected_undeclared", "{refused:?}");
    assert_eq!(error.operation, operation, "{refused:?}");
}

/// Re-enable a disabled package whose plugin the event plane now rejects:
/// the enable is refused and the prior record is restored, still disabled.
#[test]
fn event_plane_rejection_refuses_a_re_enable_and_restores_the_disabled_record() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-reenable-event-refusal");
    let package_dir = unique_test_dir("daemon-reenable-event-refusal-package");
    write_versioned_package(&package_dir, "reenable.event", "1.0.0");
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        UnixRouteClient::connect(&socket_endpoint(&data_dir)).expect("external connect");
    let enabled = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        })
        .expect("enable the loadable version");
    assert_eq!(
        enabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision,
        "{enabled:?}"
    );
    let disabled = connection
        .request(&botster_hub_client::DaemonRequest::DisablePackage {
            package_name: "reenable.event".to_string(),
        })
        .expect("disable the package");
    assert_eq!(
        disabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision,
        "{disabled:?}"
    );

    write_versioned_package_with(
        &package_dir,
        "reenable.event",
        "1.0.0",
        UNDECLARED_SUBSCRIPTION,
    );
    let refused = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackage {
            package_name: "reenable.event".to_string(),
        })
        .expect("the refusal arrives on the same connection");
    assert_event_plane_refusal(&refused, "enable");

    let restored =
        listed_package(&mut connection, "reenable.event").expect("the prior record is restored");
    assert_eq!(restored.version, "1.0.0");
    assert_eq!(restored.state, "disabled");
    assert_connection_serves(&mut connection);

    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}

/// Update a package by reloading it at a new version the event plane
/// rejects: the reload is refused, the prior record is restored, and the
/// prior plugin version keeps serving.
#[test]
fn event_plane_rejection_refuses_reload_and_keeps_the_old_version_serving() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-reload-event-refusal");
    let package_dir = unique_test_dir("daemon-reload-event-refusal-package");
    write_versioned_package(&package_dir, "reload.event", "1.0.0");
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        UnixRouteClient::connect(&socket_endpoint(&data_dir)).expect("external connect");
    let enabled = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        })
        .expect("enable the loadable version");
    assert_eq!(
        enabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision,
        "{enabled:?}"
    );

    write_versioned_package_with(&package_dir, "reload.event", "2.0.0", UNDECLARED_SUBSCRIPTION);
    let refused = connection
        .request(&botster_hub_client::DaemonRequest::ReloadPackage {
            package_name: "reload.event".to_string(),
        })
        .expect("the refusal arrives on the same connection");
    assert_event_plane_refusal(&refused, "reload");
    let restored =
        listed_package(&mut connection, "reload.event").expect("the prior record is restored");
    assert_eq!(restored.version, "1.0.0");
    assert_eq!(restored.state, "enabled");
    assert_eq!(
        call_plugin_tool(&data_dir, "reload.event.version", serde_json::json!({}))["version"],
        "1.0.0",
        "the prior plugin version keeps serving"
    );
    assert_connection_serves(&mut connection);

    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}
