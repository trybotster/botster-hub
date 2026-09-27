/// A package whose plugin entrypoint does not parse.
fn write_syntax_error_package(root: &Path, name: &str) {
    fs::create_dir_all(root).expect("create syntax-error package root");
    fs::write(root.join("plugin.lua"), "return botster.register({\n  handlers = {\n")
        .expect("write unparsable plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": name,
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        }))
        .expect("serialize syntax-error manifest"),
    )
    .expect("write syntax-error manifest");
}

/// A plugin that fails to load refuses its enable with a typed error on the
/// same connection, and that connection keeps serving requests.
#[test]
fn plugin_load_failure_refuses_enable_and_keeps_the_connection() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-load-refusal");
    let package_dir = unique_test_dir("daemon-syntax-error-package");
    write_syntax_error_package(&package_dir, "syntax.error");
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
    let mut connection = UnixRouteClient::connect(&endpoint).expect("external connect");

    let refused = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        })
        .expect("the refusal arrives on the same connection");
    assert_eq!(
        refused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "{refused:?}"
    );
    let error = refused.error.as_ref().expect("typed refusal");
    assert_eq!(error.code, "lua_load_failed");
    assert_eq!(error.operation, "enable");
    // The Lua source is named relative to the package, so this syntax error
    // names plugin.lua and carries no host path.
    assert!(error.message.contains("plugin.lua:"), "{}", error.message);
    let package_root = package_dir.canonicalize().expect("canonical package root");
    assert!(
        !error.message.contains(&*package_root.to_string_lossy()),
        "{}",
        error.message
    );

    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("the connection keeps serving after the refusal");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}

/// A reload whose new plugin fails to load refuses with a typed error after
/// the previous version is restored, and the connection keeps serving.
#[test]
fn plugin_load_failure_refuses_reload_and_keeps_the_connection() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-reload-refusal");
    let package_dir = unique_test_dir("daemon-reload-refusal-package");
    write_syntax_error_package(&package_dir, "reload.error");
    fs::write(package_dir.join("plugin.lua"), "return botster.register({})\n")
        .expect("write a loadable plugin first");
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
    let mut connection = UnixRouteClient::connect(&endpoint).expect("external connect");
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

    fs::write(package_dir.join("plugin.lua"), "return botster.register({\n")
        .expect("break the plugin");
    let refused = connection
        .request(&botster_hub_client::DaemonRequest::ReloadPackage {
            package_name: "reload.error".to_string(),
        })
        .expect("the refusal arrives on the same connection");
    assert_eq!(
        refused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "{refused:?}"
    );
    let error = refused.error.as_ref().expect("typed refusal");
    assert_eq!(error.code, "lua_load_failed");
    assert!(error.message.contains("plugin.lua:"), "{}", error.message);

    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("the connection keeps serving after the refusal");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}
