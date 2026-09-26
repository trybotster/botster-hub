const PLUGIN_DB_PROBE: &str = r#"
return botster.register({
  tools = {{
    name = "__NAME__.store_probe",
    description = "Write and read one plugin_db record in the package's own namespace.",
    handler = "store_probe",
    call = function()
      local ok, result = pcall(function()
        botster.capabilities.plugin_db.set({ key = "probe", payload = { written = true } })
        return botster.capabilities.plugin_db.get({ key = "probe" })
      end)
      if ok then
        return { ok = true, written = result.record.payload.written }
      end
      return { ok = false, message = tostring(result) }
    end,
  }},
})
"#;

fn write_plugin_db_probe_package(root: &Path, name: &str, capabilities: serde_json::Value) {
    fs::create_dir_all(root).expect("create grant probe package root");
    fs::write(root.join("plugin.lua"), PLUGIN_DB_PROBE.replace("__NAME__", name))
        .expect("write grant probe plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": name,
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": capabilities,
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        }))
        .expect("serialize grant probe manifest"),
    )
    .expect("write grant probe manifest");
}

/// Runtime grants come from each package's own admitted capabilities, not
/// from a Hub-wide list of package names. The old list granted plugin_db to
/// any package named `project-pipelines`, and refused the capability to every
/// other package at enable time; both directions are checked here.
#[test]
fn live_daemon_grants_plugin_db_from_each_packages_own_admission() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("grants-data");
    let undeclared_dir = unique_short_test_dir("grants-undeclared");
    let declared_dir = unique_short_test_dir("grants-declared");
    write_plugin_db_probe_package(
        &undeclared_dir,
        "project-pipelines",
        serde_json::json!([{ "surface": "mcp" }]),
    );
    write_plugin_db_probe_package(
        &declared_dir,
        "grants.own",
        serde_json::json!([
            { "surface": "mcp" },
            { "surface": "plugin_db", "scope": "grants.own" }
        ]),
    );

    let daemon = PanicSafeCliDaemon::start(&data_dir, "plugin grants daemon cleanup");
    for package_dir in [&undeclared_dir, &declared_dir] {
        let enabled = botster_hub::daemon_transport_request(
            &explicit_config(&data_dir),
            botster_hub::DaemonRequest::EnablePackageLocalPath { path: package_dir.clone() },
        )
        .expect("enable grant probe package");
        assert_eq!(
            enabled.kind,
            botster_hub::DaemonResponseKind::PackageDecision,
            "{} must enable: {enabled:?}",
            package_dir.display()
        );
    }

    let undeclared = call_plugin_tool(&data_dir, "project-pipelines.store_probe", serde_json::json!({}));
    assert_eq!(undeclared["ok"], false, "{undeclared}");
    assert!(
        undeclared["message"]
            .as_str()
            .unwrap_or_default()
            .contains("was not admitted with the required capability"),
        "an undeclared plugin_db must be denied at the runtime check: {undeclared}"
    );

    let declared = call_plugin_tool(&data_dir, "grants.own.store_probe", serde_json::json!({}));
    assert_eq!(declared["ok"], true, "{declared}");
    assert_eq!(declared["written"], true, "{declared}");
    daemon.shutdown();
}
