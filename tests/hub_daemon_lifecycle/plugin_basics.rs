const BASICS_PROBE_PLUGIN: &str = r#"
return botster.register({
  tools = {{
    name = "basics.probe",
    description = "Exercise botster.json and botster.clock.",
    handler = "probe",
    call = function(request)
      local decoded = botster.json.decode({ text = request.text })
      local encoded = botster.json.encode({ value = { echo = decoded.value, empty = {} }, arrays = "empty" })
      local broken = botster.json.decode({ text = "{" })
      local now = botster.clock.now()
      local monotonic = botster.clock.monotonic()
      return {
        decoded_ok = decoded.ok,
        encoded = encoded.value,
        broken_kind = broken.error and broken.error.kind,
        now = now.value,
        monotonic_ok = monotonic.ok,
      }
    end,
  }},
})
"#;

/// A daemon-loaded plugin can round-trip JSON, gets typed errors instead of
/// raised ones, and reads the clock, all without any grant beyond `mcp`.
#[test]
fn live_daemon_plugin_uses_json_and_clock_basics() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("basics-data");
    let package_dir = unique_short_test_dir("basics-package");
    fs::create_dir_all(&package_dir).expect("create basics package root");
    fs::write(package_dir.join("plugin.lua"), BASICS_PROBE_PLUGIN).expect("write basics plugin");
    fs::write(
        package_dir.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "basics.probe",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        }))
        .expect("serialize basics manifest"),
    )
    .expect("write basics manifest");

    let daemon = PanicSafeCliDaemon::start(&data_dir, "plugin basics daemon cleanup");
    let enabled = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath { path: package_dir.clone() },
    )
    .expect("enable basics package");
    assert_eq!(enabled.kind, botster_hub::DaemonResponseKind::PackageDecision, "{enabled:?}");
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time after epoch")
        .as_millis();
    let probe = call_plugin_tool(
        &data_dir,
        "basics.probe",
        serde_json::json!({ "text": "{\"run\":\"r1\",\"steps\":[1,2]}" }),
    );
    assert_eq!(probe["decoded_ok"], true, "{probe}");
    let encoded: serde_json::Value =
        serde_json::from_str(probe["encoded"].as_str().expect("encoded text")).expect("valid JSON");
    assert_eq!(
        encoded,
        serde_json::json!({ "echo": { "run": "r1", "steps": [1, 2] }, "empty": [] }),
        "{probe}"
    );
    assert_eq!(probe["broken_kind"], "invalid_request", "{probe}");
    assert_eq!(probe["monotonic_ok"], true, "{probe}");
    let now = probe["now"].as_u64().expect("clock.now is an integer") as u128;
    assert!(now >= before && now < before + 60_000, "{now} vs {before}");
    daemon.shutdown();
}
