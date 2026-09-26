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

/// A daemon-loaded plugin splits its code across files in `lua/` and loads
/// them with `require`; a symlinked module fails the package load.
#[test]
fn live_daemon_plugin_requires_its_own_modules() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("modules-data");
    let package_dir = unique_short_test_dir("modules-package");
    let linked_dir = unique_short_test_dir("modules-linked");
    for (root, name) in [(&package_dir, "modules.probe"), (&linked_dir, "modules.linked")] {
        fs::create_dir_all(root.join("lua/lib")).expect("create module tree");
        fs::write(
            root.join("lua/lib/greet.lua"),
            "return { hello = function(who) return 'hello ' .. who end }",
        )
        .expect("write module");
        fs::write(
            root.join("plugin.lua"),
            format!(
                r#"local greet = require("lib.greet")
return botster.register({{
  tools = {{{{
    name = "{name}.greet",
    description = "Greet through a required module.",
    handler = "greet",
    call = function(request) return {{ text = greet.hello(request.who) }} end,
  }}}},
}})
"#
            ),
        )
        .expect("write entrypoint");
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
            .expect("serialize module manifest"),
        )
        .expect("write module manifest");
    }
    std::os::unix::fs::symlink(
        linked_dir.join("lua/lib/greet.lua"),
        linked_dir.join("lua/lib/escape.lua"),
    )
    .expect("create symlinked module");

    let daemon = PanicSafeCliDaemon::start(&data_dir, "plugin modules daemon cleanup");
    let enabled = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath { path: package_dir.clone() },
    )
    .expect("enable module package");
    assert_eq!(enabled.kind, botster_hub::DaemonResponseKind::PackageDecision, "{enabled:?}");
    let greeted = call_plugin_tool(&data_dir, "modules.probe.greet", serde_json::json!({ "who": "hub" }));
    assert_eq!(greeted["text"], "hello hub", "{greeted}");

    // A symlinked module fails the package load. The daemon's enable path
    // currently drops the client connection on any Lua load failure (a
    // pre-existing behavior, reported separately), so either outcome is a
    // refusal here; what matters is that nothing from the package loads and
    // the daemon keeps serving.
    let refused = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath { path: linked_dir.clone() },
    );
    if let Ok(response) = &refused {
        assert_ne!(
            response.kind,
            botster_hub::DaemonResponseKind::PackageDecision,
            "a symlinked module must fail the load: {response:?}"
        );
    }
    let unserved = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginMcpCallTool {
            name: "modules.linked.greet".to_string(),
            arguments: serde_json::json!({ "who": "hub" }),
        },
    )
    .expect("call the refused package's tool");
    assert_ne!(
        unserved.kind,
        botster_hub::DaemonResponseKind::PluginMcpToolResult,
        "the refused package must not serve tools: {unserved:?}"
    );
    let again = call_plugin_tool(&data_dir, "modules.probe.greet", serde_json::json!({ "who": "again" }));
    assert_eq!(again["text"], "hello again", "the daemon keeps serving: {again}");
    daemon.shutdown();
}
