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

/// A daemon-loaded plugin writes structured log records that an operator
/// reads back with `ReadPluginLogs`, including fields and paging by sequence.
#[test]
fn live_daemon_reads_structured_plugin_logs() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("logs-data");
    let package_dir = unique_short_test_dir("logs-package");
    fs::create_dir_all(&package_dir).expect("create logs package root");
    fs::write(
        package_dir.join("plugin.lua"),
        r#"
return botster.register({
  tools = {{
    name = "logs.probe.write",
    description = "Write two log records.",
    handler = "write",
    call = function()
      local first = botster.log.info({ message = "ticket advanced", fields = { ticket_id = "t1" } })
      local second = botster.log.warn({ message = "gate slow" })
      local bad = botster.log.error({})
      return { first = first.value, second = second.value, bad_kind = bad.error.kind }
    end,
  }},
})
"#,
    )
    .expect("write logs plugin");
    fs::write(
        package_dir.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "logs.probe",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        }))
        .expect("serialize logs manifest"),
    )
    .expect("write logs manifest");

    let daemon = PanicSafeCliDaemon::start(&data_dir, "plugin logs daemon cleanup");
    let enabled = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath { path: package_dir.clone() },
    )
    .expect("enable logs package");
    assert_eq!(enabled.kind, botster_hub::DaemonResponseKind::PackageDecision, "{enabled:?}");
    let wrote = call_plugin_tool(&data_dir, "logs.probe.write", serde_json::json!({}));
    assert_eq!(
        (wrote["first"].as_u64(), wrote["second"].as_u64()),
        (Some(1), Some(2)),
        "{wrote}"
    );
    assert_eq!(wrote["bad_kind"], "invalid_request", "{wrote}");

    let read = |after_seq: u64| {
        botster_hub::daemon_transport_request(
            &explicit_config(&data_dir),
            botster_hub::DaemonRequest::ReadPluginLogs {
                package_name: "logs.probe".to_string(),
                after_seq,
            },
        )
        .expect("read plugin logs")
    };
    let all = read(0);
    assert_eq!(all.kind, botster_hub::DaemonResponseKind::PluginLogs, "{all:?}");
    let logs = all.plugin_logs.expect("plugin logs page");
    assert_eq!(logs.records.len(), 2, "{logs:?}");
    assert_eq!(logs.records[0].level, "info");
    assert_eq!(logs.records[0].message, "ticket advanced");
    assert_eq!(
        logs.records[0].fields_json.as_deref(),
        Some(r#"{"ticket_id":"t1"}"#)
    );
    assert_eq!(logs.records[1].level, "warn");
    assert_eq!((logs.next_seq, logs.first_available_seq), (3, 1));
    let later = read(1).plugin_logs.expect("paged plugin logs");
    assert_eq!(later.records.len(), 1);
    assert_eq!(later.records[0].seq, 2);
    daemon.shutdown();
}

fn read_plugin_logs(data_dir: &Path, package_name: &str) -> botster_hub_client::DaemonPluginLogs {
    let response = botster_hub::daemon_transport_request(
        &explicit_config(data_dir),
        botster_hub::DaemonRequest::ReadPluginLogs {
            package_name: package_name.to_string(),
            after_seq: 0,
        },
    )
    .expect("read plugin logs");
    assert_eq!(
        response.kind,
        botster_hub::DaemonResponseKind::PluginLogs,
        "{response:?}"
    );
    response.plugin_logs.expect("plugin logs page")
}

/// A plugin that floods its log in one call through a live daemon keeps every
/// accepted record, and any refusal is a typed, retryable `backpressured`
/// that counts its drop. The real clock refills one record per 10 ms, so
/// whether a refusal happens depends on speed; every assertion here holds
/// either way. Refusal itself is proven with a frozen rate clock in
/// `lua_runtime::basics` (`log_refuses_past_the_burst_...`).
#[test]
fn live_daemon_log_flood_keeps_every_accepted_record_and_types_any_refusal() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("logs-flood-data");
    let package_dir = unique_short_test_dir("logs-flood-package");
    fs::create_dir_all(&package_dir).expect("create flood package root");
    fs::write(
        package_dir.join("plugin.lua"),
        r#"
return botster.register({
  tools = {{
    name = "logs.flood.write",
    description = "Write 250 log records in one call.",
    handler = "write",
    call = function()
      local accepted, refused, first = 0, 0, nil
      for index = 1, 250 do
        local result = botster.log.info({ message = "record " .. index })
        if result.ok then
          accepted = accepted + 1
        else
          refused = refused + 1
          first = first or result.error
        end
      end
      local report = { accepted = accepted, refused = refused }
      if first then
        report.kind = first.kind
        report.retryable = first.retryable
        report.dropped = first.detail.dropped
      end
      return report
    end,
  }},
})
"#,
    )
    .expect("write flood plugin");
    write_manifest(&package_dir, "logs.flood", "1.0.0");
    let daemon = PanicSafeCliDaemon::start(&data_dir, "plugin log flood daemon cleanup");
    let enabled = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("enable flood package");
    assert_eq!(
        enabled.kind,
        botster_hub::DaemonResponseKind::PackageDecision,
        "{enabled:?}"
    );
    let flooded = call_plugin_tool(&data_dir, "logs.flood.write", serde_json::json!({}));
    let accepted = flooded["accepted"].as_u64().expect("accepted count");
    let refused = flooded["refused"].as_u64().expect("refused count");
    assert_eq!(accepted + refused, 250, "{flooded}");
    assert!(accepted >= 200, "the full bucket accepts the burst: {flooded}");
    if refused > 0 {
        assert_eq!(flooded["kind"], "backpressured", "{flooded}");
        assert_eq!(flooded["retryable"], true, "{flooded}");
        assert_eq!(
            flooded["dropped"], 1,
            "the first refusal after an accepted record counts one drop: {flooded}"
        );
    }
    let logs = read_plugin_logs(&data_dir, "logs.flood");
    let kept = usize::try_from(accepted).unwrap().min(256);
    assert_eq!(logs.records.len(), kept, "only accepted records are kept");
    assert_eq!(logs.records.last().map(|record| record.seq), Some(accepted));
    daemon.shutdown();
}

/// A first load whose entrypoint logs and then fails leaves no records.
#[test]
fn a_failed_first_load_removes_the_records_its_entrypoint_wrote() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("logs-failed-load");
    let package_dir = unique_test_dir("logs-failed-load-package");
    write_versioned_package_with(
        &package_dir,
        "logs.failed",
        "1.0.0",
        "botster.log.info({ message = 'loading' })\nerror('this version refuses to load')",
    );
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        UnixRouteClient::connect(&socket_endpoint(&data_dir)).expect("external connect");
    let refused = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        })
        .expect("the refusal arrives on the connection");
    assert_eq!(
        refused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "{refused:?}"
    );
    let logs = read_plugin_logs(&data_dir, "logs.failed");
    assert!(logs.records.is_empty(), "{logs:?}");
    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}

/// A reload whose entrypoint logs and then fails keeps its records beside the
/// serving version's: they explain the failure. Every load's records carry
/// their own generation, in one chronological log.
#[test]
fn a_failed_reload_keeps_the_records_its_entrypoint_wrote() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("logs-failed-reload");
    let package_dir = unique_test_dir("logs-failed-reload-package");
    write_versioned_package_with(
        &package_dir,
        "logs.reload",
        "1.0.0",
        "botster.log.info({ message = 'v1 loaded' })",
    );
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        UnixRouteClient::connect(&socket_endpoint(&data_dir)).expect("external connect");
    let enabled = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        })
        .expect("enable v1");
    assert_eq!(
        enabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision,
        "{enabled:?}"
    );
    let reload = |connection: &mut UnixRouteClient| {
        connection
            .request(&botster_hub_client::DaemonRequest::ReloadPackage {
                package_name: "logs.reload".to_string(),
            })
            .expect("reload answer")
    };
    write_versioned_package_with(
        &package_dir,
        "logs.reload",
        "2.0.0",
        "botster.log.info({ message = 'v2 loading' })\nerror('v2 refuses to load')",
    );
    let refused = reload(&mut connection);
    assert_eq!(
        refused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "{refused:?}"
    );
    let after_failure = read_plugin_logs(&data_dir, "logs.reload");
    let messages: Vec<&str> = after_failure
        .records
        .iter()
        .map(|record| record.message.as_str())
        .collect();
    assert_eq!(messages, ["v1 loaded", "v2 loading"], "{after_failure:?}");
    assert_ne!(
        after_failure.records[0].generation, after_failure.records[1].generation,
        "the failed candidate's records carry its own generation"
    );

    write_versioned_package_with(
        &package_dir,
        "logs.reload",
        "3.0.0",
        "botster.log.info({ message = 'v3 loaded' })",
    );
    let reloaded = reload(&mut connection);
    assert_ne!(
        reloaded.kind,
        botster_hub_client::DaemonResponseKind::OperatorError,
        "{reloaded:?}"
    );
    let after_success = read_plugin_logs(&data_dir, "logs.reload");
    let records: Vec<(&str, u64)> = after_success
        .records
        .iter()
        .map(|record| (record.message.as_str(), record.generation))
        .collect();
    assert_eq!(records.len(), 3, "{after_success:?}");
    assert_eq!(
        [records[0].0, records[1].0, records[2].0],
        ["v1 loaded", "v2 loading", "v3 loaded"]
    );
    assert!(
        records[0].1 != records[1].1 && records[1].1 != records[2].1 && records[0].1 != records[2].1,
        "each load has its own generation"
    );
    drop(connection);
    shutdown_cli_daemon(&data_dir, child);
}
