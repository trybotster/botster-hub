fn write_sessions_probe_package(root: &Path, name: &str, tool: &str, scopes: &[&str]) {
    fs::create_dir_all(root).expect("create sessions probe package root");
    fs::write(
        root.join("plugin.lua"),
        format!(
            r#"
return botster.register({{
  tools = {{{{
    name = "{tool}",
    description = "Read sessions through botster.capabilities.sessions.",
    handler = "probe",
    call = function(args)
      local sessions = botster.capabilities.sessions
      return {{
        list = sessions.list({{ owner = "any" }}),
        list_own = sessions.list({{}}),
        get = sessions.get({{ session = {{ session_id = args.session_id }} }}),
        get_string = sessions.get({{ session = args.session_id }}),
        missing = sessions.get({{ session = {{ session_id = "no-such-session" }} }}),
        remote_list = sessions.list({{ hub_id = "another-hub" }}),
        remote_get = sessions.get({{ session = {{ hub_id = "another-hub", session_id = args.session_id }} }}),
        bad_owner = sessions.list({{ owner = "nobody" }}),
        second_page = args.after and sessions.list({{ owner = "any", after = args.after }}) or nil,
      }}
    end,
  }}}},
}})
"#
        ),
    )
    .expect("write sessions probe plugin");
    let mut capabilities = vec![serde_json::json!({ "surface": "mcp" })];
    for scope in scopes {
        capabilities.push(serde_json::json!({ "surface": "session_actions", "scope": scope }));
    }
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
        .expect("serialize sessions probe manifest"),
    )
    .expect("write sessions probe manifest");
}

/// A plugin reads sessions from the Hub owner, names them by { hub_id,
/// session_id }, gets typed refusals for a missing session, another Hub and a
/// bad owner, and pages through more than one page of rows. A plugin with only
/// `session_read` sees no rows and no cursor, whatever the Hub holds, and a
/// plugin without a grant is denied.
#[test]
fn live_daemon_plugin_reads_sessions_by_hub_and_session_id() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("sessions-read");
    let reader = unique_short_test_dir("sessions-reader");
    let own_only = unique_short_test_dir("sessions-own");
    let denied = unique_short_test_dir("sessions-denied");
    write_sessions_probe_package(&reader, "sessions.reader", "sessions.reader.probe", &["session_read:any"]);
    write_sessions_probe_package(&own_only, "sessions.own", "sessions.own.probe", &["session_read"]);
    write_sessions_probe_package(&denied, "sessions.denied", "sessions.denied.probe", &[]);

    let daemon = PanicSafeCliDaemon::start(&data_dir, "sessions read daemon cleanup");
    let config = explicit_config(&data_dir);
    for package in [&reader, &own_only, &denied] {
        let enabled = botster_hub::daemon_transport_request(
            &config,
            botster_hub::DaemonRequest::EnablePackageLocalPath { path: package.clone() },
        )
        .expect("enable sessions probe package");
        assert_eq!(enabled.kind, botster_hub::DaemonResponseKind::PackageDecision, "{enabled:?}");
    }
    let mut sessions =
        botster_hub_client::subscribe_entities(&socket_endpoint(&data_dir), "session", "sessions-read")
            .expect("subscribe to sessions");
    // More sessions than one page holds. `cat` waits on its terminal input.
    let ids: Vec<String> = (0..10).map(|index| format!("view-session-{index:02}")).collect();
    for id in &ids {
        let spawned = botster_hub::daemon_transport_request(
            &config,
            botster_hub::DaemonRequest::Spawn {
                session_id: id.clone(),
                command: "cat".to_string(),
            },
        )
        .expect("spawn a plain session");
        assert_eq!(spawned.kind, botster_hub::DaemonResponseKind::Spawned, "{spawned:?}");
    }
    // The owner answers from its projection, which has every session once the
    // last one's entity frame is delivered.
    let last = ids.last().expect("ids").clone();
    wait_for_entity_frame(&mut sessions, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert { id, .. } if *id == last
        )
    });

    let read = call_plugin_tool(
        &data_dir,
        "sessions.reader.probe",
        serde_json::json!({ "session_id": ids[0], "after": ids[7] }),
    );
    assert_eq!(read["list"]["ok"], true, "{read}");
    let first_page = read["list"]["value"]["sessions"].as_array().expect("session rows");
    assert_eq!(first_page.len(), 8, "one page holds eight rows: {read}");
    assert_eq!(read["list"]["value"]["next_after"], ids[7], "{read}");
    let row = &first_page[0];
    assert_eq!(row["session_id"], ids[0], "{row}");
    assert!(row["hub_id"].as_str().is_some_and(|hub_id| !hub_id.is_empty()), "{row}");
    assert_eq!(row["lifecycle_class"], "current", "{row}");
    let second_page = read["second_page"]["value"]["sessions"].as_array().expect("second page");
    assert_eq!(second_page.len(), 2, "{read}");
    assert_eq!(read["second_page"]["value"]["next_after"], serde_json::Value::Null, "{read}");
    assert_eq!(read["get"]["ok"], true, "{read}");
    assert_eq!(read["get"]["value"]["session_id"], ids[0], "{read}");
    assert_eq!(read["get"]["value"]["hub_id"], row["hub_id"], "{read}");
    assert_eq!(read["get_string"]["value"]["session_id"], ids[0], "{read}");
    assert_eq!(read["missing"]["error"]["kind"], "not_found", "{read}");
    assert_eq!(read["remote_list"]["error"]["kind"], "remote_hub_unsupported", "{read}");
    assert_eq!(read["remote_get"]["error"]["kind"], "remote_hub_unsupported", "{read}");
    assert_eq!(read["bad_owner"]["error"]["kind"], "invalid_request", "{read}");

    // Own-session width: no rows and no cursor, though ten sessions exist.
    let own = call_plugin_tool(
        &data_dir,
        "sessions.own.probe",
        serde_json::json!({ "session_id": ids[0] }),
    );
    assert_eq!(own["list_own"]["ok"], true, "{own}");
    assert_eq!(own["list_own"]["value"]["sessions"].as_array().map(Vec::len), Some(0), "{own}");
    assert_eq!(own["list_own"]["value"]["next_after"], serde_json::Value::Null, "{own}");
    assert_eq!(own["list"]["error"]["kind"], "capability_denied", "{own}");
    assert_eq!(own["get"]["error"]["kind"], "not_found", "{own}");

    let refused = call_plugin_tool(
        &data_dir,
        "sessions.denied.probe",
        serde_json::json!({ "session_id": ids[0] }),
    );
    assert_eq!(refused["list"]["error"]["kind"], "capability_denied", "{refused}");
    assert_eq!(refused["list_own"]["error"]["kind"], "capability_denied", "{refused}");
    assert_eq!(refused["get"]["error"]["kind"], "capability_denied", "{refused}");
    drop(sessions);
    daemon.shutdown();
}
