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
    call = function(args, request)
      local sessions = botster.capabilities.sessions
      return {{
        caller = request.caller,
        list = sessions.list({{ owner = "any" }}),
        get = sessions.get({{ session = {{ session_id = args.session_id }} }}),
        get_string = sessions.get({{ session = args.session_id }}),
        missing = sessions.get({{ session = {{ session_id = "no-such-session" }} }}),
        remote_list = sessions.list({{ hub_id = "another-hub" }}),
        remote_get = sessions.get({{ session = {{ hub_id = "another-hub", session_id = args.session_id }} }}),
        bad_owner = sessions.list({{ owner = "nobody" }}),
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

/// A plugin with `session_read:any` lists and gets sessions from the Hub's
/// projection, names them by { hub_id, session_id }, and gets typed refusals
/// for a missing session, another Hub and a bad owner. A plugin without the
/// grant is denied.
#[test]
fn live_daemon_plugin_reads_sessions_by_hub_and_session_id() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("sessions-read");
    let reader = unique_short_test_dir("sessions-reader");
    let denied = unique_short_test_dir("sessions-denied");
    write_sessions_probe_package(&reader, "sessions.reader", "sessions.reader.probe", &["session_read:any"]);
    write_sessions_probe_package(&denied, "sessions.denied", "sessions.denied.probe", &[]);

    let daemon = PanicSafeCliDaemon::start(&data_dir, "sessions read daemon cleanup");
    let config = explicit_config(&data_dir);
    for package in [&reader, &denied] {
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
    let session_id = "view-plain-session";
    let spawned = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "sleep 60".to_string(),
        },
    )
    .expect("spawn a plain session");
    assert_eq!(spawned.kind, botster_hub::DaemonResponseKind::Spawned, "{spawned:?}");
    // The view is written before the entity frame is delivered.
    wait_for_entity_frame(&mut sessions, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, |frame| {
        matches!(
            frame,
            botster_hub_client::DaemonEntityFrame::Upsert { id, .. } if id == session_id
        )
    });

    let read = call_plugin_tool(
        &data_dir,
        "sessions.reader.probe",
        serde_json::json!({ "session_id": session_id }),
    );
    // A tool called over the Hub socket runs for the local operator.
    assert_eq!(read["caller"], serde_json::json!({ "kind": "operator" }), "{read}");
    assert_eq!(read["list"]["ok"], true, "{read}");
    let rows = read["list"]["value"]["sessions"].as_array().expect("session rows");
    let row = rows
        .iter()
        .find(|row| row["session_id"] == session_id)
        .unwrap_or_else(|| panic!("the spawned session is listed: {read}"));
    assert!(row["hub_id"].as_str().is_some_and(|hub_id| !hub_id.is_empty()), "{row}");
    assert_eq!(row["lifecycle_class"], "current", "{row}");
    assert_eq!(read["get"]["ok"], true, "{read}");
    assert_eq!(read["get"]["value"]["session_id"], session_id, "{read}");
    assert_eq!(read["get"]["value"]["hub_id"], row["hub_id"], "{read}");
    assert_eq!(read["get_string"]["value"]["session_id"], session_id, "{read}");
    assert_eq!(read["missing"]["error"]["kind"], "not_found", "{read}");
    assert_eq!(read["remote_list"]["error"]["kind"], "remote_hub_unsupported", "{read}");
    assert_eq!(read["remote_get"]["error"]["kind"], "remote_hub_unsupported", "{read}");
    assert_eq!(read["bad_owner"]["error"]["kind"], "invalid_request", "{read}");

    let refused = call_plugin_tool(
        &data_dir,
        "sessions.denied.probe",
        serde_json::json!({ "session_id": session_id }),
    );
    assert_eq!(refused["list"]["error"]["kind"], "capability_denied", "{refused}");
    assert_eq!(refused["get"]["error"]["kind"], "capability_denied", "{refused}");
    drop(sessions);
    daemon.shutdown();
}
