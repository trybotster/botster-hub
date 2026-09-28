// Agent coordination over live sessions: identity injection and routed messages.

fn coordination_mcp_call(
    data_dir: &Path,
    caller_session_id: Option<&str>,
    tool: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("mcp-serve")
        .arg("--data-dir")
        .arg(data_dir)
        .env_remove(botster_hub::session_types::SESSION_ID_ENVIRONMENT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(caller_session_id) = caller_session_id {
        command.env(
            botster_hub::session_types::SESSION_ID_ENVIRONMENT,
            caller_session_id,
        );
    }
    let mut child = command.spawn().expect("spawn botster-hub mcp-serve");
    {
        let stdin = child.stdin.as_mut().expect("mcp stdin");
        for request in [
            serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": { "name": tool, "arguments": arguments }
            }),
        ] {
            writeln!(stdin, "{request}").expect("write MCP request");
        }
    }
    let output = child.wait_with_output().expect("wait for mcp-serve");
    assert!(
        output.status.success(),
        "mcp-serve failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("mcp stdout is utf8");
    let response = stdout
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("MCP line is JSON"))
        .find(|message| message["id"] == 2)
        .expect("tools/call response");
    response["result"].clone()
}

fn coordination_spawn(data_dir: &Path, session_id: &str, command: &str) {
    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-id")
        .arg(session_id)
        .arg("--")
        .arg(command)
        .output()
        .expect("run botster-hub sessions spawn");
    let stdout = String::from_utf8_lossy(&spawn.stdout);
    assert!(
        spawn.status.success() && stdout.contains("lifecycle=running"),
        "spawn {session_id} failed: {stdout} {}",
        String::from_utf8_lossy(&spawn.stderr)
    );
}

fn coordination_shutdown_session(data_dir: &Path, session_id: &str) {
    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .arg(session_id)
        .output()
        .expect("run botster-hub sessions shutdown");
    assert!(
        shutdown.status.success(),
        "shutdown {session_id} failed: {}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
}

fn make_fifo(path: &Path) {
    let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).expect("fifo path");
    // SAFETY: `name` is a valid NUL-terminated path for the duration of the call.
    let created = unsafe { libc::mkfifo(name.as_ptr(), 0o600) };
    assert_eq!(created, 0, "mkfifo {}", path.display());
}

/// Read everything one writer sends through `fifo`. The writer's close is the
/// event; the deadline only guards a session that never writes.
fn read_fifo_to_end(fifo: &Path) -> String {
    use std::os::unix::fs::OpenOptionsExt;
    let (sender, receiver) = mpsc::channel();
    let reader_path = fifo.to_path_buf();
    thread::spawn(move || {
        let text = fs::read_to_string(&reader_path).expect("read session fifo");
        let _ = sender.send(text);
    });
    // timer: deadline — the session writes and closes the fifo; expiry means it never did.
    match receiver.recv_timeout(Duration::from_secs(10)) {
        Ok(text) => text,
        Err(_) => {
            // Unblock the reader thread before failing: open and close a writer.
            drop(
                fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(fifo),
            );
            panic!("session never wrote {}", fifo.display());
        }
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[test]
fn spawned_sessions_carry_the_identity_that_mcp_serve_reports() {
    let _guard = daemon_test_guard();
    // Sessions reach the daemon through the absolute data directory, so it
    // must keep the socket path under the Unix limit.
    let data_dir = unique_short_test_dir("coord-id");
    let daemon = start_cli_daemon(&data_dir);
    let fifo_dir = unique_short_test_dir("coord-fifo");
    fs::create_dir_all(&fifo_dir).expect("create fifo dir");

    let requests = format!(
        "{}\n{}\n",
        serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} }),
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": { "name": "whoami", "arguments": {} }
        }),
    );
    let fifo = fifo_dir.join("raw-identity.fifo");
    make_fifo(&fifo);
    // The session runs mcp-serve from its own environment, as an agent would.
    coordination_spawn(
        &data_dir,
        "raw-identity",
        &format!(
            "printf '%s' {} | \"$BOTSTER_HUB_BIN\" mcp-serve --data-dir \"$BOTSTER_HUB_DATA_DIR\" > {}; exec cat",
            shell_quote(&requests),
            shell_quote(&fifo.display().to_string()),
        ),
    );
    let output = read_fifo_to_end(&fifo);
    let whoami = output
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("MCP line is JSON"))
        .find(|message| message["id"] == 2)
        .unwrap_or_else(|| panic!("whoami response in {output:?}"));
    let identity = &whoami["result"]["structuredContent"]["identity"];
    assert_eq!(identity["caller_session_id"], "raw-identity", "{whoami}");
    assert_eq!(
        identity["identity_source"],
        botster_hub::session_types::SESSION_ID_ENVIRONMENT
    );

    coordination_shutdown_session(&data_dir, "raw-identity");
    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn mcp_messages_round_trip_between_live_sessions() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("coordination-round-trip");
    let daemon = start_cli_daemon(&data_dir);
    for session_id in ["session-alpha", "session-beta", "session-slow"] {
        coordination_spawn(&data_dir, session_id, "exec cat");
    }

    let identity = coordination_mcp_call(&data_dir, Some("session-alpha"), "whoami", serde_json::json!({}));
    assert_eq!(
        identity["structuredContent"]["identity"]["caller_session_id"],
        "session-alpha"
    );
    let posted = coordination_mcp_call(
        &data_dir,
        Some("session-alpha"),
        "post_message",
        serde_json::json!({ "session_id": "session-beta", "envelope_id": "mcp-envelope-1", "body": "hello beta" }),
    );
    let delivery = &posted["structuredContent"]["publish"]["deliveries"][0];
    assert_eq!(delivery["envelope_id"], "mcp-envelope-1");
    assert_eq!(delivery["status"], "queued");
    for (envelope_id, body) in [("mcp-slow-1", "slow one"), ("mcp-slow-2", "slow two")] {
        coordination_mcp_call(
            &data_dir,
            Some("session-alpha"),
            "post_message",
            serde_json::json!({ "session_id": "session-slow", "envelope_id": envelope_id, "body": body }),
        );
    }

    let received = coordination_mcp_call(&data_dir, Some("session-beta"), "receive_messages", serde_json::json!({}));
    let message = &received["structuredContent"]["messages"][0];
    assert_eq!(message["envelope_id"], "mcp-envelope-1");
    assert_eq!(message["body"], "hello beta");
    assert_eq!(message["source"], "session:session-alpha");
    let next_cursor = received["structuredContent"]["next_cursor"]
        .as_u64()
        .expect("receive response includes next cursor");
    let acked = coordination_mcp_call(
        &data_dir,
        Some("session-beta"),
        "ack_message",
        serde_json::json!({ "envelope_id": "mcp-envelope-1" }),
    );
    assert_eq!(acked["structuredContent"]["ack"]["status"], "acknowledged");
    let after = coordination_mcp_call(
        &data_dir,
        Some("session-beta"),
        "receive_messages",
        serde_json::json!({ "after": next_cursor }),
    );
    assert_eq!(
        after["structuredContent"]["messages"].as_array().map(Vec::len),
        Some(0),
        "an after-cursor drain does not redeliver the observed envelope"
    );

    let slow = coordination_mcp_call(&data_dir, Some("session-slow"), "receive_messages", serde_json::json!({ "limit": 2 }));
    assert_eq!(
        slow["structuredContent"]["messages"].as_array().map(Vec::len),
        Some(2),
        "session-slow backlog stays independent from session-beta cursor and ack"
    );

    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn mcp_post_message_refuses_a_session_that_is_not_running() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("coordination-refusal");
    let daemon = start_cli_daemon(&data_dir);
    coordination_spawn(&data_dir, "session-ended", "exec cat");
    coordination_shutdown_session(&data_dir, "session-ended");

    for target in ["session-missing", "session-ended"] {
        let refused = coordination_mcp_call(
            &data_dir,
            Some("session-alpha"),
            "post_message",
            serde_json::json!({ "session_id": target, "body": "nobody reads this" }),
        );
        assert_eq!(refused["isError"], true, "{target}: {refused}");
        assert_eq!(
            refused["structuredContent"]["error"]["code"], "unknown_session",
            "{target}: {refused}"
        );
    }

    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn mcp_routed_envelopes_are_not_restart_durable_today() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("coordination-restart-loss");
    let daemon = start_cli_daemon(&data_dir);
    coordination_spawn(&data_dir, "session-restart", "exec cat");
    let posted = coordination_mcp_call(
        &data_dir,
        Some("session-alpha"),
        "post_message",
        serde_json::json!({ "session_id": "session-restart", "envelope_id": "mcp-restart-1", "body": "lost after restart" }),
    );
    assert_eq!(
        posted["structuredContent"]["publish"]["deliveries"][0]["status"],
        "queued"
    );
    shutdown_cli_daemon(&data_dir, daemon);

    let restarted = start_cli_daemon(&data_dir);
    let received = coordination_mcp_call(&data_dir, Some("session-restart"), "receive_messages", serde_json::json!({}));
    shutdown_cli_daemon(&data_dir, restarted);
    assert_eq!(
        received["structuredContent"]["messages"].as_array().map(Vec::len),
        Some(0),
        "routed-envelope queues are in memory and empty after a daemon restart"
    );
}

fn coordination_cli(data_dir: &Path, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .args(&args[..2.min(args.len())])
        .arg("--data-dir")
        .arg(data_dir)
        .args(&args[2.min(args.len())..])
        .output()
        .expect("run botster-hub");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        output.status.success(),
        "botster-hub {args:?} failed: {stdout} {}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
}

/// Parse `env | grep '^BOTSTER_' | sort` output into name/value pairs.
fn botster_environment(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

#[test]
fn spawned_sessions_do_not_inherit_the_launchers_botster_environment() {
    let _guard = daemon_test_guard();
    // Short absolute paths keep the injected socket path under SUN_LEN.
    let data_dir = unique_short_test_dir("coord-leak");
    let root = unique_short_test_dir("coord-leak-root");
    fs::create_dir_all(&root).expect("create spawn root");
    // The daemon's own environment carries another session's identity, as it
    // does when an agent shell starts the Hub.
    let daemon = start_cli_daemon_with_env(
        &data_dir,
        &[
            ("BOTSTER_SESSION_ID", "leaked-session-id"),
            ("BOTSTER_SESSION_UUID", "leaked-session-uuid"),
            ("BOTSTER_CONTEXT_ID", "leaked-context"),
            ("BOTSTER_MCP_TOKEN", "leaked-token"),
            // A credential under a name the test does not check one by one.
            ("BOTSTER_SYNTHETIC_SECRET", "leaked-synthetic-secret-3f9c1a"),
        ],
    );
    let print_environment = "env | grep '^BOTSTER_' | sort";

    // A raw spawn.
    let raw_fifo = root.join("raw.fifo");
    make_fifo(&raw_fifo);
    coordination_spawn(
        &data_dir,
        "leak-raw",
        &format!(
            "{print_environment} > {}; exec cat",
            shell_quote(&raw_fifo.display().to_string())
        ),
    );
    let raw = botster_environment(&read_fifo_to_end(&raw_fifo));

    // A session-type spawn.
    let typed_fifo = root.join("typed.fifo");
    make_fifo(&typed_fifo);
    coordination_cli(
        &data_dir,
        &[
            "spawn-targets",
            "create",
            "--root",
            &root.display().to_string(),
            "--id",
            "leak-root",
        ],
    );
    let definition = serde_json::json!({
        "id": "leak-probe",
        "label": "Leak probe",
        "role": "botster.agent",
        "interaction": "interactive",
        "lifecycle": "task",
        "execution": { "mode": "shell_command" },
        "command": format!(
            "{print_environment} > {}; exec cat",
            shell_quote(&typed_fifo.display().to_string())
        ),
    });
    coordination_cli(
        &data_dir,
        &["session-types", "create", "device", &definition.to_string()],
    );
    coordination_cli(
        &data_dir,
        &[
            "session-types",
            "spawn",
            "leak-probe",
            "--session-id",
            "leak-typed",
            "--target-id",
            "leak-root",
        ],
    );
    let typed = botster_environment(&read_fifo_to_end(&typed_fifo));

    // Failure messages name variables only; a value could be a credential.
    for (session_id, environment) in [("leak-raw", &raw), ("leak-typed", &typed)] {
        assert!(
            environment.get("BOTSTER_SESSION_ID").map(String::as_str) == Some(session_id),
            "{session_id}: BOTSTER_SESSION_ID is not its own session id"
        );
        let inherited = environment
            .iter()
            .filter(|(_, value)| value.starts_with("leaked-"))
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        assert!(
            inherited.is_empty(),
            "{session_id} inherited the launcher's value of {inherited:?}"
        );
        for name in ["BOTSTER_SESSION_UUID", "BOTSTER_MCP_TOKEN", "BOTSTER_SYNTHETIC_SECRET"] {
            assert!(
                !environment.contains_key(name),
                "{session_id} inherited {name}"
            );
        }
    }
    // A raw spawn has no context record, so no context id at all.
    assert!(
        !raw.contains_key("BOTSTER_CONTEXT_ID"),
        "leak-raw has a BOTSTER_CONTEXT_ID"
    );
    assert!(
        typed
            .get("BOTSTER_CONTEXT_ID")
            .is_some_and(|context| !context.starts_with("leaked-")),
        "leak-typed lacks its own BOTSTER_CONTEXT_ID"
    );

    for session_id in ["leak-raw", "leak-typed"] {
        coordination_shutdown_session(&data_dir, session_id);
    }
    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn mcp_receive_redelivers_until_ack_and_republish_is_idempotent() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("coordination-at-least-once");
    let daemon = start_cli_daemon(&data_dir);
    coordination_spawn(&data_dir, "session-inbox", "exec cat");
    let post = |envelope_id: &str, body: &str| {
        coordination_mcp_call(
            &data_dir,
            Some("session-sender"),
            "post_message",
            serde_json::json!({ "session_id": "session-inbox", "envelope_id": envelope_id, "body": body }),
        )["structuredContent"]["publish"]["deliveries"][0]
            .clone()
    };
    let receive = || {
        coordination_mcp_call(&data_dir, Some("session-inbox"), "receive_messages", serde_json::json!({}))
            ["structuredContent"]["messages"]
            .as_array()
            .expect("messages array")
            .iter()
            .map(|message| message["envelope_id"].as_str().expect("envelope id").to_string())
            .collect::<Vec<_>>()
    };

    let first = post("retry-1", "first");
    assert_eq!(first["status"], "queued");
    // Publishing the same id while it is outstanding changes nothing.
    let retried = post("retry-1", "first again");
    assert_eq!(retried["cursor"], first["cursor"], "{retried}");
    post("other-1", "second");

    // Without an ack, every receive returns the same envelopes again.
    assert_eq!(receive(), ["retry-1", "other-1"]);
    assert_eq!(receive(), ["retry-1", "other-1"], "unacked envelopes are redelivered");

    let acked = coordination_mcp_call(
        &data_dir,
        Some("session-inbox"),
        "ack_message",
        serde_json::json!({ "envelope_id": "retry-1" }),
    );
    assert_eq!(acked["structuredContent"]["ack"]["status"], "acknowledged");
    assert_eq!(receive(), ["other-1"], "an acked envelope is not redelivered");

    // After its ack, the same id can be published again.
    assert_eq!(post("retry-1", "after ack")["status"], "queued");
    assert_eq!(receive(), ["other-1", "retry-1"]);

    coordination_shutdown_session(&data_dir, "session-inbox");
    shutdown_cli_daemon(&data_dir, daemon);
}
