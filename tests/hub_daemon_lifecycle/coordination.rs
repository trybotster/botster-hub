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
