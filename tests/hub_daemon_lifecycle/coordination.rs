// Agent coordination over live sessions: identity injection and routed messages.

/// A session's bearer token. It prints nothing, so a failing assertion can
/// never put a credential into a test log.
struct SessionToken(String);

impl std::fmt::Debug for SessionToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SessionToken(..)")
    }
}

/// One HTTP reply, reduced to what the tests read.
struct HttpReply {
    status: u16,
    body: String,
}

/// The daemon's MCP endpoint, from the file the daemon records in its data
/// directory: `(port, url)`.
fn mcp_endpoint(data_dir: &Path) -> (u16, String) {
    let port = fs::read_to_string(data_dir.join("mcp-http.endpoint"))
        .expect("the daemon records its MCP port")
        .trim()
        .parse::<u16>()
        .expect("the recorded MCP port is a number");
    (port, format!("http://127.0.0.1:{port}/mcp"))
}

/// Send one raw HTTP request to the daemon's MCP port and read the reply to
/// the end of the connection. `head_lines` are the header lines, verbatim.
fn mcp_http_raw(port: u16, head_lines: &[String], body: &str) -> HttpReply {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to the MCP port");
    // timer: deadline — hang guard for a daemon that never answers or closes;
    // expiry ends the read, so the caller's status assertion fails the test.
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set MCP read deadline");
    let mut request = String::from("POST /mcp HTTP/1.1\r\n");
    for line in head_lines {
        request.push_str(line);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).expect("write MCP head");
    stream.write_all(body.as_bytes()).expect("write MCP body");
    let mut reply = Vec::new();
    // The daemon closes after a refusal, and the request asks for `close`
    // otherwise, so the end of the stream ends the reply.
    let _ = stream.read_to_end(&mut reply);
    let text = String::from_utf8_lossy(&reply).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((text.as_str(), ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    HttpReply {
        status,
        body: body.to_string(),
    }
}

/// A standard MCP request from `token`'s session, with the given extra headers.
fn mcp_http_post(
    data_dir: &Path,
    token: Option<&SessionToken>,
    extra_headers: &[&str],
    body: &str,
) -> HttpReply {
    let (port, _) = mcp_endpoint(data_dir);
    let mut lines = vec![
        format!("Host: 127.0.0.1:{port}"),
        "Content-Type: application/json".to_string(),
        format!("Content-Length: {}", body.len()),
        "Connection: close".to_string(),
    ];
    if let Some(token) = token {
        lines.push(format!("Authorization: Bearer {}", token.0));
    }
    lines.extend(extra_headers.iter().map(ToString::to_string));
    mcp_http_raw(port, &lines, body)
}

/// One tool call as the session `token` names; the reply's `result`.
fn coordination_mcp_call(
    data_dir: &Path,
    token: &SessionToken,
    tool: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": { "name": tool, "arguments": arguments }
    })
    .to_string();
    let reply = mcp_http_post(data_dir, Some(token), &[], &body);
    assert_eq!(reply.status, 200, "tool {tool} was refused with {}", reply.status);
    let message: serde_json::Value =
        serde_json::from_str(&reply.body).expect("the MCP reply is JSON");
    message["result"].clone()
}

/// Spawn a session whose command runs `command`, and read back its token and
/// endpoint URL from its own environment through a FIFO.
fn coordination_spawn_with_token(
    data_dir: &Path,
    session_id: &str,
    command: &str,
) -> (SessionToken, String) {
    let fifo_dir = unique_short_test_dir("coord-token");
    fs::create_dir_all(&fifo_dir).expect("create token fifo dir");
    let fifo = fifo_dir.join(format!("{session_id}.fifo"));
    make_fifo(&fifo);
    coordination_spawn(
        data_dir,
        session_id,
        &format!(
            "printf '%s\\n%s' \"$BOTSTER_MCP_TOKEN\" \"$BOTSTER_MCP_URL\" > {}; {command}",
            shell_quote(&fifo.display().to_string()),
        ),
    );
    let text = read_fifo_to_end(&fifo);
    let (token, url) = text.split_once('\n').expect("token and URL");
    (SessionToken(token.to_string()), url.to_string())
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
fn spawned_sessions_carry_the_identity_that_the_mcp_endpoint_reports() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-id");
    let daemon = start_cli_daemon(&data_dir);
    let (token, url) = coordination_spawn_with_token(&data_dir, "raw-identity", "exec cat");
    // The URL the session holds is the daemon's recorded endpoint.
    assert_eq!(url, mcp_endpoint(&data_dir).1);
    let whoami = coordination_mcp_call(&data_dir, &token, "whoami", serde_json::json!({}));
    let identity = &whoami["structuredContent"]["identity"];
    assert_eq!(identity["caller_session_id"], "raw-identity", "{whoami}");
    assert_eq!(identity["identity_source"], "caller_token");
    assert_eq!(identity["role"], "session");
    // A session is named with its hub: a bare session id means nothing across hubs.
    assert!(
        identity["host_id"].as_str().is_some_and(|host| !host.is_empty()),
        "the identity names its hub"
    );

    coordination_shutdown_session(&data_dir, "raw-identity");
    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn mcp_messages_round_trip_between_live_sessions() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-round-trip");
    let daemon = start_cli_daemon(&data_dir);
    let (alpha, _) = coordination_spawn_with_token(&data_dir, "session-alpha", "exec cat");
    let (beta, _) = coordination_spawn_with_token(&data_dir, "session-beta", "exec cat");
    let (slow, _) = coordination_spawn_with_token(&data_dir, "session-slow", "exec cat");

    let identity = coordination_mcp_call(&data_dir, &alpha, "whoami", serde_json::json!({}));
    assert_eq!(
        identity["structuredContent"]["identity"]["caller_session_id"],
        "session-alpha"
    );
    let hub_id = identity["structuredContent"]["identity"]["host_id"].clone();
    let posted = coordination_mcp_call(
        &data_dir,
        &alpha,
        "post_message",
        serde_json::json!({ "session_id": "session-beta", "envelope_id": "mcp-envelope-1", "body": "hello beta" }),
    );
    let delivery = &posted["structuredContent"]["publish"]["deliveries"][0];
    assert_eq!(delivery["envelope_id"], "mcp-envelope-1");
    assert_eq!(delivery["status"], "queued");
    for (envelope_id, body) in [("mcp-slow-1", "slow one"), ("mcp-slow-2", "slow two")] {
        coordination_mcp_call(
            &data_dir,
            &alpha,
            "post_message",
            serde_json::json!({ "session_id": "session-slow", "envelope_id": envelope_id, "body": body }),
        );
    }

    let received = coordination_mcp_call(&data_dir, &beta, "receive_messages", serde_json::json!({}));
    let message = &received["structuredContent"]["messages"][0];
    assert_eq!(message["envelope_id"], "mcp-envelope-1");
    assert_eq!(message["body"], "hello beta");
    // The sender is derived from alpha's token, structured, and names its hub.
    assert_eq!(message["source"]["kind"], "session");
    assert_eq!(message["source"]["session_id"], "session-alpha");
    assert_eq!(message["source"]["hub_id"], hub_id);
    let next_cursor = received["structuredContent"]["next_cursor"]
        .as_u64()
        .expect("receive response includes next cursor");
    let acked = coordination_mcp_call(
        &data_dir,
        &beta,
        "ack_message",
        serde_json::json!({ "envelope_id": "mcp-envelope-1" }),
    );
    assert_eq!(acked["structuredContent"]["ack"]["status"], "acknowledged");
    let after = coordination_mcp_call(
        &data_dir,
        &beta,
        "receive_messages",
        serde_json::json!({ "after": next_cursor }),
    );
    assert_eq!(
        after["structuredContent"]["messages"].as_array().map(Vec::len),
        Some(0),
        "an after-cursor drain does not redeliver the observed envelope"
    );

    let slow_messages =
        coordination_mcp_call(&data_dir, &slow, "receive_messages", serde_json::json!({ "limit": 2 }));
    assert_eq!(
        slow_messages["structuredContent"]["messages"].as_array().map(Vec::len),
        Some(2),
        "session-slow backlog stays independent from session-beta cursor and ack"
    );

    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn mcp_post_message_refuses_a_session_that_is_not_running() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-refusal");
    let daemon = start_cli_daemon(&data_dir);
    let (alpha, _) = coordination_spawn_with_token(&data_dir, "session-alpha", "exec cat");
    coordination_spawn(&data_dir, "session-ended", "exec cat");
    coordination_shutdown_session(&data_dir, "session-ended");

    for target in ["session-missing", "session-ended"] {
        let refused = coordination_mcp_call(
            &data_dir,
            &alpha,
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
    let data_dir = unique_short_test_dir("coord-restart-loss");
    let daemon = start_cli_daemon(&data_dir);
    let (sender, _) = coordination_spawn_with_token(&data_dir, "session-sender", "exec cat");
    let (target, url) = coordination_spawn_with_token(&data_dir, "session-restart", "exec cat");
    let posted = coordination_mcp_call(
        &data_dir,
        &sender,
        "post_message",
        serde_json::json!({ "session_id": "session-restart", "envelope_id": "mcp-restart-1", "body": "lost after restart" }),
    );
    assert_eq!(
        posted["structuredContent"]["publish"]["deliveries"][0]["status"],
        "queued"
    );
    // The sessions outlive the daemon; the restarted daemon adopts them.
    shutdown_cli_daemon(&data_dir, daemon.transfer_sessions());

    let restarted = start_cli_daemon(&data_dir);
    // Sessions hold the URL in their environment, so it must not move.
    assert_eq!(mcp_endpoint(&data_dir).1, url, "the endpoint URL survives a restart");
    // The same token still identifies the same session after the restart.
    let identity = coordination_mcp_call(&data_dir, &target, "whoami", serde_json::json!({}));
    assert_eq!(
        identity["structuredContent"]["identity"]["caller_session_id"],
        "session-restart"
    );
    let received = coordination_mcp_call(&data_dir, &target, "receive_messages", serde_json::json!({}));
    coordination_shutdown_session(&data_dir, "session-sender");
    coordination_shutdown_session(&data_dir, "session-restart");
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
            ("BOTSTER_MCP_URL", "http://leaked.invalid/mcp"),
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
        for name in ["BOTSTER_SESSION_UUID", "BOTSTER_SYNTHETIC_SECRET"] {
            assert!(
                !environment.contains_key(name),
                "{session_id} inherited {name}"
            );
        }
        // The token and URL are the session's own, issued at spawn.
        assert!(
            environment
                .get("BOTSTER_MCP_TOKEN")
                .is_some_and(|token| token.starts_with(&format!("{session_id}."))),
            "{session_id} does not hold its own BOTSTER_MCP_TOKEN"
        );
        assert!(
            environment
                .get("BOTSTER_MCP_URL")
                .is_some_and(|url| url == &mcp_endpoint(&data_dir).1),
            "{session_id} does not hold the daemon's BOTSTER_MCP_URL"
        );
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
    let data_dir = unique_short_test_dir("coord-at-least-once");
    let daemon = start_cli_daemon(&data_dir);
    let (sender, _) = coordination_spawn_with_token(&data_dir, "session-sender", "exec cat");
    let (inbox, _) = coordination_spawn_with_token(&data_dir, "session-inbox", "exec cat");
    let post = |envelope_id: &str, body: &str| {
        coordination_mcp_call(
            &data_dir,
            &sender,
            "post_message",
            serde_json::json!({ "session_id": "session-inbox", "envelope_id": envelope_id, "body": body }),
        )["structuredContent"]["publish"]["deliveries"][0]
            .clone()
    };
    let receive = || {
        coordination_mcp_call(&data_dir, &inbox, "receive_messages", serde_json::json!({}))
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
        &inbox,
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

#[test]
fn every_http_request_needs_a_token_that_proves_a_running_session() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-auth");
    let daemon = start_cli_daemon(&data_dir);
    let (alpha, _) = coordination_spawn_with_token(&data_dir, "session-alpha", "exec cat");
    let (beta, _) = coordination_spawn_with_token(&data_dir, "session-beta", "exec cat");
    let call = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "whoami", "arguments": {} }
    })
    .to_string();
    let initialize = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}
    })
    .to_string();

    // No token: there is no operator over HTTP.
    let missing = mcp_http_post(&data_dir, None, &[], &call);
    assert_eq!(missing.status, 401);
    assert!(missing.body.contains("caller_unauthenticated"));

    // alpha's id with beta's secret, and an unknown session with a well-formed
    // secret: both are refused with the same body, and neither is the operator.
    let (_, beta_secret) = beta.0.rsplit_once('.').expect("token separator");
    let forged = SessionToken(format!("session-alpha.{beta_secret}"));
    let unknown = SessionToken(format!("session-nobody.{beta_secret}"));
    let forged_reply = mcp_http_post(&data_dir, Some(&forged), &[], &call);
    let unknown_reply = mcp_http_post(&data_dir, Some(&unknown), &[], &call);
    assert_eq!(forged_reply.status, 401);
    assert_eq!(unknown_reply.status, 401);
    assert_eq!(forged_reply.body, unknown_reply.body, "no oracle for which sessions exist");
    // Even initialize, which needs no owner work, proves the token first.
    assert_eq!(mcp_http_post(&data_dir, Some(&forged), &[], &initialize).status, 401);

    // Every refusal that names a token or a secret leaves them out of the reply.
    for reply in [&missing, &forged_reply, &unknown_reply] {
        assert!(!reply.body.contains(beta_secret), "a refusal repeats a secret");
    }

    // A malformed token never reaches Core.
    let malformed = SessionToken("session-alpha.not-hex".to_string());
    assert_eq!(mcp_http_post(&data_dir, Some(&malformed), &[], &call).status, 401);

    // Cross-inbox: alpha cannot drain or ack beta's inbox, whatever it names.
    coordination_mcp_call(
        &data_dir,
        &alpha,
        "post_message",
        serde_json::json!({ "session_id": "session-beta", "envelope_id": "cross-1", "body": "for beta" }),
    );
    let alpha_view = coordination_mcp_call(&data_dir, &alpha, "receive_messages", serde_json::json!({}));
    assert_eq!(
        alpha_view["structuredContent"]["messages"].as_array().map(Vec::len),
        Some(0),
        "alpha's inbox holds nothing of beta's"
    );
    let named = coordination_mcp_call(
        &data_dir,
        &alpha,
        "receive_messages",
        serde_json::json!({ "session_id": "session-beta" }),
    );
    assert_eq!(named["isError"], true, "a target inbox argument is refused: {named}");
    let beta_view = coordination_mcp_call(&data_dir, &beta, "receive_messages", serde_json::json!({}));
    assert_eq!(
        beta_view["structuredContent"]["messages"][0]["source"]["session_id"],
        "session-alpha",
        "the sender is alpha's own session, derived from its token"
    );

    coordination_shutdown_session(&data_dir, "session-alpha");
    coordination_shutdown_session(&data_dir, "session-beta");
    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn tool_calls_are_audited_by_verified_caller_target_and_outcome() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-audit");
    let daemon = start_cli_daemon(&data_dir);
    let (alpha, _) = coordination_spawn_with_token(&data_dir, "session-alpha", "exec cat");
    coordination_spawn(&data_dir, "session-beta", "exec cat");

    let secret_body = "audit-body-must-not-be-logged";
    coordination_mcp_call(
        &data_dir,
        &alpha,
        "post_message",
        serde_json::json!({ "session_id": "session-beta", "body": secret_body }),
    );
    coordination_mcp_call(
        &data_dir,
        &alpha,
        "post_message",
        serde_json::json!({ "session_id": "session-missing", "body": secret_body }),
    );
    // A refused call to a remote hub keeps the hub it asked for.
    let remote = coordination_mcp_call(
        &data_dir,
        &alpha,
        "post_message",
        serde_json::json!({ "hub_id": "some-other-hub", "session_id": "session-beta", "body": secret_body }),
    );
    assert_eq!(remote["structuredContent"]["error"]["code"], "remote_hub_unsupported");
    // A hub that is not a string is not attributed to this hub either.
    let malformed = coordination_mcp_call(
        &data_dir,
        &alpha,
        "post_message",
        serde_json::json!({ "hub_id": 7, "session_id": "session-beta", "body": secret_body }),
    );
    assert_eq!(malformed["structuredContent"]["error"]["code"], "invalid_arguments");
    // A call whose token proves nothing is refused, and logged without a caller.
    let (_, alpha_secret) = alpha.0.rsplit_once('.').expect("token separator");
    let forged = SessionToken(format!("session-beta.{alpha_secret}"));
    let refused = mcp_http_post(
        &data_dir,
        Some(&forged),
        &[],
        &serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "whoami", "arguments": {} }
        })
        .to_string(),
    );
    assert_eq!(refused.status, 401);

    let all = coordination_cli(&data_dir, &["audit", "tools"]);
    let calls: Vec<serde_json::Value> = all
        .lines()
        .map(|line| serde_json::from_str(line).expect("an audit line is JSON"))
        .collect();
    assert_eq!(calls.len(), 5, "one line per tool call");
    // alpha posted to beta: caller alpha, target beta, ok.
    assert_eq!(calls[0]["tool"], "post_message");
    assert_eq!(calls[0]["caller"]["session_id"], "session-alpha");
    assert_eq!(calls[0]["target"]["session_id"], "session-beta");
    assert_eq!(calls[0]["outcome"], "ok");
    assert_eq!(calls[0]["caller"]["hub_id"], calls[0]["target"]["hub_id"]);
    // A refused post records its error code.
    assert_eq!(calls[1]["target"]["session_id"], "session-missing");
    assert_eq!(calls[1]["outcome"], "unknown_session");
    // The refused remote call names the remote hub, not this one.
    assert_eq!(calls[2]["outcome"], "remote_hub_unsupported");
    assert_eq!(calls[2]["target"]["hub_id"], "some-other-hub");
    assert_eq!(calls[2]["target"]["session_id"], "session-beta");
    assert_eq!(calls[2]["caller"]["hub_id"], calls[0]["caller"]["hub_id"]);
    // A malformed hub has no hub in the line, never this one.
    assert_eq!(calls[3]["outcome"], "invalid_arguments");
    assert!(calls[3]["target"]["hub_id"].is_null());
    assert_eq!(calls[3]["target"]["session_id"], "session-beta");
    // An unproven token is not a caller.
    assert_eq!(calls[4]["tool"], "whoami");
    assert!(calls[4]["caller"].is_null());
    assert_eq!(calls[4]["outcome"], "caller_unauthenticated");

    // Only the listed fields: no body, no token, no secret.
    assert!(!all.contains(secret_body), "a message body is in the audit log");
    assert!(!all.contains(alpha_secret), "a secret is in the audit log");
    for call in &calls {
        let mut fields: Vec<_> = call.as_object().expect("an object").keys().cloned().collect();
        fields.sort();
        assert_eq!(fields, ["caller", "outcome", "target", "tool", "ts_ms"]);
    }

    // The session filter keeps the calls a session made or received.
    let beta_only = coordination_cli(&data_dir, &["audit", "tools", "--session", "session-beta"]);
    assert_eq!(
        beta_only.lines().count(),
        3,
        "beta was the target of three calls: one served, one refused for a remote hub, one malformed"
    );
    let alpha_only = coordination_cli(&data_dir, &["audit", "tools", "--session", "session-alpha"]);
    assert_eq!(alpha_only.lines().count(), 4, "alpha made four calls");

    coordination_shutdown_session(&data_dir, "session-alpha");
    coordination_shutdown_session(&data_dir, "session-beta");
    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn a_token_stops_proving_when_its_session_ends() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-ended");
    let daemon = start_cli_daemon(&data_dir);
    let (first, _) = coordination_spawn_with_token(&data_dir, "session-cycle", "exec cat");
    let whoami = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "whoami", "arguments": {} }
    })
    .to_string();
    assert_eq!(mcp_http_post(&data_dir, Some(&first), &[], &whoami).status, 200);

    // The session ends, but Core keeps its registry row and its digest until
    // it releases the session: the token must not prove a dead session.
    coordination_shutdown_session(&data_dir, "session-cycle");
    for method in ["initialize", "ping", "tools/call"] {
        let request = if method == "tools/call" {
            whoami.clone()
        } else {
            serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": {} })
                .to_string()
        };
        assert_eq!(
            mcp_http_post(&data_dir, Some(&first), &[], &request).status,
            401,
            "an ended session's token proved {method}"
        );
    }

    // A token whose secret is not the stored one never proves the session, even
    // for a live one; a restart that replaces the digest (RestartSession) refuses
    // the old token the same way, and its test waits for that request.
    let (live, _) = coordination_spawn_with_token(&data_dir, "session-live", "exec cat");
    let (_, live_secret) = live.0.rsplit_once('.').expect("token separator");
    let other_secret = if live_secret.starts_with('0') { "1" } else { "0" }.repeat(64);
    let stale = SessionToken(format!("session-live.{other_secret}"));
    assert_eq!(mcp_http_post(&data_dir, Some(&live), &[], &whoami).status, 200);
    assert_eq!(mcp_http_post(&data_dir, Some(&stale), &[], &whoami).status, 401);
    coordination_shutdown_session(&data_dir, "session-live");
    coordination_shutdown_session(&data_dir, "session-cycle");
    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn a_remote_hub_is_refused_before_any_effect_and_listed_sessions_name_their_hub() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-remote");
    let daemon = start_cli_daemon(&data_dir);
    let (alpha, _) = coordination_spawn_with_token(&data_dir, "session-alpha", "exec cat");
    let (beta, _) = coordination_spawn_with_token(&data_dir, "session-beta", "exec cat");

    let hub_id = coordination_mcp_call(&data_dir, &alpha, "whoami", serde_json::json!({}))
        ["structuredContent"]["identity"]["host_id"]
        .clone();
    // A remote hub with an id that exists locally is not the local session.
    for (tool, arguments) in [
        (
            "post_message",
            serde_json::json!({ "hub_id": "some-other-hub", "session_id": "session-beta", "body": "not for beta" }),
        ),
        (
            "notify_session",
            serde_json::json!({ "hub_id": "some-other-hub", "session_id": "session-beta", "message": "doorbell" }),
        ),
    ] {
        let refused = coordination_mcp_call(&data_dir, &alpha, tool, arguments);
        assert_eq!(refused["isError"], true, "{tool}: {refused}");
        assert_eq!(
            refused["structuredContent"]["error"]["code"], "remote_hub_unsupported",
            "{tool}: {refused}"
        );
    }
    let inbox = coordination_mcp_call(&data_dir, &beta, "receive_messages", serde_json::json!({}));
    assert_eq!(
        inbox["structuredContent"]["messages"].as_array().map(Vec::len),
        Some(0),
        "a message for a remote hub reached a local session"
    );
    // This hub's own id is accepted.
    let posted = coordination_mcp_call(
        &data_dir,
        &alpha,
        "post_message",
        serde_json::json!({ "hub_id": hub_id, "session_id": "session-beta", "body": "for beta" }),
    );
    assert_eq!(posted["isError"], false, "{posted}");

    // Session references carry their hub.
    let listed = coordination_mcp_call(&data_dir, &alpha, "hub.sessions.list", serde_json::json!({}));
    let sessions = listed["structuredContent"]["sessions"].as_array().expect("sessions");
    assert!(!sessions.is_empty());
    assert!(
        sessions.iter().all(|session| session["hub_id"] == hub_id),
        "a listed session has no hub_id: {listed}"
    );

    coordination_shutdown_session(&data_dir, "session-alpha");
    coordination_shutdown_session(&data_dir, "session-beta");
    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn a_taken_port_is_reported_at_start_and_new_sessions_get_the_new_url() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-port");
    fs::create_dir_all(&data_dir).expect("create data dir");
    // Another process holds the port the data directory recorded.
    let holder = TcpListener::bind("127.0.0.1:0").expect("hold a port");
    let taken = holder.local_addr().expect("held address").port();
    fs::write(data_dir.join("mcp-http.endpoint"), format!("{taken}\n")).expect("record the port");

    let daemon = start_cli_daemon(&data_dir);
    let (port, url) = mcp_endpoint(&data_dir);
    assert_ne!(port, taken, "the daemon took a port that was held");
    let (token, session_url) = coordination_spawn_with_token(&data_dir, "session-port", "exec cat");
    assert_eq!(session_url, url, "a new session holds the new URL");
    let whoami = coordination_mcp_call(&data_dir, &token, "whoami", serde_json::json!({}));
    assert_eq!(whoami["structuredContent"]["identity"]["caller_session_id"], "session-port");

    coordination_shutdown_session(&data_dir, "session-port");
    let output = shutdown_cli_daemon(&data_dir, daemon);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("mcp http port changed") && stderr.contains(&port.to_string()),
        "the daemon did not report the port change: {stderr}"
    );
    drop(holder);
}

#[test]
fn the_http_listener_refuses_browsers_and_oversized_or_odd_requests() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-http-gates");
    let daemon = start_cli_daemon(&data_dir);
    let (alpha, _) = coordination_spawn_with_token(&data_dir, "session-alpha", "exec cat");
    let call = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "whoami", "arguments": {} }
    })
    .to_string();
    let (port, _) = mcp_endpoint(&data_dir);

    // A page in a browser always sends Origin; any Origin is refused.
    let browser = mcp_http_post(&data_dir, Some(&alpha), &["Origin: http://evil.example"], &call);
    assert_eq!(browser.status, 403);
    // DNS rebinding: a Host that is not this listener.
    let rebound = mcp_http_raw(
        port,
        &[
            "Host: evil.example".to_string(),
            "Content-Type: application/json".to_string(),
            format!("Content-Length: {}", call.len()),
            format!("Authorization: Bearer {}", alpha.0),
            "Connection: close".to_string(),
        ],
        &call,
    );
    assert_eq!(rebound.status, 403);
    // The body bound: a length over the limit is refused before any body.
    let oversized = mcp_http_raw(
        port,
        &[
            format!("Host: 127.0.0.1:{port}"),
            "Content-Type: application/json".to_string(),
            format!("Content-Length: {}", botster_hub_client::MAX_CONTROL_REQUEST_BYTES + 1),
            format!("Authorization: Bearer {}", alpha.0),
            "Connection: close".to_string(),
        ],
        "",
    );
    assert_eq!(oversized.status, 413);
    // Chunked framing is refused.
    let chunked = mcp_http_raw(
        port,
        &[
            format!("Host: 127.0.0.1:{port}"),
            "Content-Type: application/json".to_string(),
            "Transfer-Encoding: chunked".to_string(),
            format!("Authorization: Bearer {}", alpha.0),
            "Connection: close".to_string(),
        ],
        "0\r\n\r\n",
    );
    assert_eq!(chunked.status, 411);
    // The daemon still serves the right request after every refusal.
    let ok = mcp_http_post(&data_dir, Some(&alpha), &[], &call);
    assert_eq!(ok.status, 200);

    coordination_shutdown_session(&data_dir, "session-alpha");
    shutdown_cli_daemon(&data_dir, daemon);
}

#[test]
fn a_session_credential_reaches_only_its_session() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("coord-cred");
    let daemon = start_cli_daemon(&data_dir);
    let (token, _) = coordination_spawn_with_token(&data_dir, "cred-session", "exec cat");
    let (session_id, secret) = token.0.rsplit_once('.').expect("token separator");
    assert_eq!(session_id, "cred-session");
    assert_eq!(secret.len(), 64, "a 256-bit secret in hex");
    assert!(secret.bytes().all(|byte| byte.is_ascii_hexdigit()));

    // Nothing a client or operator can read carries the token or its secret.
    let mut observed = vec![
        ("sessions list", coordination_cli(&data_dir, &["sessions", "list"])),
        ("status", coordination_cli(&data_dir, &["status"])),
    ];
    for (tool, arguments) in [
        ("whoami", serde_json::json!({})),
        ("hub.sessions.list", serde_json::json!({})),
        ("hub.status", serde_json::json!({})),
        (
            "post_message",
            serde_json::json!({ "session_id": "cred-missing", "body": "refused" }),
        ),
        (
            "notify_session",
            serde_json::json!({ "session_id": "cred-session", "message": "deferred" }),
        ),
    ] {
        observed.push((
            tool,
            coordination_mcp_call(&data_dir, &token, tool, arguments).to_string(),
        ));
    }
    // A refused request repeats nothing of the token either.
    let refused = mcp_http_post(&data_dir, None, &[], "{}");
    observed.push(("a refused request", refused.body));
    coordination_shutdown_session(&data_dir, "cred-session");
    let output = shutdown_cli_daemon(&data_dir, daemon);
    observed.push(("daemon stdout", String::from_utf8_lossy(&output.stdout).into_owned()));
    observed.push(("daemon stderr", String::from_utf8_lossy(&output.stderr).into_owned()));
    for (surface, text) in &observed {
        assert_secret_absent(surface, text, secret);
    }
}

/// Fail naming the surface only: printing the text that holds a secret would
/// turn a failed secrecy check into a leak of its own.
fn assert_secret_absent(surface: &str, text: &str, secret: &str) {
    assert!(!text.contains(secret), "a session secret appears in {surface}");
}

#[test]
fn a_leaked_secret_is_reported_by_surface_name_only() {
    let secret = "synthetic-secret-9d41c7";
    let leaked = format!("{{\"token\":\"cred-session.{secret}\"}}");
    let failure = std::panic::catch_unwind(|| {
        assert_secret_absent("synthetic surface", &leaked, secret);
    })
    .expect_err("a leaked secret fails the check");
    let message = failure
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| failure.downcast_ref::<&str>().map(ToString::to_string))
        .expect("a panic message");
    assert!(message.contains("synthetic surface"), "the failure names the surface");
    assert!(!message.contains(secret), "the failure does not repeat the secret");
}
