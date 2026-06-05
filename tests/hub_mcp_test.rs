#![cfg(unix)]

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

static MCP_DAEMON_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn mcp_daemon_test_lock() -> &'static Mutex<()> {
    MCP_DAEMON_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("mcp")
        .join(name)
        .join(nanos.to_string())
}

fn start_cli_daemon(data_dir: &Path) -> Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn botster-hub start");

    wait_for_status(data_dir, &mut child);
    child
}

fn wait_for_status(data_dir: &Path, child: &mut Child) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("check daemon child") {
            panic!("daemon exited before ready with {status}");
        }
        let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(data_dir)
            .output()
            .expect("run botster-hub status");
        if output.status.success() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("daemon did not become ready");
}

fn shutdown_cli_daemon(data_dir: &Path, child: Child) -> Output {
    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run botster-hub shutdown");
    assert!(
        shutdown.status.success(),
        "shutdown failed: {}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    let output = child.wait_with_output().expect("wait for daemon child");
    assert!(
        output.status.success(),
        "daemon failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn run_mcp_serve(data_dir: &Path, requests: &[Value]) -> Output {
    run_mcp_serve_with_session(data_dir, None, requests)
}

fn run_mcp_serve_with_session(
    data_dir: &Path,
    caller_session_id: Option<&str>,
    requests: &[Value],
) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("mcp-serve")
        .arg("--data-dir")
        .arg(data_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .envs(caller_session_id.map(|value| ("BOTSTER_SESSION_UUID", value)))
        .spawn()
        .expect("spawn botster-hub mcp-serve");

    {
        let stdin = child.stdin.as_mut().expect("mcp stdin");
        for request in requests {
            let line = serde_json::to_string(request).expect("serialize MCP request");
            stdin.write_all(line.as_bytes()).expect("write MCP request");
            stdin.write_all(b"\n").expect("write MCP newline");
        }
    }

    child.wait_with_output().expect("wait for mcp-serve")
}

fn parse_mcp_output(output: Output, label: &str) -> Vec<Value> {
    assert!(
        output.status.success(),
        "{label} mcp-serve failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "{label} diagnostics on stderr were unexpected: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("mcp stdout utf8");
    assert!(!stdout.contains("Content-Length"));
    stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("stdout line is JSON-RPC"))
        .collect()
}

fn initialize_request(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "clientInfo": {
                "name": "botster-hub-test",
                "version": "0.0.0"
            },
            "capabilities": {}
        }
    })
}

#[test]
fn mcp_serve_supports_initialize_list_and_native_status_over_stdio() {
    let _guard = mcp_daemon_test_lock().lock().expect("lock MCP daemon test");
    let data_dir = unique_test_dir("status-round-trip");
    let _ = fs::remove_dir_all(&data_dir);
    let daemon = start_cli_daemon(&data_dir);

    let output = run_mcp_serve(
        &data_dir,
        &[
            initialize_request(1),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list"
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "hub.status",
                    "arguments": {}
                }
            }),
        ],
    );
    let daemon_output = shutdown_cli_daemon(&data_dir, daemon);

    let messages = parse_mcp_output(output, "status");
    assert_eq!(messages.len(), 3);

    assert_eq!(messages[0]["id"], 1);
    assert_eq!(messages[0]["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(
        messages[0]["result"]["capabilities"]["tools"]["listChanged"],
        false
    );

    let tool_names = messages[1]["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"hub.status"));
    assert!(tool_names.contains(&"hub.sessions.list"));
    assert!(tool_names.contains(&"whoami"));
    assert!(tool_names.contains(&"post_message"));
    assert!(tool_names.contains(&"receive_messages"));
    assert!(tool_names.contains(&"ack_message"));
    assert!(tool_names.contains(&"notify_session"));

    assert_eq!(messages[2]["result"]["isError"], false);
    assert_eq!(
        messages[2]["result"]["structuredContent"]["lifecycle_state"],
        "running"
    );
    assert_eq!(
        messages[2]["result"]["structuredContent"]["core_initialized"],
        true
    );
    assert!(
        String::from_utf8_lossy(&daemon_output.stdout).contains("event=stopped"),
        "daemon should shut down cleanly"
    );
}

#[test]
fn mcp_native_coordination_tools_route_messages_through_daemon_envelopes() {
    let _guard = mcp_daemon_test_lock().lock().expect("lock MCP daemon test");
    let data_dir = unique_test_dir("coordination-round-trip");
    let _ = fs::remove_dir_all(&data_dir);
    let daemon = start_cli_daemon(&data_dir);

    let post = run_mcp_serve_with_session(
        &data_dir,
        Some("session-alpha"),
        &[
            initialize_request(1),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "whoami",
                    "arguments": {}
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "post_message",
                    "arguments": {
                        "session_id": "session-beta",
                        "envelope_id": "mcp-envelope-1",
                        "body": "hello beta"
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "post_message",
                    "arguments": {
                        "session_id": "session-slow",
                        "envelope_id": "mcp-slow-1",
                        "body": "slow one"
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "post_message",
                    "arguments": {
                        "session_id": "session-slow",
                        "envelope_id": "mcp-slow-2",
                        "body": "slow two"
                    }
                }
            }),
        ],
    );
    let post_messages = parse_mcp_output(post, "post");
    assert_eq!(
        post_messages[1]["result"]["structuredContent"]["identity"]["caller_session_id"],
        "session-alpha"
    );
    assert_eq!(
        post_messages[2]["result"]["structuredContent"]["publish"]["deliveries"][0]["envelope_id"],
        "mcp-envelope-1"
    );
    assert_eq!(
        post_messages[2]["result"]["structuredContent"]["publish"]["deliveries"][0]["status"],
        "queued"
    );

    let receive = run_mcp_serve_with_session(
        &data_dir,
        Some("session-beta"),
        &[
            initialize_request(1),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "receive_messages",
                    "arguments": {}
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "ack_message",
                    "arguments": {
                        "envelope_id": "mcp-envelope-1"
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "receive_messages",
                    "arguments": {
                        "after": 1
                    }
                }
            }),
        ],
    );
    let receive_messages = parse_mcp_output(receive, "receive");
    assert_eq!(
        receive_messages[1]["result"]["structuredContent"]["messages"][0]["envelope_id"],
        "mcp-envelope-1"
    );
    assert_eq!(
        receive_messages[1]["result"]["structuredContent"]["messages"][0]["body"],
        "hello beta"
    );
    assert!(
        receive_messages[1]["result"]["structuredContent"]["next_cursor"]
            .as_u64()
            .is_some(),
        "receive response should include next cursor"
    );
    assert_eq!(
        receive_messages[2]["result"]["structuredContent"]["ack"]["status"],
        "acknowledged"
    );
    assert_eq!(
        receive_messages[3]["result"]["structuredContent"]["messages"]
            .as_array()
            .expect("messages array")
            .len(),
        0,
        "after-cursor drain should not redeliver the already observed envelope"
    );

    let slow_receive = run_mcp_serve_with_session(
        &data_dir,
        Some("session-slow"),
        &[
            initialize_request(1),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "receive_messages",
                    "arguments": {
                        "limit": 2
                    }
                }
            }),
        ],
    );
    let slow_messages = parse_mcp_output(slow_receive, "slow receive");
    assert_eq!(
        slow_messages[1]["result"]["structuredContent"]["messages"]
            .as_array()
            .expect("slow messages array")
            .len(),
        2,
        "session-slow backlog should remain independent from session-beta cursor and ack"
    );

    let notify = run_mcp_serve_with_session(
        &data_dir,
        Some("session-alpha"),
        &[
            initialize_request(1),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "notify_session",
                    "arguments": {
                        "session_id": "missing-session",
                        "message": "doorbell"
                    }
                }
            }),
        ],
    );
    let notify_messages = parse_mcp_output(notify, "notify");
    assert!(
        notify_messages[1]["result"]["structuredContent"]["notify"]["decision"]
            .as_str()
            .expect("notify decision")
            .contains("unknown session"),
        "notify_session should report guarded-write fallback for unavailable sessions"
    );

    let daemon_output = shutdown_cli_daemon(&data_dir, daemon);
    assert!(
        String::from_utf8_lossy(&daemon_output.stdout).contains("event=stopped"),
        "daemon should shut down cleanly"
    );
}

#[test]
fn mcp_routed_envelopes_are_not_restart_durable_today() {
    let _guard = mcp_daemon_test_lock().lock().expect("lock MCP daemon test");
    let data_dir = unique_test_dir("coordination-restart-loss");
    let _ = fs::remove_dir_all(&data_dir);
    let daemon = start_cli_daemon(&data_dir);

    let post = run_mcp_serve_with_session(
        &data_dir,
        Some("session-alpha"),
        &[
            initialize_request(1),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "post_message",
                    "arguments": {
                        "session_id": "session-restart",
                        "envelope_id": "mcp-restart-1",
                        "body": "lost after restart"
                    }
                }
            }),
        ],
    );
    let post_messages = parse_mcp_output(post, "restart post");
    assert_eq!(
        post_messages[1]["result"]["structuredContent"]["publish"]["deliveries"][0]["status"],
        "queued"
    );
    shutdown_cli_daemon(&data_dir, daemon);

    let restarted = start_cli_daemon(&data_dir);
    let receive = run_mcp_serve_with_session(
        &data_dir,
        Some("session-restart"),
        &[
            initialize_request(1),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "receive_messages",
                    "arguments": {}
                }
            }),
        ],
    );
    let receive_messages = parse_mcp_output(receive, "restart receive");
    shutdown_cli_daemon(&data_dir, restarted);

    assert_eq!(
        receive_messages[1]["result"]["structuredContent"]["messages"]
            .as_array()
            .expect("messages array")
            .len(),
        0,
        "routed-envelope queues are in-memory and should be empty after daemon restart"
    );
}

#[test]
fn mcp_serve_returns_structured_tool_error_when_daemon_is_unavailable() {
    let _guard = mcp_daemon_test_lock().lock().expect("lock MCP daemon test");
    let data_dir = unique_test_dir("daemon-unavailable");
    let _ = fs::remove_dir_all(&data_dir);

    let output = run_mcp_serve(
        &data_dir,
        &[
            initialize_request(1),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "hub.status",
                    "arguments": {}
                }
            }),
        ],
    );

    let messages = parse_mcp_output(output, "daemon unavailable");
    assert_eq!(messages.len(), 2);
    let call = &messages[1];

    assert_eq!(call["id"], 2);
    assert_eq!(call["result"]["isError"], true);
    assert_eq!(
        call["result"]["structuredContent"]["error"]["code"],
        "daemon_unavailable"
    );
}
