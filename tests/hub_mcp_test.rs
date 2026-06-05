#![cfg(unix)]

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

mod support;
use support::ensure_session_worker_binary;

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
    ensure_session_worker_binary();
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

fn enable_project_pipelines_package(data_dir: &Path) {
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--path")
        .arg(Path::new("examples").join("project-pipelines"))
        .output()
        .expect("run botster-hub packages enable --path examples/project-pipelines");
    assert!(
        output.status.success(),
        "project pipelines package enable failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn disable_project_pipelines_package(data_dir: &Path) {
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("disable")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("project-pipelines")
        .output()
        .expect("run botster-hub packages disable project-pipelines");
    assert!(
        output.status.success(),
        "project pipelines package disable failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn run_mcp_serve(data_dir: &Path, requests: &[Value]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("mcp-serve")
        .arg("--data-dir")
        .arg(data_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
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

fn mcp_messages(output: Output, expected_count: usize) -> Vec<Value> {
    assert!(
        output.status.success(),
        "mcp-serve failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "mcp diagnostics on stderr were unexpected: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("mcp stdout utf8");
    assert!(!stdout.contains("Content-Length"));
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), expected_count);
    lines
        .iter()
        .map(|line| serde_json::from_str::<Value>(line).expect("stdout line is JSON-RPC"))
        .collect()
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
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "clientInfo": {
                        "name": "botster-hub-test",
                        "version": "0.0.0"
                    },
                    "capabilities": {}
                }
            }),
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

    assert!(
        output.status.success(),
        "mcp-serve failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "mcp diagnostics on stderr were unexpected: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("mcp stdout utf8");
    assert!(!stdout.contains("Content-Length"));
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 3);
    let messages = lines
        .iter()
        .map(|line| serde_json::from_str::<Value>(line).expect("stdout line is JSON-RPC"))
        .collect::<Vec<_>>();

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
fn mcp_serve_returns_structured_tool_error_when_daemon_is_unavailable() {
    let _guard = mcp_daemon_test_lock().lock().expect("lock MCP daemon test");
    let data_dir = unique_test_dir("daemon-unavailable");
    let _ = fs::remove_dir_all(&data_dir);

    let output = run_mcp_serve(
        &data_dir,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "clientInfo": {
                        "name": "botster-hub-test",
                        "version": "0.0.0"
                    },
                    "capabilities": {}
                }
            }),
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

    assert!(
        output.status.success(),
        "mcp-serve failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "mcp diagnostics on stderr were unexpected: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("mcp stdout utf8");
    assert!(!stdout.contains("Content-Length"));
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    let call = serde_json::from_str::<Value>(lines[1]).expect("tool call response is JSON-RPC");

    assert_eq!(call["id"], 2);
    assert_eq!(call["result"]["isError"], true);
    assert_eq!(
        call["result"]["structuredContent"]["error"]["code"],
        "daemon_unavailable"
    );
}

#[test]
fn mcp_serve_lists_calls_and_reloads_project_pipelines_plugin_tools() {
    let _guard = mcp_daemon_test_lock().lock().expect("lock MCP daemon test");
    let data_dir = unique_test_dir("project-pipelines-plugin");
    let _ = fs::remove_dir_all(&data_dir);
    let daemon = start_cli_daemon(&data_dir);
    enable_project_pipelines_package(&data_dir);

    let output = run_mcp_serve(
        &data_dir,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "clientInfo": { "name": "botster-hub-test", "version": "0.0.0" },
                    "capabilities": {}
                }
            }),
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "project_pipelines.create",
                    "arguments": { "title": "Local workflow parity" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "project_pipelines.update",
                    "arguments": {
                        "ticket_id": "ticket_local_1",
                        "status": "planned"
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "project_pipelines.start",
                    "arguments": {
                        "ticket_id": "ticket_local_1",
                        "target_id": "tgt_local_project",
                        "worktree": "worktrees/project-pipelines-local",
                        "agent_name": "codex"
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "project_pipelines.submit_gate",
                    "arguments": {
                        "run_id": "run_local_1",
                        "gate_id": "implement",
                        "status": "passed",
                        "summary": "local gate evidence",
                        "evidence": { "command": "mcp e2e" }
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "project_pipelines.request_step_advance",
                    "arguments": {
                        "run_id": "run_local_1",
                        "summary": "ready for review"
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "project_pipelines.current_context",
                    "arguments": {}
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "project_pipelines.start",
                    "arguments": {
                        "ticket_id": "ticket_local_missing",
                        "target_id": "tgt_local_project",
                        "worktree": "worktrees/project-pipelines-local"
                    }
                }
            }),
        ],
    );
    let messages = mcp_messages(output, 9);

    let tool_names = messages[1]["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"project_pipelines.create"));
    assert!(tool_names.contains(&"project_pipelines.start"));

    assert_eq!(messages[2]["result"]["isError"], false);
    assert_eq!(
        messages[2]["result"]["structuredContent"]["ticket"]["id"],
        "ticket_local_1"
    );
    assert_eq!(messages[4]["result"]["isError"], false);
    assert_eq!(
        messages[4]["result"]["structuredContent"]["run"]["coordination"]["target_id"],
        "tgt_local_project"
    );
    assert_eq!(
        messages[4]["result"]["structuredContent"]["run"]["coordination"]["assigned_worktree"],
        "worktrees/project-pipelines-local"
    );
    assert_eq!(
        messages[4]["result"]["structuredContent"]["run"]["coordination"]["owner_plugin"],
        "project-pipelines"
    );
    assert_eq!(
        messages[7]["result"]["structuredContent"]["runs"][0]["status"],
        "ready_for_review"
    );
    assert_eq!(messages[8]["result"]["structuredContent"]["ok"], false);
    assert_eq!(
        messages[8]["result"]["structuredContent"]["error"]["code"],
        "not_found"
    );
    assert!(
        data_dir
            .join("plugin-data")
            .join("project-pipelines")
            .join("state.json")
            .exists(),
        "Project Pipelines state should live under plugin-data/project-pipelines"
    );

    shutdown_cli_daemon(&data_dir, daemon);
    let restarted = start_cli_daemon(&data_dir);
    let output = run_mcp_serve(
        &data_dir,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "clientInfo": { "name": "botster-hub-test", "version": "0.0.0" },
                    "capabilities": {}
                }
            }),
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "project_pipelines.current_context",
                    "arguments": {}
                }
            }),
        ],
    );
    let messages = mcp_messages(output, 3);
    let tool_names = messages[1]["result"]["tools"]
        .as_array()
        .expect("tools array after restart")
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert!(
        tool_names.contains(&"project_pipelines.current_context"),
        "Project Pipelines tools should be re-registered after daemon restart"
    );
    assert_eq!(
        messages[2]["result"]["structuredContent"]["tickets"][0]["title"],
        "Local workflow parity"
    );
    assert_eq!(
        messages[2]["result"]["structuredContent"]["runs"][0]["coordination"]["request_id"],
        "project-pipelines:ticket_local_1:1"
    );

    disable_project_pipelines_package(&data_dir);
    let output = run_mcp_serve(
        &data_dir,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "clientInfo": { "name": "botster-hub-test", "version": "0.0.0" },
                    "capabilities": {}
                }
            }),
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
        ],
    );
    let messages = mcp_messages(output, 2);
    let tool_names = messages[1]["result"]["tools"]
        .as_array()
        .expect("tools array after disable")
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert!(
        !tool_names.contains(&"project_pipelines.current_context"),
        "Project Pipelines tools should be removed after package disable"
    );
    shutdown_cli_daemon(&data_dir, restarted);
}
