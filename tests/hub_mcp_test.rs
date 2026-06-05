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
