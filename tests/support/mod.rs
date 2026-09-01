use std::path::Path;
use std::process::{Child, Command, Output};
use std::sync::{Mutex, MutexGuard, Once};

#[allow(dead_code)]
pub fn ensure_session_worker_binary() {
    static BUILD_WORKER: Once = Once::new();
    BUILD_WORKER.call_once(|| {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let status = Command::new("cargo")
            .args([
                "build",
                "--locked",
                "-p",
                "botster-core-daemon",
                "--bin",
                "botster-session-worker",
            ])
            .current_dir(manifest_dir)
            .status()
            .expect("worker binary build command should run");
        assert!(status.success(), "botster-session-worker should build");
    });
}

#[allow(dead_code)]
pub fn wait_for_cli_daemon_shutdown(shutdown: &Output, child: Child) -> Output {
    let daemon = child.wait_with_output().expect("wait for daemon child");
    validate_cli_daemon_shutdown(shutdown, &daemon).unwrap_or_else(|error| panic!("{error}"));
    daemon
}

#[allow(dead_code)]
pub fn validate_cli_daemon_shutdown(shutdown: &Output, daemon: &Output) -> Result<(), String> {
    let shutdown_stdout = String::from_utf8_lossy(&shutdown.stdout);
    let shutdown_stderr = String::from_utf8_lossy(&shutdown.stderr);

    if !daemon.status.success() {
        return Err(format!(
            "daemon failed: status={} stdout={:?} stderr={:?}; shutdown status={} stdout={:?} stderr={:?}",
            daemon.status,
            String::from_utf8_lossy(&daemon.stdout),
            String::from_utf8_lossy(&daemon.stderr),
            shutdown.status,
            shutdown_stdout,
            shutdown_stderr,
        ));
    }

    if shutdown.status.success() {
        Ok(())
    } else {
        Err(format!(
            "shutdown failed: status={} stdout={shutdown_stdout:?} stderr={shutdown_stderr:?}",
            shutdown.status,
        ))
    }
}

#[allow(dead_code)]
pub fn recovering_mutex_guard(lock: &'static Mutex<()>) -> MutexGuard<'static, ()> {
    lock.lock().unwrap_or_else(|error| error.into_inner())
}

#[allow(dead_code)]
pub fn bind_shared_terminal_adapter(
    runtime: &mut botster_hub::HubRuntime,
    client_id: botster_core::ClientId,
    session_id: botster_core::SessionId,
    subscription_id: botster_core::SubscriptionId,
) -> botster_core_test_support::terminal_adapter::SharedFakeTerminalAdapter {
    let generation = runtime
        .list_terminal_subscriptions()
        .into_iter()
        .find(|row| row.session_id == session_id && row.subscription_id == subscription_id)
        .map(|row| row.generation)
        .expect("live terminal generation");
    let adapter =
        botster_core_test_support::terminal_adapter::SharedFakeTerminalAdapter::auto_complete();
    runtime
        .bind_terminal_adapter(
            client_id,
            session_id,
            subscription_id,
            generation,
            botster_core::TerminalCapabilitySet::from_tokens(["terminal_streaming", "resize"])
                .expect("terminal capabilities"),
            Box::new(adapter.clone()),
        )
        .expect("bind shared terminal adapter");
    adapter
}

#[allow(dead_code)]
pub fn send_terminal_input(
    adapter: &botster_core_test_support::terminal_adapter::SharedFakeTerminalAdapter,
    data: &[u8],
) {
    let body_len = u16::try_from(data.len()).expect("test terminal input fits u16");
    let mut frame = Vec::with_capacity(4 + data.len());
    frame.extend_from_slice(&[1, 1]);
    frame.extend_from_slice(&body_len.to_be_bytes());
    frame.extend_from_slice(data);
    adapter.inject_ingress_frame(frame);
    let _ = adapter.wake(botster_core::TerminalWakeKind::Writable);
}

#[allow(dead_code)]
pub fn send_terminal_resize(
    adapter: &botster_core_test_support::terminal_adapter::SharedFakeTerminalAdapter,
    rows: u16,
    cols: u16,
) {
    let mut frame = vec![1, 3, 0, 4];
    frame.extend_from_slice(&rows.to_be_bytes());
    frame.extend_from_slice(&cols.to_be_bytes());
    adapter.inject_ingress_frame(frame);
    let _ = adapter.wake(botster_core::TerminalWakeKind::Writable);
}
