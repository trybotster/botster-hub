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
                "botster-core",
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
    let disconnected_during_shutdown =
        shutdown_stderr.trim() == "botster-hub shutdown error: client disconnected";

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

    if shutdown.status.success() || disconnected_during_shutdown {
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
