//! Local runtime daemon process ownership.
//!
//! Owns start, reuse, metadata, PID validation, signal, reap, and stale-daemon
//! recovery. Package refresh, web launch, and operator-console composition stay
//! in `main`. WebRTC smoke lives in `local_webrtc_smoke`.

use std::env;
use std::io::{self, BufRead, BufReader};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use botster_hub::{DaemonRequest, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, daemon_transport_request};
use serde::{Deserialize, Serialize};

use super::{
    LocalRuntimeDaemonOwnership, LocalRuntimeError, LocalRuntimeOptions, sanitize_runtime_message,
};

const LOCAL_RUNTIME_DAEMON_METADATA_FILE: &str = ".botster-hub-runtime-daemon.json";
const TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS_ENV: &str =
    "BOTSTER_HUB_TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS";

pub(crate) struct StartedRuntimeCleanup<'a> {
    config: &'a botster_hub::HubConfig,
    armed: bool,
}

impl<'a> StartedRuntimeCleanup<'a> {
    pub(crate) fn new(
        config: &'a botster_hub::HubConfig,
        daemon_ownership: LocalRuntimeDaemonOwnership,
    ) -> Self {
        Self {
            config,
            armed: matches!(daemon_ownership, LocalRuntimeDaemonOwnership::Started),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StartedRuntimeCleanup<'_> {
    fn drop(&mut self) {
        if self.armed {
            let metadata = read_runtime_daemon_metadata(&self.config.data_directory)
                .ok()
                .flatten();
            let _ = daemon_transport_request(self.config, DaemonRequest::DaemonShutdown);
            if let Some(metadata) = metadata {
                let _ = wait_for_runtime_daemon_exit(metadata.pid);
            }
            let _ = remove_configured_local_socket(self.config);
            let _ = remove_runtime_daemon_metadata(&self.config.data_directory);
        }
    }
}

pub(crate) fn ensure_local_runtime_daemon(
    hub_bin: &Path,
    options: &LocalRuntimeOptions,
    config: &botster_hub::HubConfig,
) -> Result<LocalRuntimeDaemonOwnership, LocalRuntimeError> {
    match daemon_transport_request(config, DaemonRequest::Status) {
        Ok(_) => return Ok(LocalRuntimeDaemonOwnership::Reused),
        Err(
            botster_hub::DaemonTransportError::NotRunning
            | botster_hub::DaemonTransportError::ClientDisconnected,
        ) => {}
        Err(botster_hub::DaemonTransportError::Compatibility(error)) => {
            if recover_owned_stale_runtime_daemon(&options.data_directory, config)? {
                return spawn_local_runtime_daemon(hub_bin, options, config);
            }
            return Err(LocalRuntimeError::IncompatibleDaemon(error.to_string()));
        }
        Err(botster_hub::DaemonTransportError::Protocol(message)) => {
            if recover_owned_stale_runtime_daemon(&options.data_directory, config)? {
                return spawn_local_runtime_daemon(hub_bin, options, config);
            }
            return Err(LocalRuntimeError::IncompatibleDaemon(message.to_string()));
        }
        Err(error) => return Err(error.into()),
    }

    spawn_local_runtime_daemon(hub_bin, options, config)
}

pub(crate) fn spawn_local_runtime_daemon(
    hub_bin: &Path,
    options: &LocalRuntimeOptions,
    config: &botster_hub::HubConfig,
) -> Result<LocalRuntimeDaemonOwnership, LocalRuntimeError> {
    if !hub_bin.is_file() {
        return Err(LocalRuntimeError::MissingHubBinary(hub_bin.to_path_buf()));
    }
    let session_worker_bin = options.session_worker_bin(hub_bin)?;

    let mut command = Command::new(hub_bin);
    command
        .arg("start")
        .arg("--data-dir")
        .arg(&options.data_directory)
        .arg("--session-worker-bin")
        .arg(&session_worker_bin)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    unsafe {
        // SAFETY: this hook runs in the daemon child after fork and only creates a new process
        // group, keeping terminal-generated signals scoped away from the operator console.
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .map_err(|source| LocalRuntimeError::SpawnDaemon {
            path: hub_bin.to_path_buf(),
            source,
        })?;
    let (stderr_tx, stderr_rx) = mpsc::channel();
    if let Some(stderr) = child.stderr.take() {
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let _ = stderr_tx.send(line);
            }
        });
    }

    if let Err(error) = write_runtime_daemon_metadata(
        &options.data_directory,
        config,
        hub_bin,
        &session_worker_bin,
        child.id(),
    ) {
        let _ = terminate_owned_runtime_child(&mut child);
        return Err(error);
    }

    if let Err(error) = wait_for_local_runtime_ready(
        config,
        &mut child,
        local_runtime_daemon_readiness_budget(),
        &stderr_rx,
    ) {
        let _ = terminate_owned_runtime_child(&mut child);
        let _ = remove_runtime_daemon_metadata(&options.data_directory);
        let _ = remove_configured_local_socket(config);
        return Err(error);
    }
    reap_local_runtime_daemon_on_exit(child);
    Ok(LocalRuntimeDaemonOwnership::Started)
}

fn reap_local_runtime_daemon_on_exit(mut child: Child) {
    thread::spawn(move || {
        let _ = child.wait();
    });
}

fn wait_for_local_runtime_ready(
    config: &botster_hub::HubConfig,
    child: &mut Child,
    readiness_budget: Duration,
    stderr_rx: &mpsc::Receiver<String>,
) -> Result<(), LocalRuntimeError> {
    let started_at = Instant::now();
    let deadline = started_at + readiness_budget;
    let mut last_probe = "status probe not attempted".to_string();
    let mut stderr_tail = String::new();
    while Instant::now() < deadline {
        drain_runtime_stderr(stderr_rx, &mut stderr_tail);
        if let Some(status) = child.try_wait().map_err(LocalRuntimeError::PollDaemon)? {
            thread::sleep(Duration::from_millis(20));
            drain_runtime_stderr(stderr_rx, &mut stderr_tail);
            return Err(LocalRuntimeError::DaemonExited {
                status: status.to_string(),
                elapsed: started_at.elapsed(),
                readiness_budget,
                last_probe,
                stderr_tail,
            });
        }
        match daemon_transport_request(config, DaemonRequest::Status) {
            Ok(_) => return Ok(()),
            Err(error) => last_probe = error.to_string(),
        }
        thread::sleep(Duration::from_millis(50));
    }

    let child_pid = child.id();
    let child_status = terminate_owned_runtime_child(child)?;
    Err(LocalRuntimeError::ReadinessTimeout {
        elapsed: started_at.elapsed(),
        readiness_budget,
        last_probe,
        child_pid,
        child_status,
    })
}

fn drain_runtime_stderr(stderr_rx: &mpsc::Receiver<String>, stderr_tail: &mut String) {
    for line in stderr_rx.try_iter() {
        if !stderr_tail.is_empty() {
            stderr_tail.push(' ');
        }
        stderr_tail.push_str(&sanitize_runtime_message(&line));
        if stderr_tail.len() > 8_192 {
            let keep_from = stderr_tail.len() - 8_192;
            stderr_tail.drain(..keep_from);
        }
    }
}

fn local_runtime_daemon_readiness_budget() -> Duration {
    if env::var("BOTSTER_ENV").as_deref() == Ok("test")
        && let Some(milliseconds) = env::var_os(TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS_ENV)
            .and_then(|value| value.to_str().and_then(|value| value.parse::<u64>().ok()))
    {
        return Duration::from_millis(milliseconds);
    }
    LOCAL_RUNTIME_DAEMON_READINESS_BUDGET
}

fn terminate_owned_runtime_child(child: &mut Child) -> Result<String, LocalRuntimeError> {
    let pid = child.id();
    let mut child_status = child
        .try_wait()
        .map_err(LocalRuntimeError::PollDaemon)?
        .map(|status| status.to_string());
    signal_owned_runtime_child(pid, libc::SIGTERM).map_err(LocalRuntimeError::TerminateDaemon)?;
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        if child_status.is_none() {
            child_status = child
                .try_wait()
                .map_err(LocalRuntimeError::PollDaemon)?
                .map(|status| status.to_string());
        }
        if !process_group_exists(pid)
            && let Some(status) = child_status.take()
        {
            return Ok(status);
        }
        thread::sleep(Duration::from_millis(20));
    }
    signal_owned_runtime_child(pid, libc::SIGKILL).map_err(LocalRuntimeError::TerminateDaemon)?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if child_status.is_none() {
            child_status = child
                .try_wait()
                .map_err(LocalRuntimeError::PollDaemon)?
                .map(|status| status.to_string());
        }
        if !process_group_exists(pid)
            && let Some(status) = child_status.take()
        {
            return Ok(status);
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(LocalRuntimeError::TerminateDaemonTimeout(pid))
}

fn process_group_exists(pid: u32) -> bool {
    if unsafe { libc::killpg(pid as libc::pid_t, 0) } == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

fn signal_owned_runtime_child(pid: u32, signal: libc::c_int) -> io::Result<()> {
    if unsafe { libc::killpg(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let group_error = io::Error::last_os_error();
    if group_error.raw_os_error() != Some(libc::ESRCH) {
        return Err(group_error);
    }
    if unsafe { libc::kill(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let child_error = io::Error::last_os_error();
    if child_error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(child_error)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct LocalRuntimeDaemonMetadata {
    pub(crate) pid: u32,
    data_directory: String,
    #[serde(default)]
    data_directory_arg: Option<String>,
    socket_path: String,
    hub_bin: String,
    #[serde(default)]
    pub(crate) session_worker_bin: Option<String>,
}

pub(crate) fn recover_owned_stale_runtime_daemon(
    data_directory: &Path,
    config: &botster_hub::HubConfig,
) -> Result<bool, LocalRuntimeError> {
    let Some(metadata) = read_runtime_daemon_metadata(data_directory)? else {
        return Ok(false);
    };
    if !runtime_daemon_metadata_matches(&metadata, data_directory, config)? {
        return Ok(false);
    }
    let Some(command) = process_command(metadata.pid)? else {
        return Ok(false);
    };
    if !runtime_daemon_command_matches(&metadata, &command) {
        return Ok(false);
    }

    terminate_process(metadata.pid)?;
    wait_for_runtime_daemon_exit(metadata.pid)?;
    remove_configured_local_socket(config)?;
    remove_runtime_daemon_metadata(data_directory)?;
    Ok(true)
}

pub(crate) fn owned_runtime_daemon_pid(
    data_directory: &Path,
    config: &botster_hub::HubConfig,
) -> Result<Option<u32>, LocalRuntimeError> {
    let Some(metadata) = read_runtime_daemon_metadata(data_directory)? else {
        return Ok(None);
    };
    if !runtime_daemon_metadata_matches(&metadata, data_directory, config)? {
        return Ok(None);
    }
    let Some(command) = process_command(metadata.pid)? else {
        return Ok(None);
    };
    if !runtime_daemon_command_matches(&metadata, &command) {
        return Ok(None);
    }
    Ok(Some(metadata.pid))
}

fn write_runtime_daemon_metadata(
    data_directory: &Path,
    config: &botster_hub::HubConfig,
    hub_bin: &Path,
    session_worker_bin: &Path,
    pid: u32,
) -> Result<(), LocalRuntimeError> {
    let metadata = LocalRuntimeDaemonMetadata {
        pid,
        data_directory: stable_path_string(data_directory),
        data_directory_arg: Some(data_directory.display().to_string()),
        socket_path: configured_local_socket_path(config)?.display().to_string(),
        hub_bin: stable_path_string(hub_bin),
        session_worker_bin: Some(stable_path_string(session_worker_bin)),
    };
    let bytes =
        serde_json::to_vec_pretty(&metadata).map_err(LocalRuntimeError::SerializeMetadata)?;
    std::fs::write(runtime_daemon_metadata_path(data_directory), bytes).map_err(|source| {
        LocalRuntimeError::WriteDaemonMetadata {
            path: runtime_daemon_metadata_path(data_directory),
            source,
        }
    })
}

pub(crate) fn read_runtime_daemon_metadata(
    data_directory: &Path,
) -> Result<Option<LocalRuntimeDaemonMetadata>, LocalRuntimeError> {
    let path = runtime_daemon_metadata_path(data_directory);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(LocalRuntimeError::ReadDaemonMetadata { path, source }),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(LocalRuntimeError::ReadDaemonMetadataJson)
}

fn runtime_daemon_metadata_matches(
    metadata: &LocalRuntimeDaemonMetadata,
    data_directory: &Path,
    config: &botster_hub::HubConfig,
) -> Result<bool, LocalRuntimeError> {
    Ok(
        metadata.data_directory == stable_path_string(data_directory)
            && metadata.socket_path == configured_local_socket_path(config)?.display().to_string(),
    )
}

fn runtime_daemon_command_matches(metadata: &LocalRuntimeDaemonMetadata, command: &str) -> bool {
    let hub_bin_name = Path::new(&metadata.hub_bin)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("botster-hub");
    // PID reuse cannot be proven away with macOS process-table primitives alone.
    // Recovery therefore treats the live PID's command line as required ownership
    // evidence and refuses to signal when any recorded daemon token is missing.
    command.contains(hub_bin_name)
        && command.contains(" start ")
        && command.contains("--data-dir")
        && (command.contains(&metadata.data_directory)
            || metadata
                .data_directory_arg
                .as_ref()
                .is_some_and(|argument| command.contains(argument)))
}

fn configured_local_socket_path(
    config: &botster_hub::HubConfig,
) -> Result<PathBuf, LocalRuntimeError> {
    config
        .transports
        .local_socket
        .as_ref()
        .map(|binding| binding.path.clone())
        .ok_or(LocalRuntimeError::MissingLocalSocket)
}

fn remove_configured_local_socket(
    config: &botster_hub::HubConfig,
) -> Result<(), LocalRuntimeError> {
    let socket_path = configured_local_socket_path(config)?;
    match std::fs::remove_file(&socket_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(LocalRuntimeError::RemoveLocalSocket {
            path: socket_path,
            source,
        }),
    }
}

fn remove_runtime_daemon_metadata(data_directory: &Path) -> Result<(), LocalRuntimeError> {
    let path = runtime_daemon_metadata_path(data_directory);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(LocalRuntimeError::RemoveDaemonMetadata { path, source }),
    }
}

pub(crate) fn complete_owned_runtime_daemon_shutdown(
    data_directory: &Path,
    config: &botster_hub::HubConfig,
    owned_daemon_pid: Option<u32>,
) -> Result<(), LocalRuntimeError> {
    let Some(pid) = owned_daemon_pid else {
        return Ok(());
    };
    wait_for_owned_runtime_daemon_reaped(pid)?;
    remove_configured_local_socket(config)?;
    remove_runtime_daemon_metadata(data_directory)
}

fn runtime_daemon_metadata_path(data_directory: &Path) -> PathBuf {
    data_directory.join(LOCAL_RUNTIME_DAEMON_METADATA_FILE)
}

fn stable_path_string(path: &Path) -> String {
    std::fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

fn process_command(pid: u32) -> Result<Option<String>, LocalRuntimeError> {
    let output = Command::new("ps")
        .arg("-p")
        .arg(pid.to_string())
        .arg("-o")
        .arg("command=")
        .output()
        .map_err(LocalRuntimeError::InspectProcess)?;
    if !output.status.success() {
        return Ok(None);
    }
    let command = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if command.is_empty() {
        Ok(None)
    } else {
        Ok(Some(command))
    }
}

fn terminate_process(pid: u32) -> Result<(), LocalRuntimeError> {
    let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    if result == 0 {
        Ok(())
    } else {
        Err(LocalRuntimeError::TerminateDaemon(
            io::Error::last_os_error(),
        ))
    }
}

fn wait_for_runtime_daemon_exit(pid: u32) -> Result<(), LocalRuntimeError> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match process_state(pid)? {
            None => return Ok(()),
            Some(state) if state.starts_with('Z') => return Ok(()),
            Some(_) => {}
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(LocalRuntimeError::TerminateDaemonTimeout(pid))
}

fn wait_for_owned_runtime_daemon_reaped(pid: u32) -> Result<(), LocalRuntimeError> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if reap_owned_child_if_exited(pid)? {
            return Ok(());
        }
        if process_state(pid)?.is_none() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(LocalRuntimeError::TerminateDaemonTimeout(pid))
}

fn reap_owned_child_if_exited(pid: u32) -> Result<bool, LocalRuntimeError> {
    loop {
        let mut status = 0;
        let result = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) };
        if result == pid as libc::pid_t {
            return Ok(true);
        }
        if result == 0 {
            return Ok(false);
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::ECHILD) | Some(libc::ESRCH) => return Ok(false),
            Some(libc::EINTR) => {}
            _ => return Err(LocalRuntimeError::InspectProcess(error)),
        }
    }
}

fn process_state(pid: u32) -> Result<Option<String>, LocalRuntimeError> {
    let output = Command::new("ps")
        .arg("-p")
        .arg(pid.to_string())
        .arg("-o")
        .arg("stat=")
        .output()
        .map_err(LocalRuntimeError::InspectProcess)?;
    if !output.status.success() {
        return Ok(None);
    }
    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if state.is_empty() {
        Ok(None)
    } else {
        Ok(Some(state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_runtime_cleanup_falls_back_to_direct_child_and_remains_bounded() {
        let mut child = Command::new("/bin/sleep")
            .arg("60")
            .spawn()
            .expect("spawn non-process-group-leader fixture");
        let pid = child.id();
        let started = Instant::now();

        let status =
            terminate_owned_runtime_child(&mut child).expect("terminate direct child fallback");

        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(!status.is_empty());
        assert!(
            child
                .try_wait()
                .expect("confirm fallback child was reaped")
                .is_some(),
            "cleanup must reap a child even when killpg reports ESRCH for its pid"
        );
        assert_eq!(unsafe { libc::kill(pid as libc::pid_t, 0) }, -1);
    }
}
