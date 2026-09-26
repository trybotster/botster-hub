//! Local runtime daemon process ownership.
//!
//! Owns start, reuse, metadata, PID validation, signal, reap, and stale-daemon
//! recovery. Package refresh, web launch, and operator-console composition stay
//! in `main`. WebRTC smoke lives in `local_webrtc_smoke`.

use std::io::{self, BufRead, BufReader};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use botster_hub::daemon::readiness::ReadyLine;
use botster_hub::process_exit::{
    ReapRegistration, ReapWatch, wait_for_child_group_exit, wait_for_pid_exit,
};
use botster_hub::{DaemonRequest, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, daemon_transport_request};

/// The descriptor that carries the daemon's `--ready-fd` pipe.
const DAEMON_READY_FD: libc::c_int = 3;
use serde::{Deserialize, Serialize};

use super::{
    LocalRuntimeDaemonOwnership, LocalRuntimeError, LocalRuntimeOptions, sanitize_runtime_message,
};

const LOCAL_RUNTIME_DAEMON_METADATA_FILE: &str = ".botster-hub-runtime-daemon.json";

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
        .arg(&session_worker_bin);
    if let Some(source_root) = &options.update_source_root {
        command.arg("--update-source-root").arg(source_root);
    }
    command
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
    let (mut child, ready_reader) =
        spawn_with_ready_fd(&mut command).map_err(|source| LocalRuntimeError::SpawnDaemon {
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
        &mut child,
        ready_reader,
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

/// Spawns `command` with `--ready-fd DAEMON_READY_FD` and the write end of a
/// fresh pipe at that descriptor. Returns the read end.
fn spawn_with_ready_fd(command: &mut Command) -> io::Result<(Child, io::PipeReader)> {
    let (ready_reader, ready_writer) = io::pipe()?;
    let ready_writer_fd = ready_writer.as_raw_fd();
    command.arg("--ready-fd").arg(DAEMON_READY_FD.to_string());
    unsafe {
        // SAFETY: this hook runs in the child after fork. It places the pipe's write end
        // at DAEMON_READY_FD without close-on-exec; fcntl and dup2 are async-signal-safe.
        command.pre_exec(move || {
            let placed = if ready_writer_fd == DAEMON_READY_FD {
                libc::fcntl(DAEMON_READY_FD, libc::F_SETFD, 0)
            } else {
                libc::dup2(ready_writer_fd, DAEMON_READY_FD)
            };
            if placed == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn();
    // Only the child may hold the write end, so its exit closes the pipe.
    drop(ready_writer);
    Ok((child?, ready_reader))
}

/// Waits for the daemon's ready line on the `--ready-fd` pipe. EOF without it
/// means the daemon is exiting; its exit status and stderr explain why.
fn wait_for_local_runtime_ready(
    child: &mut Child,
    ready_reader: io::PipeReader,
    readiness_budget: Duration,
    stderr_rx: &mpsc::Receiver<String>,
) -> Result<(), LocalRuntimeError> {
    let started_at = Instant::now();
    let deadline = started_at + readiness_budget;
    let (line_tx, line_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(ready_reader)
            .read_line(&mut line)
            .map(|_| line);
        let _ = line_tx.send(result);
    });
    // timer: deadline — the product readiness budget; the ready line or pipe EOF ends the normal wait.
    let outcome = line_rx.recv_timeout(readiness_budget);
    if let Ok(Ok(line)) = &outcome
        && !line.is_empty()
    {
        if ReadyLine::parse(line).is_some() {
            return Ok(());
        }
        let _ = terminate_owned_runtime_child(child);
        return Err(LocalRuntimeError::MalformedReadiness(
            sanitize_runtime_message(line),
        ));
    }
    if matches!(outcome, Err(mpsc::RecvTimeoutError::Timeout)) {
        return Err(readiness_timeout(child, started_at, readiness_budget));
    }
    // The pipe closed without a ready line, so the daemon is exiting.
    if !wait_for_pid_exit(child.id(), deadline).map_err(LocalRuntimeError::PollDaemon)? {
        return Err(readiness_timeout(child, started_at, readiness_budget));
    }
    // The exit event can precede the moment waitpid can reap the child (macOS
    // refuses a watch on an exiting process before it is a zombie), so reap
    // with a blocking wait; the child is already exiting.
    let status = child.wait().map_err(LocalRuntimeError::PollDaemon)?;
    Err(LocalRuntimeError::DaemonExited {
        status: status.to_string(),
        elapsed: started_at.elapsed(),
        readiness_budget,
        stderr_tail: collect_runtime_stderr(stderr_rx, deadline),
    })
}

fn readiness_timeout(
    child: &mut Child,
    started_at: Instant,
    readiness_budget: Duration,
) -> LocalRuntimeError {
    let child_pid = child.id();
    match terminate_owned_runtime_child(child) {
        Ok(child_status) => LocalRuntimeError::ReadinessTimeout {
            elapsed: started_at.elapsed(),
            readiness_budget,
            child_pid,
            child_status,
        },
        Err(error) => error,
    }
}

/// Collects the exited daemon's stderr until its reader reaches EOF. The
/// readiness deadline also bounds this collection, even while lines keep
/// arriving; the result then says that it was cut short.
fn collect_runtime_stderr(stderr_rx: &mpsc::Receiver<String>, deadline: Instant) -> String {
    let mut stderr_tail = String::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            append_runtime_stderr(
                &mut stderr_tail,
                "[stderr collection stopped at the readiness deadline]",
            );
            return stderr_tail;
        }
        // timer: deadline — the readiness budget also bounds diagnostics; expiry marks them truncated.
        match stderr_rx.recv_timeout(remaining) {
            Ok(line) => append_runtime_stderr(&mut stderr_tail, &line),
            Err(mpsc::RecvTimeoutError::Disconnected) => return stderr_tail,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}

fn append_runtime_stderr(stderr_tail: &mut String, line: &str) {
    if !stderr_tail.is_empty() {
        stderr_tail.push(' ');
    }
    stderr_tail.push_str(&sanitize_runtime_message(line));
    if stderr_tail.len() > 8_192 {
        let keep_from = stderr_tail.len() - 8_192;
        stderr_tail.drain(..keep_from);
    }
}

const fn local_runtime_daemon_readiness_budget() -> Duration {
    LOCAL_RUNTIME_DAEMON_READINESS_BUDGET
}

fn terminate_owned_runtime_child(child: &mut Child) -> Result<String, LocalRuntimeError> {
    let pid = child.id();
    signal_owned_runtime_child(pid, libc::SIGTERM).map_err(LocalRuntimeError::TerminateDaemon)?;
    // timer: deadline — SIGTERM grace for the runtime group; expiry escalates to SIGKILL.
    if let Some(status) =
        wait_for_child_group_exit(child, Instant::now() + Duration::from_millis(500))
            .map_err(LocalRuntimeError::PollDaemon)?
    {
        return Ok(status.to_string());
    }
    signal_owned_runtime_child(pid, libc::SIGKILL).map_err(LocalRuntimeError::TerminateDaemon)?;
    // timer: deadline — SIGKILL cleanup bound; expiry reports TerminateDaemonTimeout.
    if let Some(status) = wait_for_child_group_exit(child, Instant::now() + Duration::from_secs(2))
        .map_err(LocalRuntimeError::PollDaemon)?
    {
        return Ok(status.to_string());
    }
    Err(LocalRuntimeError::TerminateDaemonTimeout(pid))
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

/// A runtime daemon that this data directory's metadata owns, with a reap
/// watch registered while it still runs.
pub(crate) struct OwnedRuntimeDaemon {
    pid: u32,
    reap: ReapRegistration,
}

impl OwnedRuntimeDaemon {
    pub(crate) fn pid(&self) -> u32 {
        self.pid
    }
}

/// Identifies the owned runtime daemon and registers its reap watch. Call it
/// before requesting shutdown: macOS cannot watch a process that has already
/// exited.
pub(crate) fn owned_runtime_daemon(
    data_directory: &Path,
    config: &botster_hub::HubConfig,
) -> Result<Option<OwnedRuntimeDaemon>, LocalRuntimeError> {
    let Some(pid) = owned_runtime_daemon_pid(data_directory, config)? else {
        return Ok(None);
    };
    let reap = ReapWatch::register(pid).map_err(LocalRuntimeError::InspectProcess)?;
    Ok(Some(OwnedRuntimeDaemon { pid, reap }))
}

fn owned_runtime_daemon_pid(
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
    owned_daemon: Option<OwnedRuntimeDaemon>,
) -> Result<(), LocalRuntimeError> {
    let Some(owned_daemon) = owned_daemon else {
        return Ok(());
    };
    wait_for_owned_runtime_daemon_reaped(owned_daemon)?;
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
    // timer: deadline — bound on the daemon's exit after SIGTERM; expiry reports TerminateDaemonTimeout.
    if wait_for_pid_exit(pid, Instant::now() + Duration::from_secs(10))
        .map_err(LocalRuntimeError::InspectProcess)?
    {
        return Ok(());
    }
    Err(LocalRuntimeError::TerminateDaemonTimeout(pid))
}

/// Waits until the owned daemon is reaped. Its parent reaps it: the
/// launcher's reaper thread, or the process that adopted it.
fn wait_for_owned_runtime_daemon_reaped(
    daemon: OwnedRuntimeDaemon,
) -> Result<(), LocalRuntimeError> {
    let mut watch = match daemon.reap {
        ReapRegistration::Watching(watch) => watch,
        ReapRegistration::AlreadyReaped => return Ok(()),
        ReapRegistration::ExitedBeforeRegistration => {
            return Err(LocalRuntimeError::InspectProcess(io::Error::other(
                format!(
                    "runtime daemon {} exited before its reap watch was registered and awaits its parent",
                    daemon.pid
                ),
            )));
        }
    };
    // timer: deadline — bound on the daemon's reap after shutdown; expiry reports TerminateDaemonTimeout.
    if watch
        .wait(Instant::now() + Duration::from_secs(10))
        .map_err(LocalRuntimeError::InspectProcess)?
    {
        return Ok(());
    }
    Err(LocalRuntimeError::TerminateDaemonTimeout(daemon.pid))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spawns `script` the way the launcher spawns the daemon: a ready pipe at
    /// DAEMON_READY_FD, stdin held open by the test, and stderr collected.
    fn spawn_ready_fixture(script: &str) -> (Child, io::PipeReader, mpsc::Receiver<String>) {
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let (mut child, ready_reader) = spawn_with_ready_fd(&mut command).expect("spawn fixture");
        let (stderr_tx, stderr_rx) = mpsc::channel();
        let stderr = child.stderr.take().expect("fixture stderr");
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let _ = stderr_tx.send(line);
            }
        });
        (child, ready_reader, stderr_rx)
    }

    #[test]
    fn ready_line_on_the_ready_fd_ends_the_readiness_wait() {
        // The fixture writes the ready line, then blocks on stdin until the test closes it.
        let (mut child, ready, stderr) =
            spawn_ready_fixture("printf 'ready 10 dev\\n' >&3; exec cat >/dev/null");

        wait_for_local_runtime_ready(&mut child, ready, Duration::from_secs(10), &stderr)
            .expect("the ready line ends the wait");

        drop(child.stdin.take());
        assert!(child.wait().expect("reap fixture").success());
    }

    #[test]
    fn pipe_eof_without_a_ready_line_reports_the_exit_and_stderr() {
        let (mut child, ready, stderr) = spawn_ready_fixture("echo boom >&2; exit 3");

        let error =
            wait_for_local_runtime_ready(&mut child, ready, Duration::from_secs(10), &stderr)
                .expect_err("exit without a ready line fails");

        let LocalRuntimeError::DaemonExited {
            status,
            stderr_tail,
            ..
        } = error
        else {
            panic!("expected DaemonExited, got {error}");
        };
        assert!(status.contains('3'), "{status}");
        assert_eq!(stderr_tail, "boom");
    }

    #[test]
    fn stderr_collection_stops_at_the_deadline_while_lines_keep_arriving() {
        let (stderr_tx, stderr_rx) = mpsc::channel();
        for _ in 0..4 {
            stderr_tx.send("noise".to_string()).unwrap();
        }
        // The sender stays alive and the channel stays nonempty.
        let tail = collect_runtime_stderr(&stderr_rx, Instant::now());

        assert_eq!(
            tail,
            "[stderr collection stopped at the readiness deadline]"
        );
        assert_eq!(stderr_rx.try_iter().count(), 4);
        drop(stderr_tx);
    }

    #[test]
    fn a_malformed_ready_line_fails_and_terminates_the_child() {
        let (mut child, ready, stderr) =
            spawn_ready_fixture("printf 'ready soon\\n' >&3; exec cat >/dev/null");

        let error =
            wait_for_local_runtime_ready(&mut child, ready, Duration::from_secs(10), &stderr)
                .expect_err("a malformed record fails");

        assert!(
            matches!(&error, LocalRuntimeError::MalformedReadiness(line) if line.contains("ready soon")),
            "{error}"
        );
        assert!(child.try_wait().expect("inspect fixture").is_some());
    }

    #[test]
    fn a_silent_child_times_out_and_is_terminated() {
        // The fixture holds the ready pipe open without writing to it.
        let (mut child, ready, stderr) = spawn_ready_fixture("exec cat >/dev/null");
        let pid = child.id();

        let error =
            wait_for_local_runtime_ready(&mut child, ready, Duration::from_millis(200), &stderr)
                .expect_err("silence past the budget fails");

        assert!(
            matches!(error, LocalRuntimeError::ReadinessTimeout { child_pid, .. } if child_pid == pid),
            "{error}"
        );
        assert!(child.try_wait().expect("inspect fixture").is_some());
    }

    #[test]
    fn owned_runtime_cleanup_falls_back_to_direct_child_and_remains_bounded() {
        // The fixture blocks on its open stdin pipe until cleanup signals it.
        let mut child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
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
