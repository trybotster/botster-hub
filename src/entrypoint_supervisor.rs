//! Hub-owned local package entrypoint process supervision.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::io::{BufReader, Read};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::process_exit::{process_group_running, wait_for_child_group_exit};
use crate::{PackageRecord, PackageRegistry, PackageRunnableEntrypoint, PackageState};
use botster_core::{
    PackageSource, RunnableEntrypointLaunchMode, RunnableEntrypointLaunchResult,
    RunnableEntrypointProcessState, RunnableEntrypointResultField,
};

const OUTPUT_LIMIT_BYTES: usize = 4096;
const OUTPUT_FINALIZATION_GRACE: Duration = Duration::from_millis(500);
const STOP_GRACE: Duration = Duration::from_millis(500);
const LAUNCH_RESULT_READINESS_BUDGET: Duration = Duration::from_secs(15);
/// Names the inherited descriptor that carries the entrypoint's launch record.
const READY_FD_ENV: &str = "BOTSTER_ENTRYPOINT_READY_FD";
/// The descriptor number the launch record pipe has in the entrypoint.
const READY_FD: libc::c_int = 3;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntrypointKey {
    pub package_name: String,
    pub entrypoint_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntrypointProcessSnapshot {
    pub package_name: String,
    pub entrypoint_id: String,
    pub state: String,
    pub pid: Option<u32>,
    pub started_at: Option<u64>,
    pub exited_at: Option<u64>,
    pub exit_status: Option<String>,
    pub diagnostics: Vec<EntrypointDiagnostic>,
    pub launch_result: Option<RunnableEntrypointLaunchResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntrypointDiagnostic {
    pub kind: String,
    pub message: String,
}

#[derive(Debug)]
pub enum EntrypointSupervisorError {
    PackageNotInstalled(String),
    PackageDisabled(String),
    PackageNotLocal(String),
    EntrypointNotFound {
        package_name: String,
        entrypoint_id: String,
    },
    EntrypointNotSupervisable {
        package_name: String,
        entrypoint_id: String,
    },
    ReadinessFailed {
        package_name: String,
        entrypoint_id: String,
        details: String,
    },
    ReadinessTimeout {
        package_name: String,
        entrypoint_id: String,
        details: String,
    },
    LaunchContract {
        package_name: String,
        entrypoint_id: String,
        details: String,
    },
    Watch(String),
    Io(std::io::Error),
}

pub type EntrypointSupervisorResult<T> = Result<T, EntrypointSupervisorError>;

#[derive(Default)]
pub struct EntrypointSupervisor {
    processes: BTreeMap<EntrypointKey, SupervisedProcess>,
    retained: BTreeMap<EntrypointKey, EntrypointProcessSnapshot>,
}

impl EntrypointSupervisor {
    pub fn start(
        &mut self,
        registry: &PackageRegistry,
        package_name: &str,
        entrypoint_id: &str,
        arguments: &[String],
        environment_overrides: &BTreeMap<String, String>,
    ) -> EntrypointSupervisorResult<EntrypointProcessSnapshot> {
        self.refresh();
        let (record, entrypoint, package_root) =
            find_supervisable_entrypoint(registry, package_name, entrypoint_id)?;
        let key = EntrypointKey {
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
        };
        if let Some(process) = self.processes.get(&key)
            && process.is_running()
        {
            return Ok(process.snapshot(&key));
        }

        let declares_launch_result = entrypoint_declares_launch_result(entrypoint);
        let command_path = resolve_command(&package_root, entrypoint.command.as_str());
        let working_directory = resolve_working_directory(&package_root, entrypoint)?;
        let mut command = Command::new(command_path);
        command.args(arguments);
        command.current_dir(working_directory);
        for (name, value) in environment_overrides {
            command.env(name, value);
        }
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let spawned = if declares_launch_result {
            spawn_with_launch_record(&mut command).map(|(child, reader)| (child, Some(reader)))
        } else {
            command.spawn().map(|child| (child, None))
        };
        let (mut child, record_reader) = match spawned {
            Ok(spawned) => spawned,
            Err(error) => {
                let snapshot = failed_snapshot(package_name, entrypoint_id, error);
                self.retained.insert(key, snapshot.clone());
                return Ok(snapshot);
            }
        };
        let readiness_deadline = Instant::now() + LAUNCH_RESULT_READINESS_BUDGET;
        let stdout = child.stdout.take().map(spawn_reader);
        let stderr = child.stderr.take().map(spawn_reader);
        let process = SupervisedProcess {
            child,
            environment: environment_overrides.clone(),
            started_at: now_seconds(),
            exited_at: None,
            exit_status: None,
            stdout,
            stderr,
            diagnostics: Vec::new(),
            launch_result: Some(RunnableEntrypointLaunchResult {
                entrypoint_id: entrypoint_id.to_string(),
                process_state: RunnableEntrypointProcessState::Running,
                local_url: None,
            }),
            state: ProcessState::Running,
            pending_terminal_state: None,
            output_finalization_deadline: None,
        };
        let snapshot = process.snapshot(&key);
        self.retained.insert(key.clone(), snapshot.clone());
        self.processes.insert(key.clone(), process);
        let _ = record;
        match record_reader {
            Some(reader) => self.wait_for_launch_record(&key, reader, readiness_deadline),
            None => Ok(snapshot),
        }
    }

    pub fn stop(&mut self, package_name: &str, entrypoint_id: &str) -> EntrypointProcessSnapshot {
        let key = EntrypointKey {
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
        };
        if let Some(process) = self.processes.get_mut(&key) {
            process.stop();
            let snapshot = process.snapshot(&key);
            self.retained.insert(key, snapshot.clone());
            return snapshot;
        }
        let snapshot = stopped_snapshot(package_name, entrypoint_id);
        self.retained.insert(key, snapshot.clone());
        snapshot
    }

    pub fn restart(
        &mut self,
        registry: &PackageRegistry,
        package_name: &str,
        entrypoint_id: &str,
        arguments: &[String],
        environment_overrides: &BTreeMap<String, String>,
    ) -> EntrypointSupervisorResult<EntrypointProcessSnapshot> {
        let _ = self.stop(package_name, entrypoint_id);
        self.start(
            registry,
            package_name,
            entrypoint_id,
            arguments,
            environment_overrides,
        )
    }

    pub fn launch_environment(
        &self,
        package_name: &str,
        entrypoint_id: &str,
    ) -> BTreeMap<String, String> {
        let key = EntrypointKey {
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
        };
        self.processes
            .get(&key)
            .map(|process| process.environment.clone())
            .unwrap_or_default()
    }

    pub fn status(&mut self, package_name: &str, entrypoint_id: &str) -> EntrypointProcessSnapshot {
        self.refresh();
        let key = EntrypointKey {
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
        };
        self.processes
            .get(&key)
            .map(|process| process.snapshot(&key))
            .or_else(|| self.retained.get(&key).cloned())
            .unwrap_or_else(|| stopped_snapshot(package_name, entrypoint_id))
    }

    pub fn snapshots(&mut self) -> Vec<EntrypointProcessSnapshot> {
        self.refresh();
        let mut snapshots = self.retained.clone();
        for (key, process) in &self.processes {
            snapshots.insert(key.clone(), process.snapshot(key));
        }
        snapshots.into_values().collect()
    }

    pub fn stop_package(&mut self, package_name: &str) {
        for (key, process) in &mut self.processes {
            if key.package_name == package_name {
                process.stop();
            }
        }
    }

    pub fn stop_all(&mut self) {
        for process in self.processes.values_mut() {
            process.stop();
        }
    }

    fn refresh(&mut self) {
        for process in self.processes.values_mut() {
            process.refresh();
        }
    }

    /// Waits for the entrypoint's one launch record. Readiness is decided from
    /// the record channel alone: a record written before the child exited is
    /// still in the pipe, so its exit timing cannot discard it, and EOF means
    /// no record will come. Every failure stops the process group and then
    /// collects its output.
    fn wait_for_launch_record(
        &mut self,
        key: &EntrypointKey,
        reader: LaunchRecordReader,
        deadline: Instant,
    ) -> EntrypointSupervisorResult<EntrypointProcessSnapshot> {
        let process = self
            .processes
            .get_mut(key)
            .expect("entrypoint was inserted before readiness wait");
        let remaining = deadline.saturating_duration_since(Instant::now());
        // Expiry is checked before the receive, so a record queued after the
        // deadline does not count.
        let outcome = if remaining.is_zero() {
            Err(mpsc::RecvTimeoutError::Timeout)
        } else {
            // timer: deadline — the launch-record readiness budget; the record or pipe EOF ends the normal wait.
            reader.events.recv_timeout(remaining)
        };
        // Dropping the reader cancels its thread on every path below.
        drop(reader);
        let failure = match outcome {
            Ok(LaunchRecordEvent::Record(line)) => match parse_launch_record(&line, key) {
                Ok(result) => {
                    let ready = result
                        .local_url
                        .as_deref()
                        .is_some_and(|url| !url.trim().is_empty());
                    process.launch_result = Some(result);
                    // The child supplies result fields; process state comes from reaping.
                    process.sync_launch_result_process_state();
                    process.refresh();
                    let snapshot = process.snapshot(key);
                    self.retained.insert(key.clone(), snapshot.clone());
                    if ready {
                        return Ok(snapshot);
                    }
                    "the launch record has no local_url".to_string()
                }
                Err(reason) => reason,
            },
            Ok(LaunchRecordEvent::Eof) => {
                "the launch-record pipe closed without a record".to_string()
            }
            Ok(LaunchRecordEvent::ReadError(error)) => {
                format!("reading the launch record failed: {error}")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                "the launch-record reader stopped without a record".to_string()
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                process.refresh();
                let snapshot = process.snapshot(key);
                process.stop();
                process.collect_output_after_stop();
                self.retained.insert(key.clone(), process.snapshot(key));
                return Err(EntrypointSupervisorError::ReadinessTimeout {
                    package_name: key.package_name.clone(),
                    entrypoint_id: key.entrypoint_id.clone(),
                    details: readiness_details(&snapshot),
                });
            }
        };
        process.stop();
        process.collect_output_after_stop();
        let snapshot = process.snapshot(key);
        self.retained.insert(key.clone(), snapshot.clone());
        Err(EntrypointSupervisorError::ReadinessFailed {
            package_name: key.package_name.clone(),
            entrypoint_id: key.entrypoint_id.clone(),
            details: format!("{failure}; {}", readiness_details(&snapshot)),
        })
    }
}

struct SupervisedProcess {
    child: Child,
    environment: BTreeMap<String, String>,
    started_at: u64,
    exited_at: Option<u64>,
    exit_status: Option<String>,
    stdout: Option<Receiver<Vec<u8>>>,
    stderr: Option<Receiver<Vec<u8>>>,
    diagnostics: Vec<EntrypointDiagnostic>,
    launch_result: Option<RunnableEntrypointLaunchResult>,
    state: ProcessState,
    pending_terminal_state: Option<ProcessState>,
    output_finalization_deadline: Option<Instant>,
}

impl SupervisedProcess {
    fn is_running(&self) -> bool {
        self.exited_at.is_none()
    }

    fn refresh(&mut self) {
        self.refresh_process_state();
    }

    fn refresh_process_state(&mut self) {
        drain_output("stdout", &mut self.stdout, &mut self.diagnostics);
        drain_output("stderr", &mut self.stderr, &mut self.diagnostics);
        if self.exited_at.is_none() {
            match self.child.try_wait() {
                Ok(Some(status)) => self.mark_exit(status),
                Ok(None) => {}
                Err(error) => {
                    self.exited_at = Some(now_seconds());
                    self.begin_terminal_finalization(ProcessState::Failed);
                    self.diagnostics.push(EntrypointDiagnostic {
                        kind: "wait_error".to_string(),
                        message: bounded_message(error.to_string()),
                    });
                }
            }
        }
        self.publish_terminal_state_if_ready();
    }

    /// After stop, blocks on the output readers until both finish or
    /// OUTPUT_FINALIZATION_GRACE passes, so a failure snapshot carries the
    /// child's last output.
    fn collect_output_after_stop(&mut self) {
        let deadline = Instant::now() + OUTPUT_FINALIZATION_GRACE;
        for (kind, reader) in [("stdout", &mut self.stdout), ("stderr", &mut self.stderr)] {
            let Some(receiver) = reader else {
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            // timer: deadline — OUTPUT_FINALIZATION_GRACE; reader EOF ends the normal wait and expiry reports without the rest.
            match receiver.recv_timeout(remaining) {
                Ok(bytes) => {
                    push_output_diagnostic(kind, &bytes, &mut self.diagnostics);
                    *reader = None;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => *reader = None,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
    }

    /// Blocks on the output readers until both finish or the output
    /// finalization deadline passes, then publishes the terminal state.
    fn await_output(&mut self) {
        let Some(deadline) = self.output_finalization_deadline else {
            return;
        };
        for (kind, reader) in [("stdout", &mut self.stdout), ("stderr", &mut self.stderr)] {
            let Some(receiver) = reader else {
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            // timer: deadline — OUTPUT_FINALIZATION_GRACE; reader EOF ends the normal wait and expiry publishes without the rest.
            match receiver.recv_timeout(remaining) {
                Ok(bytes) => {
                    push_output_diagnostic(kind, &bytes, &mut self.diagnostics);
                    *reader = None;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => *reader = None,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
        // Both readers finished, or the deadline passed; either publishes.
        self.publish_terminal_state_if_ready();
    }

    fn stop(&mut self) {
        self.refresh();
        let pid = self.child.id();
        if self.exited_at.is_some() && !self.group_running(pid) {
            self.mark_stopped();
            return;
        }
        if let Err(error) = signal_process_group_or_child(pid, libc::SIGTERM) {
            self.diagnostics.push(EntrypointDiagnostic {
                kind: "cleanup_signal".to_string(),
                message: bounded_message(error.to_string()),
            });
            return;
        }
        // timer: deadline — SIGTERM grace for the entrypoint group; expiry escalates to SIGKILL.
        if self.wait_for_group_exit(Instant::now() + STOP_GRACE) {
            self.mark_stopped();
            return;
        }
        if let Err(error) = signal_process_group_or_child(pid, libc::SIGKILL) {
            self.diagnostics.push(EntrypointDiagnostic {
                kind: "cleanup_signal".to_string(),
                message: bounded_message(error.to_string()),
            });
            return;
        }
        // timer: deadline — SIGKILL cleanup bound; expiry records cleanup_timeout.
        if self.wait_for_group_exit(Instant::now() + Duration::from_secs(2)) {
            self.mark_stopped();
            return;
        }
        self.diagnostics.push(EntrypointDiagnostic {
            kind: "cleanup_timeout".to_string(),
            message: format!(
                "process {} did not exit within the bounded cleanup deadline",
                pid
            ),
        });
        self.mark_stopped();
    }

    /// Blocks on the child's and its group's exit events until `deadline`.
    fn wait_for_group_exit(&mut self, deadline: Instant) -> bool {
        let exited = match wait_for_child_group_exit(&mut self.child, deadline) {
            Ok(status) => status.is_some(),
            Err(error) => {
                self.diagnostics.push(EntrypointDiagnostic {
                    kind: "wait_error".to_string(),
                    message: bounded_message(error.to_string()),
                });
                false
            }
        };
        // The child is reaped once its exit event fires, so this records the exit.
        self.refresh();
        exited
    }

    fn group_running(&mut self, pgid: u32) -> bool {
        process_group_running(pgid).unwrap_or_else(|error| {
            self.diagnostics.push(EntrypointDiagnostic {
                kind: "wait_error".to_string(),
                message: bounded_message(error.to_string()),
            });
            true
        })
    }

    fn mark_exit(&mut self, status: ExitStatus) {
        self.exited_at = Some(now_seconds());
        self.exit_status = Some(exit_status_label(status));
        let state = if status.success() {
            ProcessState::Exited
        } else {
            ProcessState::Failed
        };
        self.begin_terminal_finalization(state);
    }

    fn begin_terminal_finalization(&mut self, state: ProcessState) {
        self.pending_terminal_state = Some(state);
        self.output_finalization_deadline = Some(Instant::now() + OUTPUT_FINALIZATION_GRACE);
    }

    fn publish_terminal_state_if_ready(&mut self) {
        let Some(state) = self.pending_terminal_state else {
            return;
        };
        let readers_complete = self.stdout.is_none() && self.stderr.is_none();
        let deadline_expired = self
            .output_finalization_deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        if !readers_complete && !deadline_expired {
            return;
        }

        self.pending_terminal_state = None;
        self.output_finalization_deadline = None;
        self.state = state;
        self.sync_launch_result_process_state();
    }

    fn sync_launch_result_process_state(&mut self) {
        if let Some(result) = &mut self.launch_result {
            result.process_state = match self.state {
                ProcessState::Running => RunnableEntrypointProcessState::Running,
                ProcessState::Exited => RunnableEntrypointProcessState::Exited,
                ProcessState::Failed => RunnableEntrypointProcessState::Failed,
                ProcessState::Stopped => return,
            };
        }
    }

    fn mark_stopped(&mut self) {
        if let Some(state) = self.pending_terminal_state.take() {
            self.state = state;
            self.sync_launch_result_process_state();
        }
        self.output_finalization_deadline = None;
        self.state = ProcessState::Stopped;
    }

    fn snapshot(&self, key: &EntrypointKey) -> EntrypointProcessSnapshot {
        EntrypointProcessSnapshot {
            package_name: key.package_name.clone(),
            entrypoint_id: key.entrypoint_id.clone(),
            state: self.state.label().to_string(),
            pid: self.exited_at.is_none().then_some(self.child.id()),
            started_at: Some(self.started_at),
            exited_at: self.exited_at,
            exit_status: self.exit_status.clone(),
            diagnostics: self.diagnostics.clone(),
            launch_result: self.launch_result.clone(),
        }
    }
}

impl Drop for SupervisedProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Drop for EntrypointSupervisor {
    fn drop(&mut self) {
        self.stop_all();
    }
}

#[derive(Clone, Copy)]
enum ProcessState {
    Running,
    Exited,
    Failed,
    Stopped,
}

impl ProcessState {
    const fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Exited => "exited",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }
}

fn find_supervisable_entrypoint<'a>(
    registry: &'a PackageRegistry,
    package_name: &str,
    entrypoint_id: &str,
) -> EntrypointSupervisorResult<(&'a PackageRecord, &'a PackageRunnableEntrypoint, PathBuf)> {
    let record = registry
        .package(package_name)
        .ok_or_else(|| EntrypointSupervisorError::PackageNotInstalled(package_name.to_string()))?;
    if !matches!(record.state, PackageState::Enabled) {
        return Err(EntrypointSupervisorError::PackageDisabled(
            package_name.to_string(),
        ));
    }
    let Some(entrypoint) = record
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id)
    else {
        return Err(EntrypointSupervisorError::EntrypointNotFound {
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
        });
    };
    if !entrypoint.may_supervise
        || !matches!(
            entrypoint.launch_mode,
            RunnableEntrypointLaunchMode::Background
        )
    {
        return Err(EntrypointSupervisorError::EntrypointNotSupervisable {
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
        });
    }
    let package_root = match &record.manifest.source {
        Some(PackageSource::Path { path }) => PathBuf::from(path),
        _ => {
            return Err(EntrypointSupervisorError::PackageNotLocal(
                package_name.to_string(),
            ));
        }
    };
    Ok((record, entrypoint, package_root))
}

fn resolve_command(package_root: &Path, command: &str) -> PathBuf {
    let command_path = PathBuf::from(command);
    if command_path.components().count() > 1 {
        package_root.join(command_path)
    } else {
        command_path
    }
}

fn resolve_working_directory(
    package_root: &Path,
    entrypoint: &PackageRunnableEntrypoint,
) -> EntrypointSupervisorResult<PathBuf> {
    match &entrypoint.working_directory {
        crate::PackageRunnableWorkingDirectory::PackageRoot => Ok(package_root.to_path_buf()),
        crate::PackageRunnableWorkingDirectory::EntrypointDir => {
            Ok(resolve_command(package_root, entrypoint.command.as_str())
                .parent()
                .unwrap_or(package_root)
                .to_path_buf())
        }
        crate::PackageRunnableWorkingDirectory::Relative { path } => Ok(package_root.join(path)),
    }
}

fn entrypoint_declares_launch_result(entrypoint: &PackageRunnableEntrypoint) -> bool {
    entrypoint.readiness.as_ref().is_some_and(|readiness| {
        readiness
            .result_fields
            .iter()
            .any(|field| matches!(field, RunnableEntrypointResultField::LocalUrl))
    })
}

/// What the launch-record reader reports. It sends exactly one event.
#[derive(Debug)]
enum LaunchRecordEvent {
    /// The first line, without its newline.
    Record(Vec<u8>),
    /// The pipe closed before a complete line.
    Eof,
    ReadError(String),
}

/// The Hub side of the launch-record pipe. Dropping it cancels the reader
/// thread, even when a descendant still holds the pipe's write end.
struct LaunchRecordReader {
    events: Receiver<LaunchRecordEvent>,
    _cancel: std::io::PipeWriter,
    #[cfg(test)]
    retired: Receiver<()>,
}

#[cfg(test)]
impl LaunchRecordReader {
    /// Takes the signal the reader thread sends when it exits.
    fn take_retired(&mut self) -> Receiver<()> {
        std::mem::replace(&mut self.retired, mpsc::sync_channel(1).1)
    }
}

/// Spawns `command` with a launch-record pipe at READY_FD and
/// READY_FD_ENV naming it, and starts the reader thread.
fn spawn_with_launch_record(command: &mut Command) -> std::io::Result<(Child, LaunchRecordReader)> {
    use std::os::fd::AsRawFd;

    let (record_reader, record_writer) = std::io::pipe()?;
    // Every fallible reader resource exists before the child does, so a setup
    // failure can never drop a spawned child without stopping and reaping it.
    let cancel = launch_record_cancel_pipe()?;
    let writer_fd = record_writer.as_raw_fd();
    command.env(READY_FD_ENV, READY_FD.to_string());
    unsafe {
        // SAFETY: this hook runs in the child after fork; fcntl and dup2 are
        // async-signal-safe. The write end lands at READY_FD without close-on-exec.
        command.pre_exec(move || place_ready_fd(writer_fd));
    }
    let child = command.spawn();
    // Only the child may hold the write end, so its exit can close the pipe.
    drop(record_writer);
    let child = child?;
    Ok((child, start_reader_with_cancel(record_reader, cancel)))
}

#[cfg(test)]
thread_local! {
    /// Test-only: makes the next cancel-pipe creation on this thread fail.
    static FAIL_CANCEL_PIPE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn launch_record_cancel_pipe() -> std::io::Result<(std::io::PipeReader, std::io::PipeWriter)> {
    #[cfg(test)]
    if FAIL_CANCEL_PIPE.with(|fail| fail.replace(false)) {
        return Err(std::io::Error::other("test-injected cancel pipe failure"));
    }
    std::io::pipe()
}

/// Places `writer_fd` at READY_FD without close-on-exec. Runs in the child
/// after fork, so it calls only async-signal-safe functions. When the write
/// end already is READY_FD, dup2 would keep close-on-exec, so the flag is
/// cleared instead.
fn place_ready_fd(writer_fd: libc::c_int) -> std::io::Result<()> {
    // SAFETY: fcntl and dup2 on descriptors this process owns.
    let placed = unsafe {
        if writer_fd == READY_FD {
            libc::fcntl(READY_FD, libc::F_SETFD, 0)
        } else {
            libc::dup2(writer_fd, READY_FD)
        }
    };
    if placed == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
fn start_launch_record_reader(record: std::io::PipeReader) -> std::io::Result<LaunchRecordReader> {
    Ok(start_reader_with_cancel(record, std::io::pipe()?))
}

/// Starts the reader thread. Infallible: the pipes already exist.
fn start_reader_with_cancel(
    record: std::io::PipeReader,
    (cancel_reader, cancel_writer): (std::io::PipeReader, std::io::PipeWriter),
) -> LaunchRecordReader {
    let (events_tx, events) = mpsc::sync_channel(1);
    #[cfg(test)]
    let (retired_tx, retired) = mpsc::sync_channel(1);
    thread::spawn(move || {
        if let Some(event) = read_launch_record(
            record,
            cancel_reader,
            #[cfg(test)]
            None,
        ) {
            let _ = events_tx.send(event);
        }
        #[cfg(test)]
        let _ = retired_tx.send(());
    });
    LaunchRecordReader {
        events,
        _cancel: cancel_writer,
        #[cfg(test)]
        retired,
    }
}

/// Reads until the first newline or EOF. It waits on poll() for either the
/// record pipe or the cancel pipe, and each read is a finite nonblocking read,
/// so a partial record never blocks cancellation. The record has no size
/// bound, the same as the file-based reader it replaces. Returns None when
/// cancelled.
fn read_launch_record(
    mut record: std::io::PipeReader,
    cancel: std::io::PipeReader,
    #[cfg(test)] mut partial: Option<Box<dyn FnMut(usize) + Send>>,
) -> Option<LaunchRecordEvent> {
    use std::os::fd::AsRawFd;

    let record_fd = record.as_raw_fd();
    let flags = unsafe { libc::fcntl(record_fd, libc::F_GETFL) };
    if flags == -1
        || unsafe { libc::fcntl(record_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
    {
        return Some(LaunchRecordEvent::ReadError(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    let mut line = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let mut polls = [
            libc::pollfd {
                fd: record_fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: cancel.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // No timeout: the record, EOF, or cancellation ends this wait.
        if unsafe { libc::poll(polls.as_mut_ptr(), 2, -1) } == -1 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Some(LaunchRecordEvent::ReadError(error.to_string()));
        }
        if polls[1].revents != 0 {
            return None;
        }
        // One finite read per poll: the next poll rechecks cancellation, so a
        // producer that never stops writing cannot hold the reader past it.
        match record.read(&mut chunk) {
            Ok(0) => return Some(LaunchRecordEvent::Eof),
            Ok(read) => {
                let start = line.len();
                line.extend_from_slice(&chunk[..read]);
                if let Some(end) = line[start..].iter().position(|byte| *byte == b'\n') {
                    line.truncate(start + end);
                    return Some(LaunchRecordEvent::Record(line));
                }
                #[cfg(test)]
                if let Some(partial) = partial.as_mut() {
                    partial(line.len());
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) => {}
            Err(error) => return Some(LaunchRecordEvent::ReadError(error.to_string())),
        }
    }
}

/// Parses one launch record: a Core RunnableEntrypointLaunchResult for this
/// entrypoint.
fn parse_launch_record(
    line: &[u8],
    key: &EntrypointKey,
) -> Result<RunnableEntrypointLaunchResult, String> {
    let result = serde_json::from_slice::<RunnableEntrypointLaunchResult>(line)
        .map_err(|error| format!("the launch record is malformed: {error}"))?;
    if result.entrypoint_id != key.entrypoint_id {
        return Err(format!(
            "the launch record names entrypoint {:?}, not {:?}",
            result.entrypoint_id, key.entrypoint_id
        ));
    }
    Ok(result)
}

fn readiness_details(snapshot: &EntrypointProcessSnapshot) -> String {
    let mut details = format!("process_state={}", snapshot.state);
    if let Some(exit_status) = &snapshot.exit_status {
        details.push_str(&format!(" exit_status={exit_status}"));
    }
    for diagnostic in snapshot.diagnostics.iter().take(4) {
        details.push_str(&format!(
            "; {}={}",
            diagnostic.kind,
            bounded_message(diagnostic.message.clone())
        ));
    }
    details
}

fn spawn_reader(pipe: impl Read + Send + 'static) -> Receiver<Vec<u8>> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(pipe).take(OUTPUT_LIMIT_BYTES as u64);
        let mut buffer = Vec::new();
        let _ = reader.read_to_end(&mut buffer);
        let _ = tx.send(buffer);
    });
    rx
}

fn push_output_diagnostic(kind: &str, bytes: &[u8], diagnostics: &mut Vec<EntrypointDiagnostic>) {
    if !bytes.is_empty() {
        diagnostics.push(EntrypointDiagnostic {
            kind: kind.to_string(),
            message: bounded_message(String::from_utf8_lossy(bytes).to_string()),
        });
    }
}

fn drain_output(
    kind: &str,
    rx: &mut Option<Receiver<Vec<u8>>>,
    diagnostics: &mut Vec<EntrypointDiagnostic>,
) {
    let Some(receiver) = rx else {
        return;
    };
    match receiver.try_recv() {
        Ok(bytes) => {
            push_output_diagnostic(kind, &bytes, diagnostics);
            *rx = None;
        }
        Err(mpsc::TryRecvError::Empty) => {}
        Err(mpsc::TryRecvError::Disconnected) => {
            *rx = None;
        }
    }
}

fn signal_process_group_or_child(pid: u32, signal: libc::c_int) -> std::io::Result<()> {
    if unsafe { libc::killpg(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let group_error = std::io::Error::last_os_error();
    if group_error.raw_os_error() != Some(libc::ESRCH) {
        return Err(group_error);
    }
    if unsafe { libc::kill(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let child_error = std::io::Error::last_os_error();
    if child_error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(child_error)
    }
}

fn exit_status_label(status: ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit:{code}");
    }
    signal_number(status)
        .map(|signal| format!("signal:{signal}"))
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(unix)]
fn signal_number(status: ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

fn bounded_message(message: String) -> String {
    let mut value: String = message.chars().take(OUTPUT_LIMIT_BYTES).collect();
    for needle in [
        std::env::var_os("HOME"),
        std::env::current_dir().ok().map(|p| p.into_os_string()),
    ]
    .into_iter()
    .flatten()
    {
        let needle = os_string_lossy(&needle);
        if !needle.is_empty() {
            value = value.replace(&needle, "<redacted-path>");
        }
    }
    value
}

fn os_string_lossy(value: &OsStr) -> String {
    value.to_string_lossy().into_owned()
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn stopped_snapshot(package_name: &str, entrypoint_id: &str) -> EntrypointProcessSnapshot {
    EntrypointProcessSnapshot {
        package_name: package_name.to_string(),
        entrypoint_id: entrypoint_id.to_string(),
        state: "stopped".to_string(),
        pid: None,
        started_at: None,
        exited_at: None,
        exit_status: None,
        diagnostics: Vec::new(),
        launch_result: None,
    }
}

fn failed_snapshot(
    package_name: &str,
    entrypoint_id: &str,
    error: std::io::Error,
) -> EntrypointProcessSnapshot {
    EntrypointProcessSnapshot {
        package_name: package_name.to_string(),
        entrypoint_id: entrypoint_id.to_string(),
        state: "failed".to_string(),
        pid: None,
        started_at: Some(now_seconds()),
        exited_at: Some(now_seconds()),
        exit_status: None,
        diagnostics: vec![EntrypointDiagnostic {
            kind: "spawn_error".to_string(),
            message: bounded_message(error.to_string()),
        }],
        launch_result: Some(RunnableEntrypointLaunchResult {
            entrypoint_id: entrypoint_id.to_string(),
            process_state: RunnableEntrypointProcessState::Failed,
            local_url: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_exit::{ReapRegistration, ReapWatch, wait_for_pid_exit};
    use std::io::BufRead;
    use std::os::unix::process::CommandExt;
    use std::sync::mpsc::Sender;

    const READY_RECORD: &str =
        r#"{"entrypoint_id":"web","process_state":"running","local_url":"http://127.0.0.1:4321"}"#;

    fn test_deadline() -> Instant {
        Instant::now() + Duration::from_secs(2)
    }

    /// Registers a reap watch while `pid` still runs, so a later
    /// `assert_reaped` proves the process left the process table.
    fn watch_reap(pid: u32) -> ReapWatch {
        match ReapWatch::register(pid).expect("register reap watch") {
            ReapRegistration::Watching(watch) => watch,
            _ => panic!("pid {pid} exited before its reap watch"),
        }
    }

    fn assert_reaped(mut watch: ReapWatch, pid: u32) {
        assert!(
            watch.wait(test_deadline()).expect("wait for reap"),
            "pid {pid} survived"
        );
    }

    /// Spawns `script` as the leader of its own process group, with stdin held
    /// by the child and stdout piped, and hands the child to a
    /// SupervisedProcess at once, so a later panic still stops the group. The
    /// receiver yields the first stdout line.
    fn supervised_group_leader(
        script: &str,
    ) -> (SupervisedProcess, Receiver<std::io::Result<String>>) {
        let mut command = Command::new("sh");
        command
            .args(["-c", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().expect("spawn controlled process group");
        let stdout = child.stdout.take().expect("leader stdout");
        let process = supervised_running_process(child);
        let (line_tx, line_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut line = String::new();
            let result = std::io::BufReader::new(stdout)
                .read_line(&mut line)
                .map(|_| line.trim().to_string());
            let _ = line_tx.send(result);
        });
        (process, line_rx)
    }

    fn read_published_line(published: &Receiver<std::io::Result<String>>) -> String {
        // timer: deadline — the fixture's two seconds to publish; expiry fails the test.
        published
            .recv_timeout(Duration::from_secs(2))
            .expect("the fixture publishes a line within two seconds")
            .expect("read the published line")
    }

    /// A group leader whose descendant blocks on the leader's stdin pipe
    /// through fd 3 until the group is signalled. The leader publishes the
    /// descendant pid on stdout.
    fn controlled_running_process() -> (SupervisedProcess, u32) {
        let (process, published) = supervised_group_leader(
            "exec 3<&0; cat <&3 >/dev/null & printf '%s\\n' \"$!\"; exec >/dev/null; wait",
        );
        let descendant_pid = read_published_line(&published)
            .parse()
            .expect("controlled descendant pid is published as a number");
        (process, descendant_pid)
    }

    fn supervised_running_process(child: Child) -> SupervisedProcess {
        SupervisedProcess {
            child,
            environment: BTreeMap::new(),
            started_at: now_seconds(),
            exited_at: None,
            exit_status: None,
            stdout: None,
            stderr: None,
            diagnostics: Vec::new(),
            launch_result: Some(RunnableEntrypointLaunchResult {
                entrypoint_id: "web".to_string(),
                process_state: RunnableEntrypointProcessState::Running,
                local_url: None,
            }),
            state: ProcessState::Running,
            pending_terminal_state: None,
            output_finalization_deadline: None,
        }
    }

    fn controlled_failed_process() -> (SupervisedProcess, Sender<Vec<u8>>, Sender<Vec<u8>>) {
        let child = Command::new("sh")
            .args(["-c", "exit 42"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn controlled failing child");
        let (stdout_tx, stdout) = mpsc::channel();
        let (stderr_tx, stderr) = mpsc::channel();
        (
            SupervisedProcess {
                child,
                environment: BTreeMap::new(),
                started_at: now_seconds(),
                exited_at: None,
                exit_status: None,
                stdout: Some(stdout),
                stderr: Some(stderr),
                diagnostics: Vec::new(),
                launch_result: Some(RunnableEntrypointLaunchResult {
                    entrypoint_id: "web".to_string(),
                    process_state: RunnableEntrypointProcessState::Running,
                    local_url: None,
                }),
                state: ProcessState::Running,
                pending_terminal_state: None,
                output_finalization_deadline: None,
            },
            stdout_tx,
            stderr_tx,
        )
    }

    fn observe_child_exit(process: &mut SupervisedProcess) {
        assert!(
            wait_for_pid_exit(process.child.id(), test_deadline()).expect("watch child exit"),
            "controlled child did not exit"
        );
        process.refresh();
        assert!(process.exited_at.is_some(), "controlled child did not exit");
    }

    #[test]
    fn terminal_state_waits_for_both_output_readers() {
        let (mut process, stdout_tx, stderr_tx) = controlled_failed_process();
        observe_child_exit(&mut process);

        assert!(matches!(process.state, ProcessState::Running));
        assert!(!process.is_running());
        assert_eq!(process.exit_status.as_deref(), Some("exit:42"));
        assert!(
            process
                .snapshot(&EntrypointKey {
                    package_name: "fixture".to_string(),
                    entrypoint_id: "web".to_string(),
                })
                .pid
                .is_none()
        );

        stdout_tx.send(Vec::new()).expect("settle stdout reader");
        process.refresh();
        assert!(process.stdout.is_none());
        assert!(process.stderr.is_some());
        assert!(matches!(process.state, ProcessState::Running));

        stderr_tx
            .send(b"fixture failure\n".to_vec())
            .expect("settle stderr reader");
        process.refresh();

        assert!(matches!(process.state, ProcessState::Failed));
        assert!(process.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == "stderr" && diagnostic.message == "fixture failure\n"
        }));
        assert!(matches!(
            process
                .launch_result
                .as_ref()
                .map(|result| &result.process_state),
            Some(RunnableEntrypointProcessState::Failed)
        ));
    }

    #[test]
    fn terminal_state_waits_when_only_stderr_reader_completes() {
        let (mut process, stdout_tx, stderr_tx) = controlled_failed_process();
        observe_child_exit(&mut process);

        stderr_tx
            .send(b"fixture failure\n".to_vec())
            .expect("settle stderr reader");
        process.refresh();

        assert!(process.stderr.is_none());
        assert!(process.stdout.is_some());
        assert!(matches!(process.state, ProcessState::Running));
        assert!(process.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == "stderr" && diagnostic.message == "fixture failure\n"
        }));

        stdout_tx.send(Vec::new()).expect("settle stdout reader");
        process.refresh();
        assert!(matches!(process.state, ProcessState::Failed));
    }

    /// Starts `script` the way `start` does for an entrypoint that declares a
    /// launch result: a launch-record pipe at fd 3, stdout and stderr readers,
    /// and a SupervisedProcess that owns the child (held stdin) at once.
    fn start_record_fixture(script: &str) -> (EntrypointSupervisor, LaunchRecordReader) {
        let mut command = Command::new("sh");
        command
            .args(["-c", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let (mut child, reader) = spawn_with_launch_record(&mut command).expect("spawn fixture");
        let stdout = child.stdout.take().map(spawn_reader);
        let stderr = child.stderr.take().map(spawn_reader);
        let mut process = supervised_running_process(child);
        process.stdout = stdout;
        process.stderr = stderr;
        let mut supervisor = EntrypointSupervisor::default();
        supervisor.processes.insert(fixture_key(), process);
        (supervisor, reader)
    }

    fn fixture_key() -> EntrypointKey {
        EntrypointKey {
            package_name: "fixture".to_string(),
            entrypoint_id: "web".to_string(),
        }
    }

    fn readiness_failure(error: EntrypointSupervisorError) -> String {
        match error {
            EntrypointSupervisorError::ReadinessFailed { details, .. } => details,
            other => panic!("expected ReadinessFailed, got {other:?}"),
        }
    }

    #[test]
    fn a_launch_record_makes_the_entrypoint_ready_before_the_deadline() {
        let (mut supervisor, reader) = start_record_fixture(&format!(
            "printf '%s\\n' '{READY_RECORD}' >&3; exec cat >/dev/null"
        ));
        let deadline = Instant::now() + Duration::from_secs(10);

        let snapshot = supervisor
            .wait_for_launch_record(&fixture_key(), reader, deadline)
            .expect("the record makes the entrypoint ready");

        let result = snapshot.launch_result.expect("launch result");
        assert_eq!(result.local_url.as_deref(), Some("http://127.0.0.1:4321"));
        assert!(
            matches!(
                result.process_state,
                RunnableEntrypointProcessState::Running
            ),
            "the supervisor, not the record, owns process_state"
        );
        assert!(Instant::now() < deadline);
    }

    #[test]
    fn a_record_written_before_the_producer_exits_is_not_lost() {
        let (mut supervisor, reader) =
            start_record_fixture(&format!("printf '%s\\n' '{READY_RECORD}' >&3; exit 0"));

        let snapshot = supervisor
            .wait_for_launch_record(
                &fixture_key(),
                reader,
                Instant::now() + Duration::from_secs(10),
            )
            .expect("the record written before exit still counts");

        assert_eq!(
            snapshot
                .launch_result
                .and_then(|result| result.local_url)
                .as_deref(),
            Some("http://127.0.0.1:4321")
        );
    }

    #[test]
    fn a_descendant_may_publish_after_the_leader_exits() {
        // Behavior change: EOF, not leader exit, fails readiness. The leader
        // starts a descendant that holds fd 3 and publishes only after the test
        // releases it, which the test does once the leader has exited.
        let (mut supervisor, reader) = start_record_fixture(&format!(
            "exec 4<&0; (read -r _ <&4; printf '%s\\n' '{READY_RECORD}' >&3; exec cat <&4 >/dev/null) & exit 0"
        ));
        let leader = supervisor.processes[&fixture_key()].child.id();
        let mut release = supervisor
            .processes
            .get_mut(&fixture_key())
            .expect("fixture process")
            .child
            .stdin
            .take()
            .expect("fixture stdin");
        let releaser = thread::spawn(move || {
            assert!(wait_for_pid_exit(leader, test_deadline()).expect("watch leader exit"));
            std::io::Write::write_all(&mut release, b"go\n").expect("release descendant");
            release
        });

        let snapshot = supervisor
            .wait_for_launch_record(
                &fixture_key(),
                reader,
                Instant::now() + Duration::from_secs(10),
            )
            .expect("the descendant's record makes the entrypoint ready");
        let release = releaser.join().expect("join releaser");

        assert!(snapshot.exited_at.is_some(), "the leader has exited");
        assert!(process_group_running(leader).expect("inspect group"));
        supervisor.stop_all();
        assert!(!process_group_running(leader).expect("inspect group after stop"));
        drop(release);
    }

    #[test]
    fn a_write_end_already_at_the_ready_fd_stays_open_across_exec() {
        use std::io::Read;
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;

        let (mut reader, writer) = std::io::pipe().unwrap();
        let source = writer.as_raw_fd();
        let mut command = Command::new("sh");
        command
            .args(["-c", "printf 'record\\n' >&3"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        unsafe {
            // SAFETY: dup2 and fcntl are async-signal-safe. This makes the
            // write end READY_FD with close-on-exec set, the case under test.
            command.pre_exec(move || {
                if libc::dup2(source, READY_FD) == -1
                    || libc::fcntl(READY_FD, libc::F_SETFD, libc::FD_CLOEXEC) == -1
                {
                    return Err(std::io::Error::last_os_error());
                }
                place_ready_fd(READY_FD)
            });
        }
        let mut child = command.spawn().expect("spawn the fd 3 writer");
        drop(writer);
        let status = child.wait().expect("wait for the writer");
        let mut record = String::new();
        reader.read_to_string(&mut record).unwrap();
        assert!(status.success(), "the shell could write fd 3: {status}");
        assert_eq!(record, "record\n");
    }

    #[test]
    fn a_failed_reader_setup_spawns_no_child() {
        let (mut started, stdout) = std::io::pipe().expect("stdout pipe");
        let mut command = Command::new("sh");
        command
            .args(["-c", "printf started"])
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::null());
        FAIL_CANCEL_PIPE.with(|fail| fail.set(true));
        let result = spawn_with_launch_record(&mut command);
        assert!(result.is_err(), "the injected setup failure is reported");
        // Once the Command is dropped, only a spawned child could still hold
        // the stdout write end; with no child the read ends at once, empty.
        drop(command);
        let mut output = String::new();
        std::io::Read::read_to_string(&mut started, &mut output).unwrap();
        assert_eq!(output, "", "no child may run when reader setup fails");
    }

    #[test]
    fn continuous_partial_input_cannot_delay_cancellation() {
        let (record, mut writer) = std::io::pipe().expect("record pipe");
        let (cancel, cancel_writer) = std::io::pipe().expect("cancel pipe");
        std::io::Write::write_all(&mut writer, b"xxxx").unwrap();
        let mut cancel_writer = Some(cancel_writer);
        let mut reads = 0;
        // Refill the pipe inside each read, so data is always ready; cancel
        // after the third read.
        let refill = Box::new(move |_length: usize| {
            reads += 1;
            if reads == 3 {
                drop(cancel_writer.take());
            }
            let _ = std::io::Write::write_all(&mut writer, b"xxxx");
        });
        let (done_tx, done) = mpsc::channel();
        thread::spawn(move || {
            let _ = done_tx.send(read_launch_record(record, cancel, Some(refill)));
        });
        // timer: deadline — bounds a reader that never rechecks cancellation; expiry fails the test.
        let outcome = done
            .recv_timeout(Duration::from_secs(5))
            .expect("cancellation ends the reader under continuous input");
        assert!(
            outcome.is_none(),
            "a cancelled reader reports nothing: {outcome:?}"
        );
    }

    #[test]
    fn a_fragmented_record_is_assembled() {
        let (record, mut writer) = std::io::pipe().expect("record pipe");
        let (cancel, _cancel_writer) = std::io::pipe().expect("cancel pipe");
        let (partial_tx, partial) = mpsc::channel();
        let reading = thread::spawn(move || {
            read_launch_record(
                record,
                cancel,
                Some(Box::new(move |length| {
                    let _ = partial_tx.send(length);
                })),
            )
        });
        let (head, tail) = READY_RECORD.split_at(20);

        std::io::Write::write_all(&mut writer, head.as_bytes()).expect("write the first fragment");
        // timer: deadline — the reader's report that it consumed the first fragment; expiry fails the test.
        assert_eq!(
            partial
                .recv_timeout(Duration::from_secs(2))
                .expect("partial read"),
            20
        );
        std::io::Write::write_all(&mut writer, format!("{tail}\n").as_bytes())
            .expect("write the second fragment");

        match reading.join().expect("join reader") {
            Some(LaunchRecordEvent::Record(line)) => assert_eq!(line, READY_RECORD.as_bytes()),
            other => panic!("expected the whole record, got {other:?}"),
        }
    }

    #[test]
    fn a_record_for_another_entrypoint_fails_readiness() {
        let (mut supervisor, reader) = start_record_fixture(
            "printf '%s\\n' '{\"entrypoint_id\":\"other\",\"process_state\":\"running\",\"local_url\":\"http://127.0.0.1:1\"}' >&3; exec cat >/dev/null",
        );

        let details = readiness_failure(
            supervisor
                .wait_for_launch_record(
                    &fixture_key(),
                    reader,
                    Instant::now() + Duration::from_secs(10),
                )
                .expect_err("a record for another entrypoint fails"),
        );

        assert!(details.contains("names entrypoint \"other\""), "{details}");
        assert!(!supervisor.processes[&fixture_key()].is_running());
    }

    #[test]
    fn a_record_without_a_local_url_fails_readiness() {
        let (mut supervisor, reader) = start_record_fixture(
            "printf '%s\\n' '{\"entrypoint_id\":\"web\",\"process_state\":\"running\"}' >&3; exec cat >/dev/null",
        );

        let details = readiness_failure(
            supervisor
                .wait_for_launch_record(
                    &fixture_key(),
                    reader,
                    Instant::now() + Duration::from_secs(10),
                )
                .expect_err("a record without local_url fails"),
        );

        assert!(details.contains("no local_url"), "{details}");
    }

    #[test]
    fn an_incomplete_record_then_eof_fails_readiness() {
        let (mut supervisor, reader) =
            start_record_fixture("printf '%s' '{\"entrypoint_id\"' >&3; exit 0");

        let details = readiness_failure(
            supervisor
                .wait_for_launch_record(
                    &fixture_key(),
                    reader,
                    Instant::now() + Duration::from_secs(10),
                )
                .expect_err("EOF inside a record fails"),
        );

        assert!(details.contains("closed without a record"), "{details}");
    }

    #[test]
    fn eof_without_a_record_fails_with_the_child_output() {
        let (mut supervisor, reader) =
            start_record_fixture("printf 'startup failed marker\\n' >&2; exit 3");

        let details = readiness_failure(
            supervisor
                .wait_for_launch_record(
                    &fixture_key(),
                    reader,
                    Instant::now() + Duration::from_secs(10),
                )
                .expect_err("EOF without a record fails"),
        );

        assert!(details.contains("closed without a record"), "{details}");
        assert!(details.contains("startup failed marker"), "{details}");
    }

    #[test]
    fn a_queued_record_after_the_deadline_does_not_count() {
        let (mut supervisor, _unused_reader) = start_record_fixture("exec cat >/dev/null");
        let (events_tx, events) = mpsc::sync_channel(1);
        events_tx
            .send(LaunchRecordEvent::Record(READY_RECORD.as_bytes().to_vec()))
            .expect("queue a record");
        let (_cancel_reader, cancel_writer) = std::io::pipe().expect("cancel pipe");
        let queued = LaunchRecordReader {
            events,
            _cancel: cancel_writer,
            retired: mpsc::sync_channel(1).1,
        };

        let error = supervisor
            .wait_for_launch_record(&fixture_key(), queued, Instant::now())
            .expect_err("an expired deadline wins over a queued record");

        assert!(
            matches!(error, EntrypointSupervisorError::ReadinessTimeout { .. }),
            "{error:?}"
        );
        assert!(!supervisor.processes[&fixture_key()].is_running());
    }

    #[test]
    fn dropping_the_reader_retires_its_thread_while_the_writer_stays_open() {
        let (record, writer) = std::io::pipe().expect("record pipe");
        let mut reader = start_launch_record_reader(record).expect("start reader");
        let retired = reader.take_retired();

        drop(reader);

        // timer: deadline — the reader's positive retirement signal; expiry fails the test.
        retired
            .recv_timeout(Duration::from_secs(2))
            .expect("the reader thread exits on cancellation");
        drop(writer);
    }

    #[test]
    fn readiness_failure_waits_for_delayed_exit_diagnostics() {
        let (mut supervisor, reader) = start_record_fixture("exit 42");
        // A rendezvous channel: the send completes only while the supervisor is
        // receiving, so the diagnostics arrive after the failure path began.
        let (stderr_tx, stderr) = mpsc::sync_channel(0);
        supervisor
            .processes
            .get_mut(&fixture_key())
            .expect("fixture process")
            .stderr = Some(stderr);
        let output = thread::spawn(move || {
            stderr_tx
                .send(b"delayed fixture failure\n".to_vec())
                .expect("finish stderr reader");
        });

        let details = readiness_failure(
            supervisor
                .wait_for_launch_record(
                    &fixture_key(),
                    reader,
                    Instant::now() + Duration::from_secs(10),
                )
                .expect_err("an exit without a record fails"),
        );
        output.join().expect("join delayed output fixture");

        assert!(details.contains("delayed fixture failure"), "{details}");
    }

    #[test]
    fn stop_preserves_pending_terminal_launch_result_state() {
        let (mut process, _stdout_tx, _stderr_tx) = controlled_failed_process();
        observe_child_exit(&mut process);
        assert!(matches!(
            process
                .launch_result
                .as_ref()
                .map(|result| &result.process_state),
            Some(RunnableEntrypointProcessState::Running)
        ));

        process.stop();

        assert!(matches!(process.state, ProcessState::Stopped));
        assert!(matches!(
            process
                .launch_result
                .as_ref()
                .map(|result| &result.process_state),
            Some(RunnableEntrypointProcessState::Failed)
        ));
    }

    #[test]
    fn terminal_state_publishes_when_output_finalization_deadline_expires() {
        let (mut process, _stdout_tx, stderr_tx) = controlled_failed_process();
        observe_child_exit(&mut process);
        process.output_finalization_deadline = Some(Instant::now());

        process.refresh();

        assert!(matches!(process.state, ProcessState::Failed));
        assert!(
            process
                .snapshot(&EntrypointKey {
                    package_name: "fixture".to_string(),
                    entrypoint_id: "web".to_string(),
                })
                .pid
                .is_none()
        );
        stderr_tx
            .send(b"late stderr\n".to_vec())
            .expect("deliver stderr after terminal publication");
        process.refresh();
        assert!(matches!(process.state, ProcessState::Failed));
        assert!(process.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == "stderr" && diagnostic.message == "late stderr\n"
        }));
        let started = Instant::now();
        process.stop();
        assert!(started.elapsed() < STOP_GRACE);
        assert!(matches!(process.state, ProcessState::Stopped));
        assert!(matches!(
            process
                .launch_result
                .as_ref()
                .map(|result| &result.process_state),
            Some(RunnableEntrypointProcessState::Failed)
        ));
    }

    #[test]
    fn supervisor_drop_terminates_and_reaps_owned_process_group() {
        let mut command = Command::new("sh");
        command
            .args(["-c", "exec cat >/dev/null"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().expect("spawn controlled process group");
        let pid = child.id();
        let key = EntrypointKey {
            package_name: "fixture".to_string(),
            entrypoint_id: "web".to_string(),
        };
        let mut supervisor = EntrypointSupervisor::default();
        supervisor.processes.insert(
            key,
            SupervisedProcess {
                child,
                environment: BTreeMap::new(),
                started_at: now_seconds(),
                exited_at: None,
                exit_status: None,
                stdout: None,
                stderr: None,
                diagnostics: Vec::new(),
                launch_result: None,
                state: ProcessState::Running,
                pending_terminal_state: None,
                output_finalization_deadline: None,
            },
        );

        drop(supervisor);

        assert_eq!(
            unsafe { libc::kill(pid as libc::pid_t, 0) },
            -1,
            "supervisor drop must leave no owned child"
        );
    }

    /// A fixture whose pid publication fails must still leave no process
    /// group: the panic drops the SupervisedProcess, which stops the group.
    fn failed_publication_reaps_owned_process_group(label: &str, script: &str) {
        let _ = label;
        let (process, published) = supervised_group_leader(script);
        let pid = process.child.id();
        let leader_reaped = watch_reap(pid);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _process = process;
            let _: u32 = read_published_line(&published)
                .parse()
                .expect("an unparseable publication must fail loudly");
        }));

        assert!(result.is_err(), "a failed pid publication must fail loudly");
        assert_reaped(leader_reaped, pid);
        assert!(
            !process_group_running(pid).expect("inspect owned process group"),
            "a failed publication must leave no owned process group"
        );
    }

    #[test]
    fn missing_descendant_pid_publication_reaps_owned_process_group() {
        // The leader and its descendant keep stdout open and never write.
        failed_publication_reaps_owned_process_group(
            "missing-publication",
            "exec 3<&0; cat <&3 >/dev/null & wait",
        );
    }

    #[test]
    fn unparseable_descendant_pid_publication_reaps_owned_process_group() {
        failed_publication_reaps_owned_process_group(
            "unparseable-publication",
            "exec 3<&0; cat <&3 >/dev/null & printf 'not-a-pid\\n'; exec >/dev/null; wait",
        );
    }

    /// A reader whose write end the test holds and never writes.
    fn silent_reader() -> (LaunchRecordReader, std::io::PipeWriter) {
        let (record, writer) = std::io::pipe().expect("record pipe");
        (
            start_launch_record_reader(record).expect("start reader"),
            writer,
        )
    }

    #[test]
    fn readiness_timeout_stops_real_owned_process_group_and_descendant() {
        let (process, descendant_pid) = controlled_running_process();
        let descendant_reaped = watch_reap(descendant_pid);
        let mut supervisor = EntrypointSupervisor::default();
        supervisor.processes.insert(fixture_key(), process);
        let (reader, _held_writer) = silent_reader();

        let error = supervisor
            .wait_for_launch_record(
                &fixture_key(),
                reader,
                Instant::now() + Duration::from_millis(100),
            )
            .expect_err("a silent record pipe must time out");

        assert!(matches!(
            error,
            EntrypointSupervisorError::ReadinessTimeout { .. }
        ));
        assert!(
            supervisor.processes[&fixture_key()].exited_at.is_some(),
            "readiness timeout must reap the supervised child"
        );
        assert_reaped(descendant_reaped, descendant_pid);
    }

    #[test]
    fn a_stopped_record_reader_stops_the_owned_process_group() {
        let (process, descendant_pid) = controlled_running_process();
        let descendant_reaped = watch_reap(descendant_pid);
        let mut supervisor = EntrypointSupervisor::default();
        supervisor.processes.insert(fixture_key(), process);
        let (events_tx, events) = mpsc::sync_channel(1);
        drop(events_tx);
        let (_cancel_reader, cancel_writer) = std::io::pipe().expect("cancel pipe");
        let stopped = LaunchRecordReader {
            events,
            _cancel: cancel_writer,
            retired: mpsc::sync_channel(1).1,
        };

        let details = readiness_failure(
            supervisor
                .wait_for_launch_record(
                    &fixture_key(),
                    stopped,
                    Instant::now() + Duration::from_secs(1),
                )
                .expect_err("a stopped reader fails readiness"),
        );

        assert!(details.contains("reader stopped"), "{details}");
        assert!(supervisor.processes[&fixture_key()].exited_at.is_some());
        assert_reaped(descendant_reaped, descendant_pid);
    }
}
