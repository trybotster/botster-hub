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

use botster_core::{
    PackageSource, RunnableEntrypointLaunchMode, RunnableEntrypointLaunchResult,
    RunnableEntrypointProcessState, RunnableEntrypointResultField,
};
use notify::{RecursiveMode, Watcher};

use crate::{PackageRecord, PackageRegistry, PackageRunnableEntrypoint, PackageState};

const OUTPUT_LIMIT_BYTES: usize = 4096;
const OUTPUT_FINALIZATION_GRACE: Duration = Duration::from_millis(500);
const STOP_GRACE: Duration = Duration::from_millis(500);
const LAUNCH_RESULT_READINESS_BUDGET: Duration = Duration::from_secs(15);
const LAUNCH_RESULT_ENV: &str = "BOTSTER_ENTRYPOINT_LAUNCH_RESULT";

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

        let launch_result_path = entrypoint_declares_launch_result(entrypoint)
            .then(|| launch_result_path(&key.package_name, &key.entrypoint_id, now_seconds()));
        let launch_result_watcher = launch_result_path
            .as_ref()
            .map(|path| watch_launch_result_parent(path))
            .transpose()?;
        let command_path = resolve_command(&package_root, entrypoint.command.as_str());
        let working_directory = resolve_working_directory(&package_root, entrypoint)?;
        let mut command = Command::new(command_path);
        command.args(arguments);
        command.current_dir(working_directory);
        for (name, value) in environment_overrides {
            command.env(name, value);
        }
        if let Some(path) = &launch_result_path {
            let _ = fs::remove_file(path);
            command.env(LAUNCH_RESULT_ENV, path);
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

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                let snapshot = failed_snapshot(package_name, entrypoint_id, error);
                self.retained.insert(key, snapshot.clone());
                return Ok(snapshot);
            }
        };
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
            launch_result_path,
            state: ProcessState::Running,
            pending_terminal_state: None,
            output_finalization_deadline: None,
        };
        let snapshot = process.snapshot(&key);
        self.retained.insert(key.clone(), snapshot.clone());
        self.processes.insert(key.clone(), process);
        let _ = record;
        if let Some((_watcher, events)) = launch_result_watcher {
            self.wait_for_launch_result(&key, &events, LAUNCH_RESULT_READINESS_BUDGET)
        } else {
            Ok(snapshot)
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

    fn wait_for_launch_result(
        &mut self,
        key: &EntrypointKey,
        events: &Receiver<notify::Result<notify::Event>>,
        readiness_budget: Duration,
    ) -> EntrypointSupervisorResult<EntrypointProcessSnapshot> {
        let deadline = Instant::now() + readiness_budget;
        let launch_result_path = self
            .processes
            .get(key)
            .and_then(|process| process.launch_result_path.clone())
            .expect("readiness wait requires a launch-result path");
        let mut launch_result_may_have_changed = true;
        loop {
            let process = self
                .processes
                .get_mut(key)
                .expect("entrypoint was inserted before readiness wait");
            if launch_result_may_have_changed {
                process.refresh();
            } else {
                process.refresh_process_state();
            }
            let snapshot = process.snapshot(key);
            self.retained.insert(key.clone(), snapshot.clone());

            if snapshot
                .launch_result
                .as_ref()
                .and_then(|result| result.local_url.as_deref())
                .is_some_and(|url| !url.trim().is_empty())
            {
                return Ok(snapshot);
            }
            if process.exited_at.is_some() && process.pending_terminal_state.is_none() {
                return Err(EntrypointSupervisorError::ReadinessFailed {
                    package_name: key.package_name.clone(),
                    entrypoint_id: key.entrypoint_id.clone(),
                    details: readiness_details(&snapshot),
                });
            }

            let now = Instant::now();
            if now >= deadline {
                let error = EntrypointSupervisorError::ReadinessTimeout {
                    package_name: key.package_name.clone(),
                    entrypoint_id: key.entrypoint_id.clone(),
                    details: readiness_details(&snapshot),
                };
                process.stop();
                return Err(error);
            }
            let wait = (deadline - now).min(Duration::from_millis(50));
            match events.recv_timeout(wait) {
                Ok(Ok(event)) => {
                    launch_result_may_have_changed = event
                        .paths
                        .iter()
                        .any(|path| path.file_name() == launch_result_path.file_name());
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Filesystem backends may coalesce a create/write sequence or
                    // report only the watched parent. Re-read the tiny result file
                    // on timeout as a portability fallback; only valid structured
                    // readiness can succeed.
                    launch_result_may_have_changed = true;
                }
                Ok(Err(error)) => {
                    process.stop();
                    return Err(EntrypointSupervisorError::Watch(error.to_string()));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    process.stop();
                    return Err(EntrypointSupervisorError::Watch(
                        "launch-result watcher disconnected".to_string(),
                    ));
                }
            }
        }
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
    launch_result_path: Option<PathBuf>,
    state: ProcessState,
    pending_terminal_state: Option<ProcessState>,
    output_finalization_deadline: Option<Instant>,
}

impl SupervisedProcess {
    fn is_running(&self) -> bool {
        self.exited_at.is_none()
    }

    fn refresh(&mut self) {
        self.refresh_launch_result();
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

    fn refresh_launch_result(&mut self) {
        let Some(path) = &self.launch_result_path else {
            return;
        };
        let Ok(bytes) = fs::read(path) else {
            return;
        };
        let Ok(result) = serde_json::from_slice::<RunnableEntrypointLaunchResult>(&bytes) else {
            return;
        };
        if let Some(current) = &self.launch_result
            && result.entrypoint_id != current.entrypoint_id
        {
            return;
        }
        self.launch_result = Some(result);
        // The child supplies result fields, but supervised process state comes from reaping.
        self.sync_launch_result_process_state();
    }

    fn stop(&mut self) {
        self.refresh();
        let pid = self.child.id();
        if self.exited_at.is_some() && !supervised_process_group_exists(pid) {
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
        let deadline = std::time::Instant::now() + STOP_GRACE;
        while std::time::Instant::now() < deadline {
            self.refresh();
            if self.exited_at.is_some() && !supervised_process_group_exists(pid) {
                self.mark_stopped();
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        if let Err(error) = signal_process_group_or_child(pid, libc::SIGKILL) {
            self.diagnostics.push(EntrypointDiagnostic {
                kind: "cleanup_signal".to_string(),
                message: bounded_message(error.to_string()),
            });
            return;
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            self.refresh();
            if self.exited_at.is_some() && !supervised_process_group_exists(pid) {
                self.mark_stopped();
                return;
            }
            thread::sleep(Duration::from_millis(20));
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

    fn mark_exit(&mut self, status: ExitStatus) {
        self.exited_at = Some(now_seconds());
        self.exit_status = Some(exit_status_label(status));
        if let Some(path) = self.launch_result_path.take()
            && let Err(error) = fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            self.diagnostics.push(EntrypointDiagnostic {
                kind: "launch_result_cleanup".to_string(),
                message: bounded_message(error.to_string()),
            });
        }
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

fn launch_result_path(package_name: &str, entrypoint_id: &str, started_at: u64) -> PathBuf {
    let file_name = format!(
        "botster-launch-result-{}-{}-{started_at}.json",
        sanitized_path_component(package_name),
        sanitized_path_component(entrypoint_id)
    );
    std::env::temp_dir().join(file_name)
}

fn watch_launch_result_parent(
    launch_result_path: &Path,
) -> EntrypointSupervisorResult<(
    notify::RecommendedWatcher,
    Receiver<notify::Result<notify::Event>>,
)> {
    let (tx, rx) = mpsc::channel();
    let mut watcher = notify::recommended_watcher(tx)
        .map_err(|error| EntrypointSupervisorError::Watch(error.to_string()))?;
    let parent = launch_result_path.parent().ok_or_else(|| {
        EntrypointSupervisorError::Watch("launch-result path has no parent".to_string())
    })?;
    watcher
        .watch(parent, RecursiveMode::NonRecursive)
        .map_err(|error| EntrypointSupervisorError::Watch(error.to_string()))?;
    Ok((watcher, rx))
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

fn sanitized_path_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect()
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
            if !bytes.is_empty() {
                diagnostics.push(EntrypointDiagnostic {
                    kind: kind.to_string(),
                    message: bounded_message(String::from_utf8_lossy(&bytes).to_string()),
                });
            }
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

fn supervised_process_group_exists(pid: u32) -> bool {
    if unsafe { libc::killpg(pid as libc::pid_t, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
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
    use std::os::unix::process::CommandExt;
    use std::sync::mpsc::Sender;

    fn unique_test_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "botster-entrypoint-supervisor-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("test clock")
                .as_nanos()
        ))
    }

    fn controlled_running_process(
        descendant_pid_path: &Path,
        launch_result_path: PathBuf,
    ) -> (SupervisedProcess, libc::pid_t) {
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                &format!(
                    "sleep 60 & printf '%s\\n' \"$!\" > '{}'; wait",
                    descendant_pid_path.display()
                ),
            ])
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
        let process = supervised_running_process(child, launch_result_path);
        let descendant_pid = wait_for_pid_file(descendant_pid_path);
        (process, descendant_pid)
    }

    fn supervised_running_process(child: Child, launch_result_path: PathBuf) -> SupervisedProcess {
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
            launch_result_path: Some(launch_result_path),
            state: ProcessState::Running,
            pending_terminal_state: None,
            output_finalization_deadline: None,
        }
    }

    fn wait_for_pid_file(path: &Path) -> libc::pid_t {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(pid) = fs::read_to_string(path)
                .ok()
                .and_then(|contents| contents.trim().parse().ok())
            {
                return pid;
            }
            assert!(
                Instant::now() < deadline,
                "controlled descendant pid was not published as parseable content: {}",
                path.display()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn assert_pid_gone(pid: libc::pid_t) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while unsafe { libc::kill(pid, 0) } == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "pid {pid} survived");
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
                launch_result_path: None,
                state: ProcessState::Running,
                pending_terminal_state: None,
                output_finalization_deadline: None,
            },
            stdout_tx,
            stderr_tx,
        )
    }

    fn observe_child_exit(process: &mut SupervisedProcess) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while process.exited_at.is_none() && Instant::now() < deadline {
            process.refresh();
            thread::yield_now();
        }
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

    #[test]
    fn readiness_failure_waits_for_delayed_exit_diagnostics() {
        let (mut process, stdout_tx, stderr_tx) = controlled_failed_process();
        process.launch_result_path = Some(unique_test_path("delayed-readiness-diagnostics"));
        let key = EntrypointKey {
            package_name: "fixture".to_string(),
            entrypoint_id: "web".to_string(),
        };
        let mut supervisor = EntrypointSupervisor::default();
        supervisor.processes.insert(key.clone(), process);
        let (_events_tx, events) = mpsc::channel();
        let output = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            stdout_tx.send(Vec::new()).expect("finish stdout reader");
            stderr_tx
                .send(b"delayed fixture failure\n".to_vec())
                .expect("finish stderr reader");
        });

        let error = supervisor
            .wait_for_launch_result(&key, &events, Duration::from_secs(1))
            .expect_err("exited process must fail readiness");
        output.join().expect("join delayed output fixture");

        assert!(matches!(
            error,
            EntrypointSupervisorError::ReadinessFailed { details, .. }
                if details.contains("delayed fixture failure")
        ));
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
            .args(["-c", "while :; do sleep 1; done"])
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
                launch_result_path: None,
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

    #[test]
    fn missing_descendant_pid_publication_reaps_owned_process_group() {
        let descendant_pid_path = unique_test_path("missing-descendant");
        let launch_result_path = unique_test_path("missing-result");
        let mut command = Command::new("sh");
        command
            .args(["-c", "sleep 60 & wait"])
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
        let child = command
            .spawn()
            .expect("spawn missing-publication process group");
        let pid = child.id();
        let process = supervised_running_process(child, launch_result_path.clone());

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _process = process;
            let _ = wait_for_pid_file(&descendant_pid_path);
        }));

        assert!(
            result.is_err(),
            "missing descendant PID publication must fail loudly"
        );
        assert_pid_gone(pid as libc::pid_t);
        assert!(
            !supervised_process_group_exists(pid),
            "missing publication must leave no owned process group"
        );
        let _ = fs::remove_file(descendant_pid_path);
        let _ = fs::remove_file(launch_result_path);
    }

    #[test]
    fn readiness_timeout_stops_real_owned_process_group_and_descendant() {
        let descendant_pid_path = unique_test_path("timeout-descendant");
        let launch_result_path = unique_test_path("timeout-result");
        let (process, descendant_pid) =
            controlled_running_process(&descendant_pid_path, launch_result_path);
        let key = EntrypointKey {
            package_name: "fixture".to_string(),
            entrypoint_id: "web".to_string(),
        };
        let mut supervisor = EntrypointSupervisor::default();
        supervisor.processes.insert(key.clone(), process);
        let (_events_tx, events) = mpsc::channel();

        let error = supervisor
            .wait_for_launch_result(&key, &events, Duration::from_millis(100))
            .expect_err("missing launch result must time out");

        assert!(matches!(
            error,
            EntrypointSupervisorError::ReadinessTimeout { .. }
        ));
        assert!(
            supervisor.processes[&key].exited_at.is_some(),
            "readiness timeout must reap the supervised child"
        );
        assert_pid_gone(descendant_pid);
        let _ = fs::remove_file(descendant_pid_path);
    }

    #[test]
    fn disconnected_launch_result_watcher_stops_owned_process_group() {
        let descendant_pid_path = unique_test_path("watch-descendant");
        let launch_result_path = unique_test_path("watch-result");
        let (process, descendant_pid) =
            controlled_running_process(&descendant_pid_path, launch_result_path);
        let key = EntrypointKey {
            package_name: "fixture".to_string(),
            entrypoint_id: "web".to_string(),
        };
        let mut supervisor = EntrypointSupervisor::default();
        supervisor.processes.insert(key.clone(), process);
        let (_events_tx, events) = mpsc::channel::<notify::Result<notify::Event>>();
        drop(_events_tx);

        let error = supervisor
            .wait_for_launch_result(&key, &events, Duration::from_secs(1))
            .expect_err("disconnected watcher must fail readiness");

        assert!(
            matches!(error, EntrypointSupervisorError::Watch(message) if message.contains("disconnected"))
        );
        assert!(supervisor.processes[&key].exited_at.is_some());
        assert_pid_gone(descendant_pid);
        let _ = fs::remove_file(descendant_pid_path);
    }

    #[test]
    fn observed_exit_removes_launch_result_file() {
        let launch_result_path = unique_test_path("exit-result");
        fs::write(&launch_result_path, b"{}").expect("write launch result fixture");
        let child = Command::new("sh")
            .args(["-c", "exit 0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn exiting child");
        let mut process = SupervisedProcess {
            child,
            environment: BTreeMap::new(),
            started_at: now_seconds(),
            exited_at: None,
            exit_status: None,
            stdout: None,
            stderr: None,
            diagnostics: Vec::new(),
            launch_result: None,
            launch_result_path: Some(launch_result_path.clone()),
            state: ProcessState::Running,
            pending_terminal_state: None,
            output_finalization_deadline: None,
        };

        observe_child_exit(&mut process);

        assert!(!launch_result_path.exists());
        assert!(process.launch_result_path.is_none());
    }
}
