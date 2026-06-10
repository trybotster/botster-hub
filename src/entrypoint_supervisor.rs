//! Hub-owned local package entrypoint process supervision.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io::{BufReader, Read};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use botster_core::PackageSource;

use crate::{
    PackageRecord, PackageRegistry, PackageRunnableEntrypoint, PackageRunnableMode, PackageState,
};

const OUTPUT_LIMIT_BYTES: usize = 4096;
const STOP_GRACE: Duration = Duration::from_millis(500);

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

        let command_path = resolve_command(&package_root, entrypoint.command.as_str());
        let working_directory = resolve_working_directory(&package_root, entrypoint)?;
        let mut command = Command::new(command_path);
        command.args(&entrypoint.args);
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
            started_at: now_seconds(),
            exited_at: None,
            exit_status: None,
            stdout,
            stderr,
            diagnostics: Vec::new(),
            state: ProcessState::Running,
        };
        let snapshot = process.snapshot(&key);
        self.retained.insert(key.clone(), snapshot.clone());
        self.processes.insert(key, process);
        let _ = record;
        Ok(snapshot)
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
        environment_overrides: &BTreeMap<String, String>,
    ) -> EntrypointSupervisorResult<EntrypointProcessSnapshot> {
        let _ = self.stop(package_name, entrypoint_id);
        self.start(registry, package_name, entrypoint_id, environment_overrides)
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
}

struct SupervisedProcess {
    child: Child,
    started_at: u64,
    exited_at: Option<u64>,
    exit_status: Option<String>,
    stdout: Option<Receiver<Vec<u8>>>,
    stderr: Option<Receiver<Vec<u8>>>,
    diagnostics: Vec<EntrypointDiagnostic>,
    state: ProcessState,
}

impl SupervisedProcess {
    fn is_running(&self) -> bool {
        matches!(self.state, ProcessState::Running)
    }

    fn refresh(&mut self) {
        drain_output("stdout", &mut self.stdout, &mut self.diagnostics);
        drain_output("stderr", &mut self.stderr, &mut self.diagnostics);
        if self.exited_at.is_none() {
            match self.child.try_wait() {
                Ok(Some(status)) => self.mark_exit(status),
                Ok(None) => {}
                Err(error) => {
                    self.state = ProcessState::Failed;
                    self.exited_at = Some(now_seconds());
                    self.diagnostics.push(EntrypointDiagnostic {
                        kind: "wait_error".to_string(),
                        message: bounded_message(error.to_string()),
                    });
                }
            }
        }
    }

    fn stop(&mut self) {
        self.refresh();
        if self.exited_at.is_some() {
            self.state = ProcessState::Stopped;
            return;
        }
        kill_process_group(self.child.id(), libc::SIGTERM);
        let deadline = std::time::Instant::now() + STOP_GRACE;
        while std::time::Instant::now() < deadline {
            self.refresh();
            if self.exited_at.is_some() {
                self.state = ProcessState::Stopped;
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        kill_process_group(self.child.id(), libc::SIGKILL);
        let _ = self.child.wait().map(|status| self.mark_exit(status));
        self.state = ProcessState::Stopped;
        self.refresh();
    }

    fn mark_exit(&mut self, status: ExitStatus) {
        self.exited_at = Some(now_seconds());
        self.exit_status = Some(exit_status_label(status));
        self.state = if status.success() {
            ProcessState::Exited
        } else {
            ProcessState::Failed
        };
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
        }
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
            entrypoint.mode,
            PackageRunnableMode::Dev | PackageRunnableMode::Local
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

fn kill_process_group(pid: u32, signal: libc::c_int) {
    unsafe {
        libc::killpg(pid as libc::pid_t, signal);
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
    }
}
