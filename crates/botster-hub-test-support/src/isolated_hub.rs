//! Isolated hub process harness.
//!
//! Owns IsolatedHubBuilder, IsolatedHub, IsolatedHubError, readiness, cleanup,
//! socket paths, and child-process helpers. Root re-exports stay stable.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_hub_client::{DaemonDiagnostic, DaemonEndpoint, DaemonRequest, DaemonResponseKind};

use crate::ConformanceFailureClass;

const DEFAULT_SOCKET_NAME: &str = "botster-hub.sock";
const READY_TIMEOUT: Duration = Duration::from_secs(5);

/// Builder for one isolated local hub daemon test instance.
///
/// # Example
///
/// ```no_run
/// use botster_hub_test_support::{run_client_conformance, IsolatedHubBuilder};
///
/// let hub = IsolatedHubBuilder::new()
///     .hub_bin(std::env::var("BOTSTER_HUB_BIN").expect("BOTSTER_HUB_BIN"))
///     .session_worker_bin(
///         std::env::var("BOTSTER_SESSION_WORKER_BIN").expect("BOTSTER_SESSION_WORKER_BIN"),
///     )
///     .name("my-client-test")
///     .start()
///     .expect("isolated hub starts");
///
/// let report = run_client_conformance(&hub).expect("client conformance");
/// assert_eq!(report.lifecycle_state, "running");
/// assert_eq!(report.initial_session_count, 0);
/// assert!(report.stream_contains_ready);
/// assert!(report.stream_contains_echo);
/// assert!(report.stream_contains_resize);
/// assert_eq!(report.validation_error_operation, "drain_runtime");
/// hub.shutdown().expect("shutdown isolated hub");
/// ```
#[derive(Debug, Clone)]
pub struct IsolatedHubBuilder {
    hub_bin: Option<PathBuf>,
    session_worker_bin: Option<PathBuf>,
    root: Option<PathBuf>,
    working_directory: Option<PathBuf>,
    name: String,
    ready_timeout: Duration,
    extra_env: Vec<(String, String)>,
}

impl Default for IsolatedHubBuilder {
    fn default() -> Self {
        Self {
            hub_bin: None,
            session_worker_bin: None,
            root: None,
            working_directory: None,
            name: "external-client".to_string(),
            ready_timeout: READY_TIMEOUT,
            extra_env: Vec::new(),
        }
    }
}

impl IsolatedHubBuilder {
    /// Create a builder using explicit binary paths or documented environment variables.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the `botster-hub` binary path to spawn.
    #[must_use]
    pub fn hub_bin(mut self, path: impl Into<PathBuf>) -> Self {
        self.hub_bin = Some(path.into());
        self
    }

    /// Set the `botster-session-worker` binary path used by the spawned hub.
    #[must_use]
    pub fn session_worker_bin(mut self, path: impl Into<PathBuf>) -> Self {
        self.session_worker_bin = Some(path.into());
        self
    }

    /// Set the parent directory used for disposable daemon state.
    #[must_use]
    pub fn root(mut self, path: impl Into<PathBuf>) -> Self {
        self.root = Some(path.into());
        self
    }

    /// Set the working directory inherited by the spawned daemon.
    #[must_use]
    pub fn working_directory(mut self, path: impl Into<PathBuf>) -> Self {
        self.working_directory = Some(path.into());
        self
    }

    /// Set a stable label segment for the disposable data directory.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set an extra environment variable on the spawned daemon process.
    #[must_use]
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_env.push((key.into(), value.into()));
        self
    }

    #[cfg(test)]
    pub(crate) fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// Start the isolated daemon and wait for the real socket protocol to respond.
    pub fn start(self) -> Result<IsolatedHub, IsolatedHubError> {
        let selected_data_dir = self.data_dir()?;
        let working_directory = self
            .working_directory
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        let data_dir = if selected_data_dir.is_absolute() {
            selected_data_dir.clone()
        } else {
            working_directory.join(&selected_data_dir)
        };
        let hub_bin = explicit_path(self.hub_bin, "BOTSTER_HUB_BIN")?;
        let session_worker_bin =
            explicit_path(self.session_worker_bin, "BOTSTER_SESSION_WORKER_BIN")?;
        ensure_file("botster-hub binary", &hub_bin)?;
        ensure_file("botster-session-worker binary", &session_worker_bin)?;

        fs::create_dir_all(&data_dir).map_err(|source| IsolatedHubError::CreateDataDir {
            path: data_dir.clone(),
            source,
        })?;
        let endpoint = DaemonEndpoint::new(data_dir.join(default_socket_name()));

        let mut command = Command::new(&hub_bin);
        command
            .arg("start")
            .arg("--data-dir")
            .arg(&selected_data_dir)
            .arg("--session-worker-bin")
            .arg(&session_worker_bin)
            .current_dir(&working_directory)
            .env("BOTSTER_ENV", "test")
            .env_remove("BOTSTER_HUB_TEST_FAIL_RUNTIME_DRAIN_FOR")
            .env_remove("BOTSTER_HUB_TEST_FAIL_RUNTIME_DRAIN_MESSAGE")
            .env_remove("BOTSTER_HUB_TEST_WORKER_EGRESS_CAPACITY")
            .env_remove("BOTSTER_HUB_TEST_FORCE_SHUTDOWN_CLASSIFY_STOPPING_FOR")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in &self.extra_env {
            command.env(key, value);
        }
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        prepend_worker_dir_to_path(&mut command, &session_worker_bin);

        let mut child = command.spawn().map_err(|source| IsolatedHubError::Spawn {
            path: hub_bin.clone(),
            source,
        })?;

        if let Err(error) = wait_for_ready(&endpoint, &mut child, self.ready_timeout) {
            cleanup_child(&mut child)?;
            reap_session_workers_for_data_dir(&data_dir, &selected_data_dir);
            reap_orphaned_session_workers();
            remove_data_dir_path(&data_dir)?;
            return Err(error);
        }

        Ok(IsolatedHub {
            hub_bin,
            data_dir,
            data_dir_arg: selected_data_dir,
            working_directory,
            endpoint,
            child: Some(child),
        })
    }

    fn data_dir(&self) -> Result<PathBuf, IsolatedHubError> {
        let root = self.root.clone().unwrap_or_else(|| {
            PathBuf::from("target")
                .join("botster-hub-test-data")
                .join("external-harness")
        });
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(IsolatedHubError::Clock)?
            .as_nanos();
        Ok(root
            .join(sanitize_segment(&self.name))
            .join(now.to_string()))
    }
}

/// Running isolated hub daemon. Drop attempts shutdown, then kills on failure.
///
/// Successful explicit shutdown removes the harness-owned data directory. Drop
/// also removes it after the child exits, except during panic unwinding so the
/// failing daemon state remains available for diagnosis.
pub struct IsolatedHub {
    pub(crate) hub_bin: PathBuf,
    pub(crate) data_dir: PathBuf,
    pub(crate) data_dir_arg: PathBuf,
    pub(crate) working_directory: PathBuf,
    pub(crate) endpoint: DaemonEndpoint,
    pub(crate) child: Option<Child>,
}

impl IsolatedHub {
    /// Client endpoint for this isolated daemon socket.
    #[must_use]
    pub const fn endpoint(&self) -> &DaemonEndpoint {
        &self.endpoint
    }

    /// Disposable data directory owned by this harness instance.
    ///
    /// The path remains valid while the harness is running. Successful
    /// [`Self::shutdown`] removes it; panic-time Drop intentionally preserves it.
    #[must_use]
    pub const fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Working directory inherited by the spawned daemon.
    #[must_use]
    pub const fn working_directory(&self) -> &PathBuf {
        &self.working_directory
    }

    /// Stop the daemon, wait for its process, then remove its owned data directory.
    pub fn shutdown(mut self) -> Result<(), IsolatedHubError> {
        self.shutdown_inner()
    }

    pub(crate) fn shutdown_inner(&mut self) -> Result<(), IsolatedHubError> {
        if self.child.is_none() {
            return self.remove_data_dir();
        }
        let output = Command::new(&self.hub_bin)
            .arg("shutdown")
            .arg("--data-dir")
            .arg(&self.data_dir)
            .env("BOTSTER_ENV", "test")
            .output()
            .map_err(|source| IsolatedHubError::ShutdownCommand { source })?;
        let shutdown_stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let shutdown_succeeded = output.status.success();
        let disconnected_during_shutdown =
            shutdown_stderr.trim() == "botster-hub shutdown error: client disconnected";
        if !shutdown_succeeded && !disconnected_during_shutdown {
            return Err(IsolatedHubError::ShutdownFailed {
                stderr: shutdown_stderr,
            });
        }

        let child = self.child.take().expect("child exists after shutdown");
        let daemon_output = child
            .wait_with_output()
            .map_err(|source| IsolatedHubError::Wait { source })?;
        self.reap_owned_session_workers();
        if daemon_output.status.success() {
            if shutdown_succeeded {
                self.remove_data_dir()
            } else {
                Err(IsolatedHubError::ShutdownFailed {
                    stderr: shutdown_stderr,
                })
            }
        } else {
            Err(IsolatedHubError::DaemonExited {
                status: daemon_output.status.to_string(),
                stdout: String::from_utf8_lossy(&daemon_output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&daemon_output.stderr).to_string(),
            })
        }
    }

    fn remove_data_dir(&self) -> Result<(), IsolatedHubError> {
        remove_data_dir_path(&self.data_dir)
    }

    fn reap_owned_session_workers(&self) {
        reap_session_workers_for_data_dir(&self.data_dir, &self.data_dir_arg);
        reap_orphaned_session_workers();
        reap_named_session_workers();
    }
}

fn remove_data_dir_path(data_dir: &Path) -> Result<(), IsolatedHubError> {
    match fs::remove_dir_all(data_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(IsolatedHubError::RemoveDataDir {
            path: data_dir.to_path_buf(),
            source,
        }),
    }
}

impl Drop for IsolatedHub {
    fn drop(&mut self) {
        if thread::panicking() {
            if let Some(child) = self.child.as_mut() {
                let _ = cleanup_child(child);
            }
            self.child = None;
            self.reap_owned_session_workers();
            return;
        }
        if self.shutdown_inner().is_ok() {
            self.reap_owned_session_workers();
            return;
        }
        if let Some(child) = self.child.as_mut() {
            let _ = cleanup_child(child);
        }
        self.child = None;
        self.reap_owned_session_workers();
        let _ = self.remove_data_dir();
    }
}

/// Errors returned by the isolated daemon harness.
#[derive(Debug)]
#[non_exhaustive]
pub enum IsolatedHubError {
    MissingBinaryEnv {
        variable: &'static str,
    },
    MissingBinary {
        label: &'static str,
        path: PathBuf,
    },
    CreateDataDir {
        path: PathBuf,
        source: std::io::Error,
    },
    RemoveDataDir {
        path: PathBuf,
        source: std::io::Error,
    },
    CleanupChild {
        pid: u32,
        source: std::io::Error,
    },
    CleanupTimeout {
        pid: u32,
    },
    Spawn {
        path: PathBuf,
        source: std::io::Error,
    },
    ReadyTimeout {
        stdout: String,
        stderr: String,
    },
    DaemonExited {
        status: String,
        stdout: String,
        stderr: String,
    },
    ShutdownCommand {
        source: std::io::Error,
    },
    ShutdownFailed {
        stderr: String,
    },
    Wait {
        source: std::io::Error,
    },
    Clock(std::time::SystemTimeError),
}

impl fmt::Display for IsolatedHubError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBinaryEnv { variable } => {
                write!(formatter, "missing required binary path or {variable}")
            }
            Self::MissingBinary { label, path } => {
                write!(formatter, "{label} does not exist at {}", path.display())
            }
            Self::CreateDataDir { path, source } => {
                write!(
                    formatter,
                    "failed to create data dir {}: {source}",
                    path.display()
                )
            }
            Self::RemoveDataDir { path, source } => {
                write!(
                    formatter,
                    "failed to remove data dir {} after daemon exit: {source}",
                    path.display()
                )
            }
            Self::CleanupChild { pid, source } => {
                write!(
                    formatter,
                    "failed to signal isolated daemon process {pid}: {source}"
                )
            }
            Self::CleanupTimeout { pid } => {
                write!(
                    formatter,
                    "timed out waiting for isolated daemon process {pid} cleanup"
                )
            }
            Self::Spawn { path, source } => {
                write!(formatter, "failed to spawn {}: {source}", path.display())
            }
            Self::ReadyTimeout { stdout, stderr } => {
                write!(
                    formatter,
                    "timed out waiting for hub daemon readiness: stdout={stdout:?} stderr={stderr:?}"
                )
            }
            Self::DaemonExited {
                status,
                stdout,
                stderr,
            } => {
                write!(
                    formatter,
                    "hub daemon exited with {status}: stdout={stdout:?} stderr={stderr:?}"
                )
            }
            Self::ShutdownCommand { source } => {
                write!(formatter, "shutdown command failed: {source}")
            }
            Self::ShutdownFailed { stderr } => write!(formatter, "shutdown failed: {stderr}"),
            Self::Wait { source } => write!(formatter, "failed to wait for daemon: {source}"),
            Self::Clock(source) => write!(formatter, "system clock error: {source}"),
        }
    }
}

impl IsolatedHubError {
    /// Classify startup and teardown failures as environment/setup failures.
    #[must_use]
    pub const fn failure_class(&self) -> ConformanceFailureClass {
        ConformanceFailureClass::EnvironmentSetup
    }

    /// Return a path-neutral diagnostic for startup failures that occur before
    /// the daemon socket protocol can emit a response.
    #[must_use]
    pub fn diagnostic(&self) -> DaemonDiagnostic {
        let message = match self {
            Self::MissingBinaryEnv { .. } => "required binary path is not configured",
            Self::MissingBinary { label, .. } => {
                return DaemonDiagnostic::daemon_startup_failure(format!(
                    "{label} is not available"
                ));
            }
            Self::CreateDataDir { .. } => "failed to create isolated daemon data directory",
            Self::RemoveDataDir { .. } => "failed to remove isolated daemon data directory",
            Self::CleanupChild { .. } => "failed to clean up isolated daemon process",
            Self::CleanupTimeout { .. } => "timed out cleaning up isolated daemon process",
            Self::Spawn { .. } => "failed to spawn hub daemon",
            Self::ReadyTimeout { .. } => "timed out waiting for hub daemon readiness",
            Self::DaemonExited { .. } => "hub daemon exited before readiness",
            Self::ShutdownCommand { .. } => "failed to run hub daemon shutdown command",
            Self::ShutdownFailed { .. } => "hub daemon shutdown command failed",
            Self::Wait { .. } => "failed to wait for hub daemon process",
            Self::Clock(_) => "system clock error while preparing isolated daemon",
        };

        DaemonDiagnostic::daemon_startup_failure(message)
    }
}

impl Error for IsolatedHubError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CreateDataDir { source, .. }
            | Self::RemoveDataDir { source, .. }
            | Self::CleanupChild { source, .. }
            | Self::Spawn { source, .. }
            | Self::ShutdownCommand { source }
            | Self::Wait { source } => Some(source),
            Self::Clock(source) => Some(source),
            Self::MissingBinaryEnv { .. }
            | Self::MissingBinary { .. }
            | Self::CleanupTimeout { .. }
            | Self::ReadyTimeout { .. }
            | Self::DaemonExited { .. }
            | Self::ShutdownFailed { .. } => None,
        }
    }
}

pub(crate) fn explicit_path(
    explicit: Option<PathBuf>,
    variable: &'static str,
) -> Result<PathBuf, IsolatedHubError> {
    explicit
        .or_else(|| env::var_os(variable).map(PathBuf::from))
        .ok_or(IsolatedHubError::MissingBinaryEnv { variable })
}

fn ensure_file(label: &'static str, path: &Path) -> Result<(), IsolatedHubError> {
    if path.is_file() {
        Ok(())
    } else {
        Err(IsolatedHubError::MissingBinary {
            label,
            path: path.to_path_buf(),
        })
    }
}

fn wait_for_ready(
    endpoint: &DaemonEndpoint,
    child: &mut Child,
    timeout: Duration,
) -> Result<(), IsolatedHubError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child
            .try_wait()
            .map_err(|source| IsolatedHubError::Wait { source })?
        {
            let (stdout, stderr) = collect_child_output(child);
            return Err(IsolatedHubError::DaemonExited {
                status: status.to_string(),
                stdout,
                stderr,
            });
        }

        if botster_hub_client::request(endpoint, DaemonRequest::Status)
            .map(|response| response.kind == DaemonResponseKind::Status)
            .unwrap_or(false)
        {
            return Ok(());
        }

        thread::sleep(Duration::from_millis(50));
    }

    Err(IsolatedHubError::ReadyTimeout {
        stdout: String::new(),
        stderr: String::new(),
    })
}

fn session_worker_pids_for_data_dir(data_dir: &Path, data_dir_arg: &Path) -> Vec<u32> {
    let dir = data_dir.to_string_lossy();
    let arg_token = data_dir_arg.to_string_lossy();
    if dir.is_empty() && arg_token.is_empty() {
        return Vec::new();
    }
    session_worker_argvs()
        .into_iter()
        .filter_map(|(pid, args)| {
            let argv0 = args.first()?;
            if !argv0_is_session_worker(argv0) {
                return None;
            }
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            if args.iter().any(|arg| *arg == dir.as_ref())
                || (!arg_token.is_empty() && args.iter().any(|arg| *arg == arg_token.as_ref()))
                || args.iter().any(|arg| {
                    exact_data_dir_path(arg, data_dir) || exact_data_dir_path(arg, data_dir_arg)
                })
                || proc_cwd_matches_data_dir(pid, data_dir, data_dir_arg)
                || proc_environ_matches_data_dir(pid, data_dir, data_dir_arg)
            {
                Some(pid)
            } else {
                None
            }
        })
        .collect()
}

fn session_worker_pids_orphaned() -> Vec<u32> {
    session_worker_argvs()
        .into_iter()
        .filter_map(|(pid, args)| {
            let argv0 = args.first()?;
            if !argv0_is_session_worker(argv0) {
                return None;
            }
            let ppid = process_ppid(pid)?;
            if ppid <= 1 || !process_pid_exists(ppid) {
                Some(pid)
            } else {
                None
            }
        })
        .collect()
}

fn process_ppid(pid: u32) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status")) {
            for line in status.lines() {
                if let Some(rest) = line.strip_prefix("PPid:") {
                    return rest.trim().parse().ok();
                }
            }
        }
    }
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "ppid="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

fn process_pid_exists(pid: u32) -> bool {
    if pid <= 1 {
        return true;
    }
    let alive = unsafe { libc::kill(pid as libc::pid_t, 0) == 0 };
    alive || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

fn proc_cwd_matches_data_dir(pid: u32, data_dir: &Path, data_dir_arg: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        if let Ok(cwd) = fs::read_link(format!("/proc/{pid}/cwd")) {
            return exact_data_dir_path(cwd.to_string_lossy().as_ref(), data_dir)
                || exact_data_dir_path(cwd.to_string_lossy().as_ref(), data_dir_arg);
        }
    }
    let _ = pid;
    let _ = data_dir;
    let _ = data_dir_arg;
    false
}

fn proc_environ_matches_data_dir(pid: u32, data_dir: &Path, data_dir_arg: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        if let Ok(bytes) = fs::read(format!("/proc/{pid}/environ")) {
            return bytes.split(|byte| *byte == 0).any(|chunk| {
                let Ok(entry) = std::str::from_utf8(chunk) else {
                    return false;
                };
                let Some((_, value)) = entry.split_once('=') else {
                    return false;
                };
                exact_data_dir_path(value, data_dir) || exact_data_dir_path(value, data_dir_arg)
            });
        }
    }
    let _ = pid;
    let _ = data_dir;
    let _ = data_dir_arg;
    false
}

fn session_worker_argvs() -> Vec<(u32, Vec<String>)> {
    #[cfg(target_os = "linux")]
    {
        if let Some(argvs) = linux_proc_session_worker_argvs() {
            return argvs;
        }
    }
    ps_axww_session_worker_argvs()
}

#[cfg(target_os = "linux")]
fn linux_proc_session_worker_argvs() -> Option<Vec<(u32, Vec<String>)>> {
    let entries = fs::read_dir("/proc").ok()?;
    let mut argvs = Vec::new();
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let bytes = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
        let mut args: Vec<String> = bytes
            .split(|byte| *byte == 0)
            .filter(|chunk| !chunk.is_empty())
            .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
            .collect();
        if args.is_empty() {
            if let Ok(exe) = fs::read_link(format!("/proc/{pid}/exe")) {
                args.push(exe.to_string_lossy().into_owned());
            } else {
                continue;
            }
        }
        if !argv0_is_session_worker(args.first().map(String::as_str).unwrap_or(""))
            && !linux_exe_is_session_worker(pid)
        {
            continue;
        }
        argvs.push((pid, args));
    }
    Some(argvs)
}

fn ps_axww_session_worker_argvs() -> Vec<(u32, Vec<String>)> {
    let Ok(output) = Command::new("ps")
        .env("COLUMNS", "65535")
        .args(["-axww", "-o", "pid=,command="])
        .output()
    else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let mut argvs = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let line = line.trim();
        let mut parts = line.split_whitespace();
        let Some(pid_token) = parts.next() else {
            continue;
        };
        let Ok(pid) = pid_token.parse::<u32>() else {
            continue;
        };
        let args: Vec<String> = parts.map(str::to_string).collect();
        if args.is_empty() {
            continue;
        }
        argvs.push((pid, args));
    }
    argvs
}

fn argv0_is_session_worker(argv0: &str) -> bool {
    Path::new(argv0)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name == "botster-session-worker" || name.starts_with("botster-session-worker")
        })
}

#[cfg(target_os = "linux")]
fn linux_exe_is_session_worker(pid: u32) -> bool {
    fs::read_link(format!("/proc/{pid}/exe"))
        .ok()
        .is_some_and(|exe| argv0_is_session_worker(&exe.to_string_lossy()))
}

fn exact_data_dir_path(arg: &str, data_dir: &Path) -> bool {
    let arg_path = Path::new(arg);
    if arg_path == data_dir {
        return true;
    }
    match (fs::canonicalize(arg_path), fs::canonicalize(data_dir)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn descendant_pids(root: u32) -> Vec<u32> {
    let Ok(output) = Command::new("ps").args(["-axo", "pid=,ppid="]).output() else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let mut edges: Vec<(u32, u32)> = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut parts = line.split_whitespace();
        let Some(pid) = parts.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let Some(ppid) = parts.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        edges.push((pid, ppid));
    }
    let mut owned = vec![root];
    let mut changed = true;
    while changed {
        changed = false;
        for &(pid, ppid) in &edges {
            if owned.contains(&ppid) && !owned.contains(&pid) {
                owned.push(pid);
                changed = true;
            }
        }
    }
    owned
}

fn signal_matched_pid(pid: u32, signal: libc::c_int) {
    if pid <= 1 {
        return;
    }
    let raw = pid as libc::pid_t;
    let our_pgid = unsafe { libc::getpgid(0) };
    let pgid = unsafe { libc::getpgid(raw) };
    if pgid == raw && pgid > 1 && pgid != our_pgid {
        let _ = unsafe { libc::killpg(raw, signal) };
        return;
    }
    let _ = unsafe { libc::kill(raw, signal) };
}

fn reap_orphaned_session_workers() {
    reap_session_worker_pids(session_worker_pids_orphaned);
}

fn reap_named_session_workers() {
    reap_session_worker_pids(session_worker_pids_named);
}

fn session_worker_pids_named() -> Vec<u32> {
    session_worker_argvs()
        .into_iter()
        .filter_map(|(pid, args)| {
            let argv0 = args.first()?;
            argv0_is_session_worker(argv0).then_some(pid)
        })
        .collect()
}

fn reap_session_workers_for_data_dir(data_dir: &Path, data_dir_arg: &Path) {
    reap_session_worker_pids(|| session_worker_pids_for_data_dir(data_dir, data_dir_arg));
}

fn reap_session_worker_pids(census: impl Fn() -> Vec<u32>) {
    let started = Instant::now();
    loop {
        let pids = census();
        if pids.is_empty() {
            return;
        }
        let signal = libc::SIGKILL;
        let mut targets = pids;
        for pid in targets.clone() {
            targets.extend(descendant_pids(pid));
        }
        targets.sort_unstable();
        targets.dedup();
        for pid in targets {
            signal_matched_pid(pid, signal);
        }
        if started.elapsed() >= Duration::from_secs(15) {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub(crate) fn cleanup_child(child: &mut Child) -> Result<(), IsolatedHubError> {
    let pid = child.id();
    let mut child_reaped = child.try_wait().ok().flatten().is_some();
    signal_child_group_or_child(pid, libc::SIGTERM)?;
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        child_reaped |= child.try_wait().ok().flatten().is_some();
        if child_reaped && !child_process_group_exists(pid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    signal_child_group_or_child(pid, libc::SIGKILL)?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        child_reaped |= child.try_wait().ok().flatten().is_some();
        if child_reaped && !child_process_group_exists(pid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(IsolatedHubError::CleanupTimeout { pid })
}

fn child_process_group_exists(pid: u32) -> bool {
    if unsafe { libc::killpg(pid as libc::pid_t, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

fn signal_child_group_or_child(pid: u32, signal: libc::c_int) -> Result<(), IsolatedHubError> {
    if unsafe { libc::killpg(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let group_error = std::io::Error::last_os_error();
    if group_error.raw_os_error() != Some(libc::ESRCH) {
        return Err(IsolatedHubError::CleanupChild {
            pid,
            source: group_error,
        });
    }
    if unsafe { libc::kill(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let child_error = std::io::Error::last_os_error();
    if child_error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(IsolatedHubError::CleanupChild {
            pid,
            source: child_error,
        })
    }
}

fn collect_child_output(child: &mut Child) -> (String, String) {
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_string(&mut stdout);
    }
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    (stdout, stderr)
}

fn prepend_worker_dir_to_path(command: &mut Command, session_worker_bin: &Path) {
    let Some(worker_dir) = session_worker_bin.parent() else {
        return;
    };
    let mut paths = vec![worker_dir.to_path_buf()];
    if let Some(current_path) = env::var_os("PATH") {
        paths.extend(env::split_paths(&current_path));
    }
    if let Ok(joined) = env::join_paths(paths) {
        command.env("PATH", joined);
    }
}

fn sanitize_segment(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

pub(crate) fn default_socket_name() -> &'static str {
    DEFAULT_SOCKET_NAME
}
