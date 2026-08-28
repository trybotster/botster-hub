//! Isolated hub process harness.
//!
//! Owns IsolatedHubBuilder, IsolatedHub, IsolatedHubError, readiness, cleanup,
//! socket paths, and child-process helpers. Root re-exports stay stable.

use std::cell::Cell;
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_hub_client::{DaemonDiagnostic, DaemonEndpoint, DaemonRequest, DaemonResponseKind};

use crate::ConformanceFailureClass;

const DEFAULT_SOCKET_NAME: &str = "botster-hub.sock";
const READY_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_COMMAND_BUDGET: Duration = Duration::from_secs(5);
const HUB_CHILD_WAIT_BUDGET: Duration = Duration::from_secs(5);
const FREEZE_CONFIRM_BUDGET: Duration = Duration::from_millis(2_500);
const REAP_CEILING: Duration = Duration::from_secs(10);
const WHOLE_PATH_BUDGET: Duration = Duration::from_millis(22_500);
const REAP_SIGKILL_AFTER: Duration = Duration::from_millis(400);
const REAP_POLL: Duration = Duration::from_millis(50);
const WAIT_POLL: Duration = Duration::from_millis(20);
const DRAIN_CAP: usize = 64 * 1024;
const SESSION_WORKER_BASENAME: &str = "botster-session-worker";

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
        let _guard = isolated_hub_start_guard();
        if let Some(taint) = current_taint() {
            return Err(IsolatedHubError::Tainted {
                pgid: taint.pgid,
                data_dir: taint.data_dir,
            });
        }
        run_after_taint_check_hook();
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
        let hub_pid = child.id();

        if let Err(error) = wait_for_ready(&endpoint, &mut child, self.ready_timeout) {
            let _ = cleanup_child(&mut child);
            let _ = reap_owned_session_workers(
                hub_pid,
                None,
                &TeardownBudget::new(),
                &IsolatedHubSeams::default(),
            );
            remove_data_dir_path(&data_dir)?;
            return Err(error);
        }

        Ok(IsolatedHub {
            hub_bin,
            data_dir,
            working_directory,
            endpoint,
            child: Some(child),
            hub_pid,
            lifecycle: IsolatedHubLifecycle::Pending,
            seams: IsolatedHubSeams::default(),
            drop_retry_used: false,
            teardown_started: None,
            unconfirmed_deadline: None,
            stop_polls: Cell::new(0),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IsolatedHubLifecycle {
    Pending,
    Completed,
    QuiescenceUnconfirmed,
}

#[derive(Debug, Clone, Default)]
struct IsolatedHubSeams {
    force_quiescence_unconfirmed: bool,
    skip_freeze: bool,
    skip_stop_confirmation: bool,
    reuse_remembered_set_on_drop_retry: bool,
    restart_budget_on_drop_retry: bool,
    fail_census: bool,
    skip_captured_reap: bool,
    panic_after_freeze: bool,
    remembered_set: Option<OwnedSet>,
}

/// Running isolated hub daemon. Drop attempts shutdown, then kills on failure.
///
/// Successful explicit shutdown removes the harness-owned data directory. Drop
/// also removes it after the child exits, except during panic unwinding so the
/// failing daemon state remains available for diagnosis.
pub struct IsolatedHub {
    pub(crate) hub_bin: PathBuf,
    pub(crate) data_dir: PathBuf,
    pub(crate) working_directory: PathBuf,
    pub(crate) endpoint: DaemonEndpoint,
    pub(crate) child: Option<Child>,
    pub(crate) hub_pid: u32,
    lifecycle: IsolatedHubLifecycle,
    seams: IsolatedHubSeams,
    drop_retry_used: bool,
    teardown_started: Option<Instant>,
    unconfirmed_deadline: Option<Instant>,
    stop_polls: Cell<u32>,
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

    /// Hub-child pid captured at spawn. Equals the owned process-group id.
    #[must_use]
    pub const fn hub_child_pid(&self) -> u32 {
        self.hub_pid
    }

    /// Live session workers in this instance's Hub-child process group.
    #[must_use]
    pub fn owned_session_worker_pids(&self) -> Vec<u32> {
        census_sample()
            .unwrap_or_default()
            .into_iter()
            .filter(|row| row.is_owned_worker(self.hub_pid) && !row.is_zombie())
            .map(|row| row.pid)
            .collect()
    }

    /// Live descendants of owned session workers, including the workers.
    #[must_use]
    pub fn owned_live_descendant_pids(&self) -> Vec<u32> {
        let Ok(sample) = census_sample() else {
            return Vec::new();
        };
        owned_set_from_sample(&sample, self.hub_pid)
            .live_signal_targets(&sample)
            .into_iter()
            .collect()
    }

    /// Force the next freeze path to treat quiescence as unconfirmed.
    pub fn force_quiescence_unconfirmed(&mut self) {
        self.seams.force_quiescence_unconfirmed = true;
    }

    #[cfg(test)]
    pub(crate) fn skip_freeze(&mut self) {
        self.seams.skip_freeze = true;
    }

    #[cfg(test)]
    pub(crate) fn skip_stop_confirmation(&mut self) {
        self.seams.skip_stop_confirmation = true;
    }

    #[cfg(test)]
    pub(crate) fn reuse_remembered_set_on_drop_retry(&mut self) {
        self.seams.reuse_remembered_set_on_drop_retry = true;
    }

    #[cfg(test)]
    pub(crate) fn restart_budget_on_drop_retry(&mut self) {
        self.seams.restart_budget_on_drop_retry = true;
    }

    #[cfg(test)]
    pub(crate) fn fail_census(&mut self) {
        self.seams.fail_census = true;
    }

    #[cfg(test)]
    pub(crate) fn skip_captured_reap(&mut self) {
        self.seams.skip_captured_reap = true;
    }

    #[cfg(test)]
    pub(crate) fn panic_after_freeze(&mut self) {
        self.seams.panic_after_freeze = true;
    }

    #[cfg(test)]
    pub(crate) fn expire_residual_deadline(&mut self) {
        self.unconfirmed_deadline = Some(Instant::now());
        if self.lifecycle == IsolatedHubLifecycle::Pending {
            self.lifecycle = IsolatedHubLifecycle::QuiescenceUnconfirmed;
        }
    }

    #[cfg(test)]
    pub(crate) fn stop_polls(&self) -> u32 {
        self.stop_polls.get()
    }

    /// Stop the daemon, wait for its process, then remove its owned data directory.
    pub fn shutdown(mut self) -> Result<(), IsolatedHubError> {
        self.shutdown_inner()
    }

    #[cfg(test)]
    pub(crate) fn from_running_child(
        hub_bin: PathBuf,
        data_dir: PathBuf,
        working_directory: PathBuf,
        child: Child,
    ) -> Self {
        let hub_pid = child.id();
        Self {
            hub_bin,
            endpoint: DaemonEndpoint::new(data_dir.join(default_socket_name())),
            data_dir,
            working_directory,
            child: Some(child),
            hub_pid,
            lifecycle: IsolatedHubLifecycle::Pending,
            seams: IsolatedHubSeams::default(),
            drop_retry_used: false,
            teardown_started: None,
            unconfirmed_deadline: None,
            stop_polls: Cell::new(0),
        }
    }

    pub(crate) fn shutdown_inner(&mut self) -> Result<(), IsolatedHubError> {
        match self.lifecycle {
            IsolatedHubLifecycle::Completed => return self.remove_data_dir(),
            IsolatedHubLifecycle::QuiescenceUnconfirmed => {
                return Err(IsolatedHubError::QuiescenceUnconfirmed {
                    pgid: self.hub_pid as i32,
                    data_dir: self.data_dir.clone(),
                });
            }
            IsolatedHubLifecycle::Pending => {}
        }
        let started = Instant::now();
        self.teardown_started = Some(started);
        let budget = TeardownBudget {
            deadline: started + WHOLE_PATH_BUDGET,
        };
        self.shutdown_inner_with_budget(budget)
    }

    fn shutdown_inner_with_budget(
        &mut self,
        budget: TeardownBudget,
    ) -> Result<(), IsolatedHubError> {
        if self.child.is_none() {
            self.lifecycle = IsolatedHubLifecycle::Completed;
            return self.remove_data_dir();
        }

        let hub_pid = self.hub_pid;
        let live_snapshot = match census_sample() {
            Ok(sample) => owned_set_from_sample(&sample, hub_pid),
            Err(error) => {
                let hub_child = self.child.take().expect("child exists after shutdown");
                return self.unconfirmed_child_handoff(
                    hub_child,
                    None,
                    None,
                    budget,
                    error,
                    OwnedSet::default(),
                );
            }
        };
        self.seams.remembered_set = Some(live_snapshot.clone());

        let mut hub_child = self.child.take().expect("child exists after shutdown");

        let shutdown_deadline = budget.phase_deadline(SHUTDOWN_COMMAND_BUDGET);
        let shutdown = run_shutdown_command(
            &self.hub_bin,
            &self.data_dir,
            &self.working_directory,
            shutdown_deadline,
        );

        let shutdown_outcome = match shutdown {
            Ok(outcome) => outcome,
            Err(error) => {
                self.child = Some(hub_child);
                return Err(error);
            }
        };

        if shutdown_outcome.timed_out {
            let hub_stdout = take_drain(hub_child.stdout.take());
            let hub_stderr = take_drain(hub_child.stderr.take());
            return self.timeout_teardown(
                hub_child,
                hub_stdout,
                hub_stderr,
                live_snapshot,
                TeardownPhase::ShutdownCommand,
                budget,
                shutdown_outcome,
            );
        }

        if !shutdown_outcome.succeeded && !shutdown_outcome.disconnected_during_shutdown {
            self.child = Some(hub_child);
            return Err(IsolatedHubError::ShutdownFailed {
                stderr: shutdown_outcome.stderr,
            });
        }

        let hub_stdout = take_drain(hub_child.stdout.take());
        let hub_stderr = take_drain(hub_child.stderr.take());

        let hub_deadline = budget.phase_deadline(HUB_CHILD_WAIT_BUDGET);
        match wait_child_bounded(&mut hub_child, hub_deadline) {
            Ok(Some(status)) => {
                let stdout = join_drain(hub_stdout);
                let stderr = join_drain(hub_stderr);
                if status.success() {
                    if let Err(error) = reap_owned_session_workers(
                        hub_pid,
                        Some(&live_snapshot),
                        &budget,
                        &self.seams,
                    ) {
                        record_taint(hub_pid as i32, self.data_dir.clone());
                        self.lifecycle = IsolatedHubLifecycle::QuiescenceUnconfirmed;
                        return Err(error);
                    }
                    self.lifecycle = IsolatedHubLifecycle::Completed;
                    if shutdown_outcome.succeeded {
                        self.remove_data_dir()
                    } else {
                        Err(IsolatedHubError::ShutdownFailed {
                            stderr: shutdown_outcome.stderr,
                        })
                    }
                } else {
                    if let Err(error) = reap_owned_session_workers(
                        hub_pid,
                        Some(&live_snapshot),
                        &budget,
                        &self.seams,
                    ) {
                        record_taint(hub_pid as i32, self.data_dir.clone());
                        self.lifecycle = IsolatedHubLifecycle::QuiescenceUnconfirmed;
                        return Err(error);
                    }
                    self.lifecycle = IsolatedHubLifecycle::Completed;
                    Err(IsolatedHubError::DaemonExited {
                        status: status.to_string(),
                        stdout,
                        stderr,
                    })
                }
            }
            Ok(None) => self.timeout_teardown(
                hub_child,
                hub_stdout,
                hub_stderr,
                live_snapshot,
                TeardownPhase::HubChildWait,
                budget,
                shutdown_outcome,
            ),
            Err(source) => {
                self.child = Some(hub_child);
                let _ = join_drain(hub_stdout);
                let _ = join_drain(hub_stderr);
                Err(IsolatedHubError::Wait { source })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn timeout_teardown(
        &mut self,
        mut hub_child: Child,
        hub_stdout: Option<JoinHandle<Vec<u8>>>,
        hub_stderr: Option<JoinHandle<Vec<u8>>>,
        live_snapshot: OwnedSet,
        phase: TeardownPhase,
        budget: TeardownBudget,
        shutdown_outcome: ShutdownOutcome,
    ) -> Result<(), IsolatedHubError> {
        let hub_pid = self.hub_pid;
        match freeze_confirm_snapshot(hub_pid, &budget, &mut self.seams, &self.stop_polls) {
            Ok(confirmed) => {
                let _ = signal_owned_hub_group(hub_pid, libc::SIGKILL);
                let reap_deadline = Instant::now() + Duration::from_secs(1);
                let reap_deadline = if reap_deadline < budget.deadline {
                    reap_deadline
                } else {
                    budget.deadline
                };
                let _ = wait_child_bounded(&mut hub_child, reap_deadline);
                let stdout = join_drain(hub_stdout);
                let stderr = join_drain(hub_stderr);
                if let Err(error) =
                    reap_owned_session_workers(hub_pid, Some(&confirmed), &budget, &self.seams)
                {
                    record_taint(hub_pid as i32, self.data_dir.clone());
                    self.lifecycle = IsolatedHubLifecycle::QuiescenceUnconfirmed;
                    return Err(error);
                }
                self.lifecycle = IsolatedHubLifecycle::Completed;
                let _ = (stdout, stderr, shutdown_outcome, live_snapshot);
                Err(IsolatedHubError::TeardownTimeout { phase })
            }
            Err(error) => self.unconfirmed_child_handoff(
                hub_child,
                hub_stdout,
                hub_stderr,
                budget,
                error,
                live_snapshot,
            ),
        }
    }

    fn unconfirmed_child_handoff(
        &mut self,
        mut hub_child: Child,
        hub_stdout: Option<JoinHandle<Vec<u8>>>,
        hub_stderr: Option<JoinHandle<Vec<u8>>>,
        budget: TeardownBudget,
        cause: IsolatedHubError,
        _live_snapshot: OwnedSet,
    ) -> Result<(), IsolatedHubError> {
        let _ = hub_child.kill();
        let reap_deadline = Instant::now() + Duration::from_secs(1);
        let reap_deadline = if reap_deadline < budget.deadline {
            reap_deadline
        } else {
            budget.deadline
        };
        let _ = wait_child_bounded(&mut hub_child, reap_deadline);
        let _ = join_drain(hub_stdout);
        let _ = join_drain(hub_stderr);
        record_taint(self.hub_pid as i32, self.data_dir.clone());
        self.unconfirmed_deadline = Some(budget.deadline);
        self.lifecycle = IsolatedHubLifecycle::QuiescenceUnconfirmed;
        match cause {
            IsolatedHubError::QuiescenceUnconfirmed { .. } => {
                Err(IsolatedHubError::QuiescenceUnconfirmed {
                    pgid: self.hub_pid as i32,
                    data_dir: self.data_dir.clone(),
                })
            }
            other => Err(other),
        }
    }

    fn drop_retry_confirmed_cleanup(&mut self, budget: TeardownBudget) {
        let hub_pid = self.hub_pid;
        let mut seams = if self.seams.reuse_remembered_set_on_drop_retry {
            self.seams.clone()
        } else {
            IsolatedHubSeams {
                remembered_set: None,
                reuse_remembered_set_on_drop_retry: false,
                ..self.seams.clone()
            }
        };
        if let Ok(confirmed) =
            freeze_confirm_snapshot(hub_pid, &budget, &mut seams, &self.stop_polls)
        {
            let _ = signal_owned_hub_group(hub_pid, libc::SIGKILL);
            if reap_owned_session_workers(hub_pid, Some(&confirmed), &budget, &seams).is_ok() {
                self.lifecycle = IsolatedHubLifecycle::Completed;
            }
        }
    }

    fn remove_data_dir(&self) -> Result<(), IsolatedHubError> {
        remove_data_dir_path(&self.data_dir)
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
        match self.lifecycle {
            IsolatedHubLifecycle::Completed => {}
            IsolatedHubLifecycle::QuiescenceUnconfirmed => {
                if self.drop_retry_used {
                    return;
                }
                self.drop_retry_used = true;
                let deadline = if self.seams.restart_budget_on_drop_retry {
                    Instant::now() + WHOLE_PATH_BUDGET
                } else {
                    self.unconfirmed_deadline
                        .unwrap_or_else(|| Instant::now() + WHOLE_PATH_BUDGET)
                };
                if Instant::now() >= deadline {
                    return;
                }
                self.drop_retry_confirmed_cleanup(TeardownBudget { deadline });
            }
            IsolatedHubLifecycle::Pending => {
                if thread::panicking() {
                    let mut child_cleaned = true;
                    if let Some(child) = self.child.as_mut() {
                        child_cleaned = cleanup_child(child).is_ok();
                    }
                    self.child = None;
                    if child_cleaned {
                        let _ = reap_owned_session_workers(
                            self.hub_pid,
                            None,
                            &TeardownBudget::new(),
                            &self.seams,
                        );
                    }
                    return;
                }
                if self.shutdown_inner().is_ok() {
                    return;
                }
                let mut child_cleaned = true;
                if let Some(child) = self.child.as_mut() {
                    child_cleaned = cleanup_child(child).is_ok();
                }
                self.child = None;
                if child_cleaned {
                    let _ = reap_owned_session_workers(
                        self.hub_pid,
                        None,
                        &TeardownBudget::new(),
                        &self.seams,
                    );
                    let _ = self.remove_data_dir();
                }
            }
        }
    }
}

/// Phase of IsolatedHub teardown that exhausted its budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownPhase {
    ShutdownCommand,
    HubChildWait,
    FreezeConfirm,
    Reap,
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
    TeardownTimeout {
        phase: TeardownPhase,
    },
    QuiescenceUnconfirmed {
        pgid: i32,
        data_dir: PathBuf,
    },
    Tainted {
        pgid: i32,
        data_dir: PathBuf,
    },
    CensusFailed {
        reason: &'static str,
    },
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
            Self::TeardownTimeout { phase } => {
                write!(
                    formatter,
                    "isolated hub teardown timed out during {phase:?}"
                )
            }
            Self::QuiescenceUnconfirmed { pgid, .. } => {
                write!(
                    formatter,
                    "isolated hub quiescence was not confirmed for pgid {pgid}; descendants may remain"
                )
            }
            Self::Tainted { pgid, .. } => {
                write!(
                    formatter,
                    "isolated hub start refused because fixture pgid {pgid} is tainted"
                )
            }
            Self::CensusFailed { reason } => {
                write!(formatter, "isolated hub process census failed: {reason}")
            }
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
            Self::TeardownTimeout { .. } => "isolated hub teardown exceeded its budget",
            Self::QuiescenceUnconfirmed { .. } => {
                "isolated hub teardown could not confirm owned-group quiescence"
            }
            Self::Tainted { .. } => {
                "isolated hub start refused because a previous teardown left the fixture tainted"
            }
            Self::CensusFailed { .. } => "isolated hub process census failed",
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
            | Self::ShutdownFailed { .. }
            | Self::TeardownTimeout { .. }
            | Self::QuiescenceUnconfirmed { .. }
            | Self::Tainted { .. }
            | Self::CensusFailed { .. } => None,
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

#[cfg(test)]
pub(crate) fn process_pgid(pid: u32) -> Option<u32> {
    let output = Command::new("ps")
        .args(["-o", "pgid=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

struct TeardownBudget {
    deadline: Instant,
}

impl TeardownBudget {
    fn new() -> Self {
        Self {
            deadline: Instant::now() + WHOLE_PATH_BUDGET,
        }
    }

    fn phase_deadline(&self, cap: Duration) -> Instant {
        let phase = Instant::now() + cap;
        if phase < self.deadline {
            phase
        } else {
            self.deadline
        }
    }
}

struct IsolatedHubTaint {
    pgid: i32,
    data_dir: PathBuf,
}

static ISOLATED_HUB_TAINT: Mutex<Option<IsolatedHubTaint>> = Mutex::new(None);
static ISOLATED_HUB_START_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static AFTER_TAINT_CHECK_HOOK: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

thread_local! {
    static START_GUARD_DEPTH: Cell<u32> = const { Cell::new(0) };
}

static BYPASS_START_GUARD: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn isolated_hub_start_lock() -> &'static Mutex<()> {
    ISOLATED_HUB_START_LOCK.get_or_init(|| Mutex::new(()))
}

/// Reentrant IsolatedHub start/taint guard.
pub struct IsolatedHubStartGuard {
    _inner: Option<MutexGuard<'static, ()>>,
}

impl Drop for IsolatedHubStartGuard {
    fn drop(&mut self) {
        START_GUARD_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Acquire the IsolatedHub start guard. Tests that mutate taint must hold this
/// for the visible interval.
#[must_use]
pub fn isolated_hub_start_guard() -> IsolatedHubStartGuard {
    START_GUARD_DEPTH.with(|depth| {
        if depth.get() == 0 {
            if BYPASS_START_GUARD.load(std::sync::atomic::Ordering::SeqCst) {
                return IsolatedHubStartGuard { _inner: None };
            }
            let inner = isolated_hub_start_lock()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            depth.set(1);
            IsolatedHubStartGuard {
                _inner: Some(inner),
            }
        } else {
            depth.set(depth.get() + 1);
            IsolatedHubStartGuard { _inner: None }
        }
    })
}

#[cfg(test)]
pub(crate) fn bypass_isolated_hub_start_guard(bypass: bool) {
    BYPASS_START_GUARD.store(bypass, std::sync::atomic::Ordering::SeqCst);
}

#[cfg(test)]
pub(crate) fn inject_isolated_hub_taint(pgid: i32, data_dir: PathBuf) {
    record_taint(pgid, data_dir);
}

#[cfg(test)]
pub(crate) fn set_after_taint_check_hook(hook: Box<dyn FnOnce() + Send>) {
    *AFTER_TAINT_CHECK_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
}

fn run_after_taint_check_hook() {
    let hook = AFTER_TAINT_CHECK_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    if let Some(hook) = hook {
        hook();
    }
}

fn current_taint() -> Option<IsolatedHubTaint> {
    match ISOLATED_HUB_TAINT.lock() {
        Ok(guard) => guard.as_ref().map(|taint| IsolatedHubTaint {
            pgid: taint.pgid,
            data_dir: taint.data_dir.clone(),
        }),
        Err(poisoned) => poisoned
            .into_inner()
            .as_ref()
            .map(|taint| IsolatedHubTaint {
                pgid: taint.pgid,
                data_dir: taint.data_dir.clone(),
            }),
    }
}

fn record_taint(pgid: i32, data_dir: PathBuf) {
    let _start = isolated_hub_start_guard();
    let mut guard = ISOLATED_HUB_TAINT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = Some(IsolatedHubTaint { pgid, data_dir });
}

/// Current IsolatedHub fixture taint, if any.
#[must_use]
pub fn isolated_hub_taint() -> Option<(i32, PathBuf)> {
    current_taint().map(|taint| (taint.pgid, taint.data_dir))
}

/// Clear IsolatedHub fixture taint. Tests must call this after asserting taint.
pub fn clear_isolated_hub_taint() {
    let _start = isolated_hub_start_guard();
    let mut guard = ISOLATED_HUB_TAINT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = None;
}

#[derive(Debug, Clone)]
struct ProcessRow {
    pid: u32,
    ppid: u32,
    pgid: u32,
    stat: String,
    command: String,
}

fn path_basename(token: &str) -> Option<&str> {
    Path::new(token).file_name().and_then(|name| name.to_str())
}

fn command_names_session_worker(command: &str) -> bool {
    let mut tokens = command.split_whitespace();
    let Some(argv0) = tokens.next() else {
        return false;
    };
    if path_basename(argv0) == Some(SESSION_WORKER_BASENAME) {
        return true;
    }
    matches!(path_basename(argv0), Some("sh" | "bash" | "dash"))
        && tokens
            .next()
            .is_some_and(|argv1| path_basename(argv1) == Some(SESSION_WORKER_BASENAME))
}

impl ProcessRow {
    fn is_owned_worker(&self, hub_pid: u32) -> bool {
        self.pgid == hub_pid && command_names_session_worker(&self.command)
    }

    fn is_zombie(&self) -> bool {
        self.stat.contains('Z')
    }

    fn is_stopped(&self) -> bool {
        self.stat.starts_with('T')
    }

    fn in_group(&self, hub_pid: u32) -> bool {
        self.pgid == hub_pid
    }
}

fn census_sample() -> Result<Vec<ProcessRow>, IsolatedHubError> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,ppid=,pgid=,stat=,command="])
        .output()
        .map_err(|_| IsolatedHubError::CensusFailed {
            reason: "ps spawn failed",
        })?;
    if !output.status.success() {
        return Err(IsolatedHubError::CensusFailed {
            reason: "ps exited nonzero",
        });
    }
    let mut rows = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let line = line.trim();
        let mut parts = line.split_whitespace();
        let Some(pid) = parts.next().and_then(|value| value.parse().ok()) else {
            continue;
        };
        let Some(ppid) = parts.next().and_then(|value| value.parse().ok()) else {
            continue;
        };
        let Some(pgid) = parts.next().and_then(|value| value.parse().ok()) else {
            continue;
        };
        let Some(stat) = parts.next() else {
            continue;
        };
        let command = parts.collect::<Vec<_>>().join(" ");
        rows.push(ProcessRow {
            pid,
            ppid,
            pgid,
            stat: stat.to_string(),
            command,
        });
    }
    Ok(rows)
}

#[derive(Debug, Clone, Default)]
struct OwnedSet {
    workers: Vec<u32>,
    descendants: Vec<u32>,
}

impl OwnedSet {
    fn live_signal_targets(&self, sample: &[ProcessRow]) -> HashSet<u32> {
        let allowed: HashSet<u32> = self
            .workers
            .iter()
            .chain(self.descendants.iter())
            .copied()
            .collect();
        sample
            .iter()
            .filter(|row| allowed.contains(&row.pid) && !row.is_zombie())
            .map(|row| row.pid)
            .collect()
    }
}

fn owned_set_from_sample(sample: &[ProcessRow], hub_pid: u32) -> OwnedSet {
    let workers: Vec<u32> = sample
        .iter()
        .filter(|row| row.is_owned_worker(hub_pid))
        .map(|row| row.pid)
        .collect();
    let mut descendants = workers.clone();
    let mut changed = true;
    while changed {
        changed = false;
        for row in sample {
            if descendants.contains(&row.ppid) && !descendants.contains(&row.pid) {
                descendants.push(row.pid);
                changed = true;
            }
        }
    }
    OwnedSet {
        workers,
        descendants,
    }
}

fn group_exists(hub_pid: u32) -> bool {
    child_process_group_exists(hub_pid)
}

fn group_quiescent(sample: &[ProcessRow], hub_pid: u32) -> Result<bool, IsolatedHubError> {
    let members: Vec<&ProcessRow> = sample.iter().filter(|row| row.in_group(hub_pid)).collect();
    if members.is_empty() && group_exists(hub_pid) {
        return Err(IsolatedHubError::CensusFailed {
            reason: "live process group missing from census",
        });
    }
    Ok(members
        .iter()
        .filter(|row| !row.is_zombie())
        .all(|row| row.is_stopped()))
}

struct FreezeGuard {
    pgid: u32,
    discharged: bool,
}

impl FreezeGuard {
    fn arm(pgid: u32) -> Result<Self, IsolatedHubError> {
        signal_owned_hub_group(pgid, libc::SIGSTOP)?;
        Ok(Self {
            pgid,
            discharged: false,
        })
    }

    fn discharge(&mut self) {
        self.discharged = true;
    }
}

impl Drop for FreezeGuard {
    fn drop(&mut self) {
        if !self.discharged {
            let _ = signal_owned_hub_group(self.pgid, libc::SIGCONT);
            self.discharged = true;
        }
    }
}

fn signal_owned_hub_group(pgid: u32, signal: libc::c_int) -> Result<(), IsolatedHubError> {
    if pgid <= 1 {
        return Ok(());
    }
    let raw = pgid as libc::pid_t;
    let our_pgid = unsafe { libc::getpgid(0) };
    if raw == our_pgid {
        return Err(IsolatedHubError::CleanupChild {
            pid: pgid,
            source: std::io::Error::other("refusing to signal the test harness process group"),
        });
    }
    if unsafe { libc::killpg(raw, signal) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(IsolatedHubError::CleanupChild {
            pid: pgid,
            source: error,
        })
    }
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

fn freeze_confirm_snapshot(
    hub_pid: u32,
    budget: &TeardownBudget,
    seams: &mut IsolatedHubSeams,
    stop_polls: &Cell<u32>,
) -> Result<OwnedSet, IsolatedHubError> {
    if seams.fail_census {
        return Err(IsolatedHubError::CensusFailed {
            reason: "injected census failure",
        });
    }
    if seams.skip_freeze {
        let set = if let Some(remembered) = seams.remembered_set.clone() {
            remembered
        } else {
            owned_set_from_sample(&census_sample()?, hub_pid)
        };
        signal_owned_hub_group(hub_pid, libc::SIGKILL)?;
        return Ok(set);
    }
    if seams.reuse_remembered_set_on_drop_retry
        && let Some(remembered) = seams.remembered_set.clone()
    {
        return Ok(remembered);
    }
    let mut guard = FreezeGuard::arm(hub_pid)?;
    if seams.panic_after_freeze {
        panic!("injected freeze-guard panic");
    }
    if seams.force_quiescence_unconfirmed {
        return Err(IsolatedHubError::QuiescenceUnconfirmed {
            pgid: hub_pid as i32,
            data_dir: PathBuf::new(),
        });
    }
    let confirm_deadline = budget.phase_deadline(FREEZE_CONFIRM_BUDGET);
    if seams.skip_stop_confirmation {
        let sample = census_sample()?;
        let owned = owned_set_from_sample(&sample, hub_pid);
        signal_owned_hub_group(hub_pid, libc::SIGKILL)?;
        guard.discharge();
        return Ok(owned);
    }
    loop {
        let sample = census_sample()?;
        stop_polls.set(stop_polls.get().saturating_add(1));
        if group_quiescent(&sample, hub_pid)? {
            let owned = owned_set_from_sample(&sample, hub_pid);
            signal_owned_hub_group(hub_pid, libc::SIGKILL)?;
            guard.discharge();
            return Ok(owned);
        }
        if Instant::now() >= confirm_deadline {
            return Err(IsolatedHubError::QuiescenceUnconfirmed {
                pgid: hub_pid as i32,
                data_dir: PathBuf::new(),
            });
        }
        thread::sleep(WAIT_POLL);
    }
}

fn reap_owned_session_workers(
    hub_pid: u32,
    captured: Option<&OwnedSet>,
    budget: &TeardownBudget,
    seams: &IsolatedHubSeams,
) -> Result<(), IsolatedHubError> {
    if seams.fail_census {
        return Err(IsolatedHubError::CensusFailed {
            reason: "injected census failure",
        });
    }
    let started = Instant::now();
    let reap_deadline = budget.phase_deadline(REAP_CEILING);
    loop {
        let sample = census_sample()?;
        let current = owned_set_from_sample(&sample, hub_pid);
        let mut allowed: HashSet<u32> = current.workers.iter().copied().collect();
        if !seams.skip_captured_reap {
            allowed.extend(current.descendants.iter().copied());
            if let Some(captured) = captured {
                allowed.extend(captured.workers.iter().copied());
                allowed.extend(captured.descendants.iter().copied());
            }
        }
        let live: Vec<u32> = sample
            .iter()
            .filter(|row| allowed.contains(&row.pid) && !row.is_zombie())
            .map(|row| row.pid)
            .collect();
        if live.is_empty() {
            return Ok(());
        }
        let signal = if started.elapsed() >= REAP_SIGKILL_AFTER {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        for pid in live {
            signal_matched_pid(pid, signal);
        }
        if Instant::now() >= reap_deadline {
            return Ok(());
        }
        thread::sleep(REAP_POLL);
    }
}

fn take_drain(pipe: Option<impl Read + Send + 'static>) -> Option<JoinHandle<Vec<u8>>> {
    pipe.map(|mut reader| {
        thread::spawn(move || {
            let mut stored = Vec::new();
            let mut buf = [0_u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        let room = DRAIN_CAP.saturating_sub(stored.len());
                        stored.extend_from_slice(&buf[..n.min(room)]);
                    }
                    Err(_) => break,
                }
            }
            stored
        })
    })
}

fn join_drain(handle: Option<JoinHandle<Vec<u8>>>) -> String {
    handle
        .and_then(|handle| handle.join().ok())
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_default()
}

fn wait_child_bounded(
    child: &mut Child,
    deadline: Instant,
) -> Result<Option<ExitStatus>, std::io::Error> {
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        thread::sleep(WAIT_POLL);
    }
}

struct ShutdownOutcome {
    succeeded: bool,
    disconnected_during_shutdown: bool,
    timed_out: bool,
    stderr: String,
}

fn run_shutdown_command(
    hub_bin: &Path,
    data_dir: &Path,
    working_directory: &Path,
    deadline: Instant,
) -> Result<ShutdownOutcome, IsolatedHubError> {
    let mut command = Command::new(hub_bin);
    command
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .current_dir(working_directory)
        .env("BOTSTER_ENV", "test")
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
    let mut child = command
        .spawn()
        .map_err(|source| IsolatedHubError::ShutdownCommand { source })?;
    let stdout = take_drain(child.stdout.take());
    let stderr = take_drain(child.stderr.take());
    match wait_child_bounded(&mut child, deadline) {
        Ok(Some(status)) => {
            let _stdout = join_drain(stdout);
            let stderr = join_drain(stderr);
            let disconnected_during_shutdown =
                stderr.trim() == "botster-hub shutdown error: client disconnected";
            Ok(ShutdownOutcome {
                succeeded: status.success(),
                disconnected_during_shutdown,
                timed_out: false,
                stderr,
            })
        }
        Ok(None) => {
            let pid = child.id();
            let _ = signal_owned_hub_group(pid, libc::SIGKILL);
            let _ = child.kill();
            let _ = wait_child_bounded(&mut child, Instant::now() + Duration::from_secs(1));
            let _stdout = join_drain(stdout);
            let stderr = join_drain(stderr);
            Ok(ShutdownOutcome {
                succeeded: false,
                disconnected_during_shutdown: false,
                timed_out: true,
                stderr,
            })
        }
        Err(source) => {
            let pid = child.id();
            let _ = signal_owned_hub_group(pid, libc::SIGKILL);
            let _ = child.kill();
            let _ = join_drain(stdout);
            let _ = join_drain(stderr);
            Err(IsolatedHubError::ShutdownCommand { source })
        }
    }
}
