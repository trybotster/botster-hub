//! Isolated daemon harness for external `botster-hub-client` integration tests.
//!
//! This crate intentionally depends only on the client protocol crate and starts
//! the `botster-hub` binary as a subprocess. Downstream tests must supply the
//! hub and session-worker binary paths explicitly, or via `BOTSTER_HUB_BIN` and
//! `BOTSTER_SESSION_WORKER_BIN`.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_hub_client::{
    DaemonCompatibilityRequirement, DaemonEndpoint, DaemonOperatorError, DaemonRequest,
    DaemonResponse, DaemonResponseKind, DaemonTransportError, ensure_compatible,
};

const CONFORMANCE_SESSION_ID: &str = "botster-conformance-session";
const CONFORMANCE_SUBSCRIPTION_ID: &str = "botster-conformance-subscription";
const CONFORMANCE_READY: &str = "conformance-ready";
const CONFORMANCE_ECHO: &str = "echo:from-conformance";
const CONFORMANCE_WINSIZE_PREFIX: &str = "winsize:";
const PROJECT_PIPELINES_PACKAGE: &str = "project-pipelines";
const PROJECT_PIPELINES_SURFACE: &str = "project-pipelines.create-ticket";
const PROJECT_PIPELINES_ACTION: &str = "project_pipelines.create_ticket";

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
    name: String,
    ready_timeout: Duration,
}

impl Default for IsolatedHubBuilder {
    fn default() -> Self {
        Self {
            hub_bin: None,
            session_worker_bin: None,
            root: None,
            name: "external-client".to_string(),
            ready_timeout: READY_TIMEOUT,
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

    /// Set a stable label segment for the disposable data directory.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    #[cfg(test)]
    fn ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// Start the isolated daemon and wait for the real socket protocol to respond.
    pub fn start(self) -> Result<IsolatedHub, IsolatedHubError> {
        let data_dir = self.data_dir()?;
        let hub_bin = explicit_path(self.hub_bin, "BOTSTER_HUB_BIN")?;
        let session_worker_bin =
            explicit_path(self.session_worker_bin, "BOTSTER_SESSION_WORKER_BIN")?;
        ensure_file("botster-hub binary", &hub_bin)?;
        ensure_file("botster-session-worker binary", &session_worker_bin)?;

        fs::create_dir_all(&data_dir).map_err(|source| IsolatedHubError::CreateDataDir {
            path: data_dir.clone(),
            source,
        })?;
        let endpoint = DaemonEndpoint::new(data_dir.join(DEFAULT_SOCKET_NAME));

        let mut command = Command::new(&hub_bin);
        command
            .arg("start")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--session-worker-bin")
            .arg(&session_worker_bin)
            .env("BOTSTER_ENV", "test")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        prepend_worker_dir_to_path(&mut command, &session_worker_bin);

        let mut child = command.spawn().map_err(|source| IsolatedHubError::Spawn {
            path: hub_bin.clone(),
            source,
        })?;

        if let Err(error) = wait_for_ready(&endpoint, &mut child, self.ready_timeout) {
            cleanup_child(&mut child);
            return Err(error);
        }

        Ok(IsolatedHub {
            hub_bin,
            data_dir,
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
pub struct IsolatedHub {
    hub_bin: PathBuf,
    data_dir: PathBuf,
    endpoint: DaemonEndpoint,
    child: Option<Child>,
}

impl IsolatedHub {
    /// Client endpoint for this isolated daemon socket.
    #[must_use]
    pub const fn endpoint(&self) -> &DaemonEndpoint {
        &self.endpoint
    }

    /// Disposable data directory owned by this harness instance.
    #[must_use]
    pub const fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Stop the daemon through the operator command and wait for the process.
    pub fn shutdown(mut self) -> Result<(), IsolatedHubError> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> Result<(), IsolatedHubError> {
        if self.child.is_none() {
            return Ok(());
        }
        let output = Command::new(&self.hub_bin)
            .arg("shutdown")
            .arg("--data-dir")
            .arg(&self.data_dir)
            .env("BOTSTER_ENV", "test")
            .output()
            .map_err(|source| IsolatedHubError::ShutdownCommand { source })?;
        if !output.status.success() {
            return Err(IsolatedHubError::ShutdownFailed {
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            });
        }

        let child = self.child.take().expect("child exists after shutdown");
        let output = child
            .wait_with_output()
            .map_err(|source| IsolatedHubError::Wait { source })?;
        if output.status.success() {
            Ok(())
        } else {
            Err(IsolatedHubError::DaemonExited {
                status: output.status.to_string(),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            })
        }
    }
}

/// Stable observation returned by [`run_client_conformance`].
///
/// The report intentionally excludes raw event ordering and timing-dependent
/// daemon details so downstream client CI can compare it across repeated runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConformanceReport {
    pub lifecycle_state: String,
    pub compatibility_protocol: String,
    pub compatibility_protocol_version: u16,
    pub compatibility_features: Vec<String>,
    pub compatibility_conformance_fixture_revision: u16,
    pub initial_session_count: usize,
    pub session_id: String,
    pub spawned_lifecycle: String,
    pub stream_contains_ready: bool,
    pub stream_contains_echo: bool,
    pub stream_contains_resize: bool,
    pub validation_error_code: String,
    pub validation_error_operation: String,
}

/// Stable observation returned by [`run_project_pipelines_conformance`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectPipelinesConformanceReport {
    pub package_state: String,
    pub surface_kind: String,
    pub surface_id: String,
    pub invalid_action_status: String,
    pub invalid_title_error: String,
}

/// Run the hub-owned conformance flow for same-device external clients.
///
/// The flow starts from an already isolated hub, then exercises status, session
/// list, spawn, attach/drain through `botster_hub_client::stream_attach`, input,
/// resize, validation error handling, and session teardown using only public
/// `botster-hub-client` calls.
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
///     .start()
///     .expect("isolated hub starts");
///
/// let report = run_client_conformance(&hub).expect("client conformance");
/// assert_eq!(report.lifecycle_state, "running");
/// assert_eq!(report.validation_error_operation, "drain_runtime");
/// assert!(report.stream_contains_resize);
/// hub.shutdown().expect("shutdown isolated hub");
/// ```
pub fn run_client_conformance(
    hub: &IsolatedHub,
) -> Result<ClientConformanceReport, ConformanceError> {
    let status = request(hub.endpoint(), DaemonRequest::Status, "status")?;
    expect_kind(&status, DaemonResponseKind::Status, "status")?;
    let status = status.status.ok_or(ConformanceError::MissingBody {
        operation: "status",
        field: "status",
    })?;
    ensure_compatible(
        &DaemonCompatibilityRequirement::current(),
        &status.compatibility,
    )
    .map_err(|source| ConformanceError::Client {
        operation: "compatibility",
        source: DaemonTransportError::Compatibility(source),
    })?;

    let list = request(hub.endpoint(), DaemonRequest::ListSessions, "list_sessions")?;
    expect_kind(&list, DaemonResponseKind::Sessions, "list_sessions")?;
    let initial_session_count = list.sessions.len();

    let spawn = request(
        hub.endpoint(),
        DaemonRequest::Spawn {
            session_id: CONFORMANCE_SESSION_ID.to_string(),
            command: format!(
                "printf '{CONFORMANCE_READY}\\n'; while IFS= read -r line; do if [ \"$line\" = size-check ]; then printf '{CONFORMANCE_WINSIZE_PREFIX}%s\\n' \"$(stty size)\"; elif [ \"$line\" = quit ]; then printf 'conformance-bye\\n'; exit 0; else printf 'echo:%s\\n' \"$line\"; fi; done"
            ),
        },
        "spawn",
    )?;
    expect_kind(&spawn, DaemonResponseKind::Spawned, "spawn")?;
    let spawned_lifecycle = spawn
        .sessions
        .iter()
        .find(|session| session.session_id == CONFORMANCE_SESSION_ID)
        .map(|session| session.lifecycle.clone())
        .ok_or(ConformanceError::MissingSession {
            session_id: CONFORMANCE_SESSION_ID.to_string(),
        })?;

    let endpoint = hub.endpoint().clone();
    let attach_handle = thread::spawn(move || {
        let mut output = Vec::new();
        botster_hub_client::stream_attach(
            &endpoint,
            CONFORMANCE_SESSION_ID,
            CONFORMANCE_SUBSCRIPTION_ID,
            &mut output,
        )?;
        Ok::<_, DaemonTransportError>(output)
    });
    // `stream_attach` writes the initial attach events and then drains the
    // session, so late-arriving output is still captured; this just gives the
    // subscription a head start before we drive input.
    thread::sleep(Duration::from_millis(100));

    expect_kind(
        &request(
            hub.endpoint(),
            DaemonRequest::Resize {
                session_id: CONFORMANCE_SESSION_ID.to_string(),
                rows: 33,
                cols: 102,
            },
            "resize",
        )?,
        DaemonResponseKind::Events,
        "resize",
    )?;
    expect_kind(
        &request(
            hub.endpoint(),
            DaemonRequest::SendInput {
                session_id: CONFORMANCE_SESSION_ID.to_string(),
                data: "from-conformance\n".to_string(),
            },
            "send_input",
        )?,
        DaemonResponseKind::Events,
        "send_input",
    )?;
    expect_kind(
        &request(
            hub.endpoint(),
            DaemonRequest::SendInput {
                session_id: CONFORMANCE_SESSION_ID.to_string(),
                data: "size-check\n".to_string(),
            },
            "send_size_check",
        )?,
        DaemonResponseKind::Events,
        "send_size_check",
    )?;
    expect_kind(
        &request(
            hub.endpoint(),
            DaemonRequest::SendInput {
                session_id: CONFORMANCE_SESSION_ID.to_string(),
                data: "quit\n".to_string(),
            },
            "send_quit",
        )?,
        DaemonResponseKind::Events,
        "send_quit",
    )?;

    let output = attach_handle
        .join()
        .map_err(|_| ConformanceError::AttachThreadPanicked)?
        .map_err(|source| ConformanceError::Client {
            operation: "stream_attach",
            source,
        })?;
    let output = String::from_utf8_lossy(&output).to_string();
    let stream_contains_ready = output.contains(CONFORMANCE_READY);
    let stream_contains_echo = output.contains(CONFORMANCE_ECHO);
    let resize_needle = format!("{CONFORMANCE_WINSIZE_PREFIX}33 102");
    let stream_contains_resize = output.contains(&resize_needle);
    if !stream_contains_ready {
        return Err(ConformanceError::MissingOutput {
            needle: CONFORMANCE_READY,
            output,
        });
    }
    if !stream_contains_echo {
        return Err(ConformanceError::MissingOutput {
            needle: CONFORMANCE_ECHO,
            output,
        });
    }
    if !stream_contains_resize {
        return Err(ConformanceError::MissingOutput {
            needle: "winsize:33 102",
            output,
        });
    }

    let validation = request(
        hub.endpoint(),
        DaemonRequest::Drain {
            session_id: "missing-conformance-session".to_string(),
        },
        "validation_error",
    )?;
    expect_kind(
        &validation,
        DaemonResponseKind::OperatorError,
        "validation_error",
    )?;
    let validation_error = validation.error.ok_or(ConformanceError::MissingBody {
        operation: "validation_error",
        field: "error",
    })?;

    let _ = request(
        hub.endpoint(),
        DaemonRequest::ShutdownSession {
            session_id: CONFORMANCE_SESSION_ID.to_string(),
        },
        "shutdown_session",
    );

    Ok(ClientConformanceReport {
        lifecycle_state: status.lifecycle_state,
        compatibility_protocol: status.compatibility.protocol,
        compatibility_protocol_version: status.compatibility.protocol_version,
        compatibility_features: status.compatibility.features,
        compatibility_conformance_fixture_revision: status
            .compatibility
            .conformance_fixture_revision,
        initial_session_count,
        session_id: CONFORMANCE_SESSION_ID.to_string(),
        spawned_lifecycle,
        stream_contains_ready,
        stream_contains_echo,
        stream_contains_resize,
        validation_error_code: validation_error.code,
        validation_error_operation: validation_error.operation,
    })
}

/// Run the public Project Pipelines surface/action validation subflow.
///
/// Callers pass a package checkout path, usually `examples/project-pipelines`
/// from this repository checkout. The helper enables the package through the
/// daemon and exercises render/action dispatch without linking plugin internals.
///
/// # Example
///
/// ```no_run
/// use botster_hub_test_support::{run_project_pipelines_conformance, IsolatedHubBuilder};
///
/// let hub = IsolatedHubBuilder::new()
///     .hub_bin(std::env::var("BOTSTER_HUB_BIN").expect("BOTSTER_HUB_BIN"))
///     .session_worker_bin(
///         std::env::var("BOTSTER_SESSION_WORKER_BIN").expect("BOTSTER_SESSION_WORKER_BIN"),
///     )
///     .start()
///     .expect("isolated hub starts");
///
/// let plugin_report = run_project_pipelines_conformance(&hub, "examples/project-pipelines")
///     .expect("project pipelines conformance");
/// assert_eq!(plugin_report.package_state, "enabled");
/// assert_eq!(plugin_report.surface_id, "project-pipelines-create-panel");
/// assert_eq!(plugin_report.invalid_action_status, "failure");
/// assert_eq!(plugin_report.invalid_title_error, "Title is required");
/// hub.shutdown().expect("shutdown isolated hub");
/// ```
pub fn run_project_pipelines_conformance(
    hub: &IsolatedHub,
    package_path: impl Into<PathBuf>,
) -> Result<ProjectPipelinesConformanceReport, ConformanceError> {
    let enabled = request(
        hub.endpoint(),
        DaemonRequest::EnablePackageLocalPath {
            path: package_path.into(),
        },
        "enable_project_pipelines",
    )?;
    expect_kind(
        &enabled,
        DaemonResponseKind::PackageDecision,
        "enable_project_pipelines",
    )?;
    let package_state = enabled
        .package_decision
        .ok_or(ConformanceError::MissingBody {
            operation: "enable_project_pipelines",
            field: "package_decision",
        })?
        .state;

    let surface = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceRender {
            package_name: PROJECT_PIPELINES_PACKAGE.to_string(),
            surface_id: PROJECT_PIPELINES_SURFACE.to_string(),
            payload: serde_json::json!({}),
        },
        "project_pipelines_surface",
    )?;
    expect_kind(
        &surface,
        DaemonResponseKind::PluginSurface,
        "project_pipelines_surface",
    )?;
    let surface = surface
        .plugin_surface
        .ok_or(ConformanceError::MissingBody {
            operation: "project_pipelines_surface",
            field: "plugin_surface",
        })?;
    let surface_kind = value_string(&surface, "type", "project_pipelines_surface")?;
    let surface_id = value_string(&surface, "id", "project_pipelines_surface")?;

    let invalid = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PROJECT_PIPELINES_PACKAGE.to_string(),
            surface_id: PROJECT_PIPELINES_SURFACE.to_string(),
            action_id: PROJECT_PIPELINES_ACTION.to_string(),
            payload: serde_json::json!({
                "request_id": "invalid-project-pipelines-conformance",
                "title": "   ",
                "pipeline_id": "local_pipeline",
            }),
        },
        "project_pipelines_invalid_action",
    )?;
    expect_kind(
        &invalid,
        DaemonResponseKind::PluginActionResult,
        "project_pipelines_invalid_action",
    )?;
    let invalid = invalid
        .plugin_action_result
        .ok_or(ConformanceError::MissingBody {
            operation: "project_pipelines_invalid_action",
            field: "plugin_action_result",
        })?;
    let invalid_action_status = action_status_string(&invalid, "project_pipelines_invalid_action")?;
    let invalid_title_error = field_error_string(
        &invalid,
        "project-pipelines-create-title",
        "project_pipelines_invalid_action",
    )?;

    Ok(ProjectPipelinesConformanceReport {
        package_state,
        surface_kind,
        surface_id,
        invalid_action_status,
        invalid_title_error,
    })
}

fn request(
    endpoint: &DaemonEndpoint,
    request: DaemonRequest,
    operation: &'static str,
) -> Result<DaemonResponse, ConformanceError> {
    botster_hub_client::request(endpoint, request)
        .map_err(|source| ConformanceError::Client { operation, source })
}

fn expect_kind(
    response: &DaemonResponse,
    expected: DaemonResponseKind,
    operation: &'static str,
) -> Result<(), ConformanceError> {
    if response.kind == expected {
        Ok(())
    } else {
        Err(ConformanceError::UnexpectedKind {
            operation,
            expected,
            actual: response.kind,
            error: response.error.clone(),
        })
    }
}

fn value_string(
    value: &serde_json::Value,
    field: &'static str,
    operation: &'static str,
) -> Result<String, ConformanceError> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField { operation, field })
}

fn action_status_string(
    value: &serde_json::Value,
    operation: &'static str,
) -> Result<String, ConformanceError> {
    if let Some(status) = value.get("status").and_then(serde_json::Value::as_str) {
        return Ok(status.to_string());
    }
    match value.get("state").and_then(serde_json::Value::as_str) {
        Some("accepted") => Ok("success".to_string()),
        Some("rejected" | "error") => Ok("failure".to_string()),
        Some(state) => Ok(state.to_string()),
        None => Err(ConformanceError::MissingJsonField {
            operation,
            field: "status",
        }),
    }
}

fn field_error_string(
    value: &serde_json::Value,
    field_id: &'static str,
    operation: &'static str,
) -> Result<String, ConformanceError> {
    let error = value
        .get("payload")
        .and_then(|payload| payload.get("field_errors"))
        .and_then(|errors| errors.get(field_id))
        .or_else(|| {
            value
                .get("field_errors")
                .and_then(|errors| errors.get(field_id))
        })
        .ok_or(ConformanceError::MissingJsonField {
            operation,
            field: "field_errors",
        })?;

    if let Some(message) = error.as_str() {
        return Ok(message.to_string());
    }
    error
        .as_array()
        .and_then(|messages| messages.first())
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField {
            operation,
            field: "field_errors.message",
        })
}

/// Errors returned by published conformance fixture flows.
#[derive(Debug)]
pub enum ConformanceError {
    Client {
        operation: &'static str,
        source: DaemonTransportError,
    },
    UnexpectedKind {
        operation: &'static str,
        expected: DaemonResponseKind,
        actual: DaemonResponseKind,
        error: Option<DaemonOperatorError>,
    },
    MissingBody {
        operation: &'static str,
        field: &'static str,
    },
    MissingJsonField {
        operation: &'static str,
        field: &'static str,
    },
    MissingSession {
        session_id: String,
    },
    MissingOutput {
        needle: &'static str,
        output: String,
    },
    AttachThreadPanicked,
}

impl fmt::Display for ConformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client { operation, source } => {
                write!(formatter, "{operation} request failed: {source}")
            }
            Self::UnexpectedKind {
                operation,
                expected,
                actual,
                error,
            } => {
                write!(
                    formatter,
                    "{operation} returned {actual:?}, expected {expected:?}"
                )?;
                if let Some(error) = error {
                    write!(formatter, ": {} {}", error.code, error.message)?;
                }
                Ok(())
            }
            Self::MissingBody { operation, field } => {
                write!(formatter, "{operation} response missing {field}")
            }
            Self::MissingJsonField { operation, field } => {
                write!(formatter, "{operation} response missing JSON field {field}")
            }
            Self::MissingSession { session_id } => {
                write!(formatter, "spawn response missing session {session_id}")
            }
            Self::MissingOutput { needle, output } => {
                write!(formatter, "stream output missing {needle:?}: {output:?}")
            }
            Self::AttachThreadPanicked => write!(formatter, "stream attach thread panicked"),
        }
    }
}

impl Error for ConformanceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Client { source, .. } => Some(source),
            Self::UnexpectedKind { .. }
            | Self::MissingBody { .. }
            | Self::MissingJsonField { .. }
            | Self::MissingSession { .. }
            | Self::MissingOutput { .. }
            | Self::AttachThreadPanicked => None,
        }
    }
}

impl Drop for IsolatedHub {
    fn drop(&mut self) {
        if self.shutdown_inner().is_ok() {
            return;
        }
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }
}

/// Errors returned by the isolated daemon harness.
#[derive(Debug)]
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

impl Error for IsolatedHubError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CreateDataDir { source, .. }
            | Self::Spawn { source, .. }
            | Self::ShutdownCommand { source }
            | Self::Wait { source } => Some(source),
            Self::Clock(source) => Some(source),
            Self::MissingBinaryEnv { .. }
            | Self::MissingBinary { .. }
            | Self::ReadyTimeout { .. }
            | Self::DaemonExited { .. }
            | Self::ShutdownFailed { .. } => None,
        }
    }
}

fn explicit_path(
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

fn cleanup_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }
    let _ = child.kill();
    let _ = child.wait();
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

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn explicit_path_reports_missing_hub_binary_env() {
        let error = explicit_path(None, "__BOTSTER_HUB_TEST_MISSING_BIN")
            .expect_err("missing hub env returns an error");

        assert!(matches!(
            error,
            IsolatedHubError::MissingBinaryEnv {
                variable: "__BOTSTER_HUB_TEST_MISSING_BIN"
            }
        ));
    }

    #[test]
    fn explicit_path_reports_missing_session_worker_binary_env() {
        let error = explicit_path(None, "__BOTSTER_SESSION_WORKER_TEST_MISSING_BIN")
            .expect_err("missing worker env returns an error");

        assert!(matches!(
            error,
            IsolatedHubError::MissingBinaryEnv {
                variable: "__BOTSTER_SESSION_WORKER_TEST_MISSING_BIN"
            }
        ));
    }

    #[test]
    fn start_reports_missing_hub_binary_path() {
        let root = unique_root("missing-hub-path");
        let worker = existing_file(&root, "botster-session-worker");
        let missing_hub = root.join("missing-botster-hub");

        let error = IsolatedHubBuilder::new()
            .hub_bin(&missing_hub)
            .session_worker_bin(worker)
            .root(&root)
            .start_error();

        assert!(matches!(
            error,
            IsolatedHubError::MissingBinary {
                label: "botster-hub binary",
                path
            } if path == missing_hub
        ));
    }

    #[test]
    fn start_reports_missing_session_worker_binary_path() {
        let root = unique_root("missing-worker-path");
        let hub = existing_file(&root, "botster-hub");
        let missing_worker = root.join("missing-botster-session-worker");

        let error = IsolatedHubBuilder::new()
            .hub_bin(hub)
            .session_worker_bin(&missing_worker)
            .root(&root)
            .start_error();

        assert!(matches!(
            error,
            IsolatedHubError::MissingBinary {
                label: "botster-session-worker binary",
                path
            } if path == missing_worker
        ));
    }

    #[cfg(unix)]
    #[test]
    fn start_reports_daemon_exit_before_readiness() {
        let root = unique_root("daemon-exit");
        let worker = existing_file(&root, "botster-session-worker");
        let hub = executable_script(
            &root,
            "botster-hub",
            r#"#!/bin/sh
printf 'fake hub stdout\n'
printf 'fake hub stderr\n' >&2
exit 42
"#,
        );

        let error = IsolatedHubBuilder::new()
            .hub_bin(hub)
            .session_worker_bin(worker)
            .root(&root)
            .ready_timeout(Duration::from_millis(500))
            .start_error();

        assert!(matches!(
            error,
            IsolatedHubError::DaemonExited {
                status,
                stdout,
                stderr
            } if status.contains("42")
                && stdout.contains("fake hub stdout")
                && stderr.contains("fake hub stderr")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn start_timeout_cleans_up_unready_child() {
        let root = unique_root("ready-timeout");
        let worker = existing_file(&root, "botster-session-worker");
        let pid_file = root.join("fake-hub.pid");
        let script = format!(
            r#"#!/bin/sh
printf '%s\n' "$$" > '{}'
while :; do
  sleep 1
done
"#,
            pid_file.display()
        );
        let hub = executable_script(&root, "botster-hub", &script);

        let error = IsolatedHubBuilder::new()
            .hub_bin(hub)
            .session_worker_bin(worker)
            .root(&root)
            .ready_timeout(Duration::from_secs(1))
            .start_error();

        assert!(matches!(error, IsolatedHubError::ReadyTimeout { .. }));
        let pid = read_fake_pid(&pid_file);
        assert_process_exits(pid);
    }

    fn unique_root(name: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        let root = env::temp_dir()
            .join("botster-hub-test-support")
            .join(format!("{name}-{}-{now}", std::process::id()));
        fs::create_dir_all(&root).expect("create unique test root");
        root
    }

    trait StartErrorExt {
        fn start_error(self) -> IsolatedHubError;
    }

    impl StartErrorExt for IsolatedHubBuilder {
        fn start_error(self) -> IsolatedHubError {
            match self.start() {
                Ok(_) => panic!("isolated hub start unexpectedly succeeded"),
                Err(error) => error,
            }
        }
    }

    fn existing_file(root: &Path, name: &str) -> PathBuf {
        let path = root.join(name);
        fs::write(&path, b"").expect("write fake binary placeholder");
        path
    }

    #[cfg(unix)]
    fn executable_script(root: &Path, name: &str, body: &str) -> PathBuf {
        let path = root.join(name);
        let mut file = fs::File::create(&path).expect("create fake executable");
        file.write_all(body.as_bytes())
            .expect("write fake executable");
        let mut permissions = file
            .metadata()
            .expect("read fake executable metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).expect("mark fake executable executable");
        path
    }

    #[cfg(unix)]
    fn read_fake_pid(pid_file: &Path) -> u32 {
        fs::read_to_string(pid_file)
            .expect("read fake pid")
            .trim()
            .parse()
            .expect("fake pid is numeric")
    }

    #[cfg(unix)]
    fn assert_process_exits(pid: u32) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if !process_exists(pid) {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("process {pid} still exists after readiness timeout cleanup");
    }

    #[cfg(unix)]
    fn process_exists(pid: u32) -> bool {
        Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
}
