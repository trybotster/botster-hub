//! Isolated daemon harness for external `botster-hub-client` integration tests.
//!
//! This crate intentionally depends only on the client protocol crate and starts
//! the `botster-hub` binary as a subprocess. Downstream tests must supply the
//! hub and session-worker binary paths explicitly, or via `BOTSTER_HUB_BIN` and
//! `BOTSTER_SESSION_WORKER_BIN`.

use std::collections::BTreeMap;
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
    DaemonCompatibility, DaemonCompatibilityRequirement, DaemonDiagnostic, DaemonDiagnosticKind,
    DaemonEndpoint, DaemonEvent, DaemonOperatorError, DaemonRequest, DaemonResponse,
    DaemonResponseKind, DaemonTransportError, ensure_compatible,
};
use serde::{Deserialize, Serialize};

const CONFORMANCE_SESSION_ID: &str = "botster-conformance-session";
const CONFORMANCE_SUBSCRIPTION_ID: &str = "botster-conformance-subscription";
const CONFORMANCE_READY: &str = "conformance-ready";
const CONFORMANCE_ECHO: &str = "echo:from-conformance";
const CONFORMANCE_WINSIZE_PREFIX: &str = "winsize:";
const LATE_ATTACH_HISTORY_SESSION_ID: &str = "late-attach-history-fixture-session";
const LATE_ATTACH_HISTORY_SUBSCRIPTION_ID: &str = "late-attach-history-fixture-subscription";
const LATE_ATTACH_NO_HISTORY_SESSION_ID: &str = "late-attach-no-history-fixture-session";
const LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID: &str = "late-attach-no-history-fixture-subscription";
const LATE_ATTACH_HISTORY_DATA: &str = "history-before-live\r\n";
const LATE_ATTACH_LIVE_DATA: &str = "live-after-attach\r\n";
const LATE_ATTACH_NO_HISTORY_LIVE_DATA: &str = "live-without-history\r\n";
const PROJECT_PIPELINES_PACKAGE: &str = "project-pipelines";
const PROJECT_PIPELINES_SURFACE: &str = "project-pipelines.create-ticket";
const PROJECT_PIPELINES_ACTION: &str = "project_pipelines.create_ticket";
const PLUGIN_CONTRACT_MATRIX_PACKAGE: &str = "botster.plugin-contract-matrix";
const PLUGIN_CONTRACT_MATRIX_FIXTURE_ARTIFACT: &str = "fixtures/plugin-contract-matrix";
const DAEMON_PROTOCOL_TYPESCRIPT_ARTIFACT: &str =
    "crates/botster-hub-client/generated/daemon-protocol.ts";
const PLUGIN_CONTRACT_APP_SURFACE: &str = "contract.app";
const PLUGIN_CONTRACT_EMPTY_SURFACE: &str = "contract.empty";
const PLUGIN_CONTRACT_BLOCKED_SURFACE: &str = "contract.blocked";
const PLUGIN_CONTRACT_INVALID_BODY_SURFACE: &str = "contract.invalid_body";
const PLUGIN_CONTRACT_SETTINGS_SURFACE: &str = "contract.settings";
const PLUGIN_CONTRACT_ACTION: &str = "contract.action";
const SUPPORTED_PLUGIN_SURFACE_JSON_ACTIONS: &str = "plugin_surface_json_actions";
const UNSUPPORTED_PLUGIN_ENTITY_FRAMES: &str = "plugin_entity_frames";

const DEFAULT_SOCKET_NAME: &str = "botster-hub.sock";
const READY_TIMEOUT: Duration = Duration::from_secs(5);

/// Hub-owned support matrix for first-party same-device clients.
///
/// The matrix is intentionally published from test support instead of the hub
/// daemon runtime. Downstream TUI/browser tests can serialize this value to a
/// stable JSON fixture while production clients continue to rely on the daemon
/// compatibility descriptor and conformance flows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirstPartyClientSupportMatrix {
    pub protocol: String,
    pub protocol_version: u16,
    pub conformance_fixture_revision: u16,
    pub required_features: Vec<String>,
    pub supported_features: Vec<String>,
    pub diagnostic_kinds: Vec<String>,
    pub session_actions: Vec<String>,
    pub terminal_streaming: TerminalStreamingSupport,
    pub resize: ResizeSupport,
    pub plugin_surfaces: PluginSurfaceSupport,
    pub entity_actions: EntityActionSupport,
    pub late_attach_history: LateAttachHistorySupport,
    pub known_limitations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalStreamingSupport {
    pub supported: bool,
    pub feature: String,
    pub helper: String,
    pub held_open_stream: bool,
    pub conformance_ready_output: String,
    pub conformance_echo_output: String,
    pub missing_session_diagnostic_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResizeSupport {
    pub supported: bool,
    pub feature: String,
    pub action: String,
    pub conformance_output_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginSurfaceSupport {
    pub render_supported: bool,
    pub render_feature: String,
    pub action_supported: bool,
    pub action_feature: String,
    pub package_name: String,
    pub surface_id: String,
    pub rendered_surface_kind: String,
    pub rendered_surface_node_id: String,
    pub invalid_action_diagnostic_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityActionSupport {
    pub supported_capabilities: Vec<String>,
    pub unsupported_capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LateAttachHistorySupport {
    pub supported: bool,
    pub fixture_path: String,
    pub json_helper: String,
    pub event_type: String,
    pub runtime_regression: String,
}

/// Public client-shaped scenario for late terminal attach history rendering.
///
/// The events use [`botster_hub_client::DaemonEvent`] values only, so
/// downstream web/TUI tests can either consume this struct in Rust test code or
/// mirror the serde JSON emitted by [`late_attach_history_conformance_fixture_json`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LateAttachHistoryConformanceScenario {
    pub conformance_fixture_revision: u16,
    pub session_id: String,
    pub subscription_id: String,
    pub no_history_session_id: String,
    pub no_history_subscription_id: String,
    pub history_then_live: Vec<DaemonEvent>,
    pub no_history_then_live: Vec<DaemonEvent>,
}

/// A stable file published by `botster-hub-test-support` for downstream tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TestAssetFile {
    pub relative_path: &'static str,
    pub contents: &'static [u8],
}

/// Published plugin fixture asset set for cross-repo package conformance tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginContractMatrixFixtureAsset {
    pub package_name: &'static str,
    pub artifact_path: &'static str,
    pub files: &'static [TestAssetFile],
}

/// Published descriptor for the application-primitives surface inside the matrix fixture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationPrimitivesFixtureDescriptor {
    pub fixture_package_name: &'static str,
    pub artifact_path: &'static str,
    pub surface_id: &'static str,
    pub route_id: &'static str,
    pub renderer_entrypoint: &'static str,
    pub node_kinds: &'static [&'static str],
}

/// Published generated daemon protocol artifact for client drift checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonProtocolTypescriptArtifact {
    pub artifact_path: &'static str,
    pub contents: String,
}

static PLUGIN_CONTRACT_MATRIX_FIXTURE_ASSET_FILES: &[TestAssetFile] = &[
    TestAssetFile {
        relative_path: "README.md",
        contents: include_bytes!("../fixtures/plugin-contract-matrix/README.md"),
    },
    TestAssetFile {
        relative_path: "botster-package.json",
        contents: include_bytes!("../fixtures/plugin-contract-matrix/botster-package.json"),
    },
    TestAssetFile {
        relative_path: "plugin.lua",
        contents: include_bytes!("../fixtures/plugin-contract-matrix/plugin.lua"),
    },
];

const APPLICATION_PRIMITIVE_NODE_KINDS: &[&str] = &[
    "button",
    "button",
    "empty_state",
    "empty_state",
    "form",
    "metric",
    "metric_grid",
    "panel",
    "section",
    "status_badge",
    "table",
    "text_input",
    "toolbar",
];

/// Return the crate-managed plugin contract matrix fixture assets.
///
/// The repo-root `fixtures/plugins/plugin-contract-matrix` directory remains the
/// source of truth. Hub tests assert this published asset set has the same
/// recursive file list and byte contents.
#[must_use]
pub fn plugin_contract_matrix_fixture_asset() -> PluginContractMatrixFixtureAsset {
    PluginContractMatrixFixtureAsset {
        package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE,
        artifact_path: PLUGIN_CONTRACT_MATRIX_FIXTURE_ARTIFACT,
        files: PLUGIN_CONTRACT_MATRIX_FIXTURE_ASSET_FILES,
    }
}

/// Return the application-primitives descriptor published for downstream renderers.
#[must_use]
pub fn application_primitives_fixture_descriptor() -> ApplicationPrimitivesFixtureDescriptor {
    ApplicationPrimitivesFixtureDescriptor {
        fixture_package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE,
        artifact_path: PLUGIN_CONTRACT_MATRIX_FIXTURE_ARTIFACT,
        surface_id: PLUGIN_CONTRACT_APP_SURFACE,
        route_id: "surface:contract.app",
        renderer_entrypoint: "ui_tree_snapshot.body",
        node_kinds: APPLICATION_PRIMITIVE_NODE_KINDS,
    }
}

/// Copy the published plugin contract matrix fixture into a caller-owned directory.
///
/// The returned path is the copied package root. Pass it to
/// [`run_plugin_contract_matrix_conformance`] so tests never mutate crate source
/// or rely on a sibling hub checkout.
pub fn copy_plugin_contract_matrix_fixture(
    destination: impl AsRef<Path>,
) -> Result<PathBuf, std::io::Error> {
    let package_dir = destination
        .as_ref()
        .join(PLUGIN_CONTRACT_MATRIX_FIXTURE_ARTIFACT);
    fs::create_dir_all(&package_dir)?;
    for file in plugin_contract_matrix_fixture_asset().files {
        let path = package_dir.join(file.relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, file.contents)?;
    }
    Ok(package_dir)
}

/// Return the authoritative generated daemon TypeScript protocol artifact.
///
/// This is a convenience wrapper around `botster-hub-client`, which remains the
/// source of truth for protocol DTOs and TypeScript generation.
#[must_use]
pub fn daemon_protocol_typescript_artifact() -> DaemonProtocolTypescriptArtifact {
    DaemonProtocolTypescriptArtifact {
        artifact_path: DAEMON_PROTOCOL_TYPESCRIPT_ARTIFACT,
        contents: botster_hub_client::daemon_protocol_typescript(),
    }
}

/// Return the current first-party support matrix for downstream client tests.
#[must_use]
pub fn first_party_client_support_matrix() -> FirstPartyClientSupportMatrix {
    let compatibility = DaemonCompatibility::current();
    let requirement = DaemonCompatibilityRequirement::current();

    FirstPartyClientSupportMatrix {
        protocol: compatibility.protocol,
        protocol_version: compatibility.protocol_version,
        conformance_fixture_revision: compatibility.conformance_fixture_revision,
        required_features: requirement.required_features,
        supported_features: compatibility.features,
        diagnostic_kinds: daemon_diagnostic_kind_labels()
            .into_iter()
            .map(str::to_string)
            .collect(),
        session_actions: vec![
            "status".to_string(),
            "list_sessions".to_string(),
            "spawn".to_string(),
            "attach".to_string(),
            "drain".to_string(),
            "send_input".to_string(),
            "resize".to_string(),
            "shutdown_session".to_string(),
        ],
        terminal_streaming: TerminalStreamingSupport {
            supported: true,
            feature: botster_hub_client::FEATURE_TERMINAL_STREAMING.to_string(),
            helper: "botster_hub_client::stream_attach".to_string(),
            held_open_stream: true,
            conformance_ready_output: CONFORMANCE_READY.to_string(),
            conformance_echo_output: CONFORMANCE_ECHO.to_string(),
            missing_session_diagnostic_kind: diagnostic_kind_label(
                DaemonDiagnosticKind::TerminalStreamUnavailable,
            )
            .to_string(),
        },
        resize: ResizeSupport {
            supported: true,
            feature: botster_hub_client::FEATURE_RESIZE.to_string(),
            action: "resize".to_string(),
            conformance_output_prefix: CONFORMANCE_WINSIZE_PREFIX.to_string(),
        },
        plugin_surfaces: PluginSurfaceSupport {
            render_supported: true,
            render_feature: botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER.to_string(),
            action_supported: true,
            action_feature: botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION.to_string(),
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            surface_id: PLUGIN_CONTRACT_APP_SURFACE.to_string(),
            rendered_surface_kind: "panel".to_string(),
            rendered_surface_node_id: "contract-app-panel".to_string(),
            invalid_action_diagnostic_kind: diagnostic_kind_label(
                DaemonDiagnosticKind::ActionFailure,
            )
            .to_string(),
        },
        entity_actions: EntityActionSupport {
            supported_capabilities: vec![SUPPORTED_PLUGIN_SURFACE_JSON_ACTIONS.to_string()],
            unsupported_capabilities: vec![UNSUPPORTED_PLUGIN_ENTITY_FRAMES.to_string()],
        },
        late_attach_history: LateAttachHistorySupport {
            supported: true,
            fixture_path: "botster_hub_test_support::late_attach_history_conformance_scenario"
                .to_string(),
            json_helper: "botster_hub_test_support::late_attach_history_conformance_fixture_json"
                .to_string(),
            event_type: "botster_hub_client::DaemonEvent".to_string(),
            runtime_regression:
                "external_daemon_attach_replays_prior_history_with_renderable_byte_count"
                    .to_string(),
        },
        known_limitations: vec![
            "The matrix is a test/docs contract, not a daemon runtime endpoint.".to_string(),
            "Full plugin entity-frame hydration is intentionally outside this conformance fixture."
                .to_string(),
            "Clients own renderer-specific presentation policy for diagnostics.".to_string(),
        ],
    }
}

/// Return the typed late-attach history fixture for first-party client tests.
#[must_use]
pub fn late_attach_history_conformance_scenario() -> LateAttachHistoryConformanceScenario {
    LateAttachHistoryConformanceScenario {
        conformance_fixture_revision: botster_hub_client::CONFORMANCE_FIXTURE_REVISION,
        session_id: LATE_ATTACH_HISTORY_SESSION_ID.to_string(),
        subscription_id: LATE_ATTACH_HISTORY_SUBSCRIPTION_ID.to_string(),
        no_history_session_id: LATE_ATTACH_NO_HISTORY_SESSION_ID.to_string(),
        no_history_subscription_id: LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID.to_string(),
        history_then_live: late_attach_history_events(),
        no_history_then_live: late_attach_no_history_events(),
    }
}

/// Return the positive late-attach sequence: metadata, restored history, live bytes, exit.
#[must_use]
pub fn late_attach_history_events() -> Vec<DaemonEvent> {
    vec![
        DaemonEvent::AttachState {
            session_id: LATE_ATTACH_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_HISTORY_SUBSCRIPTION_ID.to_string(),
            state: "attached".to_string(),
        },
        DaemonEvent::Snapshot {
            session_id: LATE_ATTACH_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_HISTORY_SUBSCRIPTION_ID.to_string(),
            data: LATE_ATTACH_HISTORY_DATA.to_string(),
            bytes: LATE_ATTACH_HISTORY_DATA.len(),
        },
        DaemonEvent::TerminalOutput {
            session_id: LATE_ATTACH_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_HISTORY_SUBSCRIPTION_ID.to_string(),
            data: LATE_ATTACH_LIVE_DATA.to_string(),
        },
        DaemonEvent::ProcessExit {
            session_id: LATE_ATTACH_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_HISTORY_SUBSCRIPTION_ID.to_string(),
            code: Some(0),
        },
    ]
}

/// Return a late attach sequence where no restored history is fabricated.
#[must_use]
pub fn late_attach_no_history_events() -> Vec<DaemonEvent> {
    vec![
        DaemonEvent::AttachState {
            session_id: LATE_ATTACH_NO_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID.to_string(),
            state: "attached".to_string(),
        },
        DaemonEvent::TerminalOutput {
            session_id: LATE_ATTACH_NO_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID.to_string(),
            data: LATE_ATTACH_NO_HISTORY_LIVE_DATA.to_string(),
        },
        DaemonEvent::ProcessExit {
            session_id: LATE_ATTACH_NO_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID.to_string(),
            code: Some(0),
        },
    ]
}

/// Return stable serde JSON for downstream clients that mirror the fixture.
///
/// Browser-side tests should mirror this JSON shape rather than inventing
/// TypeScript-only event fields.
#[must_use]
pub fn late_attach_history_conformance_fixture_json() -> serde_json::Value {
    serde_json::to_value(late_attach_history_conformance_scenario())
        .expect("late attach history conformance fixture serializes")
}

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
    pub connected_diagnostic_operation: String,
    pub initial_session_count: usize,
    pub session_id: String,
    pub spawned_lifecycle: String,
    pub stream_contains_ready: bool,
    pub stream_contains_echo: bool,
    pub stream_contains_resize: bool,
    pub validation_error_code: String,
    pub validation_error_operation: String,
    pub validation_diagnostic_kind: String,
}

/// Stable observation returned by [`run_project_pipelines_conformance`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectPipelinesConformanceReport {
    pub package_state: String,
    pub rendered_package_name: String,
    pub rendered_surface_id: String,
    pub surface_kind: String,
    pub surface_id: String,
    pub surface_node_kinds: Vec<String>,
    pub form_node_id: String,
    pub form_node_kind: String,
    pub form_action_id: String,
    pub snapshot_package_name: String,
    pub snapshot_surface_id: String,
    pub snapshot_node_id: String,
    pub snapshot_node_kinds: Vec<String>,
    pub invalid_action_status: String,
    pub invalid_action_diagnostic_kind: String,
    pub invalid_title_error: String,
}

/// Stable observation returned by [`run_plugin_contract_matrix_conformance`].
///
/// Producer failures are returned as [`ConformanceError`] values with
/// [`ConformanceFailureClass::ProducerContract`]. Renderer/client failures are
/// intentionally downstream comparisons against the `client_render_*` fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginContractMatrixConformanceReport {
    pub package_name: String,
    pub installed_state: String,
    pub enabled_state: String,
    pub version: String,
    pub source_kind: String,
    pub surface_ids: Vec<String>,
    pub settings_surface_kind: String,
    pub settings_surface_supports: Vec<String>,
    pub app_route_path: String,
    pub app_route_target_kind: String,
    pub app_route_surface_id: String,
    pub app_route_enabled_after_install: bool,
    pub app_route_blocked_after_install: bool,
    pub enable_action_status_after_install: String,
    pub invalid_configuration_diagnostic_kind: String,
    pub invalid_configuration_diagnostic_operation: String,
    pub invalid_configuration_diagnostic_mentions_rejected_value: bool,
    pub valid_configuration_mode: String,
    pub valid_configuration_secret_state: String,
    pub list_state: String,
    pub list_surfaces_match_enabled: bool,
    pub show_state: String,
    pub show_routes_match_list: bool,
    pub settings_route_supports_settings: bool,
    pub app_surface_package_name: String,
    pub app_surface_id: String,
    pub app_surface_kind: String,
    pub app_surface_node_id: String,
    pub app_surface_node_kinds: Vec<String>,
    pub app_surface_snapshot_package_name: String,
    pub app_surface_snapshot_id: String,
    pub app_surface_snapshot_node_id: String,
    pub app_surface_snapshot_node_kinds: Vec<String>,
    pub empty_surface_node_id: String,
    pub empty_surface_child_id: String,
    pub blocked_render_error_code: String,
    pub blocked_render_operation: String,
    pub blocked_render_message_contains_failure: bool,
    pub invalid_body_error_code: String,
    pub invalid_body_operation: String,
    pub invalid_body_diagnostic_kind: String,
    pub invalid_body_diagnostic_operation: String,
    pub settings_surface_node_id: String,
    pub settings_text_contains_endpoint: bool,
    pub settings_text_contains_mode: bool,
    pub settings_text_contains_redacted_secret: bool,
    pub action_success_state: String,
    pub action_success_request_id: String,
    pub action_success_message: String,
    pub action_error_state: String,
    pub action_error_request_id: String,
    pub action_error_diagnostic_kind: String,
    pub action_error_diagnostic_operation: String,
    pub submit_action_id: String,
    pub action_field_error_state: String,
    pub action_field_error_request_id: String,
    pub action_field_error_diagnostic_kind: String,
    pub action_field_error_diagnostic_operation: String,
    pub action_field_error_message: String,
    pub client_render_check: PluginContractMatrixClientRenderCheck,
    pub failure_classes: PluginConformanceFailureClasses,
}

/// Fields downstream clients should compare against their renderer output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginContractMatrixClientRenderCheck {
    pub class: ConformanceFailureClass,
    pub app_surface_node_id: String,
    pub app_surface_node_kinds: Vec<String>,
    pub empty_surface_child_id: String,
    pub settings_surface_node_id: String,
    pub expected_redacted_secret_state: String,
}

/// Named classes exposed by the plugin UI conformance harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceFailureClass {
    ProducerContract,
    ClientRendering,
    EnvironmentSetup,
}

/// Stable labels for the three failure classes this harness distinguishes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginConformanceFailureClasses {
    pub producer_contract: ConformanceFailureClass,
    pub client_rendering: ConformanceFailureClass,
    pub environment_setup: ConformanceFailureClass,
}

/// Stable observation returned by [`run_foreground_terminal_app_open_conformance`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundTerminalAppOpenConformanceReport {
    pub package_state: String,
    pub package_name: String,
    pub app_id: String,
    pub entrypoint_id: String,
    pub app_kind: String,
    pub launch_mode: String,
    pub resolved_command: String,
    pub hub_socket_env_present: bool,
    pub hub_data_dir_env_present: bool,
    pub real_hub_action_operation: String,
    pub real_hub_action_result: String,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
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
    let connected_diagnostic_operation =
        diagnostic_operation(&status, DaemonDiagnosticKind::Connected, "status")?;
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
    let validation_diagnostic_kind = diagnostic_kind(
        &validation,
        DaemonDiagnosticKind::TerminalStreamUnavailable,
        "validation_error",
    )?;
    let validation_error = validation
        .error
        .as_ref()
        .ok_or(ConformanceError::MissingBody {
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
        connected_diagnostic_operation,
        initial_session_count,
        session_id: CONFORMANCE_SESSION_ID.to_string(),
        spawned_lifecycle,
        stream_contains_ready,
        stream_contains_echo,
        stream_contains_resize,
        validation_error_code: validation_error.code.clone(),
        validation_error_operation: validation_error.operation.clone(),
        validation_diagnostic_kind,
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
    if surface.package_name != PROJECT_PIPELINES_PACKAGE {
        return Err(ConformanceError::UnexpectedValue {
            operation: "project_pipelines_surface",
            field: "package_name",
            expected: PROJECT_PIPELINES_PACKAGE.to_string(),
            actual: surface.package_name,
        });
    }
    if surface.surface_id != PROJECT_PIPELINES_SURFACE {
        return Err(ConformanceError::UnexpectedValue {
            operation: "project_pipelines_surface",
            field: "surface_id",
            expected: PROJECT_PIPELINES_SURFACE.to_string(),
            actual: surface.surface_id,
        });
    }
    let surface_package_name = surface.package_name.clone();
    let rendered_surface_id = surface.surface_id.clone();
    let surface_kind = value_string(&surface.body, "type", "project_pipelines_surface")?;
    let surface_id = value_string(&surface.body, "id", "project_pipelines_surface")?;
    let surface_node_kinds = ui_node_type_values(&surface.body);
    let form_node = find_ui_node_by_id(&surface.body, "project-pipelines-create-form").ok_or(
        ConformanceError::MissingJsonField {
            operation: "project_pipelines_surface",
            field: "project-pipelines-create-form",
        },
    )?;
    let form_node_id = value_string(form_node, "id", "project_pipelines_surface")?;
    let form_node_kind = value_string(form_node, "type", "project_pipelines_surface")?;
    expect_value(
        "project_pipelines_surface",
        "form type",
        "form",
        &form_node_kind,
    )?;
    let form_action_id = ui_action_id(
        form_node,
        "project_pipelines_surface",
        "form.props.action.id",
    )?;
    expect_value(
        "project_pipelines_surface",
        "form.props.action.id",
        PROJECT_PIPELINES_ACTION,
        &form_action_id,
    )?;
    let snapshot = surface
        .ui_tree_snapshot
        .as_ref()
        .ok_or(ConformanceError::MissingBody {
            operation: "project_pipelines_surface",
            field: "plugin_surface.ui_tree_snapshot",
        })?;
    let snapshot_package_name = snapshot.package_name.clone();
    let snapshot_surface_id = snapshot.surface_id.clone();
    let snapshot_node_id = value_string(&snapshot.body, "id", "project_pipelines_surface")?;
    let snapshot_node_kinds = ui_node_type_values(&snapshot.body);
    if snapshot_package_name != PROJECT_PIPELINES_PACKAGE {
        return Err(ConformanceError::UnexpectedValue {
            operation: "project_pipelines_surface",
            field: "ui_tree_snapshot.package_name",
            expected: PROJECT_PIPELINES_PACKAGE.to_string(),
            actual: snapshot_package_name,
        });
    }
    if snapshot_surface_id != PROJECT_PIPELINES_SURFACE {
        return Err(ConformanceError::UnexpectedValue {
            operation: "project_pipelines_surface",
            field: "ui_tree_snapshot.surface_id",
            expected: PROJECT_PIPELINES_SURFACE.to_string(),
            actual: snapshot_surface_id,
        });
    }
    if snapshot_node_kinds != surface_node_kinds {
        return Err(ConformanceError::UnexpectedValue {
            operation: "project_pipelines_surface",
            field: "ui_tree_snapshot.body node kinds",
            expected: format!("{surface_node_kinds:?}"),
            actual: format!("{snapshot_node_kinds:?}"),
        });
    }

    let invalid = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PROJECT_PIPELINES_PACKAGE.to_string(),
            surface_id: PROJECT_PIPELINES_SURFACE.to_string(),
            action_id: form_action_id.clone(),
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
    let invalid_action_diagnostic_kind = diagnostic_kind(
        &invalid,
        DaemonDiagnosticKind::ActionFailure,
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
        rendered_package_name: surface_package_name,
        rendered_surface_id,
        surface_kind,
        surface_id,
        surface_node_kinds,
        form_node_id,
        form_node_kind,
        form_action_id,
        snapshot_package_name,
        snapshot_surface_id,
        snapshot_node_id,
        snapshot_node_kinds,
        invalid_action_status,
        invalid_action_diagnostic_kind,
        invalid_title_error,
    })
}

/// Run the reusable plugin UI conformance flow against the contract matrix fixture.
///
/// Callers should usually pass a package root returned by
/// [`copy_plugin_contract_matrix_fixture`]. An explicit package path remains
/// supported for local override tests.
/// The helper installs and enables that package through the daemon socket,
/// then verifies package descriptors, routes, render envelopes, action results,
/// configuration validation, and daemon responsiveness using only
/// `botster-hub-client` requests.
pub fn run_plugin_contract_matrix_conformance(
    hub: &IsolatedHub,
    package_path: impl Into<PathBuf>,
) -> Result<PluginContractMatrixConformanceReport, ConformanceError> {
    let package_path = package_path.into();
    let installed = request(
        hub.endpoint(),
        DaemonRequest::InstallPackageLocalPath { path: package_path },
        "contract_matrix_install",
    )?;
    expect_kind(
        &installed,
        DaemonResponseKind::PackageDecision,
        "contract_matrix_install",
    )?;
    let installed_package = package_row(&installed.packages, PLUGIN_CONTRACT_MATRIX_PACKAGE)?;
    expect_value(
        "contract_matrix_install",
        "state",
        "installed",
        &installed_package.state,
    )?;
    expect_value(
        "contract_matrix_install",
        "version",
        "1.0.0",
        &installed_package.version,
    )?;
    expect_value(
        "contract_matrix_install",
        "source_kind",
        "path",
        &installed_package.source_kind,
    )?;
    let surface_ids = installed_package
        .surfaces
        .iter()
        .map(|surface| surface.id.clone())
        .collect::<Vec<_>>();
    expect_value(
        "contract_matrix_install",
        "surface_ids",
        r#"["contract.app","contract.empty","contract.blocked","contract.invalid_body","contract.settings"]"#,
        &serde_json::to_string(&surface_ids).expect("surface ids serialize"),
    )?;
    let settings_surface_descriptor = surface_descriptor(
        &installed_package.surfaces,
        PLUGIN_CONTRACT_SETTINGS_SURFACE,
    )?;
    expect_value(
        "contract_matrix_install",
        "settings_surface.kind",
        "settings",
        &settings_surface_descriptor.kind,
    )?;
    expect_value(
        "contract_matrix_install",
        "settings_surface.supports",
        r#"["render"]"#,
        &serde_json::to_string(&settings_surface_descriptor.supports).expect("supports serialize"),
    )?;
    let app_route = package_route(&installed_package.routes, "surface:contract.app")?;
    expect_value(
        "contract_matrix_install",
        "app_route.route_path",
        "/packages/botster.plugin-contract-matrix/surfaces/contract.app",
        &app_route.route_path,
    )?;
    expect_value(
        "contract_matrix_install",
        "app_route.target.kind",
        "plugin_surface",
        &app_route.target.kind,
    )?;
    expect_value(
        "contract_matrix_install",
        "app_route.surface_id",
        PLUGIN_CONTRACT_APP_SURFACE,
        app_route.surface_id.as_deref().unwrap_or_default(),
    )?;
    if app_route.enabled {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_install",
            field: "app_route.enabled",
            expected: "false".to_string(),
            actual: "true".to_string(),
        });
    }
    if !app_route.blocked {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_install",
            field: "app_route.blocked",
            expected: "true".to_string(),
            actual: "false".to_string(),
        });
    }
    let enable_action = package_action(&installed_package.actions, "enable_package")?;
    let enable_action_status_after_install = package_action_status_label(enable_action.status);
    expect_value(
        "contract_matrix_install",
        "enable_action.status",
        "blocked",
        enable_action_status_after_install,
    )?;
    if !installed_package.configuration.missing_required.is_empty() {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_install",
            field: "configuration.missing_required",
            expected: "[]".to_string(),
            actual: format!("{:?}", installed_package.configuration.missing_required),
        });
    }

    let invalid_config = request(
        hub.endpoint(),
        DaemonRequest::SetPackageConfiguration {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            values: BTreeMap::from([(
                "mode".to_string(),
                serde_json::json!({"type":"select","value":"sideways"}),
            )]),
        },
        "contract_matrix_config_invalid",
    )?;
    expect_kind(
        &invalid_config,
        DaemonResponseKind::OperatorError,
        "contract_matrix_config_invalid",
    )?;
    let (
        invalid_configuration_diagnostic_kind,
        invalid_configuration_diagnostic_operation,
        invalid_configuration_diagnostic_message,
    ) = diagnostic_details(
        &invalid_config,
        DaemonDiagnosticKind::ActionFailure,
        Some("configure"),
        "contract_matrix_config_invalid",
    )?;
    let invalid_configuration_diagnostic_mentions_rejected_value =
        invalid_configuration_diagnostic_message.contains("sideways");
    if !invalid_configuration_diagnostic_mentions_rejected_value {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_config_invalid",
            field: "diagnostic.message",
            expected: "message mentioning rejected value sideways".to_string(),
            actual: invalid_configuration_diagnostic_message,
        });
    }

    let configured = request(
        hub.endpoint(),
        DaemonRequest::SetPackageConfiguration {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            values: BTreeMap::from([
                (
                    "endpoint".to_string(),
                    serde_json::json!({"type":"url","value":"https://example.invalid/plugin-contract-matrix/acceptance"}),
                ),
                (
                    "mode".to_string(),
                    serde_json::json!({"type":"select","value":"write"}),
                ),
                (
                    "api_token".to_string(),
                    serde_json::json!({"type":"secret","state":"write_only"}),
                ),
            ]),
        },
        "contract_matrix_config_valid",
    )?;
    expect_kind(
        &configured,
        DaemonResponseKind::Packages,
        "contract_matrix_config_valid",
    )?;
    let configured_package = package_row(&configured.packages, PLUGIN_CONTRACT_MATRIX_PACKAGE)?;
    let valid_configuration_mode = value_string(
        &configured_package.configuration.effective_values["mode"],
        "value",
        "contract_matrix_config_valid",
    )?;
    let valid_configuration_secret_state = value_string(
        &configured_package.configuration.effective_values["api_token"],
        "state",
        "contract_matrix_config_valid",
    )?;
    expect_value(
        "contract_matrix_config_valid",
        "mode.value",
        "write",
        &valid_configuration_mode,
    )?;
    expect_value(
        "contract_matrix_config_valid",
        "api_token.state",
        "redacted",
        &valid_configuration_secret_state,
    )?;

    let enable = request(
        hub.endpoint(),
        DaemonRequest::EnablePackage {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
        },
        "contract_matrix_enable",
    )?;
    expect_kind(
        &enable,
        DaemonResponseKind::PackageDecision,
        "contract_matrix_enable",
    )?;
    let enabled_package = package_row(&enable.packages, PLUGIN_CONTRACT_MATRIX_PACKAGE)?;
    expect_value(
        "contract_matrix_enable",
        "state",
        "enabled",
        &enabled_package.state,
    )?;
    let enabled_app_route = package_route(&enabled_package.routes, "surface:contract.app")?;
    if enabled_app_route.blocked {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_enable",
            field: "app_route.blocked",
            expected: "false".to_string(),
            actual: "true".to_string(),
        });
    }
    let reload_action = package_action(&enabled_package.actions, "reload_package")?;
    if reload_action.request.is_none() {
        return Err(ConformanceError::MissingJsonField {
            operation: "contract_matrix_enable",
            field: "reload_package.request",
        });
    }

    let list = request(
        hub.endpoint(),
        DaemonRequest::ListPackages,
        "contract_matrix_list",
    )?;
    expect_kind(&list, DaemonResponseKind::Packages, "contract_matrix_list")?;
    let listed = package_row(&list.packages, PLUGIN_CONTRACT_MATRIX_PACKAGE)?;
    expect_value("contract_matrix_list", "state", "enabled", &listed.state)?;
    let list_surfaces_match_enabled = listed.surfaces == enabled_package.surfaces;
    if !list_surfaces_match_enabled {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_list",
            field: "surfaces",
            expected: format!("{:?}", enabled_package.surfaces),
            actual: format!("{:?}", listed.surfaces),
        });
    }
    let settings_route_supports_settings =
        package_route(&listed.routes, "settings")?.supports_settings;
    if !settings_route_supports_settings {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_list",
            field: "settings.supports_settings",
            expected: "true".to_string(),
            actual: "false".to_string(),
        });
    }

    let show = request(
        hub.endpoint(),
        DaemonRequest::ShowPackage {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
        },
        "contract_matrix_show",
    )?;
    expect_kind(&show, DaemonResponseKind::Packages, "contract_matrix_show")?;
    let shown = package_row(&show.packages, PLUGIN_CONTRACT_MATRIX_PACKAGE)?;
    expect_value("contract_matrix_show", "state", "enabled", &shown.state)?;
    let show_routes_match_list = shown.routes == listed.routes;
    if !show_routes_match_list {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_show",
            field: "routes",
            expected: format!("{:?}", listed.routes),
            actual: format!("{:?}", shown.routes),
        });
    }

    let app_surface = render_plugin_surface(
        hub,
        PLUGIN_CONTRACT_APP_SURFACE,
        "contract_matrix_render_app",
    )?;
    let app_surface_snapshot =
        app_surface
            .ui_tree_snapshot
            .as_ref()
            .ok_or(ConformanceError::MissingBody {
                operation: "contract_matrix_render_app",
                field: "plugin_surface.ui_tree_snapshot",
            })?;
    let app_surface_kind = value_string(&app_surface.body, "type", "contract_matrix_render_app")?;
    let app_surface_node_id = value_string(&app_surface.body, "id", "contract_matrix_render_app")?;
    let app_surface_snapshot_id = value_string(
        &app_surface_snapshot.body,
        "id",
        "contract_matrix_render_app",
    )?;
    let app_surface_node_kinds = ui_node_type_values(&app_surface.body);
    let app_surface_snapshot_node_kinds = ui_node_type_values(&app_surface_snapshot.body);
    let expected_app_surface_node_kinds = application_primitives_fixture_descriptor()
        .node_kinds
        .iter()
        .map(|kind| (*kind).to_string())
        .collect::<Vec<_>>();
    if app_surface_node_kinds != expected_app_surface_node_kinds {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_render_app",
            field: "node kinds",
            expected: format!("{expected_app_surface_node_kinds:?}"),
            actual: format!("{app_surface_node_kinds:?}"),
        });
    }
    if app_surface_snapshot_node_kinds != app_surface_node_kinds {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_render_app",
            field: "ui_tree_snapshot.body node kinds",
            expected: format!("{app_surface_node_kinds:?}"),
            actual: format!("{app_surface_snapshot_node_kinds:?}"),
        });
    }
    expect_value(
        "contract_matrix_render_app",
        "package_name",
        PLUGIN_CONTRACT_MATRIX_PACKAGE,
        &app_surface.package_name,
    )?;
    expect_value(
        "contract_matrix_render_app",
        "surface_id",
        PLUGIN_CONTRACT_APP_SURFACE,
        &app_surface.surface_id,
    )?;
    expect_value(
        "contract_matrix_render_app",
        "type",
        "panel",
        &app_surface_kind,
    )?;
    expect_value(
        "contract_matrix_render_app",
        "id",
        "contract-app-panel",
        &app_surface_node_id,
    )?;
    expect_value(
        "contract_matrix_render_app",
        "ui_tree_snapshot.package_name",
        PLUGIN_CONTRACT_MATRIX_PACKAGE,
        &app_surface_snapshot.package_name,
    )?;
    expect_value(
        "contract_matrix_render_app",
        "ui_tree_snapshot.surface_id",
        PLUGIN_CONTRACT_APP_SURFACE,
        &app_surface_snapshot.surface_id,
    )?;
    expect_value(
        "contract_matrix_render_app",
        "ui_tree_snapshot.body.id",
        app_surface_node_id.as_str(),
        &app_surface_snapshot_id,
    )?;
    let submit_node = find_ui_node_by_id(&app_surface.body, "contract-app-submit").ok_or(
        ConformanceError::MissingJsonField {
            operation: "contract_matrix_render_app",
            field: "contract-app-submit",
        },
    )?;
    let submit_action_id = ui_action_id(
        submit_node,
        "contract_matrix_render_app",
        "contract-app-submit.props.action.id",
    )?;
    expect_value(
        "contract_matrix_render_app",
        "contract-app-submit.props.action.id",
        PLUGIN_CONTRACT_ACTION,
        &submit_action_id,
    )?;

    let empty_surface = render_plugin_surface(
        hub,
        PLUGIN_CONTRACT_EMPTY_SURFACE,
        "contract_matrix_render_empty",
    )?;
    let empty_surface_node_id =
        value_string(&empty_surface.body, "id", "contract_matrix_render_empty")?;
    let empty_surface_child_id = empty_surface
        .body
        .get("children")
        .and_then(serde_json::Value::as_array)
        .and_then(|children| children.first())
        .and_then(|child| child.get("id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_render_empty",
            field: "children[0].id",
        })?;
    expect_value(
        "contract_matrix_render_empty",
        "id",
        "contract-empty-panel",
        &empty_surface_node_id,
    )?;
    expect_value(
        "contract_matrix_render_empty",
        "children[0].id",
        "contract-empty-message",
        &empty_surface_child_id,
    )?;

    let blocked = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceRender {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            surface_id: PLUGIN_CONTRACT_BLOCKED_SURFACE.to_string(),
            payload: serde_json::json!({}),
        },
        "contract_matrix_render_blocked",
    )?;
    expect_kind(
        &blocked,
        DaemonResponseKind::OperatorError,
        "contract_matrix_render_blocked",
    )?;
    let blocked_error = blocked
        .error
        .as_ref()
        .ok_or(ConformanceError::MissingBody {
            operation: "contract_matrix_render_blocked",
            field: "error",
        })?;
    expect_value(
        "contract_matrix_render_blocked",
        "error.code",
        "plugin_invocation_failed",
        &blocked_error.code,
    )?;
    expect_value(
        "contract_matrix_render_blocked",
        "error.operation",
        "plugin_surface_render",
        &blocked_error.operation,
    )?;
    let blocked_render_message_contains_failure = blocked_error
        .message
        .contains("plugin surface render failed");
    if !blocked_render_message_contains_failure {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_render_blocked",
            field: "error.message",
            expected: "message containing plugin surface render failed".to_string(),
            actual: blocked_error.message.clone(),
        });
    }

    let invalid_body = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceRender {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            surface_id: PLUGIN_CONTRACT_INVALID_BODY_SURFACE.to_string(),
            payload: serde_json::json!({}),
        },
        "contract_matrix_render_invalid_body",
    )?;
    expect_kind(
        &invalid_body,
        DaemonResponseKind::OperatorError,
        "contract_matrix_render_invalid_body",
    )?;
    let invalid_body_error = invalid_body
        .error
        .as_ref()
        .ok_or(ConformanceError::MissingBody {
            operation: "contract_matrix_render_invalid_body",
            field: "error",
        })?;
    expect_value(
        "contract_matrix_render_invalid_body",
        "error.code",
        "invalid_surface",
        &invalid_body_error.code,
    )?;
    expect_value(
        "contract_matrix_render_invalid_body",
        "error.operation",
        "plugin_surface_render",
        &invalid_body_error.operation,
    )?;
    let invalid_body_diagnostic = invalid_body
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.operation.as_deref() == Some("plugin_surface_render"))
        .ok_or(ConformanceError::MissingBody {
            operation: "contract_matrix_render_invalid_body",
            field: "diagnostics[operation=plugin_surface_render]",
        })?;
    expect_value(
        "contract_matrix_render_invalid_body",
        "diagnostic.kind",
        "action_failure",
        diagnostic_kind_label(invalid_body_diagnostic.kind),
    )?;
    expect_kind(
        &request(
            hub.endpoint(),
            DaemonRequest::Status,
            "contract_matrix_status_after_blocked",
        )?,
        DaemonResponseKind::Status,
        "contract_matrix_status_after_blocked",
    )?;

    let settings_surface = render_plugin_surface(
        hub,
        PLUGIN_CONTRACT_SETTINGS_SURFACE,
        "contract_matrix_render_settings",
    )?;
    let settings_surface_node_id = value_string(
        &settings_surface.body,
        "id",
        "contract_matrix_render_settings",
    )?;
    let settings_text = settings_surface
        .body
        .get("children")
        .and_then(serde_json::Value::as_array)
        .and_then(|children| children.first())
        .and_then(|child| child.get("props"))
        .and_then(|props| props.get("text"))
        .and_then(serde_json::Value::as_str)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_render_settings",
            field: "children[0].props.text",
        })?;
    let settings_text_contains_endpoint = settings_text
        .contains("endpoint=https://example.invalid/plugin-contract-matrix/acceptance");
    let settings_text_contains_mode = settings_text.contains("mode=write");
    let settings_text_contains_redacted_secret = settings_text.contains("api_token_state=redacted");
    if !settings_text_contains_endpoint
        || !settings_text_contains_mode
        || !settings_text_contains_redacted_secret
    {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_render_settings",
            field: "settings_text",
            expected: "endpoint, mode, and redacted secret state".to_string(),
            actual: settings_text.to_string(),
        });
    }

    let action = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            surface_id: PLUGIN_CONTRACT_APP_SURFACE.to_string(),
            action_id: submit_action_id.clone(),
            payload: serde_json::json!({
                "request_id": "contract-action-success",
                "message": "hello",
            }),
        },
        "contract_matrix_action_success",
    )?;
    expect_kind(
        &action,
        DaemonResponseKind::PluginActionResult,
        "contract_matrix_action_success",
    )?;
    let action_result =
        action
            .plugin_action_result
            .as_ref()
            .ok_or(ConformanceError::MissingBody {
                operation: "contract_matrix_action_success",
                field: "plugin_action_result",
            })?;
    let action_success_state =
        value_string(action_result, "state", "contract_matrix_action_success")?;
    let action_success_request_id = value_string(
        action_result,
        "request_id",
        "contract_matrix_action_success",
    )?;
    let action_success_message = action_result
        .get("normalized_values")
        .and_then(|values| values.get("message"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_action_success",
            field: "normalized_values.message",
        })?;
    expect_value(
        "contract_matrix_action_success",
        "state",
        "accepted",
        &action_success_state,
    )?;
    if !action.diagnostics.is_empty() {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_success",
            field: "diagnostics",
            expected: "[]".to_string(),
            actual: format!("{:?}", action.diagnostics),
        });
    }

    let action_error = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            surface_id: PLUGIN_CONTRACT_APP_SURFACE.to_string(),
            action_id: submit_action_id.clone(),
            payload: serde_json::json!({
                "request_id": "contract-action-error",
                "fail": true,
            }),
        },
        "contract_matrix_action_error",
    )?;
    expect_kind(
        &action_error,
        DaemonResponseKind::PluginActionResult,
        "contract_matrix_action_error",
    )?;
    let (action_error_diagnostic_kind, action_error_diagnostic_operation, _) = diagnostic_details(
        &action_error,
        DaemonDiagnosticKind::ActionFailure,
        Some("plugin_surface_action"),
        "contract_matrix_action_error",
    )?;
    let action_error_result =
        action_error
            .plugin_action_result
            .as_ref()
            .ok_or(ConformanceError::MissingBody {
                operation: "contract_matrix_action_error",
                field: "plugin_action_result",
            })?;
    let action_error_state =
        value_string(action_error_result, "state", "contract_matrix_action_error")?;
    let action_error_request_id = value_string(
        action_error_result,
        "request_id",
        "contract_matrix_action_error",
    )?;
    expect_value(
        "contract_matrix_action_error",
        "state",
        "error",
        &action_error_state,
    )?;

    let action_field_error = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            surface_id: PLUGIN_CONTRACT_APP_SURFACE.to_string(),
            action_id: submit_action_id.clone(),
            payload: serde_json::json!({
                "request_id": "contract-action-field-error",
                "field_error": true,
            }),
        },
        "contract_matrix_action_field_error",
    )?;
    expect_kind(
        &action_field_error,
        DaemonResponseKind::PluginActionResult,
        "contract_matrix_action_field_error",
    )?;
    let (action_field_error_diagnostic_kind, action_field_error_diagnostic_operation, _) =
        diagnostic_details(
            &action_field_error,
            DaemonDiagnosticKind::ActionFailure,
            Some("plugin_surface_action"),
            "contract_matrix_action_field_error",
        )?;
    let action_field_error_result =
        action_field_error
            .plugin_action_result
            .as_ref()
            .ok_or(ConformanceError::MissingBody {
                operation: "contract_matrix_action_field_error",
                field: "plugin_action_result",
            })?;
    let action_field_error_state = value_string(
        action_field_error_result,
        "state",
        "contract_matrix_action_field_error",
    )?;
    let action_field_error_request_id = value_string(
        action_field_error_result,
        "request_id",
        "contract_matrix_action_field_error",
    )?;
    let action_field_error_message = field_error_string(
        action_field_error_result,
        "contract-app-message",
        "contract_matrix_action_field_error",
    )?;
    expect_value(
        "contract_matrix_action_field_error",
        "state",
        "error",
        &action_field_error_state,
    )?;

    Ok(PluginContractMatrixConformanceReport {
        package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
        installed_state: installed_package.state.clone(),
        enabled_state: enabled_package.state.clone(),
        version: installed_package.version.clone(),
        source_kind: installed_package.source_kind.clone(),
        surface_ids,
        settings_surface_kind: settings_surface_descriptor.kind.clone(),
        settings_surface_supports: settings_surface_descriptor.supports.clone(),
        app_route_path: app_route.route_path.clone(),
        app_route_target_kind: app_route.target.kind.clone(),
        app_route_surface_id: app_route.surface_id.clone().unwrap_or_default(),
        app_route_enabled_after_install: app_route.enabled,
        app_route_blocked_after_install: app_route.blocked,
        enable_action_status_after_install: enable_action_status_after_install.to_string(),
        invalid_configuration_diagnostic_kind,
        invalid_configuration_diagnostic_operation,
        invalid_configuration_diagnostic_mentions_rejected_value,
        valid_configuration_mode,
        valid_configuration_secret_state: valid_configuration_secret_state.clone(),
        list_state: listed.state.clone(),
        list_surfaces_match_enabled,
        show_state: shown.state.clone(),
        show_routes_match_list,
        settings_route_supports_settings,
        app_surface_package_name: app_surface.package_name.clone(),
        app_surface_id: app_surface.surface_id.clone(),
        app_surface_kind,
        app_surface_node_id: app_surface_node_id.clone(),
        app_surface_node_kinds: app_surface_node_kinds.clone(),
        app_surface_snapshot_package_name: app_surface_snapshot.package_name.clone(),
        app_surface_snapshot_id: app_surface_snapshot.surface_id.clone(),
        app_surface_snapshot_node_id: app_surface_snapshot_id,
        app_surface_snapshot_node_kinds,
        empty_surface_node_id,
        empty_surface_child_id: empty_surface_child_id.clone(),
        blocked_render_error_code: blocked_error.code.clone(),
        blocked_render_operation: blocked_error.operation.clone(),
        blocked_render_message_contains_failure,
        invalid_body_error_code: invalid_body_error.code.clone(),
        invalid_body_operation: invalid_body_error.operation.clone(),
        invalid_body_diagnostic_kind: diagnostic_kind_label(invalid_body_diagnostic.kind)
            .to_string(),
        invalid_body_diagnostic_operation: invalid_body_diagnostic
            .operation
            .clone()
            .unwrap_or_default(),
        settings_surface_node_id: settings_surface_node_id.clone(),
        settings_text_contains_endpoint,
        settings_text_contains_mode,
        settings_text_contains_redacted_secret,
        action_success_state,
        action_success_request_id,
        action_success_message,
        action_error_state,
        action_error_request_id,
        action_error_diagnostic_kind,
        action_error_diagnostic_operation,
        submit_action_id,
        action_field_error_state,
        action_field_error_request_id,
        action_field_error_diagnostic_kind,
        action_field_error_diagnostic_operation,
        action_field_error_message,
        client_render_check: PluginContractMatrixClientRenderCheck {
            class: ConformanceFailureClass::ClientRendering,
            app_surface_node_id,
            app_surface_node_kinds,
            empty_surface_child_id,
            settings_surface_node_id,
            expected_redacted_secret_state: valid_configuration_secret_state,
        },
        failure_classes: PluginConformanceFailureClasses {
            producer_contract: ConformanceFailureClass::ProducerContract,
            client_rendering: ConformanceFailureClass::ClientRendering,
            environment_setup: ConformanceFailureClass::EnvironmentSetup,
        },
    })
}

/// Run the foreground terminal app-open conformance flow for first-party clients.
///
/// The helper installs a local package with a `terminal_app` / `foreground_stdio`
/// runnable entrypoint, discovers it through `ListApps`, resolves it through
/// `ResolveAppLaunch`, then executes the daemon-resolved command with the
/// daemon-provided working directory and environment. The child process uses
/// `BOTSTER_HUB_SOCKET` to perform a real `Status` daemon request before
/// exiting.
pub fn run_foreground_terminal_app_open_conformance(
    hub: &IsolatedHub,
) -> Result<ForegroundTerminalAppOpenConformanceReport, ConformanceError> {
    let package_path = hub.data_dir().join("foreground-terminal-app-open-package");
    write_foreground_terminal_app_package(&package_path)?;

    let enabled = request(
        hub.endpoint(),
        DaemonRequest::EnablePackageLocalPath {
            path: package_path.clone(),
        },
        "enable_foreground_terminal_app",
    )?;
    expect_kind(
        &enabled,
        DaemonResponseKind::PackageDecision,
        "enable_foreground_terminal_app",
    )?;
    let package_state = enabled
        .package_decision
        .ok_or(ConformanceError::MissingBody {
            operation: "enable_foreground_terminal_app",
            field: "package_decision",
        })?
        .state;

    let apps = request(hub.endpoint(), DaemonRequest::ListApps, "list_apps")?;
    expect_kind(&apps, DaemonResponseKind::Apps, "list_apps")?;
    let app = apps
        .apps
        .iter()
        .find(|app| {
            app.package_name == "first-party.terminal-client"
                && app.entrypoint_id == "tui"
                && app.kind == "terminal_app"
                && app.launch_mode == "foreground_stdio"
        })
        .cloned()
        .ok_or(ConformanceError::MissingApp {
            package_name: "first-party.terminal-client",
            entrypoint_id: "tui",
        })?;

    let resolved = request(
        hub.endpoint(),
        DaemonRequest::ResolveAppLaunch {
            package_name: app.package_name.clone(),
            entrypoint_id: app.entrypoint_id.clone(),
        },
        "resolve_app_launch",
    )?;
    expect_kind(
        &resolved,
        DaemonResponseKind::ResolvedAppLaunch,
        "resolve_app_launch",
    )?;
    let launch = resolved
        .resolved_app_launch
        .ok_or(ConformanceError::MissingBody {
            operation: "resolve_app_launch",
            field: "resolved_app_launch",
        })?;
    let hub_socket_env_present = launch.environment.contains_key("BOTSTER_HUB_SOCKET");
    let hub_data_dir_env_present = launch.environment.contains_key("BOTSTER_HUB_DATA_DIR");
    if !hub_socket_env_present {
        return Err(ConformanceError::MissingEnvironment {
            operation: "resolve_app_launch",
            name: "BOTSTER_HUB_SOCKET",
        });
    }
    if !hub_data_dir_env_present {
        return Err(ConformanceError::MissingEnvironment {
            operation: "resolve_app_launch",
            name: "BOTSTER_HUB_DATA_DIR",
        });
    }

    let output = Command::new(&launch.command)
        .args(&launch.args)
        .current_dir(&launch.working_directory)
        .envs(&launch.environment)
        .stdin(Stdio::null())
        .output()
        .map_err(|source| ConformanceError::Io {
            operation: "foreground_terminal_app_open",
            source,
        })?;
    let exit_code = output.status.code();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        return Err(ConformanceError::ChildFailed {
            operation: "foreground_terminal_app_open",
            status: output.status.to_string(),
            stdout,
            stderr,
        });
    }
    let real_hub_action_result =
        output_value(&stdout, "daemon_status").ok_or(ConformanceError::MissingOutput {
            needle: "daemon_status=",
            output: stdout.clone(),
        })?;

    Ok(ForegroundTerminalAppOpenConformanceReport {
        package_state,
        package_name: app.package_name,
        app_id: app.app_id,
        entrypoint_id: app.entrypoint_id,
        app_kind: app.kind,
        launch_mode: app.launch_mode,
        resolved_command: launch.command,
        hub_socket_env_present,
        hub_data_dir_env_present,
        real_hub_action_operation: "status".to_string(),
        real_hub_action_result,
        exit_code,
        stdout,
        stderr,
    })
}

fn write_foreground_terminal_app_package(package_path: &Path) -> Result<(), ConformanceError> {
    let scripts_dir = package_path.join("scripts");
    fs::create_dir_all(&scripts_dir).map_err(|source| ConformanceError::Io {
        operation: "write_foreground_terminal_app_package",
        source,
    })?;
    fs::write(
        package_path.join("plugin.lua"),
        "return botster.register({})\n",
    )
    .map_err(|source| ConformanceError::Io {
        operation: "write_foreground_terminal_app_package",
        source,
    })?;
    fs::write(
        scripts_dir.join("foreground-terminal-client.mjs"),
        r#"
import fs from 'fs';
import net from 'net';

const socket = process.env.BOTSTER_HUB_SOCKET;
const dataDir = process.env.BOTSTER_HUB_DATA_DIR;

if (!socket) {
  console.error('missing BOTSTER_HUB_SOCKET');
  process.exit(42);
}
if (!dataDir) {
  console.error('missing BOTSTER_HUB_DATA_DIR');
  process.exit(43);
}
if (!fs.existsSync(socket)) {
  console.error('BOTSTER_HUB_SOCKET is not a socket path that exists');
  process.exit(44);
}
if (!fs.existsSync(dataDir) || !fs.statSync(dataDir).isDirectory()) {
  console.error('BOTSTER_HUB_DATA_DIR is not a directory');
  process.exit(45);
}

function readLine(connection) {
  const newline = connection.buffer.indexOf('\n');
  if (newline >= 0) {
    const line = connection.buffer.slice(0, newline);
    connection.buffer = connection.buffer.slice(newline + 1);
    return Promise.resolve(line);
  }

  return new Promise((resolve, reject) => {
    const onData = (chunk) => {
      connection.buffer += chunk.toString('utf8');
      const newline = connection.buffer.indexOf('\n');
      if (newline < 0) {
        return;
      }
      cleanup();
      const line = connection.buffer.slice(0, newline);
      connection.buffer = connection.buffer.slice(newline + 1);
      resolve(line);
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const cleanup = () => {
      connection.stream.off('data', onData);
      connection.stream.off('error', onError);
    };
    connection.stream.on('data', onData);
    connection.stream.once('error', onError);
  });
}

const stream = net.createConnection(socket);
const connection = { stream, buffer: '' };

await new Promise((resolve, reject) => {
  stream.once('connect', resolve);
  stream.once('error', reject);
});
stream.write(JSON.stringify({
  protocol: 'botster-hub-daemon-v1',
  compatibility: {
    protocol: 'botster-hub-daemon-v1',
    minimum_protocol_version: 1,
    required_features: [],
    minimum_conformance_fixture_revision: 1,
    client_name: 'foreground-terminal-app-open-fixture',
  },
}) + '\n');
await readLine(connection);
stream.write(JSON.stringify({ type: 'status' }) + '\n');
const response = JSON.parse(await readLine(connection));
stream.end();

if (response.kind !== 'status' || !response.status) {
  console.error(`unexpected daemon response ${JSON.stringify(response)}`);
  process.exit(46);
}

console.log(`hub_socket_present=${Boolean(socket)}`);
console.log(`hub_data_dir_present=${Boolean(dataDir)}`);
console.log(`daemon_status=${response.status.lifecycle_state}`);
"#,
    )
    .map_err(|source| ConformanceError::Io {
        operation: "write_foreground_terminal_app_package",
        source,
    })?;
    let manifest = serde_json::json!({
        "name": "first-party.terminal-client",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [{
            "id": "tui",
            "kind": "terminal_app",
            "command": "node",
            "args": ["scripts/foreground-terminal-client.mjs"],
            "working_directory": { "policy": "package_root" },
            "environment": [
                { "name": "BOTSTER_HUB_SOCKET", "required": false },
                { "name": "BOTSTER_HUB_DATA_DIR", "required": false }
            ],
            "launch_mode": "foreground_stdio"
        }]
    });
    fs::write(
        package_path.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest)
            .expect("foreground terminal app manifest serializes"),
    )
    .map_err(|source| ConformanceError::Io {
        operation: "write_foreground_terminal_app_package",
        source,
    })?;
    Ok(())
}

fn package_row<'a>(
    packages: &'a [botster_hub_client::DaemonPackage],
    package_name: &'static str,
) -> Result<&'a botster_hub_client::DaemonPackage, ConformanceError> {
    packages
        .iter()
        .find(|package| package.package_name == package_name)
        .ok_or(ConformanceError::MissingPackage { package_name })
}

fn surface_descriptor<'a>(
    surfaces: &'a [botster_hub_client::DaemonPackageSurfaceDescriptor],
    surface_id: &'static str,
) -> Result<&'a botster_hub_client::DaemonPackageSurfaceDescriptor, ConformanceError> {
    surfaces
        .iter()
        .find(|surface| surface.id == surface_id)
        .ok_or(ConformanceError::MissingSurface { surface_id })
}

fn package_route<'a>(
    routes: &'a [botster_hub_client::DaemonPackageRouteDescriptor],
    route_id: &'static str,
) -> Result<&'a botster_hub_client::DaemonPackageRouteDescriptor, ConformanceError> {
    routes
        .iter()
        .find(|route| route.route_id == route_id)
        .ok_or(ConformanceError::MissingRoute { route_id })
}

fn package_action<'a>(
    actions: &'a [botster_hub_client::DaemonPackageActionState],
    action_id: &'static str,
) -> Result<&'a botster_hub_client::DaemonPackageActionState, ConformanceError> {
    actions
        .iter()
        .find(|action| action.action_id == action_id)
        .ok_or(ConformanceError::MissingPackageAction { action_id })
}

fn package_action_status_label(
    status: botster_hub_client::DaemonPackageActionStatus,
) -> &'static str {
    match status {
        botster_hub_client::DaemonPackageActionStatus::Available => "available",
        botster_hub_client::DaemonPackageActionStatus::Blocked => "blocked",
        botster_hub_client::DaemonPackageActionStatus::Unavailable => "unavailable",
    }
}

fn render_plugin_surface(
    hub: &IsolatedHub,
    surface_id: &'static str,
    operation: &'static str,
) -> Result<botster_hub_client::DaemonPluginSurface, ConformanceError> {
    let response = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceRender {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            surface_id: surface_id.to_string(),
            payload: serde_json::json!({}),
        },
        operation,
    )?;
    expect_kind(&response, DaemonResponseKind::PluginSurface, operation)?;
    response
        .plugin_surface
        .ok_or(ConformanceError::MissingBody {
            operation,
            field: "plugin_surface",
        })
}

fn expect_value(
    operation: &'static str,
    field: &'static str,
    expected: impl Into<String>,
    actual: &str,
) -> Result<(), ConformanceError> {
    let expected = expected.into();
    if actual == expected {
        Ok(())
    } else {
        Err(ConformanceError::UnexpectedValue {
            operation,
            field,
            expected,
            actual: actual.to_string(),
        })
    }
}

fn output_value(output: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    output
        .lines()
        .find_map(|line| line.strip_prefix(&prefix).map(str::to_string))
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
            error: response.error.clone().map(Box::new),
        })
    }
}

fn diagnostic_operation(
    response: &DaemonResponse,
    kind: DaemonDiagnosticKind,
    operation: &'static str,
) -> Result<String, ConformanceError> {
    response
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.kind == kind)
        .and_then(|diagnostic| diagnostic.operation.clone())
        .ok_or(ConformanceError::MissingDiagnostic { operation, kind })
}

fn diagnostic_kind(
    response: &DaemonResponse,
    kind: DaemonDiagnosticKind,
    operation: &'static str,
) -> Result<String, ConformanceError> {
    response
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.kind == kind)
        .then(|| diagnostic_kind_label(kind).to_string())
        .ok_or(ConformanceError::MissingDiagnostic { operation, kind })
}

fn diagnostic_details(
    response: &DaemonResponse,
    kind: DaemonDiagnosticKind,
    expected_operation: Option<&'static str>,
    operation: &'static str,
) -> Result<(String, String, String), ConformanceError> {
    let diagnostic = response
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.kind == kind)
        .ok_or(ConformanceError::MissingDiagnostic { operation, kind })?;
    let diagnostic_operation =
        diagnostic
            .operation
            .clone()
            .ok_or(ConformanceError::MissingJsonField {
                operation,
                field: "diagnostic.operation",
            })?;
    if let Some(expected) = expected_operation {
        expect_value(
            operation,
            "diagnostic.operation",
            expected,
            &diagnostic_operation,
        )?;
    }
    Ok((
        diagnostic_kind_label(kind).to_string(),
        diagnostic_operation,
        diagnostic.message.clone().unwrap_or_default(),
    ))
}

fn diagnostic_kind_label(kind: DaemonDiagnosticKind) -> &'static str {
    match kind {
        DaemonDiagnosticKind::Connected => "connected",
        DaemonDiagnosticKind::Disconnected => "disconnected",
        DaemonDiagnosticKind::CompatibilityMismatch => "compatibility_mismatch",
        DaemonDiagnosticKind::UnsupportedFeature => "unsupported_feature",
        DaemonDiagnosticKind::TerminalStreamUnavailable => "terminal_stream_unavailable",
        DaemonDiagnosticKind::ActionFailure => "action_failure",
        DaemonDiagnosticKind::DaemonStartupFailure => "daemon_startup_failure",
        DaemonDiagnosticKind::Backpressure => "backpressure",
    }
}

fn daemon_diagnostic_kind_labels() -> Vec<&'static str> {
    vec![
        diagnostic_kind_label(DaemonDiagnosticKind::Connected),
        diagnostic_kind_label(DaemonDiagnosticKind::Disconnected),
        diagnostic_kind_label(DaemonDiagnosticKind::CompatibilityMismatch),
        diagnostic_kind_label(DaemonDiagnosticKind::UnsupportedFeature),
        diagnostic_kind_label(DaemonDiagnosticKind::TerminalStreamUnavailable),
        diagnostic_kind_label(DaemonDiagnosticKind::ActionFailure),
        diagnostic_kind_label(DaemonDiagnosticKind::DaemonStartupFailure),
        diagnostic_kind_label(DaemonDiagnosticKind::Backpressure),
    ]
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

fn ui_node_type_values(value: &serde_json::Value) -> Vec<String> {
    let mut values = Vec::new();
    collect_ui_node_type_values(value, &mut values);
    values.sort();
    values
}

fn collect_ui_node_type_values(value: &serde_json::Value, values: &mut Vec<String>) {
    if let Some(kind) = value.get("type").and_then(serde_json::Value::as_str) {
        values.push(kind.to_string());
    }
    if let Some(children) = value.get("children").and_then(serde_json::Value::as_array) {
        for child in children {
            collect_ui_node_type_values(child, values);
        }
    }
    if let Some(slots) = value.get("slots").and_then(serde_json::Value::as_object) {
        for slot_children in slots.values().filter_map(serde_json::Value::as_array) {
            for child in slot_children {
                collect_ui_node_type_values(child, values);
            }
        }
    }
    if let Some(props) = value.get("props").and_then(serde_json::Value::as_object) {
        for prop in props.values() {
            collect_ui_node_type_values(prop, values);
        }
    }
}

fn find_ui_node_by_id<'a>(
    value: &'a serde_json::Value,
    node_id: &str,
) -> Option<&'a serde_json::Value> {
    if value.get("type").is_some()
        && value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|id| id == node_id)
    {
        return Some(value);
    }
    if let Some(children) = value.get("children").and_then(serde_json::Value::as_array) {
        for child in children {
            if let Some(found) = find_ui_node_by_id(child, node_id) {
                return Some(found);
            }
        }
    }
    if let Some(slots) = value.get("slots").and_then(serde_json::Value::as_object) {
        for slot_children in slots.values().filter_map(serde_json::Value::as_array) {
            for child in slot_children {
                if let Some(found) = find_ui_node_by_id(child, node_id) {
                    return Some(found);
                }
            }
        }
    }
    if let Some(props) = value.get("props").and_then(serde_json::Value::as_object) {
        for prop in props.values() {
            if let Some(found) = find_ui_node_by_id(prop, node_id) {
                return Some(found);
            }
        }
    }
    None
}

fn ui_action_id(
    node: &serde_json::Value,
    operation: &'static str,
    field: &'static str,
) -> Result<String, ConformanceError> {
    node.get("props")
        .and_then(|props| props.get("action"))
        .and_then(|action| action.get("id"))
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
        error: Option<Box<DaemonOperatorError>>,
    },
    MissingBody {
        operation: &'static str,
        field: &'static str,
    },
    MissingJsonField {
        operation: &'static str,
        field: &'static str,
    },
    MissingDiagnostic {
        operation: &'static str,
        kind: DaemonDiagnosticKind,
    },
    MissingPackage {
        package_name: &'static str,
    },
    MissingSurface {
        surface_id: &'static str,
    },
    MissingRoute {
        route_id: &'static str,
    },
    MissingPackageAction {
        action_id: &'static str,
    },
    UnexpectedValue {
        operation: &'static str,
        field: &'static str,
        expected: String,
        actual: String,
    },
    MissingEnvironment {
        operation: &'static str,
        name: &'static str,
    },
    MissingApp {
        package_name: &'static str,
        entrypoint_id: &'static str,
    },
    MissingSession {
        session_id: String,
    },
    MissingOutput {
        needle: &'static str,
        output: String,
    },
    Io {
        operation: &'static str,
        source: std::io::Error,
    },
    ChildFailed {
        operation: &'static str,
        status: String,
        stdout: String,
        stderr: String,
    },
    AttachThreadPanicked,
}

impl ConformanceError {
    /// Classify conformance failures for harness output and downstream reports.
    #[must_use]
    pub const fn failure_class(&self) -> ConformanceFailureClass {
        match self {
            Self::Client { .. }
            | Self::Io { .. }
            | Self::ChildFailed { .. }
            | Self::AttachThreadPanicked => ConformanceFailureClass::EnvironmentSetup,
            Self::UnexpectedKind { .. }
            | Self::MissingBody { .. }
            | Self::MissingJsonField { .. }
            | Self::MissingDiagnostic { .. }
            | Self::MissingPackage { .. }
            | Self::MissingSurface { .. }
            | Self::MissingRoute { .. }
            | Self::MissingPackageAction { .. }
            | Self::UnexpectedValue { .. }
            | Self::MissingEnvironment { .. }
            | Self::MissingApp { .. }
            | Self::MissingSession { .. }
            | Self::MissingOutput { .. } => ConformanceFailureClass::ProducerContract,
        }
    }
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
            Self::MissingDiagnostic { operation, kind } => {
                write!(
                    formatter,
                    "{operation} response missing {kind:?} diagnostic"
                )
            }
            Self::MissingPackage { package_name } => {
                write!(formatter, "response missing package {package_name}")
            }
            Self::MissingSurface { surface_id } => {
                write!(formatter, "response missing package surface {surface_id}")
            }
            Self::MissingRoute { route_id } => {
                write!(formatter, "response missing package route {route_id}")
            }
            Self::MissingPackageAction { action_id } => {
                write!(formatter, "response missing package action {action_id}")
            }
            Self::UnexpectedValue {
                operation,
                field,
                expected,
                actual,
            } => {
                write!(
                    formatter,
                    "{operation} response field {field} was {actual:?}, expected {expected:?}"
                )
            }
            Self::MissingEnvironment { operation, name } => {
                write!(formatter, "{operation} launch missing {name}")
            }
            Self::MissingApp {
                package_name,
                entrypoint_id,
            } => {
                write!(
                    formatter,
                    "list_apps missing terminal app {package_name}/{entrypoint_id}"
                )
            }
            Self::MissingSession { session_id } => {
                write!(formatter, "spawn response missing session {session_id}")
            }
            Self::MissingOutput { needle, output } => {
                write!(formatter, "stream output missing {needle:?}: {output:?}")
            }
            Self::Io { operation, source } => {
                write!(formatter, "{operation} I/O failed: {source}")
            }
            Self::ChildFailed {
                operation,
                status,
                stdout,
                stderr,
            } => {
                write!(
                    formatter,
                    "{operation} child exited {status}; stdout={stdout:?}; stderr={stderr:?}"
                )
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
            | Self::MissingDiagnostic { .. }
            | Self::MissingPackage { .. }
            | Self::MissingSurface { .. }
            | Self::MissingRoute { .. }
            | Self::MissingPackageAction { .. }
            | Self::UnexpectedValue { .. }
            | Self::MissingEnvironment { .. }
            | Self::MissingApp { .. }
            | Self::MissingSession { .. }
            | Self::MissingOutput { .. }
            | Self::ChildFailed { .. }
            | Self::AttachThreadPanicked => None,
            Self::Io { source, .. } => Some(source),
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
    fn support_matrix_matches_current_compatibility_descriptor() {
        let matrix = first_party_client_support_matrix();
        let compatibility = DaemonCompatibility::current();
        let requirement = DaemonCompatibilityRequirement::current();

        assert_eq!(matrix.protocol, compatibility.protocol);
        assert_eq!(matrix.protocol_version, compatibility.protocol_version);
        assert_eq!(
            matrix.conformance_fixture_revision,
            compatibility.conformance_fixture_revision
        );
        assert_eq!(matrix.supported_features, compatibility.features);
        assert_eq!(matrix.required_features, requirement.required_features);
        assert!(
            matrix
                .supported_features
                .contains(&botster_hub_client::FEATURE_SESSIONS.to_string())
        );
        assert!(
            matrix
                .supported_features
                .contains(&matrix.terminal_streaming.feature)
        );
        assert!(matrix.supported_features.contains(&matrix.resize.feature));
        assert!(
            matrix
                .supported_features
                .contains(&matrix.plugin_surfaces.render_feature)
        );
        assert!(
            matrix
                .supported_features
                .contains(&matrix.plugin_surfaces.action_feature)
        );
    }

    #[test]
    fn support_matrix_diagnostic_kinds_are_exhaustive() {
        let matrix = first_party_client_support_matrix();

        assert_eq!(
            matrix.diagnostic_kinds,
            daemon_diagnostic_kind_labels()
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn plugin_contract_matrix_fixture_asset_describes_published_files() {
        let asset = plugin_contract_matrix_fixture_asset();

        assert_eq!(asset.package_name, PLUGIN_CONTRACT_MATRIX_PACKAGE);
        assert_eq!(asset.artifact_path, PLUGIN_CONTRACT_MATRIX_FIXTURE_ARTIFACT);
        assert_eq!(
            asset
                .files
                .iter()
                .map(|file| file.relative_path)
                .collect::<Vec<_>>(),
            vec!["README.md", "botster-package.json", "plugin.lua"]
        );
        assert!(
            asset.files.iter().all(|file| !file.contents.is_empty()),
            "published fixture files must not be empty"
        );
    }

    #[test]
    fn copy_plugin_contract_matrix_fixture_writes_caller_owned_package() {
        let root = unique_root("plugin-contract-copy");
        let package_dir =
            copy_plugin_contract_matrix_fixture(&root).expect("copy plugin contract fixture");

        assert_eq!(
            package_dir,
            root.join(PLUGIN_CONTRACT_MATRIX_FIXTURE_ARTIFACT)
        );
        for file in plugin_contract_matrix_fixture_asset().files {
            assert_eq!(
                fs::read(package_dir.join(file.relative_path)).expect("copied fixture file"),
                file.contents
            );
        }
    }

    #[test]
    fn published_plugin_contract_matrix_fixture_matches_repo_source_tree() {
        let repo_fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("fixtures")
            .join("plugins")
            .join("plugin-contract-matrix");
        let crate_fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures")
            .join("plugin-contract-matrix");

        assert_eq!(
            recursive_file_bytes(&crate_fixture),
            recursive_file_bytes(&repo_fixture)
        );
        assert_eq!(
            recursive_file_bytes(&repo_fixture),
            plugin_contract_matrix_fixture_asset()
                .files
                .iter()
                .map(|file| (file.relative_path.to_string(), file.contents.to_vec()))
                .collect::<BTreeMap<_, _>>()
        );
    }

    #[test]
    fn daemon_protocol_typescript_artifact_matches_checked_generated_file() {
        let artifact = daemon_protocol_typescript_artifact();
        let checked = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("botster-hub-client")
                .join("generated")
                .join("daemon-protocol.ts"),
        )
        .expect("read checked generated daemon protocol");

        assert_eq!(artifact.artifact_path, DAEMON_PROTOCOL_TYPESCRIPT_ARTIFACT);
        assert_eq!(artifact.contents, checked);
    }

    #[test]
    fn support_matrix_entity_capabilities_are_disjoint_and_complete() {
        let matrix = first_party_client_support_matrix();
        let mut declared = matrix.entity_actions.supported_capabilities.clone();
        declared.extend(matrix.entity_actions.unsupported_capabilities.clone());
        declared.sort();

        let mut known = vec![
            SUPPORTED_PLUGIN_SURFACE_JSON_ACTIONS.to_string(),
            UNSUPPORTED_PLUGIN_ENTITY_FRAMES.to_string(),
        ];
        known.sort();

        assert_eq!(declared, known);
        for supported in &matrix.entity_actions.supported_capabilities {
            assert!(
                !matrix
                    .entity_actions
                    .unsupported_capabilities
                    .contains(supported)
            );
        }
    }

    #[test]
    fn support_matrix_serializes_to_stable_json_shape() {
        let matrix = first_party_client_support_matrix();
        let value = serde_json::to_value(&matrix).expect("matrix serializes to JSON");

        assert_eq!(
            value,
            serde_json::json!({
                "protocol": botster_hub_client::PROTOCOL,
                "protocol_version": botster_hub_client::PROTOCOL_VERSION,
                "conformance_fixture_revision": botster_hub_client::CONFORMANCE_FIXTURE_REVISION,
                "required_features": [
                    botster_hub_client::FEATURE_SESSIONS,
                    botster_hub_client::FEATURE_TERMINAL_STREAMING,
                    botster_hub_client::FEATURE_RESIZE,
                    botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER,
                    botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION,
                    botster_hub_client::FEATURE_PACKAGE_ROUTES,
                    botster_hub_client::FEATURE_PACKAGE_NAVIGATION,
                    botster_hub_client::FEATURE_SPAWN_TARGETS,
                    botster_hub_client::FEATURE_WORKTREES,
                ],
                "supported_features": [
                    botster_hub_client::FEATURE_SESSIONS,
                    botster_hub_client::FEATURE_TERMINAL_STREAMING,
                    botster_hub_client::FEATURE_RESIZE,
                    botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER,
                    botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION,
                    botster_hub_client::FEATURE_PACKAGE_ROUTES,
                    botster_hub_client::FEATURE_PACKAGE_NAVIGATION,
                    botster_hub_client::FEATURE_SPAWN_TARGETS,
                    botster_hub_client::FEATURE_WORKTREES,
                ],
                "diagnostic_kinds": [
                    "connected",
                    "disconnected",
                    "compatibility_mismatch",
                    "unsupported_feature",
                    "terminal_stream_unavailable",
                    "action_failure",
                    "daemon_startup_failure",
                    "backpressure",
                ],
                "session_actions": [
                    "status",
                    "list_sessions",
                    "spawn",
                    "attach",
                    "drain",
                    "send_input",
                    "resize",
                    "shutdown_session",
                ],
                "terminal_streaming": {
                    "supported": true,
                    "feature": botster_hub_client::FEATURE_TERMINAL_STREAMING,
                    "helper": "botster_hub_client::stream_attach",
                    "held_open_stream": true,
                    "conformance_ready_output": CONFORMANCE_READY,
                    "conformance_echo_output": CONFORMANCE_ECHO,
                    "missing_session_diagnostic_kind": "terminal_stream_unavailable",
                },
                "resize": {
                    "supported": true,
                    "feature": botster_hub_client::FEATURE_RESIZE,
                    "action": "resize",
                    "conformance_output_prefix": CONFORMANCE_WINSIZE_PREFIX,
                },
                "plugin_surfaces": {
                    "render_supported": true,
                    "render_feature": botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER,
                    "action_supported": true,
                    "action_feature": botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION,
                    "package_name": PLUGIN_CONTRACT_MATRIX_PACKAGE,
                    "surface_id": PLUGIN_CONTRACT_APP_SURFACE,
                    "rendered_surface_kind": "panel",
                    "rendered_surface_node_id": "contract-app-panel",
                    "invalid_action_diagnostic_kind": "action_failure",
                },
                "entity_actions": {
                    "supported_capabilities": [SUPPORTED_PLUGIN_SURFACE_JSON_ACTIONS],
                    "unsupported_capabilities": [UNSUPPORTED_PLUGIN_ENTITY_FRAMES],
                },
                "late_attach_history": {
                    "supported": true,
                    "fixture_path": "botster_hub_test_support::late_attach_history_conformance_scenario",
                    "json_helper": "botster_hub_test_support::late_attach_history_conformance_fixture_json",
                    "event_type": "botster_hub_client::DaemonEvent",
                    "runtime_regression": "external_daemon_attach_replays_prior_history_with_renderable_byte_count",
                },
                "known_limitations": [
                    "The matrix is a test/docs contract, not a daemon runtime endpoint.",
                    "Full plugin entity-frame hydration is intentionally outside this conformance fixture.",
                    "Clients own renderer-specific presentation policy for diagnostics.",
                ],
            })
        );
    }

    #[test]
    fn late_attach_history_fixture_is_referenced_from_support_matrix() {
        let matrix = first_party_client_support_matrix();

        assert!(matrix.late_attach_history.supported);
        assert_eq!(
            matrix.late_attach_history.fixture_path,
            "botster_hub_test_support::late_attach_history_conformance_scenario"
        );
        assert_eq!(
            matrix.late_attach_history.json_helper,
            "botster_hub_test_support::late_attach_history_conformance_fixture_json"
        );
        assert_eq!(
            matrix.late_attach_history.event_type,
            "botster_hub_client::DaemonEvent"
        );
    }

    #[test]
    fn late_attach_history_fixture_orders_history_before_live_output() {
        let scenario = late_attach_history_conformance_scenario();

        let history_index = scenario
            .history_then_live
            .iter()
            .position(|event| {
                matches!(
                    event,
                    DaemonEvent::Snapshot { data, .. } | DaemonEvent::Scrollback { data, .. }
                        if data.contains("history-before-live")
                )
            })
            .expect("fixture includes restored history");
        let live_index = scenario
            .history_then_live
            .iter()
            .position(|event| {
                matches!(
                    event,
                    DaemonEvent::TerminalOutput { data, .. }
                        if data.contains("live-after-attach")
                )
            })
            .expect("fixture includes later live output");

        assert!(
            history_index < live_index,
            "restored history must precede later live output"
        );
    }

    #[test]
    fn late_attach_history_fixture_does_not_fabricate_no_history_events() {
        let scenario = late_attach_history_conformance_scenario();

        assert!(
            !scenario.no_history_then_live.iter().any(|event| {
                matches!(
                    event,
                    DaemonEvent::Snapshot { data, .. } | DaemonEvent::Scrollback { data, .. }
                        if !data.is_empty()
                )
            }),
            "no-history fixture must not contain non-empty snapshot or scrollback events"
        );
        assert!(
            scenario.no_history_then_live.iter().any(|event| {
                matches!(
                    event,
                    DaemonEvent::TerminalOutput { data, .. }
                        if data.contains("live-without-history")
                )
            }),
            "no-history fixture should still include later live terminal output"
        );
    }

    #[test]
    fn late_attach_history_fixture_byte_counts_match_renderable_data() {
        let scenario = late_attach_history_conformance_scenario();

        for event in scenario
            .history_then_live
            .iter()
            .chain(scenario.no_history_then_live.iter())
        {
            match event {
                DaemonEvent::Snapshot { data, bytes, .. }
                | DaemonEvent::Scrollback { data, bytes, .. } => {
                    assert_eq!(*bytes, data.len());
                    assert!(!data.is_empty());
                }
                _ => {}
            }
        }
    }

    #[test]
    fn late_attach_history_fixture_keeps_control_events_distinct_from_terminal_bytes() {
        let scenario = late_attach_history_conformance_scenario();
        let all_events = scenario
            .history_then_live
            .iter()
            .chain(scenario.no_history_then_live.iter())
            .collect::<Vec<_>>();

        assert!(all_events.iter().any(|event| {
            matches!(
                event,
                DaemonEvent::AttachState {
                    state,
                    ..
                } if state == "attached"
            )
        }));
        assert!(
            all_events
                .iter()
                .any(|event| matches!(event, DaemonEvent::ProcessExit { code: Some(0), .. }))
        );
        assert!(
            all_events.iter().all(|event| {
                !matches!(
                    event,
                    DaemonEvent::AttachState { .. } | DaemonEvent::ProcessExit { .. }
                ) || !matches!(
                    event,
                    DaemonEvent::TerminalOutput { .. }
                        | DaemonEvent::Snapshot { .. }
                        | DaemonEvent::Scrollback { .. }
                )
            }),
            "control events must stay separate from terminal byte events"
        );
    }

    #[test]
    fn late_attach_history_fixture_serializes_to_stable_client_json() {
        let value = late_attach_history_conformance_fixture_json();

        assert_eq!(
            value,
            serde_json::json!({
                "conformance_fixture_revision": botster_hub_client::CONFORMANCE_FIXTURE_REVISION,
                "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                "no_history_session_id": LATE_ATTACH_NO_HISTORY_SESSION_ID,
                "no_history_subscription_id": LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
                "history_then_live": [
                    {
                        "type": "attach_state",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "state": "attached",
                    },
                    {
                        "type": "snapshot",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "data": LATE_ATTACH_HISTORY_DATA,
                        "bytes": LATE_ATTACH_HISTORY_DATA.len(),
                    },
                    {
                        "type": "terminal_output",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "data": LATE_ATTACH_LIVE_DATA,
                    },
                    {
                        "type": "process_exit",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "code": 0,
                    },
                ],
                "no_history_then_live": [
                    {
                        "type": "attach_state",
                        "session_id": LATE_ATTACH_NO_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
                        "state": "attached",
                    },
                    {
                        "type": "terminal_output",
                        "session_id": LATE_ATTACH_NO_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
                        "data": LATE_ATTACH_NO_HISTORY_LIVE_DATA,
                    },
                    {
                        "type": "process_exit",
                        "session_id": LATE_ATTACH_NO_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
                        "code": 0,
                    },
                ],
            })
        );
    }

    #[test]
    fn plugin_conformance_failure_classes_are_distinct_and_stable() {
        let producer_error = ConformanceError::MissingRoute {
            route_id: "surface:contract.app",
        };
        let environment_error = IsolatedHubError::MissingBinaryEnv {
            variable: "__BOTSTER_HUB_TEST_MISSING_BIN",
        };
        let io_error = ConformanceError::Io {
            operation: "write_fixture",
            source: std::io::Error::other("fixture write failed"),
        };
        let child_error = ConformanceError::ChildFailed {
            operation: "foreground_app_child",
            status: "exit status: 1".to_string(),
            stdout: String::new(),
            stderr: "child failed".to_string(),
        };
        let attach_thread_error = ConformanceError::AttachThreadPanicked;
        let render_check = PluginContractMatrixClientRenderCheck {
            class: ConformanceFailureClass::ClientRendering,
            app_surface_node_id: "contract-app-panel".to_string(),
            app_surface_node_kinds: vec![
                "button".to_string(),
                "button".to_string(),
                "empty_state".to_string(),
                "empty_state".to_string(),
                "form".to_string(),
                "metric".to_string(),
                "metric_grid".to_string(),
                "panel".to_string(),
                "section".to_string(),
                "status_badge".to_string(),
                "table".to_string(),
                "text_input".to_string(),
                "toolbar".to_string(),
            ],
            empty_surface_child_id: "contract-empty-message".to_string(),
            settings_surface_node_id: "contract-settings-panel".to_string(),
            expected_redacted_secret_state: "redacted".to_string(),
        };

        assert_eq!(
            producer_error.failure_class(),
            ConformanceFailureClass::ProducerContract
        );
        assert_eq!(
            environment_error.failure_class(),
            ConformanceFailureClass::EnvironmentSetup
        );
        assert_eq!(
            io_error.failure_class(),
            ConformanceFailureClass::EnvironmentSetup
        );
        assert_eq!(
            child_error.failure_class(),
            ConformanceFailureClass::EnvironmentSetup
        );
        assert_eq!(
            attach_thread_error.failure_class(),
            ConformanceFailureClass::EnvironmentSetup
        );
        assert_eq!(render_check.class, ConformanceFailureClass::ClientRendering);
        assert_ne!(producer_error.failure_class(), render_check.class);
        assert_ne!(environment_error.failure_class(), render_check.class);
    }

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
            &error,
            IsolatedHubError::MissingBinary {
                label: "botster-hub binary",
                path
            } if path == &missing_hub
        ));
        let diagnostic = error.diagnostic();
        assert_eq!(diagnostic.kind, DaemonDiagnosticKind::DaemonStartupFailure);
        assert_eq!(
            diagnostic.message.as_deref(),
            Some("botster-hub binary is not available")
        );
        assert!(!format!("{diagnostic:?}").contains(&missing_hub.to_string_lossy().to_string()));
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

    fn recursive_file_bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
        let mut files = BTreeMap::new();
        collect_file_bytes(root, root, &mut files);
        files
    }

    fn collect_file_bytes(root: &Path, current: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(current).expect("read fixture directory") {
            let entry = entry.expect("read fixture entry");
            let path = entry.path();
            if path.is_dir() {
                collect_file_bytes(root, &path, files);
            } else {
                let relative_path = path
                    .strip_prefix(root)
                    .expect("fixture file under root")
                    .to_string_lossy()
                    .replace('\\', "/");
                files.insert(
                    relative_path,
                    fs::read(path).expect("read fixture file contents"),
                );
            }
        }
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
