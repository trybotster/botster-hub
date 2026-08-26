//! Frozen conformance fixtures, goldens, and support matrices.
//!
//! Runners stay in the crate root. Public root paths remain stable through
//! re-exports.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use botster_hub_client::{
    DaemonCompatibility, DaemonCompatibilityRequirement, DaemonDiagnosticKind, DaemonEntityFrame,
    DaemonEvent, DaemonLiveOutputPayload, DaemonModeFlags, DaemonRequest, DaemonResponseKind,
    DaemonSessionEntity, DaemonSessionType, DaemonSessionTypeDefinition,
    DaemonSessionTypeExecution, DaemonSessionTypeMutationSource, DaemonSessionTypeSource,
    DaemonSessionTypeWorkingDirectory,
};
use botster_ui_contract::{UiNode, realize_bind_list_descendant_id};
use serde::{Deserialize, Serialize};

pub(crate) const CONFORMANCE_SESSION_ID: &str = "botster-conformance-session";
pub(crate) const CONFORMANCE_SUBSCRIPTION_ID: &str = "botster-conformance-subscription";
pub(crate) const CONFORMANCE_READY: &str = "conformance-ready";
pub(crate) const CONFORMANCE_ECHO: &str = "echo:from-conformance";
pub(crate) const CONFORMANCE_WINSIZE_PREFIX: &str = "winsize:";
pub(crate) const LATE_ATTACH_HISTORY_SESSION_ID: &str = "late-attach-history-fixture-session";
pub(crate) const LATE_ATTACH_HISTORY_SUBSCRIPTION_ID: &str =
    "late-attach-history-fixture-subscription";
pub(crate) const LATE_ATTACH_NO_HISTORY_SESSION_ID: &str = "late-attach-no-history-fixture-session";
pub(crate) const LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID: &str =
    "late-attach-no-history-fixture-subscription";
pub(crate) const LATE_ATTACH_INCOMPLETE_SESSION_ID: &str = "late-attach-incomplete-fixture-session";
pub(crate) const LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID: &str =
    "late-attach-incomplete-fixture-subscription";
/// Frozen GHOSTSNP magic shared by both late-attach Core files.
pub(crate) const GHOSTSNP_MAGIC: &[u8] = b"GHOSTSNP";
/// Locked `botster-terminal-protocol` crate that owns the late-attach files.
pub(crate) const LATE_ATTACH_GHOSTSNP_PROTOCOL_CRATE: &str = "botster-terminal-protocol";
/// Git URL form that keeps protocol crate identity unified with hub-client.
pub(crate) const LATE_ATTACH_GHOSTSNP_PROTOCOL_GIT: &str =
    "https://github.com/trybotster/botster-core.git";
/// Core revision that owns the consumed `botster-terminal-protocol` files.
pub(crate) const LATE_ATTACH_GHOSTSNP_CORE_PIN: &str = "9cabdfd0588b6c7ed2e121e7b50086ce2a250ec6";
/// Ghostty submodule pin resolved by the locked Core revision.
pub(crate) const LATE_ATTACH_GHOSTSNP_GHOSTTY_PIN: &str =
    "eb72ec61304ea256be1d86ed8fa961c84e43ecbd";
/// Core-owned file names copied into OUT_DIR by this crate's build.rs.
pub(crate) const LATE_ATTACH_GHOSTSNP_FILES: &[&str] = &[
    "late-attach-history-ready-v2.ghostsnp",
    "late-attach-history-page-v2.ghostsnp",
    "late-attach-history-finish-v2.ghostsnp",
    "late-attach-blank-ready-v2.ghostsnp",
    "late-attach-blank-finish-v2.ghostsnp",
];
/// Incremental history READY frame from Core `late-attach-history-ready-v2.ghostsnp`.
pub(crate) const LATE_ATTACH_HISTORY_READY_PAYLOAD: &[u8] = include_bytes!(concat!(
    env!("OUT_DIR"),
    "/late-attach-history-ready-v2.ghostsnp"
));
pub(crate) const LATE_ATTACH_HISTORY_READY_PAYLOAD_LEN: usize = 2838;
pub(crate) const LATE_ATTACH_HISTORY_READY_PAYLOAD_SHA256: &str =
    "fbcdda31d682a61420251eed68f72e413485f057e3f374c57582955b0316bb6d";
/// Incremental history PAGE frame from Core `late-attach-history-page-v2.ghostsnp`.
pub(crate) const LATE_ATTACH_HISTORY_PAGE_PAYLOAD: &[u8] = include_bytes!(concat!(
    env!("OUT_DIR"),
    "/late-attach-history-page-v2.ghostsnp"
));
pub(crate) const LATE_ATTACH_HISTORY_PAGE_PAYLOAD_LEN: usize = 3365;
pub(crate) const LATE_ATTACH_HISTORY_PAGE_PAYLOAD_SHA256: &str =
    "b1b65d9d205f10a2cce4384ea15f0b6b20ee07bb3fda8e3bbdb8bd81dffb071f";
/// Incremental history FINISH frame from Core `late-attach-history-finish-v2.ghostsnp`.
pub(crate) const LATE_ATTACH_HISTORY_FINISH_PAYLOAD: &[u8] = include_bytes!(concat!(
    env!("OUT_DIR"),
    "/late-attach-history-finish-v2.ghostsnp"
));
pub(crate) const LATE_ATTACH_HISTORY_FINISH_PAYLOAD_LEN: usize = 10;
pub(crate) const LATE_ATTACH_HISTORY_FINISH_PAYLOAD_SHA256: &str =
    "6e0bfa87315d3225b0dedaa88387eb37c5cb31922b7891741445114bf19a3085";
/// First history Snapshot remains the READY frame for callers that still ask
/// for a single payload helper.
pub(crate) const LATE_ATTACH_HISTORY_PAYLOAD: &[u8] = LATE_ATTACH_HISTORY_READY_PAYLOAD;
pub(crate) const LATE_ATTACH_HISTORY_PAYLOAD_LEN: usize = LATE_ATTACH_HISTORY_READY_PAYLOAD_LEN;
pub(crate) const LATE_ATTACH_HISTORY_PAYLOAD_SHA256: &str =
    LATE_ATTACH_HISTORY_READY_PAYLOAD_SHA256;
/// Incremental blank READY frame from Core `late-attach-blank-ready-v2.ghostsnp`.
pub(crate) const LATE_ATTACH_NO_HISTORY_READY_PAYLOAD: &[u8] = include_bytes!(concat!(
    env!("OUT_DIR"),
    "/late-attach-blank-ready-v2.ghostsnp"
));
pub(crate) const LATE_ATTACH_NO_HISTORY_READY_PAYLOAD_LEN: usize = 1131;
pub(crate) const LATE_ATTACH_NO_HISTORY_READY_PAYLOAD_SHA256: &str =
    "06962b11d4a3acfb9b7c52b673a7b476904ddee2dd754b89b190ff82fdcfd0cc";
/// Incremental blank FINISH frame from Core `late-attach-blank-finish-v2.ghostsnp`.
pub(crate) const LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD: &[u8] = include_bytes!(concat!(
    env!("OUT_DIR"),
    "/late-attach-blank-finish-v2.ghostsnp"
));
pub(crate) const LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_LEN: usize = 26;
pub(crate) const LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_SHA256: &str =
    "a172e2380afec9ba9248735973f18965ee384ec2ae3440dbb4ddf4d5ced9d325";
pub(crate) const LATE_ATTACH_NO_HISTORY_PAYLOAD: &[u8] = LATE_ATTACH_NO_HISTORY_READY_PAYLOAD;
pub(crate) const LATE_ATTACH_NO_HISTORY_PAYLOAD_LEN: usize =
    LATE_ATTACH_NO_HISTORY_READY_PAYLOAD_LEN;
pub(crate) const LATE_ATTACH_NO_HISTORY_PAYLOAD_SHA256: &str =
    LATE_ATTACH_NO_HISTORY_READY_PAYLOAD_SHA256;
pub(crate) const LATE_ATTACH_HISTORY_SCREEN_TEXT: &str = "history-before-live\r\n";
pub(crate) const LATE_ATTACH_LIVE_DATA: &str = "live-after-attach\r\n";
pub(crate) const LATE_ATTACH_NO_HISTORY_LIVE_DATA: &str = "live-without-history\r\n";
pub(crate) const PROJECT_PIPELINES_PACKAGE: &str = "project-pipelines";
pub(crate) const PROJECT_PIPELINES_SURFACE: &str = "project-pipelines.create-ticket";
pub(crate) const PROJECT_PIPELINES_ACTION: &str = "project_pipelines.create_ticket";
pub(crate) const PLUGIN_CONTRACT_MATRIX_PACKAGE: &str = "botster.plugin-contract-matrix";
pub(crate) const PLUGIN_CONTRACT_MATRIX_FIXTURE_ARTIFACT: &str = "fixtures/plugin-contract-matrix";
pub(crate) const DAEMON_PROTOCOL_TYPESCRIPT_ARTIFACT: &str =
    "crates/botster-hub-client/generated/daemon-protocol.ts";
pub(crate) const PLUGIN_CONTRACT_APP_SURFACE: &str = "contract.app";
pub(crate) const PLUGIN_CONTRACT_EMPTY_SURFACE: &str = "contract.empty";
pub(crate) const PLUGIN_CONTRACT_SESSION_SURFACE: &str = "contract.sessions";
pub(crate) const PLUGIN_CONTRACT_ENTITY_SURFACE: &str = "contract.entities";
pub(crate) const PLUGIN_CONTRACT_ENTITY_FAMILY: &str =
    "bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run";
pub(crate) const PLUGIN_CONTRACT_BLOCKED_SURFACE: &str = "contract.blocked";
pub(crate) const PLUGIN_CONTRACT_INVALID_BODY_SURFACE: &str = "contract.invalid_body";
pub(crate) const PLUGIN_CONTRACT_SETTINGS_SURFACE: &str = "contract.settings";
pub(crate) const PLUGIN_CONTRACT_ACTION: &str = "contract.action";
pub(crate) const PLUGIN_CONTRACT_DIALOG_NODE_ID: &str = "contract-dialog";
pub(crate) const PLUGIN_CONTRACT_DIALOG_FORM_NODE_ID: &str = "contract-app-form";
pub(crate) const PLUGIN_CONTRACT_DIALOG_INPUT_NODE_ID: &str = "contract-app-message";
pub(crate) const PLUGIN_CONTRACT_ACCEPTED_REPLACEMENT_SCOPE: &str = "whole_surface";
pub(crate) const SUPPORTED_PLUGIN_SURFACE_JSON_ACTIONS: &str = "plugin_surface_json_actions";
pub(crate) const SUPPORTED_PLUGIN_ENTITY_FRAMES: &str = "plugin_entity_frames";
pub(crate) const SESSION_LIFECYCLE_ENTITY_TYPE: &str = "session";
pub(crate) const SESSION_LIFECYCLE_SESSION_ID: &str = "slc-session";
pub(crate) const SESSION_LIFECYCLE_FIRST_SUBSCRIPTION_ID: &str = "session-lifecycle-first";
pub(crate) const SESSION_LIFECYCLE_SECOND_SUBSCRIPTION_ID: &str = "session-lifecycle-second";
pub(crate) const SESSION_LIFECYCLE_RECONNECT_SUBSCRIPTION_ID: &str =
    "session-lifecycle-reconnected";
pub(crate) const SESSION_LIFECYCLE_OVERFLOW_REASON: &str = "subscriber_overflow";
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
    pub session_entities: SessionEntitySubscriptionSupport,
    pub resize: ResizeSupport,
    pub plugin_surfaces: PluginSurfaceSupport,
    pub entity_actions: EntityActionSupport,
    pub late_attach_history: LateAttachHistorySupport,
    pub terminal_mode_flags: TerminalModeFlagsSupport,
    pub session_type_authoring: SessionTypeAuthoringSupport,
    pub known_limitations: Vec<String>,
}

/// Lossless session-type authoring read published for first-party editors.
///
/// The sanitized `DaemonSessionType` row cannot reconstruct the authored
/// definition that `update_session_type` replaces wholesale, so an editor reads
/// `show_session_type_definition` instead. Every enumerated field here derives
/// from the public client DTOs rather than a hand-maintained list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTypeAuthoringSupport {
    pub supported: bool,
    pub request_type: String,
    pub response_kind: String,
    pub response_field: String,
    pub definition_type: String,
    pub editable_sources: Vec<String>,
    pub read_only_source: String,
    pub read_only_error_kind: String,
    /// Authored keys whose names never appear in the published `DaemonSessionType`
    /// row. `working_directory` and `environment` are the data-loss fields this
    /// read exists for; `context` is republished under the name `context_keys`.
    pub authored_fields_absent_from_published_row: Vec<String>,
    pub admission_group: String,
    pub runtime_regression: String,
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
pub struct SessionEntitySubscriptionSupport {
    pub supported: bool,
    pub feature: String,
    pub helper: String,
    pub frame_type: String,
    pub bounded_delivery: bool,
    pub explicit_snapshot_resync: bool,
    pub fixture_path: String,
    pub json_helper: String,
    pub runtime_runner: String,
    pub runtime_regression: String,
    pub binding_family: String,
    pub lifecycle_class_field: String,
    pub lifecycle_classes: Vec<String>,
    pub missing_row_state: String,
    pub plugin_surface_id: String,
    pub plugin_binding_fixture_path: String,
    pub reference_materializer: String,
    pub row_reference_materializer: String,
}

/// Source-derived session entity lifecycle contract shared by Rust and Node clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLifecycleSubscriptionConformanceScenario {
    pub conformance_fixture_revision: u16,
    pub entity_type: String,
    pub normalized_frames: Vec<DaemonEntityFrame>,
    pub fresh_subscription: FreshSubscriptionContract,
    pub overflow: SessionEntityOverflowContract,
}

/// Canonical plugin-authored `/session` binding scenario over public entity DTOs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionPluginBindingConformanceScenario {
    pub conformance_fixture_revision: u16,
    pub entity_type: String,
    pub binding_family: String,
    pub lifecycle_class_field: String,
    pub unavailable_state: String,
    pub references: Vec<String>,
    pub surface: serde_json::Value,
    pub initial_snapshot: DaemonEntityFrame,
    pub transition_frames: Vec<DaemonEntityFrame>,
    pub reconnect_snapshot: DaemonEntityFrame,
    pub expected: SessionPluginBindingExpectedStages,
    pub row_expected: SessionPluginRowExpectedStages,
}

/// Expected reference materialization after each authoritative frame stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPluginBindingExpectedStages {
    pub initial: BTreeMap<String, String>,
    pub after_ended_patch: BTreeMap<String, String>,
    pub after_indeterminate_patch: BTreeMap<String, String>,
    pub after_remove: BTreeMap<String, String>,
    pub after_reconnect: BTreeMap<String, String>,
}

/// Expected realized row identities after each authoritative frame stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPluginRowExpectedStages {
    pub initial: Vec<SessionPluginMaterializedRow>,
    pub after_ended_patch: Vec<SessionPluginMaterializedRow>,
    pub after_indeterminate_patch: Vec<SessionPluginMaterializedRow>,
    pub after_remove: Vec<SessionPluginMaterializedRow>,
    pub after_reconnect: Vec<SessionPluginMaterializedRow>,
}

/// One producer-backed BindList row after authored identity materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPluginMaterializedRow {
    pub node_id: String,
    pub controls: Vec<SessionPluginMaterializedControl>,
}

/// One identity-bearing control realized below a producer-backed BindList row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPluginMaterializedControl {
    pub key: String,
    pub node_id: String,
    pub label: String,
    pub action_payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshSubscriptionContract {
    pub prior_generation_frames_discarded: bool,
    pub requires_authoritative_snapshot_before_deltas: bool,
    pub snapshot: DaemonEntityFrame,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEntityOverflowContract {
    pub resync_reason: String,
    pub empty_snapshot_valid: bool,
    pub snapshot_precedes_later_deltas: bool,
    pub failed_snapshot_delivery_closes_subscription: bool,
    pub resync_snapshot: DaemonEntityFrame,
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
    pub runtime_runner: String,
    pub presentation_operation_kinds: Vec<String>,
    pub dialog_presence_key: String,
    pub dialog_form_node_id: String,
    pub dialog_input_node_id: String,
    pub actionable_sibling_form_forbidden: bool,
    pub accepted_replacement_scope: String,
    pub selected_workspace_equality_key: String,
    pub selected_workspace_equality_value: String,
    pub authored_set_values: BTreeMap<String, serde_json::Value>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalModeFlagsSupport {
    pub supported: bool,
    pub feature: String,
    pub fixture_path: String,
    pub json_helper: String,
    pub request_type: String,
    pub response_kind: String,
}

/// Public client-shaped scenario for late terminal attach state and screen restoration.
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
    pub read_screen_text: String,
    pub no_history_read_screen_text: String,
    pub history_then_live: Vec<DaemonEvent>,
    pub no_history_then_live: Vec<DaemonEvent>,
    pub history_incomplete_then_live: Vec<DaemonEvent>,
}

/// Public request/response conformance scenarios for authoritative terminal mode readback.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModeFlagsConformanceScenario {
    pub conformance_fixture_revision: u16,
    pub request: DaemonRequest,
    pub mouse_off: ModeFlagsConformanceSuccess,
    pub mouse_on: ModeFlagsConformanceSuccess,
    pub unknown_session: ModeFlagsConformanceFailure,
    pub backend_failure: ModeFlagsConformanceFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeFlagsConformanceSuccess {
    pub response_kind: DaemonResponseKind,
    pub mode_flags: DaemonModeFlags,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeFlagsConformanceFailure {
    pub response_kind: DaemonResponseKind,
    pub error_code: String,
    pub operation: String,
    pub mode_flags: Option<DaemonModeFlags>,
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

pub(crate) static PLUGIN_CONTRACT_MATRIX_FIXTURE_ASSET_FILES: &[TestAssetFile] = &[
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

pub(crate) const APPLICATION_PRIMITIVE_NODE_KINDS: &[&str] = &[
    "button",
    "button",
    "button",
    "dialog",
    "empty_state",
    "empty_state",
    "form",
    "metric",
    "metric_grid",
    "panel",
    "section",
    "status_badge",
    "table",
    "text",
    "text",
    "text",
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
        diagnostic_kinds: crate::daemon_diagnostic_kind_labels()
            .into_iter()
            .map(str::to_string)
            .collect(),
        session_actions: vec![
            "status".to_string(),
            "list_sessions".to_string(),
            "subscribe_entities".to_string(),
            "unsubscribe_entities".to_string(),
            "remove_session".to_string(),
            "spawn".to_string(),
            "attach".to_string(),
            "drain".to_string(),
            "send_input".to_string(),
            "resize".to_string(),
            "shutdown_session".to_string(),
        ],
        terminal_streaming: TerminalStreamingSupport {
            supported: true,
            feature: botster_terminal_protocol::FEATURE_TERMINAL_STREAMING.to_string(),
            helper: "botster_hub_client::stream_attach".to_string(),
            held_open_stream: true,
            conformance_ready_output: CONFORMANCE_READY.to_string(),
            conformance_echo_output: CONFORMANCE_ECHO.to_string(),
            missing_session_diagnostic_kind: crate::diagnostic_kind_label(
                DaemonDiagnosticKind::TerminalStreamUnavailable,
            )
            .to_string(),
        },
        session_entities: SessionEntitySubscriptionSupport {
            supported: true,
            feature: botster_hub_client::FEATURE_SESSION_ENTITY_SUBSCRIPTIONS.to_string(),
            helper: "botster_hub_client::subscribe_session_entities".to_string(),
            frame_type: "botster_hub_client::DaemonEntityFrame".to_string(),
            bounded_delivery: true,
            explicit_snapshot_resync: true,
            fixture_path:
                "botster_hub_test_support::session_lifecycle_subscription_conformance_scenario"
                    .to_string(),
            json_helper:
                "botster_hub_test_support::session_lifecycle_subscription_conformance_fixture_json"
                    .to_string(),
            runtime_runner:
                "botster_hub_test_support::run_session_lifecycle_subscription_conformance"
                    .to_string(),
            runtime_regression:
                "session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect"
                    .to_string(),
            binding_family: "/session".to_string(),
            lifecycle_class_field: "lifecycle_class".to_string(),
            lifecycle_classes: ["current", "ended", "indeterminate"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            missing_row_state: "unavailable".to_string(),
            plugin_surface_id: PLUGIN_CONTRACT_SESSION_SURFACE.to_string(),
            plugin_binding_fixture_path:
                "botster_hub_test_support::session_plugin_binding_conformance_scenario"
                    .to_string(),
            reference_materializer:
                "botster_hub_test_support::materialize_session_plugin_bindings".to_string(),
            row_reference_materializer:
                "botster_hub_test_support::materialize_session_plugin_rows".to_string(),
        },
        resize: ResizeSupport {
            supported: true,
            feature: botster_terminal_protocol::FEATURE_RESIZE.to_string(),
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
            invalid_action_diagnostic_kind: crate::diagnostic_kind_label(
                DaemonDiagnosticKind::ActionFailure,
            )
            .to_string(),
            runtime_runner: "botster_hub_test_support::run_plugin_contract_matrix_conformance"
                .to_string(),
            presentation_operation_kinds: crate::presentation_operation_kinds(),
            dialog_presence_key: PLUGIN_CONTRACT_DIALOG_NODE_ID.to_string(),
            dialog_form_node_id: PLUGIN_CONTRACT_DIALOG_FORM_NODE_ID.to_string(),
            dialog_input_node_id: PLUGIN_CONTRACT_DIALOG_INPUT_NODE_ID.to_string(),
            actionable_sibling_form_forbidden: true,
            accepted_replacement_scope: PLUGIN_CONTRACT_ACCEPTED_REPLACEMENT_SCOPE.to_string(),
            selected_workspace_equality_key: "selected-workspace".to_string(),
            selected_workspace_equality_value: "workspace-alpha".to_string(),
            authored_set_values: BTreeMap::from([
                ("contract-dialog".to_string(), serde_json::json!(true)),
                (
                    "selected-workspace".to_string(),
                    serde_json::json!("workspace-alpha"),
                ),
            ]),
        },
        entity_actions: EntityActionSupport {
            supported_capabilities: vec![
                SUPPORTED_PLUGIN_SURFACE_JSON_ACTIONS.to_string(),
                SUPPORTED_PLUGIN_ENTITY_FRAMES.to_string(),
            ],
            unsupported_capabilities: Vec::new(),
        },
        late_attach_history: LateAttachHistorySupport {
            supported: true,
            fixture_path: "botster_hub_test_support::late_attach_history_conformance_scenario"
                .to_string(),
            json_helper: "botster_hub_test_support::late_attach_history_conformance_fixture_json"
                .to_string(),
            event_type: "botster_hub_client::DaemonEvent".to_string(),
            runtime_regression:
                "external_daemon_same_session_reattach_replays_opaque_history_before_live_output"
                    .to_string(),
        },
        terminal_mode_flags: TerminalModeFlagsSupport {
            supported: true,
            feature: botster_hub_client::FEATURE_TERMINAL_READBACK.to_string(),
            fixture_path: "botster_hub_test_support::mode_flags_conformance_scenario".to_string(),
            json_helper: "botster_hub_test_support::mode_flags_conformance_fixture_json"
                .to_string(),
            request_type: "read_mode_flags".to_string(),
            response_kind: "read_mode_flags".to_string(),
        },
        session_type_authoring: session_type_authoring_support(),
        known_limitations: vec![
            "The matrix is a test/docs contract, not a daemon runtime endpoint.".to_string(),
            "Shipped Web/TUI binding resolution is owned by downstream client tickets; this Hub fixture is a producer/reference contract."
                .to_string(),
            "Clients own renderer-specific presentation policy for diagnostics.".to_string(),
        ],
    }
}

/// Derive the session-type authoring claims from the public client DTOs.
///
/// Every enumerated value comes from serializing a real DTO, so adding a
/// mutation source, renaming the request tag, or promoting an authored field
/// into the published row changes this output and fails the pinned snapshot.
fn session_type_authoring_support() -> SessionTypeAuthoringSupport {
    let request_type = serde_json::to_value(DaemonRequest::ShowSessionTypeDefinition {
        session_type_id: "init".to_string(),
    })
    .expect("serialize authoring request")["type"]
        .as_str()
        .expect("daemon requests are tagged by type")
        .to_string();
    let response_kind = serde_json::to_value(DaemonResponseKind::SessionTypeDefinition)
        .expect("serialize authoring response kind")
        .as_str()
        .expect("response kinds serialize as strings")
        .to_string();
    let source_tag = |source: DaemonSessionTypeMutationSource| {
        serde_json::to_value(source).expect("serialize mutation source")["source"]
            .as_str()
            .expect("mutation sources are tagged by source")
            .to_string()
    };

    let authored_keys = json_object_keys(&fully_populated_session_type_definition());
    let published_keys = json_object_keys(&fully_populated_session_type_row());

    SessionTypeAuthoringSupport {
        supported: true,
        request_type,
        response_kind,
        response_field: "session_type_definition".to_string(),
        definition_type: "botster_hub_client::DaemonSessionTypeEditableDefinition".to_string(),
        editable_sources: vec![
            source_tag(DaemonSessionTypeMutationSource::Device),
            source_tag(DaemonSessionTypeMutationSource::Repo {
                target_id: "repo:main".to_string(),
            }),
        ],
        read_only_source: source_tag(DaemonSessionTypeMutationSource::Package {
            package_name: "read-only.plugin".to_string(),
        }),
        read_only_error_kind: "read_only_session_type_source".to_string(),
        authored_fields_absent_from_published_row: authored_keys
            .difference(&published_keys)
            .cloned()
            .collect(),
        admission_group: "allow_runtime".to_string(),
        runtime_regression: "session_type_definition_round_trips_authored_path_and_environment"
            .to_string(),
    }
}

fn json_object_keys<T: Serialize>(value: &T) -> BTreeSet<String> {
    serde_json::to_value(value)
        .expect("serialize session type shape")
        .as_object()
        .expect("session type shapes serialize as objects")
        .keys()
        .cloned()
        .collect()
}

/// A definition with every optional field set, so `skip_serializing_if` cannot
/// hide an authored key from the published-row comparison above.
fn fully_populated_session_type_definition() -> DaemonSessionTypeDefinition {
    DaemonSessionTypeDefinition {
        id: "init".to_string(),
        label: "Init".to_string(),
        description: Some("Authoring example".to_string()),
        icon: Some("terminal".to_string()),
        role: "botster.agent".to_string(),
        interaction: "interactive".to_string(),
        traits: vec!["terminal".to_string()],
        lifecycle: "task".to_string(),
        execution: DaemonSessionTypeExecution::RelativeExecutable,
        command: "bin/init.sh".to_string(),
        args: vec!["--json".to_string()],
        working_directory: DaemonSessionTypeWorkingDirectory::Relative {
            path: "nested/dir".to_string(),
        },
        environment: BTreeMap::from([("BOTSTER_MODE".to_string(), "authoring".to_string())]),
        allowed_environment_overrides: vec!["BOTSTER_MODE".to_string()],
        context: vec!["prompt".to_string()],
        target_id: Some("device:local".to_string()),
    }
}

fn fully_populated_session_type_row() -> DaemonSessionType {
    DaemonSessionType {
        session_type_id: "device/init".to_string(),
        source_name: "device".to_string(),
        id: "init".to_string(),
        source: "device".to_string(),
        editable: true,
        overridden_sources: vec![DaemonSessionTypeSource {
            kind: "package".to_string(),
            name: "workflow.plugin".to_string(),
        }],
        diagnostics: vec!["overrides 1 lower-precedence definition(s)".to_string()],
        label: "Init".to_string(),
        description: Some("Authoring example".to_string()),
        icon: Some("terminal".to_string()),
        role: "botster.agent".to_string(),
        interaction: "interactive".to_string(),
        traits: vec!["terminal".to_string()],
        lifecycle: "task".to_string(),
        execution: DaemonSessionTypeExecution::RelativeExecutable,
        command: "bin/init.sh".to_string(),
        args: vec!["--json".to_string()],
        working_directory_policy: "relative".to_string(),
        allowed_environment_overrides: vec!["BOTSTER_MODE".to_string()],
        context_keys: vec!["prompt".to_string()],
        target_id: "device:local".to_string(),
        available: true,
    }
}

/// Return the normalized public-DTO session lifecycle subscription contract.
#[must_use]
pub fn session_lifecycle_subscription_conformance_scenario()
-> SessionLifecycleSubscriptionConformanceScenario {
    let generation_one = "session-lifecycle-generation-1";
    let generation_two = "session-lifecycle-generation-2";
    let entity = DaemonSessionEntity {
        session_uuid: SESSION_LIFECYCLE_SESSION_ID.to_string(),
        registry_state: "active".to_string(),
        lifecycle: Some("running".to_string()),
        lifecycle_class: "current".to_string(),
        rows: 24,
        cols: 80,
        updated_at: 1,
        exit_code: None,
        failure_reason: None,
        session_type_id: None,
        session_type_source: None,
        role: None,
        traits: Vec::new(),
        interaction: None,
        session_type_lifecycle: None,
    };

    SessionLifecycleSubscriptionConformanceScenario {
        conformance_fixture_revision: botster_hub_client::CONFORMANCE_FIXTURE_REVISION,
        entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
        normalized_frames: vec![
            DaemonEntityFrame::Snapshot {
                subscription_id: generation_one.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 0,
                items: Vec::new(),
                resync_reason: None,
            },
            DaemonEntityFrame::Upsert {
                subscription_id: generation_one.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 1,
                id: SESSION_LIFECYCLE_SESSION_ID.to_string(),
                entity: serde_json::to_value(entity).expect("serialize session entity"),
            },
            DaemonEntityFrame::Patch {
                subscription_id: generation_one.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 2,
                id: SESSION_LIFECYCLE_SESSION_ID.to_string(),
                patch: serde_json::json!({ "rows": 31, "cols": 101, "updated_at": 2 }),
            },
            DaemonEntityFrame::Patch {
                subscription_id: generation_one.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 3,
                id: SESSION_LIFECYCLE_SESSION_ID.to_string(),
                patch: serde_json::json!({
                    "lifecycle": "exited",
                    "lifecycle_class": "ended",
                    "exit_code": 0,
                    "updated_at": 3
                }),
            },
            DaemonEntityFrame::Remove {
                subscription_id: generation_one.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 4,
                id: SESSION_LIFECYCLE_SESSION_ID.to_string(),
            },
        ],
        fresh_subscription: FreshSubscriptionContract {
            prior_generation_frames_discarded: true,
            requires_authoritative_snapshot_before_deltas: true,
            snapshot: DaemonEntityFrame::Snapshot {
                subscription_id: generation_two.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 4,
                items: Vec::new(),
                resync_reason: None,
            },
        },
        overflow: SessionEntityOverflowContract {
            resync_reason: SESSION_LIFECYCLE_OVERFLOW_REASON.to_string(),
            empty_snapshot_valid: true,
            snapshot_precedes_later_deltas: true,
            failed_snapshot_delivery_closes_subscription: true,
            resync_snapshot: DaemonEntityFrame::Snapshot {
                subscription_id: generation_one.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 5,
                items: Vec::new(),
                resync_reason: Some(SESSION_LIFECYCLE_OVERFLOW_REASON.to_string()),
            },
        },
    }
}

/// Return the shared plugin surface and public-frame lifecycle binding scenario.
#[must_use]
pub fn session_plugin_binding_conformance_scenario() -> SessionPluginBindingConformanceScenario {
    const TRANSITION: &str = "session-transition";
    const STABLE: &str = "session-stable-current";
    const ENDED: &str = "session-ended";
    const INDETERMINATE: &str = "session-indeterminate";
    const MISSING: &str = "session-missing";
    const GENERATION_ONE: &str = "session-plugin-binding-generation-1";
    const GENERATION_TWO: &str = "session-plugin-binding-generation-2";

    let references = [TRANSITION, STABLE, ENDED, INDETERMINATE, MISSING]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let row = |session_uuid: &str,
               registry_state: &str,
               lifecycle: Option<&str>,
               lifecycle_class: &str,
               updated_at: u64| {
        serde_json::to_value(DaemonSessionEntity {
            session_uuid: session_uuid.to_string(),
            registry_state: registry_state.to_string(),
            lifecycle: lifecycle.map(str::to_string),
            lifecycle_class: lifecycle_class.to_string(),
            rows: 24,
            cols: 80,
            updated_at,
            exit_code: (lifecycle == Some("exited")).then_some(0),
            failure_reason: None,
            session_type_id: None,
            session_type_source: None,
            role: None,
            traits: Vec::new(),
            interaction: None,
            session_type_lifecycle: None,
        })
        .expect("serialize session entity")
    };
    let initial_items = vec![
        row(TRANSITION, "running", Some("running"), "current", 1),
        row(STABLE, "running", Some("running"), "current", 1),
        row(ENDED, "exited", Some("exited"), "ended", 1),
        row(INDETERMINATE, "stale", Some("running"), "indeterminate", 1),
    ];
    let expected = |transition: &str| {
        BTreeMap::from([
            (TRANSITION.to_string(), transition.to_string()),
            (STABLE.to_string(), "current".to_string()),
            (ENDED.to_string(), "ended".to_string()),
            (INDETERMINATE.to_string(), "indeterminate".to_string()),
            (MISSING.to_string(), "unavailable".to_string()),
        ])
    };
    let mut after_remove = expected("unavailable");
    after_remove.insert(TRANSITION.to_string(), "unavailable".to_string());

    SessionPluginBindingConformanceScenario {
        conformance_fixture_revision: botster_hub_client::CONFORMANCE_FIXTURE_REVISION,
        entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
        binding_family: "/session".to_string(),
        lifecycle_class_field: "lifecycle_class".to_string(),
        unavailable_state: "unavailable".to_string(),
        references: references.clone(),
        surface: session_plugin_binding_surface(&references),
        initial_snapshot: DaemonEntityFrame::Snapshot {
            subscription_id: GENERATION_ONE.to_string(),
            entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
            snapshot_seq: 1,
            items: initial_items,
            resync_reason: None,
        },
        transition_frames: vec![
            DaemonEntityFrame::Patch {
                subscription_id: GENERATION_ONE.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 2,
                id: TRANSITION.to_string(),
                patch: serde_json::json!({
                    "registry_state": "exited",
                    "lifecycle": "exited",
                    "lifecycle_class": "ended",
                    "exit_code": 0,
                    "updated_at": 2
                }),
            },
            DaemonEntityFrame::Patch {
                subscription_id: GENERATION_ONE.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 3,
                id: TRANSITION.to_string(),
                patch: serde_json::json!({
                    "registry_state": "stale",
                    "lifecycle_class": "indeterminate",
                    "updated_at": 3
                }),
            },
            DaemonEntityFrame::Remove {
                subscription_id: GENERATION_ONE.to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 4,
                id: TRANSITION.to_string(),
            },
        ],
        reconnect_snapshot: DaemonEntityFrame::Snapshot {
            subscription_id: GENERATION_TWO.to_string(),
            entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
            snapshot_seq: 4,
            items: vec![
                row(STABLE, "running", Some("running"), "current", 4),
                row(ENDED, "exited", Some("exited"), "ended", 4),
                row(INDETERMINATE, "stale", None, "indeterminate", 4),
            ],
            resync_reason: None,
        },
        expected: SessionPluginBindingExpectedStages {
            initial: expected("current"),
            after_ended_patch: expected("ended"),
            after_indeterminate_patch: expected("indeterminate"),
            after_remove,
            after_reconnect: expected("unavailable"),
        },
        row_expected: SessionPluginRowExpectedStages {
            initial: vec![
                expected_session_plugin_materialized_row(TRANSITION),
                expected_session_plugin_materialized_row(STABLE),
            ],
            after_ended_patch: vec![expected_session_plugin_materialized_row(STABLE)],
            after_indeterminate_patch: vec![expected_session_plugin_materialized_row(STABLE)],
            after_remove: vec![expected_session_plugin_materialized_row(STABLE)],
            after_reconnect: vec![expected_session_plugin_materialized_row(STABLE)],
        },
    }
}

fn session_plugin_binding_surface(references: &[String]) -> serde_json::Value {
    serde_json::json!({
        "type": "panel",
        "id": "contract-session-lifecycle-panel",
        "props": { "title": "Session lifecycle projection" },
        "children": references.iter().enumerate().map(|(index, session_uuid)| {
            serde_json::json!({
                "$kind": "bind_list",
                "source": "/session",
                "where": { "session_uuid": session_uuid },
                "item_template": {
                    "type": "text",
                    "id": format!("contract-session-{}-lifecycle", index + 1),
                    "props": { "text": { "$bind": "@/lifecycle_class" } }
                },
                "empty_template": {
                    "type": "text",
                    "id": format!("contract-session-{}-unavailable", index + 1),
                    "props": { "text": "Session unavailable" }
                }
            })
        }).chain(std::iter::once(serde_json::json!({
            "$kind": "bind_list",
            "source": "/session",
            "where": { "lifecycle_class": "current" },
            "item_template": {
                "type": "inline",
                "id": { "$bind": "@/session_uuid" },
                "children": [
                    {
                        "type": "button",
                        "id": { "$kind": "bind_list_descendant_id", "key": "spawn" },
                        "props": {
                            "label": { "$bind": "@/lifecycle_class" },
                            "action": {
                                "id": "contract.action",
                                "payload": {
                                    "operation": "spawn",
                                    "session_uuid": { "$bind": "@/session_uuid" }
                                }
                            }
                        }
                    },
                    {
                        "type": "button",
                        "id": { "$kind": "bind_list_descendant_id", "key": "rename" },
                        "props": {
                            "label": "Rename session",
                            "action": {
                                "id": "contract.action",
                                "payload": {
                                    "operation": "rename",
                                    "session_uuid": { "$bind": "@/session_uuid" }
                                }
                            }
                        }
                    },
                    {
                        "type": "button",
                        "id": { "$kind": "bind_list_descendant_id", "key": "remove" },
                        "props": {
                            "label": "Remove session",
                            "action": {
                                "id": "contract.action",
                                "payload": {
                                    "operation": "remove",
                                    "session_uuid": { "$bind": "@/session_uuid" }
                                }
                            }
                        }
                    }
                ]
            }
        }))).collect::<Vec<_>>()
    })
}

fn expected_session_plugin_materialized_row(node_id: &str) -> SessionPluginMaterializedRow {
    SessionPluginMaterializedRow {
        node_id: node_id.to_string(),
        controls: ["spawn", "rename", "remove"]
            .into_iter()
            .map(|key| SessionPluginMaterializedControl {
                key: key.to_string(),
                node_id: realize_bind_list_descendant_id(node_id, key)
                    .expect("fixture row and control keys are nonblank")
                    .0,
                label: if key == "spawn" {
                    "current".to_string()
                } else {
                    format!("{} session", uppercase_first(key))
                },
                action_payload: serde_json::json!({
                    "operation": key,
                    "session_uuid": node_id
                }),
            })
            .collect(),
    }
}

fn expected_session_plugin_identity_oracle() -> serde_json::Value {
    serde_json::json!({
        "$kind": "bind_list",
        "source": "/session",
        "where": { "lifecycle_class": "current" },
        "item_template": {
            "type": "inline",
            "id": { "$bind": "@/session_uuid" },
            "children": [
                {
                    "type": "button",
                    "id": { "$kind": "bind_list_descendant_id", "key": "spawn" },
                    "props": {
                        "label": { "$bind": "@/lifecycle_class" },
                        "action": {
                            "id": "contract.action",
                            "payload": {
                                "operation": "spawn",
                                "session_uuid": { "$bind": "@/session_uuid" }
                            }
                        }
                    }
                },
                {
                    "type": "button",
                    "id": { "$kind": "bind_list_descendant_id", "key": "rename" },
                    "props": {
                        "label": "Rename session",
                        "action": {
                            "id": "contract.action",
                            "payload": {
                                "operation": "rename",
                                "session_uuid": { "$bind": "@/session_uuid" }
                            }
                        }
                    }
                },
                {
                    "type": "button",
                    "id": { "$kind": "bind_list_descendant_id", "key": "remove" },
                    "props": {
                        "label": "Remove session",
                        "action": {
                            "id": "contract.action",
                            "payload": {
                                "operation": "remove",
                                "session_uuid": { "$bind": "@/session_uuid" }
                            }
                        }
                    }
                }
            ]
        }
    })
}

fn inspect_session_plugin_surface(
    surface: &serde_json::Value,
) -> Result<(Vec<String>, &serde_json::Value), String> {
    let children = surface
        .get("children")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "session binding surface children are missing".to_string())?;
    let mut references = Vec::new();
    let mut oracle = None;

    for child in children {
        if let Some(session_uuid) = child
            .pointer("/where/session_uuid")
            .and_then(serde_json::Value::as_str)
        {
            let expected = serde_json::json!({
                "$kind": "bind_list",
                "source": "/session",
                "where": { "session_uuid": session_uuid },
                "item_template": {
                    "type": "text",
                    "id": format!("contract-session-{}-lifecycle", references.len() + 1),
                    "props": { "text": { "$bind": "@/lifecycle_class" } }
                },
                "empty_template": {
                    "type": "text",
                    "id": format!("contract-session-{}-unavailable", references.len() + 1),
                    "props": { "text": "Session unavailable" }
                }
            });
            if child != &expected {
                return Err(
                    "surface does not use the canonical /session binding grammar".to_string(),
                );
            }
            references.push(session_uuid.to_string());
            continue;
        }

        let expected_oracle = expected_session_plugin_identity_oracle();
        if child != &expected_oracle {
            return Err("surface contains an unrecognized session binding child".to_string());
        }
        if oracle.replace(child).is_some() {
            return Err("surface contains duplicate current-row identity oracles".to_string());
        }
    }

    let oracle =
        oracle.ok_or_else(|| "surface is missing the current-row identity oracle".to_string())?;
    Ok((references, oracle))
}

fn materialize_session_entities(
    frames: &[DaemonEntityFrame],
) -> Result<Vec<(String, serde_json::Value)>, String> {
    let mut entities = Vec::<(String, serde_json::Value)>::new();
    for frame in frames {
        match frame {
            DaemonEntityFrame::Snapshot {
                entity_type, items, ..
            } if entity_type == SESSION_LIFECYCLE_ENTITY_TYPE => {
                entities = items
                    .iter()
                    .map(|item| {
                        item.get("session_uuid")
                            .and_then(serde_json::Value::as_str)
                            .map(|id| (id.to_string(), item.clone()))
                            .ok_or_else(|| "session entity requires session_uuid".to_string())
                    })
                    .collect::<Result<_, _>>()?;
            }
            DaemonEntityFrame::Upsert {
                entity_type,
                id,
                entity,
                ..
            } if entity_type == SESSION_LIFECYCLE_ENTITY_TYPE => {
                let value = entity.clone();
                if let Some((_, current)) =
                    entities.iter_mut().find(|(entity_id, _)| entity_id == id)
                {
                    *current = value;
                } else {
                    entities.push((id.clone(), value));
                }
            }
            DaemonEntityFrame::Patch {
                entity_type,
                id,
                patch,
                ..
            } if entity_type == SESSION_LIFECYCLE_ENTITY_TYPE => {
                let (_, entity) = entities
                    .iter_mut()
                    .find(|(entity_id, _)| entity_id == id)
                    .ok_or_else(|| format!("patch references unknown session row {id}"))?;
                merge_patch(entity, patch);
            }
            DaemonEntityFrame::Remove {
                entity_type, id, ..
            } if entity_type == SESSION_LIFECYCLE_ENTITY_TYPE => {
                entities.retain(|(entity_id, _)| entity_id != id);
            }
            _ => return Err("scenario contains a foreign entity family".to_string()),
        }
    }
    Ok(entities)
}

/// Apply public session entity frames and resolve the delivered fixture bindings.
pub fn materialize_session_plugin_bindings(
    surface: &serde_json::Value,
    frames: &[DaemonEntityFrame],
) -> Result<BTreeMap<String, String>, String> {
    let (references, _) = inspect_session_plugin_surface(surface)?;
    let entities = materialize_session_entities(frames)?;

    references
        .into_iter()
        .map(|session_uuid| {
            let Some((_, entity)) = entities
                .iter()
                .find(|(entity_id, _)| entity_id == &session_uuid)
            else {
                return Ok((session_uuid, "unavailable".to_string()));
            };
            let lifecycle_class = entity
                .get("lifecycle_class")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    format!("present session row {session_uuid} is missing lifecycle_class")
                })?
                .to_string();
            Ok((session_uuid, lifecycle_class))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()
}

/// Apply public session frames and realize the canonical current-row identities.
pub fn materialize_session_plugin_rows(
    surface: &serde_json::Value,
    frames: &[DaemonEntityFrame],
) -> Result<Vec<SessionPluginMaterializedRow>, String> {
    let (_, oracle) = inspect_session_plugin_surface(surface)?;
    let expected_class = oracle
        .pointer("/where/lifecycle_class")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "canonical oracle is missing its lifecycle_class filter".to_string())?;
    let entities = materialize_session_entities(frames)?;
    let mut seen = BTreeSet::new();
    if let Some(root_id) = surface.get("id").and_then(serde_json::Value::as_str) {
        insert_realized_node_id(&mut seen, root_id)?;
    }
    for child in surface["children"]
        .as_array()
        .ok_or_else(|| "session binding surface children are missing".to_string())?
    {
        if std::ptr::eq(child, oracle) {
            continue;
        }
        let session_uuid = child
            .pointer("/where/session_uuid")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "canonical session reference is missing its filter".to_string())?;
        let branch = if entities.iter().any(|(_, entity)| {
            entity
                .get("session_uuid")
                .and_then(serde_json::Value::as_str)
                == Some(session_uuid)
        }) {
            child.pointer("/item_template")
        } else {
            child.pointer("/empty_template")
        }
        .ok_or_else(|| "canonical session reference is missing its realized branch".to_string())?;
        collect_literal_node_ids(branch, &mut seen)?;
    }
    let controls = oracle
        .pointer("/item_template/children")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "canonical oracle is missing identity-bearing controls".to_string())?;
    let mut rows = Vec::new();

    for (_, entity) in entities {
        if entity
            .get("lifecycle_class")
            .and_then(serde_json::Value::as_str)
            != Some(expected_class)
        {
            continue;
        }
        let node_id = entity
            .get("session_uuid")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "selected session row is missing string session_uuid".to_string())?;
        if node_id.trim().is_empty() {
            return Err("selected session row has blank session_uuid".to_string());
        }
        insert_realized_node_id(&mut seen, node_id)?;
        let controls = controls
            .iter()
            .map(|control| {
                let key = control
                    .pointer("/id/key")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| "identity-bearing control is missing string key".to_string())?;
                let realized = realize_bind_list_descendant_id(node_id, key)
                    .map_err(|error| error.to_string())?
                    .0;
                insert_realized_node_id(&mut seen, &realized)?;
                let label = materialize_control_label(control, &entity)?;
                let action_payload = serde_json::json!({
                    "operation": key,
                    "session_uuid": node_id
                });
                let realized_control: UiNode = serde_json::from_value(serde_json::json!({
                    "type": "button",
                    "id": realized,
                    "props": {
                        "label": label,
                        "action": {
                            "id": "contract.action",
                            "payload": action_payload
                        }
                    }
                }))
                .map_err(|error| format!("materialized control is not a UiNode: {error}"))?;
                realized_control
                    .validate_realized()
                    .map_err(|error| format!("materialized control is not realized: {error}"))?;
                Ok(SessionPluginMaterializedControl {
                    key: key.to_string(),
                    node_id: realized,
                    label,
                    action_payload,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        rows.push(SessionPluginMaterializedRow {
            node_id: node_id.to_string(),
            controls,
        });
    }
    Ok(rows)
}

fn materialize_control_label(
    control: &serde_json::Value,
    entity: &serde_json::Value,
) -> Result<String, String> {
    let label = control
        .pointer("/props/label")
        .ok_or_else(|| "identity-bearing control is missing label".to_string())?;
    if let Some(label) = label.as_str() {
        return Ok(label.to_string());
    }
    let path = label
        .get("$bind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "identity-bearing control label is not a string or bind".to_string())?;
    let field = path
        .strip_prefix("@/")
        .ok_or_else(|| "identity-bearing control label bind must be item-relative".to_string())?;
    entity
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("selected session row is missing string {field}"))
}

fn uppercase_first(value: &str) -> String {
    let mut chars = value.chars();
    chars
        .next()
        .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
        .unwrap_or_default()
}

fn insert_realized_node_id(seen: &mut BTreeSet<String>, node_id: &str) -> Result<(), String> {
    if node_id.trim().is_empty() {
        return Err("realized node id cannot be blank".to_string());
    }
    if !seen.insert(node_id.to_string()) {
        return Err(format!("duplicate realized node id {node_id}"));
    }
    Ok(())
}

fn collect_literal_node_ids(
    value: &serde_json::Value,
    seen: &mut BTreeSet<String>,
) -> Result<(), String> {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_literal_node_ids(value, seen)?;
            }
        }
        serde_json::Value::Object(object) => {
            if object.contains_key("type")
                && let Some(node_id) = object.get("id").and_then(serde_json::Value::as_str)
            {
                insert_realized_node_id(seen, node_id)?;
            }
            for (key, value) in object {
                if key != "id" {
                    collect_literal_node_ids(value, seen)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    match patch {
        serde_json::Value::Object(patch) => {
            if !target.is_object() {
                *target = serde_json::json!({});
            }
            let target = target.as_object_mut().expect("target converted to object");
            for (key, value) in patch {
                if value.is_null() {
                    target.remove(key);
                } else {
                    merge_patch(
                        target.entry(key.clone()).or_insert(serde_json::Value::Null),
                        value,
                    );
                }
            }
        }
        value => *target = value.clone(),
    }
}

/// Return the stable JSON value generated for Node reference consumers.
#[must_use]
pub fn session_plugin_binding_conformance_fixture_json() -> serde_json::Value {
    serde_json::to_value(session_plugin_binding_conformance_scenario())
        .expect("session plugin binding conformance scenario serializes")
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
        read_screen_text: LATE_ATTACH_HISTORY_SCREEN_TEXT.to_string(),
        no_history_read_screen_text: String::new(),
        history_then_live: late_attach_history_events(),
        no_history_then_live: late_attach_no_history_events(),
        history_incomplete_then_live: late_attach_history_incomplete_events(),
    }
}

/// Return deterministic exact-value and error-preservation mode readback scenarios.
#[must_use]
pub fn mode_flags_conformance_scenario() -> ModeFlagsConformanceScenario {
    const SESSION_ID: &str = "mode-flags-fixture-session";

    ModeFlagsConformanceScenario {
        conformance_fixture_revision: botster_hub_client::CONFORMANCE_FIXTURE_REVISION,
        request: DaemonRequest::ReadModeFlags {
            session_id: SESSION_ID.to_string(),
        },
        mouse_off: ModeFlagsConformanceSuccess {
            response_kind: DaemonResponseKind::ReadModeFlags,
            mode_flags: DaemonModeFlags::new(
                SESSION_ID, false, true, false, 0, false, false, false, 1, 1,
            ),
        },
        mouse_on: ModeFlagsConformanceSuccess {
            response_kind: DaemonResponseKind::ReadModeFlags,
            mode_flags: DaemonModeFlags::new(
                SESSION_ID, false, true, false, 9, false, false, false, 1, 2,
            ),
        },
        unknown_session: ModeFlagsConformanceFailure {
            response_kind: DaemonResponseKind::OperatorError,
            error_code: "unknown_session".to_string(),
            operation: "read_mode_flags".to_string(),
            mode_flags: None,
        },
        backend_failure: ModeFlagsConformanceFailure {
            response_kind: DaemonResponseKind::OperatorError,
            error_code: "runtime_error".to_string(),
            operation: "read_mode_flags".to_string(),
            mode_flags: None,
        },
    }
}

fn snapshot_event(session_id: &str, subscription_id: &str, bytes: &[u8]) -> DaemonEvent {
    DaemonEvent::Snapshot {
        session_id: session_id.to_string(),
        subscription_id: subscription_id.to_string(),
        history: botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(bytes),
    }
}

/// Incremental late-attach sequence: READY, PAGE, FINISH, attached, live, exit.
#[must_use]
pub fn late_attach_history_events() -> Vec<DaemonEvent> {
    vec![
        DaemonEvent::AttachState {
            session_id: LATE_ATTACH_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_HISTORY_SUBSCRIPTION_ID.to_string(),
            state: "attaching".to_string(),
        },
        snapshot_event(
            LATE_ATTACH_HISTORY_SESSION_ID,
            LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
            LATE_ATTACH_HISTORY_READY_PAYLOAD,
        ),
        snapshot_event(
            LATE_ATTACH_HISTORY_SESSION_ID,
            LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
            LATE_ATTACH_HISTORY_PAGE_PAYLOAD,
        ),
        snapshot_event(
            LATE_ATTACH_HISTORY_SESSION_ID,
            LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
            LATE_ATTACH_HISTORY_FINISH_PAYLOAD,
        ),
        DaemonEvent::AttachState {
            session_id: LATE_ATTACH_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_HISTORY_SUBSCRIPTION_ID.to_string(),
            state: "attached".to_string(),
        },
        DaemonEvent::TerminalOutput {
            session_id: LATE_ATTACH_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_HISTORY_SUBSCRIPTION_ID.to_string(),
            payload: DaemonLiveOutputPayload::from_bytes(LATE_ATTACH_LIVE_DATA.as_bytes()),
        },
        DaemonEvent::ProcessExit {
            session_id: LATE_ATTACH_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_HISTORY_SUBSCRIPTION_ID.to_string(),
            code: Some(0),
        },
    ]
}

/// Empty-history late-attach sequence: READY, FINISH, attached, live, exit.
#[must_use]
pub fn late_attach_no_history_events() -> Vec<DaemonEvent> {
    vec![
        DaemonEvent::AttachState {
            session_id: LATE_ATTACH_NO_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID.to_string(),
            state: "attaching".to_string(),
        },
        snapshot_event(
            LATE_ATTACH_NO_HISTORY_SESSION_ID,
            LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
            LATE_ATTACH_NO_HISTORY_READY_PAYLOAD,
        ),
        snapshot_event(
            LATE_ATTACH_NO_HISTORY_SESSION_ID,
            LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
            LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD,
        ),
        DaemonEvent::AttachState {
            session_id: LATE_ATTACH_NO_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID.to_string(),
            state: "attached".to_string(),
        },
        DaemonEvent::TerminalOutput {
            session_id: LATE_ATTACH_NO_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID.to_string(),
            payload: DaemonLiveOutputPayload::from_bytes(
                LATE_ATTACH_NO_HISTORY_LIVE_DATA.as_bytes(),
            ),
        },
        DaemonEvent::ProcessExit {
            session_id: LATE_ATTACH_NO_HISTORY_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID.to_string(),
            code: Some(0),
        },
    ]
}

/// Post-READY history failure: READY, snapshot_history_incomplete, attached, live.
#[must_use]
pub fn late_attach_history_incomplete_events() -> Vec<DaemonEvent> {
    vec![
        DaemonEvent::AttachState {
            session_id: LATE_ATTACH_INCOMPLETE_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID.to_string(),
            state: "attaching".to_string(),
        },
        snapshot_event(
            LATE_ATTACH_INCOMPLETE_SESSION_ID,
            LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID,
            LATE_ATTACH_HISTORY_READY_PAYLOAD,
        ),
        DaemonEvent::AttachState {
            session_id: LATE_ATTACH_INCOMPLETE_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID.to_string(),
            state: botster_hub_client::ATTACH_STATE_SNAPSHOT_HISTORY_INCOMPLETE.to_string(),
        },
        DaemonEvent::AttachState {
            session_id: LATE_ATTACH_INCOMPLETE_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID.to_string(),
            state: "attached".to_string(),
        },
        DaemonEvent::TerminalOutput {
            session_id: LATE_ATTACH_INCOMPLETE_SESSION_ID.to_string(),
            subscription_id: LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID.to_string(),
            payload: DaemonLiveOutputPayload::from_bytes(LATE_ATTACH_LIVE_DATA.as_bytes()),
        },
    ]
}

/// Provenance and content-identity pins for Core-owned late-attach GHOSTSNP files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LateAttachGhostsnpProvenance {
    pub protocol_crate: &'static str,
    pub protocol_git: &'static str,
    pub core_pin: &'static str,
    pub ghostty_pin: &'static str,
    pub fixture_files: &'static [&'static str],
    pub terminal_rows: u16,
    pub terminal_cols: u16,
    pub history_payload_len: usize,
    pub history_payload_sha256: &'static str,
    pub blank_payload_len: usize,
    pub blank_payload_sha256: &'static str,
    pub ghostsnp_magic: &'static [u8],
}

/// Return Core protocol-crate coordinate, file names, sizes, SHAs, and magic.
#[must_use]
pub fn late_attach_ghostsnp_provenance() -> LateAttachGhostsnpProvenance {
    let _ = botster_terminal_protocol::PROTOCOL;
    LateAttachGhostsnpProvenance {
        protocol_crate: LATE_ATTACH_GHOSTSNP_PROTOCOL_CRATE,
        protocol_git: LATE_ATTACH_GHOSTSNP_PROTOCOL_GIT,
        core_pin: LATE_ATTACH_GHOSTSNP_CORE_PIN,
        ghostty_pin: LATE_ATTACH_GHOSTSNP_GHOSTTY_PIN,
        fixture_files: LATE_ATTACH_GHOSTSNP_FILES,
        terminal_rows: 24,
        terminal_cols: 80,
        history_payload_len: LATE_ATTACH_HISTORY_PAYLOAD_LEN,
        history_payload_sha256: LATE_ATTACH_HISTORY_PAYLOAD_SHA256,
        blank_payload_len: LATE_ATTACH_NO_HISTORY_PAYLOAD_LEN,
        blank_payload_sha256: LATE_ATTACH_NO_HISTORY_PAYLOAD_SHA256,
        ghostsnp_magic: GHOSTSNP_MAGIC,
    }
}

/// SHA-256 hex digest of opaque payload bytes (external smoke content identity).
#[must_use]
pub fn late_attach_history_payload_sha256() -> &'static str {
    late_attach_ghostsnp_provenance().history_payload_sha256
}

/// SHA-256 hex digest of the blank no-history GHOSTSNP golden.
#[must_use]
pub fn late_attach_no_history_payload_sha256() -> &'static str {
    late_attach_ghostsnp_provenance().blank_payload_sha256
}

/// Length and SHA pins for incremental PAGE/FINISH frames.
#[must_use]
pub fn late_attach_incremental_frame_identity() -> [(usize, &'static str); 3] {
    [
        (
            LATE_ATTACH_HISTORY_PAGE_PAYLOAD_LEN,
            LATE_ATTACH_HISTORY_PAGE_PAYLOAD_SHA256,
        ),
        (
            LATE_ATTACH_HISTORY_FINISH_PAYLOAD_LEN,
            LATE_ATTACH_HISTORY_FINISH_PAYLOAD_SHA256,
        ),
        (
            LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_LEN,
            LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_SHA256,
        ),
    ]
}

/// Core-owned history GHOSTSNP bytes (Golden A).
#[must_use]
pub fn late_attach_history_payload_bytes() -> &'static [u8] {
    debug_assert!(LATE_ATTACH_HISTORY_PAYLOAD.starts_with(GHOSTSNP_MAGIC));
    debug_assert_eq!(
        LATE_ATTACH_HISTORY_PAYLOAD.len(),
        LATE_ATTACH_HISTORY_PAYLOAD_LEN
    );
    LATE_ATTACH_HISTORY_PAYLOAD
}

/// Core-owned blank GHOSTSNP bytes (Golden B).
#[must_use]
pub fn late_attach_no_history_payload_bytes() -> &'static [u8] {
    debug_assert!(LATE_ATTACH_NO_HISTORY_PAYLOAD.starts_with(GHOSTSNP_MAGIC));
    debug_assert_eq!(
        LATE_ATTACH_NO_HISTORY_PAYLOAD.len(),
        LATE_ATTACH_NO_HISTORY_PAYLOAD_LEN
    );
    LATE_ATTACH_NO_HISTORY_PAYLOAD
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

/// Return stable serde JSON for the session lifecycle subscription contract.
#[must_use]
pub fn session_lifecycle_subscription_conformance_fixture_json() -> serde_json::Value {
    serde_json::to_value(session_lifecycle_subscription_conformance_scenario())
        .expect("session lifecycle subscription conformance fixture serializes")
}

/// Return stable serde JSON for downstream mode readback client tests.
#[must_use]
pub fn mode_flags_conformance_fixture_json() -> serde_json::Value {
    serde_json::to_value(mode_flags_conformance_scenario())
        .expect("mode flags conformance fixture serializes")
}

/// Return deterministic local WebRTC delivery-chunk scenarios for downstream clients.
#[must_use]
pub fn local_webrtc_delivery_chunk_conformance_fixture_json() -> serde_json::Value {
    serde_json::json!({
        "version": botster_hub_client::LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
        "maximum_frame_bytes_exclusive": botster_hub_client::LOCAL_WEBRTC_MAX_FRAME_BYTES,
        "maximum_delivery_bytes": botster_hub_client::LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
        "scenarios": {
            "daemon_response": [{
                "version": botster_hub_client::LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
                "delivery_kind": "daemon_response",
                "message_id": "response-single",
                "chunk_index": 0,
                "chunk_count": 1,
                "total_bytes": 18,
                "payload": "encrypted-envelope"
            }],
            "daemon_entity_frame": [
                {
                    "version": botster_hub_client::LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
                    "delivery_kind": "daemon_entity_frame",
                    "message_id": "entity-multiple",
                    "chunk_index": 0,
                    "chunk_count": 2,
                    "total_bytes": 18,
                    "payload": "encrypted-"
                },
                {
                    "version": botster_hub_client::LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
                    "delivery_kind": "daemon_entity_frame",
                    "message_id": "entity-multiple",
                    "chunk_index": 1,
                    "chunk_count": 2,
                    "total_bytes": 18,
                    "payload": "envelope"
                }
            ],
            "large_generated": {
                "message_id": "response-large-generated",
                "generator": "repeat_utf8_pattern",
                "pattern": "botster-webrtc-chunk-fixture-",
                "total_bytes": 262_145,
                "chunk_payload_bytes": 12_288,
                "expected_chunk_count": 22,
                "reassembled_sha256": "06d24e206edb54bed524319b1127725b46e20ea4aae5934688599abd42fa4317"
            },
            "over_budget_operator_error": [{
                "version": botster_hub_client::LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
                "delivery_kind": "daemon_response",
                "message_id": "response-over-budget",
                "chunk_index": 0,
                "chunk_count": 1,
                "total_bytes": 24,
                "payload": "encrypted-operator-error"
            }]
        }
    })
}
