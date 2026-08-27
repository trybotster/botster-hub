//! Isolated daemon harness for external `botster-hub-client` integration tests.
//!
//! This crate starts the `botster-hub` binary as a subprocess and consumes the
//! Core runnable-entrypoint connection DTO plus the public client protocol.
//! Downstream tests must supply the hub and session-worker binary paths
//! explicitly, or via `BOTSTER_HUB_BIN` and `BOTSTER_SESSION_WORKER_BIN`.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{RunnableEntrypointHubConnection, RunnableEntrypointHubConnectionTransport};
use botster_hub_client::{
    DaemonCompatibilityRequirement, DaemonConnection, DaemonDiagnosticKind, DaemonEndpoint,
    DaemonEntityFrame, DaemonEvent, DaemonLiveOutputPayload, DaemonOperatorError, DaemonRequest,
    DaemonResponse, DaemonResponseKind, DaemonTransportError, FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS,
    connect_for_package_event_subscriptions, ensure_compatible,
};
use botster_ui_contract::{
    UiActionId, UiActionKind, UiActionRequest, UiActionRequestId, UiActionResult,
    UiActionResultState, UiFormValues, UiPresentationOperation, UiSurfaceId,
};
use serde::{Deserialize, Serialize};

mod isolated_hub;
pub use isolated_hub::{IsolatedHub, IsolatedHubBuilder, IsolatedHubError};

/// Shared OS monotonic clock for campaign emission-to-receipt samples.
#[must_use]
pub fn monotonic_now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    #[cfg(target_os = "macos")]
    let clock = libc::CLOCK_UPTIME_RAW;
    #[cfg(not(target_os = "macos"))]
    let clock = libc::CLOCK_MONOTONIC;
    let rc = unsafe { libc::clock_gettime(clock, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime monotonic");
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}
mod conformance_data;
#[allow(unused_imports)]
pub(crate) use conformance_data::{
    APPLICATION_PRIMITIVE_NODE_KINDS, CONFORMANCE_ECHO, CONFORMANCE_READY, CONFORMANCE_SESSION_ID,
    CONFORMANCE_SUBSCRIPTION_ID, CONFORMANCE_WINSIZE_PREFIX, DAEMON_PROTOCOL_TYPESCRIPT_ARTIFACT,
    GHOSTSNP_MAGIC, LATE_ATTACH_GHOSTSNP_CORE_PIN, LATE_ATTACH_GHOSTSNP_FILES,
    LATE_ATTACH_GHOSTSNP_GHOSTTY_PIN, LATE_ATTACH_GHOSTSNP_PROTOCOL_CRATE,
    LATE_ATTACH_GHOSTSNP_PROTOCOL_GIT, LATE_ATTACH_HISTORY_FINISH_PAYLOAD,
    LATE_ATTACH_HISTORY_FINISH_PAYLOAD_LEN, LATE_ATTACH_HISTORY_FINISH_PAYLOAD_SHA256,
    LATE_ATTACH_HISTORY_PAGE_PAYLOAD, LATE_ATTACH_HISTORY_PAGE_PAYLOAD_LEN,
    LATE_ATTACH_HISTORY_PAGE_PAYLOAD_SHA256, LATE_ATTACH_HISTORY_PAYLOAD,
    LATE_ATTACH_HISTORY_PAYLOAD_LEN, LATE_ATTACH_HISTORY_PAYLOAD_SHA256,
    LATE_ATTACH_HISTORY_READY_PAYLOAD, LATE_ATTACH_HISTORY_SCREEN_TEXT,
    LATE_ATTACH_HISTORY_SESSION_ID, LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
    LATE_ATTACH_INCOMPLETE_SESSION_ID, LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID,
    LATE_ATTACH_LIVE_DATA, LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD,
    LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_LEN, LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_SHA256,
    LATE_ATTACH_NO_HISTORY_LIVE_DATA, LATE_ATTACH_NO_HISTORY_PAYLOAD,
    LATE_ATTACH_NO_HISTORY_PAYLOAD_LEN, LATE_ATTACH_NO_HISTORY_PAYLOAD_SHA256,
    LATE_ATTACH_NO_HISTORY_READY_PAYLOAD, LATE_ATTACH_NO_HISTORY_SESSION_ID,
    LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID, PLUGIN_CONTRACT_ACCEPTED_REPLACEMENT_SCOPE,
    PLUGIN_CONTRACT_ACTION, PLUGIN_CONTRACT_APP_SURFACE, PLUGIN_CONTRACT_BLOCKED_SURFACE,
    PLUGIN_CONTRACT_DIALOG_FORM_NODE_ID, PLUGIN_CONTRACT_DIALOG_INPUT_NODE_ID,
    PLUGIN_CONTRACT_DIALOG_NODE_ID, PLUGIN_CONTRACT_EMPTY_SURFACE, PLUGIN_CONTRACT_ENTITY_FAMILY,
    PLUGIN_CONTRACT_ENTITY_SURFACE, PLUGIN_CONTRACT_INVALID_BODY_SURFACE,
    PLUGIN_CONTRACT_MATRIX_FIXTURE_ARTIFACT, PLUGIN_CONTRACT_MATRIX_PACKAGE,
    PLUGIN_CONTRACT_SESSION_SURFACE, PLUGIN_CONTRACT_SETTINGS_SURFACE, PROJECT_PIPELINES_ACTION,
    PROJECT_PIPELINES_PACKAGE, PROJECT_PIPELINES_SURFACE, SESSION_LIFECYCLE_ENTITY_TYPE,
    SESSION_LIFECYCLE_FIRST_SUBSCRIPTION_ID, SESSION_LIFECYCLE_OVERFLOW_REASON,
    SESSION_LIFECYCLE_RECONNECT_SUBSCRIPTION_ID, SESSION_LIFECYCLE_SECOND_SUBSCRIPTION_ID,
    SESSION_LIFECYCLE_SESSION_ID, SUPPORTED_PLUGIN_ENTITY_FRAMES,
    SUPPORTED_PLUGIN_SURFACE_JSON_ACTIONS, merge_patch,
};
pub use conformance_data::{
    ApplicationPrimitivesFixtureDescriptor, DaemonProtocolTypescriptArtifact, EntityActionSupport,
    FirstPartyClientSupportMatrix, FreshSubscriptionContract, LateAttachGhostsnpProvenance,
    LateAttachHistoryConformanceScenario, LateAttachHistorySupport, ModeFlagsConformanceFailure,
    ModeFlagsConformanceScenario, ModeFlagsConformanceSuccess, PluginContractMatrixFixtureAsset,
    PluginSurfaceSupport, ResizeSupport, SessionEntityOverflowContract,
    SessionEntitySubscriptionSupport, SessionLifecycleSubscriptionConformanceScenario,
    SessionPluginBindingConformanceScenario, SessionPluginBindingExpectedStages,
    SessionPluginMaterializedControl, SessionPluginMaterializedRow, SessionPluginRowExpectedStages,
    SessionTypeAuthoringSupport, TerminalModeFlagsSupport, TerminalStreamingSupport, TestAssetFile,
    application_primitives_fixture_descriptor, copy_plugin_contract_matrix_fixture,
    daemon_protocol_typescript_artifact, first_party_client_support_matrix,
    late_attach_ghostsnp_provenance, late_attach_history_conformance_fixture_json,
    late_attach_history_conformance_scenario, late_attach_history_events,
    late_attach_history_incomplete_events, late_attach_history_payload_bytes,
    late_attach_history_payload_sha256, late_attach_incremental_frame_identity,
    late_attach_no_history_events, late_attach_no_history_payload_bytes,
    late_attach_no_history_payload_sha256, local_webrtc_delivery_chunk_conformance_fixture_json,
    materialize_session_plugin_bindings, materialize_session_plugin_rows,
    mode_flags_conformance_fixture_json, mode_flags_conformance_scenario,
    plugin_contract_matrix_fixture_asset, session_lifecycle_subscription_conformance_fixture_json,
    session_lifecycle_subscription_conformance_scenario,
    session_plugin_binding_conformance_fixture_json, session_plugin_binding_conformance_scenario,
};

const MANY_PTY_DEADLINE: Duration = Duration::from_secs(10);
const MANY_PTY_POLL_INTERVAL: Duration = Duration::from_millis(30);
const MANY_PTY_NOISY_SESSION_ID: &str = "many-pty-noisy";
const MANY_PTY_SUBSCRIPTION_ID: &str = "many-pty-late-subscription";
const MANY_PTY_HISTORY_MARKER: &str = "many-pty-history-ready";
const MANY_PTY_INPUT: &str = "many-pty-client-input";
const MANY_PTY_LIVE_MARKER: &str = "many-pty-live:many-pty-client-input";

/// Stable runtime observations from the real Hub/Core/session-worker topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLifecycleSubscriptionConformanceReport {
    pub entity_type: String,
    pub initial_snapshot_authoritative: bool,
    pub concurrent_subscribers_consistent: bool,
    pub spawn_upsert_observed: bool,
    pub lifecycle_patch_observed: bool,
    pub natural_exit_patch_observed: bool,
    pub remove_observed: bool,
    pub sequences_strictly_increasing: bool,
    pub disconnect_cleanup_released_subscription: bool,
    pub fresh_subscription_snapshot_authoritative: bool,
    pub overflow_resync_reason: String,
    pub failed_snapshot_delivery_closes_subscription: bool,
}

#[derive(Debug)]
pub struct SessionLifecycleSubscriptionConformanceError {
    stage: &'static str,
    message: String,
}

impl fmt::Display for SessionLifecycleSubscriptionConformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "session lifecycle conformance {} failed: {}",
            self.stage, self.message
        )
    }
}

impl Error for SessionLifecycleSubscriptionConformanceError {}

fn session_lifecycle_error(
    stage: &'static str,
    message: impl Into<String>,
) -> SessionLifecycleSubscriptionConformanceError {
    SessionLifecycleSubscriptionConformanceError {
        stage,
        message: message.into(),
    }
}

fn next_session_lifecycle_frame(
    subscription: &mut botster_hub_client::DaemonEntitySubscription,
    deadline: Instant,
    stage: &'static str,
) -> Result<DaemonEntityFrame, SessionLifecycleSubscriptionConformanceError> {
    loop {
        if Instant::now() >= deadline {
            return Err(session_lifecycle_error(
                stage,
                "timed out waiting for entity frame",
            ));
        }
        match subscription.next_frame() {
            Ok(frame) => return Ok(frame),
            Err(botster_hub_client::DaemonTransportError::Io(error))
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(error) => return Err(session_lifecycle_error(stage, error.to_string())),
        }
    }
}

/// Prove the published session lifecycle contract against a real isolated Hub topology.
///
/// This runner uses only `botster-hub-client` requests and DTOs. The supplied
/// [`IsolatedHub`] owns the real HubDaemon, CoreDaemon, and session-worker
/// processes; callers remain responsible for shutting the harness down.
pub fn run_session_lifecycle_subscription_conformance(
    hub: &IsolatedHub,
) -> Result<
    SessionLifecycleSubscriptionConformanceReport,
    SessionLifecycleSubscriptionConformanceError,
> {
    let endpoint = hub.endpoint();
    let mut first = botster_hub_client::subscribe_session_entities(
        endpoint,
        SESSION_LIFECYCLE_FIRST_SUBSCRIPTION_ID,
    )
    .map_err(|error| session_lifecycle_error("initial subscribe", error.to_string()))?;
    first
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| session_lifecycle_error("initial timeout", error.to_string()))?;
    let initial = first
        .next_frame()
        .map_err(|error| session_lifecycle_error("initial snapshot", error.to_string()))?;
    let initial_snapshot_authoritative = matches!(
        initial,
        DaemonEntityFrame::Snapshot {
            ref subscription_id,
            ref entity_type,
            snapshot_seq: 0,
            ref items,
            resync_reason: None,
        } if subscription_id == SESSION_LIFECYCLE_FIRST_SUBSCRIPTION_ID
            && entity_type == SESSION_LIFECYCLE_ENTITY_TYPE
            && items.is_empty()
    );
    if !initial_snapshot_authoritative {
        return Err(session_lifecycle_error(
            "initial snapshot",
            format!("expected empty authoritative snapshot, got {initial:?}"),
        ));
    }

    let mut second = botster_hub_client::subscribe_session_entities(
        endpoint,
        SESSION_LIFECYCLE_SECOND_SUBSCRIPTION_ID,
    )
    .map_err(|error| session_lifecycle_error("concurrent subscribe", error.to_string()))?;
    second
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| session_lifecycle_error("concurrent timeout", error.to_string()))?;
    let second_initial = second
        .next_frame()
        .map_err(|error| session_lifecycle_error("concurrent snapshot", error.to_string()))?;
    if !matches!(second_initial, DaemonEntityFrame::Snapshot { ref items, .. } if items.is_empty())
    {
        return Err(session_lifecycle_error(
            "concurrent snapshot",
            format!("expected empty authoritative snapshot, got {second_initial:?}"),
        ));
    }

    let spawn = botster_hub_client::request(
        endpoint,
        DaemonRequest::Spawn {
            session_id: SESSION_LIFECYCLE_SESSION_ID.to_string(),
            command: "printf 'session-lifecycle-started\\n'; sleep 2".to_string(),
        },
    )
    .map_err(|error| session_lifecycle_error("spawn", error.to_string()))?;
    if spawn.kind != DaemonResponseKind::Spawned {
        return Err(session_lifecycle_error(
            "spawn",
            format!(
                "expected spawned response, got {:?}: {:?}",
                spawn.kind, spawn.error
            ),
        ));
    }

    let first_upsert = next_session_lifecycle_frame(
        &mut first,
        Instant::now() + Duration::from_secs(5),
        "spawn upsert",
    )?;
    let upsert_sequence = match first_upsert {
        DaemonEntityFrame::Upsert {
            snapshot_seq,
            ref id,
            ..
        } if id == SESSION_LIFECYCLE_SESSION_ID => snapshot_seq,
        other => {
            return Err(session_lifecycle_error(
                "spawn upsert",
                format!("expected session upsert, got {other:?}"),
            ));
        }
    };
    let second_upsert = next_session_lifecycle_frame(
        &mut second,
        Instant::now() + Duration::from_secs(5),
        "concurrent upsert",
    )?;
    let concurrent_subscribers_consistent = matches!(
        second_upsert,
        DaemonEntityFrame::Upsert {
            snapshot_seq,
            ref id,
            ..
        } if id == SESSION_LIFECYCLE_SESSION_ID && snapshot_seq == upsert_sequence
    );
    if !concurrent_subscribers_consistent {
        return Err(session_lifecycle_error(
            "concurrent upsert",
            format!("subscriber sequences diverged: {second_upsert:?}"),
        ));
    }

    botster_hub_client::request(
        endpoint,
        DaemonRequest::Resize {
            session_id: SESSION_LIFECYCLE_SESSION_ID.to_string(),
            rows: 31,
            cols: 101,
        },
    )
    .map_err(|error| session_lifecycle_error("lifecycle patch", error.to_string()))?;
    let resize_deadline = Instant::now() + Duration::from_secs(5);
    let resize_sequence = loop {
        match next_session_lifecycle_frame(&mut first, resize_deadline, "lifecycle patch")? {
            DaemonEntityFrame::Patch {
                snapshot_seq,
                patch,
                ..
            } if patch.get("rows").and_then(serde_json::Value::as_u64) == Some(31)
                && patch.get("cols").and_then(serde_json::Value::as_u64) == Some(101) =>
            {
                break snapshot_seq;
            }
            _ => {}
        }
    };
    let second_resize_deadline = Instant::now() + Duration::from_secs(5);
    let second_resize_sequence = loop {
        match next_session_lifecycle_frame(&mut second, second_resize_deadline, "concurrent patch")?
        {
            DaemonEntityFrame::Patch {
                snapshot_seq,
                patch,
                ..
            } if patch.get("rows").and_then(serde_json::Value::as_u64) == Some(31)
                && patch.get("cols").and_then(serde_json::Value::as_u64) == Some(101) =>
            {
                break snapshot_seq;
            }
            _ => {}
        }
    };
    if resize_sequence != second_resize_sequence {
        return Err(session_lifecycle_error(
            "concurrent patch",
            "subscriber resize sequences diverged",
        ));
    }

    let exit_deadline = Instant::now() + Duration::from_secs(10);
    let exit_sequence = loop {
        match next_session_lifecycle_frame(&mut first, exit_deadline, "natural exit")? {
            DaemonEntityFrame::Patch {
                snapshot_seq,
                patch,
                ..
            } if patch.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited") => {
                break snapshot_seq;
            }
            _ => {}
        }
    };

    let removed = botster_hub_client::request(
        endpoint,
        DaemonRequest::RemoveSession {
            session_id: SESSION_LIFECYCLE_SESSION_ID.to_string(),
        },
    )
    .map_err(|error| session_lifecycle_error("remove", error.to_string()))?;
    if removed.kind != DaemonResponseKind::SessionRemoved {
        return Err(session_lifecycle_error(
            "remove",
            format!("expected session_removed response, got {:?}", removed.kind),
        ));
    }
    let remove_deadline = Instant::now() + Duration::from_secs(5);
    let remove_sequence = loop {
        match next_session_lifecycle_frame(&mut first, remove_deadline, "remove delta")? {
            DaemonEntityFrame::Remove {
                snapshot_seq,
                ref id,
                ..
            } if id == SESSION_LIFECYCLE_SESSION_ID => break snapshot_seq,
            _ => {}
        }
    };
    let sequences_strictly_increasing = upsert_sequence < resize_sequence
        && resize_sequence < exit_sequence
        && exit_sequence < remove_sequence;
    if !sequences_strictly_increasing {
        return Err(session_lifecycle_error(
            "sequence order",
            format!(
                "expected upsert < patch < exit < remove, got {upsert_sequence}, {resize_sequence}, {exit_sequence}, {remove_sequence}"
            ),
        ));
    }

    drop(first);
    let cleanup_deadline = Instant::now() + Duration::from_secs(2);
    let cleanup_probe = loop {
        match botster_hub_client::subscribe_session_entities(
            endpoint,
            SESSION_LIFECYCLE_FIRST_SUBSCRIPTION_ID,
        ) {
            Ok(subscription) => break subscription,
            Err(_) if Instant::now() < cleanup_deadline => thread::sleep(Duration::from_millis(20)),
            Err(error) => {
                return Err(session_lifecycle_error(
                    "disconnect cleanup",
                    error.to_string(),
                ));
            }
        }
    };
    cleanup_probe
        .unsubscribe()
        .map_err(|error| session_lifecycle_error("disconnect cleanup", error.to_string()))?;

    let mut reconnected = botster_hub_client::subscribe_session_entities(
        endpoint,
        SESSION_LIFECYCLE_RECONNECT_SUBSCRIPTION_ID,
    )
    .map_err(|error| session_lifecycle_error("fresh subscribe", error.to_string()))?;
    reconnected
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| session_lifecycle_error("fresh timeout", error.to_string()))?;
    let reconnect_frame = reconnected
        .next_frame()
        .map_err(|error| session_lifecycle_error("fresh snapshot", error.to_string()))?;
    let fresh_subscription_snapshot_authoritative = matches!(
        reconnect_frame,
        DaemonEntityFrame::Snapshot {
            ref subscription_id,
            ref items,
            resync_reason: None,
            ..
        } if subscription_id == SESSION_LIFECYCLE_RECONNECT_SUBSCRIPTION_ID && items.is_empty()
    );
    if !fresh_subscription_snapshot_authoritative {
        return Err(session_lifecycle_error(
            "fresh snapshot",
            format!("expected fresh authoritative snapshot, got {reconnect_frame:?}"),
        ));
    }
    reconnected
        .unsubscribe()
        .map_err(|error| session_lifecycle_error("fresh unsubscribe", error.to_string()))?;
    second
        .unsubscribe()
        .map_err(|error| session_lifecycle_error("concurrent unsubscribe", error.to_string()))?;

    let scenario = session_lifecycle_subscription_conformance_scenario();
    Ok(SessionLifecycleSubscriptionConformanceReport {
        entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
        initial_snapshot_authoritative,
        concurrent_subscribers_consistent,
        spawn_upsert_observed: true,
        lifecycle_patch_observed: true,
        natural_exit_patch_observed: true,
        remove_observed: true,
        sequences_strictly_increasing,
        disconnect_cleanup_released_subscription: true,
        fresh_subscription_snapshot_authoritative,
        overflow_resync_reason: scenario.overflow.resync_reason,
        failed_snapshot_delivery_closes_subscription: scenario
            .overflow
            .failed_snapshot_delivery_closes_subscription,
    })
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

/// Stable failure stages for the joint many-PTY product-path proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManyPtyConformanceStage {
    Spawn,
    Attach,
    Drain,
    Input,
    History,
    Cleanup,
}

impl ManyPtyConformanceStage {
    /// Return the machine-readable label used by test and CI output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spawn => "spawn",
            Self::Attach => "attach",
            Self::Drain => "drain",
            Self::Input => "input",
            Self::History => "history",
            Self::Cleanup => "cleanup",
        }
    }
}

impl fmt::Display for ManyPtyConformanceStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Path-neutral failure from [`run_many_pty_client_attach_conformance`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManyPtyConformanceError {
    pub stage: ManyPtyConformanceStage,
    pub session_id: String,
    pub details: String,
    pub cleanup_failures: Vec<String>,
}

impl fmt::Display for ManyPtyConformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} failure for {}: {}",
            self.stage, self.session_id, self.details
        )?;
        if !self.cleanup_failures.is_empty() {
            write!(
                formatter,
                "; cleanup also failed for {:?}",
                self.cleanup_failures
            )?;
        }
        Ok(())
    }
}

impl Error for ManyPtyConformanceError {}

/// Stable observations from the joint many-PTY product-path proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManyPtyConformanceReport {
    pub total_sessions: usize,
    pub quiet_sessions: usize,
    pub quiet_sessions_exited: usize,
    pub history_observed: bool,
    pub screen_marker_observed: bool,
    pub snapshot_payload_bytes: usize,
    pub live_output_observed: bool,
    pub cleaned_sessions: usize,
}

/// Prove many worker-backed PTYs, quiet completion, late attach/history, input, and cleanup.
///
/// The flow uses only the public daemon socket client. Quiet sessions are never
/// attached; public drains advance lifecycle reconciliation before bounded
/// `ListSessions` polling observes their completion.
pub fn run_many_pty_client_attach_conformance(
    hub: &IsolatedHub,
    total_sessions: usize,
) -> Result<ManyPtyConformanceReport, ManyPtyConformanceError> {
    if total_sessions < 2 {
        return Err(many_pty_error(
            ManyPtyConformanceStage::Spawn,
            "scenario",
            "at least two sessions are required",
        ));
    }

    let mut connection = DaemonConnection::connect(hub.endpoint()).map_err(|_| {
        many_pty_error(
            ManyPtyConformanceStage::Spawn,
            "daemon",
            "public daemon connection failed",
        )
    })?;
    let mut created_sessions = Vec::with_capacity(total_sessions);
    let result =
        run_many_pty_client_attach_scenario(&mut connection, total_sessions, &mut created_sessions);
    let (cleaned_sessions, cleanup_failures) =
        cleanup_many_pty_sessions(&mut connection, &created_sessions);

    match result {
        Ok(mut report) if cleanup_failures.is_empty() => {
            report.cleaned_sessions = cleaned_sessions;
            Ok(report)
        }
        Ok(_) => Err(ManyPtyConformanceError {
            stage: ManyPtyConformanceStage::Cleanup,
            session_id: cleanup_failures[0].clone(),
            details: "one or more session cleanup requests failed".to_string(),
            cleanup_failures,
        }),
        Err(mut error) => {
            error.cleanup_failures = cleanup_failures;
            Err(error)
        }
    }
}

fn run_many_pty_client_attach_scenario(
    connection: &mut DaemonConnection,
    total_sessions: usize,
    created_sessions: &mut Vec<String>,
) -> Result<ManyPtyConformanceReport, ManyPtyConformanceError> {
    let baseline = many_pty_request(
        connection,
        &DaemonRequest::ListSessions,
        ManyPtyConformanceStage::Spawn,
        "baseline",
    )?;
    many_pty_expect_kind(
        &baseline,
        DaemonResponseKind::Sessions,
        ManyPtyConformanceStage::Spawn,
        "baseline",
    )?;
    if !baseline.sessions.is_empty() {
        return Err(many_pty_error(
            ManyPtyConformanceStage::Spawn,
            "baseline",
            "isolated hub did not start with an empty session list",
        ));
    }

    created_sessions.push(MANY_PTY_NOISY_SESSION_ID.to_string());
    let noisy = many_pty_request(
        connection,
        &DaemonRequest::Spawn {
            session_id: MANY_PTY_NOISY_SESSION_ID.to_string(),
            command: format!(
                "i=0; while [ \"$i\" -lt 48 ]; do printf 'many-pty-noise-%03d-abcdefghijklmnopqrstuvwxyz0123456789\\n' \"$i\"; i=$((i + 1)); done; printf '{MANY_PTY_HISTORY_MARKER}\\n'; while IFS= read -r line; do printf 'many-pty-live:%s\\n' \"$line\"; done"
            ),
        },
        ManyPtyConformanceStage::Spawn,
        MANY_PTY_NOISY_SESSION_ID,
    )?;
    many_pty_expect_kind(
        &noisy,
        DaemonResponseKind::Spawned,
        ManyPtyConformanceStage::Spawn,
        MANY_PTY_NOISY_SESSION_ID,
    )?;

    let quiet_session_ids = (1..total_sessions)
        .map(|index| format!("many-pty-quiet-{index:02}"))
        .collect::<Vec<_>>();
    for (index, session_id) in quiet_session_ids.iter().enumerate() {
        created_sessions.push(session_id.clone());
        let marker = format!("many-pty-quiet-complete-{index:02}");
        let spawn = many_pty_request(
            connection,
            &DaemonRequest::Spawn {
                session_id: session_id.clone(),
                command: format!("printf '{marker}\\n'"),
            },
            ManyPtyConformanceStage::Spawn,
            session_id,
        )?;
        many_pty_expect_kind(
            &spawn,
            DaemonResponseKind::Spawned,
            ManyPtyConformanceStage::Spawn,
            session_id,
        )?;
    }

    wait_for_quiet_sessions(connection, &quiet_session_ids)?;
    wait_for_many_pty_screen_marker(connection)?;

    let attach = many_pty_request(
        connection,
        &DaemonRequest::Attach {
            session_id: MANY_PTY_NOISY_SESSION_ID.to_string(),
            subscription_id: MANY_PTY_SUBSCRIPTION_ID.to_string(),
        },
        ManyPtyConformanceStage::Attach,
        MANY_PTY_NOISY_SESSION_ID,
    )?;
    many_pty_expect_kind(
        &attach,
        DaemonResponseKind::Events,
        ManyPtyConformanceStage::Attach,
        MANY_PTY_NOISY_SESSION_ID,
    )?;
    if !attach.events.is_empty() {
        return Err(many_pty_error(
            ManyPtyConformanceStage::Attach,
            MANY_PTY_NOISY_SESSION_ID,
            "attach must not return terminal bodies",
        ));
    }

    // Re-confirm that the pre-attach marker remains readable after attach;
    // the earlier wait is the readiness oracle for the marker itself.
    let screen = many_pty_request(
        connection,
        &DaemonRequest::ReadScreen {
            session_id: MANY_PTY_NOISY_SESSION_ID.to_string(),
        },
        ManyPtyConformanceStage::History,
        MANY_PTY_NOISY_SESSION_ID,
    )?;
    many_pty_expect_kind(
        &screen,
        DaemonResponseKind::ReadScreen,
        ManyPtyConformanceStage::History,
        MANY_PTY_NOISY_SESSION_ID,
    )?;
    let screen = screen.read_screen.ok_or_else(|| {
        many_pty_error(
            ManyPtyConformanceStage::History,
            MANY_PTY_NOISY_SESSION_ID,
            "read_screen response was missing its body",
        )
    })?;
    if !screen.text.contains(MANY_PTY_HISTORY_MARKER) {
        return Err(many_pty_error(
            ManyPtyConformanceStage::History,
            MANY_PTY_NOISY_SESSION_ID,
            "read_screen did not contain the pre-attach marker",
        ));
    }

    let snapshot = many_pty_request(
        connection,
        &DaemonRequest::CaptureSnapshot {
            session_id: MANY_PTY_NOISY_SESSION_ID.to_string(),
        },
        ManyPtyConformanceStage::History,
        MANY_PTY_NOISY_SESSION_ID,
    )?;
    many_pty_expect_kind(
        &snapshot,
        DaemonResponseKind::CaptureSnapshot,
        ManyPtyConformanceStage::History,
        MANY_PTY_NOISY_SESSION_ID,
    )?;
    let snapshot = snapshot.capture_snapshot.ok_or_else(|| {
        many_pty_error(
            ManyPtyConformanceStage::History,
            MANY_PTY_NOISY_SESSION_ID,
            "capture_snapshot response was missing its body",
        )
    })?;
    if snapshot.payload_bytes == 0 || snapshot.payload_format.is_none() {
        return Err(many_pty_error(
            ManyPtyConformanceStage::History,
            MANY_PTY_NOISY_SESSION_ID,
            "capture_snapshot did not return a non-empty opaque payload with a declared format",
        ));
    }

    let input = many_pty_request(
        connection,
        &DaemonRequest::SendInput {
            session_id: MANY_PTY_NOISY_SESSION_ID.to_string(),
            data: format!("{MANY_PTY_INPUT}\n"),
        },
        ManyPtyConformanceStage::Input,
        MANY_PTY_NOISY_SESSION_ID,
    )?;
    many_pty_expect_kind(
        &input,
        DaemonResponseKind::Events,
        ManyPtyConformanceStage::Input,
        MANY_PTY_NOISY_SESSION_ID,
    )?;
    let deadline = Instant::now() + MANY_PTY_DEADLINE;
    let mut live_screen = String::new();
    while Instant::now() < deadline {
        let drain = many_pty_request(
            connection,
            &DaemonRequest::Drain {
                session_id: MANY_PTY_NOISY_SESSION_ID.to_string(),
                subscription_id: None,
            },
            ManyPtyConformanceStage::Drain,
            MANY_PTY_NOISY_SESSION_ID,
        )?;
        many_pty_expect_kind(
            &drain,
            DaemonResponseKind::Events,
            ManyPtyConformanceStage::Drain,
            MANY_PTY_NOISY_SESSION_ID,
        )?;
        if drain.events.iter().any(|event| {
            matches!(
                event,
                DaemonEvent::AttachState { .. }
                    | DaemonEvent::Snapshot { .. }
                    | DaemonEvent::Scrollback { .. }
                    | DaemonEvent::TerminalOutput { .. }
                    | DaemonEvent::ProcessExit { .. }
            )
        }) {
            return Err(many_pty_error(
                ManyPtyConformanceStage::Drain,
                MANY_PTY_NOISY_SESSION_ID,
                "host Drain must not return terminal bodies",
            ));
        }
        let screen = many_pty_request(
            connection,
            &DaemonRequest::ReadScreen {
                session_id: MANY_PTY_NOISY_SESSION_ID.to_string(),
            },
            ManyPtyConformanceStage::Drain,
            MANY_PTY_NOISY_SESSION_ID,
        )?;
        if let Some(body) = screen.read_screen {
            live_screen = body.text;
            if live_screen.contains(MANY_PTY_LIVE_MARKER) {
                break;
            }
        }
        thread::sleep(MANY_PTY_POLL_INTERVAL);
    }
    if !live_screen.contains(MANY_PTY_LIVE_MARKER) {
        return Err(many_pty_error(
            ManyPtyConformanceStage::Drain,
            MANY_PTY_NOISY_SESSION_ID,
            format!(
                "ReadScreen did not observe the input-driven live marker; text={live_screen:?}"
            ),
        ));
    }

    Ok(ManyPtyConformanceReport {
        total_sessions,
        quiet_sessions: quiet_session_ids.len(),
        quiet_sessions_exited: quiet_session_ids.len(),
        history_observed: true,
        screen_marker_observed: true,
        snapshot_payload_bytes: snapshot.payload_bytes,
        live_output_observed: true,
        cleaned_sessions: 0,
    })
}

fn wait_for_quiet_sessions(
    connection: &mut DaemonConnection,
    quiet_session_ids: &[String],
) -> Result<(), ManyPtyConformanceError> {
    let deadline = Instant::now() + MANY_PTY_DEADLINE;
    let mut observed_lifecycles = BTreeMap::new();
    while Instant::now() < deadline {
        for session_id in quiet_session_ids {
            let drain = many_pty_request(
                connection,
                &DaemonRequest::Drain {
                    session_id: session_id.clone(),
                    subscription_id: None,
                },
                ManyPtyConformanceStage::Spawn,
                session_id,
            )?;
            many_pty_expect_kind(
                &drain,
                DaemonResponseKind::Events,
                ManyPtyConformanceStage::Spawn,
                session_id,
            )?;
        }
        let list = many_pty_request(
            connection,
            &DaemonRequest::ListSessions,
            ManyPtyConformanceStage::Spawn,
            "quiet-sessions",
        )?;
        many_pty_expect_kind(
            &list,
            DaemonResponseKind::Sessions,
            ManyPtyConformanceStage::Spawn,
            "quiet-sessions",
        )?;
        let mut all_exited = true;
        for session_id in quiet_session_ids {
            let lifecycle = list
                .sessions
                .iter()
                .find(|session| session.session_id == *session_id)
                .map(|session| session.lifecycle.as_str());
            observed_lifecycles.insert(
                session_id.clone(),
                lifecycle.unwrap_or("missing").to_string(),
            );
            match lifecycle {
                Some("exited") => {}
                Some("failed") => {
                    return Err(many_pty_error(
                        ManyPtyConformanceStage::Spawn,
                        session_id,
                        "quiet session reached lifecycle failed",
                    ));
                }
                Some(_) | None => all_exited = false,
            }
        }
        if all_exited {
            return Ok(());
        }
        thread::sleep(MANY_PTY_POLL_INTERVAL);
    }

    Err(many_pty_error(
        ManyPtyConformanceStage::Spawn,
        observed_lifecycles
            .iter()
            .find(|(_, lifecycle)| lifecycle.as_str() != "exited")
            .map(|(session_id, _)| session_id.as_str())
            .unwrap_or("quiet-sessions"),
        format!(
            "quiet sessions did not all reach lifecycle exited before the deadline; observed lifecycles: {observed_lifecycles:?}"
        ),
    ))
}

fn wait_for_many_pty_screen_marker(
    connection: &mut DaemonConnection,
) -> Result<(), ManyPtyConformanceError> {
    let deadline = Instant::now() + MANY_PTY_DEADLINE;
    let mut last_screen = String::new();
    while Instant::now() < deadline {
        let drain = many_pty_request(
            connection,
            &DaemonRequest::Drain {
                session_id: MANY_PTY_NOISY_SESSION_ID.to_string(),
                subscription_id: None,
            },
            ManyPtyConformanceStage::History,
            MANY_PTY_NOISY_SESSION_ID,
        )?;
        many_pty_expect_kind(
            &drain,
            DaemonResponseKind::Events,
            ManyPtyConformanceStage::History,
            MANY_PTY_NOISY_SESSION_ID,
        )?;
        let response = many_pty_request(
            connection,
            &DaemonRequest::ReadScreen {
                session_id: MANY_PTY_NOISY_SESSION_ID.to_string(),
            },
            ManyPtyConformanceStage::History,
            MANY_PTY_NOISY_SESSION_ID,
        )?;
        many_pty_expect_kind(
            &response,
            DaemonResponseKind::ReadScreen,
            ManyPtyConformanceStage::History,
            MANY_PTY_NOISY_SESSION_ID,
        )?;
        if let Some(screen) = response.read_screen {
            if screen.text.contains(MANY_PTY_HISTORY_MARKER) {
                return Ok(());
            }
            last_screen = screen.text;
        }
        thread::sleep(MANY_PTY_POLL_INTERVAL);
    }
    let screen_tail = many_pty_tail(&last_screen);
    Err(many_pty_error(
        ManyPtyConformanceStage::History,
        MANY_PTY_NOISY_SESSION_ID,
        format!(
            "pre-attach screen marker did not appear before the deadline; screen tail: {screen_tail:?}"
        ),
    ))
}

#[allow(dead_code)]
fn many_pty_saw_live_output(events: &[DaemonEvent]) -> bool {
    many_pty_live_output_index(events).is_some()
}

#[allow(dead_code)]
fn many_pty_live_output_index(events: &[DaemonEvent]) -> Option<usize> {
    let mut output = String::new();
    for (index, event) in events.iter().enumerate() {
        if let DaemonEvent::TerminalOutput {
            subscription_id,
            payload,
            ..
        } = event
            && subscription_id == MANY_PTY_SUBSCRIPTION_ID
        {
            output.push_str(&live_output_utf8(payload));
            if output.contains(MANY_PTY_LIVE_MARKER) {
                return Some(index);
            }
        }
    }
    None
}

#[allow(dead_code)]
fn many_pty_terminal_output(events: &[DaemonEvent]) -> String {
    let mut output = String::new();
    for event in events {
        if let DaemonEvent::TerminalOutput {
            subscription_id,
            payload,
            ..
        } = event
            && subscription_id == MANY_PTY_SUBSCRIPTION_ID
        {
            output.push_str(&live_output_utf8(payload));
        }
    }
    output
}

#[allow(dead_code)]
fn live_output_utf8(payload: &DaemonLiveOutputPayload) -> String {
    String::from_utf8_lossy(&payload.decoded_bytes().unwrap_or_default()).into_owned()
}

#[cfg(test)]
fn live_output_contains(payload: &DaemonLiveOutputPayload, needle: &str) -> bool {
    payload
        .decoded_bytes()
        .map(|bytes| {
            bytes
                .windows(needle.len())
                .any(|window| window == needle.as_bytes())
        })
        .unwrap_or(false)
}

fn many_pty_tail(text: &str) -> String {
    text.chars()
        .rev()
        .take(240)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

fn cleanup_many_pty_sessions(
    connection: &mut DaemonConnection,
    session_ids: &[String],
) -> (usize, Vec<String>) {
    let mut cleaned = 0;
    let mut failures = Vec::new();
    for session_id in session_ids {
        match connection.request(&DaemonRequest::ShutdownSession {
            session_id: session_id.clone(),
        }) {
            Ok(response)
                if matches!(
                    response.kind,
                    DaemonResponseKind::Events | DaemonResponseKind::SessionCleanup
                ) =>
            {
                cleaned += 1;
            }
            Ok(_) | Err(_) => failures.push(session_id.clone()),
        }
    }
    (cleaned, failures)
}

fn many_pty_request(
    connection: &mut DaemonConnection,
    request: &DaemonRequest,
    stage: ManyPtyConformanceStage,
    session_id: &str,
) -> Result<DaemonResponse, ManyPtyConformanceError> {
    connection
        .request(request)
        .map_err(|_| many_pty_error(stage, session_id, "public daemon request failed"))
}

fn many_pty_expect_kind(
    response: &DaemonResponse,
    expected: DaemonResponseKind,
    stage: ManyPtyConformanceStage,
    session_id: &str,
) -> Result<(), ManyPtyConformanceError> {
    if response.kind == expected {
        Ok(())
    } else {
        Err(many_pty_error(
            stage,
            session_id,
            format!(
                "daemon returned response kind {:?}, expected {expected:?}",
                response.kind
            ),
        ))
    }
}

fn many_pty_error(
    stage: ManyPtyConformanceStage,
    session_id: impl Into<String>,
    details: impl Into<String>,
) -> ManyPtyConformanceError {
    ManyPtyConformanceError {
        stage,
        session_id: session_id.into(),
        details: details.into(),
        cleanup_failures: Vec::new(),
    }
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
#[non_exhaustive]
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
    pub navigation_item_ids: Vec<String>,
    pub app_navigation_route_path: String,
    pub settings_navigation_route_path: String,
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
    pub session_surface_id: String,
    pub session_surface_node_id: String,
    pub session_surface_binding_family: String,
    pub session_surface_references: Vec<String>,
    pub session_surface_matches_fixture: bool,
    pub package_entity_surface_id: String,
    pub package_entity_surface_node_id: String,
    pub package_entity_binding_family: String,
    pub package_entity_initial_snapshot: DaemonEntityFrame,
    pub package_entity_reconnect_snapshot: DaemonEntityFrame,
    pub session_materialized_rows: Vec<SessionPluginMaterializedRow>,
    pub session_action_node_id: String,
    pub session_action_payload: serde_json::Value,
    pub session_action_state: String,
    pub session_action_result_node_id: String,
    pub session_action_result_payload: serde_json::Value,
    pub session_remove_action_node_id: String,
    pub session_remove_action_payload: serde_json::Value,
    pub session_remove_action_state: String,
    pub session_remove_action_result_node_id: String,
    pub session_remove_action_result_payload: serde_json::Value,
    pub dialog_presence_key: String,
    pub selected_workspace_equality_key: String,
    pub selected_workspace_equality_value: String,
    pub open_action_id: String,
    pub open_action_node_id: String,
    pub open_action_payload: serde_json::Value,
    pub open_set_values: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub form_reachable_before_open: bool,
    pub dialog_visible_after_open: bool,
    pub selected_workspace_visible_after_open: bool,
    #[serde(default)]
    pub dialog_form_node_id: String,
    #[serde(default)]
    pub dialog_input_node_id: String,
    #[serde(default)]
    pub submit_action_node_id: String,
    #[serde(default)]
    pub actionable_sibling_form_during_dialog: bool,
    #[serde(default)]
    pub invalid_submit_values: serde_json::Value,
    #[serde(default)]
    pub valid_submit_values: serde_json::Value,
    pub rejected_state_retained: bool,
    pub rejected_tree_retained: bool,
    #[serde(default)]
    pub rejected_dialog_retained: bool,
    #[serde(default)]
    pub rejected_form_retained: bool,
    #[serde(default)]
    pub rejected_field_error_node_id: String,
    #[serde(default)]
    pub accepted_normalized_values: serde_json::Value,
    #[serde(default)]
    pub accepted_replacement_applied: bool,
    #[serde(default)]
    pub dialog_state_cleared: bool,
    pub dialog_visible_after_valid_submit: bool,
    pub toggle_action_id: String,
    pub toggle_action_node_id: String,
    pub toggle_action_payload: serde_json::Value,
    pub toggle_key: String,
    pub toggle_visible_states: Vec<bool>,
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
    pub action_success_presentation_clear_key: String,
    pub action_success_replacement_node_id: String,
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
    pub identity_mismatch_error_code: String,
    pub identity_mismatch_error_operation: String,
    pub invalid_replacement_error_code: String,
    pub invalid_replacement_error_operation: String,
    pub client_render_check: PluginContractMatrixClientRenderCheck,
    pub failure_classes: PluginConformanceFailureClasses,
}

#[derive(Debug, Clone, PartialEq)]
struct RenderedAction {
    node_id: String,
    action_id: String,
    payload: Option<serde_json::Value>,
}

#[derive(Debug, Default)]
struct ScopedPresentationState {
    values: BTreeMap<(String, String), BTreeMap<String, serde_json::Value>>,
}

fn presentation_operation_kind(operation: &UiPresentationOperation) -> &'static str {
    match operation {
        UiPresentationOperation::Set { .. } => "set",
        UiPresentationOperation::Clear { .. } => "clear",
        UiPresentationOperation::Toggle { .. } => "toggle",
    }
}

pub(crate) fn presentation_operation_kinds() -> Vec<String> {
    [
        UiPresentationOperation::Set {
            key: botster_ui_contract::UiPresentationKey("key".to_string()),
            value: serde_json::Value::Null,
        },
        UiPresentationOperation::Clear {
            key: botster_ui_contract::UiPresentationKey("key".to_string()),
        },
        UiPresentationOperation::Toggle {
            key: botster_ui_contract::UiPresentationKey("key".to_string()),
        },
    ]
    .iter()
    .map(presentation_operation_kind)
    .map(str::to_string)
    .collect()
}

impl ScopedPresentationState {
    fn apply(&mut self, package_name: &str, surface_id: &str, result: &UiActionResult) {
        if result.state != UiActionResultState::Accepted {
            return;
        }
        let values = self
            .values
            .entry((package_name.to_string(), surface_id.to_string()))
            .or_default();
        for operation in result.presentation.iter().flatten() {
            match operation {
                UiPresentationOperation::Set { key, value } => {
                    values.insert(key.0.clone(), value.clone());
                }
                UiPresentationOperation::Clear { key } => {
                    values.remove(&key.0);
                }
                UiPresentationOperation::Toggle { key } => {
                    let next = !values.get(&key.0).is_some_and(json_truthy);
                    values.insert(key.0.clone(), serde_json::Value::Bool(next));
                }
            }
        }
    }

    fn values_for(
        &self,
        package_name: &str,
        surface_id: &str,
    ) -> Option<&BTreeMap<String, serde_json::Value>> {
        self.values
            .get(&(package_name.to_string(), surface_id.to_string()))
    }
}

fn apply_action_result_to_client(
    presentation_state: &mut ScopedPresentationState,
    rendered_tree: &mut serde_json::Value,
    package_name: &str,
    surface_id: &str,
    result: &UiActionResult,
) -> Result<(), serde_json::Error> {
    presentation_state.apply(package_name, surface_id, result);
    if result.state != UiActionResultState::Accepted {
        return Ok(());
    }
    if let Some(replacement) = &result.replacement {
        *rendered_tree = serde_json::to_value(replacement)?;
    }
    Ok(())
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
    pub hub_connection_env_present: bool,
    pub hub_connection_transport: String,
    pub hub_connection_socket_path_absolute: bool,
    pub hub_data_dir_env_present: bool,
    pub hub_data_dir_env_absolute: bool,
    pub launch_working_directory_is_package_root: bool,
    pub launch_working_directory_differs_from_daemon_cwd: bool,
    pub real_hub_action_operation: String,
    pub real_hub_action_result: String,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// Run the hub-owned conformance flow for same-device external clients.
///
/// The flow starts from an already isolated hub, then exercises status, session
/// list, spawn, `DaemonConnection` Attach plus scoped Drain, input, resize,
/// validation error handling, and session teardown using only public
/// `botster-hub-client` calls. Held-open `botster_hub_client::stream_attach` is
/// a separate production helper; live IsolatedHub proof lives in
/// `unix_adapter_always_bind_stream_attach_restores_current_screen`.
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

    let mut terminal =
        DaemonConnection::connect(hub.endpoint()).map_err(|source| ConformanceError::Client {
            operation: "connect",
            source,
        })?;
    let attach = terminal
        .request(&DaemonRequest::Attach {
            session_id: CONFORMANCE_SESSION_ID.to_string(),
            subscription_id: CONFORMANCE_SUBSCRIPTION_ID.to_string(),
        })
        .map_err(|source| ConformanceError::Client {
            operation: "attach",
            source,
        })?;
    expect_kind(&attach, DaemonResponseKind::Events, "attach")?;
    if !attach.events.is_empty() {
        return Err(ConformanceError::MissingOutput {
            needle: "empty attach bodies",
            output: format!("{:?}", attach.events),
        });
    }
    let mut drain_output = String::new();
    let attached_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < attached_deadline {
        let drain = terminal
            .request(&DaemonRequest::drain_subscription(
                CONFORMANCE_SESSION_ID,
                CONFORMANCE_SUBSCRIPTION_ID,
            ))
            .map_err(|source| ConformanceError::Client {
                operation: "attach_drain",
                source,
            })?;
        if !drain.events.is_empty() {
            return Err(ConformanceError::MissingOutput {
                needle: "empty host drain",
                output: format!("{:?}", drain.events),
            });
        }
        append_read_screen(&mut terminal, &mut drain_output)?;
        if drain_output.contains(CONFORMANCE_READY) {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    expect_kind(
        &terminal
            .request(&DaemonRequest::Resize {
                session_id: CONFORMANCE_SESSION_ID.to_string(),
                rows: 33,
                cols: 102,
            })
            .map_err(|source| ConformanceError::Client {
                operation: "resize",
                source,
            })?,
        DaemonResponseKind::Events,
        "resize",
    )?;
    expect_kind(
        &terminal
            .request(&DaemonRequest::SendInput {
                session_id: CONFORMANCE_SESSION_ID.to_string(),
                data: "from-conformance\r".to_string(),
            })
            .map_err(|source| ConformanceError::Client {
                operation: "send_input",
                source,
            })?,
        DaemonResponseKind::Events,
        "send_input",
    )?;
    let echo_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < echo_deadline {
        append_read_screen(&mut terminal, &mut drain_output)?;
        if drain_output.contains(CONFORMANCE_ECHO) {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    expect_kind(
        &terminal
            .request(&DaemonRequest::SendInput {
                session_id: CONFORMANCE_SESSION_ID.to_string(),
                data: "size-check\r".to_string(),
            })
            .map_err(|source| ConformanceError::Client {
                operation: "send_size_check",
                source,
            })?,
        DaemonResponseKind::Events,
        "send_size_check",
    )?;
    let resize_needle = format!("{CONFORMANCE_WINSIZE_PREFIX}33 102");
    let size_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < size_deadline {
        append_read_screen(&mut terminal, &mut drain_output)?;
        if drain_output.contains(&resize_needle) {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    expect_kind(
        &terminal
            .request(&DaemonRequest::SendInput {
                session_id: CONFORMANCE_SESSION_ID.to_string(),
                data: "quit\r".to_string(),
            })
            .map_err(|source| ConformanceError::Client {
                operation: "send_quit",
                source,
            })?,
        DaemonResponseKind::Events,
        "send_quit",
    )?;
    let output = drain_output;
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
            subscription_id: None,
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

const EVENT_CONFORMANCE_PRODUCER: &str = "event-plane-producer";
const EVENT_CONFORMANCE_NAME: &str = "sample.ready";

/// Stable observations from [`run_client_event_conformance`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientEventConformanceReport {
    pub negotiated_package_event_subscriptions: bool,
    pub exact_subscribe: bool,
    pub event_received: bool,
    pub subject_filter_dropped_non_matching: bool,
    pub reconnect_without_replay: bool,
    pub unsubscribed: bool,
    pub control_progressed_during_events: bool,
    pub event_gap: bool,
}

/// Prove generic client package-event consumption at the public Unix host-control boundary.
///
/// The caller supplies the Hub-owned `examples/event-plane-producer` checkout
/// (or a copy). This helper enables that package through the public daemon API
/// and drives exact owner-plus-name subscribe, receive, subject filtering,
/// reconnect without replay, unsubscribe, and continued Status progress.
///
/// Slow-consumer `event_gap` runs only when `stall_path` is `Some` and the
/// IsolatedHub child was started with `BOTSTER_HUB_TEST_CLIENT_EVENT_QUEUE_MAX=1`
/// and `BOTSTER_HUB_TEST_STALL_UNIX_EVENT_FLUSH` equal to that path.
///
/// This entrypoint does not change published npm fixture bytes.
pub fn run_client_event_conformance(
    hub: &IsolatedHub,
    producer_path: impl AsRef<Path>,
    stall_path: Option<&Path>,
) -> Result<ClientEventConformanceReport, ConformanceError> {
    let producer_dir = materialize_event_producer(hub, producer_path.as_ref())?;
    let enabled = request(
        hub.endpoint(),
        DaemonRequest::EnablePackageLocalPath { path: producer_dir },
        "enable_event_producer",
    )?;
    expect_kind(
        &enabled,
        DaemonResponseKind::PackageDecision,
        "enable_event_producer",
    )?;

    let mut matching =
        connect_for_package_event_subscriptions(hub.endpoint()).map_err(|source| {
            ConformanceError::Client {
                operation: "connect_events",
                source,
            }
        })?;
    let negotiated = matching
        .required_features()
        .iter()
        .any(|feature| feature == FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS);
    if !negotiated {
        return Err(ConformanceError::UnexpectedValue {
            operation: "connect_events",
            field: "required_features",
            expected: FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.to_string(),
            actual: matching.required_features().join(","),
        });
    }
    let subscribed = matching
        .subscribe_events(
            "sub-event-conformance",
            EVENT_CONFORMANCE_PRODUCER,
            EVENT_CONFORMANCE_NAME,
            vec!["session-match".to_string()],
        )
        .map_err(|source| ConformanceError::Client {
            operation: "subscribe_events",
            source,
        })?;
    expect_kind(
        &subscribed,
        DaemonResponseKind::EventSubscribed,
        "subscribe_events",
    )?;

    emit_event_ready(
        hub.endpoint(),
        "event-ok",
        Some("session-match"),
        Some("ready"),
        None,
    )?;
    emit_event_ready(
        hub.endpoint(),
        "event-other",
        Some("session-other"),
        Some("ignored"),
        None,
    )?;
    wait_for_event_token(&mut matching, "event-ok")?;
    let status_during =
        matching
            .request(&DaemonRequest::Status)
            .map_err(|source| ConformanceError::Client {
                operation: "status_during_events",
                source,
            })?;
    expect_kind(
        &status_during,
        DaemonResponseKind::Status,
        "status_during_events",
    )?;
    let skipped_after_filter = matching.take_skipped_events();
    if skipped_after_filter.iter().any(|event| match event {
        DaemonEvent::PackageEvent { payload, .. } => payload["token"] == "event-other",
        _ => false,
    }) {
        return Err(ConformanceError::UnexpectedValue {
            operation: "subject_filter",
            field: "skipped_events",
            expected: "no non-matching token".to_string(),
            actual: format!("{skipped_after_filter:?}"),
        });
    }

    drop(matching);
    let mut reconnect =
        connect_for_package_event_subscriptions(hub.endpoint()).map_err(|source| {
            ConformanceError::Client {
                operation: "reconnect_events",
                source,
            }
        })?;
    let resubscribed = reconnect
        .subscribe_events(
            "sub-event-reconnect",
            EVENT_CONFORMANCE_PRODUCER,
            EVENT_CONFORMANCE_NAME,
            Vec::new(),
        )
        .map_err(|source| ConformanceError::Client {
            operation: "resubscribe_events",
            source,
        })?;
    expect_kind(
        &resubscribed,
        DaemonResponseKind::EventSubscribed,
        "resubscribe_events",
    )?;
    let _ = reconnect
        .request(&DaemonRequest::Status)
        .map_err(|source| ConformanceError::Client {
            operation: "status_after_reconnect",
            source,
        })?;
    if !reconnect.take_skipped_events().is_empty() {
        return Err(ConformanceError::UnexpectedValue {
            operation: "reconnect_replay",
            field: "skipped_events",
            expected: "empty".to_string(),
            actual: "replayed events".to_string(),
        });
    }

    emit_event_ready(hub.endpoint(), "after-reconnect", None, None, None)?;
    wait_for_event_token(&mut reconnect, "after-reconnect")?;
    let unsubscribed = reconnect
        .unsubscribe_events("sub-event-reconnect")
        .map_err(|source| ConformanceError::Client {
            operation: "unsubscribe_events",
            source,
        })?;
    expect_kind(
        &unsubscribed,
        DaemonResponseKind::EventUnsubscribed,
        "unsubscribe_events",
    )?;
    let status_after = reconnect
        .request(&DaemonRequest::Status)
        .map_err(|source| ConformanceError::Client {
            operation: "status_after_unsubscribe",
            source,
        })?;
    expect_kind(
        &status_after,
        DaemonResponseKind::Status,
        "status_after_unsubscribe",
    )?;

    let mut event_gap = false;
    if let Some(stall_path) = stall_path {
        let mut gap_client =
            connect_for_package_event_subscriptions(hub.endpoint()).map_err(|source| {
                ConformanceError::Client {
                    operation: "gap_connect",
                    source,
                }
            })?;
        let gap_sub = gap_client
            .subscribe_events(
                "sub-event-gap",
                EVENT_CONFORMANCE_PRODUCER,
                EVENT_CONFORMANCE_NAME,
                Vec::new(),
            )
            .map_err(|source| ConformanceError::Client {
                operation: "gap_subscribe",
                source,
            })?;
        expect_kind(
            &gap_sub,
            DaemonResponseKind::EventSubscribed,
            "gap_subscribe",
        )?;
        fs::write(stall_path, b"stall").map_err(|source| ConformanceError::Io {
            operation: "create_event_stall",
            source,
        })?;
        emit_event_ready(hub.endpoint(), "queued", None, None, None)?;
        emit_event_ready(hub.endpoint(), "overflow", None, None, None)?;
        let status_stalled = gap_client
            .request(&DaemonRequest::Status)
            .map_err(|source| ConformanceError::Client {
                operation: "status_during_stall",
                source,
            })?;
        expect_kind(
            &status_stalled,
            DaemonResponseKind::Status,
            "status_during_stall",
        )?;
        let _ = fs::remove_file(stall_path);
        gap_client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .map_err(|source| ConformanceError::Client {
                operation: "gap_timeout",
                source,
            })?;
        match gap_client.next_event() {
            Ok(DaemonEvent::EventGap { .. }) => event_gap = true,
            Ok(other) => {
                return Err(ConformanceError::UnexpectedValue {
                    operation: "event_gap",
                    field: "next_event",
                    expected: "EventGap".to_string(),
                    actual: format!("{other:?}"),
                });
            }
            Err(source) => {
                return Err(ConformanceError::Client {
                    operation: "event_gap",
                    source,
                });
            }
        }
        let status_after_gap = gap_client
            .request(&DaemonRequest::Status)
            .map_err(|source| ConformanceError::Client {
                operation: "status_after_gap",
                source,
            })?;
        expect_kind(
            &status_after_gap,
            DaemonResponseKind::Status,
            "status_after_gap",
        )?;
    }

    Ok(ClientEventConformanceReport {
        negotiated_package_event_subscriptions: negotiated,
        exact_subscribe: true,
        event_received: true,
        subject_filter_dropped_non_matching: true,
        reconnect_without_replay: true,
        unsubscribed: true,
        control_progressed_during_events: true,
        event_gap,
    })
}

fn materialize_event_producer(
    hub: &IsolatedHub,
    producer_path: &Path,
) -> Result<PathBuf, ConformanceError> {
    let dest = hub
        .data_dir()
        .join("packages")
        .join("event-plane-producer-conformance");
    copy_dir_recursive(producer_path, &dest).map_err(|source| ConformanceError::Io {
        operation: "copy_event_producer",
        source,
    })?;
    let manifest_path = dest.join("botster-package.json");
    let mut value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&manifest_path).map_err(|source| {
            ConformanceError::Io {
                operation: "read_event_producer_manifest",
                source,
            }
        })?)
        .map_err(ConformanceError::Json)?;
    value["source"]["path"] = serde_json::json!(dest.display().to_string());
    fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&value).map_err(ConformanceError::Json)?,
    )
    .map_err(|source| ConformanceError::Io {
        operation: "write_event_producer_manifest",
        source,
    })?;
    Ok(dest)
}

fn copy_dir_recursive(from: &Path, to: &Path) -> Result<(), std::io::Error> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let dest = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &dest)?;
        } else {
            fs::copy(entry.path(), dest)?;
        }
    }
    Ok(())
}

fn emit_event_ready(
    endpoint: &DaemonEndpoint,
    token: &str,
    subject: Option<&str>,
    notice: Option<&str>,
    pad: Option<&str>,
) -> Result<(), ConformanceError> {
    let mut arguments = serde_json::json!({ "token": token });
    if let Some(subject) = subject {
        arguments["subject"] = serde_json::Value::String(subject.to_string());
    }
    if let Some(notice) = notice {
        arguments["notice"] = serde_json::Value::String(notice.to_string());
    }
    if let Some(pad) = pad {
        arguments["pad"] = serde_json::Value::String(pad.to_string());
    }
    let emitted = request(
        endpoint,
        DaemonRequest::PluginMcpCallTool {
            name: "event_plane.emit_ready".to_string(),
            arguments,
        },
        "emit_ready",
    )?;
    expect_kind(
        &emitted,
        DaemonResponseKind::PluginMcpToolResult,
        "emit_ready",
    )?;
    let status = emitted
        .plugin_tool_result
        .get("status")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    if status != "accepted" {
        return Err(ConformanceError::UnexpectedValue {
            operation: "emit_ready",
            field: "status",
            expected: "accepted".to_string(),
            actual: status.to_string(),
        });
    }
    Ok(())
}

fn wait_for_event_token(
    connection: &mut DaemonConnection,
    token: &str,
) -> Result<serde_json::Value, ConformanceError> {
    let started = Instant::now();
    let deadline = Duration::from_secs(10);
    loop {
        for event in connection.take_skipped_events() {
            if let DaemonEvent::PackageEvent { payload, .. } = event
                && payload["token"] == token
            {
                return Ok(payload);
            }
        }
        if started.elapsed() >= deadline {
            return Err(ConformanceError::MissingOutput {
                needle: "package event token",
                output: token.to_string(),
            });
        }
        let remaining = deadline.saturating_sub(started.elapsed());
        let _ = connection
            .request(&DaemonRequest::Status)
            .map_err(|source| ConformanceError::Client {
                operation: "status_flush_events",
                source,
            })?;
        for event in connection.take_skipped_events() {
            if let DaemonEvent::PackageEvent { payload, .. } = event
                && payload["token"] == token
            {
                return Ok(payload);
            }
        }
        connection
            .set_read_timeout(Some(remaining.min(Duration::from_millis(50))))
            .map_err(|source| ConformanceError::Client {
                operation: "event_read_timeout",
                source,
            })?;
        match connection.next_event() {
            Ok(DaemonEvent::PackageEvent { payload, .. }) if payload["token"] == token => {
                return Ok(payload);
            }
            Ok(DaemonEvent::PackageEvent { .. }) | Ok(DaemonEvent::EventGap { .. }) => continue,
            Ok(other) => {
                return Err(ConformanceError::UnexpectedValue {
                    operation: "wait_event",
                    field: "event",
                    expected: "PackageEvent".to_string(),
                    actual: format!("{other:?}"),
                });
            }
            Err(DaemonTransportError::Io(error))
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(source) => {
                return Err(ConformanceError::Client {
                    operation: "wait_event",
                    source,
                });
            }
        }
    }
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
    let surface_body = serde_json::to_value(&surface.body)?;
    let surface_kind = value_string(&surface_body, "type", "project_pipelines_surface")?;
    let surface_id = value_string(&surface_body, "id", "project_pipelines_surface")?;
    let surface_node_kinds = ui_node_type_values(&surface_body);
    let form_node = find_ui_node_by_id(&surface_body, "project-pipelines-create-form").ok_or(
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
    let snapshot_body = serde_json::to_value(&snapshot.body)?;
    let snapshot_node_id = value_string(&snapshot_body, "id", "project_pipelines_surface")?;
    let snapshot_node_kinds = ui_node_type_values(&snapshot_body);
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
            request: ui_action_request(
                "invalid-project-pipelines-conformance",
                PROJECT_PIPELINES_SURFACE,
                &form_action_id,
                "project-pipelines-create-form",
                Some(serde_json::json!({ "title": "   " })),
                Some(serde_json::json!({ "pipeline_id": "local_pipeline" })),
            )?,
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
    let invalid_value = serde_json::to_value(&invalid)?;
    let invalid_action_status =
        action_status_string(&invalid_value, "project_pipelines_invalid_action")?;
    let invalid_title_error = field_error_string(
        &invalid_value,
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
        r#"["contract.app","contract.empty","contract.sessions","contract.entities","contract.blocked","contract.invalid_body","contract.settings"]"#,
        &serde_json::to_string(&surface_ids).expect("surface ids serialize"),
    )?;
    let settings_surface_descriptor = surface_descriptor(
        &installed_package.surfaces,
        PLUGIN_CONTRACT_SETTINGS_SURFACE,
    )?;
    let settings_surface_kind: String =
        serde_json::from_value(serde_json::to_value(settings_surface_descriptor.kind)?)?;
    let settings_surface_supports: Vec<String> =
        serde_json::from_value(serde_json::to_value(&settings_surface_descriptor.supports)?)?;
    expect_value(
        "contract_matrix_install",
        "settings_surface.kind",
        "settings",
        &settings_surface_kind,
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

    let navigation = request(
        hub.endpoint(),
        DaemonRequest::ListPackageNavigation,
        "contract_matrix_navigation",
    )?;
    expect_kind(
        &navigation,
        DaemonResponseKind::PackageNavigation,
        "contract_matrix_navigation",
    )?;
    let package_navigation = navigation
        .package_navigation
        .iter()
        .filter(|entry| entry.package_name == PLUGIN_CONTRACT_MATRIX_PACKAGE)
        .collect::<Vec<_>>();
    let navigation_item_ids = package_navigation
        .iter()
        .map(|entry| entry.item_id.clone())
        .collect::<Vec<_>>();
    expect_value(
        "contract_matrix_navigation",
        "item_ids",
        r#"["contract.app","contract.settings"]"#,
        &serde_json::to_string(&navigation_item_ids).expect("navigation ids serialize"),
    )?;
    let app_navigation = package_navigation
        .iter()
        .find(|entry| entry.item_id == "contract.app")
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_navigation",
            field: "contract.app",
        })?;
    let settings_navigation = package_navigation
        .iter()
        .find(|entry| entry.item_id == "contract.settings")
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_navigation",
            field: "contract.settings",
        })?;
    expect_value(
        "contract_matrix_navigation",
        "contract.app.route_path",
        "/packages/botster.plugin-contract-matrix/surfaces/contract.app",
        &app_navigation.route_path,
    )?;
    expect_value(
        "contract_matrix_navigation",
        "contract.settings.route_path",
        "/packages/botster.plugin-contract-matrix/surfaces/contract.settings",
        &settings_navigation.route_path,
    )?;

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
    let app_surface_body = serde_json::to_value(&app_surface.body)?;
    let app_surface_snapshot_body = serde_json::to_value(&app_surface_snapshot.body)?;
    let app_surface_kind = value_string(&app_surface_body, "type", "contract_matrix_render_app")?;
    let app_surface_node_id = value_string(&app_surface_body, "id", "contract_matrix_render_app")?;
    let app_surface_snapshot_id = value_string(
        &app_surface_snapshot_body,
        "id",
        "contract_matrix_render_app",
    )?;
    let app_surface_node_kinds = ui_node_type_values(&app_surface_body);
    let app_surface_snapshot_node_kinds = ui_node_type_values(&app_surface_snapshot_body);
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
    let session_binding_scenario = session_plugin_binding_conformance_scenario();
    let session_surface = render_plugin_surface_with_payload(
        hub,
        PLUGIN_CONTRACT_SESSION_SURFACE,
        serde_json::json!({ "session_uuids": session_binding_scenario.references.clone() }),
        "contract_matrix_render_sessions",
    )?;
    let session_surface_body = serde_json::to_value(&session_surface.body)?;
    let session_surface_matches_fixture = session_surface_body == session_binding_scenario.surface;
    if !session_surface_matches_fixture {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_render_sessions",
            field: "surface.body",
            expected: session_binding_scenario.surface.to_string(),
            actual: session_surface_body.to_string(),
        });
    }
    let session_surface_node_id = value_string(
        &session_surface_body,
        "id",
        "contract_matrix_render_sessions",
    )?;
    let session_materialized_rows = materialize_session_plugin_rows(
        &session_surface_body,
        std::slice::from_ref(&session_binding_scenario.initial_snapshot),
    )
    .map_err(|error| ConformanceError::UnexpectedValue {
        operation: "contract_matrix_materialize_session_rows",
        field: "rows",
        expected: format!("{:?}", session_binding_scenario.row_expected.initial),
        actual: error,
    })?;
    let session_action_row =
        session_materialized_rows
            .get(1)
            .ok_or(ConformanceError::MissingJsonField {
                operation: "contract_matrix_materialize_session_rows",
                field: "rows[1]",
            })?;
    let session_action_control = session_action_row
        .controls
        .iter()
        .find(|control| control.key == "rename")
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_materialize_session_rows",
            field: "rows[1].controls[key=rename]",
        })?;
    let session_remove_action_control = session_action_row
        .controls
        .iter()
        .find(|control| control.key == "remove")
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_materialize_session_rows",
            field: "rows[1].controls[key=remove]",
        })?;
    let session_action = run_session_plugin_action(
        hub,
        session_action_control,
        "contract-action-rename-session",
        "contract_matrix_action_rename_session",
    )?;
    let session_remove_action = run_session_plugin_action(
        hub,
        session_remove_action_control,
        "contract-action-remove-session",
        "contract_matrix_action_remove_session",
    )?;
    let session_action_node_id = session_action.node_id;
    let session_action_payload = session_action.payload;
    let session_action_state = session_action.state;
    let session_action_result_node_id = session_action.result_node_id;
    let session_action_result_payload = session_action.result_payload;
    let session_remove_action_node_id = session_remove_action.node_id;
    let session_remove_action_payload = session_remove_action.payload;
    let session_remove_action_state = session_remove_action.state;
    let session_remove_action_result_node_id = session_remove_action.result_node_id;
    let session_remove_action_result_payload = session_remove_action.result_payload;
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
    let dialog_binding =
        find_presentation_binding_by_node_id(&app_surface_snapshot_body, "contract-dialog").ok_or(
            ConformanceError::MissingJsonField {
                operation: "contract_matrix_render_app",
                field: "ui_tree_snapshot.body contract-dialog presentation binding",
            },
        )?;
    let dialog_predicate =
        dialog_binding
            .get("predicate")
            .ok_or(ConformanceError::MissingJsonField {
                operation: "contract_matrix_render_app",
                field: "contract-dialog predicate",
            })?;
    expect_value(
        "contract_matrix_render_app",
        "contract-dialog predicate.kind",
        "present",
        &value_string(dialog_predicate, "kind", "contract_matrix_render_app")?,
    )?;
    let dialog_presence_key = value_string(dialog_predicate, "key", "contract_matrix_render_app")?;
    expect_value(
        "contract_matrix_render_app",
        "contract-dialog predicate.key",
        "contract-dialog",
        &dialog_presence_key,
    )?;
    let equality_binding = find_presentation_binding_by_node_id(
        &app_surface_snapshot_body,
        "contract-selected-workspace",
    )
    .ok_or(ConformanceError::MissingJsonField {
        operation: "contract_matrix_render_app",
        field: "ui_tree_snapshot.body selected-workspace presentation binding",
    })?;
    let equality_predicate =
        equality_binding
            .get("predicate")
            .ok_or(ConformanceError::MissingJsonField {
                operation: "contract_matrix_render_app",
                field: "selected-workspace predicate",
            })?;
    expect_value(
        "contract_matrix_render_app",
        "selected-workspace predicate.kind",
        "equals",
        &value_string(equality_predicate, "kind", "contract_matrix_render_app")?,
    )?;
    let selected_workspace_equality_key =
        value_string(equality_predicate, "key", "contract_matrix_render_app")?;
    let selected_workspace_equality_value =
        value_string(equality_predicate, "value", "contract_matrix_render_app")?;
    expect_value(
        "contract_matrix_render_app",
        "selected-workspace predicate.key",
        "selected-workspace",
        &selected_workspace_equality_key,
    )?;
    expect_value(
        "contract_matrix_render_app",
        "selected-workspace predicate.value",
        "workspace-alpha",
        &selected_workspace_equality_value,
    )?;
    let toggle_binding =
        find_presentation_binding_by_node_id(&app_surface_snapshot_body, "contract-toggle-state")
            .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_render_app",
            field: "ui_tree_snapshot.body contract-toggle-state presentation binding",
        })?;
    let toggle_key = value_string(
        toggle_binding
            .get("predicate")
            .ok_or(ConformanceError::MissingJsonField {
                operation: "contract_matrix_render_app",
                field: "contract-toggle-state predicate",
            })?,
        "key",
        "contract_matrix_render_app",
    )?;
    let initial_visible_tree = materialize_visible_tree(&app_surface_snapshot_body, None).ok_or(
        ConformanceError::MissingBody {
            operation: "contract_matrix_render_app",
            field: "initial visible tree",
        },
    )?;
    let form_reachable_before_open =
        find_ui_node_by_id(&initial_visible_tree, PLUGIN_CONTRACT_DIALOG_FORM_NODE_ID).is_some();
    if form_reachable_before_open
        || find_ui_node_by_id(&initial_visible_tree, PLUGIN_CONTRACT_DIALOG_NODE_ID).is_some()
    {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_render_app",
            field: "initial modal reachability",
            expected: "dialog and form hidden before open".to_string(),
            actual: initial_visible_tree.to_string(),
        });
    }
    let open_node = find_ui_node_by_id(&initial_visible_tree, "contract-app-open").ok_or(
        ConformanceError::MissingJsonField {
            operation: "contract_matrix_render_app",
            field: "contract-app-open",
        },
    )?;
    let open_action = rendered_action(
        open_node,
        "contract_matrix_render_app",
        "contract-app-open.props.action",
    )?;
    let open_action_payload =
        open_action
            .payload
            .clone()
            .ok_or(ConformanceError::MissingJsonField {
                operation: "contract_matrix_render_app",
                field: "contract-app-open.props.action.payload",
            })?;
    expect_value(
        "contract_matrix_render_app",
        "contract-app-open.props.action.id",
        PLUGIN_CONTRACT_ACTION,
        &open_action.action_id,
    )?;
    let toggle_node = find_ui_node_by_id(&initial_visible_tree, "contract-app-toggle").ok_or(
        ConformanceError::MissingJsonField {
            operation: "contract_matrix_render_app",
            field: "contract-app-toggle",
        },
    )?;
    let toggle_action = rendered_action(
        toggle_node,
        "contract_matrix_render_app",
        "contract-app-toggle.props.action",
    )?;
    let toggle_action_payload =
        toggle_action
            .payload
            .clone()
            .ok_or(ConformanceError::MissingJsonField {
                operation: "contract_matrix_render_app",
                field: "contract-app-toggle.props.action.payload",
            })?;
    expect_value(
        "contract_matrix_render_app",
        "contract-app-toggle.props.action.id",
        PLUGIN_CONTRACT_ACTION,
        &toggle_action.action_id,
    )?;
    let empty_surface = render_plugin_surface(
        hub,
        PLUGIN_CONTRACT_EMPTY_SURFACE,
        "contract_matrix_render_empty",
    )?;
    let empty_surface_body = serde_json::to_value(&empty_surface.body)?;
    let empty_surface_node_id =
        value_string(&empty_surface_body, "id", "contract_matrix_render_empty")?;
    let empty_surface_child_id = empty_surface_body
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
    let settings_surface_body = serde_json::to_value(&settings_surface.body)?;
    let settings_surface_node_id = value_string(
        &settings_surface_body,
        "id",
        "contract_matrix_render_settings",
    )?;
    let settings_text = settings_surface_body
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

    let package_entity_surface = render_plugin_surface(
        hub,
        PLUGIN_CONTRACT_ENTITY_SURFACE,
        "contract_matrix_render_package_entities",
    )?;
    let package_entity_surface_body = serde_json::to_value(&package_entity_surface.body)?;
    let package_entity_surface_node_id = value_string(
        &package_entity_surface_body,
        "id",
        "contract_matrix_render_package_entities",
    )?;
    let package_entity_binding_family = package_entity_surface_body
        .get("children")
        .and_then(serde_json::Value::as_array)
        .and_then(|children| children.first())
        .and_then(|child| child.get("source"))
        .and_then(serde_json::Value::as_str)
        .and_then(|source| source.strip_prefix('/'))
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_render_package_entities",
            field: "children[0].source",
        })?;
    expect_value(
        "contract_matrix_render_package_entities",
        "children[0].source",
        PLUGIN_CONTRACT_ENTITY_FAMILY,
        &package_entity_binding_family,
    )?;

    let mut package_entity_snapshots = Vec::new();
    for (subscription_id, expected_generation) in [
        ("contract-package-entities-first", 1_u64),
        ("contract-package-entities-reconnect", 2_u64),
    ] {
        let mut subscription = botster_hub_client::subscribe_entities(
            hub.endpoint(),
            PLUGIN_CONTRACT_ENTITY_FAMILY,
            subscription_id,
        )
        .map_err(|source| ConformanceError::Client {
            operation: "contract_matrix_subscribe_package_entities",
            source,
        })?;
        subscription
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|source| ConformanceError::Client {
                operation: "contract_matrix_subscribe_package_entities",
                source,
            })?;
        let frame = subscription
            .next_frame()
            .map_err(|source| ConformanceError::Client {
                operation: "contract_matrix_subscribe_package_entities",
                source,
            })?;
        let expected_status = format!("generation-{expected_generation}");
        if !matches!(
            &frame,
            DaemonEntityFrame::Snapshot {
                entity_type,
                snapshot_seq,
                items,
                resync_reason: None,
                ..
            } if entity_type == PLUGIN_CONTRACT_ENTITY_FAMILY
                && *snapshot_seq == expected_generation
                && items.first().and_then(|item| item.get("id")).and_then(serde_json::Value::as_str)
                    == Some("contract-run-1")
                && items.first().and_then(|item| item.get("status")).and_then(serde_json::Value::as_str)
                    == Some(expected_status.as_str())
        ) {
            return Err(ConformanceError::UnexpectedValue {
                operation: "contract_matrix_subscribe_package_entities",
                field: "snapshot",
                expected: format!("authoritative generation {expected_generation}"),
                actual: format!("{frame:?}"),
            });
        }
        subscription
            .unsubscribe()
            .map_err(|source| ConformanceError::Client {
                operation: "contract_matrix_unsubscribe_package_entities",
                source,
            })?;
        package_entity_snapshots.push(frame);
        if expected_generation == 1 {
            let mutation = request(
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
                            serde_json::json!({"type":"select","value":"read"}),
                        ),
                        (
                            "api_token".to_string(),
                            serde_json::json!({"type":"secret","state":"write_only"}),
                        ),
                    ]),
                },
                "contract_matrix_advance_package_entities",
            )?;
            expect_kind(
                &mutation,
                DaemonResponseKind::Packages,
                "contract_matrix_advance_package_entities",
            )?;
            let reload = request(
                hub.endpoint(),
                DaemonRequest::ReloadPackage {
                    package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
                },
                "contract_matrix_reload_package_entities",
            )?;
            expect_kind(
                &reload,
                DaemonResponseKind::PackageDecision,
                "contract_matrix_reload_package_entities",
            )?;
        }
    }
    let package_entity_initial_snapshot = package_entity_snapshots.remove(0);
    let package_entity_reconnect_snapshot = package_entity_snapshots.remove(0);

    let mut presentation_state = ScopedPresentationState::default();
    let original_rendered_tree = app_surface_snapshot_body.clone();
    let mut client_rendered_tree = original_rendered_tree.clone();
    let open = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            request: ui_action_request(
                "contract-action-open",
                PLUGIN_CONTRACT_APP_SURFACE,
                &open_action.action_id,
                &open_action.node_id,
                None,
                open_action.payload.clone(),
            )?,
        },
        "contract_matrix_action_open",
    )?;
    expect_kind(
        &open,
        DaemonResponseKind::PluginActionResult,
        "contract_matrix_action_open",
    )?;
    let open_result = open
        .plugin_action_result
        .as_ref()
        .ok_or(ConformanceError::MissingBody {
            operation: "contract_matrix_action_open",
            field: "plugin_action_result",
        })?;
    presentation_state.apply(
        PLUGIN_CONTRACT_MATRIX_PACKAGE,
        PLUGIN_CONTRACT_APP_SURFACE,
        open_result,
    );
    let open_values =
        presentation_state.values_for(PLUGIN_CONTRACT_MATRIX_PACKAGE, PLUGIN_CONTRACT_APP_SURFACE);
    let dialog_visible_after_open = presentation_binding_visible(dialog_binding, open_values);
    let selected_workspace_visible_after_open =
        presentation_binding_visible(equality_binding, open_values);
    if !dialog_visible_after_open || !selected_workspace_visible_after_open {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_open",
            field: "presentation visibility",
            expected: "dialog and selected workspace visible".to_string(),
            actual: format!(
                "dialog={dialog_visible_after_open} selected_workspace={selected_workspace_visible_after_open}"
            ),
        });
    }
    let open_visible_tree = materialize_visible_tree(&app_surface_snapshot_body, open_values)
        .ok_or(ConformanceError::MissingBody {
            operation: "contract_matrix_action_open",
            field: "visible tree",
        })?;
    let visible_dialog_ids = ui_node_ids_by_type(&open_visible_tree, "dialog");
    if visible_dialog_ids != [PLUGIN_CONTRACT_DIALOG_NODE_ID] {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_open",
            field: "active dialogs",
            expected: "[\"contract-dialog\"]".to_string(),
            actual: format!("{visible_dialog_ids:?}"),
        });
    }
    let active_dialog = find_ui_node_by_id(&open_visible_tree, PLUGIN_CONTRACT_DIALOG_NODE_ID)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_action_open",
            field: "visible contract-dialog",
        })?;
    let visible_form_ids = ui_node_ids_by_type(&open_visible_tree, "form");
    let dialog_form_ids = ui_node_ids_by_type(active_dialog, "form");
    let actionable_sibling_form_during_dialog = visible_form_ids
        .iter()
        .any(|form_id| !dialog_form_ids.contains(form_id));
    if actionable_sibling_form_during_dialog
        || dialog_form_ids != [PLUGIN_CONTRACT_DIALOG_FORM_NODE_ID]
    {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_open",
            field: "blocking dialog form reachability",
            expected: "one form inside contract-dialog and no sibling form".to_string(),
            actual: format!("visible_forms={visible_form_ids:?} dialog_forms={dialog_form_ids:?}"),
        });
    }
    let submit_node = find_ui_node_by_id(active_dialog, PLUGIN_CONTRACT_DIALOG_FORM_NODE_ID)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_action_open",
            field: "contract-dialog contract-app-form",
        })?;
    let submit_action = rendered_action(
        submit_node,
        "contract_matrix_action_open",
        "contract-app-form.props.action",
    )?;
    let submit_action_payload =
        submit_action
            .payload
            .clone()
            .ok_or(ConformanceError::MissingJsonField {
                operation: "contract_matrix_action_open",
                field: "contract-app-form.props.action.payload",
            })?;
    expect_value(
        "contract_matrix_action_open",
        "contract-app-form.props.action.id",
        PLUGIN_CONTRACT_ACTION,
        &submit_action.action_id,
    )?;
    let dialog_form_node_id = submit_action.node_id.clone();
    let submit_action_node_id = submit_action.node_id.clone();
    let submit_action_id = submit_action.action_id.clone();
    let input_node = find_ui_node_by_id(active_dialog, PLUGIN_CONTRACT_DIALOG_INPUT_NODE_ID)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_action_open",
            field: "contract-dialog contract-app-message",
        })?;
    let dialog_input_node_id = value_string(input_node, "id", "contract_matrix_action_open")?;
    let input_name = input_node
        .get("props")
        .and_then(|props| props.get("name"))
        .and_then(serde_json::Value::as_str)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_action_open",
            field: "contract-app-message.props.name",
        })?;
    let invalid_submit_values = form_values(input_name, serde_json::json!("   "));
    let valid_submit_values = form_values(input_name, serde_json::json!("hello"));
    let open_set_values = open_result
        .presentation
        .iter()
        .flatten()
        .filter_map(|operation| match operation {
            UiPresentationOperation::Set { key, value } => Some((key.0.clone(), value.clone())),
            UiPresentationOperation::Clear { .. } | UiPresentationOperation::Toggle { .. } => None,
        })
        .collect::<BTreeMap<_, _>>();
    let expected_open_set_values = BTreeMap::from([
        ("contract-dialog".to_string(), serde_json::json!(true)),
        (
            "selected-workspace".to_string(),
            serde_json::json!("workspace-alpha"),
        ),
    ]);
    if open_set_values != expected_open_set_values {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_open",
            field: "presentation set values",
            expected: format!("{expected_open_set_values:?}"),
            actual: format!("{open_set_values:?}"),
        });
    }
    let open_state_before_rejection = open_values.cloned().unwrap_or_default();

    let action_field_error = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            request: ui_action_request(
                "contract-action-field-error",
                PLUGIN_CONTRACT_APP_SURFACE,
                &submit_action.action_id,
                &submit_action.node_id,
                Some(invalid_submit_values.clone()),
                Some(submit_action_payload.clone()),
            )?,
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
    let action_field_error_result_value = serde_json::to_value(action_field_error_result)?;
    let action_field_error_state = value_string(
        &action_field_error_result_value,
        "state",
        "contract_matrix_action_field_error",
    )?;
    let action_field_error_request_id = value_string(
        &action_field_error_result_value,
        "request_id",
        "contract_matrix_action_field_error",
    )?;
    let action_field_error_message = field_error_string(
        &action_field_error_result_value,
        "contract-app-message",
        "contract_matrix_action_field_error",
    )?;
    expect_value(
        "contract_matrix_action_field_error",
        "state",
        "rejected",
        &action_field_error_state,
    )?;
    apply_action_result_to_client(
        &mut presentation_state,
        &mut client_rendered_tree,
        PLUGIN_CONTRACT_MATRIX_PACKAGE,
        PLUGIN_CONTRACT_APP_SURFACE,
        action_field_error_result,
    )?;
    let rejected_state_retained = presentation_state
        .values_for(PLUGIN_CONTRACT_MATRIX_PACKAGE, PLUGIN_CONTRACT_APP_SURFACE)
        .is_some_and(|values| values == &open_state_before_rejection);
    let rejected_tree_retained = client_rendered_tree == original_rendered_tree;
    let rejected_visible_tree = materialize_visible_tree(
        &client_rendered_tree,
        presentation_state.values_for(PLUGIN_CONTRACT_MATRIX_PACKAGE, PLUGIN_CONTRACT_APP_SURFACE),
    )
    .ok_or(ConformanceError::MissingBody {
        operation: "contract_matrix_action_field_error",
        field: "visible tree",
    })?;
    let rejected_dialog =
        find_ui_node_by_id(&rejected_visible_tree, PLUGIN_CONTRACT_DIALOG_NODE_ID);
    let rejected_dialog_retained = rejected_dialog.is_some();
    let rejected_form_retained = rejected_dialog.is_some_and(|dialog| {
        find_ui_node_by_id(dialog, PLUGIN_CONTRACT_DIALOG_FORM_NODE_ID).is_some()
    });
    if !rejected_state_retained
        || !rejected_tree_retained
        || !rejected_dialog_retained
        || !rejected_form_retained
        || find_ui_node_by_id(&rejected_visible_tree, &dialog_input_node_id).is_none()
    {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_field_error",
            field: "rejected modal retention",
            expected: "state, tree, dialog, form, and field-error input retained".to_string(),
            actual: rejected_visible_tree.to_string(),
        });
    }
    let rejected_field_error_node_id = dialog_input_node_id.clone();

    let action = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            request: ui_action_request(
                "contract-action-success",
                PLUGIN_CONTRACT_APP_SURFACE,
                &submit_action_id,
                &submit_action.node_id,
                Some(valid_submit_values.clone()),
                Some(submit_action_payload),
            )?,
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
    let action_result_value = serde_json::to_value(action_result)?;
    let action_success_state = value_string(
        &action_result_value,
        "state",
        "contract_matrix_action_success",
    )?;
    let action_success_request_id = value_string(
        &action_result_value,
        "request_id",
        "contract_matrix_action_success",
    )?;
    let action_success_message = action_result_value
        .get("normalized_values")
        .and_then(|values| values.get("message"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_action_success",
            field: "normalized_values.message",
        })?;
    let accepted_normalized_values = action_result_value
        .get("normalized_values")
        .cloned()
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_action_success",
            field: "normalized_values",
        })?;
    let action_success_presentation_clear_key = action_result_value
        .get("presentation")
        .and_then(serde_json::Value::as_array)
        .and_then(|operations| operations.first())
        .and_then(|operation| operation.get("key"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_action_success",
            field: "presentation[0].key",
        })?;
    expect_value(
        "contract_matrix_action_success",
        "presentation[0].key",
        &dialog_presence_key,
        &action_success_presentation_clear_key,
    )?;
    let action_success_replacement_node_id = action_result_value
        .get("replacement")
        .and_then(|replacement| replacement.get("id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField {
            operation: "contract_matrix_action_success",
            field: "replacement.id",
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
    apply_action_result_to_client(
        &mut presentation_state,
        &mut client_rendered_tree,
        PLUGIN_CONTRACT_MATRIX_PACKAGE,
        PLUGIN_CONTRACT_APP_SURFACE,
        action_result,
    )?;
    let accepted_replacement_applied = client_rendered_tree != original_rendered_tree
        && client_rendered_tree
            .get("id")
            .and_then(serde_json::Value::as_str)
            == Some(action_success_replacement_node_id.as_str());
    if !accepted_replacement_applied {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_success",
            field: "client rendered tree",
            expected: format!("replacement node {action_success_replacement_node_id}"),
            actual: client_rendered_tree.to_string(),
        });
    }
    let dialog_state_cleared = presentation_state
        .values_for(PLUGIN_CONTRACT_MATRIX_PACKAGE, PLUGIN_CONTRACT_APP_SURFACE)
        .is_none_or(|values| !values.contains_key(&dialog_presence_key));
    let dialog_visible_after_valid_submit = presentation_binding_visible(
        dialog_binding,
        presentation_state.values_for(PLUGIN_CONTRACT_MATRIX_PACKAGE, PLUGIN_CONTRACT_APP_SURFACE),
    );
    if dialog_visible_after_valid_submit {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_success",
            field: "dialog visibility after clear",
            expected: "false".to_string(),
            actual: "true".to_string(),
        });
    }
    if !dialog_state_cleared {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_success",
            field: "dialog state after accepted replacement",
            expected: "dialog presence key cleared".to_string(),
            actual: format!("{presentation_state:?}"),
        });
    }

    let mut toggle_visible_states = vec![presentation_binding_visible(
        toggle_binding,
        presentation_state.values_for(PLUGIN_CONTRACT_MATRIX_PACKAGE, PLUGIN_CONTRACT_APP_SURFACE),
    )];
    for (request_id, operation) in [
        (
            "contract-action-toggle-on",
            "contract_matrix_action_toggle_on",
        ),
        (
            "contract-action-toggle-off",
            "contract_matrix_action_toggle_off",
        ),
    ] {
        let response = request(
            hub.endpoint(),
            DaemonRequest::PluginSurfaceAction {
                package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
                request: ui_action_request(
                    request_id,
                    PLUGIN_CONTRACT_APP_SURFACE,
                    &toggle_action.action_id,
                    &toggle_action.node_id,
                    None,
                    toggle_action.payload.clone(),
                )?,
            },
            operation,
        )?;
        expect_kind(&response, DaemonResponseKind::PluginActionResult, operation)?;
        let result =
            response
                .plugin_action_result
                .as_ref()
                .ok_or(ConformanceError::MissingBody {
                    operation,
                    field: "plugin_action_result",
                })?;
        presentation_state.apply(
            PLUGIN_CONTRACT_MATRIX_PACKAGE,
            PLUGIN_CONTRACT_APP_SURFACE,
            result,
        );
        toggle_visible_states.push(presentation_binding_visible(
            toggle_binding,
            presentation_state
                .values_for(PLUGIN_CONTRACT_MATRIX_PACKAGE, PLUGIN_CONTRACT_APP_SURFACE),
        ));
    }
    if toggle_visible_states != [false, true, false] {
        return Err(ConformanceError::UnexpectedValue {
            operation: "contract_matrix_action_toggle",
            field: "toggle visibility states",
            expected: "[false, true, false]".to_string(),
            actual: format!("{toggle_visible_states:?}"),
        });
    }

    let action_error = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            request: ui_action_request(
                "contract-action-error",
                PLUGIN_CONTRACT_APP_SURFACE,
                &submit_action_id,
                &submit_action.node_id,
                None,
                Some(serde_json::json!({ "fail": true })),
            )?,
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
    let action_error_result_value = serde_json::to_value(action_error_result)?;
    let action_error_state = value_string(
        &action_error_result_value,
        "state",
        "contract_matrix_action_error",
    )?;
    let action_error_request_id = value_string(
        &action_error_result_value,
        "request_id",
        "contract_matrix_action_error",
    )?;
    expect_value(
        "contract_matrix_action_error",
        "state",
        "error",
        &action_error_state,
    )?;

    let identity_mismatch = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            request: ui_action_request(
                "contract-action-identity-mismatch",
                PLUGIN_CONTRACT_APP_SURFACE,
                &submit_action_id,
                &submit_action.node_id,
                None,
                Some(serde_json::json!({ "identity_mismatch": true })),
            )?,
        },
        "contract_matrix_action_identity_mismatch",
    )?;
    expect_kind(
        &identity_mismatch,
        DaemonResponseKind::OperatorError,
        "contract_matrix_action_identity_mismatch",
    )?;
    let identity_mismatch_error =
        identity_mismatch
            .error
            .as_ref()
            .ok_or(ConformanceError::MissingBody {
                operation: "contract_matrix_action_identity_mismatch",
                field: "error",
            })?;
    let identity_mismatch_error_code = identity_mismatch_error.code.clone();
    let identity_mismatch_error_operation = identity_mismatch_error.operation.clone();
    expect_value(
        "contract_matrix_action_identity_mismatch",
        "error.code",
        "invalid_action_result",
        &identity_mismatch_error_code,
    )?;
    expect_value(
        "contract_matrix_action_identity_mismatch",
        "error.operation",
        "plugin_surface_action",
        &identity_mismatch_error_operation,
    )?;

    let invalid_replacement = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            request: ui_action_request(
                "contract-action-invalid-replacement",
                PLUGIN_CONTRACT_APP_SURFACE,
                &submit_action_id,
                &submit_action.node_id,
                None,
                Some(serde_json::json!({ "invalid_replacement": true })),
            )?,
        },
        "contract_matrix_action_invalid_replacement",
    )?;
    expect_kind(
        &invalid_replacement,
        DaemonResponseKind::OperatorError,
        "contract_matrix_action_invalid_replacement",
    )?;
    let invalid_replacement_error =
        invalid_replacement
            .error
            .as_ref()
            .ok_or(ConformanceError::MissingBody {
                operation: "contract_matrix_action_invalid_replacement",
                field: "error",
            })?;
    let invalid_replacement_error_code = invalid_replacement_error.code.clone();
    let invalid_replacement_error_operation = invalid_replacement_error.operation.clone();
    expect_value(
        "contract_matrix_action_invalid_replacement",
        "error.code",
        "invalid_action_result",
        &invalid_replacement_error_code,
    )?;
    expect_value(
        "contract_matrix_action_invalid_replacement",
        "error.operation",
        "plugin_surface_action",
        &invalid_replacement_error_operation,
    )?;

    Ok(PluginContractMatrixConformanceReport {
        package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
        installed_state: installed_package.state.clone(),
        enabled_state: enabled_package.state.clone(),
        version: installed_package.version.clone(),
        source_kind: installed_package.source_kind.clone(),
        surface_ids,
        settings_surface_kind,
        settings_surface_supports,
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
        navigation_item_ids,
        app_navigation_route_path: app_navigation.route_path.clone(),
        settings_navigation_route_path: settings_navigation.route_path.clone(),
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
        session_surface_id: session_surface.surface_id,
        session_surface_node_id,
        session_surface_binding_family: session_binding_scenario.binding_family,
        session_surface_references: session_binding_scenario.references,
        session_surface_matches_fixture,
        package_entity_surface_id: package_entity_surface.surface_id,
        package_entity_surface_node_id,
        package_entity_binding_family,
        package_entity_initial_snapshot,
        package_entity_reconnect_snapshot,
        session_materialized_rows,
        session_action_node_id,
        session_action_payload,
        session_action_state,
        session_action_result_node_id,
        session_action_result_payload,
        session_remove_action_node_id,
        session_remove_action_payload,
        session_remove_action_state,
        session_remove_action_result_node_id,
        session_remove_action_result_payload,
        dialog_presence_key,
        selected_workspace_equality_key,
        selected_workspace_equality_value,
        open_action_id: open_action.action_id,
        open_action_node_id: open_action.node_id,
        open_action_payload,
        open_set_values,
        form_reachable_before_open,
        dialog_visible_after_open,
        selected_workspace_visible_after_open,
        dialog_form_node_id,
        dialog_input_node_id,
        submit_action_node_id,
        actionable_sibling_form_during_dialog,
        invalid_submit_values,
        valid_submit_values,
        rejected_state_retained,
        rejected_tree_retained,
        rejected_dialog_retained,
        rejected_form_retained,
        rejected_field_error_node_id,
        accepted_normalized_values,
        accepted_replacement_applied,
        dialog_state_cleared,
        dialog_visible_after_valid_submit,
        toggle_action_id: toggle_action.action_id,
        toggle_action_node_id: toggle_action.node_id,
        toggle_action_payload,
        toggle_key,
        toggle_visible_states,
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
        action_success_presentation_clear_key,
        action_success_replacement_node_id,
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
        identity_mismatch_error_code,
        identity_mismatch_error_operation,
        invalid_replacement_error_code,
        invalid_replacement_error_operation,
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
/// daemon-provided working directory and environment. The child process decodes
/// Core's structured Hub connection descriptor and uses its Unix socket
/// transport to perform a real `Status` daemon request before exiting.
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
    let hub_connection_env_present = launch.environment.contains_key("BOTSTER_HUB_CONNECTION");
    let hub_data_dir_env_present = launch.environment.contains_key("BOTSTER_HUB_DATA_DIR");
    if !hub_connection_env_present {
        return Err(ConformanceError::MissingEnvironment {
            operation: "resolve_app_launch",
            name: "BOTSTER_HUB_CONNECTION",
        });
    }
    if !hub_data_dir_env_present {
        return Err(ConformanceError::MissingEnvironment {
            operation: "resolve_app_launch",
            name: "BOTSTER_HUB_DATA_DIR",
        });
    }
    let hub_connection: RunnableEntrypointHubConnection =
        serde_json::from_str(&launch.environment["BOTSTER_HUB_CONNECTION"]).map_err(|error| {
            ConformanceError::UnexpectedValue {
                operation: "resolve_app_launch",
                field: "BOTSTER_HUB_CONNECTION",
                expected: "Core RunnableEntrypointHubConnection JSON".to_string(),
                actual: error.to_string(),
            }
        })?;
    hub_connection
        .validate()
        .map_err(|error| ConformanceError::UnexpectedValue {
            operation: "resolve_app_launch",
            field: "BOTSTER_HUB_CONNECTION",
            expected: "valid Core RunnableEntrypointHubConnection".to_string(),
            actual: error.to_string(),
        })?;
    let (hub_connection_transport, hub_connection_socket_path_absolute) =
        match &hub_connection.transport {
            RunnableEntrypointHubConnectionTransport::UnixSocket { path } => {
                ("unix_socket".to_string(), Path::new(path).is_absolute())
            }
        };
    let hub_data_dir_env_absolute =
        Path::new(&launch.environment["BOTSTER_HUB_DATA_DIR"]).is_absolute();
    let launch_working_directory_is_package_root = fs::canonicalize(&package_path)
        .map(|package_root| launch.working_directory == package_root)
        .unwrap_or(false);
    let launch_working_directory_differs_from_daemon_cwd =
        launch.working_directory != *hub.working_directory();

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
        hub_connection_env_present,
        hub_connection_transport,
        hub_connection_socket_path_absolute,
        hub_data_dir_env_present,
        hub_data_dir_env_absolute,
        launch_working_directory_is_package_root,
        launch_working_directory_differs_from_daemon_cwd,
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

let hubConnection;
try {
  hubConnection = JSON.parse(process.env.BOTSTER_HUB_CONNECTION || 'null');
} catch (error) {
  console.error(`invalid BOTSTER_HUB_CONNECTION: ${error}`);
  process.exit(42);
}
const socket = hubConnection?.transport?.type === 'unix_socket'
  ? hubConnection.transport.path
  : undefined;
const dataDir = process.env.BOTSTER_HUB_DATA_DIR;

if (!socket) {
  console.error('BOTSTER_HUB_CONNECTION does not contain a Unix socket');
  process.exit(42);
}
if (!dataDir) {
  console.error('missing BOTSTER_HUB_DATA_DIR');
  process.exit(43);
}
if (!fs.existsSync(socket)) {
  console.error('BOTSTER_HUB_CONNECTION Unix socket path does not exist');
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
    protocol_version: 1,
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

console.log(`hub_connection_present=${Boolean(hubConnection)}`);
console.log(`hub_connection_transport=${hubConnection.transport.type}`);
console.log(`hub_connection_socket_absolute=${socket.startsWith('/')}`);
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
            "injections": [
                {
                    "kind": "hub_connection",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_CONNECTION"
                    },
                    "required": true
                },
                {
                    "kind": "data_dir",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_DATA_DIR"
                    },
                    "required": true
                }
            ],
            "environment": [],
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
    surfaces: &'a [botster_ui_contract::PackageSurfaceDescriptor],
    surface_id: &'static str,
) -> Result<&'a botster_ui_contract::PackageSurfaceDescriptor, ConformanceError> {
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
    render_plugin_surface_with_payload(hub, surface_id, serde_json::json!({}), operation)
}

fn render_plugin_surface_with_payload(
    hub: &IsolatedHub,
    surface_id: &'static str,
    payload: serde_json::Value,
    operation: &'static str,
) -> Result<botster_hub_client::DaemonPluginSurface, ConformanceError> {
    let response = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceRender {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            surface_id: surface_id.to_string(),
            payload,
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

fn append_read_screen(
    terminal: &mut DaemonConnection,
    output: &mut String,
) -> Result<(), ConformanceError> {
    let screen = terminal
        .request(&DaemonRequest::ReadScreen {
            session_id: CONFORMANCE_SESSION_ID.to_string(),
        })
        .map_err(|source| ConformanceError::Client {
            operation: "read_screen",
            source,
        })?;
    if let Some(screen) = screen.read_screen {
        *output = screen.text;
    }
    Ok(())
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

pub(crate) fn diagnostic_kind_label(kind: DaemonDiagnosticKind) -> &'static str {
    match kind {
        DaemonDiagnosticKind::Connected => "connected",
        DaemonDiagnosticKind::Disconnected => "disconnected",
        DaemonDiagnosticKind::CompatibilityMismatch => "compatibility_mismatch",
        DaemonDiagnosticKind::UnsupportedFeature => "unsupported_feature",
        DaemonDiagnosticKind::TerminalStreamUnavailable => "terminal_stream_unavailable",
        DaemonDiagnosticKind::WorkerCompatibility => "worker_compatibility",
        DaemonDiagnosticKind::ActionFailure => "action_failure",
        DaemonDiagnosticKind::DaemonStartupFailure => "daemon_startup_failure",
        DaemonDiagnosticKind::Backpressure => "backpressure",
    }
}

pub(crate) fn daemon_diagnostic_kind_labels() -> Vec<&'static str> {
    vec![
        diagnostic_kind_label(DaemonDiagnosticKind::Connected),
        diagnostic_kind_label(DaemonDiagnosticKind::Disconnected),
        diagnostic_kind_label(DaemonDiagnosticKind::CompatibilityMismatch),
        diagnostic_kind_label(DaemonDiagnosticKind::UnsupportedFeature),
        diagnostic_kind_label(DaemonDiagnosticKind::TerminalStreamUnavailable),
        diagnostic_kind_label(DaemonDiagnosticKind::WorkerCompatibility),
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
    if let Some(bound_node) = value.get("node") {
        collect_ui_node_type_values(bound_node, values);
    }
}

fn find_presentation_binding_by_node_id<'a>(
    value: &'a serde_json::Value,
    node_id: &str,
) -> Option<&'a serde_json::Value> {
    if value
        .get("$kind")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind == "presentation_if")
        && value
            .get("node")
            .and_then(|node| node.get("id"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|id| id == node_id)
    {
        return Some(value);
    }
    if let Some(children) = value.get("children").and_then(serde_json::Value::as_array) {
        for child in children {
            if let Some(found) = find_presentation_binding_by_node_id(child, node_id) {
                return Some(found);
            }
        }
    }
    if let Some(slots) = value.get("slots").and_then(serde_json::Value::as_object) {
        for slot_children in slots.values().filter_map(serde_json::Value::as_array) {
            for child in slot_children {
                if let Some(found) = find_presentation_binding_by_node_id(child, node_id) {
                    return Some(found);
                }
            }
        }
    }
    if let Some(bound_node) = value.get("node") {
        return find_presentation_binding_by_node_id(bound_node, node_id);
    }
    None
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
    if let Some(bound_node) = value.get("node")
        && let Some(found) = find_ui_node_by_id(bound_node, node_id)
    {
        return Some(found);
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

fn rendered_action(
    node: &serde_json::Value,
    operation: &'static str,
    field: &'static str,
) -> Result<RenderedAction, ConformanceError> {
    let node_id = node
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField {
            operation,
            field: "action node id",
        })?;
    let action = node
        .get("props")
        .and_then(|props| props.get("action"))
        .ok_or(ConformanceError::MissingJsonField { operation, field })?;
    let action_id = action
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField { operation, field })?;
    Ok(RenderedAction {
        node_id,
        action_id,
        payload: action.get("payload").cloned(),
    })
}

fn json_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(value) => *value,
        serde_json::Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        serde_json::Value::String(value) => !value.is_empty(),
        serde_json::Value::Array(value) => !value.is_empty(),
        serde_json::Value::Object(value) => !value.is_empty(),
    }
}

fn presentation_binding_visible(
    binding: &serde_json::Value,
    values: Option<&BTreeMap<String, serde_json::Value>>,
) -> bool {
    let Some(predicate) = binding.get("predicate") else {
        return false;
    };
    let Some(key) = predicate.get("key").and_then(serde_json::Value::as_str) else {
        return false;
    };
    let value = values.and_then(|values| values.get(key));
    match predicate.get("kind").and_then(serde_json::Value::as_str) {
        Some("present") => value.is_some(),
        Some("truthy") => value.is_some_and(json_truthy),
        Some("equals") => value == predicate.get("value"),
        _ => false,
    }
}

fn materialize_visible_tree(
    value: &serde_json::Value,
    values: Option<&BTreeMap<String, serde_json::Value>>,
) -> Option<serde_json::Value> {
    if value
        .get("$kind")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind == "presentation_if")
    {
        return presentation_binding_visible(value, values)
            .then(|| value.get("node"))
            .flatten()
            .and_then(|node| materialize_visible_tree(node, values));
    }

    let mut materialized = value.clone();
    let object = materialized.as_object_mut()?;
    if let Some(children) = object
        .get_mut("children")
        .and_then(serde_json::Value::as_array_mut)
    {
        *children = children
            .iter()
            .filter_map(|child| materialize_visible_tree(child, values))
            .collect();
    }
    if let Some(slots) = object
        .get_mut("slots")
        .and_then(serde_json::Value::as_object_mut)
    {
        for slot_children in slots
            .values_mut()
            .filter_map(serde_json::Value::as_array_mut)
        {
            *slot_children = slot_children
                .iter()
                .filter_map(|child| materialize_visible_tree(child, values))
                .collect();
        }
    }
    Some(materialized)
}

fn ui_node_ids_by_type(value: &serde_json::Value, node_type: &str) -> Vec<String> {
    let mut ids = Vec::new();
    collect_ui_node_ids_by_type(value, node_type, &mut ids);
    ids
}

fn collect_ui_node_ids_by_type(value: &serde_json::Value, node_type: &str, ids: &mut Vec<String>) {
    if value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind == node_type)
        && let Some(id) = value.get("id").and_then(serde_json::Value::as_str)
    {
        ids.push(id.to_string());
    }
    if let Some(children) = value.get("children").and_then(serde_json::Value::as_array) {
        for child in children {
            collect_ui_node_ids_by_type(child, node_type, ids);
        }
    }
    if let Some(slots) = value.get("slots").and_then(serde_json::Value::as_object) {
        for slot_children in slots.values().filter_map(serde_json::Value::as_array) {
            for child in slot_children {
                collect_ui_node_ids_by_type(child, node_type, ids);
            }
        }
    }
}

fn form_values(name: &str, value: serde_json::Value) -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::from_iter([(name.to_string(), value)]))
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

fn ui_action_request(
    request_id: &str,
    surface_id: &str,
    action_id: &str,
    node_id: &str,
    values: Option<serde_json::Value>,
    payload: Option<serde_json::Value>,
) -> Result<UiActionRequest, ConformanceError> {
    let values =
        values
            .map(|value| {
                value.as_object().cloned().map(UiFormValues).ok_or(
                    ConformanceError::UnexpectedValue {
                        operation: "ui_action_request",
                        field: "values",
                        expected: "object".to_string(),
                        actual: value.to_string(),
                    },
                )
            })
            .transpose()?;

    Ok(UiActionRequest {
        request_id: UiActionRequestId(request_id.to_string()),
        surface_id: UiSurfaceId(surface_id.to_string()),
        action_id: UiActionId(action_id.to_string()),
        node_id: Some(botster_ui_contract::UiNodeId(node_id.to_string())),
        kind: UiActionKind::Submit,
        values,
        payload,
    })
}

struct SessionPluginActionObservation {
    node_id: String,
    payload: serde_json::Value,
    state: String,
    result_node_id: String,
    result_payload: serde_json::Value,
}

fn run_session_plugin_action(
    hub: &IsolatedHub,
    control: &SessionPluginMaterializedControl,
    request_id: &str,
    operation: &'static str,
) -> Result<SessionPluginActionObservation, ConformanceError> {
    let node_id = control.node_id.clone();
    let payload = control.action_payload.clone();
    let response = request(
        hub.endpoint(),
        DaemonRequest::PluginSurfaceAction {
            package_name: PLUGIN_CONTRACT_MATRIX_PACKAGE.to_string(),
            request: ui_action_request(
                request_id,
                PLUGIN_CONTRACT_SESSION_SURFACE,
                PLUGIN_CONTRACT_ACTION,
                &node_id,
                None,
                Some(payload.clone()),
            )?,
        },
        operation,
    )?;
    expect_kind(&response, DaemonResponseKind::PluginActionResult, operation)?;
    let result = response
        .plugin_action_result
        .ok_or(ConformanceError::MissingBody {
            operation,
            field: "plugin_action_result",
        })?;
    let state = serde_json::to_value(result.state)?
        .as_str()
        .map(str::to_string)
        .ok_or(ConformanceError::MissingJsonField {
            operation,
            field: "state",
        })?;
    let result_node_id = result.node_id.as_ref().map(|id| id.0.clone()).ok_or(
        ConformanceError::MissingJsonField {
            operation,
            field: "node_id",
        },
    )?;
    let result_payload = result.payload.ok_or(ConformanceError::MissingJsonField {
        operation,
        field: "payload",
    })?;

    Ok(SessionPluginActionObservation {
        node_id,
        payload,
        state,
        result_node_id,
        result_payload,
    })
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
    Json(serde_json::Error),
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
            | Self::MissingOutput { .. }
            | Self::Json(_) => ConformanceFailureClass::ProducerContract,
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
            Self::Json(source) => write!(formatter, "UI contract JSON projection failed: {source}"),
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
            Self::Json(source) => Some(source),
        }
    }
}

impl From<serde_json::Error> for ConformanceError {
    fn from(source: serde_json::Error) -> Self {
        Self::Json(source)
    }
}

#[cfg(test)]
mod tests {
    use super::isolated_hub::{cleanup_child, default_socket_name, explicit_path};
    use super::*;
    use botster_hub_client::DaemonCompatibility;
    use std::env;
    #[cfg(unix)]
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    use std::process::Child;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn node_package_asset(path: &str) -> String {
        fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("..")
                .join("packages")
                .join("hub-test-support")
                .join(path),
        )
        .unwrap_or_else(|error| panic!("read Node package asset {path}: {error}"))
    }

    #[test]
    fn many_pty_failure_stage_labels_are_stable() {
        assert_eq!(
            [
                ManyPtyConformanceStage::Spawn,
                ManyPtyConformanceStage::Attach,
                ManyPtyConformanceStage::Drain,
                ManyPtyConformanceStage::Input,
                ManyPtyConformanceStage::History,
                ManyPtyConformanceStage::Cleanup,
            ]
            .map(ManyPtyConformanceStage::as_str),
            ["spawn", "attach", "drain", "input", "history", "cleanup"]
        );
    }

    #[test]
    fn session_plugin_binding_reference_materializer_distinguishes_present_and_absent_rows() {
        let scenario = session_plugin_binding_conformance_scenario();
        let mut frames = vec![scenario.initial_snapshot.clone()];
        let initial = materialize_session_plugin_bindings(&scenario.surface, &frames)
            .expect("materialize initial snapshot");
        assert_eq!(initial, scenario.expected.initial);
        for lifecycle_class in ["current", "ended", "indeterminate", "unavailable"] {
            assert!(
                initial.values().any(|value| value == lifecycle_class),
                "materialized initial state must contain {lifecycle_class}"
            );
        }

        frames.push(scenario.transition_frames[0].clone());
        assert_eq!(
            materialize_session_plugin_bindings(&scenario.surface, &frames)
                .expect("materialize ended patch"),
            scenario.expected.after_ended_patch
        );
        frames.push(scenario.transition_frames[1].clone());
        assert_eq!(
            materialize_session_plugin_bindings(&scenario.surface, &frames)
                .expect("materialize indeterminate patch"),
            scenario.expected.after_indeterminate_patch
        );
        frames.push(scenario.transition_frames[2].clone());
        assert_eq!(
            materialize_session_plugin_bindings(&scenario.surface, &frames)
                .expect("materialize remove"),
            scenario.expected.after_remove
        );
        assert_eq!(
            materialize_session_plugin_bindings(
                &scenario.surface,
                std::slice::from_ref(&scenario.reconnect_snapshot)
            )
            .expect("materialize authoritative reconnect snapshot"),
            scenario.expected.after_reconnect
        );
        let DaemonEntityFrame::Patch { patch, .. } = &scenario.transition_frames[1] else {
            panic!("indeterminate transition must be a patch");
        };
        assert_eq!(
            patch
                .as_object()
                .map(|patch| patch.keys().cloned().collect::<Vec<_>>()),
            Some(vec![
                "lifecycle_class".to_string(),
                "registry_state".to_string(),
                "updated_at".to_string()
            ]),
            "fixture must preserve the producer's omitted optional lifecycle field"
        );
        let DaemonEntityFrame::Snapshot { items, .. } = &scenario.initial_snapshot else {
            panic!("initial frame must be a snapshot");
        };
        let mut transition_row = items
            .iter()
            .find(|item| {
                item.get("session_uuid").and_then(serde_json::Value::as_str)
                    == Some("session-transition")
            })
            .expect("transition row in initial snapshot")
            .clone();
        for frame in &scenario.transition_frames[..2] {
            let DaemonEntityFrame::Patch { patch, .. } = frame else {
                panic!("first two transition frames must be patches");
            };
            merge_patch(&mut transition_row, patch);
        }
        assert_eq!(transition_row["lifecycle"], "exited");
        assert_eq!(transition_row["lifecycle_class"], "indeterminate");

        let malformed_patch = DaemonEntityFrame::Patch {
            subscription_id: "session-plugin-binding-generation-1".to_string(),
            entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
            snapshot_seq: 2,
            id: "session-transition".to_string(),
            patch: serde_json::json!({ "lifecycle_class": null }),
        };
        assert_eq!(
            materialize_session_plugin_bindings(
                &scenario.surface,
                &[scenario.initial_snapshot.clone(), malformed_patch]
            ),
            Err("present session row session-transition is missing lifecycle_class".to_string())
        );
    }

    #[test]
    fn session_plugin_row_materializer_realizes_identity_and_payload_in_producer_order() {
        let scenario = session_plugin_binding_conformance_scenario();
        let mut frames = vec![scenario.initial_snapshot.clone()];
        let initial = materialize_session_plugin_rows(&scenario.surface, &frames)
            .expect("materialize initial current rows");
        assert_eq!(initial, scenario.row_expected.initial);
        assert!(initial.iter().all(|row| row.controls[0].label == "current"));
        assert!(
            initial
                .iter()
                .all(|row| row.controls[1].label == "Rename session")
        );
        frames.push(scenario.transition_frames[0].clone());
        assert_eq!(
            materialize_session_plugin_rows(&scenario.surface, &frames)
                .expect("materialize ended patch rows"),
            scenario.row_expected.after_ended_patch
        );
        frames.push(scenario.transition_frames[1].clone());
        assert_eq!(
            materialize_session_plugin_rows(&scenario.surface, &frames)
                .expect("materialize indeterminate patch rows"),
            scenario.row_expected.after_indeterminate_patch
        );
        frames.push(scenario.transition_frames[2].clone());
        assert_eq!(
            materialize_session_plugin_rows(&scenario.surface, &frames)
                .expect("materialize removed rows"),
            scenario.row_expected.after_remove
        );
        assert_eq!(
            materialize_session_plugin_rows(
                &scenario.surface,
                std::slice::from_ref(&scenario.reconnect_snapshot)
            )
            .expect("materialize reconnect rows"),
            scenario.row_expected.after_reconnect
        );
    }

    #[test]
    fn session_plugin_materializers_reject_malformed_or_ambiguous_oracles() {
        let scenario = session_plugin_binding_conformance_scenario();
        let frames = std::slice::from_ref(&scenario.initial_snapshot);
        let assert_both_reject = |surface: &serde_json::Value| {
            assert!(materialize_session_plugin_bindings(surface, frames).is_err());
            assert!(materialize_session_plugin_rows(surface, frames).is_err());
        };

        let mut missing = scenario.surface.clone();
        missing["children"].as_array_mut().expect("children").pop();
        assert_both_reject(&missing);

        let mut duplicate = scenario.surface.clone();
        let oracle = duplicate["children"]
            .as_array()
            .and_then(|children| children.last())
            .cloned()
            .expect("oracle");
        duplicate["children"]
            .as_array_mut()
            .expect("children")
            .push(oracle);
        assert_both_reject(&duplicate);

        let mut malformed_lifecycle = scenario.surface.clone();
        malformed_lifecycle["children"][0]["item_template"]["props"]["text"]["$bind"] =
            serde_json::json!("@/registry_state");
        assert_both_reject(&malformed_lifecycle);

        let mut malformed_oracle = scenario.surface.clone();
        let last = malformed_oracle["children"]
            .as_array()
            .expect("children")
            .len()
            - 1;
        malformed_oracle["children"][last]["item_template"]["id"]["$bind"] =
            serde_json::json!("@/registry_state");
        assert_both_reject(&malformed_oracle);

        let mut unresolved_label = scenario.surface.clone();
        let last = unresolved_label["children"]
            .as_array()
            .expect("children")
            .len()
            - 1;
        unresolved_label["children"][last]["item_template"]["children"][0]["props"]["label"]["$bind"] =
            serde_json::json!("@/missing_label");
        assert_both_reject(&unresolved_label);

        let mut extra = scenario.surface.clone();
        extra["children"]
            .as_array_mut()
            .expect("children")
            .push(serde_json::json!({
                "$kind": "bind_list",
                "source": "/session",
                "where": { "registry_state": "running" },
                "item_template": {
                    "type": "text",
                    "id": "extra",
                    "props": { "text": "Extra" }
                }
            }));
        assert_both_reject(&extra);
    }

    #[test]
    fn session_plugin_row_materializer_rejects_invalid_and_duplicate_realized_ids() {
        let scenario = session_plugin_binding_conformance_scenario();
        for invalid in [serde_json::Value::Null, serde_json::json!(" \t")] {
            let patch = DaemonEntityFrame::Patch {
                subscription_id: "session-plugin-binding-generation-1".to_string(),
                entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
                snapshot_seq: 2,
                id: "session-transition".to_string(),
                patch: serde_json::json!({ "session_uuid": invalid }),
            };
            assert!(
                materialize_session_plugin_rows(
                    &scenario.surface,
                    &[scenario.initial_snapshot.clone(), patch]
                )
                .is_err()
            );
        }

        let duplicate = DaemonEntityFrame::Patch {
            subscription_id: "session-plugin-binding-generation-1".to_string(),
            entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
            snapshot_seq: 2,
            id: "session-transition".to_string(),
            patch: serde_json::json!({ "session_uuid": "session-stable-current" }),
        };
        assert_eq!(
            materialize_session_plugin_rows(
                &scenario.surface,
                &[scenario.initial_snapshot.clone(), duplicate]
            ),
            Err("duplicate realized node id session-stable-current".to_string())
        );

        let static_collision = DaemonEntityFrame::Patch {
            subscription_id: "session-plugin-binding-generation-1".to_string(),
            entity_type: SESSION_LIFECYCLE_ENTITY_TYPE.to_string(),
            snapshot_seq: 2,
            id: "session-transition".to_string(),
            patch: serde_json::json!({
                "session_uuid": "contract-session-lifecycle-panel"
            }),
        };
        assert_eq!(
            materialize_session_plugin_rows(
                &scenario.surface,
                &[scenario.initial_snapshot.clone(), static_collision]
            ),
            Err("duplicate realized node id contract-session-lifecycle-panel".to_string())
        );
    }

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
            !matrix
                .supported_features
                .contains(&matrix.terminal_streaming.feature),
            "terminal mechanism tokens belong on Hello.terminal_compatibility, not host features"
        );
        assert!(
            !matrix.supported_features.contains(&matrix.resize.feature),
            "terminal mechanism tokens belong on Hello.terminal_compatibility, not host features"
        );
        assert!(!matrix.supported_features.contains(
            &botster_terminal_protocol::FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY.to_string()
        ));
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
    fn rejected_action_result_preserves_scoped_state_and_rendered_tree() {
        let package_name = "botster.test";
        let surface_id = "test.surface";
        let mut presentation_state = ScopedPresentationState::default();
        let mut rendered_tree = serde_json::json!({
            "type": "text",
            "id": "original",
            "props": { "text": "Original" },
        });
        let accepted_seed = UiActionResult {
            request_id: botster_ui_contract::UiActionRequestId("seed".to_string()),
            surface_id: botster_ui_contract::UiSurfaceId(surface_id.to_string()),
            action_id: botster_ui_contract::UiActionId("seed".to_string()),
            node_id: None,
            state: UiActionResultState::Accepted,
            field_errors: BTreeMap::new(),
            form_errors: Vec::new(),
            warnings: Vec::new(),
            normalized_values: None,
            presentation: Some(vec![UiPresentationOperation::Set {
                key: botster_ui_contract::UiPresentationKey("dialog".to_string()),
                value: serde_json::json!(true),
            }]),
            replacement: None,
            payload: None,
            error: None,
        };
        presentation_state.apply(package_name, surface_id, &accepted_seed);
        let state_before = presentation_state
            .values_for(package_name, surface_id)
            .cloned()
            .expect("seeded scoped presentation state");
        let tree_before = rendered_tree.clone();
        let rejected = UiActionResult {
            request_id: botster_ui_contract::UiActionRequestId("rejected".to_string()),
            surface_id: botster_ui_contract::UiSurfaceId(surface_id.to_string()),
            action_id: botster_ui_contract::UiActionId("reject".to_string()),
            node_id: None,
            state: UiActionResultState::Rejected,
            field_errors: BTreeMap::new(),
            form_errors: vec!["Rejected".to_string()],
            warnings: Vec::new(),
            normalized_values: None,
            presentation: Some(vec![UiPresentationOperation::Set {
                key: botster_ui_contract::UiPresentationKey("dialog".to_string()),
                value: serde_json::json!(false),
            }]),
            replacement: Some(Box::new(
                serde_json::from_value(serde_json::json!({
                    "type": "text",
                    "id": "replacement",
                    "props": { "text": "Replacement" },
                }))
                .expect("deserialize replacement node"),
            )),
            payload: None,
            error: Some("Rejected".to_string()),
        };

        apply_action_result_to_client(
            &mut presentation_state,
            &mut rendered_tree,
            package_name,
            surface_id,
            &rejected,
        )
        .expect("ignore rejected effects");

        assert_eq!(
            presentation_state.values_for(package_name, surface_id),
            Some(&state_before)
        );
        assert_eq!(rendered_tree, tree_before);
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
    fn published_plugin_contract_matrix_fixture_declares_a_session_notice() {
        let manifest: serde_json::Value = serde_json::from_slice(
            plugin_contract_matrix_fixture_asset()
                .files
                .iter()
                .find(|file| file.relative_path == "botster-package.json")
                .expect("published fixture includes botster-package.json")
                .contents,
        )
        .expect("fixture manifest is JSON");
        let notices = manifest["events"]["notices"]
            .as_array()
            .expect("fixture declares events.notices");
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0]["name"], "contract.ready");
        assert_eq!(notices[0]["subject_scope"], "session");
        assert_eq!(notices[0]["text_pointer"], "/notice");
        assert!(notices[0].get("owner").is_none());
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
    fn daemon_protocol_typescript_artifact_matches_node_package_copy() {
        assert!(
            daemon_protocol_typescript_artifact()
                .contents
                .contains("export interface DaemonPluginWorkerCounters")
        );
        assert!(
            daemon_protocol_typescript_artifact()
                .contents
                .contains("plugin_worker_counters?: DaemonPluginWorkerCounters | null;")
        );
        assert_eq!(
            daemon_protocol_typescript_artifact().contents,
            node_package_asset("daemon-protocol.ts")
        );
    }

    #[test]
    fn first_party_client_support_matrix_matches_node_package_copy() {
        let expected = format!(
            "{}\n",
            serde_json::to_string_pretty(&first_party_client_support_matrix())
                .expect("serialize first-party client support matrix")
        );

        assert_eq!(
            expected,
            node_package_asset("first-party-client-support-matrix.json")
        );
    }

    #[test]
    fn session_lifecycle_subscription_fixture_matches_node_package_copy() {
        let expected = format!(
            "{}\n",
            serde_json::to_string_pretty(
                &session_lifecycle_subscription_conformance_fixture_json()
            )
            .expect("serialize session lifecycle subscription conformance fixture")
        );

        assert_eq!(
            expected,
            node_package_asset("session-lifecycle-subscription-conformance-fixture.json")
        );
    }

    #[test]
    fn session_plugin_binding_fixture_matches_node_package_copy() {
        let expected = format!(
            "{}\n",
            serde_json::to_string_pretty(&session_plugin_binding_conformance_fixture_json())
                .expect("serialize session plugin binding conformance fixture")
        );

        assert_eq!(
            expected,
            node_package_asset("session-plugin-binding-conformance-fixture.json")
        );
    }

    #[test]
    fn session_lifecycle_subscription_fixture_uses_public_ordered_entity_frames() {
        let scenario = session_lifecycle_subscription_conformance_scenario();
        let sequences = scenario
            .normalized_frames
            .iter()
            .filter_map(|frame| match frame {
                DaemonEntityFrame::Snapshot { snapshot_seq, .. }
                | DaemonEntityFrame::Upsert { snapshot_seq, .. }
                | DaemonEntityFrame::Patch { snapshot_seq, .. }
                | DaemonEntityFrame::Remove { snapshot_seq, .. } => Some(*snapshot_seq),
                DaemonEntityFrame::Error { .. } => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(sequences, vec![0, 1, 2, 3, 4]);
        assert!(matches!(
            scenario.normalized_frames.as_slice(),
            [
                DaemonEntityFrame::Snapshot { .. },
                DaemonEntityFrame::Upsert { .. },
                DaemonEntityFrame::Patch { .. },
                DaemonEntityFrame::Patch { .. },
                DaemonEntityFrame::Remove { .. }
            ]
        ));
        assert_eq!(scenario.overflow.resync_reason, "subscriber_overflow");
        assert!(scenario.overflow.empty_snapshot_valid);
        assert!(scenario.overflow.snapshot_precedes_later_deltas);
        assert!(
            scenario
                .overflow
                .failed_snapshot_delivery_closes_subscription
        );
        assert!(
            scenario
                .fresh_subscription
                .prior_generation_frames_discarded
        );
        assert!(
            scenario
                .fresh_subscription
                .requires_authoritative_snapshot_before_deltas
        );
    }

    #[test]
    fn late_attach_history_conformance_fixture_matches_node_package_copy() {
        let expected = format!(
            "{}\n",
            serde_json::to_string_pretty(&late_attach_history_conformance_fixture_json())
                .expect("serialize late-attach history conformance fixture")
        );

        assert_eq!(
            expected,
            node_package_asset("late-attach-history-conformance-fixture.json")
        );
    }

    #[test]
    fn mode_flags_conformance_fixture_matches_node_package_copy() {
        let expected = format!(
            "{}\n",
            serde_json::to_string_pretty(&mode_flags_conformance_fixture_json())
                .expect("serialize mode-flags conformance fixture")
        );

        assert_eq!(
            expected,
            node_package_asset("mode-flags-conformance-fixture.json")
        );
    }

    #[test]
    fn support_matrix_entity_capabilities_are_disjoint_and_complete() {
        let matrix = first_party_client_support_matrix();
        let mut declared = matrix.entity_actions.supported_capabilities.clone();
        declared.extend(matrix.entity_actions.unsupported_capabilities.clone());
        declared.sort();

        let mut known = vec![
            SUPPORTED_PLUGIN_SURFACE_JSON_ACTIONS.to_string(),
            SUPPORTED_PLUGIN_ENTITY_FRAMES.to_string(),
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
                    botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER,
                    botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION,
                    botster_hub_client::FEATURE_PACKAGE_ROUTES,
                    botster_hub_client::FEATURE_PACKAGE_NAVIGATION,
                    botster_hub_client::FEATURE_SPAWN_TARGETS,
                    botster_hub_client::FEATURE_WORKTREES,
                    botster_hub_client::FEATURE_TERMINAL_READBACK,
                    botster_hub_client::FEATURE_SESSION_ENTITY_SUBSCRIPTIONS,
                    botster_hub_client::FEATURE_SESSION_TYPE_ENTITY_SUBSCRIPTIONS,
                    botster_hub_client::FEATURE_PLUGIN_ENTITY_SUBSCRIPTIONS,
                    botster_hub_client::FEATURE_MODE_GATED_INPUT,
                ],
                "supported_features": [
                    botster_hub_client::FEATURE_SESSIONS,
                    botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER,
                    botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION,
                    botster_hub_client::FEATURE_PACKAGE_ROUTES,
                    botster_hub_client::FEATURE_PACKAGE_NAVIGATION,
                    botster_hub_client::FEATURE_SPAWN_TARGETS,
                    botster_hub_client::FEATURE_WORKTREES,
                    botster_hub_client::FEATURE_TERMINAL_READBACK,
                    botster_hub_client::FEATURE_SESSION_ENTITY_SUBSCRIPTIONS,
                    botster_hub_client::FEATURE_SESSION_TYPE_ENTITY_SUBSCRIPTIONS,
                    botster_hub_client::FEATURE_PLUGIN_ENTITY_SUBSCRIPTIONS,
                    botster_hub_client::FEATURE_MODE_GATED_INPUT,
                    botster_hub_client::FEATURE_HUB_SOURCE_UPDATE,
                    botster_hub_client::FEATURE_UNIX_TERMINAL_ADAPTER,
                    botster_hub_client::FEATURE_TERMINAL_SUBSCRIPTION_CLOSED,
                    botster_hub_client::FEATURE_WEBRTC_TERMINAL_ADAPTER,
                    botster_hub_client::FEATURE_ATTACH_OCCUPANCY,
                    botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS,
                ],
                "diagnostic_kinds": [
                    "connected",
                    "disconnected",
                    "compatibility_mismatch",
                    "unsupported_feature",
                    "terminal_stream_unavailable",
                    "worker_compatibility",
                    "action_failure",
                    "daemon_startup_failure",
                    "backpressure",
                ],
                "session_actions": [
                    "status",
                    "list_sessions",
                    "subscribe_entities",
                    "unsubscribe_entities",
                    "remove_session",
                    "spawn",
                    "attach",
                    "drain",
                    "send_input",
                    "resize",
                    "shutdown_session",
                ],
                "terminal_streaming": {
                    "supported": true,
                    "feature": botster_terminal_protocol::FEATURE_TERMINAL_STREAMING,
                    "helper": "botster_hub_client::stream_attach",
                    "held_open_stream": true,
                    "conformance_ready_output": CONFORMANCE_READY,
                    "conformance_echo_output": CONFORMANCE_ECHO,
                    "missing_session_diagnostic_kind": "terminal_stream_unavailable",
                },
                "session_entities": {
                    "supported": true,
                    "feature": botster_hub_client::FEATURE_SESSION_ENTITY_SUBSCRIPTIONS,
                    "helper": "botster_hub_client::subscribe_session_entities",
                    "frame_type": "botster_hub_client::DaemonEntityFrame",
                    "bounded_delivery": true,
                    "explicit_snapshot_resync": true,
                    "fixture_path": "botster_hub_test_support::session_lifecycle_subscription_conformance_scenario",
                    "json_helper": "botster_hub_test_support::session_lifecycle_subscription_conformance_fixture_json",
                    "runtime_runner": "botster_hub_test_support::run_session_lifecycle_subscription_conformance",
                    "runtime_regression": "session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect",
                    "binding_family": "/session",
                    "lifecycle_class_field": "lifecycle_class",
                    "lifecycle_classes": ["current", "ended", "indeterminate"],
                    "missing_row_state": "unavailable",
                    "plugin_surface_id": PLUGIN_CONTRACT_SESSION_SURFACE,
                    "plugin_binding_fixture_path": "botster_hub_test_support::session_plugin_binding_conformance_scenario",
                    "reference_materializer": "botster_hub_test_support::materialize_session_plugin_bindings",
                    "row_reference_materializer": "botster_hub_test_support::materialize_session_plugin_rows",
                },
                "resize": {
                    "supported": true,
                    "feature": botster_terminal_protocol::FEATURE_RESIZE,
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
                    "runtime_runner": "botster_hub_test_support::run_plugin_contract_matrix_conformance",
                    "presentation_operation_kinds": ["set", "clear", "toggle"],
                    "dialog_presence_key": "contract-dialog",
                    "dialog_form_node_id": "contract-app-form",
                    "dialog_input_node_id": "contract-app-message",
                    "actionable_sibling_form_forbidden": true,
                    "accepted_replacement_scope": "whole_surface",
                    "selected_workspace_equality_key": "selected-workspace",
                    "selected_workspace_equality_value": "workspace-alpha",
                    "authored_set_values": {
                        "contract-dialog": true,
                        "selected-workspace": "workspace-alpha",
                    },
                },
                "entity_actions": {
                    "supported_capabilities": [
                        SUPPORTED_PLUGIN_SURFACE_JSON_ACTIONS,
                        SUPPORTED_PLUGIN_ENTITY_FRAMES,
                    ],
                    "unsupported_capabilities": [],
                },
                "late_attach_history": {
                    "supported": true,
                    "fixture_path": "botster_hub_test_support::late_attach_history_conformance_scenario",
                    "json_helper": "botster_hub_test_support::late_attach_history_conformance_fixture_json",
                    "event_type": "botster_hub_client::DaemonEvent",
                    "runtime_regression": "external_daemon_same_session_reattach_replays_opaque_history_before_live_output",
                },
                "terminal_mode_flags": {
                    "supported": true,
                    "feature": botster_hub_client::FEATURE_TERMINAL_READBACK,
                    "fixture_path": "botster_hub_test_support::mode_flags_conformance_scenario",
                    "json_helper": "botster_hub_test_support::mode_flags_conformance_fixture_json",
                    "request_type": "read_mode_flags",
                    "response_kind": "read_mode_flags",
                },
                "session_type_authoring": {
                    "supported": true,
                    "request_type": "show_session_type_definition",
                    "response_kind": "session_type_definition",
                    "response_field": "session_type_definition",
                    "definition_type": "botster_hub_client::DaemonSessionTypeEditableDefinition",
                    "editable_sources": ["device", "repo"],
                    "read_only_source": "package",
                    "read_only_error_kind": "read_only_session_type_source",
                    // Derived by differencing the authored and published shapes:
                    // `working_directory` and `environment` are the data-loss
                    // fields, and `context` is republished as `context_keys`.
                    "authored_fields_absent_from_published_row": [
                        "context",
                        "environment",
                        "working_directory",
                    ],
                    "admission_group": "allow_runtime",
                    "runtime_regression":
                        "session_type_definition_round_trips_authored_path_and_environment",
                },
                "known_limitations": [
                    "The matrix is a test/docs contract, not a daemon runtime endpoint.",
                    "Shipped Web/TUI binding resolution is owned by downstream client tickets; this Hub fixture is a producer/reference contract.",
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
    fn mode_flags_fixture_preserves_exact_values_attribution_and_errors() {
        let scenario = mode_flags_conformance_scenario();

        assert_eq!(
            scenario.request,
            DaemonRequest::ReadModeFlags {
                session_id: "mode-flags-fixture-session".to_string(),
            }
        );
        assert_eq!(scenario.mouse_off.mode_flags.mouse_mode, 0);
        assert_eq!(scenario.mouse_on.mode_flags.mouse_mode, 9);
        assert_eq!(
            scenario.mouse_off.mode_flags.session_id,
            "mode-flags-fixture-session"
        );
        assert_eq!(
            scenario.mouse_on.mode_flags.session_id,
            "mode-flags-fixture-session"
        );
        assert_eq!(
            scenario.unknown_session.response_kind,
            DaemonResponseKind::OperatorError
        );
        assert_eq!(scenario.unknown_session.error_code, "unknown_session");
        assert!(scenario.unknown_session.mode_flags.is_none());
        assert_eq!(
            scenario.backend_failure.response_kind,
            DaemonResponseKind::OperatorError
        );
        assert_eq!(scenario.backend_failure.error_code, "runtime_error");
        assert!(scenario.backend_failure.mode_flags.is_none());
    }

    #[test]
    fn late_attach_history_fixture_orders_history_before_live_output() {
        let scenario = late_attach_history_conformance_scenario();

        let attaching_index = scenario
            .history_then_live
            .iter()
            .position(|event| {
                matches!(event, DaemonEvent::AttachState { state, .. } if state == "attaching")
            })
            .expect("fixture includes attaching state");
        let history_index = scenario
            .history_then_live
            .iter()
            .position(|event| {
                matches!(
                    event,
                    DaemonEvent::Snapshot { history, .. }
                        | DaemonEvent::Scrollback { history, .. }
                        if history.bytes > 0
                )
            })
            .expect("fixture includes opaque initial state");
        let live_index = scenario
            .history_then_live
            .iter()
            .position(|event| {
                matches!(
                    event,
                    DaemonEvent::TerminalOutput { payload, .. }
                        if live_output_contains(payload, "live-after-attach")
                )
            })
            .expect("fixture includes later live output");
        let attached_index = scenario
            .history_then_live
            .iter()
            .position(|event| {
                matches!(event, DaemonEvent::AttachState { state, .. } if state == "attached")
            })
            .expect("fixture includes attached state");

        assert!(
            attaching_index < history_index
                && history_index < attached_index
                && attached_index < live_index,
            "fixture must preserve attaching < history < attached < live"
        );
        assert_eq!(
            scenario
                .read_screen_text
                .matches("history-before-live")
                .count(),
            1,
            "ReadScreen fixture text is the semantic restored-history oracle"
        );
    }

    #[test]
    fn late_attach_history_fixture_idle_case_does_not_fabricate_scrollback() {
        let scenario = late_attach_history_conformance_scenario();

        assert!(
            !scenario
                .no_history_then_live
                .iter()
                .any(|event| { matches!(event, DaemonEvent::Scrollback { .. }) }),
            "idle fixture must not fabricate scrollback"
        );
        assert!(scenario.no_history_read_screen_text.is_empty());
        let no_history_snapshot =
            scenario
                .no_history_then_live
                .iter()
                .find_map(|event| match event {
                    DaemonEvent::Snapshot { history, .. } => Some(history),
                    _ => None,
                });
        let no_history_snapshot =
            no_history_snapshot.expect("no_history emits explicit blank GHOSTSNP Snapshot");
        let blank = no_history_snapshot
            .decoded_bytes()
            .expect("no_history snapshot decodes");
        assert!(
            blank.starts_with(GHOSTSNP_MAGIC),
            "no_history Snapshot must be GHOSTSNP"
        );
        assert_eq!(blank, LATE_ATTACH_NO_HISTORY_PAYLOAD);
        let attaching = scenario
            .no_history_then_live
            .iter()
            .position(|event| {
                matches!(event, DaemonEvent::AttachState { state, .. } if state == "attaching")
            })
            .expect("attaching");
        let snapshot = scenario
            .no_history_then_live
            .iter()
            .position(|event| matches!(event, DaemonEvent::Snapshot { .. }))
            .expect("snapshot");
        let attached = scenario
            .no_history_then_live
            .iter()
            .position(|event| {
                matches!(event, DaemonEvent::AttachState { state, .. } if state == "attached")
            })
            .expect("attached");
        let live = scenario
            .no_history_then_live
            .iter()
            .position(|event| {
                matches!(
                    event,
                    DaemonEvent::TerminalOutput { payload, .. }
                        if live_output_contains(payload, "live-without-history")
                )
            })
            .expect("live");
        assert!(
            attaching < snapshot && snapshot < attached && attached < live,
            "no_history must preserve attaching < Snapshot < attached < live"
        );
    }

    #[test]
    fn late_attach_history_fixture_preserves_opaque_payload_bytes() {
        let scenario = late_attach_history_conformance_scenario();

        for event in scenario
            .history_then_live
            .iter()
            .chain(scenario.no_history_then_live.iter())
        {
            match event {
                DaemonEvent::Snapshot { history, .. } | DaemonEvent::Scrollback { history, .. } => {
                    let payload = history.decoded_bytes().expect("fixture payload decodes");
                    assert_eq!(history.bytes, payload.len());
                    assert_eq!(
                        history.payload_encoding,
                        botster_hub_client::DaemonHistoryEncoding::Base64
                    );
                    assert!(!payload.is_empty());
                }
                _ => {}
            }
        }
    }

    #[test]
    fn late_attach_goldens_have_distinct_content_identity_and_pinned_provenance() {
        assert_eq!(
            LATE_ATTACH_HISTORY_PAYLOAD.len(),
            LATE_ATTACH_HISTORY_PAYLOAD_LEN
        );
        assert_eq!(
            LATE_ATTACH_NO_HISTORY_PAYLOAD.len(),
            LATE_ATTACH_NO_HISTORY_PAYLOAD_LEN
        );
        assert_ne!(
            LATE_ATTACH_HISTORY_PAYLOAD, LATE_ATTACH_NO_HISTORY_PAYLOAD,
            "history and blank goldens must not dual-use the same bytes"
        );
        assert_ne!(
            LATE_ATTACH_HISTORY_PAYLOAD_SHA256,
            LATE_ATTACH_NO_HISTORY_PAYLOAD_SHA256
        );
        assert_eq!(
            hex_sha256(LATE_ATTACH_HISTORY_PAYLOAD),
            LATE_ATTACH_HISTORY_PAYLOAD_SHA256
        );
        assert_eq!(
            hex_sha256(LATE_ATTACH_NO_HISTORY_PAYLOAD),
            LATE_ATTACH_NO_HISTORY_PAYLOAD_SHA256
        );
        assert_eq!(
            LATE_ATTACH_HISTORY_PAGE_PAYLOAD.len(),
            LATE_ATTACH_HISTORY_PAGE_PAYLOAD_LEN
        );
        assert_eq!(
            LATE_ATTACH_HISTORY_FINISH_PAYLOAD.len(),
            LATE_ATTACH_HISTORY_FINISH_PAYLOAD_LEN
        );
        assert_eq!(
            LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD.len(),
            LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_LEN
        );
        assert_eq!(
            hex_sha256(LATE_ATTACH_HISTORY_PAGE_PAYLOAD),
            LATE_ATTACH_HISTORY_PAGE_PAYLOAD_SHA256
        );
        assert_eq!(
            hex_sha256(LATE_ATTACH_HISTORY_FINISH_PAYLOAD),
            LATE_ATTACH_HISTORY_FINISH_PAYLOAD_SHA256
        );
        assert_eq!(
            hex_sha256(LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD),
            LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_SHA256
        );
        assert!(LATE_ATTACH_HISTORY_READY_PAYLOAD.starts_with(GHOSTSNP_MAGIC));
        assert!(LATE_ATTACH_NO_HISTORY_READY_PAYLOAD.starts_with(GHOSTSNP_MAGIC));
        assert!(!LATE_ATTACH_HISTORY_PAGE_PAYLOAD.starts_with(GHOSTSNP_MAGIC));
        assert!(!LATE_ATTACH_HISTORY_FINISH_PAYLOAD.starts_with(GHOSTSNP_MAGIC));
        assert_eq!(
            late_attach_history_payload_bytes(),
            LATE_ATTACH_HISTORY_READY_PAYLOAD
        );
        assert_eq!(
            late_attach_no_history_payload_bytes(),
            LATE_ATTACH_NO_HISTORY_READY_PAYLOAD
        );
        assert_eq!(
            late_attach_history_payload_sha256(),
            LATE_ATTACH_HISTORY_PAYLOAD_SHA256
        );
        assert_eq!(
            late_attach_no_history_payload_sha256(),
            LATE_ATTACH_NO_HISTORY_PAYLOAD_SHA256
        );
        assert_eq!(
            late_attach_incremental_frame_identity(),
            [
                (
                    LATE_ATTACH_HISTORY_PAGE_PAYLOAD_LEN,
                    LATE_ATTACH_HISTORY_PAGE_PAYLOAD_SHA256
                ),
                (
                    LATE_ATTACH_HISTORY_FINISH_PAYLOAD_LEN,
                    LATE_ATTACH_HISTORY_FINISH_PAYLOAD_SHA256
                ),
                (
                    LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_LEN,
                    LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD_SHA256
                ),
            ]
        );
        let provenance = late_attach_ghostsnp_provenance();
        assert_eq!(
            provenance.protocol_crate,
            LATE_ATTACH_GHOSTSNP_PROTOCOL_CRATE
        );
        assert_eq!(provenance.protocol_git, LATE_ATTACH_GHOSTSNP_PROTOCOL_GIT);
        assert_eq!(
            LATE_ATTACH_GHOSTSNP_CORE_PIN,
            "9cabdfd0588b6c7ed2e121e7b50086ce2a250ec6"
        );
        assert_eq!(provenance.core_pin, LATE_ATTACH_GHOSTSNP_CORE_PIN);
        assert_eq!(
            provenance.fixture_files,
            [
                "late-attach-history-ready-v2.ghostsnp",
                "late-attach-history-page-v2.ghostsnp",
                "late-attach-history-finish-v2.ghostsnp",
                "late-attach-blank-ready-v2.ghostsnp",
                "late-attach-blank-finish-v2.ghostsnp",
            ]
        );
        assert_eq!(provenance.fixture_files, LATE_ATTACH_GHOSTSNP_FILES);
        assert_eq!(
            LATE_ATTACH_GHOSTSNP_GHOSTTY_PIN,
            "eb72ec61304ea256be1d86ed8fa961c84e43ecbd"
        );
    }

    #[test]
    fn late_attach_ghostsnp_goldens_import_with_semantic_screen_state() {
        use botster_core::contract::terminal_screen::TerminalScreenSize;
        use botster_terminal_ghostty::{GhosttyClientProjection, GhosttySnapshotDecodeProgress};

        let mut history_client =
            GhosttyClientProjection::new(TerminalScreenSize::new(24, 80)).expect("history client");
        assert_eq!(
            history_client
                .install_ghostsnp_ready(LATE_ATTACH_HISTORY_READY_PAYLOAD.to_vec())
                .expect("history READY"),
            GhosttySnapshotDecodeProgress::Ready
        );
        assert_eq!(
            history_client
                .apply_ghostsnp_history(LATE_ATTACH_HISTORY_PAGE_PAYLOAD.to_vec())
                .expect("history PAGE"),
            GhosttySnapshotDecodeProgress::History
        );
        assert_eq!(
            history_client
                .apply_ghostsnp_history(LATE_ATTACH_HISTORY_FINISH_PAYLOAD.to_vec())
                .expect("history FINISH"),
            GhosttySnapshotDecodeProgress::Finish
        );
        let history_text: String = history_client
            .project_viewport()
            .expect("history viewport")
            .cells
            .iter()
            .map(|cell| cell.grapheme.as_str())
            .collect();
        assert!(
            history_text.contains("history-before-live"),
            "READY+PAGE+FINISH must restore history marker; got {:?}",
            history_text.chars().take(120).collect::<String>()
        );
        assert!(
            !history_text.contains("alternate"),
            "Golden A must not be the complete-v1 alternate-screen story"
        );

        let mut blank_client =
            GhosttyClientProjection::new(TerminalScreenSize::new(24, 80)).expect("blank client");
        assert_eq!(
            blank_client
                .install_ghostsnp_ready(LATE_ATTACH_NO_HISTORY_READY_PAYLOAD.to_vec())
                .expect("blank READY"),
            GhosttySnapshotDecodeProgress::Ready
        );
        assert_eq!(
            blank_client
                .apply_ghostsnp_history(LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD.to_vec())
                .expect("blank FINISH"),
            GhosttySnapshotDecodeProgress::Finish
        );
        let blank_text: String = blank_client
            .project_viewport()
            .expect("blank viewport")
            .cells
            .iter()
            .map(|cell| cell.grapheme.as_str())
            .collect();
        let non_ws: String = blank_text.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            non_ws.is_empty(),
            "blank READY+FINISH must be empty; got {:?}",
            blank_text.chars().take(120).collect::<String>()
        );
        assert!(!blank_text.contains("history-before-live"));
        assert!(!blank_text.contains("alternate"));
    }

    fn hex_sha256(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
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
        let history_ready = botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(
            LATE_ATTACH_HISTORY_READY_PAYLOAD,
        );
        let history_page = botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(
            LATE_ATTACH_HISTORY_PAGE_PAYLOAD,
        );
        let history_finish = botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(
            LATE_ATTACH_HISTORY_FINISH_PAYLOAD,
        );
        let blank_ready = botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(
            LATE_ATTACH_NO_HISTORY_READY_PAYLOAD,
        );
        let blank_finish = botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(
            LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD,
        );
        let history_live = DaemonLiveOutputPayload::from_bytes(LATE_ATTACH_LIVE_DATA.as_bytes());
        let no_history_live =
            DaemonLiveOutputPayload::from_bytes(LATE_ATTACH_NO_HISTORY_LIVE_DATA.as_bytes());

        assert_eq!(
            value,
            serde_json::json!({
                "conformance_fixture_revision": botster_hub_client::CONFORMANCE_FIXTURE_REVISION,
                "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                "no_history_session_id": LATE_ATTACH_NO_HISTORY_SESSION_ID,
                "no_history_subscription_id": LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
                "read_screen_text": LATE_ATTACH_HISTORY_SCREEN_TEXT,
                "no_history_read_screen_text": "",
                "history_then_live": [
                    {
                        "type": "attach_state",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "state": "attaching",
                    },
                    {
                        "type": "snapshot",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "payload_base64": history_ready.payload_base64,
                        "payload_encoding": "base64",
                        "bytes": LATE_ATTACH_HISTORY_READY_PAYLOAD.len(),
                    },
                    {
                        "type": "snapshot",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "payload_base64": history_page.payload_base64,
                        "payload_encoding": "base64",
                        "bytes": LATE_ATTACH_HISTORY_PAGE_PAYLOAD.len(),
                    },
                    {
                        "type": "snapshot",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "payload_base64": history_finish.payload_base64,
                        "payload_encoding": "base64",
                        "bytes": LATE_ATTACH_HISTORY_FINISH_PAYLOAD.len(),
                    },
                    {
                        "type": "attach_state",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "state": "attached",
                    },
                    {
                        "type": "terminal_output",
                        "session_id": LATE_ATTACH_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_HISTORY_SUBSCRIPTION_ID,
                        "payload_base64": history_live.payload_base64,
                        "payload_encoding": "base64",
                        "bytes": LATE_ATTACH_LIVE_DATA.len(),
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
                        "state": "attaching",
                    },
                    {
                        "type": "snapshot",
                        "session_id": LATE_ATTACH_NO_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
                        "payload_base64": blank_ready.payload_base64,
                        "payload_encoding": "base64",
                        "bytes": LATE_ATTACH_NO_HISTORY_READY_PAYLOAD.len(),
                    },
                    {
                        "type": "snapshot",
                        "session_id": LATE_ATTACH_NO_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
                        "payload_base64": blank_finish.payload_base64,
                        "payload_encoding": "base64",
                        "bytes": LATE_ATTACH_NO_HISTORY_FINISH_PAYLOAD.len(),
                    },
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
                        "payload_base64": no_history_live.payload_base64,
                        "payload_encoding": "base64",
                        "bytes": LATE_ATTACH_NO_HISTORY_LIVE_DATA.len(),
                    },
                    {
                        "type": "process_exit",
                        "session_id": LATE_ATTACH_NO_HISTORY_SESSION_ID,
                        "subscription_id": LATE_ATTACH_NO_HISTORY_SUBSCRIPTION_ID,
                        "code": 0,
                    },
                ],
                "history_incomplete_then_live": [
                    {
                        "type": "attach_state",
                        "session_id": LATE_ATTACH_INCOMPLETE_SESSION_ID,
                        "subscription_id": LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID,
                        "state": "attaching",
                    },
                    {
                        "type": "snapshot",
                        "session_id": LATE_ATTACH_INCOMPLETE_SESSION_ID,
                        "subscription_id": LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID,
                        "payload_base64": history_ready.payload_base64,
                        "payload_encoding": "base64",
                        "bytes": LATE_ATTACH_HISTORY_READY_PAYLOAD.len(),
                    },
                    {
                        "type": "attach_state",
                        "session_id": LATE_ATTACH_INCOMPLETE_SESSION_ID,
                        "subscription_id": LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID,
                        "state": "snapshot_history_incomplete",
                    },
                    {
                        "type": "attach_state",
                        "session_id": LATE_ATTACH_INCOMPLETE_SESSION_ID,
                        "subscription_id": LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID,
                        "state": "attached",
                    },
                    {
                        "type": "terminal_output",
                        "session_id": LATE_ATTACH_INCOMPLETE_SESSION_ID,
                        "subscription_id": LATE_ATTACH_INCOMPLETE_SUBSCRIPTION_ID,
                        "payload_base64": history_live.payload_base64,
                        "payload_encoding": "base64",
                        "bytes": LATE_ATTACH_LIVE_DATA.len(),
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
                "dialog".to_string(),
                "empty_state".to_string(),
                "empty_state".to_string(),
                "form".to_string(),
                "metric".to_string(),
                "metric_grid".to_string(),
                "panel".to_string(),
                "section".to_string(),
                "status_badge".to_string(),
                "table".to_string(),
                "text".to_string(),
                "text".to_string(),
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
    fn shutdown_rejects_client_disconnect_after_clean_daemon_exit() {
        let root = unique_root("shutdown-disconnect-clean-exit");
        let hub_bin = shutdown_script(
            &root,
            "printf 'botster-hub shutdown error: client disconnected\\n' >&2\nexit 1",
        );
        let child = Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .spawn()
            .expect("spawn clean daemon child");
        let mut hub = isolated_hub(hub_bin, root, child);

        let error = hub
            .shutdown_inner()
            .expect_err("shutdown disconnect must remain visible after clean daemon exit");
        assert!(matches!(
            error,
            IsolatedHubError::ShutdownFailed { stderr }
                if stderr.contains("client disconnected")
        ));
        assert!(hub.child.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn shutdown_rejects_client_disconnect_after_failed_daemon_exit() {
        let root = unique_root("shutdown-disconnect-failed-exit");
        let hub_bin = shutdown_script(
            &root,
            "printf 'botster-hub shutdown error: client disconnected\\n' >&2\nexit 1",
        );
        let child = Command::new("sh")
            .arg("-c")
            .arg("exit 42")
            .spawn()
            .expect("spawn failed daemon child");
        let mut hub = isolated_hub(hub_bin, root, child);

        let error = hub
            .shutdown_inner()
            .expect_err("failed daemon exit must not be accepted");

        assert!(matches!(
            error,
            IsolatedHubError::DaemonExited { status, .. } if status.contains("42")
        ));
        assert!(hub.child.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn shutdown_rejects_unrelated_failure_without_waiting_for_live_daemon() {
        let root = unique_root("shutdown-unrelated-live-daemon");
        let hub_bin = shutdown_script(
            &root,
            "printf 'botster-hub shutdown error: permission denied\\n' >&2\nexit 1",
        );
        let child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .spawn()
            .expect("spawn live daemon child");
        let pid = child.id();
        let mut hub = isolated_hub(hub_bin, root, child);

        let error = hub
            .shutdown_inner()
            .expect_err("unrelated shutdown failure must remain an error");

        assert!(matches!(
            error,
            IsolatedHubError::ShutdownFailed { stderr }
                if stderr.contains("permission denied")
        ));
        let child = hub
            .child
            .as_mut()
            .expect("unrelated shutdown failure must retain live daemon child");
        assert!(
            child
                .try_wait()
                .expect("inspect retained live daemon child")
                .is_none(),
            "unrelated shutdown failure must not wait for live daemon child"
        );
        drop(hub);
        assert_process_exits(pid);
    }

    #[cfg(unix)]
    #[test]
    fn panic_drop_reaps_daemon_but_preserves_data_directory_for_diagnosis() {
        let root = unique_root("panic-drop-diagnostics");
        let hub_bin = shutdown_script(&root, "exit 1");
        let child = Command::new("/bin/sleep")
            .arg("60")
            .spawn()
            .expect("spawn panic-drop daemon child");
        let pid = child.id();
        let hub = isolated_hub(hub_bin, root.clone(), child);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _hub = hub;
            panic!("controlled harness panic");
        }));

        assert!(result.is_err());
        assert_process_exits(pid);
        assert!(
            root.exists(),
            "panic-time Drop must preserve failing daemon state"
        );
        fs::remove_dir_all(root).expect("remove preserved panic diagnostics");
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
            .ready_timeout(Duration::from_secs(2))
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
            .ready_timeout(Duration::from_secs(2))
            .start_error();

        assert!(matches!(error, IsolatedHubError::ReadyTimeout { .. }));
        assert_process_exits(wait_for_fake_pid(&pid_file));
    }

    #[test]
    fn isolated_hub_drop_reaps_session_workers_for_the_owned_data_dir() {
        let source = include_str!("isolated_hub.rs");
        let drop_impl = source
            .split("impl Drop for IsolatedHub")
            .nth(1)
            .expect("IsolatedHub Drop");
        let drop_impl = drop_impl
            .split("/// Errors returned")
            .next()
            .unwrap_or(drop_impl);
        assert!(
            drop_impl.contains("reap_session_workers_for_data_dir"),
            "IsolatedHub Drop must reap leftover session-workers for its data dir"
        );
        assert!(
            source.contains("file_name()")
                && source.contains("botster-session-worker")
                && source.contains("args.iter().any(|arg| *arg == dir.as_ref())"),
            "reap must match argv0 botster-session-worker and an exact data-dir argument, not pkill -f"
        );
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_child_falls_back_when_child_is_not_a_process_group_leader() {
        let mut child = Command::new("/bin/sleep")
            .arg("60")
            .spawn()
            .expect("spawn direct child fixture");
        let started = Instant::now();

        cleanup_child(&mut child).expect("clean up direct child fallback");

        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(
            child
                .try_wait()
                .expect("confirm direct child was reaped")
                .is_some()
        );
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_child_terminates_and_reaps_owned_descendant() {
        struct DescendantGuard(libc::pid_t);
        impl Drop for DescendantGuard {
            fn drop(&mut self) {
                unsafe {
                    libc::kill(self.0, libc::SIGKILL);
                }
            }
        }

        let root = unique_root("cleanup-descendant");
        let descendant_pid_file = root.join("descendant.pid");
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                &format!(
                    "sleep 60 & printf '%s\\n' \"$!\" > '{}'; wait",
                    descendant_pid_file.display()
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
        let child = command.spawn().expect("spawn owned descendant fixture");
        let child_pid = child.id();
        let child_guard = CleanupChildGuard::new(child);
        let descendant_pid = wait_for_fake_pid(&descendant_pid_file);
        let _descendant_guard = DescendantGuard(descendant_pid as libc::pid_t);
        let mut child = child_guard.into_child();

        cleanup_child(&mut child).expect("clean up owned process group");

        assert_process_exits(child_pid);
        assert_process_exits(descendant_pid);
        fs::remove_dir_all(root).expect("remove descendant fixture");
    }

    #[cfg(unix)]
    #[test]
    fn missing_pid_publication_reaps_owned_fixture_process_group() {
        let root = unique_root("missing-pid-publication");
        let missing_pid_file = root.join("never-published.pid");
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
        let child = command.spawn().expect("spawn missing-publication fixture");
        let child_pid = child.id();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _child_guard = CleanupChildGuard::new(child);
            let _ = wait_for_fake_pid(&missing_pid_file);
        }));

        assert!(result.is_err(), "missing PID publication must fail loudly");
        assert_process_exits(child_pid);
        assert_process_group_exits(child_pid);
        fs::remove_dir_all(root).expect("remove missing-publication fixture");
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

    #[cfg(unix)]
    fn shutdown_script(root: &Path, shutdown_body: &str) -> PathBuf {
        executable_script(
            root,
            "botster-hub",
            &format!("#!/bin/sh\n{shutdown_body}\n"),
        )
    }

    #[cfg(unix)]
    fn isolated_hub(hub_bin: PathBuf, data_dir: PathBuf, child: Child) -> IsolatedHub {
        IsolatedHub {
            hub_bin,
            endpoint: DaemonEndpoint::new(data_dir.join(default_socket_name())),
            data_dir,
            working_directory: std::env::current_dir().expect("read test working directory"),
            child: Some(child),
        }
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
    fn wait_for_fake_pid(pid_file: &Path) -> u32 {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(pid) = fs::read_to_string(pid_file)
                .ok()
                .and_then(|contents| contents.trim().parse().ok())
            {
                return pid;
            }
            assert!(
                Instant::now() < deadline,
                "fake pid was not published as parseable content: {}",
                pid_file.display()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(unix)]
    struct CleanupChildGuard {
        child: Option<Child>,
    }

    #[cfg(unix)]
    impl CleanupChildGuard {
        fn new(child: Child) -> Self {
            Self { child: Some(child) }
        }

        fn into_child(mut self) -> Child {
            self.child.take().expect("owned child is present")
        }
    }

    #[cfg(unix)]
    impl Drop for CleanupChildGuard {
        fn drop(&mut self) {
            if let Some(child) = &mut self.child {
                let _ = cleanup_child(child);
            }
        }
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
    fn assert_process_group_exits(pgid: u32) {
        let pgid = pgid as libc::pid_t;
        let deadline = Instant::now() + Duration::from_secs(2);
        while unsafe { libc::killpg(pgid, 0) } == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            unsafe { libc::killpg(pgid, 0) },
            -1,
            "process group {pgid} survived"
        );
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
