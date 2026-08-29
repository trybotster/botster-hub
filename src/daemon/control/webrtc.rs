//! Local WebRTC bootstrap, signal, and peer-closed control family.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use botster_hub_client::{
    DaemonDiagnostic, DaemonOperatorError, DaemonRequest, DaemonResponse, DaemonResponseKind,
};

use crate::HubDaemon;
use crate::client_api_dto::response::{daemon_local_webrtc_bootstrap, daemon_response_base};
use crate::daemon::control::message::ControlSender;
use crate::daemon::control::{DaemonObservability, handle_control_request};
use crate::daemon::error::{DaemonTransportResult, local_webrtc_bootstrap_issue_error};
use crate::daemon::owner_loop::PendingRuntimeState;
use crate::daemon_projection::app_local_url;
use crate::transport::webrtc::{
    LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES, LocalWebrtcAttachedSubscription,
    LocalWebrtcSenderTerminalRecord,
};

pub(crate) fn persist_local_webrtc_terminal_record(
    path: &Path,
    record: &LocalWebrtcSenderTerminalRecord,
) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(record)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if bytes.len() > LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "local WebRTC sender terminal record exceeded size bound",
        ));
    }
    let temporary_path = path.with_extension("json.tmp");
    fs::write(&temporary_path, bytes)?;
    if let Err(error) = fs::rename(&temporary_path, path) {
        let _ = fs::remove_file(&temporary_path);
        return Err(error);
    }
    Ok(())
}

pub(crate) fn detach_local_webrtc_subscriptions(
    daemon: &mut HubDaemon,
    logical_clock: &mut u64,
    drain_cursors: &mut BTreeMap<String, u64>,
    pending_runtime: &mut PendingRuntimeState,
    control_tx: ControlSender,
    observability: DaemonObservability<'_>,
    attached_subscriptions: Vec<LocalWebrtcAttachedSubscription>,
) {
    for subscription in attached_subscriptions {
        let _ = handle_control_request(
            daemon,
            logical_clock,
            drain_cursors,
            pending_runtime,
            observability,
            control_tx.clone(),
            DaemonRequest::Detach {
                session_id: subscription.session_id,
                subscription_id: subscription.subscription_id,
            },
        );
    }
}

pub(crate) fn issue_local_webrtc_bootstrap_response(
    daemon: &mut HubDaemon,
    package_name: &str,
    entrypoint_id: &str,
    origin: &str,
) -> DaemonTransportResult<DaemonResponse> {
    if package_name != "botster-web" || entrypoint_id != "web-client" {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_unsupported_entrypoint",
            "local WebRTC page-load bootstrap is only supported for botster-web/web-client",
        ));
    }

    let packages = daemon.package_registry().clone();
    let Some(record) = packages.package(package_name) else {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_package_not_installed",
            format!("package {package_name} is not installed"),
        ));
    };
    if !record.is_enabled() {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_package_disabled",
            format!("package {package_name} is not enabled"),
        ));
    }
    let Some(entrypoint) = record
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id)
    else {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_entrypoint_not_found",
            format!("entrypoint {entrypoint_id} was not found for package {package_name}"),
        ));
    };

    let snapshot = daemon
        .entrypoint_supervisor()
        .status(package_name, entrypoint_id);
    if snapshot.state != "running" {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_entrypoint_not_running",
            format!("entrypoint {package_name}/{entrypoint_id} is not running"),
        ));
    }

    let Some(local_url) = app_local_url(entrypoint, Some(&snapshot)) else {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_local_url_unavailable",
            format!("entrypoint {package_name}/{entrypoint_id} has no structured local_url"),
        ));
    };
    let Some(expected_origin) = crate::admission::grants::origin_from_local_url(&local_url) else {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_invalid_local_url",
            format!("entrypoint {package_name}/{entrypoint_id} local_url has no origin"),
        ));
    };
    if origin != expected_origin {
        return Ok(local_webrtc_bootstrap_issue_error(
            "local_webrtc_bootstrap_origin_mismatch",
            "requested origin does not match running entrypoint local_url origin",
        ));
    }

    let bootstrap =
        daemon
            .local_webrtc()
            .issue_bootstrap(package_name, entrypoint_id, &expected_origin)?;
    Ok(daemon_local_webrtc_bootstrap(bootstrap))
}

pub(crate) fn local_webrtc_peer_gone_request_error(operation: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "local_webrtc_peer_gone".to_string(),
        request_id: format!("local-webrtc-{operation}"),
        operation: operation.to_string(),
        message: "local WebRTC peer is no longer live".to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(
            operation,
            "local WebRTC peer is no longer live",
        )],
    });
    response
}
