//! Local WebRTC bootstrap, signal, and peer-closed control family.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use botster_hub_client::{
    DaemonDiagnostic, DaemonOperatorError, DaemonRequest, DaemonResponse, DaemonResponseKind,
};
use serde_json::Value;

use crate::HubDaemon;
use crate::client_api_dto::response::{
    daemon_local_webrtc_answer, daemon_local_webrtc_bootstrap, daemon_response_base,
};
use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::control::{DaemonObservability, handle_control_request};
use crate::daemon::error::{DaemonTransportResult, local_webrtc_bootstrap_issue_error};
use crate::daemon::owner_loop::{DaemonControlState, PendingRuntimeState};
use crate::daemon_projection::app_local_url;
use crate::subscription::attach_routes::{
    AttachedSubscription, AttachedSubscriptionChange, record_attached_subscription_change,
};
use crate::transport::webrtc::{
    LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES, LocalWebrtcAttachedSubscription,
    LocalWebrtcSenderTerminalRecord, LocalWebrtcSignalRequest,
};

pub(crate) fn handle_request(
    daemon: &mut HubDaemon,
    control_tx: ControlSender,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name,
            entrypoint_id,
            origin,
        } => issue_local_webrtc_bootstrap_response(daemon, &package_name, &entrypoint_id, &origin),
        DaemonRequest::LocalWebrtcSignal {
            grant_id,
            grant_secret,
            origin,
            offer,
        } => signal_response(daemon, control_tx, grant_id, grant_secret, origin, offer),
        _ => unreachable!("webrtc family received a non-webrtc request"),
    }
}

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

pub(crate) fn signal_response(
    daemon: &mut HubDaemon,
    control_tx: ControlSender,
    grant_id: String,
    grant_secret: String,
    origin: String,
    offer: Value,
) -> DaemonTransportResult<DaemonResponse> {
    let signal = LocalWebrtcSignalRequest {
        grant_id,
        grant_secret,
        origin,
        offer,
    };
    let answer = daemon.local_webrtc().signal(signal, control_tx)?;
    Ok(daemon_local_webrtc_answer(answer))
}

pub(crate) fn handle_peer_closed(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    local_webrtc_terminal_record_path: &Path,
    control_tx: ControlSender,
    message: ControlMessage,
) -> bool {
    let ControlMessage::LocalWebrtcPeerClosed {
        grant_id,
        attached_subscriptions,
        entity_subscription_ids,
        terminal_record,
    } = message
    else {
        unreachable!("webrtc peer-closed owner received a non-peer-closed control message");
    };
    let cleanup_reason = format!("webrtc_{}", terminal_record.cause);
    *state
        .lifecycle_counters
        .cleanup_by_reason
        .entry(cleanup_reason)
        .or_default() += 1;
    state.lifecycle_counters.cleanup_completed =
        state.lifecycle_counters.cleanup_completed.saturating_add(1);
    if let Err(error) =
        persist_local_webrtc_terminal_record(local_webrtc_terminal_record_path, &terminal_record)
    {
        eprintln!(
            "local WebRTC sender terminal record persistence failed: kind={:?}",
            error.kind()
        );
    }
    let remove_result = daemon.local_webrtc().remove_peer(&grant_id);
    let mut removed_grants: BTreeSet<String> =
        remove_result.removed_grant_ids.into_iter().collect();
    // Always include the closing grant so entity/attach sweep runs even if the peer
    // map entry was already gone (idempotent PeerClosed).
    removed_grants.insert(grant_id.clone());

    // Snapshot IDs are only removed when the current row is unowned or still owned by a
    // removed grant. A reused subscription_id owned by a different live peer is preserved.
    let mut removed_entity_ids = BTreeSet::new();
    for subscription_id in entity_subscription_ids {
        let should_remove = match state.entity_subscriptions.get(&subscription_id) {
            None => false,
            Some(subscription) => match subscription.owner_grant_id.as_deref() {
                None => true,
                Some(owner) => removed_grants.contains(owner),
            },
        };
        if should_remove {
            removed_entity_ids.insert(subscription_id);
        }
    }
    // Independent of the peer-side snapshot: remove every daemon entity subscription
    // owned by any grant this forget removed (primary + fail-closed siblings).
    for (id, subscription) in &state.entity_subscriptions {
        if let Some(owner) = subscription.owner_grant_id.as_deref()
            && removed_grants.contains(owner)
        {
            removed_entity_ids.insert(id.clone());
        }
    }
    for subscription_id in removed_entity_ids {
        if state
            .entity_subscriptions
            .remove(&subscription_id)
            .is_some()
        {
            state.lifecycle_counters.live_entity_subscriptions = state
                .lifecycle_counters
                .live_entity_subscriptions
                .saturating_sub(1);
            state.released_entity_generations = state.released_entity_generations.saturating_add(1);
        }
    }

    // Merge attach candidates from the PeerClosed snapshot and any fail-closed siblings.
    // Owner-check every row: a delayed snapshot must not detach an attach that a
    // different live grant now owns after (session_id, subscription_id) reuse.
    let mut detach_candidates = attached_subscriptions;
    for subscription in remove_result.attached_subscriptions {
        if !detach_candidates.iter().any(|existing| {
            existing.session_id == subscription.session_id
                && existing.subscription_id == subscription.subscription_id
        }) {
            detach_candidates.push(subscription);
        }
    }
    // Independent of the peer-side snapshot: include every attach currently owned by a
    // removed grant so residual Attach rows that raced after cleanup_once still get cleaned.
    for ((session_id, subscription_id), owner) in &state.pending_runtime.attach_owner_grant_ids {
        if removed_grants.contains(owner.as_str())
            && !detach_candidates.iter().any(|existing| {
                existing.session_id == *session_id && existing.subscription_id == *subscription_id
            })
        {
            detach_candidates.push(LocalWebrtcAttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            });
        }
    }
    let detach_list: Vec<LocalWebrtcAttachedSubscription> = detach_candidates
        .into_iter()
        .filter(|subscription| {
            match state
                .pending_runtime
                .attach_owner_grant_ids
                .get(&(
                    subscription.session_id.clone(),
                    subscription.subscription_id.clone(),
                ))
                .map(String::as_str)
            {
                // Unowned residual (socket path or missing index): allow cleanup.
                None => true,
                // Only detach when the current owner is one of the grants this forget removes.
                Some(owner) => removed_grants.contains(owner),
            }
        })
        .collect();
    // Occupancy set is the counter source of truth. PeerClosed must release
    // live_attach_routes here so a replacement Attach can become live.
    for subscription in &detach_list {
        record_attached_subscription_change(
            &mut state.pending_runtime,
            &mut state.attach_close,
            &mut state.lifecycle_counters,
            Some(AttachedSubscriptionChange::Detach(AttachedSubscription {
                session_id: subscription.session_id.clone(),
                subscription_id: subscription.subscription_id.clone(),
            })),
            None,
        );
    }
    let mut bound_detach = Vec::new();
    let mut unbound_detach = Vec::new();
    for subscription in detach_list {
        if state
            .pending_runtime
            .is_adapter_bound(&subscription.session_id, &subscription.subscription_id)
        {
            bound_detach.push(subscription);
        } else {
            unbound_detach.push(subscription);
        }
    }
    if !bound_detach.is_empty() {
        *state
            .lifecycle_counters
            .cleanup_by_reason
            .entry("bound_adapter_close".to_string())
            .or_insert(0) += bound_detach.len() as u64;
    }
    for grant_id in &removed_grants {
        state.pending_runtime.close_adapters_for_grant(grant_id);
    }
    for subscription in &bound_detach {
        state
            .pending_runtime
            .cancel_stream(&subscription.session_id, &subscription.subscription_id);
    }
    for grant_id in &removed_grants {
        state
            .pending_runtime
            .admission
            .webrtc_admissions
            .remove(grant_id);
        state
            .pending_runtime
            .admission
            .host_compatibility
            .remove(grant_id);
        if let Some(runtime) = daemon.runtime() {
            state
                .event_plane
                .cleanup_connection(grant_id, runtime.package_event_router());
        }
    }
    // Residual same-grant index rows can survive a no-op Core Detach. Drop them
    // after occupancy release. Preserve replacement owners.
    state
        .pending_runtime
        .attach_owner_grant_ids
        .retain(|_, owner| !removed_grants.contains(owner.as_str()));
    detach_local_webrtc_subscriptions(
        daemon,
        &mut state.logical_clock,
        &mut state.drain_cursors,
        &mut state.pending_runtime,
        control_tx,
        DaemonObservability {
            egress: &state.egress_diagnostics,
            lifecycle: &state.lifecycle_counters,
            client_id: None,
            grant_id: None,
        },
        unbound_detach,
    );
    false
}
