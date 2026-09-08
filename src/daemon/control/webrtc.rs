//! Local WebRTC bootstrap, signal, and peer-closed control family.

use std::collections::{BTreeMap, BTreeSet};

use botster_hub_client::{
    DaemonDiagnostic, DaemonOperatorError, DaemonRequest, DaemonResponse, DaemonResponseKind,
};
use serde_json::Value;

use crate::HubDaemon;
use crate::client_api_dto::response::{
    daemon_local_webrtc_answer, daemon_local_webrtc_bootstrap, daemon_response_base,
};
use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::control::pending::retire_abandoned_requests;
use crate::daemon::control::runtime_client_id;
use crate::daemon::error::{DaemonTransportResult, local_webrtc_bootstrap_issue_error};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_loop::tick;
use crate::daemon_projection::app_local_url;
use crate::subscription::attach_routes::{
    AttachStreamOwner, AttachedSubscription, AttachedSubscriptionChange,
    record_attached_subscription_change,
};
use crate::subscription::route_cleanup::{
    CleanupCandidate, candidate_for_departing_owner, retain_route_cleanup,
};
use crate::transport::webrtc::LocalWebrtcSignalRequest;

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

fn issue_local_webrtc_bootstrap_response(
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

fn signal_response(
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
    // A duplicate close (the grant's permit was already taken by an earlier
    // close) has no effect: no counters, no route cleanup.
    let first_close = state.budget.peer_holds_permit(&grant_id);
    if first_close {
        let cleanup_reason = format!("webrtc_{}", terminal_record.cause);
        *state
            .lifecycle_counters
            .cleanup_by_reason
            .entry(cleanup_reason)
            .or_default() += 1;
        state.lifecycle_counters.cleanup_completed =
            state.lifecycle_counters.cleanup_completed.saturating_add(1);
    }
    if let Err(error) = daemon
        .local_webrtc()
        .retain_terminal_record(terminal_record)
    {
        eprintln!("local WebRTC sender terminal record rejected: {error}");
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

    // Duplicate or unknown close snapshots: a grant whose permit was already
    // taken by an earlier close owns nothing now. Only grants still holding
    // their permit take part in route cleanup, and no candidate is built
    // for any other grant.
    let cleaning_grants: BTreeSet<String> = removed_grants
        .iter()
        .filter(|grant| state.budget.peer_holds_permit(grant))
        .cloned()
        .collect();
    let snapshot: BTreeSet<(String, String)> = attached_subscriptions
        .iter()
        .chain(remove_result.attached_subscriptions.iter())
        .map(|subscription| {
            (
                subscription.session_id.clone(),
                subscription.subscription_id.clone(),
            )
        })
        .collect();
    // One validated set drives every cleanup mutation and every Core
    // candidate: a key whose current stream belongs to anyone else (a Unix
    // client or a live grant) is a replacement and is never touched.
    let departing =
        state
            .pending_runtime
            .departing_routes_for_grants(&cleaning_grants, &grant_id, &snapshot);
    for grant in &cleaning_grants {
        let _ = state.pending_runtime.take_owner_routes(grant);
    }
    let mut candidates_by_grant: BTreeMap<String, Vec<CleanupCandidate>> = BTreeMap::new();
    let mut bound_closes = 0u64;
    for (grant, keys) in &departing {
        let departing_owner = AttachStreamOwner {
            client_id: String::new(),
            grant_id: Some(grant.clone()),
        };
        for (session_id, subscription_id) in keys {
            let core_client_id = runtime_client_id(&DaemonRequest::Detach {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            });
            let Some(candidate) = candidate_for_departing_owner(
                &state.pending_runtime,
                &departing_owner,
                &core_client_id,
                session_id,
                subscription_id,
            ) else {
                continue;
            };
            // Synchronous owner bookkeeping, fenced on the captured identity:
            // close and cancel only the departing grant's own stream, and
            // release the live-attach occupancy so a replacement can attach.
            if let Some(identity) = candidate.identity.as_ref() {
                if state
                    .pending_runtime
                    .is_adapter_bound(session_id, subscription_id)
                {
                    bound_closes += 1;
                }
                let _ =
                    state
                        .pending_runtime
                        .close_adapter_if(session_id, subscription_id, identity);
                let _ =
                    state
                        .pending_runtime
                        .cancel_stream_if(session_id, subscription_id, identity);
            }
            record_attached_subscription_change(
                &mut state.pending_runtime,
                &mut state.attach_close,
                &mut state.lifecycle_counters,
                Some(AttachedSubscriptionChange::Detach(AttachedSubscription {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                })),
                None,
            );
            candidates_by_grant
                .entry(grant.clone())
                .or_default()
                .push(candidate);
        }
    }
    if bound_closes > 0 {
        *state
            .lifecycle_counters
            .cleanup_by_reason
            .entry("bound_adapter_close".to_string())
            .or_insert(0) += bound_closes;
    }
    for grant_id in &removed_grants {
        let peer_generation = state
            .pending_runtime
            .admission
            .webrtc_admissions
            .get(grant_id)
            .map(|admission| match admission {
                crate::admission::unix_hello::WebrtcTerminalAdmission::Admitted {
                    peer_generation,
                    ..
                }
                | crate::admission::unix_hello::WebrtcTerminalAdmission::Rejected {
                    peer_generation,
                    ..
                } => *peer_generation,
            });
        if let Some(peer_generation) = peer_generation {
            state
                .pending_runtime
                .admission
                .grant_by_peer_generation
                .remove(&peer_generation);
            let labels = state
                .pending_runtime
                .admission
                .reservations
                .forget_peer(peer_generation);
            crate::daemon::owner_loop::retire_reservation_deadlines(state, labels.iter().cloned());
            if let Some(mut budget) = state
                .pending_runtime
                .admission
                .connection_budgets
                .remove(&peer_generation)
            {
                for label in labels {
                    let _ = budget.release(&label);
                }
            }
        }
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
    let _ = control_tx;
    // Every removed grant (primary and fail-closed siblings) retires its
    // abandoned reads. Every grant still holding its permit hands it to its
    // own cleanup obligation, or releases it when it owns no route.
    for removed in &removed_grants {
        crate::daemon::control::entities::retire_plugin_entity_connection(daemon, state, removed);
        retire_abandoned_requests(daemon, state, removed);
    }
    for grant in &cleaning_grants {
        let Some(permit) = state.budget.take_peer_permit(grant) else {
            continue;
        };
        let candidates = candidates_by_grant.remove(grant).unwrap_or_default();
        if candidates.is_empty() || daemon.runtime().is_none() {
            state.budget.release(permit);
            continue;
        }
        let now = tick(&mut state.logical_clock);
        retain_route_cleanup(
            state,
            permit,
            "webrtc_peer_cleanup",
            None,
            candidates,
            now,
            |state, applied| {
                if applied.failed {
                    state.lifecycle_counters.cleanup_failed =
                        state.lifecycle_counters.cleanup_failed.saturating_add(1);
                }
            },
        );
    }
    false
}
