//! Entity subscription control-message family.

use std::collections::BTreeMap;
use std::time::Instant;

use botster_core::{
    PluginAdmissionResult, PluginHandlerRef, PluginInvocationClass, PluginInvocationResult,
    RequestId,
};

use crate::HubDaemon;
use crate::client_api_dto::response::daemon_response_base;
use crate::daemon::control::message::{ControlMessage, ControlReplySender};
use crate::daemon::control::reply::{RetainedPluginResult, RetainedPluginResultCharge};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_budget::{OwnerPermit, RETAINED_OPERATION_DEADLINE};
use crate::daemon::owner_loop::DaemonControlState;
use crate::runtime::PluginEntitySnapshotInvocation;
use crate::subscription::entity::{
    EntityFrameSender, apply_package_entity_resync_snapshot, entity_subscription_error,
    register_builtin_entity_subscription,
};
use botster_hub_client::{DaemonRequest, DaemonResponse, DaemonResponseKind};
use tokio::sync::mpsc as tokio_mpsc;

const OWNER_ENTITY_REQUEST_PREFIX: &str = "daemon-owner-entity-";

#[derive(Debug, Clone, PartialEq, Eq)]
struct PluginEntityIdentity {
    connection_id: String,
    connection_generation: String,
    transport_request_id: String,
    plugin_key: String,
    handler: PluginHandlerRef,
}

struct PendingEntitySubscribe {
    request: EntitySubscribeRequest,
    permit: OwnerPermit,
    accepted_at: Instant,
}

enum PendingPluginEntityKind {
    Subscribe(PendingEntitySubscribe),
    Resync {
        entity_type: String,
        permit: OwnerPermit,
        accepted_at: Instant,
    },
}

struct PendingPluginEntity {
    identity: PluginEntityIdentity,
    invocation: PluginEntitySnapshotInvocation,
    kind: PendingPluginEntityKind,
    result: Option<RoutedPluginEntityCompletion>,
}

enum RoutedPluginEntityCompletion {
    Invocation(RetainedPluginResult<PluginInvocationResult>),
    Inconsistent(RetainedPluginResult<String>),
}

/// Bounded owner-side state for asynchronous entity-provider calls.
#[derive(Default)]
pub(crate) struct PluginEntityState {
    next_serial: u64,
    pending: BTreeMap<String, PendingPluginEntity>,
    completion_inconsistencies: u64,
}

impl std::fmt::Debug for PluginEntityState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginEntityState")
            .field("pending", &self.pending.len())
            .field(
                "completion_inconsistencies",
                &self.completion_inconsistencies,
            )
            .finish_non_exhaustive()
    }
}

impl PluginEntityState {
    fn next_request_id(&mut self) -> Option<RequestId> {
        self.next_serial = self.next_serial.checked_add(1)?;
        Some(RequestId(format!(
            "{OWNER_ENTITY_REQUEST_PREFIX}{}",
            self.next_serial
        )))
    }

    fn connection_has_capacity(&self, generation: &str) -> bool {
        self.pending
            .values()
            .filter(|entry| {
                entry.identity.connection_generation == generation
                    && matches!(entry.kind, PendingPluginEntityKind::Subscribe(_))
            })
            .count()
            < botster_hub_client::MAX_OUTSTANDING_REQUESTS
    }

    fn insert(
        &mut self,
        invocation: PluginEntitySnapshotInvocation,
        identity: PluginEntityIdentity,
        kind: PendingPluginEntityKind,
    ) {
        self.pending.insert(
            invocation.request.request_id.0.clone(),
            PendingPluginEntity {
                identity,
                invocation,
                kind,
                result: None,
            },
        );
    }

    pub(crate) fn route_completion(
        &mut self,
        completion: RetainedPluginResult<botster_core::PluginCompletion>,
    ) -> Option<RetainedPluginResult<botster_core::PluginCompletion>> {
        let (request_id, handler) = plugin_completion_identity(&completion.value().result);
        let request_id = request_id.clone();
        let handler = handler.clone();
        if !request_id.0.starts_with(OWNER_ENTITY_REQUEST_PREFIX) {
            return Some(completion);
        }
        let Some(entry) = self.pending.get_mut(&request_id.0) else {
            return None;
        };
        if completion.value().class != PluginInvocationClass::RequestResponse
            || entry.identity.plugin_key != handler.plugin_key.0
            || entry.identity.handler != handler
        {
            self.completion_inconsistencies = self.completion_inconsistencies.saturating_add(1);
            if entry.result.is_none() {
                entry.result = Some(RoutedPluginEntityCompletion::Inconsistent(completion.map(
                    |_| {
                        format!(
                            "plugin completion identity did not match admitted request {}",
                            request_id.0
                        )
                    },
                )));
            }
            return None;
        }
        if entry.result.is_none() {
            entry.result = Some(RoutedPluginEntityCompletion::Invocation(
                completion.map(|completion| completion.result),
            ));
        }
        None
    }

    pub(crate) fn next_reply_deadline(&self) -> Option<Instant> {
        self.pending
            .values()
            .filter_map(|entry| match &entry.kind {
                PendingPluginEntityKind::Subscribe(subscribe) => {
                    Some(subscribe.accepted_at + RETAINED_OPERATION_DEADLINE)
                }
                PendingPluginEntityKind::Resync { accepted_at, .. } => {
                    Some(*accepted_at + RETAINED_OPERATION_DEADLINE)
                }
            })
            .min()
    }

    pub(crate) fn has_resync(&self, entity_type: &str) -> bool {
        self.pending.values().any(|entry| {
            matches!(
                &entry.kind,
                PendingPluginEntityKind::Resync {
                    entity_type: pending,
                    ..
                } if pending == entity_type
            )
        })
    }

    fn ready_ids(&self, max_items: usize) -> Vec<String> {
        self.pending
            .iter()
            .filter(|(_, entry)| entry.result.is_some())
            .map(|(request_id, _)| request_id.clone())
            .take(max_items)
            .collect()
    }

    fn take_ready(&mut self, request_id: &str) -> Option<PendingPluginEntity> {
        if !self
            .pending
            .get(request_id)
            .is_some_and(|entry| entry.result.is_some())
        {
            return None;
        }
        let entry = self.pending.remove(request_id)?;
        Some(entry)
    }

    fn take_matching_subscriptions(
        &mut self,
        mut predicate: impl FnMut(&PendingEntitySubscribe, &PluginEntityIdentity) -> bool,
    ) -> Vec<PendingPluginEntity> {
        let request_ids: Vec<String> = self
            .pending
            .iter()
            .filter_map(|(request_id, entry)| match &entry.kind {
                PendingPluginEntityKind::Subscribe(subscribe)
                    if predicate(subscribe, &entry.identity) =>
                {
                    Some(request_id.clone())
                }
                _ => None,
            })
            .collect();
        let removed = request_ids
            .into_iter()
            .filter_map(|request_id| self.pending.remove(&request_id))
            .collect();
        removed
    }

    fn has_subscription(&self, subscription_id: &str) -> bool {
        self.pending.values().any(|entry| {
            matches!(
                &entry.kind,
                PendingPluginEntityKind::Subscribe(subscribe)
                    if subscribe.request.subscription_id == subscription_id
            )
        })
    }

    fn take_expired_resyncs(&mut self, now: Instant) -> Vec<PendingPluginEntity> {
        let expired_pending: Vec<String> = self
            .pending
            .iter()
            .filter_map(|(request_id, entry)| match &entry.kind {
                PendingPluginEntityKind::Resync { accepted_at, .. }
                    if now.saturating_duration_since(*accepted_at)
                        >= RETAINED_OPERATION_DEADLINE =>
                {
                    Some(request_id.clone())
                }
                _ => None,
            })
            .collect();
        let removed = expired_pending
            .into_iter()
            .filter_map(|request_id| self.pending.remove(&request_id))
            .collect();
        removed
    }
}

fn plugin_completion_identity(result: &PluginInvocationResult) -> (&RequestId, &PluginHandlerRef) {
    match result {
        PluginInvocationResult::Completed(success) => (&success.request_id, &success.handler),
        PluginInvocationResult::Failed(failure) => (&failure.request_id, &failure.handler),
    }
}

pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    message: ControlMessage,
) -> bool {
    match message {
        ControlMessage::SubscribeEntities {
            entity_type,
            subscription_id,
            transport_request_id,
            client_id,
            frame_tx,
            frame_rx,
            reply_tx,
            grant_id,
        } => subscribe(
            daemon,
            state,
            EntitySubscribeRequest {
                entity_type,
                subscription_id,
                transport_request_id,
                client_id,
                frame_tx,
                frame_rx,
                reply_tx,
                grant_id,
            },
        ),
        ControlMessage::UnsubscribeEntities {
            subscription_id,
            reply_tx,
            grant_id,
        } => unsubscribe(daemon, state, subscription_id, reply_tx, grant_id),
        _ => unreachable!("entity family received a non-entity control message"),
    }
}

struct EntitySubscribeRequest {
    entity_type: String,
    subscription_id: String,
    transport_request_id: Option<String>,
    client_id: Option<String>,
    frame_tx: EntityFrameSender,
    frame_rx: Option<tokio_mpsc::Receiver<botster_hub_client::DaemonEntityFrame>>,
    reply_tx: ControlReplySender,
    grant_id: Option<String>,
}

fn subscribe(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    request: EntitySubscribeRequest,
) -> bool {
    let EntitySubscribeRequest {
        entity_type,
        subscription_id,
        transport_request_id,
        client_id,
        frame_tx,
        frame_rx,
        reply_tx,
        grant_id,
    } = request;
    // Late WebRTC control messages after PeerClosed must not recreate peer-owned state.
    if let Some(grant_id) = grant_id.as_deref()
        && !daemon.local_webrtc().has_live_peer(grant_id)
    {
        let _ = reply_tx.send(Ok(entity_subscription_error(
            "local_webrtc_peer_gone",
            &subscription_id,
            "local WebRTC peer is no longer live",
        )));
        return false;
    }
    if entity_type != "session" && entity_type != "session_type" {
        return begin_plugin_entity_subscription(
            daemon,
            state,
            EntitySubscribeRequest {
                entity_type,
                subscription_id,
                transport_request_id,
                client_id,
                frame_tx,
                frame_rx,
                reply_tx,
                grant_id,
            },
        );
    }
    let mut response = register_builtin_entity_subscription(
        daemon,
        state,
        entity_type,
        subscription_id.clone(),
        frame_tx,
        grant_id.clone(),
    );
    finish_entity_subscribe_response(
        state,
        &subscription_id,
        frame_rx,
        grant_id,
        reply_tx,
        &mut response,
        None,
    );
    false
}

fn begin_plugin_entity_subscription(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    request: EntitySubscribeRequest,
) -> bool {
    if state
        .entity_subscriptions
        .contains_key(&request.subscription_id)
        || state
            .plugin_entities
            .has_subscription(&request.subscription_id)
    {
        let _ = request.reply_tx.send(Ok(entity_subscription_error(
            "duplicate_entity_subscription",
            &request.subscription_id,
            "entity subscription id is already active",
        )));
        return false;
    }
    let connection_id = request
        .grant_id
        .clone()
        .or_else(|| request.client_id.clone())
        .unwrap_or_else(|| "botster-hub-daemon-socket".to_string());
    let connection_generation = request
        .grant_id
        .as_deref()
        .and_then(|grant_id| {
            state
                .pending_runtime
                .admission
                .webrtc_admissions
                .get(grant_id)
        })
        .map(|admission| match admission {
            crate::admission::unix_hello::WebrtcTerminalAdmission::Admitted {
                peer_generation,
                ..
            }
            | crate::admission::unix_hello::WebrtcTerminalAdmission::Rejected {
                peer_generation,
                ..
            } => format!("webrtc-{peer_generation}"),
        })
        .unwrap_or_else(|| connection_id.clone());
    if !state
        .plugin_entities
        .connection_has_capacity(&connection_generation)
    {
        let _ = request.reply_tx.send(Ok(entity_subscription_error(
            "plugin_entity_subscription_limit",
            &request.subscription_id,
            "the connection already holds its maximum pending entity-provider requests",
        )));
        return false;
    }
    let Some(permit) = state.budget.reserve() else {
        let _ = request.reply_tx.send(Ok(entity_subscription_error(
            crate::daemon::owner_budget::OWNER_BUDGET_EXHAUSTED,
            &request.subscription_id,
            "the daemon holds its maximum retained requests and cleanup; retry later",
        )));
        return false;
    };
    let Some(request_id) = state.plugin_entities.next_request_id() else {
        state.budget.release(permit);
        let _ = request.reply_tx.send(Ok(entity_subscription_error(
            "plugin_request_id_exhausted",
            &request.subscription_id,
            "the daemon exhausted unique plugin request identifiers",
        )));
        return false;
    };
    let Some(runtime) = daemon.runtime() else {
        state.budget.release(permit);
        let _ = request
            .reply_tx
            .send(Err(DaemonTransportError::DaemonNotRunning));
        return false;
    };
    let invocation = match runtime.prepare_plugin_entity_snapshot(
        &request.entity_type,
        &request.subscription_id,
        request_id,
        None,
    ) {
        Ok(invocation) => invocation,
        Err(error) => {
            state.budget.release(permit);
            let _ = request.reply_tx.send(Ok(entity_subscription_error(
                &error.code,
                &request.subscription_id,
                &error.message,
            )));
            return false;
        }
    };
    let identity = PluginEntityIdentity {
        connection_id,
        connection_generation,
        transport_request_id: request
            .transport_request_id
            .clone()
            .unwrap_or_else(|| invocation.request.request_id.0.clone()),
        plugin_key: invocation.request.handler.plugin_key.0.clone(),
        handler: invocation.request.handler.clone(),
    };
    match runtime.try_admit_plugin(
        PluginInvocationClass::RequestResponse,
        invocation.request.clone(),
    ) {
        PluginAdmissionResult::Queued { .. } => {
            state.plugin_entities.insert(
                invocation,
                identity,
                PendingPluginEntityKind::Subscribe(PendingEntitySubscribe {
                    request,
                    permit,
                    accepted_at: Instant::now(),
                }),
            );
            state.maintenance.scheduler.prefer_completion_drain();
        }
        admission => {
            runtime.retire_plugin_entity_snapshot(&invocation);
            state.budget.release(permit);
            let (code, message) = plugin_entity_admission_error(admission);
            let _ = request.reply_tx.send(Ok(entity_subscription_error(
                code,
                &request.subscription_id,
                &message,
            )));
        }
    }
    false
}

fn plugin_entity_admission_error(admission: PluginAdmissionResult) -> (&'static str, String) {
    match admission {
        PluginAdmissionResult::Backpressured { reason, .. } => {
            ("plugin_invocation_backpressured", reason)
        }
        PluginAdmissionResult::RejectedBudget { reason, .. } => {
            ("plugin_invocation_rejected", reason)
        }
        PluginAdmissionResult::WorkerStopped { reason, .. } => ("plugin_worker_stopped", reason),
        _ => (
            "plugin_invocation_rejected",
            "the plugin worker refused the invocation".to_string(),
        ),
    }
}

pub(crate) fn begin_plugin_entity_resync(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    entity_type: String,
    subscription_id: String,
) {
    if state.plugin_entities.has_resync(&entity_type) {
        return;
    }
    let Some(permit) = state.budget.reserve() else {
        return;
    };
    let Some(request_id) = state.plugin_entities.next_request_id() else {
        state.budget.release(permit);
        return;
    };
    let Some(runtime) = daemon.runtime() else {
        state.budget.release(permit);
        return;
    };
    let invocation = match runtime.prepare_plugin_entity_snapshot(
        &entity_type,
        &subscription_id,
        request_id,
        None,
    ) {
        Ok(invocation) => invocation,
        Err(_) => {
            state.budget.release(permit);
            return;
        }
    };
    let identity = PluginEntityIdentity {
        connection_id: format!("entity-resync:{entity_type}"),
        connection_generation: subscription_id.clone(),
        transport_request_id: invocation.request.request_id.0.clone(),
        plugin_key: invocation.request.handler.plugin_key.0.clone(),
        handler: invocation.request.handler.clone(),
    };
    match runtime.try_admit_plugin(
        PluginInvocationClass::RequestResponse,
        invocation.request.clone(),
    ) {
        PluginAdmissionResult::Queued { .. } => {
            state.plugin_entities.insert(
                invocation,
                identity,
                PendingPluginEntityKind::Resync {
                    entity_type,
                    permit,
                    accepted_at: Instant::now(),
                },
            );
            state.maintenance.scheduler.prefer_completion_drain();
        }
        _ => {
            runtime.retire_plugin_entity_snapshot(&invocation);
            state.budget.release(permit);
        }
    }
}

/// Apply ready entity-provider completions without waiting on a worker.
pub(crate) fn drive_plugin_entity_completions(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
) {
    for request_id in state.plugin_entities.ready_ids(8) {
        let Some(mut entry) = state.plugin_entities.take_ready(&request_id) else {
            continue;
        };
        let completion = entry
            .result
            .take()
            .expect("ready entity-provider entry has a completion");
        match entry.kind {
            PendingPluginEntityKind::Subscribe(subscribe) => {
                let PendingEntitySubscribe {
                    request, permit, ..
                } = subscribe;
                let EntitySubscribeRequest {
                    entity_type,
                    subscription_id,
                    frame_rx,
                    frame_tx,
                    reply_tx,
                    grant_id,
                    ..
                } = request;
                let (completion, plugin_result_charge) = match completion {
                    RoutedPluginEntityCompletion::Invocation(result) => {
                        let (result, charge) = result.into_parts();
                        (Ok(result), charge)
                    }
                    RoutedPluginEntityCompletion::Inconsistent(message) => {
                        let (message, charge) = message.into_parts();
                        (Err(message), charge)
                    }
                };
                let mut response = match daemon.runtime() {
                    Some(runtime) => match completion {
                        Ok(result) => match runtime
                            .complete_plugin_entity_snapshot(entry.invocation, result)
                        {
                            Ok((snapshot_seq, items)) => {
                                crate::subscription::entity::register_package_entity_subscription_snapshot(
                                    daemon,
                                    state,
                                    entity_type,
                                    subscription_id.clone(),
                                    frame_tx,
                                    grant_id.clone(),
                                    snapshot_seq,
                                    items,
                                )
                            }
                            Err(error) => Ok(entity_subscription_error(
                                &error.code,
                                &subscription_id,
                                &error.message,
                            )),
                        },
                        Err(message) => {
                            runtime.retire_plugin_entity_snapshot(&entry.invocation);
                            Ok(entity_subscription_error(
                                "plugin_completion_inconsistent",
                                &subscription_id,
                                &message,
                            ))
                        }
                    },
                    None => Err(DaemonTransportError::DaemonNotRunning),
                };
                state.budget.release(permit);
                finish_entity_subscribe_response(
                    state,
                    &subscription_id,
                    frame_rx,
                    grant_id,
                    reply_tx,
                    &mut response,
                    Some(plugin_result_charge),
                );
            }
            PendingPluginEntityKind::Resync {
                entity_type,
                permit,
                accepted_at: _,
            } => {
                let completed = daemon.runtime().and_then(|runtime| match completion {
                    RoutedPluginEntityCompletion::Invocation(result) => {
                        let (result, _charge) = result.into_parts();
                        runtime
                            .complete_plugin_entity_snapshot(entry.invocation, result)
                            .ok()
                    }
                    RoutedPluginEntityCompletion::Inconsistent(message) => {
                        let (_message, _charge) = message.into_parts();
                        runtime.retire_plugin_entity_snapshot(&entry.invocation);
                        None
                    }
                });
                state.budget.release(permit);
                if let Some((snapshot_seq, items)) = completed {
                    apply_package_entity_resync_snapshot(
                        daemon,
                        state,
                        &entity_type,
                        snapshot_seq,
                        items,
                    );
                }
            }
        }
    }
}

/// Retire closed or expired entity-provider replies. Execution can continue.
pub(crate) fn retire_plugin_entity_replies(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    now: Instant,
) {
    let retired = state
        .plugin_entities
        .take_matching_subscriptions(|subscribe, _| {
            subscribe.request.reply_tx.is_closed()
                || now.saturating_duration_since(subscribe.accepted_at)
                    >= RETAINED_OPERATION_DEADLINE
        });
    retire_plugin_entity_entries(daemon, state, retired, true);
}

/// Retire expired entity-provider resync work. Execution can continue.
pub(crate) fn expire_plugin_entity_resyncs(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    now: Instant,
) {
    let retired = state.plugin_entities.take_expired_resyncs(now);
    retire_plugin_entity_entries(daemon, state, retired, false);
}

/// Retire pending entity-provider replies owned by one closed connection.
pub(crate) fn retire_plugin_entity_connection(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    connection_id: &str,
) {
    let retired = state
        .plugin_entities
        .take_matching_subscriptions(|_, identity| identity.connection_id == connection_id);
    retire_plugin_entity_entries(daemon, state, retired, true);
}

fn retire_plugin_entity_entries(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    entries: Vec<PendingPluginEntity>,
    count_abandoned: bool,
) {
    for entry in entries {
        if let Some(runtime) = daemon.runtime() {
            runtime.retire_plugin_entity_snapshot(&entry.invocation);
        }
        let permit = match entry.kind {
            PendingPluginEntityKind::Subscribe(subscribe) => subscribe.permit,
            PendingPluginEntityKind::Resync { permit, .. } => permit,
        };
        state.budget.release(permit);
        if count_abandoned {
            state.budget.counters.retired_abandoned =
                state.budget.counters.retired_abandoned.saturating_add(1);
        }
    }
}

fn retire_plugin_entity_subscription(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    subscription_id: &str,
    owner: Option<&str>,
) {
    let retired = state
        .plugin_entities
        .take_matching_subscriptions(|subscribe, _| {
            subscribe.request.subscription_id == subscription_id
                && owner.is_none_or(|owner| subscribe.request.grant_id.as_deref() == Some(owner))
        });
    retire_plugin_entity_entries(daemon, state, retired, true);
}

fn finish_entity_subscribe_response(
    state: &mut DaemonControlState,
    subscription_id: &str,
    frame_rx: Option<tokio_mpsc::Receiver<botster_hub_client::DaemonEntityFrame>>,
    grant_id: Option<String>,
    reply_tx: ControlReplySender,
    response: &mut DaemonTransportResult<DaemonResponse>,
    plugin_result_charge: Option<RetainedPluginResultCharge>,
) {
    if response
        .as_ref()
        .is_ok_and(|response| response.kind == DaemonResponseKind::EntitySubscribed)
        && let (Some(grant_id), Some(frame_rx)) = (grant_id.as_deref(), frame_rx)
    {
        let Some(peer_generation) = state
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
            })
        else {
            remove_entity_subscription(state, subscription_id);
            let _ = reply_tx.send(Ok(entity_subscription_error(
                "local_webrtc_peer_gone",
                subscription_id,
                "local WebRTC peer admission is no longer live",
            )));
            return;
        };
        state.pending_runtime.admission.next_subscription_generation = state
            .pending_runtime
            .admission
            .next_subscription_generation
            .saturating_add(1);
        let generation = state.pending_runtime.admission.next_subscription_generation;
        let reserved = state
            .pending_runtime
            .admission
            .reservations
            .reserve_subscription(
                crate::admission::connection_budget::ChannelClass::Entity,
                subscription_id.to_string(),
                generation,
                peer_generation,
                crate::admission::reservations::now_seconds(),
                crate::admission::reservations::ReservationBinding::Entity {
                    receiver: std::sync::Arc::new(std::sync::Mutex::new(Some(frame_rx))),
                },
            );
        match reserved {
            Ok(reservation) => {
                let budget = state
                    .pending_runtime
                    .admission
                    .connection_budgets
                    .get_mut(&peer_generation)
                    .and_then(|budget| {
                        budget
                            .reserve(
                                reservation.label.clone(),
                                crate::admission::connection_budget::ChannelClass::Entity,
                            )
                            .ok()
                    });
                if budget.is_some() {
                    if let Ok(response) = response.as_mut() {
                        response.subscription_reservation = Some(reservation);
                    }
                } else {
                    let _ = state
                        .pending_runtime
                        .admission
                        .reservations
                        .forget_label(&reservation.label, peer_generation);
                    remove_entity_subscription(state, subscription_id);
                    *response = Ok(entity_subscription_error(
                        "connection_channel_limit",
                        subscription_id,
                        "the WebRTC connection channel budget rejected the reservation",
                    ));
                }
            }
            Err(_) => {
                remove_entity_subscription(state, subscription_id);
                *response = Ok(entity_subscription_error(
                    "reservation_label_conflict",
                    subscription_id,
                    "a live entity reservation already exists for this route",
                ));
            }
        }
    }
    let response = std::mem::replace(response, Err(DaemonTransportError::UnexpectedResponse));
    let _ = match plugin_result_charge {
        Some(charge) => reply_tx.send_retained(RetainedPluginResult::new(response, charge)),
        None => reply_tx.send(response),
    };
}

fn unsubscribe(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    subscription_id: String,
    reply_tx: Option<ControlReplySender>,
    grant_id: Option<String>,
) -> bool {
    retire_plugin_entity_subscription(daemon, state, &subscription_id, grant_id.as_deref());
    let peer_generation = grant_id.as_deref().and_then(|grant_id| {
        state
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
            })
    });
    if let Some(grant_id) = grant_id.as_deref()
        && !daemon.local_webrtc().has_live_peer(grant_id)
    {
        // Peer already gone: owner-checked residual cleanup only. Never delete a row now
        // owned by a different live grant (subscription-id reuse after PeerClosed).
        let should_remove = match state.entity_subscriptions.get(&subscription_id) {
            None => false,
            Some(subscription) => match subscription.owner_grant_id.as_deref() {
                None => true,
                Some(owner) => owner == grant_id,
            },
        };
        if should_remove
            && state
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
        if let Some(reply_tx) = reply_tx {
            // Idempotent unsubscribed reply for the stale client even when the row is
            // preserved for a replacement owner.
            let _ = reply_tx.send(Ok(daemon_response_base(
                DaemonResponseKind::EntityUnsubscribed,
            )));
        }
        return false;
    }
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
    if let Some(peer_generation) = peer_generation {
        let labels = state
            .pending_runtime
            .admission
            .reservations
            .forget_subscription(
                crate::admission::connection_budget::ChannelClass::Entity,
                &subscription_id,
                peer_generation,
            );
        if let Some(budget) = state
            .pending_runtime
            .admission
            .connection_budgets
            .get_mut(&peer_generation)
        {
            for label in labels {
                let _ = budget.release(&label);
            }
        }
    }
    if let Some(reply_tx) = reply_tx {
        let _ = reply_tx.send(Ok(daemon_response_base(
            DaemonResponseKind::EntityUnsubscribed,
        )));
    }
    false
}

pub(crate) fn remove_entity_subscription(state: &mut DaemonControlState, subscription_id: &str) {
    if state.entity_subscriptions.remove(subscription_id).is_some() {
        state.lifecycle_counters.live_entity_subscriptions = state
            .lifecycle_counters
            .live_entity_subscriptions
            .saturating_sub(1);
        state.released_entity_generations = state.released_entity_generations.saturating_add(1);
    }
}

pub(crate) fn reject_json_request(request: DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::SubscribeEntities { .. } | DaemonRequest::UnsubscribeEntities { .. } => {
            Err(DaemonTransportError::Protocol(
                "entity subscriptions require the held-open stream handler",
            ))
        }
        _ => unreachable!("entity family received a non-entity request"),
    }
}
