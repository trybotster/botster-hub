//! Entity subscription control-message family.

mod worker;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
    EntityFrameSender, entity_subscription_error, register_builtin_entity_subscription,
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
}

enum PendingPluginEntityKind {
    Subscribe(PendingEntitySubscribe),
    Resync {
        entity_type: String,
        permit: OwnerPermit,
    },
    Fanout {
        permit: OwnerPermit,
    },
}

struct PendingPluginEntity {
    request_id: String,
    waiter_id: crate::owner_identity::WaiterId,
    ready_key: Option<crate::daemon::owner_schedule::ReadyKey>,
    deadline_key: Option<crate::daemon::owner_schedule::DeadlineKey>,
    identity: Option<PluginEntityIdentity>,
    invocation: Option<PluginEntitySnapshotInvocation>,
    kind: PendingPluginEntityKind,
    result: Option<RoutedPluginEntityCompletion>,
    work: worker::EntityWork,
}

enum RoutedPluginEntityCompletion {
    Invocation(RetainedPluginResult<PluginInvocationResult>),
    Inconsistent(RetainedPluginResult<PluginInvocationResult>),
}

/// Bounded owner-side state for asynchronous entity-provider calls.
#[derive(Default)]
pub(crate) struct PluginEntityState {
    pub(crate) targets: BTreeMap<String, std::sync::Arc<crate::plugin_entity::Target>>,
    next_serial: u64,
    pending: BTreeMap<String, PendingPluginEntity>,
    by_waiter: BTreeMap<crate::owner_identity::WaiterId, String>,
    ready: VecDeque<crate::owner_identity::WaiterId>,
    completion_inconsistencies: u64,
    capacity_waiters: BTreeSet<crate::owner_identity::WaiterId>,
    delivery_waiters: BTreeSet<crate::owner_identity::WaiterId>,
    active_delivery: Option<crate::owner_identity::WaiterId>,
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
                entry
                    .identity
                    .as_ref()
                    .is_some_and(|identity| identity.connection_generation == generation)
                    && matches!(entry.kind, PendingPluginEntityKind::Subscribe(_))
            })
            .count()
            < botster_hub_client::MAX_OUTSTANDING_REQUESTS
    }

    fn insert(
        &mut self,
        waiter_id: crate::owner_identity::WaiterId,
        invocation: PluginEntitySnapshotInvocation,
        identity: PluginEntityIdentity,
        mut kind: PendingPluginEntityKind,
    ) {
        let request_id = invocation.request.request_id.0.clone();
        let target = match &mut kind {
            PendingPluginEntityKind::Subscribe(subscribe) => {
                Some(std::sync::Arc::new(crate::plugin_entity::Target {
                    subscription_id: std::mem::take(&mut subscribe.request.subscription_id),
                    entity_type: std::mem::take(&mut subscribe.request.entity_type),
                    sender: subscribe.request.frame_tx.clone(),
                }))
            }
            _ => None,
        };
        let mut work = worker::EntityWork::new(target);
        work.family_generation = Some(invocation.family_generation);
        work.family = Some(std::sync::Arc::new(
            invocation.expected_entity_kind().as_str().to_string(),
        ));
        self.by_waiter.insert(waiter_id, request_id.clone());
        self.pending.insert(
            request_id.clone(),
            PendingPluginEntity {
                request_id,
                waiter_id,
                ready_key: None,
                deadline_key: None,
                identity: Some(identity),
                invocation: Some(invocation),
                kind,
                result: None,
                work,
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
        let mut became_ready = false;
        if completion.value().class != PluginInvocationClass::RequestResponse
            || !entry.identity.as_ref().is_some_and(|identity| {
                identity.plugin_key == handler.plugin_key.0 && identity.handler == handler
            })
        {
            self.completion_inconsistencies = self.completion_inconsistencies.saturating_add(1);
            if entry.result.is_none() {
                entry.result = Some(RoutedPluginEntityCompletion::Inconsistent(
                    completion.map(|completion| completion.result),
                ));
                became_ready = true;
            }
        } else if entry.result.is_none() {
            entry.result = Some(RoutedPluginEntityCompletion::Invocation(
                completion.map(|completion| completion.result),
            ));
            became_ready = true;
        }
        if became_ready {
            self.ready.push_back(entry.waiter_id);
        }
        None
    }

    pub(crate) fn take_ready_waiters(
        &mut self,
        max_items: usize,
    ) -> Vec<crate::owner_identity::WaiterId> {
        self.ready
            .drain(..self.ready.len().min(max_items))
            .collect()
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

    #[cfg(test)]
    pub(crate) fn has_retained_snapshot_payload(&self) -> bool {
        self.pending.values().any(|entry| {
            entry
                .work
                .payload
                .as_ref()
                .is_some_and(|payload| payload.sequence().is_some())
        })
    }

    fn take_waiter(
        &mut self,
        waiter_id: crate::owner_identity::WaiterId,
    ) -> Option<PendingPluginEntity> {
        let request_id = self.by_waiter.remove(&waiter_id)?;
        let entry = self.pending.remove(&request_id)?;
        self.ready.retain(|ready| *ready != waiter_id);
        Some(entry)
    }

    fn entry_for_waiter_mut(
        &mut self,
        waiter_id: crate::owner_identity::WaiterId,
    ) -> Option<&mut PendingPluginEntity> {
        let request_id = self.by_waiter.get(&waiter_id)?.clone();
        self.pending.get_mut(&request_id)
    }

    fn restore(&mut self, entry: PendingPluginEntity) {
        self.by_waiter
            .insert(entry.waiter_id, entry.request_id.clone());
        self.pending.insert(entry.request_id.clone(), entry);
    }

    pub(crate) fn accepts_host_completion(
        &self,
        identity: crate::host_executor::HostJobIdentity,
    ) -> bool {
        self.by_waiter
            .get(&identity.waiter_id)
            .and_then(|request| self.pending.get(request))
            .is_some_and(|entry| entry.work.accepts(identity))
    }

    pub(crate) fn retain_host_completion(
        &mut self,
        completion: crate::host_executor::HostCompletion,
    ) {
        let entry = self
            .entry_for_waiter_mut(completion.identity.waiter_id)
            .expect("an accepted entity completion has a pending row");
        entry
            .work
            .retain_completion(completion)
            .expect("the entity completion matches its retained phase");
    }

    pub(crate) fn has_capacity_waiters(&self) -> bool {
        !self.capacity_waiters.is_empty()
    }

    pub(crate) fn pop_capacity_waiter(&mut self) -> Option<crate::owner_identity::WaiterId> {
        self.capacity_waiters.pop_first()
    }

    pub(crate) fn clear_deadline(&mut self, waiter_id: crate::owner_identity::WaiterId) -> bool {
        let Some(entry) = self.entry_for_waiter_mut(waiter_id) else {
            return false;
        };
        entry.deadline_key = None;
        entry.work.cancel();
        true
    }

    fn remove_request_id(&mut self, request_id: &str) -> Option<PendingPluginEntity> {
        let entry = self.pending.remove(request_id)?;
        self.by_waiter.remove(&entry.waiter_id);
        self.ready.retain(|ready| *ready != entry.waiter_id);
        Some(entry)
    }

    fn take_matching_subscriptions(
        &mut self,
        mut predicate: impl FnMut(
            &PendingEntitySubscribe,
            &PluginEntityIdentity,
            &crate::plugin_entity::Target,
        ) -> bool,
    ) -> Vec<PendingPluginEntity> {
        let request_ids: Vec<String> = self
            .pending
            .iter()
            .filter_map(|(request_id, entry)| match &entry.kind {
                PendingPluginEntityKind::Subscribe(subscribe)
                    if entry
                        .identity
                        .as_ref()
                        .zip(entry.work.target.as_deref())
                        .is_some_and(|(identity, target)| {
                            predicate(subscribe, identity, target)
                        }) =>
                {
                    Some(request_id.clone())
                }
                _ => None,
            })
            .collect();
        let removed = request_ids
            .into_iter()
            .filter_map(|request_id| self.remove_request_id(&request_id))
            .collect();
        removed
    }

    fn has_subscription(&self, subscription_id: &str) -> bool {
        self.pending.values().any(|entry| {
            matches!(
                &entry.kind,
                PendingPluginEntityKind::Subscribe(subscribe)
                    if entry.work.target.as_ref().is_some_and(|target| target.subscription_id == subscription_id) && !entry.work.cancelled
            )
        })
    }
}

pub(crate) fn mark_plugin_entity_ready(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
    class: crate::daemon::owner_schedule::ReadyClass,
    reason: crate::daemon::owner_schedule::ReadyReasons,
) -> bool {
    if state
        .plugin_entities
        .entry_for_waiter_mut(waiter_id)
        .is_none()
    {
        return false;
    }
    let Ok(key) = state.owner_ready.mark(waiter_id, class, reason) else {
        return false;
    };
    state
        .plugin_entities
        .entry_for_waiter_mut(waiter_id)
        .expect("a marked entity waiter must exist")
        .ready_key = Some(key);
    true
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
    frame_rx: Option<tokio_mpsc::Receiver<crate::entity_delivery::EntityDelivery>>,
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
    if state.shutdown_waiter.is_some() {
        let _ = reply_tx.send(Ok(entity_subscription_error(
            "daemon_shutting_down",
            &subscription_id,
            "the daemon is finishing accepted work before shutdown",
        )));
        return false;
    }
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
    let Some(waiter_id) = state.waiter_ids.next() else {
        state.budget.release(permit);
        let _ = request.reply_tx.send(Ok(entity_subscription_error(
            "owner_waiter_id_exhausted",
            &request.subscription_id,
            "the daemon exhausted unique owner waiter identifiers",
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
                waiter_id,
                invocation,
                identity,
                PendingPluginEntityKind::Subscribe(PendingEntitySubscribe { request, permit }),
            );
            let now = Instant::now();
            let arm = state
                .deadlines
                .arm(waiter_id, now + RETAINED_OPERATION_DEADLINE, now)
                .expect("an initial entity deadline always makes progress");
            state
                .plugin_entities
                .entry_for_waiter_mut(waiter_id)
                .expect("the entity waiter was inserted")
                .deadline_key = Some(arm.key());
            state
                .maintenance
                .wakes
                .mark(crate::daemon_maintenance::MaintenanceSliceKind::CompletionDrain);
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
    if state.shutdown_waiter.is_some() || state.plugin_entities.has_resync(&entity_type) {
        return;
    }
    let Some(permit) = state.budget.reserve() else {
        return;
    };
    let Some(waiter_id) = state.waiter_ids.next() else {
        state.budget.release(permit);
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
                waiter_id,
                invocation,
                identity,
                PendingPluginEntityKind::Resync {
                    entity_type,
                    permit,
                },
            );
            let now = Instant::now();
            let arm = state
                .deadlines
                .arm(waiter_id, now + RETAINED_OPERATION_DEADLINE, now)
                .expect("an initial entity deadline always makes progress");
            state
                .plugin_entities
                .entry_for_waiter_mut(waiter_id)
                .expect("the entity waiter was inserted")
                .deadline_key = Some(arm.key());
            state
                .maintenance
                .wakes
                .mark(crate::daemon_maintenance::MaintenanceSliceKind::CompletionDrain);
        }
        _ => {
            runtime.retire_plugin_entity_snapshot(&invocation);
            state.budget.release(permit);
        }
    }
}

pub(crate) fn begin_package_entity_fanout(daemon: &HubDaemon, state: &mut DaemonControlState) {
    if state.shutdown_waiter.is_some()
        || state.plugin_entities.active_delivery.is_some()
        || !daemon
            .runtime()
            .is_some_and(|runtime| runtime.has_package_entity_fanout())
    {
        return;
    }
    let Some(permit) = state.budget.reserve() else {
        return;
    };
    let Some(waiter_id) = state.waiter_ids.next() else {
        state.budget.release(permit);
        return;
    };
    let Some(request_id) = state.plugin_entities.next_request_id() else {
        state.budget.release(permit);
        return;
    };
    let mut work = worker::EntityWork::new(None);
    work.snapshot = false;
    work.stage = worker::Stage::Begin;
    state.plugin_entities.restore(PendingPluginEntity {
        request_id: request_id.0,
        waiter_id,
        ready_key: None,
        deadline_key: None,
        identity: None,
        invocation: None,
        kind: PendingPluginEntityKind::Fanout { permit },
        result: None,
        work,
    });
    state.plugin_entities.active_delivery = Some(waiter_id);
    mark_plugin_entity_ready(
        state,
        waiter_id,
        crate::daemon::owner_schedule::ReadyClass::HostCompletion,
        crate::daemon::control::pending::READY_HOST_COMPLETION,
    );
}

/// Apply one exact entity-provider completion or deadline.
pub(crate) fn drive_plugin_entity_ready_item(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    item: crate::daemon::owner_schedule::ReadyItem,
) -> bool {
    let waiter_id = item.key().waiter_id();
    let Some(mut entry) = state.plugin_entities.take_waiter(waiter_id) else {
        return false;
    };
    entry.ready_key = None;
    let step = worker::step(daemon, state, &mut entry);
    match step {
        worker::Step::Done => {
            if entry.work.snapshot
                && let Some(runtime) = daemon.runtime()
            {
                runtime.note_package_entity_resync_changed();
            }
            state.deadlines.retire(waiter_id);
            state.plugin_entities.capacity_waiters.remove(&waiter_id);
            state.plugin_entities.delivery_waiters.remove(&waiter_id);
            if state.plugin_entities.active_delivery == Some(waiter_id) {
                state.plugin_entities.active_delivery = None;
                if let Some(waiter) = state.plugin_entities.delivery_waiters.pop_first() {
                    mark_plugin_entity_ready(
                        state,
                        waiter,
                        crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                        crate::daemon::control::pending::READY_HOST_COMPLETION,
                    );
                }
                state
                    .maintenance
                    .wakes
                    .mark(crate::daemon_maintenance::MaintenanceSliceKind::ProviderResync);
            }
            let permit = match entry.kind {
                PendingPluginEntityKind::Subscribe(subscribe) => subscribe.permit,
                PendingPluginEntityKind::Resync { permit, .. }
                | PendingPluginEntityKind::Fanout { permit } => permit,
            };
            state.budget.release(permit);
            crate::daemon::control::pending::wake_shutdown_waiter(state);
        }
        step => {
            state.plugin_entities.restore(entry);
            if matches!(step, worker::Step::Again) {
                mark_plugin_entity_ready(
                    state,
                    waiter_id,
                    crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                    crate::daemon::control::pending::READY_HOST_COMPLETION,
                );
            }
        }
    }
    true
}

/// Cancel one retained row. The row keeps its payload and permit until its worker finishes.
pub(crate) fn cancel_next_plugin_entity_for_shutdown(
    state: &mut DaemonControlState,
    after: &mut Option<crate::owner_identity::WaiterId>,
) -> bool {
    use std::ops::Bound::{Excluded, Unbounded};

    let next = match *after {
        Some(after) => state
            .plugin_entities
            .by_waiter
            .range((Excluded(after), Unbounded))
            .next(),
        None => state.plugin_entities.by_waiter.first_key_value(),
    }
    .map(|(waiter, _)| *waiter);
    let Some(waiter) = next else {
        return false;
    };
    *after = Some(waiter);
    let entry = state
        .plugin_entities
        .entry_for_waiter_mut(waiter)
        .expect("the entity waiter index retains its row");
    entry.work.cancel();
    entry.deadline_key = None;
    state.deadlines.retire(waiter);
    mark_plugin_entity_ready(
        state,
        waiter,
        crate::daemon::owner_schedule::ReadyClass::HostCompletion,
        crate::daemon::control::pending::READY_HOST_COMPLETION,
    );
    true
}

pub(crate) fn plugin_entity_cleanup_pending(state: &DaemonControlState) -> bool {
    !state.plugin_entities.pending.is_empty()
}

/// Retire pending entity-provider replies owned by one closed connection.
pub(crate) fn retire_plugin_entity_connection(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    connection_id: &str,
) {
    let retired = state
        .plugin_entities
        .take_matching_subscriptions(|_, identity, _| identity.connection_id == connection_id);
    retire_plugin_entity_entries(daemon, state, retired, true);
}

fn retire_plugin_entity_entries(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    entries: Vec<PendingPluginEntity>,
    count_abandoned: bool,
) {
    for mut entry in entries {
        if let Some(key) = entry.ready_key.take() {
            state.owner_ready.remove(key);
        }
        state.deadlines.retire(entry.waiter_id);
        entry.work.cancel();
        let waiter_id = entry.waiter_id;
        state.plugin_entities.restore(entry);
        mark_plugin_entity_ready(
            state,
            waiter_id,
            crate::daemon::owner_schedule::ReadyClass::HostCompletion,
            crate::daemon::control::pending::READY_HOST_COMPLETION,
        );
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
        .take_matching_subscriptions(|subscribe, _, target| {
            target.subscription_id == subscription_id
                && owner.is_none_or(|owner| subscribe.request.grant_id.as_deref() == Some(owner))
        });
    retire_plugin_entity_entries(daemon, state, retired, true);
}

fn prepare_package_entity_reservation(
    state: &mut DaemonControlState,
    subscription_id: &str,
    frame_rx: Option<tokio_mpsc::Receiver<crate::entity_delivery::EntityDelivery>>,
    grant_id: Option<&str>,
    identity: &mut Option<crate::admission::reservations::PreparedSubscriptionIdentity>,
    error: &mut Option<(&'static str, &'static str)>,
) -> Option<botster_hub_client::DaemonSubscriptionReservation> {
    let (Some(grant_id), Some(frame_rx)) = (grant_id, frame_rx) else {
        return None;
    };
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
        *error = Some((
            "local_webrtc_peer_gone",
            "local WebRTC peer admission is no longer live",
        ));
        return None;
    };
    let Some(generation) = state
        .pending_runtime
        .admission
        .next_subscription_generation
        .checked_add(1)
    else {
        *error = Some((
            "subscription_generation_exhausted",
            "the daemon exhausted subscription generations",
        ));
        return None;
    };
    state.pending_runtime.admission.next_subscription_generation = generation;
    let reserved = state
        .pending_runtime
        .admission
        .reservations
        .reserve_subscription_prepared(
            crate::admission::connection_budget::ChannelClass::Entity,
            identity
                .take()
                .expect("a registered package subscription retains its prepared identity"),
            generation,
            peer_generation,
            crate::admission::reservations::now_seconds(),
            crate::admission::reservations::ReservationBinding::Entity {
                receiver: std::sync::Arc::new(std::sync::Mutex::new(Some(frame_rx))),
            },
        );
    let reservation = match reserved {
        Ok(reservation) => reservation,
        Err(_) => {
            *error = Some((
                "reservation_label_conflict",
                "a live entity reservation already exists for this route",
            ));
            return None;
        }
    };
    let reserved_budget = state
        .pending_runtime
        .admission
        .connection_budgets
        .get_mut(&peer_generation)
        .is_some_and(|budget| {
            budget
                .reserve(
                    reservation.label.clone(),
                    crate::admission::connection_budget::ChannelClass::Entity,
                )
                .is_ok()
        });
    if reserved_budget
        && crate::daemon::owner_loop::arm_reservation_deadline(
            state,
            reservation.label.clone(),
            peer_generation,
            reservation.expires_in_seconds,
        )
    {
        return Some(reservation);
    }
    if let Some(budget) = state
        .pending_runtime
        .admission
        .connection_budgets
        .get_mut(&peer_generation)
    {
        let _ = budget.release(&reservation.label);
    }
    let _ = state
        .pending_runtime
        .admission
        .reservations
        .forget_label(&reservation.label, peer_generation);
    remove_entity_subscription(state, subscription_id);
    *error = Some((
        "connection_channel_limit",
        "the WebRTC connection channel budget rejected the reservation",
    ));
    None
}

fn finish_entity_subscribe_response(
    state: &mut DaemonControlState,
    subscription_id: &str,
    frame_rx: Option<tokio_mpsc::Receiver<crate::entity_delivery::EntityDelivery>>,
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
                if budget.is_some()
                    && crate::daemon::owner_loop::arm_reservation_deadline(
                        state,
                        reservation.label.clone(),
                        peer_generation,
                        reservation.expires_in_seconds,
                    )
                {
                    if let Ok(response) = response.as_mut() {
                        response.subscription_reservation = Some(reservation);
                    }
                } else {
                    if let Some(budget) = state
                        .pending_runtime
                        .admission
                        .connection_budgets
                        .get_mut(&peer_generation)
                    {
                        let _ = budget.release(&reservation.label);
                    }
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
        if should_remove {
            remove_entity_subscription(state, &subscription_id);
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
    remove_entity_subscription(state, &subscription_id);
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
        crate::daemon::owner_loop::retire_reservation_deadlines(state, labels.iter().cloned());
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
    state.plugin_entities.targets.remove(subscription_id);
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
