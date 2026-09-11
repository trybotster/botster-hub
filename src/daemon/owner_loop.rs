//! Hub owner thread.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use botster_hub_client::{
    DaemonDiagnostic, DaemonLifecycleCounters, DaemonRequest, DaemonResponse, DaemonResponseKind,
};
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use tokio::net::UnixListener as TokioUnixListener;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::{Semaphore, watch};

use crate::HubConfig;
use crate::HubDaemon;
use crate::HubDaemonStatus;
use crate::admission::budgets::{
    DAEMON_CLIENT_WRITE_TIMEOUT, DAEMON_CONTROL_QUEUE_CAPACITY, DAEMON_MAX_CONNECTIONS,
};
use crate::admission::unix_hello::{AdmissionState, WebrtcTerminalAdmission};
use crate::daemon::control::dispatch_control_message;
#[cfg(test)]
use crate::daemon::control::handle_control_message;
use crate::daemon::control::message::{
    ControlMessage, ControlReplySender, ControlSender, DaemonDeliveryKind, EgressWriteClass,
};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon_maintenance::{
    MaintenanceSliceKind, MaintenanceState, OBSERVE_SLICE_BUDGET, PUMP_MAX_ROUTES_VALIDATED,
    PumpState, run_completion_drain_slice_for_owner, run_maintenance_kind_for_owner,
};
use crate::subscription::attach_routes::{
    AttachStreamRegistry, AttachedSubscription, AttachedSubscriptionChange,
    record_attached_subscription_change,
};
use crate::subscription::entity::{
    EntitySubscriptionState, drive_entity_subscriptions, drive_package_entity_fanout,
    seed_lifecycle_reconciliation,
};
use crate::subscription::entity_resync::drive_package_entity_resync;
use crate::transport::unix::connection::{
    handle_connection_async, handle_connection_cleanup, reap_finished_connection_tasks,
    wait_for_connection_tasks,
};
use crate::transport::unix::listener::{
    accept_connections, acquire_socket_owner_lock, cleanup_socket_path, prepare_socket_path,
    socket_path,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BackgroundWork {
    DataPlaneProgress,
    CoreCompletion,
    HostCompletion,
    CausalProgress,
    CausalDrain,
    EntityPublish,
    Coordination,
    EventOwner,
    ManagedSpawn,
    PluginReady,
    PluginEntityReady,
    Deadline,
    Maintenance(MaintenanceSliceKind),
    PumpObserve,
    InventoryReconcile,
}

fn causal_waiter_upper_bound(
    state: &DaemonControlState,
) -> Option<crate::owner_identity::WaiterId> {
    state
        .family_cleanup_waiters
        .last_key_value()
        .map(|(id, _)| *id)
        .into_iter()
        .chain(state.plugin_entities.causal_waiters.last().copied())
        .chain(state.plugin_entities.model_waiters.last().copied())
        .max()
}

#[derive(Debug, Clone, Copy)]
struct BackgroundWaiter {
    work: BackgroundWork,
    last_phase: u64,
}

#[derive(Debug)]
struct ReservationDeadline {
    label: String,
    peer_generation: u64,
    ready_key: Option<crate::daemon::owner_schedule::ReadyKey>,
}

pub(crate) fn arm_reservation_deadline(
    state: &mut DaemonControlState,
    label: String,
    peer_generation: u64,
    expires_in_seconds: u32,
) -> bool {
    let Some(waiter_id) = state.waiter_ids.next() else {
        return false;
    };
    let now = Instant::now();
    let deadline = now + Duration::from_secs(u64::from(expires_in_seconds));
    let Ok(arm) = state.deadlines.arm(waiter_id, deadline, now) else {
        return false;
    };
    state
        .reservation_waiters_by_label
        .insert(label.clone(), waiter_id);
    state.reservation_deadlines.insert(
        waiter_id,
        ReservationDeadline {
            label,
            peer_generation,
            ready_key: None,
        },
    );
    true
}

pub(crate) fn retire_reservation_deadline(state: &mut DaemonControlState, label: &str) {
    let Some(waiter_id) = state.reservation_waiters_by_label.remove(label) else {
        return;
    };
    if let Some(deadline) = state.reservation_deadlines.remove(&waiter_id)
        && let Some(key) = deadline.ready_key
    {
        state.owner_ready.remove(key);
    }
    state.deadlines.retire(waiter_id);
}

pub(crate) fn retire_reservation_deadlines(
    state: &mut DaemonControlState,
    labels: impl IntoIterator<Item = String>,
) {
    for label in labels {
        retire_reservation_deadline(state, &label);
    }
}

pub(crate) fn mark_reservation_deadline_ready(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
) -> bool {
    if !state.reservation_deadlines.contains_key(&waiter_id) {
        return false;
    }
    let Ok(key) = state.owner_ready.mark(
        waiter_id,
        crate::daemon::owner_schedule::ReadyClass::Deadline,
        crate::daemon::control::pending::READY_DEADLINE,
    ) else {
        return true;
    };
    state
        .reservation_deadlines
        .get_mut(&waiter_id)
        .expect("a reservation deadline remains registered")
        .ready_key = Some(key);
    true
}

fn run_reservation_deadline_item(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    item: crate::daemon::owner_schedule::ReadyItem,
) -> bool {
    let waiter_id = item.key().waiter_id();
    let Some(deadline) = state.reservation_deadlines.remove(&waiter_id) else {
        return false;
    };
    state.reservation_waiters_by_label.remove(&deadline.label);
    state.deadlines.retire(waiter_id);
    let Some(grant_id) = state
        .pending_runtime
        .admission
        .grant_by_peer_generation
        .get(&deadline.peer_generation)
        .cloned()
    else {
        return true;
    };
    crate::daemon::control::connection::emit_reservation_expired(
        daemon,
        state,
        &grant_id,
        deadline.peer_generation,
        &deadline.label,
        crate::admission::reservations::now_seconds(),
    );
    true
}

fn background_waiter_id(
    state: &mut DaemonControlState,
    work: BackgroundWork,
) -> Option<crate::owner_identity::WaiterId> {
    if let Some(waiter_id) = state.background_waiter_ids.get(&work) {
        return Some(*waiter_id);
    }
    let waiter_id = state.waiter_ids.next()?;
    state.background_waiter_ids.insert(work, waiter_id);
    state.background_core_waiters.insert(
        waiter_id,
        BackgroundWaiter {
            work,
            last_phase: 0,
        },
    );
    Some(waiter_id)
}

fn maintenance_core_work(kind: MaintenanceSliceKind) -> Option<BackgroundWork> {
    match kind {
        MaintenanceSliceKind::Observe
        | MaintenanceSliceKind::JournalPull
        | MaintenanceSliceKind::Baseline => Some(BackgroundWork::Maintenance(kind)),
        _ => None,
    }
}

fn background_ready_class(work: BackgroundWork) -> crate::daemon::owner_schedule::ReadyClass {
    use crate::daemon::owner_schedule::ReadyClass;

    match work {
        BackgroundWork::CoreCompletion | BackgroundWork::DataPlaneProgress => {
            ReadyClass::CoreCompletion
        }
        BackgroundWork::HostCompletion
        | BackgroundWork::CausalProgress
        | BackgroundWork::ManagedSpawn => ReadyClass::HostCompletion,
        BackgroundWork::EventOwner
        | BackgroundWork::Coordination
        | BackgroundWork::EntityPublish
        | BackgroundWork::CausalDrain => ReadyClass::HostBridge,
        BackgroundWork::PluginReady | BackgroundWork::PluginEntityReady => {
            ReadyClass::PluginCompletion
        }
        BackgroundWork::Deadline => ReadyClass::Deadline,
        BackgroundWork::Maintenance(MaintenanceSliceKind::Observe)
        | BackgroundWork::PumpObserve => ReadyClass::Observe,
        BackgroundWork::InventoryReconcile => ReadyClass::InventoryReconcile,
        BackgroundWork::Maintenance(MaintenanceSliceKind::JournalPull) => ReadyClass::JournalPull,
        BackgroundWork::Maintenance(MaintenanceSliceKind::ProjectionApply) => {
            ReadyClass::ProjectionApply
        }
        BackgroundWork::Maintenance(MaintenanceSliceKind::Baseline) => ReadyClass::Baseline,
        BackgroundWork::Maintenance(MaintenanceSliceKind::HostBridge) => ReadyClass::HostBridge,
        BackgroundWork::Maintenance(MaintenanceSliceKind::SubscriberDelivery) => {
            ReadyClass::SubscriberDelivery
        }
        BackgroundWork::Maintenance(MaintenanceSliceKind::CompletionDrain) => {
            ReadyClass::PluginCompletion
        }
        BackgroundWork::Maintenance(MaintenanceSliceKind::ProviderResync) => {
            ReadyClass::ProviderResync
        }
        BackgroundWork::Maintenance(MaintenanceSliceKind::PackageEventDelivery) => {
            ReadyClass::PackageEventDelivery
        }
    }
}

fn mark_background_ready(state: &mut DaemonControlState, work: BackgroundWork) -> bool {
    let Some(waiter_id) = background_waiter_id(state, work) else {
        return false;
    };
    state
        .owner_ready
        .mark(
            waiter_id,
            background_ready_class(work),
            crate::daemon::control::pending::READY_BACKGROUND,
        )
        .is_ok()
}

fn mark_host_drain_ready(state: &mut DaemonControlState) {
    if state.host_completion_drain_faulted {
        return;
    }
    if !mark_background_ready(state, BackgroundWork::HostCompletion) {
        // The executor retains every receipt and permit when scheduling becomes impossible.
        state.host_completion_drain_faulted = true;
    }
}

/// Use the shared deadline index for the next resync policy deadline.
pub(crate) fn arm_package_entity_resync_deadline(
    state: &mut DaemonControlState,
    deadline: Option<Instant>,
) {
    if let Some(key) = state.package_entity_resync_scan.deadline_key.take() {
        state.deadlines.disarm(key);
    }
    let Some(deadline) = deadline else {
        return;
    };
    let now = Instant::now();
    if deadline <= now {
        crate::subscription::entity_resync::note_package_entity_resync_change(state);
        return;
    }
    // Production reaches this call through ProviderResync dispatch. That
    // dispatch already allocated this waiter, which remains registered.
    let waiter_id = background_waiter_id(
        state,
        BackgroundWork::Maintenance(MaintenanceSliceKind::ProviderResync),
    )
    .expect("resync dispatch retains its background waiter");
    let arm = state
        .deadlines
        .arm(waiter_id, deadline, now)
        .expect("a future resync deadline is later than every fired deadline");
    state.package_entity_resync_scan.deadline_key = Some(arm.key());
}

pub(crate) fn mark_package_entity_resync_deadline_ready(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
) -> bool {
    if state
        .background_waiter_ids
        .get(&BackgroundWork::Maintenance(
            MaintenanceSliceKind::ProviderResync,
        ))
        != Some(&waiter_id)
    {
        return false;
    }
    state.package_entity_resync_scan.deadline_key = None;
    crate::subscription::entity_resync::note_package_entity_resync_change(state);
    true
}

pub(crate) fn mark_publication_owner_ready(state: &mut DaemonControlState) {
    mark_background_ready(state, BackgroundWork::EntityPublish);
}

pub(crate) fn mark_event_owner_ready(state: &mut DaemonControlState) {
    mark_background_ready(state, BackgroundWork::EventOwner);
}

fn publish_maintenance_wakes(state: &mut DaemonControlState) {
    for kind in MaintenanceSliceKind::ALL {
        if state.maintenance.wakes.take(kind) {
            mark_background_ready(state, BackgroundWork::Maintenance(kind));
        }
    }
}

/// Read persistent notification bits before the owner can block.
/// Collectors process their payloads through the shared ready queues.
pub(crate) fn publish_completion_wakes(daemon: &HubDaemon, state: &mut DaemonControlState) {
    if state.budget.take_capacity_notification() {
        if state.coordination_waiting_for_owner && state.coordination_fault.is_none() {
            state.coordination_waiting_for_owner = false;
            if !mark_background_ready(state, BackgroundWork::Coordination) {
                state.coordination_fault = Some(
                    crate::daemon::control::coordination::CoordinationFault::SchedulerExhausted,
                );
            }
        }
        if state.event_owner.waiting_for_owner {
            mark_event_owner_ready(state);
        }
        state.publication_owner.waiting_for_owner = false;
    }
    if let Some(runtime) = daemon.runtime() {
        if runtime.take_event_plane_owner_ops_notification() {
            mark_event_owner_ready(state);
        }
        if runtime.take_package_entity_resync_notification() {
            crate::subscription::entity_resync::note_package_entity_resync_change(state);
        }
        if runtime.data_plane_progress_pending() {
            mark_background_ready(state, BackgroundWork::DataPlaneProgress);
        }
        if runtime.take_core_completion_notification() {
            mark_background_ready(state, BackgroundWork::CoreCompletion);
        }
        if state.coordination_fault.is_none()
            && runtime.coordination_bridge().take_progress_notification()
            && !mark_background_ready(state, BackgroundWork::Coordination)
        {
            state.coordination_fault =
                Some(crate::daemon::control::coordination::CoordinationFault::SchedulerExhausted);
        }
        let table_progress = runtime.causal_scopes().take_progress_notification();
        let capacity_progress = runtime.take_causal_capacity_notification();
        runtime.note_entity_publish_progress(table_progress, capacity_progress);
        if runtime.entity_publish_bridge().take_progress_notification() {
            state.publication_owner.note_progress();
            crate::daemon::control::pending::wake_shutdown_waiter(state);
        }
        let causal_progress = table_progress | capacity_progress;
        let model_progress = runtime.take_entity_model_notification();
        if causal_progress || model_progress {
            state.publication_owner.note_progress();
            state.package_entity_resync_scan.note_progress();
        }
        if state.publication_owner.ready(runtime) {
            mark_background_ready(state, BackgroundWork::EntityPublish);
        }
        if causal_progress || model_progress {
            if state.causal_wake_active {
                state.causal_wake_again = true;
            } else {
                state.causal_wake_active = true;
                state.causal_wake_after = None;
                state.causal_wake_through = causal_waiter_upper_bound(state);
            }
            state.maintenance.note_causal_capacity_progress();
            mark_background_ready(state, BackgroundWork::CausalProgress);
        }
        if state.package_entity_resync_scan.ready(runtime) {
            crate::subscription::entity_resync::mark_ready(state);
        }
        if runtime.causal_owner_ops_ready() {
            mark_background_ready(state, BackgroundWork::CausalDrain);
        }
        let executor = runtime.host_executor();
        state.host_completion_drain_pending |= executor.take_completion_notification();
        let host_capacity = executor.take_capacity_notification();
        state.host_capacity_wake_pending |= host_capacity;
        if state.host_completion_drain_pending || state.host_capacity_wake_pending {
            mark_host_drain_ready(state);
        }
        if runtime.take_managed_spawn_notification() {
            mark_background_ready(state, BackgroundWork::ManagedSpawn);
        }
    }
    let completed = state.plugin_result_budget.take_completion_notification();
    let released = state.plugin_result_budget.take_release_notification();
    if completed || released {
        mark_background_ready(
            state,
            BackgroundWork::Maintenance(MaintenanceSliceKind::CompletionDrain),
        );
    }
    if state.deadlines.has_due(Instant::now()) {
        mark_background_ready(state, BackgroundWork::Deadline);
    }
}

pub(crate) fn mark_pump_ready(state: &mut DaemonControlState) {
    mark_background_ready(state, BackgroundWork::PumpObserve);
    mark_background_ready(state, BackgroundWork::InventoryReconcile);
}

/// One Core inventory read for the reconcile phase. `read_epoch` is the
/// registry attach epoch at submission: the read's rows cover every attach
/// with epoch <= read_epoch and none newer (one owner thread submits both in
/// program order; the Core bridge is one FIFO consumed by one thread).
pub(crate) struct InventoryRead {
    read_epoch: u64,
    ticket: crate::data_plane::driver::CoreTicket<
        Vec<(
            String,
            String,
            Option<botster_core::TerminalSubscriptionGeneration>,
        )>,
    >,
}

impl DaemonControlState {
    /// Replace the result of the submitted reconcile read without changing
    /// its submission epoch. The next reconcile phase applies this inventory.
    /// Test-only: the production submission branch must create the read first.
    #[cfg(test)]
    pub(crate) fn resolve_submitted_reconcile_inventory_for_test(
        &mut self,
        inventory: Vec<botster_core::TerminalSubscriptionRecord>,
    ) {
        let read = self
            .reconcile_inventory
            .as_mut()
            .expect("the reconcile read must be submitted before its result is controlled");
        read.ticket = crate::data_plane::driver::CoreTicket::resolved(
            inventory
                .into_iter()
                .map(|row| {
                    (
                        row.session_id.0,
                        row.subscription_id.0,
                        Some(row.generation),
                    )
                })
                .collect(),
        );
    }

    pub(crate) fn note_terminal_inventory_changed(&mut self) {
        let reconcile_active =
            self.reconcile_inventory.is_some() || self.pump.reconcile_after.is_some();
        self.pump
            .note_inventory_change_during_reconcile(reconcile_active);
        mark_pump_ready(self);
    }

    pub(crate) fn absorb_background_core_completion(
        &mut self,
        identity: crate::owner_identity::OwnerWorkIdentity,
    ) -> bool {
        let Some(waiter) = self.background_core_waiters.get_mut(&identity.waiter_id) else {
            return false;
        };
        let Some(expected) = waiter.last_phase.checked_add(1) else {
            return true;
        };
        if identity.phase == expected {
            waiter.last_phase = identity.phase;
            let work = waiter.work;
            mark_background_ready(self, work);
        }
        true
    }
}

/// Earliest deadline among retained obligations and pending requests, so the
/// owner wakes to retire abandoned work even when no control traffic arrives.
fn next_owner_deadline(state: &DaemonControlState) -> Option<Instant> {
    state.deadlines.next_deadline()
}

enum OwnerEvent {
    Control(Box<Option<ControlMessage>>),
    Reconcile,
}

enum OwnerPollDecision {
    ServeControl(Box<Option<ControlMessage>>),
    RunSlice,
    Block,
}

/// Classify one busy-path owner poll. A queued control message precedes a due
/// maintenance slice so at most one already-running owner turn can precede it.
fn classify_owner_poll(
    poll: Result<ControlMessage, tokio_mpsc::error::TryRecvError>,
    slice_due: bool,
) -> OwnerPollDecision {
    match poll {
        Ok(message) => OwnerPollDecision::ServeControl(Box::new(Some(message))),
        Err(_) if slice_due => OwnerPollDecision::RunSlice,
        Err(_) => OwnerPollDecision::Block,
    }
}

async fn receive_owner_event(
    control_rx: &mut tokio_mpsc::Receiver<ControlMessage>,
    deadline: Option<Instant>,
) -> OwnerEvent {
    match deadline {
        Some(deadline) if deadline <= Instant::now() => OwnerEvent::Reconcile,
        Some(deadline) => tokio::select! {
            biased;
            message = control_rx.recv() => OwnerEvent::Control(Box::new(message)),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                OwnerEvent::Reconcile
            }
        },
        None => OwnerEvent::Control(Box::new(control_rx.recv().await)),
    }
}

fn control_ready_class(message: &ControlMessage) -> crate::daemon::owner_schedule::ReadyClass {
    use crate::daemon::owner_schedule::ReadyClass;

    match message {
        ControlMessage::ConnectionCleanup(_) => ReadyClass::Cleanup,
        ControlMessage::CoreCompletionPublished | ControlMessage::DataPlaneProgress => {
            ReadyClass::CoreCompletion
        }
        ControlMessage::HostProgressPublished
        | ControlMessage::EntityPublishProgress
        | ControlMessage::CoordinationProgress
        | ControlMessage::CausalProgressPublished
        | ControlMessage::ManagedSessionSpawnQueued => ReadyClass::HostCompletion,
        ControlMessage::PluginCompletionPublished
        | ControlMessage::PluginResultCapacityReleased => ReadyClass::PluginCompletion,
        _ => ReadyClass::ControlIngress,
    }
}

fn enqueue_control_message(
    state: &mut DaemonControlState,
    message: ControlMessage,
) -> Result<(), ControlMessage> {
    let Some(waiter_id) = state.waiter_ids.next() else {
        return Err(message);
    };
    let class = control_ready_class(&message);
    state.control_ingress.insert(waiter_id, message);
    if state
        .owner_ready
        .mark(
            waiter_id,
            class,
            crate::daemon::control::pending::READY_INITIAL,
        )
        .is_err()
    {
        return Err(state
            .control_ingress
            .remove(&waiter_id)
            .expect("a failed ingress mark retains its message"));
    }
    Ok(())
}

fn run_owner_maintenance_slice(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    kind: MaintenanceSliceKind,
) {
    let started = Instant::now();
    match kind {
        MaintenanceSliceKind::SubscriberDelivery => {
            drive_entity_subscriptions(daemon, state);
        }
        MaintenanceSliceKind::ProviderResync => {
            drive_package_entity_fanout(daemon, state);
            drive_package_entity_resync(daemon, state);
        }
        MaintenanceSliceKind::PackageEventDelivery => {
            if let Some(runtime) = daemon.runtime() {
                crate::daemon_maintenance::run_maintenance_kind(
                    runtime,
                    &mut state.maintenance,
                    &mut state.maintenance_reads,
                    MaintenanceSliceKind::PackageEventDelivery,
                );
            }
        }
        MaintenanceSliceKind::CompletionDrain => {
            if let Some(runtime) = daemon.runtime() {
                let progress = run_completion_drain_slice_for_owner(
                    runtime,
                    &mut state.maintenance,
                    &mut state.plugin_controls,
                    &mut state.plugin_entities,
                    &state.plugin_result_budget,
                );
                // A productive partial drain can continue. A zero-item drain
                // waits for either a later Core publication or a retained-byte
                // release. Core publishes each notifier after its mailbox push,
                // and Hub clears that bit before this drain. A concurrent
                // publication therefore leaves a new bit for the next turn.
                // If retained bytes blocked the head, an existing charge must
                // later drop because startup rejects any single completion
                // larger than the full retained-result capacity.
                if completion_drain_needs_followup(progress) {
                    state
                        .maintenance
                        .wakes
                        .mark(MaintenanceSliceKind::CompletionDrain);
                }
            }
        }
        other => {
            if let Some(runtime) = daemon.runtime() {
                if runtime.package_event_router().peek_delivery_wake() {
                    state.maintenance.try_wake();
                }
                if let Some(core_work) = maintenance_core_work(other) {
                    if let Some(waiter_id) = background_waiter_id(state, core_work) {
                        run_maintenance_kind_for_owner(
                            runtime,
                            &mut state.maintenance,
                            &mut state.maintenance_reads,
                            other,
                            waiter_id,
                        );
                    }
                } else {
                    crate::daemon_maintenance::run_maintenance_kind(
                        runtime,
                        &mut state.maintenance,
                        &mut state.maintenance_reads,
                        other,
                    );
                }
            }
        }
    }
    state.maintenance.last_owner_turn = started.elapsed();
    if let Some(runtime) = daemon.runtime() {
        runtime.event_plane_counters().record_owner_turn(
            u64::try_from(state.maintenance.last_owner_turn.as_micros()).unwrap_or(u64::MAX),
        );
    }
    state.lifecycle_counters.lifecycle_change_reads = state.maintenance.journal_page_reads;
    state.lifecycle_counters.lifecycle_baseline_reads = state.maintenance.baseline_page_reads;
    state.lifecycle_counters.lifecycle_resync_reads = state.maintenance.resync_reads;
}

fn completion_drain_needs_followup(
    progress: crate::daemon_maintenance::CompletionDrainProgress,
) -> bool {
    progress.item_count > 0 && progress.has_remaining
}

pub(crate) fn run_background_ready_item(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    item: crate::daemon::owner_schedule::ReadyItem,
    owner_turn: &mut crate::daemon::owner_turn::OwnerTurnBudget,
) -> bool {
    let Some(waiter) = state
        .background_core_waiters
        .get(&item.key().waiter_id())
        .copied()
    else {
        return false;
    };
    match waiter.work {
        BackgroundWork::DataPlaneProgress => {
            crate::daemon::control::record_data_plane_progress(daemon, state);
        }
        BackgroundWork::CoreCompletion => {
            if let Some(runtime) = daemon.runtime() {
                let identities = runtime.take_owner_core_completions(1);
                let consumed = crate::daemon::control::pending::absorb_core_completions(
                    state,
                    &identities,
                    owner_turn,
                );
                runtime.restore_owner_core_completions(&identities[consumed..]);
                runtime.reap_detached_core_operations();
                if !identities.is_empty() {
                    mark_background_ready(state, BackgroundWork::CoreCompletion);
                }
            }
        }
        BackgroundWork::HostCompletion => {
            crate::subscription::entity::absorb_session_type_catalog_completions(
                daemon, state, owner_turn,
            );
            if state.host_completion_drain_pending || state.host_capacity_wake_pending {
                mark_host_drain_ready(state);
            }
        }
        BackgroundWork::EntityPublish => {
            if crate::daemon::publication_owner::drive(daemon, state) {
                mark_background_ready(state, BackgroundWork::EntityPublish);
            }
        }
        BackgroundWork::Coordination => {
            crate::daemon::control::coordination::accept_one(daemon, state);
        }
        BackgroundWork::CausalDrain => {
            if let Some(runtime) = daemon.runtime() {
                // Charge the supplied operation. Table traversal has separate costs.
                if owner_turn
                    .try_charge(
                        Instant::now(),
                        crate::daemon::owner_turn::OwnerTurnCharge::inspection(
                            std::mem::size_of::<crate::package_event_router::CausalOp>(),
                        ),
                    )
                    .is_err()
                {
                    mark_background_ready(state, BackgroundWork::CausalDrain);
                    return true;
                }
                runtime.apply_causal_owner_ops();
                if runtime.causal_owner_ops_ready() {
                    mark_background_ready(state, BackgroundWork::CausalDrain);
                }
            }
        }
        BackgroundWork::CausalProgress => {
            use std::ops::Bound::{Excluded, Included, Unbounded};
            let next = state.causal_wake_through.and_then(|through| {
                let lower = state.causal_wake_after.map_or(Unbounded, Excluded);
                let family = state
                    .family_cleanup_waiters
                    .range((lower, Included(through)))
                    .next()
                    .map(|(id, _)| *id);
                let entity = state
                    .plugin_entities
                    .causal_waiters
                    .range((lower, Included(through)))
                    .next()
                    .copied();
                let model = state
                    .plugin_entities
                    .model_waiters
                    .range((lower, Included(through)))
                    .next()
                    .copied();
                family.into_iter().chain(entity).chain(model).min()
            });
            if let Some(waiter) = next {
                state.causal_wake_after = Some(waiter);
                if state.plugin_entities.causal_waiters.contains(&waiter)
                    || state.plugin_entities.model_waiters.contains(&waiter)
                {
                    let marked = crate::daemon::control::entities::mark_plugin_entity_ready(
                        state,
                        waiter,
                        crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                        crate::daemon::control::pending::READY_HOST_COMPLETION,
                    );
                    if marked || !state.plugin_entities.has_waiter(waiter) {
                        state.plugin_entities.causal_waiters.remove(&waiter);
                        state.plugin_entities.model_waiters.remove(&waiter);
                    }
                } else {
                    let marked = crate::daemon::control::pending::mark_owner_ready(
                        state,
                        waiter,
                        crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                        crate::daemon::control::pending::READY_HOST_COMPLETION,
                    );
                    if marked || !state.pending_requests.contains_key(&waiter) {
                        state.family_cleanup_waiters.remove(&waiter);
                    }
                }
                mark_background_ready(state, BackgroundWork::CausalProgress);
            } else if state.causal_wake_again {
                state.causal_wake_again = false;
                state.causal_wake_after = None;
                state.causal_wake_through = causal_waiter_upper_bound(state);
                mark_background_ready(state, BackgroundWork::CausalProgress);
            } else {
                state.causal_wake_active = false;
            }
        }
        BackgroundWork::EventOwner => {
            if crate::daemon::event_owner::drive(daemon, state) {
                mark_event_owner_ready(state);
            }
        }
        BackgroundWork::ManagedSpawn => {
            crate::daemon::control::managed_git::accept_one(daemon, state);
        }
        BackgroundWork::PluginReady => {
            if let Some(waiter_id) = state.plugin_controls.take_ready_waiters(1).pop() {
                crate::daemon::control::pending::mark_owner_ready(
                    state,
                    waiter_id,
                    crate::daemon::owner_schedule::ReadyClass::PluginCompletion,
                    crate::daemon::control::pending::READY_PLUGIN_COMPLETION,
                );
                mark_background_ready(state, BackgroundWork::PluginReady);
            }
        }
        BackgroundWork::PluginEntityReady => {
            if let Some(waiter_id) = state.plugin_entities.take_ready_waiters(1).pop() {
                crate::daemon::control::entities::mark_plugin_entity_ready(
                    state,
                    waiter_id,
                    crate::daemon::owner_schedule::ReadyClass::PluginCompletion,
                    crate::daemon::control::pending::READY_PLUGIN_COMPLETION,
                );
                mark_background_ready(state, BackgroundWork::PluginEntityReady);
            }
        }
        BackgroundWork::Deadline => {
            crate::daemon::control::pending::mark_due_owner_deadlines(
                state,
                Instant::now(),
                owner_turn,
            );
            if state.deadlines.has_due(Instant::now()) {
                mark_background_ready(state, BackgroundWork::Deadline);
            }
        }
        BackgroundWork::Maintenance(kind) => {
            state.lifecycle_counters.reconciliation_wakes = state
                .lifecycle_counters
                .reconciliation_wakes
                .saturating_add(1);
            run_owner_maintenance_slice(daemon, state, kind);
            if kind == MaintenanceSliceKind::CompletionDrain {
                mark_background_ready(state, BackgroundWork::PluginReady);
                mark_background_ready(state, BackgroundWork::PluginEntityReady);
            }
        }
        BackgroundWork::PumpObserve => run_pump_observe_slice(daemon, state),
        BackgroundWork::InventoryReconcile => run_inventory_reconcile_slice(daemon, state),
    }
    publish_maintenance_wakes(state);
    true
}

/// Dispatch one ready item through the production owner handlers.
pub(crate) fn dispatch_owner_ready_item(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    item: crate::daemon::owner_schedule::ReadyItem,
    owner_turn: &mut crate::daemon::owner_turn::OwnerTurnBudget,
) -> bool {
    let handled = crate::daemon::client_events::drive_ready(daemon, state, item)
        || crate::subscription::entity::drive_session_type_catalog_ready_item(daemon, state, item)
        || run_reservation_deadline_item(daemon, state, item)
        || run_background_ready_item(daemon, state, item, owner_turn)
        || crate::daemon::control::entities::drive_plugin_entity_ready_item(daemon, state, item)
        || crate::daemon::owner_budget::poll_owner_obligation_item(daemon, state, item);
    let shutdown = !handled && crate::daemon::control::request::poll_one_ready(daemon, state, item);
    publish_maintenance_wakes(state);
    shutdown
}

/// Run a bounded test turn with the production wake and dispatch paths.
#[cfg(test)]
pub(crate) fn drive_ready_test_turn(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
) -> bool {
    let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
    publish_completion_wakes(daemon, state);
    publish_maintenance_wakes(state);
    while budget
        .try_charge(
            Instant::now(),
            crate::daemon::owner_turn::OwnerTurnCharge::opaque_move(),
        )
        .is_ok()
    {
        let Some(item) = state.owner_ready.pop_next() else {
            break;
        };
        if dispatch_owner_ready_item(daemon, state, item, &mut budget) {
            return true;
        }
    }
    false
}

fn run_control_ingress_item(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    transport_runtime: &tokio::runtime::Runtime,
    control_tx: ControlSender,
    shutdown_tx: &watch::Sender<bool>,
    connection_tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    item: crate::daemon::owner_schedule::ReadyItem,
) -> Option<bool> {
    let message = state.control_ingress.remove(&item.key().waiter_id())?;
    match message {
        ControlMessage::AcceptedConnection {
            stream,
            admission_permit,
            cleanup_permit,
        } => {
            let Some(connection_permit) = state.budget.reserve_connection() else {
                state.lifecycle_counters.rejected_connections = state
                    .lifecycle_counters
                    .rejected_connections
                    .saturating_add(1);
                *state
                    .lifecycle_counters
                    .cleanup_by_reason
                    .entry("owner_budget_refused_connection".to_string())
                    .or_insert(0) += 1;
                drop(stream);
                drop(admission_permit);
                drop(cleanup_permit);
                return Some(false);
            };
            state.lifecycle_counters.accepted_connections = state
                .lifecycle_counters
                .accepted_connections
                .saturating_add(1);
            let tx = control_tx.clone();
            let shutdown = shutdown_tx.subscribe();
            state.lifecycle_counters.live_connections =
                state.lifecycle_counters.live_connections.saturating_add(1);
            state.lifecycle_counters.high_water_live_connections = state
                .lifecycle_counters
                .high_water_live_connections
                .max(state.lifecycle_counters.live_connections);
            connection_tasks.push(transport_runtime.spawn(async move {
                let _admission_permit = admission_permit;
                if let Err(error) =
                    handle_connection_async(stream, tx, cleanup_permit, shutdown, connection_permit)
                        .await
                {
                    eprintln!("botster-hub daemon connection error: {error}");
                }
            }));
            Some(false)
        }
        ControlMessage::RejectedConnection => {
            state.lifecycle_counters.rejected_connections = state
                .lifecycle_counters
                .rejected_connections
                .saturating_add(1);
            Some(false)
        }
        ControlMessage::ConnectionCleanup(cleanup) => {
            handle_connection_cleanup(daemon, state, control_tx, cleanup);
            Some(false)
        }
        message => Some(dispatch_control_message(
            daemon,
            state,
            transport_runtime.handle(),
            control_tx,
            message,
        )),
    }
}

pub fn serve_daemon(config: HubConfig) -> DaemonTransportResult<HubDaemonStatus> {
    serve_daemon_inner(
        config,
        #[cfg(test)]
        None,
    )
}

#[cfg(test)]
struct ServeTerminalTest {
    start: Box<
        dyn FnOnce(&mut HubDaemon, &mut tokio_mpsc::Receiver<ControlMessage>, &ControlSender)
            + Send,
    >,
    selected: Box<
        dyn FnOnce(
                &mut HubDaemon,
                &mut DaemonControlState,
                &tokio::runtime::Runtime,
                &ControlSender,
            ) + Send,
    >,
    stopped: Box<dyn FnOnce() + Send>,
}

fn serve_daemon_inner(
    config: HubConfig,
    #[cfg(test)] terminal_test: Option<ServeTerminalTest>,
) -> DaemonTransportResult<HubDaemonStatus> {
    let socket_path = socket_path(&config)?;
    let socket_owner = acquire_socket_owner_lock(&socket_path)?;
    prepare_socket_path(&socket_path, &socket_owner)?;
    let listener = UnixListener::bind(&socket_path).map_err(DaemonTransportError::Io)?;
    listener
        .set_nonblocking(true)
        .map_err(DaemonTransportError::Io)?;

    let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
    let (shutdown_tx, _) = watch::channel(false);
    #[cfg(test)]
    if terminal_test.is_none() {
        install_signal_forwarder(control_tx.clone())?;
    }
    #[cfg(not(test))]
    install_signal_forwarder(control_tx.clone())?;
    let transport_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("botster-hub-transport")
        .build()
        .map_err(DaemonTransportError::Io)?;
    let listener = {
        let _runtime = transport_runtime.enter();
        TokioUnixListener::from_std(listener).map_err(DaemonTransportError::Io)?
    };
    let mut daemon = HubDaemon::start(config)?;
    if let Some(runtime) = daemon.runtime() {
        runtime.bind_data_plane_owner_wake(control_tx.clone());
        runtime.bind_host_owner_wake(control_tx.clone());
        runtime.bind_managed_spawn_owner_wake(control_tx.clone());
    }
    let mut control_state = DaemonControlState {
        event_plane: daemon.local_webrtc().event_plane(),
        client_events: crate::daemon::client_events::ClientEvents::default(),
        pending_runtime: PendingRuntimeState {
            close_source: daemon
                .runtime()
                .map(|runtime| runtime.close_work_source())
                .unwrap_or_default(),
            ..PendingRuntimeState::default()
        },
        plugin_result_budget: crate::daemon::control::reply::RetainedPluginResultBudget::new(),
        ..DaemonControlState::default()
    };
    control_state
        .plugin_result_budget
        .bind_owner_wake(control_tx.clone());
    if let Some(runtime) = daemon.runtime() {
        runtime.install_plugin_completion_notifier(
            control_state.plugin_result_budget.completion_notifier(),
        );
    }
    seed_lifecycle_reconciliation(&mut daemon, &mut control_state);
    let mut connection_tasks = vec![transport_runtime.spawn(accept_connections(
        listener,
        control_tx.clone(),
        shutdown_tx.subscribe(),
        Arc::new(Semaphore::new(DAEMON_MAX_CONNECTIONS)),
    ))];
    #[cfg(test)]
    let (selected, stopped) = match terminal_test {
        Some(test) => {
            (test.start)(&mut daemon, &mut control_rx, &control_tx);
            (Some(test.selected), Some(test.stopped))
        }
        None => (None, None),
    };
    let outcome = 'owner: loop {
        let mut owner_turn = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
        reap_finished_connection_tasks(&mut connection_tasks);
        publish_completion_wakes(&daemon, &mut control_state);
        publish_maintenance_wakes(&mut control_state);
        let slice_due = !control_state.owner_ready.is_empty();
        let event = match classify_owner_poll(control_rx.try_recv(), slice_due) {
            OwnerPollDecision::ServeControl(message) => Some(OwnerEvent::Control(message)),
            OwnerPollDecision::RunSlice => None,
            OwnerPollDecision::Block => {
                match transport_runtime.block_on(receive_owner_event(
                    &mut control_rx,
                    next_owner_deadline(&control_state),
                )) {
                    OwnerEvent::Control(message) => Some(OwnerEvent::Control(message)),
                    OwnerEvent::Reconcile => None,
                }
            }
        };
        if let Some(OwnerEvent::Control(message)) = event {
            match *message {
                Some(message) => {
                    if let Err(message) = enqueue_control_message(&mut control_state, message) {
                        if let ControlMessage::ConnectionCleanup(cleanup) = message {
                            handle_connection_cleanup(
                                &mut daemon,
                                &mut control_state,
                                control_tx.clone(),
                                cleanup,
                            );
                            continue;
                        }
                        break 'owner Err(DaemonTransportError::Protocol(
                            "owner waiter identifiers are exhausted",
                        ));
                    }
                }
                None => break 'owner Err(DaemonTransportError::ControlThreadStopped),
            }
        }
        publish_maintenance_wakes(&mut control_state);
        loop {
            if owner_turn
                .try_charge(
                    Instant::now(),
                    crate::daemon::owner_turn::OwnerTurnCharge::opaque_move(),
                )
                .is_err()
            {
                break;
            }
            let Some(item) = control_state.owner_ready.pop_next() else {
                break;
            };
            if let Some(shutdown) = run_control_ingress_item(
                &mut daemon,
                &mut control_state,
                &transport_runtime,
                control_tx.clone(),
                &shutdown_tx,
                &mut connection_tasks,
                item,
            ) {
                if shutdown {
                    break 'owner Ok(());
                }
                continue;
            }
            if dispatch_owner_ready_item(&mut daemon, &mut control_state, item, &mut owner_turn) {
                break 'owner Ok(());
            }
            publish_maintenance_wakes(&mut control_state);
        }
    };
    #[cfg(test)]
    if let Some(selected) = selected {
        selected(
            &mut daemon,
            &mut control_state,
            &transport_runtime,
            &control_tx,
        );
    }
    if let Some(runtime) = daemon.runtime() {
        runtime.bind_terminal_core_owner();
    }
    let _ = shutdown_tx.send(true);
    wait_for_connection_tasks(
        &transport_runtime,
        &mut connection_tasks,
        &mut control_rx,
        &mut daemon,
        &mut control_state,
        control_tx,
    );
    drain_terminal_host_work(&mut daemon, &mut control_state);
    let status = daemon.stop();
    #[cfg(test)]
    if let Some(stopped) = stopped {
        stopped();
    }
    cleanup_socket_path(&socket_path, socket_owner);
    outcome.map(|()| status)
}

/// Ingress has stopped. Each original row keeps its charges until Host acknowledges disposal.
fn drain_terminal_host_work(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let Some(runtime) = daemon.runtime_mut() else {
        return;
    };
    runtime.host_executor().bind_terminal_owner();
    runtime.bind_terminal_core_owner();
    loop {
        runtime.take_core_completion_notification();
        runtime.host_executor().take_completion_notification();
        runtime.host_executor().take_capacity_notification();
        if !state.terminal_lifecycle.ordinary_disposed
            && dispose_terminal_host_slice(runtime, state)
        {
            state.terminal_lifecycle.ordinary_disposed = true;
        }
        if state.terminal_fault.is_none()
            && state.terminal_lifecycle.ordinary_disposed
            && state.terminal_lifecycle.dispose(runtime)
        {
            match runtime.host_executor().close_and_dispose_completions() {
                Ok(()) => return,
                Err(failure) => {
                    state.terminal_fault = Some(TerminalDrainFault::MailboxClose(failure))
                }
            }
        }
        if !runtime.take_core_completion_notification() {
            thread::park();
        }
    }
}

fn dispose_terminal_host_slice(
    runtime: &crate::HubRuntime,
    state: &mut DaemonControlState,
) -> bool {
    if state.terminal_fault.is_none() {
        while let crate::host_executor::HostCompletionPoll::Ready(completion) =
            runtime.host_executor().poll_completion()
        {
            if let Err(completion) =
                crate::subscription::entity::route_terminal_host_completion(state, completion)
            {
                state.terminal_fault = Some(TerminalDrainFault::Receipt(completion));
                break;
            }
        }
    }
    crate::daemon::control::pending::dispose_terminal_requests(runtime, state);
    crate::daemon::control::host_work::dispose_terminal_recovery(runtime, state);
    let publication = state
        .publication_owner
        .dispose_terminal(runtime, &mut state.budget);
    let events = state
        .event_owner
        .dispose_terminal(runtime, &mut state.budget);
    let entities = state
        .plugin_entities
        .dispose_terminal(runtime, &mut state.budget);
    let resync = state.package_entity_resync_scan.dispose_terminal(runtime);
    let clients = state
        .client_events
        .dispose_terminal(runtime, &state.event_plane);
    let catalog = state
        .session_type_catalog
        .dispose_terminal(runtime.host_executor());
    let obligations = publication
        && events
        && entities
        && resync
        && clients
        && state.pending_requests.is_empty()
        && state.host_recovery.is_empty()
        && state
            .budget
            .dispose_terminal_obligations(runtime.host_executor());
    publication
        && events
        && entities
        && resync
        && clients
        && catalog
        && obligations
        && state.terminal_fault.is_none()
        && state.pending_requests.is_empty()
        && state.host_recovery.is_empty()
        && state.host_completions.is_empty()
        && runtime.host_executor().outstanding() == 0
}

pub(crate) fn record_egress_write_failure(
    diagnostics: &mut DaemonEgressDiagnostics,
    counters: &mut DaemonLifecycleCounters,
    runtime: Option<&crate::HubRuntime>,
    delivery_kind: DaemonDeliveryKind,
    write_class: EgressWriteClass,
) {
    diagnostics.record_write_failure(delivery_kind);
    counters.stalled_writes = counters.stalled_writes.saturating_add(1);
    if write_class == EgressWriteClass::Timeout
        && let Some(runtime) = runtime
    {
        runtime
            .event_plane_counters()
            .record_stalled_write_timeout();
    }
}

pub(crate) fn send_control_response(
    reply_tx: ControlReplySender,
    response: DaemonTransportResult<DaemonResponse>,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    send_control_reply(
        reply_tx,
        crate::daemon::control::reply::ControlReply::plain(response),
        response_delivery_rx,
    )
}

pub(crate) fn send_control_reply(
    reply_tx: ControlReplySender,
    response: crate::daemon::control::reply::ControlReply,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    let should_stop = response.kind() == Some(DaemonResponseKind::Shutdown);
    let response_received = reply_tx.send_reply(response).is_ok();
    wait_for_response_delivery(should_stop, response_received, response_delivery_rx);
    should_stop
}

pub(crate) fn wait_for_response_delivery(
    should_stop: bool,
    response_received: bool,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    if should_stop
        && response_received
        && let Some(response_delivery_rx) = response_delivery_rx
    {
        let _ = response_delivery_rx.recv_timeout(DAEMON_CLIENT_WRITE_TIMEOUT);
        return true;
    }
    false
}

pub(crate) fn tick(logical_clock: &mut u64) -> u64 {
    let current = *logical_clock;
    *logical_clock += 1;
    current
}

fn install_signal_forwarder(control_tx: ControlSender) -> DaemonTransportResult<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM]).map_err(DaemonTransportError::Io)?;
    thread::spawn(move || {
        if signals.forever().next().is_some() {
            let (reply_tx, _reply_rx) = crate::daemon::control::message::control_reply_channel();
            let _ = control_tx.blocking_send(ControlMessage::Request {
                request: Box::new(DaemonRequest::DaemonShutdown),
                transport_request_id: None,
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client_id: None,
                enqueued_at: Instant::now(),
            });
        }
    });
    Ok(())
}

#[derive(Default)]
pub(crate) struct PendingRuntimeState {
    pub(crate) streams: AttachStreamRegistry,
    pub(crate) admission: AdmissionState,
    pub(crate) close_work: Arc<AtomicBool>,
    pub(crate) close_source: crate::data_plane::CloseWorkSource,
}

impl fmt::Debug for PendingRuntimeState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingRuntimeState")
            .finish_non_exhaustive()
    }
}

impl std::ops::Deref for PendingRuntimeState {
    type Target = AttachStreamRegistry;
    fn deref(&self) -> &Self::Target {
        &self.streams
    }
}

impl std::ops::DerefMut for PendingRuntimeState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.streams
    }
}

impl PendingRuntimeState {
    #[allow(dead_code)]
    fn take_close_work(&self) -> bool {
        self.close_work.swap(false, Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn webrtc_is_admitted(&self, grant_id: &str) -> bool {
        matches!(
            self.admission.webrtc_admissions.get(grant_id),
            Some(WebrtcTerminalAdmission::Admitted { .. })
        )
    }

    #[cfg(test)]
    pub(crate) fn has_webrtc_admission_row(&self, grant_id: &str) -> bool {
        self.admission.webrtc_admissions.contains_key(grant_id)
    }

    #[cfg(test)]
    pub(crate) fn has_host_compatibility_row(&self, grant_id: &str) -> bool {
        self.admission.host_compatibility.contains_key(grant_id)
    }
}

fn run_inventory_reconcile_slice(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    if run_inventory_reconcile_phase_progress(daemon, state) == BackgroundProgress::Runnable {
        mark_background_ready(state, BackgroundWork::InventoryReconcile);
    }
}

fn run_pump_observe_slice(daemon: &HubDaemon, state: &mut DaemonControlState) {
    if run_pump_observe_phase(daemon, state) == BackgroundProgress::Runnable {
        mark_background_ready(state, BackgroundWork::PumpObserve);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundProgress {
    Waiting,
    Runnable,
    Done,
}

/// Validate owner route bookkeeping against one Core inventory read.
///
/// The read is requested on one pump turn and consumed on a later one; the
/// owner never waits for it. Returns `true` while more work is pending.
pub(crate) fn run_inventory_reconcile_phase(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
) -> bool {
    run_inventory_reconcile_phase_progress(daemon, state) != BackgroundProgress::Done
}

fn run_inventory_reconcile_phase_progress(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
) -> BackgroundProgress {
    use crate::data_plane::driver::CoreTicketPoll;

    let Some(runtime) = daemon.runtime() else {
        state.pump.reconcile_after = None;
        state.pump.take_inventory_reconcile_again();
        state.reconcile_inventory = None;
        return BackgroundProgress::Done;
    };
    let Some(read) = state.reconcile_inventory.as_mut() else {
        // The epoch is captured at submission, on this thread, before the
        // request is enqueued: the read cannot cover any later attach.
        let read_epoch = state.pending_runtime.attach_epoch();
        let routes = state.pending_runtime.inventory_reconcile_routes(
            read_epoch,
            state.pump.reconcile_after.as_ref(),
            PUMP_MAX_ROUTES_VALIDATED,
        );
        let Some(waiter_id) = background_waiter_id(state, BackgroundWork::InventoryReconcile)
        else {
            return BackgroundProgress::Done;
        };
        state.reconcile_inventory = Some(InventoryRead {
            read_epoch,
            ticket: runtime.terminal_subscription_generations_for_owner(waiter_id, routes),
        });
        return BackgroundProgress::Waiting;
    };
    let read_epoch = read.read_epoch;
    let inventory = match read.ticket.poll() {
        CoreTicketPoll::Pending => return BackgroundProgress::Waiting,
        // Refused admission: clear the single slot; the cursor stays and the
        // next pump resubmits (one ticket in flight, no queue).
        CoreTicketPoll::Refused => {
            state.reconcile_inventory = None;
            return BackgroundProgress::Runnable;
        }
        CoreTicketPoll::Lost => {
            state.reconcile_inventory = None;
            state.pump.reconcile_after = None;
            return BackgroundProgress::Done;
        }
        CoreTicketPoll::Ready(inventory) => inventory,
    };
    state.reconcile_inventory = None;
    // Core keeps one live owner per (session, subscription). A takeover gets
    // a fresh monotonic generation, so the generation identifies that owner.
    let lookup = |_client_id: &str, session_id: &str, subscription_id: &str| {
        inventory
            .iter()
            .find(|(live_session, live_subscription, _)| {
                live_session == session_id && live_subscription == subscription_id
            })
            .and_then(|(_, _, generation)| *generation)
    };
    let progress = state.pending_runtime.reconcile_inventory_slice(
        lookup,
        read_epoch,
        state.pump.reconcile_after.clone(),
        PUMP_MAX_ROUTES_VALIDATED,
    );
    for (session_id, subscription_id) in progress.retired {
        record_attached_subscription_change(
            &mut state.pending_runtime,
            &mut state.attach_close,
            &mut state.lifecycle_counters,
            Some(AttachedSubscriptionChange::Detach(AttachedSubscription {
                session_id,
                subscription_id,
            })),
            None,
        );
    }
    if progress.more {
        state.pump.reconcile_after = progress.after;
        BackgroundProgress::Runnable
    } else {
        state.pump.reconcile_after = None;
        if state.pump.take_inventory_reconcile_again() {
            BackgroundProgress::Runnable
        } else {
            BackgroundProgress::Done
        }
    }
}

/// Drive one bounded observe slice through a Core ticket.
///
/// Returns `true` while the pass is incomplete or the read is in flight.
fn run_pump_observe_phase(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
) -> BackgroundProgress {
    use crate::data_plane::driver::CoreTicketPoll;

    let Some(runtime) = daemon.runtime() else {
        state.observe_resume = None;
        state.observe_read = None;
        return BackgroundProgress::Done;
    };
    let Some(ticket) = state.observe_read.as_mut() else {
        let now = tick(&mut state.logical_clock);
        let Some(waiter_id) = background_waiter_id(state, BackgroundWork::PumpObserve) else {
            return BackgroundProgress::Done;
        };
        state.observe_read = Some(runtime.observe_lifecycle_slice_for_owner(
            waiter_id,
            now,
            state.observe_resume.as_ref(),
            OBSERVE_SLICE_BUDGET,
        ));
        return BackgroundProgress::Waiting;
    };
    let slice = match ticket.poll() {
        CoreTicketPoll::Pending => return BackgroundProgress::Waiting,
        // Refused admission: clear the single slot and resubmit next pump.
        CoreTicketPoll::Refused => {
            state.observe_read = None;
            return BackgroundProgress::Runnable;
        }
        CoreTicketPoll::Lost => {
            state.observe_read = None;
            state.observe_resume = None;
            return BackgroundProgress::Done;
        }
        CoreTicketPoll::Ready(slice) => slice,
    };
    state.observe_read = None;
    if let Ok(slice) = slice {
        state.lifecycle_counters.lifecycle_session_drains = state
            .lifecycle_counters
            .lifecycle_session_drains
            .saturating_add(1);
        state.observe_resume = if slice.complete || slice.resync_required.is_some() {
            None
        } else {
            Some(botster_core_daemon::ObserveLifecycleCursor {
                pass_id: slice.pass_id,
                last_visited: slice.last_visited,
            })
        };
        if state.maintenance.take_journal_wake() {
            state.maintenance.note_authoritative_mutation();
            mark_pump_ready(state);
        }
        if state.observe_resume.is_some() {
            BackgroundProgress::Runnable
        } else {
            BackgroundProgress::Done
        }
    } else {
        BackgroundProgress::Done
    }
}

enum TerminalDrainFault {
    Receipt(crate::host_executor::HostCompletion),
    MailboxClose(crate::host_executor::HostCompletionDisposalFailure),
}

struct TerminalLifecycle {
    identity: crate::host_executor::HostJobIdentity,
    ordinary_disposed: bool,
    job: Option<crate::host_disposal::Job>,
    stage: TerminalLifecycleStage,
}

enum TerminalLifecycleStage {
    Engine,
    Bridges,
    Disposed,
}

impl TerminalLifecycle {
    fn new(waiter: crate::owner_identity::WaiterId) -> Self {
        Self {
            identity: crate::host_executor::HostJobIdentity::first(waiter),
            ordinary_disposed: false,
            job: None,
            stage: TerminalLifecycleStage::Engine,
        }
    }

    fn dispose(&mut self, runtime: &mut crate::HubRuntime) -> bool {
        if matches!(self.stage, TerminalLifecycleStage::Disposed) {
            return true;
        }
        if self.job.is_none() {
            let Some(permit) = runtime.host_executor().try_reserve() else {
                return false;
            };
            let lifecycle = runtime
                .take_plugin_lifecycle()
                .expect("terminal disposal takes the lifecycle owner once");
            self.job = Some(crate::host_disposal::Job::new(
                crate::host_disposal::Parts {
                    storage: None,
                    identity: self.identity,
                    permit,
                    payload: Box::new(lifecycle),
                    model: None,
                },
            ));
        }
        if let crate::host_disposal::Poll::Disposed(permit) =
            self.job.as_mut().expect("terminal lifecycle job").poll()
        {
            match self.stage {
                TerminalLifecycleStage::Engine => {
                    self.job = Some(crate::host_disposal::Job::new_plugin_bridges(
                        crate::host_disposal::Parts {
                            storage: None,
                            identity: self.identity,
                            permit,
                            payload: Box::new(()),
                            model: None,
                        },
                        runtime.terminal_plugin_bridges(),
                    ));
                    self.stage = TerminalLifecycleStage::Bridges;
                }
                TerminalLifecycleStage::Bridges => {
                    drop(permit);
                    self.job = None;
                    self.stage = TerminalLifecycleStage::Disposed;
                }
                TerminalLifecycleStage::Disposed => {
                    unreachable!("a completed lifecycle has no disposal job")
                }
            }
        }
        matches!(self.stage, TerminalLifecycleStage::Disposed)
    }
}

pub(crate) struct DaemonControlState {
    pub(crate) coordination_fault: Option<crate::daemon::control::coordination::CoordinationFault>,
    pub(crate) coordination_waiting_for_owner: bool,
    pub(crate) coordination_capacity_waiters: BTreeSet<crate::owner_identity::WaiterId>,
    terminal_fault: Option<TerminalDrainFault>,
    terminal_lifecycle: TerminalLifecycle,
    pub(crate) event_owner: crate::daemon::event_owner::EventOwnerState,
    pub(crate) publication_owner: crate::daemon::publication_owner::PublicationOwnerState,
    pub(crate) logical_clock: u64,
    pub(crate) drain_cursors: BTreeMap<String, u64>,
    pub(crate) egress_diagnostics: DaemonEgressDiagnostics,
    pub(crate) entity_subscriptions: BTreeMap<String, EntitySubscriptionState>,
    pub(crate) event_plane: std::sync::Arc<crate::subscription::package_events::ClientEventPlane>,
    pub(crate) client_events: crate::daemon::client_events::ClientEvents,
    pub(crate) pending_runtime: PendingRuntimeState,
    pub(crate) lifecycle_counters: DaemonLifecycleCounters,
    pub(crate) maintenance: MaintenanceState,
    pub(crate) pump: PumpState,
    pub(crate) released_entity_generations: u64,
    pub(crate) attach_close: crate::subscription::closed_events::AttachCloseBookkeeping,
    pub(crate) pending_hub_update_reply: Option<ControlReplySender>,
    /// Requests whose response waits on a Core owner-thread result.
    pub(crate) pending_requests: BTreeMap<
        crate::owner_identity::WaiterId,
        crate::daemon::control::pending::PendingControlRequest,
    >,
    /// Explicit reservations this owner still holds after a spawn failure.
    /// Bounded by Core's pending-spawn capacity; retried on the next Spawn.
    pub(crate) retained_explicit_reservations: Vec<botster_core::SessionReservation>,
    pub(crate) waiter_ids: crate::owner_identity::WaiterIdSource,
    pub(crate) current_waiter_id: Option<crate::owner_identity::WaiterId>,
    pub(crate) shutdown_waiter: Option<crate::owner_identity::WaiterId>,
    pub(crate) owner_ready: crate::daemon::owner_schedule::ReadyQueues,
    control_ingress: BTreeMap<crate::owner_identity::WaiterId, ControlMessage>,
    pub(crate) deadlines: crate::daemon::owner_schedule::DeadlineIndex,
    reservation_deadlines: BTreeMap<crate::owner_identity::WaiterId, ReservationDeadline>,
    reservation_waiters_by_label: BTreeMap<String, crate::owner_identity::WaiterId>,
    background_waiter_ids: BTreeMap<BackgroundWork, crate::owner_identity::WaiterId>,
    background_core_waiters: BTreeMap<crate::owner_identity::WaiterId, BackgroundWaiter>,
    pub(crate) host_completions:
        BTreeMap<crate::owner_identity::WaiterId, crate::host_executor::HostCompletion>,
    // Each recovery record retains one of the eight host operation slots.
    // Exclusive document admission limits package rollback to one record.
    // Executor failure can retain all eight already-admitted operations.
    pub(crate) host_recovery: BTreeMap<
        crate::owner_identity::WaiterId,
        crate::daemon::control::host_work::HostRecoveryRequired,
    >,
    pub(crate) family_cleanup_waiters: BTreeMap<crate::owner_identity::WaiterId, u64>,
    causal_wake_after: Option<crate::owner_identity::WaiterId>,
    causal_wake_through: Option<crate::owner_identity::WaiterId>,
    causal_wake_active: bool,
    causal_wake_again: bool,
    pub(crate) document_owner: Option<crate::owner_identity::WaiterId>,
    pub(crate) document_waiters: std::collections::BTreeSet<crate::owner_identity::WaiterId>,
    pub(crate) host_completion_drain_pending: bool,
    pub(crate) host_completion_drain_faulted: bool,
    pub(crate) host_capacity_wake_pending: bool,
    pub(crate) blocked_session_type_roots:
        BTreeMap<std::path::PathBuf, crate::owner_identity::WaiterId>,
    /// Correlation for non-blocking plugin request-response work.
    pub(crate) plugin_controls: crate::daemon::control::plugins::PluginControlState,
    /// Correlation and retained replies for asynchronous entity providers.
    pub(crate) plugin_entities: crate::daemon::control::entities::PluginEntityState,
    pub(crate) package_entity_resync_scan:
        crate::subscription::entity_resync::PackageEntityResyncScan,
    /// Global logical-byte ownership for drained plugin results and replies.
    pub(crate) plugin_result_budget: crate::daemon::control::reply::RetainedPluginResultBudget,
    /// Bounded ownership of connections, pending requests, and cleanup
    /// obligations (reserved-channel binds, connection and peer cleanup,
    /// exact-generation releases).
    pub(crate) budget: crate::daemon::owner_budget::OwnerBudget,
    /// Lifecycle reads the maintenance slices have in flight.
    pub(crate) maintenance_reads: crate::daemon_maintenance::MaintenanceCoreReads,
    /// Close-event registry decisions cached for the current pump pass.
    pub(crate) close_event_decisions: crate::subscription::closed_events::CloseEventDecisions,
    /// Session-type catalog built off the owner thread.
    pub(crate) session_type_catalog: crate::subscription::entity::SessionTypeCatalogCache,
    /// Inventory read in flight for the pump reconcile phase, with the
    /// attach epoch captured when it was submitted.
    reconcile_inventory: Option<InventoryRead>,
    observe_resume: Option<botster_core_daemon::ObserveLifecycleCursor>,
    /// Observe slice in flight for the pump observe phase.
    observe_read: Option<
        crate::data_plane::driver::CoreTicket<
            Result<
                botster_core_daemon::ObserveLifecycleSlice,
                botster_core_daemon::SessionLifecyclePageError,
            >,
        >,
    >,
}

impl fmt::Debug for DaemonControlState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DaemonControlState")
            .field("logical_clock", &self.logical_clock)
            .field("entity_subscriptions", &self.entity_subscriptions.len())
            .field("lifecycle_counters", &self.lifecycle_counters)
            .field("pending_requests", &self.pending_requests.len())
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

impl Default for DaemonControlState {
    fn default() -> Self {
        let waiter_ids = crate::owner_identity::WaiterIdSource::default();
        let session_type_catalog = crate::subscription::entity::SessionTypeCatalogCache::for_owner(
            waiter_ids
                .next()
                .expect("a fresh Owner identity source has capacity"),
        );
        let terminal_lifecycle = TerminalLifecycle::new(
            waiter_ids
                .next()
                .expect("a fresh Owner identity source has capacity"),
        );
        Self {
            terminal_fault: None,
            coordination_fault: None,
            coordination_waiting_for_owner: false,
            coordination_capacity_waiters: BTreeSet::new(),
            terminal_lifecycle,
            event_owner: crate::daemon::event_owner::EventOwnerState::default(),
            publication_owner: crate::daemon::publication_owner::PublicationOwnerState::default(),
            logical_clock: 1,
            drain_cursors: BTreeMap::new(),
            egress_diagnostics: DaemonEgressDiagnostics::default(),
            entity_subscriptions: BTreeMap::new(),
            client_events: crate::daemon::client_events::ClientEvents::default(),
            event_plane: std::sync::Arc::new(
                crate::subscription::package_events::ClientEventPlane::default(),
            ),
            pending_runtime: PendingRuntimeState::default(),
            lifecycle_counters: DaemonLifecycleCounters::default(),
            maintenance: MaintenanceState::default(),
            pump: PumpState::default(),
            released_entity_generations: 0,
            attach_close: crate::subscription::closed_events::AttachCloseBookkeeping::default(),
            pending_hub_update_reply: None,
            pending_requests: BTreeMap::new(),
            retained_explicit_reservations: Vec::new(),
            waiter_ids,
            current_waiter_id: None,
            shutdown_waiter: None,
            owner_ready: crate::daemon::owner_schedule::ReadyQueues::new(),
            control_ingress: BTreeMap::new(),
            deadlines: crate::daemon::owner_schedule::DeadlineIndex::new(),
            reservation_deadlines: BTreeMap::new(),
            reservation_waiters_by_label: BTreeMap::new(),
            background_waiter_ids: BTreeMap::new(),
            background_core_waiters: BTreeMap::new(),
            host_completions: BTreeMap::new(),
            family_cleanup_waiters: BTreeMap::new(),
            causal_wake_after: None,
            causal_wake_through: None,
            causal_wake_active: false,
            causal_wake_again: false,
            host_recovery: BTreeMap::new(),
            document_owner: None,
            document_waiters: std::collections::BTreeSet::new(),
            host_completion_drain_pending: false,
            host_completion_drain_faulted: false,
            host_capacity_wake_pending: false,
            blocked_session_type_roots: BTreeMap::new(),
            plugin_controls: crate::daemon::control::plugins::PluginControlState::default(),
            plugin_entities: crate::daemon::control::entities::PluginEntityState::default(),
            package_entity_resync_scan:
                crate::subscription::entity_resync::PackageEntityResyncScan::default(),
            plugin_result_budget:
                crate::daemon::control::reply::RetainedPluginResultBudget::default(),
            budget: crate::daemon::owner_budget::OwnerBudget::default(),
            maintenance_reads: crate::daemon_maintenance::MaintenanceCoreReads::default(),
            close_event_decisions: crate::subscription::closed_events::CloseEventDecisions::default(
            ),
            session_type_catalog,
            reconcile_inventory: None,
            observe_resume: None,
            observe_read: None,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct DaemonEgressDiagnostics {
    terminal_write_failures: u64,
    control_write_failures: u64,
}

impl DaemonEgressDiagnostics {
    pub(crate) fn record_write_failure(&mut self, delivery_kind: DaemonDeliveryKind) {
        match delivery_kind {
            DaemonDeliveryKind::Terminal => {
                self.terminal_write_failures = self.terminal_write_failures.saturating_add(1);
            }
            DaemonDeliveryKind::Control => {
                self.control_write_failures = self.control_write_failures.saturating_add(1);
            }
        }
    }

    /// Count the two possible rows before formatting either message.
    pub(crate) fn diagnostics_bytes(&self, limit: usize) -> Option<usize> {
        let mut bytes = std::mem::size_of::<Vec<DaemonDiagnostic>>();
        if bytes > limit {
            return None;
        }
        for (kind, failures) in [
            (DaemonDeliveryKind::Terminal, self.terminal_write_failures),
            (DaemonDeliveryKind::Control, self.control_write_failures),
        ] {
            if failures == 0 {
                continue;
            }
            let digits = failures.ilog10() as usize + 1;
            let message_bytes = "daemon client  egress observed  bounded write failure(s)"
                .len()
                .checked_add(kind.label().len())?
                .checked_add(digits)?;
            bytes = bytes
                .checked_add(std::mem::size_of::<DaemonDiagnostic>())?
                .checked_add("daemon_client_egress".len())?
                .checked_add(message_bytes)?;
            if bytes > limit {
                return None;
            }
        }
        Some(bytes)
    }

    pub(crate) fn bounded_diagnostics(
        &self,
        limit: usize,
    ) -> Option<(Vec<DaemonDiagnostic>, usize)> {
        let bytes = self.diagnostics_bytes(limit)?;
        Some((self.diagnostics(), bytes))
    }

    pub(crate) fn diagnostics(&self) -> Vec<DaemonDiagnostic> {
        let mut diagnostics = Vec::new();
        if self.terminal_write_failures > 0 {
            diagnostics.push(egress_backpressure_diagnostic(
                DaemonDeliveryKind::Terminal,
                self.terminal_write_failures,
            ));
        }
        if self.control_write_failures > 0 {
            diagnostics.push(egress_backpressure_diagnostic(
                DaemonDeliveryKind::Control,
                self.control_write_failures,
            ));
        }
        diagnostics
    }
}

fn egress_backpressure_diagnostic(
    delivery_kind: DaemonDeliveryKind,
    failures: u64,
) -> DaemonDiagnostic {
    DaemonDiagnostic::backpressure(
        "daemon_client_egress",
        format!(
            "daemon client {} egress observed {failures} bounded write failure(s)",
            delivery_kind.label()
        ),
    )
}

pub(crate) fn request_succeeded(response: Result<&DaemonResponse, &DaemonTransportError>) -> bool {
    matches!(
        response,
        Ok(response) if response.kind != DaemonResponseKind::OperatorError
    )
}

pub(crate) fn should_mark_pump_after_control(request: &DaemonRequest, succeeded: bool) -> bool {
    match request {
        DaemonRequest::Spawn { .. }
        | DaemonRequest::SpawnSessionType { .. }
        | DaemonRequest::Attach { .. } => succeeded,
        DaemonRequest::Detach { .. }
        | DaemonRequest::ShutdownSession { .. }
        | DaemonRequest::RemoveSession { .. } => true,
        _ => false,
    }
}

#[cfg(test)]
fn receive_test_control_message(
    receiver: &mut tokio_mpsc::Receiver<ControlMessage>,
) -> ControlMessage {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build bounded test receive runtime");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("timed out waiting for daemon control message")
            .expect("daemon control sender remains live")
    })
}

#[cfg(test)]
fn receive_test_control_request(
    receiver: &mut tokio_mpsc::Receiver<ControlMessage>,
) -> ControlMessage {
    loop {
        match receive_test_control_message(receiver) {
            ControlMessage::RegisterUnixAdmission { reply_tx, .. } => {
                let _ = reply_tx.send(());
            }
            ControlMessage::RegisterWebrtcAdmission { .. } => {}
            message => return message,
        }
    }
}

#[cfg(test)]
fn receive_test_control_reply(
    receiver: crate::daemon::control::message::ControlReplyReceiver,
) -> DaemonTransportResult<DaemonResponse> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build bounded test reply runtime");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), receiver)
            .await
            .expect("timed out waiting for daemon control reply")
            .expect("daemon control reply sender remains live")
            .into_parts()
            .0
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileHubStateStore;
    use crate::HubDaemon;
    use crate::HubStateStore;
    use crate::PackageState;
    use crate::admission::budgets::DAEMON_CONTROL_QUEUE_CAPACITY;
    use crate::admission::unix_hello::UnixTerminalAdmission;
    use crate::client_api_dto::response::{daemon_events, daemon_response_base};
    use crate::daemon::control::message::{daemon_delivery_kind, egress_write_class};
    use crate::daemon::control::{
        ControlMessage, DaemonObservability, attach_bind_operator_error, handle_control_request,
    };
    use crate::daemon::error::daemon_operator_error;
    use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
    use crate::transport::unix::connection::handle_connection;
    use botster_core::RequestId;
    use botster_core::contract::terminal_adapter::TerminalAdapter;
    use botster_hub_client::{
        ClientFrame, DaemonCompatibilityRequirement, DaemonHello, DaemonHelloAck, DaemonRequest,
        DaemonResponse, DaemonResponseKind, DaemonUnixFrameReader, DaemonUnixMuxFrame,
        DaemonUnixTerminalFrame, PROTOCOL, ServerFrame, encode_client_frame, write_client_frame,
    };
    use botster_terminal_protocol::{RouteId, RoutedTerminalFrame, encode_output};
    use std::io::Write;
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn terminal_serve_normal_and_error_exits_wait_for_all_host_disposal_receipts() {
        struct ThreadProbe(mpsc::Sender<String>);
        impl Drop for ThreadProbe {
            fn drop(&mut self) {
                self.0
                    .send(thread::current().name().unwrap_or("unnamed").into())
                    .unwrap();
            }
        }
        struct PluginRuntimeProbe(mpsc::Sender<(&'static str, String)>);
        impl botster_core::PluginRuntime for PluginRuntimeProbe {
            fn invoke(
                &self,
                request: botster_core::PluginInvocationRequest,
                _cancellation: botster_core::PluginCancellationToken,
            ) -> botster_core::PluginInvocationResult {
                botster_core::PluginInvocationResult::Completed(
                    botster_core::PluginInvocationSuccess {
                        request_id: request.request_id,
                        handler: request.handler,
                        payload: Some(botster_core::BoundaryJson(serde_json::json!({
                            "retained": "x".repeat(16 * 1024),
                        }))),
                    },
                )
            }

            fn stop(&self, _plugin_key: &botster_core::PluginKey) {
                self.0
                    .send(("stop", thread::current().name().unwrap_or("unnamed").into()))
                    .unwrap();
            }
        }
        impl Drop for PluginRuntimeProbe {
            fn drop(&mut self) {
                self.0
                    .send(("drop", thread::current().name().unwrap_or("unnamed").into()))
                    .unwrap();
            }
        }
        struct Gate(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
        impl Gate {
            fn release(&self) {
                *self.0.0.lock().unwrap() = true;
                self.0.1.notify_all();
            }
        }
        impl Drop for Gate {
            fn drop(&mut self) {
                self.release();
            }
        }
        struct Probe {
            gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
            entered: mpsc::Sender<String>,
            disposed: mpsc::Sender<String>,
        }
        impl Drop for Probe {
            fn drop(&mut self) {
                let name = thread::current().name().unwrap_or("unnamed").to_string();
                let _ = self.entered.send(name.clone());
                let mut released = self.gate.0.lock().unwrap();
                while !*released {
                    released = self.gate.1.wait(released).unwrap();
                }
                let _ = self.disposed.send(name);
            }
        }
        for error_exit in [false, true] {
            let root = PathBuf::from("/private/tmp").join(format!(
                "hub-exit-{}-{}",
                std::process::id(),
                error_exit
            ));
            let mut config = package_control_config(root.join("data"));
            config.transports.local_socket = Some(crate::config::LocalSocketBinding {
                path: root.join("hub.sock"),
            });
            let gate = Gate(Arc::new((
                std::sync::Mutex::new(false),
                std::sync::Condvar::new(),
            )));
            let host_gate = gate.0.clone();
            let engine_gate = Gate(Arc::new((
                std::sync::Mutex::new(false),
                std::sync::Condvar::new(),
            )));
            let final_engine_gate = engine_gate.0.clone();
            let (engine_entered_tx, engine_entered_rx) = mpsc::channel();
            let (engine_disposed_tx, engine_disposed_rx) = mpsc::channel();
            let (plugin_runtime_tx, plugin_runtime_rx) = mpsc::channel();
            let publication_gate = Gate(Arc::new((
                std::sync::Mutex::new(false),
                std::sync::Condvar::new(),
            )));
            let queued_publication_gate = publication_gate.0.clone();
            let (publication_entered_tx, publication_entered_rx) = mpsc::channel();
            let (publication_disposed_tx, publication_disposed_rx) = mpsc::channel();
            let (coordination_disposed_tx, coordination_disposed_rx) = mpsc::channel();
            let (spawner_gate_tx, spawner_gate_rx) = mpsc::channel();
            let (bridges_tx, bridges_rx) = mpsc::channel();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (disposed_tx, disposed_rx) = mpsc::channel();
            let (inspect_tx, inspect_rx) =
                mpsc::channel::<Box<dyn Fn() -> (usize, bool, bool) + Send>>();
            let (stopped_tx, stopped_rx) = mpsc::channel();
            let (finished_tx, finished_rx) = mpsc::channel();
            let owner = thread::Builder::new()
                .name("terminal-serve-owner".into())
                .spawn(move || {
                    let result = serve_daemon_inner(
                        config,
                        Some(ServeTerminalTest {
                            start: Box::new(move |_daemon, receiver, sender| {
                                if error_exit {
                                    receiver.close();
                                } else {
                                    let (reply_tx, _reply_rx) =
                                        crate::daemon::control::message::control_reply_channel();
                                    sender
                                        .try_send(ControlMessage::Request {
                                            request: Box::new(DaemonRequest::DaemonShutdown),
                                            transport_request_id: None,
                                            reply_tx,
                                            response_delivery_rx: None,
                                            grant_id: None,
                                            client_id: None,
                                            enqueued_at: Instant::now(),
                                        })
                                        .unwrap();
                                }
                            }),
                            selected: Box::new(move |daemon, state, transport, sender| {
                                assert_eq!(
                                    daemon.runtime().unwrap().host_executor().outstanding(),
                                    0
                                );
                                let lifecycle = daemon.runtime().unwrap().plugin_lifecycle_handle();
                                let mut registry = crate::PackageRegistry::new(Default::default());
                                registry
                                    .install(
                                        serde_json::from_value(serde_json::json!({
                                            "name": "terminal-probe",
                                            "version": "1.0.0",
                                            "kind": "plugin",
                                            "botster": ">=0.1.0",
                                            "source": { "type": "path", "path": "." },
                                            "capabilities": [],
                                            "entrypoints": []
                                        }))
                                        .unwrap(),
                                        crate::PackageProvenance {
                                            source: "terminal-probe".into(),
                                            checksum: None,
                                        },
                                        "install terminal fixture",
                                    )
                                    .unwrap();
                                registry.enable("terminal-probe", "enable fixture").unwrap();
                                let handler = botster_core::PluginHandlerRef {
                                    plugin_key: botster_core::PluginKey("terminal-probe".into()),
                                    kind: botster_core::PluginHandlerKind::Command,
                                    handler_id: "retained".into(),
                                };
                                lifecycle
                                    .load_package(
                                        &registry,
                                        "terminal-probe",
                                        crate::HubPluginRuntimeBundle {
                                            runtime: Arc::new(PluginRuntimeProbe(
                                                plugin_runtime_tx,
                                            )),
                                            handlers: vec![
                                                botster_core::PluginHandlerRegistration {
                                                    handler: handler.clone(),
                                                    required_capability: None,
                                                },
                                            ],
                                            event_handlers: Vec::new(),
                                            descriptors: Vec::new(),
                                            resources: Vec::new(),
                                            entrypoint: Some("terminal-probe".into()),
                                            metadata: None,
                                        },
                                    )
                                    .unwrap();
                                let engine_probe = Probe {
                                    gate: final_engine_gate,
                                    entered: engine_entered_tx,
                                    disposed: engine_disposed_tx,
                                };
                                lifecycle.install_completion_notifier(Arc::new(move || {
                                    let _ = &engine_probe;
                                }));
                                assert!(matches!(
                                    lifecycle.try_admit(
                                        botster_core::PluginInvocationClass::RequestResponse,
                                        botster_core::PluginInvocationRequest {
                                            request_id: RequestId(
                                                "undrained-terminal-result".into()
                                            ),
                                            handler,
                                            timeout_ms: 1_000,
                                            context: botster_core::PluginInvocationContext {
                                                client_id: None,
                                                session_id: None,
                                                subscription_id: None,
                                                surface_id: None,
                                                origin: None,
                                                metadata: None,
                                            },
                                            payload: botster_core::BoundaryJson(serde_json::json!(
                                                {}
                                            )),
                                        },
                                    ),
                                    botster_core::PluginAdmissionResult::Queued { .. }
                                ));
                                let deadline = Instant::now() + Duration::from_secs(3);
                                while lifecycle.debug_snapshot().undrained_completions != 1 {
                                    assert!(
                                        Instant::now() < deadline,
                                        "the plugin publishes its result"
                                    );
                                    thread::yield_now();
                                }
                                // The fixture adds admitted work at the common exit boundary.
                                for _ in 0..crate::host_executor::HOST_OPERATION_CAPACITY {
                                    let (reply_tx, _reply_rx) =
                                        crate::daemon::control::message::control_reply_channel();
                                    assert!(!crate::daemon::control::request::handle(
                                        daemon,
                                        state,
                                        transport.handle(),
                                        sender.clone(),
                                        ControlMessage::Request {
                                            request: Box::new(DaemonRequest::Status),
                                            transport_request_id: None,
                                            reply_tx,
                                            response_delivery_rx: None,
                                            grant_id: None,
                                            client_id: None,
                                            enqueued_at: Instant::now(),
                                        }
                                    ));
                                }
                                for entry in state.pending_requests.values_mut() {
                                    let probe = Probe {
                                        gate: host_gate.clone(),
                                        entered: entered_tx.clone(),
                                        disposed: disposed_tx.clone(),
                                    };
                                    let lifecycle = lifecycle.clone();
                                    entry.retire = Some(Box::new(move |_, _, _, _| {
                                        drop((probe, lifecycle));
                                    }));
                                }
                                let identity = state.terminal_lifecycle.identity;
                                assert!(state.waiter_ids.next().unwrap().0 > identity.waiter_id.0);
                                state.waiter_ids =
                                    crate::owner_identity::WaiterIdSource::with_next(u64::MAX);
                                assert!(state.waiter_ids.next().is_none());
                                assert_eq!(state.terminal_lifecycle.identity, identity);
                                let coordination = daemon.runtime().unwrap().coordination_bridge();
                                let coordination_response = coordination.test_queue_pending(
                                    crate::lua_runtime::PendingCoordinationOperation::Drain {
                                        target: botster_core::EnvelopeTarget::Endpoint {
                                            endpoint_id: botster_core::EndpointId(
                                                "terminal-probe".into(),
                                            ),
                                        },
                                        after: None,
                                        limit: 1,
                                    },
                                );
                                coordination.test_set_pending_drop_probe(ThreadProbe(
                                    coordination_disposed_tx,
                                ));
                                let publication = daemon.runtime().unwrap().entity_publish_bridge();
                                let publication_response = publication.test_queue_stale_publish(
                                    botster_core::PluginKey("terminal-probe".into()),
                                    serde_json::json!({
                                        "type": "entity_patch",
                                        "entity_type": "terminal-probe.items",
                                        "snapshot_seq": 1,
                                        "id": "item",
                                        "patch": { "body": "x".repeat(16 * 1024) },
                                    }),
                                    None,
                                );
                                publication.test_set_pending_drop_probe(Probe {
                                    gate: queued_publication_gate,
                                    entered: publication_entered_tx,
                                    disposed: publication_disposed_tx,
                                });
                                let spawner = daemon.runtime().unwrap().session_type_spawner();
                                let spawner_drops =
                                    spawner.test_seed_terminal_pending(Some(spawner_gate_rx));
                                bridges_tx
                                    .send((
                                        coordination,
                                        coordination_response,
                                        publication,
                                        publication_response,
                                        spawner,
                                        spawner_drops,
                                    ))
                                    .unwrap();
                                assert_eq!(
                                    state.pending_requests.len(),
                                    crate::host_executor::HOST_OPERATION_CAPACITY
                                );
                                assert_eq!(
                                    state.budget.outstanding(),
                                    crate::host_executor::HOST_OPERATION_CAPACITY
                                );
                                inspect_tx
                                    .send(Box::new(
                                        daemon
                                            .runtime()
                                            .unwrap()
                                            .host_executor()
                                            .test_terminal_probe(),
                                    ))
                                    .unwrap();
                            }),
                            stopped: Box::new(move || {
                                stopped_tx.send(()).unwrap();
                            }),
                        }),
                    );
                    finished_tx.send(result).unwrap();
                })
                .unwrap();
            let inspect = inspect_rx
                .recv_timeout(Duration::from_secs(10))
                .unwrap_or_else(|error| {
                    panic!(
                        "serve reaches the selected exit: {error:?}; outcome: {:?}",
                        finished_rx.recv_timeout(Duration::from_secs(1))
                    )
                });
            let worker = entered_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("Host starts terminal disposal");
            let (
                coordination,
                coordination_response,
                publication,
                publication_response,
                spawner,
                spawner_drops,
            ) = bridges_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert!(worker.starts_with("botster-hub-host"), "{worker}");
            assert_eq!(
                inspect(),
                (crate::host_executor::HOST_OPERATION_CAPACITY, true, false)
            );
            assert!(
                stopped_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "daemon.stop must wait for disposal acknowledgement"
            );
            assert!(disposed_rx.try_recv().is_err());
            assert!(engine_entered_rx.try_recv().is_err());
            assert!(plugin_runtime_rx.try_recv().is_err());
            assert_eq!(coordination.test_pending_count(), 1);
            assert_eq!(publication.pending_publish_count(), 1);
            let publication_charge = publication.retained_counts();
            assert_eq!(publication_charge.0, 1);
            assert!(publication_charge.1 > 16 * 1024);
            assert_eq!(spawner.test_terminal_pending_counts(), (1, 1, true));
            gate.release();
            let engine_worker = engine_entered_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the final engine owner reaches destruction");
            assert!(
                engine_worker.starts_with("botster-hub-host"),
                "{engine_worker}"
            );
            assert_eq!(inspect(), (1, true, false));
            assert!(stopped_rx.recv_timeout(Duration::from_millis(50)).is_err());
            let runtime_events: Vec<_> = plugin_runtime_rx.try_iter().collect();
            assert_eq!(runtime_events.len(), 2);
            assert_eq!(runtime_events[0].0, "stop");
            assert_eq!(runtime_events[1].0, "drop");
            assert!(
                runtime_events
                    .iter()
                    .all(|(_, name)| name.starts_with("botster-hub-host"))
            );
            assert_eq!(coordination.test_pending_count(), 1);
            assert_eq!(publication.pending_publish_count(), 1);
            assert_eq!(publication.retained_counts(), publication_charge);
            assert_eq!(spawner.test_terminal_pending_counts(), (1, 1, true));
            engine_gate.release();
            let publication_worker = publication_entered_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("Host destroys the queued publication after engine disposal");
            assert!(publication_worker.starts_with("botster-hub-host"));
            assert_eq!(inspect(), (1, true, false));
            assert!(stopped_rx.try_recv().is_err());
            assert_eq!(coordination.test_pending_count(), 0);
            assert!(
                coordination_disposed_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .starts_with("botster-hub-host")
            );
            assert!(matches!(
                coordination_response.try_recv(),
                Err(mpsc::TryRecvError::Disconnected)
            ));
            assert_eq!(publication.pending_publish_count(), 0);
            assert_eq!(publication.retained_counts(), publication_charge);
            assert!(matches!(
                publication_response.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            publication_gate.release();
            let first_spawner_drop = spawner_drops
                .recv_timeout(Duration::from_secs(10))
                .expect("Host destroys spawner payloads after publication disposal");
            assert!(first_spawner_drop.0.starts_with("botster-hub-host"));
            assert!(
                first_spawner_drop.1,
                "spawner queue locks are released before destruction"
            );
            assert_eq!(inspect(), (1, true, false));
            assert!(stopped_rx.try_recv().is_err());
            assert_eq!(publication.retained_counts(), (0, 0));
            assert!(matches!(
                publication_response.try_recv(),
                Err(mpsc::TryRecvError::Disconnected)
            ));
            assert_eq!(spawner.test_terminal_pending_counts(), (0, 0, true));
            spawner_gate_tx.send(()).unwrap();
            let result = finished_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("serve exits after Host disposal");
            if error_exit {
                assert!(matches!(
                    result,
                    Err(DaemonTransportError::ControlThreadStopped)
                ));
            } else {
                assert!(result.is_ok(), "{result:?}");
            }
            stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(inspect(), (0, false, true));
            assert_eq!(spawner.test_terminal_pending_counts(), (0, 0, false));
            let remaining_spawner_drops: Vec<_> = spawner_drops.try_iter().collect();
            assert_eq!(remaining_spawner_drops.len(), 1);
            assert!(
                remaining_spawner_drops
                    .iter()
                    .all(|(name, unlocked)| name.starts_with("botster-hub-host") && *unlocked)
            );
            assert!(
                publication_disposed_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .starts_with("botster-hub-host")
            );
            assert!(
                engine_disposed_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .starts_with("botster-hub-host")
            );
            let disposed: Vec<_> = disposed_rx.try_iter().collect();
            assert_eq!(
                disposed.len(),
                crate::host_executor::HOST_OPERATION_CAPACITY
            );
            assert!(
                disposed
                    .iter()
                    .all(|name| name.starts_with("botster-hub-host"))
            );
            owner.join().unwrap();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    fn admit_cleanup_test_subscription(daemon: &HubDaemon, state: &mut DaemonControlState) {
        use crate::package_event_router::{EmittedContract, EventAudience};

        let router = daemon.runtime().unwrap().package_event_router();
        router
            .try_register_contracts(vec![EmittedContract {
                owner: "cleanup-owner".into(),
                name: "ready".into(),
                audience: std::collections::BTreeSet::from([EventAudience::Clients]),
                schema: crate::package_event_schema::CompiledEventSchema::compile(
                    &serde_json::json!({"type": "object", "additionalProperties": true}),
                )
                .expect("compile cleanup contract"),
                package_generation: 1,
            }])
            .expect("register cleanup contract");
        crate::daemon::client_events::admit_connection(state, "cleanup-connection").unwrap();
        state
            .event_plane
            .try_subscribe(
                "cleanup-connection",
                "cleanup-subscription",
                "cleanup-owner",
                "ready",
                Vec::new(),
                router.policy(),
                router,
            )
            .expect("admit cleanup subscription");
    }

    fn settle_cleanup_test_owner(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(!drive_ready_test_turn(daemon, state));
            if state.owner_ready.is_empty()
                && daemon.runtime().unwrap().host_executor().outstanding() == 0
                && !state.maintenance.wakes.has_any()
            {
                return;
            }
            assert!(Instant::now() < deadline, "initial owner work must settle");
            thread::yield_now();
        }
    }

    fn close_cleanup_test_connection(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        permit: crate::daemon::owner_budget::OwnerPermit,
    ) {
        use crate::transport::unix::connection::{
            ConnectionCleanupGuard, ConnectionTerminalReason,
        };
        let (control_tx, mut control_rx) = tokio_mpsc::channel(1);
        let cleanup_permit = control_tx.clone().try_reserve_owned().unwrap();
        drop(ConnectionCleanupGuard::new(
            cleanup_permit,
            "cleanup-connection".into(),
            ConnectionTerminalReason::NormalClose,
            permit,
        ));
        let ControlMessage::ConnectionCleanup(cleanup) = control_rx.try_recv().unwrap() else {
            panic!("the connection guard must send its cleanup message");
        };
        handle_connection_cleanup(daemon, state, control_tx, cleanup);
    }

    #[test]
    fn client_event_cleanup_unix_sibling_wakes_during_blocked_reclamation() {
        use crate::daemon::control::handle_control_message;
        use crate::package_event_router::{EmittedContract, EventAudience};
        use crate::transport::unix::connection::handle_connection_async;

        let root = unique_package_control_dir("client-event-unix-sibling-wake");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        let permit = state.budget.reserve_connection().unwrap();
        let router = daemon.runtime().unwrap().package_event_router().clone();
        router
            .try_register_contracts(vec![EmittedContract {
                owner: "cleanup-owner".into(),
                name: "ready".into(),
                audience: std::collections::BTreeSet::from([EventAudience::Clients]),
                schema: crate::package_event_schema::CompiledEventSchema::compile(
                    &serde_json::json!({"type": "object", "additionalProperties": true}),
                )
                .unwrap(),
                package_generation: 1,
            }])
            .unwrap();
        let (server, mut client) = UnixStream::pair().unwrap();
        server.set_nonblocking(true).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let (control_tx, mut control_rx) = tokio_mpsc::channel(32);
        let connection_tx = control_tx.clone();
        let cleanup_permit = connection_tx.clone().try_reserve_owned().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let transport = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let stream = {
                let _entered = runtime.enter();
                tokio::net::UnixStream::from_std(server).unwrap()
            };
            runtime.block_on(handle_connection_async(
                stream,
                connection_tx,
                cleanup_permit,
                shutdown_rx,
                permit,
            ))
        });
        write_client_frame(
            &mut client,
            &ClientFrame::Hello {
                hello: DaemonHello {
                    protocol: PROTOCOL.into(),
                    compatibility: DaemonCompatibilityRequirement::for_package_event_subscriptions(
                    ),
                    terminal_compatibility: None,
                },
            },
        )
        .unwrap();
        let mut reader = DaemonUnixFrameReader::new();
        read_hello_ack(&mut client, &mut reader);
        let registration = receive_test_control_message(&mut control_rx);
        let (client_id, event_reader) = match &registration {
            ControlMessage::RegisterUnixAdmission {
                client_id,
                event_reader,
                ..
            } => (client_id.clone(), event_reader.clone()),
            _ => panic!("expected Unix admission"),
        };
        let owner_transport = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(!handle_control_message(
            &mut daemon,
            &mut state,
            owner_transport.handle(),
            control_tx.clone(),
            registration
        ));
        for (serial, subscription_id) in [(1, "retiring"), (2, "sibling")] {
            write_request(
                &mut client,
                serial,
                DaemonRequest::SubscribeEvents {
                    subscription_id: subscription_id.into(),
                    owner: "cleanup-owner".into(),
                    name: "ready".into(),
                    subjects: Vec::new(),
                },
            );
            let message = receive_test_control_message(&mut control_rx);
            assert!(!handle_control_message(
                &mut daemon,
                &mut state,
                owner_transport.handle(),
                control_tx.clone(),
                message
            ));
            let response = read_response(&mut client, &mut reader, serial);
            assert!(
                response.error.is_none(),
                "subscription {subscription_id}: {response:?}"
            );
        }
        settle_cleanup_test_owner(&mut daemon, &mut state);
        let retiring = state
            .event_plane
            .subscription_mailbox(&client_id, "retiring")
            .unwrap();
        let sibling = state
            .event_plane
            .subscription_mailbox(&client_id, "sibling")
            .unwrap();
        let connection = retiring.connection().unwrap();
        router.test_with_inner_held(|| {
            write_request(
                &mut client,
                3,
                DaemonRequest::UnsubscribeEvents {
                    subscription_id: "retiring".into(),
                },
            );
            let message = receive_test_control_message(&mut control_rx);
            assert!(!handle_control_message(
                &mut daemon,
                &mut state,
                owner_transport.handle(),
                control_tx.clone(),
                message
            ));
            assert!(read_response(&mut client, &mut reader, 3).error.is_none());
            let deadline = Instant::now() + Duration::from_secs(3);
            while !connection.test_cleanup_started() {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    Instant::now() < deadline,
                    "cleanup must reach the held router"
                );
                thread::yield_now();
            }
            // Hold the reader after its empty read, before it registers the next wake.
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel();
            event_reader.test_on_empty_read(move || {
                entered_tx.send(()).unwrap();
                resume_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            });
            entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            let timer_ticks = event_reader.test_timer_ticks();
            sibling
                .try_push(
                    "sibling",
                    "cleanup-owner",
                    "ready",
                    serde_json::json!({"serial": 1}),
                    12,
                )
                .unwrap();
            resume_tx.send(()).unwrap();
            for serial in 1..=2 {
                match reader
                    .read_frame(&mut client)
                    .expect("sibling event must wake the idle Unix writer")
                {
                    DaemonUnixMuxFrame::Server(ServerFrame::Event {
                        event:
                            botster_hub_client::DaemonEvent::PackageEvent {
                                subscription_id,
                                payload,
                                ..
                            },
                    }) => {
                        assert_eq!(subscription_id, "sibling");
                        assert_eq!(payload, serde_json::json!({"serial": serial}));
                    }
                    other => panic!("expected sibling event, got {other:?}"),
                }
                assert_eq!(
                    event_reader.test_timer_ticks(),
                    timer_ticks,
                    "delivery must not need a timer"
                );
                if serial == 1 {
                    sibling
                        .try_push(
                            "sibling",
                            "cleanup-owner",
                            "ready",
                            serde_json::json!({"serial": 2}),
                            12,
                        )
                        .unwrap();
                }
            }
        });
        settle_cleanup_test_owner(&mut daemon, &mut state);
        shutdown_tx.send(true).unwrap();
        transport.join().unwrap().unwrap();
        let ControlMessage::ConnectionCleanup(cleanup) =
            receive_test_control_message(&mut control_rx)
        else {
            panic!("the Unix transport must return its original cleanup permit");
        };
        handle_connection_cleanup(&mut daemon, &mut state, control_tx.clone(), cleanup);
        let deadline = Instant::now() + Duration::from_secs(3);
        while state.budget.outstanding() != 0 {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                Instant::now() < deadline,
                "connection cleanup must release admission"
            );
            thread::yield_now();
        }
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn client_event_cleanup_waiter_exhaustion_refuses_before_subscription_effects() {
        let root = unique_package_control_dir("client-event-waiter-exhaustion");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        let permit = state.budget.reserve_connection().unwrap();
        admit_cleanup_test_subscription(&daemon, &mut state);
        settle_cleanup_test_owner(&mut daemon, &mut state);
        state.pending_runtime.admission.host_compatibility.insert(
            "exhausted-connection".into(),
            crate::admission::unix_hello::HostCompatibilityRecord {
                event_reader: None,
                required_features: vec![
                    botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.into(),
                ],
            },
        );
        state.waiter_ids = crate::owner_identity::WaiterIdSource::with_next(u64::MAX);
        let response = crate::daemon::control::events::handle_client_event_request(
            &mut daemon,
            &mut state,
            "exhausted-connection",
            DaemonRequest::SubscribeEvents {
                subscription_id: "refused".into(),
                owner: "cleanup-owner".into(),
                name: "ready".into(),
                subjects: Vec::new(),
            },
        );
        assert!(response.error.is_some());
        assert_eq!(
            state.event_plane.test_residency("exhausted-connection"),
            None
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .package_event_router()
                .test_client_holder_count("exhausted-connection"),
            0
        );
        assert_eq!(state.budget.outstanding(), 1);
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
        assert!(state.owner_ready.is_empty());
        state.budget.release(permit);
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn client_event_cleanup_ready_exhaustion_retains_initial_and_completed_work() {
        for completed in [false, true] {
            let root = unique_package_control_dir("client-event-ready-exhaustion");
            let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
            let mut state = DaemonControlState::default();
            let permit = state.budget.reserve_connection().unwrap();
            admit_cleanup_test_subscription(&daemon, &mut state);
            settle_cleanup_test_owner(&mut daemon, &mut state);
            if completed {
                close_cleanup_test_connection(&mut daemon, &mut state, permit);
                let item = state.owner_ready.pop_next().unwrap();
                let mut turn = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
                assert!(!dispatch_owner_ready_item(
                    &mut daemon,
                    &mut state,
                    item,
                    &mut turn
                ));
                state.owner_ready =
                    crate::daemon::owner_schedule::ReadyQueues::with_next_enqueue_serial(u64::MAX);
                let deadline = Instant::now() + Duration::from_secs(3);
                let completion = loop {
                    match daemon.runtime().unwrap().host_executor().poll_completion() {
                        crate::host_executor::HostCompletionPoll::Ready(completion) => {
                            break completion;
                        }
                        crate::host_executor::HostCompletionPoll::Empty => {
                            assert!(Instant::now() < deadline, "the cleanup worker must finish");
                            thread::yield_now();
                        }
                        crate::host_executor::HostCompletionPoll::Stopped => {
                            panic!("the executor remains live")
                        }
                    }
                };
                crate::subscription::entity::route_host_completion(&mut state, completion);
                assert!(state.client_events.test_recovery("cleanup-connection"));
                assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
            } else {
                state.owner_ready =
                    crate::daemon::owner_schedule::ReadyQueues::with_next_enqueue_serial(u64::MAX);
                close_cleanup_test_connection(&mut daemon, &mut state, permit);
                assert!(state.client_events.test_recovery("cleanup-connection"));
                assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
                assert_eq!(
                    daemon
                        .runtime()
                        .unwrap()
                        .package_event_router()
                        .test_client_holder_count("cleanup-connection"),
                    1
                );
            }
            assert_eq!(state.budget.outstanding(), 1);
            for _ in 0..3 {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(state.owner_ready.is_empty());
            }
            daemon.stop();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn client_event_cleanup_unsubscribe_after_unload_finishes_without_retry() {
        use crate::package_event_router::{OwnerOp, OwnerOpKind};
        let root = unique_package_control_dir("client-event-unsubscribe-after-unload");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        let permit = state.budget.reserve_connection().unwrap();
        admit_cleanup_test_subscription(&daemon, &mut state);
        state.pending_runtime.admission.host_compatibility.insert(
            "cleanup-connection".into(),
            crate::admission::unix_hello::HostCompatibilityRecord {
                event_reader: None,
                required_features: vec![
                    botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.into(),
                ],
            },
        );
        settle_cleanup_test_owner(&mut daemon, &mut state);
        daemon
            .runtime()
            .unwrap()
            .record_event_plane_owner_op(OwnerOp {
                kind: OwnerOpKind::Unload,
                owner: "cleanup-owner".into(),
                generation: 1,
            });
        settle_cleanup_test_owner(&mut daemon, &mut state);
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .package_event_router()
                .test_client_holder_count("cleanup-connection"),
            0
        );
        let response = crate::daemon::control::events::handle_client_event_request(
            &mut daemon,
            &mut state,
            "cleanup-connection",
            DaemonRequest::UnsubscribeEvents {
                subscription_id: "cleanup-subscription".into(),
            },
        );
        assert_eq!(response.kind, DaemonResponseKind::EventUnsubscribed);
        settle_cleanup_test_owner(&mut daemon, &mut state);
        assert_eq!(
            state.event_plane.test_residency("cleanup-connection"),
            Some((0, 0, 0))
        );
        let duplicate = crate::daemon::control::events::handle_client_event_request(
            &mut daemon,
            &mut state,
            "cleanup-connection",
            DaemonRequest::UnsubscribeEvents {
                subscription_id: "cleanup-subscription".into(),
            },
        );
        assert!(duplicate.error.is_some());
        for _ in 0..3 {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(state.owner_ready.is_empty());
        }
        close_cleanup_test_connection(&mut daemon, &mut state, permit);
        let deadline = Instant::now() + Duration::from_secs(3);
        while state.budget.outstanding() != 0 {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn client_event_cleanup_reservation_rollback_keeps_the_original_slot_until_reclaimed() {
        use crate::admission::connection_budget::ChannelClass;
        use crate::admission::reservations::ReservationBinding;
        for duplicate in [false, true] {
            let root = unique_package_control_dir("client-event-reservation-rollback");
            let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
            let mut state = DaemonControlState::default();
            let permit = state.budget.reserve_connection().unwrap();
            admit_cleanup_test_subscription(&daemon, &mut state);
            settle_cleanup_test_owner(&mut daemon, &mut state);
            state.pending_runtime.admission.host_compatibility.insert(
                "cleanup-connection".into(),
                crate::admission::unix_hello::HostCompatibilityRecord {
                    event_reader: None,
                    required_features: vec![
                        botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.into(),
                    ],
                },
            );
            state.pending_runtime.admission.webrtc_admissions.insert(
                "cleanup-connection".into(),
                WebrtcTerminalAdmission::Admitted {
                    required_features: Vec::new(),
                    terminal_requirement: None,
                    mux: crate::transport::webrtc::WebRtcConnectionMux::new(),
                    peer_generation: 7,
                },
            );
            let original = state
                .event_plane
                .subscription_mailbox("cleanup-connection", "cleanup-subscription")
                .unwrap();
            let connection = original.connection().unwrap();
            let reservation = duplicate.then(|| {
                state
                    .pending_runtime
                    .admission
                    .reservations
                    .reserve_subscription(
                        ChannelClass::Event,
                        "rollback".into(),
                        1,
                        7,
                        crate::admission::reservations::now_seconds(),
                        ReservationBinding::Event {
                            mailbox: original.clone(),
                        },
                    )
                    .unwrap()
            });
            let permits = (0..crate::host_executor::HOST_OPERATION_CAPACITY)
                .map(|_| {
                    daemon
                        .runtime()
                        .unwrap()
                        .host_executor()
                        .try_reserve()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let response = crate::daemon::control::events::handle_client_event_request(
                &mut daemon,
                &mut state,
                "cleanup-connection",
                DaemonRequest::SubscribeEvents {
                    subscription_id: "rollback".into(),
                    owner: "cleanup-owner".into(),
                    name: "ready".into(),
                    subjects: Vec::new(),
                },
            );
            assert!(response.error.is_some());
            assert_eq!(connection.test_slot_count(), 2);
            assert_eq!(
                daemon
                    .runtime()
                    .unwrap()
                    .package_event_router()
                    .test_client_holder_count("cleanup-connection"),
                2
            );
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(state.client_events.has_capacity_waiters());
            assert_eq!(state.budget.outstanding(), 1);
            for _ in 0..3 {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(state.owner_ready.is_empty());
            }
            drop(permits);
            settle_cleanup_test_owner(&mut daemon, &mut state);
            assert_eq!(connection.test_slot_count(), 1);
            assert_eq!(
                daemon
                    .runtime()
                    .unwrap()
                    .package_event_router()
                    .test_client_holder_count("cleanup-connection"),
                1
            );
            assert!(!original.is_retired());
            if let Some(reservation) = reservation {
                assert!(
                    state
                        .pending_runtime
                        .admission
                        .reservations
                        .forget_label(&reservation.label, 7)
                );
            }
            close_cleanup_test_connection(&mut daemon, &mut state, permit);
            let deadline = Instant::now() + Duration::from_secs(3);
            while state.budget.outstanding() != 0 {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(Instant::now() < deadline);
                thread::yield_now();
            }
            daemon.stop();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn client_event_cleanup_global_drain_exhaustion_retains_the_executor_receipt() {
        let root = unique_package_control_dir("client-event-global-drain-exhaustion");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        let permit = state.budget.reserve_connection().unwrap();
        admit_cleanup_test_subscription(&daemon, &mut state);
        settle_cleanup_test_owner(&mut daemon, &mut state);
        close_cleanup_test_connection(&mut daemon, &mut state, permit);
        let item = state.owner_ready.pop_next().unwrap();
        let mut turn = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
        assert!(!dispatch_owner_ready_item(
            &mut daemon,
            &mut state,
            item,
            &mut turn
        ));
        state.owner_ready =
            crate::daemon::owner_schedule::ReadyQueues::with_next_enqueue_serial(u64::MAX);
        let deadline = Instant::now() + Duration::from_secs(3);
        while !state.host_completion_drain_faulted {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                Instant::now() < deadline,
                "the production Host wake must record scheduling failure"
            );
            thread::yield_now();
        }
        assert!(state.host_completion_drain_pending);
        assert!(!state.client_events.test_recovery("cleanup-connection"));
        assert_eq!(state.budget.outstanding(), 1);
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
        // Even a fresh test queue must not restart a terminal drain fault.
        state.owner_ready = crate::daemon::owner_schedule::ReadyQueues::new();
        for _ in 0..3 {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(state.owner_ready.is_empty());
            assert!(state.host_completion_drain_faulted);
        }
        let crate::host_executor::HostCompletionPoll::Ready(receipt) =
            daemon.runtime().unwrap().host_executor().poll_completion()
        else {
            panic!("the executor must retain the original receipt");
        };
        assert!(matches!(
            &receipt.result,
            crate::host_executor::HostResult::ClientEventCleanup(Ok(_))
        ));
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
        drop(receipt);
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn client_event_cleanup_faults_keep_the_original_connection_and_host_permits() {
        for fault in [
            "router",
            "mailbox",
            "pool",
            "connection",
            "lookup",
            "phase",
            "panic",
        ] {
            let root = unique_package_control_dir(&format!("client-cleanup-fault-{fault}"));
            let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
            let mut state = DaemonControlState::default();
            let permit = state.budget.reserve_connection().unwrap();
            admit_cleanup_test_subscription(&daemon, &mut state);
            settle_cleanup_test_owner(&mut daemon, &mut state);
            let router = daemon.runtime().unwrap().package_event_router().clone();
            let mailbox = state
                .event_plane
                .subscription_mailbox("cleanup-connection", "cleanup-subscription")
                .unwrap();
            mailbox
                .try_push(
                    "cleanup-subscription",
                    "cleanup-owner",
                    "ready",
                    serde_json::json!({"value": 1}),
                    23,
                )
                .unwrap();
            let connection = mailbox.connection().unwrap();
            match fault {
                "router" => {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        router.test_with_inner_held(|| panic!("poison cleanup router"))
                    }));
                }
                "mailbox" => {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        mailbox.test_with_inner_held(|| panic!("poison cleanup mailbox"))
                    }));
                }
                "pool" | "connection" => connection.test_poison(fault),
                "lookup" => state.event_plane.test_poison_lookup(),
                "phase" => state
                    .client_events
                    .test_set_phase("cleanup-connection", u64::MAX),
                "panic" => connection.test_panic_cleanup(),
                _ => unreachable!(),
            }
            close_cleanup_test_connection(&mut daemon, &mut state, permit);
            let deadline = Instant::now() + Duration::from_secs(5);
            while !state.client_events.test_recovery("cleanup-connection") {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    Instant::now() < deadline,
                    "the cleanup fault must reach recovery: {fault}"
                );
                thread::yield_now();
            }
            assert_eq!(connection.test_slot_count(), usize::from(fault != "lookup"));
            assert_eq!(
                state.budget.outstanding(),
                1,
                "the disconnect obligation owns its permit: {fault}"
            );
            assert_eq!(
                daemon.runtime().unwrap().host_executor().outstanding(),
                1,
                "recovery owns the submitted Host slot: {fault}"
            );
            for _ in 0..3 {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    state.owner_ready.is_empty(),
                    "terminal cleanup faults must not remain runnable: {fault}"
                );
            }
            daemon.stop();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn client_event_cleanup_stopped_host_retains_connection_without_host_admission() {
        let root = unique_package_control_dir("client-cleanup-stopped-before-admission");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        let permit = state.budget.reserve_connection().unwrap();
        admit_cleanup_test_subscription(&daemon, &mut state);
        settle_cleanup_test_owner(&mut daemon, &mut state);
        let mailbox = state
            .event_plane
            .subscription_mailbox("cleanup-connection", "cleanup-subscription")
            .unwrap();
        let connection = mailbox.connection().unwrap();
        mailbox
            .try_push(
                "cleanup-subscription",
                "cleanup-owner",
                "ready",
                serde_json::json!({"value": 1}),
                23,
            )
            .unwrap();
        daemon.runtime_mut().unwrap().test_stop_host_submissions();
        close_cleanup_test_connection(&mut daemon, &mut state, permit);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !state.client_events.has_capacity_waiters() {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                Instant::now() < deadline,
                "cleanup must retain its capacity wait"
            );
            thread::yield_now();
        }
        assert!(!state.client_events.test_recovery("cleanup-connection"));
        assert_eq!(connection.test_slot_count(), 1);
        assert_eq!(state.budget.outstanding(), 1);
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
        assert!(
            daemon
                .runtime()
                .unwrap()
                .host_executor()
                .try_reserve()
                .is_none()
        );
        assert!(!connection.test_cleanup_started());
        for _ in 0..3 {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                state.owner_ready.is_empty(),
                "a capacity wait must not remain runnable"
            );
            assert_eq!(connection.test_slot_count(), 1);
            assert_eq!(state.budget.outstanding(), 1);
        }
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn client_event_cleanup_churn_counts_failed_and_retiring_slots_at_full_host_capacity() {
        for poison in [false, true] {
            let root = unique_package_control_dir(&format!("client-cleanup-slot-churn-{poison}"));
            let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
            let mut state = DaemonControlState::default();
            let permit = state.budget.reserve_connection().unwrap();
            admit_cleanup_test_subscription(&daemon, &mut state);
            settle_cleanup_test_owner(&mut daemon, &mut state);
            state.pending_runtime.admission.host_compatibility.insert(
                "cleanup-connection".into(),
                crate::admission::unix_hello::HostCompatibilityRecord {
                    event_reader: None,
                    required_features: vec![
                        botster_hub_client::FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS.into(),
                    ],
                },
            );
            let router = daemon.runtime().unwrap().package_event_router().clone();
            let original = state
                .event_plane
                .subscription_mailbox("cleanup-connection", "cleanup-subscription")
                .unwrap();
            let connection = original.connection().unwrap();
            let mut permits = (0..crate::host_executor::HOST_OPERATION_CAPACITY)
                .map(|_| {
                    daemon
                        .runtime()
                        .unwrap()
                        .host_executor()
                        .try_reserve()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let response = crate::daemon::control::events::handle_client_event_request(
                &mut daemon,
                &mut state,
                "cleanup-connection",
                DaemonRequest::UnsubscribeEvents {
                    subscription_id: "cleanup-subscription".into(),
                },
            );
            assert_eq!(response.kind, DaemonResponseKind::EventUnsubscribed);
            assert!(original.is_retired());
            for index in 1..crate::subscription::package_events::MAX_SUBSCRIPTIONS_PER_CONNECTION {
                let response = crate::daemon::control::events::handle_client_event_request(
                    &mut daemon,
                    &mut state,
                    "cleanup-connection",
                    DaemonRequest::SubscribeEvents {
                        subscription_id: format!("failed-{index}"),
                        owner: "undeclared".into(),
                        name: "missing".into(),
                        subjects: Vec::new(),
                    },
                );
                assert_eq!(response.error.unwrap().code, "rejected_undeclared");
            }
            assert_eq!(
                connection.test_slot_count(),
                crate::subscription::package_events::MAX_SUBSCRIPTIONS_PER_CONNECTION
            );
            for subscription_id in ["cleanup-subscription", "overflow"] {
                let response = crate::daemon::control::events::handle_client_event_request(
                    &mut daemon,
                    &mut state,
                    "cleanup-connection",
                    DaemonRequest::SubscribeEvents {
                        subscription_id: subscription_id.into(),
                        owner: "cleanup-owner".into(),
                        name: "ready".into(),
                        subjects: Vec::new(),
                    },
                );
                assert_eq!(
                    response.error.unwrap().code,
                    if subscription_id == "overflow" {
                        "too_many_event_subscriptions"
                    } else {
                        "duplicate_event_subscription"
                    }
                );
            }
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(state.client_events.has_capacity_waiters());
            for _ in 0..3 {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    state.owner_ready.is_empty(),
                    "capacity waiting must not schedule another cleanup attempt"
                );
            }
            assert_eq!(router.test_client_holder_count("cleanup-connection"), 1);
            assert_eq!(
                state.event_plane.test_residency("cleanup-connection"),
                Some((0, 0, 64))
            );
            if poison {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    router.test_with_inner_held(|| panic!("poison full-slot cleanup"))
                }));
            }
            close_cleanup_test_connection(&mut daemon, &mut state, permit);
            assert_eq!(state.budget.outstanding(), 1);
            drop(permits.pop());
            let deadline = Instant::now() + Duration::from_secs(5);
            while if poison {
                !state.client_events.test_recovery("cleanup-connection")
            } else {
                state.budget.outstanding() != 0
            } {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    Instant::now() < deadline,
                    "capacity release must complete cleanup or retain the exact fault"
                );
                thread::yield_now();
            }
            if poison {
                assert_eq!(connection.test_slot_count(), 64);
                assert_eq!(state.budget.outstanding(), 1);
                assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 8);
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(state.owner_ready.is_empty());
            } else {
                assert_eq!(connection.test_slot_count(), 0);
                assert_eq!(router.test_client_holder_count("cleanup-connection"), 0);
                crate::daemon::client_events::retire_mailbox(&mut state, &original);
                assert!(
                    state.owner_ready.is_empty(),
                    "a stale mailbox must not recreate a closed connection row"
                );
            }
            drop(permits);
            daemon.stop();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn client_event_cleanup_webrtc_without_routes_retains_payload_until_worker_reclamation() {
        use crate::transport::webrtc::peer::{
            LocalWebrtcChannelTerminalSignal, LocalWebrtcCleanupDisposition,
            LocalWebrtcSenderTerminalRecord, LocalWebrtcTerminalCause,
        };

        let root = unique_package_control_dir("client-cleanup-webrtc-payload");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        assert!(state.budget.reserve_peer("cleanup-connection"));
        admit_cleanup_test_subscription(&daemon, &mut state);
        settle_cleanup_test_owner(&mut daemon, &mut state);
        let router = daemon.runtime().unwrap().package_event_router().clone();
        let mailbox = state
            .event_plane
            .subscription_mailbox("cleanup-connection", "cleanup-subscription")
            .unwrap();
        let payload = serde_json::json!({"value": "x".repeat(2048)});
        let bytes = serde_json::to_vec(&payload).unwrap().len();
        mailbox
            .try_push(
                "cleanup-subscription",
                "cleanup-owner",
                "ready",
                payload,
                bytes,
            )
            .unwrap();
        let mut permits = (0..crate::host_executor::HOST_OPERATION_CAPACITY)
            .map(|_| {
                daemon
                    .runtime()
                    .unwrap()
                    .host_executor()
                    .try_reserve()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let (control_tx, _control_rx) = tokio_mpsc::channel(1);
        crate::daemon::control::webrtc::handle_peer_closed(
            &mut daemon,
            &mut state,
            control_tx,
            ControlMessage::LocalWebrtcPeerClosed {
                grant_id: "cleanup-connection".into(),
                attached_subscriptions: Vec::new(),
                entity_subscription_ids: Vec::new(),
                terminal_record: LocalWebrtcSenderTerminalRecord {
                    schema_version: 1,
                    grant_id: "cleanup-connection".into(),
                    request_operation: "event_delivery".into(),
                    message_id: None,
                    next_chunk_index: 0,
                    last_sent_chunk_index: None,
                    total_chunks: 0,
                    pressured: false,
                    peer_connection_state: "closed".into(),
                    channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
                    cause: LocalWebrtcTerminalCause::PeerClosed,
                    cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
                },
            },
        );
        assert_eq!(
            state.budget.outstanding(),
            1,
            "a peer without routes retains its event cleanup permit"
        );
        assert!(!drive_ready_test_turn(&mut daemon, &mut state));
        assert_eq!(router.test_client_holder_count("cleanup-connection"), 1);
        mailbox.test_with_inner_held(|| {
            drop(permits.pop());
            let deadline = Instant::now() + Duration::from_secs(5);
            while router.test_client_holder_count("cleanup-connection") != 0 {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    Instant::now() < deadline,
                    "the worker must detach the exact holder before reclamation"
                );
                thread::yield_now();
            }
            assert_eq!(
                state.event_plane.test_residency("cleanup-connection"),
                Some((1, bytes, 1))
            );
            assert_eq!(state.budget.outstanding(), 1);
            assert_eq!(
                daemon.runtime().unwrap().host_executor().outstanding(),
                crate::host_executor::HOST_OPERATION_CAPACITY
            );
            for _ in 0..3 {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    state.owner_ready.is_empty(),
                    "mailbox contention must not make the Owner retry"
                );
            }
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while state.budget.outstanding() != 0 {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                Instant::now() < deadline,
                "worker reclamation must release the peer permit"
            );
            thread::yield_now();
        }
        assert!(
            state
                .event_plane
                .test_residency("cleanup-connection")
                .is_none()
        );
        drop(permits);
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn client_event_cleanup_rejects_a_forged_completion_without_releasing_admission() {
        for mismatch in ["phase", "connection"] {
            let root =
                unique_package_control_dir(&format!("client-cleanup-forged-completion-{mismatch}"));
            let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
            let mut state = DaemonControlState::default();
            let permit = state.budget.reserve_connection().unwrap();
            admit_cleanup_test_subscription(&daemon, &mut state);
            settle_cleanup_test_owner(&mut daemon, &mut state);
            close_cleanup_test_connection(&mut daemon, &mut state, permit);
            // Dispatch the admitted child without consuming its completion.
            let item = state
                .owner_ready
                .pop_next()
                .expect("initial client cleanup item");
            let mut turn = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
            assert!(!dispatch_owner_ready_item(
                &mut daemon,
                &mut state,
                item,
                &mut turn
            ));
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut completion = loop {
                match daemon.runtime().unwrap().host_executor().poll_completion() {
                    crate::host_executor::HostCompletionPoll::Ready(completion) => {
                        break completion;
                    }
                    crate::host_executor::HostCompletionPoll::Empty => {
                        assert!(Instant::now() < deadline, "the admitted worker must finish");
                        thread::yield_now();
                    }
                    crate::host_executor::HostCompletionPoll::Stopped => {
                        panic!("the cleanup executor remains live")
                    }
                }
            };
            if mismatch == "phase" {
                completion.identity.phase -= 1;
            } else {
                let crate::host_executor::HostResult::ClientEventCleanup(Ok(done)) =
                    &mut completion.result
                else {
                    panic!("the original worker must produce a successful cleanup receipt");
                };
                done.connection = state
                    .event_plane
                    .admit_connection("forged-connection")
                    .unwrap();
            }
            crate::subscription::entity::route_host_completion(&mut state, completion);
            while !state.client_events.test_recovery("cleanup-connection") {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    Instant::now() < deadline,
                    "the wrong connection receipt must reach recovery"
                );
            }
            assert_eq!(state.budget.outstanding(), 1);
            assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
            for _ in 0..3 {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(state.owner_ready.is_empty());
            }
            daemon.stop();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn client_event_cleanup_expiry_publishes_its_initial_wake() {
        let root = unique_package_control_dir("client-event-cleanup-initial-wake");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
            .expect("start client cleanup daemon");
        let mut state = DaemonControlState::default();
        assert!(state.budget.reserve_peer("cleanup-connection"));
        admit_cleanup_test_subscription(&daemon, &mut state);
        settle_cleanup_test_owner(&mut daemon, &mut state);
        let mailbox = state
            .event_plane
            .subscription_mailbox("cleanup-connection", "cleanup-subscription")
            .expect("accepted mailbox");
        let reservation = state
            .pending_runtime
            .admission
            .reservations
            .reserve_subscription(
                crate::admission::connection_budget::ChannelClass::Event,
                "cleanup-subscription".into(),
                1,
                1,
                0,
                crate::admission::reservations::ReservationBinding::Event { mailbox },
            )
            .expect("reserve event channel");
        state
            .pending_runtime
            .admission
            .grant_by_peer_generation
            .insert(1, "cleanup-connection".into());
        assert!(arm_reservation_deadline(
            &mut state,
            reservation.label.clone(),
            1,
            0,
        ));
        let waiter = state.reservation_waiters_by_label[&reservation.label];
        assert!(mark_reservation_deadline_ready(&mut state, waiter));
        let item = state.owner_ready.pop_next().expect("reservation deadline");
        assert!(state.owner_ready.is_empty());
        let router = daemon.runtime().unwrap().package_event_router().clone();
        router.test_with_inner_held(|| {
            let mut turn = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
            assert!(!dispatch_owner_ready_item(
                &mut daemon,
                &mut state,
                item,
                &mut turn
            ));
            assert!(
                !state
                    .reservation_waiters_by_label
                    .contains_key(&reservation.label)
            );
            assert!(
                !state.owner_ready.is_empty()
                    || daemon.runtime().unwrap().host_executor().outstanding() > 0,
                "reservation expiry must publish its own cleanup wake or submit its worker"
            );
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while router.test_client_holder_count("cleanup-connection") != 0
            || daemon.runtime().unwrap().host_executor().outstanding() != 0
        {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                Instant::now() < deadline,
                "the cleanup wake must retire the holder"
            );
            thread::yield_now();
        }
        let permit = state.budget.take_peer_permit("cleanup-connection").unwrap();
        state.budget.release(permit);
        daemon.stop();
        std::fs::remove_dir_all(root).expect("remove client cleanup test directory");
    }

    #[test]
    fn client_event_cleanup_disconnect_retains_work_at_full_host_capacity() {
        use crate::transport::unix::connection::{
            ConnectionCleanupGuard, ConnectionTerminalReason,
        };

        let root = unique_package_control_dir("client-event-cleanup-full-host");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
            .expect("start client cleanup daemon");
        let mut state = DaemonControlState::default();
        let connection_permit = state
            .budget
            .reserve_connection()
            .expect("connection permit");
        admit_cleanup_test_subscription(&daemon, &mut state);
        settle_cleanup_test_owner(&mut daemon, &mut state);
        let mut host_permits = (0..crate::host_executor::HOST_OPERATION_CAPACITY)
            .map(|_| {
                daemon
                    .runtime()
                    .unwrap()
                    .host_executor()
                    .try_reserve()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let (control_tx, mut control_rx) = tokio_mpsc::channel(1);
        let cleanup_permit = control_tx.clone().try_reserve_owned().unwrap();
        drop(ConnectionCleanupGuard::new(
            cleanup_permit,
            "cleanup-connection".into(),
            ConnectionTerminalReason::NormalClose,
            connection_permit,
        ));
        let ControlMessage::ConnectionCleanup(cleanup) = control_rx.try_recv().unwrap() else {
            panic!("the connection guard must send its cleanup message");
        };
        handle_connection_cleanup(&mut daemon, &mut state, control_tx, cleanup);
        let router = daemon.runtime().unwrap().package_event_router().clone();
        assert_eq!(
            router.test_client_holder_count("cleanup-connection"),
            1,
            "full host capacity must retain the router holder for worker cleanup"
        );
        for _ in 0..3 {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert_eq!(
                state.budget.outstanding(),
                1,
                "event cleanup retains the connection permit"
            );
        }
        drop(host_permits.pop());
        let deadline = Instant::now() + Duration::from_secs(5);
        while state.budget.outstanding() != 0 {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                Instant::now() < deadline,
                "host capacity must resume connection cleanup"
            );
            thread::yield_now();
        }
        assert_eq!(router.test_client_holder_count("cleanup-connection"), 0);
        assert!(
            state
                .event_plane
                .subscription_mailbox("cleanup-connection", "cleanup-subscription")
                .is_none()
        );
        drop(host_permits);
        daemon.stop();
        std::fs::remove_dir_all(root).expect("remove client cleanup test directory");
    }

    #[test]
    fn package_event_cleanup_reuses_its_slot_at_full_host_capacity() {
        for (disconnected, contended, stale) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let root = unique_package_control_dir(&format!(
                "event-cleanup-full-{disconnected}-{contended}-{stale}"
            ));
            let package_dir = root.join("cleanup.plugin");
            write_package_control_manifest(&package_dir, "cleanup.plugin", serde_json::json!({}));
            let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
                .expect("start package cleanup daemon");
            drive_package_request(
                &mut daemon,
                DaemonRequest::InstallPackageLocalPath { path: package_dir },
            )
            .expect("install cleanup package");
            drive_package_request(
                &mut daemon,
                DaemonRequest::EnablePackage {
                    package_name: "cleanup.plugin".into(),
                },
            )
            .expect("enable cleanup package");
            let scopes = daemon.runtime().unwrap().causal_scopes().clone();
            let scope = scopes.mint().unwrap();
            for seq in [1, 2] {
                daemon.runtime().unwrap().test_store_family_payload(
                    crate::package_entity_fanout::PackageEntityMutation::Upsert {
                        admission: None,
                        entity_type: "cleanup.plugin.item".into(),
                        snapshot_seq: seq,
                        id: "item".into(),
                        entity: serde_json::json!({"id": "item"}),
                    },
                );
                assert!(
                    scopes.acquire(
                        scope,
                        crate::package_event_router::LeaseIdentity::AdmittedEntityMutation {
                            family_token: daemon
                                .runtime()
                                .unwrap()
                                .test_family_causal_token("cleanup.plugin.item"),
                            seq,
                        }
                    )
                );
                daemon.runtime().unwrap().test_store_pending_lease(
                    scope,
                    "cleanup.plugin.item",
                    seq,
                );
            }
            let mut state = DaemonControlState::default();
            let reply = start_async_control_request(
                &mut daemon,
                &mut state,
                DaemonRequest::DisablePackage {
                    package_name: "cleanup.plugin".into(),
                },
                "cleanup-client",
                "cleanup-request",
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let retained = state.host_completions.values().any(|completion| matches!(
                    &completion.result,
                    crate::host_executor::HostResult::Mutation(crate::host_mutations::HostMutationResult::PackageEffectApplied { cleanup, .. })
                        if !cleanup.event_plane_unloads.is_empty()
                ));
                if retained {
                    break;
                }
                publish_completion_wakes(&daemon, &mut state);
                publish_maintenance_wakes(&mut state);
                if let Some(item) = state.owner_ready.pop_next() {
                    let mut budget =
                        crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
                    assert!(!dispatch_owner_ready_item(
                        &mut daemon,
                        &mut state,
                        item,
                        &mut budget
                    ));
                }
                assert!(
                    Instant::now() < deadline,
                    "the package effect must return its cleanup identities"
                );
                thread::yield_now();
            }
            let waiter = *state
                .host_completions
                .keys()
                .next()
                .expect("retained package result");
            let reserved = (1..crate::host_executor::HOST_OPERATION_CAPACITY)
                .map(|_| {
                    daemon
                        .runtime()
                        .unwrap()
                        .host_executor()
                        .try_reserve()
                        .expect("reserve every other Host slot")
                })
                .collect::<Vec<_>>();
            assert!(
                daemon
                    .runtime()
                    .unwrap()
                    .host_executor()
                    .try_reserve()
                    .is_none()
            );
            let mut reply = Some(reply);
            if disconnected {
                drop(reply.take());
                crate::daemon::control::pending::retire_abandoned_requests(
                    &mut daemon,
                    &mut state,
                    "cleanup-client",
                );
            }
            if stale {
                loop {
                    if let Some(completion) = state.host_completions.get_mut(&waiter)
                        && matches!(
                            completion.result,
                            crate::host_executor::HostResult::EntityModelComplete(
                                crate::runtime::entity_model::Kind::CleanupDetached
                            )
                        )
                    {
                        completion.identity = completion.identity.next_phase().unwrap();
                        break;
                    }
                    publish_completion_wakes(&daemon, &mut state);
                    publish_maintenance_wakes(&mut state);
                    if let Some(item) = state.owner_ready.pop_next() {
                        let mut budget =
                            crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
                        assert!(!dispatch_owner_ready_item(
                            &mut daemon,
                            &mut state,
                            item,
                            &mut budget
                        ));
                    }
                    assert!(
                        Instant::now() < deadline,
                        "family completion must reach its waiter"
                    );
                    thread::yield_now();
                }
            }
            if contended {
                scopes.test_with_inner_held(|| {
                    for _ in 0..crate::runtime::CAUSAL_OWNER_CAPACITY {
                        assert_eq!(
                            daemon.runtime().unwrap().admit_causal_op(
                                crate::package_event_router::CausalOp::Release {
                                    scope_id: u64::MAX,
                                    identity:
                                        crate::package_event_router::LeaseIdentity::EventInFlight,
                                }
                            ),
                            crate::package_event_router::CausalAdmitResult::Applied
                        );
                    }
                    while !state.family_cleanup_waiters.contains_key(&waiter) {
                        assert!(
                            Instant::now() < deadline,
                            "cleanup must park on rejected release"
                        );
                        assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                        thread::yield_now();
                    }
                    for _ in 0..20 {
                        assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                    }
                    let unexpected = state.owner_ready.pop_next();
                    assert!(
                        unexpected.is_none(),
                        "lock contention must not poll the cleanup: {:?}",
                        unexpected.map(|item| (
                            item,
                            state.background_core_waiters.get(&item.key().waiter_id())
                        ))
                    );
                    assert!(state.pending_requests.contains_key(&waiter));
                    assert!(state.family_cleanup_waiters.contains_key(&waiter));
                    assert!(state.host_recovery.is_empty());
                    assert_eq!(state.budget.outstanding(), 1);
                    assert!(
                        daemon
                            .runtime()
                            .unwrap()
                            .host_executor()
                            .try_reserve()
                            .is_none()
                    );
                });
            }
            while state.pending_requests.contains_key(&waiter) {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    Instant::now() < deadline,
                    "cleanup must finish with no second Host admission"
                );
                thread::yield_now();
            }
            if stale {
                let Some(
                    crate::daemon::control::host_work::HostRecoveryRequired::PackageFamilyWork {
                        owner_permit: Some(_),
                        _work,
                        ..
                    },
                ) = state.host_recovery.get(&waiter)
                else {
                    panic!("stale completion must retain cleanup and both permits");
                };
                assert!(_work.test_retained_release());
                assert!(scopes.is_live(scope));
                assert_eq!(state.budget.outstanding(), 1);
                assert!(
                    daemon
                        .runtime()
                        .unwrap()
                        .host_executor()
                        .try_reserve()
                        .is_none()
                );
                let response = receive_test_control_reply(reply.take().unwrap()).unwrap();
                assert!(response.error.is_some());
                drop(reserved);
                daemon.stop();
                std::fs::remove_dir_all(root).unwrap();
                continue;
            }
            assert!(
                !daemon
                    .runtime()
                    .unwrap()
                    .test_family_exists("cleanup.plugin.item")
            );
            assert!(state.host_recovery.is_empty());
            assert!(state.family_cleanup_waiters.is_empty());
            while daemon.runtime().unwrap().causal_operation_count() > 0
                || state.package_entity_resync_scan.test_running()
            {
                assert!(Instant::now() < deadline, "admitted releases must drain");
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            }
            assert!(!scopes.is_live(scope));
            assert!(state.document_owner.is_none());
            assert_eq!(state.budget.outstanding(), 0);
            if let Some(reply) = reply {
                let response = receive_test_control_reply(reply).expect("disable response");
                assert!(response.error.is_none());
            }
            assert!(
                daemon
                    .runtime()
                    .unwrap()
                    .host_executor()
                    .try_reserve()
                    .is_some()
            );
            drop(reserved);
            daemon.stop();
            std::fs::remove_dir_all(root).expect("remove package cleanup test directory");
        }
    }

    #[test]
    fn terminal_package_cleanup_recovery_keeps_owner_admission_for_accepted_requests() {
        for family_failure in [false, true] {
            let root = unique_package_control_dir(if family_failure {
                "family-recovery-owner-admission"
            } else {
                "event-recovery-owner-admission"
            });
            let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
                .expect("start event recovery daemon");
            for name in ["alpha.plugin", "beta.plugin"] {
                let package_dir = root.join(name);
                write_package_control_manifest(&package_dir, name, serde_json::json!({}));
                drive_package_request(
                    &mut daemon,
                    DaemonRequest::InstallPackageLocalPath { path: package_dir },
                )
                .expect("install package");
                drive_package_request(
                    &mut daemon,
                    DaemonRequest::EnablePackage {
                        package_name: name.into(),
                    },
                )
                .expect("enable package");
            }
            let mut state = DaemonControlState::default();
            state.budget = crate::daemon::owner_budget::OwnerBudget::with_capacity(2);
            let first_reply = start_async_control_request(
                &mut daemon,
                &mut state,
                DaemonRequest::DisablePackage {
                    package_name: "alpha.plugin".into(),
                },
                "alpha-client",
                "alpha-request",
            );
            let second_reply = start_async_control_request(
                &mut daemon,
                &mut state,
                DaemonRequest::DisablePackage {
                    package_name: "beta.plugin".into(),
                },
                "beta-client",
                "beta-request",
            );
            assert_eq!(state.budget.outstanding(), 2);
            assert_eq!(state.pending_requests.len(), 2);
            if family_failure {
                daemon
                    .runtime()
                    .unwrap()
                    .test_exhaust_package_entity_epochs();
            } else {
                let router = daemon.runtime().unwrap().package_event_router().clone();
                let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    router.test_with_inner_held(|| panic!("inject terminal event router failure"));
                }));
                assert!(poisoned.is_err());
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while state.host_recovery.is_empty() {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    Instant::now() < deadline,
                    "the accepted cleanup must report terminal recovery"
                );
                thread::yield_now();
            }
            assert!(state.host_recovery.values().any(|recovery| if family_failure {
            matches!(recovery, crate::daemon::control::host_work::HostRecoveryRequired::PackageFamilies {
                owner_permit: Some(_), ..
            })
        } else { matches!(
            recovery,
            crate::daemon::control::host_work::HostRecoveryRequired::PackageEvents {
                owner_permit: Some(_),
                ..
            }
        ) }));
            assert_eq!(
                state.pending_requests.len(),
                1,
                "the other accepted mutation retains its document wait"
            );
            assert_eq!(state.budget.outstanding(), 2);
            assert!(
                state.budget.reserve().is_none(),
                "terminal recovery must not free accepted Owner capacity"
            );
            drop(first_reply);
            drop(second_reply);
            for client in ["alpha-client", "beta-client"] {
                crate::daemon::control::pending::retire_abandoned_requests(
                    &mut daemon,
                    &mut state,
                    client,
                );
            }
            assert_eq!(state.budget.outstanding(), 2);
            assert_eq!(state.host_recovery.len(), 1);
            assert_eq!(state.pending_requests.len(), 1);
            let expected_code = if family_failure {
                "entity_family_generation_exhausted"
            } else {
                "event_plane_cleanup_failed"
            };
            for _ in 0..crate::host_executor::HOST_OPERATION_CAPACITY {
                let response = drive_package_request_with_state(
                    &mut daemon,
                    &mut state,
                    DaemonRequest::DisablePackage {
                        package_name: "alpha.plugin".into(),
                    },
                )
                .expect("new package work must fail before Host admission");
                assert_eq!(response.error.expect("recovery error").code, expected_code);
                assert_eq!(state.host_recovery.len(), 1);
                assert_eq!(state.budget.outstanding(), 2);
            }
            for request in [DaemonRequest::Status, DaemonRequest::DaemonShutdown] {
                assert!(
                    crate::daemon::control::host_work::recovery_response(&state, &request)
                        .is_none()
                );
            }
            let other_host_slots = (2..crate::host_executor::HOST_OPERATION_CAPACITY)
                .map(|_| {
                    daemon
                        .runtime()
                        .unwrap()
                        .host_executor()
                        .try_reserve()
                        .expect("unrelated Host slot")
                })
                .collect::<Vec<_>>();
            assert!(
                daemon
                    .runtime()
                    .unwrap()
                    .host_executor()
                    .try_reserve()
                    .is_none()
            );
            drop(other_host_slots);
            drop(state);
            daemon.stop();
            std::fs::remove_dir_all(root).expect("remove event recovery test directory");
        }
    }

    #[test]
    fn shutdown_waits_for_queued_event_worker_cleanup() {
        use crate::package_event_router::{OwnerOp, OwnerOpKind};

        let root = unique_package_control_dir("shutdown-event-owner");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
            .expect("start queued cleanup shutdown daemon");
        let mut state = DaemonControlState::default();
        let router = daemon.runtime().unwrap().package_event_router().clone();
        daemon
            .runtime()
            .unwrap()
            .record_event_plane_owner_op(OwnerOp {
                kind: OwnerOpKind::Unload,
                owner: "queued".into(),
                generation: 0,
            });
        let mut reply = None;
        router.test_with_inner_held(|| {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(!daemon.runtime().unwrap().event_plane_owner_op_ready());
            let mut response = start_async_control_request(
                &mut daemon,
                &mut state,
                DaemonRequest::DaemonShutdown,
                "shutdown-client",
                "shutdown-request",
            );
            let waiter = state.shutdown_waiter.expect("accepted shutdown");
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                assert!(
                    !drive_ready_test_turn(&mut daemon, &mut state),
                    "shutdown must not finish before the event worker"
                );
                assert!(matches!(
                    response.try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                ));
                if state
                    .pending_requests
                    .get(&waiter)
                    .is_some_and(|entry| entry.last_core_phase > 0)
                    && state.owner_ready.is_empty()
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "shutdown must reach its cleanup wait"
                );
                thread::yield_now();
            }
            assert!(daemon.runtime().unwrap().event_plane_owner_ops_pending());
            // The other worker must pass every earlier job before this barrier starts.
            let barrier = std::sync::Arc::new(crate::host_executor::TestHostGate::default());
            barrier.release();
            let executor = daemon.runtime().unwrap().host_executor();
            executor
                .submit(
                    crate::host_executor::HostJobIdentity::first(state.waiter_ids.next().unwrap()),
                    crate::host_executor::HostCommand::Wait {
                        generation: 0,
                        gate: barrier.clone(),
                    },
                    executor.try_reserve().expect("reserve the worker barrier"),
                )
                .expect("submit the worker barrier");
            while !barrier.has_started() {
                assert!(
                    Instant::now() < deadline,
                    "the free worker must reach the barrier"
                );
                thread::yield_now();
            }
            assert!(
                !drive_ready_test_turn(&mut daemon, &mut state),
                "the free worker must not finish shutdown before cleanup"
            );
            assert!(matches!(
                response.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            reply = Some(response);
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while !drive_ready_test_turn(&mut daemon, &mut state) {
            assert!(
                Instant::now() < deadline,
                "event completion must wake shutdown"
            );
            thread::yield_now();
        }
        let response = receive_test_control_reply(reply.unwrap()).expect("shutdown response");
        assert_eq!(response.kind, DaemonResponseKind::Shutdown);
        assert!(!daemon.runtime().unwrap().event_plane_owner_ops_pending());
        assert_eq!(state.budget.outstanding(), 0);
        daemon.stop();
        std::fs::remove_dir_all(root).expect("remove queued cleanup shutdown test directory");
    }

    #[test]
    fn event_owner_waits_for_capacity_and_worker_completion_without_spinning() {
        use crate::package_event_router::{OwnerOp, OwnerOpKind};

        for capacity in ["host", "owner"] {
            let root = unique_package_control_dir(&format!("event-owner-capacity-{capacity}"));
            let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
                .expect("start queued event cleanup daemon");
            let mut state = DaemonControlState::default();
            state.budget = crate::daemon::owner_budget::OwnerBudget::with_capacity(1);
            let mut owner_permit = (capacity == "owner").then(|| state.budget.reserve().unwrap());
            let mut host_permits = if capacity == "host" {
                (0..crate::host_executor::HOST_OPERATION_CAPACITY)
                    .map(|_| {
                        daemon
                            .runtime()
                            .unwrap()
                            .host_executor()
                            .try_reserve()
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            for _ in 0..2 {
                daemon
                    .runtime()
                    .unwrap()
                    .record_event_plane_owner_op(OwnerOp {
                        kind: OwnerOpKind::Unload,
                        owner: "queued".into(),
                        generation: 0,
                    });
            }
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert_eq!(state.event_owner.waiting_for_host, capacity == "host");
            assert_eq!(state.event_owner.waiting_for_owner, capacity == "owner");
            assert!(
                state.owner_ready.is_empty(),
                "capacity waiting must not retain a runnable item"
            );

            let router = daemon.runtime().unwrap().package_event_router().clone();
            router.test_with_inner_held(|| {
                if let Some(permit) = owner_permit.take() {
                    state.budget.release(permit);
                }
                drop(host_permits.pop());
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(!daemon.runtime().unwrap().event_plane_owner_op_ready());
                assert!(daemon.runtime().unwrap().event_plane_owner_ops_pending());
                assert_eq!(state.budget.outstanding(), 1);
                for _ in 0..3 {
                    assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                    assert!(
                        state.owner_ready.is_empty(),
                        "an unfinished worker must not make the owner runnable"
                    );
                }
            });
            let deadline = Instant::now() + Duration::from_secs(5);
            while daemon.runtime().unwrap().event_plane_owner_ops_pending() {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    Instant::now() < deadline,
                    "completion must resume the next queued operation"
                );
                thread::yield_now();
            }
            assert_eq!(state.budget.outstanding(), 0);
            drop(host_permits);
            daemon.stop();
            std::fs::remove_dir_all(root).expect("remove queued cleanup test directory");
        }
    }

    fn retain_family_waiter_for_wake_test(state: &mut DaemonControlState, id: u64) {
        use crate::daemon::control::pending::{OwnerRequestCompletion, PendingControlRequest};
        let waiter_id = crate::owner_identity::WaiterId(id);
        let (reply_tx, _reply) = crate::daemon::control::message::control_reply_channel();
        let permit = state.budget.reserve().unwrap();
        state.pending_requests.insert(
            waiter_id,
            PendingControlRequest {
                waiter_id,
                ready_class: crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                ready_key: None,
                deadline_key: None,
                last_core_phase: 0,
                last_host_phase: 0,
                completion: OwnerRequestCompletion::default(),
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client: None,
                core_retirement: None,
                permit: Some(permit),
                must_finish: true,
                past_deadline: false,
                continuation: crate::daemon::control::pending::ControlContinuation::callback(
                    |_, _| crate::daemon::control::pending::ControlPoll::Pending,
                ),
                retire: None,
            },
        );
        state.family_cleanup_waiters.insert(waiter_id, 1);
    }

    #[test]
    fn causal_wake_pass_preserves_its_cursor_and_upper_bound_during_reinsertion() {
        use crate::owner_identity::WaiterId;
        let root = unique_package_control_dir("causal-wake-reinsertion");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        for id in [10, 20, 30] {
            retain_family_waiter_for_wake_test(&mut state, id);
        }
        state.causal_wake_active = true;
        state.causal_wake_through = causal_waiter_upper_bound(&state);
        assert!(mark_background_ready(
            &mut state,
            BackgroundWork::CausalProgress
        ));
        let item = state.owner_ready.pop_next().unwrap();
        let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
        run_background_ready_item(&mut daemon, &mut state, item, &mut budget);
        assert_eq!(state.causal_wake_after, Some(WaiterId(10)));
        let key = state
            .pending_requests
            .get_mut(&WaiterId(10))
            .unwrap()
            .ready_key
            .take()
            .unwrap();
        assert!(state.owner_ready.remove(key));
        state.family_cleanup_waiters.insert(WaiterId(10), 2);
        retain_family_waiter_for_wake_test(&mut state, 40);
        let runtime = daemon.runtime().unwrap();
        assert!(matches!(
            runtime.admit_causal_op(crate::package_event_router::CausalOp::Release {
                scope_id: u64::MAX,
                identity: crate::package_event_router::LeaseIdentity::EventInFlight,
            }),
            crate::package_event_router::CausalAdmitResult::Applied
        ));
        runtime.apply_causal_owner_ops();
        publish_completion_wakes(&daemon, &mut state);
        assert!(state.causal_wake_again);
        assert_eq!(state.causal_wake_through, Some(WaiterId(30)));
        for expected in [20, 30] {
            run_background_ready_item(&mut daemon, &mut state, item, &mut budget);
            assert_eq!(state.causal_wake_after, Some(WaiterId(expected)));
            assert!(
                !state
                    .family_cleanup_waiters
                    .contains_key(&WaiterId(expected))
            );
        }
        assert!(state.family_cleanup_waiters.contains_key(&WaiterId(10)));
        assert!(state.family_cleanup_waiters.contains_key(&WaiterId(40)));
        run_background_ready_item(&mut daemon, &mut state, item, &mut budget);
        assert_eq!(state.causal_wake_after, None);
        assert_eq!(state.causal_wake_through, Some(WaiterId(40)));
        for expected in [10, 40] {
            run_background_ready_item(&mut daemon, &mut state, item, &mut budget);
            assert_eq!(state.causal_wake_after, Some(WaiterId(expected)));
        }
        assert!(state.family_cleanup_waiters.is_empty());
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn causal_wake_retains_a_live_waiter_when_ready_serials_are_exhausted() {
        use crate::owner_identity::WaiterId;
        let root = unique_package_control_dir("causal-wake-serial-exhaustion");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        retain_family_waiter_for_wake_test(&mut state, 10);
        state.causal_wake_active = true;
        state.causal_wake_through = Some(WaiterId(10));
        assert!(mark_background_ready(
            &mut state,
            BackgroundWork::CausalProgress
        ));
        let item = state.owner_ready.pop_next().unwrap();
        state.owner_ready =
            crate::daemon::owner_schedule::ReadyQueues::with_next_enqueue_serial(u64::MAX);
        let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
        run_background_ready_item(&mut daemon, &mut state, item, &mut budget);
        assert!(state.pending_requests.contains_key(&WaiterId(10)));
        assert!(state.family_cleanup_waiters.contains_key(&WaiterId(10)));
        assert!(state.pending_requests[&WaiterId(10)].ready_key.is_none());
        assert_eq!(state.budget.outstanding(), 1);
        assert_eq!(
            state.causal_wake_after,
            Some(WaiterId(10)),
            "the current pass must not poll the same failed waiter"
        );
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shutdown_finishes_after_publication_continuation() {
        let root = unique_package_control_dir("publication-continuation-shutdown");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let (runtime, provider_root) =
            crate::runtime::tests::publication_provider_runtime("shutdown-continuation");
        daemon.runtime = Some(runtime);
        let mut state = DaemonControlState::default();
        let frame = |seq| serde_json::json!({"type": "entity_remove", "entity_type": "producer.item", "snapshot_seq": seq, "id": "item"});
        for seq in 2..=16 {
            daemon
                .runtime()
                .unwrap()
                .test_admit_publish("producer", frame(seq), None)
                .unwrap();
        }
        let publication = daemon
            .runtime()
            .unwrap()
            .entity_publish_bridge()
            .test_queue_publish(botster_core::PluginKey("producer".into()), frame(1), None);
        assert!(crate::daemon::publication_owner::drive(&daemon, &mut state));
        assert!(publication.try_recv().is_err());
        let mut shutdown = start_async_control_request(
            &mut daemon,
            &mut state,
            DaemonRequest::DaemonShutdown,
            "publication-shutdown",
            "publication-shutdown",
        );
        assert!(shutdown.try_recv().is_err());
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            drive_ready_test_turn(&mut daemon, &mut state);
            if let Ok(reply) = shutdown.try_recv() {
                assert_eq!(
                    reply.into_parts().0.unwrap().kind,
                    DaemonResponseKind::Shutdown
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "shutdown must finish after the publication"
            );
            thread::yield_now();
        }
        let result = publication.try_recv().unwrap().unwrap();
        assert_eq!(result.last_accepted_seq, 16);
        assert!(
            !daemon
                .runtime()
                .unwrap()
                .entity_publish_retirement_pending()
        );
        drop(daemon);
        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(provider_root).unwrap();
    }

    #[test]
    fn publication_retraction_wakes_the_shutdown_waiter_without_a_completion() {
        let root = unique_package_control_dir("publication-retraction-shutdown");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        let waiter = crate::owner_identity::WaiterId(10);
        retain_family_waiter_for_wake_test(&mut state, 10);
        state.family_cleanup_waiters.clear();
        state.shutdown_waiter = Some(waiter);
        let bridge = daemon.runtime().unwrap().entity_publish_bridge();
        let _response = bridge.test_queue_stale_publish(botster_core::PluginKey("absent".into()),
            serde_json::json!({"type": "entity_remove", "entity_type": "absent.items", "snapshot_seq": 1, "id": "item"}), None);
        bridge.take_progress_notification();
        assert!(state.pending_requests[&waiter].ready_key.is_none());
        assert!(bridge.test_retract(1));
        assert_eq!(bridge.pending_publish_count(), 0);
        publish_completion_wakes(&daemon, &mut state);
        assert!(state.pending_requests[&waiter].ready_key.is_some());
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_wait_ignores_capacity_progress_until_causal_unlock() {
        use crate::package_event_router::{CausalAdmitResult, CausalOp, LeaseIdentity};
        let root = unique_package_control_dir("publication-table-wait");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let runtime = daemon.runtime().unwrap();
        let scopes = runtime.causal_scopes();
        let scope = scopes.mint().unwrap();
        for _ in 0..crate::runtime::CAUSAL_OWNER_CAPACITY - 1 {
            assert!(matches!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: u64::MAX,
                    identity: LeaseIdentity::EventInFlight,
                }),
                CausalAdmitResult::Applied
            ));
        }
        let bridge = runtime.entity_publish_bridge();
        let response = bridge.test_queue_stale_publish(
            botster_core::PluginKey("absent".into()),
            serde_json::json!({"type": "entity_remove", "entity_type": "absent.items", "snapshot_seq": 1, "id": "item"}),
            Some(scope),
        );
        scopes.test_with_inner_held(|| {
            runtime.step_entity_publish();
            assert_eq!(bridge.pending_publish_count(), 1);
            assert!(response.try_recv().is_err());
            assert!(!runtime.entity_publish_ready());
            let capacity = runtime.take_causal_capacity_notification();
            assert!(capacity, "the unused final reservation returns capacity");
            runtime.note_entity_publish_progress(false, capacity);
            assert!(
                !runtime.entity_publish_ready(),
                "capacity must not retry a table-lock wait"
            );
            assert!(!scopes.take_progress_notification());
        });
        let progress = scopes.take_progress_notification();
        assert!(progress);
        runtime.note_entity_publish_progress(progress, false);
        assert!(runtime.entity_publish_ready());
        runtime.step_entity_publish();
        assert_eq!(bridge.pending_publish_count(), 0);
        assert!(
            response.try_recv().unwrap().is_err(),
            "the retired registration rejects after acquisition"
        );
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert!(!scopes.is_live(scope));
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_capacity_wait_enters_fault_retention_on_table_progress() {
        use crate::package_event_router::{CausalAdmitResult, CausalOp, LeaseIdentity};
        let root = unique_package_control_dir("publication-capacity-fault");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        let runtime = daemon.runtime().unwrap();
        let scopes = runtime.causal_scopes().clone();
        for _ in 0..crate::runtime::CAUSAL_OWNER_CAPACITY {
            assert!(matches!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: u64::MAX,
                    identity: LeaseIdentity::EventInFlight,
                }),
                CausalAdmitResult::Applied
            ));
        }
        let bridge = runtime.entity_publish_bridge();
        let _response = bridge.test_queue_stale_publish(
            botster_core::PluginKey("absent".into()),
            serde_json::json!({"type": "entity_remove", "entity_type": "absent.items", "snapshot_seq": 1, "id": "item"}),
            None,
        );
        runtime.step_entity_publish();
        assert!(!runtime.entity_publish_ready());
        assert_eq!(bridge.pending_publish_count(), 1);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            scopes.test_with_inner_held(|| panic!("inject causal table fault"));
        }));
        publish_completion_wakes(&daemon, &mut state);
        assert!(daemon.runtime().unwrap().entity_publish_ready());
        daemon.runtime().unwrap().step_entity_publish();
        assert!(!daemon.runtime().unwrap().entity_publish_ready());
        let refusal = bridge.test_queue_stale_publish(
            botster_core::PluginKey("absent".into()),
            serde_json::json!({"type": "entity_remove", "entity_type": "absent.items", "snapshot_seq": 1, "id": "item"}),
            None,
        );
        assert!(refusal.try_recv().unwrap().is_err());
        assert_eq!(
            bridge.pending_publish_count(),
            1,
            "fault retention preserves the original queued source"
        );
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn causal_operation_waits_for_its_metadata_budget_without_removing_the_head() {
        use crate::daemon::owner_turn::{
            OWNER_TURN_INSPECTED_BYTE_LIMIT, OwnerTurnBudget, OwnerTurnCharge,
        };
        use crate::package_event_router::{CausalAdmitResult, CausalOp, LeaseIdentity};
        let root = unique_package_control_dir("causal-metadata-budget");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        let mut state = DaemonControlState::default();
        let runtime = daemon.runtime().unwrap();
        let scopes = runtime.causal_scopes().clone();
        let scope_id = scopes
            .mint_with_lease(Some(LeaseIdentity::EventInFlight))
            .unwrap();
        assert_eq!(
            runtime.admit_causal_op(CausalOp::Release {
                scope_id,
                identity: LeaseIdentity::EventInFlight,
            }),
            CausalAdmitResult::Applied
        );
        assert!(mark_background_ready(
            &mut state,
            BackgroundWork::CausalDrain
        ));
        let ready = state.owner_ready.pop_next().unwrap();
        let operation_bytes = std::mem::size_of::<CausalOp>();
        let now = Instant::now();
        let mut budget = OwnerTurnBudget::new(now);
        let initial_bytes = OWNER_TURN_INSPECTED_BYTE_LIMIT - operation_bytes + 1;
        budget
            .try_charge(now, OwnerTurnCharge::inspection(initial_bytes))
            .unwrap();
        assert!(run_background_ready_item(
            &mut daemon,
            &mut state,
            ready,
            &mut budget
        ));
        assert_eq!(budget.spent_inspected_bytes(), initial_bytes);
        assert_eq!(daemon.runtime().unwrap().causal_operation_count(), 1);
        assert_eq!(scopes.lease_count(scope_id), Some(1));

        let ready = state
            .owner_ready
            .pop_next()
            .expect("the deferred drain remains ready");
        let mut next_turn = OwnerTurnBudget::new(Instant::now());
        assert!(run_background_ready_item(
            &mut daemon,
            &mut state,
            ready,
            &mut next_turn
        ));
        assert_eq!(next_turn.spent_inspected_bytes(), operation_bytes);
        assert_eq!(daemon.runtime().unwrap().causal_operation_count(), 0);
        assert_eq!(scopes.lease_count(scope_id), None);
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn causal_operations_share_the_owner_budget_without_control_traffic() {
        use crate::package_event_router::{CausalAdmitResult, CausalOp, LeaseIdentity};

        let root = unique_package_control_dir("causal-owner-budget");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
            .expect("start causal owner test daemon");
        let mut state = DaemonControlState::default();
        let runtime = daemon.runtime().expect("runtime");
        let scopes = runtime.causal_scopes().clone();
        let ids: Vec<_> = (0..100)
            .map(|_| {
                let identity = LeaseIdentity::EventInFlight;
                let scope_id = scopes.mint_with_lease(Some(identity.clone())).unwrap();
                assert!(matches!(
                    runtime.admit_causal_op(CausalOp::Release { scope_id, identity }),
                    CausalAdmitResult::Applied
                ));
                scope_id
            })
            .collect();
        publish_completion_wakes(&daemon, &mut state);
        publish_maintenance_wakes(&mut state);
        let now = Instant::now();
        let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(now);
        let mut remaining = ids.len();
        while budget
            .try_charge(
                now,
                crate::daemon::owner_turn::OwnerTurnCharge::opaque_move(),
            )
            .is_ok()
        {
            let item = state
                .owner_ready
                .pop_next()
                .expect("causal work remains ready");
            dispatch_owner_ready_item(&mut daemon, &mut state, item, &mut budget);
            let next = ids
                .iter()
                .filter(|id| scopes.identities(**id).is_some())
                .count();
            assert!(
                remaining - next <= 1,
                "one ready item releases at most one scope"
            );
            remaining = next;
        }
        assert_eq!(
            budget.spent_items(),
            crate::daemon::owner_turn::OWNER_TURN_ITEM_LIMIT
        );
        assert!(remaining > 0 && remaining < ids.len());
        let deadline = Instant::now() + Duration::from_secs(3);
        while daemon.runtime().unwrap().causal_owner_ops_pending() {
            drive_ready_test_turn(&mut daemon, &mut state);
            assert!(
                Instant::now() < deadline,
                "causal work must finish without control traffic"
            );
        }
        assert!(ids.iter().all(|id| scopes.identities(*id).is_none()));
        daemon.stop();
        std::fs::remove_dir_all(root).expect("remove causal owner test directory");
    }

    #[test]
    fn queued_event_owner_operations_resume_without_control_traffic() {
        use crate::package_event_router::{OwnerOp, OwnerOpKind};

        let root = unique_package_control_dir("event-owner-wake");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
            .expect("start event owner test daemon");
        let mut state = DaemonControlState::default();
        let runtime = daemon.runtime().expect("runtime");
        runtime.package_event_router().test_with_inner_held(|| {
            for generation in 1..=1_000 {
                runtime.record_event_plane_owner_op(OwnerOp {
                    kind: OwnerOpKind::Reload,
                    owner: "queued".into(),
                    generation,
                });
            }
        });
        assert!(runtime.event_plane_owner_ops_pending());
        drive_ready_test_turn(&mut daemon, &mut state);
        assert!(
            daemon.runtime().unwrap().event_plane_owner_ops_pending(),
            "one owner turn cannot drain all queued operations"
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while daemon.runtime().unwrap().event_plane_owner_ops_pending() {
            drive_ready_test_turn(&mut daemon, &mut state);
            assert!(Instant::now() < deadline, "queued operations must finish");
            thread::yield_now();
        }
        daemon.stop();
        std::fs::remove_dir_all(root).expect("remove event owner test directory");
    }

    fn write_hello(client: &mut UnixStream) {
        write_client_frame(
            client,
            &ClientFrame::Hello {
                hello: DaemonHello {
                    protocol: PROTOCOL.to_string(),
                    compatibility: DaemonCompatibilityRequirement::current(),
                    terminal_compatibility: None,
                },
            },
        )
        .expect("write daemon hello");
    }

    fn write_request(client: &mut UnixStream, request_id: u64, request: DaemonRequest) {
        write_client_frame(
            client,
            &ClientFrame::Request {
                request_id: request_id.to_string(),
                request,
            },
        )
        .expect("write client request");
    }

    fn read_hello_ack(
        client: &mut UnixStream,
        reader: &mut DaemonUnixFrameReader,
    ) -> DaemonHelloAck {
        match reader.read_frame(client).expect("read daemon hello ack") {
            DaemonUnixMuxFrame::Server(ServerFrame::HelloAck { ack }) => ack,
            other => panic!("expected hello ack, got {other:?}"),
        }
    }

    fn read_response(
        client: &mut UnixStream,
        reader: &mut DaemonUnixFrameReader,
        expected_request_id: u64,
    ) -> DaemonResponse {
        match reader.read_frame(client).expect("read daemon response") {
            DaemonUnixMuxFrame::Server(ServerFrame::Response {
                request_id,
                response,
            }) => {
                assert_eq!(request_id, expected_request_id.to_string());
                response
            }
            other => panic!("expected correlated response, got {other:?}"),
        }
    }

    fn read_terminal(
        client: &mut UnixStream,
        reader: &mut DaemonUnixFrameReader,
    ) -> DaemonUnixTerminalFrame {
        match reader.read_frame(client).expect("read terminal frame") {
            DaemonUnixMuxFrame::Terminal(frame) => frame,
            other => panic!("expected terminal container, got {other:?}"),
        }
    }

    #[test]
    fn due_deadline_precedes_an_already_ready_control_message() {
        let (control_tx, mut control_rx) = tokio_mpsc::channel(1);
        control_tx
            .try_send(ControlMessage::RejectedConnection)
            .expect("prefill owner control queue");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build owner event test runtime");

        assert!(matches!(
            runtime.block_on(receive_owner_event(&mut control_rx, Some(Instant::now()))),
            OwnerEvent::Reconcile
        ));
        let OwnerEvent::Control(message) = runtime.block_on(receive_owner_event(
            &mut control_rx,
            Some(Instant::now() + Duration::from_secs(1)),
        )) else {
            panic!("ready control message must win before a future reconciliation deadline");
        };
        assert!(matches!(*message, Some(ControlMessage::RejectedConnection)));
    }

    #[test]
    fn queued_control_precedes_ready_owner_work() {
        assert!(matches!(
            classify_owner_poll(Ok(ControlMessage::RejectedConnection), true),
            OwnerPollDecision::ServeControl(message)
                if matches!(*message, Some(ControlMessage::RejectedConnection))
        ));
        assert!(matches!(
            classify_owner_poll(Ok(ControlMessage::RejectedConnection), true),
            OwnerPollDecision::ServeControl(_)
        ));
    }

    #[test]
    fn deadline_collector_preserves_work_after_budget_exhaustion_and_yields_to_control() {
        let directory = unique_package_control_dir("deadline-collector");
        let mut daemon = HubDaemon::start(package_control_config(directory.clone()))
            .expect("start the collector test daemon");
        let mut state = DaemonControlState::default();
        assert!(arm_reservation_deadline(&mut state, "first".into(), 1, 0));
        assert!(arm_reservation_deadline(&mut state, "second".into(), 2, 0));
        assert!(mark_background_ready(&mut state, BackgroundWork::Deadline));
        let item = state
            .owner_ready
            .pop_next()
            .expect("the collector is ready");
        let now = Instant::now();
        let mut exhausted = crate::daemon::owner_turn::OwnerTurnBudget::new(now);
        exhausted
            .try_charge(
                now,
                crate::daemon::owner_turn::OwnerTurnCharge::inspection(
                    crate::daemon::owner_turn::OWNER_TURN_INSPECTED_BYTE_LIMIT,
                ),
            )
            .expect("consume the byte budget");

        assert!(run_background_ready_item(
            &mut daemon,
            &mut state,
            item,
            &mut exhausted
        ));
        assert!(
            state
                .reservation_deadlines
                .values()
                .all(|entry| entry.ready_key.is_none())
        );
        enqueue_control_message(&mut state, ControlMessage::RejectedConnection)
            .expect("queue control work");
        loop {
            let ready = state.owner_ready.pop_next().expect("control work is ready");
            assert_ne!(
                ready.key().class(),
                crate::daemon::owner_schedule::ReadyClass::Deadline
            );
            if ready.key().class() == crate::daemon::owner_schedule::ReadyClass::ControlIngress {
                break;
            }
        }
        let item = loop {
            let item = state
                .owner_ready
                .pop_next()
                .expect("the collector remains ready");
            if item.key().class() == crate::daemon::owner_schedule::ReadyClass::Deadline {
                break item;
            }
        };
        let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
        assert!(run_background_ready_item(
            &mut daemon,
            &mut state,
            item,
            &mut budget
        ));
        assert_eq!(
            state
                .reservation_deadlines
                .values()
                .filter(|entry| entry.ready_key.is_some())
                .count(),
            1
        );
        assert!(state.deadlines.has_due(Instant::now()));

        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove the collector test directory");
    }

    #[test]
    fn only_a_productive_partial_completion_drain_self_rearms() {
        assert!(!completion_drain_needs_followup(
            crate::daemon_maintenance::CompletionDrainProgress {
                item_count: 0,
                has_remaining: true,
            }
        ));
        assert!(completion_drain_needs_followup(
            crate::daemon_maintenance::CompletionDrainProgress {
                item_count: 1,
                has_remaining: true,
            }
        ));
        assert!(!completion_drain_needs_followup(
            crate::daemon_maintenance::CompletionDrainProgress {
                item_count: 1,
                has_remaining: false,
            }
        ));
    }

    #[test]
    fn read_mode_flags_path_does_not_observe_lifecycle() {
        const TRANSPORT: &str = include_str!("owner_loop.rs");
        let deleted = ["fn observe_", "lifecycle_turn"].concat();
        assert!(
            !TRANSPORT.contains(&deleted),
            "broad operation-path observation must stay deleted"
        );
        let read_mode = TRANSPORT
            .split("DaemonRequest::ReadModeFlags")
            .nth(1)
            .expect("ReadModeFlags arm");
        let arm = read_mode.split("DaemonRequest::").next().expect("arm end");
        assert!(
            !arm.contains("observe_session_lifecycle"),
            "ReadModeFlags must not observe lifecycle"
        );
        assert!(
            !arm.contains("observe_lifecycle"),
            "ReadModeFlags must not call a lifecycle observe slice"
        );
    }

    #[test]
    fn status_and_read_mode_flags_do_not_mark_pump() {
        assert!(!should_mark_pump_after_control(
            &DaemonRequest::Status,
            true
        ));
        assert!(!should_mark_pump_after_control(
            &DaemonRequest::ReadModeFlags {
                session_id: "s".into(),
            },
            true
        ));
        assert!(!should_mark_pump_after_control(
            &DaemonRequest::ReadScreen {
                session_id: "s".into(),
            },
            true
        ));
        assert!(!should_mark_pump_after_control(
            &DaemonRequest::ListSessions,
            true
        ));
        assert!(should_mark_pump_after_control(
            &DaemonRequest::Attach {
                session_id: "s".into(),
                subscription_id: "sub".into(),
            },
            true
        ));
        assert!(!should_mark_pump_after_control(
            &DaemonRequest::Attach {
                session_id: "s".into(),
                subscription_id: "sub".into(),
            },
            false
        ));
        assert!(should_mark_pump_after_control(
            &DaemonRequest::RemoveSession {
                session_id: "s".into(),
            },
            false
        ));
        const TRANSPORT: &str = include_str!("owner_loop.rs");
        let production = TRANSPORT.split("mod tests").next().expect("production");
        assert!(
            !production.contains("prefer_close_events"),
            "close work must not rewrite the Pump phase pointer"
        );
        assert!(
            !production.contains("queue_unix_subscription_closed_events"),
            "control must not scan every Unix mux for close events"
        );
        assert!(
            !production.contains("queue_webrtc_subscription_closed_events"),
            "control must not scan every WebRTC mux for close events"
        );
        assert!(
            production.contains("should_mark_pump_after_control"),
            "control must mark Pump only through the documented request sources"
        );
    }

    #[test]
    fn pump_work_does_not_list_subscriptions_or_sessions() {
        const TRANSPORT: &str = include_str!("owner_loop.rs");
        let pump = TRANSPORT
            .split("fn run_inventory_reconcile_slice")
            .nth(1)
            .expect("inventory reconcile runner");
        let pump = pump
            .split("pub(crate) struct DaemonControlState")
            .next()
            .unwrap_or(pump);
        assert!(
            !pump.contains("run_close_events_phase"),
            "Pump region must not contain the retired close-events phase"
        );
        assert!(
            pump.contains("run_inventory_reconcile_phase"),
            "Pump region must keep inventory reconcile"
        );
        assert!(
            !pump.contains("list_terminal_subscriptions"),
            "Pump must use the exact membership query"
        );
        assert!(
            !pump.contains("list_sessions"),
            "Pump close classification must not list sessions"
        );
        assert!(
            !pump.contains("observe_session_lifecycle"),
            "Observe must not mutate lifecycle through the retired session API"
        );
    }

    #[test]
    fn unix_listener_connection_and_mux_left_daemon_transport() {
        const TRANSPORT: &str = include_str!("owner_loop.rs");
        let production = TRANSPORT.split("mod tests").next().expect("production");
        let needles = [
            "fn accept_connections(",
            "async fn handle_connection_async",
            "struct MuxWriteState",
            "struct ConnectionCleanupGuard",
            "async fn read_async_inbound",
            "fn prepare_socket_path",
        ];
        for needle in needles {
            assert!(
                !production.contains(needle),
                "moved {needle} must leave src/daemon/**"
            );
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/daemon");
        let mut pending = vec![root];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).expect("read src/daemon") {
                let path = entry.expect("entry").path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("read daemon source");
                let production = source.split("mod tests").next().unwrap_or(&source);
                for needle in needles {
                    assert!(
                        !production.contains(needle),
                        "{} must not contain {needle}",
                        path.display()
                    );
                }
            }
        }
        let listener = include_str!("../transport/unix/listener.rs");
        let connection = include_str!("../transport/unix/connection.rs");
        let mux = include_str!("../transport/unix/mux_write.rs");
        assert!(
            listener.contains("pub(crate) fn accept_connections(")
                && listener.contains("pub(crate) fn prepare_socket_path"),
            "listener owns accept and socket path"
        );
        assert!(
            connection.contains("pub(crate) async fn handle_connection_async")
                && connection.contains("pub(crate) struct ConnectionCleanupGuard"),
            "connection owns the accepted-connection driver"
        );
        assert!(
            mux.contains("pub(crate) struct MuxWriteState")
                && mux.contains("pub(crate) async fn read_async_inbound"),
            "mux_write owns framing and mux scheduling"
        );
    }

    #[test]
    fn read_mode_flags_runtime_failure_projects_operator_error_without_default_body() {
        let response = daemon_operator_error(crate::HubClientError::Runtime {
            request_id: RequestId("mode-flags-backend-failure".to_string()),
            operation: crate::HubClientOperation::ReadModeFlags,
            kind: crate::HubClientRuntimeErrorKind::ModeReadFailed,
        });

        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        assert!(response.mode_flags.is_none());
        let error = response.error.expect("operator error body");
        assert_eq!(error.code, "mode_read_failed");
        assert_eq!(error.operation, "read_mode_flags");
        assert!(error.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::WorkerCompatibility
        }));
    }

    #[test]
    fn client_eof_detaches_connection_subscriptions() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_hello(&mut client);
        let mut reader = DaemonUnixFrameReader::new();
        let _ = read_hello_ack(&mut client, &mut reader);

        write_request(
            &mut client,
            1,
            DaemonRequest::Attach {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            },
        );
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected attach control request");
        };
        assert!(matches!(*request, DaemonRequest::Attach { .. }));
        reply_tx
            .send(Ok(daemon_events(Vec::new())))
            .expect("reply to attach request");
        let _ = read_response(&mut client, &mut reader, 1);

        client
            .shutdown(Shutdown::Both)
            .expect("disconnect daemon client");
        connection
            .join()
            .expect("join daemon connection")
            .expect("client disconnect is a clean connection close");
        assert!(
            control_rx.try_recv().is_err(),
            "Unix EOF must not enqueue pair-only DaemonRequest::Detach"
        );
    }

    #[test]
    fn register_unix_admission_acks_before_request_loop() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_hello(&mut client);
        let mut reader = DaemonUnixFrameReader::new();
        let _ = read_hello_ack(&mut client, &mut reader);

        let ControlMessage::RegisterUnixAdmission { reply_tx, .. } =
            receive_test_control_message(&mut control_rx)
        else {
            panic!("expected RegisterUnixAdmission after Hello");
        };
        write_request(&mut client, 1, DaemonRequest::Status);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build admission-wait runtime");
        let late = runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(80), control_rx.recv()).await
        });
        assert!(
            late.is_err(),
            "the request loop must wait for the admission ack: {late:?}"
        );
        reply_tx.send(()).expect("ack unix admission");
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected Status after admission ack");
        };
        assert!(matches!(*request, DaemonRequest::Status));
        reply_tx
            .send(Ok(daemon_response_base(DaemonResponseKind::Status)))
            .expect("reply to status");
        let _ = read_response(&mut client, &mut reader, 1);
        client
            .shutdown(Shutdown::Both)
            .expect("disconnect daemon client");
        connection
            .join()
            .expect("join daemon connection")
            .expect("client disconnect is a clean connection close");
    }

    #[test]
    fn unix_writer_wake_preserves_a_partial_inbound_request() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("bound daemon client reads");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_hello(&mut client);
        let mut reader = DaemonUnixFrameReader::new();
        let _ = read_hello_ack(&mut client, &mut reader);

        let ControlMessage::RegisterUnixAdmission {
            admission,
            reply_tx,
            ..
        } = receive_test_control_message(&mut control_rx)
        else {
            panic!("expected RegisterUnixAdmission after Hello");
        };
        let UnixTerminalAdmission::Admitted { mux, .. } = admission else {
            panic!("expected terminal admission");
        };
        let (mut adapter, handle) = mux.create_adapter();
        assert!(mux.register(
            "partial-session".to_string(),
            "partial-subscription".to_string(),
            1,
            handle,
        ));

        let request_bytes = encode_client_frame(&ClientFrame::Request {
            request_id: "1".to_string(),
            request: DaemonRequest::Status,
        })
        .expect("encode status request");
        let split = request_bytes.len() / 2;
        client
            .write_all(&request_bytes[..split])
            .expect("write partial status request");

        let body = encode_output(b"writer-wake").expect("encode terminal output");
        let body_bytes = body.as_bytes().to_vec();
        let frame = RoutedTerminalFrame::new(
            RouteId::new("partial-subscription").expect("route"),
            1,
            0,
            body,
        );
        adapter.try_write(&frame).expect("store terminal output");
        reply_tx.send(()).expect("ack unix admission");
        let terminal = read_terminal(&mut client, &mut reader);
        assert_eq!(terminal.route, "partial-subscription");
        assert_eq!(terminal.generation, 1);
        assert_eq!(terminal.body, body_bytes);

        client
            .write_all(&request_bytes[split..])
            .expect("complete status request");
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected complete Status request");
        };
        assert!(matches!(*request, DaemonRequest::Status));
        reply_tx
            .send(Ok(daemon_response_base(DaemonResponseKind::Status)))
            .expect("reply to status");
        let response = read_response(&mut client, &mut reader, 1);
        assert_eq!(response.kind, DaemonResponseKind::Status);

        client
            .shutdown(Shutdown::Both)
            .expect("disconnect daemon client");
        connection
            .join()
            .expect("join daemon connection")
            .expect("client disconnect is a clean connection close");
    }

    #[test]
    fn attach_operator_error_does_not_detach_on_client_eof() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_hello(&mut client);
        let mut reader = DaemonUnixFrameReader::new();
        let _ = read_hello_ack(&mut client, &mut reader);

        write_request(
            &mut client,
            1,
            DaemonRequest::Attach {
                session_id: "missing-session".to_string(),
                subscription_id: "missing-sub".to_string(),
            },
        );
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected attach control request");
        };
        assert!(matches!(*request, DaemonRequest::Attach { .. }));
        reply_tx
            .send(Ok(attach_bind_operator_error(
                "invalid_request",
                "attach failed before adapter bind",
            )))
            .expect("reply with attach operator error");
        let _ = read_response(&mut client, &mut reader, 1);

        client
            .shutdown(Shutdown::Both)
            .expect("disconnect daemon client");
        connection
            .join()
            .expect("join daemon connection")
            .expect("client disconnect is a clean connection close");
        assert!(
            control_rx.try_recv().is_err(),
            "pre-bind OperatorError must not enqueue Detach cleanup"
        );
    }

    #[test]
    fn status_after_pre_bind_attach_error_does_not_enqueue_detach() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_hello(&mut client);
        let mut reader = DaemonUnixFrameReader::new();
        let _ = read_hello_ack(&mut client, &mut reader);

        write_request(
            &mut client,
            1,
            DaemonRequest::Attach {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            },
        );
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected attach control request");
        };
        assert!(matches!(*request, DaemonRequest::Attach { .. }));
        reply_tx
            .send(Ok(attach_bind_operator_error(
                "invalid_request",
                "attach failed before adapter bind",
            )))
            .expect("reply with attach operator error");
        let _ = read_response(&mut client, &mut reader, 1);

        write_request(&mut client, 2, DaemonRequest::Status);
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected status control request");
        };
        assert!(matches!(*request, DaemonRequest::Status));
        reply_tx
            .send(Ok(daemon_events(Vec::new())))
            .expect("reply with status");
        let _ = read_response(&mut client, &mut reader, 2);

        client
            .shutdown(Shutdown::Both)
            .expect("disconnect daemon client");
        connection
            .join()
            .expect("join daemon connection")
            .expect("client disconnect is a clean connection close");
        assert!(
            control_rx.try_recv().is_err(),
            "pre-bind OperatorError plus Status must not enqueue Detach cleanup"
        );
    }

    #[test]
    fn status_egress_preflight_counts_formatted_rows_and_rechecks_counter_growth() {
        let mut diagnostics = DaemonEgressDiagnostics {
            terminal_write_failures: 9,
            control_write_failures: u64::MAX,
        };
        let bytes = diagnostics.diagnostics_bytes(usize::MAX).unwrap();
        assert!(diagnostics.bounded_diagnostics(bytes - 1).is_none());
        let (rows, charged) = diagnostics.bounded_diagnostics(bytes).unwrap();
        let actual = std::mem::size_of_val(&rows)
            + rows.len() * std::mem::size_of::<DaemonDiagnostic>()
            + rows
                .iter()
                .map(|row| {
                    row.operation.as_ref().unwrap().len() + row.message.as_ref().unwrap().len()
                })
                .sum::<usize>();
        assert_eq!(actual, bytes);
        assert_eq!(charged, bytes);
        diagnostics.record_write_failure(DaemonDeliveryKind::Terminal);
        assert!(diagnostics.bounded_diagnostics(bytes).is_none());
        let empty = DaemonEgressDiagnostics::default();
        assert!(
            empty
                .bounded_diagnostics(std::mem::size_of::<Vec<DaemonDiagnostic>>() - 1)
                .is_none()
        );
    }

    #[test]
    fn daemon_egress_diagnostics_classify_terminal_and_control_backpressure() {
        let control = daemon_response_base(DaemonResponseKind::Sessions);
        assert_eq!(daemon_delivery_kind(&control), DaemonDeliveryKind::Control);

        let mut diagnostics = DaemonEgressDiagnostics::default();
        let mut counters = DaemonLifecycleCounters::default();
        let data_directory = std::env::temp_dir().join(format!(
            "hub-t4-egress-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "t4-egress".to_string(),
                display_name: "T4 Egress".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("config");
        let runtime = crate::HubRuntime::new(config).expect("runtime");
        record_egress_write_failure(
            &mut diagnostics,
            &mut counters,
            Some(&runtime),
            DaemonDeliveryKind::Terminal,
            EgressWriteClass::Other,
        );
        record_egress_write_failure(
            &mut diagnostics,
            &mut counters,
            Some(&runtime),
            daemon_delivery_kind(&control),
            EgressWriteClass::Timeout,
        );
        let rows = diagnostics.diagnostics();

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Backpressure
                && diagnostic.operation.as_deref() == Some("daemon_client_egress")
        }));
        let debug = format!("{rows:?}");
        assert!(debug.contains("terminal"));
        assert!(debug.contains("control"));
        assert!(!debug.contains("private terminal payload"));
        assert!(!debug.contains("session-redacted"));
        assert!(!debug.contains("subscription-redacted"));
        assert_eq!(counters.stalled_writes, 2);
        let observability = runtime.event_plane_counters_snapshot();
        assert_eq!(observability.stalled_write_timeouts, 1);
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn write_deadline_error_increments_t4_while_other_write_failure_does_not() {
        let timeout_error = DaemonTransportError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "daemon client write deadline elapsed",
        ));
        let other_error = DaemonTransportError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "broken pipe",
        ));
        let timeout_class = egress_write_class(&timeout_error);
        let other_class = egress_write_class(&other_error);
        assert_eq!(timeout_class, EgressWriteClass::Timeout);
        assert_eq!(other_class, EgressWriteClass::Other);

        let mut diagnostics = DaemonEgressDiagnostics::default();
        let mut counters = DaemonLifecycleCounters::default();
        let data_directory = std::env::temp_dir().join(format!(
            "hub-t4-class-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "t4-class".to_string(),
                display_name: "T4 Class".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("config");
        let runtime = crate::HubRuntime::new(config).expect("runtime");
        record_egress_write_failure(
            &mut diagnostics,
            &mut counters,
            Some(&runtime),
            DaemonDeliveryKind::Control,
            other_class,
        );
        record_egress_write_failure(
            &mut diagnostics,
            &mut counters,
            Some(&runtime),
            DaemonDeliveryKind::Control,
            timeout_class,
        );
        assert_eq!(counters.stalled_writes, 2);
        assert_eq!(
            runtime
                .event_plane_counters_snapshot()
                .stalled_write_timeouts,
            1
        );
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn daemon_shutdown_waits_for_response_delivery_before_stopping() {
        let (completed_delivery_tx, completed_delivery_rx) = mpsc::channel();
        completed_delivery_tx
            .send(())
            .expect("pre-signal completed shutdown response delivery");
        assert!(
            wait_for_response_delivery(true, true, Some(completed_delivery_rx)),
            "shutdown response delivery must pass through the wait enforcement seam"
        );

        let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
        let (response_delivery_tx, response_delivery_rx) = mpsc::channel();
        let (stopped_tx, stopped_rx) = mpsc::channel();

        thread::spawn(move || {
            let should_stop = send_control_response(
                reply_tx,
                Ok(daemon_response_base(DaemonResponseKind::Shutdown)),
                Some(response_delivery_rx),
            );
            let _ = stopped_tx.send(should_stop);
        });

        let response = receive_test_control_reply(reply_rx).expect("shutdown response succeeds");
        assert_eq!(response.kind, DaemonResponseKind::Shutdown);
        assert!(
            stopped_rx.try_recv().is_err(),
            "daemon must remain alive until the transport attempts delivery"
        );

        response_delivery_tx
            .send(())
            .expect("report shutdown response delivery attempt");
        assert!(
            stopped_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("daemon stop decision follows delivery attempt")
        );
    }

    #[test]
    fn daemon_shutdown_releases_when_delivery_owner_drops() {
        let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
        let (response_delivery_tx, response_delivery_rx) = mpsc::channel();
        let (stopped_tx, stopped_rx) = mpsc::channel();

        thread::spawn(move || {
            let should_stop = send_control_response(
                reply_tx,
                Ok(daemon_response_base(DaemonResponseKind::Shutdown)),
                Some(response_delivery_rx),
            );
            let _ = stopped_tx.send(should_stop);
        });

        let _ = receive_test_control_reply(reply_rx);
        drop(response_delivery_tx);

        assert!(
            stopped_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("dropped delivery owner releases daemon stop")
        );
    }

    #[test]
    fn daemon_shutdown_releases_when_response_receiver_drops() {
        let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
        let (_response_delivery_tx, response_delivery_rx) = mpsc::channel();
        drop(reply_rx);

        assert!(send_control_response(
            reply_tx,
            Ok(daemon_response_base(DaemonResponseKind::Shutdown)),
            Some(response_delivery_rx),
        ));
    }

    #[test]
    fn daemon_shutdown_write_failure_releases_stop_and_preserves_error() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let server_control = server.try_clone().expect("clone daemon server socket");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_hello(&mut client);
        let mut reader = DaemonUnixFrameReader::new();
        let _ = read_hello_ack(&mut client, &mut reader);
        write_request(&mut client, 1, DaemonRequest::DaemonShutdown);

        let ControlMessage::Request {
            request,
            reply_tx,
            response_delivery_rx,
            grant_id,
            ..
        } = receive_test_control_request(&mut control_rx)
        else {
            panic!("expected shutdown control request");
        };
        assert!(matches!(*request, DaemonRequest::DaemonShutdown));
        assert_eq!(
            grant_id, None,
            "socket path must leave Request grant_id unset"
        );
        let response_delivery_rx = response_delivery_rx.expect("shutdown has delivery receiver");
        server_control
            .shutdown(Shutdown::Write)
            .expect("fail daemon shutdown response write");
        let (stopped_tx, stopped_rx) = mpsc::channel();
        thread::spawn(move || {
            let should_stop = send_control_response(
                reply_tx,
                Ok(daemon_response_base(DaemonResponseKind::Shutdown)),
                Some(response_delivery_rx),
            );
            let _ = stopped_tx.send(should_stop);
        });

        assert!(
            connection.join().expect("join daemon connection").is_err(),
            "failed shutdown response write remains a transport error"
        );
        assert!(
            stopped_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("failed response delivery releases daemon stop")
        );
    }

    fn unique_package_control_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        PathBuf::from("target")
            .join("botster-hub-test-data")
            .join("package-control")
            .join(name)
            .join(nanos.to_string())
    }

    fn package_control_config(data_directory: PathBuf) -> crate::HubConfig {
        crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "package-control-test".to_string(),
                display_name: "Package Control Test".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build package control config")
    }

    fn write_package_control_manifest(root: &Path, name: &str, extra: serde_json::Value) {
        std::fs::create_dir_all(root).expect("create package root");
        let mut manifest = serde_json::json!({
            "name": name,
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": [],
            "entrypoints": []
        });
        if let Some(object) = extra.as_object() {
            for (key, value) in object {
                manifest[key] = value.clone();
            }
        }
        std::fs::write(
            root.join("botster-package.json"),
            serde_json::to_vec_pretty(&manifest).expect("serialize package manifest"),
        )
        .expect("write package manifest");
    }

    fn drive_package_request(
        daemon: &mut HubDaemon,
        request: DaemonRequest,
    ) -> DaemonTransportResult<DaemonResponse> {
        let mut state = DaemonControlState::default();
        drive_package_request_with_state(daemon, &mut state, request)
    }

    fn drive_package_request_with_state(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        request: DaemonRequest,
    ) -> DaemonTransportResult<DaemonResponse> {
        let (control_tx, _control_rx) = tokio_mpsc::channel(8);
        let observability = DaemonObservability {
            egress: Vec::new(),
            lifecycle: DaemonLifecycleCounters::default(),
            client_id: None,
            grant_id: None,
            transport_request_id: None,
        };
        state.current_waiter_id = daemon
            .runtime()
            .and_then(|runtime| runtime.next_waiter_id());
        match handle_control_request(daemon, state, observability, control_tx, request) {
            crate::daemon::control::pending::ControlStep::Ready(response) => response,
            crate::daemon::control::pending::ControlStep::Pending(mut step) => loop {
                match step.continuation.poll(daemon, state) {
                    crate::daemon::control::pending::ControlPoll::DeliverStatusResponse(
                        _,
                        _,
                        _,
                    )
                    | crate::daemon::control::pending::ControlPoll::StatusResponseDelivered {
                        ..
                    }
                    | crate::daemon::control::pending::ControlPoll::StatusResponseRefused {
                        ..
                    }
                    | crate::daemon::control::pending::ControlPoll::FinishedInternal => {
                        panic!("package helper must not receive a status response")
                    }
                    crate::daemon::control::pending::ControlPoll::Again => continue,
                    crate::daemon::control::pending::ControlPoll::Pending => {}
                    crate::daemon::control::pending::ControlPoll::Ready(response) => {
                        break response;
                    }
                    crate::daemon::control::pending::ControlPoll::ReadyHost(response, charge) => {
                        drop(charge);
                        break response;
                    }
                    crate::daemon::control::pending::ControlPoll::PreparePluginResponse(_, _)
                    | crate::daemon::control::pending::ControlPoll::SubmitPluginHost(_) => {
                        panic!("package mutation must not prepare a plugin reply")
                    }
                }
                let completion = loop {
                    let runtime = daemon.runtime().expect("package test runtime");
                    match runtime.host_executor().poll_completion() {
                        crate::host_executor::HostCompletionPoll::Ready(completion) => {
                            break completion;
                        }
                        crate::host_executor::HostCompletionPoll::Empty => {
                            std::thread::yield_now();
                        }
                        crate::host_executor::HostCompletionPoll::Stopped => {
                            panic!("package test host executor stopped")
                        }
                    }
                };
                state
                    .host_completions
                    .insert(completion.identity.waiter_id, completion);
            },
        }
    }

    fn live_and_durable_registries(
        daemon: &HubDaemon,
        config: &crate::HubConfig,
    ) -> (
        crate::PackageRegistrySnapshot,
        crate::PackageRegistrySnapshot,
    ) {
        let live = daemon.package_registry().snapshot();
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let durable = store
            .load_or_initialize(config)
            .expect("load durable hub state")
            .package_registry;
        (live, durable)
    }

    fn package_state(daemon: &HubDaemon, name: &str) -> PackageState {
        daemon
            .package_registry()
            .package(name)
            .expect("package record")
            .state
    }

    fn plugin_is_loaded(daemon: &HubDaemon, name: &str) -> bool {
        daemon
            .runtime()
            .expect("runtime")
            .plugin_lifecycle_status(daemon.package_registry())
            .into_iter()
            .any(|status| status.package_name == name && status.loaded)
    }

    fn write_sleeper_script(package_dir: &Path) {
        std::fs::create_dir_all(package_dir.join("bin")).expect("create bin");
        std::fs::write(
            package_dir.join("bin/sleeper"),
            "#!/bin/sh\nexec /bin/sleep \"$@\"\n",
        )
        .expect("write sleeper");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(package_dir.join("bin/sleeper"))
                .expect("sleeper metadata")
                .permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(package_dir.join("bin/sleeper"), permissions)
                .expect("chmod sleeper");
        }
    }

    fn sleeper_manifest(args: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "runnable_entrypoints": [{
                "id": "sleeper",
                "kind": "terminal_app",
                "command": "bin/sleeper",
                "args": args,
                "launch_mode": "background",
                "may_supervise": true
            }]
        })
    }

    fn lua_and_sleeper_manifest(args: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "runnable_entrypoints": [{
                "id": "sleeper",
                "kind": "terminal_app",
                "command": "bin/sleeper",
                "args": args,
                "launch_mode": "background",
                "may_supervise": true
            }]
        })
    }

    fn write_lua_plugin(package_dir: &Path) {
        std::fs::write(
            package_dir.join("plugin.lua"),
            "return botster.register({})\n",
        )
        .expect("write lua plugin");
    }

    struct ControlledPluginGateGuard {
        _owner: std::sync::MutexGuard<'static, ()>,
    }

    impl ControlledPluginGateGuard {
        fn acquire() -> Self {
            static OWNER: std::sync::Mutex<()> = std::sync::Mutex::new(());
            Self {
                _owner: OWNER
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            }
        }
    }

    impl Drop for ControlledPluginGateGuard {
        fn drop(&mut self) {
            // Release the worker even when a fixture assertion fails.
            crate::lua_runtime::release_test_plugin_invocation_gate();
        }
    }

    fn write_controlled_gate_lua_plugin(package_dir: &Path) {
        std::fs::write(
            package_dir.join("plugin.lua"),
            r#"
return botster.register({
  tools = {{
    name = "owner.controlled_gate",
    description = "Controlled owner progress test.",
    input_schema = { type = "object", additionalProperties = false },
    handler = "controlled_gate",
    call = function(args)
      return { value = "released", token = args.token }
    end,
  }},
})
"#,
        )
        .expect("write controlled gate lua plugin");
    }

    fn write_controlled_entity_gate_lua_plugin(package_dir: &Path) {
        std::fs::write(
            package_dir.join("plugin.lua"),
            r#"
return botster.register({
  handlers = {
    {
      id = "controlled_gate",
      kind = "entity_provider",
      descriptor_id = "owner-entity-gate.entity",
      descriptor = { entity_type = "owner-entity-gate.entity", id_field = "id" },
      call = function(_request)
        return {
          type = "entity_snapshot",
          entity_type = "owner-entity-gate.entity",
          snapshot_seq = 1,
          items = {{ id = "entity-1" }},
        }
      end,
    },
  },
})
"#,
        )
        .expect("write controlled entity gate lua plugin");
    }

    fn collect_entity_test_host_completions(daemon: &HubDaemon, state: &mut DaemonControlState) {
        while let crate::host_executor::HostCompletionPoll::Ready(completion) =
            daemon.runtime().unwrap().host_executor().poll_completion()
        {
            let waiter = completion.identity.waiter_id;
            assert!(
                state
                    .plugin_entities
                    .accepts_host_completion(completion.identity)
            );
            state.plugin_entities.retain_host_completion(completion);
            crate::daemon::control::entities::mark_plugin_entity_ready(
                state,
                waiter,
                crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                crate::daemon::control::pending::READY_HOST_COMPLETION,
            );
        }
    }

    #[test]
    fn early_provider_result_waits_for_admission_and_survives_ambiguous_host_failure() {
        let _gate_owner = ControlledPluginGateGuard::acquire();
        use crate::host_executor::{HostCompletion, HostCompletionPoll, HostError, HostResult};
        use crate::package_event_router::LeaseIdentity;
        for outcome in [
            "prepare_cancel",
            "acquire_cancel",
            "acquire_queue_cancel",
            "deliver",
            "cancel",
            "fault",
        ] {
            let fault = outcome == "fault";
            let before_admission = matches!(
                outcome,
                "prepare_cancel" | "acquire_cancel" | "acquire_queue_cancel"
            );
            let root = unique_package_control_dir(&format!("early-provider-{outcome}"));
            let package_dir = root.join("owner-entity-gate");
            write_package_control_manifest(
                &package_dir,
                "owner-entity-gate",
                serde_json::json!({"entrypoints": [{"runtime":"lua", "path":"plugin.lua", "bootstrap":false}]}),
            );
            write_controlled_entity_gate_lua_plugin(&package_dir);
            let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
            drive_package_request(
                &mut daemon,
                DaemonRequest::InstallPackageLocalPath { path: package_dir },
            )
            .unwrap();
            drive_package_request(
                &mut daemon,
                DaemonRequest::EnablePackage {
                    package_name: "owner-entity-gate".into(),
                },
            )
            .unwrap();
            let runtime = daemon.runtime().unwrap();
            let metadata = runtime.shared_view_budget();
            let metadata_baseline = metadata.used();
            let scopes = runtime.causal_scopes().clone();
            let scope = scopes
                .mint_with_lease(Some(LeaseIdentity::ProviderResyncNeed {
                    family_token: runtime.test_family_causal_token("owner-entity-gate.entity"),
                }))
                .unwrap();
            runtime.test_store_resync_lease(scope, "owner-entity-gate.entity");
            let mut state = DaemonControlState::default();
            runtime.install_plugin_completion_notifier(
                state.plugin_result_budget.completion_notifier(),
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            let (completion, mut reply, mut frames) = 'admission: loop {
                let (frame_tx, frame_rx) = tokio_mpsc::channel(8);
                let (reply_tx, mut reply_rx) =
                    crate::daemon::control::message::control_reply_channel();
                crate::daemon::control::entities::handle(
                    &mut daemon,
                    &mut state,
                    ControlMessage::SubscribeEntities {
                        entity_type: "owner-entity-gate.entity".into(),
                        subscription_id: "early-provider".into(),
                        transport_request_id: None,
                        client_id: Some("early-client".into()),
                        frame_tx: crate::subscription::entity::EntityFrameSender::Async(frame_tx),
                        frame_rx: None,
                        reply_tx,
                        grant_id: None,
                    },
                );
                let mut refused = false;
                loop {
                    if let HostCompletionPoll::Ready(completion) =
                        daemon.runtime().unwrap().host_executor().poll_completion()
                    {
                        if (before_admission
                            && matches!(
                                completion.result,
                                HostResult::PluginEntity(
                                    crate::plugin_entity::Completion::ProviderPrepared(_)
                                )
                            ))
                            || matches!(
                                completion.result,
                                HostResult::PluginEntity(
                                    crate::plugin_entity::Completion::ProviderAdmitted(None)
                                )
                            )
                        {
                            break 'admission (completion, reply_rx, frame_rx);
                        }
                        let waiter = completion.identity.waiter_id;
                        state.plugin_entities.retain_host_completion(completion);
                        crate::daemon::control::entities::mark_plugin_entity_ready(
                            &mut state,
                            waiter,
                            crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                            crate::daemon::control::pending::READY_HOST_COMPLETION,
                        );
                    }
                    if let Some(item) = state.owner_ready.pop_next() {
                        crate::daemon::control::entities::drive_plugin_entity_ready_item(
                            &mut daemon,
                            &mut state,
                            item,
                        );
                    }
                    if let Ok(response) = reply_rx.try_recv() {
                        assert_eq!(
                            response.into_parts().0.unwrap().error.unwrap().code,
                            "plugin_invocation_backpressured"
                        );
                        refused = true;
                    }
                    if refused && state.budget.outstanding() == 0 {
                        break;
                    }
                    assert!(Instant::now() < deadline, "provider admission must finish");
                    thread::yield_now();
                }
            };
            let waiter = completion.identity.waiter_id;
            if before_admission {
                assert!(metadata.used() > metadata_baseline);
                state.plugin_entities.retain_host_completion(completion);
                crate::daemon::control::entities::mark_plugin_entity_ready(
                    &mut state,
                    waiter,
                    crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                    crate::daemon::control::pending::READY_HOST_COMPLETION,
                );
                if outcome == "acquire_cancel" {
                    while !state.plugin_entities.causal_waiters.contains(&waiter) {
                        collect_entity_test_host_completions(&daemon, &mut state);
                        scopes.test_with_inner_held(|| {
                            if let Some(item) = state.owner_ready.pop_next() {
                                crate::daemon::control::entities::drive_plugin_entity_ready_item(
                                    &mut daemon,
                                    &mut state,
                                    item,
                                );
                            }
                        });
                        assert!(
                            Instant::now() < deadline,
                            "selection must reach causal acquisition"
                        );
                        thread::yield_now();
                    }
                    let (selected_scope, token) =
                        state.plugin_entities.test_provider_lease(waiter).unwrap();
                    assert_eq!(selected_scope, scope);
                    assert!(!scopes.identities(scope).unwrap().contains(
                        &LeaseIdentity::ProviderInFlight {
                            invocation_token: token
                        }
                    ));
                    assert!(state.plugin_entities.causal_waiters.contains(&waiter));
                }
                if outcome == "acquire_queue_cancel" {
                    let selection = loop {
                        if let HostCompletionPoll::Ready(completion) =
                            daemon.runtime().unwrap().host_executor().poll_completion()
                        {
                            assert!(matches!(
                                completion.result,
                                HostResult::EntityModelComplete(
                                    crate::runtime::entity_model::Kind::SelectProvider
                                )
                            ));
                            break completion;
                        }
                        if let Some(item) = state.owner_ready.pop_next() {
                            crate::daemon::control::entities::drive_plugin_entity_ready_item(
                                &mut daemon,
                                &mut state,
                                item,
                            );
                        }
                        assert!(
                            Instant::now() < deadline,
                            "the Host selection must complete"
                        );
                        thread::yield_now();
                    };
                    let family_token = daemon
                        .runtime()
                        .unwrap()
                        .test_family_causal_token("owner-entity-gate.entity");
                    assert!(matches!(
                        daemon.runtime().unwrap().admit_causal_op(
                            crate::package_event_router::CausalOp::Release {
                                scope_id: scope,
                                identity: LeaseIdentity::ProviderResyncNeed { family_token },
                            }
                        ),
                        crate::package_event_router::CausalAdmitResult::Applied
                    ));
                    state.plugin_entities.retain_host_completion(selection);
                    crate::daemon::control::entities::mark_plugin_entity_ready(
                        &mut state,
                        waiter,
                        crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                        crate::daemon::control::pending::READY_HOST_COMPLETION,
                    );
                    let item = state.owner_ready.pop_next().unwrap();
                    crate::daemon::control::entities::drive_plugin_entity_ready_item(
                        &mut daemon,
                        &mut state,
                        item,
                    );
                    assert!(state.plugin_entities.causal_waiters.contains(&waiter));
                    let (selected_scope, token) =
                        state.plugin_entities.test_provider_lease(waiter).unwrap();
                    assert_eq!(selected_scope, scope);
                    assert!(!scopes.identities(scope).unwrap().contains(
                        &LeaseIdentity::ProviderInFlight {
                            invocation_token: token
                        }
                    ));
                    assert_eq!(daemon.runtime().unwrap().causal_operation_count(), 1);
                    assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
                    assert_eq!(state.budget.outstanding(), 1);
                    daemon.runtime().unwrap().apply_causal_owner_ops();
                    assert!(
                        !scopes.is_live(scope),
                        "the queued release applies before any provider acquisition"
                    );
                }
                crate::daemon::control::entities::retire_plugin_entity_connection(
                    &daemon,
                    &mut state,
                    "early-client",
                );
                while state.plugin_entities.has_waiter(waiter)
                    || state.package_entity_resync_scan.test_running()
                {
                    collect_entity_test_host_completions(&daemon, &mut state);
                    if let Some(item) = state.owner_ready.pop_next() {
                        crate::daemon::control::entities::drive_plugin_entity_ready_item(
                            &mut daemon,
                            &mut state,
                            item,
                        );
                    }
                    assert!(
                        Instant::now() < deadline,
                        "cancelled preparation must dispose of its metadata on Host"
                    );
                    thread::yield_now();
                }
                assert_eq!(metadata.used(), metadata_baseline);
                assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
                assert_eq!(state.budget.outstanding(), 0);
                assert!(reply.try_recv().is_err());
                assert!(frames.try_recv().is_err());
                assert_eq!(
                    run_completion_drain_slice_for_owner(
                        daemon.runtime().unwrap(),
                        &mut state.maintenance,
                        &mut state.plugin_controls,
                        &mut state.plugin_entities,
                        &state.plugin_result_budget,
                    )
                    .item_count,
                    0
                );
                daemon.stop();
                drop(state);
                std::fs::remove_dir_all(root).unwrap();
                continue;
            }
            let (accepted_scope, invocation_token) =
                state.plugin_entities.test_provider_lease(waiter).unwrap();
            assert_eq!(accepted_scope, scope);
            let accepted_lease = LeaseIdentity::ProviderInFlight { invocation_token };
            let retained_metadata = metadata.used();
            assert!(retained_metadata > metadata_baseline);
            loop {
                let count = run_completion_drain_slice_for_owner(
                    daemon.runtime().unwrap(),
                    &mut state.maintenance,
                    &mut state.plugin_controls,
                    &mut state.plugin_entities,
                    &state.plugin_result_budget,
                )
                .item_count;
                if count > 0 {
                    assert_eq!(count, 1);
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "Core must finish before admission completion is delivered"
                );
                thread::yield_now();
            }
            let retained_bytes = state.plugin_result_budget.retained_bytes();
            assert!(retained_bytes > 0);
            for ready in state
                .plugin_entities
                .take_ready_waiters(crate::host_executor::HOST_OPERATION_CAPACITY)
            {
                crate::daemon::control::entities::mark_plugin_entity_ready(
                    &mut state,
                    ready,
                    crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                    crate::daemon::control::pending::READY_HOST_COMPLETION,
                );
            }
            while let Some(item) = state.owner_ready.pop_next() {
                crate::daemon::control::entities::drive_plugin_entity_ready_item(
                    &mut daemon,
                    &mut state,
                    item,
                );
            }
            assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
            assert!(frames.try_recv().is_err());
            assert!(reply.try_recv().is_err());
            assert!(
                scopes
                    .identities(scope)
                    .unwrap()
                    .iter()
                    .any(|lease| *lease == accepted_lease)
            );
            let completion = if fault {
                let (identity, result, permit) = completion.into_parts();
                drop(result);
                HostCompletion::for_test(
                    identity,
                    HostResult::Failed {
                        generation: 0,
                        error: HostError::new(
                            "host_worker_panicked",
                            "injected failure after Core admission",
                        ),
                    },
                    permit,
                )
            } else {
                completion
            };
            state.plugin_entities.retain_host_completion(completion);
            crate::daemon::control::entities::mark_plugin_entity_ready(
                &mut state,
                waiter,
                crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                crate::daemon::control::pending::READY_HOST_COMPLETION,
            );
            if outcome != "deliver" {
                crate::daemon::control::entities::retire_plugin_entity_connection(
                    &daemon,
                    &mut state,
                    "early-client",
                );
            }
            if fault {
                while let Some(item) = state.owner_ready.pop_next() {
                    crate::daemon::control::entities::drive_plugin_entity_ready_item(
                        &mut daemon,
                        &mut state,
                        item,
                    );
                }
                assert!(state.plugin_entities.has_waiter(waiter));
                assert_eq!(state.budget.outstanding(), 1);
                assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
                assert_eq!(state.plugin_result_budget.retained_bytes(), retained_bytes);
                assert_eq!(metadata.used(), retained_metadata);
                assert!(
                    scopes
                        .identities(scope)
                        .unwrap()
                        .iter()
                        .any(|lease| *lease == accepted_lease)
                );
                assert!(reply.try_recv().is_err());
                assert!(frames.try_recv().is_err());
            } else {
                while state.plugin_entities.has_waiter(waiter) {
                    assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                    assert!(
                        Instant::now() < deadline,
                        "known admission must deliver the retained result"
                    );
                    thread::yield_now();
                }
                if outcome == "deliver" {
                    assert!(receive_test_control_reply(reply).unwrap().error.is_none());
                    assert!(frames.try_recv().is_ok());
                } else {
                    assert!(reply.try_recv().is_err());
                    assert!(frames.try_recv().is_err());
                }
                while daemon.runtime().unwrap().host_executor().outstanding() != 0 {
                    assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                    assert!(
                        Instant::now() < deadline,
                        "Host must acknowledge result disposal: {outcome}"
                    );
                    thread::yield_now();
                }
                assert_eq!(state.plugin_result_budget.retained_bytes(), 0);
                assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
                while daemon.runtime().unwrap().causal_operation_count() > 0 {
                    daemon.runtime().unwrap().apply_causal_owner_ops();
                }
                assert!(
                    !scopes
                        .identities(accepted_scope)
                        .unwrap_or_default()
                        .contains(&accepted_lease)
                );
            }
            daemon.stop();
            drop(state);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn admitted_providers_publish_while_all_expectations_remain_charged() {
        let _gate_owner = ControlledPluginGateGuard::acquire();
        let root = unique_package_control_dir("provider-publish-capacity");
        let package_dir = root.join("owner-entity-gate");
        write_package_control_manifest(
            &package_dir,
            "owner-entity-gate",
            serde_json::json!({"entrypoints": [{"runtime":"lua", "path":"plugin.lua", "bootstrap":false}]}),
        );
        std::fs::write(package_dir.join("plugin.lua"), r#"
local sequence = 0
return botster.register({ handlers = {{
  id = "controlled_gate", kind = "entity_provider", descriptor_id = "owner-entity-gate.entity",
  descriptor = { entity_type = "owner-entity-gate.entity", id_field = "id" },
  call = function(_request)
    sequence = sequence + 1
    local result = botster.entity_publish({ type = "entity_remove", entity_type = "owner-entity-gate.entity",
      snapshot_seq = sequence, id = "removed" })
    assert(result.ok)
    return { type = "entity_snapshot", entity_type = "owner-entity-gate.entity",
      snapshot_seq = sequence, items = {{ id = "item" }} }
  end,
}} })
"#).unwrap();
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "owner-entity-gate".into(),
            },
        )
        .unwrap();
        let mut state = DaemonControlState::default();
        daemon
            .runtime()
            .unwrap()
            .install_plugin_completion_notifier(state.plugin_result_budget.completion_notifier());
        let metadata = daemon.runtime().unwrap().shared_view_budget();
        let baseline = metadata.used();
        let mut replies = Vec::new();
        let mut frames = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        crate::lua_runtime::arm_test_plugin_invocation_gate();
        for index in 0..crate::host_executor::HOST_OPERATION_CAPACITY {
            loop {
                let previous_metadata = metadata.used();
                let (frame_tx, frame_rx) = tokio_mpsc::channel(32);
                let (reply_tx, mut reply_rx) =
                    crate::daemon::control::message::control_reply_channel();
                crate::daemon::control::entities::handle(
                    &mut daemon,
                    &mut state,
                    ControlMessage::SubscribeEntities {
                        entity_type: "owner-entity-gate.entity".into(),
                        subscription_id: format!("publishing-{index}"),
                        transport_request_id: None,
                        client_id: None,
                        frame_tx: crate::subscription::entity::EntityFrameSender::Async(frame_tx),
                        frame_rx: None,
                        reply_tx,
                        grant_id: None,
                    },
                );
                let mut refused = false;
                loop {
                    assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                    if let Ok(response) = reply_rx.try_recv() {
                        assert_eq!(
                            response.into_parts().0.unwrap().error.unwrap().code,
                            "plugin_invocation_backpressured"
                        );
                        refused = true;
                    }
                    if daemon.runtime().unwrap().host_executor().outstanding() == 0
                        && (refused || metadata.used() > previous_metadata)
                    {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "admission must release Host capacity while Lua waits"
                    );
                    thread::yield_now();
                }
                if refused {
                    continue;
                }
                replies.push(reply_rx);
                frames.push(frame_rx);
                break;
            }
        }
        assert!(crate::lua_runtime::wait_for_test_plugin_invocation_gate(
            Duration::ZERO
        ));
        assert_eq!(
            state.budget.outstanding(),
            crate::host_executor::HOST_OPERATION_CAPACITY
        );
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
        assert!(metadata.used() > baseline);
        let mut available = Vec::new();
        while let Some(permit) = daemon.runtime().unwrap().host_executor().try_reserve() {
            available.push(permit);
        }
        assert_eq!(
            available.len(),
            crate::host_executor::HOST_OPERATION_CAPACITY
        );
        drop(available);
        crate::lua_runtime::release_test_plugin_invocation_gate();
        while state.budget.outstanding() != 0
            || daemon.runtime().unwrap().package_entity_work_pending()
        {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                Instant::now() < deadline,
                "providers must publish and finish through the owner dispatcher"
            );
            thread::yield_now();
        }
        for reply in replies {
            assert!(receive_test_control_reply(reply).unwrap().error.is_none());
        }
        for mut frame in frames {
            assert!(frame.try_recv().is_ok());
        }
        assert_eq!(state.plugin_result_budget.retained_bytes(), 0);
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
        assert!(
            state.lifecycle_counters.package_entity_publish_accepted
                >= crate::host_executor::HOST_OPERATION_CAPACITY as u64
        );
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prepared_snapshots_and_fanout_progress_when_host_capacity_is_full() {
        let _gate_owner = ControlledPluginGateGuard::acquire();
        let root = unique_package_control_dir("entity-delivery-capacity-order");
        let package_dir = root.join("owner-entity-gate");
        write_package_control_manifest(
            &package_dir,
            "owner-entity-gate",
            serde_json::json!({
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            }),
        );
        write_controlled_entity_gate_lua_plugin(&package_dir);
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "owner-entity-gate".into(),
            },
        )
        .unwrap();
        let mut state = DaemonControlState::default();
        daemon
            .runtime()
            .unwrap()
            .install_plugin_completion_notifier(state.plugin_result_budget.completion_notifier());
        let mut deferred = Vec::new();
        let mut replies = Vec::new();
        let mut frames = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        for index in 0..crate::host_executor::HOST_OPERATION_CAPACITY {
            let (reply, frame) = 'request: loop {
                let (frame_tx, frame_rx) = tokio_mpsc::channel(8);
                let (reply_tx, mut reply_rx) =
                    crate::daemon::control::message::control_reply_channel();
                crate::daemon::control::entities::handle(
                    &mut daemon,
                    &mut state,
                    ControlMessage::SubscribeEntities {
                        entity_type: "owner-entity-gate.entity".into(),
                        subscription_id: format!("capacity-{index}"),
                        transport_request_id: Some(format!("request-{index}")),
                        client_id: Some("capacity-client".into()),
                        frame_tx: crate::subscription::entity::EntityFrameSender::Async(frame_tx),
                        frame_rx: None,
                        reply_tx,
                        grant_id: None,
                    },
                );
                let mut provider_completed = false;
                let mut refused = false;
                loop {
                    let completed = run_completion_drain_slice_for_owner(
                        daemon.runtime().unwrap(),
                        &mut state.maintenance,
                        &mut state.plugin_controls,
                        &mut state.plugin_entities,
                        &state.plugin_result_budget,
                    )
                    .item_count;
                    if completed > 0 {
                        assert_eq!(completed, 1);
                        provider_completed = true;
                    }
                    if provider_completed
                        && daemon.runtime().unwrap().host_executor().outstanding() == 0
                    {
                        while let Some(item) = state.owner_ready.pop_next() {
                            deferred.push(item);
                        }
                        break 'request (reply_rx, frame_rx);
                    }
                    if let Ok(reply) = reply_rx.try_recv() {
                        let response = reply.into_parts().0.unwrap();
                        assert_eq!(
                            response.error.unwrap().code,
                            "plugin_invocation_backpressured"
                        );
                        refused = true;
                    }
                    if refused && daemon.runtime().unwrap().host_executor().outstanding() == 0 {
                        break;
                    }
                    // Collect admission phases before retaining each completed provider.
                    collect_entity_test_host_completions(&daemon, &mut state);
                    if let Some(item) = state.owner_ready.pop_next() {
                        let mut budget =
                            crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
                        assert!(!dispatch_owner_ready_item(
                            &mut daemon,
                            &mut state,
                            item,
                            &mut budget
                        ));
                    }
                    assert!(
                        Instant::now() < deadline,
                        "Lua snapshot {index} must complete"
                    );
                    thread::yield_now();
                }
                assert!(Instant::now() < deadline, "Core admission must recover");
            };
            replies.push(reply);
            frames.push(frame);
        }
        for item in deferred {
            let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
            assert!(!dispatch_owner_ready_item(
                &mut daemon,
                &mut state,
                item,
                &mut budget
            ));
        }
        // Route all real Core results before the next owner dispatch batch.
        let waiters = state
            .plugin_entities
            .take_ready_waiters(crate::host_executor::HOST_OPERATION_CAPACITY);
        for waiter in waiters {
            crate::daemon::control::entities::mark_plugin_entity_ready(
                &mut state,
                waiter,
                crate::daemon::owner_schedule::ReadyClass::HostCompletion,
                crate::daemon::control::pending::READY_HOST_COMPLETION,
            );
        }
        // One dispatch batch can submit every preparation before collecting Host results.
        while daemon.runtime().unwrap().host_executor().outstanding()
            < crate::host_executor::HOST_OPERATION_CAPACITY
        {
            let item = state
                .owner_ready
                .pop_next()
                .expect("provider preparation is ready");
            let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
            assert!(!dispatch_owner_ready_item(
                &mut daemon,
                &mut state,
                item,
                &mut budget
            ));
            assert!(
                Instant::now() < deadline,
                "all preparations must reserve capacity"
            );
        }
        daemon.runtime().unwrap().test_admit_publish("owner-entity-gate",
            serde_json::json!({"type": "entity_remove", "entity_type": "owner-entity-gate.entity",
                "snapshot_seq": 1, "id": "entity-1"}), None,
        ).unwrap();
        crate::daemon::control::entities::begin_package_entity_fanout(&daemon, &mut state);
        let deadline = Instant::now() + Duration::from_secs(2);
        while crate::daemon::control::entities::plugin_entity_cleanup_pending(&state)
            || daemon.runtime().unwrap().has_package_entity_fanout()
        {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                Instant::now() < deadline,
                "snapshots and fanout must complete without a cancellation deadline: {:?}",
                state.plugin_entities
            );
            thread::yield_now();
        }
        for mut reply in replies {
            assert!(
                reply
                    .try_recv()
                    .unwrap()
                    .into_parts()
                    .0
                    .unwrap()
                    .error
                    .is_none()
            );
        }
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
        let decode = |delivery| match delivery {
            crate::entity_delivery::EntityDelivery::Typed(frame) => frame,
            crate::entity_delivery::EntityDelivery::Encoded(frame) => frame.into_typed(),
        };
        for receiver in &mut frames {
            assert!(matches!(
                decode(receiver.try_recv().unwrap()),
                botster_hub_client::DaemonEntityFrame::Snapshot {
                    snapshot_seq: 1,
                    ..
                }
            ));
            assert!(
                receiver.try_recv().is_err(),
                "the sequence-1 mutation is superseded"
            );
        }
        let bridge = daemon.runtime().unwrap().entity_publish_bridge();
        let publication = bridge.test_queue_publish(
            botster_core::PluginKey("owner-entity-gate".into()),
            serde_json::json!({"type": "entity_remove", "entity_type": "owner-entity-gate.entity",
                "snapshot_seq": 2, "id": "entity-1"}),
            None,
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            if bridge.pending_publish_count() == 0
                && state.budget.outstanding() == 0
                && !daemon
                    .runtime()
                    .unwrap()
                    .entity_publish_retirement_pending()
                && !daemon.runtime().unwrap().has_package_entity_fanout()
                && !crate::daemon::control::entities::plugin_entity_cleanup_pending(&state)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the next mutation must reach every subscriber"
            );
            thread::yield_now();
        }
        assert_eq!(
            publication.try_recv().unwrap().unwrap().last_accepted_seq,
            2
        );
        for receiver in &mut frames {
            assert!(matches!(
                decode(receiver.try_recv().unwrap()),
                botster_hub_client::DaemonEntityFrame::Remove {
                    snapshot_seq: 2,
                    ..
                }
            ));
            assert!(receiver.try_recv().is_err());
        }
        assert_eq!(bridge.retained_counts(), (0, 0));
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
        drop(frames);
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    fn write_async_plugin_response_fixture(package_dir: &Path) {
        std::fs::write(
            package_dir.join("plugin.lua"),
            r#"
local function fail_if_requested(request)
  if request.fail == true or (request.payload and request.payload.fail == true) then
    error("controlled plugin failure")
  end
end

return botster.register({
  tools = {
    {
      name = "owner-responses.responses",
      description = "Return one controlled result.",
      input_schema = { type = "object", additionalProperties = true },
      handler = "responses",
      call = function(args)
        fail_if_requested(args)
        return { path = "mcp", ok = true }
      end,
    },
  },
  handlers = {
    {
      id = "surface",
      kind = "surface_route",
      descriptor_id = "response.surface",
      descriptor = { title = "Response", surface_id = "response.surface" },
      call = function(request)
        fail_if_requested(request)
        return {
          type = "panel",
          id = "response-panel",
          children = {{ type = "text", id = "response-label", props = { text = "ok" } }},
        }
      end,
    },
    {
      id = "action",
      kind = "ui_action",
      descriptor_id = "response.action",
      descriptor = { action_id = "response.action", surface_id = "response.surface" },
      call = function(request)
        fail_if_requested(request)
        return {
          request_id = request.request_id,
          surface_id = request.surface_id,
          action_id = request.action_id,
          node_id = request.node_id,
          state = "accepted",
          payload = { path = "action", ok = true },
        }
      end,
    },
    {
      id = "entities",
      kind = "entity_provider",
      descriptor_id = "owner-responses.entity",
      descriptor = { entity_type = "owner-responses.entity", id_field = "id" },
      call = function(request)
        if request.subscription_id == "entity-failure" then
          error("controlled entity failure")
        end
        return {
          type = "entity_snapshot",
          entity_type = "owner-responses.entity",
          snapshot_seq = 1,
          items = {{ id = "entity-1", ok = true }},
        }
      end,
    },
  },
})
"#,
        )
        .expect("write asynchronous response fixture");
    }

    fn start_async_control_request(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        request: DaemonRequest,
        client_id: &str,
        transport_request_id: &str,
    ) -> crate::daemon::control::message::ControlReplyReceiver {
        let (control_tx, _control_rx) = tokio_mpsc::channel(8);
        let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
        let transport_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test transport runtime");
        daemon
            .runtime()
            .expect("plugin runtime")
            .install_plugin_completion_notifier(state.plugin_result_budget.completion_notifier());
        crate::daemon::control::request::handle(
            daemon,
            state,
            transport_runtime.handle(),
            control_tx,
            ControlMessage::Request {
                request: Box::new(request),
                transport_request_id: Some(transport_request_id.to_string()),
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client_id: Some(client_id.to_string()),
                enqueued_at: Instant::now(),
            },
        );
        reply_rx
    }

    fn finish_async_plugin_control(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        mut reply_rx: crate::daemon::control::message::ControlReplyReceiver,
    ) -> DaemonTransportResult<DaemonResponse> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut response = None;
        loop {
            drive_ready_test_turn(daemon, state);
            if response.is_none() {
                match reply_rx.try_recv() {
                    Ok(reply) => {
                        let (value, charge, encoded) = reply.into_parts();
                        assert!(encoded.is_some(), "the worker must encode the response");
                        assert!(
                            charge.is_some(),
                            "the response must retain its host byte charge"
                        );
                        response = Some(value);
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                    Err(error) => panic!("plugin reply failed: {error}"),
                }
            }
            if response.is_some() && !state.plugin_controls.has_pending() {
                return response.expect("plugin reply exists");
            }
            assert!(Instant::now() < deadline, "plugin response timed out");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn drive_async_plugin_control(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        request: DaemonRequest,
        transport_request_id: &str,
    ) -> DaemonTransportResult<DaemonResponse> {
        let reply_rx = start_async_control_request(
            daemon,
            state,
            request,
            "response-fixture-connection",
            transport_request_id,
        );
        finish_async_plugin_control(daemon, state, reply_rx)
    }

    #[test]
    fn refused_entity_admission_retires_with_reserved_host_capacity() {
        let _gate_owner = ControlledPluginGateGuard::acquire();
        use crate::package_event_router::{CausalAdmitResult, CausalOp, LeaseIdentity};
        let root = unique_package_control_dir("entity-admission-retirement");
        let package_dir = root.join("owner-entity-gate");
        write_package_control_manifest(
            &package_dir,
            "owner-entity-gate",
            serde_json::json!({
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            }),
        );
        write_controlled_entity_gate_lua_plugin(&package_dir);
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "owner-entity-gate".into(),
            },
        )
        .unwrap();
        let runtime = daemon.runtime().unwrap();
        let scopes = runtime.causal_scopes().clone();
        let scope = scopes
            .mint_with_lease(Some(LeaseIdentity::ProviderResyncNeed {
                family_token: daemon
                    .runtime()
                    .unwrap()
                    .test_family_causal_token("owner-entity-gate.entity"),
            }))
            .unwrap();
        runtime.test_store_resync_lease(scope, "owner-entity-gate.entity");
        let executor = runtime.host_executor();
        let mut host_permits = Vec::new();
        for _ in 1..crate::host_executor::HOST_OPERATION_CAPACITY {
            host_permits.push(executor.try_reserve().unwrap());
        }
        runtime.set_test_plugin_admit_backpressure(true);
        let mut state = DaemonControlState::default();
        let (frame_tx, _frame_rx) = tokio_mpsc::channel(1);
        let (reply_tx, mut reply) = crate::daemon::control::message::control_reply_channel();
        crate::daemon::control::entities::handle(
            &mut daemon,
            &mut state,
            ControlMessage::SubscribeEntities {
                entity_type: "owner-entity-gate.entity".into(),
                subscription_id: "refused".into(),
                transport_request_id: None,
                client_id: None,
                frame_tx: crate::subscription::entity::EntityFrameSender::Async(frame_tx),
                frame_rx: None,
                reply_tx,
                grant_id: None,
            },
        );
        let item = state
            .owner_ready
            .pop_next()
            .expect("refusal schedules its retirement");
        let waiter = item.key().waiter_id();
        crate::daemon::control::entities::drive_plugin_entity_ready_item(
            &mut daemon,
            &mut state,
            item,
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut causal_filled = false;
        while !state.plugin_entities.causal_waiters.contains(&waiter) {
            if !causal_filled
                && scopes
                    .identities(scope)
                    .unwrap()
                    .iter()
                    .any(|identity| matches!(identity, LeaseIdentity::ProviderInFlight { .. }))
            {
                for _ in 0..crate::runtime::CAUSAL_OWNER_CAPACITY {
                    assert!(matches!(
                        daemon
                            .runtime()
                            .unwrap()
                            .admit_causal_op(CausalOp::Release {
                                scope_id: u64::MAX,
                                identity: LeaseIdentity::EventInFlight,
                            }),
                        CausalAdmitResult::Applied
                    ));
                }
                causal_filled = true;
            }
            collect_entity_test_host_completions(&daemon, &mut state);
            if let Some(item) = state.owner_ready.pop_next() {
                crate::daemon::control::entities::drive_plugin_entity_ready_item(
                    &mut daemon,
                    &mut state,
                    item,
                );
            }
            assert!(
                Instant::now() < deadline,
                "refusal must reach causal retirement"
            );
            thread::yield_now();
        }
        assert_eq!(
            daemon.runtime().unwrap().host_executor().outstanding(),
            crate::host_executor::HOST_OPERATION_CAPACITY
        );
        assert!(state.plugin_entities.causal_waiters.contains(&waiter));
        assert_eq!(state.budget.outstanding(), 1);
        assert!(
            reply.try_recv().is_err(),
            "the refusal waits for lease retirement admission"
        );
        assert!(
            scopes
                .identities(scope)
                .unwrap()
                .iter()
                .any(|identity| matches!(identity, LeaseIdentity::ProviderInFlight { .. }))
        );
        daemon.runtime().unwrap().apply_causal_owner_ops();
        assert!(crate::daemon::control::entities::mark_plugin_entity_ready(
            &mut state,
            waiter,
            crate::daemon::owner_schedule::ReadyClass::HostCompletion,
            crate::daemon::control::pending::READY_HOST_COMPLETION
        ));
        let item = state.owner_ready.pop_next().unwrap();
        crate::daemon::control::entities::drive_plugin_entity_ready_item(
            &mut daemon,
            &mut state,
            item,
        );
        while state.plugin_entities.has_waiter(waiter) {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state));
            assert!(
                Instant::now() < deadline,
                "refusal must finish with its reserved Host slot"
            );
            thread::yield_now();
        }
        let response = receive_test_control_reply(reply).unwrap();
        assert!(response.error.is_some());
        assert_eq!(state.budget.outstanding(), 0);
        assert!(!state.plugin_entities.has_waiter(waiter));
        assert!(!state.plugin_entities.causal_waiters.contains(&waiter));
        let runtime = daemon.runtime().unwrap();
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert!(
            !scopes
                .identities(scope)
                .unwrap_or_default()
                .iter()
                .any(|identity| matches!(identity, LeaseIdentity::ProviderInFlight { .. }))
        );
        assert_eq!(
            daemon.runtime().unwrap().host_executor().outstanding(),
            host_permits.len()
        );
        drop(host_permits);
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resync_scopes_outlive_event_and_publication_permits() {
        use crate::package_event_router::{EventPlaneStatus, HUB_EVENT_OWNER, LeaseIdentity};
        let root = unique_package_control_dir("retained-resync-scopes");
        let package_dir = root.join("resync-probe");
        write_package_control_manifest(
            &package_dir,
            "resync-probe",
            serde_json::json!({
                "capabilities": [],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            }),
        );
        std::fs::write(
            package_dir.join("plugin.lua"),
            r#"
events.on("hub", "worktree_created", function()
  local result = botster.entity_publish({
    type = "entity_remove", entity_type = "resync-probe.item", snapshot_seq = 100, id = "item"
  })
  assert(result.ok)
end)
return botster.register({ handlers = {{
  id = "items", kind = "entity_provider", descriptor_id = "resync-probe.item",
  descriptor = { entity_type = "resync-probe.item", id_field = "id" },
  call = function() return {
    type = "entity_snapshot", entity_type = "resync-probe.item", snapshot_seq = 0, items = {}
  } end,
}} })
"#,
        )
        .unwrap();
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "resync-probe".into(),
            },
        )
        .unwrap();
        let mut state = DaemonControlState::default();
        daemon
            .runtime()
            .unwrap()
            .install_plugin_completion_notifier(state.plugin_result_budget.completion_notifier());
        let count = crate::host_executor::HOST_OPERATION_CAPACITY + 1;
        for expected in 1..=count {
            assert_eq!(
                daemon
                    .runtime()
                    .unwrap()
                    .package_event_router()
                    .try_ingress(
                        HUB_EVENT_OWNER,
                        "worktree_created",
                        &serde_json::json!({"event": "worktree_created"}),
                        Instant::now()
                    ),
                EventPlaneStatus::Accepted
            );
            state
                .maintenance
                .wakes
                .mark(MaintenanceSliceKind::PackageEventDelivery);
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                drive_ready_test_turn(&mut daemon, &mut state);
                let runtime = daemon.runtime().unwrap();
                if runtime
                    .event_plane_counters()
                    .snapshot()
                    .event_handler_completed_ok
                    == expected as u64
                    && runtime.causal_operation_count() == 0
                    && !runtime.entity_publish_retirement_pending()
                    && state.maintenance.event_in_flight.is_empty()
                    && state.maintenance.pending_retirements.is_empty()
                    && state.budget.outstanding() == 0
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "event and publication work must retire"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            let runtime = daemon.runtime().unwrap();
            assert_eq!(
                runtime.test_resync_lease_count("resync-probe.item"),
                expected
            );
            assert_eq!(runtime.entity_publish_bridge().pending_publish_count(), 0);
            assert_eq!(
                runtime.entity_publish_bridge().retained_counts().0,
                expected
            );
        }
        let runtime = daemon.runtime().unwrap();
        let token = runtime.test_family_causal_token("resync-probe.item");
        let scope_ids = runtime.test_resync_scope_ids("resync-probe.item");
        assert_eq!(scope_ids.len(), count);
        for scope_id in scope_ids {
            assert_eq!(
                runtime.causal_scopes().identities(scope_id),
                Some(std::collections::BTreeSet::from([
                    LeaseIdentity::ProviderResyncNeed {
                        family_token: token
                    },
                ]))
            );
        }
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lifetime_budget_allows_a_live_event_to_complete_512_publications() {
        use crate::package_event_router::{EventPlaneStatus, HUB_EVENT_OWNER};
        let root = unique_package_control_dir("lifetime-live-event");
        let package_dir = root.join("resync-probe");
        write_package_control_manifest(
            &package_dir,
            "resync-probe",
            serde_json::json!({
                "capabilities": [],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            }),
        );
        std::fs::write(
            package_dir.join("plugin.lua"),
            r#"
events.on("hub", "worktree_created", function()
  for seq = 1, 512 do
    local result = botster.entity_publish({
      type = "entity_remove", entity_type = "resync-probe.item", snapshot_seq = seq, id = "item"
    })
    assert(result.ok)
  end
end)
return botster.register({ handlers = {{
  id = "items", kind = "entity_provider", descriptor_id = "resync-probe.item",
  descriptor = { entity_type = "resync-probe.item", id_field = "id" },
  call = function() return {
    type = "entity_snapshot", entity_type = "resync-probe.item", snapshot_seq = 0, items = {}
  } end,
}} })
"#,
        )
        .unwrap();
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "resync-probe".into(),
            },
        )
        .unwrap();
        let mut state = DaemonControlState::default();
        daemon
            .runtime()
            .unwrap()
            .install_plugin_completion_notifier(state.plugin_result_budget.completion_notifier());
        let count = 1;
        for expected in 1..=count {
            assert_eq!(
                daemon
                    .runtime()
                    .unwrap()
                    .package_event_router()
                    .try_ingress(
                        HUB_EVENT_OWNER,
                        "worktree_created",
                        &serde_json::json!({"event": "worktree_created"}),
                        Instant::now()
                    ),
                EventPlaneStatus::Accepted
            );
            state
                .maintenance
                .wakes
                .mark(MaintenanceSliceKind::PackageEventDelivery);
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                drive_ready_test_turn(&mut daemon, &mut state);
                let runtime = daemon.runtime().unwrap();
                if runtime
                    .event_plane_counters()
                    .snapshot()
                    .event_handler_completed_ok
                    == expected as u64
                    && runtime.causal_operation_count() == 0
                    && !runtime.entity_publish_retirement_pending()
                    && state.maintenance.event_in_flight.is_empty()
                    && state.maintenance.pending_retirements.is_empty()
                    && state.budget.outstanding() == 0
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "event and publication work must retire"
                );
                std::thread::yield_now();
            }
            let runtime = daemon.runtime().unwrap();
            assert_eq!(runtime.test_resync_lease_count("resync-probe.item"), 0);
            assert_eq!(runtime.entity_publish_bridge().pending_publish_count(), 0);
        }
        let runtime = daemon.runtime().unwrap();
        assert_eq!(runtime.test_family_seq("resync-probe.item"), 512);
        assert_eq!(runtime.entity_publish_bridge().retained_counts(), (0, 0));
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn asynchronous_lua_publications_complete_through_owner_ready_work() {
        let root = unique_package_control_dir("async-entity-publish");
        let package_dir = root.join("owner-publisher");
        write_package_control_manifest(
            &package_dir,
            "owner-publisher",
            serde_json::json!({
                "capabilities": [{ "surface": "mcp" }],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            }),
        );
        std::fs::write(package_dir.join("plugin.lua"), r#"
return botster.register({
  tools = {{
    name = "owner-publisher.publish", description = "Publish two mutations.",
    input_schema = { type = "object" }, handler = "publish",
    call = function()
      local first = botster.entity_publish({ type = "entity_upsert", entity_type = "owner-publisher.item", snapshot_seq = 1, id = "one", entity = { id = "one" } })
      local second = botster.entity_publish({ type = "entity_upsert", entity_type = "owner-publisher.item", snapshot_seq = 2, id = "two", entity = { id = "two" } })
      return { first = first.ok, second = second.ok }
    end,
  }},
  handlers = {{
    id = "items", kind = "entity_provider", descriptor_id = "owner-publisher.item",
    descriptor = { entity_type = "owner-publisher.item", id_field = "id" },
    call = function() return { type = "entity_snapshot", entity_type = "owner-publisher.item", snapshot_seq = 0, items = {} } end,
  }},
})
"#).unwrap();
        let mut daemon = HubDaemon::start(package_control_config(root.join("data"))).unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .unwrap();
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "owner-publisher".into(),
            },
        )
        .unwrap();
        let mut state = DaemonControlState::default();
        let response = drive_async_plugin_control(
            &mut daemon,
            &mut state,
            DaemonRequest::PluginMcpCallTool {
                name: "owner-publisher.publish".into(),
                arguments: serde_json::json!({}),
            },
            "async-publish",
        )
        .unwrap();
        assert!(response.error.is_none(), "{response:?}");
        assert_eq!(
            response.plugin_tool_result,
            serde_json::json!({"first":true,"second":true})
        );
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .entity_publish_bridge()
                .pending_publish_count(),
            0
        );
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn real_daemon_routes_two_coordination_acknowledgements_through_core_wakes() {
        real_daemon_coordination_acknowledgements(2);
    }

    #[test]
    fn real_daemon_coordination_core_waits_preserve_host_capacity_for_sibling() {
        real_daemon_coordination_acknowledgements(crate::host_executor::HOST_OPERATION_CAPACITY);
    }

    fn real_daemon_coordination_acknowledgements(call_count: usize) {
        struct ReleaseCore(Option<mpsc::Sender<()>>);
        impl Drop for ReleaseCore {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        struct StopServe(ControlSender);
        impl Drop for StopServe {
            fn drop(&mut self) {
                let _ = self.0.try_send(ControlMessage::Request {
                    request: Box::new(DaemonRequest::DaemonShutdown),
                    transport_request_id: None,
                    reply_tx: ControlReplySender::absent(),
                    response_delivery_rx: None,
                    grant_id: None,
                    client_id: None,
                    enqueued_at: Instant::now(),
                });
            }
        }
        let _guard = ControlledPluginGateGuard::acquire();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            PathBuf::from("/private/tmp").join(format!("hub-c1-{}-{unique}", std::process::id()));
        let mut config = package_control_config(root.join("data"));
        config.transports.local_socket = Some(crate::config::LocalSocketBinding {
            path: root.join("hub.sock"),
        });
        let package_dirs = (0..call_count).map(|index| {
            let name = format!("owner.c1-{index}");
            let directory = root.join(&name);
            write_package_control_manifest(&directory, &name, serde_json::json!({
                "capabilities": [{"surface":"mcp"}],
                "entrypoints": [{"runtime":"lua","path":"plugin.lua","bootstrap":false}]
            }));
            let plugin = r#"
return botster.register({tools = {{
  name = "owner.c1___INDEX__",
  description = "Acknowledge one seeded envelope.",
  input_schema = {type="object", properties={token={type="string"}, topic={type="string"}, envelope_id={type="string"}}},
  handler = "acknowledge",
  call = function(args)
    local result = botster.coordination.acknowledge({target={type="topic",topic=args.topic}, envelope_id=args.envelope_id})
    return {token=args.token, result=result}
  end,
}}})
"#.replace("__INDEX__", &index.to_string());
            std::fs::write(directory.join("plugin.lua"), plugin).unwrap();
            (name, directory)
        }).collect::<Vec<_>>();
        let (release_tx, release_rx) = mpsc::channel();
        let mut release = ReleaseCore(Some(release_tx));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let server_config = config.clone();
        let owner = thread::Builder::new()
            .name("c1-real-serve-owner".into())
            .spawn(move || {
                serve_daemon_inner(
                    server_config,
                    Some(ServeTerminalTest {
                        start: Box::new(move |daemon, _receiver, sender| {
                            // Fixture installation finishes before the tested socket requests start.
                            for (name, directory) in package_dirs {
                                drive_package_request(
                                    daemon,
                                    DaemonRequest::InstallPackageLocalPath { path: directory },
                                )
                                .unwrap();
                                drive_package_request(
                                    daemon,
                                    DaemonRequest::EnablePackage { package_name: name },
                                )
                                .unwrap();
                            }
                            let runtime = daemon.runtime().unwrap();
                            for index in 0..call_count {
                                let envelope = botster_core::RoutedEnvelope::new(
                                    botster_core::EnvelopeId(format!("c1-envelope-{index}")),
                                    botster_core::EndpointId("hub:c1-fixture".into()),
                                    vec![botster_core::EnvelopeTarget::Topic {
                                        topic: format!("c1-topic-{index}"),
                                    }],
                                    botster_core::RoutedEnvelopePayload {
                                        content_type: "text/plain".into(),
                                        body: vec![index as u8],
                                        extension: None,
                                    },
                                    41,
                                );
                                runtime
                                    .publish_routed_envelope(envelope)
                                    .wait(Duration::from_secs(5))
                                    .unwrap()
                                    .unwrap();
                            }
                            drop(runtime.submit_core(move |_| {
                                entered_tx.send(()).unwrap();
                                release_rx
                                    .recv_timeout(Duration::from_secs(10))
                                    .expect("the fixture releases Core");
                            }));
                            let probe: Box<dyn Fn(crate::owner_identity::WaiterId) -> bool + Send> =
                                Box::new(runtime.test_core_waiter_probe());
                            let callback_usage: Box<dyn Fn() -> usize + Send> =
                                Box::new(runtime.test_lua_callback_usage_probe());
                            started_tx
                                .send((
                                    runtime.coordination_bridge(),
                                    probe,
                                    callback_usage,
                                    sender.clone(),
                                ))
                                .unwrap();
                        }),
                        selected: Box::new(|_daemon, state, _, _| {
                            assert!(
                                state.coordination_fault.is_none(),
                                "{:?}",
                                state.coordination_fault
                            );
                        }),
                        stopped: Box::new(|| {}),
                    }),
                )
            })
            .unwrap();
        let (bridge, retains_waiter, callback_usage, control) =
            started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let stop = StopServe(control);
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let mut client =
            UnixStream::connect(&config.transports.local_socket.as_ref().unwrap().path).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = DaemonUnixFrameReader::new();
        write_hello(&mut client);
        read_hello_ack(&mut client, &mut reader);
        let list_packages_id = 41 + call_count as u64;
        let status_id = list_packages_id + 1;
        let shutdown_id = list_packages_id + 2;
        let held_started = Instant::now();
        let setup_deadline = held_started + Duration::from_millis(500);
        for index in 0..call_count {
            write_request(
                &mut client,
                41 + index as u64,
                DaemonRequest::PluginMcpCallTool {
                    name: format!("owner.c1_{index}"),
                    arguments: serde_json::json!({"token":format!("ack-{index}"), "topic":format!("c1-topic-{index}"), "envelope_id":format!("c1-envelope-{index}")}),
                },
            );
        }
        while bridge.test_admitted_waiters().len() != call_count {
            assert!(
                Instant::now() < setup_deadline,
                "setup timing: Core admission must leave half of the callback timeout unused"
            );
            thread::yield_now();
        }
        let waiters = bridge.test_admitted_waiters();
        assert_eq!(
            waiters.iter().copied().collect::<BTreeSet<_>>().len(),
            call_count
        );
        assert!(waiters.iter().all(|waiter| retains_waiter(*waiter)));
        assert!(
            callback_usage() > 0,
            "admitted acknowledgements must hold callback storage until disposal"
        );
        write_request(&mut client, list_packages_id, DaemonRequest::ListPackages);
        let remaining = setup_deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "setup timing: the held window expired before ListPackages"
        );
        client.set_read_timeout(Some(remaining)).unwrap();
        let frame = reader
            .read_frame(&mut client)
            .expect("setup timing: ListPackages must complete inside the held window");
        assert!(
            held_started.elapsed() < Duration::from_millis(500),
            "setup timing: the held window must stay below half of the callback timeout"
        );
        let DaemonUnixMuxFrame::Server(ServerFrame::Response {
            request_id,
            response: listed,
        }) = frame
        else {
            panic!("ListPackages must return a socket response");
        };
        assert_eq!(request_id, list_packages_id.to_string());
        assert_eq!(listed.kind, DaemonResponseKind::Packages, "{listed:?}");
        assert_eq!(
            listed.packages.len(),
            call_count,
            "the real owner serves a sibling request while Core is held"
        );
        release.0.take().unwrap().send(()).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut responses = BTreeMap::new();
        for _ in 0..call_count {
            let DaemonUnixMuxFrame::Server(ServerFrame::Response {
                request_id,
                response,
            }) = reader.read_frame(&mut client).unwrap()
            else {
                panic!("the daemon must return a correlated callback response");
            };
            assert!(responses.insert(request_id, response).is_none());
        }
        for index in 0..call_count {
            let response = responses.remove(&(41 + index).to_string()).unwrap();
            assert_eq!(
                response.kind,
                DaemonResponseKind::PluginMcpToolResult,
                "{response:?}"
            );
            assert_eq!(response.plugin_tool_result["token"], format!("ack-{index}"));
            assert_eq!(
                response.plugin_tool_result["result"]["state"]["envelope_id"],
                format!("c1-envelope-{index}")
            );
            assert_eq!(
                response.plugin_tool_result["result"]["state"]["status"],
                "acknowledged"
            );
        }
        let retirement_deadline = Instant::now() + Duration::from_secs(2);
        while waiters.iter().any(|waiter| retains_waiter(*waiter)) {
            assert!(
                Instant::now() < retirement_deadline,
                "normal delivery must retire all C1 waiters before shutdown"
            );
            thread::yield_now();
        }
        assert_eq!(
            callback_usage(),
            0,
            "callback storage must release after conversion and disposal, before shutdown"
        );
        write_request(&mut client, status_id, DaemonRequest::Status);
        assert_eq!(
            read_response(&mut client, &mut reader, status_id).kind,
            DaemonResponseKind::Status
        );
        write_request(&mut client, shutdown_id, DaemonRequest::DaemonShutdown);
        read_response(&mut client, &mut reader, shutdown_id);
        drop(client);
        assert!(owner.join().unwrap().is_ok());
        assert!(waiters.iter().all(|waiter| !retains_waiter(*waiter)));
        drop(stop);
        drop(retains_waiter);
        drop(bridge);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn terminal_coordination_drain_collects_two_staggered_core_completions() {
        struct ReleaseCore(Option<mpsc::Sender<()>>);
        impl Drop for ReleaseCore {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        struct DisposedOnHost(mpsc::Sender<String>);
        impl Drop for DisposedOnHost {
            fn drop(&mut self) {
                let _ = self
                    .0
                    .send(thread::current().name().unwrap_or("unnamed").into());
            }
        }
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = PathBuf::from("/private/tmp")
            .join(format!("hub-c1-terminal-{}-{unique}", std::process::id()));
        let config = package_control_config(root.join("data"));
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let (release_second_tx, release_second_rx) = mpsc::channel();
        let mut first_gate = ReleaseCore(Some(release_first_tx));
        let mut second_gate = ReleaseCore(Some(release_second_tx));
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (second_entered_tx, second_entered_rx) = mpsc::channel();
        let (first_disposed_tx, first_disposed_rx) = mpsc::channel();
        let (second_disposed_tx, second_disposed_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let owner = thread::Builder::new()
            .name("c1-terminal-owner".into())
            .spawn(move || {
                let mut daemon = HubDaemon::start(config).unwrap();
                let mut state = DaemonControlState::default();
                let (control, receiver) = tokio_mpsc::channel(8);
                let runtime = daemon.runtime().unwrap();
                runtime.bind_data_plane_owner_wake(control.clone());
                runtime.bind_host_owner_wake(control);
                drop(runtime.submit_core(move |_| {
                    first_entered_tx.send(()).unwrap();
                    release_first_rx
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }));
                let bridge = runtime.coordination_bridge();
                let response_first = bridge.test_queue_pending(
                    crate::lua_runtime::PendingCoordinationOperation::Drain {
                        target: botster_core::EnvelopeTarget::Topic {
                            topic: "terminal-first".into(),
                        },
                        after: None,
                        limit: 1,
                    },
                );
                bridge.test_set_pending_drop_probe(DisposedOnHost(first_disposed_tx));
                crate::daemon::control::coordination::accept_one(&mut daemon, &mut state);
                let runtime = daemon.runtime().unwrap();
                drop(runtime.submit_core(move |_| {
                    second_entered_tx.send(()).unwrap();
                    release_second_rx
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }));
                let response_second = bridge.test_queue_pending(
                    crate::lua_runtime::PendingCoordinationOperation::Drain {
                        target: botster_core::EnvelopeTarget::Topic {
                            topic: "terminal-second".into(),
                        },
                        after: None,
                        limit: 1,
                    },
                );
                bridge.test_set_pending_drop_probe(DisposedOnHost(second_disposed_tx));
                crate::daemon::control::coordination::accept_one(&mut daemon, &mut state);
                assert_eq!(state.pending_requests.len(), 2);
                let runtime = daemon.runtime().unwrap();
                let probe: Box<dyn Fn(crate::owner_identity::WaiterId) -> bool + Send> =
                    Box::new(runtime.test_core_waiter_probe());
                runtime.bind_terminal_core_owner();
                drop(receiver);
                started_tx
                    .send((bridge, probe, response_first, response_second))
                    .unwrap();
                drain_terminal_host_work(&mut daemon, &mut state);
                assert!(state.pending_requests.is_empty());
                assert_eq!(state.budget.outstanding(), 0);
                daemon.stop();
                finished_tx.send(()).unwrap();
            })
            .unwrap();
        let (bridge, retains_waiter, response_first, response_second) =
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let waiters = bridge.test_admitted_waiters();
        assert_eq!(waiters.len(), 2);
        first_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert!(matches!(
            response_first.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert!(matches!(
            response_second.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        first_gate.0.take().unwrap().send(()).unwrap();
        second_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert!(
            first_disposed_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .starts_with("botster-hub-host")
        );
        assert!(matches!(
            response_first.recv_timeout(Duration::from_secs(5)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        assert!(matches!(
            response_second.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert!(
            retains_waiter(waiters[1]),
            "the second row keeps its phase history while Core is held"
        );
        assert!(finished_rx.try_recv().is_err());
        second_gate.0.take().unwrap().send(()).unwrap();
        assert!(
            second_disposed_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .starts_with("botster-hub-host")
        );
        assert!(matches!(
            response_second.recv_timeout(Duration::from_secs(5)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        finished_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        owner.join().unwrap();
        assert!(waiters.iter().all(|waiter| !retains_waiter(*waiter)));
        drop(retains_waiter);
        drop(bridge);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn blocked_plugin_request_does_not_block_unrelated_owner_control() {
        let _gate_owner = ControlledPluginGateGuard::acquire();
        let root = unique_package_control_dir("controlled-plugin-gate");
        let data_directory = root.join("data");
        let package_dir = root.join("owner.controlled-gate");
        write_package_control_manifest(
            &package_dir,
            "owner.controlled-gate",
            serde_json::json!({
                "capabilities": [{ "surface": "mcp" }],
                "entrypoints": [
                    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
                ]
            }),
        );
        write_controlled_gate_lua_plugin(&package_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config).expect("start controlled gate daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install controlled gate plugin");
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "owner.controlled-gate".to_string(),
            },
        )
        .expect("enable controlled gate plugin");

        let mut state = DaemonControlState::default();
        crate::lua_runtime::arm_test_plugin_invocation_gate();
        let held_reply_rx = start_async_control_request(
            &mut daemon,
            &mut state,
            DaemonRequest::PluginMcpCallTool {
                name: "owner.controlled_gate".to_string(),
                arguments: serde_json::json!({ "token": "held-request-41" }),
            },
            "connection-held",
            "41",
        );
        // Two seconds is a test safety bound for gate entry.
        assert!(
            crate::lua_runtime::wait_for_test_plugin_invocation_gate(Duration::from_secs(2)),
            "the controlled plugin worker must enter the gate"
        );
        assert!(
            state
                .plugin_controls
                .has_transport_correlation("connection-held", "41"),
            "the held Core request must retain its exact transport correlation"
        );

        // Two seconds is a test safety bound. It is not a Status latency requirement.
        let status_started = Instant::now();
        let mut status_reply = start_async_control_request(
            &mut daemon,
            &mut state,
            DaemonRequest::Status,
            "connection-status",
            "7",
        );
        let status_deadline = Instant::now() + Duration::from_secs(2);
        let status = loop {
            drive_ready_test_turn(&mut daemon, &mut state);
            match status_reply.try_recv() {
                Ok(reply) => break reply.expect("status response"),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    assert!(
                        Instant::now() < status_deadline,
                        "unrelated status exceeded the safety deadline"
                    );
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("status reply channel failed: {error}"),
            }
        };
        assert_eq!(status.kind, DaemonResponseKind::Status);
        assert_eq!(
            state.pending_requests.len(),
            1,
            "the blocked plugin row must remain"
        );
        assert_eq!(state.budget.outstanding(), 1);
        assert!(
            status_started.elapsed() < Duration::from_secs(2),
            "unrelated owner control exceeded the safety deadline"
        );

        crate::lua_runtime::release_test_plugin_invocation_gate();
        let response = finish_async_plugin_control(&mut daemon, &mut state, held_reply_rx)
            .expect("held plugin response");
        assert_eq!(response.kind, DaemonResponseKind::PluginMcpToolResult);
        assert_eq!(response.plugin_tool_result["value"], "released");
        assert_eq!(response.plugin_tool_result["token"], "held-request-41");
        assert!(
            !state
                .plugin_controls
                .has_transport_correlation("connection-held", "41"),
            "the correlated row must retire after the exact held reply"
        );
        daemon.stop();
    }

    #[test]
    fn blocked_plugin_connection_returns_correlated_too_many_requests_before_release() {
        let _gate_owner = ControlledPluginGateGuard::acquire();
        let root = unique_package_control_dir("controlled-plugin-admission-limit");
        let data_directory = root.join("data");
        let package_dir = root.join("owner.controlled-gate");
        write_package_control_manifest(
            &package_dir,
            "owner.controlled-gate",
            serde_json::json!({
                "capabilities": [{ "surface": "mcp" }],
                "entrypoints": [
                    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
                ]
            }),
        );
        write_controlled_gate_lua_plugin(&package_dir);
        let config = package_control_config(data_directory);
        let core_capacity = config.plugin_worker_config().completion_queue_byte_capacity
            / (crate::lifecycle::REQUEST_RESPONSE_COMPLETION_ALLOWANCE
                + botster_core::PluginWorkerEngine::completion_reservation_metadata_bytes());
        assert_eq!(
            core_capacity + 1,
            botster_hub_client::MAX_OUTSTANDING_REQUESTS
        );
        let mut daemon = HubDaemon::start(config).expect("start admission-limit daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install admission-limit plugin");
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "owner.controlled-gate".to_string(),
            },
        )
        .expect("enable admission-limit plugin");

        let (server, mut client) = UnixStream::pair().expect("create plugin pressure socket pair");
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bound plugin pressure response reads");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection_tx = control_tx.clone();
        let connection = thread::spawn(move || handle_connection(server, connection_tx));
        write_hello(&mut client);
        let mut reader = DaemonUnixFrameReader::new();
        let _ = read_hello_ack(&mut client, &mut reader);

        let mut state = DaemonControlState::default();
        daemon
            .runtime()
            .expect("the runtime is active")
            .install_plugin_completion_notifier(state.plugin_result_budget.completion_notifier());
        let transport_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build plugin pressure transport runtime");
        let registration = receive_test_control_message(&mut control_rx);
        let terminal_mux = match &registration {
            ControlMessage::RegisterUnixAdmission {
                admission: UnixTerminalAdmission::Admitted { mux, .. },
                ..
            } => mux.clone(),
            _ => panic!("expected admitted Unix registration"),
        };
        let (mut terminal_adapter, terminal_handle) = terminal_mux.create_adapter();
        assert!(terminal_mux.register(
            "pressure-session".to_string(),
            "pressure-subscription".to_string(),
            1,
            terminal_handle,
        ));
        assert!(!handle_control_message(
            &mut daemon,
            &mut state,
            transport_runtime.handle(),
            control_tx.clone(),
            registration,
        ));

        crate::lua_runtime::arm_test_plugin_invocation_gate();
        for serial in 1..=botster_hub_client::MAX_OUTSTANDING_REQUESTS + 1 {
            write_request(
                &mut client,
                u64::try_from(serial).expect("request serial"),
                DaemonRequest::PluginMcpCallTool {
                    name: "owner.controlled_gate".to_string(),
                    arguments: serde_json::json!({ "token": serial }),
                },
            );
        }

        let mut admitted_messages = Vec::new();
        for serial in 1..=botster_hub_client::MAX_OUTSTANDING_REQUESTS {
            let message = receive_test_control_message(&mut control_rx);
            assert!(
                matches!(
                    &message,
                    ControlMessage::Request {
                        request,
                        transport_request_id: Some(request_id),
                        ..
                    } if matches!(request.as_ref(), DaemonRequest::PluginMcpCallTool { .. })
                        && request_id == &serial.to_string()
                ),
                "request {serial} must reach production connection and owner admission"
            );
            admitted_messages.push(message);
        }
        // The transport must observe all occupied slots before Core can release one.
        let first_refusal = reader
            .read_frame(&mut client)
            .expect("read capacity refusal");
        for message in admitted_messages {
            assert!(!handle_control_message(
                &mut daemon,
                &mut state,
                transport_runtime.handle(),
                control_tx.clone(),
                message,
            ));
        }
        assert!(
            crate::lua_runtime::wait_for_test_plugin_invocation_gate(Duration::from_secs(2)),
            "the controlled plugin worker must enter the gate"
        );

        let transport_refused_id = botster_hub_client::MAX_OUTSTANDING_REQUESTS + 1;
        let core_refused_id = core_capacity + 1;
        let second_refusal = reader
            .read_frame(&mut client)
            .expect("read capacity refusal");
        let mut refusals = std::collections::BTreeMap::new();
        for frame in [first_refusal, second_refusal] {
            let DaemonUnixMuxFrame::Server(ServerFrame::Response {
                request_id,
                response,
            }) = frame
            else {
                panic!("capacity refusal must use a response frame");
            };
            let request_id = request_id.parse::<usize>().expect("numeric request id");
            assert!(
                [core_refused_id, transport_refused_id].contains(&request_id),
                "unexpected refusal request ID {request_id}"
            );
            assert!(
                refusals.insert(request_id, response).is_none(),
                "request {request_id} answered twice"
            );
        }
        for (request_id, code) in [
            (
                transport_refused_id,
                botster_hub_client::OPERATOR_ERROR_TOO_MANY_REQUESTS,
            ),
            (core_refused_id, "plugin_invocation_backpressured"),
        ] {
            let response = refusals
                .remove(&request_id)
                .expect("the exact request has a refusal");
            assert_eq!(response.kind, DaemonResponseKind::OperatorError);
            assert_eq!(response.error.as_ref().unwrap().code, code);
        }
        assert!(refusals.is_empty());
        assert_eq!(state.pending_requests.len(), core_capacity);

        let terminal_body =
            encode_output(b"terminal-after-refusal").expect("encode terminal output after refusal");
        let expected_terminal_body = terminal_body.as_bytes().to_vec();
        terminal_adapter
            .try_write(&RoutedTerminalFrame::new(
                RouteId::new("pressure-subscription").expect("pressure route"),
                1,
                0,
                terminal_body,
            ))
            .expect("write terminal output after refusal");
        let terminal = read_terminal(&mut client, &mut reader);
        assert_eq!(terminal.route, "pressure-subscription");
        assert_eq!(terminal.generation, 1);
        assert_eq!(terminal.body, expected_terminal_body);

        let (sibling_server, mut sibling_client) =
            UnixStream::pair().expect("create sibling status socket pair");
        sibling_client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("bound sibling status reads");
        let sibling_tx = control_tx.clone();
        let sibling_connection =
            thread::spawn(move || handle_connection(sibling_server, sibling_tx));
        write_hello(&mut sibling_client);
        let mut sibling_reader = DaemonUnixFrameReader::new();
        let _ = read_hello_ack(&mut sibling_client, &mut sibling_reader);
        let sibling_registration = receive_test_control_message(&mut control_rx);
        assert!(matches!(
            sibling_registration,
            ControlMessage::RegisterUnixAdmission { .. }
        ));
        assert!(!handle_control_message(
            &mut daemon,
            &mut state,
            transport_runtime.handle(),
            control_tx.clone(),
            sibling_registration,
        ));
        write_request(&mut sibling_client, 1, DaemonRequest::Status);
        let sibling_status = receive_test_control_message(&mut control_rx);
        assert!(matches!(
            &sibling_status,
            ControlMessage::Request { request, .. }
                if matches!(request.as_ref(), DaemonRequest::Status)
        ));
        assert!(!handle_control_message(
            &mut daemon,
            &mut state,
            transport_runtime.handle(),
            control_tx.clone(),
            sibling_status,
        ));
        let sibling_deadline = Instant::now() + Duration::from_secs(2);
        while state.pending_requests.len() > core_capacity {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state,));
            assert!(
                Instant::now() < sibling_deadline,
                "the sibling Status request must complete while the plugin handler is held"
            );
            if state.pending_requests.len() > core_capacity {
                thread::sleep(Duration::from_millis(5));
            }
        }
        assert_eq!(
            read_response(&mut sibling_client, &mut sibling_reader, 1).kind,
            DaemonResponseKind::Status
        );
        sibling_client
            .shutdown(Shutdown::Both)
            .expect("disconnect sibling status client");
        sibling_connection
            .join()
            .expect("join sibling status connection")
            .expect("sibling status disconnect is clean");

        crate::lua_runtime::release_test_plugin_invocation_gate();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !state.pending_requests.is_empty() {
            assert!(!drive_ready_test_turn(&mut daemon, &mut state,));
            assert!(
                Instant::now() < deadline,
                "admitted plugin requests did not complete after gate release"
            );
            if !state.pending_requests.is_empty() {
                thread::sleep(Duration::from_millis(5));
            }
        }

        let mut completed = vec![false; botster_hub_client::MAX_OUTSTANDING_REQUESTS + 1];
        completed[core_refused_id] = true;
        for _ in 0..core_capacity {
            let DaemonUnixMuxFrame::Server(ServerFrame::Response {
                request_id,
                response,
            }) = reader
                .read_frame(&mut client)
                .expect("read admitted plugin response")
            else {
                panic!("admitted plugin request must return a response frame");
            };
            let request_id = request_id.parse::<usize>().expect("numeric request id");
            assert!((1..=botster_hub_client::MAX_OUTSTANDING_REQUESTS).contains(&request_id));
            assert!(
                !completed[request_id],
                "request {request_id} answered twice"
            );
            assert_eq!(response.kind, DaemonResponseKind::PluginMcpToolResult);
            completed[request_id] = true;
        }
        assert!(completed[1..].iter().all(|completed| *completed));
        assert!(!state.plugin_controls.has_pending());
        client
            .shutdown(Shutdown::Both)
            .expect("disconnect plugin pressure client");
        connection
            .join()
            .expect("join plugin pressure connection")
            .expect("plugin pressure disconnect is clean");
        daemon.stop();
    }

    #[test]
    fn asynchronous_plugin_paths_preserve_success_and_failure_response_shapes() {
        let root = unique_package_control_dir("async-plugin-response-shapes");
        let data_directory = root.join("data");
        let package_dir = root.join("owner-responses");
        write_package_control_manifest(
            &package_dir,
            "owner-responses",
            serde_json::json!({
                "capabilities": [
                    { "surface": "mcp" },
                    { "surface": "surfaces" }
                ],
                "surfaces": [{
                    "id": "response.surface",
                    "kind": "app",
                    "title": "Response",
                    "supports": ["render", "action"]
                }],
                "entrypoints": [
                    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
                ]
            }),
        );
        write_async_plugin_response_fixture(&package_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config).expect("start response-shape daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install response-shape plugin");
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "owner-responses".to_string(),
            },
        )
        .expect("enable response-shape plugin");
        let mut state = DaemonControlState::default();

        let mcp_success = drive_async_plugin_control(
            &mut daemon,
            &mut state,
            DaemonRequest::PluginMcpCallTool {
                name: "owner-responses.responses".to_string(),
                arguments: serde_json::json!({ "fail": false }),
            },
            "mcp-success",
        )
        .expect("MCP success response");
        assert_eq!(mcp_success.kind, DaemonResponseKind::PluginMcpToolResult);
        assert_eq!(mcp_success.plugin_tool_result["path"], "mcp");

        let mcp_failure = drive_async_plugin_control(
            &mut daemon,
            &mut state,
            DaemonRequest::PluginMcpCallTool {
                name: "owner-responses.responses".to_string(),
                arguments: serde_json::json!({ "fail": true }),
            },
            "mcp-failure",
        )
        .expect("MCP handler failures are typed operator responses");
        assert_eq!(mcp_failure.kind, DaemonResponseKind::OperatorError);
        assert!(mcp_failure.error.is_some());

        let render_success = drive_async_plugin_control(
            &mut daemon,
            &mut state,
            DaemonRequest::PluginSurfaceRender {
                package_name: "owner-responses".to_string(),
                surface_id: "response.surface".to_string(),
                payload: serde_json::json!({ "fail": false }),
            },
            "render-success",
        )
        .expect("render success response");
        assert_eq!(render_success.kind, DaemonResponseKind::PluginSurface);
        assert_eq!(
            render_success
                .plugin_surface
                .as_ref()
                .map(|surface| surface.surface_id.as_str()),
            Some("response.surface")
        );
        assert!(matches!(
            drive_async_plugin_control(
                &mut daemon,
                &mut state,
                DaemonRequest::PluginSurfaceRender {
                    package_name: "owner-responses".to_string(),
                    surface_id: "response.surface".to_string(),
                    payload: serde_json::json!({ "fail": true }),
                },
                "render-failure",
            ),
            Ok(DaemonResponse {
                kind: DaemonResponseKind::OperatorError,
                ..
            })
        ));

        let action_request = |request_id: &str, fail: bool| {
            serde_json::from_value(serde_json::json!({
                "request_id": request_id,
                "surface_id": "response.surface",
                "action_id": "response.action",
                "node_id": "response-form",
                "kind": "submit",
                "values": {},
                "payload": { "fail": fail }
            }))
            .expect("canonical UI action request")
        };
        let action_success = drive_async_plugin_control(
            &mut daemon,
            &mut state,
            DaemonRequest::PluginSurfaceAction {
                package_name: "owner-responses".to_string(),
                request: action_request("action-success", false),
            },
            "action-success",
        )
        .expect("action success response");
        assert_eq!(action_success.kind, DaemonResponseKind::PluginActionResult);
        assert_eq!(
            action_success
                .plugin_action_result
                .as_ref()
                .and_then(|result| result.payload.as_ref())
                .map(|payload| &payload["path"]),
            Some(&serde_json::json!("action"))
        );
        assert!(matches!(
            drive_async_plugin_control(
                &mut daemon,
                &mut state,
                DaemonRequest::PluginSurfaceAction {
                    package_name: "owner-responses".to_string(),
                    request: action_request("action-failure", true),
                },
                "action-failure",
            ),
            Ok(DaemonResponse {
                kind: DaemonResponseKind::OperatorError,
                ..
            })
        ));

        for (subscription_id, expected_kind) in [
            ("entity-success", DaemonResponseKind::EntitySubscribed),
            ("entity-failure", DaemonResponseKind::OperatorError),
        ] {
            let (frame_tx, _frame_rx) = tokio_mpsc::channel(8);
            let (reply_tx, mut reply_rx) = crate::daemon::control::message::control_reply_channel();
            crate::daemon::control::entities::handle(
                &mut daemon,
                &mut state,
                ControlMessage::SubscribeEntities {
                    entity_type: "owner-responses.entity".to_string(),
                    subscription_id: subscription_id.to_string(),
                    transport_request_id: Some(format!("transport-{subscription_id}")),
                    client_id: Some("response-fixture-connection".to_string()),
                    frame_tx: crate::subscription::entity::EntityFrameSender::Async(frame_tx),
                    frame_rx: None,
                    reply_tx,
                    grant_id: None,
                },
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            let response = loop {
                drive_ready_test_turn(&mut daemon, &mut state);
                match reply_rx.try_recv() {
                    Ok(reply) => break reply.into_parts().0.expect("entity response"),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                        assert!(Instant::now() < deadline, "entity response timed out");
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("entity reply channel failed: {error}"),
                }
            };
            assert_eq!(response.kind, expected_kind);
            if expected_kind == DaemonResponseKind::OperatorError {
                assert!(response.error.is_some());
            }
        }
        daemon.stop();
    }

    #[test]
    fn plugin_response_waits_for_host_capacity_and_retains_the_same_row_through_cleanup() {
        let _gate_owner = ControlledPluginGateGuard::acquire();
        for retirement in [None, Some("connection_close"), Some("deadline")] {
            let root = unique_package_control_dir(&format!("controlled-plugin-{retirement:?}"));
            let data_directory = root.join("data");
            let package_dir = root.join("owner.controlled-gate");
            write_package_control_manifest(
                &package_dir,
                "owner.controlled-gate",
                serde_json::json!({
                    "capabilities": [{ "surface": "mcp" }],
                    "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
                }),
            );
            write_controlled_gate_lua_plugin(&package_dir);
            let config = package_control_config(data_directory);
            let mut daemon = HubDaemon::start(config).expect("start controlled gate daemon");
            drive_package_request(
                &mut daemon,
                DaemonRequest::InstallPackageLocalPath { path: package_dir },
            )
            .expect("install controlled gate plugin");
            drive_package_request(
                &mut daemon,
                DaemonRequest::EnablePackage {
                    package_name: "owner.controlled-gate".to_string(),
                },
            )
            .expect("enable controlled gate plugin");
            let mut host_permits = (0..crate::host_executor::HOST_OPERATION_CAPACITY)
                .map(|_| {
                    daemon
                        .runtime()
                        .expect("runtime")
                        .host_executor()
                        .try_reserve()
                        .expect("fill host capacity")
                })
                .collect::<Vec<_>>();
            let mut state = DaemonControlState::default();
            crate::lua_runtime::arm_test_plugin_invocation_gate();
            let reply_rx = start_async_control_request(
                &mut daemon,
                &mut state,
                DaemonRequest::PluginMcpCallTool {
                    name: "owner.controlled_gate".to_string(),
                    arguments: serde_json::json!({ "token": "host-capacity" }),
                },
                "response-fixture-connection",
                "91",
            );
            assert!(crate::lua_runtime::wait_for_test_plugin_invocation_gate(
                Duration::from_secs(2)
            ));
            crate::lua_runtime::release_test_plugin_invocation_gate();
            let deadline = Instant::now() + Duration::from_secs(5);
            while !state.plugin_controls.has_capacity_waiters() {
                drive_ready_test_turn(&mut daemon, &mut state);
                assert!(
                    Instant::now() < deadline,
                    "raw result must park for host capacity"
                );
                thread::sleep(Duration::from_millis(5));
            }
            let waiter_id = *state
                .pending_requests
                .keys()
                .next()
                .expect("same owner row");
            assert_eq!(state.pending_requests.len(), 1);
            assert_eq!(state.budget.outstanding(), 1);
            assert!(state.plugin_result_budget.retained_bytes() > 0);
            let mut reply_rx = Some(reply_rx);
            match retirement {
                Some("connection_close") => {
                    drop(reply_rx.take());
                    crate::daemon::control::pending::retire_abandoned_requests(
                        &mut daemon,
                        &mut state,
                        "response-fixture-connection",
                    );
                }
                Some("deadline") => {
                    crate::daemon::control::pending::mark_owner_ready(
                        &mut state,
                        waiter_id,
                        crate::daemon::owner_schedule::ReadyClass::Deadline,
                        crate::daemon::control::pending::READY_DEADLINE,
                    );
                    drive_ready_test_turn(&mut daemon, &mut state);
                }
                None => {}
                _ => unreachable!(),
            }
            assert!(state.pending_requests.contains_key(&waiter_id));
            assert_eq!(state.budget.outstanding(), 1);
            assert!(state.plugin_result_budget.retained_bytes() > 0);
            drop(host_permits.pop());
            if retirement.is_none() {
                let response =
                    finish_async_plugin_control(&mut daemon, &mut state, reply_rx.take().unwrap())
                        .expect("host capacity wake delivers reply");
                assert_eq!(response.plugin_tool_result["token"], "host-capacity");
            } else {
                while state.plugin_controls.has_pending() {
                    drive_ready_test_turn(&mut daemon, &mut state);
                    assert!(
                        Instant::now() < deadline,
                        "cancelled response must finish worker cleanup"
                    );
                    thread::sleep(Duration::from_millis(5));
                }
                if let Some(mut receiver) = reply_rx {
                    assert!(matches!(
                        receiver.try_recv(),
                        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
                    ));
                }
            }
            assert!(state.pending_requests.is_empty());
            assert_eq!(state.budget.outstanding(), 0);
            assert_eq!(state.plugin_result_budget.retained_bytes(), 0);
            drop(host_permits);
            daemon.stop();
        }
    }

    #[test]
    fn second_session_subscriber_receives_snapshot_without_a_new_journal_change() {
        let root = unique_package_control_dir("second-session-subscriber");
        let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
            .expect("start the session subscription daemon");
        let session_id = botster_core::SessionId("shared-session".into());
        daemon
            .runtime_mut()
            .unwrap()
            .spawn_session_for_test(
                botster_core::SessionSpawnRequest {
                    request_id: botster_core::RequestId("shared-session-spawn".into()),
                    session_id: session_id.clone(),
                    executable: "/bin/sh".into(),
                    arguments: vec!["-c".into(), "while IFS= read -r line; do :; done".into()],
                    working_directory: botster_core::SpawnWorkingDirectory { path: ".".into() },
                    environment: botster_core::SpawnEnvironment::default(),
                    initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                },
                botster_core::CoreSessionMetadata::new(),
            )
            .expect("spawn the shared session");
        let mut state = DaemonControlState::default();
        seed_lifecycle_reconciliation(&mut daemon, &mut state);
        let mut receivers = Vec::new();
        let mut first_cursor = None;
        for peer in ["peer-a", "peer-b"] {
            let (sender, receiver) = mpsc::sync_channel(8);
            let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
            crate::daemon::control::entities::handle(
                &mut daemon,
                &mut state,
                ControlMessage::SubscribeEntities {
                    entity_type: "session".into(),
                    subscription_id: peer.into(),
                    transport_request_id: Some("1".into()),
                    client_id: Some(peer.into()),
                    frame_tx: crate::subscription::entity::EntityFrameSender::Blocking(sender),
                    frame_rx: None,
                    reply_tx,
                    grant_id: None,
                },
            );
            let response =
                receive_test_control_reply(reply_rx).expect("the daemon admits the subscriber");
            assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
            assert!(state.entity_subscriptions.contains_key(peer));
            let deadline = Instant::now() + Duration::from_secs(3);
            let frame = loop {
                drive_ready_test_turn(&mut daemon, &mut state);
                if let Ok(frame) = receiver.try_recv() {
                    break frame;
                }
                assert!(
                    Instant::now() < deadline,
                    "{peer} must receive a snapshot through the production owner dispatcher"
                );
                thread::sleep(Duration::from_millis(1));
            };
            let botster_hub_client::DaemonEntityFrame::Snapshot {
                subscription_id,
                items,
                ..
            } = frame
            else {
                panic!("the first frame must be a snapshot");
            };
            assert_eq!(subscription_id, peer);
            assert!(
                items
                    .iter()
                    .any(|item| item["session_uuid"] == session_id.0)
            );
            assert!(state.maintenance.projection_caught_up());
            assert!(!state.maintenance.projection_dirty);
            let cursor = state.maintenance.projection.cursor.clone();
            if peer == "peer-a" {
                first_cursor = cursor;
            } else {
                assert_eq!(
                    cursor, first_cursor,
                    "the second subscription must not need a new journal change"
                );
            }
            receivers.push(receiver);
        }
        assert_eq!(state.entity_subscriptions.len(), 2);
        daemon
            .runtime_mut()
            .unwrap()
            .shutdown_session_for_test(session_id)
            .expect("stop the shared session");
        daemon.stop();
        std::fs::remove_dir_all(root).expect("remove the session subscription test directory");
    }

    #[test]
    fn shutdown_waits_for_worker_reclamation_of_entity_payload() {
        let _gate_owner = ControlledPluginGateGuard::acquire();
        for source in ["provider", "fanout"] {
            let root = unique_package_control_dir(&format!("shutdown-entity-payload-{source}"));
            let package_dir = root.join("owner-entity-gate");
            write_package_control_manifest(
                &package_dir,
                "owner-entity-gate",
                serde_json::json!({
                    "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
                }),
            );
            write_controlled_entity_gate_lua_plugin(&package_dir);
            let mut daemon = HubDaemon::start(package_control_config(root.join("data")))
                .expect("start entity shutdown daemon");
            drive_package_request(
                &mut daemon,
                DaemonRequest::InstallPackageLocalPath { path: package_dir },
            )
            .expect("install entity provider");
            drive_package_request(
                &mut daemon,
                DaemonRequest::EnablePackage {
                    package_name: "owner-entity-gate".into(),
                },
            )
            .expect("enable entity provider");
            let mut state = DaemonControlState::default();
            daemon
                .runtime()
                .unwrap()
                .install_plugin_completion_notifier(
                    state.plugin_result_budget.completion_notifier(),
                );
            let baseline = state.budget.outstanding();
            let (frame_tx, _frame_rx) = tokio_mpsc::channel(8);
            let (reply_tx, mut entity_reply) =
                crate::daemon::control::message::control_reply_channel();
            if source == "provider" {
                crate::daemon::control::entities::handle(
                    &mut daemon,
                    &mut state,
                    ControlMessage::SubscribeEntities {
                        entity_type: "owner-entity-gate.entity".into(),
                        subscription_id: "shutdown-entity".into(),
                        transport_request_id: Some("entity-request".into()),
                        client_id: Some("entity-client".into()),
                        frame_tx: crate::subscription::entity::EntityFrameSender::Async(frame_tx),
                        frame_rx: None,
                        reply_tx,
                        grant_id: None,
                    },
                );
            } else {
                let admitted = daemon
                    .runtime()
                    .unwrap()
                    .test_admit_publish(
                        "owner-entity-gate",
                        serde_json::json!({
                            "type": "entity_upsert",
                            "entity_type": "owner-entity-gate.entity",
                            "snapshot_seq": 1,
                            "id": "entity-1",
                            "entity": {"id": "entity-1", "text": "x".repeat(262_144)},
                        }),
                        None,
                    )
                    .expect("admit the large entity mutation through production admission");
                assert!(admitted.ok);
                crate::daemon::control::entities::begin_package_entity_fanout(&daemon, &mut state);
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while !state.plugin_entities.has_retained_snapshot_payload() {
                if let Ok(reply) = entity_reply.try_recv() {
                    panic!(
                        "the provider must prepare a snapshot before replying: {:?}",
                        reply.into_parts().0
                    );
                }
                publish_completion_wakes(&daemon, &mut state);
                publish_maintenance_wakes(&mut state);
                if let Some(item) = state.owner_ready.pop_next() {
                    let mut budget =
                        crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
                    assert!(!dispatch_owner_ready_item(
                        &mut daemon,
                        &mut state,
                        item,
                        &mut budget
                    ));
                }
                assert!(
                    Instant::now() < deadline,
                    "the entity row must retain its prepared payload"
                );
                thread::yield_now();
            }
            let mut shutdown_reply = start_async_control_request(
                &mut daemon,
                &mut state,
                DaemonRequest::DaemonShutdown,
                "shutdown-client",
                "shutdown-request",
            );
            let shutdown_waiter = state.shutdown_waiter.unwrap();
            // Hold the real preparation receipt before it can start entity cancellation.
            // Shutdown preparation must use a Host worker before all workers are gated.
            let prepared = loop {
                let runtime = daemon.runtime().unwrap();
                let identities = runtime.take_owner_core_completions(
                    crate::data_plane::driver::CORE_OWNER_COMPLETION_CAPACITY,
                );
                let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
                let consumed = crate::daemon::control::pending::absorb_core_completions(
                    &mut state,
                    &identities,
                    &mut budget,
                );
                runtime.restore_owner_core_completions(&identities[consumed..]);
                let mut entry = state.pending_requests.remove(&shutdown_waiter).unwrap();
                assert!(matches!(
                    entry.continuation.poll(&mut daemon, &mut state),
                    crate::daemon::control::pending::ControlPoll::Pending,
                ));
                state.pending_requests.insert(shutdown_waiter, entry);
                match daemon.runtime().unwrap().host_executor().poll_completion() {
                    crate::host_executor::HostCompletionPoll::Ready(completion)
                        if completion.identity.waiter_id == shutdown_waiter =>
                    {
                        assert_eq!(completion.identity.phase, 1);
                        assert!(matches!(
                            completion.result,
                            crate::host_executor::HostResult::StatusResponsePrepared(_)
                        ));
                        break completion;
                    }
                    crate::host_executor::HostCompletionPoll::Ready(completion) => {
                        crate::subscription::entity::route_host_completion(&mut state, completion);
                    }
                    crate::host_executor::HostCompletionPoll::Empty => {}
                    crate::host_executor::HostCompletionPoll::Stopped => {
                        panic!("Host must prepare Shutdown")
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "Shutdown preparation must complete"
                );
                thread::yield_now();
            };
            let mut gates = Vec::new();
            for _ in 0..crate::host_executor::HOST_WORKER_COUNT {
                let gate = std::sync::Arc::new(crate::host_executor::TestHostGate::default());
                let executor = daemon.runtime().unwrap().host_executor();
                executor
                    .submit(
                        crate::host_executor::HostJobIdentity::first(
                            state.waiter_ids.next().unwrap(),
                        ),
                        crate::host_executor::HostCommand::Wait {
                            generation: 0,
                            gate: gate.clone(),
                        },
                        executor.try_reserve().expect("reserve a worker gate"),
                    )
                    .expect("submit worker gate");
                while !gate.has_started() {
                    assert!(Instant::now() < deadline, "the worker gate must start");
                    thread::yield_now();
                }
                gates.push(gate);
            }

            crate::subscription::entity::route_host_completion(&mut state, prepared);
            let (late_frame_tx, _late_frame_rx) = tokio_mpsc::channel(8);
            let (late_reply_tx, late_reply_rx) =
                crate::daemon::control::message::control_reply_channel();
            crate::daemon::control::entities::handle(
                &mut daemon,
                &mut state,
                ControlMessage::SubscribeEntities {
                    entity_type: "owner-entity-gate.entity".into(),
                    subscription_id: "late-entity".into(),
                    transport_request_id: Some("late-request".into()),
                    client_id: Some("late-client".into()),
                    frame_tx: crate::subscription::entity::EntityFrameSender::Async(late_frame_tx),
                    frame_rx: None,
                    reply_tx: late_reply_tx,
                    grant_id: None,
                },
            );
            let refusal =
                receive_test_control_reply(late_reply_rx).expect("late subscription response");
            assert_eq!(
                refusal.error.as_ref().map(|error| error.code.as_str()),
                Some("daemon_shutting_down")
            );
            loop {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                if !state.host_completions.contains_key(&shutdown_waiter)
                    && state
                        .plugin_entities
                        .test_cancelled_host_identity()
                        .is_some()
                    && state.owner_ready.is_empty()
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "Shutdown must consume preparation and wait for Host cleanup: {source}"
                );
                thread::yield_now();
            }
            assert_eq!(state.pending_requests[&shutdown_waiter].last_host_phase, 1);
            assert_eq!(state.shutdown_waiter, Some(shutdown_waiter));
            let cancelled_host_identity = state
                .plugin_entities
                .test_cancelled_host_identity()
                .expect("the cancelled entity owns a running Host phase");
            assert_ne!(cancelled_host_identity.waiter_id, shutdown_waiter);
            assert!(
                state
                    .plugin_entities
                    .has_waiter(cancelled_host_identity.waiter_id)
            );
            assert!(
                state
                    .plugin_entities
                    .accepts_host_completion(cancelled_host_identity)
            );
            // Model cleanup can precede payload disposal under the same retained permit.
            if state.plugin_entities.has_retained_snapshot_payload() {
                assert!(
                    !daemon.runtime().unwrap().entity_model_available(),
                    "a retained payload must wait for its Host model operation: {source}"
                );
            }
            assert!(
                daemon.runtime().unwrap().host_executor().outstanding()
                    >= crate::host_executor::HOST_WORKER_COUNT + 2
            );
            for _ in 0..3 {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    state.owner_ready.is_empty(),
                    "blocked cleanup must not spin: {source}"
                );
                assert_eq!(
                    state.plugin_entities.test_cancelled_host_identity(),
                    Some(cancelled_host_identity)
                );
            }
            assert!(crate::daemon::control::entities::plugin_entity_cleanup_pending(&state));
            assert_eq!(state.budget.outstanding(), baseline + 2);
            assert!(matches!(
                shutdown_reply.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            for gate in gates {
                gate.release();
            }
            while !drive_ready_test_turn(&mut daemon, &mut state) {
                assert!(
                    Instant::now() < deadline,
                    "shutdown must wait for entity worker cleanup"
                );
                thread::yield_now();
            }
            let response = receive_test_control_reply(shutdown_reply).expect("shutdown response");
            assert_eq!(response.kind, DaemonResponseKind::Shutdown);
            assert!(!crate::daemon::control::entities::plugin_entity_cleanup_pending(&state));
            assert!(!state.plugin_entities.has_retained_snapshot_payload());
            assert!(
                !state
                    .plugin_entities
                    .has_waiter(cancelled_host_identity.waiter_id)
            );
            assert!(
                state
                    .plugin_entities
                    .test_cancelled_host_identity()
                    .is_none()
            );
            assert_eq!(state.budget.outstanding(), baseline);
            assert_eq!(state.plugin_result_budget.retained_bytes(), 0);
            daemon.stop();
            std::fs::remove_dir_all(root).expect("remove entity shutdown test directory");
        }
    }

    #[test]
    fn abandoned_plugin_entity_subscriptions_release_owner_capacity() {
        let _gate_owner = ControlledPluginGateGuard::acquire();
        for retire_reason in ["connection_close", "deadline"] {
            let root =
                unique_package_control_dir(&format!("controlled-plugin-entity-{retire_reason}"));
            let data_directory = root.join("data");
            let package_dir = root.join("owner-entity-gate");
            write_package_control_manifest(
                &package_dir,
                "owner-entity-gate",
                serde_json::json!({
                    "entrypoints": [
                        { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
                    ]
                }),
            );
            write_controlled_entity_gate_lua_plugin(&package_dir);
            let config = package_control_config(data_directory);
            let mut daemon = HubDaemon::start(config).expect("start controlled entity daemon");
            drive_package_request(
                &mut daemon,
                DaemonRequest::InstallPackageLocalPath { path: package_dir },
            )
            .expect("install controlled entity plugin");
            drive_package_request(
                &mut daemon,
                DaemonRequest::EnablePackage {
                    package_name: "owner-entity-gate".to_string(),
                },
            )
            .expect("enable controlled entity plugin");

            let mut state = DaemonControlState::default();
            daemon
                .runtime()
                .unwrap()
                .install_plugin_completion_notifier(
                    state.plugin_result_budget.completion_notifier(),
                );
            let baseline = state.budget.outstanding();
            let connection_id = format!("entity-connection-{retire_reason}");
            let (frame_tx, _frame_rx) = tokio_mpsc::channel(8);
            let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
            crate::lua_runtime::arm_test_plugin_invocation_gate();
            crate::daemon::control::entities::handle(
                &mut daemon,
                &mut state,
                ControlMessage::SubscribeEntities {
                    entity_type: "owner-entity-gate.entity".to_string(),
                    subscription_id: format!("entity-subscription-{retire_reason}"),
                    transport_request_id: Some("92".to_string()),
                    client_id: Some(connection_id.clone()),
                    frame_tx: crate::subscription::entity::EntityFrameSender::Async(frame_tx),
                    frame_rx: None,
                    reply_tx,
                    grant_id: None,
                },
            );
            assert_eq!(state.budget.outstanding(), baseline + 1);
            let deadline = Instant::now() + Duration::from_secs(2);
            while !crate::lua_runtime::wait_for_test_plugin_invocation_gate(Duration::ZERO)
                || daemon.runtime().unwrap().host_executor().outstanding() != 0
            {
                assert!(!drive_ready_test_turn(&mut daemon, &mut state));
                assert!(
                    Instant::now() < deadline,
                    "provider admission must release Host capacity while Lua waits"
                );
                thread::yield_now();
            }

            if retire_reason == "connection_close" {
                crate::daemon::control::entities::retire_plugin_entity_connection(
                    &daemon,
                    &mut state,
                    &connection_id,
                );
            } else {
                let mut budget = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
                crate::daemon::control::pending::mark_due_owner_deadlines(
                    &mut state,
                    Instant::now() + crate::daemon::owner_budget::RETAINED_OPERATION_DEADLINE,
                    &mut budget,
                );
                let item = state.owner_ready.pop_next().expect("expired entity waiter");
                assert!(
                    crate::daemon::control::entities::drive_plugin_entity_ready_item(
                        &mut daemon,
                        &mut state,
                        item,
                    )
                );
            }

            assert_eq!(state.budget.outstanding(), baseline + 1);
            assert!(state.deadlines.is_empty());
            assert!(
                crate::lua_runtime::wait_for_test_plugin_invocation_gate(Duration::ZERO),
                "Core execution must remain live while Hub retains entity cleanup capacity"
            );

            crate::lua_runtime::release_test_plugin_invocation_gate();
            drop(reply_rx);
            let deadline = Instant::now() + Duration::from_secs(5);
            while state.budget.outstanding() != baseline {
                drive_ready_test_turn(&mut daemon, &mut state);
                assert!(
                    Instant::now() < deadline,
                    "entity cancellation must finish worker cleanup"
                );
                thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(state.plugin_result_budget.retained_bytes(), 0);
            daemon.stop();
        }
    }

    fn write_broken_entrypoint(package_dir: &Path) {
        std::fs::create_dir_all(package_dir.join("bin")).expect("create bin");
        std::fs::write(package_dir.join("bin/gone"), "not-executable\n").expect("write decoy");
    }

    fn broken_sleeper_manifest() -> serde_json::Value {
        serde_json::json!({
            "runnable_entrypoints": [{
                "id": "sleeper",
                "kind": "terminal_app",
                "command": "bin/gone",
                "args": ["30"],
                "launch_mode": "background",
                "may_supervise": true
            }]
        })
    }

    fn entrypoint_is_running(daemon: &mut HubDaemon, package_name: &str) -> bool {
        let response = drive_package_request(
            daemon,
            DaemonRequest::PackageEntrypointStatus {
                package_name: package_name.to_string(),
                entrypoint_id: "sleeper".to_string(),
            },
        )
        .expect("read entrypoint status through the host executor");
        response.packages.iter().any(|package| {
            package.package_name == package_name
                && package.runnable_entrypoints.iter().any(|entrypoint| {
                    entrypoint.id == "sleeper" && entrypoint.process.state == "running"
                })
        })
    }

    fn entrypoint_command<'a>(daemon: &'a HubDaemon, package_name: &str) -> &'a str {
        daemon
            .package_registry()
            .package(package_name)
            .expect("package")
            .runnable_entrypoints
            .iter()
            .find(|entrypoint| entrypoint.id == "sleeper")
            .expect("sleeper entrypoint")
            .command
            .as_str()
    }

    #[test]
    fn shutdown_finishes_accepted_package_reload_before_stopping_entrypoints() {
        let root = unique_package_control_dir("shutdown-package-reload");
        let package_dir = root.join("shutdown.plugin");
        write_package_control_manifest(&package_dir, "shutdown.plugin", sleeper_manifest(&["30"]));
        write_sleeper_script(&package_dir);
        let mut daemon =
            HubDaemon::start(package_control_config(root.join("data"))).expect("daemon");
        for request in [
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
            DaemonRequest::EnablePackage {
                package_name: "shutdown.plugin".to_string(),
            },
            DaemonRequest::StartPackageEntrypoint {
                package_name: "shutdown.plugin".to_string(),
                entrypoint_id: "sleeper".to_string(),
                environment_overrides: BTreeMap::new(),
            },
        ] {
            assert!(
                drive_package_request(&mut daemon, request)
                    .expect("package setup")
                    .error
                    .is_none()
            );
        }
        assert!(entrypoint_is_running(&mut daemon, "shutdown.plugin"));
        let mut state = DaemonControlState::default();
        let transport = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("transport runtime");
        let (control_tx, _control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let mut replies = Vec::new();
        // Admit both requests before the first continuation can commit or reload.
        for request in [
            DaemonRequest::ReloadPackage {
                package_name: "shutdown.plugin".to_string(),
            },
            DaemonRequest::DaemonShutdown,
        ] {
            let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
            assert!(!crate::daemon::control::request::handle(
                &mut daemon,
                &mut state,
                transport.handle(),
                control_tx.clone(),
                ControlMessage::Request {
                    request: Box::new(request),
                    transport_request_id: None,
                    reply_tx,
                    response_delivery_rx: None,
                    grant_id: None,
                    client_id: None,
                    enqueued_at: Instant::now(),
                },
            ));
            replies.push(reply_rx);
        }
        assert_eq!(state.pending_requests.len(), 2);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !drive_ready_test_turn(&mut daemon, &mut state) {
            assert!(
                Instant::now() < deadline,
                "shutdown must finish through the production dispatcher"
            );
            thread::yield_now();
        }
        let shutdown = receive_test_control_reply(replies.pop().expect("shutdown reply"))
            .expect("shutdown response");
        let reload = receive_test_control_reply(replies.pop().expect("reload reply"))
            .expect("reload response");
        assert!(
            reload.error.is_none(),
            "accepted reload must finish: {:?}",
            reload.error
        );
        assert_eq!(shutdown.kind, DaemonResponseKind::Shutdown);
        assert!(state.pending_requests.is_empty());
        assert_eq!(state.budget.outstanding(), 0);
        assert!(
            !entrypoint_is_running(&mut daemon, "shutdown.plugin"),
            "shutdown cannot leave a reloaded process running"
        );
        daemon.stop();
        std::fs::remove_dir_all(root).expect("remove shutdown test directory");
    }

    #[test]
    fn failed_package_persist_leaves_live_registry_equal_to_durable_snapshot() {
        let root = unique_package_control_dir("persist-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("mutate.plugin");
        write_package_control_manifest(&package_dir, "mutate.plugin", serde_json::json!({}));
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start package control daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install local package");
        assert_eq!(
            package_state(&daemon, "mutate.plugin"),
            PackageState::Installed
        );

        FileHubStateStore::inject_next_save_failure(&config.data_directory);
        let response = drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "mutate.plugin".to_string(),
            },
        )
        .expect("host failure response");
        assert_eq!(
            response.error.expect("host error").code,
            "hub_state_commit_failed"
        );
        assert_eq!(
            package_state(&daemon, "mutate.plugin"),
            PackageState::Installed
        );
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn failed_disable_commit_does_not_stop_or_unload_running_package() {
        let root = unique_package_control_dir("disable-running");
        let data_directory = root.join("data");
        let package_dir = root.join("running.plugin");
        write_package_control_manifest(
            &package_dir,
            "running.plugin",
            lua_and_sleeper_manifest(&["30"]),
        );
        write_sleeper_script(&package_dir);
        write_lua_plugin(&package_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start disable-running daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install running package");
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "running.plugin".to_string(),
            },
        )
        .expect("enable running package");
        assert!(
            plugin_is_loaded(&daemon, "running.plugin"),
            "lua plugin must be loaded before failed disable"
        );
        drive_package_request(
            &mut daemon,
            DaemonRequest::StartPackageEntrypoint {
                package_name: "running.plugin".to_string(),
                entrypoint_id: "sleeper".to_string(),
                environment_overrides: BTreeMap::new(),
            },
        )
        .expect("start supervised sleeper");
        assert!(
            entrypoint_is_running(&mut daemon, "running.plugin"),
            "sleeper must be running before failed disable"
        );

        FileHubStateStore::inject_next_save_failure(&config.data_directory);
        let response = drive_package_request(
            &mut daemon,
            DaemonRequest::DisablePackage {
                package_name: "running.plugin".to_string(),
            },
        )
        .expect("host failure response");
        assert_eq!(
            response.error.expect("host error").code,
            "hub_state_commit_failed"
        );
        assert_eq!(
            package_state(&daemon, "running.plugin"),
            PackageState::Enabled
        );
        assert!(
            entrypoint_is_running(&mut daemon, "running.plugin"),
            "failed disable must not stop the running entrypoint"
        );
        assert!(
            plugin_is_loaded(&daemon, "running.plugin"),
            "failed disable commit must keep the plugin loaded"
        );
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn enable_load_failure_rolls_back_registry_and_durable_state() {
        let root = unique_package_control_dir("enable-load-rollback");
        let data_directory = root.join("data");
        let package_dir = root.join("broken.plugin");
        write_package_control_manifest(
            &package_dir,
            "broken.plugin",
            serde_json::json!({
                "capabilities": [{ "surface": "surfaces" }],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            }),
        );
        std::fs::write(package_dir.join("plugin.lua"), "-- placeholder").expect("write lua");
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start enable-load daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install broken package");
        std::fs::remove_file(package_dir.join("plugin.lua")).expect("remove lua before enable");

        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "broken.plugin".to_string(),
            },
        )
        .expect_err("missing lua must fail enable load");
        assert_eq!(
            package_state(&daemon, "broken.plugin"),
            PackageState::Installed
        );
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn failed_reload_keeps_prior_registry_and_runtime_state() {
        let root = unique_package_control_dir("reload-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("reload.plugin");
        write_package_control_manifest(&package_dir, "reload.plugin", serde_json::json!({}));
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start reload daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install reload package");
        let before = daemon.package_registry().snapshot();
        FileHubStateStore::inject_next_save_failure(&config.data_directory);
        let response = drive_package_request(
            &mut daemon,
            DaemonRequest::ReloadPackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect("host failure response");
        assert_eq!(
            response.error.expect("host error").code,
            "hub_state_commit_failed"
        );
        assert_eq!(daemon.package_registry().snapshot(), before);
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn failed_reload_after_commit_restores_prior_process() {
        let root = unique_package_control_dir("reload-restart-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("reload.plugin");
        write_package_control_manifest(&package_dir, "reload.plugin", sleeper_manifest(&["30"]));
        write_sleeper_script(&package_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start reload runtime daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install");
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect("enable");
        drive_package_request(
            &mut daemon,
            DaemonRequest::StartPackageEntrypoint {
                package_name: "reload.plugin".to_string(),
                entrypoint_id: "sleeper".to_string(),
                environment_overrides: BTreeMap::new(),
            },
        )
        .expect("start");
        assert!(entrypoint_is_running(&mut daemon, "reload.plugin"));

        write_broken_entrypoint(&package_dir);
        write_package_control_manifest(&package_dir, "reload.plugin", broken_sleeper_manifest());
        drive_package_request(
            &mut daemon,
            DaemonRequest::ReloadPackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect_err("broken candidate restart must fail");
        assert_eq!(entrypoint_command(&daemon, "reload.plugin"), "bin/sleeper");
        assert!(
            entrypoint_is_running(&mut daemon, "reload.plugin"),
            "compensation must restart the prior sleeper definition"
        );
        let (live, durable) = live_and_durable_registries(&daemon, &config);
        assert_eq!(live, durable);
        daemon.stop();
    }

    #[test]
    fn failed_refresh_restores_earlier_package_runtime() {
        let root = unique_package_control_dir("refresh-later-failure");
        let data_directory = root.join("data");
        let alpha_dir = root.join("alpha.plugin");
        let zeta_dir = root.join("zeta.plugin");
        write_package_control_manifest(&alpha_dir, "alpha.plugin", sleeper_manifest(&["30"]));
        write_sleeper_script(&alpha_dir);
        write_package_control_manifest(&zeta_dir, "zeta.plugin", sleeper_manifest(&["30"]));
        write_sleeper_script(&zeta_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start refresh daemon");
        for dir in [&alpha_dir, &zeta_dir] {
            drive_package_request(
                &mut daemon,
                DaemonRequest::InstallPackageLocalPath { path: dir.clone() },
            )
            .expect("install");
        }
        for name in ["alpha.plugin", "zeta.plugin"] {
            drive_package_request(
                &mut daemon,
                DaemonRequest::EnablePackage {
                    package_name: name.to_string(),
                },
            )
            .expect("enable");
            drive_package_request(
                &mut daemon,
                DaemonRequest::StartPackageEntrypoint {
                    package_name: name.to_string(),
                    entrypoint_id: "sleeper".to_string(),
                    environment_overrides: BTreeMap::new(),
                },
            )
            .expect("start");
        }
        write_package_control_manifest(&alpha_dir, "alpha.plugin", sleeper_manifest(&["31"]));
        write_broken_entrypoint(&zeta_dir);
        write_package_control_manifest(&zeta_dir, "zeta.plugin", broken_sleeper_manifest());

        drive_package_request(&mut daemon, DaemonRequest::RefreshLocalPackages)
            .expect_err("later zeta restart must fail refresh");
        assert_eq!(entrypoint_command(&daemon, "alpha.plugin"), "bin/sleeper");
        assert_eq!(
            daemon
                .package_registry()
                .package("alpha.plugin")
                .expect("alpha")
                .runnable_entrypoints[0]
                .args,
            ["30"]
        );
        assert!(entrypoint_is_running(&mut daemon, "alpha.plugin"));
        assert_eq!(entrypoint_command(&daemon, "zeta.plugin"), "bin/sleeper");
        assert!(entrypoint_is_running(&mut daemon, "zeta.plugin"));
        daemon.stop();
    }

    #[test]
    fn enable_load_rollback_persist_failure_preserves_original_error() {
        let root = unique_package_control_dir("rollback-persist-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("broken.plugin");
        write_package_control_manifest(
            &package_dir,
            "broken.plugin",
            serde_json::json!({
                "capabilities": [{ "surface": "surfaces" }],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            }),
        );
        std::fs::write(package_dir.join("plugin.lua"), "-- placeholder").expect("write lua");
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start rollback persist daemon");
        let mut state = DaemonControlState::default();
        drive_package_request_with_state(
            &mut daemon,
            &mut state,
            DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install");
        std::fs::remove_file(package_dir.join("plugin.lua")).expect("remove lua");
        FileHubStateStore::inject_save_failure_after(&config.data_directory, 1);
        let error = drive_package_request_with_state(
            &mut daemon,
            &mut state,
            DaemonRequest::EnablePackage {
                package_name: "broken.plugin".to_string(),
            },
        )
        .expect_err("load failure plus rollback persist failure");
        let DaemonTransportError::PackageCompensation {
            original,
            rollbacks,
        } = error
        else {
            panic!("expected typed compensation error, got {error:?}");
        };
        assert!(matches!(&*original, DaemonTransportError::Package(_)));
        assert!(rollbacks.iter().any(|rollback| rollback.step == "persist"
            && matches!(
                &*rollback.error,
                DaemonTransportError::State(crate::HubStateStoreError::InjectedWriteFailure)
            )));
        assert!(
            state.host_recovery.values().any(|recovery| matches!(
                recovery,
                crate::daemon::control::host_work::HostRecoveryRequired::Package(_)
            )),
            "the failed restore must retain one bounded recovery row"
        );
        let entrypoint_cases = [
            (
                "start",
                DaemonRequest::StartPackageEntrypoint {
                    package_name: "broken.plugin".to_string(),
                    entrypoint_id: "missing".to_string(),
                    environment_overrides: BTreeMap::new(),
                },
                true,
            ),
            (
                "restart",
                DaemonRequest::RestartPackageEntrypoint {
                    package_name: "broken.plugin".to_string(),
                    entrypoint_id: "missing".to_string(),
                },
                true,
            ),
            (
                "stop",
                DaemonRequest::StopPackageEntrypoint {
                    package_name: "broken.plugin".to_string(),
                    entrypoint_id: "missing".to_string(),
                },
                false,
            ),
            (
                "status",
                DaemonRequest::PackageEntrypointStatus {
                    package_name: "broken.plugin".to_string(),
                    entrypoint_id: "missing".to_string(),
                },
                false,
            ),
        ];
        for (operation, request, blocked) in entrypoint_cases {
            let response = drive_package_request_with_state(&mut daemon, &mut state, request)
                .unwrap_or_else(|error| panic!("{operation} returned {error:?}"));
            assert_eq!(
                response.error.as_ref().map(|error| error.code.as_str())
                    == Some("package_recovery_required"),
                blocked,
                "unexpected recovery gate result for {operation}"
            );
        }
        let rejected =
            drive_package_request_with_state(&mut daemon, &mut state, DaemonRequest::ListPackages)
                .expect("recovery-required package work returns a typed operator response");
        assert_eq!(rejected.kind, DaemonResponseKind::OperatorError);
        assert_eq!(
            rejected.error.as_ref().map(|error| error.code.as_str()),
            Some("package_recovery_required")
        );
        let spawn_targets = drive_package_request_with_state(
            &mut daemon,
            &mut state,
            DaemonRequest::ListSpawnTargets,
        )
        .expect("unrelated control work must progress during package recovery");
        assert_eq!(spawn_targets.kind, DaemonResponseKind::SpawnTargets);
        daemon.stop();
    }

    #[test]
    fn reload_restore_failure_preserves_original_and_runtime_error() {
        let root = unique_package_control_dir("restore-runtime-failure");
        let data_directory = root.join("data");
        let package_dir = root.join("reload.plugin");
        write_package_control_manifest(&package_dir, "reload.plugin", sleeper_manifest(&["30"]));
        write_sleeper_script(&package_dir);
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start restore-failure daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install");
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect("enable");
        drive_package_request(
            &mut daemon,
            DaemonRequest::StartPackageEntrypoint {
                package_name: "reload.plugin".to_string(),
                entrypoint_id: "sleeper".to_string(),
                environment_overrides: BTreeMap::new(),
            },
        )
        .expect("start");
        write_broken_entrypoint(&package_dir);
        write_package_control_manifest(&package_dir, "reload.plugin", broken_sleeper_manifest());
        std::fs::remove_file(package_dir.join("bin/sleeper")).expect("delete prior binary");
        let error = drive_package_request(
            &mut daemon,
            DaemonRequest::ReloadPackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect_err("restart and restore should both fail");
        let DaemonTransportError::PackageCompensation {
            original,
            rollbacks,
        } = error
        else {
            panic!("expected typed compensation error, got {error:?}");
        };
        assert!(matches!(&*original, DaemonTransportError::Entrypoint(_)));
        assert!(
            rollbacks
                .iter()
                .any(|rollback| rollback.step == "entrypoint"
                    && rollback.package_name.as_deref() == Some("reload.plugin"))
        );
        daemon.stop();
    }

    #[test]
    fn session_type_generation_advances_only_after_successful_commit() {
        let root = unique_package_control_dir("session-type-generation");
        let data_directory = root.join("data");
        let package_dir = root.join("types.plugin");
        write_package_control_manifest(
            &package_dir,
            "types.plugin",
            serde_json::json!({
                "session_types": [{
                    "id": "init",
                    "label": "Mutate agent",
                    "role": "botster.agent",
                    "interaction": "interactive",
                    "traits": ["test"],
                    "lifecycle": "task",
                    "command": "bin/init.sh"
                }]
            }),
        );
        let config = package_control_config(data_directory);
        let mut daemon = HubDaemon::start(config.clone()).expect("start generation daemon");
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath { path: package_dir },
        )
        .expect("install types package");
        let generation_after_install = daemon
            .runtime()
            .expect("runtime")
            .state()
            .session_type_generation;

        FileHubStateStore::inject_next_save_failure(&config.data_directory);
        let failed = drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "types.plugin".to_string(),
            },
        )
        .expect("typed enable failure");
        assert_eq!(
            failed.error.as_ref().map(|error| error.code.as_str()),
            Some("hub_state_commit_failed")
        );
        assert_eq!(
            daemon
                .runtime()
                .expect("runtime")
                .state()
                .session_type_generation,
            generation_after_install
        );

        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "types.plugin".to_string(),
            },
        )
        .expect("enable types package");
        assert!(
            daemon
                .runtime()
                .expect("runtime")
                .state()
                .session_type_generation
                > generation_after_install,
            "successful enable must advance session-type generation after commit"
        );
        daemon.stop();
    }

    /// Owner-loop wiring for the reconcile read: attach a route through the
    /// real control path on an in-process daemon, install a read result that
    /// was submitted (epoch captured) before that attach, apply it through
    /// run_inventory_reconcile_phase, and require the newer route to survive.
    /// The control captures the epoch after the attach and requires the
    /// absent route to be closed.
    fn reconcile_wiring_fixture(
        name: &str,
    ) -> (
        HubDaemon,
        DaemonControlState,
        crate::transport::unix::UnixConnectionMux,
        String,
    ) {
        let data_directory = std::env::temp_dir().join(format!(
            "botster-hub-reconcile-wiring-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let config = package_control_config(data_directory);
        let daemon = HubDaemon::start(config).expect("start reconcile wiring daemon");
        let session_id = format!("reconcile-{name}-session");
        daemon
            .runtime()
            .expect("runtime")
            .spawn_session_for_test(
                botster_core::SessionSpawnRequest {
                    request_id: botster_core::RequestId(format!("reconcile-{name}-spawn")),
                    session_id: botster_core::SessionId(session_id.clone()),
                    executable: "/bin/sleep".to_string(),
                    arguments: vec!["8".to_string()],
                    working_directory: botster_core::SpawnWorkingDirectory {
                        path: ".".to_string(),
                    },
                    environment: botster_core::SpawnEnvironment::default(),
                    initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                },
                botster_core::CoreSessionMetadata::new(),
            )
            .expect("spawn session");
        let mut state = DaemonControlState::default();
        let mux = crate::transport::unix::UnixConnectionMux::new();
        let capabilities =
            crate::subscription::attach_routes::negotiated_unix_capability_set(&[], None)
                .expect("capabilities");
        state.pending_runtime.admission.unix_admissions.insert(
            "reconcile-client".to_string(),
            crate::admission::unix_hello::UnixTerminalAdmission::Admitted {
                required_features: Vec::new(),
                capabilities,
                mux: mux.clone(),
            },
        );
        (daemon, state, mux, session_id)
    }

    /// Attach through the real control path and drive its continuation to
    /// completion. Returns the Core generation the response carried.
    fn attach_through_control(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        session_id: &str,
        subscription_id: &str,
    ) -> u64 {
        let (control_tx, _control_rx) = tokio_mpsc::channel(8);
        let observability = DaemonObservability {
            egress: Vec::new(),
            lifecycle: DaemonLifecycleCounters::default(),
            client_id: Some("reconcile-client".to_string()),
            grant_id: None,
            transport_request_id: None,
        };
        state.current_waiter_id = state.waiter_ids.next();
        let step = handle_control_request(
            daemon,
            state,
            observability,
            control_tx,
            DaemonRequest::Attach {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            },
        );
        let crate::daemon::control::pending::ControlStep::Pending(mut pending) = step else {
            panic!("attach waits on a Core turn");
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        let response = loop {
            if let Some(runtime) = daemon.runtime() {
                // Collect registered Core identities before polling their returned values.
                // Keep reconcile application under the calling fixture's control.
                runtime.take_owner_core_completions(
                    crate::data_plane::driver::CORE_OWNER_COMPLETION_CAPACITY,
                );
                runtime.reap_detached_core_operations();
            }
            match pending.continuation.poll(daemon, state) {
                crate::daemon::control::pending::ControlPoll::FinishedInternal => {
                    panic!("attach must return a response")
                }
                crate::daemon::control::pending::ControlPoll::DeliverStatusResponse(_, _, _)
                | crate::daemon::control::pending::ControlPoll::StatusResponseDelivered {
                    ..
                }
                | crate::daemon::control::pending::ControlPoll::StatusResponseRefused { .. } => {
                    panic!("attach helper must not receive a status response")
                }
                crate::daemon::control::pending::ControlPoll::Again => continue,
                crate::daemon::control::pending::ControlPoll::Ready(response) => {
                    break response.expect("attach response");
                }
                crate::daemon::control::pending::ControlPoll::PreparePluginResponse(_, _)
                | crate::daemon::control::pending::ControlPoll::SubmitPluginHost(_) => {
                    panic!("attach must not carry a plugin-result charge")
                }
                crate::daemon::control::pending::ControlPoll::ReadyHost(_, _) => {
                    panic!("attach must not carry a host-result charge")
                }
                crate::daemon::control::pending::ControlPoll::Pending => {
                    assert!(Instant::now() < deadline, "attach continuation timed out");
                    thread::sleep(Duration::from_millis(5));
                }
            }
        };
        assert_eq!(
            response.kind,
            DaemonResponseKind::TerminalAttached,
            "attach must bind: {response:?}"
        );
        let generation = response
            .terminal_attach
            .expect("terminal attach body")
            .generation;
        // This fixture drives the continuation directly, so it must apply
        // the response bookkeeping that `finish` applies in production.
        record_attached_subscription_change(
            &mut state.pending_runtime,
            &mut state.attach_close,
            &mut state.lifecycle_counters,
            Some(AttachedSubscriptionChange::Attach(AttachedSubscription {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            })),
            None,
        );
        generation
    }

    #[test]
    fn terminal_inventory_wake_survives_maintenance_first_journal_consumption() {
        let mut state = DaemonControlState::default();
        state.maintenance.note_journal_advanced();
        state.note_terminal_inventory_changed();

        assert!(
            state.maintenance.take_journal_wake(),
            "the Maintenance Observe path consumes the independent journal bit"
        );
        let classes = std::iter::from_fn(|| state.owner_ready.pop_next())
            .map(|item| item.key().class())
            .collect::<Vec<_>>();
        assert!(classes.contains(&crate::daemon::owner_schedule::ReadyClass::Observe));
        assert!(classes.contains(&crate::daemon::owner_schedule::ReadyClass::InventoryReconcile));
    }

    #[test]
    fn terminal_inventory_change_during_reconcile_schedules_one_fresh_pass_without_spin() {
        let (mut daemon, mut state, _mux, _session_id) =
            reconcile_wiring_fixture("inventory-wake-during-reconcile");

        assert!(run_inventory_reconcile_phase(&daemon, &mut state));
        assert!(state.reconcile_inventory.is_some());

        state.note_terminal_inventory_changed();
        state.resolve_submitted_reconcile_inventory_for_test(Vec::new());
        assert!(
            run_inventory_reconcile_phase(&daemon, &mut state),
            "an inventory change during the read must schedule a fresh pass"
        );

        assert!(run_inventory_reconcile_phase(&daemon, &mut state));
        assert!(state.reconcile_inventory.is_some());
        state.resolve_submitted_reconcile_inventory_for_test(Vec::new());
        assert!(!run_inventory_reconcile_phase(&daemon, &mut state));
        assert!(state.reconcile_inventory.is_none());
        assert!(!state.pump.take_inventory_reconcile_again());
        let _ = daemon.stop();
    }

    #[test]
    fn reconcile_read_submitted_before_an_attach_leaves_that_attach_bound() {
        let (mut daemon, mut state, mux, session_id) = reconcile_wiring_fixture("before");
        assert!(
            run_inventory_reconcile_phase(&daemon, &mut state),
            "the first phase turn submits the inventory read"
        );
        let generation = attach_through_control(&mut daemon, &mut state, &session_id, "newer");
        assert!(state.pending_runtime.is_adapter_bound(&session_id, "newer"));
        // Control only the result application. The read keeps the epoch that
        // the production submission branch captured before this attach.
        state.resolve_submitted_reconcile_inventory_for_test(Vec::new());
        let more = run_inventory_reconcile_phase(&daemon, &mut state);
        assert!(!more);
        assert!(
            state.pending_runtime.is_adapter_bound(&session_id, "newer"),
            "a route attached after the read was submitted stays bound"
        );
        let handle = mux
            .route_handle(&session_id, "newer", generation)
            .expect("registered route");
        assert!(!handle.host_closed(), "the newer route is not host-closed");
        let _ = daemon.stop();
    }

    #[test]
    fn reconcile_read_submitted_after_an_attach_closes_it_when_absent() {
        let (mut daemon, mut state, mux, session_id) = reconcile_wiring_fixture("after");
        let generation = attach_through_control(&mut daemon, &mut state, &session_id, "older");
        let handle = mux
            .route_handle(&session_id, "older", generation)
            .expect("registered route");
        assert_eq!(state.lifecycle_counters.live_attach_subscriptions, 1);
        assert!(
            state
                .pending_runtime
                .live_attach_routes
                .contains(&(session_id.clone(), "older".to_string()))
        );
        // The read is submitted after the attach; an empty result means Core
        // ended the route, and reconcile must close it.
        assert!(
            run_inventory_reconcile_phase(&daemon, &mut state),
            "the first phase turn submits the inventory read"
        );
        state.resolve_submitted_reconcile_inventory_for_test(Vec::new());
        let _ = run_inventory_reconcile_phase(&daemon, &mut state);
        assert!(!state.pending_runtime.is_adapter_bound(&session_id, "older"));
        assert!(
            state
                .pending_runtime
                .stream_identity(&session_id, "older")
                .is_none(),
            "the absent older route is released from the registry"
        );
        assert!(
            handle.host_closed(),
            "the absent older route is host-closed"
        );
        assert_eq!(state.lifecycle_counters.live_attach_subscriptions, 0);
        assert_eq!(state.attach_close.released_attach_generations, 1);
        assert!(state.pending_runtime.live_attach_routes.is_empty());
        let occupancy = crate::subscription::attach_routes::live_attach_occupancy_rows(
            &state.pending_runtime.live_attach_routes,
            &[],
            &state.pending_runtime,
        );
        assert!(
            occupancy.is_empty(),
            "the reconciled route must not remain in Status occupancy: {occupancy:?}"
        );

        // A later inventory read cannot retire or decrement the same route
        // again because the first pass removed its exact stream.
        assert!(run_inventory_reconcile_phase(&daemon, &mut state));
        state.resolve_submitted_reconcile_inventory_for_test(Vec::new());
        assert!(!run_inventory_reconcile_phase(&daemon, &mut state));
        assert_eq!(state.lifecycle_counters.live_attach_subscriptions, 0);
        assert_eq!(state.attach_close.released_attach_generations, 1);
        let _ = daemon.stop();
    }
}
