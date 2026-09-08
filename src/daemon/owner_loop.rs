//! Hub owner thread.

use std::collections::BTreeMap;
use std::fmt;
use std::os::unix::net::UnixListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self};
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
use crate::daemon::control::handle_control_message;
use crate::daemon::control::message::{
    ControlMessage, ControlReplySender, ControlSender, DaemonDeliveryKind, EgressWriteClass,
};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon_maintenance::{
    BackgroundClass, BackgroundClassScheduler, BackgroundTurnDecision, MaintenanceSliceKind,
    MaintenanceState, OBSERVE_SLICE_BUDGET, PUMP_MAX_ROUTES_VALIDATED, PumpPhase, PumpScheduler,
    decide_background_slice, run_completion_drain_slice_for_owner, run_maintenance_kind,
};
use crate::subscription::attach_routes::{
    AttachStreamRegistry, AttachedSubscription, AttachedSubscriptionChange,
    record_attached_subscription_change,
};
use crate::subscription::entity::{
    EntitySubscriptionState, drive_entity_subscriptions, drive_package_entity_fanout,
    drive_package_entity_resync, seed_lifecycle_reconciliation, session_subscribers_need_delivery,
};
use crate::transport::unix::connection::{
    handle_connection_async, handle_connection_cleanup, reap_finished_connection_tasks,
    wait_for_connection_tasks,
};
use crate::transport::unix::listener::{
    accept_connections, acquire_socket_owner_lock, cleanup_socket_path, prepare_socket_path,
    rebind_missing_socket_path, socket_path,
};

const ENTITY_RECONCILIATION_INTERVAL: Duration = Duration::from_millis(500);

/// One Core inventory read for the reconcile phase. `read_epoch` is the
/// registry attach epoch at submission: the read's rows cover every attach
/// with epoch <= read_epoch and none newer (one owner thread submits both in
/// program order; the Core bridge is one FIFO consumed by one thread).
pub(crate) struct InventoryRead {
    read_epoch: u64,
    ticket: crate::data_plane::driver::CoreTicket<Vec<botster_core::TerminalSubscriptionRecord>>,
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
        read.ticket = crate::data_plane::driver::CoreTicket::resolved(inventory);
    }

    pub(crate) fn note_terminal_inventory_changed(&mut self) {
        let reconcile_active =
            self.reconcile_inventory.is_some() || self.pump.reconcile_after.is_some();
        self.pump
            .note_inventory_change_during_reconcile(reconcile_active);
        self.background.mark_pump();
    }
}

/// Earliest deadline among retained obligations and pending requests, so the
/// owner wakes to retire abandoned work even when no control traffic arrives.
fn next_owner_deadline(state: &DaemonControlState) -> Option<Instant> {
    let obligation = state.budget.next_obligation_deadline();
    let request = crate::daemon::control::pending::next_request_deadline(state);
    [
        obligation,
        request,
        state.plugin_entities.next_reply_deadline(),
    ]
    .into_iter()
    .flatten()
    .min()
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
    reconciliation_wait: Duration,
) -> OwnerEvent {
    if reconciliation_wait.is_zero() {
        return OwnerEvent::Reconcile;
    }
    tokio::select! {
        biased;
        message = control_rx.recv() => OwnerEvent::Control(Box::new(message)),
        _ = tokio::time::sleep(reconciliation_wait) => OwnerEvent::Reconcile,
    }
}

fn retry_client_event_cleanups(daemon: &HubDaemon, state: &mut DaemonControlState) {
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    if state
        .event_plane
        .apply_pending_cleanups(runtime.package_event_router())
    {
        state.maintenance.try_wake();
    }
}

fn owner_maintenance_pending(daemon: &HubDaemon, state: &DaemonControlState) -> bool {
    state.maintenance.needs_work()
        || session_subscribers_need_delivery(state)
        || daemon
            .runtime()
            .is_some_and(crate::HubRuntime::package_entity_resync_still_needed)
        || daemon.runtime().is_some_and(|runtime| {
            runtime.package_event_router().peek_delivery_wake()
                || runtime.event_plane_owner_ops_pending()
                || runtime.package_entity_work_pending()
        })
        || state.event_plane.has_pending_cleanup()
}

fn mark_due_reconciliation(state: &mut DaemonControlState, now: Instant) {
    if state.next_reconciliation <= now {
        state.background.mark_pump();
        state.maintenance.try_wake();
        state.next_reconciliation = now + ENTITY_RECONCILIATION_INTERVAL;
    }
}

fn run_one_owner_background_slice(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let maintenance_pending = owner_maintenance_pending(daemon, state);
    let BackgroundTurnDecision::OneSlice(class) =
        decide_background_slice(&mut state.background, maintenance_pending)
    else {
        return;
    };
    match class {
        BackgroundClass::Maintenance => {
            state.lifecycle_counters.reconciliation_wakes = state
                .lifecycle_counters
                .reconciliation_wakes
                .saturating_add(1);
            run_one_owner_maintenance_slice(daemon, state);
        }
        BackgroundClass::Pump => run_one_pump_phase(daemon, state),
    }
}

fn run_one_owner_maintenance_slice(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    retry_client_event_cleanups(daemon, state);
    let started = Instant::now();
    let kind = state.maintenance.scheduler.take_slice();
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
                run_maintenance_kind(
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
                    state.maintenance.scheduler.prefer_completion_drain();
                }
            }
        }
        other => {
            if let Some(runtime) = daemon.runtime() {
                let _ = runtime.apply_event_plane_owner_ops();
                if runtime.package_event_router().peek_delivery_wake()
                    || runtime.event_plane_owner_ops_pending()
                {
                    state.maintenance.try_wake();
                }
                run_maintenance_kind(
                    runtime,
                    &mut state.maintenance,
                    &mut state.maintenance_reads,
                    other,
                );
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
    if owner_maintenance_pending(daemon, state) {
        state.maintenance.try_wake();
    }
}

fn completion_drain_needs_followup(
    progress: crate::daemon_maintenance::CompletionDrainProgress,
) -> bool {
    progress.item_count > 0 && progress.has_remaining
}

pub fn serve_daemon(config: HubConfig) -> DaemonTransportResult<HubDaemonStatus> {
    let socket_path = socket_path(&config)?;
    let socket_owner = acquire_socket_owner_lock(&socket_path)?;
    prepare_socket_path(&socket_path, &socket_owner)?;
    let listener = UnixListener::bind(&socket_path).map_err(DaemonTransportError::Io)?;
    listener
        .set_nonblocking(true)
        .map_err(DaemonTransportError::Io)?;

    let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
    let (cleanup_tx, cleanup_rx) = mpsc::sync_channel(DAEMON_MAX_CONNECTIONS);
    let (shutdown_tx, _) = watch::channel(false);
    install_signal_forwarder(control_tx.clone())?;
    let mut daemon = HubDaemon::start(config)?;
    if let Some(runtime) = daemon.runtime() {
        runtime.bind_data_plane_owner_wake(control_tx.clone());
        runtime.bind_host_owner_wake(control_tx.clone());
    }
    let mut control_state = DaemonControlState {
        event_plane: daemon.local_webrtc().event_plane(),
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
    let (rebind_tx, rebind_rx) = tokio_mpsc::channel(1);
    let mut connection_tasks = vec![transport_runtime.spawn(accept_connections(
        listener,
        control_tx.clone(),
        shutdown_tx.subscribe(),
        Arc::new(Semaphore::new(DAEMON_MAX_CONNECTIONS)),
        rebind_rx,
    ))];
    loop {
        let mut owner_turn = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
        reap_finished_connection_tasks(&mut connection_tasks);
        while let Ok(cleanup) = cleanup_rx.try_recv() {
            handle_connection_cleanup(&mut daemon, &mut control_state, control_tx.clone(), cleanup);
        }
        // Read the shared completion bit on every turn. If the bounded
        // doorbell queue was full, another owner event still exposes results.
        if let Some(runtime) = daemon.runtime()
            && runtime.take_core_completion_notification()
        {
            let identities = runtime
                .take_owner_core_completions(crate::daemon::owner_turn::OWNER_TURN_ITEM_LIMIT);
            let consumed = crate::daemon::control::pending::absorb_core_completions(
                &mut control_state,
                &identities,
                &mut owner_turn,
            );
            runtime.restore_owner_core_completions(&identities[consumed..]);
            runtime.reap_detached_core_operations();
        }
        crate::daemon::control::record_data_plane_progress(&daemon, &mut control_state);
        crate::subscription::entity::absorb_session_type_catalog_completions(
            &daemon,
            &mut control_state,
        );
        let completion_published = control_state
            .plugin_result_budget
            .take_completion_notification();
        let result_capacity_released = control_state
            .plugin_result_budget
            .take_release_notification();
        if completion_published || result_capacity_released {
            control_state
                .maintenance
                .scheduler
                .prefer_completion_drain();
        }
        crate::daemon::control::entities::retire_plugin_entity_replies(
            &daemon,
            &mut control_state,
            Instant::now(),
        );
        crate::daemon::control::entities::expire_plugin_entity_resyncs(
            &daemon,
            &mut control_state,
            Instant::now(),
        );
        mark_due_reconciliation(&mut control_state, Instant::now());
        crate::daemon::control::pending::mark_due_request_deadlines(
            &mut control_state,
            Instant::now(),
            &mut owner_turn,
        );
        let slice_due = control_state
            .background
            .has_pending(owner_maintenance_pending(&daemon, &control_state))
            || !control_state.request_ready.is_empty();
        let event = match classify_owner_poll(control_rx.try_recv(), slice_due) {
            OwnerPollDecision::ServeControl(message) => Some(OwnerEvent::Control(message)),
            OwnerPollDecision::RunSlice => None,
            OwnerPollDecision::Block => {
                let wake_at = match next_owner_deadline(&control_state) {
                    Some(deadline) => control_state.next_reconciliation.min(deadline),
                    None => control_state.next_reconciliation,
                };
                let wait = wake_at.saturating_duration_since(Instant::now());
                match transport_runtime.block_on(receive_owner_event(&mut control_rx, wait)) {
                    OwnerEvent::Control(message) => Some(OwnerEvent::Control(message)),
                    OwnerEvent::Reconcile => None,
                }
            }
        };
        if let Some(OwnerEvent::Control(message)) = event {
            match *message {
                Some(ControlMessage::AcceptedConnection {
                    stream,
                    admission_permit,
                }) => {
                    // The owner budget permit outlives the transport permit:
                    // it travels in the connection's cleanup guard and comes
                    // back with the cleanup message.
                    let Some(connection_permit) = control_state.budget.reserve_connection() else {
                        control_state.lifecycle_counters.rejected_connections = control_state
                            .lifecycle_counters
                            .rejected_connections
                            .saturating_add(1);
                        *control_state
                            .lifecycle_counters
                            .cleanup_by_reason
                            .entry("owner_budget_refused_connection".to_string())
                            .or_insert(0) += 1;
                        drop(stream);
                        drop(admission_permit);
                        continue;
                    };
                    control_state.lifecycle_counters.accepted_connections = control_state
                        .lifecycle_counters
                        .accepted_connections
                        .saturating_add(1);
                    let tx = control_tx.clone();
                    let cleanup = cleanup_tx.clone();
                    let shutdown = shutdown_tx.subscribe();
                    let event_plane = control_state.event_plane.clone();
                    control_state.lifecycle_counters.live_connections = control_state
                        .lifecycle_counters
                        .live_connections
                        .saturating_add(1);
                    control_state.lifecycle_counters.high_water_live_connections = control_state
                        .lifecycle_counters
                        .high_water_live_connections
                        .max(control_state.lifecycle_counters.live_connections);
                    connection_tasks.push(transport_runtime.spawn(async move {
                        let _admission_permit = admission_permit;
                        if let Err(error) = handle_connection_async(
                            stream,
                            tx,
                            cleanup,
                            shutdown,
                            event_plane,
                            connection_permit,
                        )
                        .await
                        {
                            eprintln!("botster-hub daemon connection error: {error}");
                        }
                    }));
                }
                Some(ControlMessage::RejectedConnection) => {
                    control_state.lifecycle_counters.rejected_connections = control_state
                        .lifecycle_counters
                        .rejected_connections
                        .saturating_add(1);
                }
                Some(message) => {
                    if handle_control_message(
                        &mut daemon,
                        &mut control_state,
                        transport_runtime.handle(),
                        control_tx.clone(),
                        message,
                    ) {
                        let _ = shutdown_tx.send(true);
                        wait_for_connection_tasks(
                            &transport_runtime,
                            &mut connection_tasks,
                            &cleanup_rx,
                            &mut daemon,
                            &mut control_state,
                            control_tx.clone(),
                        );
                        let status = daemon.stop();
                        cleanup_socket_path(&socket_path, socket_owner);
                        return Ok(status);
                    }
                }
                None => return Err(DaemonTransportError::ControlThreadStopped),
            }
        }
        mark_due_reconciliation(&mut control_state, Instant::now());
        if control_state
            .background
            .has_pending(owner_maintenance_pending(&daemon, &control_state))
        {
            run_one_owner_background_slice(&mut daemon, &mut control_state);
        }
        for waiter_id in control_state
            .plugin_controls
            .take_ready_waiters(crate::daemon::owner_turn::OWNER_TURN_ITEM_LIMIT)
        {
            crate::daemon::control::pending::mark_request_ready(
                &mut control_state,
                waiter_id,
                crate::daemon::owner_schedule::ReadyClass::PluginCompletion,
                crate::daemon::control::pending::READY_PLUGIN_COMPLETION,
            );
        }
        crate::daemon::control::entities::drive_plugin_entity_completions(
            &mut daemon,
            &mut control_state,
        );
        crate::daemon::owner_budget::poll_owner_obligations(
            &mut daemon,
            &mut control_state,
            Instant::now(),
        );
        if crate::daemon::control::request::poll_deferred_with_budget(
            &mut daemon,
            &mut control_state,
            &mut owner_turn,
        ) {
            let _ = shutdown_tx.send(true);
            wait_for_connection_tasks(
                &transport_runtime,
                &mut connection_tasks,
                &cleanup_rx,
                &mut daemon,
                &mut control_state,
                control_tx.clone(),
            );
            let status = daemon.stop();
            cleanup_socket_path(&socket_path, socket_owner);
            return Ok(status);
        }
        if !socket_path.exists() {
            rebind_missing_socket_path(&rebind_tx, &socket_path);
        }
    }
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
    let should_stop = matches!(
        response.response(),
        Ok(DaemonResponse {
            kind: DaemonResponseKind::Shutdown,
            ..
        })
    );
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

fn run_one_pump_phase(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let phase = state.pump.take_phase();
    let incomplete = match phase {
        PumpPhase::InventoryReconcile => {
            let expired = state
                .pending_runtime
                .admission
                .reservations
                .retire_expired(crate::admission::reservations::now_seconds());
            for reservation in expired {
                let grant_id = state
                    .pending_runtime
                    .admission
                    .webrtc_admissions
                    .iter()
                    .find_map(|(grant_id, admission)| match admission {
                        WebrtcTerminalAdmission::Admitted {
                            peer_generation, ..
                        }
                        | WebrtcTerminalAdmission::Rejected {
                            peer_generation, ..
                        } if *peer_generation == reservation.peer_generation => {
                            Some(grant_id.clone())
                        }
                        _ => None,
                    });
                if let Some(grant_id) = grant_id.as_deref() {
                    crate::daemon::control::connection::retire_route_owner(
                        daemon,
                        state,
                        grant_id,
                        &reservation,
                    );
                }
                if let Some(budget) = state
                    .pending_runtime
                    .admission
                    .connection_budgets
                    .get_mut(&reservation.peer_generation)
                {
                    let _ = budget.release(&reservation.label);
                }
                if let Some(mux) = state
                    .pending_runtime
                    .admission
                    .webrtc_admissions
                    .values()
                    .find_map(|admission| match admission {
                        WebrtcTerminalAdmission::Admitted {
                            mux,
                            peer_generation,
                            ..
                        }
                        | WebrtcTerminalAdmission::Rejected {
                            mux,
                            peer_generation,
                            ..
                        } if *peer_generation == reservation.peer_generation => Some(mux),
                        _ => None,
                    })
                {
                    let event = match reservation.class {
                        crate::admission::connection_budget::ChannelClass::Terminal => {
                            botster_hub_client::DaemonEvent::TerminalSubscriptionClosed {
                                session_id: reservation.session_id,
                                subscription_id: reservation.subscription_id,
                                generation: reservation.generation,
                                reason: botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_RESERVATION_EXPIRED.to_string(),
                            }
                        }
                        crate::admission::connection_budget::ChannelClass::Entity => {
                            botster_hub_client::DaemonEvent::RuntimeObservation {
                                kind: format!(
                                    "entity_subscription_closed:{}:{}:reservation_expired",
                                    reservation.subscription_id, reservation.generation
                                ),
                            }
                        }
                        crate::admission::connection_budget::ChannelClass::Event => {
                            botster_hub_client::DaemonEvent::RuntimeObservation {
                                kind: format!(
                                    "package_event_subscription_closed:{}:{}:reservation_expired",
                                    reservation.subscription_id, reservation.generation
                                ),
                            }
                        }
                        crate::admission::connection_budget::ChannelClass::Control => continue,
                    };
                    mux.push_host_event(event);
                }
            }
            run_inventory_reconcile_phase(daemon, state)
        }
        PumpPhase::Observe => run_pump_observe_phase(daemon, state),
    };
    if incomplete {
        state.background.mark_pump();
    }
}

/// Validate owner route bookkeeping against one Core inventory read.
///
/// The read is requested on one pump turn and consumed on a later one; the
/// owner never waits for it. Returns `true` while more work is pending.
pub(crate) fn run_inventory_reconcile_phase(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
) -> bool {
    use crate::data_plane::driver::CoreTicketPoll;

    let Some(runtime) = daemon.runtime() else {
        state.pump.reconcile_after = None;
        state.pump.take_inventory_reconcile_again();
        state.reconcile_inventory = None;
        return false;
    };
    let Some(read) = state.reconcile_inventory.as_mut() else {
        // The epoch is captured at submission, on this thread, before the
        // request is enqueued: the read cannot cover any later attach.
        let read_epoch = state.pending_runtime.attach_epoch();
        state.reconcile_inventory = Some(InventoryRead {
            read_epoch,
            ticket: runtime.list_terminal_subscriptions(),
        });
        return true;
    };
    let read_epoch = read.read_epoch;
    let inventory = match read.ticket.poll() {
        CoreTicketPoll::Pending => return true,
        // Refused admission: clear the single slot; the cursor stays and the
        // next pump resubmits (one ticket in flight, no queue).
        CoreTicketPoll::Refused => {
            state.reconcile_inventory = None;
            return true;
        }
        CoreTicketPoll::Lost => {
            state.reconcile_inventory = None;
            state.pump.reconcile_after = None;
            return false;
        }
        CoreTicketPoll::Ready(inventory) => inventory,
    };
    state.reconcile_inventory = None;
    // Core keys ownership by (client, session, subscription); a row for
    // another client on the same route is not this stream's row.
    let lookup = |client_id: &str, session_id: &str, subscription_id: &str| {
        inventory
            .iter()
            .find(|row| {
                row.client_id.0 == client_id
                    && row.session_id.0 == session_id
                    && row.subscription_id.0 == subscription_id
            })
            .map(|row| row.generation)
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
        true
    } else {
        state.pump.reconcile_after = None;
        state.pump.take_inventory_reconcile_again()
    }
}

/// Drive one bounded observe slice through a Core ticket.
///
/// Returns `true` while the pass is incomplete or the read is in flight.
fn run_pump_observe_phase(daemon: &HubDaemon, state: &mut DaemonControlState) -> bool {
    use crate::data_plane::driver::CoreTicketPoll;

    let Some(runtime) = daemon.runtime() else {
        state.observe_resume = None;
        state.observe_read = None;
        return false;
    };
    let Some(ticket) = state.observe_read.as_mut() else {
        let now = tick(&mut state.logical_clock);
        state.observe_read = Some(runtime.observe_lifecycle_slice(
            now,
            state.observe_resume.as_ref(),
            OBSERVE_SLICE_BUDGET,
        ));
        return true;
    };
    let slice = match ticket.poll() {
        CoreTicketPoll::Pending => return true,
        // Refused admission: clear the single slot and resubmit next pump.
        CoreTicketPoll::Refused => {
            state.observe_read = None;
            return true;
        }
        CoreTicketPoll::Lost => {
            state.observe_read = None;
            state.observe_resume = None;
            return false;
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
            state.background.mark_pump();
        }
        state.observe_resume.is_some()
    } else {
        false
    }
}

pub(crate) struct DaemonControlState {
    pub(crate) logical_clock: u64,
    pub(crate) drain_cursors: BTreeMap<String, u64>,
    pub(crate) egress_diagnostics: DaemonEgressDiagnostics,
    pub(crate) entity_subscriptions: BTreeMap<String, EntitySubscriptionState>,
    pub(crate) event_plane: std::sync::Arc<crate::subscription::package_events::ClientEventPlane>,
    pub(crate) pending_runtime: PendingRuntimeState,
    pub(crate) lifecycle_counters: DaemonLifecycleCounters,
    pub(crate) maintenance: MaintenanceState,
    pub(crate) background: BackgroundClassScheduler,
    pub(crate) pump: PumpScheduler,
    next_reconciliation: Instant,
    pub(crate) released_entity_generations: u64,
    pub(crate) attach_close: crate::subscription::closed_events::AttachCloseBookkeeping,
    pub(crate) pending_hub_update_reply: Option<ControlReplySender>,
    /// Requests whose response waits on a Core owner-thread result.
    pub(crate) pending_requests: BTreeMap<
        crate::owner_identity::WaiterId,
        crate::daemon::control::pending::PendingControlRequest,
    >,
    pub(crate) waiter_ids: crate::owner_identity::WaiterIdSource,
    pub(crate) current_waiter_id: Option<crate::owner_identity::WaiterId>,
    pub(crate) request_ready: crate::daemon::owner_schedule::ReadyQueues,
    pub(crate) request_deadlines: crate::daemon::owner_schedule::DeadlineIndex,
    pub(crate) host_completions:
        BTreeMap<crate::owner_identity::WaiterId, crate::host_executor::HostCompletion>,
    pub(crate) document_owner: Option<crate::owner_identity::WaiterId>,
    pub(crate) document_waiters: std::collections::BTreeSet<crate::owner_identity::WaiterId>,
    pub(crate) host_recovery_waiters: std::collections::BTreeSet<crate::owner_identity::WaiterId>,
    pub(crate) blocked_session_type_roots:
        BTreeMap<std::path::PathBuf, crate::owner_identity::WaiterId>,
    /// Correlation for non-blocking plugin request-response work.
    pub(crate) plugin_controls: crate::daemon::control::plugins::PluginControlState,
    /// Correlation and retained replies for asynchronous entity providers.
    pub(crate) plugin_entities: crate::daemon::control::entities::PluginEntityState,
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
        Self {
            logical_clock: 1,
            drain_cursors: BTreeMap::new(),
            egress_diagnostics: DaemonEgressDiagnostics::default(),
            entity_subscriptions: BTreeMap::new(),
            event_plane: std::sync::Arc::new(
                crate::subscription::package_events::ClientEventPlane::default(),
            ),
            pending_runtime: PendingRuntimeState::default(),
            lifecycle_counters: DaemonLifecycleCounters::default(),
            maintenance: MaintenanceState::default(),
            background: BackgroundClassScheduler::default(),
            pump: PumpScheduler::default(),
            next_reconciliation: Instant::now(),
            released_entity_generations: 0,
            attach_close: crate::subscription::closed_events::AttachCloseBookkeeping::default(),
            pending_hub_update_reply: None,
            pending_requests: BTreeMap::new(),
            waiter_ids: crate::owner_identity::WaiterIdSource::default(),
            current_waiter_id: None,
            request_ready: crate::daemon::owner_schedule::ReadyQueues::new(),
            request_deadlines: crate::daemon::owner_schedule::DeadlineIndex::new(),
            host_completions: BTreeMap::new(),
            document_owner: None,
            document_waiters: std::collections::BTreeSet::new(),
            host_recovery_waiters: std::collections::BTreeSet::new(),
            blocked_session_type_roots: BTreeMap::new(),
            plugin_controls: crate::daemon::control::plugins::PluginControlState::default(),
            plugin_entities: crate::daemon::control::entities::PluginEntityState::default(),
            plugin_result_budget:
                crate::daemon::control::reply::RetainedPluginResultBudget::default(),
            budget: crate::daemon::owner_budget::OwnerBudget::default(),
            maintenance_reads: crate::daemon_maintenance::MaintenanceCoreReads::default(),
            close_event_decisions: crate::subscription::closed_events::CloseEventDecisions::default(
            ),
            session_type_catalog: crate::subscription::entity::SessionTypeCatalogCache::default(),
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
    fn due_reconciliation_precedes_an_already_ready_control_message() {
        let (control_tx, mut control_rx) = tokio_mpsc::channel(1);
        control_tx
            .try_send(ControlMessage::RejectedConnection)
            .expect("prefill owner control queue");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build owner event test runtime");

        assert!(matches!(
            runtime.block_on(receive_owner_event(&mut control_rx, Duration::ZERO)),
            OwnerEvent::Reconcile
        ));
        let OwnerEvent::Control(message) =
            runtime.block_on(receive_owner_event(&mut control_rx, Duration::from_secs(1)))
        else {
            panic!("ready control message must win before a future reconciliation deadline");
        };
        assert!(matches!(*message, Some(ControlMessage::RejectedConnection)));
    }

    #[test]
    fn queued_control_precedes_a_due_maintenance_slice() {
        assert!(matches!(
            classify_owner_poll(Ok(ControlMessage::RejectedConnection), true),
            OwnerPollDecision::ServeControl(message)
                if matches!(*message, Some(ControlMessage::RejectedConnection))
        ));
        let mut scheduler = BackgroundClassScheduler::default();
        scheduler.mark_pump();
        assert!(matches!(
            classify_owner_poll(Ok(ControlMessage::RejectedConnection), true),
            OwnerPollDecision::ServeControl(_)
        ));
        assert!(matches!(
            decide_background_slice(&mut scheduler, true),
            BackgroundTurnDecision::OneSlice(_)
        ));
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
    fn pump_phases_do_not_list_subscriptions_or_sessions() {
        const TRANSPORT: &str = include_str!("owner_loop.rs");
        let pump = TRANSPORT
            .split("fn run_one_pump_phase")
            .nth(1)
            .expect("pump runner");
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
            "async fn accept_connections",
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
            listener.contains("pub(crate) async fn accept_connections")
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
        let (control_tx, _control_rx) = tokio_mpsc::channel(8);
        let observability = DaemonObservability {
            egress: Vec::new(),
            lifecycle: DaemonLifecycleCounters::default(),
            client_id: None,
            grant_id: None,
            transport_request_id: None,
        };
        let mut state = DaemonControlState::default();
        match handle_control_request(daemon, &mut state, observability, control_tx, request) {
            crate::daemon::control::pending::ControlStep::Ready(response) => response,
            crate::daemon::control::pending::ControlStep::Pending(_) => {
                panic!("package requests answer without a Core turn")
            }
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

    fn drive_async_plugin_control(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        request: DaemonRequest,
        transport_request_id: &str,
    ) -> DaemonTransportResult<DaemonResponse> {
        let (control_tx, _control_rx) = tokio_mpsc::channel(8);
        let step = handle_control_request(
            daemon,
            state,
            DaemonObservability {
                egress: Vec::new(),
                lifecycle: DaemonLifecycleCounters::default(),
                client_id: Some("response-fixture-connection".to_string()),
                grant_id: None,
                transport_request_id: Some(transport_request_id.to_string()),
            },
            control_tx,
            request,
        );
        let crate::daemon::control::pending::ControlStep::Pending(mut pending) = step else {
            panic!("the plugin response fixture request must be asynchronous");
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(runtime) = daemon.runtime() {
                let DaemonControlState {
                    maintenance,
                    plugin_controls,
                    plugin_entities,
                    plugin_result_budget,
                    ..
                } = state;
                let _ = run_completion_drain_slice_for_owner(
                    runtime,
                    maintenance,
                    plugin_controls,
                    plugin_entities,
                    plugin_result_budget,
                );
            }
            match (pending.continuation)(daemon, state) {
                crate::daemon::control::pending::ControlPoll::Ready(response) => return response,
                crate::daemon::control::pending::ControlPoll::ReadyRetained(response) => {
                    assert!(
                        state.plugin_result_budget.retained_bytes() > 0,
                        "the reply must retain its Core logical-byte charge"
                    );
                    return response.into_parts().0;
                }
                crate::daemon::control::pending::ControlPoll::ReadyHost(_, _) => {
                    panic!("plugin response must not carry a host-result charge")
                }
                crate::daemon::control::pending::ControlPoll::Pending => {
                    assert!(Instant::now() < deadline, "plugin response timed out");
                    thread::sleep(Duration::from_millis(5));
                }
            }
        }
    }

    #[test]
    fn blocked_plugin_request_does_not_block_unrelated_owner_control() {
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

        let (control_tx, _control_rx) = tokio_mpsc::channel(8);
        let mut state = DaemonControlState::default();
        crate::lua_runtime::arm_test_plugin_invocation_gate();
        let held = handle_control_request(
            &mut daemon,
            &mut state,
            DaemonObservability {
                egress: Vec::new(),
                lifecycle: DaemonLifecycleCounters::default(),
                client_id: Some("connection-held".to_string()),
                grant_id: None,
                transport_request_id: Some("41".to_string()),
            },
            control_tx.clone(),
            DaemonRequest::PluginMcpCallTool {
                name: "owner.controlled_gate".to_string(),
                arguments: serde_json::json!({ "token": "held-request-41" }),
            },
        );
        let crate::daemon::control::pending::ControlStep::Pending(mut held) = held else {
            panic!("the controlled plugin request must wait for its worker");
        };
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
        let status = handle_control_request(
            &mut daemon,
            &mut state,
            DaemonObservability {
                egress: Vec::new(),
                lifecycle: DaemonLifecycleCounters::default(),
                client_id: Some("connection-status".to_string()),
                grant_id: None,
                transport_request_id: Some("7".to_string()),
            },
            control_tx,
            DaemonRequest::Status,
        );
        let crate::daemon::control::pending::ControlStep::Pending(mut status) = status else {
            panic!("status must wait only for its independent Core read");
        };
        let status_deadline = Instant::now() + Duration::from_secs(2);
        let status = loop {
            match (status.continuation)(&mut daemon, &mut state) {
                crate::daemon::control::pending::ControlPoll::Ready(response) => break response,
                crate::daemon::control::pending::ControlPoll::ReadyRetained(_) => {
                    panic!("status must not carry a plugin-result charge")
                }
                crate::daemon::control::pending::ControlPoll::ReadyHost(_, _) => {
                    panic!("status must not carry a host-result charge")
                }
                crate::daemon::control::pending::ControlPoll::Pending => {
                    assert!(
                        Instant::now() < status_deadline,
                        "unrelated status exceeded the safety deadline"
                    );
                    thread::sleep(Duration::from_millis(5));
                }
            }
        };
        assert_eq!(
            status.expect("status response").kind,
            DaemonResponseKind::Status
        );
        assert!(
            status_started.elapsed() < Duration::from_secs(2),
            "unrelated owner control exceeded the safety deadline"
        );

        crate::lua_runtime::release_test_plugin_invocation_gate();
        // Five seconds is a test safety bound. It is not a plugin latency requirement.
        let deadline = Instant::now() + Duration::from_secs(5);
        let response = loop {
            if let Some(runtime) = daemon.runtime() {
                let DaemonControlState {
                    maintenance,
                    plugin_controls,
                    plugin_entities,
                    plugin_result_budget,
                    ..
                } = &mut state;
                run_completion_drain_slice_for_owner(
                    runtime,
                    maintenance,
                    plugin_controls,
                    plugin_entities,
                    plugin_result_budget,
                );
            }
            match (held.continuation)(&mut daemon, &mut state) {
                crate::daemon::control::pending::ControlPoll::Ready(response) => break response,
                crate::daemon::control::pending::ControlPoll::ReadyRetained(response) => {
                    break response.into_parts().0;
                }
                crate::daemon::control::pending::ControlPoll::ReadyHost(_, _) => {
                    panic!("plugin response must not carry a host-result charge")
                }
                crate::daemon::control::pending::ControlPoll::Pending => {
                    assert!(
                        Instant::now() < deadline,
                        "held plugin reply exceeded the safety deadline"
                    );
                    thread::sleep(Duration::from_millis(5));
                }
            }
        }
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

        let refused_id = u64::try_from(botster_hub_client::MAX_OUTSTANDING_REQUESTS + 1)
            .expect("refused request id");
        let refusal = read_response(&mut client, &mut reader, refused_id);
        assert_eq!(refusal.kind, DaemonResponseKind::OperatorError);
        assert_eq!(
            refusal.error.as_ref().map(|error| error.code.as_str()),
            Some(botster_hub_client::OPERATOR_ERROR_TOO_MANY_REQUESTS)
        );
        assert_eq!(
            state.pending_requests.len(),
            botster_hub_client::MAX_OUTSTANDING_REQUESTS
        );

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
        while state.pending_requests.len() > botster_hub_client::MAX_OUTSTANDING_REQUESTS {
            assert!(!crate::daemon::control::request::poll_deferred(
                &mut daemon,
                &mut state,
            ));
            assert!(
                Instant::now() < sibling_deadline,
                "the sibling Status request must complete while the plugin handler is held"
            );
            if state.pending_requests.len() > botster_hub_client::MAX_OUTSTANDING_REQUESTS {
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
            if let Some(runtime) = daemon.runtime() {
                let DaemonControlState {
                    maintenance,
                    plugin_controls,
                    plugin_entities,
                    plugin_result_budget,
                    ..
                } = &mut state;
                let _ = run_completion_drain_slice_for_owner(
                    runtime,
                    maintenance,
                    plugin_controls,
                    plugin_entities,
                    plugin_result_budget,
                );
            }
            assert!(!crate::daemon::control::request::poll_deferred(
                &mut daemon,
                &mut state,
            ));
            assert!(
                Instant::now() < deadline,
                "admitted plugin requests did not complete after gate release"
            );
            if !state.pending_requests.is_empty() {
                thread::sleep(Duration::from_millis(5));
            }
        }

        let mut completed = vec![false; botster_hub_client::MAX_OUTSTANDING_REQUESTS + 1];
        for _ in 0..botster_hub_client::MAX_OUTSTANDING_REQUESTS {
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
            Err(DaemonTransportError::Client(_))
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
            Err(DaemonTransportError::Client(_))
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
                if let Some(runtime) = daemon.runtime() {
                    let DaemonControlState {
                        maintenance,
                        plugin_controls,
                        plugin_entities,
                        plugin_result_budget,
                        ..
                    } = &mut state;
                    let _ = run_completion_drain_slice_for_owner(
                        runtime,
                        maintenance,
                        plugin_controls,
                        plugin_entities,
                        plugin_result_budget,
                    );
                }
                crate::daemon::control::entities::drive_plugin_entity_completions(
                    &mut daemon,
                    &mut state,
                );
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
    fn abandoned_plugin_replies_release_owner_capacity_while_execution_remains_live() {
        for retire_reason in ["reply_closed", "deadline"] {
            let root = unique_package_control_dir(&format!("controlled-plugin-{retire_reason}"));
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

            let (control_tx, _control_rx) = tokio_mpsc::channel(8);
            let mut state = DaemonControlState::default();
            let waiter_id = crate::owner_identity::WaiterId(1);
            state.current_waiter_id = Some(waiter_id);
            crate::lua_runtime::arm_test_plugin_invocation_gate();
            let request = DaemonRequest::PluginMcpCallTool {
                name: "owner.controlled_gate".to_string(),
                arguments: serde_json::json!({ "token": retire_reason }),
            };
            let step = handle_control_request(
                &mut daemon,
                &mut state,
                DaemonObservability {
                    egress: Vec::new(),
                    lifecycle: DaemonLifecycleCounters::default(),
                    client_id: Some(format!("connection-{retire_reason}")),
                    grant_id: None,
                    transport_request_id: Some("91".to_string()),
                },
                control_tx,
                request.clone(),
            );
            let crate::daemon::control::pending::ControlStep::Pending(step) = step else {
                panic!("the controlled plugin request must wait for its worker");
            };
            state.current_waiter_id = None;
            assert!(
                crate::lua_runtime::wait_for_test_plugin_invocation_gate(Duration::from_secs(2)),
                "the controlled plugin worker must enter the gate"
            );
            let permit = state.budget.reserve().expect("pending request permit");
            let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
            let mut reply_rx = Some(reply_rx);
            let accepted_at = if retire_reason == "deadline" {
                Instant::now()
                    .checked_sub(crate::daemon::owner_budget::RETAINED_OPERATION_DEADLINE)
                    .expect("deadline timestamp")
            } else {
                Instant::now()
            };
            if retire_reason == "reply_closed" {
                drop(reply_rx.take());
            }
            state.pending_requests.insert(
                waiter_id,
                crate::daemon::control::pending::PendingControlRequest {
                    waiter_id,
                    ready_class: step.ready_class,
                    ready_key: None,
                    deadline_key: None,
                    last_core_phase: 0,
                    last_host_phase: 0,
                    request,
                    reply_tx,
                    response_delivery_rx: None,
                    grant_id: None,
                    client: Some(format!("connection-{retire_reason}")),
                    permit: Some(permit),
                    accepted_at,
                    must_finish: false,
                    past_deadline: false,
                    continuation: step.continuation,
                    retire: step.retire,
                },
            );
            assert_eq!(state.budget.outstanding(), 1);

            crate::daemon::control::pending::poll_pending_requests(
                &mut daemon,
                &mut state,
                Instant::now(),
                |_, _, _, _| panic!("an abandoned held request must retire before completion"),
            );

            assert!(state.pending_requests.is_empty());
            assert_eq!(state.budget.outstanding(), 0);
            assert!(
                !state
                    .plugin_controls
                    .has_transport_correlation(&format!("connection-{retire_reason}"), "91"),
                "retirement must remove the reply correlation"
            );
            assert!(
                crate::lua_runtime::wait_for_test_plugin_invocation_gate(Duration::ZERO),
                "Core execution must remain live after Hub releases reply capacity"
            );

            crate::lua_runtime::release_test_plugin_invocation_gate();
            drop(reply_rx);
            daemon.stop();
        }
    }

    #[test]
    fn abandoned_plugin_entity_subscriptions_release_owner_capacity() {
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
            assert!(
                crate::lua_runtime::wait_for_test_plugin_invocation_gate(Duration::from_secs(2)),
                "the controlled entity worker must enter the gate"
            );

            if retire_reason == "connection_close" {
                crate::daemon::control::entities::retire_plugin_entity_connection(
                    &daemon,
                    &mut state,
                    &connection_id,
                );
            } else {
                crate::daemon::control::entities::retire_plugin_entity_replies(
                    &daemon,
                    &mut state,
                    Instant::now() + crate::daemon::owner_budget::RETAINED_OPERATION_DEADLINE,
                );
            }

            assert_eq!(state.budget.outstanding(), baseline);
            assert!(state.plugin_entities.next_reply_deadline().is_none());
            assert!(
                crate::lua_runtime::wait_for_test_plugin_invocation_gate(Duration::ZERO),
                "Core execution must remain live after Hub releases entity reply capacity"
            );

            crate::lua_runtime::release_test_plugin_invocation_gate();
            drop(reply_rx);
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
        daemon
            .entrypoint_supervisor()
            .snapshots()
            .iter()
            .any(|snapshot| {
                snapshot.package_name == package_name
                    && snapshot.entrypoint_id == "sleeper"
                    && snapshot.state == "running"
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

        FileHubStateStore::inject_next_save_failure();
        let error = drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "mutate.plugin".to_string(),
            },
        )
        .expect_err("injected persist failure");
        assert!(matches!(
            error,
            DaemonTransportError::State(crate::HubStateStoreError::InjectedWriteFailure)
        ));
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
            daemon
                .entrypoint_supervisor()
                .snapshots()
                .iter()
                .any(|snapshot| {
                    snapshot.package_name == "running.plugin"
                        && snapshot.entrypoint_id == "sleeper"
                        && snapshot.state == "running"
                }),
            "sleeper must be running before failed disable"
        );

        FileHubStateStore::inject_next_save_failure();
        drive_package_request(
            &mut daemon,
            DaemonRequest::DisablePackage {
                package_name: "running.plugin".to_string(),
            },
        )
        .expect_err("injected disable persist failure");
        assert_eq!(
            package_state(&daemon, "running.plugin"),
            PackageState::Enabled
        );
        assert!(
            daemon
                .entrypoint_supervisor()
                .snapshots()
                .iter()
                .any(|snapshot| {
                    snapshot.package_name == "running.plugin"
                        && snapshot.entrypoint_id == "sleeper"
                        && snapshot.state == "running"
                }),
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
        FileHubStateStore::inject_next_save_failure();
        drive_package_request(
            &mut daemon,
            DaemonRequest::ReloadPackage {
                package_name: "reload.plugin".to_string(),
            },
        )
        .expect_err("injected reload persist failure");
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
        drive_package_request(
            &mut daemon,
            DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install");
        std::fs::remove_file(package_dir.join("plugin.lua")).expect("remove lua");
        FileHubStateStore::inject_save_failure_after(1);
        let error = drive_package_request(
            &mut daemon,
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

        FileHubStateStore::inject_next_save_failure();
        drive_package_request(
            &mut daemon,
            DaemonRequest::EnablePackage {
                package_name: "types.plugin".to_string(),
            },
        )
        .expect_err("injected enable persist failure");
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
                runtime.reap_detached_core_operations();
            }
            match (pending.continuation)(daemon, state) {
                crate::daemon::control::pending::ControlPoll::Ready(response) => {
                    break response.expect("attach response");
                }
                crate::daemon::control::pending::ControlPoll::ReadyRetained(_) => {
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
        assert!(
            state.background.pump_pending(),
            "journal consumption must not clear the terminal inventory Pump latch"
        );
        assert_eq!(state.background.select(false), Some(BackgroundClass::Pump));
        assert!(!state.background.pump_pending());
    }

    #[test]
    fn terminal_inventory_change_during_reconcile_schedules_one_fresh_pass_without_spin() {
        let (mut daemon, mut state, _mux, _session_id) =
            reconcile_wiring_fixture("inventory-wake-during-reconcile");

        state.background.mark_pump();
        assert_eq!(state.background.select(false), Some(BackgroundClass::Pump));
        state.pump.force_next(PumpPhase::InventoryReconcile);
        run_one_pump_phase(&mut daemon, &mut state);
        assert!(state.reconcile_inventory.is_some());
        assert!(state.background.pump_pending());

        state.note_terminal_inventory_changed();
        state.resolve_submitted_reconcile_inventory_for_test(Vec::new());
        assert_eq!(state.background.select(false), Some(BackgroundClass::Pump));
        state.pump.force_next(PumpPhase::InventoryReconcile);
        run_one_pump_phase(&mut daemon, &mut state);
        assert!(
            state.background.pump_pending(),
            "an inventory change during the read must schedule a fresh pass"
        );

        assert_eq!(state.background.select(false), Some(BackgroundClass::Pump));
        state.pump.force_next(PumpPhase::InventoryReconcile);
        run_one_pump_phase(&mut daemon, &mut state);
        assert!(state.reconcile_inventory.is_some());
        state.resolve_submitted_reconcile_inventory_for_test(Vec::new());
        assert_eq!(state.background.select(false), Some(BackgroundClass::Pump));
        state.pump.force_next(PumpPhase::InventoryReconcile);
        run_one_pump_phase(&mut daemon, &mut state);
        assert!(state.reconcile_inventory.is_none());
        assert!(
            !state.background.pump_pending(),
            "a clean fresh pass must not create idle Pump work"
        );
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
