//! Hub owner thread.

use std::collections::BTreeMap;
use std::fmt;
use std::os::unix::net::UnixListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self};
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{SessionId, SubscriptionId};
use botster_hub_client::{
    DaemonDiagnostic, DaemonLifecycleCounters, DaemonRequest, DaemonResponse, DaemonResponseKind,
};
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use tokio::net::UnixListener as TokioUnixListener;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::{Semaphore, oneshot, watch};

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
    decide_background_slice, run_maintenance_kind,
};
use crate::subscription::attach_routes::AttachStreamRegistry;
use crate::subscription::entity::{
    EntitySubscriptionState, drive_entity_subscriptions, drive_package_entity_fanout,
    drive_package_entity_resync, seed_lifecycle_reconciliation, session_subscribers_need_delivery,
};
use crate::transport::unix::connection::{
    handle_connection_async, handle_connection_cleanup, reap_finished_connection_tasks,
    wait_for_connection_tasks,
};
use crate::transport::unix::listener::{
    accept_connections, cleanup_socket_path, prepare_socket_path, rebind_missing_socket_path,
    socket_path,
};
use crate::transport::webrtc::LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE;

const ENTITY_RECONCILIATION_INTERVAL: Duration = Duration::from_millis(500);

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
                    MaintenanceSliceKind::PackageEventDelivery,
                );
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
                run_maintenance_kind(runtime, &mut state.maintenance, other);
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

pub fn serve_daemon(config: HubConfig) -> DaemonTransportResult<HubDaemonStatus> {
    let socket_path = socket_path(&config)?;
    let local_webrtc_terminal_record_path = config
        .data_directory
        .join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    prepare_socket_path(&socket_path)?;
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
        ..DaemonControlState::default()
    };
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
    let mut connection_tasks = vec![transport_runtime.spawn(accept_connections(
        listener,
        control_tx.clone(),
        shutdown_tx.subscribe(),
        Arc::new(Semaphore::new(DAEMON_MAX_CONNECTIONS)),
    ))];
    loop {
        reap_finished_connection_tasks(&mut connection_tasks);
        while let Ok(cleanup) = cleanup_rx.try_recv() {
            handle_connection_cleanup(&mut daemon, &mut control_state, control_tx.clone(), cleanup);
        }
        mark_due_reconciliation(&mut control_state, Instant::now());
        let slice_due = control_state
            .background
            .has_pending(owner_maintenance_pending(&daemon, &control_state));
        let event = match classify_owner_poll(control_rx.try_recv(), slice_due) {
            OwnerPollDecision::ServeControl(message) => Some(OwnerEvent::Control(message)),
            OwnerPollDecision::RunSlice => None,
            OwnerPollDecision::Block => {
                let wait = control_state
                    .next_reconciliation
                    .saturating_duration_since(Instant::now());
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
                        if let Err(error) =
                            handle_connection_async(stream, tx, cleanup, shutdown, event_plane)
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
                        &local_webrtc_terminal_record_path,
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
                        cleanup_socket_path(&socket_path);
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
        if !socket_path.exists() {
            rebind_missing_socket_path(&socket_path);
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
    let should_stop = matches!(
        response,
        Ok(DaemonResponse {
            kind: DaemonResponseKind::Shutdown,
            ..
        })
    );
    let response_received = reply_tx.send(response).is_ok();
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
            let (reply_tx, _reply_rx) = oneshot::channel();
            let _ = control_tx.blocking_send(ControlMessage::Request {
                request: Box::new(DaemonRequest::DaemonShutdown),
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

pub(crate) fn run_inventory_reconcile_phase(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
) -> bool {
    let Some(runtime) = daemon.runtime() else {
        state.pump.reconcile_after = None;
        return false;
    };
    let lookup = |session_id: &str, subscription_id: &str| {
        runtime.terminal_subscription_generation(
            &SessionId(session_id.to_string()),
            &SubscriptionId(subscription_id.to_string()),
        )
    };
    let progress = state.pending_runtime.reconcile_inventory_slice(
        lookup,
        state.pump.reconcile_after.clone(),
        PUMP_MAX_ROUTES_VALIDATED,
    );
    if progress.more {
        state.pump.reconcile_after = progress.after;
        true
    } else {
        state.pump.reconcile_after = None;
        false
    }
}

fn run_pump_observe_phase(daemon: &HubDaemon, state: &mut DaemonControlState) -> bool {
    let Some(runtime) = daemon.runtime() else {
        state.observe_resume = None;
        return false;
    };
    let now = tick(&mut state.logical_clock);
    let resume = state.observe_resume.as_ref();
    let slice = runtime.observe_lifecycle_slice(now, resume, OBSERVE_SLICE_BUDGET);
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
        if runtime.take_journal_advanced_wake() {
            state.maintenance.note_authoritative_mutation();
            state.background.mark_pump();
        }
        state.observe_resume.is_some()
    } else {
        false
    }
}

#[derive(Debug)]

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
    observe_resume: Option<botster_core_daemon::ObserveLifecycleCursor>,
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
            observe_resume: None,
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
    receiver: oneshot::Receiver<DaemonTransportResult<DaemonResponse>>,
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
    use crate::subscription::entity::entity_subscription_error;
    use crate::transport::unix::connection::{cleanup_detach_failed, handle_connection};
    use botster_core::RequestId;
    use botster_core::contract::terminal_adapter::TerminalAdapter;
    use botster_hub_client::{
        DaemonHello, DaemonHelloAck, DaemonRequest, DaemonResponse, DaemonResponseKind, PROTOCOL,
        read_frame, write_frame,
    };
    use botster_terminal_protocol::TerminalFrame;
    use serde_json::Value;
    use std::io::Write;
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

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
            "async fn read_async_frame",
            "fn prepare_socket_path",
            "fn unix_event_flush_stalled",
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
                && mux.contains("pub(crate) async fn read_async_frame")
                && mux.contains("pub(crate) fn unix_event_flush_stalled"),
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
    fn connection_cleanup_ignores_only_an_already_removed_session() {
        let unknown_session = Err(DaemonTransportError::Client(
            crate::HubClientError::Runtime {
                request_id: RequestId("cleanup-detach".to_string()),
                operation: crate::HubClientOperation::Detach,
                kind: crate::HubClientRuntimeErrorKind::UnknownSession,
            },
        ));
        assert!(!cleanup_detach_failed(&unknown_session));

        let unavailable_runtime: DaemonTransportResult<DaemonResponse> =
            Err(DaemonTransportError::DaemonNotRunning);
        assert!(cleanup_detach_failed(&unavailable_runtime));
        assert!(cleanup_detach_failed(&Ok(entity_subscription_error(
            "detach_failed",
            "cleanup-detach",
            "detach failed",
        ))));
    }

    #[test]
    fn client_eof_detaches_connection_subscriptions() {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        let (control_tx, mut control_rx) = tokio_mpsc::channel(DAEMON_CONTROL_QUEUE_CAPACITY);
        let connection = thread::spawn(move || handle_connection(server, control_tx));

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        )
        .expect("write daemon hello");
        let _: DaemonHelloAck = read_frame(&mut client).expect("read daemon hello ack");

        write_frame(
            &mut client,
            &DaemonRequest::Attach {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            },
        )
        .expect("write attach request");
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
        let _: DaemonResponse = read_frame(&mut client).expect("read attach response");

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

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        )
        .expect("write daemon hello");
        let _: DaemonHelloAck = read_frame(&mut client).expect("read daemon hello ack");

        let ControlMessage::RegisterUnixAdmission { reply_tx, .. } =
            receive_test_control_message(&mut control_rx)
        else {
            panic!("expected RegisterUnixAdmission after Hello");
        };
        write_frame(&mut client, &DaemonRequest::Status)
            .expect("write status while admission ack is held");
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
        let _: DaemonResponse = read_frame(&mut client).expect("read status response");
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

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        )
        .expect("write daemon hello");
        let _: DaemonHelloAck = read_frame(&mut client).expect("read daemon hello ack");

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
        mux.register(
            "partial-session".to_string(),
            "partial-subscription".to_string(),
            1,
            handle,
        );

        let mut request_bytes =
            serde_json::to_vec(&DaemonRequest::Status).expect("encode status request");
        request_bytes.push(b'\n');
        let split = request_bytes.len() / 2;
        client
            .write_all(&request_bytes[..split])
            .expect("write partial status request");

        let frame =
            TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"writer-wake"}"#)
                .expect("create terminal output frame");
        adapter.try_write(&frame).expect("store terminal output");
        reply_tx.send(()).expect("ack unix admission");
        let terminal: Value = read_frame(&mut client).expect("read terminal output");
        assert_eq!(
            terminal.get("plane").and_then(Value::as_str),
            Some(botster_hub_client::UNIX_TERMINAL_PLANE)
        );

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
        let response: DaemonResponse = read_frame(&mut client).expect("read status response");
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

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        )
        .expect("write daemon hello");
        let _: DaemonHelloAck = read_frame(&mut client).expect("read daemon hello ack");

        write_frame(
            &mut client,
            &DaemonRequest::Attach {
                session_id: "missing-session".to_string(),
                subscription_id: "missing-sub".to_string(),
            },
        )
        .expect("write attach request");
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
        let _: DaemonResponse = read_frame(&mut client).expect("read attach operator error");

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

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        )
        .expect("write daemon hello");
        let _: DaemonHelloAck = read_frame(&mut client).expect("read daemon hello ack");

        write_frame(
            &mut client,
            &DaemonRequest::Attach {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            },
        )
        .expect("write attach request");
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
        let _: DaemonResponse = read_frame(&mut client).expect("read attach operator error");

        write_frame(&mut client, &DaemonRequest::Status).expect("write status");
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
        let _: DaemonResponse = read_frame(&mut client).expect("read status");

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
        let runtime = crate::HubRuntime::new(config);
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
        let runtime = crate::HubRuntime::new(config);
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

        let (reply_tx, reply_rx) = oneshot::channel();
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
        let (reply_tx, reply_rx) = oneshot::channel();
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
        let (reply_tx, reply_rx) = oneshot::channel();
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

        write_frame(
            &mut client,
            &DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        )
        .expect("write daemon hello");
        let _: DaemonHelloAck = read_frame(&mut client).expect("read daemon hello ack");
        write_frame(&mut client, &DaemonRequest::DaemonShutdown)
            .expect("write daemon shutdown request");

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
        let egress = DaemonEgressDiagnostics::default();
        let lifecycle = DaemonLifecycleCounters::default();
        let observability = DaemonObservability {
            egress: &egress,
            lifecycle: &lifecycle,
            client_id: None,
            grant_id: None,
        };
        let mut clock = 1;
        let mut drain_cursors = BTreeMap::new();
        let mut pending = PendingRuntimeState::default();
        handle_control_request(
            daemon,
            &mut clock,
            &mut drain_cursors,
            &mut pending,
            observability,
            control_tx,
            request,
        )
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
}
