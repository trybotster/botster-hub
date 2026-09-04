//! Unix accepted-connection driver and client connection role.
use std::collections::BTreeSet;
use std::io::Write;
#[cfg(test)]
use std::os::unix::net::UnixStream;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{ClientId, SessionId, SubscriptionId};
use botster_core_daemon::DetachTerminalSubscriptionResult;
use botster_hub_client::DaemonConnection as ClientDaemonConnection;
use botster_hub_client::DaemonTransportError as ClientDaemonTransportError;
use botster_hub_client::{
    DaemonHello, DaemonRequest, DaemonResponse, DaemonResponseKind, PROTOCOL,
};
use tokio::io::BufReader as AsyncBufReader;
use tokio::net::UnixStream as TokioUnixStream;
use tokio::sync::{mpsc as tokio_mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::HubConfig;
use crate::HubDaemon;
use crate::admission::budgets::{
    DAEMON_CLIENT_WRITE_TIMEOUT, DAEMON_HANDSHAKE_TIMEOUT, ENTITY_SUBSCRIPTION_QUEUE_CAPACITY,
};
use crate::admission::unix_hello::{UnixTerminalAdmission, unix_hello_admission};
use crate::daemon::control::message::{
    ControlMessage, ControlSender, DaemonDeliveryKind, daemon_delivery_kind, egress_write_class,
};
use crate::daemon::control::{DaemonObservability, handle_control_request};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::{DaemonControlState, tick};
use crate::subscription::attach_routes::{
    AttachedSubscription, AttachedSubscriptionChange, UnixEofAblation,
    apply_attached_subscription_change, attached_subscription_change_for_response,
    live_generation_for_route, record_attached_subscription_change, unix_eof_cleanup_ablation,
};
use crate::subscription::entity::EntityFrameSender;
use crate::transport::unix::UnixConnectionMux;
use crate::transport::unix::listener::{NEXT_SOCKET_CLIENT_ID, daemon_endpoint};
use crate::transport::unix::mux_write::{
    MuxWriteState, UnixInbound, entity_subscription_mux_busy_error, flush_pending_responses,
    flush_unix_mux_writes, read_async_frame, read_async_inbound, unix_event_flush_stalled,
    unix_mux_blocks_entity_subscription, write_async_frame,
};

pub fn request(
    config: &HubConfig,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let endpoint = daemon_endpoint(config)?;
    botster_hub_client::request(&endpoint, request).map_err(DaemonTransportError::from)
}

/// Persistent daemon connection for clients that own attach subscription state.
pub struct DaemonConnection {
    inner: ClientDaemonConnection,
}

impl DaemonConnection {
    /// Connect to the daemon and complete the socket protocol handshake.
    pub fn connect(config: &HubConfig) -> DaemonTransportResult<Self> {
        let endpoint = daemon_endpoint(config)?;
        let inner =
            ClientDaemonConnection::connect(&endpoint).map_err(DaemonTransportError::from)?;
        Ok(Self { inner })
    }

    /// Send one request over this persistent connection.
    pub fn request(&mut self, request: &DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
        self.inner
            .request(request)
            .map_err(DaemonTransportError::from)
    }

    /// Write one opaque terminal input frame on this muxed Unix connection.
    pub fn send_terminal_frame(
        &mut self,
        session_id: impl Into<String>,
        subscription_id: impl Into<String>,
        frame_bytes: &[u8],
    ) -> DaemonTransportResult<()> {
        self.inner
            .send_terminal_frame(session_id, subscription_id, frame_bytes)
            .map_err(DaemonTransportError::from)
    }
}

/// Attach and stream terminal bytes until the session exits or the connection closes.
pub fn stream_attach(
    config: &HubConfig,
    session_id: SessionId,
    subscription_id: SubscriptionId,
    output: &mut impl Write,
) -> DaemonTransportResult<()> {
    let endpoint = daemon_endpoint(config)?;
    botster_hub_client::stream_attach(&endpoint, &session_id.0, &subscription_id.0, output)
        .map_err(DaemonTransportError::from)
}

pub(crate) async fn handle_connection_async(
    stream: TokioUnixStream,
    control_tx: ControlSender,
    cleanup_tx: SyncSender<ConnectionCleanup>,
    mut shutdown_rx: watch::Receiver<bool>,
    event_plane: std::sync::Arc<crate::subscription::package_events::ClientEventPlane>,
) -> DaemonTransportResult<()> {
    let client_id = format!(
        "botster-hub-daemon-socket-{}",
        NEXT_SOCKET_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
    );
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = AsyncBufReader::new(read_half);
    let mut cleanup = ConnectionCleanupGuard::new(
        cleanup_tx,
        client_id.clone(),
        ConnectionTerminalReason::Protocol,
    );
    let hello: DaemonHello =
        match read_async_frame(&mut reader, Some(DAEMON_HANDSHAKE_TIMEOUT)).await {
            Ok(hello) => hello,
            Err(ClientDaemonTransportError::ClientDisconnected) => {
                cleanup.set_reason(ConnectionTerminalReason::Eof);
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
    if hello.protocol != PROTOCOL {
        return Err(DaemonTransportError::Protocol("unexpected hello protocol"));
    }
    let (admission, hello_ack) = unix_hello_admission(&hello);
    if let Err(error) = write_async_frame(&mut write_half, &hello_ack).await {
        cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
        return Err(error);
    }
    cleanup.set_reason(ConnectionTerminalReason::Eof);

    let mut mux_write = MuxWriteState::default();
    let mux = match &admission {
        UnixTerminalAdmission::Admitted { mux, .. } => mux.clone(),
        UnixTerminalAdmission::Rejected { .. } => UnixConnectionMux::new(),
    };
    let (admission_ack_tx, admission_ack_rx) = oneshot::channel();
    control_tx
        .send(ControlMessage::RegisterUnixAdmission {
            client_id: client_id.clone(),
            admission,
            reply_tx: admission_ack_tx,
            host_required_features: hello.compatibility.required_features.clone(),
        })
        .await
        .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
    admission_ack_rx
        .await
        .map_err(|_| DaemonTransportError::ControlThreadStopped)?;

    loop {
        let request = {
            let inbound = read_async_inbound(&mut reader, None);
            tokio::pin!(inbound);
            loop {
                let event_mailbox = event_plane.mailbox(&client_id);
                let event_output_ready = !unix_event_flush_stalled()
                    && event_mailbox
                        .as_ref()
                        .is_some_and(|mailbox| mailbox.has_ready_event());
                tokio::select! {
                    biased;
                    request = &mut inbound => break request,
                    _ = mux.wait_for_write() => {
                        mux.clear_deferred_flushes();
                        if let Err(error) = flush_unix_mux_writes(
                            &mut write_half,
                            &mux,
                            &mut mux_write,
                            event_mailbox.as_deref(),
                        ).await {
                            cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                            mux.close_all();
                            return Err(error);
                        }
                    }
                    _ = async {
                        if unix_event_flush_stalled() {
                            std::future::pending::<()>().await;
                            return;
                        }
                        if let Some(mailbox) = event_mailbox.as_ref() {
                            let notified = mailbox.notify().notified();
                            tokio::pin!(notified);
                            if mailbox.take_wake() || mailbox.has_ready_event() {
                                return;
                            }
                            notified.await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => {
                        mux.clear_deferred_flushes();
                        if let Err(error) = flush_unix_mux_writes(
                            &mut write_half,
                            &mux,
                            &mut mux_write,
                            event_mailbox.as_deref(),
                        ).await {
                            cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                            mux.close_all();
                            return Err(error);
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(25)), if mux.has_unsent_mux_writes() || mux_write.has_pending() || event_output_ready || (unix_event_flush_stalled() && event_mailbox.as_ref().is_some_and(|mailbox| mailbox.has_ready_event())) => {
                        mux.clear_deferred_flushes();
                        if let Err(error) = flush_unix_mux_writes(
                            &mut write_half,
                            &mux,
                            &mut mux_write,
                            event_mailbox.as_deref(),
                        ).await {
                            cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                            mux.close_all();
                            return Err(error);
                        }
                    }
                    changed = shutdown_rx.changed() => {
                        let _ = changed;
                        cleanup.set_reason(ConnectionTerminalReason::Shutdown);
                        return Ok(());
                    }
                }
            }
        };
        let request = match request {
            Ok(UnixInbound::Request(request)) => request,
            Ok(UnixInbound::Terminal(envelope)) => {
                match envelope.payload_bytes() {
                    Ok(bytes) => {
                        if let Some(handle) =
                            mux.live_handle(&envelope.session_id, &envelope.subscription_id)
                            && handle
                                .push_ingress_for_route(
                                    bytes,
                                    &envelope.session_id,
                                    &envelope.subscription_id,
                                )
                                .is_err()
                        {
                            handle.close();
                        }
                    }
                    Err(_) => {
                        if let Some(handle) =
                            mux.live_handle(&envelope.session_id, &envelope.subscription_id)
                        {
                            handle.close();
                        }
                    }
                }
                mux.clear_deferred_flushes();
                if let Err(error) = flush_unix_mux_writes(
                    &mut write_half,
                    &mux,
                    &mut mux_write,
                    event_plane.mailbox(&client_id).as_deref(),
                )
                .await
                {
                    cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                    mux.close_all();
                    return Err(error);
                }
                continue;
            }
            Err(ClientDaemonTransportError::ClientDisconnected) => {
                cleanup.set_reason(ConnectionTerminalReason::Eof);
                return Ok(());
            }
            Err(error) => {
                cleanup.set_reason(ConnectionTerminalReason::Protocol);
                return Err(error.into());
            }
        };
        if let DaemonRequest::SubscribeEntities {
            entity_type,
            subscription_id,
        } = request
        {
            if unix_mux_blocks_entity_subscription(&mux, &mux_write) {
                mux_write.enqueue_response(&entity_subscription_mux_busy_error(), None, false)?;
                if let Err(error) = flush_pending_responses(
                    &mut write_half,
                    &mux,
                    &mut mux_write,
                    Instant::now(),
                    event_plane.mailbox(&client_id).as_deref(),
                )
                .await
                {
                    cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                    mux.close_all();
                    return Err(error);
                }
                continue;
            }
            return handle_entity_subscription_async(
                write_half,
                reader,
                control_tx,
                entity_type,
                subscription_id,
                cleanup,
                shutdown_rx,
            )
            .await;
        }
        mux.clear_deferred_flushes();
        if let Err(error) = flush_unix_mux_writes(
            &mut write_half,
            &mux,
            &mut mux_write,
            event_plane.mailbox(&client_id).as_deref(),
        )
        .await
        {
            cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
            mux.close_all();
            return Err(error);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let close_after_response = matches!(request, DaemonRequest::DaemonShutdown);
        let requires_delivery_ack =
            close_after_response || matches!(request, DaemonRequest::StartHubUpdate { .. });
        let (response_delivery_tx, response_delivery_rx) = if requires_delivery_ack {
            let (tx, rx) = mpsc::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let ownership_request = request.clone();
        control_tx
            .send(ControlMessage::Request {
                request: Box::new(request),
                reply_tx,
                response_delivery_rx,
                grant_id: None,
                client_id: Some(client_id.clone()),
                enqueued_at: Instant::now(),
            })
            .await
            .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        let response = receive_control_response(reply_rx).await?;
        cleanup.apply_subscription_change(attached_subscription_change_for_response(
            &ownership_request,
            &response,
        ));
        mux_write.enqueue_response(&response, response_delivery_tx, close_after_response)?;
        debug_assert_eq!(mux_write.pending_response_count(), 1);
        if let Err(error) = flush_pending_responses(
            &mut write_half,
            &mux,
            &mut mux_write,
            Instant::now(),
            event_plane.mailbox(&client_id).as_deref(),
        )
        .await
        {
            cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
            let _ = control_tx.try_send(ControlMessage::EgressWriteFailed {
                delivery_kind: daemon_delivery_kind(&response),
                write_class: egress_write_class(&error),
            });
            mux.close_all();
            return Err(error);
        }
        mux.clear_deferred_flushes();
        if let Err(error) = flush_unix_mux_writes(
            &mut write_half,
            &mux,
            &mut mux_write,
            event_plane.mailbox(&client_id).as_deref(),
        )
        .await
        {
            cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
            mux.close_all();
            return Err(error);
        }
        if close_after_response {
            debug_assert!(!mux_write.has_close_after_pending());
            cleanup.set_reason(ConnectionTerminalReason::NormalClose);
            return Ok(());
        }
    }
}

pub(crate) async fn handle_entity_subscription_async(
    mut write_half: tokio::net::unix::OwnedWriteHalf,
    mut reader: AsyncBufReader<tokio::net::unix::OwnedReadHalf>,
    control_tx: ControlSender,
    entity_type: String,
    subscription_id: String,
    mut cleanup: ConnectionCleanupGuard,
    mut shutdown_rx: watch::Receiver<bool>,
) -> DaemonTransportResult<()> {
    let (frame_tx, mut frame_rx) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
    let (reply_tx, reply_rx) = oneshot::channel();
    control_tx
        .send(ControlMessage::SubscribeEntities {
            entity_type,
            subscription_id: subscription_id.clone(),
            frame_tx: EntityFrameSender::Async(frame_tx),
            frame_rx: None,
            reply_tx,
            grant_id: None,
        })
        .await
        .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
    let response = receive_control_response(reply_rx).await?;
    write_async_frame(&mut write_half, &response).await?;
    if response.kind != DaemonResponseKind::EntitySubscribed {
        cleanup.set_reason(ConnectionTerminalReason::NormalClose);
        return Ok(());
    }
    cleanup.add_entity_subscription(subscription_id.clone());
    let inbound_request = read_async_frame::<DaemonRequest, _>(&mut reader, None);
    tokio::pin!(inbound_request);

    loop {
        tokio::select! {
            frame = frame_rx.recv() => {
                let Some(frame) = frame else {
                    cleanup.set_reason(ConnectionTerminalReason::Cancellation);
                    return Ok(());
                };
                if let Err(error) = write_async_frame(&mut write_half, &frame).await {
                    cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                    let _ = control_tx.try_send(ControlMessage::EgressWriteFailed {
                        delivery_kind: DaemonDeliveryKind::Control,
                        write_class: egress_write_class(&error),
                    });
                    return Err(error);
                }
            }
            request = &mut inbound_request => {
                let request = match request {
                    Ok(request) => request,
                    Err(ClientDaemonTransportError::ClientDisconnected) => {
                        cleanup.set_reason(ConnectionTerminalReason::Eof);
                        return Ok(());
                    }
                    Err(error) => {
                        cleanup.set_reason(ConnectionTerminalReason::Protocol);
                        return Err(error.into());
                    }
                };
                if !matches!(
                    request,
                    DaemonRequest::UnsubscribeEntities { subscription_id: ref requested }
                        if requested == &subscription_id
                ) {
                    cleanup.set_reason(ConnectionTerminalReason::Protocol);
                    return Err(DaemonTransportError::Protocol(
                        "entity stream accepts only its matching unsubscribe request",
                    ));
                }
                let (reply_tx, reply_rx) = oneshot::channel();
                control_tx
                    .send(ControlMessage::UnsubscribeEntities {
                        subscription_id: subscription_id.clone(),
                        reply_tx: Some(reply_tx),
                        grant_id: None,
                    })
                    .await
                    .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
                let response = receive_control_response(reply_rx).await?;
                write_async_frame(&mut write_half, &response).await?;
                cleanup.remove_entity_subscription(&subscription_id);
                cleanup.set_reason(ConnectionTerminalReason::NormalClose);
                return Ok(());
            }
            changed = shutdown_rx.changed() => {
                let _ = changed;
                cleanup.set_reason(ConnectionTerminalReason::Shutdown);
                return Ok(());
            }
        }
    }
}

pub(crate) async fn receive_control_response(
    reply_rx: oneshot::Receiver<DaemonTransportResult<DaemonResponse>>,
) -> DaemonTransportResult<DaemonResponse> {
    reply_rx
        .await
        .map_err(|_| DaemonTransportError::ControlThreadStopped)?
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionTerminalReason {
    Eof,
    Protocol,
    WriteFailure,
    Cancellation,
    Shutdown,
    NormalClose,
}

impl ConnectionTerminalReason {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Eof => "eof",
            Self::Protocol => "protocol",
            Self::WriteFailure => "write_failure",
            Self::Cancellation => "cancellation",
            Self::Shutdown => "shutdown",
            Self::NormalClose => "normal_close",
        }
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionCleanup {
    client_id: String,
    attached_subscriptions: Vec<AttachedSubscription>,
    entity_subscription_ids: BTreeSet<String>,
    reason: ConnectionTerminalReason,
}

pub(crate) struct ConnectionCleanupGuard {
    cleanup_tx: SyncSender<ConnectionCleanup>,
    cleanup: Option<ConnectionCleanup>,
}

impl ConnectionCleanupGuard {
    pub(crate) fn new(
        cleanup_tx: SyncSender<ConnectionCleanup>,
        client_id: String,
        reason: ConnectionTerminalReason,
    ) -> Self {
        Self {
            cleanup_tx,
            cleanup: Some(ConnectionCleanup {
                client_id,
                attached_subscriptions: Vec::new(),
                entity_subscription_ids: BTreeSet::new(),
                reason,
            }),
        }
    }

    pub(crate) fn apply_subscription_change(&mut self, change: Option<AttachedSubscriptionChange>) {
        let Some(cleanup) = self.cleanup.as_mut() else {
            return;
        };
        apply_attached_subscription_change(&mut cleanup.attached_subscriptions, change);
    }

    pub(crate) fn add_entity_subscription(&mut self, subscription_id: String) {
        if let Some(cleanup) = self.cleanup.as_mut() {
            cleanup.entity_subscription_ids.insert(subscription_id);
        }
    }

    pub(crate) fn remove_entity_subscription(&mut self, subscription_id: &str) {
        if let Some(cleanup) = self.cleanup.as_mut() {
            cleanup.entity_subscription_ids.remove(subscription_id);
        }
    }

    pub(crate) fn set_reason(&mut self, reason: ConnectionTerminalReason) {
        if let Some(cleanup) = self.cleanup.as_mut() {
            cleanup.reason = reason;
        }
    }
}

impl Drop for ConnectionCleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take()
            && let Err(error) = self.cleanup_tx.try_send(cleanup)
        {
            eprintln!("botster-hub connection cleanup enqueue failed: {error}");
        }
    }
}

pub(crate) fn reap_finished_connection_tasks(tasks: &mut Vec<JoinHandle<()>>) {
    tasks.retain(|task| !task.is_finished());
}

pub(crate) fn wait_for_connection_tasks(
    runtime: &tokio::runtime::Runtime,
    tasks: &mut Vec<JoinHandle<()>>,
    cleanup_rx: &mpsc::Receiver<ConnectionCleanup>,
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    control_tx: ControlSender,
) {
    let deadline = Instant::now() + DAEMON_CLIENT_WRITE_TIMEOUT;
    while !tasks.iter().all(JoinHandle::is_finished) && Instant::now() < deadline {
        while let Ok(cleanup) = cleanup_rx.try_recv() {
            handle_connection_cleanup(daemon, state, control_tx.clone(), cleanup);
        }
        thread::sleep(Duration::from_millis(10));
    }
    for task in tasks.iter() {
        if !task.is_finished() {
            task.abort();
        }
    }
    runtime.block_on(async {
        for task in tasks.drain(..) {
            let _ = task.await;
        }
    });
    while let Ok(cleanup) = cleanup_rx.try_recv() {
        handle_connection_cleanup(daemon, state, control_tx.clone(), cleanup);
    }
}

pub(crate) fn handle_connection_cleanup(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    control_tx: ControlSender,
    cleanup: ConnectionCleanup,
) {
    state.lifecycle_counters.live_connections =
        state.lifecycle_counters.live_connections.saturating_sub(1);
    *state
        .lifecycle_counters
        .cleanup_by_reason
        .entry(cleanup.reason.label().to_string())
        .or_default() += 1;

    let mut failed = false;
    for subscription_id in cleanup.entity_subscription_ids {
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
    let unix_admission = state
        .pending_runtime
        .admission
        .unix_admissions
        .remove(&cleanup.client_id);
    let ablation = unix_eof_cleanup_ablation();
    let mut candidates = BTreeSet::new();
    for claim in state
        .pending_runtime
        .take_connection_bound_routes(&cleanup.client_id)
    {
        candidates.insert((claim.session_id, claim.subscription_id));
    }
    state
        .pending_runtime
        .admission
        .host_compatibility
        .remove(&cleanup.client_id);
    if let Some(runtime) = daemon.runtime() {
        state
            .event_plane
            .cleanup_connection(&cleanup.client_id, runtime.package_event_router());
    }
    for subscription in &cleanup.attached_subscriptions {
        candidates.insert((
            subscription.session_id.clone(),
            subscription.subscription_id.clone(),
        ));
    }
    let inventory_snapshot = if candidates.is_empty() {
        Vec::new()
    } else {
        daemon
            .runtime()
            .map(crate::HubRuntime::list_terminal_subscriptions)
            .unwrap_or_default()
    };

    let mut bound_closes = 0u64;
    for (session_id, subscription_id) in candidates {
        if ablation == UnixEofAblation::PairOnlyDetach {
            *state
                .lifecycle_counters
                .cleanup_by_reason
                .entry("cleanup_hub_detach".to_string())
                .or_insert(0) += 1;
            let result = handle_control_request(
                daemon,
                &mut state.logical_clock,
                &mut state.drain_cursors,
                &mut state.pending_runtime,
                DaemonObservability {
                    egress: &state.egress_diagnostics,
                    lifecycle: &state.lifecycle_counters,
                    client_id: Some(&cleanup.client_id),
                    grant_id: None,
                },
                control_tx.clone(),
                DaemonRequest::Detach {
                    session_id,
                    subscription_id,
                },
            );
            failed |= cleanup_detach_failed(&result);
            continue;
        }

        let generation = live_generation_for_route(
            &inventory_snapshot,
            &cleanup.client_id,
            &session_id,
            &subscription_id,
        )
        .or_else(|| {
            let owner = state
                .pending_runtime
                .stream_owner_client_id(&session_id, &subscription_id);
            owner.as_ref().and_then(|owner| {
                live_generation_for_route(&inventory_snapshot, owner, &session_id, &subscription_id)
                    .filter(|_| owner == &cleanup.client_id)
            })
        });
        let foreign_core_owner = inventory_snapshot.iter().any(|row| {
            row.session_id.0 == session_id
                && row.subscription_id.0 == subscription_id
                && row.client_id.0 != cleanup.client_id
        });
        let foreign_stream_owner = state
            .pending_runtime
            .stream_owner_client_id(&session_id, &subscription_id)
            .is_some_and(|owner| owner != cleanup.client_id);
        if generation.is_none() && (foreign_core_owner || foreign_stream_owner) {
            continue;
        }
        let Some(generation) = generation else {
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
            continue;
        };

        if ablation != UnixEofAblation::SkipCoreDetach {
            let now = tick(&mut state.logical_clock);
            match daemon.runtime_mut().map(|runtime| {
                runtime.detach_terminal_subscription(
                    ClientId(cleanup.client_id.clone()),
                    SessionId(session_id.clone()),
                    SubscriptionId(subscription_id.clone()),
                    generation,
                    now,
                )
            }) {
                Some(Ok(
                    DetachTerminalSubscriptionResult::Detached { .. }
                    | DetachTerminalSubscriptionResult::AlreadyGone
                    | DetachTerminalSubscriptionResult::GenerationMismatch { .. },
                )) => {}
                Some(Err(_)) => {
                    failed = true;
                    continue;
                }
                None => {}
            }
            *state
                .lifecycle_counters
                .cleanup_by_reason
                .entry("cleanup_generation_detach".to_string())
                .or_insert(0) += 1;
        }

        let was_bound = state
            .pending_runtime
            .is_adapter_bound(&session_id, &subscription_id);
        if ablation != UnixEofAblation::SkipCoreDetach {
            state
                .pending_runtime
                .close_adapter(&session_id, &subscription_id);
        }
        if ablation == UnixEofAblation::SkipCoreDetach {
            state
                .pending_runtime
                .forget_stream_without_adapter_close(&session_id, &subscription_id);
        } else {
            state
                .pending_runtime
                .cancel_stream(&session_id, &subscription_id);
        }
        if was_bound && ablation != UnixEofAblation::SkipCoreDetach {
            bound_closes += 1;
        }

        if ablation != UnixEofAblation::LeaveRoute {
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
    }
    if ablation != UnixEofAblation::SkipCoreDetach
        && let Some(UnixTerminalAdmission::Admitted { mux, .. }) = unix_admission
    {
        mux.close_all();
    }
    if bound_closes > 0 {
        *state
            .lifecycle_counters
            .cleanup_by_reason
            .entry("bound_adapter_close".to_string())
            .or_insert(0) += bound_closes;
    }
    if failed {
        // `connection_cleanup_ignores_only_an_already_removed_session` is the
        // designated positive control for predicate-true cleanup failures.
        state.lifecycle_counters.cleanup_failed =
            state.lifecycle_counters.cleanup_failed.saturating_add(1);
    } else {
        state.lifecycle_counters.cleanup_completed =
            state.lifecycle_counters.cleanup_completed.saturating_add(1);
    }
}

pub(crate) fn cleanup_detach_failed(result: &DaemonTransportResult<DaemonResponse>) -> bool {
    match result {
        Err(DaemonTransportError::Client(crate::HubClientError::Runtime {
            operation: crate::HubClientOperation::Detach,
            kind: crate::HubClientRuntimeErrorKind::UnknownSession,
            ..
        })) => false,
        Ok(response) => response.kind == DaemonResponseKind::OperatorError,
        Err(_) => true,
    }
}

#[cfg(test)]
pub(crate) fn handle_connection(
    stream: UnixStream,
    control_tx: ControlSender,
) -> DaemonTransportResult<()> {
    stream
        .set_nonblocking(true)
        .map_err(DaemonTransportError::Io)?;
    let (cleanup_tx, cleanup_rx) = mpsc::sync_channel(1);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(DaemonTransportError::Io)?;
    let stream = {
        let _runtime = runtime.enter();
        TokioUnixStream::from_std(stream).map_err(DaemonTransportError::Io)?
    };
    let result = runtime.block_on(handle_connection_async(
        stream,
        control_tx,
        cleanup_tx,
        shutdown_rx,
        std::sync::Arc::new(crate::subscription::package_events::ClientEventPlane::default()),
    ));
    let _ = cleanup_rx.try_recv();
    result
}
