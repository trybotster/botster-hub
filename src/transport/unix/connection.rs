//! Unix accepted-connection driver and client connection role.
//!
//! One task serves one muxed connection under host-control protocol 9: Hello
//! first, then correlated requests that may complete out of order, entity
//! subscription frames, host events, and content-blind terminal containers.
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
    DaemonCloseReason, DaemonOperatorError, DaemonProtocolErrorCode, DaemonRequest, DaemonResponse,
    DaemonResponseKind, MAX_OUTSTANDING_REQUESTS, OPERATOR_ERROR_TOO_MANY_REQUESTS, PROTOCOL,
    PROTOCOL_VERSION, ServerFrame, parse_request_id,
};
use tokio::io::BufReader as AsyncBufReader;
use tokio::net::UnixStream as TokioUnixStream;
use tokio::sync::{mpsc as tokio_mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::HubConfig;
use crate::HubDaemon;
use crate::admission::budgets::{
    DAEMON_CLIENT_WRITE_TIMEOUT, DAEMON_HANDSHAKE_TIMEOUT, DAEMON_MAX_CONNECTIONS,
    UNIX_CONNECTION_ENTITY_QUEUE_CAPACITY,
};
use crate::admission::unix_hello::{UnixTerminalAdmission, unix_hello_admission};
use crate::client_api_dto::response::daemon_response_base;
use crate::daemon::control::control_request_operation_label;
use crate::daemon::control::message::{
    ControlMessage, ControlSender, daemon_delivery_kind, egress_write_class,
};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::{DaemonControlState, tick};
use crate::subscription::attach_routes::{
    AttachedSubscription, AttachedSubscriptionChange, AttachmentIdentity,
    apply_attached_subscription_change, attached_subscription_change_for_response,
    live_generation_for_route, record_attached_subscription_change,
};
use crate::subscription::entity::EntityFrameSender;
use crate::transport::unix::UnixConnectionMux;
use crate::transport::unix::listener::{NEXT_SOCKET_CLIENT_ID, daemon_endpoint};
use crate::transport::unix::mux_write::{
    MuxWriteState, UnixInbound, UnixInboundError, flush_pending_responses, flush_unix_mux_writes,
    read_async_inbound, write_async_server_frame,
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

    /// Send one request over this persistent connection and wait for its response.
    pub fn request(&mut self, request: &DaemonRequest) -> DaemonTransportResult<DaemonResponse> {
        self.inner
            .request(request)
            .map_err(DaemonTransportError::from)
    }

    /// Write one opaque terminal input frame for one route on this muxed connection.
    pub fn send_terminal_frame(
        &mut self,
        route: &str,
        generation: u64,
        stream_epoch: u32,
        body: &[u8],
    ) -> DaemonTransportResult<()> {
        self.inner
            .send_terminal_frame(route, generation, stream_epoch, body)
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

/// One request whose owner reply arrived, ready to be written as a correlated response.
struct CompletedRequest {
    request_id: String,
    request: DaemonRequest,
    response: DaemonTransportResult<DaemonResponse>,
    response_delivery_tx: Option<mpsc::Sender<()>>,
    close_after: bool,
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
    let hello = match read_async_inbound(&mut reader, Some(DAEMON_HANDSHAKE_TIMEOUT)).await {
        Ok(UnixInbound::Hello(hello)) => hello,
        Ok(UnixInbound::Request { .. } | UnixInbound::Terminal(_)) => {
            return close_with_protocol_error(
                &mut write_half,
                &mut cleanup,
                None,
                DaemonProtocolErrorCode::HandshakeOrder,
            )
            .await;
        }
        Err(UnixInboundError::Transport(ClientDaemonTransportError::ClientDisconnected)) => {
            cleanup.set_reason(ConnectionTerminalReason::Eof);
            return Ok(());
        }
        Err(UnixInboundError::Protocol(code)) => {
            return close_with_protocol_error(&mut write_half, &mut cleanup, None, code).await;
        }
        Err(UnixInboundError::Transport(error)) => return Err(error.into()),
    };
    if hello.protocol != PROTOCOL {
        return Err(DaemonTransportError::Protocol("unexpected hello protocol"));
    }
    let (admission, hello_ack) = unix_hello_admission(&hello);
    if let Err(error) =
        write_async_server_frame(&mut write_half, &ServerFrame::HelloAck { ack: hello_ack }).await
    {
        cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
        return Err(error);
    }
    if hello.compatibility.protocol_version != PROTOCOL_VERSION {
        // The ack carries the running descriptor; the client reports the
        // mismatch. There is no negotiation and no request service.
        cleanup.set_reason(ConnectionTerminalReason::NormalClose);
        return Ok(());
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

    let (entity_tx, mut entity_rx) = tokio_mpsc::channel(UNIX_CONNECTION_ENTITY_QUEUE_CAPACITY);
    let mut in_flight: JoinSet<CompletedRequest> = JoinSet::new();
    let mut last_request_id: u64 = 0;

    loop {
        let inbound = {
            let inbound = read_async_inbound(&mut reader, None);
            tokio::pin!(inbound);
            loop {
                let event_mailbox = event_plane.mailbox(&client_id);
                let event_output_ready = event_mailbox
                    .as_ref()
                    .is_some_and(|mailbox| mailbox.has_ready_event());
                tokio::select! {
                    biased;
                    inbound = &mut inbound => break inbound,
                    completed = in_flight.join_next(), if !in_flight.is_empty() => {
                        let Some(Ok(completed)) = completed else {
                            cleanup.set_reason(ConnectionTerminalReason::Protocol);
                            return Err(DaemonTransportError::ControlThreadStopped);
                        };
                        if let Some(close) = deliver_completed_request(
                            &mut write_half,
                            &mux,
                            &mut mux_write,
                            &mut cleanup,
                            &control_tx,
                            event_mailbox.as_deref(),
                            completed,
                        )
                        .await?
                        {
                            cleanup.set_reason(close);
                            return Ok(());
                        }
                    }
                    entity = entity_rx.recv() => {
                        if let Some(entity) = entity {
                            mux_write.enqueue_entity_frame(entity)?;
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
                    }
                    _ = mux.wait_for_write() => {
                        for _ in 0..16 {
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
                            if mux_write.has_pending() || !mux.has_unsent_mux_writes() {
                                break;
                            }
                        }
                    }
                    _ = async {
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
                    _ = tokio::time::sleep(Duration::from_millis(25)), if mux.has_unsent_mux_writes() || mux_write.has_pending() || event_output_ready => {
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
        let (request_id, request) = match inbound {
            Ok(UnixInbound::Request {
                request_id,
                request,
            }) => (request_id, request),
            Ok(UnixInbound::Hello(_)) => {
                return close_with_protocol_error(
                    &mut write_half,
                    &mut cleanup,
                    Some(&mux),
                    DaemonProtocolErrorCode::HandshakeOrder,
                )
                .await;
            }
            Ok(UnixInbound::Terminal(frame)) => {
                if let Some(handle) = mux.live_handle_for_route(&frame.route, frame.generation)
                    && handle.push_ingress(frame.body).is_err()
                {
                    handle.close();
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
            Err(UnixInboundError::Transport(ClientDaemonTransportError::ClientDisconnected)) => {
                cleanup.set_reason(ConnectionTerminalReason::Eof);
                return Ok(());
            }
            Err(UnixInboundError::Protocol(code)) => {
                return close_with_protocol_error(&mut write_half, &mut cleanup, Some(&mux), code)
                    .await;
            }
            Err(UnixInboundError::Transport(error)) => {
                cleanup.set_reason(ConnectionTerminalReason::Protocol);
                return Err(error.into());
            }
        };
        let Some(parsed_id) = parse_request_id(&request_id) else {
            return close_with_protocol_error(
                &mut write_half,
                &mut cleanup,
                Some(&mux),
                DaemonProtocolErrorCode::InvalidRequestId,
            )
            .await;
        };
        if parsed_id <= last_request_id {
            return close_with_protocol_error(
                &mut write_half,
                &mut cleanup,
                Some(&mux),
                DaemonProtocolErrorCode::NonincreasingRequestId,
            )
            .await;
        }
        last_request_id = parsed_id;
        if in_flight.len() >= MAX_OUTSTANDING_REQUESTS {
            let response = too_many_requests_response(&request_id, &request);
            mux_write.enqueue_response(&request_id, &response, None, false)?;
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
        let (reply_tx, reply_rx) = oneshot::channel();
        let close_after = matches!(request, DaemonRequest::DaemonShutdown);
        let requires_delivery_ack =
            close_after || matches!(request, DaemonRequest::StartHubUpdate { .. });
        let (response_delivery_tx, response_delivery_rx) = if requires_delivery_ack {
            let (tx, rx) = mpsc::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let sent = match request.clone() {
            DaemonRequest::SubscribeEntities {
                entity_type,
                subscription_id,
            } => {
                control_tx
                    .send(ControlMessage::SubscribeEntities {
                        entity_type,
                        subscription_id,
                        frame_tx: EntityFrameSender::Async(entity_tx.clone()),
                        frame_rx: None,
                        reply_tx,
                        grant_id: None,
                    })
                    .await
            }
            DaemonRequest::UnsubscribeEntities { subscription_id } => {
                control_tx
                    .send(ControlMessage::UnsubscribeEntities {
                        subscription_id,
                        reply_tx: Some(reply_tx),
                        grant_id: None,
                    })
                    .await
            }
            request => {
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
            }
        };
        sent.map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        in_flight.spawn(async move {
            let response = receive_control_response(reply_rx).await;
            CompletedRequest {
                request_id,
                request,
                response,
                response_delivery_tx,
                close_after,
            }
        });
    }
}

/// Write one completed request as a correlated response.
///
/// Returns the terminal reason when the response closes the connection.
async fn deliver_completed_request(
    write_half: &mut tokio::net::unix::OwnedWriteHalf,
    mux: &UnixConnectionMux,
    mux_write: &mut MuxWriteState,
    cleanup: &mut ConnectionCleanupGuard,
    control_tx: &ControlSender,
    event_mailbox: Option<&crate::subscription::package_events::ClientEventMailbox>,
    completed: CompletedRequest,
) -> DaemonTransportResult<Option<ConnectionTerminalReason>> {
    let response = completed.response?;
    cleanup.apply_subscription_change(attached_subscription_change_for_response(
        &completed.request,
        &response,
    ));
    match (&completed.request, response.kind) {
        (
            DaemonRequest::SubscribeEntities {
                subscription_id, ..
            },
            DaemonResponseKind::EntitySubscribed,
        ) => cleanup.add_entity_subscription(subscription_id.clone()),
        (
            DaemonRequest::UnsubscribeEntities { subscription_id },
            DaemonResponseKind::EntityUnsubscribed,
        ) => cleanup.remove_entity_subscription(subscription_id),
        _ => {}
    }
    mux_write.enqueue_response(
        &completed.request_id,
        &response,
        completed.response_delivery_tx,
        completed.close_after,
    )?;
    if let Err(error) =
        flush_pending_responses(write_half, mux, mux_write, Instant::now(), event_mailbox).await
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
    if let Err(error) = flush_unix_mux_writes(write_half, mux, mux_write, event_mailbox).await {
        cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
        mux.close_all();
        return Err(error);
    }
    if completed.close_after {
        debug_assert!(!mux_write.has_close_after_pending());
        return Ok(Some(ConnectionTerminalReason::NormalClose));
    }
    Ok(None)
}

fn too_many_requests_response(request_id: &str, request: &DaemonRequest) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: OPERATOR_ERROR_TOO_MANY_REQUESTS.to_string(),
        request_id: request_id.to_string(),
        operation: control_request_operation_label(request).to_string(),
        message: format!(
            "connection already holds {MAX_OUTSTANDING_REQUESTS} outstanding requests"
        ),
        diagnostics: Vec::new(),
    });
    response
}

/// Send a typed close reason when the socket still accepts a write, then fail.
async fn close_with_protocol_error(
    write_half: &mut tokio::net::unix::OwnedWriteHalf,
    cleanup: &mut ConnectionCleanupGuard,
    mux: Option<&UnixConnectionMux>,
    code: DaemonProtocolErrorCode,
) -> DaemonTransportResult<()> {
    let _ = write_async_server_frame(
        write_half,
        &ServerFrame::Close {
            reason: DaemonCloseReason::ProtocolError { code },
        },
    )
    .await;
    if let Some(mux) = mux {
        mux.close_all();
    }
    cleanup.set_reason(ConnectionTerminalReason::Protocol);
    Err(DaemonTransportError::Protocol(code.as_str()))
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
    Shutdown,
    NormalClose,
}

impl ConnectionTerminalReason {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Eof => "eof",
            Self::Protocol => "protocol",
            Self::WriteFailure => "write_failure",
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
    _control_tx: ControlSender,
    cleanup: ConnectionCleanup,
) {
    state.lifecycle_counters.live_connections =
        state.lifecycle_counters.live_connections.saturating_sub(1);
    *state
        .lifecycle_counters
        .cleanup_by_reason
        .entry(cleanup.reason.label().to_string())
        .or_default() += 1;

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
    let mut candidates = BTreeSet::new();
    for claim in state
        .pending_runtime
        .take_connection_bound_routes(&cleanup.client_id)
    {
        candidates.insert((claim.session_id, claim.subscription_id));
    }
    // Attach streams this client started but never bound are cancelled now,
    // so a late attach or bind continuation finds an identity mismatch and
    // releases its own generation. Core-side membership for those routes is
    // still detached by the Core turn below.
    for (session_id, subscription_id, identity) in state
        .pending_runtime
        .unbound_routes_for_client(&cleanup.client_id)
    {
        let _ = state
            .pending_runtime
            .cancel_stream_if(&session_id, &subscription_id, &identity);
        candidates.insert((session_id, subscription_id));
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
        runtime.release_owner_captures(botster_core_daemon::CaptureOwner(format!(
            "client:{}",
            cleanup.client_id
        )));
    }
    for subscription in &cleanup.attached_subscriptions {
        candidates.insert((
            subscription.session_id.clone(),
            subscription.subscription_id.clone(),
        ));
    }
    if let Some(UnixTerminalAdmission::Admitted { mux, .. }) = unix_admission {
        mux.close_all();
    }
    if candidates.is_empty() {
        state.lifecycle_counters.cleanup_completed =
            state.lifecycle_counters.cleanup_completed.saturating_add(1);
        return;
    }
    let Some(runtime) = daemon.runtime() else {
        state.lifecycle_counters.cleanup_completed =
            state.lifecycle_counters.cleanup_completed.saturating_add(1);
        return;
    };
    // Detach every route this connection owned in one Core owner turn, then
    // finish the owner bookkeeping when the turn reports back.
    // Capture the attachment identity of every candidate now. The owner-side
    // bookkeeping after the Core turn only mutates a stream that still has
    // this identity; a replacement attached meanwhile is left alone.
    let client_id = cleanup.client_id.clone();
    let owners: Vec<CleanupCandidate> = candidates
        .iter()
        .map(|(session_id, subscription_id)| {
            (
                session_id.clone(),
                subscription_id.clone(),
                state
                    .pending_runtime
                    .stream_identity(session_id, subscription_id),
            )
        })
        .collect();
    let now = tick(&mut state.logical_clock);
    let mut ticket =
        Some(runtime.submit_core(cleanup_core_turn(owners.clone(), client_id.clone(), now)));
    state
        .pending_owner_work
        .push(Box::new(move |daemon, state| {
            use crate::data_plane::driver::CoreTicketPoll;
            let Some(live_ticket) = ticket.as_mut() else {
                // The previous admission was refused; one ticket in flight.
                let Some(runtime) = daemon.runtime() else {
                    return true;
                };
                ticket = Some(runtime.submit_core(cleanup_core_turn(
                    owners.clone(),
                    client_id.clone(),
                    now,
                )));
                return false;
            };
            let outcomes = match live_ticket.poll() {
                CoreTicketPoll::Pending => return false,
                CoreTicketPoll::Refused => {
                    ticket = None;
                    return false;
                }
                CoreTicketPoll::Lost => Vec::new(),
                CoreTicketPoll::Ready(outcomes) => outcomes,
            };
            let mut failed = false;
            let mut bound_closes = 0u64;
            for (session_id, subscription_id, identity, outcome) in outcomes {
                match outcome {
                    CleanupRouteOutcome::Foreign => continue,
                    CleanupRouteOutcome::NoGeneration => {
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
                        continue;
                    }
                    CleanupRouteOutcome::DetachFailed => {
                        failed = true;
                        continue;
                    }
                    CleanupRouteOutcome::Detached => {}
                }
                *state
                    .lifecycle_counters
                    .cleanup_by_reason
                    .entry("cleanup_generation_detach".to_string())
                    .or_insert(0) += 1;
                // Mutate only the stream captured at cleanup start. A replacement
                // that attached during the Core turn keeps its adapter.
                let owned_stream = identity.as_ref().is_some_and(|identity| {
                    state
                        .pending_runtime
                        .stream_matches(&session_id, &subscription_id, identity)
                });
                if owned_stream {
                    let identity = identity.as_ref().expect("checked above");
                    let was_bound = state
                        .pending_runtime
                        .is_adapter_bound(&session_id, &subscription_id);
                    let _ = state.pending_runtime.close_adapter_if(
                        &session_id,
                        &subscription_id,
                        identity,
                    );
                    let _ = state.pending_runtime.cancel_stream_if(
                        &session_id,
                        &subscription_id,
                        identity,
                    );
                    if was_bound {
                        bound_closes += 1;
                    }
                } else if state
                    .pending_runtime
                    .stream_identity(&session_id, &subscription_id)
                    .is_some()
                {
                    // A replacement owns the route key now; its live attach
                    // record stays untouched.
                    continue;
                }
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
            true
        }));
}

/// One Core owner turn that detaches every candidate route the closed
/// connection owned. Built as a value so a refused admission can resubmit.
fn cleanup_core_turn(
    owners: Vec<CleanupCandidate>,
    client_id: String,
    now: u64,
) -> impl FnOnce(&mut botster_core_daemon::CoreDaemon) -> Vec<CleanupRouteReport> + Send + 'static {
    move |daemon| {
        let inventory = daemon.list_terminal_subscriptions();
        let mut outcomes = Vec::with_capacity(owners.len());
        for (session_id, subscription_id, identity) in owners {
            let generation =
                live_generation_for_route(&inventory, &client_id, &session_id, &subscription_id);
            let foreign_core_owner = inventory.iter().any(|row| {
                row.session_id.0 == session_id
                    && row.subscription_id.0 == subscription_id
                    && row.client_id.0 != client_id
            });
            let foreign_stream_owner = identity
                .as_ref()
                .is_some_and(|identity| identity.client_id != client_id);
            let outcome = match generation {
                None if foreign_core_owner || foreign_stream_owner => CleanupRouteOutcome::Foreign,
                None => CleanupRouteOutcome::NoGeneration,
                Some(generation) => match daemon.detach_terminal_subscription(
                    ClientId(client_id.clone()),
                    SessionId(session_id.clone()),
                    SubscriptionId(subscription_id.clone()),
                    generation,
                    now,
                ) {
                    Ok(
                        DetachTerminalSubscriptionResult::Detached { .. }
                        | DetachTerminalSubscriptionResult::AlreadyGone
                        | DetachTerminalSubscriptionResult::GenerationMismatch { .. },
                    ) => CleanupRouteOutcome::Detached,
                    Err(_) => CleanupRouteOutcome::DetachFailed,
                },
            };
            outcomes.push((session_id, subscription_id, identity, outcome));
        }
        outcomes
    }
}

/// One route the closed connection may own, with the attachment identity
/// captured when cleanup started.
type CleanupCandidate = (String, String, Option<AttachmentIdentity>);

/// One candidate with its Core-turn outcome.
type CleanupRouteReport = (
    String,
    String,
    Option<AttachmentIdentity>,
    CleanupRouteOutcome,
);

/// Per-route result of one connection cleanup turn on the Core owner thread.
enum CleanupRouteOutcome {
    /// Another client or stream owns the route; leave it alone.
    Foreign,
    /// No live generation existed; record the detach without Core work.
    NoGeneration,
    Detached,
    DetachFailed,
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

#[allow(dead_code)]
const _: usize = DAEMON_MAX_CONNECTIONS;
