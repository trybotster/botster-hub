//! Unix accepted-connection driver and client connection role.
//!
//! One task serves one muxed connection under host-control protocol 13: Hello
//! first, then correlated requests that may complete out of order, entity
//! subscription frames, host events, and content-blind terminal containers.
use std::collections::BTreeSet;
use std::io::Write;
#[cfg(test)]
use std::os::unix::net::UnixStream;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{SessionId, SubscriptionId};
use botster_core_daemon::CaptureOwner;
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
    ControlMessage, ControlReplyReceiver, ControlSender, control_reply_channel, egress_write_class,
};
use crate::daemon::control::pending::retire_abandoned_requests;
use crate::daemon::control::reply::ControlReply;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_budget::OwnerPermit;
use crate::daemon::owner_loop::{DaemonControlState, tick};
use crate::subscription::attach_routes::{
    AttachStreamOwner, AttachedSubscription, AttachedSubscriptionChange, EntitySubscriptionChange,
    RequestCompletionProjection, apply_attached_subscription_change,
};
use crate::subscription::entity::EntityFrameSender;
use crate::subscription::route_cleanup::{
    CleanupCandidate, candidate_for_departing_owner, retain_route_cleanup,
};
use crate::transport::shared::ingress::IngressStore;
use crate::transport::unix::UnixConnectionMux;
use crate::transport::unix::adapter::UnixTerminalAdapterHandle;
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

/// Doctor-only Status/handshake path. Existing `request` stays blocking.
///
/// Passes Hub's existing 2 s write and handshake constants into the client.
/// Does not bound AF_UNIX `connect`.
pub fn request_for_doctor(
    config: &HubConfig,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    let endpoint = daemon_endpoint(config)?;
    botster_hub_client::request_with_handshake_deadlines(
        &endpoint,
        request,
        &botster_hub_client::DaemonCompatibilityRequirement::current(),
        Some(DAEMON_CLIENT_WRITE_TIMEOUT),
        Some(DAEMON_HANDSHAKE_TIMEOUT),
    )
    .map_err(DaemonTransportError::from)
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
    projection: RequestCompletionProjection,
    response: ControlReply,
    response_delivery_tx: Option<mpsc::Sender<()>>,
    close_after: bool,
}

pub(crate) async fn handle_connection_async(
    stream: TokioUnixStream,
    control_tx: ControlSender,
    entity_capacity_wake: crate::subscription::entity::EntitySubscriptionCapacityWake,
    cleanup_permit: tokio_mpsc::OwnedPermit<ControlMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
    permit: OwnerPermit,
) -> DaemonTransportResult<()> {
    let client_id = format!(
        "botster-hub-daemon-socket-{}",
        NEXT_SOCKET_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
    );
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = AsyncBufReader::new(read_half);
    let mut cleanup = ConnectionCleanupGuard::new(
        cleanup_permit,
        client_id.clone(),
        ConnectionTerminalReason::Protocol,
        permit,
    );
    let hello = match read_async_inbound(&mut reader, Some(DAEMON_HANDSHAKE_TIMEOUT)).await {
        Ok(UnixInbound::Hello(hello)) => hello,
        Ok(UnixInbound::Request { .. } | UnixInbound::Terminal(_) | UnixInbound::Grant { .. }) => {
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
    let event_reader =
        std::sync::Arc::new(crate::subscription::package_events::ClientEventReader::default());
    let (admission_ack_tx, admission_ack_rx) = oneshot::channel();
    control_tx
        .send(ControlMessage::RegisterUnixAdmission {
            event_reader: std::sync::Arc::clone(&event_reader),
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
                let event_mailbox = event_reader.mailbox();
                let event_output_ready = event_mailbox
                    .as_ref()
                    .is_some_and(|mailbox| mailbox.has_ready_event());
                #[cfg(test)]
                if !event_output_ready {
                    event_reader.test_after_empty_read();
                }
                tokio::select! {
                    biased;
                    // S13: the reader never pauses. Input credit bounds
                    // each route's ingress, so control frames and output
                    // grants always get through.
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
                            entity_capacity_wake.publish();
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
                    _ = event_reader.wait() => {
                        let event_mailbox = event_reader.mailbox();
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
                        #[cfg(test)]
                        event_reader.test_note_timer();
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
                    && store_credited_input(&handle, frame.body) == InputCredit::Exceeded
                {
                    return close_with_protocol_error(
                        &mut write_half,
                        &mut cleanup,
                        Some(&mux),
                        DaemonProtocolErrorCode::InputCreditExceeded,
                    )
                    .await;
                }
                mux.clear_deferred_flushes();
                if let Err(error) = flush_unix_mux_writes(
                    &mut write_half,
                    &mux,
                    &mut mux_write,
                    event_reader.mailbox().as_deref(),
                )
                .await
                {
                    cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                    mux.close_all();
                    return Err(error);
                }
                continue;
            }
            Ok(UnixInbound::Grant {
                route,
                generation,
                items,
                bytes,
            }) => {
                // A grant for a retired or unknown generation is stale: the
                // client releases it at that generation's `CLOSED`.
                if let Some(handle) = mux.live_handle_for_route(&route, generation) {
                    handle.grant(items, bytes);
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
            mux_write.enqueue_response(&request_id, response, None, false)?;
            if let Err(error) = flush_pending_responses(
                &mut write_half,
                &mux,
                &mut mux_write,
                Instant::now(),
                event_reader.mailbox().as_deref(),
            )
            .await
            {
                cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
                mux.close_all();
                return Err(error);
            }
            continue;
        }
        let (reply_tx, reply_rx) = control_reply_channel();
        let close_after = matches!(request, DaemonRequest::DaemonShutdown);
        let projection = RequestCompletionProjection::from_request(&request);
        let requires_delivery_ack =
            close_after || matches!(request, DaemonRequest::StartHubUpdate { .. });
        let (response_delivery_tx, response_delivery_rx) = if requires_delivery_ack {
            let (tx, rx) = mpsc::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let sent = match request {
            DaemonRequest::SubscribeEntities {
                entity_type,
                subscription_id,
            } => {
                control_tx
                    .send(ControlMessage::SubscribeEntities {
                        entity_type,
                        subscription_id,
                        transport_request_id: Some(request_id.clone()),
                        client_id: Some(client_id.clone()),
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
                        transport_request_id: Some(request_id.clone()),
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
                projection,
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
    match completed.response {
        ControlReply::Typed {
            response,
            charge,
            delivery,
        } => {
            let response = (*response)?;
            // S13: the route's credit frames wait for this response.
            let attached_route = response.terminal_attach.as_ref().and_then(|attach| {
                mux.live_handle_for_route(&attach.subscription_id, attach.generation)
            });
            cleanup.apply_subscription_change(
                completed.projection.attached_subscription_change(&response),
            );
            match completed.projection.entity_subscription_change(&response) {
                Some(EntitySubscriptionChange::Subscribe(subscription_id)) => {
                    cleanup.add_entity_subscription(subscription_id)
                }
                Some(EntitySubscriptionChange::Unsubscribe(subscription_id)) => {
                    cleanup.remove_entity_subscription(&subscription_id)
                }
                None => {}
            }
            mux_write.enqueue_response_with_receipt(
                &completed.request_id,
                response,
                completed.response_delivery_tx,
                completed.close_after,
                delivery,
            )?;
            if let Some(route) = attached_route {
                mux_write.tag_last_response_attach(route);
            }
            drop(charge);
        }
        ControlReply::EncodedPlugin {
            encoded_frame,
            charge,
            ..
        } => {
            let queued = mux_write.enqueue_encoded_response(
                &encoded_frame,
                completed.response_delivery_tx,
                completed.close_after,
            );
            drop(encoded_frame);
            drop(charge);
            queued?;
        }
    }
    let delivery_kind = crate::daemon::control::message::DaemonDeliveryKind::Control;
    if let Err(error) =
        flush_pending_responses(write_half, mux, mux_write, Instant::now(), event_mailbox).await
    {
        cleanup.set_reason(ConnectionTerminalReason::WriteFailure);
        let _ = control_tx.try_send(ControlMessage::EgressWriteFailed {
            delivery_kind,
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

pub(crate) async fn receive_control_response(reply_rx: ControlReplyReceiver) -> ControlReply {
    reply_rx
        .await
        .unwrap_or_else(|_| ControlReply::plain(Err(DaemonTransportError::ControlThreadStopped)))
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
    /// The owner budget permit reserved when the connection was accepted.
    /// It returns to the owner with this message and carries the cleanup.
    permit: OwnerPermit,
}

pub(crate) struct ConnectionCleanupGuard {
    cleanup_permit: Option<tokio_mpsc::OwnedPermit<ControlMessage>>,
    cleanup: Option<ConnectionCleanup>,
}

impl ConnectionCleanupGuard {
    pub(crate) fn new(
        cleanup_permit: tokio_mpsc::OwnedPermit<ControlMessage>,
        client_id: String,
        reason: ConnectionTerminalReason,
        permit: OwnerPermit,
    ) -> Self {
        Self {
            cleanup_permit: Some(cleanup_permit),
            cleanup: Some(ConnectionCleanup {
                client_id,
                attached_subscriptions: Vec::new(),
                entity_subscription_ids: BTreeSet::new(),
                reason,
                permit,
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
        if let (Some(cleanup), Some(permit)) = (self.cleanup.take(), self.cleanup_permit.take()) {
            permit.send(ControlMessage::ConnectionCleanup(cleanup));
        }
    }
}

pub(crate) fn reap_finished_connection_tasks(tasks: &mut Vec<JoinHandle<()>>) {
    tasks.retain(|task| !task.is_finished());
}

pub(crate) fn wait_for_connection_tasks(
    runtime: &tokio::runtime::Runtime,
    tasks: &mut Vec<JoinHandle<()>>,
    control_rx: &mut tokio_mpsc::Receiver<ControlMessage>,
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    control_tx: ControlSender,
) {
    let deadline = Instant::now() + DAEMON_CLIENT_WRITE_TIMEOUT;
    while !tasks.iter().all(JoinHandle::is_finished) && Instant::now() < deadline {
        drain_shutdown_cleanups(control_rx, daemon, state, control_tx.clone());
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
    drain_shutdown_cleanups(control_rx, daemon, state, control_tx);
}

fn drain_shutdown_cleanups(
    control_rx: &mut tokio_mpsc::Receiver<ControlMessage>,
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    control_tx: ControlSender,
) {
    while let Ok(message) = control_rx.try_recv() {
        if let ControlMessage::ConnectionCleanup(cleanup) = message {
            handle_connection_cleanup(daemon, state, control_tx.clone(), cleanup);
        }
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
    // Every route this connection may own in Core, captured with the stream
    // identity and generation before any owner-side mutation. Attach admission
    // bounds the count per owner, so this vector is bounded.
    let mut keys = BTreeSet::new();
    for claim in state
        .pending_runtime
        .take_connection_bound_routes(&cleanup.client_id)
    {
        keys.insert((claim.session_id, claim.subscription_id));
    }
    for subscription in &cleanup.attached_subscriptions {
        keys.insert((
            subscription.session_id.clone(),
            subscription.subscription_id.clone(),
        ));
    }
    let unbound = state
        .pending_runtime
        .unbound_routes_for_client(&cleanup.client_id);
    for (session_id, subscription_id, _) in &unbound {
        keys.insert((session_id.clone(), subscription_id.clone()));
    }
    keys.extend(state.pending_runtime.take_owner_routes(&cleanup.client_id));
    // The departing owner is this connection; a route whose current stream
    // belongs to a replacement is targeted only under this client's own
    // Core id, never through the replacement's identity or generation.
    let departing = AttachStreamOwner {
        client_id: cleanup.client_id.clone(),
        grant_id: None,
    };
    let candidates: Vec<CleanupCandidate> = keys
        .iter()
        .filter_map(|(session_id, subscription_id)| {
            candidate_for_departing_owner(
                &state.pending_runtime,
                &departing,
                &cleanup.client_id,
                session_id,
                subscription_id,
            )
        })
        .collect();
    // Attach streams this client started but never bound are cancelled now,
    // so a late attach continuation finds an identity mismatch and releases
    // its own generation. Core-side membership is detached by the obligation.
    for (session_id, subscription_id, identity) in unbound {
        let _ = state
            .pending_runtime
            .cancel_stream_if(&session_id, &subscription_id, &identity);
    }
    state
        .pending_runtime
        .admission
        .host_compatibility
        .remove(&cleanup.client_id);
    crate::daemon::client_events::close_connection(state, &cleanup.client_id);
    if let Some(UnixTerminalAdmission::Admitted { mux, .. }) = unix_admission {
        mux.close_all();
    }
    // Reads this client left pending are retired; requests that must finish
    // keep their permits and run to completion.
    crate::daemon::control::entities::retire_plugin_entity_connection(
        daemon,
        state,
        &cleanup.client_id,
    );
    retire_abandoned_requests(daemon, state, &cleanup.client_id);
    // The permit reserved when the connection was accepted now carries the
    // cleanup obligation: captures released and every route detached in
    // Core, then identity-fenced owner bookkeeping.
    let permit = cleanup.permit;
    if daemon.runtime().is_none() {
        state.budget.release(permit);
        state.lifecycle_counters.cleanup_completed =
            state.lifecycle_counters.cleanup_completed.saturating_add(1);
        return;
    }
    let now = tick(&mut state.logical_clock);
    let capture_owner = CaptureOwner(format!("client:{}", cleanup.client_id));
    retain_route_cleanup(
        state,
        permit,
        "unix_connection_cleanup",
        Some(capture_owner),
        Some(cleanup.client_id),
        candidates,
        now,
        |state, applied| {
            if applied.bound_closes > 0 {
                *state
                    .lifecycle_counters
                    .cleanup_by_reason
                    .entry("bound_adapter_close".to_string())
                    .or_insert(0) += applied.bound_closes;
            }
            if applied.failed {
                // `connection_cleanup_ignores_only_an_already_removed_session`
                // is the designated positive control for predicate-true
                // cleanup failures.
                state.lifecycle_counters.cleanup_failed =
                    state.lifecycle_counters.cleanup_failed.saturating_add(1);
            } else {
                state.lifecycle_counters.cleanup_completed =
                    state.lifecycle_counters.cleanup_completed.saturating_add(1);
            }
        },
    );
}

#[cfg(test)]
pub(crate) fn handle_connection(
    stream: UnixStream,
    control_tx: ControlSender,
) -> DaemonTransportResult<()> {
    stream
        .set_nonblocking(true)
        .map_err(DaemonTransportError::Io)?;
    let (cleanup_control_tx, mut cleanup_control_rx) = tokio_mpsc::channel(1);
    let cleanup_permit = cleanup_control_tx
        .try_reserve_owned()
        .expect("cleanup channel capacity");
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(DaemonTransportError::Io)?;
    let stream = {
        let _runtime = runtime.enter();
        TokioUnixStream::from_std(stream).map_err(DaemonTransportError::Io)?
    };
    let permit = crate::daemon::owner_budget::OwnerBudget::with_capacity(1)
        .reserve_connection()
        .expect("test permit");
    let result = runtime.block_on(handle_connection_async(
        stream,
        control_tx,
        crate::subscription::entity::EntitySubscriptionCapacityWake::default(),
        cleanup_permit,
        shutdown_rx,
        permit,
    ));
    let _ = cleanup_control_rx.try_recv();
    result
}

#[allow(dead_code)]
const _: usize = DAEMON_MAX_CONNECTIONS;

#[derive(Debug, PartialEq, Eq)]
enum InputCredit {
    Within,
    Exceeded,
}

/// Store one input frame. The route's input credit equals its ingress
/// capacity, so a compliant client never meets a full ingress: a full
/// ingress, or input before the route's `TerminalAttached` response is on
/// the socket, exceeds the client's credit.
fn store_credited_input(handle: &UnixTerminalAdapterHandle, bytes: Vec<u8>) -> InputCredit {
    if !handle.is_attach_written()
        || !handle.reserve_input_credit(
            u32::try_from(
                botster_core::contract::terminal_adapter::MIN_ADAPTER_INGRESS_BUFFER_FRAMES,
            )
            .expect("the ingress capacity fits u32"),
        )
    {
        return InputCredit::Exceeded;
    }
    match handle.try_push_ingress(bytes) {
        IngressStore::Full(_) => InputCredit::Exceeded,
        IngressStore::Stored | IngressStore::Closed | IngressStore::Malformed => {
            InputCredit::Within
        }
    }
}

#[cfg(test)]
mod input_backpressure_tests {
    use super::*;
    use crate::admission::budgets::DAEMON_CONTROL_QUEUE_CAPACITY;
    use crate::admission::unix_hello::UnixTerminalAdmission;
    use crate::client_api_dto::response::daemon_response_base;
    use crate::daemon::control::ControlMessage;
    use crate::transport::unix::test_harness::{
        read_hello_ack, read_response, receive_test_control_message, write_hello, write_request,
    };
    use botster_core::contract::terminal_adapter::{
        MIN_ADAPTER_INGRESS_BUFFER_FRAMES, TerminalAdapter, TerminalIngress,
    };
    use botster_core::contract::terminal_wake::{TerminalWakeSource, WakingTerminalAdapter};
    use botster_core::{SessionId, SubscriptionId, TerminalSubscriptionGeneration};
    use botster_hub_client::{
        DaemonCloseReason, DaemonRequest, DaemonResponseKind, DaemonUnixCreditFrame,
        DaemonUnixFrameReader, DaemonUnixMuxFrame, ServerFrame, write_unix_terminal_frame,
    };
    use std::net::Shutdown;

    fn input_frame(index: usize) -> Vec<u8> {
        use botster_terminal_protocol::{
            INPUT_HEADER_BYTES, TERMINAL_INPUT_SCHEME_VERSION, TerminalInputKind,
        };
        let data = format!("paste-{index:03}");
        let len = u16::try_from(data.len()).expect("fixture body fits");
        let mut bytes = Vec::with_capacity(INPUT_HEADER_BYTES + data.len());
        bytes.push(TERMINAL_INPUT_SCHEME_VERSION);
        bytes.push(TerminalInputKind::RawBytes.as_byte());
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(&(index as u64 + 1).to_be_bytes());
        bytes.extend_from_slice(data.as_bytes());
        bytes
    }

    struct CreditConnection {
        client: UnixStream,
        reader: DaemonUnixFrameReader,
        control_rx: tokio_mpsc::Receiver<ControlMessage>,
        connection: thread::JoinHandle<DaemonTransportResult<()>>,
        adapter: crate::transport::unix::UnixTerminalAdapter,
        handle: UnixTerminalAdapterHandle,
        wakes: TerminalWakeSource,
    }

    /// An admitted connection with one registered route, "paste" at
    /// generation 1. `attach_written` stands in for the writer finishing the
    /// route's `TerminalAttached` response.
    fn credit_connection(attach_written: bool) -> CreditConnection {
        let (server, mut client) = UnixStream::pair().expect("create daemon socket pair");
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
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
        assert!(mux.register("s".into(), "paste".into(), 1, handle.clone()));
        let wakes = TerminalWakeSource::new();
        adapter.set_wake_sink(wakes.bind_route(
            SessionId("s".into()),
            SubscriptionId("paste".into()),
            TerminalSubscriptionGeneration(1),
        ));
        if attach_written {
            handle.mark_attach_written();
        }
        reply_tx.send(()).expect("ack unix admission");
        CreditConnection {
            client,
            reader,
            control_rx,
            connection,
            adapter,
            handle,
            wakes,
        }
    }

    fn assert_closed_for_input_credit(mut connection: CreditConnection) {
        match connection
            .reader
            .read_frame(&mut connection.client)
            .expect("read the typed close")
        {
            DaemonUnixMuxFrame::Server(ServerFrame::Close {
                reason: DaemonCloseReason::ProtocolError { code },
            }) => assert_eq!(code, DaemonProtocolErrorCode::InputCreditExceeded),
            other => panic!("expected the input_credit_exceeded close, got {other:?}"),
        }
        let result = connection
            .connection
            .join()
            .expect("join daemon connection");
        assert!(
            matches!(
                result,
                Err(DaemonTransportError::Protocol("input_credit_exceeded"))
            ),
            "the connection ends typed: {result:?}"
        );
        assert!(
            connection.handle.is_closed(),
            "the typed close ends the route"
        );
    }

    /// S13 (plan test 6): a client sends its whole input window while Core
    /// reads nothing. The reader never pauses: a Status request behind the
    /// window is read and answered while the ingress is full. Each frame Core
    /// then removes returns one unit of input credit.
    #[test]
    fn input_within_credit_never_pauses_the_reader_and_its_credit_returns() {
        let mut connection = credit_connection(true);
        let window = MIN_ADAPTER_INGRESS_BUFFER_FRAMES;
        for index in 0..window {
            write_unix_terminal_frame(&mut connection.client, "paste", 1, 0, &input_frame(index))
                .expect("write input frame");
        }
        write_request(&mut connection.client, 1, DaemonRequest::Status);
        let ControlMessage::Request {
            request, reply_tx, ..
        } = receive_test_control_message(&mut connection.control_rx)
        else {
            panic!("the request behind a full ingress is read");
        };
        assert!(matches!(*request, DaemonRequest::Status));
        reply_tx
            .send(Ok(daemon_response_base(DaemonResponseKind::Status)))
            .expect("reply to status");
        let response = read_response(&mut connection.client, &mut connection.reader, 1);
        assert_eq!(response.kind, DaemonResponseKind::Status);

        // timer: deadline — bounds a lost ingress wake.
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut delivered = Vec::new();
        while delivered.len() < window {
            assert!(
                Instant::now() < deadline,
                "input stalled at {}",
                delivered.len()
            );
            match connection.adapter.try_read() {
                TerminalIngress::Frame(frame) => delivered.push(frame),
                TerminalIngress::Empty => {
                    let _ = connection
                        .wakes
                        .wait_wakes(deadline.saturating_duration_since(Instant::now()));
                }
                other => panic!("input within credit must not end the route: {other:?}"),
            }
        }
        assert_eq!(
            delivered,
            (0..window).map(input_frame).collect::<Vec<_>>(),
            "every frame arrives once, in order"
        );
        let mut returned = 0_usize;
        while returned < window {
            match connection
                .reader
                .read_frame(&mut connection.client)
                .expect("read returned input credit")
            {
                DaemonUnixMuxFrame::Credit(DaemonUnixCreditFrame::InputCredit {
                    route,
                    generation,
                    items,
                }) => {
                    assert_eq!((route.as_str(), generation), ("paste", 1));
                    returned += usize::try_from(items).expect("items fit usize");
                }
                other => panic!("expected INPUT_CREDIT, got {other:?}"),
            }
        }
        assert_eq!(returned, window, "exactly the consumed frames return");
        assert!(!connection.handle.is_closed());
        connection
            .client
            .shutdown(Shutdown::Both)
            .expect("disconnect daemon client");
        connection
            .connection
            .join()
            .expect("join daemon connection")
            .expect("client disconnect is a clean connection close");
    }

    /// S13 (plan test 9): one input frame past the window, while Core reads
    /// nothing, closes the connection typed. The Hub neither buffers it nor
    /// stops reading.
    #[test]
    fn input_beyond_credit_closes_the_connection_typed() {
        let mut connection = credit_connection(true);
        for index in 0..=MIN_ADAPTER_INGRESS_BUFFER_FRAMES {
            write_unix_terminal_frame(&mut connection.client, "paste", 1, 0, &input_frame(index))
                .expect("write input frame");
        }
        assert_closed_for_input_credit(connection);
    }

    /// S13 (plan test 9): input before the route's `TerminalAttached`
    /// response is on the socket has no credit yet.
    #[test]
    fn input_before_the_attach_response_closes_the_connection_typed() {
        let mut connection = credit_connection(false);
        write_unix_terminal_frame(&mut connection.client, "paste", 1, 0, &input_frame(0))
            .expect("write input frame");
        assert_closed_for_input_credit(connection);
    }
}
