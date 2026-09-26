//! Local WebRTC control DataChannel driver.
//!
//! The control channel carries host-control protocol 10: encrypted JSON
//! [`ClientFrame`] messages in, chunked encrypted [`ServerFrame`] deliveries
//! out. Requests are correlated by `request_id`; Hub serves them in arrival
//! order and answers requests beyond the outstanding limit with a correlated
//! `too_many_requests` operator error. Protocol violations close the channel
//! after one `ServerFrame::Close` carrying the typed reason.
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use botster_core::{AesGcmEnvelope, AesGcmKey, decrypt_aes_gcm};
use botster_hub_client::{
    ClientFrame, DaemonCloseReason, DaemonCompatibility, DaemonDiagnostic, DaemonEvent,
    DaemonHello, DaemonHelloAck, DaemonLocalWebrtcDeliveryChunk, DaemonOperatorError,
    DaemonProtocolErrorCode, DaemonRequest, DaemonResponse, DaemonResponseKind,
    LOCAL_WEBRTC_MAX_FRAME_BYTES, MAX_CONTROL_REQUEST_BYTES, MAX_OUTSTANDING_REQUESTS,
    OPERATOR_ERROR_RUNTIME_REPLY_CLOSED, OPERATOR_ERROR_RUNTIME_REQUEST_FAILED,
    OPERATOR_ERROR_RUNTIME_REQUEST_TIMED_OUT, OPERATOR_ERROR_TOO_MANY_REQUESTS, PROTOCOL,
    PROTOCOL_VERSION, ServerFrame, parse_request_id,
};
use bytes::BytesMut;
use tokio::sync::{mpsc as tokio_mpsc, watch};
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::runtime::timeout;

use botster_terminal_protocol::{
    TerminalCompatibility, ensure_compatible as ensure_terminal_compatible,
};

use crate::admission::unix_hello::WebrtcTerminalAdmission;
use crate::client_api_dto::response::daemon_response_base;
use crate::daemon::control::control_request_operation_label;
use crate::daemon::control::message::{ControlMessage, ControlSender, control_reply_channel};
use crate::subscription::attach_routes::{
    EntitySubscriptionChange, RequestCompletionProjection,
    hello_requires_terminal_subscription_closed,
};
use crate::subscription::entity::EntityFrameSender;
use crate::transport::webrtc::adapter::WebRtcConnectionMux;
use crate::transport::webrtc::delivery::{
    LocalWebrtcSendFailure, framed_daemon_response, framed_encoded_daemon_response,
    framed_server_frame,
};
use crate::transport::webrtc::peer::{
    LOCAL_WEBRTC_PEER_CLOSE_BOUND, LocalWebrtcPeerState, LocalWebrtcTerminalCause, webrtc_runtime,
};

/// Requests held in the inbound queue while one request is in service.
pub(crate) const LOCAL_WEBRTC_PENDING_REQUESTS: usize = MAX_OUTSTANDING_REQUESTS;
/// Correlated `too_many_requests` rejections held beyond the request limit.
/// A client that exceeds this bound as well is closed as a flood.
pub(crate) const LOCAL_WEBRTC_PENDING_REJECTIONS: usize = MAX_OUTSTANDING_REQUESTS;
pub(crate) const LOCAL_WEBRTC_EVENT_PROBE: Duration = Duration::ZERO;
pub(crate) const LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW: u32 = LOCAL_WEBRTC_MAX_FRAME_BYTES as u32;
pub(crate) const LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH: u32 = (LOCAL_WEBRTC_MAX_FRAME_BYTES * 2) as u32;
#[async_trait]
pub(crate) trait LocalWebrtcDataChannel: Send + Sync {
    async fn local_set_buffered_amount_low_threshold(&self, threshold: u32) -> Result<(), String>;
    async fn local_set_buffered_amount_high_threshold(&self, threshold: u32) -> Result<(), String>;
    async fn local_outstanding_bytes(&self) -> Result<usize, webrtc::error::Error> {
        Ok(0)
    }
    async fn local_send_text(&self, text: &str) -> Result<(), String>;
    async fn local_send_binary(&self, bytes: &[u8]) -> Result<(), webrtc::error::Error>;
    async fn local_poll(&self) -> Option<DataChannelEvent>;
    async fn local_close(&self) -> Result<(), String>;
}

#[async_trait]
impl<T> LocalWebrtcDataChannel for T
where
    T: DataChannel + ?Sized,
{
    async fn local_set_buffered_amount_low_threshold(&self, threshold: u32) -> Result<(), String> {
        self.set_buffered_amount_low_threshold(threshold)
            .await
            .map_err(|error| error.to_string())
    }

    async fn local_set_buffered_amount_high_threshold(&self, threshold: u32) -> Result<(), String> {
        self.set_buffered_amount_high_threshold(threshold)
            .await
            .map_err(|error| error.to_string())
    }

    async fn local_outstanding_bytes(&self) -> Result<usize, webrtc::error::Error> {
        self.outstanding_bytes().await
    }

    async fn local_send_text(&self, text: &str) -> Result<(), String> {
        self.send_text(text)
            .await
            .map_err(|error| error.to_string())
    }

    async fn local_send_binary(&self, bytes: &[u8]) -> Result<(), webrtc::error::Error> {
        self.send(BytesMut::from(bytes)).await
    }

    async fn local_poll(&self) -> Option<DataChannelEvent> {
        self.poll().await
    }

    async fn local_close(&self) -> Result<(), String> {
        match self.close().await {
            Ok(()) | Err(webrtc::error::Error::ErrDataChannelClosed) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }
}

/// One admitted inbound control frame waiting for service.
pub(crate) enum PendingLocalWebrtcRequest {
    Hello(Box<DaemonHello>),
    Request {
        request_id: String,
        request: Box<DaemonRequest>,
    },
    /// A request that arrived past the outstanding limit. Hub answers it with
    /// a correlated `too_many_requests` operator error and never services it.
    TooManyRequests {
        request_id: String,
        operation: &'static str,
    },
}

pub(crate) enum LocalWebrtcInbound {
    Channel(Result<Option<DataChannelEvent>, LocalWebrtcTerminalCause>),
    AdapterReady,
}

/// Per-channel protocol state shared by the driver and its send paths.
#[derive(Debug, Default)]
pub(crate) struct LocalWebrtcFlowControl {
    pub(crate) pressured: bool,
    /// Set when the first `ClientFrame::Hello` is admitted. Requests before
    /// it, and a second hello, are `handshake_order` violations.
    pub(crate) hello_accepted: bool,
    /// Last admitted request id; every request id must exceed it.
    pub(crate) last_request_id: u64,
}
pub(crate) fn pop_pending_request(
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
) -> Option<PendingLocalWebrtcRequest> {
    pending_requests.pop_front()
}
pub(crate) fn local_webrtc_request_operation(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Status => "status",
        DaemonRequest::Spawn { .. } => "spawn",
        DaemonRequest::Attach { .. } => "attach",
        DaemonRequest::ShutdownSession { .. } => "shutdown_session",
        _ => "other",
    }
}
pub(crate) async fn send_text_or_peer_terminal<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    frame: &str,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
    mux: &WebRtcConnectionMux,
    peer_terminal_rx: &mut watch::Receiver<Option<LocalWebrtcTerminalCause>>,
) -> Result<(), LocalWebrtcTerminalCause>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    if let Some(cause) = *peer_terminal_rx.borrow_and_update() {
        return Err(cause);
    }
    let send = data_channel.local_send_text(frame);
    tokio::pin!(send);
    let deadline = tokio::time::sleep(LOCAL_WEBRTC_PEER_CLOSE_BOUND);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            send = &mut send => {
                return send.map_err(|_| LocalWebrtcTerminalCause::SendText);
            }
            event = poll_data_channel_or_peer_terminal(data_channel, peer_terminal_rx) => {
                match event {
                    Ok(Some(channel_event)) => apply_data_channel_event(
                        channel_event,
                        stream_key,
                        pending_requests,
                        flow_control,
                        mux,
                    )?,
                    Ok(None) => return Err(LocalWebrtcTerminalCause::PollEnded),
                    Err(cause) => return Err(cause),
                }
            }
            () = &mut deadline => {
                return Err(LocalWebrtcTerminalCause::SendText);
            }
        }
    }
}

pub(crate) async fn poll_data_channel_or_peer_terminal<D>(
    data_channel: &D,
    peer_terminal_rx: &mut watch::Receiver<Option<LocalWebrtcTerminalCause>>,
) -> Result<Option<DataChannelEvent>, LocalWebrtcTerminalCause>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    if let Some(cause) = *peer_terminal_rx.borrow_and_update() {
        return Err(cause);
    }
    tokio::select! {
        event = data_channel.local_poll() => Ok(event),
        changed = peer_terminal_rx.changed() => {
            changed.expect("local WebRTC peer terminal sender remains owned by peer state");
            Err(peer_terminal_rx
                .borrow_and_update()
                .expect("peer terminal watch changes only when a terminal cause is published"))
        }
    }
}

pub(crate) async fn run_data_channel<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
    runtime_tx: &ControlSender,
) -> Option<LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    let mut pending_requests = VecDeque::new();
    let mut flow_control = LocalWebrtcFlowControl::default();
    let mut send_failure = None;
    let mut terminal_cause = LocalWebrtcTerminalCause::PollEnded;
    let mut peer_terminal_rx = peer_state.subscribe_peer_terminal();
    let mut open = true;
    while open {
        if let Err(failure) = flush_ready_webrtc_host_control(
            data_channel,
            stream_key,
            peer_state,
            &mut pending_requests,
            &mut flow_control,
        )
        .await
        {
            eprintln!("{failure}");
            terminal_cause = failure.cause;
            send_failure = Some(failure);
            break;
        }
        let pending = if let Some(request) = pop_pending_request(&mut pending_requests) {
            request
        } else {
            if host_event_ready(peer_state) {
                continue;
            }
            let inbound = tokio::select! {
                biased;
                channel = poll_data_channel_or_peer_terminal(data_channel, &mut peer_terminal_rx) => {
                    LocalWebrtcInbound::Channel(channel)
                }
                _ = peer_state.mux.wait_for_write() => {
                    LocalWebrtcInbound::AdapterReady
                }
            };
            match inbound {
                LocalWebrtcInbound::Channel(Err(cause)) => {
                    terminal_cause = cause;
                    break;
                }
                LocalWebrtcInbound::AdapterReady => continue,
                LocalWebrtcInbound::Channel(Ok(Some(DataChannelEvent::OnMessage(message)))) => {
                    match admit_client_frame(stream_key, message.data.as_ref(), &mut flow_control) {
                        Ok(pending) => pending,
                        Err(cause) => {
                            terminal_cause = cause;
                            break;
                        }
                    }
                }
                LocalWebrtcInbound::Channel(Ok(Some(
                    DataChannelEvent::OnClose | DataChannelEvent::OnClosing,
                ))) => {
                    terminal_cause = LocalWebrtcTerminalCause::ChannelClosed;
                    break;
                }
                LocalWebrtcInbound::Channel(Ok(Some(DataChannelEvent::OnError))) => {
                    terminal_cause = LocalWebrtcTerminalCause::ChannelError;
                    break;
                }
                LocalWebrtcInbound::Channel(Ok(None)) => {
                    terminal_cause = LocalWebrtcTerminalCause::PollEnded;
                    break;
                }
                LocalWebrtcInbound::Channel(Ok(Some(
                    event @ (DataChannelEvent::OnBufferedAmountHigh
                    | DataChannelEvent::OnBufferedAmountLow),
                ))) => {
                    let _ = apply_data_channel_event(
                        event,
                        stream_key,
                        &mut pending_requests,
                        &mut flow_control,
                        &peer_state.mux,
                    );
                    continue;
                }
                LocalWebrtcInbound::Channel(Ok(Some(_))) => continue,
            }
        };

        let (request_id, request) = match pending {
            PendingLocalWebrtcRequest::Hello(hello) => {
                if hello.protocol != PROTOCOL {
                    terminal_cause = LocalWebrtcTerminalCause::InvalidRequest;
                    break;
                }
                let version_matches = hello.compatibility.protocol_version == PROTOCOL_VERSION;
                if version_matches && !peer_state.cleanup_sent.load(Ordering::Acquire) {
                    let admission = if let Some(requirement) = hello.terminal_compatibility.as_ref()
                        && let Err(error) = ensure_terminal_compatible(
                            requirement,
                            &TerminalCompatibility::current(),
                        ) {
                        WebrtcTerminalAdmission::Rejected {
                            code: "terminal_compatibility",
                            diagnostic: DaemonDiagnostic::compatibility_mismatch(error.diagnostic),
                            mux: peer_state.mux.clone(),
                            peer_generation: 0,
                        }
                    } else {
                        WebrtcTerminalAdmission::Admitted {
                            required_features: hello.compatibility.required_features.clone(),
                            mux: {
                                if hello_requires_terminal_subscription_closed(
                                    &hello.compatibility.required_features,
                                ) {
                                    peer_state.mux.admit_close_events();
                                }
                                peer_state.mux.clone()
                            },
                            terminal_requirement: hello.terminal_compatibility.clone(),
                            peer_generation: 0,
                        }
                    };
                    let _ = runtime_tx
                        .send(ControlMessage::RegisterWebrtcAdmission {
                            grant_id: peer_state.grant_id.clone(),
                            admission,
                            host_required_features: hello.compatibility.required_features.clone(),
                        })
                        .await;
                }
                peer_state.begin_operation("hello");
                let ack = DaemonHelloAck {
                    protocol: PROTOCOL.to_string(),
                    compatibility: DaemonCompatibility::current(),
                    terminal_compatibility: Some(TerminalCompatibility::current()),
                    diagnostics: vec![DaemonDiagnostic::connected("hello")],
                    terminal_generation: None,
                };
                let Ok(frames) = framed_server_frame(stream_key, &ServerFrame::HelloAck { ack })
                else {
                    terminal_cause = LocalWebrtcTerminalCause::ResponseFraming;
                    break;
                };
                match send_response_frames(
                    data_channel,
                    stream_key,
                    &frames,
                    &mut pending_requests,
                    &mut flow_control,
                    peer_state,
                )
                .await
                {
                    Ok(()) if version_matches => continue,
                    Ok(()) => {
                        // The ack carries the running descriptor; the client
                        // reports the mismatch. There is no request service.
                        terminal_cause = LocalWebrtcTerminalCause::ProtocolVersionMismatch;
                        break;
                    }
                    Err(failure) => {
                        eprintln!("{failure}");
                        terminal_cause = failure.cause;
                        send_failure = Some(failure);
                        break;
                    }
                }
            }
            PendingLocalWebrtcRequest::TooManyRequests {
                request_id,
                operation,
            } => {
                peer_state.begin_overflow_response();
                let response = too_many_requests_response(&request_id, operation);
                let Ok(frames) = framed_daemon_response(stream_key, &request_id, &response) else {
                    terminal_cause = LocalWebrtcTerminalCause::ResponseFraming;
                    break;
                };
                match send_response_frames(
                    data_channel,
                    stream_key,
                    &frames,
                    &mut pending_requests,
                    &mut flow_control,
                    peer_state,
                )
                .await
                {
                    Ok(()) => open = true,
                    Err(failure) => {
                        eprintln!("{failure}");
                        terminal_cause = failure.cause;
                        send_failure = Some(failure);
                        open = false;
                    }
                }
                continue;
            }
            PendingLocalWebrtcRequest::Request {
                request_id,
                request,
            } => (request_id, request),
        };

        peer_state.begin_request(&request);
        let request_operation = control_request_operation_label(&request).to_string();
        let completion_projection = RequestCompletionProjection::from_request(request.as_ref());
        let daemon_shutdown = matches!(*request, DaemonRequest::DaemonShutdown);
        let (reply_tx, reply_rx) = control_reply_channel();
        let (response_delivery_tx, response_delivery_rx) = if daemon_shutdown {
            let (tx, rx) = mpsc::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let request_sent = match *request {
            DaemonRequest::SubscribeEntities {
                entity_type,
                subscription_id,
            } => {
                let (frame_tx, frame_rx) = tokio_mpsc::channel(
                    crate::admission::budgets::ENTITY_SUBSCRIPTION_QUEUE_CAPACITY,
                );
                runtime_tx
                    .send(ControlMessage::SubscribeEntities {
                        entity_type,
                        subscription_id,
                        transport_request_id: Some(request_id.clone()),
                        client_id: Some(format!("botster-hub-webrtc-{}", peer_state.grant_id)),
                        frame_tx: EntityFrameSender::Async(frame_tx),
                        frame_rx: Some(frame_rx),
                        reply_tx,
                        grant_id: Some(peer_state.grant_id.clone()),
                    })
                    .await
            }
            DaemonRequest::UnsubscribeEntities { subscription_id } => {
                runtime_tx
                    .send(ControlMessage::UnsubscribeEntities {
                        subscription_id,
                        reply_tx: Some(reply_tx),
                        grant_id: Some(peer_state.grant_id.clone()),
                    })
                    .await
            }
            request => {
                runtime_tx
                    .send(ControlMessage::Request {
                        request: Box::new(request),
                        transport_request_id: Some(request_id.clone()),
                        reply_tx,
                        response_delivery_rx,
                        grant_id: Some(peer_state.grant_id.clone()),
                        client_id: Some(format!("botster-hub-webrtc-{}", peer_state.grant_id)),
                        enqueued_at: Instant::now(),
                    })
                    .await
            }
        };
        if request_sent.is_err() {
            terminal_cause = LocalWebrtcTerminalCause::RuntimeQueueClosed;
            break;
        }
        use crate::daemon::control::reply::ControlReply;
        let reply = match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => ControlReply::plain(Ok(correlated_response_with_diagnostic(
                &request_id,
                &request_operation,
                OPERATOR_ERROR_RUNTIME_REPLY_CLOSED,
                DaemonDiagnostic::action_failure(
                    "local_webrtc_data_channel",
                    "runtime reply channel closed",
                ),
            ))),
            Err(_) => ControlReply::plain(Ok(correlated_response_with_diagnostic(
                &request_id,
                &request_operation,
                OPERATOR_ERROR_RUNTIME_REQUEST_TIMED_OUT,
                DaemonDiagnostic::action_failure(
                    "local_webrtc_data_channel",
                    "runtime request timed out",
                ),
            ))),
        };
        let (framed, response_kind, delivery_receipt) = match reply {
            ControlReply::EncodedPlugin {
                kind,
                encoded_frame,
                charge,
            } => {
                let framed =
                    framed_encoded_daemon_response(stream_key, &request_id, &encoded_frame);
                drop(encoded_frame);
                drop(charge);
                (framed, kind, None)
            }
            ControlReply::Typed {
                response,
                charge,
                delivery,
            } => {
                let response = response.unwrap_or_else(|error| {
                    correlated_response_with_diagnostic(
                        &request_id,
                        &request_operation,
                        OPERATOR_ERROR_RUNTIME_REQUEST_FAILED,
                        DaemonDiagnostic::action_failure(
                            "local_webrtc_data_channel",
                            error.to_string(),
                        ),
                    )
                });
                // Failed attaches create no subscription ownership.
                peer_state.apply_subscription_change(
                    completion_projection
                        .attached_subscription_change(&response)
                        .map(Into::into),
                );
                match completion_projection.entity_subscription_change(&response) {
                    Some(EntitySubscriptionChange::Subscribe(subscription_id)) => {
                        peer_state.add_entity_subscription(subscription_id)
                    }
                    Some(EntitySubscriptionChange::Unsubscribe(subscription_id)) => {
                        peer_state.remove_entity_subscription(&subscription_id)
                    }
                    None => {}
                }
                let framed = framed_daemon_response(stream_key, &request_id, &response);
                let kind = response.kind;
                drop(response);
                drop(charge);
                (framed, kind, delivery)
            }
        };
        let Ok(frames) = framed else {
            if let Some(response_delivery_tx) = response_delivery_tx {
                let _ = response_delivery_tx.send(());
            }
            terminal_cause = LocalWebrtcTerminalCause::ResponseFraming;
            break;
        };
        let delivery = send_response_frames(
            data_channel,
            stream_key,
            &frames,
            &mut pending_requests,
            &mut flow_control,
            peer_state,
        )
        .await;
        if delivery.is_ok()
            && let Some(receipt) = delivery_receipt
        {
            receipt.delivered();
        }
        if let Some(response_delivery_tx) = response_delivery_tx {
            let _ = response_delivery_tx.send(());
        }
        match delivery {
            Ok(()) if daemon_shutdown && response_kind == DaemonResponseKind::Shutdown => {
                terminal_cause = LocalWebrtcTerminalCause::DaemonShutdown;
                open = false;
            }
            Ok(()) => open = true,
            Err(failure) => {
                eprintln!("{failure}");
                terminal_cause = failure.cause;
                send_failure = Some(failure);
                open = false;
            }
        }
    }
    close_data_channel(
        data_channel,
        stream_key,
        &mut pending_requests,
        peer_state,
        terminal_cause,
    )
    .await;
    send_failure
}

/// Typed close frame for causes the client must learn from Hub itself.
fn close_frame_for_cause(cause: LocalWebrtcTerminalCause) -> Option<ServerFrame> {
    match cause {
        LocalWebrtcTerminalCause::ProtocolViolation(code) => Some(ServerFrame::Close {
            reason: DaemonCloseReason::ProtocolError { code },
        }),
        LocalWebrtcTerminalCause::DaemonShutdown => Some(ServerFrame::Close {
            reason: DaemonCloseReason::DaemonShutdown,
        }),
        _ => None,
    }
}

pub(crate) async fn close_data_channel<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    peer_state: &LocalWebrtcPeerState,
    cause: LocalWebrtcTerminalCause,
) where
    D: LocalWebrtcDataChannel + ?Sized,
{
    pending_requests.clear();
    peer_state.mux.close_all();
    if let Some(frame) = close_frame_for_cause(cause)
        && let Ok(frames) = framed_server_frame(stream_key, &frame)
    {
        // Best effort: the channel closes below whether or not the peer
        // receives the reason.
        let _ = tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, async {
            for frame in &frames {
                if data_channel.local_send_text(frame).await.is_err() {
                    break;
                }
            }
        })
        .await;
    }
    #[cfg(test)]
    let force_hang = peer_state
        .force_local_close_hang
        .swap(false, Ordering::AcqRel);
    #[cfg(not(test))]
    let force_hang = false;
    match tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, async {
        if force_hang {
            std::future::pending::<()>().await;
        }
        data_channel.local_close().await
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("local WebRTC data channel close failed: {error}");
        }
        Err(_) => {
            eprintln!(
                "local WebRTC data channel close timed out after {:?}: grant_id={}",
                LOCAL_WEBRTC_PEER_CLOSE_BOUND, peer_state.grant_id
            );
        }
    }
    peer_state.cleanup_once(cause).await;
}

pub(crate) async fn send_response_frames<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    frames: &[String],
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
    peer_state: &LocalWebrtcPeerState,
) -> Result<(), LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    let mut peer_terminal_rx = peer_state.subscribe_peer_terminal();
    let total_chunks = frames.len();
    let message_id = frames.first().and_then(|frame| {
        serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(frame)
            .ok()
            .map(|chunk| chunk.message_id)
    });
    peer_state.begin_response(message_id.clone(), total_chunks, flow_control.pressured);

    let failure =
        |next_chunk_index, cause, flow_control: &LocalWebrtcFlowControl| LocalWebrtcSendFailure {
            message_id: message_id
                .clone()
                .unwrap_or_else(|| "unavailable".to_string()),
            next_chunk_index,
            last_sent_chunk_index: next_chunk_index.checked_sub(1),
            total_chunks,
            pressured: flow_control.pressured,
            cause,
        };

    for (chunk_index, frame) in frames.iter().enumerate() {
        peer_state.record_response_progress(chunk_index, flow_control.pressured);
        while flow_control.pressured {
            match poll_data_channel_or_peer_terminal(data_channel, &mut peer_terminal_rx).await {
                Ok(Some(event)) => apply_data_channel_event(
                    event,
                    stream_key,
                    pending_requests,
                    flow_control,
                    &peer_state.mux,
                )
                .map_err(|cause| failure(chunk_index, cause, flow_control))?,
                Ok(None) => {
                    return Err(failure(
                        chunk_index,
                        LocalWebrtcTerminalCause::PollEnded,
                        flow_control,
                    ));
                }
                Err(cause) => {
                    return Err(failure(chunk_index, cause, flow_control));
                }
            }
        }

        if let Err(cause) = send_text_or_peer_terminal(
            data_channel,
            stream_key,
            frame,
            pending_requests,
            flow_control,
            &peer_state.mux,
            &mut peer_terminal_rx,
        )
        .await
        {
            return Err(failure(chunk_index, cause, flow_control));
        }
        peer_state.record_response_progress(chunk_index + 1, flow_control.pressured);

        match timeout(
            webrtc_runtime().as_ref(),
            LOCAL_WEBRTC_EVENT_PROBE,
            data_channel.local_poll(),
        )
        .await
        {
            Ok(Some(event)) => {
                apply_data_channel_event(
                    event,
                    stream_key,
                    pending_requests,
                    flow_control,
                    &peer_state.mux,
                )
                .map_err(|cause| failure(chunk_index + 1, cause, flow_control))?;
            }
            Ok(None) => {
                return Err(failure(
                    chunk_index + 1,
                    LocalWebrtcTerminalCause::PollEnded,
                    flow_control,
                ));
            }
            Err(_) => {}
        }
    }
    Ok(())
}

pub(crate) fn apply_data_channel_event(
    event: DataChannelEvent,
    stream_key: &AesGcmKey,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
    _mux: &WebRtcConnectionMux,
) -> Result<(), LocalWebrtcTerminalCause> {
    match event {
        DataChannelEvent::OnBufferedAmountHigh => {
            // Pause only the in-flight DataChannel send path. Do not mark every
            // mux handle WouldBlock: that silences healthy siblings while one
            // stalled generation fills the peer send buffer.
            flow_control.pressured = true;
            Ok(())
        }
        DataChannelEvent::OnBufferedAmountLow => {
            flow_control.pressured = false;
            Ok(())
        }
        DataChannelEvent::OnMessage(message) => {
            let pending = admit_client_frame(stream_key, message.data.as_ref(), flow_control)?;
            let mut requests = 0usize;
            let mut rejections = 0usize;
            for queued in pending_requests.iter() {
                match queued {
                    PendingLocalWebrtcRequest::Request { .. }
                    | PendingLocalWebrtcRequest::Hello(_) => requests += 1,
                    PendingLocalWebrtcRequest::TooManyRequests { .. } => rejections += 1,
                }
            }
            let pending = match pending {
                PendingLocalWebrtcRequest::Request {
                    request_id,
                    request,
                } if requests >= LOCAL_WEBRTC_PENDING_REQUESTS => {
                    if rejections >= LOCAL_WEBRTC_PENDING_REJECTIONS {
                        return Err(LocalWebrtcTerminalCause::RequestQueueOverflow);
                    }
                    PendingLocalWebrtcRequest::TooManyRequests {
                        request_id,
                        operation: control_request_operation_label(&request),
                    }
                }
                pending => pending,
            };
            pending_requests.push_back(pending);
            Ok(())
        }
        DataChannelEvent::OnClose | DataChannelEvent::OnClosing => {
            Err(LocalWebrtcTerminalCause::ChannelClosed)
        }
        DataChannelEvent::OnError => Err(LocalWebrtcTerminalCause::ChannelError),
        _ => Ok(()),
    }
}

/// Decrypt one control message and apply the protocol 10 admission rules:
/// hello first and once, canonical strictly increasing request ids.
pub(crate) fn admit_client_frame(
    key: &AesGcmKey,
    bytes: &[u8],
    flow_control: &mut LocalWebrtcFlowControl,
) -> Result<PendingLocalWebrtcRequest, LocalWebrtcTerminalCause> {
    let Some(frame) = decrypt_client_frame(key, bytes) else {
        return Err(LocalWebrtcTerminalCause::InvalidEncryptedRequest);
    };
    match frame {
        ClientFrame::Hello { hello } => {
            if flow_control.hello_accepted {
                return Err(LocalWebrtcTerminalCause::ProtocolViolation(
                    DaemonProtocolErrorCode::HandshakeOrder,
                ));
            }
            flow_control.hello_accepted = true;
            Ok(PendingLocalWebrtcRequest::Hello(Box::new(hello)))
        }
        ClientFrame::Request {
            request_id,
            request,
        } => {
            if !flow_control.hello_accepted {
                return Err(LocalWebrtcTerminalCause::ProtocolViolation(
                    DaemonProtocolErrorCode::HandshakeOrder,
                ));
            }
            let Some(parsed) = parse_request_id(&request_id) else {
                return Err(LocalWebrtcTerminalCause::ProtocolViolation(
                    DaemonProtocolErrorCode::InvalidRequestId,
                ));
            };
            if parsed <= flow_control.last_request_id {
                return Err(LocalWebrtcTerminalCause::ProtocolViolation(
                    DaemonProtocolErrorCode::NonincreasingRequestId,
                ));
            }
            flow_control.last_request_id = parsed;
            Ok(PendingLocalWebrtcRequest::Request {
                request_id,
                request: Box::new(request),
            })
        }
    }
}

/// Decrypt one JSON `AesGcmEnvelope` text message into a [`ClientFrame`].
pub(crate) fn decrypt_client_frame(key: &AesGcmKey, bytes: &[u8]) -> Option<ClientFrame> {
    let envelope = serde_json::from_slice::<AesGcmEnvelope>(bytes).ok()?;
    let plaintext = decrypt_aes_gcm(key, &envelope).ok()?;
    if plaintext.len() > MAX_CONTROL_REQUEST_BYTES {
        return None;
    }
    serde_json::from_slice::<ClientFrame>(&plaintext).ok()
}
pub(crate) fn host_event_ready(peer_state: &LocalWebrtcPeerState) -> bool {
    peer_state.mux.has_pending_event()
}

pub(crate) fn take_host_event(peer_state: &LocalWebrtcPeerState) -> Option<DaemonEvent> {
    peer_state.mux.pop_pending_event()
}

pub(crate) async fn flush_ready_webrtc_host_control<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
) -> Result<(), LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    let Some(event) = take_host_event(peer_state) else {
        return Ok(());
    };
    if matches!(event, DaemonEvent::TerminalSubscriptionClosed { .. })
        && !peer_state.mux.close_events_admitted()
    {
        return Ok(());
    }
    peer_state.begin_operation("host_event_delivery");
    let Ok(frames) = framed_server_frame(stream_key, &ServerFrame::Event { event }) else {
        return Ok(());
    };
    send_response_frames(
        data_channel,
        stream_key,
        &frames,
        pending_requests,
        flow_control,
        peer_state,
    )
    .await
}

pub(crate) fn response_with_diagnostic(diagnostic: DaemonDiagnostic) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.diagnostics = vec![diagnostic];
    response
}

fn correlated_response_with_diagnostic(
    request_id: &str,
    operation: &str,
    code: &str,
    diagnostic: DaemonDiagnostic,
) -> DaemonResponse {
    let message = diagnostic
        .message
        .clone()
        .unwrap_or_else(|| "local WebRTC request failed".to_string());
    let mut response = response_with_diagnostic(diagnostic.clone());
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: request_id.to_string(),
        operation: operation.to_string(),
        message,
        diagnostics: vec![diagnostic],
    });
    response
}

/// Correlated rejection for a request past the outstanding limit.
pub(crate) fn too_many_requests_response(request_id: &str, operation: &str) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: OPERATOR_ERROR_TOO_MANY_REQUESTS.to_string(),
        request_id: request_id.to_string(),
        operation: operation.to_string(),
        message: format!(
            "connection already holds {MAX_OUTSTANDING_REQUESTS} outstanding requests"
        ),
        diagnostics: Vec::new(),
    });
    response
}
#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::admission::budgets::ENTITY_SUBSCRIPTION_QUEUE_CAPACITY;
    use crate::admission::unix_hello::WebrtcTerminalAdmission;
    use crate::daemon::control::handle_control_message;
    use crate::daemon::control::message::{ControlMessage, ControlSender};
    use crate::daemon::owner_loop::DaemonControlState;
    use crate::subscription::attach_routes::negotiated_unix_capability_set;
    use crate::subscription::entity::EntityFrameSender;
    use crate::transport::webrtc::adapter::WebRtcConnectionMux;
    use crate::transport::webrtc::control_channel::*;
    use crate::transport::webrtc::delivery::*;
    use crate::transport::webrtc::peer::*;
    use crate::transport::webrtc::subscription_channel::*;
    use crate::transport::webrtc::test_support::*;
    use crate::transport::webrtc::{LocalWebrtcError, LocalWebrtcResult};
    use crate::{
        DataDirectoryOption, HostIdentityOptions, HubDaemon, HubStartupOptions,
        PackageEventPlaneOptions, RuntimeEnvironment, SessionDefaults,
    };
    use async_trait::async_trait;
    use botster_core::contract::terminal_adapter::{
        TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError,
    };
    use botster_core::{AesGcmKey, encrypt_aes_gcm};
    use botster_hub_client::{
        DaemonDiagnostic, DaemonEntityFrame, DaemonHello, DaemonRequest, DaemonResponse,
        LOCAL_WEBRTC_MAX_DELIVERY_BYTES, OPERATOR_ERROR_TOO_MANY_REQUESTS,
    };
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;
    use tokio::sync::mpsc as tokio_mpsc;
    use webrtc::data_channel::RTCDataChannelInit;
    use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelMessage};
    use webrtc::peer_connection::{
        PeerConnection, PeerConnectionEventHandler, RTCIceGatheringState, RTCPeerConnectionState,
    };
    use webrtc::runtime::{
        Receiver as AsyncReceiver, Sender as AsyncSender, channel as webrtc_channel,
        default_runtime, timeout,
    };

    #[test]
    fn correlated_runtime_failure_preserves_request_and_operation() {
        let request = DaemonRequest::PluginSurfaceRender {
            package_name: "workspaces".to_string(),
            surface_id: "home".to_string(),
            payload: serde_json::json!({}),
        };
        let response = correlated_response_with_diagnostic(
            "73",
            control_request_operation_label(&request),
            OPERATOR_ERROR_RUNTIME_REQUEST_TIMED_OUT,
            DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                "runtime request timed out",
            ),
        );

        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        assert_eq!(response.diagnostics.len(), 1);
        let error = response.error.expect("correlated operator error");
        assert_eq!(error.code, OPERATOR_ERROR_RUNTIME_REQUEST_TIMED_OUT);
        assert_eq!(error.request_id, "73");
        assert_eq!(error.operation, "plugin_surface_render");
        assert_eq!(error.diagnostics, response.diagnostics);
    }

    #[test]
    fn informational_diagnostic_does_not_invent_request_correlation() {
        let response = response_with_diagnostic(DaemonDiagnostic::connected("hello"));

        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        assert!(response.error.is_none());
        assert_eq!(response.diagnostics.len(), 1);
    }
    fn run_idle_pressure_case(
        terminal_cause: Option<LocalWebrtcTerminalCause>,
    ) -> (FakeDataChannel, Option<LocalWebrtcSendFailure>) {
        let key = AesGcmKey::from_slice(&[15; 32]).unwrap();
        reset_test_request_ids();
        let data_channel = FakeDataChannel::default();
        {
            let mut events = data_channel.events.lock().unwrap();
            events.push_back(encrypted_hello_event(&key, &webrtc_adapter_hello()));
            events.push_back(DataChannelEvent::OnBufferedAmountHigh);
            events.push_back(encrypted_request_event(&key, &DaemonRequest::Status));
            if terminal_cause.is_none() {
                events.push_back(DataChannelEvent::OnBufferedAmountLow);
                events.push_back(DataChannelEvent::OnClose);
            }
        }
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-idle-pressure".to_string(),
            runtime_tx,
        ));
        let responder = std::thread::spawn(move || {
            let ControlMessage::Request {
                request, reply_tx, ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected daemon request before peer cleanup");
            };
            assert_eq!(*request, DaemonRequest::Status);
            reply_tx
                .send(Ok(response_with_diagnostic(DaemonDiagnostic::connected(
                    "fixture",
                ))))
                .unwrap();
            assert!(matches!(
                receive_test_runtime_message(&mut runtime_rx),
                ControlMessage::LocalWebrtcPeerClosed { grant_id, .. }
                    if grant_id == "grant-idle-pressure"
            ));
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_sender = peer_state.runtime_tx.clone();
        let failure = runtime.block_on(async {
            let delivery =
                run_data_channel(&data_channel, &key, peer_state.as_ref(), &runtime_sender);
            tokio::pin!(delivery);
            if let Some(cause) = terminal_cause {
                assert!(
                    timeout(
                        webrtc_runtime().as_ref(),
                        Duration::from_millis(20),
                        delivery.as_mut(),
                    )
                    .await
                    .is_err(),
                    "scheduler time alone must not close a live pressured peer"
                );
                peer_state.publish_peer_terminal(cause);
            }
            timeout(
                webrtc_runtime().as_ref(),
                Duration::from_millis(250),
                delivery.as_mut(),
            )
            .await
            .expect("outer data-channel loop must finish on low water, close, or peer terminal")
        });
        responder.join().unwrap();
        (data_channel, failure)
    }

    #[test]
    fn channel_close_during_first_status_chunk_retains_owner_terminal_progress() {
        struct ReleaseBlockedSend {
            data_channel: Arc<FakeDataChannel>,
            peer_state: Arc<LocalWebrtcPeerState>,
        }
        impl Drop for ReleaseBlockedSend {
            fn drop(&mut self) {
                self.data_channel.send_hangs.store(false, Ordering::Release);
                self.data_channel.send_notify.notify_waiters();
                self.peer_state
                    .publish_peer_terminal(LocalWebrtcTerminalCause::ChannelClosed);
            }
        }

        reset_test_request_ids();
        let mut harness = PeerHarness::new("status-send-close-record");
        let live_peer = harness.signal_peer("http://127.0.0.1:41822");
        let grant_id = live_peer.grant_id.clone();
        let key = live_peer.stream_key.clone();
        let data_channel = Arc::new(FakeDataChannel::default());
        data_channel.push_event(encrypted_hello_event(&key, &webrtc_adapter_hello()));
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            grant_id.clone(),
            runtime_tx.clone(),
        ));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let driver = runtime.spawn({
            let data_channel = Arc::clone(&data_channel);
            let peer_state = Arc::clone(&peer_state);
            let key = key.clone();
            async move {
                run_data_channel(
                    data_channel.as_ref(),
                    &key,
                    peer_state.as_ref(),
                    &runtime_tx,
                )
                .await
            }
        });
        let _release = ReleaseBlockedSend {
            data_channel: Arc::clone(&data_channel),
            peer_state: Arc::clone(&peer_state),
        };

        // Finish the complete HelloAck before arming the response-send hang.
        let ack_frames = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let sent = data_channel.sent.lock().unwrap().clone();
                    if let Some(first) = sent.first() {
                        let first: DaemonLocalWebrtcDeliveryChunk =
                            serde_json::from_str(first).expect("parse first HelloAck chunk");
                        if sent.len() >= first.chunk_count as usize {
                            break sent;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("complete HelloAck delivery")
        });
        let first: DaemonLocalWebrtcDeliveryChunk =
            serde_json::from_str(&ack_frames[0]).expect("parse HelloAck chunk");
        assert!(first.chunk_count > 0);
        assert_eq!(ack_frames.len(), first.chunk_count as usize);
        let mut encrypted = String::new();
        for (index, frame) in ack_frames.iter().enumerate() {
            let chunk: DaemonLocalWebrtcDeliveryChunk =
                serde_json::from_str(frame).expect("parse HelloAck chunk");
            assert_eq!(chunk.message_id, first.message_id);
            assert_eq!(chunk.chunk_index as usize, index);
            assert_eq!(chunk.chunk_count, first.chunk_count);
            encrypted.push_str(&chunk.payload);
        }
        let envelope: AesGcmEnvelope = serde_json::from_str(&encrypted).expect("HelloAck envelope");
        let plaintext = decrypt_aes_gcm(&key, &envelope).expect("decrypt HelloAck");
        assert!(matches!(
            serde_json::from_slice::<ServerFrame>(&plaintext).expect("decode HelloAck"),
            ServerFrame::HelloAck { .. }
        ));

        data_channel.send_entered.store(false, Ordering::Release);
        data_channel.send_hangs.store(true, Ordering::Release);
        data_channel.push_event(encrypted_request_event(&key, &DaemonRequest::Status));
        let registration @ ControlMessage::RegisterWebrtcAdmission { .. } =
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(2), runtime_rx.recv())
                    .await
                    .expect("Hello registration reaches owner")
                    .expect("owner channel remains open")
            })
        else {
            panic!("Hello must register its admission before Status");
        };
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.transport_handle,
            harness.control_tx.clone(),
            registration,
        );
        assert!(
            harness
                .state
                .pending_runtime
                .has_webrtc_admission_row(&grant_id)
        );
        let ControlMessage::Request {
            request,
            transport_request_id,
            reply_tx,
            ..
        } = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), runtime_rx.recv())
                .await
                .expect("Status request reaches owner")
                .expect("owner channel remains open")
        })
        else {
            panic!("Status must reach the owner");
        };
        assert_eq!(*request, DaemonRequest::Status);
        let request_id = transport_request_id.expect("Status request id");
        let response = response_with_diagnostic(DaemonDiagnostic::connected("status-fixture"));
        let expected_chunks = framed_daemon_response(&key, &request_id, &response)
            .expect("frame Status response")
            .len();
        assert!(expected_chunks > 0);
        reply_tx.send(Ok(response)).expect("reply to Status");
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !data_channel.send_entered.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("first Status chunk enters blocked send")
        });
        assert!(data_channel.sent.lock().unwrap().len() == ack_frames.len());
        peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::ChannelClosed);
        let failure = runtime
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(2), driver)
                    .await
                    .expect("channel driver exits after terminal close")
                    .expect("channel driver task")
            })
            .expect("blocked Status send must fail");
        assert_eq!(failure.cause, LocalWebrtcTerminalCause::ChannelClosed);

        let message = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), runtime_rx.recv())
                .await
                .expect("peer close reaches owner")
                .expect("owner channel remains open")
        });
        assert!(
            matches!(&message, ControlMessage::LocalWebrtcPeerClosed { grant_id: closed, .. } if closed == &grant_id)
        );
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.transport_handle,
            harness.control_tx.clone(),
            message,
        );
        let record = harness
            .daemon
            .local_webrtc()
            .terminal_records()
            .into_iter()
            .find(|record| record.grant_id == grant_id)
            .expect("owner retains sender terminal record");
        assert_eq!(record.request_operation, "status");
        assert_eq!(record.cause, "channel_closed");
        assert_eq!(record.channel_terminal_signal, "on_close");
        assert_eq!(record.total_chunks, expected_chunks);
        assert_eq!(record.next_chunk_index, 0);
        assert_eq!(record.last_sent_chunk_index, None);
        live_peer.close_offer();
        harness.cleanup();
    }

    fn run_shutdown_response_delivery_case(
        send_fails: bool,
    ) -> (FakeDataChannel, Option<LocalWebrtcSendFailure>) {
        let key = AesGcmKey::from_slice(&[16; 32]).unwrap();
        reset_test_request_ids();
        let data_channel = FakeDataChannel::default();
        {
            let mut events = data_channel.events.lock().unwrap();
            events.push_back(encrypted_hello_event(&key, &webrtc_adapter_hello()));
            events.push_back(encrypted_request_event(
                &key,
                &DaemonRequest::DaemonShutdown,
            ));
        }
        // The hello ack must reach the peer; only the shutdown response may fail.
        data_channel
            .fail_sends_after
            .store(if send_fails { 1 } else { 0 }, Ordering::Release);
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-shutdown-delivery".to_string(),
            runtime_tx,
        ));
        let responder_peer_state = peer_state.clone();
        let responder = std::thread::spawn(move || {
            let ControlMessage::Request {
                request,
                reply_tx,
                response_delivery_rx,
                grant_id,
                ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected daemon shutdown request");
            };
            assert_eq!(grant_id.as_deref(), Some("grant-shutdown-delivery"));
            assert_eq!(*request, DaemonRequest::DaemonShutdown);
            let response_delivery_rx =
                response_delivery_rx.expect("WebRTC shutdown has delivery receiver");
            reply_tx
                .send(Ok(response_with_diagnostic(DaemonDiagnostic::connected(
                    "shutdown-fixture",
                ))))
                .unwrap();
            response_delivery_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("WebRTC delivery outcome releases shutdown completion");
            responder_peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::PeerClosed);
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_sender = peer_state.runtime_tx.clone();
        let failure = runtime.block_on(run_data_channel(
            &data_channel,
            &key,
            peer_state.as_ref(),
            &runtime_sender,
        ));
        responder.join().unwrap();
        (data_channel, failure)
    }

    #[test]
    fn local_webrtc_shutdown_success_releases_delivery_completion() {
        let (data_channel, failure) = run_shutdown_response_delivery_case(false);

        assert!(failure.is_none());
        assert!(!data_channel.sent.lock().unwrap().is_empty());
    }

    #[test]
    fn local_webrtc_shutdown_send_failure_releases_delivery_completion() {
        let (_data_channel, failure) = run_shutdown_response_delivery_case(true);

        assert_eq!(
            failure.expect("send failure remains visible").cause,
            LocalWebrtcTerminalCause::SendText
        );
    }

    #[test]
    fn recoverable_disconnect_after_response_preserves_followup_shutdown() {
        let key = AesGcmKey::from_slice(&[17; 32]).unwrap();
        reset_test_request_ids();
        let data_channel = Arc::new(FakeDataChannel::default());
        {
            let mut events = data_channel.events.lock().unwrap();
            events.push_back(encrypted_hello_event(&key, &webrtc_adapter_hello()));
            events.push_back(encrypted_request_event(&key, &DaemonRequest::Status));
            events.push_back(encrypted_request_event(
                &key,
                &DaemonRequest::ShutdownSession {
                    session_id: "recoverable-disconnect-session".to_string(),
                },
            ));
        }
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-recoverable-disconnect".to_string(),
            runtime_tx,
        ));
        let responder_peer_state = peer_state.clone();
        let responder_data_channel = data_channel.clone();
        let responder = std::thread::spawn(move || {
            let ControlMessage::Request {
                request, reply_tx, ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected status request");
            };
            assert_eq!(*request, DaemonRequest::Status);
            reply_tx
                .send(Ok(response_with_diagnostic(DaemonDiagnostic::connected(
                    "completed-response",
                ))))
                .unwrap();

            assert_eq!(
                responder_peer_state
                    .observe_peer_connection_state(RTCPeerConnectionState::Disconnected),
                None,
                "disconnected is recoverable and must not terminate the peer"
            );

            let ControlMessage::Request {
                request, reply_tx, ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected shutdown-session request after recoverable disconnect");
            };
            assert_eq!(
                *request,
                DaemonRequest::ShutdownSession {
                    session_id: "recoverable-disconnect-session".to_string(),
                }
            );
            reply_tx
                .send(Ok(response_with_diagnostic(DaemonDiagnostic::connected(
                    "followup-shutdown",
                ))))
                .unwrap();

            let deadline = Instant::now() + Duration::from_secs(1);
            // hello ack plus two responses
            while responder_data_channel.sent.lock().unwrap().len() < 3 {
                assert!(
                    Instant::now() < deadline,
                    "both responses must complete before terminal close"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(
                responder_peer_state.observe_peer_connection_state(RTCPeerConnectionState::Closed),
                Some(LocalWebrtcTerminalCause::PeerClosed)
            );
            assert!(matches!(
                receive_test_runtime_message(&mut runtime_rx),
                ControlMessage::LocalWebrtcPeerClosed { grant_id, .. }
                    if grant_id == "grant-recoverable-disconnect"
            ));
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_sender = peer_state.runtime_tx.clone();
        let failure = runtime.block_on(run_data_channel(
            data_channel.as_ref(),
            &key,
            peer_state.as_ref(),
            &runtime_sender,
        ));

        responder.join().unwrap();
        assert!(failure.is_none());
        assert_eq!(
            data_channel.sent.lock().unwrap().len(),
            3,
            "hello ack, status response, and shutdown response"
        );
        assert!(data_channel.closed.load(Ordering::Acquire));
    }

    #[test]
    fn outer_loop_routes_idle_pressure_before_next_request_delivery() {
        let (resumed_channel, _) = run_idle_pressure_case(None);
        assert!(
            !resumed_channel
                .sent_before_low_water
                .load(Ordering::Acquire)
        );
        assert_eq!(
            resumed_channel.sent.lock().unwrap().len(),
            2,
            "hello ack and the status response"
        );
        assert!(resumed_channel.closed.load(Ordering::Acquire));
    }

    #[test]
    fn idle_pressure_wakes_for_each_distinct_peer_terminal_cause() {
        for cause in [
            LocalWebrtcTerminalCause::PeerDisconnected,
            LocalWebrtcTerminalCause::PeerFailed,
            LocalWebrtcTerminalCause::PeerClosed,
        ] {
            let (channel, failure) = run_idle_pressure_case(Some(cause));
            assert_eq!(failure.unwrap().cause, cause);
            assert_eq!(
                channel.sent.lock().unwrap().len(),
                1,
                "only the hello ack leaves before the pressured response"
            );
            assert!(channel.closed.load(Ordering::Acquire));
        }
    }
    #[test]
    fn flow_control_pressure_is_cleared_only_by_low_water() {
        let key = AesGcmKey::from_slice(&[9; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        assert!(
            apply_data_channel_event(
                DataChannelEvent::OnBufferedAmountHigh,
                &key,
                &mut pending,
                &mut flow_control,
                &WebRtcConnectionMux::new(),
            )
            .is_ok()
        );
        assert!(flow_control.pressured);

        assert!(
            apply_data_channel_event(
                DataChannelEvent::OnOpen,
                &key,
                &mut pending,
                &mut flow_control,
                &WebRtcConnectionMux::new(),
            )
            .is_ok()
        );
        assert!(flow_control.pressured);

        assert!(
            apply_data_channel_event(
                DataChannelEvent::OnBufferedAmountLow,
                &key,
                &mut pending,
                &mut flow_control,
                &WebRtcConnectionMux::new(),
            )
            .is_ok()
        );
        assert!(!flow_control.pressured);
    }

    #[test]
    fn buffered_amount_high_does_not_mark_sibling_handles_would_block() {
        use botster_core::contract::terminal_adapter::{
            TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError,
        };
        use botster_terminal_protocol::{RouteId, RoutedTerminalFrame, encode_output};

        let mux = WebRtcConnectionMux::new();
        let (stall, stall_handle) = mux.create_adapter();
        let (mut sibling, sibling_handle) = mux.create_adapter();
        mux.register(
            "wwb-stall".to_string(),
            "sub-stall".to_string(),
            1,
            stall_handle,
        );
        mux.register(
            "wwb-live".to_string(),
            "sub-live".to_string(),
            1,
            sibling_handle,
        );
        let key = AesGcmKey::from_slice(&[9; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();

        assert!(
            apply_data_channel_event(
                DataChannelEvent::OnBufferedAmountHigh,
                &key,
                &mut pending,
                &mut flow_control,
                &mux,
            )
            .is_ok()
        );
        assert!(flow_control.pressured);
        assert_eq!(stall.pressure(), TerminalAdapterPressure::Ready);
        assert_eq!(sibling.pressure(), TerminalAdapterPressure::Ready);

        let frame = RoutedTerminalFrame::new(
            RouteId::new("wwb-live").expect("route"),
            1,
            0,
            encode_output(b"sibling-under-high-water").expect("output frame"),
        );
        assert_eq!(sibling.try_write(&frame), Ok(()));
        assert_eq!(sibling.pressure(), TerminalAdapterPressure::Full);
        assert_ne!(
            sibling.try_write(&frame),
            Err(TerminalAdapterWriteError::WouldBlock),
            "DataChannel high water must not convert a healthy sibling into WouldBlock"
        );
    }

    fn active_pressure_peer_terminal_case(
        cause: LocalWebrtcTerminalCause,
    ) -> (LocalWebrtcSendFailure, LocalWebrtcSenderTerminalRecord) {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountHigh);
        let key = AesGcmKey::from_slice(&[5; 32]).unwrap();
        let mut pending = VecDeque::from([PendingLocalWebrtcRequest::Request {
            request_id: "1".to_string(),
            request: Box::new(DaemonRequest::Status),
        }]);
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-fixture".to_string(),
            runtime_tx,
        ));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut flow_control = LocalWebrtcFlowControl::default();

        let failure = runtime.block_on(async {
            let frames = ["partial".to_string(), "completion".to_string()];
            let delivery = send_response_frames(
                &data_channel,
                &key,
                &frames,
                &mut pending,
                &mut flow_control,
                peer_state.as_ref(),
            );
            tokio::pin!(delivery);
            assert!(
                timeout(
                    webrtc_runtime().as_ref(),
                    Duration::from_millis(20),
                    delivery.as_mut(),
                )
                .await
                .is_err(),
                "elapsed scheduler time must not close a live pressured peer"
            );
            peer_state.publish_peer_terminal(cause);
            timeout(
                webrtc_runtime().as_ref(),
                Duration::from_millis(250),
                delivery.as_mut(),
            )
            .await
            .expect("peer terminal state must wake active pressure")
            .expect_err("peer terminal state must fail pending delivery")
        });
        assert_eq!(failure.cause, cause);
        assert_eq!(failure.next_chunk_index, 1);
        assert_eq!(failure.last_sent_chunk_index, Some(0));
        assert_eq!(failure.total_chunks, 2);
        assert!(failure.pressured);
        assert_eq!(data_channel.sent.lock().unwrap().as_slice(), &["partial"]);

        runtime.block_on(close_data_channel(
            &data_channel,
            &key,
            &mut pending,
            peer_state.as_ref(),
            cause,
        ));
        assert!(pending.is_empty());
        assert!(data_channel.closed.load(Ordering::Acquire));
        let ControlMessage::LocalWebrtcPeerClosed {
            grant_id,
            terminal_record,
            ..
        } = receive_test_runtime_message(&mut runtime_rx)
        else {
            panic!("expected peer cleanup");
        };
        assert_eq!(grant_id, "grant-fixture");
        (failure, terminal_record)
    }

    #[test]
    fn active_pressure_does_not_expire_and_wakes_for_each_peer_terminal_cause() {
        for cause in [
            LocalWebrtcTerminalCause::PeerDisconnected,
            LocalWebrtcTerminalCause::PeerFailed,
            LocalWebrtcTerminalCause::PeerClosed,
        ] {
            let (failure, terminal_record) = active_pressure_peer_terminal_case(cause);
            assert_eq!(failure.cause, cause);
            assert_eq!(terminal_record.cause, cause);
            assert_eq!(terminal_record.next_chunk_index, 1);
            assert_eq!(terminal_record.last_sent_chunk_index, Some(0));
            assert_eq!(terminal_record.total_chunks, 2);
            assert!(terminal_record.pressured);
        }
    }

    #[test]
    fn partial_chunked_response_records_message_and_nonzero_progress() {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountHigh);
        let key = AesGcmKey::from_slice(&[16; 32]).unwrap();
        let frames =
            frame_encrypted_daemon_delivery("response-progress", &"a".repeat(256 * 1024)).unwrap();
        assert!(frames.len() > 1);
        let mut pending = VecDeque::new();
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-progress".to_string(),
            runtime_tx,
        ));
        peer_state.begin_request(&DaemonRequest::Status);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut flow_control = LocalWebrtcFlowControl::default();

        let failure = runtime.block_on(async {
            let delivery = send_response_frames(
                &data_channel,
                &key,
                &frames,
                &mut pending,
                &mut flow_control,
                peer_state.as_ref(),
            );
            tokio::pin!(delivery);
            assert!(
                timeout(
                    webrtc_runtime().as_ref(),
                    Duration::from_millis(20),
                    delivery.as_mut(),
                )
                .await
                .is_err()
            );
            peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::PeerDisconnected);
            delivery
                .await
                .expect_err("peer terminal must retain partial progress")
        });
        assert_eq!(failure.cause, LocalWebrtcTerminalCause::PeerDisconnected);
        assert_eq!(failure.message_id, "response-progress");
        assert_eq!(failure.next_chunk_index, 1);
        assert_eq!(failure.total_chunks, frames.len());

        runtime.block_on(close_data_channel(
            &data_channel,
            &key,
            &mut pending,
            peer_state.as_ref(),
            failure.cause,
        ));
        let ControlMessage::LocalWebrtcPeerClosed {
            terminal_record, ..
        } = receive_test_runtime_message(&mut runtime_rx)
        else {
            panic!("expected terminal record after partial response");
        };
        assert_eq!(
            terminal_record.message_id.as_deref(),
            Some("response-progress")
        );
        assert_eq!(terminal_record.next_chunk_index, 1);
        assert_eq!(terminal_record.last_sent_chunk_index, Some(0));
        assert_eq!(terminal_record.total_chunks, frames.len());
        assert!(terminal_record.pressured);
    }
    #[test]
    fn high_then_low_water_resumes_and_completes_response_in_order() {
        let data_channel = FakeDataChannel::default();
        data_channel.events.lock().unwrap().extend([
            DataChannelEvent::OnBufferedAmountHigh,
            DataChannelEvent::OnBufferedAmountLow,
        ]);
        let key = AesGcmKey::from_slice(&[6; 32]).unwrap();
        let mut pending = VecDeque::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let peer_state = test_peer_state("grant-high-low");

        let completed = runtime.block_on(send_response_frames(
            &data_channel,
            &key,
            &["first".to_string(), "second".to_string()],
            &mut pending,
            &mut flow_control,
            &peer_state,
        ));

        assert!(completed.is_ok());
        assert_eq!(
            data_channel.sent.lock().unwrap().as_slice(),
            &["first", "second"]
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn one_lifecycle_event_precedes_queued_control_work_per_turn() {
        let data_channel = FakeDataChannel::default();
        let key = AesGcmKey::from_slice(&[19; 32]).unwrap();
        let peer_state = test_peer_state("grant-lifecycle-fairness");
        peer_state
            .mux
            .push_host_event(DaemonEvent::RuntimeObservation {
                kind: "first-lifecycle".to_string(),
            });
        peer_state
            .mux
            .push_host_event(DaemonEvent::RuntimeObservation {
                kind: "second-lifecycle".to_string(),
            });
        let mut pending = VecDeque::from([PendingLocalWebrtcRequest::Request {
            request_id: "1".to_string(),
            request: Box::new(DaemonRequest::Status),
        }]);
        let mut flow_control = LocalWebrtcFlowControl::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime
            .block_on(flush_ready_webrtc_host_control(
                &data_channel,
                &key,
                &peer_state,
                &mut pending,
                &mut flow_control,
            ))
            .expect("lifecycle event write");

        assert_eq!(pending.len(), 1, "the writer does not consume control work");
        assert!(
            peer_state.mux.has_pending_event(),
            "one event remains because one turn writes at most one lifecycle event"
        );
        assert!(!data_channel.sent.lock().unwrap().is_empty());
    }

    #[test]
    fn post_final_high_water_survives_response_boundary_and_idle_low_clears_it() {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountHigh);
        let key = AesGcmKey::from_slice(&[12; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-response-boundary");

        let first = runtime.block_on(send_response_frames(
            &data_channel,
            &key,
            &["response-one".to_string()],
            &mut pending,
            &mut flow_control,
            &peer_state,
        ));
        assert!(first.is_ok());
        assert!(flow_control.pressured);

        assert!(
            apply_data_channel_event(
                DataChannelEvent::OnBufferedAmountLow,
                &key,
                &mut pending,
                &mut flow_control,
                &WebRtcConnectionMux::new(),
            )
            .is_ok()
        );
        let second = runtime.block_on(send_response_frames(
            &data_channel,
            &key,
            &["response-two".to_string()],
            &mut pending,
            &mut flow_control,
            &peer_state,
        ));

        assert!(second.is_ok());
        assert!(!flow_control.pressured);
        assert_eq!(
            data_channel.sent.lock().unwrap().as_slice(),
            &["response-one", "response-two"]
        );
    }

    #[test]
    fn next_response_waits_for_low_water_when_pressure_blocks_its_first_frame() {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountHigh);
        let key = AesGcmKey::from_slice(&[13; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-next-response");

        assert!(
            runtime
                .block_on(send_response_frames(
                    &data_channel,
                    &key,
                    &["response-one".to_string()],
                    &mut pending,
                    &mut flow_control,
                    &peer_state,
                ))
                .is_ok()
        );
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountLow);
        runtime
            .block_on(send_response_frames(
                &data_channel,
                &key,
                &["response-two".to_string()],
                &mut pending,
                &mut flow_control,
                &peer_state,
            ))
            .expect("low water must resume the pressured next response");

        assert!(!flow_control.pressured);
        assert_eq!(
            data_channel.sent.lock().unwrap().as_slice(),
            &["response-one", "response-two"]
        );
    }

    #[test]
    fn send_failures_report_distinct_bounded_terminal_causes() {
        let key = AesGcmKey::from_slice(&[14; 32]).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-send-failures");

        for (event, expected_cause) in [
            (
                DataChannelEvent::OnClose,
                LocalWebrtcTerminalCause::ChannelClosed,
            ),
            (
                DataChannelEvent::OnClosing,
                LocalWebrtcTerminalCause::ChannelClosed,
            ),
            (
                DataChannelEvent::OnError,
                LocalWebrtcTerminalCause::ChannelError,
            ),
        ] {
            let data_channel = FakeDataChannel::default();
            data_channel.events.lock().unwrap().push_back(event);
            let mut pending = VecDeque::new();
            let mut flow_control = LocalWebrtcFlowControl::default();
            let failure = runtime
                .block_on(send_response_frames(
                    &data_channel,
                    &key,
                    &["response".to_string()],
                    &mut pending,
                    &mut flow_control,
                    &peer_state,
                ))
                .expect_err("terminal channel event must fail response delivery");
            assert_eq!(failure.cause, expected_cause);
            assert!(
                failure.next_chunk_index <= 1,
                "terminal event must fail the in-flight or next chunk: {failure:?}"
            );
            assert_eq!(failure.total_chunks, 1);
        }

        let ended_channel = FakeDataChannel::default();
        ended_channel.poll_ends.store(true, Ordering::Release);
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let ended = runtime
            .block_on(send_response_frames(
                &ended_channel,
                &key,
                &["response".to_string()],
                &mut pending,
                &mut flow_control,
                &peer_state,
            ))
            .expect_err("ended polling must fail response delivery");
        assert_eq!(ended.cause, LocalWebrtcTerminalCause::PollEnded);

        let failed_channel = FakeDataChannel::default();
        failed_channel.send_fails.store(true, Ordering::Release);
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let send = runtime
            .block_on(send_response_frames(
                &failed_channel,
                &key,
                &["response".to_string()],
                &mut pending,
                &mut flow_control,
                &peer_state,
            ))
            .expect_err("send_text failure must fail response delivery");
        assert_eq!(send.cause, LocalWebrtcTerminalCause::SendText);
        assert_eq!(send.next_chunk_index, 0);
        assert_eq!(send.last_sent_chunk_index, None);
    }

    #[test]
    fn hung_send_text_fails_when_peer_terminal_arrives() {
        let data_channel = FakeDataChannel::default();
        data_channel.send_hangs.store(true, Ordering::Release);
        let key = AesGcmKey::from_slice(&[16; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-hung-send-terminal");

        let frames = ["response".to_string()];
        let failure = runtime
            .block_on(async {
                let send = send_response_frames(
                    &data_channel,
                    &key,
                    &frames,
                    &mut pending,
                    &mut flow_control,
                    &peer_state,
                );
                tokio::pin!(send);
                tokio::select! {
                    result = &mut send => result,
                    () = async {
                        tokio::task::yield_now().await;
                        peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::PeerClosed);
                        std::future::pending::<()>().await;
                    } => unreachable!("peer terminal must abort the hung send"),
                }
            })
            .expect_err("peer terminal must abort a hung send_text");
        assert_eq!(failure.cause, LocalWebrtcTerminalCause::PeerClosed);
        assert_eq!(failure.next_chunk_index, 0);
        assert!(data_channel.sent.lock().unwrap().is_empty());
    }
    #[test]
    fn nonterminal_channel_event_does_not_drop_in_flight_send() {
        let data_channel = FakeDataChannel::default();
        data_channel.send_hangs.store(true, Ordering::Release);
        let key = AesGcmKey::from_slice(&[18; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-keep-in-flight-send");
        let frames = ["keep-me".to_string()];
        runtime
            .block_on(async {
                let send = send_response_frames(
                    &data_channel,
                    &key,
                    &frames,
                    &mut pending,
                    &mut flow_control,
                    &peer_state,
                );
                tokio::pin!(send);
                tokio::select! {
                    result = &mut send => result,
                    () = async {
                        tokio::task::yield_now().await;
                        data_channel.push_event(DataChannelEvent::OnBufferedAmountHigh);
                        tokio::task::yield_now().await;
                        data_channel.release_hung_send();
                        std::future::pending::<()>().await;
                    } => unreachable!("in-flight send must complete after a nonterminal event"),
                }
            })
            .expect("high-water during send must not drop the frame");
        assert_eq!(data_channel.sent.lock().unwrap().as_slice(), &["keep-me"]);
        assert!(flow_control.pressured);
    }

    #[test]
    fn ready_send_completes_before_queued_on_close() {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnClose);
        let key = AesGcmKey::from_slice(&[19; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-send-first");
        let failure = runtime
            .block_on(send_response_frames(
                &data_channel,
                &key,
                &["must-send".to_string()],
                &mut pending,
                &mut flow_control,
                &peer_state,
            ))
            .expect_err("queued OnClose must still fail after the ready send");
        assert_eq!(failure.cause, LocalWebrtcTerminalCause::ChannelClosed);
        assert_eq!(failure.next_chunk_index, 1);
        assert_eq!(data_channel.sent.lock().unwrap().as_slice(), &["must-send"]);
    }

    #[test]
    fn idle_open_channel_does_not_wait_between_response_frames() {
        let data_channel = FakeDataChannel::default();
        let key = AesGcmKey::from_slice(&[10; 32]).unwrap();
        let mut pending = VecDeque::new();
        let frames = (0..20)
            .map(|index| format!("frame-{index}"))
            .collect::<Vec<_>>();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let peer_state = test_peer_state("grant-idle-open");

        let (elapsed, completed) = runtime.block_on(async {
            let started = tokio::time::Instant::now();
            let completed = send_response_frames(
                &data_channel,
                &key,
                &frames,
                &mut pending,
                &mut flow_control,
                &peer_state,
            )
            .await;
            (started.elapsed(), completed)
        });

        assert!(completed.is_ok());
        assert_eq!(data_channel.sent.lock().unwrap().len(), frames.len());
        assert!(
            elapsed.is_zero(),
            "idle event probes must not throttle response frames: {:?}",
            elapsed
        );
    }

    #[test]
    fn inbound_request_during_response_is_retained_for_fifo_processing() {
        let data_channel = FakeDataChannel::default();
        let key = AesGcmKey::from_slice(&[7; 32]).unwrap();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(encrypted_request_event(&key, &DaemonRequest::Status));
        let mut pending = VecDeque::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut flow_control = LocalWebrtcFlowControl {
            hello_accepted: true,
            ..LocalWebrtcFlowControl::default()
        };
        let peer_state = test_peer_state("grant-inbound-request");

        let completed = runtime.block_on(send_response_frames(
            &data_channel,
            &key,
            &["first".to_string(), "second".to_string()],
            &mut pending,
            &mut flow_control,
            &peer_state,
        ));

        assert!(completed.is_ok());
        assert_eq!(data_channel.sent.lock().unwrap().len(), 2);
        assert!(matches!(
            pending.pop_front(),
            Some(PendingLocalWebrtcRequest::Request { request, .. })
                if *request == DaemonRequest::Status
        ));
        assert!(pending.is_empty());
    }

    #[test]
    fn overflowing_requests_each_preserve_one_fifo_operator_response() {
        let key = AesGcmKey::from_slice(&[8; 32]).unwrap();
        reset_test_request_ids();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl {
            hello_accepted: true,
            ..LocalWebrtcFlowControl::default()
        };

        let inbound_requests = LOCAL_WEBRTC_PENDING_REQUESTS + 4;
        for _ in 0..inbound_requests {
            assert!(
                apply_data_channel_event(
                    encrypted_request_event(&key, &DaemonRequest::Status),
                    &key,
                    &mut pending,
                    &mut flow_control,
                    &WebRtcConnectionMux::new(),
                )
                .is_ok()
            );
        }

        assert_eq!(pending.len(), inbound_requests);
        let rejected = pending
            .iter()
            .filter(|queued| matches!(queued, PendingLocalWebrtcRequest::TooManyRequests { .. }))
            .count();
        assert_eq!(
            rejected, 4,
            "every request past the limit keeps its own rejection"
        );
        assert!(matches!(
            pending.back(),
            Some(PendingLocalWebrtcRequest::TooManyRequests { request_id, operation })
                if request_id == &inbound_requests.to_string() && *operation == "status"
        ));
        let mut responses_emitted = 0;
        while pop_pending_request(&mut pending).is_some() {
            responses_emitted += 1;
        }
        assert_eq!(responses_emitted, inbound_requests);
        let response = too_many_requests_response("33", "status");
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        let error = response.error.expect("correlated operator error");
        assert_eq!(error.code, OPERATOR_ERROR_TOO_MANY_REQUESTS);
        assert_eq!(error.request_id, "33");
        assert_eq!(error.operation, "status");
    }

    #[test]
    fn interleaved_overflow_runs_preserve_fifo_response_order() {
        let key = AesGcmKey::from_slice(&[11; 32]).unwrap();
        reset_test_request_ids();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl {
            hello_accepted: true,
            ..LocalWebrtcFlowControl::default()
        };
        {
            let mut apply_request = |request: &DaemonRequest| {
                apply_data_channel_event(
                    encrypted_request_event(&key, request),
                    &key,
                    &mut pending,
                    &mut flow_control,
                    &WebRtcConnectionMux::new(),
                )
            };

            for _ in 0..LOCAL_WEBRTC_PENDING_REQUESTS {
                assert!(apply_request(&DaemonRequest::Status).is_ok());
            }
            assert!(apply_request(&DaemonRequest::Status).is_ok());
        }
        assert!(matches!(
            pop_pending_request(&mut pending),
            Some(PendingLocalWebrtcRequest::Request { request, .. })
                if *request == DaemonRequest::Status
        ));

        assert!(
            apply_data_channel_event(
                encrypted_request_event(&key, &DaemonRequest::ListSessions),
                &key,
                &mut pending,
                &mut flow_control,
                &WebRtcConnectionMux::new(),
            )
            .is_ok()
        );
        assert!(
            apply_data_channel_event(
                encrypted_request_event(&key, &DaemonRequest::Status),
                &key,
                &mut pending,
                &mut flow_control,
                &WebRtcConnectionMux::new(),
            )
            .is_ok()
        );

        let emitted_order = std::iter::from_fn(|| pop_pending_request(&mut pending))
            .map(|pending| match pending {
                PendingLocalWebrtcRequest::Request { request, .. }
                    if *request == DaemonRequest::ListSessions =>
                {
                    "new-request"
                }
                PendingLocalWebrtcRequest::Request { .. } => "status",
                PendingLocalWebrtcRequest::Hello(_) => "hello",
                PendingLocalWebrtcRequest::TooManyRequests { .. } => "overflow",
            })
            .collect::<Vec<_>>();
        let mut expected_order = vec!["status"; LOCAL_WEBRTC_PENDING_REQUESTS - 1];
        expected_order.extend(["overflow", "new-request", "overflow"]);
        assert_eq!(emitted_order, expected_order);
    }
}
