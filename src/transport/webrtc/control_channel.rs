use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use botster_core::{AesGcmEnvelope, AesGcmKey, decrypt_aes_gcm};
use botster_hub_client::{
    DaemonCompatibility, DaemonDiagnostic, DaemonEntityFrame, DaemonEvent, DaemonHello,
    DaemonHelloAck, DaemonLocalWebrtcDeliveryChunk, DaemonRequest, DaemonResponse,
    LOCAL_WEBRTC_MAX_FRAME_BYTES, PROTOCOL,
};
use serde_json::Value;
use tokio::sync::{mpsc as tokio_mpsc, oneshot, watch};
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::runtime::timeout;

use botster_terminal_protocol::{
    TerminalCompatibility, ensure_compatible as ensure_terminal_compatible,
};

use crate::admission::unix_hello::WebrtcTerminalAdmission;
use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::subscription::attach_routes::hello_requires_terminal_subscription_closed;
use crate::subscription::entity::EntityFrameSender;
use crate::transport::webrtc::adapter::WebRtcConnectionMux;
use crate::transport::webrtc::delivery::{
    LocalWebrtcSendFailure, framed_daemon_entity_frame, framed_daemon_event,
    framed_daemon_hello_ack, framed_daemon_response,
};
use crate::transport::webrtc::peer::{
    LOCAL_WEBRTC_PEER_CLOSE_BOUND, LocalWebrtcPeerState, LocalWebrtcTerminalCause,
    TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV, webrtc_runtime,
};
use crate::transport::webrtc::subscription_channel::{
    entity_frame_subscription_id, local_webrtc_attach_change_for_response,
};
pub(crate) const LOCAL_WEBRTC_PENDING_REQUESTS: usize = 16;
pub(crate) const LOCAL_WEBRTC_EVENT_PROBE: Duration = Duration::ZERO;
pub(crate) const LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW: u32 = LOCAL_WEBRTC_MAX_FRAME_BYTES as u32;
pub(crate) const LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH: u32 = (LOCAL_WEBRTC_MAX_FRAME_BYTES * 2) as u32;
#[async_trait]
pub(crate) trait LocalWebrtcDataChannel: Send + Sync {
    async fn local_set_buffered_amount_low_threshold(&self, threshold: u32) -> Result<(), String>;
    async fn local_set_buffered_amount_high_threshold(&self, threshold: u32) -> Result<(), String>;
    async fn local_send_text(&self, text: &str) -> Result<(), String>;
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

    async fn local_send_text(&self, text: &str) -> Result<(), String> {
        self.send_text(text)
            .await
            .map_err(|error| error.to_string())
    }

    async fn local_poll(&self) -> Option<DataChannelEvent> {
        self.poll().await
    }

    async fn local_close(&self) -> Result<(), String> {
        self.close().await.map_err(|error| error.to_string())
    }
}
pub(crate) enum PendingLocalWebrtcRequest {
    Request(Box<DaemonRequest>),
    Hello(Box<DaemonHello>),
    EntityFrame(Box<DaemonEntityFrame>),
    QueueOverflow(usize),
}

pub(crate) enum LocalWebrtcInbound {
    Channel(Result<Option<DataChannelEvent>, LocalWebrtcTerminalCause>),
    Entity(DaemonEntityFrame),
    AdapterReady,
    HostEventReady,
}

pub(crate) enum DataChannelPlaintext {
    Hello(Box<DaemonHello>),
    Request(Box<DaemonRequest>),
}

#[derive(Debug, Default)]
pub(crate) struct LocalWebrtcFlowControl {
    pub(crate) pressured: bool,
}
pub(crate) fn pop_pending_request(
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
) -> Option<PendingLocalWebrtcRequest> {
    let pending = pending_requests.pop_front()?;
    let PendingLocalWebrtcRequest::QueueOverflow(count) = pending else {
        return Some(pending);
    };
    if count > 1 {
        pending_requests.push_front(PendingLocalWebrtcRequest::QueueOverflow(count - 1));
    }
    Some(PendingLocalWebrtcRequest::QueueOverflow(1))
}
pub(crate) fn local_webrtc_request_operation(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Status => "status",
        DaemonRequest::Spawn { .. } => "spawn",
        DaemonRequest::Attach { .. } => "attach",
        DaemonRequest::Drain { .. } => "drain",
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
    entity_frame_tx: tokio_mpsc::Sender<DaemonEntityFrame>,
    mut entity_frame_rx: tokio_mpsc::Receiver<DaemonEntityFrame>,
) -> Option<LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    let mut pending_requests = VecDeque::new();
    let mut flow_control = LocalWebrtcFlowControl::default();
    let mut send_failure = None;
    let mut terminal_cause = LocalWebrtcTerminalCause::PollEnded;
    let mut peer_terminal_rx = peer_state.subscribe_peer_terminal();
    let mut last_host_class = None;
    let mut pending_entity = None;
    let mut open = true;
    while open {
        if let Err(failure) = flush_ready_webrtc_host_control(
            data_channel,
            stream_key,
            peer_state,
            &mut pending_requests,
            &mut flow_control,
            &mut entity_frame_rx,
            &mut pending_entity,
            &mut last_host_class,
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
            let mailbox = peer_state.event_plane.mailbox(&peer_state.grant_id);
            if pending_entity.is_some()
                || host_event_ready(peer_state)
                || mailbox.as_ref().is_some_and(|mailbox| mailbox.take_wake())
            {
                continue;
            }
            let inbound = tokio::select! {
                biased;
                channel = poll_data_channel_or_peer_terminal(data_channel, &mut peer_terminal_rx) => {
                    LocalWebrtcInbound::Channel(channel)
                }
                frame = entity_frame_rx.recv() => {
                    LocalWebrtcInbound::Entity(
                        frame.expect("local WebRTC peer owns its entity subscription sender")
                    )
                }
                _ = async {
                    if let Some(mailbox) = mailbox.as_ref() {
                        let notified = mailbox.notify().notified();
                        tokio::pin!(notified);
                        if mailbox.take_wake() || mailbox.has_ready_event() {
                            return;
                        }
                        notified.await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => LocalWebrtcInbound::HostEventReady,
                _ = peer_state.mux.wait_for_write() => {
                    LocalWebrtcInbound::AdapterReady
                }
            };
            match inbound {
                LocalWebrtcInbound::Entity(frame) => {
                    if !peer_state.owns_entity_subscription(entity_frame_subscription_id(&frame)) {
                        continue;
                    }
                    PendingLocalWebrtcRequest::EntityFrame(Box::new(frame))
                }
                LocalWebrtcInbound::Channel(Err(cause)) => {
                    terminal_cause = cause;
                    break;
                }
                LocalWebrtcInbound::HostEventReady => continue,
                LocalWebrtcInbound::AdapterReady => continue,
                LocalWebrtcInbound::Channel(Ok(Some(DataChannelEvent::OnMessage(message)))) => {
                    match decrypt_data_channel_plaintext(stream_key, message.data.as_ref()) {
                        Some(DataChannelPlaintext::Hello(hello)) => {
                            PendingLocalWebrtcRequest::Hello(hello)
                        }
                        Some(DataChannelPlaintext::Request(request)) => {
                            PendingLocalWebrtcRequest::Request(request)
                        }
                        None => {
                            terminal_cause = LocalWebrtcTerminalCause::InvalidEncryptedRequest;
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

        if let PendingLocalWebrtcRequest::Hello(hello) = &pending {
            if hello.protocol != PROTOCOL {
                terminal_cause = LocalWebrtcTerminalCause::InvalidRequest;
                break;
            }
            if !peer_state.cleanup_sent.load(Ordering::Acquire) {
                let admission = if let Some(requirement) = hello.terminal_compatibility.as_ref()
                    && let Err(error) =
                        ensure_terminal_compatible(requirement, &TerminalCompatibility::current())
                {
                    WebrtcTerminalAdmission::Rejected {
                        code: "terminal_compatibility",
                        diagnostic: DaemonDiagnostic::compatibility_mismatch(error.diagnostic),
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
            };
            let Ok(frames) = framed_daemon_hello_ack(stream_key, &ack) else {
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
                Ok(()) => continue,
                Err(failure) => {
                    eprintln!("{failure}");
                    terminal_cause = failure.cause;
                    send_failure = Some(failure);
                    break;
                }
            }
        }

        if let PendingLocalWebrtcRequest::EntityFrame(frame) = &pending {
            peer_state.begin_operation("entity_delivery");
            let Ok(frames) = framed_daemon_entity_frame(stream_key, frame) else {
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
                Ok(()) => continue,
                Err(failure) => {
                    eprintln!("{failure}");
                    terminal_cause = failure.cause;
                    send_failure = Some(failure);
                    break;
                }
            }
        }

        let request = match pending {
            PendingLocalWebrtcRequest::Request(request) => request,
            PendingLocalWebrtcRequest::Hello(_) | PendingLocalWebrtcRequest::EntityFrame(_) => {
                unreachable!("hello and entity frame handled above")
            }
            PendingLocalWebrtcRequest::QueueOverflow(_) => {
                peer_state.begin_overflow_response();
                let response = queued_request_overflow_response();
                let Ok(frames) = framed_daemon_response(stream_key, &response) else {
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
        };

        peer_state.begin_request(&request);
        if std::env::var(TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV).as_deref()
            == Ok(local_webrtc_request_operation(&request))
        {
            let _ = data_channel.local_close().await;
            terminal_cause = LocalWebrtcTerminalCause::ChannelClosed;
            break;
        }
        let ownership_request = request.as_ref().clone();
        let entity_subscription_change = match request.as_ref() {
            DaemonRequest::SubscribeEntities {
                subscription_id, ..
            } => Some((true, subscription_id.clone())),
            DaemonRequest::UnsubscribeEntities { subscription_id } => {
                Some((false, subscription_id.clone()))
            }
            _ => None,
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        let (response_delivery_tx, response_delivery_rx) =
            if matches!(*request, DaemonRequest::DaemonShutdown) {
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
                runtime_tx
                    .send(ControlMessage::SubscribeEntities {
                        entity_type,
                        subscription_id,
                        frame_tx: EntityFrameSender::Async(entity_frame_tx.clone()),
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
        let response = match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
            Ok(Ok(Ok(response))) => response,
            Ok(Ok(Err(error))) => response_with_diagnostic(DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                error.to_string(),
            )),
            Ok(Err(_)) => response_with_diagnostic(DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                "runtime reply channel closed",
            )),
            Err(_) => response_with_diagnostic(DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                "runtime request timed out",
            )),
        };
        // OperatorError and Attach attach_failed create no ownership. Drain attach_failed
        // releases any pending route so PeerClosed does not send Detach for a failed attach.
        peer_state.apply_subscription_change(local_webrtc_attach_change_for_response(
            &ownership_request,
            &response,
        ));
        if let Some((subscribed, subscription_id)) = entity_subscription_change {
            if subscribed
                && response.kind == botster_hub_client::DaemonResponseKind::EntitySubscribed
            {
                peer_state.add_entity_subscription(subscription_id);
            } else if !subscribed
                && response.kind == botster_hub_client::DaemonResponseKind::EntityUnsubscribed
            {
                peer_state.remove_entity_subscription(&subscription_id);
            }
        }
        let Ok(frames) = framed_daemon_response(stream_key, &response) else {
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
        if let Some(response_delivery_tx) = response_delivery_tx {
            let _ = response_delivery_tx.send(());
        }
        match delivery {
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
        &mut pending_requests,
        peer_state,
        terminal_cause,
    )
    .await;
    send_failure
}

pub(crate) async fn close_data_channel<D>(
    data_channel: &D,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    peer_state: &LocalWebrtcPeerState,
    cause: LocalWebrtcTerminalCause,
) where
    D: LocalWebrtcDataChannel + ?Sized,
{
    pending_requests.clear();
    peer_state.mux.close_all();
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
            let pending = match decrypt_data_channel_plaintext(stream_key, message.data.as_ref()) {
                Some(DataChannelPlaintext::Hello(hello)) => PendingLocalWebrtcRequest::Hello(hello),
                Some(DataChannelPlaintext::Request(request)) => {
                    PendingLocalWebrtcRequest::Request(request)
                }
                None => return Err(LocalWebrtcTerminalCause::InvalidRequest),
            };
            let request_count = pending_requests
                .iter()
                .filter(|queued| {
                    matches!(
                        queued,
                        PendingLocalWebrtcRequest::Request(_) | PendingLocalWebrtcRequest::Hello(_)
                    )
                })
                .count();
            if request_count >= LOCAL_WEBRTC_PENDING_REQUESTS {
                if let Some(PendingLocalWebrtcRequest::QueueOverflow(count)) =
                    pending_requests.back_mut()
                {
                    let Some(next_count) = count.checked_add(1) else {
                        return Err(LocalWebrtcTerminalCause::RequestQueueOverflow);
                    };
                    *count = next_count;
                } else {
                    pending_requests.push_back(PendingLocalWebrtcRequest::QueueOverflow(1));
                }
                return Ok(());
            }
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
pub(crate) fn decrypt_data_channel_plaintext(
    key: &AesGcmKey,
    bytes: &[u8],
) -> Option<DataChannelPlaintext> {
    let envelope = serde_json::from_slice::<AesGcmEnvelope>(bytes).ok()?;
    let plaintext = decrypt_aes_gcm(key, &envelope).ok()?;
    let value = serde_json::from_slice::<Value>(&plaintext).ok()?;
    if value.get("type").is_none() && value.get("protocol").is_some() {
        serde_json::from_value::<DaemonHello>(value)
            .ok()
            .map(Box::new)
            .map(DataChannelPlaintext::Hello)
    } else {
        serde_json::from_value::<DaemonRequest>(value)
            .ok()
            .map(Box::new)
            .map(DataChannelPlaintext::Request)
    }
}
pub(crate) fn host_event_ready(peer_state: &LocalWebrtcPeerState) -> bool {
    peer_state.mux.has_pending_event()
        || peer_state
            .event_plane
            .mailbox(&peer_state.grant_id)
            .is_some_and(|mailbox| mailbox.has_ready_event())
}

pub(crate) fn take_host_event(peer_state: &LocalWebrtcPeerState) -> Option<DaemonEvent> {
    if let Some(event) = peer_state.mux.pop_pending_event() {
        return Some(event);
    }
    peer_state
        .event_plane
        .mailbox(&peer_state.grant_id)
        .and_then(|mailbox| mailbox.take_ready_event())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn flush_ready_webrtc_host_control<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
    entity_frame_rx: &mut tokio_mpsc::Receiver<DaemonEntityFrame>,
    pending_entity: &mut Option<DaemonEntityFrame>,
    last_host_class: &mut Option<crate::host_control_fair_write::HostControlClass>,
) -> Result<(), LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    use crate::host_control_fair_write::{
        HostControlClass, MAX_HOST_FRAMES_PER_FLUSH_TURN, next_ready_host_control_class,
    };

    let mut host_frames = 0;
    loop {
        if pending_entity.is_none()
            && let Ok(frame) = entity_frame_rx.try_recv()
        {
            *pending_entity = Some(frame);
        }
        let entity_ready = pending_entity.as_ref().is_some_and(|frame| {
            peer_state.owns_entity_subscription(entity_frame_subscription_id(frame))
        });
        if pending_entity.is_some() && !entity_ready {
            *pending_entity = None;
            continue;
        }
        let control_ready = webrtc_control_request_ready(pending_requests);
        match next_ready_host_control_class(
            *last_host_class,
            control_ready,
            entity_ready,
            host_event_ready(peer_state),
        ) {
            Some(HostControlClass::Control) => return Ok(()),
            Some(HostControlClass::Entity) => {
                if host_frames >= MAX_HOST_FRAMES_PER_FLUSH_TURN {
                    return Ok(());
                }
                let Some(frame) = pending_entity.take() else {
                    return Ok(());
                };
                *last_host_class = Some(HostControlClass::Entity);
                let frames = match framed_daemon_entity_frame(stream_key, &frame) {
                    Ok(frames) => frames,
                    Err(_) => continue,
                };
                send_response_frames(
                    data_channel,
                    stream_key,
                    &frames,
                    pending_requests,
                    flow_control,
                    peer_state,
                )
                .await?;
                host_frames += 1;
            }
            Some(HostControlClass::Event) => {
                if host_frames >= MAX_HOST_FRAMES_PER_FLUSH_TURN {
                    return Ok(());
                }
                let Some(event) = take_host_event(peer_state) else {
                    return Ok(());
                };
                if matches!(event, DaemonEvent::TerminalSubscriptionClosed { .. })
                    && !peer_state.mux.close_events_admitted()
                {
                    continue;
                }
                *last_host_class = Some(HostControlClass::Event);
                peer_state.begin_operation("host_event_delivery");
                let frames = match framed_daemon_event(stream_key, &event) {
                    Ok(frames) => frames,
                    Err(_) => continue,
                };
                send_response_frames(
                    data_channel,
                    stream_key,
                    &frames,
                    pending_requests,
                    flow_control,
                    peer_state,
                )
                .await?;
                host_frames += 1;
            }
            None => return Ok(()),
        }
    }
}

pub(crate) fn webrtc_control_request_ready(
    pending_requests: &VecDeque<PendingLocalWebrtcRequest>,
) -> bool {
    pending_requests.iter().any(|pending| {
        matches!(
            pending,
            PendingLocalWebrtcRequest::Request(_)
                | PendingLocalWebrtcRequest::Hello(_)
                | PendingLocalWebrtcRequest::QueueOverflow(_)
        )
    })
}

#[allow(dead_code)]
pub(crate) async fn flush_webrtc_host_events<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
) -> Result<(), LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    if !peer_state.mux.close_events_admitted() {
        peer_state.mux.drop_pending_events();
        return Ok(());
    }
    while let Some(event) = peer_state.mux.pop_pending_event() {
        peer_state.begin_operation("host_event_delivery");
        let frames = match framed_daemon_event(stream_key, &event) {
            Ok(frames) => frames,
            Err(_) => continue,
        };
        send_response_frames(
            data_channel,
            stream_key,
            &frames,
            pending_requests,
            flow_control,
            peer_state,
        )
        .await?;
    }
    Ok(())
}
pub(crate) fn response_with_diagnostic(diagnostic: DaemonDiagnostic) -> DaemonResponse {
    DaemonResponse {
        kind: botster_hub_client::DaemonResponseKind::OperatorError,
        status: None,
        sessions: Vec::new(),
        session_types: Vec::new(),
        session_type_definition: None,
        resolved_session_type: None,
        session_context: None,
        read_screen: None,
        mode_flags: None,
        mode_gated_input: None,
        terminal_reservation: None,
        capture_snapshot: None,
        spawn_targets: Vec::new(),
        spawn_target_validation: None,
        worktrees: Vec::new(),
        apps: Vec::new(),
        resolved_app_launch: None,
        resolved_package_route: None,
        package_navigation: Vec::new(),
        packages: Vec::new(),
        available_packages: Vec::new(),
        install_plan: None,
        update_status: None,
        hub_update: None,
        hub_update_execution: None,
        package_decision: None,
        lifecycle: Vec::new(),
        plugin_worker_counters: None,
        plugin_resource_counters: None,
        plugin_tools: Vec::new(),
        plugin_tool_result: Value::Null,
        plugin_surface: None,
        plugin_action_result: None,
        local_webrtc_bootstrap: None,
        local_webrtc_answer: None,
        events: Vec::new(),
        cleanup: None,
        coordination: None,
        error: None,
        diagnostics: vec![diagnostic],
    }
}

pub(crate) fn queued_request_overflow_response() -> DaemonResponse {
    response_with_diagnostic(DaemonDiagnostic::action_failure(
        "local_webrtc_data_channel",
        "inbound request queue capacity exceeded; request was rejected",
    ))
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
    use botster_hub_client::DaemonLocalWebrtcDeliveryKind;
    use botster_hub_client::{
        DaemonDiagnostic, DaemonEntityFrame, DaemonHello, DaemonRequest, DaemonResponse,
        LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
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
    fn run_idle_pressure_case(
        terminal_cause: Option<LocalWebrtcTerminalCause>,
    ) -> (FakeDataChannel, Option<LocalWebrtcSendFailure>) {
        let key = AesGcmKey::from_slice(&[15; 32]).unwrap();
        let data_channel = FakeDataChannel::default();
        {
            let mut events = data_channel.events.lock().unwrap();
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
        let (entity_frame_tx, entity_frame_rx) =
            tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let failure = runtime.block_on(async {
            let delivery = run_data_channel(
                &data_channel,
                &key,
                peer_state.as_ref(),
                &runtime_sender,
                entity_frame_tx,
                entity_frame_rx,
            );
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

    fn run_shutdown_response_delivery_case(
        send_fails: bool,
    ) -> (FakeDataChannel, Option<LocalWebrtcSendFailure>) {
        let key = AesGcmKey::from_slice(&[16; 32]).unwrap();
        let data_channel = FakeDataChannel::default();
        data_channel.send_fails.store(send_fails, Ordering::Release);
        {
            let mut events = data_channel.events.lock().unwrap();
            events.push_back(encrypted_request_event(
                &key,
                &DaemonRequest::DaemonShutdown,
            ));
        }
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
        let (entity_frame_tx, entity_frame_rx) =
            tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let failure = runtime.block_on(run_data_channel(
            &data_channel,
            &key,
            peer_state.as_ref(),
            &runtime_sender,
            entity_frame_tx,
            entity_frame_rx,
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
    fn entity_subscription_multiplexes_after_ack_and_cleans_up_with_peer() {
        let key = AesGcmKey::from_slice(&[21; 32]).unwrap();
        let data_channel = Arc::new(FakeDataChannel::default());
        {
            let mut events = data_channel.events.lock().unwrap();
            events.push_back(encrypted_request_event(
                &key,
                &DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "entity-fixture".to_string(),
                },
            ));
        }
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-entity-fixture".to_string(),
            runtime_tx,
        ));
        let responder_peer_state = peer_state.clone();
        let responder_data_channel = data_channel.clone();
        let responder_key = key.clone();
        let responder = std::thread::spawn(move || {
            let ControlMessage::SubscribeEntities {
                entity_type,
                subscription_id,
                frame_tx,
                reply_tx,
                grant_id,
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected WebRTC entity subscription registration");
            };
            assert_eq!(entity_type, "session");
            assert_eq!(subscription_id, "entity-fixture");
            assert_eq!(grant_id.as_deref(), Some("grant-entity-fixture"));
            let mut subscribed = response_with_diagnostic(DaemonDiagnostic::connected("fixture"));
            subscribed.kind = botster_hub_client::DaemonResponseKind::EntitySubscribed;
            reply_tx.send(Ok(subscribed)).unwrap();
            frame_tx
                .try_send(DaemonEntityFrame::Snapshot {
                    subscription_id: "entity-fixture".to_string(),
                    entity_type: "session".to_string(),
                    snapshot_seq: 1,
                    items: Vec::new(),
                    resync_reason: None,
                })
                .unwrap();
            frame_tx
                .try_send(DaemonEntityFrame::Snapshot {
                    subscription_id: "entity-fixture".to_string(),
                    entity_type: "session".to_string(),
                    snapshot_seq: 2,
                    items: Vec::new(),
                    resync_reason: Some("subscriber_overflow".to_string()),
                })
                .unwrap();

            let deadline = Instant::now() + Duration::from_secs(1);
            while responder_data_channel.sent.lock().unwrap().len() < 3 {
                assert!(
                    Instant::now() < deadline,
                    "subscribe ack and encrypted overflow recovery must complete"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            responder_data_channel
                .events
                .lock()
                .unwrap()
                .push_back(encrypted_request_event(
                    &responder_key,
                    &DaemonRequest::Status,
                ));
            responder_data_channel.event_notify.notify_one();

            let ControlMessage::Request {
                request, reply_tx, ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected ordinary request while entity subscription is active");
            };
            assert_eq!(*request, DaemonRequest::Status);
            let mut status = response_with_diagnostic(DaemonDiagnostic::connected("fixture"));
            status.kind = botster_hub_client::DaemonResponseKind::Status;
            reply_tx.send(Ok(status)).unwrap();

            let deadline = Instant::now() + Duration::from_secs(1);
            while responder_data_channel.sent.lock().unwrap().len() < 4 {
                assert!(
                    Instant::now() < deadline,
                    "all multiplexed deliveries complete"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            responder_peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::PeerClosed);
            let ControlMessage::LocalWebrtcPeerClosed {
                entity_subscription_ids,
                ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected peer cleanup");
            };
            assert_eq!(entity_subscription_ids, vec!["entity-fixture"]);
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_sender = peer_state.runtime_tx.clone();
        let (entity_frame_tx, entity_frame_rx) =
            tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let failure = runtime.block_on(run_data_channel(
            data_channel.as_ref(),
            &key,
            peer_state.as_ref(),
            &runtime_sender,
            entity_frame_tx,
            entity_frame_rx,
        ));
        responder.join().unwrap();
        assert!(failure.is_none());

        let deliveries = data_channel
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|serialized| {
                let chunk =
                    serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(serialized).unwrap();
                assert_eq!(chunk.chunk_count, 1);
                let envelope = serde_json::from_str::<AesGcmEnvelope>(&chunk.payload).unwrap();
                let plaintext = decrypt_aes_gcm(&key, &envelope).unwrap();
                (chunk.delivery_kind, plaintext)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            deliveries.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
            vec![
                DaemonLocalWebrtcDeliveryKind::DaemonResponse,
                DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame,
                DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame,
                DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            ]
        );
        let snapshot: DaemonEntityFrame = serde_json::from_slice(&deliveries[1].1).unwrap();
        assert_eq!(entity_frame_subscription_id(&snapshot), "entity-fixture");
        let resync: DaemonEntityFrame = serde_json::from_slice(&deliveries[2].1).unwrap();
        assert!(matches!(
            resync,
            DaemonEntityFrame::Snapshot {
                snapshot_seq: 2,
                ref items,
                resync_reason: Some(ref reason),
                ..
            } if items.is_empty() && reason == "subscriber_overflow"
        ));
    }

    #[test]
    fn replacement_peer_rejects_prior_generation_frames_and_delivers_current_generation() {
        let key = AesGcmKey::from_slice(&[22; 32]).unwrap();
        let data_channel = Arc::new(FakeDataChannel::default());
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "replacement-grant".to_string(),
            runtime_tx,
        ));
        peer_state.add_entity_subscription("generation-2".to_string());
        let responder_peer_state = peer_state.clone();
        let responder_data_channel = data_channel.clone();
        let runtime_sender = peer_state.runtime_tx.clone();
        let (entity_frame_tx, entity_frame_rx) = tokio_mpsc::channel(2);
        entity_frame_tx
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: "generation-1".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 1,
                items: Vec::new(),
                resync_reason: None,
            })
            .unwrap();
        entity_frame_tx
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: "generation-2".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 2,
                items: Vec::new(),
                resync_reason: None,
            })
            .unwrap();
        let responder = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(1);
            while responder_data_channel.sent.lock().unwrap().is_empty() {
                assert!(
                    Instant::now() < deadline,
                    "current-generation frame is delivered"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            responder_peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::PeerClosed);
            assert!(matches!(
                receive_test_runtime_message(&mut runtime_rx),
                ControlMessage::LocalWebrtcPeerClosed { grant_id, .. }
                    if grant_id == "replacement-grant"
            ));
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let failure = runtime.block_on(run_data_channel(
            data_channel.as_ref(),
            &key,
            peer_state.as_ref(),
            &runtime_sender,
            entity_frame_tx,
            entity_frame_rx,
        ));
        responder.join().unwrap();
        assert!(failure.is_none());

        let sent = data_channel.sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "the prior-generation frame must be dropped");
        let chunk: DaemonLocalWebrtcDeliveryChunk = serde_json::from_str(&sent[0]).unwrap();
        assert_eq!(
            chunk.delivery_kind,
            DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame
        );
        let envelope: AesGcmEnvelope = serde_json::from_str(&chunk.payload).unwrap();
        let plaintext = decrypt_aes_gcm(&key, &envelope).unwrap();
        let frame: DaemonEntityFrame = serde_json::from_slice(&plaintext).unwrap();
        assert_eq!(entity_frame_subscription_id(&frame), "generation-2");
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
        let data_channel = Arc::new(FakeDataChannel::default());
        {
            let mut events = data_channel.events.lock().unwrap();
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
            while responder_data_channel.sent.lock().unwrap().len() < 2 {
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
        let (entity_frame_tx, entity_frame_rx) =
            tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let failure = runtime.block_on(run_data_channel(
            data_channel.as_ref(),
            &key,
            peer_state.as_ref(),
            &runtime_sender,
            entity_frame_tx,
            entity_frame_rx,
        ));

        responder.join().unwrap();
        assert!(failure.is_none());
        assert_eq!(data_channel.sent.lock().unwrap().len(), 2);
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
        assert_eq!(resumed_channel.sent.lock().unwrap().len(), 1);
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
            assert!(channel.sent.lock().unwrap().is_empty());
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
        use botster_terminal_protocol::TerminalFrame;

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

        let frame = TerminalFrame::from_bytes(
            br#"{"type":"terminal_output","marker":"sibling-under-high-water"}"#,
        )
        .expect("opaque sibling frame");
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
        let mut pending = VecDeque::from([PendingLocalWebrtcRequest::Request(Box::new(
            DaemonRequest::Status,
        ))]);
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
        let frames = frame_encrypted_daemon_delivery(
            DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            "response-progress",
            &"a".repeat(256 * 1024),
        )
        .unwrap();
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
        let mut flow_control = LocalWebrtcFlowControl::default();
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
            Some(PendingLocalWebrtcRequest::Request(request)) if *request == DaemonRequest::Status
        ));
        assert!(pending.is_empty());
    }

    #[test]
    fn overflowing_requests_each_preserve_one_fifo_operator_response() {
        let key = AesGcmKey::from_slice(&[8; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();

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

        assert_eq!(pending.len(), LOCAL_WEBRTC_PENDING_REQUESTS + 1);
        assert!(matches!(
            pending.back(),
            Some(PendingLocalWebrtcRequest::QueueOverflow(4))
        ));
        let mut responses_emitted = 0;
        while pop_pending_request(&mut pending).is_some() {
            responses_emitted += 1;
        }
        assert_eq!(responses_emitted, inbound_requests);
        let response = queued_request_overflow_response();
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert!(
            response.diagnostics[0]
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("capacity exceeded")
        );
    }

    #[test]
    fn interleaved_overflow_runs_preserve_fifo_response_order() {
        let key = AesGcmKey::from_slice(&[11; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
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
            Some(PendingLocalWebrtcRequest::Request(request)) if *request == DaemonRequest::Status
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
                PendingLocalWebrtcRequest::Request(request)
                    if *request == DaemonRequest::ListSessions =>
                {
                    "new-request"
                }
                PendingLocalWebrtcRequest::Request(_) => "status",
                PendingLocalWebrtcRequest::Hello(_) => "hello",
                PendingLocalWebrtcRequest::EntityFrame(_) => "entity",
                PendingLocalWebrtcRequest::QueueOverflow(_) => "overflow",
            })
            .collect::<Vec<_>>();
        let mut expected_order = vec!["status"; LOCAL_WEBRTC_PENDING_REQUESTS - 1];
        expected_order.extend(["overflow", "new-request", "overflow"]);
        assert_eq!(emitted_order, expected_order);
    }
}
