use botster_core::AesGcmKey;
use botster_hub_client::{
    DaemonCompatibility, DaemonDiagnostic, DaemonEntityFrame, DaemonHello, DaemonHelloAck,
    DaemonRequest, DaemonResponse, PROTOCOL,
};
use botster_terminal_protocol::{
    TerminalCompatibility, ensure_compatible as ensure_terminal_compatible,
};
use tokio::sync::oneshot;

use crate::daemon::control::message::{
    BindReservedError, BoundSubscription, ControlMessage, ReservationInspectReply,
};
use crate::transport::webrtc::adapter::WebRtcTerminalAdapterHandle;
use crate::transport::webrtc::control_channel::{
    DataChannelPlaintext, LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH, LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW,
    decrypt_data_channel_plaintext,
};
use crate::transport::webrtc::delivery::{
    InboundTerminalEnvelopeAssembly, framed_daemon_entity_frame, framed_daemon_event,
    framed_daemon_hello_ack, framed_daemon_terminal_frame,
};
use crate::transport::webrtc::peer::LocalWebrtcPeerState;

use crate::subscription::attach_routes::response_records_attach_ownership;
use crate::transport::webrtc::control_channel::LocalWebrtcDataChannel;
use crate::transport::webrtc::peer::LOCAL_WEBRTC_PEER_CLOSE_BOUND;
pub(crate) const TEST_EXTRA_CHANNEL_CLOSE_MARKER_ENV: &str =
    "BOTSTER_HUB_TEST_EXTRA_CHANNEL_CLOSE_MARKER";
pub(crate) const TEST_EXTRA_CHANNEL_OBSERVATION_ENV: &str =
    "BOTSTER_HUB_TEST_EXTRA_CHANNEL_OBSERVATION";
#[cfg(test)]
pub(crate) const EXTRA_DATA_CHANNEL_LABEL: &str = "botster-extra";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionChannelRejectReason {
    Late,
    Stale,
    Duplicate,
    Unreserved,
    OverLimit,
    InvalidHello,
    BindFailed,
}

impl SubscriptionChannelRejectReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Late => "late",
            Self::Stale => "stale",
            Self::Duplicate => "duplicate",
            Self::Unreserved => "unreserved",
            Self::OverLimit => "over_limit",
            Self::InvalidHello => "invalid_hello",
            Self::BindFailed => "bind_failed",
        }
    }
}

pub(crate) fn observe_rejected_data_channel_for_test(
    claimed: bool,
    close: &Result<Result<(), String>, tokio::time::error::Elapsed>,
    label: &str,
) {
    if std::env::var("BOTSTER_ENV").as_deref() != Ok("test") {
        return;
    }
    let lost_claim = !claimed;
    let close_ok = matches!(close, Ok(Ok(())));
    // extra-channel close marker requires lost_claim && close_ok
    if lost_claim
        && close_ok
        && let Ok(path) = std::env::var(TEST_EXTRA_CHANNEL_CLOSE_MARKER_ENV)
        && !path.is_empty()
    {
        let _ = std::fs::write(path, "closed\n");
    }
    if let Ok(path) = std::env::var(TEST_EXTRA_CHANNEL_OBSERVATION_ENV)
        && !path.is_empty()
    {
        let body = serde_json::json!({
            "lost_claim": lost_claim,
            "close_ok": close_ok,
            "label": label,
        })
        .to_string();
        let path = std::path::PathBuf::from(path);
        let temporary = path.with_extension("tmp");
        if std::fs::write(&temporary, body).is_ok() {
            let _ = std::fs::rename(temporary, path);
        }
    }
}
pub(crate) async fn reject_extra_data_channel<C>(
    grant_id: &str,
    claimed: bool,
    label: &str,
    data_channel: &C,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    eprintln!("local WebRTC rejecting extra DataChannel: grant_id={grant_id}");
    let close = close_subscription_channel(data_channel).await;
    observe_rejected_data_channel_for_test(claimed, &close, label);
}

async fn reject_reserved_data_channel<C>(
    grant_id: &str,
    label: &str,
    reason: SubscriptionChannelRejectReason,
    data_channel: &C,
    peer_state: &LocalWebrtcPeerState,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    peer_state
        .mux
        .push_host_event(botster_hub_client::DaemonEvent::RuntimeObservation {
            kind: format!("subscription_channel_rejected:{}:{label}", reason.as_str()),
        });
    reject_extra_data_channel(grant_id, false, label, data_channel).await;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalWebrtcAttachedSubscription {
    pub session_id: String,
    pub subscription_id: String,
}

pub(crate) enum LocalWebrtcAttachedSubscriptionChange {
    Attach(LocalWebrtcAttachedSubscription),
    Detach(LocalWebrtcAttachedSubscription),
}

pub(crate) fn local_webrtc_attach_change_for_response(
    request: &DaemonRequest,
    response: &DaemonResponse,
) -> Option<LocalWebrtcAttachedSubscriptionChange> {
    if !response_records_attach_ownership(response) {
        return None;
    }
    LocalWebrtcAttachedSubscriptionChange::from_request(request)
}

impl LocalWebrtcAttachedSubscriptionChange {
    pub(crate) fn from_request(request: &DaemonRequest) -> Option<Self> {
        match request {
            DaemonRequest::Attach {
                session_id,
                subscription_id,
            } => Some(Self::Attach(LocalWebrtcAttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            DaemonRequest::Detach {
                session_id,
                subscription_id,
            } => Some(Self::Detach(LocalWebrtcAttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            _ => None,
        }
    }
}
pub(crate) async fn admit_reserved_subscription_channel<C>(
    grant_id: &str,
    label: &str,
    data_channel: &C,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let (inspect_tx, inspect_rx) = oneshot::channel();
    if peer_state
        .runtime_tx
        .send(ControlMessage::InspectReservation {
            grant_id: grant_id.to_string(),
            label: label.to_string(),
            reply_tx: inspect_tx,
        })
        .await
        .is_err()
    {
        reject_extra_data_channel(grant_id, false, label, data_channel).await;
        return;
    }
    let inspect = match inspect_rx.await {
        Ok(inspect) => inspect,
        Err(_) => {
            reject_extra_data_channel(grant_id, false, label, data_channel).await;
            return;
        }
    };
    let (subscription_id, generation) = match inspect {
        ReservationInspectReply::Unknown => {
            reject_reserved_data_channel(
                grant_id,
                label,
                SubscriptionChannelRejectReason::Unreserved,
                data_channel,
                peer_state,
            )
            .await;
            return;
        }
        ReservationInspectReply::Stale => {
            reject_reserved_data_channel(
                grant_id,
                label,
                SubscriptionChannelRejectReason::Stale,
                data_channel,
                peer_state,
            )
            .await;
            return;
        }
        ReservationInspectReply::Bound => {
            reject_reserved_data_channel(
                grant_id,
                label,
                SubscriptionChannelRejectReason::Duplicate,
                data_channel,
                peer_state,
            )
            .await;
            return;
        }
        ReservationInspectReply::OverLimit => {
            reject_reserved_data_channel(
                grant_id,
                label,
                SubscriptionChannelRejectReason::OverLimit,
                data_channel,
                peer_state,
            )
            .await;
            return;
        }
        ReservationInspectReply::Expired { .. } => {
            reject_reserved_data_channel(
                grant_id,
                label,
                SubscriptionChannelRejectReason::Late,
                data_channel,
                peer_state,
            )
            .await;
            return;
        }
        ReservationInspectReply::Live {
            subscription_id,
            generation,
            ..
        } => (subscription_id, generation),
    };
    let hello_permits =
        match admit_subscription_hello(data_channel, stream_key, peer_state, grant_id, label).await
        {
            Ok(permits) => permits,
            Err(()) => {
                reject_reserved_data_channel(
                    grant_id,
                    label,
                    SubscriptionChannelRejectReason::InvalidHello,
                    data_channel,
                    peer_state,
                )
                .await;
                return;
            }
        };
    let (bind_tx, bind_rx) = oneshot::channel();
    if peer_state
        .runtime_tx
        .send(ControlMessage::BindReservedSubscription {
            grant_id: grant_id.to_string(),
            label: label.to_string(),
            reply_tx: bind_tx,
        })
        .await
        .is_err()
    {
        reject_extra_data_channel(grant_id, false, label, data_channel).await;
        return;
    }
    match bind_rx.await {
        Ok(Ok(bound)) => {
            let route = BoundSubscriptionRoute {
                peer_state,
                grant_id,
                label,
                subscription_id: &subscription_id,
                generation,
            };
            run_bound_subscription_channel_and_retire(
                data_channel,
                stream_key,
                route,
                bound,
                hello_permits,
            )
            .await;
        }
        Ok(Err(error)) => {
            let reason = match error {
                BindReservedError::Unknown => SubscriptionChannelRejectReason::Unreserved,
                BindReservedError::Stale => SubscriptionChannelRejectReason::Stale,
                BindReservedError::OverLimit => SubscriptionChannelRejectReason::OverLimit,
                BindReservedError::Expired => SubscriptionChannelRejectReason::Late,
                BindReservedError::Bound => SubscriptionChannelRejectReason::Duplicate,
                BindReservedError::BindFailed => SubscriptionChannelRejectReason::BindFailed,
            };
            reject_reserved_data_channel(grant_id, label, reason, data_channel, peer_state).await;
        }
        Err(_) => {
            reject_reserved_data_channel(
                grant_id,
                label,
                SubscriptionChannelRejectReason::BindFailed,
                data_channel,
                peer_state,
            )
            .await;
        }
    }
}

async fn run_bound_subscription_channel_and_retire<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    route: BoundSubscriptionRoute<'_>,
    bound: BoundSubscription,
    hello_permits: Vec<crate::admission::connection_budget::AggregateSendPermit>,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    run_bound_subscription_channel(data_channel, stream_key, route, bound, hello_permits).await;
    let _ = route
        .peer_state
        .runtime_tx
        .send(ControlMessage::RetireReservedSubscription {
            grant_id: route.grant_id.to_string(),
            label: route.label.to_string(),
        })
        .await;
}

async fn admit_subscription_hello<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
    grant_id: &str,
    label: &str,
) -> Result<Vec<crate::admission::connection_budget::AggregateSendPermit>, ()>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    loop {
        match data_channel.local_poll().await {
            Some(webrtc::data_channel::DataChannelEvent::OnMessage(message)) => {
                match decrypt_data_channel_plaintext(stream_key, message.data.as_ref()) {
                    Some(DataChannelPlaintext::Hello(hello)) => {
                        return acknowledge_subscription_hello(
                            data_channel,
                            stream_key,
                            &hello,
                            peer_state,
                            grant_id,
                            label,
                        )
                        .await;
                    }
                    _ => return Err(()),
                }
            }
            Some(webrtc::data_channel::DataChannelEvent::OnClose)
            | Some(webrtc::data_channel::DataChannelEvent::OnError)
            | None => return Err(()),
            Some(_) => continue,
        }
    }
}

async fn acknowledge_subscription_hello<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    hello: &DaemonHello,
    peer_state: &LocalWebrtcPeerState,
    grant_id: &str,
    label: &str,
) -> Result<Vec<crate::admission::connection_budget::AggregateSendPermit>, ()>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    if hello.protocol != PROTOCOL {
        return Err(());
    }
    if let Some(requirement) = hello.terminal_compatibility.as_ref()
        && ensure_terminal_compatible(requirement, &TerminalCompatibility::current()).is_err()
    {
        return Err(());
    }
    let ack = DaemonHelloAck {
        protocol: PROTOCOL.to_string(),
        compatibility: DaemonCompatibility::current(),
        terminal_compatibility: Some(TerminalCompatibility::current()),
        diagnostics: vec![DaemonDiagnostic::connected("hello")],
    };
    let frames = framed_daemon_hello_ack(stream_key, &ack).map_err(|_| ())?;
    let mut permits = Vec::with_capacity(frames.len());
    for frame in frames {
        let permit = authorize_subscription_hello_ack(peer_state, grant_id, label, frame.len())
            .await
            .ok_or(())?;
        data_channel.local_send_text(&frame).await.map_err(|_| ())?;
        permits.push(permit);
    }
    Ok(permits)
}

#[derive(Clone, Copy)]
struct BoundSubscriptionRoute<'a> {
    peer_state: &'a LocalWebrtcPeerState,
    grant_id: &'a str,
    label: &'a str,
    subscription_id: &'a str,
    generation: u64,
}

async fn run_bound_subscription_channel<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    route: BoundSubscriptionRoute<'_>,
    bound: BoundSubscription,
    hello_permits: Vec<crate::admission::connection_budget::AggregateSendPermit>,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    if data_channel
        .local_set_buffered_amount_low_threshold(LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW)
        .await
        .is_err()
        || data_channel
            .local_set_buffered_amount_high_threshold(LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH)
            .await
            .is_err()
    {
        if let BoundSubscription::Terminal { handle, .. } = &bound {
            handle.close();
        }
        let _ = close_subscription_channel(data_channel).await;
        return;
    }
    let usage = match &bound {
        BoundSubscription::Terminal { usage, .. }
        | BoundSubscription::Entity { usage, .. }
        | BoundSubscription::Event { usage, .. } => usage,
    };
    if publish_channel_usage(data_channel, usage).await.is_err() {
        if let BoundSubscription::Terminal { handle, .. } = &bound {
            handle.close();
        }
        let _ = close_subscription_channel(data_channel).await;
        return;
    }
    drop(hello_permits);
    match bound {
        BoundSubscription::Terminal { handle, usage } => {
            run_bound_terminal_channel(data_channel, stream_key, route.peer_state, handle, usage)
                .await;
        }
        BoundSubscription::Entity { receiver, usage } => {
            run_bound_entity_channel(data_channel, stream_key, route, receiver, usage).await;
        }
        BoundSubscription::Event { mailbox, usage } => {
            run_bound_event_channel(data_channel, stream_key, route, mailbox, usage).await;
        }
    }
}

async fn run_bound_terminal_channel<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
    handle: WebRtcTerminalAdapterHandle,
    usage: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let mut inbound_assembly = InboundTerminalEnvelopeAssembly::default();
    loop {
        if let Err(()) =
            flush_subscription_adapter_frames(data_channel, stream_key, &handle, &usage).await
        {
            let _ = close_subscription_channel(data_channel).await;
            handle.close();
            return;
        }
        let _ = publish_channel_usage(data_channel, &usage).await;
        peer_state.mux.refresh_aggregate_pressure();
        tokio::select! {
            biased;
            _ = handle.wait_for_write() => {}
            inbound = data_channel.local_poll() => {
                match inbound {
                    Some(webrtc::data_channel::DataChannelEvent::OnMessage(message)) => {
                        let encrypted = match inbound_assembly.push(message.data.as_ref()) {
                            Ok(Some(encrypted)) => encrypted,
                            Ok(None) => continue,
                            Err(()) => {
                                handle.close();
                                let _ = close_subscription_channel(data_channel).await;
                                return;
                            }
                        };
                        let Ok(envelope) = serde_json::from_str::<botster_core::AesGcmEnvelope>(
                            &encrypted,
                        ) else {
                            handle.close();
                            let _ = close_subscription_channel(data_channel).await;
                            return;
                        };
                        let Ok(bytes) = botster_core::decrypt_aes_gcm(stream_key, &envelope) else {
                            handle.close();
                            let _ = close_subscription_channel(data_channel).await;
                            return;
                        };
                        if handle.push_ingress(bytes).is_err() {
                            handle.close();
                            let _ = close_subscription_channel(data_channel).await;
                            return;
                        }
                    }
                    Some(event @ (webrtc::data_channel::DataChannelEvent::OnBufferedAmountHigh
                    | webrtc::data_channel::DataChannelEvent::OnBufferedAmountLow)) => {
                        apply_subscription_pressure_event(&handle, &event);
                        let _ = publish_channel_usage(data_channel, &usage).await;
                        peer_state.mux.refresh_aggregate_pressure();
                    }
                    Some(webrtc::data_channel::DataChannelEvent::OnClose)
                    | Some(webrtc::data_channel::DataChannelEvent::OnError)
                    | None => {
                        let _ = flush_subscription_adapter_frames(
                            data_channel,
                            stream_key,
                            &handle,
                            &usage,
                        )
                        .await;
                        handle.close();
                        return;
                    }
                    Some(_) => {}
                }
            }
        }
    }
}

async fn run_bound_entity_channel<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    route: BoundSubscriptionRoute<'_>,
    mut receiver: tokio::sync::mpsc::Receiver<DaemonEntityFrame>,
    usage: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let mut peer_terminal_rx = route.peer_state.subscribe_peer_terminal();
    'driver: loop {
        tokio::select! {
            biased;
            frame = receiver.recv() => {
                let Some(frame) = frame else { break };
                let Ok(frames) = framed_daemon_entity_frame(stream_key, &frame) else { break };
                for frame in frames {
                    let Some(permit) = authorize_subscription_send(
                        route.peer_state,
                        route.grant_id,
                        route.label,
                        frame.len(),
                    ).await else {
                        route.peer_state.mux.push_host_event(
                            botster_hub_client::DaemonEvent::RuntimeObservation {
                                kind: format!(
                                    "entity_subscription_closed:{}:{}:entity_subscription_overflow",
                                    route.subscription_id,
                                    route.generation,
                                ),
                            },
                        );
                        break 'driver;
                    };
                    if data_channel.local_send_text(&frame).await.is_err() {
                        let _ = close_subscription_channel(data_channel).await;
                        drop(permit);
                        return;
                    }
                    if publish_channel_usage(data_channel, &usage).await.is_err() {
                        let _ = close_subscription_channel(data_channel).await;
                        drop(permit);
                        return;
                    }
                    drop(permit);
                    route.peer_state.mux.refresh_aggregate_pressure();
                }
            }
            event = crate::transport::webrtc::control_channel::poll_data_channel_or_peer_terminal(
                data_channel,
                &mut peer_terminal_rx,
            ) => {
                match event {
                    Ok(Some(webrtc::data_channel::DataChannelEvent::OnBufferedAmountHigh
                        | webrtc::data_channel::DataChannelEvent::OnBufferedAmountLow)) => {
                        let _ = publish_channel_usage(data_channel, &usage).await;
                        route.peer_state.mux.refresh_aggregate_pressure();
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }
    let _ = close_subscription_channel(data_channel).await;
}

async fn authorize_subscription_send(
    peer_state: &LocalWebrtcPeerState,
    grant_id: &str,
    label: &str,
    frame_len: usize,
) -> Option<crate::admission::connection_budget::AggregateSendPermit> {
    let (reply_tx, reply_rx) = oneshot::channel();
    if peer_state
        .runtime_tx
        .send(ControlMessage::AuthorizeSubscriptionSend {
            grant_id: grant_id.to_string(),
            label: label.to_string(),
            frame_len,
            reply_tx,
        })
        .await
        .is_err()
    {
        return None;
    }
    reply_rx.await.unwrap_or(None)
}

async fn authorize_subscription_hello_ack(
    peer_state: &LocalWebrtcPeerState,
    grant_id: &str,
    label: &str,
    frame_len: usize,
) -> Option<crate::admission::connection_budget::AggregateSendPermit> {
    let (reply_tx, reply_rx) = oneshot::channel();
    if peer_state
        .runtime_tx
        .send(ControlMessage::AuthorizeSubscriptionHelloAck {
            grant_id: grant_id.to_string(),
            label: label.to_string(),
            frame_len,
            reply_tx,
        })
        .await
        .is_err()
    {
        return None;
    }
    reply_rx.await.unwrap_or(None)
}

async fn run_bound_event_channel<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    route: BoundSubscriptionRoute<'_>,
    mailbox: std::sync::Arc<crate::subscription::package_events::ClientEventMailbox>,
    usage: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let mut peer_terminal_rx = route.peer_state.subscribe_peer_terminal();
    'driver: loop {
        if mailbox.is_retired() {
            break;
        }
        while let Some(event) = mailbox.take_ready_event() {
            let Ok(frames) = framed_daemon_event(stream_key, &event) else {
                break 'driver;
            };
            for frame in frames {
                let Some(permit) = authorize_subscription_send(
                    route.peer_state,
                    route.grant_id,
                    route.label,
                    frame.len(),
                )
                .await
                else {
                    if let botster_hub_client::DaemonEvent::PackageEvent {
                        subscription_id,
                        owner,
                        name,
                        ..
                    } = &event
                    {
                        mailbox.set_gap(subscription_id, owner, name);
                    }
                    route.peer_state.mux.push_host_event(
                        botster_hub_client::DaemonEvent::RuntimeObservation {
                            kind: format!(
                                "package_event_subscription_closed:{}:{}:aggregate_overflow",
                                route.subscription_id, route.generation,
                            ),
                        },
                    );
                    break 'driver;
                };
                if data_channel.local_send_text(&frame).await.is_err() {
                    let _ = close_subscription_channel(data_channel).await;
                    drop(permit);
                    return;
                }
                if publish_channel_usage(data_channel, &usage).await.is_err() {
                    let _ = close_subscription_channel(data_channel).await;
                    drop(permit);
                    return;
                }
                drop(permit);
                route.peer_state.mux.refresh_aggregate_pressure();
            }
        }
        let notified = mailbox.notify().notified();
        tokio::pin!(notified);
        if mailbox.is_retired() {
            break;
        }
        if mailbox.take_wake() || mailbox.has_ready_event() {
            continue;
        }
        tokio::select! {
            biased;
            () = &mut notified => {}
            event = crate::transport::webrtc::control_channel::poll_data_channel_or_peer_terminal(
                data_channel,
                &mut peer_terminal_rx,
            ) => {
                match event {
                    Ok(Some(webrtc::data_channel::DataChannelEvent::OnBufferedAmountHigh
                        | webrtc::data_channel::DataChannelEvent::OnBufferedAmountLow)) => {
                        let _ = publish_channel_usage(data_channel, &usage).await;
                        route.peer_state.mux.refresh_aggregate_pressure();
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }
    let _ = close_subscription_channel(data_channel).await;
}

async fn close_subscription_channel<C>(
    data_channel: &C,
) -> Result<Result<(), String>, tokio::time::error::Elapsed>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, data_channel.local_close()).await
}

async fn publish_channel_usage<C>(
    data_channel: &C,
    usage: &std::sync::atomic::AtomicUsize,
) -> Result<(), ()>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let bytes = data_channel
        .local_outstanding_bytes()
        .await
        .map_err(|_| ())?;
    usage.store(bytes, std::sync::atomic::Ordering::Release);
    Ok(())
}

fn apply_subscription_pressure_event(
    handle: &WebRtcTerminalAdapterHandle,
    event: &webrtc::data_channel::DataChannelEvent,
) {
    match event {
        webrtc::data_channel::DataChannelEvent::OnBufferedAmountHigh => {
            handle.set_would_block(true);
        }
        webrtc::data_channel::DataChannelEvent::OnBufferedAmountLow => {
            handle.set_would_block(false);
        }
        _ => {}
    }
}

async fn flush_subscription_adapter_frames<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    handle: &WebRtcTerminalAdapterHandle,
    usage: &std::sync::atomic::AtomicUsize,
) -> Result<(), ()>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let (bytes, from_late) = match handle.snapshot_active() {
        Some(bytes) => (bytes, false),
        None => match handle.take_late_egress() {
            Some(bytes) => (bytes, true),
            None => {
                handle.release_aggregate_permit();
                return if handle.is_closed() { Err(()) } else { Ok(()) };
            }
        },
    };
    let frames = match framed_daemon_terminal_frame(stream_key, &bytes) {
        Ok(frames) => frames,
        Err(_) => {
            if from_late {
                handle.restore_late_egress(bytes);
            }
            return Err(());
        }
    };
    let wire_len = frames.iter().map(String::len).sum();
    if !handle.resize_aggregate_permit(wire_len) {
        if from_late {
            handle.restore_late_egress(bytes);
        }
        return Err(());
    }
    for frame in frames {
        data_channel.local_send_text(&frame).await.map_err(|_| ())?;
    }
    publish_channel_usage(data_channel, usage).await?;
    let _ = handle.complete_active();
    let _ = handle.take_late_egress();
    handle.release_aggregate_permit();
    Ok(())
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

    fn require_host_event_before_close(
        mux: &WebRtcConnectionMux,
        channel: &FakeDataChannel,
        expected: botster_hub_client::DaemonEvent,
    ) {
        let event_admitted = Arc::new(AtomicBool::new(false));
        let observer_flag = Arc::clone(&event_admitted);
        mux.set_host_event_observer(Some(Arc::new(move |event| {
            if event == &expected {
                observer_flag.store(true, Ordering::Release);
            }
        })));
        *channel.close_probe.lock().expect("close probe mutex") =
            Some(Arc::new(move || event_admitted.load(Ordering::Acquire)));
    }

    #[test]
    fn terminal_channel_pressure_targets_one_adapter_and_low_water_resumes_it() {
        let (adapter, handle) = crate::transport::webrtc::adapter::WebRtcTerminalAdapter::pair();
        let (sibling, _sibling_handle) =
            crate::transport::webrtc::adapter::WebRtcTerminalAdapter::pair();

        apply_subscription_pressure_event(&handle, &DataChannelEvent::OnBufferedAmountHigh);
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::WouldBlock);
        assert_eq!(sibling.pressure(), TerminalAdapterPressure::Ready);

        apply_subscription_pressure_event(&handle, &DataChannelEvent::OnBufferedAmountLow);
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Ready);
        assert_eq!(sibling.pressure(), TerminalAdapterPressure::Ready);
    }

    fn run_hanging_close_error_path(channel: &FakeDataChannel, threshold_fails: bool) {
        channel.close_hangs.store(true, Ordering::Release);
        channel
            .threshold_fails
            .store(threshold_fails, Ordering::Release);
        if !threshold_fails {
            channel.push_event(DataChannelEvent::OnMessage(RTCDataChannelMessage {
                is_string: true,
                data: b"not-json".as_slice().into(),
            }));
        }
        let peer_state = test_peer_state("grant-close-bound");
        let (_adapter, handle) = crate::transport::webrtc::adapter::WebRtcTerminalAdapter::pair();
        let usage = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let key = AesGcmKey::from_slice(&[13; 32]).expect("test key");
        let route = BoundSubscriptionRoute {
            peer_state: &peer_state,
            grant_id: "grant-close-bound",
            label: "route-close-bound",
            subscription_id: "sub-close-bound",
            generation: 1,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build close-bound runtime");
        let started = std::time::Instant::now();
        runtime.block_on(run_bound_subscription_channel(
            channel,
            &key,
            route,
            BoundSubscription::Terminal {
                handle: handle.clone(),
                usage,
            },
            Vec::new(),
        ));
        assert!(channel.close_started.load(Ordering::Acquire));
        assert!(handle.is_closed());
        assert!(
            started.elapsed() < LOCAL_WEBRTC_PEER_CLOSE_BOUND + Duration::from_secs(1),
            "subscription close must finish within its close bound"
        );
    }

    #[test]
    fn threshold_failure_bounds_a_hanging_subscription_close() {
        run_hanging_close_error_path(&FakeDataChannel::default(), true);
    }

    #[test]
    fn terminal_ingress_failure_bounds_a_hanging_subscription_close() {
        run_hanging_close_error_path(&FakeDataChannel::default(), false);
    }

    #[test]
    fn flush_after_close_sends_parked_late_egress_under_the_existing_permit() {
        use crate::admission::connection_budget::{ChannelClass, ConnectionBudget};
        use botster_core::contract::terminal_adapter::TerminalAdapter;
        use botster_terminal_protocol::TerminalFrame;

        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("route".to_string(), ChannelClass::Terminal)
            .expect("reserve terminal route");
        let mux = WebRtcConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        mux.register("late".into(), "terminal".into(), 1, handle.clone());
        let frame = TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"late"}"#)
            .expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        let authorized_before_close = budget.aggregate_buffered();
        assert!(authorized_before_close > 0);
        handle.close();
        assert!(handle.snapshot_active().is_none());
        assert_eq!(budget.aggregate_buffered(), authorized_before_close);
        let channel = FakeDataChannel::default();
        let key = AesGcmKey::from_slice(&[7; 32]).expect("test key");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("flush runtime");
        runtime
            .block_on(flush_subscription_adapter_frames(
                &channel, &key, &handle, &usage,
            ))
            .expect("flush parked late egress");
        assert!(
            !channel.sent.lock().expect("sent frames").is_empty(),
            "production flush must send parked late bytes after close"
        );
        assert_eq!(
            budget.aggregate_buffered(),
            usage.load(Ordering::Acquire),
            "usage publication must replace the close-time permit"
        );
        assert!(handle.take_late_egress().is_none());
    }

    fn initial_usage_blocks_first_payload(
        class: crate::admission::connection_budget::ChannelClass,
    ) {
        let mut budget = crate::admission::connection_budget::ConnectionBudget::default();
        let usage = budget
            .reserve("route".to_string(), class)
            .expect("reserve route");
        let channel = FakeDataChannel::default();
        channel.outstanding_bytes.store(
            crate::admission::connection_budget::AGGREGATE_BUFFERED_HIGH - 1,
            Ordering::Release,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build usage runtime");
        runtime
            .block_on(publish_channel_usage(&channel, &usage))
            .expect("publish HelloAck usage");
        assert!(budget.authorize_send("route", 2).is_none());
    }

    #[test]
    fn entity_route_counts_hello_ack_before_first_payload() {
        initial_usage_blocks_first_payload(
            crate::admission::connection_budget::ChannelClass::Entity,
        );
    }

    #[test]
    fn event_route_counts_hello_ack_before_first_payload() {
        initial_usage_blocks_first_payload(
            crate::admission::connection_budget::ChannelClass::Event,
        );
    }

    #[test]
    fn subscription_hello_ack_is_refused_before_write_at_aggregate_ceiling() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build HelloAck runtime");
        runtime.block_on(async {
            let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(4);
            let peer_state = LocalWebrtcPeerState::new("grant".to_string(), runtime_tx);
            let mut budget = crate::admission::connection_budget::ConnectionBudget::default();
            let usage = budget
                .reserve(
                    "route".to_string(),
                    crate::admission::connection_budget::ChannelClass::Entity,
                )
                .expect("reserve route");
            usage.store(
                crate::admission::connection_budget::AGGREGATE_BUFFERED_HIGH - 1,
                Ordering::Release,
            );
            let responder = tokio::spawn(async move {
                let Some(ControlMessage::AuthorizeSubscriptionHelloAck {
                    label,
                    frame_len,
                    reply_tx,
                    ..
                }) = runtime_rx.recv().await
                else {
                    panic!("expected HelloAck authorization");
                };
                let _ = reply_tx.send(budget.authorize_send(&label, frame_len));
            });
            let channel = FakeDataChannel::default();
            let key = AesGcmKey::from_slice(&[31; 32]).expect("test key");
            let hello = DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            };
            assert!(
                acknowledge_subscription_hello(
                    &channel,
                    &key,
                    &hello,
                    &peer_state,
                    "grant",
                    "route",
                )
                .await
                .is_err()
            );
            responder.await.expect("authorization responder");
            assert!(channel.sent.lock().expect("sent frames").is_empty());
        });
    }

    #[test]
    fn entity_overflow_reports_before_close_then_retires_only_the_target_route() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("entity-overflow-matrix");
        let mut peer = harness.signal_peer("http://127.0.0.1:41918");
        harness.ensure_webrtc_adapter_hello(&mut peer);
        peer.enable_host_events();

        let mut reservations = Vec::new();
        for index in 0..31 {
            let response = harness.subscribe_entities(&mut peer, &format!("overflow-{index}"));
            assert_eq!(
                response.kind,
                botster_hub_client::DaemonResponseKind::EntitySubscribed
            );
            reservations.push(
                response
                    .subscription_reservation
                    .expect("entity reservation"),
            );
        }
        let target = reservations.last().expect("target reservation").clone();
        let peer_generation = target.peer_generation;

        let (bind_tx, bind_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::BindReservedSubscription {
                grant_id: peer.grant_id.clone(),
                label: target.label.clone(),
                reply_tx: bind_tx,
            },
        );
        let BoundSubscription::Entity {
            receiver: target_receiver,
            usage: target_usage,
        } = bind_rx
            .blocking_recv()
            .expect("target bind reply")
            .expect("target bind")
        else {
            panic!("target reservation must bind as an entity route");
        };
        drop(target_receiver);

        {
            let budget = harness
                .state
                .pending_runtime
                .admission
                .connection_budgets
                .get(&peer_generation)
                .expect("peer budget");
            for (index, reservation) in reservations.iter().enumerate() {
                budget
                    .usage(&reservation.label)
                    .expect("entity route usage")
                    .store(if index < 29 { 65_536 } else { 98_304 }, Ordering::Release);
            }
            assert_eq!(
                budget.aggregate_buffered(),
                crate::admission::connection_budget::AGGREGATE_BUFFERED_HIGH
            );
            assert_eq!(budget.channel_count(), 32);
        }

        let refused = harness.subscribe_entities(&mut peer, "overflow-31");
        assert_eq!(
            refused.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            refused.error.as_ref().map(|error| error.code.as_str()),
            Some("connection_channel_limit")
        );
        let budget = harness
            .state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&peer_generation)
            .expect("peer budget");
        assert_eq!(
            budget.aggregate_buffered(),
            crate::admission::connection_budget::AGGREGATE_BUFFERED_HIGH
        );
        assert_eq!(
            budget.channel_count(),
            crate::admission::connection_budget::MAX_TOTAL_CHANNELS - 1,
            "aggregate admission must reject while one subscription slot remains free"
        );

        let (authorize_tx, authorize_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::AuthorizeSubscriptionSend {
                grant_id: peer.grant_id.clone(),
                label: target.label.clone(),
                frame_len: 65_536,
                reply_tx: authorize_tx,
            },
        );
        assert!(
            authorize_rx
                .blocking_recv()
                .expect("65,536-byte authorization reply")
                .is_none(),
            "C_cross must refuse a 65,536-byte frame before transport write"
        );

        let peer_state = harness
            .daemon
            .local_webrtc()
            .peer_states
            .get(&peer.grant_id)
            .expect("live peer state")
            .clone();
        let channel = Arc::new(FakeDataChannel::default());
        require_host_event_before_close(
            &peer_state.mux,
            channel.as_ref(),
            botster_hub_client::DaemonEvent::RuntimeObservation {
                kind: format!(
                    "entity_subscription_closed:{}:{}:entity_subscription_overflow",
                    target.subscription_id, target.generation
                ),
            },
        );
        let (frame_tx, frame_rx) = tokio_mpsc::channel(1);
        frame_tx
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: target.subscription_id.clone(),
                entity_type: "session".to_string(),
                snapshot_seq: 1,
                items: vec![serde_json::json!({"payload": "x".repeat(65_536)})],
                resync_reason: None,
            })
            .expect("queue overflowing entity frame");
        drop(frame_tx);
        let key = peer.stream_key.clone();
        let grant_id = peer.grant_id.clone();
        let label = target.label.clone();
        let subscription_id = target.subscription_id.clone();
        let generation = target.generation;
        let handler_channel = Arc::clone(&channel);
        let handler_peer_state = Arc::clone(&peer_state);
        let handler = harness.transport_handle.spawn(async move {
            let route = BoundSubscriptionRoute {
                peer_state: handler_peer_state.as_ref(),
                grant_id: &grant_id,
                label: &label,
                subscription_id: &subscription_id,
                generation,
            };
            run_bound_entity_channel(
                handler_channel.as_ref(),
                &key,
                route,
                frame_rx,
                target_usage,
            )
            .await;
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let authorization = loop {
            match harness.control_rx.try_recv() {
                Ok(message) => break message,
                Err(tokio_mpsc::error::TryRecvError::Empty)
                    if std::time::Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("entity authorization was not queued: {error}"),
            }
        };
        assert!(matches!(
            authorization,
            ControlMessage::AuthorizeSubscriptionSend { .. }
        ));
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            authorization,
        );
        harness
            ._transport_runtime
            .block_on(handler)
            .expect("entity handler joins");
        assert!(channel.sent.lock().expect("sent frames").is_empty());
        assert!(channel.close_started.load(Ordering::Acquire));
        assert!(channel.closed.load(Ordering::Acquire));
        assert!(
            channel.close_probe_passed.load(Ordering::Acquire),
            "the typed control event must enter the control send path before local_close starts"
        );
        assert_eq!(
            harness
                .state
                .pending_runtime
                .admission
                .connection_budgets
                .get(&peer_generation)
                .expect("peer budget")
                .aggregate_buffered(),
            crate::admission::connection_budget::AGGREGATE_BUFFERED_HIGH,
            "the aggregate must stay exact through refusal and control reporting"
        );

        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::RetireReservedSubscription {
                grant_id: peer.grant_id.clone(),
                label: target.label.clone(),
            },
        );
        let budget = harness
            .state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&peer_generation)
            .expect("peer budget");
        assert_eq!(budget.aggregate_buffered(), 1_998_848);
        assert!(budget.usage(&target.label).is_none());
        assert!(
            harness
                .state
                .pending_runtime
                .admission
                .reservations
                .reservation_for_label(&target.label, peer_generation)
                .is_none()
        );
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key(&target.subscription_id)
        );
        for (index, reservation) in reservations.iter().take(30).enumerate() {
            assert_eq!(
                budget
                    .usage(&reservation.label)
                    .expect("sibling usage")
                    .load(Ordering::Acquire),
                if index < 29 { 65_536 } else { 98_304 }
            );
            assert!(
                harness
                    .state
                    .pending_runtime
                    .admission
                    .reservations
                    .reservation_for_label(&reservation.label, peer_generation)
                    .is_some(),
                "sibling reservation must remain live"
            );
            assert!(
                harness
                    .state
                    .entity_subscriptions
                    .contains_key(&reservation.subscription_id),
                "sibling entity subscription must remain live"
            );
        }
        let replacement = harness.subscribe_entities(&mut peer, "overflow-replacement");
        assert_eq!(
            replacement.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(replacement.subscription_reservation.is_some());

        let event = harness.wait_for_host_event(&mut peer, "entity overflow");
        assert_eq!(
            event,
            botster_hub_client::DaemonEvent::RuntimeObservation {
                kind: format!(
                    "entity_subscription_closed:{}:{}:entity_subscription_overflow",
                    target.subscription_id, target.generation
                ),
            }
        );

        peer.close_offer();
        harness.cleanup();
    }

    fn pump_test_control_until(
        harness: &mut PeerHarness,
        label: &str,
        mut complete: impl FnMut(&PeerHarness) -> bool,
    ) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !complete(harness) {
            match harness.control_rx.try_recv() {
                Ok(message) => {
                    handle_control_message(
                        &mut harness.daemon,
                        &mut harness.state,
                        &harness.terminal_path,
                        &harness.transport_handle,
                        harness.control_tx.clone(),
                        message,
                    );
                }
                Err(tokio_mpsc::error::TryRecvError::Empty)
                    if std::time::Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("timed out waiting for {label}: {error}"),
            };
        }
    }

    #[test]
    fn entity_overflow_full_host_auto_retires_target_and_keeps_sibling_hosts_usable() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("entity-overflow-full-host");
        let mut peer = harness.signal_peer("http://127.0.0.1:41919");
        harness.ensure_webrtc_adapter_hello(&mut peer);
        peer.enable_host_events();

        let mut reservations = Vec::new();
        for index in 0..31 {
            let response = harness.request_on_peer(
                &mut peer,
                DaemonRequest::SubscribeEntities {
                    entity_type: "session_type".to_string(),
                    subscription_id: format!("host-overflow-{index}"),
                },
                "SubscribeEntities",
            );
            assert_eq!(
                response.kind,
                botster_hub_client::DaemonResponseKind::EntitySubscribed
            );
            reservations.push(
                response
                    .subscription_reservation
                    .expect("entity reservation"),
            );
        }
        let target = reservations.last().expect("target reservation").clone();
        let peer_generation = target.peer_generation;
        let peer_state = harness
            .daemon
            .local_webrtc()
            .peer_states
            .get(&peer.grant_id)
            .expect("live peer state")
            .clone();
        let hello = DaemonHello {
            protocol: PROTOCOL.to_string(),
            compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
            terminal_compatibility: None,
        };
        let mut channels = Vec::new();
        let mut hosts = Vec::new();
        for reservation in &reservations {
            let channel = Arc::new(FakeDataChannel::default());
            channel.push_event(encrypted_hello_event(&peer.stream_key, &hello));
            let host_channel = Arc::clone(&channel);
            let host_peer_state = Arc::clone(&peer_state);
            let grant_id = peer.grant_id.clone();
            let label = reservation.label.clone();
            let key = peer.stream_key.clone();
            hosts.push(harness.transport_handle.spawn(async move {
                admit_reserved_subscription_channel(
                    &grant_id,
                    &label,
                    host_channel.as_ref(),
                    &key,
                    host_peer_state.as_ref(),
                )
                .await;
            }));
            channels.push(channel);
        }
        pump_test_control_until(&mut harness, "all entity channel hosts to bind", |_| {
            channels
                .iter()
                .all(|channel| channel.sent.lock().expect("sent frames").len() >= 2)
        });

        {
            let budget = harness
                .state
                .pending_runtime
                .admission
                .connection_budgets
                .get(&peer_generation)
                .expect("peer budget");
            for (index, (reservation, channel)) in
                reservations.iter().zip(channels.iter()).enumerate()
            {
                let bytes = if index < 29 { 65_536 } else { 98_304 };
                channel.outstanding_bytes.store(bytes, Ordering::Release);
                budget
                    .usage(&reservation.label)
                    .expect("entity route usage")
                    .store(bytes, Ordering::Release);
            }
            assert_eq!(
                budget.aggregate_buffered(),
                crate::admission::connection_budget::AGGREGATE_BUFFERED_HIGH
            );
            assert_eq!(budget.channel_count(), 32);
        }

        let refused = harness.request_on_peer(
            &mut peer,
            DaemonRequest::SubscribeEntities {
                entity_type: "session_type".to_string(),
                subscription_id: "host-overflow-31".to_string(),
            },
            "SubscribeEntities",
        );
        assert_eq!(
            refused.error.as_ref().map(|error| error.code.as_str()),
            Some("connection_channel_limit")
        );
        let budget = harness
            .state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&peer_generation)
            .expect("peer budget");
        assert_eq!(
            budget.channel_count(),
            crate::admission::connection_budget::MAX_TOTAL_CHANNELS - 1
        );
        assert_eq!(
            budget.aggregate_buffered(),
            crate::admission::connection_budget::AGGREGATE_BUFFERED_HIGH
        );

        let (authorize_tx, authorize_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::AuthorizeSubscriptionSend {
                grant_id: peer.grant_id.clone(),
                label: target.label.clone(),
                frame_len: 65_536,
                reply_tx: authorize_tx,
            },
        );
        assert!(
            authorize_rx
                .blocking_recv()
                .expect("65,536-byte authorization reply")
                .is_none()
        );

        let target_channel = channels.last().expect("target channel");
        let sent_before_overflow = target_channel
            .sent
            .lock()
            .expect("target sent frames")
            .len();
        require_host_event_before_close(
            &peer_state.mux,
            target_channel.as_ref(),
            botster_hub_client::DaemonEvent::RuntimeObservation {
                kind: format!(
                    "entity_subscription_closed:{}:{}:entity_subscription_overflow",
                    target.subscription_id, target.generation
                ),
            },
        );
        harness
            .state
            .entity_subscriptions
            .get(&target.subscription_id)
            .expect("target entity subscription")
            .send_frame_for_test(DaemonEntityFrame::Snapshot {
                subscription_id: target.subscription_id.clone(),
                entity_type: "session_type".to_string(),
                snapshot_seq: 2,
                items: vec![serde_json::json!({"payload": "x".repeat(65_536)})],
                resync_reason: None,
            })
            .expect("queue target overflow frame");
        let authorization_deadline = std::time::Instant::now() + Duration::from_secs(10);
        let authorization = loop {
            match harness.control_rx.try_recv() {
                Ok(message @ ControlMessage::AuthorizeSubscriptionSend { .. }) => break message,
                Ok(other) => {
                    handle_control_message(
                        &mut harness.daemon,
                        &mut harness.state,
                        &harness.terminal_path,
                        &harness.transport_handle,
                        harness.control_tx.clone(),
                        other,
                    );
                }
                Err(tokio_mpsc::error::TryRecvError::Empty)
                    if std::time::Instant::now() < authorization_deadline =>
                {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    panic!("timed out waiting for target authorization")
                }
                Err(error) => panic!("target authorization was not queued: {error}"),
            }
        };
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            authorization,
        );
        let target_host = hosts.pop().expect("target host");
        harness
            ._transport_runtime
            .block_on(target_host)
            .expect("target host joins");
        assert_eq!(
            target_channel
                .sent
                .lock()
                .expect("target sent frames")
                .len(),
            sent_before_overflow,
            "the refused entity frame must not reach the transport"
        );
        assert!(target_channel.closed.load(Ordering::Acquire));
        assert!(target_channel.close_probe_passed.load(Ordering::Acquire));
        assert_eq!(
            harness
                .state
                .pending_runtime
                .admission
                .connection_budgets
                .get(&peer_generation)
                .expect("peer budget")
                .aggregate_buffered(),
            crate::admission::connection_budget::AGGREGATE_BUFFERED_HIGH
        );

        let retirement_deadline = std::time::Instant::now() + Duration::from_secs(10);
        let automatic_retirement = loop {
            match harness.control_rx.try_recv() {
                Ok(message @ ControlMessage::RetireReservedSubscription { .. }) => break message,
                Ok(other) => panic!("expected automatic retirement, got {other:?}"),
                Err(tokio_mpsc::error::TryRecvError::Empty)
                    if std::time::Instant::now() < retirement_deadline =>
                {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    panic!("timed out waiting for automatic retirement")
                }
                Err(error) => panic!("automatic retirement was not queued: {error}"),
            }
        };
        assert!(matches!(
            &automatic_retirement,
            ControlMessage::RetireReservedSubscription { grant_id, label }
                if grant_id == &peer.grant_id && label == &target.label
        ));
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            automatic_retirement,
        );
        let budget = harness
            .state
            .pending_runtime
            .admission
            .connection_budgets
            .get(&peer_generation)
            .expect("peer budget");
        assert_eq!(budget.aggregate_buffered(), 1_998_848);
        assert!(budget.usage(&target.label).is_none());

        let sibling_sent_before = channels
            .iter()
            .take(30)
            .map(|channel| channel.sent.lock().expect("sibling sent frames").len())
            .collect::<Vec<_>>();
        for (index, reservation) in reservations.iter().take(30).enumerate() {
            assert!(!channels[index].close_started.load(Ordering::Acquire));
            assert!(!hosts[index].is_finished());
            harness
                .state
                .entity_subscriptions
                .get(&reservation.subscription_id)
                .expect("sibling entity subscription")
                .send_frame_for_test(DaemonEntityFrame::Error {
                    subscription_id: reservation.subscription_id.clone(),
                    entity_type: "session_type".to_string(),
                    code: "sibling_probe".to_string(),
                    message: "sibling remains usable".to_string(),
                })
                .expect("queue sibling probe");
        }
        pump_test_control_until(&mut harness, "all sibling payload controls", |_| {
            channels
                .iter()
                .take(30)
                .enumerate()
                .all(|(index, channel)| {
                    channel.sent.lock().expect("sibling sent frames").len()
                        > sibling_sent_before[index]
                })
        });
        for (index, channel) in channels.iter().take(30).enumerate() {
            assert!(!channel.close_started.load(Ordering::Acquire));
            assert!(!hosts[index].is_finished());
        }

        let replacement = harness.request_on_peer(
            &mut peer,
            DaemonRequest::SubscribeEntities {
                entity_type: "session_type".to_string(),
                subscription_id: "host-overflow-replacement".to_string(),
            },
            "SubscribeEntities",
        );
        assert_eq!(
            replacement.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        let event = harness.wait_for_host_event(&mut peer, "entity overflow");
        assert_eq!(
            event,
            botster_hub_client::DaemonEvent::RuntimeObservation {
                kind: format!(
                    "entity_subscription_closed:{}:{}:entity_subscription_overflow",
                    target.subscription_id, target.generation
                ),
            }
        );

        for channel in channels.iter().take(30) {
            channel.poll_ends.store(true, Ordering::Release);
            channel.event_notify.notify_waiters();
        }
        for host in hosts {
            harness
                ._transport_runtime
                .block_on(host)
                .expect("sibling host joins");
        }
        while let Ok(message) = harness.control_rx.try_recv() {
            handle_control_message(
                &mut harness.daemon,
                &mut harness.state,
                &harness.terminal_path,
                &harness.transport_handle,
                harness.control_tx.clone(),
                message,
            );
        }
        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn reject_extra_data_channel_closes_the_unclaimed_channel() {
        let extra = FakeDataChannel::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build extra-channel close runtime");
        runtime.block_on(reject_extra_data_channel(
            "grant-extra",
            false,
            EXTRA_DATA_CHANNEL_LABEL,
            &extra,
        ));
        assert!(
            extra.closed.load(Ordering::Acquire),
            "production reject path must finish local_close"
        );
    }

    #[test]
    fn extra_channel_close_marker_requires_lost_claim_and_close_ok() {
        let _lock = EXTRA_CHANNEL_ORACLE_ENV
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = std::env::temp_dir().join(format!(
            "so-2ch-label-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create label-control dir");
        let marker = dir.join("extra-closed");
        let observation = dir.join("extra-observation.json");
        let previous_env = std::env::var("BOTSTER_ENV").ok();
        let previous_marker = std::env::var(TEST_EXTRA_CHANNEL_CLOSE_MARKER_ENV).ok();
        let previous_observation = std::env::var(TEST_EXTRA_CHANNEL_OBSERVATION_ENV).ok();
        unsafe {
            std::env::set_var("BOTSTER_ENV", "test");
            std::env::set_var(TEST_EXTRA_CHANNEL_CLOSE_MARKER_ENV, &marker);
            std::env::set_var(TEST_EXTRA_CHANNEL_OBSERVATION_ENV, &observation);
        }
        let close = Ok(Ok(()));
        observe_rejected_data_channel_for_test(true, &close, "botster-client");
        assert!(
            !marker.exists(),
            "close marker must stay absent when the channel kept the claim"
        );
        observe_rejected_data_channel_for_test(false, &close, "botster-client");
        assert!(
            marker.exists(),
            "close marker must write for any rejected label after lost_claim and Ok(Ok(()))"
        );
        let observed: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&observation).expect("read complete observation"),
        )
        .expect("observation must contain complete JSON");
        assert_eq!(observed["lost_claim"], true);
        assert_eq!(observed["close_ok"], true);
        assert!(
            !observation.with_extension("tmp").exists(),
            "atomic observation publication must retire its temporary file"
        );
        std::fs::remove_file(&marker).expect("reset close marker");
        observe_rejected_data_channel_for_test(false, &close, EXTRA_DATA_CHANNEL_LABEL);
        assert!(
            marker.exists(),
            "close marker must write for botster-extra after lost_claim and Ok(Ok(()))"
        );
        unsafe {
            match previous_env {
                Some(value) => std::env::set_var("BOTSTER_ENV", value),
                None => std::env::remove_var("BOTSTER_ENV"),
            }
            match previous_marker {
                Some(value) => std::env::set_var(TEST_EXTRA_CHANNEL_CLOSE_MARKER_ENV, value),
                None => std::env::remove_var(TEST_EXTRA_CHANNEL_CLOSE_MARKER_ENV),
            }
            match previous_observation {
                Some(value) => std::env::set_var(TEST_EXTRA_CHANNEL_OBSERVATION_ENV, value),
                None => std::env::remove_var(TEST_EXTRA_CHANNEL_OBSERVATION_ENV),
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reserved_channel_rejection_reasons_are_distinct() {
        let reasons = [
            SubscriptionChannelRejectReason::Late,
            SubscriptionChannelRejectReason::Stale,
            SubscriptionChannelRejectReason::Duplicate,
            SubscriptionChannelRejectReason::Unreserved,
            SubscriptionChannelRejectReason::OverLimit,
        ];
        let tokens = reasons
            .into_iter()
            .map(SubscriptionChannelRejectReason::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(tokens.len(), reasons.len());
    }
}
