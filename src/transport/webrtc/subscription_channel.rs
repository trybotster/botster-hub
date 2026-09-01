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
    framed_daemon_entity_frame, framed_daemon_event, framed_daemon_hello_ack,
    framed_daemon_terminal_frame,
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
    if let Ok(path) = std::env::var(TEST_EXTRA_CHANNEL_OBSERVATION_ENV)
        && !path.is_empty()
    {
        let body = serde_json::json!({
            "lost_claim": lost_claim,
            "close_ok": close_ok,
            "label": label,
        })
        .to_string();
        let _ = std::fs::write(path, body);
    }
    if lost_claim
        && close_ok
        && let Ok(path) = std::env::var(TEST_EXTRA_CHANNEL_CLOSE_MARKER_ENV)
        && !path.is_empty()
    {
        let _ = std::fs::write(path, "closed\n");
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
            run_bound_subscription_channel(data_channel, stream_key, route, bound, hello_permits)
                .await;
            let _ = peer_state
                .runtime_tx
                .send(ControlMessage::RetireReservedSubscription {
                    grant_id: grant_id.to_string(),
                    label: label.to_string(),
                })
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
            _ = handle.wait_for_write() => {}
            inbound = data_channel.local_poll() => {
                match inbound {
                    Some(webrtc::data_channel::DataChannelEvent::OnMessage(message)) => {
                        let Ok(envelope) = serde_json::from_str::<botster_core::AesGcmEnvelope>(
                            std::str::from_utf8(message.data.as_ref()).unwrap_or(""),
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
    if handle.is_closed() {
        return Err(());
    }
    let Some(bytes) = handle.snapshot_active() else {
        return Ok(());
    };
    let frames = framed_daemon_terminal_frame(stream_key, &bytes).map_err(|_| ())?;
    let wire_len = frames.iter().map(String::len).sum();
    if !handle.resize_aggregate_permit(wire_len) {
        return Err(());
    }
    for frame in frames {
        data_channel.local_send_text(&frame).await.map_err(|_| ())?;
    }
    publish_channel_usage(data_channel, usage).await?;
    if !handle.is_closed() {
        let _ = handle.complete_active();
    }
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
