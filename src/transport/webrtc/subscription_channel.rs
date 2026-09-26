//! Reserved local WebRTC subscription DataChannels.
//!
//! One reserved channel carries one bound subscription: a terminal route
//! (binary sealed chunks both ways), an entity subscription, or a package
//! event subscription (encrypted JSON [`ServerFrame`] deliveries). The
//! channel opens with one encrypted `ClientFrame::Hello` and one
//! `ServerFrame::HelloAck`; it carries no requests.
use botster_core::AesGcmKey;
use botster_hub_client::{
    ClientFrame, DaemonCompatibility, DaemonDiagnostic, DaemonHello, DaemonHelloAck, PROTOCOL,
    PROTOCOL_VERSION, ServerFrame,
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
    LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH, LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW, decrypt_client_frame,
};
use crate::transport::webrtc::delivery::{
    InboundTerminalChunkAssembly, framed_server_frame, sealed_terminal_chunks,
};
use crate::transport::webrtc::peer::LocalWebrtcPeerState;

use crate::transport::webrtc::control_channel::LocalWebrtcDataChannel;
use crate::transport::webrtc::peer::LOCAL_WEBRTC_PEER_CLOSE_BOUND;
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
            Self::Late => RESERVATION_EXPIRED_REASON,
            Self::Stale => "stale",
            Self::Duplicate => "duplicate",
            Self::Unreserved => "unreserved",
            Self::OverLimit => "over_limit",
            Self::InvalidHello => "invalid_hello",
            Self::BindFailed => "bind_failed",
        }
    }
}

/// Typed reason for a reservation that expired before its channel opened.
/// Sent at expiry and again if the channel opens later.
pub(crate) const RESERVATION_EXPIRED_REASON: &str = "reservation_expired";

pub(crate) async fn reject_extra_data_channel<C>(grant_id: &str, label: &str, data_channel: &C)
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    eprintln!("local WebRTC rejecting extra DataChannel: grant_id={grant_id} label={label}");
    let _ = close_subscription_channel(data_channel).await;
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
    reject_extra_data_channel(grant_id, label, data_channel).await;
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

impl From<crate::subscription::attach_routes::AttachedSubscriptionChange>
    for LocalWebrtcAttachedSubscriptionChange
{
    fn from(change: crate::subscription::attach_routes::AttachedSubscriptionChange) -> Self {
        match change {
            crate::subscription::attach_routes::AttachedSubscriptionChange::Attach(
                subscription,
            ) => Self::Attach(LocalWebrtcAttachedSubscription {
                session_id: subscription.session_id,
                subscription_id: subscription.subscription_id,
            }),
            crate::subscription::attach_routes::AttachedSubscriptionChange::Detach(
                subscription,
            ) => Self::Detach(LocalWebrtcAttachedSubscription {
                session_id: subscription.session_id,
                subscription_id: subscription.subscription_id,
            }),
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
        reject_extra_data_channel(grant_id, label, data_channel).await;
        return;
    }
    let inspect = match inspect_rx.await {
        Ok(inspect) => inspect,
        Err(_) => {
            reject_extra_data_channel(grant_id, label, data_channel).await;
            return;
        }
    };
    let (class, subscription_id, generation) = match inspect {
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
            class,
            subscription_id,
            generation,
            ..
        } => (class, subscription_id, generation),
    };
    let Ok(hello) = receive_subscription_hello(data_channel, stream_key).await else {
        reject_reserved_data_channel(
            grant_id,
            label,
            SubscriptionChannelRejectReason::InvalidHello,
            data_channel,
            peer_state,
        )
        .await;
        return;
    };
    // A terminal channel acknowledges its Hello only after Core attached and
    // bound the route, because the HelloAck carries that route's generation.
    // Entity and event channels acknowledge before the bind.
    let early_permits = if class == crate::admission::connection_budget::ChannelClass::Terminal {
        None
    } else {
        match acknowledge_subscription_hello(
            data_channel,
            stream_key,
            &hello,
            peer_state,
            grant_id,
            label,
            None,
        )
        .await
        {
            Ok(permits) => Some(permits),
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
        }
    };
    if class == crate::admission::connection_budget::ChannelClass::Terminal
        && validate_subscription_hello(&hello).is_err()
    {
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
        reject_extra_data_channel(grant_id, label, data_channel).await;
        return;
    }
    match bind_rx.await {
        Ok(Ok(bound)) => {
            let (generation, hello_permits) = match (&bound, early_permits) {
                (
                    BoundSubscription::Terminal {
                        handle, generation, ..
                    },
                    _,
                ) => {
                    match acknowledge_subscription_hello(
                        data_channel,
                        stream_key,
                        &hello,
                        peer_state,
                        grant_id,
                        label,
                        Some(*generation),
                    )
                    .await
                    {
                        Ok(permits) => (*generation, permits),
                        Err(()) => {
                            // The route is bound but the client never learned
                            // its generation. Close the adapter so Core ends
                            // exactly this route, then retire the reservation
                            // as a driver exit does.
                            handle.close();
                            close_subscription_channel_or_fail_peer(data_channel, peer_state).await;
                            if !peer_state
                                .cleanup_sent
                                .load(std::sync::atomic::Ordering::Acquire)
                            {
                                let _ = peer_state
                                    .runtime_tx
                                    .send(ControlMessage::RetireReservedSubscription {
                                        grant_id: grant_id.to_string(),
                                        label: label.to_string(),
                                    })
                                    .await;
                            }
                            return;
                        }
                    }
                }
                (_, Some(permits)) => (generation, permits),
                (_, None) => unreachable!("entity and event channels acknowledge before the bind"),
            };
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
    if route
        .peer_state
        .cleanup_sent
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return;
    }
    let _ = route
        .peer_state
        .runtime_tx
        .send(ControlMessage::RetireReservedSubscription {
            grant_id: route.grant_id.to_string(),
            label: route.label.to_string(),
        })
        .await;
}

async fn receive_subscription_hello<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
) -> Result<DaemonHello, ()>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    loop {
        match data_channel.local_poll().await {
            Some(webrtc::data_channel::DataChannelEvent::OnMessage(message)) => {
                match decrypt_client_frame(stream_key, message.data.as_ref()) {
                    Some(ClientFrame::Hello { hello }) => return Ok(hello),
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

fn validate_subscription_hello(hello: &DaemonHello) -> Result<(), ()> {
    if hello.protocol != PROTOCOL || hello.compatibility.protocol_version != PROTOCOL_VERSION {
        return Err(());
    }
    if let Some(requirement) = hello.terminal_compatibility.as_ref()
        && ensure_terminal_compatible(requirement, &TerminalCompatibility::current()).is_err()
    {
        return Err(());
    }
    Ok(())
}

async fn acknowledge_subscription_hello<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    hello: &DaemonHello,
    peer_state: &LocalWebrtcPeerState,
    grant_id: &str,
    label: &str,
    terminal_generation: Option<u64>,
) -> Result<Vec<crate::admission::connection_budget::AggregateSendPermit>, ()>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    validate_subscription_hello(hello)?;
    let ack = DaemonHelloAck {
        protocol: PROTOCOL.to_string(),
        compatibility: DaemonCompatibility::current(),
        terminal_compatibility: Some(TerminalCompatibility::current()),
        diagnostics: vec![DaemonDiagnostic::connected("hello")],
        terminal_generation,
    };
    let frames = framed_server_frame(stream_key, &ServerFrame::HelloAck { ack }).map_err(|_| ())?;
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

/// Why the terminal channel driver stopped. Recorded once per driver termination
/// as a `RuntimeObservation` so a close can be attributed without changing flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalDriverExit {
    /// The driver observed a closed adapter before sending. It does not say who closed it.
    AdapterClosed,
    /// The driver observed a closed adapter while a frame send was in flight.
    AdapterClosedInFlight,
    ThresholdFailed,
    UsageFailed,
    FrameEncode,
    PermitRefused,
    SendFailed,
    /// An inbound chunk failed the header, order, generation, size, or
    /// authentication rules of the binary terminal channel contract.
    IngressAssembly,
    IngressRejected,
    RemoteClose,
    RemoteError,
    PollEnded,
}

impl TerminalDriverExit {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AdapterClosed => "adapter_closed",
            Self::AdapterClosedInFlight => "adapter_closed_in_flight",
            Self::ThresholdFailed => "threshold_failed",
            Self::UsageFailed => "usage_failed",
            Self::FrameEncode => "frame_encode",
            Self::PermitRefused => "permit_refused",
            Self::SendFailed => "send_failed",
            Self::IngressAssembly => "ingress_assembly",
            Self::IngressRejected => "ingress_rejected",
            Self::RemoteClose => "remote_close",
            Self::RemoteError => "remote_error",
            Self::PollEnded => "poll_ended",
        }
    }
}

fn observe_terminal_driver_exit(route: &BoundSubscriptionRoute<'_>, exit: TerminalDriverExit) {
    route
        .peer_state
        .mux
        .push_host_event(botster_hub_client::DaemonEvent::RuntimeObservation {
            kind: format!(
                "terminal_channel_closed:{}:{}:{}",
                route.subscription_id,
                route.generation,
                exit.as_str(),
            ),
        });
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
            observe_terminal_driver_exit(&route, TerminalDriverExit::ThresholdFailed);
        }
        close_subscription_channel_or_fail_peer(data_channel, route.peer_state).await;
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
            observe_terminal_driver_exit(&route, TerminalDriverExit::UsageFailed);
        }
        close_subscription_channel_or_fail_peer(data_channel, route.peer_state).await;
        return;
    }
    drop(hello_permits);
    match bound {
        BoundSubscription::Terminal { handle, usage, .. } => {
            let exit = run_bound_terminal_channel(
                data_channel,
                stream_key,
                route.peer_state,
                route.generation,
                handle,
                usage,
            )
            .await;
            observe_terminal_driver_exit(&route, exit);
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
    generation: u64,
    handle: WebRtcTerminalAdapterHandle,
    usage: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> TerminalDriverExit
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let mut inbound_assembly = InboundTerminalChunkAssembly::new(generation);
    // Per-channel outbound message id; the first Hub-to-client message is 1.
    let mut next_message_id: u64 = 1;
    let mut close_deadline = None;
    loop {
        if handle.is_closed() {
            close_subscription_channel_or_fail_peer(data_channel, peer_state).await;
            return TerminalDriverExit::AdapterClosed;
        }
        if close_deadline.is_none() {
            match flush_subscription_adapter_frames(
                data_channel,
                stream_key,
                &handle,
                &usage,
                &mut next_message_id,
            )
            .await
            {
                Ok(TerminalFlushOutcome::Ready) => {}
                Ok(TerminalFlushOutcome::ChannelClosed) => {
                    // The dependency removes the channel before it delivers OnClose.
                    // Keep reading accepted input until the terminal event arrives.
                    close_deadline =
                        Some(tokio::time::Instant::now() + LOCAL_WEBRTC_PEER_CLOSE_BOUND);
                }
                Err(exit) => {
                    close_subscription_channel_or_fail_peer(data_channel, peer_state).await;
                    handle.close();
                    return exit;
                }
            }
        }
        if close_deadline.is_none() {
            let _ = publish_channel_usage(data_channel, &usage).await;
            peer_state.mux.refresh_aggregate_pressure();
        }
        tokio::select! {
            biased;
            _ = handle.wait_for_write() => {}
            inbound = data_channel.local_poll() => {
                match inbound {
                    Some(webrtc::data_channel::DataChannelEvent::OnMessage(message)) => {
                        let bytes = match inbound_assembly.push(stream_key, message.data.as_ref()) {
                            Ok(Some(bytes)) => bytes,
                            Ok(None) => continue,
                            Err(()) => {
                                handle.close();
                                close_subscription_channel_or_fail_peer(data_channel, peer_state).await;
                                return TerminalDriverExit::IngressAssembly;
                            }
                        };
                        if handle.push_ingress(bytes).is_err() {
                            handle.close();
                            close_subscription_channel_or_fail_peer(data_channel, peer_state).await;
                            return TerminalDriverExit::IngressRejected;
                        }
                    }
                    Some(event @ (webrtc::data_channel::DataChannelEvent::OnBufferedAmountHigh
                    | webrtc::data_channel::DataChannelEvent::OnBufferedAmountLow)) => {
                        apply_subscription_pressure_event(&handle, &event);
                        let _ = publish_channel_usage(data_channel, &usage).await;
                        peer_state.mux.refresh_aggregate_pressure();
                    }
                    Some(webrtc::data_channel::DataChannelEvent::OnClose) => {
                        handle.close();
                        close_subscription_channel_or_fail_peer(data_channel, peer_state).await;
                        return TerminalDriverExit::RemoteClose;
                    }
                    Some(webrtc::data_channel::DataChannelEvent::OnError) => {
                        handle.close();
                        close_subscription_channel_or_fail_peer(data_channel, peer_state).await;
                        return TerminalDriverExit::RemoteError;
                    }
                    None => {
                        handle.close();
                        close_subscription_channel_or_fail_peer(data_channel, peer_state).await;
                        return TerminalDriverExit::PollEnded;
                    }
                    Some(_) => {}
                }
            }
            _ = async {
                match close_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                handle.close();
                close_subscription_channel_or_fail_peer(data_channel, peer_state).await;
                return TerminalDriverExit::SendFailed;
            }
        }
    }
}

async fn run_bound_entity_channel<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    route: BoundSubscriptionRoute<'_>,
    mut receiver: tokio::sync::mpsc::Receiver<crate::entity_delivery::EntityDelivery>,
    usage: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let mut peer_terminal_rx = route.peer_state.subscribe_peer_terminal();
    'driver: loop {
        tokio::select! {
            biased;
            frame = receiver.recv() => {
                let Some(entity) = frame else { break };
                route.peer_state.entity_capacity_wake.publish();
                let (frames, retained_delivery) = match entity {
                    crate::entity_delivery::EntityDelivery::Typed(entity) => (framed_server_frame(stream_key, &ServerFrame::Entity { entity }), None),
                    crate::entity_delivery::EntityDelivery::Encoded(delivery) => {
                        let frames = crate::transport::webrtc::delivery::framed_encoded_entity(stream_key, delivery.json());
                        (frames, Some(delivery))
                    }
                };
                let Ok(frames) = frames else { break };
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
                        close_subscription_channel_or_fail_peer(data_channel, route.peer_state).await;
                        drop(permit);
                        return;
                    }
                    if publish_channel_usage(data_channel, &usage).await.is_err() {
                        close_subscription_channel_or_fail_peer(data_channel, route.peer_state).await;
                        drop(permit);
                        return;
                    }
                    drop(permit);
                    route.peer_state.mux.refresh_aggregate_pressure();
                }
                drop(retained_delivery);
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
    close_subscription_channel_or_fail_peer(data_channel, route.peer_state).await;
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
            let Ok(frames) = framed_server_frame(
                stream_key,
                &ServerFrame::Event {
                    event: event.clone(),
                },
            ) else {
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
                    close_subscription_channel_or_fail_peer(data_channel, route.peer_state).await;
                    drop(permit);
                    return;
                }
                if publish_channel_usage(data_channel, &usage).await.is_err() {
                    close_subscription_channel_or_fail_peer(data_channel, route.peer_state).await;
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
    close_subscription_channel_or_fail_peer(data_channel, route.peer_state).await;
}

async fn close_subscription_channel_or_fail_peer<C>(
    data_channel: &C,
    peer_state: &LocalWebrtcPeerState,
) where
    C: LocalWebrtcDataChannel + ?Sized,
{
    if !matches!(close_subscription_channel(data_channel).await, Ok(Ok(()))) {
        let cause = crate::transport::webrtc::peer::LocalWebrtcTerminalCause::ChannelError;
        peer_state.publish_peer_terminal(cause);
        peer_state.cleanup_once(cause).await;
    }
}

async fn close_subscription_channel<C>(
    data_channel: &C,
) -> Result<Result<(), String>, tokio::time::error::Elapsed>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, data_channel.local_close()).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UsageQueryFailure {
    ChannelClosed,
    Other,
}

async fn publish_channel_usage<C>(
    data_channel: &C,
    usage: &std::sync::atomic::AtomicUsize,
) -> Result<(), UsageQueryFailure>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let bytes = data_channel
        .local_outstanding_bytes()
        .await
        .map_err(|error| match error {
            webrtc::error::Error::ErrDataChannelClosed => UsageQueryFailure::ChannelClosed,
            _ => UsageQueryFailure::Other,
        })?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalFlushOutcome {
    Ready,
    ChannelClosed,
}

/// Seal and send the adapter's active routed frame as ordered binary chunks.
/// The sender seals the shared terminal bytes without text encoding or serialization.
async fn flush_subscription_adapter_frames<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    handle: &WebRtcTerminalAdapterHandle,
    usage: &std::sync::atomic::AtomicUsize,
    next_message_id: &mut u64,
) -> Result<TerminalFlushOutcome, TerminalDriverExit>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    let Some(frame) = handle.snapshot_active() else {
        return if handle.is_closed() {
            Err(TerminalDriverExit::AdapterClosed)
        } else {
            Ok(TerminalFlushOutcome::Ready)
        };
    };
    let message_id = *next_message_id;
    let chunks = sealed_terminal_chunks(stream_key, &frame, message_id)
        .map_err(|_| TerminalDriverExit::FrameEncode)?;
    let wire_len = chunks.iter().map(Vec::len).sum();
    if !handle.transfer_aggregate_permit(wire_len, usage) {
        return Err(if handle.is_closed() {
            TerminalDriverExit::AdapterClosed
        } else {
            TerminalDriverExit::PermitRefused
        });
    }
    // The id is consumed once sealing succeeds; a failed send closes the
    // channel, so the gap is never observed.
    *next_message_id = message_id.wrapping_add(1);
    let sent = async {
        for chunk in &chunks {
            let send = data_channel.local_send_binary(chunk);
            tokio::pin!(send);
            loop {
                if handle.is_closed() {
                    return Err(TerminalDriverExit::AdapterClosedInFlight);
                }
                tokio::select! {
                    biased;
                    _ = handle.wait_for_write() => {}
                    result = &mut send => {
                        match result {
                            Ok(()) => {}
                            Err(webrtc::error::Error::ErrDataChannelClosed) => {
                                return Ok(TerminalFlushOutcome::ChannelClosed);
                            }
                            Err(_) => return Err(TerminalDriverExit::SendFailed),
                        }
                        break;
                    }
                }
            }
        }
        Ok(TerminalFlushOutcome::Ready)
    }
    .await;
    // Cancellation keeps the conservative count until this refresh succeeds.
    let published = publish_channel_usage(data_channel, usage).await;
    let outcome = sent?;
    if outcome == TerminalFlushOutcome::ChannelClosed {
        return Ok(outcome);
    }
    match published {
        Ok(()) => {}
        Err(UsageQueryFailure::ChannelClosed) => {
            return Ok(TerminalFlushOutcome::ChannelClosed);
        }
        Err(UsageQueryFailure::Other) => return Err(TerminalDriverExit::UsageFailed),
    }
    let _ = handle.complete_active();
    Ok(TerminalFlushOutcome::Ready)
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;
    use botster_terminal_protocol::{RouteId, RoutedTerminalFrame, encode_output};

    fn test_frame(body: &[u8]) -> RoutedTerminalFrame {
        RoutedTerminalFrame::new(
            RouteId::new("route").expect("route"),
            1,
            0,
            encode_output(body).expect("output frame"),
        )
    }
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
    fn terminal_closed_send_waits_for_close_event_without_replay() {
        check_terminal_send_failure(true, true, TerminalDriverExit::RemoteClose);
    }

    #[test]
    fn terminal_closed_send_without_close_event_is_bounded() {
        check_terminal_send_failure(true, false, TerminalDriverExit::SendFailed);
    }

    #[test]
    fn terminal_send_error_stays_send_failed() {
        check_terminal_send_failure(false, false, TerminalDriverExit::SendFailed);
    }

    #[test]
    fn terminal_closed_usage_query_enters_close_grace_after_send() {
        check_terminal_usage_query_failure(true, Ok(TerminalFlushOutcome::ChannelClosed));
    }

    #[test]
    fn terminal_other_usage_query_error_stays_usage_failed_after_send() {
        check_terminal_usage_query_failure(false, Err(TerminalDriverExit::UsageFailed));
    }

    fn check_terminal_usage_query_failure(
        closed: bool,
        expected: Result<TerminalFlushOutcome, TerminalDriverExit>,
    ) {
        let channel = FakeDataChannel::default();
        channel.usage_closed.store(closed, Ordering::Release);
        channel.usage_fails.store(!closed, Ordering::Release);
        let (mut adapter, handle) =
            crate::transport::webrtc::adapter::WebRtcTerminalAdapter::pair();
        adapter
            .try_write(&test_frame(b"pending output"))
            .expect("queue terminal output");
        let key = AesGcmKey::from_slice(&[20; 32]).expect("test key");
        let initial_usage = 17;
        let usage = std::sync::atomic::AtomicUsize::new(initial_usage);
        let mut next_message_id = 1;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build the usage-query runtime");
        let result = runtime.block_on(flush_subscription_adapter_frames(
            &channel,
            &key,
            &handle,
            &usage,
            &mut next_message_id,
        ));
        assert_eq!(result, expected);
        let sent = channel.sent_binary.lock().expect("sent chunks");
        assert_eq!(sent.len(), 1);
        let sent_bytes = sent.iter().map(Vec::len).sum::<usize>();
        assert!(channel.usage_entered.load(Ordering::Acquire));
        assert_eq!(usage.load(Ordering::Acquire), initial_usage + sent_bytes);
        assert!(handle.snapshot_active().is_some());
        assert_eq!(next_message_id, 2);
    }

    fn check_terminal_send_failure(
        closed: bool,
        deliver_close: bool,
        expected: TerminalDriverExit,
    ) {
        let channel = FakeDataChannel::default();
        channel.send_closed.store(closed, Ordering::Release);
        channel.send_fails.store(!closed, Ordering::Release);
        let peer_state = test_peer_state("send-close-order");
        let (mut adapter, handle) =
            crate::transport::webrtc::adapter::WebRtcTerminalAdapter::pair();
        adapter
            .try_write(&test_frame(b"pending output"))
            .expect("queue terminal output");
        let key = AesGcmKey::from_slice(&[19; 32]).expect("test key");
        let usage = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build the close-order runtime");
        let exit = runtime.block_on(async {
            let driver = run_bound_terminal_channel(&channel, &key, &peer_state, 1, handle.clone(), usage);
            tokio::pin!(driver);
            if closed {
                tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::select! {
                        exit = &mut driver => panic!("the driver exited before the close event: {exit:?}"),
                        _ = channel.send_notify.notified() => {}
                    }
                }).await.expect("the driver attempted the closed send");
                assert!(channel.send_entered.load(Ordering::Acquire));
                assert!(!handle.is_closed());
                // A retry would now succeed and appear in the send log.
                channel.send_closed.store(false, Ordering::Release);
                if deliver_close {
                    channel.push_event(DataChannelEvent::OnClose);
                }
            }
            tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND + Duration::from_secs(1), driver)
                .await.expect("the driver has a bounded terminal exit")
        });
        assert_eq!(exit.as_str(), expected.as_str());
        assert!(handle.is_closed());
        assert!(channel.closed.load(Ordering::Acquire));
        assert!(channel.sent.lock().expect("sent frames").is_empty());
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
                generation: 1,
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

    fn flush_once(
        channel: &FakeDataChannel,
        handle: &WebRtcTerminalAdapterHandle,
        usage: &std::sync::atomic::AtomicUsize,
        next_message_id: &mut u64,
    ) -> Result<TerminalFlushOutcome, TerminalDriverExit> {
        let key = AesGcmKey::from_slice(&[13; 32]).expect("test key");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(flush_subscription_adapter_frames(
                channel,
                &key,
                handle,
                usage,
                next_message_id,
            ))
    }

    /// The write permit covers the frame's sealed size, so an admitted frame
    /// always flushes: a full aggregate refuses the write, never the flush.
    #[test]
    fn an_admitted_frame_flushes_when_the_aggregate_is_exactly_full() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, ChannelClass, ConnectionBudget,
        };
        use crate::transport::webrtc::delivery::sealed_terminal_wire_len;
        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("route".to_string(), ChannelClass::Terminal)
            .expect("reserve route");
        let sibling_usage = budget
            .reserve("sibling".to_string(), ChannelClass::Terminal)
            .expect("reserve sibling");
        let mux = WebRtcConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        let frame = test_frame(b"output");
        let wire_len = sealed_terminal_wire_len(frame.frame.len()).expect("wire len");
        assert!(wire_len > frame.frame.len());
        sibling_usage.store(AGGREGATE_BUFFERED_HIGH - wire_len, Ordering::Release);
        assert_eq!(adapter.try_write(&frame), Ok(()));
        // A sibling takes every byte the admitted write left free before
        // the route flushes.
        sibling_usage.fetch_add(
            AGGREGATE_BUFFERED_HIGH - budget.aggregate_buffered(),
            Ordering::AcqRel,
        );
        assert_eq!(budget.aggregate_buffered(), AGGREGATE_BUFFERED_HIGH);
        let channel = FakeDataChannel::default();
        let mut next_message_id = 1u64;
        assert_eq!(
            flush_once(&channel, &handle, &usage, &mut next_message_id),
            Ok(TerminalFlushOutcome::Ready)
        );
        assert_eq!(next_message_id, 2);
        assert!(!channel.sent_binary.lock().expect("sent").is_empty());
    }

    /// The high mark is a high-water mark: a frame larger than the mark is
    /// delivered on an idle peer, and the next oversize frame waits for the
    /// drain. The transport's only drain event is a channel crossing down to
    /// its low threshold, so that event must resume the waiter even though
    /// bytes at the threshold remain, and a small sibling that never crossed
    /// it holds bytes with no event of its own.
    #[test]
    fn an_oversize_frame_on_an_idle_peer_is_delivered_and_the_next_waits_for_the_drain() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, CHANNEL_DRAINED_BYTES, ChannelClass, ConnectionBudget,
        };
        use crate::transport::webrtc::delivery::{
            LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES, sealed_terminal_wire_len,
        };
        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("route".to_string(), ChannelClass::Terminal)
            .expect("reserve route");
        let sibling_usage = budget
            .reserve("sibling".to_string(), ChannelClass::Terminal)
            .expect("reserve sibling");
        let mux = WebRtcConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        mux.register("s".into(), "route".into(), 1, handle.clone());
        let (mut next, next_handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        mux.register("s".into(), "next".into(), 1, next_handle.clone());
        let oversize = test_frame(&vec![
            b'o';
            AGGREGATE_BUFFERED_HIGH + AGGREGATE_BUFFERED_HIGH / 4
        ]);
        let wire_len = sealed_terminal_wire_len(oversize.frame.len()).expect("wire len");
        assert!(wire_len > AGGREGATE_BUFFERED_HIGH);
        assert_eq!(budget.aggregate_buffered(), 0);
        assert_eq!(adapter.try_write(&oversize), Ok(()));
        // A small sibling sends meanwhile. Published at its send and never
        // above its low threshold, so the transport reports nothing further
        // for it.
        sibling_usage.store(1024, Ordering::Release);
        // Above the mark, nothing more is authorized.
        let next_frame = oversize.clone();
        assert_eq!(
            next.try_write(&next_frame),
            Err(TerminalAdapterWriteError::WouldBlock)
        );
        assert!(next_handle.aggregate_blocked_for_test());

        let channel = FakeDataChannel::default();
        // The transport still holds every sealed byte after the send.
        channel.outstanding_bytes.store(wire_len, Ordering::Release);
        let mut next_message_id = 1u64;
        assert_eq!(
            flush_once(&channel, &handle, &usage, &mut next_message_id),
            Ok(TerminalFlushOutcome::Ready)
        );
        let sent = channel.sent_binary.lock().expect("sent").clone();
        assert_eq!(
            sent.len(),
            oversize
                .frame
                .len()
                .div_ceil(LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES)
        );
        assert_eq!(sent.iter().map(Vec::len).sum::<usize>(), wire_len);
        assert_eq!(budget.aggregate_buffered(), wire_len + 1024);
        mux.refresh_aggregate_pressure();
        assert!(
            next_handle.aggregate_blocked_for_test(),
            "the next frame waits while the oversize frame is buffered"
        );

        // The drain event: the channel crosses down to its low threshold
        // (rtc-sctp fires once, as old > threshold and new <= threshold),
        // and the driver's event arm publishes usage, then refreshes the
        // peer's routes. Bytes at the threshold remain.
        channel
            .outstanding_bytes
            .store(CHANNEL_DRAINED_BYTES, Ordering::Release);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async {
                publish_channel_usage(&channel, &usage)
                    .await
                    .expect("publish usage");
                mux.refresh_aggregate_pressure();
                tokio::time::timeout(Duration::from_secs(1), next_handle.wait_for_write())
                    .await
                    .expect("the drain wakes the waiting frame");
            });
        assert!(!next_handle.aggregate_blocked_for_test());
        assert_eq!(next.try_write(&next_frame), Ok(()));
    }

    /// A write the aggregate refuses waits, and capacity released outside
    /// the route's own loop wakes it: a retired sibling channel, or a
    /// closing sibling adapter's permit.
    #[test]
    fn released_capacity_wakes_a_route_the_aggregate_refused() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, ChannelClass, ConnectionBudget,
        };
        use crate::transport::webrtc::delivery::sealed_terminal_wire_len;
        for release_by_closing_sibling in [false, true] {
            let mut budget = ConnectionBudget::default();
            let usage = budget
                .reserve("route".to_string(), ChannelClass::Terminal)
                .expect("reserve route");
            let sibling_usage = budget
                .reserve("sibling".to_string(), ChannelClass::Terminal)
                .expect("reserve sibling");
            let mux = WebRtcConnectionMux::new();
            let (mut adapter, handle) = mux.create_adapter_with_aggregate(budget.aggregate());
            let (mut sibling_adapter, sibling_handle) =
                mux.create_adapter_with_aggregate(budget.aggregate());
            let frame = test_frame(b"output");
            let wire_len = sealed_terminal_wire_len(frame.frame.len()).expect("wire len");
            let sibling_held = if release_by_closing_sibling {
                let sibling_frame = test_frame(b"sibling output");
                assert_eq!(sibling_adapter.try_write(&sibling_frame), Ok(()));
                sealed_terminal_wire_len(sibling_frame.frame.len()).expect("wire len")
            } else {
                0
            };
            // One byte short of room for the route's sealed frame.
            sibling_usage.store(
                AGGREGATE_BUFFERED_HIGH - wire_len - sibling_held + 1,
                Ordering::Release,
            );
            assert_eq!(
                adapter.try_write(&frame),
                Err(TerminalAdapterWriteError::WouldBlock)
            );
            assert!(handle.aggregate_blocked_for_test());
            assert!(!handle.is_closed());

            if release_by_closing_sibling {
                sibling_usage.store(0, Ordering::Release);
                sibling_handle.close();
            } else {
                assert!(budget.release("sibling"));
            }
            assert!(
                !handle.aggregate_blocked_for_test(),
                "the release woke the refused route"
            );
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(1), handle.wait_for_write())
                        .await
                        .expect("the release notifies the route's writer");
                });
            assert_eq!(adapter.try_write(&frame), Ok(()));
            let channel = FakeDataChannel::default();
            let mut next_message_id = 1u64;
            assert_eq!(
                flush_once(&channel, &handle, &usage, &mut next_message_id),
                Ok(TerminalFlushOutcome::Ready)
            );
            assert!(!channel.sent_binary.lock().expect("sent").is_empty());
            drop(sibling_adapter);
        }
    }

    /// close_all holds the route table while it closes handles; a handle
    /// that releases its permit there must wake waiters without that table.
    #[test]
    fn close_all_releases_held_permits_without_reentering_the_route_table() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, AGGREGATE_BUFFERED_LOW, ConnectionBudget,
        };
        let budget = ConnectionBudget::default();
        let mux = WebRtcConnectionMux::new();
        let (mut holder, holder_handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        mux.register("s".into(), "holder".into(), 1, holder_handle.clone());
        let (mut waiter, waiter_handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        // The held permit alone refuses the waiter's write, and its release
        // brings the aggregate below the low mark, so the release runs the
        // waiters while close_all holds the route table.
        assert_eq!(
            holder.try_write(&test_frame(&vec![
                b'h';
                AGGREGATE_BUFFERED_HIGH / 2 - 64 * 1024
            ])),
            Ok(())
        );
        assert!(budget.aggregate_buffered() < AGGREGATE_BUFFERED_LOW);
        assert_eq!(
            waiter.try_write(&test_frame(&vec![
                b'w';
                AGGREGATE_BUFFERED_HIGH / 2 + 128 * 1024
            ])),
            Err(TerminalAdapterWriteError::WouldBlock)
        );
        assert!(waiter_handle.aggregate_blocked_for_test());
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let closing = mux.clone();
        std::thread::spawn(move || {
            closing.close_all();
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("close_all returns while it releases a held permit");
        assert!(holder_handle.is_closed());
        assert!(
            !waiter_handle.aggregate_blocked_for_test(),
            "the released permit woke the refused writer"
        );
        drop((holder, waiter));
    }

    #[test]
    fn hard_close_abandons_occupied_frame_before_flush() {
        use crate::admission::connection_budget::{ChannelClass, ConnectionBudget};
        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("route".to_string(), ChannelClass::Terminal)
            .expect("reserve route");
        let mux = WebRtcConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        let frame = test_frame(b"output");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert!(budget.aggregate_buffered() > 0);
        handle.close();
        let channel = FakeDataChannel::default();
        let key = AesGcmKey::from_slice(&[7; 32]).expect("test key");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        assert!(
            runtime
                .block_on(flush_subscription_adapter_frames(
                    &channel, &key, &handle, &usage, &mut 1u64,
                ))
                .is_err()
        );
        assert!(channel.sent.lock().expect("sent frames").is_empty());
        assert_eq!(budget.aggregate_buffered(), 0);
        assert!(handle.snapshot_active().is_none());
        assert_eq!(
            adapter.try_write(&frame),
            Err(botster_core::contract::terminal_adapter::TerminalAdapterWriteError::Closed)
        );
    }

    #[test]
    fn hard_close_cancels_pending_send_without_replay() {
        use crate::admission::connection_budget::{ChannelClass, ConnectionBudget};
        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("route".to_string(), ChannelClass::Terminal)
            .expect("reserve route");
        let mux = WebRtcConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        let frame = test_frame(b"output");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        let channel = Arc::new(FakeDataChannel::default());
        channel.send_hangs.store(true, Ordering::Release);
        let key = AesGcmKey::from_slice(&[11; 32]).expect("test key");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let flush = runtime.spawn({
            let channel = Arc::clone(&channel);
            let handle = handle.clone();
            let usage = Arc::clone(&usage);
            let key = key.clone();
            async move {
                flush_subscription_adapter_frames(
                    channel.as_ref(),
                    &key,
                    &handle,
                    &usage,
                    &mut 1u64,
                )
                .await
            }
        });
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !channel.send_entered.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("send entered");
        });
        // Sibling progress while this route stays blocked: a second route on
        // its own channel completes a send before the first route closes.
        let sibling_channel = FakeDataChannel::default();
        let (mut sibling, sibling_handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        assert_eq!(sibling.try_write(&frame), Ok(()));
        runtime
            .block_on(flush_subscription_adapter_frames(
                &sibling_channel,
                &key,
                &sibling_handle,
                &usage,
                &mut 1u64,
            ))
            .expect("sibling sends while the first route is blocked");
        assert_eq!(sibling_channel.sent.lock().expect("sibling sends").len(), 1);
        assert!(
            !flush.is_finished(),
            "the blocked route must still be pending when the sibling completes"
        );
        assert!(channel.sent.lock().expect("sent frames").is_empty());
        // Bounded growth: the blocked route keeps one in-flight frame and
        // refuses a second one instead of queueing it.
        let buffered_while_blocked = budget.aggregate_buffered();
        assert!(
            matches!(
                adapter.try_write(&frame),
                Err(TerminalAdapterWriteError::Full | TerminalAdapterWriteError::WouldBlock)
            ),
            "a blocked route must not queue beyond its bounded slot"
        );
        assert_eq!(budget.aggregate_buffered(), buffered_while_blocked);
        handle.close_from_host();
        handle.close();
        let result = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), flush)
                .await
                .expect("close cancels pending send")
                .expect("flush task")
        });
        assert!(result.is_err());
        assert!(handle.host_closed());
        assert!(channel.sent.lock().expect("sent frames").is_empty());
        assert_eq!(budget.aggregate_buffered(), 0);
        channel.send_hangs.store(false, Ordering::Release);
        channel.send_notify.notify_waiters();
        assert!(
            runtime
                .block_on(flush_subscription_adapter_frames(
                    channel.as_ref(),
                    &key,
                    &handle,
                    &usage,
                    &mut 2u64,
                ))
                .is_err()
        );
        assert!(channel.sent.lock().expect("sent frames").is_empty());
        // The sibling route keeps sending after the first route closed.
        assert_eq!(sibling.try_write(&frame), Ok(()));
        runtime
            .block_on(flush_subscription_adapter_frames(
                &sibling_channel,
                &key,
                &sibling_handle,
                &usage,
                &mut 2u64,
            ))
            .expect("sibling sends after the first route closed");
        assert_eq!(sibling_channel.sent.lock().expect("sibling sends").len(), 2);
    }

    #[test]
    fn hard_close_keeps_accepted_chunks_budgeted_until_usage_refresh() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, ChannelClass, ConnectionBudget,
        };
        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("route".to_string(), ChannelClass::Terminal)
            .expect("reserve route");
        let mux = WebRtcConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        // Six sealed chunks: the fake accepts the first and hangs the second.
        // The frame's sealed bytes exceed one channel's drained mark, so the
        // admission below counts them rather than the quiescent exception,
        // and the frame stays within the chunk reassembly limit used below.
        let frame = test_frame(&vec![b'x'; 65_532]);
        assert!(
            crate::transport::webrtc::delivery::sealed_terminal_wire_len(frame.frame.len())
                .expect("wire len")
                > crate::admission::connection_budget::CHANNEL_DRAINED_BYTES
        );
        assert_eq!(adapter.try_write(&frame), Ok(()));
        let channel = Arc::new(FakeDataChannel::default());
        channel.hang_after_first_send.store(true, Ordering::Release);
        channel.usage_hangs.store(true, Ordering::Release);
        let key = AesGcmKey::from_slice(&[13; 32]).expect("test key");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let flush = runtime.spawn({
            let channel = Arc::clone(&channel);
            let handle = handle.clone();
            let usage = Arc::clone(&usage);
            let key = key.clone();
            async move {
                flush_subscription_adapter_frames(
                    channel.as_ref(),
                    &key,
                    &handle,
                    &usage,
                    &mut 1u64,
                )
                .await
            }
        });
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !channel.send_entered.load(Ordering::Acquire)
                    || channel.sent.lock().expect("sent").len() != 1
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("first chunk accepted and second send pending");
        });
        let accepted = channel.outstanding_bytes.load(Ordering::Acquire);
        assert!(accepted > 0);
        assert!(budget.aggregate_buffered() >= accepted);
        handle.close();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !channel.usage_entered.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled send reaches usage refresh");
        });
        assert!(
            budget.aggregate_buffered() >= accepted,
            "close must retain accounting for accepted chunks"
        );
        assert!(
            budget
                .aggregate()
                .try_authorize(AGGREGATE_BUFFERED_HIGH - accepted + 1)
                .is_none(),
            "concurrent admission must include accepted bytes"
        );
        assert_eq!(channel.sent.lock().expect("sent").len(), 1);
        channel.usage_hangs.store(false, Ordering::Release);
        channel.usage_notify.notify_waiters();
        assert!(
            runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(2), flush)
                        .await
                        .expect("refresh completes")
                        .expect("flush task")
                })
                .is_err()
        );
        assert_eq!(budget.aggregate_buffered(), accepted);
        // Accounting alone is not completion: the one accepted chunk never
        // reassembles into a message on the receiving side.
        let sent = channel.sent_binary.lock().expect("sent chunks").clone();
        assert_eq!(sent.len(), 1);
        let mut assembly = InboundTerminalChunkAssembly::new(1);
        assert_eq!(
            assembly.push(&key, &sent[0]),
            Ok(None),
            "the accepted chunk alone must not complete the message"
        );
        assert!(assembly.is_partial());
        channel.outstanding_bytes.store(0, Ordering::Release);
        runtime
            .block_on(publish_channel_usage(channel.as_ref(), &usage))
            .expect("drained channel refresh");
        assert_eq!(budget.aggregate_buffered(), 0);
        assert_eq!(
            channel.sent.lock().expect("sent").len(),
            1,
            "cancelled chunk must not replay"
        );
    }

    #[test]
    fn hard_close_in_flight_ends_the_stream_before_a_truncated_message_completes() {
        use botster_hub_client::LocalWebrtcTerminalChunkHeader;
        let (mut adapter, handle) =
            crate::transport::webrtc::adapter::WebRtcTerminalAdapter::pair();
        // Four sealed chunks: the fake accepts the first and hangs the second.
        assert_eq!(adapter.try_write(&test_frame(&vec![b'x'; 40_000])), Ok(()));
        let channel = Arc::new(FakeDataChannel::default());
        channel.hang_after_first_send.store(true, Ordering::Release);
        let usage = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let key = AesGcmKey::from_slice(&[17; 32]).expect("test key");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let driver = runtime.spawn({
            let channel = Arc::clone(&channel);
            let handle = handle.clone();
            let usage = Arc::clone(&usage);
            let key = key.clone();
            async move {
                let peer_state = test_peer_state("grant-truncated");
                run_bound_subscription_channel(
                    channel.as_ref(),
                    &key,
                    BoundSubscriptionRoute {
                        peer_state: &peer_state,
                        grant_id: "grant-truncated",
                        label: "route-truncated",
                        subscription_id: "sub-truncated",
                        generation: 1,
                    },
                    BoundSubscription::Terminal {
                        handle,
                        usage,
                        generation: 1,
                    },
                    Vec::new(),
                )
                .await;
            }
        });
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !channel.send_entered.load(Ordering::Acquire)
                    || channel.sent.lock().expect("sent").len() != 1
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("first chunk accepted and second send pending");
        });
        assert!(!driver.is_finished());
        handle.close_from_host();
        handle.close();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), driver)
                .await
                .expect("the driver exits after the in-flight close")
                .expect("driver task");
        });
        assert!(
            channel.closed.load(Ordering::Acquire),
            "the stream must end with an explicit DataChannel close"
        );
        assert!(handle.is_closed());
        let sent = channel.sent_binary.lock().expect("sent chunks").clone();
        assert_eq!(
            sent.len(),
            1,
            "no further chunk of the abandoned message may follow the close"
        );
        let (header, _) = LocalWebrtcTerminalChunkHeader::decode(&sent[0]).expect("chunk header");
        assert_eq!(header.chunk_index, 0);
        assert!(
            header.chunk_count > 1,
            "the abandoned message spans several chunks"
        );
        let mut assembly = InboundTerminalChunkAssembly::new(1);
        assert_eq!(
            assembly.push(&key, &sent[0]),
            Ok(None),
            "a truncated message must never complete on the receiving side"
        );
        assert!(assembly.is_partial());
    }

    #[test]
    fn remote_closed_subscription_keeps_host_sibling_live() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("remote-closed-subscription");
        let mut peer = harness.signal_peer("http://127.0.0.1:41919");
        harness.ensure_webrtc_adapter_hello(&mut peer);
        // The driver reports why a terminal channel stopped as a RuntimeObservation host
        // event to the owning peer, admission-free (`observe_terminal_driver_exit`). This
        // peer parks host events so that report can be asserted after the remote close.
        peer.enable_host_events();
        let session_id = "remote-close-session";
        let subscription_id = "remote-close-target";
        harness.spawn_and_attach_on_peer(&mut peer, session_id, subscription_id);
        let label = peer
            .offer_peer
            .as_ref()
            .expect("offer peer")
            .reserved_channels
            .keys()
            .next()
            .expect("terminal channel label")
            .clone();
        let peer_state = harness
            .daemon
            .local_webrtc()
            .peer_states
            .get(&peer.grant_id)
            .expect("live peer state")
            .clone();
        let channel = Arc::clone(
            &peer
                .offer_peer
                .as_ref()
                .expect("offer peer")
                .reserved_channels
                .get(&label)
                .expect("target channel")
                .channel,
        );
        let peer_generation = match harness
            .state
            .pending_runtime
            .admission
            .webrtc_admissions
            .get(&peer.grant_id)
        {
            Some(WebrtcTerminalAdmission::Admitted {
                peer_generation, ..
            }) => *peer_generation,
            _ => panic!("WebRTC admission must be live"),
        };
        assert!(
            harness
                .state
                .pending_runtime
                .admission
                .connection_budgets
                .get(&peer_generation)
                .and_then(|budget| budget.usage(&label))
                .is_some(),
            "a bound terminal label must hold a budget slot before the remote close"
        );
        // The route generation the driver will name in its exit observation, read from the
        // live Core inventory for this exact route rather than assumed.
        let route_generation = harness
            .daemon
            .runtime_mut()
            .expect("runtime")
            .list_terminal_subscriptions(crate::host_executor::HOST_PREPARED_BYTE_CAPACITY)
            .wait(Duration::from_secs(5))
            .expect("inventory")
            .expect("inventory fits the test allowance")
            .records
            .iter()
            .find(|row| row.session_id.0 == session_id && row.subscription_id.0 == subscription_id)
            .expect("the bound route must be live before the remote close")
            .generation
            .0;
        peer.offer_runtime
            .block_on(channel.close())
            .expect("remote close");
        // Production owner path. The Hub driver observes the remote close and closes
        // the adapter; pinned Core retires the route on adapter pressure `Closed`;
        // the driver's RetireReservedSubscription releases the label and budget.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            while let Ok(message) = harness.try_receive_owner_message() {
                handle_control_message(
                    &mut harness.daemon,
                    &mut harness.state,
                    &harness.transport_handle,
                    harness.control_tx.clone(),
                    message,
                );
            }
            let runtime = harness.daemon.runtime_mut().expect("runtime");
            let core_present = runtime
                .list_terminal_subscriptions(crate::host_executor::HOST_PREPARED_BYTE_CAPACITY)
                .wait(Duration::from_secs(5))
                .expect("inventory")
                .expect("inventory fits the test allowance")
                .records
                .iter()
                .any(|row| {
                    row.session_id.0 == session_id && row.subscription_id.0 == subscription_id
                });
            let reservation = harness
                .state
                .pending_runtime
                .admission
                .reservations
                .lookup_label(
                    &label,
                    peer_generation,
                    crate::admission::reservations::now_seconds(),
                );
            if !core_present
                && reservation == crate::admission::reservations::ReservationLookup::Unknown
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for Core route retirement and reservation release: core_present={core_present} reservation={reservation:?}"
            );
            thread::sleep(Duration::from_millis(5));
        }
        // Drive the production dispatcher until its inventory wake removes the route.
        let reconcile_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while harness
            .state
            .pending_runtime
            .is_adapter_bound(session_id, subscription_id)
        {
            if let Ok(message) = harness.try_receive_owner_message() {
                handle_control_message(
                    &mut harness.daemon,
                    &mut harness.state,
                    &harness.transport_handle,
                    harness.control_tx.clone(),
                    message,
                );
            }
            assert!(
                std::time::Instant::now() < reconcile_deadline,
                "owner dispatcher must reconcile the retired adapter route"
            );
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            !harness
                .state
                .pending_runtime
                .is_adapter_bound(session_id, subscription_id),
            "owner-loop reconcile must remove the retired terminal adapter route"
        );
        assert!(
            harness
                .state
                .pending_runtime
                .admission
                .connection_budgets
                .get(&peer_generation)
                .and_then(|budget| budget.usage(&label))
                .is_none(),
            "the retired label must release its budget slot"
        );
        assert!(
            !peer_state.cleanup_sent.load(Ordering::Acquire),
            "remote channel close must not clean up the peer"
        );
        let sibling_response = harness.subscribe_entities(&mut peer, "live-host-sibling");
        assert_eq!(
            sibling_response.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed,
            "the host sibling must carry a request and response after remote close"
        );
        // The driver's exit report for this route is the positive proof that the Hub
        // observed the remote close on the production owner path.
        let observed = harness.wait_for_host_event(&mut peer, "remote close observation");
        assert_eq!(
            observed,
            botster_hub_client::DaemonEvent::RuntimeObservation {
                kind: format!(
                    "terminal_channel_closed:{subscription_id}:{route_generation}:remote_close"
                ),
            },
            "the owning peer must receive exactly the driver's remote-close observation"
        );
        peer.offer_runtime.block_on(async {
            assert!(
                matches!(
                    channel.close().await,
                    Err(webrtc::error::Error::ErrDataChannelClosed)
                ),
                "the dependency must report the removed channel"
            );
            channel.local_close().await.expect("idempotent local close");
        });
        assert!(!peer_state.cleanup_sent.load(Ordering::Acquire));
        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn removed_subscription_channel_keeps_host_sibling_live() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("remote-closed-subscription");
        let mut peer = harness.signal_peer("http://127.0.0.1:41919");
        let target = harness
            .subscribe_entities(&mut peer, "removed-close-target")
            .subscription_reservation
            .expect("target reservation");
        let label = target.label;
        harness.bind_reserved_on_peer(&mut peer, &label);
        harness.wait_until_reservation_bound(&peer.grant_id, &label);
        let peer_state = harness
            .daemon
            .local_webrtc()
            .peer_states
            .get(&peer.grant_id)
            .expect("live peer state")
            .clone();
        let channel = Arc::clone(
            &peer
                .offer_peer
                .as_ref()
                .expect("offer peer")
                .reserved_channels
                .get(&label)
                .expect("target channel")
                .channel,
        );
        peer.offer_runtime
            .block_on(channel.close())
            .expect("remote close");
        peer.offer_runtime.block_on(async {
            let receiver = &mut peer
                .offer_peer
                .as_mut()
                .expect("offer peer")
                .reserved_channels
                .get_mut(&label)
                .expect("target channel")
                .message_rx;
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut frames = 0;
                while receiver.recv().await.is_some() {
                    frames += 1;
                    assert!(frames <= 256, "bounded pending frames");
                }
            })
            .await
            .expect("local channel poll task must end");
            assert!(
                matches!(
                    channel.close().await,
                    Err(webrtc::error::Error::ErrDataChannelClosed)
                ),
                "the dependency must report the removed channel"
            );
            close_subscription_channel_or_fail_peer(channel.as_ref(), &peer_state).await;
        });
        assert!(
            !peer_state.cleanup_sent.load(Ordering::Acquire),
            "an absent channel must not clean up the peer"
        );
        let sibling_response = harness.subscribe_entities(&mut peer, "live-host-sibling");
        assert_eq!(
            sibling_response.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed,
            "the host sibling must carry a request and response after channel removal"
        );
        peer.offer_runtime.block_on(async {
            assert!(
                matches!(
                    channel.close().await,
                    Err(webrtc::error::Error::ErrDataChannelClosed)
                ),
                "the dependency must report the removed channel"
            );
            channel.local_close().await.expect("idempotent local close");
        });
        assert!(!peer_state.cleanup_sent.load(Ordering::Acquire));
        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn failed_channel_close_keeps_usage_until_peer_cleanup() {
        assert_failed_channel_close_keeps_usage(false);
    }

    #[test]
    fn timed_out_channel_close_keeps_usage_until_peer_cleanup() {
        assert_failed_channel_close_keeps_usage(true);
    }

    fn assert_failed_channel_close_keeps_usage(hang: bool) {
        use crate::admission::connection_budget::{ChannelClass, ConnectionBudget};
        let (tx, mut rx) = tokio_mpsc::channel(8);
        let peer_state = LocalWebrtcPeerState::new("grant".to_string(), tx);
        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("route".to_string(), ChannelClass::Terminal)
            .expect("reserve route");
        usage.store(4096, Ordering::Release);
        let (_adapter, handle) = peer_state
            .mux
            .create_adapter_with_aggregate(budget.aggregate());
        handle.close();
        let channel = FakeDataChannel::default();
        channel.outstanding_bytes.store(4096, Ordering::Release);
        channel.close_hangs.store(hang, Ordering::Release);
        channel.close_fails.store(!hang, Ordering::Release);
        let key = AesGcmKey::from_slice(&[17; 32]).expect("test key");
        let route = BoundSubscriptionRoute {
            peer_state: &peer_state,
            grant_id: "grant",
            label: "route",
            subscription_id: "sub",
            generation: 1,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(2),
                run_bound_subscription_channel_and_retire(
                    &channel,
                    &key,
                    route,
                    BoundSubscription::Terminal {
                        handle,
                        usage,
                        generation: 1,
                    },
                    Vec::new(),
                ),
            )
            .await
            .expect("bounded channel failure");
        });
        assert!(channel.close_started.load(Ordering::Acquire));
        assert!(!channel.closed.load(Ordering::Acquire));
        assert_eq!(
            budget.aggregate_buffered(),
            4096,
            "live channel bytes must remain counted"
        );
        assert!(
            matches!(rx.try_recv(), Ok(ControlMessage::LocalWebrtcPeerClosed { terminal_record, .. })
            if terminal_record.cause == crate::transport::webrtc::peer::LocalWebrtcTerminalCause::ChannelError)
        );
        assert!(
            rx.try_recv().is_err(),
            "failed close must not queue ordinary channel retirement"
        );
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
                    None,
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
            .try_send(
                DaemonEntityFrame::Snapshot {
                    subscription_id: target.subscription_id.clone(),
                    entity_type: "session".to_string(),
                    snapshot_seq: 1,
                    items: vec![serde_json::json!({"payload": "x".repeat(65_536)})],
                    resync_reason: None,
                }
                .into(),
            )
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
            match harness.try_receive_owner_message() {
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
            match harness.try_receive_owner_message() {
                Ok(message) => {
                    handle_control_message(
                        &mut harness.daemon,
                        &mut harness.state,
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
            match harness.try_receive_owner_message() {
                Ok(message @ ControlMessage::AuthorizeSubscriptionSend { .. }) => break message,
                Ok(other) => {
                    handle_control_message(
                        &mut harness.daemon,
                        &mut harness.state,
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
            match harness.try_receive_owner_message() {
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
        while let Ok(message) = harness.try_receive_owner_message() {
            handle_control_message(
                &mut harness.daemon,
                &mut harness.state,
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
            EXTRA_DATA_CHANNEL_LABEL,
            &extra,
        ));
        assert!(
            extra.closed.load(Ordering::Acquire),
            "production reject path must finish local_close"
        );
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
