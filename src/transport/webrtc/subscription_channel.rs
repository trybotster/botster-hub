use botster_core::AesGcmKey;
use botster_hub_client::{
    DaemonCompatibility, DaemonDiagnostic, DaemonEntityFrame, DaemonHello, DaemonHelloAck,
    DaemonRequest, DaemonResponse, PROTOCOL,
};
use botster_terminal_protocol::{
    TerminalCompatibility, ensure_compatible as ensure_terminal_compatible,
};
use tokio::sync::oneshot;

use crate::daemon::control::message::{BindReservedError, ControlMessage, ReservationInspectReply};
use crate::transport::webrtc::adapter::WebRtcTerminalAdapterHandle;
use crate::transport::webrtc::control_channel::{
    DataChannelPlaintext, LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH, LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW,
    decrypt_data_channel_plaintext,
};
use crate::transport::webrtc::delivery::{framed_daemon_hello_ack, framed_daemon_terminal_frame};
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
    let close =
        tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, data_channel.local_close()).await;
    observe_rejected_data_channel_for_test(claimed, &close, label);
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
        .send(ControlMessage::InspectTerminalReservation {
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
    match inspect {
        ReservationInspectReply::Unknown | ReservationInspectReply::Bound => {
            reject_extra_data_channel(grant_id, false, label, data_channel).await;
            return;
        }
        ReservationInspectReply::Expired { .. } => {
            reject_extra_data_channel(grant_id, false, label, data_channel).await;
            return;
        }
        ReservationInspectReply::Live { .. } => {}
    }
    if admit_subscription_hello(data_channel, stream_key)
        .await
        .is_err()
    {
        reject_extra_data_channel(grant_id, false, label, data_channel).await;
        return;
    }
    let (bind_tx, bind_rx) = oneshot::channel();
    if peer_state
        .runtime_tx
        .send(ControlMessage::BindReservedTerminal {
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
        Ok(Ok(handle)) => {
            run_bound_subscription_channel(data_channel, stream_key, handle).await;
        }
        Ok(Err(BindReservedError::Expired)) | Ok(Err(_)) | Err(_) => {
            reject_extra_data_channel(grant_id, false, label, data_channel).await;
        }
    }
}

async fn admit_subscription_hello<C>(data_channel: &C, stream_key: &AesGcmKey) -> Result<(), ()>
where
    C: LocalWebrtcDataChannel + ?Sized,
{
    loop {
        match data_channel.local_poll().await {
            Some(webrtc::data_channel::DataChannelEvent::OnMessage(message)) => {
                match decrypt_data_channel_plaintext(stream_key, message.data.as_ref()) {
                    Some(DataChannelPlaintext::Hello(hello)) => {
                        return acknowledge_subscription_hello(data_channel, stream_key, &hello)
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
) -> Result<(), ()>
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
    for frame in frames {
        data_channel.local_send_text(&frame).await.map_err(|_| ())?;
    }
    Ok(())
}

async fn run_bound_subscription_channel<C>(
    data_channel: &C,
    stream_key: &AesGcmKey,
    handle: WebRtcTerminalAdapterHandle,
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
        handle.close();
        let _ = data_channel.local_close().await;
        return;
    }
    loop {
        if let Err(()) = flush_subscription_adapter_frames(data_channel, stream_key, &handle).await
        {
            handle.close();
            let _ = data_channel.local_close().await;
            return;
        }
        tokio::select! {
            _ = handle.wait_for_write() => {}
            inbound = data_channel.local_poll() => {
                match inbound {
                    Some(webrtc::data_channel::DataChannelEvent::OnMessage(message)) => {
                        let Ok(envelope) = serde_json::from_str::<botster_core::AesGcmEnvelope>(
                            std::str::from_utf8(message.data.as_ref()).unwrap_or(""),
                        ) else {
                            handle.close();
                            let _ = data_channel.local_close().await;
                            return;
                        };
                        let Ok(bytes) = botster_core::decrypt_aes_gcm(stream_key, &envelope) else {
                            handle.close();
                            let _ = data_channel.local_close().await;
                            return;
                        };
                        if handle.push_ingress(bytes).is_err() {
                            handle.close();
                            let _ = data_channel.local_close().await;
                            return;
                        }
                    }
                    Some(event @ (webrtc::data_channel::DataChannelEvent::OnBufferedAmountHigh
                    | webrtc::data_channel::DataChannelEvent::OnBufferedAmountLow)) => {
                        apply_subscription_pressure_event(&handle, &event);
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
    for frame in frames {
        data_channel.local_send_text(&frame).await.map_err(|_| ())?;
    }
    if !handle.is_closed() {
        let _ = handle.complete_active();
    }
    Ok(())
}

pub(crate) fn entity_frame_subscription_id(frame: &DaemonEntityFrame) -> &str {
    match frame {
        DaemonEntityFrame::Snapshot {
            subscription_id, ..
        }
        | DaemonEntityFrame::Upsert {
            subscription_id, ..
        }
        | DaemonEntityFrame::Patch {
            subscription_id, ..
        }
        | DaemonEntityFrame::Remove {
            subscription_id, ..
        }
        | DaemonEntityFrame::Error {
            subscription_id, ..
        } => subscription_id,
    }
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
}
