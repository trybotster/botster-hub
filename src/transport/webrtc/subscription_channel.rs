use botster_hub_client::{DaemonEntityFrame, DaemonRequest, DaemonResponse};

use crate::daemon_transport::response_records_attach_ownership;
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
    use crate::daemon_transport::{ControlMessage, ControlSender};
    use crate::daemon_transport::{DaemonControlState, EntityFrameSender, handle_control_message};
    use crate::subscription::attach_routes::negotiated_unix_capability_set;
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
