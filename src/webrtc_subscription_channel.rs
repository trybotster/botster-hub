//! Per-subscription WebRTC channel host.
//!
//! This module owns terminal DataChannel framing and adapter flush. Reservation,
//! label matching, and bind-at-open arrive in the following behavior commit.

use std::collections::VecDeque;

use botster_core::{AesGcmKey, encrypt_aes_gcm};
use botster_hub_client::{DaemonLocalWebrtcDeliveryKind, LOCAL_WEBRTC_MAX_DELIVERY_BYTES};

use super::{
    LocalWebrtcDataChannel, LocalWebrtcError, LocalWebrtcFlowControl, LocalWebrtcPeerState,
    LocalWebrtcResult, LocalWebrtcSendFailure, PendingLocalWebrtcRequest,
    frame_encrypted_daemon_delivery, random_token, send_response_frames,
};

pub(super) fn framed_daemon_terminal_frame(
    key: &AesGcmKey,
    bytes: &[u8],
) -> LocalWebrtcResult<Vec<String>> {
    let envelope = encrypt_aes_gcm(key, bytes, 1)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let encrypted = serde_json::to_string(&envelope)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon terminal frame exceeded {LOCAL_WEBRTC_MAX_DELIVERY_BYTES} byte limit"
        )));
    }
    let message_id = random_token("terminal")?;
    frame_encrypted_daemon_delivery(
        DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame,
        &message_id,
        &encrypted,
    )
}

pub(super) async fn flush_webrtc_adapter_frames<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
) -> Result<(), LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    for (_session_id, _subscription_id, handle, bytes) in peer_state.mux.snapshot_writes() {
        if handle.is_closed() {
            continue;
        }
        peer_state.begin_operation("terminal_delivery");
        let frames = match framed_daemon_terminal_frame(stream_key, &bytes) {
            Ok(frames) => frames,
            Err(_) => {
                handle.close();
                continue;
            }
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
        if handle.is_closed() {
            continue;
        }
        let _ = handle.complete_active();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::framed_daemon_terminal_frame;
    use crate::webrtc_terminal_adapter::WebRtcConnectionMux;
    use botster_core::AesGcmKey;
    use botster_core::contract::terminal_adapter::TerminalAdapter;
    use botster_hub_client::{DaemonLocalWebrtcDeliveryChunk, DaemonLocalWebrtcDeliveryKind};

    #[test]
    fn terminal_frame_chunks_are_one_delivery_and_complete_once() {
        let key = AesGcmKey::from_slice(&[21; 32]).unwrap();
        let payload = vec![0x61; 20_000];
        let frames = framed_daemon_terminal_frame(&key, &payload).expect("frame terminal bytes");
        assert!(frames.len() > 1);
        let mut message_id = None;
        for (index, serialized) in frames.iter().enumerate() {
            let chunk: DaemonLocalWebrtcDeliveryChunk =
                serde_json::from_str(serialized).expect("parse terminal chunk");
            assert_eq!(
                chunk.delivery_kind,
                DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame
            );
            assert_eq!(chunk.chunk_index, index as u32);
            if let Some(message_id) = &message_id {
                assert_eq!(&chunk.message_id, message_id);
            } else {
                message_id = Some(chunk.message_id.clone());
            }
        }
        let second = framed_daemon_terminal_frame(&key, &payload).expect("second delivery");
        let first_id = serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(&frames[0])
            .unwrap()
            .message_id;
        let second_id = serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(&second[0])
            .unwrap()
            .message_id;
        assert_ne!(first_id, second_id);
        let mux = WebRtcConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter();
        let frame = botster_terminal_protocol::TerminalFrame::from_bytes(
            br#"{"type":"terminal_output","marker":"once"}"#,
        )
        .expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert!(handle.complete_active().is_some());
        assert!(handle.complete_active().is_none());
    }
}
