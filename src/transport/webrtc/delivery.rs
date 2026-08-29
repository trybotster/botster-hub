use std::fmt;

use botster_core::{AesGcmKey, encrypt_aes_gcm};
use botster_hub_client::{
    DaemonDiagnostic, DaemonEntityFrame, DaemonEvent, DaemonHelloAck,
    DaemonLocalWebrtcDeliveryChunk, DaemonLocalWebrtcDeliveryKind, DaemonResponse,
    LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION, LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
    LOCAL_WEBRTC_MAX_FRAME_BYTES,
};

use crate::transport::webrtc::control_channel::response_with_diagnostic;
use crate::transport::webrtc::peer::LocalWebrtcTerminalCause;
use crate::transport::webrtc::signaling::random_token;
use crate::transport::webrtc::{LocalWebrtcError, LocalWebrtcResult};
// The current Rust WebRTC peer's message receive path is bounded at 16 KiB;
// 12 KiB leaves transport and JSON framing headroom for every first-party peer.
pub(crate) const LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES: usize = 12 * 1024;
#[derive(Debug)]
pub(crate) struct LocalWebrtcSendFailure {
    pub(crate) message_id: String,
    pub(crate) next_chunk_index: usize,
    pub(crate) last_sent_chunk_index: Option<usize>,
    pub(crate) total_chunks: usize,
    pub(crate) pressured: bool,
    pub(crate) cause: LocalWebrtcTerminalCause,
}

impl fmt::Display for LocalWebrtcSendFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "local WebRTC response delivery failed: message_id={} next_chunk={} last_sent_chunk={} total_chunks={} pressured={} cause={}",
            self.message_id,
            self.next_chunk_index,
            self.last_sent_chunk_index
                .map_or_else(|| "none".to_string(), |index| index.to_string()),
            self.total_chunks,
            self.pressured,
            self.cause,
        )
    }
}
pub(crate) fn encrypt_daemon_response(
    key: &AesGcmKey,
    response: &DaemonResponse,
) -> LocalWebrtcResult<String> {
    let plaintext = serde_json::to_vec(response)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let envelope = encrypt_aes_gcm(key, &plaintext, 1)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    serde_json::to_string(&envelope).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))
}

pub(crate) fn encrypt_daemon_entity_frame(
    key: &AesGcmKey,
    frame: &DaemonEntityFrame,
) -> LocalWebrtcResult<String> {
    let plaintext =
        serde_json::to_vec(frame).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let envelope = encrypt_aes_gcm(key, &plaintext, 1)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    serde_json::to_string(&envelope).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))
}

pub(crate) fn framed_daemon_response(
    key: &AesGcmKey,
    response: &DaemonResponse,
) -> LocalWebrtcResult<Vec<String>> {
    let encrypted = encrypt_daemon_response(key, response)?;
    let encrypted = if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        encrypt_daemon_response(
            key,
            &response_with_diagnostic(DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                format!(
                    "encrypted daemon response exceeded {} byte limit",
                    LOCAL_WEBRTC_MAX_DELIVERY_BYTES
                ),
            )),
        )?
    } else {
        encrypted
    };
    let message_id = random_token("response")?;
    frame_encrypted_daemon_delivery(
        DaemonLocalWebrtcDeliveryKind::DaemonResponse,
        &message_id,
        &encrypted,
    )
}

pub(crate) fn framed_daemon_hello_ack(
    key: &AesGcmKey,
    ack: &DaemonHelloAck,
) -> LocalWebrtcResult<Vec<String>> {
    let plaintext =
        serde_json::to_vec(ack).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let envelope = encrypt_aes_gcm(key, &plaintext, 1)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let encrypted = serde_json::to_string(&envelope)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon hello ack exceeded {LOCAL_WEBRTC_MAX_DELIVERY_BYTES} byte limit"
        )));
    }
    let message_id = random_token("hello")?;
    frame_encrypted_daemon_delivery(
        DaemonLocalWebrtcDeliveryKind::DaemonResponse,
        &message_id,
        &encrypted,
    )
}

pub(crate) fn framed_daemon_event(
    key: &AesGcmKey,
    event: &DaemonEvent,
) -> LocalWebrtcResult<Vec<String>> {
    let plaintext =
        serde_json::to_vec(event).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let envelope = encrypt_aes_gcm(key, &plaintext, 1)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let encrypted = serde_json::to_string(&envelope)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon event exceeded {LOCAL_WEBRTC_MAX_DELIVERY_BYTES} byte limit"
        )));
    }
    let message_id = random_token("event")?;
    frame_encrypted_daemon_delivery(
        DaemonLocalWebrtcDeliveryKind::DaemonEvent,
        &message_id,
        &encrypted,
    )
}
pub(crate) fn framed_daemon_terminal_frame(
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
pub(crate) fn framed_daemon_entity_frame(
    key: &AesGcmKey,
    frame: &DaemonEntityFrame,
) -> LocalWebrtcResult<Vec<String>> {
    let encrypted = encrypt_daemon_entity_frame(key, frame)?;
    if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon entity frame exceeded {LOCAL_WEBRTC_MAX_DELIVERY_BYTES} byte limit"
        )));
    }
    let message_id = random_token("entity")?;
    frame_encrypted_daemon_delivery(
        DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame,
        &message_id,
        &encrypted,
    )
}

pub(crate) fn frame_encrypted_daemon_delivery(
    delivery_kind: DaemonLocalWebrtcDeliveryKind,
    message_id: &str,
    encrypted: &str,
) -> LocalWebrtcResult<Vec<String>> {
    if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon delivery exceeded {LOCAL_WEBRTC_MAX_DELIVERY_BYTES} byte limit"
        )));
    }
    let total_bytes = u32::try_from(encrypted.len())
        .map_err(|_| LocalWebrtcError::Webrtc("response byte length overflow".to_string()))?;
    let chunk_count = encrypted
        .len()
        .max(1)
        .div_ceil(LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES);
    let chunk_count = u32::try_from(chunk_count)
        .map_err(|_| LocalWebrtcError::Webrtc("response chunk count overflow".to_string()))?;
    let mut frames = Vec::with_capacity(chunk_count as usize);

    for (chunk_index, payload) in encrypted
        .as_bytes()
        .chunks(LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES)
        .enumerate()
    {
        let payload = std::str::from_utf8(payload)
            .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
        let frame = DaemonLocalWebrtcDeliveryChunk {
            version: LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
            delivery_kind,
            message_id: message_id.to_string(),
            chunk_index: u32::try_from(chunk_index).map_err(|_| {
                LocalWebrtcError::Webrtc("response chunk index overflow".to_string())
            })?,
            chunk_count,
            total_bytes,
            payload: payload.to_string(),
        };
        let serialized = serde_json::to_string(&frame)
            .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
        if serialized.len() >= LOCAL_WEBRTC_MAX_FRAME_BYTES {
            return Err(LocalWebrtcError::Webrtc(format!(
                "serialized local WebRTC response frame was {} bytes",
                serialized.len()
            )));
        }
        frames.push(serialized);
    }
    if frames.is_empty() {
        let frame = DaemonLocalWebrtcDeliveryChunk {
            version: LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
            delivery_kind,
            message_id: message_id.to_string(),
            chunk_index: 0,
            chunk_count: 1,
            total_bytes: 0,
            payload: String::new(),
        };
        frames.push(
            serde_json::to_string(&frame)
                .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?,
        );
    }
    Ok(frames)
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
    use botster_core::{AesGcmEnvelope, decrypt_aes_gcm};
    use botster_core::{AesGcmKey, encrypt_aes_gcm};
    use botster_hub_client::{
        DaemonDiagnostic, DaemonEntityFrame, DaemonHello, DaemonRequest, DaemonResponse,
        LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
    };
    use serde_json::Value;
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
    fn response_frames_use_one_bounded_protocol_for_small_and_large_envelopes() {
        let small = frame_encrypted_daemon_delivery(
            DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            "response-small",
            "encrypted",
        )
        .unwrap();
        assert_eq!(small.len(), 1);
        let small: DaemonLocalWebrtcDeliveryChunk = serde_json::from_str(&small[0]).unwrap();
        assert_eq!(small.version, LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION);
        assert_eq!(
            small.delivery_kind,
            DaemonLocalWebrtcDeliveryKind::DaemonResponse
        );
        assert_eq!(small.message_id, "response-small");
        assert_eq!(small.chunk_index, 0);
        assert_eq!(small.chunk_count, 1);
        assert_eq!(small.total_bytes, 9);
        assert_eq!(small.payload, "encrypted");

        let encrypted = "a".repeat(256 * 1024 + 1);
        let frames = frame_encrypted_daemon_delivery(
            DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            "response-large",
            &encrypted,
        )
        .unwrap();
        assert!(frames.len() > 1);
        let chunks = frames
            .iter()
            .map(|frame| {
                assert!(frame.len() < LOCAL_WEBRTC_MAX_FRAME_BYTES);
                serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(frame).unwrap()
            })
            .collect::<Vec<_>>();
        assert!(chunks.iter().all(|chunk| {
            chunk.message_id == "response-large"
                && chunk.chunk_count == chunks.len() as u32
                && chunk.total_bytes == encrypted.len() as u32
        }));
        assert_eq!(
            chunks
                .iter()
                .flat_map(|chunk| chunk.payload.bytes())
                .collect::<Vec<_>>(),
            encrypted.as_bytes()
        );
    }

    #[test]
    fn response_frames_reject_encrypted_envelopes_over_the_assembly_budget() {
        let encrypted = "a".repeat(LOCAL_WEBRTC_MAX_DELIVERY_BYTES + 1);
        let error = frame_encrypted_daemon_delivery(
            DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            "response-over-budget",
            &encrypted,
        )
        .expect_err("over-budget response must fail before framing");
        assert!(error.to_string().contains("exceeded 16777216 byte limit"));
    }

    #[test]
    fn over_budget_response_is_replaced_before_any_rejected_payload_is_framed() {
        let key = AesGcmKey::from_slice(&[7; 32]).unwrap();
        let mut response = response_with_diagnostic(DaemonDiagnostic::connected("fixture"));
        response.plugin_tool_result = Value::String("x".repeat(LOCAL_WEBRTC_MAX_DELIVERY_BYTES));

        let frames = framed_daemon_response(&key, &response).unwrap();
        assert_eq!(frames.len(), 1);
        let chunk: DaemonLocalWebrtcDeliveryChunk = serde_json::from_str(&frames[0]).unwrap();
        assert!(!chunk.payload.contains(&"x".repeat(1024)));
        let envelope: AesGcmEnvelope = serde_json::from_str(&chunk.payload).unwrap();
        let plaintext = decrypt_aes_gcm(&key, &envelope).unwrap();
        let replacement: DaemonResponse = serde_json::from_slice(&plaintext).unwrap();
        assert_eq!(
            replacement.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert!(
            replacement.diagnostics[0]
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("exceeded 16777216 byte limit")
        );
    }
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
        use botster_core::contract::terminal_adapter::TerminalAdapter;
        let frame = botster_terminal_protocol::TerminalFrame::from_bytes(
            br#"{"type":"terminal_output","marker":"once"}"#,
        )
        .expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert!(handle.complete_active().is_some());
        assert!(handle.complete_active().is_none());
    }
}
