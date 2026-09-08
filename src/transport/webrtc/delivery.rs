//! Local WebRTC delivery framing.
//!
//! Control deliveries are JSON text chunks of one encrypted [`ServerFrame`].
//! Terminal deliveries are binary chunks: a fixed
//! [`LocalWebrtcTerminalChunkHeader`] followed by one AES-GCM sealed slice of
//! the shared `TerminalBody`. Hub seals slices of the shared bytes directly;
//! it never re-serializes or base64-encodes terminal payloads.
use std::fmt;

use botster_core::{AesGcmKey, encrypt_aes_gcm, open_aes_gcm, seal_aes_gcm};
use botster_hub_client::{
    DaemonDiagnostic, DaemonLocalWebrtcDeliveryChunk, DaemonLocalWebrtcDeliveryKind,
    DaemonResponse, LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION, LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
    LOCAL_WEBRTC_MAX_FRAME_BYTES, LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES,
    LOCAL_WEBRTC_TERMINAL_CHUNK_MAX_PLAINTEXT_BYTES, LocalWebrtcTerminalChunkHeader, ServerFrame,
};
use botster_terminal_protocol::{MAX_TERMINAL_INPUT_FRAME_BYTES, RoutedTerminalFrame};

use crate::transport::webrtc::control_channel::response_with_diagnostic;
use crate::transport::webrtc::peer::LocalWebrtcTerminalCause;
use crate::transport::webrtc::signaling::random_token;
use crate::transport::webrtc::{LocalWebrtcError, LocalWebrtcResult};

// The current Rust WebRTC peer's message receive path is bounded at 16 KiB;
// 12 KiB leaves transport and framing headroom for every first-party peer.
pub(crate) const LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES: usize =
    LOCAL_WEBRTC_TERMINAL_CHUNK_MAX_PLAINTEXT_BYTES;

/// Reassembles one binary terminal input message from ordered sealed chunks.
///
/// Each chunk is opened on arrival; the plaintext slices are concatenated in
/// `chunk_index` order. Every chunk must repeat the bound generation. A
/// violation returns `Err(())` and the caller closes that channel only.
#[derive(Debug)]
pub(crate) struct InboundTerminalChunkAssembly {
    generation: u64,
    message_id: Option<u64>,
    last_completed_message_id: Option<u64>,
    chunk_count: u32,
    total_bytes: usize,
    next_chunk_index: u32,
    plaintext: Vec<u8>,
}

impl InboundTerminalChunkAssembly {
    pub(crate) fn new(generation: u64) -> Self {
        Self {
            generation,
            message_id: None,
            last_completed_message_id: None,
            chunk_count: 0,
            total_bytes: 0,
            next_chunk_index: 0,
            plaintext: Vec::new(),
        }
    }

    /// Consume one binary DataChannel message. Returns the complete plaintext
    /// input frame when the last chunk arrives.
    pub(crate) fn push(&mut self, key: &AesGcmKey, message: &[u8]) -> Result<Option<Vec<u8>>, ()> {
        let (header, sealed) = LocalWebrtcTerminalChunkHeader::decode(message).ok_or(())?;
        if header.generation != self.generation
            || header.total_bytes as usize > MAX_TERMINAL_INPUT_FRAME_BYTES
            || header.total_bytes == 0
            || self
                .last_completed_message_id
                .is_some_and(|last| header.message_id <= last)
        {
            return Err(());
        }
        match self.message_id {
            None if header.chunk_index == 0 => {
                self.message_id = Some(header.message_id);
                self.chunk_count = header.chunk_count;
                self.total_bytes = header.total_bytes as usize;
                self.plaintext = Vec::with_capacity(self.total_bytes);
            }
            Some(message_id)
                if message_id == header.message_id
                    && self.chunk_count == header.chunk_count
                    && self.total_bytes == header.total_bytes as usize => {}
            _ => return Err(()),
        }
        if header.chunk_index != self.next_chunk_index {
            return Err(());
        }
        let slice = open_aes_gcm(key, sealed).map_err(|_| ())?;
        if slice.is_empty()
            || slice.len() > LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES
            || self.plaintext.len() + slice.len() > self.total_bytes
        {
            return Err(());
        }
        self.plaintext.extend_from_slice(&slice);
        self.next_chunk_index += 1;
        if self.next_chunk_index == self.chunk_count {
            if self.plaintext.len() != self.total_bytes {
                return Err(());
            }
            self.last_completed_message_id = self.message_id.take();
            self.chunk_count = 0;
            self.total_bytes = 0;
            self.next_chunk_index = 0;
            return Ok(Some(std::mem::take(&mut self.plaintext)));
        }
        let remaining_chunks = (self.chunk_count - self.next_chunk_index) as usize;
        if self.plaintext.len() >= self.total_bytes
            || self.total_bytes - self.plaintext.len() < remaining_chunks
        {
            return Err(());
        }
        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn is_partial(&self) -> bool {
        self.message_id.is_some()
    }
}

/// Seal one routed terminal frame into ordered binary chunks.
///
/// `message_id` is the per-channel outbound counter. Each chunk carries the
/// fixed header, then `seal_aes_gcm` output for one slice of the shared body.
pub(crate) fn sealed_terminal_chunks(
    key: &AesGcmKey,
    frame: &RoutedTerminalFrame,
    message_id: u64,
) -> LocalWebrtcResult<Vec<Vec<u8>>> {
    let body = frame.frame.as_bytes();
    if body.is_empty() {
        return Err(LocalWebrtcError::Webrtc(
            "terminal body must not be empty".to_string(),
        ));
    }
    let total_bytes = u32::try_from(body.len())
        .map_err(|_| LocalWebrtcError::Webrtc("terminal body length overflow".to_string()))?;
    let chunk_count = body.len().div_ceil(LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES);
    let chunk_count = u32::try_from(chunk_count)
        .map_err(|_| LocalWebrtcError::Webrtc("terminal chunk count overflow".to_string()))?;
    let mut chunks = Vec::with_capacity(chunk_count as usize);
    for (chunk_index, slice) in body.chunks(LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES).enumerate() {
        let header = LocalWebrtcTerminalChunkHeader {
            message_id,
            chunk_index: u32::try_from(chunk_index).map_err(|_| {
                LocalWebrtcError::Webrtc("terminal chunk index overflow".to_string())
            })?,
            chunk_count,
            total_bytes,
            generation: frame.generation,
            stream_epoch: frame.stream_epoch,
        };
        let mut message = Vec::with_capacity(
            LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES
                + slice.len()
                + botster_core::AES_GCM_SEALED_OVERHEAD_BYTES,
        );
        message.extend_from_slice(&header.encode());
        seal_aes_gcm(key, slice, &mut message)
            .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
        if message.len() >= LOCAL_WEBRTC_MAX_FRAME_BYTES {
            return Err(LocalWebrtcError::Webrtc(format!(
                "sealed terminal chunk was {} bytes",
                message.len()
            )));
        }
        chunks.push(message);
    }
    Ok(chunks)
}

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

/// Encrypt one server frame into the JSON envelope text used by control deliveries.
pub(crate) fn encrypt_server_frame(
    key: &AesGcmKey,
    frame: &ServerFrame,
) -> LocalWebrtcResult<String> {
    let plaintext =
        serde_json::to_vec(frame).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    encrypt_encoded_server_frame(key, &plaintext)
}

fn encrypt_encoded_server_frame(key: &AesGcmKey, plaintext: &[u8]) -> LocalWebrtcResult<String> {
    let envelope = encrypt_aes_gcm(key, plaintext, 1)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    serde_json::to_string(&envelope).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))
}

/// Frame one correlated response. An oversized response is replaced by a
/// correlated operator error before any rejected payload is framed.
pub(crate) fn framed_daemon_response(
    key: &AesGcmKey,
    request_id: &str,
    response: &DaemonResponse,
) -> LocalWebrtcResult<Vec<String>> {
    let frame = ServerFrame::Response {
        request_id: request_id.to_string(),
        response: response.clone(),
    };
    let encrypted = encrypt_server_frame(key, &frame)?;
    frame_correlated_response(key, request_id, encrypted)
}

pub(crate) fn framed_encoded_entity(
    key: &AesGcmKey,
    encoded_frame: &[u8],
) -> LocalWebrtcResult<Vec<String>> {
    let encrypted = encrypt_encoded_server_frame(key, encoded_frame)?;
    if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon entity frame exceeded {LOCAL_WEBRTC_MAX_DELIVERY_BYTES} byte limit"
        )));
    }
    let message_id = random_token("entity")?;
    frame_encrypted_daemon_delivery(&message_id, &encrypted)
}

pub(crate) fn framed_encoded_daemon_response(
    key: &AesGcmKey,
    request_id: &str,
    encoded_frame: &[u8],
) -> LocalWebrtcResult<Vec<String>> {
    let encrypted = encrypt_encoded_server_frame(key, encoded_frame)?;
    frame_correlated_response(key, request_id, encrypted)
}

fn frame_correlated_response(
    key: &AesGcmKey,
    request_id: &str,
    encrypted: String,
) -> LocalWebrtcResult<Vec<String>> {
    let encrypted = if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        encrypt_server_frame(
            key,
            &ServerFrame::Response {
                request_id: request_id.to_string(),
                response: response_with_diagnostic(DaemonDiagnostic::action_failure(
                    "local_webrtc_data_channel",
                    format!(
                        "encrypted daemon response exceeded {} byte limit",
                        LOCAL_WEBRTC_MAX_DELIVERY_BYTES
                    ),
                )),
            },
        )?
    } else {
        encrypted
    };
    let message_id = random_token("response")?;
    frame_encrypted_daemon_delivery(&message_id, &encrypted)
}

/// Frame any other server frame: hello ack, event, entity frame, or close.
pub(crate) fn framed_server_frame(
    key: &AesGcmKey,
    frame: &ServerFrame,
) -> LocalWebrtcResult<Vec<String>> {
    let encrypted = encrypt_server_frame(key, frame)?;
    if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon server frame exceeded {LOCAL_WEBRTC_MAX_DELIVERY_BYTES} byte limit"
        )));
    }
    let prefix = match frame {
        ServerFrame::HelloAck { .. } => "hello",
        ServerFrame::Response { .. } => "response",
        ServerFrame::Event { .. } => "event",
        ServerFrame::Entity { .. } => "entity",
        ServerFrame::Close { .. } => "close",
    };
    let message_id = random_token(prefix)?;
    frame_encrypted_daemon_delivery(&message_id, &encrypted)
}

pub(crate) fn frame_encrypted_daemon_delivery(
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
            delivery_kind: DaemonLocalWebrtcDeliveryKind::ServerFrame,
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
            delivery_kind: DaemonLocalWebrtcDeliveryKind::ServerFrame,
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
mod tests {
    use super::*;
    use botster_core::{AesGcmEnvelope, decrypt_aes_gcm};
    use botster_hub_client::{DaemonRequest, DaemonResponseKind};
    use botster_terminal_protocol::{RouteId, encode_output};
    use serde_json::Value;

    fn routed(route: &str, generation: u64, stream_epoch: u32, body: &[u8]) -> RoutedTerminalFrame {
        RoutedTerminalFrame::new(
            RouteId::new(route).expect("route"),
            generation,
            stream_epoch,
            encode_output(body).expect("output frame"),
        )
    }

    #[test]
    fn encoded_response_encrypts_the_supplied_json_without_serializing_again() {
        let key = AesGcmKey::from_slice(&[31; 32]).unwrap();
        let encoded =
            br#"{ "frame": "response", "request_id": "42", "response": { "kind": "status" } }"#;
        let frames =
            framed_encoded_daemon_response(&key, "42", encoded).expect("frame encoded response");
        assert_eq!(frames.len(), 1);
        let chunk: DaemonLocalWebrtcDeliveryChunk = serde_json::from_str(&frames[0]).unwrap();
        let envelope: AesGcmEnvelope = serde_json::from_str(&chunk.payload).unwrap();
        let plaintext = decrypt_aes_gcm(&key, &envelope).unwrap();
        assert_eq!(plaintext, encoded);
    }

    #[test]
    fn delivery_fixture_publishes_the_enforced_terminal_chunk_plaintext_limit() {
        let fixture =
            botster_hub_test_support::local_webrtc_delivery_chunk_conformance_fixture_json();

        assert_eq!(
            fixture.pointer("/terminal_chunk/maximum_plaintext_bytes"),
            Some(&serde_json::json!(LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES))
        );
    }

    #[test]
    fn response_frames_use_one_bounded_protocol_for_small_and_large_envelopes() {
        let small = frame_encrypted_daemon_delivery("response-small", "encrypted").unwrap();
        assert_eq!(small.len(), 1);
        let small: DaemonLocalWebrtcDeliveryChunk = serde_json::from_str(&small[0]).unwrap();
        assert_eq!(small.version, LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION);
        assert_eq!(
            small.delivery_kind,
            DaemonLocalWebrtcDeliveryKind::ServerFrame
        );
        assert_eq!(small.message_id, "response-small");
        assert_eq!(small.chunk_index, 0);
        assert_eq!(small.chunk_count, 1);
        assert_eq!(small.total_bytes, 9);
        assert_eq!(small.payload, "encrypted");

        let encrypted = "a".repeat(256 * 1024 + 1);
        let frames = frame_encrypted_daemon_delivery("response-large", &encrypted).unwrap();
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
        let error = frame_encrypted_daemon_delivery("response-over-budget", &encrypted)
            .expect_err("over-budget response must fail before framing");
        assert!(error.to_string().contains("exceeded 16777216 byte limit"));
    }

    #[test]
    fn sealed_terminal_chunks_carry_the_routing_header_and_reassemble() {
        let key = AesGcmKey::from_slice(&[7; 32]).unwrap();
        let frame = routed("sub", 4, 2, &vec![0x61; 30_000]);
        let chunks = sealed_terminal_chunks(&key, &frame, 9).expect("seal");
        assert_eq!(chunks.len(), 3);
        for (index, chunk) in chunks.iter().enumerate() {
            assert!(chunk.len() < LOCAL_WEBRTC_MAX_FRAME_BYTES);
            let (header, _) = LocalWebrtcTerminalChunkHeader::decode(chunk).expect("header");
            assert_eq!(header.message_id, 9);
            assert_eq!(header.chunk_index, index as u32);
            assert_eq!(header.chunk_count, 3);
            assert_eq!(header.total_bytes, frame.frame.len() as u32);
            assert_eq!(header.generation, 4);
            assert_eq!(header.stream_epoch, 2);
        }
        let mut assembly = InboundTerminalChunkAssembly::new(4);
        let mut complete = None;
        for chunk in &chunks {
            complete = assembly.push(&key, chunk).expect("ordered chunk");
        }
        assert_eq!(complete.as_deref(), Some(frame.frame.as_bytes()));
    }

    #[test]
    fn terminal_input_assembly_reassembles_one_large_opaque_message() {
        let key = AesGcmKey::from_slice(&[21; 32]).unwrap();
        let frame = routed("sub", 1, 0, &vec![0x62; 20_000]);
        let chunks = sealed_terminal_chunks(&key, &frame, 1).expect("seal");
        assert!(chunks.len() > 1);
        let mut assembly = InboundTerminalChunkAssembly::new(1);
        let mut complete = None;
        for chunk in &chunks {
            complete = assembly.push(&key, chunk).expect("ordered bounded chunk");
        }
        assert_eq!(complete.as_deref(), Some(frame.frame.as_bytes()));
        assert!(!assembly.is_partial());
        assert_eq!(
            assembly.push(&key, &chunks[0]),
            Err(()),
            "a replayed message id is rejected"
        );
    }

    #[test]
    fn terminal_input_assembly_fails_closed_on_generation_mismatch_and_disorder() {
        let key = AesGcmKey::from_slice(&[13; 32]).unwrap();
        let frame = routed(
            "sub",
            3,
            0,
            &vec![0x63; LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES + 1],
        );
        let chunks = sealed_terminal_chunks(&key, &frame, 5).expect("seal");
        assert_eq!(chunks.len(), 2);

        let mut wrong_generation = InboundTerminalChunkAssembly::new(2);
        assert_eq!(wrong_generation.push(&key, &chunks[0]), Err(()));

        let mut disorder = InboundTerminalChunkAssembly::new(3);
        assert_eq!(disorder.push(&key, &chunks[1]), Err(()));

        let mut duplicate = InboundTerminalChunkAssembly::new(3);
        assert_eq!(duplicate.push(&key, &chunks[0]), Ok(None));
        assert!(duplicate.is_partial());
        assert_eq!(duplicate.push(&key, &chunks[0]), Err(()));

        let other_key = AesGcmKey::from_slice(&[14; 32]).unwrap();
        let mut wrong_key = InboundTerminalChunkAssembly::new(3);
        assert_eq!(wrong_key.push(&other_key, &chunks[0]), Err(()));
    }

    #[test]
    fn over_budget_response_is_replaced_before_any_rejected_payload_is_framed() {
        let key = AesGcmKey::from_slice(&[7; 32]).unwrap();
        let mut response = response_with_diagnostic(DaemonDiagnostic::connected("fixture"));
        response.plugin_tool_result = Value::String("x".repeat(LOCAL_WEBRTC_MAX_DELIVERY_BYTES));

        let frames = framed_daemon_response(&key, "42", &response).unwrap();
        assert_eq!(frames.len(), 1);
        let chunk: DaemonLocalWebrtcDeliveryChunk = serde_json::from_str(&frames[0]).unwrap();
        assert!(!chunk.payload.contains(&"x".repeat(1024)));
        let envelope: AesGcmEnvelope = serde_json::from_str(&chunk.payload).unwrap();
        let plaintext = decrypt_aes_gcm(&key, &envelope).unwrap();
        let replacement: ServerFrame = serde_json::from_slice(&plaintext).unwrap();
        let ServerFrame::Response {
            request_id,
            response: replacement,
        } = replacement
        else {
            panic!("expected a correlated response");
        };
        assert_eq!(request_id, "42");
        assert_eq!(replacement.kind, DaemonResponseKind::OperatorError);
        assert!(
            replacement.diagnostics[0]
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("exceeded 16777216 byte limit")
        );
        let _ = DaemonRequest::Status;
    }
}
