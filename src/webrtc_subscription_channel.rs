//! Per-subscription WebRTC channel host.
//!
//! Owns reservation, the section 8.3 label scheme, the single section 9 limit
//! table, open-event validation, and terminal DataChannel framing.

use std::collections::VecDeque;
use std::time::Duration;

use botster_core::{AesGcmKey, encrypt_aes_gcm};
use botster_hub_client::{DaemonLocalWebrtcDeliveryKind, LOCAL_WEBRTC_MAX_DELIVERY_BYTES};

#[allow(dead_code)]
pub(crate) const MAX_CONTROL_CHANNELS: usize = 1;
pub(crate) const MAX_SUBSCRIPTION_CHANNELS: usize = 32;
#[allow(dead_code)]
pub(crate) const MAX_TOTAL_CHANNELS: usize = MAX_CONTROL_CHANNELS + MAX_SUBSCRIPTION_CHANNELS;
pub(crate) const AGGREGATE_BUFFERED_HIGH: u32 = 2_097_152;
pub(crate) const AGGREGATE_BUFFERED_LOW: u32 = 1_048_576;

pub(crate) const LOCAL_WEBRTC_CHANNEL_OPEN_BOUND: Duration = Duration::from_secs(5);
/// Owner bind after a reserved channel opens. This is not the never-open sweep.
pub(crate) const LOCAL_WEBRTC_RESERVED_BIND_REPLY_BOUND: Duration = Duration::from_secs(30);

const LABEL_SCHEME: &str = "bs";
const LABEL_VERSION: &str = "1";
pub(crate) const CONTROL_CHANNEL_LABEL: &str = "botster-client";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriptionChannelKind {
    Term,
}

impl SubscriptionChannelKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Term => "term",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "term" => Some(Self::Term),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubscriptionChannelLabel {
    pub kind: SubscriptionChannelKind,
    pub session_id: String,
    pub subscription_id: String,
    pub generation: u64,
}

impl SubscriptionChannelLabel {
    pub(crate) fn terminal(session_id: String, subscription_id: String, generation: u64) -> Self {
        Self {
            kind: SubscriptionChannelKind::Term,
            session_id,
            subscription_id,
            generation,
        }
    }

    pub(crate) fn format(&self) -> String {
        format!(
            "{LABEL_SCHEME}/{LABEL_VERSION}/{}/{}/{}/{}",
            self.kind.as_str(),
            percent_encode(&self.session_id),
            percent_encode(&self.subscription_id),
            self.generation
        )
    }

    pub(crate) fn parse(label: &str) -> Option<Self> {
        let mut parts = label.split('/');
        let scheme = parts.next()?;
        let version = parts.next()?;
        let kind = SubscriptionChannelKind::parse(parts.next()?)?;
        let session_id = percent_decode(parts.next()?)?;
        let subscription_id = percent_decode(parts.next()?)?;
        let generation = parts.next()?.parse().ok()?;
        if parts.next().is_some() || scheme != LABEL_SCHEME || version != LABEL_VERSION {
            return None;
        }
        Some(Self {
            kind,
            session_id,
            subscription_id,
            generation,
        })
    }
}

fn percent_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn percent_decode(value: &str) -> Option<String> {
    let mut bytes = Vec::new();
    let raw = value.as_bytes();
    let mut index = 0;
    while index < raw.len() {
        if raw[index] == b'%' {
            if index + 2 >= raw.len() {
                return None;
            }
            let hex = std::str::from_utf8(&raw[index + 1..index + 3]).ok()?;
            bytes.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            bytes.push(raw[index]);
            index += 1;
        }
    }
    String::from_utf8(bytes).ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriptionRouteState {
    Reserved,
    Bound,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenRejectReason {
    Unreserved,
    Limit,
    Duplicate,
    Retired,
    Suppressed,
    PeerDying,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenEventDecision {
    Bind,
    Reject(OpenRejectReason),
}

#[derive(Debug, Clone)]
pub(crate) struct OpenEventView {
    #[allow(dead_code)]
    pub label: SubscriptionChannelLabel,
    pub matching_state: Option<SubscriptionRouteState>,
    pub charged_subscription_count: usize,
    pub generation_suppressed: bool,
    pub peer_dying: bool,
}

/// Production open-event decision. A8h and A8i drive this function.
///
/// The subscription slot is charged at Reserved. 32 charged routes are
/// permitted. The limit predicate is `> 32`, never `>= 32`.
pub(crate) fn decide_open_event(view: &OpenEventView) -> OpenEventDecision {
    if view.peer_dying {
        return OpenEventDecision::Reject(OpenRejectReason::PeerDying);
    }
    if view.generation_suppressed {
        return OpenEventDecision::Reject(OpenRejectReason::Suppressed);
    }
    match view.matching_state {
        None => OpenEventDecision::Reject(OpenRejectReason::Unreserved),
        Some(SubscriptionRouteState::Retired) => {
            OpenEventDecision::Reject(OpenRejectReason::Retired)
        }
        Some(SubscriptionRouteState::Bound) => {
            OpenEventDecision::Reject(OpenRejectReason::Duplicate)
        }
        Some(SubscriptionRouteState::Reserved) => {
            if view.charged_subscription_count > MAX_SUBSCRIPTION_CHANNELS {
                OpenEventDecision::Reject(OpenRejectReason::Limit)
            } else {
                OpenEventDecision::Bind
            }
        }
    }
}

pub(crate) fn reject_admission_on_count(charged_subscription_count: usize) -> bool {
    charged_subscription_count >= MAX_SUBSCRIPTION_CHANNELS
}

pub(crate) fn reject_admission_on_aggregate(aggregate_buffered: u32) -> bool {
    aggregate_buffered >= AGGREGATE_BUFFERED_HIGH
}

pub(crate) fn refuse_send_on_aggregate(aggregate_buffered: u32, frame_len: u32) -> bool {
    aggregate_buffered.saturating_add(frame_len) > AGGREGATE_BUFFERED_HIGH
}

/// Derive a per-subscription AES-GCM key from the grant secret and exact label.
///
/// The control channel keeps `secret_stream_key`. A frame captured on one
/// subscription channel must fail authentication on another.
pub(crate) fn subscription_channel_key(
    grant_secret: &str,
    label: &str,
) -> Result<AesGcmKey, String> {
    let encoded = grant_secret
        .strip_prefix("secret-")
        .ok_or_else(|| "invalid bootstrap secret".to_string())?;
    let mut bytes =
        decode_secret_hex(encoded).ok_or_else(|| "invalid bootstrap secret".to_string())?;
    if bytes.len() != 32 {
        return Err("invalid bootstrap secret".to_string());
    }
    for (index, byte) in label.as_bytes().iter().enumerate() {
        bytes[index % 32] ^= byte.wrapping_add((index % 251) as u8);
    }
    AesGcmKey::from_slice(&bytes).map_err(|error| error.to_string())
}

fn decode_secret_hex(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    let mut output = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        output.push((high << 4) | low);
    }
    Some(output)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

use super::{
    LocalWebrtcDataChannel, LocalWebrtcError, LocalWebrtcFlowControl, LocalWebrtcPeerState,
    LocalWebrtcResult, LocalWebrtcSendFailure, PendingLocalWebrtcRequest,
    frame_encrypted_daemon_delivery, random_token, send_subscription_frames,
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

pub(super) async fn flush_one_adapter_handle<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
    label: &SubscriptionChannelLabel,
    subscription_handle: &crate::webrtc_terminal_adapter::WebRtcTerminalAdapterHandle,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
) -> Result<bool, LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    let Some((handle, bytes)) = peer_state.mux.snapshot_write_for(
        &label.session_id,
        &label.subscription_id,
        label.generation,
    ) else {
        return Ok(false);
    };
    peer_state.begin_operation("terminal_delivery");
    let frames = match framed_daemon_terminal_frame(stream_key, &bytes) {
        Ok(frames) => frames,
        Err(_) => {
            handle.close();
            return Ok(false);
        }
    };
    send_subscription_frames(
        data_channel,
        stream_key,
        &frames,
        pending_requests,
        flow_control,
        peer_state,
        subscription_handle,
    )
    .await?;
    if handle.is_closed() {
        return Ok(false);
    }
    let emptied = handle.complete_active().is_some();
    if emptied && handle.ready_for_host_pump() {
        peer_state.mux.drain_empty_slot(&label.session_id);
    }
    Ok(emptied)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_SUBSCRIPTION_CHANNELS, OpenEventDecision, OpenEventView, OpenRejectReason,
        SubscriptionChannelLabel, SubscriptionRouteState, decide_open_event,
        framed_daemon_terminal_frame, subscription_channel_key,
    };
    use crate::webrtc_terminal_adapter::WebRtcConnectionMux;
    use botster_core::AesGcmKey;
    use botster_core::contract::terminal_adapter::TerminalAdapter;
    use botster_hub_client::{DaemonLocalWebrtcDeliveryChunk, DaemonLocalWebrtcDeliveryKind};

    #[test]
    fn slot_ready_does_not_block_the_datachannel_loop_on_control_send() {
        let source = include_str!("local_webrtc.rs");
        let func = source
            .split("async fn run_subscription_channel")
            .nth(1)
            .expect("run_subscription_channel");
        let func = func
            .split("async fn sleep_until_reserved_open_deadline")
            .next()
            .expect("function body");
        assert!(
            func.contains("note_slot_ready"),
            "flush must coalesce SlotReady on the mux before any control send"
        );
        assert!(
            func.contains("try_send"),
            "SlotReady control send must not park the DataChannel loop"
        );
        assert!(
            !func.contains(".send(ControlMessage::ReservedWebrtcSlotReady"),
            "awaiting SlotReady enqueue can stall a ready route behind generic control"
        );
        let flush = include_str!("webrtc_subscription_channel.rs")
            .split("async fn flush_one_adapter_handle")
            .nth(1)
            .expect("flush_one_adapter_handle");
        let flush = flush.split("mod tests").next().unwrap_or(flush);
        assert!(
            flush.contains("drain_empty_slot") && flush.contains("ready_for_host_pump"),
            "after a consumer flush, the mux pumps Core on the empty ready slot"
        );
    }

    #[test]
    fn reserved_open_stops_the_never_open_sweep_before_bind() {
        let source = include_str!("local_webrtc.rs");
        let func = source
            .split("async fn handle_subscription_open")
            .nth(1)
            .expect("handle_subscription_open");
        let func = func
            .split("async fn send_text_or_peer_terminal")
            .next()
            .expect("function body");
        assert!(
            func.contains("mark_reserved_open_in_flight"),
            "open must stop the never-open sweep before Bind is queued"
        );
        assert!(
            func.contains("LOCAL_WEBRTC_RESERVED_BIND_REPLY_BOUND"),
            "bind reply wait is not the never-open sweep"
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
        let frame = botster_terminal_protocol::TerminalFrame::from_bytes(
            br#"{"type":"terminal_output","marker":"once"}"#,
        )
        .expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert!(handle.complete_active().is_some());
        assert!(handle.complete_active().is_none());
    }

    fn matching_reserved_view(charged: usize) -> OpenEventView {
        OpenEventView {
            label: SubscriptionChannelLabel::terminal("s".into(), "sub".into(), 1),
            matching_state: Some(SubscriptionRouteState::Reserved),
            charged_subscription_count: charged,
            generation_suppressed: false,
            peer_dying: false,
        }
    }

    #[test]
    fn a8h_over_limit_reserved_open_is_refused_with_the_limit_reason() {
        // 32 Bound + 1 matching Reserved = 33 charged, including the Reserved route.
        let decision = decide_open_event(&matching_reserved_view(33));
        assert_eq!(decision, OpenEventDecision::Reject(OpenRejectReason::Limit));
    }

    #[test]
    fn a8i_at_limit_matching_reservation_binds() {
        // 31 Bound + 1 matching Reserved = 32 charged, including the Reserved route.
        let decision = decide_open_event(&matching_reserved_view(32));
        assert_eq!(decision, OpenEventDecision::Bind);
    }

    #[test]
    fn a8h_ablation_without_greater_than_maximum_would_bind() {
        let over = matching_reserved_view(33);
        let at_limit = matching_reserved_view(MAX_SUBSCRIPTION_CHANNELS);
        assert_eq!(decide_open_event(&at_limit), OpenEventDecision::Bind);
        assert_eq!(
            decide_open_event(&over),
            OpenEventDecision::Reject(OpenRejectReason::Limit)
        );
    }

    #[test]
    fn a8d_unreserved_label_is_refused_without_charging() {
        let view = OpenEventView {
            label: SubscriptionChannelLabel::terminal("s".into(), "sub".into(), 1),
            matching_state: None,
            charged_subscription_count: 0,
            generation_suppressed: false,
            peer_dying: false,
        };
        assert_eq!(
            decide_open_event(&view),
            OpenEventDecision::Reject(OpenRejectReason::Unreserved)
        );
    }

    #[test]
    fn a8f_partial_label_mismatch_does_not_select_a_reserved_route() {
        let reserved = SubscriptionChannelLabel::terminal("sess".into(), "sub".into(), 7);
        let mismatches = [
            SubscriptionChannelLabel {
                kind: reserved.kind,
                session_id: "other".into(),
                subscription_id: reserved.subscription_id.clone(),
                generation: reserved.generation,
            },
            SubscriptionChannelLabel {
                kind: reserved.kind,
                session_id: reserved.session_id.clone(),
                subscription_id: "other".into(),
                generation: reserved.generation,
            },
            SubscriptionChannelLabel {
                kind: reserved.kind,
                session_id: reserved.session_id.clone(),
                subscription_id: reserved.subscription_id.clone(),
                generation: 8,
            },
        ];
        for label in mismatches {
            assert_ne!(label, reserved);
            assert_eq!(
                decide_open_event(&OpenEventView {
                    label,
                    matching_state: None,
                    charged_subscription_count: 1,
                    generation_suppressed: false,
                    peer_dying: false,
                }),
                OpenEventDecision::Reject(OpenRejectReason::Unreserved)
            );
        }
        assert_eq!(
            decide_open_event(&OpenEventView {
                label: reserved,
                matching_state: Some(SubscriptionRouteState::Reserved),
                charged_subscription_count: 1,
                generation_suppressed: false,
                peer_dying: false,
            }),
            OpenEventDecision::Bind
        );
    }

    #[test]
    fn label_round_trip_percent_encodes_reserved_characters() {
        let label = SubscriptionChannelLabel::terminal("s/1".into(), "sub a".into(), 3);
        let formatted = label.format();
        assert_eq!(formatted, "bs/1/term/s%2F1/sub%20a/3");
        assert_eq!(SubscriptionChannelLabel::parse(&formatted), Some(label));
    }

    #[test]
    fn a25_aggregate_predicates_arm_only_at_the_exact_ceiling() {
        use super::{
            AGGREGATE_BUFFERED_HIGH, refuse_send_on_aggregate, reject_admission_on_aggregate,
        };
        assert!(!reject_admission_on_aggregate(AGGREGATE_BUFFERED_HIGH - 1));
        assert!(reject_admission_on_aggregate(AGGREGATE_BUFFERED_HIGH));
        assert!(!refuse_send_on_aggregate(AGGREGATE_BUFFERED_HIGH, 0));
        assert!(refuse_send_on_aggregate(AGGREGATE_BUFFERED_HIGH, 1));
        assert!(refuse_send_on_aggregate(AGGREGATE_BUFFERED_HIGH, 65_536));
        assert!(!refuse_send_on_aggregate(1_900_544, 65_536));
    }

    #[test]
    fn per_channel_keys_do_not_authenticate_across_labels() {
        use botster_core::{decrypt_aes_gcm, encrypt_aes_gcm};
        let secret = "secret-00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let first = subscription_channel_key(secret, "bs/1/term/s/a/1").expect("first key");
        let second = subscription_channel_key(secret, "bs/1/term/s/b/1").expect("second key");
        let envelope = encrypt_aes_gcm(&first, b"marker", 1).expect("encrypt");
        assert!(decrypt_aes_gcm(&first, &envelope).is_ok());
        assert!(decrypt_aes_gcm(&second, &envelope).is_err());
    }
}
