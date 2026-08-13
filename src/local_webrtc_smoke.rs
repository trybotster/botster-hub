//! Local runtime WebRTC smoke offerer.
//!
//! Owns the smoke offerer, framing, waits, and sender terminal-record proof.
//! CLI argument handling and top-level result reporting stay in `main`.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{AesGcmEnvelope, AesGcmKey, decrypt_aes_gcm, encrypt_aes_gcm};
use botster_hub::{DaemonEvent, DaemonRequest, DaemonResponse, daemon_transport_request};
use botster_hub_client::{
    DaemonLocalWebrtcBootstrap, DaemonLocalWebrtcDeliveryChunk, DaemonLocalWebrtcDeliveryKind,
    LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION, LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
    LOCAL_WEBRTC_MAX_FRAME_BYTES,
};
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{
    Receiver as AsyncReceiver, Sender as AsyncSender, block_on, channel, default_runtime, sleep,
    timeout,
};

use super::SmokeError;

const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE: &str = "local-webrtc-sender-terminal.json";
const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_WAIT: Duration = Duration::from_secs(2);

pub(crate) fn smoke_local_webrtc_round_trip(
    config: &botster_hub::HubConfig,
    bootstrap: &DaemonLocalWebrtcBootstrap,
) -> Result<(), SmokeError> {
    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret)?;
    let result = block_on(async {
        let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer().await?;
        let signal = daemon_transport_request(
            config,
            DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: bootstrap.grant_secret.clone(),
                origin: bootstrap.expected_origin.clone(),
                offer,
            },
        )?;
        let answer = signal
            .local_webrtc_answer
            .as_ref()
            .ok_or_else(|| SmokeError::Webrtc("missing local WebRTC answer".to_string()))?
            .answer
            .clone();
        offer_peer.accept_answer(answer).await?;
        offer_peer
            .encrypted_request(&stream_key, &DaemonRequest::Status)
            .await?;

        let session_id = "smoke-local-webrtc-session".to_string();
        let subscription_id = "smoke-local-webrtc-subscription".to_string();
        offer_peer
            .encrypted_request(
                &stream_key,
                &DaemonRequest::Spawn {
                    session_id: session_id.clone(),
                    command: "printf 'webrtc-smoke-ready\\n'; while IFS= read -r line; do printf 'webrtc:%s\\n' \"$line\"; done".to_string(),
                },
            )
            .await?;
        offer_peer
            .encrypted_request(
                &stream_key,
                &DaemonRequest::Attach {
                    session_id: session_id.clone(),
                    subscription_id,
                },
            )
            .await?;
        offer_peer
            .encrypted_request(
                &stream_key,
                &DaemonRequest::SendInput {
                    session_id: session_id.clone(),
                    data: "from-smoke-webrtc\n".to_string(),
                },
            )
            .await?;
        let mut observed = Vec::new();
        let marker = b"webrtc:from-smoke-webrtc";
        for _ in 0..120 {
            let drain = offer_peer
                .encrypted_request(
                    &stream_key,
                    &DaemonRequest::Drain {
                        session_id: session_id.clone(),
                        subscription_id: None,
                    },
                )
                .await?;
            for event in drain.events {
                if let DaemonEvent::TerminalOutput { payload, .. } = event {
                    let bytes = payload.decoded_bytes().map_err(SmokeError::Webrtc)?;
                    observed.extend_from_slice(&bytes);
                }
            }
            if observed
                .windows(marker.len())
                .any(|window| window == marker)
            {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
        let _ = offer_peer
            .encrypted_request(
                &stream_key,
                &DaemonRequest::ShutdownSession {
                    session_id: session_id.clone(),
                },
            )
            .await;
        let _ = offer_peer.peer.close().await;
        if observed
            .windows(marker.len())
            .any(|window| window == marker)
        {
            Ok(())
        } else {
            Err(SmokeError::Webrtc(format!(
                "local WebRTC terminal marker not observed; observed_bytes={}",
                observed.len()
            )))
        }
    });
    if result.is_err() {
        wait_for_local_webrtc_sender_terminal_record(config, &bootstrap.grant_id);
    }
    result
}

fn wait_for_local_webrtc_sender_terminal_record(
    config: &botster_hub::HubConfig,
    expected_grant_id: &str,
) {
    let path = config
        .data_directory
        .join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    let deadline = Instant::now() + LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_WAIT;
    loop {
        if std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|record| {
                record
                    .get("grant_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .as_deref()
            == Some(expected_grant_id)
        {
            return;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return;
        };
        thread::sleep(remaining.min(Duration::from_millis(10)));
    }
}

struct LocalWebrtcOffererHandler {
    gather_complete_tx: AsyncSender<()>,
    connected_tx: AsyncSender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for LocalWebrtcOffererHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_complete_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }
}

struct LocalWebrtcOfferPeer {
    peer: Box<dyn PeerConnection>,
    data_channel: Arc<dyn DataChannel>,
    connected_rx: AsyncReceiver<()>,
    data_channel_open_rx: AsyncReceiver<()>,
    data_channel_message_rx: AsyncReceiver<String>,
}

impl LocalWebrtcOfferPeer {
    async fn create_offer() -> Result<(Self, serde_json::Value), SmokeError> {
        let runtime = default_runtime()
            .ok_or_else(|| SmokeError::Webrtc("no async runtime found".to_string()))?;
        let (gather_complete_tx, mut gather_complete_rx) = channel::<()>(1);
        let (connected_tx, connected_rx) = channel::<()>(1);
        let (data_channel_open_tx, data_channel_open_rx) = channel::<()>(1);
        let (data_channel_message_tx, data_channel_message_rx) = channel::<String>(256);
        let handler = Arc::new(LocalWebrtcOffererHandler {
            gather_complete_tx,
            connected_tx,
        });
        let peer = PeerConnectionBuilder::new()
            .with_handler(handler)
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let data_channel = peer
            .create_data_channel(
                "botster-client",
                Some(RTCDataChannelInit {
                    ordered: true,
                    max_retransmits: None,
                    max_packet_life_time: None,
                    ..Default::default()
                }),
            )
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;

        {
            let data_channel = data_channel.clone();
            let open_tx = data_channel_open_tx.clone();
            let message_tx = data_channel_message_tx.clone();
            runtime.spawn(Box::pin(async move {
                while let Some(event) = data_channel.poll().await {
                    match event {
                        DataChannelEvent::OnOpen => {
                            let _ = open_tx.try_send(());
                        }
                        DataChannelEvent::OnMessage(message) => {
                            if let Ok(text) = String::from_utf8(message.data.to_vec()) {
                                let _ = message_tx.try_send(text);
                            }
                        }
                        DataChannelEvent::OnClose => break,
                        _ => {}
                    }
                }
            }));
        }

        let offer = peer
            .create_offer(None)
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        peer.set_local_description(offer)
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let _ = timeout(Duration::from_secs(5), gather_complete_rx.recv()).await;
        let offer = peer
            .local_description()
            .await
            .ok_or_else(|| SmokeError::Webrtc("offer local description missing".to_string()))?;
        let offer =
            serde_json::to_value(offer).map_err(|error| SmokeError::Webrtc(error.to_string()))?;

        Ok((
            Self {
                peer: Box::new(peer),
                data_channel,
                connected_rx,
                data_channel_open_rx,
                data_channel_message_rx,
            },
            offer,
        ))
    }

    async fn accept_answer(&mut self, answer: serde_json::Value) -> Result<(), SmokeError> {
        let answer = serde_json::from_value::<RTCSessionDescription>(answer)
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        self.peer
            .set_remote_description(answer)
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        timeout(Duration::from_secs(15), self.connected_rx.recv())
            .await
            .map_err(|_| {
                SmokeError::Webrtc("timed out waiting for WebRTC connection".to_string())
            })?;
        timeout(Duration::from_secs(10), self.data_channel_open_rx.recv())
            .await
            .map_err(|_| {
                SmokeError::Webrtc("timed out waiting for data channel open".to_string())
            })?;
        Ok(())
    }

    async fn encrypted_request(
        &mut self,
        key: &AesGcmKey,
        request: &DaemonRequest,
    ) -> Result<DaemonResponse, SmokeError> {
        let operation = smoke_local_webrtc_request_operation(request);
        let plaintext =
            serde_json::to_vec(request).map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1)
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        self.data_channel
            .send_text(
                &serde_json::to_string(&envelope)
                    .map_err(|error| SmokeError::Webrtc(error.to_string()))?,
            )
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let mut encrypted = String::new();
        let mut message_id = None;
        let mut chunk_count = None;
        let mut next_chunk_index = 0;
        loop {
            let response = timeout(Duration::from_secs(10), self.data_channel_message_rx.recv())
                .await
                .map_err(|_| {
                    SmokeError::Webrtc(local_webrtc_response_progress_error(
                        operation,
                        "response_timeout",
                        message_id.as_deref(),
                        next_chunk_index,
                        chunk_count,
                    ))
                })?
                .ok_or_else(|| {
                    SmokeError::Webrtc(local_webrtc_response_progress_error(
                        operation,
                        "channel_closed",
                        message_id.as_deref(),
                        next_chunk_index,
                        chunk_count,
                    ))
                })?;
            if response.len() >= LOCAL_WEBRTC_MAX_FRAME_BYTES {
                return Err(SmokeError::Webrtc(
                    "local WebRTC response chunk exceeded frame bound".to_string(),
                ));
            }
            let chunk = serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(&response)
                .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
            if chunk.version != LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION
                || chunk.delivery_kind != DaemonLocalWebrtcDeliveryKind::DaemonResponse
                || chunk.chunk_index != next_chunk_index
                || chunk.total_bytes as usize > LOCAL_WEBRTC_MAX_DELIVERY_BYTES
                || message_id
                    .as_ref()
                    .is_some_and(|id| id != &chunk.message_id)
                || chunk_count.is_some_and(|count| count != chunk.chunk_count)
            {
                return Err(SmokeError::Webrtc(
                    "invalid local WebRTC response chunk sequence".to_string(),
                ));
            }
            message_id.get_or_insert(chunk.message_id);
            chunk_count.get_or_insert(chunk.chunk_count);
            encrypted.push_str(&chunk.payload);
            next_chunk_index += 1;
            if chunk.chunk_index + 1 == chunk.chunk_count {
                if encrypted.len() != chunk.total_bytes as usize {
                    return Err(SmokeError::Webrtc(
                        "local WebRTC response byte count mismatch".to_string(),
                    ));
                }
                break;
            }
        }
        let envelope = serde_json::from_str::<AesGcmEnvelope>(&encrypted)
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let plaintext = decrypt_aes_gcm(key, &envelope)
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        serde_json::from_slice(&plaintext).map_err(|error| SmokeError::Webrtc(error.to_string()))
    }
}

fn smoke_local_webrtc_request_operation(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Status => "status",
        DaemonRequest::Spawn { .. } => "spawn",
        DaemonRequest::Attach { .. } => "attach",
        DaemonRequest::SendInput { .. } => "send_input",
        DaemonRequest::Drain { .. } => "drain",
        DaemonRequest::ShutdownSession { .. } => "shutdown_session",
        _ => "other",
    }
}

fn local_webrtc_response_progress_error(
    operation: &str,
    cause: &str,
    message_id: Option<&str>,
    next_chunk_index: u32,
    expected_chunk_count: Option<u32>,
) -> String {
    format!(
        "local WebRTC response incomplete: operation={operation} cause={cause} message_id={} next_chunk={} expected_chunks={}",
        message_id.unwrap_or("pending"),
        next_chunk_index,
        expected_chunk_count.map_or_else(|| "pending".to_string(), |count| count.to_string()),
    )
}

fn local_webrtc_stream_key(secret: &str) -> Result<AesGcmKey, SmokeError> {
    let hex = secret
        .strip_prefix("secret-")
        .ok_or_else(|| SmokeError::Webrtc("local WebRTC secret prefix missing".to_string()))?;
    let bytes = decode_hex_bytes(hex)
        .ok_or_else(|| SmokeError::Webrtc("local WebRTC secret hex invalid".to_string()))?;
    AesGcmKey::from_slice(&bytes).map_err(|error| SmokeError::Webrtc(error.to_string()))
}

fn decode_hex_bytes(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    let mut output = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        output.push((high << 4) | low);
    }
    Some(output)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
