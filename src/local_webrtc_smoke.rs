//! Local runtime WebRTC smoke offerer.
//!
//! Owns the smoke offerer, host-control protocol 9 framing, waits, and the
//! sender terminal-record proof. CLI argument handling and top-level result
//! reporting stay in `main`.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{AesGcmEnvelope, AesGcmKey, decrypt_aes_gcm, encrypt_aes_gcm, seal_aes_gcm};
use botster_hub::{DaemonRequest, DaemonResponse, daemon_transport_request};
use botster_hub_client::{
    ClientFrame, DaemonCompatibilityRequirement, DaemonHello, DaemonHelloAck,
    DaemonLocalWebrtcBootstrap, DaemonLocalWebrtcDeliveryChunk, DaemonLocalWebrtcDeliveryKind,
    DaemonLocalWebrtcTerminalRecord, LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
    LOCAL_WEBRTC_MAX_DELIVERY_BYTES, LOCAL_WEBRTC_MAX_FRAME_BYTES, LocalWebrtcTerminalChunkHeader,
    PROTOCOL, RequestIdSequence, ServerFrame, encode_request_id,
};
use botster_terminal_protocol_client::{TerminalInputCommand, encode_terminal_input};
use bytes::BytesMut;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{
    Receiver as AsyncReceiver, Runtime, Sender as AsyncSender, channel, default_runtime, timeout,
};

use super::SmokeError;

const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_WAIT: Duration = Duration::from_secs(2);
/// Plaintext bytes per sealed terminal chunk, matching the Hub bound.
const TERMINAL_CHUNK_PAYLOAD_BYTES: usize = 12 * 1024;

/// One scheme 2 raw-bytes input operation encoded by the Core client codec.
fn terminal_input_frame(operation_id: u64, data: &[u8]) -> Result<Vec<u8>, SmokeError> {
    encode_terminal_input(&TerminalInputCommand::RawBytes {
        operation_id,
        data: data.to_vec(),
    })
    .map(|frame| frame.into_bytes())
    .map_err(|error| SmokeError::Webrtc(format!("encode terminal input: {error}")))
}

fn webrtc_runtime() -> std::sync::Arc<dyn Runtime> {
    default_runtime().expect("webrtc default runtime")
}

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for local WebRTC smoke")
        .block_on(fut)
}

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
            .encrypted_hello(
                &stream_key,
                &DaemonHello {
                    protocol: PROTOCOL.to_string(),
                    compatibility: DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
                    terminal_compatibility: None,
                },
            )
            .await?;
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
        let attach = offer_peer
            .encrypted_request(
                &stream_key,
                &DaemonRequest::Attach {
                    session_id: session_id.clone(),
                    subscription_id,
                },
            )
            .await?;
        let reservation = attach.terminal_reservation.ok_or_else(|| {
            SmokeError::Webrtc("Attach did not return a terminal reservation".to_string())
        })?;
        offer_peer
            .open_reserved_terminal(
                &stream_key,
                &reservation.label,
                reservation.generation,
                terminal_input_frame(1, b"from-smoke-webrtc\n")?,
            )
            .await?;
        let mut observed = Vec::new();
        let marker = b"webrtc:from-smoke-webrtc";
        for _ in 0..120 {
            let screen = offer_peer
                .encrypted_request(
                    &stream_key,
                    &DaemonRequest::ReadScreen {
                        session_id: session_id.clone(),
                    },
                )
                .await?;
            if let Some(screen) = screen.read_screen {
                observed = screen.text.into_bytes();
            }
            if observed
                .windows(marker.len())
                .any(|window| window == marker)
            {
                break;
            }
            webrtc_runtime().sleep(Duration::from_millis(30)).await;
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
        match wait_for_local_webrtc_sender_terminal_record(config, &bootstrap.grant_id) {
            Some(record) => eprintln!(
                "local_webrtc_terminal_record={}",
                serde_json::to_string(&record).expect("terminal record serializes")
            ),
            None => eprintln!(
                "local_webrtc_terminal_record=not_retained grant_id={}",
                bootstrap.grant_id
            ),
        }
    }
    result
}

fn wait_for_local_webrtc_sender_terminal_record(
    config: &botster_hub::HubConfig,
    expected_grant_id: &str,
) -> Option<DaemonLocalWebrtcTerminalRecord> {
    let deadline = Instant::now() + LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_WAIT;
    loop {
        let retained = daemon_transport_request(config, DaemonRequest::Status)
            .ok()
            .and_then(|response| response.status)
            .and_then(|status| {
                status
                    .local_webrtc_terminal_records
                    .into_iter()
                    .find(|record| record.grant_id == expected_grant_id)
            });
        if let Some(record) = retained
            && !record.cause.is_empty()
        {
            return Some(record);
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return None;
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
    request_ids: RequestIdSequence,
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
        let _ = timeout(
            runtime.as_ref(),
            Duration::from_secs(5),
            gather_complete_rx.recv(),
        )
        .await;
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
                request_ids: RequestIdSequence::new(),
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
        timeout(
            webrtc_runtime().as_ref(),
            Duration::from_secs(15),
            self.connected_rx.recv(),
        )
        .await
        .map_err(|_| SmokeError::Webrtc("timed out waiting for WebRTC connection".to_string()))?;
        timeout(
            webrtc_runtime().as_ref(),
            Duration::from_secs(10),
            self.data_channel_open_rx.recv(),
        )
        .await
        .map_err(|_| SmokeError::Webrtc("timed out waiting for data channel open".to_string()))?;
        Ok(())
    }

    /// Open the reserved terminal channel, complete its hello, and send one
    /// scheme 2 input frame as sealed binary chunks.
    async fn open_reserved_terminal(
        &mut self,
        key: &AesGcmKey,
        label: &str,
        generation: u64,
        input: Vec<u8>,
    ) -> Result<(), SmokeError> {
        let (open_tx, mut open_rx) = channel::<()>(1);
        let (message_tx, mut message_rx) = channel::<String>(256);
        let channel = self
            .peer
            .create_data_channel(
                label,
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
            let channel = channel.clone();
            webrtc_runtime().spawn(Box::pin(async move {
                while let Some(event) = channel.poll().await {
                    match event {
                        DataChannelEvent::OnOpen => {
                            let _ = open_tx.try_send(());
                        }
                        DataChannelEvent::OnMessage(message) => {
                            // Terminal frames arrive as binary chunks; only the
                            // JSON hello ack is text.
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
        timeout(
            webrtc_runtime().as_ref(),
            Duration::from_secs(10),
            open_rx.recv(),
        )
        .await
        .map_err(|_| SmokeError::Webrtc("reserved channel open timeout".to_string()))?;
        let hello = ClientFrame::Hello {
            hello: DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
                terminal_compatibility: None,
            },
        };
        channel
            .send_text(&encrypt_client_frame(key, &hello)?)
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        loop {
            let text = timeout(
                webrtc_runtime().as_ref(),
                Duration::from_secs(10),
                message_rx.recv(),
            )
            .await
            .map_err(|_| SmokeError::Webrtc("reserved hello ack timeout".to_string()))?
            .ok_or_else(|| {
                SmokeError::Webrtc("reserved channel closed during hello".to_string())
            })?;
            if let ServerFrame::HelloAck { .. } = assemble_server_frame(key, &text, &mut None)? {
                break;
            }
        }
        for chunk in sealed_terminal_chunks(key, &input, 1, generation)? {
            channel
                .send(BytesMut::from(chunk.as_slice()))
                .await
                .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        }
        Ok(())
    }

    async fn encrypted_hello(
        &mut self,
        key: &AesGcmKey,
        hello: &DaemonHello,
    ) -> Result<DaemonHelloAck, SmokeError> {
        let frame = ClientFrame::Hello {
            hello: hello.clone(),
        };
        self.data_channel
            .send_text(&encrypt_client_frame(key, &frame)?)
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        loop {
            let text = timeout(
                webrtc_runtime().as_ref(),
                Duration::from_secs(10),
                self.data_channel_message_rx.recv(),
            )
            .await
            .map_err(|_| SmokeError::Webrtc("hello ack timeout".to_string()))?
            .ok_or_else(|| SmokeError::Webrtc("channel closed during hello".to_string()))?;
            let mut assembly = None;
            match assemble_server_frame(key, &text, &mut assembly)? {
                ServerFrame::HelloAck { ack } => return Ok(ack),
                ServerFrame::Close { reason } => {
                    return Err(SmokeError::Webrtc(format!(
                        "hub closed the control channel during hello: {reason:?}"
                    )));
                }
                _ => {}
            }
        }
    }

    async fn encrypted_request(
        &mut self,
        key: &AesGcmKey,
        request: &DaemonRequest,
    ) -> Result<DaemonResponse, SmokeError> {
        let operation = smoke_local_webrtc_request_operation(request);
        let request_id = encode_request_id(self.request_ids.next());
        let frame = ClientFrame::Request {
            request_id: request_id.clone(),
            request: request.clone(),
        };
        self.data_channel
            .send_text(&encrypt_client_frame(key, &frame)?)
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let mut assembly: Option<ChunkAssembly> = None;
        loop {
            let progress = assembly.as_ref().map(|assembly| {
                (
                    assembly.message_id.clone(),
                    assembly.next_chunk_index,
                    assembly.chunk_count,
                )
            });
            let text = timeout(
                webrtc_runtime().as_ref(),
                Duration::from_secs(10),
                self.data_channel_message_rx.recv(),
            )
            .await
            .map_err(|_| {
                SmokeError::Webrtc(local_webrtc_response_progress_error(
                    operation,
                    "response_timeout",
                    &progress,
                ))
            })?
            .ok_or_else(|| {
                SmokeError::Webrtc(local_webrtc_response_progress_error(
                    operation,
                    "channel_closed",
                    &progress,
                ))
            })?;
            if text.len() >= LOCAL_WEBRTC_MAX_FRAME_BYTES {
                return Err(SmokeError::Webrtc(
                    "local WebRTC response chunk exceeded frame bound".to_string(),
                ));
            }
            let Some(frame) = push_server_chunk(key, &text, &mut assembly)? else {
                continue;
            };
            match frame {
                ServerFrame::Response {
                    request_id: answered,
                    response,
                } if answered == request_id => return Ok(response),
                ServerFrame::Response { .. } => {
                    return Err(SmokeError::Webrtc(format!(
                        "local WebRTC response correlation mismatch: operation={operation}"
                    )));
                }
                ServerFrame::Close { reason } => {
                    return Err(SmokeError::Webrtc(format!(
                        "hub closed the control channel: operation={operation} reason={reason:?}"
                    )));
                }
                ServerFrame::HelloAck { .. }
                | ServerFrame::Event { .. }
                | ServerFrame::Entity { .. } => {}
            }
        }
    }
}

/// Reassembly of one chunked control delivery.
struct ChunkAssembly {
    message_id: String,
    chunk_count: u32,
    total_bytes: usize,
    next_chunk_index: u32,
    encrypted: String,
}

/// Feed one text message; returns the decoded frame when the delivery completes.
fn push_server_chunk(
    key: &AesGcmKey,
    text: &str,
    assembly: &mut Option<ChunkAssembly>,
) -> Result<Option<ServerFrame>, SmokeError> {
    let chunk = serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(text)
        .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
    if chunk.delivery_kind != DaemonLocalWebrtcDeliveryKind::ServerFrame
        || chunk.version != LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION
        || chunk.total_bytes as usize > LOCAL_WEBRTC_MAX_DELIVERY_BYTES
    {
        return Err(SmokeError::Webrtc(
            "invalid local WebRTC delivery chunk".to_string(),
        ));
    }
    let current = match assembly.as_mut() {
        Some(current)
            if current.message_id == chunk.message_id
                && current.chunk_count == chunk.chunk_count
                && current.total_bytes == chunk.total_bytes as usize =>
        {
            current
        }
        Some(_) => {
            return Err(SmokeError::Webrtc(
                "interleaved local WebRTC delivery chunks".to_string(),
            ));
        }
        None => {
            if chunk.chunk_index != 0 {
                return Err(SmokeError::Webrtc(
                    "local WebRTC delivery started mid-message".to_string(),
                ));
            }
            assembly.insert(ChunkAssembly {
                message_id: chunk.message_id.clone(),
                chunk_count: chunk.chunk_count,
                total_bytes: chunk.total_bytes as usize,
                next_chunk_index: 0,
                encrypted: String::with_capacity(chunk.total_bytes as usize),
            })
        }
    };
    if chunk.chunk_index != current.next_chunk_index {
        return Err(SmokeError::Webrtc(
            "invalid local WebRTC response chunk sequence".to_string(),
        ));
    }
    current.encrypted.push_str(&chunk.payload);
    current.next_chunk_index += 1;
    if current.next_chunk_index < current.chunk_count {
        return Ok(None);
    }
    if current.encrypted.len() != current.total_bytes {
        return Err(SmokeError::Webrtc(
            "local WebRTC response byte count mismatch".to_string(),
        ));
    }
    let encrypted = std::mem::take(&mut current.encrypted);
    *assembly = None;
    let envelope = serde_json::from_str::<AesGcmEnvelope>(&encrypted)
        .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
    let plaintext =
        decrypt_aes_gcm(key, &envelope).map_err(|error| SmokeError::Webrtc(error.to_string()))?;
    serde_json::from_slice(&plaintext)
        .map(Some)
        .map_err(|error| SmokeError::Webrtc(error.to_string()))
}

/// Assemble one complete server frame from a single-message delivery stream.
fn assemble_server_frame(
    key: &AesGcmKey,
    text: &str,
    assembly: &mut Option<ChunkAssembly>,
) -> Result<ServerFrame, SmokeError> {
    push_server_chunk(key, text, assembly)?.ok_or_else(|| {
        SmokeError::Webrtc("local WebRTC hello ack spans several chunks".to_string())
    })
}

fn encrypt_client_frame(key: &AesGcmKey, frame: &ClientFrame) -> Result<String, SmokeError> {
    let plaintext =
        serde_json::to_vec(frame).map_err(|error| SmokeError::Webrtc(error.to_string()))?;
    let envelope = encrypt_aes_gcm(key, &plaintext, 1)
        .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
    serde_json::to_string(&envelope).map_err(|error| SmokeError::Webrtc(error.to_string()))
}

/// Seal one input frame into ordered binary terminal chunks.
///
/// Client-to-Hub chunks carry the fixed attachment generation and a zero
/// `stream_epoch`; Hub validates only the generation.
fn sealed_terminal_chunks(
    key: &AesGcmKey,
    plaintext: &[u8],
    message_id: u64,
    generation: u64,
) -> Result<Vec<Vec<u8>>, SmokeError> {
    let chunk_count = u32::try_from(
        plaintext
            .len()
            .max(1)
            .div_ceil(TERMINAL_CHUNK_PAYLOAD_BYTES),
    )
    .map_err(|_| SmokeError::Webrtc("terminal chunk count overflow".to_string()))?;
    let total_bytes = u32::try_from(plaintext.len())
        .map_err(|_| SmokeError::Webrtc("terminal input length overflow".to_string()))?;
    let mut chunks = Vec::with_capacity(chunk_count as usize);
    for (chunk_index, slice) in plaintext.chunks(TERMINAL_CHUNK_PAYLOAD_BYTES).enumerate() {
        let header = LocalWebrtcTerminalChunkHeader {
            message_id,
            chunk_index: chunk_index as u32,
            chunk_count,
            total_bytes,
            generation,
            stream_epoch: 0,
        };
        let mut message = header.encode().to_vec();
        seal_aes_gcm(key, slice, &mut message)
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        if message.len() >= LOCAL_WEBRTC_MAX_FRAME_BYTES {
            return Err(SmokeError::Webrtc(
                "local WebRTC terminal input chunk exceeded frame bound".to_string(),
            ));
        }
        chunks.push(message);
    }
    Ok(chunks)
}

fn smoke_local_webrtc_request_operation(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Status => "status",
        DaemonRequest::Spawn { .. } => "spawn",
        DaemonRequest::Attach { .. } => "attach",
        DaemonRequest::ReadScreen { .. } => "read_screen",
        DaemonRequest::ShutdownSession { .. } => "shutdown_session",
        _ => "other",
    }
}

fn local_webrtc_response_progress_error(
    operation: &str,
    cause: &str,
    progress: &Option<(String, u32, u32)>,
) -> String {
    match progress {
        Some((message_id, next_chunk, chunk_count)) => format!(
            "local WebRTC response incomplete: operation={operation} cause={cause} message_id={message_id} next_chunk={next_chunk} expected_chunks={chunk_count}"
        ),
        None => format!(
            "local WebRTC response incomplete: operation={operation} cause={cause} message_id=pending next_chunk=0 expected_chunks=pending"
        ),
    }
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

#[cfg(test)]
mod tests {
    use super::local_webrtc_response_progress_error;

    #[test]
    fn response_error_reports_channel_close_before_first_chunk() {
        assert_eq!(
            local_webrtc_response_progress_error("status", "channel_closed", &None),
            "local WebRTC response incomplete: operation=status cause=channel_closed message_id=pending next_chunk=0 expected_chunks=pending"
        );
    }

    #[test]
    fn response_error_reports_timeout_before_first_chunk() {
        assert_eq!(
            local_webrtc_response_progress_error("status", "response_timeout", &None),
            "local WebRTC response incomplete: operation=status cause=response_timeout message_id=pending next_chunk=0 expected_chunks=pending"
        );
    }

    #[test]
    fn response_error_reports_channel_close_after_partial_chunks() {
        assert_eq!(
            local_webrtc_response_progress_error(
                "read_screen",
                "channel_closed",
                &Some(("response-7".to_string(), 1, 3)),
            ),
            "local WebRTC response incomplete: operation=read_screen cause=channel_closed message_id=response-7 next_chunk=1 expected_chunks=3"
        );
    }

    #[test]
    fn response_error_reports_timeout_after_partial_chunks() {
        assert_eq!(
            local_webrtc_response_progress_error(
                "read_screen",
                "response_timeout",
                &Some(("response-7".to_string(), 1, 3)),
            ),
            "local WebRTC response incomplete: operation=read_screen cause=response_timeout message_id=response-7 next_chunk=1 expected_chunks=3"
        );
    }
}
