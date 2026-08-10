//! Long-lived local WebRTC offerer for the live no-spin oracle.
//!
//! Modes:
//! - `write-fixture <pkg-dir>` — write the shared botster-web/web-client fixture
//! - `connect --socket ... --package ... --entrypoint ... --origin ...` — complete
//!   bootstrap + answer signaling, open a data channel, print readiness, and park

use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use botster_core::{AesGcmEnvelope, AesGcmKey, decrypt_aes_gcm, encrypt_aes_gcm};
use botster_hub_client::{
    DaemonConnection, DaemonEndpoint, DaemonLocalWebrtcBootstrap, DaemonLocalWebrtcDeliveryChunk,
    DaemonLocalWebrtcDeliveryKind, DaemonRequest, DaemonResponse,
    LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION, LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
    LOCAL_WEBRTC_MAX_FRAME_BYTES,
};
use botster_hub_test_support::write_botster_web_production_fixture;
use tokio::runtime::Builder as TokioRuntimeBuilder;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{
    Receiver as AsyncReceiver, Sender as AsyncSender, channel, default_runtime, timeout,
};

type BoxError = Box<dyn Error + Send + Sync>;

fn main() {
    if let Err(error) = run() {
        eprintln!("local_webrtc_offerer error: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), BoxError> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        return Err("usage: local_webrtc_offerer write-fixture <pkg-dir> | connect --socket <path> --package <name> --entrypoint <id> --origin <origin>".into());
    }
    match args.remove(0).as_str() {
        "write-fixture" => {
            let pkg_dir = args
                .first()
                .ok_or("write-fixture requires <pkg-dir>")?
                .clone();
            write_botster_web_production_fixture(PathBuf::from(&pkg_dir).as_path());
            println!("fixture_written path={pkg_dir}");
            Ok(())
        }
        "connect" => connect_mode(&args),
        other => Err(format!("unknown mode: {other}").into()),
    }
}

fn connect_mode(args: &[String]) -> Result<(), BoxError> {
    let socket = require_flag(args, "--socket")?;
    let package = require_flag(args, "--package")?;
    let entrypoint = require_flag(args, "--entrypoint")?;
    let origin = require_flag(args, "--origin")?;

    let runtime = TokioRuntimeBuilder::new_multi_thread()
        .worker_threads(2)
        .thread_name("local-webrtc-offerer")
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let endpoint = DaemonEndpoint::new(PathBuf::from(socket));
        let mut connection = DaemonConnection::connect(&endpoint)?;
        let bootstrap_response = connection.request(&DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: package,
            entrypoint_id: entrypoint,
            origin: origin.clone(),
        })?;
        let bootstrap = bootstrap_response
            .local_webrtc_bootstrap
            .ok_or("issue_local_webrtc_bootstrap returned no bootstrap")?;
        println!("local_webrtc_grant_id={}", bootstrap.grant_id);

        let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer().await?;
        let signal_response = connection.request(&DaemonRequest::LocalWebrtcSignal {
            grant_id: bootstrap.grant_id.clone(),
            grant_secret: bootstrap.grant_secret.clone(),
            origin,
            offer,
        })?;
        let answer = signal_response
            .local_webrtc_answer
            .ok_or("local_webrtc_signal returned no answer")?
            .answer;
        offer_peer.accept_answer(answer).await?;

        // Prove the data channel is live with a status round-trip, then park.
        let stream_key = stream_key_from_secret(&bootstrap.grant_secret)?;
        let status = offer_peer
            .encrypted_request(&stream_key, &DaemonRequest::Status)
            .await?;
        if status.kind != botster_hub_client::DaemonResponseKind::Status {
            return Err(format!("unexpected status response kind: {:?}", status.kind).into());
        }

        let pid = process::id();
        println!("offerer_ready pid={pid}");
        // Hold the peer open until killed externally (kill -9). ICE keepalives continue.
        loop {
            thread::sleep(Duration::from_secs(3600));
        }
    })
}

fn require_flag(args: &[String], flag: &str) -> Result<String, BoxError> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == flag {
            return iter
                .next()
                .cloned()
                .ok_or_else(|| format!("{flag} requires a value").into());
        }
    }
    Err(format!("missing required flag {flag}").into())
}

fn stream_key_from_secret(secret: &str) -> Result<AesGcmKey, BoxError> {
    let hex = secret
        .strip_prefix("secret-")
        .ok_or("local WebRTC secret prefix missing")?;
    let bytes = decode_hex(hex).ok_or("local WebRTC secret hex invalid")?;
    AesGcmKey::from_slice(&bytes).map_err(|error| error.to_string().into())
}

fn decode_hex(encoded: &str) -> Option<Vec<u8>> {
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

struct LocalWebrtcOfferPeer {
    peer: Box<dyn PeerConnection>,
    data_channel: Arc<dyn DataChannel>,
    connected_rx: AsyncReceiver<()>,
    data_channel_open_rx: AsyncReceiver<()>,
    data_channel_message_rx: AsyncReceiver<String>,
}

struct LocalWebrtcOffererHandler {
    gather_complete_tx: AsyncSender<()>,
    connected_tx: AsyncSender<()>,
}

#[async_trait]
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

impl LocalWebrtcOfferPeer {
    async fn create_offer() -> Result<(Self, serde_json::Value), BoxError> {
        let runtime = default_runtime().ok_or("no async runtime found")?;
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
            .map_err(|error| error.to_string())?;
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
            .map_err(|error| error.to_string())?;

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
            .map_err(|error| error.to_string())?;
        peer.set_local_description(offer)
            .await
            .map_err(|error| error.to_string())?;
        let _ = timeout(Duration::from_secs(5), gather_complete_rx.recv()).await;
        let offer = peer
            .local_description()
            .await
            .ok_or("offer local description missing")?;
        let offer = serde_json::to_value(offer)?;

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

    async fn accept_answer(&mut self, answer: serde_json::Value) -> Result<(), BoxError> {
        let answer = serde_json::from_value::<RTCSessionDescription>(answer)?;
        self.peer
            .set_remote_description(answer)
            .await
            .map_err(|error| error.to_string())?;
        timeout(Duration::from_secs(15), self.connected_rx.recv())
            .await
            .map_err(|_| "timed out waiting for WebRTC connection".to_string())?
            .ok_or("connected channel closed")?;
        timeout(Duration::from_secs(10), self.data_channel_open_rx.recv())
            .await
            .map_err(|_| "timed out waiting for data channel open".to_string())?
            .ok_or("data channel open channel closed")?;
        Ok(())
    }

    async fn encrypted_request(
        &mut self,
        key: &AesGcmKey,
        request: &DaemonRequest,
    ) -> Result<DaemonResponse, BoxError> {
        let plaintext = serde_json::to_vec(request)?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1).map_err(|error| error.to_string())?;
        self.data_channel
            .send_text(&serde_json::to_string(&envelope)?)
            .await
            .map_err(|error| error.to_string())?;
        let mut encrypted = String::new();
        let mut message_id = None;
        let mut chunk_count = None;
        let mut next_chunk_index = 0u32;
        loop {
            let response = timeout(Duration::from_secs(10), self.data_channel_message_rx.recv())
                .await
                .map_err(|_| "response timeout".to_string())?
                .ok_or("channel closed")?;
            if response.len() >= LOCAL_WEBRTC_MAX_FRAME_BYTES {
                return Err("local WebRTC response chunk exceeded frame bound".into());
            }
            let chunk = serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(&response)?;
            if chunk.version != LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION
                || chunk.delivery_kind != DaemonLocalWebrtcDeliveryKind::DaemonResponse
                || chunk.chunk_index != next_chunk_index
                || chunk.total_bytes as usize > LOCAL_WEBRTC_MAX_DELIVERY_BYTES
                || message_id
                    .as_ref()
                    .is_some_and(|id| id != &chunk.message_id)
                || chunk_count.is_some_and(|count| count != chunk.chunk_count)
            {
                return Err("invalid local WebRTC response chunk sequence".into());
            }
            message_id.get_or_insert(chunk.message_id);
            chunk_count.get_or_insert(chunk.chunk_count);
            encrypted.push_str(&chunk.payload);
            next_chunk_index += 1;
            if chunk.chunk_index + 1 == chunk.chunk_count {
                if encrypted.len() != chunk.total_bytes as usize {
                    return Err("local WebRTC response byte count mismatch".into());
                }
                break;
            }
        }
        let envelope = serde_json::from_str::<AesGcmEnvelope>(&encrypted)?;
        let plaintext = decrypt_aes_gcm(key, &envelope).map_err(|error| error.to_string())?;
        Ok(serde_json::from_slice(&plaintext)?)
    }
}

// Silence unused-import if DaemonLocalWebrtcBootstrap is only used for type inference.
#[allow(dead_code)]
fn _bootstrap_type_anchor(_: &DaemonLocalWebrtcBootstrap) {}
