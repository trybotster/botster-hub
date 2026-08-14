#![allow(dead_code, unused_imports)]

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::{
    AesGcmEnvelope, AesGcmKey, Capability, CapabilitySurface, CoreSessionMetadata,
    ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, HostProfileMetadata,
    HostProfilePolicySection, PackageSource, ProcessIdentity, RequestId, ResizePayload, SessionId,
    SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, decrypt_aes_gcm,
    encrypt_aes_gcm,
};
use botster_core_daemon::{RegistryRecord, SessionRegistry};
use botster_hub::{
    CoreEngineOptions, DataDirectoryOption, FileHubStateStore, HostIdentityOptions, HubClientApi,
    HubClientEvent, HubClientRequest, HubClientResponseBody, HubDaemon, HubDaemonState,
    HubPackageManifest, HubStartupOptions, HubStateLoadSource, HubStateStore,
    LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, PackageAdmissionPolicy, PackageProvenance,
    PackageRegistry, RuntimeEnvironment, SessionDefaults, SpawnTarget, TransportBindings,
};
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{
    Receiver as AsyncReceiver, Sender as AsyncSender, block_on, channel, default_runtime, sleep,
    timeout,
};

use crate::support::{
    ensure_session_worker_binary, recovering_mutex_guard, validate_cli_daemon_shutdown,
    wait_for_cli_daemon_shutdown,
};

use super::*;

pub(crate) const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE: &str =
    "local-webrtc-sender-terminal.json";
pub(crate) const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES: usize = 4096;
pub(crate) const TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV: &str =
    "BOTSTER_HUB_TEST_CLOSE_LOCAL_WEBRTC_OPERATION";
pub(crate) struct LocalWebrtcOffererHandler {
    pub(crate) gather_complete_tx: AsyncSender<()>,
    pub(crate) connected_tx: AsyncSender<()>,
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

pub(crate) struct LocalWebrtcOfferPeer {
    pub(crate) peer: Box<dyn PeerConnection>,
    pub(crate) data_channel: Arc<dyn DataChannel>,
    pub(crate) connected_rx: AsyncReceiver<()>,
    pub(crate) data_channel_open_rx: AsyncReceiver<()>,
    pub(crate) data_channel_message_rx: AsyncReceiver<String>,
    pub(crate) pending_entity_frames: VecDeque<botster_hub_client::DaemonEntityFrame>,
    pub(crate) pending_terminal_frames: VecDeque<(String, Vec<u8>)>,
    pub(crate) pending_host_events: VecDeque<botster_hub_client::DaemonEvent>,
    pub(crate) accept_host_events: bool,
}

pub(crate) struct ExtraWebrtcDataChannel {
    pub(crate) messages: AsyncReceiver<String>,
}

impl LocalWebrtcOfferPeer {
    pub(crate) async fn create_offer()
    -> Result<(Self, serde_json::Value), Box<dyn std::error::Error>> {
        let runtime =
            default_runtime().ok_or_else(|| std::io::Error::other("no async runtime found"))?;
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
            .await?;
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
            .await?;
        assert!(data_channel.ordered().await?);
        assert_eq!(data_channel.max_retransmits().await?, None);
        assert_eq!(data_channel.max_packet_life_time().await?, None);

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

        let offer = peer.create_offer(None).await?;
        peer.set_local_description(offer).await?;
        let _ = timeout(Duration::from_secs(5), gather_complete_rx.recv()).await;
        let offer = peer
            .local_description()
            .await
            .ok_or_else(|| std::io::Error::other("offer local description missing"))?;
        let offer = serde_json::to_value(offer)?;

        Ok((
            Self {
                peer: Box::new(peer),
                data_channel,
                connected_rx,
                data_channel_open_rx,
                data_channel_message_rx,
                pending_entity_frames: VecDeque::new(),
                pending_terminal_frames: VecDeque::new(),
                pending_host_events: VecDeque::new(),
                accept_host_events: false,
            },
            offer,
        ))
    }

    pub(crate) async fn accept_answer(
        &mut self,
        answer: serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let answer = serde_json::from_value::<RTCSessionDescription>(answer)?;
        self.peer.set_remote_description(answer).await?;
        timeout(Duration::from_secs(15), self.connected_rx.recv())
            .await
            .map_err(|_| std::io::Error::other("timed out waiting for WebRTC connection"))?;
        timeout(Duration::from_secs(10), self.data_channel_open_rx.recv())
            .await
            .map_err(|_| std::io::Error::other("timed out waiting for data channel open"))?;
        Ok(())
    }

    pub(crate) async fn encrypted_request(
        &mut self,
        key: &AesGcmKey,
        request: &botster_hub_client::DaemonRequest,
    ) -> Result<botster_hub_client::DaemonResponse, Box<dyn std::error::Error>> {
        Ok(self.encrypted_request_with_metrics(key, request).await?.0)
    }

    pub(crate) async fn encrypted_request_with_metrics(
        &mut self,
        key: &AesGcmKey,
        request: &botster_hub_client::DaemonRequest,
    ) -> Result<
        (
            botster_hub_client::DaemonResponse,
            LocalWebrtcResponseMetrics,
        ),
        Box<dyn std::error::Error>,
    > {
        let plaintext = serde_json::to_vec(request)?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1)?;
        self.data_channel
            .send_text(&serde_json::to_string(&envelope)?)
            .await?;
        loop {
            let (delivery_kind, plaintext, metrics) = self.receive_delivery(key).await?;
            match delivery_kind {
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonResponse => {
                    return Ok((serde_json::from_slice(&plaintext)?, metrics));
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame => {
                    self.pending_entity_frames
                        .push_back(serde_json::from_slice(&plaintext)?);
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame => {
                    self.pending_terminal_frames
                        .push_back((String::new(), plaintext));
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEvent => {
                    self.park_or_reject_host_event(&plaintext)?;
                }
            }
        }
    }

    pub(crate) async fn next_entity_frame(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<botster_hub_client::DaemonEntityFrame, Box<dyn std::error::Error>> {
        if let Some(frame) = self.pending_entity_frames.pop_front() {
            return Ok(frame);
        }
        loop {
            let (delivery_kind, plaintext, _) = self.receive_delivery(key).await?;
            match delivery_kind {
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame => {
                    return Ok(serde_json::from_slice(&plaintext)?);
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonResponse => {
                    return Err(std::io::Error::other(
                        "received uncorrelated daemon response while waiting for entity frame",
                    )
                    .into());
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame => {
                    self.pending_terminal_frames
                        .push_back((String::new(), plaintext));
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEvent => {
                    self.park_or_reject_host_event(&plaintext)?;
                }
            }
        }
    }

    pub(crate) async fn encrypted_hello(
        &mut self,
        key: &AesGcmKey,
        hello: &botster_hub_client::DaemonHello,
    ) -> Result<botster_hub_client::DaemonHelloAck, Box<dyn std::error::Error>> {
        let plaintext = serde_json::to_vec(hello)?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1)?;
        self.data_channel
            .send_text(&serde_json::to_string(&envelope)?)
            .await?;
        let (delivery_kind, plaintext, _) = self.receive_delivery(key).await?;
        if delivery_kind != botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonResponse {
            return Err(std::io::Error::other(format!(
                "hello ack used unexpected delivery kind {delivery_kind:?}"
            ))
            .into());
        }
        Ok(serde_json::from_slice(&plaintext)?)
    }

    pub(crate) async fn next_terminal_frame(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        if let Some((_message_id, bytes)) = self.pending_terminal_frames.pop_front() {
            return Ok(bytes);
        }
        loop {
            let (delivery_kind, plaintext, _) = self.receive_delivery(key).await?;
            match delivery_kind {
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame => {
                    return Ok(plaintext);
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame => {
                    self.pending_entity_frames
                        .push_back(serde_json::from_slice(&plaintext)?);
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonResponse => {
                    return Err(std::io::Error::other(
                        "received daemon response while waiting for terminal frame",
                    )
                    .into());
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEvent => {
                    self.park_or_reject_host_event(&plaintext)?;
                }
            }
        }
    }

    pub(crate) fn enable_host_events(&mut self) {
        self.accept_host_events = true;
    }

    fn park_or_reject_host_event(
        &mut self,
        plaintext: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.accept_host_events {
            return Err(std::io::Error::other(
                "unnegotiated IsolatedHub receive path must not decode daemon_event",
            )
            .into());
        }
        self.pending_host_events
            .push_back(serde_json::from_slice(plaintext)?);
        Ok(())
    }

    pub(crate) async fn next_host_event(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<botster_hub_client::DaemonEvent, Box<dyn std::error::Error>> {
        if let Some(event) = self.pending_host_events.pop_front() {
            return Ok(event);
        }
        loop {
            let (delivery_kind, plaintext, _) = self.receive_delivery(key).await?;
            match delivery_kind {
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEvent => {
                    return Ok(serde_json::from_slice(&plaintext)?);
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame => {
                    self.pending_terminal_frames
                        .push_back((String::new(), plaintext));
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame => {
                    self.pending_entity_frames
                        .push_back(serde_json::from_slice(&plaintext)?);
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonResponse => {
                    return Err(std::io::Error::other(
                        "received daemon response while waiting for host event",
                    )
                    .into());
                }
            }
        }
    }

    pub(crate) async fn receive_delivery(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<
        (
            botster_hub_client::DaemonLocalWebrtcDeliveryKind,
            Vec<u8>,
            LocalWebrtcResponseMetrics,
        ),
        Box<dyn std::error::Error>,
    > {
        let mut encrypted = String::new();
        let mut delivery_kind = None;
        let mut message_id = None;
        let mut expected_chunk_count = None;
        let mut maximum_frame_bytes = 0;
        let mut next_chunk_index = 0;
        loop {
            let response =
                match timeout(Duration::from_secs(10), self.data_channel_message_rx.recv()).await {
                    Ok(Some(response)) => response,
                    Ok(None) => {
                        return Err(local_webrtc_response_progress_error(
                            "channel_closed",
                            message_id.as_deref(),
                            next_chunk_index,
                            expected_chunk_count,
                        )
                        .into());
                    }
                    Err(_) => {
                        return Err(local_webrtc_response_progress_error(
                            "response_timeout",
                            message_id.as_deref(),
                            next_chunk_index,
                            expected_chunk_count,
                        )
                        .into());
                    }
                };
            maximum_frame_bytes = maximum_frame_bytes.max(response.len());
            assert!(
                response.len() < botster_hub_client::LOCAL_WEBRTC_MAX_FRAME_BYTES,
                "response frame exceeded 64 KiB"
            );
            let chunk = serde_json::from_str::<botster_hub_client::DaemonLocalWebrtcDeliveryChunk>(
                &response,
            )?;
            assert_eq!(
                chunk.version,
                botster_hub_client::LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION
            );
            if let Some(delivery_kind) = delivery_kind {
                assert_eq!(delivery_kind, chunk.delivery_kind);
            } else {
                delivery_kind = Some(chunk.delivery_kind);
            }
            assert_eq!(chunk.chunk_index, next_chunk_index);
            if let Some(message_id) = &message_id {
                assert_eq!(message_id, &chunk.message_id);
            } else {
                message_id = Some(chunk.message_id.clone());
                expected_chunk_count = Some(chunk.chunk_count);
            }
            assert_eq!(expected_chunk_count, Some(chunk.chunk_count));
            encrypted.push_str(&chunk.payload);
            next_chunk_index += 1;
            if chunk.chunk_index + 1 == chunk.chunk_count {
                assert_eq!(encrypted.len(), chunk.total_bytes as usize);
                break;
            }
        }
        let envelope_bytes = encrypted.len();
        let chunk_count = expected_chunk_count.unwrap_or(0) as usize;
        let envelope = serde_json::from_str::<AesGcmEnvelope>(&encrypted)?;
        let plaintext = decrypt_aes_gcm(key, &envelope)?;
        Ok((
            delivery_kind.expect("complete delivery declares a kind"),
            plaintext,
            LocalWebrtcResponseMetrics {
                envelope_bytes,
                chunk_count,
                maximum_frame_bytes,
            },
        ))
    }

    pub(crate) async fn create_extra_data_channel(
        &mut self,
    ) -> Result<ExtraWebrtcDataChannel, Box<dyn std::error::Error>> {
        let runtime =
            default_runtime().ok_or_else(|| std::io::Error::other("no async runtime found"))?;
        let (open_tx, mut open_rx) = channel::<()>(1);
        let (message_tx, message_rx) = channel::<String>(256);
        let extra = self
            .peer
            .create_data_channel(
                "botster-extra",
                Some(RTCDataChannelInit {
                    ordered: true,
                    max_retransmits: None,
                    max_packet_life_time: None,
                    ..Default::default()
                }),
            )
            .await?;
        {
            let extra = extra.clone();
            runtime.spawn(Box::pin(async move {
                while let Some(event) = extra.poll().await {
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
        let _ = timeout(Duration::from_secs(5), open_rx.recv()).await;
        Ok(ExtraWebrtcDataChannel {
            messages: message_rx,
        })
    }
}

#[derive(Debug)]
pub(crate) struct LocalWebrtcResponseMetrics {
    pub(crate) envelope_bytes: usize,
    pub(crate) chunk_count: usize,
    pub(crate) maximum_frame_bytes: usize,
}

pub(crate) fn local_webrtc_response_progress_error(
    cause: &str,
    message_id: Option<&str>,
    next_chunk_index: u32,
    expected_chunk_count: Option<u32>,
) -> std::io::Error {
    std::io::Error::other(format!(
        "local WebRTC response incomplete: cause={cause} message_id={} next_chunk={} expected_chunks={}",
        message_id.unwrap_or("pending"),
        next_chunk_index,
        expected_chunk_count.map_or_else(|| "pending".to_string(), |count| count.to_string()),
    ))
}

pub(crate) fn local_webrtc_stream_key(secret: &str) -> AesGcmKey {
    let hex = secret
        .strip_prefix("secret-")
        .expect("local WebRTC secret prefix");
    let bytes = decode_hex_bytes(hex).expect("local WebRTC secret hex");
    AesGcmKey::from_slice(&bytes).expect("local WebRTC secret is an AES-GCM key")
}

pub(crate) async fn open_local_webrtc_peer(
    endpoint: &botster_hub_client::DaemonEndpoint,
    bootstrap: &botster_hub_client::DaemonLocalWebrtcBootstrap,
) -> (LocalWebrtcOfferPeer, AesGcmKey) {
    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);
    let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer()
        .await
        .expect("create WebRTC offer peer");
    let signal = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::LocalWebrtcSignal {
            grant_id: bootstrap.grant_id.clone(),
            grant_secret: bootstrap.grant_secret.clone(),
            origin: bootstrap.expected_origin.clone(),
            offer,
        },
    )
    .expect("signal local WebRTC offer");
    assert_eq!(
        signal.kind,
        botster_hub_client::DaemonResponseKind::LocalWebrtcAnswer
    );
    let answer = signal
        .local_webrtc_answer
        .as_ref()
        .expect("signal response includes WebRTC answer")
        .answer
        .clone();
    offer_peer
        .accept_answer(answer)
        .await
        .expect("offer peer accepts answer and opens channel");
    (offer_peer, stream_key)
}

pub(crate) fn write_botster_web_package(root: &Path) {
    fs::create_dir_all(root.join("scripts")).expect("create botster-web package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write botster-web core entrypoint");
    fs::write(
        root.join("scripts").join("local-package-server.mjs"),
        r#"
import fs from 'fs';
import http from 'http';
import net from 'net';

const port = Number(process.env.BOTSTER_WEB_PORT || '0');
const connection = JSON.parse(process.env.BOTSTER_HUB_CONNECTION || 'null');
const socket = connection?.transport?.type === 'unix_socket'
  ? connection.transport.path
  : undefined;
const dataDir = process.env.BOTSTER_HUB_DATA_DIR;
const launchResult = process.env.BOTSTER_ENTRYPOINT_LAUNCH_RESULT;
const source = socket ? 'socket' : (dataDir ? 'data_dir' : 'spawned');
const mode = socket || dataDir ? 'existing_hub' : 'spawned_hub';
const startupDelayMs = Number(process.env.BOTSTER_WEB_TEST_STARTUP_DELAY_MS || '0');
const connections = new Map();
let boundPort = null;

function currentRequirement() {
  return {
    protocol: 'botster-hub-daemon-v1',
    protocol_version: 1,
    required_features: [
      'sessions',
      'terminal_streaming',
      'resize',
      'plugin_surface_render',
      'plugin_surface_action',
    ],
    minimum_conformance_fixture_revision: 1,
    client_name: 'botster-web-production-runtime-fixture',
  };
}

function readLine(connection) {
  const newline = connection.buffer.indexOf('\n');
  if (newline >= 0) {
    const line = connection.buffer.slice(0, newline);
    connection.buffer = connection.buffer.slice(newline + 1);
    return Promise.resolve(line);
  }

  return new Promise((resolve, reject) => {
    const onData = (chunk) => {
      connection.buffer += chunk.toString('utf8');
      const newline = connection.buffer.indexOf('\n');
      if (newline < 0) {
        return;
      }
      cleanup();
      const line = connection.buffer.slice(0, newline);
      connection.buffer = connection.buffer.slice(newline + 1);
      resolve(line);
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const cleanup = () => {
      connection.stream.off('data', onData);
      connection.stream.off('error', onError);
    };
    connection.stream.on('data', onData);
    connection.stream.once('error', onError);
  });
}

async function connectDaemon() {
  if (!socket) {
    throw new Error('BOTSTER_HUB_CONNECTION does not contain a Unix socket');
  }
  const stream = net.createConnection(socket);
  const connection = { stream, buffer: '' };
  try {
    await new Promise((resolve, reject) => {
      stream.once('connect', resolve);
      stream.once('error', reject);
    });
    stream.write(JSON.stringify({
      protocol: 'botster-hub-daemon-v1',
      compatibility: currentRequirement(),
    }) + '\n');
    const helloAck = JSON.parse(await readLine(connection));
    if (helloAck.protocol !== 'botster-hub-daemon-v1' || !helloAck.compatibility) {
      throw new Error(`unexpected daemon hello ack: ${JSON.stringify(helloAck)}`);
    }
    return connection;
  } catch (error) {
    stream.destroy();
    throw error;
  }
}

async function probeDaemon() {
  let connection = null;
  try {
    connection = await connectDaemon();
    connection.stream.write(JSON.stringify({ type: 'status' }) + '\n');
    const response = JSON.parse(await readLine(connection));
    if (response.kind !== 'status' || !response.status) {
      throw new Error(`unexpected daemon status response: ${JSON.stringify(response)}`);
    }
  } finally {
    connection?.stream.destroy();
  }
}

function currentSocketExists() {
  return socket ? fs.existsSync(socket) : false;
}

async function daemonRequest(payload) {
  const connectionId = payload.connection_id || null;
  let connection = connectionId ? connections.get(connectionId) : null;
  if (!connection) {
    connection = await connectDaemon();
    if (connectionId) {
      connections.set(connectionId, connection);
    }
  }

  connection.stream.write(JSON.stringify(payload.request) + '\n');
  const response = JSON.parse(await readLine(connection));

  if (!connectionId || payload.close === true) {
    connection.stream.end();
    if (connectionId) {
      connections.delete(connectionId);
    }
  }

  return response;
}

const server = http.createServer(async (request, response) => {
  if (request.url === '/') {
    try {
      let bootstrap = null;
      if (socket && fs.existsSync(socket)) {
        const origin = `http://${request.headers.host}`;
        try {
          const daemonResponse = await daemonRequest({
            request: {
              type: 'issue_local_webrtc_bootstrap',
              package_name: 'botster-web',
              entrypoint_id: 'web-client',
              origin,
            },
          });
          bootstrap = daemonResponse.local_webrtc_bootstrap || null;
          if (!bootstrap) {
            throw new Error(`missing local WebRTC bootstrap: ${JSON.stringify(daemonResponse)}`);
          }
        } catch (error) {
          if (!String(error && error.message ? error.message : error).includes('ENOENT')) {
            throw error;
          }
        }
      }
      response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      response.end(`<!doctype html><html><head><title>Botster Web</title><script>globalThis.__BOTSTER_LOCAL_WEBRTC_BOOTSTRAP__ = ${JSON.stringify(bootstrap)};</script></head><body><main id="root">botster-web packaged UI</main><script type="module" src="/assets/index.js"></script></body></html>`);
    } catch (error) {
      response.writeHead(502, { 'content-type': 'text/plain; charset=utf-8' });
      response.end(String(error && error.message ? error.message : error));
    }
    return;
  }
  if (request.url !== '/health') {
    response.writeHead(404);
    response.end('not found');
    return;
  }
  let daemonReady = false;
  let error = null;
  try {
    await probeDaemon();
    daemonReady = true;
  } catch (probeError) {
    error = String(probeError && probeError.message ? probeError.message : probeError).slice(0, 240);
  }
  const socketExists = currentSocketExists();
  response.writeHead(200, { 'content-type': 'application/json' });
  response.end(JSON.stringify({
    ok: mode === 'existing_hub' && source === 'socket' && daemonReady,
    mode,
    source,
    port: boundPort,
    socketExists,
    daemonReady,
    error,
  }));
});

const listen = () => server.listen(port, '127.0.0.1', () => {
  boundPort = server.address().port;
  console.log(`web_listening=http://127.0.0.1:${boundPort}`);
  if (launchResult) {
    fs.writeFileSync(launchResult, JSON.stringify({
      entrypoint_id: 'web-client',
      process_state: 'running',
      local_url: `http://127.0.0.1:${boundPort}/`,
    }));
  }
});

if (startupDelayMs > 0) {
  setTimeout(listen, startupDelayMs);
} else {
  listen();
}
"#,
    )
    .expect("write botster-web package server script");
    let manifest = serde_json::json!({
        "name": "botster-web",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [{
            "id": "web-client",
            "kind": "web_app",
            "command": "node",
            "args": ["scripts/local-package-server.mjs"],
            "working_directory": { "policy": "package_root" },
            "injections": [
                {
                    "kind": "hub_connection",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_CONNECTION"
                    },
                    "required": true
                },
                {
                    "kind": "data_dir",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_DATA_DIR"
                    },
                    "required": true
                }
            ],
            "environment": [
                { "name": "BOTSTER_WEB_PORT", "required": false, "default": "0" },
                { "name": "BOTSTER_WEB_TEST_STARTUP_DELAY_MS", "required": false }
            ],
            "launch_mode": "background",
            "readiness": { "result_fields": ["local_url"] },
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize botster-web manifest"),
    )
    .expect("write botster-web manifest");
}

pub(crate) fn rewrite_botster_web_entrypoint(
    root: &Path,
    version: &str,
    script_name: &str,
    marker_name: &str,
) {
    let original = fs::read_to_string(root.join("scripts/local-package-server.mjs"))
        .expect("read original botster-web entrypoint");
    let original = original
        .strip_prefix("#!/usr/bin/env node\n")
        .unwrap_or(&original);
    let marker = format!(
        "fs.writeFileSync(new URL('../{marker_name}', import.meta.url), 'refreshed');\nconst port ="
    );
    let refreshed = format!(
        "#!/usr/bin/env node\n{}",
        original.replacen("const port =", &marker, 1)
    );
    let script_path = root.join("scripts").join(script_name);
    fs::write(&script_path, refreshed).expect("write refreshed botster-web entrypoint");
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755))
        .expect("make refreshed botster-web entrypoint executable");

    let manifest_path = root.join("botster-package.json");
    let mut manifest: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(&manifest_path).expect("read botster-web manifest"),
    )
    .expect("parse botster-web manifest");
    manifest["version"] = serde_json::Value::String(version.to_string());
    manifest["runnable_entrypoints"][0]["command"] =
        serde_json::Value::String(format!("scripts/{script_name}"));
    fs::write(
        manifest_path,
        serde_json::to_string_pretty(&manifest).expect("serialize refreshed botster-web manifest"),
    )
    .expect("write refreshed botster-web manifest");
}

pub(crate) fn botster_web_page_bootstrap(
    web_origin: &str,
) -> botster_hub_client::DaemonLocalWebrtcBootstrap {
    let (headers, body) = read_http_path(web_origin, "/");
    assert!(
        headers.starts_with("HTTP/1.1 200") || headers.starts_with("HTTP/1.0 200"),
        "botster-web page returned non-200: {headers} body={body}"
    );
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: text/html"),
        "botster-web page should be HTML: {headers}"
    );
    let marker = "globalThis.__BOTSTER_LOCAL_WEBRTC_BOOTSTRAP__ = ";
    let start = body
        .find(marker)
        .map(|index| index + marker.len())
        .expect("HTML page includes local WebRTC bootstrap global");
    let rest = &body[start..];
    let end = rest
        .find(";</script>")
        .expect("HTML bootstrap script terminates");
    serde_json::from_str(&rest[..end]).expect("HTML bootstrap JSON")
}

pub(crate) fn log_botster_web_phase(test_started: Instant, phase: &str) {
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_millis();
    eprintln!(
        "botster_web_reload_phase phase={phase} unix_ms={unix_ms} elapsed_ms={}",
        test_started.elapsed().as_millis()
    );
}

pub(crate) fn probe_botster_web_health(web_origin: &str) -> Result<serde_json::Value, String> {
    let port = web_origin
        .strip_prefix("http://127.0.0.1:")
        .expect("local HTTP URL")
        .parse::<u16>()
        .expect("HTTP port");

    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .map_err(|error| format!("connect error: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .map_err(|error| format!("set read timeout: {error}"))?;
    let request =
        format!("GET /health HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("write error: {error}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| format!("read error: {error}"))?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("missing HTTP response body: {response:?}"))?;
    let body = if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        decode_chunked_http_body(body)
    } else {
        body.to_string()
    };
    if !headers.starts_with("HTTP/1.1 200") && !headers.starts_with("HTTP/1.0 200") {
        return Err(format!("non-200 response: {headers} body={body}"));
    }
    let health: serde_json::Value = serde_json::from_str(body.trim())
        .map_err(|error| format!("invalid health JSON: {error}; body={body}"))?;
    let expected = serde_json::json!({
        "ok": true,
        "mode": "existing_hub",
        "source": "socket",
        "port": port,
        "socketExists": true,
        "daemonReady": true,
        "error": null,
    });
    if expected
        .as_object()
        .expect("expected health object")
        .iter()
        .any(|(key, value)| health.get(key) != Some(value))
    {
        return Err(format!(
            "unexpected health response: {health}; expected={expected}"
        ));
    }
    Ok(health)
}

pub(crate) fn wait_for_botster_web_readiness(
    endpoint: &botster_hub_client::DaemonEndpoint,
    web_origin: &str,
    expected_local_url: &str,
    test_started: Instant,
) -> botster_hub_client::DaemonResponse {
    let wait_started = Instant::now();
    let mut health_ready = false;
    let mut last_health = "not probed".to_string();
    let mut last_apps: String;

    loop {
        match botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListApps) {
            Ok(response) => {
                last_apps = format!("{response:#?}");
                if let Some(app) = response.apps.iter().find(|app| {
                    app.package_name == "botster-web" && app.entrypoint_id == "web-client"
                }) {
                    if matches!(
                        app.lifecycle_state.as_str(),
                        "exited" | "failed" | "stopped"
                    ) {
                        let entrypoint_status = botster_hub_client::request(
                            endpoint,
                            botster_hub_client::DaemonRequest::PackageEntrypointStatus {
                                package_name: "botster-web".to_string(),
                                entrypoint_id: "web-client".to_string(),
                            },
                        );
                        let daemon_status = botster_hub_client::request(
                            endpoint,
                            botster_hub_client::DaemonRequest::Status,
                        );
                        panic!(
                            "botster-web package server reached terminal state while waiting for readiness; elapsed_ms={} expected_local_url={expected_local_url} app={app:#?} entrypoint_status={entrypoint_status:#?} daemon_status={daemon_status:#?} last_health={last_health}",
                            wait_started.elapsed().as_millis()
                        );
                    }
                    if let Some(actual_url) = app.launch_target.local_url.as_deref() {
                        assert_eq!(
                            actual_url,
                            expected_local_url,
                            "botster-web app published unexpected local_url after {}ms; app={app:#?}",
                            wait_started.elapsed().as_millis()
                        );
                        if health_ready {
                            log_botster_web_phase(test_started, "local_url_published");
                            return response;
                        }
                    }
                }
            }
            Err(error) => {
                last_apps = format!("ListApps request error: {error:#?}");
            }
        }

        if !health_ready {
            match probe_botster_web_health(web_origin) {
                Ok(health) => {
                    health_ready = true;
                    last_health = health.to_string();
                    log_botster_web_phase(test_started, "health_ready");
                }
                Err(error) => last_health = error,
            }
        }

        if wait_started.elapsed() >= BOTSTER_WEB_READINESS_LIVENESS_BACKSTOP {
            let entrypoint_status = botster_hub_client::request(
                endpoint,
                botster_hub_client::DaemonRequest::PackageEntrypointStatus {
                    package_name: "botster-web".to_string(),
                    entrypoint_id: "web-client".to_string(),
                },
            );
            let daemon_status =
                botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::Status);
            panic!(
                "botster-web package server liveness backstop expired without readiness; elapsed_ms={} health_ready={health_ready} expected_local_url={expected_local_url} last_health={last_health} last_apps={last_apps} entrypoint_status={entrypoint_status:#?} daemon_status={daemon_status:#?}",
                wait_started.elapsed().as_millis()
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
}

pub(crate) fn local_webrtc_sender_failure(stderr: &[u8]) -> Option<&str> {
    std::str::from_utf8(stderr)
        .ok()?
        .lines()
        .rev()
        .find(|line| line.starts_with("local WebRTC response delivery failed:"))
}

pub(crate) fn local_webrtc_grant_id(output: &Output) -> Option<String> {
    command_output_text(output)
        .lines()
        .find_map(|line| line.strip_prefix("local_webrtc_grant_id="))
        .filter(|grant_id| !grant_id.is_empty() && grant_id.len() <= 128)
        .map(str::to_string)
}

pub(crate) fn local_webrtc_sender_terminal_record(
    data_dir: &Path,
    expected_grant_id: &str,
) -> serde_json::Value {
    let path = data_dir.join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    assert!(
        !path.with_extension("json.tmp").exists(),
        "same-directory replacement must not leave a temporary sender record"
    );
    let bytes = fs::read(&path).expect("read persisted local WebRTC sender terminal record");
    assert!(
        bytes.len() <= LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES,
        "sender terminal record exceeded fixed size bound"
    );
    let record: serde_json::Value = serde_json::from_slice(&bytes)
        .expect("parse persisted local WebRTC sender terminal record");
    let object = record
        .as_object()
        .expect("sender terminal record has a fixed JSON object schema");
    let mut actual_fields = object.keys().map(String::as_str).collect::<Vec<_>>();
    actual_fields.sort_unstable();
    let mut expected_fields = vec![
        "schema_version",
        "grant_id",
        "request_operation",
        "message_id",
        "next_chunk_index",
        "last_sent_chunk_index",
        "total_chunks",
        "pressured",
        "peer_connection_state",
        "channel_terminal_signal",
        "cause",
        "cleanup_disposition",
    ];
    expected_fields.sort_unstable();
    assert_eq!(actual_fields, expected_fields);
    assert_eq!(record["schema_version"], 1);
    assert_eq!(record["grant_id"], expected_grant_id);
    assert!(
        matches!(
            record["request_operation"].as_str(),
            Some(
                "status"
                    | "spawn"
                    | "attach"
                    | "send_input"
                    | "drain"
                    | "shutdown_session"
                    | "request_queue_overflow"
                    | "none"
                    | "other"
            )
        ),
        "sender record has a typed request operation: {record}"
    );
    assert!(record["message_id"].is_null() || record["message_id"].is_string());
    assert!(record["next_chunk_index"].is_u64());
    assert!(record["last_sent_chunk_index"].is_null() || record["last_sent_chunk_index"].is_u64());
    assert!(record["total_chunks"].is_u64());
    assert!(record["pressured"].is_boolean());
    assert!(
        matches!(
            record["peer_connection_state"].as_str(),
            Some(
                "unspecified"
                    | "new"
                    | "connecting"
                    | "connected"
                    | "disconnected"
                    | "failed"
                    | "closed"
            )
        ),
        "sender record has a typed peer state: {record}"
    );
    assert!(
        matches!(
            record["channel_terminal_signal"].as_str(),
            Some("none" | "on_close" | "on_error" | "poll_ended")
        ),
        "sender record has a typed channel signal: {record}"
    );
    assert!(
        matches!(
            record["cause"].as_str(),
            Some(
                "send_text"
                    | "channel_closed"
                    | "channel_error"
                    | "poll_ended"
                    | "invalid_request"
                    | "request_queue_overflow"
                    | "invalid_encrypted_request"
                    | "runtime_queue_closed"
                    | "response_framing"
                    | "low_water_threshold_setup"
                    | "high_water_threshold_setup"
                    | "peer_disconnected"
                    | "peer_failed"
                    | "peer_closed"
            )
        ),
        "sender record has a typed terminal cause: {record}"
    );
    assert_eq!(record["cleanup_disposition"], "newly_sent");
    let text = String::from_utf8(bytes).expect("sender terminal record is UTF-8 JSON");
    for forbidden in [
        "grant_secret",
        "payload",
        "request_body",
        "response_body",
        env!("CARGO_MANIFEST_DIR"),
    ] {
        assert!(
            !text.contains(forbidden),
            "sender terminal record contains forbidden data {forbidden:?}: {text}"
        );
    }
    assert!(
        !text.contains(&data_dir.display().to_string()),
        "sender terminal record contains its data-directory path"
    );
    record
}

pub(crate) fn local_webrtc_smoke_failure_evidence(output: &Output, data_dir: &Path) -> String {
    let text = command_output_text(output);
    let Some(grant_id) = local_webrtc_grant_id(output) else {
        return format!("smoke failed before local WebRTC bootstrap: {text}");
    };
    let record_path = data_dir.join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    if !record_path.is_file() {
        return format!(
            "smoke failed: {text}; sender_record=missing file={LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE}"
        );
    }
    let terminal_record = local_webrtc_sender_terminal_record(data_dir, &grant_id);
    format!("smoke failed: {text}; sender_record={terminal_record}")
}

pub(crate) fn local_webrtc_bounded_stderr_tail(stderr: &[u8], data_dir: &Path) -> String {
    const MAX_LINES: usize = 20;
    const MAX_CHARS_PER_LINE: usize = 512;

    let stderr = String::from_utf8_lossy(stderr);
    let mut lines = stderr.lines().rev().take(MAX_LINES).collect::<Vec<_>>();
    lines.reverse();
    let mut tail = lines
        .into_iter()
        .map(|line| {
            let mut bounded = line.chars().take(MAX_CHARS_PER_LINE).collect::<String>();
            if line.chars().count() > MAX_CHARS_PER_LINE {
                bounded.push_str("<truncated>");
            }
            bounded
        })
        .collect::<Vec<_>>()
        .join("\n");

    for (path, replacement) in [
        (Some(data_dir.to_path_buf()), "<data-dir>"),
        (
            Some(PathBuf::from(env!("CARGO_MANIFEST_DIR"))),
            "<workspace>",
        ),
        (std::env::var_os("HOME").map(PathBuf::from), "<home>"),
        (Some(std::env::temp_dir()), "<temp>"),
    ] {
        if let Some(path) = path.and_then(|path| path.to_str().map(str::to_owned))
            && !path.is_empty()
        {
            tail = tail.replace(&path, replacement);
        }
    }

    if tail.is_empty() {
        "<empty>".to_string()
    } else {
        tail
    }
}
