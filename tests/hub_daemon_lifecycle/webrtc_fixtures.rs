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
    atomic::{AtomicBool, AtomicU64, Ordering},
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

use botster_hub_test_support::monotonic_now_ns;

use super::*;

pub(crate) const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE: &str =
    "local-webrtc-sender-terminal.json";
pub(crate) const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES: usize = 4096;
/// Match shipped client mailbox event bound so this fixture cannot hide lag.
pub(crate) const WEBRTC_INBOUND_MAX_FRAMES: usize = 128;
pub(crate) const WEBRTC_INBOUND_MAX_BYTES: usize = 512 * 1024;
pub(crate) const WEBRTC_PENDING_HOST_EVENTS_MAX: usize = 128;
pub(crate) const WEBRTC_PENDING_HOST_EVENTS_MAX_BYTES: usize = 512 * 1024;
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

struct InboundReassembly {
    encrypted: String,
    delivery_kind: Option<botster_hub_client::DaemonLocalWebrtcDeliveryKind>,
    message_id: Option<String>,
    expected_chunk_count: Option<u32>,
    maximum_frame_bytes: usize,
    next_chunk_index: u32,
}

pub(crate) struct FixtureQueueSnapshot {
    pub count: u64,
    pub bytes: u64,
    pub high_water_count: u64,
    pub high_water_bytes: u64,
    pub oldest_age_us: Option<u64>,
    pub overflow: u64,
    pub max_count: u64,
    pub max_bytes: u64,
}

struct FixtureQueueOccupancy {
    count: AtomicU64,
    bytes: AtomicU64,
    high_water_count: AtomicU64,
    high_water_bytes: AtomicU64,
    overflow: AtomicU64,
    max_count: u64,
    max_bytes: u64,
    enqueue_ns: Mutex<VecDeque<u64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InboundAdmitError {
    CountLimit,
    ByteLimit,
    ChannelFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingHostEventAdmitError {
    CountLimit,
    ByteLimit,
}

impl std::fmt::Display for PendingHostEventAdmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CountLimit => {
                write!(
                    f,
                    "product_failure webrtc pending_host_events overflow count"
                )
            }
            Self::ByteLimit => {
                write!(
                    f,
                    "product_failure webrtc pending_host_events overflow bytes"
                )
            }
        }
    }
}

impl std::error::Error for PendingHostEventAdmitError {}

fn saturating_sub_u64(cell: &AtomicU64, amount: u64) {
    let mut current = cell.load(Ordering::Relaxed);
    while current > 0 {
        let next = current.saturating_sub(amount);
        match cell.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

impl FixtureQueueOccupancy {
    fn new(max_count: usize, max_bytes: usize) -> Self {
        Self {
            count: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            high_water_count: AtomicU64::new(0),
            high_water_bytes: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
            max_count: max_count as u64,
            max_bytes: max_bytes as u64,
            enqueue_ns: Mutex::new(VecDeque::new()),
        }
    }

    fn lock_enqueue_ns(&self) -> std::sync::MutexGuard<'_, VecDeque<u64>> {
        self.enqueue_ns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn would_exceed_count(&self) -> bool {
        self.count.load(Ordering::Relaxed) + 1 > self.max_count
    }

    fn would_exceed_bytes(&self, add_bytes: u64) -> bool {
        self.bytes.load(Ordering::Relaxed) + add_bytes > self.max_bytes
    }

    fn reserve(&self, add_bytes: u64) {
        let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        let bytes = self.bytes.fetch_add(add_bytes, Ordering::Relaxed) + add_bytes;
        self.high_water_count.fetch_max(count, Ordering::Relaxed);
        self.high_water_bytes.fetch_max(bytes, Ordering::Relaxed);
        self.lock_enqueue_ns().push_back(monotonic_now_ns());
    }

    fn rollback_reserve(&self, add_bytes: u64) {
        saturating_sub_u64(&self.count, 1);
        saturating_sub_u64(&self.bytes, add_bytes);
        let _ = self.lock_enqueue_ns().pop_back();
    }

    fn record_pop(&self, sub_bytes: u64) {
        saturating_sub_u64(&self.count, 1);
        saturating_sub_u64(&self.bytes, sub_bytes);
        let _ = self.lock_enqueue_ns().pop_front();
    }

    fn record_overflow(&self) {
        self.overflow.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> FixtureQueueSnapshot {
        let oldest_age_us = self
            .lock_enqueue_ns()
            .front()
            .copied()
            .map(|oldest_ns| monotonic_now_ns().saturating_sub(oldest_ns) / 1_000);
        FixtureQueueSnapshot {
            count: self.count.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            high_water_count: self.high_water_count.load(Ordering::Relaxed),
            high_water_bytes: self.high_water_bytes.load(Ordering::Relaxed),
            oldest_age_us,
            overflow: self.overflow.load(Ordering::Relaxed),
            max_count: self.max_count,
            max_bytes: self.max_bytes,
        }
    }
}

fn admit_inbound_frame_with_send<F>(
    occupancy: &FixtureQueueOccupancy,
    add_bytes: u64,
    send: F,
) -> Result<(), InboundAdmitError>
where
    F: FnOnce() -> Result<(), InboundAdmitError>,
{
    if occupancy.would_exceed_count() {
        occupancy.record_overflow();
        return Err(InboundAdmitError::CountLimit);
    }
    if occupancy.would_exceed_bytes(add_bytes) {
        occupancy.record_overflow();
        return Err(InboundAdmitError::ByteLimit);
    }
    occupancy.reserve(add_bytes);
    match send() {
        Ok(()) => Ok(()),
        Err(error) => {
            occupancy.rollback_reserve(add_bytes);
            occupancy.record_overflow();
            Err(error)
        }
    }
}

fn admit_inbound_frame(
    occupancy: &FixtureQueueOccupancy,
    tx: &AsyncSender<String>,
    text: String,
) -> Result<(), InboundAdmitError> {
    let add_bytes = text.len() as u64;
    admit_inbound_frame_with_send(occupancy, add_bytes, || {
        tx.try_send(text)
            .map_err(|_| InboundAdmitError::ChannelFull)
    })
}

struct WebrtcInboundMailbox {
    rx: AsyncReceiver<String>,
    occupancy: Arc<FixtureQueueOccupancy>,
    reassembly: Option<InboundReassembly>,
}

impl WebrtcInboundMailbox {
    fn bounded(max_count: usize, max_bytes: usize) -> (AsyncSender<String>, Self) {
        let (tx, rx) = channel::<String>(max_count);
        (
            tx,
            Self {
                rx,
                occupancy: Arc::new(FixtureQueueOccupancy::new(max_count, max_bytes)),
                reassembly: None,
            },
        )
    }

    async fn receive_delivery(
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
        loop {
            let response = match timeout(Duration::from_secs(10), self.rx.recv()).await {
                Ok(Some(response)) => {
                    self.occupancy.record_pop(response.len() as u64);
                    response
                }
                Ok(None) => {
                    let progress = self.reassembly.take();
                    return Err(local_webrtc_response_progress_error(
                        "channel_closed",
                        progress.as_ref().and_then(|row| row.message_id.as_deref()),
                        progress
                            .as_ref()
                            .map(|row| row.next_chunk_index)
                            .unwrap_or(0),
                        progress.as_ref().and_then(|row| row.expected_chunk_count),
                    )
                    .into());
                }
                Err(_) => {
                    let progress = self.reassembly.take();
                    return Err(local_webrtc_response_progress_error(
                        "response_timeout",
                        progress.as_ref().and_then(|row| row.message_id.as_deref()),
                        progress
                            .as_ref()
                            .map(|row| row.next_chunk_index)
                            .unwrap_or(0),
                        progress.as_ref().and_then(|row| row.expected_chunk_count),
                    )
                    .into());
                }
            };
            if let Some(finished) = apply_inbound_chunk(&mut self.reassembly, &response)? {
                let envelope_bytes = finished.encrypted.len();
                let chunk_count = finished.expected_chunk_count.unwrap_or(0) as usize;
                let envelope = serde_json::from_str::<AesGcmEnvelope>(&finished.encrypted)?;
                let plaintext = decrypt_aes_gcm(key, &envelope)?;
                return Ok((
                    finished
                        .delivery_kind
                        .expect("complete delivery declares a kind"),
                    plaintext,
                    LocalWebrtcResponseMetrics {
                        envelope_bytes,
                        chunk_count,
                        maximum_frame_bytes: finished.maximum_frame_bytes,
                    },
                ));
            }
        }
    }
}

struct PendingHostEventState {
    events: VecDeque<botster_hub_client::DaemonEvent>,
    sizes: VecDeque<u64>,
    enqueued_ns: VecDeque<u64>,
    bytes: u64,
    overflow: u64,
    high_water_count: u64,
    high_water_bytes: u64,
}

impl PendingHostEventState {
    fn new() -> Self {
        Self {
            events: VecDeque::new(),
            sizes: VecDeque::new(),
            enqueued_ns: VecDeque::new(),
            bytes: 0,
            overflow: 0,
            high_water_count: 0,
            high_water_bytes: 0,
        }
    }

    fn oldest_age_us(&self) -> Option<u64> {
        self.enqueued_ns
            .front()
            .copied()
            .map(|oldest_ns| monotonic_now_ns().saturating_sub(oldest_ns) / 1_000)
    }

    fn try_park(&mut self, plaintext: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let bytes = plaintext.len() as u64;
        if self.events.len() >= WEBRTC_PENDING_HOST_EVENTS_MAX {
            self.overflow += 1;
            return Err(PendingHostEventAdmitError::CountLimit.into());
        }
        if self.bytes + bytes > WEBRTC_PENDING_HOST_EVENTS_MAX_BYTES as u64 {
            self.overflow += 1;
            return Err(PendingHostEventAdmitError::ByteLimit.into());
        }
        let event = serde_json::from_slice::<botster_hub_client::DaemonEvent>(plaintext)?;
        self.events.push_back(event);
        self.sizes.push_back(bytes);
        self.enqueued_ns.push_back(monotonic_now_ns());
        self.bytes += bytes;
        self.high_water_count = self.high_water_count.max(self.events.len() as u64);
        self.high_water_bytes = self.high_water_bytes.max(self.bytes);
        Ok(())
    }

    fn pop_front(&mut self) -> Option<botster_hub_client::DaemonEvent> {
        let event = self.events.pop_front()?;
        let bytes = self.sizes.pop_front().unwrap_or(0);
        let _ = self.enqueued_ns.pop_front();
        self.bytes = self.bytes.saturating_sub(bytes);
        Some(event)
    }

    fn take_at(&mut self, index: usize) -> Option<botster_hub_client::DaemonEvent> {
        if index >= self.events.len() {
            return None;
        }
        let event = self.events.remove(index)?;
        let bytes = self.sizes.remove(index).unwrap_or(0);
        let _ = self.enqueued_ns.remove(index);
        self.bytes = self.bytes.saturating_sub(bytes);
        Some(event)
    }
}

fn apply_inbound_chunk(
    assembly: &mut Option<InboundReassembly>,
    response: &str,
) -> Result<Option<InboundReassembly>, Box<dyn std::error::Error>> {
    assert!(
        response.len() < botster_hub_client::LOCAL_WEBRTC_MAX_FRAME_BYTES,
        "response frame exceeded 64 KiB"
    );
    let chunk =
        serde_json::from_str::<botster_hub_client::DaemonLocalWebrtcDeliveryChunk>(response)?;
    assert_eq!(
        chunk.version,
        botster_hub_client::LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION
    );
    let complete = chunk.chunk_index + 1 == chunk.chunk_count;
    {
        let row = assembly.get_or_insert_with(|| InboundReassembly {
            encrypted: String::new(),
            delivery_kind: None,
            message_id: None,
            expected_chunk_count: None,
            maximum_frame_bytes: 0,
            next_chunk_index: 0,
        });
        row.maximum_frame_bytes = row.maximum_frame_bytes.max(response.len());
        if let Some(delivery_kind) = row.delivery_kind {
            assert_eq!(delivery_kind, chunk.delivery_kind);
        } else {
            row.delivery_kind = Some(chunk.delivery_kind);
        }
        assert_eq!(chunk.chunk_index, row.next_chunk_index);
        if let Some(message_id) = &row.message_id {
            assert_eq!(message_id, &chunk.message_id);
        } else {
            row.message_id = Some(chunk.message_id.clone());
            row.expected_chunk_count = Some(chunk.chunk_count);
        }
        assert_eq!(row.expected_chunk_count, Some(chunk.chunk_count));
        row.encrypted.push_str(&chunk.payload);
        row.next_chunk_index += 1;
        if complete {
            assert_eq!(row.encrypted.len(), chunk.total_bytes as usize);
        }
    }
    if complete {
        Ok(assembly.take())
    } else {
        Ok(None)
    }
}

pub(crate) struct LocalWebrtcOfferPeer {
    pub(crate) peer: Box<dyn PeerConnection>,
    pub(crate) data_channel: Arc<dyn DataChannel>,
    pub(crate) connected_rx: AsyncReceiver<()>,
    pub(crate) data_channel_open_rx: AsyncReceiver<()>,
    pub(crate) pending_entity_frames: VecDeque<botster_hub_client::DaemonEntityFrame>,
    pub(crate) pending_terminal_frames: VecDeque<(String, Vec<u8>)>,
    pending_host: PendingHostEventState,
    pub(crate) accept_host_events: bool,
    inbound: WebrtcInboundMailbox,
}

pub(crate) struct ExtraWebrtcDataChannel {
    pub(crate) messages: AsyncReceiver<String>,
    pub(crate) closed: AsyncReceiver<()>,
}

impl LocalWebrtcOfferPeer {
    pub(crate) async fn create_offer()
    -> Result<(Self, serde_json::Value), Box<dyn std::error::Error>> {
        let (peer, extra, offer) = Self::create_offer_inner(false).await?;
        assert!(extra.is_none());
        Ok((peer, offer))
    }

    pub(crate) async fn create_offer_with_extra_data_channel()
    -> Result<(Self, ExtraWebrtcDataChannel, serde_json::Value), Box<dyn std::error::Error>> {
        let (peer, extra, offer) = Self::create_offer_inner(true).await?;
        Ok((
            peer,
            extra.expect("extra DataChannel requested in the initial offer"),
            offer,
        ))
    }

    async fn create_offer_inner(
        with_extra: bool,
    ) -> Result<(Self, Option<ExtraWebrtcDataChannel>, serde_json::Value), Box<dyn std::error::Error>>
    {
        let runtime =
            default_runtime().ok_or_else(|| std::io::Error::other("no async runtime found"))?;
        let (gather_complete_tx, mut gather_complete_rx) = channel::<()>(1);
        let (connected_tx, connected_rx) = channel::<()>(1);
        let (data_channel_open_tx, data_channel_open_rx) = channel::<()>(1);
        let (data_channel_message_tx, inbound) =
            WebrtcInboundMailbox::bounded(WEBRTC_INBOUND_MAX_FRAMES, WEBRTC_INBOUND_MAX_BYTES);
        let occupancy = Arc::clone(&inbound.occupancy);
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
            let occupancy = Arc::clone(&occupancy);
            runtime.spawn(Box::pin(async move {
                while let Some(event) = data_channel.poll().await {
                    match event {
                        DataChannelEvent::OnOpen => {
                            let _ = open_tx.try_send(());
                        }
                        DataChannelEvent::OnMessage(message) => {
                            if let Ok(text) = String::from_utf8(message.data.to_vec()) {
                                let _ = admit_inbound_frame(&occupancy, &message_tx, text);
                            }
                        }
                        DataChannelEvent::OnClose => break,
                        _ => {}
                    }
                }
            }));
        }

        let extra_channel = if with_extra {
            let (open_tx, mut open_rx) = channel::<()>(1);
            let (message_tx, message_rx) = channel::<String>(256);
            let (closed_tx, closed_rx) = channel::<()>(1);
            let extra = peer
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
                            DataChannelEvent::OnClose => {
                                let _ = closed_tx.try_send(());
                                break;
                            }
                            _ => {}
                        }
                    }
                }));
            }
            let _ = timeout(Duration::from_secs(1), open_rx.recv()).await;
            Some(ExtraWebrtcDataChannel {
                messages: message_rx,
                closed: closed_rx,
            })
        } else {
            None
        };

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
                pending_entity_frames: VecDeque::new(),
                pending_terminal_frames: VecDeque::new(),
                pending_host: PendingHostEventState::new(),
                accept_host_events: false,
                inbound,
            },
            extra_channel,
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

    #[must_use]
    pub(crate) fn inbound_overflow(&self) -> u64 {
        self.inbound.occupancy.overflow.load(Ordering::Relaxed)
    }

    #[must_use]
    pub(crate) fn pending_host_events(&self) -> &VecDeque<botster_hub_client::DaemonEvent> {
        &self.pending_host.events
    }

    pub(crate) fn take_pending_host_event_at(
        &mut self,
        index: usize,
    ) -> Option<botster_hub_client::DaemonEvent> {
        self.pending_host.take_at(index)
    }

    #[must_use]
    pub(crate) fn fixture_queue_snapshot(&self) -> serde_json::Value {
        let inbound = self.inbound.occupancy.snapshot();
        serde_json::json!({
            "inbound_frames": {
                "count": inbound.count,
                "bytes": inbound.bytes,
                "high_water_count": inbound.high_water_count,
                "high_water_bytes": inbound.high_water_bytes,
                "oldest_age_us": inbound.oldest_age_us,
                "overflow": inbound.overflow,
                "max_count": inbound.max_count,
                "max_bytes": inbound.max_bytes
            },
            "pending_host_events": {
                "count": self.pending_host.events.len() as u64,
                "bytes": self.pending_host.bytes,
                "high_water_count": self.pending_host.high_water_count,
                "high_water_bytes": self.pending_host.high_water_bytes,
                "oldest_age_us": self.pending_host.oldest_age_us(),
                "overflow": self.pending_host.overflow,
                "max_count": WEBRTC_PENDING_HOST_EVENTS_MAX as u64,
                "max_bytes": WEBRTC_PENDING_HOST_EVENTS_MAX_BYTES as u64
            }
        })
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
        self.pending_host.try_park(plaintext)
    }

    pub(crate) async fn next_host_event(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<botster_hub_client::DaemonEvent, Box<dyn std::error::Error>> {
        if let Some(event) = self.pending_host.pop_front() {
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
        self.inbound.receive_delivery(key).await
    }

    pub(crate) async fn create_extra_data_channel(
        &mut self,
    ) -> Result<ExtraWebrtcDataChannel, Box<dyn std::error::Error>> {
        let runtime =
            default_runtime().ok_or_else(|| std::io::Error::other("no async runtime found"))?;
        let (open_tx, mut open_rx) = channel::<()>(1);
        let (message_tx, message_rx) = channel::<String>(256);
        let (closed_tx, closed_rx) = channel::<()>(1);
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
                        DataChannelEvent::OnClose => {
                            let _ = closed_tx.try_send(());
                            break;
                        }
                        _ => {}
                    }
                }
            }));
        }
        let _ = timeout(Duration::from_secs(5), open_rx.recv()).await;
        Ok(ExtraWebrtcDataChannel {
            messages: message_rx,
            closed: closed_rx,
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
    let (peer, extra, key) = open_local_webrtc_peer_inner(endpoint, bootstrap, false).await;
    assert!(extra.is_none());
    (peer, key)
}

pub(crate) async fn open_local_webrtc_peer_with_extra_channel(
    endpoint: &botster_hub_client::DaemonEndpoint,
    bootstrap: &botster_hub_client::DaemonLocalWebrtcBootstrap,
) -> (LocalWebrtcOfferPeer, ExtraWebrtcDataChannel, AesGcmKey) {
    let (peer, extra, key) = open_local_webrtc_peer_inner(endpoint, bootstrap, true).await;
    (
        peer,
        extra.expect("extra DataChannel requested in the initial offer"),
        key,
    )
}

async fn open_local_webrtc_peer_inner(
    endpoint: &botster_hub_client::DaemonEndpoint,
    bootstrap: &botster_hub_client::DaemonLocalWebrtcBootstrap,
    with_extra: bool,
) -> (
    LocalWebrtcOfferPeer,
    Option<ExtraWebrtcDataChannel>,
    AesGcmKey,
) {
    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);
    let (mut offer_peer, extra, offer) = if with_extra {
        let (peer, extra, offer) = LocalWebrtcOfferPeer::create_offer_with_extra_data_channel()
            .await
            .expect("create WebRTC offer peer with extra DataChannel");
        (peer, Some(extra), offer)
    } else {
        let (peer, offer) = LocalWebrtcOfferPeer::create_offer()
            .await
            .expect("create WebRTC offer peer");
        (peer, None, offer)
    };
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
    (offer_peer, extra, stream_key)
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

pub(crate) fn typed_operator_error_body(response: &botster_hub_client::DaemonResponse) -> String {
    match &response.error {
        Some(error) => format!(
            "kind={:?} code={} operation={} message={}",
            response.kind, error.code, error.operation, error.message
        ),
        None => format!("kind={:?} error=None", response.kind),
    }
}

pub(crate) fn assert_daemon_response_ok(
    response: &botster_hub_client::DaemonResponse,
    expected: botster_hub_client::DaemonResponseKind,
    context: &str,
) {
    assert_eq!(
        response.kind,
        expected,
        "{context}: {}",
        typed_operator_error_body(response)
    );
}

pub(crate) fn wait_for_published_web_origin(
    endpoint: &botster_hub_client::DaemonEndpoint,
) -> String {
    let deadline = Instant::now() + BOTSTER_WEB_READINESS_LIVENESS_BACKSTOP;
    let mut last = "ListApps not attempted".to_string();
    while Instant::now() < deadline {
        match botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListApps) {
            Ok(response) => {
                last = typed_operator_error_body(&response);
                if let Some(url) = response.apps.iter().find_map(|app| {
                    (app.package_name == "botster-web" && app.entrypoint_id == "web-client")
                        .then(|| app.launch_target.local_url.clone())
                        .flatten()
                }) {
                    return url.trim_end_matches('/').to_string();
                }
            }
            Err(error) => last = format!("ListApps error: {error}"),
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("botster-web did not publish local_url after bind: {last}");
}

pub(crate) fn start_botster_web_and_issue_bootstrap(
    endpoint: &botster_hub_client::DaemonEndpoint,
) -> (String, botster_hub_client::DaemonLocalWebrtcBootstrap) {
    let start = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_PORT".to_string(),
                "0".to_string(),
            )]),
        },
    )
    .expect("start botster-web entrypoint");
    assert_daemon_response_ok(
        &start,
        botster_hub_client::DaemonResponseKind::Packages,
        "start botster-web entrypoint",
    );
    let origin = wait_for_published_web_origin(endpoint);
    let expected_local_url = format!("{origin}/");
    wait_for_botster_web_readiness(endpoint, &origin, &expected_local_url, Instant::now());
    let bootstrap = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            origin: origin.clone(),
        },
    )
    .unwrap_or_else(|error| panic!("issue local WebRTC bootstrap: {error}"));
    assert_daemon_response_ok(
        &bootstrap,
        botster_hub_client::DaemonResponseKind::LocalWebrtcBootstrap,
        "issue local WebRTC bootstrap",
    );
    let grant = match bootstrap.local_webrtc_bootstrap {
        Some(grant) => grant,
        None => panic!(
            "bootstrap response includes local WebRTC bootstrap: {}",
            typed_operator_error_body(&bootstrap)
        ),
    };
    (origin, grant)
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

fn test_chunk(index: u32, count: u32, payload: &str, total_bytes: usize) -> String {
    serde_json::json!({
        "version": botster_hub_client::LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
        "delivery_kind": "daemon_response",
        "message_id": "reassembly-test",
        "chunk_index": index,
        "chunk_count": count,
        "total_bytes": total_bytes,
        "payload": payload
    })
    .to_string()
}

fn session_lifecycle_event(session_id: &str) -> Vec<u8> {
    serde_json::to_vec(&botster_hub_client::DaemonEvent::SessionLifecycle {
        session_id: session_id.to_string(),
        state: "running".to_string(),
    })
    .expect("session lifecycle event")
}

#[test]
fn inbound_chunk_reassembly_survives_cancelled_read() {
    let _runtime = default_runtime().expect("async runtime");
    let key = AesGcmKey::from_slice(&[9; 32]).expect("test AES key");
    let envelope = encrypt_aes_gcm(&key, b"delivery-ok", 1).expect("encrypt delivery");
    let encrypted = serde_json::to_string(&envelope).expect("envelope json");
    let mid = encrypted.len() / 2;
    let first_chunk = test_chunk(0, 2, &encrypted[..mid], encrypted.len());
    let second_chunk = test_chunk(1, 2, &encrypted[mid..], encrypted.len());

    let (tx, mut inbound) = WebrtcInboundMailbox::bounded(8, 64 * 1024);
    admit_inbound_frame(&inbound.occupancy, &tx, first_chunk).expect("admit first chunk");

    let cancelled = block_on(async {
        timeout(Duration::from_millis(50), inbound.receive_delivery(&key)).await
    });
    assert!(
        cancelled.is_err(),
        "first chunk must leave receive_delivery waiting for the remainder"
    );
    assert!(
        inbound.reassembly.is_some(),
        "cancelled receive_delivery must keep reassembly state"
    );

    admit_inbound_frame(&inbound.occupancy, &tx, second_chunk).expect("admit second chunk");
    let (kind, plaintext, metrics) =
        block_on(inbound.receive_delivery(&key)).expect("resume delivery");
    assert_eq!(
        kind,
        botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonResponse
    );
    assert_eq!(plaintext, b"delivery-ok");
    assert_eq!(metrics.chunk_count, 2);
    assert!(inbound.reassembly.is_none());
    let snap = inbound.occupancy.snapshot();
    assert_eq!(snap.count, 0);
    assert_eq!(snap.bytes, 0);
    assert_eq!(snap.overflow, 0);
}

#[test]
fn inbound_occupancy_overflows_at_explicit_count_and_byte_limits() {
    let _runtime = default_runtime().expect("async runtime");
    let (tx, inbound) = WebrtcInboundMailbox::bounded(1, 8);
    admit_inbound_frame(&inbound.occupancy, &tx, "abcd".to_string()).expect("first frame");
    let count_err = admit_inbound_frame(&inbound.occupancy, &tx, "efgh".to_string())
        .expect_err("count overflow");
    assert_eq!(count_err, InboundAdmitError::CountLimit);
    let count_snap = inbound.occupancy.snapshot();
    assert_eq!(count_snap.count, 1);
    assert_eq!(count_snap.bytes, 4);
    assert_eq!(count_snap.overflow, 1);
    assert_eq!(count_snap.max_count, 1);
    assert_eq!(count_snap.max_bytes, 8);

    let (tx, inbound) = WebrtcInboundMailbox::bounded(8, 8);
    let byte_err = admit_inbound_frame(&inbound.occupancy, &tx, "ninebytes".to_string())
        .expect_err("byte overflow");
    assert_eq!(byte_err, InboundAdmitError::ByteLimit);
    let byte_snap = inbound.occupancy.snapshot();
    assert_eq!(byte_snap.count, 0);
    assert_eq!(byte_snap.bytes, 0);
    assert_eq!(byte_snap.overflow, 1);
}

#[test]
fn inbound_occupancy_reserves_before_send_so_consumer_cannot_underflow() {
    let _runtime = default_runtime().expect("async runtime");
    let occupancy = FixtureQueueOccupancy::new(8, 1024);
    admit_inbound_frame_with_send(&occupancy, 4, || {
        occupancy.record_pop(4);
        Ok(())
    })
    .expect("reentrant consumer after reserve");
    let snap = occupancy.snapshot();
    assert_eq!(
        snap.count, 0,
        "consumer pop after reserve must land at zero"
    );
    assert_eq!(snap.bytes, 0);
    assert!(snap.oldest_age_us.is_none());
    assert_eq!(snap.overflow, 0);

    let occupancy = FixtureQueueOccupancy::new(8, 1024);
    let (narrow_tx, _narrow_rx) = channel::<String>(1);
    admit_inbound_frame(&occupancy, &narrow_tx, "full".to_string()).expect("fill channel");
    let full_err =
        admit_inbound_frame(&occupancy, &narrow_tx, "drop".to_string()).expect_err("channel full");
    assert_eq!(full_err, InboundAdmitError::ChannelFull);
    let snap = occupancy.snapshot();
    assert_eq!(snap.count, 1);
    assert_eq!(snap.bytes, 4);
    assert_eq!(snap.overflow, 1);
}

#[test]
fn pending_host_events_reject_count_and_byte_limits_and_publish_age() {
    let mut pending = PendingHostEventState::new();
    let event = session_lifecycle_event("s0");
    pending.try_park(&event).expect("park first");
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.bytes, event.len() as u64);
    assert!(pending.oldest_age_us().is_some());

    for index in 1..WEBRTC_PENDING_HOST_EVENTS_MAX {
        pending
            .try_park(&session_lifecycle_event(&format!("s{index}")))
            .expect("park up to count bound");
    }
    let count_err = pending
        .try_park(&session_lifecycle_event("overflow"))
        .expect_err("count overflow");
    assert_eq!(
        count_err.downcast_ref::<PendingHostEventAdmitError>(),
        Some(&PendingHostEventAdmitError::CountLimit)
    );
    assert_eq!(pending.events.len(), WEBRTC_PENDING_HOST_EVENTS_MAX);
    assert_eq!(pending.overflow, 1);

    let mut pending = PendingHostEventState::new();
    let oversized = vec![b'x'; WEBRTC_PENDING_HOST_EVENTS_MAX_BYTES + 1];
    let byte_err = pending.try_park(&oversized).expect_err("byte overflow");
    assert_eq!(
        byte_err.downcast_ref::<PendingHostEventAdmitError>(),
        Some(&PendingHostEventAdmitError::ByteLimit)
    );
    assert!(pending.events.is_empty());
    assert_eq!(pending.bytes, 0);
    assert_eq!(pending.overflow, 1);
    assert!(pending.oldest_age_us().is_none());
}
