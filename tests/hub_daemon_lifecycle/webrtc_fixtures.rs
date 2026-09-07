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
    Receiver as AsyncReceiver, Runtime, Sender as AsyncSender, channel, default_runtime, timeout,
};

fn webrtc_runtime() -> Arc<dyn Runtime> {
    default_runtime().expect("webrtc default runtime")
}

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for WebRTC fixture block_on")
        .block_on(fut)
}

use crate::support::{
    candidate_session_worker_binary_path, recovering_mutex_guard, validate_cli_daemon_shutdown,
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

/// One DataChannel message as the fixture received it.
#[derive(Debug, Clone)]
pub(crate) enum InboundMessage {
    /// A control delivery chunk (JSON text).
    Text(String),
    /// A sealed terminal chunk on a reserved terminal channel.
    Binary(Vec<u8>),
}

impl InboundMessage {
    fn len(&self) -> usize {
        match self {
            Self::Text(text) => text.len(),
            Self::Binary(bytes) => bytes.len(),
        }
    }
}

/// One complete delivery decoded by the fixture.
#[derive(Debug)]
pub(crate) enum FixtureDelivery {
    /// A control-plane server frame.
    Server(botster_hub_client::ServerFrame),
    /// One plaintext terminal frame body from a reserved terminal channel.
    Terminal {
        generation: u64,
        stream_epoch: u32,
        body: Vec<u8>,
    },
}

struct InboundReassembly {
    encrypted: String,
    delivery_kind: Option<botster_hub_client::DaemonLocalWebrtcDeliveryKind>,
    message_id: Option<String>,
    expected_chunk_count: Option<u32>,
    maximum_frame_bytes: usize,
    next_chunk_index: u32,
}

/// Client-side reassembly of the binary terminal chunk stream on one channel.
struct TerminalReassembly {
    message_id: u64,
    chunk_count: u32,
    total_bytes: usize,
    next_chunk_index: u32,
    plaintext: Vec<u8>,
    generation: u64,
    stream_epoch: u32,
    maximum_frame_bytes: usize,
}

/// Seal one input frame body into binary terminal chunks for a reserved channel.
fn seal_terminal_chunks(
    key: &AesGcmKey,
    generation: u64,
    body: &[u8],
    message_id: u64,
) -> Vec<Vec<u8>> {
    const CHUNK_BYTES: usize = 12 * 1024;
    assert!(!body.is_empty(), "terminal body must not be empty");
    let total_bytes = u32::try_from(body.len()).expect("terminal body fits u32");
    let chunk_count = u32::try_from(body.len().div_ceil(CHUNK_BYTES)).expect("chunk count");
    body.chunks(CHUNK_BYTES)
        .enumerate()
        .map(|(chunk_index, slice)| {
            let header = botster_hub_client::LocalWebrtcTerminalChunkHeader {
                message_id,
                chunk_index: chunk_index as u32,
                chunk_count,
                total_bytes,
                generation,
                stream_epoch: 0,
            };
            let mut message = Vec::with_capacity(
                botster_hub_client::LOCAL_WEBRTC_TERMINAL_CHUNK_HEADER_BYTES
                    + slice.len()
                    + botster_core::AES_GCM_SEALED_OVERHEAD_BYTES,
            );
            message.extend_from_slice(&header.encode());
            botster_core::seal_aes_gcm(key, slice, &mut message).expect("seal terminal chunk");
            message
        })
        .collect()
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
    tx: &AsyncSender<InboundMessage>,
    message: InboundMessage,
) -> Result<(), InboundAdmitError> {
    let add_bytes = message.len() as u64;
    admit_inbound_frame_with_send(occupancy, add_bytes, || {
        tx.try_send(message)
            .map_err(|_| InboundAdmitError::ChannelFull)
    })
}

struct WebrtcInboundMailbox {
    rx: AsyncReceiver<InboundMessage>,
    occupancy: Arc<FixtureQueueOccupancy>,
    reassembly: Option<InboundReassembly>,
    terminal: Option<TerminalReassembly>,
    last_terminal_message_id: Option<u64>,
}

impl WebrtcInboundMailbox {
    fn bounded(max_count: usize, max_bytes: usize) -> (AsyncSender<InboundMessage>, Self) {
        let (tx, rx) = channel::<InboundMessage>(max_count);
        (
            tx,
            Self {
                rx,
                occupancy: Arc::new(FixtureQueueOccupancy::new(max_count, max_bytes)),
                reassembly: None,
                terminal: None,
                last_terminal_message_id: None,
            },
        )
    }

    /// Apply one sealed terminal chunk; a complete message returns its plaintext.
    fn apply_terminal_chunk(
        &mut self,
        key: &AesGcmKey,
        message: &[u8],
    ) -> Result<Option<TerminalReassembly>, Box<dyn std::error::Error>> {
        let (header, sealed) = botster_hub_client::LocalWebrtcTerminalChunkHeader::decode(message)
            .ok_or_else(|| std::io::Error::other("terminal chunk header failed to decode"))?;
        if let Some(last) = self.last_terminal_message_id
            && header.message_id <= last
        {
            return Err(std::io::Error::other("terminal message id did not advance").into());
        }
        let slice = botster_core::open_aes_gcm(key, sealed)?;
        let row = match self.terminal.as_mut() {
            None => {
                if header.chunk_index != 0 {
                    return Err(std::io::Error::other("terminal message began mid-stream").into());
                }
                self.terminal.insert(TerminalReassembly {
                    message_id: header.message_id,
                    chunk_count: header.chunk_count,
                    total_bytes: header.total_bytes as usize,
                    next_chunk_index: 0,
                    plaintext: Vec::with_capacity(header.total_bytes as usize),
                    generation: header.generation,
                    stream_epoch: header.stream_epoch,
                    maximum_frame_bytes: 0,
                })
            }
            Some(row) => {
                if row.message_id != header.message_id
                    || row.chunk_count != header.chunk_count
                    || row.total_bytes != header.total_bytes as usize
                    || row.next_chunk_index != header.chunk_index
                {
                    return Err(std::io::Error::other("terminal chunk out of order").into());
                }
                row
            }
        };
        row.maximum_frame_bytes = row.maximum_frame_bytes.max(message.len());
        row.plaintext.extend_from_slice(&slice);
        row.next_chunk_index += 1;
        if row.next_chunk_index == row.chunk_count {
            let finished = self.terminal.take().expect("assembling row");
            if finished.plaintext.len() != finished.total_bytes {
                return Err(std::io::Error::other("terminal message length mismatch").into());
            }
            self.last_terminal_message_id = Some(finished.message_id);
            return Ok(Some(finished));
        }
        Ok(None)
    }

    async fn recv_raw(&mut self, bound: Duration) -> Option<InboundMessage> {
        match timeout(webrtc_runtime().as_ref(), bound, self.rx.recv()).await {
            Ok(Some(message)) => {
                self.occupancy.record_pop(message.len() as u64);
                Some(message)
            }
            _ => None,
        }
    }

    async fn receive_delivery(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<(FixtureDelivery, LocalWebrtcResponseMetrics), Box<dyn std::error::Error>> {
        loop {
            let message = match timeout(
                webrtc_runtime().as_ref(),
                Duration::from_secs(10),
                self.rx.recv(),
            )
            .await
            {
                Ok(Some(message)) => {
                    self.occupancy.record_pop(message.len() as u64);
                    message
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
                        "timeout",
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
            match message {
                InboundMessage::Text(response) => {
                    if let Some(finished) = apply_inbound_chunk(&mut self.reassembly, &response)? {
                        let envelope_bytes = finished.encrypted.len();
                        let chunk_count = finished.expected_chunk_count.unwrap_or(0) as usize;
                        let envelope = serde_json::from_str::<AesGcmEnvelope>(&finished.encrypted)?;
                        let plaintext = decrypt_aes_gcm(key, &envelope)?;
                        let frame: botster_hub_client::ServerFrame =
                            serde_json::from_slice(&plaintext)?;
                        return Ok((
                            FixtureDelivery::Server(frame),
                            LocalWebrtcResponseMetrics {
                                envelope_bytes,
                                chunk_count,
                                maximum_frame_bytes: finished.maximum_frame_bytes,
                            },
                        ));
                    }
                }
                InboundMessage::Binary(bytes) => {
                    if let Some(finished) = self.apply_terminal_chunk(key, &bytes)? {
                        return Ok((
                            FixtureDelivery::Terminal {
                                generation: finished.generation,
                                stream_epoch: finished.stream_epoch,
                                body: finished.plaintext,
                            },
                            LocalWebrtcResponseMetrics {
                                envelope_bytes: finished.total_bytes,
                                chunk_count: finished.chunk_count as usize,
                                maximum_frame_bytes: finished.maximum_frame_bytes,
                            },
                        ));
                    }
                }
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
    pub(crate) control_label: String,
    pub(crate) connected_rx: AsyncReceiver<()>,
    pub(crate) data_channel_open_rx: AsyncReceiver<()>,
    pub(crate) pending_entity_frames: VecDeque<botster_hub_client::DaemonEntityFrame>,
    pub(crate) pending_terminal_frames: VecDeque<(String, Vec<u8>)>,
    pending_host: PendingHostEventState,
    pub(crate) accept_host_events: bool,
    inbound: WebrtcInboundMailbox,
    subscription_channels: Vec<Arc<dyn DataChannel>>,
    subscription_inbounds: Vec<WebrtcInboundMailbox>,
    subscription_labels: Vec<String>,
    pub(crate) subscription_receive_errors: Vec<(String, String)>,
    pub(crate) control_terminal_frame_count: u64,
    request_ids: botster_hub_client::RequestIdSequence,
    /// Attachment generation observed per reserved channel label.
    route_generations: BTreeMap<String, u64>,
    route_operation_ids: RouteOperationIds,
    next_terminal_message_id: u64,
    /// Label of the reserved channel that produced the last terminal delivery.
    last_terminal_label: String,
}

pub(crate) struct ExtraWebrtcDataChannel {
    pub(crate) label: String,
    pub(crate) data_channel: Arc<dyn DataChannel>,
    inbound: WebrtcInboundMailbox,
    pub(crate) messages: AsyncReceiver<InboundMessage>,
    pub(crate) closed: AsyncReceiver<()>,
    open_rx: AsyncReceiver<()>,
}

impl ExtraWebrtcDataChannel {
    pub(crate) async fn count_terminal_frames(&mut self, bound: Duration) -> usize {
        let deadline = Instant::now() + bound;
        let mut extra_terminal_frames = 0;
        while Instant::now() < deadline {
            let message = match timeout(
                webrtc_runtime().as_ref(),
                Duration::from_millis(50),
                self.messages.recv(),
            )
            .await
            {
                Ok(Some(message)) => Some(message),
                _ => self.inbound.recv_raw(Duration::from_millis(50)).await,
            };
            // Terminal data only ever travels as binary chunks.
            if let Some(InboundMessage::Binary(_)) = message {
                extra_terminal_frames += 1;
            }
        }
        extra_terminal_frames
    }
}

fn spawn_offerer_channel_poll(
    runtime: std::sync::Arc<dyn webrtc::runtime::Runtime>,
    data_channel: Arc<dyn DataChannel>,
    open_tx: AsyncSender<()>,
    inbound_tx: AsyncSender<InboundMessage>,
    occupancy: Arc<FixtureQueueOccupancy>,
    messages_tx: Option<AsyncSender<InboundMessage>>,
    closed_tx: Option<AsyncSender<()>>,
) {
    runtime.spawn(Box::pin(async move {
        while let Some(event) = data_channel.poll().await {
            match event {
                DataChannelEvent::OnOpen => {
                    let _ = open_tx.try_send(());
                }
                DataChannelEvent::OnMessage(message) => {
                    let inbound = if message.is_string {
                        match String::from_utf8(message.data.to_vec()) {
                            Ok(text) => InboundMessage::Text(text),
                            Err(_) => continue,
                        }
                    } else {
                        InboundMessage::Binary(message.data.to_vec())
                    };
                    if let Some(messages_tx) = &messages_tx {
                        let _ = messages_tx.try_send(inbound.clone());
                    }
                    let _ = admit_inbound_frame(&occupancy, &inbound_tx, inbound);
                }
                DataChannelEvent::OnClose => {
                    if let Some(closed_tx) = &closed_tx {
                        let _ = closed_tx.try_send(());
                    }
                    break;
                }
                _ => {}
            }
        }
    }));
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

        spawn_offerer_channel_poll(
            runtime.clone(),
            data_channel.clone(),
            data_channel_open_tx.clone(),
            data_channel_message_tx,
            Arc::clone(&occupancy),
            None,
            None,
        );

        let extra_channel = if with_extra {
            let (open_tx, open_rx) = channel::<()>(1);
            let (message_tx, message_rx) = channel::<InboundMessage>(256);
            let (closed_tx, closed_rx) = channel::<()>(1);
            let (inbound_tx, inbound) =
                WebrtcInboundMailbox::bounded(WEBRTC_INBOUND_MAX_FRAMES, WEBRTC_INBOUND_MAX_BYTES);
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
            spawn_offerer_channel_poll(
                runtime.clone(),
                extra.clone(),
                open_tx,
                inbound_tx,
                Arc::clone(&inbound.occupancy),
                Some(message_tx),
                Some(closed_tx),
            );
            Some(ExtraWebrtcDataChannel {
                label: "botster-extra".to_string(),
                data_channel: extra,
                inbound,
                messages: message_rx,
                closed: closed_rx,
                open_rx,
            })
        } else {
            None
        };

        let offer = peer.create_offer(None).await?;
        peer.set_local_description(offer).await?;
        let _ = timeout(
            runtime.as_ref(),
            Duration::from_secs(5),
            gather_complete_rx.recv(),
        )
        .await;
        let offer = peer
            .local_description()
            .await
            .ok_or_else(|| std::io::Error::other("offer local description missing"))?;
        let offer = serde_json::to_value(offer)?;

        Ok((
            Self {
                peer: Box::new(peer),
                data_channel,
                control_label: "botster-client".to_string(),
                connected_rx,
                data_channel_open_rx,
                pending_entity_frames: VecDeque::new(),
                pending_terminal_frames: VecDeque::new(),
                pending_host: PendingHostEventState::new(),
                accept_host_events: false,
                inbound,
                subscription_channels: Vec::new(),
                subscription_inbounds: Vec::new(),
                subscription_labels: Vec::new(),
                subscription_receive_errors: Vec::new(),
                control_terminal_frame_count: 0,
                request_ids: botster_hub_client::RequestIdSequence::new(),
                route_generations: BTreeMap::new(),
                route_operation_ids: RouteOperationIds::default(),
                next_terminal_message_id: 1,
                last_terminal_label: String::new(),
            },
            extra_channel,
            offer,
        ))
    }

    pub(crate) async fn accept_answer(
        &mut self,
        answer: serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.accept_answer_inner(answer, None).await
    }

    pub(crate) async fn accept_answer_with_extra_open(
        &mut self,
        answer: serde_json::Value,
        extra: &mut ExtraWebrtcDataChannel,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.accept_answer_inner(answer, Some(&mut extra.open_rx))
            .await
    }

    async fn accept_answer_inner(
        &mut self,
        answer: serde_json::Value,
        extra_open_rx: Option<&mut AsyncReceiver<()>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let answer = serde_json::from_value::<RTCSessionDescription>(answer)?;
        self.peer.set_remote_description(answer).await?;
        timeout(
            webrtc_runtime().as_ref(),
            Duration::from_secs(15),
            self.connected_rx.recv(),
        )
        .await
        .map_err(|_| std::io::Error::other("timed out waiting for WebRTC connection"))?;
        if let Some(extra_open_rx) = extra_open_rx {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut opened = false;
            while Instant::now() < deadline && !opened {
                opened = timeout(
                    webrtc_runtime().as_ref(),
                    Duration::from_millis(50),
                    self.data_channel_open_rx.recv(),
                )
                .await
                .is_ok()
                    || timeout(
                        webrtc_runtime().as_ref(),
                        Duration::from_millis(50),
                        extra_open_rx.recv(),
                    )
                    .await
                    .is_ok();
            }
            if !opened {
                return Err(std::io::Error::other(
                    "timed out waiting for either offerer DataChannel to open",
                )
                .into());
            }
        } else {
            timeout(
                webrtc_runtime().as_ref(),
                Duration::from_secs(10),
                self.data_channel_open_rx.recv(),
            )
            .await
            .map_err(|_| std::io::Error::other("timed out waiting for data channel open"))?;
        }
        Ok(())
    }

    pub(crate) fn swap_offerer_channel(
        &mut self,
        incoming: ExtraWebrtcDataChannel,
    ) -> ExtraWebrtcDataChannel {
        let (_unused_messages_tx, unused_messages) = channel::<InboundMessage>(1);
        let (_unused_closed_tx, unused_closed) = channel::<()>(1);
        let (_unused_open_tx, unused_open) = channel::<()>(1);
        let rejected = ExtraWebrtcDataChannel {
            label: self.control_label.clone(),
            data_channel: std::mem::replace(&mut self.data_channel, incoming.data_channel),
            inbound: std::mem::replace(&mut self.inbound, incoming.inbound),
            messages: unused_messages,
            closed: unused_closed,
            open_rx: unused_open,
        };
        self.control_label = incoming.label;
        rejected
    }

    pub(crate) async fn admit_surviving_dual_channel(
        &mut self,
        extra: ExtraWebrtcDataChannel,
        key: &AesGcmKey,
        hello: &botster_hub_client::DaemonHello,
    ) -> Result<ExtraWebrtcDataChannel, Box<dyn std::error::Error>> {
        match timeout(
            webrtc_runtime().as_ref(),
            Duration::from_secs(3),
            self.encrypted_hello(key, hello),
        )
        .await
        {
            Ok(Ok(_)) => Ok(extra),
            Ok(Err(_)) | Err(_) => {
                let rejected = self.swap_offerer_channel(extra);
                self.encrypted_hello(key, hello).await.map_err(|error| {
                    std::io::Error::other(format!(
                        "neither offerer DataChannel completed encrypted Hello: {error}"
                    ))
                })?;
                Ok(rejected)
            }
        }
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
        let request_id = self.request_ids.next().to_string();
        let frame = botster_hub_client::ClientFrame::Request {
            request_id: request_id.clone(),
            request: request.clone(),
        };
        let plaintext = serde_json::to_vec(&frame)?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1)?;
        self.data_channel
            .send_text(&serde_json::to_string(&envelope)?)
            .await?;
        loop {
            let (delivery, metrics) = self.receive_delivery(key).await?;
            match delivery {
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Response {
                    request_id: answered,
                    response,
                }) => {
                    if answered != request_id {
                        return Err(std::io::Error::other(format!(
                            "response {answered} does not correlate with request {request_id}"
                        ))
                        .into());
                    }
                    if let Some(reservation) = response.terminal_reservation.as_ref() {
                        self.route_generations
                            .insert(reservation.label.clone(), reservation.generation);
                    }
                    if let Some(reservation) = response.subscription_reservation.as_ref() {
                        let compatibility = match reservation.kind {
                            botster_hub_client::DaemonSubscriptionReservationKind::Entity => {
                                botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter()
                            }
                            botster_hub_client::DaemonSubscriptionReservationKind::PackageEvent => {
                                botster_hub_client::DaemonCompatibilityRequirement::for_package_event_subscriptions()
                            }
                        };
                        let hello = botster_hub_client::DaemonHello {
                            protocol: botster_hub_client::PROTOCOL.to_string(),
                            compatibility,
                            terminal_compatibility: None,
                        };
                        self.open_reserved_subscription(key, &reservation.label, &hello)
                            .await?;
                    }
                    return Ok((response, metrics));
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Entity { entity }) => {
                    self.pending_entity_frames.push_back(entity);
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Event { event }) => {
                    self.park_or_reject_host_event(&event)?;
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Close { reason }) => {
                    return Err(std::io::Error::other(format!(
                        "hub closed the control channel: {reason:?}"
                    ))
                    .into());
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::HelloAck { .. }) => {
                    return Err(std::io::Error::other("unexpected hello ack").into());
                }
                FixtureDelivery::Terminal { body, .. } => {
                    self.control_terminal_frame_count += 1;
                    self.pending_terminal_frames
                        .push_back((String::new(), body));
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
            let (delivery, _) = if self.subscription_inbounds.is_empty() {
                self.receive_delivery(key).await?
            } else {
                self.receive_subscription_delivery(key).await?
            };
            match delivery {
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Entity { entity }) => {
                    return Ok(entity);
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Response { .. }) => {
                    return Err(std::io::Error::other(
                        "received uncorrelated daemon response while waiting for entity frame",
                    )
                    .into());
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Event { event }) => {
                    self.park_or_reject_host_event(&event)?;
                }
                FixtureDelivery::Server(other) => {
                    return Err(std::io::Error::other(format!(
                        "unexpected server frame while waiting for entity frame: {other:?}"
                    ))
                    .into());
                }
                FixtureDelivery::Terminal { body, .. } => {
                    let label = self.last_terminal_label.clone();
                    self.pending_terminal_frames.push_back((label, body));
                }
            }
        }
    }

    pub(crate) async fn encrypted_hello(
        &mut self,
        key: &AesGcmKey,
        hello: &botster_hub_client::DaemonHello,
    ) -> Result<botster_hub_client::DaemonHelloAck, Box<dyn std::error::Error>> {
        let frame = botster_hub_client::ClientFrame::Hello {
            hello: hello.clone(),
        };
        let plaintext = serde_json::to_vec(&frame)?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1)?;
        self.data_channel
            .send_text(&serde_json::to_string(&envelope)?)
            .await?;
        loop {
            let (delivery, _) = self.receive_delivery(key).await?;
            match delivery {
                FixtureDelivery::Server(botster_hub_client::ServerFrame::HelloAck { ack }) => {
                    return Ok(ack);
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Event { event }) => {
                    self.park_or_reject_host_event(&event)?;
                }
                other => {
                    return Err(std::io::Error::other(format!(
                        "hello ack expected, got {other:?}"
                    ))
                    .into());
                }
            }
        }
    }

    /// The next plaintext terminal frame body from a reserved terminal channel.
    pub(crate) async fn next_terminal_frame(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.next_terminal_frame_with_label(key)
            .await
            .map(|(_, bytes)| bytes)
    }

    /// The next plaintext terminal frame body with the label of its channel.
    pub(crate) async fn next_terminal_frame_with_label(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<(String, Vec<u8>), Box<dyn std::error::Error>> {
        if let Some(pending) = self.pending_terminal_frames.pop_front() {
            return Ok(pending);
        }
        loop {
            let from_control = self.subscription_inbounds.is_empty();
            let (delivery, _) = if from_control {
                self.receive_delivery(key).await?
            } else {
                self.receive_subscription_delivery(key).await?
            };
            match delivery {
                FixtureDelivery::Terminal { body, .. } => {
                    if from_control {
                        self.control_terminal_frame_count += 1;
                        return Ok((String::new(), body));
                    }
                    return Ok((self.last_terminal_label.clone(), body));
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Entity { entity }) => {
                    self.pending_entity_frames.push_back(entity);
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Response { .. }) => {
                    return Err(std::io::Error::other(
                        "received daemon response while waiting for terminal frame",
                    )
                    .into());
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Event { event }) => {
                    self.park_or_reject_host_event(&event)?;
                }
                FixtureDelivery::Server(other) => {
                    return Err(std::io::Error::other(format!(
                        "unexpected server frame while waiting for terminal frame: {other:?}"
                    ))
                    .into());
                }
            }
        }
    }

    /// Attachment generation observed on the reserved channel `label`.
    pub(crate) fn route_generation(&self, label: &str) -> Option<u64> {
        self.route_generations.get(label).copied()
    }

    /// Send one input command on the reserved terminal channel `label`.
    ///
    /// The route must have shown its generation, from the attach response or
    /// from a frame already read on the channel.
    pub(crate) async fn send_terminal_input(
        &mut self,
        key: &AesGcmKey,
        label: &str,
        input: &botster_terminal_protocol_client::TerminalInputCommand,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let index = self
            .subscription_labels
            .iter()
            .position(|candidate| candidate == label)
            .ok_or_else(|| std::io::Error::other(format!("no open reserved channel {label}")))?;
        let generation = self.route_generation(label).ok_or_else(|| {
            std::io::Error::other(format!("generation for {label} is not known yet"))
        })?;
        let operation_id = self.route_operation_ids.assign(label, input);
        let message_id = self.next_terminal_message_id;
        self.next_terminal_message_id += 1;
        let channel = Arc::clone(&self.subscription_channels[index]);
        Self::send_reserved_terminal_frame(
            &channel,
            key,
            generation,
            message_id,
            &encode_input_with_operation_id(input, operation_id),
        )
        .await?;
        Ok(operation_id)
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
        event: &botster_hub_client::DaemonEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let plaintext = serde_json::to_vec(event)?;
        if !self.accept_host_events {
            return Err(std::io::Error::other(format!(
                "unnegotiated IsolatedHub receive path must not decode daemon_event: {}",
                String::from_utf8_lossy(&plaintext)
            ))
            .into());
        }
        self.pending_host.try_park(&plaintext)
    }

    pub(crate) async fn next_host_event(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<botster_hub_client::DaemonEvent, Box<dyn std::error::Error>> {
        if let Some(event) = self.pending_host.pop_front() {
            return Ok(event);
        }
        loop {
            let (delivery, _) = if self.subscription_inbounds.is_empty() {
                self.receive_delivery(key).await?
            } else {
                self.receive_subscription_delivery(key).await?
            };
            match delivery {
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Event { event }) => {
                    return Ok(event);
                }
                FixtureDelivery::Terminal { body, .. } => {
                    let label = self.last_terminal_label.clone();
                    self.pending_terminal_frames.push_back((label, body));
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Entity { entity }) => {
                    self.pending_entity_frames.push_back(entity);
                }
                FixtureDelivery::Server(botster_hub_client::ServerFrame::Response { .. }) => {
                    return Err(std::io::Error::other(
                        "received daemon response while waiting for host event",
                    )
                    .into());
                }
                FixtureDelivery::Server(other) => {
                    return Err(std::io::Error::other(format!(
                        "unexpected server frame while waiting for host event: {other:?}"
                    ))
                    .into());
                }
            }
        }
    }

    pub(crate) async fn receive_delivery(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<(FixtureDelivery, LocalWebrtcResponseMetrics), Box<dyn std::error::Error>> {
        self.inbound.receive_delivery(key).await
    }

    pub(crate) async fn create_extra_data_channel(
        &mut self,
    ) -> Result<ExtraWebrtcDataChannel, Box<dyn std::error::Error>> {
        self.create_labeled_data_channel("botster-extra").await
    }

    pub(crate) async fn create_labeled_data_channel(
        &mut self,
        label: &str,
    ) -> Result<ExtraWebrtcDataChannel, Box<dyn std::error::Error>> {
        let runtime =
            default_runtime().ok_or_else(|| std::io::Error::other("no async runtime found"))?;
        let (open_tx, open_rx) = channel::<()>(1);
        let (message_tx, message_rx) = channel::<InboundMessage>(256);
        let (closed_tx, closed_rx) = channel::<()>(1);
        let (inbound_tx, inbound) =
            WebrtcInboundMailbox::bounded(WEBRTC_INBOUND_MAX_FRAMES, WEBRTC_INBOUND_MAX_BYTES);
        let extra = self
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
            .await?;
        // Creation completion instant (after create_data_channel returned), reported on a
        // timeout for correlation with the Hub's admission-entry receipt.
        let creation_completed_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or(0);
        spawn_offerer_channel_poll(
            runtime,
            extra.clone(),
            open_tx,
            inbound_tx,
            Arc::clone(&inbound.occupancy),
            Some(message_tx),
            Some(closed_tx),
        );
        let mut extra_channel = ExtraWebrtcDataChannel {
            label: label.to_string(),
            data_channel: extra,
            inbound,
            messages: message_rx,
            closed: closed_rx,
            open_rx,
        };
        match timeout(
            webrtc_runtime().as_ref(),
            Duration::from_secs(5),
            extra_channel.open_rx.recv(),
        )
        .await
        {
            Ok(Some(())) => {}
            Ok(None) => {
                return Err(std::io::Error::other("labeled DataChannel closed before open").into());
            }
            Err(_) => {
                // Timeout instant, captured on entering this branch before any diagnostic
                // read.
                let timeout_unix_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|elapsed| elapsed.as_millis())
                    .unwrap_or(0);
                // Diagnostic only; the open deadline is unchanged. This reads the channel's
                // state after the deadline expired, so it reports the state at that later
                // instant (Connecting, Open, or Closed) plus whether the poll observed an
                // OnClose; it does not establish that the channel never opened or that it
                // closed before the deadline, and it does not establish the network cause.
                // The read is bounded so it cannot hang the failure path.
                let ready_state = match timeout(
                    webrtc_runtime().as_ref(),
                    Duration::from_millis(500),
                    extra_channel.data_channel.ready_state(),
                )
                .await
                {
                    Ok(Ok(state)) => format!("{state:?}"),
                    Ok(Err(error)) => format!("unavailable({error})"),
                    Err(_) => "unavailable(timeout)".to_string(),
                };
                let closed_observed = extra_channel.closed.try_recv().is_ok();
                return Err(std::io::Error::other(format!(
                    "timed out waiting for labeled DataChannel open: label={} ready_state={ready_state} closed_observed={closed_observed} creation_completed_unix_ms={creation_completed_unix_ms} timeout_unix_ms={timeout_unix_ms}",
                    extra_channel.label
                ))
                .into());
            }
        }
        Ok(extra_channel)
    }

    pub(crate) async fn open_reserved_terminal(
        &mut self,
        key: &AesGcmKey,
        label: &str,
        hello: &botster_hub_client::DaemonHello,
    ) -> Result<Arc<dyn DataChannel>, Box<dyn std::error::Error>> {
        self.open_reserved_subscription(key, label, hello).await
    }

    pub(crate) async fn open_reserved_subscription(
        &mut self,
        key: &AesGcmKey,
        label: &str,
        hello: &botster_hub_client::DaemonHello,
    ) -> Result<Arc<dyn DataChannel>, Box<dyn std::error::Error>> {
        let extra = self.create_labeled_data_channel(label).await?;
        let channel = extra.data_channel.clone();
        let frame = botster_hub_client::ClientFrame::Hello {
            hello: hello.clone(),
        };
        let plaintext = serde_json::to_vec(&frame)?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1)?;
        channel
            .send_text(&serde_json::to_string(&envelope)?)
            .await?;
        let mut extra = extra;
        let (delivery, _) = extra.inbound.receive_delivery(key).await?;
        match delivery {
            FixtureDelivery::Server(botster_hub_client::ServerFrame::HelloAck { .. }) => {}
            other => {
                return Err(std::io::Error::other(format!(
                    "reserved hello ack expected, got {other:?}"
                ))
                .into());
            }
        }
        self.subscription_channels.push(Arc::clone(&channel));
        self.subscription_inbounds.push(extra.inbound);
        self.subscription_labels.push(label.to_string());
        Ok(channel)
    }

    pub(crate) async fn bind_subscription_reservation(
        &mut self,
        key: &AesGcmKey,
        response: &botster_hub_client::DaemonResponse,
        hello: &botster_hub_client::DaemonHello,
    ) -> Result<Arc<dyn DataChannel>, Box<dyn std::error::Error>> {
        let label = &response
            .subscription_reservation
            .as_ref()
            .ok_or_else(|| std::io::Error::other("response has no subscription reservation"))?
            .label;
        self.open_reserved_subscription(key, label, hello).await
    }

    /// Send raw input body bytes on a reserved terminal channel at `generation`.
    pub(crate) async fn send_reserved_terminal_frame(
        channel: &Arc<dyn DataChannel>,
        key: &AesGcmKey,
        generation: u64,
        message_id: u64,
        frame: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        for chunk in seal_terminal_chunks(key, generation, frame, message_id) {
            if chunk.len() >= botster_hub_client::LOCAL_WEBRTC_MAX_FRAME_BYTES {
                return Err(
                    std::io::Error::other("terminal chunk exceeds WebRTC frame bound").into(),
                );
            }
            channel
                .send(bytes::BytesMut::from(chunk.as_slice()))
                .await?;
        }
        Ok(())
    }

    async fn receive_subscription_delivery(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<(FixtureDelivery, LocalWebrtcResponseMetrics), Box<dyn std::error::Error>> {
        if self.subscription_inbounds.is_empty() {
            return Err(std::io::Error::other("no reserved subscription channel").into());
        }
        loop {
            let mut index = 0;
            while index < self.subscription_inbounds.len() {
                match timeout(
                    webrtc_runtime().as_ref(),
                    Duration::from_millis(50),
                    self.subscription_inbounds[index].receive_delivery(key),
                )
                .await
                {
                    Ok(Ok(delivery)) => {
                        if let FixtureDelivery::Terminal { generation, .. } = &delivery.0 {
                            let label = self.subscription_labels[index].clone();
                            self.route_generations.insert(label.clone(), *generation);
                            self.last_terminal_label = label;
                        }
                        return Ok(delivery);
                    }
                    Ok(Err(error)) => {
                        let label = self.subscription_labels.remove(index);
                        assert!(
                            self.subscription_receive_errors.len() < 256,
                            "subscription receive errors exceeded the fixture storage bound: label={label}; error={error}"
                        );
                        self.subscription_receive_errors
                            .push((label, error.to_string()));
                        self.subscription_channels.remove(index);
                        self.subscription_inbounds.remove(index);
                    }
                    Err(_) => index += 1,
                }
            }
            if let Ok(delivery) = timeout(
                webrtc_runtime().as_ref(),
                Duration::from_millis(50),
                self.inbound.receive_delivery(key),
            )
            .await
            {
                return delivery;
            }
        }
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

pub(crate) async fn open_local_webrtc_peer_with_extra_channel(
    endpoint: &botster_hub_client::DaemonEndpoint,
    bootstrap: &botster_hub_client::DaemonLocalWebrtcBootstrap,
) -> (LocalWebrtcOfferPeer, ExtraWebrtcDataChannel, AesGcmKey) {
    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);
    let (mut offer_peer, mut extra, offer) =
        LocalWebrtcOfferPeer::create_offer_with_extra_data_channel()
            .await
            .expect("create WebRTC offer peer with extra DataChannel");
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
        .accept_answer_with_extra_open(answer, &mut extra)
        .await
        .expect("offer peer accepts answer and opens a DataChannel");
    (offer_peer, extra, stream_key)
}

pub(crate) fn write_botster_web_package(root: &Path) {
    fs::create_dir_all(root.join("scripts")).expect("create botster-web package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write botster-web core entrypoint");
    let mut daemon_requirement =
        botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter();
    daemon_requirement.client_name = "botster-web-production-runtime-fixture".to_string();
    let daemon_requirement =
        serde_json::to_string(&daemon_requirement).expect("serialize botster-web requirement");
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

// Framing values come from botster_hub_client constants. daemon-protocol.ts is derived from them.
const PROTOCOL_VERSION = __PROTOCOL_VERSION__;
const UNIX_FRAME_LENGTH_PREFIX_BYTES = __UNIX_FRAME_LENGTH_PREFIX_BYTES__;
const UNIX_CONTAINER_CONTROL = __UNIX_CONTAINER_CONTROL__;
const MAX_CONTROL_REQUEST_BYTES = __MAX_CONTROL_REQUEST_BYTES__;
const MAX_CONTROL_RESPONSE_BYTES = __MAX_CONTROL_RESPONSE_BYTES__;
const DAEMON_REQUIREMENT = __DAEMON_REQUIREMENT__;

function encodeControlFrame(frame) {
  const payload = Buffer.from(JSON.stringify(frame), 'utf8');
  if (payload.length > MAX_CONTROL_REQUEST_BYTES) {
    throw new Error(`control payload exceeds ${MAX_CONTROL_REQUEST_BYTES} bytes`);
  }
  const header = Buffer.alloc(UNIX_FRAME_LENGTH_PREFIX_BYTES + 1);
  header.writeUInt32LE(1 + payload.length, 0);
  header.writeUInt8(UNIX_CONTAINER_CONTROL, UNIX_FRAME_LENGTH_PREFIX_BYTES);
  return Buffer.concat([header, payload]);
}

function takeControlFrame(connection) {
  if (connection.buffer.length < UNIX_FRAME_LENGTH_PREFIX_BYTES) {
    return undefined;
  }
  const frameLength = connection.buffer.readUInt32LE(0);
  if (frameLength === 0 || frameLength > 1 + MAX_CONTROL_RESPONSE_BYTES) {
    throw new Error(`invalid control frame length ${frameLength}`);
  }
  if (connection.buffer.length < UNIX_FRAME_LENGTH_PREFIX_BYTES + frameLength) {
    return undefined;
  }
  const container = connection.buffer.readUInt8(UNIX_FRAME_LENGTH_PREFIX_BYTES);
  const payload = connection.buffer.subarray(
    UNIX_FRAME_LENGTH_PREFIX_BYTES + 1,
    UNIX_FRAME_LENGTH_PREFIX_BYTES + frameLength,
  );
  connection.buffer = connection.buffer.subarray(
    UNIX_FRAME_LENGTH_PREFIX_BYTES + frameLength,
  );
  if (container !== UNIX_CONTAINER_CONTROL) {
    throw new Error(`unexpected daemon container ${container}`);
  }
  return JSON.parse(payload.toString('utf8'));
}

function readControlFrame(connection) {
  const ready = takeControlFrame(connection);
  if (ready !== undefined) {
    return Promise.resolve(ready);
  }

  return new Promise((resolve, reject) => {
    const onData = (chunk) => {
      connection.buffer = Buffer.concat([connection.buffer, chunk]);
      let frame;
      try {
        frame = takeControlFrame(connection);
      } catch (error) {
        cleanup();
        reject(error);
        return;
      }
      if (frame === undefined) {
        return;
      }
      cleanup();
      resolve(frame);
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const onEnd = () => {
      cleanup();
      reject(new Error('daemon connection ended before a complete control frame'));
    };
    const onClose = () => {
      cleanup();
      reject(new Error('daemon connection closed before a complete control frame'));
    };
    const cleanup = () => {
      connection.stream.off('data', onData);
      connection.stream.off('error', onError);
      connection.stream.off('end', onEnd);
      connection.stream.off('close', onClose);
    };
    connection.stream.on('data', onData);
    connection.stream.once('error', onError);
    connection.stream.once('end', onEnd);
    connection.stream.once('close', onClose);
  });
}

function currentRequirement() {
  return DAEMON_REQUIREMENT;
}

async function connectDaemon() {
  if (!socket) {
    throw new Error('BOTSTER_HUB_CONNECTION does not contain a Unix socket');
  }
  const stream = net.createConnection(socket);
  const connection = { stream, buffer: Buffer.alloc(0), nextRequestId: 1 };
  try {
    await new Promise((resolve, reject) => {
      stream.once('connect', resolve);
      stream.once('error', reject);
    });
    stream.write(encodeControlFrame({
      frame: 'hello',
      hello: {
        protocol: 'botster-hub-daemon-v1',
        compatibility: currentRequirement(),
      },
    }));
    const helloFrame = await readControlFrame(connection);
    const helloAck = helloFrame.ack;
    if (helloFrame.frame !== 'hello_ack'
        || helloAck?.protocol !== 'botster-hub-daemon-v1'
        || helloAck?.compatibility?.protocol_version !== PROTOCOL_VERSION) {
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
    const response = await sendDaemonRequest(connection, { type: 'status' });
    if (response.kind !== 'status' || !response.status) {
      throw new Error(`unexpected daemon status response: ${JSON.stringify(response)}`);
    }
  } finally {
    connection?.stream.destroy();
  }
}

async function sendDaemonRequest(connection, request) {
  const requestId = String(connection.nextRequestId++);
  connection.stream.write(encodeControlFrame({
    frame: 'request',
    request_id: requestId,
    request,
  }));
  while (true) {
    const frame = await readControlFrame(connection);
    if (frame.frame === 'close') {
      throw new Error(`daemon closed connection: ${JSON.stringify(frame.reason)}`);
    }
    if (frame.frame === 'response' && frame.request_id === requestId) {
      return frame.response;
    }
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

  try {
    return await sendDaemonRequest(connection, payload.request);
  } finally {
    if (!connectionId || payload.close === true) {
      connection.stream.end();
      if (connectionId) {
        connections.delete(connectionId);
      }
    }
  }
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
"#
        .replace("__DAEMON_REQUIREMENT__", &daemon_requirement)
        .replace(
            "__PROTOCOL_VERSION__",
            &botster_hub_client::PROTOCOL_VERSION.to_string(),
        )
        .replace(
            "__UNIX_FRAME_LENGTH_PREFIX_BYTES__",
            &botster_hub_client::UNIX_FRAME_LENGTH_PREFIX_BYTES.to_string(),
        )
        .replace(
            "__UNIX_CONTAINER_CONTROL__",
            &botster_hub_client::UNIX_CONTAINER_CONTROL.to_string(),
        )
        .replace(
            "__MAX_CONTROL_REQUEST_BYTES__",
            &botster_hub_client::MAX_CONTROL_REQUEST_BYTES.to_string(),
        )
        .replace(
            "__MAX_CONTROL_RESPONSE_BYTES__",
            &botster_hub_client::MAX_CONTROL_RESPONSE_BYTES.to_string(),
        ),
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
    let frame = botster_hub_client::ServerFrame::Event {
        event: botster_hub_client::DaemonEvent::RuntimeObservation {
            kind: "delivery-ok".to_string(),
        },
    };
    let plaintext = serde_json::to_vec(&frame).expect("server frame json");
    let envelope = encrypt_aes_gcm(&key, &plaintext, 1).expect("encrypt delivery");
    let encrypted = serde_json::to_string(&envelope).expect("envelope json");
    let mid = encrypted.len() / 2;
    let first_chunk = test_chunk(0, 2, &encrypted[..mid], encrypted.len());
    let second_chunk = test_chunk(1, 2, &encrypted[mid..], encrypted.len());

    let (tx, mut inbound) = WebrtcInboundMailbox::bounded(8, 64 * 1024);
    admit_inbound_frame(&inbound.occupancy, &tx, InboundMessage::Text(first_chunk))
        .expect("admit first chunk");

    let cancelled = block_on(async {
        timeout(
            webrtc_runtime().as_ref(),
            Duration::from_millis(50),
            inbound.receive_delivery(&key),
        )
        .await
    });
    assert!(
        cancelled.is_err(),
        "first chunk must leave receive_delivery waiting for the remainder"
    );
    assert!(
        inbound.reassembly.is_some(),
        "cancelled receive_delivery must keep reassembly state"
    );

    admit_inbound_frame(&inbound.occupancy, &tx, InboundMessage::Text(second_chunk))
        .expect("admit second chunk");
    let (delivery, metrics) = block_on(inbound.receive_delivery(&key)).expect("resume delivery");
    assert!(matches!(
        delivery,
        FixtureDelivery::Server(botster_hub_client::ServerFrame::Event {
            event: botster_hub_client::DaemonEvent::RuntimeObservation { ref kind }
        }) if kind == "delivery-ok"
    ));
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
    admit_inbound_frame(
        &inbound.occupancy,
        &tx,
        InboundMessage::Text("abcd".to_string()),
    )
    .expect("first frame");
    let count_err = admit_inbound_frame(
        &inbound.occupancy,
        &tx,
        InboundMessage::Text("efgh".to_string()),
    )
    .expect_err("count overflow");
    assert_eq!(count_err, InboundAdmitError::CountLimit);
    let count_snap = inbound.occupancy.snapshot();
    assert_eq!(count_snap.count, 1);
    assert_eq!(count_snap.bytes, 4);
    assert_eq!(count_snap.overflow, 1);
    assert_eq!(count_snap.max_count, 1);
    assert_eq!(count_snap.max_bytes, 8);

    let (tx, inbound) = WebrtcInboundMailbox::bounded(8, 8);
    let byte_err = admit_inbound_frame(
        &inbound.occupancy,
        &tx,
        InboundMessage::Text("ninebytes".to_string()),
    )
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
    let (narrow_tx, _narrow_rx) = channel::<InboundMessage>(1);
    admit_inbound_frame(
        &occupancy,
        &narrow_tx,
        InboundMessage::Text("full".to_string()),
    )
    .expect("fill channel");
    let full_err = admit_inbound_frame(
        &occupancy,
        &narrow_tx,
        InboundMessage::Text("drop".to_string()),
    )
    .expect_err("channel full");
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
