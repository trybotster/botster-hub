#![allow(dead_code)]
#![allow(unused_imports)]

use super::*;
use crate::admission::budgets::ENTITY_SUBSCRIPTION_QUEUE_CAPACITY;
use crate::admission::unix_hello::WebrtcTerminalAdmission;
use crate::daemon::control::handle_control_message;
use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::owner_loop::DaemonControlState;
use crate::subscription::attach_routes::negotiated_unix_capability_set;
use crate::subscription::entity::EntityFrameSender;
use crate::transport::webrtc::adapter::WebRtcConnectionMux;
use crate::transport::webrtc::control_channel::LocalWebrtcDataChannel;
use crate::transport::webrtc::delivery::{
    encrypt_daemon_response, frame_encrypted_daemon_delivery,
};
use crate::transport::webrtc::peer::*;
use crate::transport::webrtc::{LocalWebrtcError, LocalWebrtcResult};
use crate::{
    DataDirectoryOption, HostIdentityOptions, HubDaemon, HubStartupOptions,
    PackageEventPlaneOptions, RuntimeEnvironment, SessionDefaults,
};
use async_trait::async_trait;
use botster_core::{AesGcmEnvelope, AesGcmKey, decrypt_aes_gcm, encrypt_aes_gcm};
use botster_hub_client::{
    DaemonCompatibilityRequirement, DaemonDiagnostic, DaemonEvent, DaemonHello, DaemonHelloAck,
    DaemonLocalWebrtcDeliveryChunk, DaemonLocalWebrtcDeliveryKind, DaemonRequest, DaemonResponse,
    LOCAL_WEBRTC_MAX_DELIVERY_BYTES, PROTOCOL,
};
use serde_json::Value;
use std::collections::{BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use webrtc::data_channel::{
    DataChannel, DataChannelEvent, RTCDataChannelInit, RTCDataChannelMessage,
};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{
    Receiver as AsyncReceiver, Runtime, Sender as AsyncSender, channel as webrtc_channel,
    default_runtime, timeout,
};

pub(crate) static EXTRA_CHANNEL_ORACLE_ENV: Mutex<()> = Mutex::new(());

#[derive(Default)]
pub(crate) struct FakeDataChannel {
    pub(crate) events: Mutex<VecDeque<DataChannelEvent>>,
    pub(crate) sent: Mutex<Vec<String>>,
    pub(crate) closed: AtomicBool,
    pub(crate) send_fails: AtomicBool,
    pub(crate) send_hangs: AtomicBool,
    pub(crate) sent_before_low_water: AtomicBool,
    pub(crate) poll_ends: AtomicBool,
    pub(crate) event_notify: tokio::sync::Notify,
    pub(crate) send_notify: tokio::sync::Notify,
}

#[async_trait]
impl LocalWebrtcDataChannel for FakeDataChannel {
    async fn local_set_buffered_amount_low_threshold(&self, _threshold: u32) -> Result<(), String> {
        Ok(())
    }

    async fn local_set_buffered_amount_high_threshold(
        &self,
        _threshold: u32,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn local_send_text(&self, text: &str) -> Result<(), String> {
        while self.send_hangs.load(Ordering::Acquire) {
            let notified = self.send_notify.notified();
            tokio::pin!(notified);
            if !self.send_hangs.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        if self.send_fails.load(Ordering::Acquire) {
            return Err("fixture send failure".to_string());
        }
        if self
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(event, DataChannelEvent::OnBufferedAmountLow))
        {
            self.sent_before_low_water.store(true, Ordering::Release);
        }
        self.sent.lock().unwrap().push(text.to_string());
        Ok(())
    }

    async fn local_poll(&self) -> Option<DataChannelEvent> {
        loop {
            let notified = self.event_notify.notified();
            if let Some(event) = self.events.lock().unwrap().pop_front() {
                return Some(event);
            }
            if self.poll_ends.load(Ordering::Acquire) {
                return None;
            }
            notified.await;
        }
    }

    async fn local_close(&self) -> Result<(), String> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}

impl FakeDataChannel {
    pub(crate) fn push_event(&self, event: DataChannelEvent) {
        self.events.lock().unwrap().push_back(event);
        self.event_notify.notify_waiters();
    }

    pub(crate) fn release_hung_send(&self) {
        self.send_hangs.store(false, Ordering::Release);
        self.send_notify.notify_waiters();
    }
}

pub(crate) fn encrypted_request_event(
    key: &AesGcmKey,
    request: &DaemonRequest,
) -> DataChannelEvent {
    let plaintext = serde_json::to_vec(request).unwrap();
    let envelope = encrypt_aes_gcm(key, &plaintext, 1).unwrap();
    let data = serde_json::to_vec(&envelope).unwrap();
    DataChannelEvent::OnMessage(RTCDataChannelMessage {
        is_string: true,
        data: data.as_slice().into(),
    })
}

pub(crate) fn test_peer_state(grant_id: &str) -> LocalWebrtcPeerState {
    let (runtime_tx, _runtime_rx) = tokio_mpsc::channel(64);
    LocalWebrtcPeerState::new(grant_id.to_string(), runtime_tx)
}
/// Serializes teardown tests that inject a close hang, so parallel cargo tests
/// do not false-fail each other. The dedicated-runtime worker counter is
/// instance-scoped on each LocalWebrtcTransport.
pub(crate) fn teardown_test_lock() -> std::sync::MutexGuard<'static, ()> {
    pub(crate) static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Serializes Spawn → worker census capture. Session-worker sockets may not live under the
/// hub data directory (core uses a separate control-socket path), so capture relies on a
/// process-global "new pid" baseline that is only safe while no other harness is spawning.
pub(crate) fn spawn_capture_lock() -> std::sync::MutexGuard<'static, ()> {
    pub(crate) static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) struct TestOfferHandler {
    pub(crate) gather_complete_tx: AsyncSender<()>,
    pub(crate) connected_tx: AsyncSender<()>,
}

#[async_trait]
impl PeerConnectionEventHandler for TestOfferHandler {
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

pub(crate) struct TestOfferPeer {
    pub(crate) peer: Box<dyn PeerConnection>,
    pub(crate) data_channel: Arc<dyn DataChannel>,
    pub(crate) connected_rx: AsyncReceiver<()>,
    pub(crate) data_channel_open_rx: AsyncReceiver<()>,
    pub(crate) data_channel_message_rx: AsyncReceiver<String>,
    pub(crate) accept_host_events: bool,
    pub(crate) pending_host_events: VecDeque<DaemonEvent>,
}

impl TestOfferPeer {
    pub(crate) async fn create_offer() -> (Self, Value) {
        let runtime = default_runtime().expect("webrtc default runtime for offer peer");
        let (gather_complete_tx, mut gather_complete_rx) = webrtc_channel::<()>(1);
        let (connected_tx, connected_rx) = webrtc_channel::<()>(1);
        let (data_channel_open_tx, data_channel_open_rx) = webrtc_channel::<()>(1);
        let (data_channel_message_tx, data_channel_message_rx) = webrtc_channel::<String>(256);
        let handler = Arc::new(TestOfferHandler {
            gather_complete_tx,
            connected_tx,
        });
        let peer = PeerConnectionBuilder::new()
            .with_handler(handler)
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await
            .expect("build offer peer");
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
            .expect("create offer data channel");
        {
            let data_channel = data_channel.clone();
            let open_tx = data_channel_open_tx;
            let message_tx = data_channel_message_tx;
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
        let offer = peer.create_offer(None).await.expect("create offer");
        peer.set_local_description(offer)
            .await
            .expect("set local offer");
        let _ = timeout(
            runtime.as_ref(),
            Duration::from_secs(5),
            gather_complete_rx.recv(),
        )
        .await;
        let offer = peer
            .local_description()
            .await
            .expect("offer local description");
        (
            Self {
                peer: Box::new(peer),
                data_channel,
                connected_rx,
                data_channel_open_rx,
                data_channel_message_rx,
                accept_host_events: false,
                pending_host_events: VecDeque::new(),
            },
            serde_json::to_value(offer).expect("serialize offer"),
        )
    }

    pub(crate) async fn accept_answer(&mut self, answer: Value) {
        let answer = serde_json::from_value::<RTCSessionDescription>(answer).expect("parse answer");
        self.peer
            .set_remote_description(answer)
            .await
            .expect("set remote answer");
        timeout(
            webrtc_runtime().as_ref(),
            Duration::from_secs(15),
            self.connected_rx.recv(),
        )
        .await
        .expect("timed out waiting for offer peer connected")
        .expect("connected signal");
        timeout(
            webrtc_runtime().as_ref(),
            Duration::from_secs(10),
            self.data_channel_open_rx.recv(),
        )
        .await
        .expect("timed out waiting for data channel open")
        .expect("open signal");
    }

    pub(crate) async fn encrypted_request(
        &mut self,
        key: &AesGcmKey,
        request: &DaemonRequest,
    ) -> DaemonResponse {
        let plaintext = serde_json::to_vec(request).expect("serialize request");
        let envelope = encrypt_aes_gcm(key, &plaintext, 1).expect("encrypt request");
        self.data_channel
            .send_text(&serde_json::to_string(&envelope).expect("serialize envelope"))
            .await
            .expect("send encrypted request");
        loop {
            let mut encrypted = String::new();
            let mut next_chunk_index = 0u32;
            let mut delivery_kind = None;
            loop {
                let response = timeout(
                    webrtc_runtime().as_ref(),
                    Duration::from_secs(10),
                    self.data_channel_message_rx.recv(),
                )
                .await
                .expect("response frame timeout")
                .expect("data channel remains open for response");
                let chunk: DaemonLocalWebrtcDeliveryChunk =
                    serde_json::from_str(&response).expect("parse delivery chunk");
                if let Some(kind) = delivery_kind {
                    assert_eq!(kind, chunk.delivery_kind);
                } else {
                    delivery_kind = Some(chunk.delivery_kind);
                }
                assert_eq!(chunk.chunk_index, next_chunk_index);
                encrypted.push_str(&chunk.payload);
                next_chunk_index += 1;
                if chunk.chunk_index + 1 == chunk.chunk_count {
                    break;
                }
            }
            let envelope: AesGcmEnvelope =
                serde_json::from_str(&encrypted).expect("parse response envelope");
            let plaintext = decrypt_aes_gcm(key, &envelope).expect("decrypt response");
            match delivery_kind.expect("complete delivery declares a kind") {
                DaemonLocalWebrtcDeliveryKind::DaemonResponse => {
                    return serde_json::from_slice(&plaintext).expect("parse daemon response");
                }
                DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame => {
                    // Entity frames can interleave while a subscription is live; keep waiting.
                }
                DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame => {
                    panic!("unbound peer helper must not receive daemon_terminal_frame");
                }
                DaemonLocalWebrtcDeliveryKind::DaemonEvent => {
                    self.park_or_reject_host_event(&plaintext);
                }
            }
        }
    }

    pub(crate) fn enable_host_events(&mut self) {
        self.accept_host_events = true;
    }

    pub(crate) fn park_or_reject_host_event(&mut self, plaintext: &[u8]) {
        if !self.accept_host_events {
            panic!("unnegotiated peer helper must not receive daemon_event");
        }
        self.pending_host_events
            .push_back(serde_json::from_slice(plaintext).expect("parse daemon event"));
    }

    pub(crate) async fn next_host_event(&mut self, key: &AesGcmKey) -> DaemonEvent {
        if let Some(event) = self.pending_host_events.pop_front() {
            return event;
        }
        loop {
            let mut encrypted = String::new();
            let mut next_chunk_index = 0u32;
            let mut delivery_kind = None;
            loop {
                let response = timeout(
                    webrtc_runtime().as_ref(),
                    Duration::from_secs(10),
                    self.data_channel_message_rx.recv(),
                )
                .await
                .expect("host event frame timeout")
                .expect("data channel remains open for host event");
                let chunk: DaemonLocalWebrtcDeliveryChunk =
                    serde_json::from_str(&response).expect("parse delivery chunk");
                if let Some(kind) = delivery_kind {
                    assert_eq!(kind, chunk.delivery_kind);
                } else {
                    delivery_kind = Some(chunk.delivery_kind);
                }
                assert_eq!(chunk.chunk_index, next_chunk_index);
                encrypted.push_str(&chunk.payload);
                next_chunk_index += 1;
                if chunk.chunk_index + 1 == chunk.chunk_count {
                    break;
                }
            }
            let envelope: AesGcmEnvelope =
                serde_json::from_str(&encrypted).expect("parse event envelope");
            let plaintext = decrypt_aes_gcm(key, &envelope).expect("decrypt event");
            match delivery_kind.expect("complete delivery declares a kind") {
                DaemonLocalWebrtcDeliveryKind::DaemonEvent => {
                    return serde_json::from_slice(&plaintext).expect("parse daemon event");
                }
                DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame
                | DaemonLocalWebrtcDeliveryKind::DaemonResponse => {}
                DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame => {
                    panic!("unbound peer helper must not receive daemon_terminal_frame");
                }
            }
        }
    }

    pub(crate) async fn encrypted_hello(
        &mut self,
        key: &AesGcmKey,
        hello: &DaemonHello,
    ) -> DaemonHelloAck {
        let plaintext = serde_json::to_vec(hello).expect("serialize hello");
        let envelope = encrypt_aes_gcm(key, &plaintext, 1).expect("encrypt hello");
        self.data_channel
            .send_text(&serde_json::to_string(&envelope).expect("serialize envelope"))
            .await
            .expect("send encrypted hello");
        let mut encrypted = String::new();
        let mut next_chunk_index = 0u32;
        loop {
            let response = timeout(
                webrtc_runtime().as_ref(),
                Duration::from_secs(10),
                self.data_channel_message_rx.recv(),
            )
            .await
            .expect("hello ack timeout")
            .expect("data channel remains open for hello ack");
            let chunk: DaemonLocalWebrtcDeliveryChunk =
                serde_json::from_str(&response).expect("parse hello ack chunk");
            assert_eq!(
                chunk.delivery_kind,
                DaemonLocalWebrtcDeliveryKind::DaemonResponse
            );
            assert_eq!(chunk.chunk_index, next_chunk_index);
            encrypted.push_str(&chunk.payload);
            next_chunk_index += 1;
            if chunk.chunk_index + 1 == chunk.chunk_count {
                break;
            }
        }
        let envelope: AesGcmEnvelope =
            serde_json::from_str(&encrypted).expect("parse hello ack envelope");
        let plaintext = decrypt_aes_gcm(key, &envelope).expect("decrypt hello ack");
        serde_json::from_slice(&plaintext).expect("parse daemon hello ack")
    }

    pub(crate) async fn open_reserved_terminal(
        &mut self,
        key: &AesGcmKey,
        label: &str,
        hello: &DaemonHello,
    ) {
        let runtime = webrtc_runtime();
        let (open_tx, mut open_rx) = webrtc_channel::<()>(1);
        let (message_tx, mut message_rx) = webrtc_channel::<String>(256);
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
            .expect("create reserved subscription channel");
        {
            let channel = channel.clone();
            runtime.spawn(Box::pin(async move {
                while let Some(event) = channel.poll().await {
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
        timeout(runtime.as_ref(), Duration::from_secs(10), open_rx.recv())
            .await
            .expect("timed out waiting for reserved channel open")
            .expect("reserved channel open signal");
        let plaintext = serde_json::to_vec(hello).expect("serialize reserved hello");
        let envelope = encrypt_aes_gcm(key, &plaintext, 1).expect("encrypt reserved hello");
        channel
            .send_text(
                &serde_json::to_string(&envelope).expect("serialize reserved hello envelope"),
            )
            .await
            .expect("send reserved hello");
        let mut encrypted = String::new();
        let mut next_chunk_index = 0u32;
        loop {
            let response = timeout(runtime.as_ref(), Duration::from_secs(10), message_rx.recv())
                .await
                .expect("reserved hello ack timeout")
                .expect("reserved channel remains open for hello ack");
            let chunk: DaemonLocalWebrtcDeliveryChunk =
                serde_json::from_str(&response).expect("parse reserved hello ack chunk");
            assert_eq!(
                chunk.delivery_kind,
                DaemonLocalWebrtcDeliveryKind::DaemonResponse
            );
            assert_eq!(chunk.chunk_index, next_chunk_index);
            encrypted.push_str(&chunk.payload);
            next_chunk_index += 1;
            if chunk.chunk_index + 1 == chunk.chunk_count {
                break;
            }
        }
        let envelope: AesGcmEnvelope =
            serde_json::from_str(&encrypted).expect("parse reserved hello ack envelope");
        let plaintext = decrypt_aes_gcm(key, &envelope).expect("decrypt reserved hello ack");
        let _: DaemonHelloAck =
            serde_json::from_slice(&plaintext).expect("parse reserved hello ack");
    }
}

pub(crate) fn unique_test_data_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "botster-hub-webrtc-teardown-{label}-{}-{nanos}",
        std::process::id()
    ))
}

pub(crate) fn start_test_daemon_with_event_queue(
    data_directory: PathBuf,
    consumer_queue_max_events: Option<usize>,
) -> HubDaemon {
    let mut package_event_plane = PackageEventPlaneOptions::default();
    if let Some(max_events) = consumer_queue_max_events {
        package_event_plane.consumer_queue_max_events = max_events;
    }
    let config = HubStartupOptions {
        host: HostIdentityOptions {
            id: "local-webrtc-teardown-test".to_string(),
            display_name: "Local WebRTC Teardown Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(data_directory),
        session_defaults: SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        package_event_plane,
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("build teardown test config");
    HubDaemon::start(config).expect("start teardown test daemon")
}

pub(crate) fn wait_until(deadline: Instant, mut predicate: impl FnMut() -> bool, label: &str) {
    if soft_wait_until(deadline, &mut predicate) {
        return;
    }
    panic!("timed out waiting for {label}");
}

/// Soft wait used from Drop/cleanup paths. Never panics (panic-in-drop aborts).
pub(crate) fn soft_wait_until(deadline: Instant, predicate: &mut dyn FnMut() -> bool) -> bool {
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

pub(crate) struct PeerHarness {
    pub(crate) daemon: HubDaemon,
    pub(crate) state: DaemonControlState,
    pub(crate) terminal_path: PathBuf,
    pub(crate) control_tx: ControlSender,
    pub(crate) control_rx: tokio_mpsc::Receiver<ControlMessage>,
    pub(crate) transport_handle: tokio::runtime::Handle,
    /// Keep a multi-thread runtime alive so Handle remains valid.
    pub(crate) _transport_runtime: tokio::runtime::Runtime,
    pub(crate) data_directory: PathBuf,
    /// Worker-backed sessions that must be shut down before Hub stop.
    pub(crate) owned_sessions: Vec<String>,
    /// Exact session-worker identities captured at Spawn readiness.
    pub(crate) owned_workers: Vec<OwnedWorkerIdentity>,
    pub(crate) sessions_cleaned: bool,
}

thread_local! {
    pub(crate) static LAST_SESSION_CLEANUP_ERROR: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

impl Drop for PeerHarness {
    fn drop(&mut self) {
        // Panic-safe: HubDaemon::stop preserves workers via release_for_restart.
        // Never panic inside Drop while already panicking (would abort).
        if let Err(error) = self.shutdown_owned_sessions() {
            LAST_SESSION_CLEANUP_ERROR.with(|slot| {
                *slot.borrow_mut() = Some(error.clone());
            });
            if !std::thread::panicking() {
                panic!("session cleanup failed: {error}");
            }
        }
    }
}

impl PeerHarness {
    pub(crate) fn new(label: &str) -> Self {
        Self::new_with_event_queue(label, None)
    }

    pub(crate) fn new_with_event_queue(
        label: &str,
        consumer_queue_max_events: Option<usize>,
    ) -> Self {
        LAST_SESSION_CLEANUP_ERROR.with(|slot| *slot.borrow_mut() = None);
        let data_directory = unique_test_data_dir(label);
        let terminal_path = data_directory.join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
        let mut daemon =
            start_test_daemon_with_event_queue(data_directory.clone(), consumer_queue_max_events);
        let (control_tx, control_rx) = tokio_mpsc::channel(256);
        let transport_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("botster-webrtc-test-control")
            .build()
            .expect("control transport runtime");
        let transport_handle = transport_runtime.handle().clone();
        #[allow(clippy::field_reassign_with_default)]
        let mut state = DaemonControlState::default();
        state.event_plane = daemon.local_webrtc().event_plane();
        Self {
            daemon,
            state,
            terminal_path,
            control_tx,
            control_rx,
            transport_handle,
            _transport_runtime: transport_runtime,
            data_directory,
            owned_sessions: Vec::new(),
            owned_workers: Vec::new(),
            sessions_cleaned: false,
        }
    }

    pub(crate) fn control_request(&mut self, request: DaemonRequest) -> Option<DaemonResponse> {
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut self.daemon,
            &mut self.state,
            &self.terminal_path,
            &self.transport_handle,
            self.control_tx.clone(),
            ControlMessage::Request {
                request: Box::new(request),
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client_id: None,
                enqueued_at: Instant::now(),
            },
        );
        reply_rx.blocking_recv().ok().and_then(|result| result.ok())
    }

    pub(crate) fn list_session_lifecycle(&mut self, session_id: &str) -> Option<String> {
        let response = self.control_request(DaemonRequest::ListSessions)?;
        response
            .sessions
            .into_iter()
            .find(|session| session.session_id == session_id)
            .map(|session| session.lifecycle)
    }

    pub(crate) fn shutdown_and_remove_session(&mut self, session_id: &str) -> Result<(), String> {
        let shutdown = self
            .control_request(DaemonRequest::ShutdownSession {
                session_id: session_id.to_string(),
            })
            .ok_or_else(|| format!("ShutdownSession returned no response for {session_id}"))?;
        if shutdown.kind == botster_hub_client::DaemonResponseKind::OperatorError {
            return Err(format!(
                "ShutdownSession operator error for {session_id}: {:?}",
                shutdown.error
            ));
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        let terminal = soft_wait_until(deadline, &mut || match self
            .list_session_lifecycle(session_id)
            .as_deref()
        {
            None => true,
            Some(lifecycle) => {
                lifecycle == "exited"
                    || lifecycle == "failed"
                    || lifecycle == "stopped"
                    || lifecycle.contains("exit")
            }
        });
        if !terminal {
            return Err(format!(
                "session {session_id} did not become terminal after ShutdownSession"
            ));
        }

        let remove = self
            .control_request(DaemonRequest::RemoveSession {
                session_id: session_id.to_string(),
            })
            .ok_or_else(|| format!("RemoveSession returned no response for {session_id}"))?;
        if remove.kind != botster_hub_client::DaemonResponseKind::SessionRemoved {
            return Err(format!(
                "RemoveSession expected SessionRemoved for {session_id}, got {:?}",
                remove.kind
            ));
        }
        if self.list_session_lifecycle(session_id).is_some() {
            return Err(format!(
                "session {session_id} still listed after successful RemoveSession"
            ));
        }
        Ok(())
    }

    pub(crate) fn shutdown_owned_sessions(&mut self) -> Result<(), String> {
        if self.sessions_cleaned {
            return Ok(());
        }
        // Always mark cleaned before any fallible wait so Drop cannot re-enter and
        // panic a second time (panic-in-destructor aborts).
        self.sessions_cleaned = true;
        let sessions = std::mem::take(&mut self.owned_sessions);
        let workers = std::mem::take(&mut self.owned_workers);
        let mut errors = Vec::new();
        for session_id in sessions {
            if let Err(error) = self.shutdown_and_remove_session(&session_id) {
                errors.push(error);
            }
        }
        // Production-path wait only: ShutdownSession + RemoveSession must reap the
        // worker tree and control socket without harness kill/unlink assistance.
        let production_deadline = Instant::now() + Duration::from_secs(5);
        let production_gone = soft_wait_until(production_deadline, &mut || {
            workers.iter().all(|worker| worker.is_fully_gone())
        });
        if !production_gone {
            for worker in &workers {
                if !worker.is_fully_gone() {
                    errors.push(format!(
                            "production ShutdownSession/RemoveSession left survivor (before harness reap): {worker:?} (pid_alive={} socket_exists={} residual_owned_pids={:?})",
                            process_is_alive(worker.pid),
                            !worker.control_socket.as_os_str().is_empty()
                                && worker.control_socket.exists(),
                            worker.residual_group_members(),
                        ));
                }
            }
            // Hygiene only: kill/unlink residual processes so the suite does not
            // leave orphans. This MUST NOT clear the production-survivor error —
            // tests must fail when production cleanup leaked.
            for worker in &workers {
                if !worker.is_fully_gone() {
                    reap_owned_worker(worker);
                }
            }
            let hygiene_deadline = Instant::now() + Duration::from_secs(2);
            let _ = soft_wait_until(hygiene_deadline, &mut || {
                workers.iter().all(|worker| worker.is_fully_gone())
            });
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    pub(crate) fn process_until_peer_closed(&mut self, grant_id: &str, deadline: Instant) {
        loop {
            if Instant::now() >= deadline {
                panic!("timed out waiting for LocalWebrtcPeerClosed for {grant_id}");
            }
            match self.control_rx.try_recv() {
                Ok(message) => {
                    let is_closed = matches!(
                        &message,
                        ControlMessage::LocalWebrtcPeerClosed { grant_id: closed, .. }
                            if closed == grant_id
                    );
                    handle_control_message(
                        &mut self.daemon,
                        &mut self.state,
                        &self.terminal_path,
                        &self.transport_handle,
                        self.control_tx.clone(),
                        message,
                    );
                    if is_closed {
                        return;
                    }
                }
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                    panic!("control channel closed before LocalWebrtcPeerClosed");
                }
            }
        }
    }

    pub(crate) fn signal_peer(&mut self, origin: &str) -> LiveSignaledPeer {
        let bootstrap = self
            .daemon
            .local_webrtc()
            .issue_bootstrap("botster-web", "web-client", origin)
            .expect("issue bootstrap");
        let stream_key = crate::admission::grants::secret_stream_key(&bootstrap.grant_secret)
            .expect("bootstrap secret is stream key");
        let offer_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("botster-webrtc-offer-test")
            .build()
            .expect("offer peer runtime");
        let (mut offer_peer, offer) = offer_runtime.block_on(TestOfferPeer::create_offer());
        let answer = self
            .daemon
            .local_webrtc()
            .signal(
                LocalWebrtcSignalRequest {
                    grant_id: bootstrap.grant_id.clone(),
                    grant_secret: bootstrap.grant_secret.clone(),
                    origin: origin.to_string(),
                    offer,
                },
                self.control_tx.clone(),
            )
            .expect("signal real local WebRTC peer");
        offer_runtime.block_on(offer_peer.accept_answer(answer.answer));
        LiveSignaledPeer {
            grant_id: bootstrap.grant_id,
            stream_key,
            offer_peer: Some(offer_peer),
            offer_runtime,
        }
    }

    pub(crate) fn request_on_peer(
        &mut self,
        peer: &mut LiveSignaledPeer,
        request: DaemonRequest,
        label: &str,
    ) -> DaemonResponse {
        let key = peer.stream_key.clone();
        let mut offer_peer = peer
            .offer_peer
            .take()
            .expect("offer peer available for request");
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let offer_handle = peer.offer_runtime.handle().clone();
        let worker = thread::spawn(move || {
            let response = offer_handle.block_on(offer_peer.encrypted_request(&key, &request));
            response_tx
                .send((offer_peer, response))
                .expect("return encrypted response");
        });

        let deadline = Instant::now() + Duration::from_secs(15);
        let (offer_peer, response) = loop {
            if let Ok(result) = response_rx.try_recv() {
                break result;
            }
            if Instant::now() >= deadline {
                panic!("timed out waiting for {label} response");
            }
            match self.control_rx.try_recv() {
                Ok(message) => {
                    handle_control_message(
                        &mut self.daemon,
                        &mut self.state,
                        &self.terminal_path,
                        &self.transport_handle,
                        self.control_tx.clone(),
                        message,
                    );
                }
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                    panic!("control channel closed during {label}");
                }
            }
        };
        worker.join().expect("request worker joins");
        peer.offer_peer = Some(offer_peer);
        response
    }

    pub(crate) fn wait_for_host_event(
        &mut self,
        peer: &mut LiveSignaledPeer,
        label: &str,
    ) -> DaemonEvent {
        let key = peer.stream_key.clone();
        let mut offer_peer = peer
            .offer_peer
            .take()
            .expect("offer peer available for host event");
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let offer_handle = peer.offer_runtime.handle().clone();
        let worker = thread::spawn(move || {
            let event = offer_handle.block_on(offer_peer.next_host_event(&key));
            response_tx
                .send((offer_peer, event))
                .expect("return host event");
        });

        let deadline = Instant::now() + Duration::from_secs(15);
        let (offer_peer, event) = loop {
            if let Ok(result) = response_rx.try_recv() {
                break result;
            }
            if Instant::now() >= deadline {
                panic!("timed out waiting for {label} host event");
            }
            match self.control_rx.try_recv() {
                Ok(message) => {
                    handle_control_message(
                        &mut self.daemon,
                        &mut self.state,
                        &self.terminal_path,
                        &self.transport_handle,
                        self.control_tx.clone(),
                        message,
                    );
                }
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                    panic!("control channel closed during {label}");
                }
            }
        };
        worker.join().expect("host event worker joins");
        peer.offer_peer = Some(offer_peer);
        event
    }

    pub(crate) fn enable_event_plane_producer(&mut self) {
        let producer_src =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/event-plane-producer");
        let producer_dir = self.data_directory.join("event-plane-producer");
        copy_dir_all(&producer_src, &producer_dir);
        rewrite_package_source_path(&producer_dir);
        let enabled = self
            .control_request(DaemonRequest::EnablePackageLocalPath { path: producer_dir })
            .expect("enable producer");
        assert_eq!(
            enabled.kind,
            botster_hub_client::DaemonResponseKind::PackageDecision
        );
    }

    pub(crate) fn emit_sample_ready(&mut self, token: &str) {
        let emitted = self
            .control_request(DaemonRequest::PluginMcpCallTool {
                name: "event_plane.emit_ready".to_string(),
                arguments: serde_json::json!({ "token": token }),
            })
            .expect("emit ready");
        assert_eq!(
            emitted.kind,
            botster_hub_client::DaemonResponseKind::PluginMcpToolResult
        );
    }

    pub(crate) fn ensure_webrtc_adapter_hello(&mut self, peer: &mut LiveSignaledPeer) {
        if self
            .state
            .pending_runtime
            .webrtc_is_admitted(&peer.grant_id)
        {
            return;
        }
        let _ = self.hello_on_peer(
            peer,
            DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility:
                    botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(
                    ),
                terminal_compatibility: None,
            },
        );
    }

    pub(crate) fn hello_on_peer(
        &mut self,
        peer: &mut LiveSignaledPeer,
        hello: DaemonHello,
    ) -> DaemonHelloAck {
        let key = peer.stream_key.clone();
        let mut offer_peer = peer
            .offer_peer
            .take()
            .expect("offer peer available for hello");
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let offer_handle = peer.offer_runtime.handle().clone();
        let worker = thread::spawn(move || {
            let ack = offer_handle.block_on(offer_peer.encrypted_hello(&key, &hello));
            response_tx
                .send((offer_peer, ack))
                .expect("return encrypted hello ack");
        });

        let deadline = Instant::now() + Duration::from_secs(15);
        let (offer_peer, ack) = loop {
            if let Ok(result) = response_rx.try_recv() {
                break result;
            }
            if Instant::now() >= deadline {
                panic!("timed out waiting for DataChannel HelloAck");
            }
            match self.control_rx.try_recv() {
                Ok(message) => {
                    handle_control_message(
                        &mut self.daemon,
                        &mut self.state,
                        &self.terminal_path,
                        &self.transport_handle,
                        self.control_tx.clone(),
                        message,
                    );
                }
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                    panic!("control channel closed during hello");
                }
            }
        };
        worker.join().expect("hello worker joins");
        peer.offer_peer = Some(offer_peer);
        ack
    }

    pub(crate) fn bind_reserved_on_peer(&mut self, peer: &mut LiveSignaledPeer, label: &str) {
        let key = peer.stream_key.clone();
        let hello = DaemonHello {
            protocol: PROTOCOL.to_string(),
            compatibility: DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
            terminal_compatibility: None,
        };
        let mut offer_peer = peer
            .offer_peer
            .take()
            .expect("offer peer available for reserved-channel bind");
        let reserved_label = label.to_string();
        let (response_tx, response_rx) = std::sync::mpsc::channel();
        let offer_handle = peer.offer_runtime.handle().clone();
        let worker = thread::spawn(move || {
            offer_handle.block_on(offer_peer.open_reserved_terminal(&key, &reserved_label, &hello));
            response_tx
                .send(offer_peer)
                .expect("return reserved-channel offer peer");
        });

        let deadline = Instant::now() + Duration::from_secs(15);
        let offer_peer = loop {
            if let Ok(result) = response_rx.try_recv() {
                break result;
            }
            if Instant::now() >= deadline {
                panic!("timed out waiting for reserved-channel bind");
            }
            match self.control_rx.try_recv() {
                Ok(message) => {
                    handle_control_message(
                        &mut self.daemon,
                        &mut self.state,
                        &self.terminal_path,
                        &self.transport_handle,
                        self.control_tx.clone(),
                        message,
                    );
                }
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                    panic!("control channel closed during reserved-channel bind");
                }
            }
        };
        worker.join().expect("reserved-channel worker joins");
        peer.offer_peer = Some(offer_peer);
    }

    pub(crate) fn wait_until_adapter_bound(&mut self, session_id: &str, subscription_id: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if self
                .state
                .pending_runtime
                .is_adapter_bound(session_id, subscription_id)
            {
                return;
            }
            if Instant::now() >= deadline {
                panic!(
                    "timed out waiting for reserved-channel adapter bind session={session_id} subscription={subscription_id}"
                );
            }
            match self.control_rx.try_recv() {
                Ok(message) => {
                    handle_control_message(
                        &mut self.daemon,
                        &mut self.state,
                        &self.terminal_path,
                        &self.transport_handle,
                        self.control_tx.clone(),
                        message,
                    );
                }
                Err(tokio_mpsc::error::TryRecvError::Empty) => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                    panic!("control channel closed while waiting for reserved-channel bind");
                }
            }
        }
    }

    pub(crate) fn subscribe_entities(
        &mut self,
        peer: &mut LiveSignaledPeer,
        subscription_id: &str,
    ) -> DaemonResponse {
        self.request_on_peer(
            peer,
            DaemonRequest::SubscribeEntities {
                entity_type: "session".to_string(),
                subscription_id: subscription_id.to_string(),
            },
            "SubscribeEntities",
        )
    }

    pub(crate) fn spawn_and_attach_on_peer(
        &mut self,
        peer: &mut LiveSignaledPeer,
        session_id: &str,
        subscription_id: &str,
    ) {
        // Hold spawn capture lock for the full baseline → Spawn → census window so
        // process-global "new pid" adoption cannot pick a sibling test's worker.
        let _spawn_capture = spawn_capture_lock();
        // Baseline MUST be taken before Spawn returns a worker-backed session.
        // Census matches true worker binaries only (not hub --session-worker-bin args).
        let workers_before_spawn = session_worker_identities();
        let data_dir = self.data_directory.clone();
        let spawn = self.request_on_peer(
                peer,
                DaemonRequest::Spawn {
                    session_id: session_id.to_string(),
                    command: "printf 'webrtc-attach-ready\\n'; while IFS= read -r line; do printf 'a:%s\\n' \"$line\"; done".to_string(),
                },
                "Spawn",
            );
        assert_eq!(
            spawn.kind,
            botster_hub_client::DaemonResponseKind::Spawned,
            "spawn over local WebRTC must succeed for attach proof: {:?}",
            spawn.error
        );
        // Arm panic-safe cleanup immediately after Spawn readiness.
        self.owned_sessions.push(session_id.to_string());

        let before_pids: BTreeSet<u32> = workers_before_spawn
            .iter()
            .map(|worker| worker.pid)
            .collect();
        // Never adopt host-global "any new pid" — other pipeline worktrees spawn workers
        // concurrently. Require this worktree's session-worker executable path, then prefer
        // data-dir attribution when available. No fallback to foreign executables.
        wait_until(
            Instant::now() + Duration::from_secs(5),
            || {
                session_worker_identities().into_iter().any(|worker| {
                    !before_pids.contains(&worker.pid)
                        && process_is_alive(worker.pid)
                        && worker.executable_from_this_worktree()
                })
            },
            "live this-worktree session-worker after Spawn",
        );
        let live_ours: Vec<OwnedWorkerIdentity> = session_worker_identities()
            .into_iter()
            .filter(|worker| {
                !before_pids.contains(&worker.pid)
                    && process_is_alive(worker.pid)
                    && worker.executable_from_this_worktree()
            })
            .collect();
        let owned_by_dir: Vec<OwnedWorkerIdentity> = live_ours
            .iter()
            .filter(|worker| worker.belongs_to_data_dir(&data_dir))
            .cloned()
            .collect();
        self.owned_workers = if !owned_by_dir.is_empty() {
            owned_by_dir
        } else {
            // Core control sockets may sit outside hub data_dir; still require this
            // worktree's executable (never a foreign ticket's worker binary).
            live_ours
        };
        assert!(
            !self.owned_workers.is_empty(),
            "spawn must observe a live botster-session-worker from this worktree after Spawn; baseline_before={before_pids:?} data_dir={} worktree={}",
            data_dir.display(),
            env!("CARGO_MANIFEST_DIR")
        );
        assert!(
            self.owned_workers
                .iter()
                .all(|worker| worker.executable_from_this_worktree()),
            "owned workers must not include foreign worktree executables: {:?}",
            self.owned_workers
        );
        // Capture worker + descendant tree (shell children). Do not treat every
        // ambient process-group member as owned — workers often share a pgid with
        // the hub daemon, and mass-killing that group would terminate the harness.
        thread::sleep(Duration::from_millis(50));
        // Drop any pid that died during settle (should not happen for our worker under lock).
        self.owned_workers
            .retain(|worker| process_is_alive(worker.pid));
        assert!(
            !self.owned_workers.is_empty(),
            "owned worker died during readiness settle; data_dir={}",
            data_dir.display()
        );
        for worker in &mut self.owned_workers {
            worker.group_member_pids = worker_owned_process_tree(worker.pid);
            assert!(
                process_is_alive(worker.pid),
                "owned worker pid must be live at readiness: {worker:?}"
            );
            assert!(
                !worker.control_socket.as_os_str().is_empty(),
                "owned worker must expose a control socket path: {worker:?}"
            );
            assert!(
                worker.pgid > 0,
                "owned worker must expose a process group id: {worker:?}"
            );
            assert!(
                worker.group_member_pids.contains(&worker.pid),
                "captured process tree must include the worker pid: {worker:?}"
            );
            // Process-group evidence: at least the worker is a live member of its pgid.
            assert!(
                live_pids_in_process_group(worker.pgid).contains(&worker.pid),
                "worker pid must appear in its process group census at readiness: {worker:?}"
            );
        }

        let attach = self.request_on_peer(
            peer,
            DaemonRequest::Attach {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            },
            "Attach",
        );
        assert_eq!(
            attach.kind,
            botster_hub_client::DaemonResponseKind::TerminalReservation,
            "attach over local WebRTC must succeed"
        );
        let reservation = attach
            .terminal_reservation
            .as_ref()
            .expect("WebRTC Attach must return a reservation body")
            .clone();
        self.bind_reserved_on_peer(peer, &reservation.label);
        self.wait_until_adapter_bound(session_id, subscription_id);
        assert!(
            self.state
                .pending_runtime
                .active_subscriptions
                .get(session_id)
                .is_some_and(|subs| subs.contains(subscription_id)),
            "daemon must track active attach subscription after production Attach"
        );
        assert!(
            self.state.lifecycle_counters.live_attach_subscriptions >= 1,
            "live attach counter must reflect production Attach"
        );
        assert!(
            self.list_session_lifecycle(session_id).is_some(),
            "spawned session must be listed after attach readiness"
        );
    }

    pub(crate) fn cleanup(mut self) {
        self.shutdown_owned_sessions()
            .expect("owned sessions must shut down and remove cleanly");
        self.daemon.local_webrtc().stop_all();
        self.daemon.stop();
        let _ = std::fs::remove_dir_all(&self.data_directory);
        // Drop runs after this; sessions_cleaned prevents double work.
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedWorkerIdentity {
    pub(crate) pid: u32,
    pub(crate) pgid: u32,
    pub(crate) control_socket: PathBuf,
    /// Full `ps` command remainder after pid/pgid (used for harness data-dir matching).
    pub(crate) command: String,
    /// Exact live PIDs observed in this worker's process group at Spawn readiness.
    /// Absence proof is over this captured set, not the ambient group forever
    /// (workers may share a pgid with hub/test processes on some platforms).
    pub(crate) group_member_pids: Vec<u32>,
}

impl OwnedWorkerIdentity {
    pub(crate) fn is_fully_gone(&self) -> bool {
        if process_is_alive(self.pid) {
            return false;
        }
        if !self.control_socket.as_os_str().is_empty() && self.control_socket.exists() {
            return false;
        }
        // Prove the readiness-captured group members exited (session worker + shell).
        !self
            .group_member_pids
            .iter()
            .any(|pid| process_is_alive(*pid))
    }

    pub(crate) fn residual_group_members(&self) -> Vec<u32> {
        self.group_member_pids
            .iter()
            .copied()
            .filter(|pid| process_is_alive(*pid))
            .collect()
    }

    pub(crate) fn socket_under_data_dir(&self, data_dir: &Path) -> bool {
        if self.control_socket.as_os_str().is_empty() {
            return false;
        }
        self.control_socket.starts_with(data_dir)
            || self
                .control_socket
                .canonicalize()
                .ok()
                .zip(data_dir.canonicalize().ok())
                .is_some_and(|(socket, root)| socket.starts_with(root))
    }

    /// True when this worker was started for `data_dir` (socket path or any argv token).
    /// Prefer this over process-global "first new pid" adoption under parallel tests.
    pub(crate) fn belongs_to_data_dir(&self, data_dir: &Path) -> bool {
        if self.socket_under_data_dir(data_dir) {
            return true;
        }
        let dir = data_dir.to_string_lossy();
        if dir.is_empty() {
            return false;
        }
        self.command.contains(dir.as_ref())
            || data_dir
                .canonicalize()
                .ok()
                .is_some_and(|canon| self.command.contains(&canon.to_string_lossy().to_string()))
    }

    /// True when argv0 / command is this hub worktree's `botster-session-worker` binary.
    /// Rejects workers from other pipeline tickets / worktrees on the same host.
    pub(crate) fn executable_from_this_worktree(&self) -> bool {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let argv0 = self.command.split_whitespace().next().unwrap_or("");
        if argv0.is_empty() {
            return false;
        }
        let exe = Path::new(argv0);
        if exe.starts_with(root) {
            return true;
        }
        match (exe.canonicalize(), root.canonicalize()) {
            (Ok(exe), Ok(root)) => exe.starts_with(root),
            _ => self.command.contains(&root.display().to_string()),
        }
    }
}

pub(crate) fn session_worker_identities() -> Vec<OwnedWorkerIdentity> {
    // Portable census: true worker binaries only. Hub processes carry
    // `--session-worker-bin .../botster-session-worker` in argv and must not match.
    let Ok(output) = std::process::Command::new("ps")
        .args(["-axo", "pid=,pgid=,command="])
        .output()
    else {
        return Vec::new();
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let mut parts = line.split_whitespace();
            let pid = parts.next()?.parse::<u32>().ok()?;
            let pgid = parts.next()?.parse::<u32>().ok()?;
            let argv0 = parts.next()?;
            // Match the worker binary path/name, not a hub arg that mentions it.
            let is_worker_binary = Path::new(argv0)
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "botster-session-worker");
            if !is_worker_binary {
                return None;
            }
            let rest = parts.collect::<Vec<_>>().join(" ");
            let command = format!("{argv0} {rest}");
            let control_socket = std::iter::once(argv0)
                .chain(rest.split_whitespace())
                .skip_while(|token| *token != "--control-socket")
                .nth(1)
                .map(PathBuf::from)
                .unwrap_or_default();
            Some(OwnedWorkerIdentity {
                pid,
                pgid,
                control_socket,
                command,
                // Refined after settle to worker + descendant tree.
                group_member_pids: vec![pid],
            })
        })
        .collect()
}

/// Worker PID plus live descendants (shell children under the session worker).
pub(crate) fn worker_owned_process_tree(root_pid: u32) -> Vec<u32> {
    let Ok(output) = std::process::Command::new("ps")
        .args(["-axo", "pid=,ppid="])
        .output()
    else {
        return vec![root_pid];
    };
    let mut parent_of: Vec<(u32, u32)> = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut parts = line.split_whitespace();
        let Some(pid) = parts.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let Some(ppid) = parts.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        parent_of.push((pid, ppid));
    }
    let mut owned = vec![root_pid];
    let mut changed = true;
    while changed {
        changed = false;
        for &(pid, ppid) in &parent_of {
            if owned.contains(&ppid) && !owned.contains(&pid) && process_is_alive(pid) {
                owned.push(pid);
                changed = true;
            }
        }
    }
    owned
}

pub(crate) fn process_is_alive(pid: u32) -> bool {
    // Prefer libc-free existence check via kill -0 with stderr discarded.
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

pub(crate) fn live_pids_in_process_group(pgid: u32) -> Vec<u32> {
    if pgid == 0 {
        return Vec::new();
    }
    let Ok(output) = std::process::Command::new("ps")
        .args(["-axo", "pid=,pgid="])
        .output()
    else {
        return Vec::new();
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let pid = parts.next()?.parse::<u32>().ok()?;
            let group = parts.next()?.parse::<u32>().ok()?;
            if group == pgid && process_is_alive(pid) {
                Some(pid)
            } else {
                None
            }
        })
        .collect()
}

pub(crate) fn signal_pid(pid: u32, signal: &str) {
    let _ = std::process::Command::new("kill")
        .args([signal, &pid.to_string()])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

/// Kill exact worker PID + readiness-captured descendants, then unlink control socket.
pub(crate) fn reap_owned_worker(worker: &OwnedWorkerIdentity) {
    let self_pid = std::process::id();
    // Prefer killing children first, then the worker root.
    let mut targets: Vec<u32> = worker
        .group_member_pids
        .iter()
        .copied()
        .filter(|pid| *pid != worker.pid)
        .collect();
    targets.push(worker.pid);
    for pid in &targets {
        if *pid == self_pid {
            continue;
        }
        if process_is_alive(*pid) {
            signal_pid(*pid, "-TERM");
        }
    }
    thread::sleep(Duration::from_millis(50));
    for pid in &targets {
        if *pid == self_pid {
            continue;
        }
        if process_is_alive(*pid) {
            signal_pid(*pid, "-KILL");
        }
    }
    if !worker.control_socket.as_os_str().is_empty() && worker.control_socket.exists() {
        let _ = std::fs::remove_file(&worker.control_socket);
    }
}

pub(crate) fn wait_for_owned_workers_gone(workers: &[OwnedWorkerIdentity], deadline: Instant) {
    // Observation only: do not hard-reap here. Harness kill/unlink belongs solely
    // inside shutdown_owned_sessions as post-error hygiene, never as a greenwash.
    wait_until(
        deadline,
        || workers.iter().all(|worker| worker.is_fully_gone()),
        &format!("owned workers to fully exit after production cleanup: {workers:?}"),
    );
}

pub(crate) fn take_last_session_cleanup_error() -> Option<String> {
    LAST_SESSION_CLEANUP_ERROR.with(|slot| slot.borrow_mut().take())
}

pub(crate) struct LiveSignaledPeer {
    pub(crate) grant_id: String,
    pub(crate) stream_key: AesGcmKey,
    pub(crate) offer_peer: Option<TestOfferPeer>,
    pub(crate) offer_runtime: tokio::runtime::Runtime,
}

impl LiveSignaledPeer {
    pub(crate) fn close_offer(mut self) {
        if let Some(offer_peer) = self.offer_peer.take() {
            let _ = self.offer_runtime.block_on(offer_peer.peer.close());
        }
    }

    pub(crate) fn enable_host_events(&mut self) {
        self.offer_peer
            .as_mut()
            .expect("offer peer available")
            .enable_host_events();
    }
}

pub(crate) fn copy_dir_all(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create dest");
    for entry in std::fs::read_dir(from).expect("read src") {
        let entry = entry.expect("entry");
        let dest = to.join(entry.file_name());
        if entry.file_type().expect("ty").is_dir() {
            copy_dir_all(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).expect("copy file");
        }
    }
}

pub(crate) fn rewrite_package_source_path(package_dir: &Path) {
    let manifest_path = package_dir.join("botster-package.json");
    let mut value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    value["source"]["path"] = serde_json::json!(package_dir.display().to_string());
    std::fs::write(
        manifest_path,
        serde_json::to_string_pretty(&value).expect("serialize"),
    )
    .expect("write manifest");
}

pub(crate) fn read_terminal_record(path: &Path) -> LocalWebrtcSenderTerminalRecord {
    let bytes = std::fs::read(path).expect("read terminal record");
    serde_json::from_slice(&bytes).expect("parse terminal record")
}

pub(crate) fn receive_test_runtime_message(
    receiver: &mut tokio_mpsc::Receiver<ControlMessage>,
) -> ControlMessage {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build bounded WebRTC test receive runtime");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("timed out waiting for WebRTC runtime message")
            .expect("WebRTC runtime sender remains live")
    })
}
