//! Local WebRTC signaling and DataChannel adapter for installed browser packages.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::mpsc::{self, Sender};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use botster_core::{AesGcmEnvelope, AesGcmKey, decrypt_aes_gcm, encrypt_aes_gcm};
use botster_hub_client::{
    DaemonDiagnostic, DaemonLocalWebrtcAnswer, DaemonLocalWebrtcBootstrap,
    DaemonLocalWebrtcResponseChunk, DaemonRequest, DaemonResponse, LOCAL_WEBRTC_MAX_FRAME_BYTES,
    LOCAL_WEBRTC_MAX_RESPONSE_BYTES, LOCAL_WEBRTC_RESPONSE_CHUNK_VERSION,
};
use serde_json::Value;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{Runtime, Sender as AsyncSender, channel, default_runtime, timeout};

use crate::daemon_transport::ControlMessage;

const GRANT_TTL_SECONDS: u64 = 120;
const WEBRTC_SIGNAL_OPERATION: &str = "local_webrtc_signal";
// The current Rust WebRTC peer's message receive path is bounded at 16 KiB;
// 12 KiB leaves transport and JSON framing headroom for every first-party peer.
const LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES: usize = 12 * 1024;
const LOCAL_WEBRTC_PENDING_REQUESTS: usize = 16;
const LOCAL_WEBRTC_FLOW_CONTROL_DEADLINE: Duration = Duration::from_secs(5);
const LOCAL_WEBRTC_EVENT_PROBE: Duration = Duration::ZERO;
const LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW: u32 = LOCAL_WEBRTC_MAX_FRAME_BYTES as u32;
const LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH: u32 = (LOCAL_WEBRTC_MAX_FRAME_BYTES * 2) as u32;

#[async_trait]
trait LocalWebrtcDataChannel: Send + Sync {
    async fn local_set_buffered_amount_low_threshold(&self, threshold: u32) -> Result<(), String>;
    async fn local_set_buffered_amount_high_threshold(&self, threshold: u32) -> Result<(), String>;
    async fn local_send_text(&self, text: &str) -> Result<(), String>;
    async fn local_poll(&self) -> Option<DataChannelEvent>;
    async fn local_close(&self) -> Result<(), String>;
}

#[async_trait]
impl<T> LocalWebrtcDataChannel for T
where
    T: DataChannel + ?Sized,
{
    async fn local_set_buffered_amount_low_threshold(&self, threshold: u32) -> Result<(), String> {
        self.set_buffered_amount_low_threshold(threshold)
            .await
            .map_err(|error| error.to_string())
    }

    async fn local_set_buffered_amount_high_threshold(&self, threshold: u32) -> Result<(), String> {
        self.set_buffered_amount_high_threshold(threshold)
            .await
            .map_err(|error| error.to_string())
    }

    async fn local_send_text(&self, text: &str) -> Result<(), String> {
        self.send_text(text)
            .await
            .map_err(|error| error.to_string())
    }

    async fn local_poll(&self) -> Option<DataChannelEvent> {
        self.poll().await
    }

    async fn local_close(&self) -> Result<(), String> {
        self.close().await.map_err(|error| error.to_string())
    }
}
/// Ephemeral local WebRTC admission and peer registry.
#[derive(Default)]
pub struct LocalWebrtcTransport {
    grants: BTreeMap<String, LocalWebrtcGrant>,
    peers: BTreeMap<String, Arc<dyn PeerConnection>>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl LocalWebrtcTransport {
    /// Mint a local, single-use bootstrap grant for the installed botster-web app.
    pub fn issue_botster_web_bootstrap(
        &mut self,
        entrypoint_id: &str,
        environment: &mut BTreeMap<String, String>,
    ) -> LocalWebrtcResult<Option<DaemonLocalWebrtcBootstrap>> {
        if entrypoint_id != "web-client" {
            return Ok(None);
        }
        let expected_origin = expected_origin(environment);
        let bootstrap = self.issue_bootstrap("botster-web", entrypoint_id, &expected_origin)?;
        environment.insert(
            "BOTSTER_LOCAL_WEBRTC_GRANT_ID".to_string(),
            bootstrap.grant_id.clone(),
        );
        environment.insert(
            "BOTSTER_LOCAL_WEBRTC_GRANT_SECRET".to_string(),
            bootstrap.grant_secret.clone(),
        );
        environment.insert(
            "BOTSTER_LOCAL_WEBRTC_SIGNALING_TRANSPORT".to_string(),
            "daemon_request".to_string(),
        );
        environment.insert(
            "BOTSTER_LOCAL_WEBRTC_EXPECTED_ORIGIN".to_string(),
            expected_origin.clone(),
        );
        Ok(Some(bootstrap))
    }

    /// Mint a local, single-use bootstrap grant bound to an already-running app origin.
    pub fn issue_bootstrap(
        &mut self,
        package_name: &str,
        entrypoint_id: &str,
        expected_origin: &str,
    ) -> LocalWebrtcResult<DaemonLocalWebrtcBootstrap> {
        let now = now_seconds();
        self.prune_expired_grants(now);
        let grant_id = random_token("grant")?;
        let grant_secret = random_secret_token()?;
        let bootstrap = DaemonLocalWebrtcBootstrap {
            grant_id: grant_id.clone(),
            grant_secret: grant_secret.clone(),
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
            expected_origin: expected_origin.to_string(),
            expires_at: now + GRANT_TTL_SECONDS,
            signaling_transport: "daemon_request".to_string(),
            data_plane: "webrtc_data_channel".to_string(),
            ordered: true,
            max_retransmits: None,
            max_packet_lifetime_ms: None,
        };
        self.grants.insert(
            grant_id.clone(),
            LocalWebrtcGrant {
                grant_id,
                grant_secret,
                expected_origin: expected_origin.to_string(),
                expires_at: bootstrap.expires_at,
                redeemed: false,
            },
        );
        Ok(bootstrap)
    }

    /// Redeem one grant and create a WebRTC answer for the supplied offer.
    pub(crate) fn signal(
        &mut self,
        request: LocalWebrtcSignalRequest,
        runtime_tx: Sender<ControlMessage>,
    ) -> LocalWebrtcResult<DaemonLocalWebrtcAnswer> {
        let Some(grant) = self.grants.get_mut(&request.grant_id) else {
            return Err(LocalWebrtcError::MissingGrant);
        };
        grant.validate(&request)?;
        grant.redeemed = true;
        let grant_id = grant.grant_id.clone();

        let answer = self
            .runtime()?
            .block_on(answer_offer(request, runtime_tx))?;
        self.peers.insert(grant_id.clone(), answer.peer);
        Ok(DaemonLocalWebrtcAnswer {
            grant_id,
            answer: answer.answer,
            diagnostics: vec![DaemonDiagnostic::connected(WEBRTC_SIGNAL_OPERATION)],
        })
    }

    /// Close all active local peers. Used during daemon shutdown.
    pub fn stop_all(&mut self) {
        let peers = std::mem::take(&mut self.peers);
        self.grants.clear();
        if let Some(runtime) = self.runtime.take() {
            for peer in peers.into_values() {
                let _ = runtime.block_on(peer.close());
            }
        }
    }

    /// Forget one active peer after its DataChannel or peer connection closes.
    pub(crate) fn remove_peer(&mut self, grant_id: &str) {
        self.peers.remove(grant_id);
    }

    fn runtime(&mut self) -> LocalWebrtcResult<&tokio::runtime::Runtime> {
        if self.runtime.is_none() {
            self.runtime = Some(
                tokio::runtime::Builder::new_multi_thread()
                    .thread_name("botster-local-webrtc")
                    .enable_all()
                    .build()
                    .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?,
            );
        }
        Ok(self.runtime.as_ref().expect("runtime was initialized"))
    }

    fn prune_expired_grants(&mut self, now: u64) {
        self.grants.retain(|_, grant| grant.expires_at > now);
    }
}

pub struct LocalWebrtcSignalRequest {
    pub grant_id: String,
    pub grant_secret: String,
    pub origin: String,
    pub offer: Value,
}

struct LocalWebrtcGrant {
    grant_id: String,
    grant_secret: String,
    expected_origin: String,
    expires_at: u64,
    redeemed: bool,
}

impl LocalWebrtcGrant {
    fn validate(&self, request: &LocalWebrtcSignalRequest) -> LocalWebrtcResult<()> {
        if self.redeemed {
            return Err(LocalWebrtcError::RedeemedGrant);
        }
        if self.expires_at <= now_seconds() {
            return Err(LocalWebrtcError::ExpiredGrant);
        }
        if self.grant_secret != request.grant_secret {
            return Err(LocalWebrtcError::SecretMismatch);
        }
        if self.expected_origin != request.origin {
            return Err(LocalWebrtcError::OriginMismatch);
        }
        Ok(())
    }
}

struct LocalWebrtcAnswer {
    answer: Value,
    peer: Arc<dyn PeerConnection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalWebrtcAttachedSubscription {
    pub session_id: String,
    pub subscription_id: String,
}

enum LocalWebrtcAttachedSubscriptionChange {
    Attach(LocalWebrtcAttachedSubscription),
    Detach(LocalWebrtcAttachedSubscription),
}

impl LocalWebrtcAttachedSubscriptionChange {
    fn from_request(request: &DaemonRequest) -> Option<Self> {
        match request {
            DaemonRequest::Attach {
                session_id,
                subscription_id,
            } => Some(Self::Attach(LocalWebrtcAttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            DaemonRequest::Detach {
                session_id,
                subscription_id,
            } => Some(Self::Detach(LocalWebrtcAttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            _ => None,
        }
    }
}

struct LocalWebrtcPeerState {
    grant_id: String,
    runtime_tx: Sender<ControlMessage>,
    attached_subscriptions: Mutex<Vec<LocalWebrtcAttachedSubscription>>,
    cleanup_sent: AtomicBool,
}

enum PendingLocalWebrtcRequest {
    Request(Box<DaemonRequest>),
    QueueOverflow(usize),
}

fn pop_pending_request(
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
) -> Option<PendingLocalWebrtcRequest> {
    let pending = pending_requests.pop_front()?;
    let PendingLocalWebrtcRequest::QueueOverflow(count) = pending else {
        return Some(pending);
    };
    if count > 1 {
        pending_requests.push_front(PendingLocalWebrtcRequest::QueueOverflow(count - 1));
    }
    Some(PendingLocalWebrtcRequest::QueueOverflow(1))
}

impl LocalWebrtcPeerState {
    fn new(grant_id: String, runtime_tx: Sender<ControlMessage>) -> Self {
        Self {
            grant_id,
            runtime_tx,
            attached_subscriptions: Mutex::new(Vec::new()),
            cleanup_sent: AtomicBool::new(false),
        }
    }

    fn apply_subscription_change(&self, change: Option<LocalWebrtcAttachedSubscriptionChange>) {
        let Some(change) = change else {
            return;
        };
        let mut attached_subscriptions = self
            .attached_subscriptions
            .lock()
            .expect("local WebRTC peer subscription mutex");
        match change {
            LocalWebrtcAttachedSubscriptionChange::Attach(subscription) => {
                if !attached_subscriptions.contains(&subscription) {
                    attached_subscriptions.push(subscription);
                }
            }
            LocalWebrtcAttachedSubscriptionChange::Detach(subscription) => {
                attached_subscriptions.retain(|attached| attached != &subscription);
            }
        }
    }

    fn cleanup_once(&self) {
        if self.cleanup_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        let attached_subscriptions = self
            .attached_subscriptions
            .lock()
            .expect("local WebRTC peer subscription mutex")
            .clone();
        let _ = self.runtime_tx.send(ControlMessage::LocalWebrtcPeerClosed {
            grant_id: self.grant_id.clone(),
            attached_subscriptions,
        });
    }
}

#[derive(Clone)]
struct LocalWebrtcHandler {
    stream_key: AesGcmKey,
    runtime: Arc<dyn Runtime>,
    peer_state: Arc<LocalWebrtcPeerState>,
    gather_complete_tx: AsyncSender<()>,
}

#[async_trait]
impl PeerConnectionEventHandler for LocalWebrtcHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_complete_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if matches!(
            state,
            RTCPeerConnectionState::Disconnected
                | RTCPeerConnectionState::Failed
                | RTCPeerConnectionState::Closed
        ) {
            let _ = self.gather_complete_tx.try_send(());
            self.peer_state.cleanup_once();
        }
    }

    async fn on_data_channel(&self, data_channel: Arc<dyn DataChannel>) {
        let peer_state = self.peer_state.clone();
        let runtime_tx = peer_state.runtime_tx.clone();
        let stream_key = self.stream_key.clone();
        self.runtime.spawn(Box::pin(async move {
            if let Err(error) = data_channel
                .local_set_buffered_amount_low_threshold(LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW)
                .await
            {
                eprintln!("local WebRTC low-water threshold setup failed: {error}");
                let _ = data_channel.local_close().await;
                peer_state.cleanup_once();
                return;
            }
            if let Err(error) = data_channel
                .local_set_buffered_amount_high_threshold(LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH)
                .await
            {
                eprintln!("local WebRTC high-water threshold setup failed: {error}");
                let _ = data_channel.local_close().await;
                peer_state.cleanup_once();
                return;
            }

            let mut pending_requests = VecDeque::new();
            let mut open = true;
            while open {
                let pending = if let Some(request) = pop_pending_request(&mut pending_requests) {
                    request
                } else {
                    match data_channel.poll().await {
                        Some(DataChannelEvent::OnMessage(message)) => {
                            let Some(request) =
                                decrypt_daemon_request(&stream_key, message.data.as_ref())
                            else {
                                break;
                            };
                            PendingLocalWebrtcRequest::Request(Box::new(request))
                        }
                        Some(DataChannelEvent::OnClose | DataChannelEvent::OnError) | None => break,
                        Some(_) => continue,
                    }
                };

                let request = match pending {
                    PendingLocalWebrtcRequest::Request(request) => request,
                    PendingLocalWebrtcRequest::QueueOverflow(_) => {
                        let response = queued_request_overflow_response();
                        let Ok(frames) = framed_daemon_response(&stream_key, &response) else {
                            break;
                        };
                        open = send_response_frames(
                            data_channel.as_ref(),
                            &stream_key,
                            &frames,
                            &mut pending_requests,
                        )
                        .await;
                        continue;
                    }
                };

                let subscription_change =
                    LocalWebrtcAttachedSubscriptionChange::from_request(&request);
                let (reply_tx, reply_rx) = mpsc::channel();
                if runtime_tx
                    .send(ControlMessage::Request { request, reply_tx })
                    .is_err()
                {
                    break;
                }
                let response = reply_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap_or_else(|_| {
                        Ok(response_with_diagnostic(DaemonDiagnostic::action_failure(
                            "local_webrtc_data_channel",
                            "runtime request timed out",
                        )))
                    })
                    .unwrap_or_else(|error| {
                        response_with_diagnostic(DaemonDiagnostic::action_failure(
                            "local_webrtc_data_channel",
                            error.to_string(),
                        ))
                    });
                peer_state.apply_subscription_change(subscription_change);
                let Ok(frames) = framed_daemon_response(&stream_key, &response) else {
                    break;
                };
                open = send_response_frames(
                    data_channel.as_ref(),
                    &stream_key,
                    &frames,
                    &mut pending_requests,
                )
                .await;
            }
            close_data_channel(
                data_channel.as_ref(),
                &mut pending_requests,
                peer_state.as_ref(),
            )
            .await;
        }));
    }
}

async fn close_data_channel<D>(
    data_channel: &D,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    peer_state: &LocalWebrtcPeerState,
) where
    D: LocalWebrtcDataChannel + ?Sized,
{
    pending_requests.clear();
    if let Err(error) = data_channel.local_close().await {
        eprintln!("local WebRTC data channel close failed: {error}");
    }
    peer_state.cleanup_once();
}

async fn send_response_frames<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    frames: &[String],
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
) -> bool
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    send_response_frames_with_deadline(
        data_channel,
        stream_key,
        frames,
        pending_requests,
        LOCAL_WEBRTC_FLOW_CONTROL_DEADLINE,
    )
    .await
}

async fn send_response_frames_with_deadline<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    frames: &[String],
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control_deadline: Duration,
) -> bool
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    let mut paused = false;
    let mut pause_deadline = None;

    for frame in frames {
        if let Err(error) = data_channel.local_send_text(frame).await {
            eprintln!("local WebRTC response frame send failed: {error}");
            return false;
        }

        match timeout(LOCAL_WEBRTC_EVENT_PROBE, data_channel.local_poll()).await {
            Ok(Some(event)) => {
                if !apply_data_channel_event(
                    event,
                    stream_key,
                    pending_requests,
                    &mut paused,
                    &mut pause_deadline,
                    flow_control_deadline,
                ) {
                    return false;
                }
            }
            Ok(None) => return false,
            Err(_) => {}
        }

        while paused {
            let deadline = pause_deadline.expect("paused response has a deadline");
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            match timeout(remaining, data_channel.local_poll()).await {
                Ok(Some(event)) => {
                    if !apply_data_channel_event(
                        event,
                        stream_key,
                        pending_requests,
                        &mut paused,
                        &mut pause_deadline,
                        flow_control_deadline,
                    ) {
                        return false;
                    }
                }
                Ok(None) | Err(_) => return false,
            }
        }
    }
    true
}

fn apply_data_channel_event(
    event: DataChannelEvent,
    stream_key: &AesGcmKey,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    paused: &mut bool,
    pause_deadline: &mut Option<Instant>,
    flow_control_deadline: Duration,
) -> bool {
    match event {
        DataChannelEvent::OnBufferedAmountHigh => {
            if !*paused {
                *paused = true;
                *pause_deadline = Some(Instant::now() + flow_control_deadline);
            }
            true
        }
        DataChannelEvent::OnBufferedAmountLow => {
            *paused = false;
            *pause_deadline = None;
            true
        }
        DataChannelEvent::OnMessage(message) => {
            let Some(request) = decrypt_daemon_request(stream_key, message.data.as_ref()) else {
                return false;
            };
            let request_count = pending_requests
                .iter()
                .filter(|pending| matches!(pending, PendingLocalWebrtcRequest::Request(_)))
                .count();
            if request_count >= LOCAL_WEBRTC_PENDING_REQUESTS {
                if let Some(PendingLocalWebrtcRequest::QueueOverflow(count)) = pending_requests
                    .iter_mut()
                    .find(|pending| matches!(pending, PendingLocalWebrtcRequest::QueueOverflow(_)))
                {
                    let Some(next_count) = count.checked_add(1) else {
                        return false;
                    };
                    *count = next_count;
                } else {
                    pending_requests.push_back(PendingLocalWebrtcRequest::QueueOverflow(1));
                }
                return true;
            }
            pending_requests.push_back(PendingLocalWebrtcRequest::Request(Box::new(request)));
            true
        }
        DataChannelEvent::OnClose | DataChannelEvent::OnError => false,
        _ => true,
    }
}

async fn answer_offer(
    request: LocalWebrtcSignalRequest,
    runtime_tx: Sender<ControlMessage>,
) -> LocalWebrtcResult<LocalWebrtcAnswer> {
    let runtime = default_runtime()
        .ok_or_else(|| LocalWebrtcError::Webrtc("no async runtime".to_string()))?;
    let stream_key = secret_stream_key(&request.grant_secret)?;
    let (gather_complete_tx, mut gather_complete_rx) = channel::<()>(1);
    let peer_state = Arc::new(LocalWebrtcPeerState::new(
        request.grant_id.clone(),
        runtime_tx,
    ));
    let handler = Arc::new(LocalWebrtcHandler {
        stream_key,
        runtime: runtime.clone(),
        peer_state,
        gather_complete_tx,
    });

    let peer_connection = PeerConnectionBuilder::new()
        .with_handler(handler)
        .with_runtime(runtime)
        .with_udp_addrs(vec!["127.0.0.1:0"])
        .build()
        .await
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let peer: Arc<dyn PeerConnection> = Arc::new(peer_connection);
    let offer = serde_json::from_value::<RTCSessionDescription>(request.offer)
        .map_err(|error| LocalWebrtcError::InvalidOffer(error.to_string()))?;
    peer.set_remote_description(offer)
        .await
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let answer = peer
        .create_answer(None)
        .await
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    peer.set_local_description(answer)
        .await
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let _ = timeout(Duration::from_secs(5), gather_complete_rx.recv()).await;
    let answer = peer
        .local_description()
        .await
        .ok_or_else(|| LocalWebrtcError::Webrtc("missing local description".to_string()))?;
    let answer = serde_json::to_value(answer)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    Ok(LocalWebrtcAnswer { answer, peer })
}

fn decrypt_daemon_request(key: &AesGcmKey, bytes: &[u8]) -> Option<DaemonRequest> {
    let envelope = serde_json::from_slice::<AesGcmEnvelope>(bytes).ok()?;
    let plaintext = decrypt_aes_gcm(key, &envelope).ok()?;
    serde_json::from_slice::<DaemonRequest>(&plaintext).ok()
}

fn encrypt_daemon_response(
    key: &AesGcmKey,
    response: &DaemonResponse,
) -> LocalWebrtcResult<String> {
    let plaintext = serde_json::to_vec(response)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let envelope = encrypt_aes_gcm(key, &plaintext, 1)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    serde_json::to_string(&envelope).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))
}

fn framed_daemon_response(
    key: &AesGcmKey,
    response: &DaemonResponse,
) -> LocalWebrtcResult<Vec<String>> {
    let encrypted = encrypt_daemon_response(key, response)?;
    let encrypted = if encrypted.len() > LOCAL_WEBRTC_MAX_RESPONSE_BYTES {
        encrypt_daemon_response(
            key,
            &response_with_diagnostic(DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                format!(
                    "encrypted daemon response exceeded {} byte limit",
                    LOCAL_WEBRTC_MAX_RESPONSE_BYTES
                ),
            )),
        )?
    } else {
        encrypted
    };
    let message_id = random_token("response")?;
    frame_encrypted_daemon_response(&message_id, &encrypted)
}

fn frame_encrypted_daemon_response(
    message_id: &str,
    encrypted: &str,
) -> LocalWebrtcResult<Vec<String>> {
    if encrypted.len() > LOCAL_WEBRTC_MAX_RESPONSE_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon response exceeded {LOCAL_WEBRTC_MAX_RESPONSE_BYTES} byte limit"
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
        let frame = DaemonLocalWebrtcResponseChunk {
            version: LOCAL_WEBRTC_RESPONSE_CHUNK_VERSION,
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
        let frame = DaemonLocalWebrtcResponseChunk {
            version: LOCAL_WEBRTC_RESPONSE_CHUNK_VERSION,
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

fn response_with_diagnostic(diagnostic: DaemonDiagnostic) -> DaemonResponse {
    DaemonResponse {
        kind: botster_hub_client::DaemonResponseKind::OperatorError,
        status: None,
        sessions: Vec::new(),
        session_templates: Vec::new(),
        resolved_session_template: None,
        session_context: None,
        read_screen: None,
        capture_snapshot: None,
        spawn_targets: Vec::new(),
        spawn_target_validation: None,
        worktrees: Vec::new(),
        apps: Vec::new(),
        resolved_app_launch: None,
        resolved_package_route: None,
        package_navigation: Vec::new(),
        packages: Vec::new(),
        available_packages: Vec::new(),
        install_plan: None,
        update_status: None,
        package_decision: None,
        lifecycle: Vec::new(),
        plugin_tools: Vec::new(),
        plugin_tool_result: Value::Null,
        plugin_surface: None,
        plugin_action_result: None,
        local_webrtc_bootstrap: None,
        local_webrtc_answer: None,
        events: Vec::new(),
        cleanup: None,
        coordination: None,
        error: None,
        diagnostics: vec![diagnostic],
    }
}

fn queued_request_overflow_response() -> DaemonResponse {
    response_with_diagnostic(DaemonDiagnostic::action_failure(
        "local_webrtc_data_channel",
        "inbound request queue capacity exceeded; request was rejected",
    ))
}

fn expected_origin(environment: &BTreeMap<String, String>) -> String {
    environment
        .get("BOTSTER_WEB_DOGFOOD_BRIDGE_PORT")
        .map(|port| format!("http://127.0.0.1:{port}"))
        .unwrap_or_else(|| "http://127.0.0.1".to_string())
}

fn random_token(prefix: &str) -> LocalWebrtcResult<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| LocalWebrtcError::Random(error.to_string()))?;
    Ok(format!("{prefix}-{}", hex(&bytes)))
}

fn random_secret_token() -> LocalWebrtcResult<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| LocalWebrtcError::Random(error.to_string()))?;
    Ok(format!("secret-{}", hex(&bytes)))
}

fn secret_stream_key(secret: &str) -> LocalWebrtcResult<AesGcmKey> {
    let encoded = secret
        .strip_prefix("secret-")
        .ok_or_else(|| LocalWebrtcError::Webrtc("invalid bootstrap secret".to_string()))?;
    let bytes = decode_hex(encoded)
        .ok_or_else(|| LocalWebrtcError::Webrtc("invalid bootstrap secret".to_string()))?;
    AesGcmKey::from_slice(&bytes).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))
}

fn decode_hex(encoded: &str) -> Option<Vec<u8>> {
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

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub type LocalWebrtcResult<T> = Result<T, LocalWebrtcError>;

#[derive(Debug)]
pub enum LocalWebrtcError {
    MissingGrant,
    ExpiredGrant,
    RedeemedGrant,
    SecretMismatch,
    OriginMismatch,
    InvalidOffer(String),
    Random(String),
    Webrtc(String),
}

impl fmt::Display for LocalWebrtcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingGrant => write!(formatter, "local WebRTC bootstrap grant was not found"),
            Self::ExpiredGrant => write!(formatter, "local WebRTC bootstrap grant expired"),
            Self::RedeemedGrant => write!(
                formatter,
                "local WebRTC bootstrap grant was already redeemed"
            ),
            Self::SecretMismatch => {
                write!(formatter, "local WebRTC bootstrap grant secret mismatch")
            }
            Self::OriginMismatch => write!(formatter, "local WebRTC bootstrap origin mismatch"),
            Self::InvalidOffer(error) => write!(formatter, "invalid local WebRTC offer: {error}"),
            Self::Random(error) => write!(formatter, "local WebRTC random token failed: {error}"),
            Self::Webrtc(error) => write!(formatter, "local WebRTC signaling failed: {error}"),
        }
    }
}

impl Error for LocalWebrtcError {}

#[cfg(test)]
mod tests {
    use super::*;
    use webrtc::data_channel::RTCDataChannelMessage;

    #[derive(Default)]
    struct FakeDataChannel {
        events: Mutex<VecDeque<DataChannelEvent>>,
        sent: Mutex<Vec<String>>,
        closed: AtomicBool,
    }

    #[async_trait]
    impl LocalWebrtcDataChannel for FakeDataChannel {
        async fn local_set_buffered_amount_low_threshold(
            &self,
            _threshold: u32,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn local_set_buffered_amount_high_threshold(
            &self,
            _threshold: u32,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn local_send_text(&self, text: &str) -> Result<(), String> {
            self.sent.lock().unwrap().push(text.to_string());
            Ok(())
        }

        async fn local_poll(&self) -> Option<DataChannelEvent> {
            if let Some(event) = self.events.lock().unwrap().pop_front() {
                return Some(event);
            }
            std::future::pending().await
        }

        async fn local_close(&self) -> Result<(), String> {
            self.closed.store(true, Ordering::Release);
            Ok(())
        }
    }

    fn encrypted_request_event(key: &AesGcmKey, request: &DaemonRequest) -> DataChannelEvent {
        let plaintext = serde_json::to_vec(request).unwrap();
        let envelope = encrypt_aes_gcm(key, &plaintext, 1).unwrap();
        let data = serde_json::to_vec(&envelope).unwrap();
        DataChannelEvent::OnMessage(RTCDataChannelMessage {
            is_string: true,
            data: data.as_slice().into(),
        })
    }

    #[test]
    fn response_frames_use_one_bounded_protocol_for_small_and_large_envelopes() {
        let small = frame_encrypted_daemon_response("response-small", "encrypted").unwrap();
        assert_eq!(small.len(), 1);
        let small: DaemonLocalWebrtcResponseChunk = serde_json::from_str(&small[0]).unwrap();
        assert_eq!(small.version, LOCAL_WEBRTC_RESPONSE_CHUNK_VERSION);
        assert_eq!(small.message_id, "response-small");
        assert_eq!(small.chunk_index, 0);
        assert_eq!(small.chunk_count, 1);
        assert_eq!(small.total_bytes, 9);
        assert_eq!(small.payload, "encrypted");

        let encrypted = "a".repeat(256 * 1024 + 1);
        let frames = frame_encrypted_daemon_response("response-large", &encrypted).unwrap();
        assert!(frames.len() > 1);
        let chunks = frames
            .iter()
            .map(|frame| {
                assert!(frame.len() < LOCAL_WEBRTC_MAX_FRAME_BYTES);
                serde_json::from_str::<DaemonLocalWebrtcResponseChunk>(frame).unwrap()
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
        let encrypted = "a".repeat(LOCAL_WEBRTC_MAX_RESPONSE_BYTES + 1);
        let error = frame_encrypted_daemon_response("response-over-budget", &encrypted)
            .expect_err("over-budget response must fail before framing");
        assert!(error.to_string().contains("exceeded 16777216 byte limit"));
    }

    #[test]
    fn over_budget_response_is_replaced_before_any_rejected_payload_is_framed() {
        let key = AesGcmKey::from_slice(&[7; 32]).unwrap();
        let mut response = response_with_diagnostic(DaemonDiagnostic::connected("fixture"));
        response.plugin_tool_result = Value::String("x".repeat(LOCAL_WEBRTC_MAX_RESPONSE_BYTES));

        let frames = framed_daemon_response(&key, &response).unwrap();
        assert_eq!(frames.len(), 1);
        let chunk: DaemonLocalWebrtcResponseChunk = serde_json::from_str(&frames[0]).unwrap();
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
    fn flow_control_pause_deadline_is_bounded_and_not_reset_by_other_events() {
        let key = AesGcmKey::from_slice(&[9; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut paused = false;
        let mut deadline = None;
        assert!(apply_data_channel_event(
            DataChannelEvent::OnBufferedAmountHigh,
            &key,
            &mut pending,
            &mut paused,
            &mut deadline,
            Duration::ZERO,
        ));
        let original_deadline = deadline;
        assert!(paused);
        assert!(original_deadline.unwrap() <= Instant::now());

        assert!(apply_data_channel_event(
            DataChannelEvent::OnOpen,
            &key,
            &mut pending,
            &mut paused,
            &mut deadline,
            Duration::from_secs(60),
        ));
        assert_eq!(deadline, original_deadline);
        assert!(paused);

        assert!(apply_data_channel_event(
            DataChannelEvent::OnBufferedAmountLow,
            &key,
            &mut pending,
            &mut paused,
            &mut deadline,
            Duration::ZERO,
        ));
        assert!(!paused);
        assert_eq!(deadline, None);
    }

    #[test]
    fn missing_low_water_event_terminates_partial_response_and_cleans_peer() {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountHigh);
        let key = AesGcmKey::from_slice(&[5; 32]).unwrap();
        let mut pending = VecDeque::from([PendingLocalWebrtcRequest::Request(Box::new(
            DaemonRequest::Status,
        ))]);
        let (runtime_tx, runtime_rx) = mpsc::channel();
        let peer_state = LocalWebrtcPeerState::new("grant-fixture".to_string(), runtime_tx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let completed = runtime.block_on(send_response_frames_with_deadline(
            &data_channel,
            &key,
            &["partial".to_string(), "completion".to_string()],
            &mut pending,
            Duration::ZERO,
        ));
        assert!(!completed);
        assert_eq!(data_channel.sent.lock().unwrap().as_slice(), &["partial"]);

        runtime.block_on(close_data_channel(&data_channel, &mut pending, &peer_state));
        assert!(pending.is_empty());
        assert!(data_channel.closed.load(Ordering::Acquire));
        assert!(matches!(
            runtime_rx.recv().unwrap(),
            ControlMessage::LocalWebrtcPeerClosed { grant_id, .. }
                if grant_id == "grant-fixture"
        ));
    }

    #[test]
    fn high_then_low_water_resumes_and_completes_response_in_order() {
        let data_channel = FakeDataChannel::default();
        data_channel.events.lock().unwrap().extend([
            DataChannelEvent::OnBufferedAmountHigh,
            DataChannelEvent::OnBufferedAmountLow,
        ]);
        let key = AesGcmKey::from_slice(&[6; 32]).unwrap();
        let mut pending = VecDeque::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let completed = runtime.block_on(send_response_frames_with_deadline(
            &data_channel,
            &key,
            &["first".to_string(), "second".to_string()],
            &mut pending,
            Duration::from_secs(1),
        ));

        assert!(completed);
        assert_eq!(
            data_channel.sent.lock().unwrap().as_slice(),
            &["first", "second"]
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn idle_open_channel_does_not_wait_between_response_frames() {
        let data_channel = FakeDataChannel::default();
        let key = AesGcmKey::from_slice(&[10; 32]).unwrap();
        let mut pending = VecDeque::new();
        let frames = (0..20)
            .map(|index| format!("frame-{index}"))
            .collect::<Vec<_>>();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let started = Instant::now();
        let completed = runtime.block_on(send_response_frames_with_deadline(
            &data_channel,
            &key,
            &frames,
            &mut pending,
            Duration::from_secs(1),
        ));

        assert!(completed);
        assert_eq!(data_channel.sent.lock().unwrap().len(), frames.len());
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "idle event probes must not throttle response frames"
        );
    }

    #[test]
    fn inbound_request_during_response_is_retained_for_fifo_processing() {
        let data_channel = FakeDataChannel::default();
        let key = AesGcmKey::from_slice(&[7; 32]).unwrap();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(encrypted_request_event(&key, &DaemonRequest::Status));
        let mut pending = VecDeque::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let completed = runtime.block_on(send_response_frames_with_deadline(
            &data_channel,
            &key,
            &["first".to_string(), "second".to_string()],
            &mut pending,
            Duration::from_secs(1),
        ));

        assert!(completed);
        assert_eq!(data_channel.sent.lock().unwrap().len(), 2);
        assert!(matches!(
            pending.pop_front(),
            Some(PendingLocalWebrtcRequest::Request(request)) if *request == DaemonRequest::Status
        ));
        assert!(pending.is_empty());
    }

    #[test]
    fn overflowing_requests_each_preserve_one_fifo_operator_response() {
        let key = AesGcmKey::from_slice(&[8; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut paused = false;
        let mut deadline = None;

        let inbound_requests = LOCAL_WEBRTC_PENDING_REQUESTS + 4;
        for _ in 0..inbound_requests {
            assert!(apply_data_channel_event(
                encrypted_request_event(&key, &DaemonRequest::Status),
                &key,
                &mut pending,
                &mut paused,
                &mut deadline,
                Duration::from_secs(1),
            ));
        }

        assert_eq!(pending.len(), LOCAL_WEBRTC_PENDING_REQUESTS + 1);
        assert!(matches!(
            pending.back(),
            Some(PendingLocalWebrtcRequest::QueueOverflow(4))
        ));
        let mut responses_emitted = 0;
        while pop_pending_request(&mut pending).is_some() {
            responses_emitted += 1;
        }
        assert_eq!(responses_emitted, inbound_requests);
        let response = queued_request_overflow_response();
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert!(
            response.diagnostics[0]
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("capacity exceeded")
        );
    }

    #[test]
    fn issuing_bootstrap_prunes_expired_grants_and_keeps_live_replay_diagnostics() {
        let now = now_seconds();
        let mut transport = LocalWebrtcTransport::default();
        transport.grants.insert(
            "grant-expired".to_string(),
            LocalWebrtcGrant {
                grant_id: "grant-expired".to_string(),
                grant_secret: "secret-expired".to_string(),
                expected_origin: "http://127.0.0.1:1".to_string(),
                expires_at: now.saturating_sub(1),
                redeemed: true,
            },
        );
        transport.grants.insert(
            "grant-live-redeemed".to_string(),
            LocalWebrtcGrant {
                grant_id: "grant-live-redeemed".to_string(),
                grant_secret: "secret-live".to_string(),
                expected_origin: "http://127.0.0.1:2".to_string(),
                expires_at: now + GRANT_TTL_SECONDS,
                redeemed: true,
            },
        );

        let bootstrap = transport
            .issue_bootstrap("botster-web", "web-client", "http://127.0.0.1:41739")
            .expect("issue bootstrap");

        assert!(!transport.grants.contains_key("grant-expired"));
        assert!(transport.grants.contains_key("grant-live-redeemed"));
        assert!(transport.grants.contains_key(&bootstrap.grant_id));
        assert_eq!(transport.grants.len(), 2);
    }
}
