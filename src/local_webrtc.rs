//! Local WebRTC signaling and DataChannel adapter for installed browser packages.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::mpsc::{self, Sender};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use botster_core::{AesGcmEnvelope, AesGcmKey, decrypt_aes_gcm, encrypt_aes_gcm};
use botster_hub_client::{
    DaemonDiagnostic, DaemonLocalWebrtcAnswer, DaemonLocalWebrtcBootstrap, DaemonRequest,
    DaemonResponse,
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
            while let Some(event) = data_channel.poll().await {
                match event {
                    DataChannelEvent::OnMessage(message) => {
                        let Some(request) =
                            decrypt_daemon_request(&stream_key, message.data.as_ref())
                        else {
                            break;
                        };
                        let subscription_change =
                            LocalWebrtcAttachedSubscriptionChange::from_request(&request);
                        let (reply_tx, reply_rx) = mpsc::channel();
                        if runtime_tx
                            .send(ControlMessage::Request {
                                request: Box::new(request),
                                reply_tx,
                            })
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
                        let Ok(response) = encrypt_daemon_response(&stream_key, &response) else {
                            break;
                        };
                        if data_channel.send_text(&response).await.is_err() {
                            break;
                        }
                    }
                    DataChannelEvent::OnClose => break,
                    DataChannelEvent::OnError => break,
                    _ => {}
                }
            }
            peer_state.cleanup_once();
        }));
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

fn response_with_diagnostic(diagnostic: DaemonDiagnostic) -> DaemonResponse {
    DaemonResponse {
        kind: botster_hub_client::DaemonResponseKind::OperatorError,
        status: None,
        sessions: Vec::new(),
        session_templates: Vec::new(),
        resolved_session_template: None,
        session_context: None,
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
