//! Local WebRTC signaling and DataChannel adapter for installed browser packages.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
#[cfg(test)]
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use botster_core::{AesGcmEnvelope, AesGcmKey, decrypt_aes_gcm, encrypt_aes_gcm};
use botster_hub_client::{
    DaemonDiagnostic, DaemonEntityFrame, DaemonLocalWebrtcAnswer, DaemonLocalWebrtcBootstrap,
    DaemonLocalWebrtcDeliveryChunk, DaemonLocalWebrtcDeliveryKind, DaemonRequest, DaemonResponse,
    LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION, LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
    LOCAL_WEBRTC_MAX_FRAME_BYTES,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc as tokio_mpsc, oneshot, watch};
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{Runtime, Sender as AsyncSender, channel, default_runtime, timeout};

use crate::daemon_transport::{
    ControlMessage, ControlSender, ENTITY_SUBSCRIPTION_QUEUE_CAPACITY, EntityFrameSender,
};

const GRANT_TTL_SECONDS: u64 = 120;
const WEBRTC_SIGNAL_OPERATION: &str = "local_webrtc_signal";
// The current Rust WebRTC peer's message receive path is bounded at 16 KiB;
// 12 KiB leaves transport and JSON framing headroom for every first-party peer.
const LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES: usize = 12 * 1024;
const LOCAL_WEBRTC_PENDING_REQUESTS: usize = 16;
const LOCAL_WEBRTC_EVENT_PROBE: Duration = Duration::ZERO;
const LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW: u32 = LOCAL_WEBRTC_MAX_FRAME_BYTES as u32;
const LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH: u32 = (LOCAL_WEBRTC_MAX_FRAME_BYTES * 2) as u32;
/// Hard bound for production `peer.close()` waits on the forget path.
/// Timeout is treated as ultimate close failure → fail-closed dedicated-runtime drop.
#[cfg(not(test))]
pub(crate) const LOCAL_WEBRTC_PEER_CLOSE_BOUND: Duration = Duration::from_secs(3);
/// Test bound is short so hang injection does not starve parallel worker-join oracles that
/// share the process-global dedicated-runtime worker counter.
#[cfg(test)]
pub(crate) const LOCAL_WEBRTC_PEER_CLOSE_BOUND: Duration = Duration::from_millis(200);
/// Test join deadline for production PeerClosed handler under forced close hang.
/// Must be strictly greater than [`LOCAL_WEBRTC_PEER_CLOSE_BOUND`].
#[cfg(test)]
pub(crate) const LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE: Duration = Duration::from_secs(2);
const TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV: &str = "BOTSTER_HUB_TEST_CLOSE_LOCAL_WEBRTC_OPERATION";
pub(crate) const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE: &str =
    "local-webrtc-sender-terminal.json";
pub(crate) const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES: usize = 4096;

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
    /// Live peer ownership records used for fail-closed sibling cleanup.
    peer_states: BTreeMap<String, Arc<LocalWebrtcPeerState>>,
    /// Peers whose `close()` failed while siblings kept the shared runtime alive.
    /// Retained so a later empty-map park / `stop_all` can still force driver stop.
    stale_close_peers: BTreeMap<String, Arc<dyn PeerConnection>>,
    runtime: Option<tokio::runtime::Runtime>,
    #[cfg(test)]
    close_completions: Mutex<Vec<String>>,
    #[cfg(test)]
    peer_handlers: BTreeMap<String, Arc<LocalWebrtcHandler>>,
    #[cfg(test)]
    force_close_errors: Mutex<BTreeSet<String>>,
    #[cfg(test)]
    force_close_hangs: Mutex<BTreeSet<String>>,
}

enum ClosePeerOutcome {
    Closed,
    Failed(Arc<dyn PeerConnection>),
}

/// Grants removed from the live peer map by a forget operation (primary and any fail-closed siblings).
#[derive(Debug, Default)]
pub(crate) struct PeerRemoveResult {
    pub removed_grant_ids: Vec<String>,
    pub attached_subscriptions: Vec<LocalWebrtcAttachedSubscription>,
}

#[cfg(test)]
static LOCAL_WEBRTC_WORKER_THREADS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl LocalWebrtcTransport {
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
        runtime_tx: ControlSender,
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
        self.peer_states.insert(grant_id.clone(), answer.peer_state);
        #[cfg(test)]
        self.peer_handlers.insert(grant_id.clone(), answer.handler);
        Ok(DaemonLocalWebrtcAnswer {
            grant_id,
            answer: answer.answer,
            diagnostics: vec![DaemonDiagnostic::connected(WEBRTC_SIGNAL_OPERATION)],
        })
    }

    /// Close all active local peers. Used during daemon shutdown.
    pub fn stop_all(&mut self) {
        let peers = std::mem::take(&mut self.peers);
        let stale = std::mem::take(&mut self.stale_close_peers);
        self.peer_states.clear();
        self.grants.clear();
        #[cfg(test)]
        self.peer_handlers.clear();
        // Hard stop: drop the dedicated runtime without sequential close waits so shutdown
        // cannot block the control plane for N × close-bound.
        drop(peers);
        drop(stale);
        let _ = self.runtime.take();
    }

    /// Close one peer, remove it from the live map, and drop the dedicated runtime when empty.
    ///
    /// This is the sole production forget path for `LocalWebrtcPeerClosed`.
    /// Returns every grant removed (including fail-closed siblings) so the control plane can
    /// sweep grant-owned daemon state synchronously.
    pub(crate) fn remove_peer(&mut self, grant_id: &str) -> PeerRemoveResult {
        let Some(peer) = self.peers.remove(grant_id) else {
            return PeerRemoveResult::default();
        };
        #[cfg(test)]
        self.peer_handlers.remove(grant_id);
        if let Some(runtime) = self.runtime.as_ref() {
            match self.close_peer_on_runtime(runtime, grant_id, peer) {
                ClosePeerOutcome::Closed => {
                    let result = self.take_remove_result(std::iter::once(grant_id.to_string()));
                    self.park_runtime_if_idle();
                    result
                }
                ClosePeerOutcome::Failed(peer) => {
                    // The consumed webrtc crate can fail or hang before aborting the driver.
                    // Leaving that peer on a shared runtime kept alive by siblings recreates the
                    // multi-core timeout storm. Fail-closed: drop ownership and the dedicated
                    // runtime immediately (no sequential re-close waits that scale with peer count).
                    eprintln!(
                        "local WebRTC peer close failed ultimately; fail-closed drop of dedicated runtime: grant_id={grant_id}"
                    );
                    // Drop the failed peer Arc without another close wait; runtime drop is the hard stop.
                    // Primary grant is already out of `peers` — pass it so peer_states / ownership
                    // are still swept (fail_closed only sees remaining live/stale map keys).
                    drop(peer);
                    self.fail_closed_drop_dedicated_runtime(Some(grant_id.to_string()))
                }
            }
        } else {
            let result = self.take_remove_result(std::iter::once(grant_id.to_string()));
            self.park_runtime_if_idle();
            result
        }
    }

    fn take_remove_result(
        &mut self,
        grant_ids: impl IntoIterator<Item = String>,
    ) -> PeerRemoveResult {
        let mut result = PeerRemoveResult::default();
        for grant_id in grant_ids {
            if let Some(peer_state) = self.peer_states.remove(&grant_id) {
                let attached = peer_state
                    .attached_subscriptions
                    .lock()
                    .expect("local WebRTC peer subscription mutex")
                    .clone();
                result.attached_subscriptions.extend(attached);
            }
            result.removed_grant_ids.push(grant_id);
        }
        result
    }

    /// True while a signaled peer still occupies the live peer map.
    pub(crate) fn has_live_peer(&self, grant_id: &str) -> bool {
        self.peers.contains_key(grant_id)
    }

    fn park_runtime_if_idle(&mut self) {
        if !self.peers.is_empty() {
            return;
        }
        // No live peers: drop quarantined peers and the runtime without sequential close waits.
        // Runtime drop is the hard stop for residual driver tasks.
        self.stale_close_peers.clear();
        let _ = self.runtime.take();
    }

    /// Stop every dedicated-runtime peer driver after an unrecoverable single-peer close failure.
    ///
    /// Ownership is removed and the dedicated runtime is dropped immediately. Do **not**
    /// sequentially re-close peers here: each close can wait up to
    /// [`LOCAL_WEBRTC_PEER_CLOSE_BOUND`], and N peers would make handler latency unbounded.
    ///
    /// `primary_grant` is the grant already removed from `peers` whose close failed/timed out;
    /// it must still be ownership-swept even though it is no longer in the live map.
    fn fail_closed_drop_dedicated_runtime(
        &mut self,
        primary_grant: Option<String>,
    ) -> PeerRemoveResult {
        let live = std::mem::take(&mut self.peers);
        let stale = std::mem::take(&mut self.stale_close_peers);
        let mut removed_grants: Vec<String> =
            live.keys().cloned().chain(stale.keys().cloned()).collect();
        if let Some(primary) = primary_grant
            && !removed_grants.iter().any(|grant| grant == &primary)
        {
            removed_grants.push(primary);
        }
        let result = self.take_remove_result(removed_grants);
        #[cfg(test)]
        self.peer_handlers.clear();
        // Hard stop for driver loops: drop peers and runtime without further close waits.
        drop(live);
        drop(stale);
        let _ = self.runtime.take();
        result
    }

    fn close_peer_on_runtime(
        &self,
        runtime: &tokio::runtime::Runtime,
        grant_id: &str,
        peer: Arc<dyn PeerConnection>,
    ) -> ClosePeerOutcome {
        #[cfg(test)]
        if self
            .force_close_errors
            .lock()
            .expect("force close error mutex")
            .remove(grant_id)
        {
            // Simulate close() failing before the driver is stopped (webrtc can fail in
            // core.close() before abort).
            eprintln!("local WebRTC peer close forced failure for test: grant_id={grant_id}");
            return ClosePeerOutcome::Failed(peer);
        }

        // Hang inject shares the production timeout wrapper around the close future so that
        // removing the bound leaves a never-completing close (red-on-revert hangs the handler).
        #[cfg(test)]
        let force_hang = self
            .force_close_hangs
            .lock()
            .expect("force close hang mutex")
            .remove(grant_id);
        #[cfg(not(test))]
        let force_hang = false;
        if force_hang {
            eprintln!("local WebRTC peer close forced hang for test: grant_id={grant_id}");
        }

        let close_once = || -> Result<(), bool> {
            // Ok = closed; Err(true) = timeout; Err(false) = library error.
            // timeout() must be created inside block_on (needs Handle::current for the timer).
            // Production path: always wrap the close future — hang inject replaces the future
            // with pending(), still cancelled only by LOCAL_WEBRTC_PEER_CLOSE_BOUND.
            match runtime.block_on(async {
                tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, async {
                    if force_hang {
                        // Stand in for a peer.close() future that never completes. The only
                        // cancel path is LOCAL_WEBRTC_PEER_CLOSE_BOUND (production timeout).
                        std::future::pending::<()>().await;
                    }
                    peer.close().await
                })
                .await
            }) {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => {
                    eprintln!("local WebRTC peer close failed: grant_id={grant_id} error={error}");
                    Err(false)
                }
                Err(_) => {
                    eprintln!(
                        "local WebRTC peer close timed out after {:?}: grant_id={grant_id}",
                        LOCAL_WEBRTC_PEER_CLOSE_BOUND
                    );
                    Err(true)
                }
            }
        };

        let close_result = match close_once() {
            Ok(()) => Ok(()),
            Err(true) => {
                // Timeout is ultimate failure: do not retry a hung close on the control thread.
                Err(())
            }
            Err(false) => {
                eprintln!("local WebRTC peer close failed (retrying once): grant_id={grant_id}");
                close_once().map_err(|_| ())
            }
        };

        match close_result {
            Ok(()) => {
                #[cfg(test)]
                {
                    // Close-completion evidence records that production forget invoked and
                    // completed PeerConnection::close for this grant. Never record when close()
                    // was skipped or ultimately failed.
                    self.close_completions
                        .lock()
                        .expect("local WebRTC close completion mutex")
                        .push(grant_id.to_string());
                }
                ClosePeerOutcome::Closed
            }
            Err(()) => ClosePeerOutcome::Failed(peer),
        }
    }

    fn runtime(&mut self) -> LocalWebrtcResult<&tokio::runtime::Runtime> {
        if self.runtime.is_none() {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.thread_name("botster-local-webrtc").enable_all();
            #[cfg(test)]
            {
                builder
                    .on_thread_start(|| {
                        LOCAL_WEBRTC_WORKER_THREADS.fetch_add(1, Ordering::SeqCst);
                    })
                    .on_thread_stop(|| {
                        LOCAL_WEBRTC_WORKER_THREADS.fetch_sub(1, Ordering::SeqCst);
                    });
            }
            self.runtime = Some(
                builder
                    .build()
                    .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?,
            );
        }
        Ok(self.runtime.as_ref().expect("runtime was initialized"))
    }

    fn prune_expired_grants(&mut self, now: u64) {
        self.grants.retain(|_, grant| grant.expires_at > now);
    }

    #[cfg(test)]
    pub(crate) fn active_peer_count(&self) -> usize {
        self.peers.len()
    }

    #[cfg(test)]
    pub(crate) fn has_dedicated_runtime(&self) -> bool {
        self.runtime.is_some()
    }

    #[cfg(test)]
    pub(crate) fn stale_close_peer_count(&self) -> usize {
        self.stale_close_peers.len()
    }

    #[cfg(test)]
    pub(crate) fn close_completion_count_for(&self, grant_id: &str) -> usize {
        self.close_completions
            .lock()
            .expect("local WebRTC close completion mutex")
            .iter()
            .filter(|completed| completed.as_str() == grant_id)
            .count()
    }

    #[cfg(test)]
    pub(crate) fn dedicated_runtime_worker_threads() -> usize {
        LOCAL_WEBRTC_WORKER_THREADS.load(Ordering::SeqCst)
    }

    /// Live peer ownership records remaining in the transport (test oracle).
    #[cfg(test)]
    pub(crate) fn peer_state_count(&self) -> usize {
        self.peer_states.len()
    }

    /// Next `close()` for this grant is treated as a hard failure (driver not stopped).
    #[cfg(test)]
    pub(crate) fn force_next_close_error_for_test(&self, grant_id: &str) {
        self.force_close_errors
            .lock()
            .expect("force close error mutex")
            .insert(grant_id.to_string());
    }

    /// Next `close()` for this grant hangs until the production close bound times out.
    #[cfg(test)]
    pub(crate) fn force_next_close_hang_for_test(&self, grant_id: &str) {
        self.force_close_hangs
            .lock()
            .expect("force close hang mutex")
            .insert(grant_id.to_string());
    }

    /// Deterministic production-path failure injection for tests.
    ///
    /// Calls the same `LocalWebrtcHandler::on_connection_state_change` body that the live
    /// WebRTC stack invokes when a peer reaches a terminal connection state.
    #[cfg(test)]
    pub(crate) fn inject_peer_connection_state_for_test(
        &mut self,
        grant_id: &str,
        state: RTCPeerConnectionState,
    ) {
        let handler = self
            .peer_handlers
            .get(grant_id)
            .cloned()
            .unwrap_or_else(|| panic!("missing production handler for grant {grant_id}"));
        let runtime = self
            .runtime
            .as_ref()
            .expect("dedicated runtime required to inject peer connection state");
        runtime.block_on(handler.on_connection_state_change(state));
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
    peer_state: Arc<LocalWebrtcPeerState>,
    #[cfg(test)]
    handler: Arc<LocalWebrtcHandler>,
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
    runtime_tx: ControlSender,
    attached_subscriptions: Mutex<Vec<LocalWebrtcAttachedSubscription>>,
    entity_subscription_ids: Mutex<BTreeSet<String>>,
    terminal_state: Mutex<LocalWebrtcTerminalState>,
    peer_terminal_tx: watch::Sender<Option<LocalWebrtcTerminalCause>>,
    peer_terminal_published: AtomicBool,
    cleanup_sent: AtomicBool,
}

enum PendingLocalWebrtcRequest {
    Request(Box<DaemonRequest>),
    EntityFrame(Box<DaemonEntityFrame>),
    QueueOverflow(usize),
}

enum LocalWebrtcInbound {
    Channel(Result<Option<DataChannelEvent>, LocalWebrtcTerminalCause>),
    Entity(DaemonEntityFrame),
}

#[derive(Debug, Default)]
struct LocalWebrtcFlowControl {
    pressured: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LocalWebrtcTerminalCause {
    SendText,
    ChannelClosed,
    ChannelError,
    PollEnded,
    InvalidRequest,
    RequestQueueOverflow,
    InvalidEncryptedRequest,
    RuntimeQueueClosed,
    ResponseFraming,
    LowWaterThresholdSetup,
    HighWaterThresholdSetup,
    PeerDisconnected,
    PeerFailed,
    PeerClosed,
}

impl fmt::Display for LocalWebrtcTerminalCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cause = match self {
            Self::SendText => "send_text",
            Self::ChannelClosed => "channel_closed",
            Self::ChannelError => "channel_error",
            Self::PollEnded => "poll_ended",
            Self::InvalidRequest => "invalid_request",
            Self::RequestQueueOverflow => "request_queue_overflow",
            Self::InvalidEncryptedRequest => "invalid_encrypted_request",
            Self::RuntimeQueueClosed => "runtime_queue_closed",
            Self::ResponseFraming => "response_framing",
            Self::LowWaterThresholdSetup => "low_water_threshold_setup",
            Self::HighWaterThresholdSetup => "high_water_threshold_setup",
            Self::PeerDisconnected => "peer_disconnected",
            Self::PeerFailed => "peer_failed",
            Self::PeerClosed => "peer_closed",
        };
        formatter.write_str(cause)
    }
}

#[derive(Debug)]
struct LocalWebrtcSendFailure {
    message_id: String,
    next_chunk_index: usize,
    last_sent_chunk_index: Option<usize>,
    total_chunks: usize,
    pressured: bool,
    cause: LocalWebrtcTerminalCause,
}

impl fmt::Display for LocalWebrtcSendFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "local WebRTC response delivery failed: message_id={} next_chunk={} last_sent_chunk={} total_chunks={} pressured={} cause={}",
            self.message_id,
            self.next_chunk_index,
            self.last_sent_chunk_index
                .map_or_else(|| "none".to_string(), |index| index.to_string()),
            self.total_chunks,
            self.pressured,
            self.cause,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LocalWebrtcChannelTerminalSignal {
    None,
    OnClose,
    OnError,
    PollEnded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LocalWebrtcCleanupDisposition {
    NewlySent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalWebrtcSenderTerminalRecord {
    pub schema_version: u32,
    pub grant_id: String,
    pub request_operation: String,
    pub message_id: Option<String>,
    pub next_chunk_index: usize,
    pub last_sent_chunk_index: Option<usize>,
    pub total_chunks: usize,
    pub pressured: bool,
    pub peer_connection_state: String,
    pub channel_terminal_signal: LocalWebrtcChannelTerminalSignal,
    pub cause: LocalWebrtcTerminalCause,
    pub cleanup_disposition: LocalWebrtcCleanupDisposition,
}

#[derive(Debug)]
struct LocalWebrtcTerminalState {
    request_operation: String,
    message_id: Option<String>,
    next_chunk_index: usize,
    last_sent_chunk_index: Option<usize>,
    total_chunks: usize,
    pressured: bool,
    peer_connection_state: String,
    channel_terminal_signal: LocalWebrtcChannelTerminalSignal,
}

impl Default for LocalWebrtcTerminalState {
    fn default() -> Self {
        Self {
            request_operation: "none".to_string(),
            message_id: None,
            next_chunk_index: 0,
            last_sent_chunk_index: None,
            total_chunks: 0,
            pressured: false,
            peer_connection_state: "new".to_string(),
            channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
        }
    }
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
    fn new(grant_id: String, runtime_tx: ControlSender) -> Self {
        let (peer_terminal_tx, _peer_terminal_rx) = watch::channel(None);
        Self {
            grant_id,
            runtime_tx,
            attached_subscriptions: Mutex::new(Vec::new()),
            entity_subscription_ids: Mutex::new(BTreeSet::new()),
            terminal_state: Mutex::new(LocalWebrtcTerminalState::default()),
            peer_terminal_tx,
            peer_terminal_published: AtomicBool::new(false),
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

    fn add_entity_subscription(&self, subscription_id: String) {
        self.entity_subscription_ids
            .lock()
            .expect("local WebRTC entity subscription mutex")
            .insert(subscription_id);
    }

    fn remove_entity_subscription(&self, subscription_id: &str) {
        self.entity_subscription_ids
            .lock()
            .expect("local WebRTC entity subscription mutex")
            .remove(subscription_id);
    }

    fn owns_entity_subscription(&self, subscription_id: &str) -> bool {
        self.entity_subscription_ids
            .lock()
            .expect("local WebRTC entity subscription mutex")
            .contains(subscription_id)
    }

    fn begin_request(&self, request: &DaemonRequest) {
        self.begin_operation(local_webrtc_request_operation(request));
    }

    fn begin_overflow_response(&self) {
        self.begin_operation("request_queue_overflow");
    }

    fn begin_operation(&self, operation: &str) {
        let mut terminal_state = self
            .terminal_state
            .lock()
            .expect("local WebRTC terminal state mutex");
        terminal_state.request_operation = operation.to_string();
        terminal_state.message_id = None;
        terminal_state.next_chunk_index = 0;
        terminal_state.last_sent_chunk_index = None;
        terminal_state.total_chunks = 0;
        terminal_state.pressured = false;
    }

    fn begin_response(&self, message_id: Option<String>, total_chunks: usize, pressured: bool) {
        let mut terminal_state = self
            .terminal_state
            .lock()
            .expect("local WebRTC terminal state mutex");
        terminal_state.message_id = message_id;
        terminal_state.next_chunk_index = 0;
        terminal_state.last_sent_chunk_index = None;
        terminal_state.total_chunks = total_chunks;
        terminal_state.pressured = pressured;
    }

    fn record_response_progress(&self, next_chunk_index: usize, pressured: bool) {
        let mut terminal_state = self
            .terminal_state
            .lock()
            .expect("local WebRTC terminal state mutex");
        terminal_state.next_chunk_index = next_chunk_index;
        terminal_state.last_sent_chunk_index = next_chunk_index.checked_sub(1);
        terminal_state.pressured = pressured;
    }

    fn set_peer_connection_state(&self, state: RTCPeerConnectionState) {
        self.terminal_state
            .lock()
            .expect("local WebRTC terminal state mutex")
            .peer_connection_state = local_webrtc_peer_connection_state(state).to_string();
    }

    fn observe_peer_connection_state(
        &self,
        state: RTCPeerConnectionState,
    ) -> Option<LocalWebrtcTerminalCause> {
        self.set_peer_connection_state(state);
        let cause = match state {
            RTCPeerConnectionState::Failed => LocalWebrtcTerminalCause::PeerFailed,
            RTCPeerConnectionState::Closed => LocalWebrtcTerminalCause::PeerClosed,
            _ => return None,
        };
        self.publish_peer_terminal(cause);
        Some(cause)
    }

    fn subscribe_peer_terminal(&self) -> watch::Receiver<Option<LocalWebrtcTerminalCause>> {
        self.peer_terminal_tx.subscribe()
    }

    fn publish_peer_terminal(&self, cause: LocalWebrtcTerminalCause) {
        if self
            .peer_terminal_published
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.peer_terminal_tx.send_replace(Some(cause));
        }
    }

    async fn cleanup_once(&self, cause: LocalWebrtcTerminalCause) {
        {
            let mut terminal_state = self
                .terminal_state
                .lock()
                .expect("local WebRTC terminal state mutex");
            terminal_state.channel_terminal_signal = match cause {
                LocalWebrtcTerminalCause::ChannelClosed => {
                    LocalWebrtcChannelTerminalSignal::OnClose
                }
                LocalWebrtcTerminalCause::ChannelError => LocalWebrtcChannelTerminalSignal::OnError,
                LocalWebrtcTerminalCause::PollEnded => LocalWebrtcChannelTerminalSignal::PollEnded,
                _ => terminal_state.channel_terminal_signal,
            };
        }
        if self.cleanup_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        let terminal_record = {
            let terminal_state = self
                .terminal_state
                .lock()
                .expect("local WebRTC terminal state mutex");
            LocalWebrtcSenderTerminalRecord {
                schema_version: 1,
                grant_id: self.grant_id.clone(),
                request_operation: terminal_state.request_operation.clone(),
                message_id: terminal_state.message_id.clone(),
                next_chunk_index: terminal_state.next_chunk_index,
                last_sent_chunk_index: terminal_state.last_sent_chunk_index,
                total_chunks: terminal_state.total_chunks,
                pressured: terminal_state.pressured,
                peer_connection_state: terminal_state.peer_connection_state.clone(),
                channel_terminal_signal: terminal_state.channel_terminal_signal,
                cause,
                cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
            }
        };
        let attached_subscriptions = self
            .attached_subscriptions
            .lock()
            .expect("local WebRTC peer subscription mutex")
            .clone();
        let entity_subscription_ids = self
            .entity_subscription_ids
            .lock()
            .expect("local WebRTC entity subscription mutex")
            .iter()
            .cloned()
            .collect();
        if self
            .runtime_tx
            .send(ControlMessage::LocalWebrtcPeerClosed {
                grant_id: self.grant_id.clone(),
                attached_subscriptions,
                entity_subscription_ids,
                terminal_record,
            })
            .await
            .is_err()
        {
            eprintln!("local WebRTC cleanup queue closed before peer cleanup");
        }
    }
}

fn local_webrtc_request_operation(request: &DaemonRequest) -> &'static str {
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

fn local_webrtc_peer_connection_state(state: RTCPeerConnectionState) -> &'static str {
    match state {
        RTCPeerConnectionState::Unspecified => "unspecified",
        RTCPeerConnectionState::New => "new",
        RTCPeerConnectionState::Connecting => "connecting",
        RTCPeerConnectionState::Connected => "connected",
        RTCPeerConnectionState::Disconnected => "disconnected",
        RTCPeerConnectionState::Failed => "failed",
        RTCPeerConnectionState::Closed => "closed",
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
        if let Some(cause) = self.peer_state.observe_peer_connection_state(state) {
            self.peer_state.cleanup_once(cause).await;
            let _ = self.gather_complete_tx.try_send(());
        }
    }

    async fn on_data_channel(&self, data_channel: Arc<dyn DataChannel>) {
        let peer_state = self.peer_state.clone();
        let runtime_tx = peer_state.runtime_tx.clone();
        let stream_key = self.stream_key.clone();
        let (entity_frame_tx, entity_frame_rx) =
            tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        self.runtime.spawn(Box::pin(async move {
            if let Err(error) = data_channel
                .local_set_buffered_amount_low_threshold(LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW)
                .await
            {
                eprintln!("local WebRTC low-water threshold setup failed: {error}");
                let _ = data_channel.local_close().await;
                peer_state
                    .cleanup_once(LocalWebrtcTerminalCause::LowWaterThresholdSetup)
                    .await;
                return;
            }
            if let Err(error) = data_channel
                .local_set_buffered_amount_high_threshold(LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH)
                .await
            {
                eprintln!("local WebRTC high-water threshold setup failed: {error}");
                let _ = data_channel.local_close().await;
                peer_state
                    .cleanup_once(LocalWebrtcTerminalCause::HighWaterThresholdSetup)
                    .await;
                return;
            }

            let _ = run_data_channel(
                data_channel.as_ref(),
                &stream_key,
                peer_state.as_ref(),
                &runtime_tx,
                entity_frame_tx,
                entity_frame_rx,
            )
            .await;
        }));
    }
}

async fn poll_data_channel_or_peer_terminal<D>(
    data_channel: &D,
    peer_terminal_rx: &mut watch::Receiver<Option<LocalWebrtcTerminalCause>>,
) -> Result<Option<DataChannelEvent>, LocalWebrtcTerminalCause>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    if let Some(cause) = *peer_terminal_rx.borrow_and_update() {
        return Err(cause);
    }
    tokio::select! {
        event = data_channel.local_poll() => Ok(event),
        changed = peer_terminal_rx.changed() => {
            changed.expect("local WebRTC peer terminal sender remains owned by peer state");
            Err(peer_terminal_rx
                .borrow_and_update()
                .expect("peer terminal watch changes only when a terminal cause is published"))
        }
    }
}

async fn run_data_channel<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    peer_state: &LocalWebrtcPeerState,
    runtime_tx: &ControlSender,
    entity_frame_tx: tokio_mpsc::Sender<DaemonEntityFrame>,
    mut entity_frame_rx: tokio_mpsc::Receiver<DaemonEntityFrame>,
) -> Option<LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    let mut pending_requests = VecDeque::new();
    let mut flow_control = LocalWebrtcFlowControl::default();
    let mut send_failure = None;
    let mut terminal_cause = LocalWebrtcTerminalCause::PollEnded;
    let mut peer_terminal_rx = peer_state.subscribe_peer_terminal();
    let mut open = true;
    while open {
        let pending = if let Some(request) = pop_pending_request(&mut pending_requests) {
            request
        } else {
            let inbound = tokio::select! {
                channel = poll_data_channel_or_peer_terminal(data_channel, &mut peer_terminal_rx) => {
                    LocalWebrtcInbound::Channel(channel)
                }
                frame = entity_frame_rx.recv() => {
                    LocalWebrtcInbound::Entity(
                        frame.expect("local WebRTC peer owns its entity subscription sender")
                    )
                }
            };
            match inbound {
                LocalWebrtcInbound::Entity(frame) => {
                    if !peer_state.owns_entity_subscription(entity_frame_subscription_id(&frame)) {
                        continue;
                    }
                    PendingLocalWebrtcRequest::EntityFrame(Box::new(frame))
                }
                LocalWebrtcInbound::Channel(Err(cause)) => {
                    terminal_cause = cause;
                    break;
                }
                LocalWebrtcInbound::Channel(Ok(Some(DataChannelEvent::OnMessage(message)))) => {
                    let Some(request) = decrypt_daemon_request(stream_key, message.data.as_ref())
                    else {
                        terminal_cause = LocalWebrtcTerminalCause::InvalidEncryptedRequest;
                        break;
                    };
                    PendingLocalWebrtcRequest::Request(Box::new(request))
                }
                LocalWebrtcInbound::Channel(Ok(Some(DataChannelEvent::OnClose))) => {
                    terminal_cause = LocalWebrtcTerminalCause::ChannelClosed;
                    break;
                }
                LocalWebrtcInbound::Channel(Ok(Some(DataChannelEvent::OnError))) => {
                    terminal_cause = LocalWebrtcTerminalCause::ChannelError;
                    break;
                }
                LocalWebrtcInbound::Channel(Ok(None)) => {
                    terminal_cause = LocalWebrtcTerminalCause::PollEnded;
                    break;
                }
                LocalWebrtcInbound::Channel(Ok(Some(
                    event @ (DataChannelEvent::OnBufferedAmountHigh
                    | DataChannelEvent::OnBufferedAmountLow),
                ))) => {
                    let _ = apply_data_channel_event(
                        event,
                        stream_key,
                        &mut pending_requests,
                        &mut flow_control,
                    );
                    continue;
                }
                LocalWebrtcInbound::Channel(Ok(Some(_))) => continue,
            }
        };

        if let PendingLocalWebrtcRequest::EntityFrame(frame) = &pending {
            peer_state.begin_operation("entity_delivery");
            let Ok(frames) = framed_daemon_entity_frame(stream_key, frame) else {
                terminal_cause = LocalWebrtcTerminalCause::ResponseFraming;
                break;
            };
            match send_response_frames(
                data_channel,
                stream_key,
                &frames,
                &mut pending_requests,
                &mut flow_control,
                peer_state,
            )
            .await
            {
                Ok(()) => continue,
                Err(failure) => {
                    eprintln!("{failure}");
                    terminal_cause = failure.cause;
                    send_failure = Some(failure);
                    break;
                }
            }
        }

        let request = match pending {
            PendingLocalWebrtcRequest::Request(request) => request,
            PendingLocalWebrtcRequest::EntityFrame(_) => unreachable!("entity frame handled above"),
            PendingLocalWebrtcRequest::QueueOverflow(_) => {
                peer_state.begin_overflow_response();
                let response = queued_request_overflow_response();
                let Ok(frames) = framed_daemon_response(stream_key, &response) else {
                    terminal_cause = LocalWebrtcTerminalCause::ResponseFraming;
                    break;
                };
                match send_response_frames(
                    data_channel,
                    stream_key,
                    &frames,
                    &mut pending_requests,
                    &mut flow_control,
                    peer_state,
                )
                .await
                {
                    Ok(()) => open = true,
                    Err(failure) => {
                        eprintln!("{failure}");
                        terminal_cause = failure.cause;
                        send_failure = Some(failure);
                        open = false;
                    }
                }
                continue;
            }
        };

        peer_state.begin_request(&request);
        if std::env::var(TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV).as_deref()
            == Ok(local_webrtc_request_operation(&request))
        {
            let _ = data_channel.local_close().await;
            terminal_cause = LocalWebrtcTerminalCause::ChannelClosed;
            break;
        }
        let subscription_change = LocalWebrtcAttachedSubscriptionChange::from_request(&request);
        let entity_subscription_change = match request.as_ref() {
            DaemonRequest::SubscribeEntities {
                subscription_id, ..
            } => Some((true, subscription_id.clone())),
            DaemonRequest::UnsubscribeEntities { subscription_id } => {
                Some((false, subscription_id.clone()))
            }
            _ => None,
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        let (response_delivery_tx, response_delivery_rx) =
            if matches!(*request, DaemonRequest::DaemonShutdown) {
                let (tx, rx) = mpsc::channel();
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };
        let request_sent = match *request {
            DaemonRequest::SubscribeEntities {
                entity_type,
                subscription_id,
            } => {
                runtime_tx
                    .send(ControlMessage::SubscribeEntities {
                        entity_type,
                        subscription_id,
                        frame_tx: EntityFrameSender::Async(entity_frame_tx.clone()),
                        reply_tx,
                        grant_id: Some(peer_state.grant_id.clone()),
                    })
                    .await
            }
            DaemonRequest::UnsubscribeEntities { subscription_id } => {
                runtime_tx
                    .send(ControlMessage::UnsubscribeEntities {
                        subscription_id,
                        reply_tx: Some(reply_tx),
                        grant_id: Some(peer_state.grant_id.clone()),
                    })
                    .await
            }
            request => {
                runtime_tx
                    .send(ControlMessage::Request {
                        request: Box::new(request),
                        reply_tx,
                        response_delivery_rx,
                        grant_id: Some(peer_state.grant_id.clone()),
                    })
                    .await
            }
        };
        if request_sent.is_err() {
            terminal_cause = LocalWebrtcTerminalCause::RuntimeQueueClosed;
            break;
        }
        let response = match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
            Ok(Ok(Ok(response))) => response,
            Ok(Ok(Err(error))) => response_with_diagnostic(DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                error.to_string(),
            )),
            Ok(Err(_)) => response_with_diagnostic(DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                "runtime reply channel closed",
            )),
            Err(_) => response_with_diagnostic(DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                "runtime request timed out",
            )),
        };
        // Only record peer-side attach ownership when the control plane accepted the change.
        // Stale/failed Attach must not create residual bookkeeping for PeerClosed snapshots.
        if response.kind != botster_hub_client::DaemonResponseKind::OperatorError {
            peer_state.apply_subscription_change(subscription_change);
        }
        if let Some((subscribed, subscription_id)) = entity_subscription_change {
            if subscribed
                && response.kind == botster_hub_client::DaemonResponseKind::EntitySubscribed
            {
                peer_state.add_entity_subscription(subscription_id);
            } else if !subscribed
                && response.kind == botster_hub_client::DaemonResponseKind::EntityUnsubscribed
            {
                peer_state.remove_entity_subscription(&subscription_id);
            }
        }
        let Ok(frames) = framed_daemon_response(stream_key, &response) else {
            if let Some(response_delivery_tx) = response_delivery_tx {
                let _ = response_delivery_tx.send(());
            }
            terminal_cause = LocalWebrtcTerminalCause::ResponseFraming;
            break;
        };
        let delivery = send_response_frames(
            data_channel,
            stream_key,
            &frames,
            &mut pending_requests,
            &mut flow_control,
            peer_state,
        )
        .await;
        if let Some(response_delivery_tx) = response_delivery_tx {
            let _ = response_delivery_tx.send(());
        }
        match delivery {
            Ok(()) => open = true,
            Err(failure) => {
                eprintln!("{failure}");
                terminal_cause = failure.cause;
                send_failure = Some(failure);
                open = false;
            }
        }
    }
    close_data_channel(
        data_channel,
        &mut pending_requests,
        peer_state,
        terminal_cause,
    )
    .await;
    send_failure
}

async fn close_data_channel<D>(
    data_channel: &D,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    peer_state: &LocalWebrtcPeerState,
    cause: LocalWebrtcTerminalCause,
) where
    D: LocalWebrtcDataChannel + ?Sized,
{
    pending_requests.clear();
    if let Err(error) = data_channel.local_close().await {
        eprintln!("local WebRTC data channel close failed: {error}");
    }
    peer_state.cleanup_once(cause).await;
}

async fn send_response_frames<D>(
    data_channel: &D,
    stream_key: &AesGcmKey,
    frames: &[String],
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
    peer_state: &LocalWebrtcPeerState,
) -> Result<(), LocalWebrtcSendFailure>
where
    D: LocalWebrtcDataChannel + ?Sized,
{
    let mut peer_terminal_rx = peer_state.subscribe_peer_terminal();
    let total_chunks = frames.len();
    let message_id = frames.first().and_then(|frame| {
        serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(frame)
            .ok()
            .map(|chunk| chunk.message_id)
    });
    peer_state.begin_response(message_id.clone(), total_chunks, flow_control.pressured);

    let failure =
        |next_chunk_index, cause, flow_control: &LocalWebrtcFlowControl| LocalWebrtcSendFailure {
            message_id: message_id
                .clone()
                .unwrap_or_else(|| "unavailable".to_string()),
            next_chunk_index,
            last_sent_chunk_index: next_chunk_index.checked_sub(1),
            total_chunks,
            pressured: flow_control.pressured,
            cause,
        };

    for (chunk_index, frame) in frames.iter().enumerate() {
        peer_state.record_response_progress(chunk_index, flow_control.pressured);
        while flow_control.pressured {
            match poll_data_channel_or_peer_terminal(data_channel, &mut peer_terminal_rx).await {
                Ok(Some(event)) => {
                    apply_data_channel_event(event, stream_key, pending_requests, flow_control)
                        .map_err(|cause| failure(chunk_index, cause, flow_control))?
                }
                Ok(None) => {
                    return Err(failure(
                        chunk_index,
                        LocalWebrtcTerminalCause::PollEnded,
                        flow_control,
                    ));
                }
                Err(cause) => {
                    return Err(failure(chunk_index, cause, flow_control));
                }
            }
        }

        if data_channel.local_send_text(frame).await.is_err() {
            return Err(failure(
                chunk_index,
                LocalWebrtcTerminalCause::SendText,
                flow_control,
            ));
        }
        peer_state.record_response_progress(chunk_index + 1, flow_control.pressured);

        match timeout(LOCAL_WEBRTC_EVENT_PROBE, data_channel.local_poll()).await {
            Ok(Some(event)) => {
                apply_data_channel_event(event, stream_key, pending_requests, flow_control)
                    .map_err(|cause| failure(chunk_index + 1, cause, flow_control))?;
            }
            Ok(None) => {
                return Err(failure(
                    chunk_index + 1,
                    LocalWebrtcTerminalCause::PollEnded,
                    flow_control,
                ));
            }
            Err(_) => {}
        }
    }
    Ok(())
}

fn apply_data_channel_event(
    event: DataChannelEvent,
    stream_key: &AesGcmKey,
    pending_requests: &mut VecDeque<PendingLocalWebrtcRequest>,
    flow_control: &mut LocalWebrtcFlowControl,
) -> Result<(), LocalWebrtcTerminalCause> {
    match event {
        DataChannelEvent::OnBufferedAmountHigh => {
            flow_control.pressured = true;
            Ok(())
        }
        DataChannelEvent::OnBufferedAmountLow => {
            flow_control.pressured = false;
            Ok(())
        }
        DataChannelEvent::OnMessage(message) => {
            let Some(request) = decrypt_daemon_request(stream_key, message.data.as_ref()) else {
                return Err(LocalWebrtcTerminalCause::InvalidRequest);
            };
            let request_count = pending_requests
                .iter()
                .filter(|pending| matches!(pending, PendingLocalWebrtcRequest::Request(_)))
                .count();
            if request_count >= LOCAL_WEBRTC_PENDING_REQUESTS {
                if let Some(PendingLocalWebrtcRequest::QueueOverflow(count)) =
                    pending_requests.back_mut()
                {
                    let Some(next_count) = count.checked_add(1) else {
                        return Err(LocalWebrtcTerminalCause::RequestQueueOverflow);
                    };
                    *count = next_count;
                } else {
                    pending_requests.push_back(PendingLocalWebrtcRequest::QueueOverflow(1));
                }
                return Ok(());
            }
            pending_requests.push_back(PendingLocalWebrtcRequest::Request(Box::new(request)));
            Ok(())
        }
        DataChannelEvent::OnClose => Err(LocalWebrtcTerminalCause::ChannelClosed),
        DataChannelEvent::OnError => Err(LocalWebrtcTerminalCause::ChannelError),
        _ => Ok(()),
    }
}

async fn answer_offer(
    request: LocalWebrtcSignalRequest,
    runtime_tx: ControlSender,
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
        peer_state: peer_state.clone(),
        gather_complete_tx,
    });

    let peer_connection = PeerConnectionBuilder::new()
        .with_handler(handler.clone())
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
    Ok(LocalWebrtcAnswer {
        answer,
        peer,
        peer_state,
        #[cfg(test)]
        handler,
    })
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

fn encrypt_daemon_entity_frame(
    key: &AesGcmKey,
    frame: &DaemonEntityFrame,
) -> LocalWebrtcResult<String> {
    let plaintext =
        serde_json::to_vec(frame).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let envelope = encrypt_aes_gcm(key, &plaintext, 1)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    serde_json::to_string(&envelope).map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))
}

fn framed_daemon_response(
    key: &AesGcmKey,
    response: &DaemonResponse,
) -> LocalWebrtcResult<Vec<String>> {
    let encrypted = encrypt_daemon_response(key, response)?;
    let encrypted = if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        encrypt_daemon_response(
            key,
            &response_with_diagnostic(DaemonDiagnostic::action_failure(
                "local_webrtc_data_channel",
                format!(
                    "encrypted daemon response exceeded {} byte limit",
                    LOCAL_WEBRTC_MAX_DELIVERY_BYTES
                ),
            )),
        )?
    } else {
        encrypted
    };
    let message_id = random_token("response")?;
    frame_encrypted_daemon_delivery(
        DaemonLocalWebrtcDeliveryKind::DaemonResponse,
        &message_id,
        &encrypted,
    )
}

fn framed_daemon_entity_frame(
    key: &AesGcmKey,
    frame: &DaemonEntityFrame,
) -> LocalWebrtcResult<Vec<String>> {
    let encrypted = encrypt_daemon_entity_frame(key, frame)?;
    if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon entity frame exceeded {LOCAL_WEBRTC_MAX_DELIVERY_BYTES} byte limit"
        )));
    }
    let message_id = random_token("entity")?;
    frame_encrypted_daemon_delivery(
        DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame,
        &message_id,
        &encrypted,
    )
}

fn frame_encrypted_daemon_delivery(
    delivery_kind: DaemonLocalWebrtcDeliveryKind,
    message_id: &str,
    encrypted: &str,
) -> LocalWebrtcResult<Vec<String>> {
    if encrypted.len() > LOCAL_WEBRTC_MAX_DELIVERY_BYTES {
        return Err(LocalWebrtcError::Webrtc(format!(
            "encrypted daemon delivery exceeded {LOCAL_WEBRTC_MAX_DELIVERY_BYTES} byte limit"
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
        let frame = DaemonLocalWebrtcDeliveryChunk {
            version: LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
            delivery_kind,
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
        let frame = DaemonLocalWebrtcDeliveryChunk {
            version: LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
            delivery_kind,
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

fn entity_frame_subscription_id(frame: &DaemonEntityFrame) -> &str {
    match frame {
        DaemonEntityFrame::Snapshot {
            subscription_id, ..
        }
        | DaemonEntityFrame::Upsert {
            subscription_id, ..
        }
        | DaemonEntityFrame::Patch {
            subscription_id, ..
        }
        | DaemonEntityFrame::Remove {
            subscription_id, ..
        }
        | DaemonEntityFrame::Error {
            subscription_id, ..
        } => subscription_id,
    }
}

fn response_with_diagnostic(diagnostic: DaemonDiagnostic) -> DaemonResponse {
    DaemonResponse {
        kind: botster_hub_client::DaemonResponseKind::OperatorError,
        status: None,
        sessions: Vec::new(),
        session_types: Vec::new(),
        session_type_definition: None,
        resolved_session_type: None,
        session_context: None,
        read_screen: None,
        mode_flags: None,
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
        hub_update: None,
        package_decision: None,
        lifecycle: Vec::new(),
        plugin_worker_counters: None,
        plugin_resource_counters: None,
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
        send_fails: AtomicBool,
        sent_before_low_water: AtomicBool,
        poll_ends: AtomicBool,
        event_notify: tokio::sync::Notify,
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

    fn encrypted_request_event(key: &AesGcmKey, request: &DaemonRequest) -> DataChannelEvent {
        let plaintext = serde_json::to_vec(request).unwrap();
        let envelope = encrypt_aes_gcm(key, &plaintext, 1).unwrap();
        let data = serde_json::to_vec(&envelope).unwrap();
        DataChannelEvent::OnMessage(RTCDataChannelMessage {
            is_string: true,
            data: data.as_slice().into(),
        })
    }

    fn test_peer_state(grant_id: &str) -> LocalWebrtcPeerState {
        let (runtime_tx, _runtime_rx) = tokio_mpsc::channel(64);
        LocalWebrtcPeerState::new(grant_id.to_string(), runtime_tx)
    }

    fn receive_test_runtime_message(
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

    fn run_idle_pressure_case(
        terminal_cause: Option<LocalWebrtcTerminalCause>,
    ) -> (FakeDataChannel, Option<LocalWebrtcSendFailure>) {
        let key = AesGcmKey::from_slice(&[15; 32]).unwrap();
        let data_channel = FakeDataChannel::default();
        {
            let mut events = data_channel.events.lock().unwrap();
            events.push_back(DataChannelEvent::OnBufferedAmountHigh);
            events.push_back(encrypted_request_event(&key, &DaemonRequest::Status));
            if terminal_cause.is_none() {
                events.push_back(DataChannelEvent::OnBufferedAmountLow);
                events.push_back(DataChannelEvent::OnClose);
            }
        }
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-idle-pressure".to_string(),
            runtime_tx,
        ));
        let responder = std::thread::spawn(move || {
            let ControlMessage::Request {
                request, reply_tx, ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected daemon request before peer cleanup");
            };
            assert_eq!(*request, DaemonRequest::Status);
            reply_tx
                .send(Ok(response_with_diagnostic(DaemonDiagnostic::connected(
                    "fixture",
                ))))
                .unwrap();
            assert!(matches!(
                receive_test_runtime_message(&mut runtime_rx),
                ControlMessage::LocalWebrtcPeerClosed { grant_id, .. }
                    if grant_id == "grant-idle-pressure"
            ));
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_sender = peer_state.runtime_tx.clone();
        let (entity_frame_tx, entity_frame_rx) =
            tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let failure = runtime.block_on(async {
            let delivery = run_data_channel(
                &data_channel,
                &key,
                peer_state.as_ref(),
                &runtime_sender,
                entity_frame_tx,
                entity_frame_rx,
            );
            tokio::pin!(delivery);
            if let Some(cause) = terminal_cause {
                assert!(
                    timeout(Duration::from_millis(20), delivery.as_mut())
                        .await
                        .is_err(),
                    "scheduler time alone must not close a live pressured peer"
                );
                peer_state.publish_peer_terminal(cause);
            }
            timeout(Duration::from_millis(250), delivery.as_mut())
                .await
                .expect("outer data-channel loop must finish on low water, close, or peer terminal")
        });
        responder.join().unwrap();
        (data_channel, failure)
    }

    fn run_shutdown_response_delivery_case(
        send_fails: bool,
    ) -> (FakeDataChannel, Option<LocalWebrtcSendFailure>) {
        let key = AesGcmKey::from_slice(&[16; 32]).unwrap();
        let data_channel = FakeDataChannel::default();
        data_channel.send_fails.store(send_fails, Ordering::Release);
        {
            let mut events = data_channel.events.lock().unwrap();
            events.push_back(encrypted_request_event(
                &key,
                &DaemonRequest::DaemonShutdown,
            ));
        }
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-shutdown-delivery".to_string(),
            runtime_tx,
        ));
        let responder_peer_state = peer_state.clone();
        let responder = std::thread::spawn(move || {
            let ControlMessage::Request {
                request,
                reply_tx,
                response_delivery_rx,
                grant_id,
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected daemon shutdown request");
            };
            assert_eq!(grant_id.as_deref(), Some("grant-shutdown-delivery"));
            assert_eq!(*request, DaemonRequest::DaemonShutdown);
            let response_delivery_rx =
                response_delivery_rx.expect("WebRTC shutdown has delivery receiver");
            reply_tx
                .send(Ok(response_with_diagnostic(DaemonDiagnostic::connected(
                    "shutdown-fixture",
                ))))
                .unwrap();
            response_delivery_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("WebRTC delivery outcome releases shutdown completion");
            responder_peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::PeerClosed);
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_sender = peer_state.runtime_tx.clone();
        let (entity_frame_tx, entity_frame_rx) =
            tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let failure = runtime.block_on(run_data_channel(
            &data_channel,
            &key,
            peer_state.as_ref(),
            &runtime_sender,
            entity_frame_tx,
            entity_frame_rx,
        ));
        responder.join().unwrap();
        (data_channel, failure)
    }

    #[test]
    fn local_webrtc_shutdown_success_releases_delivery_completion() {
        let (data_channel, failure) = run_shutdown_response_delivery_case(false);

        assert!(failure.is_none());
        assert!(!data_channel.sent.lock().unwrap().is_empty());
    }

    #[test]
    fn entity_subscription_multiplexes_after_ack_and_cleans_up_with_peer() {
        let key = AesGcmKey::from_slice(&[21; 32]).unwrap();
        let data_channel = Arc::new(FakeDataChannel::default());
        {
            let mut events = data_channel.events.lock().unwrap();
            events.push_back(encrypted_request_event(
                &key,
                &DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "entity-fixture".to_string(),
                },
            ));
        }
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-entity-fixture".to_string(),
            runtime_tx,
        ));
        let responder_peer_state = peer_state.clone();
        let responder_data_channel = data_channel.clone();
        let responder_key = key.clone();
        let responder = std::thread::spawn(move || {
            let ControlMessage::SubscribeEntities {
                entity_type,
                subscription_id,
                frame_tx,
                reply_tx,
                grant_id,
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected WebRTC entity subscription registration");
            };
            assert_eq!(entity_type, "session");
            assert_eq!(subscription_id, "entity-fixture");
            assert_eq!(grant_id.as_deref(), Some("grant-entity-fixture"));
            let mut subscribed = response_with_diagnostic(DaemonDiagnostic::connected("fixture"));
            subscribed.kind = botster_hub_client::DaemonResponseKind::EntitySubscribed;
            reply_tx.send(Ok(subscribed)).unwrap();
            frame_tx
                .try_send(DaemonEntityFrame::Snapshot {
                    subscription_id: "entity-fixture".to_string(),
                    entity_type: "session".to_string(),
                    snapshot_seq: 1,
                    items: Vec::new(),
                    resync_reason: None,
                })
                .unwrap();
            frame_tx
                .try_send(DaemonEntityFrame::Snapshot {
                    subscription_id: "entity-fixture".to_string(),
                    entity_type: "session".to_string(),
                    snapshot_seq: 2,
                    items: Vec::new(),
                    resync_reason: Some("subscriber_overflow".to_string()),
                })
                .unwrap();

            let deadline = Instant::now() + Duration::from_secs(1);
            while responder_data_channel.sent.lock().unwrap().len() < 3 {
                assert!(
                    Instant::now() < deadline,
                    "subscribe ack and encrypted overflow recovery must complete"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            responder_data_channel
                .events
                .lock()
                .unwrap()
                .push_back(encrypted_request_event(
                    &responder_key,
                    &DaemonRequest::Status,
                ));
            responder_data_channel.event_notify.notify_one();

            let ControlMessage::Request {
                request, reply_tx, ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected ordinary request while entity subscription is active");
            };
            assert_eq!(*request, DaemonRequest::Status);
            let mut status = response_with_diagnostic(DaemonDiagnostic::connected("fixture"));
            status.kind = botster_hub_client::DaemonResponseKind::Status;
            reply_tx.send(Ok(status)).unwrap();

            let deadline = Instant::now() + Duration::from_secs(1);
            while responder_data_channel.sent.lock().unwrap().len() < 4 {
                assert!(
                    Instant::now() < deadline,
                    "all multiplexed deliveries complete"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            responder_peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::PeerClosed);
            let ControlMessage::LocalWebrtcPeerClosed {
                entity_subscription_ids,
                ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected peer cleanup");
            };
            assert_eq!(entity_subscription_ids, vec!["entity-fixture"]);
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_sender = peer_state.runtime_tx.clone();
        let (entity_frame_tx, entity_frame_rx) =
            tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let failure = runtime.block_on(run_data_channel(
            data_channel.as_ref(),
            &key,
            peer_state.as_ref(),
            &runtime_sender,
            entity_frame_tx,
            entity_frame_rx,
        ));
        responder.join().unwrap();
        assert!(failure.is_none());

        let deliveries = data_channel
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|serialized| {
                let chunk =
                    serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(serialized).unwrap();
                assert_eq!(chunk.chunk_count, 1);
                let envelope = serde_json::from_str::<AesGcmEnvelope>(&chunk.payload).unwrap();
                let plaintext = decrypt_aes_gcm(&key, &envelope).unwrap();
                (chunk.delivery_kind, plaintext)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            deliveries.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
            vec![
                DaemonLocalWebrtcDeliveryKind::DaemonResponse,
                DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame,
                DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame,
                DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            ]
        );
        let snapshot: DaemonEntityFrame = serde_json::from_slice(&deliveries[1].1).unwrap();
        assert_eq!(entity_frame_subscription_id(&snapshot), "entity-fixture");
        let resync: DaemonEntityFrame = serde_json::from_slice(&deliveries[2].1).unwrap();
        assert!(matches!(
            resync,
            DaemonEntityFrame::Snapshot {
                snapshot_seq: 2,
                ref items,
                resync_reason: Some(ref reason),
                ..
            } if items.is_empty() && reason == "subscriber_overflow"
        ));
    }

    #[test]
    fn replacement_peer_rejects_prior_generation_frames_and_delivers_current_generation() {
        let key = AesGcmKey::from_slice(&[22; 32]).unwrap();
        let data_channel = Arc::new(FakeDataChannel::default());
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "replacement-grant".to_string(),
            runtime_tx,
        ));
        peer_state.add_entity_subscription("generation-2".to_string());
        let responder_peer_state = peer_state.clone();
        let responder_data_channel = data_channel.clone();
        let runtime_sender = peer_state.runtime_tx.clone();
        let (entity_frame_tx, entity_frame_rx) = tokio_mpsc::channel(2);
        entity_frame_tx
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: "generation-1".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 1,
                items: Vec::new(),
                resync_reason: None,
            })
            .unwrap();
        entity_frame_tx
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: "generation-2".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 2,
                items: Vec::new(),
                resync_reason: None,
            })
            .unwrap();
        let responder = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(1);
            while responder_data_channel.sent.lock().unwrap().is_empty() {
                assert!(
                    Instant::now() < deadline,
                    "current-generation frame is delivered"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            responder_peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::PeerClosed);
            assert!(matches!(
                receive_test_runtime_message(&mut runtime_rx),
                ControlMessage::LocalWebrtcPeerClosed { grant_id, .. }
                    if grant_id == "replacement-grant"
            ));
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let failure = runtime.block_on(run_data_channel(
            data_channel.as_ref(),
            &key,
            peer_state.as_ref(),
            &runtime_sender,
            entity_frame_tx,
            entity_frame_rx,
        ));
        responder.join().unwrap();
        assert!(failure.is_none());

        let sent = data_channel.sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "the prior-generation frame must be dropped");
        let chunk: DaemonLocalWebrtcDeliveryChunk = serde_json::from_str(&sent[0]).unwrap();
        assert_eq!(
            chunk.delivery_kind,
            DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame
        );
        let envelope: AesGcmEnvelope = serde_json::from_str(&chunk.payload).unwrap();
        let plaintext = decrypt_aes_gcm(&key, &envelope).unwrap();
        let frame: DaemonEntityFrame = serde_json::from_slice(&plaintext).unwrap();
        assert_eq!(entity_frame_subscription_id(&frame), "generation-2");
    }

    #[test]
    fn local_webrtc_shutdown_send_failure_releases_delivery_completion() {
        let (_data_channel, failure) = run_shutdown_response_delivery_case(true);

        assert_eq!(
            failure.expect("send failure remains visible").cause,
            LocalWebrtcTerminalCause::SendText
        );
    }

    #[test]
    fn recoverable_disconnect_after_response_preserves_followup_shutdown() {
        let key = AesGcmKey::from_slice(&[17; 32]).unwrap();
        let data_channel = Arc::new(FakeDataChannel::default());
        {
            let mut events = data_channel.events.lock().unwrap();
            events.push_back(encrypted_request_event(&key, &DaemonRequest::Status));
            events.push_back(encrypted_request_event(
                &key,
                &DaemonRequest::ShutdownSession {
                    session_id: "recoverable-disconnect-session".to_string(),
                },
            ));
        }
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-recoverable-disconnect".to_string(),
            runtime_tx,
        ));
        let responder_peer_state = peer_state.clone();
        let responder_data_channel = data_channel.clone();
        let responder = std::thread::spawn(move || {
            let ControlMessage::Request {
                request, reply_tx, ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected status request");
            };
            assert_eq!(*request, DaemonRequest::Status);
            reply_tx
                .send(Ok(response_with_diagnostic(DaemonDiagnostic::connected(
                    "completed-response",
                ))))
                .unwrap();

            assert_eq!(
                responder_peer_state
                    .observe_peer_connection_state(RTCPeerConnectionState::Disconnected),
                None,
                "disconnected is recoverable and must not terminate the peer"
            );

            let ControlMessage::Request {
                request, reply_tx, ..
            } = receive_test_runtime_message(&mut runtime_rx)
            else {
                panic!("expected shutdown-session request after recoverable disconnect");
            };
            assert_eq!(
                *request,
                DaemonRequest::ShutdownSession {
                    session_id: "recoverable-disconnect-session".to_string(),
                }
            );
            reply_tx
                .send(Ok(response_with_diagnostic(DaemonDiagnostic::connected(
                    "followup-shutdown",
                ))))
                .unwrap();

            let deadline = Instant::now() + Duration::from_secs(1);
            while responder_data_channel.sent.lock().unwrap().len() < 2 {
                assert!(
                    Instant::now() < deadline,
                    "both responses must complete before terminal close"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(
                responder_peer_state.observe_peer_connection_state(RTCPeerConnectionState::Closed),
                Some(LocalWebrtcTerminalCause::PeerClosed)
            );
            assert!(matches!(
                receive_test_runtime_message(&mut runtime_rx),
                ControlMessage::LocalWebrtcPeerClosed { grant_id, .. }
                    if grant_id == "grant-recoverable-disconnect"
            ));
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_sender = peer_state.runtime_tx.clone();
        let (entity_frame_tx, entity_frame_rx) =
            tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let failure = runtime.block_on(run_data_channel(
            data_channel.as_ref(),
            &key,
            peer_state.as_ref(),
            &runtime_sender,
            entity_frame_tx,
            entity_frame_rx,
        ));

        responder.join().unwrap();
        assert!(failure.is_none());
        assert_eq!(data_channel.sent.lock().unwrap().len(), 2);
        assert!(data_channel.closed.load(Ordering::Acquire));
    }

    #[test]
    fn outer_loop_routes_idle_pressure_before_next_request_delivery() {
        let (resumed_channel, _) = run_idle_pressure_case(None);
        assert!(
            !resumed_channel
                .sent_before_low_water
                .load(Ordering::Acquire)
        );
        assert_eq!(resumed_channel.sent.lock().unwrap().len(), 1);
        assert!(resumed_channel.closed.load(Ordering::Acquire));
    }

    #[test]
    fn idle_pressure_wakes_for_each_distinct_peer_terminal_cause() {
        for cause in [
            LocalWebrtcTerminalCause::PeerDisconnected,
            LocalWebrtcTerminalCause::PeerFailed,
            LocalWebrtcTerminalCause::PeerClosed,
        ] {
            let (channel, failure) = run_idle_pressure_case(Some(cause));
            assert_eq!(failure.unwrap().cause, cause);
            assert!(channel.sent.lock().unwrap().is_empty());
            assert!(channel.closed.load(Ordering::Acquire));
        }
    }

    #[test]
    fn response_frames_use_one_bounded_protocol_for_small_and_large_envelopes() {
        let small = frame_encrypted_daemon_delivery(
            DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            "response-small",
            "encrypted",
        )
        .unwrap();
        assert_eq!(small.len(), 1);
        let small: DaemonLocalWebrtcDeliveryChunk = serde_json::from_str(&small[0]).unwrap();
        assert_eq!(small.version, LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION);
        assert_eq!(
            small.delivery_kind,
            DaemonLocalWebrtcDeliveryKind::DaemonResponse
        );
        assert_eq!(small.message_id, "response-small");
        assert_eq!(small.chunk_index, 0);
        assert_eq!(small.chunk_count, 1);
        assert_eq!(small.total_bytes, 9);
        assert_eq!(small.payload, "encrypted");

        let encrypted = "a".repeat(256 * 1024 + 1);
        let frames = frame_encrypted_daemon_delivery(
            DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            "response-large",
            &encrypted,
        )
        .unwrap();
        assert!(frames.len() > 1);
        let chunks = frames
            .iter()
            .map(|frame| {
                assert!(frame.len() < LOCAL_WEBRTC_MAX_FRAME_BYTES);
                serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(frame).unwrap()
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
        let encrypted = "a".repeat(LOCAL_WEBRTC_MAX_DELIVERY_BYTES + 1);
        let error = frame_encrypted_daemon_delivery(
            DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            "response-over-budget",
            &encrypted,
        )
        .expect_err("over-budget response must fail before framing");
        assert!(error.to_string().contains("exceeded 16777216 byte limit"));
    }

    #[test]
    fn over_budget_response_is_replaced_before_any_rejected_payload_is_framed() {
        let key = AesGcmKey::from_slice(&[7; 32]).unwrap();
        let mut response = response_with_diagnostic(DaemonDiagnostic::connected("fixture"));
        response.plugin_tool_result = Value::String("x".repeat(LOCAL_WEBRTC_MAX_DELIVERY_BYTES));

        let frames = framed_daemon_response(&key, &response).unwrap();
        assert_eq!(frames.len(), 1);
        let chunk: DaemonLocalWebrtcDeliveryChunk = serde_json::from_str(&frames[0]).unwrap();
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
    fn flow_control_pressure_is_cleared_only_by_low_water() {
        let key = AesGcmKey::from_slice(&[9; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        assert!(
            apply_data_channel_event(
                DataChannelEvent::OnBufferedAmountHigh,
                &key,
                &mut pending,
                &mut flow_control,
            )
            .is_ok()
        );
        assert!(flow_control.pressured);

        assert!(
            apply_data_channel_event(
                DataChannelEvent::OnOpen,
                &key,
                &mut pending,
                &mut flow_control,
            )
            .is_ok()
        );
        assert!(flow_control.pressured);

        assert!(
            apply_data_channel_event(
                DataChannelEvent::OnBufferedAmountLow,
                &key,
                &mut pending,
                &mut flow_control,
            )
            .is_ok()
        );
        assert!(!flow_control.pressured);
    }

    fn active_pressure_peer_terminal_case(
        cause: LocalWebrtcTerminalCause,
    ) -> (LocalWebrtcSendFailure, LocalWebrtcSenderTerminalRecord) {
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
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-fixture".to_string(),
            runtime_tx,
        ));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut flow_control = LocalWebrtcFlowControl::default();

        let failure = runtime.block_on(async {
            let frames = ["partial".to_string(), "completion".to_string()];
            let delivery = send_response_frames(
                &data_channel,
                &key,
                &frames,
                &mut pending,
                &mut flow_control,
                peer_state.as_ref(),
            );
            tokio::pin!(delivery);
            assert!(
                timeout(Duration::from_millis(20), delivery.as_mut())
                    .await
                    .is_err(),
                "elapsed scheduler time must not close a live pressured peer"
            );
            peer_state.publish_peer_terminal(cause);
            timeout(Duration::from_millis(250), delivery.as_mut())
                .await
                .expect("peer terminal state must wake active pressure")
                .expect_err("peer terminal state must fail pending delivery")
        });
        assert_eq!(failure.cause, cause);
        assert_eq!(failure.next_chunk_index, 1);
        assert_eq!(failure.last_sent_chunk_index, Some(0));
        assert_eq!(failure.total_chunks, 2);
        assert!(failure.pressured);
        assert_eq!(data_channel.sent.lock().unwrap().as_slice(), &["partial"]);

        runtime.block_on(close_data_channel(
            &data_channel,
            &mut pending,
            peer_state.as_ref(),
            cause,
        ));
        assert!(pending.is_empty());
        assert!(data_channel.closed.load(Ordering::Acquire));
        let ControlMessage::LocalWebrtcPeerClosed {
            grant_id,
            terminal_record,
            ..
        } = receive_test_runtime_message(&mut runtime_rx)
        else {
            panic!("expected peer cleanup");
        };
        assert_eq!(grant_id, "grant-fixture");
        (failure, terminal_record)
    }

    #[test]
    fn active_pressure_does_not_expire_and_wakes_for_each_peer_terminal_cause() {
        for cause in [
            LocalWebrtcTerminalCause::PeerDisconnected,
            LocalWebrtcTerminalCause::PeerFailed,
            LocalWebrtcTerminalCause::PeerClosed,
        ] {
            let (failure, terminal_record) = active_pressure_peer_terminal_case(cause);
            assert_eq!(failure.cause, cause);
            assert_eq!(terminal_record.cause, cause);
            assert_eq!(terminal_record.next_chunk_index, 1);
            assert_eq!(terminal_record.last_sent_chunk_index, Some(0));
            assert_eq!(terminal_record.total_chunks, 2);
            assert!(terminal_record.pressured);
        }
    }

    #[test]
    fn partial_chunked_response_records_message_and_nonzero_progress() {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountHigh);
        let key = AesGcmKey::from_slice(&[16; 32]).unwrap();
        let frames = frame_encrypted_daemon_delivery(
            DaemonLocalWebrtcDeliveryKind::DaemonResponse,
            "response-progress",
            &"a".repeat(256 * 1024),
        )
        .unwrap();
        assert!(frames.len() > 1);
        let mut pending = VecDeque::new();
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-progress".to_string(),
            runtime_tx,
        ));
        peer_state.begin_request(&DaemonRequest::Status);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut flow_control = LocalWebrtcFlowControl::default();

        let failure = runtime.block_on(async {
            let delivery = send_response_frames(
                &data_channel,
                &key,
                &frames,
                &mut pending,
                &mut flow_control,
                peer_state.as_ref(),
            );
            tokio::pin!(delivery);
            assert!(
                timeout(Duration::from_millis(20), delivery.as_mut())
                    .await
                    .is_err()
            );
            peer_state.publish_peer_terminal(LocalWebrtcTerminalCause::PeerDisconnected);
            delivery
                .await
                .expect_err("peer terminal must retain partial progress")
        });
        assert_eq!(failure.cause, LocalWebrtcTerminalCause::PeerDisconnected);
        assert_eq!(failure.message_id, "response-progress");
        assert_eq!(failure.next_chunk_index, 1);
        assert_eq!(failure.total_chunks, frames.len());

        runtime.block_on(close_data_channel(
            &data_channel,
            &mut pending,
            peer_state.as_ref(),
            failure.cause,
        ));
        let ControlMessage::LocalWebrtcPeerClosed {
            terminal_record, ..
        } = receive_test_runtime_message(&mut runtime_rx)
        else {
            panic!("expected terminal record after partial response");
        };
        assert_eq!(
            terminal_record.message_id.as_deref(),
            Some("response-progress")
        );
        assert_eq!(terminal_record.next_chunk_index, 1);
        assert_eq!(terminal_record.last_sent_chunk_index, Some(0));
        assert_eq!(terminal_record.total_chunks, frames.len());
        assert!(terminal_record.pressured);
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
        let mut flow_control = LocalWebrtcFlowControl::default();
        let peer_state = test_peer_state("grant-high-low");

        let completed = runtime.block_on(send_response_frames(
            &data_channel,
            &key,
            &["first".to_string(), "second".to_string()],
            &mut pending,
            &mut flow_control,
            &peer_state,
        ));

        assert!(completed.is_ok());
        assert_eq!(
            data_channel.sent.lock().unwrap().as_slice(),
            &["first", "second"]
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn post_final_high_water_survives_response_boundary_and_idle_low_clears_it() {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountHigh);
        let key = AesGcmKey::from_slice(&[12; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-response-boundary");

        let first = runtime.block_on(send_response_frames(
            &data_channel,
            &key,
            &["response-one".to_string()],
            &mut pending,
            &mut flow_control,
            &peer_state,
        ));
        assert!(first.is_ok());
        assert!(flow_control.pressured);

        assert!(
            apply_data_channel_event(
                DataChannelEvent::OnBufferedAmountLow,
                &key,
                &mut pending,
                &mut flow_control,
            )
            .is_ok()
        );
        let second = runtime.block_on(send_response_frames(
            &data_channel,
            &key,
            &["response-two".to_string()],
            &mut pending,
            &mut flow_control,
            &peer_state,
        ));

        assert!(second.is_ok());
        assert!(!flow_control.pressured);
        assert_eq!(
            data_channel.sent.lock().unwrap().as_slice(),
            &["response-one", "response-two"]
        );
    }

    #[test]
    fn next_response_waits_for_low_water_when_pressure_blocks_its_first_frame() {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountHigh);
        let key = AesGcmKey::from_slice(&[13; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-next-response");

        assert!(
            runtime
                .block_on(send_response_frames(
                    &data_channel,
                    &key,
                    &["response-one".to_string()],
                    &mut pending,
                    &mut flow_control,
                    &peer_state,
                ))
                .is_ok()
        );
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnBufferedAmountLow);
        runtime
            .block_on(send_response_frames(
                &data_channel,
                &key,
                &["response-two".to_string()],
                &mut pending,
                &mut flow_control,
                &peer_state,
            ))
            .expect("low water must resume the pressured next response");

        assert!(!flow_control.pressured);
        assert_eq!(
            data_channel.sent.lock().unwrap().as_slice(),
            &["response-one", "response-two"]
        );
    }

    #[test]
    fn send_failures_report_distinct_bounded_terminal_causes() {
        let key = AesGcmKey::from_slice(&[14; 32]).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-send-failures");

        for (event, expected_cause) in [
            (
                DataChannelEvent::OnClose,
                LocalWebrtcTerminalCause::ChannelClosed,
            ),
            (
                DataChannelEvent::OnError,
                LocalWebrtcTerminalCause::ChannelError,
            ),
        ] {
            let data_channel = FakeDataChannel::default();
            data_channel.events.lock().unwrap().push_back(event);
            let mut pending = VecDeque::new();
            let mut flow_control = LocalWebrtcFlowControl::default();
            let failure = runtime
                .block_on(send_response_frames(
                    &data_channel,
                    &key,
                    &["response".to_string()],
                    &mut pending,
                    &mut flow_control,
                    &peer_state,
                ))
                .expect_err("terminal channel event must fail response delivery");
            assert_eq!(failure.cause, expected_cause);
            assert_eq!(failure.next_chunk_index, 1);
            assert_eq!(failure.total_chunks, 1);
        }

        let ended_channel = FakeDataChannel::default();
        ended_channel.poll_ends.store(true, Ordering::Release);
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let ended = runtime
            .block_on(send_response_frames(
                &ended_channel,
                &key,
                &["response".to_string()],
                &mut pending,
                &mut flow_control,
                &peer_state,
            ))
            .expect_err("ended polling must fail response delivery");
        assert_eq!(ended.cause, LocalWebrtcTerminalCause::PollEnded);

        let failed_channel = FakeDataChannel::default();
        failed_channel.send_fails.store(true, Ordering::Release);
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let send = runtime
            .block_on(send_response_frames(
                &failed_channel,
                &key,
                &["response".to_string()],
                &mut pending,
                &mut flow_control,
                &peer_state,
            ))
            .expect_err("send_text failure must fail response delivery");
        assert_eq!(send.cause, LocalWebrtcTerminalCause::SendText);
        assert_eq!(send.next_chunk_index, 0);
        assert_eq!(send.last_sent_chunk_index, None);
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
            .start_paused(true)
            .build()
            .unwrap();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let peer_state = test_peer_state("grant-idle-open");

        let (elapsed, completed) = runtime.block_on(async {
            let started = tokio::time::Instant::now();
            let completed = send_response_frames(
                &data_channel,
                &key,
                &frames,
                &mut pending,
                &mut flow_control,
                &peer_state,
            )
            .await;
            (started.elapsed(), completed)
        });

        assert!(completed.is_ok());
        assert_eq!(data_channel.sent.lock().unwrap().len(), frames.len());
        assert!(
            elapsed.is_zero(),
            "idle event probes must not throttle response frames: {:?}",
            elapsed
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
        let mut flow_control = LocalWebrtcFlowControl::default();
        let peer_state = test_peer_state("grant-inbound-request");

        let completed = runtime.block_on(send_response_frames(
            &data_channel,
            &key,
            &["first".to_string(), "second".to_string()],
            &mut pending,
            &mut flow_control,
            &peer_state,
        ));

        assert!(completed.is_ok());
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
        let mut flow_control = LocalWebrtcFlowControl::default();

        let inbound_requests = LOCAL_WEBRTC_PENDING_REQUESTS + 4;
        for _ in 0..inbound_requests {
            assert!(
                apply_data_channel_event(
                    encrypted_request_event(&key, &DaemonRequest::Status),
                    &key,
                    &mut pending,
                    &mut flow_control,
                )
                .is_ok()
            );
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
    fn interleaved_overflow_runs_preserve_fifo_response_order() {
        let key = AesGcmKey::from_slice(&[11; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        {
            let mut apply_request = |request: &DaemonRequest| {
                apply_data_channel_event(
                    encrypted_request_event(&key, request),
                    &key,
                    &mut pending,
                    &mut flow_control,
                )
            };

            for _ in 0..LOCAL_WEBRTC_PENDING_REQUESTS {
                assert!(apply_request(&DaemonRequest::Status).is_ok());
            }
            assert!(apply_request(&DaemonRequest::Status).is_ok());
        }
        assert!(matches!(
            pop_pending_request(&mut pending),
            Some(PendingLocalWebrtcRequest::Request(request)) if *request == DaemonRequest::Status
        ));

        assert!(
            apply_data_channel_event(
                encrypted_request_event(&key, &DaemonRequest::ListSessions),
                &key,
                &mut pending,
                &mut flow_control,
            )
            .is_ok()
        );
        assert!(
            apply_data_channel_event(
                encrypted_request_event(&key, &DaemonRequest::Status),
                &key,
                &mut pending,
                &mut flow_control,
            )
            .is_ok()
        );

        let emitted_order = std::iter::from_fn(|| pop_pending_request(&mut pending))
            .map(|pending| match pending {
                PendingLocalWebrtcRequest::Request(request)
                    if *request == DaemonRequest::ListSessions =>
                {
                    "new-request"
                }
                PendingLocalWebrtcRequest::Request(_) => "status",
                PendingLocalWebrtcRequest::EntityFrame(_) => "entity",
                PendingLocalWebrtcRequest::QueueOverflow(_) => "overflow",
            })
            .collect::<Vec<_>>();
        let mut expected_order = vec!["status"; LOCAL_WEBRTC_PENDING_REQUESTS - 1];
        expected_order.extend(["overflow", "new-request", "overflow"]);
        assert_eq!(emitted_order, expected_order);
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

    // --- Production peer_failed teardown harnesses (H1–H3) ---

    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::thread;
    use webrtc::data_channel::RTCDataChannelInit;
    use webrtc::runtime::{
        Receiver as AsyncReceiver, Sender as AsyncSender, channel as webrtc_channel,
        default_runtime,
    };

    use crate::daemon_transport::{DaemonControlState, EntityFrameSender, handle_control_message};
    use crate::{
        DataDirectoryOption, HostIdentityOptions, HubDaemon, HubStartupOptions, RuntimeEnvironment,
        SessionDefaults,
    };

    /// Serializes teardown tests that share the process-global dedicated-runtime worker
    /// counter or inject a close hang, so parallel cargo tests do not false-fail each other.
    fn teardown_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Serializes Spawn → worker census capture. Session-worker sockets may not live under the
    /// hub data directory (core uses a separate control-socket path), so capture relies on a
    /// process-global "new pid" baseline that is only safe while no other harness is spawning.
    fn spawn_capture_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    struct TestOfferHandler {
        gather_complete_tx: AsyncSender<()>,
        connected_tx: AsyncSender<()>,
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

    struct TestOfferPeer {
        peer: Box<dyn PeerConnection>,
        data_channel: Arc<dyn DataChannel>,
        connected_rx: AsyncReceiver<()>,
        data_channel_open_rx: AsyncReceiver<()>,
        data_channel_message_rx: AsyncReceiver<String>,
    }

    impl TestOfferPeer {
        async fn create_offer() -> (Self, Value) {
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
            let _ = timeout(Duration::from_secs(5), gather_complete_rx.recv()).await;
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
                },
                serde_json::to_value(offer).expect("serialize offer"),
            )
        }

        async fn accept_answer(&mut self, answer: Value) {
            let answer =
                serde_json::from_value::<RTCSessionDescription>(answer).expect("parse answer");
            self.peer
                .set_remote_description(answer)
                .await
                .expect("set remote answer");
            timeout(Duration::from_secs(15), self.connected_rx.recv())
                .await
                .expect("timed out waiting for offer peer connected")
                .expect("connected signal");
            timeout(Duration::from_secs(10), self.data_channel_open_rx.recv())
                .await
                .expect("timed out waiting for data channel open")
                .expect("open signal");
        }

        async fn encrypted_request(
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
                    let response =
                        timeout(Duration::from_secs(10), self.data_channel_message_rx.recv())
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
                }
            }
        }
    }

    fn unique_test_data_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "botster-hub-webrtc-teardown-{label}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn start_test_daemon(data_directory: PathBuf) -> HubDaemon {
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
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build teardown test config");
        HubDaemon::start(config).expect("start teardown test daemon")
    }

    fn wait_until(deadline: Instant, mut predicate: impl FnMut() -> bool, label: &str) {
        if soft_wait_until(deadline, &mut predicate) {
            return;
        }
        panic!("timed out waiting for {label}");
    }

    /// Soft wait used from Drop/cleanup paths. Never panics (panic-in-drop aborts).
    fn soft_wait_until(deadline: Instant, predicate: &mut dyn FnMut() -> bool) -> bool {
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        false
    }

    struct PeerHarness {
        daemon: HubDaemon,
        state: DaemonControlState,
        terminal_path: PathBuf,
        control_tx: ControlSender,
        control_rx: tokio_mpsc::Receiver<ControlMessage>,
        transport_handle: tokio::runtime::Handle,
        /// Keep a multi-thread runtime alive so Handle remains valid.
        _transport_runtime: tokio::runtime::Runtime,
        data_directory: PathBuf,
        /// Worker-backed sessions that must be shut down before Hub stop.
        owned_sessions: Vec<String>,
        /// Exact session-worker identities captured at Spawn readiness.
        owned_workers: Vec<OwnedWorkerIdentity>,
        sessions_cleaned: bool,
    }

    thread_local! {
        static LAST_SESSION_CLEANUP_ERROR: std::cell::RefCell<Option<String>> =
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
        fn new(label: &str) -> Self {
            LAST_SESSION_CLEANUP_ERROR.with(|slot| *slot.borrow_mut() = None);
            let data_directory = unique_test_data_dir(label);
            let terminal_path = data_directory.join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
            let daemon = start_test_daemon(data_directory.clone());
            let (control_tx, control_rx) = tokio_mpsc::channel(256);
            let transport_runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .thread_name("botster-webrtc-test-control")
                .build()
                .expect("control transport runtime");
            let transport_handle = transport_runtime.handle().clone();
            Self {
                daemon,
                state: DaemonControlState::default(),
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

        fn control_request(&mut self, request: DaemonRequest) -> Option<DaemonResponse> {
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
                },
            );
            reply_rx.blocking_recv().ok().and_then(|result| result.ok())
        }

        fn list_session_lifecycle(&mut self, session_id: &str) -> Option<String> {
            let response = self.control_request(DaemonRequest::ListSessions)?;
            response
                .sessions
                .into_iter()
                .find(|session| session.session_id == session_id)
                .map(|session| session.lifecycle)
        }

        fn shutdown_and_remove_session(&mut self, session_id: &str) -> Result<(), String> {
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

        fn shutdown_owned_sessions(&mut self) -> Result<(), String> {
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

        fn process_until_peer_closed(&mut self, grant_id: &str, deadline: Instant) {
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

        fn signal_peer(&mut self, origin: &str) -> LiveSignaledPeer {
            let bootstrap = self
                .daemon
                .local_webrtc()
                .issue_bootstrap("botster-web", "web-client", origin)
                .expect("issue bootstrap");
            let stream_key =
                secret_stream_key(&bootstrap.grant_secret).expect("bootstrap secret is stream key");
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

        fn request_on_peer(
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

        fn subscribe_entities(
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

        fn spawn_and_attach_on_peer(
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
                "spawn over local WebRTC must succeed for attach proof"
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
                botster_hub_client::DaemonResponseKind::Events,
                "attach over local WebRTC must succeed"
            );
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

        fn cleanup(mut self) {
            self.shutdown_owned_sessions()
                .expect("owned sessions must shut down and remove cleanly");
            self.daemon.local_webrtc().stop_all();
            self.daemon.stop();
            let _ = std::fs::remove_dir_all(&self.data_directory);
            // Drop runs after this; sessions_cleaned prevents double work.
        }
    }

    #[derive(Debug, Clone)]
    struct OwnedWorkerIdentity {
        pid: u32,
        pgid: u32,
        control_socket: PathBuf,
        /// Full `ps` command remainder after pid/pgid (used for harness data-dir matching).
        command: String,
        /// Exact live PIDs observed in this worker's process group at Spawn readiness.
        /// Absence proof is over this captured set, not the ambient group forever
        /// (workers may share a pgid with hub/test processes on some platforms).
        group_member_pids: Vec<u32>,
    }

    impl OwnedWorkerIdentity {
        fn is_fully_gone(&self) -> bool {
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

        fn residual_group_members(&self) -> Vec<u32> {
            self.group_member_pids
                .iter()
                .copied()
                .filter(|pid| process_is_alive(*pid))
                .collect()
        }

        fn socket_under_data_dir(&self, data_dir: &Path) -> bool {
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
        fn belongs_to_data_dir(&self, data_dir: &Path) -> bool {
            if self.socket_under_data_dir(data_dir) {
                return true;
            }
            let dir = data_dir.to_string_lossy();
            if dir.is_empty() {
                return false;
            }
            self.command.contains(dir.as_ref())
                || data_dir.canonicalize().ok().is_some_and(|canon| {
                    self.command.contains(&canon.to_string_lossy().to_string())
                })
        }

        /// True when argv0 / command is this hub worktree's `botster-session-worker` binary.
        /// Rejects workers from other pipeline tickets / worktrees on the same host.
        fn executable_from_this_worktree(&self) -> bool {
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

    fn session_worker_identities() -> Vec<OwnedWorkerIdentity> {
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
    fn worker_owned_process_tree(root_pid: u32) -> Vec<u32> {
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

    fn process_is_alive(pid: u32) -> bool {
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

    fn live_pids_in_process_group(pgid: u32) -> Vec<u32> {
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

    fn signal_pid(pid: u32, signal: &str) {
        let _ = std::process::Command::new("kill")
            .args([signal, &pid.to_string()])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }

    /// Kill exact worker PID + readiness-captured descendants, then unlink control socket.
    fn reap_owned_worker(worker: &OwnedWorkerIdentity) {
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

    fn wait_for_owned_workers_gone(workers: &[OwnedWorkerIdentity], deadline: Instant) {
        // Observation only: do not hard-reap here. Harness kill/unlink belongs solely
        // inside shutdown_owned_sessions as post-error hygiene, never as a greenwash.
        wait_until(
            deadline,
            || workers.iter().all(|worker| worker.is_fully_gone()),
            &format!("owned workers to fully exit after production cleanup: {workers:?}"),
        );
    }

    fn take_last_session_cleanup_error() -> Option<String> {
        LAST_SESSION_CLEANUP_ERROR.with(|slot| slot.borrow_mut().take())
    }

    struct LiveSignaledPeer {
        grant_id: String,
        stream_key: AesGcmKey,
        offer_peer: Option<TestOfferPeer>,
        offer_runtime: tokio::runtime::Runtime,
    }

    impl LiveSignaledPeer {
        fn close_offer(mut self) {
            if let Some(offer_peer) = self.offer_peer.take() {
                let _ = self.offer_runtime.block_on(offer_peer.peer.close());
            }
        }
    }

    fn read_terminal_record(path: &Path) -> LocalWebrtcSenderTerminalRecord {
        let bytes = std::fs::read(path).expect("read terminal record");
        serde_json::from_slice(&bytes).expect("parse terminal record")
    }

    #[test]
    fn local_webrtc_peer_failed_closes_live_peer_parks_runtime_and_clears_driver_threads() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("h1");
        let origin = "http://127.0.0.1:41791";
        let mut peer = harness.signal_peer(origin);
        let grant_id = peer.grant_id.clone();
        let subscription_id = "entity-delivery-h1".to_string();

        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 1);
        assert!(harness.daemon.local_webrtc().has_dedicated_runtime());
        assert!(LocalWebrtcTransport::dedicated_runtime_worker_threads() >= 1);
        assert_eq!(
            harness
                .daemon
                .local_webrtc()
                .close_completion_count_for(&grant_id),
            0
        );

        let subscribe = harness.subscribe_entities(&mut peer, &subscription_id);
        assert_eq!(
            subscribe.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(
            harness
                .state
                .entity_subscriptions
                .contains_key(&subscription_id),
            "entity subscription must be registered before peer_failed"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions,
            1
        );

        // Production path: handler on_connection_state_change(Failed) → cleanup_once(PeerFailed)
        // → LocalWebrtcPeerClosed → handle_control_message → remove_peer close+map+runtime drop.
        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));

        let terminal = read_terminal_record(&harness.terminal_path);
        assert_eq!(terminal.grant_id, grant_id);
        assert_eq!(terminal.cause, LocalWebrtcTerminalCause::PeerFailed);
        assert_eq!(terminal.peer_connection_state, "failed");

        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
        assert!(!harness.daemon.local_webrtc().has_dedicated_runtime());
        assert_eq!(
            harness
                .daemon
                .local_webrtc()
                .close_completion_count_for(&grant_id),
            1,
            "production forget must invoke and complete PeerConnection::close"
        );
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key(&subscription_id),
            "entity subscription must be removed on LocalWebrtcPeerClosed"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions,
            0
        );

        wait_until(
            Instant::now() + Duration::from_secs(2),
            || LocalWebrtcTransport::dedicated_runtime_worker_threads() == 0,
            "dedicated botster-local-webrtc worker threads to join",
        );

        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("h2");
        let origin = "http://127.0.0.1:41792";
        let mut peer_a = harness.signal_peer(origin);
        let mut peer_b = harness.signal_peer(origin);
        let grant_a = peer_a.grant_id.clone();
        let grant_b = peer_b.grant_id.clone();

        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 2);
        assert!(harness.daemon.local_webrtc().has_dedicated_runtime());

        let subscribe_a = harness.subscribe_entities(&mut peer_a, "entity-a");
        assert_eq!(
            subscribe_a.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        let subscribe_b = harness.subscribe_entities(&mut peer_b, "entity-b");
        assert_eq!(
            subscribe_b.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions,
            2
        );

        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&grant_a, Instant::now() + Duration::from_secs(10));

        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 1);
        assert!(
            harness.daemon.local_webrtc().has_dedicated_runtime(),
            "sibling peer must keep the dedicated runtime alive"
        );
        assert_eq!(
            harness
                .daemon
                .local_webrtc()
                .close_completion_count_for(&grant_a),
            1
        );
        assert_eq!(
            harness
                .daemon
                .local_webrtc()
                .close_completion_count_for(&grant_b),
            0
        );
        assert!(!harness.state.entity_subscriptions.contains_key("entity-a"));
        assert!(harness.state.entity_subscriptions.contains_key("entity-b"));
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions,
            1
        );

        peer_a.close_offer();
        peer_b.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("h3");
        let origin = "http://127.0.0.1:41793";
        let mut first = harness.signal_peer(origin);
        let first_grant = first.grant_id.clone();
        let subscribe = harness.subscribe_entities(&mut first, "entity-h3");
        assert_eq!(
            subscribe.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );

        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&first_grant, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&first_grant, Instant::now() + Duration::from_secs(10));
        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
        assert!(!harness.daemon.local_webrtc().has_dedicated_runtime());
        wait_until(
            Instant::now() + Duration::from_secs(2),
            || LocalWebrtcTransport::dedicated_runtime_worker_threads() == 0,
            "first dedicated runtime workers to join",
        );

        let second = harness.signal_peer(origin);
        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 1);
        assert!(
            harness.daemon.local_webrtc().has_dedicated_runtime(),
            "new signal after last-peer park must recreate the dedicated runtime"
        );
        assert!(LocalWebrtcTransport::dedicated_runtime_worker_threads() >= 1);

        first.close_offer();
        second.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_late_subscribe_entities_after_peer_closed_does_not_recreate_state() {
        let mut harness = PeerHarness::new("late-subscribe");
        let origin = "http://127.0.0.1:41794";
        let peer = harness.signal_peer(origin);
        let grant_id = peer.grant_id.clone();
        let subscription_id = "late-entity".to_string();

        // Terminal cleanup wins first (adverse queue order vs a still-queued SubscribeEntities).
        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));
        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

        let (frame_tx, _frame_rx) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::SubscribeEntities {
                entity_type: "session".to_string(),
                subscription_id: subscription_id.clone(),
                frame_tx: EntityFrameSender::Async(frame_tx),
                reply_tx,
                grant_id: Some(grant_id.clone()),
            },
        );

        let response = reply_rx
            .blocking_recv()
            .expect("reply channel open")
            .expect("daemon returns operator response");
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("local_webrtc_peer_gone")
        );
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key(&subscription_id),
            "late SubscribeEntities must not recreate daemon entity ownership after PeerClosed"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions,
            0
        );
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("close-fail-sibling");
        let origin = "http://127.0.0.1:41795";
        let mut peer_a = harness.signal_peer(origin);
        let mut peer_b = harness.signal_peer(origin);
        let grant_a = peer_a.grant_id.clone();
        let grant_b = peer_b.grant_id.clone();
        let session_b = "fail-closed-sibling-session";
        let attach_b = "fail-closed-sibling-attach";

        let _ = harness.subscribe_entities(&mut peer_a, "entity-fail-a");
        let _ = harness.subscribe_entities(&mut peer_b, "entity-fail-b");
        harness.spawn_and_attach_on_peer(&mut peer_b, session_b, attach_b);
        assert!(
            harness
                .state
                .entity_subscriptions
                .contains_key("entity-fail-b")
        );
        let live_attach_before = harness.state.lifecycle_counters.live_attach_subscriptions;
        assert!(live_attach_before >= 1);
        assert!(LocalWebrtcTransport::dedicated_runtime_worker_threads() >= 1);
        let owned_workers = harness.owned_workers.clone();
        assert!(
            !owned_workers.is_empty(),
            "spawn must capture exact worker pid/pgid/socket identity"
        );
        assert!(
            harness.list_session_lifecycle(session_b).is_some(),
            "spawned session must remain listed after attach readiness"
        );
        assert!(
            owned_workers
                .iter()
                .all(|worker| process_is_alive(worker.pid)),
            "captured session-worker PIDs must still be live after attach readiness"
        );

        harness
            .daemon
            .local_webrtc()
            .force_next_close_error_for_test(&grant_a);
        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&grant_a, Instant::now() + Duration::from_secs(10));

        // Fail-closed: ultimate close failure tears down the dedicated runtime so residual
        // PeerConnectionDriver work cannot continue while a sibling would otherwise keep it alive.
        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_a));
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_b));
        assert_eq!(harness.daemon.local_webrtc().stale_close_peer_count(), 0);
        assert!(!harness.daemon.local_webrtc().has_dedicated_runtime());
        assert_eq!(
            harness.daemon.local_webrtc().peer_state_count(),
            0,
            "fail-closed must remove primary + sibling peer_states, not only the live peer map"
        );
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key("entity-fail-a"),
            "primary grant entity ownership must be cleared"
        );
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key("entity-fail-b"),
            "fail-closed sibling grant entity ownership must be cleared synchronously"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions,
            0
        );
        assert!(
            !harness
                .state
                .pending_runtime
                .active_subscriptions
                .get(session_b)
                .is_some_and(|subs| subs.contains(attach_b)),
            "fail-closed sibling attach must be detached from runtime active subscriptions"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_attach_subscriptions, 0,
            "live attach counter must reach zero after fail-closed sibling detach"
        );
        assert!(
            harness.state.released_attach_generations >= 1,
            "released attach generations must account for sibling detach"
        );
        wait_until(
            Instant::now() + Duration::from_secs(2),
            || LocalWebrtcTransport::dedicated_runtime_worker_threads() == 0,
            "dedicated runtime workers must join after fail-closed teardown",
        );

        peer_a.close_offer();
        peer_b.close_offer();
        harness
            .shutdown_owned_sessions()
            .expect("fail-closed attach session must shut down and remove cleanly");
        assert!(
            harness.list_session_lifecycle(session_b).is_none(),
            "logical session must be absent after validated RemoveSession"
        );
        wait_for_owned_workers_gone(&owned_workers, Instant::now() + Duration::from_secs(5));
        for worker in &owned_workers {
            assert!(
                worker.is_fully_gone(),
                "owned worker must be fully gone after cleanup: {worker:?}"
            );
            assert!(
                !live_pids_in_process_group(worker.pgid).contains(&worker.pid),
                "worker pid must no longer appear in its process group after cleanup: {worker:?}"
            );
        }
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_spawned_session_is_cleaned_even_if_attach_proof_panics_after_ready() {
        // Deliberate failure after Spawn readiness must still reap the worker via Drop unwind.
        // Keep the harness live until panic so Drop runs during catch_unwind stack unwind.
        let mut owned_workers = Vec::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut harness = PeerHarness::new("attach-panic-cleanup");
            let origin = "http://127.0.0.1:41798";
            let mut peer = harness.signal_peer(origin);
            harness.spawn_and_attach_on_peer(
                &mut peer,
                "panic-cleanup-session",
                "panic-cleanup-attach",
            );
            owned_workers = harness.owned_workers.clone();
            assert!(!owned_workers.is_empty());
            assert!(
                harness
                    .list_session_lifecycle("panic-cleanup-session")
                    .is_some(),
                "session must exist before deliberate panic"
            );
            peer.close_offer();
            // Do not drop harness here — panic while it is still live so Drop runs on unwind.
            panic!("deliberate failure after spawn readiness");
        }));
        assert!(result.is_err(), "deliberate panic must fire");
        assert!(
            take_last_session_cleanup_error().is_none(),
            "Drop cleanup must succeed during unwind"
        );
        wait_for_owned_workers_gone(&owned_workers, Instant::now() + Duration::from_secs(5));
        for worker in &owned_workers {
            assert!(
                worker.is_fully_gone(),
                "owned worker must be fully gone after Drop unwind cleanup: {worker:?}"
            );
            assert!(
                !live_pids_in_process_group(worker.pgid).contains(&worker.pid),
                "worker pid must no longer appear in its process group after Drop cleanup: {worker:?}"
            );
        }
    }

    #[test]
    fn local_webrtc_stale_peer_snapshot_does_not_remove_replacement_subscription_owner() {
        // Peer A cleanup_once captures subscription_id S, then unsubscribes and peer B reuses S.
        // Delayed PeerClosed for A must not delete B's row.
        let mut harness = PeerHarness::new("stale-snapshot");
        let origin = "http://127.0.0.1:41797";
        let mut peer_a = harness.signal_peer(origin);
        let peer_b = harness.signal_peer(origin);
        let grant_a = peer_a.grant_id.clone();
        let grant_b = peer_b.grant_id.clone();
        let subscription_id = "reused-entity-id".to_string();

        let _ = harness.subscribe_entities(&mut peer_a, &subscription_id);
        assert_eq!(
            harness
                .state
                .entity_subscriptions
                .get(&subscription_id)
                .and_then(|sub| sub.owner_grant_id.as_deref()),
            Some(grant_a.as_str())
        );

        // Unsubscribe A and register B with the same subscription_id (replacement owner).
        if harness
            .state
            .entity_subscriptions
            .remove(&subscription_id)
            .is_some()
        {
            harness.state.lifecycle_counters.live_entity_subscriptions = harness
                .state
                .lifecycle_counters
                .live_entity_subscriptions
                .saturating_sub(1);
        }
        let (frame_tx, _frame_rx) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::SubscribeEntities {
                entity_type: "session".to_string(),
                subscription_id: subscription_id.clone(),
                frame_tx: EntityFrameSender::Async(frame_tx),
                reply_tx,
                grant_id: Some(grant_b.clone()),
            },
        );
        assert_eq!(
            reply_rx.blocking_recv().expect("reply").expect("ok").kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert_eq!(
            harness
                .state
                .entity_subscriptions
                .get(&subscription_id)
                .and_then(|sub| sub.owner_grant_id.as_deref()),
            Some(grant_b.as_str())
        );

        let terminal_record = LocalWebrtcSenderTerminalRecord {
            schema_version: 1,
            grant_id: grant_a.clone(),
            request_operation: "entity_delivery".to_string(),
            message_id: None,
            next_chunk_index: 0,
            last_sent_chunk_index: None,
            total_chunks: 0,
            pressured: false,
            peer_connection_state: "failed".to_string(),
            channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
            cause: LocalWebrtcTerminalCause::PeerFailed,
            cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
        };
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::LocalWebrtcPeerClosed {
                grant_id: grant_a,
                attached_subscriptions: Vec::new(),
                // Stale snapshot still names the reused subscription id.
                entity_subscription_ids: vec![subscription_id.clone()],
                terminal_record,
            },
        );

        assert!(
            harness
                .state
                .entity_subscriptions
                .contains_key(&subscription_id),
            "replacement owner B must keep the reused subscription id"
        );
        assert_eq!(
            harness
                .state
                .entity_subscriptions
                .get(&subscription_id)
                .and_then(|sub| sub.owner_grant_id.as_deref()),
            Some(grant_b.as_str())
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions,
            1
        );

        peer_a.close_offer();
        peer_b.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_subscribe_before_peer_closed_is_swept_by_owner_grant() {
        // Subscribe-first race: daemon registers the entity subscription while the peer is still
        // live, but PeerClosed's cleanup_once snapshot did not include the id. Sweep by
        // owner_grant_id must still remove the daemon row.
        let mut harness = PeerHarness::new("subscribe-first");
        let origin = "http://127.0.0.1:41796";
        let peer = harness.signal_peer(origin);
        let grant_id = peer.grant_id.clone();
        let subscription_id = "subscribe-first-entity".to_string();

        assert!(harness.daemon.local_webrtc().has_live_peer(&grant_id));

        let (frame_tx, _frame_rx) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::SubscribeEntities {
                entity_type: "session".to_string(),
                subscription_id: subscription_id.clone(),
                frame_tx: EntityFrameSender::Async(frame_tx),
                reply_tx,
                grant_id: Some(grant_id.clone()),
            },
        );
        let response = reply_rx
            .blocking_recv()
            .expect("reply channel open")
            .expect("subscribe response");
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(
            harness
                .state
                .entity_subscriptions
                .contains_key(&subscription_id)
        );
        assert_eq!(
            harness
                .state
                .entity_subscriptions
                .get(&subscription_id)
                .and_then(|sub| sub.owner_grant_id.as_deref()),
            Some(grant_id.as_str())
        );

        // PeerClosed with an empty ownership snapshot (as if cleanup_once raced before add).
        let terminal_record = LocalWebrtcSenderTerminalRecord {
            schema_version: 1,
            grant_id: grant_id.clone(),
            request_operation: "entity_delivery".to_string(),
            message_id: None,
            next_chunk_index: 0,
            last_sent_chunk_index: None,
            total_chunks: 0,
            pressured: false,
            peer_connection_state: "failed".to_string(),
            channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
            cause: LocalWebrtcTerminalCause::PeerFailed,
            cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
        };
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::LocalWebrtcPeerClosed {
                grant_id: grant_id.clone(),
                attached_subscriptions: Vec::new(),
                entity_subscription_ids: Vec::new(),
                terminal_record,
            },
        );

        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key(&subscription_id),
            "PeerClosed must remove grant-owned subscriptions even when the peer snapshot was empty"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions,
            0
        );
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_late_attach_after_peer_closed_does_not_recreate_state() {
        let mut harness = PeerHarness::new("late-attach");
        let origin = "http://127.0.0.1:41799";
        let peer = harness.signal_peer(origin);
        let grant_id = peer.grant_id.clone();
        let session_id = "late-attach-session".to_string();
        let subscription_id = "late-attach-sub".to_string();
        let live_attach_before = harness.state.lifecycle_counters.live_attach_subscriptions;

        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));
        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 0);
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::Request {
                request: Box::new(DaemonRequest::Attach {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                }),
                reply_tx,
                response_delivery_rx: None,
                grant_id: Some(grant_id.clone()),
            },
        );

        let response = reply_rx
            .blocking_recv()
            .expect("reply channel open")
            .expect("daemon returns operator response");
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("local_webrtc_peer_gone")
        );
        assert!(
            !harness
                .state
                .pending_runtime
                .active_subscriptions
                .get(&session_id)
                .is_some_and(|subs| subs.contains(&subscription_id)),
            "late Attach must not create residual active attach ownership after PeerClosed"
        );
        assert!(
            !harness
                .state
                .pending_runtime
                .attach_owner_grant_ids
                .contains_key(&(session_id, subscription_id)),
            "late Attach must not record attach owner grant after PeerClosed"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_attach_subscriptions, live_attach_before,
            "live attach counter must not increase for rejected late Attach"
        );

        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_late_spawn_after_peer_closed_does_not_create_session() {
        let mut harness = PeerHarness::new("late-spawn");
        let origin = "http://127.0.0.1:41800";
        let peer = harness.signal_peer(origin);
        let grant_id = peer.grant_id.clone();
        let session_id = "late-spawn-session".to_string();

        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::Request {
                request: Box::new(DaemonRequest::Spawn {
                    session_id: session_id.clone(),
                    command: "true".to_string(),
                }),
                reply_tx,
                response_delivery_rx: None,
                grant_id: Some(grant_id.clone()),
            },
        );

        let response = reply_rx
            .blocking_recv()
            .expect("reply channel open")
            .expect("daemon returns operator response");
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("local_webrtc_peer_gone")
        );
        assert!(
            harness.list_session_lifecycle(&session_id).is_none(),
            "late Spawn must not create durable session ownership after PeerClosed"
        );

        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_late_unsubscribe_does_not_delete_replacement_owner_row() {
        // Peer A subscribed with id S, then B reused S after A is not live. Late Unsubscribe
        // from A must preserve B's row and counters (owner-checked cleanup).
        let mut harness = PeerHarness::new("late-unsub-reuse");
        let origin = "http://127.0.0.1:41801";
        let peer_a = harness.signal_peer(origin);
        let peer_b = harness.signal_peer(origin);
        let grant_a = peer_a.grant_id.clone();
        let grant_b = peer_b.grant_id.clone();
        let subscription_id = "reused-unsub-entity".to_string();

        let (frame_tx_a, _frame_rx_a) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::SubscribeEntities {
                entity_type: "session".to_string(),
                subscription_id: subscription_id.clone(),
                frame_tx: EntityFrameSender::Async(frame_tx_a),
                reply_tx,
                grant_id: Some(grant_a.clone()),
            },
        );
        assert_eq!(
            reply_rx.blocking_recv().expect("reply").expect("ok").kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );

        // Close A first so grant A is not live, then hand the same id to live peer B.
        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&grant_a, Instant::now() + Duration::from_secs(10));
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_a));
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key(&subscription_id),
            "PeerClosed for A must sweep A's entity row before B reuses the id"
        );

        let (frame_tx_b, _frame_rx_b) = tokio_mpsc::channel(ENTITY_SUBSCRIPTION_QUEUE_CAPACITY);
        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::SubscribeEntities {
                entity_type: "session".to_string(),
                subscription_id: subscription_id.clone(),
                frame_tx: EntityFrameSender::Async(frame_tx_b),
                reply_tx,
                grant_id: Some(grant_b.clone()),
            },
        );
        assert_eq!(
            reply_rx.blocking_recv().expect("reply").expect("ok").kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert_eq!(
            harness
                .state
                .entity_subscriptions
                .get(&subscription_id)
                .and_then(|sub| sub.owner_grant_id.as_deref()),
            Some(grant_b.as_str())
        );
        let live_entity_before = harness.state.lifecycle_counters.live_entity_subscriptions;

        let (reply_tx, reply_rx) = oneshot::channel();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::UnsubscribeEntities {
                subscription_id: subscription_id.clone(),
                reply_tx: Some(reply_tx),
                grant_id: Some(grant_a.clone()),
            },
        );
        let response = reply_rx
            .blocking_recv()
            .expect("reply channel open")
            .expect("idempotent unsubscribed reply");
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::EntityUnsubscribed
        );
        assert!(
            harness
                .state
                .entity_subscriptions
                .contains_key(&subscription_id),
            "late Unsubscribe from stale grant A must preserve replacement owner B's row"
        );
        assert_eq!(
            harness
                .state
                .entity_subscriptions
                .get(&subscription_id)
                .and_then(|sub| sub.owner_grant_id.as_deref()),
            Some(grant_b.as_str())
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions, live_entity_before,
            "entity counter must not drop when preserving replacement owner"
        );

        peer_a.close_offer();
        peer_b.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_attach_owner_sweep_on_empty_snapshot() {
        let _teardown_guard = teardown_test_lock();
        // Attach succeeds while peer is live; PeerClosed with empty attach snapshot must still
        // detach grant-owned attach via control-plane owner index.
        let mut harness = PeerHarness::new("attach-owner-sweep");
        let origin = "http://127.0.0.1:41802";
        let mut peer = harness.signal_peer(origin);
        let grant_id = peer.grant_id.clone();
        let session_id = "attach-sweep-session";
        let subscription_id = "attach-sweep-sub";

        harness.spawn_and_attach_on_peer(&mut peer, session_id, subscription_id);
        assert!(
            harness
                .state
                .pending_runtime
                .attach_owner_grant_ids
                .get(&(session_id.to_string(), subscription_id.to_string()))
                .map(String::as_str)
                == Some(grant_id.as_str()),
            "successful WebRTC Attach must record grant ownership"
        );
        assert!(
            harness
                .state
                .pending_runtime
                .active_subscriptions
                .get(session_id)
                .is_some_and(|subs| subs.contains(subscription_id))
        );

        let terminal_record = LocalWebrtcSenderTerminalRecord {
            schema_version: 1,
            grant_id: grant_id.clone(),
            request_operation: "attach".to_string(),
            message_id: None,
            next_chunk_index: 0,
            last_sent_chunk_index: None,
            total_chunks: 0,
            pressured: false,
            peer_connection_state: "failed".to_string(),
            channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
            cause: LocalWebrtcTerminalCause::PeerFailed,
            cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
        };
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::LocalWebrtcPeerClosed {
                grant_id: grant_id.clone(),
                // Empty peer-side attach snapshot (raced before peer recorded attach).
                attached_subscriptions: Vec::new(),
                entity_subscription_ids: Vec::new(),
                terminal_record,
            },
        );

        assert!(
            !harness
                .state
                .pending_runtime
                .active_subscriptions
                .get(session_id)
                .is_some_and(|subs| subs.contains(subscription_id)),
            "PeerClosed must detach grant-owned attach even when peer snapshot was empty"
        );
        assert!(
            !harness
                .state
                .pending_runtime
                .attach_owner_grant_ids
                .contains_key(&(session_id.to_string(), subscription_id.to_string())),
            "attach owner index must be cleared for removed grant"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_attach_subscriptions,
            0
        );
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));

        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner() {
        let _teardown_guard = teardown_test_lock();
        // Peer A attached (session S, sub X), then B reused the same attach ids while A is gone.
        // Delayed PeerClosed for A still carries A's attach snapshot and must not detach B's row.
        let mut harness = PeerHarness::new("stale-attach-snapshot");
        let origin = "http://127.0.0.1:41804";
        let mut peer_a = harness.signal_peer(origin);
        let mut peer_b = harness.signal_peer(origin);
        let grant_a = peer_a.grant_id.clone();
        let grant_b = peer_b.grant_id.clone();
        let session_id = "reused-attach-session";
        let subscription_id = "reused-attach-sub";

        harness.spawn_and_attach_on_peer(&mut peer_a, session_id, subscription_id);
        assert_eq!(
            harness
                .state
                .pending_runtime
                .attach_owner_grant_ids
                .get(&(session_id.to_string(), subscription_id.to_string()))
                .map(String::as_str),
            Some(grant_a.as_str())
        );

        // Close A without clearing B's future ownership of the reused attach id.
        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&grant_a, Instant::now() + Duration::from_secs(10));
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_a));

        // B attaches with the same session/subscription ids (replacement owner).
        // Session may still exist after A's PeerClosed detach; re-attach under B.
        let attach_b = harness.request_on_peer(
            &mut peer_b,
            DaemonRequest::Attach {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            },
            "Attach-B-reuse",
        );
        assert_eq!(
            attach_b.kind,
            botster_hub_client::DaemonResponseKind::Events,
            "replacement owner B must attach successfully: {:?}",
            attach_b.error
        );
        assert_eq!(
            harness
                .state
                .pending_runtime
                .attach_owner_grant_ids
                .get(&(session_id.to_string(), subscription_id.to_string()))
                .map(String::as_str),
            Some(grant_b.as_str())
        );
        let live_attach_before = harness.state.lifecycle_counters.live_attach_subscriptions;
        assert!(live_attach_before >= 1);

        // Delayed PeerClosed for A with a stale attach snapshot naming the reused ids.
        let terminal_record = LocalWebrtcSenderTerminalRecord {
            schema_version: 1,
            grant_id: grant_a.clone(),
            request_operation: "attach".to_string(),
            message_id: None,
            next_chunk_index: 0,
            last_sent_chunk_index: None,
            total_chunks: 0,
            pressured: false,
            peer_connection_state: "failed".to_string(),
            channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
            cause: LocalWebrtcTerminalCause::PeerFailed,
            cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
        };
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::LocalWebrtcPeerClosed {
                grant_id: grant_a.clone(),
                attached_subscriptions: vec![LocalWebrtcAttachedSubscription {
                    session_id: session_id.to_string(),
                    subscription_id: subscription_id.to_string(),
                }],
                entity_subscription_ids: Vec::new(),
                terminal_record,
            },
        );

        assert!(
            harness
                .state
                .pending_runtime
                .active_subscriptions
                .get(session_id)
                .is_some_and(|subs| subs.contains(subscription_id)),
            "delayed PeerClosed for A must not detach B's reused attach"
        );
        assert_eq!(
            harness
                .state
                .pending_runtime
                .attach_owner_grant_ids
                .get(&(session_id.to_string(), subscription_id.to_string()))
                .map(String::as_str),
            Some(grant_b.as_str()),
            "replacement owner B must remain in attach owner index"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_attach_subscriptions, live_attach_before,
            "live attach counter must not drop when preserving replacement owner"
        );
        assert!(harness.daemon.local_webrtc().has_live_peer(&grant_b));

        peer_a.close_offer();
        peer_b.close_offer();
        harness.cleanup();
    }

    /// Child env for the hang-close subprocess oracle. Parent kills the child when the
    /// whole-child deadline is exceeded so ablating the production close timeout yields a
    /// finite red result instead of hanging the suite.
    const HANG_CLOSE_CHILD_ENV: &str = "BOTSTER_HUB_WEBRTC_HANG_CLOSE_CHILD";
    /// Whole-child budget: signal peers + entity subscribe + close bound + fail-closed + cleanup.
    /// Intentionally avoids durable session workers so a parent kill cannot orphan them.
    const HANG_CLOSE_CHILD_DEADLINE: Duration = Duration::from_secs(15);

    fn run_close_hang_fail_closed_body() {
        let _teardown_guard = teardown_test_lock();
        // Deterministic hang on production remove_peer/close path. Handler must return within
        // HANDLER_JOIN_DEADLINE and take the fail-closed sibling path (timeout ≡ ultimate failure).
        // No Spawn/Attach: durable session workers would be orphaned if the parent hard-kills the
        // child after timeout ablation. Sibling attach fail-closed is covered by the forced-error
        // path (`local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads`).
        let mut harness = PeerHarness::new("close-hang-sibling");
        let origin = "http://127.0.0.1:41803";
        let mut peer_a = harness.signal_peer(origin);
        let mut peer_b = harness.signal_peer(origin);
        let grant_a = peer_a.grant_id.clone();
        let grant_b = peer_b.grant_id.clone();

        let _ = harness.subscribe_entities(&mut peer_a, "entity-hang-a");
        let _ = harness.subscribe_entities(&mut peer_b, "entity-hang-b");
        assert!(
            harness
                .state
                .entity_subscriptions
                .contains_key("entity-hang-b")
        );
        assert!(
            harness.owned_workers.is_empty(),
            "hang hard-stop child must not create durable session workers"
        );
        assert!(LocalWebrtcTransport::dedicated_runtime_worker_threads() >= 1);

        harness
            .daemon
            .local_webrtc()
            .force_next_close_hang_for_test(&grant_a);
        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_a, RTCPeerConnectionState::Failed);

        // Drain until PeerClosed is available, but do not handle it on this thread yet.
        let peer_closed_message = {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if Instant::now() >= deadline {
                    panic!("timed out waiting for LocalWebrtcPeerClosed for hang test");
                }
                match harness.control_rx.try_recv() {
                    Ok(message) => {
                        let is_closed = matches!(
                            &message,
                            ControlMessage::LocalWebrtcPeerClosed { grant_id: closed, .. }
                                if closed == &grant_a
                        );
                        if is_closed {
                            break message;
                        }
                        // Handle non-PeerClosed control traffic so the channel does not stall.
                        handle_control_message(
                            &mut harness.daemon,
                            &mut harness.state,
                            &harness.terminal_path,
                            &harness.transport_handle,
                            harness.control_tx.clone(),
                            message,
                        );
                    }
                    Err(tokio_mpsc::error::TryRecvError::Empty) => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                        panic!("control channel closed before LocalWebrtcPeerClosed");
                    }
                }
            }
        };

        // Production PeerClosed handler under forced hang must return within HANDLER_JOIN_DEADLINE.
        // Hang inject goes through the production timeout wrapper around the close future.
        let handler_started = Instant::now();
        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            peer_closed_message,
        );
        let handler_elapsed = handler_started.elapsed();
        assert!(
            handler_elapsed <= LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE,
            "production PeerClosed handler elapsed {handler_elapsed:?} must be within HANDLER_JOIN_DEADLINE {:?} under forced close hang",
            LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE
        );

        assert_eq!(
            harness.daemon.local_webrtc().active_peer_count(),
            0,
            "fail-closed hang path must clear live peer map"
        );
        assert!(
            !harness.daemon.local_webrtc().has_dedicated_runtime(),
            "fail-closed hang path must drop dedicated runtime"
        );
        assert_eq!(
            harness.daemon.local_webrtc().peer_state_count(),
            0,
            "fail-closed hang path must clear primary + sibling peer_states"
        );
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_a));
        assert!(
            !harness.daemon.local_webrtc().has_live_peer(&grant_b),
            "timeout fail-closed must sacrifice sibling peers"
        );
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key("entity-hang-a")
        );
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key("entity-hang-b"),
            "fail-closed hang path must clear sibling entity ownership"
        );
        assert_eq!(
            harness.state.lifecycle_counters.live_entity_subscriptions,
            0
        );
        wait_until(
            Instant::now() + Duration::from_secs(2),
            || LocalWebrtcTransport::dedicated_runtime_worker_threads() == 0,
            "dedicated runtime workers must join after hang fail-closed teardown",
        );

        peer_a.close_offer();
        peer_b.close_offer();
        harness.cleanup();
    }

    #[test]
    fn local_webrtc_close_hang_fail_closed_returns_handler_within_deadline() {
        // External hard-stop oracle: parent process waits on a child that runs the production
        // hang path. If the production close timeout is ablated, the child never exits and the
        // parent kills it after HANG_CLOSE_CHILD_DEADLINE — finite red, not suite hang.
        if std::env::var_os(HANG_CLOSE_CHILD_ENV).is_some() {
            run_close_hang_fail_closed_body();
            return;
        }

        let exe = std::env::current_exe().expect("test executable path");
        let mut child = std::process::Command::new(&exe)
            .env(HANG_CLOSE_CHILD_ENV, "1")
            .env("RUST_BACKTRACE", "0")
            .args([
                "--exact",
                "local_webrtc::tests::local_webrtc_close_hang_fail_closed_returns_handler_within_deadline",
                "--nocapture",
            ])
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .expect("spawn hang-close oracle child");

        let deadline = Instant::now() + HANG_CLOSE_CHILD_DEADLINE;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    assert!(
                        status.success(),
                        "hang-close child must exit 0 when production close bound is present; status={status}"
                    );
                    return;
                }
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!(
                        "hang-close child exceeded {:?}; production close timeout missing or hang path blocked (red-on-revert hard stop)",
                        HANG_CLOSE_CHILD_DEADLINE
                    );
                }
                Ok(None) => thread::sleep(Duration::from_millis(20)),
                Err(error) => panic!("hang-close child wait failed: {error}"),
            }
        }
    }
}
