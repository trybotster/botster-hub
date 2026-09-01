use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use botster_core::AesGcmKey;
use botster_hub_client::DaemonRequest;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use webrtc::data_channel::DataChannel;
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionEventHandler, RTCIceGatheringState, RTCPeerConnectionState,
};
use webrtc::runtime::{Runtime, Sender as AsyncSender, default_runtime};

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::transport::webrtc::adapter::WebRtcConnectionMux;
use crate::transport::webrtc::control_channel::{
    LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH, LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW, LocalWebrtcDataChannel,
    local_webrtc_request_operation, run_data_channel,
};
use crate::transport::webrtc::subscription_channel::{
    LocalWebrtcAttachedSubscription, LocalWebrtcAttachedSubscriptionChange,
};
use crate::transport::webrtc::{LocalWebrtcError, LocalWebrtcResult};
pub(crate) fn webrtc_runtime() -> Arc<dyn Runtime> {
    default_runtime().expect("webrtc default runtime")
}
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
pub(crate) const TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV: &str =
    "BOTSTER_HUB_TEST_CLOSE_LOCAL_WEBRTC_OPERATION";
pub(crate) const TEST_DISABLE_ONE_SHOT_CLAIM_ENV: &str = "BOTSTER_HUB_TEST_DISABLE_ONE_SHOT_CLAIM";
pub(crate) const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE: &str =
    "local-webrtc-sender-terminal.json";
pub(crate) const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES: usize = 4096;
/// Ephemeral local WebRTC admission and peer registry.
#[derive(Clone)]
pub(crate) struct SharedEventPlane(
    pub(crate) Arc<crate::subscription::package_events::ClientEventPlane>,
);

impl Default for SharedEventPlane {
    fn default() -> Self {
        Self(Arc::new(
            crate::subscription::package_events::ClientEventPlane::default(),
        ))
    }
}

#[derive(Default)]
pub struct LocalWebrtcTransport {
    pub(crate) grants: crate::admission::grants::GrantRegistry,
    pub(crate) event_plane: SharedEventPlane,
    pub(crate) peers: BTreeMap<String, Arc<dyn PeerConnection>>,
    /// Live peer ownership records used for fail-closed sibling cleanup.
    pub(crate) peer_states: BTreeMap<String, Arc<LocalWebrtcPeerState>>,
    /// Peers whose `close()` failed while siblings kept the shared runtime alive.
    /// Retained so a later empty-map park / `stop_all` can still force driver stop.
    pub(crate) stale_close_peers: BTreeMap<String, Arc<dyn PeerConnection>>,
    pub(crate) runtime: Option<tokio::runtime::Runtime>,
    #[cfg(test)]
    pub(crate) close_completions: Mutex<Vec<String>>,
    #[cfg(test)]
    pub(crate) peer_handlers: BTreeMap<String, Arc<LocalWebrtcHandler>>,
    #[cfg(test)]
    pub(crate) force_close_errors: Mutex<BTreeSet<String>>,
    #[cfg(test)]
    pub(crate) force_close_hangs: Mutex<BTreeSet<String>>,
    /// Instance-scoped dedicated-runtime worker census.
    /// A process-global counter made `== 0` waits observe other tests' runtimes
    /// under default-concurrency lib load.
    #[cfg(test)]
    pub(crate) worker_threads: Arc<AtomicUsize>,
}

pub(crate) enum ClosePeerOutcome {
    Closed,
    Failed(Arc<dyn PeerConnection>),
}

/// Grants removed from the live peer map by a forget operation (primary and any fail-closed siblings).
#[derive(Debug, Default)]
pub(crate) struct PeerRemoveResult {
    pub removed_grant_ids: Vec<String>,
    pub attached_subscriptions: Vec<LocalWebrtcAttachedSubscription>,
}
impl LocalWebrtcTransport {
    #[must_use]
    pub(crate) fn event_plane(&self) -> Arc<crate::subscription::package_events::ClientEventPlane> {
        self.event_plane.0.clone()
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

    pub(crate) fn take_remove_result(
        &mut self,
        grant_ids: impl IntoIterator<Item = String>,
    ) -> PeerRemoveResult {
        let mut result = PeerRemoveResult::default();
        for grant_id in grant_ids {
            if let Some(peer_state) = self.peer_states.remove(&grant_id) {
                peer_state.mux.close_all();
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

    pub(crate) fn park_runtime_if_idle(&mut self) {
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
    pub(crate) fn fail_closed_drop_dedicated_runtime(
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

    pub(crate) fn close_peer_on_runtime(
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

    pub(crate) fn runtime(&mut self) -> LocalWebrtcResult<&tokio::runtime::Runtime> {
        if self.runtime.is_none() {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.thread_name("botster-local-webrtc").enable_all();
            #[cfg(test)]
            {
                let worker_threads_start = Arc::clone(&self.worker_threads);
                let worker_threads_stop = Arc::clone(&self.worker_threads);
                builder
                    .on_thread_start(move || {
                        worker_threads_start.fetch_add(1, Ordering::SeqCst);
                    })
                    .on_thread_stop(move || {
                        worker_threads_stop.fetch_sub(1, Ordering::SeqCst);
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
    pub(crate) fn dedicated_runtime_worker_threads(&self) -> usize {
        self.worker_threads.load(Ordering::SeqCst)
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
pub(crate) struct LocalWebrtcPeerState {
    pub(crate) grant_id: String,
    pub(crate) runtime_tx: ControlSender,
    pub(crate) attached_subscriptions: Mutex<Vec<LocalWebrtcAttachedSubscription>>,
    pub(crate) entity_subscription_ids: Mutex<BTreeSet<String>>,
    pub(crate) terminal_state: Mutex<LocalWebrtcTerminalState>,
    pub(crate) peer_terminal_tx: watch::Sender<Option<LocalWebrtcTerminalCause>>,
    pub(crate) peer_terminal_published: AtomicBool,
    pub(crate) cleanup_sent: AtomicBool,
    pub(crate) data_channel_claimed: AtomicBool,
    pub(crate) mux: WebRtcConnectionMux,
    #[cfg(test)]
    pub(crate) force_local_close_hang: AtomicBool,
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
pub(crate) struct LocalWebrtcTerminalState {
    pub(crate) request_operation: String,
    pub(crate) message_id: Option<String>,
    pub(crate) next_chunk_index: usize,
    pub(crate) last_sent_chunk_index: Option<usize>,
    pub(crate) total_chunks: usize,
    pub(crate) pressured: bool,
    pub(crate) peer_connection_state: String,
    pub(crate) channel_terminal_signal: LocalWebrtcChannelTerminalSignal,
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
impl LocalWebrtcPeerState {
    #[allow(dead_code)]
    pub(crate) fn new(grant_id: String, runtime_tx: ControlSender) -> Self {
        Self::new_with_event_plane(
            grant_id,
            runtime_tx,
            Arc::new(crate::subscription::package_events::ClientEventPlane::default()),
        )
    }

    pub(crate) fn new_with_event_plane(
        grant_id: String,
        runtime_tx: ControlSender,
        _event_plane: Arc<crate::subscription::package_events::ClientEventPlane>,
    ) -> Self {
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
            data_channel_claimed: AtomicBool::new(false),
            mux: WebRtcConnectionMux::new(),
            #[cfg(test)]
            force_local_close_hang: AtomicBool::new(false),
        }
    }

    pub(crate) fn apply_subscription_change(
        &self,
        change: Option<LocalWebrtcAttachedSubscriptionChange>,
    ) {
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

    pub(crate) fn add_entity_subscription(&self, subscription_id: String) {
        self.entity_subscription_ids
            .lock()
            .expect("local WebRTC entity subscription mutex")
            .insert(subscription_id);
    }

    pub(crate) fn remove_entity_subscription(&self, subscription_id: &str) {
        self.entity_subscription_ids
            .lock()
            .expect("local WebRTC entity subscription mutex")
            .remove(subscription_id);
    }

    pub(crate) fn claim_data_channel(&self) -> bool {
        if std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
            && std::env::var(TEST_DISABLE_ONE_SHOT_CLAIM_ENV).as_deref() == Ok("1")
        {
            return true;
        }
        self.data_channel_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn begin_request(&self, request: &DaemonRequest) {
        self.begin_operation(local_webrtc_request_operation(request));
    }

    pub(crate) fn begin_overflow_response(&self) {
        self.begin_operation("request_queue_overflow");
    }

    pub(crate) fn begin_operation(&self, operation: &str) {
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

    pub(crate) fn begin_response(
        &self,
        message_id: Option<String>,
        total_chunks: usize,
        pressured: bool,
    ) {
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

    pub(crate) fn record_response_progress(&self, next_chunk_index: usize, pressured: bool) {
        let mut terminal_state = self
            .terminal_state
            .lock()
            .expect("local WebRTC terminal state mutex");
        terminal_state.next_chunk_index = next_chunk_index;
        terminal_state.last_sent_chunk_index = next_chunk_index.checked_sub(1);
        terminal_state.pressured = pressured;
    }

    pub(crate) fn set_peer_connection_state(&self, state: RTCPeerConnectionState) {
        self.terminal_state
            .lock()
            .expect("local WebRTC terminal state mutex")
            .peer_connection_state = local_webrtc_peer_connection_state(state).to_string();
    }

    pub(crate) fn observe_peer_connection_state(
        &self,
        state: RTCPeerConnectionState,
    ) -> Option<LocalWebrtcTerminalCause> {
        self.set_peer_connection_state(state);
        let cause = match state {
            RTCPeerConnectionState::Failed => LocalWebrtcTerminalCause::PeerFailed,
            RTCPeerConnectionState::Closed => LocalWebrtcTerminalCause::PeerClosed,
            RTCPeerConnectionState::Disconnected if self.mux.has_bound_routes() => {
                LocalWebrtcTerminalCause::PeerDisconnected
            }
            _ => return None,
        };
        self.publish_peer_terminal(cause);
        Some(cause)
    }

    pub(crate) fn subscribe_peer_terminal(
        &self,
    ) -> watch::Receiver<Option<LocalWebrtcTerminalCause>> {
        self.peer_terminal_tx.subscribe()
    }

    pub(crate) fn publish_peer_terminal(&self, cause: LocalWebrtcTerminalCause) {
        if self
            .peer_terminal_published
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.peer_terminal_tx.send_replace(Some(cause));
        }
    }

    pub(crate) async fn cleanup_once(&self, cause: LocalWebrtcTerminalCause) {
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
pub(crate) fn local_webrtc_peer_connection_state(state: RTCPeerConnectionState) -> &'static str {
    match state {
        RTCPeerConnectionState::Unspecified => "unspecified",
        RTCPeerConnectionState::New => "new",
        RTCPeerConnectionState::Connecting => "connecting",
        RTCPeerConnectionState::Connected => "connected",
        RTCPeerConnectionState::Disconnected => "disconnected",
        RTCPeerConnectionState::Failed => "failed",
        RTCPeerConnectionState::Closed => "closed",
        _ => "unknown",
    }
}

#[derive(Clone)]
pub(crate) struct LocalWebrtcHandler {
    pub(crate) stream_key: AesGcmKey,
    pub(crate) runtime: Arc<dyn Runtime>,
    pub(crate) peer_state: Arc<LocalWebrtcPeerState>,
    pub(crate) gather_complete_tx: AsyncSender<()>,
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
        let claimed = self.peer_state.claim_data_channel();
        if !claimed {
            let label = data_channel.label().await.unwrap_or_else(|_| String::new());
            let grant_id = self.peer_state.grant_id.clone();
            let peer_state = self.peer_state.clone();
            let stream_key = self.stream_key.clone();
            self.runtime.spawn(Box::pin(async move {
                crate::transport::webrtc::subscription_channel::admit_reserved_subscription_channel(
                    &grant_id,
                    &label,
                    data_channel.as_ref(),
                    &stream_key,
                    peer_state.as_ref(),
                )
                .await;
            }));
            return;
        }
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
            )
            .await;
        }));
    }
}
#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::admission::budgets::ENTITY_SUBSCRIPTION_QUEUE_CAPACITY;
    use crate::admission::unix_hello::WebrtcTerminalAdmission;
    use crate::daemon::control::handle_control_message;
    use crate::daemon::control::message::{ControlMessage, ControlSender, ReservationInspectReply};
    use crate::daemon::owner_loop::DaemonControlState;
    use crate::subscription::attach_routes::negotiated_unix_capability_set;
    use crate::subscription::entity::EntityFrameSender;
    use crate::transport::webrtc::adapter::WebRtcConnectionMux;
    use crate::transport::webrtc::control_channel::*;
    use crate::transport::webrtc::delivery::*;
    use crate::transport::webrtc::peer::*;
    use crate::transport::webrtc::subscription_channel::*;
    use crate::transport::webrtc::test_support::*;
    use crate::transport::webrtc::{LocalWebrtcError, LocalWebrtcResult};
    use crate::{
        DataDirectoryOption, HostIdentityOptions, HubDaemon, HubStartupOptions,
        PackageEventPlaneOptions, RuntimeEnvironment, SessionDefaults,
    };
    use async_trait::async_trait;
    use botster_core::contract::terminal_adapter::{
        TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError,
    };
    use botster_core::{AesGcmKey, encrypt_aes_gcm};
    use botster_hub_client::{
        DaemonDiagnostic, DaemonEntityFrame, DaemonHello, DaemonRequest, DaemonResponse,
        LOCAL_WEBRTC_MAX_DELIVERY_BYTES,
    };
    use botster_hub_client::{DaemonEvent, PROTOCOL};
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;
    use tokio::sync::mpsc as tokio_mpsc;
    use tokio::sync::oneshot;
    use webrtc::data_channel::RTCDataChannelInit;
    use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelMessage};
    use webrtc::peer_connection::PeerConnectionBuilder;
    use webrtc::peer_connection::{
        PeerConnection, PeerConnectionEventHandler, RTCIceGatheringState, RTCPeerConnectionState,
    };
    use webrtc::runtime::{
        Receiver as AsyncReceiver, Sender as AsyncSender, channel as webrtc_channel,
        default_runtime, timeout,
    };
    #[test]
    fn peer_admits_only_the_first_data_channel() {
        let peer_state = test_peer_state("grant-one-channel");
        assert!(peer_state.claim_data_channel());
        assert!(!peer_state.claim_data_channel());
    }
    #[test]
    fn hanging_data_channel_local_close_still_runs_cleanup_once_within_bound() {
        let data_channel = FakeDataChannel::default();
        let mut pending = VecDeque::new();
        pending.push_back(PendingLocalWebrtcRequest::Request(Box::new(
            DaemonRequest::Status,
        )));
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-local-close-hang".to_string(),
            runtime_tx,
        ));
        peer_state
            .force_local_close_hang
            .store(true, Ordering::SeqCst);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let started = Instant::now();
        runtime.block_on(close_data_channel(
            &data_channel,
            &mut pending,
            peer_state.as_ref(),
            LocalWebrtcTerminalCause::ChannelClosed,
        ));
        let elapsed = started.elapsed();
        assert!(
            elapsed >= LOCAL_WEBRTC_PEER_CLOSE_BOUND,
            "hang inject must wait for the production bound: {elapsed:?}"
        );
        assert!(
            elapsed < LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE,
            "cleanup_once must still run after a hung local_close: {elapsed:?}"
        );
        assert!(pending.is_empty());
        let ControlMessage::LocalWebrtcPeerClosed {
            grant_id,
            terminal_record,
            ..
        } = receive_test_runtime_message(&mut runtime_rx)
        else {
            panic!("hung local_close must still emit LocalWebrtcPeerClosed");
        };
        assert_eq!(grant_id, "grant-local-close-hang");
        assert_eq!(
            terminal_record.cause,
            LocalWebrtcTerminalCause::ChannelClosed
        );
    }

    #[test]
    fn production_on_close_hangs_local_close_and_still_cleans_up() {
        let data_channel = FakeDataChannel::default();
        data_channel
            .events
            .lock()
            .unwrap()
            .push_back(DataChannelEvent::OnClose);
        let key = AesGcmKey::from_slice(&[22; 32]).unwrap();
        let (runtime_tx, mut runtime_rx) = tokio_mpsc::channel(64);
        let peer_state = Arc::new(LocalWebrtcPeerState::new(
            "grant-on-close-hang".to_string(),
            runtime_tx.clone(),
        ));
        peer_state
            .force_local_close_hang
            .store(true, Ordering::SeqCst);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let started = Instant::now();
        let failure = runtime.block_on(run_data_channel(
            &data_channel,
            &key,
            peer_state.as_ref(),
            &runtime_tx,
        ));
        let elapsed = started.elapsed();
        assert!(failure.is_none());
        assert!(
            elapsed >= LOCAL_WEBRTC_PEER_CLOSE_BOUND,
            "OnClose must wait for the local_close bound: {elapsed:?}"
        );
        assert!(
            elapsed < LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE,
            "OnClose hang must still reach cleanup_once: {elapsed:?}"
        );
        let ControlMessage::LocalWebrtcPeerClosed { grant_id, .. } =
            receive_test_runtime_message(&mut runtime_rx)
        else {
            panic!("OnClose hang must still emit LocalWebrtcPeerClosed");
        };
        assert_eq!(grant_id, "grant-on-close-hang");
    }
    #[test]
    fn hung_send_text_times_out_within_close_bound() {
        let data_channel = FakeDataChannel::default();
        data_channel.send_hangs.store(true, Ordering::Release);
        let key = AesGcmKey::from_slice(&[17; 32]).unwrap();
        let mut pending = VecDeque::new();
        let mut flow_control = LocalWebrtcFlowControl::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let peer_state = test_peer_state("grant-hung-send-timeout");
        let started = Instant::now();
        let failure = runtime
            .block_on(send_response_frames(
                &data_channel,
                &key,
                &["response".to_string()],
                &mut pending,
                &mut flow_control,
                &peer_state,
            ))
            .expect_err("hung send_text must fail within the close bound");
        let elapsed = started.elapsed();
        assert_eq!(failure.cause, LocalWebrtcTerminalCause::SendText);
        assert!(
            elapsed >= LOCAL_WEBRTC_PEER_CLOSE_BOUND,
            "hung send must wait the close bound: {elapsed:?}"
        );
        assert!(
            elapsed < LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE,
            "hung send must not block cleanup: {elapsed:?}"
        );
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
        assert!(
            harness
                .daemon
                .local_webrtc()
                .dedicated_runtime_worker_threads()
                >= 1
        );
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
            || {
                harness
                    .daemon
                    .local_webrtc()
                    .dedicated_runtime_worker_threads()
                    == 0
            },
            "dedicated botster-local-webrtc worker threads to join",
        );

        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn webrtc_subscribe_events_requires_host_negotiation() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("evt");
        let mut peer = harness.signal_peer("http://127.0.0.1:41901");
        harness.hello_on_peer(
            &mut peer,
            DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                terminal_compatibility: Some(
                    botster_terminal_protocol::TerminalCompatibilityRequirement {
                        protocol: "botster-terminal-v1".to_string(),
                        protocol_version: 99,
                        required_features: vec!["missing_terminal_feature".to_string()],
                        minimum_conformance_fixture_revision: 1,
                        client_name: "webrtc-event-reject-terminal".to_string(),
                    },
                ),
            },
        );
        let unnegotiated = harness.request_on_peer(
            &mut peer,
            DaemonRequest::SubscribeEvents {
                subscription_id: "sub".to_string(),
                owner: "event-plane-producer".to_string(),
                name: "sample.ready".to_string(),
                subjects: Vec::new(),
            },
            "SubscribeEvents",
        );
        assert_eq!(
            unnegotiated.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            unnegotiated.error.as_ref().map(|error| error.code.as_str()),
            Some("package_event_subscriptions_not_negotiated")
        );
        let status = harness.request_on_peer(&mut peer, DaemonRequest::Status, "Status");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

        let mut negotiated = harness.signal_peer("http://127.0.0.1:41902");
        harness.hello_on_peer(
            &mut negotiated,
            DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility:
                    botster_hub_client::DaemonCompatibilityRequirement::for_package_event_subscriptions(),
                terminal_compatibility: Some(
                    botster_terminal_protocol::TerminalCompatibilityRequirement {
                        protocol: "botster-terminal-v1".to_string(),
                        protocol_version: 99,
                        required_features: vec!["missing_terminal_feature".to_string()],
                        minimum_conformance_fixture_revision: 1,
                        client_name: "webrtc-event-reject-terminal".to_string(),
                    },
                ),
            },
        );
        let subscribed = harness.request_on_peer(
            &mut negotiated,
            DaemonRequest::SubscribeEvents {
                subscription_id: "sub-neg".to_string(),
                owner: "event-plane-producer".to_string(),
                name: "sample.ready".to_string(),
                subjects: Vec::new(),
            },
            "SubscribeEvents",
        );
        assert_eq!(
            subscribed.error.as_ref().map(|error| error.code.as_str()),
            Some("rejected_undeclared")
        );
        let status = harness.request_on_peer(&mut negotiated, DaemonRequest::Status, "Status");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        peer.close_offer();
        negotiated.close_offer();
        harness.cleanup();
    }

    #[test]
    fn webrtc_entity_subscription_returns_and_binds_a_dedicated_channel() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("entity-dedicated");
        let mut peer = harness.signal_peer("http://127.0.0.1:41903");
        harness.ensure_webrtc_adapter_hello(&mut peer);

        let subscribed = harness.subscribe_entities(&mut peer, "entity-dedicated-sub");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        let reservation = subscribed
            .subscription_reservation
            .expect("entity subscribe returns a reserved channel");
        assert_eq!(
            reservation.kind,
            botster_hub_client::DaemonSubscriptionReservationKind::Entity
        );
        assert!(!reservation.label.is_empty());
        assert!(reservation.generation > 0);
        assert_eq!(
            harness
                .state
                .pending_runtime
                .admission
                .connection_budgets
                .get(&reservation.peer_generation)
                .expect("peer budget")
                .channel_count(),
            2
        );

        harness.bind_reserved_on_peer(&mut peer, &reservation.label);
        harness.wait_until_reservation_bound(&peer.grant_id, &reservation.label);
        assert!(
            harness
                .state
                .entity_subscriptions
                .contains_key("entity-dedicated-sub")
        );

        let unsubscribed = harness.request_on_peer(
            &mut peer,
            DaemonRequest::UnsubscribeEntities {
                subscription_id: "entity-dedicated-sub".to_string(),
            },
            "UnsubscribeEntities",
        );
        assert_eq!(
            unsubscribed.kind,
            botster_hub_client::DaemonResponseKind::EntityUnsubscribed
        );
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key("entity-dedicated-sub")
        );
        assert!(
            harness
                .state
                .pending_runtime
                .admission
                .reservations
                .reservation_for_label(&reservation.label, reservation.peer_generation)
                .is_none()
        );
        assert_eq!(
            harness
                .state
                .pending_runtime
                .admission
                .connection_budgets
                .get(&reservation.peer_generation)
                .expect("peer budget remains for control")
                .channel_count(),
            1
        );

        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn reservation_rejection_states_and_timeout_release_are_distinct() {
        let _teardown_guard = teardown_test_lock();
        let previous_expiry = std::env::var("BOTSTER_HUB_TEST_RESERVATION_EXPIRES_IN_SECONDS").ok();
        let previous_botster_env = std::env::var("BOTSTER_ENV").ok();
        unsafe {
            std::env::set_var("BOTSTER_ENV", "test");
            std::env::set_var("BOTSTER_HUB_TEST_RESERVATION_EXPIRES_IN_SECONDS", "1");
        }
        let mut harness = PeerHarness::new("reservation-matrix");
        let mut peer_a = harness.signal_peer("http://127.0.0.1:41904");
        let mut peer_b = harness.signal_peer("http://127.0.0.1:41905");
        harness.ensure_webrtc_adapter_hello(&mut peer_a);
        harness.ensure_webrtc_adapter_hello(&mut peer_b);

        let inspect = |harness: &mut PeerHarness, grant_id: &str, label: &str| {
            let (reply_tx, reply_rx) = oneshot::channel();
            handle_control_message(
                &mut harness.daemon,
                &mut harness.state,
                &harness.terminal_path,
                &harness.transport_handle,
                harness.control_tx.clone(),
                ControlMessage::InspectReservation {
                    grant_id: grant_id.to_string(),
                    label: label.to_string(),
                    reply_tx,
                },
            );
            reply_rx.blocking_recv().expect("reservation inspection")
        };

        let live = harness.subscribe_entities(&mut peer_a, "matrix-live");
        let live_reservation = live.subscription_reservation.expect("live reservation");
        assert!(matches!(
            inspect(&mut harness, &peer_a.grant_id, &live_reservation.label),
            ReservationInspectReply::Live { .. }
        ));
        assert_eq!(
            inspect(&mut harness, &peer_b.grant_id, &live_reservation.label),
            ReservationInspectReply::Stale
        );
        assert_eq!(
            inspect(&mut harness, &peer_a.grant_id, "never-reserved"),
            ReservationInspectReply::Unknown
        );

        harness.bind_reserved_on_peer(&mut peer_a, &live_reservation.label);
        harness.wait_until_reservation_bound(&peer_a.grant_id, &live_reservation.label);
        assert_eq!(
            inspect(&mut harness, &peer_a.grant_id, &live_reservation.label),
            ReservationInspectReply::Bound
        );

        let over_limit = harness.subscribe_entities(&mut peer_a, "matrix-over-limit");
        let over_limit_reservation = over_limit
            .subscription_reservation
            .expect("over-limit backstop reservation");
        harness
            .state
            .pending_runtime
            .admission
            .connection_budgets
            .get_mut(&over_limit_reservation.peer_generation)
            .expect("peer budget")
            .release(&over_limit_reservation.label);
        assert_eq!(
            inspect(
                &mut harness,
                &peer_a.grant_id,
                &over_limit_reservation.label,
            ),
            ReservationInspectReply::OverLimit
        );
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key("matrix-over-limit")
        );

        let late = harness.subscribe_entities(&mut peer_a, "matrix-late");
        let late_reservation = late.subscription_reservation.expect("late reservation");
        std::thread::sleep(Duration::from_millis(1_100));
        assert!(matches!(
            inspect(&mut harness, &peer_a.grant_id, &late_reservation.label),
            ReservationInspectReply::Expired { .. }
        ));
        assert!(
            !harness
                .state
                .entity_subscriptions
                .contains_key("matrix-late")
        );
        assert_eq!(
            harness
                .state
                .pending_runtime
                .admission
                .connection_budgets
                .get(&late_reservation.peer_generation)
                .expect("peer budget")
                .channel_count(),
            2,
            "control and the bound live route remain after timeout cleanup"
        );

        unsafe {
            match previous_botster_env {
                Some(value) => std::env::set_var("BOTSTER_ENV", value),
                None => std::env::remove_var("BOTSTER_ENV"),
            }
            match previous_expiry {
                Some(value) => {
                    std::env::set_var("BOTSTER_HUB_TEST_RESERVATION_EXPIRES_IN_SECONDS", value)
                }
                None => std::env::remove_var("BOTSTER_HUB_TEST_RESERVATION_EXPIRES_IN_SECONDS"),
            }
        }
        peer_a.close_offer();
        peer_b.close_offer();
        harness.cleanup();
    }

    #[test]
    fn webrtc_negotiated_peer_receives_package_event_and_gap_without_later_traffic() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new_with_event_queue("evt-live", Some(1));
        harness.enable_event_plane_producer();
        let mut peer = harness.signal_peer("http://127.0.0.1:41911");
        harness.hello_on_peer(
            &mut peer,
            DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility:
                    botster_hub_client::DaemonCompatibilityRequirement::for_package_event_subscriptions(),
                terminal_compatibility: Some(
                    botster_terminal_protocol::TerminalCompatibilityRequirement {
                        protocol: "botster-terminal-v1".to_string(),
                        protocol_version: 99,
                        required_features: vec!["missing_terminal_feature".to_string()],
                        minimum_conformance_fixture_revision: 1,
                        client_name: "webrtc-event-live".to_string(),
                    },
                ),
            },
        );
        peer.enable_host_events();
        let subscribed = harness.request_on_peer(
            &mut peer,
            DaemonRequest::SubscribeEvents {
                subscription_id: "sub-live".to_string(),
                owner: "event-plane-producer".to_string(),
                name: "sample.ready".to_string(),
                subjects: Vec::new(),
            },
            "SubscribeEvents",
        );
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EventSubscribed
        );
        let reservation = subscribed
            .subscription_reservation
            .as_ref()
            .expect("event subscribe returns a reserved channel");
        assert_eq!(
            reservation.kind,
            botster_hub_client::DaemonSubscriptionReservationKind::PackageEvent
        );
        assert!(!reservation.label.is_empty());
        assert!(
            !harness
                .state
                .pending_runtime
                .webrtc_is_admitted(&peer.grant_id),
            "package-event Hello must not admit a terminal adapter"
        );
        let mailbox = harness
            .daemon
            .local_webrtc()
            .event_plane()
            .mailbox(&peer.grant_id)
            .expect("subscribed connection has a mailbox");
        let mut saw_full = false;
        for index in 0..8 {
            match mailbox.try_push(
                "sub-live",
                "event-plane-producer",
                "sample.ready",
                serde_json::json!({ "ok": true, "token": format!("fill-{index}") }),
                8,
            ) {
                Ok(()) => {}
                Err(crate::package_event_router::EventPlaneStatus::ShedFull) => {
                    saw_full = true;
                    break;
                }
                other => panic!("unexpected mailbox fill result: {other:?}"),
            }
        }
        assert!(saw_full, "one-event mailbox must shed and set a gap bit");
        let reservation_label = reservation.label.clone();
        harness.bind_reserved_on_peer(&mut peer, &reservation_label);
        harness.wait_until_reservation_bound(&peer.grant_id, &reservation_label);
        let first = harness.wait_for_reserved_event(&mut peer, &reservation_label);
        match first {
            DaemonEvent::EventGap {
                subscription_id,
                owner,
                name,
            } => {
                assert_eq!(subscription_id, "sub-live");
                assert_eq!(owner, "event-plane-producer");
                assert_eq!(name, "sample.ready");
            }
            other => panic!("full mailbox must emit EventGap first: {other:?}"),
        }
        let queued = harness.wait_for_reserved_event(&mut peer, &reservation_label);
        match queued {
            DaemonEvent::PackageEvent {
                subscription_id, ..
            } => {
                assert_eq!(subscription_id, "sub-live");
            }
            other => panic!("queued event remains after gap: {other:?}"),
        }
        harness.emit_sample_ready("after-drain");
        let live = harness.wait_for_reserved_event(&mut peer, &reservation_label);
        match live {
            DaemonEvent::PackageEvent {
                subscription_id,
                payload,
                ..
            } => {
                assert_eq!(subscription_id, "sub-live");
                assert_eq!(payload["token"], "after-drain");
            }
            other => panic!("live emit after drain must be PackageEvent: {other:?}"),
        }
        let status = harness.request_on_peer(&mut peer, DaemonRequest::Status, "Status");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let entities = harness.subscribe_entities(&mut peer, "entity-under-event-pressure");
        assert_eq!(
            entities.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(
            !harness
                .state
                .pending_runtime
                .webrtc_is_admitted(&peer.grant_id),
            "event delivery must not create a terminal adapter"
        );
        peer.close_offer();
        harness.cleanup();
    }

    #[test]
    fn webrtc_status_and_entity_progress_under_event_flood() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new_with_event_queue("evt-flood", Some(8));
        harness.enable_event_plane_producer();
        let mut peer = harness.signal_peer("http://127.0.0.1:41912");
        harness.hello_on_peer(
            &mut peer,
            DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility:
                    botster_hub_client::DaemonCompatibilityRequirement::for_package_event_subscriptions(),
                terminal_compatibility: None,
            },
        );
        peer.enable_host_events();
        let subscribed = harness.request_on_peer(
            &mut peer,
            DaemonRequest::SubscribeEvents {
                subscription_id: "sub-flood".to_string(),
                owner: "event-plane-producer".to_string(),
                name: "sample.ready".to_string(),
                subjects: Vec::new(),
            },
            "SubscribeEvents",
        );
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EventSubscribed
        );
        let reservation_label = subscribed
            .subscription_reservation
            .as_ref()
            .expect("event subscribe returns a reserved channel")
            .label
            .clone();
        harness.bind_reserved_on_peer(&mut peer, &reservation_label);
        harness.wait_until_reservation_bound(&peer.grant_id, &reservation_label);
        let mailbox = harness
            .daemon
            .local_webrtc()
            .event_plane()
            .mailbox(&peer.grant_id)
            .expect("subscribed connection has a mailbox");
        for index in 0..8 {
            'admit: for attempt in 0..1_000 {
                match mailbox.try_push(
                    "sub-flood",
                    "event-plane-producer",
                    "sample.ready",
                    serde_json::json!({ "ok": true, "token": format!("flood-{index}") }),
                    8,
                ) {
                    Ok(()) => break 'admit,
                    Err(crate::package_event_router::EventPlaneStatus::ShedBusy) => {
                        assert!(
                            attempt < 999,
                            "mailbox stayed busy while admitting flood event"
                        );
                        std::thread::yield_now();
                    }
                    Err(status) => panic!("admit flood event: {status:?}"),
                }
            }
        }
        let status = harness.request_on_peer(&mut peer, DaemonRequest::Status, "Status");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let entities = harness.subscribe_entities(&mut peer, "entity-under-event-flood");
        assert_eq!(
            entities.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
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
            || {
                harness
                    .daemon
                    .local_webrtc()
                    .dedicated_runtime_worker_threads()
                    == 0
            },
            "first dedicated runtime workers to join",
        );

        let second = harness.signal_peer(origin);
        assert_eq!(harness.daemon.local_webrtc().active_peer_count(), 1);
        assert!(
            harness.daemon.local_webrtc().has_dedicated_runtime(),
            "new signal after last-peer park must recreate the dedicated runtime"
        );
        assert!(
            harness
                .daemon
                .local_webrtc()
                .dedicated_runtime_worker_threads()
                >= 1
        );

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
                frame_rx: None,
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
    fn webrtc_hello_bind_echoes_capability_set_and_closes_adapter_on_peer_loss() {
        let _teardown_guard = teardown_test_lock();
        let mut harness = PeerHarness::new("webrtc-hello-bind");
        let origin = "http://127.0.0.1:41821";
        let mut peer = harness.signal_peer(origin);
        let grant_id = peer.grant_id.clone();
        let session_id = "webrtc-hello-bind-session";
        let subscription_id = "webrtc-hello-bind-sub";
        let hello = DaemonHello {
            protocol: PROTOCOL.to_string(),
            compatibility:
                botster_hub_client::DaemonCompatibilityRequirement::for_webrtc_terminal_adapter(),
            terminal_compatibility: None,
        };
        let ack = harness.hello_on_peer(&mut peer, hello);
        assert_eq!(ack.protocol, PROTOCOL);
        assert!(
            ack.compatibility
                .supports_feature(botster_hub_client::FEATURE_WEBRTC_TERMINAL_ADAPTER)
        );
        let before = harness.state.lifecycle_counters.clone();
        harness.spawn_and_attach_on_peer(&mut peer, session_id, subscription_id);
        let inventory = harness
            .daemon
            .runtime_mut()
            .expect("runtime")
            .list_terminal_subscriptions();
        let bound = inventory
            .iter()
            .find(|row| row.session_id.0 == session_id && row.subscription_id.0 == subscription_id);
        let bound = bound.expect("bound inventory row");
        assert!(bound.adapter_bound);
        let expected = negotiated_unix_capability_set(
            &[botster_hub_client::FEATURE_WEBRTC_TERMINAL_ADAPTER.to_string()],
            None,
        )
        .expect("capability set");
        assert_eq!(bound.capabilities.as_ref(), Some(&expected));
        assert!(
            harness
                .state
                .pending_runtime
                .is_adapter_bound(session_id, subscription_id)
        );

        harness
            .daemon
            .local_webrtc()
            .inject_peer_connection_state_for_test(&grant_id, RTCPeerConnectionState::Failed);
        harness.process_until_peer_closed(&grant_id, Instant::now() + Duration::from_secs(10));
        assert!(!harness.daemon.local_webrtc().has_live_peer(&grant_id));
        let after = harness.state.lifecycle_counters.clone();
        let bound_closes = after
            .cleanup_by_reason
            .get("bound_adapter_close")
            .copied()
            .unwrap_or(0)
            .saturating_sub(
                before
                    .cleanup_by_reason
                    .get("bound_adapter_close")
                    .copied()
                    .unwrap_or(0),
            );
        let hub_detaches = after
            .cleanup_by_reason
            .get("cleanup_hub_detach")
            .copied()
            .unwrap_or(0)
            .saturating_sub(
                before
                    .cleanup_by_reason
                    .get("cleanup_hub_detach")
                    .copied()
                    .unwrap_or(0),
            );
        assert!(
            bound_closes >= 1,
            "bound peer loss must close the adapter: before={before:?} after={after:?}"
        );
        assert_eq!(
            hub_detaches, 0,
            "bound peer loss must not Hub-Detach: before={before:?} after={after:?}"
        );
        let _ = harness
            .daemon
            .runtime_mut()
            .expect("runtime")
            .observe_lifecycle_slice(
                1,
                None,
                botster_core_daemon::ObserveLifecycleBudget {
                    max_sessions: 32,
                    max_encoded_result_bytes: 64 * 1024,
                    max_elapsed: Duration::from_millis(25),
                },
            );
        let inventory = harness
            .daemon
            .runtime_mut()
            .expect("runtime")
            .list_terminal_subscriptions();
        assert!(
            inventory.iter().all(|row| {
                row.session_id.0 != session_id || row.subscription_id.0 != subscription_id
            }),
            "adapter Closed is the one Core detach: {inventory:?}"
        );
        assert!(harness.list_session_lifecycle(session_id).is_some());
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

        harness.ensure_webrtc_adapter_hello(&mut peer_a);
        harness.ensure_webrtc_adapter_hello(&mut peer_b);
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
        assert!(
            harness
                .daemon
                .local_webrtc()
                .dedicated_runtime_worker_threads()
                >= 1
        );
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
            harness.state.attach_close.released_attach_generations >= 1,
            "released attach generations must account for sibling detach"
        );
        wait_until(
            Instant::now() + Duration::from_secs(2),
            || {
                harness
                    .daemon
                    .local_webrtc()
                    .dedicated_runtime_worker_threads()
                    == 0
            },
            "dedicated runtime workers must join after fail-closed teardown",
        );

        let _ = harness
            .daemon
            .runtime_mut()
            .expect("runtime")
            .observe_lifecycle_slice(
                1,
                None,
                botster_core_daemon::ObserveLifecycleBudget {
                    max_sessions: 32,
                    max_encoded_result_bytes: 64 * 1024,
                    max_elapsed: Duration::from_millis(25),
                },
            );
        let inventory = harness
            .daemon
            .runtime_mut()
            .expect("runtime")
            .list_terminal_subscriptions();
        assert!(
            inventory.is_empty(),
            "fail-closed must leave zero Core inventory rows before session shutdown: {inventory:?}"
        );
        assert!(
            harness.state.pending_runtime.live_attach_routes.is_empty(),
            "fail-closed must leave zero Hub attach routes before session shutdown: {:?}",
            harness.state.pending_runtime.live_attach_routes
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
    fn ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners() {
        run_close_hang_fail_closed_body();
        local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads();
        let inventory_source = include_str!("peer.rs");
        assert!(
            inventory_source.contains("timeout fail-closed must sacrifice sibling peers"),
            "ultimate close failure must keep the bound-exceeded sibling-sacrifice oracle"
        );
        assert!(
            inventory_source.contains(
                "fail-closed must leave zero Core inventory rows before session shutdown"
            ),
            "ultimate close failure must keep the Core inventory sweep"
        );
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
        let mut peer_b = harness.signal_peer(origin);
        let grant_a = peer_a.grant_id.clone();
        let grant_b = peer_b.grant_id.clone();
        let subscription_id = "reused-entity-id".to_string();

        let _ = harness.subscribe_entities(&mut peer_a, &subscription_id);
        harness.ensure_webrtc_adapter_hello(&mut peer_b);
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
                frame_rx: None,
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
                frame_rx: None,
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
                client_id: Some(format!("botster-hub-webrtc-{grant_id}")),
                enqueued_at: Instant::now(),
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
                client_id: Some(format!("botster-hub-webrtc-{grant_id}")),
                enqueued_at: Instant::now(),
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
                frame_rx: None,
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
                frame_rx: None,
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

        harness.ensure_webrtc_adapter_hello(&mut peer);
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

        harness.ensure_webrtc_adapter_hello(&mut peer_a);
        harness.ensure_webrtc_adapter_hello(&mut peer_b);
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
            botster_hub_client::DaemonResponseKind::TerminalReservation,
            "replacement owner B must reserve successfully: {:?}",
            attach_b.error
        );
        assert!(attach_b.terminal_reservation.is_some());
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
        assert!(
            harness
                .daemon
                .local_webrtc()
                .dedicated_runtime_worker_threads()
                >= 1
        );

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
            || {
                harness
                    .daemon
                    .local_webrtc()
                    .dedicated_runtime_worker_threads()
                    == 0
            },
            "dedicated runtime workers must join after hang fail-closed teardown",
        );
        let inventory = harness
            .daemon
            .runtime_mut()
            .expect("runtime")
            .list_terminal_subscriptions();
        assert!(
            inventory.is_empty(),
            "timeout fail-closed must leave zero Core inventory rows: {inventory:?}"
        );
        assert!(
            harness.state.pending_runtime.live_attach_routes.is_empty(),
            "timeout fail-closed must leave zero Hub attach routes: {:?}",
            harness.state.pending_runtime.live_attach_routes
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
                "transport::webrtc::peer::tests::local_webrtc_close_hang_fail_closed_returns_handler_within_deadline",
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

    #[test]
    fn runtime_spawn_detach_on_drop_runs_to_completion() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime for spawn detach proof");
        tokio_rt.block_on(async {
            let runtime = default_runtime().expect("webrtc default runtime");
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            {
                let _handle = runtime.spawn(Box::pin(async move {
                    let _ = started_tx.send(());
                    tokio::task::yield_now().await;
                    let _ = done_tx.send(());
                }));
                started_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("spawned task must start");
            }
            done_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("Runtime::spawn must run to completion after JoinHandle drop");
        });
    }

    struct LateChannelHandler {
        gather_complete_tx: AsyncSender<()>,
        connected_tx: AsyncSender<()>,
        incoming_tx: AsyncSender<Arc<dyn DataChannel>>,
    }

    #[async_trait]
    impl PeerConnectionEventHandler for LateChannelHandler {
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

        async fn on_data_channel(&self, data_channel: Arc<dyn DataChannel>) {
            let _ = self.incoming_tx.try_send(data_channel);
        }
    }

    #[test]
    fn post_handshake_data_channel_opens_and_delivers_bytes() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("botster-webrtc-late-channel")
            .build()
            .expect("late-channel tokio runtime");
        tokio_rt.block_on(async {
            let runtime = default_runtime().expect("webrtc default runtime");
            let (offerer_gather_tx, mut offerer_gather_rx) = webrtc_channel::<()>(1);
            let (answerer_gather_tx, mut answerer_gather_rx) = webrtc_channel::<()>(1);
            let (offerer_connected_tx, mut offerer_connected_rx) = webrtc_channel::<()>(1);
            let (answerer_connected_tx, mut answerer_connected_rx) = webrtc_channel::<()>(1);
            let (incoming_tx, mut incoming_rx) = webrtc_channel::<Arc<dyn DataChannel>>(4);

            let offerer = PeerConnectionBuilder::new()
                .with_handler(Arc::new(LateChannelHandler {
                    gather_complete_tx: offerer_gather_tx,
                    connected_tx: offerer_connected_tx,
                    incoming_tx: incoming_tx.clone(),
                }))
                .with_runtime(runtime.clone())
                .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
                .build()
                .await
                .expect("build offerer");
            let answerer = PeerConnectionBuilder::new()
                .with_handler(Arc::new(LateChannelHandler {
                    gather_complete_tx: answerer_gather_tx,
                    connected_tx: answerer_connected_tx,
                    incoming_tx,
                }))
                .with_runtime(runtime.clone())
                .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
                .build()
                .await
                .expect("build answerer");

            let setup = offerer
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
                .expect("create pre-handshake setup DataChannel");
            let (setup_open_tx, mut setup_open_rx) = webrtc_channel::<()>(1);
            runtime.spawn(Box::pin({
                let setup = setup.clone();
                async move {
                    while let Some(event) = setup.poll().await {
                        match event {
                            DataChannelEvent::OnOpen => {
                                let _ = setup_open_tx.try_send(());
                            }
                            DataChannelEvent::OnClose => break,
                            _ => {}
                        }
                    }
                }
            }));

            let offer = offerer.create_offer(None).await.expect("create offer");
            offerer
                .set_local_description(offer)
                .await
                .expect("set local offer");
            timeout(
                runtime.as_ref(),
                Duration::from_secs(5),
                offerer_gather_rx.recv(),
            )
            .await
            .expect("offerer ICE gather")
            .expect("offerer gather signal");
            let offer = offerer
                .local_description()
                .await
                .expect("offerer local description");
            answerer
                .set_remote_description(offer)
                .await
                .expect("answerer set remote offer");
            let answer = answerer.create_answer(None).await.expect("create answer");
            answerer
                .set_local_description(answer)
                .await
                .expect("set local answer");
            timeout(
                runtime.as_ref(),
                Duration::from_secs(5),
                answerer_gather_rx.recv(),
            )
            .await
            .expect("answerer ICE gather")
            .expect("answerer gather signal");
            let answer = answerer
                .local_description()
                .await
                .expect("answerer local description");
            offerer
                .set_remote_description(answer)
                .await
                .expect("offerer set remote answer");

            timeout(
                runtime.as_ref(),
                Duration::from_secs(15),
                offerer_connected_rx.recv(),
            )
            .await
            .expect("offerer Connected")
            .expect("offerer connected signal");
            timeout(
                runtime.as_ref(),
                Duration::from_secs(15),
                answerer_connected_rx.recv(),
            )
            .await
            .expect("answerer Connected")
            .expect("answerer connected signal");
            timeout(
                runtime.as_ref(),
                Duration::from_secs(10),
                setup_open_rx.recv(),
            )
            .await
            .expect("pre-handshake setup channel open")
            .expect("setup open signal");
            let setup_remote = timeout(
                runtime.as_ref(),
                Duration::from_secs(10),
                incoming_rx.recv(),
            )
            .await
            .expect("remote setup on_data_channel")
            .expect("remote setup channel");
            assert_eq!(
                setup_remote.label().await.expect("setup remote label"),
                "botster-client"
            );

            let late = offerer
                .create_data_channel(
                    "botster-late",
                    Some(RTCDataChannelInit {
                        ordered: true,
                        max_retransmits: None,
                        max_packet_life_time: None,
                        ..Default::default()
                    }),
                )
                .await
                .expect("create post-handshake DataChannel");
            assert!(late.ordered().await.expect("late ordered"));
            assert_eq!(
                late.max_retransmits().await.expect("late retransmits"),
                None
            );
            assert_eq!(
                late.max_packet_life_time().await.expect("late lifetime"),
                None
            );

            let (late_open_tx, mut late_open_rx) = webrtc_channel::<()>(1);
            let (late_message_tx, late_message_rx) = webrtc_channel::<String>(8);
            runtime.spawn(Box::pin({
                let late = late.clone();
                async move {
                    while let Some(event) = late.poll().await {
                        match event {
                            DataChannelEvent::OnOpen => {
                                let _ = late_open_tx.try_send(());
                            }
                            DataChannelEvent::OnMessage(message) => {
                                if let Ok(text) = String::from_utf8(message.data.to_vec()) {
                                    let _ = late_message_tx.try_send(text);
                                }
                            }
                            DataChannelEvent::OnClose => break,
                            _ => {}
                        }
                    }
                }
            }));

            let remote = timeout(runtime.as_ref(), Duration::from_secs(10), async {
                loop {
                    let channel = incoming_rx
                        .recv()
                        .await
                        .expect("remote late on_data_channel");
                    if channel.label().await.expect("incoming label") == "botster-late" {
                        break channel;
                    }
                }
            })
            .await
            .expect("remote late channel by label");
            assert_eq!(remote.label().await.expect("remote label"), "botster-late");

            let (remote_open_tx, mut remote_open_rx) = webrtc_channel::<()>(1);
            let (remote_message_tx, mut remote_message_rx) = webrtc_channel::<String>(8);
            runtime.spawn(Box::pin({
                let remote = remote.clone();
                async move {
                    while let Some(event) = remote.poll().await {
                        match event {
                            DataChannelEvent::OnOpen => {
                                let _ = remote_open_tx.try_send(());
                            }
                            DataChannelEvent::OnMessage(message) => {
                                if let Ok(text) = String::from_utf8(message.data.to_vec()) {
                                    let _ = remote_message_tx.try_send(text);
                                }
                            }
                            DataChannelEvent::OnClose => break,
                            _ => {}
                        }
                    }
                }
            }));

            timeout(
                runtime.as_ref(),
                Duration::from_secs(10),
                late_open_rx.recv(),
            )
            .await
            .expect("creating side OnOpen")
            .expect("late open signal");
            timeout(
                runtime.as_ref(),
                Duration::from_secs(10),
                remote_open_rx.recv(),
            )
            .await
            .expect("remote OnOpen")
            .expect("remote open signal");

            const PAYLOAD: &str = "post-handshake-bytes";
            late.send_text(PAYLOAD)
                .await
                .expect("send on late DataChannel");
            let received = timeout(
                runtime.as_ref(),
                Duration::from_secs(10),
                remote_message_rx.recv(),
            )
            .await
            .expect("remote delivery")
            .expect("remote payload");
            assert_eq!(received, PAYLOAD);
            let _ = late_message_rx;
            let _ = offerer.close().await;
            let _ = answerer.close().await;
        });
    }

    #[test]
    fn peer_closed_removes_webrtc_admission_and_host_compatibility() {
        let mut harness = PeerHarness::new("hello-sweep");
        let origin = "http://127.0.0.1:41821";
        let peer = harness.signal_peer(origin);
        let grant_id = peer.grant_id.clone();

        handle_control_message(
            &mut harness.daemon,
            &mut harness.state,
            &harness.terminal_path,
            &harness.transport_handle,
            harness.control_tx.clone(),
            ControlMessage::RegisterWebrtcAdmission {
                grant_id: grant_id.clone(),
                admission: WebrtcTerminalAdmission::Rejected {
                    code: "test_admission_row",
                    diagnostic: DaemonDiagnostic::connected("hello"),
                    mux: WebRtcConnectionMux::new(),
                    peer_generation: 0,
                },
                host_required_features: vec!["host-feature".to_string()],
            },
        );
        assert!(
            harness
                .state
                .pending_runtime
                .has_webrtc_admission_row(&grant_id),
            "positive control: RegisterWebrtcAdmission must insert the admission row"
        );
        assert!(
            harness
                .state
                .pending_runtime
                .has_host_compatibility_row(&grant_id),
            "positive control: RegisterWebrtcAdmission must insert the host compatibility row"
        );
        assert!(
            !harness.state.pending_runtime.webrtc_is_admitted(&grant_id),
            "Rejected rows must not satisfy webrtc_is_admitted; the sweep uses contains_key"
        );

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
                terminal_record: LocalWebrtcSenderTerminalRecord {
                    schema_version: 1,
                    grant_id: grant_id.clone(),
                    request_operation: "hello".to_string(),
                    message_id: None,
                    next_chunk_index: 0,
                    last_sent_chunk_index: None,
                    total_chunks: 0,
                    pressured: false,
                    peer_connection_state: "closed".to_string(),
                    channel_terminal_signal: LocalWebrtcChannelTerminalSignal::None,
                    cause: LocalWebrtcTerminalCause::PeerClosed,
                    cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
                },
            },
        );
        assert!(
            !harness
                .state
                .pending_runtime
                .has_webrtc_admission_row(&grant_id),
            "PeerClosed must remove the webrtc admission row"
        );
        assert!(
            !harness
                .state
                .pending_runtime
                .has_host_compatibility_row(&grant_id),
            "PeerClosed must remove the host compatibility row"
        );

        peer.close_offer();
        harness.cleanup();
    }
}
