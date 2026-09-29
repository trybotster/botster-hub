use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
use botster_hub_client::{DaemonLocalWebrtcTerminalRecord, DaemonProtocolErrorCode, DaemonRequest};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use webrtc::data_channel::DataChannel;
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionEventHandler, RTCIceGatheringState, RTCPeerConnectionState,
};
use webrtc::runtime::{Runtime, Sender as AsyncSender, default_runtime};

#[cfg(test)]
use crate::daemon::control::message::control_reply_channel;
use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::subscription::entity::EntitySubscriptionCapacityWake;
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
pub(crate) const LOCAL_WEBRTC_TERMINAL_RECORD_MAX_BYTES: usize = 2 * 1024;
pub(crate) const LOCAL_WEBRTC_TERMINAL_RECORD_MAX_ENTRIES: usize = 64;
/// The status section uses at most one eighth of the 1 MiB response limit.
pub(crate) const LOCAL_WEBRTC_TERMINAL_RECORD_MAX_TOTAL_BYTES: usize = 128 * 1024;
const LOCAL_WEBRTC_TERMINAL_GRANT_ID_MAX_BYTES: usize = 64;
const LOCAL_WEBRTC_TERMINAL_OPERATION_MAX_BYTES: usize = 64;
const LOCAL_WEBRTC_TERMINAL_MESSAGE_ID_MAX_BYTES: usize = 64;
const LOCAL_WEBRTC_TERMINAL_PEER_STATE_MAX_BYTES: usize = 32;
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
    pub(crate) entity_capacity_wake: EntitySubscriptionCapacityWake,
    pub(crate) peers: BTreeMap<String, Arc<dyn PeerConnection>>,
    /// Live peer ownership records used for fail-closed sibling cleanup.
    pub(crate) peer_states: BTreeMap<String, Arc<LocalWebrtcPeerState>>,
    /// Peers whose `close()` failed while siblings kept the shared runtime alive.
    /// Retained so a later empty-map park / `stop_all` can still force driver stop.
    pub(crate) stale_close_peers: BTreeMap<String, Arc<dyn PeerConnection>>,
    /// The latest bounded close evidence for each retained grant, oldest first.
    terminal_records: VecDeque<RetainedLocalWebrtcTerminalRecord>,
    terminal_record_bytes: usize,
    terminal_record_evictions: u64,
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

#[derive(Debug)]
struct RetainedLocalWebrtcTerminalRecord {
    record: DaemonLocalWebrtcTerminalRecord,
    encoded_len: usize,
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
    /// A transport whose client event plane raises the Hub owner's `signal`.
    pub(crate) fn with_owner_signal(signal: Arc<crate::daemon::owner_signal::OwnerSignal>) -> Self {
        Self {
            event_plane: SharedEventPlane(Arc::new(
                crate::subscription::package_events::ClientEventPlane::new(signal),
            )),
            ..Self::default()
        }
    }

    pub(crate) fn bind_entity_capacity_wake(&mut self, wake: EntitySubscriptionCapacityWake) {
        self.entity_capacity_wake = wake;
    }

    #[must_use]
    pub(crate) fn event_plane(&self) -> Arc<crate::subscription::package_events::ClientEventPlane> {
        self.event_plane.0.clone()
    }

    pub(crate) fn retain_terminal_record(
        &mut self,
        record: LocalWebrtcSenderTerminalRecord,
    ) -> Result<(), &'static str> {
        let record = record.into_daemon_record()?;
        let encoded_len = serde_json::to_vec(&record)
            .map_err(|_| "local WebRTC terminal record did not serialize")?
            .len();
        if encoded_len > LOCAL_WEBRTC_TERMINAL_RECORD_MAX_BYTES {
            return Err("local WebRTC terminal record exceeded its byte bound");
        }
        if let Some(index) = self
            .terminal_records
            .iter()
            .position(|retained| retained.record.grant_id == record.grant_id)
        {
            let replaced = self
                .terminal_records
                .remove(index)
                .expect("terminal record index came from the same queue");
            self.terminal_record_bytes = self
                .terminal_record_bytes
                .saturating_sub(replaced.encoded_len);
        }
        self.terminal_record_bytes = self.terminal_record_bytes.saturating_add(encoded_len);
        self.terminal_records
            .push_back(RetainedLocalWebrtcTerminalRecord {
                record,
                encoded_len,
            });
        while self.terminal_records.len() > LOCAL_WEBRTC_TERMINAL_RECORD_MAX_ENTRIES
            || self.terminal_record_bytes > LOCAL_WEBRTC_TERMINAL_RECORD_MAX_TOTAL_BYTES
        {
            let evicted = self
                .terminal_records
                .pop_front()
                .expect("a retained terminal record exceeds a nonzero bound");
            self.terminal_record_bytes = self
                .terminal_record_bytes
                .saturating_sub(evicted.encoded_len);
            self.terminal_record_evictions = self.terminal_record_evictions.saturating_add(1);
        }
        Ok(())
    }

    /// Retained JSON lengths bound string bytes without copying the records.
    pub(crate) fn terminal_records_bytes(&self, limit: usize) -> Option<usize> {
        let mut bytes = std::mem::size_of::<Vec<DaemonLocalWebrtcTerminalRecord>>().checked_add(
            self.terminal_records
                .len()
                .checked_mul(std::mem::size_of::<DaemonLocalWebrtcTerminalRecord>())?,
        )?;
        if bytes > limit {
            return None;
        }
        for retained in &self.terminal_records {
            bytes = bytes.checked_add(retained.encoded_len)?;
            if bytes > limit {
                return None;
            }
        }
        Some(bytes)
    }

    pub(crate) fn bounded_terminal_records(
        &self,
        limit: usize,
    ) -> Option<(Vec<DaemonLocalWebrtcTerminalRecord>, usize)> {
        let bytes = self.terminal_records_bytes(limit)?;
        Some((self.terminal_records(), bytes))
    }

    pub(crate) fn terminal_records(&self) -> Vec<DaemonLocalWebrtcTerminalRecord> {
        self.terminal_records
            .iter()
            .map(|retained| retained.record.clone())
            .collect()
    }

    #[cfg(test)]
    pub(crate) const fn terminal_record_evictions(&self) -> u64 {
        self.terminal_record_evictions
    }

    #[cfg(test)]
    pub(crate) fn terminal_record(
        &self,
        grant_id: &str,
    ) -> Option<&DaemonLocalWebrtcTerminalRecord> {
        self.terminal_records
            .iter()
            .find(|retained| retained.record.grant_id == grant_id)
            .map(|retained| &retained.record)
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
    pub(crate) entity_capacity_wake: EntitySubscriptionCapacityWake,
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
    /// The client broke a host-control protocol rule; Hub sent the typed
    /// close reason before closing the channel.
    ProtocolViolation(DaemonProtocolErrorCode),
    /// The hello named another protocol version; Hub answered the ack and
    /// closed without request service.
    ProtocolVersionMismatch,
    /// Hub delivered a `shutdown` response and closed the channel.
    DaemonShutdown,
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
            Self::ProtocolViolation(code) => {
                return write!(formatter, "protocol_violation:{}", code.as_str());
            }
            Self::ProtocolVersionMismatch => "protocol_version_mismatch",
            Self::DaemonShutdown => "daemon_shutdown",
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

impl LocalWebrtcSenderTerminalRecord {
    fn into_daemon_record(self) -> Result<DaemonLocalWebrtcTerminalRecord, &'static str> {
        if self.grant_id.len() > LOCAL_WEBRTC_TERMINAL_GRANT_ID_MAX_BYTES {
            return Err("local WebRTC terminal grant id exceeded its byte bound");
        }
        if self.request_operation.len() > LOCAL_WEBRTC_TERMINAL_OPERATION_MAX_BYTES {
            return Err("local WebRTC terminal operation exceeded its byte bound");
        }
        if self
            .message_id
            .as_ref()
            .is_some_and(|message_id| message_id.len() > LOCAL_WEBRTC_TERMINAL_MESSAGE_ID_MAX_BYTES)
        {
            return Err("local WebRTC terminal message id exceeded its byte bound");
        }
        if self.peer_connection_state.len() > LOCAL_WEBRTC_TERMINAL_PEER_STATE_MAX_BYTES {
            return Err("local WebRTC terminal peer state exceeded its byte bound");
        }
        Ok(DaemonLocalWebrtcTerminalRecord {
            schema_version: self.schema_version,
            grant_id: self.grant_id,
            request_operation: self.request_operation,
            message_id: self.message_id,
            next_chunk_index: self.next_chunk_index,
            last_sent_chunk_index: self.last_sent_chunk_index,
            total_chunks: self.total_chunks,
            pressured: self.pressured,
            peer_connection_state: self.peer_connection_state,
            channel_terminal_signal: match self.channel_terminal_signal {
                LocalWebrtcChannelTerminalSignal::None => "none",
                LocalWebrtcChannelTerminalSignal::OnClose => "on_close",
                LocalWebrtcChannelTerminalSignal::OnError => "on_error",
                LocalWebrtcChannelTerminalSignal::PollEnded => "poll_ended",
            }
            .to_string(),
            cause: self.cause.to_string(),
            cleanup_disposition: match self.cleanup_disposition {
                LocalWebrtcCleanupDisposition::NewlySent => "newly_sent",
            }
            .to_string(),
        })
    }
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
            EntitySubscriptionCapacityWake::default(),
        )
    }

    pub(crate) fn new_with_event_plane(
        grant_id: String,
        runtime_tx: ControlSender,
        _event_plane: Arc<crate::subscription::package_events::ClientEventPlane>,
        entity_capacity_wake: EntitySubscriptionCapacityWake,
    ) -> Self {
        let (peer_terminal_tx, _peer_terminal_rx) = watch::channel(None);
        Self {
            grant_id,
            runtime_tx,
            entity_capacity_wake,
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

    /// Whether this peer owns any terminal route: a bound adapter route or
    /// an attach it holds (including an unbound reservation).
    fn owns_routes(&self) -> bool {
        self.mux.has_bound_routes()
            || !self
                .attached_subscriptions
                .lock()
                .expect("local WebRTC peer subscription mutex")
                .is_empty()
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
            // A peer that owns routes, bound or only reserved, fails closed on
            // disconnect. Its reservations hold Core attaches and occupancy.
            RTCPeerConnectionState::Disconnected if self.owns_routes() => {
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
#[path = "peer_tests.rs"]
mod tests;
