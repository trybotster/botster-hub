//! Control requests that wait on Core owner-thread results.
//!
//! The Hub owner never blocks on Core. A request family that needs Core
//! returns [`ControlStep::Pending`] with a continuation; the owner stores it
//! as a [`PendingControlRequest`], polls it on every turn, and sends the
//! response through the transport's reply channel when the continuation
//! reports [`ControlPoll::Ready`]. Continuations run on the owner thread and
//! may mutate owner state exactly as the synchronous handlers did.

use std::sync::mpsc;
use std::time::Instant;

use botster_hub_client::{DaemonRequest, DaemonResponse};

use crate::HubDaemon;
use crate::daemon::control::message::ControlReplySender;
use crate::daemon::error::DaemonTransportResult;
use crate::daemon::owner_loop::DaemonControlState;

/// Outcome of one continuation poll.
pub(crate) enum ControlPoll {
    /// Core has not answered yet.
    Pending,
    /// The response is complete.
    Ready(DaemonTransportResult<DaemonResponse>),
}

/// One owner-thread continuation for a request that waits on Core.
pub(crate) type ControlContinuation =
    Box<dyn FnMut(&mut HubDaemon, &mut DaemonControlState) -> ControlPoll + Send>;

/// Result of starting one control request.
pub(crate) enum ControlStep {
    Ready(DaemonTransportResult<DaemonResponse>),
    Pending(ControlContinuation),
}

impl ControlStep {
    pub(crate) fn ready(response: DaemonResponse) -> Self {
        Self::Ready(Ok(response))
    }

    pub(crate) fn pending(
        continuation: impl FnMut(&mut HubDaemon, &mut DaemonControlState) -> ControlPoll
        + Send
        + 'static,
    ) -> Self {
        Self::Pending(Box::new(continuation))
    }
}

impl From<DaemonTransportResult<DaemonResponse>> for ControlStep {
    fn from(result: DaemonTransportResult<DaemonResponse>) -> Self {
        Self::Ready(result)
    }
}

/// One request the owner accepted and is still waiting to answer.
pub(crate) struct PendingControlRequest {
    pub(crate) request: DaemonRequest,
    pub(crate) reply_tx: ControlReplySender,
    pub(crate) response_delivery_rx: Option<mpsc::Receiver<()>>,
    pub(crate) grant_id: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) accepted_at: Instant,
    pub(crate) continuation: ControlContinuation,
}

/// Poll every pending request once. Finished requests are answered through
/// `finish` in acceptance order. Returns `true` when a `shutdown` response
/// was sent, matching the synchronous handler's return contract.
pub(crate) fn poll_pending_requests(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    mut finish: impl FnMut(
        &mut HubDaemon,
        &mut DaemonControlState,
        PendingControlRequest,
        DaemonTransportResult<DaemonResponse>,
    ) -> bool,
) -> bool {
    if state.pending_requests.is_empty() {
        return false;
    }
    if let Some(runtime) = daemon.runtime() {
        runtime.absorb_core_completions();
    }
    let mut pending = std::mem::take(&mut state.pending_requests);
    let mut retained = Vec::with_capacity(pending.len());
    let mut shutdown = false;
    for mut entry in pending.drain(..) {
        if shutdown {
            retained.push(entry);
            continue;
        }
        match (entry.continuation)(daemon, state) {
            ControlPoll::Pending => retained.push(entry),
            ControlPoll::Ready(response) => {
                shutdown = finish(daemon, state, entry, response);
            }
        }
    }
    // Requests accepted while a continuation ran (none today) would be
    // appended by the handler; keep them after the retained ones.
    retained.append(&mut state.pending_requests);
    state.pending_requests = retained;
    shutdown
}
