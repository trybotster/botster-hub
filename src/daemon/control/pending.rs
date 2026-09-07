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
use crate::daemon::owner_budget::{OwnerPermit, RETAINED_OPERATION_DEADLINE};
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

/// Retirement for a request that owns Core work: it receives the entry's
/// permit and must retain an obligation that cancels or releases that work.
pub(crate) type RetireHook =
    Box<dyn FnOnce(&mut HubDaemon, &mut DaemonControlState, OwnerPermit) + Send>;

/// A deferred request: its continuation, and how to retire it when its
/// client leaves or its deadline passes. Without a hook, retirement drops
/// the continuation (a pure read) and releases the permit.
pub(crate) struct PendingStep {
    pub(crate) continuation: ControlContinuation,
    pub(crate) retire: Option<RetireHook>,
}

/// Result of starting one control request.
pub(crate) enum ControlStep {
    Ready(DaemonTransportResult<DaemonResponse>),
    Pending(PendingStep),
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
        Self::Pending(PendingStep {
            continuation: Box::new(continuation),
            retire: None,
        })
    }

    /// A deferred request whose Core work must be cancelled or released
    /// when the request is retired.
    pub(crate) fn pending_retirable(
        continuation: impl FnMut(&mut HubDaemon, &mut DaemonControlState) -> ControlPoll
        + Send
        + 'static,
        retire: impl FnOnce(&mut HubDaemon, &mut DaemonControlState, OwnerPermit) + Send + 'static,
    ) -> Self {
        Self::Pending(PendingStep {
            continuation: Box::new(continuation),
            retire: Some(Box::new(retire)),
        })
    }
}

impl From<DaemonTransportResult<DaemonResponse>> for ControlStep {
    fn from(result: DaemonTransportResult<DaemonResponse>) -> Self {
        Self::Ready(result)
    }
}

/// One request the owner accepted and is still waiting to answer.
///
/// The entry owns one budget permit for its whole life. `client` names the
/// connection or grant that sent it, so cleanup can retire abandoned reads
/// promptly; a request that must finish (it has Core side effects, or it
/// owns cleanup) keeps running after its client left.
pub(crate) struct PendingControlRequest {
    pub(crate) request: DaemonRequest,
    pub(crate) reply_tx: ControlReplySender,
    pub(crate) response_delivery_rx: Option<mpsc::Receiver<()>>,
    pub(crate) grant_id: Option<String>,
    pub(crate) client: Option<String>,
    pub(crate) permit: Option<OwnerPermit>,
    pub(crate) accepted_at: Instant,
    pub(crate) must_finish: bool,
    pub(crate) past_deadline: bool,
    pub(crate) continuation: ControlContinuation,
    pub(crate) retire: Option<RetireHook>,
}

/// Requests whose Core work has side effects the owner must observe, or
/// that consume state (ReceiveMessages drains routed envelopes). Everything
/// else is a read that can be retired when its client left or the deadline
/// passed: the Core answer is dropped, or the request's retire hook cancels
/// and releases what it owns (CaptureSnapshot).
pub(crate) fn request_must_finish(request: &DaemonRequest) -> bool {
    !matches!(
        request,
        DaemonRequest::Status { .. }
            | DaemonRequest::ListSessions { .. }
            | DaemonRequest::Whoami { .. }
            | DaemonRequest::ReadScreen { .. }
            | DaemonRequest::ReadModeFlags { .. }
            | DaemonRequest::CaptureSnapshot { .. }
            | DaemonRequest::ListSessionTypes { .. }
            | DaemonRequest::ListSessionTypesForTarget { .. }
            | DaemonRequest::ShowSessionType { .. }
            | DaemonRequest::ShowSessionTypeDefinition { .. }
            | DaemonRequest::ResolveSessionType { .. }
            | DaemonRequest::CheckHubUpdate { .. }
            | DaemonRequest::GetHubUpdateExecution { .. }
    )
}

/// Earliest deadline among pending requests not yet flagged.
pub(crate) fn next_request_deadline(pending: &[PendingControlRequest]) -> Option<Instant> {
    pending
        .iter()
        .filter(|entry| !entry.past_deadline)
        .map(|entry| entry.accepted_at + RETAINED_OPERATION_DEADLINE)
        .min()
}

fn retire(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    mut entry: PendingControlRequest,
    reason: &str,
) {
    match (entry.permit.take(), entry.retire.take()) {
        // The request owns Core work: the hook keeps the permit in an
        // obligation that cancels or releases it.
        (Some(permit), Some(hook)) => hook(daemon, state, permit),
        (Some(permit), None) => state.budget.release(permit),
        (None, _) => {}
    }
    state.budget.counters.retired_abandoned =
        state.budget.counters.retired_abandoned.saturating_add(1);
    *state
        .lifecycle_counters
        .cleanup_by_reason
        .entry(format!("request_retired:{reason}"))
        .or_insert(0) += 1;
    // Dropping the continuation drops its Core ticket; the Core answer is
    // discarded. Dropping `reply_tx` closes the reply channel.
    drop(entry);
}

/// Retire every retirable pending request `client` left behind.
pub(crate) fn retire_abandoned_requests(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    client: &str,
) {
    let pending = std::mem::take(&mut state.pending_requests);
    let mut retained = Vec::with_capacity(pending.len());
    for entry in pending {
        if !entry.must_finish && entry.client.as_deref() == Some(client) {
            retire(daemon, state, entry, "client_left");
        } else {
            retained.push(entry);
        }
    }
    retained.append(&mut state.pending_requests);
    state.pending_requests = retained;
}

/// Poll every pending request once. Finished requests are answered through
/// `finish` in acceptance order. Returns `true` when a `shutdown` response
/// was sent, matching the synchronous handler's return contract.
pub(crate) fn poll_pending_requests(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    now: Instant,
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
        let expired =
            now.saturating_duration_since(entry.accepted_at) >= RETAINED_OPERATION_DEADLINE;
        if !entry.must_finish && entry.reply_tx.is_closed() {
            retire(daemon, state, entry, "reply_closed");
            continue;
        }
        if !entry.must_finish && expired {
            retire(daemon, state, entry, "deadline");
            continue;
        }
        // A must-finish entry past its deadline is flagged once and then
        // excluded from the wake calculation, so it cannot busy-wake.
        if expired && !entry.past_deadline {
            entry.past_deadline = true;
            state.budget.counters.requests_past_deadline = state
                .budget
                .counters
                .requests_past_deadline
                .saturating_add(1);
            *state
                .lifecycle_counters
                .cleanup_by_reason
                .entry("request_past_deadline".to_string())
                .or_insert(0) += 1;
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
