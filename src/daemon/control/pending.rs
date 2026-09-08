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
use crate::daemon::control::reply::{ControlReply, RetainedPluginResult};
use crate::daemon::error::DaemonTransportResult;
use crate::daemon::owner_budget::OwnerPermit;
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::{DeadlineKey, ReadyClass, ReadyKey, ReadyReasons};
use crate::daemon::owner_turn::{OwnerTurnBudget, OwnerTurnCharge};
use crate::owner_identity::{OwnerWorkIdentity, WaiterId};

pub(crate) const READY_INITIAL: ReadyReasons = ReadyReasons::from_bits(1 << 0);
pub(crate) const READY_CORE_COMPLETION: ReadyReasons = ReadyReasons::from_bits(1 << 1);
pub(crate) const READY_PLUGIN_COMPLETION: ReadyReasons = ReadyReasons::from_bits(1 << 2);
pub(crate) const READY_HOST_COMPLETION: ReadyReasons = ReadyReasons::from_bits(1 << 3);
pub(crate) const READY_DEADLINE: ReadyReasons = ReadyReasons::from_bits(1 << 4);

/// Outcome of one continuation poll.
pub(crate) enum ControlPoll {
    /// Core has not answered yet.
    Pending,
    /// The response is complete.
    Ready(DaemonTransportResult<DaemonResponse>),
    /// The response still owns the logical-byte charge for a plugin result.
    ReadyRetained(RetainedPluginResult<DaemonTransportResult<DaemonResponse>>),
    /// The response owns a host prepared-byte charge through transport framing.
    ReadyHost(
        DaemonTransportResult<DaemonResponse>,
        crate::host_executor::HostPreparedCharge,
    ),
}

/// One owner-thread continuation for a request that waits on Core.
pub(crate) type ControlContinuation =
    Box<dyn FnMut(&mut HubDaemon, &mut DaemonControlState) -> ControlPoll + Send>;

/// Retirement for a request that owns deferred work. The hook receives the
/// entry permit. It must cancel, release, or transfer the work to another
/// bounded owner such as the plugin worker's executor and completion pools.
pub(crate) type RetireHook =
    Box<dyn FnOnce(&mut HubDaemon, &mut DaemonControlState, OwnerPermit) + Send>;

/// A deferred request: its continuation, and how to retire it when its
/// client leaves or its deadline passes. Without a hook, retirement drops
/// the continuation (a pure read) and releases the permit.
pub(crate) struct PendingStep {
    pub(crate) continuation: ControlContinuation,
    pub(crate) retire: Option<RetireHook>,
    pub(crate) ready_class: ReadyClass,
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
            ready_class: ReadyClass::CoreCompletion,
        })
    }

    pub(crate) fn pending_in(
        ready_class: ReadyClass,
        continuation: impl FnMut(&mut HubDaemon, &mut DaemonControlState) -> ControlPoll
        + Send
        + 'static,
    ) -> Self {
        Self::Pending(PendingStep {
            continuation: Box::new(continuation),
            retire: None,
            ready_class,
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
            ready_class: ReadyClass::CoreCompletion,
        })
    }

    pub(crate) fn pending_retirable_in(
        ready_class: ReadyClass,
        continuation: impl FnMut(&mut HubDaemon, &mut DaemonControlState) -> ControlPoll
        + Send
        + 'static,
        retire: impl FnOnce(&mut HubDaemon, &mut DaemonControlState, OwnerPermit) + Send + 'static,
    ) -> Self {
        Self::Pending(PendingStep {
            continuation: Box::new(continuation),
            retire: Some(Box::new(retire)),
            ready_class,
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
    pub(crate) waiter_id: WaiterId,
    pub(crate) ready_class: ReadyClass,
    pub(crate) ready_key: Option<ReadyKey>,
    pub(crate) deadline_key: Option<DeadlineKey>,
    pub(crate) last_core_phase: u64,
    pub(crate) last_host_phase: u64,
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

/// Requests whose Core work has effects that require an owner continuation,
/// or that consume state, must finish. Plugin actions are the exception.
/// Their execution remains charged in the plugin worker after reply
/// retirement, and a late completion is drained without replay or delivery.
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
            | DaemonRequest::PluginMcpCallTool { .. }
            | DaemonRequest::PluginSurfaceRender { .. }
            | DaemonRequest::PluginSurfaceAction { .. }
    )
}

/// Earliest deadline among pending requests not yet flagged.
pub(crate) fn next_request_deadline(state: &DaemonControlState) -> Option<Instant> {
    state.request_deadlines.next_deadline()
}

pub(crate) fn mark_request_ready(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    class: ReadyClass,
    reasons: ReadyReasons,
) -> bool {
    if !state.pending_requests.contains_key(&waiter_id) {
        return false;
    }
    let Ok(key) = state.request_ready.mark(waiter_id, class, reasons) else {
        return false;
    };
    state
        .pending_requests
        .get_mut(&waiter_id)
        .expect("a marked pending waiter must exist")
        .ready_key = Some(key);
    true
}

pub(crate) fn absorb_core_completions(
    state: &mut DaemonControlState,
    identities: &[OwnerWorkIdentity],
    budget: &mut OwnerTurnBudget,
) -> usize {
    for (index, identity) in identities.iter().copied().enumerate() {
        if budget
            .try_charge(Instant::now(), OwnerTurnCharge::opaque_move())
            .is_err()
        {
            return index;
        }
        let Some(entry) = state.pending_requests.get_mut(&identity.waiter_id) else {
            continue;
        };
        let Some(expected) = entry.last_core_phase.checked_add(1) else {
            continue;
        };
        if identity.phase != expected {
            continue;
        }
        entry.last_core_phase = identity.phase;
        mark_request_ready(
            state,
            identity.waiter_id,
            ReadyClass::CoreCompletion,
            READY_CORE_COMPLETION,
        );
    }
    identities.len()
}

pub(crate) fn absorb_host_completion(
    state: &mut DaemonControlState,
    completion: crate::host_executor::HostCompletion,
) {
    let identity = completion.identity;
    let Some(entry) = state.pending_requests.get_mut(&identity.waiter_id) else {
        return;
    };
    let Some(expected) = entry.last_host_phase.checked_add(1) else {
        return;
    };
    if identity.phase != expected || state.host_completions.contains_key(&identity.waiter_id) {
        return;
    }
    entry.last_host_phase = identity.phase;
    state
        .host_completions
        .insert(identity.waiter_id, completion);
    mark_request_ready(
        state,
        identity.waiter_id,
        ReadyClass::HostCompletion,
        READY_HOST_COMPLETION,
    );
}

pub(crate) fn mark_due_request_deadlines(
    state: &mut DaemonControlState,
    now: Instant,
    budget: &mut OwnerTurnBudget,
) {
    while state.request_deadlines.has_due(now) {
        if budget
            .try_charge(Instant::now(), OwnerTurnCharge::opaque_move())
            .is_err()
        {
            break;
        }
        let Some(key) = state.request_deadlines.pop_due(now, 1).into_iter().next() else {
            break;
        };
        if let Some(entry) = state.pending_requests.get_mut(&key.waiter_id()) {
            entry.deadline_key = None;
        }
        mark_request_ready(state, key.waiter_id(), ReadyClass::Deadline, READY_DEADLINE);
    }
}

fn retire(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    mut entry: PendingControlRequest,
    reason: &str,
) {
    if let Some(key) = entry.ready_key.take() {
        state.request_ready.remove(key);
    }
    state.request_deadlines.retire(entry.waiter_id);
    state.host_completions.remove(&entry.waiter_id);
    state.document_waiters.remove(&entry.waiter_id);
    state.host_recovery_waiters.remove(&entry.waiter_id);
    state
        .blocked_session_type_roots
        .retain(|_, waiter_id| *waiter_id != entry.waiter_id);
    if let Some(runtime) = daemon.runtime() {
        runtime.retire_owner_core_waiter(entry.waiter_id);
    }
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

fn flag_past_deadline(state: &mut DaemonControlState, entry: &mut PendingControlRequest) {
    if entry.past_deadline {
        return;
    }
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

/// Retire every retirable pending request `client` left behind.
pub(crate) fn retire_abandoned_requests(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    client: &str,
) {
    let waiter_ids = state
        .pending_requests
        .iter()
        .filter_map(|(waiter_id, entry)| {
            (!entry.must_finish && entry.client.as_deref() == Some(client)).then_some(*waiter_id)
        })
        .collect::<Vec<_>>();
    for waiter_id in waiter_ids {
        if let Some(entry) = state.pending_requests.remove(&waiter_id) {
            retire(daemon, state, entry, "client_left");
        }
    }
}

/// Poll every pending request once. Finished requests are answered through
/// `finish` in acceptance order. Returns `true` when a `shutdown` response
/// was sent, matching the synchronous handler's return contract.
pub(crate) fn poll_ready_requests(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    _now: Instant,
    budget: &mut OwnerTurnBudget,
    mut finish: impl FnMut(
        &mut HubDaemon,
        &mut DaemonControlState,
        PendingControlRequest,
        ControlReply,
    ) -> bool,
) -> bool {
    let mut shutdown = false;
    while !shutdown
        && budget
            .try_charge(Instant::now(), OwnerTurnCharge::opaque_move())
            .is_ok()
    {
        // One host continuation may release the document and mark one parked
        // waiter ready. That bounded handoff belongs to this charged item. If
        // the charge fails, the continuation does not run and the waiter stays
        // parked in `document_waiters`.
        let Some(item) = state.request_ready.pop_next() else {
            break;
        };
        let waiter_id = item.key().waiter_id();
        let Some(mut entry) = state.pending_requests.remove(&waiter_id) else {
            continue;
        };
        entry.ready_key = None;
        if !entry.must_finish && entry.reply_tx.is_closed() {
            retire(daemon, state, entry, "reply_closed");
            continue;
        }
        let reasons = item.reasons();
        let has_completion = reasons.contains(READY_INITIAL)
            || reasons.contains(READY_CORE_COMPLETION)
            || reasons.contains(READY_PLUGIN_COMPLETION)
            || reasons.contains(READY_HOST_COMPLETION);
        if has_completion {
            state.current_waiter_id = Some(waiter_id);
            let poll = (entry.continuation)(daemon, state);
            state.current_waiter_id = None;
            match poll {
                ControlPoll::Pending => {}
                ControlPoll::Ready(response) => {
                    if reasons.contains(READY_DEADLINE) && entry.must_finish {
                        flag_past_deadline(state, &mut entry);
                    }
                    state.request_deadlines.retire(waiter_id);
                    state.host_completions.remove(&waiter_id);
                    if let Some(runtime) = daemon.runtime() {
                        runtime.retire_owner_core_waiter(waiter_id);
                    }
                    shutdown = finish(daemon, state, entry, ControlReply::plain(response));
                    continue;
                }
                ControlPoll::ReadyRetained(response) => {
                    if reasons.contains(READY_DEADLINE) && entry.must_finish {
                        flag_past_deadline(state, &mut entry);
                    }
                    state.request_deadlines.retire(waiter_id);
                    state.host_completions.remove(&waiter_id);
                    if let Some(runtime) = daemon.runtime() {
                        runtime.retire_owner_core_waiter(waiter_id);
                    }
                    shutdown = finish(daemon, state, entry, ControlReply::retained(response));
                    continue;
                }
                ControlPoll::ReadyHost(response, charge) => {
                    if reasons.contains(READY_DEADLINE) && entry.must_finish {
                        flag_past_deadline(state, &mut entry);
                    }
                    state.request_deadlines.retire(waiter_id);
                    state.host_completions.remove(&waiter_id);
                    if let Some(runtime) = daemon.runtime() {
                        runtime.retire_owner_core_waiter(waiter_id);
                    }
                    shutdown = finish(daemon, state, entry, ControlReply::host(response, charge));
                    continue;
                }
            }
        }
        if reasons.contains(READY_DEADLINE) {
            if entry.must_finish {
                flag_past_deadline(state, &mut entry);
            } else {
                retire(daemon, state, entry, "deadline");
                continue;
            }
        }
        state.pending_requests.insert(waiter_id, entry);
    }
    shutdown
}

#[cfg(test)]
pub(crate) fn poll_pending_requests(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    now: Instant,
    finish: impl FnMut(
        &mut HubDaemon,
        &mut DaemonControlState,
        PendingControlRequest,
        ControlReply,
    ) -> bool,
) -> bool {
    let waiter_ids = state.pending_requests.keys().copied().collect::<Vec<_>>();
    for waiter_id in waiter_ids {
        let entry = &state.pending_requests[&waiter_id];
        let expired = now.saturating_duration_since(entry.accepted_at)
            >= crate::daemon::owner_budget::RETAINED_OPERATION_DEADLINE;
        let class = entry.ready_class;
        mark_request_ready(state, waiter_id, class, READY_INITIAL);
        if expired {
            mark_request_ready(state, waiter_id, ReadyClass::Deadline, READY_DEADLINE);
        }
    }
    let mut budget = OwnerTurnBudget::new(Instant::now());
    poll_ready_requests(daemon, state, now, &mut budget, finish)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use botster_hub_client::DaemonResponseKind;

    fn test_daemon(label: &str) -> (HubDaemon, PathBuf) {
        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        let directory = PathBuf::from("target")
            .join("botster-hub-test-data")
            .join("owner-ready")
            .join(format!(
                "{label}-{unique}-{}",
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "owner-ready-test".to_string(),
                display_name: "Owner Ready Test".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build owner ready test config");
        (
            HubDaemon::start(config).expect("start owner ready test daemon"),
            directory,
        )
    }

    fn run_coalesced_completion_deadline(first_class: ReadyClass, must_finish: bool) {
        let (mut daemon, directory) = test_daemon(if must_finish {
            "must-finish"
        } else {
            "retirable"
        });
        let mut state = DaemonControlState::default();
        let waiter_id = WaiterId(1);
        let permit = state.budget.reserve().expect("reserve owner permit");
        let (reply_tx, _reply_rx) = crate::daemon::control::message::control_reply_channel();
        let now = Instant::now();
        let arm = state
            .request_deadlines
            .arm(waiter_id, now + Duration::from_secs(1), now)
            .expect("arm request deadline");
        state.pending_requests.insert(
            waiter_id,
            PendingControlRequest {
                waiter_id,
                ready_class: ReadyClass::CoreCompletion,
                ready_key: None,
                deadline_key: Some(arm.key()),
                last_core_phase: 0,
                last_host_phase: 0,
                request: DaemonRequest::Status,
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client: None,
                permit: Some(permit),
                accepted_at: now,
                must_finish,
                past_deadline: false,
                continuation: Box::new(|_, _| {
                    ControlPoll::Ready(Ok(crate::client_api_dto::response::daemon_response_base(
                        DaemonResponseKind::Status,
                    )))
                }),
                retire: None,
            },
        );
        let second_class = if first_class == ReadyClass::CoreCompletion {
            ReadyClass::Deadline
        } else {
            ReadyClass::CoreCompletion
        };
        let first_reason = if first_class == ReadyClass::CoreCompletion {
            READY_CORE_COMPLETION
        } else {
            READY_DEADLINE
        };
        let second_reason = if second_class == ReadyClass::CoreCompletion {
            READY_CORE_COMPLETION
        } else {
            READY_DEADLINE
        };
        assert!(mark_request_ready(
            &mut state,
            waiter_id,
            first_class,
            first_reason,
        ));
        assert!(mark_request_ready(
            &mut state,
            waiter_id,
            second_class,
            second_reason,
        ));

        let mut finished = 0;
        let mut budget = OwnerTurnBudget::new(Instant::now());
        assert!(!poll_ready_requests(
            &mut daemon,
            &mut state,
            Instant::now(),
            &mut budget,
            |_, state, mut entry, _| {
                finished += 1;
                state
                    .budget
                    .release(entry.permit.take().expect("release owner permit once"));
                false
            },
        ));

        assert_eq!(finished, 1);
        assert_eq!(state.budget.outstanding(), 0);
        assert!(state.pending_requests.is_empty());
        assert!(state.request_ready.is_empty());
        assert!(state.request_deadlines.is_empty());
        assert_eq!(
            state.budget.counters.requests_past_deadline,
            u64::from(must_finish),
        );
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove owner ready test directory");
    }

    #[test]
    fn completion_then_deadline_finishes_retirable_waiter_once() {
        run_coalesced_completion_deadline(ReadyClass::CoreCompletion, false);
    }

    #[test]
    fn deadline_then_completion_finishes_retirable_waiter_once() {
        run_coalesced_completion_deadline(ReadyClass::Deadline, false);
    }

    #[test]
    fn completion_then_deadline_flags_and_finishes_must_finish_waiter_once() {
        run_coalesced_completion_deadline(ReadyClass::CoreCompletion, true);
    }

    #[test]
    fn deadline_then_completion_flags_and_finishes_must_finish_waiter_once() {
        run_coalesced_completion_deadline(ReadyClass::Deadline, true);
    }

    #[test]
    fn exhausted_budget_preserves_a_waiter_parked_by_a_host_continuation() {
        let (mut daemon, directory) = test_daemon("parked-document-waiter");
        let mut state = DaemonControlState::default();
        let host_waiter = WaiterId(1);
        let parked_waiter = WaiterId(2);
        let permit = state.budget.reserve().expect("reserve owner permit");
        let (reply_tx, _reply_rx) = crate::daemon::control::message::control_reply_channel();
        let now = Instant::now();
        state.document_waiters.insert(parked_waiter);
        state.pending_requests.insert(
            host_waiter,
            PendingControlRequest {
                waiter_id: host_waiter,
                ready_class: ReadyClass::HostCompletion,
                ready_key: None,
                deadline_key: None,
                last_core_phase: 0,
                last_host_phase: 0,
                request: DaemonRequest::Status,
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client: None,
                permit: Some(permit),
                accepted_at: now,
                must_finish: true,
                past_deadline: false,
                continuation: Box::new(|_, state| {
                    state.document_waiters.pop_first();
                    ControlPoll::Pending
                }),
                retire: None,
            },
        );
        assert!(mark_request_ready(
            &mut state,
            host_waiter,
            ReadyClass::HostCompletion,
            READY_HOST_COMPLETION,
        ));
        let mut budget = OwnerTurnBudget::new(now);
        budget
            .try_charge(
                now,
                OwnerTurnCharge::inspection(
                    crate::daemon::owner_turn::OWNER_TURN_INSPECTED_BYTE_LIMIT,
                ),
            )
            .expect("fill the exact inspected-byte budget");

        assert!(!poll_ready_requests(
            &mut daemon,
            &mut state,
            now,
            &mut budget,
            |_, _, _, _| panic!("an exhausted turn cannot finish the request"),
        ));

        assert_eq!(
            state.document_waiters,
            [parked_waiter].into_iter().collect()
        );
        assert!(state.pending_requests.contains_key(&host_waiter));
        assert!(!state.request_ready.is_empty());
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove owner ready test directory");
    }
}
