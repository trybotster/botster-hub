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
use crate::daemon::control::reply::ControlReply;
use crate::daemon::error::DaemonTransportResult;
use crate::daemon::owner_budget::OwnerPermit;
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::{DeadlineKey, ReadyClass, ReadyKey, ReadyReasons};
use crate::daemon::owner_turn::{OwnerTurnBudget, OwnerTurnCharge};
use crate::owner_identity::{OwnerWorkIdentity, WaiterId};
use crate::subscription::attach_routes::{AttachedSubscription, AttachedSubscriptionChange};

pub(crate) const READY_INITIAL: ReadyReasons = ReadyReasons::from_bits(1 << 0);
pub(crate) const READY_CORE_COMPLETION: ReadyReasons = ReadyReasons::from_bits(1 << 1);
pub(crate) const READY_PLUGIN_COMPLETION: ReadyReasons = ReadyReasons::from_bits(1 << 2);
pub(crate) const READY_HOST_COMPLETION: ReadyReasons = ReadyReasons::from_bits(1 << 3);
pub(crate) const READY_DEADLINE: ReadyReasons = ReadyReasons::from_bits(1 << 4);
pub(crate) const READY_BACKGROUND: ReadyReasons = ReadyReasons::from_bits(1 << 5);

/// Outcome of one continuation poll.
pub(crate) enum ControlPoll {
    /// An internal callback has completed its own response delivery.
    FinishedInternal,
    DeliverStatusResponse(
        crate::status_response::PreparedStatusResponse,
        crate::host_executor::HostWorkPermit,
        u64,
    ),
    StatusResponseDelivered {
        shutdown: bool,
        received: bool,
    },
    StatusResponseRefused {
        shutdown: bool,
    },
    /// The request waits for a completion or another explicit wake.
    Pending,
    /// This request made partial progress and can continue through the ready queue.
    Again,
    /// The response is complete.
    Ready(DaemonTransportResult<DaemonResponse>),
    /// Transfer plugin shaping and delivery to the existing host executor.
    PreparePluginResponse(
        crate::plugin_response::PluginResponseInput,
        crate::host_executor::HostWorkPermit,
    ),
    SubmitPluginHost(crate::host_executor::HostSubmissionFailure),
    /// The response owns a host prepared-byte charge through transport framing.
    ReadyHost(
        DaemonTransportResult<DaemonResponse>,
        crate::host_executor::HostPreparedCharge,
    ),
}

/// Retained Host work has a typed owner so terminal disposal can extract its original permit.
pub(crate) enum ControlContinuation {
    Coordination(
        Box<super::coordination::CoordinationContinuation>,
        #[allow(dead_code)] // callback charge retained until the continuation drops
        Option<crate::lua_memory::LuaCallbackCharge>,
    ),
    Callback(Box<dyn FnMut(&mut HubDaemon, &mut DaemonControlState) -> ControlPoll + Send>),
    HostMutation(Box<super::host_work::HostMutationContinuation>),
    Status(Box<super::status::StatusContinuation>),
    ManagedSpawn(Box<super::managed_git::ManagedSpawnOperation>),
    Terminal(
        Box<TerminalContinuation>,
        #[allow(dead_code)] // disposal lease retained until the terminal row drops
        Option<crate::lua_memory::LuaCallbackStorageLease>,
    ),
}

pub(crate) struct TerminalContinuation {
    original: ControlContinuation,
    job: crate::host_disposal::Job,
}

#[allow(dead_code)] // owner fields drop with the terminal payload
struct TerminalOwnerPayload {
    completion: OwnerRequestCompletion,
    reply: ControlReplySender,
    response_delivery: Option<mpsc::Receiver<()>>,
    grant_id: Option<String>,
    client: Option<String>,
    retire: Option<RetireHook>,
}

pub(crate) fn terminal_storage_bytes(payload: usize) -> Option<usize> {
    payload
        .checked_add(std::mem::size_of::<(Box<dyn Send>, TerminalOwnerPayload)>())?
        .checked_add(crate::host_disposal::boxed_payload_storage_bytes()?)?
        .checked_add(std::mem::size_of::<TerminalContinuation>())
}

impl ControlContinuation {
    pub(crate) fn callback(
        callback: impl FnMut(&mut HubDaemon, &mut DaemonControlState) -> ControlPoll + Send + 'static,
    ) -> Self {
        Self::Callback(Box::new(callback))
    }

    pub(crate) fn poll(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        match self {
            Self::Coordination(work, _) => work.poll(daemon, state),
            Self::Callback(callback) => callback(daemon, state),
            Self::HostMutation(work) => work.poll(daemon, state),
            Self::Status(work) => work.poll(daemon, state),
            Self::ManagedSpawn(work) => work.poll(daemon, state),
            Self::Terminal(..) => panic!("terminal requests cannot resume normal execution"),
        }
    }

    /// Keep the original request row until Host destroys its payload.
    fn take_terminal_parts(
        &mut self,
        runtime: &crate::HubRuntime,
        identity: crate::host_executor::HostJobIdentity,
        completion: &mut Option<crate::host_executor::HostCompletion>,
    ) -> Option<crate::host_disposal::Parts> {
        match self {
            Self::Coordination(work, _) => work.take_terminal_parts(runtime, identity, completion),
            Self::HostMutation(work) => work.take_terminal_parts(identity, completion),
            Self::Status(work) => work.take_terminal_parts(identity, completion),
            Self::ManagedSpawn(work) => work.take_terminal_parts(identity, completion),
            Self::Callback(_) | Self::Terminal(..) => None,
        }
    }

    fn begin_terminal(&mut self, parts: crate::host_disposal::Parts) {
        let storage = parts.storage.clone();
        let mut original = std::mem::replace(self, Self::callback(|_, _| ControlPoll::Pending));
        let parts = if matches!(original, Self::Callback(_)) {
            let Self::Callback(callback) =
                std::mem::replace(&mut original, Self::callback(|_, _| ControlPoll::Pending))
            else {
                unreachable!()
            };
            parts.with_payload(callback)
        } else {
            parts
        };
        *self = Self::Terminal(
            Box::new(TerminalContinuation {
                original,
                job: crate::host_disposal::Job::new(parts),
            }),
            storage,
        );
    }

    pub(crate) fn poll_terminal(&mut self, runtime: &crate::HubRuntime) -> bool {
        let Self::Terminal(terminal, _) = self else {
            return false;
        };
        match terminal.job.poll() {
            crate::host_disposal::Poll::Disposed(permit) => {
                if let Self::HostMutation(work) = &mut terminal.original {
                    assert!(
                        work.retire_terminal(runtime),
                        "disposal precedes causal retirement"
                    );
                }
                drop(permit);
                true
            }
            crate::host_disposal::Poll::Retired => true,
            crate::host_disposal::Poll::Pending
            | crate::host_disposal::Poll::PartialDestruction
            | crate::host_disposal::Poll::SharedCleanupFault => false,
        }
    }
}

/// The terminal driver calls this only after it seals normal control ingress.
/// Each request keeps its Owner permit until Host reports disposal.
pub(crate) fn dispose_terminal_requests(
    runtime: &crate::HubRuntime,
    state: &mut DaemonControlState,
) {
    state.pending_requests.retain(|waiter_id, entry| {
        let mut completion = state.host_completions.remove(waiter_id);
        let identity = crate::host_executor::HostJobIdentity {
            waiter_id: *waiter_id,
            phase: entry.last_host_phase,
        };
        let parts = if matches!(entry.continuation, ControlContinuation::Callback(_)) {
            if state.plugin_controls.owns_waiter(*waiter_id) {
                state.plugin_controls.take_terminal_parts(
                    *waiter_id,
                    &mut completion,
                    runtime.host_executor(),
                )
            } else if let Some(completion) = completion.take() {
                let (identity, result, permit) = completion.into_parts();
                Some(crate::host_disposal::Parts {
                    storage: None,
                    identity,
                    permit,
                    payload: Box::new(result),
                    model: None,
                })
            } else {
                runtime
                    .host_executor()
                    .try_reserve()
                    .map(|permit| crate::host_disposal::Parts {
                        storage: None,
                        identity,
                        permit,
                        payload: Box::new(()),
                        model: None,
                    })
            }
        } else {
            entry
                .continuation
                .take_terminal_parts(runtime, identity, &mut completion)
        };
        if let Some(parts) = parts {
            let parts = parts.with_payload(TerminalOwnerPayload {
                completion: std::mem::take(&mut entry.completion),
                reply: entry.reply_tx.take(),
                response_delivery: entry.response_delivery_rx.take(),
                grant_id: entry.grant_id.take(),
                client: entry.client.take(),
                retire: entry.retire.take(),
            });
            entry.continuation.begin_terminal(parts);
        }
        if let Some(completion) = completion {
            state.host_completions.insert(*waiter_id, completion);
        }
        if !entry.continuation.poll_terminal(runtime) {
            return true;
        }
        state.coordination_capacity_waiters.remove(waiter_id);
        // Destroy the continuation before another request can use this Owner slot.
        drop(std::mem::replace(
            &mut entry.continuation,
            ControlContinuation::callback(|_, _| ControlPoll::Pending),
        ));
        drop(entry.core_retirement.take());
        if let Some(permit) = entry.permit.take() {
            state.budget.release(permit);
        }
        false
    });
}

/// Retirement for a request that owns deferred work. The hook receives the
/// entry permit. It must cancel, release, or transfer the work to another
/// bounded owner such as the plugin worker's executor and completion pools.
pub(crate) type RetireHook =
    Box<dyn FnOnce(&mut HubDaemon, &mut DaemonControlState, WaiterId, OwnerPermit) + Send>;

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
            continuation: crate::daemon::control::pending::ControlContinuation::callback(
                continuation,
            ),
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
            continuation: crate::daemon::control::pending::ControlContinuation::callback(
                continuation,
            ),
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
        retire: impl FnOnce(&mut HubDaemon, &mut DaemonControlState, WaiterId, OwnerPermit)
        + Send
        + 'static,
    ) -> Self {
        Self::Pending(PendingStep {
            continuation: crate::daemon::control::pending::ControlContinuation::callback(
                continuation,
            ),
            retire: Some(Box::new(retire)),
            ready_class: ReadyClass::CoreCompletion,
        })
    }

    pub(crate) fn pending_retirable_in(
        ready_class: ReadyClass,
        continuation: impl FnMut(&mut HubDaemon, &mut DaemonControlState) -> ControlPoll
        + Send
        + 'static,
        retire: impl FnOnce(&mut HubDaemon, &mut DaemonControlState, WaiterId, OwnerPermit)
        + Send
        + 'static,
    ) -> Self {
        Self::Pending(PendingStep {
            continuation: crate::daemon::control::pending::ControlContinuation::callback(
                continuation,
            ),
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
    pub(crate) completion: OwnerRequestCompletion,
    pub(crate) reply_tx: ControlReplySender,
    pub(crate) response_delivery_rx: Option<mpsc::Receiver<()>>,
    pub(crate) grant_id: Option<String>,
    pub(crate) client: Option<String>,
    pub(crate) core_retirement: Option<crate::data_plane::driver::CoreWaiterRetirement>,
    pub(crate) permit: Option<OwnerPermit>,
    pub(crate) must_finish: bool,
    pub(crate) past_deadline: bool,
    pub(crate) continuation: ControlContinuation,
    pub(crate) retire: Option<RetireHook>,
}

#[derive(Debug, Default)]
pub(crate) struct OwnerRequestCompletion {
    kind: OwnerRequestKind,
    route_change: Option<AttachedSubscriptionChange>,
}

#[derive(Debug, Default)]
enum OwnerRequestKind {
    #[default]
    Other,
    Spawn {
        session_id: String,
    },
    SpawnSessionType,
    Attach,
    Detach,
    ShutdownSession {
        session_id: String,
    },
    RemoveSession,
    PluginSurfaceAction,
}

impl OwnerRequestCompletion {
    pub(crate) fn from_request(request: &DaemonRequest) -> Self {
        let (kind, route_change) = match request {
            DaemonRequest::Spawn { session_id, .. } => (
                OwnerRequestKind::Spawn {
                    session_id: session_id.clone(),
                },
                None,
            ),
            DaemonRequest::SpawnSessionType { .. } => (OwnerRequestKind::SpawnSessionType, None),
            DaemonRequest::Attach {
                session_id,
                subscription_id,
            } => (
                OwnerRequestKind::Attach,
                Some(AttachedSubscriptionChange::Attach(AttachedSubscription {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                })),
            ),
            DaemonRequest::Detach {
                session_id,
                subscription_id,
            } => (
                OwnerRequestKind::Detach,
                Some(AttachedSubscriptionChange::Detach(AttachedSubscription {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                })),
            ),
            DaemonRequest::ShutdownSession { session_id } => (
                OwnerRequestKind::ShutdownSession {
                    session_id: session_id.clone(),
                },
                None,
            ),
            DaemonRequest::RemoveSession { .. } => (OwnerRequestKind::RemoveSession, None),
            DaemonRequest::PluginSurfaceAction { .. } => {
                (OwnerRequestKind::PluginSurfaceAction, None)
            }
            _ => (OwnerRequestKind::Other, None),
        };
        Self { kind, route_change }
    }

    pub(crate) fn route_change(&self) -> Option<AttachedSubscriptionChange> {
        self.route_change.clone()
    }

    pub(crate) fn is_detach(&self) -> bool {
        matches!(self.kind, OwnerRequestKind::Detach)
    }

    pub(crate) fn shutdown_session_id(&self) -> Option<&str> {
        match &self.kind {
            OwnerRequestKind::ShutdownSession { session_id } => Some(session_id),
            _ => None,
        }
    }

    pub(crate) fn spawned_session_id(&self) -> Option<&str> {
        match &self.kind {
            OwnerRequestKind::Spawn { session_id } => Some(session_id),
            _ => None,
        }
    }

    pub(crate) fn reconciles_after_success(&self) -> bool {
        matches!(
            self.kind,
            OwnerRequestKind::Spawn { .. }
                | OwnerRequestKind::Attach
                | OwnerRequestKind::ShutdownSession { .. }
                | OwnerRequestKind::RemoveSession
        )
    }

    pub(crate) fn marks_pump(&self, succeeded: bool) -> bool {
        match self.kind {
            OwnerRequestKind::Spawn { .. }
            | OwnerRequestKind::SpawnSessionType
            | OwnerRequestKind::Attach => succeeded,
            OwnerRequestKind::Detach
            | OwnerRequestKind::ShutdownSession { .. }
            | OwnerRequestKind::RemoveSession => true,
            _ => false,
        }
    }

    pub(crate) fn is_plugin_surface_action(&self) -> bool {
        matches!(self.kind, OwnerRequestKind::PluginSurfaceAction)
    }
}

/// Requests whose Core work has effects that require an owner continuation,
/// or that consume state, must finish. Plugin requests retain their row until
/// a host worker completes delivery or discards the cancelled response.
pub(crate) fn request_must_finish(request: &DaemonRequest) -> bool {
    !matches!(
        request,
        DaemonRequest::ListSessions { .. }
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

pub(crate) fn mark_owner_ready(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    class: ReadyClass,
    reasons: ReadyReasons,
) -> bool {
    if !state.pending_requests.contains_key(&waiter_id) {
        return false;
    }
    let Ok(key) = state.owner_ready.mark(waiter_id, class, reasons) else {
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
            if !crate::daemon::owner_budget::absorb_obligation_core_completion(state, identity) {
                state.absorb_background_core_completion(identity);
            }
            continue;
        };
        let Some(expected) = entry.last_core_phase.checked_add(1) else {
            continue;
        };
        if identity.phase != expected {
            continue;
        }
        let coordination = matches!(entry.continuation, ControlContinuation::Coordination(..));
        entry.last_core_phase = identity.phase;
        if !mark_owner_ready(
            state,
            identity.waiter_id,
            ReadyClass::CoreCompletion,
            READY_CORE_COMPLETION,
        ) && coordination
        {
            state.coordination_fault =
                Some(super::coordination::CoordinationFault::SchedulerExhausted);
        }
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
    let coordination = matches!(entry.continuation, ControlContinuation::Coordination(..));
    entry.last_host_phase = identity.phase;
    state
        .host_completions
        .insert(identity.waiter_id, completion);
    if !mark_owner_ready(
        state,
        identity.waiter_id,
        ReadyClass::HostCompletion,
        READY_HOST_COMPLETION,
    ) && coordination
    {
        state.coordination_fault = Some(super::coordination::CoordinationFault::SchedulerExhausted);
    }
}

pub(crate) fn mark_due_owner_deadlines(
    state: &mut DaemonControlState,
    now: Instant,
    budget: &mut OwnerTurnBudget,
) {
    if budget
        .try_charge(Instant::now(), OwnerTurnCharge::opaque_move())
        .is_err()
    {
        return;
    }
    let Some(key) = state.deadlines.pop_due(now, 1).into_iter().next() else {
        return;
    };
    if let Some(entry) = state.pending_requests.get_mut(&key.waiter_id()) {
        entry.deadline_key = None;
        mark_owner_ready(state, key.waiter_id(), ReadyClass::Deadline, READY_DEADLINE);
        return;
    }
    if crate::daemon::owner_loop::mark_package_entity_resync_deadline_ready(state, key.waiter_id())
    {
        return;
    }
    if state.plugin_entities.clear_deadline(key.waiter_id()) {
        crate::daemon::control::entities::mark_plugin_entity_ready(
            state,
            key.waiter_id(),
            ReadyClass::Deadline,
            READY_DEADLINE,
        );
        return;
    }
    if state.budget.clear_obligation_deadline(key.waiter_id()) {
        crate::daemon::owner_budget::mark_obligation_ready(state, key.waiter_id(), READY_DEADLINE);
        return;
    }
    crate::daemon::owner_loop::mark_reservation_deadline_ready(state, key.waiter_id());
}

fn retire(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    mut entry: PendingControlRequest,
    reason: &str,
) {
    if let Some(key) = entry.ready_key.take() {
        state.owner_ready.remove(key);
    }
    state.deadlines.retire(entry.waiter_id);
    state.host_completions.remove(&entry.waiter_id);
    state.document_waiters.remove(&entry.waiter_id);
    state
        .blocked_session_type_roots
        .retain(|_, waiter_id| *waiter_id != entry.waiter_id);
    drop(entry.core_retirement.take());
    match (entry.permit.take(), entry.retire.take()) {
        // The request owns Core work: the hook keeps the permit in an
        // obligation that cancels or releases it.
        (Some(permit), Some(hook)) => hook(daemon, state, entry.waiter_id, permit),
        (Some(permit), None) => {
            if let Some(runtime) = daemon.runtime() {
                runtime.retire_owner_core_waiter(entry.waiter_id);
            }
            state.budget.release(permit);
        }
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
    wake_shutdown_waiter(state);
}

pub(crate) fn wake_shutdown_waiter(state: &mut DaemonControlState) {
    if let Some(waiter_id) = state.shutdown_waiter {
        mark_owner_ready(
            state,
            waiter_id,
            ReadyClass::HostCompletion,
            READY_HOST_COMPLETION,
        );
    }
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
            if entry.client.as_deref() != Some(client) {
                return None;
            }
            state.plugin_controls.cancel_reply(*waiter_id);
            (!entry.must_finish).then_some(*waiter_id)
        })
        .collect::<Vec<_>>();
    for waiter_id in waiter_ids {
        if let Some(entry) = state.pending_requests.remove(&waiter_id) {
            retire(daemon, state, entry, "client_left");
        }
    }
}

/// Apply one ready item for one retained control request.
pub(crate) fn poll_ready_request_item(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    item: crate::daemon::owner_schedule::ReadyItem,
    finish: &mut impl FnMut(
        &mut HubDaemon,
        &mut DaemonControlState,
        PendingControlRequest,
        ControlReply,
    ) -> bool,
) -> bool {
    let waiter_id = item.key().waiter_id();
    let Some(mut entry) = state.pending_requests.remove(&waiter_id) else {
        return false;
    };
    entry.ready_key = None;
    if !entry.must_finish && entry.reply_tx.is_receiver_closed() {
        retire(daemon, state, entry, "reply_closed");
        return false;
    }
    let reasons = item.reasons();
    if reasons.contains(READY_DEADLINE) {
        state.plugin_controls.cancel_reply(waiter_id);
    }
    let has_completion = reasons.contains(READY_INITIAL)
        || reasons.contains(READY_CORE_COMPLETION)
        || reasons.contains(READY_PLUGIN_COMPLETION)
        || reasons.contains(READY_HOST_COMPLETION);
    let mut again = false;
    if has_completion {
        state.current_waiter_id = Some(waiter_id);
        let poll = entry.continuation.poll(daemon, state);
        state.current_waiter_id = None;
        let reply = match poll {
            ControlPoll::FinishedInternal => {
                debug_assert!(entry.retire.is_none());
                state.coordination_capacity_waiters.remove(&waiter_id);
                state.deadlines.retire(waiter_id);
                state.host_completions.remove(&waiter_id);
                let retirement = entry.core_retirement.take();
                let permit = entry.permit.take();
                drop(entry);
                drop(retirement);
                if let Some(permit) = permit {
                    state.budget.release(permit);
                }
                wake_shutdown_waiter(state);
                return false;
            }
            ControlPoll::StatusResponseRefused { shutdown } => {
                if reasons.contains(READY_DEADLINE) && entry.must_finish {
                    flag_past_deadline(state, &mut entry);
                }
                state.deadlines.retire(waiter_id);
                if let Some(runtime) = daemon.runtime() {
                    runtime.retire_owner_core_waiter(waiter_id);
                }
                let received = entry
                    .reply_tx
                    .take()
                    .send(Err(
                        crate::daemon::error::DaemonTransportError::ControlThreadStopped,
                    ))
                    .is_ok();
                let should_stop =
                    super::request::finish_status_delivery(state, entry, shutdown, received);
                if !should_stop {
                    wake_shutdown_waiter(state);
                }
                return should_stop;
            }
            ControlPoll::DeliverStatusResponse(prepared, permit, phase) => {
                again = super::status::submit_delivery(
                    daemon, state, &mut entry, prepared, permit, phase,
                );
                None
            }
            ControlPoll::StatusResponseDelivered { shutdown, received } => {
                if reasons.contains(READY_DEADLINE) && entry.must_finish {
                    flag_past_deadline(state, &mut entry);
                }
                state.deadlines.retire(waiter_id);
                if let Some(runtime) = daemon.runtime() {
                    runtime.retire_owner_core_waiter(waiter_id);
                }
                let should_stop =
                    super::request::finish_status_delivery(state, entry, shutdown, received);
                if !should_stop {
                    wake_shutdown_waiter(state);
                }
                return should_stop;
            }
            ControlPoll::Pending => None,
            ControlPoll::Again => {
                again = true;
                None
            }
            ControlPoll::Ready(response) => Some(ControlReply::plain(response)),
            ControlPoll::PreparePluginResponse(input, permit) => {
                crate::daemon::control::plugins::submit_response(
                    daemon, state, &mut entry, input, permit,
                );
                None
            }
            ControlPoll::SubmitPluginHost(job) => {
                crate::daemon::control::plugins::submit_host_job(
                    daemon,
                    state,
                    job.identity,
                    job.command,
                    job.permit,
                );
                None
            }
            ControlPoll::ReadyHost(response, charge) => Some(ControlReply::host(response, charge)),
        };
        if let Some(reply) = reply {
            if reasons.contains(READY_DEADLINE) && entry.must_finish {
                flag_past_deadline(state, &mut entry);
            }
            state.deadlines.retire(waiter_id);
            state.host_completions.remove(&waiter_id);
            if let Some(runtime) = daemon.runtime() {
                runtime.retire_owner_core_waiter(waiter_id);
            }
            let shutdown = finish(daemon, state, entry, reply);
            if !shutdown {
                wake_shutdown_waiter(state);
            }
            return shutdown;
        }
    }
    if reasons.contains(READY_DEADLINE) {
        if entry.must_finish {
            flag_past_deadline(state, &mut entry);
        } else if entry.reply_tx.is_transferred() {
            // A Host worker owns delivery. Keep the row until that worker
            // returns its completion and original capacity.
            flag_past_deadline(state, &mut entry);
        } else {
            retire(daemon, state, entry, "deadline");
            return false;
        }
    }
    state.pending_requests.insert(waiter_id, entry);
    if again {
        mark_owner_ready(
            state,
            waiter_id,
            ReadyClass::HostCompletion,
            READY_HOST_COMPLETION,
        );
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use botster_hub_client::DaemonResponseKind;

    #[test]
    fn terminal_status_rows_dispose_all_eight_original_slots_before_owner_retirement() {
        struct Gate(std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
        impl Gate {
            fn release(&self) {
                *self.0.0.lock().unwrap() = true;
                self.0.1.notify_all();
            }
        }
        impl Drop for Gate {
            fn drop(&mut self) {
                self.release();
            }
        }
        struct Probe {
            disposed: std::sync::mpsc::Sender<String>,
            entered: std::sync::mpsc::Sender<()>,
            gate: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
        }
        impl Drop for Probe {
            fn drop(&mut self) {
                let _ = self.entered.send(());
                let mut released = self.gate.0.lock().unwrap();
                while !*released {
                    released = self.gate.1.wait(released).unwrap();
                }
                self.disposed
                    .send(
                        std::thread::current()
                            .name()
                            .unwrap_or("unnamed")
                            .to_string(),
                    )
                    .unwrap();
            }
        }
        let (mut daemon, directory) = test_daemon("terminal-status-slots");
        let mut state = DaemonControlState::default();
        let transport = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (control_tx, _control_rx) = tokio::sync::mpsc::channel(32);
        let (disposed_tx, disposed_rx) = std::sync::mpsc::channel();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let gate = Gate(std::sync::Arc::new((
            std::sync::Mutex::new(false),
            std::sync::Condvar::new(),
        )));
        let mut replies = Vec::new();
        for _ in 0..crate::host_executor::HOST_OPERATION_CAPACITY {
            let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
            replies.push(reply_rx);
            assert!(!super::super::request::handle(
                &mut daemon,
                &mut state,
                transport.handle(),
                control_tx.clone(),
                crate::daemon::control::message::ControlMessage::Request {
                    request: Box::new(DaemonRequest::Status),
                    transport_request_id: None,
                    reply_tx,
                    response_delivery_rx: None,
                    grant_id: None,
                    client_id: None,
                    enqueued_at: Instant::now(),
                },
            ));
        }
        for entry in state.pending_requests.values_mut() {
            let probe = Probe {
                disposed: disposed_tx.clone(),
                entered: entered_tx.clone(),
                gate: gate.0.clone(),
            };
            entry.retire = Some(Box::new(move |_, _, _, _| drop(probe)));
            assert!(matches!(entry.continuation, ControlContinuation::Status(_)));
        }
        drop(disposed_tx);
        let runtime = daemon.runtime().unwrap();
        assert_eq!(
            runtime.host_executor().outstanding(),
            crate::host_executor::HOST_OPERATION_CAPACITY
        );
        assert!(runtime.host_executor().try_reserve().is_none());
        assert_eq!(
            state.budget.outstanding(),
            crate::host_executor::HOST_OPERATION_CAPACITY
        );
        dispose_terminal_requests(runtime, &mut state);
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a Host worker enters the destructor gate");
        dispose_terminal_requests(runtime, &mut state);
        assert_eq!(
            state.pending_requests.len(),
            crate::host_executor::HOST_OPERATION_CAPACITY
        );
        assert_eq!(
            state.budget.outstanding(),
            crate::host_executor::HOST_OPERATION_CAPACITY
        );
        assert_eq!(
            runtime.host_executor().outstanding(),
            crate::host_executor::HOST_OPERATION_CAPACITY
        );
        assert!(runtime.host_executor().try_reserve().is_none());
        assert!(disposed_rx.try_recv().is_err());
        gate.release();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !state.pending_requests.is_empty() || runtime.host_executor().prepared_bytes() != 0 {
            dispose_terminal_requests(runtime, &mut state);
            assert_eq!(state.budget.outstanding(), state.pending_requests.len());
            assert!(
                Instant::now() < deadline,
                "all original Host slots must dispose"
            );
            std::thread::yield_now();
        }
        assert_eq!(runtime.host_executor().outstanding(), 0);
        assert_eq!(runtime.host_executor().prepared_bytes(), 0);
        let threads = disposed_rx.try_iter().collect::<Vec<_>>();
        assert_eq!(threads.len(), crate::host_executor::HOST_OPERATION_CAPACITY);
        assert!(
            threads
                .iter()
                .all(|thread| thread.starts_with("botster-hub-host")),
            "{threads:?}"
        );
        assert_eq!(state.budget.outstanding(), 0);
        drop(replies);
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

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
            .deadlines
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
                completion: OwnerRequestCompletion::default(),
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client: None,
                core_retirement: None,
                permit: Some(permit),
                must_finish,
                past_deadline: false,
                continuation: crate::daemon::control::pending::ControlContinuation::callback(
                    |_, _| {
                        ControlPoll::Ready(Ok(
                            crate::client_api_dto::response::daemon_response_base(
                                DaemonResponseKind::Status,
                            ),
                        ))
                    },
                ),
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
        assert!(mark_owner_ready(
            &mut state,
            waiter_id,
            first_class,
            first_reason,
        ));
        assert!(mark_owner_ready(
            &mut state,
            waiter_id,
            second_class,
            second_reason,
        ));

        let mut finished = 0;
        let item = state.owner_ready.pop_next().expect("coalesced ready row");
        assert!(!poll_ready_request_item(
            &mut daemon,
            &mut state,
            item,
            &mut |_, state, mut entry, _| {
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
        assert!(state.owner_ready.is_empty());
        assert!(state.deadlines.is_empty());
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
                completion: OwnerRequestCompletion::default(),
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client: None,
                core_retirement: None,
                permit: Some(permit),
                must_finish: true,
                past_deadline: false,
                continuation: crate::daemon::control::pending::ControlContinuation::callback(
                    |_, state| {
                        state.document_waiters.pop_first();
                        ControlPoll::Pending
                    },
                ),
                retire: None,
            },
        );
        assert!(mark_owner_ready(
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

        assert!(
            budget
                .try_charge(now, OwnerTurnCharge::opaque_move())
                .is_err()
        );

        assert_eq!(
            state.document_waiters,
            [parked_waiter].into_iter().collect()
        );
        assert!(state.pending_requests.contains_key(&host_waiter));
        assert!(!state.owner_ready.is_empty());
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove owner ready test directory");
    }
}
