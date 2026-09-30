//! Cleanup obligations: owner-thread work that outlives the request or
//! connection that started it.
//!
//! An obligation is retained until its Core resources are released. One plain
//! bound, [`OWNER_BUDGET_CAPACITY`], counts outstanding obligations: at the
//! bound the owner refuses new admissions with a typed error instead of
//! letting a reconnect loop pile obligations up while Core is stuck. Nothing
//! is discarded at the bound. The other admission bounds live where the work
//! enters: the accept semaphore (connections), `MAX_OUTSTANDING_REQUESTS`
//! (requests per connection), and `MAX_ATTACH_ROUTES_PER_OWNER` (routes).
//!
//! An obligation keeps at most one Core ticket in flight. A refused admission
//! resubmits on the next owner turn; a lost driver ends the obligation.
//! Retained operations carry a deadline that the owner loop wakes for; past
//! the deadline an obligation is counted but kept, because releasing Core
//! resources is not optional.

use std::time::{Duration, Instant};

use botster_hub_client::MAX_OUTSTANDING_REQUESTS;

use crate::HubDaemon;
use crate::HubRuntime;
use crate::admission::budgets::DAEMON_MAX_CONNECTIONS;
use crate::daemon::owner_loop::DaemonControlState;
use crate::data_plane::driver::{CoreTicket, CoreTicketPoll};

/// The bound on outstanding cleanup obligations. It keeps the value the
/// retired permit ledger enforced (every connection and every request it may
/// have in flight), so no new number is introduced.
pub(crate) const OWNER_BUDGET_CAPACITY: usize =
    DAEMON_MAX_CONNECTIONS * (MAX_OUTSTANDING_REQUESTS + 1);

/// Deadline for a retained operation. Abandoned reads are retired at the
/// deadline; obligations and side-effecting requests are counted and kept.
pub(crate) const RETAINED_OPERATION_DEADLINE: Duration = Duration::from_secs(30);

/// Operator error code when the budget refuses a new request.
pub(crate) const OWNER_BUDGET_EXHAUSTED: &str = "owner_budget_exhausted";

/// Outcome of one obligation poll.
pub(crate) enum ObligationPoll {
    Pending,
    ReadyAgain,
    Done,
}

type ObligationFn = Box<
    dyn FnMut(
            &mut HubDaemon,
            &mut DaemonControlState,
            crate::owner_identity::WaiterId,
        ) -> ObligationPoll
        + Send,
>;

/// Owner work that must complete.
pub(crate) struct CleanupObligation {
    terminal: Option<crate::host_disposal::Job>,
    label: &'static str,
    ready_key: Option<crate::daemon::owner_schedule::ReadyKey>,
    deadline_key: Option<crate::daemon::owner_schedule::DeadlineKey>,
    last_core_phase: u64,
    past_deadline: bool,
    poll: ObligationFn,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct OwnerBudgetCounters {
    /// Admissions refused because the obligation bound was reached.
    pub refused: u64,
    /// Pending reads retired because their client left or the deadline passed.
    pub retired_abandoned: u64,
    /// Side-effecting requests still pending past the deadline (counted once).
    pub requests_past_deadline: u64,
    /// Obligations still pending past the deadline (counted once).
    pub obligations_past_deadline: u64,
}

pub(crate) struct OwnerBudget {
    bound: usize,
    /// An obligation finished since the last capacity notification.
    released: bool,
    /// Admitted WebRTC peers, by grant id, until peer cleanup.
    admitted_peers: std::collections::BTreeSet<String>,
    obligations: std::collections::BTreeMap<crate::owner_identity::WaiterId, CleanupObligation>,
    pub(crate) counters: OwnerBudgetCounters,
}

impl Default for OwnerBudget {
    fn default() -> Self {
        Self::with_bound(OWNER_BUDGET_CAPACITY)
    }
}

impl std::fmt::Debug for OwnerBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnerBudget")
            .field("bound", &self.bound)
            .field("admitted_peers", &self.admitted_peers.len())
            .field("obligations", &self.obligations.len())
            .field("counters", &self.counters)
            .finish()
    }
}

impl OwnerBudget {
    /// Shared Host cleanup finishes before terminal shutdown disposes connection obligations.
    pub(crate) fn dispose_terminal_obligations(
        &mut self,
        executor: &crate::host_executor::HostExecutor,
    ) -> bool {
        let mut after = None;
        loop {
            let next = match after {
                None => self.obligations.keys().next().copied(),
                Some(previous) => self
                    .obligations
                    .range((
                        std::ops::Bound::Excluded(previous),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
                    .map(|(key, _)| *key),
            };
            let Some(waiter) = next else {
                break;
            };
            after = Some(waiter);
            let obligation = self
                .obligations
                .get_mut(&waiter)
                .expect("terminal cleanup retains its obligation");
            if let Some(job) = obligation.terminal.as_mut() {
                if let crate::host_disposal::Poll::Disposed(permit) = job.poll() {
                    self.obligations
                        .remove(&waiter)
                        .expect("the obligation retires after disposal");
                    self.released = true;
                    drop(permit);
                }
            } else if let Some(permit) = executor.try_reserve() {
                let poll = std::mem::replace(
                    &mut obligation.poll,
                    Box::new(|_, _, _| ObligationPoll::Pending),
                );
                obligation.terminal = Some(crate::host_disposal::Job::new(
                    crate::host_disposal::Parts {
                        storage: None,
                        identity: crate::host_executor::HostJobIdentity::first(waiter),
                        permit,
                        payload: Box::new(poll),
                        model: None,
                    },
                ));
            }
        }
        self.obligations.is_empty()
    }

    pub(crate) fn with_bound(bound: usize) -> Self {
        Self {
            bound,
            released: false,
            admitted_peers: std::collections::BTreeSet::new(),
            obligations: std::collections::BTreeMap::new(),
            counters: OwnerBudgetCounters::default(),
        }
    }

    /// Outstanding cleanup obligations.
    #[cfg(any(test, feature = "plugin-test-kit"))]
    pub(crate) fn outstanding(&self) -> usize {
        self.obligations.len()
    }

    /// Whether the owner may admit new work: false at the obligation bound.
    /// A refusal is counted; the caller answers with its typed error.
    #[must_use]
    pub(crate) fn admits_work(&mut self) -> bool {
        if self.obligations.len() >= self.bound {
            self.counters.refused = self.counters.refused.saturating_add(1);
            return false;
        }
        true
    }

    pub(crate) fn take_capacity_notification(&mut self) -> bool {
        std::mem::take(&mut self.released)
    }

    /// Admit a WebRTC peer, held until peer cleanup. `false` means refused.
    #[must_use]
    pub(crate) fn admit_peer(&mut self, grant_id: &str) -> bool {
        if !self.admits_work() {
            return false;
        }
        self.admitted_peers.insert(grant_id.to_string());
        true
    }

    /// End one peer's admission. `true` when the peer was admitted.
    pub(crate) fn release_peer(&mut self, grant_id: &str) -> bool {
        self.admitted_peers.remove(grant_id)
    }

    /// Whether a peer is still admitted; attach admission requires it.
    pub(crate) fn peer_admitted(&self, grant_id: &str) -> bool {
        self.admitted_peers.contains(grant_id)
    }

    pub(crate) fn clear_obligation_deadline(
        &mut self,
        waiter_id: crate::owner_identity::WaiterId,
    ) -> bool {
        let Some(obligation) = self.obligations.get_mut(&waiter_id) else {
            return false;
        };
        obligation.deadline_key = None;
        true
    }
}

pub(crate) fn retain_owner_obligation(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
    label: &'static str,
    poll: impl FnMut(
        &mut HubDaemon,
        &mut DaemonControlState,
        crate::owner_identity::WaiterId,
    ) -> ObligationPoll
    + Send
    + 'static,
) {
    let now = Instant::now();
    let arm = state
        .deadlines
        .arm(waiter_id, now + RETAINED_OPERATION_DEADLINE, now)
        .expect("an initial obligation deadline always makes progress");
    state.budget.obligations.insert(
        waiter_id,
        CleanupObligation {
            terminal: None,
            label,
            ready_key: None,
            deadline_key: Some(arm.key()),
            last_core_phase: 0,
            past_deadline: false,
            poll: Box::new(poll),
        },
    );
    mark_obligation_ready(
        state,
        waiter_id,
        crate::daemon::control::pending::READY_INITIAL,
    );
}

pub(crate) fn allocate_and_retain_owner_obligation(
    state: &mut DaemonControlState,
    label: &'static str,
    poll: impl FnMut(
        &mut HubDaemon,
        &mut DaemonControlState,
        crate::owner_identity::WaiterId,
    ) -> ObligationPoll
    + Send
    + 'static,
) {
    let waiter_id = state
        .waiter_ids
        .next()
        .expect("an admitted owner permit must have an available waiter identifier");
    retain_owner_obligation(state, waiter_id, label, poll);
}

pub(crate) fn mark_obligation_ready(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
    reason: crate::daemon::owner_schedule::ReadyReasons,
) -> bool {
    if !state.budget.obligations.contains_key(&waiter_id) {
        return false;
    }
    let Ok(key) = state.owner_ready.mark(
        waiter_id,
        crate::daemon::owner_schedule::ReadyClass::Cleanup,
        reason,
    ) else {
        return false;
    };
    state
        .budget
        .obligations
        .get_mut(&waiter_id)
        .expect("a marked obligation must exist")
        .ready_key = Some(key);
    true
}

pub(crate) fn absorb_obligation_core_completion(
    state: &mut DaemonControlState,
    identity: crate::owner_identity::OwnerWorkIdentity,
) -> bool {
    let Some(obligation) = state.budget.obligations.get_mut(&identity.waiter_id) else {
        return false;
    };
    let Some(expected) = obligation.last_core_phase.checked_add(1) else {
        return true;
    };
    if identity.phase == expected {
        obligation.last_core_phase = identity.phase;
        mark_obligation_ready(
            state,
            identity.waiter_id,
            crate::daemon::control::pending::READY_CORE_COMPLETION,
        );
    }
    true
}

pub(crate) fn poll_owner_obligation_item(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    item: crate::daemon::owner_schedule::ReadyItem,
) -> bool {
    let waiter_id = item.key().waiter_id();
    let Some(mut obligation) = state.budget.obligations.remove(&waiter_id) else {
        return false;
    };
    obligation.ready_key = None;
    if let Some(runtime) = daemon.runtime() {
        runtime.reap_detached_core_operations();
    }
    if item
        .reasons()
        .contains(crate::daemon::control::pending::READY_DEADLINE)
        && !obligation.past_deadline
    {
        obligation.past_deadline = true;
        state.budget.counters.obligations_past_deadline = state
            .budget
            .counters
            .obligations_past_deadline
            .saturating_add(1);
        *state
            .lifecycle_counters
            .cleanup_by_reason
            .entry(format!("past_deadline:{}", obligation.label))
            .or_insert(0) += 1;
    }
    let result = (obligation.poll)(daemon, state, waiter_id);
    match result {
        ObligationPoll::Done => {
            state.deadlines.retire(waiter_id);
            if let Some(runtime) = daemon.runtime() {
                runtime.retire_owner_core_waiter(waiter_id);
            }
            state.budget.released = true;
        }
        ObligationPoll::Pending | ObligationPoll::ReadyAgain => {
            let ready_again = matches!(result, ObligationPoll::ReadyAgain);
            state.budget.obligations.insert(waiter_id, obligation);
            if ready_again {
                mark_obligation_ready(
                    state,
                    waiter_id,
                    crate::daemon::control::pending::READY_INITIAL,
                );
            }
        }
    }
    true
}

/// Result of driving one Core ticket slot.
pub(crate) enum CoreWorkPoll<T> {
    /// No answer yet, or the admission was refused and will be retried.
    Pending,
    /// Core refused admission. The caller must remain ready to retry.
    Retry,
    /// The driver stopped; no answer will come.
    Lost,
    Ready(T),
}

/// Drive one Core ticket slot: submit when empty, clear on a refused
/// admission so the next turn resubmits, and hand back the answer.
///
/// `submit` runs at most once per turn, so a caller holds exactly one
/// in-flight ticket per slot.
pub(crate) fn drive_core_slot<T: Send + 'static>(
    slot: &mut Option<CoreTicket<T>>,
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
    submit: impl FnOnce(
        &HubRuntime,
        &mut DaemonControlState,
        crate::owner_identity::WaiterId,
    ) -> CoreTicket<T>,
) -> CoreWorkPoll<T> {
    if slot.is_none() {
        let Some(runtime) = daemon.runtime() else {
            return CoreWorkPoll::Lost;
        };
        *slot = Some(submit(runtime, state, waiter_id));
    }
    match slot.as_mut().expect("slot filled above").poll() {
        CoreTicketPoll::Pending => CoreWorkPoll::Pending,
        CoreTicketPoll::Refused => {
            *slot = None;
            CoreWorkPoll::Retry
        }
        CoreTicketPoll::Lost => {
            *slot = None;
            CoreWorkPoll::Lost
        }
        CoreTicketPoll::Ready(value) => {
            *slot = None;
            CoreWorkPoll::Ready(value)
        }
    }
}

/// Fill the obligation bound in a test: hold `count` obligations that never finish.
#[cfg(test)]
pub(crate) fn hold_test_obligations(
    state: &mut DaemonControlState,
    count: usize,
) -> Vec<crate::owner_identity::WaiterId> {
    (0..count)
        .map(|_| {
            let waiter_id = state.waiter_ids.next().expect("test waiter identifier");
            retain_owner_obligation(state, waiter_id, "test_hold", |_, _, _| {
                ObligationPoll::Pending
            });
            waiter_id
        })
        .collect()
}

/// End one held test obligation the way a finished obligation ends.
#[cfg(test)]
pub(crate) fn finish_test_obligation(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
) {
    assert!(state.budget.obligations.remove(&waiter_id).is_some());
    state.deadlines.retire(waiter_id);
    state.budget.released = true;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bound_keeps_the_value_the_permit_ledger_enforced() {
        assert_eq!(OWNER_BUDGET_CAPACITY, 2112);
    }

    #[test]
    fn work_is_refused_at_the_obligation_bound_and_counted() {
        let mut state = DaemonControlState::default();
        state.budget = OwnerBudget::with_bound(2);
        assert!(state.budget.admits_work());
        let held = hold_test_obligations(&mut state, 2);
        assert_eq!(state.budget.outstanding(), 2);
        assert!(!state.budget.admits_work());
        assert_eq!(state.budget.counters.refused, 1);
        assert!(!state.budget.take_capacity_notification());
        finish_test_obligation(&mut state, held[0]);
        assert!(state.budget.take_capacity_notification());
        assert!(state.budget.admits_work());
        assert_eq!(state.budget.counters.refused, 1);
    }

    #[test]
    fn a_peer_is_refused_at_the_bound_and_ends_once() {
        let mut state = DaemonControlState::default();
        state.budget = OwnerBudget::with_bound(1);
        hold_test_obligations(&mut state, 1);
        assert!(!state.budget.admit_peer("grant"));
        assert!(!state.budget.peer_admitted("grant"));
        assert!(!state.budget.release_peer("grant"));
        let mut open = OwnerBudget::with_bound(1);
        assert!(open.admit_peer("grant"));
        assert!(open.peer_admitted("grant"));
        assert!(open.release_peer("grant"));
        assert!(!open.release_peer("grant"));
    }

    #[test]
    fn obligation_uses_the_shared_deadline_index() {
        let mut state = DaemonControlState::default();
        state.budget = OwnerBudget::with_bound(2);
        assert!(state.deadlines.next_deadline().is_none());
        retain_owner_obligation(
            &mut state,
            crate::owner_identity::WaiterId(1),
            "first",
            |_, _, _| ObligationPoll::Pending,
        );
        let deadline = state.deadlines.next_deadline().expect("deadline");
        assert!(deadline > Instant::now());
        assert!(deadline <= Instant::now() + RETAINED_OPERATION_DEADLINE);
    }
}
