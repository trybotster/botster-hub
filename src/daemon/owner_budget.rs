//! Bounded ownership for every owner-thread operation that outlives the
//! request or connection that started it.
//!
//! One global budget of [`OWNER_BUDGET_CAPACITY`] permits covers accepted
//! connections, accepted control requests, and cleanup obligations. A permit
//! is reserved before the owner admits work and stays held until the work
//! completes or transfers to a cleanup obligation, so a connection that
//! disconnects with cleanup outstanding does not free capacity for new
//! admissions. Nothing is discarded at the limit: new admissions are refused
//! with a typed error instead, and no path creates a permit past capacity.
//! Every accepted resource physically carries its permit: a Unix connection
//! in its cleanup guard, a WebRTC peer in the budget's peer table, a pending
//! request in its entry.
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

/// Global permit capacity: every connection holds one permit for its whole
/// lifetime including cleanup, and every accepted request holds one while it
/// is pending. Attach and reserved-bind requests reserve a second permit for
/// the cleanup they may leave behind, so they are refused earlier under load.
pub(crate) const OWNER_BUDGET_CAPACITY: usize =
    DAEMON_MAX_CONNECTIONS * (MAX_OUTSTANDING_REQUESTS + 1);

/// Deadline for a retained operation. Abandoned reads are retired at the
/// deadline; obligations and side-effecting requests are counted and kept.
pub(crate) const RETAINED_OPERATION_DEADLINE: Duration = Duration::from_secs(30);

/// Operator error code when the budget refuses a new request.
pub(crate) const OWNER_BUDGET_EXHAUSTED: &str = "owner_budget_exhausted";

/// One unit of the owner budget. Not `Clone`: it is returned through
/// [`OwnerBudget::release`] or converted into an obligation.
#[derive(Debug)]
pub(crate) struct OwnerPermit(());

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

/// Owner work that must complete: it holds its permit until done.
pub(crate) struct CleanupObligation {
    terminal: Option<crate::host_disposal::Job>,
    waiter_id: crate::owner_identity::WaiterId,
    label: &'static str,
    permit: OwnerPermit,
    ready_key: Option<crate::daemon::owner_schedule::ReadyKey>,
    deadline_key: Option<crate::daemon::owner_schedule::DeadlineKey>,
    last_core_phase: u64,
    past_deadline: bool,
    poll: ObligationFn,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct OwnerBudgetCounters {
    /// Admissions refused because no permit was free.
    pub refused: u64,
    /// Pending reads retired because their client left or the deadline passed.
    pub retired_abandoned: u64,
    /// Side-effecting requests still pending past the deadline (counted once).
    pub requests_past_deadline: u64,
    /// Obligations still pending past the deadline (counted once).
    pub obligations_past_deadline: u64,
}

pub(crate) struct OwnerBudget {
    capacity: usize,
    outstanding: usize,
    released: bool,
    /// Permits reserved by admitted WebRTC peers, keyed by grant id.
    peer_permits: std::collections::BTreeMap<String, OwnerPermit>,
    obligations: std::collections::BTreeMap<crate::owner_identity::WaiterId, CleanupObligation>,
    pub(crate) counters: OwnerBudgetCounters,
}

impl Default for OwnerBudget {
    fn default() -> Self {
        Self::with_capacity(OWNER_BUDGET_CAPACITY)
    }
}

impl std::fmt::Debug for OwnerBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnerBudget")
            .field("capacity", &self.capacity)
            .field("outstanding", &self.outstanding)
            .field("peer_permits", &self.peer_permits.len())
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
                    let obligation = self
                        .obligations
                        .remove(&waiter)
                        .expect("the obligation retires after disposal");
                    self.release(obligation.permit);
                    drop(permit);
                }
            } else if let Some(permit) = executor.try_reserve() {
                let poll = std::mem::replace(
                    &mut obligation.poll,
                    Box::new(|_, _, _| ObligationPoll::Pending),
                );
                obligation.terminal = Some(crate::host_disposal::Job::new(
                    crate::host_disposal::Parts {
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

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            outstanding: 0,
            released: false,
            peer_permits: std::collections::BTreeMap::new(),
            obligations: std::collections::BTreeMap::new(),
            counters: OwnerBudgetCounters::default(),
        }
    }

    /// Permits held by connections, peers, pending requests, and obligations.
    #[cfg(test)]
    pub(crate) fn outstanding(&self) -> usize {
        self.outstanding
    }

    #[cfg(test)]
    pub(crate) fn queued_obligations(&self) -> usize {
        self.obligations.len()
    }

    /// Reserve one permit, or refuse when the budget is exhausted.
    #[must_use]
    pub(crate) fn reserve(&mut self) -> Option<OwnerPermit> {
        if self.outstanding >= self.capacity {
            self.counters.refused = self.counters.refused.saturating_add(1);
            return None;
        }
        self.outstanding += 1;
        Some(OwnerPermit(()))
    }

    pub(crate) fn release(&mut self, permit: OwnerPermit) {
        drop(permit);
        self.outstanding = self.outstanding.saturating_sub(1);
        self.released = true;
    }

    pub(crate) fn take_capacity_notification(&mut self) -> bool {
        std::mem::take(&mut self.released)
    }

    /// Reserve the permit an accepted connection carries in its cleanup
    /// guard until its cleanup completes. `None` means the connection must
    /// be refused.
    #[must_use]
    pub(crate) fn reserve_connection(&mut self) -> Option<OwnerPermit> {
        self.reserve()
    }

    /// Reserve the permit an admitted WebRTC peer holds until peer cleanup.
    #[must_use]
    pub(crate) fn reserve_peer(&mut self, grant_id: &str) -> bool {
        match self.reserve() {
            Some(permit) => {
                self.peer_permits.insert(grant_id.to_string(), permit);
                true
            }
            None => false,
        }
    }

    /// Take the permit for one peer's cleanup, when the peer was admitted.
    pub(crate) fn take_peer_permit(&mut self, grant_id: &str) -> Option<OwnerPermit> {
        self.peer_permits.remove(grant_id)
    }

    /// Whether a peer still holds its permit; attach admission requires it.
    pub(crate) fn peer_holds_permit(&self, grant_id: &str) -> bool {
        self.peer_permits.contains_key(grant_id)
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
    permit: OwnerPermit,
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
            waiter_id,
            label,
            permit,
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
    permit: OwnerPermit,
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
    retain_owner_obligation(state, waiter_id, permit, label, poll);
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
            state.budget.release(obligation.permit);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_refuses_at_capacity_and_counts_it() {
        let mut budget = OwnerBudget::with_capacity(2);
        let first = budget.reserve().expect("first");
        let second = budget.reserve().expect("second");
        assert!(budget.reserve().is_none());
        assert_eq!(budget.counters.refused, 1);
        assert_eq!(budget.outstanding(), 2);
        budget.release(first);
        assert!(budget.reserve().is_some());
        budget.release(second);
    }

    #[test]
    fn connection_permit_survives_disconnect_until_cleanup_completes() {
        let mut state = DaemonControlState::default();
        state.budget = OwnerBudget::with_capacity(1);
        let permit = state
            .budget
            .reserve_connection()
            .expect("connection permit");
        // The transport permit is gone; the budget permit is not.
        assert!(state.budget.reserve_connection().is_none());
        retain_owner_obligation(
            &mut state,
            crate::owner_identity::WaiterId(1),
            permit,
            "test",
            |_, _, _| ObligationPoll::Pending,
        );
        assert!(
            state.budget.reserve_connection().is_none(),
            "a new connection cannot replenish the budget while cleanup remains"
        );
        assert_eq!(state.budget.queued_obligations(), 1);
        assert_eq!(state.budget.outstanding(), 1);
    }

    #[test]
    fn nothing_creates_a_permit_past_capacity() {
        let mut budget = OwnerBudget::with_capacity(0);
        assert!(budget.reserve().is_none());
        assert!(budget.reserve_connection().is_none());
        assert!(!budget.reserve_peer("grant"));
        assert!(budget.take_peer_permit("grant").is_none());
        assert!(!budget.peer_holds_permit("grant"));
        assert_eq!(budget.outstanding(), 0);
    }

    #[test]
    fn peer_permit_is_taken_once() {
        let mut budget = OwnerBudget::with_capacity(1);
        assert!(budget.reserve_peer("grant"));
        assert!(!budget.reserve_peer("other"));
        assert!(budget.take_peer_permit("grant").is_some());
        assert!(budget.take_peer_permit("grant").is_none());
    }

    #[test]
    fn obligation_uses_the_shared_deadline_index() {
        let mut state = DaemonControlState::default();
        state.budget = OwnerBudget::with_capacity(2);
        assert!(state.deadlines.next_deadline().is_none());
        let permit = state.budget.reserve().expect("permit");
        retain_owner_obligation(
            &mut state,
            crate::owner_identity::WaiterId(1),
            permit,
            "first",
            |_, _, _| ObligationPoll::Pending,
        );
        let deadline = state.deadlines.next_deadline().expect("deadline");
        assert!(deadline > Instant::now());
        assert!(deadline <= Instant::now() + RETAINED_OPERATION_DEADLINE);
    }
}
