//! Bounded ownership for every owner-thread operation that outlives the
//! request or connection that started it.
//!
//! One global budget of [`OWNER_BUDGET_CAPACITY`] permits covers accepted
//! connections, accepted control requests, and cleanup obligations. A permit
//! is reserved before the owner admits work and stays held until the work
//! completes or transfers to a cleanup obligation, so a connection that
//! disconnects with cleanup outstanding does not free capacity for new
//! admissions. Nothing is discarded at the limit: new admissions are refused
//! with a typed error instead.
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
pub(crate) struct OwnerPermit {
    owner: String,
}

impl OwnerPermit {
    pub(crate) fn owner(&self) -> &str {
        &self.owner
    }
}

/// Outcome of one obligation poll.
pub(crate) enum ObligationPoll {
    Pending,
    Done,
}

type ObligationFn =
    Box<dyn FnMut(&mut HubDaemon, &mut DaemonControlState) -> ObligationPoll + Send>;

/// Owner work that must complete: it holds its permit until done.
pub(crate) struct CleanupObligation {
    label: &'static str,
    permit: OwnerPermit,
    started: Instant,
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
    /// Cleanup that arrived without a matching reserved permit.
    pub forced: u64,
}

pub(crate) struct OwnerBudget {
    capacity: usize,
    outstanding: usize,
    /// Permits reserved by accepted Unix connections, consumed by cleanup.
    connection_permits: Vec<OwnerPermit>,
    /// Permits reserved by admitted WebRTC peers, keyed by grant id.
    peer_permits: std::collections::BTreeMap<String, OwnerPermit>,
    obligations: Vec<CleanupObligation>,
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
            .field("connection_permits", &self.connection_permits.len())
            .field("peer_permits", &self.peer_permits.len())
            .field("obligations", &self.obligations.len())
            .field("counters", &self.counters)
            .finish()
    }
}

impl OwnerBudget {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            outstanding: 0,
            connection_permits: Vec::new(),
            peer_permits: std::collections::BTreeMap::new(),
            obligations: Vec::new(),
            counters: OwnerBudgetCounters::default(),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Permits held by connections, pending requests, and obligations.
    pub(crate) fn outstanding(&self) -> usize {
        self.outstanding
    }

    pub(crate) fn queued_obligations(&self) -> usize {
        self.obligations.len()
    }

    /// Reserve one permit, or refuse when the budget is exhausted.
    #[must_use]
    pub(crate) fn reserve(&mut self, owner: impl Into<String>) -> Option<OwnerPermit> {
        if self.outstanding >= self.capacity {
            self.counters.refused = self.counters.refused.saturating_add(1);
            return None;
        }
        self.outstanding += 1;
        Some(OwnerPermit {
            owner: owner.into(),
        })
    }

    /// Reserve a permit for cleanup that arrived without one. This is the
    /// only way past the capacity, counted so the invariant break is visible;
    /// cleanup is never discarded.
    fn force_reserve(&mut self, owner: impl Into<String>) -> OwnerPermit {
        self.counters.forced = self.counters.forced.saturating_add(1);
        self.outstanding += 1;
        OwnerPermit {
            owner: owner.into(),
        }
    }

    pub(crate) fn release(&mut self, permit: OwnerPermit) {
        drop(permit);
        self.outstanding = self.outstanding.saturating_sub(1);
    }

    /// Reserve the permit an accepted connection holds until its cleanup
    /// completes. `false` means the connection must be refused.
    #[must_use]
    pub(crate) fn reserve_connection(&mut self) -> bool {
        match self.reserve("connection") {
            Some(permit) => {
                self.connection_permits.push(permit);
                true
            }
            None => false,
        }
    }

    /// Take the permit for one connection's cleanup.
    pub(crate) fn take_connection_permit(&mut self, client_id: &str) -> OwnerPermit {
        match self.connection_permits.pop() {
            Some(mut permit) => {
                permit.owner = format!("cleanup:{client_id}");
                permit
            }
            None => self.force_reserve(format!("cleanup:{client_id}")),
        }
    }

    /// Reserve the permit an admitted WebRTC peer holds until peer cleanup.
    #[must_use]
    pub(crate) fn reserve_peer(&mut self, grant_id: &str) -> bool {
        match self.reserve(format!("peer:{grant_id}")) {
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

    /// Take the peer's permit, or force one when cleanup work exists for a
    /// peer that never reserved (counted; cleanup is never discarded).
    pub(crate) fn take_peer_permit_or_force(&mut self, grant_id: &str) -> OwnerPermit {
        self.take_peer_permit(grant_id)
            .unwrap_or_else(|| self.force_reserve(format!("peer-cleanup:{grant_id}")))
    }

    /// Retain owner work that must complete, holding `permit` until it does.
    pub(crate) fn retain(
        &mut self,
        permit: OwnerPermit,
        label: &'static str,
        poll: impl FnMut(&mut HubDaemon, &mut DaemonControlState) -> ObligationPoll + Send + 'static,
    ) {
        self.obligations.push(CleanupObligation {
            label,
            permit,
            started: Instant::now(),
            past_deadline: false,
            poll: Box::new(poll),
        });
    }

    /// Earliest deadline among retained obligations that have not passed it.
    pub(crate) fn next_obligation_deadline(&self) -> Option<Instant> {
        self.obligations
            .iter()
            .filter(|obligation| !obligation.past_deadline)
            .map(|obligation| obligation.started + RETAINED_OPERATION_DEADLINE)
            .min()
    }

    #[cfg(test)]
    pub(crate) fn obligation_labels(&self) -> Vec<&'static str> {
        self.obligations
            .iter()
            .map(|obligation| obligation.label)
            .collect()
    }
}

/// Poll every retained obligation once. Finished obligations release their
/// permit; the rest are kept in order, followed by any obligations retained
/// while polling.
pub(crate) fn poll_owner_obligations(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    now: Instant,
) {
    if state.budget.obligations.is_empty() {
        return;
    }
    if let Some(runtime) = daemon.runtime() {
        runtime.absorb_core_completions();
    }
    let obligations = std::mem::take(&mut state.budget.obligations);
    let mut retained = Vec::with_capacity(obligations.len());
    for mut obligation in obligations {
        match (obligation.poll)(daemon, state) {
            ObligationPoll::Done => {
                state.budget.release(obligation.permit);
            }
            ObligationPoll::Pending => {
                if !obligation.past_deadline
                    && now.saturating_duration_since(obligation.started)
                        >= RETAINED_OPERATION_DEADLINE
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
                retained.push(obligation);
            }
        }
    }
    retained.append(&mut state.budget.obligations);
    state.budget.obligations = retained;
}

/// Result of driving one Core ticket slot.
pub(crate) enum CoreWorkPoll<T> {
    /// No answer yet, or the admission was refused and will be retried.
    Pending,
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
    submit: impl FnOnce(&HubRuntime, &mut DaemonControlState) -> CoreTicket<T>,
) -> CoreWorkPoll<T> {
    if slot.is_none() {
        let Some(runtime) = daemon.runtime() else {
            return CoreWorkPoll::Lost;
        };
        *slot = Some(submit(runtime, state));
    }
    match slot.as_mut().expect("slot filled above").poll() {
        CoreTicketPoll::Pending => CoreWorkPoll::Pending,
        CoreTicketPoll::Refused => {
            *slot = None;
            CoreWorkPoll::Pending
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
        let first = budget.reserve("a").expect("first");
        let second = budget.reserve("b").expect("second");
        assert!(budget.reserve("c").is_none());
        assert_eq!(budget.counters.refused, 1);
        assert_eq!(budget.outstanding(), 2);
        budget.release(first);
        assert!(budget.reserve("c").is_some());
        budget.release(second);
    }

    #[test]
    fn connection_permit_survives_disconnect_until_cleanup_completes() {
        let mut budget = OwnerBudget::with_capacity(1);
        assert!(budget.reserve_connection());
        // The transport permit is gone; the budget permit is not.
        assert!(!budget.reserve_connection());
        let permit = budget.take_connection_permit("client-1");
        assert_eq!(permit.owner(), "cleanup:client-1");
        budget.retain(permit, "test", |_, _| ObligationPoll::Pending);
        assert!(
            !budget.reserve_connection(),
            "a new connection cannot replenish the budget while cleanup remains"
        );
        assert_eq!(budget.queued_obligations(), 1);
        assert_eq!(budget.outstanding(), 1);
    }

    #[test]
    fn cleanup_without_a_reserved_permit_is_forced_not_discarded() {
        let mut budget = OwnerBudget::with_capacity(0);
        let permit = budget.take_connection_permit("orphan");
        assert_eq!(budget.counters.forced, 1);
        assert_eq!(budget.outstanding(), 1);
        budget.release(permit);
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
    fn deadline_is_the_earliest_unflagged_obligation() {
        let mut budget = OwnerBudget::with_capacity(2);
        assert!(budget.next_obligation_deadline().is_none());
        let permit = budget.reserve("x").expect("permit");
        budget.retain(permit, "first", |_, _| ObligationPoll::Pending);
        let deadline = budget.next_obligation_deadline().expect("deadline");
        assert!(deadline > Instant::now());
        assert!(deadline <= Instant::now() + RETAINED_OPERATION_DEADLINE);
    }
}
