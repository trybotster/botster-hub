//! Per-route output credit on a Unix connection (S13, plan §4.3).
//!
//! The client grants credit for whole terminal frames. The adapter writes a
//! frame only when the route's pool covers one item plus the frame's
//! `body.len()`. Otherwise it records the frame's need, demands it once, and
//! refuses the write (`WouldBlock`). A grant adds to the pool and wakes Core
//! only when the pool then covers the recorded need, so a refusal never
//! spends Core's write attempts. The caller holds this state under one lock
//! across the credit check and the slot write, so a grant cannot land between
//! the need being recorded and the final check.

/// Outcome of a credit check for one offered frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreditCheck {
    /// The pool covers the frame: write it, then call [`RouteCredit::commit`].
    Covered,
    /// The pool does not cover the frame. The adapter refuses the write.
    /// `demand` carries the bytes to demand, or `None` while an earlier
    /// demand is still unanswered (a route has at most one).
    Short { demand: Option<u64> },
}

/// Outcome of applying one grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GrantOutcome {
    /// The pool now covers the recorded need: raise the writable wake once.
    pub(crate) wake: bool,
    /// The pool still does not cover the recorded need: demand the shortfall.
    pub(crate) demand: Option<u64>,
}

/// Credit given back to the client in a `RETURN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CreditReturn {
    pub(crate) items: u32,
    pub(crate) bytes: u64,
}

/// One route's output credit for one generation.
#[derive(Debug, Default)]
pub(crate) struct RouteCredit {
    pool_items: u64,
    pool_bytes: u64,
    spent_items: u64,
    spent_bytes: u64,
    /// The refused head's bytes, recorded before the final check. The wake is
    /// armed while it is set.
    need: Option<u64>,
    demand_outstanding: bool,
}

impl RouteCredit {
    fn covers(&self, bytes: u64) -> bool {
        self.pool_items >= 1 && self.pool_bytes >= bytes
    }

    /// Check the offered frame of `bytes` against the pool. When the pool is
    /// short, record the need (arming the wake) and demand it unless a demand
    /// is already outstanding.
    pub(crate) fn check(&mut self, bytes: u64) -> CreditCheck {
        if self.covers(bytes) {
            return CreditCheck::Covered;
        }
        self.need = Some(bytes);
        CreditCheck::Short {
            demand: self.next_demand(bytes),
        }
    }

    fn next_demand(&mut self, bytes: u64) -> Option<u64> {
        if self.demand_outstanding {
            return None;
        }
        self.demand_outstanding = true;
        // The client grants whole frames: it covers one item and the
        // demanded bytes. Demand the frame's shortfall against the pool.
        Some(bytes.saturating_sub(self.pool_bytes).max(1))
    }

    /// Debit an accepted frame of `bytes` from the pool. Call only after
    /// [`Self::check`] returned [`CreditCheck::Covered`] and the slot accepted
    /// the frame.
    pub(crate) fn commit(&mut self, bytes: u64) {
        debug_assert!(self.covers(bytes), "commit only a covered frame");
        self.pool_items -= 1;
        self.pool_bytes -= bytes;
        self.need = None;
    }

    /// Count a frame of `bytes` that is fully on the socket. Spend counts
    /// written frames, not accepted ones: a close can drop an accepted frame
    /// from the slot unwritten, and the client must release that credit at
    /// `CLOSED`.
    pub(crate) fn record_written(&mut self, bytes: u64) {
        self.spent_items += 1;
        self.spent_bytes += bytes;
    }

    /// Apply a grant. It answers the outstanding demand. When the pool now
    /// covers the recorded need, the wake is raised once and disarmed; when
    /// it still does not, the shortfall is demanded again.
    pub(crate) fn grant(&mut self, items: u32, bytes: u64) -> GrantOutcome {
        self.pool_items += u64::from(items);
        self.pool_bytes += bytes;
        self.demand_outstanding = false;
        match self.need {
            Some(need) if self.covers(need) => {
                self.need = None;
                GrantOutcome {
                    wake: true,
                    demand: None,
                }
            }
            Some(need) => GrantOutcome {
                wake: false,
                demand: self.next_demand(need),
            },
            None => GrantOutcome {
                wake: false,
                demand: None,
            },
        }
    }

    /// Core withdrew the refused head (`head_withdrawn`): drop the need and
    /// give the whole pool back, so an idle route holds no credit.
    pub(crate) fn withdraw_head(&mut self) -> Option<CreditReturn> {
        self.need = None;
        self.take_pool()
    }

    fn take_pool(&mut self) -> Option<CreditReturn> {
        if self.pool_items == 0 && self.pool_bytes == 0 {
            return None;
        }
        let items = u32::try_from(self.pool_items).unwrap_or(u32::MAX);
        let bytes = self.pool_bytes;
        self.pool_items -= u64::from(items);
        self.pool_bytes = 0;
        Some(CreditReturn { items, bytes })
    }

    /// The generation's written total, reported in `CLOSED`. The client
    /// releases what it granted minus this and minus what was returned, so
    /// pooled, dropped, and in-transit credit need no separate return.
    pub(crate) fn spent(&self) -> (u64, u64) {
        (self.spent_items, self.spent_bytes)
    }

    #[cfg(test)]
    fn pool(&self) -> (u64, u64) {
        (self.pool_items, self.pool_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_route_holds_no_credit_until_it_has_output_and_demands_once() {
        let mut credit = RouteCredit::default();
        assert_eq!(credit.check(100), CreditCheck::Short { demand: Some(100) });
        // A second refusal of the same head demands nothing more.
        assert_eq!(credit.check(100), CreditCheck::Short { demand: None });
    }

    #[test]
    fn a_covering_grant_wakes_once_and_the_write_debits_the_pool() {
        let mut credit = RouteCredit::default();
        assert!(matches!(credit.check(100), CreditCheck::Short { .. }));
        assert_eq!(
            credit.grant(1, 100),
            GrantOutcome {
                wake: true,
                demand: None
            }
        );
        assert_eq!(credit.check(100), CreditCheck::Covered);
        credit.commit(100);
        assert_eq!(credit.pool(), (0, 0));
        assert_eq!(credit.spent(), (0, 0), "an accepted frame is not yet spent");
        credit.record_written(100);
        assert_eq!(credit.spent(), (1, 100));
    }

    #[test]
    fn a_short_grant_raises_no_wake_and_demands_the_shortfall() {
        let mut credit = RouteCredit::default();
        assert!(matches!(credit.check(100), CreditCheck::Short { .. }));
        assert_eq!(
            credit.grant(1, 40),
            GrantOutcome {
                wake: false,
                demand: Some(60)
            }
        );
        assert_eq!(
            credit.grant(1, 60),
            GrantOutcome {
                wake: true,
                demand: None
            }
        );
    }

    #[test]
    fn nonzero_but_insufficient_credit_refuses_the_whole_frame() {
        let mut credit = RouteCredit::default();
        assert!(matches!(credit.check(10), CreditCheck::Short { .. }));
        credit.grant(1, 10);
        // A larger head arrives in front (for example a resync): the pool
        // covers neither its bytes, so the write is refused whole.
        assert!(matches!(credit.check(50), CreditCheck::Short { .. }));
        assert_eq!(credit.pool(), (1, 10));
    }

    #[test]
    fn a_grant_with_no_recorded_need_raises_nothing() {
        let mut credit = RouteCredit::default();
        assert_eq!(
            credit.grant(1, 10),
            GrantOutcome {
                wake: false,
                demand: None
            }
        );
    }

    #[test]
    fn a_withdrawn_head_returns_the_whole_pool() {
        let mut credit = RouteCredit::default();
        assert!(matches!(credit.check(100), CreditCheck::Short { .. }));
        credit.grant(1, 100);
        assert_eq!(
            credit.withdraw_head(),
            Some(CreditReturn {
                items: 1,
                bytes: 100
            })
        );
        assert_eq!(credit.pool(), (0, 0));
        assert_eq!(credit.withdraw_head(), None, "nothing left to return");
        assert_eq!(credit.spent(), (0, 0));
    }

    #[test]
    fn spend_counts_only_the_written_frames_of_the_generation() {
        let mut credit = RouteCredit::default();
        for bytes in [10, 20, 30] {
            assert!(matches!(credit.check(bytes), CreditCheck::Short { .. }));
            credit.grant(1, bytes);
            assert_eq!(credit.check(bytes), CreditCheck::Covered);
            credit.commit(bytes);
        }
        // The last accepted frame is dropped from the slot at close unwritten.
        credit.record_written(10);
        credit.record_written(20);
        assert_eq!(credit.spent(), (2, 30));
    }
}
