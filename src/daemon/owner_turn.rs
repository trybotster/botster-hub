//! Shared cooperative budget for one hub-owner turn.
//!
//! The owner must charge each work item before it starts that work. Only a typed
//! value that moves without payload access can use [`OwnerTurnCharge::opaque_move`].
//! Scanning, serialization, and proportional destruction must charge inspected
//! bytes or run off the owner.

use std::time::{Duration, Instant};

pub(crate) const OWNER_TURN_TIME_LIMIT: Duration = Duration::from_millis(2);
pub(crate) const OWNER_TURN_ITEM_LIMIT: usize = 64;
pub(crate) const OWNER_TURN_INSPECTED_BYTE_LIMIT: usize = 256 * 1024;

/// The charge for one owner work item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OwnerTurnCharge {
    inspected_bytes: usize,
}

impl OwnerTurnCharge {
    /// Charge one typed buffer movement that does not access its payload.
    pub(crate) const fn opaque_move() -> Self {
        Self { inspected_bytes: 0 }
    }

    /// Charge one work item and the bytes that the item inspects.
    pub(crate) const fn inspection(inspected_bytes: usize) -> Self {
        Self { inspected_bytes }
    }
}

/// The limit that refused an owner work item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnerTurnBudgetError {
    /// The supplied time is invalid or the cooperative time limit has expired.
    Time,
    /// The item limit cannot admit another item.
    Items { requested: usize, remaining: usize },
    /// The inspected-byte limit cannot admit the item.
    InspectedBytes { requested: usize, remaining: usize },
    /// A spent counter cannot represent the requested charge.
    Overflow,
}

/// The shared budget for all work in one owner turn.
#[derive(Debug, Clone)]
pub(crate) struct OwnerTurnBudget {
    started_at: Instant,
    spent_items: usize,
    spent_inspected_bytes: usize,
}

impl OwnerTurnBudget {
    pub(crate) const fn new(started_at: Instant) -> Self {
        Self {
            started_at,
            spent_items: 0,
            spent_inspected_bytes: 0,
        }
    }

    /// Admit and charge one item, or leave both spent counters unchanged.
    pub(crate) fn try_charge(
        &mut self,
        now: Instant,
        charge: OwnerTurnCharge,
    ) -> Result<(), OwnerTurnBudgetError> {
        let Some(elapsed) = now.checked_duration_since(self.started_at) else {
            return Err(OwnerTurnBudgetError::Time);
        };
        if elapsed >= OWNER_TURN_TIME_LIMIT {
            return Err(OwnerTurnBudgetError::Time);
        }
        if self.spent_items >= OWNER_TURN_ITEM_LIMIT {
            return Err(OwnerTurnBudgetError::Items {
                requested: 1,
                remaining: 0,
            });
        }
        if self.spent_inspected_bytes >= OWNER_TURN_INSPECTED_BYTE_LIMIT {
            return Err(OwnerTurnBudgetError::InspectedBytes {
                requested: charge.inspected_bytes,
                remaining: 0,
            });
        }

        let spent_items = self
            .spent_items
            .checked_add(1)
            .ok_or(OwnerTurnBudgetError::Overflow)?;
        let spent_inspected_bytes = self
            .spent_inspected_bytes
            .checked_add(charge.inspected_bytes)
            .ok_or(OwnerTurnBudgetError::Overflow)?;

        if spent_items > OWNER_TURN_ITEM_LIMIT {
            return Err(OwnerTurnBudgetError::Items {
                requested: 1,
                remaining: OWNER_TURN_ITEM_LIMIT - self.spent_items,
            });
        }
        if spent_inspected_bytes > OWNER_TURN_INSPECTED_BYTE_LIMIT {
            return Err(OwnerTurnBudgetError::InspectedBytes {
                requested: charge.inspected_bytes,
                remaining: OWNER_TURN_INSPECTED_BYTE_LIMIT - self.spent_inspected_bytes,
            });
        }

        self.spent_items = spent_items;
        self.spent_inspected_bytes = spent_inspected_bytes;
        Ok(())
    }

    pub(crate) const fn spent_items(&self) -> usize {
        self.spent_items
    }

    pub(crate) const fn spent_inspected_bytes(&self) -> usize {
        self.spent_inspected_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_item_limit_is_admitted_and_the_next_item_is_refused() {
        let started_at = Instant::now();
        let mut budget = OwnerTurnBudget::new(started_at);

        for _ in 0..OWNER_TURN_ITEM_LIMIT {
            assert_eq!(
                budget.try_charge(started_at, OwnerTurnCharge::opaque_move()),
                Ok(())
            );
        }

        assert_eq!(budget.spent_items(), OWNER_TURN_ITEM_LIMIT);
        assert_eq!(budget.spent_inspected_bytes(), 0);
        assert_eq!(
            budget.try_charge(started_at, OwnerTurnCharge::opaque_move()),
            Err(OwnerTurnBudgetError::Items {
                requested: 1,
                remaining: 0,
            })
        );
    }

    #[test]
    fn exact_inspected_byte_limit_is_admitted_and_ends_the_turn() {
        let started_at = Instant::now();
        let mut budget = OwnerTurnBudget::new(started_at);

        assert_eq!(
            budget.try_charge(
                started_at,
                OwnerTurnCharge::inspection(OWNER_TURN_INSPECTED_BYTE_LIMIT),
            ),
            Ok(())
        );
        assert_eq!(budget.spent_items(), 1);
        assert_eq!(
            budget.spent_inspected_bytes(),
            OWNER_TURN_INSPECTED_BYTE_LIMIT
        );
        assert_eq!(
            budget.try_charge(started_at, OwnerTurnCharge::opaque_move()),
            Err(OwnerTurnBudgetError::InspectedBytes {
                requested: 0,
                remaining: 0,
            })
        );
    }

    #[test]
    fn cumulative_charges_share_item_and_byte_counters() {
        let started_at = Instant::now();
        let mut budget = OwnerTurnBudget::new(started_at);

        assert_eq!(
            budget.try_charge(started_at, OwnerTurnCharge::inspection(100_000)),
            Ok(())
        );
        assert_eq!(
            budget.try_charge(started_at, OwnerTurnCharge::inspection(100_000)),
            Ok(())
        );
        assert_eq!(budget.spent_items(), 2);
        assert_eq!(budget.spent_inspected_bytes(), 200_000);

        assert_eq!(
            budget.try_charge(started_at, OwnerTurnCharge::inspection(62_145)),
            Err(OwnerTurnBudgetError::InspectedBytes {
                requested: 62_145,
                remaining: 62_144,
            })
        );
        assert_eq!(budget.spent_items(), 2);
        assert_eq!(budget.spent_inspected_bytes(), 200_000);
    }

    #[test]
    fn elapsed_limit_refuses_work_at_the_exact_boundary() {
        let started_at = Instant::now();
        let mut before_limit = OwnerTurnBudget::new(started_at);
        let mut at_limit = OwnerTurnBudget::new(started_at);

        assert_eq!(
            before_limit.try_charge(
                started_at + OWNER_TURN_TIME_LIMIT - Duration::from_nanos(1),
                OwnerTurnCharge::opaque_move(),
            ),
            Ok(())
        );
        assert_eq!(
            at_limit.try_charge(
                started_at + OWNER_TURN_TIME_LIMIT,
                OwnerTurnCharge::opaque_move(),
            ),
            Err(OwnerTurnBudgetError::Time)
        );
        assert_eq!(at_limit.spent_items(), 0);
    }

    #[test]
    fn opaque_move_does_not_inspect_the_payload() {
        let started_at = Instant::now();
        let mut budget = OwnerTurnBudget::new(started_at);
        let payload = vec![0_u8; OWNER_TURN_INSPECTED_BYTE_LIMIT + 1];

        assert_eq!(
            budget.try_charge(started_at, OwnerTurnCharge::opaque_move()),
            Ok(())
        );
        let moved_payload = payload;

        assert_eq!(moved_payload.len(), OWNER_TURN_INSPECTED_BYTE_LIMIT + 1);
        assert_eq!(budget.spent_items(), 1);
        assert_eq!(budget.spent_inspected_bytes(), 0);
    }

    #[test]
    fn oversized_inspection_is_refused_without_an_exemption() {
        let started_at = Instant::now();
        let mut budget = OwnerTurnBudget::new(started_at);

        assert_eq!(
            budget.try_charge(
                started_at,
                OwnerTurnCharge::inspection(OWNER_TURN_INSPECTED_BYTE_LIMIT + 1),
            ),
            Err(OwnerTurnBudgetError::InspectedBytes {
                requested: OWNER_TURN_INSPECTED_BYTE_LIMIT + 1,
                remaining: OWNER_TURN_INSPECTED_BYTE_LIMIT,
            })
        );
        assert_eq!(budget.spent_items(), 0);
        assert_eq!(budget.spent_inspected_bytes(), 0);
    }

    #[test]
    fn inspected_byte_overflow_is_refused_without_changing_counters() {
        let started_at = Instant::now();
        let mut budget = OwnerTurnBudget::new(started_at);

        assert_eq!(
            budget.try_charge(started_at, OwnerTurnCharge::inspection(1)),
            Ok(())
        );
        assert_eq!(
            budget.try_charge(started_at, OwnerTurnCharge::inspection(usize::MAX)),
            Err(OwnerTurnBudgetError::Overflow)
        );
        assert_eq!(budget.spent_items(), 1);
        assert_eq!(budget.spent_inspected_bytes(), 1);
    }

    #[test]
    fn a_time_before_the_turn_is_refused_without_changing_counters() {
        let started_at = Instant::now();
        let mut budget = OwnerTurnBudget::new(started_at);

        assert_eq!(
            budget.try_charge(
                started_at - Duration::from_nanos(1),
                OwnerTurnCharge::opaque_move(),
            ),
            Err(OwnerTurnBudgetError::Time)
        );
        assert_eq!(budget.spent_items(), 0);
        assert_eq!(budget.spent_inspected_bytes(), 0);
    }
}
