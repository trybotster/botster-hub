//! Checked identities for work that leaves the Hub owner thread.

use std::sync::atomic::{AtomicU64, Ordering};

/// One identity from the owner lifetime's monotonically increasing sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct WaiterId(pub(crate) u64);

/// One off-owner phase owned by a waiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct OwnerWorkIdentity {
    pub(crate) waiter_id: WaiterId,
    pub(crate) phase: u64,
}

impl OwnerWorkIdentity {
    pub(crate) fn first(waiter_id: WaiterId) -> Self {
        Self {
            waiter_id,
            phase: 1,
        }
    }

    pub(crate) fn next_phase(self) -> Option<Self> {
        Some(Self {
            waiter_id: self.waiter_id,
            phase: self.phase.checked_add(1)?,
        })
    }
}

/// The single checked identity source for one Hub runtime lifetime.
#[derive(Debug, Default)]
pub(crate) struct WaiterIdSource {
    next: AtomicU64,
}

impl WaiterIdSource {
    pub(crate) fn next(&self) -> Option<WaiterId> {
        let mut current = self.next.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(1)?;
            match self.next.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(WaiterId(next)),
                Err(observed) => current = observed,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn with_next(next: u64) -> Self {
        Self {
            next: AtomicU64::new(next),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waiter_ids_stop_before_wrap() {
        let source = WaiterIdSource::with_next(u64::MAX - 1);
        assert_eq!(source.next(), Some(WaiterId(u64::MAX)));
        assert_eq!(source.next(), None);
        assert_eq!(source.next(), None);
    }

    #[test]
    fn phase_serials_stop_before_wrap() {
        let identity = OwnerWorkIdentity {
            waiter_id: WaiterId(1),
            phase: u64::MAX,
        };
        assert_eq!(identity.next_phase(), None);
    }
}
