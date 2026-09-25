//! Allocation-owned logical byte accounting for immutable Hub views.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) const SHARED_VIEW_BYTE_CAPACITY: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct SharedViewBudget {
    capacity: usize,
    used: AtomicUsize,
}

impl SharedViewBudget {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            capacity: SHARED_VIEW_BYTE_CAPACITY,
            used: AtomicUsize::new(0),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_capacity(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            used: AtomicUsize::new(0),
        })
    }

    pub(crate) fn reserve(
        self: &Arc<Self>,
        logical_bytes: usize,
    ) -> Result<SharedViewCharge, SharedViewCapacityError> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let Some(next) = used.checked_add(logical_bytes) else {
                return Err(self.capacity_error(logical_bytes, used));
            };
            if next > self.capacity {
                return Err(self.capacity_error(logical_bytes, used));
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Ok(SharedViewCharge {
                        budget: Arc::clone(self),
                        logical_bytes,
                    });
                }
                Err(observed) => used = observed,
            }
        }
    }

    fn capacity_error(&self, requested: usize, used: usize) -> SharedViewCapacityError {
        SharedViewCapacityError {
            requested,
            available: self.capacity.saturating_sub(used),
        }
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn available(&self) -> usize {
        self.capacity.saturating_sub(self.used())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SharedViewCapacityError {
    pub(crate) requested: usize,
    pub(crate) available: usize,
}

pub(crate) struct SharedViewCharge {
    budget: Arc<SharedViewBudget>,
    logical_bytes: usize,
}

impl Drop for SharedViewCharge {
    fn drop(&mut self) {
        self.budget
            .used
            .fetch_sub(self.logical_bytes, Ordering::AcqRel);
    }
}

struct SharedViewAllocation<T> {
    value: T,
    _charge: SharedViewCharge,
}

/// One immutable allocation whose clones share one logical byte charge.
pub struct SharedView<T>(Arc<SharedViewAllocation<T>>);

impl<T> SharedView<T> {
    pub(crate) fn try_new(
        budget: &Arc<SharedViewBudget>,
        value: T,
        logical_bytes: usize,
    ) -> Result<Self, SharedViewCapacityError> {
        let charge = budget.reserve(logical_bytes)?;
        Ok(Self::from_reserved(value, charge))
    }

    /// Attach a charge acquired before the caller allocated the value.
    pub(crate) fn from_reserved(value: T, charge: SharedViewCharge) -> Self {
        Self(Arc::new(SharedViewAllocation {
            value,
            _charge: charge,
        }))
    }

    pub(crate) fn budget(&self) -> Arc<SharedViewBudget> {
        Arc::clone(&self.0._charge.budget)
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(left: &Self, right: &Self) -> bool {
        Arc::ptr_eq(&left.0, &right.0)
    }
}

impl<T> Clone for SharedView<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Deref for SharedView<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0.value
    }
}

impl<T> AsRef<T> for SharedView<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T: fmt::Debug> fmt::Debug for SharedView<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.value.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_count_one_allocation_until_the_last_clone_drops() {
        let budget = SharedViewBudget::with_capacity(10);
        let view = SharedView::try_new(&budget, "value", 6).expect("view fits");
        let lease = view.clone();
        assert_eq!(budget.used(), 6);
        drop(view);
        assert_eq!(budget.used(), 6);
        drop(lease);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn overlapping_allocations_use_the_same_capacity() {
        let budget = SharedViewBudget::with_capacity(10);
        let current = SharedView::try_new(&budget, "current", 6).expect("current fits");
        let error = SharedView::try_new(&budget, "candidate", 5).expect_err("overlap fails");
        assert_eq!(error.available, 4);
        drop(current);
        SharedView::try_new(&budget, "candidate", 5).expect("candidate fits after release");
    }
}
