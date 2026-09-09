//! Ordered causal transitions owned by the runtime thread.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

use crate::package_event_router::CausalOp;

pub const CAUSAL_OWNER_CAPACITY: usize = 256;
/// Maximum occupied payload bytes, excluding the queue header and allocator overhead.
pub const CAUSAL_OWNER_PAYLOAD_BYTES: usize =
    CAUSAL_OWNER_CAPACITY * std::mem::size_of::<CausalOp>();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CausalTransitionStatus {
    Applied,
    Waiting,
    Fault,
}

pub(super) struct CausalOwnerQueue {
    pending: RefCell<VecDeque<CausalOp>>,
    reserved: Cell<usize>,
    capacity_changed: Cell<bool>,
}

impl Default for CausalOwnerQueue {
    fn default() -> Self {
        Self {
            pending: RefCell::new(VecDeque::with_capacity(CAUSAL_OWNER_CAPACITY)),
            reserved: Cell::new(0),
            capacity_changed: Cell::new(false),
        }
    }
}

/// A reservation covers the immediate transition, before its source changes.
pub(crate) struct CausalReservation<'a> {
    queue: &'a CausalOwnerQueue,
    committed: bool,
}

impl CausalOwnerQueue {
    pub(super) fn reserve(&self) -> Option<CausalReservation<'_>> {
        if self.pending.borrow().len() + self.reserved.get() >= CAUSAL_OWNER_CAPACITY {
            return None;
        }
        self.reserved.set(self.reserved.get() + 1);
        Some(CausalReservation {
            queue: self,
            committed: false,
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.borrow().is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.pending.borrow().len()
    }

    pub(super) fn take_head(&self) -> Option<CausalOp> {
        self.pending.borrow_mut().pop_front()
    }

    pub(super) fn restore_head(&self, op: CausalOp) {
        self.pending.borrow_mut().push_front(op);
    }

    pub(super) fn note_applied(&self) {
        self.capacity_changed.set(true);
    }

    pub(super) fn take_capacity_notification(&self) -> bool {
        self.capacity_changed.replace(false)
    }
}

impl CausalReservation<'_> {
    pub(crate) fn commit(mut self, op: CausalOp) {
        self.queue.pending.borrow_mut().push_back(op);
        self.queue.reserved.set(
            self.queue
                .reserved
                .get()
                .checked_sub(1)
                .expect("a live reservation owns one slot"),
        );
        self.committed = true;
    }
}

impl Drop for CausalReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let was_full = self.queue.pending.borrow().len() + self.queue.reserved.get()
                == CAUSAL_OWNER_CAPACITY;
            self.queue.reserved.set(
                self.queue
                    .reserved
                    .get()
                    .checked_sub(1)
                    .expect("a live reservation owns one slot"),
            );
            if was_full {
                self.queue.capacity_changed.set(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package_event_router::LeaseIdentity;

    #[test]
    fn full_queue_wrap_and_retry_preserve_the_initial_allocation() {
        let queue = CausalOwnerQueue::default();
        let capacity = queue.pending.borrow().capacity();
        assert!(capacity >= CAUSAL_OWNER_CAPACITY);
        assert!(!std::mem::needs_drop::<CausalOp>());
        for scope_id in 0..CAUSAL_OWNER_CAPACITY as u64 {
            queue.reserve().unwrap().commit(CausalOp::Release {
                scope_id,
                identity: LeaseIdentity::EventInFlight,
            });
        }
        assert_eq!(
            queue.len() * std::mem::size_of::<CausalOp>(),
            CAUSAL_OWNER_PAYLOAD_BYTES
        );
        for scope_id in 0..(2 * CAUSAL_OWNER_CAPACITY) as u64 {
            assert!(queue.reserve().is_none());
            let head = queue.take_head().unwrap();
            queue.restore_head(head);
            assert_eq!(queue.take_head(), Some(head));
            queue.note_applied();
            queue.reserve().unwrap().commit(CausalOp::Release {
                scope_id,
                identity: LeaseIdentity::EventInFlight,
            });
            assert_eq!(queue.pending.borrow().capacity(), capacity);
            assert_eq!(queue.len(), CAUSAL_OWNER_CAPACITY);
        }
    }

    #[test]
    fn reservations_and_operations_share_capacity() {
        let queue = CausalOwnerQueue::default();
        let mut reservations: Vec<_> = (0..CAUSAL_OWNER_CAPACITY)
            .map(|_| queue.reserve().expect("reserve one transition"))
            .collect();
        assert!(queue.reserve().is_none());
        reservations.pop().unwrap().commit(CausalOp::Release {
            scope_id: 1,
            identity: LeaseIdentity::EventInFlight,
        });
        assert!(
            queue.reserve().is_none(),
            "commit must not create spare capacity"
        );
        drop(reservations.pop());
        assert!(queue.take_capacity_notification());
        let replacement = queue
            .reserve()
            .expect("unused reservation returns capacity");
        assert!(queue.reserve().is_none());
        drop(replacement);
        drop(reservations);
        assert_eq!(
            queue.len(),
            1,
            "dropping unused reservations must preserve the queued operation"
        );
    }

    #[test]
    fn unwinding_returns_every_uncommitted_reservation() {
        let queue = CausalOwnerQueue::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _reservations: Vec<_> = (0..CAUSAL_OWNER_CAPACITY)
                .map(|_| queue.reserve().unwrap())
                .collect();
            assert!(queue.reserve().is_none());
            panic!("source preparation failed before commit");
        }));
        assert!(result.is_err());
        assert!(queue.take_capacity_notification());
        let _reservations: Vec<_> = (0..CAUSAL_OWNER_CAPACITY)
            .map(|_| {
                queue
                    .reserve()
                    .expect("unwinding returns the original capacity")
            })
            .collect();
        assert!(queue.reserve().is_none());
    }
}
