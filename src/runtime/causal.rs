//! Ordered causal transitions owned by the runtime thread.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

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
    inner: Rc<QueueState>,
}

struct QueueState {
    pending: RefCell<VecDeque<CausalOp>>,
    reserved: Cell<usize>,
    committed: Cell<u64>,
    applied: Cell<u64>,
    capacity_changed: Cell<bool>,
}

impl Default for CausalOwnerQueue {
    fn default() -> Self {
        Self {
            inner: Rc::new(QueueState {
                pending: RefCell::new(VecDeque::with_capacity(CAUSAL_OWNER_CAPACITY)),
                reserved: Cell::new(0),
                committed: Cell::new(0),
                applied: Cell::new(0),
                capacity_changed: Cell::new(false),
            }),
        }
    }
}

/// The owner can retain a reservation while a finite Host phase executes.
/// Rc keeps the reservation on the runtime thread.
pub(crate) struct CausalReservation {
    queue: Rc<QueueState>,
    committed: bool,
}

/// FIFO application reaches this position only after this operation reaches the table.
pub(crate) struct CausalReceipt {
    queue: Rc<QueueState>,
    sequence: u64,
}

impl CausalReceipt {
    pub(crate) fn is_applied(&self) -> bool {
        self.queue.applied.get() >= self.sequence
    }
}

impl CausalOwnerQueue {
    pub(super) fn reserve(&self) -> Result<CausalReservation, CausalTransitionStatus> {
        let queue = &self.inner;
        let remaining = u64::MAX - queue.committed.get();
        if remaining == 0 {
            return Err(CausalTransitionStatus::Fault);
        }
        // Every live reservation must still have an unused FIFO position.
        if queue.pending.borrow().len() + queue.reserved.get() >= CAUSAL_OWNER_CAPACITY
            || remaining <= queue.reserved.get() as u64
        {
            return Err(CausalTransitionStatus::Waiting);
        }
        queue.reserved.set(queue.reserved.get() + 1);
        Ok(CausalReservation {
            queue: Rc::clone(queue),
            committed: false,
        })
    }

    pub(super) fn has_capacity(&self) -> bool {
        self.inner.pending.borrow().len() + self.inner.reserved.get() < CAUSAL_OWNER_CAPACITY
            && u64::MAX - self.inner.committed.get() > self.inner.reserved.get() as u64
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.inner.committed.get() == u64::MAX
    }

    pub(super) fn is_empty(&self) -> bool {
        self.inner.pending.borrow().is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.inner.pending.borrow().len()
    }

    pub(super) fn take_head(&self) -> Option<CausalOp> {
        self.inner.pending.borrow_mut().pop_front()
    }

    pub(super) fn restore_head(&self, op: CausalOp) {
        self.inner.pending.borrow_mut().push_front(op);
    }

    pub(super) fn note_applied(&self) {
        let next = self
            .inner
            .applied
            .get()
            .checked_add(1)
            .expect("an applied operation has a reserved position");
        assert!(
            next <= self.inner.committed.get(),
            "application follows FIFO admission"
        );
        self.inner.applied.set(next);
        self.inner.capacity_changed.set(true);
    }

    pub(super) fn take_capacity_notification(&self) -> bool {
        self.inner.capacity_changed.replace(false)
    }
}

impl CausalReservation {
    pub(crate) fn commit(mut self, op: CausalOp) -> CausalReceipt {
        let sequence = self
            .queue
            .committed
            .get()
            .checked_add(1)
            .expect("a reservation owns an unused FIFO position");
        self.queue.pending.borrow_mut().push_back(op);
        self.queue.committed.set(sequence);
        self.queue.reserved.set(
            self.queue
                .reserved
                .get()
                .checked_sub(1)
                .expect("a live reservation owns one slot"),
        );
        if sequence == u64::MAX {
            self.queue.capacity_changed.set(true);
        }
        self.committed = true;
        CausalReceipt {
            queue: Rc::clone(&self.queue),
            sequence,
        }
    }
}

impl Drop for CausalReservation {
    fn drop(&mut self) {
        if !self.committed {
            let was_full = self.queue.pending.borrow().len() + self.queue.reserved.get()
                == CAUSAL_OWNER_CAPACITY;
            let positions_reserved =
                u64::MAX - self.queue.committed.get() == self.queue.reserved.get() as u64;
            self.queue.reserved.set(
                self.queue
                    .reserved
                    .get()
                    .checked_sub(1)
                    .expect("a live reservation owns one slot"),
            );
            if was_full || positions_reserved {
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
        let capacity = queue.inner.pending.borrow().capacity();
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
            assert!(queue.reserve().is_err());
            let head = queue.take_head().unwrap();
            queue.restore_head(head);
            assert_eq!(queue.take_head(), Some(head));
            queue.note_applied();
            queue.reserve().unwrap().commit(CausalOp::Release {
                scope_id,
                identity: LeaseIdentity::EventInFlight,
            });
            assert_eq!(queue.inner.pending.borrow().capacity(), capacity);
            assert_eq!(queue.len(), CAUSAL_OWNER_CAPACITY);
        }
    }

    #[test]
    fn reservations_and_operations_share_capacity() {
        let queue = CausalOwnerQueue::default();
        let mut reservations: Vec<_> = (0..CAUSAL_OWNER_CAPACITY)
            .map(|_| queue.reserve().expect("reserve one transition"))
            .collect();
        assert!(queue.reserve().is_err());
        assert!(
            !queue.has_capacity(),
            "retained reservations suppress capacity readiness"
        );
        reservations.pop().unwrap().commit(CausalOp::Release {
            scope_id: 1,
            identity: LeaseIdentity::EventInFlight,
        });
        assert!(
            queue.reserve().is_err(),
            "commit must not create spare capacity"
        );
        drop(reservations.pop());
        assert!(queue.take_capacity_notification());
        assert!(queue.has_capacity());
        let replacement = queue
            .reserve()
            .expect("unused reservation returns capacity");
        assert!(queue.reserve().is_err());
        drop(replacement);
        drop(reservations);
        assert_eq!(
            queue.len(),
            1,
            "dropping unused reservations must preserve the queued operation"
        );
    }

    #[test]
    fn receipts_follow_commit_order_when_reservations_finish_out_of_order() {
        let queue = CausalOwnerQueue::default();
        let earlier_reservation = queue.reserve().unwrap();
        let later_reservation = queue.reserve().unwrap();
        let first = later_reservation.commit(CausalOp::Release {
            scope_id: 1,
            identity: LeaseIdentity::EventInFlight,
        });
        let second = earlier_reservation.commit(CausalOp::Release {
            scope_id: 2,
            identity: LeaseIdentity::EventInFlight,
        });
        assert!(!first.is_applied());
        assert!(!second.is_applied());
        let head = queue.take_head().unwrap();
        queue.restore_head(head);
        assert!(
            !first.is_applied(),
            "a table refusal does not complete the FIFO position"
        );
        assert_eq!(queue.take_head(), Some(head));
        queue.note_applied();
        assert!(first.is_applied());
        assert!(!second.is_applied());
        queue.take_head().unwrap();
        queue.note_applied();
        assert!(first.is_applied());
        assert!(second.is_applied());
    }

    #[test]
    fn reservations_cover_remaining_positions_and_receipts_never_wrap() {
        let queue = CausalOwnerQueue::default();
        queue.inner.committed.set(u64::MAX - 2);
        queue.inner.applied.set(u64::MAX - 2);
        let first_reservation = queue.reserve().unwrap();
        let second_reservation = queue.reserve().unwrap();
        assert!(matches!(
            queue.reserve(),
            Err(CausalTransitionStatus::Waiting)
        ));
        drop(second_reservation);
        assert!(queue.take_capacity_notification());
        let replacement = queue.reserve().unwrap();
        let first = replacement.commit(CausalOp::Release {
            scope_id: 1,
            identity: LeaseIdentity::EventInFlight,
        });
        assert!(matches!(
            queue.reserve(),
            Err(CausalTransitionStatus::Waiting)
        ));
        let last = first_reservation.commit(CausalOp::Release {
            scope_id: 2,
            identity: LeaseIdentity::EventInFlight,
        });
        assert!(queue.is_exhausted());
        assert!(queue.take_capacity_notification());
        assert!(matches!(
            queue.reserve(),
            Err(CausalTransitionStatus::Fault)
        ));
        assert!(!first.is_applied());
        assert!(!last.is_applied());
        queue.take_head().unwrap();
        queue.note_applied();
        assert!(first.is_applied());
        assert!(!last.is_applied());
        queue.take_head().unwrap();
        queue.note_applied();
        assert!(last.is_applied());
        assert!(queue.is_empty());
        assert!(matches!(
            queue.reserve(),
            Err(CausalTransitionStatus::Fault)
        ));
    }

    #[test]
    fn unwinding_returns_every_uncommitted_reservation() {
        let queue = CausalOwnerQueue::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _reservations: Vec<_> = (0..CAUSAL_OWNER_CAPACITY)
                .map(|_| queue.reserve().unwrap())
                .collect();
            assert!(queue.reserve().is_err());
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
        assert!(queue.reserve().is_err());
    }
}
