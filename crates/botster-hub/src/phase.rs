//! The stop state (design section 5, "Stop state and wakes"): one shared atomic phase that only moves forward. Each move is
//! a compare-and-swap, so each stop step runs once, and every move wakes both loops out of band.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};

/// The phases, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Phase {
    Running = 0,
    Stopping = 1,
    OwnerDone = 2,
    SocketRemoved = 3,
    LockReleased = 4,
}

impl Phase {
    fn from_u8(value: u8) -> Phase {
        match value {
            0 => Phase::Running,
            1 => Phase::Stopping,
            2 => Phase::OwnerDone,
            3 => Phase::SocketRemoved,
            _ => Phase::LockReleased,
        }
    }
}

/// Something that a phase move wakes: a `mio::Waker` in the daemon, a counter in a test.
pub trait Wake: Send + Sync {
    fn wake(&self);
}

impl Wake for mio::Waker {
    fn wake(&self) {
        // A failed wake leaves the loop on its poll timeout; there is nothing else to do with the error.
        let _ = mio::Waker::wake(self);
    }
}

/// The shared phase, with the two loops' wakers.
pub struct SharedPhase {
    phase: AtomicU8,
    owner: OnceLock<Arc<dyn Wake>>,
    io: OnceLock<Arc<dyn Wake>>,
}

impl Default for SharedPhase {
    fn default() -> Self {
        SharedPhase {
            phase: AtomicU8::new(Phase::Running as u8),
            owner: OnceLock::new(),
            io: OnceLock::new(),
        }
    }
}

impl SharedPhase {
    pub fn new() -> Arc<SharedPhase> {
        Arc::new(SharedPhase::default())
    }

    /// Registers the owner's waker. A second registration is ignored.
    pub fn set_owner_waker(&self, waker: Arc<dyn Wake>) {
        let _ = self.owner.set(waker);
    }

    /// Registers the `io` thread's waker. A second registration is ignored.
    pub fn set_io_waker(&self, waker: Arc<dyn Wake>) {
        let _ = self.io.set(waker);
    }

    pub fn get(&self) -> Phase {
        Phase::from_u8(self.phase.load(Ordering::SeqCst))
    }

    /// True from `Stopping` on: the bridge refuses new requests, and a dropped token answers `hub_shutdown`.
    pub fn stopping(&self) -> bool {
        self.get() >= Phase::Stopping
    }

    /// Moves `from` to `to` by compare-and-swap. Only the caller that wins runs the step; it returns `true`. Every winning
    /// move wakes both loops.
    pub fn advance(&self, from: Phase, to: Phase) -> bool {
        debug_assert!(to > from);
        let won = self
            .phase
            .compare_exchange(from as u8, to as u8, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
        if won {
            self.wake_both();
        }
        won
    }

    /// Wakes both loops.
    pub fn wake_both(&self) {
        if let Some(waker) = self.owner.get() {
            waker.wake();
        }
        if let Some(waker) = self.io.get() {
            waker.wake();
        }
    }
}

/// Why a stop started (the `StopRequested` cause).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopCause {
    /// SIGTERM or SIGINT.
    Signal,
    /// A `Shutdown` request.
    Requested,
    /// The owner failed (a panic, or adoption failed).
    OwnerFailed,
    /// The store failed (design section 5, "Store fault").
    StoreFailed,
    /// The `io` thread ended before the stop (a panic).
    IoFailed,
}

/// How a thread ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    Clean,
    Panicked,
}

/// The messages on the lifecycle channel (capacity 4, one slot per kind; each kind is sent at most once).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    StopRequested(StopCause),
    OwnerGone(Ended),
    SocketRemoved,
    IoGone(Ended),
}

/// The capacity of the lifecycle channel: one slot per [`Lifecycle`] kind.
pub const LIFECYCLE_CAPACITY: usize = 4;

/// Moves `Running` to `Stopping` and, if this caller won, sends `StopRequested`. A full or closed channel cannot happen
/// for the first `StopRequested` (one slot per kind); a send error is ignored.
pub fn request_stop(
    phase: &SharedPhase,
    lifecycle: &std::sync::mpsc::SyncSender<Lifecycle>,
    cause: StopCause,
) -> bool {
    let won = phase.advance(Phase::Running, Phase::Stopping);
    if won {
        let _ = lifecycle.try_send(Lifecycle::StopRequested(cause));
    }
    won
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    pub(crate) struct Count(pub AtomicUsize);

    impl Wake for Count {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Count {
        pub(crate) fn get(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// Each move runs once, only forward, and every winning move wakes both loops.
    #[test]
    fn moves_once_and_wakes_both() {
        let phase = SharedPhase::new();
        let owner = Arc::new(Count::default());
        let io = Arc::new(Count::default());
        phase.set_owner_waker(owner.clone());
        phase.set_io_waker(io.clone());
        assert!(!phase.stopping());
        assert!(phase.advance(Phase::Running, Phase::Stopping));
        assert!(!phase.advance(Phase::Running, Phase::Stopping));
        assert!(phase.stopping());
        assert_eq!((owner.get(), io.get()), (1, 1));
        assert!(phase.advance(Phase::Stopping, Phase::OwnerDone));
        assert!(phase.advance(Phase::OwnerDone, Phase::SocketRemoved));
        assert!(phase.advance(Phase::SocketRemoved, Phase::LockReleased));
        assert_eq!(phase.get(), Phase::LockReleased);
        assert_eq!((owner.get(), io.get()), (4, 4));
    }

    /// Only the first stop request sends `StopRequested`.
    #[test]
    fn first_stop_request_wins() {
        let phase = SharedPhase::new();
        let (tx, rx) = std::sync::mpsc::sync_channel(LIFECYCLE_CAPACITY);
        assert!(request_stop(&phase, &tx, StopCause::Signal));
        assert!(!request_stop(&phase, &tx, StopCause::Requested));
        assert_eq!(
            rx.try_recv(),
            Ok(Lifecycle::StopRequested(StopCause::Signal))
        );
        assert!(rx.try_recv().is_err());
    }
}
