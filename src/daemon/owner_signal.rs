//! Cross-thread wakes for Hub owner work (readiness plan, section 2.2).
//!
//! A producer on another thread changes shared state, then calls
//! [`OwnerSignal::raise`] for the key of that state. Each key has an epoch that
//! only moves forward, and one doorbell wakes the owner's wait.
//!
//! A waiting source registers before it looks: it reads [`OwnerSignal::seen`]
//! before its final attempt on the shared state. If that attempt finds no work,
//! the source parks on the [`Seen`] value and becomes ready when the epoch
//! moves. So a raise either comes before the read, and the attempt sees its
//! state, or after it, and the epoch has moved. The doorbell is a
//! [`Notify`], which stores one permit when the owner is not waiting, so a raise
//! between the owner's last check and its wait still wakes it.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use tokio::sync::Notify;

/// One kind of shared state that a producer on another thread changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalKey {
    /// The package event router gained a ready consumer copy.
    PackageEvents,
    /// The package event router's lock was released while a waiter was armed.
    EventRouter,
}

impl SignalKey {
    const COUNT: usize = 2;

    const fn index(self) -> usize {
        match self {
            Self::PackageEvents => 0,
            Self::EventRouter => 1,
        }
    }
}

/// A key's epoch, read before a source's final attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Seen {
    key: SignalKey,
    epoch: u64,
}

/// Epochs and the owner doorbell.
#[derive(Debug, Default)]
pub(crate) struct OwnerSignal {
    epochs: [AtomicU64; SignalKey::COUNT],
    doorbell: Notify,
}

impl OwnerSignal {
    /// Publish a change to `key`'s state. Call it after the change.
    pub(crate) fn raise(&self, key: SignalKey) {
        self.epochs[key.index()].fetch_add(1, Ordering::Release);
        self.doorbell.notify_one();
    }

    /// Read `key`'s epoch. Call it before the final attempt on the state.
    pub(crate) fn seen(&self, key: SignalKey) -> Seen {
        Seen {
            key,
            epoch: self.epochs[key.index()].load(Ordering::Acquire),
        }
    }

    /// Whether `key` was raised after `seen` was read.
    pub(crate) fn moved(&self, seen: Seen) -> bool {
        self.epochs[seen.key.index()].load(Ordering::Acquire) != seen.epoch
    }

    /// Wait for the next raise, or return at once for a stored one.
    pub(crate) async fn rung(&self) {
        self.doorbell.notified().await;
    }
}

/// A mutex shared with the owner that raises its own key when a guard drops
/// while an owner attempt is armed on it.
///
/// The owner tries the lock; on contention it calls [`Self::arm`] and tries once
/// more. If that retry also fails, it parks on the returned [`Seen`]. The
/// holder's guard releases the lock first and then raises, so the retry after
/// the wake can take it. An unarmed release raises nothing, so ordinary use
/// costs no owner wake. A key belongs to this one lock: releasing some other
/// resource can never wake a waiter that is armed here.
#[derive(Debug)]
pub(crate) struct SignalingMutex<T> {
    mutex: Mutex<T>,
    armed: AtomicBool,
    signal: Arc<OwnerSignal>,
    key: SignalKey,
}

impl<T> SignalingMutex<T> {
    pub(crate) fn new(value: T, signal: Arc<OwnerSignal>, key: SignalKey) -> Self {
        Self {
            mutex: Mutex::new(value),
            armed: AtomicBool::new(false),
            signal,
            key,
        }
    }

    /// Block for the lock. A poisoned lock is a fault: its guard is released
    /// without a raise, so a failed attempt never wakes its own armed waiter.
    pub(crate) fn lock(&self) -> Result<SignalingGuard<'_, T>, LockPoisoned> {
        self.mutex
            .lock()
            .map(|guard| self.wrap(guard))
            .map_err(|_| LockPoisoned)
    }

    /// Block for the lock and take it even if poisoned. Only for holders that
    /// deliberately repair or discard state after a panic; the release raises
    /// as any holder's does.
    pub(crate) fn lock_or_recover(&self) -> SignalingGuard<'_, T> {
        self.wrap(
            self.mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Try the lock once. Only [`TryLock::WouldBlock`] is contention.
    pub(crate) fn try_lock(&self) -> Result<SignalingGuard<'_, T>, TryLock> {
        match self.mutex.try_lock() {
            Ok(guard) => Ok(self.wrap(guard)),
            Err(TryLockError::WouldBlock) => Err(TryLock::WouldBlock),
            Err(TryLockError::Poisoned(_)) => Err(TryLock::Poisoned),
        }
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.mutex.is_poisoned()
    }

    /// Register an owner wait on this lock. Call it before the final attempt.
    pub(crate) fn arm(&self) -> Seen {
        let seen = self.signal.seen(self.key);
        self.armed.store(true, Ordering::SeqCst);
        seen
    }

    fn wrap<'a>(&'a self, guard: MutexGuard<'a, T>) -> SignalingGuard<'a, T> {
        SignalingGuard {
            guard: Some(guard),
            owner: self,
        }
    }
}

/// A poisoned [`SignalingMutex`]: a fault, never a wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LockPoisoned;

/// Why [`SignalingMutex::try_lock`] did not lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TryLock {
    /// Another holder has it: contention, which a caller may arm on.
    WouldBlock,
    /// A holder panicked: a fault, which a caller must not wait on.
    Poisoned,
}

/// A [`SignalingMutex`] guard. Dropping it unlocks, then raises if armed.
pub(crate) struct SignalingGuard<'a, T> {
    guard: Option<MutexGuard<'a, T>>,
    owner: &'a SignalingMutex<T>,
}

impl<T> Deref for SignalingGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.guard.as_deref().expect("a live guard holds its lock")
    }
}

impl<T> DerefMut for SignalingGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard
            .as_deref_mut()
            .expect("a live guard holds its lock")
    }
}

impl<T> Drop for SignalingGuard<'_, T> {
    fn drop(&mut self) {
        drop(self.guard.take());
        if self.owner.armed.swap(false, Ordering::SeqCst) {
            self.owner.signal.raise(self.owner.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_raise_after_the_read_moves_the_epoch() {
        let signal = OwnerSignal::default();
        let seen = signal.seen(SignalKey::PackageEvents);
        assert!(!signal.moved(seen));
        signal.raise(SignalKey::PackageEvents);
        assert!(signal.moved(seen));
    }

    #[test]
    fn a_raise_before_the_read_is_not_a_later_change() {
        let signal = OwnerSignal::default();
        signal.raise(SignalKey::PackageEvents);
        let seen = signal.seen(SignalKey::PackageEvents);
        assert!(!signal.moved(seen));
    }

    #[test]
    fn a_release_after_arming_moves_the_lock_key_and_an_unarmed_release_does_not() {
        let signal = Arc::new(OwnerSignal::default());
        let mutex = SignalingMutex::new(0_u8, Arc::clone(&signal), SignalKey::EventRouter);
        let before = signal.seen(SignalKey::EventRouter);
        drop(mutex.try_lock().expect("free lock"));
        assert!(!signal.moved(before), "an unarmed release raises nothing");

        let held = mutex.try_lock().expect("free lock");
        assert!(matches!(mutex.try_lock(), Err(TryLock::WouldBlock)));
        let seen = mutex.arm();
        assert!(matches!(mutex.try_lock(), Err(TryLock::WouldBlock)));
        assert!(
            !signal.moved(seen),
            "the failed retry itself raises nothing"
        );
        drop(held);
        assert!(
            signal.moved(seen),
            "the holder's release wakes the armed waiter"
        );
        assert!(
            mutex.try_lock().is_ok(),
            "the lock is free when the key moves"
        );
    }

    #[test]
    fn a_failed_attempt_on_a_poisoned_lock_never_raises_its_armed_key() {
        let signal = Arc::new(OwnerSignal::default());
        let mutex = SignalingMutex::new(0_u8, Arc::clone(&signal), SignalKey::EventRouter);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.try_lock().expect("free lock");
            panic!("poison the lock");
        }));
        let seen = mutex.arm();
        for _ in 0..3 {
            assert_eq!(mutex.try_lock().err(), Some(TryLock::Poisoned));
            assert_eq!(mutex.lock().err(), Some(LockPoisoned));
        }
        assert!(
            !signal.moved(seen),
            "attempts on a poisoned lock must not wake their own armed waiter"
        );
    }

    #[test]
    fn a_holder_that_panics_while_a_waiter_is_armed_wakes_it_into_the_fault() {
        let signal = Arc::new(OwnerSignal::default());
        let mutex = SignalingMutex::new(0_u8, Arc::clone(&signal), SignalKey::EventRouter);
        let mut seen = None;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.try_lock().expect("free lock");
            seen = Some(mutex.arm());
            panic!("poison the lock while armed");
        }));
        let seen = seen.expect("armed while held");
        assert!(
            signal.moved(seen),
            "the unwinding holder wakes the armed waiter"
        );
        assert_eq!(mutex.try_lock().err(), Some(TryLock::Poisoned));
    }

    #[test]
    fn a_release_before_arming_leaves_the_retry_free_to_lock() {
        let signal = Arc::new(OwnerSignal::default());
        let mutex = SignalingMutex::new(0_u8, Arc::clone(&signal), SignalKey::EventRouter);
        drop(mutex.try_lock().expect("free lock"));
        let seen = mutex.arm();
        let retry = mutex
            .try_lock()
            .expect("a release before arming frees the retry");
        drop(retry);
        // The waiter's own successful retry releases while armed; the key moves,
        // and that only costs one spurious re-evaluation.
        assert!(signal.moved(seen));
    }

    #[test]
    fn a_raise_with_no_waiter_leaves_one_permit_for_the_next_wait() {
        let signal = OwnerSignal::default();
        signal.raise(SignalKey::PackageEvents);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        // The stored permit completes the wait at once; no timer is involved.
        runtime.block_on(signal.rung());
    }
}
