//! Route-specific close-work registry that mirrors the Core wake source.
//!
//! Overflow recovery walks only queued, non-retired route states. It never
//! scans admission maps.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, Weak};

use botster_hub_client::{
    DaemonEvent, TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER,
    TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER,
};

use crate::subscription::closed_events::ClosedEventLedger;

/// Bounded ready-channel capacity. Matches the Core wake channel.
pub(crate) const CLOSE_WORK_QUEUE_CAPACITY: usize = 64;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct RouteCloseKey {
    pub session_id: String,
    pub subscription_id: String,
    pub generation: u64,
}

pub(crate) struct RouteCloseState {
    pub queued: AtomicBool,
    pub retired: AtomicBool,
    pub reported: AtomicBool,
    pub host_closed: AtomicBool,
    pub key: RouteCloseKey,
    ledger: ClosedEventLedger,
    wake: Arc<dyn Fn() + Send + Sync>,
}

impl RouteCloseState {
    fn enqueue_closed_event(&self) {
        if self.reported.swap(true, Ordering::SeqCst) {
            return;
        }
        let reason = if self.host_closed.load(Ordering::SeqCst) {
            TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER
        } else {
            TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER
        };
        self.ledger
            .push_event(DaemonEvent::TerminalSubscriptionClosed {
                session_id: self.key.session_id.clone(),
                subscription_id: self.key.subscription_id.clone(),
                generation: self.key.generation,
                reason: reason.to_string(),
            });
        (self.wake)();
    }
}

#[derive(Clone)]
pub(crate) struct CloseWorkHook {
    state: Weak<RouteCloseState>,
    tx: SyncSender<Arc<RouteCloseState>>,
    overflow: Arc<AtomicBool>,
}

impl CloseWorkHook {
    pub(crate) fn notify_closed(&self, host_closed: bool) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        if state.retired.load(Ordering::Acquire) {
            return;
        }
        state.host_closed.store(host_closed, Ordering::SeqCst);
        if state
            .queued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if force_close_work_overflow() {
            self.overflow.store(true, Ordering::Release);
            return;
        }
        match self.tx.try_send(Arc::clone(&state)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.overflow.store(true, Ordering::Release);
            }
            Err(TrySendError::Disconnected(_)) => {
                state.queued.store(false, Ordering::Release);
            }
        }
    }
}

struct CloseWorkInner {
    tx: SyncSender<Arc<RouteCloseState>>,
    rx: Mutex<Receiver<Arc<RouteCloseState>>>,
    overflow: Arc<AtomicBool>,
    registry: Mutex<HashMap<RouteCloseKey, Arc<RouteCloseState>>>,
}

#[derive(Clone)]
pub(crate) struct CloseWorkSource {
    inner: Arc<CloseWorkInner>,
}

impl CloseWorkSource {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::sync_channel(CLOSE_WORK_QUEUE_CAPACITY);
        Self {
            inner: Arc::new(CloseWorkInner {
                tx,
                rx: Mutex::new(rx),
                overflow: Arc::new(AtomicBool::new(false)),
                registry: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) fn register(
        &self,
        session_id: String,
        subscription_id: String,
        generation: u64,
        ledger: ClosedEventLedger,
        wake: Arc<dyn Fn() + Send + Sync>,
    ) -> CloseWorkHook {
        let key = RouteCloseKey {
            session_id,
            subscription_id,
            generation,
        };
        let state = Arc::new(RouteCloseState {
            queued: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            reported: AtomicBool::new(false),
            host_closed: AtomicBool::new(false),
            key: key.clone(),
            ledger,
            wake,
        });
        if let Ok(mut registry) = self.inner.registry.lock()
            && let Some(previous) = registry.insert(key, Arc::clone(&state))
        {
            previous.retired.store(true, Ordering::Release);
        }
        CloseWorkHook {
            state: Arc::downgrade(&state),
            tx: self.inner.tx.clone(),
            overflow: Arc::clone(&self.inner.overflow),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn retire(&self, session_id: &str, subscription_id: &str, generation: u64) {
        let key = RouteCloseKey {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
            generation,
        };
        if let Ok(mut registry) = self.inner.registry.lock()
            && let Some(state) = registry.remove(&key)
        {
            state.retired.store(true, Ordering::Release);
        }
    }

    pub(crate) fn take_batch(&self, max_keys: usize) -> Vec<Arc<RouteCloseState>> {
        let mut batch = Vec::new();
        if let Ok(rx) = self.inner.rx.lock() {
            while batch.len() < max_keys {
                match rx.try_recv() {
                    Ok(state) if !state.retired.load(Ordering::Acquire) => batch.push(state),
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        }
        if self.inner.overflow.swap(false, Ordering::AcqRel)
            && let Ok(registry) = self.inner.registry.lock()
        {
            for state in registry.values() {
                if state.retired.load(Ordering::Acquire) {
                    continue;
                }
                if state.queued.load(Ordering::Acquire) {
                    batch.push(Arc::clone(state));
                }
            }
        }
        let mut seen = std::collections::HashSet::new();
        batch
            .into_iter()
            .filter(|state| seen.insert(Arc::as_ptr(state) as usize))
            .take(max_keys)
            .collect()
    }

    pub(crate) fn live_count(&self) -> usize {
        self.inner
            .registry
            .lock()
            .map(|registry| registry.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn force_overflow_for_test(&self) {
        self.inner.overflow.store(true, Ordering::Release);
    }
}

impl Default for CloseWorkSource {
    fn default() -> Self {
        Self::new()
    }
}

impl RouteCloseState {
    pub(crate) fn take_queued(&self) -> bool {
        self.queued.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn report_if_live(&self, emit: bool) {
        if self.retired.load(Ordering::Acquire) {
            return;
        }
        if !self.take_queued() && self.reported.load(Ordering::Acquire) {
            return;
        }
        if emit {
            self.enqueue_closed_event();
        } else {
            self.reported.store(true, Ordering::SeqCst);
        }
    }
}

fn force_close_work_overflow() -> bool {
    std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
        && std::env::var("BOTSTER_HUB_TEST_FORCE_CLOSE_WORK_OVERFLOW").as_deref() == Ok("1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_recovers_only_queued_non_retired_routes() {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let source = CloseWorkSource::new();
        let idle = source.register(
            "idle".into(),
            "sub".into(),
            1,
            ClosedEventLedger::default(),
            Arc::clone(&wake),
        );
        let live = source.register(
            "live".into(),
            "sub".into(),
            2,
            ClosedEventLedger::default(),
            Arc::clone(&wake),
        );
        let retired = source.register(
            "dead".into(),
            "sub".into(),
            3,
            ClosedEventLedger::default(),
            Arc::clone(&wake),
        );
        live.notify_closed(false);
        retired.notify_closed(false);
        source.retire("dead", "sub", 3);
        source.force_overflow_for_test();
        let batch = source.take_batch(8);
        let keys: Vec<_> = batch.iter().map(|state| state.key.clone()).collect();
        assert!(
            keys.iter()
                .any(|key| key.session_id == "live" && key.generation == 2)
        );
        assert!(!keys.iter().any(|key| key.session_id == "idle"));
        assert!(!keys.iter().any(|key| key.session_id == "dead"));
        drop(idle);
    }
}
