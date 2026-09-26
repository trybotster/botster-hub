//! Route-specific close-work registry that mirrors the Core wake source.
//!
//! Overflow recovery walks only queued, non-retired route states. It never
//! scans admission maps.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use botster_hub_client::DaemonEvent;

use crate::subscription::closed_events::{ClosedEventLedger, subscription_close_reason};
use crate::transport::shared::close_reason::CloseSource;

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
    /// The adapter's first close source, recorded by its close hook.
    pub source: OnceLock<CloseSource>,
    pub key: RouteCloseKey,
    ledger: ClosedEventLedger,
    wake: Arc<dyn Fn() + Send + Sync>,
}

impl RouteCloseState {
    fn enqueue_closed_event(&self) {
        if self.ledger.generation_is_suppressed(
            &self.key.session_id,
            &self.key.subscription_id,
            self.key.generation,
        ) {
            self.reported.store(true, Ordering::SeqCst);
            return;
        }
        if self.reported.swap(true, Ordering::SeqCst) {
            return;
        }
        let reason = subscription_close_reason(self.source.get().copied());
        self.ledger
            .push_event(DaemonEvent::TerminalSubscriptionClosed {
                session_id: self.key.session_id.clone(),
                subscription_id: self.key.subscription_id.clone(),
                generation: self.key.generation,
                reason,
            });
        (self.wake)();
    }
}

/// The data-plane wake that close work raises after it enqueues a route.
type DataPlaneWake = Arc<OnceLock<Arc<dyn Fn() + Send + Sync>>>;

#[derive(Clone)]
pub(crate) struct CloseWorkHook {
    state: Weak<RouteCloseState>,
    tx: SyncSender<Arc<RouteCloseState>>,
    overflow: Arc<AtomicBool>,
    data_plane_wake: DataPlaneWake,
}

impl CloseWorkHook {
    pub(crate) fn notify_closed(&self, source: CloseSource) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        if state.retired.load(Ordering::Acquire) {
            return;
        }
        let _ = state.source.set(source);
        if state
            .queued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        match self.tx.try_send(Arc::clone(&state)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.overflow.store(true, Ordering::Release);
            }
            Err(TrySendError::Disconnected(_)) => {
                state.queued.store(false, Ordering::Release);
                return;
            }
        }
        // Publish after enqueue: the data plane takes the route on the turn
        // this wake starts.
        if let Some(wake) = self.data_plane_wake.get() {
            wake();
        }
    }

    #[cfg(test)]
    fn notify_closed_through_overflow(&self, source: CloseSource) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let _ = state.source.set(source);
        state.queued.store(true, Ordering::Release);
        self.overflow.store(true, Ordering::Release);
    }
}

struct CloseWorkInner {
    tx: SyncSender<Arc<RouteCloseState>>,
    rx: Mutex<Receiver<Arc<RouteCloseState>>>,
    overflow: Arc<AtomicBool>,
    registry: Mutex<HashMap<RouteCloseKey, Arc<RouteCloseState>>>,
    data_plane_wake: DataPlaneWake,
    /// Test-only: reports every ready-batch take, including empty ones.
    #[cfg(test)]
    take_observer: Mutex<Option<mpsc::Sender<Vec<RouteCloseKey>>>>,
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
                data_plane_wake: Arc::new(OnceLock::new()),
                #[cfg(test)]
                take_observer: Mutex::new(None),
            }),
        }
    }

    /// Binds the wake that every enqueue raises. The data-plane driver binds
    /// it once, when it starts.
    pub(crate) fn bind_data_plane_wake(&self, wake: Arc<dyn Fn() + Send + Sync>) {
        let _ = self.inner.data_plane_wake.set(wake);
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
            source: OnceLock::new(),
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
            data_plane_wake: Arc::clone(&self.inner.data_plane_wake),
        }
    }

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

    #[cfg(test)]
    pub(crate) fn take_batch(&self, max_keys: usize) -> Vec<Arc<RouteCloseState>> {
        self.take_ready_batch(max_keys).0
    }

    /// Takes at most `max_keys` queued routes. The flag reports that queued
    /// routes may remain, so the caller runs another turn without waiting.
    pub(crate) fn take_ready_batch(&self, max_keys: usize) -> (Vec<Arc<RouteCloseState>>, bool) {
        let mut batch = Vec::new();
        let mut seen = std::collections::HashSet::new();
        if let Ok(rx) = self.inner.rx.lock() {
            while batch.len() < max_keys {
                match rx.try_recv() {
                    Ok(state)
                        if !state.retired.load(Ordering::Acquire)
                            && seen.insert(Arc::as_ptr(&state) as usize) =>
                    {
                        batch.push(state)
                    }
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
                if batch.len() < max_keys
                    && state.queued.load(Ordering::Acquire)
                    && seen.insert(Arc::as_ptr(state) as usize)
                {
                    batch.push(Arc::clone(state));
                }
            }
            if registry.values().any(|state| {
                !state.retired.load(Ordering::Acquire)
                    && state.queued.load(Ordering::Acquire)
                    && !seen.contains(&(Arc::as_ptr(state) as usize))
            }) {
                self.inner.overflow.store(true, Ordering::Release);
            }
        }
        let more_ready = batch.len() == max_keys || self.inner.overflow.load(Ordering::Acquire);
        #[cfg(test)]
        if let Ok(observer) = self.inner.take_observer.lock()
            && let Some(observer) = observer.as_ref()
        {
            let _ = observer.send(batch.iter().map(|state| state.key.clone()).collect());
        }
        (batch, more_ready)
    }

    /// Test-only: returns a receiver for every ready-batch take.
    #[cfg(test)]
    pub(crate) fn observe_takes(&self) -> Receiver<Vec<RouteCloseKey>> {
        let (tx, rx) = mpsc::channel();
        *self.inner.take_observer.lock().unwrap() = Some(tx);
        rx
    }

    #[cfg(test)]
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
        live.notify_closed(CloseSource::Core(
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::AdapterClosed,
        ));
        retired.notify_closed(CloseSource::Core(
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::AdapterClosed,
        ));
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

    #[test]
    fn overflow_preserves_queued_routes_after_the_batch_limit() {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let source = CloseWorkSource::new();
        let hooks: Vec<_> = (0..10)
            .map(|generation| {
                source.register(
                    format!("session-{generation}"),
                    "sub".into(),
                    generation,
                    ClosedEventLedger::default(),
                    Arc::clone(&wake),
                )
            })
            .collect();
        for hook in &hooks {
            hook.notify_closed_through_overflow(CloseSource::Core(
                botster_core::contract::terminal_adapter::TerminalRouteCloseReason::AdapterClosed,
            ));
        }

        let first = source.take_batch(8);
        assert_eq!(first.len(), 8);
        for state in first {
            source.retire(
                &state.key.session_id,
                &state.key.subscription_id,
                state.key.generation,
            );
        }
        let second = source.take_batch(8);
        assert_eq!(second.len(), 2);
    }

    #[test]
    fn notify_closed_wakes_the_data_plane_after_the_route_is_queued() {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let source = CloseWorkSource::new();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observer_source = source.clone();
        let observer = Arc::clone(&observed);
        source.bind_data_plane_wake(Arc::new(move || {
            // The wake runs after the enqueue, so the route is already takeable.
            let taken = observer_source.take_batch(8);
            observer
                .lock()
                .unwrap()
                .extend(taken.into_iter().map(|state| state.key.clone()));
        }));
        let hook = source.register(
            "session".into(),
            "sub".into(),
            1,
            ClosedEventLedger::default(),
            wake,
        );

        hook.notify_closed(CloseSource::Core(
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::AdapterClosed,
        ));

        assert_eq!(
            *observed.lock().unwrap(),
            vec![RouteCloseKey {
                session_id: "session".into(),
                subscription_id: "sub".into(),
                generation: 1,
            }]
        );
    }

    #[test]
    fn a_full_batch_reports_that_more_close_work_may_be_ready() {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let source = CloseWorkSource::new();
        let hooks: Vec<_> = (0..3)
            .map(|generation| {
                source.register(
                    format!("session-{generation}"),
                    "sub".into(),
                    generation,
                    ClosedEventLedger::default(),
                    Arc::clone(&wake),
                )
            })
            .collect();
        for hook in &hooks {
            hook.notify_closed(CloseSource::Core(
                botster_core::contract::terminal_adapter::TerminalRouteCloseReason::AdapterClosed,
            ));
        }

        let (first, more) = source.take_ready_batch(2);
        assert_eq!((first.len(), more), (2, true));
        let (second, more) = source.take_ready_batch(2);
        assert_eq!((second.len(), more), (1, false));
    }

    #[test]
    fn retire_returns_the_registry_to_its_baseline() {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let source = CloseWorkSource::new();
        let baseline = source.live_count();
        let hook = source.register(
            "session".into(),
            "sub".into(),
            7,
            ClosedEventLedger::default(),
            wake,
        );
        assert_eq!(source.live_count(), baseline + 1);
        source.retire("session", "sub", 7);
        assert_eq!(source.live_count(), baseline);
        hook.notify_closed(CloseSource::Core(
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::AdapterClosed,
        ));
        assert!(source.take_batch(8).is_empty());
    }

    #[test]
    fn exact_suppression_blocks_the_direct_close_work_path() {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let source = CloseWorkSource::new();
        let ledger = ClosedEventLedger::default();
        let hook = source.register("session".into(), "sub".into(), 7, ledger.clone(), wake);
        ledger.suppress_generation("session", "sub", 7);
        hook.notify_closed(CloseSource::Core(
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::AdapterClosed,
        ));
        let state = source.take_batch(1).pop().expect("queued close work");
        state.report_if_live(true);
        assert!(
            ledger.pop_pending_event().is_none(),
            "exact suppression must cover the direct close-work path"
        );
    }

    #[test]
    fn rolled_back_suppression_preserves_the_direct_close_work_path() {
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        let source = CloseWorkSource::new();
        let ledger = ClosedEventLedger::default();
        let hook = source.register("session".into(), "sub".into(), 7, ledger.clone(), wake);
        ledger.suppress_generation("session", "sub", 7);
        ledger.unsuppress_generation("session", "sub", 7);
        hook.notify_closed(CloseSource::Core(
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::AdapterClosed,
        ));
        let state = source.take_batch(1).pop().expect("queued close work");
        state.report_if_live(true);
        assert!(matches!(
            ledger.pop_pending_event(),
            Some(DaemonEvent::TerminalSubscriptionClosed {
                session_id,
                subscription_id,
                generation: 7,
                ..
            }) if session_id == "session" && subscription_id == "sub"
        ));
    }
}
