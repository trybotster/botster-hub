//! Production Unix terminal adapter and Core harness driver.
//!
//! The adapter owns one in-flight write slot. `try_write` serializes an opaque
//! [`TerminalFrame`] and does not inspect snapshot phases or snapshot bodies.
//! `close` and `Drop` return without waiting on socket I/O or a writer lock.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use botster_core::contract::terminal_adapter::{
    TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError,
};
use botster_hub_client::{
    DaemonEvent, TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER,
    TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER,
};
use botster_terminal_protocol::TerminalFrame;
use tokio::sync::Notify;

/// One-slot Unix adapter bound to an admitted control connection.
pub struct UnixTerminalAdapter {
    inner: Arc<UnixTerminalAdapterInner>,
}

/// Close-safe handle for the connection writer and Hub route record.
#[derive(Clone)]
pub(crate) struct UnixTerminalAdapterHandle {
    inner: Arc<UnixTerminalAdapterInner>,
}

struct UnixTerminalAdapterInner {
    closed: AtomicBool,
    host_closed: AtomicBool,
    would_block: AtomicBool,
    deferred: AtomicBool,
    slot: Mutex<Option<Vec<u8>>>,
    notify: Arc<Notify>,
}

impl UnixTerminalAdapterInner {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            host_closed: AtomicBool::new(false),
            would_block: AtomicBool::new(false),
            deferred: AtomicBool::new(false),
            slot: Mutex::new(None),
            notify: Arc::new(Notify::new()),
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn close_from_host(&self) {
        if !self.is_closed() {
            self.host_closed.store(true, Ordering::SeqCst);
        }
        self.close();
    }

    fn host_closed(&self) -> bool {
        self.host_closed.load(Ordering::SeqCst)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        match self.slot.try_lock() {
            Ok(mut slot) => {
                *slot = None;
            }
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => {
                *poisoned.into_inner() = None;
            }
        }
        self.notify.notify_waiters();
    }

    fn pressure(&self) -> TerminalAdapterPressure {
        if self.is_closed() {
            return TerminalAdapterPressure::Closed;
        }
        match self.slot.try_lock() {
            Ok(slot) => {
                if slot.is_some() {
                    TerminalAdapterPressure::Full
                } else if self.would_block.load(Ordering::SeqCst) {
                    TerminalAdapterPressure::WouldBlock
                } else {
                    TerminalAdapterPressure::Ready
                }
            }
            Err(TryLockError::WouldBlock) => TerminalAdapterPressure::Full,
            Err(TryLockError::Poisoned(_)) => TerminalAdapterPressure::Closed,
        }
    }

    fn try_write(&self, frame: &TerminalFrame) -> Result<(), TerminalAdapterWriteError> {
        if self.is_closed() {
            return Err(TerminalAdapterWriteError::Closed);
        }
        if self.would_block.load(Ordering::SeqCst) {
            return Err(TerminalAdapterWriteError::WouldBlock);
        }
        let bytes = match frame.to_bytes() {
            Ok(bytes) => bytes,
            Err(_) => {
                self.close();
                return Err(TerminalAdapterWriteError::Closed);
            }
        };
        let mut slot = match self.slot.try_lock() {
            Ok(slot) => slot,
            Err(TryLockError::WouldBlock) => return Err(TerminalAdapterWriteError::Full),
            Err(TryLockError::Poisoned(_)) => {
                self.close();
                return Err(TerminalAdapterWriteError::Closed);
            }
        };
        if self.is_closed() {
            *slot = None;
            return Err(TerminalAdapterWriteError::Closed);
        }
        if slot.is_some() {
            return Err(TerminalAdapterWriteError::Full);
        }
        *slot = Some(bytes);
        drop(slot);
        self.notify.notify_waiters();
        Ok(())
    }

    fn snapshot_active(&self) -> Option<Vec<u8>> {
        if self.is_closed() {
            match self.slot.try_lock() {
                Ok(mut slot) => *slot = None,
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Poisoned(poisoned)) => {
                    *poisoned.into_inner() = None;
                }
            }
            return None;
        }
        match self.slot.try_lock() {
            Ok(slot) => {
                if self.is_closed() {
                    return None;
                }
                slot.clone()
            }
            Err(_) => None,
        }
    }

    fn defer_flush(&self) {
        self.deferred.store(true, Ordering::SeqCst);
    }

    fn is_flush_deferred(&self) -> bool {
        self.deferred.load(Ordering::SeqCst)
    }

    fn complete_active(&self) -> Option<Vec<u8>> {
        if self.is_closed() {
            return None;
        }
        let taken = match self.slot.try_lock() {
            Ok(mut slot) => {
                if self.is_closed() {
                    *slot = None;
                    None
                } else {
                    slot.take()
                }
            }
            Err(_) => None,
        };
        self.notify.notify_waiters();
        taken
    }
}

impl UnixTerminalAdapter {
    /// Create an in-memory adapter for harness and isolated unit tests.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(UnixTerminalAdapterInner::new()),
        }
    }

    /// Create the production adapter and the connection-owned write handle.
    #[must_use]
    pub(crate) fn pair() -> (Self, UnixTerminalAdapterHandle) {
        Self::pair_with_notify(Arc::new(Notify::new()))
    }

    /// Create an adapter that wakes `notify` on write or close.
    #[must_use]
    pub(crate) fn pair_with_notify(notify: Arc<Notify>) -> (Self, UnixTerminalAdapterHandle) {
        let mut inner = UnixTerminalAdapterInner::new();
        inner.notify = notify;
        let inner = Arc::new(inner);
        (
            Self {
                inner: Arc::clone(&inner),
            },
            UnixTerminalAdapterHandle { inner },
        )
    }

    #[cfg(test)]
    fn force_would_block(&self) {
        self.inner.would_block.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn clear_would_block(&self) {
        self.inner.would_block.store(false, Ordering::SeqCst);
    }
}

impl Default for UnixTerminalAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for UnixTerminalAdapter {
    fn drop(&mut self) {
        self.inner.close();
    }
}

impl TerminalAdapter for UnixTerminalAdapter {
    fn try_write(&mut self, frame: &TerminalFrame) -> Result<(), TerminalAdapterWriteError> {
        self.inner.try_write(frame)
    }

    fn close(&mut self) {
        self.inner.close();
    }

    fn pressure(&self) -> TerminalAdapterPressure {
        self.inner.pressure()
    }
}

/// Per-connection mux of bound Unix adapter write handles.
#[derive(Clone)]
pub(crate) struct UnixConnectionMux {
    inner: Arc<UnixMuxInner>,
}

impl std::fmt::Debug for UnixConnectionMux {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UnixConnectionMux")
            .finish_non_exhaustive()
    }
}

struct UnixMuxInner {
    notify: Arc<Notify>,
    dying: AtomicBool,
    routes: Mutex<Vec<UnixMuxRoute>>,
    pending_events: Mutex<Vec<DaemonEvent>>,
    suppress_sessions: Mutex<Vec<String>>,
    suppress_generations: Mutex<Vec<(String, String, u64)>>,
}

struct UnixMuxRoute {
    session_id: String,
    subscription_id: String,
    generation: u64,
    handle: UnixTerminalAdapterHandle,
    reported: bool,
}

impl UnixConnectionMux {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(UnixMuxInner {
                notify: Arc::new(Notify::new()),
                dying: AtomicBool::new(false),
                routes: Mutex::new(Vec::new()),
                pending_events: Mutex::new(Vec::new()),
                suppress_sessions: Mutex::new(Vec::new()),
                suppress_generations: Mutex::new(Vec::new()),
            }),
        }
    }

    pub(crate) fn notify_arc(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.notify)
    }

    pub(crate) fn create_adapter(&self) -> (UnixTerminalAdapter, UnixTerminalAdapterHandle) {
        UnixTerminalAdapter::pair_with_notify(self.notify_arc())
    }

    pub(crate) fn register(
        &self,
        session_id: String,
        subscription_id: String,
        generation: u64,
        handle: UnixTerminalAdapterHandle,
    ) {
        if let Ok(mut routes) = self.inner.routes.lock() {
            routes.retain(|route| {
                !(route.session_id == session_id
                    && route.subscription_id == subscription_id
                    && route.generation == generation)
            });
            routes.push(UnixMuxRoute {
                session_id,
                subscription_id,
                generation,
                handle,
                reported: false,
            });
        }
        self.inner.notify.notify_waiters();
    }

    pub(crate) fn close_all(&self) {
        self.inner.dying.store(true, Ordering::SeqCst);
        if let Ok(mut routes) = self.inner.routes.lock() {
            for route in routes.drain(..) {
                route.handle.close_from_host();
            }
        }
        self.inner.notify.notify_waiters();
    }

    pub(crate) fn is_dying(&self) -> bool {
        self.inner.dying.load(Ordering::SeqCst)
    }

    pub(crate) fn suppress_session(&self, session_id: impl Into<String>) {
        if let Ok(mut sessions) = self.inner.suppress_sessions.lock() {
            let session_id = session_id.into();
            if !sessions.iter().any(|existing| existing == &session_id) {
                sessions.push(session_id);
            }
        }
    }

    pub(crate) fn suppress_generation(
        &self,
        session_id: impl Into<String>,
        subscription_id: impl Into<String>,
        generation: u64,
    ) {
        if let Ok(mut generations) = self.inner.suppress_generations.lock() {
            let key = (session_id.into(), subscription_id.into(), generation);
            if !generations.iter().any(|existing| existing == &key) {
                generations.push(key);
            }
        }
    }

    pub(crate) fn queue_closed_subscription_events(
        &self,
        session_is_live: impl Fn(&str) -> bool,
    ) -> usize {
        if self.is_dying() {
            if let Ok(mut routes) = self.inner.routes.lock() {
                for route in routes.iter_mut() {
                    route.reported = true;
                }
            }
            return 0;
        }
        let suppressed_sessions = self
            .inner
            .suppress_sessions
            .lock()
            .map(|sessions| sessions.clone())
            .unwrap_or_default();
        let suppressed_generations = self
            .inner
            .suppress_generations
            .lock()
            .map(|generations| generations.clone())
            .unwrap_or_default();
        let mut queued = Vec::new();
        if let Ok(mut routes) = self.inner.routes.lock() {
            for route in routes.iter_mut() {
                if route.reported || !route.handle.is_closed() {
                    continue;
                }
                route.reported = true;
                if suppressed_sessions
                    .iter()
                    .any(|session| session == &route.session_id)
                    || suppressed_generations.iter().any(|key| {
                        key.0 == route.session_id
                            && key.1 == route.subscription_id
                            && key.2 == route.generation
                    })
                    || !session_is_live(&route.session_id)
                {
                    continue;
                }
                let reason = if route.handle.host_closed() {
                    TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER
                } else {
                    TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER
                };
                queued.push(DaemonEvent::TerminalSubscriptionClosed {
                    session_id: route.session_id.clone(),
                    subscription_id: route.subscription_id.clone(),
                    generation: route.generation,
                    reason: reason.to_string(),
                });
            }
        }
        let count = queued.len();
        if count > 0 {
            if let Ok(mut pending) = self.inner.pending_events.lock() {
                pending.extend(queued);
            }
            self.inner.notify.notify_waiters();
        }
        count
    }

    pub(crate) fn pop_pending_event(&self) -> Option<DaemonEvent> {
        self.inner
            .pending_events
            .lock()
            .ok()
            .and_then(|mut pending| {
                if pending.is_empty() {
                    None
                } else {
                    Some(pending.remove(0))
                }
            })
    }

    pub(crate) fn has_unsent_mux_writes(&self) -> bool {
        let pending = self
            .inner
            .pending_events
            .lock()
            .is_ok_and(|pending| !pending.is_empty());
        pending || !self.snapshot_writes().is_empty()
    }

    pub(crate) fn snapshot_writes(
        &self,
    ) -> Vec<(String, String, UnixTerminalAdapterHandle, Vec<u8>)> {
        let Ok(routes) = self.inner.routes.lock() else {
            return Vec::new();
        };
        routes
            .iter()
            .filter_map(|route| {
                if route.handle.is_flush_deferred() {
                    return None;
                }
                route.handle.snapshot_active().map(|bytes| {
                    (
                        route.session_id.clone(),
                        route.subscription_id.clone(),
                        route.handle.clone(),
                        bytes,
                    )
                })
            })
            .collect()
    }

    pub(crate) fn notify(&self) -> &Notify {
        self.inner.notify.as_ref()
    }
}

impl UnixTerminalAdapterHandle {
    pub(crate) fn close(&self) {
        self.inner.close();
    }

    pub(crate) fn close_from_host(&self) {
        self.inner.close_from_host();
    }

    pub(crate) fn host_closed(&self) -> bool {
        self.inner.host_closed()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub(crate) fn snapshot_active(&self) -> Option<Vec<u8>> {
        self.inner.snapshot_active()
    }

    pub(crate) fn complete_active(&self) -> Option<Vec<u8>> {
        self.inner.complete_active()
    }

    pub(crate) fn defer_flush(&self) {
        self.inner.defer_flush();
    }

    pub(crate) fn is_flush_deferred(&self) -> bool {
        self.inner.is_flush_deferred()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core_test_support::terminal_adapter::{
        TerminalAdapterHarnessDriver, assert_terminal_adapter_conformance,
    };

    struct UnixTerminalAdapterDriver {
        adapter: UnixTerminalAdapter,
        handle: UnixTerminalAdapterHandle,
        delivered: Vec<Vec<u8>>,
    }

    impl Default for UnixTerminalAdapterDriver {
        fn default() -> Self {
            let (adapter, handle) = UnixTerminalAdapter::pair();
            Self {
                adapter,
                handle,
                delivered: Vec::new(),
            }
        }
    }

    impl TerminalAdapterHarnessDriver for UnixTerminalAdapterDriver {
        type Adapter = UnixTerminalAdapter;

        fn adapter(&mut self) -> &mut Self::Adapter {
            &mut self.adapter
        }

        fn force_would_block(&mut self) {
            self.adapter.force_would_block();
        }

        fn clear_would_block(&mut self) {
            self.adapter.clear_would_block();
        }

        fn complete_active_write(&mut self) {
            if let Some(bytes) = self.handle.complete_active() {
                self.delivered.push(bytes);
            }
        }

        fn force_closed(&mut self) {
            self.handle.close();
        }

        fn delivered_frame_bytes(&self) -> &[Vec<u8>] {
            &self.delivered
        }
    }

    #[test]
    fn production_unix_adapter_passes_core_conformance_harness() {
        let mut driver = UnixTerminalAdapterDriver::default();
        assert_terminal_adapter_conformance(&mut driver);
    }

    #[test]
    fn host_close_after_core_close_does_not_claim_host_reason() {
        let (mut adapter, handle) = UnixTerminalAdapter::pair();
        adapter.close();
        assert!(handle.is_closed());
        assert!(!handle.host_closed());
        handle.close_from_host();
        assert!(handle.is_closed());
        assert!(
            !handle.host_closed(),
            "a later host sweep must not rewrite Core close as host_adapter_closed"
        );
    }

    #[test]
    fn deferred_route_is_omitted_from_snapshot_writes() {
        let mux = UnixConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter();
        mux.register("stall".to_string(), "sub".to_string(), 1, handle.clone());
        let frame = TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"flood"}"#)
            .expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert_eq!(mux.snapshot_writes().len(), 1);
        handle.defer_flush();
        assert!(mux.snapshot_writes().is_empty());
        assert!(handle.snapshot_active().is_some());
    }

    #[test]
    fn close_does_not_wait_on_occupied_slot() {
        let (mut adapter, handle) = UnixTerminalAdapter::pair();
        let frame =
            TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"in-flight"}"#)
                .expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Full);
        handle.close();
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Closed);
        assert!(handle.snapshot_active().is_none());
        assert!(handle.complete_active().is_none());
        assert!(handle.snapshot_active().is_none());
    }

    #[test]
    fn production_adapter_source_does_not_name_snapshot_phases() {
        let source = include_str!("unix_terminal_adapter.rs");
        let production = source.split("mod tests").next().expect("production source");
        for forbidden in [r#""READY""#, r#""PAGE""#, r#""FINISH""#, "GHOSTSNP"] {
            assert!(
                !production.contains(forbidden),
                "unix adapter must stay content-blind: found {forbidden}"
            );
        }
    }
}
