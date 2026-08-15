//! Production WebRTC terminal adapter and Core harness driver.
//!
//! The adapter owns one in-flight write slot. `try_write` serializes an opaque
//! [`TerminalFrame`] and does not inspect snapshot phases or snapshot bodies.
//! `close` and `Drop` return without waiting on DataChannel I/O or a writer lock.

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

/// Wake that stores a permit so a write cannot be lost before the sender waits.
#[derive(Clone)]
struct AdapterWake {
    notify: Arc<Notify>,
    pending: Arc<AtomicBool>,
}

impl AdapterWake {
    fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            pending: Arc::new(AtomicBool::new(false)),
        }
    }

    fn wake(&self) {
        self.pending.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            if self.pending.swap(false, Ordering::SeqCst) {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            if self.pending.swap(false, Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

/// One-slot WebRTC adapter bound to an admitted DataChannel.
pub struct WebRtcTerminalAdapter {
    inner: Arc<WebRtcTerminalAdapterInner>,
}

/// Close-safe handle for the DataChannel writer and Hub route record.
#[derive(Clone)]
pub(crate) struct WebRtcTerminalAdapterHandle {
    inner: Arc<WebRtcTerminalAdapterInner>,
}

struct WebRtcTerminalAdapterInner {
    closed: AtomicBool,
    host_closed: AtomicBool,
    would_block: AtomicBool,
    slot: Mutex<Option<Vec<u8>>>,
    wake: AdapterWake,
}

impl WebRtcTerminalAdapterInner {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            host_closed: AtomicBool::new(false),
            would_block: AtomicBool::new(false),
            slot: Mutex::new(None),
            wake: AdapterWake::new(),
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
        self.wake.wake();
    }

    fn set_would_block(&self, pressured: bool) {
        self.would_block.store(pressured, Ordering::SeqCst);
        self.wake.wake();
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
        self.wake.wake();
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
        self.wake.wake();
        taken
    }
}

impl WebRtcTerminalAdapter {
    /// Create an in-memory adapter for harness and isolated unit tests.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(WebRtcTerminalAdapterInner::new()),
        }
    }

    /// Create the production adapter and the peer-owned write handle.
    #[must_use]
    pub(crate) fn pair() -> (Self, WebRtcTerminalAdapterHandle) {
        Self::pair_with_wake(AdapterWake::new())
    }

    fn pair_with_wake(wake: AdapterWake) -> (Self, WebRtcTerminalAdapterHandle) {
        let mut inner = WebRtcTerminalAdapterInner::new();
        inner.wake = wake;
        let inner = Arc::new(inner);
        (
            Self {
                inner: Arc::clone(&inner),
            },
            WebRtcTerminalAdapterHandle { inner },
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

impl Default for WebRtcTerminalAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WebRtcTerminalAdapter {
    fn drop(&mut self) {
        self.inner.close();
    }
}

impl TerminalAdapter for WebRtcTerminalAdapter {
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

/// Per-peer mux of bound WebRTC adapter write handles.
#[derive(Clone)]
pub(crate) struct WebRtcConnectionMux {
    inner: Arc<WebRtcMuxInner>,
}

impl std::fmt::Debug for WebRtcConnectionMux {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebRtcConnectionMux")
            .finish_non_exhaustive()
    }
}

struct WebRtcMuxInner {
    wake: AdapterWake,
    dying: AtomicBool,
    close_events_admitted: AtomicBool,
    routes: Mutex<Vec<WebRtcMuxRoute>>,
    pending_events: Mutex<Vec<DaemonEvent>>,
    suppress_sessions: Mutex<Vec<String>>,
    suppress_generations: Mutex<Vec<(String, String, u64)>>,
}

struct WebRtcMuxRoute {
    session_id: String,
    subscription_id: String,
    generation: u64,
    handle: WebRtcTerminalAdapterHandle,
    reported: bool,
}

impl WebRtcConnectionMux {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(WebRtcMuxInner {
                wake: AdapterWake::new(),
                dying: AtomicBool::new(false),
                close_events_admitted: AtomicBool::new(false),
                routes: Mutex::new(Vec::new()),
                pending_events: Mutex::new(Vec::new()),
                suppress_sessions: Mutex::new(Vec::new()),
                suppress_generations: Mutex::new(Vec::new()),
            }),
        }
    }

    pub(crate) fn create_adapter(&self) -> (WebRtcTerminalAdapter, WebRtcTerminalAdapterHandle) {
        WebRtcTerminalAdapter::pair_with_wake(self.inner.wake.clone())
    }

    pub(crate) fn admit_close_events(&self) {
        self.inner
            .close_events_admitted
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn close_events_admitted(&self) -> bool {
        self.inner.close_events_admitted.load(Ordering::SeqCst)
    }

    pub(crate) fn register(
        &self,
        session_id: String,
        subscription_id: String,
        generation: u64,
        handle: WebRtcTerminalAdapterHandle,
    ) {
        if let Ok(mut routes) = self.inner.routes.lock() {
            routes.retain(|route| {
                !(route.session_id == session_id
                    && route.subscription_id == subscription_id
                    && route.generation == generation)
            });
            routes.push(WebRtcMuxRoute {
                session_id,
                subscription_id,
                generation,
                handle,
                reported: false,
            });
        }
        self.inner.wake.wake();
    }

    pub(crate) fn has_bound_routes(&self) -> bool {
        self.inner
            .routes
            .lock()
            .is_ok_and(|routes| !routes.is_empty())
    }

    pub(crate) fn close_all(&self) {
        self.inner.dying.store(true, Ordering::SeqCst);
        if let Ok(mut routes) = self.inner.routes.lock() {
            for route in routes.drain(..) {
                route.handle.close();
            }
        }
        self.inner.wake.wake();
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
            self.inner.wake.wake();
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

    pub(crate) fn drop_pending_events(&self) {
        if let Ok(mut pending) = self.inner.pending_events.lock() {
            pending.clear();
        }
    }

    pub(crate) fn set_would_block(&self, pressured: bool) {
        if let Ok(routes) = self.inner.routes.lock() {
            for route in routes.iter() {
                route.handle.set_would_block(pressured);
            }
        }
        self.inner.wake.wake();
    }

    pub(crate) fn snapshot_writes(
        &self,
    ) -> Vec<(String, String, WebRtcTerminalAdapterHandle, Vec<u8>)> {
        let Ok(routes) = self.inner.routes.lock() else {
            return Vec::new();
        };
        routes
            .iter()
            .filter_map(|route| {
                if route.handle.is_closed() {
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

    pub(crate) async fn wait_for_write(&self) {
        self.inner.wake.wait().await;
    }
}

impl WebRtcTerminalAdapterHandle {
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

    pub(crate) fn set_would_block(&self, pressured: bool) {
        self.inner.set_would_block(pressured);
    }

    pub(crate) fn snapshot_active(&self) -> Option<Vec<u8>> {
        self.inner.snapshot_active()
    }

    pub(crate) fn complete_active(&self) -> Option<Vec<u8>> {
        self.inner.complete_active()
    }

    pub(crate) fn write_opaque_frame(&self, frame: &TerminalFrame) {
        let _ = self.inner.try_write(frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use botster_core_test_support::terminal_adapter::{
        TerminalAdapterHarnessDriver, assert_terminal_adapter_conformance,
    };

    struct WebRtcTerminalAdapterDriver {
        adapter: WebRtcTerminalAdapter,
        handle: WebRtcTerminalAdapterHandle,
        delivered: Vec<Vec<u8>>,
    }

    impl Default for WebRtcTerminalAdapterDriver {
        fn default() -> Self {
            let (adapter, handle) = WebRtcTerminalAdapter::pair();
            Self {
                adapter,
                handle,
                delivered: Vec::new(),
            }
        }
    }

    impl TerminalAdapterHarnessDriver for WebRtcTerminalAdapterDriver {
        type Adapter = WebRtcTerminalAdapter;

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
    fn production_webrtc_adapter_passes_core_conformance_harness() {
        let mut driver = WebRtcTerminalAdapterDriver::default();
        assert_terminal_adapter_conformance(&mut driver);
    }

    #[test]
    fn close_does_not_wait_on_occupied_slot() {
        let (mut adapter, handle) = WebRtcTerminalAdapter::pair();
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
    fn completing_twice_does_not_duplicate_the_active_frame() {
        let (mut adapter, handle) = WebRtcTerminalAdapter::pair();
        let frame = TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"once"}"#)
            .expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert!(handle.complete_active().is_some());
        assert!(handle.complete_active().is_none());
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Ready);
    }

    #[test]
    fn wait_observes_a_write_that_happens_after_an_empty_scan() {
        let mux = WebRtcConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter();
        mux.register("s".into(), "sub".into(), 1, handle);
        assert!(
            mux.snapshot_writes().is_empty(),
            "scan is empty before the race write"
        );
        let frame = TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"race"}"#)
            .expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(50), mux.wait_for_write())
                .await
                .expect("write after empty scan must store a wake permit");
        });
        assert_eq!(mux.snapshot_writes().len(), 1);
    }

    #[test]
    fn close_from_host_does_not_rewrite_an_already_closed_handle() {
        let (mut adapter, handle) = WebRtcTerminalAdapter::pair();
        adapter.close();
        handle.close_from_host();
        assert!(handle.is_closed());
        assert!(
            !handle.host_closed(),
            "Core close must keep host_closed false after later host reconciliation"
        );
        let mux = WebRtcConnectionMux::new();
        let (_, live) = mux.create_adapter();
        mux.register("s".into(), "sub".into(), 3, live.clone());
        live.close();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);
        match mux.pop_pending_event() {
            Some(DaemonEvent::TerminalSubscriptionClosed {
                session_id,
                subscription_id,
                generation,
                reason,
            }) => {
                assert_eq!(session_id, "s");
                assert_eq!(subscription_id, "sub");
                assert_eq!(generation, 3);
                assert_eq!(reason, TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER);
            }
            other => panic!("expected core close event, got {other:?}"),
        }
        mux.close_all();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 0);
        assert!(mux.pop_pending_event().is_none());
    }

    #[test]
    fn production_adapter_source_does_not_name_snapshot_phases() {
        let source = include_str!("webrtc_terminal_adapter.rs");
        let production = source.split("mod tests").next().expect("production source");
        for forbidden in [r#""READY""#, r#""PAGE""#, r#""FINISH""#, "GHOSTSNP"] {
            assert!(
                !production.contains(forbidden),
                "webrtc adapter must stay content-blind: found {forbidden}"
            );
        }
    }
}
