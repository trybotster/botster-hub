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
    would_block: AtomicBool,
    slot: Mutex<Option<Vec<u8>>>,
    wake: AdapterWake,
}

impl WebRtcTerminalAdapterInner {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            would_block: AtomicBool::new(false),
            slot: Mutex::new(None),
            wake: AdapterWake::new(),
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
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
    routes: Mutex<Vec<(String, String, WebRtcTerminalAdapterHandle)>>,
}

impl WebRtcConnectionMux {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(WebRtcMuxInner {
                wake: AdapterWake::new(),
                routes: Mutex::new(Vec::new()),
            }),
        }
    }

    pub(crate) fn create_adapter(&self) -> (WebRtcTerminalAdapter, WebRtcTerminalAdapterHandle) {
        WebRtcTerminalAdapter::pair_with_wake(self.inner.wake.clone())
    }

    pub(crate) fn register(
        &self,
        session_id: String,
        subscription_id: String,
        handle: WebRtcTerminalAdapterHandle,
    ) {
        if let Ok(mut routes) = self.inner.routes.lock() {
            routes.retain(|(existing_session, existing_subscription, _)| {
                existing_session != &session_id || existing_subscription != &subscription_id
            });
            routes.push((session_id, subscription_id, handle));
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
        if let Ok(mut routes) = self.inner.routes.lock() {
            for (_, _, handle) in routes.drain(..) {
                handle.close();
            }
        }
        self.inner.wake.wake();
    }

    pub(crate) fn set_would_block(&self, pressured: bool) {
        if let Ok(routes) = self.inner.routes.lock() {
            for (_, _, handle) in routes.iter() {
                handle.set_would_block(pressured);
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
            .filter_map(|(session_id, subscription_id, handle)| {
                handle.snapshot_active().map(|bytes| {
                    (
                        session_id.clone(),
                        subscription_id.clone(),
                        handle.clone(),
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
        mux.register("s".into(), "sub".into(), handle);
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
