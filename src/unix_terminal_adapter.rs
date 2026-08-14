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
    would_block: AtomicBool,
    slot: Mutex<Option<Vec<u8>>>,
    notify: Arc<Notify>,
}

impl UnixTerminalAdapterInner {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            would_block: AtomicBool::new(false),
            slot: Mutex::new(None),
            notify: Arc::new(Notify::new()),
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
    routes: Mutex<Vec<(String, String, UnixTerminalAdapterHandle)>>,
}

impl UnixConnectionMux {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(UnixMuxInner {
                notify: Arc::new(Notify::new()),
                routes: Mutex::new(Vec::new()),
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
        handle: UnixTerminalAdapterHandle,
    ) {
        if let Ok(mut routes) = self.inner.routes.lock() {
            routes.retain(|(existing_session, existing_subscription, _)| {
                existing_session != &session_id || existing_subscription != &subscription_id
            });
            routes.push((session_id, subscription_id, handle));
        }
        self.inner.notify.notify_waiters();
    }

    pub(crate) fn close_all(&self) {
        if let Ok(mut routes) = self.inner.routes.lock() {
            for (_, _, handle) in routes.drain(..) {
                handle.close();
            }
        }
        self.inner.notify.notify_waiters();
    }

    pub(crate) fn snapshot_writes(
        &self,
    ) -> Vec<(String, String, UnixTerminalAdapterHandle, Vec<u8>)> {
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

    pub(crate) fn notify(&self) -> &Notify {
        self.inner.notify.as_ref()
    }
}

impl UnixTerminalAdapterHandle {
    pub(crate) fn close(&self) {
        self.inner.close();
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
