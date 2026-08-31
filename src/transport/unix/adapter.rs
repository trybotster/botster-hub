//! Production Unix terminal adapter and Core harness driver.
//!
//! The adapter owns one in-flight write slot. `try_write` serializes an opaque
//! [`TerminalFrame`] and does not inspect snapshot phases or snapshot bodies.
//! `close` and `Drop` return without waiting on socket I/O or a writer lock.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use botster_core::contract::terminal_adapter::{
    TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError, TerminalIngress,
};
use botster_core::contract::terminal_wake::{TerminalWakeSink, WakingTerminalAdapter};
use botster_hub_client::DaemonEvent;
use botster_terminal_protocol::TerminalFrame;
use tokio::sync::Notify;

use crate::data_plane::CloseWorkSource;
use crate::subscription::closed_events::{
    ClosedEventLedger, ClosedEventRoute, ClosedEventSliceProgress, ClosedHandle,
};
use crate::transport::shared::adapter_slot::AdapterSlot;
use crate::transport::shared::wake::NotifyWaiters;

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
    slot: AdapterSlot<NotifyWaiters>,
    deferred: AtomicBool,
}

impl UnixTerminalAdapterInner {
    fn new() -> Self {
        Self {
            slot: AdapterSlot::with_wake_and_close_work(
                NotifyWaiters::new(),
                Arc::new(AtomicBool::new(false)),
            ),
            deferred: AtomicBool::new(false),
        }
    }

    fn is_closed(&self) -> bool {
        self.slot.is_closed()
    }

    fn close_from_host(&self) {
        self.slot.close_from_host();
    }

    #[allow(dead_code)]
    fn host_closed(&self) -> bool {
        self.slot.host_closed()
    }

    fn close(&self) {
        self.slot.close();
    }

    fn pressure(&self) -> TerminalAdapterPressure {
        self.slot.pressure()
    }

    fn try_write(&self, frame: &TerminalFrame) -> Result<(), TerminalAdapterWriteError> {
        self.slot.try_write(frame)
    }

    fn try_read(&self) -> TerminalIngress {
        self.slot.try_read()
    }

    fn snapshot_active(&self) -> Option<Vec<u8>> {
        self.slot.snapshot_active()
    }

    fn defer_flush(&self) {
        self.deferred.store(true, Ordering::SeqCst);
    }

    fn clear_defer_flush(&self) {
        self.deferred.store(false, Ordering::SeqCst);
    }

    fn is_flush_deferred(&self) -> bool {
        self.deferred.load(Ordering::SeqCst)
    }

    fn complete_active(&self) -> Option<Vec<u8>> {
        self.slot.complete_active()
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
        Self::pair_with_notify_and_close_work(notify, Arc::new(AtomicBool::new(false)))
    }

    fn pair_with_notify_and_close_work(
        notify: Arc<Notify>,
        close_work: Arc<AtomicBool>,
    ) -> (Self, UnixTerminalAdapterHandle) {
        let inner = Arc::new(UnixTerminalAdapterInner {
            slot: AdapterSlot::with_wake_and_close_work(
                NotifyWaiters::from_arc(notify),
                close_work,
            ),
            deferred: AtomicBool::new(false),
        });
        (
            Self {
                inner: Arc::clone(&inner),
            },
            UnixTerminalAdapterHandle { inner },
        )
    }

    #[cfg(test)]
    fn force_would_block(&self) {
        self.inner.slot.set_would_block(true);
    }

    #[cfg(test)]
    fn clear_would_block(&self) {
        self.inner.slot.set_would_block(false);
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

    fn try_read(&mut self) -> TerminalIngress {
        self.inner.try_read()
    }
}

impl WakingTerminalAdapter for UnixTerminalAdapter {
    fn set_wake_sink(&mut self, sink: TerminalWakeSink) {
        self.inner.slot.set_wake_sink(sink);
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
    routes: Mutex<BTreeMap<(String, String, u64), ClosedEventRoute<UnixTerminalAdapterHandle>>>,
    closed_events: ClosedEventLedger,
    close_work: Mutex<Arc<AtomicBool>>,
    close_source: Mutex<Option<CloseWorkSource>>,
}

impl UnixConnectionMux {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(UnixMuxInner {
                notify: Arc::new(Notify::new()),
                dying: AtomicBool::new(false),
                routes: Mutex::new(BTreeMap::new()),
                closed_events: ClosedEventLedger::default(),
                close_work: Mutex::new(Arc::new(AtomicBool::new(false))),
                close_source: Mutex::new(None),
            }),
        }
    }

    pub(crate) fn bind_close_work(&self, flag: Arc<AtomicBool>) {
        if let Ok(mut slot) = self.inner.close_work.lock() {
            *slot = flag;
        }
    }

    pub(crate) fn bind_close_source(&self, source: CloseWorkSource) {
        if let Ok(mut slot) = self.inner.close_source.lock() {
            *slot = Some(source);
        }
    }

    pub(crate) fn notify_arc(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.notify)
    }

    pub(crate) fn create_adapter(&self) -> (UnixTerminalAdapter, UnixTerminalAdapterHandle) {
        let close_work = self
            .inner
            .close_work
            .lock()
            .ok()
            .map(|slot| Arc::clone(&*slot))
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        UnixTerminalAdapter::pair_with_notify_and_close_work(self.notify_arc(), close_work)
    }

    pub(crate) fn register(
        &self,
        session_id: String,
        subscription_id: String,
        generation: u64,
        handle: UnixTerminalAdapterHandle,
    ) {
        if let Ok(mut routes) = self.inner.routes.lock() {
            let key = (session_id.clone(), subscription_id.clone(), generation);
            routes.insert(
                key,
                ClosedEventRoute {
                    session_id: session_id.clone(),
                    subscription_id: subscription_id.clone(),
                    generation,
                    handle: handle.clone(),
                    reported: false,
                },
            );
        }
        if let Ok(source) = self.inner.close_source.lock()
            && let Some(source) = source.as_ref()
        {
            let notify = self.notify_arc();
            let hook = source.register(
                session_id,
                subscription_id,
                generation,
                self.inner.closed_events.clone(),
                Arc::new(move || notify.notify_waiters()),
            );
            handle.attach_close_hook(move |host_closed| hook.notify_closed(host_closed));
        }
        self.inner.notify.notify_waiters();
    }

    pub(crate) fn close_all(&self) {
        self.inner.dying.store(true, Ordering::SeqCst);
        if let Ok(mut routes) = self.inner.routes.lock() {
            for (_, route) in std::mem::take(&mut *routes) {
                route.handle.close_from_host();
            }
        }
        self.inner.notify.notify_waiters();
    }

    #[allow(dead_code)]
    pub(crate) fn is_dying(&self) -> bool {
        self.inner.dying.load(Ordering::SeqCst)
    }

    pub(crate) fn suppress_session_route_generations(&self, session_id: &str) {
        let keys = match self.inner.routes.lock() {
            Ok(routes) => routes
                .keys()
                .filter(|(route_session, _, _)| route_session == session_id)
                .cloned()
                .collect::<Vec<_>>(),
            Err(_) => return,
        };
        if keys.is_empty() {
            return;
        }
        self.inner.closed_events.suppress_session_keys(keys);
    }

    pub(crate) fn suppress_generation(
        &self,
        session_id: impl Into<String>,
        subscription_id: impl Into<String>,
        generation: u64,
    ) {
        self.inner
            .closed_events
            .suppress_generation(session_id, subscription_id, generation);
    }

    #[cfg(test)]
    pub(crate) fn queue_closed_subscription_events(
        &self,
        session_is_live: impl Fn(&str) -> bool,
    ) -> usize {
        self.queue_closed_subscription_events_bounded(
            |session_id| Some(session_is_live(session_id)),
            usize::MAX,
            None,
            usize::MAX,
        )
        .classified
    }

    #[allow(dead_code)]
    pub(crate) fn queue_closed_subscription_events_bounded(
        &self,
        classify: impl FnMut(&str) -> Option<bool>,
        max_candidates: usize,
        after_route: Option<&(String, String, u64)>,
        max_entries_visited: usize,
    ) -> ClosedEventSliceProgress {
        let Ok(mut routes) = self.inner.routes.lock() else {
            return ClosedEventSliceProgress {
                classified: 0,
                more: false,
                after_route: None,
            };
        };
        let notify = Arc::clone(&self.inner.notify);
        self.inner
            .closed_events
            .queue_closed_subscription_events_bounded(
                self.is_dying(),
                &mut routes,
                classify,
                max_candidates,
                after_route,
                max_entries_visited,
                || notify.notify_waiters(),
            )
    }

    pub(crate) fn has_pending_event(&self) -> bool {
        self.inner.closed_events.has_pending_event()
    }

    pub(crate) fn pop_pending_event(&self) -> Option<DaemonEvent> {
        self.inner.closed_events.pop_pending_event()
    }

    pub(crate) fn has_unsent_mux_writes(&self) -> bool {
        self.has_pending_event() || self.has_occupied_adapter_slot()
    }

    fn has_occupied_adapter_slot(&self) -> bool {
        let Ok(routes) = self.inner.routes.lock() else {
            return false;
        };
        routes
            .values()
            .any(|route| route.handle.snapshot_active().is_some())
    }

    pub(crate) fn has_bound_routes(&self) -> bool {
        self.inner
            .routes
            .lock()
            .is_ok_and(|routes| !routes.is_empty())
    }

    pub(crate) fn live_handle(
        &self,
        session_id: &str,
        subscription_id: &str,
    ) -> Option<UnixTerminalAdapterHandle> {
        let Ok(routes) = self.inner.routes.lock() else {
            return None;
        };
        routes.values().rev().find_map(|route| {
            if route.session_id == session_id
                && route.subscription_id == subscription_id
                && !route.handle.is_closed()
            {
                Some(route.handle.clone())
            } else {
                None
            }
        })
    }

    pub(crate) fn snapshot_writes(
        &self,
    ) -> Vec<(String, String, UnixTerminalAdapterHandle, Vec<u8>)> {
        let Ok(routes) = self.inner.routes.lock() else {
            return Vec::new();
        };
        routes
            .values()
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

    /// Allow a later flush to retry a route that yielded to a host response.
    pub(crate) fn clear_deferred_flushes(&self) {
        let Ok(routes) = self.inner.routes.lock() else {
            return;
        };
        for route in routes.values() {
            route.handle.clear_defer_flush();
        }
    }
}

impl UnixTerminalAdapterHandle {
    pub(crate) fn close(&self) {
        self.inner.close();
    }

    pub(crate) fn close_from_host(&self) {
        self.inner.close_from_host();
    }

    #[allow(dead_code)]
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

    pub(crate) fn write_opaque_frame(&self, frame: &botster_terminal_protocol::TerminalFrame) {
        let _ = self.inner.try_write(frame);
    }

    pub(crate) fn defer_flush(&self) {
        self.inner.defer_flush();
    }

    pub(crate) fn clear_defer_flush(&self) {
        self.inner.clear_defer_flush();
    }

    pub(crate) fn is_flush_deferred(&self) -> bool {
        self.inner.is_flush_deferred()
    }

    pub(crate) fn attach_close_hook(&self, hook: impl Fn(bool) + Send + Sync + 'static) {
        self.inner.slot.attach_close_hook(hook);
    }

    pub(crate) fn push_ingress(&self, bytes: Vec<u8>) -> Result<(), ()> {
        self.inner.slot.push_ingress(bytes)
    }
}

impl ClosedHandle for UnixTerminalAdapterHandle {
    fn is_closed(&self) -> bool {
        UnixTerminalAdapterHandle::is_closed(self)
    }

    fn host_closed(&self) -> bool {
        UnixTerminalAdapterHandle::host_closed(self)
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

        fn inject_ingress_frame(&mut self, bytes: Vec<u8>) {
            self.adapter.inner.slot.inject_ingress_frame(bytes);
        }

        fn inject_ingress_partial(&mut self, bytes: Vec<u8>) {
            self.adapter.inner.slot.inject_ingress_partial(bytes);
        }

        fn complete_ingress_partial(&mut self) {
            self.adapter.inner.slot.complete_ingress_partial();
        }

        fn drop_buffered_ingress_frame(&mut self) {
            self.adapter.inner.slot.drop_buffered_ingress_frame();
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
        assert!(mux.has_bound_routes());
        mux.clear_deferred_flushes();
        assert_eq!(mux.snapshot_writes().len(), 1);
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
    fn close_event_slice_bounds_open_and_reported_prefixes() {
        let mux = UnixConnectionMux::new();
        let mut open_adapters = Vec::new();
        for index in 0..8 {
            let (adapter, handle) = mux.create_adapter();
            mux.register(format!("open-{index:02}"), "sub".to_string(), 1, handle);
            open_adapters.push(adapter);
        }
        let mut reported_handles = Vec::new();
        for index in 0..4 {
            let (_adapter, handle) = mux.create_adapter();
            mux.register(
                format!("reported-{index:02}"),
                "sub".to_string(),
                1,
                handle.clone(),
            );
            handle.close();
            reported_handles.push(handle);
        }
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 4);
        let (_closed_adapter, closed) = mux.create_adapter();
        mux.register("z-closed".to_string(), "sub".to_string(), 1, closed.clone());
        closed.close();
        let first = mux.queue_closed_subscription_events_bounded(|_| Some(true), 8, None, 8);
        assert_eq!(first.classified, 0);
        assert!(first.more);
        assert_eq!(
            first.after_route.as_ref().map(|key| key.0.as_str()),
            Some("open-07")
        );
        let second = mux.queue_closed_subscription_events_bounded(
            |_| Some(true),
            8,
            first.after_route.as_ref(),
            8,
        );
        assert_eq!(second.classified, 1);
        assert!(!second.more);
        assert!(mux.pop_pending_event().is_some());
        let _ = (open_adapters, reported_handles);
    }

    #[test]
    fn production_adapter_source_does_not_name_snapshot_phases() {
        let source = include_str!("adapter.rs");
        let production = source.split("mod tests").next().expect("production source");
        for forbidden in [r#""READY""#, r#""PAGE""#, r#""FINISH""#, "GHOSTSNP"] {
            assert!(
                !production.contains(forbidden),
                "unix adapter must stay content-blind: found {forbidden}"
            );
        }
    }
}
