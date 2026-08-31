//! Production WebRTC terminal adapter and Core harness driver.
//!
//! The adapter owns one in-flight write slot. `try_write` serializes an opaque
//! [`TerminalFrame`] and does not inspect snapshot phases or snapshot bodies.
//! `close` and `Drop` return without waiting on DataChannel I/O or a writer lock.

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

use crate::data_plane::CloseWorkSource;
use crate::subscription::closed_events::{
    ClosedEventLedger, ClosedEventRoute, ClosedEventSliceProgress, ClosedHandle,
};
use crate::transport::shared::adapter_slot::AdapterSlot;
use crate::transport::shared::wake::AdapterWake;

/// One-slot WebRTC adapter bound to an admitted DataChannel.
pub struct WebRtcTerminalAdapter {
    inner: Arc<WebRtcTerminalAdapterInner>,
}

/// Close-safe handle for the DataChannel writer and Hub route record.
#[derive(Clone)]
pub(crate) struct WebRtcTerminalAdapterHandle {
    inner: Arc<WebRtcTerminalAdapterInner>,
}

impl std::fmt::Debug for WebRtcTerminalAdapterHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebRtcTerminalAdapterHandle")
            .finish_non_exhaustive()
    }
}

struct WebRtcTerminalAdapterInner {
    slot: AdapterSlot<AdapterWake>,
}

impl WebRtcTerminalAdapterInner {
    fn new() -> Self {
        Self {
            slot: AdapterSlot::with_wake_and_close_work(
                AdapterWake::new(),
                Arc::new(AtomicBool::new(false)),
            ),
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

    fn set_would_block(&self, pressured: bool) {
        self.slot.set_would_block(pressured);
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

    fn complete_active(&self) -> Option<Vec<u8>> {
        self.slot.complete_active()
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
        Self::pair_with_wake_and_close_work(wake, Arc::new(AtomicBool::new(false)))
    }

    fn pair_with_wake_and_close_work(
        wake: AdapterWake,
        close_work: Arc<AtomicBool>,
    ) -> (Self, WebRtcTerminalAdapterHandle) {
        let inner = Arc::new(WebRtcTerminalAdapterInner {
            slot: AdapterSlot::with_wake_and_close_work(wake, close_work),
        });
        (
            Self {
                inner: Arc::clone(&inner),
            },
            WebRtcTerminalAdapterHandle { inner },
        )
    }

    #[cfg(test)]
    fn force_would_block(&self) {
        self.inner.set_would_block(true);
    }

    #[cfg(test)]
    fn clear_would_block(&self) {
        self.inner.set_would_block(false);
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

    fn try_read(&mut self) -> TerminalIngress {
        self.inner.try_read()
    }
}

impl WakingTerminalAdapter for WebRtcTerminalAdapter {
    fn set_wake_sink(&mut self, sink: TerminalWakeSink) {
        self.inner.slot.set_wake_sink(sink);
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
    routes: Mutex<BTreeMap<(String, String, u64), ClosedEventRoute<WebRtcTerminalAdapterHandle>>>,
    closed_events: ClosedEventLedger,
    close_work: Mutex<Arc<AtomicBool>>,
    close_source: Mutex<Option<CloseWorkSource>>,
}

impl WebRtcConnectionMux {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(WebRtcMuxInner {
                wake: AdapterWake::new(),
                dying: AtomicBool::new(false),
                close_events_admitted: AtomicBool::new(false),
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

    pub(crate) fn create_adapter(&self) -> (WebRtcTerminalAdapter, WebRtcTerminalAdapterHandle) {
        let close_work = self
            .inner
            .close_work
            .lock()
            .ok()
            .map(|slot| Arc::clone(&*slot))
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        WebRtcTerminalAdapter::pair_with_wake_and_close_work(AdapterWake::new(), close_work)
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
            let wake = self.inner.wake.clone();
            let hook = source.register(
                session_id,
                subscription_id,
                generation,
                self.inner.closed_events.clone(),
                Arc::new(move || wake.wake()),
            );
            handle.attach_close_hook(move |host_closed| hook.notify_closed(host_closed));
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
            for (_, route) in std::mem::take(&mut *routes) {
                route.handle.close();
            }
        }
        self.inner.wake.wake();
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
        let wake = self.inner.wake.clone();
        self.inner
            .closed_events
            .queue_closed_subscription_events_bounded(
                self.is_dying(),
                &mut routes,
                classify,
                max_candidates,
                after_route,
                max_entries_visited,
                || wake.wake(),
            )
    }

    pub(crate) fn has_pending_event(&self) -> bool {
        self.inner.closed_events.has_pending_event()
    }

    pub(crate) fn pop_pending_event(&self) -> Option<DaemonEvent> {
        self.inner.closed_events.pop_pending_event()
    }

    pub(crate) fn push_host_event(&self, event: DaemonEvent) {
        self.inner.closed_events.push_event(event);
        self.inner.wake.wake();
    }

    #[allow(dead_code)]
    pub(crate) fn live_handle(
        &self,
        session_id: &str,
        subscription_id: &str,
    ) -> Option<WebRtcTerminalAdapterHandle> {
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

    pub(crate) fn drop_pending_events(&self) {
        self.inner.closed_events.drop_pending_events();
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot_writes(
        &self,
    ) -> Vec<(String, String, WebRtcTerminalAdapterHandle, Vec<u8>)> {
        let Ok(routes) = self.inner.routes.lock() else {
            return Vec::new();
        };
        routes
            .values()
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
    pub(crate) async fn wait_for_write(&self) {
        self.inner.slot.wait_for_write().await;
    }

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

    pub(crate) fn attach_close_hook(&self, hook: impl Fn(bool) + Send + Sync + 'static) {
        self.inner.slot.attach_close_hook(hook);
    }

    pub(crate) fn push_ingress(&self, bytes: Vec<u8>) -> Result<(), ()> {
        self.inner.slot.push_ingress(bytes)
    }
}

impl ClosedHandle for WebRtcTerminalAdapterHandle {
    fn is_closed(&self) -> bool {
        WebRtcTerminalAdapterHandle::is_closed(self)
    }

    fn host_closed(&self) -> bool {
        WebRtcTerminalAdapterHandle::host_closed(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use botster_core_test_support::terminal_adapter::{
        TerminalAdapterHarnessDriver, assert_terminal_adapter_conformance,
    };
    use botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER;

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
        mux.register("s".into(), "sub".into(), 1, handle.clone());
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
            tokio::time::timeout(Duration::from_millis(50), handle.wait_for_write())
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
    fn close_event_slice_bounds_open_prefix() {
        let mux = WebRtcConnectionMux::new();
        let mut open_adapters = Vec::new();
        for index in 0..8 {
            let (adapter, handle) = mux.create_adapter();
            mux.register(format!("open-{index:02}"), "sub".to_string(), 1, handle);
            open_adapters.push(adapter);
        }
        let (_closed_adapter, closed) = mux.create_adapter();
        mux.register("z-closed".to_string(), "sub".to_string(), 1, closed.clone());
        closed.close();
        let first = mux.queue_closed_subscription_events_bounded(|_| Some(true), 8, None, 8);
        assert_eq!(first.classified, 0);
        assert!(first.more);
        let second = mux.queue_closed_subscription_events_bounded(
            |_| Some(true),
            8,
            first.after_route.as_ref(),
            8,
        );
        assert_eq!(second.classified, 1);
        assert!(!second.more);
        let _ = open_adapters;
    }

    #[test]
    fn production_adapter_source_does_not_name_snapshot_phases() {
        let source = include_str!("adapter.rs");
        let production = source.split("mod tests").next().expect("production source");
        for forbidden in [r#""READY""#, r#""PAGE""#, r#""FINISH""#, "GHOSTSNP"] {
            assert!(
                !production.contains(forbidden),
                "webrtc adapter must stay content-blind: found {forbidden}"
            );
        }
    }
}
