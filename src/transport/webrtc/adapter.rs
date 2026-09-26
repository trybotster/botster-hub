//! Production WebRTC terminal adapter and Core harness driver.
//!
//! The adapter owns one in-flight write slot. `try_write` holds one routed
//! opaque [`RoutedTerminalFrame`] by shared reference and does not inspect
//! snapshot phases or snapshot bodies.
//! `close` and `Drop` return without waiting on DataChannel I/O or a writer lock.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use botster_core::contract::terminal_adapter::{
    TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError, TerminalIngress,
};
use botster_core::contract::terminal_wake::{TerminalWakeSink, WakingTerminalAdapter};
use botster_hub_client::DaemonEvent;
use botster_terminal_protocol::RoutedTerminalFrame;

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
    aggregate: Option<Arc<crate::admission::connection_budget::ConnectionAggregate>>,
    aggregate_permit: Mutex<Option<crate::admission::connection_budget::AggregateSendPermit>>,
    /// Wire bytes of the write the aggregate refused; zero when none is.
    aggregate_blocked: AtomicUsize,
}

impl WebRtcTerminalAdapterInner {
    fn new() -> Self {
        Self {
            slot: AdapterSlot::with_wake_and_close_work(
                AdapterWake::new(),
                Arc::new(AtomicBool::new(false)),
            ),
            aggregate: None,
            aggregate_permit: Mutex::new(None),
            aggregate_blocked: AtomicUsize::new(0),
        }
    }

    fn is_closed(&self) -> bool {
        self.slot.is_closed()
    }

    fn close_from_host(&self) {
        self.slot.close_from_host();
        self.release_aggregate_permit();
    }

    #[allow(dead_code)]
    fn host_closed(&self) -> bool {
        self.slot.host_closed()
    }

    fn close(&self) {
        self.slot.close();
        self.release_aggregate_permit();
    }

    fn set_would_block(&self, pressured: bool) {
        self.slot.set_would_block(pressured);
    }

    fn pressure(&self) -> TerminalAdapterPressure {
        if self.is_closed() {
            return TerminalAdapterPressure::Closed;
        }
        self.refresh_aggregate_pressure();
        if self.aggregate_blocked.load(Ordering::Acquire) != 0 {
            return TerminalAdapterPressure::WouldBlock;
        }
        self.slot.pressure()
    }

    fn try_write(&self, frame: &RoutedTerminalFrame) -> Result<(), TerminalAdapterWriteError> {
        if self.is_closed() {
            return Err(TerminalAdapterWriteError::Closed);
        }
        let permit = if let Some(aggregate) = self.aggregate.as_ref() {
            // Authorize the exact sealed size, so the flush never needs more
            // than this permit holds.
            let wire_len =
                crate::transport::webrtc::delivery::sealed_terminal_wire_len(frame.frame.len())
                    .unwrap_or(usize::MAX);
            let Some(permit) = aggregate.try_authorize(wire_len) else {
                self.aggregate_blocked
                    .store(wire_len.max(1), Ordering::Release);
                // A release that ran before the mark saw nothing to wake.
                self.refresh_aggregate_pressure();
                return Err(TerminalAdapterWriteError::WouldBlock);
            };
            Some(permit)
        } else {
            None
        };
        let mut aggregate_permit = match self.aggregate_permit.try_lock() {
            Ok(permit) => permit,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(TerminalAdapterWriteError::Full);
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(TerminalAdapterWriteError::Closed);
            }
        };
        if self.is_closed() {
            aggregate_permit.take();
            return Err(TerminalAdapterWriteError::Closed);
        }
        if aggregate_permit.is_some() {
            drop(aggregate_permit);
            if self.is_closed() {
                self.release_aggregate_permit();
                return Err(TerminalAdapterWriteError::Closed);
            }
            return Err(TerminalAdapterWriteError::Full);
        }
        *aggregate_permit = permit;
        let result = self.slot.try_write(frame);
        if result.is_err() {
            aggregate_permit.take();
        }
        drop(aggregate_permit);
        if self.is_closed() {
            self.release_aggregate_permit();
            return Err(TerminalAdapterWriteError::Closed);
        }
        result
    }

    /// Resume a refused writer only when its write would now be
    /// authorized, so a resume never meets the same refusal.
    fn refresh_aggregate_pressure(&self) {
        let need = self.aggregate_blocked.load(Ordering::Acquire);
        if need == 0 {
            return;
        }
        let can_resume = self
            .aggregate
            .as_ref()
            .is_none_or(|aggregate| aggregate.admits_refused(need));
        if can_resume && self.aggregate_blocked.swap(0, Ordering::AcqRel) != 0 {
            self.slot.notify_writable();
        }
    }

    fn try_read(&self) -> TerminalIngress {
        self.slot.try_read()
    }

    fn snapshot_active(&self) -> Option<RoutedTerminalFrame> {
        self.slot.snapshot_active()
    }

    fn complete_active(&self) -> Option<RoutedTerminalFrame> {
        let mut permit = self
            .aggregate_permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let completed = self.slot.complete_active();
        if completed.is_some() || self.is_closed() {
            permit.take();
        }
        drop(permit);
        if self.is_closed() {
            self.release_aggregate_permit();
        } else if completed.is_some() {
            self.slot.notify_writable();
        }
        completed
    }

    fn transfer_aggregate_permit(
        &self,
        frame_len: usize,
        usage: &std::sync::atomic::AtomicUsize,
    ) -> bool {
        let mut permit = self
            .aggregate_permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let permitted = if self.is_closed() {
            permit.take();
            false
        } else if let Some(existing) = permit.as_mut() {
            let permitted = existing.try_resize(frame_len);
            if !permitted {
                self.aggregate_blocked
                    .store(frame_len.max(1), Ordering::Release);
            }
            permitted
        } else {
            self.aggregate.is_none()
        };
        if permitted {
            // Publish the full wire bound before releasing its authorization.
            usage.fetch_add(frame_len, Ordering::Release);
            if let Some(permit) = permit.take() {
                permit.transferred();
            }
        }
        drop(permit);
        if self.is_closed() {
            self.release_aggregate_permit();
        }
        permitted
    }

    fn release_aggregate_permit(&self) {
        // A contended holder takes the permit itself. Whichever holder
        // takes it, the permit's drop wakes the senders waiting for it.
        let released = match self.aggregate_permit.try_lock() {
            Ok(mut permit) => permit.take(),
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner().take(),
            Err(std::sync::TryLockError::WouldBlock) => None,
        };
        drop(released);
    }
}

impl crate::admission::connection_budget::CapacityWaiter for WebRtcTerminalAdapterInner {
    fn capacity_released(&self) {
        self.refresh_aggregate_pressure();
    }

    fn retired(&self) -> bool {
        self.is_closed()
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
    #[cfg(test)]
    pub(crate) fn pair() -> (Self, WebRtcTerminalAdapterHandle) {
        Self::pair_with_wake(AdapterWake::new())
    }

    #[cfg(test)]
    fn pair_with_wake(wake: AdapterWake) -> (Self, WebRtcTerminalAdapterHandle) {
        Self::pair_with_wake_and_close_work(wake, Arc::new(AtomicBool::new(false)))
    }

    #[cfg(test)]
    fn pair_with_wake_and_close_work(
        wake: AdapterWake,
        close_work: Arc<AtomicBool>,
    ) -> (Self, WebRtcTerminalAdapterHandle) {
        Self::pair_with_wake_close_work_and_aggregate(wake, close_work, None)
    }

    fn pair_with_wake_close_work_and_aggregate(
        wake: AdapterWake,
        close_work: Arc<AtomicBool>,
        aggregate: Option<Arc<crate::admission::connection_budget::ConnectionAggregate>>,
    ) -> (Self, WebRtcTerminalAdapterHandle) {
        let inner = Arc::new(WebRtcTerminalAdapterInner {
            slot: AdapterSlot::with_wake_and_close_work(wake, close_work),
            aggregate,
            aggregate_permit: Mutex::new(None),
            aggregate_blocked: AtomicUsize::new(0),
        });
        if let Some(aggregate) = inner.aggregate.as_ref() {
            let waiter: std::sync::Weak<dyn crate::admission::connection_budget::CapacityWaiter> =
                Arc::downgrade(&inner) as _;
            aggregate.register_waiter(waiter);
        }
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
    fn try_write(&mut self, frame: &RoutedTerminalFrame) -> Result<(), TerminalAdapterWriteError> {
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

#[cfg(test)]
type HostEventObserver = Arc<dyn Fn(&DaemonEvent) + Send + Sync>;

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
    #[cfg(test)]
    host_event_observer: Mutex<Option<HostEventObserver>>,
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
                #[cfg(test)]
                host_event_observer: Mutex::new(None),
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

    #[cfg(test)]
    pub(crate) fn create_adapter(&self) -> (WebRtcTerminalAdapter, WebRtcTerminalAdapterHandle) {
        self.create_adapter_with_aggregate(
            crate::admission::connection_budget::ConnectionBudget::default().aggregate(),
        )
    }

    pub(crate) fn create_adapter_with_aggregate(
        &self,
        aggregate: Arc<crate::admission::connection_budget::ConnectionAggregate>,
    ) -> (WebRtcTerminalAdapter, WebRtcTerminalAdapterHandle) {
        let close_work = self
            .inner
            .close_work
            .lock()
            .ok()
            .map(|slot| Arc::clone(&*slot))
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        WebRtcTerminalAdapter::pair_with_wake_close_work_and_aggregate(
            AdapterWake::new(),
            close_work,
            Some(aggregate),
        )
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

    pub(crate) fn refresh_aggregate_pressure(&self) {
        if let Ok(routes) = self.inner.routes.lock() {
            for route in routes.values() {
                route.handle.inner.refresh_aggregate_pressure();
            }
        }
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
        if let Ok(source) = self.inner.close_source.lock()
            && let Some(source) = source.as_ref()
        {
            for (_, subscription_id, generation) in &keys {
                source.retire(session_id, subscription_id, *generation);
            }
        }
        self.inner.closed_events.suppress_session_keys(keys);
    }

    pub(crate) fn suppress_generation(
        &self,
        session_id: impl Into<String>,
        subscription_id: impl Into<String>,
        generation: u64,
    ) {
        let session_id = session_id.into();
        let subscription_id = subscription_id.into();
        self.inner
            .closed_events
            .suppress_generation(session_id, subscription_id, generation);
    }

    pub(crate) fn commit_generation_suppression(
        &self,
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) {
        if let Ok(source) = self.inner.close_source.lock()
            && let Some(source) = source.as_ref()
        {
            source.retire(session_id, subscription_id, generation);
        }
    }

    pub(crate) fn unsuppress_generation(
        &self,
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) {
        self.inner
            .closed_events
            .unsuppress_generation(session_id, subscription_id, generation);
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
        #[cfg(test)]
        let observed_event = event.clone();
        self.inner.closed_events.push_event(event);
        #[cfg(test)]
        if let Ok(observer) = self.inner.host_event_observer.lock()
            && let Some(observer) = observer.as_ref()
        {
            observer(&observed_event);
        }
        self.inner.wake.wake();
    }

    #[cfg(test)]
    pub(crate) fn set_host_event_observer(&self, observer: Option<HostEventObserver>) {
        *self
            .inner
            .host_event_observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = observer;
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

    #[allow(dead_code)]
    pub(crate) fn snapshot_writes(
        &self,
    ) -> Vec<(
        String,
        String,
        WebRtcTerminalAdapterHandle,
        RoutedTerminalFrame,
    )> {
        let Ok(routes) = self.inner.routes.lock() else {
            return Vec::new();
        };
        routes
            .values()
            .filter_map(|route| {
                if route.handle.is_closed() {
                    return None;
                }
                route.handle.snapshot_active().map(|frame| {
                    (
                        route.session_id.clone(),
                        route.subscription_id.clone(),
                        route.handle.clone(),
                        frame,
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

    pub(crate) fn snapshot_active(&self) -> Option<RoutedTerminalFrame> {
        self.inner.snapshot_active()
    }

    pub(crate) fn complete_active(&self) -> Option<RoutedTerminalFrame> {
        self.inner.complete_active()
    }

    pub(crate) fn transfer_aggregate_permit(
        &self,
        frame_len: usize,
        usage: &std::sync::atomic::AtomicUsize,
    ) -> bool {
        self.inner.transfer_aggregate_permit(frame_len, usage)
    }

    #[cfg(test)]
    pub(crate) fn aggregate_blocked_for_test(&self) -> bool {
        self.inner.aggregate_blocked.load(Ordering::Acquire) != 0
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
    use botster_terminal_protocol::{RouteId, encode_output};

    fn test_frame(marker: &[u8]) -> RoutedTerminalFrame {
        RoutedTerminalFrame::new(
            RouteId::new("route").expect("route"),
            1,
            0,
            encode_output(marker).expect("output frame"),
        )
    }
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
            if let Some(frame) = self.handle.complete_active() {
                self.delivered.push(frame.frame.as_bytes().to_vec());
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
        let frame = test_frame(b"in-flight");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Full);
        handle.close();
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Closed);
        assert!(handle.snapshot_active().is_none());
        assert!(handle.complete_active().is_none());
        assert_eq!(
            adapter.try_write(&frame),
            Err(TerminalAdapterWriteError::Closed)
        );
    }

    #[test]
    fn occupied_close_releases_aggregate_budget_for_a_sibling_write() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, ChannelClass, ConnectionBudget,
        };

        let mut budget = ConnectionBudget::default();
        let filled = budget
            .reserve("entity".into(), ChannelClass::Entity)
            .expect("entity budget");
        let mux = WebRtcConnectionMux::new();
        let (mut first, first_handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        let (mut sibling, sibling_handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        mux.register("first".into(), "terminal".into(), 1, first_handle.clone());
        mux.register(
            "sibling".into(),
            "terminal".into(),
            1,
            sibling_handle.clone(),
        );
        let occupied = test_frame(b"occupied-late-budget");
        // The write permit covers the sealed wire size.
        let occupied_len =
            crate::transport::webrtc::delivery::sealed_terminal_wire_len(occupied.frame.len())
                .expect("wire len");
        let sibling_frame = test_frame(b"sibling-late-budget");
        filled.store(
            AGGREGATE_BUFFERED_HIGH - occupied_len - 32,
            Ordering::Release,
        );

        assert_eq!(first.try_write(&occupied), Ok(()));
        assert_eq!(budget.aggregate_buffered(), AGGREGATE_BUFFERED_HIGH - 32);
        first_handle.close();
        assert!(first_handle.snapshot_active().is_none());
        assert_eq!(budget.aggregate_buffered(), filled.load(Ordering::Acquire));
        assert_eq!(sibling.try_write(&sibling_frame), Ok(()));
        assert!(sibling_handle.snapshot_active().is_some());
    }

    /// A close that finds the permit lock held leaves the permit to that
    /// holder. The holder's later release must still wake a sender the
    /// aggregate refused.
    #[test]
    fn a_contended_close_wakes_refused_senders_from_the_holder_release() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, AGGREGATE_BUFFERED_LOW, ChannelClass, ConnectionBudget,
        };
        for holder_path in ["transfer_aggregate_permit", "complete_active"] {
            let mut budget = ConnectionBudget::default();
            let _usage = budget
                .reserve("route".into(), ChannelClass::Terminal)
                .expect("route budget");
            let mux = WebRtcConnectionMux::new();
            let (mut holder, holder_handle) = mux.create_adapter_with_aggregate(budget.aggregate());
            let (mut refused, refused_handle) =
                mux.create_adapter_with_aggregate(budget.aggregate());
            let wire = |frame: &RoutedTerminalFrame| {
                crate::transport::webrtc::delivery::sealed_terminal_wire_len(frame.frame.len())
                    .expect("wire len")
            };
            let frame = test_frame(&vec![b'h'; 64 * 1024]);
            let wire_len = wire(&frame);
            assert_eq!(holder.try_write(&frame), Ok(()));
            // A write that fits an empty aggregate but not beside the held
            // permit, which keeps the aggregate below the low mark.
            let mut refused_len = AGGREGATE_BUFFERED_HIGH;
            let big = loop {
                let candidate = test_frame(&vec![b'x'; refused_len]);
                if wire(&candidate) <= AGGREGATE_BUFFERED_HIGH - wire_len / 2 {
                    break candidate;
                }
                refused_len -= 1024;
            };
            assert!(wire(&big) + wire_len > AGGREGATE_BUFFERED_HIGH);
            assert!(budget.aggregate_buffered() < AGGREGATE_BUFFERED_LOW);
            assert_eq!(
                refused.try_write(&big),
                Err(TerminalAdapterWriteError::WouldBlock)
            );
            assert!(refused_handle.aggregate_blocked_for_test());

            // Close while another holder has the permit lock.
            let guard = holder_handle
                .inner
                .aggregate_permit
                .lock()
                .expect("hold permit lock");
            let closer = std::thread::spawn({
                let holder_handle = holder_handle.clone();
                move || holder_handle.close()
            });
            closer.join().expect("close thread");
            assert!(holder_handle.is_closed());
            assert!(guard.is_some(), "the contended close left the permit");
            drop(guard);
            assert!(refused_handle.aggregate_blocked_for_test());

            // The holder path meets the closed adapter and drops the permit.
            match holder_path {
                "transfer_aggregate_permit" => {
                    let usage = std::sync::atomic::AtomicUsize::new(0);
                    assert!(!holder_handle.transfer_aggregate_permit(wire_len, &usage));
                }
                _ => {
                    let _ = holder_handle.complete_active();
                }
            }
            assert!(
                !refused_handle.aggregate_blocked_for_test(),
                "{holder_path}: the holder's release woke the refused sender"
            );
            drop((holder, refused));
        }
    }

    /// An oversize write refused beside a small buffered frame stays refused
    /// with no wake: it would be refused again until the aggregate empties.
    /// The release that empties the aggregate resumes it.
    #[test]
    fn an_oversize_refusal_stays_blocked_until_the_aggregate_empties() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, AGGREGATE_BUFFERED_LOW, ConnectionBudget,
        };
        let budget = ConnectionBudget::default();
        let mux = WebRtcConnectionMux::new();
        let (mut holder, holder_handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        let (mut oversize, oversize_handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        assert_eq!(holder.try_write(&test_frame(b"small")), Ok(()));
        assert!(budget.aggregate_buffered() < AGGREGATE_BUFFERED_LOW);
        let frame = test_frame(&vec![b'o'; AGGREGATE_BUFFERED_HIGH + 1]);
        assert_eq!(
            oversize.try_write(&frame),
            Err(TerminalAdapterWriteError::WouldBlock)
        );
        assert!(
            oversize_handle.aggregate_blocked_for_test(),
            "no resume while the aggregate is nonempty"
        );
        assert_eq!(
            oversize_handle.inner.pressure(),
            TerminalAdapterPressure::WouldBlock
        );
        holder_handle.close();
        assert!(!oversize_handle.aggregate_blocked_for_test());
        assert_eq!(oversize.try_write(&frame), Ok(()));
        drop(holder);
    }

    #[test]
    fn close_does_not_wait_for_the_aggregate_permit_lock() {
        let (mut adapter, handle) = WebRtcTerminalAdapter::pair();
        let frame = test_frame(b"output");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        let guard = handle
            .inner
            .aggregate_permit
            .lock()
            .expect("hold permit lock");
        let (tx, rx) = std::sync::mpsc::channel();
        let closer = std::thread::spawn({
            let handle = handle.clone();
            move || {
                handle.close();
                tx.send(()).expect("close result");
            }
        });
        let result = rx.recv_timeout(Duration::from_secs(1));
        drop(guard);
        closer.join().expect("close thread");
        result.expect("close must not wait for the permit lock");
        assert!(handle.is_closed());
        assert!(!handle.transfer_aggregate_permit(1, &std::sync::atomic::AtomicUsize::new(0)));
    }

    #[test]
    fn completing_twice_does_not_duplicate_the_active_frame() {
        let (mut adapter, handle) = WebRtcTerminalAdapter::pair();
        let frame = test_frame(b"once");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert!(handle.complete_active().is_some());
        assert!(handle.complete_active().is_none());
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Ready);
    }

    #[test]
    fn aggregate_refusal_returns_would_block_without_retaining_the_frame() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, ChannelClass, ConnectionBudget,
        };

        let mut budget = ConnectionBudget::default();
        let filled = budget
            .reserve("entity".into(), ChannelClass::Entity)
            .expect("entity budget");
        filled.store(AGGREGATE_BUFFERED_HIGH, Ordering::Release);
        let mux = WebRtcConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        mux.register("session".into(), "terminal".into(), 1, handle.clone());
        let frame = test_frame(b"aggregate");

        assert_eq!(
            adapter.try_write(&frame),
            Err(TerminalAdapterWriteError::WouldBlock)
        );
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::WouldBlock);
        assert!(handle.snapshot_active().is_none());

        filled.store(0, Ordering::Release);
        mux.refresh_aggregate_pressure();
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Ready);
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert_eq!(
            handle
                .complete_active()
                .map(|completed| completed.frame.as_bytes().to_vec()),
            Some(frame.frame.as_bytes().to_vec())
        );
    }

    #[test]
    fn sustained_aggregate_pressure_reaches_core_hard_stop_and_retires_route() {
        use crate::admission::connection_budget::{
            AGGREGATE_BUFFERED_HIGH, ChannelClass, ConnectionBudget,
        };
        use botster_core::{
            ClientId, ClientWorker, SessionId, SubscriptionId, TerminalCapabilitySet,
            TerminalWakeBatch, TerminalWakeRoute, TerminalWakeSource, TransportEgress,
        };

        let mut budget = ConnectionBudget::default();
        let filled = budget
            .reserve("entity".into(), ChannelClass::Entity)
            .expect("entity budget");
        filled.store(AGGREGATE_BUFFERED_HIGH, Ordering::Release);

        let mux = WebRtcConnectionMux::new();
        let (adapter, handle) = mux.create_adapter_with_aggregate(budget.aggregate());
        let client_id = ClientId("client".into());
        let session_id = SessionId("session".into());
        let subscription_id = SubscriptionId("terminal".into());
        let mut worker = ClientWorker::new();
        worker.set_wake_source(TerminalWakeSource::new());
        let (generation, replacements) = worker
            .record_attach(
                client_id.clone(),
                session_id.clone(),
                subscription_id.clone(),
            )
            .expect("record attach");
        assert!(replacements.is_empty());
        mux.register(
            session_id.0.clone(),
            subscription_id.0.clone(),
            generation.0,
            handle.clone(),
        );
        worker
            .bind_waking_terminal_adapter(
                &client_id,
                session_id.clone(),
                subscription_id.clone(),
                generation,
                TerminalCapabilitySet::empty(),
                Box::new(adapter),
            )
            .expect("bind aggregate-backed adapter");
        let route_only = TerminalWakeBatch {
            adapter_routes: vec![TerminalWakeRoute {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            }],
            ingress_sessions: Vec::new(),
        };

        assert!(
            worker
                .push_route_frame(
                    &session_id,
                    &subscription_id,
                    botster_terminal_protocol::encode_output(b"held-by-core").unwrap(),
                )
                .expect("queue the exact route output")
                .is_none()
        );
        assert!(worker.bound_owner_has_held_frames(&session_id, &subscription_id));
        let mut egress = vec![(
            client_id.clone(),
            TransportEgress::TerminalOutput {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
                data: b"held-by-core".to_vec(),
            },
        )];
        assert!(worker.filter_bound_terminal_frames(&mut egress).is_empty());
        assert!(egress.is_empty());

        for attempt in 1..512 {
            assert!(
                worker.pump_woken(&route_only).is_empty(),
                "attempt {attempt} must retain the Core route"
            );
            assert!(worker.has_subscription(&session_id, &subscription_id));
        }
        let teardowns = worker.pump_woken(&route_only);
        assert_eq!(teardowns.len(), 1);
        assert_eq!(teardowns[0].client_id, client_id);
        assert_eq!(teardowns[0].session_id, session_id);
        assert_eq!(teardowns[0].subscription_id, subscription_id);
        assert_eq!(teardowns[0].generation, generation);
        assert!(handle.is_closed());
        assert!(!worker.has_subscription(&teardowns[0].session_id, &teardowns[0].subscription_id));
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);
        assert!(mux.live_handle("session", "terminal").is_none());
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
        let frame = test_frame(b"race");
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
