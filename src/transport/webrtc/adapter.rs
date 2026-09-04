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
    aggregate: Option<Arc<crate::admission::connection_budget::ConnectionAggregate>>,
    aggregate_permit: Mutex<Option<crate::admission::connection_budget::AggregateSendPermit>>,
    aggregate_blocked: AtomicBool,
    test_forced_would_block: AtomicBool,
    late_egress: Mutex<Option<Vec<u8>>>,
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
            aggregate_blocked: AtomicBool::new(false),
            test_forced_would_block: AtomicBool::new(false),
            late_egress: Mutex::new(None),
        }
    }

    fn is_closed(&self) -> bool {
        self.slot.is_closed()
    }

    fn close_from_host(&self) {
        self.close_retaining_occupied_budget();
        self.slot.close_from_host();
    }

    #[allow(dead_code)]
    fn host_closed(&self) -> bool {
        self.slot.host_closed()
    }

    fn close(&self) {
        self.close_retaining_occupied_budget();
        self.slot.close();
    }

    fn close_retaining_occupied_budget(&self) {
        if self.park_late_egress() || self.peek_late_egress().is_some() {
            return;
        }
        self.release_aggregate_permit();
    }

    fn park_late_egress(&self) -> bool {
        let Some(bytes) = self.slot.snapshot_active() else {
            return false;
        };
        let mut late = self
            .late_egress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if late.is_none() {
            *late = Some(bytes);
        }
        true
    }

    fn peek_late_egress(&self) -> Option<Vec<u8>> {
        self.late_egress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn restore_late_egress(&self, bytes: Vec<u8>) {
        let mut late = self
            .late_egress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if late.is_none() {
            *late = Some(bytes);
        }
    }

    fn take_late_egress(&self) -> Option<Vec<u8>> {
        self.late_egress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    fn set_would_block(&self, pressured: bool) {
        self.slot.set_would_block(pressured);
    }

    fn pressure(&self) -> TerminalAdapterPressure {
        if self.test_forced_would_block.load(Ordering::Acquire) {
            return TerminalAdapterPressure::WouldBlock;
        }
        self.refresh_aggregate_pressure();
        if self.aggregate_blocked.load(Ordering::Acquire) {
            return TerminalAdapterPressure::WouldBlock;
        }
        self.slot.pressure()
    }

    fn try_write(&self, frame: &TerminalFrame) -> Result<(), TerminalAdapterWriteError> {
        if self.test_forced_would_block.load(Ordering::Acquire) {
            return Err(TerminalAdapterWriteError::WouldBlock);
        }
        let permit = if let Some(aggregate) = self.aggregate.as_ref() {
            let frame_len = frame
                .to_bytes()
                .map_err(|_| TerminalAdapterWriteError::Closed)?
                .len();
            let Some(permit) = aggregate.try_authorize(frame_len) else {
                self.aggregate_blocked.store(true, Ordering::Release);
                return Err(TerminalAdapterWriteError::WouldBlock);
            };
            Some(permit)
        } else {
            None
        };
        let mut aggregate_permit = self
            .aggregate_permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if aggregate_permit.is_some() {
            return Err(TerminalAdapterWriteError::Full);
        }
        *aggregate_permit = permit;
        let result = self.slot.try_write(frame);
        if result.is_err() {
            aggregate_permit.take();
        }
        result
    }

    fn refresh_aggregate_pressure(&self) {
        let can_resume = self
            .aggregate
            .as_ref()
            .is_none_or(|aggregate| aggregate.below_low_water());
        if can_resume && self.aggregate_blocked.swap(false, Ordering::AcqRel) {
            self.slot.notify_writable();
        }
    }

    fn try_read(&self) -> TerminalIngress {
        self.slot.try_read()
    }

    fn snapshot_active(&self) -> Option<Vec<u8>> {
        self.slot.snapshot_active()
    }

    fn complete_active(&self) -> Option<Vec<u8>> {
        let completed = self.slot.complete_active();
        if completed.is_some() {
            self.release_aggregate_permit();
        }
        completed
    }

    fn resize_aggregate_permit(&self, frame_len: usize) -> bool {
        let mut permit = self
            .aggregate_permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match permit.as_mut() {
            Some(existing) => {
                let permitted = existing.try_resize(frame_len);
                if !permitted {
                    self.aggregate_blocked.store(true, Ordering::Release);
                }
                permitted
            }
            None => {
                let Some(aggregate) = self.aggregate.as_ref() else {
                    return true;
                };
                let Some(authorized) = aggregate.try_authorize(frame_len) else {
                    self.aggregate_blocked.store(true, Ordering::Release);
                    return false;
                };
                *permit = Some(authorized);
                true
            }
        }
    }

    fn release_aggregate_permit(&self) {
        self.aggregate_permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
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
            aggregate_blocked: AtomicBool::new(false),
            test_forced_would_block: AtomicBool::new(false),
            late_egress: Mutex::new(None),
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
        if forced_would_block(&session_id) {
            handle
                .inner
                .test_forced_would_block
                .store(true, Ordering::Release);
            record_forced_pressure("would_block");
        }

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

    pub(crate) fn take_late_egress(&self) -> Option<Vec<u8>> {
        self.inner.take_late_egress()
    }

    pub(crate) fn restore_late_egress(&self, bytes: Vec<u8>) {
        self.inner.restore_late_egress(bytes);
    }

    pub(crate) fn resize_aggregate_permit(&self, frame_len: usize) -> bool {
        self.inner.resize_aggregate_permit(frame_len)
    }

    pub(crate) fn release_aggregate_permit(&self) {
        self.inner.release_aggregate_permit();
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

fn forced_would_block(session_id: &str) -> bool {
    std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
        && std::env::var("BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_SESSION").as_deref()
            == Ok(session_id)
}

fn record_forced_pressure(name: &str) {
    if std::env::var("BOTSTER_ENV").as_deref() != Ok("test") {
        return;
    }
    if let Ok(directory) = std::env::var("BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_OBSERVATION")
        && !directory.is_empty()
    {
        let _ = std::fs::write(std::path::Path::new(&directory).join(name), name);
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
        let expected = frame.to_bytes().expect("bytes");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Full);
        handle.close();
        assert_eq!(adapter.pressure(), TerminalAdapterPressure::Closed);
        assert!(handle.snapshot_active().is_none());
        assert!(handle.complete_active().is_none());
        assert_eq!(handle.take_late_egress(), Some(expected));
        assert!(handle.take_late_egress().is_none());
        assert_eq!(
            adapter.try_write(&frame),
            Err(TerminalAdapterWriteError::Closed)
        );
    }

    #[test]
    fn occupied_close_keeps_aggregate_permit_from_a_sibling_write() {
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
        let occupied = TerminalFrame::from_bytes(
            br#"{"type":"terminal_output","marker":"occupied-late-budget"}"#,
        )
        .expect("occupied opaque frame");
        let occupied_len = occupied.to_bytes().expect("occupied bytes").len();
        let sibling_frame = TerminalFrame::from_bytes(
            br#"{"type":"terminal_output","marker":"sibling-late-budget"}"#,
        )
        .expect("sibling opaque frame");
        filled.store(
            AGGREGATE_BUFFERED_HIGH - occupied_len - 32,
            Ordering::Release,
        );

        assert_eq!(first.try_write(&occupied), Ok(()));
        assert_eq!(budget.aggregate_buffered(), AGGREGATE_BUFFERED_HIGH - 32);
        first_handle.close();
        assert!(first_handle.snapshot_active().is_none());
        assert_eq!(
            first_handle.take_late_egress().as_deref(),
            Some(occupied.to_bytes().expect("occupied bytes").as_slice())
        );
        assert_eq!(
            budget.aggregate_buffered(),
            AGGREGATE_BUFFERED_HIGH - 32,
            "parked late bytes must keep their aggregate permit"
        );
        assert_eq!(
            sibling.try_write(&sibling_frame),
            Err(TerminalAdapterWriteError::WouldBlock)
        );
        assert!(sibling_handle.snapshot_active().is_none());
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
    fn data_channel_low_water_does_not_clear_test_forced_route_pressure() {
        let (mut adapter, handle) = WebRtcTerminalAdapter::pair();
        handle
            .inner
            .test_forced_would_block
            .store(true, Ordering::Release);
        handle.set_would_block(false);
        let frame =
            TerminalFrame::from_bytes(br#"{"type":"terminal_output"}"#).expect("opaque frame");

        assert_eq!(adapter.pressure(), TerminalAdapterPressure::WouldBlock);
        assert_eq!(
            adapter.try_write(&frame),
            Err(TerminalAdapterWriteError::WouldBlock)
        );
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
        let frame =
            TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"aggregate"}"#)
                .expect("opaque frame");

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
            handle.complete_active(),
            Some(frame.to_bytes().expect("bytes"))
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
        let (generation, replacements) = worker.record_attach(
            client_id.clone(),
            session_id.clone(),
            subscription_id.clone(),
        );
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

        let mut egress = vec![(
            client_id.clone(),
            TransportEgress::TerminalOutput {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
                data: b"held-by-core".to_vec(),
            },
        )];
        assert!(worker.ingest_bound_terminal_frames(&mut egress).is_empty());
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
