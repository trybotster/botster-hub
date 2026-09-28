//! Production Unix terminal adapter and Core harness driver.
//!
//! The adapter owns one in-flight write slot holding a [`RoutedTerminalFrame`]
//! by `Arc` clones. It reads route and generation from the envelope and never
//! inspects the shared `TerminalBody` beyond its length. `close` and `Drop`
//! return without waiting on socket I/O or a writer lock.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::data_plane::CloseWorkSource;
use crate::subscription::closed_events::{
    ClosedEventLedger, ClosedEventRoute, ClosedEventSliceProgress, ClosedHandle, ClosedLedgerItem,
};
use crate::transport::shared::adapter_slot::AdapterSlot;
use crate::transport::shared::wake::AdapterWake;
use botster_core::contract::terminal_adapter::{
    TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError, TerminalIngress,
};
use botster_core::contract::terminal_wake::{TerminalWakeSink, WakingTerminalAdapter};
use botster_hub_client::{DaemonEvent, DaemonUnixCreditFrame};

use super::credit::{CreditCheck, RouteCredit};
use botster_terminal_protocol::RoutedTerminalFrame;

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
    slot: AdapterSlot<AdapterWake>,
    deferred: AtomicBool,
    /// S13 output and input credit. Core's `try_write` holds this lock across
    /// the credit check and the slot write, so a grant cannot land between
    /// the recorded need and the final check.
    credit: Mutex<AdapterCredit>,
    /// A credit need is recorded. Mirrored under the credit lock, so
    /// `pressure()` reads it without that lock.
    credit_short: AtomicBool,
}

/// One route generation's credit, and the credit frames it owes the client.
#[derive(Default)]
struct AdapterCredit {
    output: RouteCredit,
    /// The wire route (the subscription id) and generation. Set at
    /// registration, or from the first offered frame when Core offers one
    /// before the route is registered.
    route: Option<(String, u64)>,
    /// The route's `TerminalAttached` response is on the socket. Credit
    /// frames wait for it, so the client knows the route first.
    attach_written: bool,
    outbox: VecDeque<DaemonUnixCreditFrame>,
    /// Input frames Core removed from ingress, not yet returned.
    input_credit: u32,
    /// `CLOSED` was taken. Later grants are ignored and nothing more is sent.
    settled: bool,
}

impl AdapterCredit {
    fn route_or<'a>(&'a mut self, frame: &RoutedTerminalFrame) -> &'a (String, u64) {
        self.route
            .get_or_insert_with(|| (frame.route.as_str().to_string(), frame.generation))
    }

    fn has_output(&self) -> bool {
        self.attach_written && !self.settled && (!self.outbox.is_empty() || self.input_credit > 0)
    }
}

impl UnixTerminalAdapterInner {
    fn new() -> Self {
        Self::with_slot(AdapterSlot::with_wake_and_close_work(
            AdapterWake::new(),
            Arc::new(AtomicBool::new(false)),
        ))
    }

    fn with_slot(slot: AdapterSlot<AdapterWake>) -> Self {
        Self {
            slot,
            deferred: AtomicBool::new(false),
            credit: Mutex::new(AdapterCredit::default()),
            credit_short: AtomicBool::new(false),
        }
    }

    fn lock_credit(&self) -> Option<std::sync::MutexGuard<'_, AdapterCredit>> {
        match self.credit.lock() {
            Ok(credit) => Some(credit),
            Err(_) => {
                self.slot.close();
                None
            }
        }
    }

    fn is_closed(&self) -> bool {
        self.slot.is_closed()
    }

    fn close_from_host(&self) {
        self.slot.close_from_host();
    }

    fn host_closed(&self) -> bool {
        self.slot.host_closed()
    }

    fn close(&self) {
        self.slot.close();
    }

    fn close_from_core(
        &self,
        reason: botster_core::contract::terminal_adapter::TerminalRouteCloseReason,
    ) {
        self.slot.close_from_core(reason);
    }

    fn pressure(&self) -> TerminalAdapterPressure {
        match self.slot.pressure() {
            TerminalAdapterPressure::Ready if self.credit_short.load(Ordering::SeqCst) => {
                TerminalAdapterPressure::WouldBlock
            }
            pressure => pressure,
        }
    }

    /// Write one frame when the route's credit covers it. Otherwise record
    /// the need, demand it once, and refuse with `WouldBlock`, which debits
    /// nothing. A later covering grant raises the Writable wake.
    fn try_write(&self, frame: &RoutedTerminalFrame) -> Result<(), TerminalAdapterWriteError> {
        if self.slot.is_closed() {
            return Err(TerminalAdapterWriteError::Closed);
        }
        let Some(mut credit) = self.lock_credit() else {
            return Err(TerminalAdapterWriteError::Closed);
        };
        let bytes = frame_credit_bytes(frame);
        match credit.output.check(bytes) {
            CreditCheck::Covered => {
                let written = self.slot.try_write(frame);
                if written.is_ok() {
                    credit.output.commit(bytes);
                    self.credit_short.store(false, Ordering::SeqCst);
                }
                written
            }
            CreditCheck::Short { demand } => {
                self.credit_short.store(true, Ordering::SeqCst);
                if let Some(demand) = demand {
                    let (route, generation) = credit.route_or(frame).clone();
                    credit.outbox.push_back(DaemonUnixCreditFrame::Demand {
                        route,
                        generation,
                        bytes: demand,
                    });
                    let ready = credit.has_output();
                    drop(credit);
                    if ready {
                        self.slot.wake_transport();
                    }
                }
                Err(TerminalAdapterWriteError::WouldBlock)
            }
        }
    }

    fn try_read(&self) -> TerminalIngress {
        let ingress = self.slot.try_read();
        if matches!(ingress, TerminalIngress::Frame(_))
            && let Some(mut credit) = self.lock_credit()
            && !credit.settled
        {
            // S13: Core removed one input frame, so its credit returns.
            credit.input_credit = credit.input_credit.saturating_add(1);
            let ready = credit.has_output();
            drop(credit);
            if ready {
                self.slot.wake_transport();
            }
        }
        ingress
    }

    /// Core dropped the refused head. Its need is void, and the pool goes
    /// back to the client so an idle route holds no credit.
    fn head_withdrawn(&self) {
        let Some(mut credit) = self.lock_credit() else {
            return;
        };
        self.credit_short.store(false, Ordering::SeqCst);
        let returned = credit.output.withdraw_head();
        if let (Some(returned), Some((route, generation))) = (returned, credit.route.clone())
            && !credit.settled
        {
            credit.outbox.push_back(DaemonUnixCreditFrame::Return {
                route,
                generation,
                items: returned.items,
                bytes: returned.bytes,
            });
        }
        let ready = credit.has_output();
        drop(credit);
        if ready {
            self.slot.wake_transport();
        }
    }

    /// Apply a client grant for this route generation. A settled route
    /// ignores it; `CLOSED` settlement already covers it.
    fn grant(&self, items: u32, bytes: u64) {
        let Some(mut credit) = self.lock_credit() else {
            return;
        };
        if credit.settled {
            return;
        }
        let outcome = credit.output.grant(items, bytes);
        if let (Some(demand), Some((route, generation))) = (outcome.demand, credit.route.clone()) {
            credit.outbox.push_back(DaemonUnixCreditFrame::Demand {
                route,
                generation,
                bytes: demand,
            });
        }
        if outcome.wake {
            self.credit_short.store(false, Ordering::SeqCst);
        }
        let ready = credit.has_output();
        drop(credit);
        if outcome.wake {
            self.slot.notify_writable();
        } else if ready {
            self.slot.wake_transport();
        }
    }

    fn snapshot_active(&self) -> Option<RoutedTerminalFrame> {
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

    fn complete_active(&self) -> Option<RoutedTerminalFrame> {
        self.slot.complete_active()
    }
}

/// The credit one frame costs: one item plus `body.len()`, its 8-byte
/// header included, the unit the client's budget charges.
fn frame_credit_bytes(frame: &RoutedTerminalFrame) -> u64 {
    u64::try_from(frame.frame.len()).unwrap_or(u64::MAX)
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
    #[cfg(test)]
    pub(crate) fn pair() -> (Self, UnixTerminalAdapterHandle) {
        Self::pair_with_wake(AdapterWake::new())
    }

    /// Create an adapter that stores one wake permit on write or close.
    #[cfg(test)]
    pub(crate) fn pair_with_wake(wake: AdapterWake) -> (Self, UnixTerminalAdapterHandle) {
        Self::pair_with_wake_and_close_work(wake, Arc::new(AtomicBool::new(false)))
    }

    fn pair_with_wake_and_close_work(
        wake: AdapterWake,
        close_work: Arc<AtomicBool>,
    ) -> (Self, UnixTerminalAdapterHandle) {
        let inner = Arc::new(UnixTerminalAdapterInner::with_slot(
            AdapterSlot::with_wake_and_close_work(wake, close_work),
        ));
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
    fn try_write(&mut self, frame: &RoutedTerminalFrame) -> Result<(), TerminalAdapterWriteError> {
        self.inner.try_write(frame)
    }

    fn close(
        &mut self,
        reason: botster_core::contract::terminal_adapter::TerminalRouteCloseReason,
    ) {
        self.inner.close_from_core(reason);
    }

    fn pressure(&self) -> TerminalAdapterPressure {
        self.inner.pressure()
    }

    fn try_read(&mut self) -> TerminalIngress {
        self.inner.try_read()
    }

    fn head_withdrawn(&mut self) {
        self.inner.head_withdrawn();
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
    wake: AdapterWake,
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
                wake: AdapterWake::new(),
                dying: AtomicBool::new(false),
                routes: Mutex::new(BTreeMap::new()),
                closed_events: ClosedEventLedger::with_credit_settlement(),
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

    pub(crate) fn create_adapter(&self) -> (UnixTerminalAdapter, UnixTerminalAdapterHandle) {
        let close_work = self
            .inner
            .close_work
            .lock()
            .ok()
            .map(|slot| Arc::clone(&*slot))
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        UnixTerminalAdapter::pair_with_wake_and_close_work(self.inner.wake.clone(), close_work)
    }

    /// Register one bound route. Returns `false` without registering when
    /// the connection is already dying, so a late bind cannot outlive
    /// `close_all`.
    #[must_use]
    pub(crate) fn register(
        &self,
        session_id: String,
        subscription_id: String,
        generation: u64,
        handle: UnixTerminalAdapterHandle,
    ) -> bool {
        if let Ok(mut routes) = self.inner.routes.lock() {
            // Checked under the routes lock: `close_all` sets `dying` before it
            // drains the map, so a registration either lands before the drain
            // or is refused here.
            if self.inner.dying.load(Ordering::SeqCst) {
                return false;
            }
            handle.bind_route(&subscription_id, generation);
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
            handle.attach_close_hook(move |report| hook.notify_closed(report));
        }
        self.inner.wake.wake();
        true
    }

    #[cfg(test)]
    pub(crate) fn route_handle(
        &self,
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) -> Option<UnixTerminalAdapterHandle> {
        self.inner.routes.lock().ok().and_then(|routes| {
            routes
                .get(&(
                    session_id.to_string(),
                    subscription_id.to_string(),
                    generation,
                ))
                .map(|route| route.handle.clone())
        })
    }

    pub(crate) fn close_all(&self) {
        self.inner.dying.store(true, Ordering::SeqCst);
        if let Ok(mut routes) = self.inner.routes.lock() {
            for (_, route) in std::mem::take(&mut *routes) {
                route.handle.close_from_host();
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

    /// The next close event, skipping credit settlements.
    #[cfg(test)]
    pub(crate) fn pop_pending_event(&self) -> Option<DaemonEvent> {
        loop {
            match self.inner.closed_events.pop_pending_item()? {
                ClosedLedgerItem::Event(event) => return Some(event),
                ClosedLedgerItem::SettleCredit { .. } => {}
            }
        }
    }

    /// The next close-lane frame in order: a close event, or a route's
    /// `CLOSED`. A route settles once, so a second report yields nothing.
    pub(crate) fn pop_pending_close(&self) -> Option<UnixCloseLaneItem> {
        loop {
            match self.inner.closed_events.pop_pending_item()? {
                ClosedLedgerItem::Event(event) => return Some(UnixCloseLaneItem::Event(event)),
                ClosedLedgerItem::SettleCredit {
                    session_id,
                    subscription_id,
                    generation,
                } => {
                    let handle = self.inner.routes.lock().ok().and_then(|routes| {
                        routes
                            .get(&(session_id, subscription_id.clone(), generation))
                            .map(|route| route.handle.clone())
                    });
                    if let Some(closed) =
                        handle.and_then(|handle| handle.settle(&subscription_id, generation))
                    {
                        return Some(UnixCloseLaneItem::Credit(closed));
                    }
                }
            }
        }
    }

    /// Queued credit frames of every route whose attach is on the socket.
    pub(crate) fn take_credit_frames(&self) -> Vec<DaemonUnixCreditFrame> {
        let mut frames = Vec::new();
        if let Ok(routes) = self.inner.routes.lock() {
            for route in routes.values() {
                route.handle.take_credit_frames(&mut frames);
            }
        }
        frames
    }

    fn has_credit_frames(&self) -> bool {
        self.inner.routes.lock().is_ok_and(|routes| {
            routes
                .values()
                .any(|route| route.handle.has_credit_frames())
        })
    }

    pub(crate) fn has_unsent_mux_writes(&self) -> bool {
        self.has_pending_event() || self.has_occupied_adapter_slot() || self.has_credit_frames()
    }

    fn has_occupied_adapter_slot(&self) -> bool {
        let Ok(routes) = self.inner.routes.lock() else {
            return false;
        };
        routes
            .values()
            .any(|route| route.handle.snapshot_active().is_some())
    }

    #[cfg(test)]
    pub(crate) fn has_bound_routes(&self) -> bool {
        self.inner
            .routes
            .lock()
            .is_ok_and(|routes| !routes.is_empty())
    }

    /// Live handle for an ingress container addressed to `route` at `generation`.
    ///
    /// A generation that does not match a live route yields `None`; the
    /// caller discards that frame for this key only.
    pub(crate) fn live_handle_for_route(
        &self,
        route: &str,
        generation: u64,
    ) -> Option<UnixTerminalAdapterHandle> {
        let Ok(routes) = self.inner.routes.lock() else {
            return None;
        };
        routes.values().rev().find_map(|candidate| {
            if candidate.subscription_id == route
                && candidate.generation == generation
                && !candidate.handle.is_closed()
            {
                Some(candidate.handle.clone())
            } else {
                None
            }
        })
    }

    /// Occupied, non-deferred write slots. Frames are `Arc` clones.
    pub(crate) fn snapshot_writes(&self) -> Vec<(UnixTerminalAdapterHandle, RoutedTerminalFrame)> {
        let Ok(routes) = self.inner.routes.lock() else {
            return Vec::new();
        };
        routes
            .values()
            .filter_map(|route| {
                if route.handle.is_flush_deferred() {
                    return None;
                }
                route
                    .handle
                    .snapshot_active()
                    .map(|frame| (route.handle.clone(), frame))
            })
            .collect()
    }

    pub(crate) async fn wait_for_write(&self) {
        self.inner.wake.wait().await;
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

/// One frame on the Unix close lane.
#[derive(Debug)]
pub(crate) enum UnixCloseLaneItem {
    Event(DaemonEvent),
    Credit(DaemonUnixCreditFrame),
}

impl UnixTerminalAdapterHandle {
    fn bind_route(&self, subscription_id: &str, generation: u64) {
        if let Some(mut credit) = self.inner.lock_credit() {
            credit.route = Some((subscription_id.to_string(), generation));
        }
    }

    /// Apply a client `GRANT` for this route generation.
    pub(crate) fn grant(&self, items: u32, bytes: u64) {
        self.inner.grant(items, bytes);
    }

    /// The route's `TerminalAttached` response is fully on the socket; its
    /// held credit frames may follow.
    pub(crate) fn mark_attach_written(&self) {
        let Some(mut credit) = self.inner.lock_credit() else {
            return;
        };
        credit.attach_written = true;
        let ready = credit.has_output();
        drop(credit);
        if ready {
            self.inner.slot.wake_transport();
        }
    }

    pub(crate) fn is_attach_written(&self) -> bool {
        self.inner
            .credit
            .lock()
            .is_ok_and(|credit| credit.attach_written)
    }

    fn has_credit_frames(&self) -> bool {
        self.inner
            .credit
            .lock()
            .is_ok_and(|credit| credit.has_output())
    }

    /// Move this route's queued credit frames to `frames`, the returned
    /// input credit coalesced into one `INPUT_CREDIT`.
    fn take_credit_frames(&self, frames: &mut Vec<DaemonUnixCreditFrame>) {
        let Some(mut credit) = self.inner.lock_credit() else {
            return;
        };
        if !credit.has_output() {
            return;
        }
        frames.extend(credit.outbox.drain(..));
        if credit.input_credit > 0
            && let Some((route, generation)) = credit.route.clone()
        {
            frames.push(DaemonUnixCreditFrame::InputCredit {
                route,
                generation,
                items: std::mem::take(&mut credit.input_credit),
            });
        }
    }

    /// Count one terminal frame of `body_len` bytes fully on the socket.
    pub(crate) fn record_written(&self, body_len: usize) {
        if let Some(mut credit) = self.inner.lock_credit() {
            credit
                .output
                .record_written(u64::try_from(body_len).unwrap_or(u64::MAX));
        }
    }

    /// Take this generation's `CLOSED`, once. Queued credit frames are
    /// dropped: `CLOSED` settles everything they carried, and it must be the
    /// route's last frame.
    fn settle(&self, subscription_id: &str, generation: u64) -> Option<DaemonUnixCreditFrame> {
        let mut credit = self.inner.lock_credit()?;
        if credit.settled {
            return None;
        }
        credit.settled = true;
        credit.outbox.clear();
        credit.input_credit = 0;
        let (spent_items, spent_bytes) = credit.output.spent();
        Some(DaemonUnixCreditFrame::Closed {
            route: subscription_id.to_string(),
            generation,
            spent_items,
            spent_bytes,
        })
    }

    /// Grant this route more credit than any test writes, as a client with
    /// a large budget does.
    #[cfg(test)]
    pub(crate) fn grant_unbounded_for_test(&self) {
        self.inner.grant(u32::MAX, u64::from(u32::MAX));
    }

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

    pub(crate) fn snapshot_active(&self) -> Option<RoutedTerminalFrame> {
        self.inner.snapshot_active()
    }

    pub(crate) fn complete_active(&self) -> Option<RoutedTerminalFrame> {
        self.inner.complete_active()
    }

    #[cfg(test)]
    pub(crate) fn write_opaque_frame(&self, frame: &RoutedTerminalFrame) {
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

    pub(crate) fn attach_close_hook(
        &self,
        hook: impl Fn(crate::transport::shared::close_reason::CloseReport) + Send + Sync + 'static,
    ) {
        self.inner.slot.attach_close_hook(hook);
    }

    /// Validate the input header and try to buffer one complete ingress
    /// frame. A full ingress hands the frame back; see `IngressStore`.
    pub(crate) fn try_push_ingress(
        &self,
        bytes: Vec<u8>,
    ) -> crate::transport::shared::ingress::IngressStore {
        self.inner.slot.try_push_ingress(bytes)
    }

    #[cfg(test)]
    pub(crate) fn mark_ingress_lost(&self) {
        self.inner.slot.mark_ingress_lost();
    }
}

impl ClosedHandle for UnixTerminalAdapterHandle {
    fn is_closed(&self) -> bool {
        UnixTerminalAdapterHandle::is_closed(self)
    }

    fn host_closed(&self) -> bool {
        UnixTerminalAdapterHandle::host_closed(self)
    }

    fn core_close_reason(
        &self,
    ) -> Option<botster_core::contract::terminal_adapter::TerminalRouteCloseReason> {
        self.inner.slot.core_close_reason()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core_test_support::terminal_adapter::{
        TerminalAdapterHarnessDriver, assert_terminal_adapter_conformance,
    };
    use botster_terminal_protocol::{RouteId, encode_output};

    /// H2: a bind that lands after the connection mux started dying must be
    /// refused, so the route never outlives `close_all`.
    #[test]
    fn dying_mux_refuses_late_registration() {
        let mux = UnixConnectionMux::new();
        let (_adapter, early) = mux.create_adapter();
        assert!(mux.register("s".to_string(), "early".to_string(), 1, early.clone()));
        mux.close_all();
        assert!(mux.is_dying());
        assert!(early.is_closed(), "close_all closes registered routes");
        let (_late_adapter, late) = mux.create_adapter();
        assert!(
            !mux.register("s".to_string(), "late".to_string(), 2, late.clone()),
            "a dying mux fails registration closed"
        );
        assert!(mux.route_handle("s", "late", 2).is_none());
    }

    mod credit_tests {
        use super::*;
        use botster_core::contract::terminal_wake::TerminalWakeSource;
        use botster_core::{SessionId, SubscriptionId, TerminalSubscriptionGeneration};
        use std::time::Duration;

        struct CreditedRoute {
            mux: UnixConnectionMux,
            adapter: UnixTerminalAdapter,
            handle: UnixTerminalAdapterHandle,
            wakes: TerminalWakeSource,
        }

        /// Route "sub" at generation 1 of session "s", Core's wake sink bound.
        fn credited_route(attach_written: bool) -> CreditedRoute {
            let mux = UnixConnectionMux::new();
            let (mut adapter, handle) = mux.create_adapter();
            assert!(mux.register("s".to_string(), "sub".to_string(), 1, handle.clone()));
            let wakes = TerminalWakeSource::new();
            adapter.set_wake_sink(wakes.bind_route(
                SessionId("s".into()),
                SubscriptionId("sub".into()),
                TerminalSubscriptionGeneration(1),
            ));
            if attach_written {
                handle.mark_attach_written();
            }
            CreditedRoute {
                mux,
                adapter,
                handle,
                wakes,
            }
        }

        /// Core Writable wakes raised so far. Every wake here is raised
        /// synchronously, so a zero timeout drains all of them.
        fn core_wakes(wakes: &TerminalWakeSource) -> usize {
            wakes.wait_wakes(Duration::ZERO).adapter_routes.len()
        }

        fn frame_bytes(frame: &RoutedTerminalFrame) -> u64 {
            u64::try_from(frame.frame.len()).expect("frame length fits u64")
        }

        fn demand(bytes: u64) -> DaemonUnixCreditFrame {
            DaemonUnixCreditFrame::Demand {
                route: "sub".to_string(),
                generation: 1,
                bytes,
            }
        }

        #[test]
        fn a_refused_head_demands_its_bytes_once_and_reads_as_would_block() {
            let mut route = credited_route(true);
            let frame = output_frame("sub", "head");
            assert_eq!(
                route.adapter.try_write(&frame),
                Err(TerminalAdapterWriteError::WouldBlock)
            );
            assert_eq!(
                route.adapter.pressure(),
                TerminalAdapterPressure::WouldBlock
            );
            assert!(
                route.handle.snapshot_active().is_none(),
                "a refusal debits nothing"
            );
            assert_eq!(
                route.mux.take_credit_frames(),
                vec![demand(frame_bytes(&frame))]
            );
            assert_eq!(
                route.adapter.try_write(&frame),
                Err(TerminalAdapterWriteError::WouldBlock)
            );
            assert!(
                route.mux.take_credit_frames().is_empty(),
                "a route has at most one outstanding demand"
            );
        }

        #[test]
        fn a_demand_waits_for_the_attach_response() {
            let mut route = credited_route(false);
            let frame = output_frame("sub", "early");
            assert_eq!(
                route.adapter.try_write(&frame),
                Err(TerminalAdapterWriteError::WouldBlock)
            );
            assert!(!route.mux.has_unsent_mux_writes());
            assert!(route.mux.take_credit_frames().is_empty());
            route.handle.mark_attach_written();
            assert_eq!(
                route.mux.take_credit_frames(),
                vec![demand(frame_bytes(&frame))]
            );
        }

        /// Plan test 8: a short grant raises nothing and demands the
        /// shortfall; the covering grant raises exactly one Writable wake.
        #[test]
        fn only_a_grant_that_covers_the_need_wakes_core_and_it_wakes_once() {
            let mut route = credited_route(true);
            let frame = output_frame("sub", "need");
            let bytes = frame_bytes(&frame);
            assert!(route.adapter.try_write(&frame).is_err());
            let _ = route.mux.take_credit_frames();
            let _ = core_wakes(&route.wakes);

            route.handle.grant(1, bytes - 1);
            assert_eq!(core_wakes(&route.wakes), 0, "a short grant raises nothing");
            assert_eq!(route.mux.take_credit_frames(), vec![demand(1)]);
            assert_eq!(
                route.adapter.pressure(),
                TerminalAdapterPressure::WouldBlock
            );

            route.handle.grant(1, 1);
            assert_eq!(core_wakes(&route.wakes), 1, "the covering grant wakes Core");
            assert_eq!(core_wakes(&route.wakes), 0, "and wakes it once");
            assert_eq!(route.adapter.pressure(), TerminalAdapterPressure::Ready);
            assert_eq!(route.adapter.try_write(&frame), Ok(()));
            assert!(route.mux.take_credit_frames().is_empty());
        }

        /// Plan test 8: credit granted before the adapter's check is used
        /// with no demand and no wake.
        #[test]
        fn credit_granted_before_the_check_is_used() {
            let mut route = credited_route(true);
            let frame = output_frame("sub", "ready");
            let _ = core_wakes(&route.wakes);
            route.handle.grant(1, frame_bytes(&frame));
            assert_eq!(
                core_wakes(&route.wakes),
                0,
                "a grant with no need raises nothing"
            );
            assert_eq!(route.adapter.try_write(&frame), Ok(()));
            assert!(route.mux.take_credit_frames().is_empty());
        }

        /// Plan test 5: Core drops a head that a grant covered, and the pool
        /// returns in one RETURN.
        #[test]
        fn a_withdrawn_head_returns_its_granted_credit() {
            let mut route = credited_route(true);
            let frame = output_frame("sub", "dropped");
            let bytes = frame_bytes(&frame);
            assert!(route.adapter.try_write(&frame).is_err());
            let _ = route.mux.take_credit_frames();
            route.handle.grant(1, bytes);
            route.adapter.head_withdrawn();
            assert_eq!(
                route.mux.take_credit_frames(),
                vec![DaemonUnixCreditFrame::Return {
                    route: "sub".to_string(),
                    generation: 1,
                    items: 1,
                    bytes,
                }]
            );
            assert_eq!(route.adapter.pressure(), TerminalAdapterPressure::Ready);
        }

        fn closed_frame(spent_items: u64, spent_bytes: u64) -> UnixCloseLaneItem {
            UnixCloseLaneItem::Credit(DaemonUnixCreditFrame::Closed {
                route: "sub".to_string(),
                generation: 1,
                spent_items,
                spent_bytes,
            })
        }

        fn close_lane(mux: &UnixConnectionMux) -> Vec<UnixCloseLaneItem> {
            std::iter::from_fn(|| mux.pop_pending_close()).collect()
        }

        /// CLOSED follows the close event, carries the written spend, and is
        /// sent once per generation. Plan test 7: a later grant is ignored.
        #[test]
        fn closed_follows_the_close_event_once_and_retires_the_generation() {
            let mut route = credited_route(true);
            let frame = output_frame("sub", "written");
            let bytes = frame_bytes(&frame);
            route.handle.grant(1, bytes);
            assert_eq!(route.adapter.try_write(&frame), Ok(()));
            route.handle.record_written(frame.frame.len());
            route.handle.close();
            assert_eq!(route.mux.queue_closed_subscription_events(|_| true), 1);
            let lane = close_lane(&route.mux);
            assert_eq!(lane.len(), 2, "{lane:?}");
            assert!(matches!(
                &lane[0],
                UnixCloseLaneItem::Event(DaemonEvent::TerminalSubscriptionClosed { .. })
            ));
            assert_eq!(
                format!("{:?}", lane[1]),
                format!("{:?}", closed_frame(1, bytes))
            );

            assert_eq!(route.mux.queue_closed_subscription_events(|_| true), 0);
            route.handle.grant(1, bytes);
            assert!(
                route.mux.take_credit_frames().is_empty(),
                "a retired grant is ignored"
            );
            assert!(
                route.mux.pop_pending_close().is_none(),
                "CLOSED is sent once"
            );
        }

        #[test]
        fn a_suppressed_close_event_still_settles_credit() {
            let route = credited_route(true);
            route.mux.suppress_generation("s", "sub", 1);
            route.handle.close();
            route.mux.queue_closed_subscription_events(|_| true);
            let lane = close_lane(&route.mux);
            assert_eq!(
                lane.iter()
                    .map(|item| format!("{item:?}"))
                    .collect::<Vec<_>>(),
                vec![format!("{:?}", closed_frame(0, 0))],
            );
        }

        #[test]
        fn a_route_of_an_ended_session_settles_without_an_event() {
            let route = credited_route(true);
            route.handle.close();
            route.mux.queue_closed_subscription_events(|_| false);
            let lane = close_lane(&route.mux);
            assert_eq!(
                lane.iter()
                    .map(|item| format!("{item:?}"))
                    .collect::<Vec<_>>(),
                vec![format!("{:?}", closed_frame(0, 0))],
            );
        }

        #[test]
        fn connection_loss_sends_no_closed() {
            let route = credited_route(true);
            route.mux.close_all();
            route.mux.queue_closed_subscription_events(|_| true);
            assert!(route.mux.pop_pending_close().is_none());
        }

        /// Settlement drops queued credit frames: CLOSED is the route's last
        /// credit frame.
        #[test]
        fn settlement_drops_queued_credit_frames() {
            let mut route = credited_route(true);
            assert!(
                route
                    .adapter
                    .try_write(&output_frame("sub", "queued"))
                    .is_err()
            );
            route.handle.close();
            route.mux.queue_closed_subscription_events(|_| true);
            let lane = close_lane(&route.mux);
            assert!(matches!(lane.last(), Some(UnixCloseLaneItem::Credit(_))));
            assert!(route.mux.take_credit_frames().is_empty());
        }
    }

    pub(crate) fn output_frame(route: &str, marker: &str) -> RoutedTerminalFrame {
        RoutedTerminalFrame::new(
            RouteId::new(route).expect("route"),
            1,
            0,
            encode_output(marker.as_bytes()).expect("output frame"),
        )
    }

    struct UnixTerminalAdapterDriver {
        adapter: UnixTerminalAdapter,
        handle: UnixTerminalAdapterHandle,
        delivered: Vec<Vec<u8>>,
    }

    impl Default for UnixTerminalAdapterDriver {
        fn default() -> Self {
            let (adapter, handle) = UnixTerminalAdapter::pair();
            // Core's harness checks the slot contract, so the route holds
            // more credit than the harness writes.
            handle.grant_unbounded_for_test();
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
    fn production_unix_adapter_passes_core_conformance_harness() {
        let mut driver = UnixTerminalAdapterDriver::default();
        assert_terminal_adapter_conformance(&mut driver);
    }

    #[test]
    fn host_close_after_core_close_does_not_claim_host_reason() {
        let (mut adapter, handle) = UnixTerminalAdapter::pair();
        adapter.close(botster_core::contract::terminal_adapter::TerminalRouteCloseReason::Detached);
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
        assert!(mux.register("stall".to_string(), "sub".to_string(), 1, handle.clone()));
        handle.grant_unbounded_for_test();
        assert_eq!(adapter.try_write(&output_frame("sub", "flood")), Ok(()));
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
    fn slot_shares_the_body_without_copying_it() {
        let (mut adapter, handle) = UnixTerminalAdapter::pair();
        handle.grant_unbounded_for_test();
        let frame = output_frame("sub", "shared");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        let active = handle.snapshot_active().expect("occupied slot");
        assert!(Arc::ptr_eq(
            active.frame.shared_bytes(),
            frame.frame.shared_bytes()
        ));
        assert_eq!(active.route.as_str(), "sub");
        assert_eq!(active.generation, 1);
    }

    #[test]
    fn ingress_lookup_requires_the_live_generation() {
        let mux = UnixConnectionMux::new();
        let (_adapter, handle) = mux.create_adapter();
        assert!(mux.register("session".to_string(), "sub".to_string(), 3, handle.clone()));
        assert!(mux.live_handle_for_route("sub", 3).is_some());
        assert!(
            mux.live_handle_for_route("sub", 2).is_none(),
            "a stale generation is discarded for that key only"
        );
        assert!(mux.live_handle_for_route("other", 3).is_none());
        handle.close();
        assert!(mux.live_handle_for_route("sub", 3).is_none());
    }

    #[tokio::test]
    async fn unix_mux_retains_a_write_wake_before_the_connection_waits() {
        let mux = UnixConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter();
        handle.grant_unbounded_for_test();
        assert_eq!(adapter.try_write(&output_frame("sub", "early")), Ok(()));
        tokio::time::timeout(std::time::Duration::from_millis(50), mux.wait_for_write())
            .await
            .expect("a Unix adapter write before waiter registration must retain its wake");
    }

    #[test]
    fn close_does_not_wait_on_occupied_slot() {
        let (mut adapter, handle) = UnixTerminalAdapter::pair();
        handle.grant_unbounded_for_test();
        assert_eq!(adapter.try_write(&output_frame("sub", "in-flight")), Ok(()));
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
            assert!(mux.register(format!("open-{index:02}"), "sub".to_string(), 1, handle));
            open_adapters.push(adapter);
        }
        let mut reported_handles = Vec::new();
        for index in 0..4 {
            let (_adapter, handle) = mux.create_adapter();
            assert!(mux.register(
                format!("reported-{index:02}"),
                "sub".to_string(),
                1,
                handle.clone(),
            ));
            handle.close();
            reported_handles.push(handle);
        }
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 4);
        let (_closed_adapter, closed) = mux.create_adapter();
        assert!(mux.register("z-closed".to_string(), "sub".to_string(), 1, closed.clone()));
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
    #[test]
    fn worker_lost_close_is_reported_whatever_the_registry_state() {
        let mux = UnixConnectionMux::new();
        let (mut lost, lost_handle) = mux.create_adapter();
        assert!(mux.register("lost".into(), "sub".into(), 5, lost_handle.clone()));
        let (mut ended, ended_handle) = mux.create_adapter();
        assert!(mux.register("ended".into(), "sub".into(), 6, ended_handle.clone()));
        TerminalAdapter::close(
            &mut lost,
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::WorkerLinkFailed,
        );
        TerminalAdapter::close(
            &mut ended,
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::SessionEnded,
        );
        // The registry no longer calls either session live (Stale / Exited).
        // Both closed routes are classified; only the lost worker's emits.
        assert_eq!(mux.queue_closed_subscription_events(|_| false), 2);
        match mux.pop_pending_event() {
            Some(DaemonEvent::TerminalSubscriptionClosed {
                session_id,
                subscription_id,
                generation,
                reason,
            }) => {
                assert_eq!(
                    (session_id.as_str(), subscription_id.as_str(), generation),
                    ("lost", "sub", 5)
                );
                assert_eq!(
                    reason,
                    botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_WORKER_LOST
                );
            }
            other => panic!("expected the worker_lost close event, got {other:?}"),
        }
        assert!(
            mux.pop_pending_event().is_none(),
            "an ended session's close stays silent"
        );
    }

    #[test]
    fn only_the_first_close_carries_the_core_reason() {
        let (mut core_first, core_first_handle) = UnixTerminalAdapter::pair();
        TerminalAdapter::close(
            &mut core_first,
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::Replaced,
        );
        TerminalAdapter::close(
            &mut core_first,
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::WorkerLinkFailed,
        );
        assert_eq!(
            ClosedHandle::core_close_reason(&core_first_handle),
            Some(botster_core::contract::terminal_adapter::TerminalRouteCloseReason::Replaced)
        );
        let (mut host_first, host_first_handle) = UnixTerminalAdapter::pair();
        host_first_handle.close_from_host();
        TerminalAdapter::close(
            &mut host_first,
            botster_core::contract::terminal_adapter::TerminalRouteCloseReason::WorkerLinkFailed,
        );
        assert_eq!(ClosedHandle::core_close_reason(&host_first_handle), None);
    }

    /// Both close orders race through the one-slot cause: the competing
    /// close runs inside the first close's window (the commit seam). The
    /// winner decides every hook report, the handle's state and the mux
    /// event; late and repeated closes change none of them.
    #[test]
    fn competing_host_and_core_closes_report_only_the_first_close() {
        for core_starts in [true, false] {
            let mux = UnixConnectionMux::new();
            let (mut adapter, handle) = mux.create_adapter();
            assert!(mux.register("s".into(), "sub".into(), 1, handle.clone()));
            let reports = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let captured = std::sync::Arc::clone(&reports);
            handle.attach_close_hook(move |report| captured.lock().unwrap().push(report));
            let competitor = handle.clone();
            if core_starts {
                // Core observed open; the Host close lands before its commit.
                handle
                    .inner
                    .slot
                    .set_close_commit_seam(move || competitor.close_from_host());
                TerminalAdapter::close(&mut adapter, botster_core::contract::terminal_adapter::TerminalRouteCloseReason::WorkerLinkFailed);
            } else {
                handle.inner.slot.set_close_commit_seam({
                    let slot_handle = handle.clone();
                    move || slot_handle.inner.slot.close_from_core(botster_core::contract::terminal_adapter::TerminalRouteCloseReason::WorkerLinkFailed)
                });
                handle.close_from_host();
            }
            // Late and repeated closes from every side.
            TerminalAdapter::close(&mut adapter, botster_core::contract::terminal_adapter::TerminalRouteCloseReason::WorkerLinkFailed);
            TerminalAdapter::close(
                &mut adapter,
                botster_core::contract::terminal_adapter::TerminalRouteCloseReason::Replaced,
            );
            handle.close_from_host();
            drop(adapter);
            // The competitor inside the window commits first, so it wins.
            let (host_won, worker_lost) = (core_starts, !core_starts);
            let winner = crate::transport::shared::close_reason::CloseReport {
                host_closed: host_won,
                worker_lost,
            };
            let reports = reports.lock().unwrap().clone();
            assert!(!reports.is_empty(), "core_starts={core_starts}");
            assert!(
                reports.iter().all(|report| *report == winner),
                "core_starts={core_starts} reports={reports:?}"
            );
            assert_eq!(ClosedHandle::host_closed(&handle), host_won);
            assert_eq!(
                ClosedHandle::core_close_reason(&handle),
                (!core_starts).then_some(botster_core::contract::terminal_adapter::TerminalRouteCloseReason::WorkerLinkFailed)
            );
            // The registry calls the session ended: only a lost worker reports.
            mux.queue_closed_subscription_events(|_| false);
            match mux.pop_pending_event() {
                Some(DaemonEvent::TerminalSubscriptionClosed { reason, .. }) => {
                    assert!(!core_starts, "a Host-won close must stay silent here");
                    assert_eq!(
                        reason,
                        botster_hub_client::TERMINAL_SUBSCRIPTION_CLOSED_WORKER_LOST
                    );
                }
                None => assert!(core_starts, "a Core-won worker loss must report"),
                other => panic!("unexpected event {other:?}"),
            }
        }
    }
}
