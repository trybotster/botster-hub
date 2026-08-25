//! Production WebRTC terminal adapter and Core harness driver.
//!
//! The adapter owns one in-flight write slot. `try_write` serializes an opaque
//! [`TerminalFrame`] and does not inspect snapshot phases or snapshot bodies.
//! `close` and `Drop` return without waiting on DataChannel I/O or a writer lock.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, TryLockError, Weak};
use std::time::{Duration, Instant};

use botster_core::contract::terminal_adapter::{
    MIN_ADAPTER_INGRESS_BUFFER_FRAMES, TerminalAdapter, TerminalAdapterPressure,
    TerminalAdapterWriteError, TerminalIngress,
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

struct AdapterIngress {
    frames: VecDeque<Vec<u8>>,
    partial: Option<Vec<u8>>,
    lost_pending: bool,
}

impl AdapterIngress {
    fn new() -> Self {
        Self {
            frames: VecDeque::with_capacity(MIN_ADAPTER_INGRESS_BUFFER_FRAMES),
            partial: None,
            lost_pending: false,
        }
    }

    fn clear(&mut self) {
        self.frames.clear();
        self.partial = None;
        self.lost_pending = false;
    }

    fn take(&mut self) -> TerminalIngress {
        if self.lost_pending {
            self.lost_pending = false;
            return TerminalIngress::Lost;
        }
        match self.frames.pop_front() {
            Some(frame) => TerminalIngress::Frame(frame),
            None => TerminalIngress::Empty,
        }
    }

    fn push_frame(&mut self, bytes: Vec<u8>) {
        if self.frames.len() >= MIN_ADAPTER_INGRESS_BUFFER_FRAMES {
            self.lost_pending = true;
            return;
        }
        self.frames.push_back(bytes);
    }
}

struct WebRtcTerminalAdapterInner {
    closed: AtomicBool,
    host_closed: AtomicBool,
    would_block: AtomicBool,
    buffered_bytes: AtomicU32,
    mux: Weak<WebRtcMuxInner>,
    slot: Mutex<Option<Vec<u8>>>,
    host_out: Mutex<VecDeque<Vec<u8>>>,
    ingress: Mutex<AdapterIngress>,
    wake: AdapterWake,
    close_work: Arc<AtomicBool>,
}

impl WebRtcTerminalAdapterInner {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            host_closed: AtomicBool::new(false),
            would_block: AtomicBool::new(false),
            buffered_bytes: AtomicU32::new(0),
            mux: Weak::new(),
            slot: Mutex::new(None),
            host_out: Mutex::new(VecDeque::new()),
            ingress: Mutex::new(AdapterIngress::new()),
            wake: AdapterWake::new(),
            close_work: Arc::new(AtomicBool::new(false)),
        }
    }

    fn aggregate_would_block(&self) -> bool {
        let Some(mux) = self.mux.upgrade() else {
            return self.would_block.load(Ordering::SeqCst);
        };
        let aggregate = aggregate_buffered_from(&mux);
        if aggregate < crate::local_webrtc::webrtc_subscription_channel::AGGREGATE_BUFFERED_LOW {
            self.would_block.store(false, Ordering::SeqCst);
            return false;
        }
        self.would_block.load(Ordering::SeqCst)
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
        self.close_work.store(true, Ordering::SeqCst);
        match self.slot.try_lock() {
            Ok(mut slot) => {
                *slot = None;
            }
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => {
                *poisoned.into_inner() = None;
            }
        }
        match self.ingress.try_lock() {
            Ok(mut ingress) => ingress.clear(),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => {
                poisoned.into_inner().clear();
            }
        }
        match self.host_out.try_lock() {
            Ok(mut pending) => pending.clear(),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => {
                poisoned.into_inner().clear();
            }
        }
        self.wake.wake();
    }

    fn enqueue_host_frame(&self, bytes: Vec<u8>) {
        if self.is_closed() {
            return;
        }
        match self.slot.lock() {
            Ok(mut slot) => {
                if self.is_closed() {
                    *slot = None;
                    return;
                }
                if slot.is_none() {
                    *slot = Some(bytes);
                } else if let Ok(mut pending) = self.host_out.lock() {
                    pending.push_back(bytes);
                }
            }
            Err(poisoned) => {
                *poisoned.into_inner() = None;
            }
        }
        self.wake.wake();
    }

    fn try_read(&self) -> TerminalIngress {
        if self.is_closed() {
            return TerminalIngress::Closed;
        }
        match self.ingress.try_lock() {
            Ok(mut ingress) => {
                if self.is_closed() {
                    ingress.clear();
                    TerminalIngress::Closed
                } else {
                    ingress.take()
                }
            }
            Err(TryLockError::WouldBlock) => TerminalIngress::Empty,
            Err(TryLockError::Poisoned(_)) => TerminalIngress::Closed,
        }
    }

    fn push_ingress_frame(&self, bytes: Vec<u8>) {
        if self.is_closed() {
            return;
        }
        match self.ingress.try_lock() {
            Ok(mut ingress) => {
                if !self.is_closed() {
                    ingress.push_frame(bytes);
                }
            }
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => {
                poisoned.into_inner().clear();
            }
        }
    }

    #[cfg(test)]
    fn push_ingress_partial(&self, bytes: Vec<u8>) {
        if self.is_closed() {
            return;
        }
        if let Ok(mut ingress) = self.ingress.try_lock()
            && !self.is_closed()
        {
            ingress.partial = Some(bytes);
        }
    }

    #[cfg(test)]
    fn complete_ingress_partial(&self) {
        if self.is_closed() {
            return;
        }
        if let Ok(mut ingress) = self.ingress.try_lock()
            && !self.is_closed()
            && let Some(bytes) = ingress.partial.take()
        {
            ingress.push_frame(bytes);
        }
    }

    #[cfg(test)]
    fn drop_buffered_ingress_frame(&self) {
        if self.is_closed() {
            return;
        }
        if let Ok(mut ingress) = self.ingress.try_lock()
            && !self.is_closed()
            && ingress.frames.pop_back().is_some()
        {
            ingress.lost_pending = true;
        }
    }

    #[cfg(test)]
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
                } else if self.aggregate_would_block() {
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
        if let Some(mux) = self.mux.upgrade() {
            let aggregate = aggregate_buffered_from(&mux);
            if crate::local_webrtc::webrtc_subscription_channel::refuse_send_on_aggregate(
                aggregate,
                bytes.len() as u32,
            ) {
                self.would_block.store(true, Ordering::SeqCst);
                return Err(TerminalAdapterWriteError::WouldBlock);
            }
        }
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
                    let taken = slot.take();
                    if let Ok(mut pending) = self.host_out.lock() {
                        *slot = pending.pop_front();
                    }
                    taken
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
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn pair() -> (Self, WebRtcTerminalAdapterHandle) {
        Self::pair_with_wake(AdapterWake::new())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn pair_with_wake(wake: AdapterWake) -> (Self, WebRtcTerminalAdapterHandle) {
        Self::pair_with_wake_and_close_work(wake, Arc::new(AtomicBool::new(false)))
    }

    fn pair_with_wake_and_close_work(
        wake: AdapterWake,
        close_work: Arc<AtomicBool>,
    ) -> (Self, WebRtcTerminalAdapterHandle) {
        Self::pair_with_wake_close_work_and_mux(wake, close_work, Weak::new())
    }

    fn pair_with_wake_close_work_and_mux(
        wake: AdapterWake,
        close_work: Arc<AtomicBool>,
        mux: Weak<WebRtcMuxInner>,
    ) -> (Self, WebRtcTerminalAdapterHandle) {
        let mut inner = WebRtcTerminalAdapterInner::new();
        inner.wake = wake;
        inner.close_work = close_work;
        inner.mux = mux;
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
        self.inner.set_would_block(true);
    }

    #[cfg(test)]
    fn clear_would_block(&self) {
        self.inner.set_would_block(false);
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn push_ingress_frame(&self, bytes: Vec<u8>) {
        self.inner.push_ingress_frame(bytes);
    }

    #[cfg(test)]
    pub(crate) fn push_ingress_partial(&self, bytes: Vec<u8>) {
        self.inner.push_ingress_partial(bytes);
    }

    #[cfg(test)]
    pub(crate) fn complete_ingress_partial(&self) {
        self.inner.complete_ingress_partial();
    }

    #[cfg(test)]
    pub(crate) fn drop_buffered_ingress_frame(&self) {
        self.inner.drop_buffered_ingress_frame();
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
    routes: Mutex<BTreeMap<(String, String, u64), WebRtcMuxRoute>>,
    pending_events: Mutex<Vec<DaemonEvent>>,
    suppress_generations: Mutex<BTreeSet<(String, String, u64)>>,
    close_work: Mutex<Arc<AtomicBool>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MuxRouteState {
    Reserved,
    Bound,
    Retired,
}

struct WebRtcMuxRoute {
    session_id: String,
    subscription_id: String,
    generation: u64,
    state: MuxRouteState,
    handle: Option<WebRtcTerminalAdapterHandle>,
    reported: bool,
    reserved_at: Option<Instant>,
}

fn aggregate_buffered_from(inner: &WebRtcMuxInner) -> u32 {
    let Ok(routes) = inner.routes.lock() else {
        return 0;
    };
    routes
        .values()
        .filter(|route| matches!(route.state, MuxRouteState::Reserved | MuxRouteState::Bound))
        .filter_map(|route| route.handle.as_ref())
        .map(WebRtcTerminalAdapterHandle::buffered_bytes)
        .fold(0u32, u32::saturating_add)
}

impl WebRtcConnectionMux {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(WebRtcMuxInner {
                wake: AdapterWake::new(),
                dying: AtomicBool::new(false),
                close_events_admitted: AtomicBool::new(false),
                routes: Mutex::new(BTreeMap::new()),
                pending_events: Mutex::new(Vec::new()),
                suppress_generations: Mutex::new(BTreeSet::new()),
                close_work: Mutex::new(Arc::new(AtomicBool::new(false))),
            }),
        }
    }

    pub(crate) fn bind_close_work(&self, flag: Arc<AtomicBool>) {
        if let Ok(mut slot) = self.inner.close_work.lock() {
            *slot = flag;
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
        WebRtcTerminalAdapter::pair_with_wake_close_work_and_mux(
            self.inner.wake.clone(),
            close_work,
            Arc::downgrade(&self.inner),
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

    #[cfg_attr(not(test), allow(dead_code))]
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
                WebRtcMuxRoute {
                    session_id,
                    subscription_id,
                    generation,
                    state: MuxRouteState::Bound,
                    handle: Some(handle),
                    reported: false,
                    reserved_at: None,
                },
            );
        }
        self.inner.wake.wake();
    }

    pub(crate) fn has_bound_routes(&self) -> bool {
        self.inner.routes.lock().is_ok_and(|routes| {
            routes
                .values()
                .any(|route| route.state == MuxRouteState::Bound && route.handle.is_some())
        })
    }

    pub(crate) fn aggregate_buffered(&self) -> u32 {
        aggregate_buffered_from(&self.inner)
    }

    pub(crate) fn charged_subscription_count(&self) -> usize {
        self.inner.routes.lock().map_or(0, |routes| {
            routes
                .values()
                .filter(|route| {
                    matches!(route.state, MuxRouteState::Reserved | MuxRouteState::Bound)
                })
                .count()
        })
    }

    pub(crate) fn reserve_terminal(
        &self,
        session_id: String,
        subscription_id: String,
        generation: u64,
    ) -> Result<
        crate::local_webrtc::webrtc_subscription_channel::SubscriptionChannelLabel,
        &'static str,
    > {
        use crate::local_webrtc::webrtc_subscription_channel::{
            SubscriptionChannelLabel, reject_admission_on_aggregate, reject_admission_on_count,
        };
        if reject_admission_on_count(self.charged_subscription_count()) {
            return Err("subscription_channel_limit");
        }
        if reject_admission_on_aggregate(self.aggregate_buffered()) {
            return Err("subscription_channel_aggregate");
        }
        let label = SubscriptionChannelLabel::terminal(
            session_id.clone(),
            subscription_id.clone(),
            generation,
        );
        let Ok(mut routes) = self.inner.routes.lock() else {
            return Err("subscription_channel_limit");
        };
        let key = (session_id.clone(), subscription_id.clone(), generation);
        if routes.contains_key(&key) {
            return Err("subscription_channel_duplicate");
        }
        routes.insert(
            key,
            WebRtcMuxRoute {
                session_id,
                subscription_id,
                generation,
                state: MuxRouteState::Reserved,
                handle: None,
                reported: false,
                reserved_at: Some(Instant::now()),
            },
        );
        Ok(label)
    }

    pub(crate) fn open_event_view(
        &self,
        label: &crate::local_webrtc::webrtc_subscription_channel::SubscriptionChannelLabel,
    ) -> crate::local_webrtc::webrtc_subscription_channel::OpenEventView {
        use crate::local_webrtc::webrtc_subscription_channel::{
            OpenEventView, SubscriptionRouteState,
        };
        let matching_state = self.inner.routes.lock().ok().and_then(|routes| {
            routes
                .get(&(
                    label.session_id.clone(),
                    label.subscription_id.clone(),
                    label.generation,
                ))
                .map(|route| match route.state {
                    MuxRouteState::Reserved => SubscriptionRouteState::Reserved,
                    MuxRouteState::Bound => SubscriptionRouteState::Bound,
                    MuxRouteState::Retired => SubscriptionRouteState::Retired,
                })
        });
        OpenEventView {
            label: label.clone(),
            matching_state,
            charged_subscription_count: self.charged_subscription_count(),
            generation_suppressed: self.generation_is_suppressed(
                &label.session_id,
                &label.subscription_id,
                label.generation,
            ),
            peer_dying: self.is_dying(),
        }
    }

    pub(crate) fn bound_handle(
        &self,
        label: &crate::local_webrtc::webrtc_subscription_channel::SubscriptionChannelLabel,
    ) -> Option<WebRtcTerminalAdapterHandle> {
        let routes = self.inner.routes.lock().ok()?;
        routes
            .get(&(
                label.session_id.clone(),
                label.subscription_id.clone(),
                label.generation,
            ))
            .and_then(|route| {
                if route.state == MuxRouteState::Bound {
                    route.handle.clone()
                } else {
                    None
                }
            })
    }

    pub(crate) fn bind_reserved(
        &self,
        label: &crate::local_webrtc::webrtc_subscription_channel::SubscriptionChannelLabel,
        handle: WebRtcTerminalAdapterHandle,
    ) -> bool {
        let Ok(mut routes) = self.inner.routes.lock() else {
            return false;
        };
        let Some(route) = routes.get_mut(&(
            label.session_id.clone(),
            label.subscription_id.clone(),
            label.generation,
        )) else {
            return false;
        };
        if route.state != MuxRouteState::Reserved {
            return false;
        }
        route.state = MuxRouteState::Bound;
        route.handle = Some(handle);
        self.inner.wake.wake();
        true
    }

    pub(crate) fn retire_reserved(
        &self,
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) -> bool {
        let Ok(mut routes) = self.inner.routes.lock() else {
            return false;
        };
        let Some(route) = routes.get_mut(&(
            session_id.to_string(),
            subscription_id.to_string(),
            generation,
        )) else {
            return false;
        };
        if route.state != MuxRouteState::Reserved {
            return false;
        }
        route.state = MuxRouteState::Retired;
        route.handle = None;
        true
    }

    pub(crate) fn close_all(&self) {
        self.inner.dying.store(true, Ordering::SeqCst);
        if let Ok(mut routes) = self.inner.routes.lock() {
            for (_, route) in std::mem::take(&mut *routes) {
                if let Some(handle) = route.handle {
                    handle.close();
                }
            }
        }
        self.inner.wake.wake();
    }

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
        if let Ok(mut generations) = self.inner.suppress_generations.lock() {
            generations.extend(keys);
        }
    }

    pub(crate) fn suppress_generation(
        &self,
        session_id: impl Into<String>,
        subscription_id: impl Into<String>,
        generation: u64,
    ) {
        if let Ok(mut generations) = self.inner.suppress_generations.lock() {
            generations.insert((session_id.into(), subscription_id.into(), generation));
        }
    }

    fn generation_is_suppressed(
        &self,
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) -> bool {
        self.inner
            .suppress_generations
            .lock()
            .is_ok_and(|generations| {
                generations.contains(&(
                    session_id.to_string(),
                    subscription_id.to_string(),
                    generation,
                ))
            })
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

    pub(crate) fn queue_closed_subscription_events_bounded(
        &self,
        mut classify: impl FnMut(&str) -> Option<bool>,
        max_candidates: usize,
        after_route: Option<&(String, String, u64)>,
        max_entries_visited: usize,
    ) -> crate::unix_terminal_adapter::ClosedEventSliceProgress {
        if self.is_dying() {
            if let Ok(mut routes) = self.inner.routes.lock() {
                for route in routes.values_mut() {
                    route.reported = true;
                }
            }
            return crate::unix_terminal_adapter::ClosedEventSliceProgress {
                classified: 0,
                more: false,
                after_route: None,
            };
        }
        let mut queued = Vec::new();
        let mut classified = 0;
        let mut visited = 0;
        let mut more = false;
        let mut last_visited = after_route.cloned();
        if let Ok(mut routes) = self.inner.routes.lock() {
            let start = match after_route {
                Some(after) => Bound::Excluded(after.clone()),
                None => Bound::Unbounded,
            };
            for (key, route) in routes.range_mut((start, Bound::Unbounded)) {
                if visited >= max_entries_visited {
                    more = true;
                    break;
                }
                let handle_closed = route
                    .handle
                    .as_ref()
                    .is_some_and(|handle| handle.is_closed());
                if !route.reported && handle_closed && classified >= max_candidates {
                    more = true;
                    break;
                }
                visited += 1;
                last_visited = Some(key.clone());
                if route.reported || !handle_closed {
                    continue;
                }
                classified += 1;
                if self.generation_is_suppressed(
                    &route.session_id,
                    &route.subscription_id,
                    route.generation,
                ) {
                    route.reported = true;
                    continue;
                }
                match classify(&route.session_id) {
                    None => continue,
                    Some(false) => {
                        route.reported = true;
                        continue;
                    }
                    Some(true) => route.reported = true,
                }
                let reason = if route
                    .handle
                    .as_ref()
                    .is_some_and(WebRtcTerminalAdapterHandle::host_closed)
                {
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
        if !queued.is_empty() {
            if let Ok(mut pending) = self.inner.pending_events.lock() {
                pending.extend(queued);
            }
            self.inner.wake.wake();
        }
        crate::unix_terminal_adapter::ClosedEventSliceProgress {
            classified,
            more,
            after_route: last_visited,
        }
    }

    pub(crate) fn has_pending_event(&self) -> bool {
        self.inner
            .pending_events
            .lock()
            .ok()
            .is_some_and(|pending| !pending.is_empty())
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

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn snapshot_writes(
        &self,
    ) -> Vec<(String, String, WebRtcTerminalAdapterHandle, Vec<u8>)> {
        let Ok(routes) = self.inner.routes.lock() else {
            return Vec::new();
        };
        routes
            .values()
            .filter_map(|route| {
                let handle = route.handle.as_ref()?;
                if handle.is_closed() {
                    return None;
                }
                handle.snapshot_active().map(|bytes| {
                    (
                        route.session_id.clone(),
                        route.subscription_id.clone(),
                        handle.clone(),
                        bytes,
                    )
                })
            })
            .collect()
    }

    pub(crate) fn snapshot_write_for(
        &self,
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) -> Option<(WebRtcTerminalAdapterHandle, Vec<u8>)> {
        let routes = self.inner.routes.lock().ok()?;
        let route = routes.get(&(
            session_id.to_string(),
            subscription_id.to_string(),
            generation,
        ))?;
        let handle = route.handle.as_ref()?;
        if handle.is_closed() {
            return None;
        }
        handle
            .snapshot_active()
            .map(|bytes| (handle.clone(), bytes))
    }

    pub(crate) fn queue_host_event(&self, event: DaemonEvent) {
        if let Ok(mut pending) = self.inner.pending_events.lock() {
            pending.push(event);
        }
        self.inner.wake.wake();
    }

    pub(crate) fn expire_reserved_opens(&self, now: Instant, bound: Duration) {
        let expired = {
            let Ok(routes) = self.inner.routes.lock() else {
                return;
            };
            routes
                .values()
                .filter(|route| {
                    route.state == MuxRouteState::Reserved
                        && route.reserved_at.is_some_and(|reserved_at| {
                            now.saturating_duration_since(reserved_at) >= bound
                        })
                })
                .map(|route| {
                    (
                        route.session_id.clone(),
                        route.subscription_id.clone(),
                        route.generation,
                    )
                })
                .collect::<Vec<_>>()
        };
        for (session_id, subscription_id, generation) in expired {
            if self.retire_reserved(&session_id, &subscription_id, generation)
                && self.close_events_admitted()
            {
                self.queue_host_event(DaemonEvent::SubscriptionChannelOpenTimeout {
                    session_id,
                    subscription_id,
                    generation,
                });
            }
        }
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

    pub(crate) fn snapshot_active(&self) -> Option<Vec<u8>> {
        self.inner.snapshot_active()
    }

    pub(crate) fn complete_active(&self) -> Option<Vec<u8>> {
        self.inner.complete_active()
    }

    pub(crate) fn write_opaque_frame(&self, frame: &TerminalFrame) {
        let Ok(bytes) = frame.to_bytes() else {
            return;
        };
        self.inner.enqueue_host_frame(bytes);
    }

    pub(crate) fn push_ingress_frame(&self, bytes: Vec<u8>) {
        self.inner.push_ingress_frame(bytes);
    }

    pub(crate) fn buffered_bytes(&self) -> u32 {
        self.inner.buffered_bytes.load(Ordering::SeqCst)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn set_buffered_bytes(&self, bytes: u32) {
        self.inner.buffered_bytes.store(bytes, Ordering::SeqCst);
        if bytes < crate::local_webrtc::webrtc_subscription_channel::AGGREGATE_BUFFERED_LOW {
            self.inner.would_block.store(false, Ordering::SeqCst);
        }
        self.inner.wake.wake();
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

        fn inject_ingress_frame(&mut self, bytes: Vec<u8>) {
            self.adapter.push_ingress_frame(bytes);
        }

        fn inject_ingress_partial(&mut self, bytes: Vec<u8>) {
            self.adapter.push_ingress_partial(bytes);
        }

        fn complete_ingress_partial(&mut self) {
            self.adapter.complete_ingress_partial();
        }

        fn drop_buffered_ingress_frame(&mut self) {
            self.adapter.drop_buffered_ingress_frame();
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
    fn close_event_slice_uses_keyed_suppression_without_cloning_the_prefix() {
        let mux = WebRtcConnectionMux::new();
        for index in 0..64 {
            mux.suppress_generation(format!("suppressed-{index:03}"), "sub", 1);
        }
        let mut open_adapters = Vec::new();
        for index in 0..8 {
            let (adapter, handle) = mux.create_adapter();
            mux.register(format!("open-{index:02}"), "sub".to_string(), 1, handle);
            open_adapters.push(adapter);
        }
        let (_suppressed_adapter, suppressed) = mux.create_adapter();
        mux.register(
            "suppressed-000".to_string(),
            "sub".to_string(),
            1,
            suppressed.clone(),
        );
        suppressed.close();
        let (_live_adapter, live) = mux.create_adapter();
        mux.register("z-live".to_string(), "sub".to_string(), 1, live.clone());
        live.close();
        let first = mux.queue_closed_subscription_events_bounded(|_| Some(true), 8, None, 8);
        assert_eq!(first.classified, 0);
        let second = mux.queue_closed_subscription_events_bounded(
            |_| Some(true),
            8,
            first.after_route.as_ref(),
            8,
        );
        assert_eq!(second.classified, 2);
        match mux.pop_pending_event() {
            Some(DaemonEvent::TerminalSubscriptionClosed { session_id, .. }) => {
                assert_eq!(session_id, "z-live");
            }
            other => panic!("expected live close event, got {other:?}"),
        }
        let _ = open_adapters;
    }

    #[test]
    fn exact_generation_suppression_silences_running_close_and_preserves_later_generation() {
        let mux = WebRtcConnectionMux::new();
        let (_dying_adapter, dying) = mux.create_adapter();
        mux.register("s".to_string(), "sub".to_string(), 4, dying.clone());
        mux.suppress_session_route_generations("s");
        dying.close();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);
        assert!(
            mux.pop_pending_event().is_none(),
            "suppressed generation must stay silent while the classifier answers Running"
        );

        let (_host_adapter, host) = mux.create_adapter();
        mux.register("s".to_string(), "sub-host".to_string(), 4, host.clone());
        mux.suppress_session_route_generations("s");
        host.close_from_host();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);
        assert!(
            mux.pop_pending_event().is_none(),
            "host-close under exact-key suppression must not emit"
        );

        let (_later_adapter, later) = mux.create_adapter();
        mux.register("s".to_string(), "sub".to_string(), 5, later.clone());
        later.close();
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
                assert_eq!(generation, 5);
                assert_eq!(reason, TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER);
            }
            other => panic!("later generation must still emit, got {other:?}"),
        }
        assert!(mux.pop_pending_event().is_none());
    }

    #[test]
    fn empty_session_snapshot_installs_no_suppression_keys() {
        let mux = WebRtcConnectionMux::new();
        mux.suppress_session_route_generations("missing");
        let (_adapter, handle) = mux.create_adapter();
        mux.register("missing".to_string(), "sub".to_string(), 1, handle.clone());
        handle.close();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);
        assert!(
            mux.pop_pending_event().is_some(),
            "a later attach after a missing-session snapshot must still emit"
        );
    }

    #[test]
    fn a25_a26_a27_exact_aggregate_ceiling_refuses_before_write_and_drains_to_zero() {
        use crate::local_webrtc::webrtc_subscription_channel::AGGREGATE_BUFFERED_HIGH;
        use botster_core::contract::terminal_adapter::{
            TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError,
        };

        let mux = WebRtcConnectionMux::new();
        let mut adapters = Vec::new();
        for index in 0..31 {
            let (adapter, handle) = mux.create_adapter();
            let label = mux
                .reserve_terminal(format!("s{index:02}"), "sub".into(), 1)
                .expect("admit 31 channels while the aggregate is 0");
            assert!(mux.bind_reserved(&label, handle.clone()));
            adapters.push((adapter, handle));
        }
        assert_eq!(mux.charged_subscription_count(), 31);
        assert_eq!(mux.aggregate_buffered(), 0);

        for (index, (_adapter, handle)) in adapters.iter().enumerate() {
            handle.set_buffered_bytes(if index < 29 { 65_536 } else { 98_304 });
        }
        assert_eq!(mux.aggregate_buffered(), AGGREGATE_BUFFERED_HIGH);
        assert_eq!(
            mux.reserve_terminal("s31".into(), "sub".into(), 1),
            Err("subscription_channel_aggregate")
        );
        assert_eq!(mux.aggregate_buffered(), AGGREGATE_BUFFERED_HIGH);
        assert_eq!(mux.charged_subscription_count(), 31);

        let payload = format!(
            r#"{{"type":"terminal_output","marker":"{}"}}"#,
            "x".repeat(65_500)
        );
        let frame = TerminalFrame::from_bytes(payload.as_bytes()).expect("crossing frame");
        assert!(frame.to_bytes().expect("frame bytes").len() >= 65_536);

        {
            let (cross, handle) = &mut adapters[30];
            assert_eq!(
                cross.try_write(&frame),
                Err(TerminalAdapterWriteError::WouldBlock)
            );
            assert_eq!(cross.pressure(), TerminalAdapterPressure::WouldBlock);
            assert!(
                handle.snapshot_active().is_none(),
                "a refused terminal send must not retain the frame in the adapter"
            );
            for _ in 0..8 {
                assert_eq!(
                    cross.try_write(&frame),
                    Err(TerminalAdapterWriteError::WouldBlock)
                );
            }
        }
        assert_eq!(mux.aggregate_buffered(), AGGREGATE_BUFFERED_HIGH);

        for (_adapter, handle) in &adapters {
            handle.set_buffered_bytes(0);
        }
        assert_eq!(mux.aggregate_buffered(), 0);
        {
            let (cross, _handle) = &mut adapters[30];
            assert_eq!(cross.pressure(), TerminalAdapterPressure::Ready);
            assert_eq!(cross.try_write(&frame), Ok(()));
            assert_eq!(cross.pressure(), TerminalAdapterPressure::Full);
        }
    }

    #[test]
    fn a27b_sustained_aggregate_pressure_does_not_retain_the_refused_frame() {
        use crate::local_webrtc::webrtc_subscription_channel::AGGREGATE_BUFFERED_HIGH;
        use botster_core::contract::terminal_adapter::{
            TerminalAdapter, TerminalAdapterPressure, TerminalAdapterWriteError,
        };

        let mux = WebRtcConnectionMux::new();
        let mut adapters = Vec::new();
        for index in 0..31 {
            let (adapter, handle) = mux.create_adapter();
            let label = mux
                .reserve_terminal(format!("s{index:02}"), "sub".into(), 1)
                .expect("admit while aggregate is 0");
            assert!(mux.bind_reserved(&label, handle.clone()));
            adapters.push((adapter, handle));
        }
        for (index, (_adapter, handle)) in adapters.iter().enumerate() {
            handle.set_buffered_bytes(if index < 29 { 65_536 } else { 98_304 });
        }
        let payload = format!(
            r#"{{"type":"terminal_output","marker":"{}"}}"#,
            "x".repeat(65_500)
        );
        let frame = TerminalFrame::from_bytes(payload.as_bytes()).expect("pressure frame");
        let (cross, handle) = &mut adapters[0];
        for _ in 0..512 {
            assert_eq!(
                cross.try_write(&frame),
                Err(TerminalAdapterWriteError::WouldBlock)
            );
        }
        assert_eq!(cross.pressure(), TerminalAdapterPressure::WouldBlock);
        assert!(handle.snapshot_active().is_none());
        assert_eq!(mux.aggregate_buffered(), AGGREGATE_BUFFERED_HIGH);
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
