use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, TryLockError};

use botster_core::contract::terminal_adapter::{
    TerminalAdapterPressure, TerminalAdapterWriteError, TerminalIngress,
};
use botster_core::contract::terminal_wake::{TerminalWakeKind, TerminalWakeSink};
use botster_terminal_protocol::RoutedTerminalFrame;

use super::close_reason::{CloseCause, CloseReport};
use super::ingress::{IngressBuffer, IngressStore};
use super::wake::WakeSink;

type CloseHook = Arc<dyn Fn(CloseReport) + Send + Sync>;

/// One in-flight write slot shared by production terminal adapters.
///
/// The slot holds the routed envelope by `Arc` clones. The shared
/// `TerminalBody` bytes are never copied into the slot; a transport reads
/// them through [`RoutedTerminalFrame::frame`].
pub(crate) struct AdapterSlot<W: WakeSink> {
    cause: CloseCause,
    would_block: AtomicBool,
    slot: Mutex<Option<RoutedTerminalFrame>>,
    /// Whether `slot` holds a frame. Stored only while the slot mutex is
    /// held, so Core's `pressure()` reads occupancy without the lock and a
    /// contended lock never reads as Full.
    occupied: AtomicBool,
    /// Core's `try_write` met the slot mutex held by a transport driver;
    /// that driver's unlock raises the Writable wake Core waits for.
    writer_waiting: AtomicBool,
    wake: W,
    close_work: Arc<AtomicBool>,
    close_hook: Mutex<Option<CloseHook>>,
    core_sink: Mutex<Option<TerminalWakeSink>>,
    closed_woke: AtomicBool,
    ingress: IngressBuffer,
}

impl<W: WakeSink> AdapterSlot<W> {
    pub(crate) fn with_wake_and_close_work(wake: W, close_work: Arc<AtomicBool>) -> Self {
        Self {
            cause: CloseCause::new(),
            would_block: AtomicBool::new(false),
            slot: Mutex::new(None),
            occupied: AtomicBool::new(false),
            writer_waiting: AtomicBool::new(false),
            wake,
            close_work,
            close_hook: Mutex::new(None),
            core_sink: Mutex::new(None),
            closed_woke: AtomicBool::new(false),
            ingress: IngressBuffer::new(),
        }
    }

    pub(crate) fn set_wake_sink(&self, sink: TerminalWakeSink) {
        if let Ok(mut slot) = self.core_sink.lock() {
            *slot = Some(sink);
        }
        self.emit_writable();
    }

    pub(crate) fn attach_close_hook(&self, hook: impl Fn(CloseReport) + Send + Sync + 'static) {
        if let Ok(mut slot) = self.close_hook.lock() {
            *slot = Some(Arc::new(hook));
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.cause.is_closed()
    }

    pub(crate) fn host_closed(&self) -> bool {
        self.cause.host_closed()
    }

    pub(crate) fn close_from_host(&self) {
        self.cause.close_from_host();
        self.close();
    }

    /// Core's `TerminalAdapter::close`: record its reason, then close.
    pub(crate) fn close_from_core(
        &self,
        reason: botster_core::contract::terminal_adapter::TerminalRouteCloseReason,
    ) {
        self.cause.close_from_core(reason);
        self.close();
    }

    pub(crate) fn core_close_reason(
        &self,
    ) -> Option<botster_core::contract::terminal_adapter::TerminalRouteCloseReason> {
        self.cause.core_reason()
    }

    /// Run `seam` inside the next close, between its open check and commit.
    #[cfg(test)]
    pub(crate) fn set_close_commit_seam(&self, seam: impl FnOnce() + Send + 'static) {
        self.cause.set_commit_seam(seam);
    }

    pub(crate) fn close(&self) {
        self.cause.close();
        self.close_work.store(true, Ordering::SeqCst);
        self.ingress.clear();
        match self.slot.try_lock() {
            Ok(mut slot) => {
                *slot = None;
                self.occupied.store(false, Ordering::SeqCst);
            }
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => {
                *poisoned.into_inner() = None;
                self.occupied.store(false, Ordering::SeqCst);
            }
        }
        self.emit_closed();
        if let Ok(hook) = self.close_hook.lock()
            && let Some(hook) = hook.as_ref()
        {
            hook(self.cause.report());
        }
        self.wake.wake();
    }

    fn emit_writable(&self) {
        if self.is_closed() {
            return;
        }
        if let Ok(sink) = self.core_sink.lock()
            && let Some(sink) = sink.as_ref()
        {
            let _ = sink.wake(TerminalWakeKind::Writable);
        }
    }

    /// Wake the transport writer only, for queued control output such as
    /// S13 credit frames. Core is not woken.
    pub(crate) fn wake_transport(&self) {
        self.wake.wake();
    }

    pub(crate) fn notify_writable(&self) {
        self.emit_writable();
        self.wake.wake();
    }

    fn emit_closed(&self) {
        if self.closed_woke.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Ok(sink) = self.core_sink.lock()
            && let Some(sink) = sink.as_ref()
        {
            let _ = sink.wake(TerminalWakeKind::Closed);
        }
    }

    /// Record transport backpressure. Clearing it wakes Core and the writer.
    pub(crate) fn set_would_block(&self, pressured: bool) {
        self.would_block.store(pressured, Ordering::SeqCst);
        if !pressured {
            self.emit_writable();
            self.wake.wake();
        }
    }

    /// Core's owner-loop probe. It reads occupancy without the slot mutex,
    /// so a driver holding the mutex over an empty slot never reads as Full.
    pub(crate) fn pressure(&self) -> TerminalAdapterPressure {
        if self.is_closed() {
            TerminalAdapterPressure::Closed
        } else if self.occupied.load(Ordering::SeqCst) {
            TerminalAdapterPressure::Full
        } else if self.would_block.load(Ordering::SeqCst) {
            TerminalAdapterPressure::WouldBlock
        } else {
            TerminalAdapterPressure::Ready
        }
    }

    pub(crate) fn try_write(
        &self,
        frame: &RoutedTerminalFrame,
    ) -> Result<(), TerminalAdapterWriteError> {
        if self.is_closed() {
            return Err(TerminalAdapterWriteError::Closed);
        }
        if self.would_block.load(Ordering::SeqCst) {
            return Err(TerminalAdapterWriteError::WouldBlock);
        }
        // Core never waits here. A driver holding the mutex is inside a
        // short clone or take; mark the wait before the one retry, so either
        // the retry succeeds or that driver's unlock sees the mark and wakes
        // Core (see `DriverSlotGuard`).
        let mut slot = match self.slot.try_lock() {
            Ok(slot) => slot,
            Err(TryLockError::WouldBlock) => {
                #[cfg(test)]
                run_contended_write_hook();
                self.writer_waiting.store(true, Ordering::SeqCst);
                match self.slot.try_lock() {
                    Ok(slot) => {
                        self.writer_waiting.store(false, Ordering::SeqCst);
                        slot
                    }
                    Err(TryLockError::WouldBlock) => return Err(TerminalAdapterWriteError::Full),
                    Err(TryLockError::Poisoned(_)) => {
                        self.close();
                        return Err(TerminalAdapterWriteError::Closed);
                    }
                }
            }
            Err(TryLockError::Poisoned(_)) => {
                self.close();
                return Err(TerminalAdapterWriteError::Closed);
            }
        };
        if self.is_closed() {
            *slot = None;
            self.occupied.store(false, Ordering::SeqCst);
            return Err(TerminalAdapterWriteError::Closed);
        }
        if slot.is_some() {
            return Err(TerminalAdapterWriteError::Full);
        }
        *slot = Some(frame.clone());
        self.occupied.store(true, Ordering::SeqCst);
        drop(slot);
        self.wake.wake();
        Ok(())
    }

    pub(crate) fn try_read(&self) -> TerminalIngress {
        self.ingress.try_read(self.is_closed())
    }

    /// Store one input frame without latching loss. `Full` hands the frame
    /// back: the transport keeps it, stops reading, and waits for
    /// [`Self::ingress_room`]. A malformed header closes the route.
    pub(crate) fn try_push_ingress(&self, bytes: Vec<u8>) -> IngressStore {
        if self.is_closed() {
            return IngressStore::Closed;
        }
        let stored = self.ingress.try_store(bytes, || self.is_closed());
        match &stored {
            IngressStore::Stored => self.emit_writable(),
            IngressStore::Malformed => self.close(),
            IngressStore::Full(_) | IngressStore::Closed => {}
        }
        stored
    }

    #[cfg(test)]
    pub(crate) fn set_ingress_full_observer(&self, observer: std::sync::mpsc::Sender<()>) {
        self.ingress.set_full_observer(observer);
    }

    /// Resolves after Core removes an input frame or the route closes.
    pub(crate) async fn ingress_room(&self) {
        self.ingress.room().await;
    }

    #[cfg(test)]
    pub(crate) fn mark_ingress_lost(&self) {
        if !self.is_closed() {
            self.ingress.mark_lost();
            self.emit_writable();
        }
    }

    #[allow(dead_code)]
    pub(crate) fn inject_ingress_frame(&self, bytes: Vec<u8>) {
        if self.is_closed() {
            return;
        }
        if self.ingress.store_complete(bytes, || self.is_closed()) {
            self.emit_writable();
        }
    }

    #[allow(dead_code)]
    pub(crate) fn inject_ingress_partial(&self, bytes: Vec<u8>) {
        if self.is_closed() {
            return;
        }
        self.ingress.store_partial(bytes);
    }

    #[allow(dead_code)]
    pub(crate) fn complete_ingress_partial(&self) {
        if self.is_closed() {
            return;
        }
        if self.ingress.complete_partial(|| self.is_closed()) {
            self.emit_writable();
        }
    }

    #[allow(dead_code)]
    pub(crate) fn drop_buffered_ingress_frame(&self) {
        if self.is_closed() {
            return;
        }
        if self.ingress.drop_one_complete() {
            self.emit_writable();
        }
    }

    /// The slot mutex for a transport driver. Core's owner-loop calls
    /// (`try_write`, `pressure`, `close`) only `try_lock` it and never wait;
    /// a driver waits, because a lost read or completion here loses or
    /// repeats a frame. Every holder keeps it for a clone, store, or take
    /// only, with no other lock and no await. A poisoned slot is closed.
    fn lock_for_driver(&self) -> Option<DriverSlotGuard<'_, W>> {
        match self.slot.lock() {
            Ok(guard) => Some(DriverSlotGuard {
                guard: Some(guard),
                slot: self,
            }),
            Err(poisoned) => {
                *poisoned.into_inner() = None;
                self.occupied.store(false, Ordering::SeqCst);
                self.close();
                None
            }
        }
    }

    /// Holds the slot mutex exactly as a transport driver does, unlock wake
    /// included.
    #[cfg(test)]
    pub(crate) fn hold_slot_for_test(&self) -> DriverSlotGuard<'_, W> {
        self.lock_for_driver().expect("slot lock")
    }

    /// The occupying routed frame, by `Arc` clones. `None` when empty or
    /// closed. Transport drivers only; see [`Self::lock_for_driver`].
    pub(crate) fn snapshot_active(&self) -> Option<RoutedTerminalFrame> {
        let mut slot = self.lock_for_driver()?;
        if self.is_closed() {
            slot.set(None);
            return None;
        }
        slot.get().clone()
    }

    /// Release the occupying frame after the transport finished its write.
    /// Transport drivers only; see [`Self::lock_for_driver`].
    pub(crate) fn complete_active(&self) -> Option<RoutedTerminalFrame> {
        if self.is_closed() {
            return None;
        }
        let taken = match self.lock_for_driver() {
            Some(mut slot) => {
                if self.is_closed() {
                    slot.set(None);
                    None
                } else {
                    slot.take()
                }
            }
            None => None,
        };
        if taken.is_some() {
            self.emit_writable();
        }
        self.wake.wake();
        taken
    }
}

#[cfg(test)]
thread_local! {
    /// Runs once when `try_write` first meets a held slot mutex, before it
    /// marks the wait: the unlock-before-mark interleaving.
    static CONTENDED_WRITE_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_contended_write_hook(hook: impl FnOnce() + 'static) {
    CONTENDED_WRITE_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
fn run_contended_write_hook() {
    if let Some(hook) = CONTENDED_WRITE_HOOK.with(|slot| slot.borrow_mut().take()) {
        hook();
    }
}

/// The slot mutex held by a transport driver. Every store keeps `occupied`
/// in step. Dropping it unlocks first, then raises the Writable wake a
/// contended Core `try_write` marked.
pub(crate) struct DriverSlotGuard<'a, W: WakeSink> {
    guard: Option<std::sync::MutexGuard<'a, Option<RoutedTerminalFrame>>>,
    slot: &'a AdapterSlot<W>,
}

impl<W: WakeSink> DriverSlotGuard<'_, W> {
    fn get(&self) -> &Option<RoutedTerminalFrame> {
        self.guard.as_ref().expect("held until drop")
    }

    fn set(&mut self, frame: Option<RoutedTerminalFrame>) {
        self.slot.occupied.store(frame.is_some(), Ordering::SeqCst);
        **self.guard.as_mut().expect("held until drop") = frame;
    }

    fn take(&mut self) -> Option<RoutedTerminalFrame> {
        let taken = self.guard.as_mut().expect("held until drop").take();
        self.slot.occupied.store(false, Ordering::SeqCst);
        taken
    }
}

impl<W: WakeSink> Drop for DriverSlotGuard<'_, W> {
    fn drop(&mut self) {
        drop(self.guard.take());
        if self.slot.writer_waiting.swap(false, Ordering::SeqCst) {
            self.slot.notify_writable();
        }
    }
}

impl AdapterSlot<super::wake::AdapterWake> {
    pub(crate) async fn wait_for_write(&self) {
        self.wake.wait().await;
    }
}
