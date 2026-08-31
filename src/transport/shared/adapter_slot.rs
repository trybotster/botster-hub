use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, TryLockError};

use botster_core::contract::terminal_adapter::{
    TerminalAdapterPressure, TerminalAdapterWriteError, TerminalIngress,
};
use botster_core::contract::terminal_wake::{TerminalWakeKind, TerminalWakeSink};
use botster_terminal_protocol::TerminalFrame;

use super::close_reason::CloseCause;
use super::ingress::IngressBuffer;
use super::wake::WakeSink;

type CloseHook = Arc<dyn Fn(bool) + Send + Sync>;

/// One in-flight write slot shared by production terminal adapters.
pub(crate) struct AdapterSlot<W: WakeSink> {
    cause: CloseCause,
    would_block: AtomicBool,
    slot: Mutex<Option<Vec<u8>>>,
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

    pub(crate) fn attach_close_hook(&self, hook: impl Fn(bool) + Send + Sync + 'static) {
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
        self.cause.mark_host_if_open();
        self.close();
    }

    pub(crate) fn close(&self) {
        self.cause.close();
        self.close_work.store(true, Ordering::SeqCst);
        self.ingress.clear();
        match self.slot.try_lock() {
            Ok(mut slot) => {
                *slot = None;
            }
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => {
                *poisoned.into_inner() = None;
            }
        }
        self.emit_closed();
        if let Ok(hook) = self.close_hook.lock()
            && let Some(hook) = hook.as_ref()
        {
            hook(self.host_closed());
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

    #[allow(dead_code)]
    pub(crate) fn set_would_block(&self, pressured: bool) {
        self.would_block.store(pressured, Ordering::SeqCst);
        if !pressured {
            self.emit_writable();
            self.wake.wake();
        }
    }

    pub(crate) fn pressure(&self) -> TerminalAdapterPressure {
        if self.is_closed() {
            return TerminalAdapterPressure::Closed;
        }
        if forced_would_block() {
            return TerminalAdapterPressure::WouldBlock;
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

    pub(crate) fn try_write(&self, frame: &TerminalFrame) -> Result<(), TerminalAdapterWriteError> {
        if self.is_closed() {
            return Err(TerminalAdapterWriteError::Closed);
        }
        if self.would_block.load(Ordering::SeqCst) || forced_would_block() {
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

    pub(crate) fn try_read(&self) -> TerminalIngress {
        self.ingress.try_read(self.is_closed())
    }

    pub(crate) fn push_ingress(&self, bytes: Vec<u8>) -> Result<(), ()> {
        if self.is_closed() {
            return Ok(());
        }
        match self.ingress.push_complete(bytes, || self.is_closed()) {
            Ok(true) => {
                self.emit_writable();
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(()) => {
                self.close();
                Err(())
            }
        }
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

    pub(crate) fn snapshot_active(&self) -> Option<Vec<u8>> {
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

    pub(crate) fn complete_active(&self) -> Option<Vec<u8>> {
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
        if taken.is_some() {
            self.emit_writable();
        }
        self.wake.wake();
        taken
    }
}

impl AdapterSlot<super::wake::AdapterWake> {
    pub(crate) async fn wait_for_write(&self) {
        self.wake.wait().await;
    }
}

fn forced_would_block() -> bool {
    std::env::var("BOTSTER_ENV").as_deref() == Ok("test")
        && std::env::var("BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK").as_deref() == Ok("1")
}
