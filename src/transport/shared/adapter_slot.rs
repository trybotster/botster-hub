use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, TryLockError};

use botster_core::contract::terminal_adapter::{
    TerminalAdapterPressure, TerminalAdapterWriteError,
};
use botster_terminal_protocol::TerminalFrame;

use super::close_reason::CloseCause;
use super::wake::WakeSink;

/// One in-flight write slot shared by production terminal adapters.
pub(crate) struct AdapterSlot<W: WakeSink> {
    cause: CloseCause,
    would_block: AtomicBool,
    slot: Mutex<Option<Vec<u8>>>,
    wake: W,
    close_work: Arc<AtomicBool>,
}

impl<W: WakeSink> AdapterSlot<W> {
    pub(crate) fn with_wake_and_close_work(wake: W, close_work: Arc<AtomicBool>) -> Self {
        Self {
            cause: CloseCause::new(),
            would_block: AtomicBool::new(false),
            slot: Mutex::new(None),
            wake,
            close_work,
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

    #[cfg(test)]
    pub(crate) fn wake(&self) {
        self.wake.wake();
    }

    #[cfg(test)]
    pub(crate) fn set_would_block(&self, pressured: bool) {
        self.would_block.store(pressured, Ordering::SeqCst);
    }

    pub(crate) fn pressure(&self) -> TerminalAdapterPressure {
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

    pub(crate) fn try_write(&self, frame: &TerminalFrame) -> Result<(), TerminalAdapterWriteError> {
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
        self.wake.wake();
        taken
    }
}
