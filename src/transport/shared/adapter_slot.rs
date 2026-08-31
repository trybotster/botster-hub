use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, TryLockError};

use botster_core::contract::terminal_adapter::{
    MIN_ADAPTER_INGRESS_BUFFER_FRAMES, TerminalAdapterPressure, TerminalAdapterWriteError,
    TerminalIngress,
};
use botster_terminal_protocol::TerminalFrame;

use super::close_reason::CloseCause;
use super::wake::WakeSink;

/// One in-flight write slot shared by production terminal adapters.
pub(crate) struct AdapterSlot<W: WakeSink> {
    cause: CloseCause,
    would_block: AtomicBool,
    slot: Mutex<Option<Vec<u8>>>,
    ingress: Mutex<VecDeque<Vec<u8>>>,
    ingress_lost: AtomicBool,
    wake: W,
    close_work: Arc<AtomicBool>,
}

impl<W: WakeSink> AdapterSlot<W> {
    pub(crate) fn with_wake_and_close_work(wake: W, close_work: Arc<AtomicBool>) -> Self {
        Self {
            cause: CloseCause::new(),
            would_block: AtomicBool::new(false),
            slot: Mutex::new(None),
            ingress: Mutex::new(VecDeque::new()),
            ingress_lost: AtomicBool::new(false),
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
        self.ingress_lost.store(false, Ordering::SeqCst);
        match self.ingress.try_lock() {
            Ok(mut ingress) => ingress.clear(),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner().clear(),
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

    pub(crate) fn try_read(&self) -> TerminalIngress {
        if self.is_closed() {
            return TerminalIngress::Closed;
        }
        if self.ingress_lost.swap(false, Ordering::SeqCst) {
            return if self.is_closed() {
                TerminalIngress::Closed
            } else {
                TerminalIngress::Lost
            };
        }
        match self.ingress.try_lock() {
            Ok(mut ingress) => {
                if self.is_closed() {
                    ingress.clear();
                    TerminalIngress::Closed
                } else {
                    ingress
                        .pop_front()
                        .map_or(TerminalIngress::Empty, TerminalIngress::Frame)
                }
            }
            Err(TryLockError::WouldBlock) => TerminalIngress::Empty,
            Err(TryLockError::Poisoned(_)) => {
                self.close();
                TerminalIngress::Closed
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn push_ingress_frame(&self, bytes: Vec<u8>) {
        self.push_ingress_frame_after_admission(bytes, || {});
    }

    fn push_ingress_frame_after_admission(&self, bytes: Vec<u8>, admitted: impl FnOnce()) {
        if self.is_closed() {
            return;
        }
        let mut ingress = match self.ingress.try_lock() {
            Ok(ingress) => ingress,
            Err(TryLockError::WouldBlock) => {
                self.mark_ingress_lost_if_open();
                return;
            }
            Err(TryLockError::Poisoned(_)) => {
                self.close();
                return;
            }
        };
        if self.is_closed() {
            ingress.clear();
            return;
        }
        admitted();
        if ingress.len() >= MIN_ADAPTER_INGRESS_BUFFER_FRAMES {
            self.mark_ingress_lost_if_open();
            if self.is_closed() {
                ingress.clear();
            }
            return;
        }
        ingress.push_back(bytes);
        if self.is_closed() {
            ingress.clear();
            self.ingress_lost.store(false, Ordering::SeqCst);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn mark_ingress_lost(&self) {
        self.mark_ingress_lost_if_open();
    }

    fn mark_ingress_lost_if_open(&self) {
        if !self.is_closed() {
            self.ingress_lost.store(true, Ordering::SeqCst);
            if self.is_closed() {
                self.ingress_lost.store(false, Ordering::SeqCst);
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn drop_newest_ingress_frame(&self) {
        if self.is_closed() {
            return;
        }
        match self.ingress.try_lock() {
            Ok(mut ingress) => {
                if !self.is_closed() && ingress.pop_back().is_some() {
                    self.mark_ingress_lost_if_open();
                    if self.is_closed() {
                        ingress.clear();
                    }
                }
            }
            Err(TryLockError::WouldBlock) => self.mark_ingress_lost_if_open(),
            Err(TryLockError::Poisoned(_)) => self.close(),
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
        self.wake.wake();
        taken
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct TestWake;

    impl WakeSink for TestWake {
        fn wake(&self) {}
    }

    fn new_slot() -> AdapterSlot<TestWake> {
        AdapterSlot::with_wake_and_close_work(TestWake, Arc::new(AtomicBool::new(false)))
    }

    #[test]
    fn ingress_precedence_is_closed_lost_frame_empty() {
        let slot = new_slot();
        assert_eq!(slot.try_read(), TerminalIngress::Empty);
        slot.push_ingress_frame(b"keep".to_vec());
        slot.push_ingress_frame(b"drop".to_vec());
        slot.drop_newest_ingress_frame();
        assert_eq!(slot.try_read(), TerminalIngress::Lost);
        assert_eq!(slot.try_read(), TerminalIngress::Frame(b"keep".to_vec()));
        assert_eq!(slot.try_read(), TerminalIngress::Empty);

        slot.push_ingress_frame(b"discard".to_vec());
        slot.mark_ingress_lost();
        slot.close();
        assert_eq!(slot.try_read(), TerminalIngress::Closed);
        assert_eq!(slot.try_read(), TerminalIngress::Closed);
    }

    #[test]
    fn ingress_capacity_floor_and_idle_reads_are_lossless() {
        let slot = new_slot();
        for index in 0..MIN_ADAPTER_INGRESS_BUFFER_FRAMES {
            slot.push_ingress_frame(vec![index as u8]);
        }
        assert_eq!(slot.try_read(), TerminalIngress::Frame(vec![0]));

        let overflow = new_slot();
        for index in 0..=MIN_ADAPTER_INGRESS_BUFFER_FRAMES {
            overflow.push_ingress_frame(vec![index as u8]);
        }
        assert_eq!(overflow.try_read(), TerminalIngress::Lost);
        assert_eq!(overflow.try_read(), TerminalIngress::Frame(vec![0]));

        let idle = new_slot();
        for _ in 0..256 {
            assert_eq!(idle.try_read(), TerminalIngress::Empty);
        }
    }

    #[test]
    fn close_discards_ingress_for_both_close_and_push_queue_orders() {
        use std::sync::Barrier;
        use std::thread;

        let close_first = new_slot();
        close_first.close();
        close_first.push_ingress_frame(b"after-close".to_vec());
        assert!(close_first.ingress.lock().expect("ingress lock").is_empty());

        let push_first = Arc::new(new_slot());
        let admitted = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let producer_slot = Arc::clone(&push_first);
        let producer_admitted = Arc::clone(&admitted);
        let producer_release = Arc::clone(&release);
        let producer = thread::spawn(move || {
            producer_slot.push_ingress_frame_after_admission(b"racing-close".to_vec(), || {
                producer_admitted.wait();
                producer_release.wait();
            });
        });

        admitted.wait();
        push_first.close();
        release.wait();
        producer.join().expect("producer thread");

        assert_eq!(push_first.try_read(), TerminalIngress::Closed);
        assert!(push_first.ingress.lock().expect("ingress lock").is_empty());
        assert!(!push_first.ingress_lost.load(Ordering::SeqCst));
    }
}
