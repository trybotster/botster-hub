//! Bounded opaque ingress buffer for duplex terminal adapters.
//!
//! Hub validates the [`TerminalInputFrame`] header only. It never decodes the
//! body. Overflow latches [`TerminalIngress::Lost`] once. Close drops buffered
//! frames and makes later reads permanently closed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::sync::{Mutex, TryLockError};

use botster_core::contract::terminal_adapter::{
    MIN_ADAPTER_INGRESS_BUFFER_FRAMES, TerminalIngress,
};
use botster_terminal_protocol::TerminalInputFrame;

/// Bounded complete-frame ingress plus one partial assembly slot.
pub(crate) struct IngressBuffer {
    frames_tx: SyncSender<Vec<u8>>,
    frames_rx: Mutex<Receiver<Vec<u8>>>,
    partial: Mutex<Option<Vec<u8>>>,
    lost: AtomicBool,
    lost_reported: AtomicBool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IngressAdmission {
    Stored,
    Lost,
}

impl IngressBuffer {
    pub(crate) fn new() -> Self {
        let (frames_tx, frames_rx) = sync_channel(MIN_ADAPTER_INGRESS_BUFFER_FRAMES);
        Self {
            frames_tx,
            frames_rx: Mutex::new(frames_rx),
            partial: Mutex::new(None),
            lost: AtomicBool::new(false),
            lost_reported: AtomicBool::new(false),
        }
    }

    pub(crate) fn clear(&self) {
        match self.frames_rx.try_lock() {
            Ok(frames) => drain_frames(&frames),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => drain_frames(&poisoned.into_inner()),
        }
        match self.partial.try_lock() {
            Ok(mut partial) => *partial = None,
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(poisoned)) => *poisoned.into_inner() = None,
        }
        self.lost.store(false, Ordering::SeqCst);
        self.lost_reported.store(false, Ordering::SeqCst);
    }

    /// Validate the header and buffer one complete frame.
    ///
    /// Returns `Err(())` when the header is malformed so the caller can close
    /// the route. Returns `Ok(true)` when a complete frame was stored or loss
    /// was latched, so Core should receive a writable wake.
    #[allow(dead_code)]
    pub(crate) fn push_complete(
        &self,
        bytes: Vec<u8>,
        is_closed: impl Fn() -> bool,
    ) -> Result<bool, ()> {
        self.push_complete_observed(bytes, is_closed, |_| {})
    }

    pub(crate) fn push_complete_observed(
        &self,
        bytes: Vec<u8>,
        is_closed: impl Fn() -> bool,
        observed: impl FnOnce(IngressAdmission),
    ) -> Result<bool, ()> {
        if TerminalInputFrame::from_bytes(&bytes).is_err() {
            return Err(());
        }
        Ok(self.store_complete_observed(bytes, is_closed, observed))
    }

    pub(crate) fn store_complete(&self, bytes: Vec<u8>, is_closed: impl Fn() -> bool) -> bool {
        self.store_complete_with_hooks(bytes, is_closed, || {}, |_| {})
    }

    fn store_complete_observed(
        &self,
        bytes: Vec<u8>,
        is_closed: impl Fn() -> bool,
        observed: impl FnOnce(IngressAdmission),
    ) -> bool {
        self.store_complete_with_hooks(bytes, is_closed, || {}, observed)
    }

    #[cfg(test)]
    fn store_complete_after_admission(
        &self,
        bytes: Vec<u8>,
        is_closed: impl Fn() -> bool,
        admitted: impl FnOnce(),
    ) -> bool {
        self.store_complete_with_hooks(bytes, is_closed, admitted, |_| {})
    }

    fn store_complete_with_hooks(
        &self,
        bytes: Vec<u8>,
        is_closed: impl Fn() -> bool,
        admitted: impl FnOnce(),
        observed: impl FnOnce(IngressAdmission),
    ) -> bool {
        if is_closed() {
            return false;
        }
        admitted();
        if is_closed() {
            return false;
        }
        let (stored, observation) = match self.frames_tx.try_send(bytes) {
            Ok(()) => (true, Some(IngressAdmission::Stored)),
            Err(TrySendError::Full(_)) => {
                self.lost.store(true, Ordering::SeqCst);
                (false, Some(IngressAdmission::Lost))
            }
            Err(TrySendError::Disconnected(_)) => (false, None),
        };
        if let Some(observation) = observation {
            observed(observation);
        }
        if is_closed() {
            self.clear();
            return false;
        }
        stored || self.lost.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub(crate) fn store_partial(&self, bytes: Vec<u8>) {
        if let Ok(mut partial) = self.partial.lock() {
            *partial = Some(bytes);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn complete_partial(&self, is_closed: impl Fn() -> bool) -> bool {
        let bytes = match self.partial.lock() {
            Ok(mut partial) => partial.take(),
            Err(_) => None,
        };
        match bytes {
            Some(bytes) => self.store_complete(bytes, is_closed),
            None => false,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn drop_one_complete(&self) -> bool {
        let mut buffered = Vec::new();
        let dropped = match self.frames_rx.lock() {
            Ok(frames) => {
                while let Ok(frame) = frames.try_recv() {
                    buffered.push(frame);
                }
                let dropped = buffered.pop().is_some();
                for frame in buffered.drain(..) {
                    let _ = self.frames_tx.try_send(frame);
                }
                dropped
            }
            Err(_) => false,
        };
        if dropped {
            self.lost.store(true, Ordering::SeqCst);
        }
        dropped
    }

    #[cfg(test)]
    pub(crate) fn mark_lost(&self) {
        self.lost.store(true, Ordering::SeqCst);
        self.lost_reported.store(false, Ordering::SeqCst);
    }

    pub(crate) fn try_read(&self, closed: bool) -> TerminalIngress {
        if closed {
            return TerminalIngress::Closed;
        }
        if self.lost.load(Ordering::SeqCst) && !self.lost_reported.swap(true, Ordering::SeqCst) {
            return TerminalIngress::Lost;
        }
        match self.frames_rx.lock() {
            Ok(frames) => match frames.try_recv() {
                Ok(frame) => TerminalIngress::Frame(frame),
                Err(TryRecvError::Empty) => TerminalIngress::Empty,
                Err(TryRecvError::Disconnected) => TerminalIngress::Closed,
            },
            Err(_) => TerminalIngress::Closed,
        }
    }
}

fn drain_frames(frames: &Receiver<Vec<u8>>) {
    while frames.try_recv().is_ok() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    fn input_frame(data: &[u8]) -> Vec<u8> {
        let mut bytes = vec![1, 1, 0, 0];
        let len = u16::try_from(data.len()).expect("fixture body fits");
        bytes[2..4].copy_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(data);
        bytes
    }

    #[test]
    fn malformed_header_is_rejected_without_buffering() {
        let buffer = IngressBuffer::new();
        assert_eq!(buffer.push_complete(vec![0, 1, 0, 1, 1], || false), Err(()));
        assert_eq!(buffer.try_read(false), TerminalIngress::Empty);
    }

    #[test]
    fn overflow_latches_lost_once() {
        let buffer = IngressBuffer::new();
        for index in 0..MIN_ADAPTER_INGRESS_BUFFER_FRAMES {
            assert_eq!(
                buffer.push_complete(input_frame(&[index as u8]), || false),
                Ok(true)
            );
        }
        assert_eq!(
            buffer.push_complete(input_frame(&[0xff]), || false),
            Ok(true)
        );
        assert_eq!(buffer.try_read(false), TerminalIngress::Lost);
        assert_eq!(
            buffer.try_read(false),
            TerminalIngress::Frame(input_frame(&[0]))
        );
    }

    #[test]
    fn producer_does_not_wait_for_the_consumer_lock() {
        let buffer = Arc::new(IngressBuffer::new());
        let held = buffer.frames_rx.lock().expect("hold ingress consumer lock");
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
        let producer_buffer = Arc::clone(&buffer);
        let producer = thread::spawn(move || {
            started_tx.send(()).expect("publish producer attempt");
            let stored = producer_buffer.store_complete(input_frame(b"contended"), || false);
            done_tx.send(stored).expect("publish producer result");
        });

        started_rx.recv().expect("producer starts");
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("producer must not wait for the consumer lock"),
            "the producer stores while the consumer lock is held"
        );
        drop(held);
        producer.join().expect("producer thread");
        assert_eq!(
            buffer.try_read(false),
            TerminalIngress::Frame(input_frame(b"contended"))
        );
        assert!(!buffer.lost.load(Ordering::SeqCst));
    }

    #[test]
    fn close_makes_later_reads_closed() {
        let buffer = IngressBuffer::new();
        assert_eq!(buffer.push_complete(input_frame(b"x"), || false), Ok(true));
        buffer.clear();
        assert_eq!(buffer.try_read(true), TerminalIngress::Closed);
        assert_eq!(buffer.try_read(true), TerminalIngress::Closed);
    }

    #[test]
    fn close_discards_ingress_in_both_queue_orders() {
        let closed = AtomicBool::new(true);
        let close_first = IngressBuffer::new();
        assert!(
            !close_first.store_complete(input_frame(b"after-close"), || {
                closed.load(Ordering::SeqCst)
            })
        );

        let buffer = Arc::new(IngressBuffer::new());
        let closed = Arc::new(AtomicBool::new(false));
        let admitted = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let producer_buffer = Arc::clone(&buffer);
        let producer_closed = Arc::clone(&closed);
        let producer_admitted = Arc::clone(&admitted);
        let producer_release = Arc::clone(&release);
        let producer = thread::spawn(move || {
            producer_buffer.store_complete_after_admission(
                input_frame(b"racing-close"),
                || producer_closed.load(Ordering::SeqCst),
                || {
                    producer_admitted.wait();
                    producer_release.wait();
                },
            );
        });

        admitted.wait();
        closed.store(true, Ordering::SeqCst);
        buffer.clear();
        release.wait();
        producer.join().expect("producer thread");

        assert_eq!(buffer.try_read(true), TerminalIngress::Closed);
        assert!(matches!(
            buffer.frames_rx.lock().expect("frames lock").try_recv(),
            Err(TryRecvError::Empty)
        ));
        assert!(!buffer.lost.load(Ordering::SeqCst));
    }
}
