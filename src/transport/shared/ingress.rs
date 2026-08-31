//! Bounded opaque ingress buffer for duplex terminal adapters.
//!
//! Hub validates the [`TerminalInputFrame`] header only. It never decodes the
//! body. Overflow latches [`TerminalIngress::Lost`] once. Close drops buffered
//! frames and makes later reads permanently closed.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use botster_core::contract::terminal_adapter::{
    MIN_ADAPTER_INGRESS_BUFFER_FRAMES, TerminalIngress,
};
use botster_terminal_protocol::TerminalInputFrame;

/// Bounded complete-frame ingress plus one partial assembly slot.
pub(crate) struct IngressBuffer {
    frames: Mutex<VecDeque<Vec<u8>>>,
    partial: Mutex<Option<Vec<u8>>>,
    lost: AtomicBool,
    lost_reported: AtomicBool,
}

impl IngressBuffer {
    pub(crate) fn new() -> Self {
        Self {
            frames: Mutex::new(VecDeque::new()),
            partial: Mutex::new(None),
            lost: AtomicBool::new(false),
            lost_reported: AtomicBool::new(false),
        }
    }

    pub(crate) fn clear(&self) {
        if let Ok(mut frames) = self.frames.lock() {
            frames.clear();
        }
        if let Ok(mut partial) = self.partial.lock() {
            *partial = None;
        }
        self.lost.store(false, Ordering::SeqCst);
        self.lost_reported.store(false, Ordering::SeqCst);
    }

    /// Validate the header and buffer one complete frame.
    ///
    /// Returns `Err(())` when the header is malformed so the caller can close
    /// the route. Returns `Ok(true)` when a complete frame was stored or loss
    /// was latched, so Core should receive a writable wake.
    pub(crate) fn push_complete(&self, bytes: Vec<u8>) -> Result<bool, ()> {
        if TerminalInputFrame::from_bytes(&bytes).is_err() {
            return Err(());
        }
        Ok(self.store_complete(bytes))
    }

    pub(crate) fn store_complete(&self, bytes: Vec<u8>) -> bool {
        let Ok(mut frames) = self.frames.lock() else {
            self.lost.store(true, Ordering::SeqCst);
            return true;
        };
        if frames.len() >= MIN_ADAPTER_INGRESS_BUFFER_FRAMES {
            self.lost.store(true, Ordering::SeqCst);
            return true;
        }
        frames.push_back(bytes);
        true
    }

    #[allow(dead_code)]
    pub(crate) fn store_partial(&self, bytes: Vec<u8>) {
        if let Ok(mut partial) = self.partial.lock() {
            *partial = Some(bytes);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn complete_partial(&self) -> bool {
        let bytes = match self.partial.lock() {
            Ok(mut partial) => partial.take(),
            Err(_) => None,
        };
        match bytes {
            Some(bytes) => self.store_complete(bytes),
            None => false,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn drop_one_complete(&self) -> bool {
        let dropped = match self.frames.lock() {
            Ok(mut frames) => frames.pop_back().is_some(),
            Err(_) => false,
        };
        if dropped {
            self.lost.store(true, Ordering::SeqCst);
        }
        dropped
    }

    pub(crate) fn try_read(&self, closed: bool) -> TerminalIngress {
        if closed {
            return TerminalIngress::Closed;
        }
        if self.lost.load(Ordering::SeqCst) && !self.lost_reported.swap(true, Ordering::SeqCst) {
            return TerminalIngress::Lost;
        }
        match self.frames.lock() {
            Ok(mut frames) => match frames.pop_front() {
                Some(frame) => TerminalIngress::Frame(frame),
                None => TerminalIngress::Empty,
            },
            Err(_) => TerminalIngress::Closed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(buffer.push_complete(vec![0, 1, 0, 1, 1]), Err(()));
        assert_eq!(buffer.try_read(false), TerminalIngress::Empty);
    }

    #[test]
    fn overflow_latches_lost_once() {
        let buffer = IngressBuffer::new();
        for index in 0..MIN_ADAPTER_INGRESS_BUFFER_FRAMES {
            assert_eq!(buffer.push_complete(input_frame(&[index as u8])), Ok(true));
        }
        assert_eq!(buffer.push_complete(input_frame(&[0xff])), Ok(true));
        assert_eq!(buffer.try_read(false), TerminalIngress::Lost);
        assert_eq!(
            buffer.try_read(false),
            TerminalIngress::Frame(input_frame(&[0]))
        );
    }

    #[test]
    fn close_makes_later_reads_closed() {
        let buffer = IngressBuffer::new();
        assert_eq!(buffer.push_complete(input_frame(b"x")), Ok(true));
        buffer.clear();
        assert_eq!(buffer.try_read(true), TerminalIngress::Closed);
        assert_eq!(buffer.try_read(true), TerminalIngress::Closed);
    }
}
