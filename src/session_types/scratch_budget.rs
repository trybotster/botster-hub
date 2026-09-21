//! Storage events for the pinned serde_json scratch Vec<u8>.
//!
//! This model does not parse input or predict decoder errors. The caller must
//! supply events in decoder order. Capacity survives clear and pop operations.
//! The combined walk uses each event's live bytes, not a sum of separate peaks.
//!
//! Use one ScratchStorage per Deserializer. The counting pass and typed decode
//! use separate deserializers. Their scratch vectors do not coexist. Take the
//! maximum across these passes; do not carry capacity from one pass to the next.
//!
//! The caller maps decoder operations to events as follows:
//! - Clear at the start of each parsed string.
//! - Append the raw prefix before the decoder checks an escape.
//! - Append one byte for a simple escape or an ASCII unicode escape.
//! - Use append_unicode for a non-ASCII unicode escape.
//! - Append the final raw suffix if the string used scratch storage.
//! - Do not append bytes for an unescaped borrowed string.
//! - Clear at the start of an ignored value.
//! - Append one frame byte for each container nested inside that ignored value.
//! - Pop frames as the decoder closes those containers.
//! Unknown-field keys still use parse_str. The ignore_str operation copies nothing.
//! The recursion limit does not bound ignored nesting. The caller must derive
//! actual depth from input and fund growth before allocation, without a new limit.
//! This event model does not prove that a scanner emits every event, including
//! events before a malformed escape, a truncated string, or an ignored container error.

use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ScratchStorage {
    pub(super) len: usize,
    pub(super) capacity: usize,
    pub(super) peak: usize,
}

/// Requested bytes that coexist during one scratch operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ScratchEvent {
    pub(super) live: usize,
    pub(super) retained: usize,
}

impl ScratchStorage {
    /// Model Vec::reserve with Rust 1.97's u8 minimum capacity and doubling.
    /// Refusal leaves the model unchanged.
    pub(super) fn reserve(&mut self, additional: usize) -> Option<ScratchEvent> {
        let required = self.len.checked_add(additional)?;
        let capacity = if required > self.capacity {
            self.capacity.checked_mul(2)?.max(required).max(8)
        } else {
            self.capacity
        };
        Layout::array::<u8>(capacity).ok()?;
        let live = if capacity != self.capacity {
            self.capacity.checked_add(capacity)?
        } else {
            capacity
        };
        self.capacity = capacity;
        self.peak = self.peak.max(live);
        Some(ScratchEvent {
            live,
            retained: capacity,
        })
    }

    /// Model push or extend_from_slice after its exact additional length is known.
    pub(super) fn append(&mut self, bytes: usize) -> Option<ScratchEvent> {
        let len = self.len.checked_add(bytes)?;
        let event = self.reserve(bytes)?;
        self.len = len;
        Some(event)
    }

    /// Non-ASCII unicode escapes reserve four bytes before setting the length.
    /// The caller supplies the decoded UTF-8 length, from two through four bytes.
    pub(super) fn append_unicode(&mut self, bytes: usize) -> Option<ScratchEvent> {
        if !(2..=4).contains(&bytes) {
            return None;
        }
        let len = self.len.checked_add(bytes)?;
        let event = self.reserve(4)?;
        self.len = len;
        Some(event)
    }

    /// The decoder reuses its allocation across strings and ignored values.
    pub(super) fn clear(&mut self) {
        self.len = 0;
    }

    /// Ignored nested values remove frames without releasing their allocation.
    pub(super) fn pop(&mut self) -> bool {
        if let Some(len) = self.len.checked_sub(1) {
            self.len = len;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_reservation_grows_retained_capacity_from_sixteen_to_thirty_two() {
        let mut storage = ScratchStorage::default();
        storage.append(1).unwrap();
        storage.append(8).unwrap();
        assert_eq!(storage.capacity, 16);
        storage.clear();
        storage.append(13).unwrap();
        assert_eq!(
            storage.append_unicode(2),
            Some(ScratchEvent {
                live: 48,
                retained: 32
            })
        );
        assert_eq!(storage.len, 15);
        assert_eq!(storage.peak, 48);
    }

    #[test]
    fn unicode_reservation_precedes_the_decoded_length() {
        let mut storage = ScratchStorage::default();
        assert_eq!(
            storage.append(6),
            Some(ScratchEvent {
                live: 8,
                retained: 8
            })
        );
        assert_eq!(
            storage.append_unicode(2),
            Some(ScratchEvent {
                live: 24,
                retained: 16
            })
        );
        assert_eq!(storage.len, 8);
        assert_eq!(storage.peak, 24);
    }

    #[test]
    fn clear_and_pop_keep_the_scratch_allocation() {
        let mut storage = ScratchStorage::default();
        storage.append(9).unwrap();
        storage.clear();
        assert_eq!(storage.capacity, 9);
        assert_eq!(
            storage.append(1),
            Some(ScratchEvent {
                live: 9,
                retained: 9
            })
        );
        assert!(storage.pop());
        assert!(!storage.pop());
        assert_eq!(storage.capacity, 9);
    }

    #[test]
    fn slice_extension_can_exceed_doubled_capacity() {
        let mut storage = ScratchStorage::default();
        storage.append(1).unwrap();
        assert_eq!(
            storage.append(32),
            Some(ScratchEvent {
                live: 41,
                retained: 33
            })
        );
        assert_eq!(storage.len, 33);
    }

    #[test]
    fn invalid_size_and_overflow_preserve_the_model() {
        let mut storage = ScratchStorage::default();
        storage.append(1).unwrap();
        let before = storage;
        assert_eq!(storage.append_unicode(1), None);
        assert_eq!(storage.append(usize::MAX), None);
        assert_eq!(storage.reserve(isize::MAX as usize), None);
        assert_eq!(storage, before);
    }
}
