//! Requested storage for the pinned Serde tagged-content decoder.
//!
//! These values exclude input, JSON scratch, other outputs, errors, and stack storage.

use std::alloc::Layout;
use std::cell::Cell;

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

// Use the exact type used by the pinned derive implementation, not a layout mirror.
use serde::__private228::de::Content;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ContentStorage {
    pub(super) retained: usize,
    pub(super) peak: usize,
}

impl ContentStorage {
    pub(super) fn copied_string(bytes: usize) -> Option<Self> {
        Layout::array::<u8>(bytes).ok()?;
        Some(Self {
            retained: bytes,
            peak: bytes,
        })
    }

    /// A map keeps its key alive while it constructs the value.
    pub(super) fn map_entry(key: Self, value: Self) -> Option<Self> {
        Some(Self {
            retained: key.retained.checked_add(value.retained)?,
            peak: key.peak.max(key.retained.checked_add(value.peak)?),
        })
    }

    /// A borrowed Relative path needs a copy while buffered content remains live.
    /// An owned Content string transfers its charge and needs no copy term.
    pub(super) fn with_borrowed_output(self, bytes: usize) -> Option<usize> {
        Layout::array::<u8>(bytes).ok()?;
        Some(self.peak.max(self.retained.checked_add(bytes)?))
    }
}

pub(super) struct ContentContainer {
    element: Layout,
    len: usize,
    capacity: usize,
    child_bytes: usize,
    peak: usize,
}

impl ContentContainer {
    pub(super) fn output_vector<T>() -> Self {
        Self::new(Layout::new::<T>())
    }

    pub(super) fn sequence() -> Self {
        Self::new(Layout::new::<Content<'static>>())
    }

    pub(super) fn map() -> Self {
        Self::new(Layout::new::<(Content<'static>, Content<'static>)>())
    }

    fn new(element: Layout) -> Self {
        Self {
            element,
            len: 0,
            capacity: 0,
            child_bytes: 0,
            peak: 0,
        }
    }

    fn buffer_bytes(&self, capacity: usize) -> Option<usize> {
        let bytes = capacity.checked_mul(self.element.size())?;
        Layout::from_size_align(bytes, self.element.align())
            .ok()
            .map(|layout| layout.size())
    }

    /// Count every item in order, including duplicate map entries.
    /// Refusal leaves this accumulator unchanged.
    pub(super) fn push(&mut self, item: ContentStorage) -> Option<()> {
        let old_bytes = self.buffer_bytes(self.capacity)?;
        let build = old_bytes
            .checked_add(self.child_bytes)?
            .checked_add(item.peak)?;
        let children = self.child_bytes.checked_add(item.retained)?;
        let len = self.len.checked_add(1)?;
        let capacity = if self.len == self.capacity {
            // Rust 1.97 RawVec::min_non_zero_cap and grow_amortized.
            let minimum = match self.element.size() {
                1 => 8,
                2..=1024 => 4,
                _ => 1,
            };
            self.capacity.checked_mul(2)?.max(len).max(minimum)
        } else {
            self.capacity
        };
        let new_bytes = self.buffer_bytes(capacity)?;
        let push = if capacity != self.capacity {
            old_bytes.checked_add(new_bytes)?.checked_add(children)?
        } else {
            new_bytes.checked_add(children)?
        };
        self.len = len;
        self.capacity = capacity;
        self.child_bytes = children;
        self.peak = self.peak.max(build).max(push);
        Some(())
    }

    fn buffer_charge(&self) -> Option<usize> {
        self.buffer_bytes(self.capacity)
    }

    pub(super) fn storage(&self) -> Option<ContentStorage> {
        Some(ContentStorage {
            retained: self
                .buffer_bytes(self.capacity)?
                .checked_add(self.child_bytes)?,
            peak: self.peak,
        })
    }
}

/// Count raw Content recursively without constructing its vectors or strings.
/// The caller must fund the deserializer scratch and possible errors separately.
pub(super) struct ContentSeed;

/// The model retains completed allocations when decoding stops inside a child.
/// This cell is authoritative if decoding fails. On success, its retained and peak
/// values equal the returned storage when the cell starts at zero.
/// The caller funds one result, never both. The combined walk uses this cell's peak
/// as its Content contribution, without adding another Content peak.
/// Scratch, output, and error storage remain separate accounting terms.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ContentProgress {
    pub(super) retained: usize,
    pub(super) peak: usize,
}

impl ContentProgress {
    fn retain(&mut self, bytes: usize) -> Option<()> {
        let retained = self.retained.checked_add(bytes)?;
        self.retained = retained;
        self.peak = self.peak.max(retained);
        Some(())
    }

    pub(super) fn release(&mut self, bytes: usize) -> Option<()> {
        self.retained = self.retained.checked_sub(bytes)?;
        Some(())
    }
}

pub(super) struct ObservedContentSeed<'a> {
    progress: Option<&'a Cell<ContentProgress>>,
}

impl ContentSeed {
    pub(super) fn with_progress(progress: &Cell<ContentProgress>) -> ObservedContentSeed<'_> {
        ObservedContentSeed {
            progress: Some(progress),
        }
    }
}

impl<'de> DeserializeSeed<'de> for ObservedContentSeed<'_> {
    type Value = ContentStorage;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(ContentVisitor {
            progress: self.progress,
        })
    }
}

impl<'de> DeserializeSeed<'de> for ContentSeed {
    type Value = ContentStorage;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(ContentVisitor::untracked())
    }
}

pub(super) struct ContentVisitor<'a> {
    progress: Option<&'a Cell<ContentProgress>>,
}

impl ContentVisitor<'_> {
    pub(super) fn untracked() -> Self {
        Self { progress: None }
    }

    fn retain<E: de::Error>(&self, bytes: usize) -> Result<(), E> {
        if let Some(progress) = self.progress {
            let mut next = progress.get();
            next.retain(bytes)
                .ok_or_else(|| E::custom("session type Content storage overflow"))?;
            progress.set(next);
        }
        Ok(())
    }

    fn push<E: de::Error>(
        &self,
        container: &mut ContentContainer,
        item: ContentStorage,
    ) -> Result<(), E> {
        let old = container
            .buffer_charge()
            .ok_or_else(|| E::custom("session type Content storage overflow"))?;
        container
            .push(item)
            .ok_or_else(|| E::custom("session type Content storage overflow"))?;
        let new = container
            .buffer_charge()
            .ok_or_else(|| E::custom("session type Content storage overflow"))?;
        if new != old {
            self.retain::<E>(new)?;
            if let Some(progress) = self.progress {
                let mut next = progress.get();
                next.release(old)
                    .ok_or_else(|| E::custom("session type Content storage underflow"))?;
                progress.set(next);
            }
        }
        Ok(())
    }
}

impl<'de> Visitor<'de> for ContentVisitor<'_> {
    type Value = ContentStorage;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(ContentStorage::default())
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
        self.visit_unit()
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
        self.visit_unit()
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
        self.visit_unit()
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
        self.visit_unit()
    }

    fn visit_borrowed_str<E: de::Error>(self, _: &'de str) -> Result<Self::Value, E> {
        Ok(ContentStorage::default())
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let storage = ContentStorage::copied_string(value.len())
            .ok_or_else(|| E::custom("session type Content storage overflow"))?;
        self.retain::<E>(storage.retained)?;
        Ok(storage)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut storage = ContentContainer::sequence();
        while let Some(item) = sequence.next_element_seed(ObservedContentSeed {
            progress: self.progress,
        })? {
            self.push::<A::Error>(&mut storage, item)?;
        }
        storage
            .storage()
            .ok_or_else(|| <A::Error as de::Error>::custom("session type Content storage overflow"))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut storage = ContentContainer::map();
        while let Some(key) = map.next_key_seed(ObservedContentSeed {
            progress: self.progress,
        })? {
            let value = map.next_value_seed(ObservedContentSeed {
                progress: self.progress,
            })?;
            let entry = ContentStorage::map_entry(key, value).ok_or_else(|| {
                <A::Error as de::Error>::custom("session type Content storage overflow")
            })?;
            self.push::<A::Error>(&mut storage, entry)?;
        }
        storage
            .storage()
            .ok_or_else(|| <A::Error as de::Error>::custom("session type Content storage overflow"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_child_keeps_parent_and_completed_child_storage() {
        let input = br#"["\n", {"\u006b":"\u0080","broken":"#;
        let progress = Cell::new(ContentProgress::default());
        let mut decoder = serde_json::Deserializer::from_slice(input);
        assert!(
            ContentSeed::with_progress(&progress)
                .deserialize(&mut decoder)
                .is_err()
        );
        let expected = 4 * Layout::new::<Content<'static>>().size()
            + 4 * Layout::new::<(Content<'static>, Content<'static>)>().size()
            + 1
            + 1
            + 2;
        assert_eq!(progress.get().retained, expected);
        assert!(progress.get().peak >= expected);
    }

    #[test]
    fn observed_success_matches_the_container_recurrence() {
        let input = br#"["\n","\n","\n","\n","\n"]"#;
        let progress = Cell::new(ContentProgress::default());
        let mut decoder = serde_json::Deserializer::from_slice(input);
        let storage = ContentSeed::with_progress(&progress)
            .deserialize(&mut decoder)
            .unwrap();
        decoder.end().unwrap();
        assert_eq!(progress.get().retained, storage.retained);
        assert_eq!(progress.get().peak, storage.peak);
        let mut released = progress.get();
        released.release(storage.retained).unwrap();
        assert_eq!(released.retained, 0);
    }

    #[test]
    fn seed_counts_nested_duplicate_entries_and_escaped_strings() {
        let input = br#"{"same":["\n",{"nested":"\u0080"}],"same":["\n",{"nested":"\u0080"}]}"#;
        let mut decoder = serde_json::Deserializer::from_slice(input);
        let counted = ContentSeed.deserialize(&mut decoder).unwrap();
        decoder.end().unwrap();
        let mut nested = ContentContainer::map();
        nested
            .push(ContentStorage::copied_string(2).unwrap())
            .unwrap();
        let mut sequence = ContentContainer::sequence();
        sequence
            .push(ContentStorage::copied_string(1).unwrap())
            .unwrap();
        sequence.push(nested.storage().unwrap()).unwrap();
        let mut outer = ContentContainer::map();
        outer.push(sequence.storage().unwrap()).unwrap();
        outer.push(sequence.storage().unwrap()).unwrap();
        assert_eq!(counted, outer.storage().unwrap());
    }

    #[test]
    fn growth_retains_old_buffer_and_all_child_storage() {
        let mut sequence = ContentContainer::sequence();
        let item = ContentStorage::copied_string(7).unwrap();
        for _ in 0..5 {
            sequence.push(item).unwrap();
        }
        let cell = Layout::new::<Content<'static>>().size();
        assert_eq!(sequence.storage().unwrap().retained, 8 * cell + 35);
        assert_eq!(sequence.storage().unwrap().peak, 12 * cell + 35);
    }

    #[test]
    fn duplicate_map_entries_keep_both_values() {
        let entry = ContentStorage::map_entry(
            ContentStorage::copied_string(3).unwrap(),
            ContentStorage::copied_string(11).unwrap(),
        )
        .unwrap();
        let mut map = ContentContainer::map();
        map.push(entry).unwrap();
        map.push(entry).unwrap();
        let cells = 4 * Layout::new::<(Content<'static>, Content<'static>)>().size();
        assert_eq!(map.storage().unwrap().retained, cells + 28);
        assert_eq!(
            map.storage().unwrap().with_borrowed_output(19),
            Some(cells + 47)
        );
    }

    #[test]
    fn overflow_preserves_accumulator() {
        let mut sequence = ContentContainer::sequence();
        sequence
            .push(ContentStorage::copied_string(5).unwrap())
            .unwrap();
        let before = sequence.storage();
        assert!(
            sequence
                .push(ContentStorage {
                    retained: usize::MAX,
                    peak: usize::MAX
                })
                .is_none()
        );
        assert_eq!(sequence.storage(), before);
    }
}
