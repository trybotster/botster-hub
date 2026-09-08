//! Ready queues and deadline indexing for Hub owner work.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use crate::owner_identity::WaiterId;

/// One owner work class in the fixed round-robin order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ReadyClass {
    ControlIngress,
    Cleanup,
    CoreCompletion,
    HostCompletion,
    PluginCompletion,
    Deadline,
    Observe,
    InventoryReconcile,
    JournalPull,
    ProjectionApply,
    Baseline,
    HostBridge,
    SubscriberDelivery,
    ProviderResync,
    PackageEventDelivery,
}

impl ReadyClass {
    pub(crate) const ALL: [Self; 15] = [
        Self::ControlIngress,
        Self::Cleanup,
        Self::CoreCompletion,
        Self::HostCompletion,
        Self::PluginCompletion,
        Self::Deadline,
        Self::Observe,
        Self::InventoryReconcile,
        Self::JournalPull,
        Self::ProjectionApply,
        Self::Baseline,
        Self::HostBridge,
        Self::SubscriberDelivery,
        Self::ProviderResync,
        Self::PackageEventDelivery,
    ];

    const fn index(self) -> usize {
        match self {
            Self::ControlIngress => 0,
            Self::Cleanup => 1,
            Self::CoreCompletion => 2,
            Self::HostCompletion => 3,
            Self::PluginCompletion => 4,
            Self::Deadline => 5,
            Self::Observe => 6,
            Self::InventoryReconcile => 7,
            Self::JournalPull => 8,
            Self::ProjectionApply => 9,
            Self::Baseline => 10,
            Self::HostBridge => 11,
            Self::SubscriberDelivery => 12,
            Self::ProviderResync => 13,
            Self::PackageEventDelivery => 14,
        }
    }
}

/// Bit reasons that made one waiter ready.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadyReasons(u64);

impl ReadyReasons {
    pub(crate) const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    pub(crate) const fn bits(self) -> u64 {
        self.0
    }

    pub(crate) const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub(crate) const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

/// The exact row key for one queued waiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadyKey {
    class: ReadyClass,
    enqueue_serial: u64,
    waiter_id: WaiterId,
}

impl ReadyKey {
    pub(crate) const fn class(self) -> ReadyClass {
        self.class
    }

    pub(crate) const fn enqueue_serial(self) -> u64 {
        self.enqueue_serial
    }

    pub(crate) const fn waiter_id(self) -> WaiterId {
        self.waiter_id
    }
}

/// One waiter removed from a ready queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadyItem {
    key: ReadyKey,
    reasons: ReadyReasons,
}

impl ReadyItem {
    pub(crate) const fn key(self) -> ReadyKey {
        self.key
    }

    pub(crate) const fn reasons(self) -> ReadyReasons {
        self.reasons
    }
}

/// A ready mark that the queue cannot accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadyError {
    EnqueueSerialExhausted,
    ClassMismatch {
        waiter_id: WaiterId,
        queued: ReadyClass,
        requested: ReadyClass,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueuedReady {
    key: ReadyKey,
    reasons: ReadyReasons,
}

/// Per-class FIFO sets with a persistent round-robin cursor.
#[derive(Debug)]
pub(crate) struct ReadyQueues {
    queues: [BTreeSet<(u64, WaiterId)>; ReadyClass::ALL.len()],
    queued: BTreeMap<WaiterId, QueuedReady>,
    next_enqueue_serial: u64,
    next_class: usize,
}

impl Default for ReadyQueues {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadyQueues {
    pub(crate) fn new() -> Self {
        Self {
            queues: std::array::from_fn(|_| BTreeSet::new()),
            queued: BTreeMap::new(),
            next_enqueue_serial: 0,
            next_class: 0,
        }
    }

    /// Mark one waiter ready, or coalesce reasons for its existing row.
    pub(crate) fn mark(
        &mut self,
        waiter_id: WaiterId,
        class: ReadyClass,
        reasons: ReadyReasons,
    ) -> Result<ReadyKey, ReadyError> {
        if let Some(queued) = self.queued.get_mut(&waiter_id) {
            if queued.key.class != class {
                return Err(ReadyError::ClassMismatch {
                    waiter_id,
                    queued: queued.key.class,
                    requested: class,
                });
            }
            queued.reasons.insert(reasons);
            return Ok(queued.key);
        }

        let enqueue_serial = self
            .next_enqueue_serial
            .checked_add(1)
            .ok_or(ReadyError::EnqueueSerialExhausted)?;
        let key = ReadyKey {
            class,
            enqueue_serial,
            waiter_id,
        };
        self.queues[class.index()].insert((enqueue_serial, waiter_id));
        self.queued.insert(waiter_id, QueuedReady { key, reasons });
        self.next_enqueue_serial = enqueue_serial;
        Ok(key)
    }

    /// Remove one ready waiter and advance the persistent class cursor.
    pub(crate) fn pop_next(&mut self) -> Option<ReadyItem> {
        for offset in 0..ReadyClass::ALL.len() {
            let class_index = (self.next_class + offset) % ReadyClass::ALL.len();
            let Some((enqueue_serial, waiter_id)) = self.queues[class_index].first().copied()
            else {
                continue;
            };
            self.queues[class_index].remove(&(enqueue_serial, waiter_id));
            let queued = self.queued.remove(&waiter_id)?;
            debug_assert_eq!(
                queued.key,
                ReadyKey {
                    class: ReadyClass::ALL[class_index],
                    enqueue_serial,
                    waiter_id,
                }
            );
            self.next_class = (class_index + 1) % ReadyClass::ALL.len();
            return Some(ReadyItem {
                key: queued.key,
                reasons: queued.reasons,
            });
        }
        None
    }

    /// Remove one exact stored key without scanning any queue.
    pub(crate) fn remove(&mut self, key: ReadyKey) -> bool {
        if self.queued.get(&key.waiter_id).map(|queued| queued.key) != Some(key) {
            return false;
        }
        let removed = self.queues[key.class.index()].remove(&(key.enqueue_serial, key.waiter_id));
        if removed {
            self.queued.remove(&key.waiter_id);
        }
        removed
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.queued.len()
    }

    #[cfg(test)]
    fn with_next_enqueue_serial(next_enqueue_serial: u64) -> Self {
        Self {
            next_enqueue_serial,
            ..Self::new()
        }
    }
}

/// The exact ordered key for one armed deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeadlineKey {
    instant: Instant,
    waiter_id: WaiterId,
}

impl DeadlineKey {
    pub(crate) const fn instant(self) -> Instant {
        self.instant
    }

    pub(crate) const fn waiter_id(self) -> WaiterId {
        self.waiter_id
    }
}

/// The result of arming one deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeadlineArm {
    key: DeadlineKey,
    is_due: bool,
}

impl DeadlineArm {
    pub(crate) const fn key(self) -> DeadlineKey {
        self.key
    }

    pub(crate) const fn is_due(self) -> bool {
        self.is_due
    }
}

/// A deadline operation that the index cannot accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeadlineError {
    NonprogressRearm {
        waiter_id: WaiterId,
        last_fired: Instant,
        requested: Instant,
    },
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DeadlineState {
    armed: Option<Instant>,
    last_fired: Option<Instant>,
}

/// An ordered deadline index with exact removal and no stale rows.
#[derive(Debug, Default)]
pub(crate) struct DeadlineIndex {
    deadlines: BTreeSet<(Instant, WaiterId)>,
    states: BTreeMap<WaiterId, DeadlineState>,
}

impl DeadlineIndex {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Arm or re-arm one waiter and report if the deadline is already due.
    pub(crate) fn arm(
        &mut self,
        waiter_id: WaiterId,
        deadline: Instant,
        now: Instant,
    ) -> Result<DeadlineArm, DeadlineError> {
        let state = self.states.get(&waiter_id).copied().unwrap_or_default();
        if let Some(last_fired) = state.last_fired
            && deadline <= last_fired
        {
            return Err(DeadlineError::NonprogressRearm {
                waiter_id,
                last_fired,
                requested: deadline,
            });
        }

        if let Some(old_deadline) = state.armed {
            self.deadlines.remove(&(old_deadline, waiter_id));
        }
        self.deadlines.insert((deadline, waiter_id));
        self.states.insert(
            waiter_id,
            DeadlineState {
                armed: Some(deadline),
                last_fired: state.last_fired,
            },
        );

        Ok(DeadlineArm {
            key: DeadlineKey {
                instant: deadline,
                waiter_id,
            },
            is_due: deadline <= now,
        })
    }

    /// Disarm one exact stored key without removing fired history.
    pub(crate) fn disarm(&mut self, key: DeadlineKey) -> bool {
        let Some(state) = self.states.get(&key.waiter_id).copied() else {
            return false;
        };
        if state.armed != Some(key.instant) {
            return false;
        }
        if !self.deadlines.remove(&(key.instant, key.waiter_id)) {
            return false;
        }
        if state.last_fired.is_some() {
            self.states.insert(
                key.waiter_id,
                DeadlineState {
                    armed: None,
                    last_fired: state.last_fired,
                },
            );
        } else {
            self.states.remove(&key.waiter_id);
        }
        true
    }

    /// Remove all deadline state for one retiring waiter.
    pub(crate) fn retire(&mut self, waiter_id: WaiterId) -> Option<DeadlineKey> {
        let state = self.states.remove(&waiter_id)?;
        state.armed.map(|instant| {
            self.deadlines.remove(&(instant, waiter_id));
            DeadlineKey { instant, waiter_id }
        })
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|(instant, _)| *instant)
    }

    /// Remove at most `limit` due keys in deadline order.
    pub(crate) fn pop_due(&mut self, now: Instant, limit: usize) -> Vec<DeadlineKey> {
        let mut due = Vec::with_capacity(limit.min(self.deadlines.len()));
        while due.len() < limit {
            let Some((instant, waiter_id)) = self.deadlines.first().copied() else {
                break;
            };
            if instant > now {
                break;
            }
            self.deadlines.remove(&(instant, waiter_id));
            let state = self
                .states
                .get_mut(&waiter_id)
                .expect("an armed deadline must have waiter state");
            debug_assert_eq!(state.armed, Some(instant));
            state.armed = None;
            state.last_fired = Some(instant);
            due.push(DeadlineKey { instant, waiter_id });
        }
        due
    }

    pub(crate) fn has_due(&self, now: Instant) -> bool {
        self.next_deadline().is_some_and(|deadline| deadline <= now)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.deadlines.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.deadlines.len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const FIRST_REASON: ReadyReasons = ReadyReasons::from_bits(1 << 0);
    const SECOND_REASON: ReadyReasons = ReadyReasons::from_bits(1 << 1);

    #[test]
    fn ready_classes_match_the_frozen_order() {
        assert_eq!(
            ReadyClass::ALL,
            [
                ReadyClass::ControlIngress,
                ReadyClass::Cleanup,
                ReadyClass::CoreCompletion,
                ReadyClass::HostCompletion,
                ReadyClass::PluginCompletion,
                ReadyClass::Deadline,
                ReadyClass::Observe,
                ReadyClass::InventoryReconcile,
                ReadyClass::JournalPull,
                ReadyClass::ProjectionApply,
                ReadyClass::Baseline,
                ReadyClass::HostBridge,
                ReadyClass::SubscriberDelivery,
                ReadyClass::ProviderResync,
                ReadyClass::PackageEventDelivery,
            ]
        );
    }

    #[test]
    fn one_class_is_fifo() {
        let mut ready = ReadyQueues::new();
        ready
            .mark(WaiterId(8), ReadyClass::Cleanup, FIRST_REASON)
            .unwrap();
        ready
            .mark(WaiterId(3), ReadyClass::Cleanup, SECOND_REASON)
            .unwrap();

        assert_eq!(ready.pop_next().unwrap().key().waiter_id(), WaiterId(8));
        assert_eq!(ready.pop_next().unwrap().key().waiter_id(), WaiterId(3));
        assert!(ready.pop_next().is_none());
    }

    #[test]
    fn class_cursor_preserves_cross_turn_fairness() {
        let mut ready = ReadyQueues::new();
        for class in [
            ReadyClass::ControlIngress,
            ReadyClass::Cleanup,
            ReadyClass::CoreCompletion,
        ] {
            ready
                .mark(WaiterId(class.index() as u64 + 1), class, FIRST_REASON)
                .unwrap();
        }

        assert_eq!(
            ready.pop_next().unwrap().key().class(),
            ReadyClass::ControlIngress
        );
        ready
            .mark(WaiterId(20), ReadyClass::ControlIngress, FIRST_REASON)
            .unwrap();
        assert_eq!(ready.pop_next().unwrap().key().class(), ReadyClass::Cleanup);
        assert_eq!(
            ready.pop_next().unwrap().key().class(),
            ReadyClass::CoreCompletion
        );
        assert_eq!(
            ready.pop_next().unwrap().key().class(),
            ReadyClass::ControlIngress
        );
    }

    #[test]
    fn repeated_marks_coalesce_the_row_and_reasons() {
        let mut ready = ReadyQueues::new();
        let first = ready
            .mark(WaiterId(4), ReadyClass::HostCompletion, FIRST_REASON)
            .unwrap();
        let second = ready
            .mark(WaiterId(4), ReadyClass::HostCompletion, SECOND_REASON)
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(ready.len(), 1);
        let item = ready.pop_next().unwrap();
        assert!(item.reasons().contains(FIRST_REASON));
        assert!(item.reasons().contains(SECOND_REASON));
    }

    #[test]
    fn a_class_mismatch_keeps_the_original_row() {
        let mut ready = ReadyQueues::new();
        let key = ready
            .mark(WaiterId(9), ReadyClass::Observe, FIRST_REASON)
            .unwrap();

        assert_eq!(
            ready.mark(WaiterId(9), ReadyClass::Baseline, SECOND_REASON),
            Err(ReadyError::ClassMismatch {
                waiter_id: WaiterId(9),
                queued: ReadyClass::Observe,
                requested: ReadyClass::Baseline,
            })
        );
        assert_eq!(ready.len(), 1);
        let item = ready.pop_next().unwrap();
        assert_eq!(item.key(), key);
        assert_eq!(item.reasons(), FIRST_REASON);
    }

    #[test]
    fn removal_requires_the_exact_ready_key() {
        let mut ready = ReadyQueues::new();
        let key = ready
            .mark(WaiterId(12), ReadyClass::ProviderResync, FIRST_REASON)
            .unwrap();
        let stale = ReadyKey {
            enqueue_serial: key.enqueue_serial() + 1,
            ..key
        };

        assert!(!ready.remove(stale));
        assert_eq!(ready.len(), 1);
        assert!(ready.remove(key));
        assert!(ready.is_empty());
    }

    #[test]
    fn enqueue_serial_stops_before_wrap() {
        let mut ready = ReadyQueues::with_next_enqueue_serial(u64::MAX - 1);
        let key = ready
            .mark(WaiterId(1), ReadyClass::Cleanup, FIRST_REASON)
            .unwrap();
        assert_eq!(key.enqueue_serial(), u64::MAX);
        assert_eq!(
            ready.mark(WaiterId(2), ReadyClass::Cleanup, FIRST_REASON),
            Err(ReadyError::EnqueueSerialExhausted)
        );
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn initially_due_deadline_is_valid_and_due() {
        let now = Instant::now();
        let deadline = now - Duration::from_millis(1);
        let mut deadlines = DeadlineIndex::new();

        let armed = deadlines.arm(WaiterId(1), deadline, now).unwrap();
        assert!(armed.is_due());
        assert!(deadlines.has_due(now));
        assert_eq!(deadlines.next_deadline(), Some(deadline));
        assert_eq!(deadlines.pop_due(now, 1), vec![armed.key()]);
        assert!(deadlines.is_empty());
    }

    #[test]
    fn rearm_and_disarm_remove_exact_keys() {
        let now = Instant::now();
        let first = now + Duration::from_secs(1);
        let second = now + Duration::from_secs(2);
        let mut deadlines = DeadlineIndex::new();

        let first_key = deadlines.arm(WaiterId(2), first, now).unwrap().key();
        let second_key = deadlines.arm(WaiterId(2), second, now).unwrap().key();
        assert_eq!(deadlines.len(), 1);
        assert_eq!(deadlines.next_deadline(), Some(second));
        assert!(!deadlines.disarm(first_key));
        assert!(deadlines.disarm(second_key));
        assert!(deadlines.is_empty());
        assert!(deadlines.states.is_empty());
    }

    #[test]
    fn due_pop_is_bounded_and_ordered() {
        let now = Instant::now();
        let mut deadlines = DeadlineIndex::new();
        for (id, offset) in [(1, 3), (2, 1), (3, 2)] {
            deadlines
                .arm(WaiterId(id), now - Duration::from_secs(offset), now)
                .unwrap();
        }

        let first = deadlines.pop_due(now, 2);
        assert_eq!(first.len(), 2);
        assert!(first[0].instant() < first[1].instant());
        assert_eq!(deadlines.len(), 1);
        assert!(deadlines.has_due(now));
        assert_eq!(deadlines.pop_due(now, 0), Vec::<DeadlineKey>::new());
        assert_eq!(deadlines.pop_due(now, 2).len(), 1);
        assert!(deadlines.is_empty());
    }

    #[test]
    fn retire_removes_armed_and_fired_state() {
        let now = Instant::now();
        let mut deadlines = DeadlineIndex::new();
        let armed = deadlines
            .arm(WaiterId(5), now + Duration::from_secs(1), now)
            .unwrap()
            .key();

        assert_eq!(deadlines.retire(WaiterId(5)), Some(armed));
        assert!(deadlines.deadlines.is_empty());
        assert!(deadlines.states.is_empty());

        deadlines.arm(WaiterId(5), now, now).unwrap();
        deadlines.pop_due(now, 1);
        assert_eq!(deadlines.retire(WaiterId(5)), None);
        assert!(deadlines.states.is_empty());
    }

    #[test]
    fn deadline_churn_does_not_accumulate_index_rows() {
        let now = Instant::now();
        let mut deadlines = DeadlineIndex::new();
        for offset in 1..=100 {
            let armed = deadlines
                .arm(WaiterId(7), now + Duration::from_millis(offset), now)
                .unwrap();
            assert_eq!(deadlines.len(), 1);
            assert!(deadlines.disarm(armed.key()));
            assert!(deadlines.deadlines.is_empty());
            assert!(deadlines.states.is_empty());
        }
    }

    #[test]
    fn rearm_after_firing_must_advance() {
        let now = Instant::now();
        let fired = now - Duration::from_secs(1);
        let mut deadlines = DeadlineIndex::new();
        deadlines.arm(WaiterId(8), fired, now).unwrap();
        deadlines.pop_due(now, 1);

        for requested in [fired - Duration::from_nanos(1), fired] {
            assert_eq!(
                deadlines.arm(WaiterId(8), requested, now),
                Err(DeadlineError::NonprogressRearm {
                    waiter_id: WaiterId(8),
                    last_fired: fired,
                    requested,
                })
            );
        }
        let later = deadlines
            .arm(WaiterId(8), fired + Duration::from_nanos(1), now)
            .unwrap();
        assert!(later.is_due());
        assert_eq!(deadlines.len(), 1);
    }
}
