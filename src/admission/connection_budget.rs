//! One Hub-owned channel budget for each local WebRTC peer generation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) const MAX_CONTROL_CHANNELS: usize = 1;
pub(crate) const MAX_SUBSCRIPTION_CHANNELS: usize = 32;
pub(crate) const MAX_TOTAL_CHANNELS: usize = MAX_CONTROL_CHANNELS + MAX_SUBSCRIPTION_CHANNELS;
pub(crate) const AGGREGATE_BUFFERED_HIGH: usize = 2_097_152;
pub(crate) const AGGREGATE_BUFFERED_LOW: usize = 1_048_576;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ChannelClass {
    Control,
    Terminal,
    Entity,
    Event,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChannelBudgetError {
    ChannelLimit,
    AggregateBuffered,
}

#[derive(Debug)]
pub(crate) struct ConnectionBudget {
    channels: BTreeMap<String, ChannelUsage>,
    aggregate: Arc<ConnectionAggregate>,
}

#[derive(Debug)]
struct ChannelUsage {
    class: ChannelClass,
    buffered: Arc<AtomicUsize>,
    aggregate_slot: Option<usize>,
}

#[derive(Debug)]
pub(crate) struct ConnectionAggregate {
    slots: Box<[Arc<AtomicUsize>]>,
    authorized: AtomicUsize,
}

#[derive(Debug)]
pub(crate) struct AggregateSendPermit {
    aggregate: Arc<ConnectionAggregate>,
    frame_len: usize,
}

impl ConnectionAggregate {
    fn new() -> Self {
        Self {
            slots: (0..MAX_SUBSCRIPTION_CHANNELS)
                .map(|_| Arc::new(AtomicUsize::new(0)))
                .collect(),
            authorized: AtomicUsize::new(0),
        }
    }

    fn published_buffered(&self) -> usize {
        self.slots
            .iter()
            .map(|slot| slot.load(Ordering::Acquire))
            .sum()
    }

    #[must_use]
    pub(crate) fn buffered(&self) -> usize {
        self.published_buffered()
            .saturating_add(self.authorized.load(Ordering::Acquire))
    }

    pub(crate) fn try_authorize(self: &Arc<Self>, frame_len: usize) -> Option<AggregateSendPermit> {
        self.try_extend_authorized(frame_len)
            .then(|| AggregateSendPermit {
                aggregate: Arc::clone(self),
                frame_len,
            })
    }

    fn try_extend_authorized(&self, frame_len: usize) -> bool {
        // A sender publishes channel usage before it drops its permit. The
        // transition can count bytes twice, but it cannot omit them.
        let mut authorized = self.authorized.load(Ordering::Acquire);
        loop {
            if self
                .published_buffered()
                .saturating_add(authorized)
                .saturating_add(frame_len)
                > AGGREGATE_BUFFERED_HIGH
            {
                return false;
            }
            match self.authorized.compare_exchange_weak(
                authorized,
                authorized.saturating_add(frame_len),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => authorized = current,
            }
        }
    }

    #[must_use]
    pub(crate) fn below_low_water(&self) -> bool {
        self.buffered() < AGGREGATE_BUFFERED_LOW
    }
}

impl AggregateSendPermit {
    pub(crate) fn try_resize(&mut self, frame_len: usize) -> bool {
        if frame_len > self.frame_len {
            if !self
                .aggregate
                .try_extend_authorized(frame_len - self.frame_len)
            {
                return false;
            }
        } else if frame_len < self.frame_len {
            self.aggregate
                .authorized
                .fetch_sub(self.frame_len - frame_len, Ordering::AcqRel);
        }
        self.frame_len = frame_len;
        true
    }
}

impl Drop for AggregateSendPermit {
    fn drop(&mut self) {
        let previous = self
            .aggregate
            .authorized
            .fetch_sub(self.frame_len, Ordering::AcqRel);
        debug_assert!(previous >= self.frame_len);
    }
}

impl Default for ConnectionBudget {
    fn default() -> Self {
        Self {
            channels: BTreeMap::new(),
            aggregate: Arc::new(ConnectionAggregate::new()),
        }
    }
}

impl ConnectionBudget {
    pub(crate) fn reserve(
        &mut self,
        label: String,
        class: ChannelClass,
    ) -> Result<Arc<AtomicUsize>, ChannelBudgetError> {
        let subscription_count = self
            .channels
            .values()
            .filter(|usage| usage.class != ChannelClass::Control)
            .count();
        if class == ChannelClass::Control {
            if self
                .channels
                .values()
                .filter(|usage| usage.class == ChannelClass::Control)
                .count()
                >= MAX_CONTROL_CHANNELS
            {
                return Err(ChannelBudgetError::ChannelLimit);
            }
        } else if subscription_count >= MAX_SUBSCRIPTION_CHANNELS {
            return Err(ChannelBudgetError::ChannelLimit);
        }
        if self.channels.len() >= MAX_TOTAL_CHANNELS {
            return Err(ChannelBudgetError::ChannelLimit);
        }
        if class != ChannelClass::Control && self.aggregate_buffered() >= AGGREGATE_BUFFERED_HIGH {
            return Err(ChannelBudgetError::AggregateBuffered);
        }
        let aggregate_slot = if class == ChannelClass::Control {
            None
        } else {
            (0..MAX_SUBSCRIPTION_CHANNELS).find(|candidate| {
                self.channels
                    .values()
                    .all(|usage| usage.aggregate_slot != Some(*candidate))
            })
        };
        if class != ChannelClass::Control && aggregate_slot.is_none() {
            return Err(ChannelBudgetError::ChannelLimit);
        }
        let buffered = aggregate_slot
            .map(|slot| Arc::clone(&self.aggregate.slots[slot]))
            .unwrap_or_else(|| Arc::new(AtomicUsize::new(0)));
        buffered.store(0, Ordering::Release);
        self.channels.insert(
            label,
            ChannelUsage {
                class,
                buffered: Arc::clone(&buffered),
                aggregate_slot,
            },
        );
        Ok(buffered)
    }

    pub(crate) fn release(&mut self, label: &str) -> bool {
        let Some(usage) = self.channels.remove(label) else {
            return false;
        };
        usage.buffered.store(0, Ordering::Release);
        true
    }

    pub(crate) fn usage(&self, label: &str) -> Option<Arc<AtomicUsize>> {
        self.channels
            .get(label)
            .map(|usage| Arc::clone(&usage.buffered))
    }

    #[must_use]
    pub(crate) fn aggregate_buffered(&self) -> usize {
        self.aggregate.buffered()
    }

    pub(crate) fn authorize_send(
        &self,
        label: &str,
        frame_len: usize,
    ) -> Option<AggregateSendPermit> {
        self.channels.contains_key(label).then_some(())?;
        self.aggregate.try_authorize(frame_len)
    }

    pub(crate) fn aggregate(&self) -> Arc<ConnectionAggregate> {
        Arc::clone(&self.aggregate)
    }

    #[cfg(test)]
    pub(crate) fn channel_count(&self) -> usize {
        self.channels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_is_not_part_of_the_aggregate() {
        let mut budget = ConnectionBudget::default();
        let control = budget
            .reserve("control".into(), ChannelClass::Control)
            .expect("control");
        let entity = budget
            .reserve("entity".into(), ChannelClass::Entity)
            .expect("entity");
        control.store(AGGREGATE_BUFFERED_HIGH, Ordering::Release);
        entity.store(42, Ordering::Release);
        assert_eq!(budget.aggregate_buffered(), 42);
    }

    #[test]
    fn one_table_limits_all_subscription_classes() {
        let mut budget = ConnectionBudget::default();
        for index in 0..MAX_SUBSCRIPTION_CHANNELS {
            let class = match index % 3 {
                0 => ChannelClass::Terminal,
                1 => ChannelClass::Entity,
                _ => ChannelClass::Event,
            };
            budget
                .reserve(format!("route-{index}"), class)
                .expect("within limit");
        }
        assert!(matches!(
            budget.reserve("overflow".into(), ChannelClass::Entity),
            Err(ChannelBudgetError::ChannelLimit)
        ));
    }

    #[test]
    fn release_clears_the_derived_aggregate_once() {
        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("entity".into(), ChannelClass::Entity)
            .expect("entity");
        usage.store(42, Ordering::Release);
        assert_eq!(budget.aggregate_buffered(), 42);
        assert_eq!(budget.channel_count(), 1);
        assert!(budget.release("entity"));
        assert_eq!(budget.channel_count(), 0);
        assert_eq!(budget.aggregate_buffered(), 0);
        assert!(!budget.release("entity"));
        assert_eq!(budget.aggregate_buffered(), 0);
    }

    #[test]
    fn exact_ceiling_rejects_a_free_slot_then_recovers_after_targeted_release() {
        let mut budget = ConnectionBudget::default();
        budget
            .reserve("control".into(), ChannelClass::Control)
            .expect("control");
        let mut usages = Vec::new();
        for index in 0..31 {
            let usage = budget
                .reserve(format!("entity-{index}"), ChannelClass::Entity)
                .expect("entity below the aggregate ceiling");
            usage.store(if index < 29 { 65_536 } else { 98_304 }, Ordering::Release);
            usages.push(usage);
        }
        assert_eq!(budget.aggregate_buffered(), AGGREGATE_BUFFERED_HIGH);
        assert_eq!(budget.channel_count(), 32);
        assert!(matches!(
            budget.reserve("entity-31".into(), ChannelClass::Entity),
            Err(ChannelBudgetError::AggregateBuffered)
        ));
        assert!(budget.authorize_send("entity-0", 65_536).is_none());
        assert_eq!(budget.aggregate_buffered(), AGGREGATE_BUFFERED_HIGH);

        assert!(budget.release("entity-30"));
        assert_eq!(budget.aggregate_buffered(), 1_998_848);
        budget
            .reserve("replacement".into(), ChannelClass::Entity)
            .expect("the released aggregate and channel slots admit replacement");

        for index in 0..30 {
            let _ = budget.release(&format!("entity-{index}"));
        }
        let _ = budget.release("replacement");
        assert_eq!(budget.aggregate_buffered(), 0);
        assert_eq!(usages[30].load(Ordering::Acquire), 0);
    }

    #[test]
    fn concurrent_authorizations_cannot_both_claim_the_last_capacity() {
        use std::sync::Barrier;
        use std::sync::mpsc;
        use std::thread;

        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("entity".into(), ChannelClass::Entity)
            .expect("entity");
        usage.store(AGGREGATE_BUFFERED_HIGH - 100, Ordering::Release);
        let aggregate = budget.aggregate();
        let start = Arc::new(Barrier::new(3));
        let finish = Arc::new(Barrier::new(3));
        let (result_tx, result_rx) = mpsc::channel();
        let mut workers = Vec::new();
        for _ in 0..2 {
            let aggregate = Arc::clone(&aggregate);
            let start = Arc::clone(&start);
            let finish = Arc::clone(&finish);
            let result_tx = result_tx.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                let permit = aggregate.try_authorize(100);
                result_tx
                    .send(permit.is_some())
                    .expect("report authorization");
                finish.wait();
                drop(permit);
            }));
        }
        start.wait();
        let results = [
            result_rx.recv().expect("first result"),
            result_rx.recv().expect("second result"),
        ];
        assert_eq!(
            results.into_iter().filter(|permitted| *permitted).count(),
            1
        );
        assert_eq!(aggregate.buffered(), AGGREGATE_BUFFERED_HIGH);
        finish.wait();
        for worker in workers {
            worker.join().expect("authorization worker");
        }
        assert_eq!(aggregate.buffered(), AGGREGATE_BUFFERED_HIGH - 100);
    }

    #[test]
    fn authorization_stays_accounted_until_published_usage_replaces_it() {
        let mut budget = ConnectionBudget::default();
        let usage = budget
            .reserve("entity".into(), ChannelClass::Entity)
            .expect("entity");
        let aggregate = budget.aggregate();
        let mut permit = aggregate.try_authorize(100).expect("initial permit");
        assert_eq!(aggregate.buffered(), 100);
        assert!(permit.try_resize(140));
        assert_eq!(aggregate.buffered(), 140);
        usage.store(140, Ordering::Release);
        assert_eq!(aggregate.buffered(), 280);
        drop(permit);
        assert_eq!(aggregate.buffered(), 140);
    }
}
