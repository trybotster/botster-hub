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
}

impl ConnectionAggregate {
    fn new() -> Self {
        Self {
            slots: (0..MAX_SUBSCRIPTION_CHANNELS)
                .map(|_| Arc::new(AtomicUsize::new(0)))
                .collect(),
        }
    }

    #[must_use]
    pub(crate) fn buffered(&self) -> usize {
        self.slots
            .iter()
            .map(|slot| slot.load(Ordering::Acquire))
            .sum()
    }

    #[must_use]
    pub(crate) fn permits_send(&self, frame_len: usize) -> bool {
        self.buffered().saturating_add(frame_len) <= AGGREGATE_BUFFERED_HIGH
    }

    #[must_use]
    pub(crate) fn below_low_water(&self) -> bool {
        self.buffered() < AGGREGATE_BUFFERED_LOW
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

    #[must_use]
    pub(crate) fn permits_send(&self, frame_len: usize) -> bool {
        self.aggregate.permits_send(frame_len)
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
        assert!(!budget.permits_send(65_536));
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
}
