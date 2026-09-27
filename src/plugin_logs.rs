//! Per-plugin structured log records.
//!
//! Plugins write through `botster.log.*`; operators read through the
//! `ReadPluginLogs` daemon request. Every number here is a user decision
//! (docs/plans/plugin-platform.md sections 5.0 and 7.1): 64 KiB per record,
//! 256 records and 512 KiB per plugin, 100 records per second with a burst of
//! 200. The ring's bytes are charged to the callback account before they are
//! allocated. Plugin workers append under a short lock; the daemon owner reads
//! with `try_lock` and never waits.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, TryLockError};

use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount};

pub(crate) const MAX_RECORD_BYTES: usize = 64 * 1024;
const RING_RECORDS: usize = 256;
const RING_BYTES: usize = 512 * 1024;
const RATE_PER_SECOND: u64 = 100;
const BURST: u64 = 200;
/// Token bucket units per record; tokens refill at `RATE_PER_SECOND` records
/// per second, so one millisecond adds `RATE_PER_SECOND` milli-tokens.
const MILLI: u64 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogRecord {
    pub(crate) seq: u64,
    pub(crate) at_ms: u64,
    pub(crate) level: LogLevel,
    pub(crate) message: String,
    /// JSON text of the record's fields, if any.
    pub(crate) fields: Option<String>,
    /// Records refused by the rate limit since the previous accepted record.
    pub(crate) dropped_before: u64,
}

impl LogRecord {
    fn charged_bytes(message: &str, fields: Option<&str>) -> usize {
        std::mem::size_of::<LogRecord>() + message.len() + fields.map_or(0, str::len)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendOutcome {
    Accepted { seq: u64 },
    RateLimited,
    TooLarge,
    Capacity,
}

/// One page of a plugin's records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogPage {
    pub(crate) records: Vec<LogRecord>,
    /// The sequence the next record will get.
    pub(crate) next_seq: u64,
    /// The oldest sequence still in the ring (records before it were evicted).
    pub(crate) first_available_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadError {
    /// A plugin is appending right now; the caller may retry.
    Busy,
}

struct PluginLog {
    records: VecDeque<LogRecord>,
    bytes: usize,
    next_seq: u64,
    rate_dropped: u64,
    tokens_milli: u64,
    refilled_at_ms: u64,
    charge: LuaCallbackCharge,
}

pub(crate) struct PluginLogBook {
    memory: Arc<LuaMemoryAccount>,
    plugins: Mutex<BTreeMap<String, PluginLog>>,
}

impl PluginLogBook {
    pub(crate) fn new(memory: Arc<LuaMemoryAccount>) -> Self {
        Self {
            memory,
            plugins: Mutex::new(BTreeMap::new()),
        }
    }

    /// Append one record. `now_ms` is monotonic (for the rate limit) and
    /// `wall_ms` is the record's Unix time.
    pub(crate) fn append(
        &self,
        plugin: &str,
        level: LogLevel,
        message: &str,
        fields: Option<&str>,
        now_ms: u64,
        wall_ms: u64,
    ) -> AppendOutcome {
        let size = LogRecord::charged_bytes(message, fields);
        if message.len() + fields.map_or(0, str::len) > MAX_RECORD_BYTES {
            return AppendOutcome::TooLarge;
        }
        let mut plugins = self
            .plugins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !plugins.contains_key(plugin) {
            let Ok(charge) = self.memory.reserve_callback_total(0) else {
                return AppendOutcome::Capacity;
            };
            plugins.insert(
                plugin.to_string(),
                PluginLog {
                    records: VecDeque::new(),
                    bytes: 0,
                    next_seq: 1,
                    rate_dropped: 0,
                    tokens_milli: BURST * MILLI,
                    refilled_at_ms: now_ms,
                    charge,
                },
            );
        }
        let log = plugins.get_mut(plugin).expect("inserted above");
        let elapsed = now_ms.saturating_sub(log.refilled_at_ms);
        log.tokens_milli = log
            .tokens_milli
            .saturating_add(elapsed.saturating_mul(RATE_PER_SECOND))
            .min(BURST * MILLI);
        log.refilled_at_ms = now_ms;
        if log.tokens_milli < MILLI {
            log.rate_dropped = log.rate_dropped.saturating_add(1);
            return AppendOutcome::RateLimited;
        }
        // Evict the oldest records first until the new one fits both caps.
        while !log.records.is_empty()
            && (log.records.len() >= RING_RECORDS || log.bytes + size > RING_BYTES)
        {
            let evicted = log.records.pop_front().expect("not empty");
            let freed = LogRecord::charged_bytes(&evicted.message, evicted.fields.as_deref());
            drop(evicted);
            log.bytes -= freed;
            assert!(log.charge.shrink_to(log.bytes));
        }
        if log.charge.grow(size).is_err() {
            return AppendOutcome::Capacity;
        }
        log.tokens_milli -= MILLI;
        let seq = log.next_seq;
        log.next_seq += 1;
        log.bytes += size;
        log.records.push_back(LogRecord {
            seq,
            at_ms: wall_ms,
            level,
            message: message.to_string(),
            fields: fields.map(str::to_string),
            dropped_before: std::mem::take(&mut log.rate_dropped),
        });
        AppendOutcome::Accepted { seq }
    }

    /// Read the records after `after_seq` without ever waiting on a plugin.
    pub(crate) fn read(&self, plugin: &str, after_seq: u64) -> Result<LogPage, ReadError> {
        let plugins = match self.plugins.try_lock() {
            Ok(plugins) => plugins,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return Err(ReadError::Busy),
        };
        let Some(log) = plugins.get(plugin) else {
            return Ok(LogPage {
                records: Vec::new(),
                next_seq: 1,
                first_available_seq: 1,
            });
        };
        Ok(LogPage {
            records: log
                .records
                .iter()
                .filter(|record| record.seq > after_seq)
                .cloned()
                .collect(),
            next_seq: log.next_seq,
            first_available_seq: log
                .records
                .front()
                .map_or(log.next_seq, |record| record.seq),
        })
    }

    /// Drop a plugin's records and release their charge (package unload).
    pub(crate) fn remove(&self, plugin: &str) {
        self.plugins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(plugin);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_memory::LuaMemoryLimits;

    fn book() -> (PluginLogBook, Arc<LuaMemoryAccount>) {
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1024 * 1024,
            total_vm_bytes: 1024 * 1024,
            per_callback_bytes: 1024 * 1024,
            total_callback_bytes: 1024 * 1024,
        })
        .unwrap();
        (PluginLogBook::new(Arc::clone(&memory)), memory)
    }

    #[test]
    fn records_are_sequenced_charged_and_released() {
        let (book, memory) = book();
        let first = book.append("p", LogLevel::Info, "one", Some("{\"a\":1}"), 0, 10);
        assert_eq!(first, AppendOutcome::Accepted { seq: 1 });
        book.append("p", LogLevel::Warn, "two", None, 0, 11);
        let page = book.read("p", 1).unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].message, "two");
        assert_eq!(page.next_seq, 3);
        assert!(memory.usage().1 > 0);
        book.remove("p");
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn the_rate_limit_refuses_a_burst_and_reports_the_drop() {
        let (book, _memory) = book();
        for _ in 0..BURST {
            assert!(matches!(
                book.append("p", LogLevel::Info, "x", None, 0, 0),
                AppendOutcome::Accepted { .. }
            ));
        }
        assert_eq!(
            book.append("p", LogLevel::Info, "x", None, 0, 0),
            AppendOutcome::RateLimited
        );
        // 10 ms later one record's worth of tokens (100/s) has refilled.
        let after = book.append("p", LogLevel::Info, "late", None, 10, 0);
        assert!(matches!(after, AppendOutcome::Accepted { .. }));
        let page = book.read("p", 0).unwrap();
        let last = page.records.last().unwrap();
        assert_eq!((last.message.as_str(), last.dropped_before), ("late", 1));
    }

    #[test]
    fn the_ring_evicts_oldest_first_within_its_byte_cap() {
        let (book, memory) = book();
        let big = "y".repeat(MAX_RECORD_BYTES);
        let mut now = 0;
        for _ in 0..20 {
            now += 100;
            book.append("p", LogLevel::Info, &big, None, now, 0);
        }
        let page = book.read("p", 0).unwrap();
        let bytes: usize = page
            .records
            .iter()
            .map(|record| LogRecord::charged_bytes(&record.message, record.fields.as_deref()))
            .sum();
        assert!(bytes <= RING_BYTES, "{bytes}");
        assert!(
            page.first_available_seq > 1,
            "the oldest records were evicted"
        );
        assert_eq!(page.records.last().unwrap().seq, 20);
        assert!(memory.usage().1 <= RING_BYTES);
    }

    #[test]
    fn oversized_records_are_refused() {
        let (book, _memory) = book();
        let huge = "z".repeat(MAX_RECORD_BYTES + 1);
        assert_eq!(
            book.append("p", LogLevel::Error, &huge, None, 0, 0),
            AppendOutcome::TooLarge
        );
    }

    #[test]
    fn reads_never_wait_on_an_appending_plugin() {
        let (book, _memory) = book();
        let _held = book.plugins.lock().unwrap();
        assert_eq!(book.read("p", 0), Err(ReadError::Busy));
    }
}
