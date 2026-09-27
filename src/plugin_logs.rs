//! Per-plugin structured log records.
//!
//! Plugins write through `botster.log.*`; operators read through the
//! `ReadPluginLogs` daemon request. Every number here is a user decision
//! (docs/plans/plugin-platform.md sections 5.0 and 7.1): 64 KiB per record,
//! 256 records and 512 KiB of record text per plugin, 100 records per second
//! with a burst of 200.
//!
//! Funding: each plugin's entry, name, and ring (allocated once at its full
//! capacity) are charged to the callback account when the entry is created,
//! and each record's text is charged before it is copied. A read copies its
//! page under a charge that the caller carries until the reply retires.
//! Plugin workers append under a short lock; the daemon owner reads with
//! `try_lock` and never waits.
//!
//! Every record carries the generation of the VM that wrote it, so a failed
//! load removes exactly its own records, and records from different loads of
//! one package stay distinguishable.

use std::collections::{BTreeMap, VecDeque};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount};

pub(crate) const MAX_RECORD_BYTES: usize = 64 * 1024;
const RING_RECORDS: usize = 256;
const RING_TEXT_BYTES: usize = 512 * 1024;
const RATE_PER_SECOND: u64 = 100;
const BURST: u64 = 200;
/// Token bucket units per record; tokens refill at `RATE_PER_SECOND` records
/// per second, so one millisecond adds `RATE_PER_SECOND` milli-tokens.
const MILLI: u64 = 1000;

/// Generations are unique across the process; 0 is never issued.
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

/// A new log generation for one VM.
pub(crate) fn next_generation() -> u64 {
    NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

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
    pub(crate) generation: u64,
    pub(crate) at_ms: u64,
    pub(crate) level: LogLevel,
    pub(crate) message: String,
    /// JSON text of the record's fields, if any.
    pub(crate) fields: Option<String>,
    /// Records refused by the rate limit since the previous accepted record.
    pub(crate) dropped_before: u64,
}

impl LogRecord {
    /// Heap bytes the record's text owns (both strings are exact copies).
    fn text_bytes(&self) -> usize {
        self.message.len() + self.fields.as_ref().map_or(0, String::len)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendOutcome {
    Accepted { seq: u64 },
    RateLimited,
    TooLarge,
    Capacity,
}

/// One page of a plugin's records, with the charge that funds the copy.
#[derive(Debug)]
pub(crate) struct LogPage {
    pub(crate) records: Vec<LogRecord>,
    /// The sequence the next record will get.
    pub(crate) next_seq: u64,
    /// The oldest sequence still in the ring (records before it were evicted).
    pub(crate) first_available_seq: u64,
    /// Funds `records`; the caller keeps it until the reply built from the
    /// page retires.
    pub(crate) charge: Option<LuaCallbackCharge>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadError {
    /// A plugin is appending right now; the caller may retry.
    Busy,
    /// The callback account cannot fund the page copy right now.
    Capacity,
}

struct PluginLog {
    records: VecDeque<LogRecord>,
    text_bytes: usize,
    /// Charged for the entry and ring; never changes while the entry lives.
    fixed_bytes: usize,
    next_seq: u64,
    rate_dropped: u64,
    tokens_milli: u64,
    refilled_at_ms: u64,
    charge: LuaCallbackCharge,
}

impl PluginLog {
    fn release_text(&mut self, record: &LogRecord) {
        self.text_bytes -= record.text_bytes();
        assert!(self.charge.shrink_to(self.fixed_bytes + self.text_bytes));
    }
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

    /// Append one record for `plugin` written by VM `generation`. `now_ms`
    /// is monotonic (for the rate limit) and `wall_ms` is the record's Unix
    /// time. An accepted record is also written to the Hub log.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append(
        &self,
        plugin: &str,
        generation: u64,
        level: LogLevel,
        message: &str,
        fields: Option<&str>,
        now_ms: u64,
        wall_ms: u64,
    ) -> AppendOutcome {
        let text = message.len() + fields.map_or(0, str::len);
        if text > MAX_RECORD_BYTES {
            return AppendOutcome::TooLarge;
        }
        let mut plugins = self
            .plugins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !plugins.contains_key(plugin) {
            let Some(log) = self.new_log(plugin, now_ms) else {
                return AppendOutcome::Capacity;
            };
            plugins.insert(plugin.to_string(), log);
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
            && (log.records.len() >= RING_RECORDS || log.text_bytes + text > RING_TEXT_BYTES)
        {
            let evicted = log.records.pop_front().expect("not empty");
            log.release_text(&evicted);
        }
        if log.charge.grow(text).is_err() {
            return AppendOutcome::Capacity;
        }
        log.tokens_milli -= MILLI;
        let seq = log.next_seq;
        log.next_seq += 1;
        log.text_bytes += text;
        let record = LogRecord {
            seq,
            generation,
            at_ms: wall_ms,
            level,
            message: message.to_string(),
            fields: fields.map(str::to_string),
            dropped_before: std::mem::take(&mut log.rate_dropped),
        };
        write_hub_log(plugin, &record);
        log.records.push_back(record);
        AppendOutcome::Accepted { seq }
    }

    /// Create a plugin's entry with its ring at full capacity, charged first.
    fn new_log(&self, plugin: &str, now_ms: u64) -> Option<PluginLog> {
        let fixed_bytes = size_of::<(String, PluginLog)>()
            + plugin.len()
            + RING_RECORDS * size_of::<LogRecord>();
        let mut charge = self.memory.reserve_callback_total(fixed_bytes).ok()?;
        let records = VecDeque::with_capacity(RING_RECORDS);
        // `with_capacity` may round up; fund what it actually allocated.
        let extra = records
            .capacity()
            .saturating_sub(RING_RECORDS)
            .saturating_mul(size_of::<LogRecord>());
        charge.grow(extra).ok()?;
        Some(PluginLog {
            records,
            text_bytes: 0,
            fixed_bytes: fixed_bytes + extra,
            next_seq: 1,
            rate_dropped: 0,
            tokens_milli: BURST * MILLI,
            refilled_at_ms: now_ms,
            charge,
        })
    }

    /// Copy the records after `after_seq` without ever waiting on a plugin.
    /// The copy is funded before it is made; the page carries that charge.
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
                charge: None,
            });
        };
        let selected = || log.records.iter().filter(|record| record.seq > after_seq);
        let count = selected().count();
        let bytes = count * size_of::<LogRecord>()
            + selected().map(LogRecord::text_bytes).sum::<usize>();
        let charge = self
            .memory
            .reserve_callback_total(bytes)
            .map_err(|_| ReadError::Capacity)?;
        let mut records = Vec::with_capacity(count);
        records.extend(selected().cloned());
        Ok(LogPage {
            records,
            next_seq: log.next_seq,
            first_available_seq: log
                .records
                .front()
                .map_or(log.next_seq, |record| record.seq),
            charge: Some(charge),
        })
    }

    /// Drop the records one VM generation wrote (a failed load or reload).
    pub(crate) fn remove_generation(&self, plugin: &str, generation: u64) {
        let mut plugins = self
            .plugins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(log) = plugins.get_mut(plugin) else {
            return;
        };
        // Retain in place: the ring keeps its funded allocation.
        let text_bytes = &mut log.text_bytes;
        log.records.retain(|record| {
            if record.generation == generation {
                *text_bytes -= record.text_bytes();
                false
            } else {
                true
            }
        });
        assert!(log.charge.shrink_to(log.fixed_bytes + log.text_bytes));
    }

    /// Drop a plugin's records and release their charge (package unload).
    pub(crate) fn remove(&self, plugin: &str) {
        self.plugins
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(plugin);
    }
}

/// The Hub log is the daemon's standard error.
fn write_hub_log(plugin: &str, record: &LogRecord) {
    #[cfg(test)]
    HUB_LOG_LINES.with(|lines| lines.set(lines.get() + 1));
    eprintln!(
        "plugin_log package={plugin} generation={} seq={} level={} dropped_before={} message={:?} fields={}",
        record.generation,
        record.seq,
        record.level.as_str(),
        record.dropped_before,
        record.message,
        record.fields.as_deref().unwrap_or("null"),
    );
}

#[cfg(test)]
thread_local! {
    /// Hub log lines this thread wrote.
    pub(crate) static HUB_LOG_LINES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
            total_callback_bytes: 4 * 1024 * 1024,
        })
        .unwrap();
        (PluginLogBook::new(Arc::clone(&memory)), memory)
    }

    fn append(book: &PluginLogBook, message: &str, now_ms: u64) -> AppendOutcome {
        book.append("p", 7, LogLevel::Info, message, None, now_ms, 0)
    }

    #[test]
    fn records_are_sequenced_charged_and_released() {
        let (book, memory) = book();
        let first = book.append("p", 7, LogLevel::Info, "one", Some("{\"a\":1}"), 0, 10);
        assert_eq!(first, AppendOutcome::Accepted { seq: 1 });
        append(&book, "two", 0);
        let page = book.read("p", 1).unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].message, "two");
        assert_eq!(page.records[0].generation, 7);
        assert_eq!(page.next_seq, 3);
        drop(page);
        assert!(memory.usage().1 > 0);
        book.remove("p");
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn the_entry_and_its_full_ring_are_charged_before_the_first_record() {
        let (book, memory) = book();
        append(&book, "", 0);
        // An empty record owns no text: everything charged is the entry and
        // the ring's full-capacity allocation.
        assert!(
            memory.usage().1 >= RING_RECORDS * size_of::<LogRecord>(),
            "charged {} bytes",
            memory.usage().1
        );
        book.remove("p");
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn a_read_page_carries_the_charge_for_its_copy() {
        let (book, memory) = book();
        let mut now = 0;
        let big = "y".repeat(MAX_RECORD_BYTES);
        for _ in 0..20 {
            now += 100;
            append(&book, &big, now);
        }
        let ring = memory.usage().1;
        // The largest page is the whole ring.
        let page = book.read("p", 0).unwrap();
        let text: usize = page.records.iter().map(LogRecord::text_bytes).sum();
        assert!(text <= RING_TEXT_BYTES, "{text}");
        assert!(
            memory.usage().1 >= ring + text,
            "the copy is funded while the page lives"
        );
        drop(page);
        assert_eq!(memory.usage().1, ring);
        book.remove("p");
    }

    #[test]
    fn a_failed_generation_removes_only_its_own_records() {
        let (book, memory) = book();
        book.append("p", 1, LogLevel::Info, "live", None, 0, 0);
        book.append("p", 2, LogLevel::Info, "failed-load", None, 0, 0);
        let before = memory.usage().1;
        book.remove_generation("p", 2);
        let page = book.read("p", 0).unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].message, "live");
        drop(page);
        assert_eq!(memory.usage().1, before - "failed-load".len());
        book.remove("p");
    }

    #[test]
    fn accepted_records_are_written_to_the_hub_log() {
        let (book, _memory) = book();
        let before = HUB_LOG_LINES.with(std::cell::Cell::get);
        append(&book, "mirrored", 0);
        assert_eq!(HUB_LOG_LINES.with(std::cell::Cell::get), before + 1);
    }

    #[test]
    fn the_rate_limit_refuses_a_burst_and_reports_the_drop() {
        let (book, _memory) = book();
        for _ in 0..BURST {
            assert!(matches!(append(&book, "x", 0), AppendOutcome::Accepted { .. }));
        }
        assert_eq!(append(&book, "x", 0), AppendOutcome::RateLimited);
        // 10 ms later one record's worth of tokens (100/s) has refilled.
        assert!(matches!(append(&book, "late", 10), AppendOutcome::Accepted { .. }));
        let page = book.read("p", 0).unwrap();
        let last = page.records.last().unwrap();
        assert_eq!((last.message.as_str(), last.dropped_before), ("late", 1));
    }

    #[test]
    fn the_ring_evicts_oldest_first_within_its_text_cap() {
        let (book, memory) = book();
        let big = "y".repeat(MAX_RECORD_BYTES);
        let mut now = 0;
        for _ in 0..20 {
            now += 100;
            append(&book, &big, now);
        }
        let page = book.read("p", 0).unwrap();
        let text: usize = page.records.iter().map(LogRecord::text_bytes).sum();
        assert!(text <= RING_TEXT_BYTES, "{text}");
        assert!(page.first_available_seq > 1, "the oldest records were evicted");
        assert_eq!(page.records.last().unwrap().seq, 20);
        drop(page);
        book.remove("p");
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn oversized_records_are_refused() {
        let (book, _memory) = book();
        let huge = "z".repeat(MAX_RECORD_BYTES + 1);
        assert_eq!(append(&book, &huge, 0), AppendOutcome::TooLarge);
    }

    #[test]
    fn reads_never_wait_on_an_appending_plugin() {
        let (book, _memory) = book();
        let _held = book.plugins.lock().unwrap();
        assert!(matches!(book.read("p", 0), Err(ReadError::Busy)));
    }
}
