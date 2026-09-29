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
//! Plugin workers append under a short lock, and the daemon owner reads under
//! the same lock. Every critical section on it is a bounded memory copy with
//! no I/O and no plugin code (see `read`).
//!
//! The Hub log: the ring is also the Hub log's queue. One mirror thread keeps
//! a cursor per plugin, copies one record at a time under a funded charge,
//! and writes it with no lock held, so a slow or blocked sink never delays a
//! plugin or a read. Records the ring evicts before the mirror reaches them
//! are counted and reported with the next mirrored record.
//!
//! Every record carries the generation of the VM that wrote it. The ring is
//! one chronological log across generations: a failed reload keeps its
//! records, which explain the failure. A failed first load has no live
//! generation, so its caller removes the whole entry.

use std::collections::{BTreeMap, VecDeque};
use std::io::Write as _;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

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

/// Serials for log incarnations in this process; 0 is never issued.
static NEXT_LOG_SERIAL: AtomicU64 = AtomicU64::new(1);

/// The identity of one log incarnation: this process's random boot id and a
/// serial that never repeats within it. A plugin's log gets a new one each
/// time its entry is created (first load, load after an unload, restart), so
/// a reader holding a cursor into an older log can tell that it must restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogId {
    boot: u128,
    serial: u64,
}

impl LogId {
    fn next() -> Self {
        Self {
            boot: boot_id(),
            serial: NEXT_LOG_SERIAL.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// The formatted length: 32 hex digits, a dash, and the serial.
    pub(crate) fn text_len(self) -> usize {
        33 + self
            .serial
            .checked_ilog10()
            .map_or(1, |digits| digits as usize + 1)
    }

    /// Format the id into a string whose whole allocation `charge` funds.
    /// The exact length is funded before the allocation, the string never
    /// grows while it is written, and any rounding up by the allocator is
    /// funded too. `None` when the account cannot fund it.
    pub(crate) fn to_funded_string(self, charge: &mut LuaCallbackCharge) -> Option<String> {
        use std::fmt::Write as _;
        let len = self.text_len();
        charge.grow(len).ok()?;
        let mut text = String::with_capacity(len);
        charge.grow(text.capacity() - len).ok()?;
        let capacity = text.capacity();
        write!(text, "{self}").expect("writing to a String cannot fail");
        debug_assert_eq!((text.len(), text.capacity()), (len, capacity));
        Some(text)
    }
}

impl std::fmt::Display for LogId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:032x}-{}", self.boot, self.serial)
    }
}

/// Random per Hub process, so a log id never repeats across restarts.
fn boot_id() -> u128 {
    static BOOT: std::sync::OnceLock<u128> = std::sync::OnceLock::new();
    *BOOT.get_or_init(random_boot_id)
}

fn random_boot_id() -> u128 {
    let mut bytes = [0_u8; 16];
    if getrandom::fill(&mut bytes).is_ok() {
        return u128::from_le_bytes(bytes);
    }
    // Without the OS source, the start time and process id still differ
    // between restarts.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    nanos ^ (u128::from(std::process::id()) << 96)
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
    Accepted {
        seq: u64,
    },
    /// Refused by the rate limit; `dropped` counts the refusals since the
    /// last accepted record, including this one.
    RateLimited {
        dropped: u64,
    },
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
    /// The log incarnation the page belongs to; `None` for a package with no
    /// log. A different id than the reader's cursor means a new log.
    pub(crate) log_id: Option<LogId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadError {
    /// The callback account cannot fund the page copy right now.
    Capacity,
}

/// Where the mirror writes accepted records.
pub(crate) trait HubLogSink: Send {
    /// Write one record. `unmirrored_before` counts this plugin's records
    /// that left the ring, or could not be funded, before the mirror
    /// reached them.
    fn write(&mut self, plugin: &str, record: &LogRecord, unmirrored_before: u64);
}

/// The Hub log is the daemon's standard error.
struct StderrSink;

impl HubLogSink for StderrSink {
    fn write(&mut self, plugin: &str, record: &LogRecord, unmirrored_before: u64) {
        let _ = writeln!(
            std::io::stderr().lock(),
            "plugin_log package={plugin} generation={} seq={} level={} dropped_before={} unmirrored_before={unmirrored_before} message={:?} fields={}",
            record.generation,
            record.seq,
            record.level.as_str(),
            record.dropped_before,
            record.message,
            record.fields.as_deref().unwrap_or("null"),
        );
    }
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
    /// The next sequence the Hub log mirror writes.
    mirror_next: u64,
    /// Records the mirror could not write since its last written record.
    unmirrored: u64,
    charge: LuaCallbackCharge,
    log_id: LogId,
}

impl PluginLog {
    fn release_text(&mut self, record: &LogRecord) {
        self.text_bytes -= record.text_bytes();
        assert!(self.charge.shrink_to(self.fixed_bytes + self.text_bytes));
    }
}

struct BookState {
    logs: BTreeMap<String, PluginLog>,
    /// The plugin index the mirror serves next, so every plugin gets a turn.
    mirror_turn: usize,
    /// The mirror is inside its wait, so only a notify can wake it.
    #[cfg(test)]
    mirror_parked: bool,
    closed: bool,
}

struct Shared {
    memory: Arc<LuaMemoryAccount>,
    state: Mutex<BookState>,
    /// Appends and shutdown wake the mirror.
    mirror_wake: Condvar,
    /// The mirror announces each time it parks.
    #[cfg(test)]
    mirror_idle: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, BookState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One record copied out of the ring for the Hub log.
struct MirrorLine {
    plugin: String,
    record: LogRecord,
    unmirrored_before: u64,
    /// Declared last: released after the copies above are freed.
    _charge: LuaCallbackCharge,
}

pub(crate) struct PluginLogBook {
    shared: Arc<Shared>,
}

impl PluginLogBook {
    /// A book whose accepted records are mirrored to the Hub log.
    pub(crate) fn new(memory: Arc<LuaMemoryAccount>) -> Self {
        Self::with_sink(memory, Box::new(StderrSink))
    }

    /// A book whose accepted records are mirrored to `sink`.
    pub(crate) fn with_sink(memory: Arc<LuaMemoryAccount>, sink: Box<dyn HubLogSink>) -> Self {
        let book = Self::unmirrored(memory);
        let shared = Arc::clone(&book.shared);
        if let Err(error) = std::thread::Builder::new()
            .name("hub-plugin-log-mirror".to_string())
            .spawn(move || run_mirror(&shared, sink))
        {
            eprintln!("plugin_log mirror unavailable: {error}; records stay in the plugin rings");
        }
        book
    }

    fn unmirrored(memory: Arc<LuaMemoryAccount>) -> Self {
        Self {
            shared: Arc::new(Shared {
                memory,
                state: Mutex::new(BookState {
                    logs: BTreeMap::new(),
                    mirror_turn: 0,
                    #[cfg(test)]
                    mirror_parked: false,
                    closed: false,
                }),
                mirror_wake: Condvar::new(),
                #[cfg(test)]
                mirror_idle: Condvar::new(),
            }),
        }
    }

    /// Append one record for `plugin` written by VM `generation`. `now_ms`
    /// is monotonic (for the rate limit) and `wall_ms` is the record's Unix
    /// time. An accepted record wakes the Hub log mirror; the append never
    /// waits for it.
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
        let mut state = self.shared.lock();
        if !state.logs.contains_key(plugin) {
            let Some(log) = self.new_log(plugin, now_ms) else {
                return AppendOutcome::Capacity;
            };
            state.logs.insert(plugin.to_string(), log);
        }
        let log = state.logs.get_mut(plugin).expect("inserted above");
        let elapsed = now_ms.saturating_sub(log.refilled_at_ms);
        log.tokens_milli = log
            .tokens_milli
            .saturating_add(elapsed.saturating_mul(RATE_PER_SECOND))
            .min(BURST * MILLI);
        log.refilled_at_ms = now_ms;
        if log.tokens_milli < MILLI {
            log.rate_dropped = log.rate_dropped.saturating_add(1);
            return AppendOutcome::RateLimited {
                dropped: log.rate_dropped,
            };
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
        log.records.push_back(LogRecord {
            seq,
            generation,
            at_ms: wall_ms,
            level,
            message: message.to_string(),
            fields: fields.map(str::to_string),
            dropped_before: std::mem::take(&mut log.rate_dropped),
        });
        drop(state);
        self.shared.mirror_wake.notify_one();
        AppendOutcome::Accepted { seq }
    }

    /// Create a plugin's entry with its ring at full capacity, charged first.
    fn new_log(&self, plugin: &str, now_ms: u64) -> Option<PluginLog> {
        let fixed_bytes =
            size_of::<(String, PluginLog)>() + plugin.len() + RING_RECORDS * size_of::<LogRecord>();
        let mut charge = self
            .shared
            .memory
            .reserve_callback_total(fixed_bytes)
            .ok()?;
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
            mirror_next: 1,
            unmirrored: 0,
            charge,
            log_id: LogId::next(),
        })
    }

    /// Copy the records after `after_seq`. The copy is funded before it is
    /// made; the page carries that charge.
    ///
    /// The read takes the lock and waits for it. Every critical section on
    /// this lock is a bounded memory copy with no I/O and no plugin code:
    /// `append` (short ring update and one record copy), `next_mirror_line`
    /// (one record copy; the sink writes with the lock released, in
    /// `run_mirror`), `remove`, and this read. The memory charges they take
    /// are lock-free atomics. A plugin appending, or the mirror copying, can
    /// therefore delay a read only by a copy, never by a plugin or a sink.
    pub(crate) fn read(&self, plugin: &str, after_seq: u64) -> Result<LogPage, ReadError> {
        let state = self.shared.lock();
        let Some(log) = state.logs.get(plugin) else {
            return Ok(LogPage {
                records: Vec::new(),
                next_seq: 1,
                first_available_seq: 1,
                charge: None,
                log_id: None,
            });
        };
        let selected = || log.records.iter().filter(|record| record.seq > after_seq);
        let count = selected().count();
        let bytes =
            count * size_of::<LogRecord>() + selected().map(LogRecord::text_bytes).sum::<usize>();
        let charge = self
            .shared
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
            log_id: Some(log.log_id),
        })
    }

    /// Drop a plugin's records and release their charge (package unload, or
    /// a failed first load, which leaves no live generation to log for).
    pub(crate) fn remove(&self, plugin: &str) {
        self.shared.lock().logs.remove(plugin);
    }

    /// Wait until the mirror has written every record it can reach and is
    /// parked in its wait, holding no copy: from then on only an append or
    /// shutdown can wake it. Only for a book with a mirror thread.
    #[cfg(test)]
    pub(crate) fn wait_until_mirrored(&self) {
        // timer: deadline — a hang guard for a mirror that never catches up.
        let deadline = std::time::Instant::now() + crate::daemon::owner_loop::TEST_HANG_GUARD;
        let mut state = self.shared.lock();
        while !state.mirror_parked
            || state
                .logs
                .values()
                .any(|log| log.mirror_next < log.next_seq)
        {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .expect("the Hub log mirror must catch up before the hang guard");
            state = self
                .shared
                .mirror_idle
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

impl Drop for PluginLogBook {
    /// The mirror finishes the records it can still reach, then exits.
    fn drop(&mut self) {
        self.shared.lock().closed = true;
        self.shared.mirror_wake.notify_all();
    }
}

/// The mirror thread: write each accepted record once, in order per plugin,
/// with no lock held while the sink runs. It waits only on the wake that
/// appends and shutdown send.
fn run_mirror(shared: &Shared, mut sink: Box<dyn HubLogSink>) {
    let mut state = shared.lock();
    loop {
        let Some(line) = next_mirror_line(&shared.memory, &mut state) else {
            if state.closed {
                return;
            }
            #[cfg(test)]
            {
                state.mirror_parked = true;
                shared.mirror_idle.notify_all();
            }
            state = shared
                .mirror_wake
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
            #[cfg(test)]
            {
                state.mirror_parked = false;
            }
            continue;
        };
        drop(state);
        sink.write(&line.plugin, &line.record, line.unmirrored_before);
        drop(line);
        state = shared.lock();
    }
}

/// Copy the next unwritten record, taking plugins in turn. Records the ring
/// evicted, or that the account cannot fund a copy of, are skipped and
/// counted.
fn next_mirror_line(memory: &Arc<LuaMemoryAccount>, state: &mut BookState) -> Option<MirrorLine> {
    let count = state.logs.len();
    for offset in 0..count {
        let index = (state.mirror_turn + offset) % count;
        let (plugin, log) = state.logs.iter_mut().nth(index).expect("index is in range");
        if log.mirror_next >= log.next_seq {
            continue;
        }
        let at = log
            .records
            .partition_point(|record| record.seq < log.mirror_next);
        let Some(record) = log.records.get(at) else {
            log.unmirrored += log.next_seq - log.mirror_next;
            log.mirror_next = log.next_seq;
            continue;
        };
        let bytes = size_of::<MirrorLine>() + plugin.len() + record.text_bytes();
        let Ok(charge) = memory.reserve_callback_total(bytes) else {
            // The account is full now: count every pending record rather
            // than leave some waiting for an append that may never come.
            log.unmirrored += log.next_seq - log.mirror_next;
            log.mirror_next = log.next_seq;
            continue;
        };
        let skipped = record.seq - log.mirror_next;
        log.mirror_next = record.seq + 1;
        let line = MirrorLine {
            plugin: plugin.clone(),
            record: record.clone(),
            unmirrored_before: std::mem::take(&mut log.unmirrored) + skipped,
            _charge: charge,
        };
        state.mirror_turn = index + 1;
        return Some(line);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::owner_loop::TEST_HANG_GUARD;
    use crate::lua_memory::LuaMemoryLimits;
    use std::sync::mpsc;

    fn memory() -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1024 * 1024,
            total_vm_bytes: 1024 * 1024,
            per_callback_bytes: 1024 * 1024,
            total_callback_bytes: 4 * 1024 * 1024,
        })
        .unwrap()
    }

    /// A book without a mirror thread, for tests of the ring alone.
    fn book() -> (PluginLogBook, Arc<LuaMemoryAccount>) {
        let memory = memory();
        (PluginLogBook::unmirrored(Arc::clone(&memory)), memory)
    }

    fn append(book: &PluginLogBook, message: &str, now_ms: u64) -> AppendOutcome {
        book.append("p", 7, LogLevel::Info, message, None, now_ms, 0)
    }

    #[derive(Debug, PartialEq, Eq)]
    enum SinkEvent {
        Line {
            plugin: String,
            seq: u64,
            unmirrored_before: u64,
        },
        /// The mirror thread returned and dropped its sink.
        Closed,
    }

    /// Reports every write; with `release`, each write then blocks until the
    /// test sends on, or drops, the release channel.
    struct TestSink {
        events: mpsc::Sender<SinkEvent>,
        release: Option<mpsc::Receiver<()>>,
    }

    impl HubLogSink for TestSink {
        fn write(&mut self, plugin: &str, record: &LogRecord, unmirrored_before: u64) {
            let _ = self.events.send(SinkEvent::Line {
                plugin: plugin.to_string(),
                seq: record.seq,
                unmirrored_before,
            });
            if let Some(release) = &self.release {
                let _ = release.recv();
            }
        }
    }

    impl Drop for TestSink {
        fn drop(&mut self) {
            let _ = self.events.send(SinkEvent::Closed);
        }
    }

    fn mirrored_book(
        release: Option<mpsc::Receiver<()>>,
    ) -> (PluginLogBook, mpsc::Receiver<SinkEvent>) {
        let (events, received) = mpsc::channel();
        let book = PluginLogBook::with_sink(memory(), Box::new(TestSink { events, release }));
        (book, received)
    }

    fn next_event(events: &mpsc::Receiver<SinkEvent>, expected: &str) -> SinkEvent {
        // timer: deadline — a hang guard for a mirror that never writes.
        events.recv_timeout(TEST_HANG_GUARD).expect(expected)
    }

    fn line(plugin: &str, seq: u64, unmirrored_before: u64) -> SinkEvent {
        SinkEvent::Line {
            plugin: plugin.to_string(),
            seq,
            unmirrored_before,
        }
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
    fn records_from_every_generation_stay_in_one_chronological_ring() {
        let (book, _memory) = book();
        let mut now = 0;
        for _ in 0..RING_RECORDS {
            now += 10;
            book.append("p", 1, LogLevel::Info, "live", None, now, 0);
        }
        for _ in 0..3 {
            now += 10;
            book.append("p", 2, LogLevel::Error, "candidate", None, now, 0);
        }
        let page = book.read("p", 0).unwrap();
        assert_eq!(page.records.len(), RING_RECORDS);
        assert_eq!(page.first_available_seq, 4, "the three oldest were evicted");
        let (live, candidate): (Vec<_>, Vec<_>) = page
            .records
            .iter()
            .partition(|record| record.generation == 1);
        assert_eq!(live.len(), RING_RECORDS - 3);
        assert_eq!(candidate.len(), 3);
        assert!(candidate.iter().all(|record| record.message == "candidate"));
        assert!(
            page.records
                .windows(2)
                .all(|pair| pair[0].seq + 1 == pair[1].seq),
            "one sequence across generations"
        );
    }

    #[test]
    fn accepted_records_are_written_to_the_hub_log() {
        let (book, events) = mirrored_book(None);
        // Parked first: only the append's wake can bring the record out.
        book.wait_until_mirrored();
        append(&book, "mirrored", 0);
        assert_eq!(
            next_event(&events, "the accepted record reaches the Hub log"),
            line("p", 1, 0)
        );
    }

    #[test]
    fn a_blocked_hub_log_sink_never_delays_appends_or_reads() {
        let (release, blocked) = mpsc::channel();
        let (book, events) = mirrored_book(Some(blocked));
        let book = Arc::new(book);
        book.append("a", 1, LogLevel::Info, "first", None, 0, 0);
        // The mirror is now inside the sink, blocked until release.
        assert_eq!(
            next_event(&events, "the mirror enters the sink"),
            line("a", 1, 0)
        );

        let appends = RING_RECORDS as u64 + 10;
        let writer = Arc::clone(&book);
        let (done_tx, done) = mpsc::channel();
        std::thread::spawn(move || {
            // 10 ms of refill per record keeps the rate limit out of the way.
            for index in 1..=appends {
                let outcome = writer.append("a", 1, LogLevel::Info, "more", None, index * 10, 0);
                assert_eq!(outcome, AppendOutcome::Accepted { seq: index + 1 });
            }
            let other = writer.append("b", 1, LogLevel::Info, "other", None, 0, 0);
            assert_eq!(other, AppendOutcome::Accepted { seq: 1 });
            let page = writer.read("a", 0).expect("a read never waits on the sink");
            let _ = done_tx.send(page.records.len());
        });
        // timer: deadline — a hang guard for appends stuck behind the sink.
        let read = done
            .recv_timeout(TEST_HANG_GUARD)
            .expect("appends and reads must finish while the Hub log sink is blocked");
        assert_eq!(read, RING_RECORDS);

        drop(release);
        let last = appends + 1;
        let first_kept = last - RING_RECORDS as u64 + 1;
        let mut a_lines = Vec::new();
        let mut b_lines = Vec::new();
        while a_lines.len() < RING_RECORDS || b_lines.is_empty() {
            match next_event(&events, "the mirror catches up after release") {
                SinkEvent::Line {
                    plugin,
                    seq,
                    unmirrored_before,
                } if plugin == "a" => a_lines.push((seq, unmirrored_before)),
                SinkEvent::Line {
                    seq,
                    unmirrored_before,
                    ..
                } => b_lines.push((seq, unmirrored_before)),
                SinkEvent::Closed => panic!("the mirror closed early"),
            }
        }
        // Records 2 .. first_kept left the ring before the mirror reached
        // them; the first kept record reports them.
        assert_eq!(a_lines[0], (first_kept, first_kept - 2));
        assert!(
            a_lines[1..]
                .iter()
                .zip(first_kept + 1..)
                .all(|(line, seq)| *line == (seq, 0))
        );
        assert_eq!(b_lines, vec![(1, 0)]);
    }

    #[test]
    fn dropping_the_book_wakes_and_stops_an_idle_mirror() {
        let (book, events) = mirrored_book(None);
        append(&book, "before shutdown", 0);
        assert_eq!(
            next_event(&events, "the record is mirrored"),
            line("p", 1, 0)
        );
        book.wait_until_mirrored();
        drop(book);
        assert_eq!(
            next_event(
                &events,
                "shutdown must wake the idle mirror, which then exits"
            ),
            SinkEvent::Closed
        );
    }

    #[test]
    fn the_rate_limit_refuses_a_burst_and_reports_the_drop() {
        let (book, _memory) = book();
        for _ in 0..BURST {
            assert!(matches!(
                append(&book, "x", 0),
                AppendOutcome::Accepted { .. }
            ));
        }
        assert_eq!(
            append(&book, "x", 0),
            AppendOutcome::RateLimited { dropped: 1 }
        );
        // 10 ms later one record's worth of tokens (100/s) has refilled.
        assert!(matches!(
            append(&book, "late", 10),
            AppendOutcome::Accepted { .. }
        ));
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
        assert!(
            page.first_available_seq > 1,
            "the oldest records were evicted"
        );
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
    fn a_recreated_log_gets_a_new_id_and_a_continuing_log_keeps_it() {
        let (book, _memory) = book();
        assert_eq!(book.read("p", 0).unwrap().log_id, None, "no log, no id");
        append(&book, "first", 0);
        let first = book.read("p", 0).unwrap().log_id.expect("a log has an id");
        append(&book, "second", 0);
        assert_eq!(
            book.read("p", 0).unwrap().log_id,
            Some(first),
            "the same log"
        );
        book.remove("p");
        append(&book, "again", 0);
        let second = book.read("p", 0).unwrap().log_id.expect("a log has an id");
        assert_ne!(second, first, "a recreated log is a new incarnation");
        let (other, _memory) = super::tests::book();
        other.append("p", 7, LogLevel::Info, "elsewhere", None, 0, 0);
        let third = other.read("p", 0).unwrap().log_id.unwrap();
        assert!(
            third != first && third != second,
            "ids never repeat in a process"
        );
    }

    #[test]
    fn the_reply_carries_the_log_id_and_funds_its_text() {
        let (book, memory) = book();
        append(&book, "one", 0);
        let page = book.read("p", 0).unwrap();
        let id = page.log_id.expect("a log has an id");
        let with_page = memory.usage().1;
        let (response, charge) =
            crate::client_api_dto::response::daemon_plugin_logs("p".to_string(), page)
                .expect("the account funds the reply");
        let logs = response.plugin_logs.as_ref().expect("a plugin_logs page");
        let text = logs.log_id.as_ref().expect("the reply carries the id");
        assert_eq!(text, &id.to_string());
        // The reply grew the page's charge by its record vector, its level
        // strings, and the id text's whole allocation: its capacity, not
        // just its length.
        let reply = size_of::<botster_hub_client::DaemonPluginLogRecord>() + "info".len();
        assert_eq!(memory.usage().1, with_page + reply + text.capacity());
        assert_eq!(text.capacity(), id.text_len(), "the id string never grew");
        drop((response, charge));
    }

    #[test]
    fn a_log_id_text_len_is_its_formatted_length() {
        for serial in [1, 9, 10, 99, 100, 12_345, u64::MAX] {
            let id = LogId {
                boot: u128::MAX,
                serial,
            };
            assert_eq!(id.text_len(), id.to_string().len(), "serial {serial}");
        }
    }

    #[test]
    fn each_process_draws_a_random_boot_id() {
        // A restarted Hub draws again, so its log ids cannot repeat the
        // previous process's.
        assert_ne!(random_boot_id(), random_boot_id());
    }
}
