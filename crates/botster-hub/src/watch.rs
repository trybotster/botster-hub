//! Watches of the session view (design section 4): the watch table, the baseline queue and its counted continuation.
//!
//! The owner holds the [`Watches`] and drives them; the fan-out of events lives in the owner, because it advances the
//! session view one event at a time.

use crate::bridge::{DeltaPush, Outbox};
use botster_core::contract::prelude::{Exit, SessionId, SessionRecord, SessionState, Size};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

/// HC section 6 `activity` (steward R-125): `active` when the session had output within `activity_idle_ms`. Core's TM-4
/// silence edge is the timer (design section 4, "The view row").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activity {
    Active,
    Idle,
}

/// How a session's spawn operation ended without the session running (design section 7, item 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpawnEnd {
    Failed { reason: String },
    Cancelled,
}

/// One row of the session view. Rows are immutable and shared: a cut copies one pointer per row. Every field has one
/// source (design section 4, "The view row"); PC's layer builds the HC `session` row from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: SessionId,
    pub state: SessionState,
    pub size: Size,
    pub exit: Option<Exit>,
    /// Core's session labels; the Hub's fields are under `h/` (R-125).
    pub labels: BTreeMap<String, String>,
    pub activity: Activity,
    /// How the session's spawn operation ended without the session running (R-122; design section 7, item 4). The owner
    /// sets it where it writes the terminal record; the protocol layer reads it for `failure_reason`.
    pub spawn_end: Option<SpawnEnd>,
}

impl Row {
    /// A row from Core's record. The activity starts `idle`.
    pub fn from_record(record: SessionRecord) -> Row {
        Row {
            id: record.id,
            state: record.state,
            size: record.size,
            exit: record.exit,
            labels: record.labels,
            activity: Activity::Idle,
            spawn_end: None,
        }
    }
}

/// A frame of a watch (HC WT-1, WT-4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchFrame {
    /// Rows of the baseline's cut.
    SnapshotChunk(Vec<Arc<Row>>),
    /// The end of the baseline; deltas follow.
    SnapshotEnd,
    /// The watch lost deltas (its queue overflowed): a new baseline follows.
    Resync,
    /// A row changed.
    Delta(Arc<Row>),
    /// A row was released.
    Removed(SessionId),
}

/// Which rows a watch sees: every row, or one session.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WatchFilter {
    pub session: Option<SessionId>,
}

impl WatchFilter {
    pub fn matches(&self, id: &SessionId) -> bool {
        self.session.as_ref().is_none_or(|session| session == id)
    }
}

/// A watch's key: the connection and the watch id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WatchKey {
    pub conn: u64,
    pub watch: u64,
}

/// The baseline state of a watch.
#[derive(Debug)]
enum Baseline {
    /// Waits to start; `resync` sends a `Resync` frame first.
    Waiting { resync: bool },
    /// In progress: the cut and the next row.
    InProgress { cut: Vec<Arc<Row>>, cursor: usize },
    /// Done: deltas flow.
    Live,
}

#[derive(Debug)]
struct Watch {
    filter: WatchFilter,
    outbox: Arc<Outbox>,
    baseline: Baseline,
    /// The outbox had no frame room; the capacity wake clears it.
    blocked: bool,
    /// The watch is in the baseline queue.
    queued: bool,
}

/// The limits of the watch table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchLimits {
    /// Per connection (HC BD-1).
    pub max_watches: usize,
    /// Rows per `snapshot_chunk`.
    pub chunk_rows: usize,
}

/// Why a watch was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchRefused {
    TooManyWatches,
}

/// The watch table and the baseline queue (round-robin). Bound: `max_connections × max_watches` watches, one baseline
/// entry per watch.
#[derive(Debug)]
pub struct Watches {
    limits: WatchLimits,
    map: BTreeMap<WatchKey, Watch>,
    per_conn: BTreeMap<u64, usize>,
    queue: VecDeque<WatchKey>,
}

impl Watches {
    pub fn new(limits: WatchLimits) -> Watches {
        Watches {
            limits,
            map: BTreeMap::new(),
            per_conn: BTreeMap::new(),
            queue: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Adds a watch; its baseline waits in the queue (WT-1). A watch id that exists is replaced.
    ///
    /// # Errors
    /// [`WatchRefused::TooManyWatches`] at the per-connection bound (HC BD-1).
    pub fn add(
        &mut self,
        key: WatchKey,
        filter: WatchFilter,
        outbox: Arc<Outbox>,
    ) -> Result<(), WatchRefused> {
        if !self.map.contains_key(&key) {
            let count = self.per_conn.get(&key.conn).copied().unwrap_or(0);
            if count >= self.limits.max_watches {
                return Err(WatchRefused::TooManyWatches);
            }
            self.per_conn.insert(key.conn, count + 1);
        } else {
            self.queue.retain(|k| *k != key);
        }
        outbox.open_watch(key.watch);
        self.map.insert(
            key,
            Watch {
                filter,
                outbox,
                baseline: Baseline::Waiting { resync: false },
                blocked: false,
                queued: true,
            },
        );
        self.queue.push_back(key);
        Ok(())
    }

    /// Removes a watch with its baseline entry and its queue, in one step (`unwatch`; HC WT-6). Returns whether it existed.
    pub fn remove(&mut self, key: WatchKey) -> bool {
        let Some(watch) = self.map.remove(&key) else {
            return false;
        };
        watch.outbox.remove_watch(key.watch);
        if watch.queued {
            self.queue.retain(|k| *k != key);
        }
        if let Some(count) = self.per_conn.get_mut(&key.conn) {
            *count -= 1;
            if *count == 0 {
                self.per_conn.remove(&key.conn);
            }
        }
        true
    }

    /// Removes every watch of a connection whose outbox is closed (HC HS-9 disconnect cleanup).
    pub fn remove_closed(&mut self) {
        let closed: Vec<WatchKey> = self
            .map
            .iter()
            .filter(|(_, w)| w.outbox.is_closed())
            .map(|(k, _)| *k)
            .collect();
        for key in closed {
            self.remove(key);
        }
    }

    /// The first watch after `after` (all watches when `None`), in key order, for the fan-out continuation.
    pub fn next_after(&self, after: Option<WatchKey>) -> Option<WatchKey> {
        match after {
            None => self.map.keys().next().copied(),
            Some(key) => self
                .map
                .range((std::ops::Bound::Excluded(key), std::ops::Bound::Unbounded))
                .next()
                .map(|(k, _)| *k),
        }
    }

    /// Delivers one delta to one watch, if its filter matches. A delta for a watch whose baseline is in progress waits in
    /// the watch's queue behind the baseline. An overflow drops the queue and any baseline in progress, and puts the watch
    /// in the baseline queue again (a flag and a place; it cannot fail).
    pub fn deliver(&mut self, key: WatchKey, id: &SessionId, frame: &WatchFrame) {
        let Some(watch) = self.map.get_mut(&key) else {
            return;
        };
        if !watch.filter.matches(id) {
            return;
        }
        // A watch whose baseline has not started gets no delta: its cut is taken later, from a later view.
        if matches!(watch.baseline, Baseline::Waiting { .. }) {
            return;
        }
        if watch.outbox.push_delta(key.watch, id, frame.clone()) == DeltaPush::Overflow {
            watch.baseline = Baseline::Waiting { resync: true };
            watch.blocked = false;
            if !watch.queued {
                watch.queued = true;
                self.queue.push_back(key);
            }
        }
    }

    /// Whether a baseline can make progress now: it waits or has rows left, and its outbox is not known to be full (section
    /// 2, step 5). A blocked baseline whose outbox has room again (the capacity wake cleared `wants_room`) is runnable.
    pub fn runnable(&self) -> bool {
        self.queue.iter().any(|key| {
            self.map
                .get(key)
                .is_some_and(|w| !w.blocked || !w.outbox.wants_room())
        })
    }

    /// One turn of baselines (section 4): at most `rows` rows across all baselines, round-robin, in `snapshot_chunk`s. A
    /// waiting baseline takes its cut from `view` now (the caller runs this only between fan-out batches, so every later
    /// event is later than the cut). Returns the rows sent.
    pub fn baseline_turn(&mut self, rows: usize, view: &BTreeMap<SessionId, Arc<Row>>) -> usize {
        let mut left = rows.max(1);
        let mut sent = 0;
        let mut visits = self.queue.len();
        while left > 0 && visits > 0 {
            visits -= 1;
            let Some(key) = self.queue.pop_front() else {
                break;
            };
            let Some(watch) = self.map.get_mut(&key) else {
                continue;
            };
            if watch.blocked {
                if watch.outbox.wants_room() {
                    self.queue.push_back(key);
                    continue;
                }
                watch.blocked = false;
            }
            let chunk_rows = self.limits.chunk_rows.max(1);
            let done = loop {
                match &mut watch.baseline {
                    Baseline::Live => break true,
                    Baseline::Waiting { resync } => {
                        if *resync && !watch.outbox.push_frame(key.watch, WatchFrame::Resync) {
                            watch.blocked = true;
                            break false;
                        }
                        watch.outbox.reset_watch(key.watch);
                        let cut = view
                            .values()
                            .filter(|row| watch.filter.matches(&row.id))
                            .cloned()
                            .collect();
                        watch.baseline = Baseline::InProgress { cut, cursor: 0 };
                    }
                    Baseline::InProgress { cut, cursor } => {
                        if left == 0 {
                            break false;
                        }
                        if *cursor >= cut.len() {
                            if !watch.outbox.push_frame(key.watch, WatchFrame::SnapshotEnd) {
                                watch.blocked = true;
                                break false;
                            }
                            watch.outbox.set_live(key.watch);
                            watch.baseline = Baseline::Live;
                            left -= 1;
                            break true;
                        }
                        let n = chunk_rows.min(left).min(cut.len() - *cursor);
                        let rows_now = cut[*cursor..*cursor + n].to_vec();
                        if !watch
                            .outbox
                            .push_frame(key.watch, WatchFrame::SnapshotChunk(rows_now))
                        {
                            watch.blocked = true;
                            break false;
                        }
                        *cursor += n;
                        left -= n;
                        sent += n;
                    }
                }
            };
            if done {
                watch.queued = false;
            } else {
                self.queue.push_back(key);
            }
        }
        sent
    }
}
