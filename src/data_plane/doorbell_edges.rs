//! Coalescing per-session edges for the doorbell.
//!
//! Core's pump runs on the data-plane thread and reports which sessions saw
//! client input, output, or a mode change. Only that thread can read
//! [`SessionEdges`] (the Core daemon is not `Send`), so after each pump it
//! stores the LATEST edges of every session the owner watches here, and raises
//! one owner signal. The owner drains the map. The map is a latest-wins table,
//! never a queue: its size is bounded by the number of sessions the owner
//! watches or has seen type, and the owner forgets a session when it ends. The
//! thread makes no policy decision.
//!
//! The owner also needs the time of each session's last client input, for every
//! session and not only watched ones, or it could not know the quiet period at
//! the moment a ring arrives. Core reports input as a counter, so the thread
//! stamps the time it saw the advance.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use botster_core::SessionId;
use botster_core_daemon::{CoreDaemon, PumpWokenOutcome, SessionEdges};

use crate::daemon::owner_signal::{OwnerSignal, SignalKey};

#[derive(Debug, Default)]
struct Table {
    /// The last time the client's input advanced, for each session with input.
    input_at: BTreeMap<String, Instant>,
    /// Sessions the owner has a ring or an attempt for.
    watched: BTreeSet<String>,
    /// The latest edges of a watched session, not yet drained by the owner.
    latest: BTreeMap<String, SessionEdges>,
}

struct Shared {
    signal: Arc<OwnerSignal>,
    table: Mutex<Table>,
}

/// The shared handle: the data-plane thread writes, the owner drains.
#[derive(Clone)]
pub(crate) struct DoorbellEdges {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for DoorbellEdges {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DoorbellEdges")
    }
}

impl DoorbellEdges {
    pub(crate) fn new(signal: Arc<OwnerSignal>) -> Self {
        Self {
            shared: Arc::new(Shared {
                signal,
                table: Mutex::new(Table::default()),
            }),
        }
    }

    /// A poisoned table holds only stamps and latest-wins edges; keep going.
    fn table(&self) -> MutexGuard<'_, Table> {
        self.shared
            .table
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The pump reported these lists. Stamp the input time of every session
    /// whose input advanced, and store the latest edges of every WATCHED
    /// session named in any list. Raise the owner signal once if anything was
    /// stored. `read` is Core's `session_edges`.
    pub(crate) fn record_pump(
        &self,
        outcome: &PumpWokenOutcome,
        now: Instant,
        mut read: impl FnMut(&SessionId) -> Option<SessionEdges>,
    ) {
        // The lock is held only for in-memory map work. Reading `session_edges`
        // loads the registry and may be slow, so it runs with no lock held and
        // the owner never waits behind it.
        let to_read: Vec<SessionId> = {
            let mut table = self.table();
            for session in &outcome.input_advanced {
                table.input_at.insert(session.0.clone(), now);
            }
            let mut seen = BTreeSet::new();
            outcome
                .modes_advanced
                .iter()
                .chain(&outcome.output_advanced)
                .chain(&outcome.input_advanced)
                .filter(|session| {
                    table.watched.contains(&session.0) && seen.insert(session.0.clone())
                })
                .cloned()
                .collect()
        };
        let reads: Vec<(String, SessionEdges)> = to_read
            .iter()
            .filter_map(|session| read(session).map(|edges| (session.0.clone(), edges)))
            .collect();
        let mut stored = false;
        {
            let mut table = self.table();
            for (session, edges) in reads {
                // Unwatched or forgotten while the read ran: store nothing.
                if table.watched.contains(&session) {
                    table.latest.insert(session, edges);
                    stored = true;
                }
            }
        }
        if stored {
            // After the write: the owner drains the map, then re-checks.
            self.shared.signal.raise(SignalKey::Doorbell);
        }
    }

    /// Read `session_edges` from the Core daemon for [`Self::record_pump`].
    pub(crate) fn record_pump_from(
        &self,
        daemon: &CoreDaemon,
        outcome: &PumpWokenOutcome,
        now: Instant,
    ) {
        self.record_pump(outcome, now, |session| {
            daemon.session_edges(session).ok().flatten()
        });
    }

    /// The owner has a ring or an attempt for the session.
    pub(crate) fn watch(&self, session: &str) {
        self.table().watched.insert(session.to_string());
    }

    /// The owner has nothing left for the session.
    pub(crate) fn unwatch(&self, session: &str) {
        let mut table = self.table();
        table.watched.remove(session);
        table.latest.remove(session);
    }

    /// The session ended: forget everything about it.
    pub(crate) fn forget(&self, session: &str) {
        let mut table = self.table();
        table.watched.remove(session);
        table.latest.remove(session);
        table.input_at.remove(session);
    }

    /// Every session with a stamped input time, so the owner can forget the
    /// ones that ended whether or not they were ever rung.
    pub(crate) fn stamped_sessions(&self) -> Vec<String> {
        self.table().input_at.keys().cloned().collect()
    }

    /// When the session's client input last advanced, if it ever did.
    pub(crate) fn input_at(&self, session: &str) -> Option<Instant> {
        self.table().input_at.get(session).copied()
    }

    /// Whether edges wait to be drained.
    pub(crate) fn has_latest(&self) -> bool {
        !self.table().latest.is_empty()
    }

    /// Drain the latest edges, sorted by session id.
    pub(crate) fn take_latest(&self) -> Vec<(String, SessionEdges)> {
        std::mem::take(&mut self.table().latest)
            .into_iter()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn sizes(&self) -> (usize, usize, usize) {
        let table = self.table();
        (
            table.input_at.len(),
            table.watched.len(),
            table.latest.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn edges(input_seq: u64, output_seq: u64) -> SessionEdges {
        SessionEdges {
            modes_epoch: 0,
            mode_flags: None,
            output_seq,
            input_seq,
            composing: false,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
        }
    }

    fn session(name: &str) -> SessionId {
        SessionId(name.to_string())
    }

    fn outcome(modes: &[&str], output: &[&str], input: &[&str]) -> PumpWokenOutcome {
        let ids = |names: &[&str]| names.iter().map(|name| session(name)).collect();
        PumpWokenOutcome {
            pumped_routes: 0,
            terminal_inventory_changed: false,
            journal_advanced: false,
            modes_advanced: ids(modes),
            output_advanced: ids(output),
            input_advanced: ids(input),
        }
    }

    fn rig() -> (DoorbellEdges, Arc<OwnerSignal>) {
        let signal = Arc::new(OwnerSignal::default());
        (DoorbellEdges::new(Arc::clone(&signal)), signal)
    }

    #[test]
    fn only_watched_sessions_are_read_and_stored() {
        let (map, _) = rig();
        map.watch("a");
        let mut reads = Vec::new();
        map.record_pump(&outcome(&[], &["a", "b"], &[]), Instant::now(), |id| {
            reads.push(id.0.clone());
            Some(edges(0, 1))
        });
        assert_eq!(reads, ["a"], "an unwatched session is never read");
        assert_eq!(map.take_latest().len(), 1);
    }

    #[test]
    fn the_edges_are_read_with_no_lock_held() {
        let (map, _) = rig();
        map.watch("a");
        map.record_pump(&outcome(&[], &["a"], &[]), Instant::now(), |_| {
            assert!(
                map.shared.table.try_lock().is_ok(),
                "the owner must not wait behind a registry read"
            );
            Some(edges(0, 1))
        });
        assert_eq!(map.take_latest().len(), 1);
    }

    #[test]
    fn a_session_unwatched_while_its_read_runs_stores_nothing() {
        let (map, _) = rig();
        map.watch("a");
        map.record_pump(&outcome(&[], &["a"], &[]), Instant::now(), |_| {
            map.unwatch("a");
            Some(edges(0, 1))
        });
        assert!(!map.has_latest());
    }

    #[test]
    fn every_stamped_session_can_be_listed_for_forgetting() {
        let (map, _) = rig();
        map.record_pump(&outcome(&[], &[], &["a", "b"]), Instant::now(), |_| None);
        assert_eq!(map.stamped_sessions(), ["a", "b"]);
    }

    #[test]
    fn a_session_named_in_several_lists_is_read_once() {
        let (map, _) = rig();
        map.watch("a");
        let mut reads = 0;
        map.record_pump(&outcome(&["a"], &["a"], &["a"]), Instant::now(), |_| {
            reads += 1;
            Some(edges(1, 1))
        });
        assert_eq!(reads, 1);
    }

    #[test]
    fn the_latest_edges_win_and_the_owner_is_signalled_after_the_write() {
        let (map, signal) = rig();
        map.watch("a");
        let seen = signal.seen(SignalKey::Doorbell);
        map.record_pump(&outcome(&[], &["a"], &[]), Instant::now(), |_| {
            Some(edges(0, 1))
        });
        assert!(signal.moved(seen), "the signal is raised");
        map.record_pump(&outcome(&[], &["a"], &[]), Instant::now(), |_| {
            Some(edges(0, 2))
        });
        let drained = map.take_latest();
        assert_eq!(drained.len(), 1, "coalesced into one entry");
        assert_eq!(drained[0].1.output_seq, 2, "the latest edges win");
        assert!(!map.has_latest());
    }

    #[test]
    fn nothing_stored_raises_no_signal() {
        let (map, signal) = rig();
        let seen = signal.seen(SignalKey::Doorbell);
        map.record_pump(&outcome(&[], &["a"], &[]), Instant::now(), |_| {
            Some(edges(0, 1))
        });
        assert!(!signal.moved(seen), "an unwatched session stores nothing");
        map.watch("a");
        map.record_pump(&outcome(&[], &["a"], &[]), Instant::now(), |_| None);
        assert!(!signal.moved(seen), "a failed read stores nothing");
    }

    #[test]
    fn input_time_is_stamped_for_every_session_even_unwatched_ones() {
        let (map, _) = rig();
        let at = Instant::now();
        map.record_pump(&outcome(&[], &[], &["a"]), at, |_| panic!("not watched"));
        assert_eq!(map.input_at("a"), Some(at));
        let later = at + Duration::from_secs(3);
        map.record_pump(&outcome(&[], &[], &["a"]), later, |_| panic!("not watched"));
        assert_eq!(map.input_at("a"), Some(later));
        assert_eq!(map.input_at("b"), None);
    }

    #[test]
    fn forgetting_a_session_prunes_all_three_tables() {
        let (map, _) = rig();
        map.watch("a");
        map.record_pump(&outcome(&[], &["a"], &["a"]), Instant::now(), |_| {
            Some(edges(1, 1))
        });
        assert_eq!(map.sizes(), (1, 1, 1));
        map.forget("a");
        assert_eq!(map.sizes(), (0, 0, 0));
    }

    #[test]
    fn unwatching_keeps_the_input_time_and_drops_undrained_edges() {
        let (map, _) = rig();
        map.watch("a");
        map.record_pump(&outcome(&[], &["a"], &["a"]), Instant::now(), |_| {
            Some(edges(1, 1))
        });
        map.unwatch("a");
        assert_eq!(map.sizes(), (1, 0, 0));
    }
}
