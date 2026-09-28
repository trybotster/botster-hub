//! The Hub's one clock.
//!
//! Every Hub reader of time goes through a `HubClock`: `botster.clock.now`
//! and `botster.clock.monotonic`, the plugin log rate limit, and the arming
//! of plugin timers. A production Hub reads the operating system's clocks. A
//! test Hub may switch its clock to a logical one, which stands still until
//! the test advances it, so tests never wait on wall time.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// A cloneable handle to one shared clock. All clones read and move the
/// same time.
#[derive(Clone, Debug)]
pub struct HubClock {
    cell: Arc<ClockCell>,
}

#[derive(Debug)]
struct ClockCell {
    logical: AtomicBool,
    wall_ms: AtomicU64,
    monotonic_ms: AtomicU64,
}

impl HubClock {
    /// A clock that reads the operating system's clocks.
    #[must_use]
    pub fn system() -> Self {
        started();
        Self {
            cell: Arc::new(ClockCell {
                logical: AtomicBool::new(false),
                wall_ms: AtomicU64::new(0),
                monotonic_ms: AtomicU64::new(0),
            }),
        }
    }

    /// A logical clock that stands still at the given times until
    /// `advance` moves it.
    #[must_use]
    pub fn logical(wall_ms: u64, monotonic_ms: u64) -> Self {
        let clock = Self::system();
        clock.make_logical(wall_ms, monotonic_ms);
        clock
    }

    /// Switch this clock, and every clone of it, to logical time.
    ///
    /// A Hub decides its clock when it starts, before it loads any plugin.
    /// Production code never calls this.
    pub fn make_logical(&self, wall_ms: u64, monotonic_ms: u64) {
        self.cell.wall_ms.store(wall_ms, Ordering::SeqCst);
        self.cell.monotonic_ms.store(monotonic_ms, Ordering::SeqCst);
        self.cell.logical.store(true, Ordering::SeqCst);
    }

    /// Whether this clock is logical.
    #[must_use]
    pub fn is_logical(&self) -> bool {
        self.cell.logical.load(Ordering::SeqCst)
    }

    /// Whether both handles share one clock.
    #[must_use]
    pub fn is_same_clock(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cell, &other.cell)
    }

    /// Move a logical clock forward and return the new monotonic time.
    /// Returns `None`, and moves nothing, on a system clock.
    pub fn advance(&self, ms: u64) -> Option<u64> {
        if !self.is_logical() {
            return None;
        }
        self.cell.wall_ms.fetch_add(ms, Ordering::SeqCst);
        Some(self.cell.monotonic_ms.fetch_add(ms, Ordering::SeqCst) + ms)
    }

    /// Unix time in milliseconds.
    #[must_use]
    pub fn wall_ms(&self) -> u64 {
        if self.is_logical() {
            return self.cell.wall_ms.load(Ordering::SeqCst);
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }

    /// Monotonic milliseconds since this process first read the clock.
    #[must_use]
    pub fn monotonic_ms(&self) -> u64 {
        if self.is_logical() {
            return self.cell.monotonic_ms.load(Ordering::SeqCst);
        }
        u64::try_from(started().elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

impl Default for HubClock {
    fn default() -> Self {
        Self::system()
    }
}

fn started() -> Instant {
    static STARTED: OnceLock<Instant> = OnceLock::new();
    *STARTED.get_or_init(Instant::now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_clock_is_the_system_clock_and_refuses_to_advance() {
        let clock = HubClock::system();
        assert!(!clock.is_logical());
        assert_eq!(clock.advance(5), None);
        assert!(clock.wall_ms() > 1_600_000_000_000);
    }

    #[test]
    fn a_logical_clock_stands_still_until_advanced_and_clones_share_it() {
        let clock = HubClock::logical(1_000, 50);
        let clone = clock.clone();
        assert!(clock.is_same_clock(&clone));
        assert_eq!((clock.wall_ms(), clock.monotonic_ms()), (1_000, 50));
        assert_eq!(clone.advance(7), Some(57));
        assert_eq!((clock.wall_ms(), clock.monotonic_ms()), (1_007, 57));
    }

    #[test]
    fn switching_one_handle_to_logical_moves_every_clone() {
        let clock = HubClock::system();
        let clone = clock.clone();
        clock.make_logical(0, 0);
        assert!(clone.is_logical());
        assert_eq!(clone.monotonic_ms(), 0);
    }
}
