#![allow(dead_code)]
/// Bounded close-slice progress. `after_route` is a resume cursor, not a route record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClosedEventSliceProgress {
    pub classified: usize,
    pub more: bool,
    pub after_route: Option<(String, String, u64)>,
}

pub(crate) fn empty_close_event_progress() -> ClosedEventSliceProgress {
    ClosedEventSliceProgress {
        classified: 0,
        more: false,
        after_route: None,
    }
}

/// Transport-neutral counters for one bounded close-slice walk.
pub(crate) struct CloseSliceAccumulator {
    max_candidates: usize,
    max_entries_visited: usize,
    classified: usize,
    visited: usize,
    more: bool,
    last_visited: Option<(String, String, u64)>,
}

impl CloseSliceAccumulator {
    pub(crate) fn new(
        max_candidates: usize,
        after_route: Option<&(String, String, u64)>,
        max_entries_visited: usize,
    ) -> Self {
        Self {
            max_candidates,
            max_entries_visited,
            classified: 0,
            visited: 0,
            more: false,
            last_visited: after_route.cloned(),
        }
    }

    pub(crate) fn begin_entry(&mut self, is_unreported_closed: bool) -> bool {
        if self.visited >= self.max_entries_visited {
            self.more = true;
            return false;
        }
        if is_unreported_closed && self.classified >= self.max_candidates {
            self.more = true;
            return false;
        }
        true
    }

    pub(crate) fn visit(&mut self, key: &(String, String, u64)) {
        self.visited += 1;
        self.last_visited = Some(key.clone());
    }

    pub(crate) fn record_classified(&mut self) {
        self.classified += 1;
    }

    pub(crate) fn finish(self) -> ClosedEventSliceProgress {
        ClosedEventSliceProgress {
            classified: self.classified,
            more: self.more,
            after_route: self.last_visited,
        }
    }
}
