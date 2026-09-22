//! Checked allocation accounting. This module does not inspect input or schema.
//! The state and borrowed handles reside on the caller's stack, without heap
//! bookkeeping. The Host worker-stack reservation remains an open requirement.

use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Timeline {
    pub(super) live: usize,
    pub(super) maximum: usize,
    pub(super) refused: bool,
}

impl Timeline {
    fn allocate(&mut self, bytes: usize) -> Option<()> {
        let live = self.live.checked_add(bytes)?;
        self.live = live;
        self.maximum = self.maximum.max(live);
        Some(())
    }

    fn release(&mut self, bytes: usize) -> Option<()> {
        self.live = self.live.checked_sub(bytes)?;
        Some(())
    }

    fn replace(&mut self, old: usize, new: usize) -> Option<()> {
        self.allocate(new)?;
        self.release(old)
    }

    fn transfer(&mut self, bytes: usize) -> Option<()> {
        // The caller moves the corresponding byte count to its new owner.
        // Neither the live count nor its maximum changes during that move.
        (bytes <= self.live).then_some(())
    }
}

/// A borrowed handle adds no heap allocation or reference-counted owner.
#[derive(Clone, Copy, Default)]
pub(super) struct Track<'a> {
    state: Option<&'a Cell<Timeline>>,
    external_owner: Option<&'a Cell<usize>>,
}

/// The caller chooses the scope. This module knows only its byte counts.
pub(super) struct ScopeSnapshot {
    live: usize,
    external: usize,
}

impl<'a> Track<'a> {
    pub(super) fn new(state: &'a Cell<Timeline>) -> Self {
        Self {
            state: Some(state),
            external_owner: None,
        }
    }

    pub(super) fn excluding_owner(mut self, owner: &'a Cell<usize>) -> Self {
        self.external_owner = Some(owner);
        self
    }

    pub(super) fn begin_scope(self) -> Option<ScopeSnapshot> {
        let state = self.state.map_or(Timeline::default(), Cell::get);
        if state.refused {
            return None;
        }
        Some(ScopeSnapshot {
            live: state.live,
            external: self.external_owner.map_or(0, Cell::get),
        })
    }

    pub(super) fn finish_scope(self, before: ScopeSnapshot, output: usize) -> Option<()> {
        let external = self.external_owner.map_or(0, Cell::get);
        self.change(|state| {
            let external_growth = external.checked_sub(before.external)?;
            let temporary = state
                .live
                .checked_sub(before.live)?
                .checked_sub(output)?
                .checked_sub(external_growth)?;
            state.release(temporary)
        })
    }

    fn change(self, operation: impl FnOnce(&mut Timeline) -> Option<()>) -> Option<()> {
        let Some(state) = self.state else {
            return Some(());
        };
        let mut next = state.get();
        if next.refused {
            return None;
        }
        if operation(&mut next).is_none() {
            // Preserve the old accounting state on arithmetic refusal.
            let mut failed = state.get();
            failed.refused = true;
            state.set(failed);
            return None;
        }
        state.set(next);
        Some(())
    }

    pub(super) fn allocate(self, bytes: usize) -> Option<()> {
        self.change(|state| state.allocate(bytes))
    }

    pub(super) fn replace(self, old: usize, new: usize) -> Option<()> {
        self.change(|state| state.replace(old, new))
    }

    pub(super) fn transfer(self, bytes: usize) -> Option<()> {
        self.change(|state| state.transfer(bytes))
    }

    pub(super) fn replace_with_peak(self, old: usize, retained: usize, peak: usize) -> Option<()> {
        self.change(|state| {
            state.allocate(peak.checked_sub(old)?)?;
            state.release(peak.checked_sub(retained)?)
        })
    }

    pub(super) fn release(self, bytes: usize) -> Option<()> {
        self.change(|state| state.release(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_records_overlap_and_transfer_preserves_counts() {
        let state = Cell::new(Timeline::default());
        let track = Track::new(&state);
        track.allocate(8).unwrap();
        track.replace(8, 16).unwrap();
        assert_eq!(state.get().live, 16);
        assert_eq!(state.get().maximum, 24);
        let before = state.get();
        track.transfer(16).unwrap();
        assert_eq!(state.get(), before);
        track.release(16).unwrap();
        assert_eq!(state.get().live, 0);
        assert_eq!(state.get().maximum, 24);
    }

    #[test]
    fn arithmetic_refusal_preserves_counts_and_remains_distinct() {
        let state = Cell::new(Timeline {
            live: usize::MAX,
            maximum: usize::MAX,
            refused: false,
        });
        let track = Track::new(&state);
        assert!(track.allocate(1).is_none());
        assert_eq!(state.get().live, usize::MAX);
        assert_eq!(state.get().maximum, usize::MAX);
        assert!(state.get().refused);
        assert!(track.release(1).is_none());
        assert_eq!(state.get().live, usize::MAX);
    }

    #[test]
    fn scope_release_keeps_prior_owners_output_external_growth_and_maximum() {
        let state = Cell::new(Timeline::default());
        let external = Cell::new(8);
        let track = Track::new(&state).excluding_owner(&external);
        track.allocate(108).unwrap();
        let before = track.begin_scope().unwrap();
        track.allocate(20).unwrap();
        track.allocate(64).unwrap();
        track.replace_with_peak(8, 16, 24).unwrap();
        external.set(16);
        let maximum = state.get().maximum;
        track.finish_scope(before, 20).unwrap();
        assert_eq!(state.get().live, 100 + 20 + 16);
        assert_eq!(state.get().maximum, maximum);
        // A later parent allocation remains owned after the child scope ends.
        track.allocate(32).unwrap();
        assert_eq!(state.get().live, 100 + 20 + 16 + 32);
    }

    #[test]
    fn invalid_scope_output_refuses_without_releasing_storage() {
        let state = Cell::new(Timeline::default());
        let track = Track::new(&state);
        let before = track.begin_scope().unwrap();
        track.allocate(4).unwrap();
        assert!(track.finish_scope(before, 8).is_none());
        assert_eq!(state.get().live, 4);
        assert_eq!(state.get().maximum, 4);
        assert!(state.get().refused);
    }
}
