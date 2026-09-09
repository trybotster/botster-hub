//! The Host retains scan metadata and lease ownership through causal application.

use super::{CausalOp, Instant, PackageEntities};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    NextFamily,
    CheckFamily,
    RecordAttempt,
    Release,
    Clear,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Observation {
    pub(crate) generation: Option<u64>,
    pub(crate) deadline: Option<Instant>,
    pub(crate) degraded_leases: bool,
    pub(crate) attempted: bool,
    pub(crate) degraded: bool,
}

#[derive(Default)]
pub(crate) struct Cursor {
    pub(crate) after: Option<String>,
    pub(crate) observation: Observation,
    release_key: Option<(String, u64)>,
    release: Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,
}

impl Cursor {
    #[cfg(test)]
    pub(crate) fn test_release(&self) -> Option<u64> {
        self.release.as_ref().map(|(scope, _)| *scope)
    }

    pub(crate) fn input_bytes(&self) -> usize {
        self.after
            .as_ref()
            .map_or(0, String::len)
            .saturating_add(self.release_key.as_ref().map_or(0, |(name, _)| name.len()))
    }
}

impl PackageEntities {
    pub(super) fn resync_observation(&self, name: &str) -> Observation {
        let Some(family) = self.families.get(name) else {
            return Observation::default();
        };
        Observation {
            generation: Some(family.generation),
            deadline: if self.publication_drains_family(name, family.generation) {
                None
            } else {
                family.resync.next_attempt_at()
            },
            degraded_leases: family.resync.degraded && !family.resync.leases.is_empty(),
            ..Observation::default()
        }
    }

    pub(super) fn next_resync_family(&self, after: Option<&str>) -> Option<(&str, Observation)> {
        let next = match after {
            Some(after) => self
                .families
                .range::<str, _>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .next(),
            None => self.families.first_key_value(),
        };
        next.map(|(name, _)| (name.as_str(), self.resync_observation(name)))
    }

    pub(super) fn step_resync(
        &mut self,
        action: Action,
        cursor: &mut Cursor,
        causal: &mut Option<CausalOp>,
    ) {
        // The preceding phase reached the causal table before this Host phase starts.
        drop(cursor.release.take());
        drop(cursor.release_key.take());
        match action {
            Action::NextFamily => {
                if let Some((name, observation)) = self.next_resync_family(cursor.after.as_deref())
                {
                    cursor.after = Some(name.to_owned());
                    cursor.observation = observation;
                } else {
                    cursor.observation = Observation::default();
                }
            }
            Action::CheckFamily | Action::RecordAttempt => {
                let name = cursor
                    .after
                    .as_deref()
                    .expect("the scan retains its family");
                let expected = cursor.observation.generation;
                let mut observation = self.resync_observation(name);
                if observation.generation != expected {
                    observation.deadline = None;
                } else if action == Action::RecordAttempt
                    && observation
                        .deadline
                        .is_some_and(|deadline| deadline <= Instant::now())
                {
                    let degraded = self.record_resync_attempt(name);
                    observation = self.resync_observation(name);
                    observation.attempted = true;
                    observation.degraded = degraded;
                }
                cursor.observation = observation;
            }
            Action::Release => {
                cursor.release_key = self.resync_releases.pop_first();
                if let Some((name, generation)) = cursor.release_key.as_ref()
                    && self
                        .families
                        .get(name)
                        .is_some_and(|family| family.generation == *generation)
                {
                    self.take_resync_release_into(name, false, &mut cursor.release, causal);
                }
            }
            Action::Clear => {
                drop(cursor.after.take());
                cursor.observation = Observation::default();
            }
        }
    }
}
