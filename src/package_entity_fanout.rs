//! Package entity mutation admission, pending-gap state, and resync schedule.
//!
//! Ownership: HubRuntime admits publish during `invoke_plugin` pumping.
//! Daemon control fans out admitted frames and drives targeted provider resync.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

use botster_core::{EntityFrame, EntityId, EntityKind};
use serde_json::Value;

/// Pending-window width for out-of-order publish admission.
pub const PACKAGE_ENTITY_PENDING_WINDOW: u64 = 16;
/// First backoff after an initial immediate resync attempt.
pub const PACKAGE_ENTITY_RESYNC_INITIAL_BACKOFF: Duration = Duration::from_millis(50);
/// Cap for exponential resync backoff.
pub const PACKAGE_ENTITY_RESYNC_MAX_BACKOFF: Duration = Duration::from_secs(2);
/// Max provider resync calls per need cycle before degraded.
pub const PACKAGE_ENTITY_RESYNC_MAX_ATTEMPTS: u32 = 8;
/// Max provider resync calls per family per wall-clock second.
pub const PACKAGE_ENTITY_RESYNC_MAX_PER_SECOND: u32 = 2;

/// Coerce only top-level frame `items` when it is an empty JSON object → `[]`.
///
/// Nested empty objects in rows / `entity` / `patch` remain `{}`.
#[must_use]
pub fn coerce_entity_frame_empty_items(mut value: Value) -> Value {
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    let frame_type = object.get("type").and_then(Value::as_str);
    if !matches!(
        frame_type,
        Some("entity_snapshot" | "entity_scoped_snapshot")
    ) {
        return value;
    }
    if let Some(items) = object.get_mut("items")
        && items.as_object().is_some_and(serde_json::Map::is_empty)
    {
        *items = Value::Array(Vec::new());
    }
    value
}

/// Lua-visible admission status for `botster.entity_publish`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageEntityPublishStatus {
    Accepted,
    PendingGap,
    ResyncScheduled,
    StaleSequence,
    DuplicateSequence,
}

impl PackageEntityPublishStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::PendingGap => "pending_gap",
            Self::ResyncScheduled => "resync_scheduled",
            Self::StaleSequence => "stale_sequence",
            Self::DuplicateSequence => "duplicate_sequence",
        }
    }

    #[must_use]
    pub const fn ok(self) -> bool {
        matches!(
            self,
            Self::Accepted | Self::PendingGap | Self::ResyncScheduled
        )
    }
}

/// Mutation body admitted for fanout (no subscription id).
/// Keep each admission field last so payload destruction finishes before capacity returns.
#[derive(Debug, Clone, PartialEq)]
pub enum PackageEntityMutation {
    Upsert {
        entity_type: String,
        snapshot_seq: u64,
        id: String,
        entity: Value,
        admission: Option<crate::lua_runtime::EntityPublishPermit>,
    },
    Patch {
        entity_type: String,
        snapshot_seq: u64,
        id: String,
        patch: Value,
        admission: Option<crate::lua_runtime::EntityPublishPermit>,
    },
    Remove {
        entity_type: String,
        snapshot_seq: u64,
        id: String,
        admission: Option<crate::lua_runtime::EntityPublishPermit>,
    },
}

impl PackageEntityMutation {
    pub(crate) fn admission(&self) -> Option<&crate::lua_runtime::EntityPublishPermit> {
        match self {
            Self::Upsert { admission, .. }
            | Self::Patch { admission, .. }
            | Self::Remove { admission, .. } => admission.as_ref(),
        }
    }

    pub(crate) fn set_admission(
        &mut self,
        permit: Option<crate::lua_runtime::EntityPublishPermit>,
    ) {
        match self {
            Self::Upsert { admission, .. }
            | Self::Patch { admission, .. }
            | Self::Remove { admission, .. } => *admission = permit,
        }
    }

    #[must_use]
    pub fn entity_type(&self) -> &str {
        match self {
            Self::Upsert { entity_type, .. }
            | Self::Patch { entity_type, .. }
            | Self::Remove { entity_type, .. } => entity_type,
        }
    }

    #[must_use]
    pub const fn snapshot_seq(&self) -> u64 {
        match self {
            Self::Upsert { snapshot_seq, .. }
            | Self::Patch { snapshot_seq, .. }
            | Self::Remove { snapshot_seq, .. } => *snapshot_seq,
        }
    }

    pub fn from_entity_frame(frame: EntityFrame) -> Result<Self, String> {
        match frame {
            EntityFrame::Upsert {
                entity_type,
                snapshot_seq,
                id,
                entity,
            } => Ok(Self::Upsert {
                admission: None,
                entity_type: entity_type.0,
                snapshot_seq,
                id: id.0,
                entity,
            }),
            EntityFrame::Patch {
                entity_type,
                snapshot_seq,
                id,
                patch,
            } => Ok(Self::Patch {
                admission: None,
                entity_type: entity_type.0,
                snapshot_seq,
                id: id.0,
                patch,
            }),
            EntityFrame::Remove {
                entity_type,
                snapshot_seq,
                id,
            } => Ok(Self::Remove {
                admission: None,
                entity_type: entity_type.0,
                snapshot_seq,
                id: id.0,
            }),
            EntityFrame::Snapshot { .. } | EntityFrame::ScopedSnapshot { .. } => Err(
                "entity_publish accepts entity_upsert, entity_patch, or entity_remove only"
                    .to_string(),
            ),
        }
    }
}

/// Result returned to Lua after synchronous admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageEntityPublishResult {
    pub ok: bool,
    pub status: PackageEntityPublishStatus,
    pub last_accepted_seq: u64,
    pub high_water_seq: u64,
    pub resync_needed: bool,
    pub resync_degraded: bool,
}

/// Exact causal-scope identity for one admitted mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityMutationLease {
    pub scope_id: u64,
    pub family_token: u64,
    pub family: String,
    pub generation: u64,
    pub seq: u64,
    pub admission: Option<crate::lua_runtime::EntityPublishPermit>,
}

/// One queued mutation retains its generation even without a causal lease.
#[derive(Debug, PartialEq)]
pub(crate) struct LeasedFanoutMutation {
    pub(crate) generation: u64,
    pub(crate) mutation: PackageEntityMutation,
    pub(crate) lease: Option<EntityMutationLease>,
}

/// Delivery uses global FIFO order. Cleanup selects one exact family generation.
#[derive(Debug, Default)]
pub(crate) struct PackageEntityFanoutQueue {
    next_sequence: u64,
    pending_by_seq: BTreeMap<u64, LeasedFanoutMutation>,
    sequences_by_family: BTreeMap<(String, u64), BTreeSet<u64>>,
}

impl PackageEntityFanoutQueue {
    /// Check sequence capacity for one insertion without consuming a sequence.
    /// The caller must retain exclusive queue access through insertion.
    #[must_use]
    pub(crate) fn has_sequence_capacity(&self) -> bool {
        self.next_sequence.checked_add(1).is_some()
    }

    /// Return the owned mutation unchanged if the sequence cannot advance.
    pub(crate) fn try_push(
        &mut self,
        item: LeasedFanoutMutation,
    ) -> Result<(), LeasedFanoutMutation> {
        let mut retained = Some(item);
        if self.try_push_from(&mut retained) {
            Ok(())
        } else {
            Err(retained.expect("failed insertion retains the mutation"))
        }
    }

    /// Keep custody in the input slot until the queue stores the mutation.
    pub(crate) fn try_push_from(&mut self, retained: &mut Option<LeasedFanoutMutation>) -> bool {
        let item = retained.as_ref().expect("insertion requires a mutation");
        let Some(next_sequence) = self.next_sequence.checked_add(1) else {
            return false;
        };
        let family = (item.mutation.entity_type().to_string(), item.generation);
        assert!(!self.pending_by_seq.contains_key(&self.next_sequence));
        self.pending_by_seq.insert(
            self.next_sequence,
            retained
                .take()
                .expect("the input slot contains the mutation"),
        );
        self.sequences_by_family
            .entry(family)
            .or_default()
            .insert(self.next_sequence);
        self.next_sequence = next_sequence;
        true
    }

    pub(crate) fn pop_first(&mut self) -> Option<LeasedFanoutMutation> {
        let mut retained = None;
        self.pop_first_into(&mut retained);
        retained
    }

    /// Store the mutation before changing its family index.
    pub(crate) fn pop_first_into(&mut self, retained: &mut Option<LeasedFanoutMutation>) {
        assert!(retained.is_none(), "the output slot must be empty");
        if let Some((&sequence, _)) = self.pending_by_seq.first_key_value() {
            self.remove_into(sequence, retained);
        }
    }

    /// Find an old generation even when its live family state is absent.
    pub(crate) fn next_family_generation_before(&self, family: &str, epoch: u64) -> Option<u64> {
        self.sequences_by_family
            .range((family.to_string(), 0)..(family.to_string(), epoch))
            .next()
            .map(|((_, generation), _)| *generation)
    }

    /// Find one package family without inspecting payloads or other packages.
    pub(crate) fn next_package_family(&self, package: &str, after: Option<&str>) -> Option<String> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let prefix = format!("{package}.");
        let lower = after.map_or_else(
            || Included((prefix.clone(), 0)),
            |after| Excluded((after.to_string(), u64::MAX)),
        );
        self.sequences_by_family
            .range((lower, Unbounded))
            .next()
            .filter(|((family, _), _)| family.starts_with(&prefix))
            .map(|((family, _), _)| family.clone())
    }

    pub(crate) fn take_one_family(
        &mut self,
        family: &str,
        generation: u64,
    ) -> Option<LeasedFanoutMutation> {
        let mut retained = None;
        self.take_one_family_into(family, generation, &mut retained);
        retained
    }

    pub(crate) fn take_one_family_into(
        &mut self,
        family: &str,
        generation: u64,
        retained: &mut Option<LeasedFanoutMutation>,
    ) {
        assert!(retained.is_none(), "the output slot must be empty");
        if let Some(&sequence) = self
            .sequences_by_family
            .get(&(family.to_string(), generation))
            .and_then(|sequences| sequences.first())
        {
            self.remove_into(sequence, retained);
        }
    }

    fn remove_into(&mut self, sequence: u64, retained: &mut Option<LeasedFanoutMutation>) {
        assert!(retained.is_none(), "the output slot must be empty");
        *retained = self.pending_by_seq.remove(&sequence);
        let item = retained
            .as_ref()
            .expect("the selected sequence has a queued mutation");
        let family = (item.mutation.entity_type().to_string(), item.generation);
        let sequences = self
            .sequences_by_family
            .get_mut(&family)
            .expect("a queued mutation has family membership");
        assert!(
            sequences.remove(&sequence),
            "each queued mutation leaves once"
        );
        if sequences.is_empty() {
            self.sequences_by_family.remove(&family);
        }
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.pending_by_seq.is_empty()
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.pending_by_seq.len()
    }

    #[cfg(test)]
    pub(crate) fn sequence_for_test(&self) -> u64 {
        self.next_sequence
    }

    #[cfg(test)]
    pub(crate) fn set_next_sequence_for_test(&mut self, sequence: u64) {
        assert!(
            self.pending_by_seq.is_empty(),
            "the test queue must be empty"
        );
        assert!(
            self.sequences_by_family.is_empty(),
            "the test index must be empty"
        );
        self.next_sequence = sequence;
    }
}

/// Coalesced provider resync schedule for one family.
#[derive(Debug, Clone)]
pub struct PackageEntityResyncState {
    pub needed: bool,
    pub next_eligible_at: Instant,
    pub attempts: u32,
    pub last_attempt_at: Option<Instant>,
    attempt_times: VecDeque<Instant>,
    pub degraded: bool,
    pub leases: BTreeMap<u64, Option<crate::lua_runtime::EntityPublishPermit>>,
}

impl Default for PackageEntityResyncState {
    fn default() -> Self {
        Self {
            needed: false,
            next_eligible_at: Instant::now(),
            attempts: 0,
            last_attempt_at: None,
            attempt_times: VecDeque::new(),
            degraded: false,
            leases: BTreeMap::new(),
        }
    }
}

impl PackageEntityResyncState {
    /// Schedule coalesced resync when not already degraded.
    ///
    /// A degraded family stays ineligible until [`Self::rearm`] from a new
    /// publish or a new catching-up subscription. Stagnant catching_up alone
    /// must not start another attempt cycle.
    pub fn mark_needed(&mut self, now: Instant) {
        if self.degraded {
            return;
        }
        if !self.needed {
            self.needed = true;
            // Immediate first attempt for this need cycle.
            self.next_eligible_at = now;
            self.attempts = 0;
            self.attempt_times.clear();
        }
    }

    /// Explicit re-arm after a new publish or a newly catching-up subscribe.
    ///
    /// Resets the per-need-cycle attempt counter and clears degradation, but
    /// **retains** the rolling one-second attempt history so `can_attempt`
    /// still enforces ≤2 provider calls per family per wall-clock second.
    pub fn rearm(&mut self, now: Instant) {
        self.degraded = false;
        self.needed = true;
        self.next_eligible_at = now;
        self.attempts = 0;
        self.prune_attempt_times(now);
    }

    pub fn clear_needed(&mut self) {
        self.needed = false;
        self.attempts = 0;
        self.attempt_times.clear();
        self.last_attempt_at = None;
        // Degraded flag is sticky until rearm or successful convergence.
    }

    pub fn clear_degraded_on_progress(&mut self) {
        self.degraded = false;
    }

    #[must_use]
    pub fn can_attempt(&self, now: Instant) -> bool {
        if !self.needed || now < self.next_eligible_at {
            return false;
        }
        self.attempts_in_last_second(now) < PACKAGE_ENTITY_RESYNC_MAX_PER_SECOND
    }

    /// Return the next policy deadline without scanning the family collection.
    #[must_use]
    pub(crate) fn next_attempt_at(&self) -> Option<Instant> {
        if !self.needed || self.degraded {
            return None;
        }
        let mut deadline = self.next_eligible_at;
        if let Some(at) = self
            .attempt_times
            .iter()
            .rev()
            .nth(PACKAGE_ENTITY_RESYNC_MAX_PER_SECOND.saturating_sub(1) as usize)
        {
            deadline = deadline.max(at.checked_add(Duration::from_secs(1))?);
        }
        Some(deadline)
    }

    #[must_use]
    fn attempts_in_last_second(&self, now: Instant) -> u32 {
        self.attempt_times
            .iter()
            .filter(|at| now.saturating_duration_since(**at) < Duration::from_secs(1))
            .count() as u32
    }

    fn prune_attempt_times(&mut self, now: Instant) {
        while self
            .attempt_times
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= Duration::from_secs(1))
        {
            self.attempt_times.pop_front();
        }
    }

    /// Record a provider attempt. Returns true when the cycle enters degraded.
    pub fn record_attempt(&mut self, now: Instant) -> bool {
        self.attempts = self.attempts.saturating_add(1);
        self.last_attempt_at = Some(now);
        self.attempt_times.push_back(now);
        self.prune_attempt_times(now);
        let exponent = self.attempts.saturating_sub(1).min(6);
        let backoff = PACKAGE_ENTITY_RESYNC_INITIAL_BACKOFF
            .saturating_mul(1u32 << exponent)
            .min(PACKAGE_ENTITY_RESYNC_MAX_BACKOFF);
        self.next_eligible_at = now + backoff;
        if self.attempts >= PACKAGE_ENTITY_RESYNC_MAX_ATTEMPTS {
            self.degraded = true;
            self.needed = false;
            true
        } else {
            false
        }
    }
}

/// Scalar progress for an incremental provider snapshot transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackageEntityFamilyProgress {
    pub floor: u64,
    pub high_water: u64,
    pub needed: bool,
    pub degraded: bool,
    pub has_live_pending: bool,
    pub has_step_work: bool,
}

/// One bounded provider snapshot transition step.
#[derive(Debug, PartialEq)]
pub enum PackageEntityFamilyStep {
    Discarded {
        mutation: PackageEntityMutation,
        lease: Option<EntityMutationLease>,
    },
    Ready {
        mutation: PackageEntityMutation,
        lease: Option<EntityMutationLease>,
    },
    ReleaseResync {
        scope_id: u64,
        family_token: u64,
    },
    Complete(PackageEntityFamilyProgress),
}

/// The caller retains this record until the admission phase completes.
#[derive(Debug, Default)]
pub(crate) struct FamilyAdmissionWork {
    pub(crate) input: Option<PackageEntityMutation>,
    pub(crate) ready: Option<PackageEntityMutation>,
    pub(crate) discarded: Option<PackageEntityMutation>,
    pub(crate) result: Option<PackageEntityPublishResult>,
}

/// Retain a removed resync permit with the snapshot result until Host completion.
#[derive(Debug, Default)]
pub(crate) struct FamilySnapshotWork {
    pub(crate) step: Option<PackageEntityFamilyStep>,
    pub(crate) resync_lease: Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,
}

/// Per-family runtime admission state.
#[derive(Debug, Default)]
pub struct PackageEntityFamilyState {
    pub generation: u64,
    /// Allocated before this incarnation first admits a causal publication.
    pub causal_token: Option<u64>,
    pub last_accepted_seq: u64,
    pub high_water_seq: u64,
    pub pending_by_seq: BTreeMap<u64, PackageEntityMutation>,
    pub pending_leases: BTreeMap<u64, EntityMutationLease>,
    pub resync: PackageEntityResyncState,
}

impl PackageEntityFamilyState {
    /// Admit one mutation and return one ready frame and the original discarded mutation.
    pub fn admit(
        &mut self,
        mutation: PackageEntityMutation,
        now: Instant,
    ) -> (
        PackageEntityPublishResult,
        Option<PackageEntityMutation>,
        Option<PackageEntityMutation>,
    ) {
        let mut work = FamilyAdmissionWork {
            input: Some(mutation),
            ..FamilyAdmissionWork::default()
        };
        self.admit_retained(&mut work, now);
        (
            work.result.expect("admission completed"),
            work.ready,
            work.discarded,
        )
    }

    /// Store each transferred mutation before changing family progress.
    pub(crate) fn admit_retained(&mut self, work: &mut FamilyAdmissionWork, now: Instant) {
        assert!(work.ready.is_none() && work.discarded.is_none() && work.result.is_none());
        let seq = work
            .input
            .as_ref()
            .expect("admission requires a mutation")
            .snapshot_seq();
        let status = if seq < self.last_accepted_seq {
            work.discarded = work.input.take();
            PackageEntityPublishStatus::StaleSequence
        } else if seq == self.last_accepted_seq {
            work.discarded = work.input.take();
            PackageEntityPublishStatus::DuplicateSequence
        } else if self.last_accepted_seq.checked_add(1) == Some(seq) {
            work.ready = work.input.take();
            self.high_water_seq = self.high_water_seq.max(seq);
            self.last_accepted_seq = seq;
            self.after_publish_progress(now);
            PackageEntityPublishStatus::Accepted
        } else if seq
            <= self
                .last_accepted_seq
                .saturating_add(PACKAGE_ENTITY_PENDING_WINDOW)
        {
            match self.pending_by_seq.entry(seq) {
                Entry::Occupied(_) => {
                    work.discarded = work.input.take();
                    PackageEntityPublishStatus::DuplicateSequence
                }
                Entry::Vacant(slot) => {
                    slot.insert(work.input.take().expect("the input is retained"));
                    self.high_water_seq = self.high_water_seq.max(seq);
                    self.resync.rearm(now);
                    PackageEntityPublishStatus::PendingGap
                }
            }
        } else {
            work.discarded = work.input.take();
            self.high_water_seq = self.high_water_seq.max(seq);
            self.resync.rearm(now);
            PackageEntityPublishStatus::ResyncScheduled
        };
        work.result = Some(self.result(status));
    }

    fn after_publish_progress(&mut self, now: Instant) {
        let gap_or_pending =
            self.last_accepted_seq < self.high_water_seq || self.has_live_pending();
        if gap_or_pending {
            self.resync.rearm(now);
        } else {
            self.resync.clear_needed();
            self.resync.clear_degraded_on_progress();
        }
    }

    /// Begin an incremental provider snapshot transition with scalar updates only.
    pub fn begin_provider_snapshot_seq(
        &mut self,
        snapshot_seq: u64,
        now: Instant,
    ) -> PackageEntityFamilyProgress {
        self.high_water_seq = self.high_water_seq.max(snapshot_seq);
        self.last_accepted_seq = self.last_accepted_seq.max(snapshot_seq);
        self.recompute_resync_need(now);
        self.provider_snapshot_progress()
    }

    /// Apply at most one provider snapshot transition step.
    pub fn step_provider_snapshot(&mut self, now: Instant) -> PackageEntityFamilyStep {
        let mut work = FamilySnapshotWork::default();
        self.step_provider_snapshot_into(now, &mut work);
        work.step.expect("the snapshot step completed")
    }

    pub(crate) fn step_provider_snapshot_into(
        &mut self,
        now: Instant,
        work: &mut FamilySnapshotWork,
    ) {
        assert!(work.step.is_none() && work.resync_lease.is_none());
        let next_pending_seq = self.pending_by_seq.first_key_value().map(|(seq, _)| *seq);
        let stale = next_pending_seq.is_some_and(|seq| seq <= self.last_accepted_seq);
        let ready = self
            .last_accepted_seq
            .checked_add(1)
            .is_some_and(|next| next_pending_seq == Some(next));
        if stale || ready {
            let (seq, mutation) = self
                .pending_by_seq
                .pop_first()
                .expect("the pending row was observed");
            work.step = Some(if stale {
                PackageEntityFamilyStep::Discarded {
                    mutation,
                    lease: None,
                }
            } else {
                PackageEntityFamilyStep::Ready {
                    mutation,
                    lease: None,
                }
            });
            match work.step.as_mut().expect("the mutation was retained") {
                PackageEntityFamilyStep::Discarded { lease, .. }
                | PackageEntityFamilyStep::Ready { lease, .. } => {
                    *lease = self.pending_leases.remove(&seq);
                }
                _ => unreachable!("the retained step contains a mutation"),
            }
            if ready && !stale {
                self.last_accepted_seq = seq;
                self.high_water_seq = self.high_water_seq.max(seq);
            }
            self.recompute_resync_need(now);
            return;
        }

        self.recompute_resync_need(now);
        if self.converged() && !self.resync.leases.is_empty() {
            let family_token = self.causal_token.expect("resync lease has a family token");
            work.resync_lease = self.resync.leases.pop_first();
            let scope_id = work
                .resync_lease
                .as_ref()
                .expect("the resync lease was retained")
                .0;
            work.step = Some(PackageEntityFamilyStep::ReleaseResync {
                scope_id,
                family_token,
            });
            return;
        }
        work.step = Some(PackageEntityFamilyStep::Complete(
            self.provider_snapshot_progress(),
        ));
    }

    pub(crate) fn has_next_pending(&self) -> bool {
        self.last_accepted_seq
            .checked_add(1)
            .is_some_and(|next| self.pending_by_seq.contains_key(&next))
    }

    /// Move one consecutive mutation with its existing lease.
    pub(crate) fn take_next_pending(&mut self, now: Instant) -> Option<LeasedFanoutMutation> {
        let mut retained = None;
        self.take_next_pending_into(now, &mut retained);
        retained
    }

    /// Retain the mutation and lease before updating family progress.
    pub(crate) fn take_next_pending_into(
        &mut self,
        now: Instant,
        retained: &mut Option<LeasedFanoutMutation>,
    ) {
        assert!(retained.is_none(), "the output slot must be empty");
        let Some(next) = self.last_accepted_seq.checked_add(1) else {
            return;
        };
        let Some(mutation) = self.pending_by_seq.remove(&next) else {
            return;
        };
        *retained = Some(LeasedFanoutMutation {
            mutation,
            lease: None,
            generation: self.generation,
        });
        retained.as_mut().expect("the mutation was retained").lease =
            self.pending_leases.remove(&next);
        self.last_accepted_seq = next;
        self.high_water_seq = self.high_water_seq.max(next);
        self.after_publish_progress(now);
    }

    pub fn recompute_resync_need(&mut self, now: Instant) {
        let gap_or_high_water = !self.converged();
        if gap_or_high_water {
            self.resync.mark_needed(now);
        } else {
            // Always clear degraded on convergence, even when needed was already false.
            self.resync.clear_needed();
            self.resync.clear_degraded_on_progress();
        }
    }

    fn has_live_pending(&self) -> bool {
        self.pending_by_seq
            .last_key_value()
            .is_some_and(|(seq, _)| *seq > self.last_accepted_seq)
    }

    fn converged(&self) -> bool {
        self.last_accepted_seq >= self.high_water_seq && !self.has_live_pending()
    }

    /// Return scalar progress without cloning pending mutation payloads.
    #[must_use]
    pub fn provider_snapshot_progress(&self) -> PackageEntityFamilyProgress {
        let first_pending_seq = self.pending_by_seq.first_key_value().map(|(seq, _)| *seq);
        let has_stale = first_pending_seq.is_some_and(|seq| seq <= self.last_accepted_seq);
        let has_exact_next = self
            .last_accepted_seq
            .checked_add(1)
            .is_some_and(|next| first_pending_seq == Some(next));
        let has_live_pending = self.has_live_pending();
        let has_releasable_resync = self.converged() && !self.resync.leases.is_empty();
        PackageEntityFamilyProgress {
            floor: self.last_accepted_seq,
            high_water: self.high_water_seq,
            needed: self.resync.needed,
            degraded: self.resync.degraded,
            has_live_pending,
            has_step_work: has_stale || has_exact_next || has_releasable_resync,
        }
    }

    #[must_use]
    pub(crate) fn result(&self, status: PackageEntityPublishStatus) -> PackageEntityPublishResult {
        PackageEntityPublishResult {
            ok: status.ok(),
            status,
            last_accepted_seq: self.last_accepted_seq,
            high_water_seq: self.high_water_seq,
            resync_needed: self.resync.needed,
            resync_degraded: self.resync.degraded,
        }
    }

    pub fn store_pending_lease(&mut self, lease: EntityMutationLease) {
        self.pending_leases.insert(lease.seq, lease);
    }

    pub fn take_pending_lease(&mut self, seq: u64) -> Option<EntityMutationLease> {
        self.pending_leases.remove(&seq)
    }

    /// Leases whose pending row is gone. Call after moving ready leases to fanout.
    pub fn take_discarded_pending_leases(&mut self) -> Vec<EntityMutationLease> {
        let seqs: Vec<u64> = self
            .pending_leases
            .keys()
            .copied()
            .filter(|seq| !self.pending_by_seq.contains_key(seq))
            .collect();
        seqs.into_iter()
            .filter_map(|seq| self.pending_leases.remove(&seq))
            .collect()
    }

    pub fn remember_resync_lease(&mut self, scope_id: u64) -> bool {
        self.remember_resync_lease_with_admission(scope_id, None)
    }

    pub(crate) fn remember_resync_lease_with_admission(
        &mut self,
        scope_id: u64,
        admission: Option<crate::lua_runtime::EntityPublishPermit>,
    ) -> bool {
        assert!(
            self.causal_token.is_some(),
            "resync lease has a family token"
        );
        if let std::collections::btree_map::Entry::Vacant(entry) =
            self.resync.leases.entry(scope_id)
        {
            entry.insert(admission);
            true
        } else {
            false
        }
    }

    pub fn forget_resync_lease(&mut self, scope_id: u64) {
        self.resync.leases.remove(&scope_id);
    }

    #[must_use]
    pub fn active_scope_ids(&self) -> BTreeSet<u64> {
        let mut ids: BTreeSet<u64> = self
            .pending_leases
            .values()
            .map(|lease| lease.scope_id)
            .collect();
        ids.extend(self.resync.leases.keys().copied());
        ids
    }

    #[must_use]
    pub fn provider_scope_id(&self) -> Option<u64> {
        self.provider_obligation().map(|(scope_id, _)| scope_id)
    }

    pub(crate) fn provider_obligation(
        &self,
    ) -> Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)> {
        self.resync
            .leases
            .first_key_value()
            .map(|(scope_id, admission)| (*scope_id, admission.clone()))
            .or_else(|| {
                self.pending_leases
                    .values()
                    .next()
                    .map(|lease| (lease.scope_id, lease.admission.clone()))
            })
    }
}

/// Prepare a publication on the calling worker before queue admission.
pub(crate) fn prepare_publish_mutation(value: Value) -> Result<PackageEntityMutation, String> {
    let mutation = parse_publish_mutation(value)?;
    if package_entity_mutation_exceeds_limit(&mutation) {
        return Err(
            "entity_publish frame exceeds daemon frame limit (entity_provider_frame_too_large)"
                .into(),
        );
    }
    Ok(mutation)
}

fn package_entity_mutation_exceeds_limit(mutation: &PackageEntityMutation) -> bool {
    #[derive(serde::Serialize)]
    struct Frame<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        subscription_id: &'static str,
        entity_type: &'a str,
        snapshot_seq: u64,
        id: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        entity: Option<&'a Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        patch: Option<&'a Value>,
    }
    let (kind, id, entity, patch) = match mutation {
        PackageEntityMutation::Upsert { id, entity, .. } => {
            ("entity_upsert", id, Some(entity), None)
        }
        PackageEntityMutation::Patch { id, patch, .. } => ("entity_patch", id, None, Some(patch)),
        PackageEntityMutation::Remove { id, .. } => ("entity_remove", id, None, None),
    };
    // Admission bounds the mutation. Delivery checks each actual subscription frame.
    let frame = Frame {
        kind,
        subscription_id: "admission-size-check",
        entity_type: mutation.entity_type(),
        snapshot_seq: mutation.snapshot_seq(),
        id,
        entity,
        patch,
    };
    crate::bounded_json::encoded_len(&frame, crate::admission::budgets::DAEMON_MAX_FRAME_BYTES)
        .is_err()
}

/// Parse and validate a publish payload into a mutation frame.
pub fn parse_publish_mutation(value: Value) -> Result<PackageEntityMutation, String> {
    let value = coerce_entity_frame_empty_items(value);
    let frame: EntityFrame = serde_json::from_value(value)
        .map_err(|error| format!("invalid entity_publish frame: {error}"))?;
    // Validate id fields for upsert/patch when entity is an object with id.
    match &frame {
        EntityFrame::Upsert {
            entity_type,
            id,
            entity,
            ..
        } => {
            validate_mutation_record(entity_type, id, entity)?;
        }
        EntityFrame::Patch {
            entity_type: _, id, ..
        } => {
            if id.0.is_empty() {
                return Err("entity_publish patch requires non-empty id".to_string());
            }
        }
        EntityFrame::Remove {
            entity_type: _, id, ..
        } if id.0.is_empty() => {
            return Err("entity_publish remove requires non-empty id".to_string());
        }
        _ => {}
    }
    PackageEntityMutation::from_entity_frame(frame)
}

fn validate_mutation_record(
    entity_type: &EntityKind,
    id: &EntityId,
    entity: &Value,
) -> Result<(), String> {
    if id.0.is_empty() {
        return Err("entity_publish upsert requires non-empty id".to_string());
    }
    let record_id = botster_core::EntityContract::extract_record_id(entity_type, entity)
        .map_err(|error| error.to_string())?;
    if record_id.0 != id.0 {
        return Err(format!(
            "entity_publish upsert id {} does not match entity record id {}",
            id.0, record_id.0
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fanout_item(family: &str, generation: u64, seq: u64, leased: bool) -> LeasedFanoutMutation {
        LeasedFanoutMutation {
            generation,
            mutation: PackageEntityMutation::Upsert {
                admission: None,
                entity_type: family.into(),
                snapshot_seq: seq,
                id: format!("item-{seq}"),
                entity: json!({"payload": format!("payload-{family}-{generation}-{seq}")}),
            },
            lease: leased.then(|| EntityMutationLease {
                admission: None,
                family_token: 1,
                scope_id: 17,
                family: family.into(),
                generation,
                seq,
            }),
        }
    }

    fn assert_fanout_membership(queue: &PackageEntityFanoutQueue) {
        let mut expected: BTreeMap<(String, u64), BTreeSet<u64>> = BTreeMap::new();
        for (sequence, item) in &queue.pending_by_seq {
            assert!(*sequence < queue.next_sequence);
            expected
                .entry((item.mutation.entity_type().into(), item.generation))
                .or_default()
                .insert(*sequence);
        }
        assert_eq!(queue.sequences_by_family, expected);
        assert_eq!(
            queue.len(),
            expected.values().map(BTreeSet::len).sum::<usize>()
        );
        assert_eq!(queue.is_empty(), expected.is_empty());
    }

    #[test]
    fn fanout_extraction_retains_payload_and_lease_on_index_fault() {
        let mut queue = PackageEntityFanoutQueue::default();
        queue.try_push(fanout_item("a", 7, 3, true)).unwrap();
        queue.sequences_by_family.clear();
        let mut retained = None;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            queue.pop_first_into(&mut retained);
        }));
        assert!(result.is_err());
        assert_eq!(retained, Some(fanout_item("a", 7, 3, true)));
        assert!(queue.pending_by_seq.is_empty());
    }

    #[test]
    fn fanout_extraction_rejects_occupied_output_before_removal() {
        let mut queue = PackageEntityFanoutQueue::default();
        queue.try_push(fanout_item("a", 7, 3, true)).unwrap();
        let mut retained = Some(fanout_item("b", 2, 4, true));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            queue.pop_first_into(&mut retained);
        }));
        assert!(result.is_err());
        assert_eq!(retained, Some(fanout_item("b", 2, 4, true)));
        assert_eq!(queue.pop_first(), Some(fanout_item("a", 7, 3, true)));
    }

    #[test]
    fn fanout_sequence_exhaustion_preserves_input_slot() {
        let mut queue = PackageEntityFanoutQueue::default();
        queue.set_next_sequence_for_test(u64::MAX);
        let mut retained = Some(fanout_item("a", 7, 3, true));
        assert!(!queue.try_push_from(&mut retained));
        assert_eq!(retained, Some(fanout_item("a", 7, 3, true)));
        assert_fanout_membership(&queue);
    }

    #[test]
    fn snapshot_release_validates_identity_before_removing_lease() {
        let mut family = PackageEntityFamilyState::default();
        family.resync.leases.insert(17, None);
        let mut work = FamilySnapshotWork::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            family.step_provider_snapshot_into(Instant::now(), &mut work);
        }));
        assert!(result.is_err());
        assert!(family.resync.leases.contains_key(&17));
        assert!(work.step.is_none());
        assert!(work.resync_lease.is_none());
    }

    fn charged_mutation() -> (
        crate::lua_runtime::HubEntityPublishBridge,
        PackageEntityMutation,
    ) {
        let bridge = crate::lua_runtime::HubEntityPublishBridge::for_test("p", "p.item");
        let _response = bridge.test_queue_publish(
            botster_core::PluginKey("p".into()),
            json!({
                "type": "entity_upsert",
                "entity_type": "p.item",
                "snapshot_seq": 3,
                "id": "item-3",
                "entity": {"id": "item-3", "payload": "retained"}
            }),
            Some(17),
        );
        let (pending, ()) = bridge
            .take_if(|_| Some(()))
            .expect("the publication was admitted");
        (bridge, pending.mutation)
    }

    #[test]
    fn snapshot_release_retains_removed_record() {
        let (bridge, mutation) = charged_mutation();
        let mut family = PackageEntityFamilyState {
            causal_token: Some(23),
            ..PackageEntityFamilyState::default()
        };
        family
            .resync
            .leases
            .insert(17, mutation.admission().cloned());
        drop(mutation);
        let charged = bridge.retained_counts();
        assert_eq!(charged.0, 1);
        assert!(charged.1 > 0);
        let mut work = FamilySnapshotWork::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            family.step_provider_snapshot_into(Instant::now(), &mut work);
            panic!("failure after the resync record leaves the family");
        }));
        assert!(result.is_err());
        let (scope_id, admission) = work.resync_lease.as_ref().unwrap();
        assert_eq!(*scope_id, 17);
        assert!(admission.is_some());
        assert_eq!(bridge.retained_counts(), charged);
        assert_eq!(
            work.step,
            Some(PackageEntityFamilyStep::ReleaseResync {
                scope_id: 17,
                family_token: 23,
            })
        );
        assert!(family.resync.leases.is_empty());
        drop(work);
        assert_eq!(bridge.retained_counts(), (0, 0));
    }

    #[test]
    fn fanout_index_fault_retains_original_publication_credit() {
        let (bridge, mutation) = charged_mutation();
        let charged = bridge.retained_counts();
        let lease = EntityMutationLease {
            admission: mutation.admission().cloned(),
            scope_id: 17,
            family_token: 23,
            family: "p.item".into(),
            generation: 7,
            seq: 3,
        };
        let mut queue = PackageEntityFanoutQueue::default();
        queue
            .try_push(LeasedFanoutMutation {
                mutation,
                lease: Some(lease),
                generation: 7,
            })
            .unwrap();
        queue.sequences_by_family.clear();
        let mut retained = None;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            queue.pop_first_into(&mut retained);
        }));
        assert!(result.is_err());
        assert_eq!(bridge.retained_counts(), charged);
        let item = retained.as_ref().unwrap();
        let lease = item.lease.as_ref().unwrap();
        assert_eq!(
            (
                lease.scope_id,
                lease.family_token,
                lease.generation,
                lease.seq
            ),
            (17, 23, 7, 3)
        );
        assert_eq!(lease.admission.as_ref(), item.mutation.admission());
        let PackageEntityMutation::Upsert { entity, .. } = &item.mutation else {
            panic!("the original payload is an upsert");
        };
        assert_eq!(entity, &json!({"id": "item-3", "payload": "retained"}));
        assert!(queue.pending_by_seq.is_empty());
        drop(retained);
        assert_eq!(bridge.retained_counts(), (0, 0));
    }

    #[test]
    fn fanout_fifo_survives_exact_generation_cleanup() {
        let mut queue = PackageEntityFanoutQueue::default();
        assert_eq!(queue.pop_first(), None);
        assert_eq!(queue.take_one_family("a", 1), None);
        assert_fanout_membership(&queue);
        for (family, generation, seq, leased) in [
            ("a", 1, 1, true),
            ("b", 7, 1, false),
            ("a", 1, 2, false),
            ("a", 2, 1, true),
            ("a", 1, 3, true),
            ("c", 4, 1, true),
        ] {
            queue
                .try_push(fanout_item(family, generation, seq, leased))
                .expect("insert");
            assert_fanout_membership(&queue);
        }
        assert_eq!(queue.next_family_generation_before("a", 0), None);
        assert_eq!(queue.next_family_generation_before("a", 1), None);
        assert_eq!(queue.next_family_generation_before("a", 2), Some(1));
        assert_eq!(queue.next_family_generation_before("a", 3), Some(1));
        assert_eq!(
            queue.next_family_generation_before("missing", u64::MAX),
            None
        );
        assert_fanout_membership(&queue);
        assert_eq!(queue.pop_first(), Some(fanout_item("a", 1, 1, true)));
        assert_fanout_membership(&queue);
        assert_eq!(
            queue.take_one_family("a", 1),
            Some(fanout_item("a", 1, 2, false))
        );
        assert_fanout_membership(&queue);
        queue
            .try_push(fanout_item("a", 2, 2, false))
            .expect("recreated family");
        assert_fanout_membership(&queue);
        assert_eq!(
            queue.take_one_family("a", 1),
            Some(fanout_item("a", 1, 3, true))
        );
        assert_fanout_membership(&queue);
        assert_eq!(queue.next_family_generation_before("a", 2), None);
        assert_eq!(queue.next_family_generation_before("a", 3), Some(2));
        assert_fanout_membership(&queue);
        for family in ["a", "missing"] {
            assert_eq!(queue.take_one_family(family, 1), None);
            assert_fanout_membership(&queue);
        }
        for (family, generation, seq, leased) in [
            ("b", 7, 1, false),
            ("a", 2, 1, true),
            ("c", 4, 1, true),
            ("a", 2, 2, false),
        ] {
            assert_eq!(
                queue.pop_first(),
                Some(fanout_item(family, generation, seq, leased))
            );
            assert_fanout_membership(&queue);
        }
        assert!(queue.is_empty());
        assert_eq!(queue.next_sequence, 7);
        assert_eq!(queue.pop_first(), None);
        assert_fanout_membership(&queue);
    }

    #[test]
    fn fanout_capacity_check_consumes_no_sequences() {
        let mut queue = PackageEntityFanoutQueue::default();
        queue.set_next_sequence_for_test(u64::MAX - 2);
        assert!(queue.has_sequence_capacity());
        assert_eq!(queue.next_sequence, u64::MAX - 2);
        assert!(queue.is_empty());
        assert_fanout_membership(&queue);
        queue
            .try_push(fanout_item("a", 1, 1, true))
            .expect("only one item becomes ready");
        assert_eq!(queue.next_sequence, u64::MAX - 1);
        assert!(queue.has_sequence_capacity());
        assert_fanout_membership(&queue);
        queue
            .try_push(fanout_item("a", 2, 1, false))
            .expect("last available sequence");
        assert_eq!(queue.next_sequence, u64::MAX);
        assert!(!queue.has_sequence_capacity());
        assert_fanout_membership(&queue);
        assert_eq!(
            queue.take_one_family("a", 1),
            Some(fanout_item("a", 1, 1, true))
        );
        assert_fanout_membership(&queue);
        assert_eq!(queue.pop_first(), Some(fanout_item("a", 2, 1, false)));
        assert!(queue.is_empty());
        assert!(
            !queue.has_sequence_capacity(),
            "removal must not reuse sequences"
        );
        assert_fanout_membership(&queue);
    }

    #[test]
    fn fanout_exhaustion_returns_the_owned_payload_and_preserves_both_maps() {
        let mut queue = PackageEntityFanoutQueue::default();
        queue.set_next_sequence_for_test(u64::MAX - 1);
        queue
            .try_push(fanout_item("existing", 9, 1, true))
            .expect("last insertion");
        let before = queue.sequences_by_family.clone();
        let item = fanout_item("refused", 12, 4, true);
        let payload_address = match &item.mutation {
            PackageEntityMutation::Upsert { entity, .. } => {
                entity["payload"].as_str().expect("payload").as_ptr()
            }
            _ => unreachable!(),
        };
        let refused = queue.try_push(item).expect_err("sequence exhausted");
        assert_eq!(refused, fanout_item("refused", 12, 4, true));
        match &refused.mutation {
            PackageEntityMutation::Upsert { entity, .. } => assert_eq!(
                entity["payload"].as_str().expect("owned payload").as_ptr(),
                payload_address
            ),
            _ => unreachable!(),
        }
        assert_eq!(queue.next_sequence, u64::MAX);
        assert_eq!(queue.sequences_by_family, before);
        assert_eq!(
            queue.pending_by_seq.get(&(u64::MAX - 1)),
            Some(&fanout_item("existing", 9, 1, true))
        );
        assert_fanout_membership(&queue);
    }

    #[test]
    fn next_resync_deadline_preserves_backoff_and_the_rolling_rate_limit() {
        let now = Instant::now();
        let mut resync = PackageEntityResyncState::default();
        assert_eq!(resync.next_attempt_at(), None);
        resync.mark_needed(now);
        assert_eq!(resync.next_attempt_at(), Some(now));
        resync.record_attempt(now);
        assert_eq!(
            resync.next_attempt_at(),
            Some(now + PACKAGE_ENTITY_RESYNC_INITIAL_BACKOFF)
        );
        let second = now + PACKAGE_ENTITY_RESYNC_INITIAL_BACKOFF;
        assert!(resync.can_attempt(second));
        resync.record_attempt(second);
        assert_eq!(resync.next_attempt_at(), Some(now + Duration::from_secs(1)));
        resync.rearm(now + Duration::from_millis(60));
        assert_eq!(resync.next_attempt_at(), Some(now + Duration::from_secs(1)));
        for millis in [0, 49, 50, 60, 999, 1000, 1050] {
            let at = now + Duration::from_millis(millis);
            assert_eq!(
                resync.can_attempt(at),
                resync
                    .next_attempt_at()
                    .is_some_and(|deadline| at >= deadline)
            );
        }
        resync.clear_needed();
        assert_eq!(resync.next_attempt_at(), None);
    }
    use serde_json::json;

    fn pending_mutation(seq: u64, id: &str) -> PackageEntityMutation {
        PackageEntityMutation::Upsert {
            admission: None,
            entity_type: "f".into(),
            snapshot_seq: seq,
            id: id.into(),
            entity: json!({ "id": id }),
        }
    }

    fn store_pending_with_lease(
        state: &mut PackageEntityFamilyState,
        seq: u64,
        id: &str,
        scope_id: u64,
    ) {
        state.pending_by_seq.insert(seq, pending_mutation(seq, id));
        state.store_pending_lease(EntityMutationLease {
            admission: None,
            family_token: 1,
            generation: 0,
            scope_id,
            family: "f".into(),
            seq,
        });
        state.high_water_seq = state.high_water_seq.max(seq);
    }

    #[test]
    fn provider_snapshot_steps_discard_one_stale_payload_with_its_lease() {
        let now = Instant::now();
        let mut state = PackageEntityFamilyState::default();
        store_pending_with_lease(&mut state, 1, "one", 101);
        store_pending_with_lease(&mut state, 2, "two", 102);
        store_pending_with_lease(&mut state, 4, "four", 104);

        let progress = state.begin_provider_snapshot_seq(2, now);
        assert_eq!(progress.floor, 2);
        assert_eq!(progress.high_water, 4);
        assert!(progress.has_live_pending);
        assert!(progress.has_step_work);
        assert_eq!(state.pending_by_seq.len(), 3);
        assert_eq!(state.pending_leases.len(), 3);

        let first = state.step_provider_snapshot(now);
        assert!(matches!(
            first,
            PackageEntityFamilyStep::Discarded {
                mutation: PackageEntityMutation::Upsert {
                    snapshot_seq: 1,
                    ..
                },
                lease: Some(EntityMutationLease {
                    family_token: 1,
                    generation: 0,
                    scope_id: 101,
                    seq: 1,
                    ..
                })
            }
        ));
        assert_eq!(state.pending_by_seq.len(), 2);
        assert!(!state.pending_leases.contains_key(&1));

        let second = state.step_provider_snapshot(now);
        assert!(matches!(
            second,
            PackageEntityFamilyStep::Discarded {
                mutation: PackageEntityMutation::Upsert {
                    snapshot_seq: 2,
                    ..
                },
                lease: Some(EntityMutationLease {
                    family_token: 1,
                    generation: 0,
                    scope_id: 102,
                    seq: 2,
                    ..
                })
            }
        ));
        assert_eq!(state.pending_by_seq.len(), 1);
        assert!(!state.pending_leases.contains_key(&2));

        let PackageEntityFamilyStep::Complete(progress) = state.step_provider_snapshot(now) else {
            panic!("the gap must stop provider snapshot stepping");
        };
        assert_eq!(progress.floor, 2);
        assert!(progress.needed);
        assert!(progress.has_live_pending);
        assert!(!progress.has_step_work);
        assert!(state.pending_by_seq.contains_key(&4));
        assert!(state.pending_leases.contains_key(&4));
    }

    #[test]
    fn provider_snapshot_steps_move_consecutive_ready_rows_one_at_a_time() {
        let now = Instant::now();
        let mut state = PackageEntityFamilyState {
            last_accepted_seq: 1,
            high_water_seq: 1,
            ..Default::default()
        };
        store_pending_with_lease(&mut state, 3, "three", 203);
        store_pending_with_lease(&mut state, 4, "four", 204);

        let progress = state.begin_provider_snapshot_seq(2, now);
        assert_eq!(progress.floor, 2);
        assert!(progress.has_step_work);

        let first = state.step_provider_snapshot(now);
        assert!(matches!(
            first,
            PackageEntityFamilyStep::Ready {
                mutation: PackageEntityMutation::Upsert {
                    snapshot_seq: 3,
                    ..
                },
                lease: Some(EntityMutationLease {
                    family_token: 1,
                    generation: 0,
                    scope_id: 203,
                    seq: 3,
                    ..
                })
            }
        ));
        assert_eq!(state.last_accepted_seq, 3);
        assert_eq!(state.pending_by_seq.len(), 1);

        let second = state.step_provider_snapshot(now);
        assert!(matches!(
            second,
            PackageEntityFamilyStep::Ready {
                mutation: PackageEntityMutation::Upsert {
                    snapshot_seq: 4,
                    ..
                },
                lease: Some(EntityMutationLease {
                    family_token: 1,
                    generation: 0,
                    scope_id: 204,
                    seq: 4,
                    ..
                })
            }
        ));
        assert_eq!(state.last_accepted_seq, 4);
        assert!(state.pending_by_seq.is_empty());

        let PackageEntityFamilyStep::Complete(progress) = state.step_provider_snapshot(now) else {
            panic!("consecutive rows must reach completion");
        };
        assert_eq!(progress.floor, 4);
        assert!(!progress.needed);
        assert!(!progress.has_live_pending);
        assert!(!progress.has_step_work);
    }

    #[test]
    fn provider_snapshot_begin_never_lowers_floor_during_cleanup() {
        let now = Instant::now();
        let mut state = PackageEntityFamilyState::default();
        store_pending_with_lease(&mut state, 1, "one", 301);
        store_pending_with_lease(&mut state, 2, "two", 302);
        store_pending_with_lease(&mut state, 3, "three", 303);

        assert_eq!(state.begin_provider_snapshot_seq(2, now).floor, 2);
        assert_eq!(state.pending_by_seq.len(), 3);
        assert_eq!(state.begin_provider_snapshot_seq(1, now).floor, 2);
        assert_eq!(state.pending_by_seq.len(), 3);
        assert_eq!(state.begin_provider_snapshot_seq(2, now).floor, 2);
        assert_eq!(state.pending_by_seq.len(), 3);
        assert!(matches!(
            state.step_provider_snapshot(now),
            PackageEntityFamilyStep::Discarded {
                mutation: PackageEntityMutation::Upsert {
                    snapshot_seq: 1,
                    ..
                },
                ..
            }
        ));

        let progress = state.begin_provider_snapshot_seq(3, now);
        assert_eq!(progress.floor, 3);
        assert_eq!(progress.high_water, 3);
        for expected in [2, 3] {
            assert!(matches!(
                state.step_provider_snapshot(now),
                PackageEntityFamilyStep::Discarded {
                    mutation: PackageEntityMutation::Upsert { snapshot_seq, .. },
                    ..
                } if snapshot_seq == expected
            ));
        }
        assert!(matches!(
            state.step_provider_snapshot(now),
            PackageEntityFamilyStep::Complete(PackageEntityFamilyProgress {
                floor: 3,
                has_step_work: false,
                ..
            })
        ));
    }

    #[test]
    fn converged_provider_snapshot_releases_one_resync_lease_per_step() {
        let now = Instant::now();
        let mut state = PackageEntityFamilyState {
            causal_token: Some(1),
            last_accepted_seq: 5,
            high_water_seq: 5,
            ..Default::default()
        };
        state.resync.rearm(now);
        assert!(state.remember_resync_lease(401));
        assert!(state.remember_resync_lease(402));

        let progress = state.begin_provider_snapshot_seq(5, now);
        assert!(!progress.needed);
        assert!(progress.has_step_work);
        assert_eq!(
            state.step_provider_snapshot(now),
            PackageEntityFamilyStep::ReleaseResync {
                scope_id: 401,
                family_token: 1
            }
        );
        assert_eq!(state.resync.leases.len(), 1);
        assert_eq!(
            state.step_provider_snapshot(now),
            PackageEntityFamilyStep::ReleaseResync {
                scope_id: 402,
                family_token: 1
            }
        );
        assert!(state.resync.leases.is_empty());
        assert!(matches!(
            state.step_provider_snapshot(now),
            PackageEntityFamilyStep::Complete(PackageEntityFamilyProgress {
                has_step_work: false,
                ..
            })
        ));
    }

    #[test]
    fn provider_snapshot_sequence_arithmetic_handles_u64_boundary() {
        let now = Instant::now();
        let mut state = PackageEntityFamilyState {
            last_accepted_seq: u64::MAX - 1,
            high_water_seq: u64::MAX - 1,
            ..Default::default()
        };
        store_pending_with_lease(&mut state, u64::MAX, "max", 501);

        let progress = state.begin_provider_snapshot_seq(u64::MAX - 1, now);
        assert!(progress.has_step_work);
        assert!(matches!(
            state.step_provider_snapshot(now),
            PackageEntityFamilyStep::Ready {
                mutation: PackageEntityMutation::Upsert {
                    snapshot_seq: u64::MAX,
                    ..
                },
                lease: Some(EntityMutationLease {
                    family_token: 1,
                    generation: 0,
                    scope_id: 501,
                    seq: u64::MAX,
                    ..
                })
            }
        ));
        assert_eq!(state.last_accepted_seq, u64::MAX);
        assert!(matches!(
            state.step_provider_snapshot(now),
            PackageEntityFamilyStep::Complete(PackageEntityFamilyProgress {
                floor: u64::MAX,
                has_step_work: false,
                ..
            })
        ));

        let (duplicate, ready, _discarded) =
            state.admit(pending_mutation(u64::MAX, "duplicate"), now);
        assert_eq!(
            duplicate.status,
            PackageEntityPublishStatus::DuplicateSequence
        );
        assert!(ready.is_none());
        assert_eq!(state.begin_provider_snapshot_seq(0, now).floor, u64::MAX);
    }

    #[test]
    fn coerce_empty_items_object_to_array_only() {
        let coerced = coerce_entity_frame_empty_items(json!({
            "type": "entity_snapshot",
            "entity_type": "project-pipelines.run",
            "snapshot_seq": 1,
            "items": {}
        }));
        assert_eq!(coerced["items"], json!([]));
    }

    #[test]
    fn coerce_preserves_nested_empty_objects() {
        let coerced = coerce_entity_frame_empty_items(json!({
            "type": "entity_snapshot",
            "entity_type": "project-pipelines.run",
            "snapshot_seq": 1,
            "items": [{ "id": "a", "meta": {} }]
        }));
        assert_eq!(coerced["items"][0]["meta"], json!({}));
    }

    #[test]
    fn admission_accepts_in_order_and_drains_pending() {
        let mut state = PackageEntityFamilyState::default();
        let now = Instant::now();
        let (result, ready, _discarded) = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 1,
                id: "a".into(),
                entity: json!({"id":"a"}),
            },
            now,
        );
        assert_eq!(result.status, PackageEntityPublishStatus::Accepted);
        assert!(ready.is_some());

        let (gap, ready, _discarded) = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 3,
                id: "c".into(),
                entity: json!({"id":"c"}),
            },
            now,
        );
        assert_eq!(gap.status, PackageEntityPublishStatus::PendingGap);
        assert!(ready.is_none());
        assert!(state.resync.needed);

        let (accepted, ready, _discarded) = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 2,
                id: "b".into(),
                entity: json!({"id":"b"}),
            },
            now,
        );
        assert_eq!(accepted.status, PackageEntityPublishStatus::Accepted);
        assert_eq!(ready.unwrap().snapshot_seq(), 2);
        assert_eq!(state.last_accepted_seq, 2);
        assert_eq!(
            state
                .take_next_pending(now)
                .unwrap()
                .mutation
                .snapshot_seq(),
            3
        );
        assert_eq!(state.last_accepted_seq, 3);
    }

    #[test]
    fn pending_and_resync_rows_keep_distinct_scope_identities() {
        let mut state = PackageEntityFamilyState {
            causal_token: Some(1),
            ..Default::default()
        };
        let now = Instant::now();
        let (gap, ready, _discarded) = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 3,
                id: "c".into(),
                entity: json!({"id":"c"}),
            },
            now,
        );
        assert_eq!(gap.status, PackageEntityPublishStatus::PendingGap);
        assert!(ready.is_none());
        state.store_pending_lease(EntityMutationLease {
            admission: None,
            family_token: 1,
            generation: 0,
            scope_id: 7,
            family: "f".into(),
            seq: 3,
        });
        assert!(state.remember_resync_lease(7));
        let (later, _, _discarded) = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 4,
                id: "d".into(),
                entity: json!({"id":"d"}),
            },
            now,
        );
        assert_eq!(later.status, PackageEntityPublishStatus::PendingGap);
        state.store_pending_lease(EntityMutationLease {
            admission: None,
            family_token: 1,
            generation: 0,
            scope_id: 8,
            family: "f".into(),
            seq: 4,
        });
        assert!(state.remember_resync_lease(8));
        assert_eq!(state.active_scope_ids(), BTreeSet::from([7, 8]));
        assert_eq!(state.pending_leases.get(&3).map(|lease| lease.seq), Some(3));
        assert_eq!(state.provider_scope_id(), Some(7));
    }

    #[test]
    fn outside_window_sets_high_water_without_storing_body() {
        let mut state = PackageEntityFamilyState {
            last_accepted_seq: 1,
            high_water_seq: 1,
            ..Default::default()
        };
        let now = Instant::now();
        let (result, ready, _discarded) = state.admit(
            PackageEntityMutation::Remove {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 20,
                id: "x".into(),
            },
            now,
        );
        assert_eq!(result.status, PackageEntityPublishStatus::ResyncScheduled);
        assert!(ready.is_none());
        assert!(state.pending_by_seq.is_empty());
        assert_eq!(state.high_water_seq, 20);
        assert!(state.resync.needed);
    }

    #[test]
    fn duplicate_pending_sequence_rejects_without_replacing() {
        let mut state = PackageEntityFamilyState::default();
        let now = Instant::now();
        let first = PackageEntityMutation::Upsert {
            admission: None,
            entity_type: "f".into(),
            snapshot_seq: 2,
            id: "first".into(),
            entity: json!({"id":"first","status":"original"}),
        };
        let (gap, ready, _discarded) = state.admit(first.clone(), now);
        assert_eq!(gap.status, PackageEntityPublishStatus::PendingGap);
        assert!(ready.is_none());
        let (dup, ready, _discarded) = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 2,
                id: "second".into(),
                entity: json!({"id":"second","status":"replacement"}),
            },
            now,
        );
        assert_eq!(dup.status, PackageEntityPublishStatus::DuplicateSequence);
        assert!(ready.is_none());
        assert_eq!(
            state.pending_by_seq.get(&2),
            Some(&first),
            "first pending body must remain intact"
        );
    }

    #[test]
    fn degraded_mark_needed_does_not_start_new_cycle() {
        let mut resync = PackageEntityResyncState::default();
        let now = Instant::now();
        resync.rearm(now);
        for _ in 0..PACKAGE_ENTITY_RESYNC_MAX_ATTEMPTS {
            let _ = resync.record_attempt(now);
        }
        assert!(resync.degraded);
        assert!(!resync.needed);
        resync.mark_needed(now);
        assert!(resync.degraded);
        assert!(
            !resync.needed,
            "stagnant mark_needed must not re-arm degraded"
        );
        resync.rearm(now);
        assert!(!resync.degraded);
        assert!(resync.needed);
        assert_eq!(resync.attempts, 0);
    }

    #[test]
    fn upsert_validation_requires_extractable_record_id() {
        assert!(
            parse_publish_mutation(json!({
                "type": "entity_upsert",
                "entity_type": "project-pipelines.run",
                "snapshot_seq": 1,
                "id": "run-1",
                "entity": { "status": "missing-id" }
            }))
            .is_err()
        );
        assert!(
            parse_publish_mutation(json!({
                "type": "entity_upsert",
                "entity_type": "project-pipelines.run",
                "snapshot_seq": 1,
                "id": "run-1",
                "entity": { "id": "other", "status": "mismatch" }
            }))
            .is_err()
        );
        assert!(
            parse_publish_mutation(json!({
                "type": "entity_upsert",
                "entity_type": "project-pipelines.run",
                "snapshot_seq": 1,
                "id": "run-1",
                "entity": { "id": "run-1", "status": "ok" }
            }))
            .is_ok()
        );
    }

    #[test]
    fn remove_validation_requires_non_empty_id() {
        assert_eq!(
            parse_publish_mutation(json!({
                "type": "entity_remove",
                "entity_type": "project-pipelines.run",
                "snapshot_seq": 1,
                "id": ""
            })),
            Err("entity_publish remove requires non-empty id".to_string())
        );
        assert_eq!(
            parse_publish_mutation(json!({
                "type": "entity_remove",
                "entity_type": "project-pipelines.run",
                "snapshot_seq": 1,
                "id": "run-1"
            })),
            Ok(PackageEntityMutation::Remove {
                admission: None,
                entity_type: "project-pipelines.run".into(),
                snapshot_seq: 1,
                id: "run-1".into(),
            })
        );
    }

    #[test]
    fn rearm_preserves_rolling_rate_window() {
        let mut resync = PackageEntityResyncState::default();
        let now = Instant::now();
        resync.rearm(now);
        // record_attempt does not consult can_attempt; force two wall-clock hits.
        let _ = resync.record_attempt(now);
        let _ = resync.record_attempt(now);
        assert_eq!(resync.attempts, 2);
        assert!(
            !resync.can_attempt(now),
            "rate cap or backoff must block further attempts"
        );
        resync.rearm(now);
        assert_eq!(resync.attempts, 0);
        assert!(
            !resync.can_attempt(now),
            "after re-arm, retained one-second history must keep a third call ineligible"
        );
    }

    #[test]
    fn in_order_publish_rearms_when_gap_remains_and_clears_degraded_on_convergence() {
        let mut state = PackageEntityFamilyState::default();
        let now = Instant::now();
        let _ = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 1,
                id: "a".into(),
                entity: json!({"id":"a"}),
            },
            now,
        );
        let _ = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 20,
                id: "z".into(),
                entity: json!({"id":"z"}),
            },
            now,
        );
        state.resync.degraded = true;
        state.resync.needed = false;
        let (result, _, _discarded) = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 2,
                id: "b".into(),
                entity: json!({"id":"b"}),
            },
            now,
        );
        assert_eq!(result.status, PackageEntityPublishStatus::Accepted);
        assert!(state.resync.needed);
        assert!(!state.resync.degraded);
        assert_eq!(state.last_accepted_seq, 2);
        assert_eq!(state.high_water_seq, 20);

        state.last_accepted_seq = 19;
        state.resync.degraded = true;
        state.resync.needed = false;
        let (done, _, _discarded) = state.admit(
            PackageEntityMutation::Upsert {
                admission: None,
                entity_type: "f".into(),
                snapshot_seq: 20,
                id: "z2".into(),
                entity: json!({"id":"z2"}),
            },
            now,
        );
        assert_eq!(done.status, PackageEntityPublishStatus::Accepted);
        assert!(!done.resync_needed);
        assert!(!done.resync_degraded);
        assert!(!state.resync.degraded);
        assert!(!state.resync.needed);
        assert_eq!(state.last_accepted_seq, 20);
    }
}
