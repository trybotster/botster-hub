//! Package entity mutation admission, pending-gap state, and resync schedule.
//!
//! Ownership: HubRuntime admits publish during `invoke_plugin` pumping.
//! Daemon control fans out admitted frames and drives targeted provider resync.

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
#[derive(Debug, Clone, PartialEq)]
pub enum PackageEntityMutation {
    Upsert {
        entity_type: String,
        snapshot_seq: u64,
        id: String,
        entity: Value,
    },
    Patch {
        entity_type: String,
        snapshot_seq: u64,
        id: String,
        patch: Value,
    },
    Remove {
        entity_type: String,
        snapshot_seq: u64,
        id: String,
    },
}

impl PackageEntityMutation {
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
    pub family: String,
    pub seq: u64,
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
    pub leases: BTreeSet<(u64, String)>,
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
            leases: BTreeSet::new(),
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

/// Per-family runtime admission state.
#[derive(Debug, Clone, Default)]
pub struct PackageEntityFamilyState {
    pub last_accepted_seq: u64,
    pub high_water_seq: u64,
    pub pending_by_seq: BTreeMap<u64, PackageEntityMutation>,
    pub pending_leases: BTreeMap<u64, EntityMutationLease>,
    pub resync: PackageEntityResyncState,
    pub unloading: bool,
}

impl PackageEntityFamilyState {
    /// Admit one mutation and return frames ready for immediate fanout.
    pub fn admit(
        &mut self,
        mutation: PackageEntityMutation,
        now: Instant,
    ) -> (PackageEntityPublishResult, Vec<PackageEntityMutation>) {
        let seq = mutation.snapshot_seq();
        if seq < self.last_accepted_seq {
            return (
                self.result(PackageEntityPublishStatus::StaleSequence),
                Vec::new(),
            );
        }
        if seq == self.last_accepted_seq {
            return (
                self.result(PackageEntityPublishStatus::DuplicateSequence),
                Vec::new(),
            );
        }

        let mut ready = Vec::new();
        let status = if seq == self.last_accepted_seq + 1 {
            self.high_water_seq = self.high_water_seq.max(seq);
            self.last_accepted_seq = seq;
            ready.push(mutation);
            ready.extend(self.drain_consecutive_pending());
            // Every accepted publish is progress: re-arm when a gap remains, or
            // clear degraded fully when the family converges.
            self.after_publish_progress(now);
            PackageEntityPublishStatus::Accepted
        } else if seq <= self.last_accepted_seq + PACKAGE_ENTITY_PENDING_WINDOW {
            if self.pending_by_seq.contains_key(&seq) {
                return (
                    self.result(PackageEntityPublishStatus::DuplicateSequence),
                    Vec::new(),
                );
            }
            self.high_water_seq = self.high_water_seq.max(seq);
            self.pending_by_seq.insert(seq, mutation);
            // New publish re-arms even after degraded.
            self.resync.rearm(now);
            PackageEntityPublishStatus::PendingGap
        } else {
            self.high_water_seq = self.high_water_seq.max(seq);
            self.resync.rearm(now);
            PackageEntityPublishStatus::ResyncScheduled
        };

        (self.result(status), ready)
    }

    fn after_publish_progress(&mut self, now: Instant) {
        let gap_or_pending =
            self.last_accepted_seq < self.high_water_seq || !self.pending_by_seq.is_empty();
        if gap_or_pending {
            self.resync.rearm(now);
        } else {
            self.resync.clear_needed();
            self.resync.clear_degraded_on_progress();
        }
    }

    /// Apply a provider snapshot sequence to the family floor.
    ///
    /// Returns mutations that became deliverable after the floor advanced.
    pub fn apply_provider_snapshot_seq(
        &mut self,
        snapshot_seq: u64,
        now: Instant,
    ) -> Vec<PackageEntityMutation> {
        self.high_water_seq = self.high_water_seq.max(snapshot_seq);
        if snapshot_seq > self.last_accepted_seq {
            self.last_accepted_seq = snapshot_seq;
            // Drop pending at or below the new floor (provider is durable truth).
            self.pending_by_seq
                .retain(|seq, _| *seq > self.last_accepted_seq);
            let ready = self.drain_consecutive_pending();
            self.recompute_resync_need(now);
            ready
        } else {
            self.recompute_resync_need(now);
            Vec::new()
        }
    }

    fn drain_consecutive_pending(&mut self) -> Vec<PackageEntityMutation> {
        let mut ready = Vec::new();
        loop {
            let next = self.last_accepted_seq + 1;
            let Some(mutation) = self.pending_by_seq.remove(&next) else {
                break;
            };
            self.last_accepted_seq = next;
            self.high_water_seq = self.high_water_seq.max(next);
            ready.push(mutation);
        }
        ready
    }

    pub fn recompute_resync_need(&mut self, now: Instant) {
        let gap_or_high_water =
            self.last_accepted_seq < self.high_water_seq || !self.pending_by_seq.is_empty();
        if gap_or_high_water {
            self.resync.mark_needed(now);
        } else {
            // Always clear degraded on convergence, even when needed was already false.
            self.resync.clear_needed();
            self.resync.clear_degraded_on_progress();
        }
    }

    #[must_use]
    fn result(&self, status: PackageEntityPublishStatus) -> PackageEntityPublishResult {
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

    pub fn remember_resync_lease(&mut self, scope_id: u64, family: String) -> bool {
        self.resync.leases.insert((scope_id, family))
    }

    pub fn forget_resync_lease(&mut self, scope_id: u64, family: &str) {
        self.resync.leases.remove(&(scope_id, family.to_string()));
    }

    pub fn take_resync_leases(&mut self) -> BTreeSet<(u64, String)> {
        std::mem::take(&mut self.resync.leases)
    }

    #[must_use]
    pub fn active_scope_ids(&self) -> BTreeSet<u64> {
        let mut ids: BTreeSet<u64> = self
            .pending_leases
            .values()
            .map(|lease| lease.scope_id)
            .collect();
        ids.extend(self.resync.leases.iter().map(|(scope_id, _)| *scope_id));
        ids
    }

    #[must_use]
    pub fn provider_scope_id(&self) -> Option<u64> {
        self.resync
            .leases
            .iter()
            .map(|(scope_id, _)| *scope_id)
            .next()
            .or_else(|| {
                self.pending_leases
                    .values()
                    .map(|lease| lease.scope_id)
                    .next()
            })
    }
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
    use serde_json::json;

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
        let (result, ready) = state.admit(
            PackageEntityMutation::Upsert {
                entity_type: "f".into(),
                snapshot_seq: 1,
                id: "a".into(),
                entity: json!({"id":"a"}),
            },
            now,
        );
        assert_eq!(result.status, PackageEntityPublishStatus::Accepted);
        assert_eq!(ready.len(), 1);

        let (gap, ready) = state.admit(
            PackageEntityMutation::Upsert {
                entity_type: "f".into(),
                snapshot_seq: 3,
                id: "c".into(),
                entity: json!({"id":"c"}),
            },
            now,
        );
        assert_eq!(gap.status, PackageEntityPublishStatus::PendingGap);
        assert!(ready.is_empty());
        assert!(state.resync.needed);

        let (accepted, ready) = state.admit(
            PackageEntityMutation::Upsert {
                entity_type: "f".into(),
                snapshot_seq: 2,
                id: "b".into(),
                entity: json!({"id":"b"}),
            },
            now,
        );
        assert_eq!(accepted.status, PackageEntityPublishStatus::Accepted);
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].snapshot_seq(), 2);
        assert_eq!(ready[1].snapshot_seq(), 3);
        assert_eq!(state.last_accepted_seq, 3);
    }

    #[test]
    fn pending_and_resync_rows_keep_distinct_scope_identities() {
        let mut state = PackageEntityFamilyState::default();
        let now = Instant::now();
        let (gap, ready) = state.admit(
            PackageEntityMutation::Upsert {
                entity_type: "f".into(),
                snapshot_seq: 3,
                id: "c".into(),
                entity: json!({"id":"c"}),
            },
            now,
        );
        assert_eq!(gap.status, PackageEntityPublishStatus::PendingGap);
        assert!(ready.is_empty());
        state.store_pending_lease(EntityMutationLease {
            scope_id: 7,
            family: "f".into(),
            seq: 3,
        });
        assert!(state.remember_resync_lease(7, "f".into()));
        let (later, _) = state.admit(
            PackageEntityMutation::Upsert {
                entity_type: "f".into(),
                snapshot_seq: 4,
                id: "d".into(),
                entity: json!({"id":"d"}),
            },
            now,
        );
        assert_eq!(later.status, PackageEntityPublishStatus::PendingGap);
        state.store_pending_lease(EntityMutationLease {
            scope_id: 8,
            family: "f".into(),
            seq: 4,
        });
        assert!(state.remember_resync_lease(8, "f".into()));
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
        let (result, ready) = state.admit(
            PackageEntityMutation::Remove {
                entity_type: "f".into(),
                snapshot_seq: 20,
                id: "x".into(),
            },
            now,
        );
        assert_eq!(result.status, PackageEntityPublishStatus::ResyncScheduled);
        assert!(ready.is_empty());
        assert!(state.pending_by_seq.is_empty());
        assert_eq!(state.high_water_seq, 20);
        assert!(state.resync.needed);
    }

    #[test]
    fn duplicate_pending_sequence_rejects_without_replacing() {
        let mut state = PackageEntityFamilyState::default();
        let now = Instant::now();
        let first = PackageEntityMutation::Upsert {
            entity_type: "f".into(),
            snapshot_seq: 2,
            id: "first".into(),
            entity: json!({"id":"first","status":"original"}),
        };
        let (gap, ready) = state.admit(first.clone(), now);
        assert_eq!(gap.status, PackageEntityPublishStatus::PendingGap);
        assert!(ready.is_empty());
        let (dup, ready) = state.admit(
            PackageEntityMutation::Upsert {
                entity_type: "f".into(),
                snapshot_seq: 2,
                id: "second".into(),
                entity: json!({"id":"second","status":"replacement"}),
            },
            now,
        );
        assert_eq!(dup.status, PackageEntityPublishStatus::DuplicateSequence);
        assert!(ready.is_empty());
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
                entity_type: "f".into(),
                snapshot_seq: 1,
                id: "a".into(),
                entity: json!({"id":"a"}),
            },
            now,
        );
        let _ = state.admit(
            PackageEntityMutation::Upsert {
                entity_type: "f".into(),
                snapshot_seq: 20,
                id: "z".into(),
                entity: json!({"id":"z"}),
            },
            now,
        );
        state.resync.degraded = true;
        state.resync.needed = false;
        let (result, _) = state.admit(
            PackageEntityMutation::Upsert {
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
        let (done, _) = state.admit(
            PackageEntityMutation::Upsert {
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
