//! Package entity mutation admission, pending-gap state, and resync schedule.
//!
//! Ownership: HubRuntime admits publish during `invoke_plugin` pumping.
//! Daemon control fans out admitted frames and drives targeted provider resync.

use std::collections::{BTreeMap, VecDeque};
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

/// Coalesced provider resync schedule for one family.
#[derive(Debug, Clone)]
pub struct PackageEntityResyncState {
    pub needed: bool,
    pub next_eligible_at: Instant,
    pub attempts: u32,
    pub last_attempt_at: Option<Instant>,
    attempt_times: VecDeque<Instant>,
    pub degraded: bool,
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
        }
    }
}

impl PackageEntityResyncState {
    pub fn mark_needed(&mut self, now: Instant) {
        if !self.needed {
            self.needed = true;
            // Immediate first attempt for this need cycle.
            self.next_eligible_at = now;
            self.attempts = 0;
            self.attempt_times.clear();
        }
    }

    pub fn clear_needed(&mut self) {
        self.needed = false;
        self.attempts = 0;
        self.attempt_times.clear();
        self.last_attempt_at = None;
        // Degraded flag is sticky until a later successful re-arm path clears it
        // explicitly when convergence succeeds after re-arm.
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

    /// Record a provider attempt. Returns true when the cycle enters degraded.
    pub fn record_attempt(&mut self, now: Instant) -> bool {
        self.attempts = self.attempts.saturating_add(1);
        self.last_attempt_at = Some(now);
        self.attempt_times.push_back(now);
        while self
            .attempt_times
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= Duration::from_secs(1))
        {
            self.attempt_times.pop_front();
        }
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
    pub resync: PackageEntityResyncState,
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
            PackageEntityPublishStatus::Accepted
        } else if seq <= self.last_accepted_seq + PACKAGE_ENTITY_PENDING_WINDOW {
            self.high_water_seq = self.high_water_seq.max(seq);
            self.pending_by_seq.insert(seq, mutation);
            self.resync.mark_needed(now);
            PackageEntityPublishStatus::PendingGap
        } else {
            self.high_water_seq = self.high_water_seq.max(seq);
            self.resync.mark_needed(now);
            PackageEntityPublishStatus::ResyncScheduled
        };

        (self.result(status), ready)
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
        } else if self.resync.needed {
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
        } => {
            if id.0.is_empty() {
                return Err("entity_publish remove requires non-empty id".to_string());
            }
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
    if let Ok(record_id) = botster_core::EntityContract::extract_record_id(entity_type, entity)
        && record_id.0 != id.0
    {
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
}
