//! Canonical Hub session projection over Core lifecycle pages.
//!
//! Hub owns this in-memory projection. Core remains the lifecycle authority.
//! This module does not import terminal semantic bodies and does not name
//! package-owned product policy.

use std::collections::BTreeMap;

use botster_core::SessionLifecycleState;
use botster_core_daemon::{
    RegistrySessionState, SessionLifecycleChange, SessionLifecycleChangeKind,
    SessionLifecycleCursor, SessionLifecycleRecord,
};
use botster_hub_client::DaemonSessionEntity;
use serde_json::Value;

/// One projected session row and the evidence that may prove it ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionProjectionRow {
    /// Authoritative Core record last applied to this id.
    pub record: SessionLifecycleRecord,
    /// Total classifier: `current`, `ended`, or `indeterminate`.
    pub lifecycle_class: &'static str,
    /// True when a live journal upsert applied an ended class to this id.
    pub live_ended: bool,
    /// Journal sequence that last mutated this row.
    pub change_seq: u64,
    /// Derived: ended, and the durable restart-record set holds this id.
    pub restartable: bool,
}

/// One Hub lifecycle cursor and one canonical session projection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionProjection {
    /// Journal cursor after the last applied change or completed baseline.
    pub cursor: Option<SessionLifecycleCursor>,
    /// Rows keyed by session UUID.
    pub rows: BTreeMap<String, SessionProjectionRow>,
    /// True only after a complete baseline page has been assembled.
    pub baseline_complete: bool,
    /// True when delivery or source pressure requires a complete baseline.
    pub gap: bool,
}

impl SessionProjection {
    /// An upper bound on the bytes `project_entity` allocates for one record,
    /// computed from the record without allocating. Callers fund a copy with
    /// this bound before they make it.
    #[must_use]
    pub fn entity_bound_bytes(record: &SessionLifecycleRecord) -> usize {
        // The three fixed state strings ("starting", "running", "exited",
        // "failed", "stopping", "stale", and the lifecycle class) each fit 16.
        const STATE_STRINGS: usize = 3 * 16;
        let metadata = &record.metadata.entries;
        let value_bytes = |key: &str| metadata.get(key).map_or(0, String::len);
        let failure = match &record.lifecycle {
            Some(SessionLifecycleState::Failed { reason }) => reason.len(),
            _ => 0,
        };
        // Traits parse from a JSON array of strings into a `Vec<String>` that
        // grows by push, so its capacity can reach twice its length (at least
        // four). An element takes at least two bytes of JSON (`""`), so the
        // array holds at most `len / 2 + 1` elements: at most
        // `24 * 2 * (len / 2 + 1)` bytes of `String` headers, plus the string
        // data, which never exceeds `len`.
        let traits = value_bytes("botster.session_type.traits");
        std::mem::size_of::<DaemonSessionEntity>()
            + STATE_STRINGS
            + record.session.session_id.0.len()
            + failure
            + value_bytes("botster.session_type.id")
            + value_bytes("botster.session_type.source")
            + value_bytes("botster.session_type.role")
            + value_bytes("botster.session_type.interaction")
            + value_bytes("botster.session_type.lifecycle")
            + traits
            + 24 * 2 * (traits / 2 + 1)
    }

    /// Project one Core record into the Hub session entity shape.
    #[must_use]
    pub fn project_entity(record: &SessionLifecycleRecord) -> DaemonSessionEntity {
        let (lifecycle, exit_code, failure_reason) = match &record.lifecycle {
            Some(SessionLifecycleState::Starting) => (Some("starting".to_string()), None, None),
            Some(SessionLifecycleState::Running) => (Some("running".to_string()), None, None),
            Some(SessionLifecycleState::Stopping) => (Some("stopping".to_string()), None, None),
            Some(SessionLifecycleState::Exited { code }) => {
                (Some("exited".to_string()), *code, None)
            }
            Some(SessionLifecycleState::Failed { reason }) => {
                (Some("failed".to_string()), None, Some(reason.clone()))
            }
            None if record.session.registry_state == RegistrySessionState::Exited => {
                (Some("exited".to_string()), None, None)
            }
            None => (None, None, None),
        };
        let lifecycle_class =
            session_lifecycle_class(&record.session.registry_state, record.lifecycle.as_ref());
        let metadata = &record.metadata.entries;
        let traits = metadata
            .get("botster.session_type.traits")
            .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
            .unwrap_or_default();
        DaemonSessionEntity {
            session_uuid: record.session.session_id.0.clone(),
            registry_state: match record.session.registry_state {
                RegistrySessionState::Running => "running",
                RegistrySessionState::Stopping => "stopping",
                RegistrySessionState::Exited => "exited",
                RegistrySessionState::Stale => "stale",
            }
            .to_string(),
            lifecycle,
            lifecycle_class: lifecycle_class.to_string(),
            rows: record.session.size.rows,
            cols: record.session.size.cols,
            updated_at: record.session.updated_at,
            exit_code,
            failure_reason,
            session_type_id: metadata.get("botster.session_type.id").cloned(),
            session_type_source: metadata.get("botster.session_type.source").cloned(),
            role: metadata.get("botster.session_type.role").cloned(),
            traits,
            interaction: metadata.get("botster.session_type.interaction").cloned(),
            session_type_lifecycle: metadata.get("botster.session_type.lifecycle").cloned(),
            restartable: false,
        }
    }

    /// Project one row, including its derived `restartable` flag.
    #[must_use]
    pub fn project_row(row: &SessionProjectionRow) -> DaemonSessionEntity {
        DaemonSessionEntity {
            restartable: row.restartable,
            ..Self::project_entity(&row.record)
        }
    }

    /// A row is restartable when it has ended and a durable restart record
    /// exists. The record set is not copied here: ingest asks the published
    /// Hub state through a lookup, once per row it ingests.
    fn derive_restartable(lifecycle_class: &str, has_record: bool) -> bool {
        lifecycle_class == "ended" && has_record
    }

    /// One durable restart record was set or removed. Returns true when the
    /// projected row's flag flipped. A row not yet projected derives the flag
    /// through the lookup when it is ingested.
    pub fn restart_record_changed(&mut self, session_id: &str, present: bool) -> bool {
        match self.rows.get_mut(session_id) {
            Some(row) => {
                let restartable = Self::derive_restartable(row.lifecycle_class, present);
                if row.restartable == restartable {
                    return false;
                }
                row.restartable = restartable;
                true
            }
            None => false,
        }
    }

    /// `apply_change_with` for a Hub that keeps no restart records.
    #[cfg(test)]
    pub fn apply_change(&mut self, change: &SessionLifecycleChange) {
        self.apply_change_with(change, &|_| false);
    }

    /// Apply one journal change. Remove is not ended evidence. `has_record`
    /// says whether a durable restart record exists for a session id.
    pub fn apply_change_with(
        &mut self,
        change: &SessionLifecycleChange,
        has_record: &dyn Fn(&str) -> bool,
    ) {
        match &change.kind {
            SessionLifecycleChangeKind::Upsert { record } => {
                let lifecycle_class = session_lifecycle_class(
                    &record.session.registry_state,
                    record.lifecycle.as_ref(),
                );
                let live_ended = lifecycle_class == "ended";
                let restartable = Self::derive_restartable(
                    lifecycle_class,
                    has_record(&record.session.session_id.0),
                );
                self.rows.insert(
                    record.session.session_id.0.clone(),
                    SessionProjectionRow {
                        record: record.clone(),
                        lifecycle_class,
                        live_ended,
                        change_seq: change.cursor.sequence,
                        restartable,
                    },
                );
            }
            SessionLifecycleChangeKind::Removed { session_id } => {
                self.rows.remove(&session_id.0);
            }
            _ => {}
        }
        self.cursor = Some(change.cursor.clone());
    }

    /// Merge one baseline page. Incomplete pages are not ended evidence.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn apply_baseline_page(
        &mut self,
        snapshot: SessionLifecycleCursor,
        records: impl IntoIterator<Item = SessionLifecycleRecord>,
        complete: bool,
    ) {
        if !complete {
            return;
        }
        self.ingest_baseline_rows_with(snapshot.sequence, records, &|_| false);
        self.seal_baseline(snapshot);
    }

    /// `ingest_baseline_rows_with` for a Hub that keeps no restart records.
    #[cfg(test)]
    pub fn ingest_baseline_rows(
        &mut self,
        sequence: u64,
        records: impl IntoIterator<Item = SessionLifecycleRecord>,
    ) {
        self.ingest_baseline_rows_with(sequence, records, &|_| false);
    }

    /// Insert baseline rows without sealing the snapshot. `has_record` says
    /// whether a durable restart record exists for a session id; it is asked
    /// once per ingested row.
    pub fn ingest_baseline_rows_with(
        &mut self,
        sequence: u64,
        records: impl IntoIterator<Item = SessionLifecycleRecord>,
        has_record: &dyn Fn(&str) -> bool,
    ) {
        for record in records {
            let lifecycle_class =
                session_lifecycle_class(&record.session.registry_state, record.lifecycle.as_ref());
            let restartable =
                Self::derive_restartable(lifecycle_class, has_record(&record.session.session_id.0));
            self.rows.insert(
                record.session.session_id.0.clone(),
                SessionProjectionRow {
                    record,
                    lifecycle_class,
                    live_ended: false,
                    change_seq: sequence,
                    restartable,
                },
            );
        }
    }

    /// Mark the assembled baseline complete. Incomplete pages stay a gap.
    pub fn seal_baseline(&mut self, snapshot: SessionLifecycleCursor) {
        self.cursor = Some(snapshot);
        self.baseline_complete = true;
        self.gap = false;
    }

    /// Replace the projection with a complete baseline and clear the gap.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn replace_complete_baseline(
        &mut self,
        snapshot: SessionLifecycleCursor,
        records: impl IntoIterator<Item = SessionLifecycleRecord>,
    ) {
        self.rows.clear();
        self.baseline_complete = false;
        self.apply_baseline_page(snapshot, records, true);
    }

    /// Mark a gap. Later ended proof requires a complete baseline or a live ended patch.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn mark_gap(&mut self) {
        self.gap = true;
        self.baseline_complete = false;
    }

    /// Start a fresh baseline recovery without treating current rows as complete.
    pub fn begin_baseline_recovery(&mut self) {
        self.gap = true;
        self.baseline_complete = false;
        self.rows.clear();
        self.cursor = None;
    }

    /// Positive ended evidence only.
    #[cfg_attr(not(test), allow(dead_code))]
    ///
    /// Incomplete baseline, omitted UUID, indeterminate, remove, and gap do
    /// not prove ended. A live ended patch or a finished complete baseline
    /// ended row does.
    #[must_use]
    pub fn is_ended(&self, session_id: &str) -> bool {
        if session_id.is_empty() {
            return false;
        }
        let Some(row) = self.rows.get(session_id) else {
            return false;
        };
        if row.lifecycle_class != "ended" {
            return false;
        }
        if row.live_ended {
            return true;
        }
        self.baseline_complete && !self.gap
    }

    /// JSON patch from one entity to the next.
    #[must_use]
    pub fn entity_patch(previous: &DaemonSessionEntity, current: &DaemonSessionEntity) -> Value {
        let previous = serde_json::to_value(previous).expect("serialize previous session entity");
        let current = serde_json::to_value(current).expect("serialize current session entity");
        let previous = previous.as_object().expect("session entity object");
        let current = current.as_object().expect("session entity object");
        Value::Object(
            current
                .iter()
                .filter(|(key, value)| previous.get(*key) != Some(*value))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )
    }
}

/// Core's lifecycle failure reason for a session whose worker was lost.
pub(crate) const WORKER_LOST_REASON: &str = "worker_lost";

fn session_lifecycle_class(
    registry_state: &RegistrySessionState,
    lifecycle: Option<&SessionLifecycleState>,
) -> &'static str {
    if registry_state == &RegistrySessionState::Stale {
        // Stale is usually unknown (a row Hub could not adopt). A worker Core
        // saw die is known to be gone, so that session has ended.
        match lifecycle {
            Some(SessionLifecycleState::Failed { reason }) if reason == WORKER_LOST_REASON => {
                "ended"
            }
            _ => "indeterminate",
        }
    } else {
        match lifecycle {
            Some(
                SessionLifecycleState::Starting
                | SessionLifecycleState::Running
                | SessionLifecycleState::Stopping,
            ) => "current",
            Some(SessionLifecycleState::Exited { .. } | SessionLifecycleState::Failed { .. }) => {
                "ended"
            }
            None if registry_state == &RegistrySessionState::Exited => "ended",
            None => "indeterminate",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use botster_core::{CoreSessionMetadata, ResizePayload, SessionId};
    use botster_core_daemon::{DaemonSession, SessionLifecycleSourceId};

    fn cursor(sequence: u64) -> SessionLifecycleCursor {
        SessionLifecycleCursor {
            source_id: SessionLifecycleSourceId("source".to_string()),
            sequence,
        }
    }

    fn record(
        id: &str,
        registry: RegistrySessionState,
        lifecycle: Option<SessionLifecycleState>,
    ) -> SessionLifecycleRecord {
        SessionLifecycleRecord {
            session: DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: registry.clone(),
                size: ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: sequence_for(registry),
            },
            metadata: CoreSessionMetadata::new(),
            lifecycle,
        }
    }

    fn sequence_for(registry: RegistrySessionState) -> u64 {
        match registry {
            RegistrySessionState::Running => 1,
            RegistrySessionState::Stopping => 2,
            RegistrySessionState::Exited => 3,
            RegistrySessionState::Stale => 4,
        }
    }

    fn ended(id: &str) -> SessionLifecycleRecord {
        record(
            id,
            RegistrySessionState::Exited,
            Some(SessionLifecycleState::Exited { code: Some(0) }),
        )
    }

    #[test]
    fn restartable_flips_only_for_an_ended_row_with_a_durable_record() {
        let mut projection = SessionProjection::default();
        projection.replace_complete_baseline(
            cursor(1),
            [
                ended("done"),
                ended("plain"),
                record(
                    "live",
                    RegistrySessionState::Running,
                    Some(SessionLifecycleState::Running),
                ),
            ],
        );
        assert!(!SessionProjection::project_row(&projection.rows["done"]).restartable);
        assert!(projection.restart_record_changed("done", true));
        assert!(SessionProjection::project_row(&projection.rows["done"]).restartable);
        assert!(
            !projection.restart_record_changed("done", true),
            "idempotent"
        );
        // A record for a running session, or none for a plain Spawn, stays false.
        assert!(!projection.restart_record_changed("live", true));
        assert!(!projection.rows["live"].restartable);
        assert!(!projection.rows["plain"].restartable);
        assert!(projection.restart_record_changed("done", false));
        assert!(!projection.rows["done"].restartable);
    }

    #[test]
    fn a_record_that_exists_before_its_row_is_projected_is_derived_at_ingest() {
        let mut projection = SessionProjection::default();
        let has = |id: &str| id == "late" || id == "run";
        assert!(!projection.restart_record_changed("late", true));
        projection.ingest_baseline_rows_with(1, [ended("late"), ended("other")], &has);
        assert!(projection.rows["late"].restartable);
        assert!(!projection.rows["other"].restartable);
        // A live change that ends a running row derives the flag too.
        projection.apply_change_with(
            &SessionLifecycleChange {
                cursor: cursor(2),
                kind: SessionLifecycleChangeKind::Upsert {
                    record: ended("run"),
                },
            },
            &has,
        );
        assert!(projection.rows["run"].restartable);
    }

    #[test]
    fn ingest_asks_the_lookup_once_per_row_and_keeps_no_copy_of_the_record_set() {
        let mut projection = SessionProjection::default();
        let asked = std::cell::RefCell::new(Vec::new());
        let has = |id: &str| {
            asked.borrow_mut().push(id.to_string());
            false
        };
        projection.ingest_baseline_rows_with(1, [ended("a"), ended("b")], &has);
        projection.ingest_baseline_rows_with(2, [ended("c")], &has);
        assert_eq!(
            *asked.borrow(),
            ["a", "b", "c"],
            "one ask per row, only for its rows"
        );
    }

    #[test]
    fn begin_baseline_recovery_marks_a_gap_without_a_cursor() {
        let mut projection = SessionProjection::default();
        projection.replace_complete_baseline(
            cursor(4),
            [record(
                "done",
                RegistrySessionState::Exited,
                Some(SessionLifecycleState::Exited { code: Some(0) }),
            )],
        );
        projection.begin_baseline_recovery();
        assert!(projection.gap);
        assert!(!projection.baseline_complete);
        assert!(projection.cursor.is_none());
        assert!(!projection.is_ended("done"));
    }

    #[test]
    fn false_ended_matrix_rejects_incomplete_omitted_indeterminate_remove_and_gap() {
        let mut projection = SessionProjection::default();
        projection.apply_baseline_page(
            cursor(1),
            [record(
                "ended-incomplete",
                RegistrySessionState::Exited,
                Some(SessionLifecycleState::Exited { code: Some(0) }),
            )],
            false,
        );
        assert!(
            !projection.is_ended("ended-incomplete"),
            "incomplete baseline is not ended evidence"
        );
        assert!(!projection.is_ended(""), "omitted UUID is not ended");
        assert!(!projection.is_ended("missing"), "unknown UUID is not ended");

        projection.apply_change(&SessionLifecycleChange {
            cursor: cursor(2),
            kind: SessionLifecycleChangeKind::Upsert {
                record: record("stale", RegistrySessionState::Stale, None),
            },
        });
        assert!(!projection.is_ended("stale"));

        projection.apply_change(&SessionLifecycleChange {
            cursor: cursor(3),
            kind: SessionLifecycleChangeKind::Removed {
                session_id: SessionId("ended-incomplete".to_string()),
            },
        });
        assert!(!projection.is_ended("ended-incomplete"));

        projection.mark_gap();
        projection.apply_baseline_page(
            cursor(4),
            [record(
                "gapped",
                RegistrySessionState::Exited,
                Some(SessionLifecycleState::Exited { code: Some(1) }),
            )],
            false,
        );
        assert!(!projection.is_ended("gapped"));
    }

    #[test]
    fn live_ended_patch_and_complete_baseline_ended_row_prove_ended() {
        let mut projection = SessionProjection::default();
        projection.apply_change(&SessionLifecycleChange {
            cursor: cursor(1),
            kind: SessionLifecycleChangeKind::Upsert {
                record: record(
                    "live-ended",
                    RegistrySessionState::Exited,
                    Some(SessionLifecycleState::Exited { code: Some(0) }),
                ),
            },
        });
        assert!(projection.is_ended("live-ended"));

        let mut baseline = SessionProjection::default();
        baseline.replace_complete_baseline(
            cursor(8),
            [record(
                "baseline-ended",
                RegistrySessionState::Exited,
                Some(SessionLifecycleState::Failed {
                    reason: "failed".to_string(),
                }),
            )],
        );
        assert!(baseline.is_ended("baseline-ended"));
    }

    #[test]
    fn a_stale_session_is_ended_only_when_core_saw_its_worker_lost() {
        let class = |lifecycle| {
            SessionProjection::project_entity(&record("s", RegistrySessionState::Stale, lifecycle))
                .lifecycle_class
        };
        assert_eq!(
            class(Some(SessionLifecycleState::Failed {
                reason: WORKER_LOST_REASON.to_string(),
            })),
            "ended"
        );
        // A row Hub could not adopt at startup stays unknown.
        assert_eq!(
            class(Some(SessionLifecycleState::Failed {
                reason: "stale daemon session".to_string(),
            })),
            "indeterminate"
        );
        assert_eq!(class(None), "indeterminate");
        assert_eq!(class(Some(SessionLifecycleState::Running)), "indeterminate");
    }

    #[test]
    fn complete_baseline_exited_registry_without_engine_lifecycle_is_ended() {
        let mut projection = SessionProjection::default();
        projection.replace_complete_baseline(
            cursor(1),
            [record(
                "restarted-ended",
                RegistrySessionState::Exited,
                None,
            )],
        );
        assert!(
            projection.is_ended("restarted-ended"),
            "a complete baseline exited registry row is ended evidence after restart"
        );
        let entity = SessionProjection::project_entity(
            &projection
                .rows
                .get("restarted-ended")
                .expect("projected restarted row")
                .record,
        );
        assert_eq!(entity.lifecycle.as_deref(), Some("exited"));
        assert_eq!(entity.lifecycle_class, "ended");
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Running, None),
            "indeterminate"
        );
    }
}
