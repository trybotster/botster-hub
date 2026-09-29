//! Canonical Hub session projection over Core lifecycle pages.
//!
//! Hub owns this in-memory projection. Core remains the lifecycle authority.
//! This module does not import terminal semantic bodies and does not name
//! package-owned product policy.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use botster_core::SessionLifecycleState;
use botster_core_daemon::{
    RegistrySessionState, SessionLifecycleChange, SessionLifecycleChangeKind,
    SessionLifecycleCursor, SessionLifecycleRecord,
};
use botster_hub_client::DaemonSessionEntity;
use serde_json::Value;

use crate::session_view::SessionView;

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

/// The optional handle to the plugin-readable copy of the rows. It takes no
/// part in the projection's equality.
#[derive(Debug, Clone, Default)]
pub struct SessionViewHandle(Option<Arc<SessionView>>);

impl PartialEq for SessionViewHandle {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for SessionViewHandle {}

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
    /// Session ids that hold a durable restart record. Refreshed from the
    /// published Hub state at each baseline, then kept by keyed changes.
    pub restart_ids: BTreeSet<String>,
    /// The plugin-readable copy of the rows, written by the mutators below.
    view: SessionViewHandle,
}

impl SessionProjection {
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

    /// Attach the plugin-readable view. The first call wins. The view is
    /// filled from the rows already projected, and sealed when the baseline is.
    pub(crate) fn attach_view(&mut self, view: &Arc<SessionView>) {
        if self.view.0.is_some() {
            return;
        }
        for row in self.rows.values() {
            view.upsert(Self::project_row(row));
        }
        view.set_sealed(self.baseline_complete);
        self.view = SessionViewHandle(Some(Arc::clone(view)));
    }

    fn view_upsert(&self, id: &str) {
        if let (Some(view), Some(row)) = (&self.view.0, self.rows.get(id)) {
            view.upsert(Self::project_row(row));
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

    fn derive_restartable(&self, lifecycle_class: &str, session_id: &str) -> bool {
        lifecycle_class == "ended" && self.restart_ids.contains(session_id)
    }

    /// Replace the durable restart-record id set. Rows already projected are
    /// re-derived, and the ids whose flag flipped are returned.
    pub fn sync_restart_ids(&mut self, ids: BTreeSet<String>) -> Vec<String> {
        self.restart_ids = ids;
        let mut flipped = Vec::new();
        let ids = &self.restart_ids;
        for (id, row) in &mut self.rows {
            let restartable = row.lifecycle_class == "ended" && ids.contains(id);
            if restartable != row.restartable {
                row.restartable = restartable;
                flipped.push(id.clone());
            }
        }
        for id in &flipped {
            self.view_upsert(id);
        }
        flipped
    }

    /// One durable restart record was set or removed. Returns true when the
    /// projected row's flag flipped. A row not yet projected derives the flag
    /// when it is ingested.
    pub fn restart_record_changed(&mut self, session_id: &str, present: bool) -> bool {
        if present {
            self.restart_ids.insert(session_id.to_string());
        } else {
            self.restart_ids.remove(session_id);
        }
        let restartable = self
            .rows
            .get(session_id)
            .is_some_and(|row| self.derive_restartable(row.lifecycle_class, session_id));
        let flipped = match self.rows.get_mut(session_id) {
            Some(row) if row.restartable != restartable => {
                row.restartable = restartable;
                true
            }
            _ => false,
        };
        if flipped {
            self.view_upsert(session_id);
        }
        flipped
    }

    /// Apply one journal change. Remove is not ended evidence.
    pub fn apply_change(&mut self, change: &SessionLifecycleChange) {
        match &change.kind {
            SessionLifecycleChangeKind::Upsert { record } => {
                let lifecycle_class = session_lifecycle_class(
                    &record.session.registry_state,
                    record.lifecycle.as_ref(),
                );
                let live_ended = lifecycle_class == "ended";
                let restartable =
                    self.derive_restartable(lifecycle_class, &record.session.session_id.0);
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
                self.view_upsert(&record.session.session_id.0);
            }
            SessionLifecycleChangeKind::Removed { session_id } => {
                self.rows.remove(&session_id.0);
                if let Some(view) = &self.view.0 {
                    view.remove(&session_id.0);
                }
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
        self.ingest_baseline_rows(snapshot.sequence, records);
        self.seal_baseline(snapshot);
    }

    /// Insert baseline rows without sealing the snapshot.
    pub fn ingest_baseline_rows(
        &mut self,
        sequence: u64,
        records: impl IntoIterator<Item = SessionLifecycleRecord>,
    ) {
        for record in records {
            let lifecycle_class =
                session_lifecycle_class(&record.session.registry_state, record.lifecycle.as_ref());
            let id = record.session.session_id.0.clone();
            let restartable = self.derive_restartable(lifecycle_class, &id);
            self.rows.insert(
                id.clone(),
                SessionProjectionRow {
                    record,
                    lifecycle_class,
                    live_ended: false,
                    change_seq: sequence,
                    restartable,
                },
            );
            self.view_upsert(&id);
        }
    }

    /// Mark the assembled baseline complete. Incomplete pages stay a gap.
    pub fn seal_baseline(&mut self, snapshot: SessionLifecycleCursor) {
        self.cursor = Some(snapshot);
        self.baseline_complete = true;
        self.gap = false;
        if let Some(view) = &self.view.0 {
            view.set_sealed(true);
        }
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
        if let Some(view) = &self.view.0 {
            view.reset();
        }
        self.apply_baseline_page(snapshot, records, true);
    }

    /// Mark a gap. Later ended proof requires a complete baseline or a live ended patch.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn mark_gap(&mut self) {
        self.gap = true;
        self.baseline_complete = false;
        if let Some(view) = &self.view.0 {
            view.set_sealed(false);
        }
    }

    /// Start a fresh baseline recovery without treating current rows as complete.
    pub fn begin_baseline_recovery(&mut self) {
        self.gap = true;
        self.baseline_complete = false;
        self.rows.clear();
        self.cursor = None;
        if let Some(view) = &self.view.0 {
            view.reset();
        }
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

/// Fail if this module's source imports terminal bodies or names product policy.
#[cfg(test)]
pub fn assert_projection_source_stays_control_plane(source: &str) {
    let production = source.split("#[cfg(test)]").next().unwrap_or(source);
    for needle in [
        "botster-terminal-protocol-client",
        "botster_terminal_protocol_client",
        "ProcessExited",
        "botster-workspaces",
        "botster_workspaces",
        "membership",
        "cleanup_rule",
        "package cleanup",
    ] {
        assert!(
            !production.contains(needle),
            "session projection source must not contain {needle}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

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
    fn a_record_committed_before_its_row_is_projected_is_derived_at_ingest() {
        let mut projection = SessionProjection::default();
        assert!(!projection.restart_record_changed("late", true));
        projection.ingest_baseline_rows(1, [ended("late"), ended("other")]);
        assert!(projection.rows["late"].restartable);
        assert!(!projection.rows["other"].restartable);
        // A live change that ends a running row derives the flag too.
        projection.restart_record_changed("run", true);
        projection.apply_change(&SessionLifecycleChange {
            cursor: cursor(2),
            kind: SessionLifecycleChangeKind::Upsert {
                record: ended("run"),
            },
        });
        assert!(projection.rows["run"].restartable);
    }

    #[test]
    fn syncing_the_durable_set_rederives_rows_and_reports_flips() {
        let mut projection = SessionProjection::default();
        projection.ingest_baseline_rows(1, [ended("a"), ended("b")]);
        let flipped = projection.sync_restart_ids(BTreeSet::from(["a".to_string()]));
        assert_eq!(flipped, vec!["a".to_string()]);
        let flipped = projection.sync_restart_ids(BTreeSet::from(["b".to_string()]));
        assert_eq!(flipped, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn the_plugin_view_follows_every_projection_mutator() {
        use crate::session_view::{SessionView, SessionViewError};
        let view = Arc::new(SessionView::default());
        let mut projection = SessionProjection::default();
        projection.attach_view(&view);
        // Rows ingested before the baseline seals are not readable yet.
        projection.ingest_baseline_rows(1, [ended("a")]);
        assert_eq!(view.get("a"), Err(SessionViewError::NotReady));
        projection.seal_baseline(cursor(1));
        assert_eq!(view.get("a").unwrap().unwrap().lifecycle_class, "ended");
        // A journal upsert replaces the row; a removal drops it.
        projection.apply_change(&SessionLifecycleChange {
            cursor: cursor(2),
            kind: SessionLifecycleChangeKind::Upsert {
                record: record(
                    "a",
                    RegistrySessionState::Running,
                    Some(SessionLifecycleState::Running),
                ),
            },
        });
        assert_eq!(view.get("a").unwrap().unwrap().lifecycle_class, "current");
        projection.apply_change(&SessionLifecycleChange {
            cursor: cursor(3),
            kind: SessionLifecycleChangeKind::Removed {
                session_id: SessionId("a".to_string()),
            },
        });
        assert_eq!(view.get("a"), Ok(None));
        // A restart-record flip reaches the view without a journal change.
        projection.apply_change(&SessionLifecycleChange {
            cursor: cursor(4),
            kind: SessionLifecycleChangeKind::Upsert { record: ended("b") },
        });
        assert!(!view.get("b").unwrap().unwrap().restartable);
        projection.restart_record_changed("b", true);
        assert!(view.get("b").unwrap().unwrap().restartable);
        // Baseline recovery empties the view and makes it not ready, not empty.
        projection.begin_baseline_recovery();
        assert_eq!(view.get("b"), Err(SessionViewError::NotReady));
        assert_eq!(view.page(None), Err(SessionViewError::NotReady));
        projection.replace_complete_baseline(cursor(5), [ended("c")]);
        assert_eq!(view.get("b"), Ok(None));
        assert!(view.get("c").unwrap().is_some());
        // A gap makes the view not ready again.
        projection.mark_gap();
        assert_eq!(view.get("c"), Err(SessionViewError::NotReady));
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

    #[test]
    fn source_stays_control_plane() {
        let source = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/session_projection.rs"
        ));
        assert_projection_source_stays_control_plane(source);
        assert!(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/session_projection.rs")
                .exists()
        );
    }
}
