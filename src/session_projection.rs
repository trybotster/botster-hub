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
        }
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
                self.rows.insert(
                    record.session.session_id.0.clone(),
                    SessionProjectionRow {
                        record: record.clone(),
                        lifecycle_class,
                        live_ended,
                        change_seq: change.cursor.sequence,
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
            self.rows.insert(
                record.session.session_id.0.clone(),
                SessionProjectionRow {
                    record,
                    lifecycle_class,
                    live_ended: false,
                    change_seq: sequence,
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

fn session_lifecycle_class(
    registry_state: &RegistrySessionState,
    lifecycle: Option<&SessionLifecycleState>,
) -> &'static str {
    if registry_state == &RegistrySessionState::Stale {
        "indeterminate"
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
