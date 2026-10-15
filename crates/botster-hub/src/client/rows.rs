//! The session rows of the client protocol (design section 7a; steward R-125, S2-HC-3).
//!
//! - [`SessionTypes`]: the session_type registry. It is in memory, built at start, and owned by the protocol layer;
//!   the owner never reads it.
//! - The Hub's label keys in Core's own session `labels`: [`LABEL_KEY`] and [`TYPE_KEY`] under the Hub prefix `h/`.
//!   A plugin namespace is under `p/<plugin_key>/` ([`plugin_label_key`]). The two prefixes are disjoint, so a
//!   collision is impossible by construction.
//! - [`session_row`]: the one builder of an HC `SessionRow`. No other code writes a product field.

use crate::watch::{Activity as ViewActivity, Row};
use botster_core::contract::prelude::{
    Exit, LostReason, SessionId, SessionState as CoreState, Size,
};
use botster_hub_contract::prelude::{
    Activity, Interaction, Presentation, SessionExit, SessionRow, SessionSize, SessionTypeRef,
    SessionTypeRow, TypeSource,
};
use botster_route_codec::prelude::SessionState;
use std::collections::BTreeMap;

/// The Hub's own prefix in Core's session labels.
pub const HUB_PREFIX: &str = "h/";
/// The session's label (R-125: the create's `label`, else the session id).
pub const LABEL_KEY: &str = "h/label";
/// The session's type id, resolved in [`SessionTypes`].
pub const TYPE_KEY: &str = "h/session_type";
/// The device type of `create(session, {command})` (S2-HC-3).
pub const COMMAND_TYPE: &str = "command";

/// The label key of a plugin namespace entry. The Hub writes the prefix, so no plugin key lands under [`HUB_PREFIX`].
pub fn plugin_label_key(plugin_key: &str, key: &str) -> String {
    format!("p/{plugin_key}/{key}")
}

/// One registered session type: its HC row and what a session of the type inherits.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionType {
    pub row: SessionTypeRow,
    pub traits: Vec<String>,
    pub role: Option<String>,
    pub interaction: Option<Interaction>,
    /// Whether `create(session, {session_type_id})` may name the type. The `command` type is the operator's command
    /// form and has no definition files, so it is not (S2-HC-3).
    pub by_id: bool,
}

/// Why `create(session, {session_type_id})` names no type it can spawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeRefused {
    /// No such type: `invalid_arguments`.
    Unknown,
    /// The type cannot be created by id (`command`): `invalid_arguments`; its `resolve` answers `available: false`.
    NotByIdentifier,
}

/// The session_type registry (design section 7a).
#[derive(Debug, Clone)]
pub struct SessionTypes {
    types: BTreeMap<String, SessionType>,
}

impl SessionTypes {
    /// The registry of Stage 2: the one device type `command` (S2-HC-3).
    pub fn device() -> SessionTypes {
        let command = SessionType {
            row: SessionTypeRow {
                id: COMMAND_TYPE.into(),
                source: TypeSource::Device,
                label: "Command".into(),
                description: "The operator's command form: create(session, {command})".into(),
                editable: false,
                targets: vec![],
                port_env: None,
                notifications: None,
            },
            traits: vec![],
            role: None,
            interaction: None,
            by_id: false,
        };
        SessionTypes {
            types: BTreeMap::from([(COMMAND_TYPE.to_string(), command)]),
        }
    }

    /// The rows that `list(session_type)` returns, by id.
    pub fn list(&self) -> Vec<SessionTypeRow> {
        self.types.values().map(|t| t.row.clone()).collect()
    }

    pub fn get(&self, id: &str) -> Option<&SessionType> {
        self.types.get(id)
    }

    /// The check of `create(session, {session_type_id})`, before `Spawn`.
    pub fn for_create(&self, id: &str) -> Result<&SessionType, TypeRefused> {
        match self.types.get(id) {
            None => Err(TypeRefused::Unknown),
            Some(t) if !t.by_id => Err(TypeRefused::NotByIdentifier),
            Some(t) => Ok(t),
        }
    }
}

/// The labels of a new session, for `SpawnRequest.labels`: the initial label (the create's `label`, else the session
/// id; R-125 as corrected) and the type id.
pub fn create_labels(
    session: &SessionId,
    label: Option<&str>,
    type_id: &str,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            LABEL_KEY.to_string(),
            label.unwrap_or(&session.0).to_string(),
        ),
        (TYPE_KEY.to_string(), type_id.to_string()),
    ])
}

/// The HC state of a Core state (codec A.2c). `None` for `Released`: the row is gone.
///
/// `Lost`: a known termination (`WorkerGone`, `StartInterrupted`) is `ended`; an unresolved adoption is `lost`. The
/// service reasons `GuardianLost` and `EpochExhausted` (Core amendment 16), an unknown reason and a state that a later
/// Core adds claim no known termination, so they are `lost` too.
pub fn hc_state(state: &CoreState) -> Option<SessionState> {
    Some(match state {
        CoreState::Created | CoreState::Starting => SessionState::Starting,
        CoreState::Running => SessionState::Running,
        CoreState::Stopping => SessionState::Stopping,
        CoreState::Exited(_) => SessionState::Ended,
        CoreState::Lost(LostReason::WorkerGone | LostReason::StartInterrupted) => {
            SessionState::Ended
        }
        CoreState::Released => return None,
        // Every other `Lost` reason, and a state that a later Core adds (the enum is non-exhaustive).
        _ => SessionState::Lost,
    })
}

/// The `failure_reason` of a session whose worker was lost with a known termination (HC A.2c).
const WORKER_LOST: &str = "worker_lost";

pub use crate::watch::SpawnEnd;

/// What the builder reads about one session: P5's view row and the spawn operation.
#[derive(Debug, Clone, Copy)]
pub struct RowFacts<'a> {
    pub id: &'a SessionId,
    pub state: &'a CoreState,
    pub size: Size,
    pub exit: Option<&'a Exit>,
    pub labels: &'a BTreeMap<String, String>,
    pub activity: Activity,
    pub spawn_end: Option<&'a SpawnEnd>,
}

impl<'a> RowFacts<'a> {
    /// The facts of one owner view row (design section 4, "The view row").
    pub fn of(row: &'a Row) -> RowFacts<'a> {
        RowFacts {
            id: &row.id,
            state: &row.state,
            size: row.size,
            exit: row.exit.as_ref(),
            labels: &row.labels,
            activity: match row.activity {
                ViewActivity::Active => Activity::Active,
                ViewActivity::Idle => Activity::Idle,
            },
            spawn_end: row.spawn_end.as_ref(),
        }
    }
}

/// The one builder of an HC `SessionRow` (design section 7a). `None` for a `Released` session.
pub fn session_row(facts: RowFacts<'_>, hub_id: &str, types: &SessionTypes) -> Option<SessionRow> {
    let mut state = hc_state(facts.state)?;
    // A Core `Created` row whose spawn failed or was cancelled shows as `ended` (R-122; section 7, item 4).
    let failure_reason = match (facts.state, facts.spawn_end) {
        (CoreState::Created, Some(end)) => {
            state = SessionState::Ended;
            match end {
                SpawnEnd::Failed { reason } => Some(reason.clone()),
                SpawnEnd::Cancelled => None,
            }
        }
        (_, Some(SpawnEnd::Failed { reason })) => Some(reason.clone()),
        // A worker lost with a known termination (A.2c; Core AD-2 `WorkerGone`, `StartInterrupted`): `worker_lost`, when
        // no more specific spawn failure applies. An unresolved adoption (`lost`) names no failure.
        (CoreState::Lost(LostReason::WorkerGone | LostReason::StartInterrupted), _) => {
            Some(WORKER_LOST.to_owned())
        }
        _ => None,
    };
    let type_id = facts
        .labels
        .get(TYPE_KEY)
        .map(String::as_str)
        .unwrap_or(COMMAND_TYPE);
    let session_type = types.get(type_id);
    Some(SessionRow {
        id: facts.id.0.clone(),
        hub_id: hub_id.into(),
        label: facts
            .labels
            .get(LABEL_KEY)
            .cloned()
            .unwrap_or_else(|| facts.id.0.clone()),
        state,
        lifecycle_class: state.lifecycle_class(),
        exit: facts
            .exit
            .map(|e| SessionExit {
                code: e.code.map(i64::from),
                signal: e.signal.map(i64::from),
            })
            .unwrap_or_default(),
        size: SessionSize {
            rows: facts.size.rows,
            cols: facts.size.cols,
        },
        session_type: SessionTypeRef {
            id: type_id.into(),
            source: session_type.map_or(TypeSource::Device, |t| t.row.source.clone()),
        },
        traits: session_type.map_or_else(Vec::new, |t| t.traits.clone()),
        restartable: false,
        activity: facts.activity,
        presentation: Presentation::Default,
        metadata: BTreeMap::new(),
        failure_reason,
        role: session_type.and_then(|t| t.role.clone()),
        interaction: session_type.and_then(|t| t.interaction),
        port: None,
        owner: None,
        worktree_id: None,
        branch: None,
        title: None,
        task: None,
        attention: None,
        extra: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core::contract::prelude::ExitCause;
    use botster_route_codec::prelude::LifecycleClass;
    use serde_json::json;

    fn sid(s: &str) -> SessionId {
        SessionId(s.into())
    }

    fn size() -> Size {
        Size {
            rows: 24,
            cols: 80,
            cell_px: None,
        }
    }

    fn facts<'a>(
        id: &'a SessionId,
        state: &'a CoreState,
        labels: &'a BTreeMap<String, String>,
    ) -> RowFacts<'a> {
        RowFacts {
            id,
            state,
            size: size(),
            exit: None,
            labels,
            activity: Activity::Idle,
            spawn_end: None,
        }
    }

    #[test]
    fn the_registry_lists_the_command_type_and_refuses_it_by_id() {
        let types = SessionTypes::device();
        let rows = serde_json::to_value(types.list()).unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["id"], "command");
        assert_eq!(rows[0]["source"], "device");
        assert_eq!(rows[0]["editable"], false);
        assert_eq!(
            types.for_create(COMMAND_TYPE),
            Err(TypeRefused::NotByIdentifier)
        );
        assert_eq!(types.for_create("agent"), Err(TypeRefused::Unknown));
    }

    #[test]
    fn the_initial_label_is_the_creates_label_else_the_session_id() {
        let id = sid("s1");
        let with = create_labels(&id, Some("build"), COMMAND_TYPE);
        assert_eq!(with[LABEL_KEY], "build");
        assert_eq!(with[TYPE_KEY], "command");
        let without = create_labels(&id, None, COMMAND_TYPE);
        assert_eq!(without[LABEL_KEY], "s1");
    }

    #[test]
    fn a_plugin_key_never_lands_under_the_hub_prefix() {
        for (plugin, key) in [("h", "label"), ("botster", "h/label"), ("", "h/x")] {
            let k = plugin_label_key(plugin, key);
            assert!(k.starts_with("p/"), "{k}");
            assert!(!k.starts_with(HUB_PREFIX), "{k}");
        }
    }

    #[test]
    fn a_command_created_row_has_every_required_field() {
        let types = SessionTypes::device();
        let id = sid("s1");
        let labels = create_labels(&id, Some("build"), COMMAND_TYPE);
        let state = CoreState::Running;
        let mut f = facts(&id, &state, &labels);
        f.activity = Activity::Active;
        let row = serde_json::to_value(session_row(f, "hub-1", &types).unwrap()).unwrap();
        assert_eq!(
            row,
            json!({
                "id": "s1", "hub_id": "hub-1", "label": "build", "state": "running",
                "lifecycle_class": "current", "exit": {}, "size": {"rows": 24, "cols": 80},
                "session_type": {"id": "command", "source": "device"}, "traits": [],
                "restartable": false, "activity": "active", "presentation": "default", "metadata": {}
            })
        );
    }

    #[test]
    fn the_state_map_follows_codec_a_2c() {
        let exit = Exit {
            code: Some(0),
            signal: None,
            cause: ExitCause::Normal,
        };
        let cases = [
            (CoreState::Created, SessionState::Starting),
            (CoreState::Starting, SessionState::Starting),
            (CoreState::Running, SessionState::Running),
            (CoreState::Stopping, SessionState::Stopping),
            (CoreState::Exited(exit), SessionState::Ended),
            (CoreState::Lost(LostReason::WorkerGone), SessionState::Ended),
            (
                CoreState::Lost(LostReason::StartInterrupted),
                SessionState::Ended,
            ),
            (
                CoreState::Lost(LostReason::WorkerUnreachable),
                SessionState::Lost,
            ),
            (
                CoreState::Lost(LostReason::WorkerVersion),
                SessionState::Lost,
            ),
            (
                CoreState::Lost(LostReason::RegistryCorrupt),
                SessionState::Lost,
            ),
            (
                CoreState::Lost(LostReason::GuardianLost),
                SessionState::Lost,
            ),
            (
                CoreState::Lost(LostReason::EpochExhausted),
                SessionState::Lost,
            ),
            (CoreState::Lost(LostReason::Other), SessionState::Lost),
        ];
        for (core, hc) in cases {
            assert_eq!(hc_state(&core), Some(hc), "{core:?}");
        }
        assert_eq!(hc_state(&CoreState::Released), None);
        let types = SessionTypes::device();
        let labels = BTreeMap::new();
        let id = sid("gone");
        assert!(session_row(facts(&id, &CoreState::Released, &labels), "h", &types).is_none());
    }

    #[test]
    fn a_failed_spawn_shows_a_created_row_as_ended_with_its_reason() {
        let types = SessionTypes::device();
        let id = sid("s1");
        let labels = create_labels(&id, None, COMMAND_TYPE);
        let end = SpawnEnd::Failed {
            reason: "internal".into(),
        };
        let mut f = facts(&id, &CoreState::Created, &labels);
        f.spawn_end = Some(&end);
        let row = session_row(f, "h", &types).unwrap();
        assert_eq!(row.state, SessionState::Ended);
        assert_eq!(row.lifecycle_class, LifecycleClass::Ended);
        assert_eq!(row.failure_reason.as_deref(), Some("internal"));
        assert_eq!(row.label, "s1");
        let cancelled = SpawnEnd::Cancelled;
        f.spawn_end = Some(&cancelled);
        let row = session_row(f, "h", &types).unwrap();
        assert_eq!(row.state, SessionState::Ended);
        assert_eq!(row.failure_reason, None);
    }

    /// A.2c: a worker lost with a known termination is `ended` with `failure_reason: worker_lost`, with no spawn record; an
    /// unresolved adoption is `lost` with no reason; a more specific spawn failure keeps its own reason.
    #[test]
    fn a_known_termination_names_worker_lost_and_an_unresolved_adoption_does_not() {
        let types = SessionTypes::device();
        let id = sid("s1");
        let labels = BTreeMap::new();
        for reason in [LostReason::WorkerGone, LostReason::StartInterrupted] {
            let state = CoreState::Lost(reason);
            let row = session_row(facts(&id, &state, &labels), "h", &types).unwrap();
            assert_eq!(row.state, SessionState::Ended, "{state:?}");
            assert_eq!(
                row.failure_reason.as_deref(),
                Some("worker_lost"),
                "{state:?}"
            );
        }
        for reason in [
            LostReason::WorkerUnreachable,
            LostReason::WorkerVersion,
            LostReason::RegistryCorrupt,
        ] {
            let state = CoreState::Lost(reason);
            let row = session_row(facts(&id, &state, &labels), "h", &types).unwrap();
            assert_eq!(row.state, SessionState::Lost, "{state:?}");
            assert_eq!(row.failure_reason, None, "{state:?}");
        }
        let state = CoreState::Lost(LostReason::WorkerGone);
        let end = SpawnEnd::Failed {
            reason: "exec_failed".into(),
        };
        let mut f = facts(&id, &state, &labels);
        f.spawn_end = Some(&end);
        let row = session_row(f, "h", &types).unwrap();
        assert_eq!(row.failure_reason.as_deref(), Some("exec_failed"));
    }

    /// The owner's view row gives the facts, and its activity maps to the contract's (R-125).
    #[test]
    fn a_view_row_gives_the_facts_of_the_builder() {
        let types = SessionTypes::device();
        let id = sid("s1");
        let mut row = Row {
            id: id.clone(),
            state: CoreState::Running,
            size: size(),
            exit: None,
            labels: create_labels(&id, Some("build"), COMMAND_TYPE),
            activity: ViewActivity::Idle,
            spawn_end: None,
        };
        let built = session_row(RowFacts::of(&row), "h", &types).unwrap();
        assert_eq!(built.activity, Activity::Idle);
        assert_eq!(built.label, "build");
        row.activity = ViewActivity::Active;
        let built = session_row(RowFacts::of(&row), "h", &types).unwrap();
        assert_eq!(built.activity, Activity::Active);
        row.spawn_end = Some(SpawnEnd::Failed {
            reason: "exec_failed".into(),
        });
        let built = session_row(RowFacts::of(&row), "h", &types).unwrap();
        assert_eq!(built.failure_reason.as_deref(), Some("exec_failed"));
    }

    #[test]
    fn the_exit_maps_code_and_signal() {
        let types = SessionTypes::device();
        let id = sid("s1");
        let labels = BTreeMap::new();
        let exit = Exit {
            code: None,
            signal: Some(9),
            cause: ExitCause::Signal,
        };
        let state = CoreState::Exited(exit);
        let mut f = facts(&id, &state, &labels);
        f.exit = Some(&exit);
        let row = serde_json::to_value(session_row(f, "h", &types).unwrap()).unwrap();
        assert_eq!(row["exit"], json!({"signal": 9}));
        assert_eq!(row["state"], "ended");
    }
}
