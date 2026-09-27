use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
use crate::persistence::{
    FileCommitOutcome, FileHubStateStore, HubState, HubStateAuthority, HubStateError,
    HubStateStore, HubStateStoreError,
};
use crate::shared_view::SharedView;

use super::record::*;

struct Fixture {
    directory: PathBuf,
    config: crate::config::HubConfig,
    store: FileHubStateStore,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "botster-recovery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("create isolated directory");
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(directory.clone()),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let store = FileHubStateStore::for_data_directory(&directory);
        Self {
            directory,
            config,
            store,
        }
    }

    fn state(&self) -> HubState {
        self.store.load_retained(&self.config).unwrap().0
    }

    fn replace_initialized_document(&self, bytes: &[u8]) {
        if !self.store.path().exists() {
            let _ = self.state();
        }
        fs::write(self.store.path(), bytes).expect("replace initialized test document");
    }

    fn retained_view(&self) -> (SharedView<HubState>, HubStateAuthority) {
        let (state, Some(mut authority)) = self.store.load_retained(&self.config).unwrap() else {
            panic!("File load must return authority");
        };
        let view = SharedView::from_reserved(
            state,
            authority.take_startup_charge().expect("startup charge"),
        );
        (view, authority)
    }

    fn reject_both_load_paths(&self, bytes: &[u8], expected: HubStateError) {
        self.replace_initialized_document(
            &serde_json::to_vec(&HubState::from_config(&self.config)).unwrap(),
        );
        let (prior, authority) = self.retained_view();
        self.replace_initialized_document(bytes);
        assert!(matches!(
            self.store.load_for_update(&authority, &self.config),
            Err(HubStateStoreError::State(error)) if error == expected
        ));
        drop(prior);
        drop(authority);
        assert!(matches!(
            self.store.load_retained(&self.config),
            Err(HubStateStoreError::State(error)) if error == expected
        ));
        assert_eq!(fs::read(self.store.path()).unwrap(), bytes);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).expect("remove isolated fixture only");
    }
}

fn managed(fixture: &Fixture) -> ManagedIdentity {
    ManagedIdentity {
        target_id: "target".into(),
        worktree_id: "managed:target:6272616e6368".into(),
        repository_root: fixture.directory.join("repo"),
        path: fixture.directory.join("worktree"),
        common_dir: fixture.directory.join("repo/.git"),
        branch: "branch".into(),
        base_commit: "base".into(),
        head_commit: "head".into(),
        created_worktree: true,
        created_branch: true,
    }
}

/// Append one intent row the way the Hub records a new attempt: the next
/// sequence, a worktree intent for a created worktree, a spawn intent otherwise.
fn push_intent(
    ledger: &mut RecoveryLedger,
    host_id: &str,
    session_id: &str,
    managed: Option<ManagedIdentity>,
) {
    let sequence = ledger.last_sequence + 1;
    let effect = if managed.as_ref().is_some_and(|value| value.created_worktree) {
        Effect::CreateWorktree
    } else {
        Effect::SpawnSession
    };
    ledger.records.push(RecoveryRecord {
        attempt: AttemptId {
            host_id: host_id.to_owned(),
            sequence,
        },
        session_id: session_id.to_owned(),
        managed,
        phase: Phase::Intent(effect),
        confirmed: ConfirmedFacts::default(),
    });
    ledger.last_sequence = sequence;
}

#[test]
fn schema_three_migrates_both_load_paths_without_mutating_disk() {
    let fixture = Fixture::new();
    let mut value = serde_json::to_value(HubState::from_config(&fixture.config)).unwrap();
    value["schema_version"] = 3.into();
    value["session_type_generation"] = 71.into();
    value.as_object_mut().unwrap().remove("recovery");
    let bytes = serde_json::to_vec(&value).unwrap();
    fixture.replace_initialized_document(&bytes);
    let loaded = fixture.state();
    // Schema 3 migrates to the current schema, 5 (durable package quarantine).
    assert_eq!(loaded.schema_version, 5);
    assert_eq!(loaded.session_type_generation, 71);
    assert_eq!(loaded.recovery, RecoveryLedger::default());
    let (prior, authority) = fixture.retained_view();
    assert_eq!(
        fixture
            .store
            .load_for_update(&authority, &fixture.config)
            .unwrap(),
        loaded
    );
    assert_eq!(fs::read(fixture.store.path()).unwrap(), bytes);
    FileHubStateStore::inject_next_save_failure(&fixture.directory);
    assert!(
        fixture
            .store
            .save_retained_startup_state(&authority, 0, Some(prior.clone()), loaded.clone())
            .is_err()
    );
    assert_eq!(fs::read(fixture.store.path()).unwrap(), bytes);
    drop(prior);
    drop(authority);
    // Nothing was renamed, so a restart loads the unchanged document.
    assert!(fixture.store.load_retained(&fixture.config).is_ok());

    let successful = Fixture::new();
    successful.replace_initialized_document(&bytes);
    let (prior, authority) = successful.retained_view();
    assert!(matches!(
        successful
            .store
            .save_retained_startup_state(&authority, 0, Some(prior), loaded.clone()),
        Ok(FileCommitOutcome::Synced { .. })
    ));
    drop(authority);
    assert_eq!(successful.state(), loaded);
}

#[test]
fn ambiguous_old_recovery_and_future_schema_fail_closed() {
    let fixture = Fixture::new();
    let mut state = HubState::from_config(&fixture.config);
    let host_id = state.host.id.clone();
    push_intent(&mut state.recovery, &host_id, "session", None);
    state.schema_version = 3;
    let bytes = serde_json::to_vec(&state).unwrap();
    fixture.reject_both_load_paths(&bytes, HubStateError::InvalidRecoveryState);
    let mut missing = serde_json::to_value(HubState::from_config(&fixture.config)).unwrap();
    missing.as_object_mut().unwrap().remove("recovery");
    let missing_bytes = serde_json::to_vec(&missing).unwrap();
    fixture.reject_both_load_paths(&missing_bytes, HubStateError::InvalidRecoveryState);
    state.schema_version = 4;
    state.recovery.records[0].phase = Phase::ReceiptRecorded(Receipt::ConversionAcknowledged);
    let contradictory_bytes = serde_json::to_vec(&state).unwrap();
    fixture.reject_both_load_paths(&contradictory_bytes, HubStateError::InvalidRecoveryState);
    fixture.reject_both_load_paths(
        b"{\"schema_version\":99}",
        HubStateError::UnsupportedVersion(99),
    );
}

#[test]
fn recovery_clone_walk_counts_every_owned_field() {
    let fixture = Fixture::new();
    let mut state = HubState::from_config(&fixture.config);
    let before = crate::hub_state_heap::walk_hub_state(&state);
    let identity = managed(&fixture);
    let strings = state.host.id.len()
        + "session".len()
        + identity.target_id.len()
        + identity.worktree_id.len()
        + identity.repository_root.as_os_str().len()
        + identity.path.as_os_str().len()
        + identity.common_dir.as_os_str().len()
        + identity.branch.len()
        + identity.base_commit.len()
        + identity.head_commit.len();
    let host_id = state.host.id.clone();
    push_intent(&mut state.recovery, &host_id, "session", Some(identity));
    let after = crate::hub_state_heap::walk_hub_state(&state);
    assert_eq!(after.string_heaps - before.string_heaps, strings);
    assert_eq!(
        after.vec_slots - before.vec_slots,
        std::mem::size_of::<RecoveryRecord>()
    );
    assert_eq!(
        after.clone_heap - before.clone_heap,
        strings + std::mem::size_of::<RecoveryRecord>()
    );
    assert!(crate::hub_state_heap::admitted_pretty(&state).unwrap() > strings);
}
