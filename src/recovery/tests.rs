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
fn schema_five_migrates_to_six_with_no_restart_records() {
    let fixture = Fixture::new();
    let mut value = serde_json::to_value(HubState::from_config(&fixture.config)).unwrap();
    value["schema_version"] = 5.into();
    value.as_object_mut().unwrap().remove("restart_records");
    fixture.replace_initialized_document(&serde_json::to_vec(&value).unwrap());
    let loaded = fixture.state();
    assert_eq!(loaded.schema_version, 6);
    assert!(loaded.restart_records.is_empty());
}

#[test]
fn a_schema_five_document_carrying_restart_records_is_refused() {
    let fixture = Fixture::new();
    let mut state = HubState::from_config(&fixture.config);
    state
        .restart_records
        .insert("s1".to_string(), restart_record("claude"));
    let mut value = serde_json::to_value(state).unwrap();
    value["schema_version"] = 5.into();
    fixture.reject_both_load_paths(
        &serde_json::to_vec(&value).unwrap(),
        HubStateError::UnsupportedVersion(5),
    );
}

#[test]
fn a_restart_record_survives_a_save_and_a_reload() {
    let fixture = Fixture::new();
    let (prior, authority) = fixture.retained_view();
    let mut next = (*prior).clone();
    next.restart_records
        .insert("s1".to_string(), restart_record("claude"));
    let saved = fixture
        .store
        .save_retained_startup_state(&authority, 0, Some(prior), next.clone())
        .expect("save the restart record");
    assert!(matches!(saved, FileCommitOutcome::Synced { .. }));
    drop(authority);
    let (reloaded, _authority) = fixture.store.load_retained(&fixture.config).unwrap();
    assert_eq!(reloaded.restart_records, next.restart_records);
}

fn restart_record(session_type_id: &str) -> crate::restart_records::RestartRecord {
    crate::restart_records::RestartRecord::from_request(
        session_type_id,
        &crate::session_types::SessionTypeRequest {
            target_id: Some("target-1".to_string()),
            ..Default::default()
        },
    )
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
    // Schema 3 migrates to the current schema, 6 (session restart records).
    assert_eq!(loaded.schema_version, 6);
    assert!(loaded.restart_records.is_empty());
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
