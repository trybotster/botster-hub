use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
use crate::persistence::{
    FileHubStateStore, HubState, HubStateError, HubStateStore, HubStateStoreError,
};
use crate::shared_view::SharedViewBudget;

use super::record::*;
use super::store::{PreparedRecoveryWrite, RecoveryCommitError};

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
        self.store.load_or_initialize(&self.config).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).expect("remove isolated fixture only");
    }
}

fn policy(max_records: usize) -> RecoveryPolicy {
    RecoveryPolicy {
        max_records,
        retention: RetentionPolicy::PreserveAll,
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

fn observation(record: &RecoveryRecord) -> RestartObservation<'_> {
    RestartObservation {
        state_source: StateSource::Loaded,
        minimum_sequence: record.attempt.sequence,
        attempt: &record.attempt,
        session_id: &record.session_id,
        managed: record.managed.as_ref(),
        marker: Some(&record.attempt),
    }
}

#[test]
fn interrupted_intent_reload_retains_exact_ownership() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let budget = SharedViewBudget::with_capacity(1024 * 1024);
    let prepared = PreparedRecoveryWrite::admit(
        &fixture.store,
        0,
        state,
        &budget,
        "session-a".into(),
        None,
        policy(4),
    )
    .unwrap();
    let receipt = prepared.commit(0).unwrap();
    assert_eq!(receipt.phase(), Phase::Intent(Effect::SpawnSession));
    assert_eq!(
        receipt.attempt(),
        &receipt.state().recovery.records[0].attempt
    );
    assert_eq!(receipt.committed_revision(), 1);
    drop(receipt);
    let reopened = fixture.state();
    let record = &reopened.recovery.records[0];
    assert_eq!(
        classify_restart(&reopened.recovery, record, observation(record)),
        RestartClassification::IntentRecordedNoCompletion
    );
    assert_eq!(record.session_id, "session-a");
    assert_eq!(reopened.recovery.last_sequence, 1);
}

#[test]
fn interrupted_effect_preserves_unrelated_resource_and_reports_marker_mismatch() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let identity = managed(&fixture);
    let unrelated = fixture.directory.join("unrelated");
    fs::write(&unrelated, b"keep").unwrap();
    let budget = SharedViewBudget::with_capacity(1024 * 1024);
    let receipt = PreparedRecoveryWrite::admit(
        &fixture.store,
        0,
        state,
        &budget,
        "session-a".into(),
        Some(identity.clone()),
        policy(4),
    )
    .unwrap()
    .commit(0)
    .unwrap();
    // Simulate an effect after commit, then lose the completion before its write.
    fs::create_dir(&identity.path).unwrap();
    let marker = serde_json::to_string(receipt.attempt()).unwrap();
    fixture
        .store
        .update_test_fixture(&fixture.config, |state| {
            state.worktrees.push(crate::worktrees::Worktree {
                worktree_id: identity.worktree_id.clone(),
                target_id: identity.target_id.clone(),
                label: identity.branch.clone(),
                path: identity.path.clone(),
                status: "present".into(),
                management: "hub_managed_git".into(),
                git: None,
                metadata: std::collections::BTreeMap::from([("recovery_attempt".into(), marker)]),
            });
        })
        .unwrap();
    drop(receipt);
    let reopened = fixture.state();
    let record = &reopened.recovery.records[0];
    assert_eq!(
        classify_restart(&reopened.recovery, record, observation(record)),
        RestartClassification::IntentRecordedNoCompletion
    );
    let replacement = AttemptId {
        host_id: record.attempt.host_id.clone(),
        sequence: 99,
    };
    fixture
        .store
        .update_test_fixture(&fixture.config, |state| {
            state.worktrees[0].metadata.insert(
                "recovery_attempt".into(),
                serde_json::to_string(&replacement).unwrap(),
            );
        })
        .unwrap();
    let replaced = fixture.state();
    let disk_marker: AttemptId =
        serde_json::from_str(&replaced.worktrees[0].metadata["recovery_attempt"]).unwrap();
    let mut seen = observation(record);
    seen.marker = Some(&disk_marker);
    assert_eq!(
        classify_restart(&reopened.recovery, record, seen),
        RestartClassification::IdentityMismatch
    );
    assert_eq!(fs::read(&unrelated).unwrap(), b"keep");
    assert!(identity.path.exists());
    assert_eq!(fs::read_dir(&identity.path).unwrap().count(), 0);
}

#[test]
fn failed_write_returns_no_receipt_and_preserves_committed_bytes() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let bytes = fs::read(fixture.store.path()).unwrap();
    let budget = SharedViewBudget::with_capacity(1024 * 1024);
    let prepared = PreparedRecoveryWrite::admit(
        &fixture.store,
        0,
        state,
        &budget,
        "session-a".into(),
        None,
        policy(4),
    )
    .unwrap();
    FileHubStateStore::inject_next_save_failure(&fixture.directory);
    let mut effects = 0;
    let result = prepared.commit(0).map(|_| {
        effects += 1;
    });
    assert!(matches!(
        result,
        Err(RecoveryCommitError::Write {
            error: HubStateStoreError::InjectedWriteFailure,
            ..
        })
    ));
    assert_eq!(effects, 0);
    assert_eq!(fs::read(fixture.store.path()).unwrap(), bytes);
    assert!(fixture.state().recovery.records.is_empty());
}

#[test]
fn exact_receipts_gate_transitions_and_restart_does_not_authorize_rollback() {
    let fixture = Fixture::new();
    let mut ledger = RecoveryLedger::default();
    let id = ledger
        .admit("host", "session".into(), Some(managed(&fixture)), policy(4))
        .unwrap();
    ledger
        .transition(
            &id,
            "session",
            Phase::EffectRecorded(Effect::CreateWorktree),
        )
        .unwrap();
    ledger
        .transition(&id, "session", Phase::Intent(Effect::SpawnSession))
        .unwrap();
    let previous = ledger.clone();
    assert_eq!(
        ledger.transition(&id, "other", Phase::EffectRecorded(Effect::SpawnSession)),
        Err(RecordError::IdentityMismatch)
    );
    assert_eq!(
        ledger.transition(&id, "session", Phase::Intent(Effect::RollbackWorktree)),
        Err(RecordError::InvalidTransition)
    );
    assert_eq!(ledger, previous);
    ledger
        .transition(&id, "session", Phase::EffectRecorded(Effect::SpawnSession))
        .unwrap();
    ledger
        .transition(&id, "session", Phase::Intent(Effect::CleanupSession))
        .unwrap();
    let record = &ledger.records[0];
    assert!(record.confirmed.worktree_created);
    assert!(record.confirmed.session_installed);
    assert!(!record.confirmed.session_removed_and_released);
    assert_eq!(
        classify_restart(&ledger, record, observation(record)),
        RestartClassification::IntentRecordedNoCompletion
    );
    ledger
        .transition(
            &id,
            "session",
            Phase::ReceiptRecorded(Receipt::SessionRemovedAndReleased),
        )
        .unwrap();
    let record = &ledger.records[0];
    assert_eq!(
        classify_restart(&ledger, record, observation(record)),
        RestartClassification::ReceiptRecorded
    );
    ledger
        .transition(&id, "session", Phase::Intent(Effect::RollbackWorktree))
        .unwrap();
}

#[test]
fn effect_completion_round_trip_stays_unresolved_without_receipt() {
    let fixture = Fixture::new();
    let budget = SharedViewBudget::with_capacity(1024 * 1024);
    let receipt = PreparedRecoveryWrite::admit(
        &fixture.store,
        0,
        fixture.state(),
        &budget,
        "session".into(),
        None,
        policy(4),
    )
    .unwrap()
    .commit(0)
    .unwrap();
    PreparedRecoveryWrite::transition(
        &fixture.store,
        1,
        (**receipt.state()).clone(),
        &budget,
        receipt.attempt().clone(),
        "session",
        Phase::EffectRecorded(Effect::SpawnSession),
    )
    .unwrap()
    .commit(1)
    .unwrap();
    let state = fixture.state();
    let record = &state.recovery.records[0];
    assert_eq!(
        classify_restart(&state.recovery, record, observation(record)),
        RestartClassification::EffectRecordedNoReceipt
    );
}

#[test]
fn capacity_and_overflow_refuse_without_eviction_or_reuse() {
    let mut ledger = RecoveryLedger::default();
    let first = ledger
        .admit("host", "first".into(), None, policy(1))
        .unwrap();
    let previous = ledger.clone();
    assert_eq!(
        ledger.admit("host", "second".into(), None, policy(1)),
        Err(RecordError::Capacity)
    );
    assert_eq!(ledger, previous);
    // Simulate a future authorized retirement. No production retirement API exists.
    ledger.records.clear();
    let second = ledger
        .admit("host", "second".into(), None, policy(1))
        .unwrap();
    assert!(second.sequence > first.sequence);
    ledger.records.clear();
    ledger.last_sequence = u64::MAX;
    assert_eq!(
        ledger.admit("host", "third".into(), None, policy(1)),
        Err(RecordError::SequenceExhausted)
    );
    assert!(ledger.records.is_empty());
    assert_eq!(ledger.last_sequence, u64::MAX);
}

#[test]
fn initialized_or_regressed_ledger_never_proves_noncreation() {
    let mut ledger = RecoveryLedger::default();
    ledger
        .admit("host", "session".into(), None, policy(1))
        .unwrap();
    let record = &ledger.records[0];
    let mut seen = observation(record);
    seen.state_source = StateSource::Initialized;
    assert_eq!(
        classify_restart(&ledger, record, seen),
        RestartClassification::LedgerReset
    );
    let mut seen = observation(record);
    seen.minimum_sequence = ledger.last_sequence + 1;
    assert_eq!(
        classify_restart(&ledger, record, seen),
        RestartClassification::LedgerReset
    );
}

#[test]
fn schema_three_migrates_both_load_paths_without_mutating_disk() {
    let fixture = Fixture::new();
    let mut value = serde_json::to_value(HubState::from_config(&fixture.config)).unwrap();
    value["schema_version"] = 3.into();
    value["session_type_generation"] = 71.into();
    value.as_object_mut().unwrap().remove("recovery");
    let bytes = serde_json::to_vec(&value).unwrap();
    fs::write(fixture.store.path(), &bytes).unwrap();
    let loaded = fixture.state();
    assert_eq!(loaded.schema_version, 4);
    assert_eq!(loaded.session_type_generation, 71);
    assert_eq!(loaded.recovery, RecoveryLedger::default());
    assert_eq!(
        fixture.store.load_for_update(&fixture.config).unwrap(),
        loaded
    );
    assert_eq!(fs::read(fixture.store.path()).unwrap(), bytes);
    FileHubStateStore::inject_next_save_failure(&fixture.directory);
    assert!(fixture.store.save_exclusive_startup_state(&loaded).is_err());
    assert_eq!(fs::read(fixture.store.path()).unwrap(), bytes);
    fixture.store.save_exclusive_startup_state(&loaded).unwrap();
    assert_eq!(fixture.state(), loaded);
}

#[test]
fn ambiguous_old_recovery_and_future_schema_fail_closed() {
    let fixture = Fixture::new();
    let mut state = HubState::from_config(&fixture.config);
    state
        .recovery
        .admit(&state.host.id, "session".into(), None, policy(1))
        .unwrap();
    state.schema_version = 3;
    let bytes = serde_json::to_vec(&state).unwrap();
    fs::write(fixture.store.path(), &bytes).unwrap();
    assert!(matches!(
        fixture.store.load_or_initialize(&fixture.config),
        Err(HubStateStoreError::State(
            HubStateError::InvalidRecoveryState
        ))
    ));
    assert!(fixture.store.load_for_update(&fixture.config).is_err());
    assert_eq!(fs::read(fixture.store.path()).unwrap(), bytes);
    let mut missing = serde_json::to_value(HubState::from_config(&fixture.config)).unwrap();
    missing.as_object_mut().unwrap().remove("recovery");
    let missing_bytes = serde_json::to_vec(&missing).unwrap();
    fs::write(fixture.store.path(), &missing_bytes).unwrap();
    assert!(matches!(
        fixture.store.load_for_update(&fixture.config),
        Err(HubStateStoreError::State(
            HubStateError::InvalidRecoveryState
        ))
    ));
    assert!(fixture.store.load_or_initialize(&fixture.config).is_err());
    assert_eq!(fs::read(fixture.store.path()).unwrap(), missing_bytes);
    state.schema_version = 4;
    state.recovery.records[0].phase = Phase::ReceiptRecorded(Receipt::ConversionAcknowledged);
    let contradictory_bytes = serde_json::to_vec(&state).unwrap();
    fs::write(fixture.store.path(), &contradictory_bytes).unwrap();
    assert!(matches!(
        fixture.store.load_for_update(&fixture.config),
        Err(HubStateStoreError::State(
            HubStateError::InvalidRecoveryState
        ))
    ));
    assert!(fixture.store.load_or_initialize(&fixture.config).is_err());
    assert_eq!(fs::read(fixture.store.path()).unwrap(), contradictory_bytes);
    fs::write(fixture.store.path(), b"{\"schema_version\":99}").unwrap();
    assert!(matches!(
        fixture.store.load_for_update(&fixture.config),
        Err(HubStateStoreError::State(
            HubStateError::UnsupportedVersion(99)
        ))
    ));
    assert_eq!(
        fs::read(fixture.store.path()).unwrap(),
        b"{\"schema_version\":99}"
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
    state
        .recovery
        .admit(&state.host.id, "session".into(), Some(identity), policy(1))
        .unwrap();
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

#[test]
fn stale_preparation_returns_ownership_without_changing_disk() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fs::read(fixture.store.path()).unwrap();
    let budget = SharedViewBudget::with_capacity(1024 * 1024);
    let prepared = PreparedRecoveryWrite::admit(
        &fixture.store,
        0,
        state,
        &budget,
        "session".into(),
        None,
        policy(1),
    )
    .unwrap();
    let returned = match prepared.commit(1) {
        Err(RecoveryCommitError::Stale(prepared)) => prepared,
        _ => panic!("stale preparation must return to its owner"),
    };
    assert_eq!(fs::read(fixture.store.path()).unwrap(), before);
    assert!(budget.used() > 0);
    drop(returned);
    assert_eq!(budget.used(), 0);
}

#[test]
fn noncreation_receipt_preserves_record_without_banning_identity_reuse() {
    let fixture = Fixture::new();
    let mut ledger = RecoveryLedger::default();
    let id = ledger
        .admit("host", "session".into(), Some(managed(&fixture)), policy(4))
        .unwrap();
    assert_eq!(
        ledger.admit("host", "session".into(), Some(managed(&fixture)), policy(4)),
        Err(RecordError::IdentityConflict)
    );
    ledger
        .transition(
            &id,
            "session",
            Phase::ReceiptRecorded(Receipt::WorktreeNeverCreated),
        )
        .unwrap();
    assert!(ledger.records[0].confirmed.worktree_never_created);
    let next = ledger
        .admit("host", "session".into(), Some(managed(&fixture)), policy(4))
        .unwrap();
    assert!(next.sequence > id.sequence);
    assert_eq!(ledger.records.len(), 2);
    ledger
        .transition(
            &next,
            "session",
            Phase::EffectRecorded(Effect::CreateWorktree),
        )
        .unwrap();
    ledger
        .transition(
            &next,
            "session",
            Phase::ReceiptRecorded(Receipt::SessionNeverCreated),
        )
        .unwrap();
    ledger
        .transition(&next, "session", Phase::Intent(Effect::RollbackWorktree))
        .unwrap();
    assert!(ledger.records[1].confirmed.session_never_created);
}

#[test]
fn confirmed_session_head_change_does_not_erase_receipt_classification() {
    let fixture = Fixture::new();
    let mut ledger = RecoveryLedger::default();
    let mut identity = managed(&fixture);
    identity.created_worktree = false;
    identity.created_branch = false;
    let id = ledger
        .admit("host", "session".into(), Some(identity.clone()), policy(1))
        .unwrap();
    let mut changed = identity.clone();
    changed.head_commit = "new-head".into();
    let record = &ledger.records[0];
    let mut seen = observation(record);
    seen.managed = Some(&changed);
    assert_eq!(
        classify_restart(&ledger, record, seen),
        RestartClassification::IdentityMismatch
    );
    ledger
        .transition(&id, "session", Phase::EffectRecorded(Effect::SpawnSession))
        .unwrap();
    ledger
        .transition(
            &id,
            "session",
            Phase::ReceiptRecorded(Receipt::ConversionAcknowledged),
        )
        .unwrap();
    let record = &ledger.records[0];
    let mut seen = observation(record);
    seen.managed = Some(&changed);
    assert_eq!(
        classify_restart(&ledger, record, seen),
        RestartClassification::ReceiptRecorded
    );
}
