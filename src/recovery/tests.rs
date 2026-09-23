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
        self.store.load_retained(&self.config).unwrap().0
    }

    fn replace_initialized_document(&self, bytes: &[u8]) {
        if !self.directory.join("hub-recovery.log").exists() {
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
fn receipt_classification_preserves_pending_follow_up() {
    let fixture = Fixture::new();
    for receipt in [
        Receipt::WorktreeNeverCreated,
        Receipt::ConversionAcknowledged,
        Receipt::SessionNeverCreated,
        Receipt::SessionRemovedAndReleased,
        Receipt::WorktreeRollbackCompleted,
    ] {
        for managed_created in [None, Some(false), Some(true)] {
            if matches!(
                receipt,
                Receipt::WorktreeNeverCreated | Receipt::WorktreeRollbackCompleted
            ) && managed_created != Some(true)
            {
                continue;
            }
            let identity = managed_created.map(|created| {
                let mut identity = managed(&fixture);
                identity.created_worktree = created;
                identity.created_branch = created;
                identity
            });
            let mut ledger = RecoveryLedger::default();
            ledger
                .admit("host", "session".into(), identity, policy(1))
                .unwrap();
            let record = &mut ledger.records[0];
            record.phase = Phase::ReceiptRecorded(receipt);
            record.confirmed = match receipt {
                Receipt::WorktreeNeverCreated => ConfirmedFacts {
                    worktree_never_created: true,
                    ..ConfirmedFacts::default()
                },
                Receipt::ConversionAcknowledged => ConfirmedFacts {
                    session_installed: true,
                    conversion_acknowledged: true,
                    ..ConfirmedFacts::default()
                },
                Receipt::SessionNeverCreated => ConfirmedFacts {
                    session_never_created: true,
                    ..ConfirmedFacts::default()
                },
                Receipt::SessionRemovedAndReleased => ConfirmedFacts {
                    session_installed: true,
                    session_removed_and_released: true,
                    ..ConfirmedFacts::default()
                },
                Receipt::WorktreeRollbackCompleted => ConfirmedFacts {
                    session_never_created: true,
                    worktree_rollback_completed: true,
                    ..ConfirmedFacts::default()
                },
            };
            ledger.validate("host").unwrap();
            let pending = receipt == Receipt::ConversionAcknowledged
                || (managed_created == Some(true)
                    && matches!(
                        receipt,
                        Receipt::SessionNeverCreated | Receipt::SessionRemovedAndReleased
                    ));
            let record = &ledger.records[0];
            assert_eq!(
                classify_restart(&ledger, record, observation(record)),
                if pending {
                    RestartClassification::ReceiptRecordedFollowUpPending
                } else {
                    RestartClassification::ReceiptRecorded
                },
                "{receipt:?}, created={managed_created:?}"
            );
        }
    }
}

#[test]
fn terminal_classification_matches_absence_of_permitted_successors() {
    let fixture = Fixture::new();
    let phases = [
        Phase::Intent(Effect::CreateWorktree),
        Phase::Intent(Effect::SpawnSession),
        Phase::Intent(Effect::CleanupSession),
        Phase::Intent(Effect::RollbackWorktree),
        Phase::EffectRecorded(Effect::CreateWorktree),
        Phase::EffectRecorded(Effect::SpawnSession),
        Phase::EffectRecorded(Effect::CleanupSession),
        Phase::EffectRecorded(Effect::RollbackWorktree),
        Phase::ReceiptRecorded(Receipt::WorktreeNeverCreated),
        Phase::ReceiptRecorded(Receipt::ConversionAcknowledged),
        Phase::ReceiptRecorded(Receipt::SessionNeverCreated),
        Phase::ReceiptRecorded(Receipt::SessionRemovedAndReleased),
        Phase::ReceiptRecorded(Receipt::WorktreeRollbackCompleted),
    ];
    for managed_created in [None, Some(false), Some(true)] {
        let identity = managed_created.map(|created| {
            let mut identity = managed(&fixture);
            identity.created_worktree = created;
            identity.created_branch = created;
            identity
        });
        let mut initial = RecoveryLedger::default();
        let id = initial
            .admit("host", "session".into(), identity, policy(1))
            .unwrap();
        let mut pending = vec![initial];
        let mut visited = Vec::new();
        while let Some(ledger) = pending.pop() {
            let record = &ledger.records[0];
            if visited.contains(&record.phase) {
                continue;
            }
            visited.push(record.phase);
            ledger.validate("host").unwrap();
            let mut has_successor = false;
            for phase in phases {
                let mut next = ledger.clone();
                if next.transition(&id, "session", phase).is_ok() {
                    has_successor = true;
                    pending.push(next);
                }
            }
            assert_eq!(
                classify_restart(&ledger, record, observation(record))
                    == RestartClassification::ReceiptRecorded,
                !has_successor,
                "{:?}, created={managed_created:?}",
                record.phase
            );
        }
        if managed_created == Some(true) {
            assert_eq!(visited.len(), phases.len());
        }
    }
}

#[test]
fn interrupted_intent_reload_retains_exact_ownership() {
    let fixture = Fixture::new();
    let (state, authority) = fixture.retained_view();
    let budget = authority.budget();
    let prepared = PreparedRecoveryWrite::admit(
        &fixture.store,
        &authority,
        0,
        state.clone(),
        (*state).clone(),
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
    drop(authority);
    drop(state);
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
    let (state, authority) = fixture.retained_view();
    let identity = managed(&fixture);
    let unrelated = fixture.directory.join("unrelated");
    fs::write(&unrelated, b"keep").unwrap();
    let budget = authority.budget();
    let receipt = PreparedRecoveryWrite::admit(
        &fixture.store,
        &authority,
        0,
        state.clone(),
        (*state).clone(),
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
    let mut with_worktree = (**receipt.state()).clone();
    with_worktree.worktrees.push(crate::worktrees::Worktree {
        worktree_id: identity.worktree_id.clone(),
        target_id: identity.target_id.clone(),
        label: identity.branch.clone(),
        path: identity.path.clone(),
        status: "present".into(),
        management: "hub_managed_git".into(),
        git: None,
        metadata: std::collections::BTreeMap::from([("recovery_attempt".into(), marker)]),
    });
    let prepared = fixture
        .store
        .prepare_shared(
            &authority,
            1,
            Some(receipt.state().clone()),
            with_worktree,
            &budget,
        )
        .unwrap();
    let FileCommitOutcome::Synced {
        state: with_worktree,
        ..
    } = fixture.store.commit_shared(prepared, 1).unwrap()
    else {
        panic!("worktree fixture write must synchronize");
    };
    drop(receipt);
    drop(authority);
    drop(state);
    drop(with_worktree);
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
    let (current, authority) = fixture.retained_view();
    let mut changed = (*current).clone();
    changed.worktrees[0].metadata.insert(
        "recovery_attempt".into(),
        serde_json::to_string(&replacement).unwrap(),
    );
    let prepared = fixture
        .store
        .prepare_shared(
            &authority,
            2,
            Some(current.clone()),
            changed,
            &authority.budget(),
        )
        .unwrap();
    assert!(matches!(
        fixture.store.commit_shared(prepared, 2),
        Ok(FileCommitOutcome::Synced { .. })
    ));
    drop(authority);
    drop(current);
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
    let (state, authority) = fixture.retained_view();
    let bytes = fs::read(fixture.store.path()).unwrap();
    let budget = authority.budget();
    let prepared = PreparedRecoveryWrite::admit(
        &fixture.store,
        &authority,
        0,
        state.clone(),
        (*state).clone(),
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
    drop(authority);
    drop(state);
    assert!(matches!(
        fixture.store.load_retained(&fixture.config),
        Err(HubStateStoreError::RecoveryRequired {
            reason: "recovery_intent_unresolved",
            sequence: Some(2),
        })
    ));
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
        RestartClassification::ReceiptRecordedFollowUpPending
    );
    ledger
        .transition(&id, "session", Phase::Intent(Effect::RollbackWorktree))
        .unwrap();
    ledger
        .transition(
            &id,
            "session",
            Phase::EffectRecorded(Effect::RollbackWorktree),
        )
        .unwrap();
    ledger
        .transition(
            &id,
            "session",
            Phase::ReceiptRecorded(Receipt::WorktreeRollbackCompleted),
        )
        .unwrap();
    let record = &ledger.records[0];
    assert_eq!(
        classify_restart(&ledger, record, observation(record)),
        RestartClassification::ReceiptRecorded
    );
}

#[test]
fn effect_completion_round_trip_stays_unresolved_without_receipt() {
    let fixture = Fixture::new();
    let (state, authority) = fixture.retained_view();
    let budget = authority.budget();
    let receipt = PreparedRecoveryWrite::admit(
        &fixture.store,
        &authority,
        0,
        state.clone(),
        (*state).clone(),
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
        &authority,
        1,
        receipt.state().clone(),
        (**receipt.state()).clone(),
        &budget,
        receipt.attempt().clone(),
        "session",
        Phase::EffectRecorded(Effect::SpawnSession),
    )
    .unwrap()
    .commit(1)
    .unwrap();
    drop(receipt);
    drop(authority);
    drop(state);
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
    fixture.replace_initialized_document(&bytes);
    let loaded = fixture.state();
    assert_eq!(loaded.schema_version, 4);
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
    assert!(matches!(
        fixture.store.load_retained(&fixture.config),
        Err(HubStateStoreError::RecoveryRequired {
            reason: "recovery_intent_unresolved",
            sequence: Some(2),
        })
    ));

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
    state
        .recovery
        .admit(&state.host.id, "session".into(), None, policy(1))
        .unwrap();
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
    let (state, authority) = fixture.retained_view();
    let before = fs::read(fixture.store.path()).unwrap();
    let budget = authority.budget();
    let prepared = PreparedRecoveryWrite::admit(
        &fixture.store,
        &authority,
        0,
        state.clone(),
        (*state).clone(),
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
    drop(state);
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
        RestartClassification::ReceiptRecordedFollowUpPending
    );
}
