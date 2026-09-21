//! Durable intent and evidence for one exact Hub operation.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Hub identity plus a sequence committed before the operation starts.
///
/// This identity assumes the existing single writer. Restored state and
/// concurrent Hub processes require reconciliation before further admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AttemptId {
    pub(crate) host_id: String,
    pub(crate) sequence: u64,
}

/// Exact managed artifact descriptor. The Worktree row remains the registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManagedIdentity {
    pub(crate) target_id: String,
    pub(crate) worktree_id: String,
    pub(crate) repository_root: PathBuf,
    pub(crate) path: PathBuf,
    pub(crate) common_dir: PathBuf,
    pub(crate) branch: String,
    pub(crate) base_commit: String,
    pub(crate) head_commit: String,
    pub(crate) created_worktree: bool,
    pub(crate) created_branch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Effect {
    CreateWorktree,
    SpawnSession,
    CleanupSession,
    RollbackWorktree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Intent(Effect),
    EffectRecorded(Effect),
    ReceiptRecorded(Receipt),
}

/// Facts supplied by the exact live operation owner, not restart observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Receipt {
    /// The owner proved that neither a worktree nor an owned branch was created.
    WorktreeNeverCreated,
    ConversionAcknowledged,
    SessionNeverCreated,
    SessionRemovedAndReleased,
    WorktreeRollbackCompleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryRecord {
    pub(crate) attempt: AttemptId,
    pub(crate) session_id: String,
    pub(crate) managed: Option<ManagedIdentity>,
    pub(crate) phase: Phase,
    pub(crate) confirmed: ConfirmedFacts,
}

/// Completed facts survive later intents. A cleanup intent is not cleanup proof.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ConfirmedFacts {
    pub(crate) worktree_never_created: bool,
    pub(crate) worktree_created: bool,
    pub(crate) session_installed: bool,
    pub(crate) conversion_acknowledged: bool,
    pub(crate) session_never_created: bool,
    pub(crate) session_removed_and_released: bool,
    pub(crate) worktree_rollback_completed: bool,
}

/// The counter survives record retirement. This checkpoint does not retire rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecoveryLedger {
    pub(crate) last_sequence: u64,
    pub(crate) records: Vec<RecoveryRecord>,
}

/// Explicit isolated policy inputs. There is no production default.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecoveryPolicy {
    pub(crate) max_records: usize,
    pub(crate) retention: RetentionPolicy,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RetentionPolicy {
    PreserveAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordError {
    Capacity,
    SequenceExhausted,
    InvalidIdentity,
    IdentityConflict,
    MissingRecord,
    IdentityMismatch,
    InvalidTransition,
    InvalidLedger,
}

impl RecoveryLedger {
    /// Validate disk state before admission or reconciliation.
    pub(crate) fn validate(&self, host_id: &str) -> Result<(), RecordError> {
        let mut previous = 0;
        for record in &self.records {
            if record.attempt.host_id != host_id
                || record.attempt.sequence <= previous
                || record.attempt.sequence > self.last_sequence
                || record.session_id.is_empty()
                || !record.valid_shape()
            {
                return Err(RecordError::InvalidLedger);
            }
            previous = record.attempt.sequence;
        }
        Ok(())
    }

    /// Build an intent candidate. Only a successful store commit permits effects.
    pub(crate) fn admit(
        &mut self,
        host_id: &str,
        session_id: String,
        managed: Option<ManagedIdentity>,
        policy: RecoveryPolicy,
    ) -> Result<AttemptId, RecordError> {
        self.validate(host_id)?;
        if host_id.is_empty() || session_id.is_empty() {
            return Err(RecordError::InvalidIdentity);
        }
        let RetentionPolicy::PreserveAll = policy.retention;
        if self.records.len() >= policy.max_records {
            return Err(RecordError::Capacity);
        }
        if self.records.iter().any(|record| {
            matches!(record.phase, Phase::Intent(_) | Phase::EffectRecorded(_))
                && (record.session_id == session_id
                    || managed.as_ref().is_some_and(|candidate| {
                        record.managed.as_ref().is_some_and(|existing| {
                            existing.worktree_id == candidate.worktree_id
                                || existing.path == candidate.path
                                || (existing.common_dir == candidate.common_dir
                                    && existing.branch == candidate.branch)
                        })
                    }))
        }) {
            return Err(RecordError::IdentityConflict);
        }
        let sequence = self
            .last_sequence
            .checked_add(1)
            .ok_or(RecordError::SequenceExhausted)?;
        let attempt = AttemptId {
            host_id: host_id.to_owned(),
            sequence,
        };
        let effect = if managed.as_ref().is_some_and(|value| value.created_worktree) {
            Effect::CreateWorktree
        } else {
            Effect::SpawnSession
        };
        let record = RecoveryRecord {
            attempt: attempt.clone(),
            session_id,
            managed,
            phase: Phase::Intent(effect),
            confirmed: ConfirmedFacts::default(),
        };
        if !record.valid_shape() {
            return Err(RecordError::InvalidIdentity);
        }
        self.records.push(record);
        self.last_sequence = sequence;
        Ok(attempt)
    }

    /// Apply evidence only to the matching durable attempt and session.
    pub(crate) fn transition(
        &mut self,
        attempt: &AttemptId,
        session_id: &str,
        next: Phase,
    ) -> Result<(), RecordError> {
        let record = self
            .records
            .iter_mut()
            .find(|record| record.attempt == *attempt)
            .ok_or(RecordError::MissingRecord)?;
        if record.session_id != session_id {
            return Err(RecordError::IdentityMismatch);
        }
        if !record.permits(next) {
            return Err(RecordError::InvalidTransition);
        }
        record.phase = next;
        match next {
            Phase::ReceiptRecorded(Receipt::WorktreeNeverCreated) => {
                record.confirmed.worktree_never_created = true;
            }
            Phase::EffectRecorded(Effect::CreateWorktree) => {
                record.confirmed.worktree_created = true
            }
            Phase::EffectRecorded(Effect::SpawnSession) => {
                record.confirmed.session_installed = true
            }
            Phase::ReceiptRecorded(Receipt::ConversionAcknowledged) => {
                record.confirmed.conversion_acknowledged = true
            }
            Phase::ReceiptRecorded(Receipt::SessionNeverCreated) => {
                record.confirmed.session_never_created = true
            }
            Phase::ReceiptRecorded(Receipt::SessionRemovedAndReleased) => {
                record.confirmed.session_removed_and_released = true
            }
            Phase::ReceiptRecorded(Receipt::WorktreeRollbackCompleted) => {
                record.confirmed.worktree_rollback_completed = true
            }
            _ => {}
        }
        Ok(())
    }
}

impl RecoveryRecord {
    fn valid_shape(&self) -> bool {
        let phase_matches_facts = match self.phase {
            Phase::ReceiptRecorded(Receipt::WorktreeNeverCreated) => {
                self.confirmed.worktree_never_created
            }
            Phase::EffectRecorded(Effect::CreateWorktree) => self.confirmed.worktree_created,
            Phase::EffectRecorded(Effect::SpawnSession) => self.confirmed.session_installed,
            Phase::ReceiptRecorded(Receipt::ConversionAcknowledged) => {
                self.confirmed.conversion_acknowledged
            }
            Phase::ReceiptRecorded(Receipt::SessionNeverCreated) => {
                self.confirmed.session_never_created
            }
            Phase::ReceiptRecorded(Receipt::SessionRemovedAndReleased) => {
                self.confirmed.session_removed_and_released
            }
            Phase::ReceiptRecorded(Receipt::WorktreeRollbackCompleted) => {
                self.confirmed.worktree_rollback_completed
            }
            _ => true,
        };
        if !phase_matches_facts {
            return false;
        }
        if (self.confirmed.worktree_never_created && self.confirmed.worktree_created)
            || (self.confirmed.session_never_created && self.confirmed.session_installed)
            || (self.confirmed.conversion_acknowledged && !self.confirmed.session_installed)
            || (self.confirmed.worktree_rollback_completed
                && !self.confirmed.session_never_created
                && !self.confirmed.session_removed_and_released)
        {
            return false;
        }
        if let Some(identity) = &self.managed
            && (identity.target_id.is_empty()
                || identity.worktree_id.is_empty()
                || identity.branch.is_empty()
                || identity.base_commit.is_empty()
                || identity.head_commit.is_empty()
                || !identity.path.is_absolute()
                || !identity.repository_root.is_absolute()
                || !identity.common_dir.is_absolute()
                || (identity.created_branch && !identity.created_worktree))
        {
            return false;
        }
        match self.phase {
            Phase::Intent(Effect::CreateWorktree | Effect::RollbackWorktree)
            | Phase::EffectRecorded(Effect::CreateWorktree | Effect::RollbackWorktree)
            | Phase::ReceiptRecorded(
                Receipt::WorktreeNeverCreated | Receipt::WorktreeRollbackCompleted,
            ) => self
                .managed
                .as_ref()
                .is_some_and(|value| value.created_worktree),
            _ => true,
        }
    }

    fn permits(&self, next: Phase) -> bool {
        use Effect::{CleanupSession, CreateWorktree, RollbackWorktree, SpawnSession};
        use Phase::{EffectRecorded, Intent, ReceiptRecorded};
        use Receipt::{
            ConversionAcknowledged, SessionNeverCreated, SessionRemovedAndReleased,
            WorktreeNeverCreated, WorktreeRollbackCompleted,
        };
        match (self.phase, next) {
            (Intent(CreateWorktree), ReceiptRecorded(WorktreeNeverCreated)) => true,
            (Intent(effect), EffectRecorded(completed)) => effect == completed,
            (EffectRecorded(CreateWorktree), Intent(SpawnSession)) => true,
            (
                Intent(SpawnSession) | EffectRecorded(CreateWorktree),
                ReceiptRecorded(SessionNeverCreated),
            ) => true,
            (EffectRecorded(SpawnSession), ReceiptRecorded(ConversionAcknowledged)) => true,
            (
                Intent(SpawnSession)
                | EffectRecorded(SpawnSession)
                | ReceiptRecorded(ConversionAcknowledged),
                Intent(CleanupSession),
            ) => true,
            (
                Intent(CleanupSession) | EffectRecorded(CleanupSession),
                ReceiptRecorded(SessionRemovedAndReleased),
            ) => true,
            (
                ReceiptRecorded(SessionNeverCreated | SessionRemovedAndReleased),
                Intent(RollbackWorktree),
            ) => self
                .managed
                .as_ref()
                .is_some_and(|value| value.created_worktree),
            (
                Intent(RollbackWorktree) | EffectRecorded(RollbackWorktree),
                ReceiptRecorded(WorktreeRollbackCompleted),
            ) => true,
            _ => false,
        }
    }
}

/// Observations join existing startup discovery with the Worktree registry.
/// The marker comes from registry metadata, never a file inside the worktree.
pub(crate) struct RestartObservation<'a> {
    pub(crate) state_source: StateSource,
    pub(crate) minimum_sequence: u64,
    pub(crate) attempt: &'a AttemptId,
    pub(crate) session_id: &'a str,
    pub(crate) managed: Option<&'a ManagedIdentity>,
    pub(crate) marker: Option<&'a AttemptId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateSource {
    Loaded,
    Initialized,
}

/// These classifications never authorize cleanup or rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestartClassification {
    IntentRecordedNoCompletion,
    EffectRecordedNoReceipt,
    ReceiptRecorded,
    IdentityMismatch,
    LedgerReset,
}

pub(crate) fn classify_restart(
    ledger: &RecoveryLedger,
    record: &RecoveryRecord,
    observation: RestartObservation<'_>,
) -> RestartClassification {
    if observation.state_source == StateSource::Initialized
        || ledger.last_sequence < observation.minimum_sequence
        || record.attempt.sequence > ledger.last_sequence
    {
        return RestartClassification::LedgerReset;
    }
    if ledger.validate(&record.attempt.host_id).is_err()
        || !ledger.records.contains(record)
        || observation.attempt != &record.attempt
        || observation.session_id != record.session_id
        || !managed_matches(record, observation.managed)
        || (record.managed.is_some() && observation.marker != Some(&record.attempt))
    {
        return RestartClassification::IdentityMismatch;
    }
    match record.phase {
        Phase::Intent(_) => RestartClassification::IntentRecordedNoCompletion,
        Phase::EffectRecorded(_) => RestartClassification::EffectRecordedNoReceipt,
        Phase::ReceiptRecorded(_) => RestartClassification::ReceiptRecorded,
    }
}

fn managed_matches(record: &RecoveryRecord, observed: Option<&ManagedIdentity>) -> bool {
    match (record.managed.as_ref(), observed) {
        (None, None) => true,
        (Some(expected), Some(observed)) => {
            expected.target_id == observed.target_id
                && expected.worktree_id == observed.worktree_id
                && expected.repository_root == observed.repository_root
                && expected.path == observed.path
                && expected.common_dir == observed.common_dir
                && expected.branch == observed.branch
                && expected.base_commit == observed.base_commit
                // A confirmed session can advance HEAD. This is classification,
                // not permission to roll back the changed worktree.
                && (record.confirmed.session_installed || expected.head_commit == observed.head_commit)
                && expected.created_worktree == observed.created_worktree
                && expected.created_branch == observed.created_branch
        }
        _ => false,
    }
}
