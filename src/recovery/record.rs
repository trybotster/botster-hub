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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordError {
    InvalidLedger,
}

impl RecoveryLedger {
    /// Validate a loaded ledger against this host before it is trusted.
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
}
