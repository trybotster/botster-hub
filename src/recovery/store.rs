//! Preparation for the existing Hub document writer.
//!
//! Host workers call these methods under the existing mutation exclusion.
//! The caller must validate its base revision before commit. This adapter does
//! not add a second writer or make concurrent document writes safe.

use std::sync::Arc;

use crate::persistence::{
    FileCommitError, FileCommitOutcome, FileHubStateStore, HubState, HubStateAuthority,
    HubStateStoreError, HubStateUncertainWrite, PreparedHubStateWrite,
};
use crate::shared_view::{SharedView, SharedViewBudget};

use super::record::{AttemptId, ManagedIdentity, Phase, RecordError, RecoveryPolicy};

#[derive(Debug)]
pub(crate) enum RecoveryWriteError {
    Record(RecordError),
    Store(HubStateStoreError),
}

pub(crate) struct PreparedRecoveryWrite {
    store: FileHubStateStore,
    base_revision: u64,
    write: PreparedHubStateWrite,
    attempt: AttemptId,
    phase: Phase,
}

/// Returned only after the existing store commits the complete document.
pub(crate) struct DurableReceipt {
    state: SharedView<HubState>,
    attempt: AttemptId,
    phase: Phase,
    committed_revision: u64,
}

#[derive(Debug)]
pub(crate) enum RecoveryCommitError {
    /// Return the preparation to its owner without writing or discarding it.
    Stale(PreparedRecoveryWrite),
    RevisionExhausted(PreparedRecoveryWrite),
    /// The prior committed state still owns the obligation.
    Write {
        error: HubStateStoreError,
        attempt: AttemptId,
        phase: Phase,
    },
    /// Rename completed without a clean directory result. No receipt is issued.
    PublishedUncertain {
        write: HubStateUncertainWrite,
        attempt: AttemptId,
        phase: Phase,
    },
}

impl std::fmt::Debug for PreparedRecoveryWrite {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedRecoveryWrite")
            .field("base_revision", &self.base_revision)
            .field("attempt", &self.attempt)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}

impl DurableReceipt {
    pub(crate) fn state(&self) -> &SharedView<HubState> {
        &self.state
    }
    pub(crate) fn attempt(&self) -> &AttemptId {
        &self.attempt
    }
    pub(crate) fn phase(&self) -> Phase {
        self.phase
    }
    pub(crate) fn committed_revision(&self) -> u64 {
        self.committed_revision
    }
}

impl PreparedRecoveryWrite {
    /// Prepare an intent on Host. G1 allocation admission remains an integration gate.
    pub(crate) fn admit(
        store: &FileHubStateStore,
        authority: &HubStateAuthority,
        base_revision: u64,
        prior: SharedView<HubState>,
        mut candidate: HubState,
        budget: &Arc<SharedViewBudget>,
        session_id: String,
        managed: Option<ManagedIdentity>,
        policy: RecoveryPolicy,
    ) -> Result<Self, RecoveryWriteError> {
        let attempt = candidate
            .recovery
            .admit(&candidate.host.id, session_id, managed, policy)
            .map_err(RecoveryWriteError::Record)?;
        let phase = candidate
            .recovery
            .records
            .last()
            .expect("admitted record")
            .phase;
        let write = store
            .prepare_shared(authority, base_revision, Some(prior), candidate, budget)
            .map_err(RecoveryWriteError::Store)?;
        Ok(Self {
            store: store.clone(),
            base_revision,
            write,
            attempt,
            phase,
        })
    }

    pub(crate) fn transition(
        store: &FileHubStateStore,
        authority: &HubStateAuthority,
        base_revision: u64,
        prior: SharedView<HubState>,
        mut candidate: HubState,
        budget: &Arc<SharedViewBudget>,
        attempt: AttemptId,
        session_id: &str,
        phase: Phase,
    ) -> Result<Self, RecoveryWriteError> {
        candidate
            .recovery
            .transition(&attempt, session_id, phase)
            .map_err(RecoveryWriteError::Record)?;
        let write = store
            .prepare_shared(authority, base_revision, Some(prior), candidate, budget)
            .map_err(RecoveryWriteError::Store)?;
        Ok(Self {
            store: store.clone(),
            base_revision,
            write,
            attempt,
            phase,
        })
    }

    /// Commit on Host before the caller starts the effect identified by this receipt.
    // Refusal returns the complete preparation without allocating a box.
    #[allow(clippy::result_large_err)]
    pub(crate) fn commit(
        self,
        current_revision: u64,
    ) -> Result<DurableReceipt, RecoveryCommitError> {
        if current_revision != self.base_revision {
            return Err(RecoveryCommitError::Stale(self));
        }
        if self.base_revision.checked_add(1).is_none() {
            return Err(RecoveryCommitError::RevisionExhausted(self));
        }
        let PreparedRecoveryWrite {
            store,
            base_revision,
            write,
            attempt,
            phase,
        } = self;
        match store.commit_shared(write, current_revision) {
            Ok(FileCommitOutcome::Synced { state, revision }) => Ok(DurableReceipt {
                state,
                attempt,
                phase,
                committed_revision: revision,
            }),
            Ok(FileCommitOutcome::PublishedUncertain(write)) => {
                Err(RecoveryCommitError::PublishedUncertain {
                    write,
                    attempt,
                    phase,
                })
            }
            Err(FileCommitError::Stale(write)) => Err(RecoveryCommitError::Stale(Self {
                store,
                base_revision,
                write,
                attempt,
                phase,
            })),
            Err(FileCommitError::RevisionExhausted(write)) => {
                Err(RecoveryCommitError::RevisionExhausted(Self {
                    store,
                    base_revision,
                    write,
                    attempt,
                    phase,
                }))
            }
            Err(FileCommitError::Preparation(error))
            | Err(FileCommitError::BeforePublication { error, .. }) => {
                Err(RecoveryCommitError::Write {
                    error,
                    attempt,
                    phase,
                })
            }
        }
    }
}
