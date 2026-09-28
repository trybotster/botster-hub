//! One durable HubState record write, shared by every per-request record.
//!
//! A record write prepares a candidate HubState on the Host, takes the state
//! document, commits, and settles. Managed Git worktree records and session
//! restart records use it. The machine decides; it never submits Host work
//! itself. Each step returns a [`StateRecordAction`], and the caller submits
//! with its own Host permit and phase counter, which it also uses for its other
//! Host steps.
//!
//! Every wait is keyed: a busy document parks the waiter in `document_waiters`,
//! and `release_document` wakes it; a Host step waits on its own completion
//! identity. Nothing marks or sweeps.

use crate::HubDaemon;
use crate::daemon::control::host_work::{DocumentAdmission, admit_document, release_document};
use crate::daemon::owner_loop::{DaemonControlState, UncertainPublicationCleanup};
use crate::host_executor::{HostCommand, HostResult};
use crate::host_mutations::{
    HostCommit, HostMutationCommand, HostMutationResult, HostPrepare, PreparedMutation,
    RecoveryOutcome,
};
use crate::owner_identity::WaiterId;

/// What one record write changes.
pub(crate) enum StateRecordTarget {
    /// Add or replace a Hub-managed worktree record.
    ManagedWorktree(crate::Worktree),
    /// Remove a Hub-managed worktree record.
    RemoveManagedWorktree(String),
    /// Set (`Some`) or remove (`None`) a session's restart record.
    RestartRecord {
        session_id: String,
        record: Option<crate::restart_records::RestartRecord>,
    },
}

/// The step the caller takes next.
pub(crate) enum StateRecordAction {
    /// Submit this Host command with the caller's permit and next phase. When
    /// [`StateRecordWrite::awaits_commit`] is true and the submission does not
    /// go Pending, report it through [`StateRecordWrite::commit_not_submitted`].
    Submit(HostCommand),
    /// The document is busy: return Pending. The document owner's release wakes
    /// this waiter, and the next poll calls [`StateRecordWrite::admit`] with
    /// `parked = true`.
    Park,
    /// The record is durable and published; the document is released.
    Committed,
    /// The write failed before publication; the document is released. A
    /// prepared mutation that was never committed comes back in `discard`: the
    /// caller hands it to a Host step to drop, off the owner thread.
    Failed {
        stage: StateRecordStage,
        code: String,
        message: String,
        discard: Option<Box<PreparedMutation>>,
    },
    /// The write reached publication without a confirmed durable result. It
    /// is retained with the caller's cleanup, and the document is released.
    Uncertain,
    /// The commit landed on a revision other than the next one: another writer
    /// interleaved. Nothing was published; the document is released.
    RevisionMismatch,
    /// The Host returned something this write cannot have caused; the
    /// document is released.
    Reconciliation(String),
}

/// Where a failure happened, so callers map it to their own codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateRecordStage {
    Prepare,
    /// The single uncertain-publication cell is held by another writer.
    PublicationSlot,
    Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Prepare,
    Park,
    Commit,
}

/// One in-flight record write.
pub(crate) struct StateRecordWrite {
    waiter_id: WaiterId,
    target: StateRecordTarget,
    phase: Phase,
    prepared: Option<Box<PreparedMutation>>,
}

impl StateRecordWrite {
    /// Start a write: the returned command prepares the candidate state.
    pub(crate) fn begin(
        daemon: &HubDaemon,
        waiter_id: WaiterId,
        target: StateRecordTarget,
    ) -> Result<(Self, StateRecordAction), String> {
        let mut write = Self {
            waiter_id,
            target,
            phase: Phase::Prepare,
            prepared: None,
        };
        let action = write.prepare(daemon, None)?;
        Ok((write, action))
    }

    /// The write is parked on the document.
    pub(crate) fn is_parked(&self) -> bool {
        self.phase == Phase::Park
    }

    /// Settle one Host completion of this write.
    pub(crate) fn on_completion(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
        cleanup: impl FnOnce() -> Option<UncertainPublicationCleanup>,
    ) -> StateRecordAction {
        match self.phase {
            Phase::Prepare => self.prepared_result(daemon, state, result),
            Phase::Commit => self.committed(daemon, state, result, cleanup),
            Phase::Park => StateRecordAction::Reconciliation(
                "a parked record write received a Host completion".to_string(),
            ),
        }
    }

    /// Take the document for the prepared write. `parked` is true when the
    /// document owner's release woke this waiter.
    pub(crate) fn admit(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        parked: bool,
    ) -> StateRecordAction {
        let base_revision = self
            .prepared
            .as_ref()
            .expect("an admitted record write has a prepared mutation")
            .base_revision;
        match admit_document(state, self.waiter_id, base_revision, daemon.state_view().0) {
            DocumentAdmission::Granted => {
                if !state.reserve_uncertain_publication(self.waiter_id) {
                    release_document(state, self.waiter_id);
                    return StateRecordAction::Failed {
                        stage: StateRecordStage::PublicationSlot,
                        code: "state_publication_slot_occupied".to_string(),
                        message: "another unresolved state publication owns the retention cell"
                            .to_string(),
                        discard: self.prepared.take(),
                    };
                }
                let prepared = *self
                    .prepared
                    .take()
                    .expect("an admitted record write has a prepared mutation");
                self.phase = Phase::Commit;
                StateRecordAction::Submit(HostCommand::Mutation(HostMutationCommand::Commit(
                    HostCommit { prepared },
                )))
            }
            DocumentAdmission::Busy => {
                self.phase = Phase::Park;
                StateRecordAction::Park
            }
            DocumentAdmission::Stale => {
                if parked {
                    state.document_waiters.remove(&self.waiter_id);
                }
                let superseded = self.prepared.take();
                match self.prepare(daemon, superseded) {
                    Ok(action) => action,
                    Err(message) => {
                        release_document(state, self.waiter_id);
                        StateRecordAction::Failed {
                            stage: StateRecordStage::Prepare,
                            code: "state_authority_unavailable".to_string(),
                            message,
                            discard: None,
                        }
                    }
                }
            }
        }
    }

    /// A commit submission did not go Pending: free the publication claim.
    /// The document stays owned: the caller retains the failed submission as
    /// Host recovery, which holds the document until that recovery settles.
    pub(crate) fn commit_not_submitted(&mut self, state: &mut DaemonControlState) {
        state.release_uncertain_reservation(self.waiter_id);
    }

    /// The write's next Host step is its commit.
    pub(crate) fn awaits_commit(&self) -> bool {
        self.phase == Phase::Commit
    }

    /// Hand over what a dropped write still holds.
    pub(crate) fn take_prepared(&mut self) -> Option<Box<PreparedMutation>> {
        self.prepared.take()
    }

    fn prepare(
        &mut self,
        daemon: &HubDaemon,
        superseded: Option<Box<PreparedMutation>>,
    ) -> Result<StateRecordAction, String> {
        let runtime = daemon
            .runtime()
            .ok_or_else(|| "the Hub runtime stopped before the record write".to_string())?;
        let authority = runtime
            .state_authority()
            .ok_or_else(|| "the record write requires File state authority".to_string())?;
        let data_directory = runtime.config().data_directory.clone();
        let (base_revision, state) = daemon.state_view();
        let prepare = match &self.target {
            StateRecordTarget::ManagedWorktree(worktree) => HostPrepare::ManagedWorktree {
                worktree: worktree.clone(),
                base_revision,
                authority,
                state,
                data_directory,
                superseded,
            },
            StateRecordTarget::RemoveManagedWorktree(worktree_id) => {
                HostPrepare::RemoveManagedWorktree {
                    worktree_id: worktree_id.clone(),
                    base_revision,
                    authority,
                    state,
                    data_directory,
                    superseded,
                }
            }
            StateRecordTarget::RestartRecord { session_id, record } => HostPrepare::RestartRecord {
                session_id: session_id.clone(),
                record: record.clone(),
                base_revision,
                authority,
                state,
                data_directory,
                superseded,
            },
        };
        self.phase = Phase::Prepare;
        Ok(StateRecordAction::Submit(HostCommand::Mutation(
            HostMutationCommand::Prepare(Box::new(prepare)),
        )))
    }

    fn prepared_result(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
    ) -> StateRecordAction {
        match result {
            HostResult::Mutation(HostMutationResult::Prepared(prepared)) => {
                self.prepared = Some(Box::new(prepared));
                self.admit(daemon, state, false)
            }
            HostResult::Mutation(HostMutationResult::Failed(error)) => StateRecordAction::Failed {
                stage: StateRecordStage::Prepare,
                code: error.code,
                message: error.message,
                discard: None,
            },
            _ => StateRecordAction::Reconciliation(
                "the host executor returned an invalid record preparation result".to_string(),
            ),
        }
    }

    fn committed(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
        cleanup: impl FnOnce() -> Option<UncertainPublicationCleanup>,
    ) -> StateRecordAction {
        if !matches!(
            result,
            HostResult::Mutation(HostMutationResult::PublishedUncertain { .. })
        ) {
            state.release_uncertain_reservation(self.waiter_id);
        }
        let action = match result {
            HostResult::Mutation(HostMutationResult::PublishedUncertain { write, rollback }) => {
                state.retain_uncertain_publication(self.waiter_id, write, rollback, cleanup());
                StateRecordAction::Uncertain
            }
            HostResult::Mutation(HostMutationResult::Committed(committed)) => {
                if committed.committed_revision != daemon.state_view().0.saturating_add(1) {
                    StateRecordAction::RevisionMismatch
                } else {
                    // Publish before the document is released.
                    daemon.publish_state(committed.view);
                    StateRecordAction::Committed
                }
            }
            HostResult::Mutation(HostMutationResult::Failed(error)) => StateRecordAction::Failed {
                stage: StateRecordStage::Commit,
                code: error.code,
                message: error.message,
                discard: None,
            },
            HostResult::Mutation(HostMutationResult::Recovered(
                RecoveryOutcome::RegisteredWorktree { failure, .. }
                | RecoveryOutcome::RestartRecord { failure, .. },
            )) => StateRecordAction::Failed {
                stage: StateRecordStage::Commit,
                code: failure.code,
                message: failure.message,
                discard: None,
            },
            _ => StateRecordAction::Reconciliation(
                "the host executor returned an invalid record commit result".to_string(),
            ),
        };
        release_document(state, self.waiter_id);
        action
    }
}
