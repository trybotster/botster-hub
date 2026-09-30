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
    Submit(Box<HostCommand>),
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
                StateRecordAction::Submit(Box::new(HostCommand::Mutation(
                    HostMutationCommand::Commit(HostCommit { prepared }),
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
        Ok(StateRecordAction::Submit(Box::new(HostCommand::Mutation(
            HostMutationCommand::Prepare(Box::new(prepare)),
        ))))
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
                | RecoveryOutcome::RestartRecord { failure },
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::daemon::control::host_work::release_document;
    use crate::restart_records::{RestartContext, RestartRecord};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    // Parallel tests can read the same clock value; the counter keeps each
    // test daemon's directory, and so its directory lock, distinct.
    static NEXT_DAEMON: AtomicU64 = AtomicU64::new(0);

    /// A started Hub daemon with File state authority, and its data directory.
    pub(crate) fn test_daemon() -> (HubDaemon, std::path::PathBuf) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        let directory = std::path::PathBuf::from("target")
            .join("botster-hub-test-data")
            .join(format!(
                "state-record-writer-{unique}-{}",
                NEXT_DAEMON.fetch_add(1, Ordering::Relaxed)
            ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "state-record-writer".to_string(),
                display_name: "State Record Writer".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build state record writer test config");
        (
            HubDaemon::start(config).expect("start test daemon"),
            directory,
        )
    }

    pub(crate) fn target(session_id: &str) -> StateRecordTarget {
        StateRecordTarget::RestartRecord {
            session_id: session_id.to_string(),
            record: Some(RestartRecord {
                session_type_id: "init".to_string(),
                target_id: None,
                cwd: None,
                environment_keys: Vec::new(),
                context: RestartContext::default(),
            }),
        }
    }

    /// Run the Host mutation an action asks for, on this thread.
    pub(crate) fn run(action: StateRecordAction) -> HostResult {
        let StateRecordAction::Submit(command) = action else {
            panic!("the action submits a Host command");
        };
        let HostCommand::Mutation(command) = *command else {
            panic!("the action submits a Host mutation");
        };
        HostResult::Mutation(crate::host_mutations::execute(command, None))
    }

    fn begin(
        daemon: &HubDaemon,
        waiter: u64,
        session_id: &str,
    ) -> (StateRecordWrite, StateRecordAction) {
        StateRecordWrite::begin(daemon, WaiterId(waiter), target(session_id))
            .expect("begin the write")
    }

    /// Feed the result of the Host step `action` asks for back to the write.
    fn step(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        write: &mut StateRecordWrite,
        action: StateRecordAction,
    ) -> StateRecordAction {
        write.on_completion(daemon, state, run(action), || None)
    }

    fn finish(mut daemon: HubDaemon, directory: std::path::PathBuf) {
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove state record writer test directory");
    }

    fn holds(daemon: &HubDaemon, session_id: &str) -> bool {
        daemon
            .state_view()
            .1
            .restart_records
            .contains_key(session_id)
    }

    #[test]
    fn a_busy_document_parks_the_write_and_the_release_lets_it_commit() {
        let (mut daemon, directory) = test_daemon();
        let mut state = DaemonControlState::default();
        state.document_owner = Some(WaiterId(9));
        let (mut write, action) = begin(&daemon, 1, "parked-session");
        let action = step(&mut daemon, &mut state, &mut write, action);
        assert!(matches!(action, StateRecordAction::Park));
        assert!(write.is_parked());
        assert!(state.document_waiters.contains(&WaiterId(1)));
        assert!(!holds(&daemon, "parked-session"));

        release_document(&mut state, WaiterId(9));
        assert!(
            !state.document_waiters.contains(&WaiterId(1)),
            "the release wakes the parked waiter"
        );
        let action = write.admit(&daemon, &mut state, true);
        assert!(write.awaits_commit());
        let committed = step(&mut daemon, &mut state, &mut write, action);
        assert!(matches!(committed, StateRecordAction::Committed));
        assert!(holds(&daemon, "parked-session"));
        assert_eq!(state.document_owner, None);
        finish(daemon, directory);
    }

    #[test]
    fn a_stale_preparation_is_prepared_again_and_then_commits() {
        let (mut daemon, directory) = test_daemon();
        let mut state = DaemonControlState::default();
        // The first writer prepares against the current revision...
        let (mut first, first_action) = begin(&daemon, 1, "first-session");
        let first_prepared = run(first_action);
        // ...and a second writer commits before the first is admitted.
        let (mut second, second_action) = begin(&daemon, 2, "second-session");
        let action = step(&mut daemon, &mut state, &mut second, second_action);
        let committed = step(&mut daemon, &mut state, &mut second, action);
        assert!(matches!(committed, StateRecordAction::Committed));

        let action = first.on_completion(&mut daemon, &mut state, first_prepared, || None);
        assert!(
            matches!(
                &action,
                StateRecordAction::Submit(command)
                    if matches!(**command, HostCommand::Mutation(HostMutationCommand::Prepare(_)))
            ),
            "the stale preparation is prepared again"
        );
        let action = step(&mut daemon, &mut state, &mut first, action);
        let committed = step(&mut daemon, &mut state, &mut first, action);
        assert!(matches!(committed, StateRecordAction::Committed));
        assert!(holds(&daemon, "first-session"));
        assert!(holds(&daemon, "second-session"));
        finish(daemon, directory);
    }

    #[test]
    fn a_commit_submission_that_does_not_go_pending_frees_the_publication_claim() {
        let (mut daemon, directory) = test_daemon();
        let mut state = DaemonControlState::default();
        let (mut write, action) = begin(&daemon, 1, "refused-session");
        let action = step(&mut daemon, &mut state, &mut write, action);
        assert!(write.awaits_commit());
        assert!(matches!(action, StateRecordAction::Submit(_)));
        assert!(
            !state.reserve_uncertain_publication(WaiterId(2)),
            "the claim is held while the commit is in flight"
        );
        write.commit_not_submitted(&mut state);
        assert!(
            state.reserve_uncertain_publication(WaiterId(2)),
            "a refused submission frees the claim"
        );
        finish(daemon, directory);
    }

    #[test]
    fn an_uncertain_publication_is_retained_and_refuses_the_next_write_at_the_cell() {
        let (mut daemon, directory) = test_daemon();
        let mut state = DaemonControlState::default();
        let (mut write, action) = begin(&daemon, 1, "uncertain-session");
        let action = step(&mut daemon, &mut state, &mut write, action);
        crate::persistence::FileHubStateStore::inject_next_directory_sync_failure(&directory);
        let result = run(action);
        assert!(matches!(
            result,
            HostResult::Mutation(HostMutationResult::PublishedUncertain { .. })
        ));
        let action = write.on_completion(&mut daemon, &mut state, result, || None);
        assert!(matches!(action, StateRecordAction::Uncertain));
        assert!(
            state.uncertain_publication_holds_write(),
            "the uncertain write is retained in the cell"
        );
        assert_eq!(state.document_owner, None, "the document is released");

        let (mut second, second_action) = begin(&daemon, 2, "second-session");
        let action = step(&mut daemon, &mut state, &mut second, second_action);
        let StateRecordAction::Failed { stage, discard, .. } = action else {
            panic!("the retention cell is taken, so the next write fails");
        };
        assert_eq!(stage, StateRecordStage::PublicationSlot);
        assert!(
            discard.is_some(),
            "the refused write hands back its prepared mutation"
        );
        assert_eq!(state.document_owner, None);
        finish(daemon, directory);
    }
}
