//! Owner-routed managed worktree and session spawn operation.

use std::time::Instant;

use botster_hub_client::DaemonResponseKind;

use crate::HubDaemon;
use crate::daemon::control::host_work::{HostRecoveryRequired, retain_submission};
use crate::daemon::control::pending::{
    ControlPoll, PendingControlRequest, READY_DEADLINE, READY_INITIAL, mark_owner_ready,
};
use crate::daemon::control::state_record::{
    StateRecordAction, StateRecordStage, StateRecordTarget, StateRecordWrite,
};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::ReadyClass;
use crate::host_executor::{
    HostCommand, HostJobIdentity, HostResult, HostSubmissionFailure, HostSubmitError,
    HostWorkPermit,
};
use crate::host_mutations::PreparedMutation;
use crate::managed_git_worktrees::{
    MANAGED_GIT_OPERATION_TIMEOUT, ManagedGitError, ManagedWorktreeDecision,
    PreparedManagedWorktree, managed_worktree_id,
};
use crate::owner_identity::WaiterId;
use crate::runtime::{ManagedSessionSpawnStart, PendingManagedSessionSpawn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Create,
    /// The managed worktree record write (prepare, park, commit).
    Record,
    Spawn,
    /// Delivery succeeded; a removed-during-launch release settles before
    /// the worktree commit finalizes.
    Handoff,
    FinalizeCommit,
    FinalizeRollback,
    /// The managed worktree record removal (prepare, park, commit).
    Removal,
    Done,
}

pub(crate) struct ManagedSpawnOperation {
    waiter_id: WaiterId,
    worktree_id: String,
    spawner: Option<crate::runtime::SharedSessionTypeSpawner>,
    inherited_creation: Option<PreparedManagedWorktree>,
    core_release_confirmed: bool,
    pending: Option<PendingManagedSessionSpawn>,
    prepared: Option<PreparedManagedWorktree>,
    record_write: Option<StateRecordWrite>,
    spawn: Option<ManagedSessionSpawnStart>,
    permit: Option<HostWorkPermit>,
    phase: Phase,
    next_host_phase: u64,
    record_committed: bool,
    deferred_error: Option<ManagedGitError>,
    deadline: Instant,
}

pub(crate) struct ManagedGitRecoveryRequired {
    pub(crate) code: String,
    pub(crate) message: String,
    _prepared: PreparedManagedWorktree,
    _permit: HostWorkPermit,
}

impl ManagedGitRecoveryRequired {
    pub(super) fn into_terminal(self, identity: HostJobIdentity) -> crate::host_disposal::Parts {
        crate::host_disposal::Parts {
            storage: None,
            identity,
            permit: self._permit,
            model: None,
            payload: Box::new((self._prepared, self.code, self.message)),
        }
    }
}

fn accept_confirmed_rollback(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    prepared: crate::managed_git_worktrees::PreparedManagedWorktree,
) {
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    let worktree_id = &prepared.worktree_id;
    if runtime
        .session_type_spawner()
        .managed_attempt_active(worktree_id)
        || runtime.peek_pending_managed_worktree_id().as_deref() == Some(worktree_id)
    {
        runtime.defer_confirmed_worktree_rollback(prepared);
        return;
    }
    if !state.admits_work() {
        runtime.defer_confirmed_worktree_rollback(prepared);
        state.managed_spawn_waiting_for_owner = true;
        return;
    }
    let Some(waiter_id) = state.waiter_ids.next() else {
        runtime.defer_confirmed_worktree_rollback(prepared);
        state.managed_spawn_waiting_for_owner = true;
        return;
    };
    let Some(host_permit) = runtime.host_executor().try_reserve() else {
        runtime.defer_confirmed_worktree_rollback(prepared);
        state.managed_spawn_waiting_for_host = true;
        return;
    };
    let deadline = Instant::now() + MANAGED_GIT_OPERATION_TIMEOUT;
    runtime.begin_submitted_worktree_rollback(&prepared.worktree_id);
    if let Err(failure) = runtime.host_executor().submit(
        HostJobIdentity {
            waiter_id,
            phase: 1,
        },
        HostCommand::FinalizeManagedWorktree {
            prepared: prepared.clone(),
            decision: ManagedWorktreeDecision::Rollback,
            deadline,
            discard: None,
            #[cfg(test)]
            rollback_hold: runtime.test_rollback_git_hold(),
        },
        host_permit,
    ) {
        runtime.clear_submitted_worktree_rollback(&prepared.worktree_id);
        runtime.defer_confirmed_worktree_rollback(prepared);
        if matches!(failure.error, HostSubmitError::Full) {
            state.managed_spawn_waiting_for_host = true;
        }
        return;
    }
    let operation = ManagedSpawnOperation {
        waiter_id,
        worktree_id: prepared.worktree_id.clone(),
        spawner: None,
        inherited_creation: None,
        core_release_confirmed: false,
        pending: None,
        prepared: Some(prepared),
        record_write: None,
        spawn: None,
        permit: None,
        phase: Phase::FinalizeRollback,
        next_host_phase: 2,
        record_committed: true,
        deferred_error: None,
        deadline,
    };
    let (reply_tx, _reply_rx) = crate::daemon::control::message::control_reply_channel();
    state.pending_requests.insert(
        waiter_id,
        PendingControlRequest {
            waiter_id,
            ready_key: None,
            deadline_key: None,
            last_core_phase: 0,
            last_host_phase: 0,
            completion: crate::daemon::control::pending::OwnerRequestCompletion::default(),
            reply_tx,
            response_delivery_rx: None,
            grant_id: None,
            client: None,
            core_retirement: None,
            must_finish: true,
            past_deadline: false,
            continuation: crate::daemon::control::pending::ControlContinuation::ManagedSpawn(
                Box::new(operation),
            ),
            retire: None,
        },
    );
    let arm = state
        .deadlines
        .arm(waiter_id, deadline, Instant::now())
        .expect("a rollback deadline must make progress");
    state
        .pending_requests
        .get_mut(&waiter_id)
        .expect("the rollback waiter was inserted")
        .deadline_key = Some(arm.key());
    mark_owner_ready(state, waiter_id, ReadyClass::HostCompletion, READY_INITIAL);
}

pub(crate) fn accept_one(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    #[cfg(test)]
    if let Some(runtime) = daemon.runtime() {
        runtime.test_note_managed_accept_one();
    }
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    runtime.retry_retained_reservation_releases();
    runtime.retry_created_worktree_releases();
    runtime.reap_detached_core_operations();
    if let Some(worktree_id) = runtime.peek_pending_managed_worktree_id()
        && (runtime
            .session_type_spawner()
            .managed_attempt_active(&worktree_id)
            || runtime.created_worktree_cleanup_active(&worktree_id))
    {
        return;
    }
    if let Some(worktree_id) = runtime.peek_pending_managed_worktree_id()
        && runtime.submitted_worktree_rollback(&worktree_id)
    {
        if let Some(prepared) = runtime.take_one_confirmed_worktree_rollback() {
            accept_confirmed_rollback(daemon, state, prepared);
        }
        return;
    }
    let Some(pending) = runtime.take_pending_managed_spawn() else {
        if let Some(prepared) = runtime.take_one_confirmed_worktree_rollback() {
            accept_confirmed_rollback(daemon, state, prepared);
        }
        return;
    };
    // A refusal before Host admission leaves the old right in the confirmed
    // queue. Keep its existing event live without transferring ownership.
    runtime.wake_remaining_confirmed_worktree_rollbacks();
    let worktree_id = managed_worktree_id(&pending.target_id, &pending.branch);
    let spawner = runtime.session_type_spawner();
    if let Some(detail) = state
        .host_recovery
        .values()
        .find_map(|recovery| match recovery {
            HostRecoveryRequired::ManagedGit(recovery) => {
                Some(format!("{}: {}", recovery.code, recovery.message))
            }
            HostRecoveryRequired::Submission { failure, .. } => {
                Some(submit_error_message(failure.error).to_string())
            }
            HostRecoveryRequired::Package(_)
            | HostRecoveryRequired::PackageFamilyWork { .. }
            | HostRecoveryRequired::PackageEvents { .. }
            | HostRecoveryRequired::PackageFamilies { .. } => None,
            HostRecoveryRequired::Terminal(_) => None,
        })
    {
        let _ = pending.respond(Err(ManagedGitError::new("reconciliation_required", detail)));
        return;
    }
    let request = match runtime.validate_managed_git_request(&pending) {
        Ok(request) => request,
        Err(error) => {
            let _ = pending.respond(Err(error));
            return;
        }
    };
    if !state.admits_work() {
        let _ = pending.respond(Err(ManagedGitError::new(
            "ensure_backpressured",
            "the Hub owner has no available operation slot",
        )));
        return;
    }
    let Some(waiter_id) = state.waiter_ids.next() else {
        let _ = pending.respond(Err(ManagedGitError::new(
            "ensure_unavailable",
            "the Hub owner exhausted unique operation identifiers",
        )));
        return;
    };
    let Some(host_permit) = runtime.host_executor().try_reserve() else {
        let _ = pending.respond(Err(ManagedGitError::new(
            "ensure_backpressured",
            "the bounded host executor has no available operation slot",
        )));
        return;
    };
    if runtime
        .host_executor()
        .submit(
            HostJobIdentity {
                waiter_id,
                phase: 1,
            },
            HostCommand::CreateManagedWorktree { request },
            host_permit,
        )
        .is_err()
    {
        let _ = pending.respond(Err(ManagedGitError::new(
            "ensure_unavailable",
            "the host executor stopped before it accepted managed Git work",
        )));
        return;
    }
    // The owner has no fallible step between Host admission and this claim.
    spawner.begin_managed_attempt(worktree_id.clone(), waiter_id);

    let accepted_at = pending.accepted_at;
    let operation = ManagedSpawnOperation {
        waiter_id,
        worktree_id,
        spawner: Some(spawner),
        inherited_creation: None,
        core_release_confirmed: false,
        pending: Some(pending),
        prepared: None,
        record_write: None,
        spawn: None,
        permit: None,
        phase: Phase::Create,
        next_host_phase: 2,
        record_committed: false,
        deferred_error: None,
        deadline: accepted_at + MANAGED_GIT_OPERATION_TIMEOUT,
    };
    let (reply_tx, _reply_rx) = crate::daemon::control::message::control_reply_channel();
    state.pending_requests.insert(
        waiter_id,
        PendingControlRequest {
            waiter_id,
            ready_key: None,
            deadline_key: None,
            last_core_phase: 0,
            last_host_phase: 0,
            completion: crate::daemon::control::pending::OwnerRequestCompletion::default(),
            reply_tx,
            response_delivery_rx: None,
            grant_id: None,
            client: None,
            core_retirement: None,
            must_finish: true,
            past_deadline: false,
            continuation: crate::daemon::control::pending::ControlContinuation::ManagedSpawn(
                Box::new(operation),
            ),
            retire: None,
        },
    );
    let deadline = accepted_at + MANAGED_GIT_OPERATION_TIMEOUT;
    let arm = state
        .deadlines
        .arm(waiter_id, deadline, Instant::now())
        .expect("a new managed Git deadline must make progress");
    state
        .pending_requests
        .get_mut(&waiter_id)
        .expect("the managed Git waiter was inserted")
        .deadline_key = Some(arm.key());
    mark_owner_ready(state, waiter_id, ReadyClass::HostCompletion, READY_INITIAL);
    if arm.is_due() {
        mark_owner_ready(state, waiter_id, ReadyClass::Deadline, READY_DEADLINE);
    }
}

impl ManagedSpawnOperation {
    pub(crate) fn take_terminal_parts(
        &mut self,
        runtime: &crate::HubRuntime,
        identity: HostJobIdentity,
        completion: &mut Option<crate::host_executor::HostCompletion>,
    ) -> Option<crate::host_disposal::Parts> {
        self.finish_inherited_creation(runtime);
        let mut result = None;
        let (identity, permit) = if let Some(permit) = self.permit.take() {
            (identity, permit)
        } else {
            let (identity, value, permit) = completion.take()?.into_parts();
            result = Some(value);
            (identity, permit)
        };
        Some(crate::host_disposal::Parts {
            storage: None,
            identity,
            permit,
            model: None,
            payload: Box::new((
                self.pending.take(),
                self.prepared.take(),
                self.record_write
                    .as_mut()
                    .and_then(StateRecordWrite::take_prepared),
                self.spawn.take(),
                self.deferred_error.take(),
                result,
                completion.take(),
            )),
        })
    }

    pub(crate) fn poll(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        let outcome = self.poll_inner(daemon, state);
        if matches!(
            &outcome,
            ControlPoll::Ready(_) | ControlPoll::FinishedInternal
        ) {
            let runtime = daemon
                .runtime()
                .expect("managed completion runs before daemon runtime stop");
            self.finish_inherited_creation(runtime);
            self.finish_active_attempt();
        }
        outcome
    }

    fn poll_inner(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        if matches!(self.phase, Phase::Record | Phase::Removal)
            && self
                .record_write
                .as_ref()
                .is_some_and(StateRecordWrite::is_parked)
        {
            return self.admit_parked(daemon, state);
        }
        if self.phase == Phase::Spawn {
            return self.poll_spawn(daemon, state);
        }
        if self.phase == Phase::Handoff {
            let Some(runtime) = daemon.runtime() else {
                return ControlPoll::Pending;
            };
            let spawn = self.spawn.as_mut().expect("managed Core spawn exists");
            if !spawn.poll_handoff(runtime) {
                return ControlPoll::Pending;
            }
            if self.deferred_error.is_some() {
                return self.finish_deferred_error();
            }
            return self.submit_finalize(daemon, state, ManagedWorktreeDecision::Commit, None);
        }
        let Some(completion) = state.host_completions.remove(&self.waiter_id) else {
            return ControlPoll::Pending;
        };
        let (_, result, permit) = completion.into_parts();
        self.permit = Some(permit);
        match self.phase {
            Phase::Create => self.created(daemon, state, result),
            Phase::Record | Phase::Removal => self.record_write_completed(daemon, state, result),
            Phase::FinalizeCommit => self.finalized_commit(state, result),
            Phase::FinalizeRollback => self.finalized_rollback(daemon, state, result),
            Phase::Spawn | Phase::Handoff | Phase::Done => self.finish_reconciliation(
                "the managed Git owner received an unexpected host completion",
            ),
        }
    }

    fn created(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
    ) -> ControlPoll {
        match result {
            HostResult::ManagedWorktreeCreated(prepared) => {
                if let Some(runtime) = daemon.runtime() {
                    if prepared.created_worktree
                        && runtime.confirmed_worktree_rollback_exists(&self.worktree_id)
                    {
                        self.prepared = Some(prepared);
                        self.deferred_error = Some(ManagedGitError::new(
                            "reconciliation_required",
                            "a prior created worktree still owns cleanup",
                        ));
                        return self.submit_finalize(
                            daemon,
                            state,
                            ManagedWorktreeDecision::Rollback,
                            None,
                        );
                    }
                    if !prepared.created_worktree {
                        // Reuse does not create a new Git rollback right.
                        self.inherited_creation =
                            runtime.take_confirmed_worktree_rollback(&self.worktree_id);
                        #[cfg(test)]
                        if self.inherited_creation.is_some() {
                            runtime.note_inherited_managed_cleanup_transfer();
                        }
                    }
                }
                self.prepared = Some(prepared);
                if self.deadline_elapsed() {
                    self.deferred_error = Some(timeout_error());
                    return self.submit_finalize(
                        daemon,
                        state,
                        ManagedWorktreeDecision::Rollback,
                        None,
                    );
                }
                self.start_record_write(daemon, state)
            }
            HostResult::ManagedWorktreeRecoveryRequired { prepared, error } => {
                self.retain_recovery(state, prepared, error)
            }
            HostResult::ManagedWorktreeFailed(error) => self.finish_error(error),
            HostResult::Failed { error, .. } => self.finish_error(managed_host_failure(error)),
            _ => self.finish_reconciliation("the host executor returned an invalid create result"),
        }
    }

    fn start_record_write(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        let worktree = self
            .prepared
            .as_ref()
            .expect("managed worktree exists")
            .worktree();
        let (write, action) = StateRecordWrite::begin(
            daemon,
            self.waiter_id,
            StateRecordTarget::ManagedWorktree(worktree),
        )
        .expect("File managed Git mutation retains its state authority");
        self.record_write = Some(write);
        self.apply_record_action(daemon, state, Phase::Record, action)
    }

    fn start_removal_write(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        let worktree_id = self
            .prepared
            .as_ref()
            .expect("managed worktree exists")
            .worktree_id
            .clone();
        let (write, action) = StateRecordWrite::begin(
            daemon,
            self.waiter_id,
            StateRecordTarget::RemoveManagedWorktree(worktree_id),
        )
        .expect("File managed Git mutation retains its state authority");
        self.record_write = Some(write);
        self.apply_record_action(daemon, state, Phase::Removal, action)
    }

    fn admit_parked(&mut self, daemon: &HubDaemon, state: &mut DaemonControlState) -> ControlPoll {
        let phase = self.phase;
        let action = self
            .record_write
            .as_mut()
            .expect("a parked phase has a record write")
            .admit(daemon, state, true);
        self.apply_record_action(daemon, state, phase, action)
    }

    fn record_write_completed(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
    ) -> ControlPoll {
        let phase = self.phase;
        let Some(write) = self.record_write.as_mut() else {
            return self.finish_reconciliation(
                "the managed Git owner received a record completion without a record write",
            );
        };
        let prepared = &mut self.prepared;
        let action = write.on_completion(daemon, state, result, || {
            Some(
                crate::daemon::owner_loop::UncertainPublicationCleanup::ManagedGit(
                    prepared.take().expect("managed worktree exists"),
                ),
            )
        });
        self.apply_record_action(daemon, state, phase, action)
    }

    /// Carry out one record-write step. `phase` is Record or Removal: the
    /// same write, each with the outcomes it has always had.
    fn apply_record_action(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        phase: Phase,
        action: StateRecordAction,
    ) -> ControlPoll {
        let removal = phase == Phase::Removal;
        match action {
            StateRecordAction::Submit(command) => {
                let commit = self
                    .record_write
                    .as_ref()
                    .is_some_and(StateRecordWrite::awaits_commit);
                let poll = self.submit_host(daemon, state, phase, *command);
                if commit
                    && !matches!(poll, ControlPoll::Pending)
                    && let Some(write) = self.record_write.as_mut()
                {
                    write.commit_not_submitted(state);
                }
                poll
            }
            StateRecordAction::Park => ControlPoll::Pending,
            StateRecordAction::Committed => {
                self.record_write = None;
                if removal {
                    self.finish_deferred_error()
                } else {
                    self.record_published(daemon, state)
                }
            }
            StateRecordAction::Failed {
                stage,
                message,
                discard,
                ..
            } => {
                self.record_write = None;
                match (removal, stage) {
                    (false, StateRecordStage::PublicationSlot) => {
                        self.deferred_error = Some(ManagedGitError::new(
                            "state_publication_slot_occupied",
                            "another unresolved state publication owns the retention cell",
                        ));
                        self.submit_finalize(
                            daemon,
                            state,
                            ManagedWorktreeDecision::Rollback,
                            discard,
                        )
                    }
                    (false, _) => {
                        self.deferred_error =
                            Some(ManagedGitError::new("persistence_failed", message));
                        self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
                    }
                    (true, StateRecordStage::PublicationSlot) => {
                        drop(discard);
                        let prepared = self.prepared.take().expect("managed worktree exists");
                        self.retain_recovery_with_code(
                            state,
                            prepared,
                            crate::host_executor::HostError::new(
                                "state_publication_slot_occupied",
                                "another unresolved state publication owns the retention cell",
                            ),
                            "state_publication_slot_occupied",
                        )
                    }
                    (true, _) => self.finish_reconciliation(&format!(
                        "managed worktree record removal failed: {message}"
                    )),
                }
            }
            StateRecordAction::Uncertain => {
                self.record_write = None;
                self.finish_error(ManagedGitError::new(
                    "state_publication_uncertain",
                    if removal {
                        "the managed worktree removal reached publication without a confirmed durable result"
                    } else {
                        "the managed worktree record reached publication without a confirmed durable result"
                    },
                ))
            }
            StateRecordAction::RevisionMismatch => {
                self.record_write = None;
                if removal {
                    return self.finish_reconciliation(
                        "the managed worktree removal revision is not the next Hub revision",
                    );
                }
                self.deferred_error = Some(ManagedGitError::new(
                    "reconciliation_required",
                    "the managed worktree commit revision is not the next Hub revision",
                ));
                self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
            }
            StateRecordAction::Reconciliation(message) => {
                self.record_write = None;
                self.finish_reconciliation(&message)
            }
        }
    }

    /// The managed worktree record is durable and published: spawn the session.
    fn record_published(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        self.record_committed = true;
        if self.deadline_elapsed() {
            self.deferred_error = Some(timeout_error());
            return self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None);
        }
        let Some(runtime) = daemon.runtime() else {
            self.deferred_error = Some(ManagedGitError::new(
                "spawn_failed",
                "the Hub runtime stopped before the session spawn",
            ));
            return self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None);
        };
        let start = runtime.spawn_prepared_managed_session(
            self.pending.as_ref().expect("managed request exists"),
            self.prepared.as_ref().expect("managed worktree exists"),
            self.waiter_id,
        );
        match start {
            Ok(start) => {
                self.spawn = Some(start);
                self.phase = Phase::Spawn;
                ControlPoll::Pending
            }
            Err(error) => {
                self.deferred_error = Some(error);
                self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_spawn_phase(
        waiter_id: WaiterId,
        pending: PendingManagedSessionSpawn,
        prepared: PreparedManagedWorktree,
        start: crate::runtime::ManagedSessionSpawnStart,
    ) -> Self {
        Self {
            waiter_id,
            worktree_id: prepared.worktree_id.clone(),
            spawner: None,
            inherited_creation: None,
            core_release_confirmed: false,
            pending: Some(pending),
            prepared: Some(prepared),
            record_write: None,
            spawn: Some(start),
            permit: None,
            phase: Phase::Spawn,
            next_host_phase: 2,
            record_committed: true,
            deferred_error: None,
            deadline: Instant::now() + std::time::Duration::from_secs(15),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_inherited_terminal(
        waiter_id: WaiterId,
        prepared: PreparedManagedWorktree,
        permit: HostWorkPermit,
    ) -> Self {
        Self {
            waiter_id,
            worktree_id: prepared.worktree_id.clone(),
            spawner: None,
            inherited_creation: Some(prepared),
            core_release_confirmed: true,
            pending: None,
            prepared: None,
            record_write: None,
            spawn: None,
            permit: Some(permit),
            phase: Phase::Spawn,
            next_host_phase: 2,
            record_committed: false,
            deferred_error: None,
            deadline: Instant::now() + std::time::Duration::from_secs(15),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_spawn_reservation(&self) -> Option<botster_core::SessionReservation> {
        self.spawn.as_ref()?.reservation.clone()
    }

    #[cfg(test)]
    pub(crate) fn test_poll_spawn(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        self.poll_spawn(daemon, state)
    }

    #[cfg(test)]
    pub(crate) fn test_skipped_rollback(&self) -> bool {
        self.phase == Phase::Done
    }

    fn poll_spawn(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        let Some(runtime) = daemon.runtime() else {
            self.deferred_error = Some(ManagedGitError::new(
                "spawn_failed",
                "the Hub runtime stopped before the session spawn completed",
            ));
            return self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None);
        };
        let start = self.spawn.as_mut().expect("managed Core spawn exists");
        let parent = &mut self
            .pending
            .as_mut()
            .expect("managed request exists")
            .parent;
        let completion = match start.poll(runtime, parent) {
            crate::runtime::PluginSpawnPoll::Pending => return ControlPoll::Pending,
            crate::runtime::PluginSpawnPoll::Ready(result) => result,
        };
        let disposition = completion
            .as_ref()
            .err()
            .and_then(|failure| failure.disposition);
        self.core_release_confirmed = matches!(
            disposition,
            Some(botster_core::SessionReservationRelease::Released)
        );
        let result = runtime.finish_managed_session_spawn(
            self.spawn.as_ref().expect("managed Core spawn exists"),
            self.prepared.as_ref().expect("managed worktree exists"),
            completion,
        );
        match result {
            Ok(spawned) if !self.deadline_elapsed() => {
                let delivery = self
                    .pending
                    .take()
                    .expect("managed request exists")
                    .respond(Ok(spawned));
                match delivery {
                    Ok(()) => {
                        // The live reused session now owns the path. A's Core
                        // reservation was Released before this right transferred.
                        self.inherited_creation = None;
                        // The delivered session's token moves to its record.
                        let spawn = self.spawn.as_mut().expect("managed Core spawn exists");
                        if !spawn.begin_handoff(runtime) {
                            self.phase = Phase::Handoff;
                            return ControlPoll::Pending;
                        }
                        self.submit_finalize(daemon, state, ManagedWorktreeDecision::Commit, None)
                    }
                    Err(crate::runtime::ManagedSpawnDelivery {
                        result: Ok(spawned),
                        parent,
                    }) => {
                        self.queue_undelivered_created_cleanup(runtime, &spawned);
                        runtime.cleanup_managed_session(&spawned);
                        self.deferred_error = Some(ManagedGitError::new(
                            "ensure_timed_out",
                            "the managed session caller left before delivery",
                        ));
                        drop(spawned);
                        drop(parent);
                        self.settle_undelivered_token(runtime)
                    }
                    Err(crate::runtime::ManagedSpawnDelivery { result: Err(_), .. }) => {
                        unreachable!("a successful managed spawn sent an error")
                    }
                }
            }
            Ok(spawned) => {
                self.queue_undelivered_created_cleanup(runtime, &spawned);
                runtime.cleanup_managed_session(&spawned);
                self.deferred_error = Some(timeout_error());
                self.settle_undelivered_token(runtime)
            }
            Err(error) => {
                self.deferred_error = Some(error);
                let created = self
                    .prepared
                    .as_ref()
                    .is_some_and(|prepared| prepared.created_worktree);
                match disposition {
                    Some(botster_core::SessionReservationRelease::Released) | None if created => {
                        self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
                    }
                    _ => self.finish_deferred_error(),
                }
            }
        }
    }

    /// An undelivered spawn whose token no created-worktree cleanup took
    /// (a reused worktree) hands the token to its record, so the record is
    /// the sole release owner. A removal during launch settles here first.
    fn settle_undelivered_token(&mut self, runtime: &crate::HubRuntime) -> ControlPoll {
        let spawn = self.spawn.as_mut().expect("managed Core spawn exists");
        let tracked = spawn.reservation.is_none()
            || runtime.created_worktree_cleanup_tracks(spawn.session_id());
        if !tracked && !spawn.begin_handoff(runtime) {
            self.phase = Phase::Handoff;
            return ControlPoll::Pending;
        }
        self.finish_deferred_error()
    }

    fn finalized_commit(
        &mut self,
        state: &mut DaemonControlState,
        result: HostResult,
    ) -> ControlPoll {
        match result {
            HostResult::ManagedWorktreeFinalized => self.finish_internal(),
            HostResult::ManagedWorktreeRecoveryRequired { prepared, error } => {
                self.retain_recovery(state, prepared, error)
            }
            HostResult::Failed { error, .. } => self.finish_error(managed_host_failure(error)),
            _ => self
                .finish_reconciliation("the host executor returned an invalid finalization result"),
        }
    }

    fn finalized_rollback(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
    ) -> ControlPoll {
        if let Some(worktree_id) = self
            .prepared
            .as_ref()
            .map(|prepared| prepared.worktree_id.clone())
            && let Some(runtime) = daemon.runtime()
        {
            runtime.finish_submitted_worktree_rollback(&worktree_id);
        }
        match result {
            HostResult::ManagedWorktreeFinalized => {
                let remove_record = self.record_committed
                    && self
                        .prepared
                        .as_ref()
                        .is_some_and(|prepared| prepared.created_worktree);
                if remove_record {
                    self.start_removal_write(daemon, state)
                } else {
                    self.finish_deferred_error()
                }
            }
            HostResult::ManagedWorktreeRecoveryRequired { prepared, error } => {
                self.retain_recovery(state, prepared, error)
            }
            HostResult::Failed { error, .. } => {
                if self.pending.is_none()
                    && let Some(prepared) = self.prepared.clone()
                {
                    return self.retain_recovery(state, prepared, error);
                }
                self.finish_error(managed_host_failure(error))
            }
            _ => {
                self.finish_reconciliation("the host executor returned an invalid rollback result")
            }
        }
    }

    fn submit_finalize(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        decision: ManagedWorktreeDecision,
        discard: Option<Box<PreparedMutation>>,
    ) -> ControlPoll {
        let phase = match decision {
            ManagedWorktreeDecision::Commit => Phase::FinalizeCommit,
            ManagedWorktreeDecision::Rollback => Phase::FinalizeRollback,
        };
        let prepared = self
            .prepared
            .as_ref()
            .expect("managed worktree exists")
            .clone();
        if matches!(decision, ManagedWorktreeDecision::Rollback)
            && let Some(runtime) = daemon.runtime()
        {
            runtime.begin_submitted_worktree_rollback(&prepared.worktree_id);
        }
        let poll = self.submit_host(
            daemon,
            state,
            phase,
            HostCommand::FinalizeManagedWorktree {
                prepared: prepared.clone(),
                decision,
                deadline: self.deadline,
                discard,
                #[cfg(test)]
                rollback_hold: daemon
                    .runtime()
                    .and_then(|runtime| runtime.test_rollback_git_hold()),
            },
        );
        if matches!(decision, ManagedWorktreeDecision::Rollback)
            && !matches!(poll, ControlPoll::Pending)
            && let Some(runtime) = daemon.runtime()
        {
            runtime.clear_submitted_worktree_rollback(&prepared.worktree_id);
        }
        poll
    }

    fn submit_host(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        phase: Phase,
        command: HostCommand,
    ) -> ControlPoll {
        let Some(permit) = self.permit.take() else {
            return self.finish_reconciliation("the managed Git operation lost its host permit");
        };
        let identity = HostJobIdentity {
            waiter_id: self.waiter_id,
            phase: self.next_host_phase,
        };
        let next_phase = self.next_host_phase.checked_add(1);
        let submitted = match (daemon.runtime(), next_phase) {
            (_, None) => Err(HostSubmissionFailure {
                error: HostSubmitError::PhaseExhausted,
                identity,
                command: Box::new(command),
                permit,
            }),
            (None, _) => Err(HostSubmissionFailure {
                error: HostSubmitError::Stopped,
                identity,
                command: Box::new(command),
                permit,
            }),
            (Some(runtime), Some(_)) => runtime.host_executor().submit(identity, command, permit),
        };
        match submitted {
            Ok(()) => {
                self.next_host_phase = next_phase.expect("submission requires a later phase");
                self.phase = phase;
                ControlPoll::Pending
            }
            Err(failure) => {
                let detail = submit_error_message(failure.error);
                retain_submission(state, failure);
                if let Some(HostRecoveryRequired::Submission {
                    managed_worktree, ..
                }) = state.host_recovery.get_mut(&self.waiter_id)
                {
                    *managed_worktree = self.prepared.take();
                }
                self.finish_reconciliation(detail)
            }
        }
    }

    fn queue_undelivered_created_cleanup(
        &mut self,
        runtime: &crate::runtime::HubRuntime,
        spawned: &crate::runtime::PluginManagedSessionSpawned,
    ) {
        let Some(reservation) = self
            .spawn
            .as_ref()
            .and_then(|start| start.reservation.clone())
        else {
            return;
        };
        let Some(prepared) = self
            .inherited_creation
            .take()
            .or_else(|| self.prepared.clone())
        else {
            return;
        };
        runtime.queue_created_worktree_cleanup(
            botster_core::SessionId(spawned.session_id.clone()),
            prepared,
            reservation,
        );
    }

    fn deadline_elapsed(&self) -> bool {
        Instant::now() >= self.deadline
    }

    fn finish_deferred_error(&mut self) -> ControlPoll {
        let error = self.deferred_error.take().unwrap_or_else(|| {
            ManagedGitError::new("spawn_failed", "the managed session spawn failed")
        });
        self.finish_error(error)
    }

    fn finish_error(&mut self, error: ManagedGitError) -> ControlPoll {
        if let Some(pending) = self.pending.take() {
            let _ = pending.respond(Err(error));
        }
        self.finish_internal()
    }

    fn finish_reconciliation(&mut self, detail: &str) -> ControlPoll {
        self.finish_error(ManagedGitError::new(
            "reconciliation_required",
            detail.to_string(),
        ))
    }

    fn retain_recovery(
        &mut self,
        state: &mut DaemonControlState,
        prepared: PreparedManagedWorktree,
        error: crate::host_executor::HostError,
    ) -> ControlPoll {
        self.retain_recovery_with_code(state, prepared, error, "reconciliation_required")
    }

    fn retain_recovery_with_code(
        &mut self,
        state: &mut DaemonControlState,
        prepared: PreparedManagedWorktree,
        error: crate::host_executor::HostError,
        response_code: &'static str,
    ) -> ControlPoll {
        let Some(permit) = self.permit.take() else {
            return self
                .finish_reconciliation("the managed Git recovery result lost its host permit");
        };
        let detail = format!("{}: {}", error.code, error.message);
        state.host_recovery.insert(
            self.waiter_id,
            HostRecoveryRequired::ManagedGit(ManagedGitRecoveryRequired {
                code: error.code,
                message: error.message,
                _prepared: prepared,
                _permit: permit,
            }),
        );
        self.finish_error(ManagedGitError::new(response_code, detail))
    }

    fn finish_internal(&mut self) -> ControlPoll {
        self.phase = Phase::Done;
        drop(self.permit.take());
        let response =
            crate::client_api_dto::response::daemon_response_base(DaemonResponseKind::Worktrees);
        ControlPoll::Ready(Ok(response))
    }

    fn finish_inherited_creation(&mut self, runtime: &crate::HubRuntime) {
        let Some(prepared) = self.inherited_creation.take() else {
            return;
        };
        let reservation = self
            .spawn
            .as_ref()
            .and_then(|start| start.reservation.clone());
        if let Some(reservation) = reservation
            && !self.core_release_confirmed
        {
            let session_id = reservation.session_id().clone();
            // A release that never returns Released keeps this cleanup live.
            // Git rollback cannot begin while that Core obligation is unresolved.
            runtime.queue_created_worktree_cleanup(session_id, prepared, reservation);
        } else {
            runtime.defer_confirmed_worktree_rollback(prepared);
            runtime.wake_remaining_confirmed_worktree_rollbacks();
        }
    }

    fn finish_active_attempt(&mut self) {
        if let Some(spawner) = self.spawner.take() {
            spawner.finish_managed_attempt(&self.worktree_id, self.waiter_id);
        }
    }
}

impl Drop for ManagedSpawnOperation {
    fn drop(&mut self) {
        // Terminal disposal retains this continuation until Host finishes.
        self.finish_active_attempt();
    }
}

fn managed_host_failure(error: crate::host_executor::HostError) -> ManagedGitError {
    let kind = if error.code == "host_worker_panicked" {
        "host_worker_panicked"
    } else {
        "host_execution_failed"
    };
    ManagedGitError::new(kind, format!("{}: {}", error.code, error.message))
}

fn timeout_error() -> ManagedGitError {
    ManagedGitError::new(
        "ensure_timed_out",
        "the managed Git operation exceeded its owner deadline",
    )
}

fn submit_error_message(error: HostSubmitError) -> &'static str {
    match error {
        HostSubmitError::WrongExecutor => {
            "the managed Git phase used a permit from another executor"
        }
        HostSubmitError::Full => "the bounded host queue refused a reserved managed Git phase",
        HostSubmitError::Stopped => "the host executor stopped during managed Git work",
        HostSubmitError::PhaseExhausted => "the managed Git host phase identity is exhausted",
    }
}
