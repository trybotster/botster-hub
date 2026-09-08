//! Owner-routed managed worktree and session spawn operation.

use std::time::Instant;

use botster_hub_client::{DaemonRequest, DaemonResponseKind};

use crate::HubDaemon;
use crate::daemon::control::host_work::{DocumentAdmission, admit_document, release_document};
use crate::daemon::control::pending::{
    ControlPoll, PendingControlRequest, READY_DEADLINE, READY_INITIAL, mark_request_ready,
};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::ReadyClass;
use crate::host_executor::{
    HostCommand, HostJobIdentity, HostResult, HostSubmitError, HostWorkPermit,
};
use crate::host_mutations::{
    HostCommit, HostMutationCommand, HostMutationResult, HostPrepare, PreparedMutation,
    RecoveryOutcome,
};
use crate::managed_git_worktrees::{
    MANAGED_GIT_OPERATION_TIMEOUT, ManagedGitError, ManagedWorktreeDecision,
    PreparedManagedWorktree,
};
use crate::owner_identity::WaiterId;
use crate::runtime::{ManagedSessionSpawnStart, PendingManagedSessionSpawn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Create,
    PrepareRecord,
    ParkRecord,
    CommitRecord,
    Spawn,
    FinalizeCommit,
    FinalizeRollback,
    PrepareRemoval,
    ParkRemoval,
    CommitRemoval,
    Done,
}

struct ManagedSpawnOperation {
    waiter_id: WaiterId,
    pending: Option<PendingManagedSessionSpawn>,
    prepared: Option<PreparedManagedWorktree>,
    prepared_mutation: Option<Box<PreparedMutation>>,
    spawn: Option<ManagedSessionSpawnStart>,
    permit: Option<HostWorkPermit>,
    phase: Phase,
    next_host_phase: u64,
    record_committed: bool,
    deferred_error: Option<ManagedGitError>,
    deadline: Instant,
}

pub(crate) fn accept_one(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    let Some(pending) = runtime.take_pending_managed_spawn() else {
        return;
    };
    let request = match runtime.validate_managed_git_request(&pending) {
        Ok(request) => request,
        Err(error) => {
            let _ = pending.response.send(Err(error));
            return;
        }
    };
    let Some(owner_permit) = state.budget.reserve() else {
        let _ = pending.response.send(Err(ManagedGitError::new(
            "ensure_backpressured",
            "the Hub owner has no available operation slot",
        )));
        return;
    };
    let Some(waiter_id) = state.waiter_ids.next() else {
        state.budget.release(owner_permit);
        let _ = pending.response.send(Err(ManagedGitError::new(
            "ensure_unavailable",
            "the Hub owner exhausted unique operation identifiers",
        )));
        return;
    };
    let Some(host_permit) = runtime.host_executor().try_reserve() else {
        state.budget.release(owner_permit);
        let _ = pending.response.send(Err(ManagedGitError::new(
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
        state.budget.release(owner_permit);
        let _ = pending.response.send(Err(ManagedGitError::new(
            "ensure_unavailable",
            "the host executor stopped before it accepted managed Git work",
        )));
        return;
    }

    let accepted_at = pending.accepted_at;
    let mut operation = ManagedSpawnOperation {
        waiter_id,
        pending: Some(pending),
        prepared: None,
        prepared_mutation: None,
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
            ready_class: ReadyClass::HostCompletion,
            ready_key: None,
            deadline_key: None,
            last_core_phase: 0,
            last_host_phase: 0,
            request: DaemonRequest::ListWorktrees,
            reply_tx,
            response_delivery_rx: None,
            grant_id: None,
            client: None,
            permit: Some(owner_permit),
            accepted_at,
            must_finish: true,
            past_deadline: false,
            continuation: Box::new(move |daemon, state| operation.poll(daemon, state)),
            retire: None,
        },
    );
    let deadline = accepted_at + MANAGED_GIT_OPERATION_TIMEOUT;
    let arm = state
        .request_deadlines
        .arm(waiter_id, deadline, Instant::now())
        .expect("a new managed Git deadline must make progress");
    state
        .pending_requests
        .get_mut(&waiter_id)
        .expect("the managed Git waiter was inserted")
        .deadline_key = Some(arm.key());
    mark_request_ready(state, waiter_id, ReadyClass::HostCompletion, READY_INITIAL);
    if arm.is_due() {
        mark_request_ready(state, waiter_id, ReadyClass::Deadline, READY_DEADLINE);
    }
}

impl ManagedSpawnOperation {
    fn poll(&mut self, daemon: &mut HubDaemon, state: &mut DaemonControlState) -> ControlPoll {
        if matches!(self.phase, Phase::ParkRecord | Phase::ParkRemoval) {
            return self.admit_parked(daemon, state);
        }
        if self.phase == Phase::Spawn {
            return self.poll_spawn(daemon, state);
        }
        let Some(completion) = state.host_completions.remove(&self.waiter_id) else {
            return ControlPoll::Pending;
        };
        let (_, result, permit) = completion.into_parts();
        self.permit = Some(permit);
        match self.phase {
            Phase::Create => self.created(daemon, state, result),
            Phase::PrepareRecord => self.record_prepared(daemon, state, result),
            Phase::CommitRecord => self.record_committed(daemon, state, result),
            Phase::FinalizeCommit => self.finalized_commit(result),
            Phase::FinalizeRollback => self.finalized_rollback(daemon, state, result),
            Phase::PrepareRemoval => self.removal_prepared(daemon, state, result),
            Phase::CommitRemoval => self.removal_committed(daemon, state, result),
            Phase::ParkRecord | Phase::ParkRemoval | Phase::Spawn | Phase::Done => self
                .finish_reconciliation(
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
                self.submit_record_prepare(daemon, state, None)
            }
            HostResult::ManagedWorktreeRecoveryRequired { error, .. } => {
                self.finish_reconciliation(&format!("{}: {}", error.code, error.message))
            }
            HostResult::Failed { error, .. } => self.finish_error(ManagedGitError::new(
                "ensure_unavailable",
                format!("{}: {}", error.code, error.message),
            )),
            _ => self.finish_reconciliation("the host executor returned an invalid create result"),
        }
    }

    fn record_prepared(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
    ) -> ControlPoll {
        match result {
            HostResult::Mutation(HostMutationResult::Prepared(prepared)) => {
                self.prepared_mutation = Some(Box::new(prepared));
                self.admit_record(daemon, state, false)
            }
            HostResult::Mutation(HostMutationResult::Failed(error)) => {
                self.deferred_error =
                    Some(ManagedGitError::new("persistence_failed", error.message));
                self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
            }
            _ => self.finish_reconciliation(
                "the host executor returned an invalid record preparation result",
            ),
        }
    }

    fn admit_parked(&mut self, daemon: &HubDaemon, state: &mut DaemonControlState) -> ControlPoll {
        match self.phase {
            Phase::ParkRecord => self.admit_record(daemon, state, true),
            Phase::ParkRemoval => self.admit_removal(daemon, state, true),
            _ => ControlPoll::Pending,
        }
    }

    fn admit_record(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        parked: bool,
    ) -> ControlPoll {
        let base_revision = self
            .prepared_mutation
            .as_ref()
            .expect("record preparation exists")
            .base_revision;
        match admit_document(state, self.waiter_id, base_revision, daemon.state_view().0) {
            DocumentAdmission::Granted => {
                let prepared = *self
                    .prepared_mutation
                    .take()
                    .expect("record preparation exists");
                self.submit_host(
                    daemon,
                    state,
                    Phase::CommitRecord,
                    HostCommand::Mutation(HostMutationCommand::Commit(HostCommit { prepared })),
                )
            }
            DocumentAdmission::Busy => {
                self.phase = Phase::ParkRecord;
                ControlPoll::Pending
            }
            DocumentAdmission::Stale => {
                if parked {
                    state.document_waiters.remove(&self.waiter_id);
                }
                let superseded = self.prepared_mutation.take();
                self.submit_record_prepare(daemon, state, superseded)
            }
        }
    }

    fn record_committed(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
    ) -> ControlPoll {
        match result {
            HostResult::Mutation(HostMutationResult::Committed(committed)) => {
                if committed.committed_revision != daemon.state_view().0.saturating_add(1) {
                    release_document(state, self.waiter_id);
                    self.deferred_error = Some(ManagedGitError::new(
                        "reconciliation_required",
                        "the managed worktree commit revision is not the next Hub revision",
                    ));
                    return self.submit_finalize(
                        daemon,
                        state,
                        ManagedWorktreeDecision::Rollback,
                        None,
                    );
                }
                daemon.publish_state(committed.view);
                release_document(state, self.waiter_id);
                self.record_committed = true;
                if self.deadline_elapsed() {
                    self.deferred_error = Some(timeout_error());
                    return self.submit_finalize(
                        daemon,
                        state,
                        ManagedWorktreeDecision::Rollback,
                        None,
                    );
                }
                let Some(runtime) = daemon.runtime() else {
                    self.deferred_error = Some(ManagedGitError::new(
                        "spawn_failed",
                        "the Hub runtime stopped before the session spawn",
                    ));
                    return self.submit_finalize(
                        daemon,
                        state,
                        ManagedWorktreeDecision::Rollback,
                        None,
                    );
                };
                let start = runtime.spawn_prepared_managed_session(
                    self.pending.as_ref().expect("managed request exists"),
                    self.prepared.as_ref().expect("managed worktree exists"),
                    Some(self.waiter_id),
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
            HostResult::Mutation(HostMutationResult::Failed(error)) => {
                release_document(state, self.waiter_id);
                self.deferred_error =
                    Some(ManagedGitError::new("persistence_failed", error.message));
                self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
            }
            HostResult::Mutation(HostMutationResult::Recovered(
                RecoveryOutcome::RegisteredWorktree { failure, .. },
            )) => {
                release_document(state, self.waiter_id);
                self.deferred_error =
                    Some(ManagedGitError::new("persistence_failed", failure.message));
                self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
            }
            _ => {
                release_document(state, self.waiter_id);
                self.finish_reconciliation(
                    "the host executor returned an invalid record commit result",
                )
            }
        }
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
        let completion = match start.tracker.poll(runtime) {
            crate::data_plane::driver::CoreTicketPoll::Pending => return ControlPoll::Pending,
            crate::data_plane::driver::CoreTicketPoll::Lost => {
                Err(botster_core_daemon::CoreDaemonError::Shutdown)
            }
            crate::data_plane::driver::CoreTicketPoll::Refused => {
                Err(botster_core_daemon::CoreDaemonError::Shutdown)
            }
            crate::data_plane::driver::CoreTicketPoll::Ready(Err(error)) => Err(error),
            crate::data_plane::driver::CoreTicketPoll::Ready(Ok(
                botster_core_daemon::CoreCompletion::Spawn { result, .. },
            )) => result,
            crate::data_plane::driver::CoreTicketPoll::Ready(Ok(_)) => {
                Err(botster_core_daemon::CoreDaemonError::Shutdown)
            }
        };
        let result = runtime.finish_managed_session_spawn(
            self.spawn.as_ref().expect("managed Core spawn exists"),
            self.prepared.as_ref().expect("managed worktree exists"),
            completion,
        );
        match result {
            Ok(spawned) if !self.deadline_elapsed() => {
                let delivered = self
                    .pending
                    .take()
                    .expect("managed request exists")
                    .response
                    .send(Ok(spawned.clone()))
                    .is_ok();
                if delivered {
                    self.submit_finalize(daemon, state, ManagedWorktreeDecision::Commit, None)
                } else {
                    runtime.cleanup_managed_session(&spawned);
                    self.deferred_error = Some(ManagedGitError::new(
                        "ensure_timed_out",
                        "the managed session caller left before delivery",
                    ));
                    self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
                }
            }
            Ok(spawned) => {
                runtime.cleanup_managed_session(&spawned);
                self.deferred_error = Some(timeout_error());
                self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
            }
            Err(error) => {
                self.deferred_error = Some(error);
                self.submit_finalize(daemon, state, ManagedWorktreeDecision::Rollback, None)
            }
        }
    }

    fn finalized_commit(&mut self, result: HostResult) -> ControlPoll {
        match result {
            HostResult::ManagedWorktreeFinalized => self.finish_internal(),
            HostResult::ManagedWorktreeRecoveryRequired { error, .. } => {
                self.finish_reconciliation(&format!("{}: {}", error.code, error.message))
            }
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
        match result {
            HostResult::ManagedWorktreeFinalized => {
                let remove_record = self.record_committed
                    && self
                        .prepared
                        .as_ref()
                        .is_some_and(|prepared| prepared.created_worktree);
                if remove_record {
                    self.submit_removal_prepare(daemon, state, None)
                } else {
                    self.finish_deferred_error()
                }
            }
            HostResult::ManagedWorktreeRecoveryRequired { error, .. } => {
                self.finish_reconciliation(&format!("{}: {}", error.code, error.message))
            }
            _ => {
                self.finish_reconciliation("the host executor returned an invalid rollback result")
            }
        }
    }

    fn removal_prepared(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
    ) -> ControlPoll {
        match result {
            HostResult::Mutation(HostMutationResult::Prepared(prepared)) => {
                self.prepared_mutation = Some(Box::new(prepared));
                self.admit_removal(daemon, state, false)
            }
            HostResult::Mutation(HostMutationResult::Failed(error)) => self.finish_reconciliation(
                &format!("managed worktree record removal failed: {}", error.message),
            ),
            _ => self.finish_reconciliation(
                "the host executor returned an invalid removal preparation result",
            ),
        }
    }

    fn admit_removal(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        parked: bool,
    ) -> ControlPoll {
        let base_revision = self
            .prepared_mutation
            .as_ref()
            .expect("record removal exists")
            .base_revision;
        match admit_document(state, self.waiter_id, base_revision, daemon.state_view().0) {
            DocumentAdmission::Granted => {
                let prepared = *self
                    .prepared_mutation
                    .take()
                    .expect("record removal exists");
                self.submit_host(
                    daemon,
                    state,
                    Phase::CommitRemoval,
                    HostCommand::Mutation(HostMutationCommand::Commit(HostCommit { prepared })),
                )
            }
            DocumentAdmission::Busy => {
                self.phase = Phase::ParkRemoval;
                ControlPoll::Pending
            }
            DocumentAdmission::Stale => {
                if parked {
                    state.document_waiters.remove(&self.waiter_id);
                }
                let superseded = self.prepared_mutation.take();
                self.submit_removal_prepare(daemon, state, superseded)
            }
        }
    }

    fn removal_committed(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        result: HostResult,
    ) -> ControlPoll {
        match result {
            HostResult::Mutation(HostMutationResult::Committed(committed)) => {
                if committed.committed_revision != daemon.state_view().0.saturating_add(1) {
                    release_document(state, self.waiter_id);
                    return self.finish_reconciliation(
                        "the managed worktree removal revision is not the next Hub revision",
                    );
                }
                daemon.publish_state(committed.view);
                release_document(state, self.waiter_id);
                self.finish_deferred_error()
            }
            HostResult::Mutation(HostMutationResult::Failed(error)) => {
                release_document(state, self.waiter_id);
                self.finish_reconciliation(&format!(
                    "managed worktree record removal failed: {}",
                    error.message
                ))
            }
            HostResult::Mutation(HostMutationResult::Recovered(
                RecoveryOutcome::RegisteredWorktree { failure, .. },
            )) => {
                release_document(state, self.waiter_id);
                self.finish_reconciliation(&format!(
                    "managed worktree record removal failed: {}",
                    failure.message
                ))
            }
            _ => {
                release_document(state, self.waiter_id);
                self.finish_reconciliation(
                    "the host executor returned an invalid removal commit result",
                )
            }
        }
    }

    fn submit_record_prepare(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        superseded: Option<Box<PreparedMutation>>,
    ) -> ControlPoll {
        let (base_revision, view) = daemon.state_view();
        let command = HostPrepare::ManagedWorktree {
            worktree: self
                .prepared
                .as_ref()
                .expect("managed worktree exists")
                .worktree(),
            base_revision,
            state: view,
            data_directory: daemon
                .runtime()
                .expect("managed operation requires runtime")
                .config()
                .data_directory
                .clone(),
            superseded,
        };
        self.submit_host(
            daemon,
            state,
            Phase::PrepareRecord,
            HostCommand::Mutation(HostMutationCommand::Prepare(command)),
        )
    }

    fn submit_removal_prepare(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        superseded: Option<Box<PreparedMutation>>,
    ) -> ControlPoll {
        let (base_revision, view) = daemon.state_view();
        let command = HostPrepare::RemoveManagedWorktree {
            worktree_id: self
                .prepared
                .as_ref()
                .expect("managed worktree exists")
                .worktree_id
                .clone(),
            base_revision,
            state: view,
            data_directory: daemon
                .runtime()
                .expect("managed operation requires runtime")
                .config()
                .data_directory
                .clone(),
            superseded,
        };
        self.submit_host(
            daemon,
            state,
            Phase::PrepareRemoval,
            HostCommand::Mutation(HostMutationCommand::Prepare(command)),
        )
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
        self.submit_host(
            daemon,
            state,
            phase,
            HostCommand::FinalizeManagedWorktree {
                prepared,
                decision,
                deadline: self.deadline,
                discard,
            },
        )
    }

    fn submit_host(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        phase: Phase,
        command: HostCommand,
    ) -> ControlPoll {
        let Some(runtime) = daemon.runtime() else {
            if state.document_owner == Some(self.waiter_id) {
                release_document(state, self.waiter_id);
            }
            return self.finish_reconciliation("the Hub runtime stopped during managed Git work");
        };
        let Some(permit) = self.permit.take() else {
            return self.finish_reconciliation("the managed Git operation lost its host permit");
        };
        let identity = HostJobIdentity {
            waiter_id: self.waiter_id,
            phase: self.next_host_phase,
        };
        match runtime.host_executor().submit(identity, command, permit) {
            Ok(()) => {
                self.next_host_phase = self.next_host_phase.saturating_add(1);
                self.phase = phase;
                ControlPoll::Pending
            }
            Err(error) => {
                if state.document_owner == Some(self.waiter_id) {
                    release_document(state, self.waiter_id);
                }
                self.finish_reconciliation(submit_error_message(error))
            }
        }
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
            let _ = pending.response.send(Err(error));
        }
        self.finish_internal()
    }

    fn finish_reconciliation(&mut self, detail: &str) -> ControlPoll {
        self.finish_error(ManagedGitError::new(
            "reconciliation_required",
            detail.to_string(),
        ))
    }

    fn finish_internal(&mut self) -> ControlPoll {
        self.phase = Phase::Done;
        drop(self.permit.take());
        let response =
            crate::client_api_dto::response::daemon_response_base(DaemonResponseKind::Worktrees);
        ControlPoll::Ready(Ok(response))
    }
}

fn timeout_error() -> ManagedGitError {
    ManagedGitError::new(
        "ensure_timed_out",
        "the managed Git operation exceeded its owner deadline",
    )
}

fn submit_error_message(error: HostSubmitError) -> &'static str {
    match error {
        HostSubmitError::Full => "the bounded host queue refused a reserved managed Git phase",
        HostSubmitError::Stopped => "the host executor stopped during managed Git work",
    }
}
