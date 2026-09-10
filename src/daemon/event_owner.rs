//! Retained worker dispatch for queued event router operations.

use crate::HubDaemon;
use crate::daemon::owner_budget::OwnerPermit;
use crate::daemon::owner_loop::DaemonControlState;
use crate::host_executor::{
    HostCommand, HostCompletion, HostJobIdentity, HostResult, HostSubmissionFailure,
    HostSubmitError, HostWorkPermit,
};
use crate::package_event_router::{EventOwnerWork, EventOwnerWorkId, OwnerStep};

struct Pending {
    identity: HostJobIdentity,
    event_identity: EventOwnerWorkId,
    owner_permit: OwnerPermit,
}

enum Recovery {
    Completion {
        _pending: Pending,
        _completion: HostCompletion,
    },
    Submission {
        _pending: Pending,
        _failure: HostSubmissionFailure,
    },
}

#[derive(Default)]
pub(crate) struct EventOwnerState {
    terminal: Option<crate::host_disposal::Job>,
    terminal_owner: Option<OwnerPermit>,
    terminal_retirement_fault: Option<HostWorkPermit>,
    pending: Option<Pending>,
    completion: Option<HostCompletion>,
    recovery: Option<Recovery>,
    pub(crate) waiting_for_host: bool,
    pub(crate) waiting_for_owner: bool,
}

impl EventOwnerState {
    pub(crate) fn dispose_terminal(
        &mut self,
        runtime: &crate::HubRuntime,
        budget: &mut crate::daemon::owner_budget::OwnerBudget,
    ) -> bool {
        if self.terminal_retirement_fault.is_some() {
            return false;
        }
        if let Some(job) = self.terminal.as_mut() {
            if let crate::host_disposal::Poll::Disposed(permit) = job.poll() {
                if let Some(pending) = self.pending.take() {
                    let Some(retired) =
                        runtime.retire_terminal_event_plane_owner_op(&pending.event_identity)
                    else {
                        self.pending = Some(pending);
                        self.terminal_retirement_fault = Some(permit);
                        return false;
                    };
                    self.terminal_owner = Some(pending.owner_permit);
                    self.terminal = Some(crate::host_disposal::Job::new(
                        crate::host_disposal::Parts {
                            identity: pending.identity,
                            permit,
                            model: None,
                            payload: Box::new((retired, pending.event_identity)),
                        },
                    ));
                    return false;
                }
                if let Some(owner) = self.terminal_owner.take() {
                    budget.release(owner);
                }
                drop(permit);
                self.terminal.take();
                return true;
            }
            return false;
        }
        let (identity, permit, payload): (_, _, Box<dyn Send>) =
            if let Some(completion) = self.completion.take() {
                let (identity, result, permit) = completion.into_parts();
                (identity, permit, Box::new(result))
            } else if let Some(recovery) = self.recovery.take() {
                match recovery {
                    Recovery::Completion {
                        _pending,
                        _completion,
                    } => {
                        self.pending = Some(_pending);
                        let (identity, result, permit) = _completion.into_parts();
                        (identity, permit, Box::new(result))
                    }
                    Recovery::Submission { _pending, _failure } => {
                        self.pending = Some(_pending);
                        (
                            _failure.identity,
                            _failure.permit,
                            Box::new(_failure.command),
                        )
                    }
                }
            } else {
                return self.pending.is_none();
            };
        self.terminal = Some(crate::host_disposal::Job::new(
            crate::host_disposal::Parts {
                identity,
                permit,
                model: None,
                payload,
            },
        ));
        false
    }

    pub(crate) fn accepts(&self, identity: HostJobIdentity) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.identity == identity)
            && self.completion.is_none()
    }

    pub(crate) fn retain_completion(&mut self, completion: HostCompletion) {
        self.completion = Some(completion);
    }
}

/// Run one phase. Waiting work resumes only after a completion or capacity notification.
pub(crate) fn drive(daemon: &HubDaemon, state: &mut DaemonControlState) -> bool {
    let Some(runtime) = daemon.runtime() else {
        return false;
    };
    if state.event_owner.recovery.is_some() {
        return false;
    }
    if let Some(completion) = state.event_owner.completion.take() {
        let pending = state
            .event_owner
            .pending
            .take()
            .expect("an accepted completion has a pending dispatch");
        match &completion.result {
            HostResult::EventOwner(Ok(done)) if done.identity() == &pending.event_identity => {}
            HostResult::Failed { error, .. } if error.code == "host_worker_panicked" => {
                let Some(work) = runtime.restart_event_plane_owner_op(&pending.event_identity)
                else {
                    state.event_owner.recovery = Some(Recovery::Completion {
                        _pending: pending,
                        _completion: completion,
                    });
                    return false;
                };
                let (_, _, permit) = completion.into_parts();
                submit(daemon, state, pending, work, permit);
                return false;
            }
            _ => {
                // A terminal fault retains the work, its byte charges, and both permits.
                eprintln!("event router cleanup requires recovery");
                state.event_owner.recovery = Some(Recovery::Completion {
                    _pending: pending,
                    _completion: completion,
                });
                return false;
            }
        }
        let (_, result, permit) = completion.into_parts();
        let HostResult::EventOwner(Ok(done)) = result else {
            unreachable!()
        };
        let completed = runtime.complete_event_plane_owner_op(done);
        assert!(
            completed.is_some(),
            "the exact event completion retains its queued operation"
        );
        drop(permit);
        state.budget.release(pending.owner_permit);
        crate::daemon::control::pending::wake_shutdown_waiter(state);
        return runtime.event_plane_owner_op_ready();
    }
    if state.event_owner.pending.is_some() || !runtime.event_plane_owner_op_ready() {
        return false;
    }
    let Some(owner_permit) = state.budget.reserve() else {
        state.event_owner.waiting_for_owner = true;
        return false;
    };
    state.event_owner.waiting_for_owner = false;
    let Some(permit) = runtime.host_executor().try_reserve() else {
        state.budget.release(owner_permit);
        state.event_owner.waiting_for_host = true;
        return false;
    };
    state.event_owner.waiting_for_host = false;
    match runtime.step_event_plane_owner_op() {
        OwnerStep::Work(work) => {
            let waiter_id = state
                .waiter_ids
                .next()
                .expect("event dispatch identity is available");
            let pending = Pending {
                identity: HostJobIdentity {
                    waiter_id,
                    phase: 0,
                },
                event_identity: work.identity().clone(),
                owner_permit,
            };
            submit(daemon, state, pending, work, permit);
            false
        }
        OwnerStep::Applied(_) | OwnerStep::Idle | OwnerStep::Waiting => {
            drop(permit);
            state.budget.release(owner_permit);
            crate::daemon::control::pending::wake_shutdown_waiter(state);
            runtime.event_plane_owner_op_ready()
        }
    }
}

fn submit(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    mut pending: Pending,
    work: EventOwnerWork,
    permit: HostWorkPermit,
) {
    let runtime = daemon.runtime().expect("event cleanup retains its runtime");
    pending.event_identity = work.identity().clone();
    let command = HostCommand::EventOwner {
        router: runtime.package_event_router().clone(),
        work,
    };
    let Some(phase) = pending.identity.phase.checked_add(1) else {
        let failure = HostSubmissionFailure {
            error: HostSubmitError::PhaseExhausted,
            identity: pending.identity,
            command,
            permit,
        };
        state.event_owner.recovery = Some(Recovery::Submission {
            _pending: pending,
            _failure: failure,
        });
        return;
    };
    pending.identity.phase = phase;
    match runtime
        .host_executor()
        .submit(pending.identity, command, permit)
    {
        Ok(()) => state.event_owner.pending = Some(pending),
        Err(failure) => {
            eprintln!(
                "event router cleanup submission requires recovery: {:?}",
                failure.error
            );
            state.event_owner.recovery = Some(Recovery::Submission {
                _pending: pending,
                _failure: failure,
            });
        }
    }
}
