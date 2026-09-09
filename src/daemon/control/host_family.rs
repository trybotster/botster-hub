//! Package cleanup retains each Host phase through exact causal table application.

use crate::HubDaemon;
use crate::daemon::owner_loop::DaemonControlState;
use crate::host_executor::{
    HostCommand, HostCompletion, HostJobIdentity, HostResult, HostSubmissionFailure, HostWorkPermit,
};
use crate::host_mutations::HostMutationResult;
use crate::owner_identity::WaiterId;
use crate::runtime::CausalTransitionStatus;
use crate::runtime::entity_model::{Detached, Operation, Work};
use crate::runtime::family_cleanup::CleanupPhase;

pub(crate) enum Poll {
    Pending,
    Again,
    Complete(HostMutationResult, HostWorkPermit),
    Fault,
}

enum Handle {
    Live(Work),
    Detached(Detached),
}

impl Handle {
    fn work(&self) -> &Work {
        match self {
            Self::Live(work) => work,
            Self::Detached(work) => work.work(),
        }
    }
}

pub(crate) struct FamilyWork {
    pub(super) generation_fault: Option<crate::runtime::PackageEntityCleanupError>,
    result: Option<HostMutationResult>,
    operation: Option<Operation>,
    handle: Option<Handle>,
    completion: Option<HostCompletion>,
    permit: Option<HostWorkPermit>,
    submission: Option<HostSubmissionFailure>,
    identity: Option<HostJobIdentity>,
}

impl FamilyWork {
    #[cfg(test)]
    pub(crate) fn test_retained_release(&self) -> bool {
        self.completion.is_some()
            && self
                .handle
                .as_ref()
                .is_some_and(|handle| handle.work().test_cleanup_retains_release())
    }

    pub(crate) fn new(mut result: HostMutationResult, permit: HostWorkPermit) -> Self {
        let cleanup = std::mem::take(
            super::host_work::package_event_cleanup(&mut result)
                .expect("the package result retains cleanup"),
        );
        Self {
            generation_fault: None,
            result: Some(result),
            operation: Some(Operation::Cleanup {
                detached: false,
                retained: Some(cleanup),
                next: None,
                fault: None,
            }),
            handle: None,
            completion: None,
            permit: Some(permit),
            submission: None,
            identity: None,
        }
    }

    pub(crate) fn poll(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        waiter_id: WaiterId,
        next_phase: &mut u64,
    ) -> Poll {
        let runtime = daemon.runtime().expect("cleanup retains its runtime");
        if self.handle.is_some() && self.permit.is_none() {
            if self.completion.is_none() {
                self.completion = state.host_completions.remove(&waiter_id);
            }
            let Some(completion) = self.completion.as_ref() else {
                return Poll::Pending;
            };
            let handle = self.handle.as_mut().unwrap();
            let status = if self.identity != Some(completion.identity) {
                CausalTransitionStatus::Fault
            } else if let HostResult::EntityModelComplete(kind) = completion.result {
                match handle {
                    Handle::Live(_) => runtime.observe_entity_model(completion.identity, kind),
                    Handle::Detached(work) => work.observe(runtime, completion.identity, kind),
                }
            } else {
                CausalTransitionStatus::Fault
            };
            match status {
                CausalTransitionStatus::Waiting => {
                    state
                        .family_cleanup_waiters
                        .insert(waiter_id, completion.identity.phase);
                    return Poll::Pending;
                }
                CausalTransitionStatus::Fault => {
                    match handle {
                        Handle::Live(_) => runtime.fault_entity_model(self.identity.unwrap()),
                        Handle::Detached(work) => work.fault(),
                    }
                    return Poll::Fault;
                }
                CausalTransitionStatus::Applied => {}
            }
            state.family_cleanup_waiters.remove(&waiter_id);
            let mut output = match handle {
                Handle::Live(work) => runtime.entity_model_output(work),
                Handle::Detached(work) => work.output(),
            }
            .expect("exact completion permits output access");
            if !output.valid {
                if let Some(Operation::Cleanup { fault, .. }) = output.operation.as_ref() {
                    self.generation_fault = *fault;
                }
                drop(output);
                match handle {
                    Handle::Live(_) => runtime.fault_entity_model(self.identity.unwrap()),
                    Handle::Detached(work) => work.fault(),
                }
                return Poll::Fault;
            }
            let Some(Operation::Cleanup {
                next: Some(next), ..
            }) = output.operation.as_ref()
            else {
                panic!("completed cleanup retains its next phase");
            };
            let next = *next;
            let cleanup = output
                .take_cleanup()
                .expect("the shared record retains cleanup");
            if next == CleanupPhase::Complete {
                *super::host_work::package_event_cleanup(self.result.as_mut().unwrap()).unwrap() =
                    cleanup;
            } else {
                self.operation = Some(Operation::Cleanup {
                    detached: next == CleanupPhase::Detached,
                    retained: Some(cleanup),
                    next: None,
                    fault: None,
                });
            }
            drop(output);
            assert!(match handle {
                Handle::Live(work) => runtime.release_entity_model(work),
                Handle::Detached(work) => work.release(),
            });
            assert!(handle.work().owner_drop_ready());
            drop(self.handle.take());
            let (_, _, permit) = self.completion.take().unwrap().into_parts();
            if next == CleanupPhase::Complete {
                return Poll::Complete(self.result.take().unwrap(), permit);
            }
            self.permit = Some(permit);
            self.identity = None;
            return Poll::Again;
        }
        let identity = HostJobIdentity {
            waiter_id,
            phase: *next_phase,
        };
        let Some(later_phase) = next_phase.checked_add(1) else {
            return Poll::Fault;
        };
        let operation = self
            .operation
            .take()
            .expect("cleanup retains its next operation");
        let permit = self.permit.as_ref().expect("cleanup retains Host capacity");
        let detached = matches!(operation, Operation::Cleanup { detached: true, .. });
        let granted = if detached {
            runtime
                .begin_detached_entity_cleanup(identity, operation, permit)
                .map(Handle::Detached)
        } else {
            runtime
                .begin_entity_model(identity, operation, permit)
                .map(Handle::Live)
        };
        let handle = match granted {
            Ok(handle) => handle,
            Err((status, operation)) => {
                self.operation = Some(operation);
                if status == CausalTransitionStatus::Waiting {
                    state
                        .family_cleanup_waiters
                        .insert(waiter_id, identity.phase);
                    return Poll::Pending;
                }
                return Poll::Fault;
            }
        };
        state.family_cleanup_waiters.remove(&waiter_id);
        self.identity = Some(identity);
        self.handle = Some(handle);
        *next_phase = later_phase;
        let command = HostCommand::EntityModel(self.handle.as_ref().unwrap().work().clone());
        if let Err(failure) =
            runtime
                .host_executor()
                .submit(identity, command, self.permit.take().unwrap())
        {
            self.submission = Some(failure);
            return Poll::Fault;
        }
        Poll::Pending
    }
}
