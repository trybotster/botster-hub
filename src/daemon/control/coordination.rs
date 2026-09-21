//! The owner routes each coordination request through its registered Core waiter.

use crate::daemon::control::message::ControlReplySender;
use crate::daemon::control::pending::{
    ControlContinuation, ControlPoll, OwnerRequestCompletion, PendingControlRequest, READY_INITIAL,
    mark_owner_ready,
};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::ReadyClass;
use crate::data_plane::driver::{ChargedCoreTicket, CoreTicketPoll};
use crate::host_executor::{
    HostCommand, HostCompletion, HostJobIdentity, HostResult, HostSubmissionFailure,
};
use crate::lua_runtime::{
    CoordinationDelivery, CoordinationRefusal, CoordinationReply, CoordinationReplySender,
};
use crate::owner_identity::WaiterId;
use crate::{HubDaemon, HubRuntime};

type Response = CoordinationReply;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoordinationFault {
    QueuePoisoned,
    SchedulerExhausted,
    DeliverySubmission,
    UnexpectedCompletion,
}

pub(crate) struct CoordinationContinuation {
    waiter_id: WaiterId,
    ticket: Option<ChargedCoreTicket<Response>>,
    response: Option<CoordinationReplySender>,
    completed: Option<CoordinationDelivery>,
    rejected: Option<crate::data_plane::driver::CoreRejectedRequest>,
    delivery_failure: Option<HostSubmissionFailure>,
    #[cfg(test)]
    terminal_drop_probe: Option<Box<dyn Send>>,
    disposal: Option<crate::lua_memory::LuaCallbackCharge>,
    entry: Option<crate::lua_memory::LuaCallbackCharge>,
}

#[allow(dead_code)] // payload owners drop here; the lease outlives this box
struct CoordinationTerminalPayload {
    response: Option<CoordinationReplySender>,
    completed: Option<CoordinationDelivery>,
    rejected: Option<crate::data_plane::driver::CoreRejectedRequest>,
    completion: Option<HostResult>,
    command: Option<HostCommand>,
    #[cfg(test)]
    terminal_drop_probe: Option<Box<dyn Send>>,
    entry: Option<crate::lua_memory::LuaCallbackCharge>,
}

pub(crate) const fn continuation_bytes() -> usize {
    std::mem::size_of::<CoordinationContinuation>()
}

pub(crate) fn disposal_bytes() -> Option<usize> {
    crate::daemon::control::pending::terminal_storage_bytes(std::mem::size_of::<
        CoordinationTerminalPayload,
    >())
}

pub(crate) fn accept_one(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    if state.coordination_fault.is_some() {
        return;
    }
    let Some(permit) = state.budget.reserve() else {
        state.coordination_waiting_for_owner = true;
        return;
    };
    let Some(waiter_id) = state.waiter_ids.next() else {
        state.coordination_fault = Some(CoordinationFault::SchedulerExhausted);
        state.budget.release(permit);
        return;
    };
    let pending = match runtime.coordination_bridge().take_pending_for_owner() {
        crate::lua_runtime::CoordinationIngressPoll::Ready(pending) => pending,
        poll => {
            if matches!(poll, crate::lua_runtime::CoordinationIngressPoll::Poisoned) {
                state.coordination_fault = Some(CoordinationFault::QueuePoisoned);
            }
            state.budget.release(permit);
            return;
        }
    };
    let (core_storage, continuation_charge, disposal) = match pending.storage {
        Some(storage) => (
            Some(storage.core),
            Some(storage.continuation),
            Some(storage.disposal),
        ),
        None => (None, None, None),
    };
    let retirement = runtime.coordination_retirement(waiter_id);
    let submission = runtime.submit_coordination_for_owner(
        &retirement,
        pending.operation,
        pending.caller,
        core_storage,
    );
    #[cfg(test)]
    if submission.rejected.is_none() {
        runtime
            .coordination_bridge()
            .test_note_core_admission(waiter_id);
    }
    state.pending_requests.insert(
        waiter_id,
        PendingControlRequest {
            waiter_id,
            ready_class: ReadyClass::CoreCompletion,
            ready_key: None,
            deadline_key: None,
            last_core_phase: 0,
            last_host_phase: 0,
            completion: OwnerRequestCompletion::default(),
            reply_tx: ControlReplySender::absent(),
            response_delivery_rx: None,
            grant_id: None,
            client: None,
            core_retirement: Some(retirement),
            permit: Some(permit),
            must_finish: true,
            past_deadline: false,
            continuation: ControlContinuation::Coordination(
                Box::new(CoordinationContinuation {
                    waiter_id,
                    ticket: Some(submission.ticket),
                    response: Some(pending.response),
                    completed: None,
                    rejected: submission.rejected,
                    delivery_failure: None,
                    #[cfg(test)]
                    terminal_drop_probe: pending.terminal_drop_probe,
                    disposal,
                    entry: pending.entry,
                }),
                continuation_charge,
            ),
            retire: None,
        },
    );
    if !mark_owner_ready(state, waiter_id, ReadyClass::CoreCompletion, READY_INITIAL) {
        state.coordination_fault = Some(CoordinationFault::SchedulerExhausted);
    }
}

impl CoordinationContinuation {
    #[cfg(test)]
    pub(super) fn empty_for_phase_test(waiter_id: WaiterId) -> Self {
        Self {
            waiter_id,
            ticket: None,
            response: None,
            completed: None,
            rejected: None,
            delivery_failure: None,
            terminal_drop_probe: None,
            disposal: None,
            entry: None,
        }
    }

    fn collect(&mut self) -> bool {
        if self.completed.is_some() || self.response.is_none() {
            return true;
        }
        if let Some(rejected) = self.rejected.as_ref() {
            use crate::data_plane::driver::CoreRefusal;
            let message = match rejected.reason {
                CoreRefusal::Registration => CoordinationRefusal::Registration,
                CoreRefusal::Abandoned => CoordinationRefusal::Abandoned,
                CoreRefusal::Full => CoordinationRefusal::Full,
                CoreRefusal::Stopped => CoordinationRefusal::Stopped,
            };
            self.completed = Some(CoordinationDelivery::Refused(message));
            self.ticket.take();
            return true;
        }
        let Some(ticket) = self.ticket.as_mut() else {
            return true;
        };
        let result = match ticket.poll() {
            CoreTicketPoll::Pending => return false,
            CoreTicketPoll::Ready(result) => CoordinationDelivery::Reply(result),
            CoreTicketPoll::Refused => CoordinationDelivery::Refused(CoordinationRefusal::Full),
            CoreTicketPoll::Lost => CoordinationDelivery::Refused(CoordinationRefusal::Lost),
        };
        self.completed = Some(result);
        self.ticket.take();
        true
    }

    pub(crate) fn poll(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        if self.delivery_failure.is_some() {
            return ControlPoll::Pending;
        }
        if let Some(completion) = state.host_completions.remove(&self.waiter_id) {
            if !matches!(
                &completion.result,
                HostResult::CoordinationResponseDelivered | HostResult::Failed { .. }
            ) {
                state.host_completions.insert(self.waiter_id, completion);
                state.coordination_fault = Some(CoordinationFault::UnexpectedCompletion);
                return ControlPoll::Pending;
            }
            drop(completion);
            return ControlPoll::FinishedInternal;
        }
        if self.response.is_none() || !self.collect() {
            return ControlPoll::Pending;
        }
        let runtime = daemon
            .runtime()
            .expect("an admitted coordination row retains its runtime");
        let Some(host_permit) = runtime.host_executor().try_reserve() else {
            state.coordination_capacity_waiters.insert(self.waiter_id);
            return ControlPoll::Pending;
        };
        state.coordination_capacity_waiters.remove(&self.waiter_id);
        let response = self
            .response
            .take()
            .expect("coordination retains its response sender");
        let result = self
            .completed
            .take()
            .expect("coordination collected its Core result");
        if let Err(failure) = runtime.host_executor().submit(
            HostJobIdentity {
                waiter_id: self.waiter_id,
                phase: 1,
            },
            HostCommand::DeliverCoordinationResponse {
                response,
                result,
                discard: self.rejected.take(),
            },
            host_permit,
        ) {
            self.delivery_failure = Some(failure);
            state.coordination_fault = Some(CoordinationFault::DeliverySubmission);
        }
        ControlPoll::Pending
    }

    pub(crate) fn take_terminal_parts(
        &mut self,
        runtime: &HubRuntime,
        identity: HostJobIdentity,
        completion: &mut Option<HostCompletion>,
    ) -> Option<crate::host_disposal::Parts> {
        runtime.take_terminal_coordination_completion(self.waiter_id);
        if !self.collect() {
            return None;
        }
        let mut retained_completion = None;
        let mut retained_command = None;
        let (identity, permit) = if let Some(failure) = self.delivery_failure.take() {
            retained_command = Some(failure.command);
            (failure.identity, failure.permit)
        } else if self.response.is_some() {
            (identity, runtime.host_executor().try_reserve()?)
        } else {
            let (identity, result, permit) = completion.take()?.into_parts();
            retained_completion = Some(result);
            (identity, permit)
        };
        let storage = self
            .disposal
            .take()
            .map(crate::lua_memory::LuaCallbackStorageLease::new);
        Some(crate::host_disposal::Parts {
            storage,
            identity,
            permit,
            model: None,
            payload: Box::new(CoordinationTerminalPayload {
                response: self.response.take(),
                completed: self.completed.take(),
                rejected: self.rejected.take(),
                completion: retained_completion,
                command: retained_command,
                #[cfg(test)]
                terminal_drop_probe: self.terminal_drop_probe.take(),
                entry: self.entry.take(),
            }),
        })
    }
}
