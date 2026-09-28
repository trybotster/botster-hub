//! Durable session restart records, written by the requests that change them.
//!
//! A successful session-type spawn writes its restart record before its
//! `Spawned` reply; a removal deletes it. The durable record in HubState is the
//! only truth: when the write cannot be made durable (the Host is full, the
//! publication slot is held, the write is uncertain or fails), the request
//! still succeeds, the session is simply not restartable, and a Hub log line
//! says why. A reply therefore waits for the document when another write
//! holds it.

use botster_hub_client::DaemonResponse;

use crate::HubDaemon;
use crate::client_api::HubClientStep;
use crate::daemon::control::host_work::retain_submission;
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::control::state_record::{
    StateRecordAction, StateRecordTarget, StateRecordWrite,
};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::DaemonControlState;
use crate::host_executor::{HostCommand, HostJobIdentity, HostWorkPermit};
use crate::host_mutations::{HostCommit, HostMutationCommand};
use crate::owner_identity::WaiterId;
use crate::restart_records::RestartRecord;

/// Run a session-type spawn, then record how to restart it.
pub(crate) fn spawn_then_record(
    step: HubClientStep,
    waiter_id: WaiterId,
    session_id: String,
    record: RestartRecord,
    map: impl Fn(crate::HubClientResponseBody) -> DaemonTransportResult<DaemonResponse> + Send + 'static,
) -> ControlStep {
    let mut record = Some(record);
    let mut stage = match step {
        HubClientStep::Ready(result) => Stage::Spawned(Some(
            result
                .map_err(DaemonTransportError::Client)
                .and_then(|response| map(response.body)),
        )),
        HubClientStep::Pending(pending) => Stage::Spawning(Box::new(pending)),
    };
    ControlStep::pending(move |daemon, state| {
        loop {
            match &mut stage {
                Stage::Spawning(pending) => {
                    let Some(runtime) = daemon.runtime() else {
                        return ControlPoll::Ready(Err(DaemonTransportError::DaemonNotRunning));
                    };
                    let Some(result) = pending.poll(runtime) else {
                        return ControlPoll::Pending;
                    };
                    stage = Stage::Spawned(Some(
                        result
                            .map_err(DaemonTransportError::Client)
                            .and_then(|response| map(response.body)),
                    ));
                }
                Stage::Spawned(result) => {
                    let result = result.take().expect("a spawned stage is settled once");
                    let Ok(response) = result else {
                        return ControlPoll::Ready(result);
                    };
                    let target = StateRecordTarget::RestartRecord {
                        session_id: session_id.clone(),
                        record: record.take(),
                    };
                    match RecordWrite::start(daemon, state, waiter_id, &session_id, target) {
                        Some(write) => stage = Stage::Recording(Box::new(write), Some(response)),
                        None => return ControlPoll::Ready(Ok(response)),
                    }
                }
                Stage::Recording(write, response) => {
                    if !write.poll(daemon, state, waiter_id, &session_id) {
                        return ControlPoll::Pending;
                    }
                    let response = response.take().expect("a recording stage replies once");
                    return ControlPoll::Ready(Ok(response));
                }
            }
        }
    })
}

enum Stage {
    Spawning(Box<crate::client_api::HubClientPending>),
    Spawned(Option<DaemonTransportResult<DaemonResponse>>),
    Recording(Box<RecordWrite>, Option<DaemonResponse>),
}

/// One restart-record write, driven with this request's own Host permit.
pub(crate) struct RecordWrite {
    write: StateRecordWrite,
    permit: Option<HostWorkPermit>,
    next_host_phase: u64,
    /// True once the write has settled, durable or not.
    done: bool,
}

impl RecordWrite {
    /// Start the write. `None` means it cannot start, and the record is not
    /// made durable.
    pub(crate) fn start(
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        waiter_id: WaiterId,
        session_id: &str,
        target: StateRecordTarget,
    ) -> Option<Self> {
        let Some(permit) = daemon
            .runtime()
            .and_then(|runtime| runtime.host_executor().try_reserve())
        else {
            not_durable(
                session_id,
                "the bounded host executor has no available slot",
            );
            return None;
        };
        let (write, action) = match StateRecordWrite::begin(daemon, waiter_id, target) {
            Ok(begun) => begun,
            Err(reason) => {
                not_durable(session_id, &reason);
                return None;
            }
        };
        let mut record_write = Self {
            write,
            permit: Some(permit),
            next_host_phase: 1,
            done: false,
        };
        record_write.apply(daemon, state, waiter_id, session_id, action);
        (!record_write.done).then_some(record_write)
    }

    /// Advance the write. True once it has settled.
    pub(crate) fn poll(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        waiter_id: WaiterId,
        session_id: &str,
    ) -> bool {
        if self.done {
            return true;
        }
        let action = if self.write.is_parked() {
            self.write.admit(daemon, state, true)
        } else {
            let Some(completion) = state.host_completions.remove(&waiter_id) else {
                return false;
            };
            let (_, result, permit) = completion.into_parts();
            self.permit = Some(permit);
            self.write.on_completion(daemon, state, result, || None)
        };
        self.apply(daemon, state, waiter_id, session_id, action);
        self.done
    }

    fn apply(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        waiter_id: WaiterId,
        session_id: &str,
        action: StateRecordAction,
    ) {
        match action {
            StateRecordAction::Submit(command) => {
                self.submit(daemon, state, waiter_id, session_id, command);
            }
            StateRecordAction::Park => {}
            StateRecordAction::Committed => {
                // The durable set changed: re-derive this id's `restartable`.
                let present = daemon
                    .state_view()
                    .1
                    .restart_records
                    .contains_key(session_id);
                state
                    .maintenance
                    .restart_record_changed(session_id, present);
                self.done = true;
            }
            StateRecordAction::Failed {
                code,
                message,
                discard,
                ..
            } => {
                not_durable(session_id, &format!("{code}: {message}"));
                if let Some(prepared) = discard {
                    self.dispose(waiter_id, *prepared);
                }
                self.done = true;
            }
            StateRecordAction::Uncertain => {
                not_durable(
                    session_id,
                    "the write reached publication without a confirmed durable result",
                );
                self.done = true;
            }
            StateRecordAction::RevisionMismatch => {
                not_durable(
                    session_id,
                    "the commit revision is not the next Hub revision",
                );
                self.done = true;
            }
            StateRecordAction::Reconciliation(message) => {
                not_durable(session_id, &message);
                self.done = true;
            }
        }
    }

    fn submit(
        &mut self,
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        waiter_id: WaiterId,
        session_id: &str,
        command: HostCommand,
    ) {
        let permit = self
            .permit
            .take()
            .expect("a record write holds its permit between steps");
        let identity = HostJobIdentity {
            waiter_id,
            phase: self.next_host_phase,
        };
        let commit = self.write.awaits_commit();
        let submitted = match (daemon.runtime(), self.next_host_phase.checked_add(1)) {
            (Some(runtime), Some(next)) => runtime
                .host_executor()
                .submit(identity, command, permit)
                .map(|()| next),
            (runtime, _) => Err(crate::host_executor::HostSubmissionFailure {
                error: if runtime.is_none() {
                    crate::host_executor::HostSubmitError::Stopped
                } else {
                    crate::host_executor::HostSubmitError::PhaseExhausted
                },
                identity,
                command: Box::new(command),
                permit,
            }),
        };
        match submitted {
            Ok(next) => self.next_host_phase = next,
            Err(failure) => {
                if commit {
                    self.write.commit_not_submitted(state);
                }
                not_durable(
                    session_id,
                    "the record write could not be submitted to the Host",
                );
                // Retained as Host recovery, like any rejected state phase.
                let _ = retain_submission(state, failure);
                self.done = true;
            }
        }
    }

    /// Drop a prepared mutation that will never commit on a Host worker, not
    /// on the owner thread.
    fn dispose(&mut self, waiter_id: WaiterId, prepared: crate::host_mutations::PreparedMutation) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        let identity = HostJobIdentity {
            waiter_id,
            phase: self.next_host_phase,
        };
        let command = HostCommand::Mutation(HostMutationCommand::Commit(HostCommit { prepared }));
        if let Err(failure) = permit.dispose(identity, command) {
            // The executor refused the disposal; the command drops here.
            drop(failure);
        }
    }
}

/// The session stays unrestartable; say why in the Hub log.
fn not_durable(session_id: &str, reason: &str) {
    crate::hub_log::hub_log!("restart_record_not_durable session_id={session_id} reason={reason}");
}

/// A removal is done in Core. Retire the session's context and, when a
/// restart record exists, delete it before the reply.
pub(crate) fn finish_removed_session(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    session_id: &str,
    response: DaemonResponse,
    recording: &mut Option<(Box<RecordWrite>, DaemonResponse)>,
) -> ControlPoll {
    if let Some(runtime) = daemon.runtime() {
        runtime.retire_removed_session_context(session_id);
    }
    if !daemon
        .state_view()
        .1
        .restart_records
        .contains_key(session_id)
    {
        return ControlPoll::Ready(Ok(response));
    }
    let target = StateRecordTarget::RestartRecord {
        session_id: session_id.to_string(),
        record: None,
    };
    match RecordWrite::start(daemon, state, waiter_id, session_id, target) {
        Some(write) => {
            *recording = Some((Box::new(write), response));
            ControlPoll::Pending
        }
        None => ControlPoll::Ready(Ok(response)),
    }
}
