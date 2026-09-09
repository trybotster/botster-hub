//! Event cleanup belongs to the original connection and its retained subscription slots.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::HubDaemon;
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::{ReadyClass, ReadyItem};
use crate::host_executor::{
    HostCommand, HostCompletion, HostJobIdentity, HostResult, HostSubmissionFailure,
    HostSubmitError,
};
use crate::owner_identity::WaiterId;
use crate::subscription::package_events::{
    ClientCleanupWork, ClientEventAdmitError, ClientEventConnection, ClientEventMailbox,
};

enum Recovery {
    Completion(HostCompletion),
    Submission(HostSubmissionFailure),
    ReadyExhausted,
    UnexpectedCompletion {
        _previous: Option<Box<Recovery>>,
        _expected: Option<HostCompletion>,
        _received: HostCompletion,
    },
}

struct Connection {
    record: Arc<ClientEventConnection>,
    identity: HostJobIdentity,
    submitted: bool,
    completion: Option<HostCompletion>,
    recovery: Option<Recovery>,
    closed: bool,
    disconnect_waiter: Option<WaiterId>,
}

/// These rows use the connection's lifetime admission. They do not reserve child Owner permits.
#[derive(Default)]
pub(crate) struct ClientEvents {
    by_connection: HashMap<Arc<str>, WaiterId>,
    connections: BTreeMap<WaiterId, Connection>,
    capacity_waiters: BTreeSet<WaiterId>,
}

impl ClientEvents {
    pub(crate) fn owns_waiter(&self, waiter: WaiterId) -> bool {
        self.connections.contains_key(&waiter)
    }

    pub(crate) fn retain_completion(&mut self, completion: HostCompletion) {
        let waiter = completion.identity.waiter_id;
        let connection = self
            .connections
            .get_mut(&waiter)
            .expect("the client owns this waiter");
        if connection.identity != completion.identity
            || !connection.submitted
            || connection.recovery.is_some()
            || connection.completion.is_some()
            || connection.closed
        {
            connection.record.retain_fault();
            connection.recovery = Some(Recovery::UnexpectedCompletion {
                _previous: connection.recovery.take().map(Box::new),
                _expected: connection.completion.take(),
                _received: completion,
            });
        } else {
            connection.completion = Some(completion);
        }
    }

    pub(crate) fn has_capacity_waiters(&self) -> bool {
        !self.capacity_waiters.is_empty()
    }

    pub(crate) fn pop_capacity_waiter(&mut self) -> Option<WaiterId> {
        self.capacity_waiters.pop_first()
    }

    #[cfg(test)]
    pub(crate) fn test_recovery(&self, connection_id: &str) -> bool {
        self.by_connection
            .get(connection_id)
            .is_some_and(|waiter| self.connections[waiter].recovery.is_some())
    }

    #[cfg(test)]
    pub(crate) fn test_set_phase(&mut self, connection_id: &str, phase: u64) {
        let waiter = self.by_connection[connection_id];
        self.connections.get_mut(&waiter).unwrap().identity.phase = phase;
    }
}

pub(crate) fn admit_connection(
    state: &mut DaemonControlState,
    connection_id: &str,
) -> Result<(), ClientEventAdmitError> {
    let reader = state
        .pending_runtime
        .admission
        .host_compatibility
        .get(connection_id)
        .and_then(|record| record.event_reader.clone());
    if let Some(waiter) = state.client_events.by_connection.get(connection_id) {
        if let Some(reader) = reader {
            state.client_events.connections[waiter]
                .record
                .bind_reader(&reader);
        }
        return Ok(());
    }
    // Refuse before connection insertion or any subscription effect if identity allocation stops.
    let waiter_id = state
        .waiter_ids
        .next()
        .ok_or(ClientEventAdmitError::Router(
            crate::package_event_router::EventPlaneStatus::RejectedInvalid,
        ))?;
    let record = state.event_plane.admit_connection(connection_id)?;
    if let Some(reader) = reader {
        record.bind_reader(&reader);
    }
    state
        .client_events
        .by_connection
        .insert(Arc::clone(&record.identity), waiter_id);
    state.client_events.connections.insert(
        waiter_id,
        Connection {
            record,
            identity: HostJobIdentity {
                waiter_id,
                phase: 0,
            },
            submitted: false,
            completion: None,
            recovery: None,
            closed: false,
            disconnect_waiter: None,
        },
    );
    Ok(())
}

pub(crate) fn mark_ready(state: &mut DaemonControlState, waiter: WaiterId) {
    let Some(connection) = state.client_events.connections.get(&waiter) else {
        return;
    };
    if connection.closed
        || connection.recovery.is_some()
        || (connection.submitted && connection.completion.is_none())
    {
        return;
    }
    if state
        .owner_ready
        .mark(
            waiter,
            ReadyClass::Cleanup,
            crate::daemon::control::pending::READY_INITIAL,
        )
        .is_err()
    {
        let connection = state.client_events.connections.get_mut(&waiter).unwrap();
        connection.record.retain_fault();
        connection.recovery = Some(Recovery::ReadyExhausted);
    }
}

pub(crate) fn note_cleanup(state: &mut DaemonControlState, connection_id: &str) {
    let Some(&waiter) = state.client_events.by_connection.get(connection_id) else {
        return;
    };
    if state.client_events.connections[&waiter]
        .record
        .cleanup_requested()
        && !state.client_events.capacity_waiters.contains(&waiter)
    {
        mark_ready(state, waiter);
    }
}

/// The mailbox carries the exact admitted incarnation, including failed reservation cleanup.
pub(crate) fn retire_mailbox(state: &mut DaemonControlState, mailbox: &Arc<ClientEventMailbox>) {
    let Some(record) = mailbox.connection() else {
        return;
    };
    if record.fully_reclaimed() {
        return;
    }
    mailbox.retire();
    record.request_cleanup();
    let Some(&waiter) = state
        .client_events
        .by_connection
        .get(record.identity.as_ref())
    else {
        return;
    };
    if !Arc::ptr_eq(&state.client_events.connections[&waiter].record, &record) {
        return;
    }
    if !state.client_events.capacity_waiters.contains(&waiter) {
        mark_ready(state, waiter);
    }
}

pub(crate) fn close_connection(state: &mut DaemonControlState, connection_id: &str) {
    let Some(&waiter) = state.client_events.by_connection.get(connection_id) else {
        return;
    };
    state.client_events.connections[&waiter].record.close();
    if !state.client_events.capacity_waiters.contains(&waiter) {
        mark_ready(state, waiter);
    }
}

/// The disconnect obligation keeps the original permit until both cleanup families finish.
pub(crate) fn finish_connection(
    state: &mut DaemonControlState,
    connection_id: &str,
    disconnect_waiter: WaiterId,
) -> bool {
    let Some(&waiter) = state.client_events.by_connection.get(connection_id) else {
        return true;
    };
    let connection = state.client_events.connections.get_mut(&waiter).unwrap();
    if !connection.closed {
        connection.disconnect_waiter = Some(disconnect_waiter);
        return false;
    }
    state.client_events.connections.remove(&waiter);
    state.client_events.by_connection.remove(connection_id);
    true
}

pub(crate) fn drive_ready(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    item: ReadyItem,
) -> bool {
    let waiter = item.key().waiter_id();
    let Some(mut connection) = state.client_events.connections.remove(&waiter) else {
        return false;
    };
    let Some(runtime) = daemon.runtime() else {
        state.client_events.connections.insert(waiter, connection);
        return true;
    };
    if let Some(completion) = connection.completion.take() {
        match &completion.result {
            HostResult::ClientEventCleanup(Ok(done))
                if Arc::ptr_eq(&done.connection, &connection.record) =>
            {
                connection.closed = done.closed;
                connection.submitted = false;
                drop(completion);
                if connection.closed {
                    if let Some(disconnect_waiter) = connection.disconnect_waiter.take() {
                        crate::daemon::owner_budget::mark_obligation_ready(
                            state,
                            disconnect_waiter,
                            crate::daemon::control::pending::READY_HOST_COMPLETION,
                        );
                    }
                }
            }
            _ => {
                // The original slots and the Host permit remain owned after a terminal fault.
                connection.record.retain_fault();
                connection.recovery = Some(Recovery::Completion(completion));
            }
        }
    } else if connection.recovery.is_none()
        && !connection.submitted
        && connection.record.cleanup_requested()
    {
        if let Some(permit) = runtime.host_executor().try_reserve() {
            let command = HostCommand::ClientEventCleanup {
                router: Arc::clone(runtime.package_event_router()),
                work: ClientCleanupWork::new(
                    state.event_plane.as_ref().clone(),
                    Arc::clone(&connection.record),
                ),
            };
            if let Some(phase) = connection.identity.phase.checked_add(1) {
                connection.identity.phase = phase;
                match runtime
                    .host_executor()
                    .submit(connection.identity, command, permit)
                {
                    Ok(()) => connection.submitted = true,
                    Err(failure) => {
                        connection.record.retain_fault();
                        connection.recovery = Some(Recovery::Submission(failure));
                    }
                }
            } else {
                connection.record.retain_fault();
                connection.recovery = Some(Recovery::Submission(HostSubmissionFailure {
                    error: HostSubmitError::PhaseExhausted,
                    identity: connection.identity,
                    command,
                    permit,
                }));
            }
        } else {
            state.client_events.capacity_waiters.insert(waiter);
        }
    }
    let ready = connection.recovery.is_none()
        && !connection.submitted
        && connection.record.cleanup_requested()
        && !state.client_events.capacity_waiters.contains(&waiter);
    state.client_events.connections.insert(waiter, connection);
    if ready {
        mark_ready(state, waiter);
    }
    true
}
