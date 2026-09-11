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
    terminal: Option<crate::host_disposal::Job>,
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
    pub(crate) fn dispose_terminal(
        &mut self,
        runtime: &crate::HubRuntime,
        plane: &crate::subscription::package_events::ClientEventPlane,
    ) -> bool {
        self.connections.retain(|_, connection| {
            if let Some(job) = connection.terminal.as_mut() {
                if connection.completion.is_some() || connection.recovery.is_some() {
                    return true;
                }
                if let crate::host_disposal::Poll::Disposed(permit) = job.poll() {
                    assert!(connection.record.fully_reclaimed());
                    self.by_connection
                        .remove(connection.record.identity.as_ref());
                    drop(permit);
                    return false;
                }
                return true;
            }
            let mut payload: Option<Box<dyn Send>> = None;
            let (identity, permit) = if let Some(completion) = connection.completion.take() {
                let (identity, result, permit) = completion.into_parts();
                payload = Some(Box::new(result));
                (identity, permit)
            } else if let Some(recovery) = connection.recovery.take() {
                match recovery {
                    Recovery::Completion(completion) => {
                        let (identity, result, permit) = completion.into_parts();
                        payload = Some(Box::new(result));
                        (identity, permit)
                    }
                    Recovery::Submission(failure) => {
                        payload = Some(Box::new(failure.command));
                        (failure.identity, failure.permit)
                    }
                    recovery @ Recovery::UnexpectedCompletion { .. } => {
                        connection.recovery = Some(recovery);
                        return true;
                    }
                    Recovery::ReadyExhausted => {
                        let Some(permit) = runtime.host_executor().try_reserve() else {
                            connection.recovery = Some(Recovery::ReadyExhausted);
                            return true;
                        };
                        (connection.identity, permit)
                    }
                }
            } else if connection.submitted {
                return true;
            } else {
                let Some(permit) = runtime.host_executor().try_reserve() else {
                    return true;
                };
                (connection.identity, permit)
            };
            connection.record.close();
            connection.terminal = Some(crate::host_disposal::Job::new_client(
                crate::host_disposal::Parts {
                    storage: None,
                    identity,
                    permit,
                    payload: Box::new(payload),
                    model: None,
                },
                Arc::clone(runtime.package_event_router()),
                ClientCleanupWork::new(plane.clone(), Arc::clone(&connection.record)),
            ));
            true
        });
        self.connections.is_empty()
    }

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
            || connection.terminal.is_some()
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
            terminal: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_executor::{
        HOST_OPERATION_CAPACITY, HOST_PREPARED_BYTE_CAPACITY, TestDisposalProbe,
    };
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    #[test]
    fn terminal_client_duplicate_prevents_row_retirement_after_shared_cleanup() {
        let root = std::path::PathBuf::from("/private/tmp")
            .join(format!("hub-terminal-client-{}", std::process::id()));
        let config = crate::HubStartupOptions {
            data_directory: crate::DataDirectoryOption::Explicit(root.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let mut daemon = HubDaemon::start(config).unwrap();
        let runtime = daemon.runtime().unwrap();
        let mut state = DaemonControlState::default();
        admit_connection(&mut state, "terminal-client").unwrap();
        let waiter = state.client_events.by_connection["terminal-client"];
        let connection = state.client_events.connections.get_mut(&waiter).unwrap();
        connection.identity.phase = 1;
        connection.submitted = true;
        let record = Arc::clone(&connection.record);
        let identity = connection.identity;
        let executor = runtime.host_executor();
        assert_eq!(executor.outstanding(), 0);
        let original_permit = executor.try_reserve().unwrap();
        let additional_permit = executor.try_reserve().unwrap();
        let other_permits: Vec<_> = (2..HOST_OPERATION_CAPACITY)
            .map(|_| executor.try_reserve().unwrap())
            .collect();
        assert!(executor.try_reserve().is_none());
        let (original_dropped, original_drop_rx) = mpsc::channel();
        let (additional_dropped, additional_drop_rx) = mpsc::channel();
        let receipt = |bytes: &[u8], dropped| {
            HostResult::StatusResponsePrepared(crate::status_response::PreparedStatusResponse {
                kind: botster_hub_client::DaemonResponseKind::Status,
                encoded_frame: Some(bytes.to_vec()),
                shutdown: false,
                dispose_probe: Some(TestDisposalProbe {
                    dropped,
                    executed: Arc::new(AtomicBool::new(false)),
                }),
            })
        };
        state
            .client_events
            .retain_completion(HostCompletion::for_test(
                identity,
                receipt(b"original receipt", original_dropped),
                original_permit,
            ));
        assert!(
            state.client_events.connections[&waiter]
                .completion
                .is_some()
        );
        assert!(
            !state
                .client_events
                .dispose_terminal(runtime, &state.event_plane)
        );
        let connection = &state.client_events.connections[&waiter];
        assert!(connection.submitted && !connection.closed);
        assert!(connection.completion.is_none() && connection.recovery.is_none());
        assert!(connection.terminal.is_some());
        let additional = receipt(b"additional receipt", additional_dropped);
        let HostResult::StatusResponsePrepared(prepared) = &additional else {
            unreachable!();
        };
        let additional_allocation = prepared.encoded_frame.as_ref().unwrap().as_ptr();
        // The terminal job is the only rejection condition for this matching receipt.
        state
            .client_events
            .retain_completion(HostCompletion::for_test(
                identity,
                additional,
                additional_permit,
            ));
        assert!(
            original_drop_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .starts_with("botster-hub-host-")
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !state.client_events.connections[&waiter]
            .terminal
            .as_ref()
            .unwrap()
            .test_disposed()
        {
            assert!(
                Instant::now() < deadline,
                "shared cleanup must finish on Host"
            );
            std::thread::yield_now();
        }
        assert!(record.fully_reclaimed());
        assert!(
            state
                .event_plane
                .test_residency("terminal-client")
                .is_none()
        );
        for _ in 0..3 {
            assert!(
                !state
                    .client_events
                    .dispose_terminal(runtime, &state.event_plane)
            );
            assert_eq!(state.client_events.by_connection["terminal-client"], waiter);
            assert_eq!(state.client_events.connections.len(), 1);
            let connection = &state.client_events.connections[&waiter];
            assert!(Arc::ptr_eq(&connection.record, &record));
            assert_eq!(connection.identity, identity);
            assert!(connection.completion.is_none());
            assert!(connection.terminal.as_ref().unwrap().test_disposed());
            let Some(Recovery::UnexpectedCompletion {
                _previous: None,
                _expected: None,
                _received: additional,
            }) = connection.recovery.as_ref()
            else {
                panic!("the exact additional receipt must remain in terminal recovery");
            };
            assert_eq!(additional.identity, identity);
            let HostResult::StatusResponsePrepared(prepared) = &additional.result else {
                panic!("the additional receipt must retain its payload");
            };
            let bytes = prepared.encoded_frame.as_ref().unwrap();
            assert_eq!(bytes.as_slice(), b"additional receipt");
            assert_eq!(bytes.as_ptr(), additional_allocation);
            assert!(matches!(
                additional_drop_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            assert_eq!(executor.outstanding(), HOST_OPERATION_CAPACITY);
            assert_eq!(
                executor.prepared_bytes(),
                HOST_OPERATION_CAPACITY * HOST_PREPARED_BYTE_CAPACITY
            );
        }
        drop(other_permits);
        assert_eq!(executor.outstanding(), 2);
        assert_eq!(executor.prepared_bytes(), 2 * HOST_PREPARED_BYTE_CAPACITY);

        // Test cleanup transfers the extra payload through its original Host permit.
        let Some(Recovery::UnexpectedCompletion {
            _previous: None,
            _expected: None,
            _received: additional,
        }) = state
            .client_events
            .connections
            .get_mut(&waiter)
            .unwrap()
            .recovery
            .take()
        else {
            unreachable!();
        };
        let (returned_identity, result, permit) = additional.into_parts();
        assert_eq!(returned_identity, identity);
        assert_eq!(
            permit.reserved_prepared_bytes(),
            HOST_PREPARED_BYTE_CAPACITY
        );
        permit
            .dispose(
                returned_identity,
                HostCommand::DiscardCompletion(Box::new(result)),
            )
            .unwrap();
        assert!(
            additional_drop_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .starts_with("botster-hub-host-")
        );
        assert!(
            state
                .client_events
                .dispose_terminal(runtime, &state.event_plane)
        );
        assert!(state.client_events.by_connection.is_empty());
        let deadline = Instant::now() + Duration::from_secs(5);
        while executor.outstanding() != 0 || executor.prepared_bytes() != 0 {
            assert!(
                Instant::now() < deadline,
                "test cleanup must release both permits"
            );
            std::thread::yield_now();
        }
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }
}
