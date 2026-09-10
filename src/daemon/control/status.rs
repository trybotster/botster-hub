//! Status and Shutdown preparation through the existing Host executor.
//!
//! Hub copies and Core inventory share the admitted Host allowance.
//! Terminal teardown must transfer retained commands before dropping the control state.

use botster_hub_client::{DaemonDiagnostic, DaemonLifecycleCounters, DaemonRetentionAccounting};
use std::mem::size_of;
use std::path::PathBuf;

use crate::HubDaemon;
use crate::daemon::control::DaemonObservability;
use crate::daemon::control::pending::{ControlPoll, ControlStep, PendingControlRequest};
use crate::daemon::error::DaemonTransportError;
use crate::daemon::owner_loop::DaemonControlState;
use crate::data_plane::driver::CoreTicketPoll;
use crate::host_executor::{
    HostCommand, HostJobIdentity, HostResult, HostSubmissionFailure, HostWorkPermit,
};
use crate::status_response::{
    PreparedStatusResponse, StatusCoreSnapshot, StatusResponseInput, StatusResponseSeed,
    checked_live_bytes, lifecycle_bytes,
};

#[cfg(test)]
thread_local! {
    static TEST_STATUS_CORE_RESULT_GATE: std::cell::RefCell<Option<(
        std::sync::mpsc::Sender<bool>, std::sync::mpsc::Receiver<()>,
    )>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    shutdown: bool,
) -> ControlStep {
    let Some(runtime) = daemon.runtime() else {
        return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
    };
    let Some(permit) = runtime.host_executor().try_reserve() else {
        return ControlStep::ready(super::attach_bind_operator_error(
            "host_executor_full",
            if shutdown {
                "the host executor has no shutdown slot; retry after capacity is released"
            } else {
                "the host executor has no response slot; retry after capacity is released"
            },
        ));
    };
    let waiter_id = state.current_waiter_id.expect("owner waiter is assigned");
    let policy = runtime.retention_policy();
    let mut input = capture_seed(
        daemon,
        state,
        observability.transport_request_id.unwrap_or_default(),
        shutdown,
        permit.reserved_prepared_bytes(),
    );
    let core_limit = (!input.rejected)
        .then(|| core_inventory_allowance(&input, permit.reserved_prepared_bytes()))
        .flatten();
    if core_limit.is_none() {
        input.rejected = true;
    }
    let ticket = core_limit.map(|max_logical_bytes| {
        let reservation = permit.retain_prepared_reservation();
        #[cfg(test)]
        let result_gate = TEST_STATUS_CORE_RESULT_GATE.with(|slot| slot.borrow_mut().take());
        runtime.submit_core_for_owner(waiter_id, move |core| {
            // Core preflights all inventory storage before allocation or cloning.
            // The closure and its returned value retain the original reservation.
            let snapshot = StatusCoreSnapshot::new(
                core.retention_accounting(),
                if shutdown {
                    Ok(None)
                } else {
                    core.list_terminal_subscriptions(max_logical_bytes)
                        .map(Some)
                },
                reservation,
            );
            #[cfg(test)]
            if let Some((entered, release)) = result_gate {
                let _ = entered.send(snapshot.inventory.is_err());
                let _ = release.recv_timeout(std::time::Duration::from_secs(5));
            }
            snapshot
        })
    });
    if shutdown {
        state.shutdown_waiter = Some(waiter_id);
    }
    ControlStep::Pending(super::pending::PendingStep {
        continuation: super::pending::ControlContinuation::Status(Box::new(StatusContinuation {
            waiter_id,
            shutdown,
            policy,
            ticket,
            input: Some(input),
            permit: Some(permit),
            prepared: None,
            phase: 0,
            entity_cancel_after: None,
            delivery: None,
        })),
        retire: None,
        ready_class: crate::daemon::owner_schedule::ReadyClass::CoreCompletion,
    })
}

pub(crate) struct StatusContinuation {
    waiter_id: crate::owner_identity::WaiterId,
    shutdown: bool,
    policy: botster_core_daemon::RetentionPolicy,
    ticket: Option<crate::data_plane::driver::CoreTicket<StatusCoreSnapshot>>,
    input: Option<StatusResponseInput>,
    permit: Option<HostWorkPermit>,
    prepared: Option<PreparedStatusResponse>,
    phase: u64,
    entity_cancel_after: Option<crate::owner_identity::WaiterId>,
    delivery: Option<Delivery>,
}

#[derive(Clone, Copy)]
enum Delivery {
    Waiting(HostJobIdentity),
    Refused,
}

impl StatusContinuation {
    pub(crate) fn take_terminal_parts(
        &mut self,
        identity: HostJobIdentity,
        completion: &mut Option<crate::host_executor::HostCompletion>,
    ) -> Option<crate::host_disposal::Parts> {
        let mut result = None;
        let (identity, permit) = if let Some(permit) = self.permit.take() {
            (identity, permit)
        } else {
            let (identity, value, permit) = completion.take()?.into_parts();
            result = Some(value);
            (identity, permit)
        };
        Some(crate::host_disposal::Parts {
            identity,
            permit,
            model: None,
            payload: Box::new((
                self.input.take(),
                self.prepared.take(),
                self.ticket.take(),
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
        let Self {
            waiter_id,
            shutdown,
            policy,
            ticket,
            input,
            permit,
            prepared,
            phase,
            entity_cancel_after,
            delivery,
        } = self;
        let waiter_id = *waiter_id;
        let shutdown = *shutdown;
        if let Some(delivery) = delivery {
            return match *delivery {
                Delivery::Waiting(identity) => poll_delivery(state, identity),
                Delivery::Refused => ControlPoll::StatusResponseRefused { shutdown },
            };
        }
        if *phase == 0 {
            let retained = input.as_mut().expect("Status retains its admitted input");
            if !retained.rejected {
                match ticket
                    .as_mut()
                    .expect("Status retains its Core ticket")
                    .poll()
                {
                    CoreTicketPoll::Pending => return ControlPoll::Pending,
                    CoreTicketPoll::Ready(core) => {
                        let accounting = &core.accounting;
                        retained
                            .seed
                            .as_mut()
                            .expect("Status retained its seed")
                            .retention = Some(DaemonRetentionAccounting {
                            max_object_bytes: policy.max_object_bytes as u64,
                            max_total_bytes: policy.max_total_bytes as u64,
                            max_sessions: u32::try_from(policy.max_sessions).unwrap_or(u32::MAX),
                            total_bytes: accounting.total_bytes as u64,
                            sessions: u32::try_from(accounting.sessions).unwrap_or(u32::MAX),
                            evictions: accounting.evictions,
                        });
                        retained.rejected = core.inventory.is_err();
                        retained.core = Some(core);
                    }
                    CoreTicketPoll::Lost | CoreTicketPoll::Refused if shutdown => {}
                    failure @ (CoreTicketPoll::Lost | CoreTicketPoll::Refused) => {
                        let response = match failure {
                            CoreTicketPoll::Lost => {
                                super::sessions::lost_core("status", "daemon-status")
                            }
                            CoreTicketPoll::Refused => {
                                super::sessions::overloaded_core("status", "daemon-status")
                            }
                            _ => unreachable!("matched Core failure"),
                        };
                        // Host destroys the retained input before its original reservation ends.
                        let permit = permit.take().expect("Status retains its slot");
                        if let Err(failure) = permit.dispose(
                            HostJobIdentity {
                                waiter_id,
                                phase: 1,
                            },
                            HostCommand::PrepareStatusResponse(
                                input.take().expect("Status retains its input"),
                            ),
                        ) {
                            return super::host_work::retain_submission(state, failure);
                        }
                        return ControlPoll::Ready(Ok(response));
                    }
                }
                let limit = permit
                    .as_ref()
                    .expect("Status retains its slot")
                    .reserved_prepared_bytes();
                if capture_current_sources(daemon, state, retained, limit).is_none() {
                    retained.rejected = true;
                }
            }
            *phase = 1;
            return submit(
                daemon,
                state,
                HostJobIdentity {
                    waiter_id,
                    phase: *phase,
                },
                HostCommand::PrepareStatusResponse(input.take().expect("Status retains its input")),
                permit.take().expect("Status retains its slot"),
            );
        }
        if let Some(completion) = state.host_completions.remove(&waiter_id) {
            if completion.identity
                != (HostJobIdentity {
                    waiter_id,
                    phase: *phase,
                })
            {
                return retain_completion(state, completion);
            }
            let (_, result, returned_permit) = completion.into_parts();
            match result {
                HostResult::StatusResponsePrepared(response) => {
                    *prepared = Some(response);
                    *permit = Some(returned_permit);
                }
                HostResult::Failed { error, .. } => {
                    drop(returned_permit);
                    if shutdown {
                        state.shutdown_waiter = None;
                    }
                    return ControlPoll::Ready(Ok(super::attach_bind_operator_error(
                        "status_preparation_failed",
                        &error.message,
                    )));
                }
                result => {
                    return retain_completion(
                        state,
                        crate::host_executor::HostCompletion::from_parts(
                            HostJobIdentity {
                                waiter_id,
                                phase: *phase,
                            },
                            result,
                            returned_permit,
                        ),
                    );
                }
            }
        }
        if prepared.is_none() {
            return ControlPoll::Pending;
        }
        if shutdown && *phase == 1 {
            if super::entities::cancel_next_plugin_entity_for_shutdown(state, entity_cancel_after) {
                return ControlPoll::Again;
            }
            let Some(runtime) = daemon.runtime() else {
                return ControlPoll::Pending;
            };
            if super::entities::plugin_entity_cleanup_pending(state)
                || runtime.entity_publish_retirement_pending()
                || runtime.entity_publish_bridge().pending_publish_count() > 0
                || runtime.event_plane_owner_ops_pending()
                || !state.pending_requests.is_empty()
            {
                return ControlPoll::Pending;
            }
            *phase = 2;
            return submit(
                daemon,
                state,
                HostJobIdentity {
                    waiter_id,
                    phase: *phase,
                },
                HostCommand::StopForStatus(prepared.take().expect("snapshot was encoded")),
                permit.take().expect("shutdown retains its original slot"),
            );
        }
        ControlPoll::DeliverStatusResponse(
            prepared.take().expect("snapshot was encoded"),
            permit.take().expect("response retains its original slot"),
            *phase + 1,
        )
    }
}

fn seed_preflight(
    daemon: &HubDaemon,
    state: &DaemonControlState,
    request_id: &str,
    shutdown: bool,
    limit: usize,
) -> Option<(usize, usize, usize, usize)> {
    let status = daemon.status_bytes(limit)?;
    let egress = if shutdown {
        size_of::<Vec<DaemonDiagnostic>>()
    } else {
        state.egress_diagnostics.diagnostics_bytes(limit)?
    };
    let lifecycle = lifecycle_bytes(&state.lifecycle_counters, limit)?;
    let home = daemon.installation_home_bytes(limit)?;
    checked_live_bytes(
        limit,
        [
            size_of::<StatusResponseInput>(),
            request_id.len(),
            status.checked_sub(size_of::<crate::HubDaemonStatus>())?,
            egress.checked_sub(size_of::<Vec<DaemonDiagnostic>>())?,
            lifecycle.checked_sub(size_of::<DaemonLifecycleCounters>())?,
            home.checked_sub(size_of::<Option<PathBuf>>())?,
        ],
    )?;
    Some((status, egress, lifecycle, home))
}

/// Reserve distinct producer and keyed-result storage before Core allocates inventory.
fn core_inventory_allowance(input: &StatusResponseInput, limit: usize) -> Option<usize> {
    limit
        .checked_sub(input.logical_bytes(limit)?)?
        .checked_sub(size_of::<StatusCoreSnapshot>())?
        .checked_sub(size_of::<crate::owner_identity::OwnerWorkIdentity>())
}

fn capture_seed(
    daemon: &HubDaemon,
    state: &DaemonControlState,
    request_id: String,
    shutdown: bool,
    limit: usize,
) -> StatusResponseInput {
    let mut input = StatusResponseInput {
        #[cfg(test)]
        drop_probe: None,
        #[cfg(test)]
        capture_observer: None,
        seed: None,
        core: None,
        request_id,
        shutdown,
        rejected: false,
    };
    let Some((status_bytes, egress_bytes, lifecycle_bound, home_bytes)) =
        seed_preflight(daemon, state, &input.request_id, shutdown, limit)
    else {
        input.rejected = true;
        return input;
    };
    // These sources cannot mutate between preflight and capture in this Owner slice.
    let (status, _) = daemon
        .bounded_status(status_bytes)
        .expect("the preflighted status is unchanged");
    let (egress, _) = if shutdown {
        (Vec::new(), egress_bytes)
    } else {
        state
            .egress_diagnostics
            .bounded_diagnostics(egress_bytes)
            .expect("the preflighted diagnostics are unchanged")
    };
    assert!(lifecycle_bytes(&state.lifecycle_counters, lifecycle_bound).is_some());
    let (installation_home, _) = daemon
        .bounded_installation_home(home_bytes)
        .expect("the startup installation home is unchanged");
    input.seed = Some(StatusResponseSeed {
        status,
        session_count: state.maintenance.projection.rows.len(),
        egress,
        lifecycle: state.lifecycle_counters.clone(),
        installation_home,
        counters: daemon
            .runtime()
            .expect("the admitted daemon is running")
            .event_plane_counters()
            .clone(),
        retention: None,
        occupancy: Vec::new(),
        terminal_records: Vec::new(),
    });
    input
}

/// Recheck current sources after the Core wait before allocating either copy.
fn capture_current_sources(
    daemon: &mut HubDaemon,
    state: &DaemonControlState,
    input: &mut StatusResponseInput,
    limit: usize,
) -> Option<()> {
    let live = input.logical_bytes(limit)?;
    if input.shutdown {
        return Some(());
    }
    let inventory = &input
        .core
        .as_ref()?
        .inventory
        .as_ref()
        .ok()?
        .as_ref()?
        .records;
    let remaining = limit.checked_sub(live)?;
    let occupancy_bound = crate::subscription::attach_routes::live_attach_occupancy_prepared_bytes(
        &state.pending_runtime.live_attach_routes,
        inventory,
        remaining,
    )?;
    let terminal_bound = daemon.local_webrtc().terminal_records_bytes(remaining)?;
    // Both construction bounds overlap the retained seed and returned Core inventory.
    checked_live_bytes(limit, [live, occupancy_bound, terminal_bound])?;
    let (occupancy, _) = crate::subscription::attach_routes::try_live_attach_occupancy_rows(
        &state.pending_runtime.live_attach_routes,
        inventory,
        &state.pending_runtime,
        occupancy_bound,
    )?;
    let (terminal_records, _) = daemon
        .local_webrtc()
        .bounded_terminal_records(terminal_bound)
        .expect("the preflighted terminal records are unchanged in this Owner slice");
    let seed = input.seed.as_mut()?;
    seed.session_count = state.maintenance.projection.rows.len();
    seed.occupancy = occupancy;
    seed.terminal_records = terminal_records;
    Some(())
}

fn submit(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    identity: HostJobIdentity,
    command: HostCommand,
    permit: HostWorkPermit,
) -> ControlPoll {
    let Some(runtime) = daemon.runtime() else {
        clear_shutdown_waiter(state, identity.waiter_id);
        return super::host_work::retain_submission(
            state,
            HostSubmissionFailure {
                error: crate::host_executor::HostSubmitError::Stopped,
                identity,
                command,
                permit,
            },
        );
    };
    match runtime.host_executor().submit(identity, command, permit) {
        Ok(()) => ControlPoll::Pending,
        Err(failure) => {
            clear_shutdown_waiter(state, identity.waiter_id);
            super::host_work::retain_submission(state, failure)
        }
    }
}

fn retain_completion(
    state: &mut DaemonControlState,
    completion: crate::host_executor::HostCompletion,
) -> ControlPoll {
    if let Some(waiter_id) = state.current_waiter_id {
        clear_shutdown_waiter(state, waiter_id);
    }
    let (identity, result, permit) = completion.into_parts();
    super::host_work::retain_submission(
        state,
        HostSubmissionFailure {
            error: crate::host_executor::HostSubmitError::Stopped,
            identity,
            command: HostCommand::DiscardCompletion(Box::new(result)),
            permit,
        },
    )
}

fn clear_shutdown_waiter(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
) {
    if state.shutdown_waiter == Some(waiter_id) {
        state.shutdown_waiter = None;
    }
}

pub(crate) fn submit_delivery(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    entry: &mut PendingControlRequest,
    prepared: PreparedStatusResponse,
    permit: HostWorkPermit,
    phase: u64,
) -> bool {
    let identity = HostJobIdentity {
        waiter_id: entry.waiter_id,
        phase,
    };
    let shutdown = prepared.shutdown;
    let command = HostCommand::DeliverStatusResponse {
        prepared,
        reply_tx: entry.reply_tx.take(),
    };
    let result = match daemon.runtime() {
        Some(runtime) => runtime.host_executor().submit(identity, command, permit),
        None => Err(HostSubmissionFailure {
            error: crate::host_executor::HostSubmitError::Stopped,
            identity,
            command,
            permit,
        }),
    };
    if let Err(mut failure) = result {
        let HostCommand::DeliverStatusResponse { reply_tx, .. } = &mut failure.command else {
            unreachable!("delivery refusal returns its command");
        };
        entry.reply_tx = reply_tx.take();
        clear_shutdown_waiter(state, identity.waiter_id);
        state.host_recovery.insert(
            identity.waiter_id,
            super::host_work::HostRecoveryRequired::Submission {
                owner_permit: None,
                failure,
                package_restore: None,
                managed_worktree: None,
            },
        );
        let super::pending::ControlContinuation::Status(continuation) = &mut entry.continuation
        else {
            unreachable!("Status delivery retains its typed continuation");
        };
        continuation.delivery = Some(Delivery::Refused);
        return true;
    }
    let super::pending::ControlContinuation::Status(continuation) = &mut entry.continuation else {
        unreachable!("Status delivery retains its typed continuation");
    };
    continuation.delivery = Some(Delivery::Waiting(identity));
    false
}

fn poll_delivery(state: &mut DaemonControlState, identity: HostJobIdentity) -> ControlPoll {
    let Some(completion) = state.host_completions.remove(&identity.waiter_id) else {
        return ControlPoll::Pending;
    };
    if completion.identity != identity {
        return retain_completion(state, completion);
    }
    let (_, result, permit) = completion.into_parts();
    match result {
        HostResult::StatusResponseDelivered { shutdown, received } => {
            drop(permit);
            ControlPoll::StatusResponseDelivered { shutdown, received }
        }
        result => retain_completion(
            state,
            crate::host_executor::HostCompletion::from_parts(identity, result, permit),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::control::message::{ControlMessage, control_reply_channel};
    use crate::host_executor::{HOST_OPERATION_CAPACITY, HOST_PREPARED_BYTE_CAPACITY};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn transferred_status_delivery_survives_deadline_until_host_completion() {
        let (mut daemon, directory) = test_daemon("status-transferred-deadline");
        let mut state = DaemonControlState::default();
        let executor = daemon.runtime().unwrap().host_executor();
        let gates = [
            std::sync::Arc::new(crate::host_executor::TestHostGate::default()),
            std::sync::Arc::new(crate::host_executor::TestHostGate::default()),
        ];
        for (index, gate) in gates.iter().enumerate() {
            let identity =
                HostJobIdentity::first(crate::owner_identity::WaiterId(100 + index as u64));
            executor
                .submit(
                    identity,
                    HostCommand::Wait {
                        generation: index as u64,
                        gate: gate.clone(),
                    },
                    executor.try_reserve().expect("reserve blocking Host work"),
                )
                .expect("submit blocking Host work");
        }
        let start_deadline = Instant::now() + Duration::from_secs(5);
        while gates.iter().any(|gate| !gate.has_started()) {
            assert!(Instant::now() < start_deadline, "both Host workers start");
            std::thread::yield_now();
        }

        let waiter_id = crate::owner_identity::WaiterId(1);
        let owner_permit = state.budget.reserve().expect("reserve owner row");
        let (reply_tx, reply_rx) = control_reply_channel();
        let mut entry = PendingControlRequest {
            waiter_id,
            ready_class: crate::daemon::owner_schedule::ReadyClass::HostCompletion,
            ready_key: None,
            deadline_key: None,
            last_core_phase: 0,
            last_host_phase: 0,
            completion: super::super::pending::OwnerRequestCompletion::default(),
            reply_tx,
            response_delivery_rx: None,
            grant_id: None,
            client: None,
            permit: Some(owner_permit),
            must_finish: false,
            past_deadline: false,
            continuation: super::super::pending::ControlContinuation::Status(Box::new(
                StatusContinuation {
                    waiter_id,
                    shutdown: false,
                    policy: daemon.runtime().unwrap().retention_policy(),
                    ticket: None,
                    input: None,
                    permit: None,
                    prepared: None,
                    phase: 1,
                    entity_cancel_after: None,
                    delivery: None,
                },
            )),
            retire: None,
        };
        let delivery_permit = executor.try_reserve().expect("reserve delivery work");
        assert!(!submit_delivery(
            &daemon,
            &mut state,
            &mut entry,
            PreparedStatusResponse {
                dispose_probe: None,
                kind: botster_hub_client::DaemonResponseKind::Status,
                encoded_frame: Some(vec![1]),
                shutdown: false,
            },
            delivery_permit,
            1,
        ));
        assert!(entry.reply_tx.is_transferred());
        let arm = state
            .deadlines
            .arm(waiter_id, Instant::now(), Instant::now())
            .expect("arm due deadline");
        entry.deadline_key = Some(arm.key());
        state.pending_requests.insert(waiter_id, entry);
        assert!(super::super::pending::mark_owner_ready(
            &mut state,
            waiter_id,
            crate::daemon::owner_schedule::ReadyClass::Deadline,
            super::super::pending::READY_DEADLINE,
        ));
        let item = state.owner_ready.pop_next().expect("due status row");
        assert!(!super::super::pending::poll_ready_request_item(
            &mut daemon,
            &mut state,
            item,
            &mut |_, _, _, _| panic!("waiting delivery must not finish directly"),
        ));
        assert!(state.pending_requests.contains_key(&waiter_id));
        assert_eq!(state.budget.counters.retired_abandoned, 0);
        assert_eq!(state.budget.counters.requests_past_deadline, 1);
        assert_eq!(
            state
                .lifecycle_counters
                .cleanup_by_reason
                .get("request_past_deadline"),
            Some(&1)
        );

        for gate in &gates {
            gate.release();
        }
        let completion_deadline = Instant::now() + Duration::from_secs(10);
        while state.pending_requests.contains_key(&waiter_id) {
            crate::daemon::owner_loop::publish_completion_wakes(&mut daemon, &mut state);
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(
                Instant::now() < completion_deadline,
                "transferred delivery completion retires the row"
            );
            std::thread::yield_now();
        }
        assert_eq!(state.budget.outstanding(), 0);
        assert_eq!(state.budget.counters.retired_abandoned, 0);
        assert_eq!(state.budget.counters.requests_past_deadline, 1);
        let reply = reply_rx
            .blocking_recv()
            .expect("Host delivered the response");
        drop(reply);
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn test_daemon(name: &str) -> (HubDaemon, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "botster-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let config = crate::HubStartupOptions {
            data_directory: crate::DataDirectoryOption::Explicit(directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .unwrap();
        (HubDaemon::start(config).unwrap(), directory)
    }

    fn dispatch_status_for_test(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
    ) -> crate::daemon::control::message::ControlReplyReceiver {
        let transport = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (control_tx, _) =
            tokio::sync::mpsc::channel(crate::admission::budgets::DAEMON_CONTROL_QUEUE_CAPACITY);
        let (reply_tx, reply_rx) = control_reply_channel();
        assert!(!super::super::request::handle(
            daemon,
            state,
            transport.handle(),
            control_tx,
            ControlMessage::Request {
                request: Box::new(botster_hub_client::DaemonRequest::Status),
                transport_request_id: Some("41".into()),
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client_id: None,
                enqueued_at: Instant::now(),
            },
        ));
        reply_rx
    }

    fn fill_seed_for_core_allowance(
        daemon: &HubDaemon,
        state: &mut DaemonControlState,
        core_allowance: usize,
    ) {
        assert!(state.lifecycle_counters.cleanup_by_reason.is_empty());
        let input = capture_seed(
            daemon,
            state,
            "41".into(),
            false,
            HOST_PREPARED_BYTE_CAPACITY,
        );
        let remaining = core_inventory_allowance(&input, HOST_PREPARED_BYTE_CAPACITY).unwrap();
        let key_bytes = remaining
            .checked_sub(size_of::<(String, u64)>())
            .unwrap()
            .checked_sub(core_allowance)
            .unwrap();
        state
            .lifecycle_counters
            .cleanup_by_reason
            .insert("r".repeat(key_bytes), 1);
    }

    #[test]
    fn status_core_allowance_counts_retained_input_and_distinct_result_wrappers() {
        let (mut daemon, directory) = test_daemon("status-core-allowance");
        let state = DaemonControlState::default();
        let input = capture_seed(&daemon, &state, "41".into(), false, usize::MAX);
        let retained = input.logical_bytes(usize::MAX).unwrap();
        let wrappers =
            size_of::<StatusCoreSnapshot>() + size_of::<crate::owner_identity::OwnerWorkIdentity>();
        let inventory = size_of::<botster_core::TerminalSubscriptionInventory>();
        let exact = retained + wrappers + inventory;
        assert_eq!(core_inventory_allowance(&input, exact), Some(inventory));
        assert_eq!(
            core_inventory_allowance(&input, exact - 1),
            Some(inventory - 1)
        );
        assert_eq!(
            core_inventory_allowance(&input, retained + wrappers),
            Some(0)
        );
        assert_eq!(
            core_inventory_allowance(&input, retained + wrappers - 1),
            None
        );
        assert_eq!(core_inventory_allowance(&input, retained - 1), None);
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn status_dispatch_core_preallocation_uses_aggregate_exact_fit_and_one_byte_short() {
        for refuse_inventory in [false, true] {
            let (mut daemon, directory) = test_daemon("status-core-preallocation");
            let mut state = DaemonControlState::default();
            let allowance = size_of::<botster_core::TerminalSubscriptionInventory>()
                - usize::from(refuse_inventory);
            fill_seed_for_core_allowance(&daemon, &mut state, allowance);
            let reply = dispatch_status_for_test(&mut daemon, &mut state);
            let (observed, observation) = std::sync::mpsc::channel();
            let entry = state.pending_requests.values_mut().next().unwrap();
            let super::super::pending::ControlContinuation::Status(continuation) =
                &mut entry.continuation
            else {
                panic!("Status continuation");
            };
            assert!(continuation.ticket.is_some());
            let input = continuation.input.as_mut().unwrap();
            assert!(!input.rejected);
            assert!(input.seed.is_some());
            input.capture_observer = Some(observed);
            let original = continuation
                .permit
                .as_ref()
                .unwrap()
                .reserved_prepared_bytes();
            assert_eq!(original, HOST_PREPARED_BYTE_CAPACITY);
            assert_eq!(core_inventory_allowance(input, original), Some(allowance));
            assert_eq!(
                input.logical_bytes(original).unwrap()
                    + size_of::<StatusCoreSnapshot>()
                    + size_of::<crate::owner_identity::OwnerWorkIdentity>()
                    + allowance,
                original,
            );
            let deadline = Instant::now() + Duration::from_secs(10);
            while !state.pending_requests.is_empty() {
                assert!(!crate::daemon::owner_loop::drive_ready_test_turn(
                    &mut daemon,
                    &mut state,
                ));
                assert!(
                    Instant::now() < deadline,
                    "Status must finish after Core preallocation"
                );
                std::thread::yield_now();
            }
            let (_, occupancy, terminals, core_retained, core_refused) =
                observation.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!((occupancy, terminals, core_retained), (0, 0, true));
            assert_eq!(core_refused, refuse_inventory);
            let (_, charge, encoded) = reply.blocking_recv().unwrap().into_parts();
            let botster_hub_client::ServerFrame::Response {
                request_id,
                response,
            } = serde_json::from_slice(encoded.as_ref().unwrap()).unwrap()
            else {
                panic!("response frame");
            };
            assert_eq!(request_id, "41");
            // The near-full seed leaves no allowance for Host response preparation.
            let error = response.error.unwrap();
            assert_eq!(error.code, "host_result_too_large");
            assert_eq!(error.request_id, request_id);
            assert_eq!(error.operation, "status");
            drop(charge);
            assert_eq!(
                daemon.runtime().unwrap().host_executor().prepared_bytes(),
                0
            );
            daemon.stop();
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn status_dispatch_rechecks_grown_sources_after_core_before_aggregate_copy() {
        use crate::transport::webrtc::peer::{
            LocalWebrtcChannelTerminalSignal, LocalWebrtcCleanupDisposition,
            LocalWebrtcSenderTerminalRecord, LocalWebrtcTerminalCause,
        };
        let (mut daemon, directory) = test_daemon("status-dispatch-growth");
        let mut state = DaemonControlState::default();
        let (entered, ready) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let mut blocker = daemon.runtime().unwrap().submit_core(move |_| {
            let _ = entered.send(());
            let _ = gate.recv_timeout(Duration::from_secs(5));
        });
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let reply = dispatch_status_for_test(&mut daemon, &mut state);
        let (observed, observation) = std::sync::mpsc::channel();
        let entry = state.pending_requests.values_mut().next().unwrap();
        let super::super::pending::ControlContinuation::Status(continuation) =
            &mut entry.continuation
        else {
            panic!("Status continuation");
        };
        let input = continuation.input.as_mut().unwrap();
        input.capture_observer = Some(observed);
        let live = input.logical_bytes(HOST_PREPARED_BYTE_CAPACITY).unwrap();
        assert!(!input.rejected);
        assert!(input.seed.as_ref().unwrap().occupancy.is_empty());
        assert!(matches!(
            continuation.ticket.as_mut().unwrap().poll(),
            CoreTicketPoll::Pending
        ));
        daemon
            .local_webrtc()
            .retain_terminal_record(LocalWebrtcSenderTerminalRecord {
                schema_version: 1,
                grant_id: "grown-peer".into(),
                request_operation: "status".into(),
                message_id: Some("grown-message".into()),
                next_chunk_index: 1,
                last_sent_chunk_index: Some(0),
                total_chunks: 2,
                pressured: true,
                peer_connection_state: "failed".into(),
                channel_terminal_signal: LocalWebrtcChannelTerminalSignal::OnClose,
                cause: LocalWebrtcTerminalCause::PeerFailed,
                cleanup_disposition: LocalWebrtcCleanupDisposition::NewlySent,
            })
            .unwrap();
        let remaining = HOST_PREPARED_BYTE_CAPACITY - live;
        let peer_bytes = daemon
            .local_webrtc()
            .terminal_records_bytes(remaining)
            .unwrap();
        state
            .pending_runtime
            .live_attach_routes
            .insert((String::new(), String::new()));
        let row_base = crate::subscription::attach_routes::live_attach_occupancy_prepared_bytes(
            &state.pending_runtime.live_attach_routes,
            &[],
            remaining,
        )
        .unwrap();
        state.pending_runtime.live_attach_routes.clear();
        let key_bytes = (remaining - row_base - peer_bytes / 2) / 2;
        state
            .pending_runtime
            .live_attach_routes
            .insert(("s".repeat(key_bytes), String::new()));
        let occupancy_bytes =
            crate::subscription::attach_routes::live_attach_occupancy_prepared_bytes(
                &state.pending_runtime.live_attach_routes,
                &[],
                remaining,
            )
            .unwrap();
        assert!(
            checked_live_bytes(
                HOST_PREPARED_BYTE_CAPACITY,
                [live, occupancy_bytes, peer_bytes]
            )
            .is_none()
        );
        release.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !state.pending_requests.is_empty() {
            assert!(!crate::daemon::owner_loop::drive_ready_test_turn(
                &mut daemon,
                &mut state
            ));
            assert!(
                Instant::now() < deadline,
                "grown Status sources must receive a capacity response"
            );
            std::thread::yield_now();
        }
        assert!(matches!(blocker.poll(), CoreTicketPoll::Ready(())));
        assert_eq!(
            observation.recv_timeout(Duration::from_secs(5)).unwrap(),
            (true, 0, 0, true, false)
        );
        let (_, charge, encoded) = reply.blocking_recv().unwrap().into_parts();
        let botster_hub_client::ServerFrame::Response {
            request_id,
            response,
        } = serde_json::from_slice(encoded.as_ref().unwrap()).unwrap()
        else {
            panic!("response frame");
        };
        assert_eq!(request_id, "41");
        let error = response.error.unwrap();
        assert_eq!(error.code, "host_result_too_large");
        assert_eq!(error.request_id, request_id);
        assert_eq!(error.operation, "status");
        drop(charge);
        assert_eq!(
            daemon.runtime().unwrap().host_executor().prepared_bytes(),
            0
        );
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn status_core_result_retains_original_reservation_after_ticket_and_permit_disposal() {
        check_status_core_retention(true, false);
    }

    #[test]
    fn status_core_queue_retains_original_reservation_after_ticket_and_permit_disposal() {
        check_status_core_retention(false, false);
    }

    #[test]
    fn status_core_refusal_retains_original_reservation_after_ticket_and_permit_disposal() {
        check_status_core_retention(true, true);
    }

    fn check_status_core_retention(hold_result: bool, refuse_inventory: bool) {
        let (mut daemon, directory) = test_daemon("status-core-result-retention");
        let mut state = DaemonControlState::default();
        if refuse_inventory {
            fill_seed_for_core_allowance(
                &daemon,
                &mut state,
                size_of::<botster_core::TerminalSubscriptionInventory>() - 1,
            );
        }
        let (entered, ready) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let mut blocker = if hold_result {
            TEST_STATUS_CORE_RESULT_GATE.with(|slot| {
                assert!(slot.borrow_mut().replace((entered, gate)).is_none());
            });
            None
        } else {
            let blocker = daemon.runtime().unwrap().submit_core(move |_| {
                let _ = entered.send(false);
                let _ = gate.recv_timeout(Duration::from_secs(5));
            });
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
            Some(blocker)
        };
        let reply = dispatch_status_for_test(&mut daemon, &mut state);
        if hold_result {
            assert_eq!(
                ready.recv_timeout(Duration::from_secs(5)).unwrap(),
                refuse_inventory
            );
        }
        let waiter = *state.pending_requests.keys().next().unwrap();
        let mut entry = state.pending_requests.remove(&waiter).unwrap();
        let super::super::pending::ControlContinuation::Status(continuation) =
            &mut entry.continuation
        else {
            panic!("Status continuation");
        };
        assert!(
            continuation
                .permit
                .as_ref()
                .unwrap()
                .has_retained_prepared_reservation()
        );
        let mut completion = None;
        let parts = continuation
            .take_terminal_parts(HostJobIdentity::first(waiter), &mut completion)
            .unwrap();
        assert!(continuation.ticket.is_none());
        let mut job = crate::host_disposal::Job::new(parts);
        let deadline = Instant::now() + Duration::from_secs(5);
        let permit = loop {
            match job.poll() {
                crate::host_disposal::Poll::Disposed(permit) => break permit,
                crate::host_disposal::Poll::Pending => assert!(Instant::now() < deadline),
                _ => panic!("terminal input disposal must return the original permit"),
            }
            std::thread::yield_now();
        };
        drop(permit);
        let executor = daemon.runtime().unwrap().host_executor();
        assert_eq!(executor.outstanding(), 0);
        assert_eq!(executor.prepared_bytes(), HOST_PREPARED_BYTE_CAPACITY);
        release.send(()).unwrap();
        while executor.prepared_bytes() != 0 {
            assert!(
                Instant::now() < deadline,
                "Core result destruction must release the retained reservation"
            );
            std::thread::yield_now();
        }
        if let Some(blocker) = blocker.as_mut() {
            assert!(matches!(blocker.poll(), CoreTicketPoll::Ready(())));
        }
        state.budget.release(entry.permit.take().unwrap());
        drop(entry);
        drop(reply);
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn status_seed_aggregate_precedes_copies_at_exact_fit_and_one_byte_short() {
        let (mut daemon, directory) = test_daemon("status-seed-boundary");
        let mut state = DaemonControlState::default();
        state
            .lifecycle_counters
            .cleanup_by_reason
            .insert("reason".repeat(128), 1);
        let input = capture_seed(&daemon, &state, "41".into(), false, usize::MAX);
        let bytes = input.logical_bytes(usize::MAX).unwrap();
        assert!(daemon.status_bytes(bytes - 1).is_some());
        assert!(lifecycle_bytes(&state.lifecycle_counters, bytes - 1).is_some());
        assert!(daemon.installation_home_bytes(bytes - 1).is_some());
        let exact = capture_seed(&daemon, &state, "41".into(), false, bytes);
        assert!(!exact.rejected);
        assert!(exact.seed.is_some());
        let short = capture_seed(&daemon, &state, "41".into(), false, bytes - 1);
        assert!(short.rejected);
        assert!(short.seed.is_none());
        assert_eq!(short.request_id, "41");
        state
            .lifecycle_counters
            .cleanup_by_reason
            .insert("later-source-growth".into(), 2);
        assert!(
            capture_seed(&daemon, &state, "41".into(), false, bytes)
                .seed
                .is_none()
        );
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn status_current_sources_count_inventory_overlap_and_recheck_growth_before_copy() {
        let (mut daemon, directory) = test_daemon("status-current-boundary");
        let mut state = DaemonControlState::default();
        let mut input = capture_seed(&daemon, &state, "41".into(), false, usize::MAX);
        let permit = daemon
            .runtime()
            .unwrap()
            .host_executor()
            .try_reserve()
            .unwrap();
        input.core = Some(StatusCoreSnapshot::new(
            Default::default(),
            Ok(Some(botster_core::TerminalSubscriptionInventory {
                logical_bytes: size_of::<botster_core::TerminalSubscriptionInventory>()
                    + size_of::<botster_core::TerminalSubscriptionRecord>()
                    + "client".len()
                    + "session".len()
                    + "subscription".len(),
                records: vec![botster_core::TerminalSubscriptionRecord {
                    client_id: botster_core::ClientId("client".into()),
                    session_id: botster_core::SessionId("session".into()),
                    subscription_id: botster_core::SubscriptionId("subscription".into()),
                    generation: botster_core::TerminalSubscriptionGeneration(1),
                    adapter_bound: false,
                    capabilities: None,
                }],
            })),
            permit.retain_prepared_reservation(),
        ));
        state
            .pending_runtime
            .live_attach_routes
            .insert(("session".into(), "subscription".into()));
        let live = input.logical_bytes(usize::MAX).unwrap();
        let inventory = &input
            .core
            .as_ref()
            .unwrap()
            .inventory
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .records;
        let occupancy = crate::subscription::attach_routes::live_attach_occupancy_prepared_bytes(
            &state.pending_runtime.live_attach_routes,
            inventory,
            usize::MAX,
        )
        .unwrap();
        let terminal = daemon
            .local_webrtc()
            .terminal_records_bytes(usize::MAX)
            .unwrap();
        let exact = live + occupancy + terminal;
        assert!(live < exact - 1 && occupancy < exact - 1 && terminal < exact - 1);
        assert!(capture_current_sources(&mut daemon, &state, &mut input, exact - 1).is_none());
        assert!(input.seed.as_ref().unwrap().occupancy.is_empty());
        assert!(input.seed.as_ref().unwrap().terminal_records.is_empty());
        let growth = ("new-session".repeat(64), "new-subscription".repeat(64));
        state
            .pending_runtime
            .live_attach_routes
            .insert(growth.clone());
        assert!(capture_current_sources(&mut daemon, &state, &mut input, exact).is_none());
        assert!(input.seed.as_ref().unwrap().occupancy.is_empty());
        state.pending_runtime.live_attach_routes.remove(&growth);
        assert!(capture_current_sources(&mut daemon, &state, &mut input, exact).is_some());
        assert_eq!(input.seed.as_ref().unwrap().occupancy.len(), 1);
        drop(input);
        drop(permit);
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn status_dispatch_preserves_response_and_capacity_error_correlation() {
        for refuse_seed in [false, true] {
            let (mut daemon, directory) = test_daemon("status-dispatch");
            let mut state = DaemonControlState::default();
            if refuse_seed {
                state
                    .lifecycle_counters
                    .cleanup_by_reason
                    .insert("x".repeat(HOST_PREPARED_BYTE_CAPACITY), 1);
            }
            let transport = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let (control_tx, _) = tokio::sync::mpsc::channel(
                crate::admission::budgets::DAEMON_CONTROL_QUEUE_CAPACITY,
            );
            let (reply_tx, reply_rx) = control_reply_channel();
            assert!(!super::super::request::handle(
                &mut daemon,
                &mut state,
                transport.handle(),
                control_tx,
                ControlMessage::Request {
                    request: Box::new(botster_hub_client::DaemonRequest::Status),
                    transport_request_id: Some("18446744073709551615".into()),
                    reply_tx,
                    response_delivery_rx: None,
                    grant_id: None,
                    client_id: None,
                    enqueued_at: Instant::now(),
                },
            ));
            let entry = state.pending_requests.values().next().unwrap();
            let super::super::pending::ControlContinuation::Status(continuation) =
                &entry.continuation
            else {
                panic!("Status continuation");
            };
            assert_eq!(
                continuation.input.as_ref().unwrap().seed.is_none(),
                refuse_seed
            );
            assert_eq!(continuation.ticket.is_none(), refuse_seed);
            assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
            let deadline = Instant::now() + Duration::from_secs(10);
            while !state.pending_requests.is_empty() {
                assert!(!crate::daemon::owner_loop::drive_ready_test_turn(
                    &mut daemon,
                    &mut state
                ));
                assert!(Instant::now() < deadline, "Status dispatch must complete");
                std::thread::yield_now();
            }
            let (_, charge, encoded) = reply_rx.blocking_recv().unwrap().into_parts();
            let encoded = encoded.expect("Host encoded the Status response");
            let botster_hub_client::ServerFrame::Response {
                request_id,
                response,
            } = serde_json::from_slice(&encoded).unwrap()
            else {
                panic!("response frame");
            };
            assert_eq!(request_id, "18446744073709551615");
            if refuse_seed {
                let error = response.error.expect("capacity error");
                assert_eq!(error.code, "host_result_too_large");
                assert_eq!(error.request_id, request_id);
                assert_eq!(error.operation, "status");
            } else {
                assert_eq!(
                    response.kind,
                    botster_hub_client::DaemonResponseKind::Status
                );
                assert!(response.status.is_some());
            }
            let executor = daemon.runtime().unwrap().host_executor();
            assert_eq!(executor.outstanding(), 0);
            assert_eq!(executor.prepared_bytes(), encoded.len());
            drop(charge);
            assert_eq!(executor.prepared_bytes(), 0);
            daemon.stop();
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn oversized_shutdown_keeps_its_stop_effect_and_original_slot_through_delivery() {
        for closed_reply in [false, true] {
            run_shutdown_case(closed_reply, false, false);
        }
    }

    #[test]
    fn refused_shutdown_delivery_restores_sender_and_preserves_stop_and_fault_ownership() {
        run_shutdown_case(false, true, false);
    }

    #[test]
    fn seed_refused_shutdown_keeps_stop_delivery_and_request_correlation() {
        run_shutdown_case(false, false, true);
        run_shutdown_case(true, false, true);
        run_shutdown_case(false, true, true);
    }

    fn run_shutdown_case(closed_reply: bool, refuse_delivery: bool, seed_refusal: bool) {
        let directory = std::env::temp_dir().join(format!(
            "botster-status-shutdown-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            data_directory: crate::DataDirectoryOption::Explicit(directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build daemon config");
        let mut daemon = HubDaemon::start(config).expect("start daemon");
        let mut state = DaemonControlState::default();
        if refuse_delivery {
            daemon
                .runtime()
                .unwrap()
                .host_executor()
                .test_refuse_status_delivery();
        }
        state.lifecycle_counters.cleanup_by_reason.insert(
            "\\".repeat(if seed_refusal {
                HOST_PREPARED_BYTE_CAPACITY
            } else {
                botster_hub_client::MAX_CONTROL_RESPONSE_BYTES
            }),
            1,
        );
        let transport = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("transport runtime");
        let (control_tx, _control_rx) =
            tokio::sync::mpsc::channel(crate::admission::budgets::DAEMON_CONTROL_QUEUE_CAPACITY);
        let blockers = (0..HOST_OPERATION_CAPACITY - 1)
            .map(|_| {
                daemon
                    .runtime()
                    .unwrap()
                    .host_executor()
                    .try_reserve()
                    .expect("reserve other slots")
            })
            .collect::<Vec<_>>();
        let (reply_tx, reply_rx) = control_reply_channel();
        let reply_rx = if closed_reply {
            drop(reply_rx);
            None
        } else {
            Some(reply_rx)
        };
        let (update_tx, update_rx) = control_reply_channel();
        state.pending_hub_update_reply = Some(update_tx);
        assert!(!super::super::request::handle(
            &mut daemon,
            &mut state,
            transport.handle(),
            control_tx,
            ControlMessage::Request {
                request: Box::new(botster_hub_client::DaemonRequest::DaemonShutdown),
                transport_request_id: Some("18446744073709551615".to_string()),
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client_id: None,
                enqueued_at: Instant::now(),
            }
        ));
        assert_eq!(
            daemon.runtime().unwrap().host_executor().outstanding(),
            HOST_OPERATION_CAPACITY
        );
        let entry = state
            .pending_requests
            .values()
            .next()
            .expect("the admitted shutdown is retained");
        let super::super::pending::ControlContinuation::Status(continuation) = &entry.continuation
        else {
            panic!("Status continuation");
        };
        let input = continuation.input.as_ref().unwrap();
        assert_eq!(input.rejected, seed_refusal);
        assert_eq!(input.seed.is_none(), seed_refusal);
        assert_eq!(continuation.ticket.is_none(), seed_refusal);
        assert_eq!(
            daemon.runtime().unwrap().host_executor().prepared_bytes(),
            HOST_OPERATION_CAPACITY * HOST_PREPARED_BYTE_CAPACITY
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state) {
                break;
            }
            assert!(Instant::now() < deadline, "admitted shutdown must finish");
            std::thread::yield_now();
        }
        let executor = daemon.runtime().unwrap().host_executor();
        assert_eq!(executor.status_stop_count(), 1);
        assert_eq!(
            executor.outstanding(),
            HOST_OPERATION_CAPACITY - 1 + usize::from(refuse_delivery)
        );
        assert!(state.pending_requests.is_empty());
        assert!(state.shutdown_waiter.is_none());
        if let Some(reply_rx) = reply_rx {
            let reply = reply_rx.blocking_recv().expect("capacity reply");
            let (response, charge, encoded) = reply.into_parts();
            if refuse_delivery {
                assert!(matches!(
                    response,
                    Err(DaemonTransportError::ControlThreadStopped)
                ));
                assert!(charge.is_none());
                assert!(encoded.is_none());
            } else {
                let encoded = encoded.expect("Host encoded the capacity reply");
                assert_eq!(
                    executor.prepared_bytes(),
                    (HOST_OPERATION_CAPACITY - 1) * HOST_PREPARED_BYTE_CAPACITY + encoded.len()
                );
                let frame: botster_hub_client::ServerFrame =
                    serde_json::from_slice(&encoded).expect("decode capacity reply");
                let botster_hub_client::ServerFrame::Response {
                    request_id,
                    response,
                } = frame
                else {
                    panic!("response frame");
                };
                assert_eq!(request_id, "18446744073709551615");
                assert_eq!(
                    response.kind,
                    botster_hub_client::DaemonResponseKind::OperatorError
                );
                let error = response.error.expect("capacity error");
                assert_eq!(error.request_id, request_id);
                assert_eq!(error.operation, "shutdown");
                assert_eq!(error.code, "host_result_too_large");
                drop(charge);
            }
        }
        assert_eq!(
            executor.prepared_bytes(),
            (HOST_OPERATION_CAPACITY - 1 + usize::from(refuse_delivery))
                * HOST_PREPARED_BYTE_CAPACITY
        );
        if refuse_delivery {
            assert_eq!(state.host_recovery.len(), 1);
            let waiter = *state.host_recovery.keys().next().unwrap();
            let super::super::host_work::HostRecoveryRequired::Submission {
                owner_permit,
                failure,
                ..
            } = state.host_recovery.remove(&waiter).unwrap()
            else {
                panic!("delivery fault retains its submission");
            };
            assert_eq!(failure.identity.phase, 3);
            assert!(
                matches!(&failure.command, HostCommand::DeliverStatusResponse { prepared, reply_tx } if prepared.shutdown && prepared.encoded_frame.is_some() && reply_tx.is_closed())
            );
            failure
                .permit
                .dispose(failure.identity, failure.command)
                .expect("test disposes retained failure on Host");
            while executor.outstanding() != HOST_OPERATION_CAPACITY - 1
                || executor.prepared_bytes()
                    != (HOST_OPERATION_CAPACITY - 1) * HOST_PREPARED_BYTE_CAPACITY
            {
                assert!(
                    Instant::now() < deadline,
                    "retained failure disposal completes"
                );
                std::thread::yield_now();
            }
            state
                .budget
                .release(owner_permit.expect("fault retains its Owner permit"));
        }
        let update = update_rx
            .blocking_recv()
            .expect("update reply")
            .expect("update response")
            .hub_update
            .expect("update state");
        assert_eq!(update.reason.as_deref(), Some("daemon_shutdown"));
        assert!(!crate::daemon::owner_loop::drive_ready_test_turn(
            &mut daemon,
            &mut state
        ));
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .host_executor()
                .status_stop_count(),
            1
        );
        drop(blockers);
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove test state");
    }
}
