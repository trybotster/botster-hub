//! Status and Shutdown preparation through the existing Host executor.
//!
//! Owner seed capture and Core inventory allocation remain accounting dependencies.
//! Terminal teardown must transfer retained commands before dropping the control state.

use botster_hub_client::{DaemonCompatibility, DaemonRetentionAccounting};

use crate::HubDaemon;
use crate::daemon::control::DaemonObservability;
use crate::daemon::control::pending::{ControlPoll, ControlStep, PendingControlRequest};
use crate::daemon::error::DaemonTransportError;
use crate::daemon::owner_loop::DaemonControlState;
use crate::data_plane::driver::CoreTicketPoll;
use crate::host_executor::{
    HostCommand, HostJobIdentity, HostResult, HostSubmissionFailure, HostWorkPermit,
};
use crate::status_response::{PreparedStatusResponse, StatusResponseInput};

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
    let mut ticket = runtime.submit_core_for_owner(waiter_id, move |core| {
        (
            core.retention_accounting(),
            (!shutdown).then(|| core.list_terminal_subscriptions()),
        )
    });
    let initial_count = state.maintenance.projection.rows.len();
    let counters = runtime.event_plane_counters().clone();
    let mut seed = Some((daemon.status(), observability));
    let mut permit = Some(permit);
    let mut prepared: Option<PreparedStatusResponse> = None;
    let mut phase = 0;
    let mut entity_cancel_after = None;
    if shutdown {
        state.shutdown_waiter = Some(waiter_id);
    }
    ControlStep::pending(move |daemon, state| {
        if phase == 0 {
            let (accounting, inventory) = match ticket.poll() {
                CoreTicketPoll::Pending => return ControlPoll::Pending,
                CoreTicketPoll::Ready((accounting, inventory)) => (Some(accounting), inventory),
                CoreTicketPoll::Lost | CoreTicketPoll::Refused if shutdown => (None, None),
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
                    // The seed still requires worker disposal on this error path.
                    let (status, observability) = seed.take().expect("status seed exists");
                    let input = capture_input(
                        daemon,
                        state,
                        status,
                        observability,
                        counters.clone(),
                        None,
                        Vec::new(),
                        false,
                        initial_count,
                    );
                    let permit = permit.take().expect("status retains its slot");
                    if let Err(failure) = permit.dispose(
                        HostJobIdentity {
                            waiter_id,
                            phase: 1,
                        },
                        HostCommand::PrepareStatusResponse(input),
                    ) {
                        return super::host_work::retain_submission(state, failure);
                    }
                    return ControlPoll::Ready(Ok(response));
                }
            };
            let retention = accounting.map(|accounting| DaemonRetentionAccounting {
                max_object_bytes: policy.max_object_bytes as u64,
                max_total_bytes: policy.max_total_bytes as u64,
                max_sessions: u32::try_from(policy.max_sessions).unwrap_or(u32::MAX),
                total_bytes: accounting.total_bytes as u64,
                sessions: u32::try_from(accounting.sessions).unwrap_or(u32::MAX),
                evictions: accounting.evictions,
            });
            let occupancy = inventory.map_or_else(Vec::new, |inventory| {
                crate::subscription::attach_routes::live_attach_occupancy_rows(
                    &state.pending_runtime.live_attach_routes,
                    &inventory,
                    &state.pending_runtime,
                )
            });
            let (status, observability) = seed.take().expect("status seed exists");
            let input = capture_input(
                daemon,
                state,
                status,
                observability,
                counters.clone(),
                retention,
                occupancy,
                shutdown,
                initial_count,
            );
            phase = 1;
            return submit(
                daemon,
                state,
                HostJobIdentity { waiter_id, phase },
                HostCommand::PrepareStatusResponse(input),
                permit.take().expect("status retains its slot"),
            );
        }
        if let Some(completion) = state.host_completions.remove(&waiter_id) {
            if completion.identity != (HostJobIdentity { waiter_id, phase }) {
                return retain_completion(state, completion);
            }
            let (_, result, returned_permit) = completion.into_parts();
            match result {
                HostResult::StatusResponsePrepared(response) => {
                    prepared = Some(response);
                    permit = Some(returned_permit);
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
                            HostJobIdentity { waiter_id, phase },
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
        if shutdown && phase == 1 {
            if super::entities::cancel_next_plugin_entity_for_shutdown(
                state,
                &mut entity_cancel_after,
            ) {
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
            phase = 2;
            return submit(
                daemon,
                state,
                HostJobIdentity { waiter_id, phase },
                HostCommand::StopForStatus(prepared.take().expect("snapshot was encoded")),
                permit.take().expect("shutdown retains its original slot"),
            );
        }
        ControlPoll::DeliverStatusResponse(
            prepared.take().expect("snapshot was encoded"),
            permit.take().expect("response retains its original slot"),
            phase + 1,
        )
    })
}

fn capture_input(
    daemon: &mut HubDaemon,
    state: &DaemonControlState,
    status: crate::HubDaemonStatus,
    observability: DaemonObservability,
    counters: std::sync::Arc<crate::event_plane_counters::EventPlaneCounters>,
    retention: Option<DaemonRetentionAccounting>,
    occupancy: Vec<botster_hub_client::DaemonAttachOccupancy>,
    shutdown: bool,
    initial_count: usize,
) -> StatusResponseInput {
    StatusResponseInput {
        #[cfg(test)]
        drop_probe: None,
        status,
        session_count: if shutdown {
            initial_count
        } else {
            state.maintenance.projection.rows.len()
        },
        egress: if shutdown {
            Vec::new()
        } else {
            observability.egress
        },
        lifecycle: observability.lifecycle,
        software: crate::maintenance::software_identity(),
        installation: crate::maintenance::installation_identity(),
        compatibility: DaemonCompatibility::current(),
        counters,
        retention,
        occupancy,
        terminal_records: if shutdown {
            Vec::new()
        } else {
            daemon.local_webrtc().terminal_records()
        },
        request_id: observability.transport_request_id.unwrap_or_default(),
        shutdown,
    }
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
        entry.continuation = Box::new(move |_, _| ControlPoll::StatusResponseRefused { shutdown });
        return true;
    }
    entry.continuation = Box::new(move |_, state| {
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
    });
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::control::message::{ControlMessage, control_reply_channel};
    use crate::host_executor::{HOST_OPERATION_CAPACITY, HOST_PREPARED_BYTE_CAPACITY};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn oversized_shutdown_keeps_its_stop_effect_and_original_slot_through_delivery() {
        for closed_reply in [false, true] {
            run_shutdown_case(closed_reply, false);
        }
    }

    #[test]
    fn refused_shutdown_delivery_restores_sender_and_preserves_stop_and_fault_ownership() {
        run_shutdown_case(false, true);
    }

    fn run_shutdown_case(closed_reply: bool, refuse_delivery: bool) {
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
            "\\".repeat(botster_hub_client::MAX_CONTROL_RESPONSE_BYTES),
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
