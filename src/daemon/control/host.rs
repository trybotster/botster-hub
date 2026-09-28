//! Host update and daemon-shutdown request family.

use std::sync::mpsc;

use botster_hub_client::{
    DaemonHubUpdate, DaemonHubUpdateScope, DaemonHubUpdateState, DaemonRequest,
};

use crate::HubDaemon;
use crate::client_api_dto::response::{daemon_hub_update, daemon_hub_update_execution};
use crate::daemon::control::DaemonObservability;
use crate::daemon::control::message::{ControlReplySender, ControlSender};
use crate::daemon::control::pending::ControlStep;
use crate::daemon::error::hub_update_execution_error;
use crate::daemon::owner_loop::{
    DaemonControlState, send_control_response, wait_for_response_delivery,
};
use crate::maintenance::{
    HubUpdateCheckPlan, execute_managed_update_check, plan_hub_update_check, software_identity,
    source_update_refusal,
};
use crate::source_update::{current_update_execution, mark_update_failed, start_update_handoff};

pub(crate) fn handle_request(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    request: &DaemonRequest,
    reply_tx: ControlReplySender,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> Option<bool> {
    match request {
        DaemonRequest::CheckHubUpdate => Some(check_hub_update(
            state,
            transport_handle,
            control_tx,
            reply_tx,
            response_delivery_rx,
        )),
        DaemonRequest::StartHubUpdate { scope } => Some(start_hub_update(
            daemon,
            *scope,
            reply_tx,
            response_delivery_rx,
        )),
        DaemonRequest::GetHubUpdateExecution => Some(get_hub_update_execution(
            daemon,
            reply_tx,
            response_delivery_rx,
        )),
        _ => None,
    }
}

/// The one in-flight managed update check. Admission and completion follow
/// the fetch, not the reply: shutdown may answer the caller early (taking
/// `reply`) while the fetch still runs, and a check that arrives then must
/// get `busy`, so at most one fetch exists and every completion belongs to
/// this record.
pub(crate) struct HubUpdateCheck {
    pub(crate) reply: Option<ControlReplySender>,
}

fn check_hub_update(
    state: &mut DaemonControlState,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    reply_tx: ControlReplySender,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    match plan_hub_update_check() {
        HubUpdateCheckPlan::Immediate(update) => send_control_response(
            reply_tx,
            Ok(daemon_hub_update(update)),
            response_delivery_rx,
        ),
        HubUpdateCheckPlan::Managed(check) => start_managed_check(
            state,
            transport_handle,
            control_tx,
            reply_tx,
            response_delivery_rx,
            move || execute_managed_update_check(check),
        ),
    }
}

fn start_managed_check(
    state: &mut DaemonControlState,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    reply_tx: ControlReplySender,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
    fetch: impl FnOnce() -> DaemonHubUpdate + Send + 'static,
) -> bool {
    if state.hub_update_check.is_some() {
        return send_control_response(
            reply_tx,
            Ok(daemon_hub_update(DaemonHubUpdate {
                state: DaemonHubUpdateState::Unavailable,
                current_version: software_identity().version,
                available_version: None,
                build_revision: None,
                reason: Some("busy".to_string()),
                action: Some("retry".to_string()),
            })),
            response_delivery_rx,
        );
    }
    state.hub_update_check = Some(HubUpdateCheck {
        reply: Some(reply_tx),
    });
    transport_handle.spawn_blocking(move || {
        let update = fetch();
        let _ = control_tx.blocking_send(
            crate::daemon::control::message::ControlMessage::HubUpdateCheckCompleted { update },
        );
    });
    false
}

fn start_hub_update(
    daemon: &HubDaemon,
    scope: DaemonHubUpdateScope,
    reply_tx: ControlReplySender,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    // A bounded read of the installation receipt, as CheckHubUpdate does; no
    // source validation runs here. The handoff child validates the root.
    if let Some(refusal) = source_update_refusal() {
        return send_control_response(
            reply_tx,
            Ok(hub_update_execution_error(
                "hub_update_unavailable",
                "start_hub_update",
                &refusal.to_string(),
            )),
            response_delivery_rx,
        );
    }
    let (data_directory, source_root) = match daemon.runtime() {
        Some(runtime) => (
            runtime.config().data_directory.clone(),
            runtime.config().update_source_root.clone(),
        ),
        None => {
            return send_control_response(
                reply_tx,
                Ok(hub_update_execution_error(
                    "hub_update_runtime_unavailable",
                    "start_hub_update",
                    "the Hub runtime is not available",
                )),
                response_delivery_rx,
            );
        }
    };
    match start_update_handoff(&data_directory, scope, source_root.as_deref()) {
        Ok((execution, handoff)) => {
            let update_id = execution.update_id.clone();
            let response_received = reply_tx
                .send(Ok(daemon_hub_update_execution(execution)))
                .is_ok();
            wait_for_response_delivery(response_received, response_received, response_delivery_rx);
            if response_received {
                if let Err(error) = handoff.release() {
                    let _ = mark_update_failed(&data_directory, &update_id, &error);
                }
            } else {
                handoff.stop();
                let _ = mark_update_failed(
                    &data_directory,
                    &update_id,
                    "client disconnected before update handoff",
                );
            }
            false
        }
        Err(error) => send_control_response(
            reply_tx,
            Ok(hub_update_execution_error(
                if error.contains("already active") {
                    "hub_update_busy"
                } else {
                    "hub_update_start_failed"
                },
                "start_hub_update",
                &error,
            )),
            response_delivery_rx,
        ),
    }
}

fn get_hub_update_execution(
    daemon: &HubDaemon,
    reply_tx: ControlReplySender,
    response_delivery_rx: Option<mpsc::Receiver<()>>,
) -> bool {
    let response = match daemon.runtime() {
        Some(runtime) => match current_update_execution(&runtime.config().data_directory) {
            Ok(Some(execution)) => daemon_hub_update_execution(execution),
            Ok(None) => hub_update_execution_error(
                "hub_update_execution_not_found",
                "get_hub_update_execution",
                "no Hub update execution record exists",
            ),
            Err(error) => hub_update_execution_error(
                "hub_update_execution_read_failed",
                "get_hub_update_execution",
                &error,
            ),
        },
        None => hub_update_execution_error(
            "hub_update_runtime_unavailable",
            "get_hub_update_execution",
            "the Hub runtime is not available",
        ),
    };
    send_control_response(reply_tx, Ok(response), response_delivery_rx)
}

/// The in-flight fetch finished: clear its record, and answer its caller
/// unless shutdown already did.
pub(crate) fn hub_update_check_completed(
    state: &mut DaemonControlState,
    update: DaemonHubUpdate,
) -> bool {
    state
        .hub_update_check
        .take()
        .and_then(|check| check.reply)
        .is_some_and(|reply_tx| {
            send_control_response(reply_tx, Ok(daemon_hub_update(update)), None)
        })
}

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    request: DaemonRequest,
) -> ControlStep {
    match request {
        DaemonRequest::DaemonShutdown => super::status::handle(daemon, state, observability, true),
        _ => unreachable!("host runtime family received a non-host request"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::control::message::{ControlMessage, control_reply_channel};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn update(marker: &str) -> DaemonHubUpdate {
        DaemonHubUpdate {
            state: DaemonHubUpdateState::Current,
            current_version: marker.to_string(),
            available_version: None,
            build_revision: None,
            reason: None,
            action: None,
        }
    }

    fn reply_of(rx: &mut crate::daemon::control::message::ControlReplyReceiver) -> DaemonHubUpdate {
        rx.try_recv()
            .expect("a reply was sent")
            .expect("update response")
            .hub_update
            .expect("update payload")
    }

    /// A fetch the test releases, returning `marker`.
    fn held_fetch(marker: &'static str) -> (mpsc::Sender<()>, impl FnOnce() -> DaemonHubUpdate) {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        (release_tx, move || {
            release_rx.recv().expect("the test releases the fetch");
            update(marker)
        })
    }

    /// Shutdown answers check A early and the shutdown then fails. A retry
    /// while A's fetch runs is busy and leaves A's record alone; A's late
    /// completion clears the record and answers nobody; a later check runs
    /// normally and receives its own result.
    #[test]
    fn a_retry_after_an_early_shutdown_reply_is_busy_until_the_fetch_completes() {
        let transport = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("transport runtime");
        let (control_tx, mut control_rx) =
            tokio::sync::mpsc::channel(crate::admission::budgets::DAEMON_CONTROL_QUEUE_CAPACITY);
        let mut state = DaemonControlState::default();

        // A starts its fetch.
        let (release_a, fetch_a) = held_fetch("a");
        let (a_tx, mut a_rx) = control_reply_channel();
        assert!(!start_managed_check(
            &mut state,
            transport.handle(),
            control_tx.clone(),
            a_tx,
            None,
            fetch_a,
        ));

        // Shutdown starts and answers A.
        crate::daemon::control::request::finish_shutdown_update_reply(&mut state);
        assert_eq!(
            reply_of(&mut a_rx).reason.as_deref(),
            Some("daemon_shutdown")
        );
        assert!(
            state
                .hub_update_check
                .as_ref()
                .is_some_and(|check| check.reply.is_none()),
            "A's fetch still runs"
        );

        // The shutdown failed; the client retries while A's fetch runs.
        let b_fetched = Arc::new(AtomicBool::new(false));
        let b_flag = Arc::clone(&b_fetched);
        let (b_tx, mut b_rx) = control_reply_channel();
        start_managed_check(
            &mut state,
            transport.handle(),
            control_tx.clone(),
            b_tx,
            None,
            move || {
                b_flag.store(true, Ordering::SeqCst);
                update("b")
            },
        );
        // Busy admission.
        let busy = reply_of(&mut b_rx);
        assert_eq!(busy.reason.as_deref(), Some("busy"));
        assert_eq!(busy.action.as_deref(), Some("retry"));
        // The rejected retry leaves A's record unchanged and starts no fetch.
        assert!(
            state
                .hub_update_check
                .as_ref()
                .is_some_and(|check| check.reply.is_none()),
            "a rejected retry must not replace A's record"
        );

        // A's fetch completes late: completion matching.
        release_a.send(()).expect("release A");
        let ControlMessage::HubUpdateCheckCompleted { update: a_result } =
            control_rx.blocking_recv().expect("A's completion")
        else {
            panic!("expected A's completion");
        };
        assert_eq!(a_result.current_version, "a");
        // A's record holds no reply sender (asserted above), so its late
        // completion has nothing to send with; the returned bool means "stop
        // the daemon", not "sent".
        assert!(!hub_update_check_completed(&mut state, a_result));
        assert!(
            state.hub_update_check.is_none(),
            "A's completion clears its record"
        );
        assert!(
            !b_fetched.load(Ordering::SeqCst),
            "the busy retry never fetched"
        );

        // A later check starts normally and receives its own result.
        let (release_c, fetch_c) = held_fetch("c");
        let (c_tx, mut c_rx) = control_reply_channel();
        assert!(!start_managed_check(
            &mut state,
            transport.handle(),
            control_tx,
            c_tx,
            None,
            fetch_c,
        ));
        release_c.send(()).expect("release C");
        let ControlMessage::HubUpdateCheckCompleted { update: c_result } =
            control_rx.blocking_recv().expect("C's completion")
        else {
            panic!("expected C's completion");
        };
        assert!(!hub_update_check_completed(&mut state, c_result));
        assert_eq!(reply_of(&mut c_rx).current_version, "c");
        assert!(state.hub_update_check.is_none());
    }
}
