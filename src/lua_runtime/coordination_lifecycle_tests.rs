use super::*;
use crate::daemon::owner_loop::{DaemonControlState, drive_ready_test_turn};
use crate::host_executor::HOST_OPERATION_CAPACITY;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn fixture(name: &str) -> (crate::HubDaemon, DaemonControlState, std::path::PathBuf) {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::path::PathBuf::from("/private/tmp")
        .join(format!("c1-{name}-{}-{stamp}", std::process::id()));
    let config = crate::HubStartupOptions {
        host: crate::HostIdentityOptions {
            id: "c1-lifecycle-test".into(),
            display_name: "C1 Lifecycle Test".into(),
            fingerprint: None,
        },
        data_directory: crate::DataDirectoryOption::Explicit(root.clone()),
        ..crate::HubStartupOptions::default()
    }
    .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
    .unwrap();
    (
        crate::HubDaemon::start(config).unwrap(),
        DaemonControlState::default(),
        root,
    )
}

fn request(
    bridge: HubCoordinationBridge,
) -> thread::JoinHandle<Result<HubCoordinationResponse, String>> {
    thread::spawn(move || {
        bridge.request(PendingCoordinationOperation::Drain {
            target: EnvelopeTarget::Topic {
                topic: "capacity-wake".into(),
            },
            after: None,
            limit: 1,
        })
    })
}

struct OperationDrop(mpsc::Sender<String>);

impl Drop for OperationDrop {
    fn drop(&mut self) {
        let _ = self
            .0
            .send(thread::current().name().unwrap_or("unnamed").into());
    }
}

fn tracked_request(
    bridge: HubCoordinationBridge,
) -> (
    thread::JoinHandle<Result<HubCoordinationResponse, String>>,
    mpsc::Receiver<String>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let (sender, receiver) = mpsc::channel();
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let recorded = executions.clone();
    let caller = thread::spawn(move || {
        bridge.request(PendingCoordinationOperation::Tracked {
            operation: Box::new(PendingCoordinationOperation::Drain {
                target: EnvelopeTarget::Topic {
                    topic: "capacity-wake".into(),
                },
                after: None,
                limit: 1,
            }),
            probe: Box::new(OperationDrop(sender)),
            executions: recorded,
        })
    });
    (caller, receiver, executions)
}

fn assert_operation_drop(receiver: mpsc::Receiver<String>, prefix: &str) {
    let worker = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        worker.starts_with(prefix),
        "unexpected operation destructor thread: {worker}"
    );
    assert!(
        matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected)),
        "the operation must drop exactly once"
    );
}

fn drive_until(
    daemon: &mut crate::HubDaemon,
    state: &mut DaemonControlState,
    deadline: Instant,
    mut ready: impl FnMut(&DaemonControlState) -> bool,
) {
    while !ready(state) {
        assert!(
            Instant::now() < deadline,
            "fixture deadline: the expected lifecycle transition did not finish"
        );
        assert!(!drive_ready_test_turn(daemon, state));
        thread::yield_now();
    }
}

fn assert_retired(
    daemon: &crate::HubDaemon,
    state: &DaemonControlState,
    bridge: &HubCoordinationBridge,
) {
    let waiters = bridge.test_admitted_waiters();
    assert_eq!(waiters.len(), 1, "the consumer must not replay the request");
    let retains = daemon.runtime().unwrap().test_core_waiter_probe();
    assert!(
        !retains(waiters[0]),
        "final retirement must remove Core history"
    );
    assert!(state.pending_requests.is_empty());
    assert!(state.coordination_capacity_waiters.is_empty());
    assert_eq!(state.budget.outstanding(), 0);
    assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
    assert!(state.coordination_fault.is_none());
}

#[test]
fn coordination_owner_capacity_wake_resumes_retained_ingress() {
    let (mut daemon, mut state, root) = fixture("owner-capacity");
    let bridge = daemon.runtime().unwrap().coordination_bridge();
    let mut permits = Vec::new();
    while let Some(permit) = state.budget.reserve() {
        permits.push(permit);
    }
    let deadline = Instant::now() + Duration::from_millis(500);
    let caller = request(bridge.clone());
    drive_until(&mut daemon, &mut state, deadline, |state| {
        state.coordination_waiting_for_owner
    });
    assert_eq!(bridge.test_pending_count(), 1);
    assert!(bridge.test_admitted_waiters().is_empty());
    assert!(state.pending_requests.is_empty());
    state.budget.release(permits.pop().unwrap());
    drive_until(&mut daemon, &mut state, deadline, |state| {
        !bridge.test_admitted_waiters().is_empty() && state.pending_requests.is_empty()
    });
    assert!(matches!(
        caller.join().unwrap(),
        Ok(HubCoordinationResponse::Drain(_))
    ));
    for permit in permits {
        state.budget.release(permit);
    }
    assert_retired(&daemon, &state, &bridge);
    daemon.stop();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn coordination_host_capacity_wake_resumes_retained_result() {
    let (mut daemon, mut state, root) = fixture("host-capacity");
    let bridge = daemon.runtime().unwrap().coordination_bridge();
    let mut permits = (0..HOST_OPERATION_CAPACITY)
        .map(|_| {
            daemon
                .runtime()
                .unwrap()
                .host_executor()
                .try_reserve()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_millis(500);
    let caller = request(bridge.clone());
    drive_until(&mut daemon, &mut state, deadline, |state| {
        !state.coordination_capacity_waiters.is_empty()
    });
    let waiters = bridge.test_admitted_waiters();
    assert_eq!(waiters.len(), 1);
    assert!(state.coordination_capacity_waiters.contains(&waiters[0]));
    assert_eq!(state.pending_requests.len(), 1);
    assert!(daemon.runtime().unwrap().test_core_waiter_probe()(
        waiters[0]
    ));
    assert_eq!(
        daemon.runtime().unwrap().host_executor().outstanding(),
        HOST_OPERATION_CAPACITY
    );
    drop(permits.pop());
    drive_until(&mut daemon, &mut state, deadline, |state| {
        state.pending_requests.is_empty()
    });
    assert!(matches!(
        caller.join().unwrap(),
        Ok(HubCoordinationResponse::Drain(_))
    ));
    drop(permits);
    assert_retired(&daemon, &state, &bridge);
    daemon.stop();
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn coordination_stopped_core_refuses_and_retires_without_replay() {
    let (mut daemon, mut state, root) = fixture("core-stopped");
    daemon.runtime_mut().unwrap().test_stop_core_driver();
    let bridge = daemon.runtime().unwrap().coordination_bridge();
    let deadline = Instant::now() + Duration::from_millis(500);
    let (caller, disposed, executions) = tracked_request(bridge.clone());
    while bridge.test_pending_count() != 1 {
        assert!(
            Instant::now() < deadline,
            "the caller must queue its request"
        );
        thread::yield_now();
    }
    crate::daemon::control::coordination::accept_one(&mut daemon, &mut state);
    let waiter = *state.pending_requests.keys().next().unwrap();
    assert!(bridge.test_admitted_waiters().is_empty());
    drive_until(&mut daemon, &mut state, deadline, |state| {
        state.pending_requests.is_empty()
    });
    let result = caller.join().unwrap();
    assert!(
        matches!(result, Err(ref message) if message == "coordination Core driver stopped before admission")
    );
    assert!(!daemon.runtime().unwrap().test_core_waiter_probe()(waiter));
    assert!(bridge.test_admitted_waiters().is_empty());
    assert_eq!(bridge.test_pending_count(), 0);
    assert!(state.coordination_capacity_waiters.is_empty());
    assert_eq!(state.budget.outstanding(), 0);
    assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
    assert_eq!(executions.load(Ordering::Acquire), 0);
    assert_operation_drop(disposed, "botster-hub-host");
    daemon.stop();
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn coordination_poison_fault_retains_ingress_until_host_disposal() {
    let (mut daemon, mut state, root) = fixture("queue-poison");
    let bridge = daemon.runtime().unwrap().coordination_bridge();
    let response = bridge.test_queue_pending(PendingCoordinationOperation::Drain {
        target: EnvelopeTarget::Topic {
            topic: "poisoned".into(),
        },
        after: None,
        limit: 1,
    });
    let queue = bridge.pending.clone();
    let (gate, probe) = super::terminal_bridge_tests::pending_drop_gate(move || {
        !matches!(queue.try_lock(), Err(std::sync::TryLockError::WouldBlock))
    });
    bridge.test_set_pending_drop_probe(probe);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _lock = bridge.pending.lock().unwrap();
            panic!("inject coordination queue poison");
        }))
        .is_err()
    );
    crate::daemon::control::coordination::accept_one(&mut daemon, &mut state);
    assert_eq!(
        state.coordination_fault,
        Some(crate::daemon::control::coordination::CoordinationFault::QueuePoisoned)
    );
    assert_eq!(bridge.test_pending_count(), 1);
    assert!(state.pending_requests.is_empty());
    assert!(bridge.test_admitted_waiters().is_empty());
    assert_eq!(state.budget.outstanding(), 0);
    let worker_bridge = bridge.clone();
    let (executor, mut job, finished) =
        super::terminal_bridge_tests::host_clear(move || worker_bridge.dispose_terminal_pending());
    gate.wait();
    assert_eq!(bridge.test_pending_count(), 0);
    assert!(matches!(
        response.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert!(matches!(job.poll(), crate::host_disposal::Poll::Pending));
    gate.release();
    super::terminal_bridge_tests::finish_host_clear(&executor, &mut job);
    assert!(finished.recv_timeout(Duration::from_secs(5)).unwrap());
    assert!(matches!(
        response.recv_timeout(Duration::from_secs(5)),
        Err(mpsc::RecvTimeoutError::Disconnected)
    ));
    assert!(bridge.test_admitted_waiters().is_empty());
    assert!(bridge.progress.sealed.load(Ordering::Acquire));
    assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
    daemon.stop();
    std::fs::remove_dir_all(root).unwrap();
}
struct CoreGate(Option<mpsc::Sender<()>>);

impl CoreGate {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for CoreGate {
    fn drop(&mut self) {
        self.release();
    }
}

fn hold_core(daemon: &crate::HubDaemon) -> CoreGate {
    let (release, receiver) = mpsc::channel();
    let (entered, waiting) = mpsc::channel();
    drop(daemon.runtime().unwrap().submit_core(move |_| {
        entered.send(()).unwrap();
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    }));
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    CoreGate(Some(release))
}

#[test]
fn coordination_full_core_refuses_and_retires_without_replay() {
    let (mut daemon, mut state, root) = fixture("core-full");
    let mut gate = hold_core(&daemon);
    let queued = (0..crate::data_plane::driver::CORE_REQUEST_CAPACITY)
        .map(|_| daemon.runtime().unwrap().submit_core(|_| ()))
        .collect::<Vec<_>>();
    let bridge = daemon.runtime().unwrap().coordination_bridge();
    let deadline = Instant::now() + Duration::from_millis(500);
    let (caller, disposed, executions) = tracked_request(bridge.clone());
    while bridge.test_pending_count() != 1 {
        assert!(
            Instant::now() < deadline,
            "the caller must queue its request"
        );
        thread::yield_now();
    }
    crate::daemon::control::coordination::accept_one(&mut daemon, &mut state);
    let waiter = *state.pending_requests.keys().next().unwrap();
    assert!(bridge.test_admitted_waiters().is_empty());
    drive_until(&mut daemon, &mut state, deadline, |state| {
        state.pending_requests.is_empty()
    });
    assert!(
        matches!(caller.join().unwrap(), Err(ref message) if message == "coordination Core queue is full")
    );
    assert!(!daemon.runtime().unwrap().test_core_waiter_probe()(waiter));
    assert_eq!(bridge.test_pending_count(), 0);
    assert_eq!(state.budget.outstanding(), 0);
    assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
    gate.release();
    for ticket in queued {
        assert!(ticket.wait(Duration::from_secs(5)).is_ok());
    }
    assert!(!drive_ready_test_turn(&mut daemon, &mut state));
    assert!(bridge.test_admitted_waiters().is_empty());
    assert_eq!(executions.load(Ordering::Acquire), 0);
    assert_operation_drop(disposed, "botster-hub-host");
    daemon.stop();
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn coordination_queued_abandonment_disposes_without_core_execution() {
    abandonment(false);
}

#[test]
fn coordination_admitted_abandonment_keeps_one_core_effect() {
    abandonment(true);
}

fn abandonment(admitted: bool) {
    let (mut daemon, mut state, root) = fixture(if admitted {
        "admitted-abandonment"
    } else {
        "queued-abandonment"
    });
    let target = EnvelopeTarget::Topic {
        topic: "capacity-wake".into(),
    };
    for index in 0..2 {
        daemon
            .runtime()
            .unwrap()
            .publish_routed_envelope(RoutedEnvelope::new(
                botster_core::EnvelopeId(format!("abandon-{index}")),
                botster_core::EndpointId("hub:c1-fixture".into()),
                vec![target.clone()],
                botster_core::RoutedEnvelopePayload {
                    content_type: "text/plain".into(),
                    body: vec![index],
                    extension: None,
                },
                41,
            ))
            .wait(Duration::from_secs(5))
            .unwrap()
            .unwrap();
    }
    let bridge = daemon.runtime().unwrap().coordination_bridge();
    let mut gate = hold_core(&daemon);
    let (caller, disposed, executions) = tracked_request(bridge.clone());
    let setup = Instant::now() + Duration::from_millis(500);
    while bridge.test_pending_count() != 1 {
        assert!(Instant::now() < setup, "the caller must queue its request");
        thread::yield_now();
    }
    let caller_state = bridge
        .pending
        .lock()
        .unwrap()
        .front()
        .unwrap()
        .caller
        .clone();
    if admitted {
        crate::daemon::control::coordination::accept_one(&mut daemon, &mut state);
        assert_eq!(bridge.test_admitted_waiters().len(), 1);
    }
    assert!(
        matches!(caller.join().unwrap(), Err(ref message) if message == "coordination request did not complete before timeout")
    );
    assert_eq!(
        caller_state.0.load(Ordering::Acquire),
        if admitted { 3 } else { 2 }
    );
    if !admitted {
        crate::daemon::control::coordination::accept_one(&mut daemon, &mut state);
    }
    let waiter = *state.pending_requests.keys().next().unwrap();
    gate.release();
    drive_until(
        &mut daemon,
        &mut state,
        Instant::now() + Duration::from_secs(5),
        |state| state.pending_requests.is_empty(),
    );
    assert_eq!(executions.load(Ordering::Acquire), usize::from(admitted));
    assert_operation_drop(
        disposed,
        if admitted {
            "botster-hub-data-plane"
        } else {
            "botster-hub-host"
        },
    );
    assert_eq!(bridge.test_admitted_waiters().len(), usize::from(admitted));
    assert!(!daemon.runtime().unwrap().test_core_waiter_probe()(waiter));
    for index in 0..2 {
        let delivery = daemon
            .runtime()
            .unwrap()
            .routed_envelope_delivery_state(
                &target,
                &botster_core::EnvelopeId(format!("abandon-{index}")),
            )
            .wait(Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            delivery.state.unwrap().status,
            if admitted && index == 0 {
                botster_core::EnvelopeDeliveryStatus::Delivered
            } else {
                botster_core::EnvelopeDeliveryStatus::Queued
            }
        );
    }
    let remaining = daemon
        .runtime()
        .unwrap()
        .drain_routed_envelopes(target, None, 3)
        .wait(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(remaining.envelopes.len(), if admitted { 1 } else { 2 });
    assert!(state.coordination_capacity_waiters.is_empty());
    assert_eq!(state.budget.outstanding(), 0);
    assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
    assert_eq!(bridge.test_pending_count(), 0);
    daemon.stop();
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn coordination_scheduling_failure_keeps_shared_core_completions_available() {
    use crate::daemon::control::coordination::CoordinationFault;
    use crate::daemon::control::pending::{absorb_core_completions, dispose_terminal_requests};
    use crate::daemon::owner_loop::publish_completion_wakes;
    use crate::daemon::owner_schedule::ReadyQueues;
    use crate::daemon::owner_turn::OwnerTurnBudget;
    use crate::data_plane::driver::CoreTicketPoll;

    let (mut daemon, mut state, root) = fixture("scheduler-failure");
    let bridge = daemon.runtime().unwrap().coordination_bridge();
    let mut gate = hold_core(&daemon);
    let response = bridge.test_queue_pending(PendingCoordinationOperation::Drain {
        target: EnvelopeTarget::Topic {
            topic: "scheduler-failure".into(),
        },
        after: None,
        limit: 1,
    });
    let (disposed, disposal) = mpsc::channel();
    bridge.test_set_pending_drop_probe(OperationDrop(disposed));
    state.owner_ready = ReadyQueues::with_next_enqueue_serial(u64::MAX);
    crate::daemon::control::coordination::accept_one(&mut daemon, &mut state);
    let waiter = *state.pending_requests.keys().next().unwrap();
    assert_eq!(
        state.coordination_fault,
        Some(CoordinationFault::SchedulerExhausted)
    );
    let sibling = state.waiter_ids.next().unwrap();
    let retirement = daemon.runtime().unwrap().coordination_retirement(sibling);
    let mut ticket = daemon
        .runtime()
        .unwrap()
        .submit_core_for_owner(sibling, |_| 73);
    gate.release();
    daemon
        .runtime()
        .unwrap()
        .submit_core(|_| ())
        .wait(Duration::from_secs(5))
        .unwrap();
    publish_completion_wakes(&daemon, &mut state);
    assert!(
        !daemon
            .runtime()
            .unwrap()
            .take_core_completion_notification(),
        "a C1 fault must not suppress the shared notification collector"
    );
    let identities = daemon.runtime().unwrap().take_owner_core_completions(2);
    assert_eq!(identities.len(), 2);
    let mut budget = OwnerTurnBudget::new(Instant::now());
    assert_eq!(
        absorb_core_completions(&mut state, &identities, &mut budget),
        2
    );
    assert_eq!(state.pending_requests[&waiter].last_core_phase, 1);
    assert!(state.pending_requests[&waiter].ready_key.is_none());
    assert_eq!(
        state.coordination_fault,
        Some(CoordinationFault::SchedulerExhausted)
    );
    assert!(
        matches!(ticket.poll(), CoreTicketPoll::Ready(73)),
        "the unrelated registered result must remain collectable"
    );
    drop(retirement);
    assert!(!daemon.runtime().unwrap().test_core_waiter_probe()(sibling));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !state.pending_requests.is_empty() {
        assert!(
            Instant::now() < deadline,
            "terminal disposal must retire the faulted row"
        );
        dispose_terminal_requests(daemon.runtime().unwrap(), &mut state);
        thread::yield_now();
    }
    assert_operation_drop(disposal, "botster-hub-host");
    assert!(matches!(
        response.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
    assert!(!daemon.runtime().unwrap().test_core_waiter_probe()(waiter));
    assert_eq!(bridge.test_admitted_waiters(), vec![waiter]);
    assert_eq!(state.budget.outstanding(), 0);
    assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
    daemon.stop();
    std::fs::remove_dir_all(root).unwrap();
}
