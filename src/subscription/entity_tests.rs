use super::*;
use std::fs;
use std::sync::mpsc;

use botster_core::{RequestId, SessionId};
use botster_core_daemon::{RegistrySessionState, SessionLifecycleBaseline, SessionLifecycleCursor};
use botster_hub_client::{
    DaemonEntityFrame, DaemonLifecycleCounters, DaemonResponseKind, DaemonSessionEntity,
};
use serde_json::Value;

use crate::HubDaemon;
use crate::daemon::owner_loop::DaemonControlState;
use crate::owner_identity::WaiterIdSource;

fn retirement_subscription(
    sender: mpsc::SyncSender<DaemonEntityFrame>,
    entity_type: &str,
) -> EntitySubscriptionState {
    EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: entity_type.to_string(),
        cursor: None,
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Removes,
        next_seq: 0,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: false,
    }
}

fn queued_retirement_frame(subscription_id: &str) -> DaemonEntityFrame {
    DaemonEntityFrame::Error {
        subscription_id: subscription_id.to_string(),
        entity_type: "queued.family".to_string(),
        code: "queued".to_string(),
        message: "queued frame".to_string(),
    }
}

fn retirement_daemon(label: &str) -> (HubDaemon, std::path::PathBuf) {
    let data_directory = std::env::temp_dir().join(format!(
        "botster-hub-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    let config = crate::HubStartupOptions {
        host: crate::HostIdentityOptions {
            id: label.to_string(),
            display_name: "Entity Retirement Test".to_string(),
            fingerprint: None,
        },
        data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
        transports: crate::TransportBindings::default(),
        ..crate::HubStartupOptions::default()
    }
    .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
    .expect("build retirement config");
    (
        HubDaemon::start(config).expect("start retirement daemon"),
        data_directory,
    )
}

#[test]
fn provider_retirement_waits_for_one_dequeue_without_other_owner_work() {
    let (mut daemon, data_directory) = retirement_daemon("retire-after-dequeue");
    let mut state = DaemonControlState::default();
    for kind in crate::daemon_maintenance::MaintenanceSliceKind::ALL {
        assert!(state.maintenance.wakes.take(kind));
    }
    assert!(!state.maintenance.wakes.has_any());
    let (sender, receiver) = mpsc::sync_channel(1);
    sender
        .send(queued_retirement_frame("retiring"))
        .expect("fill subscriber queue");
    state.entity_subscriptions.insert(
        "retiring".to_string(),
        retirement_subscription(sender, "retiring.family"),
    );
    state.lifecycle_counters.live_entity_subscriptions = 1;

    retire_unloaded_entity_subscriptions(&mut state, |_| false);
    assert!(state.entity_subscriptions["retiring"].terminating);
    assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 1);
    assert_eq!(state.released_entity_generations, 0);

    receiver.recv().expect("drain exactly one queued frame");
    let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(1);
    state.entity_capacity_wake.bind(control_tx);
    assert!(!state.maintenance.wakes.has_any());
    state.entity_capacity_wake.publish();
    assert!(matches!(
        control_rx.try_recv(),
        Ok(ControlMessage::EntitySubscriptionCapacityReleased)
    ));
    crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
    assert!(!state.entity_subscriptions.contains_key("retiring"));
    assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
    assert_eq!(state.released_entity_generations, 1);
    assert!(matches!(
        receiver.recv().expect("terminal frame after capacity"),
        DaemonEntityFrame::Error { code, .. } if code == "entity_provider_unloaded"
    ));
    daemon.stop();
    drop(daemon);
    fs::remove_dir_all(data_directory).expect("remove retirement data directory");
}

#[test]
fn full_control_queue_keeps_entity_capacity_flag_until_owner_turn() {
    let (mut daemon, data_directory) = retirement_daemon("retire-full-control");
    let mut state = DaemonControlState::default();
    for kind in crate::daemon_maintenance::MaintenanceSliceKind::ALL {
        assert!(state.maintenance.wakes.take(kind));
    }
    assert!(!state.maintenance.wakes.has_any());
    let (sender, receiver) = mpsc::sync_channel(1);
    sender
        .send(queued_retirement_frame("retiring"))
        .expect("fill subscriber queue");
    state.entity_subscriptions.insert(
        "retiring".to_string(),
        retirement_subscription(sender, "retiring.family"),
    );
    state.lifecycle_counters.live_entity_subscriptions = 1;
    retire_unloaded_entity_subscriptions(&mut state, |_| false);
    receiver.recv().expect("release subscriber queue capacity");

    let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(1);
    state.entity_capacity_wake.bind(control_tx.clone());
    control_tx
        .try_send(ControlMessage::DataPlaneProgress)
        .expect("fill control queue");
    assert!(!state.maintenance.wakes.has_any());
    state.entity_capacity_wake.publish();
    assert!(matches!(
        control_rx.try_recv(),
        Ok(ControlMessage::DataPlaneProgress)
    ));
    assert!(
        control_rx.try_recv().is_err(),
        "capacity notice was dropped"
    );
    crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
    assert!(!state.entity_subscriptions.contains_key("retiring"));
    assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
    assert!(matches!(
        receiver.recv().expect("terminal frame after dropped notice"),
        DaemonEntityFrame::Error { code, .. } if code == "entity_provider_unloaded"
    ));
    daemon.stop();
    drop(daemon);
    fs::remove_dir_all(data_directory).expect("remove retirement data directory");
}

#[test]
fn provider_reload_does_not_revive_a_terminating_subscription() {
    let mut state = DaemonControlState::default();
    let (sender, receiver) = mpsc::sync_channel(1);
    sender
        .send(queued_retirement_frame("retiring"))
        .expect("fill subscriber queue");
    state.entity_subscriptions.insert(
        "retiring".to_string(),
        retirement_subscription(sender, "retiring.family"),
    );
    state.lifecycle_counters.live_entity_subscriptions = 1;
    retire_unloaded_entity_subscriptions(&mut state, |_| false);
    assert!(state.entity_subscriptions["retiring"].terminating);
    receiver.recv().expect("release subscriber queue capacity");

    retire_unloaded_entity_subscriptions(&mut state, |_| true);
    assert!(!state.entity_subscriptions.contains_key("retiring"));
    assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
    assert_eq!(state.released_entity_generations, 1);
    assert!(matches!(
        receiver.recv().expect("terminal frame survives reload"),
        DaemonEntityFrame::Error { code, .. } if code == "entity_provider_unloaded"
    ));
}

#[test]
fn held_target_cannot_rearm_after_terminal_intent() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let mut state = DaemonControlState::default();
    let delivery = crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery;
    assert!(state.maintenance.wakes.take(delivery));
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let target = Arc::new(crate::plugin_entity::Target {
        subscription_id: "retiring".to_string(),
        entity_type: "retiring.family".to_string(),
        sender: EntityFrameSender::Async(sender.clone()),
    });
    install_package_entity_subscription(
        &mut state,
        crate::plugin_entity::Registration {
            subscription_id: "retiring".to_string(),
            target_key: "retiring".to_string(),
            entity_type: "retiring.family".to_string(),
            reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                "retiring".to_string(),
            ),
        },
        Arc::clone(&target),
        None,
    )
    .expect("install provider subscription");
    assert!(exact_package_entity_target_catching_up(&state, &target));
    let identity =
        crate::owner_identity::OwnerWorkIdentity::first(crate::owner_identity::WaiterId(904));
    let (live, _) = arm_package_entity_delivery(&mut state, &target, identity, 1, true, 1)
        .expect("arm held target");
    sender
        .try_send(crate::entity_delivery::EntityDelivery::Typed(
            queued_retirement_frame("retiring"),
        ))
        .expect("fill the subscriber queue");

    retire_unloaded_entity_subscriptions(&mut state, |_| false);
    assert!(state.entity_subscriptions["retiring"].terminating);
    assert!(!exact_package_entity_target_catching_up(&state, &target));
    assert!(!live.load(Ordering::Acquire));
    assert!(!state.plugin_entities.targets.contains_key("retiring"));
    assert!(arm_package_entity_delivery(&mut state, &target, identity, 1, true, 1).is_none());
    assert!(!state.maintenance.wakes.take(delivery));
    assert!(!complete_package_entity_delivery(
        &mut state,
        &target,
        identity,
        1,
        true,
        1,
        crate::plugin_entity::DeliveryStatus::Sent,
    ));
    assert_eq!(
        state.entity_subscriptions["retiring"].package_last_applied_seq,
        None
    );
    assert!(state.maintenance.wakes.take(delivery));
    receiver.try_recv().expect("drain queued frame");
    retire_unloaded_entity_subscriptions(&mut state, |_| true);
    assert!(!state.entity_subscriptions.contains_key("retiring"));
    assert!(matches!(
        receiver.try_recv().expect("terminal frame after held work"),
        crate::entity_delivery::EntityDelivery::Typed(DaemonEntityFrame::Error { code, .. })
            if code == "entity_provider_unloaded"
    ));
}

/// An in-sync subscriber takes a resync snapshot only when the snapshot
/// brings it to the family floor. A floor above the applied sequence means
/// admitted deltas are queued for it, not that it needs a snapshot.
#[test]
fn in_sync_subscriber_refuses_a_snapshot_below_the_family_floor() {
    use std::sync::Arc;

    let mut state = DaemonControlState::default();
    let (sender, _receiver) = tokio::sync::mpsc::channel(8);
    let target = Arc::new(crate::plugin_entity::Target {
        subscription_id: "gate".to_string(),
        entity_type: "gate.family".to_string(),
        sender: EntityFrameSender::Async(sender),
    });
    install_package_entity_subscription(
        &mut state,
        crate::plugin_entity::Registration {
            subscription_id: "gate".to_string(),
            target_key: "gate".to_string(),
            entity_type: "gate.family".to_string(),
            reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                "gate".to_string(),
            ),
        },
        Arc::clone(&target),
        None,
    )
    .expect("install provider subscription");
    let identity = |waiter| {
        crate::owner_identity::OwnerWorkIdentity::first(crate::owner_identity::WaiterId(waiter))
    };
    let deliver = |state: &mut DaemonControlState, waiter, sequence, snapshot, floor| {
        arm_package_entity_delivery(state, &target, identity(waiter), sequence, snapshot, floor)
            .unwrap_or_else(|| panic!("arm seq {sequence} snapshot {snapshot} floor {floor}"));
        // The return value reports whether the subscriber is still
        // catching up; the test asserts that state directly.
        let _ = complete_package_entity_delivery(
            state,
            &target,
            identity(waiter),
            sequence,
            snapshot,
            floor,
            crate::plugin_entity::DeliveryStatus::Sent,
        );
    };

    // The initial snapshot brings the subscriber in sync at 0.
    assert!(exact_package_entity_target_catching_up(&state, &target));
    deliver(&mut state, 1, 0, true, 0);
    assert!(!exact_package_entity_target_catching_up(&state, &target));

    // A gap fill raised the floor to 3, and a provider still behind answers
    // its resync with seq 0 (stale) or seq 1 (partly behind).
    for (waiter, stale) in [(2, 0), (3, 1)] {
        assert!(
            arm_package_entity_delivery(&mut state, &target, identity(waiter), stale, true, 3)
                .is_none(),
            "an in-sync subscriber must refuse snapshot {stale} below floor 3"
        );
    }
    assert!(!exact_package_entity_target_catching_up(&state, &target));

    // The queued deltas then reach it in order.
    for sequence in 1..=3 {
        deliver(&mut state, 10 + sequence, sequence, false, 3);
    }
    assert_eq!(
        state.entity_subscriptions["gate"].package_last_applied_seq,
        Some(3)
    );
    assert!(!exact_package_entity_target_catching_up(&state, &target));

    // A snapshot that reaches a raised floor is still taken.
    deliver(&mut state, 20, 5, true, 5);
    assert_eq!(
        state.entity_subscriptions["gate"].package_last_applied_seq,
        Some(5)
    );
    assert!(!exact_package_entity_target_catching_up(&state, &target));
}

#[test]
fn running_publication_waits_for_the_routed_host_receipt() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    for malformed in [false, true] {
        let mut state = DaemonControlState::default();
        let delivery = crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery;
        assert!(state.maintenance.wakes.take(delivery));
        let mut executor = crate::host_executor::HostExecutor::new();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let target = Arc::new(crate::plugin_entity::Target {
            subscription_id: "retiring".to_string(),
            entity_type: "retiring.family".to_string(),
            sender: EntityFrameSender::Async(sender),
        });
        install_package_entity_subscription(
            &mut state,
            crate::plugin_entity::Registration {
                subscription_id: "retiring".to_string(),
                target_key: "retiring".to_string(),
                entity_type: "retiring.family".to_string(),
                reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                    "retiring".to_string(),
                ),
            },
            Arc::clone(&target),
            None,
        )
        .expect("install provider subscription");
        let waiter = crate::owner_identity::WaiterId(if malformed { 906 } else { 905 });
        let expected = crate::owner_identity::OwnerWorkIdentity::first(waiter);
        let (live, _) = arm_package_entity_delivery(&mut state, &target, expected, 1, true, 1)
            .expect("arm running publication");
        let command = if malformed {
            crate::plugin_entity::Command::Discard {
                payload: None,
                registration: None,
                reservation_identity: None,
            }
        } else {
            crate::plugin_entity::Command::Deliver {
                payload: crate::plugin_entity::Payload::mutation(
                    crate::package_entity_fanout::PackageEntityMutation::Upsert {
                        admission: None,
                        entity_type: "retiring.family".to_string(),
                        snapshot_seq: 1,
                        id: "one".to_string(),
                        entity: serde_json::json!({"id": "one"}),
                    },
                ),
                target: Arc::clone(&target),
                publication_live: Arc::clone(&live),
                budget: crate::shared_view::SharedViewBudget::new(),
                resync_reason: None,
            }
        };
        let identity = state.plugin_entities.test_insert_delivery_work(
            waiter,
            Arc::clone(&target),
            Arc::clone(&live),
            &mut executor,
            command,
            false,
        );
        assert_eq!(identity, expected);
        assert!(state.plugin_entities.accepts_host_completion(identity));

        retire_unloaded_entity_subscriptions(&mut state, |_| false);
        assert!(state.entity_subscriptions["retiring"].terminating);
        assert!(!live.load(Ordering::Acquire));
        assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 1);
        while let Ok(frame) = receiver.try_recv() {
            assert!(
                !matches!(
                    frame,
                    crate::entity_delivery::EntityDelivery::Typed(DaemonEntityFrame::Error { .. })
                ),
                "terminal must wait for the Host receipt"
            );
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        let completion = loop {
            match executor.poll_completion() {
                crate::host_executor::HostCompletionPoll::Ready(completion) => {
                    break completion;
                }
                crate::host_executor::HostCompletionPoll::Empty => {
                    assert!(Instant::now() < deadline, "Host receipt must arrive");
                    std::thread::yield_now();
                }
                crate::host_executor::HostCompletionPoll::Stopped => {
                    panic!("Host executor stopped before its receipt")
                }
            }
        };
        assert_eq!(completion.identity, identity);
        if malformed {
            assert!(matches!(
                &*completion.result,
                crate::host_executor::HostResult::PluginEntity(
                    crate::plugin_entity::Completion::Reclaimed
                )
            ));
        }
        assert!(!state.maintenance.wakes.take(delivery));
        route_host_completion(&mut state, completion);
        assert!(state.maintenance.wakes.take(delivery));
        retire_unloaded_entity_subscriptions(&mut state, |_| false);
        assert!(!state.entity_subscriptions.contains_key("retiring"));
        assert_eq!(state.released_entity_generations, 1);
        let mut terminal = false;
        while let Ok(frame) = receiver.try_recv() {
            assert!(!terminal, "no frame may follow the terminal Error");
            if matches!(frame,
                crate::entity_delivery::EntityDelivery::Typed(
                    DaemonEntityFrame::Error { code, .. }
                ) if code == "entity_provider_unloaded"
            ) {
                terminal = true;
            }
        }
        assert!(terminal, "terminal follows the exact Host receipt");
    }
}

#[test]
fn rejected_delivery_does_not_hold_the_terminal_frame() {
    use std::sync::Arc;

    let mut state = DaemonControlState::default();
    let mut executor = crate::host_executor::HostExecutor::new();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let target = Arc::new(crate::plugin_entity::Target {
        subscription_id: "retiring".to_string(),
        entity_type: "retiring.family".to_string(),
        sender: EntityFrameSender::Async(sender),
    });
    install_package_entity_subscription(
        &mut state,
        crate::plugin_entity::Registration {
            subscription_id: "retiring".to_string(),
            target_key: "retiring".to_string(),
            entity_type: "retiring.family".to_string(),
            reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                "retiring".to_string(),
            ),
        },
        Arc::clone(&target),
        None,
    )
    .expect("install provider subscription");
    let waiter = crate::owner_identity::WaiterId(907);
    let expected = crate::owner_identity::OwnerWorkIdentity::first(waiter);
    let (live, _) = arm_package_entity_delivery(&mut state, &target, expected, 1, true, 1)
        .expect("arm publication");
    let identity = state.plugin_entities.test_insert_delivery_work(
        waiter,
        Arc::clone(&target),
        Arc::clone(&live),
        &mut executor,
        crate::plugin_entity::Command::Discard {
            payload: None,
            registration: None,
            reservation_identity: None,
        },
        true,
    );
    assert_eq!(identity, expected);
    assert!(!state.plugin_entities.accepts_host_completion(identity));

    retire_unloaded_entity_subscriptions(&mut state, |_| false);
    assert!(!state.entity_subscriptions.contains_key("retiring"));
    assert_eq!(state.released_entity_generations, 1);
    assert!(matches!(
        receiver.try_recv().expect("rejected submission has no Host receipt"),
        crate::entity_delivery::EntityDelivery::Typed(DaemonEntityFrame::Error { code, .. })
            if code == "entity_provider_unloaded"
    ));
}

#[test]
fn provider_retirement_keeps_a_shared_connection_sibling() {
    use crate::transport::unix::connection::{
        ConnectionCleanupGuard, ConnectionTerminalReason, handle_connection_cleanup,
    };
    let (mut daemon, data_directory) = retirement_daemon("retire-sibling");
    let mut state = DaemonControlState::default();
    let (retiring_tx, retiring_rx) = mpsc::sync_channel(1);
    let (sibling_tx, sibling_rx) = mpsc::sync_channel(1);
    let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(1);
    let cleanup_permit = control_tx
        .clone()
        .try_reserve_owned()
        .expect("cleanup permit");
    let mut guard = ConnectionCleanupGuard::new(
        cleanup_permit,
        "shared-client".to_string(),
        ConnectionTerminalReason::Eof,
    );
    guard.add_entity_subscription("retiring".to_string());
    guard.add_entity_subscription("sibling".to_string());
    state.entity_subscriptions.insert(
        "retiring".to_string(),
        retirement_subscription(retiring_tx, "retiring.family"),
    );
    state.entity_subscriptions.insert(
        "sibling".to_string(),
        retirement_subscription(sibling_tx, "live.family"),
    );
    state.lifecycle_counters.live_entity_subscriptions = 2;
    retire_unloaded_entity_subscriptions(&mut state, |family| family == "live.family");
    assert!(!state.entity_subscriptions.contains_key("retiring"));
    assert!(state.entity_subscriptions.contains_key("sibling"));
    assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 1);
    assert_eq!(state.released_entity_generations, 1);
    assert!(matches!(
        retiring_rx.recv().expect("retired sibling terminal frame"),
        DaemonEntityFrame::Error { code, .. } if code == "entity_provider_unloaded"
    ));
    state.entity_subscriptions["sibling"]
        .send_frame_for_test(queued_retirement_frame("sibling"))
        .expect("surviving sibling sender remains live");
    assert!(matches!(
        sibling_rx.recv().expect("surviving sibling delivery"),
        DaemonEntityFrame::Error { code, .. } if code == "queued"
    ));
    drop(guard);
    let ControlMessage::ConnectionCleanup(cleanup) = control_rx.try_recv().expect("cleanup") else {
        panic!("shared connection must publish ConnectionCleanup");
    };
    handle_connection_cleanup(&mut daemon, &mut state, control_tx, cleanup);
    assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
    assert_eq!(state.released_entity_generations, 2);
    daemon.stop();
    drop(daemon);
    fs::remove_dir_all(data_directory).expect("remove retirement data directory");
}

#[test]
fn confirmed_unix_disconnect_retires_terminating_subscription() {
    use crate::transport::unix::connection::{
        ConnectionCleanupGuard, ConnectionTerminalReason, handle_connection_cleanup,
    };
    let (mut daemon, data_directory) = retirement_daemon("retire-disconnect");
    let mut state = DaemonControlState::default();
    let (sender, receiver) = mpsc::sync_channel(1);
    sender
        .send(queued_retirement_frame("retiring"))
        .expect("fill subscriber queue");
    state.entity_subscriptions.insert(
        "retiring".to_string(),
        retirement_subscription(sender, "retiring.family"),
    );
    state.lifecycle_counters.live_entity_subscriptions = 1;
    retire_unloaded_entity_subscriptions(&mut state, |_| false);
    assert!(state.entity_subscriptions["retiring"].terminating);

    let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(1);
    let cleanup_permit = control_tx
        .clone()
        .try_reserve_owned()
        .expect("cleanup permit");
    let mut guard = ConnectionCleanupGuard::new(
        cleanup_permit,
        "retiring-client".to_string(),
        ConnectionTerminalReason::Eof,
    );
    guard.add_entity_subscription("retiring".to_string());
    drop(guard);
    let ControlMessage::ConnectionCleanup(cleanup) = control_rx.try_recv().expect("cleanup") else {
        panic!("disconnect must publish ConnectionCleanup");
    };
    handle_connection_cleanup(&mut daemon, &mut state, control_tx, cleanup);
    assert!(!state.entity_subscriptions.contains_key("retiring"));
    assert_eq!(state.lifecycle_counters.live_entity_subscriptions, 0);
    assert_eq!(state.released_entity_generations, 1);
    assert!(matches!(
        receiver.recv().expect("original queued frame remains"),
        DaemonEntityFrame::Error { code, .. } if code == "queued"
    ));
    daemon.stop();
    drop(daemon);
    fs::remove_dir_all(data_directory).expect("remove retirement data directory");
}

#[test]
fn terminal_catalog_keeps_its_own_identity_after_waiter_exhaustion() {
    let executor = HostExecutor::new();
    let mut state = DaemonControlState::default();
    let identity = state
        .session_type_catalog
        .last_identity
        .expect("construction assigns the catalog's identity");
    assert_eq!(
        state.waiter_ids.next().unwrap().0,
        identity.waiter_id.0 + 2,
        "the state allocates the lifecycle identity after the cache identity"
    );
    state.waiter_ids = WaiterIdSource::with_next(u64::MAX);
    assert!(state.waiter_ids.next().is_none());
    assert!(state.session_type_catalog.pending.is_none());
    state.session_type_catalog.failure = Some((
        1,
        HostError::new(
            "host_waiter_id_exhausted",
            "catalog could not admit a build",
        ),
    ));
    let mut slots: Vec<_> = (0..crate::host_executor::HOST_OPERATION_CAPACITY)
        .map(|_| executor.try_reserve().unwrap())
        .collect();
    assert!(!state.session_type_catalog.dispose_terminal(&executor));
    assert!(state.session_type_catalog.failure.is_some());
    assert_eq!(state.session_type_catalog.last_identity, Some(identity));
    drop(slots.pop());
    assert!(!state.session_type_catalog.dispose_terminal(&executor));
    assert_eq!(state.session_type_catalog.last_identity, Some(identity));
    assert!(state.waiter_ids.next().is_none());
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    while !state.session_type_catalog.dispose_terminal(&executor) {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(executor.outstanding(), slots.len());
    assert!(state.session_type_catalog.failure.is_none());
    drop(slots);
    assert_eq!(executor.outstanding(), 0);
    assert_eq!(executor.prepared_bytes(), 0);
}

#[test]
fn terminal_catalog_cache_waits_for_capacity_in_the_original_host_pool() {
    let executor = HostExecutor::new();
    let identity = HostJobIdentity::first(crate::owner_identity::WaiterId(73));
    let (result, charge) = HostCompletion::for_test(
        identity,
        HostResult::SessionTypeCatalogReady {
            generation: 1,
            entities: BTreeMap::from([("type".into(), serde_json::json!({"id": "retained"}))]),
            logical_bytes: 20,
        },
        executor.try_reserve().unwrap(),
    )
    .release();
    let HostResult::SessionTypeCatalogReady {
        entities,
        logical_bytes,
        ..
    } = result
    else {
        unreachable!()
    };
    let mut cache = SessionTypeCatalogCache {
        last_identity: Some(identity),
        entities,
        logical_bytes,
        prepared_charge: Some(charge),
        ..Default::default()
    };
    let mut slots = Vec::new();
    while let Some(permit) = executor.try_reserve() {
        slots.push(permit);
    }
    assert!(!slots.is_empty());
    // The settled cache charge also consumes the existing prepared-byte budget.
    assert!(slots.len() < crate::host_executor::HOST_OPERATION_CAPACITY);
    let bytes = executor.prepared_bytes();
    assert!(!cache.dispose_terminal(&executor));
    assert_eq!(cache.entities.len(), 1);
    assert!(cache.prepared_charge.is_some());
    assert_eq!(executor.prepared_bytes(), bytes);
    drop(slots.pop());
    assert!(!cache.dispose_terminal(&executor));
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    while !cache.terminal.as_ref().unwrap().test_disposed() {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(executor.outstanding(), slots.len() + 1);
    assert!(cache.dispose_terminal(&executor));
    assert_eq!(executor.outstanding(), slots.len());
    drop(slots);
    assert_eq!(executor.prepared_bytes(), 0);
}

#[test]
fn terminal_catalog_without_an_existing_identity_retains_its_failure() {
    let executor = HostExecutor::new();
    let mut cache = SessionTypeCatalogCache {
        failure: Some((
            1,
            HostError::new(
                "host_waiter_id_exhausted",
                "catalog has no admitted identity",
            ),
        )),
        ..Default::default()
    };
    assert!(!cache.dispose_terminal(&executor));
    assert!(cache.failure.is_some());
    assert_eq!(executor.outstanding(), 0);
}

#[test]
fn terminal_catalog_duplicate_returns_the_whole_receipt_and_retains_both_slots() {
    let executor = HostExecutor::new();
    let mut state = DaemonControlState::default();
    let identity = HostJobIdentity::first(state.waiter_ids.next().unwrap());
    state.session_type_catalog.pending = Some((identity, 1));
    let result = |value: &str| HostResult::SessionTypeCatalogReady {
        generation: 1,
        entities: BTreeMap::from([("type".into(), serde_json::json!({"id": value}))]),
        logical_bytes: 20,
    };
    let original =
        HostCompletion::for_test(identity, result("first"), executor.try_reserve().unwrap());
    let duplicate =
        HostCompletion::for_test(identity, result("second"), executor.try_reserve().unwrap());
    route_terminal_host_completion(&mut state, original).unwrap();
    let duplicate = route_terminal_host_completion(&mut state, duplicate)
        .expect_err("terminal routing must return the duplicate receipt");
    assert_eq!(executor.outstanding(), 2);
    let HostResult::SessionTypeCatalogReady { entities, .. } = &*state
        .session_type_catalog
        .completion
        .as_ref()
        .unwrap()
        .result
    else {
        panic!("the original catalog result remains");
    };
    assert_eq!(entities["type"]["id"], "first");
    let HostResult::SessionTypeCatalogReady { entities, .. } = &*duplicate.result else {
        panic!("the duplicate catalog result remains");
    };
    assert_eq!(entities["type"]["id"], "second");
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    while !state.session_type_catalog.dispose_terminal(&executor) {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(
        executor.outstanding(),
        1,
        "the rejected receipt still retains its original slot"
    );
    let (identity, result, permit) = duplicate.into_parts();
    let mut disposal = crate::host_disposal::Job::new(crate::host_disposal::Parts {
        storage: None,
        identity,
        permit,
        payload: Box::new(result),
        model: None,
    });
    loop {
        if let crate::host_disposal::Poll::Disposed(permit) = disposal.poll() {
            drop(permit);
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(executor.outstanding(), 0);
}

#[test]
fn replacement_subscription_rejects_the_previous_publication() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    let mut state = DaemonControlState::default();
    let install = |state: &mut DaemonControlState| {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let target = Arc::new(crate::plugin_entity::Target {
            subscription_id: "sub".into(),
            entity_type: "task".into(),
            sender: EntityFrameSender::Async(sender),
        });
        install_package_entity_subscription(
            state,
            crate::plugin_entity::Registration {
                subscription_id: "sub".into(),
                target_key: "sub".into(),
                entity_type: "task".into(),
                reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                    "sub".into(),
                ),
            },
            Arc::clone(&target),
            None,
        )
        .unwrap();
        target
    };
    let old_target = install(&mut state);
    assert!(exact_package_entity_target_catching_up(&state, &old_target));
    let identity =
        crate::owner_identity::OwnerWorkIdentity::first(crate::owner_identity::WaiterId(904));
    let (live, _) =
        arm_package_entity_delivery(&mut state, &old_target, identity, 19, true, 19).unwrap();
    crate::daemon::control::entities::remove_entity_subscription(&mut state, "sub");
    assert!(!live.load(Ordering::Acquire));
    let new_target = install(&mut state);
    assert!(!exact_package_entity_target_catching_up(
        &state,
        &old_target
    ));
    assert!(exact_package_entity_target_catching_up(&state, &new_target));
    assert!(!complete_package_entity_delivery(
        &mut state,
        &old_target,
        identity,
        19,
        true,
        19,
        crate::plugin_entity::DeliveryStatus::Sent
    ));
    assert_eq!(
        state.entity_subscriptions["sub"].package_last_applied_seq,
        None
    );
    assert!(Arc::ptr_eq(
        &next_package_entity_target(&state, None).unwrap(),
        &new_target
    ));
    assert!(next_package_entity_target(&state, Some(&new_target)).is_none());
}

fn drive_all_maintenance(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    for kind in crate::daemon_maintenance::MaintenanceSliceKind::ALL {
        if kind == crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery {
            drive_entity_subscriptions(daemon, state);
        } else if let Some(runtime) = daemon.runtime() {
            crate::daemon_maintenance::run_maintenance_kind_to_completion(
                runtime,
                &mut state.maintenance,
                kind,
            );
        }
    }
}

#[test]
fn stale_catalog_completion_releases_capacity_without_publishing() {
    let executor = crate::host_executor::HostExecutor::new();
    let waiter_ids = WaiterIdSource::default();
    let identity = HostJobIdentity {
        waiter_id: waiter_ids.next().expect("allocate waiter identity"),
        phase: 1,
    };
    let permit = executor.try_reserve().expect("reserve catalog build");
    let mut cache = SessionTypeCatalogCache {
        generation: Some(0),
        entities: BTreeMap::from([("current".to_string(), serde_json::json!({}))]),
        logical_bytes: 9,
        pending: Some((identity, 1)),
        requested_generation: Some(2),
        ..SessionTypeCatalogCache::default()
    };
    let completion = HostCompletion::for_test(
        identity,
        HostResult::SessionTypeCatalogReady {
            generation: 1,
            entities: BTreeMap::from([("old".to_string(), serde_json::json!({}))]),
            logical_bytes: 5,
        },
        permit,
    );
    let held_permits = (1..crate::host_executor::HOST_OPERATION_CAPACITY)
        .map(|_| {
            executor
                .try_reserve()
                .expect("fill host operation capacity")
        })
        .collect::<Vec<_>>();
    assert!(executor.try_reserve().is_none());

    assert!(cache.absorb(completion, &executor));
    assert!(cache.pending.is_none());
    assert_eq!(cache.generation, Some(0));
    assert!(cache.entities.contains_key("current"));
    assert_eq!(cache.logical_bytes, 9);
    assert!(cache.retained_reclamation.is_none());
    drop(held_permits);
}

#[test]
fn accepted_catalog_replaces_its_retained_charge_and_failure_releases_it() {
    let executor = crate::host_executor::HostExecutor::new();
    let waiter_ids = WaiterIdSource::default();
    let first_identity = HostJobIdentity {
        waiter_id: waiter_ids.next().expect("allocate first waiter identity"),
        phase: 1,
    };
    let mut cache = SessionTypeCatalogCache {
        generation: Some(1),
        entities: BTreeMap::from([("old".to_string(), serde_json::json!({}))]),
        logical_bytes: 5,
        pending: Some((first_identity, 2)),
        requested_generation: Some(2),
        ..SessionTypeCatalogCache::default()
    };
    let replacement = HostCompletion::for_test(
        first_identity,
        HostResult::SessionTypeCatalogReady {
            generation: 2,
            entities: BTreeMap::from([("new".to_string(), serde_json::json!({}))]),
            logical_bytes: 7,
        },
        executor.try_reserve().expect("reserve replacement"),
    );

    assert!(cache.absorb(replacement, &executor));
    assert_eq!(cache.generation, Some(2));
    assert!(cache.entities.contains_key("new"));
    assert!(!cache.entities.contains_key("old"));
    assert_eq!(cache.logical_bytes, 7);

    let failure_identity = HostJobIdentity {
        waiter_id: waiter_ids.next().expect("allocate failure waiter identity"),
        phase: 1,
    };
    cache.pending = Some((failure_identity, 3));
    cache.requested_generation = Some(3);
    let failure = HostCompletion::for_test(
        failure_identity,
        HostResult::Failed {
            generation: 3,
            error: HostError::new("catalog_failed", "catalog failed"),
        },
        executor.try_reserve().expect("reserve failed replacement"),
    );

    assert!(cache.absorb(failure, &executor));
    assert!(cache.generation.is_none());
    assert!(cache.entities.contains_key("new"));
    assert_eq!(cache.logical_bytes, 7);
    assert!(cache.failure.is_some());
}

#[test]
fn stopped_executor_turns_a_pending_catalog_into_a_typed_failure() {
    let waiter_ids = WaiterIdSource::default();
    let mut cache = SessionTypeCatalogCache {
        pending: Some((
            HostJobIdentity {
                waiter_id: waiter_ids.next().expect("allocate waiter identity"),
                phase: 1,
            },
            4,
        )),
        requested_generation: Some(4),
        ..SessionTypeCatalogCache::default()
    };

    assert!(cache.executor_stopped());
    assert!(cache.pending.is_none());
    assert!(matches!(
        cache.failure,
        Some((
            4,
            HostError {
                ref code,
                ref message,
            },
        )) if code == "host_executor_stopped"
            && message.contains("before the catalog build completed")
    ));
}

#[test]
fn catalog_failure_keeps_session_type_subscriptions_and_delivers_each_version_once() {
    let (accepting_sender, accepting_receiver) = mpsc::sync_channel(4);
    let (full_sender, full_receiver) = mpsc::sync_channel(1);
    let (session_sender, session_receiver) = mpsc::sync_channel(1);
    let mut session = session_type_subscription_state(session_sender, 0, 0, BTreeMap::new(), None);
    session.entity_type = "session".to_string();
    full_sender
        .try_send(DaemonEntityFrame::Remove {
            subscription_id: "full".to_string(),
            entity_type: "session_type".to_string(),
            snapshot_seq: 0,
            id: "filler".to_string(),
        })
        .expect("fill the full subscriber's queue");
    let mut subscriptions = BTreeMap::from([
        (
            "accepting".to_string(),
            session_type_subscription_state(accepting_sender, 1, 1, BTreeMap::new(), None),
        ),
        (
            "full".to_string(),
            session_type_subscription_state(full_sender, 1, 1, BTreeMap::new(), None),
        ),
        ("session".to_string(), session),
    ]);

    assert!(
        drive_session_type_catalog_failure(
            &mut subscriptions,
            7,
            "catalog_failed",
            "catalog failed",
        ),
        "a full queue keeps the failure pending"
    );
    assert_eq!(subscriptions.len(), 3, "a catalog error retires nothing");
    assert!(session_receiver.try_recv().is_err());
    assert!(matches!(
        accepting_receiver.try_recv(),
        Ok(DaemonEntityFrame::Error {
            ref subscription_id,
            ref entity_type,
            ref code,
            ..
        }) if subscription_id == "accepting"
            && entity_type == "session_type"
            && code == "catalog_failed"
    ));
    let accepting = &subscriptions["accepting"];
    assert_eq!(accepting.definition_version, 7);
    assert_eq!(accepting.resync_reason.as_deref(), Some("catalog_failed"));
    assert_eq!(subscriptions["full"].definition_version, 0);

    // The full subscriber drains; the next drive reaches only it.
    assert!(matches!(
        full_receiver.try_recv(),
        Ok(DaemonEntityFrame::Remove { .. })
    ));
    assert!(!drive_session_type_catalog_failure(
        &mut subscriptions,
        7,
        "catalog_failed",
        "catalog failed",
    ));
    assert!(
        accepting_receiver.try_recv().is_err(),
        "a delivered failure version is not sent again"
    );
    assert!(matches!(
        full_receiver.try_recv(),
        Ok(DaemonEntityFrame::Error { ref subscription_id, .. }) if subscription_id == "full"
    ));
    assert_eq!(subscriptions["full"].definition_version, 7);

    // Repeated drives of the same cached failure send nothing.
    assert!(!drive_session_type_catalog_failure(
        &mut subscriptions,
        7,
        "catalog_failed",
        "catalog failed",
    ));
    assert!(accepting_receiver.try_recv().is_err());
    assert!(full_receiver.try_recv().is_err());

    // A disconnected subscriber retires on a new failure version.
    drop(full_receiver);
    assert!(!drive_session_type_catalog_failure(
        &mut subscriptions,
        8,
        "catalog_failed",
        "catalog failed",
    ));
    assert!(!subscriptions.contains_key("full"));
    assert!(subscriptions.contains_key("accepting"));
    assert!(matches!(
        accepting_receiver.try_recv(),
        Ok(DaemonEntityFrame::Error { .. })
    ));
}

/// Recovery after a delivered catalog error replaces the subscriber's
/// state with a full snapshot, even when the repaired definitions equal
/// the last delivered baseline (a plain delta would send nothing).
#[test]
fn catalog_recovery_to_identical_definitions_sends_a_replacement_snapshot() {
    let (sender, receiver) = mpsc::sync_channel(4);
    let baseline = BTreeMap::from([("agent".to_string(), serde_json::json!({"label": "agent"}))]);
    let mut subscription = session_type_subscription_state(sender, 1, 3, baseline.clone(), None);
    subscription.definition_version = 1;
    let mut subscriptions = BTreeMap::from([("types".to_string(), subscription)]);

    assert!(!drive_session_type_catalog_failure(
        &mut subscriptions,
        2,
        "invalid_repo_session_types",
        "invalid",
    ));
    assert!(matches!(
        receiver.try_recv(),
        Ok(DaemonEntityFrame::Error { .. })
    ));

    drive_session_type_subscriptions(&mut subscriptions, 1, 3, &baseline);
    assert!(matches!(
        receiver.try_recv(),
        Ok(DaemonEntityFrame::Snapshot {
            snapshot_seq: 4,
            ref items,
            ref resync_reason,
            ..
        }) if items == &baseline.values().cloned().collect::<Vec<_>>()
            && resync_reason.as_deref() == Some("invalid_repo_session_types")
    ));
    let subscription = &subscriptions["types"];
    assert!(subscription.resync_reason.is_none());
    assert_eq!(subscription.definition_version, 3);
    assert_eq!(subscription.next_seq, 4);
    drive_session_type_subscriptions(&mut subscriptions, 1, 3, &baseline);
    assert!(receiver.try_recv().is_err(), "recovery is delivered once");
}

/// A subscriber that has not received a baseline gets the error, then its
/// initial snapshot on recovery.
#[test]
fn catalog_recovery_before_a_baseline_sends_the_initial_snapshot() {
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut subscription = session_type_subscription_state(sender, 0, 0, BTreeMap::new(), None);
    subscription.awaiting_initial_snapshot = true;
    let mut subscriptions = BTreeMap::from([("types".to_string(), subscription)]);
    assert!(!drive_session_type_catalog_failure(
        &mut subscriptions,
        1,
        "invalid_repo_session_types",
        "invalid",
    ));
    assert!(matches!(
        receiver.try_recv(),
        Ok(DaemonEntityFrame::Error { .. })
    ));
    assert!(subscriptions["types"].resync_reason.is_none());

    let entities = BTreeMap::from([("agent".to_string(), serde_json::json!({"label": "agent"}))]);
    drive_session_type_subscriptions(&mut subscriptions, 5, 2, &entities);
    assert!(matches!(
        receiver.try_recv(),
        Ok(DaemonEntityFrame::Snapshot {
            snapshot_seq: 5,
            ref resync_reason,
            ..
        }) if resync_reason.is_none()
    ));
    assert!(!subscriptions["types"].awaiting_initial_snapshot);
}

/// Registration during a cached catalog failure opens the subscription
/// and sends the error. The failure version is marked only after the send
/// succeeds; a full queue leaves the error pending for the drive.
#[test]
fn registration_during_a_catalog_failure_opens_and_delivers_the_error() {
    let (mut daemon, directory) = catalog_test_daemon("catalog-failure-registration");
    let mut state = DaemonControlState::default();
    let generation = daemon
        .runtime()
        .expect("runtime")
        .state()
        .session_type_generation;
    state.session_type_catalog.install_failure(
        generation,
        HostError::new("invalid_repo_session_types", "invalid"),
    );
    let version = state.session_type_catalog.version;

    let (sender, receiver) = mpsc::sync_channel(4);
    let response = register_builtin_entity_subscription(
        &mut daemon,
        &mut state,
        "session_type".to_string(),
        "accepting".to_string(),
        EntityFrameSender::Blocking(sender),
        None,
    )
    .expect("register during a catalog failure");
    assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
    assert!(matches!(
        receiver.try_recv(),
        Ok(DaemonEntityFrame::Error { ref subscription_id, ref code, .. })
            if subscription_id == "accepting" && code == "invalid_repo_session_types"
    ));
    let accepting = &state.entity_subscriptions["accepting"];
    assert_eq!(accepting.definition_version, version);
    assert!(accepting.awaiting_initial_snapshot);

    // A rendezvous channel with no waiting reader reports Full.
    let (full_sender, _full_receiver) = mpsc::sync_channel(0);
    let response = register_builtin_entity_subscription(
        &mut daemon,
        &mut state,
        "session_type".to_string(),
        "full".to_string(),
        EntityFrameSender::Blocking(full_sender),
        None,
    )
    .expect("register with a full queue");
    assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
    assert_eq!(
        state.entity_subscriptions["full"].definition_version, 0,
        "an unsent failure is not marked delivered"
    );
    daemon.stop();
    let _ = std::fs::remove_dir_all(directory);
}

/// Every installed failure has its own version; reads of one cached
/// failure keep it.
#[test]
fn every_installed_catalog_failure_advances_the_version() {
    let waiter_ids = WaiterIdSource::default();
    let mut cache = SessionTypeCatalogCache::default();
    cache.install_failure(4, HostError::new("first", "first"));
    assert_eq!(cache.version, 1);
    cache.install_failure(4, HostError::new("first", "first"));
    assert_eq!(cache.version, 2, "an identical failure is a new result");
    cache.pending = Some((
        HostJobIdentity {
            waiter_id: waiter_ids.next().expect("allocate waiter identity"),
            phase: 1,
        },
        4,
    ));
    assert!(cache.executor_stopped());
    assert_eq!(cache.version, 3, "executor_stopped installs a new failure");
}

#[test]
fn session_lifecycle_class_is_total_and_stale_first() {
    let concrete = [
        (SessionLifecycleState::Starting, "current"),
        (SessionLifecycleState::Running, "current"),
        (SessionLifecycleState::Stopping, "current"),
        (SessionLifecycleState::Exited { code: Some(0) }, "ended"),
        (
            SessionLifecycleState::Failed {
                reason: "failed".to_string(),
            },
            "ended",
        ),
    ];
    for (lifecycle, expected) in &concrete {
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Running, Some(lifecycle)),
            *expected
        );
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Stale, Some(lifecycle)),
            "indeterminate"
        );
    }
    assert_eq!(
        session_lifecycle_class(&RegistrySessionState::Running, None),
        "indeterminate"
    );
    assert_eq!(
        session_lifecycle_class(&RegistrySessionState::Exited, None),
        "ended"
    );
    assert_eq!(
        session_lifecycle_class(&RegistrySessionState::Stale, None),
        "indeterminate"
    );
}

#[test]
fn session_entity_patch_explicitly_updates_required_lifecycle_class() {
    let entity = |registry_state: &str, lifecycle: Option<&str>, lifecycle_class: &str| {
        DaemonSessionEntity {
            session_uuid: "session-1".to_string(),
            registry_state: registry_state.to_string(),
            lifecycle: lifecycle.map(str::to_string),
            lifecycle_class: lifecycle_class.to_string(),
            rows: 24,
            cols: 80,
            updated_at: 1,
            exit_code: None,
            failure_reason: None,
            session_type_id: None,
            session_type_source: None,
            role: None,
            traits: Vec::new(),
            interaction: None,
            session_type_lifecycle: None,
            restartable: false,
        }
    };
    let current = entity("running", Some("running"), "current");
    let ended = entity("exited", Some("exited"), "ended");
    let no_lifecycle = entity("running", None, "indeterminate");
    let stale = entity("stale", Some("running"), "indeterminate");

    assert_eq!(
        session_entity_patch(&current, &ended)["lifecycle_class"],
        "ended"
    );
    assert_eq!(
        session_entity_patch(&current, &no_lifecycle)["lifecycle_class"],
        "indeterminate"
    );
    assert_eq!(
        session_entity_patch(&current, &stale)["lifecycle_class"],
        "indeterminate"
    );
}

#[test]
fn live_session_entity_subscription_emits_exact_stale_transition_patch() {
    let data_directory = std::env::temp_dir().join(format!(
        "botster-hub-stale-transition-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    ));
    let config = crate::HubStartupOptions {
        host: crate::HostIdentityOptions {
            id: "stale-transition-test".to_string(),
            display_name: "Stale Transition Test".to_string(),
            fingerprint: None,
        },
        data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
        session_defaults: crate::SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: crate::TransportBindings::default(),
        ..crate::HubStartupOptions::default()
    }
    .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
    .expect("build stale transition config");
    let mut daemon = HubDaemon::start(config).expect("start stale transition daemon");
    let session_id = SessionId("stale-transition-session".to_string());
    daemon
        .runtime_mut()
        .expect("runtime initialized")
        .spawn_session_for_test(
            botster_core::SessionSpawnRequest {
                request_id: RequestId("stale-transition-spawn".to_string()),
                session_id: session_id.clone(),
                executable: "/bin/sh".to_string(),
                arguments: vec![
                    "-c".to_string(),
                    "while IFS= read -r line; do printf '%s\\n' \"$line\"; done".to_string(),
                ],
                working_directory: botster_core::SpawnWorkingDirectory {
                    path: ".".to_string(),
                },
                environment: botster_core::SpawnEnvironment::default(),
                initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
            },
            botster_core::CoreSessionMetadata::new(),
        )
        .expect("spawn worker-backed session");

    let mut state = DaemonControlState::default();
    seed_lifecycle_reconciliation(&mut daemon, &mut state);
    for _ in 0..16 {
        drive_all_maintenance(&mut daemon, &mut state);
    }
    let (sender, receiver) = mpsc::sync_channel(4);
    let response = register_builtin_entity_subscription(
        &mut daemon,
        &mut state,
        "session".to_string(),
        "stale-transition-subscription".to_string(),
        EntityFrameSender::Blocking(sender),
        None,
    )
    .expect("register entity subscription");
    assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
    let mut first = receiver.try_recv().ok();
    for _ in 0..32 {
        if first.is_some() {
            break;
        }
        drive_all_maintenance(&mut daemon, &mut state);
        first = receiver.try_recv().ok();
    }
    let first = first.expect("initial authoritative snapshot");
    match first {
        DaemonEntityFrame::Snapshot { ref items, .. } => {
            assert!(
                items.iter().any(|entity| {
                    entity.get("session_uuid").and_then(Value::as_str) == Some(&session_id.0)
                        && entity.get("lifecycle_class").and_then(Value::as_str) == Some("current")
                }),
                "first snapshot must contain the live session"
            );
        }
        other => panic!("expected populated snapshot, got {other:?}"),
    }

    daemon
        .runtime()
        .expect("runtime initialized")
        .mark_session_stale_for_test(&session_id, 2)
        .expect("mark live session stale through core daemon");
    for _ in 0..16 {
        drive_all_maintenance(&mut daemon, &mut state);
    }
    assert!(matches!(
        receiver.recv().expect("stale transition patch"),
        DaemonEntityFrame::Patch {
            ref id,
            ref patch,
            ..
        } if id == &session_id.0
            && patch == &serde_json::json!({
                "registry_state": "stale",
                "lifecycle_class": "indeterminate",
                "updated_at": 2
            })
    ));

    daemon
        .runtime_mut()
        .expect("runtime initialized")
        .shutdown_session_for_test(session_id)
        .expect("stop worker-backed test session");
    daemon.stop();
    let _ = fs::remove_dir_all(data_directory);
}

#[test]
fn existing_session_subscriber_receives_spawn_upsert_without_another_request() {
    let data_directory = std::env::temp_dir().join(format!(
        "botster-hub-existing-sub-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    ));
    let config = crate::HubStartupOptions {
        host: crate::HostIdentityOptions {
            id: "existing-sub-test".to_string(),
            display_name: "Existing Subscriber Test".to_string(),
            fingerprint: None,
        },
        data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
        session_defaults: crate::SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: crate::TransportBindings::default(),
        ..crate::HubStartupOptions::default()
    }
    .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
    .expect("build existing subscriber config");
    let mut daemon = HubDaemon::start(config).expect("start existing subscriber daemon");
    let mut state = DaemonControlState::default();
    seed_lifecycle_reconciliation(&mut daemon, &mut state);
    for _ in 0..16 {
        drive_all_maintenance(&mut daemon, &mut state);
    }
    let (sender, receiver) = mpsc::sync_channel(8);
    let response = register_builtin_entity_subscription(
        &mut daemon,
        &mut state,
        "session".to_string(),
        "existing-sub".to_string(),
        EntityFrameSender::Blocking(sender),
        None,
    )
    .expect("register idle session subscription");
    assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
    let mut first = receiver.try_recv().ok();
    for _ in 0..32 {
        if first.is_some() {
            break;
        }
        drive_all_maintenance(&mut daemon, &mut state);
        first = receiver.try_recv().ok();
    }
    match first.expect("first snapshot before spawn") {
        DaemonEntityFrame::Snapshot { .. } => {}
        other => panic!("expected first snapshot, got {other:?}"),
    }
    let session_id = SessionId("assemble-ready-spawn".to_string());
    daemon
        .runtime_mut()
        .expect("runtime initialized")
        .spawn_session_for_test(
            botster_core::SessionSpawnRequest {
                request_id: RequestId("existing-sub-spawn".to_string()),
                session_id: session_id.clone(),
                executable: "/bin/sleep".to_string(),
                arguments: vec!["8".to_string()],
                working_directory: botster_core::SpawnWorkingDirectory {
                    path: ".".to_string(),
                },
                environment: botster_core::SpawnEnvironment::default(),
                initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
            },
            botster_core::CoreSessionMetadata::new(),
        )
        .expect("spawn after first snapshot");
    state.maintenance.note_authoritative_mutation();
    let mut saw_ready = false;
    for _ in 0..16 {
        drive_all_maintenance(&mut daemon, &mut state);
        while let Ok(frame) = receiver.try_recv() {
            match frame {
                DaemonEntityFrame::Upsert { id, .. } | DaemonEntityFrame::Patch { id, .. }
                    if id == session_id.0 =>
                {
                    saw_ready = true;
                }
                DaemonEntityFrame::Snapshot { items, .. }
                    if items.iter().any(|entity| {
                        entity.get("session_uuid").and_then(Value::as_str)
                            == Some(session_id.0.as_str())
                    }) =>
                {
                    saw_ready = true;
                }
                DaemonEntityFrame::Error { code, message, .. } => {
                    panic!("existing subscriber error: {code}: {message}");
                }
                _ => {}
            }
        }
        if saw_ready {
            break;
        }
    }
    assert!(
        saw_ready,
        "existing subscriber must receive assemble-ready-spawn without another client request"
    );
    let _ = daemon
        .runtime_mut()
        .expect("runtime initialized")
        .shutdown_session_for_test(session_id);
    daemon.stop();
    let _ = fs::remove_dir_all(data_directory);
}

#[test]
fn entity_overflow_requires_empty_snapshot_resync_and_failed_delivery_disconnects() {
    let fixture = botster_hub_test_support::session_lifecycle_subscription_conformance_scenario();
    let overflow_reason = fixture.overflow.resync_reason.clone();
    assert!(fixture.overflow.empty_snapshot_valid);
    assert!(fixture.overflow.snapshot_precedes_later_deltas);
    assert!(
        fixture
            .overflow
            .failed_snapshot_delivery_closes_subscription
    );
    let cursor = SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
        sequence: 9,
    };
    let baseline = || SessionLifecycleBaseline {
        cursor: cursor.clone(),
        sessions: Vec::new(),
    };
    let (sender, receiver) = mpsc::sync_channel(1);
    sender
        .try_send(DaemonEntityFrame::Snapshot {
            subscription_id: "subscription".to_string(),
            entity_type: "session".to_string(),
            snapshot_seq: 8,
            items: Vec::new(),
            resync_reason: None,
        })
        .expect("fill bounded subscriber queue");
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: Some(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
            sequence: 8,
        }),
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: Some(overflow_reason.clone()),
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Removes,
        next_seq: 0,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: false,
    };
    let mut counters = DaemonLifecycleCounters::default();

    assert!(try_resync_subscription(
        "subscription",
        &mut state,
        baseline(),
        overflow_reason.clone(),
        &mut counters,
    ));
    assert_eq!(
        state.resync_reason.as_deref(),
        Some(overflow_reason.as_str())
    );
    let _ = receiver.recv().expect("drain stale queued frame");
    assert!(try_resync_subscription(
        "subscription",
        &mut state,
        baseline(),
        overflow_reason.clone(),
        &mut counters,
    ));
    assert!(state.resync_reason.is_none());
    assert!(matches!(
        receiver.recv().expect("receive empty resync snapshot"),
        DaemonEntityFrame::Snapshot {
            snapshot_seq: 9,
            ref items,
            resync_reason: Some(ref reason),
            ..
        } if items.is_empty() && reason == &overflow_reason
    ));

    drop(receiver);
    state.resync_reason = Some(overflow_reason.clone());
    assert!(!try_resync_subscription(
        "subscription",
        &mut state,
        baseline(),
        overflow_reason,
        &mut counters,
    ));
    assert_eq!(counters.entity_delivery_attempts, 3);
    assert_eq!(counters.entity_delivery_successes, 1);
    assert_eq!(counters.entity_delivery_overflows, 1);
    assert_eq!(counters.entity_delivery_failures, 1);
}

#[test]
fn session_type_resync_replaces_oversized_snapshot_with_typed_error() {
    let (sender, receiver) = mpsc::sync_channel(1);
    let mut subscriptions = BTreeMap::from([(
        "oversized-session-types".to_string(),
        EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session_type".to_string(),
            cursor: None,
            entities: BTreeMap::new(),
            definition_generation: 1,
            definition_version: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: Some("subscriber_overflow".to_string()),
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        },
    )]);
    let entities = BTreeMap::from([(
        "device/oversized".to_string(),
        serde_json::json!({ "description": "x".repeat(DAEMON_MAX_FRAME_BYTES) }),
    )]);

    drive_session_type_subscriptions(&mut subscriptions, 2, 2, &entities);

    assert!(
        subscriptions.is_empty(),
        "typed error closes the subscription"
    );
    assert!(matches!(
        receiver.recv().expect("receive bounded typed error"),
        DaemonEntityFrame::Error {
            ref subscription_id,
            ref entity_type,
            ref code,
            ..
        } if subscription_id == "oversized-session-types"
            && entity_type == "session_type"
            && code == "entity_provider_frame_too_large"
    ));
}

fn session_type_subscription_state(
    sender: mpsc::SyncSender<DaemonEntityFrame>,
    definition_generation: u64,
    next_seq: u64,
    definition_entities: BTreeMap<String, Value>,
    resync_reason: Option<String>,
) -> EntitySubscriptionState {
    EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session_type".to_string(),
        cursor: None,
        entities: BTreeMap::new(),
        definition_generation,
        definition_version: 0,
        awaiting_initial_snapshot: false,
        definition_entities,
        resync_reason,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Removes,
        next_seq,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: false,
    }
}

fn session_type_delta_seqs(
    receiver: &mpsc::Receiver<DaemonEntityFrame>,
) -> Vec<(String, u64, String)> {
    receiver
        .try_iter()
        .filter_map(|frame| match frame {
            DaemonEntityFrame::Upsert {
                subscription_id,
                snapshot_seq,
                id,
                ..
            }
            | DaemonEntityFrame::Remove {
                subscription_id,
                snapshot_seq,
                id,
                ..
            } => Some((subscription_id, snapshot_seq, id)),
            DaemonEntityFrame::Snapshot {
                subscription_id,
                snapshot_seq,
                ..
            } => Some((subscription_id, snapshot_seq, "snapshot".to_string())),
            DaemonEntityFrame::Error { .. } | DaemonEntityFrame::Patch { .. } => None,
        })
        .collect()
}

#[test]
fn session_type_same_generation_multi_row_uses_contiguous_subscriber_seq() {
    let (sender, receiver) = mpsc::sync_channel(8);
    let mut subscriptions = BTreeMap::from([(
        "held-session-types".to_string(),
        session_type_subscription_state(
            sender,
            1,
            1,
            BTreeMap::from([
                (
                    "device/alpha".to_string(),
                    serde_json::json!({ "label": "Alpha" }),
                ),
                (
                    "device/beta".to_string(),
                    serde_json::json!({ "label": "Beta" }),
                ),
            ]),
            None,
        ),
    )]);
    let entities = BTreeMap::from([
        (
            "device/alpha".to_string(),
            serde_json::json!({ "label": "Alpha 2" }),
        ),
        (
            "device/beta".to_string(),
            serde_json::json!({ "label": "Beta 2" }),
        ),
    ]);

    drive_session_type_subscriptions(&mut subscriptions, 2, 2, &entities);

    let frames = session_type_delta_seqs(&receiver);
    assert_eq!(
        frames,
        vec![
            (
                "held-session-types".to_string(),
                2,
                "device/alpha".to_string()
            ),
            (
                "held-session-types".to_string(),
                3,
                "device/beta".to_string()
            ),
        ],
        "one generation with two published diffs must deliver N+1 then N+2 on the held subscription"
    );
    assert_eq!(
        subscriptions
            .get("held-session-types")
            .map(|subscription| subscription.next_seq),
        Some(3)
    );
}

#[test]
fn session_type_skipped_generation_uses_contiguous_subscriber_seq() {
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut subscriptions = BTreeMap::from([(
        "held-session-types".to_string(),
        session_type_subscription_state(
            sender,
            1,
            1,
            BTreeMap::from([(
                "device/alpha".to_string(),
                serde_json::json!({ "label": "Alpha" }),
            )]),
            None,
        ),
    )]);
    let entities = BTreeMap::from([(
        "device/alpha".to_string(),
        serde_json::json!({ "label": "Alpha 3" }),
    )]);

    drive_session_type_subscriptions(&mut subscriptions, 3, 3, &entities);

    let frames = session_type_delta_seqs(&receiver);
    assert_eq!(
        frames,
        vec![(
            "held-session-types".to_string(),
            2,
            "device/alpha".to_string()
        )],
        "a skipped dirty generation must still deliver the next contiguous subscriber seq, not the generation number"
    );
    assert_eq!(
        subscriptions
            .get("held-session-types")
            .map(|subscription| subscription.next_seq),
        Some(2)
    );
}

#[test]
fn session_type_overflow_resync_advances_subscriber_seq_not_generation() {
    let (sender, receiver) = mpsc::sync_channel(2);
    let mut subscriptions = BTreeMap::from([(
        "held-session-types".to_string(),
        session_type_subscription_state(
            sender,
            7,
            7,
            BTreeMap::from([(
                "device/alpha".to_string(),
                serde_json::json!({ "label": "Alpha" }),
            )]),
            Some("subscriber_overflow".to_string()),
        ),
    )]);
    let entities = BTreeMap::from([(
        "device/alpha".to_string(),
        serde_json::json!({ "label": "Alpha recovered" }),
    )]);

    drive_session_type_subscriptions(&mut subscriptions, 3, 3, &entities);

    let frames = session_type_delta_seqs(&receiver);
    assert_eq!(
        frames,
        vec![("held-session-types".to_string(), 8, "snapshot".to_string())],
        "overflow resync must send next_seq+1 and must not move snapshot_seq backwards to the generation"
    );
    let subscription = subscriptions
        .get("held-session-types")
        .expect("held subscription remains open");
    assert_eq!(subscription.next_seq, 8);
    assert_eq!(subscription.definition_generation, 3);
    assert!(subscription.resync_reason.is_none());
}

#[test]
fn async_entity_overflow_requires_empty_snapshot_resync_and_closed_delivery_disconnects() {
    let overflow_reason = "subscriber_overflow".to_string();
    let cursor = SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
        sequence: 9,
    };
    let baseline = || SessionLifecycleBaseline {
        cursor: cursor.clone(),
        sessions: Vec::new(),
    };
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    sender
        .try_send(
            DaemonEntityFrame::Snapshot {
                subscription_id: "async-subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 8,
                items: Vec::new(),
                resync_reason: None,
            }
            .into(),
        )
        .expect("fill bounded async subscriber queue");
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Async(sender),
        entity_type: "session".to_string(),
        cursor: Some(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
            sequence: 8,
        }),
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: Some(overflow_reason.clone()),
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Removes,
        next_seq: 0,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: false,
    };
    let mut counters = DaemonLifecycleCounters::default();

    assert!(try_resync_subscription(
        "async-subscription",
        &mut state,
        baseline(),
        overflow_reason.clone(),
        &mut counters,
    ));
    assert_eq!(
        state.resync_reason.as_deref(),
        Some(overflow_reason.as_str()),
        "a full production WebRTC queue must retain its pending resync"
    );
    let _ = receiver.try_recv().expect("drain stale async frame");
    assert!(try_resync_subscription(
        "async-subscription",
        &mut state,
        baseline(),
        overflow_reason.clone(),
        &mut counters,
    ));
    assert!(state.resync_reason.is_none());
    assert!(matches!(
        receiver.try_recv().expect("receive async resync snapshot"),
        crate::entity_delivery::EntityDelivery::Typed(DaemonEntityFrame::Snapshot {
            snapshot_seq: 9,
            ref items,
            resync_reason: Some(ref reason),
            ..
        }) if items.is_empty() && reason == &overflow_reason
    ));

    drop(receiver);
    state.resync_reason = Some(overflow_reason.clone());
    assert!(!try_resync_subscription(
        "async-subscription",
        &mut state,
        baseline(),
        overflow_reason,
        &mut counters,
    ));
    assert_eq!(counters.entity_delivery_attempts, 3);
    assert_eq!(counters.entity_delivery_successes, 1);
    assert_eq!(counters.entity_delivery_overflows, 1);
    assert_eq!(counters.entity_delivery_failures, 1);
}

#[test]
fn delivery_page_does_not_skip_a_low_id_after_a_high_remove() {
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.replace_complete_baseline(
        SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 2,
        },
        Vec::new(),
    );
    let record = |id: &str| botster_core_daemon::SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id.to_string()),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    projection.ingest_baseline_rows(2, [record("a")]);
    let (sender, receiver) = mpsc::sync_channel(8);
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: projection.cursor.clone(),
        entities: BTreeMap::from([(
            "z".to_string(),
            crate::session_projection::SessionProjection::project_entity(&record("z")),
        )]),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Removes,
        next_seq: 0,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: false,
    };
    let mut counters = DaemonLifecycleCounters::default();
    let (alive, first) = deliver_projection_delta_page(
        "sub",
        &mut state,
        &projection,
        &mut counters,
        1,
        usize::MAX,
        Duration::from_secs(1),
    );
    assert!(alive);
    assert!(first.more);
    let (alive, second) = deliver_projection_delta_page(
        "sub",
        &mut state,
        &projection,
        &mut counters,
        1,
        usize::MAX,
        Duration::from_secs(1),
    );
    assert!(alive);
    assert!(!second.more);
    let frames: Vec<_> = receiver.try_iter().collect();
    assert!(frames.iter().any(|frame| matches!(
        frame,
        DaemonEntityFrame::Remove { id, .. } if id == "z"
    )));
    assert!(frames.iter().any(|frame| matches!(
        frame,
        DaemonEntityFrame::Upsert { id, .. } if id == "a"
    )));
}

#[test]
fn delivery_page_keeps_snapshot_seq_monotonic_when_id_order_reverses_journal_order() {
    let record = |id: &str| botster_core_daemon::SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id.to_string()),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(1, [record("z")]);
    projection.ingest_baseline_rows(2, [record("a")]);
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 2,
    });
    let (sender, receiver) = mpsc::sync_channel(8);
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: projection.cursor.clone(),
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Rows,
        next_seq: 0,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: false,
    };
    let mut counters = DaemonLifecycleCounters::default();
    let _ = deliver_projection_delta_page(
        "sub",
        &mut state,
        &projection,
        &mut counters,
        1,
        usize::MAX,
        Duration::from_secs(1),
    );
    let _ = deliver_projection_delta_page(
        "sub",
        &mut state,
        &projection,
        &mut counters,
        1,
        usize::MAX,
        Duration::from_secs(1),
    );
    let seqs: Vec<u64> = receiver
        .try_iter()
        .filter_map(|frame| match frame {
            DaemonEntityFrame::Upsert { snapshot_seq, .. }
            | DaemonEntityFrame::Patch { snapshot_seq, .. } => Some(snapshot_seq),
            _ => None,
        })
        .collect();
    assert_eq!(seqs, vec![1, 2]);
}

#[test]
fn overflow_resync_does_not_move_snapshot_seq_backwards() {
    let record = |id: &str| botster_core_daemon::SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id.to_string()),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(11, [record("a"), record("b"), record("c")]);
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 11,
    });
    let (sender, receiver) = mpsc::sync_channel(8);
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: projection.cursor.clone(),
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Rows,
        next_seq: 110,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    };
    let mut counters = DaemonLifecycleCounters::default();
    let _ = deliver_projection_delta_page(
        "sub",
        &mut state,
        &projection,
        &mut counters,
        1,
        usize::MAX,
        Duration::from_secs(1),
    );
    state.resync_reason = Some("subscriber_overflow".to_string());
    assert!(try_resync_from_projection(
        "sub",
        &mut state,
        &projection,
        true,
        "subscriber_overflow".to_string(),
        &mut counters,
    ));
    let seqs: Vec<u64> = receiver
        .try_iter()
        .filter_map(|frame| match frame {
            DaemonEntityFrame::Upsert { snapshot_seq, .. }
            | DaemonEntityFrame::Patch { snapshot_seq, .. }
            | DaemonEntityFrame::Snapshot { snapshot_seq, .. }
            | DaemonEntityFrame::Remove { snapshot_seq, .. } => Some(snapshot_seq),
            DaemonEntityFrame::Error { .. } => None,
        })
        .collect();
    assert!(seqs.len() >= 2);
    for window in seqs.windows(2) {
        assert!(window[0] < window[1], "sequences moved backwards: {seqs:?}");
    }
    assert!(seqs[0] > 110 || seqs.iter().any(|seq| *seq > 110));
}

#[test]
fn paged_delivery_stays_within_owner_turn_for_a_large_registry() {
    let record = |id: String| botster_core_daemon::SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(
        1,
        (0..256).map(|index| record(format!("session-{index:03}"))),
    );
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 1,
    });
    let mut delivered = 0;
    for subscriber in 0..2 {
        let (sender, receiver) = mpsc::sync_channel(256);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_version: 0,
            definition_entities: BTreeMap::new(),
            awaiting_initial_snapshot: false,
            resync_reason: None,
            terminating: false,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            package_delivery: None,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Rows,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        };
        let mut counters = DaemonLifecycleCounters::default();
        loop {
            let (alive, page) = deliver_projection_delta_page(
                &format!("sub-{subscriber}"),
                &mut state,
                &projection,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                SESSION_DELIVERY_MAX_ELAPSED,
            );
            assert!(alive);
            assert!(page.items <= SESSION_DELIVERY_MAX_ITEMS);
            assert!(page.bytes <= SESSION_DELIVERY_MAX_BYTES);
            if !page.more {
                break;
            }
        }
        delivered += receiver.try_iter().count();
    }
    assert_eq!(delivered, 512);
}

#[test]
fn first_session_snapshot_is_complete_and_assembled_in_pages() {
    let record = |id: String| SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(
        1,
        (0..24).map(|index| record(format!("session-{index:02}"))),
    );
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 1,
    });
    let (sender, receiver) = mpsc::sync_channel(32);
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: projection.cursor.clone(),
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
        next_seq: 1,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    };
    let mut counters = DaemonLifecycleCounters::default();
    let mut pages = 0;
    let envelope = snapshot_envelope_bytes("sub", &state);
    let mut charged_item_bytes = 0usize;
    loop {
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("assembly must stay alive");
        };
        assert!(page.items <= SESSION_DELIVERY_MAX_ITEMS);
        assert!(page.bytes <= SESSION_DELIVERY_MAX_BYTES);
        charged_item_bytes = charged_item_bytes.saturating_add(page.bytes.saturating_sub(envelope));
        pages += 1;
        if !page.more {
            break;
        }
        assert!(
            matches!(state.delivery_phase, DeliveryPhase::Assembling { .. }),
            "must keep assembling until the complete snapshot"
        );
        assert!(receiver.try_iter().next().is_none());
        assert!(pages < 8);
    }
    assert!(pages > 1);
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        DaemonEntityFrame::Snapshot {
            items,
            resync_reason,
            ..
        } => {
            assert_eq!(items.len(), 24);
            assert_eq!(resync_reason, &None);
        }
        other => panic!("expected one complete snapshot, got {other:?}"),
    }
    let encoded = serde_json::to_vec(&frames[0])
        .expect("encode sent frame")
        .len();
    assert_eq!(charged_item_bytes.saturating_add(envelope), encoded);
    assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
    assert!(!state.needs_delivery);
}

#[test]
fn catch_up_restarts_when_a_prefix_id_changes() {
    let record = |id: &str, updated_at: u64| SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id.to_string()),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(
        1,
        (0..24).map(|index| record(&format!("session-{index:02}"), 1)),
    );
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 1,
    });
    let (sender, receiver) = mpsc::sync_channel(8);
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: projection.cursor.clone(),
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
        next_seq: 1,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    };
    let mut counters = DaemonLifecycleCounters::default();
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    ) else {
        panic!("first page");
    };
    assert!(page.more);
    assert!(receiver.try_iter().next().is_none());
    projection.ingest_baseline_rows(2, [record("session-00", 9)]);
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 2,
    });
    loop {
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("restarted assembly");
        };
        if !page.more {
            break;
        }
    }
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        DaemonEntityFrame::Snapshot { items, .. } => {
            let first = items
                .iter()
                .find(|item| item.get("session_uuid").and_then(Value::as_str) == Some("session-00"))
                .expect("prefix row");
            assert_eq!(first.get("updated_at").and_then(Value::as_u64), Some(9));
            assert_eq!(items.len(), 24);
        }
        other => panic!("expected complete snapshot, got {other:?}"),
    }
}

#[test]
fn oversized_first_snapshot_closes_the_subscription() {
    let huge = SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId("x".repeat(DAEMON_MAX_FRAME_BYTES)),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(1, [huge]);
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 1,
    });
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: projection.cursor.clone(),
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
        next_seq: 1,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    };
    let mut counters = DaemonLifecycleCounters::default();
    let first = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    );
    assert!(matches!(
        first,
        SnapshotAssemble::Closed {
            frame_too_large: true
        }
    ));
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    assert!(matches!(
        &frames[0],
        DaemonEntityFrame::Error { code, .. } if code == "entity_provider_frame_too_large"
    ));
    assert!(!state.needs_delivery);
    assert!(state.resync_reason.is_none());
}

#[test]
fn no_removal_scan_stays_within_owner_turn() {
    let record = |id: String| SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(
        1,
        (0..256).map(|index| record(format!("session-{index:03}"))),
    );
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 1,
    });
    let (sender, _receiver) = mpsc::sync_channel(16);
    let mut entities = BTreeMap::new();
    for (id, row) in &projection.rows {
        entities.insert(
            id.clone(),
            crate::session_projection::SessionProjection::project_entity(&row.record),
        );
    }
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: projection.cursor.clone(),
        entities,
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Removes,
        next_seq: 1,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    };
    let mut counters = DaemonLifecycleCounters::default();
    let (alive, page) = deliver_projection_delta_page(
        "sub",
        &mut state,
        &projection,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_ELAPSED,
    );
    assert!(alive);
    assert!(page.items <= SESSION_DELIVERY_MAX_ITEMS);
    assert!(page.bytes <= SESSION_DELIVERY_MAX_BYTES);
    assert!(page.more);
    assert!(state.delivery_after.is_some());
}

#[test]
fn near_limit_snapshot_assembly_stays_within_owner_turn() {
    let record = |id: String| SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(
        1,
        (0..20).map(|index| record(format!("s{index:02}-{}", "x".repeat(40 * 1024)))),
    );
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 1,
    });
    let (sender, receiver) = mpsc::sync_channel(2);
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: projection.cursor.clone(),
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
        next_seq: 1,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    };
    let mut counters = DaemonLifecycleCounters::default();
    // This case proves bounded per-call assembly work, not wall-clock latency.
    // Duration::MAX cannot cut a page, so page cuts are byte-driven: one item per page.
    const MAX_NEAR_LIMIT_PAGES: usize = 21;
    for page_index in 0..MAX_NEAR_LIMIT_PAGES {
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("near-limit assembly");
        };
        assert!(page.items >= 1);
        assert!(page.items <= SESSION_DELIVERY_MAX_ITEMS);
        assert!(page.bytes <= SESSION_DELIVERY_MAX_BYTES);
        if page.more {
            assert_eq!(receiver.try_iter().count(), 0);
            assert!(!state.assembled_items.is_empty());
            assert!(
                page_index + 1 < MAX_NEAR_LIMIT_PAGES,
                "twenty one-item pages cannot need more than twenty useful calls"
            );
        } else {
            break;
        }
    }
    assert!(state.assembled_items.is_empty());
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        DaemonEntityFrame::Snapshot { items, .. } => {
            assert_eq!(items.len(), 20);
            let encoded = serde_json::to_vec(frames.first().expect("frame"))
                .expect("encode")
                .len();
            assert!(encoded <= DAEMON_MAX_FRAME_BYTES);
        }
        other => panic!("expected complete snapshot, got {other:?}"),
    }
}

#[test]
fn snapshot_size_charges_json_array_separators() {
    assert_eq!(snapshot_separator_bytes(0, 0), 0);
    assert_eq!(snapshot_separator_bytes(0, 1), 0);
    assert_eq!(snapshot_separator_bytes(0, 3), 2);
    assert_eq!(snapshot_separator_bytes(4, 1), 1);
    assert_eq!(snapshot_separator_bytes(4, 2), 2);
}

#[test]
fn separators_close_when_item_bytes_fit_but_commas_do_not() {
    let record = |id: String| SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    };
    let probe = serde_json::to_value(
        crate::session_projection::SessionProjection::project_entity(&record("sep-00".into())),
    )
    .expect("probe");
    let probe_len = serde_json::to_vec(&probe).expect("encode probe").len();
    let envelope = {
        let (sender, _receiver) = mpsc::sync_channel(1);
        snapshot_envelope_bytes(
            "sub",
            &EntitySubscriptionState {
                sender: EntityFrameSender::Blocking(sender),
                entity_type: "session".to_string(),
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: 0,
                definition_version: 0,
                definition_entities: BTreeMap::new(),
                awaiting_initial_snapshot: false,
                resync_reason: None,
                terminating: false,
                owner_grant_id: None,
                package_last_applied_seq: None,
                package_catching_up: false,
                package_delivery: None,
                delivery_after: None,
                delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
                next_seq: 1,
                assembled_items: Vec::new(),
                assembled_item_bytes: 0,
                needs_delivery: true,
            },
        )
    };
    let mut pad = DAEMON_MAX_FRAME_BYTES
        .saturating_sub(envelope)
        .saturating_div(2)
        .saturating_sub(probe_len)
        .saturating_add(64);
    let projection = loop {
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            [
                record(format!("sep-00-{}", "y".repeat(pad))),
                record(format!("sep-01-{}", "y".repeat(pad))),
            ],
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let encoded_items: Vec<_> = projection
            .rows
            .values()
            .map(|row| {
                serde_json::to_value(
                    crate::session_projection::SessionProjection::project_entity(&row.record),
                )
                .expect("item")
            })
            .collect();
        let item_only = encoded_item_bytes(&encoded_items);
        let without_commas = item_only.saturating_add(envelope);
        let with_commas =
            without_commas.saturating_add(snapshot_separator_bytes(0, encoded_items.len()));
        if without_commas <= DAEMON_MAX_FRAME_BYTES && with_commas > DAEMON_MAX_FRAME_BYTES {
            break projection;
        }
        if without_commas > DAEMON_MAX_FRAME_BYTES {
            pad = pad.saturating_sub(8);
        } else {
            pad = pad.saturating_add(1);
        }
        assert!(pad > 32, "failed to find separator boundary pad");
    };
    let (sender, receiver) = mpsc::sync_channel(2);
    let mut state = EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: projection.cursor.clone(),
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
        next_seq: 1,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    };
    let mut counters = DaemonLifecycleCounters::default();
    // This case proves separator accounting, not owner-turn latency.
    // Duration::MAX cannot cut a page, so Closed cannot be elapsed-empty.
    const MAX_SEPARATOR_PAGES: usize = 3;
    let mut closed_too_large = false;
    for page_index in 0..MAX_SEPARATOR_PAGES {
        match continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            8,
            DAEMON_MAX_FRAME_BYTES,
            DAEMON_MAX_FRAME_BYTES,
            Duration::MAX,
        ) {
            SnapshotAssemble::Closed {
                frame_too_large: true,
            } => {
                closed_too_large = true;
                break;
            }
            SnapshotAssemble::Closed {
                frame_too_large: false,
            } => panic!("closed without frame_too_large"),
            SnapshotAssemble::Continue { page } => {
                assert!(page.items > 0, "empty-item continue is not separator proof");
                assert!(page.more, "completed snapshot without charging separators");
                assert!(
                    page_index + 1 < MAX_SEPARATOR_PAGES,
                    "two items cannot need more than two useful pages"
                );
            }
        }
    }
    assert!(closed_too_large, "separator close did not fire");
    let frames: Vec<_> = receiver.try_iter().collect();
    assert!(matches!(
        frames.first(),
        Some(DaemonEntityFrame::Error { code, .. })
            if code == "entity_provider_frame_too_large"
    ));
}

fn assemble_record(id: String) -> SessionLifecycleRecord {
    SessionLifecycleRecord {
        session: botster_core_daemon::DaemonSession {
            session_id: SessionId(id),
            registry_state: RegistrySessionState::Running,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 1,
        },
        metadata: botster_core::CoreSessionMetadata::new(),
        lifecycle: Some(SessionLifecycleState::Running),
    }
}

fn assembling_subscription(
    sender: mpsc::SyncSender<DaemonEntityFrame>,
    source_seq: u64,
) -> EntitySubscriptionState {
    EntitySubscriptionState {
        sender: EntityFrameSender::Blocking(sender),
        entity_type: "session".to_string(),
        cursor: None,
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_version: 0,
        definition_entities: BTreeMap::new(),
        awaiting_initial_snapshot: false,
        resync_reason: None,
        terminating: false,
        owner_grant_id: None,
        package_last_applied_seq: None,
        package_catching_up: false,
        package_delivery: None,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Assembling { source_seq },
        next_seq: source_seq,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    }
}

fn snapshot_item_ids(frame: &DaemonEntityFrame) -> Vec<String> {
    match frame {
        DaemonEntityFrame::Snapshot { items, .. } => items
            .iter()
            .filter_map(|item| {
                item.get("session_uuid")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect(),
        other => panic!("expected snapshot, got {other:?}"),
    }
}

#[test]
fn first_session_snapshot_holds_until_the_projection_is_caught_up() {
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(
        1,
        (0..24).map(|index| assemble_record(format!("session-{index:02}"))),
    );
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 1,
    });
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut state = assembling_subscription(sender, 1);
    let mut counters = DaemonLifecycleCounters::default();
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        false,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_ELAPSED,
    ) else {
        panic!("hold must stay alive");
    };
    assert_eq!(page.items, 0);
    assert!(page.more);
    assert!(state.needs_delivery);
    assert!(matches!(
        state.delivery_phase,
        DeliveryPhase::Assembling { .. }
    ));
    assert!(receiver.try_iter().next().is_none());
}

#[test]
fn first_session_snapshot_completes_when_caught_up() {
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(
        1,
        (0..24).map(|index| assemble_record(format!("session-{index:02}"))),
    );
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 1,
    });
    let (sender, receiver) = mpsc::sync_channel(8);
    let mut state = assembling_subscription(sender, 1);
    let mut counters = DaemonLifecycleCounters::default();
    loop {
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("caught-up assembly must stay alive");
        };
        if !page.more {
            break;
        }
    }
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    assert_eq!(snapshot_item_ids(&frames[0]).len(), 24);
    assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
    assert!(!state.needs_delivery);
}

fn sealed_projection(
    records: impl IntoIterator<Item = SessionLifecycleRecord>,
) -> crate::session_projection::SessionProjection {
    let mut projection = crate::session_projection::SessionProjection::default();
    projection.ingest_baseline_rows(1, records);
    projection.seal_baseline(SessionLifecycleCursor {
        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
        sequence: 1,
    });
    projection
}

fn encoded_session_item(record: &SessionLifecycleRecord) -> usize {
    let value =
        serde_json::to_value(crate::session_projection::SessionProjection::project_entity(record))
            .expect("item");
    serde_json::to_vec(&value).expect("encode").len()
}

fn stub_snapshot_envelope_bytes() -> usize {
    let empty = DaemonEntityFrame::Snapshot {
        subscription_id: String::new(),
        entity_type: "session".to_string(),
        snapshot_seq: 0,
        items: Vec::new(),
        resync_reason: None,
    };
    serde_json::to_vec(&empty).expect("stub envelope").len()
}

fn pad_record_to_item_len(prefix: &str, target_len: usize) -> SessionLifecycleRecord {
    let mut pad = 1usize;
    for _ in 0..target_len.saturating_add(32) {
        let record = assemble_record(format!("{prefix}-{}", "x".repeat(pad)));
        let len = encoded_session_item(&record);
        if len == target_len {
            return record;
        }
        if len < target_len {
            pad = pad.saturating_add(target_len - len);
        } else if pad > 1 {
            pad -= 1;
        } else {
            panic!("cannot hit item len {target_len}, got {len}");
        }
    }
    panic!("failed to find pad for item len {target_len}");
}

#[test]
fn snapshot_item_budget_cuts_and_resumes() {
    let projection = sealed_projection([
        assemble_record("session-00".into()),
        assemble_record("session-01".into()),
    ]);
    let (sender, _receiver) = mpsc::sync_channel(4);
    let state = assembling_subscription(sender, 1);
    let envelope = snapshot_envelope_bytes("sub", &state);
    let first = take_snapshot_item_page(
        &projection,
        None,
        0,
        envelope,
        1,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    );
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.cut, SnapshotPageCut::ItemBudget);
    let second = take_snapshot_item_page(
        &projection,
        first.last_id.as_deref(),
        first.items.len(),
        envelope,
        1,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    );
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.cut, SnapshotPageCut::Complete);
    assert_ne!(
        first.items[0].get("session_uuid"),
        second.items[0].get("session_uuid")
    );
}

#[test]
fn snapshot_byte_budget_includes_envelope_and_yields_empty_cuts() {
    let first_record = assemble_record("session-00".into());
    let second_record = assemble_record("session-01".into());
    let c1 = encoded_session_item(&first_record);
    let projection = sealed_projection([first_record, second_record]);
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut state = assembling_subscription(sender, 1);
    let envelope = snapshot_envelope_bytes("sub", &state);
    let mut counters = DaemonLifecycleCounters::default();

    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        envelope.saturating_add(c1),
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    ) else {
        panic!("exact envelope+c1 must yield, not close");
    };
    assert_eq!(page.items, 1);
    assert!(page.more);
    assert_eq!(page.bytes, envelope.saturating_add(c1));
    assert!(receiver.try_iter().next().is_none());

    let mut empty_state = assembling_subscription(mpsc::sync_channel(4).0, 1);
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut empty_state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        envelope.saturating_add(c1).saturating_sub(1),
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    ) else {
        panic!("envelope+c1-1 must yield empty, not close");
    };
    assert_eq!(page.items, 0);
    assert!(page.more);
    assert!(empty_state.needs_delivery);
    assert!(matches!(
        empty_state.delivery_phase,
        DeliveryPhase::Assembling { .. }
    ));

    let mut no_envelope_state = assembling_subscription(mpsc::sync_channel(4).0, 1);
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut no_envelope_state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        c1,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    ) else {
        panic!("c1 without envelope headroom must yield empty, not close");
    };
    assert_eq!(page.items, 0);
    assert!(page.more);
    assert!(no_envelope_state.needs_delivery);
}

#[test]
fn snapshot_page_charges_the_real_envelope_not_a_stub() {
    let first_record = assemble_record("session-00".into());
    let c1 = encoded_session_item(&first_record);
    let projection = sealed_projection([first_record, assemble_record("session-01".into())]);
    let (sender, _receiver) = mpsc::sync_channel(4);
    let mut state = assembling_subscription(sender, 1);
    state.next_seq = 7;
    state.resync_reason = Some("catch_up".into());
    let real_envelope = snapshot_envelope_bytes("sub", &state);
    let stub_envelope = stub_snapshot_envelope_bytes();
    assert!(real_envelope > stub_envelope);
    assert_eq!(real_envelope, snapshot_envelope_bytes("sub", &state));

    let stub_fit = take_snapshot_item_page(
        &projection,
        None,
        0,
        real_envelope,
        SESSION_DELIVERY_MAX_ITEMS,
        stub_envelope.saturating_add(c1),
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    );
    assert!(stub_fit.items.is_empty());
    assert_eq!(stub_fit.cut, SnapshotPageCut::ByteBudget);

    let real_fit = take_snapshot_item_page(
        &projection,
        None,
        0,
        real_envelope,
        SESSION_DELIVERY_MAX_ITEMS,
        real_envelope.saturating_add(c1),
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    );
    assert_eq!(real_fit.items.len(), 1);
    assert_eq!(real_fit.cut, SnapshotPageCut::ByteBudget);
    assert_eq!(real_fit.bytes, real_envelope.saturating_add(c1));
}

#[test]
fn oversized_row_uses_the_fresh_page_capacity_parameter() {
    let record = pad_record_to_item_len("big", SESSION_DELIVERY_MAX_BYTES.saturating_sub(8));
    let item_len = encoded_session_item(&record);
    let projection = sealed_projection([record]);
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut state = assembling_subscription(sender, 1);
    let envelope = snapshot_envelope_bytes("sub", &state);
    assert!(envelope.saturating_add(item_len) > SESSION_DELIVERY_MAX_BYTES);
    let mut counters = DaemonLifecycleCounters::default();
    let closed = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    );
    assert!(matches!(
        closed,
        SnapshotAssemble::Closed {
            frame_too_large: true
        }
    ));
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    assert!(matches!(
        &frames[0],
        DaemonEntityFrame::Error { code, .. } if code == "entity_provider_frame_too_large"
    ));

    let (sender, receiver) = mpsc::sync_channel(4);
    let mut roomy = assembling_subscription(sender, 1);
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut roomy,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        DAEMON_MAX_FRAME_BYTES,
        DAEMON_MAX_FRAME_BYTES,
        Duration::MAX,
    ) else {
        panic!("larger capacity must accept the same row");
    };
    assert_eq!(page.items, 1);
    assert!(!page.more);
    assert_eq!(
        snapshot_item_ids(&receiver.try_iter().next().expect("snapshot")).len(),
        1
    );
}

#[test]
fn later_page_separator_boundary_closes_oversized_without_livelock() {
    let first = assemble_record("session-00".into());
    let envelope = {
        let (sender, _receiver) = mpsc::sync_channel(1);
        snapshot_envelope_bytes("sub", &assembling_subscription(sender, 1))
    };
    let oversized = pad_record_to_item_len(
        "session-01",
        SESSION_DELIVERY_MAX_BYTES.saturating_sub(envelope),
    );
    assert_eq!(
        envelope.saturating_add(encoded_session_item(&oversized)),
        SESSION_DELIVERY_MAX_BYTES
    );
    let projection = sealed_projection([first.clone(), oversized]);
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut state = assembling_subscription(sender, 1);
    let mut counters = DaemonLifecycleCounters::default();
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        true,
        &mut counters,
        1,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    ) else {
        panic!("first item must assemble");
    };
    assert_eq!(page.items, 1);
    assert!(page.more);
    let closed = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    );
    assert!(matches!(
        closed,
        SnapshotAssemble::Closed {
            frame_too_large: true
        }
    ));
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    assert!(matches!(
        &frames[0],
        DaemonEntityFrame::Error { code, .. } if code == "entity_provider_frame_too_large"
    ));

    let admitted = pad_record_to_item_len(
        "session-01",
        SESSION_DELIVERY_MAX_BYTES
            .saturating_sub(envelope)
            .saturating_sub(1),
    );
    assert!(
        envelope
            .saturating_add(encoded_session_item(&admitted))
            .saturating_add(1)
            <= SESSION_DELIVERY_MAX_BYTES
    );
    let control = sealed_projection([first, admitted]);
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut state = assembling_subscription(sender, 1);
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &control,
        true,
        &mut counters,
        1,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    ) else {
        panic!("control first item");
    };
    assert_eq!(page.items, 1);
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &control,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    ) else {
        panic!("control second item must fit with comma headroom");
    };
    assert_eq!(page.items, 1);
    assert!(!page.more);
    assert_eq!(
        snapshot_item_ids(&receiver.try_iter().next().expect("snapshot")).len(),
        2
    );
}

#[test]
fn empty_elapsed_cut_yields_instead_of_closing() {
    let projection = sealed_projection([
        assemble_record("session-00".into()),
        assemble_record("session-01".into()),
    ]);
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut state = assembling_subscription(sender, 1);
    let mut counters = DaemonLifecycleCounters::default();
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::ZERO,
    ) else {
        panic!("elapsed empty cut must yield, not close");
    };
    assert_eq!(page.items, 0);
    assert!(page.more);
    assert!(state.needs_delivery);
    assert!(matches!(
        state.delivery_phase,
        DeliveryPhase::Assembling { .. }
    ));
    assert!(receiver.try_iter().next().is_none());

    loop {
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_BYTES,
            Duration::MAX,
        ) else {
            panic!("later full-budget call must complete");
        };
        if !page.more {
            break;
        }
    }
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    assert_eq!(snapshot_item_ids(&frames[0]).len(), 2);
    assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
    assert!(!state.needs_delivery);
}

#[test]
fn empty_snapshot_yields_when_remaining_budget_is_below_envelope() {
    let projection = sealed_projection([]);
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut state = assembling_subscription(sender, 1);
    let envelope = snapshot_envelope_bytes("sub", &state);
    assert!(envelope > 1);
    let mut counters = DaemonLifecycleCounters::default();
    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        envelope.saturating_sub(1),
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    ) else {
        panic!("remaining envelope-1 must yield, not send");
    };
    assert_eq!(page.items, 0);
    assert!(page.more);
    assert!(state.needs_delivery);
    assert!(matches!(
        state.delivery_phase,
        DeliveryPhase::Assembling { .. }
    ));
    assert!(receiver.try_iter().next().is_none());

    let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
        "sub",
        &mut state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    ) else {
        panic!("full-budget empty projection must complete");
    };
    assert_eq!(page.items, 0);
    assert!(!page.more);
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        DaemonEntityFrame::Snapshot { items, .. } => assert!(items.is_empty()),
        other => panic!("expected one empty snapshot, got {other:?}"),
    }
    assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
    assert!(!state.needs_delivery);
}

#[test]
fn empty_oversized_envelope_closes_instead_of_yielding_forever() {
    let projection = sealed_projection([]);
    let subscription_id = "e".repeat(SESSION_DELIVERY_MAX_BYTES);
    let (sender, receiver) = mpsc::sync_channel(4);
    let mut state = assembling_subscription(sender, 1);
    let envelope = snapshot_envelope_bytes(&subscription_id, &state);
    assert!(envelope > SESSION_DELIVERY_MAX_BYTES);
    let mut counters = DaemonLifecycleCounters::default();
    let closed = continue_session_snapshot_assembly(
        &subscription_id,
        &mut state,
        &projection,
        true,
        &mut counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_BYTES,
        Duration::MAX,
    );
    assert!(matches!(
        closed,
        SnapshotAssemble::Closed {
            frame_too_large: true
        }
    ));
    let frames: Vec<_> = receiver.try_iter().collect();
    assert_eq!(frames.len(), 1);
    assert!(matches!(
        &frames[0],
        DaemonEntityFrame::Error { code, .. } if code == "entity_provider_frame_too_large"
    ));
    assert!(!state.needs_delivery);
    assert!(matches!(state.delivery_phase, DeliveryPhase::Removes));
}

#[test]
fn exhausted_budget_preserves_catalog_capacity_release() {
    let directory = std::env::temp_dir().join(format!(
        "botster-catalog-capacity-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the system clock follows the epoch")
            .as_nanos()
    ));
    let config = crate::HubStartupOptions {
        host: crate::HostIdentityOptions {
            id: "catalog-capacity-test".into(),
            display_name: "Catalog Capacity Test".into(),
            fingerprint: None,
        },
        data_directory: crate::DataDirectoryOption::Explicit(directory.clone()),
        ..crate::HubStartupOptions::default()
    }
    .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
    .expect("build the catalog capacity configuration");
    let mut daemon = HubDaemon::start(config).expect("start the catalog capacity daemon");
    let executor = daemon
        .runtime()
        .expect("the runtime is active")
        .host_executor();
    let mut permits = (0..crate::host_executor::HOST_OPERATION_CAPACITY)
        .map(|_| executor.try_reserve().expect("reserve a host operation"))
        .collect::<Vec<_>>();
    let mut state = DaemonControlState::default();
    let delivery = crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery;
    state.maintenance.wakes.take(delivery);
    assert!(matches!(
        state
            .session_type_catalog
            .refresh(&daemon, 1, &state.waiter_ids),
        SessionTypeCatalogRefresh::Pending
    ));
    assert!(state.session_type_catalog.waiting_for_capacity());
    drop(permits.pop());
    assert!(executor.take_capacity_notification());
    state.host_capacity_wake_pending = true;
    let now = Instant::now();
    let mut budget = OwnerTurnBudget::new(now);
    budget
        .try_charge(
            now,
            OwnerTurnCharge::inspection(crate::daemon::owner_turn::OWNER_TURN_INSPECTED_BYTE_LIMIT),
        )
        .expect("consume the byte budget");
    publish_catalog_capacity_wake(&mut state, &mut budget);
    assert!(state.host_capacity_wake_pending);
    assert!(!state.maintenance.wakes.take(delivery));

    let mut fresh = OwnerTurnBudget::new(Instant::now());
    publish_catalog_capacity_wake(&mut state, &mut fresh);
    assert!(!state.host_capacity_wake_pending);
    assert!(state.maintenance.wakes.take(delivery));
    assert!(matches!(
        state
            .session_type_catalog
            .refresh(&daemon, 1, &state.waiter_ids),
        SessionTypeCatalogRefresh::Pending
    ));
    assert!(state.session_type_catalog.pending.is_some());
    assert!(!state.session_type_catalog.waiting_for_capacity());
    publish_catalog_capacity_wake(&mut state, &mut fresh);
    assert!(!state.maintenance.wakes.take(delivery));
    drop(permits);
    daemon.stop();
    std::fs::remove_dir_all(directory).expect("remove the catalog capacity directory");
}

fn catalog_test_daemon(label: &str) -> (HubDaemon, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!(
        "botster-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the system clock follows the epoch")
            .as_nanos()
    ));
    let config = crate::HubStartupOptions {
        host: crate::HostIdentityOptions {
            id: format!("{label}-test"),
            display_name: "Catalog Observation Test".into(),
            fingerprint: None,
        },
        data_directory: crate::DataDirectoryOption::Explicit(directory.clone()),
        ..crate::HubStartupOptions::default()
    }
    .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
    .expect("build the catalog observation configuration");
    (
        HubDaemon::start(config).expect("start the catalog observation daemon"),
        directory,
    )
}

#[test]
fn observed_catalog_publishes_the_cached_result_and_submits_one_follow_up() {
    let (mut daemon, directory) = catalog_test_daemon("catalog-observation");
    let mut state = DaemonControlState::default();
    let entities = BTreeMap::from([("agent".to_string(), serde_json::json!({"id": "agent"}))]);
    state.session_type_catalog.generation = Some(1);
    state.session_type_catalog.entities = entities.clone();
    state.session_type_catalog.version = 1;

    // No observation: the cache is current and no build starts.
    assert!(matches!(
        state.session_type_catalog.refresh(&daemon, 1, &state.waiter_ids),
        SessionTypeCatalogRefresh::Ready(1, 1, published) if published == &entities
    ));
    assert!(state.session_type_catalog.pending.is_none());

    // A read observed the repository: the cached result is still
    // published, and one follow-up build starts under that observation.
    state.session_type_catalog.observe_external();
    assert!(matches!(
        state.session_type_catalog.refresh(&daemon, 1, &state.waiter_ids),
        SessionTypeCatalogRefresh::Ready(1, 1, published) if published == &entities
    ));
    let first = state.session_type_catalog.pending.expect("follow-up build");
    assert_eq!(state.session_type_catalog.pending_observation, 1);

    // Reads that continue while it runs keep publishing and start no
    // second build; the newer observation stays recorded for later.
    state.session_type_catalog.observe_external();
    assert!(matches!(
        state
            .session_type_catalog
            .refresh(&daemon, 1, &state.waiter_ids),
        SessionTypeCatalogRefresh::Ready(1, 1, _)
    ));
    assert_eq!(state.session_type_catalog.pending, Some(first));
    assert_eq!(state.session_type_catalog.observation, 2);
    assert_eq!(state.session_type_catalog.pending_observation, 1);
    daemon.stop();
    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn observed_catalog_publishes_a_cached_failure_before_its_follow_up() {
    let (mut daemon, directory) = catalog_test_daemon("catalog-failure-observation");
    let mut state = DaemonControlState::default();
    state.session_type_catalog.failure =
        Some((1, HostError::new("invalid_repo_session_types", "invalid")));
    state.session_type_catalog.observe_external();
    assert!(matches!(
        state.session_type_catalog.refresh(&daemon, 1, &state.waiter_ids),
        SessionTypeCatalogRefresh::Failed(0, ref error) if error.code == "invalid_repo_session_types"
    ));
    // The follow-up is pending; a second read returns the same failure
    // version, so no subscriber receives it twice.
    assert!(matches!(
        state
            .session_type_catalog
            .refresh(&daemon, 1, &state.waiter_ids),
        SessionTypeCatalogRefresh::Failed(0, _)
    ));
    assert!(state.session_type_catalog.pending.is_some());
    daemon.stop();
    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn same_generation_rebuild_delivers_changes_only_for_a_new_version() {
    let (sender, receiver) = mpsc::sync_channel(8);
    let before = BTreeMap::from([("agent".to_string(), serde_json::json!({"label": "old"}))]);
    let after = BTreeMap::from([("agent".to_string(), serde_json::json!({"label": "new"}))]);
    let mut subscriptions = BTreeMap::from([(
        "session-types".to_string(),
        session_type_subscription_state(sender, 1, 1, before, None),
    )]);
    drive_session_type_subscriptions(&mut subscriptions, 1, 0, &after);
    assert!(
        receiver.try_recv().is_err(),
        "the delivered version must not redeliver"
    );
    drive_session_type_subscriptions(&mut subscriptions, 1, 1, &after);
    assert!(matches!(
        receiver.try_recv(),
        Ok(DaemonEntityFrame::Upsert { ref id, ref entity, .. })
            if id == "agent" && entity["label"] == "new"
    ));
}

/// Build A is in flight when read B observes a poisoned repository. A's
/// result must still publish, build B must follow, and B's failure must
/// reach the held subscriber as an entity error. Reads keep arriving.
#[test]
fn observation_during_a_build_publishes_it_then_delivers_the_follow_up_failure() {
    let (mut daemon, directory) = catalog_test_daemon("catalog-observation-race");
    let mut state = DaemonControlState::default();
    let (sender, receiver) = mpsc::sync_channel(8);
    state.entity_subscriptions.insert(
        "session-types".to_string(),
        session_type_subscription_state(sender, 0, 0, BTreeMap::new(), None),
    );

    // Build A starts under observation 0.
    assert!(matches!(
        state
            .session_type_catalog
            .refresh(&daemon, 1, &state.waiter_ids),
        SessionTypeCatalogRefresh::Pending
    ));
    let (build_a, _) = state.session_type_catalog.pending.expect("build A");
    assert_eq!(state.session_type_catalog.pending_observation, 0);

    // Read B observes the repository while A runs.
    state.session_type_catalog.observe_external();
    let executor = daemon.runtime().expect("runtime").host_executor();
    let a_entities = BTreeMap::from([("agent".to_string(), serde_json::json!({"label": "a"}))]);
    assert!(state.session_type_catalog.absorb(
        HostCompletion::for_test(
            build_a,
            HostResult::SessionTypeCatalogReady {
                generation: 1,
                entities: a_entities.clone(),
                logical_bytes: 1,
            },
            executor.try_reserve().expect("reserve build A completion"),
        ),
        executor,
    ));
    assert_eq!(state.session_type_catalog.built_observation, 0);

    // A publishes, and follow-up build B starts under observation 1.
    let published = match state
        .session_type_catalog
        .refresh(&daemon, 1, &state.waiter_ids)
    {
        SessionTypeCatalogRefresh::Ready(generation, version, entities) => {
            (generation, version, entities.clone())
        }
        _ => panic!("build A must publish"),
    };
    assert_eq!(published.2, a_entities);
    drive_session_type_subscriptions(
        &mut state.entity_subscriptions,
        published.0,
        published.1,
        &published.2,
    );
    assert!(matches!(
        receiver.try_recv(),
        Ok(DaemonEntityFrame::Upsert { ref id, .. }) if id == "agent"
    ));
    let (build_b, _) = state
        .session_type_catalog
        .pending
        .expect("follow-up build B");
    assert_ne!(build_b, build_a);
    assert_eq!(state.session_type_catalog.pending_observation, 1);

    // Reads continue while B runs; B's poison result then publishes.
    state.session_type_catalog.observe_external();
    assert!(state.session_type_catalog.absorb(
        HostCompletion::for_test(
            build_b,
            HostResult::Failed {
                generation: 1,
                error: HostError::new("invalid_repo_session_types", "invalid repository file"),
            },
            executor.try_reserve().expect("reserve build B completion"),
        ),
        executor,
    ));
    let failure = match state
        .session_type_catalog
        .refresh(&daemon, 1, &state.waiter_ids)
    {
        SessionTypeCatalogRefresh::Failed(version, error) => (version, error),
        _ => panic!("build B's failure must publish"),
    };
    assert!(!drive_session_type_catalog_failure(
        &mut state.entity_subscriptions,
        failure.0,
        &failure.1.code,
        &failure.1.message,
    ));
    assert!(matches!(
        receiver.try_recv(),
        Ok(DaemonEntityFrame::Error { ref code, .. }) if code == "invalid_repo_session_types"
    ));
    assert!(
        state.entity_subscriptions.contains_key("session-types"),
        "the catalog error is not terminal"
    );
    // The observation made while B ran starts one more build.
    assert_eq!(state.session_type_catalog.pending_observation, 2);
    daemon.stop();
    let _ = std::fs::remove_dir_all(directory);
}
