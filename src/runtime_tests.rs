use super::*;

/// A reader keeps the view it took; a later publish replaces only the
/// current view.
#[test]
fn package_publication_readers_keep_their_admitted_view_across_a_publish() {
    let publication = package_publication_for_test(Vec::new());
    let admitted = publication.current();
    let next = package_view_for_test(Vec::new());
    publication.publish(next.clone());
    let current = publication.current();
    assert!(SharedView::ptr_eq(&current, &next));
    assert!(!SharedView::ptr_eq(&current, &admitted));
    assert!(
        admitted.packages().is_empty(),
        "the admitted view stays readable"
    );
}
use crate::{
    DataDirectoryOption, HostIdentityOptions, HubStartupOptions, RuntimeEnvironment,
    SessionDefaults, TransportBindings,
};

#[test]
fn tracker_keeps_accepted_identity_after_success_without_pending_identity() {
    let id = PendingOperationId(73);
    let mut tracker = CoreOperationTracker {
        stage: CoreOperationStage::Begin {
            ticket: CoreTicket::resolved(Ok(id)),
            completion: CoreTicket::resolved(CoreCompletion::RemoveSession {
                id,
                result: Ok(true),
            }),
        },
        accepted_id: None,
    };
    assert_eq!(tracker.accepted_id(), None);
    assert_eq!(tracker.pending_id(), None);
    assert!(matches!(
        tracker.poll_without_reaping(),
        CoreTicketPoll::Ready(Ok(CoreCompletion::RemoveSession { id: actual, .. }))
            if actual == id
    ));
    assert_eq!(tracker.accepted_id(), Some(id));
    assert_eq!(tracker.pending_id(), None);
}

#[test]
fn tracker_begin_error_does_not_create_an_accepted_identity() {
    let mut tracker = CoreOperationTracker {
        stage: CoreOperationStage::Begin {
            ticket: CoreTicket::resolved(Err(CoreDaemonError::Shutdown)),
            completion: CoreTicket::resolved(CoreCompletion::RemoveSession {
                id: PendingOperationId(74),
                result: Ok(true),
            }),
        },
        accepted_id: None,
    };
    assert!(matches!(
        tracker.poll_without_reaping(),
        CoreTicketPoll::Ready(Err(CoreDaemonError::Shutdown))
    ));
    assert_eq!(tracker.accepted_id(), None);
    assert_eq!(tracker.pending_id(), None);
}

#[test]
fn tracker_pre_admission_refusal_has_no_accepted_identity() {
    let runtime = family_runtime("tracker-refused-before-admission");
    runtime.test_refuse_next_owner_begins(1);
    let waiter_id = runtime.next_waiter_id().unwrap();
    let mut tracker = runtime.begin_reserve_session_for_owner(
        waiter_id,
        SessionId("tracker-refused-before-admission".into()),
    );
    assert!(matches!(tracker.poll(&runtime), CoreTicketPoll::Refused));
    assert_eq!(tracker.accepted_id(), None);
    assert_eq!(tracker.pending_id(), None);
}

#[test]
fn tracker_pre_admission_loss_has_no_accepted_identity() {
    let runtime = family_runtime("tracker-lost-before-admission");
    runtime.test_lose_next_owner_begins(1);
    let waiter_id = runtime.next_waiter_id().unwrap();
    let mut tracker = runtime.begin_reserve_session_for_owner(
        waiter_id,
        SessionId("tracker-lost-before-admission".into()),
    );
    assert!(matches!(tracker.poll(&runtime), CoreTicketPoll::Lost));
    assert_eq!(tracker.accepted_id(), None);
    assert_eq!(tracker.pending_id(), None);
}

#[test]
fn tracker_layout_reports_selected_type_sizes() {
    fn report<T>(name: &str) {
        println!(
            "{name}: size={} align={}",
            std::mem::size_of::<T>(),
            std::mem::align_of::<T>()
        );
    }
    report::<CoreOperationTracker>("CoreOperationTracker");
    report::<Option<CoreOperationTracker>>("Option<CoreOperationTracker>");
    report::<PendingOperationId>("PendingOperationId");
    report::<Option<PendingOperationId>>("Option<PendingOperationId>");
    report::<CreatedWorktreeCleanup>("CreatedWorktreeCleanup");
    report::<InflightPluginCore>("InflightPluginCore");
    report::<ManagedSessionSpawnStart>("ManagedSessionSpawnStart");
    report::<SessionTypeSpawnStart>("SessionTypeSpawnStart");
    report::<Mutex<CoreOperationTracker>>("Mutex<CoreOperationTracker>");
    println!(
        "Arc storage for Mutex<CoreOperationTracker>: {}",
        crate::lua_memory::layout::arc_bytes::<Mutex<CoreOperationTracker>>()
    );
}

pub(super) fn family_runtime(name: &str) -> HubRuntime {
    static NEXT_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let directory_id = NEXT_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let config = HubStartupOptions {
        host: HostIdentityOptions {
            id: name.to_string(),
            display_name: name.to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(
            std::env::temp_dir().join(format!("{name}-{}-{directory_id}", std::process::id())),
        ),
        session_defaults: SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: TransportBindings::default(),
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .unwrap();
    let runtime = HubRuntime::new(config).unwrap();
    // Timer tests drain at explicit logical times that start at zero.
    runtime.clock().make_logical(0, 0);
    runtime
}

fn occupy_inflight_and_freeze_remainder(
    runtime: &HubRuntime,
    topic: &str,
) -> crate::lua_memory::LuaCallbackCharge {
    let bridge = runtime.coordination_bridge();
    bridge.test_queue_pending(PendingCoordinationOperation::Drain {
        target: EnvelopeTarget::Topic {
            topic: topic.into(),
        },
        after: None,
        limit: 1,
    });
    runtime.test_fulfill_pending_coordination_requests();
    assert_eq!(runtime.test_coordination_core_submits(), 1);
    let memory = runtime.test_lua_memory();
    let used = memory.usage().1;
    let hold = memory
        .limits()
        .total_callback_bytes
        .checked_sub(used)
        .expect("callback budget remains after one inflight slot");
    memory.reserve_shared_callback_storage(hold).unwrap()
}

#[test]
fn inflight_coordination_refuses_before_core_submit() {
    let runtime = family_runtime("inflight-coord-cap");
    let _hold = occupy_inflight_and_freeze_remainder(&runtime, "inflight-cap");
    let bridge = runtime.coordination_bridge();
    let receiver = bridge.test_queue_pending(PendingCoordinationOperation::Drain {
        target: EnvelopeTarget::Topic {
            topic: "inflight-cap-2".into(),
        },
        after: None,
        limit: 1,
    });
    runtime.test_fulfill_pending_coordination_requests();
    assert_eq!(runtime.test_coordination_core_submits(), 1);
    assert_eq!(bridge.test_pending_count(), 0);
    let reply = receiver.try_recv().expect("capacity refusal is immediate");
    assert!(
        matches!(
            reply,
            Err(crate::lua_runtime::CoordinationFailure::NonAcknowledge(ref message))
                if message == crate::lua_memory::LUA_CALLBACK_CAPACITY_EXHAUSTED
        ),
        "unexpected reply: {reply:?}"
    );
}

#[test]
fn lua_host_api_clones_share_the_runtime_memory_account() {
    let runtime = family_runtime("lua-memory-new");
    let api = runtime.lua_plugin_host_api();
    let cloned_api = api.clone();
    assert!(Arc::ptr_eq(&runtime.lua_memory, &api.memory));
    assert!(Arc::ptr_eq(&api.memory, &cloned_api.memory));
    let charge = api.memory.reserve_vm().unwrap();
    assert_eq!(
        cloned_api.memory.usage().0,
        crate::config::lua_memory_limits().per_vm_bytes
    );
    drop(charge);
    assert_eq!(runtime.lua_memory.usage(), (0, 0));
}

#[test]
fn context_alias_replacement_retains_each_generation_until_its_last_alias() {
    let runtime = family_runtime("context-alias-generation");
    let first = HubSessionContext {
        context_id: "ctx-first".into(),
        session_id: SessionId("same-session".into()),
        values: BTreeMap::from([("value".into(), "first".into())]),
    };
    let second = HubSessionContext {
        context_id: "ctx-second".into(),
        session_id: first.session_id.clone(),
        values: BTreeMap::from([("value".into(), "second".into())]),
    };
    let first_identity = runtime.test_publish_spawn_context(&first);
    let first_charge = runtime.lua_memory.usage().1;
    let second_identity = runtime.test_publish_spawn_context(&second);
    assert_ne!(first_identity, second_identity);
    assert_eq!(
        runtime.session_context("same-session"),
        Some(second.clone())
    );
    assert_eq!(runtime.session_context("ctx-first"), Some(first.clone()));
    assert!(runtime.lua_memory.usage().1 > first_charge);

    runtime.retract_spawn_context(&first, first_identity);
    assert_eq!(runtime.session_context("ctx-first"), None);
    assert_eq!(
        runtime.session_context("same-session"),
        Some(second.clone())
    );
    assert!(runtime.lua_memory.usage().1 > 0);

    runtime.retract_spawn_context(&second, second_identity);
    assert_eq!(runtime.session_context("same-session"), None);
    assert_eq!(runtime.lua_memory.usage().1, 0);
}

#[test]
fn restored_runtime_shares_its_own_lua_memory_account() {
    let first = family_runtime("lua-memory-restored");
    let config = first.config().clone();
    let first_account = Arc::clone(&first.lua_memory);
    drop(first);
    let state = HubState::from_config(&config);
    let restored = HubRuntime::from_validated_state(config, state).unwrap();
    let api = restored.lua_plugin_host_api();
    assert!(!Arc::ptr_eq(&first_account, &restored.lua_memory));
    assert!(Arc::ptr_eq(&restored.lua_memory, &api.memory));
    assert_eq!(api.memory.limits(), crate::config::lua_memory_limits());
}

fn check_terminal_spawner_disposal(poison: bool) {
    use crate::host_disposal::{Job, Parts, Poll};
    use crate::host_executor::{
        HOST_OPERATION_CAPACITY, HOST_PREPARED_BYTE_CAPACITY, HostJobIdentity,
    };

    let name = if poison {
        "terminal-spawner-poison"
    } else {
        "terminal-spawner"
    };
    let mut runtime = family_runtime(name);
    let spawner = runtime.session_type_spawner();
    let (release, gate) = mpsc::channel();
    let observed = spawner.test_seed_terminal_pending(Some(gate));
    if poison {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _pending = spawner.pending.lock().unwrap();
            let _managed = spawner.managed.lock().unwrap();
            panic!("poison the nonempty spawner queues");
        }));
        assert!(result.is_err());
        assert!(spawner.pending.is_poisoned());
        assert!(spawner.managed.is_poisoned());
    }
    let permit = runtime.host_executor().try_reserve().unwrap();
    let spare_permits = (1..HOST_OPERATION_CAPACITY)
        .map(|_| runtime.host_executor().try_reserve().unwrap())
        .collect::<Vec<_>>();
    assert!(runtime.host_executor().try_reserve().is_none());
    let identity = HostJobIdentity::first(crate::owner_identity::WaiterId(71));
    let lifecycle = runtime.take_plugin_lifecycle().unwrap();
    let mut engine_job = Job::new(Parts {
        storage: None,
        identity,
        permit,
        payload: Box::new(lifecycle),
        model: None,
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let permit = loop {
        match engine_job.poll() {
            Poll::Disposed(permit) => break permit,
            Poll::Pending => assert!(Instant::now() < deadline),
            _ => panic!("engine disposal must return its original permit"),
        }
        thread::yield_now();
    };
    assert_eq!(spawner.test_terminal_pending_counts(), (1, 1, true));
    assert!(matches!(
        observed.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    let mut bridge_job = Job::new_plugin_bridges(
        Parts {
            storage: None,
            identity,
            permit,
            payload: Box::new(()),
            model: None,
        },
        runtime.terminal_plugin_bridges(),
    );
    let (thread_name, locks_released) = observed.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(thread_name.starts_with("botster-hub-host"));
    assert!(locks_released);
    assert_eq!(spawner.test_terminal_pending_counts(), (0, 0, true));
    assert!(matches!(bridge_job.poll(), Poll::Pending));
    assert_eq!(
        runtime.host_executor().outstanding(),
        HOST_OPERATION_CAPACITY
    );
    assert_eq!(
        runtime.host_executor().prepared_bytes(),
        HOST_OPERATION_CAPACITY * HOST_PREPARED_BYTE_CAPACITY
    );
    assert!(runtime.host_executor().try_reserve().is_none());
    release.send(()).unwrap();
    let (thread_name, locks_released) = observed.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(thread_name.starts_with("botster-hub-host"));
    assert!(locks_released);
    let deadline = Instant::now() + Duration::from_secs(5);
    let permit = loop {
        match bridge_job.poll() {
            Poll::Disposed(permit) => break permit,
            Poll::Pending => assert!(Instant::now() < deadline),
            _ => {
                panic!("bridge disposal must return the same permit after payload destruction")
            }
        }
        thread::yield_now();
    };
    assert_eq!(spawner.test_terminal_pending_counts(), (0, 0, false));
    assert!(matches!(
        observed.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
    assert_eq!(
        runtime.host_executor().outstanding(),
        HOST_OPERATION_CAPACITY
    );
    drop(spare_permits);
    assert_eq!(runtime.host_executor().outstanding(), 1);
    assert_eq!(
        runtime.host_executor().prepared_bytes(),
        HOST_PREPARED_BYTE_CAPACITY
    );
    drop(permit);
    assert_eq!(runtime.host_executor().outstanding(), 0);
    assert_eq!(runtime.host_executor().prepared_bytes(), 0);
    runtime.release_for_restart();
    drop(runtime);
    let root = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn terminal_spawner_queues_clear_on_host_before_the_original_slot_returns() {
    check_terminal_spawner_disposal(false);
}

#[test]
fn terminal_spawner_poisoned_queues_clear_on_host_before_the_original_slot_returns() {
    check_terminal_spawner_disposal(true);
}

#[test]
fn causal_fifo_preserves_transfer_before_release_at_capacity() {
    let runtime = family_runtime("causal-finish-fifo");
    let pending = LeaseIdentity::PendingEntityPublish {
        publication_token: 0,
    };
    let admitted = LeaseIdentity::AdmittedEntityMutation {
        family_token: 1,
        seq: 1,
    };
    let scope_id = runtime
        .causal_scopes
        .mint_with_lease(Some(pending.clone()))
        .unwrap();
    for _ in 0..CAUSAL_OWNER_CAPACITY - 2 {
        assert!(matches!(
            runtime.admit_causal_op(CausalOp::Release {
                scope_id: 0,
                identity: pending.clone(),
            }),
            CausalAdmitResult::Applied
        ));
    }
    assert!(matches!(
        runtime.admit_causal_op(CausalOp::Transfer {
            scope_id,
            from: pending,
            to: [Some(admitted.clone()), None, None],
        }),
        CausalAdmitResult::Applied
    ));
    assert!(matches!(
        runtime.admit_causal_op(CausalOp::Release {
            scope_id,
            identity: admitted
        }),
        CausalAdmitResult::Applied
    ));
    let deadline = Instant::now() + Duration::from_secs(3);
    while runtime.causal_owner_ops_pending() {
        runtime.apply_causal_owner_ops();
        assert!(Instant::now() < deadline, "the finish FIFO must drain");
    }
    assert!(
        runtime.causal_scopes.identities(scope_id).is_none(),
        "a later release must not overtake its transfer"
    );
}

#[test]
fn causal_finish_fifo_moves_one_operation_per_owner_phase() {
    let runtime = family_runtime("causal-finish-phase");
    let mut scopes = Vec::new();
    for _ in 0..6 {
        let identity = LeaseIdentity::EventInFlight;
        let scope_id = runtime
            .causal_scopes
            .mint_with_lease(Some(identity.clone()))
            .unwrap();
        scopes.push(scope_id);
        assert!(matches!(
            runtime.admit_causal_op(CausalOp::Release { scope_id, identity }),
            CausalAdmitResult::Applied
        ));
    }
    let mut previous = runtime.causal_operation_count();
    for _ in 0..128 {
        runtime.apply_causal_owner_ops();
        let remaining = runtime.causal_operation_count();
        assert!(
            previous - remaining <= 1,
            "one phase must not drain multiple finish operations"
        );
        previous = remaining;
    }
    assert_eq!(runtime.causal_operation_count(), 0);
    for scope in scopes {
        assert!(runtime.causal_scopes.identities(scope).is_none());
    }
}

#[test]
fn family_cleanup_finds_old_fanout_without_live_state() {
    let runtime = family_runtime("orphan-fanout-cleanup");
    let family = "producer.item";
    let scope = runtime
        .causal_scopes
        .mint_with_lease(Some(LeaseIdentity::EventInFlight))
        .unwrap();
    for (generation, family_token) in [(0, 10), (1, 20)] {
        let lease = EntityMutationLease {
            admission: None,
            family_token,
            scope_id: scope,
            family: family.into(),
            generation,
            seq: 1,
        };
        assert!(runtime.causal_scopes.acquire(
            scope,
            LeaseIdentity::AdmittedEntityMutation {
                family_token,
                seq: 1
            }
        ));
        runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .fanout
            .try_push(LeasedFanoutMutation {
                generation,
                mutation: PackageEntityMutation::Upsert {
                    admission: None,
                    entity_type: family.into(),
                    snapshot_seq: 1,
                    id: "item".into(),
                    entity: serde_json::json!({"id": "item"}),
                },
                lease: Some(lease),
            })
            .unwrap();
    }
    assert!(!runtime.test_family_exists(family));
    runtime.advance_package_entity_epoch().unwrap();
    runtime.drop_package_entity_families("producer", BTreeSet::new());
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    let identities = runtime.causal_scopes.identities(scope).unwrap();
    assert!(
        !identities.contains(&LeaseIdentity::AdmittedEntityMutation {
            family_token: 10,
            seq: 1
        })
    );
    assert!(identities.contains(&LeaseIdentity::AdmittedEntityMutation {
        family_token: 20,
        seq: 1
    }));
    let remaining = runtime.take_one_package_entity_fanout().unwrap();
    assert_eq!(remaining.generation, 1);
    assert!(runtime.take_one_package_entity_fanout().is_none());
    assert_eq!(
        runtime.finish_package_entity_fanout(&remaining.finish),
        CausalTransitionStatus::Applied
    );
}

pub(crate) fn publication_provider_runtime(label: &str) -> (HubRuntime, std::path::PathBuf) {
    publication_provider_runtime_with_families(label, &["producer.item"])
}

fn publication_provider_runtime_with_families(
    label: &str,
    families: &[&str],
) -> (HubRuntime, std::path::PathBuf) {
    publication_provider_runtime_with_body(label, families, "")
}

fn publication_provider_runtime_with_body(
    label: &str,
    families: &[&str],
    before_snapshot: &str,
) -> (HubRuntime, std::path::PathBuf) {
    let mut runtime = family_runtime(label);
    let root = std::env::temp_dir().join(format!("fanout-provider-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "producer", "version": "1.0.0", "kind": "plugin",
            "botster": ">=0.1.0",
            "capabilities": [{ "surface": "timers", "scope": "callbacks" }],
            "source": { "type": "path", "path": root.canonicalize().unwrap() },
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .unwrap();
    let handlers = families
        .iter()
        .enumerate()
        .map(|(index, family)| {
            let family = serde_json::to_string(family).unwrap();
            let handler = if index == 0 {
                "items".into()
            } else {
                format!("items{index}")
            };
            format!(
                r#"{{
                id = "{handler}", kind = "entity_provider", descriptor_id = {family},
                descriptor = {{ entity_type = {family}, id_field = "id" }},
                call = function() {before_snapshot} return {{
                    type = "entity_snapshot", entity_type = {family}, snapshot_seq = 0, items = {{}}
                }} end
            }}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    std::fs::write(
        root.join("plugin.lua"),
        format!("return botster.register({{ handlers = {{ {handlers} }} }})"),
    )
    .unwrap();
    let mut policy = crate::default_package_policy();
    policy
        .install_local_path(&root, "install test provider")
        .unwrap();
    policy.enable("producer", "enable test provider").unwrap();
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap();
    (runtime, root)
}

#[test]
fn lua_load_and_reload_refuse_before_replacing_live_state() {
    use botster_core::{
        CapabilityOperation, CapabilityOperationId, CapabilityRuntimeEvent,
        CapabilityRuntimeRequest, TimerCapabilityRequest,
    };

    let (mut runtime, root) = publication_provider_runtime("lua-memory-refusal");
    let entrypoint = root.join("plugin.lua");
    let source = std::fs::read_to_string(&entrypoint).unwrap();
    std::fs::write(
        &entrypoint,
        format!(
            "botster.events.on({{ owner = 'hub', name = 'worktree_created' }}, function(event) return {{ received = event.event }} end)\n{source}"
        ),
    )
    .unwrap();
    let mut policy = crate::default_package_policy();
    policy
        .install_local_path(&root, "install memory test provider")
        .unwrap();
    policy
        .enable("producer", "enable memory test provider")
        .unwrap();
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap();
    let memory = Arc::clone(&runtime.lua_memory);
    let limits = memory.limits();
    let owners = runtime.test_callback_charge_owners();
    let owner_sum: usize = owners.iter().map(|(_, bytes)| *bytes).sum();
    eprintln!(
        "callback charge owners after load: {owners:?} sum={owner_sum} usage={}",
        memory.usage().1
    );
    assert_eq!(
        owner_sum,
        memory.usage().1,
        "callback owner sum must equal usage after load owners={owners:?}"
    );
    assert_eq!(memory.usage(), (limits.per_vm_bytes, owner_sum));
    let registration = runtime
        .plugin_lifecycle()
        .entity_provider_registrations()
        .select("producer", "producer.item")
        .unwrap();
    let generation = runtime
        .package_event_router
        .current_package_generation("producer");
    assert!(registration.is_live());
    assert!(matches!(generation, Ok(value) if value > 0));
    assert_eq!(
        runtime
            .package_event_router
            .test_subscription_count("producer"),
        1
    );
    assert!(runtime.last_capability_cleanup().is_none());
    let plugin_key = PluginKey("producer".into());
    let timer = runtime
        .submit_capability_request(CapabilityRuntimeRequest {
            plugin_key: plugin_key.clone(),
            operation_id: CapabilityOperationId("lua-memory-surviving-timer".into()),
            operation: CapabilityOperation::Timer(TimerCapabilityRequest::Interval {
                interval_ms: 5,
            }),
            timeout_ms: 1_000,
            callback: None,
        })
        .unwrap()
        .resource
        .expect("the interval timer must have a resource");
    assert_eq!(timer.plugin_key, plugin_key);
    assert_eq!(runtime.active_plugin_timer_resources(), 1);
    let held: Vec<_> = (1..limits.total_vm_bytes / limits.per_vm_bytes)
        .map(|_| memory.reserve_vm().unwrap())
        .collect();
    assert_eq!(memory.usage().0, limits.total_vm_bytes);
    for (reload, now_ms, sequence) in [(false, 5, 1), (true, 10, 2)] {
        let error = if reload {
            runtime
                .reload_lua_plugin_package(
                    RequestId("lua-memory-refusal".into()),
                    policy.registry(),
                    "producer",
                )
                .unwrap_err()
        } else {
            runtime
                .load_lua_plugin_package(policy.registry(), "producer")
                .unwrap_err()
        };
        match error {
            HubLuaPluginLoadError::Lua(crate::lua_runtime::LuaPluginRuntimeError::Load(
                message,
            )) => {
                assert_eq!(
                    message,
                    format!(
                        "Lua VM memory capacity exhausted: requested {} bytes, 0 available",
                        limits.per_vm_bytes,
                    )
                );
            }
            other => panic!("expected the VM capacity refusal, got {other:?}"),
        }
        assert!(registration.is_live());
        assert_eq!(
            runtime
                .package_event_router
                .current_package_generation("producer"),
            generation
        );
        assert_eq!(
            runtime
                .package_event_router
                .test_subscription_count("producer"),
            1
        );
        assert!(runtime.last_capability_cleanup().is_none());
        let owners = runtime.test_callback_charge_owners();
        let owner_sum: usize = owners.iter().map(|(_, bytes)| *bytes).sum();
        assert_eq!(
            owner_sum,
            memory.usage().1,
            "callback owner sum must equal usage after refused load owners={owners:?}"
        );
        assert_eq!(memory.usage(), (limits.total_vm_bytes, owner_sum));
        let events = runtime
            .drain_capability_events_at(&plugin_key, now_ms)
            .unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            CapabilityRuntimeEvent::TimerFired(event)
                if event.resource == timer && event.sequence == sequence
        )));
        assert_eq!(runtime.active_plugin_timer_resources(), 1);
    }
    drop(held);
    runtime
        .reload_lua_plugin_package(
            RequestId("lua-memory-retry".into()),
            policy.registry(),
            "producer",
        )
        .unwrap();
    assert!(!registration.is_live());
    let replaced_generation = runtime
        .package_event_router
        .current_package_generation("producer")
        .unwrap();
    assert!(replaced_generation > generation.unwrap());
    assert_eq!(
        runtime
            .package_event_router
            .test_subscription_count("producer"),
        1
    );
    assert_eq!(runtime.active_plugin_timer_resources(), 0);
    assert!(
        runtime
            .last_capability_cleanup()
            .unwrap()
            .removed_resources
            .contains(&timer)
    );
    let owners = runtime.test_callback_charge_owners();
    let owner_sum: usize = owners.iter().map(|(_, bytes)| *bytes).sum();
    assert_eq!(
        owner_sum,
        memory.usage().1,
        "callback owner sum must equal usage after reload owners={owners:?}"
    );
    assert_eq!(memory.usage(), (limits.per_vm_bytes, owner_sum));
    drop(runtime);
    assert_eq!(memory.usage(), (0, 0));
    std::fs::remove_dir_all(root).unwrap();
}

/// A provider loaded with one hub event subscription, plus its policy.
fn subscribed_provider_runtime(
    name: &str,
) -> (
    HubRuntime,
    std::path::PathBuf,
    crate::packages::PackageAdmissionPolicy,
) {
    let (mut runtime, root, policy) = subscribed_provider_enabled(name);
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap();
    (runtime, root, policy)
}

/// The same provider, installed and enabled but not loaded.
fn subscribed_provider_enabled(
    name: &str,
) -> (
    HubRuntime,
    std::path::PathBuf,
    crate::packages::PackageAdmissionPolicy,
) {
    subscribed_provider_enabled_with(name, "")
}

/// The enabled provider whose entrypoint first runs `prelude`.
fn subscribed_provider_enabled_with(
    name: &str,
    prelude: &str,
) -> (
    HubRuntime,
    std::path::PathBuf,
    crate::packages::PackageAdmissionPolicy,
) {
    let (runtime, root) = publication_provider_runtime(name);
    let entrypoint = root.join("plugin.lua");
    let source = std::fs::read_to_string(&entrypoint).unwrap();
    std::fs::write(
        &entrypoint,
        format!(
            "{prelude}\nbotster.events.on({{ owner = 'hub', name = 'worktree_created' }}, function(event) return {{ received = event.event }} end)\n{source}"
        ),
    )
    .unwrap();
    let mut policy = crate::default_package_policy();
    policy
        .install_local_path(&root, "install subscribed test provider")
        .unwrap();
    policy
        .enable("producer", "enable subscribed test provider")
        .unwrap();
    (runtime, root, policy)
}

fn logged_messages(runtime: &HubRuntime) -> Vec<(String, u64)> {
    runtime
        .plugin_logs()
        .read("producer", 0)
        .unwrap()
        .records
        .into_iter()
        .map(|record| (record.message, record.generation))
        .collect()
}

/// Callback-account bytes once the Hub log mirror holds no copy.
fn settled_callback_bytes(runtime: &HubRuntime) -> usize {
    runtime.plugin_logs().wait_until_mirrored();
    runtime.lua_memory.usage().1
}

/// An installed and enabled "producer" whose entrypoint first runs
/// `prelude`. No generation of it is loaded, so its next load is a first
/// load.
fn unloaded_logging_provider(
    label: &str,
    prelude: &str,
) -> (
    HubRuntime,
    std::path::PathBuf,
    crate::packages::PackageAdmissionPolicy,
) {
    let runtime = family_runtime(label);
    let root =
        std::env::temp_dir().join(format!("logging-provider-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "producer", "version": "1.0.0", "kind": "plugin",
            "botster": ">=0.1.0",
            "capabilities": [{ "surface": "timers", "scope": "callbacks" }],
            "source": { "type": "path", "path": root.canonicalize().unwrap() },
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        root.join("plugin.lua"),
        format!("{prelude}\nreturn botster.register({{ handlers = {{}} }})"),
    )
    .unwrap();
    let mut policy = crate::default_package_policy();
    policy
        .install_local_path(&root, "install logging test provider")
        .unwrap();
    policy
        .enable("producer", "enable logging test provider")
        .unwrap();
    assert!(!runtime.plugin_lifecycle().is_loaded("producer"));
    (runtime, root, policy)
}

#[test]
fn a_lua_load_that_fails_releases_its_log_entry_and_charge() {
    let (mut runtime, root, policy) = unloaded_logging_provider(
        "load-lua-failure-logs",
        "botster.log.info({ message = 'loading' })\nerror('refuse to load')",
    );
    let baseline = settled_callback_bytes(&runtime);
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap_err();
    assert_eq!(
        settled_callback_bytes(&runtime),
        baseline,
        "a failed first load leaves no log entry, ring, or record charged"
    );
    assert!(logged_messages(&runtime).is_empty());
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_first_load_rejected_after_the_lua_load_releases_its_log_entry_and_charge() {
    let (mut runtime, root, policy) = unloaded_logging_provider(
        "load-preflight-logs",
        "botster.log.info({ message = 'loading' })",
    );
    let baseline = settled_callback_bytes(&runtime);
    crate::lifecycle::inject_next_prepare_failure("producer");
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap_err();
    assert_eq!(
        settled_callback_bytes(&runtime),
        baseline,
        "a failed first load leaves no log entry, ring, or record charged"
    );
    assert!(logged_messages(&runtime).is_empty());
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_failed_reload_keeps_its_records_beside_the_live_ones_on_a_full_ring() {
    // Each load writes five records of about 65 KB. Ten do not fit the
    // 512 KiB text cap, so the candidate's records evict the oldest live
    // ones: the ring is full when the reload fails.
    let (mut runtime, root, policy) = unloaded_logging_provider(
        "reload-full-ring-logs",
        "for _ = 1, 5 do botster.log.info({ message = string.rep('x', 65000) }) end",
    );
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap();
    let live = logged_messages(&runtime);
    assert_eq!(live.len(), 5);
    let live_generation = live[0].1;
    crate::lifecycle::inject_next_prepare_failure("producer");
    runtime
        .reload_lua_plugin_package(
            RequestId("reload-full-ring-logs".into()),
            policy.registry(),
            "producer",
        )
        .unwrap_err();
    let page = runtime.plugin_logs().read("producer", 0).unwrap();
    assert_eq!(page.first_available_seq, 3, "two live records were evicted");
    let generations: Vec<u64> = page
        .records
        .iter()
        .map(|record| record.generation)
        .collect();
    let candidate_generation = generations[3];
    assert_ne!(candidate_generation, live_generation);
    assert_eq!(
        generations,
        [vec![live_generation; 3], vec![candidate_generation; 5]].concat(),
        "the surviving live records and the failed candidate's records stay, in order"
    );
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.seq)
            .collect::<Vec<_>>(),
        (3..=10).collect::<Vec<_>>()
    );
    drop(page);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_first_load_rejected_after_the_lua_load_revokes_the_package_grants() {
    let (mut runtime, root, policy) = unloaded_logging_provider("load-preflight-grants", "");
    let key = PluginKey("producer".into());
    crate::lifecycle::inject_next_prepare_failure("producer");
    let error = runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap_err();
    assert!(
        matches!(error, HubLuaPluginLoadError::Lifecycle(_)),
        "{error:?}"
    );
    assert!(
        !runtime
            .capability_runtime
            .lock()
            .unwrap()
            .test_has_plugin_grants(&key),
        "nothing was installed, so no grants may remain"
    );
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .expect("a refused load leaves nothing behind");
    assert!(
        runtime
            .capability_runtime
            .lock()
            .unwrap()
            .test_has_plugin_grants(&key)
    );
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_reload_keeps_the_log_id_and_a_load_after_unload_starts_a_new_one() {
    let (mut runtime, root, policy) =
        unloaded_logging_provider("log-id-lifetime", "botster.log.info({ message = 'up' })");
    let log_id = |runtime: &HubRuntime| {
        runtime
            .plugin_logs()
            .read("producer", 0)
            .unwrap()
            .log_id
            .expect("a loaded package that logged has a log id")
    };
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap();
    let first = log_id(&runtime);
    runtime
        .reload_lua_plugin_package(
            RequestId("log-id-reload".into()),
            policy.registry(),
            "producer",
        )
        .unwrap();
    assert_eq!(log_id(&runtime), first, "a reload continues the same log");
    runtime
        .unload_plugin_package(RequestId("log-id-unload".into()), "producer")
        .unwrap();
    assert_eq!(
        runtime.plugin_logs().read("producer", 0).unwrap().log_id,
        None
    );
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap();
    let second = log_id(&runtime);
    assert_ne!(second, first, "a load after an unload starts a new log");
    assert_eq!(
        runtime.plugin_logs().read("producer", 0).unwrap().records[0].seq,
        1,
        "the new log restarts its sequence, which the new id announces"
    );
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_failed_load_over_a_live_generation_keeps_its_grants() {
    // The runtime accepts a load of a package that is already loaded (the
    // commit replaces the previous worker). If that load fails, the live
    // generation keeps serving, so it keeps its grants.
    let (mut runtime, root, policy) = unloaded_logging_provider("load-over-live-grants", "");
    let key = PluginKey("producer".into());
    let has_grants = |runtime: &HubRuntime| {
        runtime
            .capability_runtime
            .lock()
            .unwrap()
            .test_has_plugin_grants(&key)
    };
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap();
    assert!(has_grants(&runtime));
    crate::lifecycle::inject_next_prepare_failure("producer");
    let error = runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap_err();
    assert!(
        matches!(error, HubLuaPluginLoadError::Lifecycle(_)),
        "{error:?}"
    );
    assert!(
        runtime.plugin_lifecycle().is_loaded("producer"),
        "the live generation still serves"
    );
    assert!(
        has_grants(&runtime),
        "a failed load over a live generation keeps the live grants"
    );
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_reload_lifecycle_rejection_keeps_the_live_plugin_and_event_generation() {
    let (mut runtime, root, policy) = subscribed_provider_runtime("reload-preflight");
    let registration = runtime
        .plugin_lifecycle()
        .entity_provider_registrations()
        .select("producer", "producer.item")
        .unwrap();
    let generation = runtime
        .package_event_router
        .current_package_generation("producer");
    assert!(matches!(generation, Ok(value) if value > 0));

    crate::lifecycle::inject_next_prepare_failure("producer");
    let error = runtime
        .reload_lua_plugin_package(
            RequestId("reload-preflight".into()),
            policy.registry(),
            "producer",
        )
        .unwrap_err();

    assert!(
        matches!(error, HubLuaPluginLoadError::Lifecycle(_)),
        "{error:?}"
    );
    assert!(registration.is_live(), "the previous plugin still runs");
    assert_eq!(
        runtime
            .package_event_router
            .current_package_generation("producer"),
        generation,
        "the previous event generation is still committed"
    );
    assert_eq!(
        runtime
            .package_event_router
            .test_subscription_count("producer"),
        1
    );
    // The refusal staged nothing, so the next reload is admitted.
    runtime
        .reload_lua_plugin_package(
            RequestId("reload-preflight-retry".into()),
            policy.registry(),
            "producer",
        )
        .expect("a refused reload leaves no staged generation behind");
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

/// A delivery pulled before a reload and looked up only after it matched
/// the previous generation, so it never reaches the new plugin.
#[test]
fn a_delivery_pulled_before_a_reload_is_refused_after_it() {
    use crate::lifecycle::EventDeliveryRefusal;
    use crate::package_event_router::HUB_EVENT_OWNER;

    let (mut runtime, root, policy) = subscribed_provider_runtime("pulled-before-reload");
    let ingress = |runtime: &HubRuntime| {
        assert_eq!(
            runtime.package_event_router.try_ingress(
                HUB_EVENT_OWNER,
                "worktree_created",
                &serde_json::json!({ "event": "worktree_created" }),
                Instant::now(),
            ),
            crate::package_event_router::EventPlaneStatus::Accepted
        );
        let mut batch = runtime
            .package_event_router
            .pull_ready_batch(8, 64 * 1024, Instant::now(), Duration::from_millis(8))
            .expect("pull");
        assert_eq!(batch.len(), 1);
        batch.remove(0)
    };
    let pulled = ingress(&runtime);
    let v1 = pulled.holder.plugin_generation;

    runtime
        .reload_lua_plugin_package(
            RequestId("pulled-before-reload".into()),
            policy.registry(),
            "producer",
        )
        .expect("reload");

    // The first lookup happens only now, after the reload completed.
    assert_eq!(
        runtime.package_event_handler(&pulled).err(),
        Some(EventDeliveryRefusal::GenerationUnloaded)
    );
    let handler_ref = botster_core::PluginHandlerRef {
        plugin_key: PluginKey("producer".into()),
        kind: botster_core::PluginHandlerKind::Event,
        handler_id: pulled.holder.handler_id.clone(),
    };
    let admission = runtime.try_admit_package_event(
        &pulled,
        PluginInvocationClass::Background,
        PluginInvocationRequest {
            request_id: RequestId("pulled-before-reload-admit".into()),
            handler: handler_ref,
            timeout_ms: 1_000,
            context: botster_core::PluginInvocationContext {
                client_id: None,
                session_id: None,
                subscription_id: None,
                surface_id: None,
                origin: None,
                metadata: None,
            },
            payload: BoundaryJson(pulled.payload_json.clone()),
        },
    );
    assert_eq!(
        admission.err(),
        Some(EventDeliveryRefusal::GenerationUnloaded)
    );
    runtime
        .package_event_router
        .complete_pulled_delivery(pulled)
        .expect("retire the refused delivery");

    // A new event matches the new generation and resolves.
    let fresh = ingress(&runtime);
    assert!(fresh.holder.plugin_generation > v1);
    assert!(runtime.package_event_handler(&fresh).is_ok());
    runtime
        .package_event_router
        .complete_pulled_delivery(fresh)
        .expect("retire the fresh delivery");
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

/// Counts released staging funding.
struct ReleasedFunding(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for ReleasedFunding {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// A panic between the stage and the install leaves the plugin and the
/// live generation untouched; the unwound runtime hands the staged
/// generation to cleanup, and aborting it releases its funding and lets a
/// later stage in.
#[test]
fn a_panic_after_the_stage_hands_the_staged_generation_to_the_restore() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (runtime, root, policy) = subscribed_provider_runtime("panic-after-stage");
    let registration = runtime
        .plugin_lifecycle()
        .entity_provider_registrations()
        .select("producer", "producer.item")
        .unwrap();
    let generation = runtime
        .package_event_router
        .current_package_generation("producer");
    let released = Arc::new(AtomicUsize::new(0));
    let mut host = runtime.host_package_runtime();
    host.fund_staging(package_effect::StagingFunding::new(
        Arc::new(ReleasedFunding(Arc::clone(&released))),
        crate::host_executor::HOST_PREPARED_BYTE_CAPACITY,
    ));

    package_effect::panic_next_load_after_stage();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        host.reload_lua_plugin_package(
            RequestId("panic-after-stage".into()),
            policy.registry(),
            "producer",
        )
    }));
    assert!(unwound.is_err(), "the injected panic unwinds the effect");
    assert!(registration.is_live(), "the plugin was never replaced");
    assert_eq!(
        runtime
            .package_event_router
            .current_package_generation("producer"),
        generation
    );
    assert_eq!(
        released.load(Ordering::SeqCst),
        0,
        "the pending entry keeps its funding"
    );

    let staged = host
        .into_cleanup()
        .staged
        .expect("cleanup carries the staged generation");
    assert_eq!(
        released.load(Ordering::SeqCst),
        0,
        "with the runtime gone, the pending entry alone keeps the funding"
    );
    let mut restore = runtime.host_package_runtime();
    restore.abort_staged(staged);
    assert_eq!(
        released.load(Ordering::SeqCst),
        1,
        "abort releases the funding"
    );

    // Nothing is pending any more, so the next reload stages and commits.
    let mut retry = runtime.host_package_runtime();
    retry.fund_staging(package_effect::StagingFunding::new(
        Arc::new(()),
        crate::host_executor::HOST_PREPARED_BYTE_CAPACITY,
    ));
    retry
        .reload_lua_plugin_package(
            RequestId("after-abort".into()),
            policy.registry(),
            "producer",
        )
        .expect("a reload after the abort");
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

/// A failed compensation unloads the package: its queued events are
/// counted as stranded and retired by the unload, and no handler of any
/// generation remains for them.
#[test]
fn a_failed_compensation_quarantines_the_package() {
    use crate::lifecycle::EventDeliveryRefusal;
    use crate::package_event_router::{HUB_EVENT_OWNER, OwnerApplyResult};

    let (mut runtime, root, _policy) = subscribed_provider_runtime("quarantine");
    assert_eq!(
        runtime.package_event_router.try_ingress(
            HUB_EVENT_OWNER,
            "worktree_created",
            &serde_json::json!({ "event": "worktree_created" }),
            Instant::now(),
        ),
        crate::package_event_router::EventPlaneStatus::Accepted
    );
    let queued = |runtime: &HubRuntime| {
        runtime
            .package_event_router
            .snapshot()
            .expect("router snapshot")
            .queued_holders
    };
    assert_eq!(queued(&runtime), 1);

    let mut host = runtime.host_package_runtime();
    let mut supervisor = crate::entrypoint_supervisor::EntrypointSupervisor::default();
    crate::daemon::control::packages::mutations::quarantine_after_failed_compensation(
        &mut host,
        &mut supervisor,
        &crate::host_mutations::PackageRuntimeEffect::Disable {
            package_name: "producer".to_string(),
        },
    );
    assert!(runtime.stranded_packages().contains("producer"));
    let mut cleanup = host.into_cleanup();
    let unload = cleanup
        .event_plane_unloads
        .pop_front()
        .expect("the quarantine records the router unload");
    assert_eq!(unload.owner, "producer");
    let OwnerApplyResult::Work(work) = runtime.package_event_router.try_apply(&unload) else {
        panic!("a package unload is owner work");
    };
    work.run(&runtime.package_event_router)
        .expect("the unload runs");
    assert_eq!(queued(&runtime), 0);
    assert!(
        runtime
            .plugin_lifecycle()
            .event_handler_for("producer", 1, "hub", "worktree_created", "worktree_created")
            .is_err_and(|refusal| refusal == EventDeliveryRefusal::PackageUnloaded),
        "no handler remains for the quarantined package"
    );
    drop(cleanup);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

/// Ingress one hub event and pull its single delivery.
fn pull_one(
    router: &crate::package_event_router::PackageEventRouter,
) -> crate::package_event_router::ReadyDelivery {
    assert_eq!(
        router.try_ingress(
            crate::package_event_router::HUB_EVENT_OWNER,
            "worktree_created",
            &serde_json::json!({ "event": "worktree_created" }),
            Instant::now(),
        ),
        crate::package_event_router::EventPlaneStatus::Accepted
    );
    let mut batch = router
        .pull_ready_batch(8, 64 * 1024, Instant::now(), Duration::from_millis(8))
        .expect("pull");
    assert_eq!(batch.len(), 1);
    batch.remove(0)
}

fn event_request(delivery: &crate::package_event_router::ReadyDelivery) -> PluginInvocationRequest {
    PluginInvocationRequest {
        request_id: RequestId(format!("seam-{}", delivery.envelope_id)),
        handler: botster_core::PluginHandlerRef {
            plugin_key: PluginKey(delivery.holder.plugin_key.clone()),
            kind: botster_core::PluginHandlerKind::Event,
            handler_id: delivery.holder.handler_id.clone(),
        },
        timeout_ms: 1_000,
        context: botster_core::PluginInvocationContext {
            client_id: None,
            session_id: None,
            subscription_id: None,
            surface_id: None,
            origin: None,
            metadata: None,
        },
        payload: BoundaryJson(delivery.payload_json.clone()),
    }
}

/// While Core replaces the worker, no delivery is admitted: the plugin is
/// swapping, so even a delivery matched under the previous generation is
/// refused rather than handed to whichever worker Core holds.
#[test]
fn no_delivery_is_admitted_while_the_worker_is_swapping() {
    use crate::lifecycle::EventDeliveryRefusal;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (mut runtime, root, policy) = subscribed_provider_runtime("swap-seam");
    let router = Arc::clone(&runtime.package_event_router);
    let pulled = pull_one(&router);
    let lifecycle = runtime.plugin_lifecycle().clone();
    let ran = Arc::new(AtomicBool::new(false));
    let hook_ran = Arc::clone(&ran);
    let (generation, owner, name) = (
        pulled.holder.plugin_generation,
        pulled.owner.clone(),
        pulled.name.clone(),
    );
    let request = event_request(&pulled);
    crate::lifecycle::on_next_swap(move || {
        let admission = lifecycle.try_admit_event(
            generation,
            &owner,
            &name,
            PluginInvocationClass::Background,
            request,
        );
        assert_eq!(
            admission.err(),
            Some(EventDeliveryRefusal::GenerationUnloaded)
        );
        hook_ran.store(true, Ordering::SeqCst);
    });
    runtime
        .reload_lua_plugin_package(RequestId("swap-seam".into()), policy.registry(), "producer")
        .expect("reload");
    assert!(ran.load(Ordering::SeqCst), "the swap hook ran");
    router
        .complete_pulled_delivery(pulled)
        .expect("retire the refused delivery");
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

/// Between the install and the activation, ingress still matches the
/// previous generation's live subscriptions; those deliveries are refused
/// against the installed generation. After activation a new event reaches
/// the installed generation.
#[test]
fn deliveries_matched_before_activation_never_reach_the_installed_plugin() {
    use crate::lifecycle::EventDeliveryRefusal;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (mut runtime, root, policy) = subscribed_provider_runtime("activation-seam");
    let router = Arc::clone(&runtime.package_event_router);
    let lifecycle = runtime.plugin_lifecycle().clone();
    let ran = Arc::new(AtomicBool::new(false));
    let hook_ran = Arc::clone(&ran);
    let hook_router = Arc::clone(&router);
    crate::runtime::package_effect::on_next_activation(move || {
        let delivery = pull_one(&hook_router);
        assert_eq!(
            lifecycle
                .event_handler_for(
                    &delivery.holder.plugin_key,
                    delivery.holder.plugin_generation,
                    &delivery.owner,
                    &delivery.name,
                    &delivery.holder.handler_id,
                )
                .err(),
            Some(EventDeliveryRefusal::GenerationUnloaded),
            "the installed plugin serves only the staged generation"
        );
        assert_eq!(
            lifecycle
                .try_admit_event(
                    delivery.holder.plugin_generation,
                    &delivery.owner,
                    &delivery.name,
                    PluginInvocationClass::Background,
                    event_request(&delivery),
                )
                .err(),
            Some(EventDeliveryRefusal::GenerationUnloaded)
        );
        hook_router
            .complete_pulled_delivery(delivery)
            .expect("retire the refused delivery");
        hook_ran.store(true, Ordering::SeqCst);
    });
    runtime
        .reload_lua_plugin_package(
            RequestId("activation-seam".into()),
            policy.registry(),
            "producer",
        )
        .expect("reload");
    assert!(ran.load(Ordering::SeqCst), "the activation hook ran");
    let fresh = pull_one(&router);
    assert!(runtime.package_event_handler(&fresh).is_ok());
    router
        .complete_pulled_delivery(fresh)
        .expect("retire the fresh delivery");
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

/// An explicit operator reload resolves a stranded package: it succeeds
/// and clears the marker, so automatic reloads may load it again.
#[test]
fn an_explicit_reload_clears_the_stranded_marker() {
    let (runtime, root, policy) = subscribed_provider_runtime("explicit-reload-clears");
    let mut host = runtime.host_package_runtime();
    host.fund_staging(package_effect::StagingFunding::new(
        Arc::new(()),
        crate::host_executor::HOST_PREPARED_BYTE_CAPACITY,
    ));
    host.mark_stranded("producer");
    assert!(runtime.stranded_packages().contains("producer"));
    let budget = crate::shared_view::SharedViewBudget::new();
    let effect = crate::host_mutations::PackageRuntimeEffect::Reload {
        package_name: "producer".to_string(),
        reload_plugin: true,
        previous_state: crate::shared_view::SharedView::try_new(
            &budget,
            crate::persistence::HubState::from_config(runtime.config()),
            0,
        )
        .expect("state view"),
        previous_packages: crate::shared_view::SharedView::try_new(
            &budget,
            policy.registry().clone(),
            0,
        )
        .expect("registry view"),
        running_entrypoints: Vec::new(),
    };
    let mut supervisor = crate::entrypoint_supervisor::EntrypointSupervisor::default();
    crate::daemon::control::packages::mutations::apply_committed_runtime_effect(
        &mut host,
        &mut supervisor,
        runtime.config(),
        policy.registry(),
        &effect,
    )
    .expect("the explicit reload succeeds");
    assert!(!runtime.stranded_packages().contains("producer"));
    drop(host);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn an_enable_event_plane_rejection_keeps_the_live_plugin_and_event_generation() {
    let (mut runtime, root, policy) = subscribed_provider_runtime("enable-event-plane");
    let registration = runtime
        .plugin_lifecycle()
        .entity_provider_registrations()
        .select("producer", "producer.item")
        .unwrap();
    let generation = runtime
        .package_event_router
        .current_package_generation("producer");
    let entrypoint = root.join("plugin.lua");
    let source = std::fs::read_to_string(&entrypoint).unwrap();
    std::fs::write(
        &entrypoint,
        format!(
            "botster.events.on({{ owner = 'hub', name = 'botster_undeclared_event' }}, function() return {{}} end)\n{source}"
        ),
    )
    .unwrap();

    let error = runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap_err();

    assert!(
        matches!(error, HubLuaPluginLoadError::EventPlane(_)),
        "{error:?}"
    );
    assert!(registration.is_live(), "the previous plugin still runs");
    assert_eq!(
        runtime
            .package_event_router
            .current_package_generation("producer"),
        generation
    );
    assert_eq!(
        runtime
            .package_event_router
            .test_subscription_count("producer"),
        1
    );
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn provider_registration_rejects_queued_publications_after_reload_and_unload() {
    let (mut runtime, root) = publication_provider_runtime("registration-replacement");
    let mut policy = crate::default_package_policy();
    policy
        .install_local_path(&root, "install replacement provider")
        .unwrap();
    policy
        .enable("producer", "enable replacement provider")
        .unwrap();
    let bridge = runtime.entity_publish_bridge();
    let frame = || {
        serde_json::json!({
            "type": "entity_remove", "entity_type": "producer.item",
            "snapshot_seq": 1, "id": "item"
        })
    };
    for reload in [true, false] {
        let scope = runtime.causal_scopes.mint().unwrap();
        let response =
            bridge.test_queue_publish(PluginKey("producer".into()), frame(), Some(scope));
        assert_eq!(bridge.pending_publish_count(), 1);
        assert_eq!(bridge.retained_counts().0, 1);
        if reload {
            runtime
                .reload_lua_plugin_package(
                    RequestId("registration-reload".into()),
                    policy.registry(),
                    "producer",
                )
                .unwrap();
        } else {
            let _ = runtime
                .plugin_lifecycle()
                .unload_package(RequestId("registration-unload".into()), "producer");
        }
        runtime.test_fulfill_pending_publishes();
        assert!(
            response
                .try_recv()
                .unwrap()
                .unwrap_err()
                .contains("registration")
        );
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert!(!runtime.causal_scopes.is_live(scope));
        assert!(!runtime.test_family_exists("producer.item"));
        assert_eq!(bridge.pending_publish_count(), 0);
        assert_eq!(bridge.retained_counts(), (0, 0));
    }
    runtime
        .load_lua_plugin_package(policy.registry(), "producer")
        .unwrap();
    let response = bridge.test_queue_publish(PluginKey("producer".into()), frame(), None);
    runtime.test_fulfill_pending_publishes();
    runtime.test_fulfill_pending_publishes();
    assert!(response.try_recv().unwrap().is_ok());
    assert!(runtime.test_family_exists("producer.item"));
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn family_tokens_distinguish_shared_scope_and_same_generation_recreation() {
    let long_family = format!("producer.{}", "a".repeat(300_000));
    let names = ["producer.item", "producer.other", long_family.as_str()];
    let (runtime, root) =
        publication_provider_runtime_with_families("family-token-incarnations", &names);
    let scope = runtime
        .causal_scopes
        .mint_with_lease(Some(LeaseIdentity::EventInFlight))
        .unwrap();
    let publish =
        |name: &str| {
            assert!(runtime.causal_scopes.acquire(
                scope,
                LeaseIdentity::PendingEntityPublish {
                    publication_token: 0
                }
            ));
            runtime.test_admit_publish("producer", serde_json::json!({
            "type": "entity_remove", "entity_type": name, "snapshot_seq": 1, "id": "item"
        }), Some(scope)).unwrap();
            runtime.apply_causal_owner_ops();
            runtime.take_one_package_entity_fanout().unwrap()
        };
    let mut mutations: Vec<_> = names.iter().map(|name| publish(name)).collect();
    let tokens: BTreeSet<_> = mutations
        .iter()
        .map(|item| item.finish.lease.as_ref().unwrap().family_token)
        .collect();
    assert_eq!(tokens.len(), 3);
    assert_eq!(runtime.causal_scopes.lease_count(scope), Some(4));
    assert!(
        mutations
            .iter()
            .all(|item| item.generation == mutations[0].generation)
    );

    let mut old = mutations.remove(0);
    let retired = runtime
        .package_entities
        .lock()
        .expect("package entity model lock")
        .families
        .remove(names[0])
        .unwrap();
    assert!(retired.pending_leases.is_empty());
    assert!(retired.resync.leases.is_empty());
    let new = publish(names[0]);
    assert_eq!(old.generation, new.generation);
    assert_ne!(
        old.finish.lease.as_ref().unwrap().family_token,
        new.finish.lease.as_ref().unwrap().family_token
    );
    old.finish.scheduled_resync = true;
    assert_eq!(
        runtime.finish_package_entity_fanout(&old.finish),
        CausalTransitionStatus::Applied
    );
    runtime.apply_causal_owner_ops();
    assert!(
        !runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .families[names[0]]
            .resync
            .needed
    );
    let identities = runtime.causal_scopes.identities(scope).unwrap();
    assert!(
        !identities.contains(&LeaseIdentity::AdmittedEntityMutation {
            family_token: old.finish.lease.as_ref().unwrap().family_token,
            seq: 1,
        })
    );
    assert!(identities.contains(&LeaseIdentity::AdmittedEntityMutation {
        family_token: new.finish.lease.as_ref().unwrap().family_token,
        seq: 1,
    }));
    mutations.push(new);
    for item in mutations {
        assert_eq!(
            runtime.finish_package_entity_fanout(&item.finish),
            CausalTransitionStatus::Applied
        );
        runtime.apply_causal_owner_ops();
    }
    assert_eq!(
        runtime.causal_scopes.identities(scope),
        Some(BTreeSet::from([LeaseIdentity::EventInFlight]))
    );
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn family_token_exhaustion_preserves_publications_and_existing_leases() {
    let (runtime, root) = publication_provider_runtime_with_families(
        "family-token-exhaustion",
        &["producer.item", "producer.other"],
    );
    let scope = runtime
        .causal_scopes
        .mint_with_lease(Some(LeaseIdentity::EventInFlight))
        .unwrap();
    let frame = |name: &str, seq| {
        serde_json::json!({
            "type": "entity_remove", "entity_type": name, "snapshot_seq": seq, "id": "original-payload"
        })
    };
    runtime.package_entities.lock().unwrap().next_family_token = u64::MAX;
    for seq in [1, 2] {
        assert!(runtime.causal_scopes.acquire(
            scope,
            LeaseIdentity::PendingEntityPublish {
                publication_token: 0
            }
        ));
        runtime
            .test_admit_publish("producer", frame("producer.item", seq), Some(scope))
            .unwrap();
        runtime.apply_causal_owner_ops();
        assert_eq!(
            runtime.package_entities.lock().unwrap().next_family_token,
            0
        );
        assert_eq!(
            runtime
                .package_entities
                .lock()
                .expect("package entity model lock")
                .families["producer.item"]
                .causal_token,
            Some(u64::MAX)
        );
    }
    let expected = prepare_publish_mutation(frame("producer.other", 1)).unwrap();
    for tokenless_exists in [false, true] {
        if tokenless_exists {
            runtime.test_set_family_seq("producer.other", 0);
        }
        let receiver = runtime.entity_publish_bridge.test_queue_publish(
            PluginKey("producer".into()),
            frame("producer.other", 1),
            Some(scope),
        );
        let original = runtime
            .begin_entity_publish()
            .expect("exhaustion retains the original payload for disposal");
        let mut expected = expected.clone();
        expected.set_admission(original.admission().cloned());
        assert_eq!(original, expected);
        assert_eq!(
            runtime.test_family_exists("producer.other"),
            tokenless_exists
        );
        if tokenless_exists {
            let model = runtime
                .package_entities
                .lock()
                .expect("package entity model lock");
            let families = &model.families;
            let family = &families["producer.other"];
            assert_eq!(family.causal_token, None);
            assert_eq!(family.last_accepted_seq, 0);
            assert_eq!(family.high_water_seq, 0);
            assert!(family.pending_by_seq.is_empty());
            assert!(family.pending_leases.is_empty());
            assert!(family.resync.leases.is_empty());
        }
        assert!(receiver.try_recv().is_err());
        drop(original);
        runtime.complete_entity_publish_disposal();
        assert_eq!(
            runtime.finish_entity_publish_retirement(),
            CausalTransitionStatus::Applied
        );
        runtime.apply_causal_owner_ops();
        assert!(
            receiver
                .try_recv()
                .unwrap()
                .unwrap_err()
                .contains("entity_family_token_exhausted")
        );
        assert_eq!(runtime.causal_scopes.lease_count(scope), Some(3));
    }
    while let Some(item) = runtime.take_one_package_entity_fanout() {
        assert_eq!(
            runtime.finish_package_entity_fanout(&item.finish),
            CausalTransitionStatus::Applied
        );
        runtime.apply_causal_owner_ops();
    }
    assert_eq!(
        runtime.causal_scopes.identities(scope),
        Some(BTreeSet::from([LeaseIdentity::EventInFlight]))
    );
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn provider_tokens_preserve_distinct_leases_through_exhaustion_and_retry() {
    let (runtime, root) = publication_provider_runtime("provider-tokens");
    let scope_id = runtime
        .causal_scopes
        .mint_with_lease(Some(LeaseIdentity::EventInFlight))
        .unwrap();
    runtime.mark_package_entity_resync_needed("producer.item");
    runtime.test_store_resync_lease(scope_id, "producer.item");
    let prepare = || {
        runtime.prepare_plugin_entity_snapshot(
            "producer.item",
            "subscription",
            RequestId("same-request".into()),
            None,
        )
    };
    let (_, first) = prepare().unwrap();
    runtime.next_provider_token.set(u64::MAX);
    let (_, last) = prepare().unwrap();
    assert_eq!(first.causal_lease, Some((scope_id, 1)));
    assert_eq!(last.causal_lease, Some((scope_id, u64::MAX)));
    assert!(prepare().is_err());
    assert_eq!(runtime.causal_scopes.lease_count(scope_id), Some(3));
    for _ in 0..CAUSAL_OWNER_CAPACITY {
        assert_eq!(
            runtime.admit_causal_op(CausalOp::Release {
                scope_id: 0,
                identity: LeaseIdentity::EventInFlight,
            }),
            CausalAdmitResult::Applied
        );
    }
    assert_eq!(
        runtime.retire_plugin_entity_snapshot(&first),
        CausalTransitionStatus::Waiting
    );
    assert_eq!(first.causal_lease, Some((scope_id, 1)));
    for _ in 0..CAUSAL_OWNER_CAPACITY {
        runtime.apply_causal_owner_ops();
    }
    assert_eq!(runtime.causal_operation_count(), 0);
    assert_eq!(
        runtime.retire_plugin_entity_snapshot(&first),
        CausalTransitionStatus::Applied
    );
    runtime.apply_causal_owner_ops();
    assert_eq!(
        runtime.causal_scopes.identities(scope_id).unwrap(),
        BTreeSet::from([
            LeaseIdentity::EventInFlight,
            LeaseIdentity::ProviderInFlight {
                invocation_token: u64::MAX,
            },
        ])
    );
    assert_eq!(
        runtime.retire_plugin_entity_snapshot(&last),
        CausalTransitionStatus::Applied
    );
    runtime.apply_causal_owner_ops();
    assert_eq!(runtime.causal_scopes.lease_count(scope_id), Some(1));
    drop(first);
    drop(last);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn fanout_sequence_exhaustion_preserves_publication_state() {
    let (runtime, root) = publication_provider_runtime("fanout-admission-exhaustion");
    let frame = |seq| {
        serde_json::json!({
            "type": "entity_upsert", "entity_type": "producer.item",
            "snapshot_seq": seq, "id": "item", "entity": { "id": "item" }
        })
    };
    runtime
        .package_entities
        .lock()
        .expect("package entity model lock")
        .fanout
        .set_next_sequence_for_test(u64::MAX);
    let error = runtime
        .test_admit_publish("producer", frame(1), None)
        .unwrap_err();
    assert!(error.contains("entity_fanout_sequence_exhausted"));
    assert!(!runtime.test_family_exists("producer.item"));

    runtime
        .package_entities
        .lock()
        .expect("package entity model lock")
        .fanout
        .set_next_sequence_for_test(u64::MAX - 1);
    let result = runtime
        .test_admit_publish("producer", frame(2), None)
        .unwrap();
    assert_eq!(result.status, PackageEntityPublishStatus::PendingGap);
    let snapshot = |family: &PackageEntityFamilyState| {
        (
            family.generation,
            family.causal_token,
            family.last_accepted_seq,
            family.high_water_seq,
            family.pending_by_seq.clone(),
            family.pending_leases.clone(),
            family.resync.needed,
            family.resync.leases.clone(),
        )
    };
    let before = snapshot(
        &runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .families["producer.item"],
    );
    runtime
        .package_entities
        .lock()
        .expect("package entity model lock")
        .fanout
        .set_next_sequence_for_test(u64::MAX);
    let scope_id = runtime
        .causal_scopes
        .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
            publication_token: 0,
        }))
        .unwrap();
    let error = runtime
        .test_admit_publish("producer", frame(1), Some(scope_id))
        .unwrap_err();
    assert!(error.contains("entity_fanout_sequence_exhausted"));
    let model = runtime
        .package_entities
        .lock()
        .expect("package entity model lock");
    let families = &model.families;
    let after = &families["producer.item"];
    assert_eq!(snapshot(after), before);
    assert!(model.fanout.is_empty());
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    assert!(runtime.causal_scopes.identities(scope_id).is_none());
    drop(model);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn provider_expectation_capacity_precedes_selection_and_returns_after_host_disposal() {
    let (runtime, root) = publication_provider_runtime("provider-expectation-capacity");
    let budget = SharedViewBudget::with_capacity(4096);
    let full = budget.reserve(4096).unwrap();
    let prepare = || {
        ProviderRequestPlan::prepare(
            runtime.plugin_lifecycle(),
            &budget,
            "producer.item",
            "sub",
            RequestId("metadata-test".into()),
            None,
        )
    };
    let error = prepare().unwrap_err();
    assert_eq!(error.code, "entity_provider_metadata_capacity");
    assert_eq!(budget.used(), 4096);
    drop(full);
    let plan = prepare().unwrap();
    let charged = budget.used();
    assert!(charged > 0);
    let invocation = runtime
        .select_plugin_entity_snapshot(&plan.expected)
        .unwrap();
    assert_eq!(
        budget.used(),
        charged,
        "selection shares the expectation charge"
    );
    let executor = runtime.host_executor();
    let identity =
        crate::host_executor::HostJobIdentity::first(crate::owner_identity::WaiterId(707));
    executor
        .submit(
            identity,
            crate::host_executor::HostCommand::PluginEntity(
                crate::plugin_entity::Command::DiscardProvider {
                    plan: Some(plan),
                    invocation: Some(invocation),
                    input: None,
                    refusal: None,
                },
            ),
            executor.try_reserve().unwrap(),
        )
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match executor.poll_completion() {
            crate::host_executor::HostCompletionPoll::Ready(completion) => {
                assert_eq!(completion.identity, identity);
                assert_eq!(
                    budget.used(),
                    0,
                    "Host disposal returns the final expectation charge"
                );
                break;
            }
            crate::host_executor::HostCompletionPoll::Empty => std::thread::yield_now(),
            crate::host_executor::HostCompletionPoll::Stopped => panic!("Host stopped"),
        }
        assert!(std::time::Instant::now() < deadline);
    }
    assert!(prepare().is_ok(), "released metadata capacity is reusable");
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn lifetime_budget_follows_provider_snapshot_after_causal_retirement() {
    use crate::daemon::control::reply::{RetainedPluginResult, RetainedPluginResultBudget};
    use crate::plugin_entity::{Command, Completion};
    let (runtime, root) = publication_provider_runtime("lifetime-provider-payload");
    let bridge = runtime.entity_publish_bridge();
    let scope = runtime.causal_scopes.mint().unwrap();
    let reply = bridge.test_queue_publish(
        PluginKey("producer".into()),
        serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
            "snapshot_seq":100, "id":"item"}),
        Some(scope),
    );
    runtime.step_entity_publish();
    assert!(reply.try_recv().unwrap().unwrap().ok);
    let (request, invocation) = runtime
        .prepare_plugin_entity_snapshot(
            "producer.item",
            "sub",
            RequestId("provider-payload".into()),
            None,
        )
        .unwrap();
    let result = runtime.invoke_plugin(request).result;
    runtime
        .package_entities
        .lock()
        .expect("package entity model lock")
        .families
        .get_mut("producer.item")
        .unwrap()
        .resync
        .degraded = true;
    assert!(runtime.release_one_degraded_package_entity_resync_lease("producer.item"));
    assert_eq!(
        runtime.retire_plugin_entity_snapshot(&invocation),
        CausalTransitionStatus::Applied
    );
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    assert!(!runtime.causal_scopes.is_live(scope));
    assert_eq!(bridge.retained_counts().0, 1);
    let result_budget = RetainedPluginResultBudget::new();
    let bytes = serde_json::to_vec(&result).unwrap().len();
    let result = RetainedPluginResult::new(result, result_budget.try_reserve(bytes).unwrap());
    let mut permit = runtime.host_executor().try_reserve().unwrap();
    let Completion::Prepared { payload, .. } = crate::plugin_entity::execute(
        Command::Prepare {
            invocation,
            result,
            inconsistent: false,
            target: None,
        },
        &mut permit,
    ) else {
        panic!("provider result prepares a payload")
    };
    assert_eq!(payload.sequence(), Some(0));
    assert_eq!(bridge.retained_counts().0, 1);
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    let target = Arc::new(crate::plugin_entity::Target {
        subscription_id: "sub".into(),
        entity_type: "producer.item".into(),
        sender: crate::subscription::entity::EntityFrameSender::Async(sender),
    });
    let Completion::Delivered { payload, .. } = crate::plugin_entity::execute(
        Command::Deliver {
            payload,
            target,
            publication_live: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            budget: crate::shared_view::SharedViewBudget::new(),
            resync_reason: None,
        },
        &mut permit,
    ) else {
        panic!("delivery retains its snapshot")
    };
    assert_eq!(bridge.retained_counts().0, 1);
    assert!(matches!(
        crate::plugin_entity::execute(Command::Reclaim(payload), &mut permit),
        Completion::Reclaimed
    ));
    assert_eq!(bridge.retained_counts(), (0, 0));
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn lifetime_budget_survives_family_cleanup_until_payload_and_table_retire() {
    use super::family_cleanup::FamilyCleanupStep;
    let (runtime, root) = publication_provider_runtime("lifetime-cleanup");
    let bridge = runtime.entity_publish_bridge();
    let scope = runtime.causal_scopes.mint().unwrap();
    for (seq, scope_id) in [(2, Some(scope)), (3, None)] {
        let reply = bridge.test_queue_publish(
            PluginKey("producer".into()),
            serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                "snapshot_seq":seq, "id":"item"}),
            scope_id,
        );
        runtime.step_entity_publish();
        assert!(reply.try_recv().unwrap().unwrap().ok);
    }
    let mut cleanup = HostPackageCleanup {
        unloaded_families: vec![("producer".into(), BTreeSet::from(["producer.item".into()]))],
        ..HostPackageCleanup::default()
    };
    runtime
        .begin_direct_package_entity_cleanup(&mut cleanup)
        .unwrap();
    assert_eq!(bridge.retained_counts().0, 2);
    let mut disposed = 0;
    loop {
        match runtime.step_direct_package_entity_cleanup(&mut cleanup) {
            FamilyCleanupStep::Pending => {}
            FamilyCleanupStep::Payload(payload) => {
                assert_eq!(bridge.retained_counts().0, 2);
                drop(payload);
                disposed += 1;
                assert_eq!(
                    bridge.retained_counts().0,
                    if disposed == 1 { 2 } else { 1 }
                );
                runtime.complete_direct_package_entity_cleanup_item(&mut cleanup);
            }
            FamilyCleanupStep::Complete => break,
            FamilyCleanupStep::Waiting | FamilyCleanupStep::Fault => {
                panic!("cleanup capacity is available")
            }
        }
    }
    assert_eq!(disposed, 2);
    assert_eq!(bridge.retained_counts().0, 1);
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    assert_eq!(bridge.retained_counts(), (0, 0));
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn lifetime_budget_bounds_resync_scopes_after_publications_finish() {
    let (runtime, root) = publication_provider_runtime_with_body(
        "lifetime-resync-limit",
        &["producer.item"],
        r#"
                local ok, error = pcall(botster.entity_publish, {
                    type = "entity_remove", entity_type = "producer.item", snapshot_seq = 400, id = "item"
                })
                assert(not ok)
                assert(string.find(tostring(error), "capacity exhausted", 1, true))
            "#,
    );
    let bridge = runtime.entity_publish_bridge();
    for seq in 100..356 {
        let scope = runtime.causal_scopes.mint().unwrap();
        let reply = bridge.test_queue_publish(
            PluginKey("producer".into()),
            serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                "snapshot_seq":seq, "id":"item"}),
            Some(scope),
        );
        runtime.step_entity_publish();
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(
            reply.try_recv().unwrap().unwrap().status,
            PackageEntityPublishStatus::ResyncScheduled
        );
        assert_eq!(bridge.pending_publish_count(), 0);
        assert_eq!(bridge.retained_counts().0, (seq - 99) as usize);
    }
    let rejected = bridge.test_queue_publish(
        PluginKey("producer".into()),
        serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
            "snapshot_seq":400, "id":"item"}),
        None,
    );
    assert!(
        rejected
            .try_recv()
            .unwrap()
            .unwrap_err()
            .contains("capacity exhausted")
    );
    assert_eq!(runtime.test_resync_scope_ids("producer.item").len(), 256);
    let (request, invocation) = runtime
        .prepare_plugin_entity_snapshot(
            "producer.item",
            "sub",
            RequestId("provider-at-capacity".into()),
            None,
        )
        .unwrap();
    let result = runtime.invoke_plugin(request).result;
    assert_eq!(
        runtime.retire_plugin_entity_snapshot(&invocation),
        CausalTransitionStatus::Applied
    );
    let (sequence, _) = runtime
        .complete_plugin_entity_snapshot(invocation, result)
        .unwrap();
    assert_eq!(sequence, 0);
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    assert_eq!(bridge.retained_counts().0, 256);
    runtime
        .package_entities
        .lock()
        .expect("package entity model lock")
        .families
        .get_mut("producer.item")
        .unwrap()
        .resync
        .degraded = true;
    for remaining in (0..256).rev() {
        assert!(runtime.release_one_degraded_package_entity_resync_lease("producer.item"));
        assert_eq!(bridge.retained_counts().0, remaining + 1);
        runtime.apply_causal_owner_ops();
        assert_eq!(bridge.retained_counts().0, remaining);
    }
    assert_eq!(bridge.retained_counts(), (0, 0));
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn lifetime_budget_preserves_coalescing_and_provider_inheritance_before_transfer() {
    let (runtime, root) = publication_provider_runtime("lifetime-provider-inheritance");
    let bridge = runtime.entity_publish_bridge();
    let scope = runtime
        .causal_scopes
        .mint_with_lease(Some(LeaseIdentity::EventInFlight))
        .unwrap();
    let publish = |seq| {
        let reply = bridge.test_queue_publish(
            PluginKey("producer".into()),
            serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                "snapshot_seq":seq, "id":"item"}),
            Some(scope),
        );
        runtime.step_entity_publish();
        assert!(reply.try_recv().unwrap().unwrap().ok);
    };
    publish(100);
    let first = runtime
        .package_entities
        .lock()
        .expect("package entity model lock")
        .families["producer.item"]
        .resync
        .leases[&scope]
        .clone()
        .unwrap();
    publish(101);
    assert_eq!(bridge.retained_counts().0, 2);
    let (_, invocation) = runtime
        .prepare_plugin_entity_snapshot(
            "producer.item",
            "sub",
            RequestId("inherit-before-transfer".into()),
            None,
        )
        .unwrap();
    assert_eq!(invocation.admission.as_ref(), Some(&first));
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    assert_eq!(bridge.retained_counts().0, 1);
    {
        let mut model = runtime
            .package_entities
            .lock()
            .expect("package entity model lock");
        let families = &mut model.families;
        let family = families.get_mut("producer.item").unwrap();
        family.forget_resync_lease(scope);
        assert!(matches!(
            runtime.admit_causal_op(CausalOp::Release {
                scope_id: scope,
                identity: LeaseIdentity::ProviderResyncNeed {
                    family_token: family.causal_token.unwrap()
                }
            }),
            CausalAdmitResult::Applied
        ));
    }
    publish(102);
    let (_, replacement) = runtime
        .prepare_plugin_entity_snapshot(
            "producer.item",
            "sub",
            RequestId("inherit-recreated-need".into()),
            None,
        )
        .unwrap();
    assert_ne!(replacement.admission.as_ref(), Some(&first));
    assert_eq!(bridge.retained_counts().0, 2);
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    assert_eq!(
        runtime.retire_plugin_entity_snapshot(&invocation),
        CausalTransitionStatus::Applied
    );
    runtime.apply_causal_owner_ops();
    drop(first);
    assert_eq!(bridge.retained_counts().0, 2);
    drop(invocation);
    assert_eq!(bridge.retained_counts().0, 1);
    runtime
        .package_entities
        .lock()
        .expect("package entity model lock")
        .families
        .get_mut("producer.item")
        .unwrap()
        .resync
        .degraded = true;
    assert!(runtime.release_one_degraded_package_entity_resync_lease("producer.item"));
    assert_eq!(
        runtime.retire_plugin_entity_snapshot(&replacement),
        CausalTransitionStatus::Applied
    );
    drop(replacement);
    assert_eq!(bridge.retained_counts().0, 1);
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    assert_eq!(bridge.retained_counts(), (0, 0));
    assert_eq!(
        runtime.causal_scopes.identities(scope),
        Some(BTreeSet::from([LeaseIdentity::EventInFlight]))
    );
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn lifetime_budget_returns_capacity_while_the_event_root_remains_live() {
    let (runtime, root) = publication_provider_runtime("lifetime-sequential");
    let bridge = runtime.entity_publish_bridge();
    let scope = runtime
        .causal_scopes
        .mint_with_lease(Some(LeaseIdentity::EventInFlight))
        .unwrap();
    for seq in 1..=512 {
        let reply = bridge.test_queue_publish(
            PluginKey("producer".into()),
            serde_json::json!({"type":"entity_remove", "entity_type":"producer.item",
                "snapshot_seq":seq, "id":"item"}),
            Some(scope),
        );
        runtime.step_entity_publish();
        while runtime.entity_publish_retirement_pending() {
            runtime.advance_entity_publish();
            runtime.finish_entity_publish_retirement();
        }
        assert!(reply.try_recv().unwrap().unwrap().ok);
        let item = runtime.take_one_package_entity_fanout().unwrap();
        let TakenPackageEntityMutation {
            mutation, finish, ..
        } = item;
        drop(mutation);
        assert_eq!(
            runtime.finish_package_entity_fanout(&finish),
            CausalTransitionStatus::Applied
        );
        drop(finish);
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert_eq!(bridge.retained_counts(), (0, 0));
        assert_eq!(
            runtime.causal_scopes.identities(scope),
            Some(BTreeSet::from([LeaseIdentity::EventInFlight]))
        );
    }
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn publication_disposal_preserves_resync_in_both_release_orders() {
    for resync_first in [false, true] {
        let (runtime, root) = publication_provider_runtime(if resync_first {
            "resync-first"
        } else {
            "disposal-first"
        });
        let scope = runtime.causal_scopes.mint().unwrap();
        let bridge = runtime.entity_publish_bridge();
        let response = bridge.test_queue_publish(PluginKey("producer".into()),
            serde_json::json!({"type": "entity_remove", "entity_type": "producer.item", "snapshot_seq": 100, "id": "item"}), Some(scope));
        let payload = runtime
            .begin_entity_publish()
            .expect("out-of-window publication returns its original payload");
        runtime.mark_entity_publish_daemon_owned();
        let pending = LeaseIdentity::PendingEntityPublish {
            publication_token: 1,
        };
        let resync = LeaseIdentity::ProviderResyncNeed {
            family_token: runtime.test_family_causal_token("producer.item"),
        };
        if resync_first {
            assert!(matches!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: scope,
                    identity: resync.clone()
                }),
                CausalAdmitResult::Applied
            ));
            runtime.apply_causal_owner_ops();
            runtime.apply_causal_owner_ops();
            assert_eq!(
                runtime.causal_scopes.identities(scope).unwrap(),
                BTreeSet::from([pending.clone()])
            );
        }
        drop(payload);
        runtime.complete_entity_publish_disposal();
        assert_eq!(
            runtime.finish_entity_publish_retirement(),
            CausalTransitionStatus::Applied
        );
        assert_eq!(
            response.try_recv().unwrap().unwrap().status,
            PackageEntityPublishStatus::ResyncScheduled
        );
        if !resync_first {
            runtime.apply_causal_owner_ops();
            runtime.apply_causal_owner_ops();
            assert_eq!(
                runtime.causal_scopes.identities(scope).unwrap(),
                BTreeSet::from([resync.clone()])
            );
            assert!(matches!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: scope,
                    identity: resync
                }),
                CausalAdmitResult::Applied
            ));
        }
        runtime.apply_causal_owner_ops();
        assert!(!runtime.causal_scopes.is_live(scope));
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn direct_family_cleanup_checks_exhaustion_before_effects() {
    let mut runtime = family_runtime("direct-family-cleanup");
    let registry = PackageRegistry::new(Default::default());
    assert!(
        runtime
            .load_lua_plugin_package(&registry, "absent")
            .is_err()
    );
    assert_eq!(runtime.package_entities.lock().unwrap().epoch, 0);
    runtime.test_set_family_seq("producer.item", 1);
    runtime
        .unload_plugin_package(RequestId("direct-unload".into()), "producer")
        .expect("direct unload reserves its boundary");
    assert_eq!(runtime.package_entities.lock().unwrap().epoch, 1);
    assert_eq!(
        runtime.package_entity_family_generation("producer.item"),
        None
    );

    runtime.test_set_family_seq("producer.item", 2);
    let generation = runtime.package_entity_family_generation("producer.item");
    runtime.test_exhaust_package_entity_epochs();
    assert!(matches!(
        runtime.unload_plugin_package(RequestId("refused-unload".into()), "producer"),
        Err(PackageEntityCleanupError::GenerationExhausted)
    ));
    assert_eq!(
        runtime.package_entity_family_generation("producer.item"),
        generation
    );
    assert_eq!(runtime.package_entities.lock().unwrap().epoch, u64::MAX);
    let error = runtime
        .load_lua_plugin_package(&registry, "absent")
        .expect_err("load must reserve capacity for rollback before execution");
    assert!(matches!(
        error,
        HubLuaPluginLoadError::EntityFamilyCleanup(PackageEntityCleanupError::GenerationExhausted)
    ));
    assert!(!error.is_package_scoped_startup_failure());
    assert_eq!(error.code(), "entity_family_generation_exhausted");
}

#[test]
fn old_family_releases_preserve_recreated_mutation_and_resync_leases() {
    let runtime = family_runtime("family-release-generation");
    let family = "producer.item";
    runtime.test_set_family_seq(family, 1);
    runtime.test_set_family_seq("other.item", 1);
    let old_token = runtime.test_family_causal_token(family);
    let old_generation = runtime.package_entity_family_generation(family).unwrap();
    let scope_id = runtime
        .causal_scopes
        .mint_with_lease(Some(LeaseIdentity::EventInFlight))
        .unwrap();
    let identities = |family_token| {
        [
            LeaseIdentity::AdmittedEntityMutation {
                family_token,
                seq: 1,
            },
            LeaseIdentity::ProviderResyncNeed { family_token },
        ]
    };
    for identity in identities(old_token) {
        assert!(runtime.causal_scopes.acquire(scope_id, identity));
    }
    let retained = runtime.causal_scopes.test_with_inner_held(|| {
        for _ in 0..CAUSAL_OWNER_CAPACITY {
            assert_eq!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: u64::MAX,
                    identity: LeaseIdentity::EventInFlight
                }),
                CausalAdmitResult::Applied
            );
        }
        identities(old_token).map(|identity| {
            let CausalAdmitResult::Retry(op) =
                runtime.admit_causal_op(CausalOp::Release { scope_id, identity })
            else {
                panic!("the caller must retain the rejected old release")
            };
            op
        })
    });
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    runtime
        .drop_package_entity_families_for("producer")
        .unwrap();
    assert_eq!(
        runtime.package_entity_family_generation("other.item"),
        Some(old_generation)
    );
    runtime.test_set_family_seq(family, 1);
    let new_generation = runtime.package_entity_family_generation(family).unwrap();
    assert_ne!(new_generation, old_generation);
    let new_token = runtime.test_family_causal_token(family);
    assert_ne!(new_token, old_token);
    for identity in identities(new_token) {
        assert!(runtime.causal_scopes.acquire(scope_id, identity));
    }
    for op in retained {
        assert_eq!(runtime.admit_causal_op(op), CausalAdmitResult::Applied);
    }
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    let live = runtime.causal_scopes.identities(scope_id).unwrap();
    for identity in identities(new_token) {
        assert!(live.contains(&identity));
    }
    for identity in identities(old_token) {
        assert!(!live.contains(&identity));
    }
    runtime.take_package_entity_resync_notification();
    assert_eq!(
        runtime.finish_package_entity_fanout(&PackageEntityFanoutFinish {
            lease: Some(EntityMutationLease {
                admission: None,
                family_token: old_token,
                scope_id,
                family: family.into(),
                generation: old_generation,
                seq: 1,
            }),
            scheduled_resync: true,
        }),
        CausalTransitionStatus::Applied
    );
    assert!(!runtime.take_package_entity_resync_notification());
    let model = runtime
        .package_entities
        .lock()
        .expect("package entity model lock");
    let families = &model.families;
    assert_eq!(families[family].generation, new_generation);
    assert!(!families[family].resync.needed);
}

#[test]
fn retained_causal_reservations_suppress_family_release_readiness_until_capacity_returns() {
    let runtime = family_runtime("causal-reservation-readiness");
    let family_token = runtime.test_family_causal_token("producer.item");
    let scope = runtime
        .causal_scopes()
        .mint_with_lease(Some(LeaseIdentity::ProviderResyncNeed { family_token }))
        .unwrap();
    runtime.test_store_resync_lease(scope, "producer.item");
    assert!(runtime.causal_family_release_ready());
    let mut reservations: Vec<_> = (0..CAUSAL_OWNER_CAPACITY)
        .map(|_| runtime.reserve_causal_transition().unwrap())
        .collect();
    assert_eq!(runtime.causal_operation_count(), 0);
    assert!(!runtime.causal_family_release_ready());
    drop(reservations.pop());
    assert!(runtime.take_causal_capacity_notification());
    assert!(runtime.causal_family_release_ready());
    runtime.retry_family_resync_release();
    assert_eq!(runtime.causal_operation_count(), 1);
    assert!(!runtime.causal_family_release_ready());
    assert!(runtime.causal_scopes().is_live(scope));
    runtime.apply_causal_owner_ops();
    assert!(!runtime.causal_scopes().is_live(scope));
    drop(reservations);
}

#[test]
fn causal_receipt_waits_for_its_exact_table_application_under_contention() {
    let runtime = family_runtime("causal-receipt-order");
    let scopes = runtime.causal_scopes();
    let scope = scopes
        .mint_with_lease(Some(LeaseIdentity::EventInFlight))
        .unwrap();
    let provider = LeaseIdentity::ProviderInFlight {
        invocation_token: 17,
    };
    assert!(scopes.acquire(scope, provider));
    let retained_reservation = runtime.reserve_causal_transition().unwrap();
    let first = runtime
        .reserve_causal_transition()
        .unwrap()
        .commit(CausalOp::Release {
            scope_id: scope,
            identity: LeaseIdentity::EventInFlight,
        });
    let retained = retained_reservation.commit(CausalOp::Release {
        scope_id: scope,
        identity: provider,
    });
    assert!(!first.is_applied());
    assert!(!retained.is_applied());
    scopes.test_with_inner_held(|| runtime.apply_causal_owner_ops());
    assert_eq!(runtime.causal_operation_count(), 2);
    assert!(!first.is_applied());
    assert!(!retained.is_applied());
    runtime.apply_causal_owner_ops();
    assert!(first.is_applied());
    assert!(!retained.is_applied());
    assert_eq!(
        scopes.identities(scope).unwrap(),
        BTreeSet::from([provider])
    );
    runtime.apply_causal_owner_ops();
    assert!(retained.is_applied());
    assert!(!scopes.is_live(scope));
    assert_eq!(runtime.causal_operation_count(), 0);
}

#[test]
fn poisoned_causal_table_keeps_retry_ownership_without_ready_polling() {
    let runtime = family_runtime("causal-poison-readiness");
    let op = CausalOp::Release {
        scope_id: 1,
        identity: LeaseIdentity::EventInFlight,
    };
    assert_eq!(
        runtime.admit_causal_op(op.clone()),
        CausalAdmitResult::Applied
    );
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime
            .causal_scopes
            .test_with_inner_held(|| panic!("poison causal inner"));
    }));
    assert!(poisoned.is_err());
    assert_eq!(runtime.causal_operation_count(), 1);
    assert!(runtime.causal_owner_ops_pending());
    assert!(!runtime.causal_owner_ops_ready());
    assert_eq!(runtime.causal_queue.take_head(), Some(op));
}

#[test]
fn family_resync_release_index_matches_live_state_after_each_transition() {
    let runtime = family_runtime("family-release-index");
    let assert_index = || {
        let model = runtime
            .package_entities
            .lock()
            .expect("package entity model lock");
        let families = &model.families;
        let expected = families
            .iter()
            .filter(|(_, family)| !family.resync.needed && !family.resync.leases.is_empty())
            .map(|(name, family)| (name.clone(), family.generation))
            .collect::<BTreeSet<_>>();
        assert_eq!(model.resync_releases, expected);
    };
    assert_index();
    for family in ["producer.a", "producer.b", "other.item"] {
        runtime.test_store_resync_lease(1, family);
        assert_index();
        runtime.mark_package_entity_resync_needed(family);
        assert_index();
        runtime.rearm_package_entity_resync(family);
        assert_index();
        runtime
            .begin_package_entity_provider_snapshot(family, 1)
            .expect("the test model is not poisoned");
        assert_index();
    }
    runtime.step_package_entity_provider_snapshot("producer.a");
    assert_index();
    runtime.retry_family_resync_release();
    assert_index();
    runtime
        .drop_package_entity_families_for("producer")
        .unwrap();
    assert_index();
    runtime.test_store_resync_lease(2, "producer.a");
    assert_index();
    assert_eq!(
        runtime.package_entity_family_generation("producer.a"),
        Some(1)
    );
    runtime.retry_family_resync_release();
    assert_index();
    runtime.retry_family_resync_release();
    assert_index();
    assert!(
        runtime
            .package_entities
            .lock()
            .unwrap()
            .resync_releases
            .is_empty()
    );
}

#[test]
fn rejected_family_release_stays_in_cursor_before_next_payload() {
    use super::family_cleanup::FamilyCleanupStep;
    let runtime = family_runtime("retained-family-release");
    let family = "producer.item";
    let identity = LeaseIdentity::AdmittedEntityMutation {
        family_token: 1,
        seq: 1,
    };
    let scope = runtime
        .causal_scopes
        .mint_with_lease(Some(identity.clone()))
        .unwrap();
    for seq in [1, 2] {
        runtime.test_store_family_payload(PackageEntityMutation::Upsert {
            admission: None,
            entity_type: family.into(),
            snapshot_seq: seq,
            id: "item".into(),
            entity: serde_json::json!({"id": "item"}),
        });
    }
    runtime.test_store_pending_lease(scope, family, 1);
    let mut cleanup = HostPackageCleanup {
        unloaded_families: vec![("producer".into(), BTreeSet::from([family.into()]))],
        ..HostPackageCleanup::default()
    };
    runtime
        .begin_direct_package_entity_cleanup(&mut cleanup)
        .unwrap();
    assert!(matches!(
        runtime.step_direct_package_entity_cleanup(&mut cleanup),
        FamilyCleanupStep::Pending
    ));
    let FamilyCleanupStep::Payload(payload) =
        runtime.step_direct_package_entity_cleanup(&mut cleanup)
    else {
        panic!("select first payload");
    };
    assert_eq!(payload.snapshot_seq(), 1);
    drop(payload);
    runtime.complete_direct_package_entity_cleanup_item(&mut cleanup);
    runtime.causal_scopes.test_with_inner_held(|| {
        for _ in 0..CAUSAL_OWNER_CAPACITY {
            assert_eq!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: u64::MAX,
                    identity: LeaseIdentity::EventInFlight,
                }),
                CausalAdmitResult::Applied
            );
        }
        runtime.causal_scopes.take_progress_notification();
        for _ in 0..2 {
            assert!(matches!(
                runtime.step_direct_package_entity_cleanup(&mut cleanup),
                FamilyCleanupStep::Waiting
            ));
            assert_eq!(
                cleanup.family_cursor.release,
                Some(CausalOp::Release {
                    scope_id: scope,
                    identity: identity.clone()
                })
            );
            assert_eq!(runtime.causal_operation_count(), CAUSAL_OWNER_CAPACITY);
            assert!(
                !runtime.causal_scopes.take_progress_notification(),
                "a full refusal must not wake itself"
            );
        }
    });
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    assert!(runtime.take_causal_capacity_notification());
    assert!(matches!(
        runtime.step_direct_package_entity_cleanup(&mut cleanup),
        FamilyCleanupStep::Pending
    ));
    assert!(cleanup.family_cursor.release.is_none());
    runtime.apply_causal_owner_ops();
    assert!(!runtime.causal_scopes.is_live(scope));
    let FamilyCleanupStep::Payload(payload) =
        runtime.step_direct_package_entity_cleanup(&mut cleanup)
    else {
        panic!("select second payload only after release admission");
    };
    assert_eq!(payload.snapshot_seq(), 2);
    drop(payload);
    runtime.complete_direct_package_entity_cleanup_item(&mut cleanup);
    runtime.drain_direct_package_entity_cleanup(&mut cleanup);
}

#[test]
fn retained_family_cleanup_preserves_recreation_and_holds_payload_lease() {
    use super::family_cleanup::FamilyCleanupStep;
    let runtime = family_runtime("retained-family-cleanup");
    let family = "producer.item";
    let payload = || PackageEntityMutation::Upsert {
        admission: None,
        entity_type: family.into(),
        snapshot_seq: 1,
        id: "item".into(),
        entity: serde_json::json!({"id": "item"}),
    };
    runtime.test_store_family_payload(payload());
    let scope = runtime
        .causal_scopes
        .mint_with_lease(Some(LeaseIdentity::EventInFlight))
        .unwrap();
    let old_token = runtime.test_family_causal_token(family);
    let identity = |family_token| LeaseIdentity::AdmittedEntityMutation {
        family_token,
        seq: 1,
    };
    let resync_identity = |family_token| LeaseIdentity::ProviderResyncNeed { family_token };
    assert!(runtime.causal_scopes.acquire(scope, identity(old_token)));
    assert!(
        runtime
            .causal_scopes
            .acquire(scope, resync_identity(old_token))
    );
    runtime.test_store_pending_lease(scope, family, 1);
    runtime.test_store_resync_lease(scope, family);
    let mut cleanup = HostPackageCleanup {
        unloaded_families: vec![("producer".into(), BTreeSet::from([family.into()]))],
        ..HostPackageCleanup::default()
    };
    runtime
        .begin_direct_package_entity_cleanup(&mut cleanup)
        .unwrap();
    assert!(matches!(
        runtime.step_direct_package_entity_cleanup(&mut cleanup),
        FamilyCleanupStep::Pending
    ));
    assert!(!runtime.test_family_exists(family));
    runtime.test_store_family_payload(payload());
    runtime.test_store_pending_lease(scope, family, 1);
    let new_token = runtime.test_family_causal_token(family);
    assert_ne!(old_token, new_token);
    assert!(runtime.causal_scopes.acquire(scope, identity(new_token)));
    assert!(
        runtime
            .causal_scopes
            .acquire(scope, resync_identity(new_token))
    );
    runtime.test_store_resync_lease(scope, family);
    let FamilyCleanupStep::Payload(old_payload) =
        runtime.step_direct_package_entity_cleanup(&mut cleanup)
    else {
        panic!("select the old detached payload");
    };
    assert!(
        runtime
            .causal_scopes
            .identities(scope)
            .unwrap()
            .contains(&identity(old_token))
    );
    assert_eq!(
        runtime
            .package_entities
            .lock()
            .expect("package entity model lock")
            .families[family]
            .pending_by_seq
            .len(),
        1
    );
    drop(old_payload);
    runtime.complete_direct_package_entity_cleanup_item(&mut cleanup);
    for _ in 0..20 {
        match runtime.step_direct_package_entity_cleanup(&mut cleanup) {
            FamilyCleanupStep::Complete => break,
            FamilyCleanupStep::Pending => {}
            FamilyCleanupStep::Waiting | FamilyCleanupStep::Fault => {
                panic!("the causal table is available")
            }
            FamilyCleanupStep::Payload(_) => panic!("new payload must remain live"),
        }
    }
    assert!(cleanup.unloaded_families.is_empty());
    while runtime.causal_operation_count() > 0 {
        runtime.apply_causal_owner_ops();
    }
    let live = runtime.causal_scopes.identities(scope).unwrap();
    assert!(!live.contains(&identity(old_token)));
    assert!(live.contains(&identity(new_token)));
    assert!(!live.contains(&resync_identity(old_token)));
    assert!(live.contains(&resync_identity(new_token)));
    let model = runtime
        .package_entities
        .lock()
        .expect("package entity model lock");
    let families = &model.families;
    assert_eq!(families[family].generation, 1);
    assert_eq!(families[family].pending_by_seq.len(), 1);
    assert_eq!(families[family].pending_leases.len(), 1);
    assert_eq!(families[family].resync.leases.len(), 1);
}

#[test]
fn family_boundary_retry_and_exhaustion_preserve_exact_state() {
    let runtime = family_runtime("family-boundary-generation");
    runtime.test_set_family_seq("producer.item", 1);
    let mut cleanup = HostPackageCleanup::default();
    cleanup
        .unloaded_families
        .push(("producer".into(), BTreeSet::from(["producer.item".into()])));
    runtime
        .begin_direct_package_entity_cleanup(&mut cleanup)
        .unwrap();
    let epoch = runtime.package_entities.lock().unwrap().epoch;
    runtime
        .begin_direct_package_entity_cleanup(&mut cleanup)
        .unwrap();
    assert_eq!(runtime.package_entities.lock().unwrap().epoch, epoch);
    assert_eq!(cleanup.family_epoch, Some(epoch));
    assert_eq!(
        runtime.package_entity_family_generation("producer.item"),
        Some(0)
    );
    runtime.test_exhaust_package_entity_epochs();
    let mut refused = HostPackageCleanup::default();
    refused
        .unloaded_families
        .push(("producer".into(), BTreeSet::from(["producer.item".into()])));
    assert_eq!(
        runtime.begin_direct_package_entity_cleanup(&mut refused),
        Err(PackageEntityCleanupError::GenerationExhausted)
    );
    assert_eq!(refused.family_epoch, None);
    assert_eq!(refused.unloaded_families.len(), 1);
    assert_eq!(
        runtime.package_entity_family_generation("producer.item"),
        Some(0)
    );
}

fn completed_entity_snapshot(payload: serde_json::Value) -> PluginInvocationResult {
    PluginInvocationResult::Completed(botster_core::PluginInvocationSuccess {
        request_id: RequestId("entity-snapshot-test".to_string()),
        handler: botster_core::PluginHandlerRef {
            plugin_key: PluginKey("project-pipelines".to_string()),
            kind: PluginHandlerKind::EntityProvider,
            handler_id: "runs".to_string(),
        },
        payload: Some(BoundaryJson(payload)),
    })
}

#[test]
fn convert_plugin_entity_snapshot_accepts_valid_snapshot() {
    let expected = EntityKind("project-pipelines.run".to_string());
    let (snapshot_seq, items) = HubRuntime::convert_plugin_entity_snapshot(
        &expected,
        completed_entity_snapshot(serde_json::json!({
            "type": "entity_snapshot",
            "entity_type": "project-pipelines.run",
            "snapshot_seq": 7,
            "items": [{ "id": "run-1", "status": "ready" }]
        })),
    )
    .expect("valid provider snapshot");

    assert_eq!(snapshot_seq, 7);
    assert_eq!(
        items,
        vec![serde_json::json!({ "id": "run-1", "status": "ready" })]
    );
}

#[test]
fn convert_plugin_entity_snapshot_rejects_wrong_family() {
    let expected = EntityKind("project-pipelines.run".to_string());
    let error = HubRuntime::convert_plugin_entity_snapshot(
        &expected,
        completed_entity_snapshot(serde_json::json!({
            "type": "entity_snapshot",
            "entity_type": "project-pipelines.ticket",
            "snapshot_seq": 1,
            "items": []
        })),
    )
    .expect_err("wrong provider family must fail");

    assert_eq!(error.code, "invalid_entity_provider");
    assert!(error.message.contains("returned wrong family"));
}

#[test]
fn convert_plugin_entity_snapshot_rejects_duplicate_ids() {
    let expected = EntityKind("project-pipelines.run".to_string());
    let error = HubRuntime::convert_plugin_entity_snapshot(
        &expected,
        completed_entity_snapshot(serde_json::json!({
            "type": "entity_snapshot",
            "entity_type": "project-pipelines.run",
            "snapshot_seq": 1,
            "items": [{ "id": "run-1" }, { "id": "run-1" }]
        })),
    )
    .expect_err("duplicate provider record ids must fail");

    assert_eq!(error.code, "invalid_entity_provider");
    assert!(error.message.contains("duplicate record id run-1"));
}

#[test]
fn convert_plugin_entity_snapshot_rejects_non_snapshot_frame() {
    let expected = EntityKind("project-pipelines.run".to_string());
    let error = HubRuntime::convert_plugin_entity_snapshot(
        &expected,
        completed_entity_snapshot(serde_json::json!({
            "type": "entity_upsert",
            "entity_type": "project-pipelines.run",
            "snapshot_seq": 1,
            "id": "run-1",
            "entity": { "id": "run-1" }
        })),
    )
    .expect_err("non-snapshot provider frame must fail");

    assert_eq!(error.code, "invalid_entity_provider");
    assert!(
        error
            .message
            .contains("authoritative whole-family snapshot")
    );
}

#[test]
fn convert_plugin_entity_snapshot_rejects_invalid_record() {
    let expected = EntityKind("project-pipelines.run".to_string());
    let error = HubRuntime::convert_plugin_entity_snapshot(
        &expected,
        completed_entity_snapshot(serde_json::json!({
            "type": "entity_snapshot",
            "entity_type": "project-pipelines.run",
            "snapshot_seq": 1,
            "items": [{ "status": "missing-id" }]
        })),
    )
    .expect_err("provider record without its id must fail");

    assert_eq!(error.code, "invalid_entity_provider");
}

#[test]
fn startup_plugin_failure_classification_is_fail_closed() {
    let package_failures = [
        EventPlaneStatus::RejectedUndeclared,
        EventPlaneStatus::RejectedForeign,
        EventPlaneStatus::RejectedInvalid,
        EventPlaneStatus::RejectedOversize,
        EventPlaneStatus::RejectedWildcard,
        EventPlaneStatus::RejectedCausalScope,
        EventPlaneStatus::RejectedAudience,
    ];
    for status in package_failures {
        assert!(
            HubLuaPluginLoadError::EventPlane(status).is_package_scoped_startup_failure(),
            "{status:?} must isolate only the failing package"
        );
    }
    let infrastructure_failures = [
        EventPlaneStatus::ShedFull,
        EventPlaneStatus::ShedBusy,
        EventPlaneStatus::RejectedOverFanout,
    ];
    for status in infrastructure_failures {
        assert!(
            !HubLuaPluginLoadError::EventPlane(status).is_package_scoped_startup_failure(),
            "{status:?} must stop startup"
        );
    }
    assert!(
        !HubLuaPluginLoadError::EventPlane(EventPlaneStatus::Accepted)
            .is_package_scoped_startup_failure()
    );
    assert!(
        HubLuaPluginLoadError::Package(PackageRegistryError::without_record(
            "broken.plugin",
            crate::PackageAction::Show,
            crate::PackageAdmissionReason::PackageNotInstalled,
            "classification test".to_string(),
        ))
        .is_package_scoped_startup_failure()
    );
    assert!(
        HubLuaPluginLoadError::Lua(LuaPluginRuntimeError::Load("broken".to_string()))
            .is_package_scoped_startup_failure()
    );
    assert!(
        HubLuaPluginLoadError::Lifecycle(crate::HubLifecycleError::MissingEntrypoint {
            package_name: "broken.plugin".to_string(),
        })
        .is_package_scoped_startup_failure()
    );
}

fn binding_test_node(child: serde_json::Value) -> UiNode {
    serde_json::from_value(serde_json::json!({
        "type": "panel",
        "id": "binding-test",
        "children": [child]
    }))
    .expect("binding test UiNode")
}

#[test]
fn plugin_surface_binding_admission_accepts_only_session_absolute_family() {
    let node = binding_test_node(serde_json::json!({
        "$kind": "bind_list",
        "source": "/session",
        "where": { "session_uuid": "session-1" },
        "item_template": {
            "type": "text",
            "id": "session-row",
            "props": {
                "text": { "$bind": "/session/session-1/lifecycle_class" }
            }
        },
        "empty_template": {
            "type": "text",
            "id": "session-unavailable",
            "props": { "text": "Session unavailable" }
        }
    }));

    validate_plugin_surface_node(&node, &BTreeSet::new())
        .expect("/session and item-relative bindings are admitted");
}

#[test]
fn plugin_surface_render_admission_scopes_bound_identity_to_item_templates() {
    let admitted = binding_test_node(serde_json::json!({
        "$kind": "bind_list",
        "source": "/session",
        "where": { "lifecycle_class": "current" },
        "item_template": {
            "type": "inline",
            "id": { "$bind": "@/session_uuid" },
            "children": [{
                "type": "button",
                "id": { "$kind": "bind_list_descendant_id", "key": "remove" },
                "props": {
                    "label": { "$bind": "@/lifecycle_class" },
                    "action": { "id": "contract.action" }
                }
            }]
        }
    }));
    validate_plugin_surface_node(&admitted, &BTreeSet::new())
        .expect("render admission accepts bound item-template identity");

    for rejected in [
        serde_json::json!({
            "type": "button",
            "id": { "$bind": "@/session_uuid" },
            "props": {
                "label": "Select session",
                "action": { "id": "contract.action" }
            }
        }),
        serde_json::json!({
            "type": "panel",
            "id": "binding-root",
            "children": [{
                "type": "button",
                "id": { "$bind": "@/session_uuid" },
                "props": {
                    "label": "Select session",
                    "action": { "id": "contract.action" }
                }
            }]
        }),
    ] {
        let node = serde_json::from_value(rejected).expect("authored UiNode");
        let error = validate_plugin_surface_node(&node, &BTreeSet::new())
            .expect_err("unresolved render id must fail");
        assert_eq!(error.code, "invalid_surface");
        assert!(error.message.contains("bind_list item_template"));
    }

    for rejected in [
        serde_json::json!({
            "type": "button",
            "id": { "$kind": "bind_list_descendant_id", "key": "remove" },
            "props": {
                "label": "Remove session",
                "action": { "id": "contract.action" }
            }
        }),
        serde_json::json!({
            "type": "panel",
            "id": "binding-root",
            "children": [{
                "$kind": "bind_list",
                "source": "/session",
                "item_template": {
                    "type": "button",
                    "id": { "$kind": "bind_list_descendant_id", "key": "remove" },
                    "props": {
                        "label": "Remove session",
                        "action": { "id": "contract.action" }
                    }
                }
            }]
        }),
    ] {
        let node = serde_json::from_value(rejected).expect("authored keyed UiNode");
        let error = validate_plugin_surface_node(&node, &BTreeSet::new())
            .expect_err("misplaced descendant identity must fail");
        assert_eq!(error.code, "invalid_surface");
        assert!(error.message.contains("bind_list descendant identity"));
    }
}

#[test]
fn plugin_surface_binding_admission_rejects_foreign_and_dotted_absolute_families() {
    for source in ["/workspace", "/project-pipelines.ticket", "/sessionish"] {
        let node = binding_test_node(serde_json::json!({
            "$kind": "bind_list",
            "source": source,
            "item_template": {
                "type": "text",
                "id": "row",
                "props": { "text": "row" }
            }
        }));
        node.validate().expect("generic UiNode validation");
        let error = validate_plugin_surface_binding_families(&node, &BTreeSet::new())
            .expect_err("foreign absolute binding family must be rejected");
        assert_eq!(error.code, "invalid_surface");
        assert!(error.message.contains(source), "{error:?}");
    }
}

#[test]
fn plugin_surface_binding_admission_accepts_only_exact_declared_plugin_family() {
    let admitted = BTreeSet::from(["project-pipelines.run".to_string()]);
    let node = binding_test_node(serde_json::json!({
        "$kind": "bind_list",
        "source": "/project-pipelines.run",
        "item_template": {
            "type": "text",
            "id": "run-row",
            "props": { "text": { "$bind": "@/id" } }
        }
    }));
    validate_plugin_surface_binding_families(&node, &admitted)
        .expect("exact declared plugin family is admitted");

    for source in [
        "/project-pipelines.ticket",
        "/project-pipelines.runaway",
        "/other.run",
    ] {
        let node = binding_test_node(serde_json::json!({
            "$kind": "bind_list",
            "source": source,
            "item_template": {
                "type": "text",
                "id": "row",
                "props": { "text": "row" }
            }
        }));
        validate_plugin_surface_binding_families(&node, &admitted)
            .expect_err("undeclared or foreign family must remain rejected");
    }
}

#[test]
fn plugin_surface_entity_options_admission_accepts_session_and_declared_exclude() {
    let admitted = BTreeSet::from(["project-pipelines.run".to_string()]);
    let node = binding_test_node(serde_json::json!({
        "type": "select",
        "id": "session-select",
        "props": {
            "name": "session",
            "label": "Session",
            "options_source": {
                "$kind": "entity_options",
                "source": "/session",
                "value_field": "session_uuid",
                "display_fields": ["label"],
                "order": ["label", "session_uuid"],
                "exclude": {
                    "source": "/project-pipelines.run",
                    "value_field": "session_uuid"
                }
            }
        }
    }));
    validate_plugin_surface_node(&node, &admitted)
        .expect("session source and declared package exclude are admitted");
}

#[test]
fn plugin_surface_entity_options_admission_rejects_undeclared_source_and_exclude() {
    let admitted = BTreeSet::from(["project-pipelines.run".to_string()]);
    for options_source in [
        serde_json::json!({
            "$kind": "entity_options",
            "source": "/project-pipelines.ticket",
            "value_field": "id",
            "display_fields": ["label"],
            "order": ["label"]
        }),
        serde_json::json!({
            "$kind": "entity_options",
            "source": "/session",
            "value_field": "session_uuid",
            "display_fields": ["label"],
            "order": ["label"],
            "exclude": {
                "source": "/project-pipelines.ticket",
                "value_field": "session_uuid"
            }
        }),
    ] {
        let node = binding_test_node(serde_json::json!({
            "type": "select",
            "id": "session-select",
            "props": {
                "name": "session",
                "label": "Session",
                "options_source": options_source
            }
        }));
        let error = validate_plugin_surface_node(&node, &admitted)
            .expect_err("undeclared entity-options family must fail");
        assert_eq!(error.code, "invalid_surface");
    }
}

#[test]
fn plugin_surface_entity_options_action_result_uses_same_admission() {
    let admitted = BTreeSet::from(["project-pipelines.run".to_string()]);
    let (request, mut result) = binding_action_result("/session");
    result.replacement = Some(Box::new(
        serde_json::from_value(serde_json::json!({
            "type": "select",
            "id": "session-select",
            "props": {
                "name": "session",
                "label": "Session",
                "options_source": {
                    "$kind": "entity_options",
                    "source": "/session",
                    "value_field": "session_uuid",
                    "display_fields": ["label"],
                    "order": ["label"],
                    "exclude": {
                        "source": "/project-pipelines.run",
                        "value_field": "session_uuid"
                    }
                }
            }
        }))
        .expect("entity-options replacement"),
    ));
    validate_plugin_surface_action_result(&result, &request, &admitted)
        .expect("action-result entity-options admitted with declared families");

    result.replacement = Some(Box::new(
        serde_json::from_value(serde_json::json!({
            "type": "select",
            "id": "session-select",
            "props": {
                "name": "session",
                "label": "Session",
                "options_source": {
                    "$kind": "entity_options",
                    "source": "/session",
                    "value_field": "session_uuid",
                    "display_fields": ["label"],
                    "order": ["label"],
                    "exclude": {
                        "source": "/project-pipelines.ticket",
                        "value_field": "session_uuid"
                    }
                }
            }
        }))
        .expect("rejected entity-options replacement"),
    ));
    let error = validate_plugin_surface_action_result(&result, &request, &admitted)
        .expect_err("undeclared exclude family must fail action result");
    assert_eq!(error.code, "invalid_action_result");
}

fn binding_action_result(source: &str) -> (UiActionRequest, UiActionResult) {
    let request = serde_json::from_value(serde_json::json!({
        "request_id": "binding-action-request",
        "surface_id": "contract.sessions",
        "action_id": "replace",
        "node_id": "binding-action",
        "kind": "submit"
    }))
    .expect("binding action request");
    let result = serde_json::from_value(serde_json::json!({
        "request_id": "binding-action-request",
        "surface_id": "contract.sessions",
        "action_id": "replace",
        "node_id": "binding-action",
        "state": "accepted",
        "replacement": {
            "type": "panel",
            "id": "binding-action-replacement",
            "children": [{
                "$kind": "bind_list",
                "source": source,
                "where": { "session_uuid": "session-1" },
                "item_template": {
                    "type": "button",
                    "id": "binding-action-row",
                    "props": {
                        "label": { "$bind": "@/lifecycle_class" },
                        "action": { "id": "contract.action" }
                    }
                }
            }]
        }
    }))
    .expect("binding action result");
    (request, result)
}

#[test]
fn plugin_surface_action_replacement_applies_binding_family_admission() {
    let (request, accepted) = binding_action_result("/session");
    validate_plugin_surface_action_result(&accepted, &request, &BTreeSet::new())
        .expect("/session replacement binding must be admitted");

    let (_, rejected) = binding_action_result("/workspace");
    let error = validate_plugin_surface_action_result(&rejected, &request, &BTreeSet::new())
        .expect_err("foreign replacement binding must be rejected");
    assert_eq!(error.code, "invalid_action_result");
    assert!(error.message.contains("/workspace"), "{error:?}");
}

#[test]
fn plugin_surface_authored_admission_rejects_malformed_required_label_bind() {
    let node: UiNode = serde_json::from_value(serde_json::json!({
        "type": "button",
        "id": "bound-button",
        "props": {
            "label": { "$bind": "@/lifecycle_class", "fallback": "current" },
            "action": { "id": "contract.action" }
        }
    }))
    .expect("authored button wire shape");

    let error = validate_plugin_surface_node(&node, &BTreeSet::new())
        .expect_err("malformed required label binding must fail Hub admission");
    assert_eq!(error.code, "invalid_surface");
    assert!(
        error.message.contains("may only contain $bind"),
        "{error:?}"
    );
}

#[test]
fn plugin_surface_action_replacement_rejects_bound_root_and_static_child_identity() {
    for replacement in [
        serde_json::json!({
            "type": "button",
            "id": { "$bind": "@/session_uuid" },
            "props": {
                "label": "Select session",
                "action": { "id": "contract.action" }
            }
        }),
        serde_json::json!({
            "type": "panel",
            "id": "replacement-root",
            "children": [{
                "type": "button",
                "id": { "$bind": "@/session_uuid" },
                "props": {
                    "label": "Select session",
                    "action": { "id": "contract.action" }
                }
            }]
        }),
    ] {
        let request = serde_json::from_value::<UiActionRequest>(serde_json::json!({
            "request_id": "binding-action-request",
            "surface_id": "contract.sessions",
            "action_id": "contract.action",
            "node_id": "session-stable-current",
            "kind": "submit"
        }))
        .expect("action request");
        let result = serde_json::from_value::<UiActionResult>(serde_json::json!({
            "request_id": "binding-action-request",
            "surface_id": "contract.sessions",
            "action_id": "contract.action",
            "node_id": "session-stable-current",
            "state": "accepted",
            "replacement": replacement
        }))
        .expect("action result");
        let error = validate_plugin_surface_action_result(&result, &request, &BTreeSet::new())
            .expect_err("unresolved replacement id must fail");
        assert_eq!(error.code, "invalid_action_result");
        assert!(error.message.contains("bind_list item_template"));
    }
}

#[test]
fn managed_session_core_error_diagnostic_is_kind_based_and_path_neutral() {
    let spawn_failed = CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
        MultiplexerEngineError::Runtime(botster_core::SessionRuntimeError::new(
            SessionRuntimeErrorKind::SpawnFailed,
            "connect worker control socket failed: /private/raw/path: worker control socket parent must be owned by the effective user with private permissions",
        )),
    ));
    assert_eq!(
        managed_session_core_error_class(&spawn_failed),
        "runtime.spawn_failed"
    );
    assert!(!managed_session_core_error_class(&spawn_failed).contains('/'));

    let generic = CoreDaemonError::Engine(ManagedSessionRuntimeError::Multiplexer(
        MultiplexerEngineError::Runtime(botster_core::SessionRuntimeError::new(
            SessionRuntimeErrorKind::SpawnFailed,
            "runtime detail that must not cross the diagnostic boundary",
        )),
    ));
    assert_eq!(
        managed_session_core_error_class(&generic),
        "runtime.spawn_failed"
    );
}

#[test]
fn session_reservation_refusal_classes_are_kind_based_and_path_neutral() {
    let mapped = [
        (
            SessionReservationRefusal::Occupied,
            "session_reservation.occupied",
        ),
        (SessionReservationRefusal::Busy, "session_reservation.busy"),
        (
            SessionReservationRefusal::Unsupported,
            "session_reservation.unsupported",
        ),
        (
            SessionReservationRefusal::IdentityExhausted,
            "session_reservation.identity_exhausted",
        ),
        (
            SessionReservationRefusal::Unavailable,
            "session_reservation.unavailable",
        ),
        (
            SessionReservationRefusal::InvalidToken,
            "session_reservation.invalid_token",
        ),
        (
            SessionReservationRefusal::Capacity,
            "session_reservation.capacity",
        ),
        (
            SessionReservationRefusal::SessionIdTooLong,
            "session_reservation.session_id_too_long",
        ),
    ];
    for (refusal, class) in mapped {
        assert_eq!(
            managed_session_core_error_class(&CoreDaemonError::SessionReservation(refusal)),
            class
        );
        assert!(!class.contains('/'));
    }
}

#[test]
fn multiplexer_reserved_spawn_classes_are_kind_based_and_path_neutral() {
    let runtime = botster_core::SessionRuntimeError::new(
        SessionRuntimeErrorKind::SpawnFailed,
        "connect worker control socket failed: /private/raw/path",
    );
    let mapped = [
        (
            MultiplexerEngineError::ReservedSpawn(ReservedSessionSpawnError::Refused(
                runtime.clone(),
            )),
            "engine.multiplexer.reserved_spawn.refused",
        ),
        (
            MultiplexerEngineError::ReservedSpawn(ReservedSessionSpawnError::Admitted(runtime)),
            "engine.multiplexer.reserved_spawn.admitted",
        ),
        (
            MultiplexerEngineError::InstallationAfterLaunch(Box::new(
                MultiplexerEngineError::MetadataTooLarge,
            )),
            "engine.multiplexer.installation_after_launch",
        ),
    ];
    for (error, class) in mapped {
        assert_eq!(
            managed_session_core_error_class(&CoreDaemonError::Engine(
                ManagedSessionRuntimeError::Multiplexer(error)
            )),
            class
        );
        assert!(!class.contains('/'));
    }
}

#[test]
fn explicit_resize_busy_class_is_path_neutral_and_distinct_from_control_plane_failure() {
    let session_id = SessionId("/private/session/resize-busy".to_string());
    assert_eq!(
        managed_session_core_error_class(&CoreDaemonError::ExplicitResizeBusy(session_id.clone())),
        "explicit_resize_busy"
    );
    assert_eq!(
        managed_session_core_error_class(&CoreDaemonError::ControlPlaneFailed(session_id)),
        "control_plane_failed"
    );
}

#[test]
fn bind_terminal_adapter_mapping_is_total_over_published_variants() {
    let session_id = SessionId("session".to_string());
    let subscription_id = SubscriptionId("sub".to_string());
    let mapped = [
        (
            BindTerminalAdapterError::BindBeforeAttach {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            },
            "bind_terminal_adapter.bind_before_attach",
        ),
        (
            BindTerminalAdapterError::UnknownSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            },
            "bind_terminal_adapter.unknown_subscription",
        ),
        (
            BindTerminalAdapterError::StaleGeneration {
                live: None,
                requested: TerminalSubscriptionGeneration(1),
            },
            "bind_terminal_adapter.stale_generation",
        ),
        (
            BindTerminalAdapterError::AlreadyBound {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
                generation: TerminalSubscriptionGeneration(1),
            },
            "bind_terminal_adapter.already_bound",
        ),
        (
            BindTerminalAdapterError::ControlPlaneFailed {
                session_id: session_id.clone(),
            },
            "bind_terminal_adapter.control_plane_failed",
        ),
    ];
    for (error, class) in mapped {
        assert_eq!(
            managed_session_core_error_class(&CoreDaemonError::BindTerminalAdapter(error)),
            class
        );
    }
    let control_plane =
        managed_session_core_error_class(&CoreDaemonError::ControlPlaneFailed(session_id));
    let bind_control_plane = managed_session_core_error_class(
        &CoreDaemonError::BindTerminalAdapter(BindTerminalAdapterError::ControlPlaneFailed {
            session_id: SessionId("session".to_string()),
        }),
    );
    assert_eq!(control_plane, "control_plane_failed");
    assert_eq!(
        bind_control_plane,
        "bind_terminal_adapter.control_plane_failed"
    );
    assert_ne!(control_plane, bind_control_plane);
    assert!(!control_plane.contains('/'));
    assert!(!bind_control_plane.contains('/'));
}
#[test]
fn hub_core_daemon_config_always_supplies_worker_path() {
    let config = HubStartupOptions {
        host: HostIdentityOptions {
            id: "runtime-test".to_string(),
            display_name: "Runtime Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(
            "target/botster-hub-test-data/runtime/worker-path-invariant".into(),
        ),
        session_defaults: SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: TransportBindings::default(),
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("runtime config should build");

    let core_config = core_daemon_config(&config);
    assert!(
        core_config.worker_path.is_some(),
        "hub CoreDaemonConfig must use worker-backed sessions so in-process durability adoption is unreachable"
    );
}

#[test]
fn settle_entity_publish_transfers_exact_seq_and_closes_on_error() {
    let scopes = crate::package_event_router::CausalScopeTable::new();
    let mut family = PackageEntityFamilyState {
        causal_token: Some(1),
        ..Default::default()
    };
    let accepted = scopes
        .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
            publication_token: 0,
        }))
        .expect("mint");
    assert_eq!(
        scopes.try_apply_or_wait(settle_entity_publish_op(
            &mut family,
            accepted,
            0,
            32,
            &PackageEntityPublishResult {
                ok: true,
                status: PackageEntityPublishStatus::PendingGap,
                last_accepted_seq: 0,
                high_water_seq: 32,
                resync_needed: true,
                resync_degraded: false,
            },
        )),
        crate::package_event_router::CausalWaitResult::Applied
    );
    assert_eq!(
        scopes.identities(accepted),
        Some(BTreeSet::from([
            LeaseIdentity::AdmittedEntityMutation {
                family_token: 1,
                seq: 32,
            },
            LeaseIdentity::ProviderResyncNeed { family_token: 1 },
        ]))
    );

    let errored = scopes
        .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
            publication_token: 0,
        }))
        .expect("mint error scope");
    assert_eq!(
        scopes.try_apply_or_wait(settle_entity_publish_op(
            &mut family,
            errored,
            0,
            1,
            &PackageEntityPublishResult {
                ok: false,
                status: PackageEntityPublishStatus::StaleSequence,
                last_accepted_seq: 5,
                high_water_seq: 5,
                resync_needed: false,
                resync_degraded: false,
            },
        )),
        crate::package_event_router::CausalWaitResult::Applied
    );
    assert!(!scopes.is_live(errored));
    assert_eq!(scopes.lease_count(errored), None);
}
