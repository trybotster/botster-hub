use super::*;
use crate::data_plane::driver::CoreSubmissionStorage;
use crate::lua_memory::LuaMemoryLimits;
use botster_core::PluginKey;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

fn slot() -> usize {
    size_of::<PendingCoordinationRequest>()
}

fn account(total: usize) -> Arc<LuaMemoryAccount> {
    LuaMemoryAccount::new(LuaMemoryLimits {
        per_vm_bytes: 64 * 1024,
        total_vm_bytes: 64 * 1024,
        per_callback_bytes: total.max(1),
        total_callback_bytes: total.max(1),
    })
    .unwrap()
}

fn drain_op() -> PendingCoordinationOperation {
    PendingCoordinationOperation::Drain {
        target: EnvelopeTarget::Topic {
            topic: "collection-capacity".into(),
        },
        after: None,
        limit: 1,
    }
}

fn pending_request() -> (
    PendingCoordinationRequest,
    mpsc::Receiver<CoordinationReply>,
) {
    let (response, receiver) = mpsc::sync_channel(1);
    (
        PendingCoordinationRequest {
            terminal_drop_probe: None,
            operation: drain_op(),
            response: CoordinationReplySender::NonAcknowledge(response),
            caller: CoordinationCaller::new(),
            storage: None,
            entry: None,
        },
        receiver,
    )
}

fn enqueue(
    bridge: &HubCoordinationBridge,
) -> Result<mpsc::Receiver<CoordinationReply>, CoordinationRequestError> {
    let (request, receiver) = pending_request();
    bridge.enqueue(request)?;
    Ok(receiver)
}

fn request_typed_from_worker(
    bridge: HubCoordinationBridge,
) -> Result<HubCoordinationResponse, CoordinationRequestError> {
    thread::spawn(move || bridge.request_typed(drain_op()))
        .join()
        .expect("request_typed worker")
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .expect("panic message")
}

fn assert_overlap_refuses(bridge: &HubCoordinationBridge, memory: &Arc<LuaMemoryAccount>) {
    enqueue(bridge).unwrap();
    assert_eq!(bridge.test_pending_capacity(), 1);
    assert_eq!(bridge.test_pending_charge_bytes(), slot());
    assert_eq!(memory.usage().1, slot());
    assert!(matches!(
        enqueue(bridge),
        Err(CoordinationRequestError::Local(
            CoordinationLocalError::Capacity
        ))
    ));
    assert_eq!(bridge.test_pending_count(), 1);
    assert_eq!(bridge.test_pending_capacity(), 1);
    assert_eq!(memory.usage().1, slot());
}

#[test]
fn pending_growth_overlap_refuses_when_old_plus_new_does_not_fit() {
    let memory = account(2 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    assert_overlap_refuses(&bridge, &memory);
}

#[test]
fn pending_growth_overlap_succeeds_when_old_plus_new_fits() {
    let memory = account(slot() + 2 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    enqueue(&bridge).unwrap();
    assert_eq!(bridge.test_pending_capacity(), 2);
    assert_eq!(bridge.test_pending_charge_bytes(), 2 * slot());
    assert_eq!(memory.usage().1, 2 * slot());
}

#[test]
fn pending_growth_overlap_ablation_charges_after_grow() {
    let memory = account(2 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    bridge.test_set_charge_after_grow(true);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_overlap_refuses(&bridge, &memory);
    }));
    let message = match result {
        Err(payload) => panic_message(payload),
        Ok(()) => panic!("charge-after-grow ablation must fail the overlap helper"),
    };
    assert!(
        message.contains("pending_capacity") || message.contains("left:") || message.contains("1"),
        "unexpected panic: {message}"
    );
}

#[test]
fn pending_saturation_refuses_when_total_is_full() {
    let memory = account(slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    assert!(matches!(
        enqueue(&bridge),
        Err(CoordinationRequestError::Local(
            CoordinationLocalError::Capacity
        ))
    ));
    assert_eq!(bridge.test_pending_count(), 1);
    assert_eq!(memory.usage().1, slot());
}

#[test]
fn pending_saturation_ablation_skips_capacity_check() {
    let memory = account(slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    bridge.test_set_skip_capacity_check(true);
    enqueue(&bridge).unwrap();
    enqueue(&bridge).unwrap();
    assert_eq!(bridge.test_pending_count(), 2);
    assert_eq!(bridge.test_pending_charge_bytes(), 0);
    assert_eq!(memory.usage().1, 0);
}

fn assert_refused_enqueue_destroys(bridge: &HubCoordinationBridge, memory: &Arc<LuaMemoryAccount>) {
    enqueue(bridge).unwrap();
    let before = memory.usage().1;
    let dropped = Arc::new(AtomicBool::new(false));
    let (response, _receiver) = mpsc::sync_channel(1);
    let entry = memory.reserve_shared_callback_storage(8).unwrap();
    let continuation = memory.reserve_shared_callback_storage(8).unwrap();
    let disposal = memory.reserve_shared_callback_storage(8).unwrap();
    let reply = memory.reserve_shared_callback_storage(8).unwrap();
    let charged = memory.usage().1;
    let error = bridge.enqueue(PendingCoordinationRequest {
        terminal_drop_probe: Some(Box::new(DropFlag(Arc::clone(&dropped)))),
        operation: drain_op(),
        response: CoordinationReplySender::NonAcknowledge(response),
        caller: CoordinationCaller::new(),
        storage: Some(CoordinationStorage {
            core: CoreSubmissionStorage {
                request: entry,
                reply,
            },
            continuation,
            disposal,
        }),
        entry: None,
    });
    assert!(matches!(
        error,
        Err(CoordinationRequestError::Local(
            CoordinationLocalError::Capacity
        ))
    ));
    assert!(dropped.load(Ordering::Acquire), "probe must run on refuse");
    assert_eq!(memory.usage().1, before);
    assert_eq!(charged, before + 32);
    assert_eq!(bridge.test_pending_count(), 1);
}

#[test]
fn refused_enqueue_destroys_request_and_restores_usage() {
    let memory = account(slot() + 32);
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    assert_refused_enqueue_destroys(&bridge, &memory);
    let error = request_typed_from_worker(bridge.clone()).expect_err("second enqueue must refuse");
    assert!(matches!(
        error,
        CoordinationRequestError::Local(CoordinationLocalError::Capacity)
    ));
    assert_eq!(error.as_str(), LUA_CALLBACK_CAPACITY_EXHAUSTED);
}

#[test]
fn request_typed_entry_admission_refuses_when_entry_does_not_fit() {
    let memory = account(slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    let before = memory.usage().1;
    let error = request_typed_from_worker(bridge.clone()).expect_err("entry cannot fit");
    assert!(matches!(
        error,
        CoordinationRequestError::Local(CoordinationLocalError::Capacity)
    ));
    assert_eq!(memory.usage().1, before);
}

#[test]
fn request_typed_entry_then_collection_growth_boundaries() {
    let e = super::nonacknowledge_entry_bytes(&drain_op()).unwrap();
    assert!(e > 0);
    let slot = slot();
    let funded = 3 * slot + e;
    let memory = account(funded);
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    let baseline = memory.usage().1;
    assert_eq!(baseline, slot);
    let producer = bridge.clone();
    let worker = thread::spawn(move || producer.request_typed(drain_op()));
    let deadline = Instant::now() + Duration::from_secs(1);
    while bridge.test_pending_count() < 2 {
        assert!(
            Instant::now() < deadline,
            "3*slot+e must admit entry and grow the collection"
        );
        thread::yield_now();
    }
    assert_eq!(bridge.test_pending_charge_bytes(), 2 * slot);
    drop(bridge.take_pending());
    drop(bridge.take_pending());
    let _ = worker.join();

    let memory = account(funded - 1);
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    let baseline = memory.usage().1;
    let error = request_typed_from_worker(bridge.clone());
    assert!(
        matches!(
            error,
            Err(CoordinationRequestError::Local(
                CoordinationLocalError::Capacity
            ))
        ),
        "3*slot+e-1 must refuse growth after admitting entry"
    );
    assert_eq!(memory.usage().1, baseline);
    assert_eq!(bridge.test_pending_count(), 1);
}

#[test]
fn refused_enqueue_ablation_skips_capacity_check() {
    let memory = account(slot() + 32);
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    bridge.test_set_skip_capacity_check(true);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_refused_enqueue_destroys(&bridge, &memory);
    }));
    drop(bridge.take_pending());
    drop(bridge.take_pending());
    let message = match result {
        Err(payload) => panic_message(payload),
        Ok(()) => panic!("skip-capacity ablation must fail the refuse helper"),
    };
    assert!(
        message.contains("Capacity") || message.contains("Local") || message.contains("probe"),
        "unexpected panic: {message}"
    );
}

#[test]
fn pending_drain_keeps_capacity_charge() {
    let memory = account(8 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    enqueue(&bridge).unwrap();
    assert!(matches!(
        bridge.take_pending_for_owner(),
        CoordinationIngressPoll::Ready(_)
    ));
    assert!(matches!(
        bridge.take_pending_for_owner(),
        CoordinationIngressPoll::Ready(_)
    ));
    assert_eq!(bridge.test_pending_count(), 0);
    assert_eq!(bridge.test_pending_capacity(), 2);
    assert_eq!(bridge.test_pending_charge_bytes(), 2 * slot());
    assert_eq!(memory.usage().1, 2 * slot());
}

#[test]
fn pending_drain_ablation_releases_capacity_on_pop() {
    let memory = account(8 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    bridge.test_set_release_capacity_on_pop(true);
    enqueue(&bridge).unwrap();
    enqueue(&bridge).unwrap();
    let _ = bridge.take_pending_for_owner();
    let _ = bridge.take_pending_for_owner();
    assert_eq!(bridge.test_pending_charge_bytes(), 0);
    assert_eq!(memory.usage().1, 0);
}

#[test]
fn pending_reuse_does_not_recharge() {
    let memory = account(8 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    enqueue(&bridge).unwrap();
    let cap = bridge.test_pending_capacity();
    let charge = bridge.test_pending_charge_bytes();
    let _ = bridge.take_pending_for_owner();
    let _ = bridge.take_pending_for_owner();
    enqueue(&bridge).unwrap();
    enqueue(&bridge).unwrap();
    assert_eq!(bridge.test_pending_capacity(), cap);
    assert_eq!(bridge.test_pending_charge_bytes(), charge);
    assert_eq!(memory.usage().1, charge);
}

#[test]
fn pending_reuse_ablation_always_grows() {
    let memory = account(8 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    enqueue(&bridge).unwrap();
    let _ = bridge.take_pending_for_owner();
    let _ = bridge.take_pending_for_owner();
    bridge.test_set_always_grow(true);
    enqueue(&bridge).unwrap();
    assert_eq!(bridge.test_pending_capacity(), 4);
    assert_eq!(bridge.test_pending_charge_bytes(), 4 * slot());
}

#[test]
fn seal_take_releases_capacity_only_when_taken_buffer_drops() {
    let memory = account(8 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    enqueue(&bridge).unwrap();
    assert_eq!(memory.usage().1, 2 * slot());
    assert!(bridge.dispose_terminal_pending());
    assert_eq!(bridge.test_pending_count(), 0);
    assert_eq!(bridge.test_pending_charge_bytes(), 0);
    assert_eq!(memory.usage().1, 0);
}

#[test]
fn seal_take_ablation_leaves_charge_on_source() {
    let memory = account(8 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    bridge.test_set_take_without_charge(true);
    enqueue(&bridge).unwrap();
    enqueue(&bridge).unwrap();
    assert!(bridge.dispose_terminal_pending());
    assert_eq!(bridge.test_pending_count(), 0);
    assert_eq!(bridge.test_pending_charge_bytes(), 2 * slot());
    assert_eq!(memory.usage().1, 2 * slot());
}

fn bind_coordination(memory: Arc<LuaMemoryAccount>) -> mlua::Lua {
    let lua = mlua::Lua::new();
    let table = super::coordination_table(
        &lua,
        PluginKey("capacity.plugin".into()),
        HubCoordinationBridge::new(Arc::clone(&memory)),
        memory,
    )
    .unwrap();
    lua.globals().set("coordination", table).unwrap();
    lua
}

#[test]
fn publish_and_drain_capacity_raise_precreated_lua_string() {
    let memory = account(crate::lua_memory::layout::lua_reference_bytes());
    let lua = bind_coordination(Arc::clone(&memory));
    let before = memory.usage().1;
    for call in [
        "coordination.publish, { id = 'e1', target = { type = 'topic', topic = 't' } }",
        "coordination.drain, { target = { type = 'topic', topic = 't' } }",
    ] {
        let chunk = format!(
            r#"
            local kept = {{}}
            for i = 1, 1000 do
                local ok, err = pcall({call})
                assert(not ok)
                assert(tostring(err) == {text:?})
                assert(not tostring(err):find('runtime error:', 1, true))
                kept[i] = err
            end
            for i = 2, 1000 do
                assert(rawequal(kept[1], kept[i]))
            end
            return true
            "#,
            text = LUA_CALLBACK_CAPACITY_EXHAUSTED
        );
        lua.load(&chunk).eval::<bool>().unwrap();
    }
    assert_eq!(memory.usage().1, before);
}
