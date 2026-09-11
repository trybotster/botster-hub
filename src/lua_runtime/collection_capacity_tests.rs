use super::*;
use crate::lua_memory::LuaMemoryLimits;
use std::mem::size_of;
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
    let (response, receiver) = mpsc::channel();
    (
        PendingCoordinationRequest {
            terminal_drop_probe: None,
            operation: drain_op(),
            response: CoordinationReplySender::NonAcknowledge(response),
            caller: CoordinationCaller::new(),
            storage: None,
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

#[test]
fn pending_growth_overlap_refuses_when_old_plus_new_does_not_fit() {
    let memory = account(2 * slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    assert_eq!(bridge.test_pending_capacity(), 1);
    assert_eq!(bridge.test_pending_charge_bytes(), slot());
    assert_eq!(memory.usage().1, slot());
    assert!(matches!(
        enqueue(&bridge),
        Err(CoordinationRequestError::Local(
            CoordinationLocalError::Capacity
        ))
    ));
    assert_eq!(bridge.test_pending_count(), 1);
    assert_eq!(bridge.test_pending_capacity(), 1);
    assert_eq!(memory.usage().1, slot());
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
    enqueue(&bridge).unwrap();
    assert!(enqueue(&bridge).is_err());
    assert_eq!(
        bridge.test_pending_capacity(),
        2,
        "ablation reallocates before the charge so overlap is not held"
    );
    assert_eq!(bridge.test_pending_count(), 1);
    assert_eq!(bridge.test_pending_charge_bytes(), slot());
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

#[test]
fn refused_enqueue_destroys_request_and_restores_usage() {
    let memory = account(slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    let before = memory.usage().1;
    assert_eq!(before, slot());
    let error = request_typed_from_worker(bridge.clone()).expect_err("second enqueue must refuse");
    assert!(matches!(
        error,
        CoordinationRequestError::Local(CoordinationLocalError::Capacity)
    ));
    assert_eq!(error.as_str(), LUA_CALLBACK_CAPACITY_EXHAUSTED);
    assert_eq!(memory.usage().1, before);
    assert_eq!(bridge.test_pending_count(), 1);
}

#[test]
fn refused_enqueue_ablation_skips_capacity_check() {
    let memory = account(slot());
    let bridge = HubCoordinationBridge::new(Arc::clone(&memory));
    enqueue(&bridge).unwrap();
    let before = memory.usage().1;
    bridge.test_set_skip_capacity_check(true);
    let producer = bridge.clone();
    let worker = thread::spawn(move || producer.request_typed(drain_op()));
    let deadline = Instant::now() + Duration::from_secs(1);
    while bridge.test_pending_count() < 2 {
        assert!(
            Instant::now() < deadline,
            "skip-capacity request_typed must enqueue instead of refusing"
        );
        thread::yield_now();
    }
    assert_eq!(memory.usage().1, before);
    drop(bridge.take_pending());
    drop(bridge.take_pending());
    worker
        .join()
        .expect("request_typed worker")
        .expect_err("abandoned worker times out without an owner");
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
