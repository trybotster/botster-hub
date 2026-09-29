use super::*;

#[test]
fn owner_takes_legacy_then_admitted_from_one_charged_fifo() {
    let memory =
        crate::lua_memory::LuaMemoryAccount::new(crate::config::lua_memory_limits()).unwrap();
    let spawner = HubSessionTypeSpawner::new_with_account(Arc::clone(&memory));
    let (legacy_sender, _legacy_receiver) = mpsc::channel();
    let channel_bytes =
        crate::lua_memory::layout::single_reply_bytes::<AdmittedSpawnDelivery>(true).unwrap();
    let channel_charge = memory.reserve_callback_total(channel_bytes).unwrap();
    let (admitted_sender, admitted_receiver) = spawn_reply_channel(channel_charge).unwrap();
    let item = |response, parent| PendingSessionTypeSpawn {
        _dispose_probe: None,
        session_type_id: "worker".into(),
        request: SessionTypeRequest::default(),
        package_records: package_view_for_test(Vec::new()),
        response,
        parent,
    };
    {
        let mut queue = spawner.pending.lock().unwrap();
        queue
            .try_push_back_owned(item(OrdinarySpawnReply::Legacy(legacy_sender), None))
            .unwrap_or_else(|_| panic!("the legacy queue item fits"));
        queue
            .try_push_back_owned(item(
                OrdinarySpawnReply::Admitted(admitted_sender),
                Some(memory.reserve_callback_total(0).unwrap()),
            ))
            .unwrap_or_else(|_| panic!("the admitted queue item fits"));
    }
    spawner.publish_session_type_spawn();
    assert!(matches!(
        spawner.take_pending_for_owner().unwrap().response,
        OrdinarySpawnReply::Legacy(_)
    ));
    assert!(spawner.ordinary_pending.load(Ordering::Acquire));
    assert!(matches!(
        spawner.take_pending_for_owner().unwrap().response,
        OrdinarySpawnReply::Admitted(_)
    ));
    assert!(!spawner.ordinary_pending.load(Ordering::Acquire));
    drop(admitted_receiver);
    drop(spawner);
    assert_eq!(memory.usage().1, 0);
}
