//! The kit drives a real Hub daemon. These tests load the in-repo fixtures.

use std::path::PathBuf;

use botster_plugin_test_kit::{
    KitHub, KitOptions, RegistrySessionState, SessionLifecycleState, session_record,
};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

fn start(name: &str) -> KitHub {
    KitHub::start(KitOptions::temporary(name).expect("kit root")).expect("kit daemon starts")
}

fn read(kit: &mut KitHub, key: &str) -> serde_json::Value {
    let response = kit
        .call_tool("kit-fixture.read", serde_json::json!({ "key": key }))
        .expect("read settles");
    assert!(response.error.is_none(), "{response:?}");
    response.plugin_tool_result
}

#[test]
fn a_plugin_that_calls_the_removed_global_events_on_fails_to_load() {
    let mut kit = start("removed-events-on");
    let response = kit
        .enable_package(&fixture("removed-events-on"))
        .expect("enable settles");
    let error = response.error.expect("the load must fail");
    let text = format!("{error:?}");
    assert!(
        text.contains("attempt to index a nil value (global 'events')"),
        "{text}"
    );
}

#[test]
fn a_tool_call_runs_the_real_handler_and_writes_plugin_db() {
    let mut kit = start("tool-call");
    let response = kit
        .enable_package(&fixture("kit-fixture"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");

    let stored = kit
        .call_tool(
            "kit-fixture.remember",
            serde_json::json!({ "key": "alpha", "value": "one" }),
        )
        .expect("call settles");
    assert!(stored.error.is_none(), "{stored:?}");
    assert_eq!(
        stored.plugin_tool_result,
        serde_json::json!({ "stored": "alpha" })
    );
    assert_eq!(
        read(&mut kit, "alpha"),
        serde_json::json!({ "payload": { "value": "one" } })
    );
    let records = kit.plugin_db("kit-fixture").expect("plugin_db reads");
    assert_eq!(records["alpha"], serde_json::json!({ "value": "one" }));
    let logs = kit.logs("kit-fixture").expect("logs read");
    assert!(
        logs.records
            .iter()
            .any(|record| record.level == "info" && record.message == "remembered"),
        "{logs:?}"
    );
    let tools = kit.list_tools().expect("tools list");
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == "kit-fixture.remember"),
        "{tools:?}"
    );
}

#[test]
fn an_emitted_event_reaches_the_downstream_handler_within_the_step() {
    let mut kit = start("emit-chain");
    let response = kit
        .enable_package(&fixture("kit-fixture"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");

    let noted = kit
        .call_tool("kit-fixture.note", serde_json::json!({ "key": "beta" }))
        .expect("call settles");
    assert_eq!(
        noted.plugin_tool_result,
        serde_json::json!({ "emitted": true })
    );
    assert_eq!(
        read(&mut kit, "noted"),
        serde_json::json!({ "payload": { "items": ["beta"] } })
    );
    assert_eq!(
        kit.emitted_events().expect("observed events read"),
        vec![serde_json::json!({
            "owner": "kit-fixture",
            "name": "kit-fixture.noted",
            "payload": { "key": "beta" },
        })]
    );
}

/// The observer keeps the events of every enabled package. A later enable
/// must not drop the earlier package's subscription.
#[test]
fn events_of_two_producers_are_both_observed_after_both_loads() {
    let mut kit = start("two-producers");
    for name in ["kit-fixture", "kit-fixture-b"] {
        let response = kit.enable_package(&fixture(name)).expect("enable settles");
        assert!(response.error.is_none(), "{response:?}");
    }
    for tool in ["kit-fixture.note", "kit-fixture-b.note"] {
        let noted = kit
            .call_tool(tool, serde_json::json!({ "key": "k" }))
            .expect("call settles");
        assert_eq!(
            noted.plugin_tool_result,
            serde_json::json!({ "emitted": true })
        );
    }
    let mut names: Vec<String> = kit
        .emitted_events()
        .expect("observed events read")
        .iter()
        .map(|event| event["name"].as_str().expect("name").to_string())
        .collect();
    names.sort();
    assert_eq!(names, ["kit-fixture-b.noted", "kit-fixture.noted"]);
}

#[test]
fn session_lifecycle_input_reaches_the_plugin_as_production_frames() {
    let mut kit = start("session-family");
    let response = kit
        .enable_package(&fixture("kit-fixture"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");

    kit.sessions_baseline(vec![session_record(
        "sess-a",
        RegistrySessionState::Running,
        Some(SessionLifecycleState::Running),
    )])
    .expect("baseline settles");
    kit.session_upsert(session_record(
        "sess-a",
        RegistrySessionState::Exited,
        Some(SessionLifecycleState::Exited { code: Some(0) }),
    ))
    .expect("upsert settles");
    kit.session_remove("sess-a").expect("remove settles");

    assert_eq!(
        read(&mut kit, "family"),
        serde_json::json!({ "payload": { "items": [
            { "type": "snapshot_begin" },
            { "type": "snapshot_chunk", "ids": ["sess-a:current"] },
            { "type": "snapshot_end" },
            { "type": "entity_upsert", "id": "sess-a", "lifecycle_class": "ended" },
            { "type": "entity_remove", "id": "sess-a" },
        ] } })
    );
}

#[test]
fn refusals_arrive_typed() {
    let mut kit = start("refusals");
    let response = kit
        .enable_package(&fixture("kit-fixture"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");

    let undeclared = kit
        .call_tool("kit-fixture.emit_undeclared", serde_json::json!({}))
        .expect("call settles");
    assert_eq!(undeclared.plugin_tool_result["ok"], false);
    assert_eq!(
        undeclared.plugin_tool_result["error"]["kind"],
        "capability_denied"
    );
    assert_eq!(
        undeclared.plugin_tool_result["error"]["detail"]["status"],
        "rejected_undeclared"
    );

    let unknown = kit
        .call_tool("kit-fixture.missing", serde_json::json!({}))
        .expect("call settles");
    let error = unknown.error.expect("an unknown tool is refused");
    assert_eq!(error.code, "unknown_tool", "{error:?}");

    assert!(matches!(
        kit.call_tool_as(
            "sess-a",
            "kit-fixture.read",
            serde_json::json!({ "key": "x" })
        ),
        Err(botster_plugin_test_kit::KitError::Unsupported {
            feature: "caller",
            gate: "G1",
        })
    ));
}

#[test]
fn routed_envelopes_are_read_from_core_without_acknowledging() {
    let mut kit = start("routed");
    let response = kit
        .enable_package(&fixture("kit-fixture"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");

    let target = botster_plugin_test_kit::EnvelopeTarget::Session {
        session_id: botster_plugin_test_kit::SessionId("sess-target".to_string()),
    };
    let routed = kit
        .call_tool(
            "kit-fixture.route",
            serde_json::json!({
                "envelope_id": "env-1",
                "target": serde_json::to_value(&target).expect("target encodes"),
                "body": "hello",
            }),
        )
        .expect("call settles");
    assert!(routed.error.is_none(), "{routed:?}");
    for _ in 0..2 {
        let envelopes = kit.routed(target.clone()).expect("routed reads");
        assert_eq!(envelopes.len(), 1, "{envelopes:?}");
        assert_eq!(envelopes[0].id.0, "env-1");
        assert_eq!(envelopes[0].payload.body, b"hello".to_vec());
    }
}

#[test]
fn published_entities_reach_a_client_subscription() {
    let mut kit = start("entities");
    let response = kit
        .enable_package(&fixture("kit-fixture"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");
    let subscribed = kit
        .subscribe_entities("kit-fixture.item")
        .expect("subscribe settles");
    assert!(subscribed.error.is_none(), "{subscribed:?}");

    let published = kit
        .call_tool(
            "kit-fixture.publish",
            serde_json::json!({ "id": "one", "label": "first" }),
        )
        .expect("call settles");
    assert_eq!(published.plugin_tool_result["ok"], true, "{published:?}");
    let frames = kit.entity_frames("kit-fixture.item");
    let text = serde_json::to_string(&frames).expect("frames encode");
    assert!(text.contains("\"first\""), "{text}");
}

/// The kit's Hub runs on a logical clock. A timer that a plugin arms after
/// the clock has moved is due one delay after arming. `advance` reports it
/// and no wall time passes.
#[test]
fn a_timer_armed_after_advance_fires_one_delay_later() {
    let mut kit = start("timer-relative");
    let response = kit
        .enable_package(&fixture("kit-fixture-timer"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");
    assert!(kit.advance(5_000).expect("advance").is_empty());

    let armed = kit
        .call_tool(
            "kit-fixture-timer.arm",
            serde_json::json!({ "delay_ms": 1_000 }),
        )
        .expect("arm settles");
    assert!(armed.error.is_none(), "{armed:?}");

    assert_eq!(kit.advance(999).expect("advance"), []);
    let fired = kit.advance(1).expect("advance");
    assert_eq!(fired.len(), 1, "{fired:?}");
    assert_eq!(fired[0].package, "kit-fixture-timer");
    assert_eq!(kit.now_ms(), 6_000);
}

/// `botster.clock` inside a handler reads the logical clock.
#[test]
fn botster_clock_inside_a_handler_returns_the_logical_time() {
    let mut kit = start("clock-in-handler");
    let response = kit
        .enable_package(&fixture("kit-fixture-timer"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");
    let read = |kit: &mut KitHub| {
        let response = kit
            .call_tool("kit-fixture-timer.clock", serde_json::json!({}))
            .expect("call settles");
        assert!(response.error.is_none(), "{response:?}");
        response.plugin_tool_result
    };
    let before = read(&mut kit);
    kit.advance(250).expect("advance");
    let after = read(&mut kit);
    assert_eq!(
        after["monotonic"].as_u64().unwrap() - before["monotonic"].as_u64().unwrap(),
        250
    );
    assert_eq!(
        after["now"].as_u64().unwrap() - before["now"].as_u64().unwrap(),
        250
    );
    assert_eq!(after["now"], 1_700_000_000_250u64);
}

/// Core's routed store delivers at least once: a receive does not remove an
/// envelope, and only the target's acknowledgement does.
#[test]
fn a_routed_envelope_is_received_again_until_its_target_acknowledges_it() {
    use botster_plugin_test_kit::{EnvelopeId, EnvelopeTarget, SessionId};
    let mut kit = start("routed-ack");
    let response = kit
        .enable_package(&fixture("kit-fixture"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");
    let target = EnvelopeTarget::Session {
        session_id: SessionId("sess-b".to_string()),
    };
    let routed = kit
        .call_tool(
            "kit-fixture.route",
            serde_json::json!({
                "envelope_id": "env-ack",
                "target": serde_json::to_value(&target).expect("target encodes"),
                "body": "hi",
            }),
        )
        .expect("call settles");
    assert!(routed.error.is_none(), "{routed:?}");

    for _ in 0..2 {
        let received = kit
            .receive_routed(target.clone(), None, 10)
            .expect("receive");
        assert_eq!(received.envelopes.len(), 1, "{received:?}");
        assert_eq!(received.envelopes[0].id.0, "env-ack");
    }
    kit.ack_routed(target.clone(), EnvelopeId("env-ack".to_string()))
        .expect("ack");
    let after_ack = kit
        .receive_routed(target, None, 10)
        .expect("receive after ack");
    assert!(after_ack.envelopes.is_empty(), "{after_ack:?}");
}

/// Timers that one `advance` crosses fire in deadline order. The resource
/// ids sort the other way here ("...-1000" before "...-200"), so an order by
/// resource id would put the 1000 ms timer first.
#[test]
fn timers_crossed_by_one_advance_fire_in_deadline_order() {
    let mut kit = start("timer-order");
    let response = kit
        .enable_package(&fixture("kit-fixture-timer"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");
    for delay_ms in [1_000, 200] {
        let armed = kit
            .call_tool(
                "kit-fixture-timer.arm",
                serde_json::json!({ "delay_ms": delay_ms }),
            )
            .expect("arm settles");
        assert!(armed.error.is_none(), "{armed:?}");
    }
    let fired = kit.advance(1_000).expect("advance");
    let ids: Vec<&str> = fired
        .iter()
        .map(|timer| timer.resource_id.as_str())
        .collect();
    assert_eq!(
        ids,
        ["timer-lua-timer-200", "timer-lua-timer-1000"],
        "{fired:?}"
    );
}
