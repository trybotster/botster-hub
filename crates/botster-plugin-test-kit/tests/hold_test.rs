//! Handler holds are process-wide, so this target runs alone: no other kit
//! test in this process can invoke the held handler.

use std::path::PathBuf;

use botster_plugin_test_kit::{KitHub, KitOptions};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

fn read(kit: &mut KitHub, key: &str) -> serde_json::Value {
    let response = kit
        .call_tool("kit-fixture.read", serde_json::json!({ "key": key }))
        .expect("read settles");
    assert!(response.error.is_none(), "{response:?}");
    response.plugin_tool_result
}

/// The step waits for the downstream handler, not only for the emitting
/// tool and empty queues. The hold keeps the downstream handler running, so
/// the step reaches its hang guard and reports `not_settled` naming the
/// in-flight event delivery. After the release, the same work settles.
#[test]
fn a_step_does_not_settle_while_a_downstream_handler_runs() {
    let mut kit = KitHub::start(KitOptions {
        step_deadline: std::time::Duration::from_secs(2),
        event_invocation_timeout: Some(std::time::Duration::from_secs(60)),
        ..KitOptions::temporary("downstream-hold").expect("kit root")
    })
    .expect("kit daemon starts");
    let response = kit
        .enable_package(&fixture("kit-fixture"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");

    let hold = botster_plugin_test_kit::hold_handler(
        "kit-fixture",
        "event:kit-fixture:kit-fixture.noted:1",
    );
    let blocked = kit.call_tool("kit-fixture.note", serde_json::json!({ "key": "gamma" }));
    match blocked {
        Err(botster_plugin_test_kit::KitError::NotSettled { pending }) => {
            assert!(pending.contains("package-event-kit-fixture"), "{pending}");
        }
        other => panic!("a held downstream handler must keep the step open: {other:?}"),
    }
    hold.release();
    kit.settle().expect("the released chain settles");
    assert_eq!(
        read(&mut kit, "noted"),
        serde_json::json!({ "payload": { "items": ["gamma"] } })
    );
}

/// A hold ends by release or drop. It has no expiry of its own, so a step
/// whose guard is longer than any internal bound cannot see the hold fail
/// and settle while the hold still exists.
#[test]
fn a_hold_has_no_expiry_of_its_own() {
    let hold = botster_plugin_test_kit::hold_handler("kit-fixture", "event:kit-fixture:x:1");
    assert_eq!(hold.expiry(), None);
}
