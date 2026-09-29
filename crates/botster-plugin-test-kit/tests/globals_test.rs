//! The strict-globals check against the real sandbox's global set.

use std::path::PathBuf;

use botster_plugin_test_kit::globals::undefined_globals_in_plugin;
use botster_plugin_test_kit::{KitHub, KitOptions};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

fn start(name: &str) -> KitHub {
    KitHub::start(KitOptions::temporary(name).expect("kit root")).expect("kit daemon starts")
}

/// The set comes from the running runtime, not from a list kept by hand.
#[test]
fn the_sandbox_global_set_is_read_from_the_real_runtime() {
    let mut kit = start("sandbox-globals");
    let defined = kit.sandbox_globals().expect("the probe lists the globals");
    // `require` is one of them: the real set is what the sandbox says, not
    // what a hand-kept list assumed.
    for name in [
        "botster", "string", "table", "pairs", "pcall", "tostring", "require",
    ] {
        assert!(defined.contains(name), "{name} is a sandbox global");
    }
    for name in ["log", "os", "io", "dofile"] {
        assert!(!defined.contains(name), "{name} is not a sandbox global");
    }
}

/// A call to an undefined global in a branch that no call reaches passes the
/// runtime spec and is found by the check.
#[test]
fn an_undefined_global_in_an_unreached_branch_is_found_although_the_tool_call_passes() {
    let mut kit = start("bad-global");
    let response = kit
        .enable_package(&fixture("kit-fixture-badglobal"))
        .expect("enable settles");
    assert!(response.error.is_none(), "{response:?}");
    let called = kit
        .call_tool("kit-fixture-badglobal.noop", serde_json::json!({}))
        .expect("call settles");
    assert_eq!(
        called.plugin_tool_result,
        serde_json::json!({ "ok": true }),
        "the runtime spec passes: the bad branch is never reached"
    );
    let defined = kit.sandbox_globals().expect("globals");
    let found = undefined_globals_in_plugin(&fixture("kit-fixture-badglobal"), &defined)
        .expect("the plugin parses");
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].usage.name, "log");
    assert_eq!(found[0].file, PathBuf::from("plugin.lua"));
    assert_eq!((found[0].usage.line, found[0].usage.write), (5, false));
}

/// The in-repo fixtures that real specs load are clean.
#[test]
fn the_clean_fixtures_have_no_undefined_globals() {
    let mut kit = start("clean-globals");
    let defined = kit.sandbox_globals().expect("globals");
    for name in [
        "kit-fixture",
        "kit-fixture-b",
        "kit-fixture-timer",
        "kit-fixture-consumer",
    ] {
        let found = undefined_globals_in_plugin(&fixture(name), &defined).expect("parses");
        assert!(found.is_empty(), "{name}: {found:?}");
    }
}
