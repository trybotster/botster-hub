//! The Lua spec runner, run in-process over the in-repo specs.

use std::path::PathBuf;

use botster_plugin_test_kit::spec::run_spec_file;

fn crate_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn the_fixture_specs_pass_against_the_real_hub() {
    let outcomes = run_spec_file(
        &crate_path("fixtures"),
        &crate_path("specs/kit_fixture_spec.lua"),
    );
    assert_eq!(outcomes.len(), 8, "{outcomes:?}");
    let failures: Vec<_> = outcomes
        .iter()
        .filter(|outcome| outcome.failure.is_some())
        .collect();
    assert!(failures.is_empty(), "{failures:#?}");
}

/// The runner must report a failed assertion and an unresolvable load as
/// failures. A runner that swallows them would make every spec pass.
#[test]
fn a_failing_assertion_and_a_failing_load_are_reported_as_failures() {
    let outcomes = run_spec_file(
        &crate_path("fixtures"),
        &crate_path("specs/failing_spec.lua"),
    );
    assert_eq!(outcomes.len(), 2, "{outcomes:?}");
    let messages: Vec<&str> = outcomes
        .iter()
        .map(|outcome| outcome.failure.as_deref().expect("each test must fail"))
        .collect();
    assert!(
        messages[0].contains("expected equal values"),
        "{messages:?}"
    );
    assert!(messages[1].contains("failed to load"), "{messages:?}");
}

#[test]
fn a_spec_that_does_not_parse_is_one_failure() {
    let directory = std::env::temp_dir().join(format!("runner-parse-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("temp dir");
    let spec = directory.join("broken_spec.lua");
    std::fs::write(&spec, "local kit = require(\"botster.test\"\nkit.test(").expect("write spec");
    let outcomes = run_spec_file(&directory, &spec);
    std::fs::remove_dir_all(&directory).expect("remove temp dir");
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert!(outcomes[0].failure.is_some());
}
