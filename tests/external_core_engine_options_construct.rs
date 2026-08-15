//! External-crate construction proof for `CoreEngineOptions`.
//!
//! This integration test is outside the `botster-hub` crate, so it uses the
//! supported public construction path: field overrides plus
//! `..CoreEngineOptions::default()`. Exhaustive struct literals are not a
//! supported seam after new fields are added.

use botster_hub::CoreEngineOptions;

#[test]
fn external_crate_constructs_core_engine_options_from_default() {
    let options = CoreEngineOptions {
        plugin_worker_queue_capacity: 9,
        plugin_worker_executor_concurrency: 3,
        ..CoreEngineOptions::default()
    };
    assert_eq!(options.plugin_worker_queue_capacity, 9);
    assert_eq!(options.plugin_worker_executor_concurrency, 3);
    assert_eq!(options.reserved_request_response_executors, 1);
}
