//! External-crate construction proof for `CoreEngineOptions`.
//!
//! This integration test is outside the `botster-hub` crate, so it uses the
//! supported public construction path: [`CoreEngineOptions::new`]. New
//! worker-queue knobs stay on their defaults. Exhaustive struct literals are
//! not a supported seam after new fields are added.

use botster_hub::CoreEngineOptions;

#[test]
fn external_crate_constructs_core_engine_options_from_new() {
    let defaults = CoreEngineOptions::default();
    let options = CoreEngineOptions::new(
        defaults.queue_capacities.clone(),
        defaults.session_worker_path.clone(),
        defaults.session_io_coalescing.clone(),
        9,
        3,
    );
    assert_eq!(options.plugin_worker_queue_capacity, 9);
    assert_eq!(options.plugin_worker_executor_concurrency, 3);
    assert_eq!(options.reserved_request_response_executors, 1);
    assert_eq!(
        options.background_queue_capacity,
        defaults.background_queue_capacity
    );
}
