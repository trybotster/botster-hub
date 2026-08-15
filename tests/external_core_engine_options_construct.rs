//! External-crate construction proof for `CoreEngineOptions`.
//!
//! The prior public seam is an exhaustive five-field literal. Class knobs
//! live on [`botster_hub::HubStartupOptions`], not on this struct.

use botster_hub::CoreEngineOptions;

#[test]
fn external_crate_constructs_core_engine_options_with_prior_exhaustive_literal() {
    let defaults = CoreEngineOptions::default();
    let options = CoreEngineOptions {
        queue_capacities: defaults.queue_capacities.clone(),
        session_worker_path: defaults.session_worker_path.clone(),
        session_io_coalescing: defaults.session_io_coalescing.clone(),
        plugin_worker_queue_capacity: 9,
        plugin_worker_executor_concurrency: 3,
    };
    assert_eq!(options.plugin_worker_queue_capacity, 9);
    assert_eq!(options.plugin_worker_executor_concurrency, 3);
    assert_eq!(
        options,
        CoreEngineOptions::new(
            defaults.queue_capacities,
            defaults.session_worker_path,
            defaults.session_io_coalescing,
            9,
            3,
        )
    );
}
