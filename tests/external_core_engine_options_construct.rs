//! External-crate construction proof for `CoreEngineOptions`.
//!
//! Supported seams are [`CoreEngineOptions::new`] and field overrides plus
//! `..CoreEngineOptions::default()`. Class knobs live on the nested
//! [`PluginWorkerClassOptions`] field so later knobs do not add more
//! top-level required fields.

use botster_hub::{CoreEngineOptions, PluginWorkerClassOptions};

#[test]
fn external_crate_constructs_core_engine_options_from_new_and_default() {
    let defaults = CoreEngineOptions::default();
    let from_new = CoreEngineOptions::new(
        defaults.queue_capacities.clone(),
        defaults.session_worker_path.clone(),
        defaults.session_io_coalescing.clone(),
        9,
        3,
    );
    let from_default = CoreEngineOptions {
        plugin_worker_queue_capacity: 9,
        plugin_worker_executor_concurrency: 3,
        plugin_worker_class: PluginWorkerClassOptions::default(),
        ..CoreEngineOptions::default()
    };
    assert_eq!(from_new.plugin_worker_queue_capacity, 9);
    assert_eq!(from_new.plugin_worker_executor_concurrency, 3);
    assert_eq!(
        from_new
            .plugin_worker_class
            .reserved_request_response_executors,
        1
    );
    assert_eq!(from_new, from_default);
}
