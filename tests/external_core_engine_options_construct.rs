//! External-crate construction proof for prior exhaustive public literals.

use botster_hub::{
    CoreEngineOptions, DataDirectoryOption, DirectoryList, HostIdentityOptions, HubConfig,
    HubConfigError, HubStartupOptions, SessionDefaults, TransportBindings,
};

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

#[test]
fn external_crate_constructs_hub_startup_options_with_prior_exhaustive_literal() {
    let options = HubStartupOptions {
        host: HostIdentityOptions::default(),
        data_directory: DataDirectoryOption::RuntimeDefault,
        session_defaults: SessionDefaults::default(),
        plugin_directories: DirectoryList::default(),
        provider_directories: DirectoryList::default(),
        transports: TransportBindings::default(),
        core_engine: CoreEngineOptions::default(),
        package_event_plane: botster_hub::PackageEventPlaneOptions::default(),
    };
    assert_eq!(
        options.core_engine.plugin_worker_queue_capacity,
        CoreEngineOptions::default().plugin_worker_queue_capacity
    );
}

#[test]
fn external_crate_constructs_hub_config_with_prior_exhaustive_literal() {
    let data_directory = std::env::temp_dir().join(format!(
        "hub-options-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let built = HubStartupOptions {
        host: HostIdentityOptions::default(),
        data_directory: DataDirectoryOption::Explicit(data_directory.clone()),
        session_defaults: SessionDefaults::default(),
        plugin_directories: DirectoryList::default(),
        provider_directories: DirectoryList::default(),
        transports: TransportBindings::default(),
        core_engine: CoreEngineOptions::default(),
        package_event_plane: botster_hub::PackageEventPlaneOptions::default(),
    }
    .build_config_for_environment(&botster_hub::RuntimeEnvironment::from_values(None, None))
    .expect("build config");
    let config = HubConfig {
        host: built.host,
        data_directory: built.data_directory,
        session_defaults: built.session_defaults,
        plugin_directories: built.plugin_directories,
        provider_directories: built.provider_directories,
        transports: built.transports,
        core_engine: built.core_engine,
        package_event_plane: built.package_event_plane,
    };
    assert_eq!(
        config.core_engine.plugin_worker_executor_concurrency,
        CoreEngineOptions::default().plugin_worker_executor_concurrency
    );
    let _ = std::fs::remove_dir_all(data_directory);
}

#[test]
fn executor_concurrency_one_is_rejected_at_hub_construction() {
    let data_directory = std::env::temp_dir().join(format!(
        "hub-concurrency-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let error = HubStartupOptions {
        host: HostIdentityOptions::default(),
        data_directory: DataDirectoryOption::Explicit(data_directory.clone()),
        session_defaults: SessionDefaults::default(),
        plugin_directories: DirectoryList::default(),
        provider_directories: DirectoryList::default(),
        transports: TransportBindings::default(),
        core_engine: CoreEngineOptions {
            plugin_worker_executor_concurrency: 1,
            ..CoreEngineOptions::default()
        },
        package_event_plane: botster_hub::PackageEventPlaneOptions::default(),
    }
    .build_config_for_environment(&botster_hub::RuntimeEnvironment::from_values(None, None))
    .expect_err("concurrency 1 must leave a background slot");
    assert!(matches!(
        error,
        HubConfigError::InvalidPluginWorkerReservation { .. }
    ));
    let _ = std::fs::remove_dir_all(data_directory);
}
