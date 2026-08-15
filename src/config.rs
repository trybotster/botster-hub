//! Profile-owned configuration policy and explicit startup options.
//!
//! The host profile resolves policy around paths, transports, session defaults,
//! and profile-owned policy for core engine knobs before handing requests to
//! `botster-core`. This module intentionally does not load concrete config
//! files yet.

use std::env;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use botster_core::{PUBLIC_QUEUE_SOURCES, PluginWorkerEngineConfig, SessionIoCoalescingPolicy};
use serde::{Deserialize, Serialize};

const DEFAULT_HOST_ID: &str = "local";
const DEFAULT_HOST_DISPLAY_NAME: &str = "Botster Hub";
const DEFAULT_SHELL: &str = "sh";
const DEFAULT_ROWS: u16 = 24;
const DEFAULT_COLS: u16 = 80;
const DEFAULT_PLUGIN_DIRECTORY: &str = "plugins";
const DEFAULT_PROVIDER_DIRECTORY: &str = "providers";
const DEFAULT_LOCAL_SOCKET: &str = "botster-hub.sock";

/// Configuration areas owned by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigArea {
    /// Device or host-level hub identity and local policy.
    Host,
    /// Provider package enablement, pinning, and capability grants.
    Providers,
    /// Client admission and transport policy.
    Clients,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubStartupOptions {
    pub host: HostIdentityOptions,
    pub data_directory: DataDirectoryOption,
    pub session_defaults: SessionDefaults,
    pub plugin_directories: DirectoryList,
    pub provider_directories: DirectoryList,
    pub transports: TransportBindings,
    pub core_engine: CoreEngineOptions,
    #[serde(default)]
    pub package_event_plane: PackageEventPlaneOptions,
}

impl Default for HubStartupOptions {
    fn default() -> Self {
        Self {
            host: HostIdentityOptions::default(),
            data_directory: DataDirectoryOption::RuntimeDefault,
            session_defaults: SessionDefaults::default(),
            plugin_directories: DirectoryList::default(),
            provider_directories: DirectoryList::default(),
            transports: TransportBindings::default(),
            core_engine: CoreEngineOptions::default(),
            package_event_plane: PackageEventPlaneOptions::default(),
        }
    }
}

impl HubStartupOptions {
    pub fn from_runtime_environment() -> Self {
        Self::default()
    }

    pub fn build_config(self) -> Result<HubConfig, HubConfigError> {
        self.build_config_for_environment(&RuntimeEnvironment::from_current_process())
    }

    pub fn build_config_for_environment(
        self,
        environment: &RuntimeEnvironment,
    ) -> Result<HubConfig, HubConfigError> {
        self.validate()?;
        let data_directory = self.data_directory.resolve(environment)?;

        let host = self.host.into_host_identity()?;
        let plugin_directories = self.plugin_directories.resolve(
            &data_directory,
            DEFAULT_PLUGIN_DIRECTORY,
            "plugin_directories",
        )?;
        let provider_directories = self.provider_directories.resolve(
            &data_directory,
            DEFAULT_PROVIDER_DIRECTORY,
            "provider_directories",
        )?;
        let transports = self.transports.resolve(&data_directory)?;

        Ok(HubConfig {
            host,
            data_directory,
            session_defaults: self.session_defaults,
            plugin_directories,
            provider_directories,
            transports,
            core_engine: self.core_engine,
            package_event_plane: self.package_event_plane.into_policy()?,
        })
    }

    fn validate(&self) -> Result<(), HubConfigError> {
        self.session_defaults.validate()?;
        self.core_engine.validate()?;
        self.transports.validate()?;
        self.package_event_plane.validate()
    }
}

pub fn build_default_config_for_runtime(
    environment: &RuntimeEnvironment,
) -> Result<HubConfig, HubConfigError> {
    HubStartupOptions::from_runtime_environment().build_config_for_environment(environment)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubConfig {
    pub host: HostIdentity,
    pub data_directory: PathBuf,
    pub session_defaults: SessionDefaults,
    pub plugin_directories: Vec<PathBuf>,
    pub provider_directories: Vec<PathBuf>,
    pub transports: TransportBindings,
    pub core_engine: CoreEngineOptions,
    #[serde(default)]
    pub package_event_plane: PackageEventPlanePolicy,
}

impl HubConfig {
    pub(crate) fn plugin_worker_config(&self) -> botster_core::PluginWorkerEngineConfig {
        self.core_engine.plugin_worker_config()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostIdentityOptions {
    pub id: String,
    pub display_name: String,
    pub fingerprint: Option<String>,
}

impl Default for HostIdentityOptions {
    fn default() -> Self {
        Self {
            id: DEFAULT_HOST_ID.to_string(),
            display_name: DEFAULT_HOST_DISPLAY_NAME.to_string(),
            fingerprint: None,
        }
    }
}

impl HostIdentityOptions {
    fn into_host_identity(self) -> Result<HostIdentity, HubConfigError> {
        validate_non_empty_string("host.id", &self.id)?;
        validate_non_empty_string("host.display_name", &self.display_name)?;

        Ok(HostIdentity {
            id: self.id,
            display_name: self.display_name,
            fingerprint: self.fingerprint,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostIdentity {
    pub id: String,
    pub display_name: String,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataDirectoryOption {
    RuntimeDefault,
    Explicit(PathBuf),
}

impl DataDirectoryOption {
    pub fn resolve(self, environment: &RuntimeEnvironment) -> Result<PathBuf, HubConfigError> {
        match self {
            Self::RuntimeDefault => environment.resolve_runtime_data_directory(),
            Self::Explicit(path) => {
                validate_non_empty_path("data_directory", &path)?;
                Ok(path)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DirectoryList {
    pub paths: Vec<PathBuf>,
}

impl DirectoryList {
    fn resolve(
        self,
        data_directory: &Path,
        default_name: &str,
        field: &'static str,
    ) -> Result<Vec<PathBuf>, HubConfigError> {
        if self.paths.is_empty() {
            return Ok(vec![data_directory.join(default_name)]);
        }

        self.paths
            .into_iter()
            .map(|path| {
                validate_non_empty_path(field, &path)?;
                Ok(if path.is_relative() {
                    data_directory.join(path)
                } else {
                    path
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDefaults {
    pub shell: String,
    pub working_directory: Option<PathBuf>,
    pub initial_rows: u16,
    pub initial_cols: u16,
}

impl Default for SessionDefaults {
    fn default() -> Self {
        Self {
            shell: DEFAULT_SHELL.to_string(),
            working_directory: None,
            initial_rows: DEFAULT_ROWS,
            initial_cols: DEFAULT_COLS,
        }
    }
}

impl SessionDefaults {
    fn validate(&self) -> Result<(), HubConfigError> {
        validate_non_empty_string("session_defaults.shell", &self.shell)?;
        validate_positive_u16("session_defaults.initial_rows", self.initial_rows)?;
        validate_positive_u16("session_defaults.initial_cols", self.initial_cols)?;

        if let Some(working_directory) = &self.working_directory {
            validate_non_empty_path("session_defaults.working_directory", working_directory)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportBindings {
    pub local_socket: Option<LocalSocketBinding>,
    pub tcp: Vec<TcpBinding>,
}

impl Default for TransportBindings {
    fn default() -> Self {
        Self {
            local_socket: Some(LocalSocketBinding {
                path: PathBuf::from(DEFAULT_LOCAL_SOCKET),
            }),
            tcp: Vec::new(),
        }
    }
}

impl TransportBindings {
    fn resolve(self, data_directory: &Path) -> Result<Self, HubConfigError> {
        Ok(Self {
            local_socket: self
                .local_socket
                .map(|binding| binding.resolve(data_directory))
                .transpose()?,
            tcp: self.tcp,
        })
    }

    fn validate(&self) -> Result<(), HubConfigError> {
        if let Some(binding) = &self.local_socket {
            validate_non_empty_path("transports.local_socket.path", &binding.path)?;
        }

        for binding in &self.tcp {
            validate_non_empty_string("transports.tcp.host", &binding.host)?;
            validate_tcp_port("transports.tcp.port", binding.port)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalSocketBinding {
    pub path: PathBuf,
}

impl LocalSocketBinding {
    fn resolve(self, data_directory: &Path) -> Result<Self, HubConfigError> {
        validate_non_empty_path("transports.local_socket.path", &self.path)?;

        Ok(Self {
            path: if self.path.is_relative() {
                data_directory.join(self.path)
            } else {
                self.path
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TcpBinding {
    pub host: String,
    pub port: u16,
}

/// Hub startup policy for core-owned runtime knobs.
///
/// Initial configurable defaults for the Hub-owned package event plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageEventPlaneOptions {
    pub payload_max_bytes: usize,
    pub subscriptions_per_plugin_max: usize,
    pub subscribers_per_event_max: usize,
    pub fanout_per_emit_max: usize,
    pub producer_queue_max_events: usize,
    pub producer_queue_max_bytes: usize,
    pub consumer_queue_max_events: usize,
    pub consumer_queue_max_bytes: usize,
    pub global_in_flight_bytes: usize,
    pub package_rate_per_sec: u32,
    pub package_burst: u32,
    pub queue_age_ms: u64,
}

impl Default for PackageEventPlaneOptions {
    fn default() -> Self {
        Self {
            payload_max_bytes: 64 * 1024,
            subscriptions_per_plugin_max: 64,
            subscribers_per_event_max: 64,
            fanout_per_emit_max: 64,
            producer_queue_max_events: 256,
            producer_queue_max_bytes: 512 * 1024,
            consumer_queue_max_events: 128,
            consumer_queue_max_bytes: 2 * 1024 * 1024,
            global_in_flight_bytes: 16 * 1024 * 1024,
            package_rate_per_sec: 100,
            package_burst: 200,
            queue_age_ms: 1_000,
        }
    }
}

impl PackageEventPlaneOptions {
    fn validate(&self) -> Result<(), HubConfigError> {
        validate_positive_usize(
            "package_event_plane.payload_max_bytes",
            self.payload_max_bytes,
        )?;
        validate_positive_usize(
            "package_event_plane.subscriptions_per_plugin_max",
            self.subscriptions_per_plugin_max,
        )?;
        validate_positive_usize(
            "package_event_plane.subscribers_per_event_max",
            self.subscribers_per_event_max,
        )?;
        validate_positive_usize(
            "package_event_plane.fanout_per_emit_max",
            self.fanout_per_emit_max,
        )?;
        validate_positive_usize(
            "package_event_plane.producer_queue_max_events",
            self.producer_queue_max_events,
        )?;
        validate_positive_usize(
            "package_event_plane.producer_queue_max_bytes",
            self.producer_queue_max_bytes,
        )?;
        validate_positive_usize(
            "package_event_plane.consumer_queue_max_events",
            self.consumer_queue_max_events,
        )?;
        validate_positive_usize(
            "package_event_plane.consumer_queue_max_bytes",
            self.consumer_queue_max_bytes,
        )?;
        validate_positive_usize(
            "package_event_plane.global_in_flight_bytes",
            self.global_in_flight_bytes,
        )?;
        if self.package_rate_per_sec == 0 {
            return Err(HubConfigError::InvalidCapacity {
                field: "package_event_plane.package_rate_per_sec",
            });
        }
        if self.package_burst == 0 {
            return Err(HubConfigError::InvalidCapacity {
                field: "package_event_plane.package_burst",
            });
        }
        if self.queue_age_ms == 0 {
            return Err(HubConfigError::InvalidCapacity {
                field: "package_event_plane.queue_age_ms",
            });
        }
        if self.payload_max_bytes > self.producer_queue_max_bytes {
            return Err(HubConfigError::InvalidEventPlaneConstraint {
                field: "package_event_plane.payload_max_bytes",
                constraint: "must be <= producer_queue_max_bytes",
            });
        }
        if self.payload_max_bytes > self.consumer_queue_max_bytes {
            return Err(HubConfigError::InvalidEventPlaneConstraint {
                field: "package_event_plane.payload_max_bytes",
                constraint: "must be <= consumer_queue_max_bytes",
            });
        }
        if self.payload_max_bytes > self.global_in_flight_bytes {
            return Err(HubConfigError::InvalidEventPlaneConstraint {
                field: "package_event_plane.payload_max_bytes",
                constraint: "must be <= global_in_flight_bytes",
            });
        }
        if self.producer_queue_max_bytes > self.global_in_flight_bytes {
            return Err(HubConfigError::InvalidEventPlaneConstraint {
                field: "package_event_plane.producer_queue_max_bytes",
                constraint: "must be <= global_in_flight_bytes",
            });
        }
        if self.fanout_per_emit_max > self.subscribers_per_event_max {
            return Err(HubConfigError::InvalidEventPlaneConstraint {
                field: "package_event_plane.fanout_per_emit_max",
                constraint: "must be <= subscribers_per_event_max",
            });
        }
        if self.package_burst < self.package_rate_per_sec {
            return Err(HubConfigError::InvalidEventPlaneConstraint {
                field: "package_event_plane.package_burst",
                constraint: "must be >= package_rate_per_sec",
            });
        }
        Ok(())
    }

    fn into_policy(self) -> Result<PackageEventPlanePolicy, HubConfigError> {
        self.validate()?;
        Ok(PackageEventPlanePolicy {
            payload_max_bytes: self.payload_max_bytes,
            subscriptions_per_plugin_max: self.subscriptions_per_plugin_max,
            subscribers_per_event_max: self.subscribers_per_event_max,
            fanout_per_emit_max: self.fanout_per_emit_max,
            producer_queue_max_events: self.producer_queue_max_events,
            producer_queue_max_bytes: self.producer_queue_max_bytes,
            consumer_queue_max_events: self.consumer_queue_max_events,
            consumer_queue_max_bytes: self.consumer_queue_max_bytes,
            global_in_flight_bytes: self.global_in_flight_bytes,
            package_rate_per_sec: self.package_rate_per_sec,
            package_burst: self.package_burst,
            queue_age: Duration::from_millis(self.queue_age_ms),
        })
    }
}

/// Validated event-plane policy passed into the router. The router does not
/// read env, files, or [`HubConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageEventPlanePolicy {
    pub payload_max_bytes: usize,
    pub subscriptions_per_plugin_max: usize,
    pub subscribers_per_event_max: usize,
    pub fanout_per_emit_max: usize,
    pub producer_queue_max_events: usize,
    pub producer_queue_max_bytes: usize,
    pub consumer_queue_max_events: usize,
    pub consumer_queue_max_bytes: usize,
    pub global_in_flight_bytes: usize,
    pub package_rate_per_sec: u32,
    pub package_burst: u32,
    #[serde(with = "queue_age_millis")]
    pub queue_age: Duration,
}

impl Default for PackageEventPlanePolicy {
    fn default() -> Self {
        PackageEventPlaneOptions::default()
            .into_policy()
            .expect("default event-plane options are valid")
    }
}

mod queue_age_millis {
    use super::Duration;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(value.as_millis() as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

/// The hub records queue, session I/O coalescing, and plugin-worker tuning here
/// so startup policy is explicit. The underlying queues, session I/O worker,
/// plugin worker engine, and delivery mechanics remain owned by `botster-core`.
/// Class-specific plugin-worker queue knobs use Core defaults. They are not
/// fields on [`HubStartupOptions`], [`HubConfig`], or this struct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreEngineOptions {
    /// Queue capacity choices keyed by core queue source name.
    pub queue_capacities: Vec<CoreQueueCapacity>,
    /// Explicit worker executable used for restart-durable daemon-backed sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_worker_path: Option<PathBuf>,
    /// Session I/O coalescing policy passed to core-owned session workers.
    pub session_io_coalescing: SessionIoCoalescingOptions,
    /// Per-plugin worker queue capacity passed to core plugin worker primitives.
    pub plugin_worker_queue_capacity: usize,
    /// Per-plugin executor concurrency passed to core plugin worker primitives.
    pub plugin_worker_executor_concurrency: usize,
}

impl Default for CoreEngineOptions {
    fn default() -> Self {
        let plugin_worker = PluginWorkerEngineConfig::default();
        Self {
            queue_capacities: PUBLIC_QUEUE_SOURCES
                .iter()
                .map(|source| CoreQueueCapacity {
                    source: source.name().to_string(),
                    capacity: source.default_capacity(),
                })
                .collect(),
            session_worker_path: None,
            session_io_coalescing: SessionIoCoalescingOptions::from(
                SessionIoCoalescingPolicy::default(),
            ),
            plugin_worker_queue_capacity: plugin_worker.per_plugin_queue_capacity,
            plugin_worker_executor_concurrency: plugin_worker.per_plugin_executor_concurrency,
        }
    }
}

impl CoreEngineOptions {
    /// Construct the original public field set. Newer worker-queue knobs use defaults.
    #[must_use]
    pub fn new(
        queue_capacities: Vec<CoreQueueCapacity>,
        session_worker_path: Option<PathBuf>,
        session_io_coalescing: SessionIoCoalescingOptions,
        plugin_worker_queue_capacity: usize,
        plugin_worker_executor_concurrency: usize,
    ) -> Self {
        Self {
            queue_capacities,
            session_worker_path,
            session_io_coalescing,
            plugin_worker_queue_capacity,
            plugin_worker_executor_concurrency,
        }
    }

    pub(crate) fn plugin_worker_config(&self) -> PluginWorkerEngineConfig {
        let defaults = PluginWorkerEngineConfig::default();
        PluginWorkerEngineConfig {
            per_plugin_queue_capacity: self.plugin_worker_queue_capacity,
            per_plugin_executor_concurrency: self.plugin_worker_executor_concurrency,
            reserved_request_response_executors: defaults.reserved_request_response_executors,
            request_response_queue_byte_capacity: defaults.request_response_queue_byte_capacity,
            background_queue_capacity: defaults.background_queue_capacity,
            background_queue_byte_capacity: defaults.background_queue_byte_capacity,
            completion_queue_capacity: defaults.completion_queue_capacity,
            completion_queue_byte_capacity: defaults.completion_queue_byte_capacity,
        }
    }

    fn validate(&self) -> Result<(), HubConfigError> {
        validate_positive_usize(
            "core_engine.plugin_worker_queue_capacity",
            self.plugin_worker_queue_capacity,
        )?;
        validate_positive_usize(
            "core_engine.plugin_worker_executor_concurrency",
            self.plugin_worker_executor_concurrency,
        )?;
        let reserved = PluginWorkerEngineConfig::default().reserved_request_response_executors;
        if reserved < 1 || reserved >= self.plugin_worker_executor_concurrency {
            return Err(HubConfigError::InvalidPluginWorkerReservation {
                field: "core_engine.plugin_worker_executor_concurrency",
            });
        }
        self.session_io_coalescing.validate()?;
        if let Some(path) = &self.session_worker_path {
            validate_non_empty_path("core_engine.session_worker_path", path)?;
        }

        for queue in &self.queue_capacities {
            validate_non_empty_string("core_engine.queue_capacities.source", &queue.source)?;
            validate_positive_usize("core_engine.queue_capacities.capacity", queue.capacity)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreQueueCapacity {
    pub source: String,
    pub capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionIoCoalescingOptions {
    pub max_output_bytes: usize,
    pub max_output_frames: usize,
    pub max_window_ms: u64,
}

impl From<SessionIoCoalescingPolicy> for SessionIoCoalescingOptions {
    fn from(policy: SessionIoCoalescingPolicy) -> Self {
        Self {
            max_output_bytes: policy.max_output_bytes,
            max_output_frames: policy.max_output_frames,
            max_window_ms: duration_millis(policy.max_window),
        }
    }
}

impl SessionIoCoalescingOptions {
    fn validate(&self) -> Result<(), HubConfigError> {
        validate_positive_usize(
            "core_engine.session_io_coalescing.max_output_bytes",
            self.max_output_bytes,
        )?;
        validate_positive_usize(
            "core_engine.session_io_coalescing.max_output_frames",
            self.max_output_frames,
        )?;

        if self.max_window_ms == 0 {
            return Err(HubConfigError::InvalidCapacity {
                field: "core_engine.session_io_coalescing.max_window_ms",
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEnvironment {
    botster_hub_data_dir: Option<PathBuf>,
    home: Option<PathBuf>,
}

impl RuntimeEnvironment {
    pub fn from_current_process() -> Self {
        Self {
            botster_hub_data_dir: env::var_os("BOTSTER_HUB_DATA_DIR").map(PathBuf::from),
            home: env::var_os("HOME").map(PathBuf::from),
        }
    }

    pub fn from_values(botster_hub_data_dir: Option<PathBuf>, home: Option<PathBuf>) -> Self {
        Self {
            botster_hub_data_dir,
            home,
        }
    }

    fn resolve_runtime_data_directory(&self) -> Result<PathBuf, HubConfigError> {
        if let Some(path) = self.botster_hub_data_dir.as_ref() {
            validate_non_empty_path("BOTSTER_HUB_DATA_DIR", path)?;
            return Ok(path.clone());
        }

        if let Some(path) = self.home.as_ref() {
            validate_non_empty_path("HOME", path)?;
            return Ok(path.join(".botster").join("hub"));
        }

        Err(HubConfigError::MissingRuntimeDataDirectory)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubConfigError {
    EmptyField {
        field: &'static str,
    },
    InvalidPath {
        field: &'static str,
    },
    InvalidPort {
        field: &'static str,
        port: u16,
    },
    MissingRuntimeDataDirectory,
    InvalidCapacity {
        field: &'static str,
    },
    InvalidPluginWorkerReservation {
        field: &'static str,
    },
    InvalidEventPlaneConstraint {
        field: &'static str,
        constraint: &'static str,
    },
}

impl fmt::Display for HubConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField { field } => write!(formatter, "{field} must not be empty"),
            Self::InvalidPath { field } => write!(formatter, "{field} must not be empty"),
            Self::InvalidPort { field, port } => {
                write!(formatter, "{field} must be between 1 and 65535, got {port}")
            }
            Self::MissingRuntimeDataDirectory => write!(
                formatter,
                "runtime data directory could not be resolved from BOTSTER_HUB_DATA_DIR or HOME"
            ),
            Self::InvalidCapacity { field } => {
                write!(formatter, "{field} must be greater than zero")
            }
            Self::InvalidPluginWorkerReservation { field } => write!(
                formatter,
                "{field} must be at least 1 and strictly less than plugin_worker_executor_concurrency"
            ),
            Self::InvalidEventPlaneConstraint { field, constraint } => {
                write!(formatter, "{field} {constraint}")
            }
        }
    }
}

impl Error for HubConfigError {}

fn validate_non_empty_string(field: &'static str, value: &str) -> Result<(), HubConfigError> {
    if value.trim().is_empty() {
        return Err(HubConfigError::EmptyField { field });
    }

    Ok(())
}

fn validate_non_empty_path(field: &'static str, path: &Path) -> Result<(), HubConfigError> {
    if path.as_os_str().is_empty() {
        return Err(HubConfigError::InvalidPath { field });
    }

    Ok(())
}

fn validate_positive_u16(field: &'static str, value: u16) -> Result<(), HubConfigError> {
    if value == 0 {
        return Err(HubConfigError::InvalidCapacity { field });
    }

    Ok(())
}

fn validate_tcp_port(field: &'static str, port: u16) -> Result<(), HubConfigError> {
    if port == 0 {
        return Err(HubConfigError::InvalidPort { field, port });
    }

    Ok(())
}

fn validate_positive_usize(field: &'static str, value: usize) -> Result<(), HubConfigError> {
    if value == 0 {
        return Err(HubConfigError::InvalidCapacity { field });
    }

    Ok(())
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_round_trip_hub_startup_options() {
        let options = HubStartupOptions::default();

        let json = serde_json::to_string(&options).expect("serialize startup options");
        let round_trip: HubStartupOptions =
            serde_json::from_str(&json).expect("deserialize startup options");

        assert_eq!(round_trip, options);
        assert!(json.contains("\"plugin_worker_queue_capacity\""));
        assert!(json.contains("\"plugin_worker_executor_concurrency\""));
        assert!(!json.contains("\"plugin_worker_class\""));
        assert!(!json.contains("\"plugin_worker_capacity\""));
    }

    #[test]
    fn omitted_worker_class_knobs_take_core_defaults() {
        let defaults = PluginWorkerEngineConfig::default();
        let options = HubStartupOptions::default();
        let mapped = options.core_engine.plugin_worker_config();
        assert_eq!(
            mapped.reserved_request_response_executors,
            defaults.reserved_request_response_executors
        );
        assert_eq!(
            mapped.background_queue_capacity,
            defaults.background_queue_capacity
        );
    }

    #[test]
    fn external_hub_consumer_constructs_core_engine_options_from_default() {
        let options = CoreEngineOptions {
            plugin_worker_queue_capacity: 9,
            plugin_worker_executor_concurrency: 3,
            ..CoreEngineOptions::default()
        };
        assert_eq!(options.plugin_worker_queue_capacity, 9);
        assert_eq!(options.plugin_worker_executor_concurrency, 3);
    }

    #[test]
    fn reserved_request_response_executors_must_leave_a_background_slot() {
        let options = HubStartupOptions {
            core_engine: CoreEngineOptions {
                plugin_worker_executor_concurrency: 1,
                ..CoreEngineOptions::default()
            },
            ..HubStartupOptions::default()
        };
        let error = options.validate().expect_err("reservation must be strict");
        assert!(matches!(
            error,
            HubConfigError::InvalidPluginWorkerReservation { .. }
        ));
    }

    #[test]
    fn core_engine_options_reject_legacy_plugin_worker_capacity() {
        let options = CoreEngineOptions::default();
        let mut value = serde_json::to_value(&options).expect("serialize core engine options");
        let queue_capacity = {
            let object = value.as_object_mut().expect("core engine options object");
            let queue_capacity = object
                .remove("plugin_worker_queue_capacity")
                .expect("queue capacity");
            object.insert("plugin_worker_capacity".to_string(), queue_capacity.clone());
            queue_capacity
        };

        let legacy_only = serde_json::from_value::<CoreEngineOptions>(value.clone())
            .expect_err("legacy-only worker capacity must fail")
            .to_string();
        assert!(legacy_only.contains("plugin_worker_capacity"));

        value
            .as_object_mut()
            .expect("core engine options object")
            .insert("plugin_worker_queue_capacity".to_string(), queue_capacity);
        let mixed = serde_json::from_value::<CoreEngineOptions>(value)
            .expect_err("mixed legacy and current worker capacity must fail")
            .to_string();
        assert!(mixed.contains("plugin_worker_capacity"));
    }

    #[test]
    fn serde_round_trip_resolved_hub_config() {
        let environment =
            RuntimeEnvironment::from_values(Some(PathBuf::from("/tmp/botster-test-data")), None);
        let config = build_default_config_for_runtime(&environment).expect("build config");

        let json = serde_json::to_string(&config).expect("serialize config");
        let round_trip: HubConfig = serde_json::from_str(&json).expect("deserialize config");

        assert_eq!(round_trip, config);
    }

    #[test]
    fn deterministic_default_options_do_not_contain_user_paths() {
        let json =
            serde_json::to_string(&HubStartupOptions::default()).expect("serialize defaults");

        assert!(!json.contains("HOME"));
        assert!(!json.contains("jason"));
        assert!(!json.contains(env!("CARGO_MANIFEST_DIR")));
    }

    #[test]
    fn runtime_data_dir_resolution_uses_injected_env() {
        let explicit = RuntimeEnvironment::from_values(
            Some(PathBuf::from("/tmp/botster-env")),
            Some(PathBuf::from("/tmp/botster-fixture-home")),
        );
        assert_eq!(
            DataDirectoryOption::RuntimeDefault
                .resolve(&explicit)
                .expect("resolve explicit env"),
            PathBuf::from("/tmp/botster-env")
        );

        let home =
            RuntimeEnvironment::from_values(None, Some(PathBuf::from("/tmp/botster-fixture-home")));
        assert_eq!(
            DataDirectoryOption::RuntimeDefault
                .resolve(&home)
                .expect("resolve home env"),
            PathBuf::from("/tmp/botster-fixture-home/.botster/hub")
        );

        let missing = RuntimeEnvironment::from_values(None, None);
        assert_eq!(
            DataDirectoryOption::RuntimeDefault.resolve(&missing),
            Err(HubConfigError::MissingRuntimeDataDirectory)
        );
    }

    #[test]
    fn invalid_values_fail_clearly() {
        assert_error_field(
            HubStartupOptions {
                host: HostIdentityOptions {
                    id: String::new(),
                    ..HostIdentityOptions::default()
                },
                ..HubStartupOptions::default()
            },
            "host.id",
        );
        assert_error_field(
            HubStartupOptions {
                host: HostIdentityOptions {
                    display_name: String::new(),
                    ..HostIdentityOptions::default()
                },
                ..HubStartupOptions::default()
            },
            "host.display_name",
        );
        assert_error_field(
            HubStartupOptions {
                data_directory: DataDirectoryOption::Explicit(PathBuf::new()),
                ..HubStartupOptions::default()
            },
            "data_directory",
        );
        assert_error_field(
            HubStartupOptions {
                session_defaults: SessionDefaults {
                    shell: String::new(),
                    ..SessionDefaults::default()
                },
                ..HubStartupOptions::default()
            },
            "session_defaults.shell",
        );
        assert_error_field(
            HubStartupOptions {
                session_defaults: SessionDefaults {
                    initial_rows: 0,
                    ..SessionDefaults::default()
                },
                ..HubStartupOptions::default()
            },
            "session_defaults.initial_rows",
        );
        assert_error_field(
            HubStartupOptions {
                session_defaults: SessionDefaults {
                    initial_cols: 0,
                    ..SessionDefaults::default()
                },
                ..HubStartupOptions::default()
            },
            "session_defaults.initial_cols",
        );
        assert_error_field(
            HubStartupOptions {
                transports: TransportBindings {
                    tcp: vec![TcpBinding {
                        host: "127.0.0.1".to_string(),
                        port: 0,
                    }],
                    ..TransportBindings::default()
                },
                ..HubStartupOptions::default()
            },
            "transports.tcp.port",
        );
        assert_error_field(
            HubStartupOptions {
                transports: TransportBindings {
                    tcp: vec![TcpBinding {
                        host: String::new(),
                        port: 3000,
                    }],
                    ..TransportBindings::default()
                },
                ..HubStartupOptions::default()
            },
            "transports.tcp.host",
        );
        assert_error_field(
            HubStartupOptions {
                core_engine: CoreEngineOptions {
                    plugin_worker_queue_capacity: 0,
                    ..CoreEngineOptions::default()
                },
                ..HubStartupOptions::default()
            },
            "core_engine.plugin_worker_queue_capacity",
        );
        assert_error_field(
            HubStartupOptions {
                core_engine: CoreEngineOptions {
                    plugin_worker_executor_concurrency: 0,
                    ..CoreEngineOptions::default()
                },
                ..HubStartupOptions::default()
            },
            "core_engine.plugin_worker_executor_concurrency",
        );
    }

    #[test]
    fn entrypoint_constructs_config() {
        let environment =
            RuntimeEnvironment::from_values(Some(PathBuf::from("/tmp/botster-entrypoint")), None);

        let config = build_default_config_for_runtime(&environment).expect("build config");

        assert_eq!(config.host.id, "local");
        assert_eq!(
            config.data_directory,
            PathBuf::from("/tmp/botster-entrypoint")
        );
        assert_eq!(
            config.plugin_directories,
            vec![PathBuf::from("/tmp/botster-entrypoint/plugins")]
        );
        assert_eq!(
            config.provider_directories,
            vec![PathBuf::from("/tmp/botster-entrypoint/providers")]
        );
        assert_eq!(
            config.transports.local_socket.expect("local socket").path,
            PathBuf::from("/tmp/botster-entrypoint/botster-hub.sock")
        );
    }

    #[test]
    fn package_event_plane_defaults_round_trip() {
        let options = PackageEventPlaneOptions::default();
        let json = serde_json::to_string(&options).expect("serialize event plane");
        let round_trip: PackageEventPlaneOptions =
            serde_json::from_str(&json).expect("deserialize event plane");
        assert_eq!(round_trip, options);
        assert_eq!(options.payload_max_bytes, 65_536);
        assert_eq!(options.subscriptions_per_plugin_max, 64);
        assert_eq!(options.subscribers_per_event_max, 64);
        assert_eq!(options.fanout_per_emit_max, 64);
        assert_eq!(options.producer_queue_max_events, 256);
        assert_eq!(options.producer_queue_max_bytes, 524_288);
        assert_eq!(options.consumer_queue_max_events, 128);
        assert_eq!(options.consumer_queue_max_bytes, 2_097_152);
        assert_eq!(options.global_in_flight_bytes, 16_777_216);
        assert_eq!(options.package_rate_per_sec, 100);
        assert_eq!(options.package_burst, 200);
        assert_eq!(options.queue_age_ms, 1_000);
        let policy = options.into_policy().expect("defaults valid");
        assert_eq!(policy.queue_age, Duration::from_millis(1_000));
    }

    #[test]
    fn package_event_plane_override_becomes_router_policy() {
        let mut options = HubStartupOptions::default();
        options.package_event_plane.payload_max_bytes = 1_024;
        options.package_event_plane.producer_queue_max_bytes = 4_096;
        options.package_event_plane.consumer_queue_max_bytes = 8_192;
        options.package_event_plane.global_in_flight_bytes = 16_384;
        options.package_event_plane.package_rate_per_sec = 5;
        options.package_event_plane.package_burst = 10;
        let environment =
            RuntimeEnvironment::from_values(Some(PathBuf::from("/tmp/botster-event-plane")), None);
        let config = options
            .build_config_for_environment(&environment)
            .expect("override must validate");
        assert_eq!(config.package_event_plane.payload_max_bytes, 1_024);
        assert_eq!(config.package_event_plane.package_burst, 10);
        let replacement = HubStartupOptions {
            package_event_plane: PackageEventPlaneOptions {
                payload_max_bytes: 2_048,
                producer_queue_max_bytes: 8_192,
                consumer_queue_max_bytes: 8_192,
                global_in_flight_bytes: 16_384,
                package_rate_per_sec: 7,
                package_burst: 9,
                ..PackageEventPlaneOptions::default()
            },
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&environment)
        .expect("second startup must replace policy");
        assert_eq!(replacement.package_event_plane.payload_max_bytes, 2_048);
        assert_eq!(replacement.package_event_plane.package_rate_per_sec, 7);
        assert_ne!(
            replacement.package_event_plane, config.package_event_plane,
            "a later HubStartupOptions must not keep the first policy"
        );
    }

    #[test]
    fn package_event_plane_rejects_zero_and_cross_field_violations() {
        let fields = [
            "package_event_plane.payload_max_bytes",
            "package_event_plane.subscriptions_per_plugin_max",
            "package_event_plane.subscribers_per_event_max",
            "package_event_plane.fanout_per_emit_max",
            "package_event_plane.producer_queue_max_events",
            "package_event_plane.producer_queue_max_bytes",
            "package_event_plane.consumer_queue_max_events",
            "package_event_plane.consumer_queue_max_bytes",
            "package_event_plane.global_in_flight_bytes",
            "package_event_plane.package_rate_per_sec",
            "package_event_plane.package_burst",
            "package_event_plane.queue_age_ms",
        ];
        for field in fields {
            let mut options = PackageEventPlaneOptions::default();
            match field {
                "package_event_plane.payload_max_bytes" => options.payload_max_bytes = 0,
                "package_event_plane.subscriptions_per_plugin_max" => {
                    options.subscriptions_per_plugin_max = 0
                }
                "package_event_plane.subscribers_per_event_max" => {
                    options.subscribers_per_event_max = 0
                }
                "package_event_plane.fanout_per_emit_max" => options.fanout_per_emit_max = 0,
                "package_event_plane.producer_queue_max_events" => {
                    options.producer_queue_max_events = 0
                }
                "package_event_plane.producer_queue_max_bytes" => {
                    options.producer_queue_max_bytes = 0
                }
                "package_event_plane.consumer_queue_max_events" => {
                    options.consumer_queue_max_events = 0
                }
                "package_event_plane.consumer_queue_max_bytes" => {
                    options.consumer_queue_max_bytes = 0
                }
                "package_event_plane.global_in_flight_bytes" => options.global_in_flight_bytes = 0,
                "package_event_plane.package_rate_per_sec" => options.package_rate_per_sec = 0,
                "package_event_plane.package_burst" => options.package_burst = 0,
                "package_event_plane.queue_age_ms" => options.queue_age_ms = 0,
                _ => unreachable!(),
            }
            let message = options.validate().expect_err("zero must fail").to_string();
            assert!(message.contains(field), "expected {field} in {message}");
        }

        let payload_gt_producer = PackageEventPlaneOptions {
            payload_max_bytes: 600_000,
            ..PackageEventPlaneOptions::default()
        };
        assert!(payload_gt_producer.validate().is_err());

        let payload_gt_consumer = PackageEventPlaneOptions {
            payload_max_bytes: 3 * 1024 * 1024,
            producer_queue_max_bytes: 4 * 1024 * 1024,
            global_in_flight_bytes: 8 * 1024 * 1024,
            ..PackageEventPlaneOptions::default()
        };
        assert!(payload_gt_consumer.validate().is_err());

        let payload_gt_global = PackageEventPlaneOptions {
            payload_max_bytes: 8_192,
            producer_queue_max_bytes: 8_192,
            consumer_queue_max_bytes: 8_192,
            global_in_flight_bytes: 4_096,
            ..PackageEventPlaneOptions::default()
        };
        assert!(payload_gt_global.validate().is_err());

        let producer_gt_global = PackageEventPlaneOptions {
            producer_queue_max_bytes: 32 * 1024 * 1024,
            ..PackageEventPlaneOptions::default()
        };
        assert!(producer_gt_global.validate().is_err());

        let fanout_gt_subscribers = PackageEventPlaneOptions {
            fanout_per_emit_max: 80,
            ..PackageEventPlaneOptions::default()
        };
        assert!(fanout_gt_subscribers.validate().is_err());

        let burst_lt_rate = PackageEventPlaneOptions {
            package_burst: 10,
            package_rate_per_sec: 20,
            ..PackageEventPlaneOptions::default()
        };
        assert!(burst_lt_rate.validate().is_err());
    }

    fn assert_error_field(options: HubStartupOptions, field: &str) {
        let environment =
            RuntimeEnvironment::from_values(Some(PathBuf::from("/tmp/botster-invalid")), None);
        let message = options
            .build_config_for_environment(&environment)
            .expect_err("expected invalid config")
            .to_string();

        assert!(
            message.contains(field),
            "expected error message to mention {field}, got {message}"
        );
    }
}
