//! Hub-owned configuration policy and explicit startup options.
//!
//! The hub resolves product-host policy around paths, transports, session
//! defaults, and core engine knobs before handing requests to `botster-core`.
//! This module intentionally does not load concrete config files yet.

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
        })
    }

    fn validate(&self) -> Result<(), HubConfigError> {
        self.session_defaults.validate()?;
        self.core_engine.validate()?;
        self.transports.validate()
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
    fn resolve(self, environment: &RuntimeEnvironment) -> Result<PathBuf, HubConfigError> {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreEngineOptions {
    pub queue_capacities: Vec<CoreQueueCapacity>,
    pub session_io_coalescing: SessionIoCoalescingOptions,
    pub plugin_worker_capacity: usize,
}

impl Default for CoreEngineOptions {
    fn default() -> Self {
        Self {
            queue_capacities: PUBLIC_QUEUE_SOURCES
                .iter()
                .map(|source| CoreQueueCapacity {
                    source: source.name().to_string(),
                    capacity: source.default_capacity(),
                })
                .collect(),
            session_io_coalescing: SessionIoCoalescingOptions::from(
                SessionIoCoalescingPolicy::default(),
            ),
            plugin_worker_capacity: PluginWorkerEngineConfig::default().per_plugin_capacity,
        }
    }
}

impl CoreEngineOptions {
    fn validate(&self) -> Result<(), HubConfigError> {
        validate_positive_usize(
            "core_engine.plugin_worker_capacity",
            self.plugin_worker_capacity,
        )?;
        self.session_io_coalescing.validate()?;

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
    xdg_data_home: Option<PathBuf>,
    home: Option<PathBuf>,
}

impl RuntimeEnvironment {
    pub fn from_current_process() -> Self {
        Self {
            botster_hub_data_dir: env::var_os("BOTSTER_HUB_DATA_DIR").map(PathBuf::from),
            xdg_data_home: env::var_os("XDG_DATA_HOME").map(PathBuf::from),
            home: env::var_os("HOME").map(PathBuf::from),
        }
    }

    pub fn from_values(
        botster_hub_data_dir: Option<PathBuf>,
        xdg_data_home: Option<PathBuf>,
        home: Option<PathBuf>,
    ) -> Self {
        Self {
            botster_hub_data_dir,
            xdg_data_home,
            home,
        }
    }

    fn resolve_runtime_data_directory(&self) -> Result<PathBuf, HubConfigError> {
        if let Some(path) = self.botster_hub_data_dir.as_ref() {
            validate_non_empty_path("BOTSTER_HUB_DATA_DIR", path)?;
            return Ok(path.clone());
        }

        if let Some(path) = self.xdg_data_home.as_ref() {
            validate_non_empty_path("XDG_DATA_HOME", path)?;
            return Ok(path.join("botster-hub"));
        }

        if let Some(path) = self.home.as_ref() {
            validate_non_empty_path("HOME", path)?;
            return Ok(path.join(".local").join("share").join("botster-hub"));
        }

        Err(HubConfigError::MissingRuntimeDataDirectory)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubConfigError {
    EmptyField { field: &'static str },
    InvalidPath { field: &'static str },
    InvalidPort { field: &'static str, port: u16 },
    MissingRuntimeDataDirectory,
    InvalidCapacity { field: &'static str },
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
                "runtime data directory could not be resolved from BOTSTER_HUB_DATA_DIR, XDG_DATA_HOME, or HOME"
            ),
            Self::InvalidCapacity { field } => {
                write!(formatter, "{field} must be greater than zero")
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
    }

    #[test]
    fn serde_round_trip_resolved_hub_config() {
        let environment = RuntimeEnvironment::from_values(
            Some(PathBuf::from("/tmp/botster-test-data")),
            None,
            None,
        );
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
        assert!(!json.contains("/Users/"));
        assert!(!json.contains(env!("CARGO_MANIFEST_DIR")));
    }

    #[test]
    fn runtime_data_dir_resolution_uses_injected_env() {
        let explicit = RuntimeEnvironment::from_values(
            Some(PathBuf::from("/tmp/botster-env")),
            Some(PathBuf::from("/tmp/xdg")),
            Some(PathBuf::from("/tmp/home")),
        );
        assert_eq!(
            DataDirectoryOption::RuntimeDefault
                .resolve(&explicit)
                .expect("resolve explicit env"),
            PathBuf::from("/tmp/botster-env")
        );

        let xdg = RuntimeEnvironment::from_values(
            None,
            Some(PathBuf::from("/tmp/xdg")),
            Some(PathBuf::from("/tmp/home")),
        );
        assert_eq!(
            DataDirectoryOption::RuntimeDefault
                .resolve(&xdg)
                .expect("resolve xdg env"),
            PathBuf::from("/tmp/xdg/botster-hub")
        );

        let home = RuntimeEnvironment::from_values(None, None, Some(PathBuf::from("/tmp/home")));
        assert_eq!(
            DataDirectoryOption::RuntimeDefault
                .resolve(&home)
                .expect("resolve home env"),
            PathBuf::from("/tmp/home/.local/share/botster-hub")
        );

        let missing = RuntimeEnvironment::from_values(None, None, None);
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
                    plugin_worker_capacity: 0,
                    ..CoreEngineOptions::default()
                },
                ..HubStartupOptions::default()
            },
            "core_engine.plugin_worker_capacity",
        );
    }

    #[test]
    fn entrypoint_constructs_config() {
        let environment = RuntimeEnvironment::from_values(
            Some(PathBuf::from("/tmp/botster-entrypoint")),
            None,
            None,
        );

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

    fn assert_error_field(options: HubStartupOptions, field: &str) {
        let environment = RuntimeEnvironment::from_values(
            Some(PathBuf::from("/tmp/botster-invalid")),
            None,
            None,
        );
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
