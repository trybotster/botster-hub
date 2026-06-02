//! Public architecture facade for the `botster-hub` first-party host profile.
//!
//! `botster-hub` is a trusted profile over reusable `botster-core` mechanics.
//! This crate defines profile-owned policy seams and a minimal runtime facade
//! over `botster-core`; provider, cloud, Rails, WebRTC, and client transport
//! implementations intentionally live outside this scaffold.
//!
//! ```
//! let profile = botster_hub::host_profile();
//! assert_eq!(profile.id, "botster-hub");
//! assert!(profile.capability_surfaces().contains(
//!     &botster_core::CapabilitySurface::SignalingRelay,
//! ));
//! ```

pub mod auth;
pub mod config;
pub mod packages;
pub mod persistence;
pub mod profile;
pub mod runtime;

pub use config::{
    CoreEngineOptions, CoreQueueCapacity, DataDirectoryOption, DirectoryList, HostIdentity,
    HostIdentityOptions, HubConfig, HubConfigError, HubStartupOptions, LocalSocketBinding,
    RuntimeEnvironment, SessionDefaults, SessionIoCoalescingOptions, TcpBinding, TransportBindings,
    build_default_config_for_runtime,
};
pub use profile::{
    CoreRuntimeRole, HostProfileManifest, HostProfileTrust, PolicyArea, Responsibility,
    host_profile,
};
pub use runtime::{
    HubRuntime, HubRuntimeError, HubRuntimeObservation, HubRuntimeOutput, HubRuntimeSpawnOutcome,
};
