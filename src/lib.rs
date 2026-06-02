//! Public architecture facade for the `botster-hub` product host.
//!
//! `botster-hub` owns product host policy around reusable `botster-core`
//! mechanics. This crate defines hub boundary contracts and a minimal runtime
//! facade over `botster-core`; provider, cloud, Rails, WebRTC, and client
//! transport implementations intentionally live outside this scaffold.
//!
//! ```
//! let summary = botster_hub::architecture_summary();
//! assert!(summary.role_labels().contains(&"botster-core"));
//! assert!(summary
//!     .responsibilities()
//!     .iter()
//!     .any(|role| role.owns.contains("PackageManifest")));
//! ```

pub mod auth;
pub mod config;
pub mod packages;
pub mod persistence;
pub mod runtime;

pub use config::{
    CoreEngineOptions, CoreQueueCapacity, DataDirectoryOption, DirectoryList, HostIdentity,
    HostIdentityOptions, HubConfig, HubConfigError, HubStartupOptions, LocalSocketBinding,
    RuntimeEnvironment, SessionDefaults, SessionIoCoalescingOptions, TcpBinding, TransportBindings,
    build_default_config_for_runtime,
};
pub use runtime::{
    HubRuntime, HubRuntimeError, HubRuntimeObservation, HubRuntimeOutput, HubRuntimeSpawnOutcome,
};

/// Compile-checked description of the crate boundaries exposed by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchitectureSummary {
    roles: &'static [Responsibility],
}

impl ArchitectureSummary {
    /// Stable role labels used by docs, tests, and the binary smoke path.
    pub fn role_labels(&self) -> Vec<&'static str> {
        self.roles.iter().map(Responsibility::label).collect()
    }

    /// Responsibility rows for README-aligned callers.
    pub fn responsibilities(&self) -> &'static [Responsibility] {
        self.roles
    }
}

/// Named ownership boundary in the Botster package layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Responsibility {
    /// Stable short label.
    pub label: &'static str,
    /// One-sentence ownership boundary.
    pub owns: &'static str,
}

impl Responsibility {
    const fn new(label: &'static str, owns: &'static str) -> Self {
        Self { label, owns }
    }

    /// Return the stable short label.
    pub fn label(&self) -> &'static str {
        self.label
    }
}

const RESPONSIBILITIES: &[Responsibility] = &[
    Responsibility::new(
        "botster-core",
        "reusable local engine mechanics, PackageManifest, Capability, CapabilitySurface, host-profile admission contracts, and capability runtime primitives",
    ),
    Responsibility::new(
        "botster-hub",
        "product host policy over core contracts: config, persistence, auth, package enablement, lifecycle, and audit hooks",
    ),
    Responsibility::new(
        "CLI",
        "thin operator entrypoints that start or attach to a hub",
    ),
    Responsibility::new(
        "clients",
        "browser, TUI, socket, and custom renderers consuming hub contracts",
    ),
    Responsibility::new(
        "plugins/providers",
        "installable behavior packages that declare core capabilities and provenance",
    ),
    Responsibility::new(
        "external providers",
        "cloud federation, signaling relay, browser shell, and API implementations",
    ),
];

/// Return the public architecture summary used by the binary smoke path.
pub fn architecture_summary() -> ArchitectureSummary {
    ArchitectureSummary {
        roles: RESPONSIBILITIES,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn architecture_summary_names_required_roles_and_core_owned_contracts() {
        let summary = architecture_summary();

        assert_eq!(
            summary.role_labels(),
            vec![
                "botster-core",
                "botster-hub",
                "CLI",
                "clients",
                "plugins/providers",
                "external providers"
            ]
        );

        let core_role = summary
            .responsibilities()
            .iter()
            .find(|role| role.label() == "botster-core")
            .expect("summary should include botster-core ownership");
        assert!(core_role.owns.contains("PackageManifest"));
        assert!(core_role.owns.contains("Capability"));
        assert!(core_role.owns.contains("CapabilitySurface"));
        assert!(core_role.owns.contains("host-profile admission contracts"));
        assert!(core_role.owns.contains("capability runtime primitives"));

        let hub_role = summary
            .responsibilities()
            .iter()
            .find(|role| role.label() == "botster-hub")
            .expect("summary should include botster-hub ownership");
        assert!(
            hub_role
                .owns
                .contains("product host policy over core contracts")
        );
    }
}
