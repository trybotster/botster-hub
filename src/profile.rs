//! First-party host profile metadata for `botster-hub`.
//!
//! The profile owns trusted Botster policy and composes the policy-free
//! production path through `botster_core_daemon::CoreDaemon`. This module is
//! static host-profile metadata, not a marketplace manifest parser or package
//! lifecycle engine.

use botster_core::{Capability, CapabilitySurface};

/// Compile-checked manifest for the first-party Botster host profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostProfileManifest {
    /// Stable profile identifier.
    pub id: &'static str,
    /// Human-readable profile name.
    pub name: &'static str,
    /// Trust tier for the profile.
    pub trust: HostProfileTrust,
    /// Embedded core role consumed by this profile.
    pub core_role: CoreRuntimeRole,
    /// Profile-owned policy areas.
    pub policy_areas: &'static [PolicyArea],
    /// Capability surfaces governed by the profile and declared by packages.
    pub capability_surfaces: &'static [CapabilitySurface],
    /// README-aligned package responsibility rows.
    pub responsibilities: &'static [Responsibility],
}

impl HostProfileManifest {
    /// Stable role labels used by docs, tests, and the binary smoke path.
    #[must_use]
    pub fn role_labels(&self) -> Vec<&'static str> {
        self.responsibilities
            .iter()
            .map(Responsibility::label)
            .collect()
    }

    /// Capability surfaces governed by this host profile.
    #[must_use]
    pub const fn capability_surfaces(&self) -> &'static [CapabilitySurface] {
        self.capability_surfaces
    }

    /// Capability grants the first-party hub profile admits by default.
    #[must_use]
    pub fn default_capability_grants(&self) -> Vec<Capability> {
        default_capability_grants()
    }

    /// Responsibility rows for README-aligned callers.
    #[must_use]
    pub const fn responsibilities(&self) -> &'static [Responsibility] {
        self.responsibilities
    }
}

/// Profile trust tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostProfileTrust {
    /// First-party trusted Botster profile shipped by the host.
    FirstPartyTrusted,
}

/// Hub-facing role for the embedded core dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreRuntimeRole {
    /// Core facade consumed by the profile.
    pub facade: &'static str,
    /// Runtime feature profile used by the facade.
    pub runtime_feature: &'static str,
    /// One-sentence boundary description.
    pub owns: &'static str,
}

/// Policy area owned by the first-party profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyArea {
    Auth,
    Config,
    Persistence,
    ProvidersAndPackages,
    TransportsAndAdapters,
    AdmissionAndCapabilities,
    Lifecycle,
    Audit,
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
    #[must_use]
    pub const fn label(&self) -> &'static str {
        self.label
    }
}

const CORE_RUNTIME_ROLE: CoreRuntimeRole = CoreRuntimeRole {
    facade: "botster_core_daemon::CoreDaemon",
    runtime_feature: "local-runtime",
    owns: "policy-free local PTY/process mechanics consumed through the typed core daemon facade",
};

const POLICY_AREAS: &[PolicyArea] = &[
    PolicyArea::Auth,
    PolicyArea::Config,
    PolicyArea::Persistence,
    PolicyArea::ProvidersAndPackages,
    PolicyArea::TransportsAndAdapters,
    PolicyArea::AdmissionAndCapabilities,
    PolicyArea::Lifecycle,
    PolicyArea::Audit,
];

const CAPABILITY_SURFACES: &[CapabilitySurface] = &[
    CapabilitySurface::ClientAdmission,
    CapabilitySurface::PairingInvites,
    CapabilitySurface::SignalingRelay,
    CapabilitySurface::HubPresence,
    CapabilitySurface::BrowserShell,
    CapabilitySurface::Secrets,
    CapabilitySurface::Crypto,
    CapabilitySurface::Network,
    CapabilitySurface::Surfaces,
    CapabilitySurface::SessionActions,
    CapabilitySurface::Mcp,
    CapabilitySurface::PluginDb,
    CapabilitySurface::Filesystem,
    CapabilitySurface::Timers,
];

fn default_capability_grants() -> Vec<Capability> {
    vec![
        Capability {
            surface: CapabilitySurface::ClientAdmission,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::PairingInvites,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::SignalingRelay,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::HubPresence,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::BrowserShell,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::Secrets,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::Crypto,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::Network,
            scope: Some("http".to_string()),
        },
        Capability {
            surface: CapabilitySurface::Network,
            scope: Some("websocket".to_string()),
        },
        Capability {
            surface: CapabilitySurface::Surfaces,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::SessionActions,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::SessionActions,
            scope: Some("session_template_spawn".to_string()),
        },
        Capability {
            surface: CapabilitySurface::SessionActions,
            scope: Some("session_template_managed_git_spawn".to_string()),
        },
        Capability {
            surface: CapabilitySurface::Mcp,
            scope: None,
        },
        Capability {
            surface: CapabilitySurface::PluginDb,
            scope: Some("project-pipelines".to_string()),
        },
        Capability {
            surface: CapabilitySurface::PluginDb,
            scope: Some("botster-workspaces".to_string()),
        },
        Capability {
            surface: CapabilitySurface::Filesystem,
            scope: Some("workspace".to_string()),
        },
        Capability {
            surface: CapabilitySurface::Timers,
            scope: Some("callbacks".to_string()),
        },
    ]
}

const RESPONSIBILITIES: &[Responsibility] = &[
    Responsibility::new(
        "botster-core",
        "policy-free reusable local engine mechanics and transport-neutral primitives",
    ),
    Responsibility::new(
        "botster-hub",
        "trusted first-party host profile policy, startup composition, admission, lifecycle, and audit hooks",
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
        "installable behavior packages that declare capabilities and provenance",
    ),
    Responsibility::new(
        "external providers",
        "cloud federation, signaling relay, browser shell, and API implementations",
    ),
];

const FIRST_PARTY_HOST_PROFILE: HostProfileManifest = HostProfileManifest {
    id: "botster-hub",
    name: "Botster Hub",
    trust: HostProfileTrust::FirstPartyTrusted,
    core_role: CORE_RUNTIME_ROLE,
    policy_areas: POLICY_AREAS,
    capability_surfaces: CAPABILITY_SURFACES,
    responsibilities: RESPONSIBILITIES,
};

/// Return the first-party host profile manifest used by the binary smoke path.
#[must_use]
pub const fn host_profile() -> &'static HostProfileManifest {
    &FIRST_PARTY_HOST_PROFILE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_profile_names_first_party_boundary_and_core_facade() {
        let profile = host_profile();

        assert_eq!(profile.id, "botster-hub");
        assert_eq!(profile.trust, HostProfileTrust::FirstPartyTrusted);
        assert_eq!(profile.core_role.facade, "botster_core_daemon::CoreDaemon");
        assert_eq!(profile.core_role.runtime_feature, "local-runtime");
        assert!(profile.core_role.owns.contains("policy-free"));
    }

    #[test]
    fn host_profile_declares_policy_areas_and_core_capability_surfaces() {
        let profile = host_profile();

        assert_eq!(
            profile.policy_areas,
            &[
                PolicyArea::Auth,
                PolicyArea::Config,
                PolicyArea::Persistence,
                PolicyArea::ProvidersAndPackages,
                PolicyArea::TransportsAndAdapters,
                PolicyArea::AdmissionAndCapabilities,
                PolicyArea::Lifecycle,
                PolicyArea::Audit,
            ]
        );
        assert!(
            profile
                .capability_surfaces()
                .contains(&CapabilitySurface::SignalingRelay)
        );
        assert!(
            profile
                .capability_surfaces()
                .contains(&CapabilitySurface::BrowserShell)
        );
        assert!(
            profile
                .capability_surfaces()
                .contains(&CapabilitySurface::ClientAdmission)
        );
        assert!(profile.default_capability_grants().contains(&Capability {
            surface: CapabilitySurface::ClientAdmission,
            scope: None,
        }));
        assert!(profile.default_capability_grants().contains(&Capability {
            surface: CapabilitySurface::Timers,
            scope: Some("callbacks".to_string()),
        }));
        assert!(profile.default_capability_grants().contains(&Capability {
            surface: CapabilitySurface::PluginDb,
            scope: Some("botster-workspaces".to_string()),
        }));
        assert!(
            profile
                .default_capability_grants()
                .iter()
                .all(|capability| profile.capability_surfaces().contains(&capability.surface))
        );
    }

    #[test]
    fn host_profile_keeps_readme_aligned_role_labels() {
        assert_eq!(
            host_profile().role_labels(),
            vec![
                "botster-core",
                "botster-hub",
                "CLI",
                "clients",
                "plugins/providers",
                "external providers",
            ]
        );
    }
}
