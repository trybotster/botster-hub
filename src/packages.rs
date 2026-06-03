//! Hub-owned package registry policy over `botster-core` manifests.
//!
//! This module stores in-memory package policy records and validates enable,
//! disable, pin, and provider admission decisions against the current core
//! manifest, capability, and host-profile admission contracts. It intentionally
//! does not fetch packages, persist records, or load plugin/provider lifecycles.

use std::collections::BTreeMap;

use botster_core::{
    AdmittedHostProfile, Capability, CapabilitySet, CapabilitySurface, ExtensionKind,
    HostProfileAdmissionError, PackageManifest, admit_host_profile,
};

use crate::host_profile;

/// Hub-owned package admission policy backed by the first-party host profile.
#[derive(Debug, Clone)]
pub struct PackageAdmissionPolicy {
    registry: PackageRegistry,
}

impl PackageAdmissionPolicy {
    /// Build the package policy from the first-party host profile grant set.
    #[must_use]
    pub fn from_host_profile() -> Self {
        Self {
            registry: PackageRegistry::new(
                host_profile()
                    .default_capability_grants()
                    .into_iter()
                    .collect(),
            ),
        }
    }

    /// Return the registry that enforces package admission.
    #[must_use]
    pub const fn registry(&self) -> &PackageRegistry {
        &self.registry
    }

    /// Return the mutable registry for package install/enable/disable/pin actions.
    pub const fn registry_mut(&mut self) -> &mut PackageRegistry {
        &mut self.registry
    }

    /// Install a package manifest as disabled until hub policy enables it.
    pub fn install(
        &mut self,
        manifest: PackageManifest,
        provenance: PackageProvenance,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<&PackageRecord> {
        self.registry.install(manifest, provenance, audit_reason)
    }

    /// Enable an installed package when current host-profile grants admit it.
    pub fn enable(
        &mut self,
        package_name: &str,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<PackageDecision> {
        self.registry.enable(package_name, audit_reason)
    }
}

/// Build the production package policy from the first-party host profile.
#[must_use]
pub fn default_package_policy() -> PackageAdmissionPolicy {
    PackageAdmissionPolicy::from_host_profile()
}

/// In-memory hub package registry.
#[derive(Debug, Clone)]
pub struct PackageRegistry {
    records: BTreeMap<String, PackageRecord>,
    granted_capabilities: CapabilitySet,
    governed_surfaces: Vec<CapabilitySurface>,
}

impl PackageRegistry {
    /// Build a registry with a hub-owned grant set.
    #[must_use]
    pub fn new(granted_capabilities: CapabilitySet) -> Self {
        Self {
            records: BTreeMap::new(),
            granted_capabilities,
            governed_surfaces: host_profile().capability_surfaces().to_vec(),
        }
    }

    /// Install a package manifest as disabled until hub policy enables it.
    pub fn install(
        &mut self,
        manifest: PackageManifest,
        provenance: PackageProvenance,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<&PackageRecord> {
        let audit_reason = audit_reason.into();
        let package_name = manifest.name.clone();

        if self.records.contains_key(&package_name) {
            return Err(PackageRegistryError::without_record(
                package_name,
                PackageAction::Install,
                PackageAdmissionReason::AlreadyInstalled,
                audit_reason,
            ));
        }

        if manifest.source.is_none() {
            return Err(PackageRegistryError::without_record(
                package_name,
                PackageAction::Install,
                PackageAdmissionReason::MissingSource,
                audit_reason,
            ));
        }

        if provenance.source.is_empty() {
            return Err(PackageRegistryError::without_record(
                package_name,
                PackageAction::Install,
                PackageAdmissionReason::MissingProvenance,
                audit_reason,
            ));
        }

        let classification = PackageClassification::from_kind(&manifest.kind);
        let record = PackageRecord {
            manifest,
            state: PackageState::Installed,
            classification,
            provenance,
            pin: None,
            update_policy: PackageUpdatePolicy::Manual,
            last_audit_reason: audit_reason,
            admitted_host_profile: None,
        };

        self.records.insert(package_name.clone(), record);
        Ok(self
            .records
            .get(&package_name)
            .expect("inserted package record should be readable"))
    }

    /// Enable an installed package when every requested capability is granted.
    pub fn enable(
        &mut self,
        package_name: &str,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<PackageDecision> {
        let audit_reason = audit_reason.into();
        let record = self.record(package_name, PackageAction::Enable, audit_reason.clone())?;
        let classification = record.classification;
        let state = record.state;

        let ungoverned_surface = record
            .manifest
            .capabilities
            .iter()
            .find(|capability| !self.governed_surfaces.contains(&capability.surface))
            .cloned();
        if let Some(capability) = ungoverned_surface {
            return Err(PackageRegistryError::with_record(
                package_name,
                PackageAction::Enable,
                PackageAdmissionReason::UngovernedCapabilitySurface(capability.surface),
                state,
                classification,
                audit_reason,
            ));
        }

        let ungranted_capability = record
            .manifest
            .capabilities
            .iter()
            .find(|capability| !self.granted_capabilities.contains(capability))
            .cloned();
        if let Some(capability) = ungranted_capability {
            return Err(PackageRegistryError::with_record(
                package_name,
                PackageAction::Enable,
                PackageAdmissionReason::UngrantedCapability(capability),
                state,
                classification,
                audit_reason,
            ));
        }

        let admitted_host_profile = match record.classification {
            PackageClassification::Plugin => {
                if record.manifest.host_profile.is_some() {
                    Some(admit_host_profile(&record.manifest, true).map_err(|error| {
                        PackageRegistryError::with_record(
                            package_name,
                            PackageAction::Enable,
                            PackageAdmissionReason::HostProfileAdmission(error),
                            state,
                            classification,
                            audit_reason.clone(),
                        )
                    })?)
                } else {
                    None
                }
            }
            PackageClassification::Provider if record.manifest.host_profile.is_none() => {
                return Err(PackageRegistryError::with_record(
                    package_name,
                    PackageAction::Enable,
                    PackageAdmissionReason::ProviderMissingHostProfile,
                    state,
                    classification,
                    audit_reason,
                ));
            }
            PackageClassification::Provider => {
                Some(admit_host_profile(&record.manifest, true).map_err(|error| {
                    PackageRegistryError::with_record(
                        package_name,
                        PackageAction::Enable,
                        PackageAdmissionReason::HostProfileAdmission(error),
                        state,
                        classification,
                        audit_reason.clone(),
                    )
                })?)
            }
        };

        let record = self
            .records
            .get_mut(package_name)
            .expect("record existence checked before enable");
        record.state = PackageState::Enabled;
        record.last_audit_reason = audit_reason.clone();
        record.admitted_host_profile = admitted_host_profile.clone();

        Ok(PackageDecision {
            package_name: package_name.to_string(),
            action: PackageAction::Enable,
            state: record.state,
            classification: record.classification,
            admitted_host_profile,
            audit_reason,
        })
    }

    /// Disable a package without removing its manifest, pin, or provenance.
    pub fn disable(
        &mut self,
        package_name: &str,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<PackageDecision> {
        let audit_reason = audit_reason.into();
        let record = self.record_mut(package_name, PackageAction::Disable, audit_reason.clone())?;
        record.state = PackageState::Disabled;
        record.last_audit_reason = audit_reason.clone();
        record.admitted_host_profile = None;

        Ok(PackageDecision {
            package_name: package_name.to_string(),
            action: PackageAction::Disable,
            state: record.state,
            classification: record.classification,
            admitted_host_profile: None,
            audit_reason,
        })
    }

    /// Record hub-owned pin and update metadata without fetching anything.
    pub fn pin(
        &mut self,
        package_name: &str,
        pin: PackagePin,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<&PackageRecord> {
        let audit_reason = audit_reason.into();
        if pin.revision.is_empty() {
            return Err(PackageRegistryError::without_record(
                package_name,
                PackageAction::Pin,
                PackageAdmissionReason::MissingPinRevision,
                audit_reason,
            ));
        }

        let record = self.record_mut(package_name, PackageAction::Pin, audit_reason.clone())?;
        record.update_policy = pin.update_policy;
        record.pin = Some(pin);
        record.last_audit_reason = audit_reason;

        Ok(record)
    }

    /// Return a package record by package name.
    #[must_use]
    pub fn package(&self, package_name: &str) -> Option<&PackageRecord> {
        self.records.get(package_name)
    }

    /// Return package records in deterministic name order.
    #[must_use]
    pub fn packages(&self) -> Vec<&PackageRecord> {
        self.records.values().collect()
    }

    /// Return the hub-owned grants used for package admission.
    #[must_use]
    pub const fn granted_capabilities(&self) -> &CapabilitySet {
        &self.granted_capabilities
    }

    fn record(
        &self,
        package_name: &str,
        action: PackageAction,
        audit_reason: String,
    ) -> PackageRegistryResult<&PackageRecord> {
        self.records.get(package_name).ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                action,
                PackageAdmissionReason::PackageNotInstalled,
                audit_reason,
            )
        })
    }

    fn record_mut(
        &mut self,
        package_name: &str,
        action: PackageAction,
        audit_reason: String,
    ) -> PackageRegistryResult<&mut PackageRecord> {
        self.records.get_mut(package_name).ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                action,
                PackageAdmissionReason::PackageNotInstalled,
                audit_reason,
            )
        })
    }
}

/// Stored package policy record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRecord {
    /// Core-owned package manifest.
    pub manifest: PackageManifest,
    /// Hub-owned enabled/disabled state.
    pub state: PackageState,
    /// Hub classification derived from the core extension kind.
    pub classification: PackageClassification,
    /// Hub-owned provenance placeholder.
    pub provenance: PackageProvenance,
    /// Optional hub-owned pin metadata.
    pub pin: Option<PackagePin>,
    /// Hub-owned update policy placeholder.
    pub update_policy: PackageUpdatePolicy,
    /// Operator/audit reason for the latest registry mutation.
    pub last_audit_reason: String,
    /// Core-admitted host-profile metadata for enabled provider packages.
    pub admitted_host_profile: Option<AdmittedHostProfile>,
}

impl PackageRecord {
    /// Whether hub policy currently treats the package as active.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        matches!(self.state, PackageState::Enabled)
    }
}

/// Hub-owned package state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageState {
    /// Installed but not active.
    Installed,
    /// Active under current hub grants.
    Enabled,
    /// Explicitly disabled after install or enable.
    Disabled,
}

/// Hub package classification derived from `botster-core::ExtensionKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageClassification {
    /// Ordinary plugin package.
    Plugin,
    /// Privileged provider package.
    Provider,
}

impl PackageClassification {
    fn from_kind(kind: &ExtensionKind) -> Self {
        match kind {
            ExtensionKind::Plugin => Self::Plugin,
            ExtensionKind::Provider => Self::Provider,
        }
    }
}

/// Hub-owned provenance placeholder around a core manifest source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageProvenance {
    /// Non-local source identifier suitable for audit displays.
    pub source: String,
    /// Optional checksum recorded by future installer/lockfile paths.
    pub checksum: Option<String>,
}

/// Hub-owned pin metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackagePin {
    /// Revision, tag, or version pinned by policy.
    pub revision: String,
    /// Optional checksum paired with the pin.
    pub checksum: Option<String>,
    /// Update behavior while the package is pinned.
    pub update_policy: PackageUpdatePolicy,
}

/// Hub-owned update policy placeholder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageUpdatePolicy {
    /// Updates require an explicit operator/package-manager action.
    Manual,
    /// Future package manager may update within the pinned source policy.
    TrackSource,
}

/// Result of a successful registry decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageDecision {
    /// Package name the decision applies to.
    pub package_name: String,
    /// Action that was accepted.
    pub action: PackageAction,
    /// Resulting package state.
    pub state: PackageState,
    /// Package classification derived from the core manifest.
    pub classification: PackageClassification,
    /// Admitted host-profile metadata when enabling a provider host profile.
    pub admitted_host_profile: Option<AdmittedHostProfile>,
    /// Operator/audit reason for this accepted decision.
    pub audit_reason: String,
}

/// Registry action used in audit-friendly decisions and errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageAction {
    Install,
    Enable,
    Disable,
    Pin,
}

/// Typed hub package policy error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRegistryError {
    /// Package name the policy decision applies to.
    pub package_name: String,
    /// Action that was denied.
    pub action: PackageAction,
    /// Typed denial reason.
    pub reason: PackageAdmissionReason,
    /// Package state before the denied action, when a record exists.
    pub state: Option<PackageState>,
    /// Package classification, when a record exists.
    pub classification: Option<PackageClassification>,
    /// Operator/audit reason attached to the denied action.
    pub audit_reason: String,
}

impl PackageRegistryError {
    fn without_record(
        package_name: impl Into<String>,
        action: PackageAction,
        reason: PackageAdmissionReason,
        audit_reason: String,
    ) -> Self {
        Self {
            package_name: package_name.into(),
            action,
            reason,
            state: None,
            classification: None,
            audit_reason,
        }
    }

    fn with_record(
        package_name: impl Into<String>,
        action: PackageAction,
        reason: PackageAdmissionReason,
        state: PackageState,
        classification: PackageClassification,
        audit_reason: String,
    ) -> Self {
        Self {
            package_name: package_name.into(),
            action,
            reason,
            state: Some(state),
            classification: Some(classification),
            audit_reason,
        }
    }
}

/// Registry result alias.
pub type PackageRegistryResult<T> = Result<T, PackageRegistryError>;

/// Typed denial reasons for package policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackageAdmissionReason {
    /// Package has already been installed in this in-memory registry.
    AlreadyInstalled,
    /// Package is not installed.
    PackageNotInstalled,
    /// Core manifest did not include package source metadata.
    MissingSource,
    /// Hub policy did not receive provenance metadata.
    MissingProvenance,
    /// Pin metadata did not identify a revision/tag/version.
    MissingPinRevision,
    /// Capability surface is not governed by the current host profile.
    UngovernedCapabilitySurface(CapabilitySurface),
    /// Manifest requested a capability not present in the hub-owned grant set.
    UngrantedCapability(Capability),
    /// Provider-classified packages must carry host-profile metadata before enablement.
    ProviderMissingHostProfile,
    /// Core host-profile admission rejected the provider package.
    HostProfileAdmission(HostProfileAdmissionError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core::{
        ExtensionEntrypoint, ExtensionRuntime, HostProfileMetadata, HostProfilePolicySection,
        PackageSource,
    };

    fn capability(surface: CapabilitySurface, scope: Option<&str>) -> Capability {
        Capability {
            surface,
            scope: scope.map(ToString::to_string),
        }
    }

    fn grants(capabilities: Vec<Capability>) -> CapabilitySet {
        capabilities.into_iter().collect()
    }

    fn provenance() -> PackageProvenance {
        PackageProvenance {
            source: "https://example.invalid/botster/packages/example".to_string(),
            checksum: Some("sha256:example".to_string()),
        }
    }

    fn plugin_manifest(name: &str, capabilities: Vec<Capability>) -> PackageManifest {
        PackageManifest {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Plugin,
            botster: ">=0.1.0".to_string(),
            source: Some(PackageSource::Git {
                repo: "https://example.invalid/botster/package.git".to_string(),
                reference: "v1.0.0".to_string(),
            }),
            capabilities,
            entrypoints: vec![ExtensionEntrypoint {
                runtime: ExtensionRuntime::Lua,
                path: "plugin.lua".to_string(),
                bootstrap: false,
            }],
            host_profile: None,
        }
    }

    fn provider_manifest(name: &str, capabilities: Vec<Capability>) -> PackageManifest {
        PackageManifest {
            name: name.to_string(),
            version: "2.0.0".to_string(),
            kind: ExtensionKind::Provider,
            botster: ">=0.1.0".to_string(),
            source: Some(PackageSource::Git {
                repo: "https://example.invalid/botster/provider.git".to_string(),
                reference: "v2.0.0".to_string(),
            }),
            capabilities: capabilities.clone(),
            entrypoints: vec![ExtensionEntrypoint {
                runtime: ExtensionRuntime::Process,
                path: "bin/provider".to_string(),
                bootstrap: true,
            }],
            host_profile: Some(HostProfileMetadata {
                profile_id: "example-provider".to_string(),
                compatibility: ">=0.1.0".to_string(),
                precedence: 10,
                required_providers: Vec::new(),
                required_capabilities: capabilities,
                policy_sections: vec![HostProfilePolicySection::Providers],
            }),
        }
    }

    #[test]
    fn install_stores_plugin_manifest_disabled() {
        let manifest = plugin_manifest(
            "workflow.plugin",
            vec![capability(CapabilitySurface::Surfaces, None)],
        );
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));

        let record = registry
            .install(manifest, provenance(), "operator installed package")
            .expect("install package");

        assert_eq!(record.state, PackageState::Installed);
        assert_eq!(record.classification, PackageClassification::Plugin);
        assert!(!record.is_enabled());
        assert_eq!(registry.packages().len(), 1);
    }

    #[test]
    fn enable_succeeds_only_when_requested_capabilities_are_granted() {
        let capability = capability(CapabilitySurface::Mcp, Some("tools"));
        let mut registry = PackageRegistry::new(grants(vec![capability.clone()]));
        registry
            .install(
                plugin_manifest("mcp.plugin", vec![capability]),
                provenance(),
                "install",
            )
            .expect("install package");

        let decision = registry
            .enable("mcp.plugin", "operator enabled package")
            .expect("enable granted package");

        assert_eq!(decision.state, PackageState::Enabled);
        assert_eq!(decision.audit_reason, "operator enabled package");
        assert_eq!(
            registry.package("mcp.plugin").expect("record").state,
            PackageState::Enabled
        );
    }

    #[test]
    fn enable_denies_ungranted_capability_scope() {
        let requested = capability(CapabilitySurface::Network, Some("websocket"));
        let mut registry = PackageRegistry::new(grants(vec![capability(
            CapabilitySurface::Network,
            Some("http"),
        )]));
        registry
            .install(
                plugin_manifest("network.plugin", vec![requested.clone()]),
                provenance(),
                "install",
            )
            .expect("install package");

        let error = registry
            .enable("network.plugin", "operator enabled package")
            .expect_err("ungranted scope should deny");

        assert_eq!(error.package_name, "network.plugin");
        assert_eq!(error.action, PackageAction::Enable);
        assert_eq!(
            error.reason,
            PackageAdmissionReason::UngrantedCapability(requested)
        );
        assert_eq!(error.state, Some(PackageState::Installed));
        assert_eq!(error.classification, Some(PackageClassification::Plugin));
        assert_eq!(error.audit_reason, "operator enabled package");
    }

    #[test]
    fn enable_admits_timer_capability_after_profile_governs_timers() {
        let requested = capability(CapabilitySurface::Timers, Some("callbacks"));
        let mut registry = PackageRegistry::new(grants(vec![requested.clone()]));
        registry
            .install(
                plugin_manifest("timer.plugin", vec![requested]),
                provenance(),
                "install",
            )
            .expect("install package");

        let decision = registry
            .enable("timer.plugin", "operator enabled package")
            .expect("governed timer surface should enable");

        assert_eq!(decision.state, PackageState::Enabled);
    }

    #[test]
    fn disabling_preserves_record_and_marks_inactive() {
        let capability = capability(CapabilitySurface::Surfaces, None);
        let mut registry = PackageRegistry::new(grants(vec![capability.clone()]));
        registry
            .install(
                plugin_manifest("surface.plugin", vec![capability]),
                provenance(),
                "install",
            )
            .expect("install package");
        registry
            .enable("surface.plugin", "enable")
            .expect("enable package");

        let decision = registry
            .disable("surface.plugin", "operator disabled package")
            .expect("disable package");

        let record = registry.package("surface.plugin").expect("record");
        assert_eq!(decision.state, PackageState::Disabled);
        assert_eq!(record.state, PackageState::Disabled);
        assert!(record.pin.is_none());
    }

    #[test]
    fn pin_records_metadata_without_fetching() {
        let mut registry = PackageRegistry::new(CapabilitySet::new());
        registry
            .install(
                plugin_manifest("pin.plugin", Vec::new()),
                provenance(),
                "install",
            )
            .expect("install package");

        let record = registry
            .pin(
                "pin.plugin",
                PackagePin {
                    revision: "v1.0.0".to_string(),
                    checksum: Some("sha256:pinned".to_string()),
                    update_policy: PackageUpdatePolicy::TrackSource,
                },
                "operator pinned package",
            )
            .expect("pin package");

        assert_eq!(record.update_policy, PackageUpdatePolicy::TrackSource);
        assert_eq!(
            record.pin.as_ref().expect("pin").checksum.as_deref(),
            Some("sha256:pinned")
        );
    }

    #[test]
    fn provider_manifests_are_classified_from_core_extension_kind() {
        let capability = capability(CapabilitySurface::ClientAdmission, None);
        let mut registry = PackageRegistry::new(grants(vec![capability.clone()]));

        let record = registry
            .install(
                provider_manifest("admission.provider", vec![capability]),
                provenance(),
                "install provider",
            )
            .expect("install provider");

        assert_eq!(record.classification, PackageClassification::Provider);
    }

    #[test]
    fn provider_without_host_profile_metadata_is_denied_before_enable() {
        let capability = capability(CapabilitySurface::ClientAdmission, None);
        let mut manifest = provider_manifest("metadata-missing.provider", vec![capability.clone()]);
        manifest.host_profile = None;
        let mut registry = PackageRegistry::new(grants(vec![capability]));
        registry
            .install(manifest, provenance(), "install provider")
            .expect("install provider");

        let error = registry
            .enable("metadata-missing.provider", "enable provider")
            .expect_err("provider without host profile metadata should deny");

        assert_eq!(
            error.reason,
            PackageAdmissionReason::ProviderMissingHostProfile
        );
    }

    #[test]
    fn provider_host_profiles_that_pass_core_admission_are_admitted() {
        let capability = capability(CapabilitySurface::ClientAdmission, None);
        let mut registry = PackageRegistry::new(grants(vec![capability.clone()]));
        registry
            .install(
                provider_manifest("admission.provider", vec![capability]),
                provenance(),
                "install provider",
            )
            .expect("install provider");

        let decision = registry
            .enable("admission.provider", "enable provider")
            .expect("admit provider");

        assert_eq!(decision.classification, PackageClassification::Provider);
        assert_eq!(decision.audit_reason, "enable provider");
        assert_eq!(
            decision
                .admitted_host_profile
                .expect("admitted profile")
                .metadata
                .profile_id,
            "example-provider"
        );
    }

    #[test]
    fn ordinary_plugin_with_host_profile_metadata_is_denied_by_core_admission() {
        let capability = capability(CapabilitySurface::ClientAdmission, None);
        let mut manifest = plugin_manifest("bad.plugin", vec![capability.clone()]);
        manifest.host_profile = Some(HostProfileMetadata {
            profile_id: "bad-plugin".to_string(),
            compatibility: ">=0.1.0".to_string(),
            precedence: 1,
            required_providers: Vec::new(),
            required_capabilities: vec![capability.clone()],
            policy_sections: vec![HostProfilePolicySection::ClientAdmission],
        });

        let mut registry = PackageRegistry::new(grants(vec![capability]));
        registry
            .install(manifest, provenance(), "install plugin")
            .expect("install plugin");

        let error = registry
            .enable("bad.plugin", "enable plugin")
            .expect_err("ordinary plugin host profile should deny");

        assert_eq!(
            error.reason,
            PackageAdmissionReason::HostProfileAdmission(HostProfileAdmissionError::NotProvider)
        );
    }

    #[test]
    fn provider_host_profile_core_deny_reasons_are_wrapped() {
        let capability = capability(CapabilitySurface::ClientAdmission, None);
        let mut manifest = provider_manifest("bad.provider", vec![capability.clone()]);
        manifest.entrypoints.clear();

        let mut registry = PackageRegistry::new(grants(vec![capability]));
        registry
            .install(manifest, provenance(), "install provider")
            .expect("install provider");

        let error = registry
            .enable("bad.provider", "enable provider")
            .expect_err("missing bootstrap should deny");

        assert_eq!(
            error.reason,
            PackageAdmissionReason::HostProfileAdmission(
                HostProfileAdmissionError::MissingBootstrapEntrypoint
            )
        );
    }

    #[test]
    fn missing_source_and_provenance_are_denied_at_install() {
        let mut registry = PackageRegistry::new(CapabilitySet::new());
        let mut manifest = plugin_manifest("source.plugin", Vec::new());
        manifest.source = None;

        let error = registry
            .install(manifest, provenance(), "install")
            .expect_err("missing source should deny");
        assert_eq!(error.reason, PackageAdmissionReason::MissingSource);

        let error = registry
            .install(
                plugin_manifest("provenance.plugin", Vec::new()),
                PackageProvenance {
                    source: String::new(),
                    checksum: None,
                },
                "install",
            )
            .expect_err("missing provenance should deny");
        assert_eq!(error.reason, PackageAdmissionReason::MissingProvenance);
    }

    #[test]
    fn default_package_policy_derives_grants_from_host_profile() {
        let capability = capability(CapabilitySurface::Surfaces, None);
        let mut policy = default_package_policy();

        assert_eq!(
            policy.registry().granted_capabilities().len(),
            host_profile().default_capability_grants().len()
        );

        policy
            .install(
                plugin_manifest("surface.plugin", vec![capability]),
                provenance(),
                "install through profile policy",
            )
            .expect("install through default package policy");

        let decision = policy
            .enable("surface.plugin", "enable through profile policy")
            .expect("enable through default package policy");

        assert_eq!(decision.state, PackageState::Enabled);
        assert_eq!(decision.audit_reason, "enable through profile policy");
    }

    #[test]
    fn package_records_are_returned_in_stable_name_order() {
        let mut policy = default_package_policy();

        policy
            .install(
                plugin_manifest("zeta.plugin", Vec::new()),
                provenance(),
                "install zeta",
            )
            .expect("install zeta");
        policy
            .install(
                plugin_manifest("alpha.plugin", Vec::new()),
                provenance(),
                "install alpha",
            )
            .expect("install alpha");

        let names: Vec<_> = policy
            .registry()
            .packages()
            .iter()
            .map(|record| record.manifest.name.as_str())
            .collect();

        assert_eq!(names, vec!["alpha.plugin", "zeta.plugin"]);
    }
}
