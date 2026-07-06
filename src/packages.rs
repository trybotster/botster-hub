//! Hub-owned package registry policy over `botster-core` manifests.
//!
//! This module stores package policy records and validates enable, disable,
//! pin, local source, and provider admission decisions against the current core
//! manifest, capability, and host-profile admission contracts. It intentionally
//! does not fetch packages or load plugin/provider lifecycles.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

use botster_core::{
    AdmittedHostProfile, Capability, CapabilitySet, CapabilitySurface, ExtensionEntrypoint,
    ExtensionKind, ExtensionRuntime, HostProfileAdmissionError, PackageConfigurationField,
    PackageConfigurationFieldType, PackageConfigurationSchema, PackageConfigurationSecretValue,
    PackageConfigurationValue, PackageManifest, PackageRequirementStatus, PackageResolutionInput,
    PackageResolutionMatrix, PackageResolutionPackage, PackageSource, RunnableEntrypointKind,
    RunnableEntrypointLaunchMode, RunnableEntrypointReadiness, admit_host_profile,
    resolve_package_dependencies,
};
use serde::{Deserialize, Serialize};

use crate::host_profile;
use crate::session_templates::{PackageSessionTemplate, validate_session_templates};

/// Conventional manifest filename used when installing a local package directory.
pub const LOCAL_PACKAGE_MANIFEST_FILE: &str = "botster-package.json";
/// Conventional local marketplace registry filename.
pub const LOCAL_PACKAGE_REGISTRY_FILE: &str = "botster-registry.json";

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

    /// Install a local package from an explicit manifest path or package directory.
    pub fn install_local_path(
        &mut self,
        path: impl AsRef<Path>,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<&PackageRecord> {
        self.registry.install_local_path(path, audit_reason)
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

struct PackageInstallOptions {
    manifest: PackageManifest,
    provenance: PackageProvenance,
    trust: PackageTrust,
    runnable_entrypoints: Vec<PackageRunnableEntrypoint>,
    session_templates: Vec<PackageSessionTemplate>,
    source_metadata: Option<PackageSourceMetadata>,
    pin: Option<PackagePin>,
    audit_reason: String,
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
        self.install_with_trust(
            manifest,
            provenance,
            PackageTrust::third_party(),
            audit_reason,
        )
    }

    /// Install a package manifest with an explicit hub-owned trust marker.
    pub fn install_with_trust(
        &mut self,
        manifest: PackageManifest,
        provenance: PackageProvenance,
        trust: PackageTrust,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<&PackageRecord> {
        self.install_with_trust_and_templates(
            manifest,
            provenance,
            trust,
            Vec::new(),
            Vec::new(),
            audit_reason,
        )
    }

    fn install_with_trust_and_templates(
        &mut self,
        manifest: PackageManifest,
        provenance: PackageProvenance,
        trust: PackageTrust,
        runnable_entrypoints: Vec<PackageRunnableEntrypoint>,
        session_templates: Vec<PackageSessionTemplate>,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<&PackageRecord> {
        self.install_with_options(PackageInstallOptions {
            manifest,
            provenance,
            trust,
            runnable_entrypoints,
            session_templates,
            source_metadata: None,
            pin: None,
            audit_reason: audit_reason.into(),
        })
    }

    fn install_with_options(
        &mut self,
        options: PackageInstallOptions,
    ) -> PackageRegistryResult<&PackageRecord> {
        let PackageInstallOptions {
            manifest,
            provenance,
            trust,
            runnable_entrypoints,
            session_templates,
            source_metadata,
            pin,
            audit_reason,
        } = options;
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

        let compatibility = PackageCompatibility::for_manifest(&manifest);
        if !compatibility.is_compatible() {
            return Err(PackageRegistryError::without_record(
                package_name,
                PackageAction::Install,
                PackageAdmissionReason::BotsterCompatibility(compatibility.diagnostics.clone()),
                audit_reason,
            ));
        }

        let classification = PackageClassification::from_kind(&manifest.kind);
        let record = PackageRecord {
            manifest,
            state: PackageState::Installed,
            classification,
            trust,
            provenance,
            source_metadata,
            pin,
            update_policy: PackageUpdatePolicy::Manual,
            admitted_capabilities: Vec::new(),
            compatibility,
            runnable_entrypoints,
            session_templates,
            configuration: PackageConfigurationState::default(),
            installed_at: None,
            updated_at: None,
            last_audit_reason: audit_reason,
            admitted_host_profile: None,
        };

        self.records.insert(package_name.clone(), record);
        Ok(self
            .records
            .get(&package_name)
            .expect("inserted package record should be readable"))
    }

    /// Install a local package from an explicit manifest path or package directory.
    pub fn install_local_path(
        &mut self,
        path: impl AsRef<Path>,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<&PackageRecord> {
        let audit_reason = audit_reason.into();
        let local_source = LocalPackageSource::resolve(path.as_ref(), audit_reason.clone())?;
        let local_manifest = local_source.read_manifest(audit_reason.clone())?;
        let mut manifest = local_manifest.manifest;
        local_source.validate_manifest_entrypoints(&manifest, audit_reason.clone())?;
        validate_runnable_entrypoints(
            &manifest.name,
            &local_manifest.runnable_entrypoints,
            PackageAction::Install,
            audit_reason.clone(),
        )?;
        validate_session_templates(&local_manifest.session_templates).map_err(|reason| {
            PackageRegistryError::without_record(
                manifest.name.clone(),
                PackageAction::Install,
                PackageAdmissionReason::UnsafeSessionTemplate(reason),
                audit_reason.clone(),
            )
        })?;
        manifest.source = Some(PackageSource::Path {
            path: local_source.package_root.to_string_lossy().into_owned(),
        });
        let provenance = PackageProvenance {
            source: format!("local:{}", local_source.package_root.to_string_lossy()),
            checksum: None,
        };

        self.install_with_trust_and_templates(
            manifest,
            provenance,
            PackageTrust::local_development(),
            local_manifest.runnable_entrypoints,
            local_manifest.session_templates,
            audit_reason,
        )
    }

    /// Re-read an installed local package from its persisted path-backed source.
    pub fn reload_local_package(
        &mut self,
        package_name: &str,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<PackageDecision> {
        let audit_reason = audit_reason.into();
        let current = self
            .record(package_name, PackageAction::Reload, audit_reason.clone())?
            .clone();
        let was_enabled = current.is_enabled();
        let package_root = package_root(&current).map_err(|message| {
            PackageRegistryError::with_record(
                package_name,
                PackageAction::Reload,
                PackageAdmissionReason::UnsafeLocalPath(message),
                current.state,
                current.classification,
                audit_reason.clone(),
            )
        })?;
        let local_source = LocalPackageSource::resolve(&package_root, audit_reason.clone())?;
        let local_manifest = local_source.read_manifest(audit_reason.clone())?;
        let mut manifest = local_manifest.manifest;
        if manifest.name != package_name {
            return Err(PackageRegistryError::with_record(
                package_name,
                PackageAction::Reload,
                PackageAdmissionReason::InvalidLocalManifest(format!(
                    "reloaded package name {} does not match installed package {package_name}",
                    manifest.name
                )),
                current.state,
                current.classification,
                audit_reason,
            ));
        }
        local_source.validate_manifest_entrypoints(&manifest, audit_reason.clone())?;
        validate_runnable_entrypoints(
            &manifest.name,
            &local_manifest.runnable_entrypoints,
            PackageAction::Reload,
            audit_reason.clone(),
        )?;
        validate_session_templates(&local_manifest.session_templates).map_err(|reason| {
            PackageRegistryError::with_record(
                package_name,
                PackageAction::Reload,
                PackageAdmissionReason::UnsafeSessionTemplate(reason),
                current.state,
                current.classification,
                audit_reason.clone(),
            )
        })?;
        manifest.source = Some(PackageSource::Path {
            path: local_source.package_root.to_string_lossy().into_owned(),
        });

        let compatibility = PackageCompatibility::for_manifest(&manifest);
        if !compatibility.is_compatible() {
            return Err(PackageRegistryError::with_record(
                package_name,
                PackageAction::Reload,
                PackageAdmissionReason::BotsterCompatibility(compatibility.diagnostics.clone()),
                current.state,
                current.classification,
                audit_reason,
            ));
        }

        let classification = PackageClassification::from_kind(&manifest.kind);
        let mut candidate = self.clone();
        candidate.records.insert(
            package_name.to_string(),
            PackageRecord {
                manifest,
                state: current.state,
                classification,
                trust: current.trust,
                provenance: current.provenance,
                source_metadata: current.source_metadata,
                pin: current.pin,
                update_policy: current.update_policy,
                admitted_capabilities: Vec::new(),
                compatibility,
                runnable_entrypoints: local_manifest.runnable_entrypoints,
                session_templates: local_manifest.session_templates,
                configuration: current.configuration,
                installed_at: current.installed_at,
                updated_at: current.updated_at,
                last_audit_reason: audit_reason.clone(),
                admitted_host_profile: None,
            },
        );
        if was_enabled {
            candidate.enable(package_name, audit_reason.clone())?;
        }

        let refreshed = candidate
            .records
            .get(package_name)
            .expect("candidate reload record should exist");
        let decision = PackageDecision {
            package_name: package_name.to_string(),
            action: PackageAction::Reload,
            state: refreshed.state,
            classification: refreshed.classification,
            admitted_host_profile: refreshed.admitted_host_profile.clone(),
            audit_reason,
        };
        self.records = candidate.records;
        Ok(decision)
    }

    /// List packages from a hub-owned local/static marketplace registry.
    pub fn available_packages(
        &self,
        registry_path: impl AsRef<Path>,
    ) -> PackageRegistryResult<Vec<AvailablePackage>> {
        let catalog = LocalRegistryCatalog::load(registry_path.as_ref())?;
        catalog
            .entries
            .iter()
            .map(|entry| self.available_package(&catalog, entry))
            .collect()
    }

    /// Inspect one package entry from a hub-owned local/static marketplace registry.
    pub fn inspect_available_package(
        &self,
        registry_path: impl AsRef<Path>,
        entry_id: &str,
    ) -> PackageRegistryResult<AvailablePackage> {
        let catalog = LocalRegistryCatalog::load(registry_path.as_ref())?;
        let entry = catalog.entry(entry_id)?;
        self.available_package(&catalog, entry)
    }

    /// Preview an explicit package install from a registry entry without mutating state.
    pub fn preview_registry_install(
        &self,
        registry_path: impl AsRef<Path>,
        entry_id: &str,
    ) -> PackageRegistryResult<PackageInstallPlan> {
        let catalog = LocalRegistryCatalog::load(registry_path.as_ref())?;
        let entry = catalog.entry(entry_id)?;
        let prepared = PreparedRegistryEntry::from_catalog_entry(&catalog, entry)?;
        Ok(self.install_plan(&catalog, entry, &prepared))
    }

    /// Explicitly install a package from a local/static registry entry.
    ///
    /// Git-shaped entries persist source and pin metadata only; this path never
    /// fetches or clones remote content and never enables or starts entrypoints.
    pub fn install_registry_entry(
        &mut self,
        registry_path: impl AsRef<Path>,
        entry_id: &str,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<&PackageRecord> {
        let catalog = LocalRegistryCatalog::load(registry_path.as_ref())?;
        let entry = catalog.entry(entry_id)?;
        let prepared = PreparedRegistryEntry::from_catalog_entry(&catalog, entry)?;
        let source_metadata = PackageSourceMetadata::from_catalog_entry(&catalog, entry);
        let trust = if entry.first_party {
            PackageTrust::first_party()
        } else {
            PackageTrust::third_party()
        };
        self.install_with_options(PackageInstallOptions {
            manifest: prepared.manifest,
            provenance: prepared.provenance,
            trust,
            runnable_entrypoints: prepared.runnable_entrypoints,
            session_templates: prepared.session_templates,
            source_metadata: Some(source_metadata),
            pin: prepared.pin,
            audit_reason: audit_reason.into(),
        })
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
        let configuration = record.configuration_view();
        if !configuration.diagnostics.is_empty() {
            return Err(PackageRegistryError::with_record(
                package_name,
                PackageAction::Enable,
                PackageAdmissionReason::InvalidConfiguration(configuration.diagnostics),
                state,
                classification,
                audit_reason,
            ));
        }
        if !configuration.missing_required.is_empty() {
            return Err(PackageRegistryError::with_record(
                package_name,
                PackageAction::Enable,
                PackageAdmissionReason::MissingRequiredConfiguration(
                    configuration.missing_required,
                ),
                state,
                classification,
                audit_reason,
            ));
        }

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

        let admitted_host_profile =
            admit_enabled_host_profile(&record.manifest, record.classification).map_err(
                |reason| {
                    PackageRegistryError::with_record(
                        package_name,
                        PackageAction::Enable,
                        reason,
                        state,
                        classification,
                        audit_reason.clone(),
                    )
                },
            )?;

        let record = self
            .records
            .get_mut(package_name)
            .expect("record existence checked before enable");
        record.state = PackageState::Enabled;
        record.admitted_capabilities = record.manifest.capabilities.clone();
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
        record.admitted_capabilities.clear();
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

    /// Remove a package record after daemon runtime cleanup has been attempted.
    pub fn remove(
        &mut self,
        package_name: &str,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<PackageDecision> {
        let audit_reason = audit_reason.into();
        let record = self.records.remove(package_name).ok_or_else(|| {
            PackageRegistryError::without_record(
                package_name,
                PackageAction::Remove,
                PackageAdmissionReason::PackageNotInstalled,
                audit_reason.clone(),
            )
        })?;

        Ok(PackageDecision {
            package_name: package_name.to_string(),
            action: PackageAction::Remove,
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

    /// Persist validated configuration values for an installed package.
    pub fn set_configuration(
        &mut self,
        package_name: &str,
        values: BTreeMap<String, PackageConfigurationValue>,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<PackageConfigurationView> {
        let audit_reason = audit_reason.into();
        let record = self.record(package_name, PackageAction::Configure, audit_reason.clone())?;
        let schema = configuration_schema(record)?;
        validate_configuration_values(&record.manifest.name, schema, &values).map_err(
            |reason| {
                PackageRegistryError::with_record(
                    package_name,
                    PackageAction::Configure,
                    reason,
                    record.state,
                    record.classification,
                    audit_reason.clone(),
                )
            },
        )?;

        let record =
            self.record_mut(package_name, PackageAction::Configure, audit_reason.clone())?;
        for (key, value) in values {
            record
                .configuration
                .values
                .insert(key, stored_configuration_value(value));
        }
        record.last_audit_reason = audit_reason;

        Ok(record.configuration_view())
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

    /// Resolve one installed package through the core dependency and feature matrix.
    #[must_use]
    pub fn resolution_matrix_for(&self, record: &PackageRecord) -> PackageResolutionMatrix {
        resolve_package_dependencies(&record.manifest, &self.resolution_input())
    }

    /// Build core's policy-free resolution input from current hub registry state.
    #[must_use]
    pub fn resolution_input(&self) -> PackageResolutionInput {
        let packages = self
            .records
            .values()
            .map(|record| PackageResolutionPackage {
                name: record.manifest.name.clone(),
                enabled: record.is_enabled(),
                providers: provider_ids_for_record(record),
                capabilities: record.admitted_capabilities.clone(),
            })
            .collect();
        let mut auth = BTreeMap::new();
        let mut config = BTreeMap::new();

        for record in self.records.values() {
            let view = record.configuration_view();
            let missing_required = view
                .missing_required
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            if let Some(schema) = &view.schema {
                for field in &schema.fields {
                    let status = if view.effective_values.contains_key(&field.key)
                        && !missing_required.contains(&field.key)
                    {
                        PackageRequirementStatus::Configured
                    } else {
                        PackageRequirementStatus::Missing
                    };
                    config.insert(field.key.clone(), status);
                    if matches!(field.field_type, PackageConfigurationFieldType::Secret) {
                        auth.insert(field.key.clone(), status);
                    }
                }
            }
        }

        PackageResolutionInput {
            packages,
            providers: Vec::new(),
            capabilities: self.granted_capabilities.iter().cloned().collect(),
            auth: auth
                .into_iter()
                .map(|(key, status)| botster_core::PackageAuthState { key, status })
                .collect(),
            config: config
                .into_iter()
                .map(|(key, status)| botster_core::PackageConfigState { key, status })
                .collect(),
        }
    }

    /// Export the trusted in-memory registry for durable hub state.
    #[must_use]
    pub fn snapshot(&self) -> PackageRegistrySnapshot {
        PackageRegistrySnapshot {
            granted_capabilities: self.granted_capabilities.iter().cloned().collect(),
            governed_surfaces: self.governed_surfaces.clone(),
            records: self.records.values().cloned().collect(),
        }
    }

    /// Rebuild trusted persisted registry state and re-derive runtime admission metadata.
    pub fn from_snapshot(
        snapshot: PackageRegistrySnapshot,
    ) -> Result<Self, PackageRegistrySnapshotError> {
        let mut records = BTreeMap::new();
        let granted_capabilities: CapabilitySet =
            snapshot.granted_capabilities.iter().cloned().collect();

        for mut record in snapshot.records {
            let package_name = record.manifest.name.clone();
            record.compatibility = PackageCompatibility::for_manifest(&record.manifest);
            if !record.compatibility.is_compatible() {
                return Err(PackageRegistrySnapshotError::BotsterCompatibility {
                    package_name,
                    diagnostics: record.compatibility.diagnostics.clone(),
                });
            }
            validate_runnable_entrypoints_for_snapshot(
                &package_name,
                &record.runnable_entrypoints,
            )?;
            validate_session_templates(&record.session_templates).map_err(|reason| {
                PackageRegistrySnapshotError::SessionTemplate {
                    package_name: package_name.clone(),
                    reason,
                }
            })?;
            record.admitted_host_profile = Self::admitted_host_profile_from_snapshot(&record)?;
            record.admitted_capabilities = Self::admitted_capabilities_from_snapshot(
                &record,
                &granted_capabilities,
                &snapshot.governed_surfaces,
            )?;
            if records.insert(package_name.clone(), record).is_some() {
                return Err(PackageRegistrySnapshotError::DuplicatePackage(package_name));
            }
        }

        Ok(Self {
            records,
            granted_capabilities: snapshot.granted_capabilities.into_iter().collect(),
            governed_surfaces: snapshot.governed_surfaces,
        })
    }

    /// Prepare enabled local packages for core lifecycle wiring.
    pub fn prepare_enabled_local_packages(
        &self,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<Vec<PreparedLocalPackage>> {
        let audit_reason = audit_reason.into();
        self.records
            .values()
            .filter(|record| record.is_enabled())
            .filter(|record| matches!(record.manifest.source, Some(PackageSource::Path { .. })))
            .map(|record| PreparedLocalPackage::from_record(record, audit_reason.clone()))
            .collect()
    }

    /// Prepare one enabled local package for core lifecycle wiring.
    pub fn prepare_local_package(
        &self,
        package_name: &str,
        audit_reason: impl Into<String>,
    ) -> PackageRegistryResult<PreparedLocalPackage> {
        let audit_reason = audit_reason.into();
        let record = self.record(package_name, PackageAction::Prepare, audit_reason.clone())?;
        if !record.is_enabled() {
            return Err(PackageRegistryError::with_record(
                package_name,
                PackageAction::Prepare,
                PackageAdmissionReason::PackageNotEnabled,
                record.state,
                record.classification,
                audit_reason,
            ));
        }
        PreparedLocalPackage::from_record(record, audit_reason)
    }

    fn admitted_host_profile_from_snapshot(
        record: &PackageRecord,
    ) -> Result<Option<AdmittedHostProfile>, PackageRegistrySnapshotError> {
        if !record.is_enabled() {
            return Ok(None);
        }

        admit_enabled_host_profile(&record.manifest, record.classification).map_err(|reason| {
            let error = match reason {
                PackageAdmissionReason::ProviderMissingHostProfile => {
                    HostProfileAdmissionError::MissingMetadata
                }
                PackageAdmissionReason::HostProfileAdmission(error) => error,
                other => unreachable!(
                    "admit_enabled_host_profile returned unexpected package admission reason: {other:?}"
                ),
            };
            PackageRegistrySnapshotError::HostProfileAdmission {
                package_name: record.manifest.name.clone(),
                error,
            }
        })
    }

    fn admitted_capabilities_from_snapshot(
        record: &PackageRecord,
        granted_capabilities: &CapabilitySet,
        governed_surfaces: &[CapabilitySurface],
    ) -> Result<Vec<Capability>, PackageRegistrySnapshotError> {
        if !record.is_enabled() {
            return Ok(Vec::new());
        }

        if let Some(capability) = record
            .manifest
            .capabilities
            .iter()
            .find(|capability| !governed_surfaces.contains(&capability.surface))
        {
            return Err(PackageRegistrySnapshotError::CapabilityAdmission {
                package_name: record.manifest.name.clone(),
                reason: PackageAdmissionReason::UngovernedCapabilitySurface(
                    capability.surface.clone(),
                ),
            });
        }

        if let Some(capability) = record
            .manifest
            .capabilities
            .iter()
            .find(|capability| !granted_capabilities.contains(capability))
        {
            return Err(PackageRegistrySnapshotError::CapabilityAdmission {
                package_name: record.manifest.name.clone(),
                reason: PackageAdmissionReason::UngrantedCapability(capability.clone()),
            });
        }

        Ok(record.manifest.capabilities.clone())
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

fn provider_ids_for_record(record: &PackageRecord) -> Vec<String> {
    if !record.is_enabled() || !matches!(record.manifest.kind, ExtensionKind::Provider) {
        return Vec::new();
    }

    let mut providers = vec![record.manifest.name.clone()];
    if let Some(host_profile) = &record.manifest.host_profile {
        providers.push(host_profile.profile_id.clone());
    }
    providers.sort();
    providers.dedup();
    providers
}

/// Stored package policy record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRecord {
    /// Core-owned package manifest.
    pub manifest: PackageManifest,
    /// Hub-owned enabled/disabled state.
    pub state: PackageState,
    /// Hub classification derived from the core extension kind.
    pub classification: PackageClassification,
    /// Hub-owned trust marker for first-party/local/third-party package policy.
    #[serde(default)]
    pub trust: PackageTrust,
    /// Hub-owned provenance placeholder.
    pub provenance: PackageProvenance,
    /// Sanitized hub-owned source metadata for registry-installed packages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_metadata: Option<PackageSourceMetadata>,
    /// Optional hub-owned pin metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<PackagePin>,
    /// Hub-owned update policy placeholder.
    pub update_policy: PackageUpdatePolicy,
    /// Capabilities admitted by the hub grant set for the current enabled record.
    #[serde(default)]
    pub admitted_capabilities: Vec<Capability>,
    /// Narrow Botster compatibility result derived from the core manifest.
    #[serde(default)]
    pub compatibility: PackageCompatibility,
    /// Hub-owned local/dev runnable entrypoint declarations.
    #[serde(default)]
    pub runnable_entrypoints: Vec<PackageRunnableEntrypoint>,
    /// Hub-owned local/dev session template declarations.
    #[serde(default)]
    pub session_templates: Vec<PackageSessionTemplate>,
    /// Hub-owned persisted package configuration values.
    #[serde(default)]
    pub configuration: PackageConfigurationState,
    /// Optional install timestamp supplied by future installer paths.
    #[serde(default)]
    pub installed_at: Option<String>,
    /// Optional latest update timestamp supplied by future installer paths.
    #[serde(default)]
    pub updated_at: Option<String>,
    /// Operator/audit reason for the latest registry mutation.
    pub last_audit_reason: String,
    /// Runtime admission metadata re-derived from persisted manifests on reload.
    ///
    /// This field is skipped in durable JSON because `AdmittedHostProfile` is a
    /// core runtime result, not a serde-stable storage contract.
    #[serde(skip)]
    pub admitted_host_profile: Option<AdmittedHostProfile>,
}

impl PackageRecord {
    /// Whether hub policy currently treats the package as active.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        matches!(self.state, PackageState::Enabled)
    }

    /// Build the sanitized configuration view exposed to clients.
    #[must_use]
    pub fn configuration_view(&self) -> PackageConfigurationView {
        PackageConfigurationView::from_record(self)
    }
}

/// Hub-owned persisted configuration state for one package.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageConfigurationState {
    /// Persisted non-secret values and redacted secret markers keyed by schema field.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub values: BTreeMap<String, PackageConfigurationValue>,
}

/// Sanitized effective configuration view for daemon/client DTOs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageConfigurationView {
    /// Manifest-declared schema.
    pub schema: Option<PackageConfigurationSchema>,
    /// Defaults plus stored values, with secrets redacted or unset.
    pub effective_values: BTreeMap<String, PackageConfigurationValue>,
    /// Required field keys still missing effective values.
    pub missing_required: Vec<String>,
    /// Schema/value diagnostics surfaced without raw secret material.
    pub diagnostics: Vec<PackageConfigurationDiagnostic>,
}

impl PackageConfigurationView {
    fn from_record(record: &PackageRecord) -> Self {
        let Some(schema) = record.manifest.configuration.clone() else {
            return Self {
                schema: None,
                effective_values: BTreeMap::new(),
                missing_required: Vec::new(),
                diagnostics: Vec::new(),
            };
        };

        let mut effective_values = BTreeMap::new();
        let mut missing_required = Vec::new();
        let mut diagnostics = Vec::new();

        for field in &schema.fields {
            if let Err(PackageAdmissionReason::InvalidConfiguration(mut field_diagnostics)) =
                validate_configuration_default(&record.manifest.name, field)
            {
                diagnostics.append(&mut field_diagnostics);
            }

            let value = record
                .configuration
                .values
                .get(&field.key)
                .cloned()
                .or_else(|| field.default.clone())
                .map(effective_configuration_value);
            if let Some(value) = value
                && !configuration_value_is_unset_secret(&value)
            {
                effective_values.insert(field.key.clone(), value);
                continue;
            }
            if field.required {
                missing_required.push(field.key.clone());
            }
        }

        Self {
            schema: Some(schema),
            effective_values,
            missing_required,
            diagnostics,
        }
    }

    /// Whether the view carries schema diagnostics.
    #[must_use]
    pub fn has_blocking_diagnostics(&self) -> bool {
        !self.diagnostics.is_empty()
    }
}

/// Sanitized package configuration diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PackageConfigurationDiagnostic {
    /// Stable diagnostic kind.
    pub kind: String,
    /// Optional field key.
    pub field: Option<String>,
    /// Path-neutral message.
    pub message: String,
}

/// Hub-owned package entrypoint declaration for local/dev runnable processes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRunnableEntrypoint {
    /// Stable manifest-local entrypoint id.
    pub id: String,
    /// Core-owned runnable entrypoint kind.
    pub kind: RunnableEntrypointKind,
    /// Core-owned host launch mode.
    pub launch_mode: RunnableEntrypointLaunchMode,
    /// Command name or package-relative command path.
    pub command: String,
    /// Command arguments. These are declarative and are not shell-expanded by this contract.
    #[serde(default)]
    pub args: Vec<String>,
    /// Working-directory policy for future process spawning.
    #[serde(default)]
    pub working_directory: PackageRunnableWorkingDirectory,
    /// Declarative environment requirements. Values are optional defaults, not host snapshots.
    #[serde(default)]
    pub environment: Vec<PackageEnvironmentRequirement>,
    /// Capability declarations needed by this entrypoint.
    #[serde(default)]
    pub capabilities: Vec<Capability>,
    /// Structured readiness metadata declared by the core runnable contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<RunnableEntrypointReadiness>,
    /// Static policy declaring whether the hub may supervise this entrypoint later.
    #[serde(default)]
    pub may_supervise: bool,
    /// Static process-state DTO. This ticket does not spawn processes, so it defaults to not_started.
    #[serde(default)]
    pub process: PackageRunnableProcess,
}

/// Working-directory policy for a runnable package entrypoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum PackageRunnableWorkingDirectory {
    #[default]
    PackageRoot,
    EntrypointDir,
    Relative {
        path: String,
    },
}

/// Declarative environment requirement for a runnable package entrypoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageEnvironmentRequirement {
    /// Environment variable name.
    pub name: String,
    /// Whether the variable must be present when a future launcher resolves it.
    #[serde(default = "default_required_environment")]
    pub required: bool,
    /// Optional manifest-supplied default. This is not a host-resolved secret value.
    #[serde(default)]
    pub default: Option<String>,
    /// Optional operator-facing description.
    #[serde(default)]
    pub description: Option<String>,
}

fn default_required_environment() -> bool {
    true
}

/// Daemon-resolved launch data for a local foreground runnable entrypoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageResolvedForegroundLaunch {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: PathBuf,
    pub environment: BTreeMap<String, String>,
}

/// Resolve the host-local foreground launch contract owned by a package row.
pub fn resolve_foreground_launch_contract(
    record: &PackageRecord,
    entrypoint: &PackageRunnableEntrypoint,
    data_directory: &Path,
    socket_path: &Path,
) -> Result<PackageResolvedForegroundLaunch, String> {
    let package_root = package_root(record)?;
    let working_directory = match &entrypoint.working_directory {
        PackageRunnableWorkingDirectory::PackageRoot => package_root.clone(),
        PackageRunnableWorkingDirectory::EntrypointDir => {
            resolve_command_path(&package_root, entrypoint.command.as_str())
                .parent()
                .unwrap_or(&package_root)
                .to_path_buf()
        }
        PackageRunnableWorkingDirectory::Relative { path } => package_root.join(path),
    };
    let mut environment = BTreeMap::new();
    environment.insert(
        "BOTSTER_HUB_DATA_DIR".to_string(),
        data_directory.display().to_string(),
    );
    environment.insert(
        "BOTSTER_HUB_SOCKET".to_string(),
        socket_path.display().to_string(),
    );
    for requirement in &entrypoint.environment {
        if let Some(default) = requirement.default.as_ref() {
            environment
                .entry(requirement.name.clone())
                .or_insert_with(|| default.clone());
        }
    }
    Ok(PackageResolvedForegroundLaunch {
        command: resolve_command_path(&package_root, entrypoint.command.as_str())
            .display()
            .to_string(),
        args: entrypoint.args.clone(),
        working_directory,
        environment,
    })
}

fn package_root(record: &PackageRecord) -> Result<PathBuf, String> {
    match &record.manifest.source {
        Some(PackageSource::Path { path }) => Ok(PathBuf::from(path)),
        _ => Err("package source is not a local path".to_string()),
    }
}

fn resolve_command_path(package_root: &Path, command: &str) -> PathBuf {
    let command_path = Path::new(command);
    if command_path.is_absolute() || command_path.components().count() == 1 {
        command_path.to_path_buf()
    } else {
        package_root.join(command_path)
    }
}

/// Static process-state DTO for package runnable entrypoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRunnableProcess {
    /// Current static state.
    pub state: PackageRunnableProcessState,
    /// Operator-facing diagnostics.
    #[serde(default)]
    pub diagnostics: Vec<PackageRunnableDiagnostic>,
}

impl Default for PackageRunnableProcess {
    fn default() -> Self {
        Self {
            state: PackageRunnableProcessState::NotStarted,
            diagnostics: Vec::new(),
        }
    }
}

/// Stable process states exposed before runtime spawning exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageRunnableProcessState {
    NotStarted,
    Starting,
    Running,
    Exited,
    Failed,
    Stopped,
}

/// Operator-facing process diagnostic row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRunnableDiagnostic {
    /// Diagnostic classifier.
    pub kind: String,
    /// Sanitized diagnostic message.
    pub message: String,
}

/// Hub-owned package trust marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageTrust {
    /// Coarse trust classification used by hub package policy.
    pub classification: PackageTrustClassification,
    /// Explicit first-party marker for trusted Botster-owned packages.
    pub first_party: bool,
}

impl PackageTrust {
    /// First-party package owned by the hub/profile operator.
    #[must_use]
    pub const fn first_party() -> Self {
        Self {
            classification: PackageTrustClassification::FirstParty,
            first_party: true,
        }
    }

    /// Local development package whose authority comes from local operator action.
    #[must_use]
    pub const fn local_development() -> Self {
        Self {
            classification: PackageTrustClassification::LocalDevelopment,
            first_party: false,
        }
    }

    /// Third-party package until an installer/operator marks it otherwise.
    #[must_use]
    pub const fn third_party() -> Self {
        Self {
            classification: PackageTrustClassification::ThirdParty,
            first_party: false,
        }
    }
}

impl Default for PackageTrust {
    fn default() -> Self {
        Self::third_party()
    }
}

/// Durable package trust classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageTrustClassification {
    /// Botster-owned or operator-designated first-party package.
    FirstParty,
    /// Local development package installed from an explicit path.
    LocalDevelopment,
    /// Package without first-party trust.
    ThirdParty,
}

/// Persisted narrow compatibility result for the current hub binary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageCompatibility {
    /// Manifest field evaluated by hub policy.
    pub botster_requirement: String,
    /// Hub version used for the evaluation.
    pub hub_version: String,
    /// Result of the narrow compatibility check.
    pub result: PackageCompatibilityResult,
    /// Operator-facing diagnostics without local paths or secrets.
    pub diagnostics: Vec<String>,
}

impl PackageCompatibility {
    fn for_manifest(manifest: &PackageManifest) -> Self {
        Self::evaluate(&manifest.botster, env!("CARGO_PKG_VERSION"))
    }

    fn evaluate(requirement: &str, hub_version: &str) -> Self {
        let mut diagnostics = Vec::new();
        let result = match compatibility_requirement_satisfied(requirement, hub_version) {
            Ok(true) => {
                diagnostics.push(format!(
                    "botster requirement {requirement} is satisfied by hub {hub_version}"
                ));
                PackageCompatibilityResult::Compatible
            }
            Ok(false) => {
                diagnostics.push(format!(
                    "botster requirement {requirement} is not satisfied by hub {hub_version}"
                ));
                PackageCompatibilityResult::Incompatible
            }
            Err(message) => {
                diagnostics.push(message);
                PackageCompatibilityResult::InvalidRequirement
            }
        };

        Self {
            botster_requirement: requirement.to_string(),
            hub_version: hub_version.to_string(),
            result,
            diagnostics,
        }
    }

    fn is_compatible(&self) -> bool {
        matches!(self.result, PackageCompatibilityResult::Compatible)
    }
}

impl Default for PackageCompatibility {
    fn default() -> Self {
        Self::evaluate(">=0.0.0", env!("CARGO_PKG_VERSION"))
    }
}

/// Narrow persisted compatibility outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageCompatibilityResult {
    /// Current hub version satisfies the manifest requirement.
    Compatible,
    /// Current hub version does not satisfy the manifest requirement.
    Incompatible,
    /// Manifest requirement syntax is outside the supported narrow contract.
    InvalidRequirement,
}

/// Hub-owned package state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageState {
    /// Installed but not active.
    Installed,
    /// Active under current hub grants.
    Enabled,
    /// Explicitly disabled after install or enable.
    Disabled,
}

/// Hub package classification derived from `botster-core::ExtensionKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageProvenance {
    /// Source identifier suitable for audit displays, including local package roots.
    pub source: String,
    /// Optional checksum recorded by future installer/lockfile paths.
    pub checksum: Option<String>,
}

/// Hub-owned pin metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackagePin {
    /// Revision, tag, or version pinned by policy.
    pub revision: String,
    /// Branch pin supplied by a git-shaped registry entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Tag pin supplied by a git-shaped registry entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Commit/revision pin supplied by a git-shaped registry entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rev: Option<String>,
    /// Optional checksum paired with the pin.
    pub checksum: Option<String>,
    /// Update behavior while the package is pinned.
    pub update_policy: PackageUpdatePolicy,
}

/// Sanitized hub-owned metadata recording which registry entry installed a package.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageSourceMetadata {
    /// Registry source identifier.
    pub registry_id: String,
    /// Registry source kind.
    pub registry_kind: PackageRegistrySourceKind,
    /// Registry entry identifier.
    pub entry_id: String,
    /// Package source kind.
    pub source_kind: PackageRegistryEntrySourceKind,
    /// Path-neutral source label for clients and audit displays.
    pub source_label: String,
    /// Git repo URL for git-shaped entries. Local path entries omit this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_repo: Option<String>,
}

impl PackageSourceMetadata {
    fn from_catalog_entry(catalog: &LocalRegistryCatalog, entry: &PackageRegistryEntry) -> Self {
        Self {
            registry_id: catalog.source.id.clone(),
            registry_kind: catalog.source.kind,
            entry_id: entry.id.clone(),
            source_kind: entry.source.kind(),
            source_label: entry.source.label(&entry.id),
            git_repo: match &entry.source {
                PackageRegistryEntrySource::Git { repo, .. } => Some(repo.clone()),
                PackageRegistryEntrySource::LocalPath { .. } => None,
            },
        }
    }
}

/// Hub-owned marketplace registry source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRegistrySource {
    /// Stable source id.
    pub id: String,
    /// Local/static registry source kind.
    pub kind: PackageRegistrySourceKind,
    /// Operator-facing label.
    #[serde(default)]
    pub label: String,
}

/// Supported hub marketplace registry source kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageRegistrySourceKind {
    LocalPath,
    StaticFirstParty,
}

/// Available package row from a local/static registry catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailablePackage {
    pub entry_id: String,
    pub package_name: String,
    pub version: String,
    pub classification: PackageClassification,
    pub source_kind: PackageRegistryEntrySourceKind,
    pub source_label: String,
    pub first_party: bool,
    pub requested_capabilities: Vec<Capability>,
    pub compatibility: PackageCompatibility,
    pub state: AvailablePackageState,
    pub pin: Option<PackagePin>,
}

/// Installed-vs-available state for a catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvailablePackageState {
    Available,
    Installed,
    Enabled,
    Disabled,
}

/// Install preview returned before an explicit registry install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageInstallPlan {
    pub entry: AvailablePackage,
    pub effects: Vec<PackageInstallEffect>,
    pub diagnostics: Vec<PackageInstallDiagnostic>,
    pub mutates_registry: bool,
    pub starts_entrypoints: bool,
}

/// Preview effect row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageInstallEffect {
    pub kind: String,
    pub message: String,
}

/// Preview diagnostic row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageInstallDiagnostic {
    pub kind: String,
    pub message: String,
}

/// Hub-owned update policy placeholder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    Show,
    Configure,
    Reload,
    Enable,
    Disable,
    Remove,
    CheckUpdate,
    PreviewUpdate,
    ApplyUpdate,
    Pin,
    Prepare,
}

/// Trusted durable export of package records and hub-owned grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRegistrySnapshot {
    /// Capabilities granted by hub package policy.
    pub granted_capabilities: Vec<Capability>,
    /// Capability surfaces governed by the current host profile.
    pub governed_surfaces: Vec<CapabilitySurface>,
    /// Package records in deterministic package-name order.
    pub records: Vec<PackageRecord>,
}

impl PackageRegistrySnapshot {
    /// Empty snapshot governed by the current first-party host profile.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            granted_capabilities: host_profile().default_capability_grants().to_vec(),
            governed_surfaces: host_profile().capability_surfaces().to_vec(),
            records: Vec::new(),
        }
    }
}

/// Error returned when persisted registry state is internally inconsistent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackageRegistrySnapshotError {
    /// More than one persisted record used the same package manifest name.
    DuplicatePackage(String),
    /// Persisted package compatibility no longer admits under the current hub.
    BotsterCompatibility {
        /// Package whose persisted compatibility state could not be re-derived.
        package_name: String,
        /// Compatibility diagnostics.
        diagnostics: Vec<String>,
    },
    /// Persisted enabled package capabilities no longer admit under current grants.
    CapabilityAdmission {
        /// Package whose capabilities no longer admit.
        package_name: String,
        /// Typed admission reason.
        reason: PackageAdmissionReason,
    },
    /// Persisted enabled provider/plugin host-profile state no longer admits cleanly.
    HostProfileAdmission {
        /// Package whose persisted admission state could not be re-derived.
        package_name: String,
        /// Core admission error.
        error: HostProfileAdmissionError,
    },
    /// Persisted runnable entrypoint declarations no longer validate.
    RunnableEntrypoint {
        /// Package whose persisted runnable entrypoints could not be re-derived.
        package_name: String,
        /// Sanitized validation reason.
        reason: String,
    },
    /// Persisted session template declarations no longer validate.
    SessionTemplate {
        /// Package whose persisted session templates could not be re-derived.
        package_name: String,
        /// Sanitized validation reason.
        reason: String,
    },
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
    pub(crate) fn without_record(
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
    /// Enabled package must be active before preparation.
    PackageNotEnabled,
    /// Local path source was absent or unsafe.
    UnsafeLocalPath(String),
    /// Local package manifest could not be read or parsed.
    InvalidLocalManifest(String),
    /// Local package entrypoint was absent or unsafe.
    UnsafeEntrypoint(String),
    /// Local package session template was absent or unsafe.
    UnsafeSessionTemplate(String),
    /// Capability surface is not governed by the current host profile.
    UngovernedCapabilitySurface(CapabilitySurface),
    /// Manifest requested a capability not present in the hub-owned grant set.
    UngrantedCapability(Capability),
    /// Provider-classified packages must carry host-profile metadata before enablement.
    ProviderMissingHostProfile,
    /// Core host-profile admission rejected the provider package.
    HostProfileAdmission(HostProfileAdmissionError),
    /// Manifest Botster compatibility requirement is unsupported or unsatisfied.
    BotsterCompatibility(Vec<String>),
    /// Submitted or manifest default configuration did not match the schema.
    InvalidConfiguration(Vec<PackageConfigurationDiagnostic>),
    /// Required configuration keys are missing before enablement.
    MissingRequiredConfiguration(Vec<String>),
}

fn configuration_schema(
    record: &PackageRecord,
) -> PackageRegistryResult<&PackageConfigurationSchema> {
    record.manifest.configuration.as_ref().ok_or_else(|| {
        PackageRegistryError::with_record(
            record.manifest.name.clone(),
            PackageAction::Configure,
            PackageAdmissionReason::InvalidConfiguration(vec![PackageConfigurationDiagnostic {
                kind: "schema_missing".to_string(),
                field: None,
                message: "package does not declare configuration schema".to_string(),
            }]),
            record.state,
            record.classification,
            "configure package".to_string(),
        )
    })
}

fn validate_configuration_values(
    package_name: &str,
    schema: &PackageConfigurationSchema,
    values: &BTreeMap<String, PackageConfigurationValue>,
) -> Result<(), PackageAdmissionReason> {
    let mut diagnostics = Vec::new();
    let fields: BTreeMap<_, _> = schema
        .fields
        .iter()
        .map(|field| (field.key.as_str(), field))
        .collect();

    for field in &schema.fields {
        if let Err(PackageAdmissionReason::InvalidConfiguration(mut field_diagnostics)) =
            validate_configuration_default(package_name, field)
        {
            diagnostics.append(&mut field_diagnostics);
        }
    }

    for (key, value) in values {
        let Some(field) = fields.get(key.as_str()) else {
            diagnostics.push(PackageConfigurationDiagnostic {
                kind: "unknown_field".to_string(),
                field: Some(key.clone()),
                message: format!("package {package_name} has no configuration field {key}"),
            });
            continue;
        };
        if !configuration_value_matches_field_type(value, &field.field_type) {
            diagnostics.push(PackageConfigurationDiagnostic {
                kind: "value_type_mismatch".to_string(),
                field: Some(key.clone()),
                message: format!(
                    "configuration field {key} expects {}",
                    configuration_field_type_label(&field.field_type)
                ),
            });
            continue;
        }
        if let PackageConfigurationValue::Select { value } = value
            && !field.options.iter().any(|option| option.value == *value)
        {
            diagnostics.push(PackageConfigurationDiagnostic {
                kind: "select_option_unknown".to_string(),
                field: Some(key.clone()),
                message: format!("configuration field {key} does not allow option {value}"),
            });
        }
    }

    if diagnostics.is_empty() {
        Ok(())
    } else {
        Err(PackageAdmissionReason::InvalidConfiguration(diagnostics))
    }
}

fn validate_configuration_default(
    package_name: &str,
    field: &PackageConfigurationField,
) -> Result<(), PackageAdmissionReason> {
    let Some(default) = field.default.as_ref() else {
        return Ok(());
    };
    if !configuration_value_matches_field_type(default, &field.field_type) {
        return Err(PackageAdmissionReason::InvalidConfiguration(vec![
            PackageConfigurationDiagnostic {
                kind: "default_type_mismatch".to_string(),
                field: Some(field.key.clone()),
                message: format!(
                    "package {package_name} configuration field {} default does not match {}",
                    field.key,
                    configuration_field_type_label(&field.field_type)
                ),
            },
        ]));
    }
    if let PackageConfigurationValue::Select { value } = default
        && !field.options.iter().any(|option| option.value == *value)
    {
        return Err(PackageAdmissionReason::InvalidConfiguration(vec![
            PackageConfigurationDiagnostic {
                kind: "default_select_option_unknown".to_string(),
                field: Some(field.key.clone()),
                message: format!(
                    "package {package_name} configuration field {} default does not allow option {value}",
                    field.key
                ),
            },
        ]));
    }
    Ok(())
}

fn configuration_value_matches_field_type(
    value: &PackageConfigurationValue,
    field_type: &PackageConfigurationFieldType,
) -> bool {
    matches!(
        (value, field_type),
        (
            PackageConfigurationValue::String { .. },
            PackageConfigurationFieldType::String
        ) | (
            PackageConfigurationValue::Number { .. },
            PackageConfigurationFieldType::Number
        ) | (
            PackageConfigurationValue::Integer { .. },
            PackageConfigurationFieldType::Integer
        ) | (
            PackageConfigurationValue::Boolean { .. },
            PackageConfigurationFieldType::Boolean
        ) | (
            PackageConfigurationValue::Select { .. },
            PackageConfigurationFieldType::Select
        ) | (
            PackageConfigurationValue::Path { .. },
            PackageConfigurationFieldType::Path
        ) | (
            PackageConfigurationValue::Url { .. },
            PackageConfigurationFieldType::Url
        ) | (
            PackageConfigurationValue::MultilineText { .. },
            PackageConfigurationFieldType::MultilineText
        ) | (
            PackageConfigurationValue::Secret { .. },
            PackageConfigurationFieldType::Secret
        )
    )
}

fn stored_configuration_value(value: PackageConfigurationValue) -> PackageConfigurationValue {
    match value {
        PackageConfigurationValue::Secret {
            state:
                PackageConfigurationSecretValue::WriteOnly | PackageConfigurationSecretValue::Redacted,
        } => PackageConfigurationValue::Secret {
            state: PackageConfigurationSecretValue::Redacted,
        },
        other => other,
    }
}

fn effective_configuration_value(value: PackageConfigurationValue) -> PackageConfigurationValue {
    match value {
        PackageConfigurationValue::Secret {
            state: PackageConfigurationSecretValue::WriteOnly,
        } => PackageConfigurationValue::Secret {
            state: PackageConfigurationSecretValue::Redacted,
        },
        other => other,
    }
}

fn configuration_value_is_unset_secret(value: &PackageConfigurationValue) -> bool {
    matches!(
        value,
        PackageConfigurationValue::Secret {
            state: PackageConfigurationSecretValue::Unset
        }
    )
}

fn configuration_field_type_label(field_type: &PackageConfigurationFieldType) -> &'static str {
    match field_type {
        PackageConfigurationFieldType::String => "string",
        PackageConfigurationFieldType::Number => "number",
        PackageConfigurationFieldType::Integer => "integer",
        PackageConfigurationFieldType::Boolean => "boolean",
        PackageConfigurationFieldType::Select => "select",
        PackageConfigurationFieldType::Path => "path",
        PackageConfigurationFieldType::Url => "url",
        PackageConfigurationFieldType::MultilineText => "multiline_text",
        PackageConfigurationFieldType::Secret => "secret",
    }
}

fn compatibility_requirement_satisfied(
    requirement: &str,
    hub_version: &str,
) -> Result<bool, String> {
    if requirement.is_empty() {
        return Err("botster requirement is empty".to_string());
    }

    let (operator, required_version) = requirement
        .strip_prefix(">=")
        .map_or(("=", requirement), |version| (">=", version));
    let required = parse_point_version(required_version).ok_or_else(|| {
        format!(
            "botster requirement {requirement} must be MAJOR.MINOR.PATCH or >=MAJOR.MINOR.PATCH"
        )
    })?;
    let current = parse_point_version(hub_version)
        .ok_or_else(|| format!("hub version {hub_version} is not MAJOR.MINOR.PATCH"))?;

    Ok(match operator {
        ">=" => current >= required,
        "=" => current == required,
        _ => unreachable!("compatibility operator is derived above"),
    })
}

fn parse_point_version(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

fn admit_enabled_host_profile(
    manifest: &PackageManifest,
    classification: PackageClassification,
) -> Result<Option<AdmittedHostProfile>, PackageAdmissionReason> {
    match classification {
        PackageClassification::Plugin if manifest.host_profile.is_none() => Ok(None),
        PackageClassification::Plugin => {
            admit_host_profile(manifest, true, env!("CARGO_PKG_VERSION"))
                .map(Some)
                .map_err(PackageAdmissionReason::HostProfileAdmission)
        }
        PackageClassification::Provider if manifest.host_profile.is_none() => {
            Err(PackageAdmissionReason::ProviderMissingHostProfile)
        }
        PackageClassification::Provider => {
            admit_host_profile(manifest, true, env!("CARGO_PKG_VERSION"))
                .map(Some)
                .map_err(PackageAdmissionReason::HostProfileAdmission)
        }
    }
}

impl PackageRegistry {
    fn available_package(
        &self,
        catalog: &LocalRegistryCatalog,
        entry: &PackageRegistryEntry,
    ) -> PackageRegistryResult<AvailablePackage> {
        let prepared = PreparedRegistryEntry::from_catalog_entry(catalog, entry)?;
        Ok(AvailablePackage {
            entry_id: entry.id.clone(),
            package_name: prepared.manifest.name.clone(),
            version: prepared.manifest.version.clone(),
            classification: PackageClassification::from_kind(&prepared.manifest.kind),
            source_kind: entry.source.kind(),
            source_label: entry.source.label(&entry.id),
            first_party: entry.first_party,
            requested_capabilities: prepared.manifest.capabilities.clone(),
            compatibility: PackageCompatibility::for_manifest(&prepared.manifest),
            state: self.available_state(&prepared.manifest.name),
            pin: prepared.pin,
        })
    }

    fn install_plan(
        &self,
        catalog: &LocalRegistryCatalog,
        entry: &PackageRegistryEntry,
        prepared: &PreparedRegistryEntry,
    ) -> PackageInstallPlan {
        let available = AvailablePackage {
            entry_id: entry.id.clone(),
            package_name: prepared.manifest.name.clone(),
            version: prepared.manifest.version.clone(),
            classification: PackageClassification::from_kind(&prepared.manifest.kind),
            source_kind: entry.source.kind(),
            source_label: entry.source.label(&entry.id),
            first_party: entry.first_party,
            requested_capabilities: prepared.manifest.capabilities.clone(),
            compatibility: PackageCompatibility::for_manifest(&prepared.manifest),
            state: self.available_state(&prepared.manifest.name),
            pin: prepared.pin.clone(),
        };
        let mut effects = Vec::new();
        let mut diagnostics = Vec::new();

        if matches!(available.state, AvailablePackageState::Available) {
            effects.push(PackageInstallEffect {
                kind: "add_package_record".to_string(),
                message: format!(
                    "would add {} from {}",
                    available.package_name, catalog.source.id
                ),
            });
        } else {
            effects.push(PackageInstallEffect {
                kind: "already_installed".to_string(),
                message: format!(
                    "{} is already {}",
                    available.package_name,
                    available_state_label(available.state)
                ),
            });
        }

        effects.push(PackageInstallEffect {
            kind: "record_source_metadata".to_string(),
            message: "would persist registry source metadata and pin details".to_string(),
        });
        effects.push(PackageInstallEffect {
            kind: "explicit_enable_required".to_string(),
            message: "would remain installed until explicitly enabled".to_string(),
        });
        effects.push(PackageInstallEffect {
            kind: "no_entrypoint_start".to_string(),
            message: "would not start package entrypoints".to_string(),
        });
        if matches!(entry.source, PackageRegistryEntrySource::Git { .. }) {
            effects.push(PackageInstallEffect {
                kind: "no_network_fetch".to_string(),
                message: "would record git metadata without clone or fetch".to_string(),
            });
        }

        if !available.compatibility.is_compatible() {
            diagnostics.extend(available.compatibility.diagnostics.iter().map(|message| {
                PackageInstallDiagnostic {
                    kind: "botster_compatibility".to_string(),
                    message: message.clone(),
                }
            }));
        }
        for capability in &prepared.manifest.capabilities {
            if !self.granted_capabilities.contains(capability) {
                diagnostics.push(PackageInstallDiagnostic {
                    kind: "ungranted_capability".to_string(),
                    message: format!(
                        "capability {} is not granted by the current hub profile",
                        capability_label(capability)
                    ),
                });
            }
        }

        PackageInstallPlan {
            entry: available,
            effects,
            diagnostics,
            mutates_registry: false,
            starts_entrypoints: false,
        }
    }

    fn available_state(&self, package_name: &str) -> AvailablePackageState {
        match self.records.get(package_name).map(|record| record.state) {
            Some(PackageState::Installed) => AvailablePackageState::Installed,
            Some(PackageState::Enabled) => AvailablePackageState::Enabled,
            Some(PackageState::Disabled) => AvailablePackageState::Disabled,
            None => AvailablePackageState::Available,
        }
    }
}

fn available_state_label(state: AvailablePackageState) -> &'static str {
    match state {
        AvailablePackageState::Available => "available",
        AvailablePackageState::Installed => "installed",
        AvailablePackageState::Enabled => "enabled",
        AvailablePackageState::Disabled => "disabled",
    }
}

fn capability_label(capability: &Capability) -> String {
    match &capability.scope {
        Some(scope) => format!("{:?}:{scope}", capability.surface),
        None => format!("{:?}", capability.surface),
    }
}

#[derive(Debug, Deserialize)]
struct LocalRegistryCatalogFile {
    source: PackageRegistrySource,
    #[serde(default)]
    entries: Vec<PackageRegistryEntry>,
}

#[derive(Debug)]
struct LocalRegistryCatalog {
    source: PackageRegistrySource,
    entries: Vec<PackageRegistryEntry>,
    root: PathBuf,
}

impl LocalRegistryCatalog {
    fn load(path: &Path) -> PackageRegistryResult<Self> {
        let source = if path.is_dir() {
            path.join(LOCAL_PACKAGE_REGISTRY_FILE)
        } else {
            path.to_path_buf()
        };
        let canonical = source.canonicalize().map_err(|error| {
            PackageRegistryError::without_record(
                "<registry>",
                PackageAction::Show,
                PackageAdmissionReason::InvalidLocalManifest(error.to_string()),
                "load package registry".to_string(),
            )
        })?;
        let root = canonical.parent().map(Path::to_path_buf).ok_or_else(|| {
            PackageRegistryError::without_record(
                "<registry>",
                PackageAction::Show,
                PackageAdmissionReason::UnsafeLocalPath(
                    "registry path has no parent directory".to_string(),
                ),
                "load package registry".to_string(),
            )
        })?;
        let bytes = fs::read(&canonical).map_err(|error| {
            PackageRegistryError::without_record(
                "<registry>",
                PackageAction::Show,
                PackageAdmissionReason::InvalidLocalManifest(error.to_string()),
                "load package registry".to_string(),
            )
        })?;
        let catalog: LocalRegistryCatalogFile =
            serde_json::from_slice(&bytes).map_err(|error| {
                PackageRegistryError::without_record(
                    "<registry>",
                    PackageAction::Show,
                    PackageAdmissionReason::InvalidLocalManifest(error.to_string()),
                    "load package registry".to_string(),
                )
            })?;
        Ok(Self {
            source: catalog.source,
            entries: catalog.entries,
            root,
        })
    }

    fn entry(&self, entry_id: &str) -> PackageRegistryResult<&PackageRegistryEntry> {
        self.entries
            .iter()
            .find(|entry| entry.id == entry_id)
            .ok_or_else(|| {
                PackageRegistryError::without_record(
                    entry_id,
                    PackageAction::Show,
                    PackageAdmissionReason::PackageNotInstalled,
                    "inspect registry package".to_string(),
                )
            })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct PackageRegistryEntry {
    id: String,
    #[serde(default)]
    first_party: bool,
    source: PackageRegistryEntrySource,
    #[serde(default)]
    manifest: Option<PackageManifest>,
    #[serde(default)]
    runnable_entrypoints: Vec<PackageRunnableEntrypoint>,
    #[serde(default)]
    session_templates: Vec<PackageSessionTemplate>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PackageRegistryEntrySource {
    LocalPath {
        path: String,
    },
    Git {
        repo: String,
        #[serde(default)]
        branch: Option<String>,
        #[serde(default)]
        tag: Option<String>,
        #[serde(default)]
        rev: Option<String>,
    },
}

impl PackageRegistryEntrySource {
    fn kind(&self) -> PackageRegistryEntrySourceKind {
        match self {
            Self::LocalPath { .. } => PackageRegistryEntrySourceKind::LocalPath,
            Self::Git { .. } => PackageRegistryEntrySourceKind::Git,
        }
    }

    fn label(&self, entry_id: &str) -> String {
        match self {
            Self::LocalPath { .. } => format!("local:{entry_id}"),
            Self::Git { repo, .. } => repo.clone(),
        }
    }
}

/// Package source kind for registry entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageRegistryEntrySourceKind {
    LocalPath,
    Git,
}

#[derive(Debug, Clone)]
struct PreparedRegistryEntry {
    manifest: PackageManifest,
    provenance: PackageProvenance,
    runnable_entrypoints: Vec<PackageRunnableEntrypoint>,
    session_templates: Vec<PackageSessionTemplate>,
    pin: Option<PackagePin>,
}

impl PreparedRegistryEntry {
    fn from_catalog_entry(
        catalog: &LocalRegistryCatalog,
        entry: &PackageRegistryEntry,
    ) -> PackageRegistryResult<Self> {
        match &entry.source {
            PackageRegistryEntrySource::LocalPath { path } => {
                let absolute = safe_registry_relative_path(&catalog.root, path)?;
                let local_source = LocalPackageSource::resolve(
                    &absolute,
                    "load registry local package".to_string(),
                )?;
                let local_manifest =
                    local_source.read_manifest("load registry local package".to_string())?;
                let mut manifest = local_manifest.manifest;
                local_source.validate_manifest_entrypoints(
                    &manifest,
                    "load registry local package".to_string(),
                )?;
                let runnable_entrypoints = if entry.runnable_entrypoints.is_empty() {
                    local_manifest.runnable_entrypoints
                } else {
                    entry.runnable_entrypoints.clone()
                };
                let session_templates = if entry.session_templates.is_empty() {
                    local_manifest.session_templates
                } else {
                    entry.session_templates.clone()
                };
                validate_runnable_entrypoints(
                    &manifest.name,
                    &runnable_entrypoints,
                    PackageAction::Install,
                    "load registry local package".to_string(),
                )?;
                validate_session_templates(&session_templates).map_err(|reason| {
                    PackageRegistryError::without_record(
                        manifest.name.clone(),
                        PackageAction::Install,
                        PackageAdmissionReason::UnsafeSessionTemplate(reason),
                        "load registry local package".to_string(),
                    )
                })?;
                manifest.source = Some(PackageSource::Path {
                    path: local_source.package_root.to_string_lossy().into_owned(),
                });
                Ok(Self {
                    manifest,
                    provenance: PackageProvenance {
                        source: format!("registry:{}:{}", catalog.source.id, entry.id),
                        checksum: None,
                    },
                    runnable_entrypoints,
                    session_templates,
                    pin: None,
                })
            }
            PackageRegistryEntrySource::Git {
                repo,
                branch,
                tag,
                rev,
            } => {
                let mut manifest = entry.manifest.clone().ok_or_else(|| {
                    PackageRegistryError::without_record(
                        &entry.id,
                        PackageAction::Show,
                        PackageAdmissionReason::InvalidLocalManifest(
                            "git registry entry must include an inline manifest".to_string(),
                        ),
                        "load registry git package".to_string(),
                    )
                })?;
                let revision = git_pin_revision(branch, tag, rev)?;
                manifest.source = Some(PackageSource::Git {
                    repo: repo.clone(),
                    reference: revision.clone(),
                });
                Ok(Self {
                    manifest,
                    provenance: PackageProvenance {
                        source: format!("registry:{}:{}", catalog.source.id, entry.id),
                        checksum: None,
                    },
                    runnable_entrypoints: entry.runnable_entrypoints.clone(),
                    session_templates: entry.session_templates.clone(),
                    pin: Some(PackagePin {
                        revision,
                        branch: branch.clone(),
                        tag: tag.clone(),
                        rev: rev.clone(),
                        checksum: None,
                        update_policy: PackageUpdatePolicy::Manual,
                    }),
                })
            }
        }
    }
}

fn safe_registry_relative_path(root: &Path, value: &str) -> PackageRegistryResult<PathBuf> {
    let relative = Path::new(value);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(PackageRegistryError::without_record(
            value,
            PackageAction::Show,
            PackageAdmissionReason::UnsafeLocalPath(
                "registry entry path must stay inside registry directory".to_string(),
            ),
            "load package registry".to_string(),
        ));
    }
    Ok(root.join(relative))
}

fn git_pin_revision(
    branch: &Option<String>,
    tag: &Option<String>,
    rev: &Option<String>,
) -> PackageRegistryResult<String> {
    rev.as_ref()
        .or(tag.as_ref())
        .or(branch.as_ref())
        .cloned()
        .ok_or_else(|| {
            PackageRegistryError::without_record(
                "<git-registry-entry>",
                PackageAction::Pin,
                PackageAdmissionReason::MissingPinRevision,
                "load registry git package".to_string(),
            )
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedLocalPackage {
    /// Installed package name.
    pub package_name: String,
    /// Canonical local package root.
    pub package_root: PathBuf,
    /// Manifest entrypoints preserved from the core package manifest.
    pub entrypoints: Vec<ExtensionEntrypoint>,
    /// Selected code-load entrypoint from the manifest, if the package has one.
    pub selected_entrypoint: Option<ExtensionEntrypoint>,
    /// Canonical filesystem path for the selected code-load entrypoint, if the package has one.
    pub selected_entrypoint_path: Option<PathBuf>,
}

impl PreparedLocalPackage {
    fn from_record(record: &PackageRecord, audit_reason: String) -> PackageRegistryResult<Self> {
        let package_root = match &record.manifest.source {
            Some(PackageSource::Path { path }) => canonical_package_root(
                Path::new(path),
                &record.manifest.name,
                PackageAction::Prepare,
                audit_reason.clone(),
            )?,
            _ => {
                return Err(PackageRegistryError::with_record(
                    record.manifest.name.clone(),
                    PackageAction::Prepare,
                    PackageAdmissionReason::UnsafeLocalPath(
                        "package manifest source is not a local path".to_string(),
                    ),
                    record.state,
                    record.classification,
                    audit_reason,
                ));
            }
        };

        let (selected_entrypoint, selected_entrypoint_path) =
            if let Some(entrypoint) = record.manifest.entrypoints.first().cloned() {
                let entrypoint_path = canonical_entrypoint_path(
                    &package_root,
                    &entrypoint.path,
                    &record.manifest.name,
                    PackageAction::Prepare,
                    audit_reason.clone(),
                )?;
                (Some(entrypoint), Some(entrypoint_path))
            } else if record.runnable_entrypoints.is_empty() {
                return Err(PackageRegistryError::with_record(
                    record.manifest.name.clone(),
                    PackageAction::Prepare,
                    PackageAdmissionReason::UnsafeEntrypoint(
                        "local package has no entrypoints".to_string(),
                    ),
                    record.state,
                    record.classification,
                    audit_reason,
                ));
            } else {
                (None, None)
            };

        Ok(Self {
            package_name: record.manifest.name.clone(),
            package_root,
            entrypoints: record.manifest.entrypoints.clone(),
            selected_entrypoint,
            selected_entrypoint_path,
        })
    }

    pub fn selected_lua_entrypoint(&self) -> Option<&ExtensionEntrypoint> {
        self.selected_entrypoint
            .as_ref()
            .filter(|entrypoint| entrypoint.runtime == ExtensionRuntime::Lua)
    }
}

#[derive(Debug, Deserialize)]
struct LocalPackageManifest {
    #[serde(flatten)]
    manifest: PackageManifest,
    #[serde(default)]
    runnable_entrypoints: Vec<PackageRunnableEntrypoint>,
    #[serde(default)]
    session_templates: Vec<PackageSessionTemplate>,
}

#[derive(Debug, Clone)]
struct LocalPackageSource {
    package_root: PathBuf,
    manifest_path: PathBuf,
}

impl LocalPackageSource {
    fn resolve(path: &Path, audit_reason: String) -> PackageRegistryResult<Self> {
        if path.as_os_str().is_empty() {
            return Err(PackageRegistryError::without_record(
                "<local-package>",
                PackageAction::Install,
                PackageAdmissionReason::UnsafeLocalPath("local package path is empty".to_string()),
                audit_reason,
            ));
        }

        let canonical = path.canonicalize().map_err(|error| {
            PackageRegistryError::without_record(
                path.to_string_lossy(),
                PackageAction::Install,
                PackageAdmissionReason::UnsafeLocalPath(error.to_string()),
                audit_reason.clone(),
            )
        })?;

        if canonical.is_dir() {
            let manifest_path = canonical.join(LOCAL_PACKAGE_MANIFEST_FILE);
            let manifest_path = manifest_path.canonicalize().map_err(|error| {
                PackageRegistryError::without_record(
                    canonical.to_string_lossy(),
                    PackageAction::Install,
                    PackageAdmissionReason::InvalidLocalManifest(error.to_string()),
                    audit_reason.clone(),
                )
            })?;
            if !manifest_path.starts_with(&canonical) {
                return Err(PackageRegistryError::without_record(
                    canonical.to_string_lossy(),
                    PackageAction::Install,
                    PackageAdmissionReason::UnsafeLocalPath(
                        "directory manifest resolves outside package root".to_string(),
                    ),
                    audit_reason,
                ));
            }
            Ok(Self {
                package_root: canonical,
                manifest_path,
            })
        } else {
            let Some(package_root) = canonical.parent() else {
                return Err(PackageRegistryError::without_record(
                    canonical.to_string_lossy(),
                    PackageAction::Install,
                    PackageAdmissionReason::UnsafeLocalPath(
                        "manifest path has no package directory".to_string(),
                    ),
                    audit_reason,
                ));
            };
            Ok(Self {
                package_root: package_root.to_path_buf(),
                manifest_path: canonical,
            })
        }
    }

    fn read_manifest(&self, audit_reason: String) -> PackageRegistryResult<LocalPackageManifest> {
        let bytes = fs::read(&self.manifest_path).map_err(|error| {
            PackageRegistryError::without_record(
                self.manifest_path.to_string_lossy(),
                PackageAction::Install,
                PackageAdmissionReason::InvalidLocalManifest(error.to_string()),
                audit_reason.clone(),
            )
        })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            PackageRegistryError::without_record(
                self.manifest_path.to_string_lossy(),
                PackageAction::Install,
                PackageAdmissionReason::InvalidLocalManifest(error.to_string()),
                audit_reason,
            )
        })
    }

    fn validate_manifest_entrypoints(
        &self,
        manifest: &PackageManifest,
        audit_reason: String,
    ) -> PackageRegistryResult<()> {
        for entrypoint in &manifest.entrypoints {
            canonical_entrypoint_path(
                &self.package_root,
                &entrypoint.path,
                &manifest.name,
                PackageAction::Install,
                audit_reason.clone(),
            )?;
        }
        Ok(())
    }
}

fn canonical_package_root(
    path: &Path,
    package_name: &str,
    action: PackageAction,
    audit_reason: String,
) -> PackageRegistryResult<PathBuf> {
    path.canonicalize().map_err(|error| {
        PackageRegistryError::without_record(
            package_name,
            action,
            PackageAdmissionReason::UnsafeLocalPath(error.to_string()),
            audit_reason,
        )
    })
}

fn canonical_entrypoint_path(
    package_root: &Path,
    entrypoint: &str,
    package_name: &str,
    action: PackageAction,
    audit_reason: String,
) -> PackageRegistryResult<PathBuf> {
    let relative = Path::new(entrypoint);
    if entrypoint.is_empty()
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(PackageRegistryError::without_record(
            package_name,
            action,
            PackageAdmissionReason::UnsafeEntrypoint(entrypoint.to_string()),
            audit_reason,
        ));
    }

    let entrypoint_path = package_root
        .join(relative)
        .canonicalize()
        .map_err(|error| {
            PackageRegistryError::without_record(
                package_name,
                action,
                PackageAdmissionReason::UnsafeEntrypoint(error.to_string()),
                audit_reason.clone(),
            )
        })?;
    if !entrypoint_path.starts_with(package_root) {
        return Err(PackageRegistryError::without_record(
            package_name,
            action,
            PackageAdmissionReason::UnsafeEntrypoint(
                "entrypoint resolves outside package root".to_string(),
            ),
            audit_reason,
        ));
    }
    Ok(entrypoint_path)
}

fn validate_runnable_entrypoints(
    package_name: &str,
    entrypoints: &[PackageRunnableEntrypoint],
    action: PackageAction,
    audit_reason: String,
) -> PackageRegistryResult<()> {
    validate_runnable_entrypoint_contract(entrypoints).map_err(|reason| {
        PackageRegistryError::without_record(
            package_name,
            action,
            PackageAdmissionReason::UnsafeEntrypoint(reason),
            audit_reason,
        )
    })
}

fn validate_runnable_entrypoints_for_snapshot(
    package_name: &str,
    entrypoints: &[PackageRunnableEntrypoint],
) -> Result<(), PackageRegistrySnapshotError> {
    validate_runnable_entrypoint_contract(entrypoints).map_err(|reason| {
        PackageRegistrySnapshotError::RunnableEntrypoint {
            package_name: package_name.to_string(),
            reason,
        }
    })
}

fn validate_runnable_entrypoint_contract(
    entrypoints: &[PackageRunnableEntrypoint],
) -> Result<(), String> {
    let mut ids = BTreeSet::new();
    for entrypoint in entrypoints {
        if entrypoint.id.trim().is_empty() {
            return Err("runnable entrypoint id is empty".to_string());
        }
        if !ids.insert(entrypoint.id.as_str()) {
            return Err(format!(
                "duplicate runnable entrypoint id {}",
                entrypoint.id
            ));
        }
        validate_runnable_command(&entrypoint.command)?;
        if let PackageRunnableWorkingDirectory::Relative { path } = &entrypoint.working_directory {
            validate_relative_manifest_path(path, "working directory")?;
        }
        for requirement in &entrypoint.environment {
            if requirement.name.trim().is_empty() {
                return Err(format!(
                    "runnable entrypoint {} has empty environment requirement name",
                    entrypoint.id
                ));
            }
        }
    }
    Ok(())
}

fn validate_runnable_command(command: &str) -> Result<(), String> {
    if command.trim().is_empty() {
        return Err("runnable entrypoint command is empty".to_string());
    }
    validate_relative_manifest_path(command, "command")
}

fn validate_relative_manifest_path(value: &str, label: &str) -> Result<(), String> {
    let relative = Path::new(value);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(format!("runnable entrypoint {label} is unsafe: {value}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core::{
        ExtensionEntrypoint, ExtensionRuntime, HostProfileMetadata, HostProfilePolicySection,
        PackageConfigurationOption, PackageSource,
    };
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
    use crate::persistence::{FileHubStateStore, HubStateStore};

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
            dependencies: Vec::new(),
            features: Vec::new(),
            configuration: None,
            host_profile: None,
            surfaces: Vec::new(),
            runnable_entrypoints: Vec::new(),
            navigation: Vec::new(),
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
            dependencies: Vec::new(),
            features: Vec::new(),
            host_profile: Some(HostProfileMetadata {
                profile_id: "example-provider".to_string(),
                compatibility: ">=0.1.0".to_string(),
                precedence: 10,
                required_providers: Vec::new(),
                required_capabilities: capabilities,
                policy_sections: vec![HostProfilePolicySection::Providers],
            }),
            configuration: None,
            surfaces: Vec::new(),
            runnable_entrypoints: Vec::new(),
            navigation: Vec::new(),
        }
    }

    fn test_root(name: &str) -> PathBuf {
        let root = PathBuf::from("target")
            .join("botster-hub-package-tests")
            .join(name);
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create package test root");
        root.canonicalize().expect("canonical package test root")
    }

    fn write_manifest(package_root: &Path, manifest: &PackageManifest) -> PathBuf {
        let manifest_path = package_root.join(LOCAL_PACKAGE_MANIFEST_FILE);
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(manifest).expect("serialize local package manifest"),
        )
        .expect("write local package manifest");
        manifest_path
    }

    fn write_manifest_json(package_root: &Path, json: &str) -> PathBuf {
        let manifest_path = package_root.join(LOCAL_PACKAGE_MANIFEST_FILE);
        fs::write(&manifest_path, json).expect("write local package manifest json");
        manifest_path
    }

    fn local_manifest(name: &str, entrypoint: &str) -> PackageManifest {
        let mut manifest =
            plugin_manifest(name, vec![capability(CapabilitySurface::Surfaces, None)]);
        manifest.source = None;
        manifest.entrypoints = vec![ExtensionEntrypoint {
            runtime: ExtensionRuntime::Lua,
            path: entrypoint.to_string(),
            bootstrap: false,
        }];
        manifest
    }

    fn package_record(manifest: PackageManifest, state: PackageState) -> PackageRecord {
        let classification = PackageClassification::from_kind(&manifest.kind);
        PackageRecord {
            compatibility: PackageCompatibility::for_manifest(&manifest),
            manifest,
            state,
            classification,
            trust: PackageTrust::third_party(),
            provenance: provenance(),
            source_metadata: None,
            pin: None,
            update_policy: PackageUpdatePolicy::Manual,
            admitted_capabilities: Vec::new(),
            runnable_entrypoints: Vec::new(),
            session_templates: Vec::new(),
            configuration: PackageConfigurationState::default(),
            installed_at: None,
            updated_at: None,
            last_audit_reason: "test fixture".to_string(),
            admitted_host_profile: None,
        }
    }

    fn package_configuration_manifest() -> PackageManifest {
        let mut manifest = plugin_manifest(
            "configuration.plugin",
            vec![capability(CapabilitySurface::Surfaces, None)],
        );
        manifest.configuration = Some(PackageConfigurationSchema {
            groups: Vec::new(),
            fields: vec![
                PackageConfigurationField {
                    key: "endpoint".to_string(),
                    field_type: PackageConfigurationFieldType::Url,
                    label: "Endpoint".to_string(),
                    description: None,
                    required: true,
                    default: None,
                    validation: None,
                    group: None,
                    order: None,
                    options: Vec::new(),
                },
                PackageConfigurationField {
                    key: "mode".to_string(),
                    field_type: PackageConfigurationFieldType::Select,
                    label: "Mode".to_string(),
                    description: None,
                    required: false,
                    default: Some(PackageConfigurationValue::Select {
                        value: "read".to_string(),
                    }),
                    validation: None,
                    group: None,
                    order: None,
                    options: vec![PackageConfigurationOption {
                        value: "read".to_string(),
                        label: "Read".to_string(),
                        description: None,
                    }],
                },
                PackageConfigurationField {
                    key: "api_token".to_string(),
                    field_type: PackageConfigurationFieldType::Secret,
                    label: "API token".to_string(),
                    description: None,
                    required: true,
                    default: Some(PackageConfigurationValue::Secret {
                        state: PackageConfigurationSecretValue::Unset,
                    }),
                    validation: None,
                    group: None,
                    order: None,
                    options: Vec::new(),
                },
            ],
        });
        manifest
    }

    #[test]
    fn package_configuration_validation_defaults_and_secret_redaction() {
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));
        registry
            .install(
                package_configuration_manifest(),
                provenance(),
                "install configurable package",
            )
            .expect("install configurable package");

        let view = registry
            .set_configuration(
                "configuration.plugin",
                BTreeMap::from([
                    (
                        "endpoint".to_string(),
                        PackageConfigurationValue::Url {
                            value: "https://example.invalid/hook".to_string(),
                        },
                    ),
                    (
                        "api_token".to_string(),
                        PackageConfigurationValue::Secret {
                            state: PackageConfigurationSecretValue::WriteOnly,
                        },
                    ),
                ]),
                "configure package",
            )
            .expect("set package configuration");

        assert_eq!(view.missing_required, Vec::<String>::new());
        assert!(matches!(
            view.effective_values.get("api_token"),
            Some(PackageConfigurationValue::Secret {
                state: PackageConfigurationSecretValue::Redacted
            })
        ));
        assert!(matches!(
            view.effective_values.get("mode"),
            Some(PackageConfigurationValue::Select { value }) if value == "read"
        ));

        let snapshot_json =
            serde_json::to_string(&registry.snapshot()).expect("serialize registry snapshot");
        assert!(snapshot_json.contains("\"state\":\"redacted\""));
        assert!(!snapshot_json.contains("write_only"));
        assert!(!snapshot_json.contains("super-secret-token"));
    }

    #[test]
    fn package_configuration_rejects_unknown_field_and_type_mismatch() {
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));
        registry
            .install(
                package_configuration_manifest(),
                provenance(),
                "install configurable package",
            )
            .expect("install configurable package");

        let error = registry
            .set_configuration(
                "configuration.plugin",
                BTreeMap::from([(
                    "missing".to_string(),
                    PackageConfigurationValue::String {
                        value: "value".to_string(),
                    },
                )]),
                "configure package",
            )
            .expect_err("unknown field should fail");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::InvalidConfiguration(ref diagnostics)
                if diagnostics.iter().any(|diagnostic| diagnostic.kind == "unknown_field")
        ));

        let error = registry
            .set_configuration(
                "configuration.plugin",
                BTreeMap::from([(
                    "endpoint".to_string(),
                    PackageConfigurationValue::String {
                        value: "https://example.invalid".to_string(),
                    },
                )]),
                "configure package",
            )
            .expect_err("type mismatch should fail");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::InvalidConfiguration(ref diagnostics)
                if diagnostics.iter().any(|diagnostic| diagnostic.kind == "value_type_mismatch")
        ));
    }

    #[test]
    fn package_configuration_rejects_select_default_outside_options() {
        let mut manifest = package_configuration_manifest();
        let schema = manifest
            .configuration
            .as_mut()
            .expect("configuration schema");
        let mode = schema
            .fields
            .iter_mut()
            .find(|field| field.key == "mode")
            .expect("mode field");
        mode.default = Some(PackageConfigurationValue::Select {
            value: "write".to_string(),
        });

        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));
        registry
            .install(manifest, provenance(), "install configurable package")
            .expect("install configurable package");

        let view = registry
            .package("configuration.plugin")
            .expect("configuration package")
            .configuration_view();
        assert!(matches!(
            view.diagnostics.as_slice(),
            [PackageConfigurationDiagnostic { kind, field: Some(field), .. }]
                if kind == "default_select_option_unknown" && field == "mode"
        ));

        let error = registry
            .enable("configuration.plugin", "enable invalid default package")
            .expect_err("invalid select default should block enable");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::InvalidConfiguration(ref diagnostics)
                if diagnostics.iter().any(|diagnostic| diagnostic.kind == "default_select_option_unknown")
        ));
    }

    #[test]
    fn package_configuration_missing_required_denies_enable_before_load() {
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));
        registry
            .install(
                package_configuration_manifest(),
                provenance(),
                "install configurable package",
            )
            .expect("install configurable package");

        let error = registry
            .enable("configuration.plugin", "enable without configuration")
            .expect_err("missing required config should fail");

        assert!(matches!(
            error.reason,
            PackageAdmissionReason::MissingRequiredConfiguration(ref fields)
                if fields == &vec!["endpoint".to_string(), "api_token".to_string()]
        ));
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
        assert_eq!(record.trust, PackageTrust::third_party());
        assert_eq!(
            record.compatibility.result,
            PackageCompatibilityResult::Compatible
        );
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
                    branch: None,
                    tag: None,
                    rev: None,
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
    fn first_party_git_package_persists_trust_pin_and_compatibility_contract() {
        let capability = capability(CapabilitySurface::Surfaces, None);
        let mut registry = PackageRegistry::new(grants(vec![capability.clone()]));
        registry
            .install_with_trust(
                plugin_manifest("first-party.plugin", vec![capability]),
                PackageProvenance {
                    source: "git:https://example.invalid/botster/first-party-plugin.git"
                        .to_string(),
                    checksum: Some("sha256:first-party".to_string()),
                },
                PackageTrust::first_party(),
                "install first-party git package",
            )
            .expect("install first-party package");
        registry
            .pin(
                "first-party.plugin",
                PackagePin {
                    revision: "8f2f4ac".to_string(),
                    branch: None,
                    tag: None,
                    rev: Some("8f2f4ac".to_string()),
                    checksum: Some("sha256:first-party-pin".to_string()),
                    update_policy: PackageUpdatePolicy::Manual,
                },
                "pin first-party package",
            )
            .expect("pin first-party package");
        registry
            .enable("first-party.plugin", "enable first-party package")
            .expect("enable first-party package");

        let json = serde_json::to_string_pretty(&registry.snapshot()).expect("serialize snapshot");
        assert!(json.contains("\"first_party\": true"));
        assert!(json.contains("\"classification\": \"first_party\""));
        assert!(json.contains("\"revision\": \"8f2f4ac\""));
        assert!(json.contains("\"result\": \"compatible\""));
        assert!(!json.contains("/Users/"));
        assert!(!json.contains("jason"));
        assert!(!json.contains('@'));
    }

    #[test]
    fn incompatible_botster_requirements_are_rejected_at_install() {
        let mut registry = PackageRegistry::new(CapabilitySet::new());
        let mut incompatible = plugin_manifest("future.plugin", Vec::new());
        incompatible.botster = "999.0.0".to_string();

        let error = registry
            .install(incompatible, provenance(), "install incompatible package")
            .expect_err("future exact requirement should fail");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::BotsterCompatibility(_)
        ));

        let mut invalid = plugin_manifest("invalid.plugin", Vec::new());
        invalid.botster = "^1.0".to_string();
        let error = registry
            .install(invalid, provenance(), "install invalid package")
            .expect_err("unsupported semver range should fail");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::BotsterCompatibility(_)
        ));
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
    fn default_package_policy_admits_botster_workspaces_plugin_db_namespace() {
        let requested = capability(CapabilitySurface::PluginDb, Some("botster-workspaces"));
        let mut policy = default_package_policy();

        policy
            .install(
                plugin_manifest("botster-workspaces", vec![requested.clone()]),
                provenance(),
                "install botster workspaces",
            )
            .expect("install botster-workspaces package");

        let decision = policy
            .enable("botster-workspaces", "enable botster workspaces")
            .expect("enable botster-workspaces package");

        assert_eq!(decision.state, PackageState::Enabled);
        assert_eq!(
            policy
                .registry()
                .package("botster-workspaces")
                .expect("botster-workspaces record")
                .admitted_capabilities,
            vec![requested]
        );
    }

    #[test]
    fn default_package_policy_denies_botster_workspaces_mismatched_plugin_db_namespace() {
        let requested = capability(CapabilitySurface::PluginDb, Some("other-plugin"));
        let mut policy = default_package_policy();

        policy
            .install(
                plugin_manifest("botster-workspaces", vec![requested.clone()]),
                provenance(),
                "install mismatched botster workspaces",
            )
            .expect("install mismatched botster-workspaces package");

        let error = policy
            .enable("botster-workspaces", "enable mismatched botster workspaces")
            .expect_err("mismatched plugin_db namespace should deny");

        assert_eq!(error.package_name, "botster-workspaces");
        assert_eq!(error.action, PackageAction::Enable);
        assert_eq!(
            error.reason,
            PackageAdmissionReason::UngrantedCapability(requested)
        );
        assert_eq!(error.state, Some(PackageState::Installed));
    }

    #[test]
    fn explicit_local_manifest_installs_path_source_and_local_provenance() {
        let root = test_root("explicit-local-manifest");
        fs::write(root.join("plugin.lua"), "-- synthetic plugin").expect("write plugin");
        let manifest_path = write_manifest(&root, &local_manifest("local.plugin", "plugin.lua"));
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));

        let record = registry
            .install_local_path(&manifest_path, "install local manifest")
            .expect("install local manifest");

        assert_eq!(record.state, PackageState::Installed);
        assert_eq!(
            record.manifest.source,
            Some(PackageSource::Path {
                path: root.to_string_lossy().into_owned()
            })
        );
        assert_eq!(
            record.provenance.source,
            format!("local:{}", root.to_string_lossy())
        );
        assert_eq!(record.trust, PackageTrust::local_development());
    }

    #[test]
    fn local_package_directory_uses_conventional_manifest_filename() {
        let root = test_root("directory-local-manifest");
        fs::write(root.join("plugin.lua"), "-- synthetic plugin").expect("write plugin");
        write_manifest(&root, &local_manifest("directory.plugin", "plugin.lua"));
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));

        registry
            .install_local_path(&root, "install package directory")
            .expect("install package directory");

        assert!(registry.package("directory.plugin").is_some());
    }

    #[test]
    fn local_registry_lists_previews_and_installs_path_entry_without_path_leak_in_available_row() {
        let root = test_root("local-registry-path-entry");
        let package_root = root.join("packages").join("local");
        fs::create_dir_all(&package_root).expect("create package root");
        fs::write(package_root.join("plugin.lua"), "return {}\n").expect("write plugin");
        write_manifest(
            &package_root,
            &local_manifest("catalog.local", "plugin.lua"),
        );
        fs::write(
            root.join(LOCAL_PACKAGE_REGISTRY_FILE),
            r#"{
  "source": { "id": "first-party-fixture", "kind": "local_path", "label": "Fixture" },
  "entries": [
    {
      "id": "catalog-local",
      "first_party": true,
      "source": { "type": "local_path", "path": "packages/local" }
    }
  ]
}
"#,
        )
        .expect("write registry");

        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));
        let available = registry
            .available_packages(&root)
            .expect("list available packages");
        assert_eq!(available.len(), 1);
        assert_eq!(available[0].entry_id, "catalog-local");
        assert_eq!(available[0].package_name, "catalog.local");
        assert_eq!(available[0].source_label, "local:catalog-local");
        assert!(
            !available[0]
                .source_label
                .contains(root.to_str().expect("utf8 root"))
        );
        assert_eq!(available[0].state, AvailablePackageState::Available);

        let plan = registry
            .preview_registry_install(&root, "catalog-local")
            .expect("preview registry install");
        assert!(!plan.mutates_registry);
        assert!(!plan.starts_entrypoints);
        assert!(registry.package("catalog.local").is_none());

        let record = registry
            .install_registry_entry(&root, "catalog-local", "install fixture entry")
            .expect("install registry entry");
        assert_eq!(record.state, PackageState::Installed);
        assert_eq!(record.trust, PackageTrust::first_party());
        assert_eq!(
            record
                .source_metadata
                .as_ref()
                .expect("source metadata")
                .entry_id,
            "catalog-local"
        );
        assert_eq!(
            registry
                .inspect_available_package(&root, "catalog-local")
                .expect("inspect after install")
                .state,
            AvailablePackageState::Installed
        );
    }

    #[test]
    fn git_shaped_registry_entry_persists_pin_and_source_metadata_without_network() {
        let root = test_root("git-shaped-registry-entry");
        fs::write(
            root.join(LOCAL_PACKAGE_REGISTRY_FILE),
            r#"{
  "source": { "id": "static-first-party", "kind": "static_first_party", "label": "Fixture" },
  "entries": [
    {
      "id": "catalog-git",
      "first_party": true,
      "source": {
        "type": "git",
        "repo": "https://example.invalid/botster/catalog-git.git",
        "branch": "main",
        "tag": "v1.2.3",
        "rev": "abc123"
      },
      "manifest": {
        "name": "catalog.git",
        "version": "1.2.3",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "capabilities": [
          { "surface": "surfaces" }
        ],
        "entrypoints": [
          { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ]
      }
    }
  ]
}
"#,
        )
        .expect("write registry");

        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));
        let available = registry
            .inspect_available_package(&root, "catalog-git")
            .expect("inspect git registry entry");
        assert_eq!(available.source_kind, PackageRegistryEntrySourceKind::Git);
        assert_eq!(
            available.pin.as_ref().expect("pin").rev.as_deref(),
            Some("abc123")
        );

        let plan = registry
            .preview_registry_install(&root, "catalog-git")
            .expect("preview git install");
        assert!(
            plan.effects
                .iter()
                .any(|effect| effect.kind == "no_network_fetch")
        );
        let record = registry
            .install_registry_entry(&root, "catalog-git", "install git-shaped entry")
            .expect("install git-shaped entry");
        assert_eq!(record.state, PackageState::Installed);
        assert_eq!(record.pin.as_ref().expect("record pin").revision, "abc123");
        assert_eq!(
            record
                .source_metadata
                .as_ref()
                .expect("source metadata")
                .git_repo
                .as_deref(),
            Some("https://example.invalid/botster/catalog-git.git")
        );

        let restored = PackageRegistry::from_snapshot(registry.snapshot()).expect("restore");
        let restored_record = restored.package("catalog.git").expect("restored package");
        assert_eq!(
            restored_record
                .source_metadata
                .as_ref()
                .expect("restored source metadata")
                .entry_id,
            "catalog-git"
        );
        assert_eq!(
            restored_record
                .pin
                .as_ref()
                .expect("restored pin")
                .branch
                .as_deref(),
            Some("main")
        );
    }

    #[test]
    fn local_manifest_installs_runnable_entrypoint_contract() {
        let root = test_root("runnable-entrypoint-contract");
        fs::create_dir_all(root.join("web")).expect("create web directory");
        fs::write(root.join("plugin.lua"), "-- synthetic plugin").expect("write plugin");
        fs::write(root.join("web").join("dev-server"), "#!/bin/sh\n")
            .expect("write runnable command");
        write_manifest_json(
            &root,
            r#"{
  "name": "runnable.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [{ "surface": "surfaces" }],
  "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
  "runnable_entrypoints": [{
    "id": "web",
    "kind": "web_app",
    "command": "web/dev-server",
    "args": ["--host", "127.0.0.1"],
    "working_directory": { "policy": "relative", "path": "web" },
    "environment": [{
      "name": "BOTSTER_WEB_PORT",
      "required": false,
      "default": "5173",
      "description": "Local web client port"
    }],
    "launch_mode": "background",
    "capabilities": [{ "surface": "network", "scope": "localhost" }],
    "may_supervise": true
  }]
}
"#,
        );
        let mut registry = PackageRegistry::new(grants(vec![
            capability(CapabilitySurface::Surfaces, None),
            capability(CapabilitySurface::Network, Some("localhost")),
        ]));

        let record = registry
            .install_local_path(&root, "install runnable package")
            .expect("install runnable package");

        assert_eq!(record.runnable_entrypoints.len(), 1);
        let entrypoint = &record.runnable_entrypoints[0];
        assert_eq!(entrypoint.id, "web");
        assert_eq!(entrypoint.kind, RunnableEntrypointKind::WebApp);
        assert_eq!(
            entrypoint.launch_mode,
            RunnableEntrypointLaunchMode::Background
        );
        assert_eq!(entrypoint.command, "web/dev-server");
        assert_eq!(entrypoint.args, ["--host", "127.0.0.1"]);
        assert_eq!(
            entrypoint.working_directory,
            PackageRunnableWorkingDirectory::Relative {
                path: "web".to_string()
            }
        );
        assert_eq!(entrypoint.environment[0].name, "BOTSTER_WEB_PORT");
        assert_eq!(entrypoint.environment[0].default.as_deref(), Some("5173"));
        assert_eq!(
            entrypoint.capabilities[0].surface,
            CapabilitySurface::Network
        );
        assert!(entrypoint.may_supervise);
        assert_eq!(
            entrypoint.process.state,
            PackageRunnableProcessState::NotStarted
        );

        let json = serde_json::to_string_pretty(&registry.snapshot()).expect("serialize snapshot");
        assert!(json.contains("\"runnable_entrypoints\""));
        assert!(json.contains("\"may_supervise\": true"));
        assert!(json.contains("\"state\": \"not_started\""));

        let restored =
            PackageRegistry::from_snapshot(registry.snapshot()).expect("restore snapshot");
        assert_eq!(
            restored
                .package("runnable.plugin")
                .expect("restored record")
                .runnable_entrypoints[0]
                .args,
            ["--host", "127.0.0.1"]
        );
    }

    #[test]
    fn invalid_runnable_entrypoint_contracts_are_rejected() {
        let root = test_root("invalid-runnable-entrypoints");
        fs::write(root.join("plugin.lua"), "-- synthetic plugin").expect("write plugin");
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));

        for (name, entrypoints) in [
            (
                "duplicate",
                r#"[
                  { "id": "web", "kind": "web_app", "command": "bin/web", "launch_mode": "background" },
                  { "id": "web", "kind": "terminal_app", "command": "bin/client", "launch_mode": "foreground_stdio" }
                ]"#,
            ),
            (
                "missing-command",
                r#"[{ "id": "web", "kind": "web_app", "command": "", "launch_mode": "background" }]"#,
            ),
            (
                "absolute-command",
                r#"[{ "id": "web", "kind": "web_app", "command": "/bin/web", "launch_mode": "background" }]"#,
            ),
            (
                "traversing-command",
                r#"[{ "id": "web", "kind": "web_app", "command": "../bin/web", "launch_mode": "background" }]"#,
            ),
            (
                "traversing-working-directory",
                r#"[{
                  "id": "web",
                  "kind": "web_app",
                  "command": "bin/web",
                  "launch_mode": "background",
                  "working_directory": { "policy": "relative", "path": "../web" }
                }]"#,
            ),
        ] {
            write_manifest_json(
                &root,
                &format!(
                    r#"{{
  "name": "{name}.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": {{ "type": "path", "path": "." }},
  "capabilities": [{{ "surface": "surfaces" }}],
  "entrypoints": [{{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }}],
  "runnable_entrypoints": {entrypoints}
}}"#
                ),
            );
            let error = registry
                .install_local_path(&root, format!("install {name}"))
                .expect_err("invalid runnable entrypoint should fail");
            assert!(matches!(
                error.reason,
                PackageAdmissionReason::UnsafeEntrypoint(_)
            ));
        }

        write_manifest_json(
            &root,
            r#"{
  "name": "unsupported-kind.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [{ "surface": "surfaces" }],
  "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
  "runnable_entrypoints": [{ "id": "bad", "kind": "sidecar", "command": "bin/sidecar" }]
}
"#,
        );
        let error = registry
            .install_local_path(&root, "install unsupported kind")
            .expect_err("unsupported kind should fail");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::InvalidLocalManifest(_)
        ));
    }

    #[test]
    fn durable_package_state_round_trips_local_metadata_and_provider_admission() {
        let root = test_root("durable-local-state");
        let data_root = root.join("data");
        let plugin_root = root.join("package");
        fs::create_dir_all(&plugin_root).expect("create local package");
        fs::write(plugin_root.join("plugin.lua"), "-- synthetic plugin").expect("write plugin");
        write_manifest(
            &plugin_root,
            &local_manifest("durable.plugin", "plugin.lua"),
        );
        let provider_capability = capability(CapabilitySurface::ClientAdmission, None);
        let mut registry = PackageRegistry::new(grants(vec![
            capability(CapabilitySurface::Surfaces, None),
            provider_capability.clone(),
        ]));
        registry
            .install_local_path(&plugin_root, "install local package")
            .expect("install local package");
        registry
            .pin(
                "durable.plugin",
                PackagePin {
                    revision: "local-dev".to_string(),
                    branch: None,
                    tag: None,
                    rev: None,
                    checksum: Some("sha256:local-dev".to_string()),
                    update_policy: PackageUpdatePolicy::TrackSource,
                },
                "pin local package",
            )
            .expect("pin local package");
        registry
            .enable("durable.plugin", "enable local package")
            .expect("enable local package");
        registry
            .install(
                provider_manifest("durable.provider", vec![provider_capability]),
                provenance(),
                "install provider",
            )
            .expect("install provider");
        registry
            .enable("durable.provider", "enable provider")
            .expect("enable provider");
        let live_provider_profile = registry
            .package("durable.provider")
            .expect("provider record before save")
            .admitted_host_profile
            .as_ref()
            .expect("live provider admission")
            .metadata
            .profile_id
            .clone();

        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(data_root),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
        .expect("explicit state config should build");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let state = store
            .update(&config, |state| {
                state.package_registry = registry.snapshot();
            })
            .expect("save package state through hub state");
        let loaded = PackageRegistry::from_snapshot(state.package_registry)
            .expect("load package state from hub state");

        let local_record = loaded.package("durable.plugin").expect("local record");
        assert_eq!(local_record.state, PackageState::Enabled);
        assert_eq!(
            local_record.pin.as_ref().expect("pin").update_policy,
            PackageUpdatePolicy::TrackSource
        );
        assert_eq!(
            local_record.provenance.source,
            format!(
                "local:{}",
                plugin_root.canonicalize().unwrap().to_string_lossy()
            )
        );
        assert!(
            loaded
                .package("durable.provider")
                .expect("provider record")
                .admitted_host_profile
                .is_some()
        );
        assert_eq!(
            loaded
                .package("durable.provider")
                .expect("provider record")
                .admitted_host_profile
                .as_ref()
                .expect("reloaded provider admission")
                .metadata
                .profile_id,
            live_provider_profile
        );
    }

    #[test]
    fn unsafe_local_entrypoints_are_rejected_before_install() {
        let root = test_root("unsafe-local-entrypoints");
        fs::write(root.join("plugin.lua"), "-- synthetic plugin").expect("write plugin");
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));

        write_manifest(&root, &local_manifest("absolute.plugin", "/tmp/plugin.lua"));
        let error = registry
            .install_local_path(&root, "install absolute entrypoint")
            .expect_err("absolute entrypoint should fail");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::UnsafeEntrypoint(_)
        ));

        write_manifest(&root, &local_manifest("traverse.plugin", "../plugin.lua"));
        let error = registry
            .install_local_path(&root, "install traversing entrypoint")
            .expect_err("traversing entrypoint should fail");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::UnsafeEntrypoint(_)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_escaped_entrypoints_are_rejected_at_install_and_prepare() {
        use std::os::unix::fs::symlink;

        let root = test_root("symlink-escaped-entrypoint");
        let outside = test_root("symlink-escaped-entrypoint-outside");
        fs::write(outside.join("outside.lua"), "-- outside").expect("write outside plugin");
        symlink(outside.join("outside.lua"), root.join("link.lua")).expect("create symlink");
        write_manifest(&root, &local_manifest("symlink.plugin", "link.lua"));
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));

        let error = registry
            .install_local_path(&root, "install symlink entrypoint")
            .expect_err("symlink escape should fail");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::UnsafeEntrypoint(_)
        ));

        fs::remove_file(root.join("link.lua")).expect("remove escaping symlink");
        fs::write(root.join("link.lua"), "-- safe during install").expect("write safe file");
        registry
            .install_local_path(&root, "install safe local package")
            .expect("install safe package");
        registry
            .enable("symlink.plugin", "enable safe local package")
            .expect("enable safe package");
        fs::remove_file(root.join("link.lua")).expect("remove safe file");
        symlink(outside.join("outside.lua"), root.join("link.lua"))
            .expect("create escaping symlink after install");

        let error = registry
            .prepare_local_package("symlink.plugin", "prepare local package")
            .expect_err("prepare should revalidate entrypoint escape");
        assert!(matches!(
            error.reason,
            PackageAdmissionReason::UnsafeEntrypoint(_)
        ));
    }

    #[test]
    fn prepare_enabled_local_packages_returns_only_enabled_local_records() {
        let root = test_root("prepare-enabled-local-packages");
        let alpha_root = root.join("alpha");
        let beta_root = root.join("beta");
        let disabled_root = root.join("disabled");
        for package_root in [&alpha_root, &beta_root, &disabled_root] {
            fs::create_dir_all(package_root).expect("create package root");
            fs::write(package_root.join("plugin.lua"), "-- synthetic plugin")
                .expect("write plugin");
        }
        write_manifest(&alpha_root, &local_manifest("alpha.local", "plugin.lua"));
        write_manifest(&beta_root, &local_manifest("beta.local", "plugin.lua"));
        write_manifest(
            &disabled_root,
            &local_manifest("disabled.local", "plugin.lua"),
        );
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));
        registry
            .install_local_path(&alpha_root, "install alpha")
            .expect("install alpha");
        registry
            .install_local_path(&beta_root, "install beta")
            .expect("install beta");
        registry
            .install_local_path(&disabled_root, "install disabled")
            .expect("install disabled");
        registry
            .install(
                plugin_manifest(
                    "nonlocal.plugin",
                    vec![capability(CapabilitySurface::Surfaces, None)],
                ),
                provenance(),
                "install nonlocal",
            )
            .expect("install nonlocal");
        for package_name in ["alpha.local", "beta.local", "nonlocal.plugin"] {
            registry
                .enable(package_name, "enable package")
                .expect("enable package");
        }

        let prepared = registry
            .prepare_enabled_local_packages("prepare local packages")
            .expect("prepare enabled local packages");
        let package_names: Vec<_> = prepared
            .iter()
            .map(|package| package.package_name.as_str())
            .collect();

        assert_eq!(package_names, vec!["alpha.local", "beta.local"]);
        let alpha_entrypoint = alpha_root
            .join("plugin.lua")
            .canonicalize()
            .expect("canonical alpha entrypoint");
        let beta_entrypoint = beta_root
            .join("plugin.lua")
            .canonicalize()
            .expect("canonical beta entrypoint");
        assert_eq!(
            prepared[0].selected_entrypoint_path.as_deref(),
            Some(alpha_entrypoint.as_path())
        );
        assert_eq!(
            prepared[1].selected_entrypoint_path.as_deref(),
            Some(beta_entrypoint.as_path())
        );
    }

    #[test]
    fn prepare_local_package_accepts_client_app_only_package() {
        let root = test_root("prepare-runnable-only-local-package");
        fs::create_dir_all(&root).expect("create package root");
        write_manifest_json(
            &root,
            r#"{
  "name": "client.app",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "capabilities": [{ "surface": "surfaces" }],
  "entrypoints": [],
  "runnable_entrypoints": [{
    "id": "client",
    "kind": "terminal_app",
    "command": "bin/client",
    "args": ["--headless"],
    "working_directory": { "policy": "package_root" },
    "launch_mode": "foreground_stdio"
  }]
}"#,
        );
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));
        registry
            .install_local_path(&root, "install runnable-only local package")
            .expect("install runnable-only package");
        registry
            .enable("client.app", "enable runnable-only local package")
            .expect("enable runnable-only package");

        let prepared = registry
            .prepare_local_package("client.app", "prepare runnable-only package")
            .expect("prepare runnable-only package");

        assert_eq!(prepared.package_name, "client.app");
        assert!(prepared.entrypoints.is_empty());
        assert!(prepared.selected_entrypoint.is_none());
        assert!(prepared.selected_entrypoint_path.is_none());
        assert!(prepared.selected_lua_entrypoint().is_none());
    }

    #[test]
    fn prepare_local_package_rejects_package_without_entrypoints_or_runnables() {
        let root = test_root("prepare-empty-local-package");
        fs::create_dir_all(&root).expect("create package root");
        write_manifest_json(
            &root,
            r#"{
  "name": "empty.local",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "capabilities": [{ "surface": "surfaces" }],
  "entrypoints": [],
  "runnable_entrypoints": []
}"#,
        );
        let mut registry =
            PackageRegistry::new(grants(vec![capability(CapabilitySurface::Surfaces, None)]));
        registry
            .install_local_path(&root, "install empty local package")
            .expect("install empty package");
        registry
            .enable("empty.local", "enable empty local package")
            .expect("enable empty package");

        let error = registry
            .prepare_local_package("empty.local", "prepare empty package")
            .expect_err("empty package should still fail prepare");

        assert!(matches!(
            error.reason,
            PackageAdmissionReason::UnsafeEntrypoint(reason)
                if reason == "local package has no entrypoints"
        ));
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

    #[test]
    fn snapshot_round_trip_rehydrates_enabled_provider_admission() {
        let capability = capability(CapabilitySurface::ClientAdmission, None);
        let mut registry = PackageRegistry::new(grants(vec![capability.clone()]));
        registry
            .install(
                provider_manifest("admission.provider", vec![capability]),
                provenance(),
                "install provider",
            )
            .expect("install provider");
        registry
            .enable("admission.provider", "enable provider")
            .expect("enable provider");

        let snapshot = registry.snapshot();
        let restored = PackageRegistry::from_snapshot(snapshot).expect("restore snapshot");
        let record = restored
            .package("admission.provider")
            .expect("restored provider");

        assert_eq!(record.state, PackageState::Enabled);
        assert_eq!(record.admitted_capabilities, record.manifest.capabilities);
        assert_eq!(
            record
                .admitted_host_profile
                .as_ref()
                .expect("re-admitted provider")
                .metadata
                .profile_id,
            "example-provider"
        );
    }

    #[test]
    fn from_snapshot_rejects_record_with_now_incompatible_botster_requirement() {
        let mut manifest = plugin_manifest("future.plugin", Vec::new());
        manifest.botster = "999.0.0".to_string();
        let snapshot = PackageRegistrySnapshot {
            granted_capabilities: Vec::new(),
            governed_surfaces: host_profile().capability_surfaces().to_vec(),
            records: vec![package_record(manifest, PackageState::Installed)],
        };

        let error =
            PackageRegistry::from_snapshot(snapshot).expect_err("incompatible package should fail");

        assert!(matches!(
            error,
            PackageRegistrySnapshotError::BotsterCompatibility {
                package_name,
                diagnostics
            } if package_name == "future.plugin"
                && diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.contains("not satisfied"))
        ));
    }

    #[test]
    fn from_snapshot_rejects_enabled_record_with_ungranted_capability() {
        let requested = capability(CapabilitySurface::Network, Some("websocket"));
        let snapshot = PackageRegistrySnapshot {
            granted_capabilities: vec![capability(CapabilitySurface::Network, Some("http"))],
            governed_surfaces: host_profile().capability_surfaces().to_vec(),
            records: vec![package_record(
                plugin_manifest("ungranted.plugin", vec![requested.clone()]),
                PackageState::Enabled,
            )],
        };

        let error =
            PackageRegistry::from_snapshot(snapshot).expect_err("ungranted capability should fail");

        assert_eq!(
            error,
            PackageRegistrySnapshotError::CapabilityAdmission {
                package_name: "ungranted.plugin".to_string(),
                reason: PackageAdmissionReason::UngrantedCapability(requested),
            }
        );
    }

    #[test]
    fn from_snapshot_rejects_enabled_record_with_ungoverned_capability_surface() {
        let requested = capability(CapabilitySurface::Timers, Some("callbacks"));
        let snapshot = PackageRegistrySnapshot {
            granted_capabilities: vec![requested.clone()],
            governed_surfaces: vec![CapabilitySurface::Surfaces],
            records: vec![package_record(
                plugin_manifest("ungoverned.plugin", vec![requested]),
                PackageState::Enabled,
            )],
        };

        let error = PackageRegistry::from_snapshot(snapshot)
            .expect_err("ungoverned capability surface should fail");

        assert_eq!(
            error,
            PackageRegistrySnapshotError::CapabilityAdmission {
                package_name: "ungoverned.plugin".to_string(),
                reason: PackageAdmissionReason::UngovernedCapabilitySurface(
                    CapabilitySurface::Timers
                ),
            }
        );
    }

    #[test]
    fn from_snapshot_rejects_duplicate_package_records() {
        let manifest = plugin_manifest("duplicate.plugin", Vec::new());
        let record = package_record(manifest, PackageState::Installed);
        let snapshot = PackageRegistrySnapshot {
            granted_capabilities: Vec::new(),
            governed_surfaces: host_profile().capability_surfaces().to_vec(),
            records: vec![record.clone(), record],
        };

        let error =
            PackageRegistry::from_snapshot(snapshot).expect_err("duplicate package should fail");

        assert_eq!(
            error,
            PackageRegistrySnapshotError::DuplicatePackage("duplicate.plugin".to_string())
        );
    }

    #[test]
    fn from_snapshot_rejects_enabled_provider_that_no_longer_admits() {
        let mut manifest = provider_manifest(
            "broken.provider",
            vec![capability(CapabilitySurface::ClientAdmission, None)],
        );
        manifest.entrypoints.clear();
        let snapshot = PackageRegistrySnapshot {
            granted_capabilities: Vec::new(),
            governed_surfaces: host_profile().capability_surfaces().to_vec(),
            records: vec![package_record(manifest, PackageState::Enabled)],
        };

        let error = PackageRegistry::from_snapshot(snapshot).expect_err("bad provider should fail");

        assert_eq!(
            error,
            PackageRegistrySnapshotError::HostProfileAdmission {
                package_name: "broken.provider".to_string(),
                error: HostProfileAdmissionError::MissingBootstrapEntrypoint,
            }
        );
    }
}
