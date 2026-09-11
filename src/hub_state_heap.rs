//! G2 Host-state clone-heap walk.
//!
//! Counts **new** heap for one `HubState::clone`. Not a G1 budget and not
//! wired into `prepare_shared`.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::path::PathBuf;

use botster_core::{Capability, CapabilitySurface};
use serde_json::Value;

use crate::config::{
    CoreEngineOptions, CoreQueueCapacity, HostIdentity, LocalSocketBinding, SessionDefaults,
    SessionIoCoalescingOptions, TcpBinding, TransportBindings,
};
use crate::credentials::{CredentialKeyPurpose, CredentialProviderKind};
use crate::packages::{
    HubEmittedEvent, HubPackageEvents, HubPackageManifest, PackageClassification,
    PackageCompatibility, PackageConfigurationState, PackagePin, PackageProvenance, PackageRecord,
    PackageRegistrySnapshot, PackageRunnableEntrypoint, PackageSourceMetadata, PackageState,
    PackageTrust, PackageUpdatePolicy,
};
use crate::persistence::{
    BootstrapGrantRecord, CapabilityGrantRecord, CredentialKeyReference, DeviceSessionTypeSource,
    HubAuditEntry, HubState, LocalRuntimeSettings, PackageAdmissionDecision, SchemaMetadata,
    TrustedBrowserIdentity,
};
use crate::session_types::PackageSessionType;
use crate::spawn_targets::SpawnTarget;
use crate::worktrees::{Worktree, WorktreeGitMetadata};

const BTREE_CAPACITY: usize = 11;
const BTREE_EDGES: usize = 12;
const BTREE_MIN_OCCUPANCY: usize = 5;

#[repr(C)]
struct BTreeLeafMirror<K, V> {
    _parent: *const u8,
    _parent_idx: u16,
    _len: u16,
    _keys: [K; BTREE_CAPACITY],
    _vals: [V; BTREE_CAPACITY],
}

#[repr(C)]
struct BTreeInternalMirror<K, V> {
    _leaf: BTreeLeafMirror<K, V>,
    _edges: [*const u8; BTREE_EDGES],
}

fn btree_internal_size<K, V>() -> usize {
    size_of::<BTreeInternalMirror<K, V>>()
}

fn btree_nodes<K, V>(len: usize) -> usize {
    if len == 0 {
        0
    } else {
        let internal = btree_internal_size::<K, V>();
        internal.saturating_add(len.saturating_mul(internal / BTREE_MIN_OCCUPANCY))
    }
}

/// New-heap walk of one `HubState` clone, excluding the retained Arc view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HeapWalk {
    pub btree_nodes: usize,
    pub string_heaps: usize,
    pub vec_slots: usize,
    pub clone_heap: usize,
}

impl HeapWalk {
    fn add(&mut self, btree: usize, strings: usize, vecs: usize) {
        self.btree_nodes = self.btree_nodes.saturating_add(btree);
        self.string_heaps = self.string_heaps.saturating_add(strings);
        self.vec_slots = self.vec_slots.saturating_add(vecs);
        self.clone_heap = self
            .clone_heap
            .saturating_add(btree)
            .saturating_add(strings)
            .saturating_add(vecs);
    }

    #[must_use]
    pub fn peak_new(self, admitted_pretty: usize) -> usize {
        self.clone_heap.saturating_add(admitted_pretty)
    }
}

trait HeapSize {
    fn add_to(&self, walk: &mut HeapWalk);
}

impl HeapSize for String {
    fn add_to(&self, walk: &mut HeapWalk) {
        walk.add(0, self.len(), 0);
    }
}

impl HeapSize for PathBuf {
    fn add_to(&self, walk: &mut HeapWalk) {
        walk.add(0, self.as_os_str().len(), 0);
    }
}

impl HeapSize for bool {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for u16 {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for u64 {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for usize {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for i64 {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for serde_json::Number {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl<T: HeapSize> HeapSize for Option<T> {
    fn add_to(&self, walk: &mut HeapWalk) {
        if let Some(value) = self {
            value.add_to(walk);
        }
    }
}

impl<T: HeapSize> HeapSize for Vec<T> {
    fn add_to(&self, walk: &mut HeapWalk) {
        walk.add(0, 0, self.len().saturating_mul(size_of::<T>()));
        for item in self {
            item.add_to(walk);
        }
    }
}

impl HeapSize for Vec<u8> {
    fn add_to(&self, walk: &mut HeapWalk) {
        walk.add(0, 0, self.len());
    }
}

impl<K: HeapSize, V: HeapSize> HeapSize for BTreeMap<K, V> {
    fn add_to(&self, walk: &mut HeapWalk) {
        walk.add(btree_nodes::<K, V>(self.len()), 0, 0);
        for (key, value) in self {
            key.add_to(walk);
            value.add_to(walk);
        }
    }
}

impl HeapSize for Value {
    fn add_to(&self, walk: &mut HeapWalk) {
        match self {
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
            Value::String(text) => text.add_to(walk),
            Value::Array(items) => items.add_to(walk),
            Value::Object(map) => {
                walk.add(btree_nodes::<String, Value>(map.len()), 0, 0);
                for (key, nested) in map {
                    key.add_to(walk);
                    nested.add_to(walk);
                }
            }
        }
    }
}

impl HeapSize for HubState {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            schema_version,
            host,
            schema,
            package_registry,
            device_session_type_sources,
            session_type_generation,
            spawn_targets,
            worktrees,
            credential_keys,
            trusted_browser_identities,
            bootstrap_grants,
            capability_grants,
            admission_decisions,
            runtime_settings,
            audit_history,
        } = self;
        schema_version.add_to(walk);
        host.add_to(walk);
        schema.add_to(walk);
        package_registry.add_to(walk);
        device_session_type_sources.add_to(walk);
        session_type_generation.add_to(walk);
        spawn_targets.add_to(walk);
        worktrees.add_to(walk);
        credential_keys.add_to(walk);
        trusted_browser_identities.add_to(walk);
        bootstrap_grants.add_to(walk);
        capability_grants.add_to(walk);
        admission_decisions.add_to(walk);
        runtime_settings.add_to(walk);
        audit_history.add_to(walk);
    }
}

impl HeapSize for HostIdentity {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            id,
            display_name,
            fingerprint,
        } = self;
        id.add_to(walk);
        display_name.add_to(walk);
        fingerprint.add_to(walk);
    }
}

impl HeapSize for SchemaMetadata {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            hub_config_version,
            migrated_from,
        } = self;
        hub_config_version.add_to(walk);
        migrated_from.add_to(walk);
    }
}

impl HeapSize for PackageRegistrySnapshot {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            granted_capabilities,
            governed_surfaces,
            records,
        } = self;
        granted_capabilities.add_to(walk);
        governed_surfaces.add_to(walk);
        records.add_to(walk);
    }
}

impl HeapSize for SpawnTarget {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            target_id,
            label,
            root,
            enabled,
            kind,
            base_ref,
            metadata,
        } = self;
        target_id.add_to(walk);
        label.add_to(walk);
        root.add_to(walk);
        enabled.add_to(walk);
        kind.add_to(walk);
        base_ref.add_to(walk);
        metadata.add_to(walk);
    }
}

impl HeapSize for Worktree {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            worktree_id,
            target_id,
            label,
            path,
            status,
            management,
            git,
            metadata,
        } = self;
        worktree_id.add_to(walk);
        target_id.add_to(walk);
        label.add_to(walk);
        path.add_to(walk);
        status.add_to(walk);
        management.add_to(walk);
        git.add_to(walk);
        metadata.add_to(walk);
    }
}

impl HeapSize for WorktreeGitMetadata {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            repository_root,
            branch,
            head,
        } = self;
        repository_root.add_to(walk);
        branch.add_to(walk);
        head.add_to(walk);
    }
}

impl HeapSize for HubAuditEntry {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            recorded_at,
            actor,
            action,
            reason,
        } = self;
        recorded_at.add_to(walk);
        actor.add_to(walk);
        action.add_to(walk);
        reason.add_to(walk);
    }
}

impl HeapSize for DeviceSessionTypeSource {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            root,
            session_types,
        } = self;
        root.add_to(walk);
        session_types.add_to(walk);
    }
}

impl HeapSize for CredentialKeyReference {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            key_id,
            provider,
            purpose,
            created_at_unix_ms,
            rotated_at_unix_ms,
        } = self;
        key_id.add_to(walk);
        provider.add_to(walk);
        purpose.add_to(walk);
        created_at_unix_ms.add_to(walk);
        rotated_at_unix_ms.add_to(walk);
    }
}

impl HeapSize for TrustedBrowserIdentity {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            browser_id,
            public_key,
            fingerprint,
            credential_key_id,
            trusted_at_unix_ms,
            expires_at_unix_ms,
            revoked_at_unix_ms,
            audit_reason,
        } = self;
        browser_id.add_to(walk);
        public_key.add_to(walk);
        fingerprint.add_to(walk);
        credential_key_id.add_to(walk);
        trusted_at_unix_ms.add_to(walk);
        expires_at_unix_ms.add_to(walk);
        revoked_at_unix_ms.add_to(walk);
        audit_reason.add_to(walk);
    }
}

impl HeapSize for BootstrapGrantRecord {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            grant_id,
            package_instance_id,
            origin,
            peer_id,
            credential_key_id,
            expires_at_unix_ms,
            revoked_at_unix_ms,
            redeemed_at_unix_ms,
            audit_reason,
        } = self;
        grant_id.add_to(walk);
        package_instance_id.add_to(walk);
        origin.add_to(walk);
        peer_id.add_to(walk);
        credential_key_id.add_to(walk);
        expires_at_unix_ms.add_to(walk);
        revoked_at_unix_ms.add_to(walk);
        redeemed_at_unix_ms.add_to(walk);
        audit_reason.add_to(walk);
    }
}

impl HeapSize for CapabilityGrantRecord {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            subject,
            capability,
            governed_surface,
            audit_reason,
        } = self;
        subject.add_to(walk);
        capability.add_to(walk);
        governed_surface.add_to(walk);
        audit_reason.add_to(walk);
    }
}

impl HeapSize for PackageAdmissionDecision {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            package_name,
            action,
            outcome,
            audit_reason,
        } = self;
        package_name.add_to(walk);
        action.add_to(walk);
        outcome.add_to(walk);
        audit_reason.add_to(walk);
    }
}

impl HeapSize for LocalRuntimeSettings {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            data_directory,
            session_defaults,
            plugin_directories,
            provider_directories,
            transports,
            core_engine,
        } = self;
        data_directory.add_to(walk);
        session_defaults.add_to(walk);
        plugin_directories.add_to(walk);
        provider_directories.add_to(walk);
        transports.add_to(walk);
        core_engine.add_to(walk);
    }
}

impl HeapSize for SessionDefaults {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            shell,
            working_directory,
            initial_rows,
            initial_cols,
        } = self;
        shell.add_to(walk);
        working_directory.add_to(walk);
        initial_rows.add_to(walk);
        initial_cols.add_to(walk);
    }
}

impl HeapSize for TransportBindings {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { local_socket, tcp } = self;
        local_socket.add_to(walk);
        tcp.add_to(walk);
    }
}

impl HeapSize for LocalSocketBinding {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { path } = self;
        path.add_to(walk);
    }
}

impl HeapSize for TcpBinding {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { host, port } = self;
        host.add_to(walk);
        port.add_to(walk);
    }
}

impl HeapSize for CoreEngineOptions {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            queue_capacities,
            session_worker_path,
            session_io_coalescing,
            plugin_worker_queue_capacity,
            plugin_worker_executor_concurrency,
        } = self;
        queue_capacities.add_to(walk);
        session_worker_path.add_to(walk);
        session_io_coalescing.add_to(walk);
        plugin_worker_queue_capacity.add_to(walk);
        plugin_worker_executor_concurrency.add_to(walk);
    }
}

impl HeapSize for CoreQueueCapacity {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { source, capacity } = self;
        source.add_to(walk);
        capacity.add_to(walk);
    }
}

impl HeapSize for SessionIoCoalescingOptions {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            max_output_bytes,
            max_output_frames,
            max_window_ms,
        } = self;
        max_output_bytes.add_to(walk);
        max_output_frames.add_to(walk);
        max_window_ms.add_to(walk);
    }
}

impl HeapSize for PackageRecord {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            manifest,
            state,
            classification,
            trust,
            provenance,
            source_metadata,
            pin,
            update_policy,
            admitted_capabilities,
            compatibility,
            runnable_entrypoints,
            session_types,
            configuration,
            installed_at,
            updated_at,
            last_audit_reason,
            admitted_host_profile,
        } = self;
        manifest.add_to(walk);
        state.add_to(walk);
        classification.add_to(walk);
        trust.add_to(walk);
        provenance.add_to(walk);
        source_metadata.add_to(walk);
        pin.add_to(walk);
        update_policy.add_to(walk);
        admitted_capabilities.add_to(walk);
        compatibility.add_to(walk);
        runnable_entrypoints.add_to(walk);
        session_types.add_to(walk);
        configuration.add_to(walk);
        installed_at.add_to(walk);
        updated_at.add_to(walk);
        last_audit_reason.add_to(walk);
        admitted_host_profile.add_to(walk);
    }
}

impl HeapSize for HubPackageManifest {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            name,
            version,
            kind,
            botster,
            source,
            capabilities,
            entrypoints,
            dependencies,
            features,
            host_profile,
            configuration,
            runnable_entrypoints,
            surfaces,
            navigation,
            events,
        } = self;
        name.add_to(walk);
        version.add_to(walk);
        kind.add_to(walk);
        botster.add_to(walk);
        source.add_to(walk);
        capabilities.add_to(walk);
        entrypoints.add_to(walk);
        dependencies.add_to(walk);
        features.add_to(walk);
        host_profile.add_to(walk);
        configuration.add_to(walk);
        runnable_entrypoints.add_to(walk);
        surfaces.add_to(walk);
        navigation.add_to(walk);
        events.add_to(walk);
    }
}

impl HeapSize for HubPackageEvents {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { emitted, notices } = self;
        emitted.add_to(walk);
        notices.add_to(walk);
    }
}

impl HeapSize for HubEmittedEvent {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            name,
            payload_schema,
            audience,
            owner,
        } = self;
        name.add_to(walk);
        payload_schema.add_to(walk);
        audience.add_to(walk);
        owner.add_to(walk);
    }
}

impl HeapSize for Capability {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { surface, scope } = self;
        surface.add_to(walk);
        scope.add_to(walk);
    }
}

impl HeapSize for CapabilitySurface {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for PackageState {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for PackageClassification {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for PackageUpdatePolicy {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for PackageTrust {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            classification,
            first_party,
        } = self;
        classification.add_to(walk);
        first_party.add_to(walk);
    }
}

impl HeapSize for PackageProvenance {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { source, checksum } = self;
        source.add_to(walk);
        checksum.add_to(walk);
    }
}

impl HeapSize for PackageSourceMetadata {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            registry_id,
            registry_kind,
            entry_id,
            source_kind,
            source_label,
            git_repo,
        } = self;
        registry_id.add_to(walk);
        registry_kind.add_to(walk);
        entry_id.add_to(walk);
        source_kind.add_to(walk);
        source_label.add_to(walk);
        git_repo.add_to(walk);
    }
}

impl HeapSize for PackagePin {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            revision,
            branch,
            tag,
            rev,
            checksum,
            update_policy,
        } = self;
        revision.add_to(walk);
        branch.add_to(walk);
        tag.add_to(walk);
        rev.add_to(walk);
        checksum.add_to(walk);
        update_policy.add_to(walk);
    }
}

impl HeapSize for PackageCompatibility {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            botster_requirement,
            hub_version,
            result,
            diagnostics,
        } = self;
        botster_requirement.add_to(walk);
        hub_version.add_to(walk);
        result.add_to(walk);
        diagnostics.add_to(walk);
    }
}

impl HeapSize for PackageRunnableEntrypoint {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            id,
            kind,
            launch_mode,
            command,
            args,
            working_directory,
            injections,
            environment,
            capabilities,
            readiness,
            may_supervise,
            process,
        } = self;
        id.add_to(walk);
        kind.add_to(walk);
        launch_mode.add_to(walk);
        command.add_to(walk);
        args.add_to(walk);
        working_directory.add_to(walk);
        injections.add_to(walk);
        environment.add_to(walk);
        capabilities.add_to(walk);
        readiness.add_to(walk);
        may_supervise.add_to(walk);
        process.add_to(walk);
    }
}

impl HeapSize for PackageConfigurationState {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { values } = self;
        values.add_to(walk);
    }
}

impl HeapSize for botster_core::PackageConfigurationValue {
    fn add_to(&self, walk: &mut HeapWalk) {
        match self {
            Self::String { value }
            | Self::Select { value }
            | Self::Path { value }
            | Self::Url { value }
            | Self::MultilineText { value } => value.add_to(walk),
            Self::Number { .. }
            | Self::Integer { .. }
            | Self::Boolean { .. }
            | Self::Secret { .. } => {}
        }
    }
}

impl HeapSize for PackageSessionType {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            id,
            label,
            description,
            icon,
            role,
            interaction,
            traits,
            lifecycle,
            execution,
            command,
            args,
            working_directory,
            environment,
            allowed_environment_overrides,
            context,
            target_id,
        } = self;
        id.add_to(walk);
        label.add_to(walk);
        description.add_to(walk);
        icon.add_to(walk);
        role.add_to(walk);
        interaction.add_to(walk);
        traits.add_to(walk);
        lifecycle.add_to(walk);
        execution.add_to(walk);
        command.add_to(walk);
        args.add_to(walk);
        working_directory.add_to(walk);
        environment.add_to(walk);
        allowed_environment_overrides.add_to(walk);
        context.add_to(walk);
        target_id.add_to(walk);
    }
}

impl HeapSize for crate::session_types::PackageSessionTypeExecution {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for crate::session_types::PackageSessionTypeWorkingDirectory {
    fn add_to(&self, walk: &mut HeapWalk) {
        match self {
            Self::PackageRoot => {}
            Self::Relative { path } => path.add_to(walk),
        }
    }
}

impl HeapSize for crate::packages::PackageRunnableWorkingDirectory {
    fn add_to(&self, walk: &mut HeapWalk) {
        match self {
            Self::PackageRoot | Self::EntrypointDir => {}
            Self::Relative { path } => path.add_to(walk),
        }
    }
}

impl HeapSize for crate::packages::PackageEnvironmentRequirement {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            name,
            required,
            default,
            description,
        } = self;
        name.add_to(walk);
        required.add_to(walk);
        default.add_to(walk);
        description.add_to(walk);
    }
}

impl HeapSize for crate::packages::PackageRunnableProcess {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { state, diagnostics } = self;
        state.add_to(walk);
        diagnostics.add_to(walk);
    }
}

impl HeapSize for CredentialProviderKind {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for CredentialKeyPurpose {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::ExtensionKind {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::PackageSource {
    fn add_to(&self, walk: &mut HeapWalk) {
        match self {
            Self::Git { repo, reference } => {
                repo.add_to(walk);
                reference.add_to(walk);
            }
            Self::Path { path } => path.add_to(walk),
        }
    }
}

impl HeapSize for botster_core::ExtensionEntrypoint {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            runtime,
            path,
            bootstrap,
        } = self;
        runtime.add_to(walk);
        path.add_to(walk);
        bootstrap.add_to(walk);
    }
}

impl HeapSize for botster_core::PackageDependency {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            id,
            package,
            kind,
            feature,
            requirements,
        } = self;
        id.add_to(walk);
        package.add_to(walk);
        kind.add_to(walk);
        feature.add_to(walk);
        requirements.add_to(walk);
    }
}

impl HeapSize for botster_core::PackageRequirement {
    fn add_to(&self, walk: &mut HeapWalk) {
        match self {
            Self::Provider { provider } => provider.add_to(walk),
            Self::Capability { capability } => capability.add_to(walk),
            Self::Auth { key } | Self::Config { key } => key.add_to(walk),
        }
    }
}

impl HeapSize for botster_core::PackageFeatureGate {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            id,
            label,
            description,
            dependencies,
            requirements,
        } = self;
        id.add_to(walk);
        label.add_to(walk);
        description.add_to(walk);
        dependencies.add_to(walk);
        requirements.add_to(walk);
    }
}

impl HeapSize for botster_core::HostProfileMetadata {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            profile_id,
            compatibility,
            precedence,
            required_providers,
            required_capabilities,
            policy_sections,
        } = self;
        profile_id.add_to(walk);
        compatibility.add_to(walk);
        precedence.add_to(walk);
        required_providers.add_to(walk);
        required_capabilities.add_to(walk);
        policy_sections.add_to(walk);
    }
}

impl HeapSize for botster_core::AdmittedHostProfile {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            package_name,
            package_version,
            metadata,
        } = self;
        package_name.add_to(walk);
        package_version.add_to(walk);
        metadata.add_to(walk);
    }
}

impl HeapSize for botster_core::PackageConfigurationSchema {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { groups, fields } = self;
        groups.add_to(walk);
        fields.add_to(walk);
    }
}

impl HeapSize for botster_core::PackageConfigurationGroup {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            id,
            label,
            description,
            order,
        } = self;
        id.add_to(walk);
        label.add_to(walk);
        description.add_to(walk);
        order.add_to(walk);
    }
}

impl HeapSize for botster_core::PackageConfigurationField {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            key,
            field_type,
            label,
            description,
            required,
            default,
            validation,
            group,
            order,
            options,
        } = self;
        key.add_to(walk);
        field_type.add_to(walk);
        label.add_to(walk);
        description.add_to(walk);
        required.add_to(walk);
        default.add_to(walk);
        validation.add_to(walk);
        group.add_to(walk);
        order.add_to(walk);
        options.add_to(walk);
    }
}

impl HeapSize for botster_core::PackageConfigurationOption {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            value,
            label,
            description,
        } = self;
        value.add_to(walk);
        label.add_to(walk);
        description.add_to(walk);
    }
}

impl HeapSize for botster_core::PackageConfigurationValidationHints {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            min_length,
            max_length,
            pattern,
            min,
            max,
            allowed_extensions,
        } = self;
        min_length.add_to(walk);
        max_length.add_to(walk);
        pattern.add_to(walk);
        min.add_to(walk);
        max.add_to(walk);
        allowed_extensions.add_to(walk);
    }
}

impl HeapSize for botster_core::RunnableEntrypoint {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            id,
            kind,
            launch_mode,
            command,
            args,
            working_directory,
            injections,
            environment,
            readiness,
        } = self;
        id.add_to(walk);
        kind.add_to(walk);
        launch_mode.add_to(walk);
        command.add_to(walk);
        args.add_to(walk);
        working_directory.add_to(walk);
        injections.add_to(walk);
        environment.add_to(walk);
        readiness.add_to(walk);
    }
}

impl HeapSize for botster_ui_contract::PackageSurfaceDescriptor {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            id,
            kind,
            title,
            description,
            icon,
            order,
            category,
            supports,
        } = self;
        id.add_to(walk);
        kind.add_to(walk);
        title.add_to(walk);
        description.add_to(walk);
        icon.add_to(walk);
        order.add_to(walk);
        category.add_to(walk);
        supports.add_to(walk);
    }
}

impl HeapSize for botster_ui_contract::PackageNavigationEntry {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            id,
            label,
            icon,
            description,
            target,
        } = self;
        id.add_to(walk);
        label.add_to(walk);
        icon.add_to(walk);
        description.add_to(walk);
        target.add_to(walk);
    }
}

impl HeapSize for botster_ui_contract::PackageNavigationTarget {
    fn add_to(&self, walk: &mut HeapWalk) {
        match self {
            Self::Surface { surface_id } => surface_id.add_to(walk),
        }
    }
}

impl HeapSize for botster_ui_contract::PackageNoticeReactionDeclaration {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            owner,
            name,
            subject_scope,
            text_pointer,
            ttl_ms,
            severity,
        } = self;
        owner.add_to(walk);
        name.add_to(walk);
        subject_scope.add_to(walk);
        text_pointer.add_to(walk);
        ttl_ms.add_to(walk);
        severity.add_to(walk);
    }
}

impl HeapSize for crate::packages::PackageRunnableDiagnostic {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { kind, message } = self;
        kind.add_to(walk);
        message.add_to(walk);
    }
}

impl HeapSize for u32 {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for crate::packages::PackageTrustClassification {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for crate::packages::PackageRegistrySourceKind {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for crate::packages::PackageRegistryEntrySourceKind {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for crate::packages::PackageCompatibilityResult {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for crate::packages::PackageRunnableProcessState {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::ExtensionRuntime {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::PackageDependencyKind {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::HostProfilePolicySection {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::PackageConfigurationFieldType {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::RunnableEntrypointKind {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::RunnableEntrypointLaunchMode {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::RunnableEntrypointWorkingDirectory {
    fn add_to(&self, walk: &mut HeapWalk) {
        match self {
            Self::PackageRoot | Self::EntrypointDir => {}
            Self::Relative { path } => path.add_to(walk),
        }
    }
}

impl HeapSize for botster_core::RunnableEntrypointInjection {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            kind,
            target,
            required,
            description,
        } = self;
        kind.add_to(walk);
        target.add_to(walk);
        required.add_to(walk);
        description.add_to(walk);
    }
}

impl HeapSize for botster_core::RunnableEntrypointInjectionKind {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_core::RunnableEntrypointInjectionTarget {
    fn add_to(&self, walk: &mut HeapWalk) {
        match self {
            Self::Environment { name } | Self::Argument { value: name } => name.add_to(walk),
        }
    }
}

impl HeapSize for botster_core::RunnableEntrypointEnvironmentRequirement {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self {
            name,
            required,
            default,
            description,
        } = self;
        name.add_to(walk);
        required.add_to(walk);
        default.add_to(walk);
        description.add_to(walk);
    }
}

impl HeapSize for botster_core::RunnableEntrypointReadiness {
    fn add_to(&self, walk: &mut HeapWalk) {
        let Self { result_fields } = self;
        result_fields.add_to(walk);
    }
}

impl HeapSize for botster_core::RunnableEntrypointResultField {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_ui_contract::PackageSurfaceKind {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_ui_contract::PackageSurfaceOperation {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_ui_contract::PackageNoticeSubjectScope {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

impl HeapSize for botster_ui_contract::PackageNoticeSeverity {
    fn add_to(&self, _walk: &mut HeapWalk) {}
}

#[must_use]
pub fn walk_hub_state(state: &HubState) -> HeapWalk {
    let mut walk = HeapWalk::default();
    state.add_to(&mut walk);
    walk
}

#[must_use]
pub fn admitted_pretty(state: &HubState) -> Result<usize, serde_json::Error> {
    serde_json::to_vec_pretty(state).map(|bytes| bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeEnvironment;
    use crate::config::HubStartupOptions;

    #[test]
    fn string_value_internal_node_is_larger_than_string_string() {
        assert_eq!(btree_internal_size::<String, String>(), 640);
        assert!(btree_internal_size::<String, Value>() > btree_internal_size::<String, String>());
        assert!(btree_internal_size::<String, Value>() >= 728);
    }

    #[test]
    fn empty_btree_walk_is_zero_nodes() {
        let map = BTreeMap::<String, String>::new();
        let mut walk = HeapWalk::default();
        map.add_to(&mut walk);
        assert_eq!(walk.btree_nodes, 0);
        assert_eq!(walk.clone_heap, 0);
    }

    #[test]
    fn one_entry_btree_includes_a_root_node() {
        let mut map = BTreeMap::<String, String>::new();
        map.insert("k".into(), "v".into());
        let mut walk = HeapWalk::default();
        map.add_to(&mut walk);
        assert_eq!(walk.string_heaps, 2);
        assert_eq!(walk.btree_nodes, btree_nodes::<String, String>(1));
    }

    #[test]
    fn payload_schema_shape_ratio_is_reported_not_selected() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "p": { "type": "string" } }
        });
        let mut walk = HeapWalk::default();
        schema.add_to(&mut walk);
        let pretty = serde_json::to_vec_pretty(&schema).unwrap().len();
        assert!(pretty > 0);
        assert!(walk.clone_heap > 0);
        let ratio = walk.clone_heap as f64 / pretty as f64;
        assert!(ratio > 0.0, "report-only ratio {ratio}");
    }

    #[test]
    fn empty_hub_state_walk_does_not_select_a_budget() {
        let config = HubStartupOptions {
            data_directory: crate::config::DataDirectoryOption::Explicit(std::path::PathBuf::from(
                "/private/tmp/hub-state-heap-walk",
            )),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let state = HubState::from_config(&config);
        let walk = walk_hub_state(&state);
        let pretty = admitted_pretty(&state).unwrap();
        let peak = walk.peak_new(pretty);
        assert!(pretty > 0);
        assert!(peak >= pretty);
        assert_ne!(
            peak,
            64 * 1024 * 1024,
            "must not silently use the logical 64 MiB as G1"
        );
    }
}
