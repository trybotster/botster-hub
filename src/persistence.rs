//! Profile-owned durable hub state boundary.
//!
//! The hub persists product and policy state here while `botster-core` remains
//! the owner of reusable session, transport, package, and admission mechanics.
//! Version 4 is a single local JSON file intended for the local runtime. It is a
//! single-writer store. Retained directory ownership excludes another writer.
//! A write that reaches rename has a distinct published outcome.

use std::error::Error;
use std::fmt;
#[cfg(test)]
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};

use botster_core::{Capability, CapabilitySurface};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::config::{
    CoreEngineOptions, HostIdentity, HubConfig, SessionDefaults, TransportBindings,
};
use crate::credentials::{CredentialKeyPurpose, CredentialProviderKind};
use crate::packages::PackageRegistrySnapshot;
use crate::recovery::journal::{
    DurableIntentReceipt, JournalError, JournalExternal, JournalIntent, RecoveryJournal,
};
use crate::recovery::state_directory::{
    StateDirectoryError, StateDirectoryOwnership, StateDocumentCommit,
};
use crate::session_types::PackageSessionType;
use crate::shared_view::{SharedView, SharedViewBudget, SharedViewCharge};
use crate::spawn_targets::SpawnTarget;
use crate::worktrees::Worktree;

const HUB_STATE_SCHEMA_VERSION: u16 = 4;
const HUB_STATE_FILE_NAME: &str = "hub-state.json";

/// Persistence buckets the host profile must govern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceBucket {
    /// Durable host and admission state.
    HostState,
    /// Installed package metadata, pins, provenance, and enabled state.
    PackageState,
    /// Provider-owned runtime metadata admitted by hub policy.
    ProviderState,
}

/// Versioned durable hub state aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubState {
    /// Version of this JSON schema. Version 4 adds recovery ownership.
    pub schema_version: u16,
    /// Host identity metadata resolved from hub config.
    pub host: HostIdentity,
    /// Config/schema metadata needed by future migrations.
    pub schema: SchemaMetadata,
    /// Package/provider registry records, grants, pins, provenance, and enabled state.
    pub package_registry: PackageRegistrySnapshot,
    /// Device-owned session type sources persisted by the hub profile.
    #[serde(default)]
    pub device_session_type_sources: Vec<DeviceSessionTypeSource>,
    /// Monotonic generation for the effective authoritative session type map.
    #[serde(default)]
    pub session_type_generation: u64,
    /// Hub-owned spawn targets admitted for client/plugin references.
    #[serde(default)]
    pub spawn_targets: Vec<SpawnTarget>,
    /// Hub-owned worktree records scoped to admitted spawn targets.
    #[serde(default)]
    pub worktrees: Vec<Worktree>,
    /// References to secret material held by the credential provider.
    #[serde(default)]
    pub credential_keys: Vec<CredentialKeyReference>,
    /// Durable trusted browser public identity metadata. No private keys or grant secrets.
    #[serde(default)]
    pub trusted_browser_identities: Vec<TrustedBrowserIdentity>,
    /// Bootstrap grant metadata. Grant secret material, when durable, is provider-owned.
    #[serde(default)]
    pub bootstrap_grants: Vec<BootstrapGrantRecord>,
    /// Audit-friendly capability grant records.
    pub capability_grants: Vec<CapabilityGrantRecord>,
    /// Admission decision history for package/provider policy.
    pub admission_decisions: Vec<PackageAdmissionDecision>,
    /// Local runtime settings derived from current hub config.
    pub runtime_settings: LocalRuntimeSettings,
    /// Append-only operator decision history.
    pub audit_history: Vec<HubAuditEntry>,
    /// Hub-owned intent and evidence. The worktree collection remains the registry.
    #[serde(default)]
    pub(crate) recovery: crate::recovery::record::RecoveryLedger,
}

impl HubState {
    /// Build an empty durable state aggregate from resolved runtime config.
    #[must_use]
    pub fn from_config(config: &HubConfig) -> Self {
        Self {
            schema_version: HUB_STATE_SCHEMA_VERSION,
            host: config.host.clone(),
            schema: SchemaMetadata::v1(),
            package_registry: PackageRegistrySnapshot::empty(),
            device_session_type_sources: Vec::new(),
            session_type_generation: 0,
            spawn_targets: Vec::new(),
            worktrees: Vec::new(),
            credential_keys: Vec::new(),
            trusted_browser_identities: Vec::new(),
            bootstrap_grants: Vec::new(),
            capability_grants: Vec::new(),
            admission_decisions: Vec::new(),
            runtime_settings: LocalRuntimeSettings::from_config(config),
            audit_history: Vec::new(),
            recovery: crate::recovery::record::RecoveryLedger::default(),
        }
    }

    fn validate_version(&self) -> HubStateResult<()> {
        if self.schema_version == HUB_STATE_SCHEMA_VERSION {
            self.recovery
                .validate(&self.host.id)
                .map_err(|_| HubStateError::InvalidRecoveryState)
        } else {
            Err(HubStateError::UnsupportedVersion(self.schema_version))
        }
    }
}

/// One durable device-level session type source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceSessionTypeSource {
    /// Root used to resolve relative session type command and cwd policy paths.
    pub root: PathBuf,
    /// Device-owned session type definitions.
    #[serde(default)]
    pub session_types: Vec<PackageSessionType>,
}

/// Reference to credential-provider-owned secret material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialKeyReference {
    /// Stable provider key id. The referenced credential value is never in hub-state.
    pub key_id: String,
    /// Concrete provider expected to hold this credential.
    pub provider: CredentialProviderKind,
    /// Hub-owned purpose for audit and lookup policy.
    pub purpose: CredentialKeyPurpose,
    /// Logical creation time supplied by the caller.
    pub created_at_unix_ms: u64,
    /// Optional rotation time supplied by the caller.
    pub rotated_at_unix_ms: Option<u64>,
}

/// Durable public browser identity trust metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedBrowserIdentity {
    /// Synthetic browser identity id. Do not use hostnames, emails, or local paths.
    pub browser_id: String,
    /// Public verifying key bytes. Private key material is credential-provider-owned.
    pub public_key: Vec<u8>,
    /// Public-key-derived fingerprint.
    pub fingerprint: String,
    /// Optional credential key reference for browser-owned secret/session material.
    pub credential_key_id: Option<String>,
    /// Time this browser identity became trusted.
    pub trusted_at_unix_ms: u64,
    /// Optional trust expiry.
    pub expires_at_unix_ms: Option<u64>,
    /// Revocation time when trust has been revoked.
    pub revoked_at_unix_ms: Option<u64>,
    /// Audit-safe reason. Callers must not put PII or secrets here.
    pub audit_reason: String,
}

/// Durable bootstrap grant metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapGrantRecord {
    /// Synthetic grant id. The raw grant token is never in hub-state.
    pub grant_id: String,
    /// Package or app instance this grant is scoped to.
    pub package_instance_id: String,
    /// Expected local origin label, such as `localhost`.
    pub origin: String,
    /// Expected peer/session route id.
    pub peer_id: String,
    /// Optional credential key reference for sealed grant material.
    pub credential_key_id: Option<String>,
    /// Grant expiry time.
    pub expires_at_unix_ms: u64,
    /// Revocation time when the grant has been revoked.
    pub revoked_at_unix_ms: Option<u64>,
    /// Redemption time when the grant has been consumed.
    pub redeemed_at_unix_ms: Option<u64>,
    /// Audit-safe reason. Callers must not put PII or secrets here.
    pub audit_reason: String,
}

/// Schema and migration metadata recorded inside the state file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaMetadata {
    /// Human-readable config schema generation owned by the hub.
    pub hub_config_version: u16,
    /// Future migration hook; v1 has no prior version.
    pub migrated_from: Option<u16>,
}

impl SchemaMetadata {
    /// Current v1 schema metadata.
    #[must_use]
    pub const fn v1() -> Self {
        Self {
            hub_config_version: 1,
            migrated_from: None,
        }
    }
}

/// One hub-owned capability grant snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityGrantRecord {
    /// Package or provider key receiving the grant.
    pub subject: String,
    /// Granted capability.
    pub capability: Capability,
    /// Surface governed by the hub host profile for audit filtering.
    pub governed_surface: CapabilitySurface,
    /// Operator-supplied reason. Callers must not put secrets or PII here.
    pub audit_reason: String,
}

/// One persisted package/provider admission decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageAdmissionDecision {
    /// Package or provider name.
    pub package_name: String,
    /// Decision action such as install, enable, disable, or pin.
    pub action: String,
    /// Decision result such as accepted or denied.
    pub outcome: String,
    /// Operator-supplied reason. Callers must not put secrets or PII here.
    pub audit_reason: String,
}

/// Local runtime settings expected to survive restarts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalRuntimeSettings {
    /// Resolved data directory used by this state file.
    pub data_directory: PathBuf,
    /// Session defaults resolved for the hub.
    pub session_defaults: SessionDefaults,
    /// Plugin directories resolved from config.
    pub plugin_directories: Vec<PathBuf>,
    /// Provider directories resolved from config.
    pub provider_directories: Vec<PathBuf>,
    /// Transport bindings resolved from config.
    pub transports: TransportBindings,
    /// Hub policy for core-owned engine knobs.
    pub core_engine: CoreEngineOptions,
}

impl LocalRuntimeSettings {
    /// Snapshot local runtime settings from resolved config.
    #[must_use]
    pub fn from_config(config: &HubConfig) -> Self {
        Self {
            data_directory: config.data_directory.clone(),
            session_defaults: config.session_defaults.clone(),
            plugin_directories: config.plugin_directories.clone(),
            provider_directories: config.provider_directories.clone(),
            transports: config.transports.clone(),
            core_engine: config.core_engine.clone(),
        }
    }
}

/// One append-only audit entry in the v1 state file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubAuditEntry {
    /// Logical or wall-clock timestamp supplied by the caller.
    pub recorded_at: String,
    /// Actor label supplied by the caller.
    pub actor: String,
    /// Action label supplied by the caller.
    pub action: String,
    /// Operator-supplied reason. Callers must not put secrets or PII here.
    pub reason: String,
}

/// Typed hub state model errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubStateError {
    /// The state file uses a future or unsupported schema version.
    UnsupportedVersion(u16),
    /// Recovery identity or migration input is inconsistent.
    InvalidRecoveryState,
}

impl fmt::Display for HubStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported hub state schema version {version}")
            }
            Self::InvalidRecoveryState => formatter.write_str("invalid Hub recovery state"),
        }
    }
}

impl Error for HubStateError {}

/// Hub state model result alias.
pub type HubStateResult<T> = Result<T, HubStateError>;

/// Storage boundary for durable hub state.
pub trait HubStateStore {
    /// Load existing state or create a current default when no file exists.
    fn load_or_initialize(&self, config: &HubConfig) -> HubStateStoreResult<HubState>;

    /// Custom stores keep their existing load behavior and return no File authority.
    fn load_retained(
        &self,
        config: &HubConfig,
    ) -> HubStateStoreResult<(HubState, Option<HubStateAuthority>)> {
        self.load_or_initialize(config).map(|state| (state, None))
    }

    /// Save state while startup has exclusive ownership and no shared view exists.
    fn save_exclusive_startup_state(&self, state: &HubState) -> HubStateStoreResult<()>;
}

/// Local-first file-backed implementation of durable hub state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHubStateStore {
    path: PathBuf,
}

/// One File document lineage. Clones share one directory lock and view budget.
pub struct HubStateAuthority {
    store_path: PathBuf,
    directory: StateDirectoryOwnership,
    journal: Arc<Mutex<RecoveryJournal>>,
    budget: Arc<SharedViewBudget>,
    startup_charge: Option<SharedViewCharge>,
}

impl HubStateAuthority {
    pub(crate) fn store(&self) -> FileHubStateStore {
        FileHubStateStore {
            path: self.store_path.clone(),
        }
    }

    fn clone_for_write(&self) -> Self {
        Self {
            store_path: self.store_path.clone(),
            directory: self.directory.clone(),
            journal: Arc::clone(&self.journal),
            budget: Arc::clone(&self.budget),
            startup_charge: None,
        }
    }

    pub(crate) fn budget(&self) -> Arc<SharedViewBudget> {
        Arc::clone(&self.budget)
    }

    /// Runtime moves this charge into its first published view.
    pub(crate) fn take_startup_charge(&mut self) -> Option<SharedViewCharge> {
        self.startup_charge.take()
    }
}

impl fmt::Debug for HubStateAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HubStateAuthority")
            .field("store_path", &self.store_path)
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub enum HubStatePublicationCause {
    DirectorySync(io::Error),
    DirectoryChanged,
    JournalCompletion,
}

impl HubStatePublicationCause {
    pub(crate) fn client_error(&self) -> (&'static str, &'static str) {
        match self {
            Self::DirectorySync(_) | Self::DirectoryChanged => (
                "state_publication_uncertain",
                "the state write reached publication without a confirmed durable result",
            ),
            Self::JournalCompletion => (
                "state_completion_record_uncertain",
                "the state write is synchronized, but recovery completion synchronization is unconfirmed",
            ),
        }
    }
}

#[derive(Debug)]
struct FileWriteEvidence {
    candidate: SharedView<HubState>,
    prior: Option<SharedView<HubState>>,
    authority: HubStateAuthority,
    base_revision: u64,
    committed_revision: u64,
    cause: Option<HubStatePublicationCause>,
    journal_error: Option<JournalError>,
    receipt: Option<DurableIntentReceipt>,
}

#[derive(Serialize)]
struct StateWriteIntentMetadata<'a> {
    kind: &'static str,
    host_id: &'a str,
    base_revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_path: Option<&'a [u8]>,
}

pub(crate) struct ExternalFileIntent<'a> {
    pub(crate) path: &'a Path,
    pub(crate) prior: Option<&'a [u8]>,
    pub(crate) candidate: &'a [u8],
}

/// Evidence owned by the caller after a write reached rename without a clean result.
#[derive(Debug)]
#[must_use]
pub struct HubStateUncertainWrite(Box<FileWriteEvidence>);

impl HubStateUncertainWrite {
    pub fn cause(&self) -> &HubStatePublicationCause {
        self.0.cause.as_ref().expect("published cause is set")
    }

    pub fn candidate(&self) -> &HubState {
        &self.0.candidate
    }

    pub fn prior(&self) -> Option<&HubState> {
        self.0.prior.as_deref()
    }

    pub fn base_revision(&self) -> u64 {
        self.0.base_revision
    }

    pub fn committed_revision(&self) -> u64 {
        self.0.committed_revision
    }

    pub(crate) fn authority(&self) -> &HubStateAuthority {
        &self.0.authority
    }

    pub(crate) fn journal_error(&self) -> Option<&JournalError> {
        self.0.journal_error.as_ref()
    }

    pub(crate) fn receipt_sequence(&self) -> Option<u64> {
        self.0.receipt.as_ref().map(DurableIntentReceipt::sequence)
    }
}

#[derive(Debug)]
#[must_use]
pub(crate) enum FileCommitOutcome {
    Synced {
        state: SharedView<HubState>,
        revision: u64,
    },
    PublishedUncertain(HubStateUncertainWrite),
}

#[derive(Debug)]
pub(crate) enum FileCommitError {
    Preparation(HubStateStoreError),
    Stale(PreparedHubStateWrite),
    RevisionExhausted(PreparedHubStateWrite),
    BeforePublication {
        error: HubStateStoreError,
        prepared: PreparedHubStateWrite,
    },
}

#[derive(Debug)]
pub(crate) struct PreparedHubStateWrite {
    evidence: Box<FileWriteEvidence>,
    bytes: Vec<u8>,
}

/// One prepared state write with its durable intent already synchronized.
#[derive(Debug)]
#[must_use]
pub(crate) struct PendingFileCommit {
    prepared: PreparedHubStateWrite,
}

impl PendingFileCommit {
    pub(crate) fn receipt_sequence(&self) -> u64 {
        self.prepared
            .evidence
            .receipt
            .as_ref()
            .expect("pending File commit has a durable receipt")
            .sequence()
    }
}

#[derive(Debug)]
pub(crate) struct FileEffectCommitError {
    pub(crate) error: HubStateStoreError,
    pub(crate) pending: PendingFileCommit,
}

impl FileHubStateStore {
    /// Build a store at `<data_directory>/hub-state.json`.
    #[must_use]
    pub fn for_data_directory(data_directory: impl AsRef<Path>) -> Self {
        let data_directory = data_directory.as_ref();
        Self {
            path: data_directory.join(HUB_STATE_FILE_NAME),
        }
    }

    /// Return the JSON state file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Update a unit-test fixture without a live runtime or retained shared views.
    #[cfg(test)]
    pub(crate) fn update_test_fixture(
        &self,
        config: &HubConfig,
        update: impl FnOnce(&mut HubState),
    ) -> HubStateStoreResult<HubState> {
        let (mut state, Some(mut authority)) = self.load_retained(config)? else {
            unreachable!("File load returns authority")
        };
        let prior = SharedView::from_reserved(
            state.clone(),
            authority.take_startup_charge().expect("startup charge"),
        );
        update(&mut state);
        let prepared =
            self.prepare_shared(&authority, 0, Some(prior), state, &authority.budget())?;
        match self.commit_shared(prepared, 0) {
            Ok(FileCommitOutcome::Synced { state, .. }) => Ok((*state).clone()),
            Ok(FileCommitOutcome::PublishedUncertain(write)) => {
                Err(HubStateStoreError::PublishedUncertain(write))
            }
            Err(FileCommitError::Preparation(error))
            | Err(FileCommitError::BeforePublication { error, .. }) => Err(error),
            Err(FileCommitError::Stale(_)) => Err(HubStateStoreError::StaleRevision),
            Err(FileCommitError::RevisionExhausted(_)) => {
                Err(HubStateStoreError::RevisionExhausted)
            }
        }
    }

    fn check_authority(&self, authority: &HubStateAuthority) -> HubStateStoreResult<()> {
        if authority.store_path != self.path {
            return Err(HubStateStoreError::AuthorityMismatch);
        }
        Ok(())
    }

    fn acquire_authority(&self) -> HubStateStoreResult<HubStateAuthority> {
        let parent = self
            .path
            .parent()
            .ok_or(HubStateStoreError::MissingParent)?;
        let directory = StateDirectoryOwnership::acquire(parent).map_err(map_directory_error)?;
        let state_exists = directory
            .open_document_bytes()
            .map_err(map_directory_error)?
            .is_some();
        let journal = RecoveryJournal::scan(directory.clone(), state_exists).map_err(|error| {
            HubStateStoreError::RecoveryRequired {
                reason: error.code(),
                sequence: error.sequence(),
            }
        })?;
        Ok(HubStateAuthority {
            store_path: self.path.clone(),
            directory,
            journal: Arc::new(Mutex::new(journal)),
            budget: SharedViewBudget::new(),
            startup_charge: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn acquire_test_authority(&self) -> HubStateStoreResult<HubStateAuthority> {
        self.acquire_authority()
    }

    /// Save startup state through the authority acquired before the initial load.
    /// The caller supplies its exclusive startup revision. This method does not
    /// compare that revision with a concurrent owner; normal updates use
    /// `prepare_shared` and `commit_shared` with the current Host revision.
    pub(crate) fn save_retained_startup_state(
        &self,
        authority: &HubStateAuthority,
        base_revision: u64,
        prior: Option<SharedView<HubState>>,
        candidate: HubState,
    ) -> Result<FileCommitOutcome, FileCommitError> {
        let budget = authority.budget();
        let prepared = self
            .prepare_shared(authority, base_revision, prior, candidate, &budget)
            .map_err(FileCommitError::Preparation)?;
        self.commit_shared(prepared, base_revision)
    }

    /// Load the update base without creating a state file when it is absent.
    pub(crate) fn load_for_update(
        &self,
        authority: &HubStateAuthority,
        config: &HubConfig,
    ) -> HubStateStoreResult<HubState> {
        self.check_authority(authority)?;
        match authority.directory.read_document() {
            Ok(bytes) => decode_hub_state(&bytes),
            Err(StateDirectoryError::DocumentRead(error))
                if error.kind() == io::ErrorKind::NotFound =>
            {
                Ok(HubState::from_config(config))
            }
            Err(error) => Err(map_directory_error(error)),
        }
    }

    /// Update state under one exclusive owner. The caller supplies the current
    /// revision; this helper has no separate revision check between preparation
    /// and commit. Use separate prepare and commit calls to reject stale work.
    pub(crate) fn update_shared(
        &self,
        authority: &HubStateAuthority,
        base_revision: u64,
        prior: SharedView<HubState>,
        budget: &Arc<SharedViewBudget>,
        update: impl FnOnce(&mut HubState),
    ) -> Result<FileCommitOutcome, FileCommitError> {
        let mut state = (*prior).clone();
        update(&mut state);
        let prepared = self
            .prepare_shared(authority, base_revision, Some(prior), state, budget)
            .map_err(FileCommitError::Preparation)?;
        self.commit_shared(prepared, base_revision)
    }

    pub(crate) fn prepare_shared(
        &self,
        authority: &HubStateAuthority,
        base_revision: u64,
        prior: Option<SharedView<HubState>>,
        state: HubState,
        budget: &Arc<SharedViewBudget>,
    ) -> HubStateStoreResult<PreparedHubStateWrite> {
        self.check_authority(authority)?;
        if !Arc::ptr_eq(budget, &authority.budget) {
            return Err(HubStateStoreError::AuthorityMismatch);
        }
        state
            .validate_version()
            .map_err(HubStateStoreError::State)?;
        // The durable pretty JSON is larger than compact JSON. Its existing
        // byte length is therefore one conservative logical view charge.
        let bytes = serde_json::to_vec_pretty(&state).map_err(HubStateStoreError::Serialize)?;
        let charge =
            budget
                .reserve(bytes.len())
                .map_err(|error| HubStateStoreError::ViewCapacity {
                    requested: error.requested,
                    available: error.available,
                })?;
        Ok(self.prepare_shared_with_charge(authority, base_revision, prior, state, bytes, charge))
    }

    fn prepare_shared_with_charge(
        &self,
        authority: &HubStateAuthority,
        base_revision: u64,
        prior: Option<SharedView<HubState>>,
        state: HubState,
        bytes: Vec<u8>,
        charge: SharedViewCharge,
    ) -> PreparedHubStateWrite {
        let view = SharedView::from_reserved(state, charge);
        let evidence = Box::new(FileWriteEvidence {
            candidate: view,
            prior,
            authority: authority.clone_for_write(),
            base_revision,
            committed_revision: base_revision,
            cause: None,
            journal_error: None,
            receipt: None,
        });
        PreparedHubStateWrite { evidence, bytes }
    }

    /// Commit a state document without an earlier external effect.
    pub(crate) fn commit_shared(
        &self,
        prepared: PreparedHubStateWrite,
        current_revision: u64,
    ) -> Result<FileCommitOutcome, FileCommitError> {
        let pending = self.begin_shared_effect(prepared, current_revision, None)?;
        self.commit_shared_effect(pending)
            .map_err(|failure| FileCommitError::BeforePublication {
                error: failure.error,
                prepared: failure.pending.prepared,
            })
    }

    /// Synchronize the one intent before the caller starts an external effect.
    pub(crate) fn begin_shared_effect(
        &self,
        prepared: PreparedHubStateWrite,
        current_revision: u64,
        external: Option<ExternalFileIntent<'_>>,
    ) -> Result<PendingFileCommit, FileCommitError> {
        if current_revision != prepared.evidence.base_revision {
            return Err(FileCommitError::Stale(prepared));
        }
        let Some(revision) = current_revision.checked_add(1) else {
            return Err(FileCommitError::RevisionExhausted(prepared));
        };
        if let Err(error) = self.check_authority(&prepared.evidence.authority) {
            return Err(FileCommitError::BeforePublication { error, prepared });
        }
        let mut prepared = prepared;
        prepared.evidence.committed_revision = revision;
        let metadata = match serde_json::to_vec(&StateWriteIntentMetadata {
            kind: if external.is_some() {
                "state_and_external_file_write"
            } else {
                "state_write"
            },
            host_id: &prepared.evidence.candidate.host.id,
            base_revision: current_revision,
            external_path: external
                .as_ref()
                .map(|external| external.path.as_os_str().as_encoded_bytes()),
        }) {
            Ok(metadata) => metadata,
            Err(error) => {
                return Err(FileCommitError::BeforePublication {
                    error: HubStateStoreError::Serialize(error),
                    prepared,
                });
            }
        };
        let journal_handle = Arc::clone(&prepared.evidence.authority.journal);
        let mut journal = match journal_handle.lock() {
            Ok(journal) => journal,
            Err(_) => {
                return Err(FileCommitError::BeforePublication {
                    error: HubStateStoreError::JournalPoisoned,
                    prepared,
                });
            }
        };
        let intent = match journal.begin(JournalIntent {
            metadata: &metadata,
            candidate_bytes: &prepared.bytes,
            prior_exists: prepared.evidence.prior.is_some(),
            external: external.as_ref().map(|external| JournalExternal {
                prior: external.prior,
                candidate: external.candidate,
            }),
        }) {
            Ok(intent) => intent,
            Err(error) => {
                return Err(FileCommitError::BeforePublication {
                    error: HubStateStoreError::RecoveryRequired {
                        reason: error.code(),
                        sequence: error.sequence(),
                    },
                    prepared,
                });
            }
        };
        prepared.evidence.receipt = Some(intent);
        Ok(PendingFileCommit { prepared })
    }

    /// Finish the same intent after every named external effect has synchronized.
    pub(crate) fn commit_shared_effect(
        &self,
        pending: PendingFileCommit,
    ) -> Result<FileCommitOutcome, FileEffectCommitError> {
        let PendingFileCommit { mut prepared } = pending;
        let revision = prepared.evidence.committed_revision;
        let journal_handle = Arc::clone(&prepared.evidence.authority.journal);
        let mut journal = match journal_handle.lock() {
            Ok(journal) => journal,
            Err(_) => {
                return Err(FileEffectCommitError {
                    error: HubStateStoreError::JournalPoisoned,
                    pending: PendingFileCommit { prepared },
                });
            }
        };
        #[cfg(test)]
        let result = if save_failure_is_due(&self.path) {
            prepared
                .evidence
                .authority
                .directory
                .write_document_with_pre_rename_failure_for_test(&prepared.bytes)
        } else if sync_failure_is_due(&self.path) {
            prepared
                .evidence
                .authority
                .directory
                .write_document_with_sync_failure_for_test(&prepared.bytes)
        } else {
            prepared
                .evidence
                .authority
                .directory
                .write_document(&prepared.bytes)
        };
        #[cfg(not(test))]
        let result = prepared
            .evidence
            .authority
            .directory
            .write_document(&prepared.bytes);
        match result {
            Err(error) => Err(FileEffectCommitError {
                error: map_directory_error(error),
                pending: PendingFileCommit { prepared },
            }),
            Ok(StateDocumentCommit::Synced) => match journal.complete(
                prepared
                    .evidence
                    .receipt
                    .as_ref()
                    .expect("durable intent receipt precedes state write"),
            ) {
                Ok(()) => {
                    prepared.evidence.receipt = None;
                    Ok(FileCommitOutcome::Synced {
                        state: prepared.evidence.candidate.clone(),
                        revision,
                    })
                }
                Err(error) => {
                    prepared.evidence.cause = Some(HubStatePublicationCause::JournalCompletion);
                    prepared.evidence.journal_error = Some(error);
                    Ok(FileCommitOutcome::PublishedUncertain(
                        HubStateUncertainWrite(prepared.evidence),
                    ))
                }
            },
            Ok(StateDocumentCommit::SyncedDirectoryChanged(_)) => {
                prepared.evidence.cause = Some(HubStatePublicationCause::DirectoryChanged);
                Ok(FileCommitOutcome::PublishedUncertain(
                    HubStateUncertainWrite(prepared.evidence),
                ))
            }
            Ok(StateDocumentCommit::RenamedSyncFailed(error)) => {
                prepared.evidence.cause = Some(HubStatePublicationCause::DirectorySync(error));
                Ok(FileCommitOutcome::PublishedUncertain(
                    HubStateUncertainWrite(prepared.evidence),
                ))
            }
        }
    }

    /// Fail the next `save` after writing the temporary file, before rename.
    #[cfg(test)]
    pub fn inject_next_save_failure(data_directory: impl AsRef<Path>) {
        Self::inject_save_failure_after(data_directory, 0);
    }

    /// Allow `successful_saves` durable writes, then fail the next `save`.
    #[cfg(test)]
    pub fn inject_save_failure_after(data_directory: impl AsRef<Path>, successful_saves: u32) {
        save_failures()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                data_directory.as_ref().join(HUB_STATE_FILE_NAME),
                successful_saves,
            );
    }

    #[cfg(test)]
    pub(crate) fn inject_next_directory_sync_failure(data_directory: impl AsRef<Path>) {
        sync_failures()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(data_directory.as_ref().join(HUB_STATE_FILE_NAME));
    }
}

#[cfg(test)]
fn save_failures() -> &'static Mutex<std::collections::BTreeMap<PathBuf, u32>> {
    static FAILURES: OnceLock<Mutex<std::collections::BTreeMap<PathBuf, u32>>> = OnceLock::new();
    FAILURES.get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
}

#[cfg(test)]
fn sync_failures() -> &'static Mutex<std::collections::BTreeSet<PathBuf>> {
    static FAILURES: OnceLock<Mutex<std::collections::BTreeSet<PathBuf>>> = OnceLock::new();
    FAILURES.get_or_init(|| Mutex::new(std::collections::BTreeSet::new()))
}

#[cfg(test)]
fn sync_failure_is_due(path: &Path) -> bool {
    sync_failures()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(path)
}

#[cfg(test)]
fn save_failure_is_due(path: &Path) -> bool {
    let mut failures = save_failures()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let Some(remaining) = failures.get_mut(path) else {
        return false;
    };
    if *remaining == 0 {
        failures.remove(path);
        true
    } else {
        *remaining -= 1;
        false
    }
}

fn map_directory_error(error: StateDirectoryError) -> HubStateStoreError {
    match error {
        StateDirectoryError::Io(error) | StateDirectoryError::DocumentRead(error) => {
            HubStateStoreError::Io(error)
        }
        StateDirectoryError::Owned(path) => HubStateStoreError::Owned(path),
        StateDirectoryError::Replaced => HubStateStoreError::DirectoryChanged,
        StateDirectoryError::Quarantined(_) => HubStateStoreError::Quarantined,
        StateDirectoryError::InvalidTemporaryFile => HubStateStoreError::InvalidTemporaryFile,
        #[cfg(test)]
        StateDirectoryError::InjectedWriteFailure => HubStateStoreError::InjectedWriteFailure,
    }
}

impl HubStateStore for FileHubStateStore {
    fn load_or_initialize(&self, _config: &HubConfig) -> HubStateStoreResult<HubState> {
        Err(HubStateStoreError::AuthorityRequired)
    }

    fn load_retained(
        &self,
        config: &HubConfig,
    ) -> HubStateStoreResult<(HubState, Option<HubStateAuthority>)> {
        let mut authority = self.acquire_authority()?;
        match authority.directory.read_document() {
            Ok(bytes) => {
                let state = decode_hub_state(&bytes)?;
                let logical_bytes = serde_json::to_vec_pretty(&state)
                    .map_err(HubStateStoreError::Serialize)?
                    .len();
                authority.startup_charge =
                    Some(authority.budget.reserve(logical_bytes).map_err(|error| {
                        HubStateStoreError::ViewCapacity {
                            requested: error.requested,
                            available: error.available,
                        }
                    })?);
                Ok((state, Some(authority)))
            }
            Err(StateDirectoryError::DocumentRead(error))
                if error.kind() == io::ErrorKind::NotFound =>
            {
                let state = HubState::from_config(config);
                let bytes =
                    serde_json::to_vec_pretty(&state).map_err(HubStateStoreError::Serialize)?;
                authority.startup_charge =
                    Some(authority.budget.reserve(bytes.len()).map_err(|error| {
                        HubStateStoreError::ViewCapacity {
                            requested: error.requested,
                            available: error.available,
                        }
                    })?);
                let candidate_charge = authority.budget.reserve(bytes.len()).map_err(|error| {
                    HubStateStoreError::ViewCapacity {
                        requested: error.requested,
                        available: error.available,
                    }
                })?;
                let candidate = state.clone();
                let prepared = self.prepare_shared_with_charge(
                    &authority,
                    0,
                    None,
                    candidate,
                    bytes,
                    candidate_charge,
                );
                match self.commit_shared(prepared, 0) {
                    Ok(FileCommitOutcome::Synced { .. }) => Ok((state, Some(authority))),
                    Ok(FileCommitOutcome::PublishedUncertain(write)) => {
                        Err(HubStateStoreError::PublishedUncertain(write))
                    }
                    Err(FileCommitError::Preparation(error))
                    | Err(FileCommitError::BeforePublication { error, .. }) => Err(error),
                    Err(FileCommitError::Stale(_)) => Err(HubStateStoreError::StaleRevision),
                    Err(FileCommitError::RevisionExhausted(_)) => {
                        Err(HubStateStoreError::RevisionExhausted)
                    }
                }
            }
            Err(error) => Err(map_directory_error(error)),
        }
    }

    fn save_exclusive_startup_state(&self, _state: &HubState) -> HubStateStoreResult<()> {
        Err(HubStateStoreError::AuthorityRequired)
    }
}

#[derive(Debug, Deserialize)]
struct HubStateVersion<'a> {
    schema_version: u16,
    #[serde(default, borrow)]
    recovery: Option<&'a RawValue>,
}

fn decode_hub_state(bytes: &[u8]) -> HubStateStoreResult<HubState> {
    let version: HubStateVersion<'_> =
        serde_json::from_slice(bytes).map_err(HubStateStoreError::Corrupt)?;
    if !matches!(version.schema_version, 3 | HUB_STATE_SCHEMA_VERSION) {
        return Err(HubStateStoreError::State(
            HubStateError::UnsupportedVersion(version.schema_version),
        ));
    }
    if version.schema_version == HUB_STATE_SCHEMA_VERSION && version.recovery.is_none() {
        return Err(HubStateStoreError::State(
            HubStateError::InvalidRecoveryState,
        ));
    }
    let mut state: HubState = serde_json::from_slice(bytes).map_err(HubStateStoreError::Corrupt)?;
    if version.schema_version == 3 {
        if state.recovery != crate::recovery::record::RecoveryLedger::default() {
            return Err(HubStateStoreError::State(
                HubStateError::InvalidRecoveryState,
            ));
        }
        state.schema_version = HUB_STATE_SCHEMA_VERSION;
    }
    state
        .validate_version()
        .map_err(HubStateStoreError::State)?;
    Ok(state)
}

#[derive(Deserialize)]
struct HubStatePackageSpan<'a> {
    #[serde(borrow)]
    package_registry: &'a RawValue,
}

/// Typed storage boundary errors.
#[derive(Debug)]
pub enum HubStateStoreError {
    /// A File operation requires the authority returned by a retained load.
    AuthorityRequired,
    /// The authority belongs to another File path or view budget.
    AuthorityMismatch,
    /// The prepared revision no longer matches the owner revision.
    StaleRevision,
    /// The owner revision cannot advance.
    RevisionExhausted,
    /// Another owner holds the state directory.
    Owned(PathBuf),
    /// The retained directory pathname no longer identifies its descriptor.
    DirectoryChanged,
    /// A published write paused further writes through this authority.
    Quarantined,
    /// A durable intent is unresolved or its journal cannot be trusted.
    RecoveryRequired {
        reason: &'static str,
        sequence: Option<u64>,
    },
    /// A poisoned journal owner cannot authorize another state write.
    JournalPoisoned,
    /// A temporary file did not have exclusive regular-file identity.
    InvalidTemporaryFile,
    /// A state write reached rename without a clean commit result.
    PublishedUncertain(HubStateUncertainWrite),
    /// State file path did not have a parent directory.
    MissingParent,
    /// Filesystem error while reading or writing durable state.
    Io(io::Error),
    /// JSON serialization failed before writing state.
    Serialize(serde_json::Error),
    /// JSON parsing failed while loading the committed state file.
    Corrupt(serde_json::Error),
    /// Loaded or saved state failed model validation.
    State(HubStateError),
    /// A candidate view cannot coexist with the retained shared views.
    ViewCapacity { requested: usize, available: usize },
    /// Test-only injected failure between temp-file flush and rename.
    #[cfg(test)]
    InjectedWriteFailure,
}

impl fmt::Display for HubStateStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthorityRequired => {
                formatter.write_str("File state requires retained authority")
            }
            Self::AuthorityMismatch => {
                formatter.write_str("File state authority does not match the store or view budget")
            }
            Self::StaleRevision => formatter.write_str("prepared Hub state revision is stale"),
            Self::RevisionExhausted => formatter.write_str("Hub state revision cannot advance"),
            Self::Owned(path) => write!(
                formatter,
                "state directory {} already has a writer",
                path.display()
            ),
            Self::DirectoryChanged => formatter.write_str("state directory changed during a write"),
            Self::Quarantined => {
                formatter.write_str("state directory writes are paused after uncertain publication")
            }
            Self::RecoveryRequired { reason, sequence } => match sequence {
                Some(sequence) => write!(formatter, "recovery required: {reason} at {sequence}"),
                None => write!(formatter, "recovery required: {reason}"),
            },
            Self::JournalPoisoned => formatter.write_str("recovery journal owner is poisoned"),
            Self::InvalidTemporaryFile => formatter
                .write_str("state temporary file is not an exclusively linked regular file"),
            Self::PublishedUncertain(write) => {
                write!(formatter, "state write reached rename: {:?}", write.cause())
            }
            Self::MissingParent => write!(formatter, "hub state file has no parent directory"),
            Self::Io(error) => write!(formatter, "hub state filesystem error: {error}"),
            Self::Serialize(error) => write!(formatter, "hub state serialization error: {error}"),
            Self::Corrupt(error) => write!(formatter, "hub state file is corrupt: {error}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::ViewCapacity {
                requested,
                available,
            } => write!(
                formatter,
                "shared view needs {requested} logical bytes but only {available} remain"
            ),
            #[cfg(test)]
            Self::InjectedWriteFailure => {
                write!(formatter, "injected hub state write failure before rename")
            }
        }
    }
}

impl Error for HubStateStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PublishedUncertain(write) => match write.cause() {
                HubStatePublicationCause::DirectorySync(error) => Some(error),
                HubStatePublicationCause::DirectoryChanged => None,
                HubStatePublicationCause::JournalCompletion => None,
            },
            Self::Io(error) => Some(error),
            Self::Serialize(error) | Self::Corrupt(error) => Some(error),
            Self::State(error) => Some(error),
            Self::AuthorityRequired
            | Self::AuthorityMismatch
            | Self::StaleRevision
            | Self::RevisionExhausted
            | Self::Owned(_)
            | Self::DirectoryChanged
            | Self::Quarantined
            | Self::RecoveryRequired { .. }
            | Self::JournalPoisoned
            | Self::InvalidTemporaryFile
            | Self::ViewCapacity { .. } => None,
            #[cfg(test)]
            Self::MissingParent | Self::InjectedWriteFailure => None,
            #[cfg(not(test))]
            Self::MissingParent => None,
        }
    }
}

/// Hub state storage result alias.
pub type HubStateStoreResult<T> = Result<T, HubStateStoreError>;

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use botster_core::{
        Capability, CapabilitySurface, CredentialRecord, CredentialStore, CredentialStoreError,
        ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, PackageConfigurationField,
        PackageConfigurationFieldType, PackageConfigurationSchema, PackageConfigurationSecretValue,
        PackageConfigurationValue, PackageSource,
    };

    use super::*;
    use crate::credentials::{
        CredentialKeyPurpose, CredentialProviderKind, TestFileCredentialStore, credential_key_id,
        validate_hub_credentials,
    };
    use crate::shared_view::SharedViewBudget;
    use crate::{
        DataDirectoryOption, HostIdentityOptions, HubPackageManifest, HubStartupOptions,
        PackageProvenance, PackageRegistry, PackageRunnableEntrypoint, PackageRunnableProcessState,
        PackageRunnableWorkingDirectory, RuntimeEnvironment,
    };

    fn test_config(name: &str) -> HubConfig {
        HubStartupOptions {
            host: HostIdentityOptions {
                id: "state-test-host".to_string(),
                display_name: "State Test Host".to_string(),
                fingerprint: None,
            },
            data_directory: DataDirectoryOption::Explicit(unique_test_dir(name)),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build test config")
    }

    fn load_file_state(
        store: &FileHubStateStore,
        config: &HubConfig,
    ) -> HubStateStoreResult<HubState> {
        store.load_retained(config).map(|(state, _authority)| state)
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        PathBuf::from("target")
            .join("botster-hub-test-data")
            .join(name)
            .join(nanos.to_string())
    }

    fn plugin_manifest() -> HubPackageManifest {
        HubPackageManifest {
            name: "workflow.plugin".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Plugin,
            botster: ">=0.1.0".to_string(),
            source: Some(PackageSource::Git {
                repo: "https://example.invalid/botster/workflow-plugin.git".to_string(),
                reference: "v1.0.0".to_string(),
            }),
            capabilities: vec![Capability {
                surface: CapabilitySurface::Surfaces,
                scope: None,
            }],
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
            events: crate::HubPackageEvents::default(),
        }
    }

    fn provenance() -> PackageProvenance {
        PackageProvenance {
            source: "https://example.invalid/botster/package-index".to_string(),
            checksum: Some("sha256:test-checksum".to_string()),
        }
    }

    #[derive(Debug, Default)]
    struct CountingCredentialStore {
        reads: Cell<usize>,
    }

    impl CredentialStore for CountingCredentialStore {
        fn get(&self, _key: &str) -> Result<Option<CredentialRecord>, CredentialStoreError> {
            self.reads.set(self.reads.get() + 1);
            Ok(Some(CredentialRecord::new(vec![41, 43, 47, 53])))
        }

        fn set(
            &mut self,
            _key: &str,
            _record: CredentialRecord,
        ) -> Result<(), CredentialStoreError> {
            Ok(())
        }

        fn delete(&mut self, _key: &str) -> Result<(), CredentialStoreError> {
            Ok(())
        }
    }

    fn configurable_plugin_manifest() -> HubPackageManifest {
        let mut manifest = plugin_manifest();
        manifest.configuration = Some(PackageConfigurationSchema {
            groups: Vec::new(),
            fields: vec![PackageConfigurationField {
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
            }],
        });
        manifest
    }

    #[test]
    fn retained_file_store_owns_load_commit_and_published_uncertainty() {
        let config = test_config("retained-file-store");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        assert!(matches!(
            store.load_or_initialize(&config),
            Err(HubStateStoreError::AuthorityRequired)
        ));
        assert!(matches!(
            store.save_exclusive_startup_state(&HubState::from_config(&config)),
            Err(HubStateStoreError::AuthorityRequired)
        ));
        assert!(!config.data_directory.exists());

        let (state, Some(mut authority)) = store.load_retained(&config).unwrap() else {
            panic!("File load must return its authority");
        };
        assert!(matches!(
            store.load_retained(&config),
            Err(HubStateStoreError::Owned(_))
        ));
        let prior = SharedView::from_reserved(
            state,
            authority.take_startup_charge().expect("startup charge"),
        );
        assert!(authority.take_startup_charge().is_none());
        assert!(authority.budget().used() > 0);
        let mut candidate = (*prior).clone();
        candidate.session_type_generation = 1;
        let prepared = store
            .prepare_shared(&authority, 0, Some(prior), candidate, &authority.budget())
            .unwrap();
        let FileCommitOutcome::Synced {
            state: confirmed,
            revision: 1,
        } = store.commit_shared(prepared, 0).unwrap()
        else {
            panic!("first commit must synchronize");
        };

        let mut uncertain_candidate = (*confirmed).clone();
        uncertain_candidate.session_type_generation = 2;
        let prepared = store
            .prepare_shared(
                &authority,
                1,
                Some(confirmed.clone()),
                uncertain_candidate,
                &authority.budget(),
            )
            .unwrap();
        FileHubStateStore::inject_next_directory_sync_failure(&config.data_directory);
        let FileCommitOutcome::PublishedUncertain(uncertain) =
            store.commit_shared(prepared, 1).unwrap()
        else {
            panic!("rename plus sync failure must preserve owned uncertainty");
        };
        assert!(matches!(
            uncertain.cause(),
            HubStatePublicationCause::DirectorySync(_)
        ));
        assert_eq!(uncertain.base_revision(), 1);
        assert_eq!(uncertain.committed_revision(), 2);
        assert_eq!(uncertain.receipt_sequence(), Some(3));
        assert_eq!(uncertain.prior().unwrap().session_type_generation, 1);
        assert_eq!(uncertain.candidate().session_type_generation, 2);
        assert_eq!(
            decode_hub_state(&authority.directory.read_document().unwrap())
                .unwrap()
                .session_type_generation,
            2
        );
        let document_bytes = authority.directory.read_document().unwrap();
        let journal_bytes = fs::read(config.data_directory.join("hub-recovery.log")).unwrap();

        let mut later = (*confirmed).clone();
        later.session_type_generation = 3;
        let prepared = store
            .prepare_shared(&authority, 1, Some(confirmed), later, &authority.budget())
            .unwrap();
        assert!(matches!(
            store.commit_shared(prepared, 1),
            Err(FileCommitError::BeforePublication {
                error: HubStateStoreError::RecoveryRequired {
                    reason: "recovery_journal_quarantined",
                    sequence: None,
                },
                ..
            })
        ));
        assert_eq!(authority.directory.read_document().unwrap(), document_bytes);
        assert_eq!(
            fs::read(config.data_directory.join("hub-recovery.log")).unwrap(),
            journal_bytes
        );
        assert!(!config.data_directory.join("hub-state.json.tmp").exists());
        drop(uncertain);
        drop(authority);
    }

    #[test]
    fn file_startup_refuses_unresolved_intent_after_directory_sync_failure() {
        let config = test_config("startup-unresolved-intent");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let (state, Some(mut authority)) = store.load_retained(&config).unwrap() else {
            panic!("File load must return its authority");
        };
        assert!(config.data_directory.join("hub-recovery.log").exists());
        let unrelated = config.data_directory.join("unrelated.txt");
        std::fs::write(&unrelated, b"leave this file alone").unwrap();

        let prior = SharedView::from_reserved(
            state,
            authority.take_startup_charge().expect("startup charge"),
        );
        let mut candidate = (*prior).clone();
        candidate.session_type_generation = 1;
        let prepared = store
            .prepare_shared(&authority, 0, Some(prior), candidate, &authority.budget())
            .unwrap();
        FileHubStateStore::inject_next_directory_sync_failure(&config.data_directory);
        let FileCommitOutcome::PublishedUncertain(write) =
            store.commit_shared(prepared, 0).unwrap()
        else {
            panic!("rename with failed directory sync must remain uncertain");
        };
        assert!(matches!(
            write.cause(),
            HubStatePublicationCause::DirectorySync(_)
        ));
        drop(write);
        drop(authority);

        assert!(matches!(
            store.load_retained(&config),
            Err(HubStateStoreError::RecoveryRequired {
                reason: "recovery_intent_unresolved",
                sequence: Some(2),
            })
        ));
        assert_eq!(std::fs::read(&unrelated).unwrap(), b"leave this file alone");
    }

    #[test]
    fn file_startup_refuses_existing_state_without_journal() {
        let config = test_config("startup-legacy-state");
        std::fs::create_dir_all(&config.data_directory).unwrap();
        let state = HubState::from_config(&config);
        std::fs::write(
            config.data_directory.join(HUB_STATE_FILE_NAME),
            serde_json::to_vec_pretty(&state).unwrap(),
        )
        .unwrap();
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        assert!(matches!(
            store.load_retained(&config),
            Err(HubStateStoreError::RecoveryRequired {
                reason: "recovery_journal_missing_for_state",
                sequence: None,
            })
        ));
        assert!(!config.data_directory.join("hub-recovery.log").exists());
    }

    #[test]
    fn retained_file_store_rejects_mismatched_store_and_budget_before_io() {
        let config = test_config("retained-authority-mismatch");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let other_directory = config.data_directory.join("other");
        let other_store = FileHubStateStore::for_data_directory(&other_directory);
        let (state, Some(authority)) = store.load_retained(&config).unwrap() else {
            panic!("File load must return its authority");
        };

        assert!(matches!(
            other_store.prepare_shared(&authority, 0, None, state.clone(), &authority.budget()),
            Err(HubStateStoreError::AuthorityMismatch)
        ));
        assert!(!other_directory.exists());

        let other_budget = SharedViewBudget::new();
        assert!(matches!(
            store.prepare_shared(&authority, 0, None, state, &other_budget),
            Err(HubStateStoreError::AuthorityMismatch)
        ));
        assert!(!store.path().with_extension("json.tmp").exists());
    }

    #[test]
    fn retained_initialization_returns_owned_uncertainty_after_rename() {
        let config = test_config("retained-initial-uncertainty");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        FileHubStateStore::inject_next_directory_sync_failure(&config.data_directory);

        let HubStateStoreError::PublishedUncertain(write) =
            store.load_retained(&config).unwrap_err()
        else {
            panic!("initial rename must return owned uncertainty");
        };
        assert!(matches!(
            write.cause(),
            HubStatePublicationCause::DirectorySync(_)
        ));
        assert!(write.prior().is_none());
        assert_eq!(write.base_revision(), 0);
        assert_eq!(write.committed_revision(), 1);
        assert_eq!(
            decode_hub_state(&write.authority().directory.read_document().unwrap()).unwrap(),
            *write.candidate()
        );
        assert!(matches!(
            store.load_retained(&config),
            Err(HubStateStoreError::Owned(_))
        ));
        drop(write);
    }

    #[test]
    fn retained_startup_save_uses_exclusive_revision_and_normal_commit_checks_current() {
        let config = test_config("retained-startup-save");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let (state, Some(mut authority)) = store.load_retained(&config).unwrap() else {
            panic!("File load must return its authority");
        };
        let prior = SharedView::from_reserved(
            state,
            authority.take_startup_charge().expect("startup charge"),
        );
        let mut candidate = (*prior).clone();
        candidate.session_type_generation = 1;
        let FileCommitOutcome::Synced {
            state: current,
            revision: 1,
        } = store
            .save_retained_startup_state(&authority, 0, Some(prior), candidate)
            .unwrap()
        else {
            panic!("exclusive startup save must synchronize");
        };
        let committed = authority.directory.read_document().unwrap();
        let mut later = (*current).clone();
        later.session_type_generation = 2;
        let prepared = store
            .prepare_shared(&authority, 1, Some(current), later, &authority.budget())
            .unwrap();
        assert!(matches!(
            store.commit_shared(prepared, 0),
            Err(FileCommitError::Stale(_))
        ));
        assert_eq!(authority.directory.read_document().unwrap(), committed);
    }

    #[test]
    fn custom_store_default_retained_load_keeps_existing_contract() {
        struct CustomStore(HubState);

        impl HubStateStore for CustomStore {
            fn load_or_initialize(&self, _config: &HubConfig) -> HubStateStoreResult<HubState> {
                Ok(self.0.clone())
            }

            fn save_exclusive_startup_state(&self, _state: &HubState) -> HubStateStoreResult<()> {
                Ok(())
            }
        }

        let config = test_config("custom-retained-default");
        let expected = HubState::from_config(&config);
        let store = CustomStore(expected.clone());
        let (loaded, authority) = store.load_retained(&config).unwrap();
        assert_eq!(loaded, expected);
        assert!(authority.is_none());
        store.save_exclusive_startup_state(&loaded).unwrap();
    }

    #[test]
    fn file_store_creates_and_loads_default_v2_state() {
        let config = test_config("creates-default");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);

        let state = load_file_state(&store, &config).expect("initialize default state");
        let reopened = load_file_state(&store, &config).expect("load committed state");

        assert_eq!(state.schema_version, HUB_STATE_SCHEMA_VERSION);
        assert_eq!(reopened, state);
        assert_eq!(reopened.host.id, "state-test-host");
        assert_eq!(
            reopened.runtime_settings.data_directory,
            config.data_directory
        );
    }

    #[test]
    fn file_store_loads_current_state_with_defaulted_optional_collections() {
        let config = test_config("loads-v2-without-session-type-sources");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let state = HubState::from_config(&config);
        let mut value = serde_json::to_value(&state).expect("serialize state value");
        let object = value.as_object_mut().expect("state serializes as object");
        object.remove("device_session_type_sources");
        object.insert(
            "spawn_targets".to_string(),
            serde_json::json!([{
                "target_id": "legacy-target",
                "root": "."
            }]),
        );
        object.insert(
            "worktrees".to_string(),
            serde_json::json!([{
                "worktree_id": "legacy-worktree",
                "target_id": "legacy-target",
                "path": "."
            }]),
        );
        object.remove("credential_keys");
        object.remove("trusted_browser_identities");
        object.remove("bootstrap_grants");
        fs::create_dir_all(&config.data_directory).expect("create data dir");
        fs::write(
            store.path(),
            serde_json::to_vec_pretty(&value).expect("serialize legacy-shaped state"),
        )
        .expect("write legacy-shaped state");

        let reopened = load_file_state(&store, &config)
            .expect("load current state with omitted optional collections");

        assert!(reopened.device_session_type_sources.is_empty());
        assert_eq!(reopened.spawn_targets.len(), 1);
        assert_eq!(reopened.spawn_targets[0].kind, "directory");
        assert_eq!(reopened.spawn_targets[0].base_ref, None);
        assert_eq!(reopened.worktrees.len(), 1);
        assert_eq!(reopened.worktrees[0].management, "registered");
        assert!(reopened.credential_keys.is_empty());
        assert!(reopened.trusted_browser_identities.is_empty());
        assert!(reopened.bootstrap_grants.is_empty());
        assert_eq!(reopened.schema_version, HUB_STATE_SCHEMA_VERSION);
    }

    #[test]
    fn file_store_rejects_v2_before_deserializing_cold_cut_session_types() {
        let config = test_config("rejects-v2-session-types");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let mut value =
            serde_json::to_value(HubState::from_config(&config)).expect("serialize current state");
        let object = value.as_object_mut().expect("state object");
        object.insert("schema_version".to_string(), serde_json::json!(2));
        object.remove("device_session_type_sources");
        object.remove("session_type_generation");
        object.insert(
            "device_session_template_sources".to_string(),
            serde_json::json!([{"root": ".", "session_templates": []}]),
        );
        fs::create_dir_all(&config.data_directory).expect("create data dir");
        fs::write(
            store.path(),
            serde_json::to_vec_pretty(&value).expect("serialize v2 state"),
        )
        .expect("write v2 state");

        assert!(matches!(
            load_file_state(&store, &config),
            Err(HubStateStoreError::State(
                HubStateError::UnsupportedVersion(2)
            ))
        ));
    }

    #[test]
    fn file_store_rejects_v1_before_deserializing_legacy_core_engine_options() {
        let config = test_config("rejects-v1-core-engine-options");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let mut value =
            serde_json::to_value(HubState::from_config(&config)).expect("serialize current state");
        let object = value.as_object_mut().expect("state object");
        object.insert("schema_version".to_string(), serde_json::json!(1));
        let core_engine = object
            .get_mut("runtime_settings")
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|runtime_settings| runtime_settings.get_mut("core_engine"))
            .and_then(serde_json::Value::as_object_mut)
            .expect("core engine object");
        let queue_capacity = core_engine
            .remove("plugin_worker_queue_capacity")
            .expect("current queue capacity");
        core_engine.remove("plugin_worker_executor_concurrency");
        core_engine.insert("plugin_worker_capacity".to_string(), queue_capacity);
        fs::create_dir_all(&config.data_directory).expect("create data dir");
        fs::write(
            store.path(),
            serde_json::to_vec_pretty(&value).expect("serialize v1 state"),
        )
        .expect("write v1 state");

        assert!(matches!(
            load_file_state(&store, &config),
            Err(HubStateStoreError::State(
                HubStateError::UnsupportedVersion(1)
            ))
        ));
    }

    #[test]
    fn browser_trust_metadata_survives_restart_without_raw_secret_material() {
        let config = test_config("browser-trust-metadata");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let public_key = b"synthetic browser public key".to_vec();
        let key_id = credential_key_id(
            &config.host.id,
            CredentialKeyPurpose::BrowserIdentity,
            "browser-a",
        );

        store
            .update_test_fixture(&config, |state| {
                state.credential_keys.push(CredentialKeyReference {
                    key_id: key_id.clone(),
                    provider: CredentialProviderKind::TestFile,
                    purpose: CredentialKeyPurpose::BrowserIdentity,
                    created_at_unix_ms: 10,
                    rotated_at_unix_ms: None,
                });
                let mut browser = TrustedBrowserIdentity::trusted(
                    "browser-a",
                    public_key.clone(),
                    11,
                    "trust synthetic browser",
                );
                browser.credential_key_id = Some(key_id.clone());
                state.trusted_browser_identities.push(browser);
                state.bootstrap_grants.push(BootstrapGrantRecord {
                    grant_id: "grant-a".to_string(),
                    package_instance_id: "package-instance-a".to_string(),
                    origin: "localhost".to_string(),
                    peer_id: "peer-a".to_string(),
                    credential_key_id: Some(key_id.clone()),
                    expires_at_unix_ms: 100,
                    revoked_at_unix_ms: None,
                    redeemed_at_unix_ms: None,
                    audit_reason: "issue synthetic bootstrap grant".to_string(),
                });
            })
            .expect("persist browser trust state");

        let mut credential_store =
            TestFileCredentialStore::new(config.data_directory.join("test-credentials.json"));
        credential_store
            .set(&key_id, CredentialRecord::new(vec![7, 11, 13, 17]))
            .expect("persist test credential outside hub-state");

        let raw_state = fs::read_to_string(store.path()).expect("read hub state");
        assert!(raw_state.contains("browser-a"));
        assert!(raw_state.contains(&key_id));
        assert!(!raw_state.contains("[7,11,13,17]"));
        assert!(!raw_state.contains("[7, 11, 13, 17]"));
        assert!(!raw_state.contains("grant-token"));
        assert!(!raw_state.contains("private key"));
        assert!(!raw_state.contains("write_only"));
        assert!(!raw_state.contains(concat!("/", "Users", "/")));
        assert!(!raw_state.contains("@example.com"));

        let reopened = load_file_state(&store, &config).expect("load browser trust metadata");
        assert_eq!(reopened.credential_keys.len(), 1);
        assert_eq!(reopened.trusted_browser_identities.len(), 1);
        assert_eq!(reopened.bootstrap_grants.len(), 1);
        assert!(reopened.trusted_browser_identities[0].is_trusted_at(50));
        assert!(reopened.bootstrap_grants[0].is_redeemable_at(50));
        validate_hub_credentials(
            &reopened,
            CredentialProviderKind::TestFile,
            &credential_store,
        )
        .expect("credential references resolve through explicit test store");
    }

    #[test]
    fn revoked_or_expired_browser_identities_and_grants_are_denied() {
        let public_key = b"revoked browser public key".to_vec();
        let mut revoked = TrustedBrowserIdentity::trusted(
            "browser-revoked",
            public_key.clone(),
            10,
            "trust synthetic browser",
        );
        revoked.revoked_at_unix_ms = Some(20);
        assert!(!revoked.is_trusted_at(30));

        let mut expired = TrustedBrowserIdentity::trusted(
            "browser-expired",
            public_key,
            10,
            "trust synthetic browser",
        );
        expired.expires_at_unix_ms = Some(20);
        assert!(!expired.is_trusted_at(20));
        assert!(!expired.is_trusted_at(21));

        let valid = BootstrapGrantRecord {
            grant_id: "grant-valid".to_string(),
            package_instance_id: "package-instance".to_string(),
            origin: "localhost".to_string(),
            peer_id: "peer".to_string(),
            credential_key_id: None,
            expires_at_unix_ms: 30,
            revoked_at_unix_ms: None,
            redeemed_at_unix_ms: None,
            audit_reason: "grant synthetic local bootstrap".to_string(),
        };
        assert!(valid.is_redeemable_at(29));

        let mut redeemed = valid.clone();
        redeemed.redeemed_at_unix_ms = Some(25);
        assert!(!redeemed.is_redeemable_at(26));

        let mut revoked_grant = valid.clone();
        revoked_grant.revoked_at_unix_ms = Some(25);
        assert!(!revoked_grant.is_redeemable_at(26));
        assert!(!valid.is_redeemable_at(30));
    }

    #[test]
    fn browser_and_grant_references_do_not_re_read_validated_credential_keys() {
        let config = test_config("browser-grant-no-duplicate-reads");
        let key_id = credential_key_id(
            &config.host.id,
            CredentialKeyPurpose::BrowserIdentity,
            "browser-a",
        );
        let public_key = b"browser reference public key".to_vec();
        let mut state = HubState::from_config(&config);
        state.credential_keys.push(CredentialKeyReference {
            key_id: key_id.clone(),
            provider: CredentialProviderKind::TestFile,
            purpose: CredentialKeyPurpose::BrowserIdentity,
            created_at_unix_ms: 10,
            rotated_at_unix_ms: None,
        });
        let mut browser =
            TrustedBrowserIdentity::trusted("browser-a", public_key, 10, "trust synthetic browser");
        browser.credential_key_id = Some(key_id.clone());
        state.trusted_browser_identities.push(browser);
        state.bootstrap_grants.push(BootstrapGrantRecord {
            grant_id: "grant-a".to_string(),
            package_instance_id: "package-instance-a".to_string(),
            origin: "localhost".to_string(),
            peer_id: "peer-a".to_string(),
            credential_key_id: Some(key_id),
            expires_at_unix_ms: 100,
            revoked_at_unix_ms: None,
            redeemed_at_unix_ms: None,
            audit_reason: "issue synthetic bootstrap grant".to_string(),
        });
        let credential_store = CountingCredentialStore::default();

        validate_hub_credentials(&state, CredentialProviderKind::TestFile, &credential_store)
            .expect("state credential references should validate");

        assert_eq!(
            credential_store.reads.get(),
            1,
            "one credential key should be read once even when browser and grant reference it"
        );
    }

    #[test]
    fn file_store_persists_package_registry_and_capability_grants_across_reopen() {
        let config = test_config("registry-grants");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let grant = Capability {
            surface: CapabilitySurface::Surfaces,
            scope: None,
        };
        let mut registry = PackageRegistry::new(vec![grant.clone()].into_iter().collect());
        let mut manifest = plugin_manifest();
        manifest.surfaces = vec![botster_ui_contract::PackageSurfaceDescriptor {
            id: "workflow.home".to_string(),
            kind: botster_ui_contract::PackageSurfaceKind::App,
            title: "Workflow".to_string(),
            description: None,
            icon: None,
            order: None,
            category: None,
            supports: vec![botster_ui_contract::PackageSurfaceOperation::Render],
        }];
        manifest.navigation = vec![botster_ui_contract::PackageNavigationEntry {
            id: "workflow.home".to_string(),
            label: "Workflow".to_string(),
            icon: None,
            description: None,
            target: botster_ui_contract::PackageNavigationTarget::Surface {
                surface_id: "workflow.home".to_string(),
            },
        }];
        registry
            .install(manifest, provenance(), "install synthetic package")
            .expect("install package");
        registry
            .enable("workflow.plugin", "enable synthetic package")
            .expect("enable package");

        store
            .update_test_fixture(&config, |state| {
                state.package_registry = registry.snapshot();
                state.capability_grants.push(CapabilityGrantRecord {
                    subject: "workflow.plugin".to_string(),
                    capability: grant.clone(),
                    governed_surface: CapabilitySurface::Surfaces,
                    audit_reason: "grant synthetic surface capability".to_string(),
                });
            })
            .expect("persist registry state");

        let reopened_store = FileHubStateStore::for_data_directory(&config.data_directory);
        let reopened = load_file_state(&reopened_store, &config).expect("load registry state");

        assert_eq!(reopened.package_registry.records.len(), 1);
        assert_eq!(reopened.schema_version, HUB_STATE_SCHEMA_VERSION);
        assert!(reopened.package_registry.records[0].is_enabled());
        assert_eq!(
            reopened.package_registry.records[0].manifest.surfaces[0].id,
            "workflow.home"
        );
        assert_eq!(
            reopened.package_registry.records[0].manifest.navigation[0].id,
            "workflow.home"
        );
        assert_eq!(reopened.capability_grants.len(), 1);
        assert_eq!(
            reopened.package_registry.granted_capabilities,
            vec![Capability {
                surface: CapabilitySurface::Surfaces,
                scope: None,
            }]
        );
    }

    #[test]
    fn package_configuration_redacted_secret_marker_persists_without_raw_secret() {
        let config = test_config("package-configuration-redaction");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let grant = Capability {
            surface: CapabilitySurface::Surfaces,
            scope: None,
        };
        let mut registry = PackageRegistry::new(vec![grant].into_iter().collect());
        registry
            .install(
                configurable_plugin_manifest(),
                provenance(),
                "install configurable package",
            )
            .expect("install package");
        registry
            .set_configuration(
                "workflow.plugin",
                BTreeMap::from([(
                    "api_token".to_string(),
                    PackageConfigurationValue::Secret {
                        state: PackageConfigurationSecretValue::WriteOnly,
                    },
                )]),
                "set secret",
            )
            .expect("set package configuration");

        store
            .update_test_fixture(&config, |state| {
                state.package_registry = registry.snapshot();
            })
            .expect("persist state");

        let raw_state = fs::read_to_string(store.path()).expect("read hub state");
        assert!(raw_state.contains("\"state\": \"redacted\""));
        assert!(!raw_state.contains("write_only"));
        assert!(!raw_state.contains("super-secret-token"));

        let reopened = load_file_state(&store, &config).expect("reopen state");
        let restored =
            PackageRegistry::from_snapshot(reopened.package_registry).expect("restore registry");
        let view = restored
            .package("workflow.plugin")
            .expect("restored package")
            .configuration_view();
        assert!(matches!(
            view.effective_values.get("api_token"),
            Some(PackageConfigurationValue::Secret {
                state: PackageConfigurationSecretValue::Redacted
            })
        ));
    }

    #[test]
    fn file_store_persists_runnable_entrypoints_in_package_registry() {
        let config = test_config("registry-runnable-entrypoints");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let grant = Capability {
            surface: CapabilitySurface::Surfaces,
            scope: None,
        };
        let mut registry = PackageRegistry::new(vec![grant].into_iter().collect());
        registry
            .install(plugin_manifest(), provenance(), "install synthetic package")
            .expect("install package");
        let mut snapshot = registry.snapshot();
        snapshot.records[0].runnable_entrypoints = vec![PackageRunnableEntrypoint {
            id: "web".to_string(),
            kind: botster_core::RunnableEntrypointKind::WebApp,
            launch_mode: botster_core::RunnableEntrypointLaunchMode::Background,
            command: "bin/botster-web".to_string(),
            args: vec!["--host".to_string(), "127.0.0.1".to_string()],
            working_directory: PackageRunnableWorkingDirectory::PackageRoot,
            injections: Vec::new(),
            environment: Vec::new(),
            capabilities: Vec::new(),
            readiness: None,
            may_supervise: true,
            process: Default::default(),
        }];

        store
            .update_test_fixture(&config, |state| {
                state.package_registry = snapshot;
            })
            .expect("persist runnable entrypoint state");

        let reopened = load_file_state(&store, &config).expect("load runnable entrypoint state");
        let entrypoint = &reopened.package_registry.records[0].runnable_entrypoints[0];

        assert_eq!(entrypoint.id, "web");
        assert_eq!(entrypoint.args, ["--host", "127.0.0.1"]);
        assert!(entrypoint.may_supervise);
        assert_eq!(
            entrypoint.process.state,
            PackageRunnableProcessState::NotStarted
        );
    }

    #[test]
    fn file_store_updates_state_atomically() {
        let config = test_config("atomic");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let (original, Some(mut authority)) = store.load_retained(&config).unwrap() else {
            panic!("File load must return its authority");
        };
        let prior = SharedView::from_reserved(
            original.clone(),
            authority.take_startup_charge().expect("startup charge"),
        );
        let mut next = original.clone();
        next.audit_history.push(HubAuditEntry {
            recorded_at: "2026-01-01T00:00:00Z".to_string(),
            actor: "test-operator".to_string(),
            action: "mutate".to_string(),
            reason: "prove interrupted write preserves old state".to_string(),
        });

        let prepared = store
            .prepare_shared(&authority, 0, Some(prior), next, &authority.budget())
            .unwrap();
        FileHubStateStore::inject_next_save_failure(&config.data_directory);
        assert!(matches!(
            store.commit_shared(prepared, 0),
            Err(FileCommitError::BeforePublication {
                error: HubStateStoreError::InjectedWriteFailure,
                ..
            })
        ));
        drop(authority);

        let reopened = load_file_state(&store, &config)
            .expect("old state still loads after interrupted write");
        assert_eq!(reopened, original);
    }

    #[test]
    fn file_store_rejects_corrupt_state_file() {
        let config = test_config("corrupt");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        fs::create_dir_all(&config.data_directory).expect("create test data dir");
        fs::write(store.path(), b"{not json").expect("write corrupt state");

        let error = load_file_state(&store, &config).expect_err("corrupt state should fail");

        assert!(matches!(error, HubStateStoreError::Corrupt(_)));
    }

    #[test]
    fn file_store_rejects_unknown_schema_version() {
        let config = test_config("unknown-version");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let mut state = HubState::from_config(&config);
        state.schema_version = 99;
        fs::create_dir_all(&config.data_directory).expect("create test data dir");
        fs::write(
            store.path(),
            serde_json::to_vec_pretty(&state).expect("serialize unsupported state"),
        )
        .expect("write unsupported state");

        let error = load_file_state(&store, &config).expect_err("unsupported version should fail");

        assert!(matches!(
            error,
            HubStateStoreError::State(HubStateError::UnsupportedVersion(99))
        ));
    }

    #[test]
    fn shared_view_capacity_failure_preserves_the_committed_file() {
        let config = test_config("shared-view-capacity");
        let store = FileHubStateStore::for_data_directory(&config.data_directory);
        let (initial, Some(mut authority)) = store.load_retained(&config).unwrap() else {
            panic!("File load must return its authority");
        };
        let committed = fs::read(store.path()).expect("read initial state");
        drop(authority.take_startup_charge());
        let budget = SharedViewBudget::with_capacity(1);
        authority.budget = Arc::clone(&budget);
        let mut candidate = initial;
        candidate.session_type_generation = 1;

        let error = store
            .prepare_shared(&authority, 0, None, candidate, &budget)
            .expect_err("candidate view must exceed one byte");

        assert!(matches!(error, HubStateStoreError::ViewCapacity { .. }));
        assert_eq!(
            fs::read(store.path()).expect("read preserved state"),
            committed
        );
    }

    #[test]
    fn prepared_state_reuses_the_package_registry_span_for_its_charge() {
        let config = test_config("package-registry-span");
        let state = HubState::from_config(&config);
        let bytes = serde_json::to_vec_pretty(&state).expect("serialize state");
        let registry_bytes = serde_json::from_slice::<HubStatePackageSpan>(&bytes)
            .expect("find registry value")
            .package_registry
            .get()
            .len();
        let compact_registry_bytes = serde_json::to_vec(&state.package_registry)
            .expect("serialize registry")
            .len();

        assert!(registry_bytes >= compact_registry_bytes);
        assert!(registry_bytes < bytes.len());
    }

    #[test]
    fn docs_and_fixture_state_do_not_contain_pii_markers() {
        let config = test_config("pii");
        let state = HubState::from_config(&config);
        let json = serde_json::to_string(&state).expect("serialize state");

        assert!(!json.contains(concat!("/", "Users", "/")));
        assert!(!json.contains("@example.com"));
        assert!(!json.contains("/home/"));
    }
}
