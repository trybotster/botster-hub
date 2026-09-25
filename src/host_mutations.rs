//! Typed host-side bodies for durable Hub reads and mutations.

use std::collections::BTreeMap;
use std::io;
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

use botster_core::{
    PackageConfigurationValue, PackageSource, RunnableEntrypointKind, RunnableEntrypointLaunchMode,
};
use botster_hub_client::{
    DaemonAvailablePackage, DaemonCapability, DaemonEvent, DaemonPackageCompatibility,
    DaemonPackageDiagnostic, DaemonPackageInstallEffect, DaemonPackageInstallPlan,
    DaemonPackagePin, DaemonPackageUpdateStatus, DaemonRequest, DaemonResolvedAppLaunch,
    DaemonResponse, DaemonResponseKind, MAX_CONTROL_RESPONSE_BYTES,
};

use crate::client_api::HubClientPackage;
use crate::client_api_dto::package::{
    daemon_package_decision_from_policy, daemon_package_pin_from_policy,
    package_classification_label, package_compatibility_label, package_pin_from_daemon,
    registry_source_kind_label, update_status_actions,
};
use crate::client_api_dto::response::{
    daemon_apps, daemon_available_packages, daemon_package_install_plan, daemon_package_navigation,
    daemon_package_update_status, daemon_packages, daemon_resolved_app_launch,
    daemon_resolved_package_route, daemon_resolved_session_type, daemon_session_type_definition,
    daemon_session_types, daemon_spawn_target_validation, daemon_spawn_targets, daemon_worktrees,
};
use crate::client_api_dto::session::{
    session_type_definition_from_daemon, session_type_mutation_source_from_daemon,
    session_type_request_from_daemon,
};
use crate::client_api_dto::workspace::worktree_lifecycle_event;
use crate::config::HubConfig;
use crate::daemon::control::session_types::{
    ensure_repo_session_types_valid_for_enabled_root, is_invalid_repo_session_types_error,
    session_type_catalog_entities,
};
use crate::daemon::error::DaemonTransportError;
use crate::daemon::error::{daemon_app_launch_error, daemon_package_route_error};
use crate::daemon_projection::{
    apps_from_registry, package_route_descriptors, package_state_label,
    runnable_entrypoint_kind_label, runnable_launch_mode_label,
};
use crate::entrypoint_supervisor::{EntrypointProcessSnapshot, EntrypointSupervisor};
use crate::host_executor::HOST_PREPARED_BYTE_CAPACITY;
use crate::packages::{
    PackageAction, PackageAdmissionReason, PackageDecision, PackageRegistryError, PackageState,
};
use crate::persistence::{
    ExternalFileIntent, FileCommitError, FileCommitOutcome, FileHubStateStore, HubState,
    HubStateAuthority, HubStateStoreError, HubStateUncertainWrite, PendingFileCommit,
    PreparedHubStateWrite,
};
use crate::runtime::package_effect::{HostPackageCleanup, HostPackageRuntime};
use crate::session_types::{
    PackageSessionType, RepoSessionTypeFileCommit, RepoSessionTypeFileSnapshot, SessionTypeError,
    SessionTypeMutation, SessionTypeMutationSource, commit_repo_session_type_bytes,
    encode_repo_session_type_bytes, list_session_types_with_staged_repo,
    restore_repo_session_type_file, snapshot_repo_session_type_file,
};
use crate::shared_view::SharedView;
use crate::{
    PackageRegistry, SpawnTargetCreate, SpawnTargetError, SpawnTargetUpdate, WorktreeCreate,
    resolve_foreground_launch_contract,
};

/// Execute one owned host mutation command.
pub(crate) fn execute(
    command: HostMutationCommand,
    entrypoints: Option<&mut EntrypointSupervisor>,
) -> HostMutationResult {
    let result = match command {
        HostMutationCommand::ApplyPackageEffect(effect) => {
            return execute_package_effect(
                effect,
                entrypoints.expect("package effect has the host supervisor"),
            );
        }
        HostMutationCommand::RestorePackageRuntime(restore) => {
            return execute_package_runtime_restore(
                restore,
                entrypoints.expect("package restore has the host supervisor"),
            );
        }
        HostMutationCommand::ValidateBootstrap {
            request,
            base_revision,
            packages,
        } => {
            let DaemonRequest::IssueLocalWebrtcBootstrap {
                package_name,
                entrypoint_id,
                origin,
            } = request
            else {
                unreachable!("bootstrap validation receives a bootstrap request")
            };
            match crate::daemon::control::webrtc::validate_local_webrtc_bootstrap(
                &packages,
                entrypoints.expect("bootstrap validation has the host supervisor"),
                &package_name,
                &entrypoint_id,
                &origin,
            ) {
                Ok(origin) => {
                    return HostMutationResult::BootstrapReady {
                        base_revision,
                        origin,
                    };
                }
                Err(response) => HostReply::try_new(response).map(HostMutationResult::ReadReady),
            }
        }
        HostMutationCommand::Read(read) => {
            execute_read(read, entrypoints).map(HostMutationResult::ReadReady)
        }
        HostMutationCommand::Prepare(prepare) => {
            execute_prepare(prepare, entrypoints).map(HostMutationResult::Prepared)
        }
        HostMutationCommand::Commit(commit) => return execute_commit(commit),
        HostMutationCommand::Recover(recover) => {
            Ok(HostMutationResult::Recovered(execute_recovery(recover)))
        }
        HostMutationCommand::RestorePackage(restore) => return execute_package_restore(restore),
    };
    result.unwrap_or_else(HostMutationResult::Failed)
}

/// One family-specific host command.
pub(crate) enum HostMutationCommand {
    ApplyPackageEffect(HostPackageEffect),
    RestorePackageRuntime(HostPackageRuntimeRestore),
    ValidateBootstrap {
        request: DaemonRequest,
        base_revision: u64,
        packages: SharedView<PackageRegistry>,
    },
    Read(HostRead),
    Prepare(HostPrepare),
    Commit(HostCommit),
    Recover(HostRecover),
    RestorePackage(HostPackageRestore),
}

impl HostMutationCommand {
    pub(crate) fn uses_entrypoints(&self) -> bool {
        matches!(
            self,
            Self::ApplyPackageEffect(_)
                | Self::RestorePackageRuntime(_)
                | Self::ValidateBootstrap { .. }
                | Self::Read(HostRead::Package { .. } | HostRead::Entrypoint { .. })
                | Self::Prepare(HostPrepare::Package { .. })
        )
    }
}

impl std::fmt::Debug for HostMutationCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::ApplyPackageEffect(_) => "ApplyPackageEffect",
            Self::RestorePackageRuntime(_) => "RestorePackageRuntime",
            Self::ValidateBootstrap { .. } => "ValidateBootstrap",
            Self::Read(HostRead::Entrypoint { .. }) => "Entrypoint",
            Self::Read(HostRead::Package { .. }) => "ReadPackage",
            Self::Read(HostRead::SpawnTarget { .. }) => "ReadSpawnTarget",
            Self::Read(HostRead::SessionType { .. }) => "ReadSessionType",
            Self::Prepare(HostPrepare::Package { .. }) => "PreparePackage",
            Self::Prepare(HostPrepare::SpawnTarget { .. }) => "PrepareSpawnTarget",
            Self::Prepare(HostPrepare::SessionType { .. }) => "PrepareSessionType",
            Self::Prepare(HostPrepare::ManagedWorktree { .. }) => "PrepareManagedWorktree",
            Self::Prepare(HostPrepare::RemoveManagedWorktree { .. }) => {
                "PrepareRemoveManagedWorktree"
            }
            Self::Commit(_) => "Commit",
            Self::Recover(_) => "Recover",
            Self::RestorePackage(_) => "RestorePackage",
        };
        formatter.write_str(name)
    }
}

/// Owned inputs for a host read.
pub(crate) enum HostRead {
    Entrypoint {
        request: DaemonRequest,
        config: HubConfig,
        packages: SharedView<PackageRegistry>,
    },
    Package {
        request: DaemonRequest,
        config: HubConfig,
        packages: SharedView<PackageRegistry>,
    },
    SpawnTarget {
        request: DaemonRequest,
        state: SharedView<HubState>,
    },
    SessionType {
        request: DaemonRequest,
        config: HubConfig,
        state: SharedView<HubState>,
        packages: SharedView<PackageRegistry>,
    },
}

/// Owned inputs for mutation preparation.
pub(crate) enum HostPrepare {
    Package {
        request: DaemonRequest,
        base_revision: u64,
        authority: Arc<HubStateAuthority>,
        state: SharedView<HubState>,
        packages: SharedView<PackageRegistry>,
        data_directory: PathBuf,
    },
    ManagedWorktree {
        worktree: crate::Worktree,
        base_revision: u64,
        authority: Arc<HubStateAuthority>,
        state: SharedView<HubState>,
        data_directory: PathBuf,
        superseded: Option<Box<PreparedMutation>>,
    },
    RemoveManagedWorktree {
        worktree_id: String,
        base_revision: u64,
        authority: Arc<HubStateAuthority>,
        state: SharedView<HubState>,
        data_directory: PathBuf,
        superseded: Option<Box<PreparedMutation>>,
    },
    SpawnTarget {
        request: DaemonRequest,
        base_revision: u64,
        authority: Arc<HubStateAuthority>,
        state: SharedView<HubState>,
        packages: SharedView<PackageRegistry>,
        data_directory: PathBuf,
    },
    SessionType {
        request: DaemonRequest,
        base_revision: u64,
        authority: Arc<HubStateAuthority>,
        config: HubConfig,
        state: SharedView<HubState>,
        packages: SharedView<PackageRegistry>,
        data_directory: PathBuf,
    },
}

/// An admitted commit that owns its prepared state.
pub(crate) struct HostCommit {
    pub(crate) prepared: PreparedMutation,
}

/// A typed recovery request retained across a failed commit.
pub(crate) struct HostRecover {
    pub(crate) rollback: RollbackDescriptor,
    pub(crate) failure: HostMutationError,
}

/// A durable package rollback after an owner-only runtime effect failed.
pub(crate) struct HostPackageRestore {
    pub(crate) base_revision: u64,
    pub(crate) authority: Arc<HubStateAuthority>,
    pub(crate) current_state: SharedView<HubState>,
    pub(crate) previous_state: SharedView<HubState>,
    pub(crate) previous_packages: SharedView<PackageRegistry>,
    pub(crate) data_directory: PathBuf,
}

/// The views restored by one durable package rollback.
pub(crate) struct RestoredPackageView {
    pub(crate) view: SharedView<HubState>,
    pub(crate) packages: SharedView<PackageRegistry>,
}

/// A committed package response that needs current entrypoint snapshots.
pub(crate) struct HostPackageFinalize {
    pub(crate) reply: HostReply,
}

pub(crate) struct HostPackageEffect {
    pub(crate) effect: PackageRuntimeEffect,
    pub(crate) runtime: HostPackageRuntime,
    pub(crate) config: HubConfig,
    pub(crate) packages: SharedView<PackageRegistry>,
    pub(crate) reply: HostReply,
}

pub(crate) struct HostPackageRuntimeRestore {
    pub(crate) effect: PackageRuntimeEffect,
    pub(crate) original: DaemonTransportError,
    pub(crate) runtime: HostPackageRuntime,
    pub(crate) config: HubConfig,
}

/// One typed host result.
pub(crate) enum HostMutationResult {
    PackageEffectApplied {
        reply: Result<HostReply, HostMutationError>,
        cleanup: HostPackageCleanup,
    },
    PackageEffectFailed {
        effect: PackageRuntimeEffect,
        error: DaemonTransportError,
        cleanup: HostPackageCleanup,
    },
    PackageRuntimeRestored {
        effect: PackageRuntimeEffect,
        original: DaemonTransportError,
        rollbacks: Vec<crate::daemon::error::PackageRollbackFailure>,
        cleanup: HostPackageCleanup,
    },
    BootstrapReady {
        base_revision: u64,
        origin: String,
    },
    ReadReady(HostReply),
    Prepared(PreparedMutation),
    Committed(CommittedView),
    PublishedUncertain {
        write: HubStateUncertainWrite,
        rollback: Option<RollbackDescriptor>,
    },
    ExternalEffectUncertain {
        pending: PendingFileCommit,
        rollback: RollbackDescriptor,
        cause: ExternalEffectCause,
    },
    Recovered(RecoveryOutcome),
    PackageRestored(RestoredPackageView),
    PackageRestoreFailed(crate::HubStateStoreError),
    Failed(HostMutationError),
}

impl std::fmt::Debug for HostMutationResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PackageEffectApplied { .. } => formatter.write_str("PackageEffectApplied(..)"),
            Self::PackageEffectFailed { .. } => formatter.write_str("PackageEffectFailed(..)"),
            Self::PackageRuntimeRestored { .. } => {
                formatter.write_str("PackageRuntimeRestored(..)")
            }
            Self::BootstrapReady { .. } => formatter.write_str("BootstrapReady(..)"),
            Self::ReadReady(reply) => formatter
                .debug_tuple("ReadReady")
                .field(&reply.logical_bytes)
                .finish(),
            Self::Prepared(prepared) => formatter
                .debug_struct("Prepared")
                .field("base_revision", &prepared.base_revision)
                .field("logical_bytes", &prepared.logical_bytes)
                .finish_non_exhaustive(),
            Self::Committed(committed) => formatter
                .debug_struct("Committed")
                .field("committed_revision", &committed.committed_revision)
                .finish_non_exhaustive(),
            Self::PublishedUncertain { write, .. } => formatter
                .debug_tuple("PublishedUncertain")
                .field(write)
                .finish(),
            Self::ExternalEffectUncertain { cause, .. } => formatter
                .debug_tuple("ExternalEffectUncertain")
                .field(cause)
                .finish(),
            Self::Recovered(_) => formatter.write_str("Recovered(..)"),
            Self::PackageRestored(_) => formatter.write_str("PackageRestored(..)"),
            Self::PackageRestoreFailed(error) => formatter
                .debug_tuple("PackageRestoreFailed")
                .field(error)
                .finish(),
            Self::Failed(error) => formatter.debug_tuple("Failed").field(error).finish(),
        }
    }
}

fn execute_package_effect(
    job: HostPackageEffect,
    entrypoints: &mut EntrypointSupervisor,
) -> HostMutationResult {
    let HostPackageEffect {
        effect,
        mut runtime,
        config,
        packages,
        reply,
    } = job;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::daemon::control::packages::mutations::apply_committed_runtime_effect(
            &mut runtime,
            entrypoints,
            &config,
            &packages,
            &effect,
        )
    }))
    .unwrap_or_else(|_| {
        Err(DaemonTransportError::Protocol(
            "host package runtime effect panicked",
        ))
    });
    let cleanup = runtime.into_cleanup();
    match result {
        Ok(()) => {
            let reply = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                finalize_package_reply(HostPackageFinalize { reply }, entrypoints)
            }))
            .unwrap_or_else(|_| {
                Err(HostMutationError::new(
                    "host_package_reply_panicked",
                    "package reply preparation panicked",
                ))
            });
            HostMutationResult::PackageEffectApplied { reply, cleanup }
        }
        Err(error) => HostMutationResult::PackageEffectFailed {
            effect,
            error,
            cleanup,
        },
    }
}

fn execute_package_runtime_restore(
    job: HostPackageRuntimeRestore,
    entrypoints: &mut EntrypointSupervisor,
) -> HostMutationResult {
    let HostPackageRuntimeRestore {
        effect,
        original,
        mut runtime,
        config,
    } = job;
    let rollbacks = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::daemon::control::packages::mutations::restore_runtime_after_failed_effect(
            &mut runtime,
            entrypoints,
            &config,
            &effect,
        )
    }))
    .unwrap_or_else(|_| {
        vec![crate::daemon::error::PackageRollbackFailure {
            step: "runtime",
            package_name: None,
            error: Box::new(DaemonTransportError::Protocol(
                "host package runtime restore panicked",
            )),
        }]
    });
    HostMutationResult::PackageRuntimeRestored {
        effect,
        original,
        rollbacks,
        cleanup: runtime.into_cleanup(),
    }
}

fn execute_package_restore(restore: HostPackageRestore) -> HostMutationResult {
    let HostPackageRestore {
        base_revision,
        authority,
        current_state,
        previous_state,
        previous_packages,
        data_directory,
    } = restore;
    let store = FileHubStateStore::for_data_directory(data_directory);
    let write = match store.prepare_shared(
        &authority,
        base_revision,
        Some(current_state),
        (*previous_state).clone(),
        &previous_state.budget(),
    ) {
        Ok(write) => write,
        Err(error) => return HostMutationResult::PackageRestoreFailed(error),
    };
    match store.commit_shared(write, base_revision) {
        Ok(FileCommitOutcome::Synced { state: view, .. }) => {
            HostMutationResult::PackageRestored(RestoredPackageView {
                view,
                packages: previous_packages,
            })
        }
        Ok(FileCommitOutcome::PublishedUncertain(write)) => {
            HostMutationResult::PublishedUncertain {
                write,
                rollback: None,
            }
        }
        Err(FileCommitError::Preparation(error))
        | Err(FileCommitError::BeforePublication { error, .. }) => {
            HostMutationResult::PackageRestoreFailed(error)
        }
        Err(FileCommitError::Stale(_)) => {
            HostMutationResult::PackageRestoreFailed(crate::HubStateStoreError::StaleRevision)
        }
        Err(FileCommitError::RevisionExhausted(_)) => {
            HostMutationResult::PackageRestoreFailed(crate::HubStateStoreError::RevisionExhausted)
        }
    }
}

fn finalize_package_reply(
    finalize: HostPackageFinalize,
    entrypoints: &mut EntrypointSupervisor,
) -> Result<HostReply, HostMutationError> {
    let HostPackageFinalize { mut reply } = finalize;
    let entrypoint_processes = entrypoints.snapshots();
    apply_daemon_entrypoint_processes(&mut reply.response, entrypoint_processes);
    HostReply::try_new(reply.response)
}

/// A response and its checked logical encoded-byte count.
pub(crate) struct HostReply {
    pub(crate) response: DaemonResponse,
    pub(crate) logical_bytes: usize,
}

impl HostReply {
    pub(crate) fn try_new(response: DaemonResponse) -> Result<Self, HostMutationError> {
        let logical_bytes = encoded_len(&response, "host_reply_encode_failed")?;
        if logical_bytes > MAX_CONTROL_RESPONSE_BYTES {
            return Err(HostMutationError::new(
                "host_reply_too_large",
                "host reply exceeds the control response limit",
            ));
        }
        Ok(Self {
            response,
            logical_bytes,
        })
    }
}

/// A prepared change and its matching family rollback descriptor.
pub(crate) struct PreparedMutation {
    pub(crate) base_revision: u64,
    pub(crate) change: PreparedChange,
    pub(crate) rollback: RollbackDescriptor,
    pub(crate) logical_bytes: usize,
}

/// A family-specific candidate write.
pub(crate) enum PreparedChange {
    PackageConfiguration(PreparedStateChange),
    SpawnTarget(PreparedStateChange),
    RegisteredWorktree(PreparedStateChange),
    SessionType(PreparedSessionTypeChange),
}

/// A prepared state write and the response produced by that write.
pub(crate) struct PreparedStateChange {
    store: FileHubStateStore,
    write: PreparedHubStateWrite,
    reply: HostReply,
    packages: Option<SharedView<PackageRegistry>>,
    package_effect: Option<PackageRuntimeEffect>,
}

/// One runtime effect that a host worker applies after a package commit.
pub(crate) enum PackageRuntimeEffect {
    Enable {
        package_name: String,
        previous_state: SharedView<HubState>,
        previous_packages: SharedView<PackageRegistry>,
    },
    Disable {
        package_name: String,
    },
    Remove {
        package_name: String,
    },
    Reload {
        package_name: String,
        reload_plugin: bool,
        previous_state: SharedView<HubState>,
        previous_packages: SharedView<PackageRegistry>,
        running_entrypoints: Vec<String>,
    },
    Refresh {
        previous_state: SharedView<HubState>,
        previous_packages: SharedView<PackageRegistry>,
        running_entrypoints: BTreeMap<String, Vec<String>>,
        packages: Vec<PackageRefreshEffect>,
    },
}

impl PackageRuntimeEffect {
    pub(crate) fn restore_command(
        &self,
        data_directory: PathBuf,
        base_revision: u64,
        authority: Arc<HubStateAuthority>,
        current_state: SharedView<HubState>,
    ) -> Option<HostPackageRestore> {
        let (previous_state, previous_packages) = match self {
            Self::Enable {
                previous_state,
                previous_packages,
                ..
            }
            | Self::Reload {
                previous_state,
                previous_packages,
                ..
            }
            | Self::Refresh {
                previous_state,
                previous_packages,
                ..
            } => (previous_state.clone(), previous_packages.clone()),
            Self::Disable { .. } | Self::Remove { .. } => return None,
        };
        Some(HostPackageRestore {
            base_revision,
            authority,
            current_state,
            previous_state,
            previous_packages,
            data_directory,
        })
    }
}

/// Post-commit work for one refreshed local package.
pub(crate) struct PackageRefreshEffect {
    pub(crate) package_name: String,
    pub(crate) reload_plugin: bool,
    pub(crate) restart_entrypoints: Vec<String>,
}

/// A prepared session-type write across Hub state and an optional repository file.
pub(crate) struct PreparedSessionTypeChange {
    state: PreparedStateChange,
    repo_write: Option<(PathBuf, Vec<PackageSessionType>)>,
}

/// Exact repository file state retained for session-type compensation.
pub(crate) struct RepoFileRollback {
    pub(crate) root: PathBuf,
    pub(crate) prior: RepoSessionTypeFileSnapshot,
}

/// A rollback descriptor that retains the previous published view.
pub(crate) enum RollbackDescriptor {
    PackageConfiguration {
        previous: SharedView<HubState>,
    },
    SpawnTarget {
        previous: SharedView<HubState>,
    },
    RegisteredWorktree {
        previous: SharedView<HubState>,
    },
    SessionType {
        previous: SharedView<HubState>,
        repo_file: Option<RepoFileRollback>,
    },
}

/// A committed immutable state view and its reply.
pub(crate) struct CommittedView {
    pub(crate) committed_revision: u64,
    pub(crate) view: SharedView<HubState>,
    pub(crate) packages: Option<SharedView<PackageRegistry>>,
    pub(crate) package_effect: Option<PackageRuntimeEffect>,
    pub(crate) reply: HostReply,
}

/// The family and state view restored after a failed commit.
pub(crate) enum RecoveryOutcome {
    PackageConfiguration {
        view: SharedView<HubState>,
        failure: HostMutationError,
    },
    SpawnTarget {
        view: SharedView<HubState>,
        failure: HostMutationError,
    },
    RegisteredWorktree {
        view: SharedView<HubState>,
        failure: HostMutationError,
    },
    SessionType {
        view: SharedView<HubState>,
        failure: HostMutationError,
        recovery: SessionTypeRecovery,
    },
}

/// The repository compensation result for a session-type commit failure.
pub(crate) enum SessionTypeRecovery {
    NotRequired,
    Restored,
    Partial {
        compensation_failure: HostMutationError,
        rollback: RepoFileRollback,
    },
}

/// A stable host mutation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostMutationError {
    pub(crate) code: String,
    pub(crate) message: String,
}

#[derive(Debug)]
pub(crate) enum ExternalEffectCause {
    RepoPublicationSyncUnconfirmed(SessionTypeError),
    RepoSyncedStateRefused(HubStateStoreError),
}

impl ExternalEffectCause {
    pub(crate) fn client_error(&self) -> (&'static str, &'static str) {
        match self {
            Self::RepoPublicationSyncUnconfirmed(_) => (
                "repo_session_type_publication_uncertain",
                "the repository file was renamed, but its synchronization is unconfirmed; Hub state was not written",
            ),
            Self::RepoSyncedStateRefused(_) => (
                "repo_session_type_state_refused",
                "the repository file is synchronized, but the Hub state write was refused",
            ),
        }
    }
}

impl HostMutationError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    fn unsupported(request: &DaemonRequest, phase: &str) -> Self {
        Self::new(
            "unsupported_host_mutation",
            format!("{phase} does not support request {request:?}"),
        )
    }
}

fn execute_read(
    read: HostRead,
    entrypoints: Option<&mut EntrypointSupervisor>,
) -> Result<HostReply, HostMutationError> {
    let response = match read {
        HostRead::Entrypoint {
            request,
            config,
            packages,
        } => {
            match crate::daemon::control::packages::handle_request(
                &config,
                &packages,
                entrypoints.expect("entrypoint request has the host supervisor"),
                request,
            ) {
                Ok(response) => response,
                Err(DaemonTransportError::Entrypoint(error)) => {
                    crate::daemon::error::daemon_entrypoint_error(error)
                }
                Err(DaemonTransportError::Package(error)) => {
                    crate::daemon::error::daemon_package_error(error)
                }
                Err(error) => return Err(daemon_error(error)),
            }
        }
        HostRead::Package {
            request,
            config,
            packages,
        } => package_read(
            request,
            &config,
            &packages,
            entrypoints
                .expect("package read has the host supervisor")
                .snapshots(),
        )?,
        HostRead::SpawnTarget { request, state } => spawn_target_read(request, &state)?,
        HostRead::SessionType {
            request,
            config,
            state,
            packages,
        } => session_type_read(request, &config, &state, &packages)?,
    };
    HostReply::try_new(response)
}

fn session_type_read(
    request: DaemonRequest,
    config: &HubConfig,
    state: &HubState,
    packages: &PackageRegistry,
) -> Result<DaemonResponse, HostMutationError> {
    let records = packages.packages();
    match request {
        DaemonRequest::ListSessionTypes => {
            crate::session_types::list_session_types(&records, state)
                .map(daemon_session_types)
                .map_err(session_type_error)
        }
        DaemonRequest::ListSessionTypesForTarget { target_id } => {
            crate::session_types::list_session_types_for_target(&records, state, &target_id)
                .map(daemon_session_types)
                .map_err(session_type_error)
        }
        DaemonRequest::ShowSessionType { session_type_id } => {
            crate::session_types::show_session_type(&records, state, &session_type_id)
                .map(|row| daemon_session_types(vec![row]))
                .map_err(session_type_error)
        }
        DaemonRequest::ShowSessionTypeDefinition { session_type_id } => {
            crate::session_types::show_session_type_definition(&records, state, &session_type_id)
                .map(daemon_session_type_definition)
                .map_err(session_type_error)
        }
        DaemonRequest::ResolveSessionType {
            session_type_id,
            request,
        } => crate::session_types::materialize_session_type(
            config,
            &records,
            state,
            &session_type_id,
            session_type_request_from_daemon(None, request),
        )
        .map(|materialized| daemon_resolved_session_type(materialized.resolved))
        .map_err(session_type_error),
        request => Err(HostMutationError::unsupported(
            &request,
            "session-type read",
        )),
    }
}

fn package_read(
    request: DaemonRequest,
    config: &HubConfig,
    packages: &PackageRegistry,
    entrypoint_processes: Vec<EntrypointProcessSnapshot>,
) -> Result<DaemonResponse, HostMutationError> {
    match request {
        DaemonRequest::ListPackages => {
            let mut rows = packages
                .packages()
                .into_iter()
                .map(|record| HubClientPackage::from_record(packages, record))
                .collect::<Vec<_>>();
            apply_entrypoint_processes(&mut rows, entrypoint_processes);
            Ok(daemon_packages(rows))
        }
        DaemonRequest::ShowPackage { package_name } => {
            let mut row = packages
                .package(&package_name)
                .map(|record| HubClientPackage::from_record(packages, record))
                .ok_or_else(|| {
                    package_error(PackageRegistryError::without_record(
                        package_name,
                        PackageAction::Show,
                        PackageAdmissionReason::PackageNotInstalled,
                        "daemon socket show package".to_string(),
                    ))
                })?;
            apply_entrypoint_processes(std::slice::from_mut(&mut row), entrypoint_processes);
            Ok(daemon_packages(vec![row]))
        }
        DaemonRequest::ListApps => Ok(daemon_apps(apps_from_registry(
            packages,
            entrypoint_processes,
        ))),
        DaemonRequest::ResolveAppLaunch {
            package_name,
            entrypoint_id,
        } => resolve_app_launch(config, packages, &package_name, &entrypoint_id),
        DaemonRequest::ResolvePackageRoute {
            package_name,
            route_id,
        } => Ok(resolve_package_route(packages, &package_name, &route_id)),
        DaemonRequest::ListPackageNavigation => {
            let rows = packages
                .packages()
                .into_iter()
                .map(|record| HubClientPackage::from_record(packages, record))
                .collect::<Vec<_>>();
            let navigation = rows
                .iter()
                .cloned()
                .flat_map(HubClientPackage::navigation_entries)
                .collect();
            Ok(daemon_package_navigation(navigation, &rows))
        }
        DaemonRequest::ListAvailablePackages { registry_path } => packages
            .available_packages(&registry_path)
            .map(|rows| daemon_available_packages(rows, &registry_path))
            .map_err(package_error),
        DaemonRequest::InspectAvailablePackage {
            registry_path,
            entry_id,
        } => packages
            .inspect_available_package(&registry_path, &entry_id)
            .map(|row| daemon_available_packages(vec![row], &registry_path))
            .map_err(package_error),
        DaemonRequest::PreviewPackageInstall {
            registry_path,
            entry_id,
        } => packages
            .preview_registry_install(registry_path, &entry_id)
            .map(daemon_package_install_plan)
            .map_err(package_error),
        DaemonRequest::CheckPackageUpdate { package_name } => {
            package_update_status(packages, &entrypoint_processes, &package_name, None)
                .map(daemon_package_update_status)
        }
        DaemonRequest::PreviewPackageUpdate { package_name, pin } => {
            let update_status = package_update_status(
                packages,
                &entrypoint_processes,
                &package_name,
                Some(pin.clone()),
            )?;
            let mut response = daemon_package_update_status(update_status.clone());
            response.install_plan = Some(package_update_plan(
                packages,
                update_status,
                &package_name,
                pin,
            )?);
            Ok(response)
        }
        request => Err(HostMutationError::unsupported(&request, "package read")),
    }
}

fn resolve_package_route(
    packages: &PackageRegistry,
    package_name: &str,
    route_id: &str,
) -> DaemonResponse {
    let Some(record) = packages.package(package_name) else {
        return daemon_package_route_error(
            package_name,
            route_id,
            "package_not_installed",
            "package is not installed",
        );
    };
    let package = HubClientPackage::from_record(packages, record);
    match package_route_descriptors(&package)
        .into_iter()
        .find(|route| route.route_id == route_id)
    {
        Some(route) => daemon_resolved_package_route(route),
        None => daemon_package_route_error(
            package_name,
            route_id,
            "route_not_found",
            "package route is not declared",
        ),
    }
}

fn resolve_app_launch(
    config: &HubConfig,
    packages: &PackageRegistry,
    package_name: &str,
    entrypoint_id: &str,
) -> Result<DaemonResponse, HostMutationError> {
    let data_directory = runtime_path(config.data_directory.clone());
    let socket =
        runtime_path(crate::transport::unix::listener::socket_path(config).map_err(daemon_error)?);
    let Some(record) = packages.package(package_name) else {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "package_not_installed",
            "package is not installed",
        ));
    };
    if !record.is_enabled() {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "package_not_enabled",
            "package is not enabled",
        ));
    }
    let Some(entrypoint) = record
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id)
    else {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "entrypoint_not_found",
            "entrypoint is not installed for package",
        ));
    };
    if !matches!(entrypoint.kind, RunnableEntrypointKind::TerminalApp) {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "unsupported_app_kind",
            "app is not a terminal_app",
        ));
    }
    if !matches!(
        entrypoint.launch_mode,
        RunnableEntrypointLaunchMode::ForegroundStdio
    ) {
        return Ok(daemon_app_launch_error(
            package_name,
            entrypoint_id,
            "unsupported_launch_mode",
            "terminal app must use foreground_stdio launch mode",
        ));
    }
    let launch =
        match resolve_foreground_launch_contract(record, entrypoint, &data_directory, &socket) {
            Ok(launch) => launch,
            Err(message) => {
                return Ok(daemon_app_launch_error(
                    package_name,
                    entrypoint_id,
                    "launch_contract_unavailable",
                    message,
                ));
            }
        };
    Ok(daemon_resolved_app_launch(DaemonResolvedAppLaunch {
        package_name: record.manifest.name.clone(),
        app_id: entrypoint.id.clone(),
        entrypoint_id: entrypoint.id.clone(),
        kind: runnable_entrypoint_kind_label(&entrypoint.kind).to_string(),
        launch_mode: runnable_launch_mode_label(&entrypoint.launch_mode).to_string(),
        command: launch.command,
        args: launch.args,
        working_directory: launch.working_directory.display().to_string(),
        environment: launch.environment,
    }))
}

fn runtime_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn package_update_status(
    packages: &PackageRegistry,
    entrypoint_processes: &[EntrypointProcessSnapshot],
    package_name: &str,
    proposed_pin: Option<DaemonPackagePin>,
) -> Result<DaemonPackageUpdateStatus, HostMutationError> {
    let record = packages.package(package_name).ok_or_else(|| {
        package_error(PackageRegistryError::without_record(
            package_name,
            PackageAction::CheckUpdate,
            PackageAdmissionReason::PackageNotInstalled,
            "daemon socket check package update".to_string(),
        ))
    })?;
    let source_metadata_present = record.source_metadata.is_some();
    let local_path_source = matches!(record.manifest.source, Some(PackageSource::Path { .. }));
    let existing_pin = record.pin.clone();
    let enabled = package_state_label(record.state.into()) == "enabled";
    let live_entrypoint = entrypoint_processes
        .iter()
        .any(|snapshot| snapshot.package_name == package_name && snapshot.state == "running");
    let pin = proposed_pin.or_else(|| existing_pin.map(daemon_package_pin_from_policy));
    let mut diagnostics = Vec::new();
    if !source_metadata_present {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "update_unavailable".to_string(),
            message:
                "update resolution is unavailable for packages without registry source metadata"
                    .to_string(),
        });
    }
    if pin.is_none() {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "pin_required".to_string(),
            message: "apply update requires explicit pinned source metadata".to_string(),
        });
    }
    if enabled && !local_path_source {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "reload_unavailable".to_string(),
            message: "enabled package changes require an operator disable/enable cycle".to_string(),
        });
    } else if enabled {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "reload_available".to_string(),
            message: "enabled local path package changes can be reloaded with reload_package"
                .to_string(),
        });
    }
    if live_entrypoint {
        diagnostics.push(DaemonPackageDiagnostic {
            kind: "restart_required".to_string(),
            message: "running package entrypoints must be restarted after update metadata changes"
                .to_string(),
        });
    }
    let has_pin = pin.is_some();
    let actions = update_status_actions(
        package_name,
        pin.as_ref(),
        has_pin,
        source_metadata_present,
        local_path_source,
    );
    Ok(DaemonPackageUpdateStatus {
        package_name: package_name.to_string(),
        update_available: has_pin && source_metadata_present,
        reload_required: enabled,
        restart_required: live_entrypoint,
        pin,
        diagnostics,
        actions,
    })
}

fn package_update_plan(
    packages: &PackageRegistry,
    update_status: DaemonPackageUpdateStatus,
    package_name: &str,
    pin: DaemonPackagePin,
) -> Result<DaemonPackageInstallPlan, HostMutationError> {
    let record = packages.package(package_name).ok_or_else(|| {
        package_error(PackageRegistryError::without_record(
            package_name,
            PackageAction::PreviewUpdate,
            PackageAdmissionReason::PackageNotInstalled,
            "daemon socket preview package update".to_string(),
        ))
    })?;
    let source = record.source_metadata.as_ref();
    Ok(DaemonPackageInstallPlan {
        entry: DaemonAvailablePackage {
            entry_id: source
                .map(|source| source.entry_id.clone())
                .unwrap_or_else(|| package_name.to_string()),
            package_name: record.manifest.name.clone(),
            version: record.manifest.version.clone(),
            classification: package_classification_label(record.classification.into()).to_string(),
            source_kind: source
                .map(|source| registry_source_kind_label(source.source_kind).to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            source_label: source
                .map(|source| source.source_label.clone())
                .unwrap_or_else(|| "installed package has no registry source metadata".to_string()),
            first_party: record.trust.first_party,
            state: package_state_label(record.state.into()).to_string(),
            requested_capabilities: record
                .manifest
                .capabilities
                .iter()
                .cloned()
                .map(|capability| DaemonCapability {
                    surface: format!("{:?}", capability.surface),
                    scope: capability.scope,
                })
                .collect(),
            compatibility: DaemonPackageCompatibility {
                botster_requirement: record.compatibility.botster_requirement.clone(),
                result: package_compatibility_label(record.compatibility.result).to_string(),
                diagnostics: record.compatibility.diagnostics.clone(),
            },
            pin: Some(pin),
            actions: Vec::new(),
        },
        effects: vec![DaemonPackageInstallEffect {
            kind: "update_pin_metadata".to_string(),
            message: "would update pinned source metadata without fetching, enabling, or starting entrypoints"
                .to_string(),
        }],
        diagnostics: update_status.diagnostics,
        mutates_registry: false,
        starts_entrypoints: false,
    })
}

fn install_decision(record: &crate::PackageRecord) -> PackageDecision {
    PackageDecision {
        package_name: record.manifest.name.clone(),
        action: PackageAction::Install,
        state: record.state,
        classification: record.classification,
        admitted_host_profile: None,
        audit_reason: record.last_audit_reason.clone(),
    }
}

fn package_list_reply(
    packages: &PackageRegistry,
    entrypoint_processes: Vec<EntrypointProcessSnapshot>,
) -> DaemonResponse {
    let mut rows = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(packages, record))
        .collect::<Vec<_>>();
    apply_entrypoint_processes(&mut rows, entrypoint_processes);
    daemon_packages(rows)
}

fn package_decision_reply(
    packages: &PackageRegistry,
    entrypoint_processes: Vec<EntrypointProcessSnapshot>,
    decision: PackageDecision,
) -> DaemonResponse {
    let mut response = package_list_reply(packages, entrypoint_processes);
    response.kind = DaemonResponseKind::PackageDecision;
    response.package_decision = Some(daemon_package_decision_from_policy(decision));
    response
}

fn running_entrypoint_ids(
    snapshots: &[EntrypointProcessSnapshot],
    package_filter: Option<&str>,
) -> BTreeMap<String, Vec<String>> {
    snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.state == "running"
                && package_filter.is_none_or(|name| snapshot.package_name == name)
        })
        .fold(BTreeMap::new(), |mut running, snapshot| {
            running
                .entry(snapshot.package_name.clone())
                .or_default()
                .push(snapshot.entrypoint_id.clone());
            running
        })
}

fn refresh_effects(
    previous: &PackageRegistry,
    candidate: &PackageRegistry,
    decisions: &[PackageDecision],
    running_entrypoints: &BTreeMap<String, Vec<String>>,
) -> Vec<PackageRefreshEffect> {
    decisions
        .iter()
        .map(|decision| {
            let restart_entrypoints = running_entrypoints
                .get(&decision.package_name)
                .into_iter()
                .flatten()
                .filter(|entrypoint_id| {
                    runnable_entrypoint_definition_changed(
                        previous,
                        candidate,
                        &decision.package_name,
                        entrypoint_id,
                    )
                })
                .cloned()
                .collect();
            PackageRefreshEffect {
                package_name: decision.package_name.clone(),
                reload_plugin: decision.state == PackageState::Enabled,
                restart_entrypoints,
            }
        })
        .collect()
}

fn runnable_entrypoint_definition_changed(
    previous: &PackageRegistry,
    candidate: &PackageRegistry,
    package_name: &str,
    entrypoint_id: &str,
) -> bool {
    let Some(previous) = previous.package(package_name) else {
        return true;
    };
    let Some(candidate) = candidate.package(package_name) else {
        return true;
    };
    let previous_entrypoint = previous
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id);
    let candidate_entrypoint = candidate
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == entrypoint_id);
    previous.manifest != candidate.manifest || previous_entrypoint != candidate_entrypoint
}

fn spawn_target_read(
    request: DaemonRequest,
    state: &HubState,
) -> Result<DaemonResponse, HostMutationError> {
    match request {
        DaemonRequest::ListSpawnTargets => Ok(daemon_spawn_targets(crate::list_spawn_targets(
            &state.spawn_targets,
        ))),
        DaemonRequest::ShowSpawnTarget { target_id } => Ok(daemon_spawn_targets(vec![
            crate::show_spawn_target(&state.spawn_targets, &target_id).map_err(spawn_error)?,
        ])),
        DaemonRequest::ValidateSpawnTarget { target_id } => Ok(daemon_spawn_target_validation(
            crate::validate_spawn_target(&state.spawn_targets, &target_id),
        )),
        DaemonRequest::ListWorktrees => Ok(daemon_worktrees(crate::list_worktrees(
            &state.worktrees,
            &state.spawn_targets,
        ))),
        DaemonRequest::ShowWorktree { worktree_id } => Ok(daemon_worktrees(vec![
            crate::show_worktree(&state.worktrees, &state.spawn_targets, &worktree_id)
                .map_err(worktree_error)?,
        ])),
        request => Err(HostMutationError::unsupported(
            &request,
            "spawn-target read",
        )),
    }
}

fn execute_prepare(
    prepare: HostPrepare,
    entrypoints: Option<&mut EntrypointSupervisor>,
) -> Result<PreparedMutation, HostMutationError> {
    match prepare {
        HostPrepare::Package {
            request,
            base_revision,
            authority,
            state,
            packages,
            data_directory,
        } => prepare_package(
            request,
            base_revision,
            authority,
            state,
            packages,
            entrypoints
                .expect("package preparation has the host supervisor")
                .snapshots(),
            data_directory,
        ),
        HostPrepare::SpawnTarget {
            request,
            base_revision,
            authority,
            state,
            packages,
            data_directory,
        } => prepare_spawn_target(
            request,
            base_revision,
            authority,
            state,
            packages,
            data_directory,
        ),
        HostPrepare::SessionType {
            request,
            base_revision,
            authority,
            config,
            state,
            packages,
            data_directory,
        } => prepare_session_type(
            request,
            base_revision,
            authority,
            config,
            state,
            packages,
            data_directory,
        ),
        HostPrepare::ManagedWorktree {
            worktree,
            base_revision,
            authority,
            state,
            data_directory,
            superseded,
        } => {
            let result = prepare_managed_worktree_record(
                worktree,
                base_revision,
                authority,
                state,
                data_directory,
            );
            drop(superseded);
            result
        }
        HostPrepare::RemoveManagedWorktree {
            worktree_id,
            base_revision,
            authority,
            state,
            data_directory,
            superseded,
        } => {
            let result = prepare_managed_worktree_removal(
                worktree_id,
                base_revision,
                authority,
                state,
                data_directory,
            );
            drop(superseded);
            result
        }
    }
}

fn prepare_managed_worktree_removal(
    worktree_id: String,
    base_revision: u64,
    authority: Arc<HubStateAuthority>,
    state: SharedView<HubState>,
    data_directory: PathBuf,
) -> Result<PreparedMutation, HostMutationError> {
    let mut candidate = (*state).clone();
    candidate.worktrees.retain(|worktree| {
        worktree.worktree_id != worktree_id || worktree.management != "hub_managed_git"
    });
    prepare_state_change(
        base_revision,
        authority,
        state,
        candidate,
        data_directory,
        HostReply::try_new(daemon_worktrees(Vec::new()))?,
        MutationFamily::RegisteredWorktree,
        None,
        None,
    )
}

fn prepare_managed_worktree_record(
    worktree: crate::Worktree,
    base_revision: u64,
    authority: Arc<HubStateAuthority>,
    state: SharedView<HubState>,
    data_directory: PathBuf,
) -> Result<PreparedMutation, HostMutationError> {
    let mut candidate = (*state).clone();
    if let Some(existing) = candidate
        .worktrees
        .iter_mut()
        .find(|existing| existing.worktree_id == worktree.worktree_id)
    {
        if existing.target_id != worktree.target_id
            || existing.path != worktree.path
            || existing.management != "hub_managed_git"
        {
            return Err(HostMutationError::new(
                "worktree_record_mismatch",
                "managed worktree record conflicts with the prepared worktree",
            ));
        }
        *existing = worktree.clone();
    } else {
        candidate.worktrees.push(worktree.clone());
    }
    prepare_state_change(
        base_revision,
        authority,
        state,
        candidate,
        data_directory,
        HostReply::try_new(daemon_worktrees(vec![worktree]))?,
        MutationFamily::RegisteredWorktree,
        None,
        None,
    )
}

fn prepare_package(
    request: DaemonRequest,
    base_revision: u64,
    authority: Arc<HubStateAuthority>,
    state: SharedView<HubState>,
    packages: SharedView<PackageRegistry>,
    entrypoint_processes: Vec<EntrypointProcessSnapshot>,
    data_directory: PathBuf,
) -> Result<PreparedMutation, HostMutationError> {
    let advances_generation = !matches!(&request, DaemonRequest::SetPackageConfiguration { .. });
    let before = advances_generation
        .then(|| session_type_catalog_entities(&packages, &state))
        .transpose()
        .map_err(daemon_error)?;
    let mut candidate_packages = (*packages).clone();
    let (response, package_effect) = match request {
        DaemonRequest::SetPackageConfiguration {
            package_name,
            values,
        } => {
            let values = decode_package_configuration(values)?;
            candidate_packages
                .set_configuration(&package_name, values, "daemon socket configure package")
                .map_err(package_error)?;
            let mut row = candidate_packages
                .package(&package_name)
                .map(|record| HubClientPackage::from_record(&candidate_packages, record))
                .expect("successful package configuration retains the package");
            apply_entrypoint_processes(
                std::slice::from_mut(&mut row),
                entrypoint_processes.clone(),
            );
            (daemon_packages(vec![row]), None)
        }
        DaemonRequest::InstallPackageRegistryEntry {
            registry_path,
            entry_id,
        } => {
            let record = candidate_packages
                .install_registry_entry(
                    registry_path,
                    &entry_id,
                    "daemon socket install registry package",
                )
                .map_err(package_error)?;
            let decision = install_decision(record);
            (
                package_decision_reply(&candidate_packages, entrypoint_processes.clone(), decision),
                None,
            )
        }
        DaemonRequest::InstallPackageLocalPath { path } => {
            let record = candidate_packages
                .install_local_path(path, "daemon socket install local package")
                .map_err(package_error)?;
            let decision = install_decision(record);
            (
                package_decision_reply(&candidate_packages, entrypoint_processes.clone(), decision),
                None,
            )
        }
        DaemonRequest::ApplyPackageUpdate { package_name, pin } => {
            let update_status = package_update_status(
                &packages,
                &entrypoint_processes,
                &package_name,
                Some(pin.clone()),
            )?;
            let pin = package_pin_from_daemon(pin).map_err(daemon_error)?;
            let record = candidate_packages
                .pin(&package_name, pin, "daemon socket apply package update")
                .map_err(package_error)?;
            let decision = PackageDecision {
                package_name: record.manifest.name.clone(),
                action: PackageAction::ApplyUpdate,
                state: record.state,
                classification: record.classification,
                admitted_host_profile: record.admitted_host_profile.clone(),
                audit_reason: record.last_audit_reason.clone(),
            };
            let mut response =
                package_decision_reply(&candidate_packages, entrypoint_processes.clone(), decision);
            response.update_status = Some(update_status);
            (response, None)
        }
        DaemonRequest::ReloadPackage { package_name } => {
            let running_entrypoints =
                running_entrypoint_ids(&entrypoint_processes, Some(package_name.as_str()))
                    .remove(&package_name)
                    .unwrap_or_default();
            let (candidate, decision) = packages
                .refreshed_local_package(&package_name, "daemon socket reload local package")
                .map_err(package_error)?;
            candidate_packages = candidate;
            let reload_plugin = decision.state == PackageState::Enabled;
            let response =
                package_decision_reply(&candidate_packages, entrypoint_processes.clone(), decision);
            (
                response,
                Some(PackageRuntimeEffect::Reload {
                    package_name,
                    reload_plugin,
                    previous_state: state.clone(),
                    previous_packages: packages.clone(),
                    running_entrypoints,
                }),
            )
        }
        DaemonRequest::RefreshLocalPackages => {
            let (candidate, decisions) = packages
                .refreshed_local_packages("daemon socket refresh local package registrations")
                .map_err(package_error)?;
            let running_entrypoints = running_entrypoint_ids(&entrypoint_processes, None);
            let effects = refresh_effects(&packages, &candidate, &decisions, &running_entrypoints);
            candidate_packages = candidate;
            (
                package_list_reply(&candidate_packages, entrypoint_processes.clone()),
                Some(PackageRuntimeEffect::Refresh {
                    previous_state: state.clone(),
                    previous_packages: packages.clone(),
                    running_entrypoints,
                    packages: effects,
                }),
            )
        }
        DaemonRequest::EnablePackageLocalPath { path } => {
            let package_name = candidate_packages
                .install_local_path(path, "daemon socket enable local package")
                .map_err(package_error)?
                .manifest
                .name
                .clone();
            let decision = candidate_packages
                .enable(&package_name, "daemon socket enable local package")
                .map_err(package_error)?;
            let response =
                package_decision_reply(&candidate_packages, entrypoint_processes.clone(), decision);
            (
                response,
                Some(PackageRuntimeEffect::Enable {
                    package_name,
                    previous_state: state.clone(),
                    previous_packages: packages.clone(),
                }),
            )
        }
        DaemonRequest::EnablePackage { package_name } => {
            let decision = candidate_packages
                .enable(&package_name, "daemon socket enable package")
                .map_err(package_error)?;
            let response =
                package_decision_reply(&candidate_packages, entrypoint_processes.clone(), decision);
            (
                response,
                Some(PackageRuntimeEffect::Enable {
                    package_name,
                    previous_state: state.clone(),
                    previous_packages: packages.clone(),
                }),
            )
        }
        DaemonRequest::DisablePackage { package_name } => {
            let decision = candidate_packages
                .disable(&package_name, "daemon socket disable package")
                .map_err(package_error)?;
            let response =
                package_decision_reply(&candidate_packages, entrypoint_processes.clone(), decision);
            (
                response,
                Some(PackageRuntimeEffect::Disable { package_name }),
            )
        }
        DaemonRequest::RemovePackage { package_name } => {
            let decision = candidate_packages
                .remove(&package_name, "daemon socket remove package")
                .map_err(package_error)?;
            let response =
                package_decision_reply(&candidate_packages, entrypoint_processes.clone(), decision);
            (
                response,
                Some(PackageRuntimeEffect::Remove { package_name }),
            )
        }
        request => return Err(HostMutationError::unsupported(&request, "package prepare")),
    };
    let mut candidate_state = (*state).clone();
    candidate_state.package_registry = candidate_packages.snapshot();
    if let Some(before) = before {
        advance_generation_after_spawn_target_change(
            &candidate_packages,
            &mut candidate_state,
            Some(before),
        )?;
    }
    let reply = HostReply::try_new(response)?;
    let package_logical_bytes = encoded_len(
        &candidate_state.package_registry,
        "host_prepared_package_registry_encode_failed",
    )?;
    let candidate_packages =
        SharedView::try_new(&state.budget(), candidate_packages, package_logical_bytes).map_err(
            |error| {
                HostMutationError::new(
                    "shared_view_capacity_exhausted",
                    format!(
                        "package registry needs {} logical bytes but only {} remain",
                        error.requested, error.available
                    ),
                )
            },
        )?;
    prepare_state_change(
        base_revision,
        authority,
        state,
        candidate_state,
        data_directory,
        reply,
        MutationFamily::PackageConfiguration,
        Some(candidate_packages),
        package_effect,
    )
}

fn prepare_spawn_target(
    request: DaemonRequest,
    base_revision: u64,
    authority: Arc<HubStateAuthority>,
    state: SharedView<HubState>,
    packages: SharedView<PackageRegistry>,
    data_directory: PathBuf,
) -> Result<PreparedMutation, HostMutationError> {
    let mut candidate = (*state).clone();
    let (reply, family) = match request {
        DaemonRequest::CreateSpawnTarget {
            target_id,
            label,
            root,
            enabled,
            kind,
            base_ref,
            metadata,
        } => {
            if enabled && root.is_dir() {
                ensure_repo_session_types_valid_for_enabled_root(&root).map_err(daemon_error)?;
            }
            let before = session_type_catalog_entities(&packages, &state).map_err(daemon_error)?;
            let target = crate::create_spawn_target(
                &mut candidate.spawn_targets,
                SpawnTargetCreate {
                    target_id,
                    label,
                    root,
                    enabled,
                    kind,
                    base_ref,
                    metadata,
                },
            )
            .map_err(spawn_error)?;
            advance_generation_after_spawn_target_change(&packages, &mut candidate, Some(before))?;
            (
                daemon_spawn_targets(vec![target]),
                MutationFamily::SpawnTarget,
            )
        }
        DaemonRequest::UpdateSpawnTarget {
            target_id,
            label,
            root,
            enabled,
            kind,
            base_ref,
            metadata,
        } => {
            let request = SpawnTargetUpdate {
                label,
                root,
                enabled,
                kind,
                base_ref,
                metadata,
            };
            validate_spawn_target_update(&candidate, &target_id, &request)?;
            let recovery_disable = request.enabled == Some(false);
            let before = match session_type_catalog_entities(&packages, &state) {
                Ok(before) => Some(before),
                Err(error) if recovery_disable && is_invalid_repo_session_types_error(&error) => {
                    None
                }
                Err(error) => return Err(daemon_error(error)),
            };
            if request.kind.as_deref().is_some_and(|kind| kind != "git")
                && candidate.worktrees.iter().any(|worktree| {
                    worktree.target_id == target_id && worktree.management == "hub_managed_git"
                })
            {
                return Err(spawn_error(SpawnTargetError::new(
                    "managed_worktrees_exist",
                    "Git target cannot be reclassified while managed worktrees reference it",
                )));
            }
            let target =
                crate::update_spawn_target(&mut candidate.spawn_targets, &target_id, request)
                    .map_err(spawn_error)?;
            advance_generation_after_spawn_target_change(&packages, &mut candidate, before)?;
            (
                daemon_spawn_targets(vec![target]),
                MutationFamily::SpawnTarget,
            )
        }
        DaemonRequest::DeleteSpawnTarget { target_id } => {
            let before = match session_type_catalog_entities(&packages, &state) {
                Ok(before) => Some(before),
                Err(error) if is_invalid_repo_session_types_error(&error) => None,
                Err(error) => return Err(daemon_error(error)),
            };
            if candidate.worktrees.iter().any(|worktree| {
                worktree.target_id == target_id && worktree.management == "hub_managed_git"
            }) {
                return Err(spawn_error(SpawnTargetError::new(
                    "managed_worktrees_exist",
                    "Git target cannot be deleted while managed worktrees reference it",
                )));
            }
            let target = crate::delete_spawn_target(&mut candidate.spawn_targets, &target_id)
                .map_err(spawn_error)?;
            advance_generation_after_spawn_target_change(&packages, &mut candidate, before)?;
            (
                daemon_spawn_targets(vec![target]),
                MutationFamily::SpawnTarget,
            )
        }
        DaemonRequest::CreateWorktree {
            worktree_id,
            target_id,
            label,
            path,
            metadata,
        } => {
            let worktree = crate::create_worktree(
                &mut candidate.worktrees,
                &candidate.spawn_targets,
                WorktreeCreate {
                    worktree_id,
                    target_id,
                    label,
                    path,
                    metadata,
                },
            )
            .map_err(worktree_error)?;
            let event = worktree_lifecycle_event(
                "worktree_created",
                Some(&worktree),
                &candidate.spawn_targets,
                None,
            );
            let mut response = daemon_worktrees(vec![worktree]);
            response
                .events
                .push(DaemonEvent::WorktreeLifecycle { event });
            (response, MutationFamily::RegisteredWorktree)
        }
        DaemonRequest::DeleteWorktree { worktree_id } => {
            let worktree = crate::delete_worktree(
                &mut candidate.worktrees,
                &candidate.spawn_targets,
                &worktree_id,
            )
            .map_err(worktree_error)?;
            let event = worktree_lifecycle_event(
                "worktree_deleted",
                Some(&worktree),
                &candidate.spawn_targets,
                None,
            );
            let mut response = daemon_worktrees(vec![worktree]);
            response
                .events
                .push(DaemonEvent::WorktreeLifecycle { event });
            (response, MutationFamily::RegisteredWorktree)
        }
        request => {
            return Err(HostMutationError::unsupported(
                &request,
                "spawn-target prepare",
            ));
        }
    };
    prepare_state_change(
        base_revision,
        authority,
        state,
        candidate,
        data_directory,
        HostReply::try_new(reply)?,
        family,
        None,
        None,
    )
}

fn prepare_session_type(
    request: DaemonRequest,
    base_revision: u64,
    authority: Arc<HubStateAuthority>,
    config: HubConfig,
    state: SharedView<HubState>,
    packages: SharedView<PackageRegistry>,
    data_directory: PathBuf,
) -> Result<PreparedMutation, HostMutationError> {
    let (source, mutation) = match request {
        DaemonRequest::CreateSessionType { source, definition } => (
            session_type_mutation_source_from_daemon(source),
            SessionTypeMutation::Create(session_type_definition_from_daemon(definition)),
        ),
        DaemonRequest::UpdateSessionType { source, definition } => (
            session_type_mutation_source_from_daemon(source),
            SessionTypeMutation::Update(session_type_definition_from_daemon(definition)),
        ),
        DaemonRequest::DeleteSessionType {
            source,
            session_type_id,
        } => (
            session_type_mutation_source_from_daemon(source),
            SessionTypeMutation::Delete {
                id: session_type_id,
            },
        ),
        request => {
            return Err(HostMutationError::unsupported(
                &request,
                "session-type prepare",
            ));
        }
    };
    let prepared = crate::session_types::prepare_session_type_mutation(
        &config,
        &state,
        source.clone(),
        mutation,
    )
    .map_err(session_type_error)?;
    let (candidate, repo_write) = prepared.into_parts();
    let repo_file = repo_write
        .as_ref()
        .map(|(root, _)| {
            snapshot_repo_session_type_file(root, HOST_PREPARED_BYTE_CAPACITY).map(|prior| {
                RepoFileRollback {
                    root: root.clone(),
                    prior,
                }
            })
        })
        .transpose()
        .map_err(session_type_error)?;

    let records = packages.packages();
    let rows = match (&source, &repo_write) {
        (SessionTypeMutationSource::Repo { target_id }, Some((_, definitions))) => {
            list_session_types_with_staged_repo(&records, &candidate, target_id, definitions)
        }
        _ => crate::session_types::list_session_types(&records, &candidate),
    }
    .map_err(session_type_error)?;
    let reply = HostReply::try_new(daemon_session_types(rows))?;
    let state_bytes = pretty_encoded_len(&candidate, "host_prepared_state_encode_failed")?;
    let repo_write_bytes = repo_write
        .as_ref()
        .map(|(root, definitions)| {
            checked_total(&[
                root.as_os_str().as_encoded_bytes().len(),
                encoded_len(definitions, "host_prepared_session_types_encode_failed")?,
            ])
        })
        .transpose()?
        .unwrap_or(0);
    let repo_bytes = repo_file.as_ref().map_or(0, repo_rollback_bytes);
    let logical_bytes = checked_total(&[
        state_bytes,
        rollback_descriptor_bytes(),
        repo_write_bytes,
        repo_bytes,
    ])?;
    if logical_bytes > HOST_PREPARED_BYTE_CAPACITY {
        return Err(HostMutationError::new(
            "host_prepared_too_large",
            "prepared mutation exceeds its prepared-byte reservation",
        ));
    }
    let store = FileHubStateStore::for_data_directory(data_directory);
    let write = store
        .prepare_shared(
            &authority,
            base_revision,
            Some(state.clone()),
            candidate,
            &state.budget(),
        )
        .map_err(|error| HostMutationError::new("hub_state_prepare_failed", error.to_string()))?;
    Ok(PreparedMutation {
        base_revision,
        change: PreparedChange::SessionType(PreparedSessionTypeChange {
            state: PreparedStateChange {
                store,
                write,
                reply,
                packages: None,
                package_effect: None,
            },
            repo_write,
        }),
        rollback: RollbackDescriptor::SessionType {
            previous: state,
            repo_file,
        },
        logical_bytes,
    })
}

fn prepare_state_change(
    base_revision: u64,
    authority: Arc<HubStateAuthority>,
    previous: SharedView<HubState>,
    candidate: HubState,
    data_directory: PathBuf,
    reply: HostReply,
    family: MutationFamily,
    packages: Option<SharedView<PackageRegistry>>,
    package_effect: Option<PackageRuntimeEffect>,
) -> Result<PreparedMutation, HostMutationError> {
    let state_bytes = pretty_encoded_len(&candidate, "host_prepared_state_encode_failed")?;
    let effect_bytes = package_effect
        .as_ref()
        .map(package_effect_bytes)
        .transpose()?
        .unwrap_or(0);
    let logical_bytes = checked_total(&[state_bytes, rollback_descriptor_bytes(), effect_bytes])?;
    if logical_bytes > HOST_PREPARED_BYTE_CAPACITY {
        return Err(HostMutationError::new(
            "host_prepared_too_large",
            "prepared mutation exceeds its prepared-byte reservation",
        ));
    }
    let store = FileHubStateStore::for_data_directory(data_directory);
    let write = store
        .prepare_shared(
            &authority,
            base_revision,
            Some(previous.clone()),
            candidate,
            &previous.budget(),
        )
        .map_err(|error| HostMutationError::new("hub_state_prepare_failed", error.to_string()))?;
    let change = PreparedStateChange {
        store,
        write,
        reply,
        packages,
        package_effect,
    };
    let (change, rollback) = match family {
        MutationFamily::PackageConfiguration => (
            PreparedChange::PackageConfiguration(change),
            RollbackDescriptor::PackageConfiguration { previous },
        ),
        MutationFamily::SpawnTarget => (
            PreparedChange::SpawnTarget(change),
            RollbackDescriptor::SpawnTarget { previous },
        ),
        MutationFamily::RegisteredWorktree => (
            PreparedChange::RegisteredWorktree(change),
            RollbackDescriptor::RegisteredWorktree { previous },
        ),
    };
    Ok(PreparedMutation {
        base_revision,
        change,
        rollback,
        logical_bytes,
    })
}

fn package_effect_bytes(effect: &PackageRuntimeEffect) -> Result<usize, HostMutationError> {
    let initial = mem::size_of::<PackageRuntimeEffect>();
    match effect {
        PackageRuntimeEffect::Enable { package_name, .. }
        | PackageRuntimeEffect::Disable { package_name }
        | PackageRuntimeEffect::Remove { package_name } => {
            checked_total(&[initial, package_name.len()])
        }
        PackageRuntimeEffect::Reload {
            package_name,
            running_entrypoints,
            ..
        } => running_entrypoints.iter().try_fold(
            checked_total(&[initial, package_name.len()])?,
            |total, entrypoint| checked_total(&[total, mem::size_of::<String>(), entrypoint.len()]),
        ),
        PackageRuntimeEffect::Refresh {
            packages,
            running_entrypoints,
            ..
        } => {
            let total = running_entrypoints.iter().try_fold(
                initial,
                |total, (package_name, entrypoints)| {
                    entrypoints.iter().try_fold(
                        checked_total(&[total, mem::size_of::<String>(), package_name.len()])?,
                        |total, entrypoint| {
                            checked_total(&[total, mem::size_of::<String>(), entrypoint.len()])
                        },
                    )
                },
            )?;
            packages.iter().try_fold(total, |total, package| {
                package.restart_entrypoints.iter().try_fold(
                    checked_total(&[
                        total,
                        mem::size_of::<PackageRefreshEffect>(),
                        package.package_name.len(),
                    ])?,
                    |total, entrypoint| {
                        checked_total(&[total, mem::size_of::<String>(), entrypoint.len()])
                    },
                )
            })
        }
    }
}

fn execute_commit(commit: HostCommit) -> HostMutationResult {
    let PreparedMutation {
        base_revision,
        change,
        rollback,
        ..
    } = commit.prepared;
    let Some(committed_revision) = base_revision.checked_add(1) else {
        return HostMutationResult::Failed(HostMutationError::new(
            "hub_revision_exhausted",
            "Hub state revision cannot advance",
        ));
    };
    if !families_match(&change, &rollback) {
        return HostMutationResult::Failed(HostMutationError::new(
            "host_mutation_family_mismatch",
            "prepared change and rollback families do not match",
        ));
    }
    if let PreparedChange::SessionType(change) = change {
        return execute_session_type_commit(committed_revision, change, rollback);
    }
    let PreparedStateChange {
        store,
        write,
        reply,
        packages,
        package_effect,
    } = into_state_change(change);
    match store.commit_shared(write, base_revision) {
        Ok(FileCommitOutcome::Synced {
            state: view,
            revision,
        }) => HostMutationResult::Committed(CommittedView {
            committed_revision: revision,
            view,
            packages,
            package_effect,
            reply,
        }),
        Ok(FileCommitOutcome::PublishedUncertain(write)) => {
            HostMutationResult::PublishedUncertain {
                write,
                rollback: None,
            }
        }
        Err(error) => {
            let failure = file_commit_error(error);
            HostMutationResult::Recovered(execute_recovery(HostRecover { rollback, failure }))
        }
    }
}

fn execute_session_type_commit(
    committed_revision: u64,
    change: PreparedSessionTypeChange,
    rollback: RollbackDescriptor,
) -> HostMutationResult {
    let PreparedSessionTypeChange { state, repo_write } = change;
    let PreparedStateChange {
        store,
        write,
        reply,
        packages,
        package_effect,
    } = state;
    debug_assert!(packages.is_none());
    debug_assert!(package_effect.is_none());
    let Some((root, definitions)) = repo_write else {
        return finish_session_type_state_commit(
            store.commit_shared(write, committed_revision - 1),
            rollback,
            reply,
        );
    };
    let RollbackDescriptor::SessionType {
        repo_file: Some(repo_prior),
        ..
    } = &rollback
    else {
        return HostMutationResult::Failed(HostMutationError::new(
            "repo_session_type_recovery_required",
            "repository file prior evidence is missing",
        ));
    };
    if repo_prior.root != root {
        return HostMutationResult::Failed(HostMutationError::new(
            "repo_session_type_recovery_required",
            "repository file root changed after preparation",
        ));
    }
    let current_prior = match snapshot_repo_session_type_file(&root, HOST_PREPARED_BYTE_CAPACITY) {
        Ok(prior) => prior,
        Err(error) => return HostMutationResult::Failed(session_type_error(error)),
    };
    if current_prior != repo_prior.prior {
        return HostMutationResult::Failed(HostMutationError::new(
            "repo_session_type_recovery_required",
            "repository file changed after preparation",
        ));
    }
    let repo_bytes = match encode_repo_session_type_bytes(&definitions) {
        Ok(bytes) => bytes,
        Err(error) => return HostMutationResult::Failed(session_type_error(error)),
    };
    let external_path = root.join(".botster/session-types.json");
    let prior_bytes = match &repo_prior.prior {
        RepoSessionTypeFileSnapshot::Missing => None,
        RepoSessionTypeFileSnapshot::Present(bytes) => Some(bytes.as_slice()),
    };
    let pending = match store.begin_shared_effect(
        write,
        committed_revision - 1,
        Some(ExternalFileIntent {
            path: &external_path,
            prior: prior_bytes,
            candidate: &repo_bytes,
        }),
    ) {
        Ok(pending) => pending,
        Err(error) => {
            return HostMutationResult::Failed(HostMutationError::new(
                "repo_session_type_recovery_required",
                file_commit_error(error).message,
            ));
        }
    };
    match commit_repo_session_type_bytes(&root, &repo_bytes) {
        Ok(RepoSessionTypeFileCommit::Synced) => {}
        Ok(RepoSessionTypeFileCommit::PublishedUncertain(error)) => {
            return HostMutationResult::ExternalEffectUncertain {
                pending,
                rollback,
                cause: ExternalEffectCause::RepoPublicationSyncUnconfirmed(error),
            };
        }
        Err(error) => {
            return HostMutationResult::Failed(HostMutationError::new(
                "repo_session_type_recovery_required",
                error.message,
            ));
        }
    }
    match store.commit_shared_effect(pending) {
        Ok(outcome) => finish_session_type_state_commit(Ok(outcome), rollback, reply),
        Err(failure) => HostMutationResult::ExternalEffectUncertain {
            pending: failure.pending,
            rollback,
            cause: ExternalEffectCause::RepoSyncedStateRefused(failure.error),
        },
    }
}

fn finish_session_type_state_commit(
    result: Result<FileCommitOutcome, FileCommitError>,
    rollback: RollbackDescriptor,
    reply: HostReply,
) -> HostMutationResult {
    match result {
        Ok(FileCommitOutcome::Synced {
            state: view,
            revision,
        }) => HostMutationResult::Committed(CommittedView {
            committed_revision: revision,
            view,
            packages: None,
            package_effect: None,
            reply,
        }),
        Ok(FileCommitOutcome::PublishedUncertain(write)) => {
            HostMutationResult::PublishedUncertain {
                write,
                rollback: Some(rollback),
            }
        }
        Err(error) => HostMutationResult::Recovered(execute_recovery(HostRecover {
            rollback,
            failure: file_commit_error(error),
        })),
    }
}

fn file_commit_error(error: FileCommitError) -> HostMutationError {
    let detail = match error {
        FileCommitError::Preparation(error) | FileCommitError::BeforePublication { error, .. } => {
            error.to_string()
        }
        FileCommitError::Stale(_) => "prepared state revision is stale".to_string(),
        FileCommitError::RevisionExhausted(_) => "Hub state revision cannot advance".to_string(),
    };
    HostMutationError::new("hub_state_commit_failed", detail)
}

fn execute_recovery(recover: HostRecover) -> RecoveryOutcome {
    match recover.rollback {
        RollbackDescriptor::PackageConfiguration { previous } => {
            RecoveryOutcome::PackageConfiguration {
                view: previous,
                failure: recover.failure,
            }
        }
        RollbackDescriptor::SpawnTarget { previous } => RecoveryOutcome::SpawnTarget {
            view: previous,
            failure: recover.failure,
        },
        RollbackDescriptor::RegisteredWorktree { previous } => {
            RecoveryOutcome::RegisteredWorktree {
                view: previous,
                failure: recover.failure,
            }
        }
        RollbackDescriptor::SessionType {
            previous,
            repo_file,
        } => {
            let recovery = match repo_file {
                None => SessionTypeRecovery::NotRequired,
                Some(rollback) => {
                    match restore_repo_session_type_file(&rollback.root, &rollback.prior) {
                        Ok(()) => SessionTypeRecovery::Restored,
                        Err(error) => SessionTypeRecovery::Partial {
                            compensation_failure: session_type_error(error),
                            rollback,
                        },
                    }
                }
            };
            RecoveryOutcome::SessionType {
                view: previous,
                failure: recover.failure,
                recovery,
            }
        }
    }
}

fn into_state_change(change: PreparedChange) -> PreparedStateChange {
    match change {
        PreparedChange::PackageConfiguration(change)
        | PreparedChange::SpawnTarget(change)
        | PreparedChange::RegisteredWorktree(change) => change,
        PreparedChange::SessionType(_) => unreachable!("session-type commit uses both stores"),
    }
}

fn families_match(change: &PreparedChange, rollback: &RollbackDescriptor) -> bool {
    matches!(
        (change, rollback),
        (
            PreparedChange::PackageConfiguration(_),
            RollbackDescriptor::PackageConfiguration { .. }
        ) | (
            PreparedChange::SpawnTarget(_),
            RollbackDescriptor::SpawnTarget { .. }
        ) | (
            PreparedChange::RegisteredWorktree(_),
            RollbackDescriptor::RegisteredWorktree { .. }
        ) | (
            PreparedChange::SessionType(_),
            RollbackDescriptor::SessionType { .. }
        )
    )
}

#[derive(Clone, Copy)]
enum MutationFamily {
    PackageConfiguration,
    SpawnTarget,
    RegisteredWorktree,
}

fn validate_spawn_target_update(
    state: &HubState,
    target_id: &str,
    request: &SpawnTargetUpdate,
) -> Result<(), HostMutationError> {
    let Some(target) = state
        .spawn_targets
        .iter()
        .find(|target| target.target_id == target_id)
    else {
        return Ok(());
    };
    if request.enabled == Some(false) {
        return Ok(());
    }
    let enabled = request.enabled.unwrap_or(target.enabled);
    if !enabled {
        return Ok(());
    }
    let root = request.root.as_ref().unwrap_or(&target.root);
    if root.is_dir() {
        ensure_repo_session_types_valid_for_enabled_root(root).map_err(daemon_error)?;
    }
    Ok(())
}

fn advance_generation_after_spawn_target_change(
    packages: &PackageRegistry,
    candidate: &mut HubState,
    before: Option<BTreeMap<String, serde_json::Value>>,
) -> Result<(), HostMutationError> {
    let should_advance = match before {
        Some(before) => {
            session_type_catalog_entities(packages, candidate).map_err(daemon_error)? != before
        }
        None => true,
    };
    if should_advance {
        candidate.session_type_generation = candidate.session_type_generation.saturating_add(1);
    }
    Ok(())
}

fn decode_package_configuration(
    values: BTreeMap<String, serde_json::Value>,
) -> Result<BTreeMap<String, PackageConfigurationValue>, HostMutationError> {
    values
        .into_iter()
        .map(|(key, value)| {
            serde_json::from_value(value)
                .map(|value| (key.clone(), value))
                .map_err(|error| {
                    HostMutationError::new(
                        "invalid_package_configuration",
                        format!("configuration field {key} is invalid: {error}"),
                    )
                })
        })
        .collect()
}

fn apply_entrypoint_processes(
    packages: &mut [HubClientPackage],
    snapshots: Vec<EntrypointProcessSnapshot>,
) {
    for snapshot in snapshots {
        let Some(package) = packages
            .iter_mut()
            .find(|package| package.package_name == snapshot.package_name)
        else {
            continue;
        };
        let Some(entrypoint) = package
            .runnable_entrypoints
            .iter_mut()
            .find(|entrypoint| entrypoint.id == snapshot.entrypoint_id)
        else {
            continue;
        };
        entrypoint.process.state = snapshot.state;
        entrypoint.process.pid = snapshot.pid;
        entrypoint.process.started_at = snapshot.started_at;
        entrypoint.process.exited_at = snapshot.exited_at;
        entrypoint.process.exit_status = snapshot.exit_status;
        entrypoint.process.diagnostics = snapshot
            .diagnostics
            .into_iter()
            .map(|diagnostic| crate::HubClientPackageDiagnostic {
                kind: diagnostic.kind,
                message: diagnostic.message,
            })
            .collect();
    }
}

fn apply_daemon_entrypoint_processes(
    response: &mut DaemonResponse,
    snapshots: Vec<EntrypointProcessSnapshot>,
) {
    for snapshot in snapshots {
        let Some(package) = response
            .packages
            .iter_mut()
            .find(|package| package.package_name == snapshot.package_name)
        else {
            continue;
        };
        let Some(entrypoint) = package
            .runnable_entrypoints
            .iter_mut()
            .find(|entrypoint| entrypoint.id == snapshot.entrypoint_id)
        else {
            continue;
        };
        entrypoint.process.state = snapshot.state;
        entrypoint.process.pid = snapshot.pid;
        entrypoint.process.started_at = snapshot.started_at;
        entrypoint.process.exited_at = snapshot.exited_at;
        entrypoint.process.exit_status = snapshot.exit_status;
        entrypoint.process.diagnostics = snapshot
            .diagnostics
            .into_iter()
            .map(|diagnostic| DaemonPackageDiagnostic {
                kind: diagnostic.kind,
                message: diagnostic.message,
            })
            .collect();
    }
}

fn daemon_error(error: DaemonTransportError) -> HostMutationError {
    match error {
        DaemonTransportError::Client(crate::HubClientError::SessionType {
            kind, message, ..
        }) => HostMutationError::new(kind, message),
        error => HostMutationError::new("host_mutation_failed", error.to_string()),
    }
}

fn package_error(error: PackageRegistryError) -> HostMutationError {
    let package_name = crate::daemon_projection::package_error_display_name(&error);
    HostMutationError::new(
        "package_policy_rejected",
        format!("package {} was rejected: {:?}", package_name, error.reason),
    )
}

fn spawn_error(error: SpawnTargetError) -> HostMutationError {
    HostMutationError::new(error.kind, error.message)
}

fn worktree_error(error: crate::WorktreeError) -> HostMutationError {
    HostMutationError::new(error.kind, error.message)
}

fn session_type_error(error: crate::SessionTypeError) -> HostMutationError {
    HostMutationError::new(error.kind, error.message)
}

fn repo_rollback_bytes(rollback: &RepoFileRollback) -> usize {
    let prior_bytes = match &rollback.prior {
        RepoSessionTypeFileSnapshot::Missing => 0,
        RepoSessionTypeFileSnapshot::Present(bytes) => bytes.len(),
    };
    rollback
        .root
        .as_os_str()
        .as_encoded_bytes()
        .len()
        .saturating_add(prior_bytes)
}

fn rollback_descriptor_bytes() -> usize {
    mem::size_of::<RollbackDescriptor>()
}

fn checked_total(values: &[usize]) -> Result<usize, HostMutationError> {
    values.iter().try_fold(0_usize, |total, value| {
        total.checked_add(*value).ok_or_else(|| {
            HostMutationError::new(
                "host_logical_bytes_overflow",
                "host logical byte count overflowed",
            )
        })
    })
}

fn encoded_len<T: serde::Serialize>(
    value: &T,
    error_code: &'static str,
) -> Result<usize, HostMutationError> {
    count_encoded(value, false, error_code)
}

fn pretty_encoded_len<T: serde::Serialize>(
    value: &T,
    error_code: &'static str,
) -> Result<usize, HostMutationError> {
    count_encoded(value, true, error_code)
}

fn count_encoded<T: serde::Serialize>(
    value: &T,
    pretty: bool,
    error_code: &'static str,
) -> Result<usize, HostMutationError> {
    let mut writer = CountingWriter::default();
    let result = if pretty {
        serde_json::to_writer_pretty(&mut writer, value)
    } else {
        serde_json::to_writer(&mut writer, value)
    };
    result.map_err(|error| HostMutationError::new(error_code, error.to_string()))?;
    Ok(writer.bytes)
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
}

impl io::Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("logical byte count overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execute(command: HostMutationCommand) -> HostMutationResult {
        super::execute(command, Some(&mut EntrypointSupervisor::default()))
    }

    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
    use crate::packages::{HubPackageEvents, HubPackageManifest, PackageProvenance};
    use botster_core::{
        ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, PackageConfigurationField,
        PackageConfigurationFieldType, PackageConfigurationSchema, PackageSource,
    };
    use botster_hub_client::DaemonSessionTypeMutationSource;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn inputs(
        name: &str,
    ) -> (
        SharedView<HubState>,
        SharedView<PackageRegistry>,
        PathBuf,
        Arc<HubStateAuthority>,
    ) {
        let data_directory = unique_test_dir(name);
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(data_directory.clone()),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build host mutation test config");
        let authority = Arc::new(
            FileHubStateStore::for_data_directory(&data_directory)
                .acquire_test_authority()
                .expect("acquire host mutation test authority"),
        );
        let budget = authority.budget();
        let packages = PackageRegistry::new(botster_core::CapabilitySet::new());
        let state = HubState::from_config(&config);
        (
            SharedView::try_new(&budget, state, 1).expect("state view fits"),
            SharedView::try_new(&budget, packages, 1).expect("package view fits"),
            data_directory,
            authority,
        )
    }

    fn persisted_inputs(
        name: &str,
    ) -> (
        SharedView<HubState>,
        SharedView<PackageRegistry>,
        PathBuf,
        Arc<HubStateAuthority>,
    ) {
        let (state, packages, data_directory, authority) = inputs(name);
        let store = FileHubStateStore::for_data_directory(&data_directory);
        let prepared = store
            .prepare_shared(&authority, 0, None, (*state).clone(), &authority.budget())
            .expect("prepare initial state fixture");
        let FileCommitOutcome::Synced { state, .. } = store
            .commit_shared(prepared, 0)
            .expect("commit initial state fixture")
        else {
            panic!("initial state fixture must synchronize");
        };
        (state, packages, data_directory, authority)
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir()
            .join("botster-host-mutations")
            .join(name)
            .join(nanos.to_string())
    }

    fn test_config(data_directory: PathBuf) -> HubConfig {
        HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(data_directory),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build host mutation test config")
    }

    fn test_pin() -> DaemonPackagePin {
        DaemonPackagePin {
            revision: "revision-2".to_string(),
            branch: None,
            tag: None,
            rev: Some("0123456789abcdef".to_string()),
            checksum: None,
            update_policy: "manual".to_string(),
        }
    }

    fn create_target_request(id: String) -> DaemonRequest {
        DaemonRequest::CreateSpawnTarget {
            target_id: Some(id),
            label: None,
            root: std::env::current_dir().expect("current directory"),
            enabled: false,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn host_package_error_uses_existing_display_name_policy() {
        let path = "/private/local-package/botster-package.json";
        for reason in [
            PackageAdmissionReason::InvalidLocalManifest("invalid JSON".to_string()),
            PackageAdmissionReason::UnsafeLocalPath("path unavailable".to_string()),
        ] {
            let error = PackageRegistryError::without_record(
                path,
                PackageAction::Install,
                reason,
                "install local package".to_string(),
            );
            let response = package_error(error);
            assert_eq!(response.code, "package_policy_rejected");
            assert!(
                response
                    .message
                    .starts_with("package <local-package> was rejected: ")
            );
            assert!(!response.message.contains(path));
        }

        let named = package_error(PackageRegistryError::without_record(
            "named.plugin",
            PackageAction::Install,
            PackageAdmissionReason::AlreadyInstalled,
            "install package".to_string(),
        ));
        assert!(named.message.contains("package named.plugin was rejected"));

        let refresh = package_error(PackageRegistryError::without_record(
            path,
            PackageAction::Show,
            PackageAdmissionReason::InvalidLocalManifest("invalid JSON".to_string()),
            "refresh local package registrations".to_string(),
        ));
        assert!(refresh.message.contains(path));
    }

    fn package_inputs(
        name: &str,
    ) -> (
        SharedView<HubState>,
        SharedView<PackageRegistry>,
        PathBuf,
        Arc<HubStateAuthority>,
    ) {
        let (state, _empty_packages, data_directory, authority) = inputs(name);
        let mut packages = PackageRegistry::new(botster_core::CapabilitySet::new());
        packages
            .install(
                HubPackageManifest {
                    name: "configured.plugin".to_string(),
                    version: "1.0.0".to_string(),
                    kind: ExtensionKind::Plugin,
                    botster: ">=0.1.0".to_string(),
                    source: Some(PackageSource::Git {
                        repo: "https://example.invalid/configured.git".to_string(),
                        reference: "v1.0.0".to_string(),
                    }),
                    capabilities: Vec::new(),
                    entrypoints: vec![ExtensionEntrypoint {
                        runtime: ExtensionRuntime::Lua,
                        path: "plugin.lua".to_string(),
                        bootstrap: false,
                    }],
                    dependencies: Vec::new(),
                    features: Vec::new(),
                    host_profile: None,
                    configuration: Some(PackageConfigurationSchema {
                        groups: Vec::new(),
                        fields: vec![PackageConfigurationField {
                            key: "label".to_string(),
                            field_type: PackageConfigurationFieldType::String,
                            label: "Label".to_string(),
                            description: None,
                            required: false,
                            default: None,
                            validation: None,
                            group: None,
                            order: None,
                            options: Vec::new(),
                        }],
                    }),
                    runnable_entrypoints: Vec::new(),
                    surfaces: Vec::new(),
                    navigation: Vec::new(),
                    events: HubPackageEvents::default(),
                },
                PackageProvenance {
                    source: "test".to_string(),
                    checksum: None,
                },
                "host mutation test",
            )
            .expect("install package fixture");
        let mut matched_state = (*state).clone();
        matched_state.package_registry = packages.snapshot();
        let budget = authority.budget();
        (
            SharedView::try_new(&budget, matched_state, 1).expect("state view fits"),
            SharedView::try_new(&budget, packages, 1).expect("package view fits"),
            data_directory,
            authority,
        )
    }

    fn session_type_inputs(
        name: &str,
    ) -> (
        HubConfig,
        SharedView<HubState>,
        SharedView<PackageRegistry>,
        PathBuf,
        String,
        Arc<HubStateAuthority>,
    ) {
        let (state, packages, data_directory, authority) = inputs(name);
        let repo_root = data_directory.join("repo");
        fs::create_dir_all(&repo_root).expect("create repository fixture");
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(data_directory.clone()),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build session-type test config");
        let mut candidate = (*state).clone();
        let target_id = format!("{name}-target");
        crate::create_spawn_target(
            &mut candidate.spawn_targets,
            SpawnTargetCreate {
                target_id: Some(target_id.clone()),
                label: None,
                root: repo_root,
                enabled: true,
                kind: Some("directory".to_string()),
                base_ref: None,
                metadata: BTreeMap::new(),
            },
        )
        .expect("create admitted target fixture");
        let budget = authority.budget();
        let store = FileHubStateStore::for_data_directory(&data_directory);
        let prepared = store
            .prepare_shared(&authority, 0, None, candidate, &budget)
            .expect("prepare initial state fixture");
        let crate::persistence::FileCommitOutcome::Synced { state, .. } = store
            .commit_shared(prepared, 0)
            .expect("commit initial state fixture")
        else {
            panic!("initial state fixture must synchronize");
        };
        (
            config,
            state,
            packages,
            data_directory,
            target_id,
            authority,
        )
    }

    fn session_type_create_request(target_id: String) -> DaemonRequest {
        let definition = PackageSessionType {
            id: "review".to_string(),
            label: "Review".to_string(),
            description: None,
            icon: None,
            role: "botster.agent".to_string(),
            interaction: "interactive".to_string(),
            traits: Vec::new(),
            lifecycle: "durable".to_string(),
            execution: crate::PackageSessionTypeExecution::RelativeExecutable,
            command: "bin/review".to_string(),
            args: Vec::new(),
            working_directory: crate::PackageSessionTypeWorkingDirectory::PackageRoot,
            environment: BTreeMap::new(),
            allowed_environment_overrides: Vec::new(),
            context: Vec::new(),
            target_id: None,
        };
        DaemonRequest::CreateSessionType {
            source: DaemonSessionTypeMutationSource::Repo { target_id },
            definition: crate::client_api_dto::session::daemon_session_type_definition_from_client(
                definition,
            ),
        }
    }

    #[test]
    fn read_owns_input_and_has_a_deterministic_checked_reply() {
        let (state, _packages, _directory, _authority) = inputs("owned-read");
        let target_id = String::from("owned-target");
        let request = DaemonRequest::ValidateSpawnTarget {
            target_id: target_id.clone(),
        };
        drop(target_id);
        let HostMutationResult::ReadReady(reply) =
            execute(HostMutationCommand::Read(HostRead::SpawnTarget {
                request,
                state,
            }))
        else {
            panic!("spawn-target read must succeed");
        };
        assert_eq!(
            reply.response.kind,
            DaemonResponseKind::SpawnTargetValidation
        );
        assert_eq!(
            reply.logical_bytes,
            serde_json::to_vec(&reply.response)
                .expect("encode reply")
                .len()
        );
        assert_eq!(
            reply
                .response
                .spawn_target_validation
                .expect("validation")
                .status,
            "not_found"
        );
    }

    #[test]
    fn package_read_uses_the_owned_registry_view() {
        let (_state, packages, directory, _authority) = inputs("package-read");
        let config = test_config(directory);
        let HostMutationResult::ReadReady(reply) =
            execute(HostMutationCommand::Read(HostRead::Package {
                request: DaemonRequest::ListPackages,
                config,
                packages,
            }))
        else {
            panic!("package read must succeed");
        };
        assert_eq!(reply.response.kind, DaemonResponseKind::Packages);
        assert!(reply.response.packages.is_empty());
    }

    #[test]
    fn package_read_routes_each_immutable_package_request() {
        let (_state, packages, data_directory, _authority) = package_inputs("package-read-routes");
        let config = test_config(data_directory.clone());
        let missing_registry = data_directory.join("missing-registry.json");
        let requests = vec![
            DaemonRequest::ResolveAppLaunch {
                package_name: "missing.plugin".to_string(),
                entrypoint_id: "terminal".to_string(),
            },
            DaemonRequest::ResolvePackageRoute {
                package_name: "missing.plugin".to_string(),
                route_id: "home".to_string(),
            },
            DaemonRequest::ListPackageNavigation,
            DaemonRequest::ListAvailablePackages {
                registry_path: missing_registry.clone(),
            },
            DaemonRequest::InspectAvailablePackage {
                registry_path: missing_registry.clone(),
                entry_id: "missing".to_string(),
            },
            DaemonRequest::PreviewPackageInstall {
                registry_path: missing_registry,
                entry_id: "missing".to_string(),
            },
            DaemonRequest::CheckPackageUpdate {
                package_name: "configured.plugin".to_string(),
            },
            DaemonRequest::PreviewPackageUpdate {
                package_name: "configured.plugin".to_string(),
                pin: test_pin(),
            },
        ];
        for request in requests {
            let result = execute(HostMutationCommand::Read(HostRead::Package {
                request,
                config: config.clone(),
                packages: packages.clone(),
            }));
            if let HostMutationResult::Failed(error) = result {
                assert_ne!(error.code, "unsupported_host_mutation");
            }
        }
    }

    #[test]
    fn package_prepare_returns_typed_runtime_effects() {
        let (state, packages, data_directory, authority) = package_inputs("package-effects");
        let requests = vec![
            DaemonRequest::EnablePackage {
                package_name: "configured.plugin".to_string(),
            },
            DaemonRequest::DisablePackage {
                package_name: "configured.plugin".to_string(),
            },
            DaemonRequest::RemovePackage {
                package_name: "configured.plugin".to_string(),
            },
            DaemonRequest::RefreshLocalPackages,
        ];
        for request in requests {
            let HostMutationResult::Prepared(prepared) =
                execute(HostMutationCommand::Prepare(HostPrepare::Package {
                    request,
                    base_revision: 3,
                    authority: authority.clone(),
                    state: state.clone(),
                    packages: packages.clone(),
                    data_directory: data_directory.clone(),
                }))
            else {
                panic!("package preparation must succeed");
            };
            let PreparedChange::PackageConfiguration(change) = prepared.change else {
                panic!("package preparation must keep its mutation family");
            };
            assert!(change.package_effect.is_some());
        }
        assert_eq!(
            packages
                .package("configured.plugin")
                .expect("base package")
                .state,
            PackageState::Installed
        );
    }

    #[test]
    fn refresh_effect_retains_exact_compensation_inputs() {
        let (state, packages, data_directory, authority) =
            package_inputs("package-refresh-compensation");
        let snapshot = EntrypointProcessSnapshot {
            package_name: "configured.plugin".to_string(),
            entrypoint_id: "worker".to_string(),
            state: "running".to_string(),
            pid: Some(42),
            started_at: Some(1),
            exited_at: None,
            exit_status: None,
            diagnostics: Vec::new(),
            launch_result: None,
        };
        let prepared = prepare_package(
            DaemonRequest::RefreshLocalPackages,
            5,
            authority,
            state.clone(),
            packages.clone(),
            vec![snapshot],
            data_directory,
        )
        .expect("package refresh preparation must succeed");
        let PreparedChange::PackageConfiguration(change) = prepared.change else {
            panic!("package refresh must keep its mutation family");
        };
        let Some(PackageRuntimeEffect::Refresh {
            previous_state,
            previous_packages,
            running_entrypoints,
            ..
        }) = change.package_effect
        else {
            panic!("package refresh must return its runtime effect");
        };
        assert!(SharedView::ptr_eq(&previous_state, &state));
        assert!(SharedView::ptr_eq(&previous_packages, &packages));
        assert_eq!(
            running_entrypoints,
            BTreeMap::from([("configured.plugin".to_string(), vec!["worker".to_string()])])
        );
    }

    #[test]
    fn package_prepare_routes_each_filesystem_mutation() {
        let (state, packages, data_directory, authority) = package_inputs("package-prepare-routes");
        let missing_path = data_directory.join("missing-package");
        let requests = vec![
            DaemonRequest::InstallPackageRegistryEntry {
                registry_path: missing_path.clone(),
                entry_id: "missing".to_string(),
            },
            DaemonRequest::InstallPackageLocalPath {
                path: missing_path.clone(),
            },
            DaemonRequest::ApplyPackageUpdate {
                package_name: "configured.plugin".to_string(),
                pin: test_pin(),
            },
            DaemonRequest::ReloadPackage {
                package_name: "configured.plugin".to_string(),
            },
            DaemonRequest::EnablePackageLocalPath { path: missing_path },
        ];
        for request in requests {
            let result = execute(HostMutationCommand::Prepare(HostPrepare::Package {
                request,
                base_revision: 3,
                authority: authority.clone(),
                state: state.clone(),
                packages: packages.clone(),
                data_directory: data_directory.clone(),
            }));
            if let HostMutationResult::Failed(error) = result {
                assert_ne!(error.code, "unsupported_host_mutation");
            }
        }
    }

    #[test]
    fn session_type_read_uses_the_owned_state_and_registry_views() {
        let (config, state, packages, data_directory, _target_id, _authority) =
            session_type_inputs("session-type-read");
        let HostMutationResult::ReadReady(reply) =
            execute(HostMutationCommand::Read(HostRead::SessionType {
                request: DaemonRequest::ListSessionTypes,
                config,
                state,
                packages,
            }))
        else {
            panic!("session-type read must succeed");
        };
        assert_eq!(reply.response.kind, DaemonResponseKind::SessionTypes);
        assert!(reply.response.session_types.is_empty());
        fs::remove_dir_all(&data_directory).expect("remove host mutation test directory");
    }

    #[test]
    fn package_configuration_prepare_keeps_the_base_registry_unchanged() {
        let (state, packages, data_directory, authority) = package_inputs("package-prepare");
        let original_snapshot = packages.snapshot();
        let value = PackageConfigurationValue::String {
            value: "updated".to_string(),
        };
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::Package {
                request: DaemonRequest::SetPackageConfiguration {
                    package_name: "configured.plugin".to_string(),
                    values: BTreeMap::from([(
                        "label".to_string(),
                        serde_json::to_value(value).expect("encode configuration value"),
                    )]),
                },
                base_revision: 9,
                authority,
                state,
                packages: packages.clone(),
                data_directory,
            }))
        else {
            panic!("package configuration prepare must succeed");
        };
        assert_eq!(prepared.base_revision, 9);
        assert!(matches!(
            prepared.change,
            PreparedChange::PackageConfiguration(_)
        ));
        assert!(matches!(
            prepared.rollback,
            RollbackDescriptor::PackageConfiguration { .. }
        ));
        assert_eq!(packages.snapshot(), original_snapshot);
    }

    #[test]
    fn prepare_retains_revision_and_does_not_mutate_or_write_the_base() {
        let (state, packages, data_directory, authority) = inputs("prepare-only");
        let original = (*state).clone();
        let state_path = data_directory.join("hub-state.json");
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
                request: create_target_request("prepared-target".to_string()),
                base_revision: 41,
                authority,
                state: state.clone(),
                packages,
                data_directory,
            }))
        else {
            panic!("spawn-target prepare must succeed");
        };
        assert_eq!(prepared.base_revision, 41);
        assert!(matches!(prepared.change, PreparedChange::SpawnTarget(_)));
        assert!(matches!(
            prepared.rollback,
            RollbackDescriptor::SpawnTarget { .. }
        ));
        assert!(prepared.logical_bytes > 0);
        assert_eq!(*state, original);
        assert!(!state_path.exists());
    }

    #[test]
    fn commit_publishes_the_candidate_and_advances_one_revision() {
        let (state, packages, data_directory, authority) = persisted_inputs("commit");
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
                request: create_target_request("committed-target".to_string()),
                base_revision: 7,
                authority,
                state,
                packages,
                data_directory: data_directory.clone(),
            }))
        else {
            panic!("spawn-target prepare must succeed");
        };
        let HostMutationResult::Committed(committed) =
            execute(HostMutationCommand::Commit(HostCommit { prepared }))
        else {
            panic!("spawn-target commit must succeed");
        };
        assert_eq!(committed.committed_revision, 8);
        assert_eq!(
            committed.reply.response.kind,
            DaemonResponseKind::SpawnTargets
        );
        assert_eq!(
            committed.view.spawn_targets[0].target_id,
            "committed-target"
        );
        assert!(data_directory.join("hub-state.json").is_file());
        fs::remove_dir_all(&data_directory).expect("remove host mutation test directory");
    }

    #[test]
    fn failed_atomic_commit_returns_the_typed_previous_view() {
        let (state, packages, data_directory, authority) = persisted_inputs("recover");
        let expected = (*state).clone();
        let prior_bytes =
            fs::read(data_directory.join("hub-state.json")).expect("read initial state fixture");
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
                request: create_target_request("recovered-target".to_string()),
                base_revision: 2,
                authority,
                state,
                packages,
                data_directory: data_directory.clone(),
            }))
        else {
            panic!("spawn-target prepare must succeed");
        };
        FileHubStateStore::inject_next_save_failure(&data_directory);
        let HostMutationResult::Recovered(RecoveryOutcome::SpawnTarget { view, failure }) =
            execute(HostMutationCommand::Commit(HostCommit { prepared }))
        else {
            panic!("failed spawn-target commit must recover");
        };
        assert_eq!(*view, expected);
        assert_eq!(failure.code, "hub_state_commit_failed");
        assert_eq!(
            fs::read(data_directory.join("hub-state.json")).expect("read unchanged state"),
            prior_bytes
        );
        fs::remove_dir_all(&data_directory).expect("remove host mutation test directory");
    }

    #[test]
    fn uncertain_commit_keeps_both_views_and_does_not_run_recovery() {
        let (state, packages, data_directory, authority) = persisted_inputs("uncertain-commit");
        let expected = (*state).clone();
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
                request: create_target_request("uncertain-target".to_string()),
                base_revision: 2,
                authority,
                state,
                packages,
                data_directory: data_directory.clone(),
            }))
        else {
            panic!("spawn-target preparation must succeed");
        };
        FileHubStateStore::inject_next_directory_sync_failure(&data_directory);
        let HostMutationResult::PublishedUncertain { write, rollback } =
            execute(HostMutationCommand::Commit(HostCommit { prepared }))
        else {
            panic!("directory sync failure must return the uncertain write");
        };
        assert_eq!(write.base_revision(), 2);
        assert_eq!(write.committed_revision(), 3);
        assert!(rollback.is_none());
        assert_eq!(write.prior(), Some(&expected));
        assert_eq!(
            write.candidate().spawn_targets[0].target_id,
            "uncertain-target"
        );
        drop(write);
        fs::remove_dir_all(&data_directory).expect("remove host mutation test directory");
    }

    #[test]
    fn repo_session_type_commit_writes_the_repo_before_publishing_state() {
        let (config, state, packages, data_directory, target_id, authority) =
            session_type_inputs("session-type-commit");
        let repo_file = data_directory.join("repo/.botster/session-types.json");
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SessionType {
                request: session_type_create_request(target_id),
                base_revision: 11,
                authority,
                config,
                state,
                packages,
                data_directory: data_directory.clone(),
            }))
        else {
            panic!("session-type prepare must succeed");
        };
        assert!(!repo_file.exists());
        let HostMutationResult::Committed(committed) =
            execute(HostMutationCommand::Commit(HostCommit { prepared }))
        else {
            panic!("session-type commit must succeed");
        };
        assert_eq!(committed.committed_revision, 12);
        assert_eq!(committed.view.session_type_generation, 1);
        assert_eq!(
            committed.reply.response.kind,
            DaemonResponseKind::SessionTypes
        );
        assert!(repo_file.is_file());
        fs::remove_dir_all(&data_directory).expect("remove host mutation test directory");
    }

    #[test]
    fn failed_state_commit_keeps_the_repo_effect_and_durable_intent() {
        let (config, state, packages, data_directory, target_id, authority) =
            session_type_inputs("session-type-recovery");
        let repo_file = data_directory.join("repo/.botster/session-types.json");
        let prior_state_bytes =
            fs::read(data_directory.join("hub-state.json")).expect("read initial state fixture");
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SessionType {
                request: session_type_create_request(target_id),
                base_revision: 4,
                authority,
                config,
                state,
                packages,
                data_directory: data_directory.clone(),
            }))
        else {
            panic!("session-type prepare must succeed");
        };
        FileHubStateStore::inject_next_save_failure(&data_directory);
        let HostMutationResult::ExternalEffectUncertain {
            pending,
            rollback:
                RollbackDescriptor::SessionType {
                    repo_file: Some(prior),
                    ..
                },
            cause: ExternalEffectCause::RepoSyncedStateRefused(_),
        } = execute(HostMutationCommand::Commit(HostCommit { prepared }))
        else {
            panic!("failed state commit must retain the repo effect for reconciliation");
        };
        assert_eq!(pending.receipt_sequence(), 2);
        assert!(matches!(prior.prior, RepoSessionTypeFileSnapshot::Missing));
        assert!(repo_file.is_file());
        assert!(data_directory.join("hub-recovery.log").is_file());
        assert_eq!(
            fs::read(data_directory.join("hub-state.json")).expect("read retained state"),
            prior_state_bytes
        );
        drop(pending);
        assert!(matches!(
            crate::recovery::journal::RecoveryJournal::scan(
                crate::recovery::state_directory::StateDirectoryOwnership::acquire(&data_directory)
                    .expect("reopen state directory"),
                true,
            ),
            Err(crate::recovery::journal::JournalError::Unresolved(2))
        ));
        fs::remove_dir_all(&data_directory).expect("remove host mutation test directory");
    }

    #[test]
    fn uncertain_session_type_commit_retains_repo_file_rollback() {
        let (config, state, packages, data_directory, target_id, authority) =
            session_type_inputs("session-type-uncertain");
        let repo_file = data_directory.join("repo/.botster/session-types.json");
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SessionType {
                request: session_type_create_request(target_id),
                base_revision: 4,
                authority,
                config,
                state,
                packages,
                data_directory: data_directory.clone(),
            }))
        else {
            panic!("session-type preparation must succeed");
        };
        FileHubStateStore::inject_next_directory_sync_failure(&data_directory);
        let HostMutationResult::PublishedUncertain {
            write,
            rollback:
                Some(RollbackDescriptor::SessionType {
                    repo_file: Some(prior),
                    ..
                }),
        } = execute(HostMutationCommand::Commit(HostCommit { prepared }))
        else {
            panic!("uncertain state publication must retain the repo rollback");
        };
        assert!(repo_file.is_file());
        assert_eq!(write.candidate().session_type_generation, 1);
        assert_eq!(
            fs::canonicalize(prior.root.join(".botster/session-types.json"))
                .expect("canonicalize retained repo file"),
            fs::canonicalize(&repo_file).expect("canonicalize fixture repo file"),
        );
        assert!(matches!(prior.prior, RepoSessionTypeFileSnapshot::Missing));
        drop(write);
        fs::remove_dir_all(&data_directory).expect("remove host mutation test directory");
    }

    #[test]
    fn repo_directory_sync_failure_retains_the_intent_and_prior_file() {
        let (config, state, packages, data_directory, target_id, authority) =
            session_type_inputs("repo-directory-sync-uncertain");
        let root = data_directory.join("repo");
        let repo_file = root.join(".botster/session-types.json");
        let prior_state_bytes =
            fs::read(data_directory.join("hub-state.json")).expect("read initial state fixture");
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SessionType {
                request: session_type_create_request(target_id),
                base_revision: 4,
                authority,
                config,
                state,
                packages,
                data_directory: data_directory.clone(),
            }))
        else {
            panic!("session-type preparation must succeed");
        };
        crate::session_types::inject_next_repo_directory_sync_failure(&root);
        let HostMutationResult::ExternalEffectUncertain {
            pending,
            rollback:
                RollbackDescriptor::SessionType {
                    repo_file: Some(prior),
                    ..
                },
            cause: ExternalEffectCause::RepoPublicationSyncUnconfirmed(error),
        } = execute(HostMutationCommand::Commit(HostCommit { prepared }))
        else {
            panic!("repo rename with failed directory sync must retain uncertainty");
        };
        assert_eq!(error.kind, "repo_session_type_sync_uncertain");
        assert_eq!(pending.receipt_sequence(), 2);
        assert!(matches!(prior.prior, RepoSessionTypeFileSnapshot::Missing));
        assert!(repo_file.is_file());
        assert!(data_directory.join("hub-recovery.log").is_file());
        assert_eq!(
            fs::read(data_directory.join("hub-state.json")).expect("read retained state"),
            prior_state_bytes
        );
        drop(pending);
        assert!(matches!(
            crate::recovery::journal::RecoveryJournal::scan(
                crate::recovery::state_directory::StateDirectoryOwnership::acquire(&data_directory)
                    .expect("reopen state directory"),
                true,
            ),
            Err(crate::recovery::journal::JournalError::Unresolved(2))
        ));
        fs::remove_dir_all(&data_directory).expect("remove host mutation test directory");
    }

    #[test]
    fn failed_session_type_compensation_retains_its_rollback() {
        let (state, _packages, data_directory, _authority) =
            inputs("session-type-partial-recovery");
        let unavailable_root = data_directory.join("unavailable-repo");
        let failure = HostMutationError::new("commit_failed", "commit failed");
        let HostMutationResult::Recovered(RecoveryOutcome::SessionType {
            view,
            failure: recovered_failure,
            recovery:
                SessionTypeRecovery::Partial {
                    compensation_failure,
                    rollback,
                },
        }) = execute(HostMutationCommand::Recover(HostRecover {
            rollback: RollbackDescriptor::SessionType {
                previous: state.clone(),
                repo_file: Some(RepoFileRollback {
                    root: unavailable_root.clone(),
                    prior: RepoSessionTypeFileSnapshot::Missing,
                }),
            },
            failure: failure.clone(),
        }))
        else {
            panic!("failed compensation must retain the rollback");
        };
        assert!(SharedView::ptr_eq(&view, &state));
        assert_eq!(recovered_failure, failure);
        assert_eq!(compensation_failure.code, "target_not_admitted");
        assert_eq!(rollback.root, unavailable_root);
        assert!(matches!(
            rollback.prior,
            RepoSessionTypeFileSnapshot::Missing
        ));
    }

    #[test]
    fn explicit_recovery_preserves_its_typed_family() {
        let (state, _packages, _directory, _authority) = inputs("explicit-recovery");
        let failure = HostMutationError::new("commit_failed", "commit failed");
        let HostMutationResult::Recovered(RecoveryOutcome::RegisteredWorktree {
            view,
            failure: recovered_failure,
        }) = execute(HostMutationCommand::Recover(HostRecover {
            rollback: RollbackDescriptor::RegisteredWorktree {
                previous: state.clone(),
            },
            failure: failure.clone(),
        }))
        else {
            panic!("registered-worktree recovery must preserve its family");
        };
        assert!(SharedView::ptr_eq(&view, &state));
        assert_eq!(recovered_failure, failure);
    }

    #[test]
    fn logical_byte_overflow_is_rejected() {
        let error = checked_total(&[usize::MAX, 1]).expect_err("sum must overflow");
        assert_eq!(error.code, "host_logical_bytes_overflow");
    }

    #[test]
    fn family_mismatch_is_rejected_before_commit() {
        let (state, packages, data_directory, authority) = inputs("family-mismatch");
        let HostMutationResult::Prepared(mut prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
                request: create_target_request("mismatch-target".to_string()),
                base_revision: 1,
                authority,
                state: state.clone(),
                packages,
                data_directory,
            }))
        else {
            panic!("spawn-target prepare must succeed");
        };
        prepared.rollback = RollbackDescriptor::PackageConfiguration { previous: state };
        let HostMutationResult::Failed(error) =
            execute(HostMutationCommand::Commit(HostCommit { prepared }))
        else {
            panic!("family mismatch must fail");
        };
        assert_eq!(error.code, "host_mutation_family_mismatch");
    }
}
