//! Typed host-side bodies for durable Hub reads and mutations.

use std::collections::BTreeMap;
use std::io;
use std::mem;
use std::path::PathBuf;

use botster_core::PackageConfigurationValue;
#[cfg(test)]
use botster_hub_client::DaemonResponseKind;
use botster_hub_client::{DaemonEvent, DaemonRequest, DaemonResponse, MAX_CONTROL_RESPONSE_BYTES};

use crate::client_api::HubClientPackage;
use crate::client_api_dto::response::{
    daemon_apps, daemon_packages, daemon_spawn_target_validation, daemon_spawn_targets,
    daemon_worktrees,
};
use crate::client_api_dto::workspace::worktree_lifecycle_event;
use crate::daemon::control::session_types::{
    ensure_repo_session_types_valid_for_enabled_root, is_invalid_repo_session_types_error,
    session_type_catalog_entities,
};
use crate::daemon::error::DaemonTransportError;
use crate::daemon_projection::apps_from_registry;
use crate::entrypoint_supervisor::EntrypointProcessSnapshot;
use crate::host_executor::HOST_PREPARED_BYTE_CAPACITY;
use crate::packages::{PackageAction, PackageAdmissionReason, PackageRegistryError};
use crate::persistence::{FileHubStateStore, HubState, PreparedHubStateWrite};
use crate::shared_view::SharedView;
use crate::{
    PackageRegistry, SpawnTargetCreate, SpawnTargetError, SpawnTargetUpdate, WorktreeCreate,
};

/// Execute one owned host mutation command.
pub(crate) fn execute(command: HostMutationCommand) -> HostMutationResult {
    let result = match command {
        HostMutationCommand::Read(read) => execute_read(read).map(HostMutationResult::ReadReady),
        HostMutationCommand::Prepare(prepare) => {
            execute_prepare(prepare).map(HostMutationResult::Prepared)
        }
        HostMutationCommand::Commit(commit) => return execute_commit(commit),
        HostMutationCommand::Recover(recover) => {
            Ok(HostMutationResult::Recovered(execute_recovery(recover)))
        }
    };
    result.unwrap_or_else(HostMutationResult::Failed)
}

/// One family-specific host command.
pub(crate) enum HostMutationCommand {
    Read(HostRead),
    Prepare(HostPrepare),
    Commit(HostCommit),
    Recover(HostRecover),
}

impl std::fmt::Debug for HostMutationCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Read(HostRead::Package { .. }) => "ReadPackage",
            Self::Read(HostRead::SpawnTarget { .. }) => "ReadSpawnTarget",
            Self::Prepare(HostPrepare::Package { .. }) => "PreparePackage",
            Self::Prepare(HostPrepare::SpawnTarget { .. }) => "PrepareSpawnTarget",
            Self::Commit(_) => "Commit",
            Self::Recover(_) => "Recover",
        };
        formatter.write_str(name)
    }
}

/// Owned inputs for a host read.
pub(crate) enum HostRead {
    Package {
        request: DaemonRequest,
        packages: SharedView<PackageRegistry>,
        entrypoint_processes: Vec<EntrypointProcessSnapshot>,
    },
    SpawnTarget {
        request: DaemonRequest,
        state: SharedView<HubState>,
    },
}

/// Owned inputs for mutation preparation.
pub(crate) enum HostPrepare {
    Package {
        request: DaemonRequest,
        base_revision: u64,
        state: SharedView<HubState>,
        packages: SharedView<PackageRegistry>,
        entrypoint_processes: Vec<EntrypointProcessSnapshot>,
        data_directory: PathBuf,
    },
    SpawnTarget {
        request: DaemonRequest,
        base_revision: u64,
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

/// One typed host result.
pub(crate) enum HostMutationResult {
    ReadReady(HostReply),
    Prepared(PreparedMutation),
    Committed(CommittedView),
    Recovered(RecoveryOutcome),
    Failed(HostMutationError),
}

impl std::fmt::Debug for HostMutationResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
            Self::Recovered(_) => formatter.write_str("Recovered(..)"),
            Self::Failed(error) => formatter.debug_tuple("Failed").field(error).finish(),
        }
    }
}

/// A response and its checked logical encoded-byte count.
pub(crate) struct HostReply {
    pub(crate) response: DaemonResponse,
    pub(crate) logical_bytes: usize,
}

impl HostReply {
    fn try_new(response: DaemonResponse) -> Result<Self, HostMutationError> {
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
}

/// A prepared state write and the response produced by that write.
pub(crate) struct PreparedStateChange {
    store: FileHubStateStore,
    write: PreparedHubStateWrite,
    reply: HostReply,
}

/// A rollback descriptor that retains the previous published view.
pub(crate) enum RollbackDescriptor {
    PackageConfiguration { previous: SharedView<HubState> },
    SpawnTarget { previous: SharedView<HubState> },
    RegisteredWorktree { previous: SharedView<HubState> },
}

/// A committed immutable state view and its reply.
pub(crate) struct CommittedView {
    pub(crate) committed_revision: u64,
    pub(crate) view: SharedView<HubState>,
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
}

/// A stable host mutation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostMutationError {
    pub(crate) code: String,
    pub(crate) message: String,
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

fn execute_read(read: HostRead) -> Result<HostReply, HostMutationError> {
    let response = match read {
        HostRead::Package {
            request,
            packages,
            entrypoint_processes,
        } => package_read(request, &packages, entrypoint_processes)?,
        HostRead::SpawnTarget { request, state } => spawn_target_read(request, &state)?,
    };
    HostReply::try_new(response)
}

fn package_read(
    request: DaemonRequest,
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
        request => Err(HostMutationError::unsupported(&request, "package read")),
    }
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

fn execute_prepare(prepare: HostPrepare) -> Result<PreparedMutation, HostMutationError> {
    match prepare {
        HostPrepare::Package {
            request,
            base_revision,
            state,
            packages,
            entrypoint_processes,
            data_directory,
        } => prepare_package(
            request,
            base_revision,
            state,
            packages,
            entrypoint_processes,
            data_directory,
        ),
        HostPrepare::SpawnTarget {
            request,
            base_revision,
            state,
            packages,
            data_directory,
        } => prepare_spawn_target(request, base_revision, state, packages, data_directory),
    }
}

fn prepare_package(
    request: DaemonRequest,
    base_revision: u64,
    state: SharedView<HubState>,
    packages: SharedView<PackageRegistry>,
    entrypoint_processes: Vec<EntrypointProcessSnapshot>,
    data_directory: PathBuf,
) -> Result<PreparedMutation, HostMutationError> {
    let DaemonRequest::SetPackageConfiguration {
        package_name,
        values,
    } = request
    else {
        return Err(HostMutationError::unsupported(&request, "package prepare"));
    };
    let values = decode_package_configuration(values)?;
    let mut candidate_packages = (*packages).clone();
    candidate_packages
        .set_configuration(&package_name, values, "daemon socket configure package")
        .map_err(package_error)?;
    let mut row = candidate_packages
        .package(&package_name)
        .map(|record| HubClientPackage::from_record(&candidate_packages, record))
        .expect("successful package configuration retains the package");
    apply_entrypoint_processes(std::slice::from_mut(&mut row), entrypoint_processes);
    let reply = HostReply::try_new(daemon_packages(vec![row]))?;
    let mut candidate_state = (*state).clone();
    candidate_state.package_registry = candidate_packages.snapshot();
    prepare_state_change(
        base_revision,
        state,
        candidate_state,
        data_directory,
        reply,
        MutationFamily::PackageConfiguration,
    )
}

fn prepare_spawn_target(
    request: DaemonRequest,
    base_revision: u64,
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
        state,
        candidate,
        data_directory,
        HostReply::try_new(reply)?,
        family,
    )
}

fn prepare_state_change(
    base_revision: u64,
    previous: SharedView<HubState>,
    candidate: HubState,
    data_directory: PathBuf,
    reply: HostReply,
    family: MutationFamily,
) -> Result<PreparedMutation, HostMutationError> {
    let state_bytes = pretty_encoded_len(&candidate, "host_prepared_state_encode_failed")?;
    let logical_bytes = checked_total(&[state_bytes, rollback_descriptor_bytes()])?;
    if logical_bytes > HOST_PREPARED_BYTE_CAPACITY {
        return Err(HostMutationError::new(
            "host_prepared_too_large",
            "prepared mutation exceeds its prepared-byte reservation",
        ));
    }
    let store = FileHubStateStore::for_data_directory(data_directory);
    let write = store
        .prepare_shared(candidate, &previous.budget())
        .map_err(|error| HostMutationError::new("hub_state_prepare_failed", error.to_string()))?;
    let change = PreparedStateChange {
        store,
        write,
        reply,
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
    let PreparedStateChange {
        store,
        write,
        reply,
    } = into_state_change(change);
    match store.commit_shared(write) {
        Ok(view) => HostMutationResult::Committed(CommittedView {
            committed_revision,
            view,
            reply,
        }),
        Err(error) => {
            let failure = HostMutationError::new("hub_state_commit_failed", error.to_string());
            HostMutationResult::Recovered(execute_recovery(HostRecover { rollback, failure }))
        }
    }
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
    }
}

fn into_state_change(change: PreparedChange) -> PreparedStateChange {
    match change {
        PreparedChange::PackageConfiguration(change)
        | PreparedChange::SpawnTarget(change)
        | PreparedChange::RegisteredWorktree(change) => change,
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

fn daemon_error(error: DaemonTransportError) -> HostMutationError {
    match error {
        DaemonTransportError::Client(crate::HubClientError::SessionType {
            kind, message, ..
        }) => HostMutationError::new(kind, message),
        error => HostMutationError::new("host_mutation_failed", error.to_string()),
    }
}

fn package_error(error: PackageRegistryError) -> HostMutationError {
    HostMutationError::new(
        "package_policy_rejected",
        format!(
            "package {} was rejected: {:?}",
            error.package_name, error.reason
        ),
    )
}

fn spawn_error(error: SpawnTargetError) -> HostMutationError {
    HostMutationError::new(error.kind, error.message)
}

fn worktree_error(error: crate::WorktreeError) -> HostMutationError {
    HostMutationError::new(error.kind, error.message)
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
    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
    use crate::packages::{HubPackageEvents, HubPackageManifest, PackageProvenance};
    use crate::shared_view::SharedViewBudget;
    use botster_core::{
        ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, PackageConfigurationField,
        PackageConfigurationFieldType, PackageConfigurationSchema, PackageSource,
    };
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn inputs(name: &str) -> (SharedView<HubState>, SharedView<PackageRegistry>, PathBuf) {
        let data_directory = unique_test_dir(name);
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(data_directory.clone()),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .expect("build host mutation test config");
        let budget = SharedViewBudget::new();
        let packages = PackageRegistry::new(botster_core::CapabilitySet::new());
        let state = HubState::from_config(&config);
        (
            SharedView::try_new(&budget, state, 1).expect("state view fits"),
            SharedView::try_new(&budget, packages, 1).expect("package view fits"),
            data_directory,
        )
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

    fn package_inputs(name: &str) -> (SharedView<HubState>, SharedView<PackageRegistry>, PathBuf) {
        let (state, _empty_packages, data_directory) = inputs(name);
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
        let budget = SharedViewBudget::new();
        (
            SharedView::try_new(&budget, matched_state, 1).expect("state view fits"),
            SharedView::try_new(&budget, packages, 1).expect("package view fits"),
            data_directory,
        )
    }

    #[test]
    fn read_owns_input_and_has_a_deterministic_checked_reply() {
        let (state, _packages, _directory) = inputs("owned-read");
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
        let (_state, packages, _directory) = inputs("package-read");
        let HostMutationResult::ReadReady(reply) =
            execute(HostMutationCommand::Read(HostRead::Package {
                request: DaemonRequest::ListPackages,
                packages,
                entrypoint_processes: Vec::new(),
            }))
        else {
            panic!("package read must succeed");
        };
        assert_eq!(reply.response.kind, DaemonResponseKind::Packages);
        assert!(reply.response.packages.is_empty());
    }

    #[test]
    fn package_configuration_prepare_keeps_the_base_registry_unchanged() {
        let (state, packages, data_directory) = package_inputs("package-prepare");
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
                state,
                packages: packages.clone(),
                entrypoint_processes: Vec::new(),
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
        let (state, packages, data_directory) = inputs("prepare-only");
        let original = (*state).clone();
        let state_path = data_directory.join("hub-state.json");
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
                request: create_target_request("prepared-target".to_string()),
                base_revision: 41,
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
        let (state, packages, data_directory) = inputs("commit");
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
                request: create_target_request("committed-target".to_string()),
                base_revision: 7,
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
        let (state, packages, data_directory) = inputs("recover");
        let expected = (*state).clone();
        let HostMutationResult::Prepared(prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
                request: create_target_request("recovered-target".to_string()),
                base_revision: 2,
                state,
                packages,
                data_directory: data_directory.clone(),
            }))
        else {
            panic!("spawn-target prepare must succeed");
        };
        FileHubStateStore::inject_next_save_failure();
        let HostMutationResult::Recovered(RecoveryOutcome::SpawnTarget { view, failure }) =
            execute(HostMutationCommand::Commit(HostCommit { prepared }))
        else {
            panic!("failed spawn-target commit must recover");
        };
        assert_eq!(*view, expected);
        assert_eq!(failure.code, "hub_state_commit_failed");
        assert!(!data_directory.join("hub-state.json").exists());
        fs::remove_dir_all(&data_directory).expect("remove host mutation test directory");
    }

    #[test]
    fn explicit_recovery_preserves_its_typed_family() {
        let (state, _packages, _directory) = inputs("explicit-recovery");
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
        let (state, packages, data_directory) = inputs("family-mismatch");
        let HostMutationResult::Prepared(mut prepared) =
            execute(HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
                request: create_target_request("mismatch-target".to_string()),
                base_revision: 1,
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
