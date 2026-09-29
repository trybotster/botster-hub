//! Bounded host execution for control reads and durable mutations.

use botster_hub_client::{
    DaemonDiagnostic, DaemonOperatorError, DaemonQuarantine, DaemonQuarantineTarget, DaemonRequest,
    DaemonResponse,
};

use crate::HubDaemon;
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::control::session_type_quarantine::{
    PendingRepoQuarantine, QuarantineCause, REPO_SESSION_TYPE_QUARANTINED, UnknownOutcomeKind,
};
use crate::daemon::error::{DaemonTransportError, PackageRollbackFailure};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::ReadyClass;
use crate::host_executor::{
    HostCommand, HostJobIdentity, HostResult, HostSubmissionFailure, HostSubmitError,
    HostWorkPermit,
};
use crate::host_mutations::{
    HostCommit, HostMutationCommand, HostMutationError, HostMutationResult, HostPackageEffect,
    HostPackageQuarantineRecord, HostPackageRestore, HostPackageRuntimeRestore, HostPrepare,
    HostRead, HostRecover, PackageQuarantineWrite, PackageRuntimeEffect, PreparedMutation,
    RecoveryOutcome, SessionTypeRecovery,
};
use crate::owner_identity::WaiterId;

pub(crate) enum DocumentAdmission {
    Granted,
    Busy,
    Stale,
}

/// One unresolved host operation. Each variant retains the original operation slot.
pub(crate) enum HostRecoveryRequired {
    Terminal(TerminalRecovery),
    PackageFamilyWork {
        _work: Box<super::host_family::FamilyWork>,
    },
    PackageFamilies {
        _work: Box<super::host_family::FamilyWork>,
        _fault: crate::runtime::PackageEntityCleanupError,
    },
    PackageEvents {
        _result: Box<HostMutationResult>,
        _fault: Option<crate::package_event_router::EventOwnerWorkError>,
        _submission: Option<HostSubmissionFailure>,
        _permit: Option<HostWorkPermit>,
    },
    Package(PackageRecoveryRequired),
    ManagedGit(crate::daemon::control::managed_git::ManagedGitRecoveryRequired),
    Submission {
        failure: HostSubmissionFailure,
        package_restore: Option<(PackageRuntimeEffect, DaemonTransportError)>,
        managed_worktree: Option<crate::managed_git_worktrees::PreparedManagedWorktree>,
    },
}

pub(crate) struct TerminalRecovery {
    family: Option<Box<super::host_family::FamilyWork>>,
    job: crate::host_disposal::Job,
}

impl HostRecoveryRequired {
    fn into_terminal(self, identity: HostJobIdentity) -> Self {
        let (family, parts) = match self {
            Self::Terminal(_) => return self,
            Self::PackageFamilyWork { mut _work } => {
                let Some(parts) = _work.take_terminal_parts(identity) else {
                    return Self::PackageFamilyWork { _work };
                };
                (Some(_work), parts)
            }
            Self::PackageFamilies { mut _work, _fault } => {
                let Some(parts) = _work.take_terminal_parts(identity) else {
                    return Self::PackageFamilies { _work, _fault };
                };
                (Some(_work), parts.with_payload(_fault))
            }
            Self::PackageEvents {
                _result,
                _fault,
                _submission,
                _permit,
            } => {
                let (identity, permit, command): (_, _, Option<Box<dyn Send>>) =
                    match (_permit, _submission) {
                        (Some(permit), submission) => (
                            identity,
                            permit,
                            submission.map(|failure| Box::new(failure) as Box<dyn Send>),
                        ),
                        (None, Some(failure)) => (
                            failure.identity,
                            failure.permit,
                            Some(Box::new(failure.command)),
                        ),
                        (None, None) => {
                            return Self::PackageEvents {
                                _result,
                                _fault,
                                _submission: None,
                                _permit: None,
                            };
                        }
                    };
                (
                    None,
                    crate::host_disposal::Parts {
                        storage: None,
                        identity,
                        permit,
                        model: None,
                        payload: Box::new((_result, _fault, command)),
                    },
                )
            }
            Self::Package(recovery) => (
                None,
                crate::host_disposal::Parts {
                    storage: None,
                    identity,
                    permit: recovery._permit,
                    model: None,
                    payload: Box::new((recovery.original, recovery.compensation, recovery._effect)),
                },
            ),
            Self::ManagedGit(recovery) => (None, recovery.into_terminal(identity)),
            Self::Submission {
                failure,
                package_restore,
                managed_worktree,
            } => (
                None,
                crate::host_disposal::Parts {
                    storage: None,
                    identity: failure.identity,
                    permit: failure.permit,
                    model: None,
                    payload: Box::new((failure.command, package_restore, managed_worktree)),
                },
            ),
        };
        Self::Terminal(TerminalRecovery {
            family,
            job: crate::host_disposal::Job::new(parts),
        })
    }
}

pub(crate) fn dispose_terminal_recovery(
    runtime: &crate::HubRuntime,
    state: &mut DaemonControlState,
) {
    let mut cursor = None;
    loop {
        let next = match cursor {
            None => state.host_recovery.keys().next().copied(),
            Some(previous) => state
                .host_recovery
                .range((
                    std::ops::Bound::Excluded(previous),
                    std::ops::Bound::Unbounded,
                ))
                .next()
                .map(|(key, _)| *key),
        };
        let Some(waiter_id) = next else {
            break;
        };
        cursor = Some(waiter_id);
        let mut recovery = state
            .host_recovery
            .remove(&waiter_id)
            .expect("the original recovery row exists");
        if let HostRecoveryRequired::PackageFamilyWork { _work, .. }
        | HostRecoveryRequired::PackageFamilies { _work, .. } = &mut recovery
        {
            let mut completion = state.host_completions.remove(&waiter_id);
            _work.retain_terminal_completion(&mut completion);
            if let Some(completion) = completion {
                state.host_completions.insert(waiter_id, completion);
            }
        }
        let mut recovery = recovery.into_terminal(HostJobIdentity {
            waiter_id,
            phase: 0,
        });
        if let HostRecoveryRequired::Terminal(terminal) = &mut recovery
            && let crate::host_disposal::Poll::Disposed(permit) = terminal.job.poll()
        {
            if let Some(family) = terminal.family.as_mut() {
                assert!(
                    family.retire_terminal(runtime),
                    "disposal precedes causal retirement"
                );
            }
            drop(permit);
            continue;
        }
        state.host_recovery.insert(waiter_id, recovery);
    }
}

/// Retain a rejected phase without releasing its document reservation.
pub(crate) fn retain_submission(
    state: &mut DaemonControlState,
    failure: HostSubmissionFailure,
) -> ControlPoll {
    let response = submit_error_response(failure.error);
    state.host_recovery.insert(
        failure.identity.waiter_id,
        HostRecoveryRequired::Submission {
            failure,
            package_restore: None,
            managed_worktree: None,
        },
    );
    ControlPoll::Ready(Ok(response))
}

/// One package rollback that keeps one host slot until the daemon restarts.
/// The retained row leaves seven host slots available during degraded operation.
pub(crate) struct PackageRecoveryRequired {
    pub(crate) original: String,
    pub(crate) compensation: String,
    /// The stranded packages this record covers. An explicit operator enable
    /// or reload of one resolves it; the record is released once none remain.
    pub(crate) packages: std::collections::BTreeSet<String>,
    /// Milliseconds since the Unix epoch.
    pub(crate) quarantined_at_ms: u64,
    _effect: PackageRuntimeEffect,
    _permit: HostWorkPermit,
    /// A staged generation whose quarantine phase never ran. The next package
    /// effect takes it and aborts its router entry before staging.
    staged: Option<crate::package_event_router::StagedGeneration>,
}

impl PackageRecoveryRequired {
    fn new(
        original: String,
        compensation: String,
        effect: PackageRuntimeEffect,
        permit: HostWorkPermit,
    ) -> Self {
        use crate::daemon::error::{COMPENSATION_MESSAGE_BOUND, bound_compensation_message};
        Self {
            original: bound_compensation_message(original, COMPENSATION_MESSAGE_BOUND),
            compensation: bound_compensation_message(compensation, COMPENSATION_MESSAGE_BOUND),
            quarantined_at_ms: crate::daemon::control::session_type_quarantine::unix_millis(
                std::time::SystemTime::now(),
            ),
            packages: effect
                .package_names()
                .into_iter()
                .map(str::to_string)
                .collect(),
            _effect: effect,
            _permit: permit,
            staged: None,
        }
    }
}

/// One package Status row, borrowed from its source.
struct PackageQuarantineRow<'a> {
    package_name: &'a str,
    original: &'a str,
    compensation: &'a str,
    durable: bool,
    loaded: bool,
    quarantined_at_ms: u64,
}

/// Visit every quarantined package once: durable registry records first,
/// then stranded packages that only this run's recovery records cover, then
/// stranded packages with no record at all, whose failure text is empty.
/// `stranded` is the set as read under its lock: a Host worker can add to
/// it, so a count and the copy it admits must read one hold of that lock.
fn for_each_package_quarantine(
    state: &DaemonControlState,
    registry: &crate::PackageRegistry,
    lifecycle: Option<&crate::HubPluginLifecycle>,
    stranded: Option<&std::collections::BTreeSet<String>>,
    mut visit: impl FnMut(PackageQuarantineRow<'_>),
) {
    let loaded = |name: &str| lifecycle.is_some_and(|lifecycle| lifecycle.is_loaded(name));
    let durable = |name: &str| {
        registry
            .package(name)
            .is_some_and(|record| record.quarantine.is_some())
    };
    for record in registry.package_records() {
        if let Some(quarantine) = record.quarantine.as_ref() {
            visit(PackageQuarantineRow {
                package_name: &record.manifest.name,
                original: &quarantine.original,
                compensation: &quarantine.compensation,
                durable: true,
                loaded: loaded(&record.manifest.name),
                quarantined_at_ms: quarantine.quarantined_at_ms,
            });
        }
    }
    let recoveries = || {
        state
            .host_recovery
            .values()
            .filter_map(|recovery| match recovery {
                HostRecoveryRequired::Package(record) => Some(record),
                _ => None,
            })
    };
    for (index, recovery) in recoveries().enumerate() {
        for package_name in &recovery.packages {
            let listed = recoveries()
                .take(index)
                .any(|earlier| earlier.packages.contains(package_name));
            if !durable(package_name) && !listed {
                visit(PackageQuarantineRow {
                    package_name,
                    original: &recovery.original,
                    compensation: &recovery.compensation,
                    durable: false,
                    loaded: loaded(package_name),
                    quarantined_at_ms: recovery.quarantined_at_ms,
                });
            }
        }
    }
    for package_name in stranded.into_iter().flatten() {
        if !durable(package_name) && !covered_by_package_recovery(state, package_name) {
            visit(PackageQuarantineRow {
                package_name,
                original: "",
                compensation: "",
                durable: false,
                loaded: loaded(package_name),
                quarantined_at_ms: 0,
            });
        }
    }
}

fn package_is_quarantined(
    daemon: &HubDaemon,
    state: &DaemonControlState,
    package_name: &str,
) -> bool {
    daemon
        .package_registry()
        .package(package_name)
        .is_some_and(|record| record.quarantine.is_some())
        || covered_by_package_recovery(state, package_name)
        || daemon
            .runtime()
            .is_some_and(|runtime| runtime.stranded_packages().contains(package_name))
}

/// The package Status row count and logical bytes, without allocating them.
pub(crate) fn package_quarantine_rows_bytes(
    state: &DaemonControlState,
    registry: &crate::PackageRegistry,
    lifecycle: Option<&crate::HubPluginLifecycle>,
    stranded: Option<&std::collections::BTreeSet<String>>,
    limit: usize,
) -> Option<(usize, usize)> {
    let mut rows = 0usize;
    let mut bytes = Some(0usize);
    for_each_package_quarantine(state, registry, lifecycle, stranded, |row| {
        rows += 1;
        bytes = bytes
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<DaemonQuarantine>()))
            .and_then(|bytes| bytes.checked_add(row.package_name.len()))
            .and_then(|bytes| bytes.checked_add(row.original.len()))
            .and_then(|bytes| bytes.checked_add(row.compensation.len()))
            .filter(|bytes| *bytes <= limit);
    });
    Some((rows, bytes?))
}

/// Append the package Status rows. Call after `package_quarantine_rows_bytes`
/// admitted them, with the same `stranded` read.
pub(crate) fn extend_package_quarantine_rows(
    state: &DaemonControlState,
    registry: &crate::PackageRegistry,
    lifecycle: Option<&crate::HubPluginLifecycle>,
    stranded: Option<&std::collections::BTreeSet<String>>,
    rows: &mut Vec<DaemonQuarantine>,
) {
    for_each_package_quarantine(state, registry, lifecycle, stranded, |row| {
        rows.push(DaemonQuarantine::Package {
            package_name: row.package_name.to_string(),
            original: row.original.to_string(),
            compensation: row.compensation.to_string(),
            durable: row.durable,
            loaded: row.loaded,
            quarantined_at_ms: row.quarantined_at_ms,
        });
    });
}

/// The package an explicit operator enable, reload, or quarantine resolve
/// names. Only these requests resolve a stranded package; an automatic
/// refresh never does.
fn explicit_resolution_target(request: &DaemonRequest) -> Option<&str> {
    match request {
        DaemonRequest::EnablePackage { package_name }
        | DaemonRequest::ReloadPackage { package_name }
        | DaemonRequest::ResolveQuarantine {
            target: DaemonQuarantineTarget::Package { package_name },
        } => Some(package_name),
        _ => None,
    }
}

/// The package an explicit enable, reload, or resolve effect settles. These
/// effects come only from those explicit operator requests.
fn explicit_effect_target(effect: &PackageRuntimeEffect) -> Option<&str> {
    match effect {
        PackageRuntimeEffect::Enable { package_name, .. }
        | PackageRuntimeEffect::Reload { package_name, .. }
        | PackageRuntimeEffect::Resolve { package_name } => Some(package_name),
        _ => None,
    }
}

fn covered_by_package_recovery(state: &DaemonControlState, package_name: &str) -> bool {
    state.host_recovery.values().any(|recovery| {
        matches!(recovery, HostRecoveryRequired::Package(record) if record.packages.contains(package_name))
    })
}

/// An explicit enable or reload of a stranded package succeeded: it is
/// resolved. Release each package recovery record that covers nothing more.
pub(crate) fn resolve_package_recovery(state: &mut DaemonControlState, package_name: &str) {
    let mut released = Vec::new();
    for (waiter, recovery) in &mut state.host_recovery {
        if let HostRecoveryRequired::Package(record) = recovery
            && record.packages.remove(package_name)
            && record.packages.is_empty()
        {
            released.push(*waiter);
        }
    }
    for waiter in released {
        state.host_recovery.remove(&waiter);
    }
}

fn host_operation_label(request: &DaemonRequest) -> &'static str {
    use crate::PackageAction;

    if matches!(request, DaemonRequest::ResolveQuarantine { .. }) {
        return "resolve_quarantine";
    }
    let action = match request {
        DaemonRequest::InstallPackageRegistryEntry { .. }
        | DaemonRequest::InstallPackageLocalPath { .. } => PackageAction::Install,
        DaemonRequest::ShowPackage { .. } => PackageAction::Show,
        DaemonRequest::SetPackageConfiguration { .. } => PackageAction::Configure,
        DaemonRequest::ReloadPackage { .. } => PackageAction::Reload,
        DaemonRequest::EnablePackageLocalPath { .. } | DaemonRequest::EnablePackage { .. } => {
            PackageAction::Enable
        }
        DaemonRequest::DisablePackage { .. } => PackageAction::Disable,
        DaemonRequest::RemovePackage { .. } => PackageAction::Remove,
        DaemonRequest::CheckPackageUpdate { .. } => PackageAction::CheckUpdate,
        DaemonRequest::PreviewPackageUpdate { .. } => PackageAction::PreviewUpdate,
        DaemonRequest::ApplyPackageUpdate { .. } => PackageAction::ApplyUpdate,
        _ => return "host_execution",
    };
    crate::daemon_projection::package_action_label(action)
}

/// Route one supported host request. A `None` result leaves the request with its existing family.
pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    request: DaemonRequest,
) -> Option<ControlStep> {
    let waiter_id = state.current_waiter_id?;
    // A durably quarantined package runs nothing until it is resolved, or
    // explicitly enabled or reloaded. Before a restart the recovery record
    // refuses these requests; after one, only the durable record remains.
    if let DaemonRequest::StartPackageEntrypoint { package_name, .. }
    | DaemonRequest::RestartPackageEntrypoint { package_name, .. } = &request
        && daemon
            .package_registry()
            .package(package_name)
            .is_some_and(|record| record.quarantine.is_some())
    {
        return Some(ControlStep::ready(error_response(
            "package_quarantined",
            host_operation_label(&request),
            &format!(
                "package {package_name} is quarantined; resolve it, or enable or reload it explicitly"
            ),
        )));
    }
    // A package resolve names a quarantine that Status lists: durable,
    // stranded, or covered by a recovery record.
    if let DaemonRequest::ResolveQuarantine {
        target: DaemonQuarantineTarget::Package { package_name },
    } = &request
        && !package_is_quarantined(daemon, state, package_name)
    {
        return Some(ControlStep::ready(error_response(
            "quarantine_not_found",
            "resolve_quarantine",
            &format!("package {package_name} is not quarantined"),
        )));
    }
    let must_finish = crate::daemon::control::pending::request_must_finish(&request);
    let operation = host_operation_label(&request);
    let (base_revision, state_view) = daemon.state_view();
    let packages = daemon.package_registry_view();
    let runtime = daemon.runtime()?;
    let config = runtime.config().clone();
    let authority = runtime.state_authority();
    let data_directory = config.data_directory.clone();
    if authority.is_none()
        && (is_package_prepare(&request)
            || is_spawn_target_prepare(&request)
            || is_session_type_prepare(&request))
    {
        return Some(ControlStep::ready(error_response(
            "state_authority_required",
            "hub_state",
            "a durable Host mutation requires retained File state authority",
        )));
    }
    let command = if matches!(request, DaemonRequest::IssueLocalWebrtcBootstrap { .. }) {
        HostMutationCommand::ValidateBootstrap {
            request,
            base_revision,
            packages,
        }
    } else if is_entrypoint_request(&request) {
        HostMutationCommand::Read(Box::new(HostRead::Entrypoint {
            request,
            config,
            packages,
        }))
    } else if is_package_read(&request) {
        HostMutationCommand::Read(Box::new(HostRead::Package {
            request,
            config: config.clone(),
            packages,
        }))
    } else if is_package_prepare(&request) {
        HostMutationCommand::Prepare(Box::new(HostPrepare::Package {
            request,
            base_revision,
            authority: authority
                .clone()
                .expect("File host mutation retains its state authority"),
            state: state_view,
            packages,
            data_directory,
            stranded: daemon
                .runtime()
                .map(crate::HubRuntime::stranded_packages)
                .unwrap_or_default(),
        }))
    } else if is_spawn_target_read(&request) {
        HostMutationCommand::Read(Box::new(HostRead::SpawnTarget {
            request,
            state: state_view,
        }))
    } else if is_spawn_target_prepare(&request) {
        HostMutationCommand::Prepare(Box::new(HostPrepare::SpawnTarget {
            request,
            base_revision,
            authority: authority
                .clone()
                .expect("File host mutation retains its state authority"),
            state: state_view,
            packages,
            data_directory,
        }))
    } else if is_session_type_read(&request) {
        HostMutationCommand::Read(Box::new(HostRead::SessionType {
            request,
            config,
            state: state_view,
            packages,
        }))
    } else if is_session_type_prepare(&request) {
        if let Some(root) = repo_session_type_root(&state_view, &request)
            && state.repo_session_type_quarantine.contains(root)
        {
            #[cfg(test)]
            state.repo_session_type_quarantine.note_refusal(
                root,
                crate::daemon::control::session_type_quarantine::QuarantineStage::Intake,
            );
            let error = quarantined_error();
            return Some(ControlStep::ready(error_response(
                &error.code,
                "session_types",
                &error.message,
            )));
        }
        if let Some(blocked_waiter) = blocked_session_type_waiter(state, &state_view, &request) {
            crate::daemon::control::pending::mark_owner_ready(
                state,
                blocked_waiter,
                ReadyClass::HostCompletion,
                crate::daemon::control::pending::READY_HOST_COMPLETION,
            );
            return Some(ControlStep::ready(error_response(
                "session_type_recovery_pending",
                "session_types",
                "the repository session-type source is blocked until recovery completes",
            )));
        }
        HostMutationCommand::Prepare(Box::new(HostPrepare::SessionType {
            request,
            base_revision,
            authority: authority.expect("File host mutation retains its state authority"),
            config,
            state: state_view,
            packages,
            data_directory,
        }))
    } else {
        return None;
    };

    let Some(permit) = runtime.host_executor().try_reserve() else {
        return Some(ControlStep::ready(error_response(
            "host_executor_full",
            "host_execution",
            "the bounded host executor has no available operation slot",
        )));
    };
    let identity = HostJobIdentity {
        waiter_id,
        phase: 1,
    };
    let observes_session_type_catalog = match &command {
        HostMutationCommand::Read(read) => matches!(**read, HostRead::SessionType { .. }),
        HostMutationCommand::Prepare(prepare) => {
            matches!(**prepare, HostPrepare::SessionType { .. })
        }
        _ => false,
    };
    if let Err(error) =
        runtime
            .host_executor()
            .submit(identity, HostCommand::Mutation(command), permit)
    {
        return Some(ControlStep::ready(submit_error_response(error.error)));
    }

    Some(ControlStep::Pending(super::pending::PendingStep {
        continuation: super::pending::ControlContinuation::HostMutation(Box::new(
            HostMutationContinuation {
                waiter_id,
                must_finish,
                operation,
                retained_prepare: None,
                prior_compensation_failure: None,
                failed_package_effect: None,
                next_phase: 2,
                family_work: None,
                event_cleanup: None,
                observes_session_type_catalog,
                commit_obligation: None,
            },
        )),
        retire: None,
        ready_class: ReadyClass::HostCompletion,
    }))
}

pub(crate) struct HostMutationContinuation {
    waiter_id: WaiterId,
    must_finish: bool,
    operation: &'static str,
    retained_prepare: Option<(PreparedMutation, HostWorkPermit)>,
    prior_compensation_failure: Option<HostMutationError>,
    failed_package_effect: Option<(PackageRuntimeEffect, DaemonTransportError)>,
    next_phase: u64,
    family_work: Option<super::host_family::FamilyWork>,
    event_cleanup: Option<(
        HostMutationResult,
        crate::package_event_router::EventOwnerWorkId,
    )>,
    /// Session-type reads and prepares may read the repository catalog. Each
    /// result of such a continuation conservatively counts as an external
    /// observation of it, even when that phase did not read the file.
    observes_session_type_catalog: bool,
    /// Pre-effect evidence held from commit submission until the commit's
    /// result arrives, so an uncertain or unknown outcome can be preserved.
    commit_obligation: Option<CommitObligation>,
}

/// What the owner keeps while a submitted commit runs on the Host.
enum CommitObligation {
    /// A repository session-types file write, with its quarantine entry funded.
    Repo(PendingRepoQuarantine),
    /// A Hub-state document write.
    Document {
        base_revision: u64,
        prior: Option<crate::shared_view::SharedView<crate::persistence::HubState>>,
        candidate: crate::shared_view::SharedView<crate::persistence::HubState>,
    },
}

/// A document commit whose Host job ended without a mutation result. The
/// publication outcome is unknown; its pre-effect views are kept with the
/// global Hub-state uncertainty obligation.
pub(crate) struct UnknownDocumentOutcome {
    _kind: UnknownOutcomeKind,
    _base_revision: u64,
    _prior: Option<crate::shared_view::SharedView<crate::persistence::HubState>>,
    _candidate: crate::shared_view::SharedView<crate::persistence::HubState>,
}

impl HostMutationContinuation {
    pub(crate) fn take_terminal_parts(
        &mut self,
        identity: HostJobIdentity,
        completion: &mut Option<crate::host_executor::HostCompletion>,
    ) -> Option<crate::host_disposal::Parts> {
        let parts = if let Some(family) = self.family_work.as_mut() {
            family.retain_terminal_completion(completion);
            family.take_terminal_parts(identity)?
        } else if let Some((prepared, permit)) = self.retained_prepare.take() {
            crate::host_disposal::Parts {
                storage: None,
                identity,
                permit,
                payload: Box::new(prepared),
                model: None,
            }
        } else {
            let (identity, result, permit) = completion.take()?.into_parts();
            crate::host_disposal::Parts {
                storage: None,
                identity,
                permit,
                payload: Box::new(result),
                model: None,
            }
        };
        Some(parts.with_payload((
            self.retained_prepare.take(),
            self.prior_compensation_failure.take(),
            self.failed_package_effect.take(),
            self.event_cleanup.take(),
            completion.take(),
        )))
    }

    pub(crate) fn retire_terminal(&mut self, runtime: &crate::HubRuntime) -> bool {
        if let Some(family) = self.family_work.as_mut()
            && !family.retire_terminal(runtime)
        {
            return false;
        }
        self.family_work.take();
        true
    }

    pub(crate) fn poll(
        &mut self,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
    ) -> ControlPoll {
        let Self {
            waiter_id,
            must_finish,
            operation,
            retained_prepare,
            prior_compensation_failure,
            failed_package_effect,
            next_phase,
            family_work,
            event_cleanup,
            observes_session_type_catalog,
            commit_obligation,
        } = self;
        let waiter_id = *waiter_id;
        let must_finish = *must_finish;
        let operation = *operation;
        if let Some((prepared, permit)) = retained_prepare.take() {
            return admit_or_park_commit(
                daemon,
                state,
                waiter_id,
                ParkableCommit {
                    prepared,
                    permit,
                    must_finish,
                    operation,
                },
                retained_prepare,
                commit_obligation,
                next_phase,
            );
        }
        let (result, permit) = if let Some(work) = family_work.as_mut() {
            match work.poll(daemon, state, waiter_id, next_phase) {
                super::host_family::Poll::Pending => return ControlPoll::Pending,
                super::host_family::Poll::Again => return ControlPoll::Again,
                super::host_family::Poll::Fault => {
                    return retain_family_cleanup(state, waiter_id, family_work.take().unwrap());
                }
                super::host_family::Poll::Complete(result, permit) => {
                    *family_work = None;
                    (*result, permit)
                }
            }
        } else {
            let Some(completion) = state.host_completions.remove(&waiter_id) else {
                return ControlPoll::Pending;
            };
            let (_identity, result, permit) = completion.into_parts();
            let result = if let Some((saved, expected)) = event_cleanup.take() {
                match result {
                    HostResult::EventOwner(Ok(completed)) if completed.identity() == &expected => {
                        saved
                    }
                    HostResult::EventOwner(Err(fault)) => {
                        return retain_event_cleanup(
                            state,
                            waiter_id,
                            saved,
                            Some(fault),
                            None,
                            Some(permit),
                        );
                    }
                    HostResult::Failed { error, .. } if error.code == "host_worker_panicked" => {
                        // The executor confirmed that the old command cannot run again.
                        let router = daemon
                            .runtime()
                            .expect("cleanup retains its runtime")
                            .package_event_router()
                            .clone();
                        let crate::package_event_router::OwnerApplyResult::Work(work) =
                            router.try_apply(expected.operation())
                        else {
                            unreachable!("package cleanup only submits unload work");
                        };
                        return submit_event_cleanup(
                            daemon,
                            state,
                            waiter_id,
                            saved,
                            work,
                            permit,
                            next_phase,
                            event_cleanup,
                        );
                    }
                    _ => {
                        return retain_event_cleanup(
                            state,
                            waiter_id,
                            saved,
                            None,
                            None,
                            Some(permit),
                        );
                    }
                }
            } else if let HostResult::Mutation(result) = result {
                result
            } else {
                // A session-type job may have touched the repository file
                // before this result, so the catalog observes it here too.
                if *observes_session_type_catalog {
                    crate::subscription::entity::note_session_type_catalog_observation(state);
                }
                // A commit whose Host job returned no mutation result has an
                // unknown outcome: preserve it, then release admission.
                if let Some(obligation) = commit_obligation.take() {
                    let (kind, detail) = match &result {
                        HostResult::Failed { error, .. } => (
                            UnknownOutcomeKind::from_host_code(&error.code),
                            format!("{}: {}", error.code, error.message),
                        ),
                        _ => (
                            UnknownOutcomeKind::Other,
                            "the host returned a non-mutation result".to_string(),
                        ),
                    };
                    return finish_unknown_commit(
                        state, waiter_id, permit, obligation, kind, &detail,
                    );
                }
                state.release_uncertain_reservation(waiter_id);
                return finish_error(
                    permit,
                    operation,
                    HostMutationError {
                        code: "host_completion_kind_mismatch".to_string(),
                        message: "the host executor returned a non-mutation result".to_string(),
                        event: None,
                    },
                );
            };
            (result, permit)
        };
        let mut result = result;
        if *observes_session_type_catalog {
            crate::subscription::entity::note_session_type_catalog_observation(state);
        }
        // Any mutation result settles the submitted commit's obligation.
        let obligation = commit_obligation.take();
        if !matches!(
            result,
            HostMutationResult::PublishedUncertain { .. }
                | HostMutationResult::PackageQuarantineRecorded {
                    write: PackageQuarantineWrite::Uncertain(_),
                    ..
                }
        ) {
            state.release_uncertain_reservation(waiter_id);
        }
        if let Some(cleanup) = package_event_cleanup(&mut result) {
            if !cleanup.event_plane_faults.is_empty() {
                return retain_event_cleanup(state, waiter_id, result, None, None, Some(permit));
            }
            if let Some(operation) = cleanup.event_plane_unloads.pop_front() {
                let router = daemon
                    .runtime()
                    .expect("cleanup retains its runtime")
                    .package_event_router()
                    .clone();
                let crate::package_event_router::OwnerApplyResult::Work(work) =
                    router.try_apply(&operation)
                else {
                    unreachable!("package cleanup only records unload work");
                };
                return submit_event_cleanup(
                    daemon,
                    state,
                    waiter_id,
                    result,
                    work,
                    permit,
                    next_phase,
                    event_cleanup,
                );
            }
            if !cleanup.unloaded_families.is_empty() {
                *family_work = Some(super::host_family::FamilyWork::new(result, permit));
                return ControlPoll::Again;
            }
        }
        match result {
            HostMutationResult::BootstrapReady {
                base_revision,
                origin,
            } => {
                if daemon.state_view().0 != base_revision {
                    return submit_phase(
                        daemon,
                        state,
                        waiter_id,
                        HostMutationCommand::ValidateBootstrap {
                            request: DaemonRequest::IssueLocalWebrtcBootstrap {
                                package_name: "botster-web".to_string(),
                                entrypoint_id: "web-client".to_string(),
                                origin,
                            },
                            base_revision: daemon.state_view().0,
                            packages: daemon.package_registry_view(),
                        },
                        permit,
                        next_phase,
                    );
                }
                let response = daemon
                    .local_webrtc()
                    .issue_bootstrap("botster-web", "web-client", &origin)
                    .map(crate::client_api_dto::response::daemon_local_webrtc_bootstrap)
                    .map_err(DaemonTransportError::from);
                drop(permit);
                ControlPoll::Ready(response)
            }
            HostMutationResult::ReadReady(reply) => finish_reply(permit, reply),
            HostMutationResult::PublishedUncertain { write, rollback } => {
                let (code, message) = write.cause().client_error();
                let cleanup = failed_package_effect.take().map(|(effect, original)| {
                    crate::daemon::owner_loop::UncertainPublicationCleanup::PackageRestore {
                        effect,
                        original,
                    }
                });
                state.retain_uncertain_publication(waiter_id, write, rollback, cleanup);
                release_document(state, waiter_id);
                // Host work completed. The unresolved Owner permit moves in pending.rs.
                drop(permit);
                ControlPoll::Ready(Ok(error_response(code, "hub_state", message)))
            }
            HostMutationResult::ExternalEffectUncertain { cause } => {
                let (code, message) = cause.client_error();
                let crate::host_mutations::ExternalEffectCause::RepoPublicationSyncUnconfirmed {
                    cause: publication,
                    _error: error,
                } = cause;
                let Some(CommitObligation::Repo(pending)) = obligation else {
                    unreachable!("a repository commit carries its funded quarantine entry");
                };
                // Scoped: only this repository root stops accepting writes.
                state.repo_session_type_quarantine.install(
                    pending,
                    QuarantineCause::PublishedUncertain(publication),
                    &error.message,
                );
                release_document(state, waiter_id);
                drop(permit);
                ControlPoll::Ready(Ok(error_response(code, "repo_session_type", message)))
            }
            HostMutationResult::RepoFileCommitted { reply } => {
                release_document(state, waiter_id);
                finish_reply(permit, reply)
            }
            HostMutationResult::Prepared(prepared) => admit_or_park_commit(
                daemon,
                state,
                waiter_id,
                ParkableCommit {
                    prepared,
                    permit,
                    must_finish,
                    operation,
                },
                retained_prepare,
                commit_obligation,
                next_phase,
            ),
            HostMutationResult::Committed(committed) => {
                let (current_revision, current_state) = daemon.state_view();
                if committed.committed_revision != current_revision.saturating_add(1) {
                    release_document(state, waiter_id);
                    return finish_error(
                        permit,
                        operation,
                        HostMutationError {
                            code: "host_commit_revision_mismatch".to_string(),
                            message:
                                "the committed host revision does not follow the published revision"
                                    .to_string(),
                            event: None,
                        },
                    );
                }
                let session_type_generation_changed =
                    current_state.session_type_generation != committed.view.session_type_generation;
                daemon.publish_state(committed.view);
                if let Some(packages) = committed.packages {
                    daemon.publish_package_registry_view(packages);
                }
                if session_type_generation_changed {
                    state
                        .maintenance
                        .wakes
                        .mark(crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery);
                }
                if let Some(effect) = committed.package_effect {
                    let runtime = daemon
                        .runtime()
                        .expect("a committed package has a running runtime");
                    let mut host_runtime = runtime.host_package_runtime();
                    host_runtime
                        .fund_staging(staging_funding(&permit, committed.reply.logical_bytes));
                    // Ownership of a retained staged generation moves to the
                    // job, which settles it before staging.
                    if let Some(staged) = take_retained_staged(state) {
                        host_runtime.settle_before_staging(staged);
                    }
                    let command =
                        HostMutationCommand::ApplyPackageEffect(Box::new(HostPackageEffect {
                            effect,
                            runtime: host_runtime,
                            config: runtime.config().clone(),
                            packages: daemon.package_registry_view(),
                            reply: committed.reply,
                        }));
                    return submit_phase(daemon, state, waiter_id, command, permit, next_phase);
                }
                release_document(state, waiter_id);
                ingest_worktree_lifecycle_events(daemon, state, &committed.reply.response.events);
                finish_reply(permit, committed.reply)
            }
            HostMutationResult::PackageRestored(restored) => {
                daemon.publish_state(restored.view);
                daemon.publish_package_registry_view(restored.packages);
                let (effect, original) = failed_package_effect
                    .take()
                    .expect("a package restore follows one failed runtime effect");
                let runtime = daemon
                    .runtime()
                    .expect("package recovery retains its runtime");
                let mut host_runtime = runtime.host_package_runtime();
                host_runtime.fund_staging(staging_funding(&permit, 0));
                // The failed effect's staged generation, if any, moves to the
                // restore, which aborts it before it stages its own.
                let staged = match state.staged_package_generation.take() {
                    Some((owner, staged)) if owner == waiter_id => Some(staged),
                    other => {
                        state.staged_package_generation = other;
                        None
                    }
                };
                let command = HostMutationCommand::RestorePackageRuntime(Box::new(
                    HostPackageRuntimeRestore {
                        effect,
                        original,
                        runtime: host_runtime,
                        config: runtime.config().clone(),
                        staged,
                        quarantine: None,
                    },
                ));
                submit_phase(daemon, state, waiter_id, command, permit, next_phase)
            }
            HostMutationResult::PackageEffectApplied {
                reply,
                cleanup,
                loaded,
            } => {
                daemon
                    .runtime_mut()
                    .expect("package effect retains its runtime")
                    .apply_host_package_cleanup(cleanup);
                if let Some(package_name) = loaded {
                    resolve_package_recovery(state, &package_name);
                }
                state
                    .maintenance
                    .wakes
                    .mark(crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery);
                release_document(state, waiter_id);
                match reply {
                    Ok(reply) => finish_reply(permit, reply),
                    Err(error) => finish_error(permit, operation, error),
                }
            }
            HostMutationResult::PackageEffectFailed {
                effect,
                error,
                mut cleanup,
            } => {
                // A generation staged but not activated stays with this
                // attempt until its restore aborts it or its unload clears it.
                if let Some(staged) = cleanup.staged.take() {
                    state.staged_package_generation = Some((waiter_id, staged));
                }
                daemon
                    .runtime_mut()
                    .expect("package effect retains its runtime")
                    .apply_host_package_cleanup(cleanup);
                let runtime = daemon
                    .runtime()
                    .expect("package recovery retains its runtime");
                let (base_revision, current_state) = daemon.state_view();
                if let Some(restore) = effect.restore_command(
                    runtime.config().data_directory.clone(),
                    base_revision,
                    runtime
                        .state_authority()
                        .expect("File package restore retains its state authority"),
                    current_state,
                ) {
                    submit_package_restore(
                        daemon,
                        state,
                        waiter_id,
                        PackageRestoreSubmission {
                            restore,
                            effect,
                            original: error,
                            permit,
                        },
                        next_phase,
                        failed_package_effect,
                    )
                } else {
                    submit_package_quarantine(
                        daemon,
                        state,
                        waiter_id,
                        effect,
                        error,
                        PackageRollbackFailure {
                            step: "runtime",
                            package_name: None,
                            error: Box::new(DaemonTransportError::Protocol(
                                "the package effect requires runtime recovery",
                            )),
                        },
                        permit,
                        next_phase,
                    )
                }
            }
            HostMutationResult::PackageRuntimeRestored {
                effect,
                original,
                rollbacks,
                cleanup,
            } => {
                daemon
                    .runtime_mut()
                    .expect("package recovery retains its runtime")
                    .apply_host_package_cleanup(cleanup);
                if rollbacks.is_empty() {
                    release_document(state, waiter_id);
                    // The durable state and runtime are restored. A plugin
                    // load failure is this request's typed refusal, never a
                    // reason to close the client connection.
                    match package_load_refusal(&original) {
                        Some(error) => finish_error(permit, operation, error),
                        None => finish_transport_error(permit, original),
                    }
                } else {
                    submit_package_quarantine_record(
                        daemon,
                        state,
                        waiter_id,
                        PackageCompensationFailure {
                            effect,
                            original,
                            rollbacks,
                        },
                        permit,
                        next_phase,
                    )
                }
            }
            HostMutationResult::PackageQuarantineRecorded {
                write,
                effect,
                original,
                rollbacks,
            } => {
                let durable = match write {
                    PackageQuarantineWrite::Synced {
                        state: view,
                        packages,
                    } => {
                        daemon.publish_state(view);
                        daemon.publish_package_registry_view(packages);
                        Ok(())
                    }
                    PackageQuarantineWrite::NotDurable(reason) => Err(reason),
                    PackageQuarantineWrite::Uncertain(write) => {
                        // Later state writes stay refused until this
                        // publication is resolved, as for any uncertain write.
                        state.retain_uncertain_publication(waiter_id, write, None, None);
                        Err("the quarantine write's durable result is unconfirmed".to_string())
                    }
                };
                release_document(state, waiter_id);
                let failure = PackageCompensationFailure {
                    effect,
                    original,
                    rollbacks,
                };
                if let Err(reason) = durable {
                    record_quarantine_not_durable(&failure.effect, &reason);
                }
                retain_compensation_failure(state, waiter_id, failure, permit)
            }
            HostMutationResult::PackageRestoreFailed(error) => {
                let failure = PackageRollbackFailure {
                    step: "persist",
                    package_name: None,
                    error: Box::new(DaemonTransportError::State(error)),
                };
                let (effect, original) = failed_package_effect
                    .take()
                    .expect("a package restore failure follows one failed runtime effect");
                submit_package_quarantine(
                    daemon, state, waiter_id, effect, original, failure, permit, next_phase,
                )
            }
            HostMutationResult::Recovered(outcome) => match outcome {
                RecoveryOutcome::PackageConfiguration { failure, .. }
                | RecoveryOutcome::SpawnTarget { failure, .. }
                | RecoveryOutcome::RegisteredWorktree { failure, .. } => {
                    release_document(state, waiter_id);
                    finish_error(permit, operation, failure)
                }
                RecoveryOutcome::SessionType {
                    failure,
                    recovery: SessionTypeRecovery::NotRequired | SessionTypeRecovery::Restored,
                    ..
                } => {
                    release_document(state, waiter_id);
                    let failure =
                        with_compensation_failure(failure, prior_compensation_failure.take());
                    finish_error(permit, operation, failure)
                }
                RecoveryOutcome::SessionType {
                    view,
                    failure,
                    recovery:
                        SessionTypeRecovery::Partial {
                            compensation_failure,
                            rollback,
                        },
                    ..
                } => {
                    state
                        .blocked_session_type_roots
                        .insert(rollback.root.clone(), waiter_id);
                    *prior_compensation_failure = Some(compensation_failure);
                    submit_phase(
                        daemon,
                        state,
                        waiter_id,
                        HostMutationCommand::Recover(HostRecover {
                            rollback: crate::host_mutations::RollbackDescriptor::SessionType {
                                previous: view,
                                repo_file: Some(rollback),
                            },
                            failure,
                        }),
                        permit,
                        next_phase,
                    )
                }
            },
            HostMutationResult::Failed(error) => {
                if state.document_owner == Some(waiter_id) {
                    release_document(state, waiter_id);
                }
                ingest_worktree_lifecycle_events(
                    daemon,
                    state,
                    error.event.as_deref().map_or(&[], std::slice::from_ref),
                );
                finish_error(permit, operation, error)
            }
        }
    }
}

/// Deliver authoritative worktree lifecycle events to plugin subscribers
/// through the existing hub-owned router ingress. Delivery is best effort:
/// the router counts backpressure refusals, and the client response is
/// unchanged either way.
fn ingest_worktree_lifecycle_events(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    events: &[botster_hub_client::DaemonEvent],
) {
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    let router = runtime.package_event_router();
    let mut accepted = false;
    for event in events {
        let botster_hub_client::DaemonEvent::WorktreeLifecycle { event } = event else {
            continue;
        };
        let Ok(payload) = serde_json::to_value(event) else {
            continue;
        };
        accepted |= router.try_ingress(
            crate::package_event_router::HUB_EVENT_OWNER,
            &event.event,
            &payload,
            std::time::Instant::now(),
        ) == crate::package_event_router::EventPlaneStatus::Accepted;
    }
    if accepted {
        state.maintenance.try_wake();
    }
}

#[cfg(test)]
thread_local! {
    static LOSE_DOCUMENT_BEFORE_RESTORE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_QUARANTINE_SUBMISSION: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Refuse this thread's next quarantine phase submission, as a full Host
/// queue would.
#[cfg(test)]
pub(crate) fn fail_next_quarantine_submission() {
    FAIL_NEXT_QUARANTINE_SUBMISSION.with(|fail| fail.set(true));
}

/// Lose this thread's next package restore's document reservation just
/// before its admission check.
#[cfg(test)]
pub(crate) fn lose_document_before_next_restore() {
    LOSE_DOCUMENT_BEFORE_RESTORE.with(|lose| lose.set(true));
}

/// The owned parts of one package restore, in their original argument order.
struct PackageRestoreSubmission {
    restore: HostPackageRestore,
    effect: PackageRuntimeEffect,
    original: DaemonTransportError,
    permit: HostWorkPermit,
}

fn submit_package_restore(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    submission: PackageRestoreSubmission,
    next_phase: &mut u64,
    failed_package_effect: &mut Option<(PackageRuntimeEffect, DaemonTransportError)>,
) -> ControlPoll {
    let PackageRestoreSubmission {
        restore,
        effect,
        original,
        permit,
    } = submission;
    #[cfg(test)]
    if LOSE_DOCUMENT_BEFORE_RESTORE.with(|lose| lose.replace(false)) {
        release_document(state, waiter_id);
    }
    if state.document_owner != Some(waiter_id) {
        let failure = PackageRollbackFailure {
            step: "restore_admission",
            package_name: None,
            error: Box::new(DaemonTransportError::Protocol(
                "package restore requires the original document reservation",
            )),
        };
        // No durable restore can run: this is a failed compensation, so the
        // effect's packages get the full quarantine.
        return submit_package_quarantine(
            daemon, state, waiter_id, effect, original, failure, permit, next_phase,
        );
    }
    if !state.reserve_uncertain_publication(waiter_id) {
        release_document(state, waiter_id);
        let failure = PackageRollbackFailure {
            step: "restore_admission",
            package_name: None,
            error: Box::new(DaemonTransportError::Protocol(
                "another unresolved state publication owns the retention cell",
            )),
        };
        return submit_package_quarantine(
            daemon, state, waiter_id, effect, original, failure, permit, next_phase,
        );
    }

    // The document reservation remains held through the first whole-state
    // restore. No code can replay this snapshot after reservation release.
    *failed_package_effect = Some((effect, original));
    let poll = submit_phase(
        daemon,
        state,
        waiter_id,
        HostMutationCommand::RestorePackage(restore),
        permit,
        next_phase,
    );
    if let Some(HostRecoveryRequired::Submission {
        package_restore, ..
    }) = state.host_recovery.get_mut(&waiter_id)
    {
        *package_restore = failed_package_effect.take();
    }
    if !matches!(poll, ControlPoll::Pending) {
        state.release_uncertain_reservation(waiter_id);
    }
    poll
}

/// No runtime restore is possible for a failed package effect: quarantine its
/// packages. They are stranded here at once, so no event or invocation
/// reaches them even while their runtime stays loaded; the Host phase then
/// aborts the attempt's staged generation and unloads them, and the answer is
/// a failed compensation. If that phase cannot be submitted, the packages
/// stay stranded and inert until an operator resolve unloads them. Document
/// ownership is released when the phases complete.
#[allow(clippy::too_many_arguments)]
fn submit_package_quarantine(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    effect: PackageRuntimeEffect,
    original: DaemonTransportError,
    failure: PackageRollbackFailure,
    permit: HostWorkPermit,
    next_phase: &mut u64,
) -> ControlPoll {
    if let Some(runtime) = daemon.runtime() {
        for package_name in effect.package_names() {
            runtime.mark_package_stranded(package_name);
            crate::hub_log::hub_log!("package_stranded package={package_name}");
        }
    }
    let staged = match state.staged_package_generation.take() {
        Some((owner, staged)) if owner == waiter_id => Some(staged),
        other => {
            state.staged_package_generation = other;
            None
        }
    };
    let Some(runtime) = daemon.runtime() else {
        release_document(state, waiter_id);
        let poll = retain_package_recovery(
            state,
            waiter_id,
            PackageCompensationFailure {
                effect,
                original,
                rollbacks: vec![failure],
            },
            permit,
        );
        keep_staged_with_recovery(state, waiter_id, staged);
        return poll;
    };
    let command = HostMutationCommand::RestorePackageRuntime(Box::new(HostPackageRuntimeRestore {
        effect,
        original,
        runtime: runtime.host_package_runtime(),
        config: runtime.config().clone(),
        staged,
        quarantine: Some(failure),
    }));
    let identity = HostJobIdentity {
        waiter_id,
        phase: *next_phase,
    };
    let command = HostCommand::Mutation(command);
    #[cfg(test)]
    let refused_for_test = FAIL_NEXT_QUARANTINE_SUBMISSION.with(|fail| fail.replace(false));
    #[cfg(not(test))]
    let refused_for_test = false;
    let submitted = match next_phase.checked_add(1) {
        None => Err(HostSubmissionFailure {
            error: HostSubmitError::PhaseExhausted,
            identity,
            command: Box::new(command),
            permit,
        }),
        Some(_) if refused_for_test => Err(HostSubmissionFailure {
            error: HostSubmitError::Full,
            identity,
            command: Box::new(command),
            permit,
        }),
        Some(later_phase) => runtime
            .host_executor()
            .submit(identity, command, permit)
            .map(|()| later_phase),
    };
    match submitted {
        Ok(later_phase) => {
            *next_phase = later_phase;
            ControlPoll::Pending
        }
        Err(failure) => retain_unsubmitted_quarantine(state, waiter_id, failure),
    }
}

/// The quarantine phase could not be submitted. The packages are already
/// stranded, so they stay inert while loaded. The refused command's staged
/// token and the permit go to a package recovery record, not a submission
/// record: a submission record refuses all Host work, and the operator
/// resolve must still reach these packages to unload them.
fn retain_unsubmitted_quarantine(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    failure: HostSubmissionFailure,
) -> ControlPoll {
    let HostSubmissionFailure {
        error,
        command,
        permit,
        ..
    } = failure;
    let HostCommand::Mutation(HostMutationCommand::RestorePackageRuntime(restore)) = *command
    else {
        unreachable!("the quarantine phase submits a runtime restore");
    };
    let HostPackageRuntimeRestore {
        effect,
        original,
        staged,
        quarantine,
        ..
    } = *restore;
    crate::hub_log::hub_log!(
        "package_quarantine_unsubmitted packages={} error={error:?}",
        effect.package_names().join(",")
    );
    let mut rollbacks: Vec<PackageRollbackFailure> = quarantine.into_iter().collect();
    rollbacks.push(PackageRollbackFailure {
        step: "quarantine_submission",
        package_name: None,
        error: Box::new(DaemonTransportError::Protocol(
            "the Host quarantine phase could not be submitted",
        )),
    });
    release_document(state, waiter_id);
    let poll = retain_package_recovery(
        state,
        waiter_id,
        PackageCompensationFailure {
            effect,
            original,
            rollbacks,
        },
        permit,
    );
    keep_staged_with_recovery(state, waiter_id, staged);
    poll
}

/// Take the one staged generation a recovery record retains, if any. At
/// most one exists: the router holds a single pending generation.
fn take_retained_staged(
    state: &mut DaemonControlState,
) -> Option<crate::package_event_router::StagedGeneration> {
    state
        .host_recovery
        .values_mut()
        .find_map(|recovery| match recovery {
            HostRecoveryRequired::Package(record) => record.staged.take(),
            _ => None,
        })
}

fn keep_staged_with_recovery(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    staged: Option<crate::package_event_router::StagedGeneration>,
) {
    if let Some(HostRecoveryRequired::Package(record)) = state.host_recovery.get_mut(&waiter_id) {
        record.staged = staged;
    }
}

/// A failed compensation: the effect, its original failure, and every
/// rollback that failed.
struct PackageCompensationFailure {
    effect: PackageRuntimeEffect,
    original: DaemonTransportError,
    rollbacks: Vec<PackageRollbackFailure>,
}

/// Persist the quarantine that a failed compensation just applied, as one
/// more phase under this waiter's document reservation. When no write can be
/// attempted, the quarantine stays in memory only and is counted.
fn submit_package_quarantine_record(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    failure: PackageCompensationFailure,
    permit: HostWorkPermit,
    next_phase: &mut u64,
) -> ControlPoll {
    use crate::daemon::error::{COMPENSATION_MESSAGE_BOUND, bound_compensation_message};

    let authority = daemon
        .runtime()
        .and_then(crate::HubRuntime::state_authority);
    let refusal = if state.document_owner != Some(waiter_id) {
        Some("the quarantine write requires the original document reservation")
    } else if authority.is_none() {
        Some("the quarantine write requires File state authority")
    } else if !state.reserve_uncertain_publication(waiter_id) {
        Some("another unresolved state publication owns the retention cell")
    } else {
        None
    };
    if let Some(reason) = refusal {
        release_document(state, waiter_id);
        record_quarantine_not_durable(&failure.effect, reason);
        return retain_compensation_failure(state, waiter_id, failure, permit);
    }
    let runtime = daemon.runtime().expect("the state authority has a runtime");
    let (base_revision, current_state) = daemon.state_view();
    let quarantine = crate::PackageQuarantine {
        original: bound_compensation_message(
            failure.original.to_string(),
            COMPENSATION_MESSAGE_BOUND,
        ),
        compensation: bound_compensation_message(
            compensation_message(&failure.rollbacks),
            COMPENSATION_MESSAGE_BOUND,
        ),
        quarantined_at_ms: crate::daemon::control::session_type_quarantine::unix_millis(
            std::time::SystemTime::now(),
        ),
    };
    let command =
        HostMutationCommand::RecordPackageQuarantine(Box::new(HostPackageQuarantineRecord {
            base_revision,
            authority: authority.expect("checked above"),
            state: current_state,
            packages: daemon.package_registry_view(),
            data_directory: runtime.config().data_directory.clone(),
            quarantine,
            effect: failure.effect,
            original: failure.original,
            rollbacks: failure.rollbacks,
        }));
    submit_phase(daemon, state, waiter_id, command, permit, next_phase)
}

fn compensation_message(rollbacks: &[PackageRollbackFailure]) -> String {
    rollbacks
        .iter()
        .map(|failure| failure.error.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// The quarantine lasts only until the Hub restarts: log why.
fn record_quarantine_not_durable(effect: &PackageRuntimeEffect, reason: &str) {
    crate::hub_log::hub_log!(
        "package_quarantine_not_durable packages={} reason={reason}",
        effect.package_names().join(",")
    );
}

/// Keep the recovery record, which holds this request's Host slot, and
/// answer the client with the compensation failure.
fn retain_compensation_failure(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    failure: PackageCompensationFailure,
    permit: HostWorkPermit,
) -> ControlPoll {
    let PackageCompensationFailure {
        effect,
        original,
        rollbacks,
    } = failure;
    state.host_recovery.insert(
        waiter_id,
        HostRecoveryRequired::Package(PackageRecoveryRequired::new(
            original.to_string(),
            compensation_message(&rollbacks),
            effect,
            permit,
        )),
    );
    ControlPoll::Ready(Err(DaemonTransportError::PackageCompensation {
        original: Box::new(original),
        rollbacks,
    }))
}

/// No compensation or quarantine phase can run: retain the recovery in
/// memory only. The quarantine is not durable.
fn retain_package_recovery(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    failure: PackageCompensationFailure,
    permit: HostWorkPermit,
) -> ControlPoll {
    record_quarantine_not_durable(&failure.effect, "no compensation phase could run");
    retain_compensation_failure(state, waiter_id, failure, permit)
}

pub(crate) fn handles(request: &DaemonRequest) -> bool {
    matches!(request, DaemonRequest::IssueLocalWebrtcBootstrap { .. })
        || is_entrypoint_request(request)
        || is_package_read(request)
        || is_package_prepare(request)
        || is_spawn_target_read(request)
        || is_spawn_target_prepare(request)
        || is_session_type_read(request)
        || is_session_type_prepare(request)
}

/// The owned parts of one parkable commit, in their original argument order.
struct ParkableCommit {
    prepared: PreparedMutation,
    permit: HostWorkPermit,
    must_finish: bool,
    operation: &'static str,
}

fn admit_or_park_commit(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    commit: ParkableCommit,
    retained: &mut Option<(PreparedMutation, HostWorkPermit)>,
    obligation: &mut Option<CommitObligation>,
    next_phase: &mut u64,
) -> ControlPoll {
    let ParkableCommit {
        prepared,
        permit,
        must_finish,
        operation,
    } = commit;
    // Anything that reaches this site can park on the document reservation.
    // Transport closure must not retire its handoff.
    debug_assert!(must_finish, "a parkable host mutation must finish");
    // An explicit enable, reload, or resolve of a stranded package is its
    // resolution and may commit; every other package mutation waits.
    let resolves_stranded = match &prepared.change {
        crate::host_mutations::PreparedChange::PackageConfiguration(change) => change
            .package_effect()
            .and_then(explicit_effect_target)
            .is_some_and(|package_name| covered_by_package_recovery(state, package_name)),
        _ => false,
    };
    if matches!(
        prepared.change,
        crate::host_mutations::PreparedChange::PackageConfiguration(_)
    ) && !resolves_stranded
        && state
            .host_recovery
            .values()
            .any(|recovery| matches!(recovery, HostRecoveryRequired::Package(_)))
    {
        state.document_waiters.remove(&waiter_id);
        wake_next_document_waiter(state);
        return finish_error(
            permit,
            operation,
            HostMutationError {
                code: "package_recovery_required".to_string(),
                message: "package recovery is required before this prepared mutation can commit"
                    .to_string(),
                event: None,
            },
        );
    }
    match admit_document(
        state,
        waiter_id,
        prepared.base_revision,
        daemon.state_view().0,
    ) {
        DocumentAdmission::Granted => {
            if let Some(repo) = prepared.repo_evidence()
                && state.repo_session_type_quarantine.contains(&repo.root)
            {
                #[cfg(test)]
                state.repo_session_type_quarantine.note_refusal(
                    &repo.root,
                    crate::daemon::control::session_type_quarantine::QuarantineStage::CommitAdmission,
                );
                release_document(state, waiter_id);
                return finish_error(permit, operation, quarantined_error());
            }
            if !state.reserve_uncertain_publication(waiter_id) {
                release_document(state, waiter_id);
                return finish_error(
                    permit,
                    operation,
                    HostMutationError {
                        code: "state_publication_slot_occupied".to_string(),
                        message: "another unresolved state publication owns the retention cell"
                            .to_string(),
                        event: None,
                    },
                );
            }
            let held = match (prepared.repo_evidence(), prepared.document_views()) {
                (Some(repo), _) => {
                    // Fund the quarantine entry before the file effect.
                    let budget = daemon.state_view().1.budget();
                    match PendingRepoQuarantine::reserve(repo, &budget) {
                        Ok(pending) => CommitObligation::Repo(pending),
                        Err(error) => {
                            state.release_uncertain_reservation(waiter_id);
                            release_document(state, waiter_id);
                            return finish_error(
                                permit,
                                operation,
                                HostMutationError {
                                    code: "recovery_capacity_exhausted".to_string(),
                                    message: format!(
                                        "the Hub-state budget cannot hold this write's recovery evidence: {} bytes requested, {} available",
                                        error.requested, error.available
                                    ),
                                    event: None,
                                },
                            );
                        }
                    }
                }
                (None, Some((prior, candidate))) => CommitObligation::Document {
                    base_revision: prepared.base_revision,
                    prior,
                    candidate,
                },
                (None, None) => {
                    unreachable!("every prepared change writes a repository file or a document")
                }
            };
            *obligation = Some(held);
            let poll = submit_phase(
                daemon,
                state,
                waiter_id,
                HostMutationCommand::Commit(HostCommit { prepared }),
                permit,
                next_phase,
            );
            if !matches!(poll, ControlPoll::Pending) {
                // The commit never reached a Host worker, so there is no effect.
                *obligation = None;
                state.release_uncertain_reservation(waiter_id);
            }
            poll
        }
        DocumentAdmission::Busy => {
            *retained = Some((prepared, permit));
            ControlPoll::Pending
        }
        DocumentAdmission::Stale => {
            wake_next_document_waiter(state);
            finish_error(
                permit,
                operation,
                HostMutationError {
                    code: "host_prepared_revision_stale".to_string(),
                    message: "the Hub state changed while the host mutation was prepared"
                        .to_string(),
                    event: None,
                },
            )
        }
    }
}

pub(super) fn package_event_cleanup(
    result: &mut HostMutationResult,
) -> Option<&mut crate::runtime::package_effect::HostPackageCleanup> {
    match result {
        HostMutationResult::PackageEffectApplied { cleanup, .. }
        | HostMutationResult::PackageEffectFailed { cleanup, .. }
        | HostMutationResult::PackageRuntimeRestored { cleanup, .. } => Some(cleanup),
        _ => None,
    }
}

fn retain_family_cleanup(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    work: super::host_family::FamilyWork,
) -> ControlPoll {
    state.family_cleanup_waiters.remove(&waiter_id);
    if let Some(fault) = work.generation_fault {
        state.host_recovery.insert(
            waiter_id,
            HostRecoveryRequired::PackageFamilies {
                _work: Box::new(work),
                _fault: fault,
            },
        );
        return ControlPoll::Ready(Ok(error_response(
            "entity_family_generation_exhausted",
            "packages",
            "entity family cleanup exhausted generation identifiers",
        )));
    }
    state.host_recovery.insert(
        waiter_id,
        HostRecoveryRequired::PackageFamilyWork {
            _work: Box::new(work),
        },
    );
    ControlPoll::Ready(Ok(error_response(
        "entity_family_cleanup_failed",
        "packages",
        "entity family cleanup requires recovery",
    )))
}

fn retain_event_cleanup(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    result: HostMutationResult,
    fault: Option<crate::package_event_router::EventOwnerWorkError>,
    submission: Option<HostSubmissionFailure>,
    permit: Option<HostWorkPermit>,
) -> ControlPoll {
    state.host_recovery.insert(
        waiter_id,
        HostRecoveryRequired::PackageEvents {
            _result: Box::new(result),
            _fault: fault,
            _submission: submission,
            _permit: permit,
        },
    );
    ControlPoll::Ready(Ok(error_response(
        "event_plane_cleanup_failed",
        "packages",
        "event router cleanup requires recovery",
    )))
}

#[allow(clippy::too_many_arguments)]
fn submit_event_cleanup(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    result: HostMutationResult,
    work: crate::package_event_router::EventOwnerWork,
    permit: HostWorkPermit,
    next_phase: &mut u64,
    retained: &mut Option<(
        HostMutationResult,
        crate::package_event_router::EventOwnerWorkId,
    )>,
) -> ControlPoll {
    let runtime = daemon
        .runtime()
        .expect("package cleanup retains its runtime");
    let expected = work.identity().clone();
    let identity = HostJobIdentity {
        waiter_id,
        phase: *next_phase,
    };
    let command = HostCommand::EventOwner {
        router: runtime.package_event_router().clone(),
        work,
    };
    let Some(later_phase) = next_phase.checked_add(1) else {
        return retain_event_cleanup(
            state,
            waiter_id,
            result,
            None,
            Some(HostSubmissionFailure {
                error: HostSubmitError::PhaseExhausted,
                identity,
                command: Box::new(command),
                permit,
            }),
            None,
        );
    };
    match runtime.host_executor().submit(identity, command, permit) {
        Ok(()) => {
            *next_phase = later_phase;
            *retained = Some((result, expected));
            ControlPoll::Pending
        }
        Err(failure) => retain_event_cleanup(state, waiter_id, result, None, Some(failure), None),
    }
}

/// Staging funding from the attempt's prepared-byte reservation: a handle
/// that keeps it alive with the pending generation, and the reserved bytes
/// the attempt does not already hold (`held_bytes`). With no reservation the
/// funding is zero, so any staged storage is refused before it changes state.
fn staging_funding(
    permit: &HostWorkPermit,
    held_bytes: usize,
) -> crate::runtime::package_effect::StagingFunding {
    let reserved = permit.reserved_prepared_bytes();
    if reserved == 0 {
        return crate::runtime::package_effect::StagingFunding::none();
    }
    crate::runtime::package_effect::StagingFunding::new(
        std::sync::Arc::new(
            permit.retain_prepared_reservation(
                crate::host_executor::ReservationHolder::StagingFunding,
            ),
        ),
        reserved.saturating_sub(held_bytes),
    )
}

fn submit_phase(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    command: HostMutationCommand,
    permit: HostWorkPermit,
    next_phase: &mut u64,
) -> ControlPoll {
    let phase = *next_phase;
    let identity = HostJobIdentity { waiter_id, phase };
    let command = HostCommand::Mutation(command);
    let Some(later_phase) = phase.checked_add(1) else {
        return retain_submission(
            state,
            HostSubmissionFailure {
                error: HostSubmitError::PhaseExhausted,
                identity,
                command: Box::new(command),
                permit,
            },
        );
    };
    let Some(runtime) = daemon.runtime() else {
        return retain_submission(
            state,
            HostSubmissionFailure {
                error: HostSubmitError::Stopped,
                identity,
                command: Box::new(command),
                permit,
            },
        );
    };
    match runtime.host_executor().submit(identity, command, permit) {
        Ok(()) => {
            *next_phase = later_phase;
            ControlPoll::Pending
        }
        Err(failure) => retain_submission(state, failure),
    }
}

pub(crate) fn admit_document(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    base_revision: u64,
    current_revision: u64,
) -> DocumentAdmission {
    match state.document_owner {
        Some(owner) if owner != waiter_id => {
            state.document_waiters.insert(waiter_id);
            DocumentAdmission::Busy
        }
        Some(_) => DocumentAdmission::Granted,
        None if base_revision != current_revision => DocumentAdmission::Stale,
        None => {
            state.document_waiters.remove(&waiter_id);
            state.document_owner = Some(waiter_id);
            DocumentAdmission::Granted
        }
    }
}

pub(crate) fn release_document(state: &mut DaemonControlState, waiter_id: WaiterId) {
    if state.document_owner != Some(waiter_id) {
        return;
    }
    state.document_owner = None;
    state
        .blocked_session_type_roots
        .retain(|_, blocked_waiter| *blocked_waiter != waiter_id);
    wake_next_document_waiter(state);
}

fn wake_next_document_waiter(state: &mut DaemonControlState) {
    if let Some(next) = state.document_waiters.pop_first() {
        crate::daemon::control::pending::mark_owner_ready(
            state,
            next,
            ReadyClass::HostCompletion,
            crate::daemon::control::pending::READY_HOST_COMPLETION,
        );
    }
}

fn finish_reply(permit: HostWorkPermit, reply: crate::host_mutations::HostReply) -> ControlPoll {
    let charge = permit.into_prepared_charge(reply.logical_bytes);
    ControlPoll::ReadyHost(Ok(*reply.response), charge)
}

fn finish_error(
    permit: HostWorkPermit,
    operation: &'static str,
    error: HostMutationError,
) -> ControlPoll {
    if error.event.is_none() {
        let charge = permit.into_prepared_charge(0);
        return ControlPoll::ReadyHost(Ok(host_error_response(operation, error)), charge);
    }
    // A failure event is client-visible payload: charge it like a reply.
    // If the event cannot be encoded within the limit, report the failure
    // itself without the event.
    let without_event = HostMutationError {
        event: None,
        ..error.clone()
    };
    match crate::host_mutations::HostReply::try_new(host_error_response(operation, error)) {
        Ok(reply) => finish_reply(permit, reply),
        Err(_) => {
            let charge = permit.into_prepared_charge(0);
            ControlPoll::ReadyHost(Ok(host_error_response(operation, without_event)), charge)
        }
    }
}

/// The typed refusal for a package runtime effect that failed to load its
/// plugin, or to find the socket binding its entrypoint restart needs.
fn package_load_refusal(error: &DaemonTransportError) -> Option<HostMutationError> {
    match error {
        DaemonTransportError::Daemon(crate::HubDaemonError::LuaPlugin(error)) => {
            Some(HostMutationError {
                code: error.code().to_string(),
                message: error.to_string(),
                event: None,
            })
        }
        // The message names the package, so a stranded event plane names
        // exactly which package runs without its subscriptions.
        DaemonTransportError::PluginNotSwapped { error: load, .. } => Some(HostMutationError {
            code: load.code().to_string(),
            message: error.to_string(),
            event: None,
        }),
        DaemonTransportError::MissingSocketBinding => Some(HostMutationError {
            code: "socket_binding_missing".to_string(),
            message: "the entrypoint restart has no socket binding".to_string(),
            event: None,
        }),
        _ => None,
    }
}

fn finish_transport_error(permit: HostWorkPermit, error: DaemonTransportError) -> ControlPoll {
    let charge = permit.into_prepared_charge(0);
    ControlPoll::ReadyHost(Err(error), charge)
}

fn host_error_response(operation: &str, error: HostMutationError) -> DaemonResponse {
    let mut response = error_response(&error.code, operation, &error.message);
    response.events.extend(error.event.map(|event| *event));
    response
}

fn submit_error_response(error: HostSubmitError) -> DaemonResponse {
    match error {
        HostSubmitError::WrongExecutor => error_response(
            "host_permit_mismatch",
            "host_execution",
            "the host phase used a permit from another executor",
        ),
        HostSubmitError::Full => error_response(
            "host_executor_full",
            "host_execution",
            "the host executor queue refused a reserved operation",
        ),
        HostSubmitError::PhaseExhausted => error_response(
            "host_phase_exhausted",
            "host_execution",
            "host phase identity is exhausted; the operation requires recovery",
        ),
        HostSubmitError::Stopped => error_response(
            "host_executor_stopped",
            "host_execution",
            "the host executor stopped before it accepted the operation",
        ),
    }
}

/// An operator resolve of a quarantined repository root. It runs on the
/// owner, which alone touches the quarantine map; a recovery write still in
/// flight for the root refuses it, typed.
pub(crate) fn resolve_repo_quarantine(
    state: &mut DaemonControlState,
    root: &std::path::Path,
) -> DaemonResponse {
    use crate::daemon::control::session_type_quarantine::ResolveRefusal;

    let write_in_flight = state.blocked_session_type_roots.contains_key(root);
    match state
        .repo_session_type_quarantine
        .resolve(root, write_in_flight)
    {
        Ok(()) => crate::client_api_dto::response::daemon_response_base(
            botster_hub_client::DaemonResponseKind::QuarantineResolved,
        ),
        Err(ResolveRefusal::NotFound) => error_response(
            "quarantine_not_found",
            "resolve_quarantine",
            &format!("{} is not quarantined", root.display()),
        ),
        Err(ResolveRefusal::WriteInFlight) => error_response(
            "quarantine_write_in_flight",
            "resolve_quarantine",
            &format!(
                "a session-type write for {} is still in flight; resolve after it completes",
                root.display()
            ),
        ),
    }
}

fn error_response(code: &str, operation: &str, message: &str) -> DaemonResponse {
    let diagnostic = DaemonDiagnostic::action_failure(operation, message);
    let mut response = crate::client_api_dto::response::daemon_response_base(
        botster_hub_client::DaemonResponseKind::OperatorError,
    );
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: format!("daemon-{operation}"),
        operation: operation.to_string(),
        message: message.to_string(),
        diagnostics: vec![diagnostic.clone()],
    });
    response.diagnostics = vec![diagnostic];
    response
}

/// Reject package operations that cannot use a possibly inconsistent registry.
pub(crate) fn recovery_response(
    state: &DaemonControlState,
    request: &DaemonRequest,
) -> Option<DaemonResponse> {
    if handles(request)
        && state
            .host_recovery
            .values()
            .any(|recovery| matches!(recovery, HostRecoveryRequired::PackageFamilyWork { .. }))
    {
        return Some(error_response(
            "entity_family_cleanup_failed",
            "packages",
            "entity family cleanup requires recovery",
        ));
    }
    if handles(request)
        && state
            .host_recovery
            .values()
            .any(|recovery| matches!(recovery, HostRecoveryRequired::PackageFamilies { .. }))
    {
        return Some(error_response(
            "entity_family_generation_exhausted",
            "packages",
            "entity family cleanup exhausted generation identifiers",
        ));
    }
    if handles(request)
        && state
            .host_recovery
            .values()
            .any(|recovery| matches!(recovery, HostRecoveryRequired::PackageEvents { .. }))
    {
        return Some(error_response(
            "event_plane_cleanup_failed",
            "packages",
            "event router cleanup requires recovery",
        ));
    }
    if handles(request)
        && let Some(failure) = state
            .host_recovery
            .values()
            .find_map(|recovery| match recovery {
                HostRecoveryRequired::Submission { failure, .. } => Some(failure),
                _ => None,
            })
    {
        return Some(error_response(
            "host_recovery_required",
            "host_execution",
            &format!(
                "host phase {:?} requires recovery after {:?}",
                failure.identity, failure.error,
            ),
        ));
    }
    let recovery = state
        .host_recovery
        .values()
        .find_map(|recovery| match recovery {
            HostRecoveryRequired::Package(recovery) => Some(recovery),
            _ => None,
        })?;
    // An explicit enable, reload, or resolve of a stranded package resolves it.
    let resolves_stranded = explicit_resolution_target(request)
        .is_some_and(|package_name| covered_by_package_recovery(state, package_name));
    let blocked = !resolves_stranded
        && (is_package_read(request)
            || is_package_prepare(request)
            || matches!(
                request,
                DaemonRequest::StartPackageEntrypoint { .. }
                    | DaemonRequest::RestartPackageEntrypoint { .. }
            ));
    blocked.then(|| {
        error_response(
            "package_recovery_required",
            "host_execution",
            &format!(
                "package recovery is required; original: {}; compensation: {}",
                recovery.original, recovery.compensation
            ),
        )
    })
}

fn with_compensation_failure(
    mut failure: HostMutationError,
    compensation: Option<HostMutationError>,
) -> HostMutationError {
    if let Some(compensation) = compensation {
        failure.message = format!(
            "{}; prior compensation failure {}: {}",
            failure.message, compensation.code, compensation.message
        );
    }
    failure
}

fn blocked_session_type_waiter(
    state: &DaemonControlState,
    hub_state: &crate::shared_view::SharedView<crate::persistence::HubState>,
    request: &DaemonRequest,
) -> Option<WaiterId> {
    let root = repo_session_type_root(hub_state, request)?;
    state.blocked_session_type_roots.get(root).copied()
}

/// The stored root of a repository session-type mutation's target. Target
/// creation and update canonicalize it, so this reads no filesystem state.
fn repo_session_type_root<'a>(
    hub_state: &'a crate::persistence::HubState,
    request: &DaemonRequest,
) -> Option<&'a std::path::PathBuf> {
    let source = match request {
        DaemonRequest::CreateSessionType { source, .. }
        | DaemonRequest::UpdateSessionType { source, .. }
        | DaemonRequest::DeleteSessionType { source, .. } => source,
        _ => return None,
    };
    let botster_hub_client::DaemonSessionTypeMutationSource::Repo { target_id } = source else {
        return None;
    };
    hub_state
        .spawn_targets
        .iter()
        .find(|target| target.target_id == *target_id)
        .map(|target| &target.root)
}

fn quarantined_error() -> HostMutationError {
    HostMutationError {
        code: REPO_SESSION_TYPE_QUARANTINED.to_string(),
        message: "session types for this repository are quarantined after an uncertain write; resolve it or restart the Hub".to_string(),
        event: None,
    }
}

/// Preserve a submitted commit whose Host job returned no mutation result,
/// then release document admission so later writers never deadlock.
fn finish_unknown_commit(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    permit: HostWorkPermit,
    obligation: CommitObligation,
    kind: UnknownOutcomeKind,
    detail: &str,
) -> ControlPoll {
    let (code, message) = match obligation {
        CommitObligation::Repo(pending) => {
            state.release_uncertain_reservation(waiter_id);
            state.repo_session_type_quarantine.install(
                pending,
                QuarantineCause::UnknownOutcome(kind),
                detail,
            );
            (
                "repo_session_type_publication_uncertain",
                "the repository session-type write ended without a result; its outcome is unknown",
            )
        }
        CommitObligation::Document {
            base_revision,
            prior,
            candidate,
        } => {
            crate::hub_log::hub_log!(
                "state_publication_outcome_unknown base_revision={base_revision} kind={kind:?} detail={detail}"
            );
            state.retain_uncertain_unknown(
                waiter_id,
                UnknownDocumentOutcome {
                    _kind: kind,
                    _base_revision: base_revision,
                    _prior: prior,
                    _candidate: candidate,
                },
            );
            (
                "state_publication_uncertain",
                "the state write ended without a result; its publication outcome is unknown",
            )
        }
    };
    release_document(state, waiter_id);
    drop(permit);
    ControlPoll::Ready(Ok(error_response(code, "hub_state", message)))
}

fn is_entrypoint_request(request: &DaemonRequest) -> bool {
    matches!(
        request,
        DaemonRequest::StartPackageEntrypoint { .. }
            | DaemonRequest::StopPackageEntrypoint { .. }
            | DaemonRequest::RestartPackageEntrypoint { .. }
            | DaemonRequest::PackageEntrypointStatus { .. }
    )
}

fn is_package_read(request: &DaemonRequest) -> bool {
    matches!(
        request,
        DaemonRequest::ListApps
            | DaemonRequest::ResolveAppLaunch { .. }
            | DaemonRequest::ResolvePackageRoute { .. }
            | DaemonRequest::ListPackageNavigation
            | DaemonRequest::ListPackages
            | DaemonRequest::ListAvailablePackages { .. }
            | DaemonRequest::InspectAvailablePackage { .. }
            | DaemonRequest::PreviewPackageInstall { .. }
            | DaemonRequest::CheckPackageUpdate { .. }
            | DaemonRequest::PreviewPackageUpdate { .. }
            | DaemonRequest::ShowPackage { .. }
    )
}

fn is_package_prepare(request: &DaemonRequest) -> bool {
    matches!(
        request,
        DaemonRequest::InstallPackageRegistryEntry { .. }
            | DaemonRequest::InstallPackageLocalPath { .. }
            | DaemonRequest::ApplyPackageUpdate { .. }
            | DaemonRequest::SetPackageConfiguration { .. }
            | DaemonRequest::ReloadPackage { .. }
            | DaemonRequest::RefreshLocalPackages
            | DaemonRequest::EnablePackageLocalPath { .. }
            | DaemonRequest::EnablePackage { .. }
            | DaemonRequest::DisablePackage { .. }
            | DaemonRequest::RemovePackage { .. }
            | DaemonRequest::ResolveQuarantine {
                target: DaemonQuarantineTarget::Package { .. },
            }
    )
}

fn is_spawn_target_read(request: &DaemonRequest) -> bool {
    matches!(
        request,
        DaemonRequest::ListSpawnTargets
            | DaemonRequest::ShowSpawnTarget { .. }
            | DaemonRequest::ValidateSpawnTarget { .. }
            | DaemonRequest::ListWorktrees
            | DaemonRequest::ShowWorktree { .. }
    )
}

fn is_spawn_target_prepare(request: &DaemonRequest) -> bool {
    matches!(
        request,
        DaemonRequest::CreateSpawnTarget { .. }
            | DaemonRequest::UpdateSpawnTarget { .. }
            | DaemonRequest::DeleteSpawnTarget { .. }
            | DaemonRequest::CreateWorktree { .. }
            | DaemonRequest::DeleteWorktree { .. }
    )
}

fn is_session_type_read(request: &DaemonRequest) -> bool {
    matches!(
        request,
        DaemonRequest::ListSessionTypes
            | DaemonRequest::ListSessionTypesForTarget { .. }
            | DaemonRequest::ShowSessionType { .. }
            | DaemonRequest::ShowSessionTypeDefinition { .. }
            | DaemonRequest::ResolveSessionType { .. }
    )
}

fn is_session_type_prepare(request: &DaemonRequest) -> bool {
    matches!(
        request,
        DaemonRequest::CreateSessionType { .. }
            | DaemonRequest::UpdateSessionType { .. }
            | DaemonRequest::DeleteSessionType { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::owner_loop::UncertainPublicationKind;
    use crate::host_executor::HostCompletion;
    use crate::host_mutations::{ExternalEffectCause, RollbackDescriptor};
    use crate::persistence::{FileCommitOutcome, FileHubStateStore};
    use crate::runtime::package_effect::HostPackageCleanup;
    use crate::session_types::SessionTypeError;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// The owner refuses a repository resolve while a recovery write for the
    /// root is in flight, then accepts it once the write settles.
    #[test]
    fn a_repository_resolve_waits_for_the_root_write_in_flight() {
        use crate::daemon::control::session_type_quarantine::{
            PendingRepoQuarantine, QuarantineCause, UnknownOutcomeKind,
        };

        let root = std::path::PathBuf::from("/repo/in-flight");
        let evidence = crate::host_mutations::RepoWriteEvidence {
            root: root.clone(),
            target_path: root.join(".botster/session-types.json"),
            session_type_id: "review".to_string(),
            operation: crate::host_mutations::SessionTypeOperation::Create,
            prior_sha256: None,
            candidate_sha256: [3; 32],
        };
        let budget = crate::shared_view::SharedViewBudget::with_capacity(4096);
        let mut state = DaemonControlState::default();
        state.repo_session_type_quarantine.install(
            PendingRepoQuarantine::reserve(&evidence, &budget).expect("reserve"),
            QuarantineCause::UnknownOutcome(UnknownOutcomeKind::Other),
            "test",
        );
        state
            .blocked_session_type_roots
            .insert(root.clone(), WaiterId(7));
        let refused = resolve_repo_quarantine(&mut state, &root);
        assert_eq!(
            refused.error.as_ref().map(|error| error.code.as_str()),
            Some("quarantine_write_in_flight"),
            "{refused:?}"
        );
        assert!(state.repo_session_type_quarantine.contains(&root));

        state.blocked_session_type_roots.remove(&root);
        let resolved = resolve_repo_quarantine(&mut state, &root);
        assert_eq!(
            resolved.kind,
            botster_hub_client::DaemonResponseKind::QuarantineResolved,
            "{resolved:?}"
        );
        assert!(!state.repo_session_type_quarantine.contains(&root));
        assert_eq!(budget.used(), 0);
        let absent = resolve_repo_quarantine(&mut state, &root);
        assert_eq!(
            absent.error.as_ref().map(|error| error.code.as_str()),
            Some("quarantine_not_found"),
            "{absent:?}"
        );
    }

    // Parallel tests can read the same clock value; the counter keeps each
    // test daemon's directory, and so its directory lock, distinct.
    static NEXT_TEST_DAEMON: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn recovery_test_daemon() -> (HubDaemon, std::path::PathBuf) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        let directory = std::path::PathBuf::from("target")
            .join("botster-hub-test-data")
            .join(format!(
                "package-restore-ownership-{unique}-{}",
                NEXT_TEST_DAEMON.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "package-restore-ownership".to_string(),
                display_name: "Package Restore Ownership".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build package restore test config");
        (
            HubDaemon::start(config).expect("start test daemon"),
            directory,
        )
    }

    #[test]
    fn host_operation_label_classifies_package_requests_and_preserves_generic_requests() {
        assert_eq!(
            host_operation_label(&DaemonRequest::ShowPackage {
                package_name: "test.plugin".to_string(),
            }),
            "show"
        );
        assert_eq!(
            host_operation_label(&DaemonRequest::SetPackageConfiguration {
                package_name: "test.plugin".to_string(),
                values: Default::default(),
            }),
            "configure"
        );
        assert_eq!(
            host_operation_label(&DaemonRequest::EnablePackage {
                package_name: "test.plugin".to_string(),
            }),
            "enable"
        );
        assert_eq!(
            host_operation_label(&DaemonRequest::RefreshLocalPackages),
            "host_execution"
        );
        assert_eq!(
            host_operation_label(&DaemonRequest::ListSpawnTargets),
            "host_execution"
        );
    }

    #[test]
    fn later_package_failure_keeps_its_action_and_non_package_failure_stays_generic() {
        let (mut daemon, directory) = recovery_test_daemon();
        for (index, operation, result) in [
            (
                0_u64,
                "enable",
                HostMutationResult::PackageEffectApplied {
                    reply: Err(HostMutationError {
                        code: "package_effect_failed".to_string(),
                        message: "test failure".to_string(),
                        event: None,
                    }),
                    cleanup: HostPackageCleanup::default(),
                    loaded: None,
                },
            ),
            (
                1_u64,
                "host_execution",
                HostMutationResult::Failed(HostMutationError {
                    code: "host_read_failed".to_string(),
                    message: "test failure".to_string(),
                    event: None,
                }),
            ),
        ] {
            let waiter_id = WaiterId(100 + index);
            let mut state = DaemonControlState::default();
            state.document_owner = Some(waiter_id);
            let permit = daemon
                .runtime()
                .expect("runtime")
                .host_executor()
                .try_reserve()
                .expect("reserve Host slot");
            state.host_completions.insert(
                waiter_id,
                HostCompletion::from_parts(
                    HostJobIdentity::first(waiter_id),
                    HostResult::Mutation(result),
                    permit,
                ),
            );
            let mut continuation = HostMutationContinuation {
                waiter_id,
                must_finish: index == 0,
                operation,
                retained_prepare: None,
                prior_compensation_failure: None,
                failed_package_effect: None,
                next_phase: 2,
                family_work: None,
                event_cleanup: None,
                observes_session_type_catalog: false,
                commit_obligation: None,
            };
            let ControlPoll::ReadyHost(Ok(response), charge) =
                continuation.poll(&mut daemon, &mut state)
            else {
                panic!("Host failure must return an operator error");
            };
            let error = response.error.expect("operator error");
            assert_eq!(error.operation, operation);
            assert_eq!(error.request_id, format!("daemon-{operation}"));
            assert_eq!(error.diagnostics[0].operation.as_deref(), Some(operation));
            assert_eq!(
                response.diagnostics[0].operation.as_deref(),
                Some(operation)
            );
            drop(charge);
            assert_eq!(state.document_owner, None);
        }
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove Host label test directory");
    }

    fn assert_retained_admission_label(
        daemon: &mut HubDaemon,
        mut prepared: PreparedMutation,
        operation: &'static str,
        waiter_id: WaiterId,
    ) {
        prepared.base_revision = daemon.state_view().0 + 1;
        let mut state = DaemonControlState::default();
        state.document_owner = Some(WaiterId(waiter_id.0 + 1));
        let permit = daemon
            .runtime()
            .expect("runtime")
            .host_executor()
            .try_reserve()
            .expect("reserve Host slot");
        state.host_completions.insert(
            waiter_id,
            HostCompletion::from_parts(
                HostJobIdentity::first(waiter_id),
                HostResult::Mutation(HostMutationResult::Prepared(prepared)),
                permit,
            ),
        );
        let mut continuation = HostMutationContinuation {
            waiter_id,
            must_finish: true,
            operation,
            retained_prepare: None,
            prior_compensation_failure: None,
            failed_package_effect: None,
            next_phase: 2,
            family_work: None,
            event_cleanup: None,
            observes_session_type_catalog: false,
            commit_obligation: None,
        };
        assert!(matches!(
            continuation.poll(daemon, &mut state),
            ControlPoll::Pending
        ));
        assert!(continuation.retained_prepare.is_some());
        assert!(state.document_waiters.contains(&waiter_id));
        state.document_owner = None;
        let ControlPoll::ReadyHost(Ok(response), charge) = continuation.poll(daemon, &mut state)
        else {
            panic!("stale retained preparation must return an operator error");
        };
        let error = response.error.expect("operator error");
        assert_eq!(error.code, "host_prepared_revision_stale");
        assert_eq!(error.operation, operation);
        assert_eq!(error.request_id, format!("daemon-{operation}"));
        assert_eq!(error.diagnostics[0].operation.as_deref(), Some(operation));
        assert_eq!(
            response.diagnostics[0].operation.as_deref(),
            Some(operation)
        );
        drop(charge);
    }

    #[test]
    fn retained_prepared_admission_keeps_its_operation_after_document_contention() {
        let (mut daemon, directory) = recovery_test_daemon();
        let (revision, view) = daemon.state_view();
        let request = DaemonRequest::CreateSpawnTarget {
            target_id: Some("retained-label-target".to_string()),
            label: None,
            root: std::env::current_dir().expect("current directory"),
            enabled: false,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: Default::default(),
        };
        let HostMutationResult::Prepared(prepared) = crate::host_mutations::execute(
            HostMutationCommand::Prepare(Box::new(HostPrepare::SpawnTarget {
                request,
                base_revision: revision,
                authority: daemon
                    .runtime()
                    .expect("runtime")
                    .state_authority()
                    .expect("File authority"),
                state: view,
                packages: daemon.package_registry_view(),
                data_directory: directory.clone(),
            })),
            None,
        ) else {
            panic!("spawn target preparation must succeed");
        };
        assert_retained_admission_label(&mut daemon, prepared, "host_execution", WaiterId(102));
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove retained label test directory");
    }

    #[test]
    fn retained_package_admission_keeps_configure_after_document_contention() {
        use crate::packages::{HubPackageEvents, HubPackageManifest, PackageProvenance};
        use botster_core::{
            ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, PackageConfigurationSchema,
            PackageSource,
        };

        let (mut daemon, directory) = recovery_test_daemon();
        let mut packages = crate::PackageRegistry::new(botster_core::CapabilitySet::new());
        packages
            .install(
                HubPackageManifest {
                    name: "retained.plugin".to_string(),
                    version: "1.0.0".to_string(),
                    kind: ExtensionKind::Plugin,
                    botster: ">=0.1.0".to_string(),
                    source: Some(PackageSource::Git {
                        repo: "https://example.invalid/retained.git".to_string(),
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
                        fields: Vec::new(),
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
                "retained admission test",
            )
            .expect("install package fixture");
        let (revision, prior) = daemon.state_view();
        let authority = daemon
            .runtime()
            .expect("runtime")
            .state_authority()
            .expect("File authority");
        let budget = authority.budget();
        let mut matched = (*prior).clone();
        matched.package_registry = packages.snapshot();
        let state = crate::shared_view::SharedView::try_new(&budget, matched, 1)
            .expect("matched state view fits");
        let packages = crate::shared_view::SharedView::try_new(&budget, packages, 1)
            .expect("package view fits");
        let mut entrypoints = crate::entrypoint_supervisor::EntrypointSupervisor::default();
        let HostMutationResult::Prepared(prepared) = crate::host_mutations::execute(
            HostMutationCommand::Prepare(Box::new(HostPrepare::Package {
                request: DaemonRequest::SetPackageConfiguration {
                    package_name: "retained.plugin".to_string(),
                    values: Default::default(),
                },
                base_revision: revision,
                authority: authority.clone(),
                state,
                packages,
                data_directory: directory.clone(),
                stranded: Default::default(),
            })),
            Some(&mut entrypoints),
        ) else {
            panic!("package configuration preparation must succeed");
        };
        assert_retained_admission_label(&mut daemon, prepared, "configure", WaiterId(104));
        drop(authority);
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove retained package test directory");
    }

    #[test]
    fn committed_session_type_generation_change_wakes_subscriber_delivery() {
        let (mut daemon, directory) = recovery_test_daemon();
        let mut state = DaemonControlState::default();
        let delivery = crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery;
        assert!(state.maintenance.wakes.take(delivery));
        for (index, changed) in [false, true].into_iter().enumerate() {
            let (revision, prior) = daemon.state_view();
            let mut next = (*prior).clone();
            if changed {
                next.session_type_generation += 1;
            }
            let view = daemon
                .runtime()
                .expect("runtime")
                .prepare_state(next)
                .expect("prepare committed view");
            let waiter_id = WaiterId(90 + index as u64);
            state.document_owner = Some(waiter_id);
            let permit = daemon
                .runtime()
                .expect("runtime")
                .host_executor()
                .try_reserve()
                .expect("reserve Host slot");
            let reply = crate::host_mutations::HostReply::try_new(
                crate::client_api_dto::response::daemon_response_base(
                    botster_hub_client::DaemonResponseKind::SessionTypes,
                ),
            )
            .expect("bounded session type reply");
            state.host_completions.insert(
                waiter_id,
                HostCompletion::from_parts(
                    HostJobIdentity::first(waiter_id),
                    HostResult::Mutation(HostMutationResult::Committed(
                        crate::host_mutations::CommittedView {
                            committed_revision: revision + 1,
                            view,
                            packages: None,
                            package_effect: None,
                            reply,
                        },
                    )),
                    permit,
                ),
            );
            let mut continuation = HostMutationContinuation {
                waiter_id,
                must_finish: false,
                operation: "host_execution",
                retained_prepare: None,
                prior_compensation_failure: None,
                failed_package_effect: None,
                next_phase: 2,
                family_work: None,
                event_cleanup: None,
                observes_session_type_catalog: false,
                commit_obligation: None,
            };
            assert!(!state.maintenance.wakes.take(delivery));
            let ControlPoll::ReadyHost(Ok(response), charge) =
                continuation.poll(&mut daemon, &mut state)
            else {
                panic!("committed session type view must return its reply");
            };
            assert_eq!(
                response.kind,
                botster_hub_client::DaemonResponseKind::SessionTypes
            );
            drop(charge);
            assert_eq!(state.maintenance.wakes.take(delivery), changed);
        }
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove generation wake test directory");
    }

    fn poll_uncertain_result(
        daemon: &mut HubDaemon,
        result: HostMutationResult,
        expected_kind: UncertainPublicationKind,
        expected_error: &str,
    ) {
        let waiter_id = WaiterId(82);
        let next_waiter = WaiterId(83);
        let later_waiter = WaiterId(84);
        let mut state = DaemonControlState::default();
        assert!(state.reserve_uncertain_publication(waiter_id));
        state.document_owner = Some(waiter_id);
        state.document_waiters = [next_waiter, later_waiter].into_iter().collect();
        let permit = daemon
            .runtime()
            .expect("runtime")
            .host_executor()
            .try_reserve()
            .expect("reserve host work");
        state.host_completions.insert(
            waiter_id,
            HostCompletion::from_parts(
                HostJobIdentity::first(waiter_id),
                HostResult::Mutation(result),
                permit,
            ),
        );
        let mut continuation = HostMutationContinuation {
            waiter_id,
            must_finish: false,
            operation: "host_execution",
            retained_prepare: None,
            prior_compensation_failure: None,
            failed_package_effect: None,
            next_phase: 2,
            family_work: None,
            event_cleanup: None,
            observes_session_type_catalog: false,
            commit_obligation: None,
        };
        let poll = continuation.poll(daemon, &mut state);
        assert!(matches!(
            poll,
            ControlPoll::Ready(Ok(response))
                if response.error.as_ref().is_some_and(|error| error.code == expected_error)
        ));
        assert_eq!(
            state.uncertain_publication_for_test(waiter_id),
            Some((expected_kind, true))
        );
        assert_eq!(state.uncertain_publication_for_test(next_waiter), None);
        assert_eq!(state.document_owner, None);
        assert_eq!(state.document_waiters, [later_waiter].into_iter().collect());
    }

    #[test]
    fn state_uncertainty_uses_the_host_completion_cell_and_releases_one_document_waiter() {
        let (mut daemon, directory) = recovery_test_daemon();
        let (revision, prior) = daemon.state_view();
        let authority = daemon
            .runtime()
            .expect("runtime")
            .state_authority()
            .expect("File authority");
        let store = FileHubStateStore::for_data_directory(&directory);
        let prepared = store
            .prepare_shared(
                &authority,
                revision,
                Some(prior.clone()),
                (*prior).clone(),
                &authority.budget(),
            )
            .expect("prepare state write");
        FileHubStateStore::inject_next_directory_sync_failure(&directory);
        let FileCommitOutcome::PublishedUncertain(write) = store
            .commit_shared(prepared, revision)
            .expect("state write reached rename")
        else {
            panic!("state directory sync must remain uncertain");
        };
        poll_uncertain_result(
            &mut daemon,
            HostMutationResult::PublishedUncertain {
                write,
                rollback: Some(RollbackDescriptor::SessionType {
                    previous: prior,
                    repo_file: None,
                }),
            },
            UncertainPublicationKind::State,
            "state_publication_uncertain",
        );
        drop(authority);
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove state uncertainty test directory");
    }

    #[test]
    fn exhausted_phase_retains_package_effect_and_document_ownership() {
        let (mut daemon, directory) = recovery_test_daemon();
        let runtime = daemon.runtime().expect("runtime");
        let permit = runtime.host_executor().try_reserve().expect("host slot");
        let command = HostMutationCommand::ApplyPackageEffect(Box::new(HostPackageEffect {
            effect: PackageRuntimeEffect::Disable {
                package_name: "retained.plugin".to_string(),
            },
            runtime: runtime.host_package_runtime(),
            config: runtime.config().clone(),
            packages: daemon.package_registry_view(),
            reply: crate::host_mutations::HostReply::try_new(
                crate::client_api_dto::response::daemon_response_base(
                    botster_hub_client::DaemonResponseKind::Packages,
                ),
            )
            .expect("bounded reply"),
        }));
        let waiter_id = WaiterId(82);
        let mut state = DaemonControlState::default();
        state.document_owner = Some(waiter_id);
        let mut next_phase = u64::MAX;
        let result = submit_phase(
            &daemon,
            &mut state,
            waiter_id,
            command,
            permit,
            &mut next_phase,
        );
        assert!(
            matches!(result, ControlPoll::Ready(Ok(response)) if response.error.as_ref().is_some_and(|error| error.code == "host_phase_exhausted"))
        );
        assert_eq!(next_phase, u64::MAX);
        assert_eq!(state.document_owner, Some(waiter_id));
        let Some(HostRecoveryRequired::Submission { failure, .. }) =
            state.host_recovery.get(&waiter_id)
        else {
            panic!("the exhausted phase must retain its effect");
        };
        assert!(
            matches!(&*failure.command, HostCommand::Mutation(HostMutationCommand::ApplyPackageEffect(effect)) if matches!(&effect.effect, PackageRuntimeEffect::Disable { package_name } if package_name == "retained.plugin"))
        );
        let executor = daemon.runtime().expect("runtime").host_executor();
        let remaining = (0..7)
            .map(|_| executor.try_reserve().expect("seven slots remain"))
            .collect::<Vec<_>>();
        assert!(executor.try_reserve().is_none());
        assert!(matches!(
            executor.poll_completion(),
            crate::host_executor::HostCompletionPoll::Empty
        ));
        assert_eq!(
            recovery_response(&state, &DaemonRequest::ListPackages)
                .expect("recovery refusal")
                .error
                .expect("error")
                .code,
            "host_recovery_required"
        );
        drop(remaining);
        drop(state);
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove phase test directory");
    }

    #[test]
    fn package_restore_without_document_ownership_strands_and_submits_the_quarantine() {
        let (mut daemon, directory) = recovery_test_daemon();
        let previous_state = daemon.state_view().1;
        let previous_packages = daemon.package_registry_view();
        let effect = PackageRuntimeEffect::Enable {
            package_name: "broken.plugin".to_string(),
            previous_state: previous_state.clone(),
            previous_packages: previous_packages.clone(),
        };
        let restore = HostPackageRestore {
            base_revision: daemon.state_view().0,
            authority: daemon
                .runtime()
                .expect("runtime")
                .state_authority()
                .expect("File runtime authority"),
            current_state: previous_state.clone(),
            previous_state,
            previous_packages,
            data_directory: directory.clone(),
        };
        let permit = daemon
            .runtime()
            .expect("runtime")
            .host_executor()
            .try_reserve()
            .expect("reserve host work");
        let mut state = DaemonControlState::default();
        let mut next_phase = 7;
        let mut failed_package_effect = None;

        let poll = submit_package_restore(
            &daemon,
            &mut state,
            WaiterId(41),
            PackageRestoreSubmission {
                restore,
                effect,
                original: DaemonTransportError::DaemonNotRunning,
                permit,
            },
            &mut next_phase,
            &mut failed_package_effect,
        );

        // A lost reservation is a failed compensation: the package is
        // stranded at once and the Host quarantine phase is submitted in
        // place of the durable restore.
        assert!(matches!(poll, ControlPoll::Pending));
        assert!(
            daemon
                .runtime()
                .expect("runtime")
                .stranded_packages()
                .contains("broken.plugin")
        );
        assert!(failed_package_effect.is_none());
        assert_eq!(next_phase, 8);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let completion = loop {
            match daemon
                .runtime()
                .expect("runtime")
                .host_executor()
                .poll_completion()
            {
                crate::host_executor::HostCompletionPoll::Ready(completion) => break completion,
                crate::host_executor::HostCompletionPoll::Empty => {
                    // timer: deadline — bounds a Host job that never completes.
                    assert!(
                        std::time::Instant::now() < deadline,
                        "quarantine phase completes"
                    );
                    std::thread::yield_now();
                }
                crate::host_executor::HostCompletionPoll::Stopped => {
                    panic!("host executor stopped")
                }
            }
        };
        assert!(matches!(
            *completion.result,
            HostResult::Mutation(HostMutationResult::PackageRuntimeRestored { ref rollbacks, .. })
                if rollbacks.len() == 1 && rollbacks[0].step == "restore_admission"
        ));

        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove package restore test directory");
    }

    #[test]
    fn document_release_and_stale_handoff_wake_one_waiter_each() {
        let owner = WaiterId(1);
        let mut state = DaemonControlState::default();
        state.document_owner = Some(owner);
        state.document_waiters = [WaiterId(2), WaiterId(3)].into_iter().collect();

        release_document(&mut state, owner);

        assert_eq!(state.document_owner, None);
        assert_eq!(state.document_waiters, [WaiterId(3)].into_iter().collect());

        wake_next_document_waiter(&mut state);

        assert!(state.document_waiters.is_empty());
    }

    // --- Scoped repository session-type quarantine and commit obligations ---

    use crate::daemon::control::session_type_quarantine::QuarantineStage;
    use crate::host_executor::HostCompletionPoll;
    use crate::host_mutations::CommitPanicPoint;

    const QUARANTINE_TARGET: &str = "quarantine-target";

    /// A daemon with one enabled directory target whose canonical root exists.
    fn quarantine_daemon() -> (HubDaemon, std::path::PathBuf, std::path::PathBuf) {
        let (mut daemon, directory) = recovery_test_daemon();
        let root = directory.join("repo");
        std::fs::create_dir_all(&root).expect("create repository root");
        let root = root.canonicalize().expect("canonical repository root");
        let view = {
            let runtime = daemon.runtime().expect("runtime");
            let mut next = (*runtime.state()).clone();
            next.spawn_targets.push(crate::spawn_targets::SpawnTarget {
                target_id: QUARANTINE_TARGET.into(),
                label: QUARANTINE_TARGET.into(),
                root: root.clone(),
                enabled: true,
                kind: "directory".into(),
                base_ref: None,
                metadata: Default::default(),
            });
            runtime.prepare_state(next).expect("target state fits")
        };
        daemon.publish_state(view);
        (daemon, directory, root)
    }

    fn session_type_request(
        id: &str,
        source: botster_hub_client::DaemonSessionTypeMutationSource,
    ) -> DaemonRequest {
        DaemonRequest::CreateSessionType {
            source,
            definition: crate::client_api_dto::session::daemon_session_type_definition_from_client(
                crate::PackageSessionType {
                    id: id.to_string(),
                    label: id.to_string(),
                    description: None,
                    icon: None,
                    role: "botster.agent".to_string(),
                    interaction: "interactive".to_string(),
                    traits: Vec::new(),
                    lifecycle: "durable".to_string(),
                    execution: crate::PackageSessionTypeExecution::RelativeExecutable,
                    command: "bin/agent".to_string(),
                    args: Vec::new(),
                    working_directory: crate::PackageSessionTypeWorkingDirectory::PackageRoot,
                    environment: std::collections::BTreeMap::new(),
                    allowed_environment_overrides: Vec::new(),
                    context: Vec::new(),
                    target_id: None,
                },
            ),
        }
    }

    fn repo_request(id: &str) -> DaemonRequest {
        session_type_request(
            id,
            botster_hub_client::DaemonSessionTypeMutationSource::Repo {
                target_id: QUARANTINE_TARGET.into(),
            },
        )
    }

    fn device_request(id: &str) -> DaemonRequest {
        session_type_request(
            id,
            botster_hub_client::DaemonSessionTypeMutationSource::Device,
        )
    }

    enum Begun {
        Pending(WaiterId, Box<HostMutationContinuation>),
        Ready(DaemonResponse),
    }

    fn begin(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        request: DaemonRequest,
    ) -> Begun {
        let waiter = daemon
            .runtime()
            .expect("runtime")
            .next_waiter_id()
            .expect("waiter id");
        state.current_waiter_id = Some(waiter);
        match handle(daemon, state, request).expect("host work handles session types") {
            ControlStep::Pending(step) => {
                let super::super::pending::ControlContinuation::HostMutation(continuation) =
                    step.continuation
                else {
                    panic!("session-type work is a host mutation continuation");
                };
                Begun::Pending(waiter, continuation)
            }
            ControlStep::Ready(response) => Begun::Ready(response.expect("typed response")),
        }
    }

    fn begin_pending(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        request: DaemonRequest,
    ) -> (WaiterId, Box<HostMutationContinuation>) {
        match begin(daemon, state, request) {
            Begun::Pending(waiter, continuation) => (waiter, continuation),
            Begun::Ready(response) => panic!("expected admitted work, got {response:?}"),
        }
    }

    /// Route Host completions until `waiter` has one. The executor publishes
    /// each completion exactly once; completions for other waiters are kept.
    fn await_completion(daemon: &HubDaemon, state: &mut DaemonControlState, waiter: WaiterId) {
        while !state.host_completions.contains_key(&waiter) {
            match daemon
                .runtime()
                .expect("runtime")
                .host_executor()
                .poll_completion()
            {
                HostCompletionPoll::Ready(completion) => {
                    // Catalog builds go to the catalog; this test's waiters,
                    // which no pending request registry owns, stay retained.
                    crate::subscription::entity::route_terminal_host_completion(state, completion)
                        .expect("each completion is routed once");
                }
                HostCompletionPoll::Empty => std::thread::yield_now(),
                HostCompletionPoll::Stopped => panic!("host executor stopped"),
            }
        }
    }

    /// Drive entity delivery, routing Host completions, until `found` accepts a
    /// frame the subscriber received. The loop ends on that frame; the bound
    /// only turns a lost delivery into a failure instead of a hang.
    fn drive_until_frame(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        receiver: &std::sync::mpsc::Receiver<botster_hub_client::DaemonEntityFrame>,
        what: &str,
        found: impl Fn(&botster_hub_client::DaemonEntityFrame) -> bool,
    ) -> botster_hub_client::DaemonEntityFrame {
        let mut seen = Vec::new();
        // timer: deadline — an iteration bound, not a wait; each turn either
        // delivers, routes a completion, or yields once.
        for _ in 0..2_000_000 {
            // One owner turn routes Host completions, absorbs the catalog
            // build, and runs subscriber delivery, as production does.
            crate::daemon::owner_loop::drive_ready_test_turn(daemon, state);
            while let Ok(frame) = receiver.try_recv() {
                if found(&frame) {
                    return frame;
                }
                seen.push(frame);
            }
            std::thread::yield_now();
        }
        panic!("the subscriber never received {what}; frames seen: {seen:?}");
    }

    fn snapshot_ids(frame: &botster_hub_client::DaemonEntityFrame) -> Option<Vec<String>> {
        let botster_hub_client::DaemonEntityFrame::Snapshot { items, .. } = frame else {
            return None;
        };
        Some(
            items
                .iter()
                .filter_map(|item| item.get("id").and_then(|id| id.as_str()))
                .map(str::to_string)
                .collect(),
        )
    }

    #[test]
    fn live_session_type_subscriber_gets_the_file_after_an_uncertain_repo_write() {
        let (mut daemon, directory, root) = quarantine_daemon();
        let mut state = DaemonControlState::default();
        let (sender, receiver) = std::sync::mpsc::sync_channel(64);
        let registered = crate::subscription::entity::register_builtin_entity_subscription(
            &mut daemon,
            &mut state,
            "session_type".to_string(),
            "types".to_string(),
            crate::subscription::entity::EntityFrameSender::Blocking(sender),
            None,
        )
        .expect("register the session-type subscription");
        assert!(registered.error.is_none(), "{:?}", registered.error);
        let baseline = drive_until_frame(
            &mut daemon,
            &mut state,
            &receiver,
            "a baseline snapshot",
            |frame| snapshot_ids(frame).is_some(),
        );
        assert!(
            !snapshot_ids(&baseline)
                .expect("snapshot")
                .contains(&"alpha".to_string())
        );

        let (waiter, mut work) = begin_pending(&mut daemon, &mut state, repo_request("alpha"));
        await_completion(&daemon, &mut state, waiter);
        crate::session_types::inject_next_repo_directory_sync_failure(&root);
        let reply = settle(&mut daemon, &mut state, waiter, &mut work);
        assert_eq!(
            error_code(&reply),
            Some("repo_session_type_publication_uncertain")
        );
        // The live subscriber receives a fresh catalog built from the current
        // file, which holds the renamed but unconfirmed definition.
        drive_until_frame(
            &mut daemon,
            &mut state,
            &receiver,
            "alpha after the write",
            |frame| {
                snapshot_ids(frame).is_some_and(|ids| ids.contains(&"alpha".to_string()))
                    || matches!(
                        frame,
                        botster_hub_client::DaemonEntityFrame::Upsert { entity, .. }
                            if entity.get("id").and_then(|id| id.as_str()) == Some("alpha")
                    )
            },
        );
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove quarantine test directory");
    }

    /// Poll `continuation` to its reply, routing its own completions.
    fn settle(
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        waiter: WaiterId,
        continuation: &mut HostMutationContinuation,
    ) -> DaemonResponse {
        loop {
            match continuation.poll(daemon, state) {
                ControlPoll::Pending => await_completion(daemon, state, waiter),
                ControlPoll::Again => {}
                other => return reply_of(other),
            }
        }
    }

    fn reply_of(poll: ControlPoll) -> DaemonResponse {
        match poll {
            ControlPoll::Ready(response) => response.expect("typed response"),
            ControlPoll::ReadyHost(response, charge) => {
                drop(charge);
                response.expect("typed response")
            }
            _ => panic!("expected a reply"),
        }
    }

    fn error_code(response: &DaemonResponse) -> Option<&str> {
        response.error.as_ref().map(|error| error.code.as_str())
    }

    #[test]
    fn uncertain_repo_write_quarantines_its_root_and_refuses_a_write_prepared_before_it() {
        let (mut daemon, directory, root) = quarantine_daemon();
        let mut state = DaemonControlState::default();
        let (first, mut first_work) = begin_pending(&mut daemon, &mut state, repo_request("alpha"));
        let (second, mut second_work) =
            begin_pending(&mut daemon, &mut state, repo_request("beta"));
        // Both writes are prepared before either commits.
        await_completion(&daemon, &mut state, first);
        await_completion(&daemon, &mut state, second);
        crate::session_types::inject_next_repo_directory_sync_failure(&root);
        assert!(matches!(
            first_work.poll(&mut daemon, &mut state),
            ControlPoll::Pending
        ));
        assert!(
            matches!(
                second_work.poll(&mut daemon, &mut state),
                ControlPoll::Pending
            ),
            "the second write waits for document admission"
        );
        await_completion(&daemon, &mut state, first);
        let first_reply = reply_of(first_work.poll(&mut daemon, &mut state));
        assert_eq!(
            error_code(&first_reply),
            Some("repo_session_type_publication_uncertain")
        );
        assert_eq!(
            state.repo_session_type_quarantine.cause_for(&root),
            Some(QuarantineCause::PublishedUncertain(
                crate::session_types::RepoPublicationCause::Injected
            ))
        );
        assert_eq!(state.uncertain_publication_for_test(first), None);
        assert_eq!(state.document_owner, None);

        let second_reply = settle(&mut daemon, &mut state, second, &mut second_work);
        assert_eq!(
            error_code(&second_reply),
            Some(REPO_SESSION_TYPE_QUARANTINED)
        );
        assert_eq!(
            state.repo_session_type_quarantine.refusals_for(&root),
            vec![QuarantineStage::CommitAdmission]
        );

        // A new write to the same root is refused at intake.
        let Begun::Ready(third) = begin(&mut daemon, &mut state, repo_request("gamma")) else {
            panic!("a quarantined root refuses at intake");
        };
        assert_eq!(error_code(&third), Some(REPO_SESSION_TYPE_QUARANTINED));
        assert_eq!(
            state.repo_session_type_quarantine.refusals_for(&root),
            vec![QuarantineStage::CommitAdmission, QuarantineStage::Intake]
        );

        // Hub-state writes elsewhere continue: a device session type commits.
        let (device, mut device_work) =
            begin_pending(&mut daemon, &mut state, device_request("device-type"));
        let device_reply = settle(&mut daemon, &mut state, device, &mut device_work);
        assert_eq!(device_reply.error, None);
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove quarantine test directory");
    }

    #[test]
    fn repo_commit_panics_are_unknown_outcomes_that_quarantine_the_root() {
        for point in [
            CommitPanicPoint::BeforeEffect,
            CommitPanicPoint::AfterEffect,
        ] {
            let (mut daemon, directory, root) = quarantine_daemon();
            let mut state = DaemonControlState::default();
            let (waiter, mut work) = begin_pending(&mut daemon, &mut state, repo_request("alpha"));
            await_completion(&daemon, &mut state, waiter);
            crate::host_mutations::inject_next_commit_panic(root.clone(), point);
            let reply = settle(&mut daemon, &mut state, waiter, &mut work);
            assert_eq!(
                error_code(&reply),
                Some("repo_session_type_publication_uncertain"),
                "{point:?}"
            );
            assert_eq!(
                state.repo_session_type_quarantine.cause_for(&root),
                Some(QuarantineCause::UnknownOutcome(
                    UnknownOutcomeKind::WorkerPanicked
                )),
                "{point:?}"
            );
            assert_eq!(state.document_owner, None, "{point:?}");
            assert_eq!(
                state.uncertain_publication_for_test(waiter),
                None,
                "{point:?}"
            );
            daemon.stop();
            std::fs::remove_dir_all(directory).expect("remove quarantine test directory");
        }
    }

    #[test]
    fn document_commit_panic_keeps_the_global_obligation_and_releases_admission() {
        let (mut daemon, directory, _root) = quarantine_daemon();
        let mut state = DaemonControlState::default();
        let (waiter, mut work) =
            begin_pending(&mut daemon, &mut state, device_request("device-type"));
        await_completion(&daemon, &mut state, waiter);
        crate::host_mutations::inject_next_commit_panic(
            directory.join("hub-state.json"),
            CommitPanicPoint::BeforeEffect,
        );
        let reply = settle(&mut daemon, &mut state, waiter, &mut work);
        assert_eq!(error_code(&reply), Some("state_publication_uncertain"));
        assert_eq!(
            state.uncertain_publication_for_test(waiter),
            Some((UncertainPublicationKind::UnknownOutcome, false))
        );
        assert_eq!(state.document_owner, None, "admission is released");

        // The global Hub-state stop holds: the next write is refused, not parked.
        let (next, mut next_work) =
            begin_pending(&mut daemon, &mut state, device_request("other-type"));
        let next_reply = settle(&mut daemon, &mut state, next, &mut next_work);
        assert_eq!(
            error_code(&next_reply),
            Some("state_publication_slot_occupied")
        );
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove quarantine test directory");
    }

    #[test]
    fn repo_failure_before_rename_neither_quarantines_nor_retains_its_charge() {
        let (mut daemon, directory, root) = quarantine_daemon();
        let mut state = DaemonControlState::default();
        let budget = daemon.runtime().expect("runtime").shared_view_budget();
        let (waiter, mut work) = begin_pending(&mut daemon, &mut state, repo_request("alpha"));
        await_completion(&daemon, &mut state, waiter);
        let used_before_commit = budget.used();
        // A file where the .botster directory belongs fails before any rename.
        std::fs::write(root.join(".botster"), b"not a directory").expect("block .botster");
        let reply = settle(&mut daemon, &mut state, waiter, &mut work);
        assert!(reply.error.is_some(), "the write fails before publication");
        assert_ne!(
            error_code(&reply),
            Some("repo_session_type_publication_uncertain")
        );
        assert_eq!(state.repo_session_type_quarantine.cause_for(&root), None);
        assert_eq!(
            budget.used(),
            used_before_commit,
            "the entry reservation is released"
        );
        assert_eq!(state.document_owner, None);
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove quarantine test directory");
    }
}
