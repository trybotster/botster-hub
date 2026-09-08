//! Bounded host execution for control reads and durable mutations.

use botster_hub_client::{DaemonDiagnostic, DaemonOperatorError, DaemonRequest, DaemonResponse};

use crate::HubDaemon;
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::error::{DaemonTransportError, PackageRollbackFailure};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::ReadyClass;
use crate::host_executor::{
    HostCommand, HostJobIdentity, HostResult, HostSubmitError, HostWorkPermit,
};
use crate::host_mutations::{
    HostCommit, HostMutationCommand, HostMutationError, HostMutationResult, HostPackageFinalize,
    HostPackageRestore, HostPrepare, HostRead, HostRecover, PackageRuntimeEffect, PreparedMutation,
    RecoveryOutcome, SessionTypeRecovery,
};
use crate::owner_identity::WaiterId;

pub(crate) enum DocumentAdmission {
    Granted,
    Busy,
    Stale,
}

/// One package rollback that keeps one host slot until the daemon restarts.
/// The retained row leaves seven host slots available during degraded operation.
pub(crate) struct PackageRecoveryRequired {
    pub(crate) original: String,
    pub(crate) compensation: String,
    _effect: PackageRuntimeEffect,
    _permit: HostWorkPermit,
}

/// Route one supported host request. A `None` result leaves the request with its existing family.
pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    request: DaemonRequest,
) -> Option<ControlStep> {
    let waiter_id = state.current_waiter_id?;
    let must_finish = crate::daemon::control::pending::request_must_finish(&request);
    let (base_revision, state_view) = daemon.state_view();
    let packages = daemon.package_registry_view();
    let entrypoint_processes = (is_package_read(&request) || is_package_prepare(&request))
        .then(|| daemon.entrypoint_supervisor().snapshots());
    let runtime = daemon.runtime()?;
    let config = runtime.config().clone();
    let data_directory = config.data_directory.clone();
    let command = if is_package_read(&request) {
        HostMutationCommand::Read(HostRead::Package {
            request,
            config: config.clone(),
            packages,
            entrypoint_processes: entrypoint_processes.expect("package snapshots were captured"),
        })
    } else if is_package_prepare(&request) {
        HostMutationCommand::Prepare(HostPrepare::Package {
            request,
            base_revision,
            state: state_view,
            packages,
            entrypoint_processes: entrypoint_processes.expect("package snapshots were captured"),
            data_directory,
        })
    } else if is_spawn_target_read(&request) {
        HostMutationCommand::Read(HostRead::SpawnTarget {
            request,
            state: state_view,
        })
    } else if is_spawn_target_prepare(&request) {
        HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
            request,
            base_revision,
            state: state_view,
            packages,
            data_directory,
        })
    } else if is_session_type_read(&request) {
        HostMutationCommand::Read(HostRead::SessionType {
            request,
            config,
            state: state_view,
            packages,
        })
    } else if is_session_type_prepare(&request) {
        if let Some(blocked_waiter) = blocked_session_type_waiter(state, &state_view, &request) {
            crate::daemon::control::pending::mark_request_ready(
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
        HostMutationCommand::Prepare(HostPrepare::SessionType {
            request,
            base_revision,
            config,
            state: state_view,
            packages,
            data_directory,
        })
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
    if let Err(error) =
        runtime
            .host_executor()
            .submit(identity, HostCommand::Mutation(command), permit)
    {
        return Some(ControlStep::ready(submit_error_response(error)));
    }

    let mut retained_prepare: Option<(PreparedMutation, HostWorkPermit)> = None;
    let mut prior_compensation_failure: Option<HostMutationError> = None;
    let mut failed_package_effect: Option<(PackageRuntimeEffect, DaemonTransportError)> = None;
    let mut finalizing_package = false;
    let mut next_phase = 2;
    Some(ControlStep::pending_in(
        ReadyClass::HostCompletion,
        move |daemon, state| {
            if let Some((prepared, permit)) = retained_prepare.take() {
                return admit_or_park_commit(
                    daemon,
                    state,
                    waiter_id,
                    prepared,
                    permit,
                    must_finish,
                    &mut retained_prepare,
                    &mut next_phase,
                );
            }
            let Some(completion) = state.host_completions.remove(&waiter_id) else {
                return ControlPoll::Pending;
            };
            let (_identity, result, permit) = completion.into_parts();
            let HostResult::Mutation(result) = result else {
                return finish_error(
                    permit,
                    HostMutationError {
                        code: "host_completion_kind_mismatch".to_string(),
                        message: "the host executor returned a non-mutation result".to_string(),
                    },
                );
            };
            match result {
                HostMutationResult::ReadReady(reply) => {
                    if finalizing_package {
                        finalizing_package = false;
                        release_document(state, waiter_id);
                    }
                    finish_reply(permit, reply)
                }
                HostMutationResult::Prepared(prepared) => admit_or_park_commit(
                    daemon,
                    state,
                    waiter_id,
                    prepared,
                    permit,
                    must_finish,
                    &mut retained_prepare,
                    &mut next_phase,
                ),
                HostMutationResult::Committed(committed) => {
                    let current_revision = daemon.state_view().0;
                    if committed.committed_revision != current_revision.saturating_add(1) {
                        release_document(state, waiter_id);
                        return finish_error(
                            permit,
                            HostMutationError {
                                code: "host_commit_revision_mismatch".to_string(),
                                message: "the committed host revision does not follow the published revision"
                                    .to_string(),
                            },
                        );
                    }
                    daemon.publish_state(committed.view);
                    if let Some(packages) = committed.packages {
                        daemon.publish_package_registry_view(packages);
                    }
                    if let Some(effect) = committed.package_effect {
                        if let Err(error) = crate::daemon::control::packages::mutations::apply_committed_runtime_effect(daemon, &effect) {
                            let Some(runtime) = daemon.runtime() else {
                                release_document(state, waiter_id);
                                return finish_transport_error(
                                    permit,
                                    DaemonTransportError::PackageCompensation {
                                        original: Box::new(error),
                                        rollbacks: vec![PackageRollbackFailure {
                                            step: "persist",
                                            package_name: None,
                                            error: Box::new(DaemonTransportError::DaemonNotRunning),
                                        }],
                                    },
                                );
                            };
                            let restore = effect
                                .restore_command(runtime.config().data_directory.clone())
                                .expect("a fallible package effect retains its restore views");
                            return submit_package_restore(
                                daemon,
                                state,
                                waiter_id,
                                restore,
                                effect,
                                error,
                                permit,
                                &mut next_phase,
                                &mut failed_package_effect,
                            );
                        }
                        finalizing_package = true;
                        let entrypoint_processes = daemon.entrypoint_supervisor().snapshots();
                        return submit_phase(
                            daemon,
                            state,
                            waiter_id,
                            HostMutationCommand::FinalizePackage(HostPackageFinalize {
                                reply: committed.reply,
                                entrypoint_processes,
                            }),
                            permit,
                            &mut next_phase,
                        );
                    }
                    release_document(state, waiter_id);
                    finish_reply(permit, committed.reply)
                }
                HostMutationResult::PackageRestored(restored) => {
                    daemon.publish_state(restored.view);
                    daemon.publish_package_registry_view(restored.packages);
                    let (effect, original) = failed_package_effect
                        .take()
                        .expect("a package restore follows one failed runtime effect");
                    let rollbacks =
                        crate::daemon::control::packages::mutations::restore_runtime_after_failed_effect(
                            daemon,
                            &effect,
                        );
                    release_document(state, waiter_id);
                    let error = if rollbacks.is_empty() {
                        original
                    } else {
                        DaemonTransportError::PackageCompensation {
                            original: Box::new(original),
                            rollbacks,
                        }
                    };
                    finish_transport_error(permit, error)
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
                    release_document(state, waiter_id);
                    retain_package_recovery(state, effect, original, failure, permit)
                }
                HostMutationResult::Recovered(outcome) => match outcome {
                    RecoveryOutcome::PackageConfiguration { failure, .. }
                    | RecoveryOutcome::SpawnTarget { failure, .. }
                    | RecoveryOutcome::RegisteredWorktree { failure, .. } => {
                        release_document(state, waiter_id);
                        finish_error(permit, failure)
                    }
                    RecoveryOutcome::SessionType {
                        failure,
                        recovery: SessionTypeRecovery::NotRequired | SessionTypeRecovery::Restored,
                        ..
                    } => {
                        release_document(state, waiter_id);
                        let failure =
                            with_compensation_failure(failure, prior_compensation_failure.take());
                        finish_error(permit, failure)
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
                        prior_compensation_failure = Some(compensation_failure);
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
                            &mut next_phase,
                        )
                    }
                },
                HostMutationResult::Failed(error) => {
                    if state.document_owner == Some(waiter_id) {
                        release_document(state, waiter_id);
                    }
                    finish_error(permit, error)
                }
            }
        },
    ))
}

fn submit_package_restore(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    restore: HostPackageRestore,
    effect: PackageRuntimeEffect,
    original: DaemonTransportError,
    permit: HostWorkPermit,
    next_phase: &mut u64,
    failed_package_effect: &mut Option<(PackageRuntimeEffect, DaemonTransportError)>,
) -> ControlPoll {
    if state.document_owner != Some(waiter_id) {
        let failure = PackageRollbackFailure {
            step: "restore_admission",
            package_name: None,
            error: Box::new(DaemonTransportError::Protocol(
                "package restore requires the original document reservation",
            )),
        };
        return retain_package_recovery(state, effect, original, failure, permit);
    }

    // The document reservation remains held through the first whole-state
    // restore. No code can replay this snapshot after reservation release.
    *failed_package_effect = Some((effect, original));
    submit_phase(
        daemon,
        state,
        waiter_id,
        HostMutationCommand::RestorePackage(restore),
        permit,
        next_phase,
    )
}

fn retain_package_recovery(
    state: &mut DaemonControlState,
    effect: PackageRuntimeEffect,
    original: DaemonTransportError,
    failure: PackageRollbackFailure,
    permit: HostWorkPermit,
) -> ControlPoll {
    state.package_recovery_required = Some(PackageRecoveryRequired {
        original: original.to_string(),
        compensation: failure.error.to_string(),
        _effect: effect,
        _permit: permit,
    });
    ControlPoll::Ready(Err(DaemonTransportError::PackageCompensation {
        original: Box::new(original),
        rollbacks: vec![failure],
    }))
}

pub(crate) fn handles(request: &DaemonRequest) -> bool {
    is_package_read(request)
        || is_package_prepare(request)
        || is_spawn_target_read(request)
        || is_spawn_target_prepare(request)
        || is_session_type_read(request)
        || is_session_type_prepare(request)
}

fn admit_or_park_commit(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    prepared: PreparedMutation,
    permit: HostWorkPermit,
    must_finish: bool,
    retained: &mut Option<(PreparedMutation, HostWorkPermit)>,
    next_phase: &mut u64,
) -> ControlPoll {
    // Anything that reaches this site can park on the document reservation.
    // Transport closure must not retire its handoff.
    debug_assert!(must_finish, "a parkable host mutation must finish");
    match admit_document(
        state,
        waiter_id,
        prepared.base_revision,
        daemon.state_view().0,
    ) {
        DocumentAdmission::Granted => submit_phase(
            daemon,
            state,
            waiter_id,
            HostMutationCommand::Commit(HostCommit { prepared }),
            permit,
            next_phase,
        ),
        DocumentAdmission::Busy => {
            *retained = Some((prepared, permit));
            ControlPoll::Pending
        }
        DocumentAdmission::Stale => {
            wake_next_document_waiter(state);
            finish_error(
                permit,
                HostMutationError {
                    code: "host_prepared_revision_stale".to_string(),
                    message: "the Hub state changed while the host mutation was prepared"
                        .to_string(),
                },
            )
        }
    }
}

fn submit_phase(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    command: HostMutationCommand,
    permit: HostWorkPermit,
    next_phase: &mut u64,
) -> ControlPoll {
    let Some(runtime) = daemon.runtime() else {
        return finish_error(
            permit,
            HostMutationError {
                code: "daemon_not_running".to_string(),
                message: "the Hub runtime stopped before host work completed".to_string(),
            },
        );
    };
    let phase = *next_phase;
    let identity = HostJobIdentity { waiter_id, phase };
    match runtime
        .host_executor()
        .submit(identity, HostCommand::Mutation(command), permit)
    {
        Ok(()) => {
            *next_phase = next_phase.saturating_add(1);
            ControlPoll::Pending
        }
        Err(error) => {
            if state.document_owner == Some(waiter_id) {
                release_document(state, waiter_id);
            }
            ControlPoll::Ready(Ok(submit_error_response(error)))
        }
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
        crate::daemon::control::pending::mark_request_ready(
            state,
            next,
            ReadyClass::HostCompletion,
            crate::daemon::control::pending::READY_HOST_COMPLETION,
        );
    }
}

fn finish_reply(permit: HostWorkPermit, reply: crate::host_mutations::HostReply) -> ControlPoll {
    let charge = permit.into_prepared_charge(reply.logical_bytes);
    ControlPoll::ReadyHost(Ok(reply.response), charge)
}

fn finish_error(permit: HostWorkPermit, error: HostMutationError) -> ControlPoll {
    let charge = permit.into_prepared_charge(0);
    ControlPoll::ReadyHost(Ok(host_error_response(error)), charge)
}

fn finish_transport_error(permit: HostWorkPermit, error: DaemonTransportError) -> ControlPoll {
    let charge = permit.into_prepared_charge(0);
    ControlPoll::ReadyHost(Err(error), charge)
}

fn host_error_response(error: HostMutationError) -> DaemonResponse {
    error_response(&error.code, "host_execution", &error.message)
}

fn submit_error_response(error: HostSubmitError) -> DaemonResponse {
    match error {
        HostSubmitError::Full => error_response(
            "host_executor_full",
            "host_execution",
            "the host executor queue refused a reserved operation",
        ),
        HostSubmitError::Stopped => error_response(
            "host_executor_stopped",
            "host_execution",
            "the host executor stopped before it accepted the operation",
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
pub(crate) fn package_recovery_response(
    state: &DaemonControlState,
    request: &DaemonRequest,
) -> Option<DaemonResponse> {
    let recovery = state.package_recovery_required.as_ref()?;
    let blocked = is_package_read(request)
        || is_package_prepare(request)
        || matches!(
            request,
            DaemonRequest::StartPackageEntrypoint { .. }
                | DaemonRequest::RestartPackageEntrypoint { .. }
        );
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
    let source = match request {
        DaemonRequest::CreateSessionType { source, .. }
        | DaemonRequest::UpdateSessionType { source, .. }
        | DaemonRequest::DeleteSessionType { source, .. } => source,
        _ => return None,
    };
    let botster_hub_client::DaemonSessionTypeMutationSource::Repo { target_id } = source else {
        return None;
    };
    let root = hub_state
        .spawn_targets
        .iter()
        .find(|target| target.target_id == *target_id)
        .map(|target| &target.root)?;
    state.blocked_session_type_roots.get(root).copied()
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn recovery_test_daemon() -> (HubDaemon, std::path::PathBuf) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        let directory = std::path::PathBuf::from("target")
            .join("botster-hub-test-data")
            .join(format!("package-restore-ownership-{unique}"));
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
    fn package_restore_without_document_ownership_retains_recovery_and_submits_nothing() {
        let (mut daemon, directory) = recovery_test_daemon();
        let previous_state = daemon.state_view().1;
        let previous_packages = daemon.package_registry_view();
        let effect = PackageRuntimeEffect::Enable {
            package_name: "broken.plugin".to_string(),
            previous_state: previous_state.clone(),
            previous_packages: previous_packages.clone(),
        };
        let restore = HostPackageRestore {
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
            restore,
            effect,
            DaemonTransportError::DaemonNotRunning,
            permit,
            &mut next_phase,
            &mut failed_package_effect,
        );

        assert!(matches!(
            poll,
            ControlPoll::Ready(Err(DaemonTransportError::PackageCompensation {
                ref rollbacks,
                ..
            })) if rollbacks.len() == 1 && rollbacks[0].step == "restore_admission"
        ));
        assert!(state.package_recovery_required.is_some());
        assert!(failed_package_effect.is_none());
        assert_eq!(next_phase, 7);
        assert!(matches!(
            daemon
                .runtime()
                .expect("runtime")
                .host_executor()
                .poll_completion(),
            crate::host_executor::HostCompletionPoll::Empty
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
}
