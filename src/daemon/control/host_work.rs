//! Bounded host execution for control reads and durable mutations.

use botster_hub_client::{DaemonDiagnostic, DaemonOperatorError, DaemonRequest, DaemonResponse};

use crate::HubDaemon;
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::error::{DaemonTransportError, PackageRollbackFailure};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::ReadyClass;
use crate::host_executor::{
    HostCommand, HostJobIdentity, HostResult, HostSubmissionFailure, HostSubmitError,
    HostWorkPermit,
};
use crate::host_mutations::{
    HostCommit, HostMutationCommand, HostMutationError, HostMutationResult, HostPackageEffect,
    HostPackageRestore, HostPackageRuntimeRestore, HostPrepare, HostRead, HostRecover,
    PackageRuntimeEffect, PreparedMutation, RecoveryOutcome, SessionTypeRecovery,
};
use crate::owner_identity::WaiterId;

pub(crate) enum DocumentAdmission {
    Granted,
    Busy,
    Stale,
}

/// One unresolved host operation. Each variant retains the original operation slot.
pub(crate) enum HostRecoveryRequired {
    PackageFamilies {
        owner_permit: Option<crate::daemon::owner_budget::OwnerPermit>,
        _result: Box<HostMutationResult>,
        _fault: crate::runtime::PackageEntityCleanupError,
        _permit: HostWorkPermit,
    },
    PackageEvents {
        owner_permit: Option<crate::daemon::owner_budget::OwnerPermit>,
        _result: Box<HostMutationResult>,
        _fault: Option<crate::package_event_router::EventOwnerWorkError>,
        _submission: Option<HostSubmissionFailure>,
        _permit: Option<HostWorkPermit>,
    },
    Package(PackageRecoveryRequired),
    ManagedGit(crate::daemon::control::managed_git::ManagedGitRecoveryRequired),
    Submission {
        owner_permit: Option<crate::daemon::owner_budget::OwnerPermit>,
        failure: HostSubmissionFailure,
        package_restore: Option<(PackageRuntimeEffect, DaemonTransportError)>,
        managed_worktree: Option<crate::managed_git_worktrees::PreparedManagedWorktree>,
    },
}

impl HostRecoveryRequired {
    /// Recovery rows live until daemon teardown. Live removal must release this permit through OwnerBudget.
    pub(crate) fn retain_owner_permit(&mut self, permit: crate::daemon::owner_budget::OwnerPermit) {
        let retained = match self {
            Self::PackageEvents { owner_permit, .. }
            | Self::PackageFamilies { owner_permit, .. }
            | Self::Submission { owner_permit, .. } => owner_permit,
            Self::Package(recovery) => &mut recovery.owner_permit,
            Self::ManagedGit(recovery) => &mut recovery.owner_permit,
        };
        assert!(
            retained.is_none(),
            "terminal recovery receives the original Owner permit once"
        );
        *retained = Some(permit);
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
            owner_permit: None,
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
    owner_permit: Option<crate::daemon::owner_budget::OwnerPermit>,
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
    let runtime = daemon.runtime()?;
    let config = runtime.config().clone();
    let data_directory = config.data_directory.clone();
    let command = if matches!(request, DaemonRequest::IssueLocalWebrtcBootstrap { .. }) {
        HostMutationCommand::ValidateBootstrap {
            request,
            base_revision,
            packages,
        }
    } else if is_entrypoint_request(&request) {
        HostMutationCommand::Read(HostRead::Entrypoint {
            request,
            config,
            packages,
        })
    } else if is_package_read(&request) {
        HostMutationCommand::Read(HostRead::Package {
            request,
            config: config.clone(),
            packages,
        })
    } else if is_package_prepare(&request) {
        HostMutationCommand::Prepare(HostPrepare::Package {
            request,
            base_revision,
            state: state_view,
            packages,
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
        return Some(ControlStep::ready(submit_error_response(error.error)));
    }

    let mut retained_prepare: Option<(PreparedMutation, HostWorkPermit)> = None;
    let mut prior_compensation_failure: Option<HostMutationError> = None;
    let mut failed_package_effect: Option<(PackageRuntimeEffect, DaemonTransportError)> = None;
    let mut next_phase = 2;
    let mut event_cleanup: Option<(
        HostMutationResult,
        crate::package_event_router::EventOwnerWorkId,
    )> = None;
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
                            &mut next_phase,
                            &mut event_cleanup,
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
                return finish_error(
                    permit,
                    HostMutationError {
                        code: "host_completion_kind_mismatch".to_string(),
                        message: "the host executor returned a non-mutation result".to_string(),
                    },
                );
            };
            let mut result = result;
            if let Some(cleanup) = package_event_cleanup(&mut result) {
                if !cleanup.event_plane_faults.is_empty() {
                    return retain_event_cleanup(
                        state,
                        waiter_id,
                        result,
                        None,
                        None,
                        Some(permit),
                    );
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
                        &mut next_phase,
                        &mut event_cleanup,
                    );
                }
                if let Err(fault) = daemon
                    .runtime()
                    .expect("family cleanup retains its runtime")
                    .begin_host_package_entity_cleanup(cleanup)
                {
                    state.host_recovery.insert(
                        waiter_id,
                        HostRecoveryRequired::PackageFamilies {
                            owner_permit: None,
                            _result: Box::new(result),
                            _fault: fault,
                            _permit: permit,
                        },
                    );
                    return ControlPoll::Ready(Ok(error_response(
                        "entity_family_generation_exhausted",
                        "packages",
                        "entity family cleanup exhausted generation identifiers",
                    )));
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
                            &mut next_phase,
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
                        let runtime = daemon
                            .runtime()
                            .expect("a committed package has a running runtime");
                        let command = HostMutationCommand::ApplyPackageEffect(HostPackageEffect {
                            effect,
                            runtime: runtime.host_package_runtime(),
                            config: runtime.config().clone(),
                            packages: daemon.package_registry_view(),
                            reply: committed.reply,
                        });
                        return submit_phase(
                            daemon,
                            state,
                            waiter_id,
                            command,
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
                    let runtime = daemon
                        .runtime()
                        .expect("package recovery retains its runtime");
                    let command =
                        HostMutationCommand::RestorePackageRuntime(HostPackageRuntimeRestore {
                            effect,
                            original,
                            runtime: runtime.host_package_runtime(),
                            config: runtime.config().clone(),
                        });
                    submit_phase(daemon, state, waiter_id, command, permit, &mut next_phase)
                }
                HostMutationResult::PackageEffectApplied { reply, cleanup } => {
                    daemon
                        .runtime_mut()
                        .expect("package effect retains its runtime")
                        .apply_host_package_cleanup(cleanup);
                    release_document(state, waiter_id);
                    match reply {
                        Ok(reply) => finish_reply(permit, reply),
                        Err(error) => finish_error(permit, error),
                    }
                }
                HostMutationResult::PackageEffectFailed {
                    effect,
                    error,
                    cleanup,
                } => {
                    daemon
                        .runtime_mut()
                        .expect("package effect retains its runtime")
                        .apply_host_package_cleanup(cleanup);
                    let runtime = daemon
                        .runtime()
                        .expect("package recovery retains its runtime");
                    if let Some(restore) =
                        effect.restore_command(runtime.config().data_directory.clone())
                    {
                        submit_package_restore(
                            daemon,
                            state,
                            waiter_id,
                            restore,
                            effect,
                            error,
                            permit,
                            &mut next_phase,
                            &mut failed_package_effect,
                        )
                    } else {
                        release_document(state, waiter_id);
                        retain_package_recovery(
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
                    release_document(state, waiter_id);
                    if rollbacks.is_empty() {
                        finish_transport_error(permit, original)
                    } else {
                        state.host_recovery.insert(
                            waiter_id,
                            HostRecoveryRequired::Package(PackageRecoveryRequired {
                                owner_permit: None,
                                original: original.to_string(),
                                compensation: rollbacks
                                    .iter()
                                    .map(|failure| failure.error.to_string())
                                    .collect::<Vec<_>>()
                                    .join("; "),
                                _effect: effect,
                                _permit: permit,
                            }),
                        );
                        ControlPoll::Ready(Err(DaemonTransportError::PackageCompensation {
                            original: Box::new(original),
                            rollbacks,
                        }))
                    }
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
                    retain_package_recovery(state, waiter_id, effect, original, failure, permit)
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
        return retain_package_recovery(state, waiter_id, effect, original, failure, permit);
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
    poll
}

fn retain_package_recovery(
    state: &mut DaemonControlState,
    waiter_id: WaiterId,
    effect: PackageRuntimeEffect,
    original: DaemonTransportError,
    failure: PackageRollbackFailure,
    permit: HostWorkPermit,
) -> ControlPoll {
    state.host_recovery.insert(
        waiter_id,
        HostRecoveryRequired::Package(PackageRecoveryRequired {
            owner_permit: None,
            original: original.to_string(),
            compensation: failure.error.to_string(),
            _effect: effect,
            _permit: permit,
        }),
    );
    ControlPoll::Ready(Err(DaemonTransportError::PackageCompensation {
        original: Box::new(original),
        rollbacks: vec![failure],
    }))
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
    if matches!(
        prepared.change,
        crate::host_mutations::PreparedChange::PackageConfiguration(_)
    ) && state
        .host_recovery
        .values()
        .any(|recovery| matches!(recovery, HostRecoveryRequired::Package(_)))
    {
        state.document_waiters.remove(&waiter_id);
        wake_next_document_waiter(state);
        return finish_error(
            permit,
            HostMutationError {
                code: "package_recovery_required".to_string(),
                message: "package recovery is required before this prepared mutation can commit"
                    .to_string(),
            },
        );
    }
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

fn package_event_cleanup(
    result: &mut HostMutationResult,
) -> Option<&mut crate::runtime::package_effect::HostPackageCleanup> {
    match result {
        HostMutationResult::PackageEffectApplied { cleanup, .. }
        | HostMutationResult::PackageEffectFailed { cleanup, .. }
        | HostMutationResult::PackageRuntimeRestored { cleanup, .. } => Some(cleanup),
        _ => None,
    }
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
            owner_permit: None,
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
                command,
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
                command,
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
                command,
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
    fn exhausted_phase_retains_package_effect_and_document_ownership() {
        let (mut daemon, directory) = recovery_test_daemon();
        let runtime = daemon.runtime().expect("runtime");
        let permit = runtime.host_executor().try_reserve().expect("host slot");
        let command = HostMutationCommand::ApplyPackageEffect(HostPackageEffect {
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
        });
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
            matches!(&failure.command, HostCommand::Mutation(HostMutationCommand::ApplyPackageEffect(effect)) if matches!(&effect.effect, PackageRuntimeEffect::Disable { package_name } if package_name == "retained.plugin"))
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
        assert!(
            state
                .host_recovery
                .values()
                .any(|recovery| matches!(recovery, HostRecoveryRequired::Package(_)))
        );
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
