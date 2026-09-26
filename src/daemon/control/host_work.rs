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
    Terminal(TerminalRecovery),
    PackageFamilyWork {
        owner_permit: Option<crate::daemon::owner_budget::OwnerPermit>,
        _work: Box<super::host_family::FamilyWork>,
    },
    PackageFamilies {
        owner_permit: Option<crate::daemon::owner_budget::OwnerPermit>,
        _work: Box<super::host_family::FamilyWork>,
        _fault: crate::runtime::PackageEntityCleanupError,
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

pub(crate) struct TerminalRecovery {
    owner_permit: Option<crate::daemon::owner_budget::OwnerPermit>,
    family: Option<Box<super::host_family::FamilyWork>>,
    job: crate::host_disposal::Job,
}

impl HostRecoveryRequired {
    /// Recovery rows live until daemon teardown. Live removal must release this permit through OwnerBudget.
    pub(crate) fn retain_owner_permit(&mut self, permit: crate::daemon::owner_budget::OwnerPermit) {
        let retained = match self {
            Self::Terminal(recovery) => &mut recovery.owner_permit,
            Self::PackageFamilyWork { owner_permit, .. }
            | Self::PackageEvents { owner_permit, .. }
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

    fn into_terminal(self, identity: HostJobIdentity) -> Self {
        let (owner_permit, family, parts) = match self {
            Self::Terminal(_) => return self,
            Self::PackageFamilyWork {
                owner_permit,
                mut _work,
            } => {
                let Some(parts) = _work.take_terminal_parts(identity) else {
                    return Self::PackageFamilyWork {
                        owner_permit,
                        _work,
                    };
                };
                (owner_permit, Some(_work), parts)
            }
            Self::PackageFamilies {
                owner_permit,
                mut _work,
                _fault,
            } => {
                let Some(parts) = _work.take_terminal_parts(identity) else {
                    return Self::PackageFamilies {
                        owner_permit,
                        _work,
                        _fault,
                    };
                };
                (owner_permit, Some(_work), parts.with_payload(_fault))
            }
            Self::PackageEvents {
                owner_permit,
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
                                owner_permit,
                                _result,
                                _fault,
                                _submission: None,
                                _permit: None,
                            };
                        }
                    };
                (
                    owner_permit,
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
                recovery.owner_permit,
                None,
                crate::host_disposal::Parts {
                    storage: None,
                    identity,
                    permit: recovery._permit,
                    model: None,
                    payload: Box::new((recovery.original, recovery.compensation, recovery._effect)),
                },
            ),
            Self::ManagedGit(recovery) => {
                let (owner, parts) = recovery.into_terminal(identity);
                (owner, None, parts)
            }
            Self::Submission {
                owner_permit,
                failure,
                package_restore,
                managed_worktree,
            } => (
                owner_permit,
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
            owner_permit,
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
            if let Some(permit) = terminal.owner_permit.take() {
                state.budget.release(permit);
            }
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

fn host_operation_label(request: &DaemonRequest) -> &'static str {
    use crate::PackageAction;

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
            authority: authority
                .clone()
                .expect("File host mutation retains its state authority"),
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
            authority: authority
                .clone()
                .expect("File host mutation retains its state authority"),
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
            authority: authority.expect("File host mutation retains its state authority"),
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
    let observes_session_type_catalog = matches!(
        &command,
        HostMutationCommand::Read(HostRead::SessionType { .. })
            | HostMutationCommand::Prepare(HostPrepare::SessionType { .. })
    );
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
        } = self;
        let waiter_id = *waiter_id;
        let must_finish = *must_finish;
        let operation = *operation;
        if let Some((prepared, permit)) = retained_prepare.take() {
            return admit_or_park_commit(
                daemon,
                state,
                waiter_id,
                prepared,
                permit,
                must_finish,
                operation,
                retained_prepare,
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
                    (result, permit)
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
        if !matches!(
            result,
            HostMutationResult::PublishedUncertain { .. }
                | HostMutationResult::ExternalEffectUncertain { .. }
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
            HostMutationResult::ExternalEffectUncertain {
                pending,
                rollback,
                cause,
            } => {
                let (code, message) = cause.client_error();
                state.retain_uncertain_external(waiter_id, pending, rollback, cause);
                release_document(state, waiter_id);
                drop(permit);
                ControlPoll::Ready(Ok(error_response(code, "repo_session_type", message)))
            }
            HostMutationResult::Prepared(prepared) => admit_or_park_commit(
                daemon,
                state,
                waiter_id,
                prepared,
                permit,
                must_finish,
                operation,
                retained_prepare,
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
                    let command = HostMutationCommand::ApplyPackageEffect(HostPackageEffect {
                        effect,
                        runtime: runtime.host_package_runtime(),
                        config: runtime.config().clone(),
                        packages: daemon.package_registry_view(),
                        reply: committed.reply,
                    });
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
                let command =
                    HostMutationCommand::RestorePackageRuntime(HostPackageRuntimeRestore {
                        effect,
                        original,
                        runtime: runtime.host_package_runtime(),
                        config: runtime.config().clone(),
                    });
                submit_phase(daemon, state, waiter_id, command, permit, next_phase)
            }
            HostMutationResult::PackageEffectApplied { reply, cleanup } => {
                daemon
                    .runtime_mut()
                    .expect("package effect retains its runtime")
                    .apply_host_package_cleanup(cleanup);
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
                cleanup,
            } => {
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
                        restore,
                        effect,
                        error,
                        permit,
                        next_phase,
                        failed_package_effect,
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
                ingest_worktree_lifecycle_events(daemon, state, error.event.as_slice());
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
    if !state.reserve_uncertain_publication(waiter_id) {
        release_document(state, waiter_id);
        let failure = PackageRollbackFailure {
            step: "restore_admission",
            package_name: None,
            error: Box::new(DaemonTransportError::Protocol(
                "another unresolved state publication owns the retention cell",
            )),
        };
        let _ = retain_package_recovery(state, waiter_id, effect, original, failure, permit);
        return ControlPoll::Ready(Ok(error_response(
            "state_publication_slot_occupied",
            "hub_state",
            "another unresolved state publication owns the retention cell",
        )));
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
    operation: &'static str,
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
            let poll = submit_phase(
                daemon,
                state,
                waiter_id,
                HostMutationCommand::Commit(HostCommit { prepared }),
                permit,
                next_phase,
            );
            if !matches!(poll, ControlPoll::Pending) {
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
                owner_permit: None,
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
            owner_permit: None,
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

fn finish_transport_error(permit: HostWorkPermit, error: DaemonTransportError) -> ControlPoll {
    let charge = permit.into_prepared_charge(0);
    ControlPoll::ReadyHost(Err(error), charge)
}

fn host_error_response(operation: &str, error: HostMutationError) -> DaemonResponse {
    let mut response = error_response(&error.code, operation, &error.message);
    response.events.extend(error.event);
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
    use crate::daemon::owner_loop::UncertainPublicationKind;
    use crate::host_executor::HostCompletion;
    use crate::host_mutations::{ExternalEffectCause, RollbackDescriptor};
    use crate::persistence::{ExternalFileIntent, FileCommitOutcome, FileHubStateStore};
    use crate::runtime::package_effect::HostPackageCleanup;
    use crate::session_types::SessionTypeError;
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
            HostMutationCommand::Prepare(HostPrepare::SpawnTarget {
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
            }),
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
            HostMutationCommand::Prepare(HostPrepare::Package {
                request: DaemonRequest::SetPackageConfiguration {
                    package_name: "retained.plugin".to_string(),
                    values: Default::default(),
                },
                base_revision: revision,
                authority: authority.clone(),
                state,
                packages,
                data_directory: directory.clone(),
            }),
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
    fn external_uncertainty_uses_the_host_completion_cell_and_releases_one_document_waiter() {
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
        let external_path = directory.join("repo/.botster/session-types.json");
        let pending = store
            .begin_shared_effect(
                prepared,
                revision,
                Some(ExternalFileIntent {
                    path: &external_path,
                    prior: None,
                    candidate: b"candidate repo bytes",
                }),
            )
            .expect("synchronize real external intent");
        poll_uncertain_result(
            &mut daemon,
            HostMutationResult::ExternalEffectUncertain {
                pending,
                rollback: RollbackDescriptor::SessionType {
                    previous: prior,
                    repo_file: None,
                },
                cause: ExternalEffectCause::RepoPublicationSyncUnconfirmed(SessionTypeError::new(
                    "repo_session_type_sync_uncertain",
                    "test uncertainty",
                )),
            },
            UncertainPublicationKind::External,
            "repo_session_type_publication_uncertain",
        );
        drop(authority);
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove external uncertainty test directory");
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
