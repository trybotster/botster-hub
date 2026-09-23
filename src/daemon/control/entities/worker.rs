//! Retained phases for package entity delivery.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::admission::reservations::PreparedSubscriptionIdentity;
use crate::host_executor::{
    HostCommand, HostCompletion, HostExecutor, HostJobIdentity, HostResult, HostSubmissionFailure,
    HostSubmitError, HostWorkPermit,
};
use crate::owner_identity::WaiterId;
use crate::plugin_entity::{Command, Completion, Payload, Registration, Target};

/// The pending row retains this state until its final worker phase completes.
pub(super) struct EntityWork {
    pub(super) target: Option<Arc<Target>>,
    pub(super) payload: Option<Payload>,
    pub(super) family: Option<Arc<String>>,
    pub(super) family_generation: Option<u64>,
    pub(super) registration: Option<Registration>,
    pub(super) reservation_identity: Option<PreparedSubscriptionIdentity>,
    pub(super) cursor: Option<Arc<Target>>,
    pub(super) delivery_target: Option<Arc<Target>>,
    pub(super) publication: Option<Arc<AtomicBool>>,
    pub(super) reply_live: Arc<AtomicBool>,
    pub(super) floor: u64,
    pub(super) snapshot: bool,
    pub(super) initial_target_visited: bool,
    pub(super) cancelled: bool,
    pub(super) registered: bool,
    pub(super) error: Option<(&'static str, &'static str)>,
    pub(super) finish: Option<crate::runtime::PackageEntityFanoutFinish>,
    pub(super) provider_input: Option<crate::plugin_entity::ProviderInput>,
    pub(super) provider_plan: Option<crate::runtime::provider::ProviderRequestPlan>,
    pub(super) provider_refusal: Option<crate::McpToolError>,
    pub(super) stage: Stage,
    phase: Phase,
    model: Option<crate::runtime::entity_model::Work>,
    model_operation: Option<crate::runtime::entity_model::Operation>,
    model_purpose: Option<ModelPurpose>,
    model_mutation: Option<(crate::package_entity_fanout::PackageEntityMutation, bool)>,
    generation_checked: Option<bool>,
    deferred_completion: Option<(HostJobIdentity, Completion)>,
    resync_pending: bool,
    catchup_seen: bool,
    cancel_resync_recorded: bool,
}

#[derive(Clone, Copy)]
enum ModelPurpose {
    CheckFamily,
    BeginSnapshot,
    SelectProvider,
    TakeFanout,
    Snapshot,
    Finish,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
    PrepareProvider,
    AcquireProvider,
    AdmitProvider,
    AdmissionRetirement,
    CausalRecovery,
    Provider,
    Begin,
    Deliver,
    Drain,
    Finish,
    Release,
}

/// A failed submission keeps the exact command and its original permit.
enum Phase {
    Unreserved(Option<HostJobIdentity>),
    Ready {
        identity: HostJobIdentity,
        permit: HostWorkPermit,
    },
    Running(HostJobIdentity),
    Completed(HostCompletion),
    Rejected(HostSubmissionFailure),
    Exhausted {
        _completion: HostCompletion,
    },
    Faulted {
        _completion: HostCompletion,
    },
    Terminal,
}

pub(super) enum Advance {
    Waiting,
    Capacity,
    Submitted,
    Degraded,
}

impl EntityWork {
    pub(super) fn take_terminal_parts(
        &mut self,
        executor: &HostExecutor,
        waiter: WaiterId,
    ) -> Option<crate::host_disposal::Parts> {
        if matches!(self.phase, Phase::Running(_)) {
            return None;
        }
        if let Phase::Unreserved(next) = self.phase {
            self.phase = Phase::Ready {
                identity: next.unwrap_or_else(|| HostJobIdentity::first(waiter)),
                permit: executor.try_reserve()?,
            };
        }
        if matches!(self.phase, Phase::Terminal) {
            self.phase = Phase::Ready {
                identity: HostJobIdentity::first(waiter),
                permit: executor.try_reserve()?,
            };
        }
        let mut result: Option<Box<dyn Send>> = None;
        let (identity, permit) = match std::mem::replace(&mut self.phase, Phase::Terminal) {
            Phase::Ready { identity, permit } => (identity, permit),
            Phase::Completed(completion)
            | Phase::Exhausted {
                _completion: completion,
            }
            | Phase::Faulted {
                _completion: completion,
            } => {
                let (identity, payload, permit) = completion.into_parts();
                result = Some(Box::new(payload));
                (identity, permit)
            }
            Phase::Rejected(failure) => {
                result = Some(Box::new(failure.command));
                (failure.identity, failure.permit)
            }
            _ => unreachable!("terminal disposal waits for the original Host receipt"),
        };
        self.cancel();
        Some(crate::host_disposal::Parts {
            storage: None,
            identity,
            permit,
            model: self.model.clone(),
            payload: Box::new((
                (
                    self.target.take(),
                    self.payload.take(),
                    self.family.take(),
                    self.registration.take(),
                    self.reservation_identity.take(),
                ),
                (
                    self.cursor.take(),
                    self.delivery_target.take(),
                    self.publication.take(),
                    self.reply_live.clone(),
                ),
                (
                    self.provider_input.take(),
                    self.provider_plan.take(),
                    self.provider_refusal.take(),
                ),
                (
                    self.model_operation.take(),
                    self.model_mutation.take(),
                    self.deferred_completion.take(),
                ),
                self.finish
                    .as_mut()
                    .and_then(crate::runtime::PackageEntityFanoutFinish::take_terminal_family),
                result,
            )),
        })
    }

    pub(super) fn retire_terminal(&mut self, runtime: &crate::HubRuntime) {
        if let Some(model) = self.model.take() {
            assert!(runtime.retire_terminal_entity_model(&model));
        }
        self.finish.take();
    }

    pub(super) fn new(target: Option<Arc<Target>>) -> Self {
        Self {
            target,
            payload: None,
            family: None,
            family_generation: None,
            registration: None,
            reservation_identity: None,
            cursor: None,
            delivery_target: None,
            publication: None,
            reply_live: Arc::new(AtomicBool::new(true)),
            floor: 0,
            snapshot: true,
            initial_target_visited: false,
            cancelled: false,
            registered: false,
            error: None,
            finish: None,
            provider_input: None,
            provider_plan: None,
            provider_refusal: None,
            stage: Stage::Provider,
            phase: Phase::Unreserved(None),
            model: None,
            model_operation: None,
            model_purpose: None,
            model_mutation: None,
            generation_checked: None,
            deferred_completion: None,
            resync_pending: false,
            catchup_seen: false,
            cancel_resync_recorded: false,
        }
    }

    #[cfg(test)]
    pub(super) fn test_cancelled_host_identity(&self) -> Option<HostJobIdentity> {
        if !self.cancelled {
            return None;
        }
        match self.phase {
            Phase::Running(identity) => Some(identity),
            _ => None,
        }
    }

    pub(super) fn cancel(&mut self) {
        self.cancelled = true;
        self.reply_live.store(false, Ordering::Release);
        if let Some(publication) = &self.publication {
            publication.store(false, Ordering::Release);
        }
    }

    /// Reserve capacity before the caller takes a provider result or mutation.
    pub(super) fn reserve(&mut self, executor: &HostExecutor, waiter: WaiterId) -> Advance {
        if let Phase::Unreserved(next) = self.phase {
            let Some(permit) = executor.try_reserve() else {
                return Advance::Capacity;
            };
            self.phase = Phase::Ready {
                identity: next.unwrap_or_else(|| HostJobIdentity::first(waiter)),
                permit,
            };
        }
        self.retry(executor)
    }

    pub(super) fn ready_identity(&self) -> Option<HostJobIdentity> {
        match &self.phase {
            Phase::Ready { identity, .. } => Some(*identity),
            _ => None,
        }
    }

    pub(super) fn accepts(&self, identity: HostJobIdentity) -> bool {
        matches!(self.phase, Phase::Running(expected) if identity == expected)
    }

    pub(super) fn retain_completion(
        &mut self,
        completion: HostCompletion,
    ) -> Result<(), HostCompletion> {
        if !self.accepts(completion.identity) {
            return Err(completion);
        }
        self.phase = Phase::Completed(completion);
        Ok(())
    }

    /// Transfer a completion without reducing its prepared-byte reservation.
    pub(super) fn take_completion(&mut self) -> Option<(HostJobIdentity, Completion)> {
        let Phase::Completed(completion) = &self.phase else {
            return None;
        };
        let expected = matches!(
            (&completion.result, self.stage),
            (
                HostResult::PluginEntity(Completion::ProviderPrepared(_)),
                Stage::PrepareProvider
            ) | (
                HostResult::PluginEntity(Completion::ProviderAdmitted(_)),
                Stage::AdmitProvider
            ) | (
                HostResult::PluginEntity(Completion::Prepared { .. }),
                Stage::PrepareProvider
                    | Stage::Provider
                    | Stage::Begin
                    | Stage::Deliver
                    | Stage::Drain
            ) | (
                HostResult::PluginEntity(Completion::Delivered { .. }),
                Stage::Deliver
            ) | (
                HostResult::PluginEntity(Completion::Reclaimed),
                Stage::Drain | Stage::Release
            ) | (
                HostResult::PluginEntity(Completion::Finished { .. }),
                Stage::Finish
            )
        );
        if !expected {
            let Phase::Completed(completion) = std::mem::replace(&mut self.phase, Phase::Terminal)
            else {
                unreachable!()
            };
            self.phase = Phase::Faulted {
                _completion: completion,
            };
            return None;
        }
        let admitted = matches!(
            completion.result,
            HostResult::PluginEntity(Completion::ProviderAdmitted(None))
        );
        let terminal = matches!(
            completion.result,
            HostResult::PluginEntity(Completion::Finished { .. })
        ) || (self.stage == Stage::Release && self.finish.is_none());
        let next = completion.identity.next_phase();
        if !terminal && next.is_none() {
            let Phase::Completed(completion) = std::mem::replace(&mut self.phase, Phase::Terminal)
            else {
                unreachable!()
            };
            self.phase = Phase::Exhausted {
                _completion: completion,
            };
            return None;
        }
        let Phase::Completed(completion) = std::mem::replace(&mut self.phase, Phase::Terminal)
        else {
            unreachable!()
        };
        let (identity, result, permit) = completion.into_parts();
        let HostResult::PluginEntity(result) = result else {
            unreachable!("an entity phase returns an entity completion")
        };
        if admitted {
            self.phase = Phase::Unreserved(next);
            drop(permit);
        } else if !terminal {
            self.phase = Phase::Ready {
                identity: next.expect("the next phase was checked"),
                permit,
            };
        }
        Some((identity, result))
    }

    pub(super) fn submit(&mut self, executor: &HostExecutor, command: Command) -> Advance {
        let Phase::Ready { identity, permit } = std::mem::replace(&mut self.phase, Phase::Terminal)
        else {
            unreachable!("reserve a phase before constructing an entity command")
        };
        self.submit_reserved(
            executor,
            identity,
            HostCommand::PluginEntity(command),
            permit,
        )
    }

    fn start_model(
        &mut self,
        runtime: &crate::HubRuntime,
    ) -> Result<Advance, crate::runtime::CausalTransitionStatus> {
        let Phase::Ready { identity, permit } = &self.phase else {
            return Ok(Advance::Waiting);
        };
        let operation = self
            .model_operation
            .take()
            .expect("the model input is retained");
        let work = match runtime.begin_entity_model(*identity, operation, permit) {
            Ok(work) => work,
            Err((status, operation)) => {
                self.model_operation = Some(operation);
                return Err(status);
            }
        };
        let command = HostCommand::EntityModel(work.clone());
        self.model = Some(work);
        let Phase::Ready { identity, permit } = std::mem::replace(&mut self.phase, Phase::Terminal)
        else {
            unreachable!();
        };
        Ok(self.submit_reserved(runtime.host_executor(), identity, command, permit))
    }

    fn submit_reserved(
        &mut self,
        executor: &HostExecutor,
        identity: HostJobIdentity,
        command: HostCommand,
        permit: HostWorkPermit,
    ) -> Advance {
        match executor.submit(identity, command, permit) {
            Ok(()) => {
                self.phase = Phase::Running(identity);
                Advance::Submitted
            }
            Err(failure) => {
                let advance = match failure.error {
                    HostSubmitError::Full => Advance::Capacity,
                    HostSubmitError::Stopped
                    | HostSubmitError::PhaseExhausted
                    | HostSubmitError::WrongExecutor => Advance::Degraded,
                };
                self.phase = Phase::Rejected(failure);
                advance
            }
        }
    }

    fn retry(&mut self, executor: &HostExecutor) -> Advance {
        match &self.phase {
            Phase::Rejected(failure) if matches!(failure.error, HostSubmitError::Full) => {}
            Phase::Rejected(_) | Phase::Exhausted { .. } | Phase::Faulted { .. } => {
                return Advance::Degraded;
            }
            _ => return Advance::Waiting,
        }
        let Phase::Rejected(failure) = std::mem::replace(&mut self.phase, Phase::Terminal) else {
            unreachable!()
        };
        self.submit_reserved(executor, failure.identity, failure.command, failure.permit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_executor::{
        HOST_OPERATION_CAPACITY, HOST_PREPARED_BYTE_CAPACITY, HostCompletionPoll,
    };
    use crate::package_entity_fanout::PackageEntityMutation;
    use std::time::{Duration, Instant};

    fn receive(executor: &HostExecutor) -> HostCompletion {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match executor.poll_completion() {
                HostCompletionPoll::Ready(completion) => return completion,
                HostCompletionPoll::Empty => {
                    assert!(Instant::now() < deadline, "the entity worker must complete");
                    std::thread::yield_now();
                }
                HostCompletionPoll::Stopped => panic!("the Host executor stopped"),
            }
        }
    }

    fn assert_reserved_slots(executor: &HostExecutor, expected: usize) {
        let mut available = Vec::new();
        while let Some(permit) = executor.try_reserve() {
            available.push(permit);
        }
        assert_eq!(available.len(), HOST_OPERATION_CAPACITY - expected);
    }

    fn mutation() -> PackageEntityMutation {
        PackageEntityMutation::Upsert {
            admission: None,
            entity_type: "task".into(),
            snapshot_seq: 17,
            id: "a".into(),
            entity: serde_json::json!({"body": "x".repeat(128 * 1024)}),
        }
    }

    #[test]
    fn cancelled_entity_retains_worker_capacity_until_reclamation_completes() {
        let executor = HostExecutor::new();
        let waiter = WaiterId(701);
        let mut work = EntityWork::new(None);
        work.stage = Stage::Begin;
        assert!(matches!(work.reserve(&executor, waiter), Advance::Waiting));
        let first = work.ready_identity().unwrap();
        assert!(matches!(
            work.submit(&executor, Command::PrepareMutation(mutation())),
            Advance::Submitted
        ));
        let publication = Arc::new(AtomicBool::new(true));
        work.publication = Some(Arc::clone(&publication));
        work.cancel();
        assert!(!publication.load(Ordering::Acquire));
        assert!(!work.reply_live.load(Ordering::Acquire));
        assert_reserved_slots(&executor, 1);
        assert!(work.retain_completion(receive(&executor)).is_ok());
        let (identity, result) = work.take_completion().unwrap();
        assert_eq!(identity, first);
        let Completion::Prepared { payload, .. } = result else {
            panic!("the worker must prepare the mutation");
        };
        assert_eq!(payload.sequence(), Some(17));
        let Phase::Ready { permit, .. } = &work.phase else {
            panic!("the next phase must retain the operation permit");
        };
        assert_eq!(
            permit.reserved_prepared_bytes(),
            HOST_PREPARED_BYTE_CAPACITY
        );
        assert_reserved_slots(&executor, 1);
        work.stage = Stage::Release;
        assert!(matches!(
            work.submit(&executor, Command::Reclaim(payload)),
            Advance::Submitted
        ));
        assert_reserved_slots(&executor, 1);
        assert!(work.retain_completion(receive(&executor)).is_ok());
        assert_reserved_slots(&executor, 1);
        assert!(matches!(
            work.take_completion(),
            Some((_, Completion::Reclaimed))
        ));
        assert_reserved_slots(&executor, 0);
    }

    #[test]
    fn cancelling_a_running_delivery_does_not_release_its_completion_fence() {
        let identity = HostJobIdentity::first(WaiterId(703));
        let mut work = EntityWork::new(None);
        work.phase = Phase::Running(identity);
        let publication = Arc::new(AtomicBool::new(true));
        work.publication = Some(Arc::clone(&publication));

        work.cancel();

        assert!(work.accepts(identity));
        assert!(!publication.load(Ordering::Acquire));
    }

    #[test]
    fn entity_completion_requires_the_exact_waiter_and_phase() {
        let executor = HostExecutor::new();
        let identity = HostJobIdentity::first(WaiterId(702));
        let mut work = EntityWork::new(None);
        work.phase = Phase::Running(identity);
        for wrong in [
            HostJobIdentity::first(WaiterId(703)),
            identity.next_phase().unwrap(),
        ] {
            let completion = HostCompletion::for_test(
                wrong,
                HostResult::PluginEntity(Completion::Reclaimed),
                executor.try_reserve().unwrap(),
            );
            let returned = work.retain_completion(completion).unwrap_err();
            assert_eq!(returned.identity, wrong);
            assert!(work.accepts(identity));
            assert_reserved_slots(&executor, 1);
            drop(returned);
        }
    }

    #[test]
    fn unexpected_provider_completion_retains_capacity_and_never_advances() {
        for stage in [
            Stage::PrepareProvider,
            Stage::AcquireProvider,
            Stage::Provider,
        ] {
            let executor = HostExecutor::new();
            let identity = HostJobIdentity::first(WaiterId(705));
            let mut work = EntityWork::new(None);
            work.stage = stage;
            work.phase = Phase::Running(identity);
            let completion = HostCompletion::for_test(
                identity,
                HostResult::PluginEntity(Completion::ProviderAdmitted(None)),
                executor.try_reserve().unwrap(),
            );
            work.retain_completion(completion).unwrap();
            assert!(work.take_completion().is_none());
            work.cancel();
            assert!(matches!(
                work.reserve(&executor, identity.waiter_id),
                Advance::Degraded
            ));
            let Phase::Faulted {
                _completion: retained,
            } = &work.phase
            else {
                panic!("an unexpected admission completion must remain faulted");
            };
            assert_eq!(retained.identity, identity);
            assert_eq!(work.stage, stage);
            assert_reserved_slots(&executor, 1);
        }
    }

    #[test]
    fn known_provider_admission_releases_capacity_and_preserves_the_next_phase() {
        let executor = HostExecutor::new();
        let identity = HostJobIdentity {
            waiter_id: WaiterId(706),
            phase: 9,
        };
        let mut work = EntityWork::new(None);
        work.stage = Stage::AdmitProvider;
        work.phase = Phase::Running(identity);
        work.retain_completion(HostCompletion::for_test(
            identity,
            HostResult::PluginEntity(Completion::ProviderAdmitted(None)),
            executor.try_reserve().unwrap(),
        ))
        .unwrap();
        assert!(matches!(
            work.take_completion(),
            Some((_, Completion::ProviderAdmitted(None)))
        ));
        assert_reserved_slots(&executor, 0);
        assert!(matches!(
            work.reserve(&executor, identity.waiter_id),
            Advance::Waiting
        ));
        assert_eq!(work.ready_identity(), identity.next_phase());
    }

    #[test]
    fn entity_phase_exhaustion_retains_the_completion_and_its_permit() {
        let executor = HostExecutor::new();
        let identity = HostJobIdentity {
            waiter_id: WaiterId(704),
            phase: u64::MAX,
        };
        let mut work = EntityWork::new(None);
        work.phase = Phase::Running(identity);
        let completion = HostCompletion::for_test(
            identity,
            HostResult::PluginEntity(Completion::Prepared {
                payload: Payload::mutation(mutation()),
                family: Arc::new("task".into()),
                registration: None,
            }),
            executor.try_reserve().unwrap(),
        );
        assert!(work.retain_completion(completion).is_ok());
        assert!(work.take_completion().is_none());
        work.cancel();
        assert!(matches!(work.phase, Phase::Exhausted { .. }));
        assert!(matches!(
            work.reserve(&executor, identity.waiter_id),
            Advance::Degraded
        ));
        assert_reserved_slots(&executor, 1);
    }

    fn causal_daemon() -> crate::HubDaemon {
        let directory = std::env::temp_dir().join(format!(
            "entity-causal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "entity-causal-test".into(),
                display_name: "Entity causal test".into(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(directory),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".into(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .unwrap();
        crate::HubDaemon::start(config).unwrap()
    }

    fn retained_fanout(
        runtime: &crate::HubRuntime,
        state: &mut crate::daemon::owner_loop::DaemonControlState,
    ) -> (super::super::PendingPluginEntity, u64) {
        use crate::package_event_router::LeaseIdentity;
        let family = "producer.item";
        runtime.test_store_family_payload(PackageEntityMutation::Upsert {
            admission: None,
            entity_type: family.into(),
            snapshot_seq: 1,
            id: "item".into(),
            entity: serde_json::json!({"id":"item"}),
        });
        let scope = runtime
            .causal_scopes()
            .mint_with_lease(Some(LeaseIdentity::AdmittedEntityMutation {
                family_token: runtime.test_family_causal_token(family),
                seq: 1,
            }))
            .unwrap();
        runtime.test_store_pending_lease(scope, family, 1);
        runtime.begin_package_entity_provider_snapshot(family, 0);
        let crate::runtime::PackageEntitySnapshotStep::Ready(item) =
            runtime.step_package_entity_provider_snapshot(family)
        else {
            panic!("the family must return the leased mutation");
        };
        let (payload, finish) = item.into_parts();
        drop(payload);
        let waiter_id = WaiterId(705);
        let mut work = EntityWork::new(None);
        work.stage = Stage::Release;
        work.finish = Some(finish);
        work.phase = Phase::Completed(HostCompletion::for_test(
            HostJobIdentity::first(waiter_id),
            HostResult::PluginEntity(Completion::Reclaimed),
            runtime.host_executor().try_reserve().unwrap(),
        ));
        (
            super::super::PendingPluginEntity {
                terminal: None,
                request_id: "retained-fanout".into(),
                waiter_id,
                ready_key: None,
                deadline_key: None,
                identity: None,
                invocation: None,
                result: None,
                work,
                kind: super::super::PendingPluginEntityKind::Fanout {
                    permit: state.budget.reserve().unwrap(),
                },
            },
            scope,
        )
    }

    #[test]
    fn reclaimed_fanout_keeps_its_lease_and_permits_until_causal_table_application() {
        use crate::package_event_router::{CausalAdmitResult, CausalOp, LeaseIdentity};
        let daemon = causal_daemon();
        let runtime = daemon.runtime().unwrap();
        let mut state = crate::daemon::owner_loop::DaemonControlState::default();
        let (mut entry, scope) = retained_fanout(runtime, &mut state);
        for _ in 0..crate::runtime::CAUSAL_OWNER_CAPACITY {
            assert!(matches!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: u64::MAX,
                    identity: LeaseIdentity::EventInFlight,
                }),
                CausalAdmitResult::Applied
            ));
        }
        assert!(matches!(step(&daemon, &mut state, &mut entry), Step::Again));
        assert!(matches!(
            step(&daemon, &mut state, &mut entry),
            Step::Waiting
        ));
        assert!(matches!(entry.work.phase, Phase::Ready { .. }));
        assert!(matches!(
            entry.work.model_operation,
            Some(crate::runtime::entity_model::Operation::FinishFanout { .. })
        ));
        assert!(
            state
                .plugin_entities
                .model_waiters
                .contains(&entry.waiter_id)
        );
        assert_eq!(state.budget.outstanding(), 1);
        assert_reserved_slots(&runtime.host_executor(), 1);
        assert!(runtime.causal_scopes().is_live(scope));
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert!(matches!(
            step(&daemon, &mut state, &mut entry),
            Step::Waiting
        ));
        entry
            .work
            .retain_completion(receive(runtime.host_executor()))
            .unwrap();
        assert!(matches!(
            step(&daemon, &mut state, &mut entry),
            Step::Waiting
        ));
        assert!(runtime.causal_scopes().is_live(scope));
        assert_reserved_slots(runtime.host_executor(), 1);
        runtime
            .causal_scopes()
            .test_with_inner_held(|| runtime.apply_causal_owner_ops());
        assert!(matches!(
            step(&daemon, &mut state, &mut entry),
            Step::Waiting
        ));
        assert!(runtime.causal_scopes().is_live(scope));
        assert_reserved_slots(runtime.host_executor(), 1);
        runtime.apply_causal_owner_ops();
        assert!(matches!(step(&daemon, &mut state, &mut entry), Step::Done));
        assert!(entry.work.finish.is_none());
        assert!(
            !state
                .plugin_entities
                .causal_waiters
                .contains(&entry.waiter_id)
        );
        assert_reserved_slots(runtime.host_executor(), 0);
        assert!(!runtime.causal_scopes().is_live(scope));
        let super::super::PendingPluginEntityKind::Fanout { permit } = entry.kind else {
            unreachable!()
        };
        state.budget.release(permit);
        assert_eq!(state.budget.outstanding(), 0);
    }

    #[test]
    fn causal_fault_keeps_reclaimed_fanout_and_permits_after_cancellation() {
        let daemon = causal_daemon();
        let runtime = daemon.runtime().unwrap();
        let mut state = crate::daemon::owner_loop::DaemonControlState::default();
        let (mut entry, _scope) = retained_fanout(runtime, &mut state);
        let table = runtime.causal_scopes();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            table.test_with_inner_held(|| panic!("inject causal table fault"));
        }));
        assert!(matches!(step(&daemon, &mut state, &mut entry), Step::Again));
        assert!(matches!(
            step(&daemon, &mut state, &mut entry),
            Step::Waiting
        ));
        assert_eq!(entry.work.stage, Stage::CausalRecovery);
        assert!(
            state
                .plugin_entities
                .causal_faults
                .contains(&entry.waiter_id)
        );
        entry.work.cancel();
        for _ in 0..3 {
            assert!(matches!(
                step(&daemon, &mut state, &mut entry),
                Step::Waiting
            ));
            assert!(matches!(entry.work.phase, Phase::Ready { .. }));
            assert!(matches!(
                entry.work.model_operation,
                Some(crate::runtime::entity_model::Operation::FinishFanout { .. })
            ));
            assert_eq!(state.budget.outstanding(), 1);
            assert_reserved_slots(&runtime.host_executor(), 1);
        }
    }
}

pub(super) enum Step {
    Waiting,
    Again,
    Done,
}

fn submission_step(
    state: &mut crate::daemon::owner_loop::DaemonControlState,
    waiter: WaiterId,
    advance: Advance,
) -> Step {
    if matches!(advance, Advance::Capacity) {
        state.plugin_entities.capacity_waiters.insert(waiter);
    }
    Step::Waiting
}

fn remove_exact_subscription(
    state: &mut crate::daemon::owner_loop::DaemonControlState,
    work: &EntityWork,
) {
    if let Some(target) = &work.target
        && state
            .plugin_entities
            .targets
            .get(&target.subscription_id)
            .is_some_and(|current| Arc::ptr_eq(current, target))
    {
        super::remove_entity_subscription(state, &target.subscription_id);
    }
}

/// Perform one owner transition. The scheduler charges the transition before this call.
fn retain_causal_transition(
    state: &mut crate::daemon::owner_loop::DaemonControlState,
    entry: &mut super::PendingPluginEntity,
    status: crate::runtime::CausalTransitionStatus,
) -> Option<Step> {
    use crate::runtime::CausalTransitionStatus;
    match status {
        CausalTransitionStatus::Applied => {
            state
                .plugin_entities
                .causal_waiters
                .remove(&entry.waiter_id);
            None
        }
        CausalTransitionStatus::Waiting => {
            state.plugin_entities.causal_waiters.insert(entry.waiter_id);
            Some(Step::Waiting)
        }
        CausalTransitionStatus::Fault => {
            state
                .plugin_entities
                .causal_waiters
                .remove(&entry.waiter_id);
            state.plugin_entities.causal_faults.insert(entry.waiter_id);
            entry.work.stage = Stage::CausalRecovery;
            entry.work.error = Some((
                "causal_recovery_required",
                "causal transition storage requires daemon restart",
            ));
            if let super::PendingPluginEntityKind::Subscribe(subscribe) = &mut entry.kind {
                let target = entry
                    .work
                    .target
                    .as_ref()
                    .expect("subscribe retains its target");
                let _ = subscribe.request.reply_tx.take().send(Ok(
                    crate::subscription::entity::entity_subscription_error(
                        "causal_recovery_required",
                        &target.subscription_id,
                        "causal transition storage requires daemon restart",
                    ),
                ));
            }
            Some(Step::Waiting)
        }
    }
}

fn drive_model(
    runtime: &crate::HubRuntime,
    state: &mut crate::daemon::owner_loop::DaemonControlState,
    entry: &mut super::PendingPluginEntity,
) -> Option<Step> {
    use crate::runtime::CausalTransitionStatus;
    if entry.work.model_operation.is_some() {
        return Some(match entry.work.start_model(runtime) {
            Ok(advance) => {
                state.plugin_entities.model_waiters.remove(&entry.waiter_id);
                submission_step(state, entry.waiter_id, advance)
            }
            Err(CausalTransitionStatus::Waiting) => {
                state.plugin_entities.model_waiters.insert(entry.waiter_id);
                Step::Waiting
            }
            Err(status) => retain_causal_transition(state, entry, status).unwrap(),
        });
    }
    if entry.work.model.is_some() && matches!(entry.work.phase, Phase::Rejected(_)) {
        let advance = entry.work.retry(runtime.host_executor());
        return Some(submission_step(state, entry.waiter_id, advance));
    }
    let work = entry.work.model.as_ref()?.clone();
    let Phase::Completed(completion) = &entry.work.phase else {
        return Some(Step::Waiting);
    };
    let status = match completion.result {
        HostResult::EntityModelComplete(kind) => {
            runtime.observe_entity_model(completion.identity, kind)
        }
        _ => {
            runtime.fault_entity_model(completion.identity);
            CausalTransitionStatus::Fault
        }
    };
    if status != CausalTransitionStatus::Applied {
        return retain_causal_transition(state, entry, status);
    }
    let Some(next) = completion.identity.next_phase() else {
        runtime.fault_entity_model(completion.identity);
        return retain_causal_transition(state, entry, CausalTransitionStatus::Fault);
    };
    let mut output = runtime
        .entity_model_output(&work)
        .expect("exact observation permits output access");
    let purpose = entry
        .work
        .model_purpose
        .expect("the model phase retains its purpose");
    if matches!(purpose, ModelPurpose::SelectProvider) && entry.work.cancelled {
        entry.work.stage = Stage::AdmissionRetirement;
    } else if matches!(purpose, ModelPurpose::SelectProvider) {
        if entry.invocation.is_none() {
            let Some(crate::runtime::entity_model::Operation::SelectProvider {
                selected: Some(selected),
                ..
            }) = output.operation.as_ref()
            else {
                unreachable!("selection completed before output access");
            };
            let expected = &entry
                .work
                .provider_plan
                .as_ref()
                .expect("selection retains its plan")
                .expected;
            match runtime.selected_plugin_entity_snapshot(
                expected,
                selected.family_generation,
                selected.obligation.as_ref(),
            ) {
                Ok(invocation) => {
                    entry.work.family_generation = Some(invocation.family_generation);
                    entry.invocation = Some(invocation);
                }
                Err(error) => {
                    entry.work.provider_refusal = Some(error);
                    entry.work.stage = Stage::AdmissionRetirement;
                }
            }
        }
        if let Some(invocation) = entry.invocation.as_mut() {
            use crate::package_event_router::CausalAcquireResult;
            match runtime.try_acquire_plugin_entity_snapshot(invocation) {
                CausalAcquireResult::Acquired => entry.work.stage = Stage::AdmitProvider,
                CausalAcquireResult::Waiting => {
                    state.plugin_entities.causal_waiters.insert(entry.waiter_id);
                    return Some(Step::Waiting);
                }
                CausalAcquireResult::MissingScope => {
                    entry.work.provider_refusal = Some(crate::McpToolError::new(
                        "causal_scope_busy",
                        "could not acquire provider causal lease",
                    ));
                    entry.work.stage = Stage::AdmissionRetirement;
                }
                CausalAcquireResult::Fault => {
                    drop(output);
                    return retain_causal_transition(state, entry, CausalTransitionStatus::Fault);
                }
            }
        }
    }
    assert!(
        entry.work.model_mutation.is_none(),
        "the consumer has an empty mutation slot"
    );
    if matches!(purpose, ModelPurpose::TakeFanout | ModelPurpose::Snapshot) {
        assert!(
            entry.work.finish.is_none(),
            "the consumer has an empty finish slot"
        );
    }
    if matches!(purpose, ModelPurpose::CheckFamily) {
        entry.work.generation_checked = Some(output.valid);
        entry.work.resync_pending = false;
    }
    let begin_valid = matches!(purpose, ModelPurpose::BeginSnapshot) && output.valid;
    if matches!(purpose, ModelPurpose::BeginSnapshot) {
        if begin_valid {
            entry.work.floor = output
                .family_progress()
                .expect("snapshot begin returns progress")
                .floor;
        } else {
            entry.work.cancel();
            entry.work.error = Some((
                "entity_provider_stale",
                "the entity family changed during delivery",
            ));
            entry.work.stage = Stage::Finish;
        }
    }
    let item = output.take_mutation();
    let empty_fanout = matches!(purpose, ModelPurpose::TakeFanout) && item.is_none();
    if let Some((item, discarded)) = item {
        entry.work.family_generation = Some(item.generation);
        let (mutation, finish) = item.into_parts();
        entry.work.model_mutation = Some((mutation, discarded));
        entry.work.finish = Some(finish);
        if !discarded {
            entry.work.snapshot = false;
            entry.work.cursor = None;
            entry.work.stage = Stage::Deliver;
        }
        if matches!(purpose, ModelPurpose::TakeFanout) {
            state.lifecycle_counters.package_entity_publish_accepted = state
                .lifecycle_counters
                .package_entity_publish_accepted
                .saturating_add(1);
        }
    } else if matches!(purpose, ModelPurpose::Snapshot)
        && (!output.valid || output.snapshot_complete())
    {
        if !output.valid {
            entry.work.cancel();
            entry.work.error = Some((
                "entity_provider_stale",
                "the entity family changed during delivery",
            ));
        }
        entry.work.stage = Stage::Finish;
    }
    drop(output);
    assert!(
        runtime.release_entity_model(&work),
        "the exact table receipt permits model release"
    );
    assert!(
        work.owner_drop_ready(),
        "the Host cleared variable inputs before completion"
    );
    drop(entry.work.model.take());
    entry.work.model_purpose = None;
    state
        .plugin_entities
        .causal_waiters
        .remove(&entry.waiter_id);
    let Phase::Completed(completion) = std::mem::replace(&mut entry.work.phase, Phase::Terminal)
    else {
        unreachable!();
    };
    let (_, _, permit) = completion.into_parts();
    let done = empty_fanout
        || (matches!(purpose, ModelPurpose::Finish) && entry.work.stage == Stage::Release);
    if done {
        drop(permit);
    } else {
        entry.work.phase = Phase::Ready {
            identity: next,
            permit,
        };
    }
    if matches!(purpose, ModelPurpose::CheckFamily) {
        return None;
    }
    if begin_valid {
        return Some(register_snapshot(state, entry));
    }
    Some(if done { Step::Done } else { Step::Again })
}

/// A delivery owner already holds Host capacity for all of its remaining phases.
fn claim_delivery(
    state: &mut crate::daemon::owner_loop::DaemonControlState,
    waiter: WaiterId,
) -> bool {
    if state
        .plugin_entities
        .active_delivery
        .is_some_and(|active| active != waiter)
    {
        state.plugin_entities.delivery_waiters.insert(waiter);
        return false;
    }
    state.plugin_entities.active_delivery = Some(waiter);
    true
}

fn register_snapshot(
    state: &mut crate::daemon::owner_loop::DaemonControlState,
    entry: &mut super::PendingPluginEntity,
) -> Step {
    if let Some(registration) = entry.work.registration.take() {
        let target = entry
            .work
            .target
            .as_ref()
            .expect("registration has an initial target");
        let super::PendingPluginEntityKind::Subscribe(subscribe) = &entry.kind else {
            unreachable!()
        };
        match crate::subscription::entity::install_package_entity_subscription(
            state,
            registration,
            Arc::clone(target),
            subscribe.request.grant_id.clone(),
        ) {
            Ok(reservation) => {
                entry.work.reservation_identity = Some(reservation);
                entry.work.registered = true;
            }
            Err(registration) => {
                entry.work.registration = Some(registration);
                entry.work.error = Some((
                    "duplicate_entity_subscription",
                    "entity subscription id is already active",
                ));
                entry.work.stage = Stage::Finish;
                return Step::Again;
            }
        }
    }
    entry.work.stage = Stage::Deliver;
    Step::Again
}

pub(super) fn step(
    daemon: &crate::HubDaemon,
    state: &mut crate::daemon::owner_loop::DaemonControlState,
    entry: &mut super::PendingPluginEntity,
) -> Step {
    use crate::subscription::entity::{
        arm_package_entity_delivery, complete_package_entity_delivery,
        exact_package_entity_target_catching_up, next_package_entity_target,
    };
    let Some(runtime) = daemon.runtime() else {
        entry.work.cancel();
        return Step::Waiting;
    };
    let executor = runtime.host_executor();
    let waiter = entry.waiter_id;
    if entry.work.stage == Stage::CausalRecovery {
        return Step::Waiting;
    }
    if let Some(step) = drive_model(runtime, state, entry) {
        return step;
    }
    if let Some((mutation, discarded)) = entry.work.model_mutation.take() {
        let command = if discarded {
            Command::Reclaim(Payload::mutation(mutation))
        } else {
            Command::PrepareMutation(mutation)
        };
        return submission_step(state, waiter, entry.work.submit(executor, command));
    }
    if entry.work.stage == Stage::AdmissionRetirement {
        if let Some(invocation) = entry.invocation.as_ref() {
            let status = runtime.retire_plugin_entity_snapshot(invocation);
            if let Some(step) = retain_causal_transition(state, entry, status) {
                return step;
            }
        }
        entry.work.family_generation = None;
        let command = if entry.work.cancelled {
            entry.work.stage = Stage::Release;
            Command::DiscardProvider {
                plan: entry.work.provider_plan.take(),
                invocation: entry.invocation.take(),
                input: entry.work.provider_input.take(),
                refusal: entry.work.provider_refusal.take(),
            }
        } else {
            entry.work.stage = Stage::Begin;
            Command::RefuseProvider {
                plan: entry.work.provider_plan.take(),
                invocation: entry.invocation.take(),
                error: entry
                    .work
                    .provider_refusal
                    .take()
                    .expect("refusal retains its error"),
            }
        };
        return submission_step(state, waiter, entry.work.submit(executor, command));
    }
    if entry.work.deferred_completion.is_none()
        && matches!(&entry.work.phase, Phase::Completed(completion)
            if matches!(completion.result, HostResult::PluginEntity(Completion::Delivered { .. })))
    {
        entry.work.deferred_completion = entry.work.take_completion();
    }
    if !entry.work.cancelled
        && entry.work.generation_checked.is_none()
        && matches!(entry.work.phase, Phase::Ready { .. })
        && matches!(entry.work.stage, Stage::Deliver | Stage::Finish)
        && let (Some(family), Some(generation)) = (&entry.work.family, entry.work.family_generation)
    {
        entry.work.model_operation = Some(crate::runtime::entity_model::Operation::Family {
            name: Some(Arc::clone(family)),
            expected_generation: generation,
            action: if entry.work.resync_pending {
                crate::runtime::entity_model::FamilyAction::MarkResync
            } else {
                crate::runtime::entity_model::FamilyAction::Check
            },
            progress: None,
        });
        entry.work.model_purpose = Some(ModelPurpose::CheckFamily);
        return Step::Again;
    }
    let generation_is_current = entry.work.generation_checked.take().unwrap_or(true);
    if !generation_is_current {
        entry.work.cancel();
        entry.work.error = Some((
            "entity_provider_stale",
            "the entity family changed during delivery",
        ));
    }
    if let Some((identity, completion)) = entry
        .work
        .deferred_completion
        .take()
        .or_else(|| entry.work.take_completion())
    {
        match completion {
            Completion::ProviderPrepared(plan) => {
                entry.work.provider_plan = Some(plan);
                entry.work.stage = Stage::AcquireProvider;
            }
            Completion::ProviderAdmitted(refusal) => {
                entry.work.stage = if let Some(error) = refusal {
                    entry.work.provider_refusal = Some(error);
                    Stage::AdmissionRetirement
                } else {
                    Stage::Provider
                };
            }

            Completion::Prepared {
                payload,
                family,
                registration,
            } => {
                entry.work.payload = Some(payload);
                entry.work.family = Some(family);
                entry.work.registration = registration;
                entry.work.stage = if entry.work.snapshot {
                    Stage::Begin
                } else {
                    Stage::Deliver
                };
            }
            Completion::Delivered { payload, status } => {
                let target = entry
                    .work
                    .delivery_target
                    .take()
                    .expect("a delivery completion retains its target");
                let sequence = payload
                    .sequence()
                    .expect("only sequenced payloads are delivered");
                entry.work.publication = None;
                let catching_up = complete_package_entity_delivery(
                    state,
                    &target,
                    identity,
                    sequence,
                    entry.work.snapshot,
                    entry.work.floor,
                    status,
                );
                if catching_up && generation_is_current {
                    entry.work.resync_pending = true;
                    entry.work.catchup_seen = true;
                    if let Some(finish) = &mut entry.work.finish {
                        finish.scheduled_resync = true;
                    }
                }
                if entry.work.snapshot
                    && entry.work.target.is_some()
                    && status != crate::plugin_entity::DeliveryStatus::Sent
                {
                    entry.work.error = Some((
                        "entity_initial_delivery_failed",
                        "the initial entity snapshot could not be delivered",
                    ));
                    remove_exact_subscription(state, &entry.work);
                }
                entry.work.payload = Some(payload);
            }
            Completion::Reclaimed => {
                if let Some(finish) = entry.work.finish.take() {
                    entry.work.model_operation =
                        Some(crate::runtime::entity_model::Operation::FinishFanout {
                            finish,
                            retained: None,
                        });
                    entry.work.model_purpose = Some(ModelPurpose::Finish);
                    return Step::Again;
                }
                if entry.work.stage == Stage::Release {
                    remove_exact_subscription(state, &entry.work);
                    return Step::Done;
                }
            }
            Completion::Finished { sent } => {
                if !sent || entry.work.error.is_some() {
                    remove_exact_subscription(state, &entry.work);
                }
                return Step::Done;
            }
        }
        return Step::Again;
    }
    if entry.work.stage == Stage::Provider && entry.result.is_none() {
        return Step::Waiting;
    }
    match entry.work.reserve(executor, waiter) {
        Advance::Capacity => {
            state.plugin_entities.capacity_waiters.insert(waiter);
            return Step::Waiting;
        }
        Advance::Submitted | Advance::Degraded => return Step::Waiting,
        Advance::Waiting => {}
    }
    let Some(identity) = entry.work.ready_identity() else {
        return Step::Waiting;
    };
    if entry.work.cancelled && entry.work.stage != Stage::Provider {
        if entry.work.provider_input.is_some()
            || entry.work.provider_plan.is_some()
            || entry.invocation.is_some()
        {
            entry.work.stage = Stage::AdmissionRetirement;
            return Step::Again;
        }

        if generation_is_current
            && !entry.work.cancel_resync_recorded
            && state.plugin_entities.active_delivery == Some(waiter)
            && let (Some(family), Some(generation)) =
                (&entry.work.family, entry.work.family_generation)
        {
            entry.work.cancel_resync_recorded = true;
            entry.work.model_operation = Some(crate::runtime::entity_model::Operation::Family {
                name: Some(Arc::clone(family)),
                expected_generation: generation,
                action: crate::runtime::entity_model::FamilyAction::MarkResync,
                progress: None,
            });
            entry.work.model_purpose = Some(ModelPurpose::CheckFamily);
            if let Some(finish) = &mut entry.work.finish {
                finish.scheduled_resync = true;
            }
            return Step::Again;
        }
        remove_exact_subscription(state, &entry.work);
        entry.work.stage = Stage::Release;
        let command = Command::Discard {
            payload: entry.work.payload.take(),
            registration: entry.work.registration.take(),
            reservation_identity: entry.work.reservation_identity.take(),
        };
        return submission_step(state, waiter, entry.work.submit(executor, command));
    }
    match entry.work.stage {
        Stage::PrepareProvider => {
            let (lifecycle, _) = runtime.plugin_provider_admission();
            let command = Command::PrepareProvider {
                lifecycle,
                budget: runtime.shared_view_budget(),
                input: entry
                    .work
                    .provider_input
                    .take()
                    .expect("provider preparation retains its input"),
                request_id: botster_core::RequestId(entry.request_id.clone()),
            };
            submission_step(state, waiter, entry.work.submit(executor, command))
        }
        Stage::AcquireProvider => {
            let expected = entry
                .work
                .provider_plan
                .as_ref()
                .expect("provider selection retains its plan")
                .expected
                .clone();
            entry.work.model_operation =
                Some(crate::runtime::entity_model::Operation::SelectProvider {
                    expected: Some(expected),
                    selected: None,
                });
            entry.work.model_purpose = Some(ModelPurpose::SelectProvider);
            Step::Again
        }
        Stage::AdmitProvider => {
            let (lifecycle, force_backpressure) = runtime.plugin_provider_admission();
            let command = Command::AdmitProvider {
                lifecycle,
                plan: entry
                    .work
                    .provider_plan
                    .take()
                    .expect("provider admission retains its request"),
                scope_id: entry
                    .invocation
                    .as_ref()
                    .unwrap()
                    .causal_lease
                    .map(|(scope, _)| scope),
                force_backpressure,
            };
            submission_step(state, waiter, entry.work.submit(executor, command))
        }

        Stage::AdmissionRetirement | Stage::CausalRecovery => {
            unreachable!("retirement states return before Host work")
        }
        Stage::Provider => {
            let invocation = entry
                .invocation
                .as_ref()
                .expect("the provider invocation is retained");
            let status = runtime.retire_plugin_entity_snapshot(invocation);
            if let Some(step) = retain_causal_transition(state, entry, status) {
                return step;
            }
            let result = entry
                .result
                .take()
                .expect("the provider result was checked");
            let (result, inconsistent) = match result {
                super::RoutedPluginEntityCompletion::Invocation(result) => (result, false),
                super::RoutedPluginEntityCompletion::Inconsistent(result) => (result, true),
            };
            let invocation = entry
                .invocation
                .take()
                .expect("the provider invocation is retained");
            let command = Command::Prepare {
                invocation,
                result,
                inconsistent,
                target: entry.work.target.clone(),
            };
            submission_step(state, waiter, entry.work.submit(executor, command))
        }
        Stage::Begin => {
            if matches!(entry.kind, super::PendingPluginEntityKind::Fanout { .. }) {
                if !claim_delivery(state, waiter) {
                    return Step::Waiting;
                }
                entry.work.model_operation =
                    Some(crate::runtime::entity_model::Operation::TakeFanout { retained: None });
                entry.work.model_purpose = Some(ModelPurpose::TakeFanout);
                return Step::Again;
            }
            let Some(sequence) = entry.work.payload.as_ref().and_then(Payload::sequence) else {
                entry.work.stage = Stage::Finish;
                return Step::Again;
            };
            if !claim_delivery(state, waiter) {
                return Step::Waiting;
            }
            let family = entry
                .work
                .family
                .as_ref()
                .expect("a prepared snapshot has a family");
            let origin = match &entry.kind {
                super::PendingPluginEntityKind::Subscribe(_) => {
                    crate::runtime::entity_model::SnapshotOrigin::Subscribe
                }
                super::PendingPluginEntityKind::Resync { .. } => {
                    crate::runtime::entity_model::SnapshotOrigin::Resync
                }
                super::PendingPluginEntityKind::Fanout { .. } => {
                    unreachable!("fanout bypasses snapshot begin")
                }
                super::PendingPluginEntityKind::Disposing => unreachable!("disposed entity work"),
            };
            entry.work.model_operation = Some(crate::runtime::entity_model::Operation::Family {
                name: Some(Arc::clone(family)),
                expected_generation: entry
                    .work
                    .family_generation
                    .expect("snapshot begin retains its generation"),
                action: crate::runtime::entity_model::FamilyAction::BeginSnapshot {
                    sequence,
                    origin,
                },
                progress: None,
            });
            entry.work.model_purpose = Some(ModelPurpose::BeginSnapshot);
            Step::Again
        }
        Stage::Deliver => {
            let family = entry
                .work
                .family
                .as_ref()
                .expect("a prepared payload has a family");
            let target = if entry.work.snapshot && entry.work.target.is_some() {
                if entry.work.initial_target_visited {
                    None
                } else {
                    entry.work.initial_target_visited = true;
                    entry.work.target.clone()
                }
            } else {
                next_package_entity_target(state, entry.work.cursor.as_deref())
            };
            let Some(target) = target else {
                entry.work.stage =
                    if matches!(entry.kind, super::PendingPluginEntityKind::Fanout { .. }) {
                        Stage::Release
                    } else {
                        Stage::Drain
                    };
                let payload = entry
                    .work
                    .payload
                    .take()
                    .expect("delivery retains its payload");
                return submission_step(
                    state,
                    waiter,
                    entry.work.submit(executor, Command::Reclaim(payload)),
                );
            };
            entry.work.cursor = Some(Arc::clone(&target));
            if target.entity_type != **family {
                return Step::Again;
            }
            let sequence = entry
                .work
                .payload
                .as_ref()
                .and_then(Payload::sequence)
                .expect("only sequenced payloads enter delivery");
            let Some((publication, reason)) = arm_package_entity_delivery(
                state,
                &target,
                identity,
                sequence,
                entry.work.snapshot,
                entry.work.floor,
            ) else {
                if exact_package_entity_target_catching_up(state, &target) {
                    entry.work.resync_pending = true;
                    entry.work.catchup_seen = true;
                    if let Some(finish) = &mut entry.work.finish {
                        finish.scheduled_resync = true;
                    }
                }
                return Step::Again;
            };
            entry.work.delivery_target = Some(Arc::clone(&target));
            entry.work.publication = Some(Arc::clone(&publication));
            let command = Command::Deliver {
                payload: entry
                    .work
                    .payload
                    .take()
                    .expect("delivery retains its payload"),
                target,
                publication_live: publication,
                budget: runtime.shared_view_budget(),
                resync_reason: reason,
            };
            submission_step(state, waiter, entry.work.submit(executor, command))
        }
        Stage::Drain => {
            let family = entry
                .work
                .family
                .as_ref()
                .expect("snapshot drain retains its family");
            entry.work.model_operation =
                Some(crate::runtime::entity_model::Operation::StepSnapshot {
                    name: Some(Arc::clone(family)),
                    expected_generation: entry
                        .work
                        .family_generation
                        .expect("snapshot drain retains its generation"),
                    preserve_resync_need: matches!(
                        &entry.kind,
                        super::PendingPluginEntityKind::Subscribe(_)
                    ) || entry.work.catchup_seen,
                    generation: None,
                    retained: Default::default(),
                });
            entry.work.model_purpose = Some(ModelPurpose::Snapshot);
            Step::Again
        }
        Stage::Finish => {
            let command = match &mut entry.kind {
                super::PendingPluginEntityKind::Subscribe(subscribe) => {
                    let target = Arc::clone(
                        entry
                            .work
                            .target
                            .as_ref()
                            .expect("a subscribe operation retains its target"),
                    );
                    let reservation = if entry.work.error.is_none() && entry.work.registered {
                        super::prepare_package_entity_reservation(
                            state,
                            &target.subscription_id,
                            subscribe.request.frame_rx.take(),
                            subscribe.request.grant_id.as_deref(),
                            &mut entry.work.reservation_identity,
                            &mut entry.work.error,
                        )
                    } else {
                        None
                    };
                    Command::Finish {
                        payload: entry.work.payload.take(),
                        registration: entry.work.registration.take(),
                        reservation_identity: entry.work.reservation_identity.take(),
                        target,
                        reservation,
                        error: entry.work.error,
                        transport_request_id: std::mem::take(
                            &mut entry
                                .identity
                                .as_mut()
                                .expect("a subscribe operation has an identity")
                                .transport_request_id,
                        ),
                        reply_tx: subscribe.request.reply_tx.take(),
                        publication_live: Arc::clone(&entry.work.reply_live),
                    }
                }
                _ => {
                    entry.work.stage = Stage::Release;
                    Command::Discard {
                        payload: entry.work.payload.take(),
                        registration: entry.work.registration.take(),
                        reservation_identity: entry.work.reservation_identity.take(),
                    }
                }
            };
            submission_step(state, waiter, entry.work.submit(executor, command))
        }
        Stage::Release => Step::Waiting,
    }
}
