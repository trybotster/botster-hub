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
    pub(super) admission_refusal: Option<(&'static str, String)>,
    pub(super) stage: Stage,
    phase: Phase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
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
    Unreserved,
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
    Terminal,
}

pub(super) enum Advance {
    Waiting,
    Capacity,
    Submitted,
    Degraded,
}

impl EntityWork {
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
            admission_refusal: None,
            stage: Stage::Provider,
            phase: Phase::Unreserved,
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
        if matches!(self.phase, Phase::Unreserved) {
            let Some(permit) = executor.try_reserve() else {
                return Advance::Capacity;
            };
            self.phase = Phase::Ready {
                identity: HostJobIdentity::first(waiter),
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
        let terminal = matches!(
            completion.result,
            HostResult::PluginEntity(Completion::Finished { .. })
        ) || self.stage == Stage::Release;
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
        if !terminal {
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
                    HostSubmitError::Stopped | HostSubmitError::PhaseExhausted => Advance::Degraded,
                };
                self.phase = Phase::Rejected(failure);
                advance
            }
        }
    }

    fn retry(&mut self, executor: &HostExecutor) -> Advance {
        match &self.phase {
            Phase::Rejected(failure) if matches!(failure.error, HostSubmitError::Full) => {}
            Phase::Rejected(_) | Phase::Exhausted { .. } => return Advance::Degraded,
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
    fn reclaimed_fanout_keeps_completion_and_permits_until_causal_capacity_returns() {
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
        assert!(matches!(
            step(&daemon, &mut state, &mut entry),
            Step::Waiting
        ));
        assert!(matches!(entry.work.phase, Phase::Completed(_)));
        assert!(entry.work.finish.is_some());
        assert!(
            state
                .plugin_entities
                .causal_waiters
                .contains(&entry.waiter_id)
        );
        assert_eq!(state.budget.outstanding(), 1);
        assert_reserved_slots(&runtime.host_executor(), 1);
        assert!(runtime.causal_scopes().is_live(scope));
        runtime.apply_causal_owner_ops();
        assert!(matches!(step(&daemon, &mut state, &mut entry), Step::Done));
        assert!(entry.work.finish.is_none());
        assert!(
            !state
                .plugin_entities
                .causal_waiters
                .contains(&entry.waiter_id)
        );
        assert_reserved_slots(&runtime.host_executor(), 0);
        assert!(
            runtime.causal_scopes().is_live(scope),
            "the FIFO owns the release before table application"
        );
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
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
            assert!(matches!(entry.work.phase, Phase::Completed(_)));
            assert!(entry.work.finish.is_some());
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

pub(super) fn step(
    daemon: &crate::HubDaemon,
    state: &mut crate::daemon::owner_loop::DaemonControlState,
    entry: &mut super::PendingPluginEntity,
) -> Step {
    use crate::subscription::entity::{
        arm_package_entity_delivery, complete_package_entity_delivery,
        install_package_entity_subscription, next_package_entity_target,
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
    if entry.work.stage == Stage::AdmissionRetirement {
        let invocation = entry
            .invocation
            .as_ref()
            .expect("refused admission retains its invocation");
        let status = runtime.retire_plugin_entity_snapshot(invocation);
        if let Some(step) = retain_causal_transition(state, entry, status) {
            return step;
        }
        entry.invocation = None;
        if let Some((code, message)) = entry.work.admission_refusal.take()
            && let super::PendingPluginEntityKind::Subscribe(subscribe) = &mut entry.kind
        {
            let target = entry
                .work
                .target
                .as_ref()
                .expect("subscribe retains its target");
            let _ = subscribe.request.reply_tx.take().send(Ok(
                crate::subscription::entity::entity_subscription_error(
                    code,
                    &target.subscription_id,
                    &message,
                ),
            ));
        }
        return Step::Done;
    }
    if matches!(&entry.work.phase, Phase::Completed(completion) if matches!(&completion.result, HostResult::PluginEntity(Completion::Reclaimed)))
        && let Some(finish) = entry.work.finish.as_ref()
    {
        let status = runtime.finish_package_entity_fanout(finish);
        if let Some(step) = retain_causal_transition(state, entry, status) {
            return step;
        }
        entry.work.finish = None;
    }
    let generation_is_current = match (&entry.work.family, entry.work.family_generation) {
        (Some(family), Some(generation)) => {
            runtime.package_entity_family_generation(family) == Some(generation)
        }
        _ => true,
    };
    if !generation_is_current {
        entry.work.cancel();
        entry.work.error = Some((
            "entity_provider_stale",
            "the entity family changed during delivery",
        ));
    }
    if let Some((identity, completion)) = entry.work.take_completion() {
        match completion {
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
                    runtime.mark_package_entity_resync_needed(&target.entity_type);
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
        if generation_is_current
            && state.plugin_entities.active_delivery == Some(waiter)
            && let Some(family) = &entry.work.family
        {
            runtime.mark_package_entity_resync_needed(family);
            if let Some(finish) = &mut entry.work.finish {
                finish.scheduled_resync = true;
            }
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
                let Some(item) = runtime.take_one_package_entity_fanout() else {
                    return Step::Done;
                };
                entry.work.family_generation = Some(item.generation);
                let (mutation, finish) = item.into_parts();
                entry.work.finish = Some(finish);
                entry.work.stage = Stage::Deliver;
                state.lifecycle_counters.package_entity_publish_accepted = state
                    .lifecycle_counters
                    .package_entity_publish_accepted
                    .saturating_add(1);
                return submission_step(
                    state,
                    waiter,
                    entry
                        .work
                        .submit(executor, Command::PrepareMutation(mutation)),
                );
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
            entry.work.floor = runtime
                .begin_package_entity_provider_snapshot(family, sequence)
                .floor;
            if sequence < entry.work.floor {
                runtime.rearm_package_entity_resync(family);
            }
            if let Some(registration) = entry.work.registration.take() {
                let target = entry
                    .work
                    .target
                    .as_ref()
                    .expect("registration has an initial target");
                let super::PendingPluginEntityKind::Subscribe(subscribe) = &entry.kind else {
                    unreachable!()
                };
                match install_package_entity_subscription(
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
                if state
                    .entity_subscriptions
                    .get(&target.subscription_id)
                    .is_some_and(|subscription| subscription.package_catching_up)
                {
                    runtime.mark_package_entity_resync_needed(family);
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
            match runtime.step_package_entity_provider_snapshot(family) {
                crate::runtime::PackageEntitySnapshotStep::Discarded(item) => {
                    let (mutation, finish) = item.into_parts();
                    entry.work.finish = Some(finish);
                    submission_step(
                        state,
                        waiter,
                        entry
                            .work
                            .submit(executor, Command::Reclaim(Payload::mutation(mutation))),
                    )
                }
                crate::runtime::PackageEntitySnapshotStep::Ready(item) => {
                    let (mutation, finish) = item.into_parts();
                    entry.work.finish = Some(finish);
                    entry.work.snapshot = false;
                    entry.work.cursor = None;
                    submission_step(
                        state,
                        waiter,
                        entry
                            .work
                            .submit(executor, Command::PrepareMutation(mutation)),
                    )
                }
                crate::runtime::PackageEntitySnapshotStep::Waiting => retain_causal_transition(
                    state,
                    entry,
                    crate::runtime::CausalTransitionStatus::Waiting,
                )
                .expect("capacity wait retains the entry"),
                crate::runtime::PackageEntitySnapshotStep::Fault => retain_causal_transition(
                    state,
                    entry,
                    crate::runtime::CausalTransitionStatus::Fault,
                )
                .expect("fault retains the entry"),
                crate::runtime::PackageEntitySnapshotStep::Pending => Step::Again,
                crate::runtime::PackageEntitySnapshotStep::Complete(_) => {
                    entry.work.stage = Stage::Finish;
                    Step::Again
                }
            }
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
