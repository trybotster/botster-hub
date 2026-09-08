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
    pub(super) stage: Stage,
    phase: Phase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
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
                if catching_up {
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
                if let Some(finish) = entry.work.finish.take() {
                    runtime.finish_package_entity_fanout(finish);
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
        if state.plugin_entities.active_delivery == Some(waiter)
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
        Stage::Provider => {
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
            runtime.retire_plugin_entity_snapshot(&invocation);
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
                let Some(item) = runtime.take_one_package_entity_fanout() else {
                    return Step::Done;
                };
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
            if state
                .plugin_entities
                .active_delivery
                .is_some_and(|active| active != waiter)
            {
                state.plugin_entities.delivery_waiters.insert(waiter);
                return Step::Waiting;
            }
            state.plugin_entities.active_delivery = Some(waiter);
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
