//! Shared records retain model inputs and extracted values across Host failures.

use std::cell::{Cell, RefCell};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::host_executor::{HostCommand, HostJobIdentity, HostRetainedPrepared, HostWorkPermit};
use crate::package_entity_fanout::{FamilySnapshotWork, LeasedFanoutMutation};
use crate::package_event_router::{CausalOp, LeaseIdentity};

use super::causal::{CausalReceipt, CausalReservation, CausalTransitionStatus};
use super::{PackageEntities, PackageEntityFamilyStep, PackageEntityFanoutFinish};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    AdmitPublication,
    AdvancePublication,
    DisposePublication,
    ReleasePublication,
    ReplyPublication,
    Resync,
    CleanupFamilies,
    CleanupDetached,
    CheckFamily,
    MarkResync,
    BeginSnapshot,
    SelectProvider,
    TakeFanout,
    StepSnapshot,
    FinishFanout,
    Reclaim,
}

pub(crate) enum PublicationSelection {
    Selected,
    Empty,
    Waiting,
    Fault,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_executor::{HostCommand, HostCompletion, HostCompletionPoll, HostResult};
    use crate::owner_identity::WaiterId;
    use crate::package_entity_fanout::{EntityMutationLease, PackageEntityMutation};
    use std::time::{Duration, Instant};

    fn identity() -> HostJobIdentity {
        HostJobIdentity::first(WaiterId(31))
    }

    fn completion(runtime: &super::super::HubRuntime) -> HostCompletion {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match runtime.host_executor().poll_completion() {
                HostCompletionPoll::Ready(completion) => return completion,
                HostCompletionPoll::Empty => std::thread::yield_now(),
                HostCompletionPoll::Stopped => panic!("the Host executor stopped"),
            }
            assert!(Instant::now() < deadline);
        }
    }

    fn charged_mutation() -> (
        crate::lua_runtime::HubEntityPublishBridge,
        PackageEntityMutation,
    ) {
        let bridge = crate::lua_runtime::HubEntityPublishBridge::for_test("p", "p.item");
        let _response = bridge.test_queue_publish(
            botster_core::PluginKey("p".into()),
            serde_json::json!({
                "type": "entity_upsert", "entity_type": "p.item", "snapshot_seq": 3,
                "id": "item-3", "entity": {"id": "item-3", "payload": "retained"}
            }),
            Some(17),
        );
        let (pending, ()) = bridge.take_if(|_| Some(())).unwrap();
        (bridge, pending.mutation)
    }

    #[test]
    fn host_failure_retains_extracted_payload_credit_and_model_reservation() {
        let runtime = super::super::tests::family_runtime("host-model-extraction-failure");
        let (bridge, mutation) = charged_mutation();
        let charge = bridge.retained_counts();
        runtime
            .package_entities
            .lock()
            .unwrap()
            .fanout
            .try_push(LeasedFanoutMutation {
                lease: Some(EntityMutationLease {
                    scope_id: 17,
                    family_token: 23,
                    family: "p.item".into(),
                    generation: 7,
                    seq: 3,
                    admission: mutation.admission().cloned(),
                }),
                mutation,
                generation: 7,
            })
            .unwrap();
        runtime
            .entity_model_owner
            .update_readiness(&runtime.package_entities.lock().unwrap());
        assert!(runtime.entity_model_readiness().fanout);
        assert!(!runtime.take_entity_model_notification());
        let permit = runtime.host_executor().try_reserve().unwrap();
        let work = runtime
            .begin_entity_model(
                identity(),
                Operation::TakeFanout { retained: None },
                &permit,
            )
            .unwrap_or_else(|_| panic!("model capacity is available"));
        work.0.state.lock().unwrap().fail_after_operation = true;
        runtime
            .host_executor()
            .submit(identity(), HostCommand::EntityModel(work.clone()), permit)
            .unwrap();
        let completed = completion(&runtime);
        assert!(matches!(completed.result, HostResult::Failed { .. }));
        assert!(work.completed(identity(), Kind::TakeFanout).is_none());
        assert_eq!(
            runtime.observe_entity_model(identity(), Kind::TakeFanout),
            CausalTransitionStatus::Fault
        );
        assert!(!runtime.release_entity_model(&work));
        assert!(!runtime.entity_model_available());
        assert!(runtime.entity_model_readiness().fanout);
        assert!(!runtime.take_entity_model_notification());
        assert_eq!(bridge.retained_counts(), charge);
        // Failure inspection is confined to this test. Production never opens poison.
        let state = work
            .0
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let Some(Operation::TakeFanout {
            retained: Some(item),
        }) = state.operation.as_ref()
        else {
            panic!("the original mutation remains in the work record");
        };
        let lease = item.lease.as_ref().unwrap();
        assert_eq!(
            (
                lease.scope_id,
                lease.family_token,
                lease.seq,
                item.generation
            ),
            (17, 23, 3, 7)
        );
        assert_eq!(lease.admission.as_ref(), item.mutation.admission());
        let PackageEntityMutation::Upsert { entity, .. } = &item.mutation else {
            panic!("the original mutation is an upsert");
        };
        assert_eq!(entity["payload"], "retained");
        drop(state);
        drop(work);
        assert_eq!(bridge.retained_counts(), charge);
        assert!(runtime.entity_model_owner.active.borrow().is_some());
        drop(completed);
        let other_permits: Vec<_> = (0..7)
            .map(|_| runtime.host_executor().try_reserve().unwrap())
            .collect();
        assert!(runtime.host_executor().try_reserve().is_none());
        drop(other_permits);
    }

    #[test]
    fn exact_host_completion_keeps_model_reserved_until_table_application() {
        for scheduled_resync in [false, true] {
            let runtime = super::super::tests::family_runtime("host-model-table-receipt");
            if scheduled_resync {
                runtime.with_direct_entity_model(|model| {
                    let family = model.family("p.item");
                    family.causal_token = Some(23);
                    family.generation = 7;
                });
            }
            assert!(!runtime.entity_model_readiness().resync);
            assert!(!runtime.take_entity_model_notification());
            let lease_identity = LeaseIdentity::AdmittedEntityMutation {
                family_token: 23,
                seq: 3,
            };
            let scope = runtime
                .causal_scopes()
                .mint_with_lease(Some(lease_identity))
                .unwrap();
            let permit = runtime.host_executor().try_reserve().unwrap();
            let work = runtime
                .begin_entity_model(
                    identity(),
                    Operation::FinishFanout {
                        finish: PackageEntityFanoutFinish {
                            lease: Some(EntityMutationLease {
                                scope_id: scope,
                                family_token: 23,
                                family: "p.item".into(),
                                generation: 7,
                                seq: 3,
                                admission: None,
                            }),
                            scheduled_resync,
                        },
                        retained: None,
                    },
                    &permit,
                )
                .unwrap_or_else(|_| panic!("model capacity is available"));
            runtime
                .host_executor()
                .submit(identity(), HostCommand::EntityModel(work.clone()), permit)
                .unwrap();
            let completed = completion(&runtime);
            assert!(!runtime.entity_model_readiness().resync);
            assert!(!runtime.take_entity_model_notification());
            assert!(matches!(
                completed.result,
                HostResult::EntityModelComplete(Kind::FinishFanout)
            ));
            assert_eq!(
                runtime.observe_entity_model(identity().next_phase().unwrap(), Kind::FinishFanout),
                CausalTransitionStatus::Fault
            );
            assert_eq!(runtime.causal_operation_count(), 0);
            assert!(work.prepare_reclaim().is_none());
            assert!(!runtime.release_entity_model(&work));
            assert_eq!(
                runtime.observe_entity_model(identity(), Kind::FinishFanout),
                CausalTransitionStatus::Waiting
            );
            runtime
                .causal_scopes()
                .test_with_inner_held(|| runtime.apply_causal_owner_ops());
            assert!(!runtime.release_entity_model(&work));
            assert!(!runtime.entity_model_available());
            assert!(work.prepare_reclaim().is_none());
            assert_eq!(
                runtime.observe_entity_model(identity(), Kind::FinishFanout),
                CausalTransitionStatus::Waiting
            );
            assert_eq!(runtime.causal_operation_count(), 1);
            assert!(!runtime.entity_model_readiness().resync);
            assert!(!runtime.take_entity_model_notification());
            runtime.apply_causal_owner_ops();
            assert_eq!(runtime.causal_scopes().is_live(scope), scheduled_resync);
            assert!(!runtime.entity_model_readiness().resync);
            assert!(!runtime.take_entity_model_notification());
            assert_eq!(
                runtime.observe_entity_model(identity(), Kind::FinishFanout),
                CausalTransitionStatus::Applied
            );
            assert!(!runtime.entity_model_readiness().resync);
            assert!(!runtime.take_entity_model_notification());
            assert!(runtime.release_entity_model(&work));
            assert!(runtime.entity_model_available());
            assert_eq!(runtime.entity_model_readiness().resync, scheduled_resync);
            assert!(runtime.take_entity_model_notification());
            assert!(!runtime.take_entity_model_notification());
            let (_, _, permit) = completed.into_parts();
            let (next, command) = work.prepare_reclaim().unwrap();
            assert!(work.prepare_reclaim().is_none());
            runtime
                .host_executor()
                .submit(next, command, permit)
                .unwrap();
            let completed = completion(&runtime);
            assert_eq!(completed.identity, next);
            assert!(matches!(
                completed.result,
                HostResult::EntityModelComplete(Kind::Reclaim)
            ));
            assert!(
                work.completed(next, Kind::Reclaim)
                    .unwrap()
                    .operation
                    .is_none()
            );
            assert!(work.completed(identity(), Kind::FinishFanout).is_none());
            assert!(work.prepare_reclaim().is_none());
        }
    }

    #[test]
    fn detached_cleanup_progresses_with_the_live_model_held_and_retains_failed_payloads() {
        use super::super::family_cleanup::CleanupPhase;
        use crate::package_event_router::CausalAcquireResult;
        for fail in [false, true] {
            let runtime = super::super::tests::family_runtime("detached-cleanup");
            let (bridge, mutation) = charged_mutation();
            let charge = bridge.retained_counts();
            let scope = runtime.causal_scopes().mint().unwrap();
            let token = runtime.test_family_causal_token("p.item");
            assert_eq!(
                runtime.causal_scopes().try_acquire_with_admission_or_wait(
                    scope,
                    &LeaseIdentity::AdmittedEntityMutation {
                        family_token: token,
                        seq: 3
                    },
                    mutation.admission(),
                ),
                CausalAcquireResult::Acquired
            );
            runtime.test_store_pending_lease(scope, "p.item", 3);
            runtime.test_store_family_payload(mutation);
            let mut cleanup = super::super::HostPackageCleanup {
                unloaded_families: vec![(
                    "p".into(),
                    std::collections::BTreeSet::from(["p.item".into()]),
                )],
                ..Default::default()
            };
            let mut permit = runtime.host_executor().try_reserve().unwrap();
            let mut id = identity();
            // The real Host first advances the epoch and detaches the old family.
            for _ in 0..2 {
                let work = runtime
                    .begin_entity_model(
                        id,
                        Operation::Cleanup {
                            detached: false,
                            retained: Some(cleanup),
                            next: None,
                            fault: None,
                        },
                        &permit,
                    )
                    .unwrap_or_else(|_| panic!("the model grant is available"));
                runtime
                    .host_executor()
                    .submit(id, HostCommand::EntityModel(work.clone()), permit)
                    .unwrap();
                let completed = completion(&runtime);
                assert_eq!(
                    runtime.observe_entity_model(completed.identity, Kind::CleanupFamilies),
                    CausalTransitionStatus::Applied
                );
                cleanup = runtime
                    .entity_model_output(&work)
                    .unwrap()
                    .take_cleanup()
                    .unwrap();
                assert!(runtime.release_entity_model(&work));
                assert!(work.owner_drop_ready());
                drop(work);
                permit = completed.into_parts().2;
                id = id.next_phase().unwrap();
            }
            assert_eq!(cleanup.entity_cleanup_phase(), CleanupPhase::Detached);
            let live_permit = runtime.host_executor().try_reserve().unwrap();
            let live_id = HostJobIdentity::first(WaiterId(99));
            let live = runtime
                .begin_entity_model(
                    live_id,
                    Operation::TakeFanout { retained: None },
                    &live_permit,
                )
                .unwrap_or_else(|_| panic!("detached cleanup released the live model"));
            let others = (2..crate::host_executor::HOST_OPERATION_CAPACITY)
                .map(|_| runtime.host_executor().try_reserve().unwrap())
                .collect::<Vec<_>>();
            let model_guard = runtime.package_entities.lock().unwrap();
            let mut released = false;
            loop {
                let mut detached = runtime
                    .begin_detached_entity_cleanup(
                        id,
                        Operation::Cleanup {
                            detached: true,
                            retained: Some(cleanup),
                            next: None,
                            fault: None,
                        },
                        &permit,
                    )
                    .unwrap_or_else(|_| panic!("detached cleanup needs no live model grant"));
                if fail {
                    detached.work().0.state.lock().unwrap().fail_after_operation = true;
                }
                runtime
                    .host_executor()
                    .submit(
                        id,
                        HostCommand::EntityModel(detached.work().clone()),
                        permit,
                    )
                    .unwrap();
                let completed = completion(&runtime);
                assert_eq!(
                    runtime.host_executor().outstanding(),
                    crate::host_executor::HOST_OPERATION_CAPACITY
                );
                if fail {
                    assert!(matches!(completed.result, HostResult::Failed { .. }));
                    assert_eq!(
                        detached.observe(&runtime, completed.identity, Kind::CleanupDetached),
                        CausalTransitionStatus::Fault
                    );
                    assert!(detached.output().is_none());
                    assert!(!detached.release());
                    assert_eq!(bridge.retained_counts(), charge);
                    assert!(runtime.causal_scopes().is_live(scope));
                    break;
                }
                let status = detached.observe(&runtime, completed.identity, Kind::CleanupDetached);
                if status == CausalTransitionStatus::Waiting {
                    runtime.causal_scopes().test_with_inner_held(|| {
                        runtime.apply_causal_owner_ops();
                        assert!(detached.output().is_none());
                        assert!(!detached.release());
                        assert_eq!(bridge.retained_counts(), charge);
                    });
                    runtime.apply_causal_owner_ops();
                    assert_eq!(
                        detached.observe(&runtime, completed.identity, Kind::CleanupDetached),
                        CausalTransitionStatus::Applied
                    );
                    assert!(!runtime.causal_scopes().is_live(scope));
                    released = true;
                } else {
                    assert_eq!(status, CausalTransitionStatus::Applied);
                }
                cleanup = detached.output().unwrap().take_cleanup().unwrap();
                assert!(detached.release());
                assert!(detached.work().owner_drop_ready());
                drop(detached);
                permit = completed.into_parts().2;
                id = id.next_phase().unwrap();
                if cleanup.entity_cleanup_phase() != CleanupPhase::Detached {
                    assert!(released);
                    assert_eq!(bridge.retained_counts(), (0, 0));
                    break;
                }
            }
            drop(model_guard);
            assert!(!runtime.entity_model_available());
            drop(live);
            drop(live_permit);
            drop(others);
        }
    }

    #[test]
    fn wrong_kind_faults_the_active_model_even_if_a_correct_completion_follows() {
        let runtime = super::super::tests::family_runtime("host-model-wrong-kind");
        let permit = runtime.host_executor().try_reserve().unwrap();
        let work = runtime
            .begin_entity_model(
                identity(),
                Operation::TakeFanout { retained: None },
                &permit,
            )
            .unwrap_or_else(|_| panic!("model capacity is available"));
        runtime
            .host_executor()
            .submit(identity(), HostCommand::EntityModel(work.clone()), permit)
            .unwrap();
        let completed = completion(&runtime);
        assert_eq!(
            runtime.observe_entity_model(identity(), Kind::FinishFanout),
            CausalTransitionStatus::Fault
        );
        assert_eq!(
            runtime.observe_entity_model(identity(), Kind::TakeFanout),
            CausalTransitionStatus::Fault
        );
        assert!(!runtime.release_entity_model(&work));
        assert!(!runtime.entity_model_available());
        assert!(work.prepare_reclaim().is_none());
        drop(completed);
    }
}

/// The worker computes readiness while it owns the model reservation.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Readiness {
    pub(crate) fanout: bool,
    pub(crate) resync: bool,
    pub(crate) releases: bool,
    pub(crate) publication: super::publication::Next,
}

impl Readiness {
    fn read(model: &PackageEntities) -> Self {
        Self {
            fanout: !model.fanout.is_empty(),
            resync: model
                .families
                .values()
                .any(|family| family.resync.needed && !family.resync.degraded),
            releases: !model.resync_releases.is_empty(),
            publication: model.publication_next(),
        }
    }
}

pub(crate) enum Operation {
    AdmitPublication(super::publication::Admission),
    AdvancePublication(super::publication::Advance),
    DisposePublication(Option<super::PackageEntityMutation>),
    ReleasePublication,
    ReplyPublication(super::publication::Reply),
    Resync {
        action: super::resync::Action,
        retained: Option<super::resync::Cursor>,
    },
    Cleanup {
        detached: bool,
        retained: Option<super::HostPackageCleanup>,
        next: Option<super::family_cleanup::CleanupPhase>,
        fault: Option<super::PackageEntityCleanupError>,
    },
    Family {
        name: Option<Arc<String>>,
        expected_generation: u64,
        action: FamilyAction,
        progress: Option<super::PackageEntityFamilyProgress>,
    },
    SelectProvider {
        expected: Option<crate::shared_view::SharedView<super::provider::ProviderExpectation>>,
        selected: Option<ProviderSelection>,
    },
    TakeFanout {
        retained: Option<LeasedFanoutMutation>,
    },
    StepSnapshot {
        name: Option<Arc<String>>,
        expected_generation: u64,
        generation: Option<u64>,
        retained: FamilySnapshotWork,
    },
    FinishFanout {
        finish: PackageEntityFanoutFinish,
        retained: Option<(CausalOp, bool)>,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum FamilyAction {
    Check,
    MarkResync,
    BeginSnapshot(u64),
}

pub(crate) struct ProviderSelection {
    pub(crate) family_generation: u64,
    pub(crate) obligation: Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,
}

impl Operation {
    fn input_bytes(&self) -> usize {
        let metadata = match self {
            Self::AdmitPublication(_)
            | Self::AdvancePublication(_)
            | Self::DisposePublication(_)
            | Self::ReleasePublication
            | Self::ReplyPublication(_)
            | Self::Cleanup { .. } => 0,
            Self::Resync { retained, .. } => {
                retained.as_ref().map_or(0, |cursor| cursor.input_bytes())
            }
            Self::Family { name, .. } => name.as_ref().map_or(0, |name| name.len()),
            Self::SelectProvider { .. } => 0,
            Self::TakeFanout { .. } => 0,
            Self::StepSnapshot { name, .. } => name.as_ref().map_or(0, |name| name.len()),
            Self::FinishFanout { finish, .. } => {
                finish.lease.as_ref().map_or(0, |lease| lease.family.len())
            }
        };
        std::mem::size_of::<Shared>().saturating_add(metadata)
    }

    fn kind(&self) -> Kind {
        match self {
            Self::AdmitPublication(_) => Kind::AdmitPublication,
            Self::AdvancePublication(_) => Kind::AdvancePublication,
            Self::DisposePublication(_) => Kind::DisposePublication,
            Self::ReleasePublication => Kind::ReleasePublication,
            Self::ReplyPublication(_) => Kind::ReplyPublication,
            Self::Resync { .. } => Kind::Resync,
            Self::Cleanup { detached, .. } => {
                if *detached {
                    Kind::CleanupDetached
                } else {
                    Kind::CleanupFamilies
                }
            }
            Self::Family { action, .. } => match action {
                FamilyAction::Check => Kind::CheckFamily,
                FamilyAction::MarkResync => Kind::MarkResync,
                FamilyAction::BeginSnapshot(_) => Kind::BeginSnapshot,
            },
            Self::SelectProvider { .. } => Kind::SelectProvider,
            Self::TakeFanout { .. } => Kind::TakeFanout,
            Self::StepSnapshot { .. } => Kind::StepSnapshot,
            Self::FinishFanout { .. } => Kind::FinishFanout,
        }
    }
}

pub(crate) struct State {
    pub(crate) operation: Option<Operation>,
    pub(crate) causal: Option<CausalOp>,
    pub(crate) readiness: Readiness,
    pub(crate) valid: bool,
    resync_changed: bool,
    phase: Phase,
    #[cfg(test)]
    fail_after_operation: bool,
}

impl State {
    pub(crate) fn take_resync_cursor(&mut self) -> Option<super::resync::Cursor> {
        match self.operation.as_mut() {
            Some(Operation::Resync { retained, .. }) => retained.take(),
            _ => None,
        }
    }

    pub(crate) fn take_cleanup(&mut self) -> Option<super::HostPackageCleanup> {
        match self.operation.as_mut() {
            Some(Operation::Cleanup { retained, .. }) => retained.take(),
            _ => None,
        }
    }

    pub(crate) fn take_discarded_publication(&mut self) -> Option<super::PackageEntityMutation> {
        match self.operation.as_mut() {
            Some(Operation::AdmitPublication(work)) => work.retained.admission.discarded.take(),
            _ => None,
        }
    }

    pub(crate) fn take_mutation(&mut self) -> Option<(super::TakenPackageEntityMutation, bool)> {
        match self.operation.as_mut()? {
            Operation::TakeFanout { retained } => retained.take().map(|item| {
                (
                    super::TakenPackageEntityMutation {
                        generation: item.generation,
                        mutation: item.mutation,
                        finish: PackageEntityFanoutFinish {
                            lease: item.lease,
                            scheduled_resync: false,
                        },
                    },
                    false,
                )
            }),
            Operation::StepSnapshot {
                generation,
                retained,
                ..
            } => {
                let generation = (*generation)?;
                let discarded = matches!(
                    retained.step,
                    Some(PackageEntityFamilyStep::Discarded { .. })
                );
                if !discarded
                    && !matches!(retained.step, Some(PackageEntityFamilyStep::Ready { .. }))
                {
                    return None;
                }
                let mutation = match retained.step.take().expect("the mutation was checked") {
                    PackageEntityFamilyStep::Ready { mutation, lease }
                    | PackageEntityFamilyStep::Discarded { mutation, lease } => {
                        super::TakenPackageEntityMutation {
                            generation,
                            mutation,
                            finish: PackageEntityFanoutFinish {
                                lease,
                                scheduled_resync: false,
                            },
                        }
                    }
                    _ => unreachable!(),
                };
                Some((mutation, discarded))
            }
            Operation::FinishFanout { .. }
            | Operation::SelectProvider { .. }
            | Operation::Family { .. }
            | Operation::AdmitPublication(_)
            | Operation::AdvancePublication(_)
            | Operation::DisposePublication(_)
            | Operation::ReleasePublication
            | Operation::ReplyPublication(_)
            | Operation::Cleanup { .. }
            | Operation::Resync { .. } => None,
        }
    }

    pub(crate) fn snapshot_complete(&self) -> bool {
        matches!(
            self.operation.as_ref(),
            Some(Operation::StepSnapshot {
                retained: FamilySnapshotWork {
                    step: Some(PackageEntityFamilyStep::Complete(_)),
                    ..
                },
                ..
            })
        )
    }

    pub(crate) fn family_progress(&self) -> Option<super::PackageEntityFamilyProgress> {
        match self.operation.as_ref() {
            Some(Operation::Family { progress, .. }) => *progress,
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Pending,
    Completed,
    Released,
    ReclaimReady(HostJobIdentity),
    Reclaimed(HostJobIdentity),
}

struct Shared {
    identity: HostJobIdentity,
    kind: Kind,
    model: Arc<Mutex<PackageEntities>>,
    state: Mutex<State>,
    _prepared: HostRetainedPrepared,
}

/// The Owner keeps one handle until exact completion and causal application.
#[derive(Clone)]
pub(crate) struct Work(Arc<Shared>);

struct Active {
    work: Work,
    reservation: Option<CausalReservation>,
    receipt: Option<CausalReceipt>,
    observed: bool,
    faulted: bool,
}

impl Active {
    fn observe(
        &mut self,
        identity: HostJobIdentity,
        kind: Kind,
        table_faulted: bool,
    ) -> CausalTransitionStatus {
        let active = self;
        if active.work.0.identity != identity || active.faulted {
            return CausalTransitionStatus::Fault;
        }
        if table_faulted {
            active.faulted = true;
            return CausalTransitionStatus::Fault;
        }
        let Some(state) = active.work.completed(identity, kind) else {
            active.faulted = true;
            return CausalTransitionStatus::Fault;
        };
        if !active.observed {
            let reservation = active
                .reservation
                .take()
                .expect("model work reserves causality");
            active.receipt = state.causal.map(|operation| reservation.commit(operation));
            active.observed = true;
        }
        if active
            .receipt
            .as_ref()
            .is_some_and(|receipt| !receipt.is_applied())
        {
            CausalTransitionStatus::Waiting
        } else {
            CausalTransitionStatus::Applied
        }
    }
}

/// The package continuation owns detached cleanup without reserving the live model.
pub(crate) struct Detached {
    active: Active,
}

impl Detached {
    pub(crate) fn work(&self) -> &Work {
        &self.active.work
    }

    pub(crate) fn observe(
        &mut self,
        runtime: &super::HubRuntime,
        identity: HostJobIdentity,
        kind: Kind,
    ) -> CausalTransitionStatus {
        self.active
            .observe(identity, kind, runtime.causal_scopes.is_faulted())
    }

    pub(crate) fn fault(&mut self) {
        self.active.faulted = true;
    }

    pub(crate) fn output(&self) -> Option<MutexGuard<'_, State>> {
        if self.active.faulted
            || !self.active.observed
            || self
                .active
                .receipt
                .as_ref()
                .is_some_and(|receipt| !receipt.is_applied())
        {
            return None;
        }
        self.active
            .work
            .completed(self.active.work.0.identity, Kind::CleanupDetached)
    }

    pub(crate) fn release(&self) -> bool {
        let Some(mut state) = self.output() else {
            return false;
        };
        if !matches!(
            state.operation.as_ref(),
            Some(Operation::Cleanup { retained: None, .. })
        ) {
            return false;
        }
        state.phase = Phase::Released;
        true
    }
}

/// The runtime retains active work even if its consumer loses a completion.
#[derive(Default)]
pub(super) struct Owner {
    active: RefCell<Option<Active>>,
    readiness: Cell<Readiness>,
    changed: Cell<bool>,
}

impl Owner {
    pub(super) fn update_readiness(&self, model: &PackageEntities) {
        self.readiness.set(Readiness::read(model));
    }
}

impl super::HubRuntime {
    /// Synchronous callers use the same model outside daemon execution.
    pub(super) fn with_direct_entity_model<R>(
        &self,
        f: impl FnOnce(&mut PackageEntities) -> R,
    ) -> R {
        let mut model = self
            .package_entities
            .lock()
            .expect("package entity model lock");
        let result = f(&mut model);
        self.entity_model_owner.update_readiness(&model);
        result
    }

    pub(crate) fn begin_detached_entity_cleanup(
        &self,
        identity: HostJobIdentity,
        operation: Operation,
        permit: &HostWorkPermit,
    ) -> Result<Detached, (CausalTransitionStatus, Operation)> {
        if operation.kind() != Kind::CleanupDetached
            || self.causal_faulted()
            || operation.input_bytes() > permit.reserved_prepared_bytes()
        {
            return Err((CausalTransitionStatus::Fault, operation));
        }
        if permit.has_retained_prepared_reservation() {
            return Err((CausalTransitionStatus::Waiting, operation));
        }
        let reservation = match self.reserve_causal_transition() {
            Ok(reservation) => reservation,
            Err(status) => return Err((status, operation)),
        };
        Ok(Detached {
            active: Active {
                work: Work::new(
                    identity,
                    Arc::clone(&self.package_entities),
                    operation,
                    permit,
                ),
                reservation: Some(reservation),
                receipt: None,
                observed: false,
                faulted: false,
            },
        })
    }

    /// Fill the reserved record before the Owner submits its first Host phase.
    pub(crate) fn select_entity_model_publication(&self, work: &Work) -> PublicationSelection {
        use crate::package_event_router::CausalAcquireResult;
        let active = self.entity_model_owner.active.borrow();
        let Some(active) = active
            .as_ref()
            .filter(|active| Arc::ptr_eq(&active.work.0, &work.0) && !active.faulted)
        else {
            return PublicationSelection::Fault;
        };
        let Ok(mut state) = active.work.0.state.try_lock() else {
            return PublicationSelection::Fault;
        };
        if state.phase != Phase::Pending {
            return PublicationSelection::Fault;
        }
        let Some(Operation::AdmitPublication(admission)) = state.operation.as_mut() else {
            return PublicationSelection::Fault;
        };
        if admission.pending.is_some() {
            return PublicationSelection::Selected;
        }
        if self.entity_publish_bridge.is_faulted() || self.causal_scopes.is_faulted() {
            self.entity_publish_bridge.retain_faulted();
            self.entity_publish_wait.set(super::PublicationWait::Fault);
            return PublicationSelection::Fault;
        }
        if !self.causal_queue.is_empty() {
            self.entity_publish_wait
                .set(super::PublicationWait::Capacity);
            return PublicationSelection::Waiting;
        }
        let selected = self.entity_publish_bridge.take_if(|pending| {
            if self.causal_faulted() {
                self.entity_publish_bridge.retain_faulted();
                self.entity_publish_wait.set(super::PublicationWait::Fault);
                return None;
            }
            if let Some(scope_id) = pending.scope_id {
                match self.causal_scopes.try_acquire_with_admission_or_wait(
                    scope_id,
                    &pending.identity,
                    pending.mutation.admission(),
                ) {
                    CausalAcquireResult::Acquired => Some(true),
                    CausalAcquireResult::MissingScope => Some(false),
                    CausalAcquireResult::Waiting => {
                        self.entity_publish_wait.set(super::PublicationWait::Table);
                        None
                    }
                    CausalAcquireResult::Fault => {
                        self.entity_publish_bridge.retain_faulted();
                        self.entity_publish_wait.set(super::PublicationWait::Fault);
                        None
                    }
                }
            } else {
                Some(true)
            }
        });
        if let Some((pending, acquired)) = selected {
            admission.pending = Some(pending);
            admission.acquired = acquired;
            self.entity_publish_wait.set(super::PublicationWait::Ready);
            PublicationSelection::Selected
        } else if self.entity_publish_bridge.is_faulted()
            || self.entity_publish_wait.get() == super::PublicationWait::Fault
        {
            self.entity_publish_bridge.retain_faulted();
            self.entity_publish_wait.set(super::PublicationWait::Fault);
            PublicationSelection::Fault
        } else if self.entity_publish_bridge.pending_publish_count() == 0 {
            self.entity_publish_wait.set(super::PublicationWait::Ready);
            PublicationSelection::Empty
        } else {
            PublicationSelection::Waiting
        }
    }

    pub(crate) fn fault_entity_model(&self, identity: HostJobIdentity) {
        if let Some(active) = self.entity_model_owner.active.borrow_mut().as_mut()
            && active.work.0.identity == identity
        {
            active.faulted = true;
        }
    }
    /// Output access requires successful observation of this active reservation.
    pub(crate) fn entity_model_output<'a>(&self, work: &'a Work) -> Option<MutexGuard<'a, State>> {
        let active = self.entity_model_owner.active.borrow();
        let active = active.as_ref()?;
        if !Arc::ptr_eq(&active.work.0, &work.0)
            || active.faulted
            || !active.observed
            || active
                .receipt
                .as_ref()
                .is_some_and(|receipt| !receipt.is_applied())
        {
            return None;
        }
        work.completed(work.0.identity, work.kind())
    }

    pub(crate) fn begin_entity_model(
        &self,
        identity: HostJobIdentity,
        operation: Operation,
        permit: &HostWorkPermit,
    ) -> Result<Work, (CausalTransitionStatus, Operation)> {
        if operation.kind() == Kind::CleanupDetached || self.causal_faulted() {
            return Err((CausalTransitionStatus::Fault, operation));
        }
        if operation.input_bytes() > permit.reserved_prepared_bytes() {
            return Err((CausalTransitionStatus::Fault, operation));
        }
        if self.entity_model_owner.active.borrow().is_some()
            || !self.causal_queue.is_empty()
            || permit.has_retained_prepared_reservation()
        {
            return Err((CausalTransitionStatus::Waiting, operation));
        }
        let reservation = match self.reserve_causal_transition() {
            Ok(reservation) => reservation,
            Err(status) => return Err((status, operation)),
        };
        let work = Work::new(
            identity,
            Arc::clone(&self.package_entities),
            operation,
            permit,
        );
        *self.entity_model_owner.active.borrow_mut() = Some(Active {
            work: work.clone(),
            reservation: Some(reservation),
            receipt: None,
            observed: false,
            faulted: false,
        });
        Ok(work)
    }

    /// Exact completion admits causality once. The model stays reserved until table application.
    pub(crate) fn observe_entity_model(
        &self,
        identity: HostJobIdentity,
        kind: Kind,
    ) -> CausalTransitionStatus {
        let mut active = self.entity_model_owner.active.borrow_mut();
        let Some(active) = active.as_mut() else {
            return CausalTransitionStatus::Fault;
        };
        active.observe(identity, kind, self.causal_scopes.is_faulted())
    }

    /// The consumer retains its work handle and outputs before it releases the reservation.
    pub(crate) fn release_entity_model(&self, work: &Work) -> bool {
        let mut active = self.entity_model_owner.active.borrow_mut();
        let Some(retained) = active.as_ref() else {
            return false;
        };
        if !Arc::ptr_eq(&retained.work.0, &work.0)
            || !retained.observed
            || retained.faulted
            || retained
                .receipt
                .as_ref()
                .is_some_and(|receipt| !receipt.is_applied())
        {
            return false;
        }
        let Some(mut state) = work.completed(work.0.identity, work.kind()) else {
            return false;
        };
        if matches!(
            state.operation.as_ref(),
            Some(Operation::Cleanup {
                retained: Some(_),
                ..
            }) | Some(Operation::Resync {
                retained: Some(_),
                ..
            }) | Some(Operation::TakeFanout { retained: Some(_) })
                | Some(Operation::StepSnapshot {
                    retained: FamilySnapshotWork {
                        step: Some(
                            PackageEntityFamilyStep::Ready { .. }
                                | PackageEntityFamilyStep::Discarded { .. }
                        ),
                        ..
                    },
                    ..
                })
        ) {
            return false;
        }
        if matches!(state.operation.as_ref(), Some(Operation::AdmitPublication(work))
            if work.retained.admission.discarded.is_some())
        {
            return false;
        }
        self.entity_model_owner.readiness.set(state.readiness);
        self.entity_model_owner.changed.set(true);
        if state.resync_changed {
            self.note_package_entity_resync_changed();
        }
        state.phase = Phase::Released;
        drop(state);
        drop(active.take());
        true
    }

    pub(crate) fn entity_model_available(&self) -> bool {
        self.entity_model_owner.active.borrow().is_none()
            && self.causal_queue.is_empty()
            && !self.causal_faulted()
    }

    pub(super) fn entity_model_in_flight(&self) -> bool {
        self.entity_model_owner.active.borrow().is_some()
    }

    pub(crate) fn take_entity_model_notification(&self) -> bool {
        self.entity_model_owner.changed.replace(false)
    }

    pub(crate) fn entity_model_readiness(&self) -> Readiness {
        self.entity_model_owner.readiness.get()
    }
}

impl Work {
    #[cfg(test)]
    pub(crate) fn test_fail_after_operation(&self) {
        self.0.state.lock().unwrap().fail_after_operation = true;
    }

    #[cfg(test)]
    pub(crate) fn test_resync_after(&self) -> Option<String> {
        let state = self.0.state.try_lock().ok()?;
        match state.operation.as_ref()? {
            Operation::Resync {
                retained: Some(cursor),
                ..
            } => cursor.after.clone(),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_resync_release(&self) -> Option<u64> {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        match state.operation.as_ref()? {
            Operation::Resync {
                retained: Some(cursor),
                ..
            } => cursor.test_release(),
            _ => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn test_cleanup_retains_release(&self) -> bool {
        let state = self.0.state.lock().unwrap();
        matches!(state.operation.as_ref(), Some(Operation::Cleanup { retained: Some(cleanup), .. })
            if cleanup.family_cursor.release.is_some())
    }

    #[cfg(test)]
    pub(crate) fn test_pending_publication(
        &self,
        inspect: impl FnOnce(&crate::lua_runtime::PendingEntityPublishRequest),
    ) {
        let state = self.0.state.lock().unwrap();
        let Some(Operation::AdmitPublication(admission)) = state.operation.as_ref() else {
            panic!("the work retains publication admission");
        };
        inspect(
            admission
                .pending
                .as_ref()
                .expect("the original request remains retained"),
        );
    }

    pub(super) fn new(
        identity: HostJobIdentity,
        model: Arc<Mutex<PackageEntities>>,
        operation: Operation,
        permit: &HostWorkPermit,
    ) -> Self {
        // Payloads retain their existing admission charges. The Host reservation
        // also covers this record before the model can remove a payload.
        assert!(permit.reserved_prepared_bytes() >= operation.input_bytes());
        Self(Arc::new(Shared {
            identity,
            kind: operation.kind(),
            model,
            state: Mutex::new(State {
                operation: Some(operation),
                causal: None,
                readiness: Readiness::default(),
                valid: true,
                resync_changed: false,
                phase: Phase::Pending,
                #[cfg(test)]
                fail_after_operation: false,
            }),
            _prepared: permit.retain_prepared_reservation(),
        }))
    }

    pub(crate) fn kind(&self) -> Kind {
        self.0.kind
    }

    /// Never inspect a poisoned record or infer success from an output slot.
    fn completed(&self, identity: HostJobIdentity, kind: Kind) -> Option<MutexGuard<'_, State>> {
        let state = self.0.state.try_lock().ok()?;
        let matches = if kind == Kind::Reclaim {
            state.phase == Phase::Reclaimed(identity)
        } else {
            identity == self.0.identity && kind == self.0.kind && state.phase == Phase::Completed
        };
        matches.then_some(state)
    }

    pub(crate) fn run(&self, identity: HostJobIdentity) -> Kind {
        let mut state = self.0.state.lock().expect("entity model work lock");
        assert_eq!(
            identity, self.0.identity,
            "the model job has its exact identity"
        );
        assert_eq!(state.phase, Phase::Pending, "a model phase executes once");
        if self.0.kind == Kind::CleanupDetached {
            let State {
                operation, causal, ..
            } = &mut *state;
            let Some(Operation::Cleanup { retained, next, .. }) = operation.as_mut() else {
                unreachable!("the detached phase retains cleanup");
            };
            let cleanup = retained.as_mut().expect("the phase retains its cleanup");
            cleanup.step_detached_entity_cleanup(causal);
            *next = Some(cleanup.entity_cleanup_phase());
            #[cfg(test)]
            assert!(
                !state.fail_after_operation,
                "injected failure after detached cleanup"
            );
            state.phase = Phase::Completed;
            return self.0.kind;
        }
        let mut model = self.0.model.lock().expect("package entity model lock");
        let State {
            operation,
            causal,
            valid,
            resync_changed,
            ..
        } = &mut *state;
        match operation
            .as_mut()
            .expect("the model phase retains its input")
        {
            Operation::AdmitPublication(work) => {
                *resync_changed = work.pending.is_some();
                model.admit_publication(work, causal);
            }
            Operation::AdvancePublication(work) => {
                model.advance_publication(work);
                *valid = work.status != Some(super::PublicationAdvance::Fault);
                *resync_changed = *valid;
            }
            Operation::DisposePublication(payload) => model.dispose_publication(payload),
            Operation::ReleasePublication => model.release_publication(causal),
            Operation::ReplyPublication(work) => model.reply_publication(work),
            Operation::Resync { action, retained } => {
                model.step_resync(
                    *action,
                    retained.as_mut().expect("the Host retains its scan cursor"),
                    causal,
                );
            }
            Operation::Cleanup {
                retained,
                next,
                fault,
                detached,
            } => {
                assert!(!*detached);
                let cleanup = retained.as_mut().expect("the model retains cleanup");
                if let Err(error) = model.step_entity_cleanup(cleanup) {
                    *fault = Some(error);
                    *valid = false;
                } else {
                    *resync_changed = true;
                    *next = Some(cleanup.entity_cleanup_phase());
                }
            }
            Operation::Family {
                name,
                expected_generation,
                action,
                progress,
            } => {
                let name = name.as_ref().expect("the family phase retains its name");
                *valid = model
                    .families
                    .get(name.as_str())
                    .is_some_and(|family| family.generation == *expected_generation);
                if *valid {
                    match action {
                        FamilyAction::Check => {}
                        FamilyAction::MarkResync => {
                            *resync_changed = true;
                            model.mark_resync(name);
                        }
                        FamilyAction::BeginSnapshot(sequence) => {
                            *resync_changed = true;
                            let next = model.begin_snapshot(name, *sequence);
                            if *sequence < next.floor {
                                model.rearm_resync(name);
                            }
                            *progress = Some(next);
                        }
                    }
                }
            }
            Operation::SelectProvider { expected, selected } => {
                let expected = expected
                    .as_ref()
                    .expect("selection retains its provider expectation");
                let family = model.family(expected.entity_kind.as_str());
                *selected = Some(ProviderSelection {
                    family_generation: family.generation,
                    obligation: family.provider_obligation(),
                });
            }
            Operation::TakeFanout { retained } => model.fanout.pop_first_into(retained),
            Operation::StepSnapshot {
                name,
                expected_generation,
                generation,
                retained,
            } => {
                let name = name.as_ref().expect("the snapshot step retains its family");
                *valid = model
                    .families
                    .get(name.as_str())
                    .is_some_and(|family| family.generation == *expected_generation);
                if *valid {
                    *resync_changed = true;
                    *generation = Some(model.step_snapshot_into(name, retained));
                }
                if let Some(PackageEntityFamilyStep::ReleaseResync {
                    scope_id,
                    family_token,
                }) = retained.step.as_ref()
                {
                    *causal = Some(CausalOp::Release {
                        scope_id: *scope_id,
                        identity: LeaseIdentity::ProviderResyncNeed {
                            family_token: *family_token,
                        },
                    });
                }
            }
            Operation::FinishFanout { finish, retained } => {
                if let Some(lease) = finish.lease.as_ref() {
                    model.finish_into(lease, finish.scheduled_resync, retained);
                    *causal = retained.as_ref().map(|(operation, _)| *operation);
                    *resync_changed = retained.as_ref().is_some_and(|(_, changed)| *changed);
                }
            }
        }
        #[cfg(test)]
        assert!(
            !state.fail_after_operation,
            "injected failure after model operation"
        );
        state.readiness = Readiness::read(&model);
        drop(model);
        match state.operation.as_mut().expect("the operation is retained") {
            Operation::AdmitPublication(work) => work.clear_inputs(),
            Operation::AdvancePublication(work) => work.clear_inputs(),
            Operation::ReplyPublication(work) => work.clear_inputs(),
            Operation::DisposePublication(_) | Operation::ReleasePublication => {}
            Operation::Family { name, .. } => drop(name.take()),
            Operation::SelectProvider { expected, .. } => drop(expected.take()),
            Operation::StepSnapshot { name, .. } => drop(name.take()),
            Operation::FinishFanout { finish, .. } => {
                if let Some(lease) = finish.lease.as_mut() {
                    drop(std::mem::take(&mut lease.family));
                }
            }
            Operation::TakeFanout { .. } | Operation::Cleanup { .. } | Operation::Resync { .. } => {
            }
        }
        // All fallible work must precede this flag, including input cleanup.
        state.phase = Phase::Completed;
        self.0.kind
    }

    /// A released record can drop on the Owner only after the Host removes variable inputs.
    pub(crate) fn owner_drop_ready(&self) -> bool {
        let Ok(state) = self.0.state.try_lock() else {
            return false;
        };
        if state.phase != Phase::Released {
            return false;
        }
        match state.operation.as_ref() {
            Some(Operation::AdmitPublication(work)) => work.owner_drop_ready(),
            Some(Operation::AdvancePublication(work)) => work.owner_drop_ready(),
            Some(Operation::ReplyPublication(work)) => work.owner_drop_ready(),
            Some(Operation::DisposePublication(None) | Operation::ReleasePublication) => true,
            Some(Operation::Family { name: None, .. }) => true,
            Some(Operation::SelectProvider { expected: None, .. }) => true,
            None
            | Some(Operation::TakeFanout { retained: None })
            | Some(Operation::Cleanup { retained: None, .. })
            | Some(Operation::Resync { retained: None, .. }) => true,
            Some(Operation::StepSnapshot {
                name: None,
                retained,
                ..
            }) => !matches!(
                retained.step,
                Some(
                    PackageEntityFamilyStep::Ready { .. }
                        | PackageEntityFamilyStep::Discarded { .. }
                )
            ),
            Some(Operation::FinishFanout { finish, .. }) => finish
                .lease
                .as_ref()
                .is_none_or(|lease| lease.family.is_empty()),
            _ => false,
        }
    }

    /// The Owner schedules reclamation after it transfers outputs and applies causality.
    pub(crate) fn prepare_reclaim(&self) -> Option<(HostJobIdentity, HostCommand)> {
        let mut state = self.0.state.try_lock().ok()?;
        if state.phase != Phase::Released {
            return None;
        }
        let identity = self.0.identity.next_phase()?;
        state.phase = Phase::ReclaimReady(identity);
        Some((identity, HostCommand::ReclaimEntityModel(self.clone())))
    }

    pub(crate) fn reclaim(&self, identity: HostJobIdentity) -> Kind {
        let mut state = self.0.state.lock().expect("entity model work lock");
        assert_eq!(
            state.phase,
            Phase::ReclaimReady(identity),
            "reclamation needs exact authorization"
        );
        drop(state.operation.take());
        state.causal = None;
        state.phase = Phase::Reclaimed(identity);
        Kind::Reclaim
    }
}
