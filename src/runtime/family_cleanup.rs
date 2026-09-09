//! One retained family item per owner phase.

use std::ops::Bound::{Excluded, Included, Unbounded};

use super::*;

#[derive(Default)]
pub(crate) struct FamilyCleanupCursor {
    retired: Option<(String, Option<PackageEntityFamilyState>)>,
    after: Option<String>,
    queued_after: Option<String>,
    live_complete: bool,
    checked_package: bool,
    pub(crate) release: Option<CausalOp>,
    awaiting_worker: bool,
}

pub(crate) enum FamilyCleanupStep {
    Pending,
    Waiting,
    Fault,
    Payload(PackageEntityMutation),
    Complete,
}

impl HubRuntime {
    /// Select one item. The package operation retains its release until worker completion.
    pub(crate) fn step_host_package_entity_cleanup(
        &self,
        cleanup: &mut HostPackageCleanup,
    ) -> FamilyCleanupStep {
        let cursor = &mut cleanup.family_cursor;
        assert!(
            !cursor.awaiting_worker,
            "wait for the exact worker completion"
        );
        if let Some(release) = cursor.release.take() {
            return match self.admit_causal_op(release) {
                CausalAdmitResult::Applied => FamilyCleanupStep::Pending,
                CausalAdmitResult::Retry(release) => {
                    cursor.release = Some(release);
                    if self.causal_faulted() {
                        FamilyCleanupStep::Fault
                    } else {
                        FamilyCleanupStep::Waiting
                    }
                }
            };
        }
        let Some(epoch) = cleanup.family_epoch else {
            assert!(cleanup.unloaded_families.is_empty());
            return FamilyCleanupStep::Complete;
        };
        if let Some((name, retired)) = &mut cursor.retired {
            if let Some(family) = retired {
                if let Some((seq, payload)) = family.pending_by_seq.pop_first() {
                    cursor.release = family.pending_leases.remove(&seq).map(mutation_release);
                    cursor.awaiting_worker = true;
                    return FamilyCleanupStep::Payload(payload);
                }
                if let Some((_, lease)) = family.pending_leases.pop_first() {
                    cursor.release = Some(mutation_release(lease));
                    return FamilyCleanupStep::Pending;
                }
                if let Some((scope_id, _admission)) = family.resync.leases.pop_first() {
                    cursor.release = Some(CausalOp::Release {
                        scope_id,
                        identity: LeaseIdentity::ProviderResyncNeed {
                            family_token: family
                                .causal_token
                                .expect("resync lease has a family token"),
                        },
                    });
                    return FamilyCleanupStep::Pending;
                }
                // The maps and lease set are empty before the owner drops this state.
                *retired = None;
                return FamilyCleanupStep::Pending;
            }
            let mut model = self
                .package_entities
                .lock()
                .expect("package entity model lock");
            let fanout = &mut model.fanout;
            if let Some(generation) = fanout.next_family_generation_before(name, epoch) {
                let item = fanout
                    .take_one_family(name, generation)
                    .expect("the indexed generation contains a mutation");
                cursor.release = item.lease.map(mutation_release);
                cursor.awaiting_worker = true;
                return FamilyCleanupStep::Payload(item.mutation);
            }
            cursor.retired = None;
            return FamilyCleanupStep::Pending;
        }
        let Some((package, provided)) = cleanup.unloaded_families.last_mut() else {
            return FamilyCleanupStep::Complete;
        };
        let mut model = self
            .package_entities
            .lock()
            .expect("package entity model lock");
        let PackageEntities {
            families,
            fanout,
            resync_releases,
            ..
        } = &mut *model;
        let name = if let Some(name) = provided.pop_first() {
            Some(name)
        } else if !cursor.checked_package {
            cursor.checked_package = true;
            Some(package.clone())
        } else if !cursor.live_complete {
            let prefix = format!("{package}.");
            let lower = cursor
                .after
                .as_ref()
                .map_or_else(|| Included(prefix.clone()), |after| Excluded(after.clone()));
            let name = families
                .range((lower, Unbounded))
                .next()
                .filter(|(name, _)| name.starts_with(&prefix))
                .map(|(name, _)| name.clone());
            if name.is_none() {
                cursor.live_complete = true;
                return FamilyCleanupStep::Pending;
            }
            cursor.after = name.clone();
            name
        } else {
            let name = fanout.next_package_family(package, cursor.queued_after.as_deref());
            cursor.queued_after = name.clone();
            name
        };
        if let Some(name) = name {
            let retired = if families
                .get(&name)
                .is_some_and(|family| family.generation < epoch)
            {
                let family = families.remove(&name).expect("the old family is present");
                resync_releases.remove(&(name.clone(), family.generation));
                Some(family)
            } else {
                None
            };
            cursor.retired = Some((name, retired));
            self.note_package_entity_resync_changed();
        } else {
            cleanup.unloaded_families.pop();
            cursor.after = None;
            cursor.queued_after = None;
            cursor.live_complete = false;
            cursor.checked_package = false;
        }
        FamilyCleanupStep::Pending
    }

    pub(crate) fn complete_host_package_entity_cleanup_item(
        &self,
        cleanup: &mut HostPackageCleanup,
    ) {
        assert!(
            cleanup.family_cursor.awaiting_worker,
            "one worker owns the selected payload"
        );
        cleanup.family_cursor.awaiting_worker = false;
    }

    pub(super) fn drain_direct_package_entity_cleanup(
        &self,
        cleanup: &mut HostPackageCleanup,
    ) -> bool {
        loop {
            match self.step_host_package_entity_cleanup(cleanup) {
                FamilyCleanupStep::Pending => {}
                FamilyCleanupStep::Waiting | FamilyCleanupStep::Fault => return false,
                FamilyCleanupStep::Payload(payload) => {
                    drop(payload);
                    self.complete_host_package_entity_cleanup_item(cleanup);
                }
                FamilyCleanupStep::Complete => return true,
            }
        }
    }
}

fn mutation_release(lease: crate::package_entity_fanout::EntityMutationLease) -> CausalOp {
    CausalOp::Release {
        scope_id: lease.scope_id,
        identity: LeaseIdentity::AdmittedEntityMutation {
            family_token: lease.family_token,
            seq: lease.seq,
        },
    }
}
